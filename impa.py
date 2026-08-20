#!/usr/bin/env python3
"""Impa -- lifecycle for the cluster CA and the mTLS certificates it signs.

The certificates under /etc/hci/spark/certs (per-node, presented by spark-daemon on
9099) and /root/.certs (the shared client identity every daemon dials out with) were
minted once by provision.py and never touched again. Nothing renewed them and nothing
watched them, so the failure mode was: on one particular day every inter-node call in
the cluster starts returning a TLS error at the same instant, and the only recovery is
re-provisioning a cluster that is already carrying data.

This tool covers the three things that were missing:

    impa status     what every certificate on this node (or every node) is, what it is
                    addressable as, and how many days it has left
    impa plan       the ordered steps a renewal will take, printed without doing them
    impa renew      the renewal itself, leaf-only or including a CA rotation
    impa selftest   mint a throwaway CA and node/client pair in a temp directory and
                    complete a real mTLS handshake against them, so the minting recipe
                    can be verified without touching a live cluster

Ordering is the whole problem in a renewal, and it is enforced in renewal_plan():
a node must never present a certificate signed by a CA that some peer does not yet
trust. See docs/mtls_lifecycle.md.

Transport is SSH, deliberately. Renewal has to work in the state that makes it
necessary -- expired certificates -- and at that point the mTLS API on 9099 is exactly
what is broken. provision.py seeds root SSH keys and known_hosts across every node for
live migration; this reuses that channel.
"""

import argparse
import base64
import json
import os
import re
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time

# Same thresholds mcli-runner's mtls_cert_expiry_warning uses, so the daemon, this tool
# and the health check never disagree about what "expiring" means.
CERT_WARN_DAYS = 30
CERT_FAIL_DAYS = 7

# provision.py mints everything at 3650 days. Renewal keeps that default rather than
# quietly changing the cluster's expiry horizon; --days overrides it.
DEFAULT_LEAF_DAYS = 3650
DEFAULT_CA_DAYS = 7300

CA_DIR = "/var/lib/hci/certs_staging"
NODE_CERT_DIR = "/etc/hci/spark/certs"
CLIENT_CERT_DIR = "/root/.certs"
INGRESS_CERT_DIR = "/etc/hci/spectrum/certs"
BACKUP_DIR = "/var/lib/hci/cert-backups"

CLUSTER_JSON = "/etc/hci/cluster.json"
SPECTRUM_ENV = "/etc/hci/spectrum/spectrum.env"

SPARK_PORT = 9099

# The only unit that holds an mTLS certificate open. Every client context in this repo
# is built inside the call that uses it (run_remote_spark and friends call
# create_default_context per request), so a renewed client certificate is picked up by
# the next outbound call with no restart at all. spark-daemon is the exception: its
# server context is built once in main() and wraps the listening socket, so it has to be
# restarted before it will present a new node certificate.
MTLS_SERVER_UNIT = "spark-daemon"

LOOPBACK_NAMES = ("127.0.0.1", "::1", "localhost")

STATUS_ORDER = {"FAIL": 0, "WARN": 1, "UNKNOWN": 2, "PASS": 3}


# ---------------------------------------------------------------------------
# Pure helpers. Everything below this line up to the executor section is free of
# subprocesses and filesystem access so it can be unit tested without a cluster --
# see test_mtls_lifecycle.py.
# ---------------------------------------------------------------------------

def parse_openssl_x509(text):
    """Parse the output of `openssl x509 -noout -subject -issuer -dates -ext subjectAltName`.

    Returns a dict with subject/issuer/not_before/not_after strings and the SAN split
    into san_ips and san_dns. Fields openssl did not print come back empty rather than
    absent, so callers never have to guess whether a missing SAN meant "no extension" or
    "parse failed" -- an unparseable certificate is reported by cert_report() as UNKNOWN,
    never as valid.
    """
    info = {"subject": "", "issuer": "", "not_before": "", "not_after": "",
            "san_ips": [], "san_dns": []}
    for line in (text or "").splitlines():
        stripped = line.strip()
        if stripped.startswith("subject="):
            info["subject"] = stripped.split("=", 1)[1].strip()
        elif stripped.startswith("issuer="):
            info["issuer"] = stripped.split("=", 1)[1].strip()
        elif stripped.startswith("notBefore="):
            info["not_before"] = stripped.split("=", 1)[1].strip()
        elif stripped.startswith("notAfter="):
            info["not_after"] = stripped.split("=", 1)[1].strip()
        elif "IP Address:" in stripped or "DNS:" in stripped:
            for item in stripped.split(","):
                item = item.strip()
                if item.startswith("IP Address:"):
                    info["san_ips"].append(item[len("IP Address:"):].strip())
                elif item.startswith("DNS:"):
                    info["san_dns"].append(item[len("DNS:"):].strip())
    return info


def common_name(dn):
    """Pull the CN out of an openssl-printed DN.

    openssl 1.x prints `subject=CN=10.0.0.1` and openssl 3.x prints `subject=CN = 10.0.0.1`;
    both forms appear across the EL versions this fleet has been installed on.
    """
    match = re.search(r"\bCN\s*=\s*([^,/]+)", dn or "")
    return match.group(1).strip() if match else ""


def openssl_date_to_epoch(date_str):
    """Seconds since the epoch for an openssl notBefore/notAfter string, or None.

    ssl.cert_time_to_seconds parses openssl's format without a locale dependency, which
    strptime("%b %d ...") does not -- a node with a non-English locale silently failed
    the strptime path.
    """
    if not date_str:
        return None
    try:
        return int(ssl.cert_time_to_seconds(date_str))
    except Exception:
        pass
    try:
        import calendar
        from datetime import datetime
        cleaned = date_str.replace("GMT", "").strip()
        return int(calendar.timegm(datetime.strptime(cleaned, "%b %d %H:%M:%S %Y").timetuple()))
    except Exception:
        return None


def classify_expiry(days_remaining, warn_days=CERT_WARN_DAYS, fail_days=CERT_FAIL_DAYS):
    """PASS / WARN / FAIL for a number of days left, or UNKNOWN for None.

    None is deliberately not treated as PASS. The check this replaces returned PASS when
    it could not parse the expiry date, which is the one answer that is never safe.
    """
    if days_remaining is None:
        return "UNKNOWN"
    if days_remaining < fail_days:
        return "FAIL"
    if days_remaining < warn_days:
        return "WARN"
    return "PASS"


def worst_status(statuses):
    return min(statuses, key=lambda s: STATUS_ORDER.get(s, 2)) if statuses else "UNKNOWN"


def node_san(ip, hostname=None, vip=None, extra=()):
    """subjectAltName for a node certificate, as an openssl config value.

    provision.py issues `subjectAltName = IP:<node ip>` and nothing else, which is why
    every client in the tree had check_hostname off: a connection addressed by loopback
    or by the floating VIP presents a certificate for a different address and fails
    verification even though it reached the right daemon.

    Loopback is in here because daemons dial 127.0.0.1:9099 to reach their own host, the
    VIP because it is answered by whichever node currently holds it, and the hostname
    because /etc/hci/cluster.json records one per node and libvirt migration uses it.
    """
    entries = ["IP:%s" % ip, "IP:127.0.0.1", "DNS:localhost"]
    if hostname:
        entries.append("DNS:%s" % hostname)
    if vip:
        entries.append("IP:%s" % vip)
    for item in extra:
        entries.append(item)
    seen, ordered = set(), []
    for entry in entries:
        if entry not in seen:
            seen.add(entry)
            ordered.append(entry)
    return ",".join(ordered)


def san_covers(info, addresses):
    """Split `addresses` into those the certificate can be verified for and those it cannot.

    This is the question that decides whether check_hostname can be left on for a given
    connection, so it is reported by `impa status` rather than being discovered as a
    handshake failure in production.
    """
    ips = set(info.get("san_ips") or ())
    names = set(info.get("san_dns") or ())
    covered, missing = [], []
    for address in addresses:
        if address in ips or address in names:
            covered.append(address)
        else:
            missing.append(address)
    return covered, missing


def renewal_plan(hosts, rotate_ca=False):
    """The ordered steps of a renewal, as a list of (phase, action, target, detail).

    The CA signs every node certificate, so the ordering constraint is one sentence: a
    node must not present a certificate until every peer already trusts the CA that
    signed it. For a leaf-only renewal that is free -- the CA has not changed, so every
    peer already trusts the signer -- and the plan is just "install, restart, verify",
    one node at a time so orchestration is never down on more than one node at once.

    A CA rotation cannot be done that way. It needs three passes over the fleet:

        trust    every node gets ca.crt = old || new and trusts both signers
        present  every node gets a leaf signed by the new CA (peers already trust it)
        prune    every node gets ca.crt = new only, once nothing presents an old leaf

    Collapsing any two of those passes strands the cluster: prune before present and a
    node is rejected by peers that already dropped the old CA; present before trust and
    a node offers a signature nobody recognises.
    """
    steps = []
    if rotate_ca:
        steps.append((1, "mint-ca", "ca", "issue the replacement CA key and certificate"))
        for ip in hosts:
            steps.append((2, "trust", ip,
                          "install ca.crt = old || new in both certificate directories, "
                          "restart %s" % MTLS_SERVER_UNIT))
        steps.append((3, "mint-leaf", "ca",
                      "issue client and per-node certificates from the new CA"))
    else:
        steps.append((1, "mint-leaf", "ca",
                      "issue client and per-node certificates from the existing CA"))
    present_phase = 4 if rotate_ca else 2
    for ip in hosts:
        steps.append((present_phase, "present", ip,
                      "install client.crt/key and node.crt/key, restart %s" % MTLS_SERVER_UNIT))
        steps.append((present_phase, "verify", ip,
                      "handshake to %s:%d with hostname verification on" % (ip, SPARK_PORT)))
    if rotate_ca:
        for ip in hosts:
            steps.append((5, "prune", ip,
                          "install ca.crt = new only, restart %s" % MTLS_SERVER_UNIT))
            steps.append((5, "verify", ip,
                          "handshake to %s:%d with hostname verification on" % (ip, SPARK_PORT)))
    return steps


def plan_violates_ordering(steps):
    """Return a reason string if a plan would ever present an untrusted signature.

    Kept separate from renewal_plan so the invariant is asserted against the plan that
    will actually run, not against a restatement of how it was built.
    """
    def indices(action):
        return [i for i, step in enumerate(steps) if step[1] == action]

    trust, present, prune = indices("trust"), indices("present"), indices("prune")
    if trust and present and max(trust) > min(present):
        return "a node presents a new-CA certificate before every peer trusts that CA"
    if prune and present and max(present) > min(prune):
        return "the old CA is dropped before every node presents a new-CA certificate"
    if prune and not trust:
        return "the old CA is dropped without a preceding trust distribution"
    return None


def peer_is_cluster_node(peercert, allowed_ips):
    """Whether a peer certificate belongs to one of the cluster's own nodes.

    Used for connections to the floating VIP, which no certificate is issued for: the
    VIP is answered by whichever node holds it, so there is no single name to pass to
    check_hostname. Requiring the peer's IP SAN to be a configured cluster host is
    weaker than binding the connection to one node, but it still rejects the shared
    client certificate (which carries no SAN at all) being used to stand up a listener
    on the VIP, which is what "any valid cert impersonates any node" allowed.
    """
    san = (peercert or {}).get("subjectAltName", ())
    peer_ips = set(value for kind, value in san if kind == "IP Address")
    allowed = set(allowed_ips or ())
    if not peer_ips:
        return False, "peer certificate carries no IP SAN"
    if not (peer_ips & allowed):
        return False, "peer certificate is issued for %s, none of which is a configured cluster node (%s)" % (
            ", ".join(sorted(peer_ips)), ", ".join(sorted(allowed)) or "none known")
    return True, ", ".join(sorted(peer_ips & allowed))


# ---------------------------------------------------------------------------
# Local and remote execution.
# ---------------------------------------------------------------------------

def run(cmd, stdin=None, timeout=180, cwd=None):
    proc = subprocess.Popen(cmd, shell=True, cwd=cwd,
                            stdin=subprocess.PIPE if stdin is not None else None,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        out, err = proc.communicate(
            input=stdin.encode("utf-8") if isinstance(stdin, str) else stdin, timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        return -1, "", "timed out after %ss: %s" % (timeout, cmd)
    return (proc.returncode,
            out.decode("utf-8", errors="ignore").strip(),
            err.decode("utf-8", errors="ignore").strip())


def local_ip():
    """This node's address as the rest of the cluster knows it.

    spectrum.env is authoritative because provision.py wrote it; the UDP-connect trick is
    the fallback every other daemon in the tree uses.
    """
    try:
        with open(SPECTRUM_ENV, "r") as handle:
            for line in handle:
                if line.startswith("LOCAL_HYPERVISOR_IP="):
                    value = line.strip().split("=", 1)[1].strip()
                    if value:
                        return value
    except Exception:
        pass
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("10.255.255.255", 1))
        address = sock.getsockname()[0]
        sock.close()
        return address
    except Exception:
        return "127.0.0.1"


def cluster_config():
    try:
        with open(CLUSTER_JSON, "r") as handle:
            return json.load(handle)
    except Exception:
        return {}


def cluster_hosts(config=None):
    config = cluster_config() if config is None else config
    return [host for host in config.get("hosts", []) if host.get("ip")]


def is_self(ip, me=None):
    return ip in LOOPBACK_NAMES or ip == (me or local_ip())


def ssh_prefix(ip):
    return ("ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes "
            "root@%s " % ip)


def run_on(ip, cmd, stdin=None, timeout=180, me=None):
    """Run a shell command on `ip`, locally when that is this node.

    The remote form quotes the whole command as a single argument so the caller writes
    one shell script and does not have to reason about two layers of quoting.
    """
    if is_self(ip, me):
        return run(cmd, stdin=stdin, timeout=timeout)
    quoted = "'" + cmd.replace("'", "'\"'\"'") + "'"
    return run(ssh_prefix(ip) + quoted, stdin=stdin, timeout=timeout)


def read_remote(ip, path, me=None):
    rc, out, err = run_on(ip, "cat %s" % path, me=me)
    return (out if rc == 0 else None), (err or out)


def put_remote(ip, path, content, mode="0644", me=None):
    """Write `content` to `path` on `ip` atomically, at `mode`, never world-readable.

    The payload is base64 so a PEM's newlines and slashes never reach a shell, and the
    file is created under a temporary name at its final mode and renamed into place: a
    spark-daemon restarting mid-write must never be able to load half a key, or load a
    key that was briefly readable at its final path.
    """
    payload = base64.b64encode(content.encode("utf-8")).decode("ascii")
    directory = os.path.dirname(path)
    script = (
        "set -e; umask 077; mkdir -p %(dir)s; "
        "printf %%s %(payload)s | base64 -d > %(path)s.impa-new; "
        "chmod %(mode)s %(path)s.impa-new; "
        "mv -f %(path)s.impa-new %(path)s" % {
            "dir": directory, "payload": payload, "path": path, "mode": mode})
    return run_on(ip, script, me=me)


# ---------------------------------------------------------------------------
# Inspection.
# ---------------------------------------------------------------------------

def describe_cert(ip, path, me=None):
    """Read one certificate and return the parsed dict, or None with a reason."""
    rc, out, err = run_on(
        ip,
        "openssl x509 -noout -subject -issuer -startdate -enddate -ext subjectAltName "
        "-in %s 2>&1" % path, me=me)
    if rc != 0:
        return None, (err or out or "openssl exited %s" % rc).strip()[:300]
    info = parse_openssl_x509(out)
    if not info["not_after"]:
        return None, "no notAfter field in openssl output: %s" % " ".join(out.split())[:200]
    return info, ""


def cert_report(ip, path, now=None, expected_addresses=(), me=None):
    """Everything `impa status` prints about one certificate file."""
    now = time.time() if now is None else now
    rc, _, _ = run_on(ip, "test -f %s" % path, me=me)
    if rc != 0:
        return {"path": path, "status": "FAIL", "detail": "missing", "days": None}

    info, problem = describe_cert(ip, path, me=me)
    if info is None:
        return {"path": path, "status": "UNKNOWN", "detail": problem, "days": None}

    epoch = openssl_date_to_epoch(info["not_after"])
    days = None if epoch is None else (epoch - now) / 86400.0
    report = {
        "path": path,
        "subject": common_name(info["subject"]) or info["subject"],
        "issuer": common_name(info["issuer"]) or info["issuer"],
        "not_after": info["not_after"],
        "san_ips": info["san_ips"],
        "san_dns": info["san_dns"],
        "days": days,
        "status": classify_expiry(None if days is None else int(days)),
    }
    if expected_addresses:
        covered, missing = san_covers(info, expected_addresses)
        report["verifiable_for"] = covered
        report["not_verifiable_for"] = missing
    return report


def expected_addresses_for(host, vip):
    """The addresses a node's spark-daemon is actually dialled at.

    Every one of these has to be in the node certificate's SAN for the client-side
    hostname verification to stay on for that route.
    """
    addresses = [host["ip"], "127.0.0.1"]
    if host.get("hostname"):
        addresses.append(host["hostname"])
    if vip:
        addresses.append(vip)
    return addresses


def node_status(ip, host=None, vip=None, me=None):
    reports = []
    expected = expected_addresses_for(host, vip) if host else ()
    reports.append(cert_report(ip, os.path.join(NODE_CERT_DIR, "ca.crt"), me=me))
    reports.append(cert_report(ip, os.path.join(NODE_CERT_DIR, "node.crt"),
                               expected_addresses=expected, me=me))
    reports.append(cert_report(ip, os.path.join(CLIENT_CERT_DIR, "ca.crt"), me=me))
    reports.append(cert_report(ip, os.path.join(CLIENT_CERT_DIR, "client.crt"), me=me))
    reports.append(cert_report(ip, os.path.join(INGRESS_CERT_DIR, "server.crt"), me=me))
    return reports


def format_report(report):
    days = report.get("days")
    left = "expired %.0fd ago" % abs(days) if days is not None and days < 0 else (
        "%.0fd left" % days if days is not None else "unknown")
    line = "  %-6s %-40s %-16s %s" % (
        report["status"], report["path"], left, report.get("not_after", report.get("detail", "")))
    extra = []
    if report.get("subject"):
        san = report.get("san_ips", []) + report.get("san_dns", [])
        extra.append("           CN=%s  issuer=%s  SAN=%s" % (
            report["subject"], report.get("issuer", ""), ", ".join(san) or "none"))
    if report.get("not_verifiable_for"):
        extra.append("           not verifiable for: %s  (hostname verification is dropped "
                     "for these routes)" % ", ".join(report["not_verifiable_for"]))
    return "\n".join([line] + extra)


def cmd_status(args):
    me = local_ip()
    config = cluster_config()
    vip = config.get("vip")
    hosts = cluster_hosts(config)
    if args.all_nodes and hosts:
        targets = [(host["ip"], host) for host in hosts]
    else:
        mine = next((host for host in hosts if host.get("ip") == me), {"ip": me})
        targets = [(me, mine)]

    payload, overall = {}, []
    for ip, host in targets:
        reports = node_status(ip, host=host, vip=vip, me=me)
        payload[ip] = reports
        overall.extend(report["status"] for report in reports)

    if args.json:
        print(json.dumps({"status": worst_status(overall), "vip": vip, "nodes": payload},
                         indent=2, sort_keys=True))
        return 0 if worst_status(overall) == "PASS" else 1

    for ip, reports in payload.items():
        print("%s:" % ip)
        for report in reports:
            print(format_report(report))
        print("")
    print("overall: %s" % worst_status(overall))
    return 0 if worst_status(overall) == "PASS" else 1


def cmd_plan(args):
    hosts = [host["ip"] for host in cluster_hosts()] or [local_ip()]
    steps = renewal_plan(hosts, rotate_ca=args.rotate_ca)
    violation = plan_violates_ordering(steps)
    if violation:
        print("refusing to print an unsafe plan: %s" % violation, file=sys.stderr)
        return 1
    current_phase = None
    for phase, action, target, detail in steps:
        if phase != current_phase:
            print("\nphase %d" % phase)
            current_phase = phase
        print("  %-10s %-16s %s" % (action, target, detail))
    print("")
    return 0


# ---------------------------------------------------------------------------
# Minting.
# ---------------------------------------------------------------------------

CA_CNF = """[req]
distinguished_name = dn
prompt = no
x509_extensions = v3_ca
[dn]
CN = %(cn)s
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
"""

LEAF_CNF = """[req]
distinguished_name = dn
req_extensions = v3_leaf
prompt = no
[dn]
CN = %(cn)s
[v3_leaf]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = %(eku)s
%(san)s"""


def write_config(directory, name, body):
    path = os.path.join(directory, name)
    with open(path, "w") as handle:
        handle.write(body)
    return path


def mint_ca(directory, cn="HCI-Root-CA", days=DEFAULT_CA_DAYS,
            key_name="ca.key", crt_name="ca.crt"):
    """Generate a self-signed CA in `directory`. Returns (key_path, crt_path)."""
    config = write_config(directory, "ca.cnf", CA_CNF % {"cn": cn})
    key = os.path.join(directory, key_name)
    crt = os.path.join(directory, crt_name)
    rc, out, err = run("openssl genrsa -out %s 2048" % key, cwd=directory)
    if rc != 0:
        raise RuntimeError("CA key generation failed: %s" % (err or out))
    rc, out, err = run(
        "openssl req -new -x509 -days %d -key %s -out %s -config %s -extensions v3_ca"
        % (days, key, crt, config), cwd=directory)
    if rc != 0:
        raise RuntimeError("CA certificate generation failed: %s" % (err or out))
    os.chmod(key, 0o600)
    return key, crt


def mint_leaf(directory, cn, days, ca_key, ca_crt, eku, san=None, stem=None):
    """Sign one leaf certificate with `ca_key`. Returns (key_path, crt_path).

    The extendedKeyUsage is passed in rather than defaulted because the node
    certificate is used in both directions: spark-daemon presents it as a server on 9099
    and also dials peers with it, so it needs serverAuth and clientAuth. OpenSSL applies
    the ssl_client purpose when a server verifies a peer and ssl_server when a client
    does, so a node certificate with only serverAuth is rejected on every outbound call.
    """
    stem = stem or cn
    body = LEAF_CNF % {"cn": cn, "eku": eku,
                       "san": ("subjectAltName = %s\n" % san) if san else ""}
    config = write_config(directory, "%s.cnf" % stem, body)
    key = os.path.join(directory, "%s.key" % stem)
    csr = os.path.join(directory, "%s.csr" % stem)
    crt = os.path.join(directory, "%s.crt" % stem)
    rc, out, err = run("openssl genrsa -out %s 2048" % key, cwd=directory)
    if rc != 0:
        raise RuntimeError("%s key generation failed: %s" % (stem, err or out))
    rc, out, err = run("openssl req -new -key %s -out %s -config %s" % (key, csr, config),
                       cwd=directory)
    if rc != 0:
        raise RuntimeError("%s CSR failed: %s" % (stem, err or out))
    rc, out, err = run(
        "openssl x509 -req -days %d -in %s -CA %s -CAkey %s -CAcreateserial -out %s "
        "-extensions v3_leaf -extfile %s" % (days, csr, ca_crt, ca_key, crt, config),
        cwd=directory)
    if rc != 0:
        raise RuntimeError("%s signing failed: %s" % (stem, err or out))
    os.chmod(key, 0o600)
    return key, crt


def ca_expiry_epoch(ca_crt):
    rc, out, _ = run("openssl x509 -noout -enddate -in %s" % ca_crt)
    if rc != 0:
        return None
    return openssl_date_to_epoch(parse_openssl_x509(out)["not_after"])


def check_leaf_fits_ca(ca_crt, leaf_days):
    """Refuse to sign a leaf that outlives its issuer.

    provision.py mints the CA and the leaves at 3650 days each, on the same day, so the
    CA expires at the same instant as the certificates it signed. A leaf renewed near
    that date would be issued with a notAfter past the CA's own, and every peer would
    reject the chain the moment the CA lapsed -- a renewal that looks like it worked and
    breaks the cluster later. Rotating the CA is the answer, not signing anyway.
    """
    expiry = ca_expiry_epoch(ca_crt)
    if expiry is None:
        return "cannot read the CA's own expiry from %s" % ca_crt
    if expiry < time.time() + leaf_days * 86400:
        remaining = (expiry - time.time()) / 86400.0
        return ("the CA at %s expires in %.0f days, which is sooner than the %d-day "
                "certificates being issued; rerun with --rotate-ca" % (ca_crt, remaining, leaf_days))
    return None


# ---------------------------------------------------------------------------
# Verification against a running daemon.
# ---------------------------------------------------------------------------

def probe(ip, ca_file=None, cert=None, key=None, timeout=10):
    """Handshake with a node's spark-daemon with hostname verification on.

    This is the check that the renewal actually worked: it proves the daemon presents a
    certificate that chains to the CA in `ca_file` *and* that is issued for the address
    it was dialled at. A chain-only check would pass while the node still presented some
    other node's certificate, which is the defect this whole exercise is about.
    """
    ca_file = ca_file or os.path.join(CLIENT_CERT_DIR, "ca.crt")
    cert = cert or os.path.join(CLIENT_CERT_DIR, "client.crt")
    key = key or os.path.join(CLIENT_CERT_DIR, "client.key")
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_file)
    context.load_cert_chain(certfile=cert, keyfile=key)
    context.check_hostname = True
    try:
        with socket.create_connection((ip, SPARK_PORT), timeout=timeout) as raw:
            with context.wrap_socket(raw, server_hostname=ip) as wrapped:
                peer = wrapped.getpeercert() or {}
        san = peer.get("subjectAltName", ())
        return True, "verified as %s" % (", ".join(v for _, v in san) or "no SAN")
    except Exception as exc:
        return False, "%s: %s" % (type(exc).__name__, exc)


# ---------------------------------------------------------------------------
# Renewal.
# ---------------------------------------------------------------------------

def backup_node(ip, stamp, me=None):
    script = (
        "set -e; mkdir -p %(backup)s; chmod 700 %(backup)s; "
        "tar czf %(backup)s/%(stamp)s.tgz -C / etc/hci/spark/certs root/.certs; "
        "chmod 600 %(backup)s/%(stamp)s.tgz; echo %(backup)s/%(stamp)s.tgz"
        % {"backup": BACKUP_DIR, "stamp": stamp})
    return run_on(ip, script, me=me)


def restart_spark(ip, me=None):
    return run_on(ip, "systemctl restart %s" % MTLS_SERVER_UNIT, timeout=120, me=me)


def install_ca(ip, pem, me=None):
    """Put the same CA material in both directories a daemon might read it from."""
    for directory in (NODE_CERT_DIR, CLIENT_CERT_DIR):
        rc, out, err = put_remote(ip, os.path.join(directory, "ca.crt"), pem, "0644", me=me)
        if rc != 0:
            return rc, out, err
    return 0, "", ""


def install_leaves(ip, node_crt, node_key, client_crt, client_key, me=None):
    payloads = (
        (os.path.join(NODE_CERT_DIR, "node.crt"), node_crt, "0644"),
        (os.path.join(NODE_CERT_DIR, "node.key"), node_key, "0600"),
        (os.path.join(CLIENT_CERT_DIR, "client.crt"), client_crt, "0644"),
        (os.path.join(CLIENT_CERT_DIR, "client.key"), client_key, "0600"),
    )
    for path, content, mode in payloads:
        rc, out, err = put_remote(ip, path, content, mode, me=me)
        if rc != 0:
            return rc, out, err
    return 0, "", ""


def read_file(path):
    with open(path, "r") as handle:
        return handle.read()


def cmd_renew(args):
    me = local_ip()
    config = cluster_config()
    vip = args.vip or config.get("vip")
    hosts = cluster_hosts(config)
    if args.nodes:
        wanted = set(part.strip() for part in args.nodes.split(",") if part.strip())
        hosts = [host for host in hosts if host["ip"] in wanted]
    if not hosts:
        hosts = [{"ip": me, "hostname": socket.gethostname()}]
    ips = [host["ip"] for host in hosts]

    steps = renewal_plan(ips, rotate_ca=args.rotate_ca)
    violation = plan_violates_ordering(steps)
    if violation:
        print("refusing to run an unsafe plan: %s" % violation, file=sys.stderr)
        return 1

    ca_key = os.path.join(CA_DIR, "ca.key")
    ca_crt = os.path.join(CA_DIR, "ca.crt")
    if not os.path.exists(ca_key):
        print("the CA private key is not on this node (%s). Renewal has to run where "
              "provision.py left it, which is the first host in cluster.json." % ca_key,
              file=sys.stderr)
        return 1
    if not args.rotate_ca:
        problem = check_leaf_fits_ca(ca_crt, args.days)
        if problem:
            print("refusing to renew: %s" % problem, file=sys.stderr)
            return 1

    print("plan (%s):" % ("CA rotation + leaf renewal" if args.rotate_ca else "leaf renewal"))
    for phase, action, target, detail in steps:
        print("  phase %d  %-10s %-16s %s" % (phase, action, target, detail))
    if args.dry_run:
        print("\n--dry-run: nothing was changed.")
        return 0
    if not args.yes:
        print("\nRerun with --yes to execute. Every node's spark-daemon will be "
              "restarted, one at a time.", file=sys.stderr)
        return 1

    stamp = time.strftime("%Y%m%d-%H%M%S")
    for ip in ips:
        rc, out, err = backup_node(ip, stamp, me=me)
        if rc != 0:
            print("backup of %s failed, stopping before anything is changed: %s"
                  % (ip, err or out), file=sys.stderr)
            return 1
        print("[%s] backed up to %s" % (ip, out.splitlines()[-1] if out else "?"))

    staging = tempfile.mkdtemp(prefix="impa-", dir="/var/lib/hci")
    os.chmod(staging, 0o700)
    try:
        old_ca_pem = read_file(ca_crt)
        signing_key, signing_crt = ca_key, ca_crt

        if args.rotate_ca:
            new_key, new_crt = mint_ca(staging, days=args.ca_days,
                                       key_name="ca-new.key", crt_name="ca-new.crt")
            bundle = old_ca_pem.rstrip() + "\n" + read_file(new_crt).rstrip() + "\n"
            for ip in ips:
                rc, out, err = install_ca(ip, bundle, me=me)
                if rc != 0:
                    print("[%s] trust distribution failed: %s" % (ip, err or out), file=sys.stderr)
                    return 1
                restart_spark(ip, me=me)
                # Probed with this node's still-current client certificate: the bundle has
                # widened trust but nothing presents a new-CA certificate yet. A node that
                # does not come back here is caught before anything is reissued.
                ok, detail = wait_for_probe(ip, ca_crt, os.path.join(CLIENT_CERT_DIR, "client.crt"),
                                            os.path.join(CLIENT_CERT_DIR, "client.key"))
                if not ok:
                    print("[%s] did not come back after the trust pass: %s" % (ip, detail),
                          file=sys.stderr)
                    return 1
                print("[%s] trusts both the old and the new CA" % ip)
            signing_key, signing_crt = new_key, new_crt

        client_key, client_crt = mint_leaf(
            staging, "HCI-Client", args.days, signing_key, signing_crt,
            eku="clientAuth", stem="client")
        minted = {}
        for host in hosts:
            key, crt = mint_leaf(
                staging, host["ip"], args.days, signing_key, signing_crt,
                eku="serverAuth,clientAuth",
                san=node_san(host["ip"], host.get("hostname"), vip),
                stem="node-%s" % host["ip"])
            minted[host["ip"]] = (read_file(crt), read_file(key))

        client_crt_pem, client_key_pem = read_file(client_crt), read_file(client_key)
        probe_ca = signing_crt if args.rotate_ca else ca_crt
        for ip in ips:
            node_crt_pem, node_key_pem = minted[ip]
            rc, out, err = install_leaves(ip, node_crt_pem, node_key_pem,
                                          client_crt_pem, client_key_pem, me=me)
            if rc != 0:
                print("[%s] certificate install failed: %s" % (ip, err or out), file=sys.stderr)
                return 1
            restart_spark(ip, me=me)
            ok, detail = wait_for_probe(ip, probe_ca, client_crt, client_key)
            print("[%s] %s %s" % (ip, "renewed," if ok else "RENEWED BUT UNVERIFIED,", detail))
            if not ok:
                print("[%s] stopping here; the remaining nodes are untouched and "
                      "%s/%s.tgz restores this one." % (ip, BACKUP_DIR, stamp), file=sys.stderr)
                return 1

        if args.rotate_ca:
            new_only = read_file(signing_crt)
            for ip in ips:
                rc, out, err = install_ca(ip, new_only, me=me)
                if rc != 0:
                    print("[%s] pruning the old CA failed: %s" % (ip, err or out), file=sys.stderr)
                    return 1
                restart_spark(ip, me=me)
                ok, detail = wait_for_probe(ip, signing_crt, client_crt, client_key)
                print("[%s] old CA dropped, %s" % (ip, detail))
                if not ok:
                    return 1
            # The retired CA is kept, not overwritten. Anything still holding a leaf it
            # signed -- a node that was down for the rotation, a restored backup -- can
            # only be readmitted by re-signing against it or by trusting it again, and
            # neither is possible once the key is gone.
            for name, path in (("ca.key", ca_key), ("ca.crt", ca_crt)):
                shutil.copy(path, os.path.join(CA_DIR, "%s.retired-%s" % (name, stamp)))
            shutil.copy(signing_key, ca_key)
            shutil.copy(signing_crt, ca_crt)
            os.chmod(ca_key, 0o600)
            os.chmod(os.path.join(CA_DIR, "ca.key.retired-%s" % stamp), 0o600)
            print("the new CA is now the cluster CA in %s; the previous one is kept as "
                  "ca.key.retired-%s / ca.crt.retired-%s" % (CA_DIR, stamp, stamp))

        for name in os.listdir(staging):
            shutil.copy(os.path.join(staging, name), os.path.join(CA_DIR, name))
        print("\nrenewal complete. Backups: %s/%s.tgz on every node." % (BACKUP_DIR, stamp))
        return 0
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def wait_for_probe(ip, ca_file, cert, key, attempts=10, delay=2):
    """spark-daemon takes a moment to rebind after a restart; a single probe races it."""
    detail = "not attempted"
    for _ in range(attempts):
        ok, detail = probe(ip, ca_file, cert, key)
        if ok:
            return True, detail
        time.sleep(delay)
    return False, detail


def cmd_rollback(args):
    me = local_ip()
    ips = [host["ip"] for host in cluster_hosts()] or [me]
    if args.nodes:
        wanted = set(part.strip() for part in args.nodes.split(",") if part.strip())
        ips = [ip for ip in ips if ip in wanted]
    archive = "%s/%s.tgz" % (BACKUP_DIR, args.backup)
    failures = 0
    for ip in ips:
        rc, out, err = run_on(ip, "test -f %s && tar xzf %s -C /" % (archive, archive), me=me)
        if rc != 0:
            print("[%s] restore failed: %s" % (ip, err or out), file=sys.stderr)
            failures += 1
            continue
        restart_spark(ip, me=me)
        print("[%s] restored from %s" % (ip, archive))
    return 1 if failures else 0


# ---------------------------------------------------------------------------
# Self test.
# ---------------------------------------------------------------------------

def cmd_selftest(args):
    """Mint a throwaway CA, node and client pair and complete a real mTLS handshake.

    Everything renewal depends on that cannot be asserted from a unit test lives here:
    that the openssl recipe produces a chain openssl and Python both accept, that the IP
    SAN it writes satisfies check_hostname, that the extendedKeyUsage split does not
    reject the node certificate on its own outbound calls, and that a certificate for
    one address is refused for another. Nothing outside the temp directory is touched.
    """
    import threading

    directory = args.dir or tempfile.mkdtemp(prefix="impa-selftest-")
    os.makedirs(directory, exist_ok=True)
    address = "127.0.0.1"
    failures = []
    print("selftest staging: %s" % directory)
    try:
        ca_key, ca_crt = mint_ca(directory, cn="HCI-Selftest-CA", days=30)
        san = node_san(address, hostname="selftest-node", vip=None)
        node_key, node_crt = mint_leaf(directory, address, 30, ca_key, ca_crt,
                                       eku="serverAuth,clientAuth", san=san, stem="node")
        client_key, client_crt = mint_leaf(directory, "HCI-Client", 30, ca_key, ca_crt,
                                           eku="clientAuth", stem="client")

        rc, out, err = run("openssl verify -CAfile %s %s %s" % (ca_crt, node_crt, client_crt))
        print("  chain verify: %s" % (out or err))
        if rc != 0:
            failures.append("openssl verify rejected the minted chain")

        info, _ = describe_cert(address, node_crt, me=address)
        print("  node SAN: IP=%s DNS=%s" % (info["san_ips"], info["san_dns"]))
        if address not in info["san_ips"]:
            failures.append("the node certificate does not carry its own IP in the SAN")

        server_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        server_ctx.load_cert_chain(certfile=node_crt, keyfile=node_key)
        server_ctx.load_verify_locations(cafile=ca_crt)
        server_ctx.verify_mode = ssl.CERT_REQUIRED

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((address, 0))
        listener.listen(8)
        port = listener.getsockname()[1]

        def serve():
            while True:
                try:
                    raw, _ = listener.accept()
                except OSError:
                    return
                try:
                    with server_ctx.wrap_socket(raw, server_side=True) as wrapped:
                        wrapped.recv(16)
                except Exception:
                    pass

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()

        def dial(server_hostname, cert, key, check_hostname=True):
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_crt)
            context.load_cert_chain(certfile=cert, keyfile=key)
            context.check_hostname = check_hostname
            with socket.create_connection((address, port), timeout=5) as raw:
                with context.wrap_socket(raw, server_hostname=server_hostname) as wrapped:
                    wrapped.send(b"x")
                    return wrapped.getpeercert()

        try:
            peer = dial(address, client_crt, client_key)
            print("  client -> node with hostname verification on: OK (%s)"
                  % (peer.get("subjectAltName"),))
        except Exception as exc:
            failures.append("the shared client certificate could not reach the node: %s" % exc)

        try:
            dial(address, node_crt, node_key)
            print("  node -> node with hostname verification on: OK")
        except Exception as exc:
            failures.append("a node certificate was rejected as a client certificate, which "
                            "means the extendedKeyUsage is wrong: %s" % exc)

        try:
            dial("10.255.255.254", client_crt, client_key)
            failures.append("a certificate for %s was accepted for 10.255.255.254; "
                            "hostname verification is not actually binding" % address)
        except ssl.SSLCertVerificationError:
            print("  wrong address is refused: OK")
        except Exception as exc:
            print("  wrong address is refused: OK (%s)" % type(exc).__name__)

        listener.close()
    except Exception as exc:
        failures.append("selftest raised: %s: %s" % (type(exc).__name__, exc))
    finally:
        if not args.dir:
            shutil.rmtree(directory, ignore_errors=True)

    if failures:
        print("\nFAILED:")
        for failure in failures:
            print("  - %s" % failure)
        return 1
    print("\nselftest passed")
    return 0


def build_parser():
    parser = argparse.ArgumentParser(
        prog="impa", description="mTLS certificate lifecycle for the cluster CA")
    sub = parser.add_subparsers(dest="command")

    status = sub.add_parser("status", help="report validity and addressability of every certificate")
    status.add_argument("--all-nodes", action="store_true",
                        help="fan out over SSH to every host in cluster.json")
    status.add_argument("--json", action="store_true")
    status.set_defaults(func=cmd_status)

    plan = sub.add_parser("plan", help="print the ordered steps a renewal would take")
    plan.add_argument("--rotate-ca", action="store_true")
    plan.set_defaults(func=cmd_plan)

    renew = sub.add_parser("renew", help="reissue certificates without re-provisioning")
    renew.add_argument("--rotate-ca", action="store_true",
                       help="replace the CA as well, using the three-pass trust ordering")
    renew.add_argument("--days", type=int, default=DEFAULT_LEAF_DAYS)
    renew.add_argument("--ca-days", type=int, default=DEFAULT_CA_DAYS)
    renew.add_argument("--nodes", help="comma-separated subset of cluster.json hosts")
    renew.add_argument("--vip", help="override the VIP written into every node SAN")
    renew.add_argument("--dry-run", action="store_true")
    renew.add_argument("--yes", action="store_true")
    renew.set_defaults(func=cmd_renew)

    rollback = sub.add_parser("rollback", help="restore the certificates a renewal replaced")
    rollback.add_argument("--backup", required=True, help="timestamp of the backup to restore")
    rollback.add_argument("--nodes")
    rollback.set_defaults(func=cmd_rollback)

    selftest = sub.add_parser(
        "selftest", help="mint a throwaway chain and handshake against it")
    selftest.add_argument("--dir", help="keep the generated material in this directory")
    selftest.set_defaults(func=cmd_selftest)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
