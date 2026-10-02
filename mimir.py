#!/usr/bin/env python3
import sys
import os
import json
import time
import socket
import helios_zk
import urllib.request
import ssl
import subprocess
import threading
import uuid

# The cluster's one CQL query layer. Fifteen files carried their own copy of this, most
# of them identical, and the guard against conditional statements had reached only three
# of them -- see helios_cql for what that cost.
from helios_cql import (  # noqa: F401  (re-exported for modules that import from here)
    ConditionalStatementError,
    cql_escape,
    cql_int,
    is_conditional_cql,
    run_conditional_cql_query,
    run_cql_query,
)

socket.setdefaulttimeout(45.0)

LOCAL_IP = "127.0.0.1"

# Load local environment settings if available
try:
    with open("/etc/hci/spectrum/spectrum.env", "r") as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                if k == "LOCAL_HYPERVISOR_IP":
                    LOCAL_IP = v
except Exception:
    pass

def spark_endpoint(ip):
    """Return (address, verify_identity) for an mTLS call to a spark-daemon.

    Node certificates carry `subjectAltName = IP:<node ip>` and nothing else, so a
    connection can only be tied to the node answering it when it is addressed by that
    same IP. Verification used to be off everywhere, which meant any certificate the
    cluster CA ever signed -- every node's own included -- satisfied a connection to any
    other node.

    Loopback is in no node's SAN. spark-daemon binds 0.0.0.0:9099, so this node's own
    address reaches the same listener and does verify; where that address is unknown the
    identity check is dropped rather than failing the call, since a loopback connection
    cannot be answered by another node in the first place.
    """
    if ip in ("127.0.0.1", "::1", "localhost"):
        if LOCAL_IP and LOCAL_IP not in ("127.0.0.1", "::1", "localhost"):
            return LOCAL_IP, True
        return ip, False
    return ip, True

def run_remote_spark(ip, command):
    ip, verify_identity = spark_endpoint(ip)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = verify_identity

    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=120) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)

# `get_zookeeper_leader_ip` used to live here, as one of nine copies of the same loop. Its
# only caller was the leadership gate below, and the gate no longer asks who leads the
# ensemble -- so the copy went with it. The question itself is still answerable, by
# `helios_zk.leader_ip`, for the callers that genuinely mean it.


_CANDIDACIES = {}
_CANDIDACY_LOCK = threading.Lock()


def candidacy(service):
    """One candidacy per job.

    Triggering the health-check schedule used to be `get_zookeeper_leader_ip() == LOCAL_IP`.
    Which node runs the checks is not a question about the ZooKeeper ensemble, and tying it
    to one meant an ensemble election -- a restart, a blip, a rolling upgrade -- relocated
    the checks along with every other leader-only workload in the cluster, onto one node.
    """
    with _CANDIDACY_LOCK:
        existing = _CANDIDACIES.get(service)
        if existing is None:
            existing = helios_zk.cluster_candidacy(service, LOCAL_IP)
            _CANDIDACIES[service] = existing
        return existing

# Certificate expiry is the one health check that cannot be left to the leader-only
# schedule below. The certificates are per-node, they are what the schedule's own
# fan-out runs over, and the day they lapse every node stops answering at once -- so
# each node surveys its own and publishes the result whether or not it is the leader,
# and whether or not anyone has run `mcli health_checks` recently.
CERT_WARN_DAYS = 30
CERT_FAIL_DAYS = 7
MTLS_CERT_DIRS = ["/etc/hci/spark/certs", "/root/.certs"]
CERT_SURVEY_INTERVAL = 900
CERT_CHECK_CATEGORY = "security.mtls.certs"
CERT_CHECK_NAME = "mtls_cert_expiration"

# Is the extent store actually on the volume it is supposed to be on?
#
# The sidon mounts carry `nofail`, which they need: without it a data disk that is slow to
# appear fails local-fs.target and drops the node into emergency mode with no network, and
# two nodes did exactly that. But nofail trades a loud failure for a silent one. A node
# whose volume did not mount boots perfectly and sidon writes extent groups to the *root
# filesystem* at the same path, where they are smaller, slower, un-replicated, and invisible
# the moment the real volume mounts underneath them.
#
# That happened here: all three nodes came up with /var/lib/hci/sidon unmounted after an
# emergency-mode boot, 202 extent groups sat unreachable on the LV, and the only symptom was
# NBD reads failing for one image. Nothing reported it, because from systemd's point of view
# a nofail mount that did not happen is not a problem.
#
# So this is the detector the nofail change owed. FAIL rather than WARN for the main volume,
# because writing guest data to the wrong filesystem is the kind of thing that looks fine
# until the mount succeeds and the data disappears.
STORAGE_SURVEY_INTERVAL = 300
STORAGE_CHECK_CATEGORY = "storage.sidon.mounts"
STORAGE_CHECK_NAME = "sidon_volumes_mounted"
SIDON_ROOT = "/var/lib/hci/sidon"

def cert_expiry_epoch(cert_path):
    """Return (epoch:int|None, detail:str) for a certificate's notAfter date.

    ssl.cert_time_to_seconds parses OpenSSL's date format without the locale dependency
    strptime("%b %d ...") carries. A date that cannot be parsed returns None so the
    caller reports the certificate as unverified -- the check this replaces answered
    PASS when parsing failed, which is the one answer that is never safe.
    """
    try:
        p = subprocess.Popen(["openssl", "x509", "-enddate", "-noout", "-in", cert_path],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        raw_out, raw_err = p.communicate(timeout=30)
    except Exception as e:
        return None, f"openssl failed: {e}"
    out = raw_out.decode("utf-8", errors="ignore").strip()
    err = raw_err.decode("utf-8", errors="ignore").strip()
    if p.returncode != 0 or "notAfter=" not in out:
        return None, (err or out or f"openssl exited {p.returncode}")[:200]
    date_str = out.split("notAfter=", 1)[1].strip().splitlines()[0].strip()
    try:
        return int(ssl.cert_time_to_seconds(date_str)), date_str
    except Exception:
        pass
    try:
        import calendar
        from datetime import datetime
        dt = datetime.strptime(date_str.replace("GMT", "").strip(), "%b %d %H:%M:%S %Y")
        return int(calendar.timegm(dt.timetuple())), date_str
    except Exception as ex:
        return None, f"unparseable notAfter '{date_str}' ({ex})"

def survey_mtls_certs(now=None):
    """Classify every certificate under the mTLS directories. Returns (status, output)."""
    now = time.time() if now is None else now
    expired, critical, expiring, healthy, unverified = [], [], [], [], []

    for cert_dir in MTLS_CERT_DIRS:
        if not os.path.isdir(cert_dir):
            unverified.append(f"{cert_dir} does not exist on this node")
            continue
        try:
            names = sorted(n for n in os.listdir(cert_dir)
                           if n.endswith(".crt") or n.endswith(".pem"))
        except Exception as e:
            unverified.append(f"cannot list {cert_dir}: {e}")
            continue
        if not names:
            unverified.append(f"no certificates found in {cert_dir}")
            continue
        for name in names:
            path = os.path.join(cert_dir, name)
            epoch, detail = cert_expiry_epoch(path)
            if epoch is None:
                unverified.append(f"{path}: {detail}")
                continue
            days = (epoch - now) / 86400.0
            if days < 0:
                expired.append(f"{path} EXPIRED {abs(days):.1f} days ago (notAfter={detail})")
            elif days < CERT_FAIL_DAYS:
                critical.append(f"{path} expires in {days:.1f} days (notAfter={detail})")
            elif days < CERT_WARN_DAYS:
                expiring.append(f"{path} expires in {days:.1f} days (notAfter={detail})")
            else:
                healthy.append(f"{path} valid for {days:.0f} more days")

    renewal_hint = ("\nRenew with `impa renew` -- see docs/mtls_lifecycle.md. Nothing "
                    "renews these automatically, so they must be replaced before the "
                    "date above or every inter-node call stops at once.")
    if expired or critical:
        status = "FAIL"
        output = ("mTLS certificate expiry is critical:\n- "
                  + "\n- ".join(expired + critical) + renewal_hint)
        if expiring or unverified:
            output += "\nAlso noted:\n- " + "\n- ".join(expiring + unverified)
    elif expiring:
        status = "WARN"
        output = (f"mTLS certificate(s) expiring within {CERT_WARN_DAYS} days:\n- "
                  + "\n- ".join(expiring) + renewal_hint)
        if unverified:
            output += "\nAlso noted:\n- " + "\n- ".join(unverified)
    elif unverified:
        status = "WARN"
        output = ("Some mTLS certificates could not be checked for expiry:\n- "
                  + "\n- ".join(unverified))
        if healthy:
            output += "\nVerified as valid:\n- " + "\n- ".join(healthy)
    elif healthy:
        status = "PASS"
        output = (f"All {len(healthy)} certificate(s) under "
                  f"{', '.join(MTLS_CERT_DIRS)} are valid for more than "
                  f"{CERT_WARN_DAYS} days:\n- " + "\n- ".join(healthy))
    else:
        status = "WARN"
        output = (f"No certificates were found under {', '.join(MTLS_CERT_DIRS)}, so "
                  f"mTLS certificate expiry could not be verified on this node.")
    return status, output

def sidon_fstab_mounts():
    """The sidon mount points /etc/fstab declares, in file order.

    Read from fstab rather than from a hardcoded list, because the extra data disks are
    discovered per host -- a node with two spare disks has two more of these than a node
    with none, and a check that assumed a fixed set would be wrong on both.
    """
    targets = []
    try:
        with open("/etc/fstab", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if line.lstrip().startswith("#"):
                    continue
                fields = line.split()
                if len(fields) >= 2 and fields[1].startswith(SIDON_ROOT):
                    targets.append(fields[1])
    except OSError:
        return []
    return targets


def survey_sidon_mounts():
    """(status, output) for whether every declared sidon volume is mounted."""
    declared = sidon_fstab_mounts()
    if not declared:
        return ("WARN",
                "No sidon mounts are declared in /etc/fstab on this node, so the extent "
                "store is on the root filesystem by configuration rather than by accident. "
                "That is valid for a single-filesystem host and worth knowing either way.")

    missing = [t for t in declared if not os.path.ismount(t)]
    if not missing:
        return ("PASS",
                "Every declared sidon volume is mounted:\n- " + "\n- ".join(declared))

    # The root of the extent store missing is the serious one: sidon keeps writing, to the
    # wrong filesystem, and says nothing.
    root_missing = SIDON_ROOT in missing
    status = "FAIL" if root_missing else "WARN"
    detail = [
        "Declared in /etc/fstab and NOT mounted:",
        "- " + "\n- ".join(missing),
        "",
        "These mounts carry `nofail`, so systemd skipped them without failing the boot. "
        "sidon will have written extent groups to the root filesystem at the same paths, "
        "where they are invisible once the real volume mounts.",
    ]
    if root_missing:
        detail.append(
            "FAIL because %s is the extent store itself. Stop sidon, preserve anything "
            "under %s on the root filesystem, mount the volume, and start sidon."
            % (SIDON_ROOT, SIDON_ROOT))
    else:
        detail.append(
            "WARN because the extent store root is mounted; what is missing is additional "
            "capacity, so the store is smaller than intended rather than misplaced.")
    return status, "\n".join(detail)


def publish_sidon_mount_survey():
    """Upsert the mount survey into hydra.mimir_results, like the certificate survey."""
    status, output = survey_sidon_mounts()
    if status != "PASS":
        sys.stderr.write(f"[Mimir] {STORAGE_CHECK_NAME}: {status}\n{output}\n")
    escaped = output.replace("'", "''")
    cql = (
        "INSERT INTO hydra.mimir_results "
        "(category, check_name, node_ip, status, output, execution_id, timestamp) "
        f"VALUES ('{STORAGE_CHECK_CATEGORY}', '{STORAGE_CHECK_NAME}', '{LOCAL_IP}', "
        f"'{status}', '{escaped}', {uuid.uuid4()}, toTimestamp(now()));"
    )
    run_cql_query(cql)
    return status


def publish_cert_survey():
    """Survey this node's certificates and upsert the result into hydra.mimir_results.

    Written under the check name the console and `mcli health_checks` already render, so
    the existing health view starts showing a continuously refreshed answer instead of
    whatever the last leader-triggered run left behind.
    """
    status, output = survey_mtls_certs()
    if status != "PASS":
        sys.stderr.write(f"[Mimir] {CERT_CHECK_NAME}: {status}\n{output}\n")
    escaped = output.replace("'", "''")
    cql = (
        "INSERT INTO hydra.mimir_results "
        "(category, check_name, node_ip, status, output, execution_id, timestamp) "
        f"VALUES ('{CERT_CHECK_CATEGORY}', '{CERT_CHECK_NAME}', '{LOCAL_IP}', "
        f"'{status}', '{escaped}', {uuid.uuid4()}, toTimestamp(now()));"
    )
    run_cql_query(cql)
    return status

def main():
    print("Mimir health checker daemon started.")
    local_last_run = {}
    last_cert_survey = 0
    last_mount_survey = 0
    schedules = candidacy(helios_zk.SERVICE_MIMIR_SCHEDULES)
    while True:
        try:
            if time.time() - last_cert_survey >= CERT_SURVEY_INTERVAL:
                last_cert_survey = time.time()
                publish_cert_survey()
            # Runs on every node rather than only on the schedule leader: a volume that
            # failed to mount is a fact about this host, and asking the leader about it
            # would miss exactly the node that has the problem.
            if time.time() - last_mount_survey >= STORAGE_SURVEY_INTERVAL:
                last_mount_survey = time.time()
                publish_sidon_mount_survey()
        except Exception as e:
            sys.stderr.write(f"Error in Mimir certificate survey: {e}\n")

        try:
            if schedules.leading():
                cql = "SELECT JSON * FROM hydra.mimir_schedules;"
                rc, stdout, stderr = run_cql_query(cql)
                if rc == 0:
                    schedules = []
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                schedules.append(json.loads(line))
                            except Exception:
                                pass
                    
                    now = int(time.time())
                    for s in schedules:
                        if s.get("enabled", False):
                            name = s.get("schedule_name")
                            last_run = s.get("last_run_epoch", 0)
                            interval = 3600 if name == "hourly_checks" else 86400
                            
                            if name in local_last_run and now - local_last_run[name] < interval:
                                continue
                                
                            if now - last_run >= interval:
                                print(f"[Mimir] Triggering check: {name}...")
                                local_last_run[name] = now
                                cql_update = f"UPDATE hydra.mimir_schedules SET last_run_epoch = {now} WHERE schedule_name = '{name}';"
                                run_cql_query(cql_update)
                                
                                category = s.get("category", "all")
                                run_cmd = f"/usr/local/bin/mcli health_checks run_all" if category == "all" else f"/usr/local/bin/mcli health_checks {category}"
                                threading.Thread(target=run_remote_spark, args=("127.0.0.1", run_cmd), daemon=True).start()
        except Exception as e:
            sys.stderr.write(f"Error in Mimir loop: {e}\n")
            
        time.sleep(60)

if __name__ == "__main__":
    main()
