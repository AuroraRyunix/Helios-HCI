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

# Is every disk sidon is meant to use actually there?
#
# Sidon owns its disks' mounts: /etc/hci/sidon-disks names the filesystems by UUID, sidon
# mounts them under /var/lib/hci/sidon/disks/<uuid> when it starts, and nothing sidon-related
# is in /etc/fstab, so a late or missing disk cannot fail the boot and `nofail` is not needed.
# What is left to survey is the thing that went wrong when it *was* in fstab: a volume that
# did not mount while sidon carried on and wrote extent groups to the root filesystem at the
# same path, where they are invisible once the real volume mounts underneath them.
#
# Sidon now refuses that itself -- no journal volume, no start; no extent disk, no writes to
# it -- so this is the independent witness rather than the only guard. It judges presence by
# `stat`, never by a mount table: the directory must be a mount point, must be the device
# carrying the filesystem UUID the manifest names, and must hold the disk.uid sentinel. A
# sentinel alone proves nothing (one left on the root filesystem during an unmounted period is
# exactly what a mount later covers) and `findmnt` listed a shadowed mount as mounted.
#
# FAIL rather than WARN for the journal volume: without it sidon cannot say what it has
# acknowledged. Extra capacity missing is a WARN: the store is smaller than intended, not
# misplaced.
STORAGE_SURVEY_INTERVAL = 300
STORAGE_CHECK_CATEGORY = "storage.sidon.mounts"
STORAGE_CHECK_NAME = "sidon_volumes_mounted"
SIDON_ROOT = "/var/lib/hci/sidon"
SIDON_DISKS_MANIFEST = "/etc/hci/sidon-disks"

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

def sidon_declared_disks():
    """[(filesystem uuid, role)] from /etc/hci/sidon-disks, journal volume first.

    None when the node has no manifest, which is a node the rollout has not reached (or a
    development host) and not a node with no disks. Read from the file rather than from a
    fixed list because the extra disks are discovered per host: a node with two spare disks
    has two more of these than a node with none.
    """
    try:
        with open(SIDON_DISKS_MANIFEST, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
    except OSError:
        return None
    disks = []
    for raw in text.splitlines():
        fields = raw.split("#", 1)[0].split()
        if len(fields) >= 2 and fields[1] in ("journal", "extent"):
            disks.append((fields[0], fields[1]))
    disks.sort(key=lambda d: 0 if d[1] == "journal" else 1)
    return disks


def sidon_disk_absence(uuid, root=SIDON_ROOT):
    """None when the disk is provably mounted where it belongs, otherwise why it is not.

    Three proofs, all by stat (see the comment above): a mount point, the right device, the
    sentinel. The order matters for the message and not for the verdict.
    """
    mount = os.path.join(root, "disks", uuid)
    by_uuid = os.path.join("/dev/disk/by-uuid", uuid)
    if not os.path.ismount(mount):
        if not os.path.exists(by_uuid):
            return "its device is not attached"
        return "not mounted (a plain directory on the root filesystem)"
    try:
        wanted = os.stat(by_uuid).st_rdev
    except OSError:
        return "mounted, but its device is not attached to prove it is the right one"
    if os.stat(mount).st_dev != wanted:
        return "something else is mounted there"
    if not os.path.isfile(os.path.join(mount, "disk.uid")):
        return "mounted but carries no disk.uid"
    return None


def sidon_fstab_mounts():
    """The sidon mount points /etc/fstab declares, in file order.

    Only for a node that has no manifest yet and so still mounts through fstab; a node the
    rollout has reached declares nothing here, and a line that is still here is reported.
    """
    targets = []
    try:
        with open("/etc/fstab", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if line.lstrip().startswith("#"):
                    continue
                fields = line.split()
                if len(fields) >= 2 and (fields[1] == SIDON_ROOT
                                         or fields[1].startswith(SIDON_ROOT + "/")):
                    targets.append(fields[1])
    except OSError:
        return []
    return targets


def survey_sidon_fstab_mounts(declared):
    """The survey for a node that still mounts the extent store through fstab."""
    missing = [t for t in declared if not os.path.ismount(t)]
    if not missing:
        return ("PASS",
                "Every sidon volume declared in /etc/fstab is mounted (this node has not yet "
                "moved to the layout where sidon owns its mounts):\n- " + "\n- ".join(declared))
    root_missing = SIDON_ROOT in missing
    status = "FAIL" if root_missing else "WARN"
    detail = [
        "Declared in /etc/fstab and NOT mounted:",
        "- " + "\n- ".join(missing),
        "",
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


def survey_sidon_mounts():
    """(status, output) for whether every disk sidon is meant to use is there."""
    disks = sidon_declared_disks()
    if disks is None:
        declared = sidon_fstab_mounts()
        if declared:
            return survey_sidon_fstab_mounts(declared)
        return ("WARN",
                "No %s and no sidon mounts in /etc/fstab, so this node declares no disks "
                "for sidon and the extent store is on the root filesystem by configuration "
                "rather than by accident. That is valid for a single-filesystem host and "
                "worth knowing either way." % SIDON_DISKS_MANIFEST)

    # The old layout, mounted at the root with a disk nested inside it, waiting for sidon's
    # next start to move it. Its disks are not at the new paths yet, so judging them there
    # would report a working node as failed.
    if os.path.ismount(SIDON_ROOT):
        return ("WARN",
                "%s is itself a mount: this node is still in the layout where the volume is "
                "mounted at the sidon root with a disk nested inside it, which a later mount "
                "of the parent can shadow. The record of its disks is written; sidon moves "
                "the node to the layout where every disk is a sibling the next time it "
                "starts (nothing is moved under a running sidon)." % SIDON_ROOT)

    entries = []
    for uuid, role in disks:
        entries.append((uuid, role, sidon_disk_absence(uuid)))
    if not any(role == "journal" for _, role, _ in entries):
        return ("FAIL",
                "%s names no journal volume, so sidon will not start: it cannot say what it "
                "has acknowledged." % SIDON_DISKS_MANIFEST)

    absent = [(u, r, why) for u, r, why in entries if why]
    if not absent:
        return ("PASS",
                "Every sidon disk is mounted and proven to be the filesystem it should be:\n- "
                + "\n- ".join("%s %s at %s/disks/%s" % (r, u, SIDON_ROOT, u)
                               for u, r, _ in entries))

    journal_gone = any(role == "journal" for _, role, _ in absent)
    status = "FAIL" if journal_gone else "WARN"
    detail = [
        "Named in %s and NOT usable:" % SIDON_DISKS_MANIFEST,
        "- " + "\n- ".join("%s %s: %s" % (r, u, why) for u, r, why in absent),
        "",
        "Nothing sidon owns is in /etc/fstab, so this did not fail the boot, and sidon "
        "refuses these paths rather than writing to the root filesystem in their place.",
    ]
    if journal_gone:
        detail.append(
            "FAIL because the journal volume holds the write-ahead journals and the replica "
            "state: sidon does not start without it, and retries until it appears. Check "
            "`/usr/local/bin/sidon mounts` and the device.")
    else:
        detail.append(
            "WARN because the journal volume is mounted; what is missing is additional "
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
                    # Not `schedules`: that name is the leadership candidacy, held for the life of
                    # the daemon. Rebinding it to the table's rows made the next iteration call
                    # .leading() on a list, which raised every minute and was swallowed by the
                    # except below -- so the scheduled health checks silently stopped after the
                    # first pass in which this node led. See test_candidacy_not_rebound.
                    rows = []
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                rows.append(json.loads(line))
                            except Exception:
                                pass
                    
                    now = int(time.time())
                    for s in rows:
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
