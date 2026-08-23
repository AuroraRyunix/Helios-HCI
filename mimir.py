#!/usr/bin/env python3
import sys
import os
import json
import time
import socket
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

def get_zookeeper_leader_ip():
    """Finds the IP of the current ZooKeeper leader, with active designated leader fallback if the leader is in maintenance."""
    ips = []
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            ips = [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        ips = [LOCAL_IP]
        
    leader_ip = None
    for ip in ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((ip, 2181))
            s.sendall(b"stat")
            resp = s.recv(1024).decode('utf-8', errors='ignore')
            s.close()
            if "mode: leader" in resp.lower() or "mode: standalone" in resp.lower():
                leader_ip = ip
                break
        except Exception:
            pass
            
    # Check if leader is active on port 9091
    leader_active = False
    if leader_ip:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((leader_ip, 9091))
            s.close()
            leader_active = True
        except Exception:
            leader_active = False
            
    if leader_active:
        return leader_ip
        
    # If leader is inactive, find active candidates with port 9091 open
    candidates = []
    for ip in ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((ip, 9091))
            s.close()
            candidates.append(ip)
        except Exception:
            pass
            
    if not candidates:
        return leader_ip if leader_ip else "127.0.0.1"
        
    candidates.sort()
    return candidates[0]

def is_zookeeper_leader():
    return get_zookeeper_leader_ip() == LOCAL_IP

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
    while True:
        try:
            if time.time() - last_cert_survey >= CERT_SURVEY_INTERVAL:
                last_cert_survey = time.time()
                publish_cert_survey()
        except Exception as e:
            sys.stderr.write(f"Error in Mimir certificate survey: {e}\n")

        try:
            if is_zookeeper_leader():
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
