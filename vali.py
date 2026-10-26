#!/usr/bin/env python3
__build__ = "1.2.2"
import sys
import os
import json
import time
import re
import shlex
import socket
import subprocess
import helios_zk
import urllib.request
import ssl
import threading
import uuid
import math
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

# The cluster's one CQL query layer. Fifteen files carried their own copy of this, most
# of them identical, and the guard against conditional statements had reached only three
# of them -- see helios_cql for what that cost.
from helios_cql import (  # noqa: F401  (re-exported for modules that import from here)
    ConditionalStatementError,
    cql_escape,
    cql_int,
    is_conditional_cql,
    parse_replication_factor,
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

_SIDON = None


def sidon_module():
    """helios_sidon, or None. Loaded from /usr/local/bin, which is not on sys.path."""
    global _SIDON
    if _SIDON is not None:
        return _SIDON or None
    try:
        import helios_sidon
        _SIDON = helios_sidon
        return _SIDON
    except ImportError:
        pass
    import importlib.util
    for candidate in ("/usr/local/bin/helios_sidon.py",
                      os.path.join(os.path.dirname(os.path.abspath(__file__)), "helios_sidon.py")):
        if os.path.exists(candidate):
            spec = importlib.util.spec_from_file_location("helios_sidon", candidate)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            _SIDON = module
            return module
    _SIDON = False
    return None


def get_dfs_engine():
    """Which storage engine backs VM disks. Was hardcoded to "linstor" and called by
    nothing; it is now the switch that decides how a VM's disks are made ready to boot."""
    module = sidon_module()
    # No module means the client is not deployed, which is a deployment fault rather
    # than a statement about the engine. "sidon" is the only answer anything can act
    # on now, so it is the honest one.
    return module.dfs_engine() if module else "sidon"


def using_sidon():
    return get_dfs_engine() == "sidon"

def get_default_container():
    return "default-pool"

# VM names and host names arrive from the unauthenticated API on port 9095 and are interpolated
# into root shell commands and CQL statements, so they are validated at every entry point.
# \Z instead of $ because $ would also accept a trailing newline.
OBJECT_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}\Z")
OBJECT_NAME_ERROR = "must start with a letter or digit and contain only letters, digits, '_', '.' or '-' (max 63 characters)"

def is_valid_object_name(name):
    if not name or not isinstance(name, str):
        return False
    return bool(OBJECT_NAME_RE.match(name))



def spark_endpoint(ip):
    """Return (address, verify_identity) for an mTLS call to a spark-daemon.

    Node certificates carry `subjectAltName = IP:<node ip>`, so a connection can only be
    tied to the node answering it when it is addressed by that same IP. Verification used
    to be off here, which meant any certificate the cluster CA ever signed -- every node's
    own included -- satisfied a connection to any other node.

    Loopback is in no node's SAN. spark-daemon binds 0.0.0.0:9099, so this node's own
    address reaches the same listener and does verify; where that address is unknown the
    identity check is dropped rather than failing a call that cannot leave the machine.
    """
    if ip in ("127.0.0.1", "::1", "localhost"):
        if LOCAL_IP and LOCAL_IP not in ("127.0.0.1", "::1", "localhost"):
            return LOCAL_IP, True
        return ip, False
    return ip, True


def run_remote_spark(ip, command, timeout=None):
    """Run a shell command on a node through its spark-daemon.

    The daemon applies 45 seconds to a command whose caller names no timeout, so anything
    that can legitimately take longer (a live migration of a large guest) must say so; the
    HTTP wait is kept longer than the command's so the command's own timeout is what reports.
    """
    ip, verify_identity = spark_endpoint(ip)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = verify_identity

    url = f"https://{ip}:9099/api/v1/execute"
    body = {"command": command}
    wait = 120
    if timeout:
        body["timeout"] = timeout
        wait = max(wait, timeout + 30)
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=wait) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)

def run_mtls_spark_api(ip, path, payload, method="POST"):
    try:
        ip, verify_identity = spark_endpoint(ip)
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
        context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
        context.check_hostname = verify_identity

        url = f"https://{ip}:9099{path}"
        data = None
        if payload is not None and method != "GET":
            data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, context=context, timeout=120) as response:
            res = json.loads(response.read().decode("utf-8"))
            return 0, res, ""
    except Exception as e:
        return -1, {}, str(e)

def run_mtls_spark_api_full(ip, path, payload=None, method="POST", timeout=120):
    """Like run_mtls_spark_api, but keeps the status code and the 4xx body.

    The typed API answers a refused parameter with 400 and a sentence naming it, and
    run_mtls_spark_api reduces every HTTPError to "HTTP Error 400: Bad Request" -- which
    throws away the only part worth reading. A refused parameter means the call site is
    wrong, so it has to arrive somewhere a person will see it.

    Returns (status, body, error). status is 0 when the request could not be made.
    """
    import urllib.error
    address, verify_identity = spark_endpoint(ip)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = verify_identity

    url = f"https://{address}:9099{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8")), ""
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8")), ""
        except Exception:
            return exc.code, {}, str(exc)
    except Exception as e:
        return 0, {}, str(e)

# A live migration of a large guest runs for as long as its memory takes to copy, and the
# daemon's default of 45 seconds for a command is far shorter. A ceiling on a hung migration,
# not a budget for a healthy one.
MIGRATION_COMMAND_TIMEOUT = 3600

# The managed units a host in maintenance stops, in the order they are started. What stays up
# (ZooKeeper, HydraDB, Daruk, Sidon, Hylia, and spark-daemon) and why is spark_daemon_decoded's
# MAINTENANCE_* block and docs/maintenance.md; test_maintenance_flow asserts this equals the
# declared table's complement, so the two cannot drift.
# What leaving maintenance starts: ZooKeeper, then the units the host kept (a no-op for those
# still up, and the repair for one that died during the window), then everything it stopped.
# Hylia was missing from the list that used to stand here, which only worked because maintenance
# never stopped it.
MAINTENANCE_EXIT_START_UNITS = ("zookeeper", "hydra-db", "daruk", "sidon", "hylia")
LEAVABLE_HOST_STATES = ("IN_MAINTENANCE", "SERVICE_MAINTENANCE", "ENTERING_MAINTENANCE", "RECOVERING")
MAINTENANCE_STOP_UNITS = (
    "spectrum", "spectrum-phx", "slate", "agahnim", "catalyst", "vali", "bifrost", "dagur",
    "mimir", "rauru", "logos", "mipha", "gatoway", "urbosa", "kass",
)
MAINTENANCE_EXIT_START_UNITS = MAINTENANCE_EXIT_START_UNITS + tuple(
    u for u in MAINTENANCE_STOP_UNITS if u not in MAINTENANCE_EXIT_START_UNITS
)

# What Sidon's takeover may take on the destination: the owner drains its journal (bounded by
# SIDON_HANDOVER_TIMEOUT, 120 s) and the claim and fence follow. Longer than either.
SIDON_TAKEOVER_TIMEOUT = 300


class MigrationStepError(Exception):
    """A migration step failed. `step` names it, so the task error says where it stopped."""

    def __init__(self, step, detail):
        super().__init__(f"step '{step}' failed: {detail}")
        self.step = step
        self.detail = detail


def domain_vdisk_ids(vm_name, disks_list):
    """The vdisks a VM's domain XML names as disks, by the rule generate_vm_xml uses.

    One rule, because a migration that attaches a different set from the one the XML names
    starts the destination qemu against a socket that does not exist.
    """
    module = sidon_module()
    if disks_list and disks_list != "NONE":
        ids = []
        for i, entry in enumerate(disks_list.split(",")):
            parts = entry.split(":")
            if len(parts) > 3 and parts[3]:
                ids.append(parts[3])
            else:
                ids.append(module.vdisk_id_for(vm_name, i))
        return ids
    if disks_list == "NONE":
        return []
    return [module.vdisk_id_for(vm_name, 0)]


def domain_image_ids(iso):
    """The image vdisks a VM's CD-ROMs are backed by."""
    module = sidon_module()
    ids = []
    for spec in (iso or "").split(","):
        spec = spec.strip()
        if spec and spec != "__empty__":
            ids.append(module.image_vdisk_id(spec))
    return ids


def sidon_op(ip, op, vdisk_id, timeout=120, **extra):
    """One DFS vdisk operation on a node. Returns (ok, body, detail); detail is the refusal
    text Sidon gave (a 409 or 503 body), which run_mtls_spark_api would reduce to a status."""
    payload = {"op": op, "vdisk_id": vdisk_id}
    payload.update(extra)
    status, body, err = run_mtls_spark_api_full(ip, "/api/v1/dfs/vdisk", payload, timeout=timeout)
    if status == 200 and isinstance(body, dict):
        return True, body, ""
    detail = body.get("error") if isinstance(body, dict) else ""
    return False, body if isinstance(body, dict) else {}, detail or err or f"HTTP {status}"


def prepare_destination_storage(target_ip, data_ids, image_ids):
    """Attach a VM's disks on the migration target before the domain is started there.

    Data disks are attached in forwarding mode: the destination qemu opens a socket whose
    Sidon relays to the node that still owns the disk, so the single writer does not change
    while two qemu processes have the export open. Images are attached ordinarily (they are
    immutable and have no owner to forward to).

    Returns the data disks this call attached, which are the ones to detach if the migration
    fails. Images are never detached on a failure: an immutable image is shared, and may be
    the CD-ROM of a guest that is running fine. Raises MigrationStepError, having already
    detached what it attached.
    """
    created = []
    try:
        for vdisk_id in data_ids:
            ok, body, detail = sidon_op(target_ip, "attach", vdisk_id, forward=True)
            if not ok:
                raise MigrationStepError(
                    f"attach {vdisk_id} on the target", detail)
            if body.get("already_attached") and not body.get("forwarding_to"):
                # The target previously had this disk directly attached (e.g. from an earlier run
                # before the VM migrated to the current source), so its in-memory attachment had
                # forwarding_to=None. Detach it so we can re-attach cleanly in forwarding mode.
                sidon_op(target_ip, "detach", vdisk_id)
                ok, body, detail = sidon_op(target_ip, "attach", vdisk_id, forward=True)
                if not ok:
                    raise MigrationStepError(
                        f"attach {vdisk_id} on the target (retry after clearing stale attachment)", detail)
            if not body.get("already_attached"):
                created.append(vdisk_id)
            if not body.get("forwarding_to"):
                # No other node owns it, so this attach took it. The guest is running on
                # the source, whose Sidon is now fenced out: the disk is not safe to migrate.
                raise MigrationStepError(
                    f"attach {vdisk_id} on the target",
                    "the target would own the disk while the guest still runs on the source; "
                    "Hydra names no other owner for it")
        for vdisk_id in image_ids:
            ok, body, detail = sidon_op(target_ip, "attach", vdisk_id)
            if not ok:
                raise MigrationStepError(f"attach image {vdisk_id} on the target", detail)
    except MigrationStepError:
        detach_destination_forwarders(target_ip, created)
        raise
    return created


def detach_destination_forwarders(target_ip, vdisk_ids):
    """Remove the forwarders a failed migration left on the target. The source never changed
    owner, so there is nothing to give back; a detach that itself fails is reported."""
    problems = []
    for vdisk_id in vdisk_ids:
        ok, _, detail = sidon_op(target_ip, "detach", vdisk_id)
        if not ok:
            problems.append(f"{vdisk_id}: {detail}")
            sys.stderr.write(f"Could not detach {vdisk_id} on {target_ip} after a failed migration: {detail}\n")
    return problems


def hand_over_storage(target_ip, data_ids, attempts=3, pause=5.0, sleep=None):
    """Make the target the owner of each disk, after the guest has resumed there.

    Sidon's takeover is idempotent and safe to repeat, so a failure is retried: the usual
    cause is the source's qemu not having let go of its socket yet. Returns the failures as
    (vdisk_id, step-named detail); empty means every disk is owned by the target.
    """
    sleep = sleep or time.sleep
    failures = []
    for vdisk_id in data_ids:
        detail = ""
        for attempt in range(attempts):
            ok, _, detail = sidon_op(target_ip, "takeover", vdisk_id,
                                     timeout=SIDON_TAKEOVER_TIMEOUT)
            if ok:
                detail = ""
                break
            if attempt + 1 < attempts:
                sleep(pause)
        if detail:
            failures.append((vdisk_id, detail))
    return failures


def spark_unit_action(ip, action, units, detach=False, ignore_failed=False):
    """Act on systemd units on a host. Returns (ok, detail).

    Replaces `run_remote_spark(ip, "systemctl <verb> <names>")`: the unit names travel as
    parameters the far side matches against its own allow-list, so nothing a caller sends
    can turn into a different command.
    """
    payload = {"action": action, "units": list(units)}
    if detach:
        payload["detach"] = True
    if ignore_failed:
        payload["ignore_failed"] = True
    status, body, error = run_mtls_spark_api_full(ip, "/api/v1/host/units", payload)
    if status == 200:
        return True, ""
    if error:
        return False, error
    return False, str((body or {}).get("error") or "spark-daemon answered %s" % status)

def get_nvram_restore_cmd(vm_name):
    import base64
    py_code = f"""
import base64, urllib.request, json, os, subprocess, shutil
vm_name = '{vm_name}'
nvram_path = f'/var/lib/hci/aether/nvram/{{vm_name}}_vars.fd'
template_path = '/usr/share/edk2/ovmf/OVMF_VARS.fd'
os.makedirs(os.path.dirname(nvram_path), exist_ok=True)
nvram_data = None
try:
    req = urllib.request.Request('http://127.0.0.1:9043/query', data=f"SELECT nvram_data FROM hydra.vm_nvram WHERE vm_name = '{{vm_name}}';".encode('utf-8'), headers={{'Content-Type': 'text/plain'}})
    with urllib.request.urlopen(req, timeout=5) as response:
        res = json.loads(response.read().decode('utf-8'))
        if res.get('status') == 'success' and res.get('rows'):
            nvram_data = res['rows'][0].get('nvram_data')
except Exception:
    pass
if not nvram_data:
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('10.255.255.255', 1))
        local_ip = s.getsockname()[0]
        s.close()
        cql = f"SELECT nvram_data FROM hydra.vm_nvram WHERE vm_name = '{{vm_name}}';"
        cmd = f"echo {{base64.b64encode(cql.encode('utf-8')).decode('utf-8')}} | base64 -d | podman exec -i systemd-hydra-db cqlsh {{local_ip}}"
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        if res.returncode == 0:
            lines = [l.strip() for l in res.stdout.decode('utf-8', errors='ignore').splitlines() if l.strip()]
            for line in lines:
                if len(line) > 50 and not line.startswith('(') and not line.startswith('-'):
                    nvram_data = line
                    break
    except Exception:
        pass
if nvram_data:
    try:
        with open(nvram_path, 'wb') as f:
            f.write(base64.b64decode(nvram_data))
        os.chmod(nvram_path, 0o666)
    except Exception:
        pass
elif os.path.exists(template_path) and not os.path.exists(nvram_path):
    try:
        shutil.copy(template_path, nvram_path)
        os.chmod(nvram_path, 0o666)
    except Exception:
        pass
"""
    b64_code = base64.b64encode(py_code.encode('utf-8')).decode('utf-8')
    return f"python3 -c \"import base64; exec(base64.b64decode('{b64_code}').decode('utf-8'))\""

def get_nvram_backup_cmd(vm_name, delete_local=False):
    import base64
    py_code = f"""
import base64, urllib.request, json, os, subprocess
vm_name = '{vm_name}'
nvram_path = f'/var/lib/hci/aether/nvram/{{vm_name}}_vars.fd'
if os.path.exists(nvram_path):
    try:
        with open(nvram_path, 'rb') as f:
            content = f.read()
        b64_data = base64.b64encode(content).decode('utf-8')
        cql = f"INSERT INTO hydra.vm_nvram (vm_name, nvram_data) VALUES ('{{vm_name}}', '{{b64_data}}');"
        saved = False
        try:
            req = urllib.request.Request('http://127.0.0.1:9043/query', data=cql.encode('utf-8'), headers={{'Content-Type': 'text/plain'}})
            with urllib.request.urlopen(req, timeout=5) as response:
                saved = json.loads(response.read().decode('utf-8')).get('status') == 'success'
        except Exception:
            saved = False
        if not saved:
            # Daruk did not take it. The statement goes to cqlsh on stdin: a varstore is about a
            # megabyte once encoded, which is past the length of a command line, so passing it as
            # an argument failed with "Argument list too long" and the failure was swallowed.
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('10.255.255.255', 1))
            local_ip = s.getsockname()[0]
            s.close()
            done = subprocess.run(['podman', 'exec', '-i', 'systemd-hydra-db', 'cqlsh', local_ip],
                                  input=cql.encode('utf-8'), timeout=30,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            saved = (done.returncode == 0)
    except Exception:
        saved = False
    # The local copy goes only once the database has the data: deleting it after a failed write
    # lost the guest's UEFI boot entries on the next stop.
    if {delete_local} and saved:
        try:
            os.remove(nvram_path)
        except Exception:
            pass
"""
    b64_code = base64.b64encode(py_code.encode('utf-8')).decode('utf-8')
    return f"python3 -c \"import base64; exec(base64.b64decode('{b64_code}').decode('utf-8'))\""


DARUK_URL = "http://127.0.0.1:9043"

def run_lwt(endpoint, params, timeout=15):
    """Call one of Daruk's typed compare-and-swap endpoints.

    Returns `(ok, applied, current, error)`.

    `ok` is False only for a genuine failure: Daruk unreachable, a malformed request, a
    database error. A compare-and-swap that was *refused* is `(True, False, {...}, "")`
    and belongs to the caller, not to an error handler -- it means someone else holds what
    was being claimed, and `current` says what they hold. Collapsing the two is what turns
    a correctly refused claim into a spurious task failure, and, in the other direction,
    a lost race into a second VM start on a second host.

    There is deliberately no cqlsh fallback. That fallback exists so host services keep
    working while Daruk is down, but it can only run statement text and cannot report
    whether a condition held; an ownership write that cannot be made conditional must not
    be made at all.
    """
    import urllib.request
    import urllib.error
    import json
    try:
        req = urllib.request.Request(
            f"{DARUK_URL}{endpoint}",
            data=json.dumps(params).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            res = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return False, False, {}, json.loads(e.read().decode("utf-8")).get("error", f"HTTP {e.code}")
        except Exception:
            return False, False, {}, f"HTTP {e.code}"
    except Exception as e:
        return False, False, {}, f"Daruk is not answering on {DARUK_URL}: {e}"
    if res.get("status") != "success":
        return False, False, {}, res.get("error", "compare-and-swap failed")
    return True, bool(res.get("applied")), res.get("current") or {}, ""

# -- Which node does which leader-only job ------------------------------------------------
#
# Two of them in this process, and they are two questions. Draining Catalyst's `vali` queue
# and running the DRS pass have nothing to do with each other beyond both being things that
# must happen once in the cluster, and both used to be `get_zookeeper_leader_ip() == LOCAL_IP`
# -- the node the ZooKeeper ensemble had elected, for ZooKeeper's own reasons. An ensemble
# election is a restart, a blip, a rolling upgrade, and when one happened both of these moved
# at once, onto whichever node ZooKeeper picked, along with every other leader-only workload
# in the cluster.
_CANDIDACIES = {}
_CANDIDACY_LOCK = threading.Lock()


def candidacy(service):
    with _CANDIDACY_LOCK:
        existing = _CANDIDACIES.get(service)
        if existing is None:
            hosts = [h.get("ip") for h in (get_cluster_hosts() or []) if h.get("ip")]
            existing = helios_zk.cluster_candidacy(service, LOCAL_IP, hosts=hosts)
            _CANDIDACIES[service] = existing
        return existing


def get_catalyst_target_ip():
    """The address of the Catalyst holding the queues.

    Read from the dispatch election: the winner publishes its own address, so finding it is
    a znode read rather than a round of `stat` probes. The probe path below is the fallback
    for an unreachable ensemble, where there is no published answer and a submission still
    has to go somewhere -- it is the behaviour this had before, kept for exactly that case
    and not as the normal path.
    """
    published = candidacy(helios_zk.SERVICE_CATALYST_DISPATCH).leader_identity()
    if published:
        address = published.decode("utf-8", "replace").strip()
        if address:
            return address
    return get_zookeeper_leader_ip()

def get_zookeeper_leader_ip():
    """Finds the IP of the current ZooKeeper leader, with active designated leader fallback if the leader is in maintenance."""
    hosts = get_cluster_hosts()
    if not hosts:
        ips = [LOCAL_IP]
    else:
        ips = [h.get("ip") for h in hosts if h.get("ip")]
        
    leader_ip = None
    # One cached probe, shared by every daemon -- see helios_zk.leader_ip. Nine
    # copies of this loop on nine timers had the ensemble answering eleven `stat`
    # probes a second forever, and ZooKeeper logs two INFO lines for each one.
    leader_ip = helios_zk.leader_ip(ips)
            
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

def call_catalyst_api(path, payload=None, method="GET"):
    import urllib.request
    import json
    leader_ip = get_catalyst_target_ip()
    # Catalyst requires a cluster-signed certificate: it dispatches VM lifecycle
    # work and used to accept it from anything that could open a socket to 9091.
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                     cafile="/etc/hci/spark/certs/ca.crt")
    ctx.load_cert_chain(certfile="/etc/hci/spark/certs/node.crt",
                        keyfile="/etc/hci/spark/certs/node.key")
    url = f"https://{leader_ip}:9091{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=35) as response:
            if response.status == 204:
                return 204, None
            res = json.loads(response.read().decode("utf-8"))
            return response.status, res
    except Exception as e:
        return -1, str(e)

POLL_INTERVAL_SEC = 2
# Live migration of a multi-GB guest routinely runs for minutes. The previous ceiling
# of 3 polls (~6s) meant callers almost always got "Task timed out" while the migration
# was still running -- indistinguishable from real failure, and it leaves the caller
# unable to safely revoke the dual-primary window it opened for the hand-over.
MIGRATE_TIMEOUT_POLLS = 300   # ~10 minutes
POWER_TIMEOUT_POLLS = 60      # ~2 minutes

def submit_and_wait_task(service, action, task_payload, timeout_polls=3, parent_task_id=None):
    if parent_task_id:
        task_payload = dict(task_payload)
        task_payload["parent_task_id"] = parent_task_id
    status, res = call_catalyst_api("/api/v1/tasks/submit", {
        "service": service,
        "action": action,
        "payload": task_payload
    }, method="POST")
    if status != 200:
        return False, f"Failed to submit task to Catalyst: {res}", ""
        
    task_id = res.get("task_id")
    for _ in range(timeout_polls):
        status, res = call_catalyst_api(f"/api/v1/tasks/status/{task_id}")
        if status == 200:
            t_status = res.get("status")
            if t_status == "completed":
                result = res.get("result", {})
                target_host = result.get("target_host", "")
                return True, "", target_host
            elif t_status == "failed":
                return False, res.get("error_msg", "Task failed."), ""
            else:
                import time
                time.sleep(POLL_INTERVAL_SEC)
                continue
        elif status == 204:
            # No content yet (task not visible to Catalyst). This must also back off:
            # continuing without sleeping burns the whole poll budget in microseconds
            # and turns any timeout_polls value into an effectively instant give-up.
            import time
            time.sleep(POLL_INTERVAL_SEC)
            continue
        else:
            return False, f"Error polling status: {res}", ""
    return False, f"Task timed out after ~{timeout_polls * POLL_INTERVAL_SEC}s (it may still be running in Catalyst).", ""

# Initialize Database Schema
def load_schema_module():
    """Import the ordered cluster schema, wherever this process is running from."""
    try:
        import helios_schema
        return helios_schema
    except ImportError:
        pass
    import importlib.util
    import os as _os
    for candidate in ("/usr/local/bin/helios_schema.py",
                      _os.path.join(_os.path.dirname(_os.path.abspath(__file__)),
                                    "helios_schema.py")):
        if not _os.path.exists(candidate):
            continue
        spec = importlib.util.spec_from_file_location("helios_schema", candidate)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    raise ImportError(
        "helios_schema.py was not found. The cluster schema cannot be applied without "
        "it; reinstall the Helios components.")


def init_db_schema():
    """Apply the cluster schema, then the parts that are still this daemon's.

    Nine tables used to be defined here, four of them also defined in
    spectrum_server.py. They now live in helios_schema as one ordered, recorded list, so
    two daemon versions cannot race to define the same table differently -- the loser's
    CREATE TABLE IF NOT EXISTS is a silent no-op and it spends the rest of its life
    reading columns that are not there.

    The ALTER TABLE statements below have deliberately not moved. Scylla errors when the
    column already exists, so they are idempotent only because that error is swallowed
    here; inside a migration, where a failed statement aborts the run, they would break
    every restart after the first. Making them migrations needs an
    add-column-if-absent step that checks system_schema.columns first.

    The unguarded executor is handed over deliberately, as Catalyst already did: the
    schema lock is an IF NOT EXISTS insert and helios_schema reads the [applied] verdict
    itself. Passing `run_cql_query` here raised ConditionalStatementError -- but only on a
    cluster with something actually outstanding, because `ensure_schema` returns before it
    takes the lock when nothing is pending. That is why it survived: it is invisible until
    the first day a migration is added, and then it is every daemon at once.
    """
    applied = load_schema_module().ensure_schema(
        run_conditional_cql_query, node_id=LOCAL_IP)
    if applied:
        sys.stderr.write("Applied schema migrations: %s\n" % ", ".join(applied))

    insert_default_network = """
    INSERT INTO hydra.gatoway_networks (net_id, name, type, vlan_id)
    VALUES (7a68e0d6-11f8-4e89-9430-b3b44b8bc438, 'Physical-Direct', 'direct', null) IF NOT EXISTS;
    """
    # IF NOT EXISTS seeding: a lost race means another node already wrote the row, which
    # is the wanted outcome. run_cql_query refuses conditional statements because it cannot
    # report whether the condition held.
    run_conditional_cql_query(insert_default_network)
    run_cql_query("ALTER TABLE hydra.vms ADD network_id text;")
    run_cql_query("ALTER TABLE hydra.vms ADD cpu_model text;")
    # Transient lifecycle lock column ('migrating'), distinct from 'state' (Running/Stopped).
    # Without it the migration guard below silently never engages.
    run_cql_query("ALTER TABLE hydra.vms ADD status text;")

    # Seed nodes from cluster.json
    try:
        hosts = get_cluster_hosts()
        if hosts:
            rc, stdout, _ = run_cql_query("SELECT JSON hostname FROM hydra.nodes;")
            existing_hostnames = set()
            if rc == 0 and stdout:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            existing_hostnames.add(json.loads(line).get("hostname"))
                        except:
                            pass
            
            for h in hosts:
                hn = h.get("hostname")
                ip = h.get("ip")
                if hn and hn not in existing_hostnames:
                    cql_seed = f"INSERT INTO hydra.nodes (hostname, ip, status, maintenance_mode) VALUES ('{hn}', '{ip}', 'NORMAL', false);"
                    run_cql_query(cql_seed)
    except Exception as e:
        sys.stderr.write(f"Error seeding nodes: {e}\n")

# Global variables for tracking DRS cooldown
last_migration_time = 0.0

def get_cluster_hosts():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            return cdata.get("hosts", [])
    except Exception:
        return []

def get_node_utilization(ip, fetch_cpu=False):
    # Memory usage via typed Spark endpoint
    u_mem = 0.0
    mem_total = 1.0
    mem_used = 0.0
    rc_m, res_m, _ = run_mtls_spark_api(ip, "/api/v1/host/memory", None, method="GET")
    if rc_m == 0 and isinstance(res_m, dict) and "total_mb" in res_m:
        mem_total = float(res_m.get("total_mb", 1.0)) or 1.0
        mem_used = float(res_m.get("used_mb", 0.0))
        u_mem = mem_used / mem_total
    else:
        # Fallback to shell if running against an older spark daemon
        rc_m, stdout_m, _ = run_remote_spark(ip, "free -m")
        if rc_m == 0:
            for line in stdout_m.splitlines():
                if line.strip().startswith("Mem:"):
                    parts = line.split()
                    mem_total = float(parts[1])
                    mem_used = float(parts[2])
                    u_mem = mem_used / mem_total
                    break

    u_cpu = 0.0
    if fetch_cpu:
        # CPU usage via typed Spark endpoint
        rc_c, res_c, _ = run_mtls_spark_api(ip, "/api/v1/host/cpu", None, method="GET")
        if rc_c == 0 and isinstance(res_c, dict) and "load_average" in res_c:
            load = res_c.get("load_average")
            cores = float(res_c.get("cores") or 1.0)
            if load and isinstance(load, list) and len(load) > 0:
                u_cpu = float(load[0]) / cores
        else:
            # Fallback to shell measurement over 0.2s
            cpu_cmd = "python3 -c \"import time; f=open('/proc/stat'); l1=f.readline().split(); f.close(); time.sleep(0.2); f=open('/proc/stat'); l2=f.readline().split(); f.close(); d1=sum(int(x) for x in l1[1:]); d2=sum(int(x) for x in l2[1:]); idle1=int(l1[4]); idle2=int(l2[4]); print(1.0 - (idle2-idle1)/(d2-d1))\""
            rc_c, stdout_c, _ = run_remote_spark(ip, cpu_cmd)
            if rc_c == 0:
                try:
                    u_cpu = float(stdout_c.strip())
                except:
                    pass

    return u_cpu, u_mem, mem_total, mem_used

# Core DRS Algorithm
DRS_VM_COOLDOWN_SECONDS = 1800


def recent_drs_migrations(now, window=DRS_VM_COOLDOWN_SECONDS):
    """{vm name: when it last moved, in seconds} for every migration in the last `window`.

    From hydra.vali_drs_history, which every successful migration writes (the table is called
    DRS but the migrate task fills it for manual moves and evacuations too). Empty when the
    table cannot be read: DRS then behaves as it always did, with its in-memory cooldown.
    """
    rc, stdout, _ = run_cql_query("SELECT JSON vm_name, event_time FROM hydra.vali_drs_history;")
    if rc != 0:
        return {}
    moved = {}
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not (line.startswith("{") and line.endswith("}")):
            continue
        try:
            row = json.loads(line)
            when = float(row.get("event_time") or 0) / 1000.0
        except Exception:
            continue
        name = row.get("vm_name")
        if name and 0 < now - when < window:
            moved[name] = max(moved.get(name, 0.0), when)
    return moved


def run_drs_loop(aggressive=False):
    global last_migration_time
    now = time.time()
    
    drs_enabled = True
    try:
        rc_set, out_set, _ = run_cql_query("SELECT value FROM hydra.cluster_settings WHERE key = 'drs_enabled';")
        if rc_set == 0 and out_set:
            for line in out_set.splitlines():
                line = line.strip()
                if line and not line.startswith('(') and not line.startswith('-') and line != 'value':
                    if line.lower() == 'false':
                        drs_enabled = False
                    break
    except Exception:
        pass
        
    hosts = get_cluster_hosts()
    if len(hosts) < 2:
        return
        
    online_hosts = []
    load_metrics = {}
    host_mem_stats = {}
    
    # Query nodes in maintenance mode
    maintenance_ips = set()
    try:
        rc_n, stdout_n, _ = run_cql_query("SELECT JSON ip, status, maintenance_mode FROM hydra.nodes;")
        if rc_n == 0 and stdout_n:
            for line in stdout_n.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        node = json.loads(line)
                        if node.get("maintenance_mode", False) or node.get("status", "NORMAL") != "NORMAL":
                            maintenance_ips.add(node.get("ip"))
                    except:
                        pass
    except Exception:
        pass

    # 1. Fetch metrics from active hosts in parallel
    from concurrent.futures import ThreadPoolExecutor

    def poll_host_metrics(h):
        ip = h["ip"]
        if ip in maintenance_ips:
            return None
        rc_st, status_data, _ = run_mtls_spark_api(ip, "/api/v1/node/status", None, method="GET")
        if rc_st == 0:
            try:
                if status_data.get("maintenance_status", "NORMAL") != "NORMAL":
                    return None
                services = status_data.get("services", {})
                all_up = True
                for sname, sdata in services.items():
                    if sdata.get("status") != "UP":
                        all_up = False
                        break
                if not all_up:
                    return None
            except:
                return None

            try:
                u_cpu, u_mem, mem_total, mem_used = get_node_utilization(ip, fetch_cpu=True)
                return ip, u_cpu, u_mem, mem_total, mem_used
            except Exception:
                return None
        return None

    with ThreadPoolExecutor(max_workers=min(10, len(hosts))) as executor:
        results = list(executor.map(poll_host_metrics, hosts))

    for res in results:
        if res:
            ip, u_cpu, u_mem, mem_total, mem_used = res
            online_hosts.append(ip)
            # Combine Load Metric (Equal weight CPU & Memory)
            load_metrics[ip] = 0.5 * u_cpu + 0.5 * u_mem
            host_mem_stats[ip] = {"total": mem_total, "used": mem_used, "u_mem": u_mem}
            
    if len(online_hosts) < 2:
        return
        
    # 2. Calculate average and standard deviation
    n = len(online_hosts)
    mean_load = sum(load_metrics.values()) / n
    variance = sum((load_metrics[ip] - mean_load) ** 2 for ip in online_hosts) / n
    std_dev = math.sqrt(variance)
    
    # Calculate VMware-style Balance Score
    # 0 deviation = 100% balance. 0.50 deviation = 0% balance.
    balance_score = max(0, min(100, int((1 - 2 * std_dev) * 100)))
    
    status_str = "Balanced (happy)"
    if balance_score >= 80:
        status_str = "Balanced (happy)"
    elif balance_score >= 50:
        status_str = "Moderate Imbalance"
    else:
        status_str = "High Imbalance"
        
    # Write status to ScyllaDB
    cql_status = f"""
    INSERT INTO hydra.vali_drs_status (cluster_name, current_deviation, status_str, last_drs_run, drs_enabled)
    VALUES ('default', {std_dev}, '{status_str}', {int(now)}, {str(drs_enabled).lower()});
    """
    run_cql_query(cql_status)
    
    if not drs_enabled:
        return
    
    # 3. Check threshold and cooldown rules
    threshold = 0.02 if aggressive else 0.15
    cooldown = 0.0 if aggressive else 300.0
    
    if std_dev > threshold:
        # What this process remembers is lost at every restart and every change of leader, and
        # a migration anyone made -- DRS, an operator, an evacuation -- is in the history table.
        # Read from there, so the cooldown holds across both, and so a VM that was just moved
        # is not picked again straight away (which is how a pair of hosts trade one guest).
        recent = {} if aggressive else recent_drs_migrations(now)
        last_migration_time = max(last_migration_time, max(recent.values(), default=0.0))
        if now - last_migration_time < cooldown:
            print(f"[DRS] Cluster imbalance detected (σ={std_dev:.3f}, Score={balance_score}%), but in cooldown.")
            return
            
        # 4. Find overloaded and underloaded hosts
        overloaded_ip = max(load_metrics, key=load_metrics.get)
        underloaded_ip = min(load_metrics, key=load_metrics.get)
        
        # 5. Get running VMs on overloaded host
        cql_vms = "SELECT JSON name, host_ip, memory, state, affinity_group, anti_affinity_group, pinned_hosts FROM hydra.vms;"
        rc_v, stdout_v, _ = run_cql_query(cql_vms)
        vms = []
        if rc_v == 0:
            for line in stdout_v.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        vms.append(json.loads(line))
                    except:
                        pass
                        
        running_vms = [v for v in vms if v.get("state") == "Running" and v.get("host_ip") == overloaded_ip]
        if not running_vms:
            return
            
        # 6. Evaluate migration benefit (Hysteresis)
        best_vm = None
        best_improvement = 0.0
        
        for vm in running_vms:
            vm_name = vm.get("name")
            vm_mem = float(vm.get("memory", 1024))

            if vm_name in recent and now - recent[vm_name] < DRS_VM_COOLDOWN_SECONDS:
                continue

            allowed_tgt, _, _ = check_vm_host_affinity(vm, underloaded_ip, all_vms=vms)
            if not allowed_tgt:
                continue
            _, src_bonus, _ = check_vm_host_affinity(vm, overloaded_ip, all_vms=vms)
            if src_bonus > 0:
                # Keep co-located affinity-group VMs together unless migrating to a host that also has peers
                _, tgt_bonus, _ = check_vm_host_affinity(vm, underloaded_ip, all_vms=vms)
                if tgt_bonus < src_bonus:
                    continue

            # Predict standard deviation post-migration
            # source host loses memory, target host gains memory
            src_mem_used = host_mem_stats[overloaded_ip]["used"] - vm_mem
            tgt_mem_used = host_mem_stats[underloaded_ip]["used"] + vm_mem
            
            # Verify target host has capacity (at least 200MB free)
            tgt_mem_total = host_mem_stats[underloaded_ip]["total"]
            if src_mem_used < 0 or tgt_mem_used > tgt_mem_total - 200:
                continue
                
            pred_load_src = 0.5 * 0.1 + 0.5 * (src_mem_used / host_mem_stats[overloaded_ip]["total"])
            pred_load_tgt = 0.5 * 0.1 + 0.5 * (tgt_mem_used / tgt_mem_total)
            
            pred_metrics = dict(load_metrics)
            pred_metrics[overloaded_ip] = pred_load_src
            pred_metrics[underloaded_ip] = pred_load_tgt
            
            pred_mean = sum(pred_metrics.values()) / n
            pred_variance = sum((pred_metrics[ip] - pred_mean) ** 2 for ip in online_hosts) / n
            pred_std_dev = math.sqrt(pred_variance)
            
            improvement = std_dev - pred_std_dev
            if improvement > best_improvement:
                best_improvement = improvement
                best_vm = vm
                
        # Trigger migration if improvement satisfies threshold
        min_improvement = 0.001 if aggressive else 0.03
        if best_vm and best_improvement >= min_improvement:
            vm_name = best_vm.get("name")
            print(f"[DRS] Triggering migration of VM '{vm_name}' from {overloaded_ip} to {underloaded_ip} (improvement = {best_improvement:.3f})")
            
            # Submit migrate task to Catalyst
            call_catalyst_api("/api/v1/tasks/submit", {
                "service": "vali",
                "action": "migrate",
                "payload": {"vm_name": vm_name, "target_host": underloaded_ip}
            }, method="POST")
            last_migration_time = now

# Background loops running on leader
def drs_thread_loop():
    drs = candidacy(helios_zk.SERVICE_VALI_DRS)
    while True:
        try:
            if drs.leading():
                run_drs_loop(aggressive=False)
        except Exception as e:
            sys.stderr.write(f"Error in DRS loop thread: {e}\n")
        time.sleep(30)

def get_vm_disk_size(vm_name):
    """The VM's primary disk size in MiB, or None if it cannot be determined.

    None means "could not determine" and the caller must refuse on it -- a guessed size
    that happens to fit lets a migration proceed onto a target that cannot hold the disk.

    This used to parse `linstor volume-definition list` output looking for a cell
    containing "gib". The map holds the number directly.
    """
    vm_data = get_vm_xml_specs(vm_name)
    if not vm_data or not vm_data.get("disks_list"):
        return None
    module = sidon_module()
    if module is None:
        return None
    vdisk_id = module.vdisk_id_for(vm_name, 0)
    rc, stdout, _ = run_cql_query(
        "SELECT JSON size_bytes FROM hydra.dfs_vdisks WHERE vdisk_id = '%s';" % vdisk_id)
    if rc != 0:
        return None
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            size = int(json.loads(line).get("size_bytes") or 0)
        except (ValueError, TypeError):
            continue
        if size > 0:
            return size // (1024 * 1024)
    return None


def get_storage_free_space(target_ip):
    """Free MiB in the target node's extent store, or None if unknown.

    None means "could not determine", and the caller must refuse on it. An earlier version
    of this returned a hard-coded 999999 MiB when the listing could not be parsed -- larger
    than any single VM disk, so the capacity gate approved everything instead of refusing.
    That is the failure this signature exists to prevent, and it is why there is no
    fallback here either.

    Sidon answers with bytes from statvfs on the filesystem holding the extents, which is
    the number that decides whether a drain can complete. The previous implementation
    parsed `linstor storage-pool list` and had to filter the rows down to the target node,
    because that listing returns every node's pools rather than the one asked about.
    """
    rc, body, _err = run_mtls_spark_api(
        target_ip, "/api/v1/dfs/vdisk", {"op": "capacity"})
    if rc != 0 or not isinstance(body, dict):
        return None
    try:
        available = int(body.get("available_bytes") or 0)
    except (TypeError, ValueError):
        return None
    if available <= 0:
        return None
    return available // (1024 * 1024)


def get_vm_xml_specs(name):
    cql = f"SELECT JSON * FROM hydra.vms WHERE name = '{name}';"
    rc, stdout, _ = run_cql_query(cql)
    if rc == 0:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                return json.loads(line)

def get_network_by_id(net_id):
    """Look up a network by UUID — checks gatoway_networks first, then urbosa_segments.
    Returns a normalized dict with at minimum: type, name, net_id.
    Gato entries:   type='direct'|'vlan', vlan_id=<int|None>
    Overlay entries: type='overlay', vni=<int>
    """
    if not net_id:
        return None
    clean_id = str(net_id).strip("'\"")

    # 1. Check Gato L2 networks
    cql = f"SELECT JSON * FROM hydra.gatoway_networks WHERE net_id = {clean_id};"
    rc, stdout, _ = run_cql_query(cql)
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    return json.loads(line)
                except Exception:
                    pass

    # 2. Fallback: check Urbosa overlay segments
    cql2 = f"SELECT JSON * FROM hydra.urbosa_segments WHERE segment_id = {clean_id};"
    rc2, stdout2, _ = run_cql_query(cql2)
    if rc2 == 0 and stdout2:
        for line in stdout2.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    seg = json.loads(line)
                    # Normalize to the same shape callers expect
                    return {
                        "net_id": str(seg.get("segment_id", clean_id)),
                        "name": seg.get("name", ""),
                        "type": "overlay",
                        "vni": seg.get("vni"),
                        "subnet_cidr": seg.get("subnet_cidr", ""),
                    }
                except Exception:
                    pass

    return None

KVM_CACHE = {}
VMWARE_CACHE = {}

# The one place that decides what counts as a SPICE console.
#
# Anything that is not exactly "spice", case and whitespace aside, is VNC -- including
# None, which is what every VM row written before the graphics column holds. Returning a
# value rather than raising is deliberate at the edges that build a domain: an unknown
# graphics type would make libvirt refuse the define at start time, long after the create
# that caused it answered 201, so a value that cannot be honoured is narrowed to the one
# that always can. The API validates separately, where it can still refuse the request.
GRAPHICS_TYPES = ("vnc", "spice")


def normalise_graphics(value):
    """"spice" or "vnc". Never anything else, never None."""
    return "spice" if str(value or "").strip().lower() == "spice" else "vnc"


# vCPU and Memory headroom for hot-add. A domain's maximum vCPU count and memory headroom
# are fixed when it is defined, so a VM that is to be able to gain vCPUs or memory while it
# runs has to be defined with room for them. The CPU topology covers the maximum vCPUs, and
# <maxMemory> reserves memory slots and address space. It is opt-in via cluster setting
# `vm_hotplug_headroom`, and takes effect at the VM's next start. See docs/vm_lifecycle.md.
HOTPLUG_MAX_VCPUS = 16
HOTPLUG_MAX_MEMORY_MB = 131072


def hotplug_vcpu_limit(vcpu):
    """The maximum vCPU count a domain with `vcpu` online is defined with when headroom is on."""
    return max(int(vcpu), min(HOTPLUG_MAX_VCPUS, int(vcpu) * 4))


def hotplug_memory_limit(memory):
    """The maximum memory in MiB a domain with `memory` online is defined with when headroom is on."""
    return max(int(memory), min(HOTPLUG_MAX_MEMORY_MB, int(memory) * 4))


def hotplug_headroom_enabled():
    """Whether new domain definitions reserve vCPU and memory headroom (cluster setting, default off)."""
    rc, out, _ = run_cql_query("SELECT value FROM hydra.cluster_settings WHERE key = 'vm_hotplug_headroom';")
    if rc != 0:
        return False
    for line in (out or "").splitlines():
        line = line.strip()
        if line and not line.startswith("(") and not line.startswith("-") and line != "value":
            return line.lower() == "true"
    return False


def generate_vm_xml(name, memory, vcpu, firmware, disks_list, iso, boot_device="", host_ip="127.0.0.1", network_id=None, cpu_model=None, audio_enabled=False, graphics="vnc", hotplug_max_vcpus=None, hotplug_max_memory=None, cpu_shares=None, cpu_limit=None, cpu_reservation=None, mem_reservation=None, mem_limit=None, pci_devices=None, usb_devices=None, vtpm=False, secure_boot=False, cloud_init_iso=None):
    primary_container = get_default_container()
    if disks_list and disks_list != "NONE":
        first_entry = disks_list.split(",")[0]
        if ":" in first_entry:
            primary_container = first_entry.split(":")[1]

    # What boots first. Per-device boot orders (`<boot order='N'/>`) are what both firmwares
    # honour; the domain-level `<os><boot dev=.../>` list is a legacy BIOS setting that UEFI
    # ignores, so a UEFI guest given an install image booted whatever OVMF's own default order
    # found first and stopped at its boot menu when that was an empty disk. The two cannot be
    # mixed in one domain, so the legacy list is emitted only for the (dead) non-Sidon disk
    # path below. Semantics: Sidon's `boot_orders` and docs/vm_lifecycle.md.
    cdrom_specs = [x.strip() for x in (iso or "").split(",")
                   if x.strip() and x.strip() != "__empty__"]
    orders = sidon_module().boot_orders(
        boot_device,
        0 if disks_list == "NONE" else (len(disks_list.split(",")) if disks_list else 1),
        len(cdrom_specs))
    if using_sidon():
        boot_devices = ""
    elif boot_device == "cdrom" or (not boot_device and cdrom_specs):
        boot_devices = "<boot dev='cdrom'/>\n    <boot dev='hd'/>"
    else:
        boot_devices = "<boot dev='hd'/>\n    <boot dev='cdrom'/>"

    if firmware == "uefi":
        nvram_path = f"/var/lib/hci/aether/nvram/{name}_vars.fd"
        if secure_boot:
            loader_xml = "<loader readonly='yes' secure='yes' type='pflash'>/usr/share/edk2/ovmf/OVMF_CODE.secboot.fd</loader>"
            nvram_tpl = "/usr/share/edk2/ovmf/OVMF_VARS.secboot.fd"
        else:
            loader_xml = "<loader readonly='yes' type='pflash'>/usr/share/edk2/ovmf/OVMF_CODE.fd</loader>"
            nvram_tpl = "/usr/share/edk2/ovmf/OVMF_VARS.fd"
        os_boot_xml = f"""<type arch='x86_64' machine='q35'>hvm</type>
    {loader_xml}
    <nvram template='{nvram_tpl}'>{nvram_path}</nvram>
    {boot_devices}
    <bootmenu enable='yes' timeout='3000'/>"""
    else:
        os_boot_xml = f"""<type arch='x86_64' machine='q35'>hvm</type>
    {boot_devices}
    <bootmenu enable='yes' timeout='3000'/>"""

    # The console device, and the video model that has to match it. Kept byte-identical to
    # the copy in spectrum_server.generate_vm_xml -- a VM defined by the console and the
    # same VM redefined by vali after a migration must come back with the same console, and
    # test_vm_graphics asserts the two agree rather than trusting them to.
    #
    # Anything that is not exactly "spice" is VNC, including None, so a VM whose row
    # predates the graphics column keeps the console it has always had.
    graphics_type = normalise_graphics(graphics)

    if graphics_type == "spice":
        # virtio, the same adapter a VNC guest gets, and deliberately not QXL.
        #
        # QXL is the classic SPICE pairing and the first thing to reach for, but
        # docs/vali.md records that it is avoided here because the BIOS ROM files it needs
        # are missing from the EL 10.2 repositories the hypervisors are built from -- and a
        # video model whose ROM is absent is a domain that will not start. The client does
        # not require it either: spice-html5 decodes the SPICE display channel, which is a
        # protocol, and QEMU serves that channel whatever adapter is behind it.
        video_xml = """<video>
      <model type='virtio' vram='65536' heads='1' primary='yes'/>
    </video>"""
        # Image compression off, deliberately -- see the matching comment in
        # spectrum_server.py for why GLZ is not what this client decodes.
        graphics_xml = """<graphics type='spice' port='-1' autoport='yes' listen='0.0.0.0'>
      <listen type='address' address='0.0.0.0'/>
      <image compression='off'/>
    </graphics>
    <channel type='spicevmc'>
      <target type='virtio' name='com.redhat.spice.0'/>
    </channel>"""
    else:
        video_xml = """<video>
      <model type='virtio' vram='65536' heads='1' primary='yes'/>
    </video>"""
        graphics_xml = """<graphics type='vnc' port='-1' autoport='yes' listen='0.0.0.0'>
      <listen type='address' address='0.0.0.0'/>
    </graphics>"""

    import string
    letters = string.ascii_lowercase
    disk_devices_xml = ""
    disk_paths_with_bus = []
    if disks_list and disks_list != "NONE":
        for idx, entry in enumerate(disks_list.split(",")):
            parts = entry.split(":")
            bus = parts[2] if len(parts) > 2 and parts[2] else "virtio"
            vdisk_id = parts[3] if len(parts) > 3 and parts[3] else sidon_module().vdisk_id_for(name, idx)
            dev_letter = parts[4] if len(parts) > 4 and parts[4] else letters[idx % 26]
            d_path = sidon_module().nbd_socket(vdisk_id)
            disk_paths_with_bus.append((d_path, bus, vdisk_id, dev_letter))
    elif disks_list == "NONE":
        disk_paths_with_bus = []
    else:
        vdisk_id = sidon_module().vdisk_id_for(name, 0)
        disk_paths_with_bus = [
            (sidon_module().nbd_socket(vdisk_id), "virtio", vdisk_id, "a")]

    sidon = sidon_module() if using_sidon() else None

    for idx, (d_path, bus, vdisk_id, dev_letter) in enumerate(disk_paths_with_bus):
        dev_prefix = "vd" if bus == "virtio" else "sd"
        if sidon is not None:
            # A network disk on a unix socket. qemu talks NBD to the local Sidon and never
            # opens a block device, so there is no /dev node to promote, demote or leak --
            # which is also why the start path attaches instead of running drbdadm.
            disk_devices_xml += sidon.disk_xml(
                vdisk_id, dev_letter, vcpu,
                boot_order=orders["disk"].get(idx))
            continue
        if bus == "virtio":
            driver_opts = f"name='qemu' type='raw' cache='none' io='native' queues='{vcpu}' iothread='1'"
        else:
            driver_opts = "name='qemu' type='raw' cache='none' io='native'"
        disk_devices_xml += f"""
    <disk type='block' device='disk'>
      <driver {driver_opts}/>
      <source dev='{d_path}'/>
      <target dev='{dev_prefix}{dev_letter}' bus='{bus}'/>
    </disk>"""

    if iso:
        cd_index = 0
        for idx, spec in enumerate(iso.split(",")):
            if spec.strip() and spec.strip() != "__empty__":
                sata_letter = letters[idx % 26]
                # The catalogue lookup that stood here only accepted a row whose stored
                # path contained "/dev/", so it stopped matching when images became Sidon
                # sockets and every call fell through to the slug anyway.
                disk_devices_xml += sidon_module().cdrom_xml(
                    sidon_module().image_vdisk_id(spec.strip()), sata_letter,
                    boot_order=orders["cdrom"].get(cd_index))
                cd_index += 1

    if cloud_init_iso:
        ci_idx = len([s for s in (iso or "").split(",") if s.strip()]) if iso else 0
        ci_letter = letters[ci_idx % 26]
        disk_devices_xml += f"""
    <disk type='file' device='cdrom'>
      <driver name='qemu' type='raw'/>
      <source file='{cloud_init_iso}'/>
      <target dev='sd{ci_letter}' bus='sata'/>
      <readonly/>
    </disk>"""

    global KVM_CACHE, VMWARE_CACHE
    has_kvm = False
    is_vmware = False
    if host_ip in KVM_CACHE and host_ip in VMWARE_CACHE:
        has_kvm = KVM_CACHE[host_ip]
        is_vmware = VMWARE_CACHE[host_ip]
    else:
        rc_caps, res_caps, _ = run_mtls_spark_api(host_ip, "/api/v1/host/capabilities", None, method="GET")
        if rc_caps == 0 and isinstance(res_caps, dict) and "kvm" in res_caps:
            has_kvm = bool(res_caps.get("kvm", False))
            virt = str(res_caps.get("virt", "")).lower()
            is_vmware = "vmware" in virt
            KVM_CACHE[host_ip] = has_kvm
            VMWARE_CACHE[host_ip] = is_vmware
        else:
            if host_ip in KVM_CACHE:
                has_kvm = KVM_CACHE[host_ip]
            else:
                try:
                    rc, _, _ = run_remote_spark(host_ip, "test -e /dev/kvm")
                    has_kvm = (rc == 0)
                    if rc >= 0:
                        KVM_CACHE[host_ip] = has_kvm
                except Exception:
                    pass

            if host_ip in VMWARE_CACHE:
                is_vmware = VMWARE_CACHE[host_ip]
            else:
                try:
                    rc, stdout, _ = run_remote_spark(host_ip, "systemd-detect-virt")
                    is_vmware = (rc == 0 and "vmware" in stdout.strip().lower())
                    if rc >= 0:
                        VMWARE_CACHE[host_ip] = is_vmware
                except Exception:
                    pass

    domain_type = "kvm" if has_kvm else "qemu"

    # With headroom the domain is defined for `max_vcpus` and starts with `vcpu` online; the
    # topology covers the maximum. Without it the two are the same and the XML is what it always was.
    max_vcpus = max(int(vcpu), int(hotplug_max_vcpus)) if hotplug_max_vcpus else int(vcpu)
    vcpu_element = (f"<vcpu placement='static' current='{vcpu}'>{max_vcpus}</vcpu>"
                    if max_vcpus != int(vcpu) else f"<vcpu placement='static'>{vcpu}</vcpu>")
    
    numa_xml = f"""    <numa>
      <cell id='0' cpus='0-{max_vcpus - 1}' memory='{memory}' unit='MiB'/>
    </numa>\n""" if hotplug_max_memory and int(hotplug_max_memory) > int(memory) else ""

    actual_cpu = cpu_model if cpu_model else ("host-model" if is_vmware else "host-passthrough")
    
    if actual_cpu == "host-passthrough" and is_vmware:
        cpu_xml = f"""<cpu mode='host-passthrough'>
    <topology sockets='1' dies='1' cores='{max_vcpus}' threads='1'/>
    <feature policy='disable' name='vmx'/>
{numa_xml}  </cpu>"""
    elif actual_cpu in ["host-model", "host-passthrough"]:
        cpu_xml = f"""<cpu mode='{actual_cpu}'>
    <topology sockets='1' dies='1' cores='{max_vcpus}' threads='1'/>
{numa_xml}  </cpu>"""
    else:
        cpu_xml = f"""<cpu mode='custom' match='exact'>
    <model>{actual_cpu}</model>
    <topology sockets='1' dies='1' cores='{max_vcpus}' threads='1'/>
{numa_xml}  </cpu>"""

    if is_vmware:
        features_xml = """<features>
    <acpi/>
    <apic/>
    <hyperv mode='custom'>
      <relaxed state='on'/>
      <vapic state='on'/>
      <spinlocks state='on' retries='8191'/>
      <reset state='on'/>
      <vendor_id state='on' value='1234567890ab'/>
    </hyperv>
    <kvm>
      <hidden state='on'/>
    </kvm>
  </features>"""
        clock_xml = """<clock offset='utc'>
    <timer name='rtc' tickpolicy='catchup'/>
    <timer name='pit' tickpolicy='delay'/>
    <timer name='hpet' present='no'/>
  </clock>"""
    else:
        features_xml = """<features>
    <acpi/>
    <apic/>
    <hyperv mode='custom'>
      <relaxed state='on'/>
      <vapic state='on'/>
      <spinlocks state='on' retries='8191'/>
      <vpindex state='on'/>
      <synic state='on'/>
      <stimer state='on'/>
      <reset state='on'/>
      <vendor_id state='on' value='1234567890ab'/>
    </hyperv>
    <kvm>
      <hidden state='on'/>
    </kvm>
  </features>"""
        clock_xml = """<clock offset='utc'>
    <timer name='hypervclock' present='yes'/>
    <timer name='rtc' tickpolicy='catchup'/>
    <timer name='pit' tickpolicy='delay'/>
    <timer name='hpet' present='no'/>
  </clock>"""

    uuid_xml = f"<uuid>{str(uuid.uuid4())}</uuid>"

    interfaces_xml = ""
    
    network_ids = []
    if network_id:
        network_id_str = str(network_id).strip()
        if network_id_str.startswith("[") and network_id_str.endswith("]"):
            try:
                network_ids = json.loads(network_id_str)
            except Exception:
                network_ids = [network_id_str]
        else:
            network_ids = [network_id_str]
    else:
        network_ids = ["7a68e0d6-11f8-4e89-9430-b3b44b8bc438"]

    for idx, net_entry in enumerate(network_ids):
        parts = net_entry.split(":")
        net_id = parts[0]
        nic_model = parts[1] if len(parts) > 1 and parts[1] else "virtio"
        if len(parts) > 2 and parts[2]:
            mac_addr = parts[2]
        else:
            import hashlib
            h = hashlib.md5(f"{name}_nic_{idx}".encode()).hexdigest()
            mac_addr = f"52:54:00:{h[0:2]}:{h[2:4]}:{h[4:6]}"
        
        nic_boot = sidon_module().boot_element(orders["nic"].get(idx))

        net = get_network_by_id(net_id)
        if net:
            if net.get("type") == "direct":
                uplink_dev = "ens192"
                # The host's default route names the uplink, and the daemon reads it
                # from `ip -j route` and answers with the interface. This used to be
                # two shell strings -- `ip route get 8.8.8.8 | grep -oP 'dev \\K\\S+'`
                # with `ip route | grep default | awk '{print $5}'` behind it when the
                # first found no -P support -- both of which reached for a field that
                # was already a key in a typed response.
                try:
                    rc_net, res_net, _ = run_mtls_spark_api(
                        host_ip, "/api/v1/host/network", None, method="GET")
                    if rc_net == 0 and str(res_net.get("default_interface") or "").strip():
                        uplink_dev = str(res_net["default_interface"]).strip()
                except Exception:
                    pass
                interfaces_xml += f"""
    <interface type='direct'>
      <mac address='{mac_addr}'/>
      <source dev='{uplink_dev}' mode='bridge'/>
      <model type='{nic_model}'/>{nic_boot}
    </interface>"""
            elif net.get("type") == "vlan" and net.get("vlan_id") is not None:
                vlan_id = net.get("vlan_id")
                interfaces_xml += f"""
    <interface type='bridge'>
      <mac address='{mac_addr}'/>
      <source bridge='br-vlan-{vlan_id}'/>
      <model type='{nic_model}'/>{nic_boot}
    </interface>"""
            elif net.get("type") == "overlay" and net.get("vni") is not None:
                vni = net.get("vni")
                interfaces_xml += f"""
    <interface type='bridge'>
      <mac address='{mac_addr}'/>
      <source bridge='br-ov-{vni}'/>
      <model type='{nic_model}'/>{nic_boot}
    </interface>"""
        else:
            interfaces_xml += f"""
    <interface type='bridge'>
      <mac address='{mac_addr}'/>
      <source bridge='virbr0'/>
      <model type='{nic_model}'/>{nic_boot}
    </interface>"""

    if audio_enabled:
        sound_xml = (
            "    <sound model='ich9'>\n"
            "      <audio id='1'/>\n"
            "    </sound>\n"
        )
    else:
        sound_xml = ""

    pci_xml = ""
    if pci_devices:
        for bdf in [b.strip() for b in str(pci_devices).split(",") if b.strip()]:
            m = re.match(r"\A(?:([0-9a-fA-F]{4}):)?([0-9a-fA-F]{2}):([0-9a-fA-F]{2})\.([0-7])\Z", bdf)
            if m:
                dom = (m.group(1) or "0000").lower()
                bus = m.group(2).lower()
                slot = m.group(3).lower()
                func = m.group(4).lower()
                pci_xml += (
                    f"    <hostdev mode='subsystem' type='pci' managed='yes'>\n"
                    f"      <source>\n"
                    f"        <address domain='0x{dom}' bus='0x{bus}' slot='0x{slot}' function='0x{func}'/>\n"
                    f"      </source>\n"
                    f"    </hostdev>\n"
                )

    usb_xml = ""
    if usb_devices:
        for usb_id in [u.strip() for u in str(usb_devices).split(",") if u.strip()]:
            m = re.match(r"\A([0-9a-fA-F]{4}):([0-9a-fA-F]{4})\Z", usb_id)
            if m:
                vendor = m.group(1).lower()
                product = m.group(2).lower()
                usb_xml += (
                    f"    <hostdev mode='subsystem' type='usb' managed='yes'>\n"
                    f"      <source>\n"
                    f"        <vendor id='0x{vendor}'/>\n"
                    f"        <product id='0x{product}'/>\n"
                    f"      </source>\n"
                    f"    </hostdev>\n"
                )

    if vtpm:
        tpm_xml = (
            "    <tpm model='tpm-crb'>\n"
            "      <backend type='emulator' version='2.0'/>\n"
            "    </tpm>\n"
        )
    else:
        tpm_xml = ""

    cputune_parts = []
    if cpu_shares is not None:
        cputune_parts.append(f"    <shares>{cpu_shares}</shares>")
    if cpu_limit is not None and int(cpu_limit) > 0:
        cputune_parts.append(f"    <period>100000</period>\n    <quota>{int(cpu_limit) * 1000}</quota>")
    cputune_xml = ("  <cputune>\n" + "\n".join(cputune_parts) + "\n  </cputune>\n") if cputune_parts else ""

    memtune_parts = []
    if mem_limit is not None and int(mem_limit) > 0:
        memtune_parts.append(f"    <hard_limit unit='MiB'>{mem_limit}</hard_limit>")
    if mem_reservation is not None and int(mem_reservation) > 0:
        memtune_parts.append(f"    <min_guarantee unit='MiB'>{mem_reservation}</min_guarantee>")
    memtune_xml = ("  <memtune>\n" + "\n".join(memtune_parts) + "\n  </memtune>\n") if memtune_parts else ""

    max_mem_xml = f"<maxMemory slots='16' unit='MiB'>{hotplug_max_memory}</maxMemory>\n  " if hotplug_max_memory and int(hotplug_max_memory) > int(memory) else ""
    vm_xml = f"""<domain type='{domain_type}'>
  <name>{name}</name>
  {uuid_xml}
  {max_mem_xml}<memory unit='MiB'>{memory}</memory>
  <currentMemory unit='MiB'>{memory}</currentMemory>
{memtune_xml}  {vcpu_element}
{cputune_xml}  <iothreads>1</iothreads>
  <os>
    {os_boot_xml}
  </os>
  {features_xml}
  {cpu_xml}
  {clock_xml}
  <devices>
    {disk_devices_xml}
    <input type='tablet' bus='usb'/>
    <serial type='pty'>
      <target port='0'/>
    </serial>
    <console type='pty'>
      <target type='serial' port='0'/>
    </console>
    {interfaces_xml}
    {graphics_xml}
    <controller type='virtio-serial' index='0'/>
    <channel type='unix'>
      <target type='virtio' name='org.qemu.guest_agent.0'/>
      <address type='virtio-serial' controller='0' bus='0' port='1'/>
    </channel>
    {video_xml}
{sound_xml}{pci_xml}{usb_xml}{tpm_xml}  </devices>
  <seclabel type='none'/>
</domain>
"""
    return vm_xml

def read_node_rows():
    """{ip: {"status", "maintenance_mode"}} from hydra.nodes, or None when it cannot be read."""
    rc, stdout, _ = run_cql_query("SELECT JSON ip, status, maintenance_mode FROM hydra.nodes;")
    if rc != 0:
        return None
    rows = {}
    for line in (stdout or "").splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                row = json.loads(line)
            except Exception:
                continue
            if row.get("ip"):
                rows[row["ip"]] = row
    return rows


def host_ineligible_reason(ip, node_rows=None):
    """Why `ip` may not be given a guest right now, or None when it may.

    One definition for every path that puts a guest on a host -- manual migration, DRS,
    evacuation and failover restart all end in a migrate or a start, and the start picks its
    host with select_best_start_host, which applies the same two tests: the host's recorded state
    is NORMAL, and it answers with every managed service UP and not in maintenance. An unreadable
    database or an unreachable host is a reason, not a pass: a guest is not sent to a host on the
    strength of not having been able to ask.
    """
    node_rows = read_node_rows() if node_rows is None else node_rows
    if node_rows is None:
        return "hydra.nodes could not be read, so its recorded state is unknown"
    row = node_rows.get(ip)
    if row is not None and (row.get("maintenance_mode") or (row.get("status") or "NORMAL") != "NORMAL"):
        return f"its recorded state is {row.get('status') or 'in maintenance'}, not NORMAL"
    rc, status, err = run_mtls_spark_api(ip, "/api/v1/node/status", None, method="GET")
    if rc != 0 or not isinstance(status, dict):
        return f"it is not answering ({err or 'no status'})"
    if status.get("maintenance_status", "NORMAL") != "NORMAL":
        return f"it is in maintenance ({status.get('maintenance_status')})"
    down = sorted(name for name, data in (status.get("services") or {}).items()
                  if (data or {}).get("status") != "UP")
    if down:
        return "these services are not up: " + ", ".join(down)
    return None


def get_memory_overcommit_policy():
    """Returns 'strict' or 'overcommit' (default) from hydra.cluster_settings."""
    try:
        rc, out, _ = run_cql_query("SELECT value FROM hydra.cluster_settings WHERE key = 'memory_overcommit_policy';")
        if rc == 0 and out:
            for line in out.splitlines():
                line = line.strip()
                if line and not line.startswith("(") and not line.startswith("-") and line != "value":
                    return line.lower()
    except Exception:
        pass
    return "overcommit"


def build_cidata_iso_bytes(vm_name, userdata, network_config=""):
    """Build a minimal ISO9660 + Rock Ridge ('cidata') seed image in memory for Cloud-Init NoCloud."""
    import struct
    SECTOR = 2048

    def both16(v):
        return struct.pack("<H", v) + struct.pack(">H", v)

    def both32(v):
        return struct.pack("<I", v) + struct.pack(">I", v)

    raw_ud = (userdata or "").strip()
    if raw_ud and not raw_ud.startswith("#cloud-config") and not raw_ud.startswith("#!"):
        raw_ud = "#cloud-config\n" + raw_ud
    if raw_ud and not raw_ud.endswith("\n"):
        raw_ud += "\n"

    meta_bytes = f"instance-id: {vm_name}\nlocal-hostname: {vm_name}\n".encode("utf-8")
    user_bytes = raw_ud.encode("utf-8")
    files = [
        ("METADATA.;1", "meta-data", meta_bytes),
        ("USERDATA.;1", "user-data", user_bytes),
    ]
    if network_config and str(network_config).strip():
        net_str = str(network_config).strip() + "\n"
        files.append(("NETCFG.;1", "network-config", net_str.encode("utf-8")))

    # Sectors 0..15: system area, 16: PVD, 17: Terminator, 18: L-Path, 19: M-Path, 20: Root Dir
    next_lba = 21
    file_entries = []
    file_sectors = bytearray()
    for iso_name, posix_name, data in files:
        lba = next_lba
        n_sec = max(1, (len(data) + SECTOR - 1) // SECTOR)
        next_lba += n_sec
        file_entries.append((iso_name, posix_name, lba, len(data)))
        padded = data + b"\x00" * (n_sec * SECTOR - len(data))
        file_sectors.extend(padded)

    total_sectors = next_lba
    date7 = bytes([126, 1, 1, 0, 0, 0, 0])  # 2026-01-01 00:00:00 UTC

    def make_dir_record(file_id_bytes, lba, length, flags, su_bytes=b""):
        fid_len = len(file_id_bytes)
        pad = b"\x00" if (fid_len % 2 == 0) else b""
        rec_len = 33 + fid_len + len(pad) + len(su_bytes)
        if rec_len % 2 == 1:
            su_bytes = su_bytes + b"\x00"
            rec_len += 1
        return (
            bytes([rec_len, 0])
            + both32(lba)
            + both32(length)
            + date7
            + bytes([flags, 0, 0])
            + both16(1)
            + bytes([fid_len])
            + file_id_bytes
            + pad
            + su_bytes
        )

    # Rock Ridge SP + ER on "." record so Linux isofs enables Rock Ridge lowercase names
    rr_sp = b"SP\x07\x01\xbe\xef\x00"
    rr_er = b"ER\x12\x01\x0a\x00\x00\x01RRIP_1991A"
    dot_rec = make_dir_record(b"\x00", 20, SECTOR, 0x02, rr_sp + rr_er)
    dotdot_rec = make_dir_record(b"\x01", 20, SECTOR, 0x02)

    root_dir = bytearray(dot_rec + dotdot_rec)
    for iso_name, posix_name, lba, size in file_entries:
        p_bytes = posix_name.encode("ascii")
        rr_nm = b"NM" + bytes([5 + len(p_bytes), 1, 0]) + p_bytes
        root_dir.extend(make_dir_record(iso_name.encode("ascii"), lba, size, 0x00, rr_nm))
    root_dir.extend(b"\x00" * (SECTOR - len(root_dir)))

    # Root directory record for PVD (34 bytes, no Rock Ridge SU)
    pvd_root_rec = make_dir_record(b"\x00", 20, SECTOR, 0x02)

    pvd = bytearray(SECTOR)
    pvd[0:7] = b"\x01CD001\x01"
    pvd[8:40] = b"LINUX".ljust(32, b" ")
    pvd[40:72] = b"cidata".ljust(32, b" ")
    pvd[80:88] = both32(total_sectors)
    pvd[120:124] = both16(1)
    pvd[124:128] = both16(1)
    pvd[128:132] = both16(SECTOR)
    pvd[132:140] = both32(10)
    pvd[140:144] = struct.pack("<I", 18)
    pvd[148:152] = struct.pack(">I", 19)
    pvd[156:190] = pvd_root_rec
    pvd[813:830] = b"2026010100000000\x00"
    pvd[830:847] = b"2026010100000000\x00"
    pvd[847:864] = b"0000000000000000\x00"
    pvd[864:881] = b"2026010100000000\x00"
    pvd[881] = 1

    term = bytearray(SECTOR)
    term[0:7] = b"\xffCD001\x01"

    lpath = bytearray(SECTOR)
    lpath[0:10] = b"\x01\x00" + struct.pack("<I", 20) + struct.pack("<H", 1) + b"\x00\x00"
    mpath = bytearray(SECTOR)
    mpath[0:10] = b"\x01\x00" + struct.pack(">I", 20) + struct.pack(">H", 1) + b"\x00\x00"

    iso_image = bytearray(16 * SECTOR)
    iso_image.extend(pvd)
    iso_image.extend(term)
    iso_image.extend(lpath)
    iso_image.extend(mpath)
    iso_image.extend(root_dir)
    iso_image.extend(file_sectors)
    return bytes(iso_image)


def check_vm_host_affinity(vm_data, candidate_ip, all_vms=None):
    """Evaluate VM-Host pinning, VM-VM anti-affinity, and VM-VM affinity for `candidate_ip`.

    Returns (allowed: bool, bonus_mb: float, reason: str).
    """
    if not vm_data or not candidate_ip:
        return True, 0.0, ""
    vm_name = vm_data.get("name", "")
    pinned_raw = str(vm_data.get("pinned_hosts") or "").strip()
    if pinned_raw:
        allowed_ips = set()
        for entry in [x.strip() for x in pinned_raw.split(",") if x.strip()]:
            resolved = get_node_ip(entry)
            if resolved:
                allowed_ips.add(resolved)
            allowed_ips.add(entry)
        if candidate_ip not in allowed_ips:
            return False, 0.0, f"VM '{vm_name}' is pinned to host(s) [{pinned_raw}], which excludes {candidate_ip}"

    aff_group = str(vm_data.get("affinity_group") or "").strip()
    anti_group = str(vm_data.get("anti_affinity_group") or "").strip()
    if not aff_group and not anti_group:
        return True, 0.0, ""

    if all_vms is None:
        all_vms = []
        rc_v, stdout_v, _ = run_cql_query(
            "SELECT JSON name, host_ip, state, affinity_group, anti_affinity_group FROM hydra.vms;")
        if rc_v == 0 and stdout_v:
            for line in stdout_v.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        all_vms.append(json.loads(line))
                    except Exception:
                        pass

    if anti_group:
        for other in all_vms:
            if other.get("name") == vm_name or other.get("state") != "Running":
                continue
            if str(other.get("anti_affinity_group") or "").strip() == anti_group and other.get("host_ip") == candidate_ip:
                return (
                    False,
                    0.0,
                    f"anti-affinity group '{anti_group}' conflict: VM '{other.get('name')}' is already running on {candidate_ip}",
                )

    bonus = 0.0
    if aff_group:
        for other in all_vms:
            if other.get("name") == vm_name or other.get("state") != "Running":
                continue
            if str(other.get("affinity_group") or "").strip() == aff_group and other.get("host_ip") == candidate_ip:
                bonus = 102400.0
                break

    return True, bonus, ""


def select_best_start_host(memory_needed, vm_data=None):
    # Query nodes in maintenance mode
    maintenance_ips = set()
    try:
        rc_n, stdout_n, _ = run_cql_query("SELECT JSON ip, status, maintenance_mode FROM hydra.nodes;")
        if rc_n == 0 and stdout_n:
            for line in stdout_n.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        node = json.loads(line)
                        if node.get("maintenance_mode", False) or node.get("status", "NORMAL") != "NORMAL":
                            maintenance_ips.add(node.get("ip"))
                    except:
                        pass
    except Exception as e:
        sys.stderr.write(f"Error querying node maintenance statuses: {e}\n")

    hosts = get_cluster_hosts()
    all_vms = None
    if vm_data and (vm_data.get("affinity_group") or vm_data.get("anti_affinity_group")):
        all_vms = []
        rc_v, stdout_v, _ = run_cql_query(
            "SELECT JSON name, host_ip, state, affinity_group, anti_affinity_group FROM hydra.vms;")
        if rc_v == 0 and stdout_v:
            for line in stdout_v.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        all_vms.append(json.loads(line))
                    except Exception:
                        pass

    best_host = None
    min_score = float('inf')
    
    for h in hosts:
        ip = h["ip"]
        if ip in maintenance_ips:
            continue
        allowed, bonus, _ = check_vm_host_affinity(vm_data, ip, all_vms=all_vms)
        if not allowed:
            continue
        # Check status and verify all services are UP
        rc_st, status_data, _ = run_mtls_spark_api(ip, "/api/v1/node/status", None, method="GET")
        if rc_st == 0:
            try:
                if status_data.get("maintenance_status", "NORMAL") != "NORMAL":
                    continue
                services = status_data.get("services", {})
                all_up = True
                for sname, sdata in services.items():
                    if sdata.get("status") != "UP":
                        all_up = False
                        break
                if not all_up:
                    continue
            except:
                continue

            _, _, total_mb, used_mb = get_node_utilization(ip, fetch_cpu=False)
            avail_mb = total_mb - used_mb
            if avail_mb >= memory_needed:
                score = used_mb - bonus
                if score < min_score:
                    min_score = score
                    best_host = ip
                    
    # Fallback to any online host if memory checks are tight
    if not best_host:
        policy = get_memory_overcommit_policy()
        if policy == "strict":
            sys.stderr.write(f"[Placement] Strict memory overcommit policy: no host has {memory_needed} MB available; refusing overcommit placement.\n")
            return None

        for h in hosts:
            ip = h["ip"]
            if ip in maintenance_ips:
                continue
            allowed, _, _ = check_vm_host_affinity(vm_data, ip, all_vms=all_vms)
            if not allowed:
                continue
            rc_st, status_data, _ = run_mtls_spark_api(ip, "/api/v1/node/status", None, method="GET")
            if rc_st == 0:
                try:
                    if status_data.get("maintenance_status", "NORMAL") != "NORMAL":
                        continue
                    services = status_data.get("services", {})
                    all_up = True
                    for sname, sdata in services.items():
                        if sdata.get("status") != "UP":
                            all_up = False
                            break
                    if all_up:
                        return ip
                except:
                    pass
                
    return best_host

def get_node_ip(host_or_ip):
    if not host_or_ip:
        return ""
    import re
    if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", host_or_ip):
        return host_or_ip
    rc, stdout, _ = run_cql_query(f"SELECT JSON ip FROM hydra.nodes WHERE hostname = '{host_or_ip}';")
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    meta = json.loads(line)
                    return meta.get("ip", "")
                except Exception:
                    pass
    return host_or_ip


def release_failed_claim(vm_name, host_ip):
    """Give back a placement claimed for a start that then failed.

    Without this the VM stays recorded as Running on a host it never booted on, and every
    later start is correctly refused because the row says someone owns it. The release is
    conditional on the claim still being ours: if something else has taken the VM in the
    meantime it is that owner's row, not ours to clear.
    """
    ok, applied, current, err = run_lwt("/v1/vm/release", {
        "name": vm_name, "expected_host_ip": host_ip,
    })
    if not ok:
        sys.stderr.write(
            f"VM {vm_name}: start failed on {host_ip} and the claim could not be released "
            f"({err}). The VM is recorded as running there and further starts will be refused.\n")
    elif not applied:
        sys.stderr.write(
            f"VM {vm_name}: start failed on {host_ip}, but the row now places it on "
            f"{current.get('host_ip')!r}, so the claim was left alone.\n")


def process_queue_task(task):
    task_id = task.get("task_id")
    vm_name = task.get("vm_name")
    action = task.get("action")
    target_host = task.get("target_host", "")
    payload = task.get("payload") or {}
    
    print(f"[Vali Queue] Processing task {task_id}: {action} on {vm_name}")

    # Reject unsafe names before they reach any shell command or CQL statement
    if vm_name or action in ["start", "stop", "poweroff", "reboot", "shutdown", "reset", "migrate"]:
        if not is_valid_object_name(vm_name):
            return False, f"Invalid VM name '{vm_name}': {OBJECT_NAME_ERROR}."
    if target_host and not is_valid_object_name(target_host):
        return False, f"Invalid target host '{target_host}': {OBJECT_NAME_ERROR}."

    vm_data = None
    if action in ["start", "stop", "poweroff", "reboot", "shutdown", "reset"]:
        vm_data = get_vm_xml_specs(vm_name)
        if not vm_data:
            return False, "VM not found in metadata database."
            
    try:
        if action == "start":
            if bool(vm_data.get("is_template", False)):
                return False, f"Refusing to start {vm_name}: it is marked as a VM template. Deploy a clone from it using 'valcli vm.template {vm_name} deploy <new_vm>'."

            # 1. Choose host if target not specified
            memory = int(vm_data.get("memory", 1024))
            vcpu = int(vm_data.get("vcpu", 1))
            firmware = vm_data.get("firmware", "uefi")
            disks_list = vm_data.get("disks_list", "")
            iso = vm_data.get("iso", "")
            boot_device = vm_data.get("boot_device", "")
            
            if target_host:
                selected_host = get_node_ip(target_host) or target_host
                allowed_aff, _, reason_aff = check_vm_host_affinity(vm_data, selected_host)
                if not allowed_aff:
                    return False, f"Refusing to start {vm_name} on {selected_host}: {reason_aff}."
            else:
                selected_host = select_best_start_host(memory, vm_data=vm_data)
            if not selected_host:
                if vm_data.get("pinned_hosts") or vm_data.get("anti_affinity_group"):
                    return False, f"No active hypervisor host satisfies memory and placement/affinity rules for {vm_name} (pinned_hosts={vm_data.get('pinned_hosts')!r}, anti_affinity_group={vm_data.get('anti_affinity_group')!r})."
                return False, "No active hypervisor host has sufficient memory."

            # 2. Claim the VM for that host before anything touches its disks.
            #
            # This used to be the last step of a start: the resources were promoted, the
            # domain defined and booted, and only then was host_ip written, unconditionally.
            # Two callers racing on the same VM -- a manual start against a DRS placement,
            # or a start issued while a stale host_ip was still in the row -- both reached
            # that point and both wrote, so two qemu processes ended up on the same raw
            # vdisk. Claiming first means one of them is turned away here.
            # Passed through as read, not coerced to "": a row whose host_ip was never
            # written holds null, and `IF host_ip = ''` does not match null. Coercing here
            # would refuse every start of such a VM as "owned by another host".
            claimed_from = vm_data.get("host_ip")
            ok, applied, current, err = run_lwt("/v1/vm/claim", {
                "name": vm_name,
                "host_ip": selected_host,
                "expected_host_ip": claimed_from,
                "state": "Running",
            })
            if not ok:
                return False, f"Refusing to start {vm_name}: its host placement could not be claimed ({err}). Starting it without the claim risks a second copy on another host."
            if not applied:
                owner = current.get("host_ip") or ""
                if owner:
                    return False, f"Refusing to start {vm_name}: another host already owns it ({owner}). Stop it there first, or start it on {owner}."
                return False, f"Refusing to start {vm_name}: its placement changed while this start was being prepared (host_ip is now {owner!r}, this start expected {claimed_from!r})."

            # 3. Compile XML (including optional Cloud-Init NoCloud seed ISO, vTPM 2.0, and Secure Boot)
            import base64
            cloud_init_iso_path = None
            ci_userdata = vm_data.get("cloud_init_userdata")
            ci_network = vm_data.get("cloud_init_network") or ""
            ci_prep_cmd = ""
            if ci_userdata and str(ci_userdata).strip():
                cloud_init_iso_path = f"/var/lib/hci/aether/nvram/{vm_name}_cidata.iso"
                ci_bytes = build_cidata_iso_bytes(vm_name, str(ci_userdata), str(ci_network))
                ci_b64 = base64.b64encode(ci_bytes).decode("utf-8")
                ci_prep_cmd = f"mkdir -p /var/lib/hci/aether/nvram && echo {ci_b64} | base64 -d > {shlex.quote(cloud_init_iso_path)} && "

            audio_enabled = bool(vm_data.get("audio_enabled", False))
            vm_xml = generate_vm_xml(vm_name, memory, vcpu, firmware, disks_list, iso, boot_device, host_ip=selected_host, network_id=vm_data.get("network_id"), cpu_model=vm_data.get("cpu_model"), audio_enabled=audio_enabled,
                                     graphics=vm_data.get("graphics"),
                                     hotplug_max_vcpus=hotplug_vcpu_limit(vcpu) if hotplug_headroom_enabled() else None,
                                     hotplug_max_memory=hotplug_memory_limit(memory) if hotplug_headroom_enabled() else None,
                                     cpu_shares=vm_data.get("cpu_shares"),
                                     cpu_limit=vm_data.get("cpu_limit"),
                                     cpu_reservation=vm_data.get("cpu_reservation"),
                                     mem_reservation=vm_data.get("mem_reservation"),
                                     mem_limit=vm_data.get("mem_limit"),
                                     pci_devices=vm_data.get("pci_devices"),
                                     usb_devices=vm_data.get("usb_devices"),
                                     vtpm=bool(vm_data.get("vtpm", False) or vm_data.get("vtpm_enabled", False)),
                                     secure_boot=bool(vm_data.get("secure_boot", False)),
                                     cloud_init_iso=cloud_init_iso_path)
            b64_xml = base64.b64encode(vm_xml.encode("utf-8")).decode("utf-8")
            
            # 4. Define and start VM via host spark-daemon
            restore_cmd = get_nvram_restore_cmd(vm_name)
            q_vm = shlex.quote(vm_name)

            # Claim the vdisks on the destination host as a separate CHECKED step before the
            # VM is defined or started. A promotion failure means the peer still holds Primary (the
            # VM is probably still running there behind a stale host_ip row), and booting anyway
            # would put two qemu processes on the same raw device.
            promoted = []
            if disks_list and disks_list != "NONE":
                disk_count = len(disks_list.split(","))
                if True:
                    # The Sidon equivalent of promotion, and a stronger one. Attaching wins
                    # the ownership compare-and-swap in Hydra and fences every journal
                    # replica at the new epoch, so a previous owner that is wedged, lying,
                    # or unreachable cannot complete another write -- where drbdadm primary
                    # can only *infer* that the peer stopped, and only where quorum is
                    # armed. A refused claim names the host that actually holds the disk.
                    module = sidon_module()
                    for idx in range(disk_count):
                        vdisk_id = module.vdisk_id_for(vm_name, idx)
                        rc_a, body_a, err_a = run_mtls_spark_api(
                            selected_host, "/api/v1/dfs/vdisk",
                            {"op": "attach", "vdisk_id": vdisk_id})
                        if rc_a != 0:
                            for done in promoted:
                                run_mtls_spark_api(selected_host, "/api/v1/dfs/vdisk",
                                                   {"op": "detach", "vdisk_id": done})
                            release_failed_claim(vm_name, selected_host)
                            detail = ""
                            if isinstance(body_a, dict):
                                detail = body_a.get("error") or ""
                            return False, f"Refusing to start {vm_name}: Sidon would not hand {vdisk_id} to {selected_host} ({detail or err_a}). Another host may still own the disk."
                        promoted.append(vdisk_id)

            # Attach the images the domain boots from, for the same reason the data disks
            # are attached: the XML names a socket, and a vdisk that is not attached has
            # no socket. Sealing an image detaches it -- deliberately, an immutable vdisk
            # should not hold the writer's attachment -- so an image is *always* detached
            # at rest, and nothing here used to re-attach it. Every VM with an ISO failed
            # to start with "Cannot access storage file .../img-<slug>.sock", which reads
            # like a missing file rather than an unattached disk.
            if iso:
                module = sidon_module()
                for spec in iso.split(","):
                    spec = spec.strip()
                    if not spec or spec == "__empty__":
                        continue
                    img_id = module.image_vdisk_id(spec)
                    rc_i, body_i, err_i = run_mtls_spark_api(
                        selected_host, "/api/v1/dfs/vdisk",
                        {"op": "attach", "vdisk_id": img_id})
                    if rc_i != 0:
                        # The data disks are this VM's alone, so they are handed back. The
                        # images are not: an immutable image is attached read-only and may
                        # be serving another guest's CD-ROM right now, and attach is
                        # idempotent precisely so that sharing works. Detaching one here to
                        # tidy up after a failure would eject a disc from a VM that is
                        # running fine.
                        for done in promoted:
                            run_mtls_spark_api(selected_host, "/api/v1/dfs/vdisk",
                                               {"op": "detach", "vdisk_id": done})
                        release_failed_claim(vm_name, selected_host)
                        detail = ""
                        if isinstance(body_i, dict):
                            detail = body_i.get("error") or ""
                        return False, (
                            f"Refusing to start {vm_name}: its image {spec} could not be "
                            f"attached on {selected_host} ({detail or err_i}). The guest "
                            f"would boot with no install medium.")
            restore_cmd = f"{ci_prep_cmd}{restore_cmd}"
            cmd = f"{restore_cmd} && virsh -c qemu:///system undefine {q_vm} --keep-nvram || true; echo {b64_xml} | base64 -d > /tmp/{q_vm}.xml && virsh -c qemu:///system define /tmp/{q_vm}.xml && rm /tmp/{q_vm}.xml && virsh -c qemu:///system start {q_vm}"
            rc, stdout, stderr = run_remote_spark(selected_host, cmd)
            if rc != 0:
                release_failed_claim(vm_name, selected_host)
                return False, f"Failed to execute start on target host {selected_host}: {stderr.strip() or stdout.strip()}"

            # The record already says Running on selected_host: step 2 wrote it as part of
            # the claim, which is the only way the write and the decision can be one event.
            return True, selected_host

        elif action in ["stop", "poweroff"]:
            host_ip = vm_data.get("host_ip", "")
            if not host_ip:
                # Already unplaced. Still conditional, so a start that claimed the VM
                # between this task's read and this write is not quietly undone. The value
                # goes back as read -- "" and null are both unplaced, and `IF host_ip = ''`
                # does not match a null.
                ok, applied, current, err = run_lwt("/v1/vm/release", {
                    "name": vm_name, "expected_host_ip": host_ip,
                })
                if not ok:
                    return False, f"Could not mark {vm_name} stopped: {err}"
                if not applied:
                    return False, f"Not stopping {vm_name}: it was placed on {current.get('host_ip') or 'another host'} while this stop was being prepared. Re-issue the stop."
                return True, ""

            # Run destroy/undefine and backup NVRAM
            backup_cmd = get_nvram_backup_cmd(vm_name, delete_local=True)
            q_vm = shlex.quote(vm_name)
            cmd = f"virsh -c qemu:///system destroy {q_vm} || true && virsh -c qemu:///system undefine {q_vm} --keep-nvram || true && {backup_cmd}"
            rc_stop, _out_stop, err_stop = run_remote_spark(host_ip, cmd)
            if rc_stop == -1:
                # The host did not answer, so nothing is known about the guest. Releasing the
                # placement now would let the next start attach and fence its disks while qemu
                # may still be writing to them.
                return False, (f"{vm_name} could not be stopped: {host_ip} did not answer "
                               f"({err_stop}). The placement was left as it is.")

            # Releasing the placement is what lets the VM be started somewhere else, so it
            # is conditional on this task still being the one that owns it. An
            # unconditional release here frees the row of a VM that a concurrent migration
            # had already moved -- and the next start then boots a second copy of a guest
            # that is still running on the new host.
            ok, applied, current, err = run_lwt("/v1/vm/release", {
                "name": vm_name, "expected_host_ip": host_ip,
            })
            if not ok:
                return False, f"{vm_name} was destroyed on {host_ip} but its placement could not be released: {err}"
            if not applied:
                return False, f"{vm_name} was destroyed on {host_ip}, but Hydra now places it on {current.get('host_ip')!r}, so this stop did not release it. Check whether it is running there."
            return True, ""

        elif action in ["reboot", "shutdown", "reset"]:
            host_ip = vm_data.get("host_ip", "")
            if not host_ip:
                return False, "VM is not running."
            rc, res_pow, err_pow = run_mtls_spark_api(
                host_ip, f"/api/v1/vm/{urllib.parse.quote(vm_name)}/power", {"action": action})
            if rc == 0:
                return True, host_ip
            # Fallback to shell if running against an older daemon
            cmd = f"virsh -c qemu:///system {action} {shlex.quote(vm_name)}"
            rc, stdout, stderr = run_remote_spark(host_ip, cmd)
            if rc != 0:
                return False, f"Failed to execute {action} on host: {err_pow or stderr.strip()}"
            return True, host_ip
            
        elif action == "migrate":
            # Target IP must be specified
            if not target_host:
                return False, "Migration target host not specified."
            
            target_ip = get_node_ip(target_host)
            if not target_ip:
                return False, f"Could not resolve target host {target_host} to IP address."
            if not is_valid_object_name(target_ip):
                return False, f"Invalid target host address '{target_ip}': {OBJECT_NAME_ERROR}."

            vm_data = get_vm_xml_specs(vm_name)
            if not vm_data:
                return False, "VM not found in metadata database."
            src_host = vm_data.get("host_ip", "")
            # The power state lives in `state`. This check used to read the `status`
            # column, which is the migration lock and is null until a VM has migrated at
            # least once -- so the comparison raised AttributeError on None and every
            # first migration of a VM failed with "'NoneType' object has no attribute
            # 'lower'" reported as the task error.
            power_state = vm_data.get("state") or ""

            if not src_host or power_state.lower() != "running":
                return False, f"VM must be running to migrate (current state: {power_state or 'unknown'}, host: {src_host or 'unplaced'})."

            if vm_data.get("pci_devices") and str(vm_data.get("pci_devices")).strip():
                return False, f"Refusing to migrate {vm_name}: physical PCI passthrough devices ({vm_data.get('pci_devices')}) are attached. Detach PCI devices before live migration."
            if vm_data.get("usb_devices") and str(vm_data.get("usb_devices")).strip():
                return False, f"Refusing to migrate {vm_name}: physical USB passthrough devices ({vm_data.get('usb_devices')}) are attached. Detach USB devices before live migration."

            allowed_aff, _, reason_aff = check_vm_host_affinity(vm_data, target_ip)
            if not allowed_aff:
                return False, f"Refusing to migrate {vm_name} to {target_host}: {reason_aff}."

            # There is no read of `status` here on purpose: whether the VM is already
            # migrating is decided by the lock below, in the same operation that takes it.

            if src_host == target_ip:
                return True, target_ip
                
            # The same test every other way of placing a guest uses (docs/vali.md, "One path for
            # moving a guest"): the host's recorded state must be NORMAL -- not DEGRADED,
            # FENCED, DOWN, RECOVERING or any maintenance state, which is what a quarantine
            # and a failover write -- and it must answer with every service up. This used to
            # read only the daemon's own maintenance flag, so a migration could be aimed at a
            # host that had quarantined or fenced itself.
            why_not = host_ineligible_reason(target_ip)
            if why_not:
                return False, f"Target host {target_host} cannot take guests: {why_not}"

            # Memory availability check on target host
            memory_needed = int(vm_data.get("memory") or 1024)
            _, _, total_mb, used_mb = get_node_utilization(target_ip, fetch_cpu=False)
            avail_mb = total_mb - used_mb
            if avail_mb < memory_needed:
                return False, f"Target host {target_host} has insufficient free memory ({int(avail_mb)} MB available, needs {memory_needed} MB)."

            # Storage capacity gate. Either input being unknown refuses the migration
            # rather than waving it through: both used to fall back to a constant, and
            # the free-space constant was large enough that the gate never refused
            # anything at all.
            disk_size = get_vm_disk_size(vm_name)
            if disk_size is None:
                return False, (f"Refusing to migrate {vm_name}: its disk size could not be "
                               f"determined, so there is no way to tell whether "
                               f"{target_host} can hold it.")
            target_free = get_storage_free_space(target_ip)
            if target_free is None:
                return False, (f"Refusing to migrate {vm_name}: the free storage on "
                               f"{target_host} could not be read, so the capacity check "
                               f"cannot be made.")
            if target_free < disk_size:
                return False, f"Target host {target_host} has insufficient free extent-store space ({target_free} MiB available, needs {disk_size} MiB)."

            # CPU / accelerator compatibility check
            try:
                rc_caps, caps_data, _ = run_mtls_spark_api(target_ip, "/api/v1/host/capabilities", None, method="GET")
                rc_src_caps, src_caps_data, _ = run_mtls_spark_api(src_host, "/api/v1/host/capabilities", None, method="GET")
                if rc_caps == 0 and isinstance(caps_data, dict) and rc_src_caps == 0 and isinstance(src_caps_data, dict):
                    if src_caps_data.get("kvm", False) and not caps_data.get("kvm", False):
                        return False, f"Target host {target_host} lacks KVM hardware acceleration (/dev/kvm), incompatible with source host {src_host}."
            except Exception as e:
                sys.stderr.write(f"[Migration] Capability pre-flight check error: {e}\n")

            # Take the migration lock. The condition and the write are one Paxos round, so
            # there is no window between them: previously the code read `status`,
            # decided nobody was migrating, and set it in a separate statement, which two
            # callers could both pass. What that used to risk was disk corruption, since
            # live migration was the window in which DRBD dual-primary was open. What it
            # risks now is two migrations racing each other's ownership CAS and leaving
            # the VM defined on a host that does not own its disk.
            print(f"Taking the migration lock for VM '{vm_name}'...")
            ok_h, mig_token, cur_h, err_h = acquire_migration_holder_lock(
                vm_name,
                holder=f"{src_host}->{target_ip}",
                reason=f"task {task_id or 'migrate'}",
            )
            if not ok_h:
                return False, f"Refusing to migrate {vm_name}: the migration holder lease could not be taken ({err_h})."
            if not mig_token:
                return False, f"Refusing to migrate {vm_name}: another migration lease is active (holder = {cur_h.get('holder')!r})."

            ok, applied, current, err = run_lwt("/v1/vm/migrate-lock", {"name": vm_name})
            if not ok:
                release_migration_holder_lock(vm_name, mig_token)
                return False, f"Refusing to migrate {vm_name}: the migration lock could not be taken ({err}). Without it a concurrent migration of the same VM would not be detected."
            if not applied:
                release_migration_holder_lock(vm_name, mig_token)
                return False, f"Refusing to migrate {vm_name}: it is already migrating (status = {current.get('status')!r})."

            try:
                q_vm = shlex.quote(vm_name)

                # 1. Force-backup NVRAM on source host to ScyllaDB
                rc_nb, _, _ = run_mtls_spark_api(
                    src_host, f"/api/v1/vm/{urllib.parse.quote(vm_name)}/nvram", {"action": "backup"}
                )
                if rc_nb != 0:
                    backup_cmd = get_nvram_backup_cmd(vm_name, delete_local=False)
                    run_remote_spark(src_host, backup_cmd)

                # 2. Restore NVRAM on target host (ensuring directories exist and files are written)
                rc_nr, _, _ = run_mtls_spark_api(
                    target_ip, f"/api/v1/vm/{urllib.parse.quote(vm_name)}/nvram", {"action": "restore"}
                )
                if rc_nr != 0:
                    restore_cmd = get_nvram_restore_cmd(vm_name)
                    run_remote_spark(target_ip, restore_cmd)

                # 3. Pre-clean stale definition on target host
                rc_un, _, _ = run_mtls_spark_api(
                    target_ip, "/api/v1/vm/undefine", {"name": vm_name, "keep_nvram": True}
                )
                if rc_un != 0:
                    run_remote_spark(target_ip, f"virsh -c qemu:///system undefine {q_vm} --keep-nvram || true")

                # 3b. Attach the guest's disks on the target. The domain XML names an NBD
                # socket per disk and the destination qemu opens it as the migration starts;
                # without this step it fails with "Failed to connect ... No such file or
                # directory". The start and HA paths attach before defining the domain, and
                # this path never did. Data disks are attached in forwarding mode so the
                # writer stays the source until `takeover` below.
                data_ids = domain_vdisk_ids(vm_name, vm_data.get("disks_list", ""))
                image_ids = domain_image_ids(vm_data.get("iso", ""))
                try:
                    destination_forwarders = prepare_destination_storage(target_ip, data_ids, image_ids)
                except MigrationStepError as step_err:
                    raise Exception(
                        f"{step_err}. Nothing changed on {src_host}: it still owns every disk "
                        f"and the guest keeps running there.")

                # 4. Live migrate command
                # --unsafe was needed when VM disks carried --allow-two-primaries permanently:
                # libvirt refuses a live migration it believes is cache-incoherent. The
                # two-primaries window is now opened and closed around the migration
                # itself, so the check libvirt performs is the one we actually want, and
                # suppressing it would only hide a genuinely unsafe migration.
                cmd = f"virsh -c qemu:///system migrate --live --persistent --undefinesource --listen-address {target_ip} {q_vm} qemu+ssh://root@{target_ip}/system"
                rc, stdout, stderr = run_remote_spark(src_host, cmd, timeout=MIGRATION_COMMAND_TIMEOUT)
                if rc != 0:
                    leftovers = detach_destination_forwarders(target_ip, destination_forwarders)
                    note = (f" The forwarders on {target_ip} could not all be removed ({'; '.join(leftovers)})."
                            if leftovers else
                            f" The forwarders on {target_ip} were removed.")
                    raise Exception(
                        f"Migration command failed at step 'virsh migrate': "
                        f"{stderr.strip() or stdout.strip()}. {src_host} still owns every disk "
                        f"and no epoch moved.{note}")

                # 5. Clean up local NVRAM file on source host after successful migration
                rc_nv, _, _ = run_mtls_spark_api(src_host, f"/api/v1/vm/{urllib.parse.quote(vm_name)}/nvram", {"action": "clean"})
                if rc_nv != 0:
                    run_remote_spark(src_host, f"rm -f /var/lib/hci/aether/nvram/{q_vm}_vars.fd")

                # Hand the placement over and drop the lock in one operation, conditional
                # on both the source host and the lock still being ours. Two statements
                # would leave a window in which the VM is recorded on the target with the
                # lock still held, or unlocked while still recorded on the source -- and a
                # start arriving in that window picks the wrong host.
                ok, applied, current, err = run_lwt("/v1/vm/migrate-commit", {
                    "name": vm_name,
                    "host_ip": target_ip,
                    "expected_host_ip": src_host,
                })
                release_migration_holder_lock(vm_name, mig_token)
                if not ok or not applied:
                    detail = err if not ok else (
                        f"the row moved underneath the migration (host_ip = "
                        f"{current.get('host_ip')!r}, status = {current.get('status')!r})")
                    # The guest is already on the target: this is a record that no longer
                    # matches reality, not a migration that failed, and saying so is what
                    # stops an operator from "recovering" a VM that is running fine.
                    run_lwt("/v1/vm/migrate-unlock", {"name": vm_name})
                    return False, (
                        f"{vm_name} migrated to {target_ip}, but its record could not be updated: {detail}. "
                        f"The guest is running on {target_ip} while Hydra still places it on {src_host}; "
                        f"repair the row before starting or stopping this VM.")

                # The guest is running on the target, and its I/O is being relayed to the
                # source. Make the target the owner of each disk (docs/dfs/ownership.md
                # section 5). Retried inside; a failure here leaves the guest running and
                # its disks served through the forwarder, and is reported as exactly that.
                handover_failures = hand_over_storage(target_ip, data_ids)

                # Insert record in history
                now = int(time.time() * 1000)
                reason = task.get("error_msg", "Manual VM migration request") # Re-use error_msg for trigger reason
                clean_reason = reason.replace("'", "''")
                cql_history = f"""
                INSERT INTO hydra.vali_drs_history (event_time, vm_name, source_host, target_host, reason)
                VALUES ({now}, '{vm_name}', '{src_host}', '{target_ip}', '{clean_reason}');
                """
                run_cql_query(cql_history)
                if handover_failures:
                    named = "; ".join(f"{v}: {d}" for v, d in handover_failures)
                    return False, (
                        f"{vm_name} migrated to {target_ip} and is running there, but the storage "
                        f"handover did not finish at step 'takeover' ({named}). Its disks are still "
                        f"served by the previous owner through {target_ip}; run the takeover again "
                        f"(it is safe to repeat) with: valcli storage.takeover <vdisk> {target_ip}")
                return True, target_ip
            except Exception as migrate_err:
                print(f"Migration error caught: {migrate_err}. Releasing the migration lock...")
                # Verify that this attempt still holds the migration holder token before
                # unlocking `hydra.vms.status`, so a stale cleanup cannot drop a newer
                # migration's lock after the first lease expired.
                ok_tok, applied_tok, _cur_tok, _err_tok = release_migration_holder_lock(vm_name, mig_token)
                if ok_tok and not applied_tok:
                    sys.stderr.write(
                        f"VM {vm_name}: migration failed, but its holder token was no longer active; "
                        f"leaving the VM migration status lock untouched.\n"
                    )
                    return False, f"Migration failed: {str(migrate_err)}"
                ok_u, applied_u, current_u, err_u = run_lwt("/v1/vm/migrate-unlock", {"name": vm_name})
                if not ok_u:
                    sys.stderr.write(f"VM {vm_name}: migration failed and the lock could not be released ({err_u}). Further migrations will be refused until it is cleared.\n")
                elif not applied_u:
                    sys.stderr.write(f"VM {vm_name}: migration failed, but the lock is no longer this migration's to release (status = {current_u.get('status')!r}); left alone.\n")
                return False, f"Migration failed: {str(migrate_err)}"

        elif action == "live_change":
            return run_live_change(vm_name, payload)

        elif action == "host_maintenance_enter":
            hostname = payload.get("hostname")
            target_ip = payload.get("target_ip")
            force_stop = payload.get("force_stop", False)
            preserve_vms = bool(payload.get("preserve_vms", False))
            # Taken by the API handler before this task was submitted; renewed below for
            # as long as the evacuation runs, and given back on every failure path.
            lock_token = payload.get("lock_token", "")

            if not is_valid_object_name(hostname) or not is_valid_object_name(target_ip):
                release_maintenance_lock(lock_token)
                return False, "Invalid payload parameters for host_maintenance_enter."

            if preserve_vms:
                # Service maintenance: running VMs remain on the host with virtqemud running.
                # Only storage and cluster management services are serviced/restarted.
                print(f"[Maintenance Catalyst Task] Service maintenance requested for host {hostname} ({target_ip}); preserving running VMs.")
                rc_v, stdout_v, _ = run_cql_query("SELECT JSON name, host_ip, state, disks_list FROM hydra.vms;")
                vms = []
                if rc_v == 0 and stdout_v:
                    for line in stdout_v.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                vms.append(json.loads(line))
                            except:
                                pass
                running_vms_preserved = [v for v in vms if v.get("state") == "Running" and v.get("host_ip") == target_ip]
                if running_vms_preserved:
                    # Nutanix Storage Autopathing: find a healthy peer storage node to take over
                    # ownership before local Sidon is stopped.
                    cluster_hosts = get_cluster_hosts()
                    peer_ip = None
                    for h in cluster_hosts:
                        hip = h.get("ip")
                        if hip and hip != target_ip:
                            # Verify peer is responsive and not in maintenance
                            rc_st, status_st, _ = run_mtls_spark_api(hip, "/api/v1/node/status", None, method="GET")
                            if rc_st == 0 and isinstance(status_st, dict):
                                if status_st.get("maintenance_status", "NORMAL") == "NORMAL":
                                    peer_ip = hip
                                    break
                    if peer_ip:
                        print(f"[Maintenance Catalyst Task] Autopathing: transferring disk ownership for {len(running_vms_preserved)} preserved VMs to peer {peer_ip}...")
                        for v in running_vms_preserved:
                            v_name = v.get("name")
                            v_disks = domain_vdisk_ids(v_name, v.get("disks_list", ""))
                            for vd in v_disks:
                                # Ensure peer has disk attached and can serve requests
                                run_mtls_spark_api(peer_ip, "/api/v1/dfs/vdisk", {"op": "attach", "vdisk_id": vd})
                                # Instruct peer to takeover epoch ownership
                                ok_to, _, err_to = sidon_op(peer_ip, "takeover", vd, timeout=SIDON_TAKEOVER_TIMEOUT)
                                if not ok_to:
                                    print(f"[Maintenance Catalyst Task] Autopathing warning: peer {peer_ip} takeover of {vd} for {v_name}: {err_to}")
                running_vms = []
            else:
                # Perform VM evacuation
                rc_v, stdout_v, _ = run_cql_query("SELECT JSON name, host_ip, state, memory FROM hydra.vms;")
                vms = []
                if rc_v == 0 and stdout_v:
                    for line in stdout_v.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                vms.append(json.loads(line))
                            except:
                                pass
                
                # Filter VMs that are running on target_ip
                running_vms = [v for v in vms if v.get("state") == "Running" and v.get("host_ip") == target_ip]
            
            success = True
            failed_vm = ""
            fail_reason = ""
            
            if running_vms:
                print(f"[Maintenance Catalyst Task] Evacuating {len(running_vms)} VMs from host {hostname} ({target_ip})...")
                for i, vm in enumerate(running_vms):
                    vm_name = vm.get("name")
                    vm_mem = int(vm.get("memory", 1024))
                    
                    # Select best target host excluding this one
                    dest_host = select_best_start_host(vm_mem)
                    if not dest_host:
                        if force_stop:
                            print(f"[Maintenance Catalyst Task] No migration target host for {vm_name}. Force stopping VM.")
                            stop_success, stop_err, _ = submit_and_wait_task("vali", "stop", {"vm_name": vm_name}, timeout_polls=POWER_TIMEOUT_POLLS, parent_task_id=task_id)
                            if not stop_success:
                                print(f"[Maintenance Catalyst Task] Force stopping failed for {vm_name}: {stop_err}")
                                success = False
                                failed_vm = vm_name
                                fail_reason = f"Force stop failed: {stop_err}"
                                break
                        else:
                            success = False
                            failed_vm = vm_name
                            fail_reason = "No active hypervisor host has sufficient memory."
                            break
                    else:
                        # Submit a migrate task to Catalyst and wait for it
                        task_success, task_err, _ = submit_and_wait_task("vali", "migrate", {"vm_name": vm_name, "target_host": dest_host}, timeout_polls=MIGRATE_TIMEOUT_POLLS, parent_task_id=task_id)
                        
                        if not task_success:
                            if force_stop:
                                print(f"[Maintenance Catalyst Task] Migration failed for {vm_name}: {task_err}. Force stopping VM.")
                                stop_success, stop_err, _ = submit_and_wait_task("vali", "stop", {"vm_name": vm_name}, timeout_polls=POWER_TIMEOUT_POLLS, parent_task_id=task_id)
                                if not stop_success:
                                    print(f"[Maintenance Catalyst Task] Force stopping failed for {vm_name}: {stop_err}")
                            else:
                                success = False
                                failed_vm = vm_name
                                fail_reason = f"Migration failed: {task_err}"
                                break
                            
                    # Update Catalyst task progress
                    progress = int(10 + (i + 1) / len(running_vms) * 80)
                    call_catalyst_api("/api/v1/tasks/update", {
                        "task_id": task_id,
                        "status": "processing",
                        "progress": progress
                    }, method="POST")

                    # Evacuating a host can run for many minutes. Renewing here is what
                    # lets the lock's TTL stay short enough that a node dying mid-drain
                    # frees it quickly.
                    renew_maintenance_lock(hostname, lock_token, f"evacuating {hostname}")

            if success:
                # The last thing before the database goes down. The gate ran once already
                # in the API handler, but the ring it saw was the ring before the
                # evacuation -- which may have taken an hour, and during which another
                # node can have died. This is the check that actually protects quorum;
                # the earlier one only avoids a pointless evacuation.
                quorum_ok, quorum_reason = check_stop_preserves_quorum(target_ip)
                if not quorum_ok:
                    cql = f"UPDATE hydra.nodes SET status = 'NORMAL', maintenance_mode = false WHERE hostname = '{hostname}';"
                    run_cql_query(cql)
                    release_maintenance_lock(lock_token)
                    print(f"[Maintenance Catalyst Task] REFUSED to stop services on {hostname}: {quorum_reason}")
                    if preserve_vms:
                        print(f"[Maintenance Catalyst Task] Host {hostname} status reverted to NORMAL.")
                    else:
                        print(f"[Maintenance Catalyst Task] Host {hostname} status reverted to NORMAL. Its VMs have already been evacuated; DRS will rebalance them.")
                    return False, f"Refused to enter maintenance mode: {quorum_reason}"

                zk_ok, zk_reason = check_zk_stop_preserves_quorum(target_ip)
                if not zk_ok:
                    cql = f"UPDATE hydra.nodes SET status = 'NORMAL', maintenance_mode = false WHERE hostname = '{hostname}';"
                    run_cql_query(cql)
                    release_maintenance_lock(lock_token)
                    print(f"[Maintenance Catalyst Task] REFUSED to stop services on {hostname}: {zk_reason}")
                    if preserve_vms:
                        print(f"[Maintenance Catalyst Task] Host {hostname} status reverted to NORMAL.")
                    else:
                        print(f"[Maintenance Catalyst Task] Host {hostname} status reverted to NORMAL. Its VMs have already been evacuated; DRS will rebalance them.")
                    return False, f"Refused to enter maintenance mode: {zk_reason}"

                target_status = 'SERVICE_MAINTENANCE' if preserve_vms else 'IN_MAINTENANCE'
                cql = f"UPDATE hydra.nodes SET status = '{target_status}', maintenance_mode = true WHERE hostname = '{hostname}';"
                run_cql_query(cql)
                print(f"[Maintenance Catalyst Task] Host {hostname} successfully entered {target_status} mode. {quorum_reason}")

                # The lock stays held for as long as the host is in maintenance, and is
                # renewed from here on by Mipha's control loop -- this task is about to
                # end, and a lock nobody renews would expire five minutes into a
                # maintenance window that lasts hours.
                renew_maintenance_lock(hostname, lock_token, f"{hostname} is in maintenance")

                # The state file is what the host's own daemons read: spark's loop ignores the
                # desired state while it exists, Mipha exempts the host, and a reboot comes back
                # into maintenance. If it cannot be written nothing has been stopped yet, so the
                # entry is abandoned and the host put back, instead of recorded in the database
                # as in maintenance while the host does not know it.
                marker_reason = "SERVICE_MAINTENANCE" if preserve_vms else f"maintenance on {hostname}"
                rc_state, _, err_state = run_mtls_spark_api(
                    target_ip, "/api/v1/host/maintenance-state", {"state": "ENTER", "reason": marker_reason})
                if rc_state != 0:
                    rc_state, _, err_state = run_remote_spark(
                        target_ip, f"mkdir -p /etc/hci && echo '{marker_reason}' > /etc/hci/maintenance.state")
                if rc_state != 0:
                    run_cql_query(f"UPDATE hydra.nodes SET status = 'NORMAL', maintenance_mode = false WHERE hostname = '{hostname}';")
                    release_maintenance_lock(lock_token)
                    print(f"[Maintenance Catalyst Task] Could not write the maintenance marker on {hostname}: {err_state}. Host status reverted to NORMAL.")
                    undo_detail = "Running VMs were preserved." if preserve_vms else "Its VMs have already been evacuated; DRS will rebalance them."
                    return False, (f"Could not enter maintenance mode: the marker file could not be "
                                   f"written on {hostname} ({err_state.strip() or 'no detail'}). {undo_detail}")
                # What maintenance stops: every managed unit that is not one of the ones kept
                # up on purpose (docs/maintenance.md). The same list spark's autostart stops
                # after a reboot in maintenance; test_maintenance_flow checks they agree.
                maintenance_units = list(MAINTENANCE_STOP_UNITS)

                # Update task progress to 100 before running the stop command, so the task status is marked completed in ScyllaDB
                call_catalyst_api("/api/v1/tasks/update", {
                    "task_id": task_id,
                    "status": "completed",
                    "progress": 100
                }, method="POST")

                # Detached, the way the shell form was a backgrounded subshell: vali
                # is in the list, so waiting for the stop to finish means waiting for
                # a reply from a service the stop has already killed.
                ok, detail = spark_unit_action(target_ip, "stop", maintenance_units,
                                               detach=True)
                if not ok:
                    print(f"[Maintenance Catalyst Task] Could not stop services on "
                          f"{hostname}: {detail}")
                    return False, (
                        f"{hostname} is recorded as in maintenance and its VMs are evacuated, but "
                        f"its services could not be stopped ({detail}). `cluster status` marks what "
                        f"is still up that should not be. Run 'valcli host.maintenance.leave "
                        f"{hostname}' to undo it.")
                return True, target_ip
            else:
                # Revert node status to NORMAL
                cql = f"UPDATE hydra.nodes SET status = 'NORMAL', maintenance_mode = false WHERE hostname = '{hostname}';"
                run_cql_query(cql)
                release_maintenance_lock(lock_token)
                print(f"[Maintenance Catalyst Task] Evacuation failed for host {hostname} on VM {failed_vm}: {fail_reason}. Host status reverted to NORMAL.")
                return False, f"Evacuation failed on VM {failed_vm}: {fail_reason}"

        elif action == "host_maintenance_leave":
            hostname = payload.get("hostname")
            target_ip = payload.get("target_ip")
            
            if not is_valid_object_name(hostname) or not is_valid_object_name(target_ip):
                return False, "Invalid payload parameters for host_maintenance_leave."
                
            # Remove state file and start services on the target host
            rc_leave, _, _ = run_mtls_spark_api(target_ip, "/api/v1/host/maintenance-state", {"state": "LEAVE"})
            if rc_leave != 0:
                run_remote_spark(target_ip, "rm -f /etc/hci/maintenance.state")
            
            # Update task progress
            call_catalyst_api("/api/v1/tasks/update", {
                "task_id": task_id,
                "status": "processing",
                "progress": 15
            }, method="POST")
            
            print(f"[Maintenance Catalyst Task] Starting services on host {hostname}...")
            start_units = list(MAINTENANCE_EXIT_START_UNITS)
            ok, detail = spark_unit_action(target_ip, "start", start_units)
            if not ok:
                print(f"[Maintenance Catalyst Task] Could not start services on "
                      f"{hostname}: {detail}")

            # Verify ZooKeeper has rejoined the ensemble circle
            print(f"[Maintenance Catalyst Task] Verifying ZooKeeper on {hostname} ({target_ip}) rejoins ensemble...")
            zk_rejoined, zk_detail = wait_zookeeper_rejoined(target_ip, timeout=30)
            if zk_rejoined:
                print(f"[Maintenance Catalyst Task] ZooKeeper on {hostname} successfully rejoined ensemble (mode: {zk_detail}).")
            else:
                print(f"[Maintenance Catalyst Task] Warning: {zk_detail}")

            # Update task progress
            call_catalyst_api("/api/v1/tasks/update", {
                "task_id": task_id,
                "status": "processing",
                "progress": 30
            }, method="POST")
            
            # Wait for services on the leaving node to bootstrap and stabilize
            print(f"[Maintenance Catalyst Task] Waiting 10 seconds for services on host {hostname} to stabilize...")
            time.sleep(10)
            
            # RECOVERING, not NORMAL yet: the host is not schedulable until its services are
            # answering. Nothing used to take it from here to NORMAL -- the lock was released
            # at this point and the row stayed RECOVERING for ever, which is what a rolling
            # upgrade waiting for NORMAL after its leave timed out on.
            cql_up = f"UPDATE hydra.nodes SET status = 'RECOVERING', maintenance_mode = false WHERE hostname = '{hostname}';"
            run_cql_query(cql_up)

            # There is no storage resync to wait for (extent groups are immutable and Purah
            # replaces absent copies in the background), but there are services to wait for.
            # NORMAL, and the lock, come when the host answers with every service up; if it
            # does not, the host stays RECOVERING with the lock held (Mipha renews it) and
            # this task fails naming what is down. Running leave again retries from here.
            back, why = finish_maintenance_exit(hostname, target_ip)
            if not back:
                return False, why

            # Autopathing Reclaim: Spawn subtask to redirect storage traffic / reclaim local storage ownership
            print(f"[Maintenance Catalyst Task] Spawning storage redirection subtask for host {hostname}...")
            submit_and_wait_task("vali", "reclaim_storage", {
                "hostname": hostname,
                "target_ip": target_ip
            }, timeout_polls=60, parent_task_id=task_id)

            # Spawn subtask to run Mimir Health Check
            print(f"[Maintenance Catalyst Task] Spawning Mimir health checks subtask for host {hostname}...")
            submit_and_wait_task("dagur", "execute", {
                "job_name": "Mimir Health Check",
                "command": "/usr/local/bin/mcli health_checks run_all"
            }, timeout_polls=60, parent_task_id=task_id)

            # Spawn subtask to run DRS Rebalance
            print(f"[Maintenance Catalyst Task] Spawning DRS rebalance subtask for host {hostname}...")
            submit_and_wait_task("vali", "balance", {
                "aggressive": False
            }, timeout_polls=60, parent_task_id=task_id)
            
            return True, target_ip

        elif action == "reclaim_storage":
            hostname = payload.get("hostname")
            target_ip = payload.get("target_ip")
            if not target_ip and hostname:
                target_ip = get_node_ip(hostname)

            if not hostname or not target_ip:
                return False, "Invalid payload parameters for reclaim_storage."

            call_catalyst_api("/api/v1/tasks/update", {
                "task_id": task_id,
                "status": "processing",
                "progress": 20
            }, method="POST")

            local_vms = []
            try:
                rc_vl, stdout_vl, _ = run_cql_query("SELECT JSON name, host_ip, state, disks_list FROM hydra.vms;")
                if rc_vl == 0 and stdout_vl:
                    for line in stdout_vl.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                v_info = json.loads(line)
                                if v_info.get("state") == "Running" and v_info.get("host_ip") == target_ip:
                                    local_vms.append(v_info)
                            except:
                                pass
            except Exception as ex:
                print(f"[Reclaim Storage Task] Error querying VMs for {hostname}: {ex}")

            if not local_vms:
                call_catalyst_api("/api/v1/tasks/update", {
                    "task_id": task_id,
                    "status": "completed",
                    "progress": 100
                }, method="POST")
                return True, "No running VMs on host; storage redirection not needed."

            print(f"[Reclaim Storage Task] Redirecting storage traffic locally for {len(local_vms)} VMs running on {hostname} ({target_ip})...")
            all_reclaimed = True
            reclaim_errors = []
            for i, lv in enumerate(local_vms):
                lv_name = lv.get("name")
                lv_disks = domain_vdisk_ids(lv_name, lv.get("disks_list", ""))
                for lvd in lv_disks:
                    run_mtls_spark_api(target_ip, "/api/v1/dfs/vdisk", {"op": "attach", "vdisk_id": lvd})
                reclaim_errs = hand_over_storage(target_ip, lv_disks)
                if reclaim_errs:
                    all_reclaimed = False
                    reclaim_errors.append(f"{lv_name}: {reclaim_errs}")
                    print(f"[Reclaim Storage Task] Autopathing warning: could not reclaim storage for {lv_name}: {reclaim_errs}")

                prog = int(20 + ((i + 1) / len(local_vms)) * 75)
                call_catalyst_api("/api/v1/tasks/update", {
                    "task_id": task_id,
                    "status": "processing",
                    "progress": prog
                }, method="POST")

            call_catalyst_api("/api/v1/tasks/update", {
                "task_id": task_id,
                "status": "completed",
                "progress": 100
            }, method="POST")

            if not all_reclaimed:
                return False, f"Storage redirection partially failed: {'; '.join(reclaim_errors)}"
            return True, f"Successfully redirected storage traffic for {len(local_vms)} VMs to {hostname}."

        elif action == "balance":
            aggressive = payload.get("aggressive", False)
            run_drs_loop(aggressive=aggressive)
            return True, ""

        elif action == "download_image":
            return download_image_from_url(task_id, payload)
            
    except Exception as e:
        return False, str(e)
        
    return False, "Unsupported action."


def _derive_image_name_from_url(url):
    try:
        parsed = urllib.parse.urlparse(str(url or "").strip())
        if parsed.query:
            qs = urllib.parse.parse_qs(parsed.query)
            for key in ("filename", "response-content-disposition", "content-disposition"):
                vals = qs.get(key) or []
                for val in vals:
                    m = re.search(r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';]+)["\']?', val, re.I)
                    if m:
                        candidate = os.path.basename(urllib.parse.unquote(m.group(1))).strip()
                        if candidate:
                            return candidate
                    elif key == "filename":
                        candidate = os.path.basename(urllib.parse.unquote(val)).strip()
                        if candidate:
                            return candidate
        base = os.path.basename(urllib.parse.unquote(parsed.path or ""))
        return base.strip()
    except Exception:
        return ""


def _probe_url_content_length(url, insecure=False):
    # First try HEAD (-sIL). Note that S3/R2 presigned GET URLs often return 403 on HEAD
    # because the signature is bound to GET; if HEAD fails or returns no length, fall back
    # to a 1-byte ranged GET (-r 0-0 -D - -o /dev/null) and parse Content-Range / Content-Length.
    for mode in ("head", "range_get"):
        if mode == "head":
            cmd = ["curl", "-sIL", "--max-redirs", "10", "--connect-timeout", "15", "-A", "Helios-Valhalla/1.0"]
        else:
            cmd = ["curl", "-sL", "-r", "0-0", "-D", "-", "-o", "/dev/null", "--max-redirs", "10", "--connect-timeout", "15", "-A", "Helios-Valhalla/1.0"]
        if insecure:
            cmd.append("-k")
        cmd.extend(["--", url])
        try:
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=25)
            if res.returncode != 0:
                continue
            length = None
            status_code = 200
            for line in res.stdout.decode("utf-8", "replace").splitlines():
                lower = line.strip().lower()
                if lower.startswith("http/"):
                    parts = lower.split()
                    if len(parts) >= 2 and parts[1].isdigit():
                        status_code = int(parts[1])
                elif lower.startswith("content-range:") and "/" in lower:
                    total_part = lower.rsplit("/", 1)[-1].strip()
                    if total_part.isdigit() and int(total_part) > 0:
                        length = int(total_part)
                elif lower.startswith("content-length:") and mode == "head":
                    val = lower.split(":", 1)[1].strip()
                    if val.isdigit() and int(val) > 0:
                        length = int(val)
            if status_code in (200, 206) and length and length > 0:
                return length
        except Exception:
            continue
    return None


def download_image_from_url(task_id, payload):
    """Stream an ISO, qcow2, vmdk, ova, or raw image from an HTTP(S) URL into a sealed Sidon vdisk."""
    import hashlib
    url = str(payload.get("url") or "").strip()
    if not url or not (url.startswith("http://") or url.startswith("https://")):
        return False, "URL must start with http:// or https://"
    if any(ord(ch) <= 0x20 or ord(ch) == 0x7f for ch in url):
        return False, "URL must not contain spaces or control characters."
    parsed = urllib.parse.urlparse(url)
    if not parsed.netloc:
        return False, "URL is missing a hostname."

    name = str(payload.get("name") or payload.get("filename") or "").strip()
    if not name:
        name = _derive_image_name_from_url(url)
    allowed_exts = (".iso", ".qcow2", ".vmdk", ".ova", ".raw", ".img")
    if not name or len(name) > 255 or "/" in name or "\\" in name or any(ord(c) < 0x20 or ord(c) == 0x7f for c in name):
        return False, "Invalid image name; provide an explicit name ending in .iso, .qcow2, .vmdk, .ova, .raw, or .img."
    lower_name = name.lower()
    if not lower_name.endswith(allowed_exts):
        return False, "Image name must end with .iso, .qcow2, .vmdk, .ova, .raw, or .img."

    container = str(payload.get("container") or "").strip() or get_default_container()
    if not is_valid_object_name(container):
        return False, f"Invalid container name: {OBJECT_NAME_ERROR}."

    expected_sha256 = str(payload.get("sha256") or "").strip().lower()
    if expected_sha256 and not re.match(r"^[0-9a-f]{64}$", expected_sha256):
        return False, "SHA-256 checksum must be a 64-character hexadecimal string."

    insecure = bool(payload.get("insecure", False))

    # Refuse if an image with this name is already catalogued
    rc_chk, out_chk, _ = run_cql_query(
        f"SELECT JSON name FROM hydra.valhalla_images WHERE name = '{cql_escape(name)}';"
    )
    if rc_chk == 0 and out_chk:
        for line in out_chk.splitlines():
            s = line.strip()
            if s.startswith("{") and s.endswith("}"):
                try:
                    if json.loads(s).get("name") == name:
                        return False, f"An image named '{name}' is already in the catalogue. Delete it first."
                except Exception:
                    pass

    sidon = sidon_module()
    vdisk_id = sidon.image_vdisk_id(name)
    socket_path = sidon.nbd_socket(vdisk_id)

    def _report(pct):
        if task_id:
            try:
                call_catalyst_api("/api/v1/tasks/update", {
                    "task_id": task_id,
                    "status": "processing",
                    "progress": max(0, min(99, int(pct)))
                }, method="POST")
            except Exception:
                pass

    def _rollback():
        try:
            sidon_op(LOCAL_IP, "detach", vdisk_id)
        except Exception:
            pass
        try:
            sidon_op(LOCAL_IP, "delete", vdisk_id)
        except Exception:
            pass

    _report(2)
    size_bytes = _probe_url_content_length(url, insecure=insecure)
    curl_base = ["curl", "-fSL", "--max-redirs", "10", "--retry", "3", "--connect-timeout", "20", "-A", "Helios-Valhalla/1.0"]
    if insecure:
        curl_base.append("-k")

    hasher = hashlib.sha256()
    created_vdisk = False
    needs_convert = lower_name.endswith((".qcow2", ".vmdk", ".ova"))
    ts_tag = f"{vdisk_id}-{int(time.time())}"
    tmp_path = f"/var/lib/hci/aether/images/.download-{ts_tag}.part"
    extracted_path = f"/var/lib/hci/aether/images/.extract-{ts_tag}.img"
    raw_path = f"/var/lib/hci/aether/images/.raw-{ts_tag}.raw"

    try:
        if not needs_convert and size_bytes and size_bytes > 0:
            # Direct zero-staging stream from curl into Sidon NBD socket for raw/ISO images
            ok_c, _, detail_c = sidon_op(LOCAL_IP, "create", vdisk_id, size_bytes=size_bytes, container=container)
            if not ok_c:
                return False, f"Could not allocate storage for '{name}' ({vdisk_id}): {detail_c}"
            created_vdisk = True

            ok_a, _, detail_a = sidon_op(LOCAL_IP, "attach", vdisk_id)
            if not ok_a:
                _rollback()
                return False, f"Could not attach '{vdisk_id}' on {LOCAL_IP}: {detail_a}"

            _report(5)
            proc = subprocess.Popen(
                curl_base + ["--", url],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            writer = None
            written = 0
            last_pct = 5
            last_ts = time.time()
            chunk_size = 8 * sidon.NBD_CHUNK
            try:
                writer = sidon.NbdWriter(socket_path, vdisk_id)
                while written < size_bytes:
                    want = min(chunk_size, size_bytes - written)
                    buf = bytearray()
                    while len(buf) < want:
                        piece = proc.stdout.read(want - len(buf))
                        if not piece:
                            break
                        buf.extend(piece)
                    if not buf:
                        break
                    data = bytes(buf)
                    hasher.update(data)
                    writer.write_pipelined(written, data, max_in_flight=8)
                    written += len(data)
                    now = time.time()
                    pct = 5 + int((written * 88) / size_bytes)
                    if pct >= last_pct + 2 and (now - last_ts) >= 1.5:
                        _report(pct)
                        last_pct = pct
                        last_ts = now
                extra = proc.stdout.read(1)
                if extra:
                    proc.kill()
                    raise RuntimeError(f"Remote stream exceeded advertised Content-Length ({size_bytes} bytes)")
                writer.flush()
            finally:
                if writer:
                    try:
                        writer.close()
                    except Exception:
                        pass
            _, stderr_bytes = proc.communicate(timeout=30)
            if proc.returncode != 0:
                err_txt = (stderr_bytes or b"").decode("utf-8", "replace").strip()
                raise RuntimeError(f"curl failed (exit {proc.returncode}): {err_txt or 'transfer error'}")
            if written != size_bytes:
                raise RuntimeError(f"Download truncated: received {written} of {size_bytes} bytes")
            actual_sha256 = hasher.hexdigest().lower()
            if expected_sha256 and actual_sha256 != expected_sha256:
                raise RuntimeError(
                    f"SHA-256 checksum mismatch for '{name}': expected {expected_sha256}, got {actual_sha256}"
                )
        else:
            # Stage to local image directory first (needed for qcow2/vmdk/ova conversion or unknown Content-Length)
            os.makedirs("/var/lib/hci/aether/images", exist_ok=True)
            _report(5)
            res = subprocess.run(
                curl_base + ["-o", tmp_path, "--", url],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            if res.returncode != 0:
                err_txt = (res.stderr or b"").decode("utf-8", "replace").strip()
                raise RuntimeError(f"curl failed (exit {res.returncode}): {err_txt or 'transfer error'}")
            if not os.path.exists(tmp_path):
                raise RuntimeError("Downloaded file was not created.")
            dl_bytes = os.path.getsize(tmp_path)
            if dl_bytes <= 0:
                raise RuntimeError("Downloaded image is empty (0 bytes).")

            # Compute SHA-256 on the downloaded artifact before any format conversion
            with open(tmp_path, "rb") as hf:
                while True:
                    hchunk = hf.read(8 * 1024 * 1024)
                    if not hchunk:
                        break
                    hasher.update(hchunk)
            actual_sha256 = hasher.hexdigest().lower()
            if expected_sha256 and actual_sha256 != expected_sha256:
                raise RuntimeError(
                    f"SHA-256 checksum mismatch for '{name}': expected {expected_sha256}, got {actual_sha256}"
                )

            _report(40)
            stream_source_path = tmp_path
            size_bytes = dl_bytes
            skip_zeroes = False

            if needs_convert:
                src_disk_path = tmp_path
                if lower_name.endswith(".ova"):
                    import tarfile
                    if not tarfile.is_tarfile(tmp_path):
                        raise RuntimeError("OVA file is not a valid tar archive.")
                    with tarfile.open(tmp_path, "r:*") as tf:
                        disk_member = None
                        for member in tf.getmembers():
                            mname = member.name.lower()
                            if member.isfile() and mname.endswith((".vmdk", ".qcow2", ".raw", ".img")):
                                disk_member = member
                                break
                        if not disk_member:
                            raise RuntimeError("OVA archive does not contain a .vmdk, .qcow2, or .raw disk image.")
                        src_f = tf.extractfile(disk_member)
                        if not src_f:
                            raise RuntimeError("Could not extract disk image from OVA archive.")
                        with open(extracted_path, "wb") as out_f:
                            while True:
                                chunk = src_f.read(8 * 1024 * 1024)
                                if not chunk:
                                    break
                                out_f.write(chunk)
                    src_disk_path = extracted_path

                info_res = subprocess.run(
                    ["qemu-img", "info", "--output=json", src_disk_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                if info_res.returncode != 0:
                    err_info = (info_res.stderr or b"").decode("utf-8", "replace").strip()
                    raise RuntimeError(f"qemu-img info failed: {err_info}")
                info_json = json.loads(info_res.stdout.decode("utf-8", "replace"))
                virtual_size = int(info_json.get("virtual-size") or 0)
                if virtual_size <= 0:
                    raise RuntimeError("qemu-img reported invalid virtual-size for image.")
                img_fmt = str(info_json.get("format") or "").lower()
                _report(50)
                if img_fmt == "raw":
                    stream_source_path = src_disk_path
                    size_bytes = virtual_size
                    skip_zeroes = True
                else:
                    conv_res = subprocess.run(
                        ["qemu-img", "convert", "-O", "raw", src_disk_path, raw_path],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    if conv_res.returncode != 0:
                        err_conv = (conv_res.stderr or b"").decode("utf-8", "replace").strip()
                        raise RuntimeError(f"qemu-img convert to raw failed: {err_conv}")
                    stream_source_path = raw_path
                    size_bytes = virtual_size
                    skip_zeroes = True

            _report(65)
            ok_c, _, detail_c = sidon_op(LOCAL_IP, "create", vdisk_id, size_bytes=size_bytes, container=container)
            if not ok_c:
                return False, f"Could not allocate storage for '{name}' ({vdisk_id}): {detail_c}"
            created_vdisk = True

            ok_a, _, detail_a = sidon_op(LOCAL_IP, "attach", vdisk_id)
            if not ok_a:
                _rollback()
                return False, f"Could not attach '{vdisk_id}' on {LOCAL_IP}: {detail_a}"

            writer = None
            written = 0
            chunk_size = 8 * sidon.NBD_CHUNK
            zero_full = b"\x00" * chunk_size
            try:
                writer = sidon.NbdWriter(socket_path, vdisk_id)
                with open(stream_source_path, "rb") as f:
                    while written < size_bytes:
                        data = f.read(min(chunk_size, size_bytes - written))
                        if not data:
                            break
                        if skip_zeroes and (data == zero_full or data == b"\x00" * len(data)):
                            written += len(data)
                            continue
                        writer.write_pipelined(written, data, max_in_flight=8)
                        written += len(data)
                writer.flush()
            finally:
                if writer:
                    try:
                        writer.close()
                    except Exception:
                        pass

        _report(95)
        ok_s, _, detail_s = sidon_op(LOCAL_IP, "seal", vdisk_id)
        if not ok_s:
            raise RuntimeError(f"Image '{vdisk_id}' was written but could not be sealed: {detail_s}")

        img_type = "iso" if lower_name.endswith(".iso") else "template"
        now_ms = int(time.time() * 1000)
        cql_ins = (
            f"INSERT INTO hydra.valhalla_images (name, filename, size_bytes, type, path, container, created_at) "
            f"VALUES ('{cql_escape(name)}', '{cql_escape(name)}', {int(size_bytes)}, '{img_type}', "
            f"'{cql_escape(socket_path)}', '{cql_escape(container)}', {now_ms});"
        )
        rc_ins, _, err_ins = run_cql_query(cql_ins)
        if rc_ins != 0:
            raise RuntimeError(f"Image was sealed as {vdisk_id} but could not be registered in Hydra: {err_ins}")

        return True, LOCAL_IP
    except Exception as exc:
        if created_vdisk:
            _rollback()
        return False, str(exc)
    finally:
        for p in (tmp_path, extracted_path, raw_path):
            try:
                if os.path.exists(p):
                    os.unlink(p)
            except Exception:
                pass


# Queue worker thread
def queue_thread_loop():
    print("Vali Catalyst worker thread started.")
    if LOCAL_IP == "127.0.0.1":
        # LOCAL_IP is read from LOCAL_HYPERVISOR_IP in /etc/hci/spectrum/spectrum.env, and
        # this is the fallback for a file that does not name the node. It no longer decides
        # whether this process is the worker -- the ballot does, and a ballot does not need
        # to know its own address to be the lowest one. What it does decide is whether the
        # candidacy publishes anything useful: the identity in the ballot is how a submitter
        # finds the queue holder, so an anonymous winner is a queue nobody can reach.
        sys.stderr.write(
            "[vali] WARNING: this node does not know its own address "
            "(LOCAL_HYPERVISOR_IP is absent from /etc/hci/spectrum/spectrum.env). It can "
            "stand for the queue worker candidacy but publishes no address with it, so "
            "anything resolving the worker by its published identity cannot reach it.\n")
        sys.stderr.flush()

    worker = candidacy(helios_zk.SERVICE_VALI_QUEUE)
    was_worker = None
    while True:
        try:
            leading = worker.leading()
            if leading != was_worker:
                # Whether this process drains the queue at all is decided here, so the
                # transition earns a line. It is the difference an operator needs between
                # "the worker is busy" and "no worker is running anywhere".
                print("Vali Catalyst worker: %s" % (
                    "draining the queue, this node holds the vali-queue candidacy" if leading
                    else "standing by, another node holds the vali-queue candidacy"))
                sys.stdout.flush()
                was_worker = leading
            if not leading:
                time.sleep(2)
                continue

            status, res = call_catalyst_api("/api/v1/queues/vali")
            if status == 200 and res:
                task_id = res.get("task_id")
                action = res.get("action")
                payload = res.get("payload", {})
                
                task = {
                    "task_id": task_id,
                    "vm_name": payload.get("vm_name"),
                    "action": action,
                    "target_host": payload.get("target_host"),
                    "payload": payload
                }
                
                # Notify Catalyst we are processing
                call_catalyst_api("/api/v1/tasks/update", {
                    "task_id": task_id,
                    "status": "processing",
                    "progress": 0
                }, method="POST")
                
                def run_task_async(t_info):
                    try:
                        success, out_msg = process_queue_task(t_info)
                        status_str = "completed" if success else "failed"
                        clean_msg = out_msg if not success else ""
                        result_data = {"target_host": out_msg} if (success and out_msg) else {}
                        
                        call_catalyst_api("/api/v1/tasks/update", {
                            "task_id": t_info["task_id"],
                            "status": status_str,
                            "progress": 100,
                            "error_msg": clean_msg,
                            "result": result_data
                        }, method="POST")
                    except Exception as ex:
                        sys.stderr.write(f"Error executing async task {t_info['task_id']}: {ex}\n")
                        call_catalyst_api("/api/v1/tasks/update", {
                            "task_id": t_info["task_id"],
                            "status": "failed",
                            "progress": 100,
                            "error_msg": str(ex)
                        }, method="POST")
                
                threading.Thread(target=run_task_async, args=(task,), daemon=True).start()
                
            elif status == 204:
                time.sleep(2)
            else:
                time.sleep(2)
        except Exception as e:
            sys.stderr.write(f"Error in Vali worker thread: {e}\n")
            time.sleep(2)

# --- ring lifecycle: the quorum gate and the cluster maintenance lock -------------------
#
# Entering maintenance stops the host's ScyllaDB along with everything else. On a three
# node cluster at RF=3 with QUORUM reads and writes, stopping the *second* node leaves
# one replica of three, quorum is two, and the whole cluster stops answering -- including
# the metadata reads that the maintenance workflow itself needs to finish. Nothing
# checked for that: the stop was unconditional and the only guard was a scan of
# hydra.nodes that two concurrent requests both passed.
#
# Three separate things are needed and they are deliberately kept separate:
#
#   1. a quorum gate, derived from the *actual* replication factor and the *actual* ring,
#      evaluated immediately before the stop as well as before the evacuation;
#   2. a cluster-wide lock with a holder and a TTL, so two hosts cannot transition at
#      once and a host that dies holding it does not wedge maintenance forever;
#   3. explicit sequencing for a node that leaves the ring for good or comes back --
#      see docs/ring_lifecycle.md.

MAINTENANCE_LOCK_NAME = "cluster-maintenance"
# Short on purpose. A node that dies mid-transition frees the lock in five minutes rather
# than holding it until an operator finds the row, and every long-running step renews it:
# Vali's evacuation task while it runs, then Mipha's control loop for as long as
# hydra.nodes says the host is still in a maintenance state.
MAINTENANCE_LOCK_TTL_SECONDS = 300

# Byte-for-byte the statement in helios_schema.py migration 0002-cluster-locks, which is
# the source of truth for this table. A test asserts the two cannot drift.
#
# It was originally repeated here because nothing called ensure_schema and
# helios_schema.py was not deployed. Both are now true -- init_db_schema() applies the
# migrations at startup -- so this is no longer the only way the table gets created.
#
# It stays as a floor rather than a duplicate. The lock is what stops two hosts draining
# at once, and the cost of it being absent is not an error message: it is a maintenance
# entry that proceeds unguarded. One no-op CREATE against a table that already exists is
# a cheap price for that not depending on a migration having succeeded earlier in boot.
CLUSTER_LOCKS_DDL = (
    "CREATE TABLE IF NOT EXISTS hydra.cluster_locks ( name text PRIMARY KEY, "
    "holder text, holder_token text, reason text, acquired_at_ms bigint );")

_cluster_locks_table_ready = False


def ensure_cluster_locks_table():
    global _cluster_locks_table_ready
    if _cluster_locks_table_ready:
        return
    run_cql_query(CLUSTER_LOCKS_DDL)
    _cluster_locks_table_ready = True




def get_hydra_replication_factor():
    """RF as the database reports it, never a guess.

    A plausible-looking default is worse than no answer here: assuming 3 on a cluster
    that is actually running RF=1 would wave through the stop that takes the only copy
    of the metadata offline.
    """
    rc, stdout, _ = run_cql_query(
        "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'hydra';")
    if rc != 0:
        return None
    return parse_replication_factor(stdout)


_HOST_ID_RE = re.compile(
    r"\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\Z")


def parse_nodetool_status(text):
    """Ring members from `nodetool status`, as {address, status, state, host_id}.

    Rows look like `UN  10.10.102.41  2.38 MB  256  ?  <uuid>  rack1`. The first column
    is two characters: U/D for up or down, then N/L/J/M for normal, leaving, joining or
    moving. Only a member that is both up and normal is a replica that can answer a
    query, so `available` is deliberately narrower than "up": a joining node has not
    finished streaming and a leaving node is on its way out.

    The host id is matched by shape, not by column index: `Load` occupies two fields
    ("2.38 MB") or one ("?"), so everything after it shifts from node to node.
    """
    members = []
    for line in (text or "").splitlines():
        fields = line.split()
        if len(fields) < 2:
            continue
        marker = fields[0]
        if len(marker) != 2 or marker[0] not in "UD" or marker[1] not in "NLJM":
            continue
        members.append({
            "address": fields[1],
            "status": marker[0],
            "state": marker[1],
            "available": marker == "UN",
            "host_id": next((f for f in fields[2:] if _HOST_ID_RE.match(f)), ""),
        })
    return members


def get_ring_members():
    """Read the ring, from this node. Returns (members, error).

    Any node's `nodetool status` describes the whole ring, so this asks the local one
    rather than the host being drained -- which is the host most likely to be the reason
    the ring is being inspected in the first place.
    """
    rc_ring, res_ring, err_ring = run_mtls_spark_api(LOCAL_IP, "/api/v1/db/ring", None, method="GET")
    if rc_ring == 0 and isinstance(res_ring, dict) and "nodes" in res_ring:
        nodes = res_ring.get("nodes") or []
        if nodes:
            members = []
            for n in nodes:
                addr = n.get("address", "")
                st = n.get("status", "")
                st_code = "U" if st.lower().startswith("u") else "D"
                state = n.get("state", "")
                state_code = "N" if state.lower().startswith("n") else ("L" if state.lower().startswith("l") else ("J" if state.lower().startswith("j") else "M"))
                is_available = n.get("available")
                if is_available is None:
                    is_available = (st_code == "U" and state_code == "N")
                members.append({
                    "address": addr,
                    "status": st_code,
                    "state": state_code,
                    "available": bool(is_available),
                    "host_id": n.get("host_id", ""),
                })
            return members, ""
    rc, stdout, stderr = run_remote_spark(LOCAL_IP, "nodetool status")
    if rc != 0:
        return [], (err_ring or stderr or stdout or "nodetool status failed").strip()[:300]
    members = parse_nodetool_status(stdout)
    if not members:
        return [], "nodetool status returned no ring members"
    return members, ""


def quorum_of(replication_factor):
    """What Scylla demands at ConsistencyLevel.QUORUM: a strict majority of RF."""
    return replication_factor // 2 + 1


def evaluate_stop(members, replication_factor, address):
    """Whether stopping `address` leaves the hydra keyspace able to serve QUORUM.

    Returns (allowed, reason, facts). The arithmetic is the worst case on purpose,
    because the cheap version of this check -- "are at least quorum-many nodes up right
    now" -- passes on the exact cluster this gate exists for.

    A given partition lives on `min(RF, ring size)` nodes. We cannot tell from
    `nodetool status` which nodes hold which partition, so every unavailable member is
    assumed to be a replica of the partition we care about; that is the case that
    actually loses data availability, and it is the one worth refusing on.
    """
    facts = {
        "replication_factor": replication_factor,
        "ring_size": len(members),
        "ring_up": sum(1 for m in members if m["available"]),
        "address": address,
    }
    target = next((m for m in members if m["address"] == address), None)
    if target is None:
        # A host that is not in the ring -- one that was never joined, or has been
        # decommissioned -- holds no replicas, so stopping it costs the ring nothing. A gate
        # keyed on "is this host in hydra.nodes" would refuse it forever.
        facts["ring_member"] = False
        return True, f"{address} is not a member of the ScyllaDB ring; it holds no replicas.", facts
    facts["ring_member"] = True
    if not target["available"]:
        return True, (f"{address} is already unavailable to the ring "
                      f"(nodetool reports '{target['status']}{target['state']}'), so stopping it "
                      "removes nothing that is currently answering."), facts

    required = quorum_of(replication_factor)
    assigned = min(replication_factor, len(members))
    unavailable_after = len(members) - (facts["ring_up"] - 1)
    worst_case_replicas_after = max(0, assigned - unavailable_after)
    facts["required"] = required
    facts["replicas_after"] = worst_case_replicas_after

    if worst_case_replicas_after >= required:
        return True, (f"RF={replication_factor} needs {required} replicas for QUORUM; "
                      f"stopping {address} leaves {worst_case_replicas_after}."), facts
    return False, (
        f"Stopping {address} would leave {worst_case_replicas_after} of "
        f"{assigned} replicas available, and QUORUM at RF={replication_factor} needs "
        f"{required}. The hydra keyspace would stop answering reads and writes for the "
        f"whole cluster. Ring: {facts['ring_up']} of {facts['ring_size']} members up."), facts


def check_stop_preserves_quorum(address):
    """The gate itself. Returns (allowed, reason).

    Refuses when the replication factor or the ring cannot be established. An unknown
    answer here is not a safe answer: the whole point is to stop a host whose database
    the cluster still needs.
    """
    replication_factor = get_hydra_replication_factor()
    if not replication_factor:
        return False, ("Could not read the hydra keyspace's replication factor, so the "
                       "effect of stopping this host's database is unknown. Refusing "
                       "rather than guessing.")
    members, error = get_ring_members()
    if error:
        return False, (f"Could not read the ScyllaDB ring ({error}), so the effect of "
                       "stopping this host's database is unknown. Refusing rather than "
                       "guessing.")
    allowed, reason, _facts = evaluate_stop(members, replication_factor, address)
    return allowed, reason


def check_zk_stop_preserves_quorum(address):
    """The ZooKeeper quorum gate. Returns (allowed, reason).

    Ensures that stopping ZooKeeper on `address` leaves at least a majority (quorum)
    of voting participants running in the ensemble.
    """
    cluster_hosts = get_cluster_hosts()
    hosts = [h.get("ip") for h in cluster_hosts if h.get("ip")]
    if len(hosts) <= 1:
        return True, "Single-node cluster; ZooKeeper quorum check not applicable."

    # Try to probe /zookeeper/config or probe each host's server_mode
    zk_config = None
    try:
        endpoints = ["127.0.0.1"] + [h for h in hosts if h != "127.0.0.1"]
        client = helios_zk.connect(endpoints, timeout=2.0)
        zk_config = helios_zk.read_ensemble_config(client)
        client.close()
    except Exception:
        pass

    modes = {}
    for h in hosts:
        try:
            modes[h] = helios_zk.server_mode(h, timeout=0.5)
        except Exception:
            modes[h] = None

    target_mode = modes.get(address)
    if not target_mode:
        return True, f"ZooKeeper on {address} is already not answering; stopping it removes nothing currently voting."

    if zk_config and "members" in zk_config:
        voters = [m["host"] for m in zk_config["members"] if m.get("role") == helios_zk.PARTICIPANT]
    else:
        voters = hosts[:min(3, len(hosts))]

    total_voters = len(voters)
    required = total_voters // 2 + 1
    live_voters = sum(1 for v in voters if modes.get(v) in ("leader", "follower", "standalone"))

    if address in voters:
        voters_after = live_voters - 1
        if voters_after < required:
            return False, (
                f"Stopping ZooKeeper on {address} would leave {voters_after} of {total_voters} "
                f"voters answering, and ZooKeeper quorum requires {required}. "
                f"Active voters right now: {live_voters} of {total_voters}."
            )
        return True, f"ZooKeeper has {total_voters} voters (needs {required} for quorum); stopping {address} leaves {voters_after}."
    else:
        return True, f"ZooKeeper on {address} is an observer or non-voter; stopping it does not affect quorum ({live_voters}/{total_voters} voters active)."


def wait_zookeeper_rejoined(address, timeout=30, poll_interval=1.0, sleep=None):
    """Wait for ZooKeeper on `address` to start and successfully rejoin the ensemble.

    Returns (rejoined, mode_or_error).
    """
    sleep_fn = sleep or time.sleep
    deadline = time.time() + timeout
    last_err = ""
    while time.time() < deadline:
        try:
            mode = helios_zk.server_mode(address, timeout=1.0)
            if mode in ("leader", "follower", "observer", "standalone"):
                return True, mode
            last_err = f"reporting mode '{mode}'" if mode else "not answering stat"
        except Exception as ex:
            last_err = str(ex)
        sleep_fn(poll_interval)
    return False, f"ZooKeeper did not rejoin the ensemble within {timeout}s ({last_err})"


def acquire_maintenance_lock(hostname, reason):
    """Take the cluster-wide maintenance lock. Returns (ok, token, current, error).

    `ok` is False only for a real failure. A lock already held is (True, "", {...}, "")
    and `current` carries the holder -- a refused acquisition is a lost race, not an
    outage, exactly as Daruk's other conditional endpoints report it.
    """
    ensure_cluster_locks_table()
    token = str(uuid.uuid4())
    ok, applied, current, error = run_lwt("/v1/lock/acquire", {
        "name": MAINTENANCE_LOCK_NAME,
        "holder": hostname,
        "holder_token": token,
        "reason": reason,
        "acquired_at_ms": int(time.time() * 1000),
        "ttl_seconds": MAINTENANCE_LOCK_TTL_SECONDS,
    })
    if not ok:
        return False, "", {}, error
    if not applied:
        return True, "", current, ""
    return True, token, {}, ""


def read_maintenance_lock():
    """The lock row as it stands, or {} if nobody holds it."""
    rc, stdout, _ = run_cql_query(
        "SELECT JSON name, holder, holder_token, reason, acquired_at_ms "
        f"FROM hydra.cluster_locks WHERE name = '{MAINTENANCE_LOCK_NAME}';")
    if rc != 0 or not stdout:
        return {}
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except Exception:
                pass
    return {}


def renew_maintenance_lock(hostname, token, reason):
    """Push the TTL out, but only for the acquisition that owns it."""
    ok, applied, _current, error = run_lwt("/v1/lock/renew", {
        "name": MAINTENANCE_LOCK_NAME,
        "holder": hostname,
        "holder_token": token,
        "reason": reason,
        "acquired_at_ms": int(time.time() * 1000),
        "ttl_seconds": MAINTENANCE_LOCK_TTL_SECONDS,
    })
    if not ok:
        sys.stderr.write(f"[Maintenance] Could not renew the maintenance lock: {error}\n")
        return False
    if not applied:
        # The TTL ran out and somebody else took it, or this host was released by hand.
        # Say so loudly: from here on nothing is excluding a second host's transition.
        sys.stderr.write(
            f"[Maintenance] Host {hostname} no longer holds the cluster maintenance lock.\n")
    return applied


def release_maintenance_lock(token):
    """Give the lock back. Conditional on the token, so it can only drop our own."""
    if not token:
        return False
    ok, applied, _current, error = run_lwt("/v1/lock/release", {
        "name": MAINTENANCE_LOCK_NAME,
        "holder_token": token,
    })
    if not ok:
        sys.stderr.write(f"[Maintenance] Could not release the maintenance lock: {error}\n")
        return False
    if not applied:
        # Expired, or already released. Not an error, and specifically not something to
        # retry unconditionally -- an unconditional delete here would drop whatever lock
        # has since been taken by whichever host is draining now.
        print("[Maintenance] The maintenance lock was no longer this transition's to release.")
    return applied


def release_maintenance_lock_for_host(hostname):
    """Release the lock held on `hostname`'s behalf, without holding its token.

    `leave` can arrive hours after `enter`, from a different Vali process after a leader
    change, so the token from the acquisition is long gone. Reading the row and then
    releasing on the token it carries is still safe: if the lock changes hands between
    the read and the release, the token no longer matches and the release is refused.
    """
    lock = read_maintenance_lock()
    if not lock:
        return False
    if lock.get("holder") != hostname:
        print(f"[Maintenance] The maintenance lock is held by '{lock.get('holder')}', "
              f"not by '{hostname}'; leaving it alone.")
        return False
    return release_maintenance_lock(lock.get("holder_token") or "")


MIGRATION_LOCK_TTL_SECONDS = 900


def migration_lock_name(vm_name):
    """Cluster lock name for a VM's active migration lease."""
    return f"vm_migration:{vm_name}"


def acquire_migration_holder_lock(vm_name, holder, reason="", ttl_seconds=MIGRATION_LOCK_TTL_SECONDS):
    """Acquire a per-VM migration lease with a unique holder token and TTL in `hydra.cluster_locks`.

    Returns `(ok, token, current, error)`.
    """
    ensure_cluster_locks_table()
    token = str(uuid.uuid4())
    ok, applied, current, error = run_lwt("/v1/lock/acquire", {
        "name": migration_lock_name(vm_name),
        "holder": str(holder or vm_name),
        "holder_token": token,
        "reason": str(reason or ""),
        "acquired_at_ms": int(time.time() * 1000),
        "ttl_seconds": int(ttl_seconds),
    })
    if not ok:
        return False, "", {}, error
    if not applied:
        return True, "", current, ""
    return True, token, {}, ""


def release_migration_holder_lock(vm_name, token):
    """Release a per-VM migration lease conditional on `holder_token`.

    Returns `(ok, applied, current, error)`. When `applied` is False, the lease expired
    or was superseded by a newer migration and the caller must NOT drop `/v1/vm/migrate-unlock`.
    """
    if not token:
        return True, False, {}, ""
    return run_lwt("/v1/lock/release", {
        "name": migration_lock_name(vm_name),
        "holder_token": token,
    })


# REST HTTP Handlers
MAINTENANCE_EXIT_WAIT_SECONDS = 180
MAINTENANCE_EXIT_POLL_SECONDS = 5


def services_down_on(target_ip):
    """The managed services not UP on a host, or None when its status cannot be read."""
    rc, status, _ = run_mtls_spark_api(target_ip, "/api/v1/node/status", None, method="GET")
    if rc != 0 or not isinstance(status, dict):
        return None
    services = status.get("services") or {}
    return sorted(name for name, data in services.items()
                  if (data or {}).get("status") != "UP")


def finish_maintenance_exit(hostname, target_ip, wait=None, poll=None, sleep=None):
    """Wait for a host that left maintenance to be fully up, then make it NORMAL and free the lock.

    Returns (True, "") once every service the host publishes is UP, the row is NORMAL (a
    compare-and-swap from RECOVERING, so a status somebody changed meanwhile is not
    overwritten) and the cluster maintenance lock is released. Otherwise (False, reason) with
    the host left RECOVERING and the lock still held, which is what makes a failed exit
    retryable by running leave again.
    """
    wait = MAINTENANCE_EXIT_WAIT_SECONDS if wait is None else wait
    poll = MAINTENANCE_EXIT_POLL_SECONDS if poll is None else poll
    sleep = sleep or time.sleep
    deadline = time.time() + wait
    down = None
    while True:
        down = services_down_on(target_ip)
        if down == []:
            break
        if time.time() >= deadline:
            what = ("its status could not be read" if down is None
                    else "these services are not up: " + ", ".join(down))
            return False, (f"Host {hostname} left maintenance but is not fully back after {wait}s "
                           f"({what}). It stays RECOVERING with the maintenance lock held; run "
                           f"'valcli host.maintenance.leave {hostname}' again to retry.")
        sleep(poll)
    ok, applied, current, err = run_lwt("/v1/node/maintenance", {
        "hostname": hostname, "status": "NORMAL", "maintenance_mode": False,
        "expected_status": "RECOVERING"})
    if not ok:
        return False, (f"Host {hostname} is back but its status could not be set to NORMAL "
                       f"({err}). It stays RECOVERING with the lock held; run leave again.")
    if not applied:
        # Somebody moved it (an operator, or Mipha fencing it): their state wins, and holding
        # a lock for a host that is no longer recovering would block every other host.
        print(f"[Maintenance Catalyst Task] {hostname} is '{current.get('status')}', not RECOVERING; "
              "left as it is.")
    release_maintenance_lock_for_host(hostname)
    return True, ""


# -- live changes to a running VM -----------------------------------------------------------------
#
# A change to a VM while it runs is applied by the spark-daemon of the host it runs on (to the
# running domain and its persistent definition, `POST /api/v1/vm/<name>/live`), after Vali has done
# the storage and network preparation that needs the cluster, and the VM's row is updated to match,
# because the domain definition is rebuilt from the row at every start. One change at a time per
# VM: it takes the same status lock a migration does. What cannot be done live is refused with the
# reason. Table: docs/vm_lifecycle.md.

LIVE_CHANGE_OPS = ("vcpus", "memory", "cdrom", "nic", "disk", "resources", "annotate", "tags", "pci", "usb")
# The sata letters and virtio letters a VM's CD-ROMs and data disks use (`sda...`, `vdb...`).
_LIVE_LETTERS = "abcdefghijklmnopqrstuvwxyz"


def vm_nic_mac(vm_name, index):
    """The MAC generate_vm_xml gives a VM's Nth NIC. One definition, so a NIC attached live has
    the address the domain would give it at its next start."""
    import hashlib
    h = hashlib.md5(f"{vm_name}_nic_{index}".encode()).hexdigest()
    return f"52:54:00:{h[0:2]}:{h[2:4]}:{h[4:6]}"


def vm_nic_entries(network_id):
    """A VM's NICs as the list of "network" / "network:model" entries its row stores."""
    raw = str(network_id or "").strip()
    if not raw:
        return ["7a68e0d6-11f8-4e89-9430-b3b44b8bc438"]
    if raw.startswith("["):
        try:
            return [str(x) for x in json.loads(raw)]
        except Exception:
            return [raw]
    return [raw]


def network_bridge(net):
    """(bridge, None) for a network a NIC can be hot-attached to, else (None, why)."""
    if not net:
        return None, "that network does not exist"
    if net.get("type") == "vlan" and net.get("vlan_id") is not None:
        return f"br-vlan-{net.get('vlan_id')}", None
    if net.get("type") == "overlay" and net.get("vni") is not None:
        return f"br-ov-{net.get('vni')}", None
    return None, ("a direct (macvtap) network cannot be attached to a running VM; stop the VM, "
                  "edit it and start it again")


def vm_disk_entries(disks_list):
    """A VM's disks as the list of entries its row stores ("20GB:container:bus")."""
    if not disks_list or disks_list == "NONE":
        return []
    return [e for e in disks_list.split(",") if e]


def _entry_size_gib(entry):
    import re as _re
    m = _re.match(r"\A\s*(\d+)\s*(T|TB|TIB|G|GB|GIB)?", entry.split(":")[0], _re.I)
    if not m:
        return None
    n = int(m.group(1))
    return n * 1024 if (m.group(2) or "G").upper().startswith("T") else n


def post_live_change(host_ip, vm_name, body):
    """Ask the host the VM runs on to apply one change. Returns (ok, answer-or-error)."""
    status, answer, err = run_mtls_spark_api_full(host_ip, f"/api/v1/vm/{vm_name}/live", body, timeout=120)
    if status == 200:
        return True, answer
    detail = answer.get("error") if isinstance(answer, dict) else ""
    return False, detail or err or f"spark-daemon answered {status}"


def _set_row(vm_name, **columns):
    """Update columns of the VM's row. Values are bound as typed literals here, never interpolated raw."""
    assignments = []
    allowed = (
        "vcpu", "memory", "iso", "network_id", "disks_list", "disk_size",
        "notes", "tags", "cpu_shares", "cpu_limit", "cpu_reservation",
        "mem_reservation", "mem_limit", "pci_devices", "usb_devices", "guest_ips"
    )
    numeric = ("vcpu", "memory", "disk_size", "cpu_shares", "cpu_limit", "cpu_reservation", "mem_reservation", "mem_limit")
    for key, value in columns.items():
        if key not in allowed:
            raise ValueError(f"not a column a live change may write: {key}")
        # By column, not by the Python type of the value: a numeric column is always an integer
        # literal (a quoted "4" is a type error in CQL), a text column always a quoted, escaped one.
        if value is None:
            assignments.append(f"{key} = null")
        elif key in numeric:
            assignments.append(f"{key} = {cql_int(value)}")
        else:
            assignments.append(f"{key} = '{cql_escape(value)}'")
    rc, _, err = run_cql_query(
        f"UPDATE hydra.vms SET {', '.join(assignments)} WHERE name = '{cql_escape(vm_name)}';")
    return rc == 0, err


def run_live_change(vm_name, payload):
    """Apply one change to a running VM. Returns (ok, message)."""
    op = payload.get("op")
    if op not in LIVE_CHANGE_OPS:
        return False, "op must be one of " + ", ".join(LIVE_CHANGE_OPS)
    vm = get_vm_xml_specs(vm_name)
    if not vm:
        return False, "VM not found in metadata database."
    host = vm.get("host_ip") or ""
    if str(vm.get("state") or "").lower() != "running" or not host:
        return False, (f"{vm_name} is not running ({vm.get('state') or 'unknown'}); live changes apply to "
                       "a running VM. A stopped VM is edited in the console.")
    ok, applied, current, err = run_lwt("/v1/vm/migrate-lock", {"name": vm_name})
    if not ok:
        return False, f"The VM's change lock could not be taken ({err}); nothing was changed."
    if not applied:
        return False, (f"{vm_name} is busy (status {current.get('status')!r}): a migration or another "
                       "change is in progress. Nothing was changed.")
    try:
        return _live_change_locked(vm_name, vm, host, op, payload)
    finally:
        ok_u, applied_u, current_u, err_u = run_lwt("/v1/vm/migrate-unlock", {"name": vm_name})
        if not ok_u:
            sys.stderr.write(f"VM {vm_name}: live change finished but its lock could not be released ({err_u}).\n")


def _live_change_locked(vm_name, vm, host, op, payload):
    module = sidon_module()

    if op == "vcpus":
        count = payload.get("count")
        ok, answer = post_live_change(host, vm_name, {"op": "vcpus", "count": count})
        if not ok:
            return False, answer
        done, err = _set_row(vm_name, vcpu=count)
        return True, f"{vm_name} now has {count} vCPUs" + ("" if done else f" (live; its record could not be updated: {err})")

    if op == "memory":
        mib = payload.get("mib")
        persist = payload.get("persist", False)
        current_mem = int(vm.get("memory") or 0)
        is_growth = mib is not None and mib > current_mem
        ok, answer = post_live_change(host, vm_name, payload)
        if not ok:
            return False, answer
        if persist or is_growth:
            done, err = _set_row(vm_name, memory=mib)
            return True, (f"{vm_name} now has {mib} MiB memory" +
                          ("" if done else f" (live; its record could not be updated: {err})"))
        return True, (f"{vm_name}'s memory balloon is {payload.get('mib')} MiB. This is a runtime setting "
                      "inside the configured memory; the VM's recorded memory, and so its next start, is unchanged.")

    if op == "cdrom":
        slot = payload.get("slot")
        if not isinstance(slot, int) or isinstance(slot, bool) or not 0 <= slot < 26:
            return False, "slot must be the number of the CD-ROM drive, starting at 0"
        image = payload.get("image") or None
        body = {"op": "cdrom", "target": "sd" + _LIVE_LETTERS[slot], "image_vdisk_id": None}
        if image:
            if not is_valid_object_name(image):
                return False, f"Invalid image name: {OBJECT_NAME_ERROR}."
            image_id = module.image_vdisk_id(image)
            ok_a, _, detail = sidon_op(host, "attach", image_id)
            if not ok_a:
                return False, f"step 'attach image {image_id} on {host}' failed: {detail}"
            body["image_vdisk_id"] = image_id
        ok, answer = post_live_change(host, vm_name, body)
        if not ok:
            return False, answer
        slots = [s.strip() for s in str(vm.get("iso") or "").split(",") if s.strip()] if vm.get("iso") else []
        while len(slots) <= slot:
            slots.append("__empty__")
        slots[slot] = image or "__empty__"
        while slots and slots[-1] == "__empty__":
            slots.pop()
        done, err = _set_row(vm_name, iso=",".join(slots))
        return True, (f"drive {slot + 1} of {vm_name} now holds {image}" if image else f"drive {slot + 1} of {vm_name} is ejected") + (
            "" if done else f" (live; its record could not be updated: {err})")

    if op == "annotate":
        notes = payload.get("notes")
        done, err = _set_row(vm_name, notes=notes)
        return True, f"Updated notes for {vm_name}" + ("" if done else f" (live; record error: {err})")

    if op == "tags":
        tags = payload.get("tags")
        done, err = _set_row(vm_name, tags=tags)
        return True, f"Updated tags for {vm_name}" + ("" if done else f" (live; record error: {err})")

    if op == "resources":
        ok, answer = post_live_change(host, vm_name, payload)
        if not ok:
            return False, answer
        cols = {}
        for k in ("cpu_shares", "cpu_limit", "cpu_reservation", "mem_reservation", "mem_limit"):
            if k in payload:
                cols[k] = payload[k]
        if cols:
            done, err = _set_row(vm_name, **cols)
            return True, f"Resource limits updated for {vm_name}" + ("" if done else f" (live; record error: {err})")
        return True, f"Resource limits updated for {vm_name}"

    if op == "pci":
        action = payload.get("action")
        bdf = str(payload.get("bdf") or "").strip().lower()
        m = re.match(r"\A(?:([0-9a-fA-F]{4}):)?([0-9a-fA-F]{2}):([0-9a-fA-F]{2})\.([0-7])\Z", bdf)
        if not m:
            return False, "bdf must be a PCI address such as 0000:01:00.0"
        norm_bdf = f"{(m.group(1) or '0000').lower()}:{m.group(2).lower()}:{m.group(3).lower()}.{m.group(4).lower()}"
        current_bdfs = [b.strip().lower() for b in str(vm.get("pci_devices") or "").split(",") if b.strip()]
        if action == "attach":
            if norm_bdf in current_bdfs:
                return True, f"PCI device {norm_bdf} is already attached to {vm_name}"
            ok, answer = post_live_change(host, vm_name, {"op": "pci", "action": "attach", "bdf": norm_bdf})
            if not ok:
                return False, answer
            current_bdfs.append(norm_bdf)
            done, err = _set_row(vm_name, pci_devices=",".join(current_bdfs))
            return True, f"PCI device {norm_bdf} attached to {vm_name}" + ("" if done else f" (live; record error: {err})")
        if action == "detach":
            if norm_bdf not in current_bdfs:
                return False, f"PCI device {norm_bdf} is not attached to {vm_name}"
            ok, answer = post_live_change(host, vm_name, {"op": "pci", "action": "detach", "bdf": norm_bdf})
            if not ok:
                return False, answer
            current_bdfs = [b for b in current_bdfs if b != norm_bdf]
            done, err = _set_row(vm_name, pci_devices=",".join(current_bdfs) if current_bdfs else "")
            return True, f"PCI device {norm_bdf} detached from {vm_name}" + ("" if done else f" (live; record error: {err})")
        return False, "action must be attach or detach"

    if op == "usb":
        action = payload.get("action")
        usb_id = str(payload.get("usb_id") or "").strip().lower()
        if not re.match(r"\A[0-9a-f]{4}:[0-9a-f]{4}\Z", usb_id):
            return False, "usb_id must be a vendor:product hex pair such as 046d:c52b"
        current_usbs = [u.strip().lower() for u in str(vm.get("usb_devices") or "").split(",") if u.strip()]
        if action == "attach":
            if usb_id in current_usbs:
                return True, f"USB device {usb_id} is already attached to {vm_name}"
            ok, answer = post_live_change(host, vm_name, {"op": "usb", "action": "attach", "usb_id": usb_id})
            if not ok:
                return False, answer
            current_usbs.append(usb_id)
            done, err = _set_row(vm_name, usb_devices=",".join(current_usbs))
            return True, f"USB device {usb_id} attached to {vm_name}" + ("" if done else f" (live; record error: {err})")
        if action == "detach":
            if usb_id not in current_usbs:
                return False, f"USB device {usb_id} is not attached to {vm_name}"
            ok, answer = post_live_change(host, vm_name, {"op": "usb", "action": "detach", "usb_id": usb_id})
            if not ok:
                return False, answer
            current_usbs = [u for u in current_usbs if u != usb_id]
            done, err = _set_row(vm_name, usb_devices=",".join(current_usbs) if current_usbs else "")
            return True, f"USB device {usb_id} detached from {vm_name}" + ("" if done else f" (live; record error: {err})")
        return False, "action must be attach or detach"

    if op == "nic":
        return _live_nic(vm_name, vm, host, payload)

    return _live_disk(vm_name, vm, host, payload, module)


def _live_nic(vm_name, vm, host, payload):
    action = payload.get("action")
    entries = vm_nic_entries(vm.get("network_id"))
    if action == "attach":
        net_id = str(payload.get("network_id") or "").strip()
        model = payload.get("model") or "virtio"
        if not net_id:
            return False, "network_id is required"
        bridge, why = network_bridge(get_network_by_id(net_id))
        if not bridge:
            return False, why
        if len(entries) >= 8:
            return False, "at most 8 network interfaces are supported"
        used_macs = set()
        for i, e in enumerate(entries):
            p = e.split(":")
            used_macs.add(p[2] if len(p) > 2 and p[2] else vm_nic_mac(vm_name, i))
        idx_cand = 0
        while vm_nic_mac(vm_name, idx_cand) in used_macs:
            idx_cand += 1
        mac = vm_nic_mac(vm_name, idx_cand)
        ok, answer = post_live_change(host, vm_name, {
            "op": "nic", "action": "attach", "mac": mac,
            "bridge": bridge, "model": model})
        if not ok:
            return False, answer
        has_explicit = any(len(e.split(":")) > 2 and bool(e.split(":")[2]) for e in entries)
        if has_explicit or idx_cand != len(entries):
            entries.append(f"{net_id}:{model}:{mac}")
        else:
            entries.append(f"{net_id}:{model}")
        done, err = _set_row(vm_name, network_id=json.dumps(entries))
        return True, f"{vm_name} has a new NIC on {bridge}" + ("" if done else f" (live; its record could not be updated: {err})")
    index = payload.get("index")
    if action == "detach" and (index == -1 or index is None):
        index = len(entries) - 1          # the CLI's "the last one"
    if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < len(entries):
        return False, "index must be the position of one of the VM's NICs, starting at 0"
    computed_entries = []
    for i, e in enumerate(entries):
        p = e.split(":")
        net = p[0]
        mod = p[1] if len(p) > 1 and p[1] else "virtio"
        mac = p[2] if len(p) > 2 and p[2] else vm_nic_mac(vm_name, i)
        had_explicit = len(p) > 2 and bool(p[2])
        computed_entries.append((net, mod, mac, had_explicit))
    target_mac = computed_entries[index][2]
    if action == "detach":
        ok, answer = post_live_change(host, vm_name, {"op": "nic", "action": "detach", "mac": target_mac})
        if not ok:
            return False, answer
        is_last = (index == len(entries) - 1)
        computed_entries.pop(index)
        if is_last:
            normalized = [
                f"{net}:{mod}:{mac}" if had_exp else f"{net}:{mod}"
                for net, mod, mac, had_exp in computed_entries
            ]
        else:
            normalized = [f"{net}:{mod}:{mac}" for net, mod, mac, _ in computed_entries]
        done, err = _set_row(vm_name, network_id=json.dumps(normalized))
        nic_desc = "the last NIC" if is_last else f"NIC {index + 1}"
        return True, f"{nic_desc} of {vm_name} is removed" + ("" if done else f" (live; its record could not be updated: {err})")
    if action == "link":
        ok, answer = post_live_change(host, vm_name, {"op": "nic", "action": "link",
                                                      "mac": target_mac,
                                                      "state": payload.get("state")})
        if not ok:
            return False, answer
        return True, (f"NIC {index + 1} of {vm_name} is {payload.get('state')}. The link state is not part of the "
                      "VM's record, so it is up again after the VM next starts.")
    if action == "change":
        net_id = str(payload.get("network_id") or "").strip()
        model = payload.get("model") or computed_entries[index][1]
        bridge, why = network_bridge(get_network_by_id(net_id))
        if not bridge:
            return False, why
        ok, answer = post_live_change(host, vm_name, {"op": "nic", "action": "detach", "mac": target_mac})
        if not ok:
            return False, answer
        ok, answer = post_live_change(host, vm_name, {"op": "nic", "action": "attach", "mac": target_mac,
                                                      "bridge": bridge, "model": model})
        if not ok:
            return False, f"NIC {index + 1} was unplugged but could not be re-plugged on {bridge}: {answer}"
        had_exp = computed_entries[index][3]
        entries[index] = f"{net_id}:{model}:{target_mac}" if had_exp else f"{net_id}:{model}"
        done, err = _set_row(vm_name, network_id=json.dumps(entries))
        return True, f"NIC {index + 1} of {vm_name} is now on {bridge}" + ("" if done else f" (live; its record could not be updated: {err})")
    return False, "action must be attach, detach, link or change"


def _live_disk(vm_name, vm, host, payload, module):
    action = payload.get("action")
    disks = vm_disk_entries(vm.get("disks_list"))
    if action == "attach":
        size = payload.get("size_gib")
        container = payload.get("container") or get_default_container()
        if not isinstance(size, int) or isinstance(size, bool) or size < 1:
            return False, "size_gib must be a whole number of GiB, at least 1"
        if not is_valid_object_name(container):
            return False, f"Invalid container name: {OBJECT_NAME_ERROR}."
        if len(disks) >= 26:
            return False, "a running VM can have at most 25 data disks after its boot disk"
        used_vdisk_ids = set()
        used_letters = set()
        for i, d in enumerate(disks):
            p = d.split(":")
            used_vdisk_ids.add(p[3] if len(p) > 3 and p[3] else module.vdisk_id_for(vm_name, i))
            used_letters.add(p[4] if len(p) > 4 and p[4] else _LIVE_LETTERS[i])
        idx_cand = 1
        while module.vdisk_id_for(vm_name, idx_cand) in used_vdisk_ids:
            idx_cand += 1
        vdisk_id = module.vdisk_id_for(vm_name, idx_cand)
        dev_letter = None
        for ch in _LIVE_LETTERS[1:]:
            if ch not in used_letters:
                dev_letter = ch
                break
        if not dev_letter:
            return False, "no disk slot letters available"
        ok_c, _, detail = sidon_op(host, "create", vdisk_id, size_bytes=size * 1024 ** 3, container=container)
        if not ok_c:
            return False, f"step 'create {vdisk_id}' failed: {detail}"
        ok_a, _, detail = sidon_op(host, "attach", vdisk_id)
        if not ok_a:
            sidon_op(host, "delete", vdisk_id)
            return False, f"step 'attach {vdisk_id} on {host}' failed: {detail}; the new vdisk was removed"
        ok, answer = post_live_change(host, vm_name, {
            "op": "disk", "action": "attach", "vdisk_id": vdisk_id, "target": "vd" + dev_letter})
        if not ok:
            sidon_op(host, "detach", vdisk_id)
            sidon_op(host, "delete", vdisk_id)
            return False, f"{answer}; the new vdisk was removed"
        disks.append(f"{size}GB:{container}:virtio:{vdisk_id}:{dev_letter}")
        done, err = _set_row(vm_name, disks_list=",".join(disks))
        return True, f"{vdisk_id} ({size} GiB) is attached to {vm_name} as vd{dev_letter}" + (
            "" if done else f" (live; its record could not be updated: {err})")
    index = payload.get("index")
    if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < len(disks):
        return False, "index must be the position of one of the VM's disks, starting at 0"
    target_entry = disks[index]
    p = target_entry.split(":")
    vdisk_id = p[3] if len(p) > 3 and p[3] else module.vdisk_id_for(vm_name, index)
    dev_letter = p[4] if len(p) > 4 and p[4] else _LIVE_LETTERS[index]
    target_dev = "vd" + dev_letter
    if action == "detach":
        if index == 0:
            return False, "the boot disk cannot be removed from a running VM"
        if payload.get("confirm_delete") is not True:
            return False, ("removing a disk deletes its data; pass confirm_delete to do it "
                           f"({vdisk_id}, {_entry_size_gib(target_entry) or '?'} GiB)")
        ok, answer = post_live_change(host, vm_name, {"op": "disk", "action": "detach", "target": target_dev})
        if not ok:
            return False, answer
        is_last = (index == len(disks) - 1)
        remaining = []
        for i, d in enumerate(disks):
            if i == index:
                continue
            parts = d.split(":")
            size_part = parts[0]
            cnt_part = parts[1] if len(parts) > 1 and parts[1] else get_default_container()
            bus_part = parts[2] if len(parts) > 2 and parts[2] else "virtio"
            vid_part = parts[3] if len(parts) > 3 and parts[3] else module.vdisk_id_for(vm_name, i)
            let_part = parts[4] if len(parts) > 4 and parts[4] else _LIVE_LETTERS[i]
            had_explicit = len(parts) > 3 and bool(parts[3])
            if is_last and not had_explicit:
                remaining.append(f"{size_part}:{cnt_part}:{bus_part}")
            else:
                remaining.append(f"{size_part}:{cnt_part}:{bus_part}:{vid_part}:{let_part}")
        done, err = _set_row(vm_name, disks_list=",".join(remaining) or "NONE")
        sidon_op(host, "detach", vdisk_id)
        ok_d, _, detail = sidon_op(host, "delete", vdisk_id)
        note = "" if ok_d else f" The vdisk could not be deleted ({detail}) and is now an orphan."
        return True, f"{vdisk_id} is removed from {vm_name}.{note}" + ("" if done else f" Its record could not be updated: {err}")
    if action == "resize":
        size = payload.get("size_gib")
        old = _entry_size_gib(target_entry)
        if not isinstance(size, int) or isinstance(size, bool) or size < 1:
            return False, "size_gib must be a whole number of GiB, at least 1"
        if old is None:
            return False, f"disk {index + 1}'s current size could not be read from the VM's record"
        if size < old:
            return False, f"disk {index + 1} is {old} GiB and can only grow ({size} GiB was asked for)"
        if size == old:
            return True, f"disk {index + 1} is already {old} GiB"
        ok_r, _, detail = sidon_op(host, "resize", vdisk_id, size_bytes=size * 1024 ** 3)
        if not ok_r:
            return False, f"step 'resize {vdisk_id}' failed: {detail}"
        parts = disks[index].split(":")
        parts[0] = f"{size}GB"
        disks[index] = ":".join(parts)
        columns = {"disks_list": ",".join(disks)}
        if index == 0:
            columns["disk_size"] = size
        _set_row(vm_name, **columns)
        ok, answer = post_live_change(host, vm_name, {
            "op": "disk", "action": "resize", "target": target_dev, "size_bytes": size * 1024 ** 3})
        if not ok:
            return False, (f"{vdisk_id} was grown to {size} GiB and the record updated, but the running guest "
                           f"could not be told: {answer}. It sees the new size after its next start.")
        return True, f"{vdisk_id} is {size} GiB and the guest has been told"
    if action == "throttle":
        body = {"op": "disk", "action": "throttle", "target": target_dev}
        if "iops" in payload and payload["iops"] is not None:
            body["iops"] = payload["iops"]
        if "bytes_sec" in payload and payload["bytes_sec"] is not None:
            body["bytes_sec"] = payload["bytes_sec"]
        ok, answer = post_live_change(host, vm_name, body)
        if not ok:
            return False, answer
        return True, f"Throttled disk {target_dev} on {vm_name}"
    return False, "action must be attach, detach, resize or throttle"


def forward_drs_balance_to_leader(leader_ip, aggressive=False):
    """Forward a DRS balance request over mTLS to the Vali node holding `vali-drs`."""
    ctx = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH, cafile="/etc/hci/spark/certs/ca.crt"
    )
    ctx.load_cert_chain(
        certfile="/etc/hci/spark/certs/node.crt",
        keyfile="/etc/hci/spark/certs/node.key",
    )
    ctx.check_hostname = True
    url = f"https://{leader_ip}:9095/api/v1/vms/balance"
    data = json.dumps({"aggressive": bool(aggressive), "forwarded": True}).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as response:
            res = json.loads(response.read().decode("utf-8"))
            return response.status == 200, res, ""
    except Exception as exc:
        return False, {}, str(exc)


def trigger_drs_balance(payload, send_json):
    """`POST /api/v1/vms/balance`: run DRS on the `vali-drs` leader (R-6).

    If this node holds `vali-drs` (or no remote leader is published), run the pass locally.
    Otherwise forward the request to the node holding `vali-drs` so only one node runs
    `run_drs_loop` at a time.
    """
    payload = payload or {}
    aggressive = bool(payload.get("aggressive", False))
    if not payload.get("forwarded"):
        drs = candidacy(helios_zk.SERVICE_VALI_DRS)
        if not drs.leading():
            published = drs.leader_identity()
            leader_ip = (
                published.decode("utf-8", "replace").strip()
                if isinstance(published, (bytes, bytearray))
                else str(published or "").strip()
            )
            if leader_ip and leader_ip not in (LOCAL_IP, "127.0.0.1", "::1", "localhost"):
                ok, res, err = forward_drs_balance_to_leader(leader_ip, aggressive=aggressive)
                if ok:
                    send_json(200, {"status": "triggered", "forwarded_to": leader_ip})
                else:
                    send_json(
                        503,
                        {
                            "error": (
                                f"This node does not hold the vali-drs election and forwarding "
                                f"to leader {leader_ip} failed: {err}"
                            )
                        },
                    )
                return

    threading.Thread(target=run_drs_loop, args=(aggressive,), daemon=True).start()
    send_json(200, {"status": "triggered", "leader": LOCAL_IP})


def handle_maintenance_request(payload, send_json):
    """`POST /api/v1/hosts/maintenance`: enter or leave maintenance for a host.

    A function of the request and a reply callback, rather than a block in the HTTP handler, so
    the state transitions can be tested with the database, the lock and Catalyst stubbed. The
    rule for what a host in maintenance keeps running, and every step and failure of this
    flow, is in docs/maintenance.md.
    """
    hostname = payload.get("hostname")
    action = payload.get("action")
    force_stop = payload.get("force_stop", False)
    preserve_vms = bool(payload.get("preserve_vms", False))
    
    if not hostname or not action:
        send_json(400, {"error": "Parameters hostname and action required."})
        return
    if not is_valid_object_name(hostname):
        send_json(400, {"error": f"Invalid hostname: {OBJECT_NAME_ERROR}."})
        return

    # Query host info from ScyllaDB
    rc, stdout, _ = run_cql_query(f"SELECT JSON hostname, ip, status, maintenance_mode FROM hydra.nodes WHERE hostname = '{hostname}';")
    host_info = None
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    host_info = json.loads(line)
                    break
                except:
                    pass
                    
    if not host_info:
        send_json(404, {"error": f"Host '{hostname}' not found in cluster database."})
        return
        
    target_ip = host_info.get("ip")
    
    if action == "enter":
        # Without an address there is no way to ask the ring about this host, and
        # the quorum gate would find no matching ring member and read that as
        # "holds no replicas" -- waving through the one case it exists to catch.
        if not target_ip:
            send_json(409, {"error": f"Host '{hostname}' has no IP recorded in hydra.nodes, so its effect on the ScyllaDB ring cannot be established."})
            return

        # Refuse before evacuating anything if the database this host carries is
        # one the cluster cannot lose. Entering maintenance stops hydra-db, and
        # on a cluster already missing a replica that stop is what takes the
        # metadata layer below quorum -- after which the maintenance workflow
        # cannot even record what it did. The gate runs again immediately before
        # the stop, because an evacuation can take long enough for a second node
        # to die in the middle of it.
        quorum_ok, quorum_reason = check_stop_preserves_quorum(target_ip)
        if not quorum_ok:
            send_json(409, {
                "error": f"Host '{hostname}' cannot enter maintenance mode: {quorum_reason}",
                "reason": "quorum",
            })
            return

        zk_ok, zk_reason = check_zk_stop_preserves_quorum(target_ip)
        if not zk_ok:
            send_json(409, {
                "error": f"Host '{hostname}' cannot enter maintenance mode: {zk_reason}",
                "reason": "quorum",
            })
            return

        current_status = host_info.get("status")
        if current_status == "IN_MAINTENANCE":
            send_json(409, {"error": f"Host '{hostname}' is already in maintenance mode."})
            return
        if current_status == "SERVICE_MAINTENANCE" and preserve_vms:
            send_json(409, {"error": f"Host '{hostname}' is already in service maintenance mode."})
            return
        if current_status not in ("NORMAL", "SERVICE_MAINTENANCE"):
            send_json(409, {"error": f"Host '{hostname}' is in state '{current_status}', so it cannot enter maintenance mode now."})
            return

        expected_status = current_status  # "NORMAL" or "SERVICE_MAINTENANCE"

        # Exclude every other host's transition, cluster-wide, in one Paxos round.
        #
        # What was here before scanned every hydra.nodes row for a host already in
        # maintenance and then wrote. Two hosts entering a second apart both read
        # "nobody" and both proceeded -- a lightweight transaction cannot span
        # partitions, so the check and the claim were never one operation. A
        # single lock row is the shape that can be: see hydra.cluster_locks.
        lock_ok, lock_token, lock_current, lock_err = acquire_maintenance_lock(
            hostname, f"maintenance entry on {hostname}")
        if not lock_ok:
            send_json(500, {"error": f"Could not take the cluster maintenance lock: {lock_err}"})
            return
        if not lock_token:
            if current_status == "SERVICE_MAINTENANCE" and lock_current.get("holder") == hostname and lock_current.get("holder_token"):
                lock_token = lock_current.get("holder_token")
                renew_maintenance_lock(hostname, lock_token, f"transition to full maintenance on {hostname}")
            else:
                holder = lock_current.get("holder") or "another host"
                send_json(409, {
                    "error": f"Host '{holder}' already holds the cluster maintenance lock "
                             f"({lock_current.get('reason') or 'no reason recorded'}). "
                             "Only one host may transition at a time, to preserve quorum.",
                    "reason": "locked",
                    "holder": lock_current.get("holder", ""),
                })
                return

        # Claim the transition rather than announce it. Draining a host is a lock
        # on that host: two requests -- a double click, or a leave racing an enter
        # that is still evacuating -- both used to write a status and both went on
        # to submit an evacuation task for the same VMs.
        #
        # Only this first transition is conditional. The ones that follow it are
        # made by the workflow that already holds the claim, and `hydra.nodes`
        # status is also written by mipha's health loop, so conditioning them too
        # would wedge a host in ENTERING_MAINTENANCE the first time the two
        # crossed.
        ok, applied, current, err = run_lwt("/v1/node/maintenance", {
            "hostname": hostname,
            "status": "ENTERING_MAINTENANCE",
            "maintenance_mode": False,
            "expected_status": expected_status,
        })
        # Every exit from here on has to give the lock back. Leaving it held on a
        # request that never started a transition blocks maintenance cluster-wide
        # until the TTL runs out.
        if not ok:
            release_maintenance_lock(lock_token)
            send_json(500, {"error": f"Could not begin the maintenance transition for '{hostname}': {err}"})
            return
        if not applied:
            release_maintenance_lock(lock_token)
            send_json(409, {"error": f"Host '{hostname}' is in state '{current.get('status')}', not {expected_status}, so it cannot enter maintenance mode now."})
            return

        # Submit Catalyst task. The lock token travels with it: the task renews
        # the lock while it evacuates and releases it if the transition fails.
        status_api, res_api = call_catalyst_api("/api/v1/tasks/submit", {
            "service": "vali",
            "action": "host_maintenance_enter",
            "payload": {
                "hostname": hostname,
                "target_ip": target_ip,
                "force_stop": force_stop,
                "preserve_vms": preserve_vms,
                "lock_token": lock_token
            }
        }, method="POST")

        if status_api == 200:
            task_id = res_api.get("task_id")
            send_json(200, {"status": "transitioning", "task_id": task_id, "message": f"Entering maintenance mode task submitted (Task ID: {task_id})."})
        else:
            # Nothing is going to run, so put the host and the lock back.
            run_lwt("/v1/node/maintenance", {
                "hostname": hostname,
                "status": expected_status,
                "maintenance_mode": (expected_status == "SERVICE_MAINTENANCE"),
                "expected_status": "ENTERING_MAINTENANCE",
            })
            release_maintenance_lock(lock_token)
            send_json(500, {"error": f"Failed to submit host maintenance task to Catalyst: {res_api}"})

    elif action == "leave":
        # Leave is also how a transition that did not finish is put right (an enter whose task
        # died leaves ENTERING_MAINTENANCE; a leave whose services did not all start leaves
        # RECOVERING), so those are allowed. A host that is NORMAL is not in maintenance: running
        # the sequence on it would restart its services and set a healthy host RECOVERING.
        current_state = host_info.get("status")
        if current_state not in LEAVABLE_HOST_STATES:
            send_json(409, {"error": f"Host '{hostname}' is in state '{current_state}', not in "
                                     "maintenance, so there is nothing to leave.",
                            "reason": "not_in_maintenance"})
            return
        # Submit Catalyst task
        status_api, res_api = call_catalyst_api("/api/v1/tasks/submit", {
            "service": "vali",
            "action": "host_maintenance_leave",
            "payload": {
                "hostname": hostname,
                "target_ip": target_ip
            }
        }, method="POST")
        
        if status_api == 200:
            task_id = res_api.get("task_id")
            send_json(200, {"status": "transitioning", "task_id": task_id, "message": f"Leaving maintenance mode task submitted (Task ID: {task_id})."})
        else:
            send_json(500, {"error": f"Failed to submit host maintenance leave task to Catalyst: {res_api}"})
        
    else:
        send_json(400, {"error": f"Invalid action '{action}'."})


class ValiAPIHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass # Disable logging to prevent stdout clutter

    def setup(self):
        """Complete the TLS handshake in the worker thread.

        Per connection rather than by wrapping the listening socket, so one slow or
        hostile client cannot stall every other connection during its handshake.
        """
        self.connection = self.server.ssl_context.wrap_socket(self.request, server_side=True)
        if self.timeout is not None:
            self.connection.settimeout(self.timeout)
        if self.disable_nagle_algorithm:
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        self.rfile = self.connection.makefile("rb", self.rbufsize)
        self.wfile = self.connection.makefile("wb", self.wbufsize)

    def do_GET(self):
        if self.path == "/api/v1/hosts":
            try:
                rc, stdout, _ = run_cql_query("SELECT JSON hostname, ip, status, maintenance_mode FROM hydra.nodes;")
                hosts = []
                if rc == 0 and stdout:
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                hosts.append(json.loads(line))
                            except:
                                pass
                self.send_json(200, {"hosts": hosts})
            except Exception as e:
                self.send_json(500, {"error": str(e)})
        elif self.path == "/api/v1/drs/status":
            try:
                # Fetch current stats
                cql_st = "SELECT JSON * FROM hydra.vali_drs_status WHERE cluster_name = 'default';"
                rc, stdout, _ = run_cql_query(cql_st)
                status_obj = {"current_deviation": 0.0, "status_str": "Balanced (happy)", "last_drs_run": 0}
                if rc == 0:
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            status_obj = json.loads(line)
                            break
                            
                # Fetch history log
                cql_hist = "SELECT JSON * FROM hydra.vali_drs_history;"
                rc_h, stdout_h, _ = run_cql_query(cql_hist)
                history = []
                if rc_h == 0:
                    for line in stdout_h.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                history.append(json.loads(line))
                            except:
                                pass
                                
                # Sort history desc
                history.sort(key=lambda x: x.get("event_time", 0), reverse=True)
                
                # Format response
                deviation = status_obj.get("current_deviation", 0.0)
                balance_score = max(0, min(100, int((1 - 2 * deviation) * 100)))
                
                res = {
                    "balance_score": balance_score,
                    "deviation": round(deviation, 3),
                    "current_deviation": deviation,
                    "status": status_obj.get("status_str", "Balanced (happy)"),
                    "status_str": status_obj.get("status_str", "Balanced (happy)"),
                    "last_run": status_obj.get("last_drs_run", 0),
                    "last_drs_run": status_obj.get("last_drs_run", 0),
                    "drs_enabled": status_obj.get("drs_enabled", True) if status_obj.get("drs_enabled") is not None else True,
                    "history": history[:15] # Return last 15 migrations
                }
                
                self.send_json(200, res)
            except Exception as e:
                self.send_json(500, {"error": str(e)})
        else:
            self.send_json(404, {"error": "Not Found"})

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length)
        payload = {}
        if post_data:
            try:
                payload = json.loads(post_data.decode("utf-8"))
            except:
                self.send_json(400, {"error": "Invalid JSON payload"})
                return

        if self.path == "/api/v1/vms/power":
            name = payload.get("name")
            action = payload.get("action")
            if not name or not action:
                self.send_json(400, {"error": "Parameters name and action required."})
                return
            if not is_valid_object_name(name):
                self.send_json(400, {"error": f"Invalid VM name: {OBJECT_NAME_ERROR}."})
                return

            db_action = "start" if action == "on" else ("stop" if action == "off" else action)
            success, err_msg, target = submit_and_wait_task("vali", db_action, {"vm_name": name}, timeout_polls=POWER_TIMEOUT_POLLS)
            if success:
                self.send_json(200, {"name": name, "status": action + "ed", "node": target})
            else:
                self.send_json(500, {"error": err_msg})
                
        elif self.path == "/api/v1/vms/migrate":
            name = payload.get("name")
            target_host = payload.get("target_host")
            if not name or not target_host:
                self.send_json(400, {"error": "Parameters name and target_host required."})
                return
            if not is_valid_object_name(name):
                self.send_json(400, {"error": f"Invalid VM name: {OBJECT_NAME_ERROR}."})
                return
            if not is_valid_object_name(target_host):
                self.send_json(400, {"error": f"Invalid target_host: {OBJECT_NAME_ERROR}."})
                return

            success, err_msg, _ = submit_and_wait_task("vali", "migrate", {"vm_name": name, "target_host": target_host}, timeout_polls=MIGRATE_TIMEOUT_POLLS)
            if success:
                self.send_json(200, {"name": name, "status": "migrated", "node": target_host})
            else:
                self.send_json(500, {"error": err_msg})
                
        elif self.path == "/api/v1/vms/live":
            name = payload.get("name")
            change = payload.get("change")
            if not name or not isinstance(change, dict):
                self.send_json(400, {"error": "Parameters name and change (an object with op) required."})
                return
            if not is_valid_object_name(name):
                self.send_json(400, {"error": f"Invalid VM name: {OBJECT_NAME_ERROR}."})
                return
            if change.get("op") not in LIVE_CHANGE_OPS:
                self.send_json(400, {"error": "change.op must be one of " + ", ".join(LIVE_CHANGE_OPS)})
                return
            body = dict(change)
            body["vm_name"] = name
            success, err_msg, detail = submit_and_wait_task("vali", "live_change", body, timeout_polls=MIGRATE_TIMEOUT_POLLS)
            if success:
                self.send_json(200, {"name": name, "status": "changed", "message": detail})
            else:
                self.send_json(409, {"error": err_msg})
        elif self.path == "/api/v1/vms/balance":
            trigger_drs_balance(payload, self.send_json)
        elif self.path == "/api/v1/hosts/maintenance":
            handle_maintenance_request(payload, self.send_json)
        else:
            self.send_json(404, {"error": "Not Found"})

    def send_json(self, status, data):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

class ReloadingServerContext(object):
    """Mutual-TLS server context that picks up renewed certificates without a restart.

    The context used to be built once at start, so a certificate renewed on disk (docs/mtls_lifecycle.md
    restarts only spark-daemon) went on being presented from memory until the old one expired, and a
    rotated CA kept being checked against the old one. The files' modification times are compared at
    each accepted connection and the context rebuilt when any changed; a rebuild that fails keeps the
    working context and says so.
    """

    def __init__(self, cert, key, ca):
        self.paths = (cert, key, ca)
        self._stamp = None
        self._context = None
        self._lock = threading.Lock()
        self._refresh()

    def _stat(self):
        return tuple(os.stat(p).st_mtime_ns for p in self.paths)

    def _build(self):
        cert, key, ca = self.paths
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=cert, keyfile=key)
        context.load_verify_locations(cafile=ca)
        context.verify_mode = ssl.CERT_REQUIRED
        return context

    def _refresh(self):
        with self._lock:
            try:
                stamp = self._stat()
            except OSError:
                return
            if stamp == self._stamp:
                return
            try:
                self._context = self._build()
                self._stamp = stamp
            except (OSError, ssl.SSLError) as exc:
                if self._context is None:
                    raise
                sys.stderr.write("[tls] certificate files changed but could not be loaded (%s); "
                                 "keeping the ones in use\n" % exc)
                self._stamp = stamp

    def wrap_socket(self, sock, **kwargs):
        self._refresh()
        return self._context.wrap_socket(sock, **kwargs)


def main():
    print("Vali VM Manager service daemon starting...")
    init_db_schema()
    
    # Start background threads for DRS and task queue worker
    threading.Thread(target=queue_thread_loop, daemon=True).start()
    threading.Thread(target=drs_thread_loop, daemon=True).start()
    
    # Mutual TLS against the cluster CA, as spark-daemon and Catalyst do.
    #
    # This API placed and moved VMs and bound 0.0.0.0 under Network=host while checking
    # neither a credential nor a source address. CERT_REQUIRED means a caller must
    # present a certificate this cluster CA signed -- which means a node -- and the
    # handshake fails before a request line is parsed.
    ca_cert = "/etc/hci/spark/certs/ca.crt"
    node_cert = "/etc/hci/spark/certs/node.crt"
    node_key = "/etc/hci/spark/certs/node.key"
    for path in (ca_cert, node_cert, node_key):
        if not os.path.exists(path):
            print(f"[ERROR] Vali cannot start without {path}. The API would otherwise "
                  f"listen without authentication.")
            sys.exit(1)

    ssl_context = ReloadingServerContext(node_cert, node_key, ca_cert)

    server_address = ("0.0.0.0", 9095)
    httpd = ThreadingHTTPServer(server_address, ValiAPIHandler)
    httpd.ssl_context = ssl_context
    print("Vali API listening on https://0.0.0.0:9095 (mutual TLS)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
