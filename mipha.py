#!/usr/bin/env python3
__build__ = "1.3.0"
import sys
import os
import json
import re
import time
import stat
import socket
import urllib.request
import ssl
import subprocess
import base64
import uuid
import shutil
import threading

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

def run_command_local(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return res.returncode, res.stdout.decode('utf-8', errors='ignore').strip(), res.stderr.decode('utf-8', errors='ignore').strip()


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

def spark_mtls_context(verify_identity):
    """Build the mTLS context Mipha dials peers with, or None if no keypair is usable.

    None rather than ssl._create_unverified_context(): Mipha fences hosts and promotes
    storage decisions off what these calls report, and an unverified context turned
    a missing keypair into a connection that trusts whatever answers on 9099. Failing the
    call is the safe reading of "this node has no identity".
    """
    cert_paths = [
        ("/etc/hci/spark/certs/ca.crt", "/etc/hci/spark/certs/node.crt", "/etc/hci/spark/certs/node.key"),
        ("/root/.certs/ca.crt", "/root/.certs/client.crt", "/root/.certs/client.key")
    ]
    for ca, cert, key in cert_paths:
        if os.path.exists(ca) and os.path.exists(cert) and os.path.exists(key):
            try:
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
                context.load_cert_chain(certfile=cert, keyfile=key)
                context.check_hostname = verify_identity
                return context
            except Exception:
                pass
    return None

def run_remote_spark(ip, command):
    ip, verify_identity = spark_endpoint(ip)
    context = spark_mtls_context(verify_identity)
    if not context:
        return -1, "", "no usable mTLS keypair in /etc/hci/spark/certs or /root/.certs"

    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=15) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)

def run_mtls_spark_api(ip, path, payload, method="POST"):
    ip, verify_identity = spark_endpoint(ip)
    context = spark_mtls_context(verify_identity)
    if not context:
        return -1, {}, "no usable mTLS keypair in /etc/hci/spark/certs or /root/.certs"

    url = f"https://{ip}:9099{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=15) as response:
            res = json.loads(response.read().decode("utf-8"))
            return 0, res, ""
    except Exception as e:
        return -1, {}, str(e)

def run_mtls_spark_api_full(ip, path, payload, method="POST"):
    """Like run_mtls_spark_api, but keeps the status code and the 4xx body.

    The fence endpoint answers 409 with the reason the fence did not take, and
    run_mtls_spark_api turns every HTTPError into `(-1, {}, "<message>")` -- which would
    reduce "two qemu processes are still running" to "the request failed". A fence is the
    one call in this daemon where the failure detail is the whole point.

    Returns (status, body, error). status is 0 when the request could not be made at all.

    The timeout is 60s rather than the 15s the other calls use, because the fence it
    carries out on the far side destroys guests and demotes resources and legitimately
    takes tens of seconds. A fence that overruns it is reported as unanswered, which is
    the safe reading -- the next control-loop pass retries and finds the work already
    done.
    """
    import urllib.error
    address, verify_identity = spark_endpoint(ip)
    context = spark_mtls_context(verify_identity)
    if not context:
        return 0, {}, "no usable mTLS keypair in /etc/hci/spark/certs or /root/.certs"

    url = f"https://{address}:9099{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=60) as response:
            return response.status, json.loads(response.read().decode("utf-8")), ""
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8")), ""
        except Exception:
            return exc.code, {}, f"HTTP {exc.code}"
    except Exception as exc:
        return 0, {}, str(exc)










def is_zookeeper_leader(ip="127.0.0.1"):
    if ip == "127.0.0.1" or ip == LOCAL_IP:
        return get_zookeeper_leader_ip() == LOCAL_IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        s.connect((ip, 2181))
        s.sendall(b"stat")
        resp = s.recv(1024).decode('utf-8', errors='ignore')
        s.close()
        return "mode: leader" in resp.lower() or "mode: standalone" in resp.lower()
    except Exception:
        return False

def get_zookeeper_leader_ip(hosts=None):
    if not hosts:
        hosts = get_cluster_hosts()
    if not hosts:
        ips = [LOCAL_IP]
    else:
        ips = [h.get("ip") for h in hosts if h.get("ip")]
        
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

def get_cluster_hosts():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            return cdata.get("hosts", [])
    except Exception:
        return []

def ping_host(ip):
    # Runs standard Linux ping command, sending 1 packet with 2 second timeout
    try:
        p = subprocess.Popen(f"ping -c 1 -W 2 {ip}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p.communicate()
        return p.returncode == 0
    except Exception:
        return False

def check_vali_health(ip):
    # Vali requires mutual TLS, so even a health probe presents a certificate. A probe
    # that could not authenticate would report every healthy host as down, and Mipha
    # fences on that signal.
    address, verify_identity = spark_endpoint(ip)
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                     cafile="/etc/hci/spark/certs/ca.crt")
    ctx.load_cert_chain(certfile="/etc/hci/spark/certs/node.crt",
                        keyfile="/etc/hci/spark/certs/node.key")
    ctx.check_hostname = verify_identity
    url = f"https://{address}:9095/api/v1/hosts"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, context=ctx, timeout=5) as response:
            return response.status == 200
    except Exception:
        return False

_SIDON = None


def sidon_module():
    """helios_sidon, or None if this node has not been updated yet."""
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
                      os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "helios_sidon.py")):
        if os.path.exists(candidate):
            spec = importlib.util.spec_from_file_location("helios_sidon", candidate)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            _SIDON = module
            return module
    _SIDON = False
    return None

def submit_catalyst_task(leader_ip, service, action, payload):
    # Catalyst requires a cluster-signed certificate: it dispatches VM lifecycle
    # work and used to accept it from anything that could open a socket to 9091.
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                     cafile="/etc/hci/spark/certs/ca.crt")
    ctx.load_cert_chain(certfile="/etc/hci/spark/certs/node.crt",
                        keyfile="/etc/hci/spark/certs/node.key")
    url = f"https://{leader_ip}:9091/api/v1/tasks/submit"
    data = json.dumps({
        "service": service,
        "action": action,
        "payload": payload
    }).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            if response.status == 200:
                res_data = json.loads(response.read().decode("utf-8"))
                return res_data.get("task_id")
    except Exception:
        pass
    return None

# ---------------------------------------------------------------------------
# Fencing.
#
# A failover is safe only if the host being failed over has stopped writing. The fence
# this replaces was one request to spark-daemon on the host itself:
#
#     fence_cmd = "systemctl stop libvirtd virtqemud || true; pkill -9 qemu || true"
#     rc, _, _ = run_remote_spark(ip, fence_cmd)
#     return rc == 0
#
# Three defects, each of which alone is enough to corrupt a disk:
#
#   1. It asks the wedged host to fence itself. A host whose storage stack has hung, or
#      whose kernel is livelocked, still answers ICMP and can still complete a TCP
#      handshake while executing nothing.
#   2. Every command in that string ends in `|| true`, so the shell exits 0 whatever
#      happened. `rc == 0` means "spark-daemon accepted the request" and has never meant
#      "no qemu is running any more".
#   3. The return value was discarded at the call site, and the fence was only attempted
#      when the host still answered ping -- so a host that stopped answering ping was
#      assumed dead on no evidence at all, which is the one case the fence exists for.
#
# What replaces it is a ladder. Every rung must *read back* the state it claims to have
# produced, and a failover does not proceed unless a rung confirmed. "Could not tell" is
# recorded as a failure, never as a success:
#
#   self     the host already fenced itself and said so (hydra.nodes status FENCED).
#   spark    in-band. Kill the guests, stop libvirt, release every vdisk -- then read back that
#            no qemu remains, nothing holds a device open and nothing is Primary.
#   bmc      out-of-band. ipmitool chassis power off, then poll chassis power status
#            until it reads off. A command that returned 0 is not a power-off.
#   storage  the host cannot be reached or powered off, so prove instead that its kernel
#            is already refusing its writes -- see storage_fence_assert().
#
# The residual case this ladder used to carry is gone. With DRBD, a cluster with no BMC
# and no armed quorum had no rung that could confirm anything, and the default was to
# refuse the failover and say so. The storage rung is now a compare-and-swap in Hydra,
# which needs neither a BMC nor a quorum nor the cooperation of the host being fenced, so
# the only way to reach an unconfirmed fence is for Hydra itself to be unreachable --
# where refusing is obviously right rather than regrettable.
# ---------------------------------------------------------------------------

FENCING_CONFIG_PATH = "/etc/hci/fencing.json"

# Written by whichever path fenced this host -- its own watchdog or spark-daemon acting
# on a remote fence request. It lives on tmpfs on purpose: a reboot ends the fence,
# because a rebooted host is not running the guests any more.
SELF_FENCE_MARKER = "/run/hci/mipha-self-fence.json"

FENCE_METHOD_NONE = "none"
FENCE_METHOD_SELF = "self"
FENCE_METHOD_SPARK = "spark"
FENCE_METHOD_BMC = "bmc"
# Not "storage-quorum" any more: nothing about this rung reads a quorum. It raises
# the epoch on the dead host's vdisks and every replica enforces it.
FENCE_METHOD_STORAGE = "storage-epoch"

# `hydra.nodes.status` for a host that has fenced itself. Vali already refuses to place
# on any host whose status is not exactly NORMAL, so this needs no scheduler change.
NODE_STATUS_FENCED = "FENCED"
NODE_STATUS_DEGRADED = "DEGRADED"

# Mount points a fence has to take down before this host counts as released. Fixed rather
# than derived, so a fence never unmounts something it did not put there.
FENCED_MOUNTS = (
                 "/var/lib/hci/aether/volumes/default-vm-container",
                 "/var/lib/hci/aether/volumes/default-image-container")

DEFAULT_FENCING_CONFIG = {
    # What to do when no rung of the ladder could confirm. "block" refuses the failover
    # and says why; "failover" is the old behaviour and can corrupt a disk.
    "unconfirmed_fence_policy": "block",
    "bmc": {
        "defaults": {
            "interface": "lanplus",
            "power_off_timeout_seconds": 60,
        },
        "hosts": {},
    },
    "self_fence": {
        "enabled": True,
        "interval_seconds": 10,
        # Three consecutive passes, matching the cluster-side failover threshold. A
        # single failed probe is a blip; Sidon is briefly unreachable across a restart
        # and virsh times out under load.
        "threshold": 3,
        # Nothing self-fences during startup: resources come up Secondary without
        # quorum and every probe would fire at once.
        "grace_seconds": 180,
        # A host that killed its own guests had a real fault. Returning it to service
        # automatically is how one flapping host takes VMs and drops them repeatedly, so
        # 0 means "an operator runs `mipha --clear-self-fence`".
        "auto_recover_after_clean_seconds": 0,
        # Stopping ZooKeeper hands leadership -- Mipha's, Purah's, Bifrost's
        # VIP -- to a node that can still serve. Refused below three nodes, where the
        # remaining ensemble could not form a quorum anyway.
        "release_zookeeper_leadership": True,
    },
}

_FENCING_CONFIG_CACHE = {"key": None, "value": None}


def _merge_defaults(defaults, override):
    """Two levels of dict merge; anything deeper is taken from the operator verbatim."""
    merged = {}
    for key, value in defaults.items():
        if isinstance(value, dict):
            branch = dict(value)
            supplied = override.get(key)
            if isinstance(supplied, dict):
                for sub_key, sub_value in supplied.items():
                    branch[sub_key] = sub_value
            merged[key] = branch
        else:
            merged[key] = override.get(key, value)
    for key, value in override.items():
        if key not in merged:
            merged[key] = value
    return merged


def fencing_config_is_private(path):
    """True when nothing but root can read the file.

    Its own function because the answer decides whether BMC credentials are usable, and
    that decision has to be assertable in a test without depending on the mode bits the
    test host's filesystem happens to support.
    """
    try:
        return not (os.stat(path).st_mode & 0o077)
    except OSError:
        return False


def host_is_in_maintenance():
    """True when an operator has put this host into maintenance.

    A host being drained on purpose looks a great deal like a host whose storage is
    failing -- resources are demoted, guests are being moved off -- and self-fencing it
    would turn a planned maintenance window into an unplanned outage.
    """
    return os.path.exists("/etc/hci/maintenance.state")


def load_fencing_config(path=None):
    """Read /etc/hci/fencing.json, or the built-in defaults when it is absent.

    Absent is a supported state and means "no BMC" -- it must never mean "assume the
    fence worked", which is why the defaults refuse an unconfirmed failover.

    BMC credentials are dropped, loudly, from a file any non-root account can read. They
    are power-off credentials for the whole cluster; a fence that silently used them out
    of a world-readable file would be a worse outcome than a fence that does not run.

    Returns (config, warnings).
    """
    path = path or FENCING_CONFIG_PATH
    warnings = []
    try:
        st = os.stat(path)
    except OSError:
        return dict(DEFAULT_FENCING_CONFIG), warnings

    cache_key = (path, st.st_mtime_ns, st.st_size)
    cached = _FENCING_CONFIG_CACHE
    if cached.get("key") == cache_key:
        return cached["value"][0], list(cached["value"][1])

    try:
        with open(path, "r") as handle:
            raw = json.load(handle)
    except Exception as exc:
        warnings.append(f"{path} could not be read ({exc}); using built-in defaults, "
                        "which means no BMC fencing.")
        raw = {}
    if not isinstance(raw, dict):
        warnings.append(f"{path} is not a JSON object; using built-in defaults.")
        raw = {}

    if not fencing_config_is_private(path):
        warnings.append(
            f"{path} is mode {oct(st.st_mode & 0o777)}: it is readable by accounts other "
            "than root and BMC credentials are therefore ignored. chmod 600 it to enable "
            "out-of-band fencing.")
        raw = dict(raw)
        raw.pop("bmc", None)

    config = _merge_defaults(DEFAULT_FENCING_CONFIG, raw)
    policy = str(config.get("unconfirmed_fence_policy", "block")).lower()
    if policy not in ("block", "failover"):
        warnings.append(f"unconfirmed_fence_policy '{policy}' is not understood; "
                        "treating it as 'block'.")
        policy = "block"
    config["unconfirmed_fence_policy"] = policy

    _FENCING_CONFIG_CACHE["key"] = cache_key
    _FENCING_CONFIG_CACHE["value"] = (config, list(warnings))
    return config, warnings


def run_argv_local(argv, timeout=45):
    """Run a local command as an argv list, never through a shell.

    The fencing paths pass vdisk ids and BMC addresses into commands. A list
    with shell=False makes each of those exactly one argument whatever it contains.
    """
    try:
        res = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             timeout=timeout)
    except FileNotFoundError:
        return 127, "", f"{argv[0]}: command not found"
    except subprocess.TimeoutExpired:
        return -1, "", f"{argv[0]}: timed out after {timeout}s"
    except OSError as exc:
        return -1, "", str(exc)
    return (res.returncode,
            res.stdout.decode("utf-8", errors="ignore"),
            res.stderr.decode("utf-8", errors="ignore"))


def local_hostname():
    """This node's name as the cluster document spells it."""
    for host in get_cluster_hosts():
        if host.get("ip") == LOCAL_IP:
            return host.get("hostname") or socket.gethostname()
    return socket.gethostname()


class FenceResult:
    """What a fence attempt proved, and how.

    `confirmed` is the only field the failover gate reads, and it is set only by a rung
    that read back the state it claims to have produced.
    """

    def __init__(self, hostname, ip):
        self.hostname = hostname
        self.ip = ip
        self.confirmed = False
        self.method = FENCE_METHOD_NONE
        self.detail = ""
        self.steps = []
        self.at = time.time()

    def record(self, method, confirmed, detail):
        self.steps.append({"method": method, "confirmed": bool(confirmed),
                           "detail": detail})
        if confirmed and not self.confirmed:
            self.confirmed = True
            self.method = method
            self.detail = detail
        return confirmed

    def summary(self):
        if self.confirmed:
            return f"confirmed by {self.method}: {self.detail}"
        attempts = "; ".join(f"{step['method']} -> {step['detail']}" for step in self.steps)
        return "NOT confirmed: " + (attempts or "no method was attempted")

    def as_dict(self):
        return {"hostname": self.hostname, "ip": self.ip, "confirmed": self.confirmed,
                "method": self.method, "detail": self.detail, "steps": self.steps,
                "at": self.at}


# Hosts fenced during the current outage, so a fence that succeeded is not repeated on
# every pass of the control loop. Only *confirmed* fences are recorded: repeating a fence
# that failed is exactly what we want to keep doing. Cleared by clear_fence_record() when
# the host answers its health check again, because the next outage is a new episode.
FENCE_LEDGER = {}


def clear_fence_record(ip):
    """Forget a host's fence once it is healthy again; its next outage is a new one."""
    return FENCE_LEDGER.pop(ip, None) is not None


def spark_fence_host(ip):
    """In-band fence: tell the host to stop, then read back that it did.

    Returns (confirmed, detail). The typed endpoint does the work and returns the
    post-state it observed; a daemon too old to have it falls back to the legacy command
    plus an explicit verification pass, because the legacy command's exit status says
    nothing (see the note at the top of this section).
    """
    status, res, err = run_mtls_spark_api_full(ip, "/api/v1/host/fence", {"confirm": True})
    if status in (200, 409) and isinstance(res, dict) and "fenced" in res:
        if res.get("fenced"):
            return True, (res.get("detail")
                          or "spark-daemon reports no guest processes and no served "
                             "device and no Primary resource")
        return False, ("spark-daemon ran the fence and it did not take: "
                       + (res.get("detail") or json.dumps(res))[:300])

    # A 404 means the daemon on that host predates the typed fence endpoint, which
    # happens mid-upgrade. Anything else means the request did not land.
    if status == 404:
        return legacy_spark_fence(ip)
    return False, f"spark-daemon did not answer the fence request: {err or 'unknown error'}"


def legacy_spark_fence(ip):
    """The pre-typed-endpoint fence, with the verification it never had.

    Kept for a rolling upgrade, where the host being fenced may still be running the old
    spark-daemon. The command itself is unchanged; what is new is that its exit status is
    ignored and the host is asked afterwards what is actually still running.
    """
    fence_cmd = ("systemctl stop libvirtd virtqemud || true; "
                 "pkill -9 qemu-system-x86_64 || true; pkill -9 qemu || true")
    run_remote_spark(ip, fence_cmd)

    rc, stdout, stderr = run_remote_spark(ip, "pgrep -a qemu")
    if rc not in (0, 1):
        return False, ("could not read the process table on the host after the fence "
                       f"({(stderr or stdout or '').strip()[:200]}), so nothing is proven")
    if rc == 0 and stdout.strip():
        return False, ("guest processes are still running after the fence: "
                       + " | ".join(stdout.strip().splitlines()[:5]))

    rc_s, status, err_s = run_mtls_spark_api(ip, "/api/v1/dfs/vdisk", {"op": "list"})
    if rc_s != 0 or not isinstance(status, dict):
        return False, ("no qemu is left, but the host's vdisk state could not be read "
                       f"({err_s or 'unparseable response'}), so it is not proven that it "
                       "released its disks")
    held = [v.get("vdisk_id", "?") for v in (status.get("attached") or [])
            if isinstance(v, dict)]
    if held:
        return False, "the host is still serving " + ", ".join(held[:5])
    return True, "no guest process is left and the host is serving no vdisk"

def bmc_entry_for(hostname, ip, config):
    """The BMC record for a host, looked up by hostname then by address."""
    hosts = ((config.get("bmc") or {}).get("hosts") or {})
    if not isinstance(hosts, dict):
        return None
    for key in (hostname, ip):
        if key and key in hosts and isinstance(hosts[key], dict):
            entry = dict((config.get("bmc") or {}).get("defaults") or {})
            entry.update(hosts[key])
            return entry
    return None


def _bmc_password(entry):
    """(password, error). A password_file is preferred; it keeps the secret off argv."""
    path = entry.get("password_file")
    if path:
        try:
            st = os.stat(path)
        except OSError as exc:
            return None, f"password_file {path} cannot be read: {exc}"
        if not fencing_config_is_private(path):
            return None, (f"password_file {path} is mode {oct(st.st_mode & 0o777)}; "
                          "chmod 600 it before it will be used")
        try:
            with open(path, "r") as handle:
                return handle.read().strip("\r\n"), None
        except OSError as exc:
            return None, f"password_file {path} cannot be read: {exc}"
    password = entry.get("password")
    if isinstance(password, str) and password:
        return password, None
    return None, "no password or password_file in the BMC entry"


def bmc_fence_host(hostname, ip, config):
    """Out-of-band fence: power the chassis off and confirm from the BMC that it is off.

    Returns (confirmed, detail).

    Nothing here treats a zero exit status as a power-off. ipmitool returns 0 for a
    chassis command the BMC accepted and then failed to carry out, and for a session it
    opened against the wrong host entirely; the only evidence that counts is
    `chassis power status` reading off.

    The password is passed in the environment (`-E`) rather than on the command line,
    because argv is readable by every account on the host through /proc.
    """
    entry = bmc_entry_for(hostname, ip, config)
    if not entry:
        return False, (f"no BMC entry for {hostname} in {FENCING_CONFIG_PATH}; "
                       "out-of-band fencing is not configured for this host")
    address = entry.get("address")
    username = entry.get("username")
    if not address or not username:
        return False, f"the BMC entry for {hostname} has no address or no username"

    tool = shutil.which("ipmitool")
    if not tool:
        return False, ("ipmitool is not installed on this host, so the configured BMC "
                       "cannot be used")

    password, error = _bmc_password(entry)
    if error:
        return False, f"BMC credentials for {hostname} are unusable: {error}"

    base = [tool, "-I", str(entry.get("interface") or "lanplus"), "-H", str(address),
            "-U", str(username), "-E"]
    if entry.get("cipher_suite") is not None:
        base += ["-C", str(entry["cipher_suite"])]
    if entry.get("privilege_level"):
        base += ["-L", str(entry["privilege_level"])]

    saved_env = os.environ.copy()
    os.environ["IPMI_PASSWORD"] = password
    try:
        rc, stdout, stderr = run_argv_local(base + ["chassis", "power", "off"], timeout=60)
        if rc != 0:
            detail = (stderr or stdout).strip()[:200]
            return False, f"ipmitool chassis power off failed against {address}: {detail}"

        deadline = time.time() + float(entry.get("power_off_timeout_seconds") or 60)
        last = ""
        while time.time() < deadline:
            rc_s, out_s, err_s = run_argv_local(base + ["chassis", "power", "status"],
                                                timeout=30)
            last = (out_s or err_s).strip()
            if rc_s == 0 and "is off" in last.lower():
                return True, f"the BMC at {address} reports the chassis powered off"
            time.sleep(3)
    finally:
        os.environ.clear()
        os.environ.update(saved_env)

    return False, (f"the BMC at {address} accepted the power-off but never reported the "
                   f"chassis off (last answer: {last or 'no answer'}). The host is not "
                   "proven to be down.")


def parse_json_rows(stdout):
    """Rows from a `SELECT JSON` result, as dicts.

    cqlsh prints one JSON object per line with a header and a blank line around them, so
    non-object lines are skipped rather than treated as an error. A row that will not
    parse is dropped for the same reason: a single malformed line should not lose the
    whole result, and the callers here all tolerate a short list better than an exception.
    """
    rows = []
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{") or not line.endswith("}"):
            continue
        try:
            parsed = json.loads(line)
        except ValueError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def storage_fence_assert(dead_hostname, dead_ip, hosts):
    """Stop the dead host's writes, and prove it. Returns (confirmed, detail).

    This used to be an argument rather than an action. With DRBD there was no way to reach
    into an unreachable host and stop it writing its own local copy, so the rung read DRBD
    quorum and *inferred* that a host which could not see a majority was already failing
    its own I/O. The inference was sound but conditional: it needed quorum armed, it needed
    more than two nodes, and where those did not hold the ladder could confirm nothing.

    Under Sidon it is a fact instead. Every vdisk carries an (owner, epoch) pair and every
    journal append carries its writer's epoch. Raising the epoch through a compare-and-swap
    makes every replica reject appends from the old one -- the deposed host does not have
    to agree, or be reachable, or even be running. It can be wedged, lying about its own
    state, or convinced it is still the owner; its next write meets a rejection.

    So this rung works on two nodes, works with no BMC, works with no quorum, and is the
    only one that has to succeed for a failover to be safe. It needs Hydra and nothing
    else. When Hydra is unreachable the honest answer is that nothing can be fenced, which
    is what it returns.
    """
    module = sidon_module()
    if module is None:
        return False, "helios_sidon is not installed on this node, so no vdisk can be fenced"

    rc, stdout, _ = run_cql_query("SELECT JSON vdisk_id, owner, epoch FROM hydra.dfs_vdisks;")
    if rc != 0:
        return False, "hydra.dfs_vdisks could not be read, so nothing can be fenced"

    owned = []
    for row in parse_json_rows(stdout):
        owner = (row.get("owner") or "").strip()
        if owner and owner in (dead_hostname, dead_ip):
            owned.append((row.get("vdisk_id"), owner, int(row.get("epoch") or 0)))

    if not owned:
        # Nothing to fence is a confirmed fence, not a failed one: the dead host owns no
        # vdisk, so there is no write left that it could land.
        return True, f"{dead_hostname} owns no vdisk; there is nothing it can still write"

    me = local_hostname()
    fenced = []
    failures = []
    for vdisk_id, owner, epoch in owned:
        payload = {
            "vdisk_id": vdisk_id,
            "owner": me,
            "epoch": epoch + 1,
            "expected_owner": owner,
            "expected_epoch": epoch,
        }
        rc_c, body, err = run_mtls_spark_api_full("127.0.0.1", "/v1/dfs/claim", payload)
        if isinstance(body, dict) and body.get("applied") is True:
            fenced.append(vdisk_id)
            continue
        # A refused claim means somebody else already moved it on, which fences the dead
        # host just as thoroughly -- provided the owner it names is not the dead host.
        current_owner = ""
        if isinstance(body, dict):
            current_owner = str((body.get("current") or {}).get("owner") or "")
        if current_owner and current_owner not in (dead_hostname, dead_ip):
            fenced.append(vdisk_id)
        else:
            failures.append(f"{vdisk_id}: {err or 'claim refused, still owned by the dead host'}")

    if failures:
        detail = "; ".join(failures[:5])
        if len(failures) > 5:
            detail += f" (and {len(failures) - 5} more)"
        return False, "could not raise the epoch on " + detail
    return True, (f"{len(fenced)} vdisk(s) moved past {dead_hostname}'s epoch; every replica "
                  "now rejects its writes")



def fence_host(hostname, ip, hosts=None, db_status=None, config=None):
    """Fence a host and return what was actually proven. Never raises.

    A host already confirmed fenced during this outage is not fenced again -- powering
    off a chassis that is already off, or killing guests that are already dead, adds
    nothing and takes time the failover does not have. The ledger records confirmations
    only, so a fence that failed is retried on the next pass.
    """
    cached = FENCE_LEDGER.get(ip)
    if cached is not None:
        result = FenceResult(hostname, ip)
        result.confirmed = True
        result.method = cached.get("method", FENCE_METHOD_NONE)
        result.detail = cached.get("detail", "")
        result.at = cached.get("at", time.time())
        result.steps = [{"method": "ledger", "confirmed": True,
                         "detail": f"already fenced by {result.method} at "
                                   f"{time.strftime('%H:%M:%S', time.localtime(result.at))}"}]
        return result

    if config is None:
        config, warnings = load_fencing_config()
        for warning in warnings:
            print(f"[Mipha HA] FENCE CONFIG: {warning}")

    result = FenceResult(hostname, ip)
    print(f"[Mipha HA] Fencing {hostname} ({ip})...")

    if db_status == NODE_STATUS_FENCED:
        result.record(FENCE_METHOD_SELF, True,
                      "the host fenced itself and recorded it in hydra.nodes")
        FENCE_LEDGER[ip] = result.as_dict()
        return result

    # Storage first now, and that reordering is the point of the whole change. It used to
    # be last because it was the weakest rung -- an inference needing quorum armed and more
    # than two nodes. It is now the strongest and the only unconditional one: a
    # compare-and-swap in Hydra that no state of the dead host can defeat. Spark and BMC
    # stay because a wedged host still holds the VIP and still burns CPU, but they are
    # hygiene now rather than the thing data safety rests on.
    for method, call in ((FENCE_METHOD_STORAGE,
                          lambda: storage_fence_assert(hostname, ip, hosts)),
                         (FENCE_METHOD_SPARK, lambda: spark_fence_host(ip)),
                         (FENCE_METHOD_BMC, lambda: bmc_fence_host(hostname, ip, config))):
        try:
            confirmed, detail = call()
        except Exception as exc:
            confirmed, detail = False, f"raised {exc.__class__.__name__}: {exc}"
        print(f"[Mipha HA] Fence [{method}] {'CONFIRMED' if confirmed else 'no'}: {detail}")
        if result.record(method, confirmed, detail):
            FENCE_LEDGER[ip] = result.as_dict()
            return result

    return result


def failover_permitted(fence, config):
    """May the failover proceed on this fence result? (allowed, reason).

    The whole point of the ladder is this decision, so it is one function rather than a
    condition buried in the control loop.

    "block" is still the default, but it should now be nearly unreachable: the storage
    rung is a compare-and-swap in Hydra rather than an inference about a host nobody can
    reach, so the only way to arrive here is with Hydra itself unavailable. That is the one
    case where blocking is obviously right -- promoting a VM whose disk ownership cannot be
    moved is precisely the split-brain this exists to prevent.
    """
    if fence.confirmed:
        return True, f"the fence was confirmed by {fence.method}"
    policy = (config or {}).get("unconfirmed_fence_policy", "block")
    if policy == "failover":
        return True, ("unconfirmed_fence_policy is 'failover', so the failover proceeds "
                      "on a host that is not proven to have stopped writing")
    return False, fence.summary()


# ---------------------------------------------------------------------------
# Self-fencing.
#
# Mipha's cluster-side liveness is a ping and a spark-daemon reply, and both keep
# answering on a host whose storage or libvirt has died. Such a host is reported healthy,
# keeps its VMs, and is never evacuated -- the guests sit there with failing I/O while
# the cluster believes nothing is wrong.
#
# This loop runs on *every* host, not only the leader, because the failure it detects is
# local and the host is the only party that can see it.
#
# Two tiers, and the difference between them is the whole design:
#
#   quarantine  the host stops accepting new placement (hydra.nodes status DEGRADED,
#               which Vali already excludes) and keeps running what it has. This is the
#               answer for libvirt being dead: qemu keeps running and the guests keep
#               working when libvirtd dies, so killing them would be a self-inflicted
#               outage, and failing them over while they are still writing would be
#               corruption. Losing management of a working VM is not a reason to destroy
#               it.
#   fence       the host stops its guests, gives up every vdisk it serves, and
#               records itself FENCED so the leader evacuates it. This is reserved for
#               conditions under which the guests are *already* broken: the drain has
#               quorum, or the device has no usable data path. Under those conditions
#               stopping is strictly better than continuing, and -- the point -- it makes
#               the host provably safe to fail over without a BMC.
#
# The failure mode of self-fencing is a healthy host that evacuates itself on a blip, so:
# a probe that cannot determine an answer never counts toward the hard tier; the trigger
# must hold for three consecutive passes; there is a startup grace period; a host in
# maintenance is exempt; a single-node cluster never self-fences (there is nowhere for
# the guests to go, so it would be a pure outage); and the hardware-fault trigger
# additionally requires a peer that could actually take the load. Quorum loss is exempt
# from that last check because quorum *is* a majority test -- if we lost it, a majority
# exists elsewhere by definition.
# ---------------------------------------------------------------------------

SELF_FENCE_STATE = {
    "active": False,
    "quarantined": False,
    "reason": "",
    "at": 0,
    "announced": False,
    "report": {},
}


def self_fence_is_active():
    """True when this host has fenced itself, including across a Mipha restart."""
    if SELF_FENCE_STATE.get("active"):
        return True
    return os.path.exists(SELF_FENCE_MARKER)


def load_self_fence_marker():
    """Re-adopt a fence recorded before this process started.

    Without this a Mipha restart on a fenced host would claim its vdisks straight back to
    Primary, which is the fence undoing itself two seconds after it was applied.
    """
    try:
        with open(SELF_FENCE_MARKER, "r") as handle:
            data = json.load(handle)
    except Exception:
        return False
    if not isinstance(data, dict):
        return False
    SELF_FENCE_STATE.update({
        "active": True,
        "reason": data.get("reason", "recorded by an earlier fence"),
        "at": data.get("at", time.time()),
        "announced": False,
        "report": data.get("report") or {},
    })
    print(f"[Mipha Self-Fence] This host is fenced ({SELF_FENCE_STATE['reason']}). "
          "It will not take Primary or accept placement until "
          "`mipha --clear-self-fence` is run.")
    return True


def write_self_fence_marker(reason, report):
    try:
        os.makedirs(os.path.dirname(SELF_FENCE_MARKER), exist_ok=True)
        with open(SELF_FENCE_MARKER, "w") as handle:
            json.dump({"reason": reason, "at": time.time(), "report": report}, handle)
        return True
    except Exception as exc:
        sys.stderr.write(f"[Mipha Self-Fence] Could not write {SELF_FENCE_MARKER}: {exc}\n")
        return False


def probe_local_health():
    """One pass of the local subsystem probes.

    Every probe returns one of "ok", "failed" or "unknown", and "unknown" is load-bearing:
    it means the probe could not reach a verdict, and it must never escalate to the tier
    that destroys running guests. That distinction is what keeps a slow virsh or a
    momentarily unreachable Sidon from evacuating a healthy host.
    """
    probe = {"libvirt": "unknown", "storage": "unknown", "unserviceable": [],
             "detail": {}}

    if shutil.which("virsh") is None:
        probe["detail"]["libvirt"] = "virsh is not installed"
    else:
        rc, stdout, stderr = run_argv_local(
            ["virsh", "-c", "qemu:///system", "list", "--name"], timeout=15)
        if rc == 0:
            probe["libvirt"] = "ok"
        elif rc == 127:
            probe["detail"]["libvirt"] = "virsh disappeared between checks"
        else:
            probe["libvirt"] = "failed"
            probe["detail"]["libvirt"] = (stderr or stdout).strip()[:200] or f"virsh exited {rc}"

    # The storage probe asks Sidon what it is serving. A vdisk reported degraded is one
    # whose drain has failed: the guest's writes are still safe in the journal, but the
    # journal is no longer emptying, so the disk will backpressure and stop. That is the
    # local-origin equivalent of the old "Primary without quorum" -- the guest is not
    # broken yet and will be.
    module = sidon_module()
    if module is None:
        probe["detail"]["storage"] = "helios_sidon is not installed on this node"
    else:
        try:
            attached = module.list_attached(timeout=15).get("attached", [])
            probe["storage"] = "ok"
            for vdisk in attached:
                if vdisk.get("degraded"):
                    probe["unserviceable"].append({
                        "resource": vdisk.get("vdisk_id", "?"),
                        "cause": "drain-failed",
                        "detail": str(vdisk.get("degraded"))[:200],
                    })
        except Exception as exc:
            # Sidon not answering while it is meant to be serving disks is a storage-stack
            # failure, and the self-fence tiers treat it as one.
            probe["storage"] = "failed"
            probe["detail"]["storage"] = str(exc)[:200]

    return probe


def healthy_peer_exists(hosts=None):
    """True when some other host answers spark-daemon and is not itself fenced."""
    for host in hosts or get_cluster_hosts():
        ip = host.get("ip")
        if not ip or ip == LOCAL_IP:
            continue
        rc, res, _ = run_mtls_spark_api(ip, "/api/v1/node/status", None, method="GET")
        if rc == 0 and isinstance(res, dict) and res.get("ip") == ip:
            return True
    return False


def self_fence_decide(probe, counters, config, hosts, uptime_seconds):
    """(action, reason) for one pass. action is "none", "quarantine" or "fence".

    `counters` is mutated: it holds the consecutive-failure count per condition, and any
    condition that is not present this pass is reset to zero. A blip therefore has to
    survive `threshold` passes in a row to do anything, and a single good pass wipes the
    history.
    """
    settings = config.get("self_fence") or {}
    if not settings.get("enabled", True):
        counters.clear()
        return "none", "self-fencing is disabled in " + FENCING_CONFIG_PATH

    hosts = hosts if hosts is not None else get_cluster_hosts()
    if len(hosts) <= 1:
        counters.clear()
        return "none", ("this is a single-node cluster: there is nowhere for the guests "
                        "to be restarted, so fencing would be a pure outage")

    if host_is_in_maintenance():
        counters.clear()
        return "none", "this host is in maintenance"

    if uptime_seconds < float(settings.get("grace_seconds", 180)):
        counters.clear()
        return "none", "within the startup grace period"

    threshold = int(settings.get("threshold", 3))

    hard = [item for item in probe["unserviceable"]]
    counters["unserviceable"] = counters.get("unserviceable", 0) + 1 if hard else 0
    soft = [name for name in ("libvirt", "storage") if probe.get(name) == "failed"]
    for name in ("libvirt", "storage"):
        counters[name] = counters.get(name, 0) + 1 if name in soft else 0

    if hard and counters["unserviceable"] >= threshold:
        causes = sorted({item["cause"] for item in hard})
        detail = "; ".join(item["detail"] for item in hard[:5])
        if not healthy_peer_exists(hosts):
            return "quarantine", (f"local storage is unserviceable ({detail}) but no peer "
                                  "is answering, so stopping the guests here would not "
                                  "get them started anywhere else")
        return "fence", f"local storage cannot serve I/O ({detail})"

    for name in ("storage", "libvirt"):
        if counters.get(name, 0) >= threshold:
            return "quarantine", (f"{name} has failed {counters[name]} consecutive probes: "
                                  + probe["detail"].get(name, "no detail"))

    return "none", ""


def local_fence_fallback():
    """Fence this host without spark-daemon. Returns the same report shape as the API.

    Only reached when the local daemon is not answering, which is exactly the case a host
    in trouble is likely to be in. Deliberately short: kill the guests, drop the mounts
    Mipha itself created, demote everything, then report what is *still* held rather than
    what was attempted.
    """
    report = {"fenced": False, "qemu_pids": [], "primary_resources": [], "open_devices": [],
              "libvirt_active": False, "detail": ""}

    run_argv_local(["systemctl", "stop", "libvirtd", "virtqemud",
                    "libvirtd.socket", "virtqemud.socket"], timeout=60)

    def qemu_pids():
        found = []
        try:
            entries = os.listdir("/proc")
        except OSError:
            return found
        for entry in entries:
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/comm", "r") as handle:
                    if handle.read().strip().startswith("qemu"):
                        found.append(int(entry))
            except Exception:
                continue
        return found

    for _attempt in range(3):
        pids = qemu_pids()
        if not pids:
            break
        for pid in pids:
            try:
                os.kill(pid, 9)
            except Exception:
                pass
        time.sleep(2)
    report["qemu_pids"] = qemu_pids()

    for mount in FENCED_MOUNTS:
        rc, _, _ = run_argv_local(["mountpoint", "-q", mount], timeout=15)
        if rc == 0:
            run_argv_local(["umount", "-l", mount], timeout=30)

    # Give up every vdisk. Detaching drains and releases, so the next owner's claim does
    # not have to race a daemon that still believes it is serving.
    still = []
    module = sidon_module()
    if module is not None:
        try:
            for vdisk in module.list_attached(timeout=15).get("attached", []):
                vdisk_id = vdisk.get("vdisk_id")
                if not vdisk_id:
                    continue
                try:
                    module.detach(vdisk_id, timeout=30)
                except Exception:
                    still.append(vdisk_id)
        except Exception:
            still.append("sidon did not answer")
    report["held_vdisks"] = still
    report["fenced"] = not report["qemu_pids"] and not still
    report["detail"] = ("fenced locally without spark-daemon" if report["fenced"]
                        else "local fence did not take: "
                             + (f"qemu still running {report['qemu_pids']} " if report["qemu_pids"] else "")
                             + (f"still Primary on {still}" if still else ""))
    return report


def execute_self_fence(reason, hosts=None, config=None):
    """Take this host out: stop the guests, give up every vdisk, tell the cluster.

    The marker is written *first*, and it still matters even though nothing promotes
    storage on a timer any more: without it, this node's own recovery path would re-claim
    the vdisks it had just released and undo the fence within seconds.

    Returns the verification report. `fenced` false means the host is NOT safe to fail
    over -- something is still serving a vdisk -- and that is reported rather than
    smoothed.
    """
    config = config or load_fencing_config()[0]
    settings = config.get("self_fence") or {}

    SELF_FENCE_STATE.update({"active": True, "reason": reason, "at": time.time(),
                             "announced": False, "report": {}})
    write_self_fence_marker(reason, {})
    print(f"[Mipha Self-Fence] FENCING THIS HOST: {reason}")

    status, res, err = run_mtls_spark_api_full(LOCAL_IP, "/api/v1/host/fence",
                                               {"confirm": True})
    if status in (200, 409) and isinstance(res, dict) and "fenced" in res:
        report = res
    else:
        print(f"[Mipha Self-Fence] The local spark-daemon could not run the fence "
              f"({err or 'unexpected response'}); fencing directly.")
        report = local_fence_fallback()

    SELF_FENCE_STATE["report"] = report
    write_self_fence_marker(reason, report)

    if report.get("fenced"):
        print("[Mipha Self-Fence] This host holds no guest process and serves no vdisk. "
              "It is safe to restart its VMs elsewhere.")
    else:
        print("[Mipha Self-Fence] CRITICAL: the fence did not fully take -- "
              f"{report.get('detail') or json.dumps(report)[:300]}. This host is NOT safe "
              "to fail over; an operator must power it off.")

    # Leadership has to move off a host that has just admitted it cannot serve storage,
    # or nothing evacuates it: the Mipha leader does not monitor itself, and the
    # controller would stay here on top of storage that does not work. Refused below three
    # nodes, where the remaining ZooKeeper ensemble could not form a quorum either.
    if settings.get("release_zookeeper_leadership", True):
        cluster_hosts = hosts if hosts is not None else get_cluster_hosts()
        if len(cluster_hosts) >= 3:
            if is_zookeeper_leader("127.0.0.1"):
                print("[Mipha Self-Fence] Releasing ZooKeeper leadership so a healthy "
                      "node takes over coordination.")
                run_argv_local(["systemctl", "stop", "zookeeper"], timeout=60)
        else:
            print(f"[Mipha Self-Fence] Not releasing ZooKeeper leadership: "
                  f"{len(cluster_hosts)} nodes cannot keep a quorum without this one. "
                  "Coordination stays here until an operator intervenes.")

    announce_node_status(self_fence_announcement())
    return report


def self_fence_announcement():
    """The status this host should publish for its own fence.

    FENCED is a claim the leader acts on: it skips its own fencing ladder entirely and
    goes straight to restarting this host's VMs elsewhere. So it may only be published
    when the local fence actually *took*. A fence that could not stop a guest or could
    not demote a resource publishes DEGRADED instead -- the host stops receiving work,
    and the leader still has to prove the fence itself before it moves anything.
    """
    if SELF_FENCE_STATE.get("report", {}).get("fenced"):
        return NODE_STATUS_FENCED
    return NODE_STATUS_DEGRADED


def announce_node_status(target_status):
    """Move this host's hydra.nodes row to `target_status`, conditionally.

    Returns True when the row now reads `target_status`. The compare-and-swap is against
    the status just read, so a status an operator changed in the meantime is not
    clobbered by a decision made before they touched it.

    A failure here costs visibility, not safety: the host is already fenced locally
    whatever the database says, and the caller retries on the next pass.
    """
    hostname = local_hostname()
    rc, stdout, _ = run_cql_query(
        f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{hostname}';")
    current = None
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    current = json.loads(line).get("status")
                except Exception:
                    pass
    if current == target_status:
        return True
    if current is None:
        sys.stderr.write("[Mipha Self-Fence] Could not read this host's row in "
                         "hydra.nodes; will retry.\n")
        return False
    ok, applied, row, error = run_lwt("/v1/node/maintenance", {
        "hostname": hostname,
        "status": target_status,
        "maintenance_mode": False,
        "expected_status": current,
    })
    if not ok:
        sys.stderr.write(f"[Mipha Self-Fence] Could not record status {target_status} for "
                         f"{hostname}: {error}\n")
        return False
    if not applied:
        print(f"[Mipha Self-Fence] {hostname} is '{row.get('status')}', not '{current}'; "
              f"leaving it alone rather than forcing {target_status}.")
        return False
    print(f"[Mipha Self-Fence] Recorded {hostname} as {target_status}.")
    return True


def clear_self_fence(force=False):
    """`mipha --clear-self-fence`: return a fenced host to service.

    Deliberately manual. A host that destroyed its own guests had a real fault, and a
    host that returns to service by itself is a host that can take VMs and drop them
    again on a loop. The probe is re-run and printed first so the operator sees whether
    the fault is actually gone.
    """
    probe = probe_local_health()
    still_bad = probe["unserviceable"] or probe["libvirt"] == "failed" or probe["storage"] == "failed"
    print("[Mipha] Local health probe:")
    print(f"  libvirt      : {probe['libvirt']} {probe['detail'].get('libvirt', '')}".rstrip())
    print(f"  storage      : {probe['storage']} {probe['detail'].get('storage', '')}".rstrip())
    for item in probe["unserviceable"]:
        print(f"  storage      : {item['cause']} -- {item['detail']}")
    if still_bad and not force:
        print("[Mipha] The condition that fenced this host is still present. Re-run with "
              "--force to clear the fence anyway.")
        return 1

    removed = False
    try:
        os.remove(SELF_FENCE_MARKER)
        removed = True
    except FileNotFoundError:
        pass
    except OSError as exc:
        print(f"[Mipha] Could not remove {SELF_FENCE_MARKER}: {exc}")
        return 1
    SELF_FENCE_STATE.update({"active": False, "quarantined": False, "reason": "",
                             "at": 0, "announced": False, "report": {}})

    hostname = local_hostname()
    rc, stdout, _ = run_cql_query(
        f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{hostname}';")
    current = None
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    current = json.loads(line).get("status")
                except Exception:
                    pass
    if current in (NODE_STATUS_FENCED, NODE_STATUS_DEGRADED):
        ok, applied, row, error = run_lwt("/v1/node/maintenance", {
            "hostname": hostname, "status": "NORMAL", "maintenance_mode": False,
            "expected_status": current})
        if ok and applied:
            print(f"[Mipha] {hostname} is NORMAL again.")
        else:
            print(f"[Mipha] Could not return {hostname} to NORMAL "
                  f"({error or row.get('status')}); set it by hand.")

    rc_z, _, _ = run_argv_local(["systemctl", "is-active", "zookeeper"], timeout=15)
    if rc_z != 0:
        print("[Mipha] Starting ZooKeeper, which the fence stopped...")
        run_argv_local(["systemctl", "start", "zookeeper"], timeout=60)

    print("[Mipha] Fence cleared." if removed else "[Mipha] This host was not fenced.")
    return 0


def self_fence_loop():
    """The per-host watchdog. Runs everywhere, leader or not."""
    started = time.time()
    counters = {}
    config, warnings = load_fencing_config()
    for warning in warnings:
        print(f"[Mipha Self-Fence] CONFIG: {warning}")
    load_self_fence_marker()
    interval = float((config.get("self_fence") or {}).get("interval_seconds", 10))
    print("[Mipha Self-Fence] Local subsystem watchdog started.")

    clean_since = None
    while True:
        try:
            config, _ = load_fencing_config()
            settings = config.get("self_fence") or {}
            interval = float(settings.get("interval_seconds", 10))
            hosts = get_cluster_hosts()

            if self_fence_is_active():
                # Keep trying to tell the cluster, because the fence is only useful to a
                # failover once somebody else can see it.
                if not SELF_FENCE_STATE.get("announced"):
                    SELF_FENCE_STATE["announced"] = announce_node_status(
                        self_fence_announcement())
                recover_after = float(settings.get("auto_recover_after_clean_seconds", 0))
                if recover_after > 0:
                    probe = probe_local_health()
                    healthy = (not probe["unserviceable"]
                               and probe["libvirt"] != "failed"
                               and probe["storage"] != "failed")
                    clean_since = clean_since if healthy else None
                    if healthy and clean_since is None:
                        clean_since = time.time()
                    if healthy and time.time() - clean_since >= recover_after:
                        print("[Mipha Self-Fence] Local health has been clean for "
                              f"{recover_after:.0f}s; clearing the fence automatically.")
                        clear_self_fence(force=True)
                        clean_since = None
                time.sleep(interval)
                continue

            probe = probe_local_health()
            action, reason = self_fence_decide(probe, counters, config, hosts,
                                               time.time() - started)

            if action == "fence":
                execute_self_fence(reason, hosts=hosts, config=config)
            elif action == "quarantine":
                if not SELF_FENCE_STATE.get("quarantined"):
                    print(f"[Mipha Self-Fence] Quarantining this host: {reason}. It keeps "
                          "its running VMs; it stops receiving new ones.")
                    SELF_FENCE_STATE["quarantined"] = True
                    SELF_FENCE_STATE["reason"] = reason
                    SELF_FENCE_STATE["announced"] = False
                # Retried until it lands, and separately from the decision above: a
                # quarantine only takes effect once Vali can see it, and a Daruk that was
                # briefly unreachable must not leave this host silently schedulable.
                if not SELF_FENCE_STATE.get("announced"):
                    SELF_FENCE_STATE["announced"] = announce_node_status(NODE_STATUS_DEGRADED)
            elif SELF_FENCE_STATE.get("quarantined"):
                if announce_node_status("NORMAL"):
                    print("[Mipha Self-Fence] Local subsystems are healthy again; this "
                          "host has left quarantine.")
                    SELF_FENCE_STATE["quarantined"] = False
                    SELF_FENCE_STATE["announced"] = False
                    SELF_FENCE_STATE["reason"] = ""
        except Exception as exc:
            sys.stderr.write(f"[Mipha Self-Fence] Error in watchdog loop: {exc}\n")
        time.sleep(interval)


def report_fence_status():
    """`mipha --fence-status`: what fencing this host could actually perform.

    Printed rather than inferred from logs, because "we thought we had a fence" is how
    the cluster gets to the split-brain this whole file exists to avoid. No secret is
    printed -- only whether one is present and usable.
    """
    config, warnings = load_fencing_config()
    for warning in warnings:
        print(f"WARNING: {warning}")

    print(f"Fencing configuration : {FENCING_CONFIG_PATH}"
          + ("" if os.path.exists(FENCING_CONFIG_PATH) else " (absent -- defaults in use)"))
    print(f"Unconfirmed fence     : {config['unconfirmed_fence_policy']}")

    hosts = get_cluster_hosts()
    print(f"\nOut-of-band (BMC) coverage, {len(hosts)} host(s) in the cluster:")
    if shutil.which("ipmitool") is None:
        print("  ipmitool is NOT installed on this host: no BMC fence can run from here.")
    for host in hosts:
        hostname = host.get("hostname") or "?"
        entry = bmc_entry_for(hostname, host.get("ip"), config)
        if not entry:
            print(f"  {hostname:<24} no BMC entry -- power fencing unavailable")
            continue
        password, error = _bmc_password(entry)
        state = "usable" if password else f"UNUSABLE ({error})"
        print(f"  {hostname:<24} {entry.get('address')} as {entry.get('username')} -- {state}")

    # The storage rung needs no arming and no diagnosis any more, so this reports what
    # it can do rather than what must be configured before it works. It used to print,
    # per resource, whether DRBD quorum was armed -- and where it was not, the two
    # linstor commands to run, plus the caveat that a resource with fewer than three
    # nodes could not be armed at all and so had no storage fence whatsoever.
    print("\nStorage fencing:")
    if sidon_module() is None:
        print("  UNAVAILABLE -- helios_sidon is not installed, so no vdisk can be fenced.")
    else:
        rc_v, stdout_v, _ = run_cql_query(
            "SELECT JSON vdisk_id, owner FROM hydra.dfs_vdisks;")
        if rc_v != 0:
            print("  UNAVAILABLE -- hydra.dfs_vdisks could not be read. The fence is a "
                  "compare-and-swap in Hydra, so it needs Hydra and nothing else.")
        else:
            rows = parse_json_rows(stdout_v)
            here = local_hostname()
            mine = sum(1 for r in rows if (r.get("owner") or "") == here)
            print(f"  ARMED -- {len(rows)} vdisk(s) in the map, {mine} owned by this host.")
            print("  Fencing a host raises the epoch on the vdisks it owns; every "
                  "replica then rejects its writes.")
            print("  This works on two nodes, with no BMC, and against a host that is "
                  "wedged or unreachable. There is nothing to arm.")

    settings = config.get("self_fence") or {}
    print(f"\nSelf-fencing          : {'enabled' if settings.get('enabled', True) else 'disabled'}"
          f" (threshold {settings.get('threshold', 3)} x {settings.get('interval_seconds', 10)}s)")
    if self_fence_is_active():
        print(f"  THIS HOST IS FENCED  : {SELF_FENCE_STATE.get('reason') or 'see the marker file'}")
        print("  Clear it with        : mipha --clear-self-fence")
    return 0

# ---------------------------------------------------------------------------
# Ring lifecycle.
#
# Two jobs live here, and only one of them acts.
#
# The first is keeping the cluster maintenance lock alive. Vali takes it when a host
# starts draining and gives it back when that host has finished rejoining, which can be
# hours apart -- far longer than any TTL short enough to be useful when the holder dies.
# Mipha already runs a ten second control loop on the leader and already knows which host
# is in maintenance, so it renews the lock for as long as hydra.nodes says the host is
# still transitioning. That is what lets the TTL stay at five minutes.
#
# The second is reporting, not acting. When a node is gone for good its ScyllaDB is still
# a ring member holding token ranges that nobody serves, and every QUORUM operation keeps
# counting it. Nutanix detaches such a node from the ring automatically; Helios does not,
# and deliberately: `nodetool removenode` and `nodetool decommission` stream data, run
# unbounded, cannot be undone, and are wrong to trigger from a health check that has been
# failing for thirty seconds -- a network partition looks exactly the same from here. So
# the ring state is surfaced with the command that would fix it, and a human runs it.
# See docs/ring_lifecycle.md.

MAINTENANCE_LOCK_NAME = "cluster-maintenance"
MAINTENANCE_LOCK_TTL_SECONDS = 300

# hydra.nodes states in which a host still holds the maintenance lock. RECOVERING is
# included because a host that has left maintenance but has not finished resyncing its
# storage is not yet a replica anyone should count on, and no second host should start
# draining until it is.
MAINTENANCE_LOCK_STATES = ("ENTERING_MAINTENANCE", "IN_MAINTENANCE", "RECOVERING")

DARUK_URL = "http://127.0.0.1:9043"


def run_lwt(endpoint, params, timeout=15):
    """Call one of Daruk's typed compare-and-swap endpoints.

    Returns `(ok, applied, current, error)`. A refused compare-and-swap is
    `(True, False, {...}, "")` -- a lost race, not a failure. Collapsing the two would
    make a lock this node does not hold look like an error, and, worse, make an error
    look like a lock it does hold.

    Deliberately no cqlsh fallback: that path can run statement text but cannot report
    whether a condition held, and a lock renewal that cannot be made conditional must not
    be made at all.
    """
    import urllib.error
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


def read_maintenance_lock():
    """The cluster maintenance lock row as it stands, or {} if nobody holds it."""
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


def renew_maintenance_lock_for(hostname):
    """Extend the lock's TTL, but only if `hostname` is the holder of record.

    The token is read rather than remembered: this process did not acquire the lock and
    may not even be the process that will release it. Reading it and then renewing
    conditionally on it is still exact -- if the lock changed hands between the read and
    the renew, the token no longer matches and the renew is refused rather than stealing
    somebody else's lock and extending it in this host's name.
    """
    lock = read_maintenance_lock()
    if not lock or lock.get("holder") != hostname:
        return False
    token = lock.get("holder_token") or ""
    if not token:
        return False
    ok, applied, _current, error = run_lwt("/v1/lock/renew", {
        "name": MAINTENANCE_LOCK_NAME,
        "holder": hostname,
        "holder_token": token,
        "reason": lock.get("reason") or f"{hostname} is in maintenance",
        "acquired_at_ms": int(time.time() * 1000),
        "ttl_seconds": MAINTENANCE_LOCK_TTL_SECONDS,
    })
    if not ok:
        sys.stderr.write(
            f"[Mipha HA] Could not renew the cluster maintenance lock for {hostname}: {error}\n")
    return applied


def release_orphaned_vm(vm_name, dead_host_ip):
    """Unplace a VM stranded on a host that died, conditional on it still being there.

    Returns True only when this failover now owns the recovery of `vm_name`, so a caller
    that gets False must not go on to start it.

    SSH fencing and three consecutive failed health checks make a live host here unlikely,
    not impossible -- and the write it guards was unconditional. The VM list was read
    seconds before this runs, and in that window the guest can have been recovered
    elsewhere: by a previous failover pass whose start task only just landed, by an
    operator, or by a Vali start that was already in flight. A blind
    `SET state='Stopped', host_ip=''` then unplaces a *running* VM, and the start task
    that follows boots a second copy of it against the same vdisk -- two qemu
    processes on one raw device, which is the corruption failover exists to prevent.

    `IF host_ip = '<dead ip>'` scopes the reset to the host that actually died. A refusal
    is not an error: it means the VM is somewhere else and needs nothing from us.
    """
    ok, applied, current, error = run_lwt("/v1/vm/release", {
        "name": vm_name,
        "expected_host_ip": dead_host_ip,
    })
    if not ok:
        # Daruk is unreachable or the write failed. Do not fall back to an unconditional
        # reset and do not start the VM: we no longer know who owns it, and guessing is
        # how both sides of a partition come to own the same guest. The loop retries in
        # ten seconds; a VM that stays down for one more pass is recoverable, a VM started
        # twice is not.
        print(f"[Mipha HA] ERROR: Could not release '{vm_name}' from {dead_host_ip}: "
              f"{error}. Leaving it placed and skipping it this pass.")
        return False
    if not applied:
        print(f"[Mipha HA] VM '{vm_name}' is no longer on the dead host "
              f"(host_ip is now '{current.get('host_ip')}'). Already recovered elsewhere; "
              f"leaving it alone.")
        return False
    return True


_HOST_ID_RE = re.compile(
    r"\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\Z")


def parse_nodetool_status(text):
    """Ring members from `nodetool status`, as {address, status, state, host_id}.

    The first column is two characters: U/D for up or down, then N/L/J/M for normal,
    leaving, joining or moving. A member is only a replica that can answer a query when
    it is both -- `UJ` has not finished streaming in and `UL` is streaming out.

    The host id is matched by shape, not by column index: `Load` occupies two fields
    ("2.38 MB") or one ("?"), so the columns after it shift from node to node -- and the
    host id is the argument `nodetool removenode` takes.
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


def report_ring_detach_candidate(hostname, ip):
    """Say whether a host that just failed over is still counted as a ScyllaDB replica.

    A failed-over host is out of the VM scheduler immediately, because hydra.nodes says
    DOWN. Its ScyllaDB is not out of anything: the ring still assigns it token ranges and
    every QUORUM operation still counts it toward the replicas it needs. On a three node
    RF=3 cluster that is the difference between "one node is down" and "the next node to
    go down takes the cluster with it" -- which is exactly the state the maintenance
    quorum gate will refuse to add to, without ever explaining why.

    This does not detach anything. See the note at the top of this section.
    """
    rc, stdout, stderr = run_remote_spark(LOCAL_IP, "nodetool status")
    if rc != 0:
        sys.stderr.write(f"[Mipha HA] Could not read the ScyllaDB ring: "
                         f"{(stderr or stdout or '').strip()[:200]}\n")
        return
    members = parse_nodetool_status(stdout)
    target = next((m for m in members if m["address"] == ip), None)
    if target is None:
        print(f"[Mipha HA] Host {hostname} ({ip}) is not a ScyllaDB ring member; "
              "nothing to detach.")
        return
    up = sum(1 for m in members if m["available"])
    print(f"[Mipha HA] RING: {hostname} ({ip}) is still a ring member, reported "
          f"'{target['status']}{target['state']}'. {up} of {len(members)} members are up.")
    print(f"[Mipha HA] RING: the ring still assigns it token ranges, so QUORUM keeps "
          f"counting it and maintenance on any other host will be refused while it is down.")
    print(f"[Mipha HA] RING: if this host is coming back, do nothing. If it is gone for "
          f"good, detach it deliberately -- 'cluster decommission -s {ip}' explains the "
          f"sequence. Automatic detach is not done here: from a health check a dead node "
          f"and a partitioned one look identical, and removenode cannot be undone.")

# ---------------------------------------------------------------------------
# Scheduled storage auto-heal.
#
# Invoked by the Dagur cron entry as `mipha --auto-heal`. This is deliberately a
# subcommand on the existing daemon rather than a separate service: the storage healing
# logic already lives here, and a second owner of storage state would race this loop.
# It also avoids a fifth place for a component to drift out of (provision embedding,
# sync_provision mapping, upgrade package, LCM inventory, deploy_updates).
#
# What belongs here is the slow work that must not run in a liveness loop: verify
# scrubs, capacity reporting, and detecting under-replicated resources.


def _heal_log(msg):
    print(f"[AutoHeal] {msg}", flush=True)


def auto_heal_scrub():
    """Run a scrub pass over every sealed extent group.

    `drbdadm verify` used to checksum peers against each other and mark differing blocks
    out-of-sync for the next resync to repair. Purah's scrub is the same idea against a
    better reference: a sealed extent group is immutable, so its hash was taken when the
    data was known good and any difference is damage rather than drift. It needs no peer
    and no lock, which is one of the things sealing buys.

    Still nightly rather than in the 10-second loop: it reads every byte on the node.
    """
    module = sidon_module()
    if module is None:
        _heal_log("helios_sidon is not installed; skipping scrub pass.")
        return 0
    try:
        report = module.call("purah-scrub", timeout=3600)
    except Exception as exc:
        _heal_log(f"scrub pass failed: {exc}")
        return 1
    checked = report.get("checked", 0)
    mismatched = report.get("mismatched") or []
    missing = report.get("missing") or []
    if not mismatched and not missing:
        _heal_log(f"scrub pass clean: {checked} sealed extent group(s) verified.")
        return 0
    for eg in mismatched:
        _heal_log(f"SCRUB FAILURE: extent group {eg} no longer matches its seal hash.")
    for eg in missing:
        _heal_log(f"SCRUB FAILURE: extent group {eg} is referenced but its file is gone.")
    return len(mismatched) + len(missing)

def auto_heal_report_capacity():
    """Report extent-store usage, flagging a node that is close to full.

    This used to parse `linstor --machine-readable storage-pool list` and derive thin-pool
    overcommit from it. Sidon answers with bytes from the filesystem holding the extents,
    which is the only number that decides whether a drain can complete.
    """
    module = sidon_module()
    if module is None:
        _heal_log("helios_sidon is not installed; skipping capacity report.")
        return 0
    try:
        cap = module.capacity(timeout=30)
    except Exception as exc:
        _heal_log(f"Could not read extent-store capacity: {exc}")
        return 0
    total = int(cap.get("total_bytes") or 0)
    avail = int(cap.get("available_bytes") or 0)
    if total <= 0:
        _heal_log("Extent store reported no capacity.")
        return 0
    used_pct = 100.0 * (total - avail) / total
    gib = 1024 ** 3
    _heal_log(f"Extent store: {(total - avail) / gib:.1f} of {total / gib:.1f} GiB used "
              f"({used_pct:.1f}%), {cap.get('egroup_count', 0)} extent group(s), "
              f"{int(cap.get('journal_bytes') or 0) / gib:.2f} GiB of journal.")
    if used_pct >= 95:
        _heal_log("CRITICAL: the extent store is nearly full. Drains will fail and guest "
                  "writes will backpressure once the journal cannot empty.")
        return 1
    if used_pct >= 80:
        _heal_log("WARNING: the extent store is over 80% full.")
        return 1
    return 0


def run_auto_heal():
    """Entry point for `mipha --auto-heal`. Exit code 0 = clean, 1 = attention needed."""
    _heal_log("Starting scheduled storage auto-heal pass.")
    issues = 0
    for step, fn in (("scrub", auto_heal_scrub),
                     ("capacity report", auto_heal_report_capacity)):
        try:
            issues += fn()
        except Exception as exc:
            _heal_log(f"{step} raised: {exc}")
            issues += 1
    if issues:
        _heal_log(f"Completed with {issues} item(s) needing attention.")
        return 1
    _heal_log("Completed cleanly.")
    return 0


def main():
    print("Mipha High-Availability Host Monitor and VM Failover Coordinator started.")
    
    # The local subsystem watchdog runs on every host, not only the ZooKeeper leader:
    # the failure it exists to catch -- storage or libvirt dead while the network keeps
    # answering -- is invisible from anywhere else in the cluster.
    threading.Thread(target=self_fence_loop, daemon=True).start()

    # Track consecutive failures per host IP
    consecutive_failures = {}
    # Hosts whose self-fence this leader has already acted on. A self-fenced host keeps
    # answering its health check, so without this the failover would re-run every pass.
    self_fence_handled = set()

    while True:
        try:
            # 1. Leadership Check
            if not is_zookeeper_leader("127.0.0.1"):
                # I am a follower, reset trackers and idle
                consecutive_failures.clear()
                time.sleep(10)
                continue
                
            hosts = get_cluster_hosts()
            if not hosts:
                time.sleep(10)
                continue
                
            # Filter out local host from checking itself
            target_hosts = [h for h in hosts if h.get("ip") != LOCAL_IP]
            
            for h in target_hosts:
                ip = h.get("ip")
                hostname = h.get("hostname")
                if not ip:
                    continue
                    
                # Query node's current status in ScyllaDB
                cql_status = f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{hostname}';"
                rc_s, stdout_s, _ = run_cql_query(cql_status)
                db_status = "NORMAL"
                if rc_s == 0 and stdout_s:
                    for line in stdout_s.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                node_obj = json.loads(line)
                                db_status = node_obj.get("status", "NORMAL")
                            except:
                                pass

                # Keep the cluster maintenance lock alive for whichever host is mid
                # transition. Vali takes it and gives it back, but the window between
                # those two events is an operator's maintenance window, not a task's
                # runtime, so nothing inside Vali is still running to renew it. Without
                # this the lock's TTL would have to cover the longest maintenance anyone
                # ever performs, which is the same TTL that leaves a dead holder's lock
                # standing for hours.
                if db_status in MAINTENANCE_LOCK_STATES:
                    renew_maintenance_lock_for(hostname)

                if db_status in ["IN_MAINTENANCE", "ENTERING_MAINTENANCE"]:
                    consecutive_failures[ip] = 0
                    continue

                # 2. Run Health checks on host
                ping_ok = ping_host(ip)
                spark_ok = False
                
                # Check Spark Daemon API
                rc, res, _ = run_mtls_spark_api(ip, "/api/v1/node/status", None, method="GET")
                if rc == 0 and res.get("ip") == ip:
                    # Verify if host is in maintenance mode
                    if res.get("maintenance_status") == "IN_MAINTENANCE":
                        # If in maintenance, skip monitoring
                        consecutive_failures[ip] = 0
                        continue
                    spark_ok = True
                
                # Host is down if spark-daemon is unresponsive
                if not spark_ok:
                    consecutive_failures[ip] = consecutive_failures.get(ip, 0) + 1
                    print(f"[Mipha HA] Host {hostname} ({ip}) health check failed (Count: {consecutive_failures[ip]}/3)")
                else:
                    consecutive_failures[ip] = 0

                    # The host is answering again, so whatever it was fenced for is over.
                    # Forget the confirmation: the next outage has to be proven on its own
                    # evidence, not on a fence that succeeded an hour ago. A host still
                    # marked FENCED is excepted -- it answers precisely because it fenced
                    # itself and is still fenced.
                    if db_status != NODE_STATUS_FENCED and clear_fence_record(ip):
                        print(f"[Mipha HA] Host {hostname} ({ip}) is answering again; "
                              "its fence record is cleared.")

                    # If host was previously marked DOWN, initiate rejoin/sync sequence
                    if db_status == "DOWN":
                        print(f"[Mipha HA] Host {hostname} ({ip}) is back online! Starting rejoin sequence...")
                        
                        # A1. Set host status to RECOVERING
                        cql_recovering = f"UPDATE hydra.nodes SET status = 'RECOVERING' WHERE hostname = '{hostname}';"
                        run_cql_query(cql_recovering)
                        
                        # A2. Create parent join task in Catalyst
                        parent_task_id = str(uuid.uuid4())
                        now_ms = int(time.time() * 1000)
                        parent_payload = json.dumps({"hostname": hostname, "ip": ip})
                        cql_parent = f"""
                        INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
                        VALUES ({parent_task_id}, 'mipha', 'host_join', 'processing', '{parent_payload.replace("'", "''")}', 10, {now_ms}, {now_ms});
                        """
                        run_cql_query(cql_parent)
                        
                        # B. Start all hypervisor services on the returning host
                        print(f"[Mipha HA] Starting all services on returning host {hostname}...")
                        start_cmd = "systemctl start zookeeper hydra-db sidon spectrum bifrost dagur mimir vali catalyst gatoway logos mipha"
                        run_remote_spark(ip, start_cmd)
                        
                        # Sleep 10 seconds to allow services (especially Aether/storage) to boot
                        time.sleep(10)
                        
                        # Update parent task progress to 20%
                        cql_up = f"UPDATE hydra.catalyst_tasks SET progress = 20, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                        run_cql_query(cql_up)
                        
                        # C. There is no resync to wait for.
                        #
                        # This used to create a child Catalyst task, poll
                        # get_linstor_pending_sync() every three seconds, and hold the
                        # rejoin open until DRBD had finished copying. Extent groups are
                        # immutable and re-replicated by Purah in the background, so a
                        # returning node has nothing to catch up on before it is usable: it
                        # can serve any vdisk it is given the moment it is up, and
                        # under-replicated groups are restored off the hot path.
                        now_ms_end = int(time.time() * 1000)
                        run_cql_query(
                            f"UPDATE hydra.catalyst_tasks SET status = 'completed', "
                            f"progress = 100, updated_at = {now_ms_end} "
                            f"WHERE task_id = {parent_task_id};")
                        run_cql_query(
                            f"UPDATE hydra.nodes SET status = 'NORMAL' "
                            f"WHERE hostname = '{hostname}';")
                        print(f"[Mipha HA] Host {hostname} rejoined; Purah will restore "
                              f"replica counts in the background.")

                # 3. Trigger failover.
                #
                # Two triggers. The first is the original one, three consecutive failed
                # health checks. The second is a host that fenced *itself*: its storage
                # or its hypervisor died while its network kept answering, so it never
                # misses a health check and the old code left its VMs stopped on a host
                # nobody was evacuating. `self_fence_handled` keeps that second trigger
                # from re-running every ten seconds for as long as the host stays FENCED.
                self_fenced = (db_status == NODE_STATUS_FENCED)
                if not self_fenced:
                    self_fence_handled.discard(ip)
                if consecutive_failures.get(ip, 0) >= 3 or (self_fenced and ip not in self_fence_handled):
                    if self_fenced:
                        print(f"[Mipha HA] Host {hostname} ({ip}) has fenced itself. "
                              "Starting failover orchestration...")
                        self_fence_handled.add(ip)
                    else:
                        print(f"[Mipha HA] Host {hostname} ({ip}) confirmed OFFLINE! Starting failover orchestration...")
                    consecutive_failures[ip] = 0 # Reset counter to avoid loop

                    # A0. The parent task is created before the fence, not after it, so
                    # a failover that is *refused* is as visible in the UI as one that
                    # runs. A refusal that only appears in the journal is a refusal
                    # nobody acts on.
                    parent_task_id = str(uuid.uuid4())
                    now_ms = int(time.time() * 1000)
                    parent_payload = json.dumps({"hostname": hostname})
                    cql_parent = f"""
                    INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
                    VALUES ({parent_task_id}, 'mipha', 'failover', 'processing', '{parent_payload.replace("'", "''")}', 0, {now_ms}, {now_ms});
                    """
                    run_cql_query(cql_parent)

                    # A0b. Fence, and require proof.
                    #
                    # The old code fenced only when the host still answered ping and then
                    # threw the answer away, so a host that had stopped answering ping was
                    # assumed dead on no evidence -- the exact case the fence exists for.
                    # The fence now always runs and its result decides whether anything
                    # else happens.
                    print(f"[Mipha HA] Host {hostname} ping={'up' if ping_ok else 'down'}, "
                          f"spark={'up' if spark_ok else 'down'}.")
                    fence = fence_host(hostname, ip, hosts=hosts, db_status=db_status)
                    print(f"[Mipha HA] Fence for {hostname}: {fence.summary()}")

                    fence_config, _ = load_fencing_config()
                    allowed, why = failover_permitted(fence, fence_config)
                    if not allowed:
                        # Marking it DOWN is safe whatever the fence did: it only stops
                        # Vali placing *new* work there. It is the restart of the old
                        # host's guests elsewhere that the fence gates.
                        run_lwt("/v1/node/maintenance", {
                            "hostname": hostname, "status": "DOWN",
                            "maintenance_mode": False, "expected_status": db_status})
                        err_msg = ("HA did not fail this host over because the fence "
                                   "could not be confirmed. " + why)
                        print(f"[Mipha HA] CRITICAL: {err_msg}")
                        print("[Mipha HA] Its VMs are left placed on it. Restarting them "
                              "elsewhere now could put two writers on one vdisk. "
                              "Power the host off, or configure a BMC in "
                              f"{FENCING_CONFIG_PATH} -- see "
                              "docs/fencing.md -- then this proceeds by itself.")
                        end_ms = int(time.time() * 1000)
                        run_cql_query(f"""
                        UPDATE hydra.catalyst_tasks
                        SET status = 'failed', progress = 100,
                            error_msg = '{err_msg.replace("'", "''")[:900]}',
                            updated_at = {end_ms}
                        WHERE task_id = {parent_task_id};
                        """)
                        continue
                    if not fence.confirmed:
                        print(f"[Mipha HA] WARNING: {why}. If the host is still running "
                              "its guests, this can corrupt their disks.")

                    # A. Mark Host as DOWN in ScyllaDB
                    #
                    # Keyed on `hostname`, which is the partition key. This was written
                    # `WHERE ip = ...` and `ip` is a plain column, so Scylla rejected the
                    # statement outright ("Cannot execute this query as it might involve
                    # data filtering") -- and run_cql_query's rc=1 was never read. A host
                    # that died has therefore never actually been marked DOWN, which left
                    # Vali free to keep scheduling VMs onto it.
                    #
                    # Conditional on the status this pass read, so a host an operator moved
                    # into maintenance in the meantime is not dragged back out by a
                    # failover decision that was made before they touched it. A refusal is
                    # not a reason to abandon the failover: the per-VM release below is
                    # what actually keeps two hosts off one disk.
                    #
                    # A host that fenced *itself* is left FENCED rather than moved to
                    # DOWN. DOWN is the state Mipha's rejoin path watches, and a
                    # self-fenced host has never stopped answering, so marking it DOWN
                    # would have it "rejoin" on the next pass -- starting its services
                    # and taking Primary again on the storage it just gave up.
                    if self_fenced:
                        print(f"[Mipha HA] Leaving {hostname} in {NODE_STATUS_FENCED}; it "
                              "returns to service through `mipha --clear-self-fence` on "
                              "that host.")
                    else:
                        print(f"[Mipha HA] Marking host {hostname} status as DOWN in metadata store...")
                        ok_down, applied_down, current_down, err_down = run_lwt(
                            "/v1/node/maintenance", {
                                "hostname": hostname,
                                "status": "DOWN",
                                "maintenance_mode": False,
                                "expected_status": db_status,
                            })
                        if not ok_down:
                            print(f"[Mipha HA] WARNING: could not mark {hostname} DOWN: {err_down}")
                        elif not applied_down:
                            print(f"[Mipha HA] {hostname} was not '{db_status}' any more "
                                  f"(it is '{current_down.get('status')}'); leaving its status alone.")

                    # Marking it DOWN takes it out of the VM scheduler. It does not take
                    # it out of the ScyllaDB ring, which is a separate lifecycle with its
                    # own consequences -- say what those are rather than leaving the
                    # cluster quietly one failure away from losing quorum.
                    report_ring_detach_candidate(hostname, ip)

                    # B. Active Polling for ZooKeeper Recovery
                    print("[Mipha HA] Waiting for ZooKeeper cluster consensus to settle...")
                    zk_leader_ip = None
                    for i in range(15): # Max 30 seconds polling
                        # Update progress: 10% to 25% during ZK wait
                        zk_progress = int(10 + (i / 15.0) * 15)
                        cql_up = f"UPDATE hydra.catalyst_tasks SET progress = {zk_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                        run_cql_query(cql_up)
                        
                        zk_leader_ip = get_zookeeper_leader_ip(hosts)
                        if zk_leader_ip:
                            print(f"[Mipha HA] ZooKeeper leader resolved at {zk_leader_ip}.")
                            break
                        time.sleep(2)
                        
                    if not zk_leader_ip:
                        print("[Mipha HA] ERROR: Failed to resolve ZooKeeper leader. Proceeding failover using local context.")
                        zk_leader_ip = LOCAL_IP
                        
                    # C. Active Polling for Vali Recovery
                    print("[Mipha HA] Verifying Vali VM Manager status...")
                    vali_ok = False
                    for i in range(10): # Check if Vali is responsive
                        # Update progress: 30% to 45% during Vali check
                        val_progress = int(30 + (i / 10.0) * 15)
                        cql_up = f"UPDATE hydra.catalyst_tasks SET progress = {val_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                        run_cql_query(cql_up)
                        
                        if check_vali_health(zk_leader_ip):
                            vali_ok = True
                            print("[Mipha HA] Vali VM Manager is active and responding.")
                            break
                        time.sleep(2)
                        
                    if not vali_ok:
                        # Vali is down, trigger active restart on all surviving hosts
                        print("[Mipha HA] Vali is unresponsive. Initiating remote restart across surviving hosts...")
                        surviving_hosts = [sh.get("ip") for sh in hosts if sh.get("ip") != ip]
                        for sh_ip in surviving_hosts:
                            run_remote_spark(sh_ip, "systemctl restart vali")
                            
                        # Active polling loop for Vali startup
                        print("[Mipha HA] Polling Vali API for recovery status...")
                        for i in range(15):
                            # Update progress: 50% to 65% during Vali startup
                            val_progress = int(50 + (i / 15.0) * 15)
                            cql_up = f"UPDATE hydra.catalyst_tasks SET progress = {val_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_up)
                            
                            if check_vali_health(zk_leader_ip):
                                vali_ok = True
                                print("[Mipha HA] Vali recovered and back online.")
                                break
                            time.sleep(2)
                            
                    if not vali_ok:
                        print("[Mipha HA] WARNING: Vali remains unresponsive. Proceeding with database orchestration.")
                        
                    # D. Query dead host's VMs in ScyllaDB
                    print(f"[Mipha HA] Scanning ScyllaDB for active VMs hosted on dead node {ip}...")
                    cql_up = f"UPDATE hydra.catalyst_tasks SET progress = 70, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                    run_cql_query(cql_up)
                    
                    cql_vms = "SELECT JSON name, memory, host_ip, state FROM hydra.vms;"
                    rc_v, stdout_v, _ = run_cql_query(cql_vms)
                    orphaned_vms = []
                    if rc_v == 0 and stdout_v:
                        for line in stdout_v.splitlines():
                            line = line.strip()
                            if line.startswith("{") and line.endswith("}"):
                                try:
                                    vm = json.loads(line)
                                    if vm.get("host_ip") == ip and vm.get("state") == "Running":
                                        orphaned_vms.append(vm)
                                except Exception:
                                    pass
                                    
                    if not orphaned_vms:
                        print(f"[Mipha HA] No running virtual machines found on dead node {ip}. Failover complete.")
                        now_ms_end = int(time.time() * 1000)
                        cql_parent_end = f"""
                        UPDATE hydra.catalyst_tasks 
                        SET status = 'completed', progress = 100, updated_at = {now_ms_end}
                        WHERE task_id = {parent_task_id};
                        """
                        run_cql_query(cql_parent_end)
                        continue
                        
                    print(f"[Mipha HA] Found {len(orphaned_vms)} orphaned VMs: {[v['name'] for v in orphaned_vms]}")
                    
                    # E. Failover VMs
                    submitted_tasks = []
                    for vm in orphaned_vms:
                        vm_name = vm.get("name")
                        print(f"[Mipha HA] Recovering VM '{vm_name}'...")
                        
                        # Give the placement back so Vali will allow a fresh start. If the
                        # VM is not on the dead host any more, it needs nothing from this
                        # failover and must not be started a second time.
                        if not release_orphaned_vm(vm_name, ip):
                            continue

                        # Submit task to Catalyst queue to start the VM.
                        # target_host is left empty so Vali schedules it on the best surviving node.
                        task_payload = {"vm_name": vm_name, "target_host": "", "parent_task_id": parent_task_id}
                        sub_task_id = submit_catalyst_task(zk_leader_ip, "vali", "start", task_payload)
                        if sub_task_id:
                            print(f"[Mipha HA] Successfully submitted failover task {sub_task_id} for '{vm_name}' to Catalyst.")
                            submitted_tasks.append((vm_name, sub_task_id))
                        else:
                            print(f"[Mipha HA] ERROR: Failed to submit failover task for '{vm_name}' to Catalyst.")
                            
                    # Wait/poll for the VM start tasks to finish
                    if submitted_tasks:
                        print(f"[Mipha HA] Polling {len(submitted_tasks)} VM start tasks for completion...")
                        finished_tasks = set()
                        # Poll up to 60 seconds (30 iterations of 2 seconds)
                        for iteration in range(30):
                            for vm_name, sub_task_id in submitted_tasks:
                                if sub_task_id in finished_tasks:
                                    continue
                                
                                cql_check = f"SELECT JSON status FROM hydra.catalyst_tasks WHERE task_id = {sub_task_id};"
                                rc_c, stdout_c, _ = run_cql_query(cql_check)
                                if rc_c == 0 and stdout_c:
                                    for line in stdout_c.splitlines():
                                        line = line.strip()
                                        if line.startswith("{") and line.endswith("}"):
                                            try:
                                                task_status_obj = json.loads(line)
                                                sub_status = task_status_obj.get("status")
                                                if sub_status in ["completed", "failed"]:
                                                    finished_tasks.add(sub_task_id)
                                                    print(f"[Mipha HA] Subtask {sub_task_id} for VM '{vm_name}' finished with status: {sub_status}")
                                            except Exception:
                                                pass
                            
                            # Update parent task progress: 70% to 95% depending on finished VM startups
                            num_finished = len(finished_tasks)
                            pct_finished = num_finished / len(submitted_tasks)
                            parent_progress = int(70 + pct_finished * 25)
                            cql_up = f"UPDATE hydra.catalyst_tasks SET progress = {parent_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_up)
                            
                            if len(finished_tasks) == len(submitted_tasks):
                                print("[Mipha HA] All VM start tasks have finished.")
                                break
                            time.sleep(2)
                            
                    # F. Mark parent task as completed in Catalyst
                    now_ms_end = int(time.time() * 1000)
                    cql_parent_end = f"""
                    UPDATE hydra.catalyst_tasks 
                    SET status = 'completed', progress = 100, updated_at = {now_ms_end}
                    WHERE task_id = {parent_task_id};
                    """
                    run_cql_query(cql_parent_end)
                    print("[Mipha HA] Failover recovery orchestration completed.")
                    
        except Exception as e:
            sys.stderr.write(f"Error in Mipha HA control loop: {e}\n")
            
        time.sleep(10)

if __name__ == "__main__":
    # `mipha --auto-heal` runs the nightly storage pass and exits; with no arguments the
    # HA daemon runs as normal.
    if "--auto-heal" in sys.argv:
        sys.exit(run_auto_heal())
    # `mipha --fence-status` reports what fencing this host could actually perform, and
    # `mipha --clear-self-fence` returns a host that fenced itself to service. Both are
    # subcommands on the daemon rather than a separate tool for the same reason
    # --auto-heal is: the state they read and write lives in this process.
    if "--fence-status" in sys.argv:
        sys.exit(report_fence_status())
    if "--clear-self-fence" in sys.argv:
        sys.exit(clear_self_fence(force="--force" in sys.argv))
    main()
