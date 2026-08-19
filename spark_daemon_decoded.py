#!/usr/bin/env python3
__build__ = "1.2.2"
import sys
import os
import ssl
import json
import subprocess
import socket
import urllib.request
import threading
import time
import base64
import re
import stat
import glob
import tempfile
import urllib.parse
import xml.etree.ElementTree as ET
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

socket.setdefaulttimeout(45.0)

PORT = 9099

def get_service_build_number(target_path):
    if not os.path.exists(target_path):
        return "Not Installed"
    try:
        with open(target_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                cleaned = line.strip()
                if cleaned.startswith("#"):
                    cleaned = cleaned[1:].strip()
                if cleaned.startswith("__build__") and "=" in line:
                    parts = line.split("=", 1)
                    val = parts[1].strip().strip("'\"")
                    return val
    except Exception:
        pass
    return "Unknown"

def run_remote_spark(ip, command):
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/etc/hci/spark/certs/ca.crt")
    context.load_cert_chain(certfile="/etc/hci/spark/certs/node.crt", keyfile="/etc/hci/spark/certs/node.key")
    context.check_hostname = False
    
    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        # Use a longer timeout for orchestration tasks
        with urllib.request.urlopen(req, context=context, timeout=120) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)


class UdevHelper:
    def __init__(self, ips):
        self.ips = ips
        self.stop_event = threading.Event()
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        if self.thread:
            self.stop_event.set()
            self.thread.join(timeout=5)

    def _run(self):
        while not self.stop_event.is_set():
            for ip in self.ips:
                try:
                    run_remote_spark(ip, "vgscan --mknodes && udevadm trigger")
                except Exception:
                    pass
            # Wait up to 2 seconds, checking the stop_event frequently
            for _ in range(20):
                if self.stop_event.is_set():
                    break
                time.sleep(0.1)


def run_mtls_spark_api(ip, path, payload, method="POST"):
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/etc/hci/spark/certs/ca.crt")
    context.load_cert_chain(certfile="/etc/hci/spark/certs/node.crt", keyfile="/etc/hci/spark/certs/node.key")
    context.check_hostname = False
    
    url = f"https://{ip}:9099{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=120) as response:
            res = json.loads(response.read().decode("utf-8"))
            return 0, res, ""
    except Exception as e:
        return -1, {}, str(e)

def run_parallel(ips, cmd):
    results = {}
    threads = []
    def worker(ip):
        rc, stdout, stderr = run_remote_spark(ip, cmd)
        results[ip] = (rc, stdout, stderr)
    for ip in ips:
        t = threading.Thread(target=worker, args=(ip,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    return results

def execute_checked(command, allow_already_exists=False):
    import subprocess
    print(f"[EXECUTE_CHECKED] Running command: {command}")
    res = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = res.stdout.decode('utf-8', errors='ignore').strip()
    stderr = res.stderr.decode('utf-8', errors='ignore').strip()
    if stdout:
        print(f"[EXECUTE_CHECKED] stdout:\n{stdout}")
    if stderr:
        print(f"[EXECUTE_CHECKED] stderr:\n{stderr}")
    if res.returncode != 0:
        harmless = False
        if allow_already_exists:
            combined = (stdout + "\n" + stderr).lower()
            if any(msg in combined for msg in [
                "already exists",
                "already defined",
                "already created",
                "already registered",
                "already configured",
                "is already",
                "already has"
            ]):
                harmless = True
        if not harmless:
            raise Exception(f"Command failed with exit code {res.returncode}.\nCommand: {command}\nStdout: {stdout}\nStderr: {stderr}")
    return res.returncode, stdout, stderr

def run_parallel_checked(ips, command, allow_already_exists=False):
    print(f"Running parallel command on {ips}: {command}")
    results = run_parallel(ips, command)
    for ip, (rc, stdout, stderr) in results.items():
        stdout = stdout.strip() if stdout else ""
        stderr = stderr.strip() if stderr else ""
        if rc != 0:
            harmless = False
            if allow_already_exists:
                combined = (stdout + "\n" + stderr).lower()
                if any(msg in combined for msg in [
                    "already exists",
                    "already defined",
                    "already created",
                    "already registered",
                    "already configured",
                    "is already",
                    "already has"
                ]):
                    harmless = True
            if not harmless:
                raise Exception(f"Parallel command failed on {ip} with exit code {rc}.\nCommand: {command}\nStdout: {stdout}\nStderr: {stderr}")
    return results

def check_urbosa_enabled():
    rc, stdout, _ = run_cql_query("SELECT value FROM hydra.cluster_settings WHERE key = 'urbosa_enabled';")
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            if "true" in line.lower():
                return True
    return False

def run_cql_query(cql_query, *args, **kwargs):
    import urllib.request
    import json
    try:
        url = "http://127.0.0.1:9043/query"
        req = urllib.request.Request(
            url,
            data=cql_query.encode('utf-8'),
            headers={'Content-Type': 'text/plain'}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            res = json.loads(response.read().decode('utf-8'))
            if res.get("status") == "success":
                lines = []
                for row in res.get("rows", []):
                    if isinstance(row, dict):
                        if "json" in row:
                            lines.append(row["json"])
                        else:
                            vals = [str(v) for v in row.values()]
                            lines.append(" ".join(vals))
                    else:
                        lines.append(str(row))
                return 0, "\n".join(lines), ""
            else:
                return 1, "", res.get("error", "Database query execution error")
    except Exception as e:
        import base64
        import subprocess
        b64_query = base64.b64encode(cql_query.encode('utf-8')).decode('utf-8')
        local_ip = "127.0.0.1"
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('10.255.255.255', 1))
            local_ip = s.getsockname()[0]
            s.close()
        except Exception:
            pass
        cmd = f'echo {b64_query} | base64 -d | podman exec -i systemd-hydra-db cqlsh {local_ip}'
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        return p.returncode, stdout.decode('utf-8', errors='ignore').strip(), stderr.decode('utf-8', errors='ignore').strip()

def run_mtls_spark_api(ip, path, payload, method="POST"):
    import ssl, urllib.request, json
    ca_cert = "/etc/hci/spark/certs/ca.crt"
    node_cert = "/etc/hci/spark/certs/node.crt"
    node_key = "/etc/hci/spark/certs/node.key"
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
    context.load_cert_chain(certfile=node_cert, keyfile=node_key)
    context.check_hostname = False
    
    url = f"https://{ip}:9099{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=30) as response:
            res = json.loads(response.read().decode("utf-8"))
            return 0, res, ""
    except Exception as e:
        return -1, {}, str(e)

def sync_cluster_settings_local():
    import json, subprocess, os, re
    try:
        cql = "SELECT JSON key, value FROM hydra.cluster_settings;"
        rc, stdout, stderr = run_cql_query(cql)
        if rc != 0 or not stdout:
            return False, f"ScyllaDB query failed or empty: {stderr}"
            
        settings = {}
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    obj = json.loads(line)
                    settings[obj.get("key")] = obj.get("value")
                except:
                    pass
                    
        # Apply DNS Settings
        dns_servers = settings.get("dns_servers", "8.8.8.8,8.8.4.4")
        dns_search = settings.get("dns_search_domains", "cluster.local")
        dns_list = [d.strip() for d in dns_servers.split(",") if d.strip()]
        resolv_conf = ""
        if dns_search:
            resolv_conf += f"search {dns_search}\n"
        for dns in dns_list:
            resolv_conf += f"nameserver {dns}\n"
            
        current_resolv = ""
        if os.path.exists("/etc/resolv.conf"):
            try:
                with open("/etc/resolv.conf", "r") as f:
                    current_resolv = f.read()
            except:
                pass
        if current_resolv != resolv_conf:
            with open("/etc/resolv.conf", "w") as f:
                f.write(resolv_conf)
            
        # Apply NTP Settings
        ntp_servers = settings.get("ntp_servers", "pool.ntp.org")
        ntp_list = [n.strip() for n in ntp_servers.split(",") if n.strip()]
        chrony_conf = ""
        for ntp in ntp_list:
            chrony_conf += f"server {ntp} iburst\n"
            
        current_chrony = ""
        if os.path.exists("/etc/chrony.conf"):
            try:
                with open("/etc/chrony.conf", "r") as f:
                    current_chrony = f.read()
            except:
                pass
        if current_chrony != chrony_conf:
            with open("/etc/chrony.conf", "w") as f:
                f.write(chrony_conf)
            subprocess.run("systemctl restart chronyd", shell=True)
        
        # Apply Timezone
        timezone = settings.get("timezone", "UTC")
        timezone_sanitized = re.sub(r'[^A-Za-z0-9/\-_]', '', timezone)
        if timezone_sanitized:
            res_tz = subprocess.run("timedatectl show --property=Timezone --value", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            current_tz = res_tz.stdout.decode().strip()
            if current_tz != timezone_sanitized:
                subprocess.run(f"timedatectl set-timezone {timezone_sanitized}", shell=True)
            
        print("[sync_cluster_settings_local] Successfully synced DNS, NTP, and Timezone from ScyllaDB.")
        return True, ""
    except Exception as e:
        return False, str(e)

def settings_sync_loop():
    print("[SPARK] Starting periodic cluster settings sync loop...")
    time.sleep(15)
    while True:
        try:
            sync_cluster_settings_local()
        except Exception as e:
            print(f"[SPARK] settings_sync_loop error: {e}")
        time.sleep(60)

# ---------------------------------------------------------------------------
# ZooKeeper-backed cluster state (Odin/Zeus).
#
# Each node holds an *ephemeral* znode under ZK_NODES_PATH describing itself. Because
# the znode's lifetime is bound to the ZooKeeper session, a node that dies has its entry
# removed by the ensemble rather than by anyone polling for it -- liveness stops being a
# sample and becomes a fact. `cluster status` then reads this tree instead of fanning
# out mTLS calls to every host on every invocation.
#
# Desired cluster state lives at ZK_CLUSTER_STATE; each node's reconcile loop converges
# its local services toward it, so `cluster start` sets an intent rather than driving
# each node imperatively.
ZK_ROOT = "/helios"
ZK_NODES_PATH = ZK_ROOT + "/nodes"
# Desired cluster state. This is the path cluster_new.py and spectrum already write via
# zkCli.sh ("started"/"stopped"), so the reconcile loop reads the existing source of
# truth rather than introducing a competing one.
ZK_CLUSTER_STATE = "/cluster_state"
ZK_PUBLISH_INTERVAL = 5          # seconds between state refreshes
ZK_DRIFT_CHECK_INTERVAL = 30     # seconds between drift re-assertions
ZK_SESSION_TIMEOUT_MS = 15000
FLAP_RESTART_THRESHOLD = 3       # restarts before "active but no PID" reads as FLAPPING

# Services this node manages when converging toward the desired cluster state, in start
# order. Stop order is the reverse. ZooKeeper is deliberately absent: it is the store the
# desired state lives in, so it is started before convergence begins and stopped last.
MANAGED_SERVICE_ORDER = ["hydra-db", "daruk", "aether", "linstor-controller", "spectrum",
                         "slate", "agahnim", "catalyst", "vali", "bifrost", "dagur",
                         "mimir", "logos", "mipha", "gatoway", "urbosa", "hylia"]


def _load_helios_zk():
    """Import the helios_zk module from wherever it was deployed."""
    try:
        import helios_zk
        return helios_zk
    except ImportError:
        pass
    import importlib.util
    import importlib.machinery
    for candidate in ("/usr/local/bin/helios_zk.py", "/usr/local/bin/helios_zk"):
        if os.path.exists(candidate):
            loader = importlib.machinery.SourceFileLoader("helios_zk", candidate)
            spec = importlib.util.spec_from_loader("helios_zk", loader)
            mod = importlib.util.module_from_spec(spec)
            loader.exec_module(mod)
            return mod
    raise ImportError("helios_zk module not found")


def get_zk_hosts():
    """Cluster ZooKeeper endpoints, local first so a healthy node prefers itself."""
    hosts = []
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            hosts = [h["ip"] for h in json.load(f).get("hosts", []) if h.get("ip")]
    except Exception:
        pass
    return ["127.0.0.1"] + [h for h in hosts if h != "127.0.0.1"]


def zk_publisher_loop():
    """Maintain this node's ephemeral znode, reconnecting whenever the session drops."""
    try:
        zkmod = _load_helios_zk()
    except ImportError as exc:
        print(f"[ZK] helios_zk unavailable ({exc}); node state will not be published.", flush=True)
        return

    client = None
    node_path = None
    while True:
        try:
            if client is None:
                client = zkmod.connect(get_zk_hosts(), session_timeout_ms=ZK_SESSION_TIMEOUT_MS)
                client.ensure_path(ZK_NODES_PATH)
                print(f"[ZK] Publisher connected to {client.connected_host}.", flush=True)
                node_path = None

            status = build_node_status()
            status["ts"] = int(time.time())
            status["build"] = globals().get("__build__", "unknown")
            if node_path is None:
                node_path = ZK_NODES_PATH + "/" + str(status.get("ip") or "unknown")
            client.upsert_ephemeral(node_path, json.dumps(status).encode("utf-8"))
        except Exception as exc:
            print(f"[ZK] Publisher error ({exc}); reconnecting.", flush=True)
            try:
                if client:
                    client.close()
            except Exception:
                pass
            client = None
            node_path = None
        time.sleep(ZK_PUBLISH_INTERVAL)


def read_desired_cluster_state(client):
    """Return the desired cluster state ('started'/'stopped'), or None if unreadable.

    None is meaningfully different from 'stopped': it means we could not determine
    intent, and the correct response is to change nothing. Treating "unknown" as
    "stopped" is what deadlocked the old autostart path -- ZooKeeper down meant the
    state could not be read, which was read as 'stopped', which stopped ZooKeeper.
    """
    try:
        raw = client.get(ZK_CLUSTER_STATE)
        if raw is None:
            return None
        value = raw.decode("utf-8", "replace").strip()
        return value or None
    except Exception:
        return None


def converge_to_desired_state(desired, full=False):
    """Bring local services into line with the desired cluster state.

    `full=True` (the desired state just changed) walks the whole set in dependency order.
    Otherwise this is a periodic drift check: it only touches units that are not already
    in the intended state, so the common case costs one `systemctl is-active` batch and
    issues no commands at all.

    The drift check exists because acting only on state *changes* leaves a hole: a service
    that dies later, while the desired state is unchanged, is left to systemd's
    Restart=always -- and a unit that exhausts its restart limit stays down until someone
    rewrites the desired state.
    """
    running = desired.startswith("start")
    order = MANAGED_SERVICE_ORDER if running else list(reversed(MANAGED_SERVICE_ORDER))
    action = "start" if running else "stop"

    if full:
        print(f"[ZK] Converging local services toward '{desired}'.", flush=True)
        targets = order
    else:
        # Batch one query for all managed units and act only on the mismatches.
        res = subprocess.run("systemctl is-active " + " ".join(order),
                             shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        states = res.stdout.decode().splitlines()
        targets = []
        for idx, svc in enumerate(order):
            state = states[idx].strip() if idx < len(states) else ""
            is_active = state == "active"
            # "activating" is in-flight, not drift; leave it alone rather than restarting
            # a unit that is already on its way to the right state.
            if state == "activating":
                continue
            if running and not is_active:
                targets.append(svc)
            elif not running and is_active:
                targets.append(svc)
        if not targets:
            return
        print(f"[ZK] Drift detected against '{desired}': {', '.join(targets)}", flush=True)

    for svc in targets:
        subprocess.run(f"systemctl {action} {svc}", shell=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def zk_reconcile_loop():
    """Converge local services toward the desired cluster state published in ZooKeeper.

    This is what makes `cluster start` a declaration rather than an imperative drive: the
    CLI records intent once, and every node moves itself toward it and republishes what
    it actually achieved.
    """
    try:
        zkmod = _load_helios_zk()
    except ImportError:
        return

    client = None
    applied = None
    last_drift_check = 0.0
    while True:
        try:
            if client is None:
                client = zkmod.connect(get_zk_hosts(), session_timeout_ms=ZK_SESSION_TIMEOUT_MS)
                print("[ZK] Reconcile loop connected.", flush=True)
            # Read inline rather than through a helper that swallows errors: a dead
            # socket must propagate to the handler below so the client is rebuilt.
            # Swallowing it returns None forever, which looks like "no desired state"
            # and silently wedges the loop against a socket that will never recover.
            try:
                raw = client.get(ZK_CLUSTER_STATE)
                desired = raw.decode("utf-8", "replace").strip() or None
            except zkmod.ZKNoNode:
                desired = None      # state genuinely unset; nothing to converge toward
            if desired:
                if os.path.exists("/etc/hci/maintenance.state"):
                    if desired != applied:
                        print(f"[ZK] Desired state '{desired}' ignored: host is in maintenance.", flush=True)
                        applied = desired
                else:
                    changed = desired != applied
                    now = time.time()
                    due = (now - last_drift_check) >= ZK_DRIFT_CHECK_INTERVAL
                    if changed or due:
                        last_drift_check = now
                        converge_to_desired_state(desired, full=changed)
                        applied = desired
        except Exception as exc:
            print(f"[ZK] Reconcile error ({exc}); reconnecting.", flush=True)
            try:
                if client:
                    client.close()
            except Exception:
                pass
            client = None
        time.sleep(ZK_PUBLISH_INTERVAL)


def build_node_status():
    """Collect this node's service and liveness state.

    Module-level so the /api/v1/node/status handler and the ZooKeeper publisher
    share one implementation. spark.py and this daemon previously derived service
    state separately and could disagree about the same host.
    """
    import json
    import subprocess
    import socket
    import os
    
    ip_addr = "127.0.0.1"
    hostname = socket.gethostname()
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            hosts = cdata.get("hosts", [])
            for h in hosts:
                if h.get("hostname") == hostname:
                    ip_addr = h.get("ip")
                    break
            if ip_addr == "127.0.0.1" and hosts:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    s.connect(("8.8.8.8", 80))
                    local_ip = s.getsockname()[0]
                    s.close()
                    for h in hosts:
                        if h.get("ip") == local_ip:
                            ip_addr = local_ip
                            hostname = h.get("hostname")
                            break
                except Exception:
                    pass
    except Exception:
        pass
        
    is_leader = False
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.1)
        s.connect(("127.0.0.1", 2181))
        s.sendall(b"stat")
        resp = s.recv(1024).decode('utf-8', errors='ignore')
        s.close()
        is_leader = "mode: leader" in resp.lower() or "mode: standalone" in resp.lower()
    except Exception:
        pass
        
    maint_status = "NORMAL"
    if os.path.exists("/etc/hci/maintenance.state"):
        maint_status = "IN_MAINTENANCE"
        
    global NODE_DISKS_CACHE
    if 'NODE_DISKS_CACHE' not in globals():
        globals()['NODE_DISKS_CACHE'] = None
        
    disks_count = globals()['NODE_DISKS_CACHE']
    if disks_count is None:
        try:
            res_d = subprocess.run("lsblk -d -n -o TYPE", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if res_d.returncode == 0:
                disks_count = sum(1 for line in res_d.stdout.decode().splitlines() if line.strip() == "disk")
                globals()['NODE_DISKS_CACHE'] = disks_count
            else:
                disks_count = 1
        except Exception:
            disks_count = 1

    global SERVICE_PIDS_CACHE, LAST_PIDS_CACHE_TIME
    if 'SERVICE_PIDS_CACHE' not in globals():
        globals()['SERVICE_PIDS_CACHE'] = {}
    if 'LAST_PIDS_CACHE_TIME' not in globals():
        globals()['LAST_PIDS_CACHE_TIME'] = 0

    services = ["zookeeper", "hydra-db", "daruk", "aether", "spark-daemon", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "hylia", "gatoway", "logos", "mipha", "agahnim", "slate"]
    svc_map = {
        "zookeeper": "ZooKeeper",
        "hydra-db": "HydraDB",
        "daruk": "Daruk",
        "aether": "Aether",
        "spark-daemon": "Spark",
        "spectrum": "Spectrum",
        "bifrost": "Bifrost",
        "dagur": "Dagur",
        "mimir": "Mimir",
        "vali": "Vali",
        "catalyst": "Catalyst",
        "hylia": "Hylia",
        "gatoway": "Gatoway",
        "logos": "Logos",
        "mipha": "Mipha",
        "agahnim": "Agahnim",
        "slate": "Slate"
    }
    if check_urbosa_enabled():
        services.append("urbosa")
        svc_map["urbosa"] = "Urbosa"
    
    result = {
        "ip": ip_addr,
        "hostname": hostname,
        "zk_leader": is_leader,
        "maintenance_status": maint_status,
        "disks": disks_count,
        "services": {}
    }
    
    cmd = f"systemctl is-active {' '.join(services)}"
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    lines = res.stdout.decode().splitlines()
    
    services_active = {}
    for idx, svc in enumerate(services):
        is_active = False
        if idx < len(lines):
            is_active = (lines[idx].strip() == "active")
        services_active[svc] = is_active

    # Refresh PIDs cache if 10 seconds elapsed
    now = time.time()
    if now - globals()['LAST_PIDS_CACHE_TIME'] > 10 or not globals()['SERVICE_PIDS_CACHE']:
        new_cache = {}
        
        # Native services
        native_svcs = ["daruk"]
        cmd_native = f"systemctl show -p MainPID --value {' '.join(native_svcs)}"
        try:
            res_nat = subprocess.run(cmd_native, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if res_nat.returncode == 0:
                nat_lines = [l.strip() for l in res_nat.stdout.decode().splitlines() if l.strip()]
                for s_idx, s_name in enumerate(native_svcs):
                    pids = []
                    if s_idx < len(nat_lines):
                        val = nat_lines[s_idx]
                        if val and val != "0":
                            pids = [int(val)]
                    new_cache[s_name] = pids
            else:
                for s_name in native_svcs:
                    new_cache[s_name] = []
        except Exception:
            for s_name in native_svcs:
                new_cache[s_name] = []
                
        # Containerized services
        container_svcs = ["spark-daemon", "bifrost", "dagur", "mimir", "vali", "catalyst", "hylia", "gatoway", "logos", "mipha", "agahnim", "zookeeper", "hydra-db", "aether", "spectrum", "slate"]
        if "urbosa" in services:
            container_svcs.append("urbosa")
        for s_name in container_svcs:
            pids = []
            if services_active.get(s_name):
                try:
                    res_cont = subprocess.run(f"podman top systemd-{s_name} hpid", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    if res_cont.returncode == 0:
                        cont_lines = res_cont.stdout.decode().strip().splitlines()
                        if len(cont_lines) > 1:
                            for line in cont_lines[1:]:
                                val = line.strip()
                                if val and val != "?":
                                    try:
                                        pids.append(int(val))
                                    except ValueError:
                                        pids.append(val)
                except Exception:
                    pass
            new_cache[s_name] = pids
            
        globals()['SERVICE_PIDS_CACHE'] = new_cache
        globals()['LAST_PIDS_CACHE_TIME'] = now

    pids_cache = globals()['SERVICE_PIDS_CACHE']
    
    # Restart counters, so a crash-looping service is not reported as healthy.
    # `systemctl is-active` returns "active" during each restart window of a unit with
    # Restart=always, so a unit that has failed 30 times in a row samples as UP roughly
    # as often as not. NRestarts makes the flapping visible instead of invisible.
    restarts = {}
    try:
        res_nr = subprocess.run(
            "systemctl show -p NRestarts --value " + " ".join(services),
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        nr_lines = res_nr.stdout.decode().splitlines()
        for idx, svc in enumerate(services):
            if idx < len(nr_lines):
                try:
                    restarts[svc] = int(nr_lines[idx].strip() or 0)
                except ValueError:
                    restarts[svc] = 0
    except Exception:
        pass

    for svc in services:
        n_restarts = restarts.get(svc, 0)
        if services_active[svc]:
            svc_pids = pids_cache.get(svc, [])
            # Active with no main PID and a restart history is a unit caught mid-respawn,
            # not a healthy one. Report it as FLAPPING so the operator sees the truth.
            flapping = n_restarts >= FLAP_RESTART_THRESHOLD and not svc_pids
            result["services"][svc_map[svc]] = {
                "status": "FLAPPING" if flapping else "UP",
                "pids": svc_pids,
                "restarts": n_restarts
            }
        else:
            result["services"][svc_map[svc]] = {
                "status": "DOWN",
                "pids": [],
                "restarts": n_restarts
            }

    return result

# ---------------------------------------------------------------------------
# Typed API (docs/spark_api.md)
#
# These endpoints exist so callers stop handing this daemon shell strings. No
# caller value is ever interpolated into a command: parameters are validated at
# the boundary and then passed as individual argv elements with shell=False, so
# nothing a caller sends can change the shape of a command. Parsing lives in
# module-level functions so it stays testable without an HTTP server.
# ---------------------------------------------------------------------------

# A name is a value, never a fragment. \Z rather than $ on purpose: $ also matches
# just before a trailing newline, so "vm1\n" would pass and then be handed to virsh.
NAME_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.-]{0,62}\Z")

# Paths must resolve under one of these roots. /dev/drbd/by-res/<res>/<vol> is a
# symlink to the minor device, so realpath legitimately lands on /dev/drbdNNNN --
# that one target is accepted as well, but only when the path as written was
# already under /dev/drbd/. Everything else (traversal, a symlink out of the
# aether tree) is rejected by the realpath check.
ALLOWED_PATH_ROOTS = ("/dev/drbd/", "/var/lib/hci/aether/")
DRBD_MINOR_RE = re.compile(r"\A/dev/drbd[0-9]+\Z")

ALLOWED_OWNERS = ("root:qemu", "root:root")
ALLOWED_MODES = ("0600", "0640", "0644", "0660", "0664", "0666", "0700", "0750", "0755", "0770")

AETHER_VOLUMES_ROOT = "/var/lib/hci/aether/volumes"
VIRSH = ["virsh", "-c", "qemu:///system"]
HYDRA_DB_CONTAINER = "systemd-hydra-db"
VM_POWER_ACTIONS = ("start", "destroy", "reboot", "shutdown", "reset")
DRBD_ROLES = ("primary", "secondary")

# -- Linstor ---------------------------------------------------------------
# The client lives in the aether container, which runs on every node, and finds
# whichever node currently holds the controller through LS_CONTROLLERS. Calling it
# there rather than in systemd-linstor-controller is what makes these endpoints work
# from any host instead of only the leader.
LINSTOR_CONTAINER = "systemd-aether"
CLUSTER_JSON = "/etc/hci/cluster.json"

# Cluster creation makes exactly one pool:
#   linstor storage-pool create lvmthin <node> default-pool vg_aether/thin_pool_aether
# and Spectrum refuses dynamic container creation on this storage engine. A pool is
# therefore an allowlisted value like an owner or a mode, never caller text; adding a
# second pool to the product means adding its name here.
ALLOWED_STORAGE_POOLS = ("default-pool",)
DEFAULT_STORAGE_POOL = "default-pool"

# 1 GiB .. 64 TiB. The floor rejects a zero-sized volume-definition; the ceiling is a
# sanity bound on the number, not a capacity check -- Linstor still refuses what the
# pool cannot actually back.
MIN_VOLUME_GIB = 1
MAX_VOLUME_GIB = 65536
KIB_PER_GIB = 1024 * 1024

# Automatic split-brain resolution, applied to every VM disk at create time.
#
# --allow-two-primaries is deliberately absent. Setting it here let one VM be started
# on two hosts at once, and two qemu processes writing one raw DRBD device corrupts
# it. Live migration needs dual-primary only for the hand-over window and enables it
# around that call itself.
DRBD_SPLIT_BRAIN_OPTIONS = [
    "--after-sb-0pri", "discard-zero-changes",
    "--after-sb-1pri", "discard-secondary",
    "--after-sb-2pri", "disconnect",
]

# LINSTOR's ApiCallRc carries its severity in the top two bits: error is both set,
# warning is the high bit alone, info the next one. Used only as a second opinion --
# the client's exit status is the primary signal, so a wrong guess about this mask
# cannot turn a successful call into a failure on its own.
LINSTOR_MASK_ERROR = 0xC000000000000000

# A resource that is already there is not a failure for an idempotent create; a
# resource that is already gone is not a failure for a delete. Kept to the phrasings
# LINSTOR actually uses: a loose marker here would read a real failure as a success and
# return 200 for a disk that does not exist, which is the bug this whole endpoint is
# meant to remove.
LINSTOR_EXISTS_MARKERS = ("already exists", "already registered")
LINSTOR_ABSENT_MARKERS = ("not found", "does not exist")

IPV4_RE = re.compile(r"\A[0-9]{1,3}(?:\.[0-9]{1,3}){3}\Z")

DNSMASQ_LEASE_FILES = ("/var/lib/dnsmasq/dnsmasq.leases", "/var/lib/misc/dnsmasq.leases")
LIBVIRT_LEASE_GLOB = "/var/lib/libvirt/dnsmasq/*.leases"
SECURE_BOOT_EFIVAR = "/sys/firmware/efi/efivars/SecureBoot-8be4df61-93ca-11d2-aa0d-00e098032b8c"


def run_argv(argv, timeout=45):
    """Run a command as an argv list with shell=False.

    Every typed endpoint goes through here. subprocess.run() with a list never
    involves a shell, so a value in argv is always exactly one argument no matter
    what characters it contains.
    """
    try:
        res = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    except FileNotFoundError:
        return 127, "", "%s: command not found" % argv[0]
    except subprocess.TimeoutExpired:
        return -1, "", "%s: timed out after %ss" % (argv[0], timeout)
    except OSError as exc:
        return -1, "", str(exc)
    return (res.returncode,
            res.stdout.decode("utf-8", errors="ignore"),
            res.stderr.decode("utf-8", errors="ignore"))


def valid_name(value):
    """True when value is a name safe to pass as a single argument."""
    return isinstance(value, str) and NAME_RE.match(value) is not None


def validate_path(value):
    """Resolve and allowlist a caller-supplied path.

    Returns (realpath, None) or (None, error_message). Rejects rather than
    sanitizes: nothing is stripped or rewritten to make a path acceptable.
    """
    if not isinstance(value, str) or not value:
        return None, "path must be a non-empty string"
    if "\x00" in value:
        return None, "path must not contain a null byte"
    if not value.startswith("/"):
        return None, "path must be absolute"

    literal = os.path.normpath(value)
    real = os.path.realpath(value)

    def under_root(candidate):
        for root in ALLOWED_PATH_ROOTS:
            if candidate.startswith(root) and len(candidate) > len(root):
                return True
        return False

    if not under_root(literal):
        return None, "path must be under " + " or ".join(ALLOWED_PATH_ROOTS)
    if not under_root(real):
        # A /dev/drbd/by-res/... symlink resolves to the bare minor device.
        if not (literal.startswith("/dev/drbd/") and DRBD_MINOR_RE.match(real)):
            return None, "path resolves outside " + " or ".join(ALLOWED_PATH_ROOTS)
    return real, None


def validate_owner(value):
    """Owner comes from a fixed allowlist, never from caller text."""
    if value in ALLOWED_OWNERS:
        return value, None
    return None, "owner must be one of " + ", ".join(ALLOWED_OWNERS)


def validate_mode(value):
    """Mode is an octal string from a fixed allowlist."""
    if value in ALLOWED_MODES:
        return value, None
    return None, "mode must be one of " + ", ".join(ALLOWED_MODES)


def validate_storage_pool(value):
    """Storage pool comes from a fixed allowlist, never from caller text."""
    if value is None:
        return DEFAULT_STORAGE_POOL, None
    if value in ALLOWED_STORAGE_POOLS:
        return value, None
    return None, "storage_pool must be one of " + ", ".join(ALLOWED_STORAGE_POOLS)


def validate_volume_gib(value):
    """Volume size is an integer number of GiB inside sane bounds.

    A bool is rejected explicitly: `isinstance(True, int)` is True in Python, and
    `True` would otherwise be accepted and formatted as a 1 GiB volume.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None, "size_gib must be an integer number of GiB"
    if value < MIN_VOLUME_GIB or value > MAX_VOLUME_GIB:
        return None, "size_gib must be between %d and %d" % (MIN_VOLUME_GIB, MAX_VOLUME_GIB)
    return value, None


def validate_node_names(value):
    """Node names for a placement: a list of names, or the cluster's own nodes.

    Omitting `nodes` places on every node in the cluster document, which is what the
    Python tier's create path did. An explicit empty list is an error rather than a
    silent no-op: a resource definition with no resources backs no disk.
    """
    if value is None:
        nodes = cluster_node_names()
        if not nodes:
            return None, "No nodes are configured on this host and none were supplied"
        return nodes, None

    if not isinstance(value, list):
        return None, "nodes must be a list of node names"
    if not value:
        return None, "nodes must not be empty"
    if len(value) > 32:
        return None, "nodes must contain at most 32 names"

    nodes = []
    for name in value:
        if not valid_name(name):
            return None, "Invalid node name"
        if name not in nodes:
            nodes.append(name)
    return nodes, None


def virsh_status_for(stderr):
    """404 when libvirt says the domain does not exist, 500 otherwise."""
    lowered = (stderr or "").lower()
    if "not found" in lowered or "no domain" in lowered:
        return 404
    return 500


def parse_virsh_domiflist(text):
    """Parse `virsh domiflist` into [{"mac","type","source","model"}].

    Columns are Interface, Type, Source, Model, MAC. The MAC is taken from the
    last column so an unexpected extra column cannot shift it.
    """
    interfaces = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if set(line) <= set("- "):          # the ---- separator row
            continue
        parts = line.split()
        if len(parts) < 5:
            continue
        if parts[0].lower() == "interface" and parts[-1].lower() == "mac":
            continue                        # header row
        def cell(value):
            return "" if value == "-" else value
        interfaces.append({
            "mac": cell(parts[-1]),
            "type": cell(parts[1]),
            "source": cell(parts[2]),
            "model": cell(parts[3]),
        })
    return interfaces


def parse_virsh_dominfo(text):
    """Parse `virsh dominfo` into {"state","vcpus","memory_kib","autostart"}."""
    fields = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key.strip().lower()] = value.strip()

    vcpus = None
    try:
        vcpus = int(fields.get("cpu(s)", "").strip())
    except (TypeError, ValueError):
        vcpus = None

    memory_kib = None
    mem_raw = fields.get("max memory", "")
    if mem_raw:
        try:
            memory_kib = int(mem_raw.split()[0])
        except (IndexError, ValueError):
            memory_kib = None

    return {
        "state": fields.get("state", ""),
        "vcpus": vcpus,
        "memory_kib": memory_kib,
        "autostart": fields.get("autostart", "").lower() == "enable",
    }


def parse_domain_graphics(xml_text):
    """Pull the first vnc/spice <graphics> element out of a domain XML.

    Returns {"graphics","port","listen"} or None when the domain has no console.
    port is -1 for an autoport device that has not been allocated yet, which is a
    fact the caller needs rather than an error.
    """
    root = ET.fromstring(xml_text)
    for graphics in root.findall("./devices/graphics"):
        gtype = graphics.get("type")
        if gtype not in ("vnc", "spice"):
            continue
        try:
            port = int(graphics.get("port", "-1"))
        except (TypeError, ValueError):
            port = -1
        listen = graphics.get("listen") or ""
        if not listen:
            listen_el = graphics.find("./listen")
            if listen_el is not None:
                listen = listen_el.get("address") or ""
        return {"graphics": gtype, "port": port, "listen": listen}
    return None


def parse_domain_name(xml_text):
    """The <name> of a domain XML document, or None."""
    root = ET.fromstring(xml_text)
    name_el = root.find("./name")
    if name_el is None or name_el.text is None:
        return None
    return name_el.text.strip()


def virsh_domain_state(name):
    """Current libvirt state of a domain, or None when it cannot be read."""
    rc, stdout, _ = run_argv(VIRSH + ["domstate", name], timeout=20)
    if rc != 0:
        return None
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    return lines[0] if lines else None


_NODETOOL_STATUS = {"U": "Up", "D": "Down", "?": "Unknown"}
_NODETOOL_STATE = {"N": "Normal", "L": "Leaving", "J": "Joining", "M": "Moving", "?": "Unknown"}
_LOAD_UNITS = ("bytes", "B", "KB", "MB", "GB", "TB", "PB",
               "KiB", "MiB", "GiB", "TiB", "PiB")


def parse_nodetool_status(text):
    """Parse `nodetool status` into [{"address","status","state","load","tokens"}].

    Data rows begin with a two-character status/state code (UN, DN, UJ, ...);
    every header and banner line fails that test and is skipped.
    """
    nodes = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        code = parts[0]
        if len(code) != 2:
            continue
        if code[0] not in _NODETOOL_STATUS or code[1] not in _NODETOOL_STATE:
            continue
        if len(parts) < 3:
            continue

        address = parts[1]
        rest = parts[2:]
        load = rest[0] if rest else ""
        consumed = 1 if rest else 0
        if len(rest) >= 2 and rest[1] in _LOAD_UNITS:
            load = rest[0] + " " + rest[1]
            consumed = 2

        tokens = None
        if len(rest) > consumed:
            try:
                tokens = int(rest[consumed])
            except ValueError:
                tokens = None

        nodes.append({
            "address": address,
            "status": _NODETOOL_STATUS[code[0]],
            "state": _NODETOOL_STATE[code[1]],
            "load": load,
            "tokens": tokens,
        })
    return nodes


def parse_meminfo(text):
    """Derive {"total_mb","used_mb","free_mb","available_mb"} from /proc/meminfo.

    Same arithmetic free(1) does (used = total - free - buffers - cache), read
    from the file free(1) itself reads, so there is no subprocess and no exposure
    to procps output-format drift between releases.
    """
    values = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        parts = value.split()
        if not parts:
            continue
        try:
            values[key.strip()] = int(parts[0])
        except ValueError:
            continue

    total = values.get("MemTotal", 0)
    free = values.get("MemFree", 0)
    available = values.get("MemAvailable", free)
    buffers = values.get("Buffers", 0)
    cached = values.get("Cached", 0) + values.get("SReclaimable", 0)
    used = total - free - buffers - cached
    if used < 0:
        used = 0
    return {
        "total_mb": total // 1024,
        "used_mb": used // 1024,
        "free_mb": free // 1024,
        "available_mb": available // 1024,
    }


def parse_dnsmasq_leases(text):
    """Parse a dnsmasq lease file into [{"mac","ip","hostname","expires"}].

    Line format: <expiry-epoch> <mac> <ip> <hostname> <client-id>.
    """
    leases = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        try:
            expires = int(parts[0])
        except ValueError:
            continue
        hostname = parts[3]
        if hostname == "*":
            hostname = ""
        leases.append({
            "mac": parts[1],
            "ip": parts[2],
            "hostname": hostname,
            "expires": expires,
        })
    return leases


def parse_ip_route_json(text):
    """(interface, gateway) of the default route from `ip -j route`."""
    try:
        routes = json.loads(text)
    except Exception:
        return None, None
    if not isinstance(routes, list):
        return None, None
    for route in routes:
        if isinstance(route, dict) and route.get("dst") == "default":
            return route.get("dev"), route.get("gateway")
    return None, None


def parse_proc_net_route(text):
    """(interface, gateway) of the default route from /proc/net/route.

    Fallback for an iproute2 without JSON support. The gateway column is a
    little-endian hex word.
    """
    for line in text.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 3:
            continue
        if parts[1] != "00000000":
            continue
        try:
            raw = int(parts[2], 16)
        except ValueError:
            continue
        gateway = ".".join(str((raw >> (8 * i)) & 0xFF) for i in range(4))
        return parts[0], gateway
    return None, None


def parse_ip_addr_json(text):
    """Addresses from `ip -j addr` as a flat list of dicts."""
    addresses = []
    try:
        links = json.loads(text)
    except Exception:
        return addresses
    if not isinstance(links, list):
        return addresses
    for link in links:
        if not isinstance(link, dict):
            continue
        ifname = link.get("ifname")
        for addr in link.get("addr_info") or []:
            if not isinstance(addr, dict):
                continue
            addresses.append({
                "interface": ifname,
                "family": addr.get("family"),
                "address": addr.get("local"),
                "prefixlen": addr.get("prefixlen"),
                "scope": addr.get("scope"),
            })
    return addresses


def _unescape_mount_field(field):
    """/proc/self/mounts escapes space, tab, newline and backslash as \\OOO."""
    out = []
    i = 0
    while i < len(field):
        char = field[i]
        if char == "\\" and i + 3 < len(field) and field[i + 1:i + 4].isdigit():
            try:
                out.append(chr(int(field[i + 1:i + 4], 8)))
                i += 4
                continue
            except ValueError:
                pass
        out.append(char)
        i += 1
    return "".join(out)


def parse_proc_mounts(text):
    """The set of mount points in a /proc/self/mounts document."""
    points = set()
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            points.add(_unescape_mount_field(parts[1]))
    return points


def path_is_mounted(path):
    """True when path is a mount point.

    Checks the mount table first, then falls back to the st_dev comparison
    mountpoint(1) uses, so a bind mount inside the same table is still caught.
    """
    try:
        with open("/proc/self/mounts", "r") as handle:
            if path in parse_proc_mounts(handle.read()):
                return True
    except Exception:
        pass
    try:
        here = os.stat(path)
        parent = os.stat(os.path.join(path, ".."))
        return here.st_dev != parent.st_dev
    except OSError:
        return False


def device_size_bytes(path, st_result):
    """Size of a block device or a regular file, without shelling out."""
    if stat.S_ISBLK(st_result.st_mode):
        try:
            fd = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        except OSError:
            return 0
        try:
            return os.lseek(fd, 0, os.SEEK_END)
        except OSError:
            return 0
        finally:
            os.close(fd)
    return st_result.st_size


def drbd_local_role(resource):
    """Local role of a DRBD resource, or None when it cannot be read.

    DRBD 8 prints "Primary/Secondary", DRBD 9 prints just the local role.
    """
    rc, stdout, _ = run_argv(["drbdadm", "role", resource], timeout=20)
    if rc != 0:
        return None
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        return None
    role = lines[0]
    if "/" in role:
        role = role.split("/", 1)[0]
    return role or None


def drbd_peer_roles(resource):
    """Peer roles of a DRBD resource from drbdsetup, [] when unknown.

    This is what makes "the peer already holds Primary" visible to the caller
    instead of surfacing as a generic promotion failure.
    """
    rc, stdout, _ = run_argv(["drbdsetup", "status", "--json", resource], timeout=20)
    if rc != 0:
        return []
    try:
        data = json.loads(stdout)
    except Exception:
        return []
    roles = []
    if isinstance(data, list):
        for resource_entry in data:
            if not isinstance(resource_entry, dict):
                continue
            for connection in resource_entry.get("connections") or []:
                if isinstance(connection, dict) and connection.get("peer-role"):
                    roles.append(connection["peer-role"])
    return roles


# ---------------------------------------------------------------------------
# Linstor
#
# Same rules as the rest of the typed API: every element of every command is a
# literal or an already-validated value, run_argv() calls subprocess.run() with a
# list and shell=False, and what comes back is parsed into this daemon's own shape
# rather than handed to the caller as stdout.
# ---------------------------------------------------------------------------


def cluster_hosts():
    """[{"hostname","ip"}] from the on-host cluster document; [] when unreadable.

    Values that could not be a node name or an address are dropped rather than
    repaired, so a damaged cluster.json cannot contribute an argument to a command.
    """
    try:
        with open(CLUSTER_JSON, "r") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return []
    hosts = data.get("hosts") if isinstance(data, dict) else None
    if not isinstance(hosts, list):
        return []

    parsed = []
    for host in hosts:
        if not isinstance(host, dict):
            continue
        hostname = host.get("hostname")
        address = host.get("ip")
        parsed.append({
            "hostname": hostname if valid_name(hostname) else None,
            "ip": address if isinstance(address, str) and IPV4_RE.match(address) else None,
        })
    return parsed


def cluster_node_names():
    """Every node name in the cluster document, in configured order."""
    return [host["hostname"] for host in cluster_hosts() if host["hostname"]]


def cluster_node_ips():
    """Every node address in the cluster document, in configured order."""
    return [host["ip"] for host in cluster_hosts() if host["ip"]]


def linstor_argv(args):
    """argv for one linstor client call inside the aether container.

    LS_CONTROLLERS is one argv element built from the cluster document, so the client
    reaches whichever node currently runs the controller. `-m` asks for the
    machine-readable document; every remaining element is a fixed literal or a value
    that has already passed validation at the boundary.
    """
    controllers = ",".join(cluster_node_ips()) or "127.0.0.1"
    return (["podman", "exec", "-e", "LS_CONTROLLERS=" + controllers,
             LINSTOR_CONTAINER, "linstor", "-m"] + list(args))


def _linstor_entries(node):
    """Yield the dicts in a machine-readable document, list nesting flattened.

    Some client versions wrap the document in an extra list ([[{...}]]), so this
    descends through lists but not through dict values -- a nested object inside a
    resource is data, not another top-level entry.
    """
    if isinstance(node, list):
        for item in node:
            for found in _linstor_entries(item):
                yield found
    elif isinstance(node, dict):
        yield node


def parse_linstor_api_call_rc(text):
    """[{"ret_code","message","details"}] from a linstor client response document."""
    try:
        data = json.loads(text.strip() or "[]")
    except ValueError:
        return []

    messages = []
    for entry in _linstor_entries(data):
        if "ret_code" not in entry and "message" not in entry:
            continue
        ret_code = entry.get("ret_code")
        if not isinstance(ret_code, int):
            ret_code = 0
        messages.append({
            "ret_code": ret_code,
            "message": str(entry.get("message") or ""),
            "details": str(entry.get("details") or ""),
            # "already exists" lands in `cause` rather than `message` for some of the
            # client's responses, and that string is what the idempotency check reads.
            "cause": str(entry.get("cause") or ""),
        })
    return messages


# The client renamed its keys between output versions. Both spellings are read here so
# that no caller ever has to know which version a given cluster's client speaks.
_RD_LIST_KEYS = ("resource_definitions", "rsc_dfns")
_RD_NAME_KEYS = ("name", "rsc_name")
_VD_LIST_KEYS = ("volume_definitions", "vlm_dfns")
_VD_NUMBER_KEYS = ("volume_number", "vlm_nr")
_VD_SIZE_KEYS = ("size_kib", "vlm_size")
_RSC_LIST_KEYS = ("resources",)
_RSC_NAME_KEYS = ("name", "rsc_name")
_RSC_NODE_KEYS = ("node_name", "node")


def _first_key(entry, keys):
    for key in keys:
        if key in entry:
            return entry[key]
    return None


def parse_linstor_resource_definitions(text):
    """[{"name","volumes":[{"number","size_kib"}]}] from `resource-definition list`.

    An unrecognised document yields [] rather than a guess: a caller that gets an
    empty list and then fails to create is recoverable, one that gets a wrong size is
    not.
    """
    try:
        data = json.loads(text.strip() or "[]")
    except ValueError:
        return []

    definitions = []
    for entry in _linstor_entries(data):
        listed = _first_key(entry, _RD_LIST_KEYS)
        if not isinstance(listed, list):
            # Piraeus 1.31's client emits the definitions bare -- [[{"name": ...}]] --
            # with no wrapper key at all, so an entry that already looks like a
            # definition is one. Verified against the deployed client; without this the
            # list endpoint reported an empty cluster while resources existed.
            if _first_key(entry, _RD_NAME_KEYS):
                listed = [entry]
            else:
                continue
        for definition in listed:
            if not isinstance(definition, dict):
                continue
            name = _first_key(definition, _RD_NAME_KEYS)
            if not isinstance(name, str) or not name:
                continue
            volumes = []
            for volume in _first_key(definition, _VD_LIST_KEYS) or []:
                if not isinstance(volume, dict):
                    continue
                number = _first_key(volume, _VD_NUMBER_KEYS)
                size_kib = _first_key(volume, _VD_SIZE_KEYS)
                volumes.append({
                    "number": number if isinstance(number, int) else 0,
                    "size_kib": size_kib if isinstance(size_kib, int) else None,
                })
            definitions.append({"name": name, "volumes": volumes})
    return definitions


def parse_linstor_resources(text):
    """[{"name","node"}] from `resource list`: which nodes actually back a resource."""
    try:
        data = json.loads(text.strip() or "[]")
    except ValueError:
        return []

    placements = []
    for entry in _linstor_entries(data):
        listed = _first_key(entry, _RSC_LIST_KEYS)
        if not isinstance(listed, list):
            continue
        for resource in listed:
            if not isinstance(resource, dict):
                continue
            name = _first_key(resource, _RSC_NAME_KEYS)
            node = _first_key(resource, _RSC_NODE_KEYS)
            if not isinstance(name, str) or not name:
                continue
            placements.append({
                "name": name,
                "node": node if isinstance(node, str) else "",
            })
    return placements


def linstor_says(detail, markers):
    """True when the client's own message contains one of `markers`."""
    lowered = (detail or "").lower()
    return any(marker in lowered for marker in markers)


def linstor_call(args, timeout=120):
    """Run one linstor client command. Returns (ok, stdout, detail).

    `ok` is False when the client exited non-zero or reported an error-masked
    ApiCallRc. The exit status is the primary signal and the mask is a second opinion,
    so a wrong assumption about the mask cannot by itself turn a successful call into
    a failure. `detail` is the client's own message text -- it drives the idempotency
    checks and the error string, and is never returned as the body of a success.
    """
    rc, stdout, stderr = run_argv(linstor_argv(args), timeout=timeout)
    messages = parse_linstor_api_call_rc(stdout)

    parts = []
    for message in messages:
        for field in ("message", "details", "cause"):
            if message[field]:
                parts.append(message[field])
    if stderr.strip():
        parts.append(stderr.strip())
    detail = " ".join(parts)

    reported_error = any(
        (message["ret_code"] & LINSTOR_MASK_ERROR) == LINSTOR_MASK_ERROR
        for message in messages
    )
    ok = rc == 0 and not reported_error
    if not ok and not detail:
        detail = stdout.strip() or ("linstor %s failed with exit code %s" % (args[0], rc))
    return ok, stdout, detail


def linstor_resource_path(resource):
    """Where a resource's volume 0 appears on every node backing it."""
    return "/dev/drbd/by-res/%s/0" % resource


def linstor_inventory():
    """(resources, error): what Linstor holds, in this daemon's shape.

    Each entry is {"name","size_kib","size_gib","nodes","device_path"}. Placement comes
    from a second call because `resource-definition list` describes the definition, not
    where it is materialised; a failure there degrades `nodes` to [] rather than failing
    the read, since the definitions are the part a caller cannot do without.
    """
    # volume-definition list, not resource-definition list: on the deployed client the
    # latter omits volume_definitions entirely, so existing sizes came back as null and
    # the size-mismatch guard below could never fire -- letting a new VM silently adopt
    # a deleted VM's disk at a different size.
    ok, stdout, detail = linstor_call(["volume-definition", "list"], timeout=60)
    if not ok:
        return None, detail or "linstor resource-definition list failed"

    placements = {}
    ok_resources, resources_stdout, _ = linstor_call(["resource", "list"], timeout=60)
    if ok_resources:
        for placement in parse_linstor_resources(resources_stdout):
            nodes = placements.setdefault(placement["name"], [])
            if placement["node"] and placement["node"] not in nodes:
                nodes.append(placement["node"])

    inventory = []
    for definition in parse_linstor_resource_definitions(stdout):
        volume = None
        for candidate in definition["volumes"]:
            if candidate["number"] == 0:
                volume = candidate
                break
        size_kib = volume["size_kib"] if volume else None
        inventory.append({
            "name": definition["name"],
            "size_kib": size_kib,
            "size_gib": (size_kib // KIB_PER_GIB) if isinstance(size_kib, int) else None,
            "nodes": placements.get(definition["name"], []),
            "device_path": linstor_resource_path(definition["name"]),
        })
    return inventory, None


def read_dhcp_leases():
    """Every dnsmasq lease this host knows about, deduplicated by (mac, ip)."""
    files = list(DNSMASQ_LEASE_FILES)
    try:
        files.extend(sorted(glob.glob(LIBVIRT_LEASE_GLOB)))
    except Exception:
        pass

    leases = []
    seen = set()
    for lease_file in files:
        try:
            with open(lease_file, "r", errors="ignore") as handle:
                content = handle.read()
        except OSError:
            continue
        for lease in parse_dnsmasq_leases(content):
            key = (lease["mac"], lease["ip"])
            if key in seen:
                continue
            seen.add(key)
            leases.append(lease)
    return leases


def read_host_capabilities():
    """{"kvm","drbd_module","secure_boot"} read straight from the kernel."""
    kvm = os.path.exists("/dev/kvm")

    drbd_module = os.path.exists("/proc/drbd")
    if not drbd_module:
        try:
            with open("/proc/modules", "r") as handle:
                for line in handle:
                    if line.split(" ", 1)[0] == "drbd":
                        drbd_module = True
                        break
        except OSError:
            pass

    secure_boot = False
    try:
        with open(SECURE_BOOT_EFIVAR, "rb") as handle:
            data = handle.read()
        # 4-byte EFI attribute prefix followed by the one-byte value.
        if data:
            secure_boot = data[-1] == 1
    except OSError:
        secure_boot = False

    return {"kvm": kvm, "drbd_module": drbd_module, "secure_boot": secure_boot}


DB_REPAIR_LOCK = threading.Lock()
DB_REPAIR_THREAD = None


def _run_db_repair(argv):
    print("[REPAIR] Starting %s. This can take a long time on a large keyspace." % " ".join(argv))
    rc, stdout, stderr = run_argv(argv, timeout=86400)
    if rc == 0:
        print("[REPAIR] Completed successfully.")
    else:
        print("[REPAIR] Failed with exit code %s: %s" % (rc, (stderr or stdout).strip()))


def start_db_repair(keyspace, primary_range):
    """Start a nodetool repair in the background.

    Returns True when a repair was started, False when one is already running. A
    repair outlives any reasonable HTTP timeout, so it never runs inline.
    """
    global DB_REPAIR_THREAD
    argv = ["podman", "exec", HYDRA_DB_CONTAINER, "nodetool", "repair"]
    if primary_range:
        argv.append("-pr")
    argv.append(keyspace)

    with DB_REPAIR_LOCK:
        if DB_REPAIR_THREAD is not None and DB_REPAIR_THREAD.is_alive():
            return False
        thread = threading.Thread(target=_run_db_repair, args=(argv,), daemon=True)
        DB_REPAIR_THREAD = thread
        thread.start()
        return True


def schedule_host_reboot(delay=2):
    """Reboot after the HTTP response has been written."""
    def worker():
        time.sleep(delay)
        rc, _, stderr = run_argv(["systemctl", "reboot"], timeout=60)
        if rc != 0:
            print("[REBOOT] systemctl reboot failed: %s" % stderr.strip())
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


class SparkDaemonHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def setup(self):
        # Perform SSL handshake in the worker thread
        self.connection = self.server.ssl_context.wrap_socket(self.request, server_side=True)
        if self.timeout is not None:
            self.connection.settimeout(self.timeout)
        if self.disable_nagle_algorithm:
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        self.rfile = self.connection.makefile('rb', self.rbufsize)
        self.wfile = self.connection.makefile('wb', self.wbufsize)

    def send_json_response(self, status, data):
        response_bytes = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def forward_to_vali(self, path, method="POST"):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length) if content_length > 0 else None
        
        # Parse payload if possible
        payload = {}
        if post_data:
            try:
                payload = json.loads(post_data.decode('utf-8'))
            except Exception:
                pass
        
        url = f"http://127.0.0.1:9095{path}"
        req = urllib.request.Request(url, data=post_data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=120) as response:
                res_bytes = response.read()
                self.send_response(response.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(res_bytes)))
                self.end_headers()
                self.wfile.write(res_bytes)
        except urllib.error.HTTPError as e:
            res_bytes = e.read()
            self.send_response(e.code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(res_bytes)))
            self.end_headers()
            self.wfile.write(res_bytes)
        except Exception as e:
            # Fallback for single-node cluster recovery when Vali is down and a leave command is requested
            if path == "/api/v1/hosts/maintenance" and payload.get("action") == "leave":
                hosts_data = []
                if os.path.exists("/etc/hci/cluster.json"):
                    try:
                        with open("/etc/hci/cluster.json", "r") as f:
                            hosts_data = json.load(f).get("hosts", [])
                    except Exception:
                        pass
                
                # Get list of other node IPs
                local_ip = '127.0.0.1'
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.connect(('10.255.255.255', 1))
                    local_ip = s.getsockname()[0]
                    s.close()
                except Exception:
                    pass
                
                other_ips = [h.get("ip") for h in hosts_data if h.get("ip") and h.get("ip") != local_ip]
                
                if len(hosts_data) > 1 and other_ips:
                    print(f"[Spark Daemon] Local Vali is offline. Multi-node cluster detected. Attempting to delegate leave maintenance request to remote spark daemons: {other_ips}")
                    context_remote = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/etc/hci/spark/certs/ca.crt")
                    context_remote.load_cert_chain(certfile="/etc/hci/spark/certs/node.crt", keyfile="/etc/hci/spark/certs/node.key")
                    context_remote.check_hostname = False
                    
                    forward_success = False
                    for remote_ip in other_ips:
                        url = f"https://{remote_ip}:9099/api/v1/host/maintenance"
                        data_bytes = json.dumps(payload).encode("utf-8")
                        req_remote = urllib.request.Request(url, data=data_bytes, headers={"Content-Type": "application/json"}, method="POST")
                        try:
                            with urllib.request.urlopen(req_remote, context=context_remote, timeout=45) as response_remote:
                                res_bytes = response_remote.read()
                                self.send_response(response_remote.status)
                                self.send_header("Content-Type", "application/json")
                                self.send_header("Content-Length", str(len(res_bytes)))
                                self.end_headers()
                                self.wfile.write(res_bytes)
                                forward_success = True
                                print(f"[Spark Daemon] Successfully delegated leave maintenance request to remote node {remote_ip}.")
                                break
                        except Exception as rex:
                            print(f"[Spark Daemon] Failed to delegate leave maintenance request to remote node {remote_ip}: {rex}")
                    
                    if forward_success:
                        return
                    else:
                        print("[Spark Daemon] All remote delegation attempts failed. Falling back to local bootstrapping...")
                
                print("[Spark Daemon] Vali is offline during maintenance leave. Bootstrapping local services directly...")
                try:
                    if os.path.exists("/etc/hci/maintenance.state"):
                        os.remove("/etc/hci/maintenance.state")
                    
                    start_cmd = "systemctl start zookeeper hydra-db aether linstor-controller spectrum bifrost dagur mimir vali catalyst gatoway logos mipha daruk agahnim slate"
                    subprocess.Popen(start_cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    
                    self.send_json_response(200, {
                        "status": "transitioning",
                        "message": "Vali offline. Bootstrapped local services on host directly to exit maintenance mode."
                    })
                    return
                except Exception as ex:
                    print(f"[Spark Daemon] Failed to bootstrap local services during maintenance recovery: {ex}")
            
            self.send_json_response(500, {"error": f"Failed to forward request to Vali: {str(e)}"})

    def do_GET(self):
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/v1/cluster/status":
            self.handle_cluster_status()
            return
        elif parsed.path == "/api/v1/node/status":
            self.handle_node_status()
            return
        elif parsed.path == "/api/v1/node/binary-version":
            self.handle_binary_version(parsed)
            return
        elif parsed.path == "/api/v1/vm/drs":
            self.forward_to_vali("/api/v1/drs/status", method="GET")
            return
        elif parsed.path == "/api/v1/hosts":
            self.forward_to_vali("/api/v1/hosts", method="GET")
            return
        elif parsed.path == "/api/v1/urbosa/tunnels/metrics":
            self.handle_urbosa_tunnels_metrics(parsed)
            return
        elif parsed.path == "/api/v1/urbosa/tunnels/status":
            self.handle_urbosa_tunnels_status()
            return

        if self.route_typed_get(parsed):
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if self.path == "/api/v1/execute":
            self.handle_execute()
            return
        elif self.path == "/api/v1/cluster/start":
            self.handle_cluster_start()
            return
        elif self.path == "/api/v1/cluster/stop":
            self.handle_cluster_stop()
            return
        elif self.path == "/api/v1/cluster/create":
            self.handle_cluster_create()
            return
        elif self.path == "/api/v1/cluster/destroy":
            self.handle_cluster_destroy()
            return
        elif self.path == "/api/v1/vm/power":
            self.forward_to_vali("/api/v1/vms/power", method="POST")
            return
        elif self.path == "/api/v1/vm/migrate":
            self.forward_to_vali("/api/v1/vms/migrate", method="POST")
            return
        elif self.path == "/api/v1/vm/balance":
            self.forward_to_vali("/api/v1/vms/balance", method="POST")
            return
        elif self.path == "/api/v1/host/maintenance":
            self.forward_to_vali("/api/v1/hosts/maintenance", method="POST")
            return
        elif self.path == "/api/v1/cluster/sync-settings":
            self.handle_sync_settings()
            return

        if self.route_typed_post(urllib.parse.urlparse(self.path)):
            return

        self.send_response(404)
        self.end_headers()

    def handle_sync_settings(self):
        success, err = sync_cluster_settings_local()
        if success:
            self.send_json_response(200, {"message": "Cluster settings synced successfully."})
        else:
            self.send_json_response(500, {"error": f"Failed to sync cluster settings: {err}"})

    def handle_execute(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length)
        
        try:
            payload = json.loads(post_data.decode('utf-8'))
            command = payload.get("command", "")
            timeout = payload.get("timeout", 45)
        except Exception as e:
            self.send_json_response(400, {"error": "Invalid JSON or payload"})
            return

        import os

        try:
            res = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
            response = {
                "returncode": res.returncode,
                "stdout": res.stdout.decode('utf-8', errors='ignore').strip(),
                "stderr": res.stderr.decode('utf-8', errors='ignore').strip()
            }
        except subprocess.TimeoutExpired as te:
            response = {
                "returncode": -1,
                "stdout": te.stdout.decode('utf-8', errors='ignore').strip() if te.stdout else "",
                "stderr": (te.stderr.decode('utf-8', errors='ignore').strip() if te.stderr else "") + f"\nCommand timed out after {timeout} seconds"
            }
        self.send_json_response(200, response)

    def handle_cluster_status(self):
        import os
        cluster_exists = os.path.exists("/etc/hci/cluster.json")

        # 1. Check if local zookeeper is active
        zk_active = False
        if cluster_exists:
            res = subprocess.run("systemctl is-active zookeeper", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            zk_active = (res.returncode == 0 and res.stdout.decode().strip() == "active")
            
        cluster_state = "stop"
        if zk_active:
            try:
                res_state = subprocess.run("podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 get /cluster_state", 
                                           shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out_state = res_state.stdout.decode("utf-8", errors="ignore")
                if "started" in out_state:
                    cluster_state = "start"
            except Exception:
                pass
        
        # 2. Get Linstor or Gluster status
        peer_status = ""
        volume_info = ""
        if cluster_exists:

            # Standardized on Linstor client
            controller_ip = "127.0.0.1"
            try:
                with open("/etc/hci/cluster.json", "r") as f:
                    cdata = json.load(f)
                    hosts = cdata.get("hosts", [])
                    if hosts:
                        controller_ip = ",".join([h["ip"] for h in hosts])
            except Exception:
                pass
            res_peer = subprocess.run(f"podman exec -e LS_CONTROLLERS={controller_ip} systemd-aether linstor node list", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            peer_status = res_peer.stdout.decode("utf-8", errors="ignore").strip()
            res_vol = subprocess.run(f"podman exec -e LS_CONTROLLERS={controller_ip} systemd-aether linstor resource list", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            volume_info = res_vol.stdout.decode("utf-8", errors="ignore").strip()
        
        # Parse query params for verbose flag
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed.query)
        is_verbose = "verbose" in query_params and query_params["verbose"][0] in ["1", "true", "True"]
        
        if not is_verbose and volume_info:
            filtered_lines = []
            skipping = False
            for line in volume_info.splitlines():
                if "volume name:" in line.lower():
                    skipping = False
                elif "options reconfigured:" in line.lower():
                    skipping = True
                
                if not skipping:
                    filtered_lines.append(line)
            volume_info = "\n".join(filtered_lines).strip()
        
        # 3. Read host list from config
        hosts = []
        try:
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = [h["ip"] for h in cdata.get("hosts", [])]
        except Exception:
            pass
            
        node_statuses = {}
        if not hosts:
            hosts = ["127.0.0.1"]
            
        for ip in hosts:
            rc, data, err = run_mtls_spark_api(ip, "/api/v1/node/status", None, method="GET")
            if rc == 0:
                try:
                    ip_addr = data.get("ip", ip)
                    hostname = data.get("hostname", "").strip()
                    is_leader = data.get("zk_leader", False)
                    leader_str = ", OdinLeader" if is_leader else ""
                    
                    GREEN = "\033[92m"
                    RED = "\033[91m"
                    BOLD = "\033[1m"
                    RESET = "\033[0m"
                    GRAY = "\033[90m"
                    YELLOW = "\033[93m"
                    
                    maint_status = data.get("maintenance_status", "NORMAL")
                    maint_str = ""
                    if maint_status == "IN_MAINTENANCE":
                        maint_str = f" {YELLOW}[MAINTENANCE]{RESET}"
                    elif maint_status == "ENTERING_MAINTENANCE":
                        maint_str = f" {YELLOW}[ENTERING MAINTENANCE]{RESET}"
                    
                    out_lines = []
                    out_lines.append(f"\n        Host: {BOLD}{ip_addr}{RESET} {GREEN}Up{RESET} {GRAY}({hostname}){leader_str}{RESET}{maint_str}")
                    
                    services = data.get("services", {})
                    svc_list = ["ZooKeeper", "HydraDB", "Daruk", "Aether", "Spark", "Spectrum", "Bifrost", "Dagur", "Mimir", "Vali", "Catalyst", "Hylia", "Gatoway", "Logos", "Mipha", "Agahnim", "Slate"]
                    if "Urbosa" in services:
                        svc_list.append("Urbosa")
                    for svc_name in svc_list:
                        svc_data = services.get(svc_name, {"status": "DOWN", "pids": []})
                        status = svc_data.get("status", "DOWN")
                        pids = svc_data.get("pids", [])
                        pid_str = f"{GRAY}[{', '.join(map(str, pids))}]{RESET}" if pids else "[]"
                        if status == "UP":
                            out_lines.append(f"                    {svc_name:<16}   {GREEN}UP{RESET}       {pid_str}")
                        else:
                            out_lines.append(f"                    {svc_name:<16}   {RED}DOWN{RESET}")
                    
                    node_statuses[ip] = {"online": True, "output": "\n".join(out_lines)}
                except Exception as ex:
                    node_statuses[ip] = {"online": True, "output": f"Parse error: {ex}"}
            else:
                node_statuses[ip] = {"online": False, "error": err}
                
        response = {
            "cluster_state": cluster_state,
            "peer_status": peer_status,
            "volume_info": volume_info,
            "node_statuses": node_statuses
        }
        self.send_json_response(200, response)

    def handle_node_status(self):
        self.send_json_response(200, build_node_status())

    def handle_binary_version(self, parsed):
        import urllib.parse
        query = urllib.parse.parse_qs(parsed.query)
        path = query.get("path", [""])[0]
        if not path:
            self.send_json_response(400, {"error": "Missing path parameter"})
            return
        version = get_service_build_number(path)
        self.send_json_response(200, {"version": version})

    def handle_urbosa_tunnels_metrics(self, parsed):
        import urllib.parse
        import json
        query_params = urllib.parse.parse_qs(parsed.query)
        node_ip = query_params.get("node_ip", [None])[0]
        interface_name = query_params.get("interface_name", [None])[0]
        limit = int(query_params.get("limit", [60])[0])
        if not node_ip or not interface_name:
            self.send_json_response(400, {"error": "Missing node_ip or interface_name parameters"})
            return
        
        cql = f"SELECT JSON timestamp, rx_kbps, tx_kbps, rx_packets, tx_packets FROM hydra.urbosa_tunnel_metrics WHERE node_ip = '{node_ip}' AND interface_name = '{interface_name}' LIMIT {limit};"
        rc, stdout, stderr = run_cql_query(cql)
        items = []
        if rc == 0 and stdout:
            for line in stdout.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        items.append(json.loads(line))
                    except Exception:
                        pass
        items.reverse()
        self.send_json_response(200, {"metrics": items})

    def handle_urbosa_tunnels_status(self):
        import json
        
        nodes = []
        try:
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                nodes = cdata.get("hosts", [])
        except Exception:
            pass
            
        if not nodes:
            rc_n, stdout_n, _ = run_cql_query("SELECT JSON hostname, ip FROM hydra.nodes;")
            if rc_n == 0 and stdout_n:
                for line in stdout_n.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            n = json.loads(line)
                            nodes.append({"hostname": n.get("hostname"), "ip": n.get("ip")})
                        except Exception:
                            pass

        cql_seg = "SELECT JSON segment_id, name, vni, t1_link_id, subnet_cidr, gateway_ip FROM hydra.urbosa_segments;"
        rc_s, stdout_s, _ = run_cql_query(cql_seg)
        segments = []
        if rc_s == 0 and stdout_s:
            for line in stdout_s.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        segments.append(json.loads(line))
                    except Exception:
                        pass
        
        metrics_map = {}
        for node in nodes:
            node_ip = node.get("ip")
            if not node_ip:
                continue
            ifaces = []
            for seg in segments:
                vni = seg.get("vni")
                if vni:
                    ifaces.extend([f"vxlan-{vni}", f"br-ov-{vni}"])
            for iface in ifaces:
                cql = f"SELECT JSON node_ip, interface_name, rx_kbps, tx_kbps, rx_packets, tx_packets, timestamp FROM hydra.urbosa_tunnel_metrics WHERE node_ip = '{node_ip}' AND interface_name = '{iface}' LIMIT 1;"
                rc_m, stdout_m, _ = run_cql_query(cql)
                if rc_m == 0 and stdout_m:
                    for line in stdout_m.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                m = json.loads(line)
                                if "node_ip" in m and "interface_name" in m:
                                    key = (m["node_ip"], m["interface_name"])
                                    metrics_map[key] = m
                            except Exception:
                                pass
        
        tunnel_stats = []
        for node in nodes:
            node_ip = node.get("ip")
            node_name = node.get("hostname", node.get("name", node_ip))
            for seg in segments:
                vni = seg.get("vni")
                if not vni:
                    continue
                ifaces = [f"vxlan-{vni}", f"br-ov-{vni}"]
                for iface in ifaces:
                    metric = metrics_map.get((node_ip, iface), {})
                    tunnel_stats.append({
                        "node_ip": node_ip,
                        "node_name": node_name,
                        "interface_name": iface,
                        "vni": vni,
                        "segment_name": seg.get("name"),
                        "rx_kbps": metric.get("rx_kbps", 0.0),
                        "tx_kbps": metric.get("tx_kbps", 0.0),
                        "rx_packets": metric.get("rx_packets", 0.0),
                        "tx_packets": metric.get("tx_packets", 0.0),
                        "timestamp": metric.get("timestamp", 0)
                    })
        self.send_json_response(200, {"tunnels": tunnel_stats})

    def handle_cluster_start(self):
        hosts = []
        try:
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = [h["ip"] for h in cdata.get("hosts", [])]
        except Exception:
            pass
            
        if not hosts:
            self.send_json_response(400, {"error": "No hosts configured. Please run cluster create first."})
            return

        try:
            # Start zookeeper on all nodes
            run_parallel_checked(hosts, "systemctl start zookeeper")
            time.sleep(3)
            
            # Set cluster state to started in ZooKeeper
            execute_checked("podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 create /cluster_state started || podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 set /cluster_state started", allow_already_exists=True)
            
            # Start hydra-db on all hosts
            print("[handle_cluster_start] Starting hydra-db on all nodes...")
            run_parallel_checked(hosts, "systemctl start hydra-db")
            
            # Wait for ScyllaDB to listen on port 9042
            for ip in hosts:
                for _ in range(60):
                    rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9042")
                    if rc == 0 and "9042" in out:
                        break
                    time.sleep(1)
                else:
                    raise Exception(f"ScyllaDB failed to listen on port 9042 on {ip}")
            
            # Copy Daruk proxy script to ScyllaDB volume directory (in case it was wiped or needs sync)
            run_parallel_checked(hosts, "mkdir -p /var/lib/hci/hydra/data && cp /usr/local/bin/daruk.py /var/lib/hci/hydra/data/daruk.py && chmod 644 /var/lib/hci/hydra/data/daruk.py")
 
            # Start and verify Daruk query proxy
            print("[handle_cluster_start] Starting and verifying Daruk on all nodes...")
            run_parallel_checked(hosts, "systemctl start daruk")
            for ip in hosts:
                for _ in range(30):
                    rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9043")
                    if rc == 0 and "9043" in out:
                        break
                    time.sleep(1)
                else:
                    raise Exception(f"Daruk proxy failed to listen on port 9043 on {ip}")
 
            # Start linstor-controller
            # run_checked_cmd is defined in cluster_new.py, not here -- calling it raised
            # NameError and broke this path. run_parallel_checked has identical semantics
            # (checked remote exec, raises on failure) and takes a list.
            run_parallel_checked([hosts[0]], "systemctl start linstor-controller")
            for ip in hosts[1:]:
                run_remote_spark(ip, "systemctl stop linstor-controller")
            # Wait for Linstor controller
            leader_ip = hosts[0]
            for _ in range(30):
                rc, out, _ = run_remote_spark(leader_ip, "ss -tlnp | grep 3370")
                if rc == 0 and "3370" in out:
                    break
                time.sleep(1)
            else:
                raise Exception(f"Linstor Controller failed to start on port 3370 on {leader_ip}")
 
            # Start other workloads
            services = ["aether", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha"]
            if check_urbosa_enabled():
                services.append("urbosa")
            for svc in services:
                run_parallel_checked(hosts, f"systemctl start {svc}")
                
            # Sync cluster settings from ScyllaDB to resolv.conf/chrony.conf/timezone on all nodes
            print("[handle_cluster_start] Syncing cluster settings on all hosts...")
            for ip in hosts:
                run_mtls_spark_api(ip, "/api/v1/cluster/sync-settings", None, method="POST")
                
            # Standardized on Linstor/DRBD storage engine (legacy container mounts skipped)
            pass
                
            self.send_json_response(200, {"message": "Cluster start command completed successfully."})
        except Exception as ex:
            import traceback
            traceback.print_exc()
            self.send_json_response(500, {"error": f"Cluster start failed: {str(ex)}"})
            return

    def handle_cluster_stop(self):
        hosts = []
        try:
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = [h["ip"] for h in cdata.get("hosts", [])]
        except Exception:
            pass
            
        if not hosts:
            self.send_json_response(400, {"error": "No hosts configured."})
            return

        # Set ZooKeeper cluster_state to stopped
        subprocess.run("podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 set /cluster_state stopped", shell=True)
        
        run_parallel(hosts, "umount -f /var/lib/hci/aether/volumes/default-vm-container || true")
        run_parallel(hosts, "umount -f /var/lib/hci/aether/volumes/default-image-container || true")
        run_parallel(hosts, "umount -l /var/lib/linstor || true")
        run_parallel(hosts, "drbdadm down all || true")
        
        # Stop services
        services = ["logos", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "linstor-controller", "aether", "hydra-db", "zookeeper"]
        for svc in services:
            run_parallel(hosts, f"systemctl stop {svc}")
            
        # Restart spark-daemons asynchronously
        def restart_worker(ip):
            run_remote_spark(ip, "(sleep 1 && systemctl restart spark-daemon) >/dev/null 2>&1 < /dev/null &")
            
        for ip in hosts:
            t = threading.Thread(target=restart_worker, args=(ip,))
            t.start()
            
        self.send_json_response(200, {"message": "Cluster stop command completed."})

    def handle_cluster_create(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length)
        try:
            payload = json.loads(post_data.decode('utf-8'))
            servers = payload.get("servers", [])
            redundancy_factor = int(payload.get("redundancy_factor", 1))
            if len(servers) == 1:
                redundancy_factor = 0
            vip = payload.get("vip", "")
        except Exception as e:
            self.send_json_response(400, {"error": f"Invalid payload: {str(e)}"})
            return
            
        if not servers:
            self.send_json_response(400, {"error": "No servers specified."})
            return

        try:
            # Retrieve hostnames
            hosts_info = []
            for idx, ip in enumerate(servers):
                rc, hostname, _ = run_remote_spark(ip, "hostname")
                hostname = hostname.strip() if rc == 0 else f"node-{idx+1}"
                hosts_info.append({
                    "node_id": idx + 1,
                    "ip": ip,
                    "hostname": hostname
                })
                
            cluster_json_data = {
                "cluster_name": "hci-01",
                "redundancy_factor": redundancy_factor,
                "dfs_engine": "linstor",
                "vip": vip,
                "hosts": hosts_info
            }
            
            json_b64 = base64.b64encode(json.dumps(cluster_json_data, indent=4).encode('utf-8')).decode('utf-8')
            write_config_cmd = f"mkdir -p /etc/hci && echo {json_b64} | base64 -d > /etc/hci/cluster.json"
            run_parallel_checked(servers, write_config_cmd)

            # Configure SELinux permanently to Permissive on all nodes to prevent helper command failures
            run_parallel_checked(servers, "setenforce 0 || true; sed -i 's/SELINUX=enforcing/SELINUX=permissive/g' /etc/selinux/config || true")
            
            # Start storage engine (linstor-controller and satellite/aether on all)
            run_parallel_checked(servers, "systemctl start aether")
            run_parallel_checked([servers[0]], "systemctl start linstor-controller")
            for ip in servers[1:]:
                run_remote_spark(ip, "systemctl stop linstor-controller")
            # Wait for Linstor controller API to start listening on port 3370 on the leader server
            leader_ip = servers[0]
            for _ in range(30):
                rc, out, _ = run_remote_spark(leader_ip, "ss -tlnp | grep 3370")
                if rc == 0 and "3370" in out:
                    break
                time.sleep(1)
            else:
                raise Exception(f"Linstor Controller failed to start on port 3370 on {leader_ip}")
            
            # Set Linstor DRBD port range to avoid conflict with ScyllaDB port 7000
            for ip in servers:
                run_remote_spark(ip, "podman exec systemd-aether linstor controller set-property TcpPortAutoRange 7700-7890")
            
            # Setup Linstor nodes and storage pools
            for h in hosts_info:
                execute_checked(f"podman exec systemd-aether linstor node create {h['hostname']} {h['ip']}", allow_already_exists=True)
                
            # Dynamic Disk Setup (Non-boot disks >= 100GB)
            disk_claim_script = """
import subprocess, json, sys, os
res_vg = subprocess.run("vgs vg_aether --noheadings -o pv_name", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
pvs = []
if res_vg.returncode == 0:
    pvs = [line.strip() for line in res_vg.stdout.decode().splitlines() if line.strip()]

if pvs:
    dev = pvs[0]
    res_lv = subprocess.run("lvs vg_aether/thin_pool_aether", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res_lv.returncode != 0:
        subprocess.run("lvcreate -y -l 100%FREE -T vg_aether/thin_pool_aether", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res_pv_sz = subprocess.run("pvs " + dev + " --units b --noheadings -o pv_size", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    size_bytes = 200 * 10**9
    if res_pv_sz.returncode == 0:
        val = res_pv_sz.stdout.decode().strip().lower().replace("b", "")
        try: size_bytes = int(val)
        except: pass
    print(json.dumps({"status": "exists", "device": dev, "size_bytes": size_bytes}))
    sys.exit(0)

res_lsblk = subprocess.run("lsblk -b -d -n -o NAME,SIZE,TYPE,ROTA", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
if res_lsblk.returncode != 0:
    print(json.dumps({"error": "lsblk failed"}))
    sys.exit(1)

candidate = None
for line in res_lsblk.stdout.decode().splitlines():
    parts = line.split()
    if len(parts) >= 4 and parts[2] == "disk":
        name = parts[0]
        try: size_bytes = int(parts[1])
        except ValueError: continue
        dev_path = "/dev/" + name
        # A claimed disk is wiped, so skip any disk with ANY non-empty mountpoint anywhere in
        # its tree (system path, /srv, /data, swap, ...) -- an in-use disk is never a candidate.
        res_m = subprocess.run("lsblk -n -o MOUNTPOINT " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        is_in_use = False
        for m in res_m.stdout.decode().splitlines():
            m = m.strip()
            if (m and m != "-") or "swap" in m.lower():
                is_in_use = True
                break
        if is_in_use: continue
        res_p = subprocess.run("lsblk -n -o TYPE " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if "part" in res_p.stdout.decode().splitlines(): continue
        if size_bytes >= 100 * 10**9:
            candidate = (dev_path, size_bytes)
            break

if not candidate:
    print(json.dumps({"error": "No empty disk >= 100GB found"}))
    sys.exit(1)

dev_path, size_bytes = candidate
subprocess.run("wipefs -a " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# Zero first 1024MB and last 1024MB of the raw disk to ensure no old DRBD metadata interferes
subprocess.run("dd if=/dev/zero of=" + dev_path + " bs=1M count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
seek_val = (size_bytes // 1048576) - 1024
subprocess.run("dd if=/dev/zero of=" + dev_path + " bs=1M seek=" + str(seek_val) + " count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("pvcreate -y " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("vgcreate vg_aether " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("lvcreate -y -l 100%FREE -T vg_aether/thin_pool_aether", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
print(json.dumps({"status": "created", "device": dev_path, "size_bytes": size_bytes}))
"""
            claim_script_b64 = base64.b64encode(disk_claim_script.strip().encode()).decode()
            cmd_claim = f"python3 -c \"import base64; exec(base64.b64decode('{claim_script_b64}').decode())\""
            claim_results = run_parallel_checked(servers, cmd_claim)
            
            host_claimed_disks = {}
            for ip, (rc, stdout, stderr) in claim_results.items():
                try:
                    disk_info = json.loads(stdout.strip())
                    if "error" in disk_info:
                        raise Exception(f"Host {ip} disk setup failed: {disk_info['error']}")
                    host_claimed_disks[ip] = disk_info
                except Exception as e:
                    raise Exception(f"Host {ip} returned invalid json: {stdout} ({e})")
            
            udev_helper = UdevHelper(servers)
            udev_helper.start()
            try:
                for h in hosts_info:
                    execute_checked(f"podman exec systemd-aether linstor storage-pool create lvmthin {h['hostname']} default-pool vg_aether/thin_pool_aether", allow_already_exists=True)
                time.sleep(2)
                # Create Linstor resource definitions (default containers skipped for Linstor engine)
                pass
                
                # Create linstor-db DRBD volume for database HA
                print("Creating linstor-db DRBD resource definition for database HA...")
                execute_checked("podman exec systemd-linstor-controller linstor resource-definition create linstor-db", allow_already_exists=True)
                execute_checked("podman exec systemd-linstor-controller linstor volume-definition create linstor-db 5G", allow_already_exists=True)

                # Set automatic split-brain resolution policy for linstor-db database resource
                execute_checked("podman exec systemd-linstor-controller linstor resource-definition drbd-options --after-sb-0pri discard-zero-changes --after-sb-1pri discard-secondary --after-sb-2pri disconnect linstor-db", allow_already_exists=True)

                print("Deploying replicated database storage volume across all nodes...")
                for h in hosts_info:
                    execute_checked(f"podman exec systemd-linstor-controller linstor resource create {h['hostname']} linstor-db --storage-pool default-pool", allow_already_exists=True)

                print("Waiting for linstor-db DRBD block device to appear on leader...")
                db_drbd_ready = False
                for _ in range(45):
                    rc_db, _, _ = run_remote_spark(servers[0], "test -b /dev/drbd/by-res/linstor-db/0")
                    if rc_db == 0:
                        db_drbd_ready = True
                        break
                    time.sleep(1)
                if not db_drbd_ready:
                    raise Exception("linstor-db DRBD block device did not appear within timeout.")

                print("Formatting linstor-db block device with XFS...")
                execute_checked("mkfs.xfs -f /dev/drbd/by-res/linstor-db/0")
            finally:
                udev_helper.stop()

            print("Migrating local database to the replicated linstor-db volume...")
            # 1. Stop controller to release database lock
            execute_checked("systemctl stop linstor-controller")
            # 2. Mount DRBD volume to temp directory
            execute_checked("mkdir -p /mnt/linstordb-temp && mount -t xfs /dev/drbd/by-res/linstor-db/0 /mnt/linstordb-temp")
            # 3. Copy files preserving permissions
            execute_checked("cp -a /var/lib/linstor/. /mnt/linstordb-temp/")
            # 4. Unmount temp directory
            execute_checked("umount -f /mnt/linstordb-temp")
            # 5. Clear local directory and mount DRBD volume to /var/lib/linstor
            execute_checked("rm -rf /var/lib/linstor/* && mount -t xfs /dev/drbd/by-res/linstor-db/0 /var/lib/linstor")
            # 6. Restart controller (it is now backed by the DRBD volume!)
            execute_checked("systemctl start linstor-controller")

            # Verify Node 1 controller is back online
            controller_ready = False
            for _ in range(30):
                rc_check, out_check, _ = run_remote_spark(servers[0], "ss -tlnp | grep 3370")
                if rc_check == 0 and "3370" in out_check:
                    controller_ready = True
                    break
                time.sleep(1)
            if not controller_ready:
                raise Exception("Linstor Controller failed to restart on leader after database migration.")

            print("Cleaning up local database directories and stopping standby nodes...")
            for target_ip in servers[1:]:
                run_remote_spark(target_ip, "systemctl stop linstor-controller")
                run_remote_spark(target_ip, "umount -l /var/lib/linstor || true")
                run_remote_spark(target_ip, "rm -rf /var/lib/linstor/*")
                run_remote_spark(target_ip, "drbdadm secondary linstor-db || true")

            print("Waiting for linstor-db DRBD replication to sync and reach UpToDate status cluster-wide...")
            db_synced = False
            for i in range(120): # up to 4 minutes
                rc_stat, out_stat, _ = run_remote_spark(servers[0], "drbdadm status linstor-db")
                if rc_stat == 0:
                    out_lower = out_stat.lower()
                    if "inconsistent" not in out_lower and "sync" not in out_lower and "uptodate" in out_lower:
                        if out_lower.count("uptodate") >= len(servers):
                            db_synced = True
                            print("linstor-db is fully synchronized and UpToDate on all nodes.")
                            break
                time.sleep(2)
            if not db_synced:
                print("[WARNING] linstor-db replication did not fully sync within timeout. Disk status:")
                rc_stat, out_stat, _ = run_remote_spark(servers[0], "drbdadm status linstor-db")
                print(out_stat)
            
            # Write storage-pools.json with linstor engine
            for ip in servers:
                disk_info = host_claimed_disks[ip]
                storage_pool_json = {
                    "storage_pool_name": "default-pool",
                    "dfs_engine": "linstor",
                    "local_disks": [{
                        "device": disk_info["device"],
                        "role": "data",
                        "media_type": "ssd",
                        "fs_type": "xfs",
                        "size_bytes": disk_info["size_bytes"],
                        "brick_path": f"/var/lib/hci/aether/bricks/{os.path.basename(disk_info['device'])}/brick"
                    }],
                    "storage_containers": []
                }
                json_str = json.dumps(storage_pool_json, indent=2)
                b64_str = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
                run_remote_spark(ip, f"mkdir -p /etc/hci/aether && echo {b64_str} | base64 -d > /etc/hci/aether/storage-pools.json")
                
                controllers_line = ",".join(servers)
                client_conf = f"[active]\ncontrollers = {controllers_line}\n"
                client_b64 = base64.b64encode(client_conf.encode('utf-8')).decode('utf-8')
                run_remote_spark(ip, f"mkdir -p /etc/linstor && echo {client_b64} | base64 -d > /etc/linstor/linstor-client.conf")
                
            # Write spectrum.env
            seeds = ",".join(servers)
            for ip in servers:
                spectrum_env = f"SPECTRUM_API_PORT=8443\nLOCAL_HYPERVISOR_IP={ip}\nCLUSTER_SEEDS={seeds}"
                env_b64 = base64.b64encode(spectrum_env.encode('utf-8')).decode('utf-8')
                run_remote_spark(ip, f"mkdir -p /etc/hci/spectrum && echo {env_b64} | base64 -d > /etc/hci/spectrum/spectrum.env")
                
            # Create local directories for images and nvram configs
            run_parallel_checked(servers, "mkdir -p /var/lib/hci/aether/images /var/lib/hci/aether/nvram")
            
            # Mount default volumes (skipped for Linstor engine)
            pass
            
            # Restart zookeeper and DB to form ring
            print("Writing dynamic ZooKeeper container configs on all hosts...")
            if len(servers) == 1:
                zoo_servers_env = ""
            else:
                zoo_servers_parts = []
                for i, ip in enumerate(servers, start=1):
                    if i > 3:
                        zoo_servers_parts.append(f"server.{i}={ip}:2888:3888:observer;2181")
                    else:
                        zoo_servers_parts.append(f"server.{i}={ip}:2888:3888;2181")
                zoo_servers_str = " ".join(zoo_servers_parts)
                zoo_servers_env = f' ZOO_SERVERS="{zoo_servers_str}"'

            for idx, ip in enumerate(servers):
                node_id = idx + 1
                peer_type_env = " ZOO_PEER_TYPE=observer" if node_id > 3 else ""
                zk_quad = (
                    "[Unit]\n"
                    "Description=ZooKeeper Cluster Consensus Service\n"
                    "After=network.target\n\n"
                    "[Service]\n"
                    "Restart=always\n"
                    "CPUWeight=100\n"
                    "MemoryMax=512M\n"
                    "MemoryHigh=400M\n\n"
                    "[Container]\n"
                    "Image=docker.io/library/zookeeper:3.9.2\n"
                    "Network=host\n"
                    "Volume=/var/lib/hci/zookeeper/data:/data:Z\n"
                    "Volume=/var/lib/hci/zookeeper/log:/datalog:Z\n"
                    f"Environment=ZOO_MY_ID={node_id}{zoo_servers_env}{peer_type_env} ZOO_4LW_COMMANDS_WHITELIST=*\n\n"
                    "[Install]\n"
                    "WantedBy=multi-user.target\n"
                )
                zk_b64 = base64.b64encode(zk_quad.encode()).decode()
                run_remote_spark(ip, f"mkdir -p /etc/containers/systemd && echo {zk_b64} | base64 -d > /etc/containers/systemd/zookeeper.container && systemctl daemon-reload")

            run_parallel_checked(servers, "systemctl restart zookeeper")
            
            # Copy Daruk proxy script to ScyllaDB volume directory on all servers
            run_parallel_checked(servers, "mkdir -p /var/lib/hci/hydra/data && cp /usr/local/bin/daruk.py /var/lib/hci/hydra/data/daruk.py && chmod 644 /var/lib/hci/hydra/data/daruk.py")
            
            run_parallel_checked(servers, "systemctl restart hydra-db")
            
            # Wait for ScyllaDB to listen on port 9042 on all servers
            for ip in servers:
                for _ in range(120):
                    rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9042")
                    if rc == 0 and "9042" in out:
                        break
                    time.sleep(1)
                else:
                    raise Exception(f"ScyllaDB failed to listen on port 9042 on {ip}")
                    
            # Start and verify Daruk query proxy on all servers
            run_parallel_checked(servers, "systemctl restart daruk")
            for ip in servers:
                for _ in range(30):
                    rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9043")
                    if rc == 0 and "9043" in out:
                        break
                    time.sleep(1)
                else:
                    raise Exception(f"Daruk proxy failed to listen on port 9043 on {ip}")
            
            # Start spectrum and other services
            services = ["spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha"]
            for svc in services:
                run_parallel_checked(servers, f"systemctl start {svc}")
                for ip in servers:
                    for _ in range(30):
                        rc, out, _ = run_remote_spark(ip, f"systemctl is-active {svc}")
                        if rc == 0 and out.strip() == "active":
                            break
                        time.sleep(1)
                    else:
                        raise Exception(f"Service {svc} failed to enter active state on {ip}")

            # Verification & Liveness Check Loop
            # Poll ScyllaDB Gossip Status until all nodes are Up-Normal (UN)
            gossip_healthy = False
            for i in range(30):
                rc, out, _ = run_remote_spark(servers[0], "podman exec systemd-hydra-db nodetool status")
                if rc == 0:
                    un_count = 0
                    for line in out.splitlines():
                        if line.strip().startswith("UN"):
                            un_count += 1
                    if un_count >= len(servers):
                        gossip_healthy = True
                        break
                time.sleep(5)
            if not gossip_healthy:
                raise Exception("ScyllaDB Gossip ring failed to stabilize in UN state on all nodes")

            # Verify Spectrum Web UI reachability on port 8443
            for ip in servers:
                reached = False
                for _ in range(20):
                    rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 8443")
                    if rc == 0 and "8443" in out:
                        reached = True
                        break
                    time.sleep(2)
                if not reached:
                    raise Exception(f"Spectrum UI is unreachable on {ip}:8443")

            self.send_json_response(200, {"message": "Cluster created and verified successfully."})
            return
        except Exception as ex:
            import traceback
            traceback.print_exc()
            self.send_json_response(500, {"error": f"Cluster creation failed: {str(ex)}"})
            return
    def handle_cluster_destroy(self):
        hosts = []
        
        # 0. Read hosts from payload or cluster.json
        payload_hosts = []
        try:
            if hasattr(self, "payload") and isinstance(self.payload, dict):
                payload_hosts = self.payload.get("servers", [])
        except Exception:
            pass
            
        try:
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = [h["ip"] for h in cdata.get("hosts", [])]
        except Exception:
            pass
            
        if payload_hosts:
            hosts = list(set(hosts + payload_hosts))
            
        if not hosts:
            hosts = ["127.0.0.1"]

        # 0.5. Storage disks are NOT resolved here. Each host discovers its own devices at wipe
        # time (see the wipe plan script below): reading this node's storage-pools.json and
        # broadcasting those device names would wipe the wrong disk on any host whose storage
        # sits elsewhere (e.g. /dev/nvme0n1).

        # 1. Stop and Delete Storage Volumes/Resources (Standardized on Linstor/DRBD)
        pass
                        
        # 2. Stop services on all hosts in parallel
        # 2. Stop services on all hosts in parallel
        services = ["logos", "mipha", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "linstor-controller", "aether", "daruk", "hydra-db", "zookeeper"]
        svc_list = " ".join(services)
        run_parallel(hosts, f"systemctl stop {svc_list} || true")
        
        # Stop and undefine all libvirt VMs
        vm_cleanup_cmd = "for vm in $(virsh list --all --name); do virsh destroy $vm || true; virsh undefine $vm --nvram || true; done"
        run_parallel(hosts, vm_cleanup_cmd)
        
        # 3. Unmount and wipe bricks, clear databases on all hosts
        run_parallel(hosts, "umount -l /var/lib/hci/aether/volumes/default-vm-container || true")
        run_parallel(hosts, "umount -l /var/lib/hci/aether/volumes/default-image-container || true")
        
        drbd_down_cmd = (
            "drbdsetup status | grep -v '^[[:space:]]' | grep -v '^#' | while read -r line; do "
            "  res=$(echo \"$line\" | awk '{print $1}'); "
            "  if [ ! -z \"$res\" ]; then "
            "    echo \"Bringing down DRBD resource $res...\"; "
            "    drbdsetup down \"$res\" || true; "
            "  fi; "
            "done"
        )
        run_parallel(hosts, drbd_down_cmd)
        # Wipe the LVM thin pool and VG (device independent) on every host first, so the storage
        # disks are left as bare unmounted devices before the signature wipe discovers them.
        lvm_wipe_cmd = "lvchange -an -f /dev/vg_aether/* || true; lvremove -y -f vg_aether || true; vgremove -y -f vg_aether || true; rm -rf /dev/vg_aether || true; dmsetup ls | grep vg_aether | awk '{print $1}' | while read -r dm; do dmsetup remove -f \"$dm\" || true; done"
        try:
            run_parallel(hosts, lvm_wipe_cmd)
        except Exception:
            pass

        # Discover and zero the physical storage disks this cluster actually claimed. Every host
        # resolves its own devices (storage-pools.json, vg_aether/orphaned PVs, scan of raw
        # unmounted disks >= 100GB); there is NO hardcoded device fallback, so a host that
        # matches nothing is a clean no-op instead of wiping a guessed device name.
        wipe_devices_script = """
import subprocess, json, sys, os
devs = []
reasons = {}
skipped = []

def add_dev(dev, reason):
    if dev and dev not in devs:
        devs.append(dev)
        reasons[dev] = reason

def mountpoints_of(dev):
    mounts = []
    res_m = subprocess.run("lsblk -n -o MOUNTPOINT " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for m in res_m.stdout.decode().splitlines():
        m = m.strip()
        if m and m != "-" and m not in mounts:
            mounts.append(m)
    return mounts

def size_of(dev):
    res_sz = subprocess.run("blockdev --getsize64 " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res_sz.returncode == 0:
        try: return int(res_sz.stdout.decode().strip())
        except ValueError: return -1
    return -1

def signatures_of(dev):
    sigs = []
    res_w = subprocess.run("wipefs " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in res_w.stdout.decode().splitlines()[1:]:
        cols = line.split()
        if len(cols) >= 3 and cols[2] not in sigs:
            sigs.append(cols[2])
    return sigs

# 0. Disks this node recorded as its own Aether storage pool members (authoritative)
try:
    with open("/etc/hci/aether/storage-pools.json", "r") as f:
        spdata = json.load(f)
    for disk in spdata.get("local_disks", []):
        add_dev(disk.get("device"), "configured in storage-pools.json")
except Exception:
    pass

# 1. Find PVs of vg_aether or orphaned PVs
res_pvs = subprocess.run("pvs --noheadings -o pv_name,vg_name", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
if res_pvs.returncode == 0:
    for line in res_pvs.stdout.decode().splitlines():
        parts = line.split()
        if len(parts) >= 1:
            pv = parts[0].strip()
            vg = parts[1].strip() if len(parts) >= 2 else ""
            if vg in ["vg_aether", ""]:
                add_dev(pv, "LVM PV (vg=" + (vg if vg else "orphaned") + ")")

# 2. Scan for candidate disks >= 100GB (unmounted, no partitions)
res_lsblk = subprocess.run("lsblk -b -d -n -o NAME,SIZE,TYPE,ROTA", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
if res_lsblk.returncode == 0:
    for line in res_lsblk.stdout.decode().splitlines():
        parts = line.split()
        if len(parts) >= 4 and parts[2] == "disk":
            name = parts[0]
            try: size_bytes = int(parts[1])
            except ValueError: continue
            dev_path = "/dev/" + name
            if dev_path in devs: continue
            # Skip any disk carrying ANY non-empty mountpoint anywhere in its tree, not just
            # recognised system paths: a disk mounted at /srv or /data is in use, not a candidate.
            skip_reason = ""
            for m in mountpoints_of(dev_path):
                if "swap" in m.lower():
                    skip_reason = "active swap (" + m + ")"
                else:
                    skip_reason = "mounted at " + m
                break
            if not skip_reason:
                res_p = subprocess.run("lsblk -n -o TYPE " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if "part" in res_p.stdout.decode().splitlines():
                    skip_reason = "disk has partitions"
            if not skip_reason and size_bytes < 100 * 10**9:
                skip_reason = "smaller than 100GB (" + ("%.1f" % (size_bytes / 10.0**9)) + " GB)"
            if skip_reason:
                skipped.append((dev_path, skip_reason))
                continue
            add_dev(dev_path, "unpartitioned unmounted disk >= 100GB")

# 3. Final veto: never touch a device that is missing or still has anything mounted on it,
#    whichever source proposed it.
vetted = []
for dev in devs:
    if not os.path.exists(dev):
        skipped.append((dev, "device not present on this host"))
        continue
    mounts = mountpoints_of(dev)
    swap_mounts = [m for m in mounts if "swap" in m.lower()]
    if swap_mounts:
        skipped.append((dev, "active swap (" + ",".join(swap_mounts) + ") -- refusing to wipe"))
        continue
    if mounts:
        skipped.append((dev, "still mounted at " + ",".join(mounts) + " -- refusing to wipe"))
        continue
    vetted.append(dev)
devs = vetted

# 4. Print the exact wipe set (and every rejection) before destroying anything
print("=== cluster destroy: disk wipe plan for this host ===")
for dev, why in skipped:
    print("  SKIP  " + dev + " -- " + why)
if not devs:
    print("  No qualifying devices found. Nothing will be wiped on this host.")
    print("=== end of wipe plan (no-op) ===")
    sys.exit(0)
for dev in devs:
    size_bytes = size_of(dev)
    size_str = ("%.1f GB" % (size_bytes / 10.0**9)) if size_bytes > 0 else "unknown"
    sigs = signatures_of(dev)
    mounts = mountpoints_of(dev)
    print("  WIPE  " + dev + " -- size=" + size_str + " signatures=" + (",".join(sigs) if sigs else "none") + " mountpoints=" + (",".join(mounts) if mounts else "none") + " reason=" + reasons.get(dev, "unknown"))
print("=== wiping " + str(len(devs)) + " device(s): " + ", ".join(devs) + " ===")

for dev in devs:
    subprocess.run("pvremove -y -f " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if os.path.exists("/etc/lvm/devices/system.devices"):
        dev_name = dev.split("/")[-1]
        subprocess.run("sed -i '/" + dev_name + "/d' /etc/lvm/devices/system.devices", shell=True)
    subprocess.run("wipefs -a -f " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    subprocess.run("dd if=/dev/zero of=" + dev + " bs=1M count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    size_bytes = size_of(dev)
    if size_bytes > 0:
        seek_val = (size_bytes // 1048576) - 1024
        if seek_val > 0:
            subprocess.run("dd if=/dev/zero of=" + dev + " bs=1M seek=" + str(seek_val) + " count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        print("Failed to determine size of " + dev + "; skipped zeroing end of device")
    print("Wiped " + dev)
"""
        wipe_devices_b64 = base64.b64encode(wipe_devices_script.strip().encode()).decode()
        cmd_wipe_devices = f"python3 -c \"import base64; exec(base64.b64decode('{wipe_devices_b64}').decode())\""
        wipe_results = run_parallel(hosts, cmd_wipe_devices)
        for wip, (rc_pv, out_pv, err_pv) in wipe_results.items():
            if out_pv.strip():
                print(f"[{wip}] Wipe log:\n{out_pv}")
            if rc_pv != 0:
                print(f"[{wip}] [WARNING] Wipe execution failed: {err_pv}")

        wipe_script = """
import subprocess
import os
res = subprocess.run("lsblk -n -o NAME,MOUNTPOINT", shell=True, stdout=subprocess.PIPE)
out = res.stdout.decode()
claimed = []
for line in out.splitlines():
    if '/var/lib/hci/aether/bricks/' in line:
        parts = line.split()
        if len(parts) >= 2:
            claimed.append((f"/dev/{parts[0]}", parts[1]))

try:
    with open("/etc/fstab", "r") as f:
        for line in f:
            if '/var/lib/hci/aether/bricks/' in line:
                parts = line.split()
                if len(parts) >= 2:
                    dev_path = parts[0]
                    mount_point = parts[1]
                    if not any(c[1] == mount_point for c in claimed):
                        claimed.append((dev_path, mount_point))
except Exception:
    pass

for dev, mount in claimed:
    real_dev = dev
    if dev.startswith("UUID="):
        uuid_val = dev.split("=", 1)[1]
        uuid_path = f"/dev/disk/by-uuid/{uuid_val}"
        if os.path.exists(uuid_path):
            real_dev = os.path.realpath(uuid_path)
        else:
            res_ff = subprocess.run(f"findfs {dev}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if res_ff.returncode == 0:
                real_dev = res_ff.stdout.decode().strip()
    subprocess.run(f"umount -l {mount}", shell=True)
    subprocess.run(f"sed -i '\\\\|{mount}|d' /etc/fstab", shell=True)
    subprocess.run(f"wipefs -a {real_dev}", shell=True)
    subprocess.run(f"rm -rf {mount}", shell=True)

# Clean up DRBD devices and Linstor directories
subprocess.run("umount -l /var/lib/linstor || true", shell=True)
subprocess.run("drbdadm down all || true", shell=True)
subprocess.run("podman rm -f systemd-hydra-db systemd-zookeeper systemd-aether systemd-spectrum systemd-linstor-controller systemd-linstor-satellite || true", shell=True)
subprocess.run("rm -rf /var/lib/hci/zookeeper/data /var/lib/hci/zookeeper/log /var/lib/hci/hydra/data /var/lib/hci/aether/data /var/lib/hci/aether/volumes /var/lib/hci/aether/images /var/lib/hci/aether/nvram /run/hci/*", shell=True)
subprocess.run("rm -rf /etc/hci/odin /etc/hci/spectrum /etc/hci/cluster.json /var/lib/linstor /etc/linstor", shell=True)
"""
        wipe_b64 = base64.b64encode(wipe_script.encode()).decode()
        cmd_wipe = f"python3 -c \"import base64; exec(base64.b64decode('{wipe_b64}').decode())\""
        run_parallel(hosts, cmd_wipe)
        
        # Restart spark daemon asynchronously
        def cleanup_spark(ip):
            run_remote_spark(ip, "(sleep 1 && systemctl restart spark-daemon) >/dev/null 2>&1 < /dev/null &")
            
        for ip in hosts:
            t = threading.Thread(target=cleanup_spark, args=(ip,))
            t.start()
            
        self.send_json_response(200, {"message": "Cluster destroyed successfully."})

    # ------------------------------------------------------------------
    # Typed API (docs/spark_api.md)
    # ------------------------------------------------------------------

    def read_json_payload(self):
        """Read the request body as a JSON object. Returns (payload, error)."""
        try:
            length = int(self.headers.get('Content-Length', 0) or 0)
        except (TypeError, ValueError):
            return None, "Invalid Content-Length header"
        raw = self.rfile.read(length) if length > 0 else b""
        if not raw:
            return {}, None
        try:
            payload = json.loads(raw.decode('utf-8'))
        except Exception:
            return None, "Invalid JSON body"
        if not isinstance(payload, dict):
            return None, "JSON body must be an object"
        return payload, None

    def reject(self, message, status=400):
        """Every rejection has the same shape: {"error": "..."}."""
        self.send_json_response(status, {"error": message})

    def query_param(self, parsed, key):
        values = urllib.parse.parse_qs(parsed.query).get(key, [])
        return values[0] if values else None

    def route_typed_get(self, parsed):
        """Dispatch the typed read endpoints. True when the request was handled."""
        path = parsed.path

        if path == "/api/v1/storage/drbd/status":
            self.handle_storage_drbd_status(parsed)
            return True
        if path == "/api/v1/storage/device":
            self.handle_storage_device(parsed)
            return True
        if path == "/api/v1/storage/container/mounted":
            self.handle_storage_container_mounted(parsed)
            return True
        if path == "/api/v1/storage/linstor/resources":
            self.handle_storage_linstor_resources(parsed)
            return True
        if path == "/api/v1/host/network":
            self.handle_host_network()
            return True
        if path == "/api/v1/host/memory":
            self.handle_host_memory()
            return True
        if path == "/api/v1/host/disks":
            self.handle_host_disks()
            return True
        if path == "/api/v1/host/capabilities":
            self.handle_host_capabilities()
            return True
        if path == "/api/v1/host/dhcp-leases":
            self.handle_host_dhcp_leases()
            return True
        if path == "/api/v1/db/ring":
            self.handle_db_ring()
            return True

        segments = [segment for segment in path.split("/") if segment]
        if len(segments) == 5 and segments[0:3] == ["api", "v1", "vm"]:
            action = segments[4]
            if action not in ("interfaces", "console", "info"):
                return False
            name = urllib.parse.unquote(segments[3])
            if not valid_name(name):
                self.reject("Invalid VM name")
                return True
            if action == "interfaces":
                self.handle_vm_interfaces(name)
            elif action == "console":
                self.handle_vm_console(name)
            else:
                self.handle_vm_info(name)
            return True

        return False

    def route_typed_post(self, parsed):
        """Dispatch the typed write endpoints. True when the request was handled."""
        path = parsed.path
        if path == "/api/v1/vm/define":
            self.handle_vm_define()
            return True
        if path == "/api/v1/vm/undefine":
            self.handle_vm_undefine()
            return True
        if path == "/api/v1/storage/drbd/role":
            self.handle_storage_drbd_role()
            return True
        if path == "/api/v1/storage/device/prepare":
            self.handle_storage_device_prepare()
            return True
        if path == "/api/v1/storage/device/write":
            self.handle_storage_device_write(parsed)
            return True
        if path == "/api/v1/storage/device/flush":
            self.handle_storage_device_flush()
            return True
        if path == "/api/v1/storage/container/ensure":
            self.handle_storage_container_ensure()
            return True
        if path == "/api/v1/storage/linstor/resource":
            self.handle_storage_linstor_resource()
            return True
        if path == "/api/v1/storage/linstor/resource/delete":
            self.handle_storage_linstor_resource_delete()
            return True
        if path == "/api/v1/host/reboot":
            self.handle_host_reboot()
            return True
        if path == "/api/v1/db/repair":
            self.handle_db_repair()
            return True

        segments = [segment for segment in path.split("/") if segment]
        if len(segments) == 5 and segments[0:3] == ["api", "v1", "vm"] and segments[4] == "power":
            name = urllib.parse.unquote(segments[3])
            if not valid_name(name):
                self.reject("Invalid VM name")
                return True
            self.handle_vm_power(name)
            return True

        return False

    # -- VM ------------------------------------------------------------

    def handle_vm_interfaces(self, name):
        rc, stdout, stderr = run_argv(VIRSH + ["domiflist", name], timeout=30)
        if rc != 0:
            self.reject((stderr or stdout).strip() or "virsh domiflist failed",
                        virsh_status_for(stderr))
            return
        self.send_json_response(200, {"interfaces": parse_virsh_domiflist(stdout)})

    def handle_vm_console(self, name):
        rc, stdout, stderr = run_argv(VIRSH + ["dumpxml", name], timeout=30)
        if rc != 0:
            self.reject((stderr or stdout).strip() or "virsh dumpxml failed",
                        virsh_status_for(stderr))
            return
        try:
            graphics = parse_domain_graphics(stdout)
        except ET.ParseError as exc:
            self.reject("Could not parse domain XML: %s" % exc, 500)
            return
        if graphics is None:
            self.reject("Domain %s has no vnc or spice console" % name, 404)
            return
        self.send_json_response(200, graphics)

    def handle_vm_info(self, name):
        rc, stdout, stderr = run_argv(VIRSH + ["dominfo", name], timeout=30)
        if rc != 0:
            self.reject((stderr or stdout).strip() or "virsh dominfo failed",
                        virsh_status_for(stderr))
            return
        self.send_json_response(200, parse_virsh_dominfo(stdout))

    def handle_vm_define(self):
        payload, error = self.read_json_payload()
        if error:
            self.reject(error)
            return

        name = payload.get("name")
        if not valid_name(name):
            self.reject("Invalid VM name")
            return

        xml_b64 = payload.get("xml_b64")
        if not isinstance(xml_b64, str) or not xml_b64.strip():
            self.reject("Missing xml_b64")
            return
        try:
            xml_bytes = base64.b64decode("".join(xml_b64.split()), validate=True)
        except Exception:
            self.reject("xml_b64 is not valid base64")
            return
        try:
            xml_text = xml_bytes.decode("utf-8")
        except UnicodeDecodeError:
            self.reject("xml_b64 must decode to UTF-8 domain XML")
            return
        try:
            xml_name = parse_domain_name(xml_text)
        except ET.ParseError as exc:
            self.reject("Domain XML is not well formed: %s" % exc)
            return
        if xml_name != name:
            self.reject("Domain XML declares name '%s', which does not match '%s'"
                        % (xml_name, name))
            return

        # The XML reaches virsh as a file path, never as a shell argument, so no
        # part of the document can be read as command syntax.
        fd, temp_path = tempfile.mkstemp(prefix="spark-domain-", suffix=".xml")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(xml_bytes)
            rc, stdout, stderr = run_argv(VIRSH + ["define", temp_path], timeout=60)
        except OSError as exc:
            self.reject("Could not stage domain XML: %s" % exc, 500)
            return
        finally:
            try:
                os.remove(temp_path)
            except OSError:
                pass

        if rc != 0:
            self.reject((stderr or stdout).strip() or "virsh define failed", 500)
            return
        self.send_json_response(200, {"defined": True})

    def handle_vm_undefine(self):
        payload, error = self.read_json_payload()
        if error:
            self.reject(error)
            return

        name = payload.get("name")
        if not valid_name(name):
            self.reject("Invalid VM name")
            return
        keep_nvram = payload.get("keep_nvram", False)
        if not isinstance(keep_nvram, bool):
            self.reject("keep_nvram must be a boolean")
            return

        argv = VIRSH + ["undefine", name, "--keep-nvram" if keep_nvram else "--nvram"]
        rc, stdout, stderr = run_argv(argv, timeout=60)
        if rc != 0:
            self.reject((stderr or stdout).strip() or "virsh undefine failed",
                        virsh_status_for(stderr))
            return
        self.send_json_response(200, {"undefined": True})

    def handle_vm_power(self, name):
        payload, error = self.read_json_payload()
        if error:
            self.reject(error)
            return

        action = payload.get("action")
        if action not in VM_POWER_ACTIONS:
            self.reject("action must be one of " + ", ".join(VM_POWER_ACTIONS))
            return

        rc, stdout, stderr = run_argv(VIRSH + [action, name], timeout=120)
        state = virsh_domain_state(name)
        if rc != 0:
            message = (stderr or stdout).strip() or ("virsh %s failed" % action)
            if virsh_status_for(stderr) == 404:
                self.reject(message, 404)
                return
            # The domain exists but did not take the transition. Report the state
            # it is actually in alongside the failure.
            self.send_json_response(409, {"state": state or "", "error": message})
            return
        self.send_json_response(200, {"state": state or ""})

    # -- Storage -------------------------------------------------------

    def handle_storage_drbd_status(self, parsed):
        resource = self.query_param(parsed, "resource")
        argv = ["drbdsetup", "status", "--json"]
        if resource is not None:
            if not valid_name(resource):
                self.reject("Invalid resource name")
                return
            argv.append(resource)

        rc, stdout, stderr = run_argv(argv, timeout=30)
        if rc != 0:
            # A named resource that drbdsetup does not know is a 404, not a
            # server fault: DRBD resources exist only on the nodes that back them.
            self.reject((stderr or stdout).strip() or "drbdsetup status failed",
                        404 if resource is not None else 500)
            return
        try:
            status = json.loads(stdout.strip() or "[]")
        except Exception:
            self.reject("Could not parse drbdsetup status output", 500)
            return
        self.send_json_response(200, status)

    def handle_storage_drbd_role(self):
        payload, error = self.read_json_payload()
        if error:
            self.reject(error)
            return

        resource = payload.get("resource")
        if not valid_name(resource):
            self.reject("Invalid resource name")
            return
        role = payload.get("role")
        if not isinstance(role, str) or role.lower() not in DRBD_ROLES:
            self.reject("role must be one of " + ", ".join(DRBD_ROLES))
            return
        role = role.lower()
        force = payload.get("force", False)
        if not isinstance(force, bool):
            self.reject("force must be a boolean")
            return

        argv = ["drbdadm", role]
        if force and role == "primary":
            argv.append("--force")
        argv.append(resource)

        rc, stdout, stderr = run_argv(argv, timeout=60)
        resulting = drbd_local_role(resource)
        if resulting is None and rc == 0:
            # drbdadm confirmed the transition; only the read-back was unavailable.
            resulting = role.capitalize()

        if resulting is not None and resulting.lower() == role:
            self.send_json_response(200, {"role": resulting})
            return

        message = (stderr or stdout).strip() or ("Could not read the role of " + resource)
        if any(peer.lower() == "primary" for peer in drbd_peer_roles(resource)):
            message = "Peer already holds Primary for %s. %s" % (resource, message)
        self.send_json_response(409, {"role": resulting or "Unknown", "error": message})

    def handle_storage_device(self, parsed):
        raw_path = self.query_param(parsed, "path")
        if raw_path is None:
            self.reject("Missing path parameter")
            return
        real, error = validate_path(raw_path)
        if error:
            self.reject(error)
            return

        try:
            st_result = os.stat(real)
        except FileNotFoundError:
            self.send_json_response(200, {"exists": False, "is_block": False, "size_bytes": 0})
            return
        except OSError as exc:
            self.reject(str(exc), 500)
            return

        self.send_json_response(200, {
            "exists": True,
            "is_block": stat.S_ISBLK(st_result.st_mode),
            "size_bytes": device_size_bytes(real, st_result),
        })

    def handle_storage_device_prepare(self):
        import pwd
        import grp

        payload, error = self.read_json_payload()
        if error:
            self.reject(error)
            return

        real, error = validate_path(payload.get("path"))
        if error:
            self.reject(error)
            return
        owner, error = validate_owner(payload.get("owner"))
        if error:
            self.reject(error)
            return
        mode, error = validate_mode(payload.get("mode"))
        if error:
            self.reject(error)
            return

        user_name, _, group_name = owner.partition(":")
        try:
            uid = pwd.getpwnam(user_name).pw_uid
            gid = grp.getgrnam(group_name).gr_gid
        except KeyError:
            self.reject("Owner '%s' does not exist on this host" % owner, 500)
            return

        try:
            os.chown(real, uid, gid)
            os.chmod(real, int(mode, 8))
        except FileNotFoundError:
            self.reject("No such path: " + real, 404)
            return
        except OSError as exc:
            self.reject(str(exc), 500)
            return
        self.send_json_response(200, {"prepared": True})

    def handle_storage_device_write(self, parsed):
        """Stream the request body directly onto a block device.

        The web tier must not touch storage at all -- not the device, and not a staging
        file on a mounted volume. It receives the upload and proxies the bytes here; this
        daemon owns the data path, the same way Stargate does on Nutanix rather than
        Prism. Spectrum's container consequently needs no /dev and no storage mount.

        The device is taken from the query string and validated against the allowlist; the
        payload is the raw body, so nothing the caller sends can influence a command.
        """
        params = urllib.parse.parse_qs(parsed.query or "")
        device = (params.get("device") or [None])[0]

        ok, err = validate_path(device)
        if not ok:
            self.send_json_response(400, {"error": err})
            return
        if not str(device).startswith("/dev/drbd/"):
            self.send_json_response(400, {"error": "device must be under /dev/drbd/"})
            return
        if not os.path.exists(device):
            self.send_json_response(404, {"error": "device does not exist: " + str(device)})
            return

        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            length = 0
        if length <= 0:
            self.send_json_response(400, {"error": "Content-Length required and must be > 0"})
            return

        written = 0
        try:
            with open(device, "r+b") as dst:
                remaining = length
                while remaining > 0:
                    chunk = self.rfile.read(min(4 * 1024 * 1024, remaining))
                    if not chunk:
                        break
                    dst.write(chunk)
                    written += len(chunk)
                    remaining -= len(chunk)
                dst.flush()
                os.fsync(dst.fileno())
        except Exception as exc:
            self.send_json_response(500, {"error": "write failed: " + str(exc)})
            return

        if written != length:
            # A short write means the client disconnected mid-upload. Say so rather than
            # reporting success, which previously let a truncated image be registered.
            self.send_json_response(400, {
                "error": "short write: %d of %d bytes" % (written, length),
                "written": written})
            return

        self.send_json_response(200, {"written": written})

    def handle_storage_device_flush(self):
        payload, error = self.read_json_payload()
        if error:
            self.reject(error)
            return
        real, error = validate_path(payload.get("path"))
        if error:
            self.reject(error)
            return
        if not os.path.exists(real):
            self.reject("No such path: " + real, 404)
            return

        rc, stdout, stderr = run_argv(["blockdev", "--flushbufs", real], timeout=60)
        if rc != 0:
            self.reject((stderr or stdout).strip() or "blockdev --flushbufs failed", 500)
            return
        self.send_json_response(200, {"flushed": True})

    def handle_storage_container_mounted(self, parsed):
        raw_path = self.query_param(parsed, "path")
        if raw_path is None:
            self.reject("Missing path parameter")
            return
        real, error = validate_path(raw_path)
        if error:
            self.reject(error)
            return
        self.send_json_response(200, {"mounted": path_is_mounted(real)})

    def handle_storage_container_ensure(self):
        payload, error = self.read_json_payload()
        if error:
            self.reject(error)
            return
        name = payload.get("name")
        if not valid_name(name):
            self.reject("Invalid container name")
            return

        path = os.path.join(AETHER_VOLUMES_ROOT, name)
        existed = os.path.isdir(path)
        try:
            os.makedirs(path, mode=0o755, exist_ok=True)
        except OSError as exc:
            self.reject(str(exc), 500)
            return
        self.send_json_response(200, {"path": path, "created": not existed})

    # -- Storage: Linstor ----------------------------------------------

    def handle_storage_linstor_resources(self, parsed):
        """Everything Linstor holds, or one resource when `?resource=` is given."""
        resource = self.query_param(parsed, "resource")
        if resource is not None and not valid_name(resource):
            self.reject("Invalid resource name")
            return

        inventory, error = linstor_inventory()
        if inventory is None:
            self.reject(error, 500)
            return

        if resource is None:
            self.send_json_response(200, {"resources": inventory})
            return

        for entry in inventory:
            if entry["name"] == resource:
                self.send_json_response(200, {"resources": [entry]})
                return
        self.reject("No such Linstor resource: " + resource, 404)

    def handle_storage_linstor_resource(self):
        """Create a VM disk: resource definition, volume definition, placement, options.

        One idempotent operation rather than four endpoints. The four commands are
        meaningless apart -- a resource definition with no volume definition backs
        nothing, and a volume definition with no resources exists on no node -- so
        exposing them separately would just move the sequencing bug into every caller.

        Idempotent in the sense that matters for a retry: each step tolerates the
        object already being there, and the response says whether this call was the one
        that created it. A resource that already exists at a *different* size is a 409
        rather than a silent reuse, because that is how a VM ends up attached to a
        disk left behind by an earlier VM of the same name.

        Partial work is cleaned up here, not left for the caller: if placement or the
        DRBD options fail after this call created the resource definition, the
        definition is deleted again. A definition that already existed is never
        deleted -- it may be backing a live VM.
        """
        payload, error = self.read_json_payload()
        if error:
            self.reject(error)
            return

        resource = payload.get("resource")
        if not valid_name(resource):
            self.reject("Invalid resource name")
            return
        size_gib, error = validate_volume_gib(payload.get("size_gib"))
        if error:
            self.reject(error)
            return
        storage_pool, error = validate_storage_pool(payload.get("storage_pool"))
        if error:
            self.reject(error)
            return
        nodes, error = validate_node_names(payload.get("nodes"))
        if error:
            self.reject(error)
            return

        # Read before write: the controller has to be reachable for this to work at
        # all, and knowing whether the resource is already there is what makes the
        # difference between a safe retry and adopting someone else's disk.
        inventory, error = linstor_inventory()
        if inventory is None:
            self.reject(error, 500)
            return

        existing = None
        for entry in inventory:
            if entry["name"] == resource:
                existing = entry
                break

        requested_kib = size_gib * KIB_PER_GIB
        if existing is not None and existing["size_kib"] not in (None, requested_kib):
            self.send_json_response(409, {
                "resource": resource,
                "size_kib": existing["size_kib"],
                "size_gib": existing["size_gib"],
                "error": ("Linstor resource %s already exists at %s KiB, not the %d KiB "
                          "requested" % (resource, existing["size_kib"], requested_kib)),
            })
            return

        created = False
        if existing is None:
            ok, _stdout, detail = linstor_call(["resource-definition", "create", resource])
            if not ok and not linstor_says(detail, LINSTOR_EXISTS_MARKERS):
                self.reject("Could not create resource definition %s: %s" % (resource, detail),
                            500)
                return
            created = ok

        def undo(reason):
            """Delete only what this call created, then report the original failure."""
            if created:
                undone, _out, undo_detail = linstor_call(
                    ["resource-definition", "delete", resource], timeout=180)
                if not undone:
                    print("[LINSTOR] Rollback of %s failed: %s" % (resource, undo_detail))
                    reason += (" (rollback of %s also failed: %s)" % (resource, undo_detail))
            self.reject(reason, 500)

        # --vlmnr 0 rather than letting the client pick the next free number. Without it a
        # retry against a resource that already has volume 0 does not fail as "already
        # exists" -- it quietly adds a *second* volume, and the VM ends up with a disk it
        # never asked for. The size check above is what decides whether volume 0 is
        # already the one that was asked for.
        if existing is None or existing["size_kib"] != requested_kib:
            ok, _stdout, detail = linstor_call(
                ["volume-definition", "create", "--vlmnr", "0", resource, "%dGiB" % size_gib])
            if not ok and not linstor_says(detail, LINSTOR_EXISTS_MARKERS):
                undo("Could not create volume definition %s: %s" % (resource, detail))
                return

        for node in nodes:
            ok, _stdout, detail = linstor_call(
                ["resource", "create", node, resource, "--storage-pool", storage_pool],
                timeout=180)
            if not ok and not linstor_says(detail, LINSTOR_EXISTS_MARKERS):
                undo("Could not place %s on %s: %s" % (resource, node, detail))
                return

        # Automatic split-brain resolution. Not dual-primary: see DRBD_SPLIT_BRAIN_OPTIONS.
        ok, _stdout, detail = linstor_call(
            ["resource-definition", "drbd-options"] + DRBD_SPLIT_BRAIN_OPTIONS + [resource])
        if not ok:
            undo("Could not set DRBD options on %s: %s" % (resource, detail))
            return

        self.send_json_response(200, {
            "resource": resource,
            "created": created,
            "size_gib": size_gib,
            "size_kib": requested_kib,
            "storage_pool": storage_pool,
            "nodes": nodes,
            "device_path": linstor_resource_path(resource),
        })

    def handle_storage_linstor_resource_delete(self):
        """Remove a resource definition, and with it its volumes on every node.

        `deleted` is false when there was nothing to delete. That is a success, not a
        404: the caller of a rollback wants the resource gone, and a delete that races
        another delete must not turn a completed rollback into an error.
        """
        payload, error = self.read_json_payload()
        if error:
            self.reject(error)
            return

        resource = payload.get("resource")
        if not valid_name(resource):
            self.reject("Invalid resource name")
            return

        ok, _stdout, detail = linstor_call(
            ["resource-definition", "delete", resource], timeout=180)
        if ok:
            self.send_json_response(200, {"resource": resource, "deleted": True})
            return
        if linstor_says(detail, LINSTOR_ABSENT_MARKERS):
            self.send_json_response(200, {"resource": resource, "deleted": False})
            return

        # The resource is there and did not go away -- in use by a running VM, or a
        # node holding it is unreachable. 409 with the state key, as elsewhere in this
        # API, so the caller learns what actually happened rather than "500".
        self.send_json_response(409, {
            "resource": resource,
            "deleted": False,
            "error": detail or ("Could not delete resource definition " + resource),
        })

    # -- Host ----------------------------------------------------------

    def handle_host_network(self):
        interface = None
        gateway = None
        rc, stdout, _ = run_argv(["ip", "-j", "route"], timeout=20)
        if rc == 0:
            interface, gateway = parse_ip_route_json(stdout)
        if interface is None:
            try:
                with open("/proc/net/route", "r") as handle:
                    interface, gateway = parse_proc_net_route(handle.read())
            except OSError:
                pass

        addresses = []
        rc_addr, stdout_addr, _ = run_argv(["ip", "-j", "addr"], timeout=20)
        if rc_addr == 0:
            addresses = parse_ip_addr_json(stdout_addr)

        self.send_json_response(200, {
            "default_interface": interface,
            "default_gateway": gateway,
            "addresses": addresses,
        })

    def handle_host_memory(self):
        try:
            with open("/proc/meminfo", "r") as handle:
                content = handle.read()
        except OSError as exc:
            self.reject("Could not read /proc/meminfo: %s" % exc, 500)
            return
        self.send_json_response(200, parse_meminfo(content))

    def handle_host_disks(self):
        columns = "NAME,PATH,SIZE,TYPE,MOUNTPOINT,FSTYPE,ROTA,MODEL,SERIAL"
        rc, stdout, stderr = run_argv(["lsblk", "-J", "-b", "-o", columns], timeout=30)
        if rc != 0:
            # An older lsblk without one of those columns still answers the plain form.
            rc, stdout, stderr = run_argv(["lsblk", "-J", "-b"], timeout=30)
        if rc != 0:
            self.reject((stderr or stdout).strip() or "lsblk failed", 500)
            return
        try:
            disks = json.loads(stdout.strip() or "{}")
        except Exception:
            self.reject("Could not parse lsblk output", 500)
            return
        self.send_json_response(200, disks)

    def handle_host_capabilities(self):
        self.send_json_response(200, read_host_capabilities())

    def handle_host_dhcp_leases(self):
        self.send_json_response(200, {"leases": read_dhcp_leases()})

    def handle_host_reboot(self):
        payload, error = self.read_json_payload()
        if error:
            self.reject(error)
            return
        if payload.get("confirm") is not True:
            self.reject('Reboot requires {"confirm": true}')
            return
        schedule_host_reboot()
        self.send_json_response(200, {"rebooting": True})

    # -- Database ------------------------------------------------------

    def handle_db_ring(self):
        rc, stdout, stderr = run_argv(
            ["podman", "exec", HYDRA_DB_CONTAINER, "nodetool", "status"], timeout=60)
        if rc != 0:
            self.reject((stderr or stdout).strip() or "nodetool status failed", 500)
            return
        self.send_json_response(200, {"nodes": parse_nodetool_status(stdout)})

    def handle_db_repair(self):
        payload, error = self.read_json_payload()
        if error:
            self.reject(error)
            return

        keyspace = payload.get("keyspace", "hydra")
        if not valid_name(keyspace):
            self.reject("Invalid keyspace name")
            return
        primary_range = payload.get("primary_range", True)
        if not isinstance(primary_range, bool):
            self.reject("primary_range must be a boolean")
            return

        if not start_db_repair(keyspace, primary_range):
            self.reject("A repair is already running on this node", 409)
            return
        self.send_json_response(200, {"started": True})

class SecureHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass, ssl_context):
        super().__init__(server_address, RequestHandlerClass)
        self.ssl_context = ssl_context

def check_cluster_and_autostart():
    # Wait a few seconds to let systemd-spark finish starting up
    time.sleep(3)
    
    # Unconditionally stop and undefine all local virtual machines on startup.
    # Because hypervisors are stateless executors, Vali will dynamically define
    # and start workloads when they are scheduled to run on this node.
    print("[AUTOSTART] Cleaning up all local libvirt virtual machines to ensure clean compute startup...")
    subprocess.run("for vm in $(virsh list --all --name); do virsh destroy $vm || true; virsh undefine $vm --nvram || true; done", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # ZooKeeper is infrastructure, not a workload: it holds the desired cluster state,
    # so it must be running before that state can be read. Start it unconditionally and
    # never stop it as part of "the cluster is stopped".
    subprocess.run("systemctl start zookeeper", shell=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if os.path.exists("/run/hci/cluster_operation.lock"):
        print("[AUTOSTART] Cluster operation is in progress. Bypassing autostart checks.")
        return

    
    # Check if cluster configuration exists
    if not os.path.exists("/etc/hci/cluster.json"):
        print("[AUTOSTART] No cluster configuration found (/etc/hci/cluster.json). Ensuring workloads are stopped.")
        services_to_stop = ["logos", "mipha", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "linstor-controller", "aether", "daruk", "hydra-db", "agahnim", "slate"]
        for svc in services_to_stop:
            subprocess.run(f"systemctl stop {svc}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return
        
    if os.path.exists("/etc/hci/maintenance.state"):
        print("[AUTOSTART] Host is in maintenance mode. Ensuring compute workloads are stopped while consensus/DB workloads start...")
        services_to_stop = ["logos", "mipha", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "linstor-controller", "agahnim", "slate"]
        for svc in services_to_stop:
            subprocess.run(f"systemctl stop {svc}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run("systemctl start zookeeper", shell=True)
        subprocess.run("systemctl start hydra-db", shell=True)
        
        # Start periodic watchdog loop directly to keep database/storage running during maintenance
        print("[WATCHDOG] Starting service health watchdog in maintenance mode...")
        while True:
            try:
                time.sleep(30)
                if os.path.exists("/run/hci/cluster_operation.lock"):
                    continue
                if not os.path.exists("/etc/hci/maintenance.state"):
                    print("[WATCHDOG] Host left maintenance mode. Exiting maintenance watchdog loop to resume normal checks.")
                    break
                for svc in ["zookeeper", "hydra-db", "aether"]:
                    res = subprocess.run(f"systemctl is-active {svc}", shell=True, stdout=subprocess.PIPE)
                    status_str = res.stdout.decode().strip()
                    if status_str not in ["active", "activating"]:
                        print(f"[WATCHDOG] Maintenance Node: Restarting critical service {svc} (current status: {status_str})...")
                        subprocess.run(f"systemctl start {svc}", shell=True)
            except Exception as wex:
                print(f"[WATCHDOG] Error in maintenance service watchdog: {wex}")

    # 1. Start ZooKeeper unconditionally if it is not active
    print("[AUTOSTART] Ensuring local ZooKeeper is started...")
    res = subprocess.run("systemctl is-active zookeeper", shell=True, stdout=subprocess.PIPE)
    if res.stdout.decode().strip() != "active":
        print("[AUTOSTART] Starting zookeeper service...")
        subprocess.run("systemctl start zookeeper", shell=True)
        
    # 2. Poll local ZooKeeper on port 2181 for quorum consensus (with a 10-second timeout)
    print("[AUTOSTART] Waiting for local ZooKeeper to establish quorum consensus...")
    quorum_established = False
    
    for _ in range(5):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect(("127.0.0.1", 2181))
            s.sendall(b"stat")
            resp = s.recv(2048).decode('utf-8', errors='ignore')
            s.close()
            
            # Check for Mode: follower or Mode: leader in response
            for line in resp.splitlines():
                if line.strip().lower().startswith("mode:"):
                    mode = line.split(":", 1)[1].strip().lower()
                    if mode in ["follower", "leader", "standalone"]:
                        print(f"[AUTOSTART] ZooKeeper quorum established (Mode: {mode}).")
                        quorum_established = True
                        break
        except Exception:
            pass
            
        if quorum_established:
            break
        time.sleep(2)
            
    # 3. Quorum established! Now query ZooKeeper for cluster state
    cluster_state = "stopped"
    if quorum_established:
        try:
            res_state = subprocess.run("podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 get /cluster_state", 
                                       shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out_state = res_state.stdout.decode("utf-8", errors="ignore")
            if "started" in out_state:
                cluster_state = "started"
        except Exception as e:
            print(f"[AUTOSTART] Error querying cluster state from ZooKeeper: {e}")

    if cluster_state == "stopped":
        print("[AUTOSTART] Cluster state is 'stopped' or uninitialized. Ensuring database, storage, and UI workloads are stopped...")
        services_to_stop = ["logos", "mipha", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "linstor-controller", "aether", "daruk", "hydra-db", "agahnim", "slate"]
        for svc in services_to_stop:
            subprocess.run(f"systemctl stop {svc}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        # Autostarting local database, storage, and UI workloads...
        services = ["hydra-db", "daruk", "aether", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha"]
        for svc in services:
            res = subprocess.run(f"systemctl is-active {svc}", shell=True, stdout=subprocess.PIPE)
            if res.stdout.decode().strip() != "active":
                print(f"[AUTOSTART] Starting local service {svc}...")
                subprocess.run(f"systemctl start {svc}", shell=True)
                if svc == "hydra-db":
                    # Give it a second to initialize connections
                    time.sleep(2)
        if check_urbosa_enabled():
            res = subprocess.run("systemctl is-active urbosa", shell=True, stdout=subprocess.PIPE)
            if res.stdout.decode().strip() != "active":
                print("[AUTOSTART] Starting local service urbosa...")
                subprocess.run("systemctl start urbosa", shell=True)
                
        # Wait for Daruk query proxy to accept queries and run settings sync
        print("[AUTOSTART] Attempting local settings sync...")
        for _ in range(30):
            success, err = sync_cluster_settings_local()
            if success:
                break
            time.sleep(1)
            
    print("[AUTOSTART] Autostart completed successfully.")
    
    # Start periodic watchdog loop
    print("[WATCHDOG] Starting service health watchdog...")
    while True:
        try:
            time.sleep(30)
            if os.path.exists("/run/hci/cluster_operation.lock"):
                continue
            if not os.path.exists("/etc/hci/cluster.json"):
                continue
            if os.path.exists("/etc/hci/maintenance.state"):
                for svc in ["zookeeper", "hydra-db", "aether"]:
                    res = subprocess.run(f"systemctl is-active {svc}", shell=True, stdout=subprocess.PIPE)
                    status_str = res.stdout.decode().strip()
                    if status_str not in ["active", "activating"]:
                        print(f"[WATCHDOG] Maintenance Node: Restarting critical service {svc} (current status: {status_str})...")
                        subprocess.run(f"systemctl start {svc}", shell=True)
                continue
            
            # Check Zookeeper quorum
            quorum_established = False
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect(("127.0.0.1", 2181))
                s.sendall(b"stat")
                resp = s.recv(2048).decode('utf-8', errors='ignore')
                s.close()
                for line in resp.splitlines():
                    if line.strip().lower().startswith("mode:"):
                        mode = line.split(":", 1)[1].strip().lower()
                        if mode in ["follower", "leader", "standalone"]:
                            quorum_established = True
                            break
            except Exception:
                pass
                
            if not quorum_established:
                continue
                
            # Query ZooKeeper for cluster state
            cluster_state = "stopped"
            try:
                res_state = subprocess.run("podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 get /cluster_state", 
                                           shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out_state = res_state.stdout.decode("utf-8", errors="ignore")
                if "started" in out_state:
                    cluster_state = "started"
            except Exception:
                pass
                
            if cluster_state == "started":
                services = ["hydra-db", "daruk", "aether", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha"]
                for svc in services:
                    res = subprocess.run(f"systemctl is-active {svc}", shell=True, stdout=subprocess.PIPE)
                    status_str = res.stdout.decode().strip()
                    if status_str not in ["active", "activating"]:
                        print(f"[WATCHDOG] Restarting failed/stopped service {svc} (current status: {status_str})...")
                        subprocess.run(f"systemctl start {svc}", shell=True)
                if check_urbosa_enabled():
                    res = subprocess.run("systemctl is-active urbosa", shell=True, stdout=subprocess.PIPE)
                    status_str = res.stdout.decode().strip()
                    if status_str not in ["active", "activating"]:
                        print(f"[WATCHDOG] Restarting failed/stopped service urbosa (current status: {status_str})...")
                        subprocess.run("systemctl start urbosa", shell=True)
        except Exception as wex:
            print(f"[WATCHDOG] Error in service watchdog: {wex}")

def main():
    ca_cert = "/etc/hci/spark/certs/ca.crt"
    node_cert = "/etc/hci/spark/certs/node.crt"
    node_key = "/etc/hci/spark/certs/node.key"

    if not (os.path.exists(ca_cert) and os.path.exists(node_cert) and os.path.exists(node_key)):
        print("[ERROR] Certificates or keys not found in /etc/hci/spark/certs/.")
        sys.exit(1)

    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=node_cert, keyfile=node_key)
    context.load_verify_locations(cafile=ca_cert)
    context.verify_mode = ssl.CERT_REQUIRED
    
    # Start the autostart checks in a background thread
    t = threading.Thread(target=check_cluster_and_autostart, daemon=True)
    t.start()
    
    # Start the periodic settings sync loop in a background thread
    t_sync = threading.Thread(target=settings_sync_loop, daemon=True)
    t_sync.start()

    # Start the NVRAM directory watcher in a background thread
    def nvram_watcher_loop():
        import os, time, base64
        nvram_dir = "/var/lib/hci/aether/nvram"
        last_mtimes = {}
        while True:
            try:
                if os.path.exists(nvram_dir):
                    for filename in os.listdir(nvram_dir):
                        if filename.endswith("_vars.fd"):
                            path = os.path.join(nvram_dir, filename)
                            try:
                                mtime = os.path.getmtime(path)
                            except Exception:
                                continue
                            if path not in last_mtimes or last_mtimes[path] < mtime:
                                last_mtimes[path] = mtime
                                vm_name = filename[:-8]
                                try:
                                    with open(path, "rb") as f:
                                        content = f.read()
                                    b64_data = base64.b64encode(content).decode('utf-8')
                                    cql = f"INSERT INTO hydra.vm_nvram (vm_name, nvram_data) VALUES ('{vm_name}', '{b64_data}');"
                                    run_cql_query(cql)
                                except Exception as fe:
                                    sys.stderr.write(f"[NVRAM Watcher] Error reading/saving {filename}: {fe}\\n")
            except Exception as e:
                sys.stderr.write(f"[NVRAM Watcher] Error: {e}\\n")
            time.sleep(5)

    t_nvram = threading.Thread(target=nvram_watcher_loop, daemon=True)
    t_nvram.start()

    # Publish this node's state into ZooKeeper as an ephemeral znode, and converge local
    # services toward the desired cluster state recorded there. Both loops tolerate
    # ZooKeeper being absent (they retry), so the daemon still serves its mTLS API on a
    # host where ZooKeeper has not started yet -- which is what the direct-probe fallback
    # in `cluster status` relies on.
    t_zk_pub = threading.Thread(target=zk_publisher_loop, daemon=True)
    t_zk_pub.start()

    t_zk_rec = threading.Thread(target=zk_reconcile_loop, daemon=True)
    t_zk_rec.start()

    server_address = ('', PORT)
    httpd = SecureHTTPServer(server_address, SparkDaemonHandler, context)
    print(f"Spark Daemon listening on port {PORT} with mTLS...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
