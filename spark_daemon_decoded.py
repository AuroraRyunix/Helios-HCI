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
            native_svcs = ["spark-daemon", "bifrost", "dagur", "mimir", "vali", "catalyst", "hylia", "gatoway", "logos", "mipha", "daruk", "agahnim"]
            if "urbosa" in services:
                native_svcs.append("urbosa")
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
            container_svcs = ["zookeeper", "hydra-db", "aether", "spectrum", "slate"]
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
        
        for svc in services:
            if services_active[svc]:
                result["services"][svc_map[svc]] = {
                    "status": "UP",
                    "pids": pids_cache.get(svc, [])
                }
            else:
                result["services"][svc_map[svc]] = {
                    "status": "DOWN",
                    "pids": []
                }
                
        self.send_json_response(200, result)

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
        
        cql_queries = []
        for node in nodes:
            node_ip = node.get("ip")
            for seg in segments:
                vni = seg.get("vni")
                if not vni:
                    continue
                ifaces = [f"vxlan-{vni}", f"br-ov-{vni}"]
                for iface in ifaces:
                    cql_queries.append(f"SELECT JSON node_ip, interface_name, rx_kbps, tx_kbps, rx_packets, tx_packets, timestamp FROM hydra.urbosa_tunnel_metrics WHERE node_ip = '{node_ip}' AND interface_name = '{iface}' LIMIT 1;")
        
        metrics_map = {}
        if cql_queries:
            combined_cql = "\n".join(cql_queries)
            rc_m, stdout_m, _ = run_cql_query(combined_cql)
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
            run_checked_cmd(hosts[0], "systemctl start linstor-controller")
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
            run_checked_cmd(servers[0], "systemctl start linstor-controller")
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
        res_m = subprocess.run("lsblk -n -o MOUNTPOINT " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        is_sys = False
        for m in res_m.stdout.decode().splitlines():
            m = m.strip()
            if m in ["/", "/boot", "/boot/efi", "/var", "/usr", "/home"] or "swap" in m.lower():
            is_sys = True
            break
        if is_sys: continue
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
                    zoo_servers_parts.append(f"server.{i}={ip}:2888:3888;2181")
                zoo_servers_str = " ".join(zoo_servers_parts)
                zoo_servers_env = f' ZOO_SERVERS="{zoo_servers_str}"'

            for idx, ip in enumerate(servers):
                node_id = idx + 1
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
                    f"Environment=ZOO_MY_ID={node_id}{zoo_servers_env} ZOO_4LW_COMMANDS_WHITELIST=*\n\n"
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

        # 0.5. Dynamically read configured storage disks
        disk_devices = ["/dev/sdb"]
        try:
            with open("/etc/hci/aether/storage-pools.json", "r") as f:
                spdata = json.load(f)
                for disk in spdata.get("local_disks", []):
                    dev = disk.get("device")
                    if dev and dev not in disk_devices:
                        disk_devices.append(dev)
        except Exception:
            pass

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
        # Wipe LVM thin pool and disk signatures dynamically on all configured disk devices
        # Ensure LVM wiping is performed on the remote hosts
        for dev in disk_devices:
            lvm_wipe_cmd = f"lvchange -an -f /dev/vg_aether/* || true; lvremove -y -f vg_aether || true; vgremove -y -f vg_aether || true; rm -rf /dev/vg_aether || true; dmsetup ls | grep vg_aether | awk '{{print $1}}' | while read -r dm; do dmsetup remove -f \"$dm\" || true; done; pvremove -y -f {dev} || true; wipefs -a -f {dev} || true"
            try:
                run_parallel(hosts, lvm_wipe_cmd)
            except Exception:
                pass
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
    
    if os.path.exists("/run/hci/cluster_operation.lock"):
        print("[AUTOSTART] Cluster operation is in progress. Bypassing autostart checks.")
        return

    
    # Check if cluster configuration exists
    if not os.path.exists("/etc/hci/cluster.json"):
        print("[AUTOSTART] No cluster configuration found (/etc/hci/cluster.json). Ensuring workloads are stopped.")
        services_to_stop = ["logos", "mipha", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "linstor-controller", "aether", "daruk", "hydra-db", "zookeeper", "agahnim", "slate"]
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
        services_to_stop = ["logos", "mipha", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "linstor-controller", "aether", "daruk", "hydra-db", "zookeeper", "agahnim", "slate"]
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
    
    server_address = ('', PORT)
    httpd = SecureHTTPServer(server_address, SparkDaemonHandler, context)
    print(f"Spark Daemon listening on port {PORT} with mTLS...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
