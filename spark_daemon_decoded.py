#!/usr/bin/env python3
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
        elif parsed.path == "/api/v1/vm/drs":
            self.forward_to_vali("/api/v1/drs/status", method="GET")
            return
        elif parsed.path == "/api/v1/hosts":
            self.forward_to_vali("/api/v1/hosts", method="GET")
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

        self.send_response(404)
        self.end_headers()

    def handle_execute(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length)
        
        try:
            payload = json.loads(post_data.decode('utf-8'))
            command = payload.get("command", "")
        except Exception as e:
            self.send_json_response(400, {"error": "Invalid JSON or payload"})
            return

        import os
        if os.path.exists("/etc/hci/maintenance.state"):
            blocked_services = ["zookeeper", "hydra-db", "aether", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway"]
            is_start_or_restart = any(x in command for x in ["systemctl start", "systemctl restart", "service start", "service restart"])
            if is_start_or_restart:
                blocked = []
                for svc in blocked_services:
                    if svc in command:
                        blocked.append(svc)
                if blocked:
                    self.send_json_response(200, {
                        "returncode": 0,
                        "stdout": f"Ignored start/restart command for {', '.join(blocked)} because the host is in maintenance mode.",
                        "stderr": ""
                    })
                    return

        try:
            res = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=45)
            response = {
                "returncode": res.returncode,
                "stdout": res.stdout.decode('utf-8', errors='ignore').strip(),
                "stderr": res.stderr.decode('utf-8', errors='ignore').strip()
            }
        except subprocess.TimeoutExpired as te:
            response = {
                "returncode": -1,
                "stdout": te.stdout.decode('utf-8', errors='ignore').strip() if te.stdout else "",
                "stderr": (te.stderr.decode('utf-8', errors='ignore').strip() if te.stderr else "") + "\nCommand timed out after 45 seconds"
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
        
        # 2. Get Gluster status
        peer_status = ""
        volume_info = ""
        if cluster_exists:
            res_peer = subprocess.run("podman exec systemd-aether gluster peer status", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            peer_status = res_peer.stdout.decode('utf-8', errors='ignore').strip()
            
            res_vol = subprocess.run("podman exec systemd-aether gluster volume info", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            volume_info = res_vol.stdout.decode('utf-8', errors='ignore').strip()
        
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
                    for svc_name in ["ZooKeeper", "HydraDB", "Aether", "Spark", "Spectrum", "Bifrost", "Dagur", "Mimir", "Vali", "Catalyst", "Gatoway", "Logos"]:
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
            is_leader = "mode: leader" in resp.lower()
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

        services = ["zookeeper", "hydra-db", "aether", "spark-daemon", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos"]
        svc_map = {
            "zookeeper": "ZooKeeper",
            "hydra-db": "HydraDB",
            "aether": "Aether",
            "spark-daemon": "Spark",
            "spectrum": "Spectrum",
            "bifrost": "Bifrost",
            "dagur": "Dagur",
            "mimir": "Mimir",
            "vali": "Vali",
            "catalyst": "Catalyst",
            "gatoway": "Gatoway",
            "logos": "Logos"
        }
        
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
            native_svcs = ["spark-daemon", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos"]
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
            container_svcs = ["zookeeper", "hydra-db", "aether", "spectrum"]
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

        # Start zookeeper on all nodes
        run_parallel(hosts, "systemctl start zookeeper")
        time.sleep(5)
        
        # Set cluster state to started in ZooKeeper
        subprocess.run("podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 create /cluster_state started || podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 set /cluster_state started", shell=True)
        
        # Start workloads
        services = ["hydra-db", "aether", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos"]
        for svc in services:
            run_parallel(hosts, f"systemctl start {svc}")
            
        # Mount default volumes
        mount_cmd = (
            "podman exec systemd-aether mkdir -p /var/lib/hci/aether/volumes/default-vm-container && "
            "podman exec systemd-aether findmnt /var/lib/hci/aether/volumes/default-vm-container || "
            "podman exec systemd-aether mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-vm-container /var/lib/hci/aether/volumes/default-vm-container"
        )
        run_parallel(hosts, mount_cmd)
        
        mount_img_cmd = (
            "podman exec systemd-aether mkdir -p /var/lib/hci/aether/volumes/default-image-container && "
            "podman exec systemd-aether findmnt /var/lib/hci/aether/volumes/default-image-container || "
            "podman exec systemd-aether mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-image-container /var/lib/hci/aether/volumes/default-image-container"
        )
        run_parallel(hosts, mount_img_cmd)
        
        self.send_json_response(200, {"message": "Cluster start command completed."})

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
        
        # Unmount volumes
        run_parallel(hosts, "podman exec systemd-aether umount -f /var/lib/hci/aether/volumes/default-vm-container || true")
        run_parallel(hosts, "podman exec systemd-aether umount -f /var/lib/hci/aether/volumes/default-image-container || true")
        
        # Stop services
        services = ["logos", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "aether", "hydra-db", "zookeeper"]
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
            vip = payload.get("vip", "")
        except Exception as e:
            self.send_json_response(400, {"error": f"Invalid payload: {str(e)}"})
            return
            
        if not servers:
            self.send_json_response(400, {"error": "No servers specified."})
            return

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
            "dfs_engine": "glusterfs",
            "vip": vip,
            "hosts": hosts_info
        }
        
        json_b64 = base64.b64encode(json.dumps(cluster_json_data, indent=4).encode('utf-8')).decode('utf-8')
        write_config_cmd = f"mkdir -p /etc/hci && echo {json_b64} | base64 -d > /etc/hci/cluster.json"
        run_parallel(servers, write_config_cmd)
        
        # Start storage engine
        run_parallel(servers, "systemctl start aether")
        time.sleep(5)
        
        # Peer nodes using resolved hostnames
        for h in hosts_info:
            subprocess.run(f"podman exec systemd-aether gluster peer probe {h['hostname']}", shell=True)
        time.sleep(2)
        
        # Python script to formatting and claim disks >= 100GB
        disk_claim_script = """
import subprocess
import json
import sys
import os

def run_cmd(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return res.returncode, res.stdout.decode('utf-8', errors='ignore').strip(), res.stderr.decode('utf-8', errors='ignore').strip()

rc, out, err = run_cmd("lsblk -b -d -n -o NAME,SIZE,TYPE,ROTA")
if rc != 0:
    print(json.dumps([]))
    sys.exit(0)

claimed = []
for line in out.splitlines():
    parts = line.split()
    if len(parts) >= 4 and parts[2] == "disk":
        name = parts[0]
        try:
            size_bytes = int(parts[1])
        except ValueError:
            continue
        rota = parts[3]
        dev_path = f"/dev/{name}"
        size_gb = size_bytes / (10**9)
        
        if size_gb < 100.0:
            continue
            
        rc_mounts, mounts_out, _ = run_cmd(f"lsblk -n -o MOUNTPOINT {dev_path}")
        is_sys = False
        for m in mounts_out.splitlines():
            m = m.strip()
            if m in ["/", "/boot", "/boot/efi", "/var", "/usr", "/home"] or "swap" in m.lower():
                is_sys = True
                break
        if is_sys:
            continue
            
        rc_parts, parts_out, _ = run_cmd(f"lsblk -n -o TYPE {dev_path}")
        if "part" in parts_out.splitlines():
            continue
            
        rc_blkid, _, _ = run_cmd(f"blkid {dev_path}")
        if rc_blkid == 0:
            continue
            
        media = "ssd" if rota == "0" else "hdd"
        rc_fmt, _, _ = run_cmd(f"mkfs.xfs -f {dev_path}")
        if rc_fmt != 0:
            continue
            
        brick_path = f"/var/lib/hci/aether/bricks/{name}"
        run_cmd(f"mkdir -p {brick_path}")
        rc_mount, _, _ = run_cmd(f"mount {dev_path} {brick_path}")
        if rc_mount != 0:
            continue
            
        run_cmd(f"mkdir -p {brick_path}/brick")
        run_cmd(f"mkdir -p {brick_path}/brick-images")
        rc_uuid, uuid_out, _ = run_cmd(f"blkid -o value -s UUID {dev_path}")
        uuid_out = uuid_out.strip()
        if rc_uuid == 0 and uuid_out:
            entry = f"UUID={uuid_out} {brick_path} xfs defaults,nofail,x-systemd.device-timeout=5s 0 0"
        else:
            entry = f"{dev_path} {brick_path} xfs defaults,nofail,x-systemd.device-timeout=5s 0 0"
        run_cmd(f"grep -q '{brick_path}' /etc/fstab || echo '{entry}' >> /etc/fstab")
        
        claimed.append({
            "device": dev_path,
            "role": "data",
            "media_type": media,
            "fs_type": "xfs",
            "size_bytes": size_bytes,
            "brick_path": f"{brick_path}/brick"
        })
print(json.dumps(claimed))
"""
        claim_script_b64 = base64.b64encode(disk_claim_script.encode()).decode()
        cmd_claim = f"python3 -c \"import base64; exec(base64.b64decode('{claim_script_b64}').decode())\""
        
        claim_results = run_parallel(servers, cmd_claim)
        
        host_claimed_disks = {}
        ssd_bricks = []
        hdd_bricks = []
        
        for ip, (rc, stdout, stderr) in claim_results.items():
            if rc == 0:
                try:
                    disks = json.loads(stdout.strip())
                    host_claimed_disks[ip] = disks
                    for d in disks:
                        brick_entry = f"{ip}:{d['brick_path']}"
                        if d["media_type"] == "ssd":
                            ssd_bricks.append(brick_entry)
                        else:
                            hdd_bricks.append(brick_entry)
                except Exception:
                    host_claimed_disks[ip] = []
            else:
                host_claimed_disks[ip] = []
                
        if not ssd_bricks and not hdd_bricks:
            self.send_json_response(500, {"error": "No empty disks claimed. Cannot construct storage volume."})
            return
            
        def get_volume_layout(bricks, redundancy_factor):
            N = len(bricks)
            F = redundancy_factor
            gluster_args = ""
            actual_ftt = 0
            if F == 0 or N <= 1:
                gluster_args = ""
                actual_ftt = 0
            elif F == 1:
                if N >= 3:
                    gluster_args = f"disperse {N} redundancy 1"
                    actual_ftt = 1
                else:
                    gluster_args = "replica 2"
                    actual_ftt = 1
            else: # F >= 2
                if N >= 2 * F + 1:
                    gluster_args = f"disperse {N} redundancy {F}"
                    actual_ftt = F
                elif N >= 3:
                    replica_num = min(N, F + 1)
                    gluster_args = f"replica {replica_num}"
                    actual_ftt = replica_num - 1
                else:
                    gluster_args = "replica 2"
                    actual_ftt = 1
            return gluster_args, actual_ftt

        volumes_to_create = []
        if ssd_bricks and hdd_bricks:
            if len(ssd_bricks) >= 2:
                g_args, aftt = get_volume_layout(ssd_bricks, redundancy_factor)
                volumes_to_create.append(("flash-container", ssd_bricks, aftt, g_args))
            else:
                hdd_bricks.extend(ssd_bricks)
            if len(hdd_bricks) >= 2:
                g_args, aftt = get_volume_layout(hdd_bricks, redundancy_factor)
                volumes_to_create.append(("default-vm-container", hdd_bricks, aftt, g_args))
        elif ssd_bricks:
            if len(ssd_bricks) >= 2:
                g_args, aftt = get_volume_layout(ssd_bricks, redundancy_factor)
                volumes_to_create.append(("default-vm-container", ssd_bricks, aftt, g_args))
        else:
            if len(hdd_bricks) >= 2:
                g_args, aftt = get_volume_layout(hdd_bricks, redundancy_factor)
                volumes_to_create.append(("default-vm-container", hdd_bricks, aftt, g_args))
                
        if not volumes_to_create:
            self.send_json_response(500, {"error": "Insufficient bricks claimed to construct a volume."})
            return
            
        # Add default-image-container alongside default-vm-container sharing physical disks
        image_vols = []
        for name, bricks, aftt, g_args in volumes_to_create:
            if name == "default-vm-container":
                image_bricks = [b[:-6] + "/brick-images" if b.endswith("/brick") else b for b in bricks]
                image_vols.append(("default-image-container", image_bricks, aftt, g_args))
        volumes_to_create.extend(image_vols)
            
        for volume_name, bricks_to_use, actual_ftt, gluster_args in volumes_to_create:
            bricks_str = " ".join(bricks_to_use)
            if gluster_args:
                cmd = f"podman exec systemd-aether gluster volume create {volume_name} {gluster_args} {bricks_str} force"
            else:
                cmd = f"podman exec systemd-aether gluster volume create {volume_name} {bricks_str} force"
            subprocess.run(cmd, shell=True)
            subprocess.run(f"podman exec systemd-aether gluster volume start {volume_name}", shell=True)
            
            # Optimize volume performance options
            opt_cmds = [
                f"podman exec systemd-aether gluster volume set {volume_name} performance.write-behind on",
                f"podman exec systemd-aether gluster volume set {volume_name} performance.cache-size 256MB",
                f"podman exec systemd-aether gluster volume set {volume_name} performance.io-thread-count 32",
                f"podman exec systemd-aether gluster volume set {volume_name} performance.read-ahead on",
                f"podman exec systemd-aether gluster volume set {volume_name} performance.quick-read on",
                f"podman exec systemd-aether gluster volume set {volume_name} performance.stat-prefetch on",
                f"podman exec systemd-aether gluster volume set {volume_name} performance.client-io-threads on"
            ]
            for cmd in opt_cmds:
                subprocess.run(cmd, shell=True)
            
        # Write storage-pools.json
        for ip in servers:
            storage_pool_json = {
                "storage_pool_name": "default-pool",
                "dfs_engine": "glusterfs",
                "local_disks": host_claimed_disks[ip],
                "storage_containers": [
                    {
                        "name": vname,
                        "path": f"/default-pool/{vname.replace('-container', '')}",
                        "ftt": aftt,
                        "compression": "lz4",
                        "quota_bytes": 0
                    } for vname, _, aftt, _ in volumes_to_create
                ]
            }
            json_str = json.dumps(storage_pool_json, indent=2)
            b64_str = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
            run_remote_spark(ip, f"mkdir -p /etc/hci/aether && echo {b64_str} | base64 -d > /etc/hci/aether/storage-pools.json")
            
        # Write spectrum.env
        seeds = ",".join(servers)
        for ip in servers:
            spectrum_env = f"SPECTRUM_API_PORT=8443\nLOCAL_HYPERVISOR_IP={ip}\nCLUSTER_SEEDS={seeds}"
            env_b64 = base64.b64encode(spectrum_env.encode('utf-8')).decode('utf-8')
            run_remote_spark(ip, f"mkdir -p /etc/hci/spectrum && echo {env_b64} | base64 -d > /etc/hci/spectrum/spectrum.env")
            
        # Mount storage containers
        mount_cmd = (
            "podman exec systemd-aether mkdir -p /var/lib/hci/aether/volumes/default-vm-container && "
            "podman exec systemd-aether findmnt /var/lib/hci/aether/volumes/default-vm-container || "
            "podman exec systemd-aether mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-vm-container /var/lib/hci/aether/volumes/default-vm-container"
        )
        run_parallel(servers, mount_cmd)
        
        mount_img_cmd = (
            "podman exec systemd-aether mkdir -p /var/lib/hci/aether/volumes/default-image-container && "
            "podman exec systemd-aether findmnt /var/lib/hci/aether/volumes/default-image-container || "
            "podman exec systemd-aether mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-image-container /var/lib/hci/aether/volumes/default-image-container"
        )
        run_parallel(servers, mount_img_cmd)
        
        # Restart zookeeper and DB to form ring
        run_parallel(servers, "systemctl restart zookeeper")
        run_parallel(servers, "systemctl restart hydra-db")
        time.sleep(5)
        
        # Start spectrum and bifrost
        run_parallel(servers, "systemctl start spectrum")
        run_parallel(servers, "systemctl start bifrost")
        run_parallel(servers, "systemctl start dagur")
        run_parallel(servers, "systemctl start mimir")
        run_parallel(servers, "systemctl start vali")
        run_parallel(servers, "systemctl start catalyst")
        run_parallel(servers, "systemctl start gatoway")
        run_parallel(servers, "systemctl start logos")
        
        self.send_json_response(200, {"message": "Cluster created successfully."})

    def handle_cluster_destroy(self):
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

        # 1. Stop and Delete Gluster Volumes dynamically
        res_list = subprocess.run("podman exec systemd-aether gluster volume list", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if res_list.returncode == 0:
            for vol in res_list.stdout.decode().splitlines():
                vol = vol.strip()
                if vol:
                    subprocess.run(f"podman exec systemd-aether gluster --mode=script volume stop {vol} force", shell=True)
                    subprocess.run(f"podman exec systemd-aether gluster --mode=script volume delete {vol}", shell=True)
                    
        # 2. Stop services
        services = ["logos", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "aether", "hydra-db", "zookeeper"]
        for ip in hosts:
            run_remote_spark(ip, "podman exec systemd-aether umount -f /var/lib/hci/aether/volumes/default-vm-container || true")
            run_remote_spark(ip, "podman exec systemd-aether umount -f /var/lib/hci/aether/volumes/default-image-container || true")
            for svc in services:
                run_remote_spark(ip, f"systemctl stop {svc}")
                
        # 3. Unmount and wipe bricks, clear databases on all hosts
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
    subprocess.run(f"umount -f {mount}", shell=True)
    subprocess.run(f"sed -i '\\\\|{mount}|d' /etc/fstab", shell=True)
    subprocess.run(f"wipefs -a {real_dev}", shell=True)
    subprocess.run(f"rm -rf {mount}", shell=True)

subprocess.run("rm -rf /var/lib/hci/zookeeper/data/* /var/lib/hci/zookeeper/log/* /var/lib/hci/hydra/data/* /var/lib/hci/aether/data/* /var/lib/hci/aether/volumes/* /run/hci/*", shell=True)
subprocess.run("rm -rf /etc/hci/odin /etc/hci/spectrum /etc/hci/cluster.json", shell=True)
subprocess.run("podman rmi localhost/spectrum:latest || true", shell=True)
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
    
    # Check if cluster configuration exists
    import os
    if not os.path.exists("/etc/hci/cluster.json"):
        print("[AUTOSTART] No cluster configuration found (/etc/hci/cluster.json). Skipping autostart.")
        return
        
    if os.path.exists("/etc/hci/maintenance.state"):
        print("[AUTOSTART] Host is in maintenance mode. Skipping database, storage, and UI workloads.")
        return
        
    # 1. Start ZooKeeper unconditionally if it is not active
    print("[AUTOSTART] Ensuring local ZooKeeper is started...")
    res = subprocess.run("systemctl is-active zookeeper", shell=True, stdout=subprocess.PIPE)
    if res.stdout.decode().strip() != "active":
        print("[AUTOSTART] Starting zookeeper service...")
        subprocess.run("systemctl start zookeeper", shell=True)
        
    # 2. Poll local ZooKeeper on port 2181 for quorum consensus
    print("[AUTOSTART] Waiting for local ZooKeeper to establish quorum consensus...")
    quorum_established = False
    
    # We will poll indefinitely every 3 seconds
    while not quorum_established:
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
            # ZooKeeper might not be fully up or accepting connections yet
            pass
            
        if not quorum_established:
            time.sleep(3)
            
    # 3. Quorum established! Now query ZooKeeper for cluster state
    cluster_state = "started"
    try:
        res_state = subprocess.run("podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 get /cluster_state", 
                                   shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out_state = res_state.stdout.decode("utf-8", errors="ignore")
        if "stopped" in out_state:
            cluster_state = "stopped"
        elif "started" in out_state:
            cluster_state = "started"
    except Exception as e:
        print(f"[AUTOSTART] Error querying cluster state from ZooKeeper: {e}")

    if cluster_state == "stopped":
        print("[AUTOSTART] Cluster state is 'stopped'. Skipping database, storage, and UI workloads.")
    else:
        # Autostarting local database, storage, and UI services...
        services = ["hydra-db", "aether", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos"]
        for svc in services:
            res = subprocess.run(f"systemctl is-active {svc}", shell=True, stdout=subprocess.PIPE)
            if res.stdout.decode().strip() != "active":
                print(f"[AUTOSTART] Starting local service {svc}...")
                subprocess.run(f"systemctl start {svc}", shell=True)
            
    print("[AUTOSTART] Autostart completed successfully.")

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
    
    server_address = ('', PORT)
    httpd = SecureHTTPServer(server_address, SparkDaemonHandler, context)
    print(f"Spark Daemon listening on port {PORT} with mTLS...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
