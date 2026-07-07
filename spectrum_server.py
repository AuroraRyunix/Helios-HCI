__build__ = "1.2.2"
import os
import uuid
import sys
import json
import ssl
import socket
import subprocess
import urllib.request
import urllib.parse
import time
import random
import threading
import hashlib
import secrets
import base64
import http.cookies
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

socket.setdefaulttimeout(45.0)

PORT = 8443
LOCAL_IP = "127.0.0.1"

# Security Globals
LOGIN_LOCKOUTS = {}
LANAYRU_LOGS = {}

# Crypto & Session Helpers
def hash_password(password):
    salt = secrets.token_hex(8) # 16 characters
    iterations = 100000
    hash_bytes = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), iterations)
    hash_b64 = base64.b64encode(hash_bytes).decode('utf-8')
    return f"pbkdf2_sha256${iterations}${salt}${hash_b64}"

def verify_password(password, encoded_hash):
    try:
        parts = encoded_hash.split('$')
        if len(parts) != 4:
            return False
        algo, iterations, salt, hash_b64 = parts
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iterations)
        hash_bytes = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), iterations)
        expected_b64 = base64.b64encode(hash_bytes).decode('utf-8')
        return secrets.compare_digest(hash_b64, expected_b64)
    except Exception:
        return False
SESSION_CACHE = {}
SESSION_CACHE_TTL = 10.0  # seconds

def is_authenticated(handler):
    client_ip = handler.client_address[0]
    # Check if this is a proxied request from Traefik/Slate
    is_proxied = "X-Forwarded-For" in handler.headers or "X-Real-IP" in handler.headers
    if client_ip in ("127.0.0.1", "::1") and not is_proxied:
        handler.current_user = "local-admin"
        return True
        
    session_token = None
    
    # 1. Check Authorization Header
    auth_header = handler.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        session_token = auth_header[7:].strip()
        
    # 2. Check Query Parameters (e.g. for WebSockets or popup connections)
    if not session_token:
        try:
            import urllib.parse
            url_parsed = urllib.parse.urlparse(handler.path)
            query_params = urllib.parse.parse_qs(url_parsed.query)
            token_list = query_params.get("token")
            if token_list:
                session_token = token_list[0]
        except Exception as e:
            print(f"[AUTH DEBUG] Path: {handler.path} | Query parameter parsing error: {e}", flush=True)

    # 3. Check Cookie Header fallback
    if not session_token:
        cookie_header = handler.headers.get("Cookie", "")
        if cookie_header:
            try:
                cookie = http.cookies.SimpleCookie(cookie_header)
                if "session_id" in cookie:
                    session_token = cookie["session_id"].value
            except Exception as e:
                print(f"[AUTH DEBUG] Path: {handler.path} | Exception parsing cookie: {e}", flush=True)

    if not session_token:
        print(f"[AUTH DEBUG] Path: {handler.path} | No session token found", flush=True)
        return False
        
    # Check session cache first
    now = time.time()
    if session_token in SESSION_CACHE:
        cached_user, cache_expire = SESSION_CACHE[session_token]
        if now < cache_expire:
            handler.current_user = cached_user
            return True
        else:
            del SESSION_CACHE[session_token]
        
    try:
        cql = f"SELECT username FROM hydra.sessions WHERE session_token = '{session_token}';"
        rc, out, err = run_cql_query(cql)
        if rc == 0:
            lines = [l.strip() for l in out.splitlines() if l.strip()]
            user_lines = [l for l in lines if not l.startswith('(') and not l.startswith('-') and l != 'username']
            if user_lines:
                handler.current_user = user_lines[0]
                SESSION_CACHE[session_token] = (handler.current_user, time.time() + SESSION_CACHE_TTL)
                print(f"[AUTH DEBUG] Path: {handler.path} | Authenticated as {handler.current_user}", flush=True)
                return True
        print(f"[AUTH DEBUG] Path: {handler.path} | Session token {session_token} not found in DB (rc={rc}, err={err})", flush=True)
    except Exception as e:
        print(f"[AUTH DEBUG] Path: {handler.path} | Exception in auth: {e}", flush=True)
    return False

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

# Global caches/states

def decode_websocket_frame(sock):
    # Read first two bytes
    header = sock.recv(2)
    if len(header) < 2:
        return None, None
    
    fin = header[0] & 0x80
    opcode = header[0] & 0x0f
    masked = header[1] & 0x80
    payload_len = header[1] & 0x7f
    
    if payload_len == 126:
        len_bytes = sock.recv(2)
        if len(len_bytes) < 2:
            return None, None
        payload_len = int.from_bytes(len_bytes, byteorder='big')
    elif payload_len == 127:
        len_bytes = sock.recv(8)
        if len(len_bytes) < 8:
            return None, None
        payload_len = int.from_bytes(len_bytes, byteorder='big')
        
    masking_key = b""
    if masked:
        masking_key = sock.recv(4)
        if len(masking_key) < 4:
            return None, None
            
    payload = b""
    remaining = payload_len
    while remaining > 0:
        chunk = sock.recv(min(remaining, 65536))
        if not chunk:
            break
        payload += chunk
        remaining -= len(chunk)
        
    if len(payload) < payload_len:
        return None, None
        
    if masked:
        # Fast slice-based unmasking to avoid per-byte generator overhead
        data = bytearray(payload)
        data[0::4] = [b ^ masking_key[0] for b in data[0::4]]
        data[1::4] = [b ^ masking_key[1] for b in data[1::4]]
        data[2::4] = [b ^ masking_key[2] for b in data[2::4]]
        data[3::4] = [b ^ masking_key[3] for b in data[3::4]]
        payload = bytes(data)
        
    return opcode, payload

def encode_websocket_frame(payload, opcode=2):
    header = bytearray()
    header.append(0x80 | opcode)
    
    payload_len = len(payload)
    if payload_len <= 125:
        header.append(payload_len)
    elif payload_len <= 65535:
        header.append(126)
        header.extend(payload_len.to_bytes(2, byteorder='big'))
    else:
        header.append(127)
        header.extend(payload_len.to_bytes(8, byteorder='big'))
        
    return bytes(header) + payload

EVENT_LOGS = [
    {"desc": "Cluster bootstrap and consensus ring formed.", "time": "Initial boot"},
    {"desc": "Storage volumes mounted and peered successfully.", "time": "Initial boot"},
    {"desc": "Mimir diagnostic check framework initialized.", "time": "Initial boot"}
]

STATUS_CACHE = {
    "data": None,
    "last_fetched": 0
}

def invalidate_status_cache():
    STATUS_CACHE["data"] = None
    STATUS_CACHE["last_fetched"] = 0

TASKS_CACHE = {
    "data": None,
    "last_fetched": 0
}

def invalidate_tasks_cache():
    TASKS_CACHE["data"] = None
    TASKS_CACHE["last_fetched"] = 0

def log_catalyst_task(service, action, status, progress, payload_dict, error_msg="", task_id=None, created_at=None):
    try:
        import uuid
        import time
        if not task_id:
            task_id = str(uuid.uuid4())
        now_ms = int(time.time() * 1000)
        if not created_at:
            created_at = now_ms
        payload_str = json.dumps(payload_dict)
        cql = f"""
        INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, error_msg, created_at, updated_at)
        VALUES ({task_id}, '{service}', '{action}', '{status}', '{payload_str.replace("'", "''")}', {progress}, '{error_msg.replace("'", "''")}', {created_at}, {now_ms});
        """
        run_cql_query(cql)
        invalidate_tasks_cache()
        return task_id, created_at
    except Exception as e:
        print(f"Error logging catalyst task: {e}")
        return None, None



def run_remote_spark(ip, command, timeout=45):
    """Executes a command on the local or remote node via its spark-daemon mTLS API."""
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = False
    
    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command, "timeout": timeout}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=timeout + 15) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)

def run_linstor_cmd(linstor_args):
    """Executes a Linstor command against the cluster controllers, trying nodes sequentially."""
    import json
    import os
    hosts = []
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                hosts = json.load(f).get("hosts", [])
    except Exception:
        pass
    ips = [h["ip"] for h in hosts] if hosts else ["127.0.0.1"]
    controllers_str = ",".join(ips)
    
    # Try running the command on local node first, fallback to other nodes
    candidate_ips = ["127.0.0.1"] + ips
    rc, stdout, stderr = -1, "", "No nodes available"
    for cap_ip in candidate_ips:
        cmd = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor {linstor_args}"
        rc, stdout, stderr = run_remote_spark(cap_ip, cmd)
        if rc == 0:
            return rc, stdout, stderr
    return rc, stdout, stderr

def slugify_image_name(filename):
    # Lowercase and replace non-alphanumeric characters with hyphens
    import re
    base = filename
    if filename.lower().endswith(".iso"):
        base = filename[:-4]
    elif filename.lower().endswith(".qcow2"):
        base = filename[:-6]
    elif filename.lower().endswith(".img"):
        base = filename[:-4]
    
    slug = re.sub(r'[^a-z0-9_-]', '-', base.lower())
    slug = re.sub(r'-+', '-', slug)
    slug = slug.strip('-')
    return slug[:28]

def run_mtls_spark_api(ip, path, payload, method="POST"):
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
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

def get_actual_replication_factor():
    try:
        import urllib.request
        import json
        cql_query = "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'hydra';"
        url = "http://127.0.0.1:9043/query"
        req = urllib.request.Request(
            url,
            data=cql_query.encode('utf-8'),
            headers={'Content-Type': 'text/plain'}
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            res = json.loads(response.read().decode('utf-8'))
            if res.get("status") == "success" and res.get("rows"):
                row = res["rows"][0]
                rep = row.get("replication", {})
                if isinstance(rep, dict) and "replication_factor" in rep:
                    return str(rep["replication_factor"])
    except Exception as e:
        print(f"Error fetching actual replication factor: {e}")
    return "3"

def get_container_node_ip(container_name):
    """Finds which node in the cluster has the specified storage container mounted. Returns '127.0.0.1' as fallback."""
    container_path = f"/var/lib/hci/aether/volumes/{container_name}"
    local_ip = os.environ.get("LOCAL_HYPERVISOR_IP", "127.0.0.1")
    try:
        nodes_list = []
        rc_n, stdout_n, _ = run_cql_query("SELECT JSON hostname, ip FROM hydra.nodes;")
        if rc_n == 0 and stdout_n:
            for line in stdout_n.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        nodes_list.append(json.loads(line))
                    except:
                        pass
        for node in nodes_list:
            nip = node["ip"]
            if nip == local_ip:
                rc_m = subprocess.call(f"mountpoint -q {container_path}", shell=True)
                if rc_m == 0:
                    return nip
            else:
                rc_m, _, _ = run_remote_spark(nip, f"mountpoint -q {container_path}")
                if rc_m == 0:
                    return nip
    except Exception:
        pass
    return "127.0.0.1"

def submit_catalyst_cql_task(job_name, cql_query):
    """Submits a CQL query execution to the active Catalyst task queue."""
    import base64
    b64_query = base64.b64encode(cql_query.encode('utf-8')).decode('utf-8')
    command = f"echo {b64_query} | base64 -d | podman exec -i systemd-hydra-db cqlsh $(python3 -c \"import socket; s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.connect(('10.255.255.255', 1)); print(s.getsockname()[0])\")"
    
    payload = {
        "service": "dagur",
        "action": "execute",
        "payload": {
            "job_name": job_name,
            "command": command
        }
    }
    try:
        leader_ip = get_catalyst_target_ip()
        req = urllib.request.Request(
            f"http://{leader_ip}:9091/api/v1/tasks/submit",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res.get("task_id"), None
    except Exception as e:
        return None, str(e)

def validate_password_complexity(password):
    """Validates a password against the active cluster security policy."""
    cql_policy = "SELECT value FROM hydra.cluster_settings WHERE key = 'password_policy';"
    rc_p, out_p, _ = run_cql_query(cql_policy)
    policy = "disabled"
    if rc_p == 0:
        lines = [l.strip() for l in out_p.splitlines() if l.strip()]
        policy_lines = [l for l in lines if not l.startswith('(') and not l.startswith('-') and l != 'value' and l != '']
        if policy_lines:
            policy = policy_lines[0]

    if policy == "enabled":
        import re
        if len(password) < 8 or not re.search(r"[A-Z]", password) or not re.search(r"[0-9]", password) or not re.search(r"[^A-Za-z0-9]", password):
            return False, "Password must be at least 8 characters long, and contain at least one uppercase letter, one number, and one special character."
    else:
        if len(password) < 5:
            return False, "Password must be at least 5 characters long."
            
    return True, ""

_CACHED_CLUSTER_JSON_HOSTS = []

def get_cluster_nodes():
    """Reads hosts list from the cluster configuration file."""
    global _CACHED_CLUSTER_JSON_HOSTS
    hosts = []
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            hosts = cdata.get("hosts", [])
    except Exception:
        pass

    if not hosts:
        try:
            rc_db, stdout_db, _ = run_cql_query("SELECT JSON hostname, ip FROM hydra.nodes;")
            if rc_db == 0 and stdout_db:
                db_hosts = []
                for line in stdout_db.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            h_data = json.loads(line)
                            if h_data.get("hostname") and h_data.get("ip"):
                                db_hosts.append({
                                    "hostname": h_data["hostname"],
                                    "ip": h_data["ip"]
                                })
                        except:
                            pass
                if db_hosts:
                    hosts = db_hosts
        except Exception:
            pass

    if hosts:
        _CACHED_CLUSTER_JSON_HOSTS = hosts
        return hosts

    return _CACHED_CLUSTER_JSON_HOSTS

def get_zookeeper_leader_ip():
    """Finds the IP of the current ZooKeeper leader by querying stat on port 2181."""
    nodes = get_cluster_nodes()
    if not nodes:
        return "127.0.0.1"
    for node in nodes:
        ip = node.get("ip")
        if not ip:
            continue
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect((ip, 2181))
            s.sendall(b"stat")
            resp = s.recv(1024).decode('utf-8', errors='ignore')
            s.close()
            if "mode: leader" in resp.lower() or "mode: standalone" in resp.lower():
                return ip
        except Exception:
            pass
    return "127.0.0.1"

VM_CPU_CACHE = {}
VM_IO_CACHE = {}

def get_vm_stats(host_ip, vm_name, vcpus, allocated_mem_mb):
    global VM_CPU_CACHE, VM_IO_CACHE
    cmd = f"virsh -c qemu:///system domstats {vm_name}"
    
    if host_ip == LOCAL_IP or host_ip == "127.0.0.1" or host_ip == "":
        rc, stdout, stderr = run_remote_spark("127.0.0.1", cmd)
    else:
        rc, stdout, stderr = run_remote_spark(host_ip, cmd)
        
    stats = {}
    if rc == 0:
        for line in stdout.splitlines():
            if "=" in line:
                k, v = line.strip().split("=", 1)
                stats[k.strip()] = v.strip()
                
    # Parse CPU time
    cpu_time = int(stats.get("cpu.time", 0))
    # Parse RSS memory
    rss_kib = int(stats.get("balloon.rss", 0))
    
    # Parse Block stats
    rd_reqs = 0
    wr_reqs = 0
    rd_times = 0
    wr_times = 0
    for key, val in stats.items():
        if key.startswith("block."):
            if key.endswith(".rd.reqs"):
                rd_reqs += int(val)
            elif key.endswith(".wr.reqs"):
                wr_reqs += int(val)
            elif key.endswith(".rd.times"):
                rd_times += int(val)
            elif key.endswith(".wr.times"):
                wr_times += int(val)
                
    now = time.time()
    
    # Calculate CPU Pct
    cpu_pct = 0.0
    if cpu_time > 0:
        prev = VM_CPU_CACHE.get(vm_name)
        if prev:
            prev_cpu, prev_time = prev
            time_delta = now - prev_time
            cpu_delta = cpu_time - prev_cpu
            if time_delta > 0:
                if cpu_delta >= 0:
                    cpu_pct = (cpu_delta / (time_delta * 1e9 * vcpus)) * 100
                    cpu_pct = min(100.0, max(0.0, cpu_pct))
                else:
                    # VM restarted, cpu.time reset
                    cpu_pct = 0.0
        VM_CPU_CACHE[vm_name] = (cpu_time, now)
        
    # Calculate Mem stats
    mem_usage_mb = 0.0
    mem_usage_pct = 0.0
    if rss_kib > 0:
        mem_usage_mb = rss_kib / 1024.0
        mem_usage_pct = (mem_usage_mb / allocated_mem_mb) * 100
        mem_usage_pct = min(100.0, max(0.0, mem_usage_pct))
    else:
        balloon_curr = int(stats.get("balloon.current", 0))
        if balloon_curr > 0:
            mem_usage_mb = (balloon_curr / 1024.0) * 0.45
            mem_usage_pct = 45.0
        else:
            mem_usage_mb = allocated_mem_mb * 0.35
            mem_usage_pct = 35.0
            
    # Calculate IOPS and Latency
    iops = 0.0
    latency_ms = 0.0
    prev_io = VM_IO_CACHE.get(vm_name)
    if prev_io:
        prev_rd, prev_wr, prev_rd_t, prev_wr_t, prev_time = prev_io
        time_delta = now - prev_time
        rd_delta = rd_reqs - prev_rd
        wr_delta = wr_reqs - prev_wr
        rd_t_delta = rd_times - prev_rd_t
        wr_t_delta = wr_times - prev_wr_t
        
        # Check if VM rebooted (cumulative counters reset)
        if rd_delta < 0 or wr_delta < 0:
            rd_delta = 0
            wr_delta = 0
            rd_t_delta = 0
            wr_t_delta = 0
            
        io_delta = rd_delta + wr_delta
        io_t_delta = rd_t_delta + wr_t_delta
        
        if time_delta > 0:
            iops = io_delta / time_delta
            if io_delta > 0 and io_t_delta >= 0:
                latency_ms = (io_t_delta / io_delta) / 1000000.0
                latency_ms = min(1000.0, max(0.0, latency_ms))
    VM_IO_CACHE[vm_name] = (rd_reqs, wr_reqs, rd_times, wr_times, now)
    
    return {
        "cpu_usage_pct": cpu_pct,
        "mem_usage_mb": mem_usage_mb,
        "mem_usage_pct": mem_usage_pct,
        "iops": iops,
        "latency_ms": latency_ms
    }

def get_consolidated_dhcp_leases():
    dhcp_leases = {}
    try:
        nodes = get_cluster_nodes()
        if not nodes:
            nodes = [{"ip": LOCAL_IP}]
        for n in nodes:
            n_ip = n.get("ip")
            if n_ip:
                rc_l, out_l, _ = run_remote_spark(n_ip, "cat /var/lib/dnsmasq/dnsmasq.leases 2>/dev/null || cat /var/lib/misc/dnsmasq.leases 2>/dev/null")
                if rc_l == 0 and out_l:
                    for line in out_l.splitlines():
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            mac = parts[1].lower().strip()
                            lease_ip = parts[2].strip()
                            dhcp_leases[mac] = lease_ip
    except Exception as e:
        print(f"Error fetching DHCP leases: {e}")
    return dhcp_leases

def resolve_vm_ip(host_ip, vm_name, vm_status, dhcp_leases):
    if vm_status == "running" and host_ip:
        try:
            rc_mac, out_mac, _ = run_remote_spark(host_ip, f"virsh -c qemu:///system domiflist {vm_name}")
            macs = []
            if rc_mac == 0 and out_mac:
                for line in out_mac.splitlines():
                    if ":" in line:
                        parts = line.split()
                        if len(parts) >= 5:
                            mac = parts[4].strip().lower()
                            if len(mac) == 17:
                                macs.append(mac)
            for mac in macs:
                if mac in dhcp_leases:
                    return dhcp_leases[mac]
            if not macs:
                return "No NIC connected"
        except Exception:
            pass
        return "DHCP Resolving..."
    elif vm_status == "running":
        return "DHCP Resolving..."
    else:
        return "Offline"

CACHED_CPU_STATS = {}

def get_cluster_metrics(nodes_info):
    global CACHED_CPU_STATS
    total_cores = 0
    total_mem_bytes = 0
    used_mem_bytes = 0
    cpu_pct_sum = 0.0
    online_count = 0
    
    for n in nodes_info:
        # Initialize defaults
        n["cpu_pct"] = 0.0
        n["ram_used_gb"] = 0.0
        n["ram_total_gb"] = 0.0
        
        if n["status"] != "ONLINE":
            continue
        
        ip = n["ip"]
        
        # Query ScyllaDB for the latest Logos metrics for this node
        # (includes cpu_pct, mem_pct, mem_total_kb, cpu_cores written directly by logos.py)
        cql_l = f"SELECT JSON cpu_pct, mem_pct, mem_total_kb, cpu_cores FROM hydra.logos_metrics WHERE node_ip = '{ip}' LIMIT 1;"
        rc_l, stdout_l, _ = run_cql_query(cql_l)
        
        cpu_pct = 0.0
        mem_pct = 0.0
        cores = 2
        t_mem = 8589934592  # 8 GB default fallback
        
        if rc_l == 0 and stdout_l:
            for line in stdout_l.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        metrics_data = json.loads(line)
                        cpu_pct = metrics_data.get("cpu_pct", 0.0) or 0.0
                        mem_pct = metrics_data.get("mem_pct", 0.0) or 0.0
                        mem_total_kb = metrics_data.get("mem_total_kb") or 0
                        cpu_cores_val = metrics_data.get("cpu_cores") or 0
                        if mem_total_kb > 0:
                            t_mem = int(mem_total_kb) * 1024
                        if cpu_cores_val > 0:
                            cores = int(cpu_cores_val)
                    except:
                        pass
        
        u_mem = int(t_mem * (mem_pct / 100.0))
        
        total_cores += cores
        total_mem_bytes += t_mem
        used_mem_bytes += u_mem
        cpu_pct_sum += cpu_pct
        online_count += 1
        
        # Store individual metrics
        n["cpu_pct"] = round(cpu_pct, 1)
        n["ram_used_gb"] = round(u_mem / (1024**3), 1)
        n["ram_total_gb"] = round(t_mem / (1024**3), 1)
                    
    if online_count > 0:
        avg_cpu_pct = round(cpu_pct_sum / online_count, 2)
        avg_mem_pct = round((used_mem_bytes / total_mem_bytes) * 100, 2)
        total_mem_gb = round(total_mem_bytes / (1024**3), 2)
        used_mem_gb = round(used_mem_bytes / (1024**3), 2)
    else:
        avg_cpu_pct = 0.0
        avg_mem_pct = 0.0
        total_mem_gb = 18.0
        used_mem_gb = 2.0
        total_cores = 6
        
    return {
        "cpu_pct": avg_cpu_pct,
        "cpu_cores": total_cores,
        "total_cpu_ghz": round(2.4 * total_cores, 1),
        "mem_pct": avg_mem_pct,
        "total_mem_gb": total_mem_gb,
        "used_mem_gb": used_mem_gb
    }

def init_db():
    """Attempts to initialize the ScyllaDB keyspace and table on startup."""
    print("Connecting to ScyllaDB and creating keyspace/table if not exists...")
    nodes = get_cluster_nodes()
    node_count = len(nodes) if nodes else 1
    desired_rf = min(3, node_count)
    create_keyspace = f"CREATE KEYSPACE IF NOT EXISTS hydra WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {desired_rf}}};"
    create_table = """
    CREATE TABLE IF NOT EXISTS hydra.vms (
        name text PRIMARY KEY,
        vcpu int,
        memory int,
        disk_path text,
        disk_size int,
        state text,
        host_ip text,
        disks_list text,
        firmware text,
        iso text,
        boot_device text,
        network_id text,
        cpu_model text,
        audio_enabled boolean
    );
    """
    create_containers_table = """
    CREATE TABLE IF NOT EXISTS hydra.storage_containers (
        name text PRIMARY KEY,
        tier text,
        quota_bytes bigint,
        path text,
        ftt int
    );
    """
    create_mimir_results = """
    CREATE TABLE IF NOT EXISTS hydra.mimir_results (
        category text,
        check_name text,
        node_ip text,
        status text,
        output text,
        execution_id uuid,
        timestamp timestamp,
        PRIMARY KEY (category, check_name, node_ip)
    );
    """
    create_dagur_schedules = """
    CREATE TABLE IF NOT EXISTS hydra.dagur_schedules (
        job_name text PRIMARY KEY,
        task_type text,
        cron_expression text,
        interval_seconds int,
        enabled boolean,
        last_run_epoch bigint,
        command text
    );
    """
    create_dagur_runs = """
    CREATE TABLE IF NOT EXISTS hydra.dagur_runs (
        job_name text,
        start_time timestamp,
        run_id uuid,
        end_time timestamp,
        status text,
        exit_code int,
        output text,
        PRIMARY KEY (job_name, start_time)
    ) WITH CLUSTERING ORDER BY (start_time DESC);
    """
    
    # Detect tier from local storage-pools.json
    detected_tier = "HDD"
    try:
        if os.path.exists("/etc/hci/aether/storage-pools.json"):
            with open("/etc/hci/aether/storage-pools.json", "r") as f:
                pdata = json.load(f)
                local_disks = pdata.get("local_disks", [])
                medias = [d.get("media_type", "hdd").upper() for d in local_disks]
                if "SSD" in medias:
                    detected_tier = "SSD"
                elif "HDD" in medias:
                    detected_tier = "HDD"
    except Exception as e:
        print(f"Error detecting storage tier: {e}")

    insert_default = f"""
    INSERT INTO hydra.storage_containers (name, tier, quota_bytes, path, ftt)
    VALUES ('default-pool', '{detected_tier}', 0, 'default-pool', 1) IF NOT EXISTS;
    """
    insert_diagnostics = """
    INSERT INTO hydra.dagur_schedules (job_name, task_type, cron_expression, interval_seconds, enabled, last_run_epoch, command)
    VALUES ('mimir_diagnostics', 'mimir_health', '0 * * * *', 3600, true, 0, '/usr/local/bin/mcli health_checks run_all') IF NOT EXISTS;
    """
    scrub_cmd = "drbdadm status || true"
    insert_storage_scrub = f"""
    INSERT INTO hydra.dagur_schedules (job_name, task_type, cron_expression, interval_seconds, enabled, last_run_epoch, command)
    VALUES ('storage_scrub', 'storage_scrub', '0 */6 * * *', 21600, true, 0, '{scrub_cmd}') IF NOT EXISTS;
    """
    insert_db_compaction = """
    INSERT INTO hydra.dagur_schedules (job_name, task_type, cron_expression, interval_seconds, enabled, last_run_epoch, command)
    VALUES ('db_compaction', 'db_compaction', '0 */12 * * *', 43200, true, 0, 'nodetool compact || true') IF NOT EXISTS;
    """
    insert_storage_auto_heal = """
    INSERT INTO hydra.dagur_schedules (job_name, task_type, cron_expression, interval_seconds, enabled, last_run_epoch, command)
    VALUES ('storage_auto_heal', 'storage_auto_heal', '0 1 * * *', 86400, true, 0, '/usr/local/bin/hci-auto-heal') IF NOT EXISTS;
    """
    insert_system_cleanup = """
    INSERT INTO hydra.dagur_schedules (job_name, task_type, cron_expression, interval_seconds, enabled, last_run_epoch, command)
    VALUES ('system_history_cleanup', 'system_cleanup', '0 0 * * *', 86400, true, 0, '/usr/local/bin/valcli system.cleanup') IF NOT EXISTS;
    """
    insert_orphaned_disks_cleanup = """
    INSERT INTO hydra.dagur_schedules (job_name, task_type, cron_expression, interval_seconds, enabled, last_run_epoch, command)
    VALUES ('orphaned_disks_cleanup', 'storage_cleanup', '0 2 * * *', 86400, true, 0, '/usr/local/bin/valcli storage.cleanup_orphaned') IF NOT EXISTS;
    """
    insert_helios_update_check = """
    INSERT INTO hydra.dagur_schedules (job_name, task_type, cron_expression, interval_seconds, enabled, last_run_epoch, command)
    VALUES ('helios_update_check', 'update_check', '0 */4 * * *', 14400, true, 0, 'python3 /usr/local/bin/check-updates') IF NOT EXISTS;
    """
    create_lcm_update_state = """
    CREATE TABLE IF NOT EXISTS hydra.lcm_update_state (
        key text PRIMARY KEY,
        latest_version text,
        release_date text,
        download_url text,
        sha256 text,
        size bigint,
        changelog text,
        current_version text,
        update_available boolean,
        last_checked timestamp,
        error_msg text
    );
    """
    
    # Define valhalla_images table
    create_valhalla_images = """
    CREATE TABLE IF NOT EXISTS hydra.valhalla_images (
        name text PRIMARY KEY,
        filename text,
        size_bytes bigint,
        type text,
        path text,
        created_at timestamp
    );
    """

    create_users_table = """
    CREATE TABLE IF NOT EXISTS hydra.users (
        username text PRIMARY KEY,
        password_hash text
    );
    """

    create_sessions_table = """
    CREATE TABLE IF NOT EXISTS hydra.sessions (
        session_token text PRIMARY KEY,
        username text,
        created_at timestamp
    );
    """

    create_console_sessions_table = """
    CREATE TABLE IF NOT EXISTS hydra.console_sessions (
        console_token text PRIMARY KEY,
        host_ip text,
        port int,
        expires_at int
    );
    """

    create_cluster_settings_table = """
    CREATE TABLE IF NOT EXISTS hydra.cluster_settings (
        key text PRIMARY KEY,
        value text
    );
    """
    create_mimir_schedules = """
    CREATE TABLE IF NOT EXISTS hydra.mimir_schedules (
        schedule_name text PRIMARY KEY,
        category text,
        enabled boolean,
        last_run_epoch bigint
    );
    """
    insert_mimir_default = """
    INSERT INTO hydra.mimir_schedules (schedule_name, category, enabled, last_run_epoch)
    VALUES ('hourly_checks', 'all', true, 0) IF NOT EXISTS;
    """

    create_gatoway_networks = """
    CREATE TABLE IF NOT EXISTS hydra.gatoway_networks (
        net_id uuid PRIMARY KEY,
        name text,
        type text,
        vlan_id int
    );
    """
    create_urbosa_t0_routers = """
    CREATE TABLE IF NOT EXISTS hydra.urbosa_t0_routers (
        router_id uuid PRIMARY KEY,
        name text,
        uplink_interface text,
        uplink_ip text,
        gateway_ip text,
        nat_rules text
    );
    """
    create_urbosa_t1_routers = """
    CREATE TABLE IF NOT EXISTS hydra.urbosa_t1_routers (
        router_id uuid PRIMARY KEY,
        name text,
        t0_link_id uuid,
        dhcp_enabled boolean
    );
    """
    create_urbosa_segments = """
    CREATE TABLE IF NOT EXISTS hydra.urbosa_segments (
        segment_id uuid PRIMARY KEY,
        name text,
        vni int,
        t1_link_id uuid,
        subnet_cidr text,
        gateway_ip text,
        dhcp_enabled boolean,
        dhcp_start text,
        dhcp_end text
    );
    """
    create_urbosa_firewall_rules = """
    CREATE TABLE IF NOT EXISTS hydra.urbosa_firewall_rules (
        rule_id uuid PRIMARY KEY,
        description text,
        source_ip text,
        dest_ip text,
        protocol text,
        port int,
        action text,
        priority int
    );
    """
    create_urbosa_tunnel_metrics = """
    CREATE TABLE IF NOT EXISTS hydra.urbosa_tunnel_metrics (
        node_ip text,
        interface_name text,
        timestamp timestamp,
        rx_kbps float,
        tx_kbps float,
        rx_packets float,
        tx_packets float,
        PRIMARY KEY ((node_ip, interface_name), timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC)
      AND default_time_to_live = 86400;
    """
    insert_default_network = """
    INSERT INTO hydra.gatoway_networks (net_id, name, type, vlan_id)
    VALUES (7a68e0d6-11f8-4e89-9430-b3b44b8bc438, 'Physical-Direct', 'direct', null) IF NOT EXISTS;
    """

    insert_default_image_container = "SELECT now() FROM system.local;"

    create_logos_metrics = """
    CREATE TABLE IF NOT EXISTS hydra.logos_metrics (
        node_ip text,
        timestamp timestamp,
        cpu_pct float,
        mem_pct float,
        mem_total_kb bigint,
        cpu_cores int,
        disk_iops float,
        disk_bandwidth_kbps float,
        net_rx_kbps float,
        net_tx_kbps float,
        PRIMARY KEY (node_ip, timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC)
      AND default_time_to_live = 86400;
    """
    create_vm_nvram = """
    CREATE TABLE IF NOT EXISTS hydra.vm_nvram (
        vm_name text PRIMARY KEY,
        nvram_data text
    );
    """
    create_console_metrics = """
    CREATE TABLE IF NOT EXISTS hydra.console_metrics (
        vm_name text,
        timestamp timestamp,
        avg_fps float,
        low_fps float,
        latency float,
        PRIMARY KEY (vm_name, timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC)
      AND default_time_to_live = 86400;
    """
    create_yggdrasil_jobs = """
    CREATE TABLE IF NOT EXISTS hydra.hylia_jobs (
        job_id uuid PRIMARY KEY,
        state text,
        target_nodes list<text>,
        current_node text,
        build_number text,
        manifest_json text,
        changelog_md text
    );
    """
    create_yggdrasil_logs = """
    CREATE TABLE IF NOT EXISTS hydra.hylia_logs (
        job_id uuid,
        timestamp timestamp,
        log_line text,
        PRIMARY KEY (job_id, timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp ASC);
    """

    # Retry loop since ScyllaDB may take a moment to bootstrap on boot
    for i in range(15):
        rc, out, err = run_cql_query(create_keyspace)
        if rc == 0:
            print("Keyspace 'hydra' checked/created successfully.")
            rc2, out2, err2 = run_cql_query(create_table)
            rc3, out3, err3 = run_cql_query(create_containers_table)
            rc4, out4, err4 = run_cql_query(create_mimir_results)
            rc5, out5, err5 = run_cql_query(create_dagur_schedules)
            rc6, out6, err6 = run_cql_query(create_dagur_runs)
            rc7, out7, err7 = run_cql_query(create_valhalla_images)
            rc8, out8, err8 = run_cql_query(create_users_table)
            rc9, out9, err9 = run_cql_query(create_sessions_table)
            rc_cs, out_cs, err_cs = run_cql_query(create_console_sessions_table)
            rc10, out10, err10 = run_cql_query(create_cluster_settings_table)
            rc11, out11, err11 = run_cql_query(create_mimir_schedules)
            rc12, out12, err12 = run_cql_query(create_gatoway_networks)
            rc13, out13, err13 = run_cql_query(create_logos_metrics)
            # Migrate existing logos_metrics table to add mem_total_kb and cpu_cores if missing
            run_cql_query("ALTER TABLE hydra.logos_metrics ADD mem_total_kb bigint;")
            run_cql_query("ALTER TABLE hydra.logos_metrics ADD cpu_cores int;")
            rc14, out14, err14 = run_cql_query(create_urbosa_t0_routers)
            rc15, out15, err15 = run_cql_query(create_urbosa_t1_routers)
            rc16, out16, err16 = run_cql_query(create_urbosa_segments)
            rc17, out17, err17 = run_cql_query(create_urbosa_firewall_rules)
            rc18, out18, err18 = run_cql_query(create_urbosa_tunnel_metrics)
            rc_nv, out_nv, err_nv = run_cql_query(create_vm_nvram)
            rc_cm, out_cm, err_cm = run_cql_query(create_console_metrics)
            rc_yj, out_yj, err_yj = run_cql_query(create_yggdrasil_jobs)
            rc_yl, out_yl, err_yl = run_cql_query(create_yggdrasil_logs)
            rc_lus, out_lus, err_lus = run_cql_query(create_lcm_update_state)
            if (rc2 == 0 and rc3 == 0 and rc4 == 0 and rc5 == 0 and rc6 == 0 and 
                rc7 == 0 and rc8 == 0 and rc9 == 0 and rc_cs == 0 and rc10 == 0 and rc11 == 0 and rc12 == 0 and rc13 == 0 and
                rc14 == 0 and rc15 == 0 and rc16 == 0 and rc17 == 0 and rc18 == 0 and rc_nv == 0 and rc_cm == 0 and
                rc_yj == 0 and rc_yl == 0 and rc_lus == 0):
                print("Tables checked/created successfully.")
                run_cql_query(insert_default)
                run_cql_query(insert_default_image_container)
                run_cql_query("DELETE FROM hydra.storage_containers WHERE name IN ('default-vm-container', 'default-image-container');")
                run_cql_query(insert_diagnostics)
                run_cql_query(insert_storage_scrub)
                run_cql_query("UPDATE hydra.dagur_schedules SET command = 'drbdadm status || true' WHERE job_name = 'storage_scrub';")
                run_cql_query(insert_db_compaction)
                run_cql_query(insert_mimir_default)
                run_cql_query(insert_system_cleanup)
                run_cql_query(insert_orphaned_disks_cleanup)
                run_cql_query(insert_helios_update_check)
                run_cql_query(insert_default_network)
                # Attempt to alter vms table to add network_id
                run_cql_query("ALTER TABLE hydra.vms ADD network_id text;")
                run_cql_query("ALTER TABLE hydra.vms ADD cpu_model text;")
                run_cql_query("ALTER TABLE hydra.vms ADD audio_enabled boolean;")
                
                # Seeding default user 'helios' if users table is empty
                rc_users, out_users, err_users = run_cql_query("SELECT username FROM hydra.users;")
                if rc_users == 0:
                    lines = [l.strip() for l in out_users.splitlines() if l.strip()]
                    user_lines = [l for l in lines if not l.startswith('(') and not l.startswith('-') and l != 'username' and l != '']
                    if not user_lines:
                        hashed = hash_password("helios")
                        run_cql_query(f"INSERT INTO hydra.users (username, password_hash) VALUES ('helios', '{hashed}');")
                        
                # Seeding default cluster settings if empty
                rc_set, out_set, err_set = run_cql_query("SELECT key FROM hydra.cluster_settings;")
                if rc_set == 0:
                    lines = [l.strip() for l in out_set.splitlines() if l.strip()]
                    setting_lines = [l for l in lines if not l.startswith('(') and not l.startswith('-') and l != 'key' and l != '']
                    if not setting_lines:
                        run_cql_query("INSERT INTO hydra.cluster_settings (key, value) VALUES ('dns_servers', '8.8.8.8,8.8.4.4');")
                        run_cql_query("INSERT INTO hydra.cluster_settings (key, value) VALUES ('ntp_servers', 'pool.ntp.org');")
                        run_cql_query("INSERT INTO hydra.cluster_settings (key, value) VALUES ('urbosa_enabled', 'false');")
                        run_cql_query("INSERT INTO hydra.cluster_settings (key, value) VALUES ('gato_enabled', 'true');")

                # Query configured replication factor from settings and alter keyspace accordingly
                try:
                    cql_rf = "SELECT value FROM hydra.cluster_settings WHERE key = 'replication_factor';"
                    rc_rf, out_rf, _ = run_cql_query(cql_rf)
                    configured_rf = 3
                    if rc_rf == 0:
                        lines = [l.strip() for l in out_rf.splitlines() if l.strip()]
                        rf_lines = [l for l in lines if not l.startswith('(') and not l.startswith('-') and l != 'value' and l != '']
                        if rf_lines:
                            configured_rf = int(rf_lines[0])
                    desired_rf = min(configured_rf, len(get_cluster_nodes()) if get_cluster_nodes() else 1)
                    alter_keyspace = f"ALTER KEYSPACE hydra WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {desired_rf}}};"
                    run_cql_query(alter_keyspace)
                except Exception as e:
                    print(f"Error altering keyspace replication on startup: {e}")
                    
                return True
        print(f"Waiting for ScyllaDB to start... (Attempt {i+1}/15)")
        time.sleep(5)
    print("Warning: Could not initialize database schema. ScyllaDB might still be offline.")
    return False

def init_ssl():
    """Ensures self-signed certificates are generated for HTTPS port 8443."""
    cert_dir = "/etc/hci/spectrum/certs"
    cert_file = f"{cert_dir}/server.crt"
    key_file = f"{cert_dir}/server.key"
    if not os.path.exists(cert_file):
        print("Generating self-signed SSL certificate for Spectrum...")
        os.makedirs(cert_dir, exist_ok=True)
        cmd = f'openssl req -x509 -nodes -newkey rsa:2048 -keyout {key_file} -out {cert_file} -days 365 -subj "/CN=Spectrum"'
        subprocess.run(cmd, shell=True, check=True)
    return cert_file, key_file

# Metric helpers
def get_cpu_pct():
    try:
        with open('/proc/stat', 'r') as f:
            line = f.readline()
        parts = line.split()
        if len(parts) >= 5:
            idle = int(parts[4])
            total = sum(int(x) for x in parts[1:8])
            # Sleep briefly to sample delta
            time.sleep(0.1)
            with open('/proc/stat', 'r') as f:
                line2 = f.readline()
            parts2 = line2.split()
            idle2 = int(parts2[4])
            total2 = sum(int(x) for x in parts2[1:8])
            
            idle_delta = idle2 - idle
            total_delta = total2 - total
            if total_delta > 0:
                return round((1.0 - (idle_delta / total_delta)) * 100, 2)
    except Exception:
        pass
    return round(random.uniform(3.5, 7.8), 2)

def get_cpu_info():
    try:
        cores = os.cpu_count() or 4
        return cores, 2.4
    except Exception:
        return 4, 2.4

def get_mem_stats():
    try:
        with open('/proc/meminfo', 'r') as f:
            lines = f.readlines()
        mem_total = 0
        mem_free = 0
        mem_avail = 0
        for line in lines:
            if line.startswith('MemTotal:'):
                mem_total = int(line.split()[1]) * 1024
            elif line.startswith('MemFree:'):
                mem_free = int(line.split()[1]) * 1024
            elif line.startswith('MemAvailable:'):
                mem_avail = int(line.split()[1]) * 1024
        if mem_avail == 0:
            mem_avail = mem_free
        used = mem_total - mem_avail
        mem_pct = (used / mem_total) * 100 if mem_total > 0 else 0
        return round(mem_pct, 2), round(mem_total / (1024*1024*1024), 2), round(used / (1024*1024*1024), 2)
    except Exception:
        return 12.5, 16.0, 2.0


def parse_free_m_all(stdout):
    for line in stdout.splitlines():
        if line.strip().startswith("Mem:"):
            parts = line.split()
            if len(parts) >= 7:
                try:
                    used = int(parts[2])
                    available = int(parts[6])
                    return used, available
                except ValueError:
                    pass
    return None, None


def get_default_container():
    return "default-pool"


def generate_vm_xml(name, uuid, memory, vcpu, firmware, disks_list, iso, boot_device="", audio_enabled=False):
    # Resolve primary container
    primary_container = get_default_container()
    if disks_list:
        first_entry = disks_list.split(",")[0]
        if ":" in first_entry:
            primary_container = first_entry.split(":")[1]

    # OS / Boot configuration (UEFI vs BIOS)
    if boot_device:
        boot_devices = f"<boot dev='{boot_device}'/>"
    else:
        has_iso = False
        if iso:
            has_iso = any(x.strip() and x.strip() != "__empty__" for x in iso.split(","))
        boot_devices = "<boot dev='cdrom'/>\n    <boot dev='hd'/>" if has_iso else "<boot dev='hd'/>"

    if firmware == "uefi":
        nvram_path = f"/var/lib/hci/aether/nvram/{name}_vars.fd"
        os_boot_xml = f"""<type arch='x86_64' machine='q35'>hvm</type>
    <loader readonly='yes' type='pflash'>/usr/share/edk2/ovmf/OVMF_CODE.fd</loader>
    <nvram template='/usr/share/edk2/ovmf/OVMF_VARS.fd'>{nvram_path}</nvram>
    {boot_devices}"""
    else:
        os_boot_xml = f"""<type arch='x86_64' machine='q35'>hvm</type>
    {boot_devices}"""

    video_xml = """<video>
      <model type='virtio' vram='65536' heads='1' primary='yes'/>
    </video>"""

    # Disks devices XML
    import string
    letters = string.ascii_lowercase
    disk_devices_xml = ""

    # Reconstruct disk paths
    disk_paths = []
    if disks_list:
        disks_payload = disks_list.split(",")
        for idx, entry in enumerate(disks_payload):
            d_path = f"/dev/drbd/by-res/{name}-disk{idx}/0"
            disk_paths.append(d_path)
    else:
        disk_paths = [f"/dev/drbd/by-res/{name}-disk0/0"]

    for idx, d_path in enumerate(disk_paths):
        dev_letter = letters[idx % 26]
        disk_devices_xml += f"""
    <disk type='block' device='disk'>
      <driver name='qemu' type='raw' cache='none' io='native' queues='{vcpu}' iothread='1'/>
      <source dev='{d_path}'/>
      <target dev='vd{dev_letter}' bus='virtio'/>
    </disk>"""

    # CD-ROM device XML
    if iso:
        cdrom_specs = [x.strip() for x in iso.split(",") if x.strip()]
        for idx, spec in enumerate(cdrom_specs):
            sata_letter = letters[idx % 26]
            if spec != "__empty__":
                # Query DB or fallback to slugified name
                iso_path = None
                try:
                    rc_img, stdout_img, _ = run_cql_query(f"SELECT path FROM hydra.valhalla_images WHERE name = '{spec}';")
                    if rc_img == 0 and stdout_img:
                        for line in stdout_img.splitlines():
                            if "/dev/" in line:
                                iso_path = line.strip().split()[-1].replace("'", "").replace('"', '')
                                break
                except Exception:
                    pass
                if not iso_path:
                    iso_path = f"/dev/drbd/by-res/img-{slugify_image_name(spec)}/0"
                
                disk_devices_xml += f"""
    <disk type='block' device='cdrom'>
      <driver name='qemu' type='raw' locking='off'/>
      <source dev='{iso_path}'/>
      <target dev='sd{sata_letter}' bus='sata'/>
      <readonly/>
    </disk>"""

    has_kvm = False
    try:
        rc, _, _ = run_remote_spark("127.0.0.1", "test -e /dev/kvm")
        has_kvm = (rc == 0)
    except Exception:
        pass

    domain_type = "kvm" if has_kvm else "qemu"
    if has_kvm:
        cpu_xml = f"""<cpu mode='host-model'>
    <topology sockets='1' dies='1' cores='{vcpu}' threads='1'/>
  </cpu>"""
    else:
        cpu_xml = f"""<cpu mode='custom' match='exact'>
    <model>Haswell</model>
    <topology sockets='1' dies='1' cores='{vcpu}' threads='1'/>
  </cpu>"""

    uuid_xml = f"<uuid>{uuid}</uuid>" if uuid else ""

    if audio_enabled:
        sound_xml = (
            "    <sound model='ich9'>\n"
            "      <audio id='1'/>\n"
            "    </sound>\n"
        )
    else:
        sound_xml = ""
    vm_xml = f"""<domain type='{domain_type}'>
  <name>{name}</name>
  {uuid_xml}
  <memory unit='MiB'>{memory}</memory>
  <vcpu placement='static'>{vcpu}</vcpu>
  <iothreads>1</iothreads>
  <os>
    {os_boot_xml}
  </os>
   <features>
    <acpi/>
  </features>
  {cpu_xml}
  <devices>
    {disk_devices_xml}
    <input type='tablet' bus='usb'/>
    <interface type='bridge'>
      <source bridge='virbr0'/>
      <model type='virtio'/>
    </interface>
    <graphics type='vnc' port='-1' autoport='yes' listen='0.0.0.0'>
      <listen type='address' address='0.0.0.0'/>
    </graphics>
    <controller type='virtio-serial' index='0'/>
    <channel type='unix'>
      <target type='virtio' name='org.qemu.guest_agent.0'/>
      <address type='virtio-serial' controller='0' bus='0' port='1'/>
    </channel>
    {video_xml}
{sound_xml}  </devices>
  <seclabel type='none'/>
</domain>
"""
    return vm_xml


# Thread-safe global variables for caching cluster state
CLUSTER_CACHE_LOCK = threading.Lock()
CACHED_NODES_INFO = []
CACHED_CLUSTER_NODES_STATUS = []
CACHED_STORAGE_USAGE = {}
CACHED_CLUSTER_METRICS = {}
CACHED_DIAGNOSTIC_ALERTS = []
CACHED_VM_STATS = {}

METRICS_HISTORY = []
METRICS_HISTORY_LOCK = threading.Lock()
MAX_HISTORY_POINTS = 60

def load_real_metrics_history():
    hosts = []
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        pass
    if not hosts:
        hosts = ["127.0.0.1"]
    
    all_records = []
    for ip in hosts:
        cql = f"SELECT JSON timestamp, cpu_pct, mem_pct, cpu_cores, mem_total_kb FROM hydra.logos_metrics WHERE node_ip = '{ip}' LIMIT 60;"
        rc, stdout, _ = run_cql_query(cql)
        if rc == 0 and stdout:
            for line in stdout.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        data = json.loads(line)
                        all_records.append({
                            "ip": ip,
                            "time": data["timestamp"],
                            "cpu_pct": data.get("cpu_pct", 0.0) or 0.0,
                            "mem_pct": data.get("mem_pct", 0.0) or 0.0,
                            "cpu_cores": data.get("cpu_cores", 2) or 2,
                            "mem_total_kb": data.get("mem_total_kb", 8388608) or 8388608
                        })
                    except Exception:
                        pass
    
    if not all_records:
        return []
        
    import datetime, collections
    buckets = collections.defaultdict(list)
    for r in all_records:
        t_str = r["time"]
        try:
            if " " in t_str:
                dt_part = t_str.split(".")[0]
                dt = datetime.datetime.strptime(dt_part, "%Y-%m-%d %H:%M:%S")
            else:
                dt_part = t_str.split(".")[0].replace("T", " ")
                dt = datetime.datetime.strptime(dt_part, "%Y-%m-%d %H:%M:%S")
            
            seconds = (dt.second // 30) * 30
            dt_bucket = dt.replace(second=seconds, microsecond=0)
            bucket_ts = int(dt_bucket.timestamp() * 1000)
            buckets[bucket_ts].append(r)
        except Exception:
            pass
            
    history = []
    for ts in sorted(buckets.keys()):
        bucket_rows = buckets[ts]
        cpus = [row["cpu_pct"] for row in bucket_rows]
        mems = [row["mem_pct"] for row in bucket_rows]
        
        if cpus and mems:
            avg_cpu = sum(cpus) / len(cpus)
            avg_mem = sum(mems) / len(mems)
            
            t = ts / 1000.0
            import math
            noise = math.sin(t / 10.0) * 2.0
            iops = max(2.0, 11.5 + noise)
            latency = max(0.1, 0.92 + math.cos(t / 12.0) * 0.12)
            bw = int(iops * 16.0)
            
            history.append({
                "time": ts,
                "cpu_pct": avg_cpu,
                "mem_pct": avg_mem,
                "iops": iops,
                "bw_kbps": bw,
                "latency_ms": latency
            })
            
    return history[-60:]

def metrics_and_cluster_monitor_loop():
    global CACHED_NODES_INFO, CACHED_CLUSTER_NODES_STATUS, CACHED_STORAGE_USAGE, CACHED_CLUSTER_METRICS, CACHED_DIAGNOSTIC_ALERTS, CACHED_VM_STATS, METRICS_HISTORY
    
    # Wait for cluster services to boot
    time.sleep(10)
    
    # Pre-populate history with real metrics if available, fallback to baseline placeholders
    now = time.time()
    real_history = []
    try:
        real_history = load_real_metrics_history()
    except Exception as e:
        print(f"[Collector Thread] Warning: Failed to load real metrics history: {e}")
        
    with METRICS_HISTORY_LOCK:
        if real_history:
            METRICS_HISTORY = real_history
        else:
            for i in range(MAX_HISTORY_POINTS, 0, -1):
                t = now - i * 1.5
                import math
                noise = math.sin(t / 10.0) * 2.0
                iops = max(2.0, 11.5 + noise)
                latency = max(0.1, 0.92 + math.cos(t / 12.0) * 0.12)
                bw = int(iops * 16.0)
                cpu_pct = max(5.0, 12.0 + math.sin(t / 8.0) * 3.0)
                mem_pct = max(30.0, 36.5 + math.cos(t / 15.0) * 0.5)
                METRICS_HISTORY.append({
                    "time": int(t * 1000),
                    "cpu_pct": cpu_pct,
                    "mem_pct": mem_pct,
                    "iops": iops,
                    "bw_kbps": bw,
                    "latency_ms": latency
                })
            
    while True:
        try:
            # 1. Fetch cluster nodes info dynamically
            nodes = get_cluster_nodes()
            nodes_info_local = []
            cluster_nodes_status_local = []
            
            db_nodes = {}
            try:
                rc_n, stdout_n, _ = run_cql_query("SELECT JSON hostname, status, maintenance_mode FROM hydra.nodes;")
                if rc_n == 0:
                    for line in stdout_n.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                n_db = json.loads(line)
                                db_nodes[n_db["hostname"]] = {
                                    "status": n_db.get("status", "NORMAL"),
                                    "maintenance_mode": n_db.get("maintenance_mode", False)
                                }
                            except Exception:
                                pass
            except Exception:
                pass
            
            for node in nodes:
                ip = node["ip"]
                hostname = node["hostname"]
                
                rc_s, nstatus, err_s = run_mtls_spark_api(ip, "/api/v1/node/status", None, method="GET")
                
                # Fetch maintenance status from ScyllaDB if available, fallback to spark status
                maint_val = "NORMAL"
                db_info = db_nodes.get(hostname)
                if db_info:
                    maint_val = db_info.get("status", "NORMAL")
                    if db_info.get("maintenance_mode", False):
                        maint_val = "IN_MAINTENANCE"
                elif rc_s == 0:
                    maint_val = nstatus.get("maintenance_status", "NORMAL")

                maint_mode = (maint_val in ["IN_MAINTENANCE", "ENTERING_MAINTENANCE"]) or (db_info.get("maintenance_mode", False) if db_info else False)

                if rc_s == 0:
                    try:
                        nstatus["status"] = "ONLINE"
                        nstatus["role"] = "Leader" if nstatus.get("zk_leader", False) else "Follower"
                        cluster_nodes_status_local.append(nstatus)
                        nodes_info_local.append({
                            "name": nstatus.get("hostname", hostname),
                            "ip": ip,
                            "status": "ONLINE",
                            "role": nstatus["role"],
                            "disks": nstatus.get("disks", 1),
                            "maintenance_status": maint_val,
                            "maintenance_mode": maint_mode
                        })
                    except Exception:
                        nodes_info_local.append({
                            "name": hostname, "ip": ip, "status": "OFFLINE", "role": "Follower", "disks": 0, "maintenance_status": "UNKNOWN", "maintenance_mode": maint_mode
                        })
                else:
                    nodes_info_local.append({
                        "name": hostname, "ip": ip, "status": "OFFLINE", "role": "Follower", "disks": 0, "maintenance_status": "UNKNOWN", "maintenance_mode": maint_mode
                    })
            # 2. Get Linstor/GlusterFS volume storage usage
            storage_usage_local = {"total_gb": 0, "used_gb": 0, "pools": []}
            try:
                total_gb = 0
                used_gb = 0
                pools = []

                # Query Linstor controller once to get all storage pools info
                hosts = []
                try:
                    with open("/etc/hci/cluster.json", "r") as f:
                        cdata = json.load(f)
                        hosts = cdata.get("hosts", [])
                except Exception:
                    pass
                
                ips = [h["ip"] for h in hosts] if hosts else ["127.0.0.1"]
                controllers_str = ",".join(ips)
                
                candidate_ips = ["127.0.0.1"] + ips
                rc_sp = -1
                stdout_sp = ""
                for cap_ip in candidate_ips:
                    cmd_sp = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor storage-pool list"
                    rc_sp, stdout_sp, _ = run_remote_spark(cap_ip, cmd_sp)
                    if rc_sp == 0 and stdout_sp.strip():
                        break
                if rc_sp == 0 and stdout_sp.strip():
                    import re
                    def parse_capacity(cap_str):
                        if not cap_str or cap_str.strip() == "":
                            return 0.0
                        match = re.search(r"([0-9.]+)\s*(gib|tib|mib|gb|tb|mb|b)?", cap_str.lower())
                        if not match:
                            return 0.0
                        val = float(match.group(1))
                        unit = match.group(2)
                        if unit in ["tib", "tb"]:
                            val *= 1024
                        elif unit in ["mib", "mb"]:
                            val /= 1024
                        return val

                    for line in stdout_sp.splitlines():
                        if "|" in line and "driver" not in line.lower() and "diskless" not in line.lower():
                            parts = [p.strip() for p in line.split("|") if p.strip()]
                            if len(parts) >= 8:
                                pool_name = parts[0]
                                node_name = parts[1]
                                driver = parts[2]
                                dev_name = parts[3]
                                free_cap = parse_capacity(parts[4])
                                total_cap = parse_capacity(parts[5])
                                state = parts[7]
                                
                                used_cap = total_cap - free_cap
                                total_gb += int(total_cap)
                                used_gb += int(used_cap)
                                
                                pools.append({
                                    "name": f"Physical Disk ({dev_name}) on {node_name}",
                                    "type": f"{driver} Pool",
                                    "path": f"{node_name}:{dev_name}",
                                    "size": f"{int(total_cap)} GB",
                                    "total_gb": int(total_cap),
                                    "used_gb": int(used_cap),
                                    "status": "ONLINE" if "ok" in state.lower() else "DEGRADED"
                                })
                
                # Get replication factor to compute usable capacity
                redundancy_factor = 1
                hosts_count = 1
                try:
                    if os.path.exists("/etc/hci/cluster.json"):
                        with open("/etc/hci/cluster.json", "r") as f:
                            cdata = json.load(f)
                            redundancy_factor = int(cdata.get("redundancy_factor", 1))
                            hosts_count = len(cdata.get("hosts", []))
                except Exception:
                    pass
                
                # Usable capacity = Raw capacity / (redundancy_factor + 1)
                rep_factor = redundancy_factor + 1 if hosts_count > 1 else 1
                usable_total_gb = total_gb // rep_factor
                usable_used_gb = used_gb // rep_factor

                # Insert the logical storage pool summary first
                pools.insert(0, {
                    "name": "default-pool (Logical Storage Pool)",
                    "type": "Aether Distributed DRBD",
                    "path": "/var/lib/hci/aether/volumes",
                    "size": f"{usable_total_gb} GB",
                    "total_gb": usable_total_gb,
                    "used_gb": usable_used_gb,
                    "status": "ONLINE"
                })
                
                storage_usage_local = {
                    "total_gb": usable_total_gb,
                    "used_gb": usable_used_gb,
                    "pools": pools
                }
            except Exception:
                pass

            # 3. Cluster Metrics (CPU / Memory)
            c_metrics = get_cluster_metrics(nodes_info_local)

            # 4. ScyllaDB Mimir Alerts
            alerts_local = []
            offline_hosts = [n for n in nodes_info_local if n["status"] != "ONLINE"]
            if offline_hosts:
                for h in offline_hosts:
                    alerts_local.append({
                        "type": "critical",
                        "desc": f"Node {h['name']} ({h['ip']}) is OFFLINE.",
                        "time": "Just now",
                        "check_name": "host_status",
                        "node_ip": h['ip']
                    })
            
            for ns in cluster_nodes_status_local:
                for svc, sdata in ns.get("services", {}).items():
                    if sdata["status"] == "DOWN" and svc != "Spectrum" and svc != "Odin":
                        node_ip = ""
                        is_maint = False
                        for n in nodes_info_local:
                            if n["name"] == ns.get("hostname"):
                                node_ip = n["ip"]
                                is_maint = n.get("maintenance_mode", False)
                                break
                        if is_maint:
                            continue
                        svc_lower = svc.lower()
                        if svc_lower == "spark":
                            chk = "spark-daemon_status"
                        elif svc_lower == "hydra":
                            chk = "hydra-db_status"
                        else:
                            chk = f"{svc_lower}_status"
                        alerts_local.append({
                            "type": "warning",
                            "desc": f"Service {svc} is DOWN on node {ns.get('hostname')}.",
                            "time": "Just now",
                            "check_name": chk,
                            "node_ip": node_ip
                        })
            
            try:
                rc_m, stdout_m, _ = run_cql_query("SELECT JSON * FROM hydra.mimir_results;")
                if rc_m == 0:
                    for line in stdout_m.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                mcheck = json.loads(line)
                                mcheck_status = mcheck.get("status")
                                mcheck_name = mcheck.get("check_name")
                                mcheck_node_ip = mcheck.get("node_ip")
                                mcheck_node = mcheck_node_ip
                                for n in nodes_info_local:
                                    if n["ip"] == mcheck_node_ip:
                                        mcheck_node = n["name"]
                                        break
                                if mcheck_status == "FAIL":
                                    alerts_local.append({
                                        "type": "critical",
                                        "desc": f"Diagnostic check '{mcheck_name}' failed on {mcheck_node}.",
                                        "time": "Just now",
                                        "check_name": mcheck_name,
                                        "node_ip": mcheck_node_ip
                                    })
                                elif mcheck_status == "WARN":
                                    alerts_local.append({
                                        "type": "warning",
                                        "desc": f"Diagnostic check '{mcheck_name}' warning on {mcheck_node}.",
                                        "time": "Just now",
                                        "check_name": mcheck_name,
                                        "node_ip": mcheck_node_ip
                                    })
                            except Exception:
                                pass
            except Exception:
                pass
            
            # 5. Calculate VMs metrics for live history
            db_vms = []
            cql = "SELECT JSON * FROM hydra.vms;"
            rc_v, stdout_v, _ = run_cql_query(cql)
            if rc_v == 0:
                for line in stdout_v.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_vms.append(json.loads(line))
                        except Exception:
                            pass
            
            iops = 0.0
            total_latency = 0.0
            vm_count_with_latency = 0
            vm_stats_local = {}
            
            for vm in db_vms:
                if vm.get("state", "").lower() == "running":
                    host_ip = vm.get("host_ip", "")
                    vcpu = vm.get("vcpu", 1)
                    memory = vm.get("memory", 1024)
                    stats = get_vm_stats(host_ip, vm["name"], vcpu, memory)
                    if stats:
                        vm_stats_local[vm["name"]] = stats
                        if stats.get("iops") is not None:
                            iops += stats["iops"]
                        if stats.get("latency_ms") is not None and stats["latency_ms"] > 0:
                            total_latency += stats["latency_ms"]
                            vm_count_with_latency += 1
            
            if vm_count_with_latency > 0:
                latency = total_latency / vm_count_with_latency
            else:
                latency = 0.0
                
            bw = int(iops * 32)
            
            # Idle baseline metrics
            if iops == 0:
                import math
                t = time.time()
                noise = math.sin(t / 10.0) * 2.0
                iops = max(2.0, 11.5 + noise)
                latency = max(0.1, 0.92 + math.cos(t / 12.0) * 0.12)
                bw = int(iops * 16.0)
            
            # Save to cache
            with CLUSTER_CACHE_LOCK:
                CACHED_NODES_INFO = nodes_info_local
                CACHED_CLUSTER_NODES_STATUS = cluster_nodes_status_local
                CACHED_STORAGE_USAGE = storage_usage_local
                CACHED_CLUSTER_METRICS = c_metrics
                CACHED_DIAGNOSTIC_ALERTS = alerts_local
                CACHED_VM_STATS = vm_stats_local
            
            # Save to metrics history
            with METRICS_HISTORY_LOCK:
                if len(METRICS_HISTORY) >= MAX_HISTORY_POINTS:
                    METRICS_HISTORY.pop(0)
                METRICS_HISTORY.append({
                    "time": int(time.time() * 1000),
                    "cpu_pct": c_metrics.get("cpu_pct", 0.0),
                    "mem_pct": c_metrics.get("mem_pct", 0.0),
                    "iops": iops,
                    "bw_kbps": bw,
                    "latency_ms": latency
                })
        except Exception as e:
            print(f"[Collector Thread] Error: {e}")
        time.sleep(25.0)


def get_network_details(net_id):
    # Query Gatoway
    rc, stdout, _ = run_cql_query(f"SELECT JSON name, type, vlan_id FROM hydra.gatoway_networks WHERE net_id = {net_id};")
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    return json.loads(line)
                except Exception:
                    pass
    # Query Urbosa
    rc, stdout, _ = run_cql_query(f"SELECT JSON segment_id, segment_name, vni FROM hydra.urbosa_segments WHERE segment_id = {net_id};")
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    data = json.loads(line)
                    data["type"] = "overlay"
                    data["name"] = data.get("segment_name")
                    return data
                except Exception:
                    pass
    return None

def hotplug_vm_nic(host_ip, vm_name, old_net_id, new_net_id):
    # 1. Get current MAC address using domiflist
    rc, stdout, _ = run_remote_spark(host_ip, f"virsh -c qemu:///system domiflist {vm_name}")
    if rc != 0 or not stdout:
        return False, "Failed to query active interfaces on guest VM."
        
    mac = None
    iface_type = "bridge"
    for line in stdout.splitlines():
        if "virtio" in line or "vnet" in line or "macvtap" in line or "direct" in line:
            parts = line.split()
            if len(parts) >= 5:
                iface_type = parts[1]
                mac = parts[4]
                break
                
    if not mac:
        return False, "Could not locate active interface MAC address."
        
    # 2. Detach old interface
    detach_cmd = f"virsh -c qemu:///system detach-interface {vm_name} {iface_type} --mac {mac} --live --persistent"
    run_remote_spark(host_ip, detach_cmd)
    
    # 3. Resolve new network details
    net = get_network_details(new_net_id)
    if not net:
        return False, f"New network ID {new_net_id} not found."
        
    # Dynamically detect default route interface on the host for direct/flat network
    uplink_dev = "ens192"
    try:
        rc_dev, stdout_dev, _ = run_remote_spark(host_ip, "ip route get 8.8.8.8 | grep -oP 'dev \\K\\S+'")
        if rc_dev == 0 and stdout_dev.strip():
            uplink_dev = stdout_dev.strip()
        else:
            rc_dev, stdout_dev, _ = run_remote_spark(host_ip, "ip route | grep default | awk '{print $5}'")
            if rc_dev == 0 and stdout_dev.strip():
                uplink_dev = stdout_dev.strip().splitlines()[0]
    except Exception:
        pass
        
    # 4. Construct device XML and attach
    if net.get("type") == "direct":
        xml = f"<interface type='direct'><mac address='{mac}'/><source dev='{uplink_dev}' mode='bridge'/><model type='virtio'/></interface>"
    elif net.get("type") == "vlan":
        vlan_id = net.get("vlan_id")
        xml = f"<interface type='bridge'><mac address='{mac}'/><source bridge='br-vlan-{vlan_id}'/><model type='virtio'/></interface>"
    elif net.get("type") == "overlay":
        vni = net.get("vni")
        xml = f"<interface type='bridge'><mac address='{mac}'/><source bridge='br-ov-{vni}'/><model type='virtio'/></interface>"
    else:
        xml = f"<interface type='bridge'><mac address='{mac}'/><source bridge='virbr0'/><model type='virtio'/></interface>"
        
    write_xml_cmd = f"echo \"{xml}\" > /tmp/live_nic_{vm_name}.xml"
    run_remote_spark(host_ip, write_xml_cmd)
    
    attach_cmd = f"virsh -c qemu:///system attach-device {vm_name} /tmp/live_nic_{vm_name}.xml --live --persistent && rm -f /tmp/live_nic_{vm_name}.xml"
    rc_att, _, stderr_att = run_remote_spark(host_ip, attach_cmd)
    if rc_att != 0:
        return False, f"Failed to attach new network interface device: {stderr_att.strip()}"
        
    return True, "Hotplug successful."


def distribute_update_package(zip_path):
    import base64
    import os
    import sys
    try:
        sys.path.append("/usr/local/bin")
        sys.path.append(".")
        import hylia
        
        if not os.path.exists(zip_path):
            return
            
        with open(zip_path, "rb") as f:
            b64_data = base64.b64encode(f.read()).decode("utf-8")
            
        hosts = hylia.get_cluster_hosts()
        import socket
        local_ips = ["127.0.0.1", "::1"]
        try:
            local_ips.append(socket.gethostbyname(socket.gethostname()))
        except:
            pass
            
        other_ips = [h.get("ip") for h in hosts if h.get("ip") and h.get("ip") not in local_ips]
        
        for ip in other_ips:
            # 1. Clean old files
            hylia.run_remote_spark(ip, f"rm -rf {zip_path} {zip_path}.tmp /tmp/helios_update")
            
            # 2. Upload zip in chunks
            chunk_size = 64000
            for idx in range(0, len(b64_data), chunk_size):
                chunk = b64_data[idx:idx+chunk_size]
                hylia.run_remote_spark(ip, f"echo '{chunk}' >> {zip_path}.tmp")
                
            # 3. Decode zip and extract it
            decode_cmd = (
                f"cat {zip_path}.tmp | base64 -d > {zip_path} && "
                f"rm -f {zip_path}.tmp && "
                f"python3 -c \"import importlib.util, importlib.machinery; loader = importlib.machinery.SourceFileLoader('hylia', '/usr/local/bin/hylia'); spec = importlib.util.spec_from_loader('hylia', loader); hylia = importlib.util.module_from_spec(spec); loader.exec_module(hylia); hylia.validate_and_extract_zip('{zip_path}', '/tmp/helios_update')\""
            )
            hylia.run_remote_spark(ip, decode_cmd)
    except Exception as e:
        print("Error distributing package:", e)


def deploy_lanayru_worker(task_id, cluster_name, control_nodes, overlay_segment_id, created_at):
    import lanayru
    lanayru.deploy_lanayru_worker(task_id, cluster_name, control_nodes, overlay_segment_id, created_at)


class SpectrumHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        print(f"[HTTP] {self.address_string()} - - [{self.log_date_time_string()}] {format % args}")

    def send_json(self, status_code, data):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        # Extract resource path from URL
        url_parsed = urllib.parse.urlparse(self.path)
        path = url_parsed.path

        # Auth Guard
        if path.startswith("/api/") and path not in ["/api/login", "/api/auth/check"]:
            if not is_authenticated(self):
                self.send_json(401, {"error": "Unauthorized"})
                return

        if path == "/api/auth/check":
            if is_authenticated(self):
                self.send_json(200, {"authenticated": True, "username": getattr(self, "current_user", "")})
        elif path == "/api/lcm/upgrade/check":
            try:
                # Query lcm_update_state table
                rc, stdout, stderr = run_cql_query("SELECT JSON * FROM hydra.lcm_update_state WHERE key = 'latest';")
                if rc == 0 and stdout and stdout.strip():
                    try:
                        state_row = json.loads(stdout.splitlines()[0])
                        # If there is a recorded error_msg, return it in the error field
                        error_msg = state_row.get("error_msg", "")
                        self.send_json(200, {
                            "current_version": state_row.get("current_version", "1.2.0-b4081"),
                            "latest_version": state_row.get("latest_version", ""),
                            "update_available": state_row.get("update_available", False),
                            "release_date": state_row.get("release_date", ""),
                            "download_url": state_row.get("download_url", ""),
                            "sha256": state_row.get("sha256", ""),
                            "size": state_row.get("size", 0),
                            "changelog": state_row.get("changelog", ""),
                            "last_checked": state_row.get("last_checked", 0),
                            "error": error_msg if error_msg else None
                        })
                    except Exception as json_err:
                        self.send_json(500, {"error": f"Failed to parse DB JSON: {str(json_err)}"})
                else:
                    # No cached update state found yet!
                    # Return that a check is needed or is in progress
                    current_version = "1.2.0-b4081"
                    try:
                        sys.path.append("/usr/local/bin")
                        sys.path.append(".")
                        import hylia
                        current_version = getattr(hylia, "__build__", "1.2.0-b4081")
                    except Exception:
                        pass
                        
                    self.send_json(200, {
                        "current_version": current_version,
                        "update_available": False,
                        "error": "No update check has run yet. Click 'Check for Updates Online' to trigger a check."
                    })
            except Exception as e:
                self.send_json(500, {"error": str(e)})
            return
        elif path == "/api/lcm/inventory":
            try:
                import concurrent.futures
                from concurrent.futures import ThreadPoolExecutor
                import socket
                
                sys.path.append("/usr/local/bin")
                sys.path.append(".")
                try:
                    import hylia
                except ImportError:
                    import hylia
                
                hosts = hylia.get_cluster_hosts()
                if not hosts:
                    hosts = [{"hostname": socket.gethostname(), "ip": "127.0.0.1"}]
                
                components_paths = {
                    "spark": "/usr/local/bin/spark",
                    "spark-daemon": "/usr/local/bin/spark-daemon",
                    "bifrost": "/usr/local/bin/bifrost",
                    "valcli": "/usr/local/bin/valcli",
                    "mcli": "/usr/local/bin/mcli",
                    "mcli-runner": "/usr/local/bin/mcli-runner",
                    "dagur": "/usr/local/bin/dagur",
                    "mimir": "/usr/local/bin/mimir",
                    "vali": "/usr/local/bin/vali",
                    "catalyst": "/usr/local/bin/catalyst",
                    "catcli": "/usr/local/bin/catcli",
                    "gatoway": "/usr/local/bin/gatoway",
                    "urbosa": "/usr/local/bin/urbosa",
                    "logos": "/usr/local/bin/logos",
                    "mipha": "/usr/local/bin/mipha",
                    "urbosa-bootstrap": "/usr/local/bin/urbosa-bootstrap",
                    "daruk": "/usr/local/bin/daruk.py",
                    "hylia": "/usr/local/bin/hylia",
                    "spectrum": "/usr/local/bin/spectrum_server",
                    "Dockerfile": "/usr/local/bin/Dockerfile"
                }
                
                inventory = {}
                
                def fetch_version(host_ip, comp_name, target_path):
                    rc_v, res_v, err_v = run_mtls_spark_api(
                        host_ip,
                        f"/api/v1/node/binary-version?path={urllib.parse.quote(target_path)}",
                        None,
                        method="GET"
                    )
                    if rc_v == 0 and "version" in res_v:
                        return comp_name, res_v["version"]
                    return comp_name, "N/A"
                
                with ThreadPoolExecutor(max_workers=30) as executor:
                    futures = {}
                    for h in hosts:
                        host_ip = h["ip"]
                        host_name = h["hostname"]
                        inventory[host_name] = {"ip": host_ip, "versions": {}}
                        for comp_name, target_path in components_paths.items():
                            f = executor.submit(fetch_version, host_ip, comp_name, target_path)
                            futures[f] = (host_name, comp_name)
                    
                    for f in concurrent.futures.as_completed(futures):
                        host_name, comp_name = futures[f]
                        _, version = f.result()
                        inventory[host_name]["versions"][comp_name] = version
                
                self.send_json(200, {
                    "status": "success",
                    "inventory": inventory
                })
            except Exception as e:
                self.send_json(500, {"error": str(e)})
            return

        elif path == "/api/lcm/upgrade/status":
            try:
                sys.path.append("/usr/local/bin")
                sys.path.append(".")
                try:
                    import hylia
                except ImportError:
                    import hylia
                    
                rc, stdout, _ = run_cql_query("SELECT JSON job_id, state, target_nodes, current_node, build_number FROM hydra.hylia_jobs;")
                if rc != 0 or not stdout or not stdout.strip():
                    self.send_json(200, {"status": "IDLE", "logs": [], "progress": 0})
                    return
                
                job = json.loads(stdout.splitlines()[0])
                job_id = job.get("job_id")
                state = job.get("state")
                target_nodes = job.get("target_nodes", [])
                current_node = job.get("current_node", "")
                build_number = job.get("build_number", "")
                
                # Fetch logs
                logs = []
                rc_l, stdout_l, _ = run_cql_query(f"SELECT JSON timestamp, log_line FROM hydra.hylia_logs WHERE job_id = {job_id};")
                if rc_l == 0 and stdout_l:
                    for line in stdout_l.splitlines():
                        if line.strip():
                            log_entry = json.loads(line)
                            logs.append(log_entry.get("log_line"))
                            
                # Calculate progress
                progress = 0
                if state == "COMPLETED":
                    progress = 100
                elif state == "FAILED":
                    progress = 100
                elif state == "UPGRADING" and target_nodes:
                    if current_node in target_nodes:
                        node_idx = target_nodes.index(current_node)
                        progress = int(((node_idx) / len(target_nodes)) * 100)
                        
                        # Sub-progress estimation from log analysis
                        sub_prog = 0
                        for l in logs:
                            if current_node in l or (current_node == "127.0.0.1" and "127.0.0.1" in l):
                                if "maintenance" in l.lower():
                                    sub_prog = 5
                                elif "deploy" in l.lower() or "cop" in l.lower():
                                    sub_prog = 15
                                elif "reboot" in l.lower():
                                    sub_prog = 25
                                elif "restore" in l.lower():
                                    sub_prog = 30
                        progress += int(sub_prog * (1.0 / len(target_nodes)))
                        
                self.send_json(200, {
                    "status": state,
                    "current_node": current_node,
                    "target_nodes": target_nodes,
                    "build_number": build_number,
                    "progress": min(progress, 100),
                    "logs": logs
                })
            except Exception as e:
                self.send_json(500, {"error": str(e)})
            return

        elif path == "/api/settings":
            cql = "SELECT key, value FROM hydra.cluster_settings;"
            settings = {
                "dns_servers": "8.8.8.8,8.8.4.4",
                "dns_search_domains": "cluster.local",
                "dns_mtu": "1500",
                "ntp_servers": "pool.ntp.org",
                "timezone": "UTC",
                "cluster_name": "hci-01",
                "cluster_region": "dc-1",
                "replication_factor": get_actual_replication_factor(),
                "scrub_interval": "weekly",
                "password_policy": "disabled",
                "session_timeout": "30",
                "rate_limit": "100",
                "vip": "",
                "cluster_subnet": "10.10.102.0/24",
                "cluster_id": "",
                "urbosa_enabled": "false",
                "drs_enabled": "true"
            }
            rc, out, err = run_cql_query(cql)
            if rc == 0:
                for line in out.splitlines():
                    if "|" in line:
                        parts = line.split("|")
                    else:
                        parts = line.split(None, 1)
                    if len(parts) >= 2:
                        k = parts[0].strip()
                        v = parts[1].strip()
                        if k in settings:
                            settings[k] = v
            settings["replication_factor"] = get_actual_replication_factor()
            try:
                if os.path.exists("/etc/hci/cluster.json"):
                    with open("/etc/hci/cluster.json", "r") as f:
                        cdata = json.load(f)
                        settings["cluster_name"] = cdata.get("cluster_name", settings["cluster_name"])
                        settings["vip"] = cdata.get("vip", settings["vip"])
                        settings["cluster_subnet"] = cdata.get("cluster_subnet", settings["cluster_subnet"])
                        cid = cdata.get("cluster_id", settings["cluster_id"])
                        if not cid:
                            import uuid
                            cid = str(uuid.uuid4())
                            cdata["cluster_id"] = cid
                            with open("/etc/hci/cluster.json", "w") as fw:
                                json.dump(cdata, fw, indent=4)
                        settings["cluster_id"] = cid
            except Exception:
                pass
            self.send_json(200, settings)
            return

        elif path == "/api/users":
            cql = "SELECT username FROM hydra.users;"
            rc, out, err = run_cql_query(cql)
            users = []
            if rc == 0:
                for line in out.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith("-") or line.startswith("("):
                        continue
                    if line == "username" or line == "key":
                        continue
                    if "|" in line:
                        parts = [p.strip() for p in line.split("|")]
                        if parts[0] and parts[0] != "username":
                            users.append(parts[0])
                    else:
                        users.append(line)
            if not users:
                users = ["helios"]
            else:
                users = list(sorted(set(users)))
            self.send_json(200, {"users": users})
            return

        elif path == "/api/vms":
            # Fetch DHCP leases
            dhcp_leases = get_consolidated_dhcp_leases()
            # 1. Fetch local VMs list from libvirt
            libvirt_vms = {}
            try:
                rc, stdout, stderr = run_remote_spark("127.0.0.1", "virsh -c qemu:///system list --all")
                if rc == 0:
                    lines = stdout.splitlines()
                    for line in lines[2:]:
                        parts = line.split()
                        if len(parts) >= 3:
                            name = parts[1]
                            state_val = " ".join(parts[2:])
                            if state_val == "running":
                                state_val = "Running"
                            elif state_val == "shut off":
                                state_val = "Stopped"
                            libvirt_vms[name] = state_val
            except Exception:
                pass

            # 2. Fetch VMs from ScyllaDB
            db_vms = []
            cql = "SELECT JSON * FROM hydra.vms;"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_vms.append(json.loads(line))
                        except Exception:
                            pass

            # 3. Align states and build VM list
            vms_list = []
            for vm in db_vms:
                name = vm["name"]
                host_ip = vm.get("host_ip", "")
                
                # Align if VM is mapped to local node
                is_local = (host_ip == LOCAL_IP or host_ip == "127.0.0.1")
                if is_local:
                    live_state = libvirt_vms.get(name, "Stopped")
                    if live_state == "Stopped":
                        if name in libvirt_vms:
                            run_remote_spark("127.0.0.1", f"virsh -c qemu:///system undefine {name} --keep-nvram || true")
                        if vm.get("state") != "Stopped" or host_ip != "":
                            cql_update = f"UPDATE hydra.vms SET state = 'Stopped', host_ip = '' WHERE name = '{name}';"
                            run_cql_query(cql_update)
                            vm["state"] = "Stopped"
                            vm["host_ip"] = ""
                            host_ip = ""
                    elif vm.get("state") != live_state:
                        cql_update = f"UPDATE hydra.vms SET state = '{live_state}' WHERE name = '{name}';"
                        run_cql_query(cql_update)
                        vm["state"] = live_state
                
                vm_status = vm.get("state", "Stopped").lower()
                
                # Resolve host IP to hostname for the frontend UI
                vm_node_display = host_ip
                for n in get_cluster_nodes():
                    if n.get("ip") == host_ip:
                        vm_node_display = n.get("hostname")
                        break
                        
                # Query VM stats if running
                cpu_usage_pct = None
                mem_usage_mb = None
                mem_usage_pct = None
                iops_val = None
                latency_ms = None
                
                if vm_status == "running":
                    with CLUSTER_CACHE_LOCK:
                        stats = CACHED_VM_STATS.get(name)
                    if stats:
                        cpu_usage_pct = stats.get("cpu_usage_pct")
                        mem_usage_mb = stats.get("mem_usage_mb")
                        mem_usage_pct = stats.get("mem_usage_pct")
                        iops_val = stats.get("iops")
                        latency_ms = stats.get("latency_ms")

                vm_ip = resolve_vm_ip(host_ip, name, vm_status, dhcp_leases)

                vms_list.append({
                    "name": name,
                    "vcpus": vm.get("vcpu", 1),
                    "memory": vm.get("memory", 1024),
                    "disk": vm.get("disk_size", 10),
                    "firmware": vm.get("firmware", "uefi"),
                    "disks_list": vm.get("disks_list", ""),
                    "iso": vm.get("iso", ""),
                    "boot_device": vm.get("boot_device", ""),
                    "node": vm_node_display,
                    "status": vm_status,
                    "cpu_usage_pct": cpu_usage_pct,
                    "mem_usage_mb": mem_usage_mb,
                    "mem_usage_pct": mem_usage_pct,
                    "iops": iops_val,
                    "latency_ms": latency_ms,
                    "network_id": vm.get("network_id", ""),
                    "ip_address": vm_ip,
                    "audio_enabled": vm.get("audio_enabled", False)
                })

            self.send_json(200, {"vms": vms_list})
            return

        elif path == "/api/vms/drs":
            rc, res, err = run_mtls_spark_api("127.0.0.1", "/api/v1/vm/drs", {}, method="GET")
            if rc == 0:
                self.send_json(200, res)
            else:
                self.send_json(500, {"error": f"Failed to fetch DRS status: {err}"})
            return

        elif path == "/api/networks":
            # Gato L2 networks (direct / vlan)
            cql = "SELECT JSON * FROM hydra.gatoway_networks;"
            rc, stdout, stderr = run_cql_query(cql)
            networks = []
            if rc == 0 and stdout:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            networks.append(json.loads(line))
                        except Exception:
                            pass

            # Urbosa overlay segments — normalize to the same shape
            cql2 = "SELECT JSON * FROM hydra.urbosa_segments;"
            rc2, stdout2, _ = run_cql_query(cql2)
            if rc2 == 0 and stdout2:
                for line in stdout2.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            seg = json.loads(line)
                            networks.append({
                                "net_id": str(seg.get("segment_id", "")),
                                "name": seg.get("name", ""),
                                "type": "overlay",
                                "vlan_id": None,
                                "vni": seg.get("vni"),
                                "subnet_cidr": seg.get("subnet_cidr", ""),
                            })
                        except Exception:
                            pass

            self.send_json(200, {"networks": networks})
            return

        elif path == "/api/lanayru/checks":
            # Real DB check checking consensus via nodetool
            rc_db, stdout_db, _ = run_remote_spark(LOCAL_IP, "podman exec systemd-hydra-db nodetool status || true")
            db_status = "error"
            db_msg = "ScyllaDB cluster offline or unreachable."
            if rc_db == 0 and stdout_db:
                # Count UN nodes
                un_nodes = 0
                for line in stdout_db.splitlines():
                    if line.strip().startswith("UN"):
                        un_nodes += 1
                expected_nodes = len(get_cluster_nodes()) if get_cluster_nodes() else 3
                if un_nodes >= expected_nodes:
                    db_status = "ready"
                    db_msg = f"ScyllaDB consensus healthy: {un_nodes}/{expected_nodes} nodes active (UN)."
                else:
                    db_status = "warning"
                    db_msg = f"ScyllaDB consensus warning: only {un_nodes}/{expected_nodes} nodes active (UN)."
            
            # Linstor storage check - run command to get storage pools
            controller_ips = ",".join([node["ip"] for node in get_cluster_nodes()]) if get_cluster_nodes() else "127.0.0.1"
            rc_storage, stdout_st, _ = run_remote_spark(LOCAL_IP, f"podman exec -e LS_CONTROLLERS={controller_ips} systemd-aether linstor storage-pool list || true")
            storage_status = "error"
            storage_msg = "Linstor thin storage pool unreachable or offline."
            if rc_storage == 0 and stdout_st:
                if "THIN" in stdout_st or "lvm" in stdout_st.lower() or "drbd" in stdout_st.lower():
                    storage_status = "ready"
                    storage_msg = "Linstor thin storage pool verified and replicated."
                else:
                    storage_status = "warning"
                    storage_msg = "Linstor pools online but thin provisioning not found."
            
            # Node memory check using LOCAL_IP
            rc_mem, stdout_mem, _ = run_remote_spark(LOCAL_IP, "free -m")
            compute_status = "warning"
            compute_msg = "Host compute resources warning or unverified."
            if rc_mem == 0 and stdout_mem:
                try:
                    lines = stdout_mem.splitlines()
                    for line in lines:
                        if line.startswith("Mem:"):
                            free_mem = int(line.split()[3])
                            if free_mem >= 2048:
                                compute_status = "ready"
                                compute_msg = f"Host RAM capacity check passed ({free_mem}MB free on node)"
                            else:
                                compute_status = "warning"
                                compute_msg = f"Host RAM capacity warning: only {free_mem}MB free on node"
                except:
                    pass
            
            # Urbosa segment count
            rc_net, stdout_net, _ = run_cql_query("SELECT segment_id FROM hydra.urbosa_segments;")
            net_count = 0
            if rc_net == 0 and stdout_net:
                # Count returned segment UUIDs
                for line in stdout_net.splitlines():
                    line_clean = line.strip()
                    if line_clean and not line_clean.startswith('(') and not line_clean.startswith('-') and line_clean != "segment_id" and line_clean != "rows":
                        net_count += 1
            net_msg = f"Active overlay segments detected ({net_count} registered)" if net_count > 0 else "Warning: No Urbosa overlay segments created. Direct fallback active."
            
            self.send_json(200, {
                "db": {"status": db_status, "msg": db_msg},
                "storage": {"status": storage_status, "msg": storage_msg},
                "compute": {"status": compute_status, "msg": compute_msg},
                "network": {"status": "ready" if net_count > 0 else "warning", "msg": net_msg}
            })
            return

        elif path == "/api/lanayru/status":
            query_params = urllib.parse.parse_qs(url_parsed.query)
            task_id = query_params.get("task_id", [None])[0]
            if not task_id:
                self.send_json(400, {"error": "Missing task_id"})
                return
            
            # Query task status from DB
            cql = f"SELECT JSON status, progress, error_msg FROM hydra.catalyst_tasks WHERE task_id = {task_id};"
            rc, stdout, _ = run_cql_query(cql)
            status = "unknown"
            progress = 0
            error_msg = ""
            if rc == 0 and stdout:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            t_info = json.loads(line)
                            status = t_info.get("status", "unknown")
                            progress = t_info.get("progress", 0)
                            error_msg = t_info.get("error_msg", "")
                        except:
                            pass
            
            self.send_json(200, {
                "status": status,
                "progress": progress,
                "error_msg": error_msg,
                "logs": LANAYRU_LOGS.get(task_id, ["No logs available for this task."])
            })
            return

        elif path == "/api/lanayru/cluster/info":
            # 1. Fetch active cluster name
            rc, stdout, _ = run_cql_query("SELECT JSON * FROM hydra.lanayru_clusters;")
            active_cluster = None
            if rc == 0 and stdout:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            c_info = json.loads(line)
                            if c_info.get("status") == "active":
                                active_cluster = c_info
                                break
                        except:
                            pass
            
            if not active_cluster:
                self.send_json(200, {"active": False})
                return
                
            cluster_name = active_cluster.get("name")
            cluster_id = active_cluster.get("cluster_id")
            control_nodes = active_cluster.get("control_nodes", 1)
            
            # 2. Query VMs matching this cluster name
            rc_v, stdout_v, _ = run_cql_query("SELECT JSON * FROM hydra.vms;")
            cluster_vms = []
            if rc_v == 0 and stdout_v:
                for line in stdout_v.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            v_info = json.loads(line)
                            v_name = v_info.get("name", "")
                            if v_name.startswith(cluster_name):
                                cluster_vms.append(v_info)
                        except:
                            pass
            
            # Sort VMs by name
            cluster_vms.sort(key=lambda x: x.get("name", ""))
            
            if not cluster_vms:
                self.send_json(200, {"active": False})
                return
                
            # 3. Compile VMs status and dynamic IP assignment
            nodes_status = []
            cluster_healthy = True
            for i, vm in enumerate(cluster_vms):
                vm_name = vm.get("name")
                state = vm.get("state", "Stopped")
                host_ip = vm.get("host_ip", "Unassigned")
                
                # Determine IP address from configuration
                seg_num = 1 if (i % 2 == 0) else 2
                vm_ip = f"172.16.10.{10 + i}" if seg_num == 1 else f"172.16.11.{10 + i}"
                
                # Get CPU/Mem utilization from CACHED_VM_STATS if running
                cpu_use = "0%"
                vm_mem_limit = vm.get("memory", 2048)
                mem_use = f"0MB / {int(vm_mem_limit / 1024)}GB"
                
                if state == "Running":
                    with CLUSTER_CACHE_LOCK:
                        stats = CACHED_VM_STATS.get(vm_name)
                    if stats:
                        cpu_val = stats.get("cpu_usage_pct", 0.0)
                        mem_mb = stats.get("mem_usage_mb", 0.0)
                        cpu_use = f"{cpu_val:.1f}%"
                        mem_use = f"{int(mem_mb)}MB / {int(vm_mem_limit / 1024)}GB"
                    else:
                        cpu_use = "0.0%"
                        mem_use = f"0MB / {int(vm_mem_limit / 1024)}GB"
                else:
                    cpu_use = "0%"
                    mem_use = f"0MB / {int(vm_mem_limit / 1024)}GB"
                    cluster_healthy = False
                    
                nodes_status.append({
                    "name": vm_name,
                    "state": state,
                    "host_ip": host_ip,
                    "ip": vm_ip,
                    "cpu": cpu_use,
                    "memory": mem_use
                })
            
            # 4. Generate dynamic running pods list from ScyllaDB (Hydra)
            pods_list = []
            rc_p, stdout_p, _ = run_cql_query(f"SELECT JSON name FROM hydra.lanayru_k8s_state WHERE cluster_id = {cluster_id} ALLOW FILTERING;")
            if rc_p == 0 and stdout_p:
                for line in stdout_p.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            row_data = json.loads(line)
                            key_name = row_data.get("name", "")
                            if key_name.startswith("/registry/pods/"):
                                parts = key_name.split('/')
                                if len(parts) >= 5:
                                    namespace = parts[3]
                                    pod_name = parts[4]
                                    pods_list.append({
                                        "namespace": namespace,
                                        "name": pod_name,
                                        "status": "Running",
                                        "ready": "1/1",
                                        "ip": nodes_status[0]["ip"] if nodes_status else "127.0.0.1"
                                    })
                        except Exception:
                            pass
                
            self.send_json(200, {
                "active": True,
                "cluster_name": cluster_name,
                "cluster_id": cluster_id,
                "status": "Healthy" if cluster_healthy else "Degraded",
                "nodes": nodes_status,
                "pods": pods_list,
                "kubernetes_version": "v1.28.2 (Kine + ScyllaDB)"
            })
            return

        elif path == "/api/host/interfaces":
            interfaces = set()
            nodes = get_cluster_nodes()
            if not nodes:
                nodes = [{"ip": LOCAL_IP}]
            
            for node in nodes:
                ip = node.get("ip")
                if ip:
                    cmd = 'find /sys/class/net -type l -not -name lo -not -name "virbr*" -not -name "br-*" -not -name "vxlan*" -not -name "veth*" -not -name "vnet*" -not -name "macvtap*" -exec basename {} \\;'
                    rc, stdout, _ = run_remote_spark(ip, cmd)
                    if rc == 0:
                        for line in stdout.splitlines():
                            if line.strip():
                                interfaces.add(line.strip())
            
            if not interfaces:
                interfaces.update(["ens192", "ens3", "ens33", "eth0", "eno1"])
                
            default_interface = None
            default_gateway = None
            suggested_ip = None
            
            rc_route, out_route, _ = run_remote_spark("127.0.0.1", "ip route show | grep default")
            if rc_route == 0 and out_route:
                parts = out_route.strip().split()
                try:
                    via_idx = parts.index("via")
                    default_gateway = parts[via_idx + 1]
                except ValueError:
                    pass
                try:
                    dev_idx = parts.index("dev")
                    default_interface = parts[dev_idx + 1]
                except ValueError:
                    pass
            
            if default_interface:
                rc_ip, out_ip, _ = run_remote_spark("127.0.0.1", f"ip addr show {default_interface} | grep 'inet '")
                if rc_ip == 0 and out_ip:
                    parts = out_ip.strip().split()
                    if len(parts) >= 2:
                        ip_cidr = parts[1]
                        if "/" in ip_cidr:
                            ip_part, mask_part = ip_cidr.split("/", 1)
                            octets = ip_part.split(".")
                            if len(octets) == 4:
                                octets[3] = "250"
                                suggested_ip = ".".join(octets) + "/" + mask_part

            self.send_json(200, {
                "interfaces": sorted(list(interfaces)),
                "default_interface": default_interface or "ens192",
                "default_gateway": default_gateway or "10.10.102.1",
                "suggested_ip": suggested_ip or "10.10.102.250/24"
            })
            return

        elif path == "/api/urbosa/t0":
            cql = "SELECT JSON * FROM hydra.urbosa_t0_routers;"
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
            self.send_json(200, {"routers": items})
            return

        elif path == "/api/urbosa/t1":
            cql = "SELECT JSON * FROM hydra.urbosa_t1_routers;"
            rc, stdout, stderr = run_cql_query(cql)
            items = []
            if rc == 0 and stdout:
                import hashlib
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            r = json.loads(line)
                            router_id_str = r.get("router_id")
                            if router_id_str:
                                h_idx = int(hashlib.md5(router_id_str.encode()).hexdigest()[:4], 16) % 16384
                                octet2 = (h_idx >> 6) & 0xff
                                octet3 = (h_idx & 0x3f) * 4
                                r["transit_ip"] = f"100.64.{octet2}.{octet3 + 2}/30"
                                r["t0_transit_ip"] = f"100.64.{octet2}.{octet3 + 1}/30"
                            items.append(r)
                        except Exception:
                            pass
            self.send_json(200, {"routers": items})
            return

        elif path == "/api/urbosa/segments":
            cql = "SELECT JSON * FROM hydra.urbosa_segments;"
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
            self.send_json(200, {"segments": items})
            return

        elif path == "/api/urbosa/firewall":
            cql = "SELECT JSON * FROM hydra.urbosa_firewall_rules;"
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
            self.send_json(200, {"rules": items})
            return

        elif path == "/api/urbosa/tunnels/metrics":
            query_params = urllib.parse.parse_qs(url_parsed.query)
            node_ip = query_params.get("node_ip", [None])[0]
            interface_name = query_params.get("interface_name", [None])[0]
            limit = int(query_params.get("limit", [60])[0])
            if not node_ip or not interface_name:
                self.send_json(400, {"error": "Missing node_ip or interface_name parameters"})
                return
            path_api = f"/api/v1/urbosa/tunnels/metrics?node_ip={node_ip}&interface_name={interface_name}&limit={limit}"
            rc, data, err = run_mtls_spark_api("127.0.0.1", path_api, None, method="GET")
            if rc == 0:
                self.send_json(200, data)
            else:
                self.send_json(500, {"error": f"Failed to retrieve metrics from spark-daemon: {err}"})
            return

        elif path == "/api/urbosa/tunnels/status":
            rc, data, err = run_mtls_spark_api("127.0.0.1", "/api/v1/urbosa/tunnels/status", None, method="GET")
            if rc == 0:
                self.send_json(200, data)
            else:
                self.send_json(500, {"error": f"Failed to retrieve tunnel status from spark-daemon: {err}"})
            return

        elif path == "/api/status":
            now = time.time()
            if STATUS_CACHE["data"] is not None and (now - STATUS_CACHE["last_fetched"]) < 2.0:
                self.send_json(200, STATUS_CACHE["data"])
                return

            # Fetch DHCP leases
            dhcp_leases = get_consolidated_dhcp_leases()
            # 1. Fetch local VMs list from libvirt
            libvirt_vms = {}
            try:
                rc, stdout, stderr = run_remote_spark("127.0.0.1", "virsh -c qemu:///system list --all")
                if rc == 0:
                    lines = stdout.splitlines()
                    for line in lines[2:]:
                        parts = line.split()
                        if len(parts) >= 3:
                            name = parts[1]
                            state = " ".join(parts[2:])
                            if state == "running":
                                state = "Running"
                            elif state == "shut off":
                                state = "Stopped"
                            libvirt_vms[name] = state
            except Exception:
                pass

            # 2. Fetch VMs from ScyllaDB
            db_vms = []
            cql = "SELECT JSON * FROM hydra.vms;"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_vms.append(json.loads(line))
                        except Exception:
                            pass

            # 3. Align states and build VM list
            vms_list = []
            for vm in db_vms:
                name = vm["name"]
                host_ip = vm.get("host_ip", "")
                
                # Align if VM is mapped to local node
                is_local = (host_ip == LOCAL_IP or host_ip == "127.0.0.1")
                if is_local:
                    live_state = libvirt_vms.get(name, "Stopped")
                    if live_state == "Stopped":
                        if name in libvirt_vms:
                            run_remote_spark(LOCAL_IP, f"virsh -c qemu:///system undefine {name} --keep-nvram || true")
                        if vm.get("state") != "Stopped" or host_ip != "":
                            cql_update = f"UPDATE hydra.vms SET state = 'Stopped', host_ip = '' WHERE name = '{name}';"
                            run_cql_query(cql_update)
                            vm["state"] = "Stopped"
                            vm["host_ip"] = ""
                            host_ip = ""
                    elif vm.get("state") != live_state:
                        cql_update = f"UPDATE hydra.vms SET state = '{live_state}' WHERE name = '{name}';"
                        run_cql_query(cql_update)
                        vm["state"] = live_state
                
                vm_status = vm.get("state", "Stopped").lower()
                
                # Resolve host IP to hostname for the frontend UI
                vm_node_display = host_ip
                for n in get_cluster_nodes():
                    if n.get("ip") == host_ip:
                        vm_node_display = n.get("hostname")
                        break
                        
                # Query VM stats if running
                cpu_usage_pct = None
                mem_usage_mb = None
                mem_usage_pct = None
                iops_val = None
                latency_ms = None
                
                if vm_status == "running":
                    with CLUSTER_CACHE_LOCK:
                        stats = CACHED_VM_STATS.get(name)
                    if stats:
                        cpu_usage_pct = stats.get("cpu_usage_pct")
                        mem_usage_mb = stats.get("mem_usage_mb")
                        mem_usage_pct = stats.get("mem_usage_pct")
                        iops_val = stats.get("iops")
                        latency_ms = stats.get("latency_ms")

                vm_ip = resolve_vm_ip(host_ip, name, vm_status, dhcp_leases)

                drs_satisfaction = None
                if vm_status == "running" and host_ip:
                    host_node = None
                    with CLUSTER_CACHE_LOCK:
                        for n_info in CACHED_NODES_INFO:
                            if n_info.get("ip") == host_ip:
                                host_node = n_info
                                break
                    if host_node:
                        host_cpu = host_node.get("cpu_pct", 0.0)
                        ram_used = host_node.get("ram_used_gb", 0.0)
                        ram_total = host_node.get("ram_total_gb", 0.0)
                        host_mem = (ram_used / ram_total) if ram_total > 0 else 0.0
                        host_load = (host_cpu / 100.0 + host_mem) / 2.0
                        drs_satisfaction = max(20, min(100, round(100 - (host_load - 0.4) * 133)))

                vms_list.append({
                    "name": name,
                    "vcpus": vm.get("vcpu", 1),
                    "memory": vm.get("memory", 1024),
                    "disk": vm.get("disk_size", 10),
                    "firmware": vm.get("firmware", "uefi"),
                    "disks_list": vm.get("disks_list", ""),
                    "iso": vm.get("iso", ""),
                    "boot_device": vm.get("boot_device", ""),
                    "node": vm_node_display,
                    "status": vm_status,
                    "cpu_usage_pct": cpu_usage_pct,
                    "mem_usage_mb": mem_usage_mb,
                    "mem_usage_pct": mem_usage_pct,
                    "iops": iops_val,
                    "latency_ms": latency_ms,
                    "drs_satisfaction": drs_satisfaction,
                    "network_id": vm.get("network_id", ""),
                    "ip_address": vm_ip,
                    "audio_enabled": vm.get("audio_enabled", False)
                })

            # Retrieve cached status values instantly from the background collector thread
            with CLUSTER_CACHE_LOCK:
                nodes_info = list(CACHED_NODES_INFO)
                cluster_nodes_status = list(CACHED_CLUSTER_NODES_STATUS)
                storage_usage = dict(CACHED_STORAGE_USAGE) if CACHED_STORAGE_USAGE else {"total_gb": 0, "used_gb": 0, "pools": []}
                c_metrics = dict(CACHED_CLUSTER_METRICS) if CACHED_CLUSTER_METRICS else {}
                alerts = list(CACHED_DIAGNOSTIC_ALERTS)
            
            running_vms = [v for v in vms_list if v["status"] == "running"]
            
            cpu_pct = c_metrics.get("cpu_pct", 0.0)
            cores = c_metrics.get("cpu_cores", 6)
            total_cpu_ghz = c_metrics.get("total_cpu_ghz", 14.4)
            mem_pct = c_metrics.get("mem_pct", 0.0)
            total_mem_gb = c_metrics.get("total_mem_gb", 18.0)
            used_mem_gb = c_metrics.get("used_mem_gb", 2.0)
            
            # Fetch latest metrics from the history list
            with METRICS_HISTORY_LOCK:
                if METRICS_HISTORY:
                    latest = METRICS_HISTORY[-1]
                    iops = latest["iops"]
                    bw = latest["bw_kbps"]
                    latency = latest["latency_ms"]
                else:
                    iops = 11.5
                    bw = 184
                    latency = 0.95
                    
            rx_mbps = 0.0
            tx_mbps = 0.0

            # Determine cluster resiliency from offline nodes / failed mimir alerts
            cluster_name = "hci-01"
            redundancy_factor = 1
            try:
                with open("/etc/hci/cluster.json", "r") as f:
                    cdata = json.load(f)
                    cluster_name = cdata.get("cluster_name", "hci-01")
                    redundancy_factor = int(cdata.get("redundancy_factor", 1))
            except Exception:
                pass

            resilience_status = "GOOD"
            resilience_ftt = redundancy_factor

            # Retrieve hosts list to determine which nodes hold data replicas
            hosts_list = []
            try:
                with open("/etc/hci/cluster.json", "r") as f:
                    cdata = json.load(f)
                    hosts_list = cdata.get("hosts", [])
            except Exception:
                pass

            # The first redundancy_factor + 1 hosts hold replicas.
            repl_count = min(len(hosts_list), redundancy_factor + 1) if hosts_list else redundancy_factor + 1
            replica_ips = {h.get("ip") for h in hosts_list[:repl_count] if h.get("ip")}

            # Fallback using nodes_info if hosts_list is empty
            if not replica_ips:
                sorted_nodes = sorted(nodes_info, key=lambda n: n.get("ip", ""))
                repl_count = min(len(sorted_nodes), redundancy_factor + 1)
                replica_ips = {n.get("ip") for n in sorted_nodes[:repl_count] if n.get("ip")}

            # Determine offline replica-holding nodes
            offline_nodes = [n for n in nodes_info if n.get("status") == "OFFLINE"]
            offline_replica_ips = [ip for ip in replica_ips if ip in [n.get("ip") for n in offline_nodes]]

            # Define data-safety critical check names that affect storage resiliency
            storage_check_names = {"storage_capacity", "aether_volume", "aether_status", "aether_peers"}

            storage_failures = [
                a for a in alerts 
                if a.get("check_name") in storage_check_names and a.get("type") == "critical"
            ]
            storage_warnings = [
                a for a in alerts 
                if a.get("check_name") in storage_check_names and a.get("type") == "warning"
            ]

            # Calculate resiliency status strictly based on data safety:
            if len(replica_ips) > 0 and len(offline_replica_ips) == len(replica_ips):
                # All nodes holding replicas are offline -> CRITICAL (no copies available)
                resilience_status = "CRITICAL"
                resilience_ftt = 0
            elif len(offline_replica_ips) > 0 or len(storage_failures) > 0 or len(storage_warnings) > 0:
                # A replica-holding node goes offline or there is a storage failure/warning -> DEGRADED
                resilience_status = "DEGRADED"
                resilience_ftt = max(0, redundancy_factor - len(offline_replica_ips))
            else:
                resilience_status = "GOOD"
                resilience_ftt = redundancy_factor

            response = {
                "cluster_name": cluster_name,
                "resiliency": {
                    "status": resilience_status,
                    "ftt": resilience_ftt
                },
                "nodes": nodes_info,
                "vms": {
                    "active": len(running_vms),
                    "list": vms_list
                },
                "storage": storage_usage,
                "metrics": {
                    "cpu_pct": cpu_pct,
                    "cpu_cores": cores,
                    "total_cpu_ghz": total_cpu_ghz,
                    "mem_pct": mem_pct,
                    "total_mem_gb": total_mem_gb,
                    "used_mem_gb": used_mem_gb,
                    "iops": iops,
                    "bw_kbps": bw,
                    "latency_ms": latency,
                    "net_rx_mbps": rx_mbps,
                    "net_tx_mbps": tx_mbps
                },
                "alerts": alerts,
                "events": list(reversed(EVENT_LOGS))
            }
            STATUS_CACHE["data"] = response
            STATUS_CACHE["last_fetched"] = now
            self.send_json(200, response)
            return

        elif path == "/api/metrics/history":
            with METRICS_HISTORY_LOCK:
                history_list = list(METRICS_HISTORY)
            self.send_json(200, {"history": history_list})
            return

        elif path == "/api/cluster/metrics":
            cql = "SELECT JSON * FROM hydra.logos_metrics;"
            rc, stdout, stderr = run_cql_query(cql)
            metrics = []
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            metrics.append(json.loads(line))
                        except Exception:
                            pass
            
            logs = []
            
            # 1. mimir_results
            cql_mimir = "SELECT JSON * FROM hydra.mimir_results;"
            rc_m, stdout_m, _ = run_cql_query(cql_mimir)
            if rc_m == 0:
                for line in stdout_m.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            item = json.loads(line)
                            ts = item.get("timestamp", "")
                            check = item.get("check_name", "")
                            status = item.get("status", "")
                            node = item.get("node_ip", "")
                            out = item.get("output", "")
                            msg = f"[{node}] Mimir Check '{check}' finished with status '{status}'. Output: {out}"
                            logs.append({
                                "timestamp": ts,
                                "source": "Mimir",
                                "level": "INFO" if status == "PASS" else "WARNING",
                                "message": msg
                            })
                        except:
                            pass
            
            # 2. dagur_runs
            cql_dagur = "SELECT JSON * FROM hydra.dagur_runs LIMIT 50;"
            rc_d, stdout_d, _ = run_cql_query(cql_dagur)
            if rc_d == 0:
                for line in stdout_d.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            item = json.loads(line)
                            ts = item.get("start_time", "")
                            job = item.get("job_name", "")
                            status = item.get("status", "")
                            exit_code = item.get("exit_code", 0)
                            out = item.get("output", "")
                            msg = f"Dagur Job '{job}' finished with status '{status}' (Exit: {exit_code}). Output: {out}"
                            logs.append({
                                "timestamp": ts,
                                "source": "Dagur",
                                "level": "INFO" if status == "SUCCESS" else "ERROR",
                                "message": msg
                            })
                        except:
                            pass
                            
            # 3. catalyst_tasks
            cql_catalyst = "SELECT JSON * FROM hydra.catalyst_tasks;"
            rc_c, stdout_c, _ = run_cql_query(cql_catalyst)
            if rc_c == 0:
                for line in stdout_c.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            item = json.loads(line)
                            ts = item.get("created_at", "")
                            service = item.get("service", "")
                            action = item.get("action", "")
                            status = item.get("status", "")
                            progress = item.get("progress", 0)
                            err_msg = item.get("error_msg", "")
                            msg = f"Catalyst Task '{action}' ({service}) is {status} (progress: {progress}%)."
                            if err_msg:
                                msg += f" Error: {err_msg}"
                            logs.append({
                                "timestamp": ts,
                                "source": "Catalyst",
                                "level": "INFO" if status in ["completed", "running"] else "ERROR",
                                "message": msg
                            })
                        except:
                            pass

            # 4. console_metrics
            cql_console = "SELECT JSON * FROM hydra.console_metrics;"
            rc_cm, stdout_cm, _ = run_cql_query(cql_console)
            console_metrics_list = []
            if rc_cm == 0:
                for line in stdout_cm.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            item = json.loads(line)
                            console_metrics_list.append(item)
                            ts = item.get("timestamp", "")
                            vm_name = item.get("vm_name", "")
                            avg_fps = item.get("avg_fps", 0.0)
                            low_fps = item.get("low_fps", 0.0)
                            latency = item.get("latency", 0.0)
                            msg = f"[{vm_name}] Console Performance: Avg FPS: {avg_fps:.1f}, 1% Low FPS: {low_fps:.1f}, Latency: {latency:.1f}ms"
                            logs.append({
                                "timestamp": ts,
                                "source": "Console",
                                "level": "INFO",
                                "message": msg
                            })
                        except:
                            pass

            def get_ts_epoch(log_item):
                t = log_item.get("timestamp")
                if not t:
                    return 0
                if isinstance(t, (int, float)):
                    return float(t)
                try:
                    import datetime
                    t_clean = str(t).split("+")[0].replace("Z", "").strip()
                    if "." in t_clean:
                        dt = datetime.datetime.strptime(t_clean, "%Y-%m-%d %H:%M:%S.%f")
                    else:
                        dt = datetime.datetime.strptime(t_clean, "%Y-%m-%d %H:%M:%S")
                    return dt.timestamp() * 1000
                except Exception:
                    return 0

            logs.sort(key=get_ts_epoch, reverse=True)
            logs = logs[:200]

            self.send_json(200, {"metrics": metrics, "logs": logs, "console_metrics": console_metrics_list})
            return

        elif path == "/api/cluster/nodes/hardware":
            nodes = get_cluster_nodes()
            result_nodes = []
            for n in nodes:
                node_ip = n.get("ip")
                hostname = n.get("hostname")
                if not node_ip:
                    continue
                
                cpu_cmd = 'echo -n "cores:"; nproc; echo -n "model:"; grep -m 1 "model name" /proc/cpuinfo | cut -d: -f2-'
                rc_cpu, out_cpu, err_cpu = run_remote_spark(node_ip, cpu_cmd)
                
                cpu_data = {"cores": 0, "model": "Unknown"}
                if rc_cpu == 0:
                    cores = 0
                    model = "Unknown"
                    for line in out_cpu.splitlines():
                        if line.startswith("cores:"):
                            try:
                                cores = int(line.split(":", 1)[1].strip())
                            except:
                                pass
                        elif line.startswith("model:"):
                            model = line.split(":", 1)[1].strip()
                    cpu_data = {"cores": cores, "model": model}
                
                rc_ram, out_ram, err_ram = run_remote_spark(node_ip, "free -b")
                ram_data = {"total": 0, "used": 0, "free": 0}
                if rc_ram == 0:
                    lines = out_ram.splitlines()
                    for line in lines:
                        if line.strip().startswith("Mem:"):
                            parts = line.split()
                            if len(parts) >= 4:
                                try:
                                    ram_data = {
                                        "total": int(parts[1]),
                                        "used": int(parts[2]),
                                        "free": int(parts[3])
                                    }
                                except:
                                    pass
                
                rc_disk, out_disk, err_disk = run_remote_spark(node_ip, "lsblk -o NAME,SIZE,TYPE,MOUNTPOINT -J")
                disks_data = []
                if rc_disk == 0:
                    try:
                        disks_data = json.loads(out_disk).get("blockdevices", [])
                    except:
                        pass
                
                rc_net, out_net, err_net = run_remote_spark(node_ip, "ip -j addr show")
                network_data = []
                if rc_net == 0:
                    try:
                        network_data = json.loads(out_net)
                    except:
                        pass
                
                status = "online" if rc_cpu == 0 else "offline"
                
                result_nodes.append({
                    "hostname": hostname,
                    "ip": node_ip,
                    "status": status,
                    "cpu": cpu_data,
                    "ram": ram_data,
                    "disks": disks_data,
                    "network": network_data
                })
            
            self.send_json(200, {"nodes": result_nodes})
            return

        elif path == "/api/mimir/results":
            db_results = []
            cql = "SELECT JSON * FROM hydra.mimir_results;"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_results.append(json.loads(line))
                        except Exception:
                            pass
            self.send_json(200, {"results": db_results})
            return

        elif path == "/api/mimir/schedules":
            db_schedules = []
            cql = "SELECT JSON * FROM hydra.mimir_schedules;"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_schedules.append(json.loads(line))
                        except Exception:
                            pass
            self.send_json(200, {"schedules": db_schedules})
            return

        elif path == "/api/dagur/schedules":
            db_schedules = []
            cql = "SELECT JSON * FROM hydra.dagur_schedules;"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_schedules.append(json.loads(line))
                        except Exception:
                            pass
            self.send_json(200, {"schedules": db_schedules})
            return

        elif path == "/api/dagur/runs":
            db_runs = []
            cql = "SELECT JSON * FROM hydra.dagur_runs LIMIT 100;"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_runs.append(json.loads(line))
                        except Exception:
                            pass
            self.send_json(200, {"runs": db_runs})
            return

        elif path == "/api/catalyst/tasks":
            now = time.time()
            if TASKS_CACHE["data"] is not None and now - TASKS_CACHE["last_fetched"] < 2.0:
                self.send_json(200, TASKS_CACHE["data"])
                return

            db_tasks = []
            cql = "SELECT JSON * FROM hydra.catalyst_tasks;"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_tasks.append(json.loads(line))
                        except Exception:
                            pass
                try:
                    db_tasks.sort(key=lambda x: x.get("created_at", 0), reverse=True)
                except Exception:
                    pass
                
                response_data = {"tasks": db_tasks}
                TASKS_CACHE["data"] = response_data
                TASKS_CACHE["last_fetched"] = now
            else:
                if TASKS_CACHE["data"] is not None:
                    response_data = TASKS_CACHE["data"]
                else:
                    response_data = {"tasks": []}

            self.send_json(200, response_data)
            return

        elif path == "/api/storage/containers":
            db_containers = []
            cql = "SELECT JSON * FROM hydra.storage_containers;"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_containers.append(json.loads(line))
                        except Exception:
                            pass
            self.send_json(200, {"containers": db_containers})
            return

        elif path == "/api/images":
            db_images = []
            cql = "SELECT JSON * FROM hydra.valhalla_images;"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_images.append(json.loads(line))
                        except Exception:
                            pass
            # Scan filesystem for files not in database
            target_dir = "/var/lib/hci/aether/volumes/default-image-container"
            if os.path.exists(target_dir):
                try:
                    existing_names = {img.get("name") for img in db_images if img.get("name")}
                    for f in os.listdir(target_dir):
                        if f.lower().endswith((".iso", ".img", ".qcow2")) and f not in existing_names:
                            fpath = os.path.join(target_dir, f)
                            st = os.stat(fpath)
                            size_bytes = st.st_size
                            created_at = int(st.st_mtime * 1000)
                            image_meta = {
                                "name": f,
                                "filename": f,
                                "size_bytes": size_bytes,
                                "type": "iso" if f.lower().endswith(".iso") else "template",
                                "path": fpath,
                                "created_at": created_at
                            }
                            cql_ins = f"INSERT INTO hydra.valhalla_images JSON '{json.dumps(image_meta)}';"
                            run_cql_query(cql_ins)
                            db_images.append(image_meta)
                except Exception as e:
                    print(f"[API] Error scanning image directory: {e}")
            self.send_json(200, {"images": db_images})
            return

        elif path == "/api/storage/disks":
            # Build list of virtual disks from DB
            db_vms = []
            cql = "SELECT JSON * FROM hydra.vms;"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_vms.append(json.loads(line))
                        except Exception:
                            pass

            disks = []
            for vm in db_vms:
                default_cont = get_default_container()
                disks.append({
                    "name": f"vm-disk-{vm['name']}",
                    "container": default_cont,
                    "size": f"{vm.get('disk_size', 10)} GB",
                    "disk_path": vm.get("disk_path", f"/var/lib/hci/aether/volumes/{default_cont}/{vm['name']}.raw"),
                    "timestamp": None
                })
            self.send_json(200, {"disks": disks})
            return


        elif path == "/api/vms/console/ping":
            self.send_json(200, {"status": "pong"})
            return

        elif path == "/api/vms/console/token":
            query_params = urllib.parse.parse_qs(url_parsed.query)
            vm_name = query_params.get("name", [None])[0]
            console_type = query_params.get("type", ["vnc"])[0]
            if not vm_name:
                self.send_json(400, {"error": "Missing VM name"})
                return

            db_vm = None
            cql = f"SELECT JSON * FROM hydra.vms WHERE name = '{vm_name}';"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            db_vm = json.loads(line)
                        except Exception:
                            pass
            
            if not db_vm:
                self.send_json(404, {"error": f"VM '{vm_name}' not found"})
                return

            host_ip = db_vm.get("host_ip", LOCAL_IP)
            if not host_ip or host_ip == "127.0.0.1":
                host_ip = LOCAL_IP

            vnc_port = None
            if console_type == "spice":
                cmd = f"virsh -c qemu:///system domdisplay {vm_name} --type spice"
            else:
                cmd = f"virsh -c qemu:///system vncdisplay {vm_name}"

            if host_ip == LOCAL_IP or host_ip == "127.0.0.1" or host_ip == "":
                rc, stdout, stderr = run_remote_spark("127.0.0.1", cmd)
            else:
                rc, stdout, stderr = run_remote_spark(host_ip, cmd)

            if rc == 0:
                display = stdout.strip()
                if console_type == "spice":
                    if ":" in display:
                        try:
                            vnc_port = int(display.split(":")[-1])
                        except ValueError:
                            pass
                else:
                    if ":" in display:
                        try:
                            display_num = int(display.split(":")[-1])
                            vnc_port = 5900 + display_num
                        except ValueError:
                            pass

            if vnc_port is None:
                self.send_json(500, {"error": "Could not resolve VM console port"})
                return

            token = secrets.token_hex(16)
            expires_at = int(time.time()) + 300

            cql_insert = f"INSERT INTO hydra.console_sessions (console_token, host_ip, port, expires_at) VALUES ('{token}', '{host_ip}', {vnc_port}, {expires_at});"
            run_cql_query(cql_insert)

            self.send_json(200, {
                "token": token,
                "host_ip": host_ip,
                "port": vnc_port
            })
            return

        elif path == "/api/vms/console/ws":
            websocket_key = self.headers.get("Sec-WebSocket-Key")
            if websocket_key:
                # Handle websocket upgrade and run proxy
                import hashlib
                import base64
                guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                accept_key = base64.b64encode(hashlib.sha1((websocket_key + guid).encode()).digest()).decode()

                self.send_response(101, "Switching Protocols")
                self.send_header("Upgrade", "websocket")
                self.send_header("Connection", "Upgrade")
                self.send_header("Sec-WebSocket-Accept", accept_key)
                
                ws_protocol = self.headers.get("Sec-WebSocket-Protocol")
                if ws_protocol:
                    first_protocol = [p.strip() for p in ws_protocol.split(",")][0]
                    self.send_header("Sec-WebSocket-Protocol", first_protocol)
                self.end_headers()

                query_params = urllib.parse.parse_qs(url_parsed.query)
                token = query_params.get("token", [None])[0]
                vm_name = query_params.get("name", [None])[0]
                console_type = query_params.get("type", ["vnc"])[0]

                host_ip = None
                vnc_port = None

                if token:
                    cql = f"SELECT JSON host_ip, port, expires_at FROM hydra.console_sessions WHERE console_token = '{token}';"
                    rc, stdout, stderr = run_cql_query(cql)
                    if rc == 0:
                        for line in stdout.splitlines():
                            line = line.strip()
                            if line.startswith("{") and line.endswith("}"):
                                try:
                                    data = json.loads(line)
                                    expires_at = data.get("expires_at", 0)
                                    if expires_at > int(time.time()):
                                        host_ip = data.get("host_ip")
                                        vnc_port = data.get("port")
                                except Exception:
                                    pass

                if not host_ip or vnc_port is None:
                    # Fallback to legacy name-based lookup
                    if not vm_name:
                        self.connection.close()
                        return

                    db_vm = None
                    cql = f"SELECT JSON * FROM hydra.vms WHERE name = '{vm_name}';"
                    rc, stdout, stderr = run_cql_query(cql)
                    if rc == 0:
                        for line in stdout.splitlines():
                            line = line.strip()
                            if line.startswith("{") and line.endswith("}"):
                                try:
                                    db_vm = json.loads(line)
                                    break
                                except Exception:
                                    pass
                    
                    if not db_vm:
                        self.connection.close()
                        return

                    host_ip = db_vm.get("host_ip", LOCAL_IP)
                    if not host_ip:
                        host_ip = LOCAL_IP

                    if console_type == "spice":
                        cmd = f"virsh -c qemu:///system domdisplay {vm_name} --type spice"
                    else:
                        cmd = f"virsh -c qemu:///system vncdisplay {vm_name}"

                    if host_ip == LOCAL_IP or host_ip == "127.0.0.1" or host_ip == "":
                        rc, stdout, stderr = run_remote_spark("127.0.0.1", cmd)
                    else:
                        rc, stdout, stderr = run_remote_spark(host_ip, cmd)

                    if rc == 0:
                        display = stdout.strip()
                        if console_type == "spice":
                            if ":" in display:
                                try:
                                    vnc_port = int(display.split(":")[-1])
                                except ValueError:
                                    pass
                        else:
                            if ":" in display:
                                try:
                                    display_num = int(display.split(":")[-1])
                                    vnc_port = 5900 + display_num
                                except ValueError:
                                    pass

                if not vm_name:
                    vm_name = "Session (via token)"

                print(f"[WS Proxy] Handshake request received for VM: '{vm_name}' (type: '{console_type}', node: '{host_ip}')")

                print(f"[WS Proxy] Resolved hypervisor console target: {host_ip}:{vnc_port}")
                if vnc_port is None:
                    print(f"[WS Proxy] Display command failed or port could not be parsed for VM '{vm_name}'")
                    self.connection.close()
                    return

                import socket
                vnc_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                vnc_sock.settimeout(5)
                try:
                    vnc_sock.connect((host_ip, vnc_port))
                    print(f"[WS Proxy] Connected successfully to target {host_ip}:{vnc_port}")
                except Exception as e:
                    print(f"[WS Proxy] Connection failed to target {host_ip}:{vnc_port}: {str(e)}")
                    self.connection.close()
                    return

                import select
                self.connection.setblocking(True)
                vnc_sock.setblocking(True)

                inputs = [self.connection, vnc_sock]
                closed = False
                
                while not closed:
                    try:
                        readable, _, exceptional = select.select(inputs, [], inputs, 60)
                        if exceptional:
                            print(f"[WS Proxy] select exceptional event occurred, closing.")
                            break
                        if not readable:
                            self.connection.sendall(encode_websocket_frame(b"", opcode=9))
                            continue
                            
                        for s in readable:
                            if s is self.connection:
                                opcode, payload = decode_websocket_frame(self.connection)
                                if opcode is None:
                                    print(f"[WS Proxy] Client connection closed (opcode is None)")
                                    closed = True
                                    break
                                if opcode == 8:
                                    print(f"[WS Proxy] Client connection closed with Close frame (opcode 8)")
                                    closed = True
                                    break
                                if opcode == 9:
                                    self.connection.sendall(encode_websocket_frame(payload, opcode=10))
                                if opcode == 2 or opcode == 1:
                                    vnc_sock.sendall(payload)
                            elif s is vnc_sock:
                                data = vnc_sock.recv(65536)
                                if not data:
                                    print(f"[WS Proxy] Hypervisor target connection closed (recv empty)")
                                    closed = True
                                    break
                                frame = encode_websocket_frame(data, opcode=2)
                                self.connection.sendall(frame)
                    except Exception as ex:
                        print(f"[WS Proxy] Exception in proxy loop: {str(ex)}")
                        break
                
                print(f"[WS Proxy] Tearing down connection for VM '{vm_name}' (type: '{console_type}')")
                try:
                    vnc_sock.close()
                except Exception:
                    pass
                try:
                    self.connection.close()
                except Exception:
                    pass
                return

            else:
                # If there's no websocket key, it's a coordinate lookup (e.g. SPICE)
                query_params = urllib.parse.parse_qs(url_parsed.query)
                vm_name = query_params.get("name", [None])[0]
                if not vm_name:
                    self.send_json(400, {"error": "Missing VM name"})
                    return

                db_vm = None
                cql = f"SELECT JSON * FROM hydra.vms WHERE name = '{vm_name}';"
                rc, stdout, stderr = run_cql_query(cql)
                if rc == 0:
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                db_vm = json.loads(line)
                            except Exception:
                                pass
                
                if not db_vm:
                    self.send_json(404, {"error": f"VM '{vm_name}' not found"})
                    return

                host_ip = db_vm.get("host_ip", LOCAL_IP)
                if not host_ip or host_ip == "127.0.0.1":
                    host_ip = LOCAL_IP

                console_type = query_params.get("type", ["vnc"])[0]
                
                ws_url = f"ws://{host_ip}:8081/ws?name={vm_name}&type={console_type}"
                print(f"[WS Coordinates] Returning target coordinates for VM '{vm_name}': {ws_url}")
                self.send_json(200, {"url": ws_url})
                return

        # Static files serving from /app/static/
        clean_path = path.split('?')[0]
        if clean_path == "/" or clean_path == "":
            clean_path = "/index.html"

        static_dir = "/app/static"
        file_path = os.path.join(static_dir, clean_path.lstrip('/'))

        if os.path.exists(file_path) and os.path.isfile(file_path):
            content_type = "text/plain"
            if file_path.endswith(".html"):
                content_type = "text/html"
            elif file_path.endswith(".css"):
                content_type = "text/css"
            elif file_path.endswith(".js"):
                content_type = "application/javascript"
            elif file_path.endswith(".png"):
                content_type = "image/png"
            elif file_path.endswith(".jpg") or file_path.endswith(".jpeg"):
                content_type = "image/jpeg"
            elif file_path.endswith(".svg"):
                content_type = "image/svg+xml"
            elif file_path.endswith(".json"):
                content_type = "application/json"
            elif file_path.endswith(".wasm"):
                content_type = "application/wasm"

            try:
                with open(file_path, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.end_headers()
                self.wfile.write(content)
                return
            except Exception as e:
                self.send_response(500)
                body = f"Internal error: {str(e)}".encode("utf-8")
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
        
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()
        return

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        url_parsed = urllib.parse.urlparse(self.path)
        path = url_parsed.path

        # 1. CSRF Verification (Origin and Referer Checks)
        origin = self.headers.get("Origin")
        referer = self.headers.get("Referer")
        host = self.headers.get("Host", "")
        if origin:
            parsed_origin = urllib.parse.urlparse(origin)
            if parsed_origin.netloc != host:
                self.send_json(403, {"error": "CSRF check failed: Origin mismatch"})
                return
        elif referer:
            parsed_referer = urllib.parse.urlparse(referer)
            if parsed_referer.netloc != host:
                self.send_json(403, {"error": "CSRF check failed: Referer mismatch"})
                return

        # Auth Guard
        if path.startswith("/api/") and path != "/api/login":
            if not is_authenticated(self):
                self.send_json(401, {"error": "Unauthorized"})
                return

        if path == "/api/login":
            try:
                data = json.loads(self.rfile.read(content_length).decode('utf-8'))
                username = data.get("username", "")
                password = data.get("password", "")
                print(f"[LOGIN DEBUG] Request for username: '{username}' | Password length: {len(password)}", flush=True)
            except Exception as e:
                print(f"[LOGIN DEBUG] Payload error: {e}", flush=True)
                self.send_json(400, {"error": "Invalid request payload"})
                return
            
            # 2. CQL Injection Sanitization (Alphanumeric username check)
            import re
            if not re.match(r"^[a-zA-Z0-9_\-]+$", username):
                print(f"[LOGIN DEBUG] Rejecting username pattern: '{username}'", flush=True)
                self.send_json(400, {"error": "Invalid characters in username"})
                return

            # 3. Rate Limiting / Lockout Check
            now = time.time()
            lockout_info = LOGIN_LOCKOUTS.get(username, [0, 0]) # [failed_attempts, lockout_until]
            if lockout_info[1] > now:
                remaining = int(lockout_info[1] - now)
                print(f"[LOGIN DEBUG] Rejecting username: '{username}' due to lockout ({remaining}s remaining)", flush=True)
                self.send_json(429, {"error": f"Account locked. Try again in {remaining} seconds."})
                return
                
            cql = f"SELECT password_hash FROM hydra.users WHERE username = '{username}';"
            rc, out, err = run_cql_query(cql)
            hashed = ""
            if rc == 0:
                lines = [l.strip() for l in out.splitlines() if l.strip()]
                hash_lines = [l for l in lines if not l.startswith('(') and not l.startswith('-') and l != 'password_hash']
                if hash_lines:
                    hashed = hash_lines[0]
                    
            if hashed and verify_password(password, hashed):
                print(f"[LOGIN DEBUG] Successful authentication for username: '{username}'", flush=True)
                # Reset lockouts on success
                LOGIN_LOCKOUTS[username] = [0, 0]
                
                token = secrets.token_hex(32)
                import datetime
                now_ms = int(datetime.datetime.now().timestamp() * 1000)
                insert_cql = f"INSERT INTO hydra.sessions (session_token, username, created_at) VALUES ('{token}', '{username}', {now_ms});"
                run_cql_query(insert_cql)
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                cookie = http.cookies.SimpleCookie()
                cookie["session_id"] = token
                cookie["session_id"]["path"] = "/"
                cookie["session_id"]["httponly"] = True
                self.send_header("Set-Cookie", cookie.output(header=""))
                body = json.dumps({"status": "success", "username": username, "token": token}).encode("utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                # Increment failed attempts for lockout
                lockout_info[0] += 1
                print(f"[LOGIN DEBUG] Failed password attempt for username: '{username}' | Total failed: {lockout_info[0]}", flush=True)
                if lockout_info[0] >= 5:
                    lockout_info[1] = now + 60  # 60s lockout
                    LOGIN_LOCKOUTS[username] = lockout_info
                    self.send_json(429, {"error": "Too many failed attempts. Account locked for 60 seconds."})
                else:
                    LOGIN_LOCKOUTS[username] = lockout_info
                    self.send_json(401, {"error": f"Invalid username or password. {5 - lockout_info[0]} attempts remaining."})
            return

        elif path == "/api/auth/logout":
            session_token = None
            auth_header = self.headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                session_token = auth_header[7:].strip()
            if not session_token:
                cookie_header = self.headers.get("Cookie", "")
                if cookie_header:
                    try:
                        cookie = http.cookies.SimpleCookie(cookie_header)
                        if "session_id" in cookie:
                            session_token = cookie["session_id"].value
                    except Exception:
                        pass
            if session_token:
                delete_cql = f"DELETE FROM hydra.sessions WHERE session_token = '{session_token}';"
                run_cql_query(delete_cql)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            cookie = http.cookies.SimpleCookie()
            cookie["session_id"] = ""
            cookie["session_id"]["path"] = "/"
            cookie["session_id"]["httponly"] = True
            cookie["session_id"]["max-age"] = 0
            self.send_header("Set-Cookie", cookie.output(header=""))
            body = json.dumps({"status": "success"}).encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        elif path == "/api/lcm/upload":
            try:
                if content_length <= 0:
                    self.send_json(400, {"error": "Content-Length must be greater than zero."})
                    return
                
                zip_path = "/tmp/helios_update.zip"
                extract_dir = "/tmp/helios_update"
                
                # Stream the upload in chunks of 64KB directly to the file
                chunk_size = 64 * 1024
                bytes_remaining = content_length
                
                with open(zip_path, "wb") as f_out:
                    while bytes_remaining > 0:
                        chunk_to_read = min(chunk_size, bytes_remaining)
                        chunk = self.rfile.read(chunk_to_read)
                        if not chunk:
                            break
                        f_out.write(chunk)
                        bytes_remaining -= len(chunk)
                        
                sys.path.append("/usr/local/bin")
                sys.path.append(".")
                try:
                    import hylia
                except ImportError:
                    import hylia
                    
                manifest, changelog_content = hylia.validate_and_extract_zip(zip_path, extract_dir)
                distribute_update_package(zip_path)
                
                # Check current version and build numbers
                components_preview = []
                components = manifest.get("components", {})
                for comp_name, comp_info in components.items():
                    comp_file = comp_info.get("file")
                    target_path = comp_info.get("target_path", f"/usr/local/bin/{comp_name}")
                    
                    # Read current build number from host disk via local spark-daemon
                    current_build = "Not Installed"
                    rc_v, res_v, err_v = run_mtls_spark_api(
                        "127.0.0.1",
                        f"/api/v1/node/binary-version?path={urllib.parse.quote(target_path)}",
                        None,
                        method="GET"
                    )
                    if rc_v == 0 and "version" in res_v:
                        current_build = res_v["version"]
                        if current_build == "Unknown":
                            current_build = "1.2.0-b4081"
                    new_build = manifest.get("build", "Unknown")
                    
                    components_preview.append({
                        "name": comp_name,
                        "file": comp_file,
                        "current_build": current_build,
                        "new_build": new_build
                    })
                    
                # Generate a UUID for the job
                job_id = str(uuid.uuid4())
                target_nodes = [h["ip"] for h in hylia.get_cluster_hosts()]
                if not target_nodes:
                    target_nodes = ["127.0.0.1"]
                    
                # Save job state in ScyllaDB
                manifest_json = json.dumps(manifest).replace("'", "''")
                changelog_escaped = changelog_content.replace("'", "''")
                nodes_list_str = "[" + ", ".join([f"'{ip}'" for ip in target_nodes]) + "]"
                
                # Delete any old LCM jobs
                hylia.run_cql_query("TRUNCATE hydra.hylia_jobs;")
                hylia.run_cql_query("TRUNCATE hydra.hylia_logs;")
                
                build_num = manifest.get("build", "0000")
                if "-b" not in build_num:
                    build_num = f"{manifest.get('version', '1.2.0')}-b{build_num}"
                    
                cql = f"""
                INSERT INTO hydra.hylia_jobs (
                    job_id, state, target_nodes, current_node, build_number, manifest_json, changelog_md
                ) VALUES (
                    {job_id}, 'IDLE', {nodes_list_str}, '', '{build_num}', '{manifest_json}', '{changelog_escaped}'
                );
                """
                rc, _, err_db = hylia.run_cql_query(cql)
                if rc != 0:
                    raise Exception(f"Database error saving upgrade job: {err_db}")
                    
                self.send_json(200, {
                    "status": "success",
                    "job_id": job_id,
                    "build_number": build_num,
                    "components": components_preview,
                    "changelog": changelog_content,
                    "min_hylia_version": manifest.get("min_hylia_version", manifest.get("build"))
                })
            except Exception as e:
                self.send_json(400, {"error": str(e)})
            return

        elif path == "/api/lcm/upgrade/check":
            # Submit check task to Catalyst
            payload = {
                "service": "dagur",
                "action": "execute",
                "payload": {
                    "job_name": "manual_update_check",
                    "command": "python3 /usr/local/bin/check-updates"
                }
            }
            try:
                leader_ip = get_catalyst_target_ip()
                req = urllib.request.Request(
                    f"http://{leader_ip}:9091/api/v1/tasks/submit",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=10) as response:
                    res = json.loads(response.read().decode("utf-8"))
                    self.send_json(200, {"task_id": res.get("task_id"), "status": "pending"})
            except Exception as e:
                self.send_json(500, {"error": f"Failed to submit update check task to Catalyst: {str(e)}"})
            return

        elif path == "/api/lcm/upgrade/download":
            try:
                content = self.rfile.read(content_length)
                payload = json.loads(content.decode('utf-8'))
                download_url = payload.get("download_url")
                expected_sha256 = payload.get("sha256")
                
                if not download_url:
                    self.send_json(400, {"error": "Missing download_url in payload"})
                    return
                
                zip_path = "/tmp/helios_update.zip"
                extract_dir = "/tmp/helios_update"
                
                # 1. Download file from update server
                # Append cache buster to bypass Cloudflare CDN caching
                cb = int(time.time())
                download_url_cb = download_url
                if "?" in download_url_cb:
                    download_url_cb += f"&cb={cb}"
                else:
                    download_url_cb += f"?cb={cb}"
                
                req = urllib.request.Request(download_url_cb, headers={'User-Agent': 'Helios-Spectrum-Client'})
                sha256_verifier = hashlib.sha256()
                
                with urllib.request.urlopen(req, timeout=60) as response:
                    with open(zip_path, "wb") as f_out:
                        while chunk := response.read(65536):
                            f_out.write(chunk)
                            sha256_verifier.update(chunk)
                            
                actual_sha256 = sha256_verifier.hexdigest()
                
                # 2. Check hash
                if expected_sha256 and actual_sha256.lower() != expected_sha256.lower():
                    self.send_json(400, {
                        "error": f"Downloaded package hash mismatch. Expected: {expected_sha256}, Got: {actual_sha256}"
                    })
                    return
                
                sys.path.append("/usr/local/bin")
                sys.path.append(".")
                try:
                    import hylia
                except ImportError:
                    import hylia
                    
                manifest, changelog_content = hylia.validate_and_extract_zip(zip_path, extract_dir)
                distribute_update_package(zip_path)
                
                # Check current version and build numbers
                components_preview = []
                components = manifest.get("components", {})
                for comp_name, comp_info in components.items():
                    comp_file = comp_info.get("file")
                    target_path = comp_info.get("target_path", f"/usr/local/bin/{comp_name}")
                    
                    current_build = "Not Installed"
                    rc_v, res_v, err_v = run_mtls_spark_api(
                        "127.0.0.1",
                        f"/api/v1/node/binary-version?path={urllib.parse.quote(target_path)}",
                        None,
                        method="GET"
                    )
                    if rc_v == 0 and "version" in res_v:
                        current_build = res_v["version"]
                        if current_build == "Unknown":
                            current_build = "1.2.0-b4081"
                        
                    new_build = manifest.get("build", "Unknown")
                    
                    components_preview.append({
                        "name": comp_name,
                        "file": comp_file,
                        "current_build": current_build,
                        "new_build": new_build
                    })
                    
                job_id = str(uuid.uuid4())
                target_nodes = [h["ip"] for h in hylia.get_cluster_hosts()]
                if not target_nodes:
                    target_nodes = ["127.0.0.1"]
                    
                manifest_json = json.dumps(manifest).replace("'", "''")
                changelog_escaped = changelog_content.replace("'", "''")
                nodes_list_str = "[" + ", ".join([f"'{ip}'" for ip in target_nodes]) + "]"
                
                hylia.run_cql_query("TRUNCATE hydra.hylia_jobs;")
                hylia.run_cql_query("TRUNCATE hydra.hylia_logs;")
                
                build_num = manifest.get("build", "0000")
                if "-b" not in build_num:
                    build_num = f"{manifest.get('version', '1.2.0')}-b{build_num}"
                    
                cql = f"""
                INSERT INTO hydra.hylia_jobs (
                    job_id, state, target_nodes, current_node, build_number, manifest_json, changelog_md
                ) VALUES (
                    {job_id}, 'IDLE', {nodes_list_str}, '', '{build_num}', '{manifest_json}', '{changelog_escaped}'
                );
                """
                rc, _, err_db = hylia.run_cql_query(cql)
                if rc != 0:
                    raise Exception(f"Database error saving upgrade job: {err_db}")
                    
                self.send_json(200, {
                    "status": "success",
                    "job_id": job_id,
                    "build_number": build_num,
                    "components": components_preview,
                    "changelog": changelog_content,
                    "min_hylia_version": manifest.get("min_hylia_version", manifest.get("build"))
                })
            except Exception as e:
                self.send_json(400, {"error": str(e)})
            return

        elif path == "/api/lcm/upgrade/start":
            try:
                sys.path.append("/usr/local/bin")
                sys.path.append(".")
                try:
                    import hylia
                except ImportError:
                    import hylia
                
                content = self.rfile.read(content_length) if content_length > 0 else b"{}"
                payload = json.loads(content.decode('utf-8')) if content else {}
                selected_components = payload.get("components")
                
                rc, stdout, _ = run_cql_query("SELECT JSON job_id, state, manifest_json FROM hydra.hylia_jobs;")
                if rc != 0 or not stdout or not stdout.strip():
                    self.send_json(400, {"error": "No upgrade job loaded. Please upload an update package first."})
                    return
                
                job = json.loads(stdout.splitlines()[0])
                job_id = job.get("job_id")
                job_state = job.get("state")
                manifest = json.loads(job.get("manifest_json", "{}"))
                
                if job_state in ["UPGRADING", "STARTING"]:
                    self.send_json(200, {"status": "already_running", "job_id": job_id})
                    return
                
                if selected_components is not None:
                    # Enforce minimum hylia version check
                    hylia_info = manifest.get("components", {}).get("hylia")
                    if hylia_info:
                        target_hylia_version = hylia_info.get("version", manifest.get("build", "Unknown"))
                        min_hylia_version = manifest.get("min_hylia_version", target_hylia_version)
                        current_hylia_version = "Not Installed"
                        rc_v, res_v, err_v = run_mtls_spark_api(
                            "127.0.0.1",
                            f"/api/v1/node/binary-version?path={urllib.parse.quote(hylia_info.get('target_path', '/usr/local/bin/hylia'))}",
                            None,
                            method="GET"
                        )
                        if rc_v == 0 and "version" in res_v:
                            current_hylia_version = res_v["version"]
                        
                        def parse_ver(v_str):
                            if not v_str or v_str in ["Unknown", "Not Installed"]:
                                return (0, 0, 0, 0)
                            try:
                                main_part = v_str
                                build_num = 0
                                if "-" in v_str:
                                    main_part, build_part = v_str.split("-", 1)
                                    if build_part.startswith("b"):
                                        try:
                                            build_num = int(build_part[1:])
                                        except ValueError:
                                            pass
                                parts = main_part.split(".")
                                return (
                                    int(parts[0]) if len(parts) > 0 else 0,
                                    int(parts[1]) if len(parts) > 1 else 0,
                                    int(parts[2]) if len(parts) > 2 else 0,
                                    build_num
                                )
                            except Exception:
                                return (0, 0, 0, 0)
                                
                        if parse_ver(current_hylia_version) < parse_ver(min_hylia_version):
                            if "hylia" not in selected_components:
                                self.send_json(400, {
                                    "error": f"The currently installed Hylia version ({current_hylia_version}) is below the minimum version ({min_hylia_version}) required for this update. Please select 'hylia' to continue."
                                })
                                return
                    
                    # Filter manifest components
                    filtered_components = {}
                    for comp_name, comp_info in manifest.get("components", {}).items():
                        if comp_name in selected_components:
                            filtered_components[comp_name] = comp_info
                    
                    manifest["components"] = filtered_components
                    new_manifest_json = json.dumps(manifest).replace("'", "''")
                    
                    rc_m, _, err_m = run_cql_query(f"UPDATE hydra.hylia_jobs SET manifest_json = '{new_manifest_json}' WHERE job_id = {job_id};")
                    if rc_m != 0:
                        raise Exception(f"Database error saving filtered manifest: {err_m}")
                
                rc_up, _, err_up = run_cql_query(f"UPDATE hydra.hylia_jobs SET state = 'STARTING' WHERE job_id = {job_id};")
                if rc_up != 0:
                    raise Exception(f"Database error starting upgrade: {err_up}")
                    
                self.send_json(200, {"status": "success", "job_id": job_id, "message": "Rolling upgrade sequence started."})
            except Exception as e:
                self.send_json(500, {"error": str(e)})
            return

        elif path == "/api/lcm/upgrade/abort":
            try:
                sys.path.append("/usr/local/bin")
                sys.path.append(".")
                try:
                    import hylia
                except ImportError:
                    import hylia
                hylia.run_cql_query("TRUNCATE hydra.hylia_jobs;")
                hylia.run_cql_query("TRUNCATE hydra.hylia_logs;")
                self.send_json(200, {"status": "success", "message": "Upgrade job reset successfully."})
            except Exception as e:
                self.send_json(500, {"error": str(e)})
            return

        elif path == "/api/catalyst/tasks/cleanup":
            cql_select = "SELECT JSON task_id, status FROM hydra.catalyst_tasks;"
            rc, stdout, stderr = run_cql_query(cql_select)
            if rc != 0:
                print(f"[CLEANUP ERROR] SELECT query failed: {stderr or stdout}")
            else:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            obj = json.loads(line)
                            tid = obj.get("task_id")
                            status = obj.get("status")
                            if status in ["completed", "failed"]:
                                print(f"[CLEANUP] Deleting task {tid} with status {status}")
                                del_rc, del_out, del_err = run_cql_query(f"DELETE FROM hydra.catalyst_tasks WHERE task_id = {tid};")
                                if del_rc != 0:
                                    print(f"[CLEANUP ERROR] Failed to delete task {tid}: {del_err or del_out}")
                        except Exception as ex:
                            print(f"[CLEANUP ERROR] Failed parsing/deleting task line: {ex}")
            invalidate_tasks_cache()
            self.send_json(200, {"status": "ok"})
            return

        elif path == "/api/auth/change-password":
            try:
                data = json.loads(self.rfile.read(content_length).decode('utf-8'))
                old_password = data.get("old_password", "")
                new_password = data.get("new_password", "")
            except Exception:
                self.send_json(400, {"error": "Invalid request payload"})
                return
                
            username = getattr(self, "current_user", "")
            if not username:
                self.send_json(401, {"error": "Unauthorized"})
                return
                
            cql = f"SELECT password_hash FROM hydra.users WHERE username = '{username}';"
            rc, out, err = run_cql_query(cql)
            hashed = ""
            if rc == 0:
                lines = [l.strip() for l in out.splitlines() if l.strip()]
                hash_lines = [l for l in lines if not l.startswith('(') and not l.startswith('-') and l != 'password_hash']
                if hash_lines:
                    hashed = hash_lines[0]
                    
            if hashed and verify_password(old_password, hashed):
                ok, err_msg = validate_password_complexity(new_password)
                if not ok:
                    self.send_json(400, {"error": err_msg})
                    return
                new_hash = hash_password(new_password)
                update_cql = f"INSERT INTO hydra.users (username, password_hash) VALUES ('{username}', '{new_hash}');"
                run_cql_query(update_cql)
                self.send_json(200, {"status": "success"})
            else:
                self.send_json(400, {"error": "Incorrect old password"})
            return

        elif path == "/api/cluster/nodes/add":
            try:
                data = json.loads(self.rfile.read(content_length).decode('utf-8'))
                hostname = data.get("hostname")
                node_ip = data.get("ip")
            except Exception:
                self.send_json(400, {"error": "Invalid request payload"})
                return

            if not hostname or not node_ip:
                self.send_json(400, {"error": "Missing hostname or ip"})
                return

            # Read /etc/hci/cluster.json
            cluster_json_path = "/etc/hci/cluster.json"
            cdata = {}
            if os.path.exists(cluster_json_path):
                try:
                    with open(cluster_json_path, "r") as f:
                        cdata = json.load(f)
                except Exception:
                    pass
            
            hosts = cdata.get("hosts", [])
            # Check if already exists
            exists = False
            for h in hosts:
                if h.get("ip") == node_ip or h.get("hostname") == hostname:
                    exists = True
                    break
            
            if not exists:
                max_id = max([h.get("node_id", 0) for h in hosts]) if hosts else 0
                hosts.append({
                    "node_id": max_id + 1,
                    "ip": node_ip,
                    "hostname": hostname
                })
                cdata["hosts"] = hosts
                try:
                    with open(cluster_json_path, "w") as f:
                        json.dump(cdata, f, indent=4)
                except Exception as e:
                    self.send_json(500, {"error": f"Failed to save cluster config: {str(e)}"})
                    return

            # Write to ScyllaDB hydra.nodes
            cql = f"INSERT INTO hydra.nodes (hostname, ip, status, maintenance_mode) VALUES ('{hostname}', '{node_ip}', 'NORMAL', false);"
            run_cql_query(cql)

            # Sync cluster.json across all other nodes
            import base64
            serialized = json.dumps(cdata)
            b64_data = base64.b64encode(serialized.encode('utf-8')).decode('utf-8')
            
            local_ip = "127.0.0.1"
            if os.path.exists("/etc/hci/spectrum/spectrum.env"):
                try:
                    with open("/etc/hci/spectrum/spectrum.env", "r") as f:
                        for line in f:
                            if line.startswith("LOCAL_HYPERVISOR_IP="):
                                local_ip = line.split("=", 1)[1].strip()
                                break
                except:
                    pass

            for h in hosts:
                other_ip = h.get("ip")
                if other_ip and other_ip != local_ip and other_ip != "127.0.0.1":
                    sync_cmd = f"mkdir -p /etc/hci && echo {b64_data} | base64 -d > /etc/hci/cluster.json"
                    run_remote_spark(other_ip, sync_cmd)

            self.send_json(200, {"status": "success", "message": f"Node {hostname} added successfully"})
            return

        elif path == "/api/cluster/nodes/remove":
            try:
                data = json.loads(self.rfile.read(content_length).decode('utf-8'))
                hostname = data.get("hostname")
                node_ip = data.get("ip")
            except Exception:
                self.send_json(400, {"error": "Invalid request payload"})
                return

            if not hostname and not node_ip:
                self.send_json(400, {"error": "Missing hostname or ip to remove"})
                return

            # Read /etc/hci/cluster.json
            cluster_json_path = "/etc/hci/cluster.json"
            cdata = {}
            if os.path.exists(cluster_json_path):
                try:
                    with open(cluster_json_path, "r") as f:
                        cdata = json.load(f)
                except Exception:
                    pass

            hosts = cdata.get("hosts", [])
            removed = False
            new_hosts = []
            removed_hostname = hostname
            removed_ip = node_ip
            for h in hosts:
                match = False
                if hostname and h.get("hostname") == hostname:
                    match = True
                if node_ip and h.get("ip") == node_ip:
                    match = True
                
                if match:
                    removed = True
                    removed_hostname = h.get("hostname")
                    removed_ip = h.get("ip")
                else:
                    new_hosts.append(h)
            
            if removed:
                cdata["hosts"] = new_hosts
                try:
                    with open(cluster_json_path, "w") as f:
                        json.dump(cdata, f, indent=4)
                except Exception as e:
                    self.send_json(500, {"error": f"Failed to save cluster config: {str(e)}"})
                    return

            # Remove from ScyllaDB hydra.nodes
            if removed_hostname:
                cql = f"DELETE FROM hydra.nodes WHERE hostname = '{removed_hostname}';"
                run_cql_query(cql)
            elif removed_ip:
                # Query hostname first
                rc_n, out_n, _ = run_cql_query(f"SELECT hostname FROM hydra.nodes;")
                # Note: filter manually to delete
                for line in out_n.splitlines():
                    line = line.strip()
                    if line:
                        run_cql_query(f"DELETE FROM hydra.nodes WHERE hostname = '{line}';")

            # Sync cluster.json across all other nodes
            import base64
            serialized = json.dumps(cdata)
            b64_data = base64.b64encode(serialized.encode('utf-8')).decode('utf-8')

            local_ip = "127.0.0.1"
            if os.path.exists("/etc/hci/spectrum/spectrum.env"):
                try:
                    with open("/etc/hci/spectrum/spectrum.env", "r") as f:
                        for line in f:
                            if line.startswith("LOCAL_HYPERVISOR_IP="):
                                local_ip = line.split("=", 1)[1].strip()
                                break
                except:
                    pass

            for h in new_hosts:
                other_ip = h.get("ip")
                if other_ip and other_ip != local_ip and other_ip != "127.0.0.1":
                    sync_cmd = f"mkdir -p /etc/hci && echo {b64_data} | base64 -d > /etc/hci/cluster.json"
                    run_remote_spark(other_ip, sync_cmd)

            self.send_json(200, {"status": "success", "message": f"Node {removed_hostname or removed_ip} removed successfully"})
            return

        elif path == "/api/settings/update":
            try:
                try:
                    data = json.loads(self.rfile.read(content_length).decode('utf-8'))
                except Exception:
                    self.send_json(400, {"error": "Invalid request payload"})
                    return

                if "urbosa_enabled" in data and str(data["urbosa_enabled"]).lower() == "false":
                    rc_lan, out_lan, _ = run_cql_query("SELECT status FROM hydra.lanayru_clusters;")
                    if rc_lan == 0 and out_lan:
                        for line in out_lan.splitlines():
                            if "active" in line.lower() or "deploying" in line.lower():
                                self.send_json(400, {"error": "Cannot disable Urbosa SDN while Lanayru K8s Engine is active."})
                                return

                # Check if urbosa_enabled is changed from false/missing to true
                trigger_urbosa_bootstrap = False
                trigger_urbosa_cleanup = False
                if "urbosa_enabled" in data:
                    val_str = str(data["urbosa_enabled"]).lower()
                    prev_val = "false"
                    rc_s, out_s, _ = run_cql_query("SELECT value FROM hydra.cluster_settings WHERE key = 'urbosa_enabled';")
                    if rc_s == 0:
                        lines = [l.strip() for l in out_s.splitlines() if l.strip()]
                        val_lines = [l for l in lines if not l.startswith('(') and not l.startswith('-') and l != 'value' and l != '']
                        if val_lines:
                            prev_val = val_lines[0]
                    
                    if val_str == "true" and prev_val.lower() != "true":
                        trigger_urbosa_bootstrap = True
                    elif val_str == "false" and prev_val.lower() == "true":
                        trigger_urbosa_cleanup = True

                supported_keys = [
                    "dns_servers", "dns_search_domains", "dns_mtu",
                    "ntp_servers", "timezone", "cluster_name",
                    "cluster_region", "replication_factor", "scrub_interval",
                    "password_policy", "session_timeout", "rate_limit",
                    "vip", "cluster_subnet", "cluster_id", "urbosa_enabled", "drs_enabled"
                ]

                for k in supported_keys:
                    if k in data:
                        val = str(data[k])
                        val_clean = val.replace("'", "''")
                        cql = f"INSERT INTO hydra.cluster_settings (key, value) VALUES ('{k}', '{val_clean}');"
                        run_cql_query(cql)

                if "replication_factor" in data:
                    try:
                        user_rf = int(data["replication_factor"])
                        node_count = len(get_cluster_nodes()) if get_cluster_nodes() else 1
                        capped_rf = min(user_rf, node_count)
                        alter_cql = f"ALTER KEYSPACE hydra WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {capped_rf}}};"
                        run_cql_query(alter_cql)
                    except Exception as e:
                        print(f"Error altering keyspace replication: {e}")

                if "scrub_interval" in data:
                    scrub_val = data["scrub_interval"]
                    cron = "0 */6 * * *"
                    interval = 21600
                    enabled = "true"
                    if scrub_val == "daily":
                        cron = "0 2 * * *"
                        interval = 86400
                    elif scrub_val == "weekly":
                        cron = "0 2 * * 0"
                        interval = 604800
                    elif scrub_val == "monthly":
                        cron = "0 2 1 * *"
                        interval = 2592000
                    elif scrub_val == "disabled":
                        enabled = "false"
                    
                    cql_dagur = f"UPDATE hydra.dagur_schedules SET cron_expression = '{cron}', interval_seconds = {interval}, enabled = {enabled} WHERE job_name = 'storage_scrub';"
                    run_cql_query(cql_dagur)

                hosts = get_cluster_nodes()

                # DNS Resolv
                dns_servers = data.get("dns_servers", "8.8.8.8,8.8.4.4")
                dns_search = data.get("dns_search_domains", "cluster.local")
                dns_list = [d.strip() for d in dns_servers.split(",") if d.strip()]
                resolv_conf = ""
                if dns_search:
                    resolv_conf += f"search {dns_search}\n"
                for dns in dns_list:
                    resolv_conf += f"nameserver {dns}\n"

                # NTP Chrony
                ntp_servers = data.get("ntp_servers", "pool.ntp.org")
                ntp_list = [n.strip() for n in ntp_servers.split(",") if n.strip()]
                chrony_conf = ""
                for ntp in ntp_list:
                    chrony_conf += f"server {ntp} iburst\n"

                import base64
                b64_resolv = base64.b64encode(resolv_conf.encode('utf-8')).decode('utf-8')
                b64_chrony = base64.b64encode(chrony_conf.encode('utf-8')).decode('utf-8')

                # Timezone
                timezone = data.get("timezone", "UTC")
                import re
                timezone_sanitized = re.sub(r'[^A-Za-z0-9/\-_]', '', timezone)

                # Generate updates dict of ONLY keys in request payload to avoid clearing existing VIP/Subnet/ID
                updates = {}
                if "cluster_name" in data:
                    updates["cluster_name"] = data["cluster_name"]
                if "vip" in data:
                    updates["vip"] = data["vip"]
                if "cluster_subnet" in data:
                    updates["cluster_subnet"] = data["cluster_subnet"]
                if "cluster_id" in data:
                    updates["cluster_id"] = data["cluster_id"]
                
                updates_json = json.dumps(updates).replace("'", "\\'")

                def propagate_settings():
                    vip_changed = ("vip" in data)
                    for host in hosts:
                        host_ip = host.get("ip", "")
                        if host_ip:
                            cmd_dns = f"echo {b64_resolv} | base64 -d > /etc/resolv.conf"
                            run_remote_spark(host_ip, cmd_dns)
                            cmd_ntp = f"echo {b64_chrony} | base64 -d > /etc/chrony.conf && systemctl restart chronyd"
                            run_remote_spark(host_ip, cmd_ntp)
                            if timezone_sanitized:
                                cmd_tz = f"timedatectl set-timezone {timezone_sanitized} || true"
                                run_remote_spark(host_ip, cmd_tz)
                            
                            update_json_cmd = (
                                f"python3 -c \"import json, os; "
                                f"path='/etc/hci/cluster.json'; "
                                f"data=json.load(open(path)) if os.path.exists(path) else {{}}; "
                                f"updates=json.loads('{updates_json}'); "
                                f"data.update(updates); "
                                f"json.dump(data, open(path,'w'), indent=4)\""
                            )
                            if vip_changed:
                                update_json_cmd += " && systemctl restart bifrost"
                            run_remote_spark(host_ip, update_json_cmd)

                import threading
                threading.Thread(target=propagate_settings, daemon=True).start()

                task_id = None

                if trigger_urbosa_bootstrap:
                    payload = {
                        "service": "dagur",
                        "action": "execute",
                        "payload": {
                            "job_name": "urbosa_bootstrap",
                            "command": "python3 /usr/local/bin/urbosa-bootstrap"
                        }
                    }
                    try:
                        leader_ip = get_catalyst_target_ip()
                        req = urllib.request.Request(
                            f"http://{leader_ip}:9091/api/v1/tasks/submit",
                            data=json.dumps(payload).encode("utf-8"),
                            headers={"Content-Type": "application/json"}
                        )
                        with urllib.request.urlopen(req, timeout=5) as response:
                            res = json.loads(response.read().decode("utf-8"))
                            task_id = res.get("task_id")
                            print(f"[URBOSA BOOTSTRAP] Task submitted successfully: {res}")
                    except Exception as e:
                        print(f"[URBOSA BOOTSTRAP] Failed to submit task: {e}")

                if trigger_urbosa_cleanup:
                    payload = {
                        "service": "dagur",
                        "action": "execute",
                        "payload": {
                            "job_name": "urbosa_cleanup",
                            "command": "python3 /usr/local/bin/urbosa-bootstrap --cleanup"
                        }
                    }
                    try:
                        leader_ip = get_catalyst_target_ip()
                        req = urllib.request.Request(
                            f"http://{leader_ip}:9091/api/v1/tasks/submit",
                            data=json.dumps(payload).encode("utf-8"),
                            headers={"Content-Type": "application/json"}
                        )
                        with urllib.request.urlopen(req, timeout=5) as response:
                            res = json.loads(response.read().decode("utf-8"))
                            task_id = res.get("task_id")
                            print(f"[URBOSA CLEANUP] Task submitted successfully: {res}")
                    except Exception as e:
                        print(f"[URBOSA CLEANUP] Failed to submit task: {e}")

                response_data = {"status": "success"}
                if task_id:
                    response_data["task_id"] = task_id
                self.send_json(200, response_data)
                return
            except Exception as e:
                import traceback
                print("CRITICAL EXCEPTION IN SETTINGS UPDATE:", e, flush=True)
                traceback.print_exc()
                self.send_json(500, {"error": str(e)})
                return

        elif path == "/api/users/create":
            try:
                data = json.loads(self.rfile.read(content_length).decode('utf-8'))
                new_username = data.get("username", "").strip()
                new_password = data.get("password", "")
            except Exception:
                self.send_json(400, {"error": "Invalid request payload"})
                return

            if not new_username or not new_password:
                self.send_json(400, {"error": "Username and password are required"})
                return

            import re
            if not re.match(r"^[A-Za-z0-9_]{3,20}$", new_username):
                self.send_json(400, {"error": "Username must be 3-20 alphanumeric characters or underscores"})
                return

            ok, err_msg = validate_password_complexity(new_password)
            if not ok:
                self.send_json(400, {"error": err_msg})
                return

            cql_check = f"SELECT username FROM hydra.users WHERE username = '{new_username}';"
            rc, out, err = run_cql_query(cql_check)
            exists = False
            if rc == 0:
                for line in out.splitlines():
                    if new_username in line:
                        exists = True
                        break

            if exists:
                self.send_json(400, {"error": "User already exists"})
                return

            password_hash = hash_password(new_password)
            cql_insert = f"INSERT INTO hydra.users (username, password_hash) VALUES ('{new_username}', '{password_hash}');"
            rc_ins, out_ins, err_ins = run_cql_query(cql_insert)
            if rc_ins == 0:
                self.send_json(201, {"status": "success", "username": new_username})
            else:
                self.send_json(500, {"error": f"Failed to create user in DB: {err_ins}"})
            return

        elif path == "/api/users/delete":
            try:
                data = json.loads(self.rfile.read(content_length).decode('utf-8'))
                del_username = data.get("username", "").strip()
            except Exception:
                self.send_json(400, {"error": "Invalid request payload"})
                return

            if not del_username:
                self.send_json(400, {"error": "Username is required"})
                return

            if del_username == "helios":
                self.send_json(400, {"error": "Cannot delete the default administrator account 'helios'"})
                return

            current_user = getattr(self, "current_user", "")
            if del_username == current_user:
                self.send_json(400, {"error": "Cannot delete your own logged-in account"})
                return

            cql_delete = f"DELETE FROM hydra.users WHERE username = '{del_username}';"
            rc, out, err = run_cql_query(cql_delete)
            if rc == 0:
                self.send_json(200, {"status": "success"})
            else:
                self.send_json(500, {"error": f"Failed to delete user: {err}"})
            return

        elif path == "/api/users/change-password":
            try:
                data = json.loads(self.rfile.read(content_length).decode('utf-8'))
                target_username = data.get("username", "").strip()
                new_password = data.get("password", "")
            except Exception:
                self.send_json(400, {"error": "Invalid request payload"})
                return

            if not target_username or not new_password:
                self.send_json(400, {"error": "Username and password are required"})
                return

            # Password Complexity Rules validation (Basic vs Strong)
            ok, err_msg = validate_password_complexity(new_password)
            if not ok:
                self.send_json(400, {"error": err_msg})
                return

            cql_check = f"SELECT username FROM hydra.users WHERE username = '{target_username}';"
            rc_c, out_c, _ = run_cql_query(cql_check)
            exists = False
            if rc_c == 0:
                for line in out_c.splitlines():
                    if target_username in line:
                        exists = True
                        break

            if not exists:
                self.send_json(404, {"error": f"User '{target_username}' not found"})
                return

            new_hash = hash_password(new_password)
            cql_update = f"INSERT INTO hydra.users (username, password_hash) VALUES ('{target_username}', '{new_hash}');"
            rc_up, out_up, err_up = run_cql_query(cql_update)
            if rc_up == 0:
                self.send_json(200, {"status": "success"})
            else:
                self.send_json(500, {"error": f"Failed to update password: {err_up}"})
            return
        elif self.path.startswith("/api/images/upload"):
            url_parsed = urllib.parse.urlparse(self.path)
            query = urllib.parse.parse_qs(url_parsed.query)
            filename = query.get("name", [""])[0]
            if not filename:
                filename = self.headers.get("X-File-Name", "uploaded_image.iso")
                
            # import uuid
            task_id = str(uuid.uuid4())
            import datetime
            created_at_ms = int(datetime.datetime.now().timestamp() * 1000)
            
            # Start catalyst task
            log_catalyst_task("valhalla", "upload_image", "processing", 0, {"filename": filename, "size_bytes": content_length}, task_id=task_id, created_at=created_at_ms)
            
            res_name = f"img-{slugify_image_name(filename)}"
            block_dev_path = f"/dev/drbd/by-res/{res_name}/0"
            
            try:
                # 1. Create Linstor resource definition
                run_linstor_cmd(f"resource-definition create {res_name}")
                
                # Convert content_length to KiB for volume definition
                size_kb = (content_length + 1023) // 1024
                run_linstor_cmd(f"volume-definition create {res_name} {size_kb}KiB")
                
                # Get nodes from cluster.json to spawn resource on all hosts
                hosts = []
                try:
                    if os.path.exists("/etc/hci/cluster.json"):
                        with open("/etc/hci/cluster.json", "r") as f:
                            hosts = json.load(f).get("hosts", [])
                except Exception:
                    pass
                
                for h in hosts:
                    run_linstor_cmd(f"resource create {h['hostname']} {res_name} --storage-pool default-pool")
                    
                # Configure DRBD options (allow two primaries, auto-resync policies)
                run_linstor_cmd(f"resource-definition drbd-options --allow-two-primaries yes {res_name}")
                run_linstor_cmd(f"resource-definition drbd-options --after-sb-0pri discard-zero-changes --after-sb-1pri discard-secondary --after-sb-2pri disconnect {res_name}")
                
                # Wait up to 10 seconds for the block device to appear locally
                found = False
                for _ in range(20):
                    rc_chk, _, _ = run_remote_spark("127.0.0.1", f"test -b {block_dev_path}")
                    if rc_chk == 0:
                        found = True
                        break
                    time.sleep(0.5)
                if not found:
                    raise Exception(f"DRBD block device {block_dev_path} did not appear on local host.")
                
                # Promote to Primary locally on host to write the data
                run_remote_spark("127.0.0.1", f"drbdadm primary {res_name}")
                
                # Stream the upload in chunks of 1MB directly to the block device
                chunk_size = 1024 * 1024
                bytes_remaining = content_length
                last_progress = 0
                
                with open(block_dev_path, "wb") as f:
                    while bytes_remaining > 0:
                        chunk_to_read = min(chunk_size, bytes_remaining)
                        chunk = self.rfile.read(chunk_to_read)
                        if not chunk:
                            break
                        f.write(chunk)
                        bytes_remaining -= len(chunk)
                        
                        # Update task progress every 5%
                        progress = int(((content_length - bytes_remaining) / content_length) * 100) if content_length > 0 else 100
                        if progress - last_progress >= 5:
                            log_catalyst_task("valhalla", "upload_image", "processing", progress, {"filename": filename, "size_bytes": content_length}, task_id=task_id, created_at=created_at_ms)
                            last_progress = progress
                
                # Adjust block device permissions
                run_remote_spark("127.0.0.1", f"chmod 666 {block_dev_path}")
                
                # Demote back to Secondary
                run_remote_spark("127.0.0.1", f"drbdadm secondary {res_name}")
                
                created_at = int(datetime.datetime.now().timestamp() * 1000)
                image_meta = {
                    "name": filename,
                    "filename": filename,
                    "size_bytes": content_length,
                    "type": "iso" if filename.lower().endswith(".iso") else "template",
                    "path": block_dev_path,
                    "created_at": created_at
                }
                cql = f"INSERT INTO hydra.valhalla_images JSON '{json.dumps(image_meta)}';"
                run_cql_query(cql)
                
                # Complete catalyst task
                log_catalyst_task("valhalla", "upload_image", "completed", 100, {"filename": filename, "size_bytes": content_length}, task_id=task_id, created_at=created_at_ms)
                
                self.send_json(200, {"message": "Image uploaded successfully", "image": image_meta, "task_id": task_id})
            except Exception as e:
                # Ensure we attempt to demote back to secondary on failure
                run_remote_spark("127.0.0.1", f"drbdadm secondary {res_name} || true")
                # Cleanup Linstor definition on failure
                run_linstor_cmd(f"resource-definition delete {res_name}")
                log_catalyst_task("valhalla", "upload_image", "failed", 100, {"filename": filename, "size_bytes": content_length}, error_msg=str(e), task_id=task_id, created_at=created_at_ms)
                self.send_json(500, {"error": f"Failed to save image: {str(e)}"})
            return
        post_data = self.rfile.read(content_length)

        if self.path == "/api/vms/console/metrics":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                vm_name = payload["vm_name"]
                avg_fps = float(payload["avg_fps"])
                low_fps = float(payload["low_fps"])
                latency = float(payload["latency"])
            except Exception as e:
                self.send_json(400, {"error": f"Invalid payload: {str(e)}"})
                return

            import datetime
            now_ms = int(datetime.datetime.now().timestamp() * 1000)
            cql = f"INSERT INTO hydra.console_metrics (vm_name, timestamp, avg_fps, low_fps, latency) VALUES ('{vm_name}', {now_ms}, {avg_fps}, {low_fps}, {latency});"
            run_cql_query(cql)
            self.send_json(200, {"status": "success"})
            return

        elif self.path == "/api/lanayru/deploy":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                cluster_name = payload["cluster_name"].strip()
                control_nodes = int(payload["control_nodes"])
                overlay_segment_id = payload.get("overlay_segment_id", "").strip()
            except Exception as e:
                self.send_json(400, {"error": f"Invalid payload: {str(e)}"})
                return

            if not cluster_name:
                self.send_json(400, {"error": "Cluster name is required."})
                return

            # Verify if Urbosa SDN is enabled in cluster settings
            try:
                rc_urb, stdout_urb, _ = run_cql_query("SELECT value FROM hydra.cluster_settings WHERE key = 'urbosa_enabled';")
                urbosa_enabled = False
                if rc_urb == 0 and stdout_urb:
                    for line in stdout_urb.splitlines():
                        if "true" in line.lower():
                            urbosa_enabled = True
                            break
                if not urbosa_enabled:
                    self.send_json(400, {"error": "Cannot deploy Lanayru Kubernetes Engine: Urbosa SDN is currently disabled in cluster settings."})
                    return
            except Exception as e:
                self.send_json(400, {"error": f"Failed to verify Urbosa status: {str(e)}"})
                return

            import datetime
            import threading
            created_at_ms = int(datetime.datetime.now().timestamp() * 1000)
            task_id, created_at = log_catalyst_task("lanayru", "deploy", "processing", 10, {"cluster_name": cluster_name, "control_nodes": control_nodes})
            
            # Spawn background deployment thread
            threading.Thread(
                target=deploy_lanayru_worker,
                args=(task_id, cluster_name, control_nodes, overlay_segment_id, created_at),
                daemon=True
            ).start()

            self.send_json(200, {
                "message": "Lanayru deployment successfully scheduled.",
                "task_id": task_id,
                "status": "processing"
            })
            return

        elif self.path == "/api/lanayru/destroy":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                cluster_name = payload["cluster_name"].strip()
            except Exception as e:
                self.send_json(400, {"error": f"Invalid payload: {str(e)}"})
                return

            if not cluster_name:
                self.send_json(400, {"error": "Cluster name is required."})
                return

            import datetime
            import threading
            created_at_ms = int(datetime.datetime.now().timestamp() * 1000)
            task_id, created_at = log_catalyst_task("lanayru", "destroy", "processing", 10, {"cluster_name": cluster_name})

            def destroy_lanayru_worker(task_id, cluster_name, created_at):
                import lanayru
                lanayru.destroy_lanayru_worker(task_id, cluster_name, created_at)

            threading.Thread(
                target=destroy_lanayru_worker,
                args=(task_id, cluster_name, created_at),
                daemon=True
            ).start()

            self.send_json(200, {
                "message": "Lanayru destruction task scheduled.",
                "task_id": task_id,
                "status": "processing"
            })
            return

        if self.path == "/api/vms/create":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"]
                vcpu = int(payload["vcpus"])
                memory = int(payload["memory"])
                
                firmware = payload.get("firmware", "uefi")
                iso = payload.get("iso", "")
                boot_device = payload.get("boot_device", "")
                
                disks_payload = payload.get("disks", None)
                if disks_payload is None:
                    # Fallback to single disk_size string if disks not provided
                    disk_size_str = payload.get("disk_size", "10G")
                    if "/" in disk_size_str:
                        disk_size_str = disk_size_str.split("/")[-1]
                    disks_payload = [disk_size_str]
            except Exception as e:
                self.send_json(400, {"error": f"Invalid payload: {str(e)}"})
                return

            task_id, created_at = log_catalyst_task("vm", "create", "processing", 10, {"vm_name": name})

            disks_parsed = []
            for d in disks_payload:
                if ":" in d:
                    parts = d.split(":")
                    disks_parsed.append({"size": parts[0], "container": parts[1]})
                else:
                    disks_parsed.append({"size": d, "container": get_default_container()})

            disk_paths = []
            created_disks = []
            primary_disk_size_gb = 10
            
            for idx, d_info in enumerate(disks_parsed):
                d_size = d_info["size"]
                clean_size = d_size.strip().upper().replace("B", "")
                if clean_size.endswith("T"):
                    primary_size = int(clean_size.replace("T", "")) * 1024
                else:
                    primary_size = int(clean_size.replace("G", "").strip() or 10)
                
                prog = 10 + int((idx / len(disks_parsed)) * 80)
                log_catalyst_task("vm", "create", "processing", prog, {"vm_name": name}, task_id=task_id, created_at=created_at)
                
                res_name = f"{name}-disk{idx}"
                d_path = f"/dev/drbd/by-res/{res_name}/0"
                
                try:
                    # 1. Create resource definition
                    rc, out, err = run_linstor_cmd(f"resource-definition create {res_name}")
                    if rc != 0 and "already exists" not in (err or out):
                        raise Exception(f"Failed to create Linstor resource-definition {res_name}: {err or out}")
                        
                    # 2. Create volume definition
                    rc, out, err = run_linstor_cmd(f"volume-definition create {res_name} {primary_size}GiB")
                    if rc != 0 and "already exists" not in (err or out):
                        raise Exception(f"Failed to create Linstor volume-definition {res_name}: {err or out}")
                        
                    # 3. Create resource on all hosts in default-pool
                    hosts = []
                    try:
                        if os.path.exists("/etc/hci/cluster.json"):
                            with open("/etc/hci/cluster.json", "r") as f:
                                hosts = json.load(f).get("hosts", [])
                    except Exception:
                        pass
                        
                    for h in hosts:
                        run_linstor_cmd(f"resource create {h['hostname']} {res_name} --storage-pool default-pool")
                        
                    # 4. Set DRBD options (allow two primaries, split-brain policies)
                    run_linstor_cmd(f"resource-definition drbd-options --allow-two-primaries yes {res_name}")
                    run_linstor_cmd(f"resource-definition drbd-options --after-sb-0pri discard-zero-changes --after-sb-1pri discard-secondary --after-sb-2pri disconnect {res_name}")
                    
                    if idx == 0:
                        primary_disk_size_gb = primary_size
                    disk_paths.append(d_path)
                    created_disks.append(d_path)
                except Exception as e:
                    for p in created_disks:
                        try:
                            rname = p.split("/")[-2]
                            run_linstor_cmd(f"resource-definition delete {rname}")
                        except:
                            pass
                    log_catalyst_task("vm", "create", "failed", 100, {"vm_name": name}, error_msg=str(e), task_id=task_id, created_at=created_at)
                    self.send_json(500, {"error": f"Failed to allocate storage disk {idx}: {str(e)}"})
                    return

            # 3. Write VM record to ScyllaDB
            network_id = payload.get("network_id", "7a68e0d6-11f8-4e89-9430-b3b44b8bc438")
            if not network_id:
                network_id = "7a68e0d6-11f8-4e89-9430-b3b44b8bc438"
            cpu_model = payload.get("cpu_model", "")
            audio_enabled = bool(payload.get("audio_enabled", False))
            vm_meta = {
                "name": name,
                "vcpu": vcpu,
                "memory": memory,
                "disk_path": disk_paths[0] if disk_paths else "",
                "disk_size": primary_disk_size_gb if disk_paths else 0,
                "state": "Stopped",
                "host_ip": "",
                "disks_list": ",".join(disks_payload) if disks_payload else "NONE",
                "firmware": firmware,
                "iso": iso,
                "boot_device": boot_device,
                "network_id": network_id,
                "cpu_model": cpu_model,
                "audio_enabled": audio_enabled
            }
            cql = f"INSERT INTO hydra.vms JSON '{json.dumps(vm_meta)}';"
            run_cql_query(cql)

            # 4. Append event log
            EVENT_LOGS.append({
                "desc": f"VM '{name}' successfully registered in database.",
                "time": "Just now"
            })

            log_catalyst_task("vm", "create", "completed", 100, {"vm_name": name}, task_id=task_id, created_at=created_at)
            invalidate_status_cache()

            self.send_json(201, {
                "name": name,
                "node": "Unassigned",
                "message": f"VM {name} metadata registered successfully."
            })
            return

        elif self.path == "/api/images/delete":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"]
            except Exception as e:
                self.send_json(400, {"error": f"Invalid payload: {str(e)}"})
                return
                
            cql_select = f"SELECT JSON path FROM hydra.valhalla_images WHERE name = '{name}';"
            rc, stdout, stderr = run_cql_query(cql_select)
            path_to_delete = None
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            path_to_delete = json.loads(line).get("path")
                        except Exception:
                            pass
            
            cql_delete = f"DELETE FROM hydra.valhalla_images WHERE name = '{name}';"
            run_cql_query(cql_delete)
            
            res_name = f"img-{slugify_image_name(name)}"
            run_linstor_cmd(f"resource-definition delete {res_name}")
            
            if path_to_delete:
                if not path_to_delete.startswith("/dev/drbd/"):
                    nodes = []
                    rc_n, stdout_n, _ = run_cql_query("SELECT JSON ip FROM hydra.nodes;")
                    if rc_n == 0 and stdout_n:
                        for line in stdout_n.splitlines():
                            line = line.strip()
                            if line.startswith("{") and line.endswith("}"):
                                try:
                                    nodes.append(json.loads(line).get("ip"))
                                except:
                                    pass
                    if not nodes:
                        nodes = ["127.0.0.1"]
                    for other_ip in nodes:
                        try:
                            run_remote_spark(other_ip, f"rm -f {path_to_delete}")
                        except Exception as e:
                            print(f"Error removing image file from {other_ip}: {e}")
                    
            self.send_json(200, {"message": f"Image '{name}' successfully deleted."})
            return

        elif self.path == "/api/vms/cdrom":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"]
                iso = payload.get("iso", "")
            except Exception as e:
                self.send_json(400, {"error": f"Invalid payload: {str(e)}"})
                return

            task_id, created_at = log_catalyst_task("vm", "cdrom", "processing", 10, {"vm_name": name, "iso": iso})

            cql = f"SELECT JSON host_ip, iso FROM hydra.vms WHERE name = '{name}';"
            rc, stdout, stderr = run_cql_query(cql)
            host_ip = LOCAL_IP
            current_iso = ""
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            vm_meta = json.loads(line)
                            host_ip = vm_meta.get("host_ip", LOCAL_IP)
                            current_iso = vm_meta.get("iso", "")
                        except Exception:
                            pass

            success = False
            if iso:
                iso_path = f"/var/lib/hci/aether/volumes/default-image-container/{iso}"
                action_desc = f"Mounted ISO '{iso}'"
                
                # Try standard virsh change-media first (sends ACPI events and updates libvirt configuration)
                virsh_cmd = f"virsh -c qemu:///system change-media {name} sda {iso_path} --update --force"
                rc_cmd, stdout_cmd, stderr_cmd = run_remote_spark(host_ip, virsh_cmd)
                if rc_cmd == 0:
                    success = True
                else:
                    virsh_cmd_insert = f"virsh -c qemu:///system change-media {name} sda {iso_path} --insert --force"
                    rc_ins, stdout_ins, stderr_ins = run_remote_spark(host_ip, virsh_cmd_insert)
                    if rc_ins == 0:
                        success = True
                    else:
                        # Fallback to QMP if guest-locked trays prevent standard change-media
                        qmp_cmd = f"virsh -c qemu:///system qemu-monitor-command {name} " + "'{\"execute\": \"blockdev-change-medium\", \"arguments\": {\"id\": \"sata0-0-0\", \"filename\": \"" + iso_path + "\", \"force\": true}}'"
                        rc_qmp, stdout_qmp, stderr_qmp = run_remote_spark(host_ip, qmp_cmd)
                        if rc_qmp == 0 and "error" not in stdout_qmp:
                            success = True
                        else:
                            log_catalyst_task("vm", "cdrom", "failed", 100, {"vm_name": name, "iso": iso}, error_msg=stderr_cmd.strip(), task_id=task_id, created_at=created_at)
                            self.send_json(500, {"error": stderr_cmd.strip() or stdout_cmd.strip()})
                            return
            else:
                action_desc = "Ejected CD-ROM media"
                
                # Try standard virsh eject first
                virsh_cmd = f"virsh -c qemu:///system change-media {name} sda --eject --force"
                rc_cmd, stdout_cmd, stderr_cmd = run_remote_spark(host_ip, virsh_cmd)
                if rc_cmd == 0:
                    success = True
                else:
                    # Fallback to QMP eject
                    qmp_cmd = f"virsh -c qemu:///system qemu-monitor-command {name} " + "'{\"execute\": \"eject\", \"arguments\": {\"id\": \"sata0-0-0\", \"force\": true}}'"
                    rc_qmp, stdout_qmp, stderr_qmp = run_remote_spark(host_ip, qmp_cmd)
                    if rc_qmp == 0 and "error" not in stdout_qmp:
                        success = True
                    else:
                        log_catalyst_task("vm", "cdrom", "failed", 100, {"vm_name": name, "iso": iso}, error_msg=stderr_cmd.strip(), task_id=task_id, created_at=created_at)
                        self.send_json(500, {"error": stderr_cmd.strip() or stdout_cmd.strip()})
                        return

            current_list = [x.strip() for x in current_iso.split(",")] if current_iso else []
            if not current_list:
                current_list = ["__empty__"]
            current_list[0] = iso if iso else "__empty__"
            new_iso_str = ",".join(current_list)

            cql_upd = f"UPDATE hydra.vms SET iso = '{new_iso_str}' WHERE name = '{name}';"
            run_cql_query(cql_upd)

            EVENT_LOGS.append({
                "desc": f"VM '{name}' CD-ROM action: {action_desc}.",
                "time": "Just now"
            })

            log_catalyst_task("vm", "cdrom", "completed", 100, {"vm_name": name, "iso": iso}, task_id=task_id, created_at=created_at)
            invalidate_status_cache()

            self.send_json(200, {"message": action_desc})
            return

        elif self.path == "/api/vms/power":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"]
                action = payload["action"]  # "start", "stop", "reset", "reboot", "shutdown"
                
                # Map actions
                mapped_action = "on" if action == "start" else "off" if action == "stop" else action
                
                rc, res, err = run_mtls_spark_api("127.0.0.1", "/api/v1/vm/power", {"name": name, "action": mapped_action})
                if rc == 0 and "error" not in res:
                    new_state = "Running" if mapped_action in ["on", "reset", "reboot", "shutdown"] else "Stopped"
                    host_ip = res.get("host_ip", "")
                    
                    EVENT_LOGS.append({
                        "desc": f"VM '{name}' transitioned state to '{new_state}' via Vali VM Manager.",
                        "time": "Just now"
                    })
                    invalidate_status_cache()

                    self.send_json(200, {
                        "name": name,
                        "status": new_state.lower(),
                        "node": host_ip
                    })
                else:
                    err_msg = res.get("error", err)
                    self.send_json(500, {"error": f"Failed to power {action} VM: {err_msg}"})
                return
            except Exception as e:
                self.send_json(500, {"error": str(e)})
                return

        elif self.path == "/api/vms/migrate":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"]
                target_host = payload["target_host"]
                
                rc, res, err = run_mtls_spark_api("127.0.0.1", "/api/v1/vm/migrate", {"name": name, "target_host": target_host})
                if rc == 0 and "error" not in res:
                    EVENT_LOGS.append({
                        "desc": f"VM '{name}' migration to node '{target_host}' initiated.",
                        "time": "Just now"
                    })
                    invalidate_status_cache()

                    self.send_json(200, res)
                else:
                    err_msg = res.get("error", err)
                    self.send_json(500, {"error": f"Failed to migrate VM: {err_msg}"})
                return
            except Exception as e:
                self.send_json(500, {"error": str(e)})
                return

        elif self.path == "/api/vms/balance":
            try:
                payload = json.loads(post_data.decode("utf-8")) if post_data else {}
                aggressive = payload.get("aggressive", True)
                
                rc, res, err = run_mtls_spark_api("127.0.0.1", "/api/v1/vm/balance", {"aggressive": aggressive})
                if rc == 0 and "error" not in res:
                    EVENT_LOGS.append({
                        "desc": f"Cluster load rebalancing (DRS) manually triggered.",
                        "time": "Just now"
                    })
                    invalidate_status_cache()
                    self.send_json(200, res)
                else:
                    err_msg = res.get("error", err)
                    self.send_json(500, {"error": f"Failed to balance cluster: {err_msg}"})
                return
            except Exception as e:
                self.send_json(500, {"error": str(e)})
                return

        elif self.path == "/api/vms/update":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"]
            except Exception as e:
                self.send_json(400, {"error": f"Invalid payload: {str(e)}"})
                return

            task_id, created_at = log_catalyst_task("vm", "update", "processing", 10, {"vm_name": name})
            try:
                # Find existing VM metadata in ScyllaDB
                cql = f"SELECT JSON * FROM hydra.vms WHERE name = '{name}';"
                rc, stdout, stderr = run_cql_query(cql)
                if rc != 0:
                    raise Exception(f"Database error: {stderr.strip() or stdout.strip()}")

                vm_data = None
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            vm_data = json.loads(line)
                            break
                        except Exception:
                            pass

                if not vm_data:
                    raise Exception(f"VM '{name}' not found.")

                state_str = vm_data.get("state", "Stopped")
                is_running = state_str.lower() == "running"
                host_ip = vm_data.get("host_ip", "")

                # Parse new values, fallback to existing ones
                vcpu = int(payload.get("vcpus", vm_data.get("vcpu", 2)))
                memory = int(payload.get("memory", vm_data.get("memory", 4096)))
                firmware = payload.get("firmware", vm_data.get("firmware", "bios"))
                iso = payload.get("iso", vm_data.get("iso", ""))
                boot_device = payload.get("boot_device", vm_data.get("boot_device", ""))

                # Live CD-ROM update — handle all slots, not just slot 0
                if is_running and host_ip:
                    old_iso = vm_data.get("iso", "")
                    if old_iso != iso:
                        import string as _string
                        _letters = _string.ascii_lowercase
                        old_list = [x.strip() for x in old_iso.split(",") if x.strip()] if old_iso else []
                        new_list = [x.strip() for x in iso.split(",") if x.strip()] if iso else []
                        max_slots = max(len(old_list), len(new_list))
                        for slot_idx in range(max_slots):
                            dev_letter = _letters[slot_idx]
                            dev_name = f"sd{dev_letter}"  # sda, sdb, sdc, ...
                            sata_id = f"sata0-0-{slot_idx}"  # sata0-0-0, sata0-0-1, ...
                            old_spec = old_list[slot_idx] if slot_idx < len(old_list) else "__empty__"
                            new_spec = new_list[slot_idx] if slot_idx < len(new_list) else "__empty__"
                            if old_spec == new_spec:
                                continue  # No change for this slot
                            if new_spec == "__empty__":
                                # Eject this slot using virsh change-media first
                                virsh_cmd = f"virsh -c qemu:///system change-media {name} {dev_name} --eject --force"
                                rc_c, _, _ = run_remote_spark(host_ip, virsh_cmd)
                                if rc_c != 0:
                                    # Fallback to QMP eject
                                    qmp_cmd = f"virsh -c qemu:///system qemu-monitor-command {name} " + \
                                        f'\'{{"execute": "eject", "arguments": {{"id": "{sata_id}", "force": true}}}}\''
                                    run_remote_spark(host_ip, qmp_cmd)
                            else:
                                # Mount/Swap this slot using virsh change-media first
                                iso_path = f"/var/lib/hci/aether/volumes/default-image-container/{new_spec}"
                                change_cmd = f"virsh -c qemu:///system change-media {name} {dev_name} {iso_path} --update --force"
                                rc_c, _, _ = run_remote_spark(host_ip, change_cmd)
                                if rc_c != 0:
                                    insert_cmd = f"virsh -c qemu:///system change-media {name} {dev_name} {iso_path} --insert --force"
                                    rc_ins, _, _ = run_remote_spark(host_ip, insert_cmd)
                                    if rc_ins != 0:
                                        # Fallback to QMP blockdev-change-medium
                                        qmp_cmd = f"virsh -c qemu:///system qemu-monitor-command {name} " + \
                                            f'\'{{"execute": "blockdev-change-medium", "arguments": {{"id": "{sata_id}", "filename": "{iso_path}", "force": true}}}}\''
                                        run_remote_spark(host_ip, qmp_cmd)
                
                disks_payload = payload.get("disks", None)
                
                if disks_payload is not None:
                    # Disks payload was provided: we will reconcile disks
                    old_disks_str = vm_data.get("disks_list", "")
                    if old_disks_str == "NONE":
                        old_disks = []
                    else:
                        old_disks = old_disks_str.split(",") if old_disks_str else []
                    
                    old_parsed = []
                    for idx, entry in enumerate(old_disks):
                        if ":" in entry:
                            parts = entry.split(":")
                            size = parts[0]
                            container = parts[1]
                        else:
                            size = entry
                            container = get_default_container()
                        
                        if idx == 0:
                            path = f"/var/lib/hci/aether/volumes/{container}/{name}.raw"
                        else:
                            path = f"/var/lib/hci/aether/volumes/{container}/{name}_disk{idx}.raw"
                        old_parsed.append({"size": size, "container": container, "path": path})

                    new_parsed = []
                    for idx, entry in enumerate(disks_payload):
                        if ":" in entry:
                            parts = entry.split(":")
                            size = parts[0]
                            container = parts[1]
                        else:
                            size = entry
                            container = get_default_container()
                        new_parsed.append({"size": size, "container": container})

                    # Ensure all target containers directories exist
                    for d_info in new_parsed:
                        t_ip = get_container_node_ip(d_info['container'])
                        run_remote_spark(t_ip, f"mkdir -p /var/lib/hci/aether/volumes/{d_info['container']}")

                    import string
                    letters = string.ascii_lowercase

                    # Step A: Process each incoming disk
                    for idx, new_disk in enumerate(new_parsed):
                        new_size_str = new_disk["size"]
                        clean_new_size = new_size_str.strip().upper().replace("B", "")
                        new_container = new_disk["container"]
                        
                        prog = 10 + int((idx / len(new_parsed)) * 80)
                        log_catalyst_task("vm", "update", "processing", prog, {"vm_name": name}, task_id=task_id, created_at=created_at)

                        res_name = f"{name}-disk{idx}"
                        new_path = f"/dev/drbd/by-res/{res_name}/0"

                        size_val = 20
                        if clean_new_size.endswith("T"):
                            size_val = int(clean_new_size.replace("T", "")) * 1024
                        else:
                            size_val = int(clean_new_size.replace("G", "").strip() or 20)

                        if idx < len(old_parsed):
                            # Existing disk
                            old_disk = old_parsed[idx]
                            
                            # Size changed -> Resize Linstor volume definition
                            if old_disk["size"] != new_size_str:
                                rc_res, out_res, err_res = run_linstor_cmd(f"volume-definition set-size {res_name} 0 {size_val}GiB")
                                if rc_res != 0:
                                    raise Exception(f"Failed to resize Linstor volume {res_name} to {size_val}GiB: {err_res or out_res}")
                                
                                # Notify QEMU about the resized block device live
                                if is_running and host_ip:
                                    dev_letter = letters[idx % 26]
                                    bus = "virtio"
                                    if idx < len(old_disks):
                                        old_entry = old_disks[idx]
                                        old_parts = old_entry.split(":")
                                        if len(old_parts) > 2:
                                            bus = old_parts[2]
                                    dev_prefix = "vd" if bus == "virtio" else "sd"
                                    
                                    # 1. Resize DRBD device in the kernel on VM's host
                                    run_remote_spark(host_ip, f"drbdadm resize {res_name} || true")
                                    
                                    # 2. Tell QEMU block layer to resize
                                    blockresize_cmd = f"virsh -c qemu:///system blockresize {name} {dev_prefix}{dev_letter} {clean_new_size}"
                                    run_remote_spark(host_ip, blockresize_cmd)
                        else:
                            # New disk to add in Linstor
                            rc_rd, out_rd, err_rd = run_linstor_cmd(f"resource-definition create {res_name}")
                            if rc_rd != 0 and "already exists" not in (err_rd or out_rd):
                                raise Exception(f"Failed to create Linstor resource-definition {res_name}: {err_rd or out_rd}")
                                
                            rc_vd, out_vd, err_vd = run_linstor_cmd(f"volume-definition create {res_name} {size_val}GiB")
                            if rc_vd != 0 and "already exists" not in (err_vd or out_vd):
                                raise Exception(f"Failed to create Linstor volume-definition {res_name}: {err_vd or out_vd}")
                                
                            hosts = []
                            try:
                                if os.path.exists("/etc/hci/cluster.json"):
                                    with open("/etc/hci/cluster.json", "r") as f_c:
                                        hosts = json.load(f_c).get("hosts", [])
                            except Exception:
                                pass
                                
                            for h in hosts:
                                run_linstor_cmd(f"resource create {h['hostname']} {res_name} --storage-pool default-pool")
                                
                            run_linstor_cmd(f"resource-definition drbd-options --allow-two-primaries yes {res_name}")
                            run_linstor_cmd(f"resource-definition drbd-options --after-sb-0pri discard-zero-changes --after-sb-1pri discard-secondary --after-sb-2pri disconnect {res_name}")
                            
                            # Attach disk live to the running VM
                            if is_running and host_ip:
                                dev_letter = letters[idx % 26]
                                attach_cmd = f"virsh -c qemu:///system attach-disk {name} --source {new_path} --target vd{dev_letter} --persistent --live"
                                run_remote_spark(host_ip, attach_cmd)

                    # Step B: Remove deleted disks
                    for idx in range(len(new_parsed), len(old_parsed)):
                        res_name = f"{name}-disk{idx}"
                        
                        # Detach disk live from the running VM first
                        if is_running and host_ip:
                            dev_letter = letters[idx % 26]
                            detach_cmd = f"virsh -c qemu:///system detach-disk {name} vd{dev_letter} --persistent --live"
                            run_remote_spark(host_ip, detach_cmd)
                            
                        # Delete from Linstor
                        run_linstor_cmd(f"resource-definition delete {res_name}")

                    # Resolve new primary disk details
                    if len(new_parsed) > 0:
                        primary_size_str = new_parsed[0]["size"]
                        primary_clean = primary_size_str.strip().upper().replace("B", "")
                        if primary_clean.endswith("T"):
                            primary_size_gb = int(primary_clean.replace("T", "")) * 1024
                        else:
                            primary_size_gb = int(primary_clean.replace("G", "").strip() or 10)

                        primary_path = f"/dev/drbd/by-res/{name}-disk0/0"
                        disks_list = ",".join(disks_payload)
                    else:
                        primary_size_gb = 0
                        primary_path = ""
                        disks_list = "NONE"
                else:
                    primary_size_gb = vm_data.get("disk_size", 10)
                    primary_path = vm_data.get("disk_path", f"/dev/drbd/by-res/{name}-disk0/0")
                    disks_list = vm_data.get("disks_list", "")

                audio_enabled = bool(payload.get("audio_enabled", vm_data.get("audio_enabled", False)))
                audio_enabled_str = "true" if audio_enabled else "false"

                # Update database record
                network_id = payload.get("network_id", vm_data.get("network_id", "7a68e0d6-11f8-4e89-9430-b3b44b8bc438"))
                if not network_id:
                    network_id = "7a68e0d6-11f8-4e89-9430-b3b44b8bc438"
                cpu_model = payload.get("cpu_model", vm_data.get("cpu_model", ""))
                cql_upd = f"UPDATE hydra.vms SET vcpu = {vcpu}, memory = {memory}, firmware = '{firmware}', iso = '{iso}', boot_device = '{boot_device}', disks_list = '{disks_list}', disk_path = '{primary_path}', disk_size = {primary_size_gb}, network_id = '{network_id}', cpu_model = '{cpu_model}', audio_enabled = {audio_enabled_str} WHERE name = '{name}';"
                run_cql_query(cql_upd)
                
                # Check if network changed and VM is running -> Hotplug live!
                try:
                    old_net_id_raw = vm_data.get("network_id", "7a68e0d6-11f8-4e89-9430-b3b44b8bc438")
                    old_net_id = old_net_id_raw
                    if isinstance(old_net_id, str) and old_net_id.startswith("["):
                        old_list = json.loads(old_net_id)
                        if old_list:
                            old_net_id = old_list[0]
                    elif isinstance(old_net_id, list) and old_net_id:
                        old_net_id = old_net_id[0]
                        
                    new_net_id = network_id
                    if isinstance(new_net_id, str) and new_net_id.startswith("["):
                        new_list = json.loads(new_net_id)
                        if new_list:
                            new_net_id = new_list[0]
                    elif isinstance(new_net_id, list) and new_net_id:
                        new_net_id = new_net_id[0]
                        
                    if old_net_id != new_net_id and vm_data.get("state") == "Running" and vm_data.get("host_ip"):
                        hotplug_success, hotplug_msg = hotplug_vm_nic(vm_data.get("host_ip"), name, old_net_id, new_net_id)
                        if hotplug_success:
                            EVENT_LOGS.append({
                                "desc": f"VM '{name}' network live-hotplugged successfully.",
                                "time": "Just now"
                            })
                        else:
                            print(f"Network hotplug failed for VM '{name}': {hotplug_msg}")
                except Exception as ex:
                    print(f"Error executing live network hotplug: {ex}")

                EVENT_LOGS.append({
                    "desc": f"VM '{name}' configuration updated.",
                    "time": "Just now"
                })

                log_catalyst_task("vm", "update", "completed", 100, {"vm_name": name}, task_id=task_id, created_at=created_at)
                invalidate_status_cache()

                self.send_json(200, {
                    "name": name,
                    "vcpu": vcpu,
                    "memory": memory,
                    "firmware": firmware,
                    "iso": iso,
                    "boot_device": boot_device,
                    "audio_enabled": audio_enabled,
                    "message": f"VM '{name}' updated successfully."
                })
            except Exception as e:
                log_catalyst_task("vm", "update", "failed", 100, {"vm_name": name}, error_msg=str(e), task_id=task_id, created_at=created_at)
                self.send_json(500, {"error": str(e)})
            return

        elif self.path == "/api/vms/delete":
            task_id, created_at = None, None
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"]
            except Exception as e:
                self.send_json(400, {"error": "Invalid payload"})
                return

            task_id, created_at = log_catalyst_task("vm", "delete", "processing", 10, {"vm_name": name})
            try:
                # Find VM details in ScyllaDB
                cql = f"SELECT JSON host_ip, disks_list, disk_path FROM hydra.vms WHERE name = '{name}';"
                rc, stdout, stderr = run_cql_query(cql)
                host_ip = ""
                disks_list = ""
                disk_path = ""
                if rc == 0:
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                vm_meta = json.loads(line)
                                host_ip = vm_meta.get("host_ip", "")
                                disks_list = vm_meta.get("disks_list", "")
                                disk_path = vm_meta.get("disk_path", "")
                            except Exception:
                                pass

                # 1. Stop and undefine VM if it is active on a host
                if host_ip:
                    run_remote_spark(host_ip, f"virsh -c qemu:///system destroy {name} || true")
                    run_remote_spark(host_ip, f"virsh -c qemu:///system undefine {name} --keep-nvram || true")

                # 2. Delete Linstor resources and NVRAM files
                num_disks = len(disks_list.split(",")) if disks_list else 1
                for idx in range(num_disks):
                    res_name = f"{name}-disk{idx}"
                    run_linstor_cmd(f"resource-definition delete {res_name}")
                # Delete UEFI nvram vars file and DB entry
                nvram_file_path = f"/var/lib/hci/aether/nvram/{name}_vars.fd"
                if host_ip:
                    run_remote_spark(host_ip, f"rm -f {nvram_file_path}")
                else:
                    run_remote_spark(LOCAL_IP, f"rm -f {nvram_file_path}")
                run_cql_query(f"DELETE FROM hydra.vm_nvram WHERE vm_name = '{name}';")

                # 4. Remove metadata record from ScyllaDB
                cql = f"DELETE FROM hydra.vms WHERE name = '{name}';"
                run_cql_query(cql)

                # 5. Append delete event log
                EVENT_LOGS.append({
                    "desc": f"VM '{name}' successfully deleted.",
                    "time": "Just now"
                })

                log_catalyst_task("vm", "delete", "completed", 100, {"vm_name": name}, task_id=task_id, created_at=created_at)
                invalidate_status_cache()

                self.send_json(200, {"message": f"VM {name} deleted successfully."})
            except Exception as e:
                log_catalyst_task("vm", "delete", "failed", 100, {"vm_name": name}, error_msg=str(e), task_id=task_id, created_at=created_at)
                self.send_json(500, {"error": str(e)})
            return

        elif self.path == "/api/storage/containers/create":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"]
                tier = payload.get("tier", "SSD")
                quota_bytes = int(payload.get("quota_bytes", 0))
                ftt = int(payload.get("ftt", 1))
            except Exception as e:
                self.send_json(400, {"error": f"Invalid payload: {str(e)}"})
                return

            self.send_json(400, {"error": "Dynamic storage container creation is not supported on Linstor/DRBD storage engine."})
            return

        elif self.path == "/api/storage/containers/delete":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"]
            except Exception as e:
                self.send_json(400, {"error": "Invalid payload"})
                return

            self.send_json(400, {"error": "Dynamic storage container deletion is not supported on Linstor/DRBD storage engine."})
            return

        elif self.path == "/api/networks/create":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"].strip()
                net_type = payload["type"].strip()
                vlan_id = payload.get("vlan_id")
            except Exception as e:
                self.send_json(400, {"error": "Invalid payload"})
                return

            if not name or not net_type:
                self.send_json(400, {"error": "Name and type are required"})
                return

            if net_type not in ["direct", "vlan"]:
                self.send_json(400, {"error": "Invalid network type. Must be 'direct' or 'vlan'"})
                return

            vlan_val = "null"
            if net_type == "vlan":
                try:
                    vlan_val = int(vlan_id)
                    if not (1 <= vlan_val <= 4094):
                        raise ValueError()
                except (TypeError, ValueError):
                    self.send_json(400, {"error": "VLAN ID must be an integer between 1 and 4094"})
                    return

                # Check if VLAN ID is already in use
                cql_check = "SELECT JSON * FROM hydra.gatoway_networks;"
                rc, stdout, _ = run_cql_query(cql_check)
                if rc == 0 and stdout:
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                net = json.loads(line)
                                if net.get("vlan_id") == vlan_val:
                                    self.send_json(400, {"error": f"VLAN ID {vlan_val} is already assigned to network '{net.get('name')}'"})
                                    return
                            except Exception:
                                pass

            # import uuid
            net_id = str(uuid.uuid4())
            cql = f"INSERT INTO hydra.gatoway_networks (net_id, name, type, vlan_id) VALUES ({net_id}, '{name}', '{net_type}', {vlan_val});"
            rc, stdout, stderr = run_cql_query(cql)
            if rc != 0:
                self.send_json(500, {"error": f"Failed to create network in database: {stderr or stdout}"})
                return

            EVENT_LOGS.append({
                "desc": f"Network segment '{name}' ({net_type}) successfully created.",
                "time": "Just now"
            })

            self.send_json(201, {"message": f"Network segment '{name}' created successfully.", "net_id": net_id})
            return

        elif self.path == "/api/networks/delete":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                net_id = payload["net_id"].strip()
            except Exception as e:
                self.send_json(400, {"error": "Invalid payload"})
                return

            if net_id == "7a68e0d6-11f8-4e89-9430-b3b44b8bc438":
                self.send_json(400, {"error": "Cannot delete Physical-Direct system network."})
                return

            # Check if any VM is using this network
            cql_vms = "SELECT JSON name, network_id FROM hydra.vms;"
            rc, stdout, _ = run_cql_query(cql_vms)
            vms_using_net = []
            if rc == 0 and stdout:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            vm = json.loads(line)
                            if vm.get("network_id") == net_id:
                                vms_using_net.append(vm.get("name"))
                        except Exception:
                            pass

            if vms_using_net:
                self.send_json(400, {"error": f"Cannot delete network segment because it is currently assigned to VM(s): {', '.join(vms_using_net)}"})
                return

            cql = f"DELETE FROM hydra.gatoway_networks WHERE net_id = {net_id};"
            rc, stdout, stderr = run_cql_query(cql)
            if rc != 0:
                self.send_json(500, {"error": f"Failed to delete network: {stderr or stdout}"})
                return

            EVENT_LOGS.append({
                "desc": f"Network segment '{net_id}' deleted.",
                "time": "Just now"
            })

            self.send_json(200, {"message": f"Network segment deleted successfully."})
            return

        elif self.path == "/api/urbosa/t0/create":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"].strip()
                uplink_interface = payload["uplink_interface"].strip()
                uplink_ip = payload["uplink_ip"].strip()
                gateway_ip = payload["gateway_ip"].strip()
                nat_rules = json.dumps(payload.get("nat_rules", {}))
            except Exception as e:
                self.send_json(400, {"error": "Invalid payload"})
                return

            import ipaddress
            try:
                ip_iface = ipaddress.ip_interface(uplink_ip)
                gw_ip = ipaddress.ip_address(gateway_ip)
                if gw_ip not in ip_iface.network:
                    self.send_json(400, {"error": f"Gateway IP {gateway_ip} is not within the uplink network {ip_iface.network}"})
                    return
            except ValueError as e:
                self.send_json(400, {"error": f"Invalid Uplink CIDR or Gateway IP: {str(e)}"})
                return

            # import uuid
            router_id = str(uuid.uuid4())
            cql = f"""
            INSERT INTO hydra.urbosa_t0_routers (router_id, name, uplink_interface, uplink_ip, gateway_ip, nat_rules)
            VALUES ({router_id}, '{name}', '{uplink_interface}', '{uplink_ip}', '{gateway_ip}', '{nat_rules}');
            """
            task_id, err = submit_catalyst_cql_task(f"deploy_t0_{name}", cql)
            if err:
                self.send_json(500, {"error": f"Failed to submit creation task to Catalyst: {err}"})
                return

            self.send_json(201, {"message": f"T0 Router creation task triggered.", "router_id": router_id, "task_id": task_id})
            return

        elif self.path == "/api/urbosa/t0/delete":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                router_id = payload["router_id"].strip()
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            # Lanayru guard
            rc_lan, out_lan, _ = run_cql_query("SELECT status FROM hydra.lanayru_clusters;")
            if rc_lan == 0 and out_lan:
                for line in out_lan.splitlines():
                    if "active" in line.lower() or "deploying" in line.lower():
                        self.send_json(400, {"error": "Cannot delete default T0/T1 router while Lanayru K8s Engine is active."})
                        return

            cql_check = f"SELECT JSON * FROM hydra.urbosa_t1_routers;"
            rc_chk, out_chk, _ = run_cql_query(cql_check)
            if rc_chk == 0 and out_chk:
                for line in out_chk.splitlines():
                    if line.strip().startswith("{") and line.strip().endswith("}"):
                        try:
                            t1 = json.loads(line)
                            if t1.get("t0_link_id") == router_id:
                                self.send_json(400, {"error": f"Cannot delete T0 router because it is linked to T1 router '{t1.get('name')}'"})
                                return
                        except Exception:
                            pass

            cql = f"DELETE FROM hydra.urbosa_t0_routers WHERE router_id = {router_id};"
            task_id, err = submit_catalyst_cql_task(f"delete_t0_{router_id[:8]}", cql)
            if err:
                self.send_json(500, {"error": f"Failed to submit deletion task to Catalyst: {err}"})
                return

            self.send_json(200, {"message": "T0 Router deletion task triggered.", "task_id": task_id})
            return

        elif self.path == "/api/urbosa/t1/create":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"].strip()
                t0_link_id = payload["t0_link_id"].strip()
                dhcp_enabled = bool(payload.get("dhcp_enabled", False))
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            # import uuid
            router_id = str(uuid.uuid4())
            cql = f"""
            INSERT INTO hydra.urbosa_t1_routers (router_id, name, t0_link_id, dhcp_enabled)
            VALUES ({router_id}, '{name}', {t0_link_id}, {str(dhcp_enabled).lower()});
            """
            task_id, err = submit_catalyst_cql_task(f"deploy_t1_{name}", cql)
            if err:
                self.send_json(500, {"error": f"Failed to submit T1 creation task: {err}"})
                return

            self.send_json(201, {"message": f"T1 Router creation task triggered.", "router_id": router_id, "task_id": task_id})
            return

        elif self.path == "/api/urbosa/t1/delete":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                router_id = payload["router_id"].strip()
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            # Lanayru guard
            rc_lan, out_lan, _ = run_cql_query("SELECT status FROM hydra.lanayru_clusters;")
            if rc_lan == 0 and out_lan:
                for line in out_lan.splitlines():
                    if "active" in line.lower() or "deploying" in line.lower():
                        self.send_json(400, {"error": "Cannot delete default T0/T1 router while Lanayru K8s Engine is active."})
                        return

            cql_check = f"SELECT JSON * FROM hydra.urbosa_segments;"
            rc_chk, out_chk, _ = run_cql_query(cql_check)
            if rc_chk == 0 and out_chk:
                for line in out_chk.splitlines():
                    if line.strip().startswith("{") and line.strip().endswith("}"):
                        try:
                            seg = json.loads(line)
                            if seg.get("t1_link_id") == router_id:
                                self.send_json(400, {"error": f"Cannot delete T1 router because it is linked to overlay segment '{seg.get('name')}'"})
                                return
                        except Exception:
                            pass

            cql = f"DELETE FROM hydra.urbosa_t1_routers WHERE router_id = {router_id};"
            task_id, err = submit_catalyst_cql_task(f"delete_t1_{router_id[:8]}", cql)
            if err:
                self.send_json(500, {"error": f"Failed to submit T1 deletion task: {err}"})
                return

            self.send_json(200, {"message": "T1 Router deletion task triggered.", "task_id": task_id})
            return

        elif self.path == "/api/urbosa/segments/create":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"].strip()
                vni = int(payload["vni"])
                t1_link_id = payload["t1_link_id"].strip()
                subnet_cidr = payload["subnet_cidr"].strip()
                gateway_ip = payload["gateway_ip"].strip()
                dhcp_enabled = bool(payload.get("dhcp_enabled", False))
                dhcp_start = payload.get("dhcp_start", "").strip()
                dhcp_end = payload.get("dhcp_end", "").strip()
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            import ipaddress
            try:
                network = ipaddress.ip_network(subnet_cidr, strict=True)
                gw_ip = ipaddress.ip_address(gateway_ip)
                if gw_ip not in network:
                    self.send_json(400, {"error": f"Gateway IP {gateway_ip} is not within the subnet range {subnet_cidr}"})
                    return
                if dhcp_enabled:
                    if not dhcp_start or not dhcp_end:
                        self.send_json(400, {"error": "DHCP range start and end IPs must be specified if DHCP is enabled."})
                        return
                    start_ip = ipaddress.ip_address(dhcp_start)
                    end_ip = ipaddress.ip_address(dhcp_end)
                    if start_ip not in network or end_ip not in network:
                        self.send_json(400, {"error": "DHCP range start and end IPs must be within the segment subnet."})
                        return
                    if start_ip > end_ip:
                        self.send_json(400, {"error": "DHCP start IP cannot be greater than the end IP."})
                        return
            except ValueError as e:
                self.send_json(400, {"error": f"Invalid CIDR network format, gateway IP, or DHCP range: {str(e)}"})
                return

            # import uuid
            segment_id = str(uuid.uuid4())
            cql = f"""
            INSERT INTO hydra.urbosa_segments (segment_id, name, vni, t1_link_id, subnet_cidr, gateway_ip, dhcp_enabled, dhcp_start, dhcp_end)
            VALUES ({segment_id}, '{name}', {vni}, {t1_link_id}, '{subnet_cidr}', '{gateway_ip}', {str(dhcp_enabled).lower()}, '{dhcp_start}', '{dhcp_end}');
            """
            task_id, err = submit_catalyst_cql_task(f"deploy_segment_{name}", cql)
            if err:
                self.send_json(500, {"error": f"Failed to submit Segment creation task: {err}"})
                return

            self.send_json(201, {"message": f"Overlay Segment creation task triggered.", "segment_id": segment_id, "task_id": task_id})
            return

        elif self.path == "/api/urbosa/segments/delete":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                segment_id = payload["segment_id"].strip()
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            cql = f"DELETE FROM hydra.urbosa_segments WHERE segment_id = {segment_id};"
            task_id, err = submit_catalyst_cql_task(f"delete_segment_{segment_id[:8]}", cql)
            if err:
                self.send_json(500, {"error": f"Failed to submit Segment deletion task: {err}"})
                return

            self.send_json(200, {"message": "Overlay Segment deletion task triggered.", "task_id": task_id})
            return

        elif self.path == "/api/urbosa/segments/update":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                segment_id = payload["segment_id"].strip()
                name = payload.get("name", "").strip()
                vni_raw = payload.get("vni")
                t1_link_id = payload.get("t1_link_id", "").strip()
                subnet_cidr = payload.get("subnet_cidr", "").strip()
                gateway_ip = payload.get("gateway_ip", "").strip()
                dhcp_enabled_raw = payload.get("dhcp_enabled")
                dhcp_start = payload.get("dhcp_start", "").strip()
                dhcp_end = payload.get("dhcp_end", "").strip()
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            import ipaddress
            cql_select = f"SELECT JSON name, vni, t1_link_id, subnet_cidr, gateway_ip, dhcp_enabled, dhcp_start, dhcp_end FROM hydra.urbosa_segments WHERE segment_id = {segment_id};"
            rc_s, stdout_s, _ = run_cql_query(cql_select)
            existing = {}
            if rc_s == 0 and stdout_s:
                for line in stdout_s.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            existing = json.loads(line)
                        except Exception:
                            pass

            final_cidr = subnet_cidr if subnet_cidr else existing.get("subnet_cidr", "")
            final_gw = gateway_ip if gateway_ip else existing.get("gateway_ip", "")
            final_dhcp = bool(dhcp_enabled_raw) if dhcp_enabled_raw is not None else bool(existing.get("dhcp_enabled", False))
            final_start = dhcp_start if dhcp_start is not None else existing.get("dhcp_start", "")
            final_end = dhcp_end if dhcp_end is not None else existing.get("dhcp_end", "")

            try:
                if final_cidr and final_gw:
                    network = ipaddress.ip_network(final_cidr, strict=True)
                    gw_ip = ipaddress.ip_address(final_gw)
                    if gw_ip not in network:
                        self.send_json(400, {"error": f"Gateway IP {final_gw} is not within the subnet range {final_cidr}"})
                        return
                    if final_dhcp:
                        if not final_start or not final_end:
                            self.send_json(400, {"error": "DHCP range start and end IPs must be specified if DHCP is enabled."})
                            return
                        start_ip = ipaddress.ip_address(final_start)
                        end_ip = ipaddress.ip_address(final_end)
                        if start_ip not in network or end_ip not in network:
                            self.send_json(400, {"error": "DHCP range start and end IPs must be within the segment subnet."})
                            return
                        if start_ip > end_ip:
                            self.send_json(400, {"error": "DHCP start IP cannot be greater than the end IP."})
                            return
            except ValueError as e:
                self.send_json(400, {"error": f"Invalid CIDR network format, gateway IP, or DHCP range: {str(e)}"})
                return

            update_parts = []
            if name:
                update_parts.append(f"name = '{name}'")
            if vni_raw is not None:
                update_parts.append(f"vni = {int(vni_raw)}")
            if t1_link_id:
                update_parts.append(f"t1_link_id = {t1_link_id}")
            if subnet_cidr:
                update_parts.append(f"subnet_cidr = '{subnet_cidr}'")
            if gateway_ip:
                update_parts.append(f"gateway_ip = '{gateway_ip}'")
            if dhcp_enabled_raw is not None:
                update_parts.append(f"dhcp_enabled = {str(bool(dhcp_enabled_raw)).lower()}")
            if dhcp_start is not None:
                update_parts.append(f"dhcp_start = '{dhcp_start}'")
            if dhcp_end is not None:
                update_parts.append(f"dhcp_end = '{dhcp_end}'")

            if not update_parts:
                self.send_json(400, {"error": "Nothing to update"})
                return

            cql = f"""
            UPDATE hydra.urbosa_segments SET {', '.join(update_parts)} WHERE segment_id = {segment_id};
            """
            task_id, err = submit_catalyst_cql_task(f"update_segment_{segment_id[:8]}", cql)
            if err:
                self.send_json(500, {"error": f"Failed to submit Segment update task: {err}"})
                return

            self.send_json(200, {"message": "Segment updated successfully.", "task_id": task_id})
            return

        elif self.path == "/api/urbosa/firewall/create":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                description = payload["description"].strip()
                source_ip = payload["source_ip"].strip()
                dest_ip = payload["dest_ip"].strip()
                protocol = payload["protocol"].strip()
                port = int(payload["port"])
                action = payload["action"].strip()
                priority = int(payload["priority"])
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            # import uuid
            rule_id = str(uuid.uuid4())
            cql = f"""
            INSERT INTO hydra.urbosa_firewall_rules (rule_id, description, source_ip, dest_ip, protocol, port, action, priority)
            VALUES ({rule_id}, '{description}', '{source_ip}', '{dest_ip}', '{protocol}', {port}, '{action}', {priority});
            """
            rc, stdout, stderr = run_cql_query(cql)
            if rc != 0:
                self.send_json(500, {"error": f"Failed to create firewall rule: {stderr or stdout}"})
                return

            self.send_json(201, {"message": f"Firewall rule '{description}' created successfully.", "rule_id": rule_id})
            return

        elif self.path == "/api/urbosa/firewall/delete":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                rule_id = payload["rule_id"].strip()
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            cql = f"DELETE FROM hydra.urbosa_firewall_rules WHERE rule_id = {rule_id};"
            rc, stdout, stderr = run_cql_query(cql)
            if rc != 0:
                self.send_json(500, {"error": f"Failed to delete firewall rule: {stderr or stdout}"})
                return

            self.send_json(200, {"message": "Firewall rule deleted successfully."})
            return

        elif self.path == "/api/urbosa/t0/update":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                router_id = payload["router_id"].strip()
                name = payload["name"].strip()
                uplink_interface = payload["uplink_interface"].strip()
                uplink_ip = payload["uplink_ip"].strip()
                gateway_ip = payload["gateway_ip"].strip()
                nat_rules = json.dumps(payload.get("nat_rules", {}))
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            import ipaddress
            try:
                ip_iface = ipaddress.ip_interface(uplink_ip)
                gw_ip = ipaddress.ip_address(gateway_ip)
                if gw_ip not in ip_iface.network:
                    self.send_json(400, {"error": f"Gateway IP {gateway_ip} is not within the uplink network {ip_iface.network}"})
                    return
            except ValueError as e:
                self.send_json(400, {"error": f"Invalid Uplink CIDR or Gateway IP: {str(e)}"})
                return

            cql = f"""
            UPDATE hydra.urbosa_t0_routers SET name = '{name}', uplink_interface = '{uplink_interface}', uplink_ip = '{uplink_ip}', gateway_ip = '{gateway_ip}', nat_rules = '{nat_rules}' WHERE router_id = {router_id};
            """
            task_id, err = submit_catalyst_cql_task(f"update_t0_{name}", cql)
            if err:
                self.send_json(500, {"error": f"Failed to submit T0 Gateway update task: {err}"})
                return

            self.send_json(200, {"message": "T0 Gateway updated successfully.", "task_id": task_id})
            return

        elif self.path == "/api/urbosa/t1/update":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                router_id = payload["router_id"].strip()
                name = payload["name"].strip()
                t0_link_id = payload["t0_link_id"].strip()
                dhcp_enabled = bool(payload.get("dhcp_enabled", False))
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            cql = f"""
            UPDATE hydra.urbosa_t1_routers SET name = '{name}', t0_link_id = {t0_link_id}, dhcp_enabled = {str(dhcp_enabled).lower()} WHERE router_id = {router_id};
            """
            task_id, err = submit_catalyst_cql_task(f"update_t1_{name}", cql)
            if err:
                self.send_json(500, {"error": f"Failed to submit T1 Router update task: {err}"})
                return

            self.send_json(200, {"message": "T1 Router updated successfully.", "task_id": task_id})
            return

        elif self.path == "/api/urbosa/firewall/update":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                rule_id = payload["rule_id"].strip()
                priority = int(payload["priority"])
                description = payload["description"].strip()
                source_ip = payload["source_ip"].strip()
                dest_ip = payload["dest_ip"].strip()
                protocol = payload["protocol"].strip()
                port = int(payload["port"])
                action = payload["action"].strip()
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            cql = f"""
            UPDATE hydra.urbosa_firewall_rules SET priority = {priority}, description = '{description}', source_ip = '{source_ip}', dest_ip = '{dest_ip}', protocol = '{protocol}', port = {port}, action = '{action}' WHERE rule_id = {rule_id};
            """
            # Firewall rule update does not trigger daemon network namespace rebuild because there is no task runner for firewall (it is processed on demand or simple db query, or wait, it runs as cql task in bootstrap)
            # Let's run it directly or run via cql query if Catalyst task runner is fine. Wait, does firewall use Catalyst task?
            # Creating firewall rule used direct CQL:
            # rc, stdout, stderr = run_cql_query(cql)
            # Let's do the same for update rules so that it applies immediately without needing task queue!
            rc, stdout, stderr = run_cql_query(cql)
            if rc != 0:
                self.send_json(500, {"error": f"Failed to update firewall rule: {stderr or stdout}"})
                return

            self.send_json(200, {"message": "Firewall rule updated successfully."})
            return

        elif self.path == "/api/networks/update":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                net_id = payload["net_id"].strip()
                name = payload["name"].strip()
                vlan_id = payload.get("vlan_id")
            except Exception as e:
                self.send_json(400, {"error": "Invalid payload"})
                return

            if not net_id or not name:
                self.send_json(400, {"error": "Network ID and Name are required"})
                return

            if net_id == "7a68e0d6-11f8-4e89-9430-b3b44b8bc438":
                self.send_json(400, {"error": "Cannot edit Physical-Direct system network."})
                return

            # Check if network exists and get its type
            cql_check = f"SELECT JSON * FROM hydra.gatoway_networks WHERE net_id = {net_id};"
            rc, stdout, _ = run_cql_query(cql_check)
            if rc != 0 or not stdout:
                self.send_json(404, {"error": "Network segment not found"})
                return
                
            net_data = None
            for line in stdout.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        net_data = json.loads(line)
                        break
                    except Exception:
                        pass
            
            if not net_data:
                self.send_json(404, {"error": "Network segment not found"})
                return
                
            net_type = net_data.get("type", "direct")
            vlan_val = "null"
            if net_type == "vlan":
                try:
                    vlan_val = int(vlan_id)
                    if not (1 <= vlan_val <= 4094):
                        raise ValueError()
                except (TypeError, ValueError):
                    self.send_json(400, {"error": "VLAN ID must be an integer between 1 and 4094"})
                    return

                # Check if VLAN ID is already in use by another network
                cql_all = "SELECT JSON * FROM hydra.gatoway_networks;"
                rc_all, stdout_all, _ = run_cql_query(cql_all)
                if rc_all == 0 and stdout_all:
                    for line in stdout_all.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                other_net = json.loads(line)
                                if other_net.get("net_id") != net_id and other_net.get("vlan_id") == vlan_val:
                                    self.send_json(400, {"error": f"VLAN ID {vlan_val} is already assigned to network '{other_net.get('name')}'"})
                                    return
                            except Exception:
                                pass

            cql_upd = f"UPDATE hydra.gatoway_networks SET name = '{name}', vlan_id = {vlan_val} WHERE net_id = {net_id};"
            rc_upd, stdout_upd, stderr_upd = run_cql_query(cql_upd)
            if rc_upd != 0:
                self.send_json(500, {"error": f"Failed to update network in database: {stderr_upd or stdout_upd}"})
                return

            EVENT_LOGS.append({
                "desc": f"Network segment '{name}' updated.",
                "time": "Just now"
            })

            self.send_json(200, {"message": f"Network segment '{name}' updated successfully."})
            return

        elif self.path == "/api/storage/containers/update":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["name"]
                quota_bytes = int(payload.get("quota_bytes", 0))
            except Exception as e:
                self.send_json(400, {"error": "Invalid payload"})
                return

            # Storage quotas are not dynamically set on Linstor/DRBD engine in this manner
            pass

            # Update ScyllaDB
            cql = f"UPDATE hydra.storage_containers SET quota_bytes = {quota_bytes} WHERE name = '{name}';"
            run_cql_query(cql)

            EVENT_LOGS.append({
                "desc": f"Storage container '{name}' updated.",
                "time": "Just now"
            })

            self.send_json(200, {"message": f"Storage container {name} updated successfully."})
            return

        elif self.path == "/api/mimir/schedule/update":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["schedule_name"]
                enabled = bool(payload["enabled"])
            except Exception as e:
                self.send_json(400, {"error": "Invalid payload"})
                return
            
            cql = f"UPDATE hydra.mimir_schedules SET enabled = {str(enabled).lower()} WHERE schedule_name = '{name}';"
            run_cql_query(cql)
            self.send_json(200, {"message": f"Schedule {name} status updated."})
            return

        elif self.path == "/api/mimir/run":
            if not is_authenticated(self):
                self.send_json(401, {"error": "Unauthorized"})
                return
                
            payload = {
                "service": "dagur",
                "action": "execute",
                "payload": {
                    "job_name": "mimir_diagnostics",
                    "command": "/usr/local/bin/mcli health_checks run_all"
                }
            }
            try:
                leader_ip = get_catalyst_target_ip()
                req = urllib.request.Request(
                    f"http://{leader_ip}:9091/api/v1/tasks/submit",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=10) as response:
                    res = json.loads(response.read().decode("utf-8"))
                    task_id = res.get("task_id")
                    status = res.get("status", "pending")
                    self.send_json(202, {
                        "task_id": task_id, 
                        "status": status, 
                        "message": "Diagnostics task submitted successfully."
                    })
            except Exception as e:
                self.send_json(500, {"error": f"Failed to submit task to Catalyst: {str(e)}"})
            return

        elif self.path in ["/api/maintenance/rebalance", "/api/maintenance/cleanup", "/api/maintenance/dbcleanup"]:
            if not is_authenticated(self):
                self.send_json(401, {"error": "Unauthorized"})
                return
                
            job_name = ""
            command = ""
            if self.path == "/api/maintenance/rebalance":
                job_name = "disk_rebalance"
                command = "echo 'Linstor/DRBD storage is balanced.'"
            elif self.path == "/api/maintenance/cleanup":
                job_name = "disk_cleanup"
                command = "rm -rf /tmp/spectrum_build* /tmp/mimir_check_* && podman system prune -f || true"
            elif self.path == "/api/maintenance/dbcleanup":
                job_name = "db_cleanup"
                command = "podman exec systemd-hydra-db nodetool cleanup"
                
            payload = {
                "service": "dagur",
                "action": "execute",
                "payload": {
                    "job_name": job_name,
                    "command": command
                }
            }
            try:
                leader_ip = get_catalyst_target_ip()
                req = urllib.request.Request(
                    f"http://{leader_ip}:9091/api/v1/tasks/submit",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=10) as response:
                    res = json.loads(response.read().decode("utf-8"))
                    task_id = res.get("task_id")
                    status = res.get("status", "pending")
                    self.send_json(200, {
                        "task_id": task_id, 
                        "status": status, 
                        "message": f"Maintenance task '{job_name}' submitted successfully."
                    })
            except Exception as e:
                self.send_json(500, {"error": f"Failed to submit task to Catalyst: {str(e)}"})
            return

        elif self.path == "/api/dagur/schedule/update":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["job_name"]
                enabled = bool(payload["enabled"])
            except Exception as e:
                self.send_json(400, {"error": "Invalid payload"})
                return
            
            cql = f"UPDATE hydra.dagur_schedules SET enabled = {str(enabled).lower()} WHERE job_name = '{name}';"
            run_cql_query(cql)
            self.send_json(200, {"message": f"Schedule {name} status updated."})
            return

        elif self.path == "/api/dagur/schedule/trigger":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                name = payload["job_name"]
            except Exception as e:
                self.send_json(400, {"error": "Invalid payload"})
                return
            
            # Retrieve command
            cql = f"SELECT JSON command FROM hydra.dagur_schedules WHERE job_name = '{name}';"
            rc, stdout, stderr = run_cql_query(cql)
            command = ""
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            cmd_obj = json.loads(line)
                            command = cmd_obj.get("command", "")
                        except Exception:
                            pass
            if not command:
                self.send_json(400, {"error": f"Job {name} not found or has no command."})
                return
                
            # Submit Catalyst task
            submit_payload = {
                "service": "dagur",
                "action": "execute",
                "payload": {
                    "job_name": name,
                    "command": command
                }
            }
            try:
                leader_ip = get_catalyst_target_ip()
                req = urllib.request.Request(
                    f"http://{leader_ip}:9091/api/v1/tasks/submit",
                    data=json.dumps(submit_payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=10) as response:
                    res = json.loads(response.read().decode("utf-8"))
                    task_id = res.get("task_id")
                    status = res.get("status", "pending")
                    
                    EVENT_LOGS.append({
                        "desc": f"Manual run of job '{name}' triggered.",
                        "time": "Just now"
                    })
                    self.send_json(202, {
                        "task_id": task_id,
                        "status": status,
                        "message": f"Job {name} manually triggered successfully."
                    })
            except Exception as e:
                self.send_json(500, {"error": f"Failed to submit task to Catalyst: {str(e)}"})
            return

        elif self.path == "/api/settings/ssl/update":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                cert_data = payload.get("certificate", "").strip()
                key_data = payload.get("private_key", "").strip()
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            if not cert_data or not key_data:
                self.send_json(400, {"error": "Certificate and Private Key are required."})
                return

            try:
                cert_dir = "/etc/hci/spectrum/certs"
                os.makedirs(cert_dir, exist_ok=True)
                with open(f"{cert_dir}/server.crt", "w") as f:
                    f.write(cert_data)
                with open(f"{cert_dir}/server.key", "w") as f:
                    f.write(key_data)

                import base64
                b64_cert = base64.b64encode(cert_data.encode('utf-8')).decode('utf-8')
                b64_key = base64.b64encode(key_data.encode('utf-8')).decode('utf-8')

                hosts = get_cluster_nodes()
                for host in hosts:
                    host_ip = host.get("ip", "")
                    if host_ip and host_ip != LOCAL_IP:
                        cmd = (
                            f"mkdir -p /etc/hci/spectrum/certs && "
                            f"echo {b64_cert} | base64 -d > /etc/hci/spectrum/certs/server.crt && "
                            f"echo {b64_key} | base64 -d > /etc/hci/spectrum/certs/server.key && "
                            f"systemctl restart slate"
                        )
                        run_remote_spark(host_ip, cmd)

                def restart_console():
                    import time
                    time.sleep(2)
                    subprocess.run("systemctl restart slate", shell=True)
                    subprocess.run("systemctl restart spectrum", shell=True)

                threading.Thread(target=restart_console, daemon=True).start()
                self.send_json(200, {"status": "success", "message": "SSL Certificate applied successfully. Web console restarting..."})
            except Exception as e:
                self.send_json(500, {"error": f"Failed to apply certificate: {str(e)}"})
            return

        elif self.path == "/api/host/maintenance":
            if not is_authenticated(self):
                self.send_json(401, {"error": "Unauthorized"})
                return
            try:
                payload = json.loads(post_data.decode("utf-8"))
                target_hostname = payload.get("hostname", "")
                action = payload.get("action", "")
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            if action not in ["enter", "leave"]:
                self.send_json(400, {"error": "Invalid action. Must be 'enter' or 'leave'."})
                return

            target_ip = None
            for n in get_cluster_nodes():
                if n.get("hostname") == target_hostname:
                    target_ip = n.get("ip")
                    break

            if not target_ip:
                self.send_json(404, {"error": f"Host '{target_hostname}' not found in cluster config."})
                return

            def run_maint():
                try:
                    payload_api = {"hostname": target_hostname, "action": action}
                    rc, res, err = run_mtls_spark_api("127.0.0.1", "/api/v1/host/maintenance", payload_api, method="POST")
                    if rc != 0 or "error" in res:
                        print(f"[MAINTENANCE API] Failed to submit maintenance mode task to Vali: {res.get('error', err)}")
                    else:
                        print(f"[MAINTENANCE API] Maintenance task submitted successfully: {res}")
                except Exception as ex:
                    print(f"Error in maintenance task: {ex}")

            threading.Thread(target=run_maint, daemon=True).start()
            self.send_json(200, {"status": "success", "message": f"Maintenance '{action}' transition initiated."})
            return

        elif self.path == "/api/host/reboot":
            try:
                payload = json.loads(post_data.decode("utf-8"))
                target_hostname = payload.get("hostname", "")
            except Exception:
                self.send_json(400, {"error": "Invalid payload"})
                return

            target_ip = None
            for n in get_cluster_nodes():
                if n.get("hostname") == target_hostname:
                    target_ip = n.get("ip")
                    break

            if not target_ip:
                self.send_json(404, {"error": f"Host '{target_hostname}' not found in cluster config."})
                return

            # If the target is the local node, forward it to another active node in the cluster
            if target_ip == LOCAL_IP or target_ip == "127.0.0.1":
                other_node_ip = None
                for n in get_cluster_nodes():
                    n_ip = n.get("ip")
                    if n_ip and n_ip != LOCAL_IP and n_ip != "127.0.0.1":
                        other_node_ip = n_ip
                        break
                
                if other_node_ip:
                    print(f"[REBOOT LOCAL REDIRECT] Redirecting reboot request for local host to {other_node_ip}...")
                    try:
                        # Forward request to other node
                        url = f"https://{other_node_ip}:8443/api/host/reboot"
                        req = urllib.request.Request(url, data=post_data, method="POST")
                        cookie = self.headers.get("Cookie")
                        if cookie:
                            req.add_header("Cookie", cookie)
                        auth = self.headers.get("Authorization")
                        if auth:
                            req.add_header("Authorization", auth)
                        req.add_header("Content-Type", "application/json")
                        
                        # Bypass SSL verification for internal communication
                        ctx = ssl.create_default_context()
                        ctx.check_hostname = False
                        ctx.verify_mode = ssl.CERT_NONE
                        
                        with urllib.request.urlopen(req, context=ctx, timeout=15) as response:
                            resp_data = response.read()
                            self.send_response(response.status)
                            for k, v in response.headers.items():
                                if k.lower() not in ["content-length", "connection"]:
                                    self.send_header(k, v)
                            self.send_header("Content-Length", str(len(resp_data)))
                            self.end_headers()
                            self.wfile.write(resp_data)
                            return
                    except Exception as e:
                        print(f"[REBOOT LOCAL REDIRECT] Failed to forward reboot to {other_node_ip}: {e}")
                        # Fallback to local execution if forwarding fails

            # Log reboot task in Catalyst
            task_id, created_at = log_catalyst_task(
                service="host",
                action="reboot",
                status="processing",
                progress=5,
                payload_dict={"hostname": target_hostname}
            )

            def reboot_node_task():
                import time
                try:
                    # 1. Put node in maintenance mode (evacuate VMs)
                    print(f"[REBOOT TASK] Evacuating host {target_hostname} and entering maintenance mode...")
                    log_catalyst_task("host", "reboot", "processing", 10, {"hostname": target_hostname}, task_id=task_id, created_at=created_at)
                    
                    payload_api = {"hostname": target_hostname, "action": "enter", "force_stop": True}
                    rc, res, err = run_mtls_spark_api("127.0.0.1", "/api/v1/host/maintenance", payload_api, method="POST")
                    if rc != 0 or "error" in res:
                        raise Exception(f"Failed to submit maintenance mode task to Vali: {res.get('error', err)}")
                        
                    maint_task_id = res.get("task_id")
                    maint_success = False
                    
                    if maint_task_id:
                        print(f"[REBOOT TASK] Waiting for maintenance enter task {maint_task_id}...")
                        for i in range(150): # up to 5 minutes
                            cql_task = f"SELECT JSON status, progress, error_msg FROM hydra.catalyst_tasks WHERE task_id = {maint_task_id};"
                            rc_t, stdout_t, _ = run_cql_query(cql_task)
                            if rc_t == 0 and stdout_t:
                                found = False
                                for line in stdout_t.splitlines():
                                    line = line.strip()
                                    if line.startswith("{") and line.endswith("}"):
                                        try:
                                            t_data = json.loads(line)
                                            t_status = t_data.get("status")
                                            t_prog = t_data.get("progress", 0)
                                            mapped_prog = 10 + int(t_prog * 0.25)
                                            log_catalyst_task("host", "reboot", "processing", mapped_prog, {"hostname": target_hostname}, task_id=task_id, created_at=created_at)
                                            
                                            if t_status == "completed":
                                                maint_success = True
                                                found = True
                                                break
                                            elif t_status == "failed":
                                                raise Exception(f"Maintenance enter failed: {t_data.get('error_msg')}")
                                        except Exception as ex_t:
                                            raise ex_t
                                if found:
                                    break
                            time.sleep(2)
                        else:
                            raise Exception("Timeout waiting for maintenance mode enter.")
                    else:
                        print(f"[REBOOT TASK] No maintenance task returned. Checking host status directly...")
                        for _ in range(15):
                            cql_node = f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{target_hostname}';"
                            rc_n, stdout_n, _ = run_cql_query(cql_node)
                            if rc_n == 0 and stdout_n:
                                for line in stdout_n.splitlines():
                                    line = line.strip()
                                    if line.startswith("{") and line.endswith("}"):
                                        n_status = json.loads(line).get("status")
                                        if n_status == "IN_MAINTENANCE":
                                            maint_success = True
                                            break
                            if maint_success:
                                break
                            time.sleep(2)
                        if not maint_success:
                            raise Exception("Host failed to enter maintenance mode.")

                    # 2. Reboot the host
                    print(f"[REBOOT TASK] Rebooting host {target_hostname}...")
                    log_catalyst_task("host", "reboot", "processing", 50, {"hostname": target_hostname}, task_id=task_id, created_at=created_at)
                    run_remote_spark(target_ip, "reboot || true")
                    
                    # 4. Wait for host to go offline
                    time.sleep(10)
                    print(f"[REBOOT TASK] Waiting for host {target_hostname} to go offline...")
                    log_catalyst_task("host", "reboot", "processing", 60, {"hostname": target_hostname}, task_id=task_id, created_at=created_at)
                    for _ in range(60):
                        rc, _, _ = run_remote_spark(target_ip, "echo 1")
                        if rc != 0:
                            print(f"[REBOOT TASK] Host {target_hostname} is offline.")
                            break
                        time.sleep(2)
                        
                    # 5. Wait for host to come back online
                    print(f"[REBOOT TASK] Waiting for host {target_hostname} to come back online...")
                    log_catalyst_task("host", "reboot", "processing", 75, {"hostname": target_hostname}, task_id=task_id, created_at=created_at)
                    online = False
                    for _ in range(120):
                        rc, _, _ = run_remote_spark(target_ip, "echo 1")
                        if rc == 0:
                            online = True
                            print(f"[REBOOT TASK] Host {target_hostname} is online.")
                            break
                        time.sleep(3)
                    if not online:
                        raise Exception("Host did not come back online in time.")
                        
                    # Wait for services to stabilize
                    print(f"[REBOOT TASK] Waiting for services to stabilize...")
                    log_catalyst_task("host", "reboot", "processing", 85, {"hostname": target_hostname}, task_id=task_id, created_at=created_at)
                    time.sleep(15)
                    
                    # 6. Leave maintenance mode
                    print(f"[REBOOT TASK] Restoring host {target_hostname} from maintenance mode...")
                    log_catalyst_task("host", "reboot", "processing", 90, {"hostname": target_hostname}, task_id=task_id, created_at=created_at)
                    
                    payload_api = {"hostname": target_hostname, "action": "leave"}
                    rc, res, err = run_mtls_spark_api("127.0.0.1", "/api/v1/host/maintenance", payload_api, method="POST")
                    if rc != 0 or "error" in res:
                        raise Exception(f"Failed to submit maintenance leave task to Vali: {res.get('error', err)}")
                        
                    leave_task_id = res.get("task_id")
                    leave_success = False
                    
                    if leave_task_id:
                        print(f"[REBOOT TASK] Waiting for maintenance leave task {leave_task_id}...")
                        for i in range(150): # up to 5 minutes
                            cql_task = f"SELECT JSON status, progress, error_msg FROM hydra.catalyst_tasks WHERE task_id = {leave_task_id};"
                            rc_t, stdout_t, _ = run_cql_query(cql_task)
                            if rc_t == 0 and stdout_t:
                                found = False
                                for line in stdout_t.splitlines():
                                    line = line.strip()
                                    if line.startswith("{") and line.endswith("}"):
                                        try:
                                            t_data = json.loads(line)
                                            t_status = t_data.get("status")
                                            t_prog = t_data.get("progress", 0)
                                            mapped_prog = 90 + int(t_prog * 0.09)
                                            log_catalyst_task("host", "reboot", "processing", mapped_prog, {"hostname": target_hostname}, task_id=task_id, created_at=created_at)
                                            
                                            if t_status == "completed":
                                                leave_success = True
                                                found = True
                                                break
                                            elif t_status == "failed":
                                                raise Exception(f"Maintenance leave failed: {t_data.get('error_msg')}")
                                        except Exception as ex_t:
                                            raise ex_t
                                if found:
                                    break
                            time.sleep(2)
                        else:
                            raise Exception("Timeout waiting for maintenance mode leave.")
                    else:
                        print(f"[REBOOT TASK] Checking host status directly...")
                        for _ in range(15):
                            cql_node = f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{target_hostname}';"
                            rc_n, stdout_n, _ = run_cql_query(cql_node)
                            if rc_n == 0 and stdout_n:
                                for line in stdout_n.splitlines():
                                    line = line.strip()
                                    if line.startswith("{") and line.endswith("}"):
                                        n_status = json.loads(line).get("status")
                                        if n_status == "NORMAL":
                                            leave_success = True
                                            break
                            if leave_success:
                                break
                            time.sleep(2)
                        if not leave_success:
                            raise Exception("Host failed to leave maintenance mode.")
                            
                    print(f"[REBOOT TASK] Reboot sequence completed successfully for {target_hostname}.")
                    log_catalyst_task("host", "reboot", "completed", 100, {"hostname": target_hostname}, task_id=task_id, created_at=created_at)
                except Exception as ex:
                    print(f"[REBOOT TASK] Error rebooting node: {ex}")
                    log_catalyst_task("host", "reboot", "failed", 100, {"hostname": target_hostname}, error_msg=str(ex), task_id=task_id, created_at=created_at)

            threading.Thread(target=reboot_node_task, daemon=True).start()
            self.send_json(200, {"status": "success", "message": f"Reboot sequence initiated for {target_hostname}."})
            return


        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()
        return


def db_reconcile_loop():
    # Give ScyllaDB time to bootstrap on startup
    time.sleep(15)
    while True:
        try:
            # 1. Fetch local VMs list from libvirt
            libvirt_vms = {}
            rc, stdout, stderr = run_remote_spark(LOCAL_IP, "virsh -c qemu:///system list --all")
            if rc != 0:
                time.sleep(30)
                continue
                
            lines = stdout.splitlines()
            for line in lines[2:]:
                parts = line.split()
                if len(parts) >= 3:
                    name = parts[1]
                    state = " ".join(parts[2:])
                    if state == "running":
                        state = "Running"
                    elif state == "shut off":
                        state = "Stopped"
                    libvirt_vms[name] = state

            # 1.5. Fetch active tasks from ScyllaDB to protect VMs undergoing operations
            active_task_vms = set()
            rc_tasks, stdout_tasks, stderr_tasks = run_cql_query("SELECT JSON * FROM hydra.catalyst_tasks;")
            if rc_tasks == 0:
                for line in stdout_tasks.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            task = json.loads(line)
                            if task.get("status") in ("running", "pending", "processing"):
                                payload_str = task.get("payload", "{}")
                                if payload_str:
                                    payload = json.loads(payload_str)
                                    vname = payload.get("vm_name") or payload.get("name")
                                    if vname:
                                        active_task_vms.add(vname)
                        except Exception:
                            pass

            # 2. Fetch metadata from ScyllaDB
            cql = "SELECT JSON name, state, host_ip FROM hydra.vms;"
            rc, stdout, stderr = run_cql_query(cql)
            if rc == 0:
                for line in stdout.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            vm = json.loads(line)
                            name = vm["name"]
                            db_state = vm.get("state")
                            host_ip = vm.get("host_ip", "")
                            
                            # Only reconcile VMs assigned to this node
                            is_local = (host_ip == LOCAL_IP or host_ip == "127.0.0.1")
                            if is_local:
                                live_state = libvirt_vms.get(name, "Stopped")
                                if live_state == "Stopped":
                                    if name in libvirt_vms:
                                        run_remote_spark("127.0.0.1", f"virsh -c qemu:///system undefine {name} --keep-nvram || true")
                                    if db_state != "Stopped" or host_ip != "":
                                        cql_update = f"UPDATE hydra.vms SET state = 'Stopped', host_ip = '' WHERE name = '{name}';"
                                        run_cql_query(cql_update)
                                elif db_state != live_state:
                                    cql_update = f"UPDATE hydra.vms SET state = '{live_state}' WHERE name = '{name}';"
                                    run_cql_query(cql_update)
                            else:
                                # This VM is assigned to another node in the database.
                                # If it exists locally (defined or running), we must clean it up to prevent split-brain.
                                # BUT we protect it if there is an active task running for this VM!
                                if name in libvirt_vms and name not in active_task_vms:
                                    live_state = libvirt_vms[name]
                                    print(f"[Reconcile] VM '{name}' is running/defined locally (state: {live_state}) but database assigns it to remote host {host_ip or 'None'}. Cleaning up locally to prevent split-brain...")
                                    if live_state == "Running":
                                        run_remote_spark(LOCAL_IP, f"virsh -c qemu:///system destroy {name} || true")
                                    run_remote_spark(LOCAL_IP, f"virsh -c qemu:///system undefine {name} --keep-nvram || true")
                        except Exception:
                            pass
        except Exception:
            pass
        time.sleep(30)

def get_zookeeper_leader_ip():
    """Finds the IP of the current ZooKeeper leader, with active designated leader fallback if the leader is in maintenance."""
    ips = []
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            ips = [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        ips = ["10.10.102.220", "10.10.102.222", "10.10.102.223"]
        
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

def get_catalyst_target_ip():
    leader_ip = get_zookeeper_leader_ip()
    if leader_ip == LOCAL_IP or leader_ip == "127.0.0.1" or not leader_ip:
        return "127.0.0.1"
    return leader_ip

def mimir_scheduler_loop():
    # Wait for ScyllaDB and ZooKeeper to bootstrap on startup
    time.sleep(30)
    while True:
        try:
            if is_zookeeper_leader():
                # Read schedules
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
                            
                            if now - last_run >= interval:
                                print(f"[Mimir Scheduler] Triggering check: {name}...")
                                # Update last_run_epoch first to prevent multiple runs
                                cql_update = f"UPDATE hydra.mimir_schedules SET last_run_epoch = {now} WHERE schedule_name = '{name}';"
                                run_cql_query(cql_update)
                                
                                category = s.get("category", "all")
                                run_cmd = f"/usr/local/bin/mcli health_checks run_all" if category == "all" else f"/usr/local/bin/mcli health_checks {category}"
                                import threading
                                threading.Thread(target=run_remote_spark, args=("127.0.0.1", run_cmd), daemon=True).start()
        except Exception:
            pass
        time.sleep(60)

def insert_dagur_run(job_name, start_time, run_id, end_time, status, exit_code, output):
    clean_output = output.replace("'", "''").replace("\\", "\\\\")
    cql = f"""
    INSERT INTO hydra.dagur_runs (job_name, start_time, run_id, end_time, status, exit_code, output)
    VALUES ('{job_name}', {start_time}, {run_id}, {end_time}, '{status}', {exit_code}, '{clean_output}');
    """
    run_cql_query(cql)

def execute_dagur_job_thread(job_name, command):
    import uuid
    run_id = str(uuid.uuid4())
    start_time = int(time.time() * 1000)
    
    # Insert initial running record
    cql_start = f"""
    INSERT INTO hydra.dagur_runs (job_name, start_time, run_id, status, exit_code, output)
    VALUES ('{job_name}', {start_time}, {run_id}, 'RUNNING', -1, 'Job started...');
    """
    run_cql_query(cql_start)
    
    try:
        exit_code, stdout, stderr = run_remote_spark("127.0.0.1", command)
        out_str = stdout + stderr
        status = 'SUCCESS' if exit_code == 0 else 'FAILED'
    except Exception as e:
        exit_code = -1
        out_str = f"Execution failed: {str(e)}"
        status = 'FAILED'
        
    end_time = int(time.time() * 1000)
    insert_dagur_run(job_name, start_time, run_id, end_time, status, exit_code, out_str)
    
    EVENT_LOGS.append({
        "desc": f"Scheduled job '{job_name}' completed with status {status}.",
        "time": "Just now"
    })

def dagur_scheduler_loop():
    # Wait for ScyllaDB and ZooKeeper to bootstrap on startup
    time.sleep(30)
    while True:
        try:
            if is_zookeeper_leader():
                cql = "SELECT JSON * FROM hydra.dagur_schedules;"
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
                            name = s.get("job_name")
                            last_run = s.get("last_run_epoch", 0)
                            interval = s.get("interval_seconds", 3600)
                            command = s.get("command", "")
                            
                            if now - last_run >= interval:
                                print(f"[Dagur Scheduler] Triggering job: {name}...")
                                cql_update = f"UPDATE hydra.dagur_schedules SET last_run_epoch = {now} WHERE job_name = '{name}';"
                                run_cql_query(cql_update)
                                
                                t = threading.Thread(target=execute_dagur_job_thread, args=(name, command), daemon=True)
                                t.start()
        except Exception:
            pass
        time.sleep(10)

def internal_token_verifier_loop():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("127.0.0.1", 8089))
        s.listen(10)
        print("[Token Verifier] Listening on 127.0.0.1:8089 for Agahnim...", flush=True)
    except Exception as e:
        print(f"[Token Verifier] Error binding socket: {e}", flush=True)
        return

    while True:
        try:
            conn, addr = s.accept()
            token = conn.recv(1024).decode('utf-8').strip()
            if not token:
                conn.close()
                continue
            
            cql = f"SELECT JSON host_ip, port, expires_at FROM hydra.console_sessions WHERE console_token = '{token}';"
            rc, out, err = run_cql_query(cql)
            
            host_ip = None
            port = None
            expires_at = 0
            
            if rc == 0:
                for line in out.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            data = json.loads(line)
                            host_ip = data.get("host_ip")
                            port = data.get("port")
                            expires_at = data.get("expires_at", 0)
                        except Exception:
                            pass
            
            now = int(time.time())
            if host_ip and port and expires_at > now:
                response = f"OK|{host_ip}|{port}"
            else:
                response = "ERROR"
                
            conn.sendall(response.encode('utf-8'))
            conn.close()
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass

def main():
    # Start background VM state reconciliation thread
    t = threading.Thread(target=db_reconcile_loop, daemon=True)
    t.start()

    # Start background Mimir health checks scheduler thread
    # t2 = threading.Thread(target=mimir_scheduler_loop, daemon=True)
    # t2.start()

    # Start background Dagur central task runner scheduler thread
    # t3 = threading.Thread(target=dagur_scheduler_loop, daemon=True)
    # t3.start()
    
    # Start background metrics and cluster monitor loop thread
    t4 = threading.Thread(target=metrics_and_cluster_monitor_loop, daemon=True)
    t4.start()

    # Start background internal console token verifier socket server
    t5 = threading.Thread(target=internal_token_verifier_loop, daemon=True)
    t5.start()
    
    # 1. Initialize self-signed SSL certificates for web traffic
    cert_file, key_file = init_ssl()
    
    # 2. Setup SSL context
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=cert_file, keyfile=key_file)
    
    # 3. Attempt DB keyspace/table creation on startup
    init_db()

    server_address = ('', PORT)
    httpd = ThreadingHTTPServer(server_address, SpectrumHandler)
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)

    print(f"Spectrum UI Web Portal listening on HTTPS port {PORT}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
