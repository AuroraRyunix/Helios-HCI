#!/usr/bin/env python3
__build__ = "1.2.2"
import sys
import os
import json
import time
import re
import shlex
import socket
import urllib.request
import ssl
import threading
import uuid
import math
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

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

def get_dfs_engine():
    return "linstor"

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



def run_remote_spark(ip, command):
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = False
    
    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=120) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)

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
        req = urllib.request.Request('http://127.0.0.1:9043/query', data=cql.encode('utf-8'), headers={{'Content-Type': 'text/plain'}})
        with urllib.request.urlopen(req, timeout=5) as response:
            pass
    except Exception:
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('10.255.255.255', 1))
            local_ip = s.getsockname()[0]
            s.close()
            cmd = f"echo {{base64.b64encode(cql.encode('utf-8')).decode('utf-8')}} | base64 -d | podman exec -i systemd-hydra-db cqlsh {{local_ip}}"
            subprocess.run(cmd, shell=True, timeout=10)
        except Exception:
            pass
    if {delete_local}:
        try:
            os.remove(nvram_path)
        except Exception:
            pass
"""
    b64_code = base64.b64encode(py_code.encode('utf-8')).decode('utf-8')
    return f"python3 -c \"import base64; exec(base64.b64decode('{b64_code}').decode('utf-8'))\""

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

def is_zookeeper_leader():
    return get_zookeeper_leader_ip() == LOCAL_IP

def get_zookeeper_leader_ip():
    """Finds the IP of the current ZooKeeper leader, with active designated leader fallback if the leader is in maintenance."""
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

def call_catalyst_api(path, payload=None, method="GET"):
    import urllib.request
    import json
    leader_ip = get_zookeeper_leader_ip()
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
def init_db_schema():
    tasks_table = """
    CREATE TABLE IF NOT EXISTS hydra.vali_tasks (
        task_id uuid PRIMARY KEY,
        vm_name text,
        action text,
        status text,
        target_host text,
        created_at bigint,
        updated_at bigint,
        error_msg text
    );
    """
    drs_status_table = """
    CREATE TABLE IF NOT EXISTS hydra.vali_drs_status (
        cluster_name text PRIMARY KEY,
        current_deviation double,
        status_str text,
        last_drs_run bigint,
        drs_enabled boolean
    );
    """
    drs_history_table = """
    CREATE TABLE IF NOT EXISTS hydra.vali_drs_history (
        event_time timestamp,
        vm_name text,
        source_host text,
        target_host text,
        reason text,
        PRIMARY KEY (event_time, vm_name)
    );
    """
    nodes_table = """
    CREATE TABLE IF NOT EXISTS hydra.nodes (
        hostname text PRIMARY KEY,
        ip text,
        status text,
        maintenance_mode boolean
    );
    """
    run_cql_query(tasks_table)
    run_cql_query(drs_status_table)
    run_cql_query("ALTER TABLE hydra.vali_drs_status ADD drs_enabled boolean;")
    run_cql_query(drs_history_table)
    run_cql_query(nodes_table)

    # Gatoway Networks schema
    create_gatoway_networks = """
    CREATE TABLE IF NOT EXISTS hydra.gatoway_networks (
        net_id uuid PRIMARY KEY,
        name text,
        type text,
        vlan_id int
    );
    """
    run_cql_query(create_gatoway_networks)
    
    # Urbosa SDN schemas
    create_urbosa_t0 = """
    CREATE TABLE IF NOT EXISTS hydra.urbosa_t0_routers (
        router_id uuid PRIMARY KEY,
        name text,
        uplink_interface text,
        uplink_ip text,
        gateway_ip text,
        nat_rules text
    );
    """
    create_urbosa_t1 = """
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
    create_urbosa_firewall = """
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
    run_cql_query(create_urbosa_t0)
    run_cql_query(create_urbosa_t1)
    run_cql_query(create_urbosa_segments)
    run_cql_query(create_urbosa_firewall)
    
    insert_default_network = """
    INSERT INTO hydra.gatoway_networks (net_id, name, type, vlan_id)
    VALUES (7a68e0d6-11f8-4e89-9430-b3b44b8bc438, 'Physical-Direct', 'direct', null) IF NOT EXISTS;
    """
    run_cql_query(insert_default_network)
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
    # Memory usage
    rc_m, stdout_m, _ = run_remote_spark(ip, "free -m")
    u_mem = 0.0
    mem_total = 1.0
    mem_used = 0.0
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
        # CPU usage over 0.5s
        cpu_cmd = "python3 -c \"import time; f=open('/proc/stat'); l1=f.readline().split(); f.close(); time.sleep(0.2); f=open('/proc/stat'); l2=f.readline().split(); f.close(); d1=sum(int(x) for x in l1[1:]); d2=sum(int(x) for x in l2[1:]); idle1=int(l1[4]); idle2=int(l2[4]); print(1.0 - (idle2-idle1)/(d2-d1))\""
        rc_c, stdout_c, _ = run_remote_spark(ip, cpu_cmd)
        if rc_c == 0:
            try:
                u_cpu = float(stdout_c.strip())
            except:
                pass

    return u_cpu, u_mem, mem_total, mem_used

# Core DRS Algorithm
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
        if now - last_migration_time < cooldown:
            print(f"[DRS] Cluster imbalance detected (σ={std_dev:.3f}, Score={balance_score}%), but in cooldown.")
            return
            
        # 4. Find overloaded and underloaded hosts
        overloaded_ip = max(load_metrics, key=load_metrics.get)
        underloaded_ip = min(load_metrics, key=load_metrics.get)
        
        # 5. Get running VMs on overloaded host
        cql_vms = "SELECT JSON name, host_ip, memory, state FROM hydra.vms;"
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
    while True:
        try:
            if is_zookeeper_leader():
                run_drs_loop(aggressive=False)
        except Exception as e:
            sys.stderr.write(f"Error in DRS loop thread: {e}\n")
        time.sleep(30)

def get_vm_disk_size(vm_name):
    vm_data = get_vm_xml_specs(vm_name)
    if vm_data and vm_data.get("disks_list"):
        res_name = vm_data["disks_list"].split(",")[0]
        ips = []
        try:
            if os.path.exists("/etc/hci/cluster.json"):
                with open("/etc/hci/cluster.json", "r") as f:
                    ips = [h["ip"] for h in json.load(f).get("hosts", [])]
        except Exception:
            pass
        if not ips:
            ips = ["127.0.0.1"]
        controllers_str = ",".join(ips)
        cmd = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor volume-definition list --resource-definition {res_name}"
        rc, stdout, _ = run_remote_spark(LOCAL_IP, cmd)
        if rc == 0 and stdout:
            for line in stdout.splitlines():
                parts = line.split("|")
                for p in parts:
                    if "gib" in p.lower():
                        try:
                            val = float(p.strip().split()[0])
                            return int(val * 1024)
                        except:
                            pass
    return 51200

def get_linstor_free_space(target_ip):
    ips = []
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                ips = [h["ip"] for h in json.load(f).get("hosts", [])]
    except Exception:
        pass
    if not ips:
        ips = ["127.0.0.1"]
    controllers_str = ",".join(ips)
    cmd = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor storage-pool list"
    rc, stdout, _ = run_remote_spark(target_ip, cmd)
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            if "default-pool" in line or "lvm_thin" in line or "thin" in line:
                parts = line.split("|")
                for p in parts:
                    if "/" in p:
                        try:
                            free_str = p.split("/")[0].strip()
                            val = float(free_str.split()[0])
                            unit = free_str.split()[1].lower()
                            if "gib" in unit:
                                return int(val * 1024)
                            elif "tib" in unit:
                                return int(val * 1024 * 1024)
                            elif "mib" in unit:
                                return int(val)
                        except:
                            pass
    return 999999

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

def generate_vm_xml(name, memory, vcpu, firmware, disks_list, iso, boot_device="", host_ip="127.0.0.1", network_id=None, cpu_model=None, audio_enabled=False):
    primary_container = get_default_container()
    if disks_list and disks_list != "NONE":
        first_entry = disks_list.split(",")[0]
        if ":" in first_entry:
            primary_container = first_entry.split(":")[1]

    if boot_device:
        if boot_device == "cdrom":
            boot_devices = "<boot dev='cdrom'/>\n    <boot dev='hd'/>"
        else:
            boot_devices = "<boot dev='hd'/>\n    <boot dev='cdrom'/>"
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
    {boot_devices}
    <bootmenu enable='yes' timeout='3000'/>"""
    else:
        os_boot_xml = f"""<type arch='x86_64' machine='q35'>hvm</type>
    {boot_devices}
    <bootmenu enable='yes' timeout='3000'/>"""

    video_xml = """<video>
      <model type='virtio' vram='65536' heads='1' primary='yes'/>
    </video>"""

    import string
    letters = string.ascii_lowercase
    disk_devices_xml = ""
    disk_paths_with_bus = []
    if disks_list and disks_list != "NONE":
        for idx, entry in enumerate(disks_list.split(",")):
            parts = entry.split(":")
            bus = parts[2] if len(parts) > 2 else "virtio"
            d_path = f"/dev/drbd/by-res/{name}-disk{idx}/0"
            disk_paths_with_bus.append((d_path, bus))
    elif disks_list == "NONE":
        disk_paths_with_bus = []
    else:
        disk_paths_with_bus = [(f"/dev/drbd/by-res/{name}-disk0/0", "virtio")]

    for idx, (d_path, bus) in enumerate(disk_paths_with_bus):
        dev_prefix = "vd" if bus == "virtio" else "sd"
        dev_letter = letters[idx % 26]
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
        for idx, spec in enumerate(iso.split(",")):
            if spec.strip() and spec.strip() != "__empty__":
                sata_letter = letters[idx % 26]
                iso_path = None
                try:
                    rc_img, stdout_img, _ = run_cql_query(f"SELECT path FROM hydra.valhalla_images WHERE name = '{spec.strip()}';")
                    if rc_img == 0 and stdout_img:
                        for line in stdout_img.splitlines():
                            if "/dev/" in line:
                                iso_path = line.strip().split()[-1].replace("'", "").replace('"', '')
                                break
                except Exception:
                    pass
                if not iso_path:
                    import re
                    base = spec.strip()
                    if base.lower().endswith(".iso"):
                        base = base[:-4]
                    elif base.lower().endswith(".qcow2"):
                        base = base[:-6]
                    elif base.lower().endswith(".img"):
                        base = base[:-4]
                    slug = re.sub(r'[^a-z0-9_-]', '-', base.lower())
                    slug = re.sub(r'-+', '-', slug)
                    slug = slug.strip('-')
                    slug = slug[:28]
                    iso_path = f"/dev/drbd/by-res/img-{slug}/0"
                    
                disk_devices_xml += f"""
    <disk type='block' device='cdrom'>
      <driver name='qemu' type='raw' locking='off'/>
      <source dev='{iso_path}'/>
      <target dev='sd{sata_letter}' bus='sata'/>
      <readonly/>
    </disk>"""

    global KVM_CACHE, VMWARE_CACHE
    has_kvm = False
    if host_ip in KVM_CACHE:
        has_kvm = KVM_CACHE[host_ip]
    else:
        try:
            rc, _, _ = run_remote_spark(host_ip, "test -e /dev/kvm")
            has_kvm = (rc == 0)
            KVM_CACHE[host_ip] = has_kvm
        except Exception:
            pass

    is_vmware = False
    if host_ip in VMWARE_CACHE:
        is_vmware = VMWARE_CACHE[host_ip]
    else:
        try:
            rc, stdout, _ = run_remote_spark(host_ip, "systemd-detect-virt")
            is_vmware = (rc == 0 and "vmware" in stdout.strip().lower())
            VMWARE_CACHE[host_ip] = is_vmware
        except Exception:
            pass

    domain_type = "kvm" if has_kvm else "qemu"
    
    actual_cpu = cpu_model if cpu_model else ("host-model" if is_vmware else "host-passthrough")
    
    if actual_cpu == "host-passthrough" and is_vmware:
        cpu_xml = f"""<cpu mode='host-passthrough'>
    <topology sockets='1' dies='1' cores='{vcpu}' threads='1'/>
    <feature policy='disable' name='vmx'/>
  </cpu>"""
    elif actual_cpu in ["host-model", "host-passthrough"]:
        cpu_xml = f"""<cpu mode='{actual_cpu}'>
    <topology sockets='1' dies='1' cores='{vcpu}' threads='1'/>
  </cpu>"""
    else:
        cpu_xml = f"""<cpu mode='custom' match='exact'>
    <model>{actual_cpu}</model>
    <topology sockets='1' dies='1' cores='{vcpu}' threads='1'/>
  </cpu>"""

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
        import hashlib
        h = hashlib.md5(f"{name}_nic_{idx}".encode()).hexdigest()
        mac_addr = f"52:54:00:{h[0:2]}:{h[2:4]}:{h[4:6]}"
        
        parts = net_entry.split(":")
        net_id = parts[0]
        nic_model = parts[1] if len(parts) > 1 else "virtio"
        
        net = get_network_by_id(net_id)
        if net:
            if net.get("type") == "direct":
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
                interfaces_xml += f"""
    <interface type='direct'>
      <mac address='{mac_addr}'/>
      <source dev='{uplink_dev}' mode='bridge'/>
      <model type='{nic_model}'/>
    </interface>"""
            elif net.get("type") == "vlan" and net.get("vlan_id") is not None:
                vlan_id = net.get("vlan_id")
                interfaces_xml += f"""
    <interface type='bridge'>
      <mac address='{mac_addr}'/>
      <source bridge='br-vlan-{vlan_id}'/>
      <model type='{nic_model}'/>
    </interface>"""
            elif net.get("type") == "overlay" and net.get("vni") is not None:
                vni = net.get("vni")
                interfaces_xml += f"""
    <interface type='bridge'>
      <mac address='{mac_addr}'/>
      <source bridge='br-ov-{vni}'/>
      <model type='{nic_model}'/>
    </interface>"""
        else:
            interfaces_xml += f"""
    <interface type='bridge'>
      <mac address='{mac_addr}'/>
      <source bridge='virbr0'/>
      <model type='{nic_model}'/>
    </interface>"""

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
  {features_xml}
  {cpu_xml}
  {clock_xml}
  <devices>
    {disk_devices_xml}
    <input type='tablet' bus='usb'/>
    {interfaces_xml}
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

def select_best_start_host(memory_needed):
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
    
    # Scheduling is cluster-wide on Linstor

    best_host = None
    min_used_mem = float('inf')
    
    for h in hosts:
        ip = h["ip"]
        if ip in maintenance_ips:
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
                if used_mb < min_used_mem:
                    min_used_mem = used_mb
                    best_host = ip
                    
    # Fallback to any online host if memory checks are tight
    if not best_host:
        for h in hosts:
            ip = h["ip"]
            if ip in maintenance_ips:
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


def get_linstor_pending_sync():
    hosts = []
    try:
        import os, json
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = cdata.get("hosts", [])
    except Exception:
        pass
    
    ips = [h["ip"] for h in hosts] if hosts else ["127.0.0.1"]
    controllers_str = ",".join(ips)
    cmd = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor volume list"
    
    # Try querying from any online node in the cluster
    candidate_ips = ["127.0.0.1"] + ips
    rc = -1
    stdout = ""
    for ip in candidate_ips:
        rc, stdout, stderr = run_remote_spark(ip, cmd)
        if rc == 0:
            break
            
    if rc != 0:
        return -1
    if "Syncing" in stdout or "PausedSync" in stdout or "Inconsistent" in stdout:
        return 1
    return 0

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
            # 1. Choose host if target not specified
            memory = int(vm_data.get("memory", 1024))
            vcpu = int(vm_data.get("vcpu", 1))
            firmware = vm_data.get("firmware", "uefi")
            disks_list = vm_data.get("disks_list", "")
            iso = vm_data.get("iso", "")
            boot_device = vm_data.get("boot_device", "")
            
            selected_host = target_host if target_host else select_best_start_host(memory)
            if not selected_host:
                return False, "No active hypervisor host has sufficient memory."

            # 2. Claim the VM for that host before anything touches its disks.
            #
            # This used to be the last step of a start: the resources were promoted, the
            # domain defined and booted, and only then was host_ip written, unconditionally.
            # Two callers racing on the same VM -- a manual start against a DRS placement,
            # or a start issued while a stale host_ip was still in the row -- both reached
            # that point and both wrote, so two qemu processes ended up on the same raw
            # DRBD device. Claiming first means one of them is turned away here.
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

            # 3. Compile XML
            audio_enabled = bool(vm_data.get("audio_enabled", False))
            vm_xml = generate_vm_xml(vm_name, memory, vcpu, firmware, disks_list, iso, boot_device, host_ip=selected_host, network_id=vm_data.get("network_id"), cpu_model=vm_data.get("cpu_model"), audio_enabled=audio_enabled)
            import base64
            b64_xml = base64.b64encode(vm_xml.encode("utf-8")).decode("utf-8")
            
            # 4. Define and start VM via host spark-daemon
            restore_cmd = get_nvram_restore_cmd(vm_name)
            q_vm = shlex.quote(vm_name)

            # Promote the DRBD devices on the destination host as a separate CHECKED step before the
            # VM is defined or started. A promotion failure means the peer still holds Primary (the
            # VM is probably still running there behind a stale host_ip row), and booting anyway
            # would put two qemu processes on the same raw device.
            promoted = []
            if disks_list and disks_list != "NONE":
                for idx, entry in enumerate(disks_list.split(",")):
                    res_name = f"{vm_name}-disk{idx}"
                    q_res = shlex.quote(res_name)
                    rc_d, stdout_d, stderr_d = run_remote_spark(selected_host, f"drbdadm primary {q_res}")
                    if rc_d != 0:
                        # Release the resources we already promoted so this host does not sit on them
                        for done in promoted:
                            run_remote_spark(selected_host, f"drbdadm secondary {shlex.quote(done)} || true")
                        release_failed_claim(vm_name, selected_host)
                        return False, f"Refusing to start {vm_name}: DRBD resource {res_name} could not be promoted to Primary on {selected_host} ({(stderr_d or stdout_d).strip()}). The peer node may still hold Primary for this resource (the VM may still be running there)."
                    promoted.append(res_name)
                    run_remote_spark(selected_host, f"drbdadm resize {q_res} || true")

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
            run_remote_spark(host_ip, cmd)

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
            cmd = f"virsh -c qemu:///system {action} {shlex.quote(vm_name)}"
            rc, stdout, stderr = run_remote_spark(host_ip, cmd)
            if rc != 0:
                return False, f"Failed to execute {action} on host: {stderr.strip()}"
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

            # There is no read of `status` here on purpose: whether the VM is already
            # migrating is decided by the lock below, in the same operation that takes it.

            if src_host == target_ip:
                return True, target_ip
                
            # Perform basic target host checks: check all services are up, host not in maintenance mode
            rc_st, status_data, stderr_st = run_mtls_spark_api(target_ip, "/api/v1/node/status", None, method="GET")
            if rc_st != 0:
                return False, f"Target host {target_host} is not responding or spark-daemon is offline: {stderr_st}"
            try:
                maint = status_data.get("maintenance_status", "NORMAL")
                if maint != "NORMAL":
                    return False, f"Target host {target_host} is in maintenance mode ({maint})."
                
                services = status_data.get("services", {})
                offline_svcs = [sname for sname, sdata in services.items() if sdata.get("status") != "UP"]
                if offline_svcs:
                    return False, f"Target host {target_host} has offline services: {', '.join(offline_svcs)}."
            except Exception as e:
                return False, f"Failed to parse target host status: {e}"

            # Memory availability check on target host
            memory_needed = int(vm_data.get("ram", 1024))
            _, _, total_mb, used_mb = get_node_utilization(target_ip, fetch_cpu=False)
            avail_mb = total_mb - used_mb
            if avail_mb < memory_needed:
                return False, f"Target host {target_host} has insufficient free memory ({int(avail_mb)} MB available, needs {memory_needed} MB)."

            # Storage capacity gate check
            disk_size = get_vm_disk_size(vm_name)
            target_free = get_linstor_free_space(target_ip)
            if target_free < disk_size:
                return False, f"Target host {target_host} has insufficient free Linstor storage pool space ({target_free} MiB available, needs {disk_size} MiB)."

            # Take the migration lock. The condition and the write are one Paxos round, so
            # there is no window between them: previously the code read `status`, decided
            # nobody was migrating, and set it in a separate statement, which two callers
            # could both pass. Live migration is exactly the window in which DRBD
            # dual-primary is open, so two of them on one VM is disk corruption.
            print(f"Taking the migration lock for VM '{vm_name}'...")
            ok, applied, current, err = run_lwt("/v1/vm/migrate-lock", {"name": vm_name})
            if not ok:
                return False, f"Refusing to migrate {vm_name}: the migration lock could not be taken ({err}). Without it a concurrent migration of the same VM would not be detected."
            if not applied:
                return False, f"Refusing to migrate {vm_name}: it is already migrating (status = {current.get('status')!r})."

            try:
                q_vm = shlex.quote(vm_name)

                # 1. Force-backup NVRAM on source host to ScyllaDB
                backup_cmd = get_nvram_backup_cmd(vm_name, delete_local=False)
                run_remote_spark(src_host, backup_cmd)

                # 2. Restore NVRAM on target host (ensuring directories exist and files are written)
                restore_cmd = get_nvram_restore_cmd(vm_name)
                run_remote_spark(target_ip, restore_cmd)

                # 3. Pre-clean stale definition on target host
                run_remote_spark(target_ip, f"virsh -c qemu:///system undefine {q_vm} --keep-nvram || true")

                # 4. Live migrate command
                # --unsafe was needed when VM disks carried --allow-two-primaries permanently:
                # libvirt refuses a live migration it believes is cache-incoherent. The
                # two-primaries window is now opened and closed around the migration
                # itself, so the check libvirt performs is the one we actually want, and
                # suppressing it would only hide a genuinely unsafe migration.
                cmd = f"virsh -c qemu:///system migrate --live --persistent --undefinesource {q_vm} qemu+ssh://root@{target_ip}/system tcp://{target_ip}"
                rc, stdout, stderr = run_remote_spark(src_host, cmd)
                if rc != 0:
                    raise Exception(f"Migration command failed: {stderr.strip() or stdout.strip()}")

                # 5. Clean up local NVRAM file on source host after successful migration
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

                # Insert record in history
                now = int(time.time() * 1000)
                reason = task.get("error_msg", "Manual VM migration request") # Re-use error_msg for trigger reason
                clean_reason = reason.replace("'", "''")
                cql_history = f"""
                INSERT INTO hydra.vali_drs_history (event_time, vm_name, source_host, target_host, reason)
                VALUES ({now}, '{vm_name}', '{src_host}', '{target_ip}', '{clean_reason}');
                """
                run_cql_query(cql_history)
                return True, target_ip
            except Exception as migrate_err:
                print(f"Migration error caught: {migrate_err}. Releasing the migration lock...")
                # Conditional on the lock still being held by this migration. An
                # unconditional reset here would clear the lock of whichever migration is
                # running now, which is how a late cleanup from a failed attempt lets a
                # second live migration of the same VM start.
                ok_u, applied_u, current_u, err_u = run_lwt("/v1/vm/migrate-unlock", {"name": vm_name})
                if not ok_u:
                    sys.stderr.write(f"VM {vm_name}: migration failed and the lock could not be released ({err_u}). Further migrations will be refused until it is cleared.\n")
                elif not applied_u:
                    sys.stderr.write(f"VM {vm_name}: migration failed, but the lock is no longer this migration's to release (status = {current_u.get('status')!r}); left alone.\n")
                return False, f"Migration failed: {str(migrate_err)}"

        elif action == "host_maintenance_enter":
            hostname = payload.get("hostname")
            target_ip = payload.get("target_ip")
            force_stop = payload.get("force_stop", False)
            # Taken by the API handler before this task was submitted; renewed below for
            # as long as the evacuation runs, and given back on every failure path.
            lock_token = payload.get("lock_token", "")

            if not is_valid_object_name(hostname) or not is_valid_object_name(target_ip):
                release_maintenance_lock(lock_token)
                return False, "Invalid payload parameters for host_maintenance_enter."

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
                    print(f"[Maintenance Catalyst Task] Host {hostname} status reverted to NORMAL. Its VMs have already been evacuated; DRS will rebalance them.")
                    return False, f"Refused to enter maintenance mode: {quorum_reason}"

                # Mark node status as IN_MAINTENANCE in DB
                cql = f"UPDATE hydra.nodes SET status = 'IN_MAINTENANCE', maintenance_mode = true WHERE hostname = '{hostname}';"
                run_cql_query(cql)
                print(f"[Maintenance Catalyst Task] Host {hostname} successfully entered maintenance mode. {quorum_reason}")

                # The lock stays held for as long as the host is in maintenance, and is
                # renewed from here on by Mipha's control loop -- this task is about to
                # end, and a lock nobody renews would expire five minutes into a
                # maintenance window that lasts hours.
                renew_maintenance_lock(hostname, lock_token, f"{hostname} is in maintenance")

                # Write state file and stop all cluster services on the target host (except spark-daemon)
                run_remote_spark(target_ip, "mkdir -p /etc/hci && touch /etc/hci/maintenance.state")
                stop_cmd = "sleep 2 && systemctl stop spectrum catalyst bifrost dagur mimir vali aether linstor-controller hydra-db gatoway urbosa logos mipha daruk agahnim slate"

                # Update task progress to 100 before running the stop command, so the task status is marked completed in ScyllaDB
                call_catalyst_api("/api/v1/tasks/update", {
                    "task_id": task_id,
                    "status": "completed",
                    "progress": 100
                }, method="POST")

                # Run the remote service shutdown in the background
                run_remote_spark(target_ip, f"({stop_cmd}) >/dev/null 2>&1 < /dev/null &")
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
            run_remote_spark(target_ip, "rm -f /etc/hci/maintenance.state")
            
            # Update task progress
            call_catalyst_api("/api/v1/tasks/update", {
                "task_id": task_id,
                "status": "processing",
                "progress": 15
            }, method="POST")
            
            print(f"[Maintenance Catalyst Task] Starting services on host {hostname}...")
            start_cmd = "systemctl start zookeeper hydra-db aether linstor-controller spectrum bifrost dagur mimir vali catalyst gatoway urbosa logos mipha daruk agahnim slate"
            run_remote_spark(target_ip, start_cmd)
            
            # Update task progress
            call_catalyst_api("/api/v1/tasks/update", {
                "task_id": task_id,
                "status": "processing",
                "progress": 30
            }, method="POST")
            
            # Wait for services on the leaving node to bootstrap and stabilize
            print(f"[Maintenance Catalyst Task] Waiting 10 seconds for services on host {hostname} to stabilize...")
            time.sleep(10)
            
            # Set status to RECOVERING in ScyllaDB (not NORMAL yet!)
            cql_up = f"UPDATE hydra.nodes SET status = 'RECOVERING', maintenance_mode = false WHERE hostname = '{hostname}';"
            run_cql_query(cql_up)
            


            # Linstor Satellite auto-heal triggered automatically via DRBD reconnection
            print(f"[Maintenance Catalyst Task] Linstor Satellite auto-heal triggered automatically via DRBD reconnection on host {hostname}.")
            
            # Create child Catalyst task for sync
            import uuid
            child_task_id = str(uuid.uuid4())
            now_ms = int(time.time() * 1000)
            child_payload = json.dumps({"hostname": hostname, "parent_task_id": task_id})
            cql_child = f"""
            INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
            VALUES ({child_task_id}, 'aether', 'sync', 'processing', '{child_payload.replace("'", "''")}', 10, {now_ms}, {now_ms});
            """
            run_cql_query(cql_child)
            
            # Poll sync status
            synced = False
            # Poll up to 60 iterations (3 minutes)
            for iteration in range(60):
                child_progress = min(95, 10 + iteration * 5)
                # Map child progress (10%-95%) to parent task progress (40%-90%)
                parent_progress = int(40 + (child_progress / 100.0) * 50)
                
                cql_up_child = f"UPDATE hydra.catalyst_tasks SET progress = {child_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {child_task_id};"
                run_cql_query(cql_up_child)
                
                call_catalyst_api("/api/v1/tasks/update", {
                    "task_id": task_id,
                    "status": "processing",
                    "progress": parent_progress
                }, method="POST")
                
                pending = get_linstor_pending_sync()
                print(f"[Maintenance Catalyst Task] Linstor sync status - pending status: {pending}")
                if pending == 0:
                    synced = True
                    print(f"[Maintenance Catalyst Task] Linstor DRBD volumes fully synced on host {hostname}!")
                    break
                    
                time.sleep(3)
                
            now_ms_end = int(time.time() * 1000)
            if synced:
                # Set child task to completed
                cql_child_end = f"UPDATE hydra.catalyst_tasks SET status = 'completed', progress = 100, updated_at = {now_ms_end} WHERE task_id = {child_task_id};"
                run_cql_query(cql_child_end)
                
                # Set node status to NORMAL
                cql_normal = f"UPDATE hydra.nodes SET status = 'NORMAL' WHERE hostname = '{hostname}';"
                run_cql_query(cql_normal)
                print(f"[Maintenance Catalyst Task] Host {hostname} rejoin and sync completed successfully.")

                # The host is back in the ring and back in service, so the cluster
                # maintenance lock is finally free. It is released here rather than when
                # `leave` was requested: until the storage has resynced this host is not
                # a replica anyone should be counting on, and letting a second host start
                # draining in that window is the case the lock exists to stop.
                #
                # The token from the acquisition is long gone -- `leave` can arrive hours
                # later, and after a leader change this is not even the same process --
                # so the row is read and released on the token it carries.
                release_maintenance_lock_for_host(hostname)
            else:
                err_msg = "DRBD volume sync timed out or failed to complete self-heal."
                cql_child_end = f"UPDATE hydra.catalyst_tasks SET status = 'failed', progress = 100, error_msg = '{err_msg}', updated_at = {now_ms_end} WHERE task_id = {child_task_id};"
                run_cql_query(cql_child_end)
                # The lock stays held. The host did not finish rejoining, so the cluster
                # is still short a replica and no other host should start draining. Mipha
                # keeps renewing it while hydra.nodes says RECOVERING; if this node is
                # abandoned instead of fixed, the TTL frees it.
                print(f"[Maintenance Catalyst Task] ERROR: Sync not complete for {hostname}. The cluster maintenance lock is still held by {hostname}.")

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

        elif action == "balance":
            aggressive = payload.get("aggressive", False)
            run_drs_loop(aggressive=aggressive)
            return True, ""
            
    except Exception as e:
        return False, str(e)
        
    return False, "Unsupported action."

# Queue worker thread
def queue_thread_loop():
    print("Vali Catalyst worker thread started.")
    while True:
        try:
            if not is_zookeeper_leader():
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
# the source of truth for this table.
#
# It is repeated here because nothing calls helios_schema.ensure_schema yet and
# helios_schema.py is not in the deployment toolkit's upload list, so migration 0002
# cannot reach a node. Without this the first maintenance request after an upgrade fails
# on an unconfigured table and the host cannot be drained at all. Delete it once the
# migration runner is wired into daemon startup.
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


def parse_replication_factor(text):
    """The number of replicas the hydra keyspace declares, or None if it cannot be read.

    The replication map arrives as a stringified dict whichever way it is fetched --
    Daruk flattens result rows into space-joined `str(value)`, and the cqlsh fallback
    prints the same shape -- so this reads the pairs out of the text rather than
    expecting a real mapping. The separator is `[:,]` because the driver's own
    `OrderedMapSerializedKey` reprs its pairs as tuples rather than with colons, and the
    difference is invisible until the gate quietly reports "replication factor unknown"
    and refuses every maintenance request.

    NetworkTopologyStrategy spreads the factor across datacenters and QUORUM is computed
    from their sum, so the per-datacenter values are added. LocalStrategy and
    EverywhereStrategy have no replication factor at all and give None.
    """
    if not text:
        return None
    lowered = text.lower()
    if "localstrategy" in lowered or "everywherestrategy" in lowered:
        return None
    pairs = re.findall(r"['\"]([^'\"]+)['\"]\s*[:,]\s*['\"]?(\d+)['\"]?", text)
    factors = {key: int(value) for key, value in pairs if key != "class"}
    if "replication_factor" in factors:
        return factors["replication_factor"]
    if not factors:
        return None
    return sum(factors.values())


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
    rc, stdout, stderr = run_remote_spark(LOCAL_IP, "nodetool status")
    if rc != 0:
        return [], (stderr or stdout or "nodetool status failed").strip()[:300]
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
        # Witness nodes run no ScyllaDB at all, so they hold no replicas and stopping
        # one costs the ring nothing.
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


def evacuate_host_thread(hostname, target_ip, force_stop):
    # Nothing calls this any more -- the maintenance API submits a Catalyst task instead
    # -- but it is a second, complete copy of the enter sequence including the
    # unconditional `systemctl stop ... hydra-db ...`, so it is gated too rather than
    # left as a way back into the bug.
    quorum_ok, quorum_reason = check_stop_preserves_quorum(target_ip)
    if not quorum_ok:
        sys.stderr.write(f"[Maintenance] Refusing to evacuate {hostname}: {quorum_reason}\n")
        return
    try:
        # Get all VMs running on this host
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
        
        if not running_vms:
            # No VMs running, transition directly to IN_MAINTENANCE!
            cql = f"UPDATE hydra.nodes SET status = 'IN_MAINTENANCE', maintenance_mode = true WHERE hostname = '{hostname}';"
            run_cql_query(cql)
            print(f"[Maintenance] Host {hostname} entered maintenance mode (no VMs to evacuate).")
            
            # Write state file and stop all cluster services on the target host (except spark-daemon)
            run_remote_spark(target_ip, "mkdir -p /etc/hci && touch /etc/hci/maintenance.state")
            stop_cmd = "systemctl stop spectrum catalyst bifrost dagur mimir vali aether linstor-controller hydra-db gatoway urbosa logos mipha daruk agahnim slate"
            run_remote_spark(target_ip, f"({stop_cmd}) >/dev/null 2>&1 < /dev/null &")
            return

        print(f"[Maintenance] Evacuating {len(running_vms)} VMs from host {hostname} ({target_ip})...")
        
        success = True
        failed_vm = ""
        fail_reason = ""
        
        for vm in running_vms:
            vm_name = vm.get("name")
            vm_mem = int(vm.get("memory", 1024))
            
            # Select best target host excluding this one
            dest_host = select_best_start_host(vm_mem)
            if not dest_host:
                if force_stop:
                    print(f"[Maintenance] No migration target host for {vm_name}. Force stopping VM.")
                    stop_success, stop_err, _ = submit_and_wait_task("vali", "stop", {"vm_name": vm_name}, timeout_polls=POWER_TIMEOUT_POLLS)
                    if not stop_success:
                        print(f"[Maintenance] Force stopping failed for {vm_name}: {stop_err}")
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
                task_success, task_err, _ = submit_and_wait_task("vali", "migrate", {"vm_name": vm_name, "target_host": dest_host}, timeout_polls=MIGRATE_TIMEOUT_POLLS)
                
                if not task_success:
                    if force_stop:
                        print(f"[Maintenance] Migration failed for {vm_name}: {task_err}. Force stopping VM.")
                        stop_success, stop_err, _ = submit_and_wait_task("vali", "stop", {"vm_name": vm_name}, timeout_polls=POWER_TIMEOUT_POLLS)
                        if not stop_success:
                            print(f"[Maintenance] Force stopping failed for {vm_name}: {stop_err}")
                    else:
                        success = False
                        failed_vm = vm_name
                        fail_reason = f"Migration failed: {task_err}"
                        break
                    
        if success:
            cql = f"UPDATE hydra.nodes SET status = 'IN_MAINTENANCE', maintenance_mode = true WHERE hostname = '{hostname}';"
            run_cql_query(cql)
            print(f"[Maintenance] Host {hostname} successfully entered maintenance mode.")
            
            # Write state file and stop all cluster services on the target host (except spark-daemon)
            run_remote_spark(target_ip, "mkdir -p /etc/hci && touch /etc/hci/maintenance.state")
            stop_cmd = "systemctl stop spectrum catalyst bifrost dagur mimir vali aether linstor-controller hydra-db gatoway urbosa logos mipha daruk agahnim slate"
            run_remote_spark(target_ip, f"({stop_cmd}) >/dev/null 2>&1 < /dev/null &")
        else:
            # Revert status to NORMAL
            cql = f"UPDATE hydra.nodes SET status = 'NORMAL', maintenance_mode = false WHERE hostname = '{hostname}';"
            run_cql_query(cql)
            print(f"[Maintenance] Evacuation failed for host {hostname} on VM {failed_vm}: {fail_reason}. Host status reverted to NORMAL.")
            
    except Exception as e:
        sys.stderr.write(f"Error in host evacuation thread: {e}\n")
        cql = f"UPDATE hydra.nodes SET status = 'NORMAL', maintenance_mode = false WHERE hostname = '{hostname}';"
        run_cql_query(cql)

# REST HTTP Handlers
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
                
        elif self.path == "/api/v1/vms/balance":
            aggressive = payload.get("aggressive", False)
            # Trigger DRS check in a background thread immediately
            threading.Thread(target=run_drs_loop, args=(aggressive,), daemon=True).start()
            self.send_json(200, {"status": "triggered"})
        elif self.path == "/api/v1/hosts/maintenance":
            hostname = payload.get("hostname")
            action = payload.get("action")
            force_stop = payload.get("force_stop", False)
            
            if not hostname or not action:
                self.send_json(400, {"error": "Parameters hostname and action required."})
                return
            if not is_valid_object_name(hostname):
                self.send_json(400, {"error": f"Invalid hostname: {OBJECT_NAME_ERROR}."})
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
                self.send_json(404, {"error": f"Host '{hostname}' not found in cluster database."})
                return
                
            target_ip = host_info.get("ip")
            
            if action == "enter":
                # Without an address there is no way to ask the ring about this host, and
                # the quorum gate would find no matching ring member and read that as
                # "holds no replicas" -- waving through the one case it exists to catch.
                if not target_ip:
                    self.send_json(409, {"error": f"Host '{hostname}' has no IP recorded in hydra.nodes, so its effect on the ScyllaDB ring cannot be established."})
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
                    self.send_json(409, {
                        "error": f"Host '{hostname}' cannot enter maintenance mode: {quorum_reason}",
                        "reason": "quorum",
                    })
                    return

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
                    self.send_json(500, {"error": f"Could not take the cluster maintenance lock: {lock_err}"})
                    return
                if not lock_token:
                    holder = lock_current.get("holder") or "another host"
                    self.send_json(409, {
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
                    "expected_status": "NORMAL",
                })
                # Every exit from here on has to give the lock back. Leaving it held on a
                # request that never started a transition blocks maintenance cluster-wide
                # until the TTL runs out.
                if not ok:
                    release_maintenance_lock(lock_token)
                    self.send_json(500, {"error": f"Could not begin the maintenance transition for '{hostname}': {err}"})
                    return
                if not applied:
                    release_maintenance_lock(lock_token)
                    self.send_json(409, {"error": f"Host '{hostname}' is in state '{current.get('status')}', not NORMAL, so it cannot enter maintenance mode now."})
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
                        "lock_token": lock_token
                    }
                }, method="POST")

                if status_api == 200:
                    task_id = res_api.get("task_id")
                    self.send_json(200, {"status": "transitioning", "task_id": task_id, "message": f"Entering maintenance mode task submitted (Task ID: {task_id})."})
                else:
                    # Nothing is going to run, so put the host and the lock back.
                    run_lwt("/v1/node/maintenance", {
                        "hostname": hostname,
                        "status": "NORMAL",
                        "maintenance_mode": False,
                        "expected_status": "ENTERING_MAINTENANCE",
                    })
                    release_maintenance_lock(lock_token)
                    self.send_json(500, {"error": f"Failed to submit host maintenance task to Catalyst: {res_api}"})

            elif action == "leave":
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
                    self.send_json(200, {"status": "transitioning", "task_id": task_id, "message": f"Leaving maintenance mode task submitted (Task ID: {task_id})."})
                else:
                    self.send_json(500, {"error": f"Failed to submit host maintenance leave task to Catalyst: {res_api}"})
                
            else:
                self.send_json(400, {"error": f"Invalid action '{action}'."})
        else:
            self.send_json(404, {"error": "Not Found"})

    def send_json(self, status, data):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

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

    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(certfile=node_cert, keyfile=node_key)
    ssl_context.load_verify_locations(cafile=ca_cert)
    ssl_context.verify_mode = ssl.CERT_REQUIRED

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
