#!/usr/bin/env python3
import sys
import json
import ssl
import socket
import subprocess
import urllib.request
import time
import os
import threading

LOCAL_IP = "127.0.0.1"
try:
    with open("/etc/hci/spectrum/spectrum.env", "r") as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                if k == "LOCAL_HYPERVISOR_IP":
                    LOCAL_IP = v
except Exception:
    pass

def run_remote_spark(ip, command):
    """Executes a command on local/remote node via spark-daemon mTLS API."""
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = False
    
    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=15) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)

def run_mtls_api(ip, path, payload, method="POST"):
    import urllib.error
    
    def execute_request(target_ip):
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
        context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
        context.check_hostname = False
        url = f"https://{target_ip}:9099{path}"
        data = None
        if payload is not None and method != "GET":
            data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, context=context, timeout=120) as response:
                res = json.loads(response.read().decode("utf-8"))
                return 0, res, ""
        except urllib.error.HTTPError as e:
            try:
                res = json.loads(e.read().decode("utf-8"))
                return 0, res, ""
            except Exception:
                return -1, {}, str(e)
        except Exception as e:
            return -1, {}, str(e)

    rc, res, err = execute_request(ip)
    if ip == "127.0.0.1" and (rc != 0 or "error" in res):
        # Try failover to other cluster nodes
        ips = []
        try:
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                ips = [h["ip"] for h in cdata.get("hosts", [])]
        except Exception:
            pass
        for other_ip in ips:
            if other_ip != "127.0.0.1":
                rc_alt, res_alt, err_alt = execute_request(other_ip)
                if rc_alt == 0 and "error" not in res_alt:
                    return rc_alt, res_alt, err_alt
    return rc, res, err

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
def print_table(headers, rows):
    """Prints a beautiful ASCII table from headers and row list."""
    if not rows:
        print("No records found.")
        return
        
    str_rows = [[str(val) for val in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in str_rows:
        for idx, val in enumerate(row):
            widths[idx] = max(widths[idx], len(val))
            
    sep = "+" + "+".join(["-" * (w + 2) for w in widths]) + "+"
    print(sep)
    header_line = "| " + " | ".join([f"{h:<{widths[idx]}}" for idx, h in enumerate(headers)]) + " |"
    print(header_line)
    print(sep)
    for row in str_rows:
        row_line = "| " + " | ".join([f"{val:<{widths[idx]}}" for idx, val in enumerate(row)]) + " |"
        print(row_line)
    print(sep)

def cmd_vm_list():
    # Fetch hostnames to IPs map
    host_map = {}
    rc_n, stdout_n, _ = run_cql_query("SELECT JSON hostname, ip FROM hydra.nodes;")
    if rc_n == 0 and stdout_n:
        for line in stdout_n.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    node = json.loads(line)
                    if node.get("ip") and node.get("hostname"):
                        host_map[node["ip"]] = node["hostname"]
                except:
                    pass

    cql = "SELECT JSON name, vcpu, memory, disk_size, state, host_ip FROM hydra.vms;"
    rc, stdout, err = run_cql_query(cql)
    if rc != 0:
        print(err)
        sys.exit(1)
        
    records = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
                
    headers = ["VM Name", "vCPUs", "Memory (MB)", "Disk (GB)", "Host", "Status"]
    rows = []
    for r in records:
        ip = r.get("host_ip")
        if not ip or ip == "None" or ip == "N/A":
            host_display = "N/A"
        else:
            host_display = f"{host_map.get(ip, ip)} ({ip})" if ip in host_map else ip
            
        rows.append([
            r.get("name", "N/A"),
            r.get("vcpu", 1),
            r.get("memory", 1024),
            r.get("disk_size", 10),
            host_display,
            r.get("state", "Stopped")
        ])
    print_table(headers, rows)

def cmd_vm_on(name):
    print(f"Requesting power-on for VM '{name}'...")
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/vm/power", {"name": name, "action": "on"})
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error starting VM: {res['error']}")
        sys.exit(1)
    print(f"Success: {res.get('message', 'VM powered on.')}")

def cmd_vm_off(name):
    print(f"Requesting power-off for VM '{name}'...")
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/vm/power", {"name": name, "action": "off"})
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error stopping VM: {res['error']}")
        sys.exit(1)
    print(f"Success: {res.get('message', 'VM powered off.')}")

def cmd_vm_migrate(name, target_host):
    print(f"Requesting migration for VM '{name}' to host {target_host}...")
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/vm/migrate", {"name": name, "target_host": target_host})
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error migrating VM: {res['error']}")
        sys.exit(1)
    print(f"Success: {res.get('message', 'VM migration triggered.')}")

def cmd_vm_balance():
    print("Requesting manual cluster load rebalancing (DRS)...")
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/vm/balance", {"aggressive": True})
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error rebalancing cluster: {res['error']}")
        sys.exit(1)
    print(f"Success: {res.get('message', 'DRS rebalancing initiated.')}")

def cmd_drs_status():
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/vm/drs", {}, method="GET")
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error querying DRS status: {res['error']}")
        sys.exit(1)
        
    print("==========================================================")
    print("                 DRS Load Balancing Status                ")
    print("==========================================================")
    deviation = res.get("current_deviation", 0.0)
    balance_score = max(0, min(100, int((1 - 2 * deviation) * 100)))
    print(f"Cluster Balance Score : {balance_score}%")
    print(f"Standard Deviation    : {deviation:.4f}")
    print(f"Status String         : {res.get('status_str', 'N/A')}")
    
    last_run = res.get("last_drs_run", 0)
    last_run_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_run)) if last_run else "N/A"
    print(f"Last DRS Run Timestamp: {last_run_str}")
    
    print("\n--- Migration History ---")
    history = res.get("history", [])
    if history:
        headers = ["Time", "VM Name", "Source Host", "Target Host", "Reason"]
        rows = []
        for h in history:
            t_val = h.get("event_time", "")
            if isinstance(t_val, (int, float)):
                t_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t_val / 1000.0))
            else:
                t_str = str(t_val)
            rows.append([
                t_str,
                h.get("vm_name", "N/A"),
                h.get("source_host", "N/A"),
                h.get("target_host", "N/A"),
                h.get("reason", "N/A")
            ])
        print_table(headers, rows)
    else:
        print("No recent DRS migration events.")
    print("==========================================================")

def cmd_storage_list():
    cql = "SELECT JSON name, tier, quota_bytes, path, ftt FROM hydra.storage_containers;"
    rc, stdout, err = run_cql_query(cql)
    if rc != 0:
        print(err)
        sys.exit(1)
        
    records = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
                
    headers = ["Container Name", "Storage Tier", "Quota (GB)", "POSIX Path", "FTT"]
    # Detect host count for FTT override
    hosts_count = 1
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts_count = len(cdata.get("hosts", []))
    except Exception:
        pass

    rows = []
    for r in records:
        quota_bytes = r.get("quota_bytes", 0)
        quota_str = f"{quota_bytes // (1024**3)} GB" if quota_bytes > 0 else "Unlimited"
        ftt_val = r.get("ftt", 1)
        if hosts_count <= 1:
            ftt_val = 0
        rows.append([
            r.get("name", "N/A"),
            r.get("tier", "SSD"),
            quota_str,
            r.get("path", "N/A"),
            ftt_val
        ])
    print("=== Aether Storage Containers ===")
    print_table(headers, rows)
    print()
    


    # Standardized on Linstor/DRBD storage engine
    controllers_str = "127.0.0.1"
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = cdata.get("hosts", [])
                if hosts:
                    controllers_str = ",".join([h["ip"] for h in hosts])
    except Exception:
        pass

    print("=== Aether Linstor Node Status ===")
    res_nodes = subprocess.run(f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor node list", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res_nodes.returncode != 0:
        print("Warning: Could not query Aether volume list (systemd-aether not active or storage daemon offline).")
        return
    print(res_nodes.stdout.decode("utf-8", errors="ignore").strip())
    print()
    
    print("=== Aether Linstor Volume Status ===")
    res_vols = subprocess.run(f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor volume list", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res_vols.returncode != 0:
        print("Warning: Could not query Aether volume list (systemd-aether not active or storage daemon offline).")
        return
    print(res_vols.stdout.decode("utf-8", errors="ignore").strip())

def cmd_db_print():
    if len(sys.argv) < 3:
        print("Error: Table name is required.")
        print("Usage: valcli db.print <table_name> [--columns col1,col2,...]")
        sys.exit(1)
        
    table_name = sys.argv[2]
    
    # Check for columns flag
    filter_cols = None
    if "--columns" in sys.argv:
        try:
            idx = sys.argv.index("--columns")
            filter_cols = [c.strip() for c in sys.argv[idx+1].split(",")]
        except Exception:
            print("Error: Invalid --columns format.")
            sys.exit(1)
            
    cql = f"SELECT JSON * FROM hydra.{table_name};"
    rc, stdout, err = run_cql_query(cql)
    if rc != 0:
        print(err)
        sys.exit(1)
        
    records = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
                
    if not records:
        print(f"No records found in table 'hydra.{table_name}'.")
        return
        
    # Set headers
    if filter_cols:
        headers = [c for c in filter_cols if c in records[0]]
        if not headers:
            print("Error: None of the specified columns exist in the table.")
            sys.exit(1)
    else:
        # Defaults for known tables to make them look nice
        known_headers = {
            "vms": ["name", "vcpu", "memory", "disk_path", "disk_size", "state", "host_ip"],
            "storage_containers": ["name", "tier", "quota_bytes", "path", "ftt"],
            "mimir_schedules": ["schedule_name", "category", "cron_expression", "enabled", "last_run_epoch"],
            "mimir_results": ["category", "check_name", "node_ip", "status", "timestamp", "execution_id"],
            "dagur_schedules": ["job_name", "task_type", "interval_seconds", "enabled", "command"],
            "dagur_runs": ["job_name", "start_time", "end_time", "status", "exit_code"]
        }
        if table_name in known_headers:
            headers = [h for h in known_headers[table_name] if h in records[0]]
        else:
            headers = sorted(records[0].keys())
            
    rows = []
    for r in records:
        rows.append([r.get(col, "N/A") for col in headers])
        
    print_table(headers, rows)

def cmd_db_query():
    if len(sys.argv) < 3:
        print("Error: CQL query string is required.")
        print("Usage: valcli db.query \"<cql_query>\"")
        sys.exit(1)
        
    query = sys.argv[2]
    rc, stdout, err = run_cql_query(query)
    if stdout:
        print(stdout)
    if err:
        print(err)
    if rc != 0:
        sys.exit(rc)

def cmd_storage_benchmark(container_name):
    # Resolve controller IPs
    controllers_str = "127.0.0.1"
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = cdata.get("hosts", [])
                if hosts:
                    controllers_str = ",".join([h["ip"] for h in hosts])
    except Exception:
        pass

    import uuid
    bench_id = str(uuid.uuid4())[:8]
    res_name = f"bench-temp-{bench_id}"
    local_hostname = socket.gethostname()
    
    print(f"Creating temporary DRBD volume '{res_name}' in storage pool '{container_name}'...")
    
    # 1. Create resource definition
    cmd_def = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor resource-definition create {res_name}"
    rc = subprocess.run(cmd_def, shell=True).returncode
    if rc != 0:
        print("Error: Failed to create temporary resource definition in Linstor.")
        sys.exit(1)
        
    # 2. Create volume definition (100MB)
    cmd_vol = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor volume-definition create {res_name} 100M"
    rc = subprocess.run(cmd_vol, shell=True).returncode
    if rc != 0:
        print("Error: Failed to create temporary volume definition in Linstor.")
        # Cleanup definition
        subprocess.run(f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor resource-definition delete {res_name}", shell=True)
        sys.exit(1)
        
    # 3. Create resource on local host
    cmd_res = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor resource create {local_hostname} {res_name} --storage-pool {container_name}"
    rc = subprocess.run(cmd_res, shell=True).returncode
    if rc != 0:
        print(f"Error: Failed to deploy temporary resource on {local_hostname}. Pool '{container_name}' might not exist.")
        # Cleanup
        subprocess.run(f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor resource-definition delete {res_name}", shell=True)
        sys.exit(1)
        
    # 4. Wait for DRBD block device to appear
    dev_path = f"/dev/drbd/by-res/{res_name}/0"
    print(f"Waiting for block device {dev_path} to appear...")
    device_ready = False
    for _ in range(15):
        if os.path.exists(dev_path):
            device_ready = True
            break
        time.sleep(1)
        
    if not device_ready:
        print(f"Error: Temporary block device {dev_path} failed to appear within 15 seconds.")
        # Cleanup
        subprocess.run(f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor resource-definition delete {res_name}", shell=True)
        sys.exit(1)
        
    # 5. Perform the benchmark
    print(f"Benchmarking storage pool '{container_name}' via raw block device {dev_path}...")
    file_size_mb = 100
    chunk_size = 1024 * 1024  # 1 MB
    data = b"0" * chunk_size
    
    try:
        # Write test
        print(f"[1/3] Writing {file_size_mb} MB of data...")
        start_write = time.time()
        with open(dev_path, "wb") as f:
            for _ in range(file_size_mb):
                f.write(data)
            f.flush()
            os.fsync(f.fileno())
        end_write = time.time()
        write_time = end_write - start_write
        write_speed = file_size_mb / write_time if write_time > 0 else 0
        print(f"      Write Speed: {write_speed:.2f} MB/s (took {write_time:.2f}s)")
        
        # Read test
        print(f"[2/3] Reading {file_size_mb} MB of data...")
        start_read = time.time()
        with open(dev_path, "rb") as f:
            while f.read(chunk_size):
                pass
        end_read = time.time()
        read_time = end_read - start_read
        read_speed = file_size_mb / read_time if read_time > 0 else 0
        print(f"      Read Speed:  {read_speed:.2f} MB/s (took {read_time:.2f}s)")
        
    except Exception as ex:
        print(f"Error during benchmark: {ex}")
    finally:
        print("[3/3] Cleaning up temporary Linstor/DRBD resource...")
        # Demote to secondary just in case
        subprocess.run(f"drbdadm secondary {res_name} || true", shell=True)
        # Delete resource definition
        cmd_del = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor resource-definition delete {res_name}"
        subprocess.run(cmd_del, shell=True)
        
    print("Benchmark completed.")

def cmd_storage_cleanup_orphaned():
    import glob
    import re
    
    print("Fetching active virtual machines from database...")
    active_vms = set()
    rc, stdout, stderr = run_cql_query("SELECT JSON name FROM hydra.vms;")
    if rc != 0:
        print(f"Error querying active VMs: {stderr or stdout}")
        sys.exit(1)
        
    if stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    if "name" in row and row["name"]:
                        active_vms.add(row["name"])
                except Exception:
                    pass
                    
    print(f"Found {len(active_vms)} active VM(s) in the cluster.")
    
    raw_files = glob.glob("/var/lib/hci/aether/volumes/*/*.raw")
    vars_files = glob.glob("/var/lib/hci/aether/volumes/*/*_vars.fd")
    
    orphaned_files = []
    for file_path in raw_files + vars_files:
        normalized = file_path.replace("\\", "/")
        if "default-image-container" in normalized:
            continue
            
        filename = os.path.basename(normalized)
        if filename.endswith(".raw"):
            base = filename[:-4]
            # Match <vm_name>_disk<idx>
            match = re.match(r"^(.*)_disk\d+$", base)
            if match:
                vm_name = match.group(1)
            else:
                vm_name = base
        elif filename.endswith("_vars.fd"):
            vm_name = filename[:-8]
        else:
            continue
            
        if vm_name not in active_vms:
            orphaned_files.append(normalized)
            
    if not orphaned_files:
        print("No orphaned virtual disk or NVRAM files found.")
        return
        
    print(f"Found {len(orphaned_files)} orphaned file(s) to clean up:")
    for file_path in orphaned_files:
        print(f"  - {file_path}")
        
    # Perform deletion
    deleted_count = 0
    for file_path in orphaned_files:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Successfully deleted: {file_path}")
                deleted_count += 1
            else:
                print(f"File not found (already deleted): {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")
            
    print(f"Orphaned storage cleanup complete. Deleted {deleted_count} file(s).")

def format_size(bytes_val):
    if bytes_val is None:
        return "N/A"
    try:
        bytes_val = float(bytes_val)
    except:
        return "N/A"
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PiB"

def cmd_image_list():
    # 1. Query ScyllaDB using SELECT JSON to avoid delimiter issues
    cql = "SELECT JSON name, filename, size_bytes, type, path FROM hydra.valhalla_images;"
    rc, stdout, err = run_cql_query(cql)
    db_images = []
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    db_images.append(json.loads(line))
                except Exception:
                    pass

    # 2. Query Linstor volume definitions
    controllers_str = "127.0.0.1"
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = cdata.get("hosts", [])
                if hosts:
                    controllers_str = ",".join([h["ip"] for h in hosts])
    except Exception:
        pass

    cmd_vols = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor -m volume-definition list"
    p = subprocess.run(cmd_vols, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    lin_vols = []
    if p.returncode == 0 and p.stdout:
        try:
            raw_vols = json.loads(p.stdout.decode("utf-8", errors="ignore"))
            if raw_vols and isinstance(raw_vols[0], list):
                lin_vols = [item for sublist in raw_vols for item in sublist]
            else:
                lin_vols = raw_vols
        except Exception:
            pass

    # Filter Linstor resources starting with img-
    lin_img_vols = {v["name"]: v for v in lin_vols if v["name"].startswith("img-")}

    # Merge and label
    records = {}

    # Process ScyllaDB images
    for db_img in db_images:
        name = db_img.get("name")
        if not name:
            continue
        path = db_img.get("path", "")
        size_bytes = db_img.get("size_bytes")
        img_type = db_img.get("type", "N/A")
        
        # Extract resource name from path
        lin_res = None
        if path.startswith("/dev/drbd/by-res/"):
            parts = path.split("/")
            if len(parts) >= 5:
                lin_res = parts[4]
                
        has_lin = False
        lin_size = None
        if lin_res and lin_res in lin_img_vols:
            has_lin = True
            vdef = lin_img_vols[lin_res]
            if vdef.get("volume_definitions"):
                lin_size = vdef["volume_definitions"][0].get("size_kib", 0) * 1024
                
        size_disp = "N/A"
        if size_bytes and size_bytes != 'None' and size_bytes != 'null':
            size_disp = format_size(size_bytes)
        elif lin_size is not None:
            size_disp = format_size(lin_size)
            
        records[name] = {
            "name": name,
            "type": img_type,
            "size": size_disp,
            "scylla": "Yes",
            "linstor": "Yes" if has_lin else "No",
            "status": "Active" if has_lin else "Missing Storage"
        }

    # Process remaining Linstor image volumes (orphaned)
    for lin_name, vdef in lin_img_vols.items():
        referenced = False
        for db_img in db_images:
            path = db_img.get("path", "")
            if f"/by-res/{lin_name}/" in path or path.endswith(f"/{lin_name}"):
                referenced = True
                break
        if not referenced:
            size_kib = 0
            if vdef.get("volume_definitions"):
                size_kib = vdef["volume_definitions"][0].get("size_kib", 0)
            size_disp = format_size(size_kib * 1024)
            img_type = "iso" if "iso" in lin_name.lower() else "disk"
            
            records[lin_name] = {
                "name": lin_name,
                "type": img_type,
                "size": size_disp,
                "scylla": "No",
                "linstor": "Yes",
                "status": "Orphaned"
            }

    headers = ["Image Name", "Type", "Size", "ScyllaDB Registered", "Linstor Resource", "Status"]
    rows = []
    for name, r in sorted(records.items()):
        rows.append([
            r["name"],
            r["type"],
            r["size"],
            r["scylla"],
            r["linstor"],
            r["status"]
        ])

    print_table(headers, rows)

def cmd_image_delete(image_name):
    # 1. Resolve Linstor resource name
    cql = f"SELECT JSON name, path FROM hydra.valhalla_images WHERE name = '{image_name}';"
    rc, stdout, err = run_cql_query(cql)
    res_name = None
    in_db = False
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    path = row.get("path", "")
                    in_db = True
                    if path.startswith("/dev/drbd/by-res/"):
                        parts = path.split("/")
                        if len(parts) >= 5:
                            res_name = parts[4]
                except Exception:
                    pass

    if not res_name:
        if image_name.startswith("img-"):
            res_name = image_name
        else:
            res_name = f"img-{image_name}"

    print(f"Target Linstor resource: '{res_name}'")
    
    # 2. Get all hosts to run drbdadm secondary on them
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

    # Demote on all hosts
    for ip in hosts:
        print(f"Demoting DRBD resource '{res_name}' to secondary on {ip}...")
        rc_sec, stdout_sec, stderr_sec = run_remote_spark(ip, f"drbdadm secondary {res_name}")
        if rc_sec != 0 and "Device or resource busy" in (stderr_sec or stdout_sec):
            print(f"Warning/Error on {ip}: {stderr_sec or stdout_sec}")

    # 3. Delete in Linstor
    controllers_str = "127.0.0.1"
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts_list = cdata.get("hosts", [])
                if hosts_list:
                    controllers_str = ",".join([h["ip"] for h in hosts_list])
    except Exception:
        pass

    print(f"Deleting resource definition '{res_name}' in Linstor...")
    cmd_del = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor resource-definition delete {res_name}"
    rc_del = subprocess.run(cmd_del, shell=True).returncode
    if rc_del != 0:
        print("Warning: Failed to delete Linstor resource definition. It may not exist or is in use.")
    else:
        print("Successfully deleted Linstor resource definition.")

    # 4. Delete from ScyllaDB
    if in_db:
        print(f"Deleting image metadata for '{image_name}' from ScyllaDB...")
        rc_db, stdout_db, err_db = run_cql_query(f"DELETE FROM hydra.valhalla_images WHERE name = '{image_name}';")
        if rc_db == 0:
            print("Successfully deleted image metadata from ScyllaDB.")
        else:
            print(f"Error deleting metadata from ScyllaDB: {err_db or stdout_db}")
    else:
        print("Image was not registered in ScyllaDB. No metadata deletion needed.")

def cmd_disk_list():
    # 1. Query ScyllaDB VMs
    cql = "SELECT JSON name, disk_path, disks_list FROM hydra.vms;"
    rc, stdout, err = run_cql_query(cql)
    disk_to_vm = {}
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    vm_name = row.get("name")
                    disks_list = row.get("disks_list", "")
                    
                    if disks_list and disks_list != "NONE" and disks_list != "None" and disks_list != 'null':
                        disks_payload = disks_list.split(",")
                        for idx, entry in enumerate(disks_payload):
                            disk_res_name = f"{vm_name}-disk{idx}"
                            disk_to_vm[disk_res_name] = vm_name
                except Exception:
                    pass

    # 2. Query Linstor volume definitions
    controllers_str = "127.0.0.1"
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = cdata.get("hosts", [])
                if hosts:
                    controllers_str = ",".join([h["ip"] for h in hosts])
    except Exception:
        pass

    cmd_vdef = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor -m volume-definition list"
    p = subprocess.run(cmd_vdef, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    lin_vdefs = []
    if p.returncode == 0 and p.stdout:
        try:
            raw_vdefs = json.loads(p.stdout.decode("utf-8", errors="ignore"))
            if raw_vdefs and isinstance(raw_vdefs[0], list):
                lin_vdefs = [item for sublist in raw_vdefs for item in sublist]
            else:
                lin_vdefs = raw_vdefs
        except Exception:
            pass

    # We query volume list to get storage pool
    cmd_vols = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor -m volume list"
    p = subprocess.run(cmd_vols, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    lin_vols = []
    if p.returncode == 0 and p.stdout:
        try:
            raw_vols = json.loads(p.stdout.decode("utf-8", errors="ignore"))
            if raw_vols and isinstance(raw_vols[0], list):
                lin_vols = [item for sublist in raw_vols for item in sublist]
            else:
                lin_vols = raw_vols
        except Exception:
            pass

    # Extract storage pool names for each resource
    res_pools = {}
    for vol in lin_vols:
        rname = vol.get("name")
        if not rname:
            continue
        for v in vol.get("volumes", []):
            pool = v.get("storage_pool_name")
            if pool and pool != "DfltDisklessStorPool":
                res_pools[rname] = pool
                break

    # Process Linstor disks (non-image, non-system, non-bench)
    disks = []
    for vd in lin_vdefs:
        rname = vd.get("name")
        if not rname:
            continue
        if rname.startswith("img-") or rname == "linstor-db" or rname.startswith("bench-temp-"):
            continue
            
        size_kib = 0
        if vd.get("volume_definitions"):
            size_kib = vd["volume_definitions"][0].get("size_kib", 0)
        size_disp = format_size(size_kib * 1024)
        
        attached_vm = disk_to_vm.get(rname, "Unattached")
        pool = res_pools.get(rname, "N/A")
        status = "Active" if attached_vm != "Unattached" else "Orphaned"
        
        disks.append({
            "name": rname,
            "size": size_disp,
            "pool": pool,
            "attached": attached_vm,
            "status": status
        })

    headers = ["Disk Name", "Size", "Storage Pool", "Attached To VM", "Status"]
    rows = []
    for d in sorted(disks, key=lambda x: x["name"]):
        rows.append([
            d["name"],
            d["size"],
            d["pool"],
            d["attached"],
            d["status"]
        ])

    print_table(headers, rows)

def cmd_disk_delete(disk_name):
    # 1. Query ScyllaDB VMs to check attachments
    cql = "SELECT JSON name, disks_list FROM hydra.vms;"
    rc, stdout, err = run_cql_query(cql)
    disk_to_vm = {}
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    vm_name = row.get("name")
                    disks_list = row.get("disks_list", "")
                    
                    if disks_list and disks_list != "NONE" and disks_list != "None" and disks_list != 'null':
                        disks_payload = disks_list.split(",")
                        for idx, entry in enumerate(disks_payload):
                            disk_res_name = f"{vm_name}-disk{idx}"
                            disk_to_vm[disk_res_name] = vm_name
                except Exception:
                    pass

    # 2. Check mapping safety
    if disk_name in disk_to_vm:
        attached_vm = disk_to_vm[disk_name]
        print(f"Error: Disk '{disk_name}' is currently attached to VM '{attached_vm}' and cannot be deleted.")
        sys.exit(1)

    print(f"Disk '{disk_name}' is not attached to any VM. Safe to delete.")
    
    # 3. Demote on all hosts
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

    for ip in hosts:
        print(f"Demoting DRBD resource '{disk_name}' to secondary on {ip}...")
        rc_sec, stdout_sec, stderr_sec = run_remote_spark(ip, f"drbdadm secondary {disk_name}")
        if rc_sec != 0 and "Device or resource busy" in (stderr_sec or stdout_sec):
            print(f"Warning/Error on {ip}: {stderr_sec or stdout_sec}")

    # 4. Delete from Linstor
    controllers_str = "127.0.0.1"
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts_list = cdata.get("hosts", [])
                if hosts_list:
                    controllers_str = ",".join([h["ip"] for h in hosts_list])
    except Exception:
        pass

    print(f"Deleting resource definition '{disk_name}' in Linstor...")
    cmd_del = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor resource-definition delete {disk_name}"
    rc_del = subprocess.run(cmd_del, shell=True).returncode
    if rc_del != 0:
        print("Warning: Failed to delete Linstor resource definition. It may not exist or is in use.")
    else:
        print("Successfully deleted Linstor resource definition.")

def run_node_checks(ip, hostname, local_ip, results_dict):
    cmd = "/usr/local/bin/mcli-runner --category all"
    if ip == local_ip or ip == "127.0.0.1":
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rc, stdout, stderr = res.returncode, res.stdout.decode('utf-8', errors='ignore'), res.stderr.decode('utf-8', errors='ignore')
    else:
        rc, stdout, stderr = run_remote_spark(ip, cmd)
    
    results_dict[ip] = {
        "rc": rc,
        "stdout": stdout,
        "stderr": stderr,
        "hostname": hostname
    }

def cmd_health_check():
    local_ip = "127.0.0.1"
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        pass
        
    hosts_info = []
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            hosts_info = cdata.get("hosts", [])
    except Exception:
        pass
        
    if not hosts_info:
        hosts_info = [{"ip": "127.0.0.1", "hostname": "localhost"}]
        
    print(f"Running Mimir diagnostics on {len(hosts_info)} cluster nodes in parallel...")
    
    results_dict = {}
    threads = []
    for h in hosts_info:
        t = threading.Thread(target=run_node_checks, args=(h["ip"], h["hostname"], local_ip, results_dict))
        t.start()
        threads.append(t)
        
    bar_width = 30
    while any(t.is_alive() for t in threads):
        done_count = sum(1 for t in threads if not t.is_alive())
        pct = (done_count / len(threads)) * 100
        filled = int(bar_width * pct / 100)
        bar = "=" * filled + ">" + " " * (bar_width - filled - 1)
        if filled == bar_width:
            bar = "=" * bar_width
        sys.stdout.write(f"\rProgress: [{bar}] {pct:.0f}% ({done_count}/{len(threads)} hosts completed)")
        sys.stdout.flush()
        time.sleep(0.1)
        
    bar = "=" * bar_width
    sys.stdout.write(f"\rProgress: [{bar}] 100% ({len(threads)}/{len(threads)} hosts completed)\n\n")
    sys.stdout.flush()
    
    failed_checks = []
    for h in hosts_info:
        ip = h["ip"]
        res = results_dict.get(ip)
        if not res or res["rc"] != 0:
            err_msg = res["stderr"] if res else "No response"
            failed_checks.append({
                "host": ip,
                "hostname": h["hostname"],
                "check": "Host Connectivity",
                "status": "FAIL",
                "output": f"Failed to execute Mimir checks on node: {err_msg}"
            })
            continue
            
        try:
            node_data = json.loads(res["stdout"])
            for check_name, check_res in node_data.items():
                status = check_res.get("status", "FAIL")
                if status != "PASS":
                    failed_checks.append({
                        "host": ip,
                        "hostname": h["hostname"],
                        "check": check_name,
                        "status": status,
                        "output": check_res.get("output", "")
                    })
        except Exception as ex:
            failed_checks.append({
                "host": ip,
                "hostname": h["hostname"],
                "check": "JSON Parsing",
                "status": "FAIL",
                "output": f"Failed to parse JSON response: {ex}\nRaw stdout: {res['stdout'][:200]}"
            })
            
    if not failed_checks:
        print("PASS: All Mimir checks passed cluster-wide! No issues detected.")
    else:
        print(f"WARN/FAIL: The following Mimir checks failed or reported warnings:\n")
        
        headers = ["Host IP", "Hostname", "Check ID", "Status"]
        rows = []
        for fc in failed_checks:
            rows.append([fc["host"], fc["hostname"], fc["check"], fc["status"]])
            
        print_table(headers, rows)
        print("\n--- Failure Details ---")
        for fc in failed_checks:
            print(f"Host: {fc['host']} ({fc['hostname']}) | Check: {fc['check']} | Status: {fc['status']}")
            indented = "  " + "\n  ".join(fc["output"].splitlines())
            print(indented)
            print("-" * 50)

def run_spectrum_api(path, method="GET", payload=None):
    import ssl
    import urllib.request
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    url = f"https://127.0.0.1:8443{path}"
    data = None
    if payload is not None:
        data = json.dumps(payload).encode('utf-8')
        
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as response:
            return 0, json.loads(response.read().decode('utf-8'))
    except Exception as e:
        return -1, str(e)

def cmd_scheduler_list():
    cql = "SELECT JSON job_name, task_type, interval_seconds, enabled, command FROM hydra.dagur_schedules;"
    rc, stdout, err = run_cql_query(cql)
    if rc != 0:
        print(err)
        sys.exit(1)
    records = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
    headers = ["Job Name", "Task Type", "Interval", "Enabled", "Command"]
    rows = []
    for r in records:
        interval = r.get("interval_seconds", 0)
        interval_str = f"{interval // 3600} Hour(s)" if interval >= 3600 else f"{interval // 60} Minute(s)"
        rows.append([
            r.get("job_name", "N/A"),
            r.get("task_type", "N/A"),
            interval_str,
            "Yes" if r.get("enabled") else "No",
            r.get("command", "N/A")
        ])
    print("=== Dagur Scheduler Policies ===")
    print_table(headers, rows)

def cmd_scheduler_history():
    cql = "SELECT JSON job_name, start_time, end_time, status, exit_code FROM hydra.dagur_runs;"
    rc, stdout, err = run_cql_query(cql)
    if rc != 0:
        print(err)
        sys.exit(1)
    records = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
    headers = ["Job Name", "Start Time", "End Time", "Status", "Exit Code"]
    rows = []
    for r in records:
        rows.append([
            r.get("job_name", "N/A"),
            r.get("start_time", "N/A"),
            r.get("end_time", "N/A") or "Running...",
            r.get("status", "N/A"),
            r.get("exit_code") if r.get("exit_code") != -1 else "N/A"
        ])
    print("=== Dagur Scheduler Execution History ===")
    print_table(headers, rows)

def cmd_scheduler_trigger(name):
    rc, err_or_res = run_spectrum_api("/api/dagur/schedule/trigger", method="POST", payload={"job_name": name})
    if rc == 0:
        print(f"Success: Job '{name}' manual execution triggered.")
    else:
        print(f"Error triggering job: {err_or_res}")

def cmd_host_list():
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/hosts", {}, method="GET")
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error querying host list: {res['error']}")
        sys.exit(1)
    
    hosts = res.get("hosts", [])
    headers = ["Hostname", "IP Address", "Status", "Maintenance Mode"]
    rows = []
    for h in hosts:
        rows.append([
            h.get("hostname", "N/A"),
            h.get("ip", "N/A"),
            h.get("status", "N/A"),
            "Yes" if h.get("maintenance_mode", False) else "No"
        ])
    print_table(headers, rows)

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

def wait_for_catalyst_task(task_id):
    leader_ip = get_zookeeper_leader_ip()
    url = f"http://{leader_ip}:9091/api/v1/tasks/status/{task_id}"
    print(f"Waiting for Catalyst task {task_id} to finish...")
    
    last_progress = -1
    while True:
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=35) as response:
                if response.status == 200:
                    res = json.loads(response.read().decode("utf-8"))
                    status = res.get("status")
                    progress = res.get("progress", 0)
                    error_msg = res.get("error_msg", "")
                    
                    if progress != last_progress:
                        print(f"Task status: {status} | Progress: {progress}%")
                        last_progress = progress
                        
                    if status == "completed":
                        print("Task completed successfully.")
                        return True
                    elif status == "failed":
                        print(f"Task failed: {error_msg}")
                        sys.exit(1)
                elif response.status == 204:
                    # Long polling timeout, update leader IP and keep waiting
                    leader_ip = get_zookeeper_leader_ip()
                    url = f"http://{leader_ip}:9091/api/v1/tasks/status/{task_id}"
                    continue
                else:
                    print(f"Unexpected response status from Catalyst: {response.status}")
                    time.sleep(2)
        except Exception as e:
            # Check if this host has entered maintenance mode locally
            if os.path.exists("/etc/hci/maintenance.state"):
                print("Host has successfully entered maintenance mode. Catalyst is offline. Exiting wait loop.")
                return True
            # Maybe leader is switching/rebooting, try to find new leader IP
            time.sleep(2)
            leader_ip = get_zookeeper_leader_ip()
            url = f"http://{leader_ip}:9091/api/v1/tasks/status/{task_id}"

def cmd_host_maintenance_enter(hostname, force_stop=False):
    if hostname == "--all":
        rc_hosts, res_hosts, err_hosts = run_mtls_api("127.0.0.1", "/api/v1/hosts", {}, method="GET")
        if rc_hosts != 0 or "error" in res_hosts:
            hosts = []
            try:
                with open("/etc/hci/cluster.json", "r") as f:
                    cdata = json.load(f)
                    hosts = cdata.get("hosts", [])
            except Exception:
                print(f"Failed to get host list for --all: {err_hosts}")
                sys.exit(1)
        else:
            hosts = res_hosts.get("hosts", [])
        
        hostnames = [h.get("hostname") for h in hosts if h.get("hostname")]
        if not hostnames:
            print("No hosts found.")
            sys.exit(1)
            
        print(f"Requesting all hosts to enter maintenance mode sequentially: {', '.join(hostnames)}...")
        for hn in hostnames:
            print(f"\n--- Processing host '{hn}' ---")
            payload = {"hostname": hn, "action": "enter", "force_stop": force_stop}
            rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/host/maintenance", payload, method="POST")
            if rc != 0:
                print(f"Failed to communicate with spark-daemon for {hn}: {err}")
            elif "error" in res:
                print(f"Error for {hn}: {res['error']}")
            else:
                task_id = res.get("task_id")
                if task_id:
                    wait_for_catalyst_task(task_id)
                else:
                    print(f"Success for {hn}: {res.get('message', 'Maintenance mode transition initiated.')}")
        return

    print(f"Requesting host '{hostname}' to enter maintenance mode (force_stop={force_stop})...")
    payload = {"hostname": hostname, "action": "enter", "force_stop": force_stop}
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/host/maintenance", payload, method="POST")
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error: {res['error']}")
        sys.exit(1)
    
    # Wait for task completion
    task_id = res.get("task_id")
    if task_id:
        wait_for_catalyst_task(task_id)
    else:
        print(f"Success: {res.get('message', 'Maintenance mode transition initiated.')}")

def cmd_host_maintenance_leave(hostname):
    if hostname == "--all":
        rc_hosts, res_hosts, err_hosts = run_mtls_api("127.0.0.1", "/api/v1/hosts", {}, method="GET")
        if rc_hosts != 0 or "error" in res_hosts:
            hosts = []
            try:
                with open("/etc/hci/cluster.json", "r") as f:
                    cdata = json.load(f)
                    hosts = cdata.get("hosts", [])
            except Exception:
                print(f"Failed to get host list for --all: {err_hosts}")
                sys.exit(1)
        else:
            hosts = res_hosts.get("hosts", [])
            
        hostnames = [h.get("hostname") for h in hosts if h.get("hostname")]
        if not hostnames:
            print("No hosts found.")
            sys.exit(1)
            
        print(f"Requesting all hosts to leave maintenance mode sequentially: {', '.join(hostnames)}...")
        for hn in hostnames:
            print(f"\n--- Processing host '{hn}' ---")
            payload = {"hostname": hn, "action": "leave"}
            rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/host/maintenance", payload, method="POST")
            if rc != 0:
                print(f"Failed to communicate with spark-daemon for {hn}: {err}")
            elif "error" in res:
                print(f"Error for {hn}: {res['error']}")
            else:
                task_id = res.get("task_id")
                if task_id:
                    wait_for_catalyst_task(task_id)
                else:
                    print(f"Success for {hn}: {res.get('message', 'Host returned to normal status.')}")
        return

    print(f"Requesting host '{hostname}' to leave maintenance mode...")
    payload = {"hostname": hostname, "action": "leave"}
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/host/maintenance", payload, method="POST")
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error: {res['error']}")
        sys.exit(1)
    
    # Wait for task completion
    task_id = res.get("task_id")
    if task_id:
        wait_for_catalyst_task(task_id)
    else:
        if res.get("status") == "transitioning" and "Vali offline" in res.get("message", ""):
            print("Vali was offline. Local services bootstrapped. Waiting for Vali to come online...")
            import socket, time
            vali_online = False
            for _ in range(30):
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(0.5)
                    s.connect(("127.0.0.1", 9095))
                    s.close()
                    vali_online = True
                    break
                except:
                    time.sleep(1)
            
            if vali_online:
                print("Vali is online. Finalizing leave maintenance sequence...")
                # Retry submitting the leave request up to 6 times (with 5 seconds sleep in between) if it fails
                for attempt in range(6):
                    rc_final, res_final, err_final = run_mtls_api("127.0.0.1", "/api/v1/host/maintenance", payload, method="POST")
                    if rc_final == 0 and "error" not in res_final:
                        final_task_id = res_final.get("task_id")
                        if final_task_id:
                            wait_for_catalyst_task(final_task_id)
                            return
                    if attempt < 5:
                        print(f"Database or Catalyst not fully initialized yet (attempt {attempt+1}/6). Retrying in 5 seconds...")
                        time.sleep(5)
                print("Success: Local services started, but database state finalization timed out. Please run the command again if status is not NORMAL.")
            else:
                print("Timeout waiting for Vali to initialize. Please check service status or run the command again.")
        else:
            print(f"Success: {res.get('message', 'Host returned to normal status.')}")

def cmd_cluster_vip_set(vip_ip):
    import base64
    if not os.path.exists("/etc/hci/cluster.json"):
        print("Error: /etc/hci/cluster.json not found on this host.")
        sys.exit(1)
        
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
    except Exception as e:
        print(f"Error reading cluster.json: {e}")
        sys.exit(1)
        
    cdata["vip"] = vip_ip
    
    try:
        with open("/etc/hci/cluster.json", "w") as f:
            json.dump(cdata, f, indent=4)
    except Exception as e:
        print(f"Error writing local cluster.json: {e}")
        sys.exit(1)
        
    hosts = [h["ip"] for h in cdata.get("hosts", [])]
    json_str = json.dumps(cdata, indent=4)
    json_b64 = base64.b64encode(json_str.encode()).decode()
    
    cmd_write = f"mkdir -p /etc/hci && echo {json_b64} | base64 -d > /etc/hci/cluster.json && systemctl restart bifrost"
    
    for ip in hosts:
        print(f"Propagating VIP configuration to host {ip}...")
        rc, stdout, stderr = run_remote_spark(ip, cmd_write)
        if rc != 0:
            print(f"Warning: Failed to configure VIP on host {ip}: {stderr or stdout}")
            
    print(f"Successfully configured cluster Virtual IP (VIP) to {vip_ip} cluster-wide.")

def cmd_system_cleanup():
    cutoff_days = 3
    cutoff_sec = int(time.time() - cutoff_days * 86400)
    
    print(f"Starting execution history cleanup (older than {cutoff_days} days)...")
    
    import datetime
    def parse_db_timestamp(ts_val):
        if ts_val is None:
            return time.time()
        if isinstance(ts_val, (int, float)):
            if ts_val > 5000000000:
                return ts_val / 1000.0
            return float(ts_val)
        if isinstance(ts_val, str):
            if ts_val.isdigit():
                val = int(ts_val)
                if val > 5000000000:
                    return val / 1000.0
                return float(val)
            for fmt in [
                "%Y-%m-%d %H:%M:%S.%f%z",
                "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S.%f+0000",
                "%Y-%m-%d %H:%M:%S+0000",
            ]:
                try:
                    clean_ts = ts_val
                    if clean_ts.endswith("Z"):
                        clean_ts = clean_ts[:-1] + "+0000"
                    dt = datetime.datetime.strptime(clean_ts, fmt)
                    return dt.timestamp()
                except:
                    pass
        return time.time()

    # 1. Clean dagur_runs
    rc, stdout, _ = run_cql_query("SELECT JSON job_name, start_time FROM hydra.dagur_runs;")
    if rc == 0 and stdout:
        cnt = 0
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    job_name = row.get("job_name")
                    st_str = row.get("start_time")
                    if job_name and st_str:
                        st_epoch = parse_db_timestamp(st_str)
                        if st_epoch < cutoff_sec:
                            run_cql_query(f"DELETE FROM hydra.dagur_runs WHERE job_name = '{job_name}' AND start_time = '{st_str}';")
                            cnt += 1
                except:
                    pass
        print(f"Cleaned {cnt} old Dagur job execution records.")

    # 2. Clean mimir_results
    rc, stdout, _ = run_cql_query("SELECT JSON category, check_name, node_ip, timestamp FROM hydra.mimir_results;")
    if rc == 0 and stdout:
        cnt = 0
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    cat = row.get("category")
                    cname = row.get("check_name")
                    nip = row.get("node_ip")
                    ts_str = row.get("timestamp")
                    if cat and cname and nip and ts_str:
                        ts_epoch = parse_db_timestamp(ts_str)
                        if ts_epoch < cutoff_sec:
                            run_cql_query(f"DELETE FROM hydra.mimir_results WHERE category = '{cat}' AND check_name = '{cname}' AND node_ip = '{nip}';")
                            cnt += 1
                except:
                    pass
        print(f"Cleaned {cnt} old Mimir diagnostic results.")

    # 3. Clean vali_tasks
    rc, stdout, _ = run_cql_query("SELECT JSON task_id, created_at FROM hydra.vali_tasks;")
    if rc == 0 and stdout:
        cnt = 0
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    tid = row.get("task_id")
                    cat_ms = row.get("created_at")
                    if tid and cat_ms:
                        cat_epoch = parse_db_timestamp(cat_ms)
                        if cat_epoch < cutoff_sec:
                            run_cql_query(f"DELETE FROM hydra.vali_tasks WHERE task_id = {tid};")
                            cnt += 1
                except:
                    pass
        print(f"Cleaned {cnt} old Vali placement tasks.")

    # 4. Clean vali_drs_history
    rc, stdout, _ = run_cql_query("SELECT JSON event_time, vm_name FROM hydra.vali_drs_history;")
    if rc == 0 and stdout:
        cnt = 0
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    ev_time = row.get("event_time")
                    vname = row.get("vm_name")
                    if ev_time and vname:
                        ev_epoch = parse_db_timestamp(ev_time)
                        if ev_epoch < cutoff_sec:
                            run_cql_query(f"DELETE FROM hydra.vali_drs_history WHERE event_time = '{ev_time}' AND vm_name = '{vname}';")
                            cnt += 1
                except:
                    pass
        print(f"Cleaned {cnt} old Vali DRS migration history records.")

def cmd_vm_create():
    if len(sys.argv) < 5:
        print("Error: Name, vCPUs, and Memory are required.")
        print("Usage: valcli vm.create <vm_name> <vcpus> <memory_mb> [options]")
        print("Options:")
        print("  --firmware <uefi|bios>    (default: uefi)")
        print("  --iso <iso_file>          (default: none)")
        print("  --boot-device <hd|cdrom>  (default: hd)")
        print("  --network-id <uuid>       (default: default)")
        print("  --disks <disks_comma>     (default: 10G)")
        print("  --cpu-model <model>       (default: host-passthrough)")
        sys.exit(1)
        
    name = sys.argv[2]
    try:
        vcpus = int(sys.argv[3])
        memory = int(sys.argv[4])
    except ValueError:
        print("Error: vCPUs and Memory must be integers.")
        sys.exit(1)
        
    firmware = "uefi"
    iso = ""
    boot_device = "hd"
    network_id = ""
    disks = ["10G"]
    cpu_model = "host-passthrough"
    
    idx = 5
    while idx < len(sys.argv):
        arg = sys.argv[idx]
        if arg == "--firmware" and idx + 1 < len(sys.argv):
            firmware = sys.argv[idx+1]
            idx += 2
        elif arg == "--iso" and idx + 1 < len(sys.argv):
            iso = sys.argv[idx+1]
            idx += 2
        elif arg == "--boot-device" and idx + 1 < len(sys.argv):
            boot_device = sys.argv[idx+1]
            idx += 2
        elif arg == "--network-id" and idx + 1 < len(sys.argv):
            network_id = sys.argv[idx+1]
            idx += 2
        elif arg == "--disks" and idx + 1 < len(sys.argv):
            disks = sys.argv[idx+1].split(",")
            idx += 2
        elif arg == "--cpu-model" and idx + 1 < len(sys.argv):
            cpu_model = sys.argv[idx+1]
            idx += 2
        else:
            print(f"Error: Unknown or malformed option '{arg}'")
            sys.exit(1)
            
    payload = {
        "name": name,
        "vcpus": vcpus,
        "memory": memory,
        "firmware": firmware,
        "iso": iso,
        "boot_device": boot_device,
        "network_id": network_id,
        "disks": disks,
        "cpu_model": cpu_model
    }
    
    print(f"Creating VM '{name}' ({vcpus} vCPUs, {memory}MB RAM)...")
    rc, data = run_spectrum_api("/api/vms/create", method="POST", payload=payload)
    if rc == 0:
        print(f"Success: {data.get('message', 'VM creation task scheduled.')}")
    else:
        print(f"Error creating VM: {data}")
        sys.exit(1)

def cmd_vm_delete():
    if len(sys.argv) < 3:
        print("Error: VM Name is required.")
        print("Usage: valcli vm.delete <vm_name>")
        sys.exit(1)
        
    name = sys.argv[2]
    print(f"Deleting VM '{name}'...")
    rc, data = run_spectrum_api("/api/vms/delete", method="POST", payload={"name": name})
    if rc == 0:
        print(f"Success: {data.get('message', 'VM deletion task scheduled.')}")
    else:
        print(f"Error deleting VM: {data}")
        sys.exit(1)

def cmd_vm_edit():
    if len(sys.argv) < 3:
        print("Error: VM Name is required.")
        print("Usage: valcli vm.edit <vm_name> [options]")
        print("Options:")
        print("  --vcpus <count>")
        print("  --memory <memory_mb>")
        print("  --firmware <uefi|bios>")
        print("  --iso <iso_file>")
        print("  --boot-device <hd|cdrom>")
        print("  --network-id <uuid>")
        print("  --disks <disks_comma>")
        print("  --cpu-model <model>")
        sys.exit(1)
        
    name = sys.argv[2]
    payload = {"name": name}
    
    idx = 3
    while idx < len(sys.argv):
        arg = sys.argv[idx]
        if arg == "--vcpus" and idx + 1 < len(sys.argv):
            payload["vcpus"] = int(sys.argv[idx+1])
            idx += 2
        elif arg == "--memory" and idx + 1 < len(sys.argv):
            payload["memory"] = int(sys.argv[idx+1])
            idx += 2
        elif arg == "--firmware" and idx + 1 < len(sys.argv):
            payload["firmware"] = sys.argv[idx+1]
            idx += 2
        elif arg == "--iso" and idx + 1 < len(sys.argv):
            payload["iso"] = sys.argv[idx+1]
            idx += 2
        elif arg == "--boot-device" and idx + 1 < len(sys.argv):
            payload["boot_device"] = sys.argv[idx+1]
            idx += 2
        elif arg == "--network-id" and idx + 1 < len(sys.argv):
            payload["network_id"] = sys.argv[idx+1]
            idx += 2
        elif arg == "--disks" and idx + 1 < len(sys.argv):
            payload["disks"] = sys.argv[idx+1].split(",")
            idx += 2
        elif arg == "--cpu-model" and idx + 1 < len(sys.argv):
            payload["cpu_model"] = sys.argv[idx+1]
            idx += 2
        else:
            print(f"Error: Unknown or malformed option '{arg}'")
            sys.exit(1)
            
    if len(payload) == 1:
        print("Error: No configuration modifications specified.")
        sys.exit(1)
        
    print(f"Updating VM '{name}' configuration...")
    rc, data = run_spectrum_api("/api/vms/update", method="POST", payload=payload)
    if rc == 0:
        print(f"Success: {data.get('message', 'VM update task scheduled.')}")
    else:
        print(f"Error updating VM: {data}")
        sys.exit(1)

def print_usage():
    print("Valkyrie CLI (valcli) v1.2.0 - Helios HCI command-line manager\n")
    print("Usage:")
    print("  valcli vm.list                     List all virtual machines in the cluster")
    print("  valcli vm.create <name> <vc> <mem> Create a new VM configuration and disks")
    print("  valcli vm.delete <name>            Delete VM configuration and its disks")
    print("  valcli vm.edit <name> [options]    Modify VM CPU, memory, disks, network, or ISO")
    print("  valcli vm.on <vm_name>             Power ON a virtual machine")
    print("  valcli vm.off <vm_name>            Power OFF (destroy) a virtual machine")
    print("  valcli vm.migrate <name> <host>    Migrate a running VM to another cluster node")
    print("  valcli vm.balance                  Manually trigger aggressive cluster DRS load balancing")
    print("  valcli drs.status                  Print cluster balance score and recent DRS migrations")
    print("  valcli host.list                   List all hosts and their maintenance state")
    print("  valcli host.maintenance.enter <h>  Put host (or '--all') into maintenance mode and evacuate VMs")
    print("      Options:")
    print("        --force-stop                 Forcefully stop/suspend VMs that fail migration")
    print("  valcli host.maintenance.leave <h>  Take host (or '--all') out of maintenance mode")
    print("  valcli cluster.vip.set <vip>       Configure cluster-wide Virtual IP (VIP)")
    print("  valcli storage.list                List all Aether storage containers")
    print("  valcli storage.benchmark <name>    Run safe read/write performance benchmark")
    print("  valcli storage.cleanup_orphaned    Delete orphaned virtual disk and NVRAM files")
    print("  valcli image.list                  List all ScyllaDB registered and Linstor images")
    print("  valcli image.delete <name>         Demote and delete image from storage and database")
    print("  valcli disk.list                   List all active and orphaned virtual disks")
    print("  valcli disk.delete <name>          Delete virtual disk (fails if disk is attached to a VM)")
    print("  valcli health.check                Run parallel Mimir diagnostics with progress bar")
    print("  valcli scheduler.list              List all Dagur scheduled policies")
    print("  valcli scheduler.history           List past executions of Dagur jobs")
    print("  valcli scheduler.trigger <name>    Manually trigger execution of a Dagur job")
    print("  valcli system.cleanup              Prune execution history tables older than 3 days")
    print("  valcli db.print <table_name>       Print ScyllaDB table contents as ASCII table")
    print("      Options:")
    print("        --columns c1,c2              Specify a comma-separated list of columns to print")
    print("  valcli db.query \"<query>\"          Execute raw CQL query and display formatted output")
    print("\nAvailable tables: vms, storage_containers, dagur_schedules, dagur_runs")

def main():
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)
        
    cmd = sys.argv[1]
    if cmd in ["--version", "-v", "-version", "version"]:
        print("Valkyrie CLI (valcli) v1.2.0")
        sys.exit(0)
        
    if cmd == "vm.list":
        cmd_vm_list()
    elif cmd == "vm.create":
        cmd_vm_create()
    elif cmd == "vm.delete":
        cmd_vm_delete()
    elif cmd == "vm.edit":
        cmd_vm_edit()
    elif cmd == "vm.on":
        if len(sys.argv) < 3:
            print("Error: VM Name is required.")
            print("Usage: valcli vm.on <vm_name>")
            sys.exit(1)
        cmd_vm_on(sys.argv[2])
    elif cmd == "vm.off":
        if len(sys.argv) < 3:
            print("Error: VM Name is required.")
            print("Usage: valcli vm.off <vm_name>")
            sys.exit(1)
        cmd_vm_off(sys.argv[2])
    elif cmd == "vm.migrate":
        if len(sys.argv) < 4:
            print("Error: VM Name and Target Host are required.")
            print("Usage: valcli vm.migrate <vm_name> <target_host>")
            sys.exit(1)
        cmd_vm_migrate(sys.argv[2], sys.argv[3])
    elif cmd == "vm.balance":
        cmd_vm_balance()
    elif cmd == "drs.status":
        cmd_drs_status()
    elif cmd == "host.list":
        cmd_host_list()
    elif cmd == "host.maintenance.enter":
        args = sys.argv[2:]
        if not args or (len(args) == 1 and args[0] == "--force-stop"):
            print("Error: Hostname is required.")
            print("Usage: valcli host.maintenance.enter <hostname> [--force-stop]")
            sys.exit(1)
        
        force_stop = "--force-stop" in args
        hostname = None
        for arg in args:
            if arg != "--force-stop":
                hostname = arg
                break
                
        if not hostname:
            print("Error: Hostname is required.")
            sys.exit(1)
            
        cmd_host_maintenance_enter(hostname, force_stop)
    elif cmd == "host.maintenance.leave":
        if len(sys.argv) < 3:
            print("Error: Hostname is required.")
            print("Usage: valcli host.maintenance.leave <hostname>")
            sys.exit(1)
        cmd_host_maintenance_leave(sys.argv[2])
    elif cmd == "cluster.vip.set":
        if len(sys.argv) < 3:
            print("Error: VIP IP address is required.")
            print("Usage: valcli cluster.vip.set <vip_ip>")
            sys.exit(1)
        cmd_cluster_vip_set(sys.argv[2])
    elif cmd == "storage.list":
        cmd_storage_list()
    elif cmd == "storage.benchmark":
        if len(sys.argv) < 3:
            print("Error: Storage container name is required.")
            print("Usage: valcli storage.benchmark <container_name>")
            sys.exit(1)
        cmd_storage_benchmark(sys.argv[2])
    elif cmd == "storage.cleanup_orphaned":
        cmd_storage_cleanup_orphaned()
    elif cmd == "image.list":
        cmd_image_list()
    elif cmd == "image.delete":
        if len(sys.argv) < 3:
            print("Error: Image name is required.")
            print("Usage: valcli image.delete <image_name>")
            sys.exit(1)
        cmd_image_delete(sys.argv[2])
    elif cmd == "disk.list":
        cmd_disk_list()
    elif cmd == "disk.delete":
        if len(sys.argv) < 3:
            print("Error: Disk name is required.")
            print("Usage: valcli disk.delete <disk_name>")
            sys.exit(1)
        cmd_disk_delete(sys.argv[2])
    elif cmd == "health.check":
        cmd_health_check()
    elif cmd == "db.print":
        cmd_db_print()
    elif cmd == "db.query":
        cmd_db_query()
    elif cmd == "scheduler.list":
        cmd_scheduler_list()
    elif cmd == "scheduler.history":
        cmd_scheduler_history()
    elif cmd == "system.cleanup":
        cmd_system_cleanup()
    elif cmd == "scheduler.trigger":
        if len(sys.argv) < 3:
            print("Error: Job name is required.")
            print("Usage: valcli scheduler.trigger <job_name>")
            sys.exit(1)
        cmd_scheduler_trigger(sys.argv[2])
    else:
        print(f"Error: Unknown command '{cmd}'")
        print_usage()
        sys.exit(1)

if __name__ == "__main__":
    main()
