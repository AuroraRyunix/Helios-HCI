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

def run_cql_query(cql_query):
    """Executes a query inside systemd-hydra-db, trying all cluster nodes sequentially."""
    ips = ["127.0.0.1"]
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            ips = [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        pass
        
    local_active = False
    res_ps = subprocess.run(["podman", "ps", "--filter", "name=systemd-hydra-db", "--format", "{{.Status}}"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res_ps.returncode == 0 and b"Up" in res_ps.stdout:
        local_active = True
        
    if local_active:
        for ip in ips:
            cmd = ["podman", "exec", "-i", "systemd-hydra-db", "cqlsh", ip, "-e", cql_query]
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if res.returncode == 0:
                stdout_str = res.stdout.decode('utf-8', errors='ignore').strip()
                if stdout_str:
                    stdout_str = stdout_str.replace('\\\\', '\\')
                return 0, stdout_str, ""
                
    # Local container is offline or connection failed, try remote execution via spark-daemon on other nodes
    local_ip = "127.0.0.1"
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        pass

    import base64
    b64_query = base64.b64encode(cql_query.encode('utf-8')).decode('utf-8')
    
    for ip in ips:
        if ip != local_ip and ip != "127.0.0.1":
            remote_cmd = f"echo {b64_query} | base64 -d | podman exec -i systemd-hydra-db cqlsh {ip}"
            rc, stdout, stderr = run_remote_spark(ip, remote_cmd)
            if rc == 0:
                if stdout:
                    stdout = stdout.replace('\\\\', '\\')
                return 0, stdout, ""
                
    return -1, "", "Error: Failed to connect to ScyllaDB on any node in the cluster."

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
    rows = []
    for r in records:
        quota_bytes = r.get("quota_bytes", 0)
        quota_str = f"{quota_bytes // (1024**3)} GB" if quota_bytes > 0 else "Unlimited"
        rows.append([
            r.get("name", "N/A"),
            r.get("tier", "SSD"),
            quota_str,
            r.get("path", "N/A"),
            r.get("ftt", 1)
        ])
    print("=== Aether Storage Containers ===")
    print_table(headers, rows)
    print()
    
    print("=== Aether Volume Status ===")
    res_list = subprocess.run("podman exec systemd-aether gluster volume list", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res_list.returncode != 0:
        print("Warning: Could not query Aether volume list (systemd-aether not active or storage daemon offline).")
        return
        
    vols = res_list.stdout.decode("utf-8", errors="ignore").splitlines()
    if not vols:
        print("No Aether volumes found.")
        return
        
    for vol in vols:
        print(f"Volume: {vol}")
        res_status = subprocess.run(f"podman exec systemd-aether gluster volume status {vol} --xml", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if res_status.returncode != 0:
            print(f"  Error: Could not retrieve status for volume '{vol}'.")
            continue
            
        xml_data = res_status.stdout.decode('utf-8', errors='ignore')
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(xml_data)
            vol_headers = ["Type", "Brick / Daemon", "Port", "Online", "PID"]
            vol_rows = []
            for node in root.findall(".//node"):
                hostname = node.findtext("hostname")
                path = node.findtext("path")
                status = node.findtext("status")
                port = node.findtext("port")
                pid = node.findtext("pid")
                
                online = "Y" if status == "1" else "N"
                if hostname == "Self-heal Daemon":
                    dtype = "Self-heal"
                    target = path
                else:
                    dtype = "Brick"
                    target = f"{hostname}:{path}"
                vol_rows.append([dtype, target, port, online, pid])
            print_table(vol_headers, vol_rows)
        except Exception as ex:
            print(f"  Error parsing XML status for volume '{vol}': {ex}")
        print()

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
    cql = f"SELECT JSON path FROM hydra.storage_containers WHERE name = '{container_name}';"
    rc, stdout, err = run_cql_query(cql)
    if rc != 0:
        print(err)
        sys.exit(1)
        
    path = None
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                path = json.loads(line).get("path")
            except Exception:
                pass
                
    if not path:
        print(f"Error: Storage container '{container_name}' not found.")
        sys.exit(1)
        
    if not os.path.exists(path):
        print(f"Error: Path '{path}' is not mounted or accessible on this host.")
        sys.exit(1)
        
    bench_file = os.path.join(path, ".valcli_bench_temp")
    file_size_mb = 50
    chunk_size = 1024 * 1024 # 1 MB
    data = b"0" * chunk_size
    
    print(f"Benchmarking storage container '{container_name}' at {path}...")
    
    try:
        # Write test
        print(f"[1/3] Writing {file_size_mb} MB test file...")
        start_write = time.time()
        with open(bench_file, "wb") as f:
            for _ in range(file_size_mb):
                f.write(data)
            f.flush()
            os.fsync(f.fileno())
        end_write = time.time()
        write_time = end_write - start_write
        write_speed = file_size_mb / write_time if write_time > 0 else 0
        print(f"      Write Speed: {write_speed:.2f} MB/s (took {write_time:.2f}s)")
        
        # Read test
        print(f"[2/3] Reading {file_size_mb} MB test file...")
        start_read = time.time()
        with open(bench_file, "rb") as f:
            while f.read(chunk_size):
                pass
        end_read = time.time()
        read_time = end_read - start_read
        read_speed = file_size_mb / read_time if read_time > 0 else 0
        print(f"      Read Speed:  {read_speed:.2f} MB/s (took {read_time:.2f}s)")
        
    except Exception as ex:
        print(f"Error during benchmark: {ex}")
    finally:
        print("[3/3] Cleaning up test file...")
        if os.path.exists(bench_file):
            try:
                os.remove(bench_file)
            except Exception as e:
                print(f"Warning: Failed to delete test file {bench_file}: {e}")
                
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
    # Try local first
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        s.connect(("127.0.0.1", 2181))
        s.sendall(b"stat")
        resp = s.recv(1024).decode('utf-8', errors='ignore')
        s.close()
        if "mode: leader" in resp.lower():
            return "127.0.0.1"
    except Exception:
        pass
        
    # Read cluster hosts from cluster.json
    ips = []
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            ips = [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        pass
        
    for ip in ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect((ip, 2181))
            s.sendall(b"stat")
            resp = s.recv(1024).decode('utf-8', errors='ignore')
            s.close()
            if "mode: leader" in resp.lower():
                return ip
        except Exception:
            pass
            
    return "127.0.0.1"

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
                    # Long polling timeout, keep waiting
                    continue
                else:
                    print(f"Unexpected response status from Catalyst: {response.status}")
                    time.sleep(2)
        except Exception as e:
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

def print_usage():
    print("Valkyrie CLI (valcli) v1.2.0 - Helios HCI command-line manager\n")
    print("Usage:")
    print("  valcli vm.list                     List all virtual machines in the cluster")
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
