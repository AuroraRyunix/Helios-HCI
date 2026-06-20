#!/usr/bin/env python3
import sys
import os
import json
import time
import socket
import urllib.request
import ssl
import subprocess
import base64
import uuid

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

def run_remote_spark(ip, command):
    # Try daemon certificates first, fallback to client certificates
    cert_paths = [
        ("/etc/hci/spark/certs/ca.crt", "/etc/hci/spark/certs/node.crt", "/etc/hci/spark/certs/node.key"),
        ("/root/.certs/ca.crt", "/root/.certs/client.crt", "/root/.certs/client.key")
    ]
    
    context = None
    for ca, cert, key in cert_paths:
        if os.path.exists(ca) and os.path.exists(cert) and os.path.exists(key):
            try:
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
                context.load_cert_chain(certfile=cert, keyfile=key)
                context.check_hostname = False
                break
            except Exception:
                pass
                
    if not context:
        context = ssl._create_unverified_context()
        
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
    cert_paths = [
        ("/etc/hci/spark/certs/ca.crt", "/etc/hci/spark/certs/node.crt", "/etc/hci/spark/certs/node.key"),
        ("/root/.certs/ca.crt", "/root/.certs/client.crt", "/root/.certs/client.key")
    ]
    
    context = None
    for ca, cert, key in cert_paths:
        if os.path.exists(ca) and os.path.exists(cert) and os.path.exists(key):
            try:
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
                context.load_cert_chain(certfile=cert, keyfile=key)
                context.check_hostname = False
                break
            except Exception:
                pass
                
    if not context:
        context = ssl._create_unverified_context()
        
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
        return "mode: leader" in resp.lower()
    except Exception:
        return False

def get_zookeeper_leader_ip(hosts=None):
    if not hosts:
        hosts = get_cluster_hosts()
    if not hosts:
        ips = ["10.10.102.220", "10.10.102.222", "10.10.102.223"]
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
            if "mode: leader" in resp.lower():
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
    url = f"http://{ip}:9095/api/v1/hosts"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status == 200
    except Exception:
        return False

def get_gluster_pending_entries(vol_name):
    cmd = f"podman exec systemd-aether gluster volume heal {vol_name} info"
    rc, stdout, stderr = run_remote_spark("127.0.0.1", cmd)
    if rc != 0:
        if "not of type replicate/disperse" in (stdout or "") or "not of type replicate/disperse" in (stderr or ""):
            return 0
        return -1
    import re
    total_entries = 0
    matches = re.findall(r"Number of entries:\s*(\d+)", stdout)
    if not matches:
        return -1
    for m in matches:
        total_entries += int(m)
    return total_entries

def submit_catalyst_task(leader_ip, service, action, payload):
    url = f"http://{leader_ip}:9091/api/v1/tasks/submit"
    data = json.dumps({
        "service": service,
        "action": action,
        "payload": payload
    }).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status == 200:
                res_data = json.loads(response.read().decode("utf-8"))
                return res_data.get("task_id")
    except Exception:
        pass
    return None

def main():
    print("Mipha High-Availability Host Monitor and VM Failover Coordinator started.")
    
    # Track consecutive failures per host IP
    consecutive_failures = {}
    
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
                
                # Host is down only if both checks fail
                if not ping_ok and not spark_ok:
                    consecutive_failures[ip] = consecutive_failures.get(ip, 0) + 1
                    print(f"[Mipha HA] Host {hostname} ({ip}) health check failed (Count: {consecutive_failures[ip]}/3)")
                else:
                    consecutive_failures[ip] = 0
                    
                    # If host was previously marked DOWN, initiate rejoin/sync sequence
                    if db_status == "DOWN":
                        print(f"[Mipha HA] Host {hostname} ({ip}) is back online! Starting rejoining and GlusterFS sync sequence...")
                        
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
                        start_cmd = "systemctl start zookeeper hydra-db aether spectrum bifrost dagur mimir vali catalyst gatoway logos mipha"
                        run_remote_spark(ip, start_cmd)
                        
                        # Sleep 10 seconds to allow services (especially Aether/Gluster) to boot
                        time.sleep(10)
                        
                        # Update parent task progress to 20%
                        cql_up = f"UPDATE hydra.catalyst_tasks SET progress = 20, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                        run_cql_query(cql_up)
                        
                        # C. Trigger GlusterFS self-heal
                        print(f"[Mipha HA] Triggering GlusterFS self-heal for default volumes...")
                        run_remote_spark("127.0.0.1", "podman exec systemd-aether gluster volume heal default-vm-container")
                        run_remote_spark("127.0.0.1", "podman exec systemd-aether gluster volume heal default-image-container")
                        
                        # D. Create child Catalyst task for GlusterFS sync
                        child_task_id = str(uuid.uuid4())
                        child_payload = json.dumps({"hostname": hostname, "parent_task_id": parent_task_id})
                        cql_child = f"""
                        INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
                        VALUES ({child_task_id}, 'aether', 'sync', 'processing', '{child_payload.replace("'", "''")}', 10, {now_ms}, {now_ms});
                        """
                        run_cql_query(cql_child)
                        
                        # E. Poll GlusterFS self-heal status
                        synced = False
                        # Poll up to 60 iterations (3 minutes)
                        for iteration in range(60):
                            child_progress = min(95, 10 + iteration * 5)
                            parent_progress = int(20 + (child_progress / 100.0) * 70)
                            
                            cql_up_child = f"UPDATE hydra.catalyst_tasks SET progress = {child_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {child_task_id};"
                            run_cql_query(cql_up_child)
                            
                            cql_up_parent = f"UPDATE hydra.catalyst_tasks SET progress = {parent_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_up_parent)
                            
                            # Check pending entries
                            vm_entries = get_gluster_pending_entries("default-vm-container")
                            img_entries = get_gluster_pending_entries("default-image-container")
                            
                            print(f"[Mipha HA] GlusterFS sync status - VM entries: {vm_entries}, Image entries: {img_entries}")
                            
                            if vm_entries == 0 and img_entries == 0:
                                synced = True
                                print(f"[Mipha HA] GlusterFS volumes fully synced on host {hostname}!")
                                break
                                
                            time.sleep(3)
                            
                        # F. Conclude task and update node status
                        now_ms_end = int(time.time() * 1000)
                        if synced:
                            # Set child & parent task to completed
                            cql_child_end = f"UPDATE hydra.catalyst_tasks SET status = 'completed', progress = 100, updated_at = {now_ms_end} WHERE task_id = {child_task_id};"
                            run_cql_query(cql_child_end)
                            
                            cql_parent_end = f"UPDATE hydra.catalyst_tasks SET status = 'completed', progress = 100, updated_at = {now_ms_end} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_parent_end)
                            
                            # Set node status to NORMAL
                            cql_normal = f"UPDATE hydra.nodes SET status = 'NORMAL' WHERE hostname = '{hostname}';"
                            run_cql_query(cql_normal)
                            print(f"[Mipha HA] Host {hostname} rejoin and sync completed successfully.")
                        else:
                            # Failed/timed out
                            err_msg = "GlusterFS volume sync timed out or failed to complete self-heal."
                            cql_child_end = f"UPDATE hydra.catalyst_tasks SET status = 'failed', progress = 100, error_msg = '{err_msg}', updated_at = {now_ms_end} WHERE task_id = {child_task_id};"
                            run_cql_query(cql_child_end)
                            
                            cql_parent_end = f"UPDATE hydra.catalyst_tasks SET status = 'failed', progress = 100, error_msg = '{err_msg}', updated_at = {now_ms_end} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_parent_end)
                            
                            # Leave status as RECOVERING so Vali does not use it
                            print(f"[Mipha HA] ERROR: Host {hostname} rejoin failed. GlusterFS sync not complete.")
                    
                # 3. Trigger Failover if threshold reached
                if consecutive_failures.get(ip, 0) >= 3:
                    print(f"[Mipha HA] Host {hostname} ({ip}) confirmed OFFLINE! Starting failover orchestration...")
                    consecutive_failures[ip] = 0 # Reset counter to avoid loop
                    
                    # A. Mark Host as DOWN in ScyllaDB
                    print(f"[Mipha HA] Marking host {hostname} status as DOWN in metadata store...")
                    cql_down = f"UPDATE hydra.nodes SET status = 'DOWN' WHERE ip = '{ip}';"
                    run_cql_query(cql_down)
                    
                    # A1. Create parent failover task in Catalyst for WebUI visibility
                    parent_task_id = str(uuid.uuid4())
                    now_ms = int(time.time() * 1000)
                    parent_payload = json.dumps({"hostname": hostname})
                    cql_parent = f"""
                    INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
                    VALUES ({parent_task_id}, 'mipha', 'failover', 'processing', '{parent_payload.replace("'", "''")}', 0, {now_ms}, {now_ms});
                    """
                    run_cql_query(cql_parent)
                    
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
                        
                        # Reset VM status in ScyllaDB so Vali will allow a fresh start
                        cql_reset = f"UPDATE hydra.vms SET state = 'Stopped', host_ip = '' WHERE name = '{vm_name}';"
                        run_cql_query(cql_reset)
                        
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
    main()
