#!/usr/bin/env python3
__build__ = "1.2.2"
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
import threading

def run_command_local(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return res.returncode, res.stdout.decode('utf-8', errors='ignore').strip(), res.stderr.decode('utf-8', errors='ignore').strip()

def check_linstor_db_mount():
    rc, stdout, stderr = run_command_local("mountpoint -q /var/lib/linstor")
    return rc == 0

def check_linstor_controller_active():
    rc, stdout, stderr = run_command_local("systemctl is-active linstor-controller")
    return stdout == "active"

def get_local_drbd_role(resource_name):
    rc, stdout, stderr = run_command_local(f"drbdadm role {resource_name}")
    if rc == 0:
        return stdout.split("/", 1)[0].strip()
    return "Unknown"

import glob

def get_all_drbd_resources():
    resources = []
    try:
        for path in glob.glob("/etc/drbd.d/*.res"):
            name = os.path.basename(path).replace(".res", "")
            if name != "global_common" and name != "loop_device_mapping":
                resources.append(name)
    except Exception:
        pass
    return resources

def ensure_drbd_resource_up(resource_name):
    rc, stdout, stderr = run_command_local(f"drbdadm status {resource_name}")
    if rc != 0:
        print(f"[Mipha HA] DRBD resource {resource_name} is not loaded. Loading with drbdadm up...")
        run_command_local(f"drbdadm up {resource_name}")

def resolve_drbd_standalone(resource_name):
    try:
        ensure_drbd_resource_up(resource_name)
        rc, stdout, stderr = run_command_local(f"drbdadm status {resource_name}")
        if rc == 0:
            if "StandAlone" in stdout:
                role = get_local_drbd_role(resource_name)
                print(f"[Mipha HA] DRBD resource {resource_name} is in StandAlone state. Resolving (role={role})...")
                if role != "Primary":
                    run_command_local(f"drbdadm disconnect {resource_name}")
                    run_command_local(f"drbdadm secondary {resource_name} || true")
                    run_command_local(f"drbdadm connect --discard-my-data {resource_name}")
                else:
                    run_command_local(f"drbdadm connect {resource_name}")
    except Exception as e:
        sys.stderr.write(f"[Mipha HA] Error resolving DRBD standalone for {resource_name}: {e}\n")

DRBD_SYNC_TRACKER = {}

def check_and_resolve_stuck_resync():
    global DRBD_SYNC_TRACKER
    rc, stdout, stderr = run_command_local("drbdsetup status --json")
    if rc != 0 or not stdout.strip():
        return
        
    try:
        data = json.loads(stdout)
    except Exception:
        return
        
    current_time = time.time()
    current_keys = set()
    
    for resource in data:
        rname = resource.get("name")
        connections = resource.get("connections", [])
        for conn in connections:
            peer_name = conn.get("name")
            peer_devices = conn.get("peer_devices", [])
            for dev in peer_devices:
                vol = dev.get("volume", 0)
                repl_state = dev.get("replication-state", "")
                out_of_sync = dev.get("out-of-sync", 0)
                
                if repl_state in ("SyncTarget", "SyncSource") and out_of_sync > 0:
                    key = (rname, peer_name, vol)
                    current_keys.add(key)
                    
                    tracker = DRBD_SYNC_TRACKER.get(key)
                    if not tracker:
                        DRBD_SYNC_TRACKER[key] = {
                            "last_out_of_sync": out_of_sync,
                            "stalled_count": 0,
                            "last_check_time": current_time
                        }
                    else:
                        # Check every 30 seconds
                        if current_time - tracker["last_check_time"] >= 30:
                            if out_of_sync == tracker["last_out_of_sync"]:
                                tracker["stalled_count"] += 1
                                print(f"[Mipha HA] DRBD resource {rname} resync with {peer_name} is stalled at {out_of_sync} bytes. Stalled count = {tracker['stalled_count']}/3.")
                            else:
                                tracker["stalled_count"] = 0
                                tracker["last_out_of_sync"] = out_of_sync
                            tracker["last_check_time"] = current_time
                            
                            if tracker["stalled_count"] >= 3:
                                print(f"[Mipha HA] DRBD resource {rname} resync with {peer_name} is STUCK (no progress for 90s). Triggering self-heal disconnect/connect...")
                                run_command_local(f"drbdadm disconnect {rname}")
                                time.sleep(1)
                                run_command_local(f"drbdadm connect {rname}")
                                tracker["stalled_count"] = 0
                                tracker["last_check_time"] = current_time
                                
    # Clean up keys that are no longer syncing
    for k in list(DRBD_SYNC_TRACKER.keys()):
        if k not in current_keys:
            DRBD_SYNC_TRACKER.pop(k, None)

def linstor_ha_loop():
    print("[Mipha HA] Linstor Controller HA Thread started.")
    while True:
        try:
            is_leader = is_zookeeper_leader()
            for r in get_all_drbd_resources():
                resolve_drbd_standalone(r)
            
            check_and_resolve_stuck_resync()
            
            # Only manage database HA if the linstor-db resource definition exists on this node
            if os.path.exists("/etc/drbd.d/linstor-db.res"):
                if is_leader:
                    mounted = check_linstor_db_mount()
                    role = get_local_drbd_role("linstor-db")
                    
                    if not mounted or role != "Primary" or not check_linstor_controller_active():
                        print(f"[Mipha HA] Leader State: linstor-db role={role}, mounted={mounted}. Aligning to active...")
                        
                        # Stop the local controller first if we are about to mount, so we don't hold file handles on the root directory
                        if not mounted and check_linstor_controller_active():
                            print("[Mipha HA] Stopping local linstor-controller prior to mounting...")
                            run_command_local("systemctl stop linstor-controller")
                        
                        hosts = get_cluster_hosts()
                        for h in hosts:
                            ip = h.get("ip")
                            if ip and ip != LOCAL_IP:
                                if ping_host(ip):
                                    print(f"[Mipha HA] Coordinating with standby node {h['hostname']} ({ip}) to release linstor-db...")
                                    stop_cmd = (
                                        "if mountpoint -q /var/lib/linstor || [ \"$(drbdadm role linstor-db 2>/dev/null)\" = \"Primary\" ]; then "
                                        "systemctl stop linstor-controller || true; "
                                        "systemctl stop aether || true; "
                                        "umount -l /var/lib/linstor || true; "
                                        "drbdadm secondary linstor-db || true; "
                                        "systemctl start aether || true; "
                                        "else "
                                        "systemctl stop linstor-controller || true; "
                                        "fi"
                                    )
                                    run_remote_spark(ip, stop_cmd)
                                    
                        if role != "Primary":
                            print("[Mipha HA] Promoting linstor-db to Primary...")
                            rc_p, stdout_p, stderr_p = run_command_local("drbdadm primary linstor-db")
                            if rc_p != 0:
                                print(f"[Mipha HA] Warning: drbdadm primary failed: {stderr_p or stdout_p}")
                                
                        if not check_linstor_db_mount():
                            print("[Mipha HA] Mounting linstor-db volume at /var/lib/linstor...")
                            run_command_local("mkdir -p /var/lib/linstor")
                            rc_m, stdout_m, stderr_m = run_command_local("mount -t xfs /dev/drbd/by-res/linstor-db/0 /var/lib/linstor")
                            if rc_m != 0:
                                print(f"[Mipha HA] ERROR: Failed to mount linstor-db volume: {stderr_m or stdout_m}")
                                
                        if check_linstor_db_mount():
                            if not check_linstor_controller_active():
                                print("[Mipha HA] Starting linstor-controller service...")
                                run_command_local("systemctl start linstor-controller")
                        else:
                            print("[Mipha HA] Refusing to start linstor-controller because mount failed.")
                else:
                    if check_linstor_controller_active():
                        print("[Mipha HA] Follower State: Stopping linstor-controller...")
                        run_command_local("systemctl stop linstor-controller")
                        
                    role = get_local_drbd_role("linstor-db")
                    if check_linstor_db_mount() or role == "Primary":
                        print("[Mipha HA] Follower State: Unmounting /var/lib/linstor and demoting to Secondary...")
                        run_command_local("systemctl stop aether || true")
                        run_command_local("umount -l /var/lib/linstor || true")
                        run_command_local("drbdadm secondary linstor-db || true")
                        run_command_local("systemctl start aether || true")

            # Align default storage containers (default-vm-container and default-image-container)
            for container in ["default-vm-container", "default-image-container"]:
                if os.path.exists(f"/etc/drbd.d/{container}.res"):
                    mount_path = f"/var/lib/hci/aether/volumes/{container}"
                    if is_leader:
                        os.makedirs(mount_path, exist_ok=True)
                        rc_m, _, _ = run_command_local(f"mountpoint -q {mount_path}")
                        mounted = (rc_m == 0)
                        role = get_local_drbd_role(container)
                        
                        if not mounted or role != "Primary":
                            print(f"[Mipha HA] Leader State: {container} role={role}, mounted={mounted}. Aligning to active...")
                            
                            # Release on peer standby nodes
                            hosts = get_cluster_hosts()
                            for h in hosts:
                                ip = h.get("ip")
                                if ip and ip != LOCAL_IP:
                                    if ping_host(ip):
                                        stop_cmd = f"umount -l {mount_path} || true; drbdadm secondary {container} || true"
                                        run_remote_spark(ip, stop_cmd)
                                        
                            if role != "Primary":
                                run_command_local(f"drbdadm primary {container}")
                            
                            rc_m, _, _ = run_command_local(f"mountpoint -q {mount_path}")
                            if rc_m != 0:
                                run_command_local(f"mount -t xfs /dev/drbd/by-res/{container}/0 {mount_path}")
                    else:
                        rc_m, _, _ = run_command_local(f"mountpoint -q {mount_path}")
                        if rc_m == 0:
                            print(f"[Mipha HA] Follower State: Unmounting {container}...")
                            run_command_local(f"umount -l {mount_path} || true")
                        role = get_local_drbd_role(container)
                        if role == "Primary":
                            print(f"[Mipha HA] Follower State: Demoting {container} to Secondary...")
                            run_command_local(f"drbdadm secondary {container} || true")
                        
        except Exception as ex:
            sys.stderr.write(f"[Mipha HA] Error in Linstor HA loop: {ex}\n")
            
        time.sleep(2)


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
        return "mode: leader" in resp.lower() or "mode: standalone" in resp.lower()
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
    url = f"http://{ip}:9095/api/v1/hosts"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status == 200
    except Exception:
        return False

def get_dfs_engine():
    return "linstor"

def get_linstor_pending_sync():
    hosts = get_cluster_hosts()
    ips = [h["ip"] for h in hosts] if hosts else ["127.0.0.1"]
    controllers_str = ",".join(ips)
    cmd = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor volume list"
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

def ssh_fence_host(ip):
    print(f"[Mipha HA] Initiating Spark-based fence for host {ip}...")
    fence_cmd = "systemctl stop libvirtd virtqemud || true; pkill -9 qemu-system-x86_64 || true; pkill -9 qemu || true"
    
    # Try fencing via mTLS Spark Daemon
    rc, stdout, stderr = run_remote_spark(ip, fence_cmd)
    if rc == 0:
        print(f"[Mipha HA] Fenced host {ip} using Spark Daemon")
        return True
        
    print(f"[Mipha HA] Spark Fencing failed for host {ip}: {stderr}")
    return False

def main():
    print("Mipha High-Availability Host Monitor and VM Failover Coordinator started.")
    
    # Start the Linstor HA thread in the background
    t = threading.Thread(target=linstor_ha_loop, daemon=True)
    t.start()
    
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
                
                # Host is down if spark-daemon is unresponsive
                if not spark_ok:
                    consecutive_failures[ip] = consecutive_failures.get(ip, 0) + 1
                    print(f"[Mipha HA] Host {hostname} ({ip}) health check failed (Count: {consecutive_failures[ip]}/3)")
                else:
                    consecutive_failures[ip] = 0
                    
                    # If host was previously marked DOWN, initiate rejoin/sync sequence
                    if db_status == "DOWN":
                        print(f"[Mipha HA] Host {hostname} ({ip}) is back online! Starting rejoining and Linstor/DRBD sync sequence...")
                        
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
                        start_cmd = "systemctl start zookeeper hydra-db aether linstor-controller spectrum bifrost dagur mimir vali catalyst gatoway logos mipha"
                        run_remote_spark(ip, start_cmd)
                        
                        # Sleep 10 seconds to allow services (especially Aether/storage) to boot
                        time.sleep(10)
                        
                        # Update parent task progress to 20%
                        cql_up = f"UPDATE hydra.catalyst_tasks SET progress = 20, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                        run_cql_query(cql_up)
                        
                        # C. Trigger self-heal (skipped for Linstor/DRBD)
                        pass
                        
                        # D. Create child Catalyst task for Linstor/DRBD sync
                        child_task_id = str(uuid.uuid4())
                        child_payload = json.dumps({"hostname": hostname, "parent_task_id": parent_task_id})
                        cql_child = f"""
                        INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
                        VALUES ({child_task_id}, 'aether', 'sync', 'processing', '{child_payload.replace("'", "''")}', 10, {now_ms}, {now_ms});
                        """
                        run_cql_query(cql_child)
                        
                        # E. Poll sync status
                        synced = False
                        # Poll up to 60 iterations (3 minutes)
                        for iteration in range(60):
                            child_progress = min(95, 10 + iteration * 5)
                            parent_progress = int(20 + (child_progress / 100.0) * 70)
                            
                            cql_up_child = f"UPDATE hydra.catalyst_tasks SET progress = {child_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {child_task_id};"
                            run_cql_query(cql_up_child)
                            
                            cql_up_parent = f"UPDATE hydra.catalyst_tasks SET progress = {parent_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_up_parent)
                            
                            pending = get_linstor_pending_sync()
                            print(f"[Mipha HA] Linstor/DRBD sync status: pending_sync_active={pending}")
                            if pending == 0:
                                synced = True
                                print(f"[Mipha HA] Linstor/DRBD resources fully synced on host {hostname}!")
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
                            err_msg = f"{storage_name} volume sync timed out or failed to complete self-heal."
                            cql_child_end = f"UPDATE hydra.catalyst_tasks SET status = 'failed', progress = 100, error_msg = '{err_msg}', updated_at = {now_ms_end} WHERE task_id = {child_task_id};"
                            run_cql_query(cql_child_end)
                            
                            cql_parent_end = f"UPDATE hydra.catalyst_tasks SET status = 'failed', progress = 100, error_msg = '{err_msg}', updated_at = {now_ms_end} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_parent_end)
                            
                            # Leave status as RECOVERING so Vali does not use it
                            print(f"[Mipha HA] ERROR: Host {hostname} rejoin failed. {storage_name} sync not complete.")
                    
                # 3. Trigger Failover if threshold reached
                if consecutive_failures.get(ip, 0) >= 3:
                    print(f"[Mipha HA] Host {hostname} ({ip}) confirmed OFFLINE! Starting failover orchestration...")
                    consecutive_failures[ip] = 0 # Reset counter to avoid loop
                    
                    # SSH Fencing if the host is still pingable
                    if ping_ok:
                        ssh_fence_host(ip)
                    
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
