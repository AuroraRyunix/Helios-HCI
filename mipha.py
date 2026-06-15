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

def run_cql_query(cql_query):
    b64_query = base64.b64encode(cql_query.encode('utf-8')).decode('utf-8')
    cmd = f'echo {b64_query} | base64 -d | podman exec -i systemd-hydra-db cqlsh {LOCAL_IP}'
    rc, stdout, stderr = run_remote_spark("127.0.0.1", cmd)
    if rc == 0 and stdout:
        stdout = stdout.replace('\\\\', '\\')
    return rc, stdout, stderr

def is_zookeeper_leader(ip="127.0.0.1"):
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

def get_zookeeper_leader_ip(hosts):
    for h in hosts:
        ip = h.get("ip")
        if not ip:
            continue
        if is_zookeeper_leader(ip):
            return ip
    return None

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
            return response.status == 200
    except Exception:
        return False

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
                    
                # 3. Trigger Failover if threshold reached
                if consecutive_failures.get(ip, 0) >= 3:
                    print(f"[Mipha HA] Host {hostname} ({ip}) confirmed OFFLINE! Starting failover orchestration...")
                    consecutive_failures[ip] = 0 # Reset counter to avoid loop
                    
                    # A. Mark Host as DOWN in ScyllaDB
                    print(f"[Mipha HA] Marking host {hostname} status as DOWN in metadata store...")
                    cql_down = f"UPDATE hydra.nodes SET status = 'DOWN' WHERE ip = '{ip}';"
                    run_cql_query(cql_down)
                    
                    # B. Active Polling for ZooKeeper Recovery
                    print("[Mipha HA] Waiting for ZooKeeper cluster consensus to settle...")
                    zk_leader_ip = None
                    for _ in range(15): # Max 30 seconds polling
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
                    for _ in range(10): # Check if Vali is responsive
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
                        for _ in range(15):
                            if check_vali_health(zk_leader_ip):
                                vali_ok = True
                                print("[Mipha HA] Vali recovered and back online.")
                                break
                            time.sleep(2)
                            
                    if not vali_ok:
                        print("[Mipha HA] WARNING: Vali remains unresponsive. Proceeding with database orchestration.")
                        
                    # D. Query dead host's VMs in ScyllaDB
                    print(f"[Mipha HA] Scanning ScyllaDB for active VMs hosted on dead node {ip}...")
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
                        continue
                        
                    print(f"[Mipha HA] Found {len(orphaned_vms)} orphaned VMs: {[v['name'] for v in orphaned_vms]}")
                    
                    # E. Failover VMs
                    for vm in orphaned_vms:
                        vm_name = vm.get("name")
                        print(f"[Mipha HA] Recovering VM '{vm_name}'...")
                        
                        # Reset VM status in ScyllaDB so Vali will allow a fresh start
                        cql_reset = f"UPDATE hydra.vms SET state = 'Stopped', host_ip = '' WHERE name = '{vm_name}';"
                        run_cql_query(cql_reset)
                        
                        # Submit task to Catalyst queue to start the VM.
                        # target_host is left empty so Vali schedules it on the best surviving node.
                        task_payload = {"vm_name": vm_name, "target_host": ""}
                        success = submit_catalyst_task(zk_leader_ip, "vali", "start", task_payload)
                        if success:
                            print(f"[Mipha HA] Successfully submitted failover task for '{vm_name}' to Catalyst.")
                        else:
                            print(f"[Mipha HA] ERROR: Failed to submit failover task for '{vm_name}' to Catalyst.")
                            
                    print("[Mipha HA] Failover recovery orchestration completed.")
                    
        except Exception as e:
            sys.stderr.write(f"Error in Mipha HA control loop: {e}\n")
            
        time.sleep(10)

if __name__ == "__main__":
    main()
