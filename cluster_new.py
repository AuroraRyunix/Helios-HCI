#!/usr/bin/env python3
import sys
import argparse
import json
import ssl
import urllib.request
import os
import time
import base64

def get_cluster_ips():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            return [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        return ["127.0.0.1"]

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

def run_cql_query(cql_query):
    ips = get_cluster_ips()
    for ip in ips:
        b64_query = base64.b64encode(cql_query.encode('utf-8')).decode('utf-8')
        cmd = f"echo {b64_query} | base64 -d | podman exec -i systemd-hydra-db cqlsh {ip}"
        rc, stdout, stderr = run_remote_spark(ip, cmd)
        if rc == 0:
            return 0, stdout, ""
    return -1, "", "Failed to connect to ScyllaDB on any node in the cluster."

def make_request(path, method="GET", payload=None):
    # Try VIP if configured
    vip = None
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                vip = cdata.get("vip")
    except Exception:
        pass

    target_ips = []
    if vip:
        target_ips.append(vip)
    target_ips.append("127.0.0.1")

    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = False

    last_err = ""
    for ip in target_ips:
        url = f"https://{ip}:9099{path}"
        data = None
        if payload is not None:
            data = json.dumps(payload).encode('utf-8')
            
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            # Short timeout for checking VIP, longer for orchestration
            timeout = 15 if "status" in path else 130
            with urllib.request.urlopen(req, context=context, timeout=timeout) as response:
                return 0, json.loads(response.read().decode('utf-8'))
        except Exception as e:
            last_err = str(e)
            
    return -1, {"error": f"Failed to connect to spark-daemon (tried {', '.join(target_ips)}): {last_err}"}

def main():
    parser = argparse.ArgumentParser(description="HCI Cluster Management Utility")
    parser.add_argument("-s", "--servers", required=False, help="Comma-separated list of host IPs")
    parser.add_argument("-r", "--redundancy_factor", type=int, default=None, help="Fault Tolerance to Tolerate (FTT) / Redundancy Factor (e.g. 0, 1, or 2)")
    parser.add_argument("-v", "--vip", required=False, help="Floating Cluster Virtual IP (VIP)")
    parser.add_argument("--verbose", action="store_true", help="Print verbose status information")
    parser.add_argument("command", choices=["create", "status", "start", "stop", "destroy"], help="Action to perform")
    
    args = parser.parse_args()
    
    if args.command == "create":
        # Ensure we have servers
        config_ips = []
        try:
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                config_ips = [h["ip"] for h in cdata.get("hosts", [])]
        except Exception:
            pass

        if args.servers:
            ips = [ip.strip() for ip in args.servers.split(",") if ip.strip()]
        elif config_ips:
            ips = config_ips
        else:
            parser.error("the following arguments are required: -s/--servers (or a valid /etc/hci/cluster.json config)")

        rf = args.redundancy_factor if args.redundancy_factor is not None else 1
        vip = args.vip if args.vip else ""

        print("==========================================================")
        print(f"   Creating HCI Cluster (Redundancy Factor/FTT={rf})  ")
        print("==========================================================")
        print("Issuing create request to spark-daemon...")

        payload = {
            "servers": ips,
            "redundancy_factor": rf,
            "vip": vip
        }
        
        rc, res = make_request("/api/v1/cluster/create", method="POST", payload=payload)
        if rc == 0:
            print("\n==========================================================")
            print("      HCI Cluster Creation Successful!                    ")
            print("==========================================================")
        else:
            print(f"\n[ERROR] Creation failed: {res.get('error')}")
            sys.exit(1)

    elif args.command == "status":
        print("==========================================================")
        print("                 HCI Cluster Status                       ")
        print("==========================================================")
        
        path = "/api/v1/cluster/status"
        if args.verbose:
            path += "?verbose=true"
            
        rc, res = make_request(path, method="GET")
        if rc == 0:
            cluster_state = res.get("cluster_state", "stop")
            # map 'start' to 'started', 'stop' to 'stopped'
            state_str = "started" if cluster_state == "start" else "stopped"
            print(f"The state of the cluster: {state_str}")
            print("Lockdown mode: Disabled")
            
            print("\n--- Storage Engine Status (Aether) ---")
            print(res.get("peer_status") or "No peer info")
            
            print("\n--- Storage Engine Volumes (Aether) ---")
            print(res.get("volume_info") or "No volume info")
            
            print("\n--- Cluster Services Status ---")
            node_statuses = res.get("node_statuses", {})
            for ip, info in node_statuses.items():
                if info.get("online"):
                    print(info.get("output"))
                else:
                    print(f"\n        Host: {ip} Down")
                    print(f"                    Error: {info.get('error')}")
            print("==========================================================")
        else:
            print(f"[ERROR] Failed to query status: {res.get('error')}")
            sys.exit(1)

    elif args.command == "start":
        print("==========================================================")
        print("                 Starting HCI Cluster                     ")
        print("==========================================================")
        ips = get_cluster_ips()
        print(f"Connecting to cluster nodes: {', '.join(ips)}")
        
        # 1. Verify spark-daemon is running on all hosts
        spark_online = {}
        for ip in ips:
            print(f"[{ip}] Contacting spark-daemon on port 9099...")
            rc, stdout, stderr = run_remote_spark(ip, "echo 'online'")
            if rc == 0 and "online" in stdout.lower():
                print(f"[{ip}] spark-daemon is online.")
                spark_online[ip] = True
            else:
                print(f"[{ip}] ERROR: spark-daemon is offline or unreachable: {stderr or 'Connection timeout'}")
                spark_online[ip] = False
                
        if not all(spark_online.values()):
            print("[ERROR] Cannot start cluster: spark-daemon must be online on all nodes.")
            sys.exit(1)

        # 2. Start ZooKeeper Service
        print("\n--- Phase 1: Starting ZooKeeper Service ---")
        for ip in ips:
            print(f"[{ip}] Starting ZooKeeper service...")
            run_remote_spark(ip, "systemctl start zookeeper")
            
        # Poll ZooKeeper active state
        for ip in ips:
            print(f"[{ip}] Waiting for ZooKeeper service to become active...")
            for _ in range(30):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active zookeeper")
                if rc == 0 and out.strip() == "active":
                    print(f"[{ip}] ZooKeeper service is active.")
                    break
                time.sleep(1)
            else:
                print(f"[{ip}] ERROR: ZooKeeper failed to start.")
                sys.exit(1)
                
        # Wait for consensus quorum
        print("Waiting for ZooKeeper quorum consensus to form...")
        time.sleep(4)
        
        leader_found = False
        for ip in ips:
            cmd_stat = "echo stat | nc 127.0.0.1 2181"
            rc_s, out_s, _ = run_remote_spark(ip, cmd_stat)
            if rc_s == 0 and "mode: leader" in out_s.lower():
                print(f"[{ip}] Found ZooKeeper Leader node.")
                leader_found = True
        if not leader_found:
            print("[WARNING] ZooKeeper leader node could not be identified, continuing anyway.")

        # 3. Set cluster state in ZooKeeper
        print("Writing cluster state 'started' to ZooKeeper consensus...")
        zk_set = False
        for ip in ips:
            rc_state, _, _ = run_remote_spark(ip, "podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 set /cluster_state started || podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 create /cluster_state started")
            if rc_state == 0:
                zk_set = True
                break
        if zk_set:
            print("Cluster state successfully set to 'started' in ZooKeeper.")
        else:
            print("[WARNING] Could not write cluster state to ZooKeeper.")

        # 4. Start ScyllaDB (hydra-db)
        print("\n--- Phase 2: Starting ScyllaDB Database Service ---")
        for ip in ips:
            print(f"[{ip}] Starting hydra-db systemd service...")
            run_remote_spark(ip, "systemctl start hydra-db")
            
        for ip in ips:
            print(f"[{ip}] Waiting for hydra-db service to become active...")
            for _ in range(35):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active hydra-db")
                if rc == 0 and out.strip() == "active":
                    break
                time.sleep(1)
            else:
                print(f"[{ip}] ERROR: hydra-db service failed to start.")
                sys.exit(1)
                
        for ip in ips:
            print(f"[{ip}] Waiting for ScyllaDB to start listening on port 9042...")
            for _ in range(60):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9042")
                if rc == 0 and "9042" in out:
                    print(f"[{ip}] ScyllaDB is accepting database connections on port 9042.")
                    break
                time.sleep(1)
            else:
                print(f"[{ip}] ERROR: ScyllaDB database connection port 9042 timeout.")
                sys.exit(1)

        # 5. Start Aether Storage Service
        print("\n--- Phase 3: Starting Aether Storage Service ---")
        for ip in ips:
            print(f"[{ip}] Starting aether systemd service...")
            run_remote_spark(ip, "systemctl start aether")
            
        for ip in ips:
            print(f"[{ip}] Waiting for aether service to become active...")
            for _ in range(30):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active aether")
                if rc == 0 and out.strip() == "active":
                    break
                time.sleep(1)
            else:
                print(f"[{ip}] ERROR: aether service failed to start.")
                sys.exit(1)
                
        # Mount GlusterFS volumes
        for ip in ips:
            print(f"[{ip}] Mounting GlusterFS volumes inside container...")
            mount_cmd = (
                "podman exec systemd-aether mkdir -p /var/lib/hci/aether/volumes/default-vm-container && "
                "podman exec systemd-aether findmnt /var/lib/hci/aether/volumes/default-vm-container || "
                "podman exec systemd-aether mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-vm-container /var/lib/hci/aether/volumes/default-vm-container"
            )
            rc_m, _, err_m = run_remote_spark(ip, mount_cmd)
            if rc_m == 0:
                print(f"[{ip}] default-vm-container volume mounted successfully.")
            else:
                print(f"[{ip}] Warning mounting default-vm-container volume: {err_m}")
                
            mount_img_cmd = (
                "podman exec systemd-aether mkdir -p /var/lib/hci/aether/volumes/default-image-container && "
                "podman exec systemd-aether findmnt /var/lib/hci/aether/volumes/default-image-container || "
                "podman exec systemd-aether mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-image-container /var/lib/hci/aether/volumes/default-image-container"
            )
            rc_m2, _, err_m2 = run_remote_spark(ip, mount_img_cmd)
            if rc_m2 == 0:
                print(f"[{ip}] default-image-container volume mounted successfully.")
            else:
                print(f"[{ip}] Warning mounting default-image-container volume: {err_m2}")

        # 6. Start remaining services
        print("\n--- Phase 4: Starting Core Workload & Coordination Services ---")
        services = ["spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha"]
        service_ports = {
            "spectrum": 8443,
            "vali": 9095,
            "catalyst": 9091
        }
        
        for svc in services:
            for ip in ips:
                print(f"[{ip}] Starting systemd service: {svc}...")
                run_remote_spark(ip, f"systemctl start {svc}")
                
        for svc in services:
            for ip in ips:
                print(f"[{ip}] Verifying service {svc} is active...")
                for _ in range(30):
                    rc, out, _ = run_remote_spark(ip, f"systemctl is-active {svc}")
                    if rc == 0 and out.strip() == "active":
                        break
                    time.sleep(1)
                else:
                    print(f"[{ip}] ERROR: Service '{svc}' failed to enter active state.")
                    sys.exit(1)
                    
                if svc in service_ports:
                    port = service_ports[svc]
                    print(f"[{ip}] Waiting for service {svc} to listen on port {port}...")
                    for _ in range(45):
                        rc_p, out_p, _ = run_remote_spark(ip, f"ss -tlnp | grep {port}")
                        if rc_p == 0 and str(port) in out_p:
                            print(f"[{ip}] Service {svc} is listening on port {port}.")
                            break
                        time.sleep(1)
                    else:
                        print(f"[{ip}] ERROR: Service {svc} failed to listen on port {port}.")
                        sys.exit(1)
                        
        print("\n==========================================================")
        print("      HCI Cluster Started & Verified Successfully!       ")
        print("==========================================================")

    elif args.command == "stop":
        print("==========================================================")
        print("                 Stopping HCI Cluster                     ")
        print("==========================================================")
        
        # 1. Stop running VMs step-by-step
        print("--- Step 1: Stopping running VMs step-by-step ---")
        rc, stdout, err = run_cql_query("SELECT JSON name, host_ip, state FROM hydra.vms;")
        vms = []
        if rc == 0:
            for line in stdout.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        vms.append(json.loads(line))
                    except:
                        pass
        
        running_vms = [v for v in vms if v.get("state") in ["Running", "start", "on"]]
        if running_vms:
            for vm in running_vms:
                name = vm.get("name")
                host_ip = vm.get("host_ip")
                if not host_ip or host_ip == "N/A":
                    continue
                print(f"Stopping VM '{name}' on host {host_ip}...")
                run_remote_spark(host_ip, f"virsh shutdown {name}")
                
                # Poll up to 5 seconds
                stopped = False
                for _ in range(5):
                    time.sleep(1)
                    rc_dom, dom_state, _ = run_remote_spark(host_ip, f"virsh domstate {name}")
                    if rc_dom == 0 and "shut off" in dom_state.lower():
                        stopped = True
                        break
                if not stopped:
                    print(f"VM '{name}' did not shut down gracefully. Forcing power off (destroy)...")
                    run_remote_spark(host_ip, f"virsh destroy {name}")
                
                # Update ScyllaDB
                run_cql_query(f"UPDATE hydra.vms SET state = 'Stopped', host_ip = '' WHERE name = '{name}';")
        else:
            print("No running VMs detected.")
            
        # 2. Set cluster state to stopped in ZooKeeper
        print("\n--- Step 2: Setting cluster state in ZooKeeper ---")
        zk_set = False
        for ip in get_cluster_ips():
            rc_zk, _, _ = run_remote_spark(ip, "podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 set /cluster_state stopped")
            if rc_zk == 0:
                zk_set = True
                break
        if zk_set:
            print("Cluster state set to 'stopped' in ZooKeeper.")
        else:
            print("Warning: Failed to set cluster state to stopped in ZooKeeper.")
            
        # 3. Unmount default volumes
        print("\n--- Step 3: Unmounting default volumes ---")
        for ip in get_cluster_ips():
            print(f"[{ip}] Unmounting default volume containers...")
            run_remote_spark(ip, "podman exec systemd-aether umount -f /var/lib/hci/aether/volumes/default-vm-container || true")
            run_remote_spark(ip, "podman exec systemd-aether umount -f /var/lib/hci/aether/volumes/default-image-container || true")
            
        # 4. Stop systemd services sequentially
        print("\n--- Step 4: Stopping systemd services sequentially ---")
        services = ["spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha", "aether", "hydra-db", "zookeeper"]
        for ip in get_cluster_ips():
            print(f"[{ip}] Stopping services...")
            for svc in services:
                print(f"[{ip}] Stopping systemd service: {svc}...")
                rc_svc, _, err_svc = run_remote_spark(ip, f"systemctl stop {svc}")
                if rc_svc != 0:
                    print(f"[{ip}] Warning: Failed to stop service '{svc}': {err_svc}")
                    
        # 5. Restart spark-daemon asynchronously
        print("\n--- Step 5: Restarting spark-daemon asynchronously ---")
        for ip in get_cluster_ips():
            print(f"[{ip}] Restarting spark-daemon...")
            run_remote_spark(ip, "(sleep 1 && systemctl restart spark-daemon) >/dev/null 2>&1 < /dev/null &")
            
        print("Stop command execution completed.")

    elif args.command == "destroy":
        print("==========================================================")
        print("                 Destroying HCI Cluster                   ")
        print("==========================================================")
        rc, res = make_request("/api/v1/cluster/destroy", method="POST")
        if rc == 0:
            print("\n==========================================================")
            print("      HCI Cluster Destroyed & Cleaned Successfully!        ")
            print("==========================================================")
        else:
            print(f"[ERROR] Destroy failed: {res.get('error')}")
            sys.exit(1)

if __name__ == "__main__":
    main()
