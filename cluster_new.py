#!/usr/bin/env python3
import sys
import argparse
import json
import ssl
import urllib.request
import os
import time
import base64
import threading
import socket

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


def get_cluster_ips():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            return [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        return ["127.0.0.1"]

def get_dfs_engine():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            return cdata.get("dfs_engine", "glusterfs")
    except Exception:
        return "glusterfs"


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

def run_checked_cmd(ip, command, allow_already_exists=False):
    print(f"[{ip}] Running command: {command}")
    rc, stdout, stderr = run_remote_spark(ip, command)
    stdout = stdout.strip() if stdout else ""
    stderr = stderr.strip() if stderr else ""
    if stdout:
        print(f"[{ip}] stdout:\n{stdout}")
    if stderr:
        print(f"[{ip}] stderr:\n{stderr}")
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
            print(f"[ERROR] Command failed on {ip} with exit code {rc}. Command: {command}")
            sys.exit(1)
    return rc, stdout, stderr

def run_parallel_checked(ips, command, allow_already_exists=False):
    print(f"Running parallel command on {ips}: {command}")
    results = run_parallel(ips, command)
    for ip, (rc, stdout, stderr) in results.items():
        stdout = stdout.strip() if stdout else ""
        stderr = stderr.strip() if stderr else ""
        if stdout:
            print(f"[{ip}] stdout:\n{stdout}")
        if stderr:
            print(f"[{ip}] stderr:\n{stderr}")
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
                print(f"[ERROR] Parallel command failed on {ip} with exit code {rc}. Command: {command}")
                sys.exit(1)
    return results

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
def check_urbosa_enabled():
    rc, stdout, _ = run_cql_query("SELECT value FROM hydra.cluster_settings WHERE key = 'urbosa_enabled';")
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            if "true" in line.lower():
                return True
    return False

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
        if len(ips) == 1:
            if rf > 0:
                print(f"[WARNING] Single-node cluster detected. Forcing redundancy factor (FTT) from {rf} to 0 (no replication).")
            rf = 0
        vip = args.vip if args.vip else ""

        print("==========================================================")
        print(f"   Creating HCI Cluster (Redundancy Factor/FTT={rf})  ")
        print("==========================================================")

        # 1. Connectivity & Pre-checks
        print("\n--- Phase 1: Connectivity & Pre-checks ---")
        for ip in ips:
            print(f"[{ip}] Testing connectivity...")
            rc, stdout, stderr = run_remote_spark(ip, "echo 'online'")
            if rc != 0 or "online" not in stdout.lower():
                print(f"[ERROR] Could not connect to spark-daemon on {ip}: {stderr}")
                sys.exit(1)
            print(f"[{ip}] spark-daemon is online.")
            
            # Check port conflicts
            print(f"[{ip}] Checking port conflicts...")
            rc, stdout, _ = run_remote_spark(ip, "ss -tlnp")
            if rc == 0:
                for port in ["7000", "3370"]:
                    if port in stdout:
                        print(f"[WARNING] Port {port} is already in use on {ip}. This may cause conflicts.")

        # 2. Hostname Resolution & Cluster JSON Config
        print("\n--- Phase 2: Hostname Resolution & Cluster Setup ---")
        hosts_info = []
        for idx, ip in enumerate(ips):
            print(f"[{ip}] Resolving hostname...")
            rc, hostname, _ = run_remote_spark(ip, "hostname")
            hostname = hostname.strip() if rc == 0 else f"node-{idx+1}"
            print(f"[{ip}] Resolved hostname: {hostname}")
            hosts_info.append({
                "node_id": idx + 1,
                "ip": ip,
                "hostname": hostname
            })

        cluster_json_data = {
            "cluster_name": "hci-01",
            "redundancy_factor": rf,
            "dfs_engine": "linstor",
            "vip": vip,
            "hosts": hosts_info
        }
        
        json_b64 = base64.b64encode(json.dumps(cluster_json_data, indent=4).encode('utf-8')).decode('utf-8')
        write_config_cmd = f"mkdir -p /etc/hci && echo {json_b64} | base64 -d > /etc/hci/cluster.json"
        print("Writing /etc/hci/cluster.json on all nodes...")
        results = run_parallel(ips, write_config_cmd)
        for ip, (rc, _, err) in results.items():
            if rc != 0:
                print(f"[ERROR] Failed to write cluster.json on {ip}: {err}")
                sys.exit(1)

        # 3. Dynamic Disk Setup (Non-boot disks >= 100GB)
        print("\n--- Phase 3: Dynamic Disk Scan & LVM Setup ---")
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
subprocess.run("pvcreate -y " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("rm -rf /dev/vg_aether", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("vgcreate vg_aether " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("lvcreate -y -l 100%FREE -T vg_aether/thin_pool_aether", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
print(json.dumps({"status": "created", "device": dev_path, "size_bytes": size_bytes}))
"""
        claim_script_b64 = base64.b64encode(disk_claim_script.strip().encode()).decode()
        cmd_claim = f"python3 -c \"import base64; exec(base64.b64decode('{claim_script_b64}').decode())\""
        
        print("Scanning and setting up storage pools on remote hosts in parallel...")
        claim_results = run_parallel(ips, cmd_claim)
        
        host_claimed_disks = {}
        for ip, (rc, stdout, stderr) in claim_results.items():
            if rc == 0:
                try:
                    disk_info = json.loads(stdout.strip())
                    if "error" in disk_info:
                        print(f"[ERROR] Host {ip} disk setup failed: {disk_info['error']}")
                        sys.exit(1)
                    host_claimed_disks[ip] = disk_info
                    print(f"[{ip}] Successfully configured storage on device {disk_info['device']} ({disk_info['size_bytes'] / 10**9:.1f} GB) - Status: {disk_info['status']}")
                except Exception as e:
                    print(f"[ERROR] Host {ip} returned invalid json: {stdout} ({e})")
                    sys.exit(1)
            else:
                print(f"[ERROR] Host {ip} failed disk claiming: {stderr}")
                sys.exit(1)

        # 4. Storage Engine Setup (Linstor)
        print("\n--- Phase 4: Initializing Linstor Storage Engine ---")
        print("Creating Linstor storage directories on all nodes...")
        run_parallel_checked(ips, "mkdir -p /var/lib/linstor /etc/linstor")
        
        print("Starting Aether storage services in parallel...")
        run_parallel_checked(ips, "systemctl start aether")
        
        print("Starting Linstor Controller on all nodes...")
        run_parallel_checked(ips, "systemctl start linstor-controller")
        
        print("Waiting for Linstor Controller to listen on port 3370 on all nodes...")
        for ip in ips:
            controller_ready = False
            for _ in range(30):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 3370")
                if rc == 0 and "3370" in out:
                    controller_ready = True
                    break
                time.sleep(1)
            if not controller_ready:
                print(f"[ERROR] Linstor Controller failed to start on port 3370 on {ip}.")
                sys.exit(1)
        print("Linstor Controller is ready on all nodes.")

        print("Setting Linstor DRBD port range (7700-7890) to avoid conflicts...")
        for ip in ips:
            run_checked_cmd(ip, "podman exec systemd-linstor-controller linstor controller set-property TcpPortAutoRange 7700-7890", allow_already_exists=True)

        print("Creating Linstor node definitions...")
        for h in hosts_info:
            print(f"Creating Linstor node for {h['hostname']} ({h['ip']})...")
            run_checked_cmd(ips[0], f"podman exec systemd-linstor-controller linstor node create {h['hostname']} {h['ip']}", allow_already_exists=True)

        print("Registering Linstor storage pools...")
        for h in hosts_info:
            print(f"[{h['ip']}] Registering vg_aether/thin_pool_aether...")
            run_checked_cmd(ips[0], f"podman exec systemd-linstor-controller linstor storage-pool create lvmthin {h['hostname']} default-pool vg_aether/thin_pool_aether", allow_already_exists=True)

        print("Creating Linstor resource and volume definitions...")
        run_checked_cmd(ips[0], "podman exec systemd-linstor-controller linstor resource-definition create default-vm-container", allow_already_exists=True)
        run_checked_cmd(ips[0], "podman exec systemd-linstor-controller linstor volume-definition create default-vm-container 120G", allow_already_exists=True)
        run_checked_cmd(ips[0], "podman exec systemd-linstor-controller linstor resource-definition create default-image-container", allow_already_exists=True)
        run_checked_cmd(ips[0], "podman exec systemd-linstor-controller linstor volume-definition create default-image-container 40G", allow_already_exists=True)

        repl_count = min(len(ips), rf + 1)
        print(f"Spawning replicated resources (replication count = {repl_count})...")
        for h in hosts_info[:repl_count]:
            print(f"Spawning storage volumes on {h['hostname']}...")
            run_checked_cmd(ips[0], f"podman exec systemd-linstor-controller linstor resource create {h['hostname']} default-vm-container --storage-pool default-pool", allow_already_exists=True)
            run_checked_cmd(ips[0], f"podman exec systemd-linstor-controller linstor resource create {h['hostname']} default-image-container --storage-pool default-pool", allow_already_exists=True)
            
        for h in hosts_info[repl_count:]:
            print(f"Spawning diskless storage client on {h['hostname']}...")
            run_checked_cmd(ips[0], f"podman exec systemd-linstor-controller linstor resource create {h['hostname']} default-vm-container --diskless", allow_already_exists=True)
            run_checked_cmd(ips[0], f"podman exec systemd-linstor-controller linstor resource create {h['hostname']} default-image-container --diskless", allow_already_exists=True)

        print("Waiting for DRBD block devices to appear on Node 1...")
        drbd_ready = False
        for _ in range(45):
            rc1, _, _ = run_remote_spark(ips[0], "test -b /dev/drbd/by-res/default-vm-container/0")
            rc2, _, _ = run_remote_spark(ips[0], "test -b /dev/drbd/by-res/default-image-container/0")
            if rc1 == 0 and rc2 == 0:
                drbd_ready = True
                print("DRBD block devices are ready on Node 1.")
                break
            time.sleep(1)
        if not drbd_ready:
            print("[ERROR] DRBD block devices did not appear on Node 1 within timeout.")
            sys.exit(1)

        print("Formatting DRBD block devices with XFS...")
        run_checked_cmd(ips[0], "mkfs.xfs -f /dev/drbd/by-res/default-vm-container/0")
        run_checked_cmd(ips[0], "mkfs.xfs -f /dev/drbd/by-res/default-image-container/0")

        print("Synchronizing Linstor database to all other controller nodes...")
        rc, db_b64, err = run_checked_cmd(ips[0], "base64 /var/lib/linstor/linstordb.mv.db")
        if rc == 0 and db_b64:
            db_b64 = db_b64.replace("\n", "").replace("\r", "").strip()
            for target_ip in ips[1:]:
                print(f"Syncing Linstor database to {target_ip}...")
                run_checked_cmd(target_ip, "systemctl stop linstor-controller")
                run_checked_cmd(target_ip, f"mkdir -p /var/lib/linstor && echo {db_b64} | base64 -d > /var/lib/linstor/linstordb.mv.db")
                run_checked_cmd(target_ip, "systemctl start linstor-controller")

        print("Writing storage pools config and spectrum configuration on all hosts...")
        for ip in ips:
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
                "storage_containers": [
                    {
                        "name": "default-vm-container",
                        "path": "/default-pool/default-vm",
                        "ftt": rf,
                        "compression": "lz4",
                        "quota_bytes": 0
                    },
                    {
                        "name": "default-image-container",
                        "path": "/default-pool/default-image",
                        "ftt": rf,
                        "compression": "lz4",
                        "quota_bytes": 0
                    }
                ]
            }
            json_str = json.dumps(storage_pool_json, indent=2)
            b64_str = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
            run_remote_spark(ip, f"mkdir -p /etc/hci/aether && echo {b64_str} | base64 -d > /etc/hci/aether/storage-pools.json")

            controllers_line = ",".join(ips)
            client_conf = f"[active]\ncontrollers = {controllers_line}\n"
            client_b64 = base64.b64encode(client_conf.encode('utf-8')).decode('utf-8')
            run_remote_spark(ip, f"mkdir -p /etc/linstor && echo {client_b64} | base64 -d > /etc/linstor/linstor-client.conf")

            seeds = ",".join(ips)
            spectrum_env = f"SPECTRUM_API_PORT=8443\nLOCAL_HYPERVISOR_IP={ip}\nCLUSTER_SEEDS={seeds}"
            env_b64 = base64.b64encode(spectrum_env.encode('utf-8')).decode('utf-8')
            run_remote_spark(ip, f"mkdir -p /etc/hci/spectrum && echo {env_b64} | base64 -d > /etc/hci/spectrum/spectrum.env")

        print("Mounting storage volumes on all nodes in parallel...")
        mount_cmd = (
            "for i in {1..30}; do if [ -b /dev/drbd/by-res/default-vm-container/0 ]; then break; fi; sleep 1; done && "
            "mkdir -p /var/lib/hci/aether/volumes/default-vm-container && "
            "mountpoint -q /var/lib/hci/aether/volumes/default-vm-container || "
            "mount -t xfs /dev/drbd/by-res/default-vm-container/0 /var/lib/hci/aether/volumes/default-vm-container"
        )
        run_parallel_checked(ips, mount_cmd)
        
        mount_img_cmd = (
            "for i in {1..30}; do if [ -b /dev/drbd/by-res/default-image-container/0 ]; then break; fi; sleep 1; done && "
            "mkdir -p /var/lib/hci/aether/volumes/default-image-container && "
            "mountpoint -q /var/lib/hci/aether/volumes/default-image-container || "
            "mount -t xfs /dev/drbd/by-res/default-image-container/0 /var/lib/hci/aether/volumes/default-image-container"
        )
        run_parallel_checked(ips, mount_img_cmd)

        # 5. Database Quorum Setup
        print("\n--- Phase 5: Starting Databases & Query Proxy ---")
        print("Creating ZooKeeper and ScyllaDB directories on all nodes...")
        run_parallel_checked(ips, "mkdir -p /var/lib/hci/zookeeper/data /var/lib/hci/zookeeper/log /var/lib/hci/hydra/data")
        
        # Copy Daruk proxy script to ScyllaDB volume directory
        print("Copying Daruk query proxy script to ScyllaDB volume directory on all nodes...")
        run_parallel_checked(ips, "mkdir -p /var/lib/hci/hydra/data && cp /usr/local/bin/daruk.py /var/lib/hci/hydra/data/daruk.py && chmod 644 /var/lib/hci/hydra/data/daruk.py")

        print("Starting ZooKeeper service in parallel...")
        run_parallel_checked(ips, "systemctl start zookeeper")
        for ip in ips:
            for _ in range(30):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active zookeeper")
                if rc == 0 and out.strip() == "active":
                    break
                time.sleep(1)
            else:
                print(f"[ERROR] ZooKeeper failed to start on {ip}")
                sys.exit(1)

        print("Writing cluster state 'started' to ZooKeeper consensus...")
        zk_set = False
        for ip in ips:
            rc_state, _, _ = run_remote_spark(ip, "podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 set /cluster_state started || podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 create /cluster_state started")
            if rc_state == 0:
                zk_set = True
                break
        if not zk_set:
            print("[WARNING] Could not write cluster state to ZooKeeper.")

        print("Starting ScyllaDB Database Service in parallel...")
        run_parallel_checked(ips, "systemctl start hydra-db")
        for ip in ips:
            for _ in range(40):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active hydra-db")
                if rc == 0 and out.strip() == "active":
                    break
                time.sleep(1)
            else:
                print(f"[ERROR] hydra-db failed to start on {ip}")
                sys.exit(1)

        print("Waiting for ScyllaDB to listen on port 9042 on all nodes...")
        for ip in ips:
            print(f"[{ip}] Waiting for ScyllaDB to listen on port 9042...")
            for _ in range(180):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9042")
                if rc == 0 and "9042" in out:
                    break
                time.sleep(1)
            else:
                print(f"[ERROR] ScyllaDB port 9042 timeout on {ip}")
                sys.exit(1)

        print("Starting Daruk query proxy service on all hosts...")
        run_parallel_checked(ips, "systemctl start daruk")
        print("Waiting for Daruk query proxy to listen on port 9043 on all nodes...")
        for ip in ips:
            daruk_ready = False
            for _ in range(30):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9043")
                if rc == 0 and "9043" in out:
                    daruk_ready = True
                    break
                time.sleep(1)
            if not daruk_ready:
                print(f"[ERROR] Daruk query proxy failed to listen on port 9043 on {ip}")
                sys.exit(1)
        print("Daruk query proxy is ready on all nodes.")

        # 6. Start Workload Services
        print("\n--- Phase 6: Starting Core HCI Services ---")
        services = ["spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha"]
        
        # Check if urbosa enabled
        urbosa_enabled = False
        time.sleep(3) # Wait briefly for ScyllaDB schemas/proxies to stabilize
        rc, out, _ = run_cql_query("SELECT value FROM hydra.cluster_settings WHERE key = 'urbosa_enabled';")
        if rc == 0 and out:
            for line in out.splitlines():
                if "true" in line.lower():
                    urbosa_enabled = True
                    break
        if urbosa_enabled:
            services.append("urbosa")

        for svc in services:
            print(f"Starting {svc} service in parallel across all nodes...")
            run_parallel_checked(ips, f"systemctl start {svc}")
            for ip in ips:
                for _ in range(30):
                    rc, out, _ = run_remote_spark(ip, f"systemctl is-active {svc}")
                    if rc == 0 and out.strip() == "active":
                        break
                    time.sleep(1)
                else:
                    print(f"[ERROR] Service {svc} failed to enter active state on {ip}")
                    sys.exit(1)

        # 7. Verification & Liveness Check Loop
        print("\n--- Phase 7: Verifying Liveness & Cluster Health ---")
        print("Polling ScyllaDB Gossip Status until all nodes are Up-Normal (UN)...")
        gossip_healthy = False
        for i in range(30):
            rc, out, _ = run_remote_spark(ips[0], "podman exec systemd-hydra-db nodetool status")
            if rc == 0:
                un_count = 0
                for line in out.splitlines():
                    if line.strip().startswith("UN"):
                        un_count += 1
                print(f"Gossip health check {i+1}/30: found {un_count}/{len(ips)} nodes in UN state.")
                if un_count >= len(ips):
                    gossip_healthy = True
                    break
            time.sleep(5)
            
        if not gossip_healthy:
            print("[ERROR] ScyllaDB Gossip ring failed to stabilize. nodetool status output:")
            rc, out, _ = run_remote_spark(ips[0], "podman exec systemd-hydra-db nodetool status")
            print(out)
            sys.exit(1)

        print("Verifying Spectrum Web UI reachability on port 8443...")
        spectrum_healthy = True
        for ip in ips:
            reached = False
            for _ in range(20):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 8443")
                if rc == 0 and "8443" in out:
                    reached = True
                    break
                time.sleep(2)
            if not reached:
                print(f"[ERROR] Spectrum UI is unreachable on {ip}:8443.")
                spectrum_healthy = False
            else:
                print(f"[{ip}] Spectrum API/UI is responsive on port 8443.")

        if not spectrum_healthy:
            sys.exit(1)

        print("\n==========================================================")
        print("      HCI Cluster Creation Successful & Verified!         ")
        print("==========================================================")

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
            run_checked_cmd(ip, "systemctl start zookeeper")
            
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
            rc_state, _, _ = run_checked_cmd(ip, "podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 set /cluster_state started || podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 create /cluster_state started")
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
            run_checked_cmd(ip, "systemctl start hydra-db")
            
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

        # 4.5 Start Daruk Query Proxy
        for ip in ips:
            print(f"[{ip}] Starting Daruk ScyllaDB query proxy...")
            run_checked_cmd(ip, "systemctl start daruk")

        print("Waiting for Daruk query proxy to listen on port 9043 on all nodes...")
        for ip in ips:
            daruk_ready = False
            for _ in range(30):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9043")
                if rc == 0 and "9043" in out:
                    daruk_ready = True
                    break
                time.sleep(1)
            if not daruk_ready:
                print(f"[ERROR] Daruk query proxy failed to listen on port 9043 on {ip}")
                sys.exit(1)
        print("Daruk query proxy is ready on all nodes.")

        # 5. Start Aether Storage Service
        print("\n--- Phase 3: Starting Aether Storage Service ---")
        for ip in ips:
            print(f"[{ip}] Starting aether systemd service...")
            run_checked_cmd(ip, "systemctl start aether")
            
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
                
        # Mount volumes depending on storage engine
        dfs_engine = get_dfs_engine()
        if dfs_engine == "linstor":
            for ip in ips:
                print(f"[{ip}] Mounting DRBD volumes on host...")
                mount_cmd = (
                    "mkdir -p /var/lib/hci/aether/volumes/default-vm-container && "
                    "mountpoint -q /var/lib/hci/aether/volumes/default-vm-container || "
                    "mount -t xfs /dev/drbd/by-res/default-vm-container/0 /var/lib/hci/aether/volumes/default-vm-container"
                )
                run_checked_cmd(ip, mount_cmd)
                    
                mount_img_cmd = (
                    "mkdir -p /var/lib/hci/aether/volumes/default-image-container && "
                    "mountpoint -q /var/lib/hci/aether/volumes/default-image-container || "
                    "mount -t xfs /dev/drbd/by-res/default-image-container/0 /var/lib/hci/aether/volumes/default-image-container"
                )
                run_checked_cmd(ip, mount_img_cmd)
        else:
            # Mount GlusterFS volumes
            for ip in ips:
                print(f"[{ip}] Mounting GlusterFS volumes inside container...")
                mount_cmd = (
                    "podman exec systemd-aether mkdir -p /var/lib/hci/aether/volumes/default-vm-container && "
                    "podman exec systemd-aether findmnt /var/lib/hci/aether/volumes/default-vm-container || "
                    "podman exec systemd-aether mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-vm-container /var/lib/hci/aether/volumes/default-vm-container"
                )
                run_checked_cmd(ip, mount_cmd)
                    
                mount_img_cmd = (
                    "podman exec systemd-aether mkdir -p /var/lib/hci/aether/volumes/default-image-container && "
                    "podman exec systemd-aether findmnt /var/lib/hci/aether/volumes/default-image-container || "
                    "podman exec systemd-aether mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-image-container /var/lib/hci/aether/volumes/default-image-container"
                )
                run_checked_cmd(ip, mount_img_cmd)

        # 6. Start remaining services
        print("\n--- Phase 4: Starting Core Workload & Coordination Services ---")
        services = ["spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha"]
        if check_urbosa_enabled():
            services.append("urbosa")
        service_ports = {
            "spectrum": 8443,
            "vali": 9095,
            "catalyst": 9091
        }
        
        for svc in services:
            for ip in ips:
                print(f"[{ip}] Starting systemd service: {svc}...")
                run_checked_cmd(ip, f"systemctl start {svc}")
                
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
        dfs_engine = get_dfs_engine()
        for ip in get_cluster_ips():
            print(f"[{ip}] Unmounting default volume containers...")
            if dfs_engine == "linstor":
                run_remote_spark(ip, "umount -l /var/lib/hci/aether/volumes/default-vm-container || true")
                run_remote_spark(ip, "umount -l /var/lib/hci/aether/volumes/default-image-container || true")
                run_remote_spark(ip, "drbdadm down all || true")
            else:
                run_remote_spark(ip, "podman exec systemd-aether umount -l /var/lib/hci/aether/volumes/default-vm-container || true")
                run_remote_spark(ip, "podman exec systemd-aether umount -l /var/lib/hci/aether/volumes/default-image-container || true")
            
        # 4. Stop systemd services sequentially
        print("\n--- Step 4: Stopping systemd services sequentially ---")
        services = ["spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha", "linstor-controller", "aether", "daruk", "hydra-db", "zookeeper"]
        if check_urbosa_enabled():
            services.insert(services.index("aether"), "urbosa")
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
            ips = ["127.0.0.1"]

        payload = {"servers": ips}
        rc, res = make_request("/api/v1/cluster/destroy", method="POST", payload=payload)
        if rc == 0:
            print("\n==========================================================")
            print("      HCI Cluster Destroyed & Cleaned Successfully!        ")
            print("==========================================================")
        else:
            print(f"[ERROR] Destroy failed: {res.get('error')}")
            sys.exit(1)

if __name__ == "__main__":
    main()
