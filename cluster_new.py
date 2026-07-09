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

def get_witness_ip():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            for h in cdata.get("hosts", []):
                if h.get("is_witness"):
                    return h["ip"]
    except Exception:
        pass
    return None

def get_dfs_engine():
    return "linstor"


def run_remote_spark(ip, command):
    cert_paths = [
        ("C:/Users/AuraFlight/.hci_temp_certs/ca.crt", "C:/Users/AuraFlight/.hci_temp_certs/client.crt", "C:/Users/AuraFlight/.hci_temp_certs/client.key"),
        ("/root/.certs/ca.crt", "/root/.certs/client.crt", "/root/.certs/client.key")
    ]
    ca_path, cert_path, key_path = None, None, None
    for ca, cert, key in cert_paths:
        if os.path.exists(ca) and os.path.exists(cert) and os.path.exists(key):
            ca_path, cert_path, key_path = ca, cert, key
            break
            
    context = ssl._create_unverified_context()
    if cert_path and key_path:
        context.load_cert_chain(certfile=cert_path, keyfile=key_path)
    
    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=120) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)


class UdevHelper:
    def __init__(self, ips):
        self.ips = ips
        self.stop_event = threading.Event()
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        if self.thread:
            self.stop_event.set()
            self.thread.join(timeout=5)

    def _run(self):
        while not self.stop_event.is_set():
            for ip in self.ips:
                try:
                    run_remote_spark(ip, "vgscan --mknodes && udevadm trigger")
                except Exception:
                    pass
            # Wait up to 2 seconds, checking the stop_event frequently
            for _ in range(20):
                if self.stop_event.is_set():
                    break
                time.sleep(0.1)


def acquire_cluster_lock(ips):
    print("Acquiring cluster operation lock on all nodes...")
    lock_cmd = "mkdir -p /run/hci && touch /run/hci/cluster_operation.lock"
    run_parallel(ips, lock_cmd)


def release_cluster_lock(ips):
    print("Releasing cluster operation lock on all nodes...")
    unlock_cmd = "rm -f /run/hci/cluster_operation.lock"
    run_parallel(ips, unlock_cmd)



def get_scylla_bootstrap_progress(ip):
    # Fetch recent logs from journalctl related to bootstrap/repair
    cmd = "journalctl -u hydra-db -n 50 | grep -E 'repair|bootstrap|compaction_manager|serving|NORMAL mode' | tail -n 1"
    rc, out, _ = run_remote_spark(ip, cmd)
    if rc == 0 and out.strip():
        line = out.strip()
        if "systemd-hydra-db" in line:
            parts = line.split("systemd-hydra-db", 1)[1]
            if ":" in parts:
                msg = parts.split(":", 1)[1].strip()
                if "]" in msg:
                    msg = msg.split("]", 1)[1].strip()
                return msg
        return line
    return None

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

def run_destroy_flow(ips):
    WITNESS_IP = get_witness_ip()
    non_witness_ips = [ip for ip in ips if ip != WITNESS_IP]

    # 1. Stop and undefine all libvirt VMs (with a timeout to prevent hanging) - skipped on witness
    print("\n--- Phase 1: Stopping & Undefining libvirt VMs ---")
    vm_cleanup_cmd = "timeout 15 sh -c 'for vm in $(virsh list --all --name); do echo \"Forcing VM destroy: $vm\"; virsh destroy $vm || true; virsh undefine $vm --nvram || true; done' || echo 'VM cleanup timed out'"
    for ip in non_witness_ips:
        print(f"[{ip}] Cleaning up virtual machines...")
        rc, out, err = run_remote_spark(ip, vm_cleanup_cmd)
        if out.strip():
            print(f"[{ip}] Log:\n{out}")
        if rc != 0:
            print(f"[{ip}] [WARNING] Failed to clean VMs: {err}")

    # 2. Stop all core HCI services in parallel
    print("\n--- Phase 2: Stopping Core HCI Services ---")
    services_non_witness = ["hylia", "logos", "mipha", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "linstor-controller", "daruk", "hydra-db"]
    services_all = ["aether", "zookeeper"]
    
    svc_non_witness_list = " ".join(services_non_witness)
    svc_all_list = " ".join(services_all)
    
    for ip in non_witness_ips:
        print(f"[{ip}] Stopping core hypervisor and database services (no-block)...")
        rc, out, err = run_remote_spark(ip, f"systemctl stop --no-block {svc_non_witness_list} || true")
        if rc != 0:
            print(f"[{ip}] [WARNING] Failed to stop core services: {err}")
            
    for ip in ips:
        print(f"[{ip}] Stopping zookeeper and storage services (no-block)...")
        rc, out, err = run_remote_spark(ip, f"systemctl stop --no-block {svc_all_list} || true")
        if rc != 0:
            print(f"[{ip}] [WARNING] Failed to stop zookeeper/storage services: {err}")

    # 3. Unmount containers and DRBD mounts - skipped on witness
    print("\n--- Phase 3: Unmounting Storage Volumes ---")
    for ip in non_witness_ips:
        print(f"[{ip}] Unmounting volume paths...")
        rc1, out1, err1 = run_remote_spark(ip, "umount -l /var/lib/hci/aether/volumes/default-vm-container || true")
        if out1.strip() or err1.strip():
            print(f"[{ip}] VM Volume Unmount Output: {out1 or err1}")
        rc2, out2, err2 = run_remote_spark(ip, "umount -l /var/lib/hci/aether/volumes/default-image-container || true")
        if out2.strip() or err2.strip():
            print(f"[{ip}] Image Volume Unmount Output: {out2 or err2}")

    # 4. Bring down DRBD resources, then verify they're actually gone. A best-effort teardown
    # under a hard `timeout` can leave a resource half torn-down (device still held), which
    # silently poisons the LVM wipe in Phase 5 with residue that a later `create` collides
    # with as "Device or resource busy" on lvcreate.
    print("\n--- Phase 4: Bringing down DRBD Resources ---")
    drbd_down_cmd = (
        "timeout 20 sh -c \""
        "drbdsetup status | grep -v '^[[:space:]]' | grep -v '^#' | while read -r line; do "
        "  res=\\$(echo \\\"\\$line\\\" | awk '{print \\$1}'); "
        "  if [ ! -z \\\"\\$res\\\" ]; then "
        "    echo \\\"Bringing down DRBD resource \\$res...\\\"; "
        "    drbdsetup down \\\"\\$res\\\" || true; "
        "  fi; "
        "done\" || echo 'DRBD down timed out'"
    )
    for ip in ips:
        print(f"[{ip}] Stopping DRBD replication...")
        rc, out, err = run_remote_spark(ip, drbd_down_cmd)
        if out.strip():
            print(f"[{ip}] Log:\n{out}")
        if rc != 0:
            print(f"[{ip}] [WARNING] Failed to stop DRBD: {err}")

        out_chk = ""
        for attempt in range(10):
            rc_chk, out_chk, _ = run_remote_spark(ip, "ls /dev/drbd[0-9]* 2>/dev/null || true")
            if not out_chk.strip():
                break
            print(f"[{ip}] DRBD devices still present ({out_chk.strip()}), forcing down (attempt {attempt + 1}/10)...")
            run_remote_spark(ip, "drbdsetup down all || true")
            time.sleep(2)
        else:
            print(f"[{ip}] [WARNING] DRBD devices still present after forced teardown: {out_chk.strip()}")

    # 5. Wipe LVM vg/thin-pool and disk signatures dynamically - skipped on witness.
    # Retries with verification because device release after a DRBD teardown can lag by a
    # few seconds even once drbdsetup reports success, and a single-shot `|| true` chain
    # would otherwise silently leave stale device-mapper entries behind.
    print("\n--- Phase 5: Wiping LVM Pools & Disk Signatures ---")
    lvm_wipe_cmd = (
        "lvchange -an -f /dev/vg_aether/* 2>/dev/null || true; "
        "lvremove -y -f vg_aether 2>/dev/null || true; "
        "vgremove -y -f vg_aether 2>/dev/null || true; "
        "rm -rf /dev/vg_aether || true; "
        "dmsetup ls 2>/dev/null | grep -Ei 'vg_aether|linstor' | awk '{print $1}' | while read -r dm; do dmsetup remove -f \"$dm\" || true; done"
    )
    for ip in non_witness_ips:
        print(f"[{ip}] Removing LVM thin pool 'thin_pool_aether' and VG 'vg_aether'...")
        out_dm = ""
        for attempt in range(5):
            rc, out, err = run_remote_spark(ip, lvm_wipe_cmd)
            if out.strip():
                print(f"[{ip}] LVM VG removal log:\n{out}")
            if rc != 0:
                print(f"[{ip}] [WARNING] LVM VG removal failed: {err}")
            rc_dm, out_dm, _ = run_remote_spark(ip, "dmsetup ls 2>/dev/null | grep -Ei 'vg_aether|linstor' || true")
            if not out_dm.strip():
                break
            print(f"[{ip}] Residual device-mapper entries found ({out_dm.strip()}), retrying wipe (attempt {attempt + 1}/5)...")
            time.sleep(2)
        else:
            print(f"[{ip}] [ERROR] Could not fully clear stale LVM/device-mapper state: {out_dm.strip()}")
            print(f"[{ip}] [ERROR] A subsequent 'create' would fail on lvcreate with 'Device or resource busy'. "
                  f"Aborting now instead of wasting a full create attempt.")
            print(f"[{ip}] [ERROR] Manual recovery: SSH in and run `dmsetup ls | grep -Ei 'vg_aether|linstor'` "
                  f"to see what's still held, force-remove each with `dmsetup remove -f <name>`, "
                  f"or reboot the node if the device stays wedged.")
            sys.exit(1)

    wipe_devices_script = """
import subprocess, json, sys, os
devs = []

res_pvs = subprocess.run("pvs --noheadings -o pv_name,vg_name", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
if res_pvs.returncode == 0:
    for line in res_pvs.stdout.decode().splitlines():
        parts = line.split()
        if len(parts) >= 1:
            pv = parts[0].strip()
            vg = parts[1].strip() if len(parts) >= 2 else ""
            if vg in ["vg_aether", ""]:
                if pv not in devs:
                    devs.append(pv)

res_lsblk = subprocess.run("lsblk -b -d -n -o NAME,SIZE,TYPE,ROTA", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
if res_lsblk.returncode == 0:
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
                if dev_path not in devs:
                    devs.append(dev_path)

if "/dev/sdb" not in devs:
    devs.append("/dev/sdb")

print("Devices identified for signature wiping:", devs)
for dev in devs:
    subprocess.run("pvremove -y " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if os.path.exists("/etc/lvm/devices/system.devices"):
        dev_name = dev.split("/")[-1]
        subprocess.run("sed -i '/" + dev_name + "/d' /etc/lvm/devices/system.devices", shell=True)
    subprocess.run("wipefs -a " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    subprocess.run("dd if=/dev/zero of=" + dev + " bs=1M count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res_sz = subprocess.run("blockdev --getsize64 " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res_sz.returncode == 0:
        try:
            size_bytes = int(res_sz.stdout.decode().strip())
            seek_val = (size_bytes // 1048576) - 1024
            if seek_val > 0:
                subprocess.run("dd if=/dev/zero of=" + dev + " bs=1M seek=" + str(seek_val) + " count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except Exception as e:
            print("Failed to zero end of dev " + dev + ": " + str(e))
"""
    wipe_script_b64 = base64.b64encode(wipe_devices_script.strip().encode()).decode()
    cmd_wipe = f"python3 -c \"import base64; exec(base64.b64decode('{wipe_script_b64}').decode())\""
    
    for ip in non_witness_ips:
        print(f"[{ip}] Running physical disk signature wipe & zeroing...")
        rc_pv, out_pv, err_pv = run_remote_spark(ip, cmd_wipe)
        if out_pv.strip():
            print(f"[{ip}] Wipe log:\n{out_pv}")
        if rc_pv != 0:
            print(f"[{ip}] [WARNING] Wipe execution failed: {err_pv}")

    # 6. Run clean-up script (removes files, folders, fstab mappings)
    print("\n--- Phase 6: Wiping Storage Directories & Containers ---")
    wipe_script = """
import subprocess
import os
import sys

def run_with_timeout(cmd, timeout=15):
    print(f"Running command: {cmd}", flush=True)
    try:
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        print(f"Status: {res.returncode}", flush=True)
        if res.stdout:
            print(res.stdout.decode(errors='ignore').strip(), flush=True)
        if res.stderr:
            print(res.stderr.decode(errors='ignore').strip(), flush=True)
        return res.returncode
    except subprocess.TimeoutExpired:
        print(f"Command timed out after {timeout} seconds", flush=True)
        return -1

print("--- Running local wipe script ---", flush=True)
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
except Exception as e:
    print(f"Error reading fstab: {e}", flush=True)

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
    print(f"Wiping mount point {mount} on device {real_dev}...", flush=True)
    run_with_timeout(f"umount -l {mount}", timeout=10)
    run_with_timeout(f"sed -i '\\\\|{mount}|d' /etc/fstab", timeout=5)
    run_with_timeout(f"wipefs -a {real_dev}", timeout=10)
    run_with_timeout(f"rm -rf {mount}", timeout=10)

print("Unmounting Linstor Controller HA database volume...", flush=True)
run_with_timeout("umount -l /var/lib/linstor || true", timeout=10)
print("Bringing down DRBD...", flush=True)
run_with_timeout("drbdadm down all", timeout=15)

print("Removing system containers...", flush=True)
run_with_timeout("podman rm -f systemd-hydra-db systemd-zookeeper systemd-aether systemd-spectrum systemd-linstor-satellite systemd-linstor-controller || true", timeout=15)

print("Removing storage directories...", flush=True)
run_with_timeout("rm -rf /var/lib/hci/zookeeper/data /var/lib/hci/zookeeper/log /var/lib/hci/hydra/data /var/lib/hci/aether/data /var/lib/hci/aether/volumes /var/lib/hci/aether/images /var/lib/hci/aether/nvram /run/hci/*", timeout=120)
run_with_timeout("rm -rf /etc/hci/odin /etc/hci/spectrum /etc/hci/cluster.json /var/lib/linstor /etc/linstor", timeout=30)
print("--- Local wipe completed ---", flush=True)
"""
    wipe_b64 = base64.b64encode(wipe_script.encode()).decode()
    cmd_wipe = f"python3 -c \"import base64; exec(base64.b64decode('{wipe_b64}').decode())\""
    for ip in ips:
        print(f"[{ip}] Wiping local filesystem data and system containers...")
        rc, out, err = run_remote_spark(ip, cmd_wipe)
        if out.strip():
            print(f"[{ip}] Log:\n{out}")
        if rc != 0:
            print(f"[{ip}] [WARNING] Cleanup failed: {err}")

    # 7. Restart spark-daemon asynchronously on all hosts to complete destroy
    print("\n--- Phase 7: Restarting spark-daemon Services ---")
    for ip in ips:
        print(f"[{ip}] Restarting spark-daemon...")
        rc, out, err = run_remote_spark(ip, "(sleep 1 && systemctl restart spark-daemon) >/dev/null 2>&1 < /dev/null &")
        if rc != 0:
            print(f"[{ip}] [WARNING] Failed to launch background spark-daemon restart: {err or out}")

    print("\n==========================================================")
    print("      HCI Cluster Destroyed & Cleaned Successfully!        ")
    print("==========================================================")

def main():
    parser = argparse.ArgumentParser(description="HCI Cluster Management Utility")
    parser.add_argument("-s", "--servers", required=False, help="Comma-separated list of host IPs")
    parser.add_argument("-r", "--redundancy_factor", type=int, default=None, help="Fault Tolerance to Tolerate (FTT) / Redundancy Factor (e.g. 0, 1, or 2)")
    parser.add_argument("-v", "--vip", required=False, help="Floating Cluster Virtual IP (VIP)")
    parser.add_argument("--verbose", action="store_true", help="Print verbose status information")
    parser.add_argument("--wipe", action="store_true", help="Automatically wipe/destroy existing cluster configuration and data before creating")
    parser.add_argument("--witness", required=False, help="IP of the witness node (lightweight quorum tie-breaker for 2-node clusters)")
    parser.add_argument("--password", required=False, help="Root password for initial SSH key bootstrap (prompted if not set and keys not yet seeded)")
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

        # Witness node is only used in 2-node clusters as a lightweight quorum tie-breaker.
        # It must be explicitly passed via --witness or pre-marked in cluster.json (is_witness=true).
        # A 3-node cluster has full quorum — all 3 nodes are full hypervisors, no witness needed.
        WITNESS_IP = args.witness if args.witness else get_witness_ip()

        if WITNESS_IP and WITNESS_IP not in ips:
            parser.error(f"--witness IP {WITNESS_IP} is not present in the server list (-s/--servers). "
                         f"The witness node must be included among the hosts being clustered.")

        if len(ips) == 2 and not WITNESS_IP:
            parser.error("2-node clusters cannot achieve ZooKeeper quorum on their own and require a "
                         "witness node to tie-break split-brain scenarios. Pass --witness <ip> "
                         "(the witness IP must also appear in -s/--servers).")

        # Many operations below (Linstor Controller, hydra-db/ScyllaDB, mcli, and other
        # non-witness-only services) are driven off ips[0] as "the leader node". The witness
        # never runs those services, so it must never land at index 0 — pin it to index 2
        # (still within the first-3 ZooKeeper voter slots) regardless of the order the caller
        # passed it in via -s/--servers.
        if WITNESS_IP:
            ips.remove(WITNESS_IP)
            ips.insert(min(2, len(ips)), WITNESS_IP)

        non_witness_ips = [ip for ip in ips if ip != WITNESS_IP]

        rf = args.redundancy_factor if args.redundancy_factor is not None else 1
        if len(ips) == 1:
            if rf > 0:
                print(f"[WARNING] Single-node cluster detected. Forcing redundancy factor (FTT) from {rf} to 0 (no replication).")
            rf = 0
        vip = args.vip if args.vip else ""

        acquire_cluster_lock(ips)
        import atexit
        atexit.register(release_cluster_lock, ips)

        if args.wipe:
            print("[INFO] --wipe flag specified. Purging existing cluster state first...")
            run_destroy_flow(ips)
            print("[INFO] Waiting for spark-daemons to stabilize after restart...")
            time.sleep(5)
            # Verify connectivity before proceeding
            for ip in ips:
                print(f"[{ip}] Waiting for spark-daemon to come back online...")
                for _ in range(15):
                    rc, stdout, _ = run_remote_spark(ip, "echo 'online'")
                    if rc == 0 and "online" in stdout.lower():
                        break
                    time.sleep(1)
                else:
                    print(f"[ERROR] spark-daemon on {ip} did not return online after wipe restart.")
                    sys.exit(1)

        print("==========================================================")
        print(f"   Creating HCI Cluster (Redundancy Factor/FTT={rf})  ")
        print("==========================================================")

        # Phase 0: SSH Key Bootstrap & Certificate Seeding
        # Two-sub-phase approach:
        #   A) SSH key seeding via password (only if keys not yet present) — uses paramiko if
        #      available (external laptop), otherwise assumes provision.py already handled it.
        #   B) mTLS cert generation & distribution via native subprocess SSH — works from
        #      any machine (laptop or cluster node) since it bypasses spark-daemon entirely.
        #      Provision.py seeds id_rsa_hci to all nodes, so key-based SSH is always available.
        print("\n--- Phase 0: SSH Key Bootstrap & Certificate Seeding ---")

        priv_key_path = os.path.expanduser("~/.ssh/id_rsa_hci")
        ssh_opts = f"-i {priv_key_path} -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes"

        def _ssh(ip, cmd):
            """Run cmd on ip via native SSH. Returns (rc, stdout, stderr)."""
            import subprocess as _sp
            r = _sp.run(
                f"ssh {ssh_opts} root@{ip} {_sp.list2cmdline([cmd])}",
                shell=True, capture_output=True, text=True
            )
            return r.returncode, r.stdout.strip(), r.stderr.strip()

        def _scp_put(ip, local_path, remote_path):
            import subprocess as _sp
            r = _sp.run(
                f"scp -i {priv_key_path} -o StrictHostKeyChecking=no {local_path} root@{ip}:{remote_path}",
                shell=True, capture_output=True, text=True
            )
            return r.returncode

        # --- Phase 0A: Password-based SSH key seeding (external machine only) ---
        try:
            import paramiko as _paramiko
        except ImportError:
            _paramiko = None

        # Only attempt password bootstrap if we can't reach nodes with key auth yet
        _needs_pw_bootstrap = False
        if os.path.exists(priv_key_path):
            for _ip in ips:
                rc_key, _, _ = _ssh(_ip, "echo ok")
                if rc_key != 0:
                    _needs_pw_bootstrap = True
                    break
        else:
            _needs_pw_bootstrap = True

        if _needs_pw_bootstrap:
            if _paramiko is None:
                print("[ERROR] Cannot reach nodes via SSH key and paramiko is not installed.")
                print("[ERROR] Run: pip install paramiko  — or run 'provision.py' first to seed SSH keys.")
                sys.exit(1)

            ssh_password = args.password or os.environ.get("HELIOS_PASSWORD")
            if not ssh_password:
                import getpass
                try:
                    ssh_password = getpass.getpass("Enter root password for SSH key bootstrap: ").strip()
                except Exception:
                    ssh_password = ""
            if not ssh_password:
                print("[ERROR] --password is required for first-time SSH key bootstrap.")
                sys.exit(1)

            # Generate local key pair if missing
            if not os.path.exists(priv_key_path):
                print(f"[*] Generating SSH key pair at {priv_key_path}...")
                os.makedirs(os.path.dirname(priv_key_path), exist_ok=True)
                new_key = _paramiko.RSAKey.generate(2048)
                new_key.write_private_key_file(priv_key_path)
                with open(priv_key_path + ".pub", "w") as _f:
                    _f.write(f"ssh-rsa {new_key.get_base64()} root@valkyrie\n")

            with open(priv_key_path + ".pub") as _f:
                pub_key_content = _f.read().strip()
            with open(priv_key_path) as _kf:
                priv_key_str = _kf.read()

            print("[*] Seeding SSH public key to all nodes via password auth...")
            for _ip in ips:
                try:
                    _c = _paramiko.SSHClient()
                    _c.set_missing_host_key_policy(_paramiko.AutoAddPolicy())
                    _c.connect(_ip, username="root", password=ssh_password, timeout=15)
                    _stdin, _sout, _ = _c.exec_command(
                        f"mkdir -p /root/.ssh && chmod 700 /root/.ssh && "
                        f"grep -qF '{pub_key_content}' /root/.ssh/authorized_keys 2>/dev/null || "
                        f"echo '{pub_key_content}' >> /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys"
                    )
                    _sout.channel.recv_exit_status()
                    _sftp = _c.open_sftp()
                    with _sftp.file("/root/.ssh/id_rsa", "w") as _rf:
                        _rf.write(priv_key_str)
                    with _sftp.file("/root/.ssh/id_rsa.pub", "w") as _rf:
                        _rf.write(pub_key_content + "\n")
                    _sftp.chmod("/root/.ssh/id_rsa", 0o600)
                    _sftp.close()
                    _c.close()
                    print(f"  [{_ip}] SSH key seeded.")
                except Exception as _e:
                    print(f"[ERROR] Failed to seed SSH key on {_ip}: {_e}")
                    sys.exit(1)
        else:
            print("[Phase 0A] SSH key access confirmed on all nodes.")

        # --- Phase 0B: mTLS cert generation & distribution via native subprocess SSH ---
        # Runs entirely via ssh/scp subprocess — no spark-daemon dependency, no paramiko needed.
        print("\n[Phase 0B] Generating and distributing mTLS & WebUI SSL certificates...")
        rc_cert_check, _, _ = _ssh(ips[0], "test -f /etc/hci/spark/certs/node.crt")
        if rc_cert_check != 0 or args.wipe:
            import subprocess as _sp
            seed_ip_list = " ".join(ips)
            cert_gen_sh = f"""#!/bin/bash
set -e
mkdir -p /var/lib/hci/certs_staging && chmod 700 /var/lib/hci/certs_staging
cd /var/lib/hci/certs_staging
openssl genrsa -out ca.key 2048
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt -subj "/CN=HCI-Root-CA"
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr -subj "/CN=HCI-Client"
openssl x509 -req -days 3650 -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt
for ip in {seed_ip_list}; do
  printf '[req]\\ndistinguished_name = req_distinguished_name\\nreq_extensions = v3_req\\nprompt = no\\n[req_distinguished_name]\\nCN = '"$ip"'\\n[v3_req]\\nsubjectAltName = IP:'"$ip"'\\n' > "node-$ip.cnf"
  openssl genrsa -out "node-$ip.key" 2048
  openssl req -new -key "node-$ip.key" -out "node-$ip.csr" -config "node-$ip.cnf"
  openssl x509 -req -days 3650 -in "node-$ip.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -out "node-$ip.crt" -extensions v3_req -extfile "node-$ip.cnf"
done
mkdir -p /etc/hci/spectrum/certs
openssl req -x509 -nodes -newkey rsa:2048 -keyout /etc/hci/spectrum/certs/server.key -out /etc/hci/spectrum/certs/server.crt -days 3650 -subj '/CN=Spectrum'
chmod 600 *.key /etc/hci/spectrum/certs/server.key
echo CERTS_OK
"""
            cert_sh_b64 = base64.b64encode(cert_gen_sh.encode()).decode()
            print(f"  [{ips[0]}] Generating CA, client, node, and SSL certificates...")
            rc_gen = _sp.run(
                f"ssh {ssh_opts} root@{ips[0]} \"echo {cert_sh_b64} | base64 -d | bash\"",
                shell=True, capture_output=True, text=True
            )
            if rc_gen.returncode != 0 or "CERTS_OK" not in rc_gen.stdout:
                print(f"[ERROR] Certificate generation failed on {ips[0]}:\n{rc_gen.stderr}")
                sys.exit(1)
            print(f"  [{ips[0]}] Certificates generated.")

            # Collect SSH known_hosts entries
            import hashlib as _hashlib
            _scan_targets = []
            for _ip in ips:
                _scan_targets.append(_ip)
                _scan_targets.append(f"Valkyrie-{_hashlib.md5(_ip.encode()).hexdigest()[:6].upper()}")
            rc_kh = _sp.run(
                f"ssh {ssh_opts} root@{ips[0]} \"ssh-keyscan -H {' '.join(_scan_targets)} 2>/dev/null\"",
                shell=True, capture_output=True, text=True
            )
            known_hosts_content = rc_kh.stdout

            def _distribute_certs_native(ip):
                is_wit = (ip == WITNESS_IP)
                # Read certs for this node from node 1
                _, node_crt, _ = _ssh(ips[0], f"cat /var/lib/hci/certs_staging/node-{ip}.crt")
                _, node_key, _ = _ssh(ips[0], f"cat /var/lib/hci/certs_staging/node-{ip}.key")
                _, ca_crt, _ = _ssh(ips[0], "cat /var/lib/hci/certs_staging/ca.crt")
                _, client_crt, _ = _ssh(ips[0], "cat /var/lib/hci/certs_staging/client.crt")
                _, client_key, _ = _ssh(ips[0], "cat /var/lib/hci/certs_staging/client.key")

                _ssh(ip, "mkdir -p /root/.certs /etc/hci/spark/certs /root/.ssh")
                for _path, _content in [
                    ("/root/.certs/ca.crt", ca_crt),
                    ("/root/.certs/client.crt", client_crt),
                    ("/root/.certs/client.key", client_key),
                    ("/etc/hci/spark/certs/ca.crt", ca_crt),
                    ("/etc/hci/spark/certs/node.crt", node_crt),
                    ("/etc/hci/spark/certs/node.key", node_key),
                    ("/root/.ssh/known_hosts", known_hosts_content),
                ]:
                    _b64 = base64.b64encode(_content.encode()).decode()
                    _ssh(ip, f"echo {_b64} | base64 -d > {_path}")
                _ssh(ip, "chmod 600 /root/.certs/client.key /etc/hci/spark/certs/node.key /root/.ssh/known_hosts")

                if not is_wit:
                    _ssh(ip, "mkdir -p /etc/hci/spectrum/certs")
                    _, ssl_crt, _ = _ssh(ips[0], "cat /etc/hci/spectrum/certs/server.crt")
                    _, ssl_key, _ = _ssh(ips[0], "cat /etc/hci/spectrum/certs/server.key")
                    for _path, _content in [
                        ("/etc/hci/spectrum/certs/server.crt", ssl_crt),
                        ("/etc/hci/spectrum/certs/server.key", ssl_key),
                    ]:
                        _b64 = base64.b64encode(_content.encode()).decode()
                        _ssh(ip, f"echo {_b64} | base64 -d > {_path}")
                    _ssh(ip, "chmod 600 /etc/hci/spectrum/certs/server.key")

                _ssh(ip, "systemctl restart spark-daemon || true")
                print(f"  [{ip}] Certificates distributed, spark-daemon restarted.")

            cert_threads = []
            for _ip in ips:
                _t = threading.Thread(target=_distribute_certs_native, args=(_ip,))
                cert_threads.append(_t)
                _t.start()
            for _t in cert_threads:
                _t.join()

            print("[Phase 0B] Waiting for spark-daemons to come up with new certificates...")
            time.sleep(5)
        else:
            print("[Phase 0B] Certificates already present. Skipping (use --wipe to force regeneration).")

        # 1. Connectivity & Pre-checks
        print("\n--- Phase 1: Connectivity & Pre-checks ---")
        for ip in ips:
            print(f"[{ip}] Testing connectivity...")
            rc, stdout, stderr = run_remote_spark(ip, "echo 'online'")
            if rc != 0 or "online" not in stdout.lower():
                print(f"[ERROR] Could not connect to spark-daemon on {ip}: {stderr}")
                sys.exit(1)
            print(f"[{ip}] spark-daemon is online.")

            # Fail fast if an existing cluster configuration file is present
            rc_file, _, _ = run_remote_spark(ip, "test -f /etc/hci/cluster.json")
            if rc_file == 0:
                print(f"[ERROR] Existing cluster configuration detected on {ip} at /etc/hci/cluster.json.")
                print("[ERROR] To redeploy or change configuration, you must clear the existing state by running 'cluster destroy' first.")
                sys.exit(1)
            
            # Check port conflicts
            print(f"[{ip}] Checking port conflicts...")
            rc, stdout, _ = run_remote_spark(ip, "ss -tlnp")
            if rc == 0:
                for port in ["7000", "3370"]:
                    if port in stdout:
                        print(f"[WARNING] Port {port} is already in use on {ip}. This may cause conflicts.")

            # Validate Secure Boot and ELRepo module signing key
            rc_sb, sb_out, _ = run_remote_spark(ip, "mokutil --is-sb-enabled")
            if rc_sb == 0 and "secureboot enabled" in sb_out.lower():
                rc_key, _, _ = run_remote_spark(ip, "mokutil --test-key /etc/pki/elrepo/SECURE-BOOT-KEY-elrepo.org.der")
                if rc_key != 0:
                    print(f"[ERROR] Secure Boot is enabled on host {ip} and the ELRepo Secure Boot key is not enrolled.")
                    print(f"[ERROR] Unsigned kernel modules like DRBD will fail to load under Secure Boot.")
                    print(f"[ERROR] Please disable Secure Boot in the UEFI/BIOS settings of {ip}, or import the key ('mokutil --import /etc/pki/elrepo/SECURE-BOOT-KEY-elrepo.org.der') and reboot to enroll it.")
                    sys.exit(1)

        # Ensure any running core services are stopped to prevent them from interfering with boot (e.g. Mipha stopping Linstor Controller during creation)
        print("Ensuring any running cluster services are stopped for a clean bootstrap...")
        cleanup_services = ["hylia", "logos", "mipha", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "linstor-controller", "aether", "daruk", "hydra-db", "zookeeper"]
        run_parallel(ips, f"systemctl stop {' '.join(cleanup_services)} || true")

        # 2. Hostname Resolution & Cluster JSON Config
        print("\n--- Phase 2: Hostname Resolution & Cluster Setup ---")
        hosts_info = []
        for idx, ip in enumerate(ips):
            is_wit = (ip == WITNESS_IP)
            print(f"[{ip}] Resolving hostname (is_witness={is_wit})...")
            rc, hostname, _ = run_remote_spark(ip, "hostname")
            hostname = hostname.strip() if rc == 0 else f"node-{idx+1}"
            print(f"[{ip}] Resolved hostname: {hostname}")
            hosts_info.append({
                "node_id": idx + 1,
                "ip": ip,
                "hostname": hostname,
                "is_witness": is_wit
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

        # Write per-node topology configs: ZooKeeper myid, spectrum.env, hydra cassandra.env, storage-pools.json
        print("Writing per-node topology configuration files...")
        zoo_servers_parts = []
        for i, _ip in enumerate(ips, start=1):
            if i > 3:
                zoo_servers_parts.append(f"server.{i}={_ip}:2888:3888:observer;2181")
            else:
                zoo_servers_parts.append(f"server.{i}={_ip}:2888:3888;2181")
        zoo_servers_str = " ".join(zoo_servers_parts)
        seed_ips = ",".join([_ip for _ip in ips if _ip != WITNESS_IP][:3])

        STORAGE_POOLS_JSON = json.dumps({
            "storage_pool_name": "default-pool",
            "dfs_engine": "linstor",
            "thin_pool_name": "thin_pool_aether",
            "vg_name": "vg_aether"
        }, indent=4)

        for idx, ip in enumerate(ips, start=1):
            is_wit = (ip == WITNESS_IP)
            myid_b64 = base64.b64encode(str(idx).encode()).decode()
            run_remote_spark(ip, f"mkdir -p /var/lib/hci/zookeeper/data && echo {myid_b64} | base64 -d > /var/lib/hci/zookeeper/data/myid")
            spectrum_env = f"SPECTRUM_HOST={ip}\nSPECTRUM_PORT=8000\nSPECTRUM_LOG_LEVEL=info\nLOCAL_HYPERVISOR_IP={ip}\nVIP={vip}\n"
            spec_env_b64 = base64.b64encode(spectrum_env.encode()).decode()
            run_remote_spark(ip, f"mkdir -p /etc/hci/spectrum && echo {spec_env_b64} | base64 -d > /etc/hci/spectrum/spectrum.env")
            if not is_wit:
                hydra_env = f"HYDRA_DB_SEEDS={seed_ips}\nHYDRA_DB_LISTEN={ip}\n"
                hydra_env_b64 = base64.b64encode(hydra_env.encode()).decode()
                run_remote_spark(ip, f"mkdir -p /etc/hci/hydra && echo {hydra_env_b64} | base64 -d > /etc/hci/hydra/cassandra.env")
                sp_b64 = base64.b64encode(STORAGE_POOLS_JSON.encode()).decode()
                run_remote_spark(ip, f"mkdir -p /etc/hci/aether && echo {sp_b64} | base64 -d > /etc/hci/aether/storage-pools.json")
            print(f"[{ip}] Topology configs written (myid={idx}, is_witness={is_wit}).")

        # Configure SELinux permanently to Permissive on all nodes to prevent helper command failures
        print("Setting SELinux to Permissive on all nodes...")
        selinux_results = run_parallel(ips, "setenforce 0 || true; sed -i 's/SELINUX=enforcing/SELINUX=permissive/g' /etc/selinux/config || true")
        for ip, (rc, _, err) in selinux_results.items():
            if rc != 0:
                print(f"[WARNING] Failed to configure SELinux on {ip}: {err}")

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
# Zero first 1024MB and last 1024MB of the raw disk to ensure no old DRBD metadata interferes
subprocess.run("dd if=/dev/zero of=" + dev_path + " bs=1M count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
seek_val = (size_bytes // 1048576) - 1024
subprocess.run("dd if=/dev/zero of=" + dev_path + " bs=1M seek=" + str(seek_val) + " count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("pvcreate -y " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("rm -rf /dev/vg_aether", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("vgcreate vg_aether " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("lvcreate -y -l 100%FREE -T vg_aether/thin_pool_aether", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
print(json.dumps({"status": "created", "device": dev_path, "size_bytes": size_bytes}))
"""
        claim_script_b64 = base64.b64encode(disk_claim_script.strip().encode()).decode()
        cmd_claim = f"python3 -c \"import base64; exec(base64.b64decode('{claim_script_b64}').decode())\""
        
        print("Scanning and setting up storage pools on remote hosts in parallel...")
        claim_results = {}
        for ip in ips:
            if ip == WITNESS_IP:
                claim_results[ip] = (0, json.dumps({"status": "witness", "device": "none", "size_bytes": 0}), "")
            else:
                rc, stdout, stderr = run_remote_spark(ip, cmd_claim)
                claim_results[ip] = (rc, stdout, stderr)
        
        host_claimed_disks = {}
        for ip, (rc, stdout, stderr) in claim_results.items():
            if rc == 0:
                try:
                    disk_info = json.loads(stdout.strip())
                    if "error" in disk_info:
                        print(f"[ERROR] Host {ip} disk setup failed: {disk_info['error']}")
                        sys.exit(1)
                    host_claimed_disks[ip] = disk_info
                    if disk_info["status"] == "witness":
                        print(f"[{ip}] Witness host: Skipping physical storage pool setup.")
                    else:
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
        run_parallel_checked(ips, "systemctl restart aether")
        
        # Start Linstor Controller on leader and stop on standby nodes
        print("Starting Linstor Controller on leader node...")
        run_checked_cmd(ips[0], "systemctl start linstor-controller")
        for ip in ips[1:]:
            if ip == WITNESS_IP:
                continue
            print(f"[{ip}] Ensuring standby Linstor Controller is stopped (active-passive)...")
            run_remote_spark(ip, "systemctl stop linstor-controller")
        
        print("Waiting for Linstor Controller to listen on port 3370 on leader node...")
        leader_ip = ips[0]
        controller_ready = False
        for _ in range(30):
            rc, out, _ = run_remote_spark(leader_ip, "ss -tlnp | grep 3370")
            if rc == 0 and "3370" in out:
                controller_ready = True
                break
            time.sleep(1)
        if not controller_ready:
            print(f"[ERROR] Linstor Controller failed to start on port 3370 on {leader_ip}.")
            sys.exit(1)
        print("Linstor Controller is ready on leader node.")

        print("Setting Linstor DRBD port range (7700-7890) to avoid conflicts...")
        run_checked_cmd(leader_ip, "podman exec systemd-linstor-controller linstor controller set-property TcpPortAutoRange 7700-7890", allow_already_exists=True)

        udev_helper = UdevHelper(ips)
        udev_helper.start()
        try:
            print("Creating Linstor node definitions...")
            for h in hosts_info:
                print(f"Creating Linstor node for {h['hostname']} ({h['ip']})...")
                run_checked_cmd(ips[0], f"podman exec systemd-linstor-controller linstor node create {h['hostname']} {h['ip']}", allow_already_exists=True)

            print("Registering Linstor storage pools...")
            for h in hosts_info:
                if h.get("is_witness", False):
                    print(f"[{h['ip']}] Witness host: Skipping physical storage-pool registration.")
                    continue
                print(f"[{h['ip']}] Registering vg_aether/thin_pool_aether...")
                run_checked_cmd(ips[0], f"podman exec systemd-linstor-controller linstor storage-pool create lvmthin {h['hostname']} default-pool vg_aether/thin_pool_aether", allow_already_exists=True)

            # Create Linstor resource definitions (default containers skipped for Linstor engine)
            pass

            # Create linstor-db DRBD volume for database HA
            print("\nCreating linstor-db DRBD resource definition for database HA...")
            run_checked_cmd(ips[0], "podman exec systemd-linstor-controller linstor resource-definition create linstor-db", allow_already_exists=True)
            run_checked_cmd(ips[0], "podman exec systemd-linstor-controller linstor volume-definition create linstor-db 5G", allow_already_exists=True)

            # Set automatic split-brain resolution policy for linstor-db database resource
            run_checked_cmd(ips[0], "podman exec systemd-linstor-controller linstor resource-definition drbd-options --after-sb-0pri discard-zero-changes --after-sb-1pri discard-secondary --after-sb-2pri disconnect linstor-db", allow_already_exists=True)

            print("Deploying replicated database storage volume across all nodes...")
            for h in hosts_info:
                print(f"Creating linstor-db resource on {h['hostname']}...")
                if h.get("is_witness", False):
                    run_checked_cmd(ips[0], f"podman exec systemd-linstor-controller linstor resource create {h['hostname']} linstor-db --diskless", allow_already_exists=True)
                else:
                    run_checked_cmd(ips[0], f"podman exec systemd-linstor-controller linstor resource create {h['hostname']} linstor-db --storage-pool default-pool", allow_already_exists=True)

            print("Waiting for linstor-db DRBD block device to appear on leader...")
            db_drbd_ready = False
            for _ in range(45):
                rc_db, _, _ = run_remote_spark(ips[0], "test -b /dev/drbd/by-res/linstor-db/0")
                if rc_db == 0:
                    db_drbd_ready = True
                    print("linstor-db DRBD block device is ready on Node 1.")
                    break
                time.sleep(1)
            if not db_drbd_ready:
                print("[ERROR] linstor-db DRBD block device did not appear within timeout.")
                sys.exit(1)

            print("Formatting linstor-db block device with XFS...")
            run_checked_cmd(ips[0], "mkfs.xfs -f /dev/drbd/by-res/linstor-db/0")
        finally:
            udev_helper.stop()

        print("Migrating local database to the replicated linstor-db volume...")
        # 1. Stop controller to release database lock
        run_checked_cmd(ips[0], "systemctl stop linstor-controller")
        # 2. Mount DRBD volume to temp directory
        run_checked_cmd(ips[0], "mkdir -p /mnt/linstordb-temp && mount -t xfs /dev/drbd/by-res/linstor-db/0 /mnt/linstordb-temp")
        # 3. Copy files preserving permissions
        run_checked_cmd(ips[0], "cp -a /var/lib/linstor/. /mnt/linstordb-temp/")
        # 4. Unmount temp directory
        run_checked_cmd(ips[0], "umount -f /mnt/linstordb-temp")
        # 5. Clear local directory and mount DRBD volume to /var/lib/linstor
        run_checked_cmd(ips[0], "rm -rf /var/lib/linstor/* && mount -t xfs /dev/drbd/by-res/linstor-db/0 /var/lib/linstor")
        # 6. Restart controller (it is now backed by the DRBD volume!)
        run_checked_cmd(ips[0], "systemctl start linstor-controller")

        # Verify Node 1 controller is back online
        controller_ready = False
        for _ in range(30):
            rc_check, out_check, _ = run_remote_spark(ips[0], "ss -tlnp | grep 3370")
            if rc_check == 0 and "3370" in out_check:
                controller_ready = True
                break
            time.sleep(1)
        if not controller_ready:
            print("[ERROR] Linstor Controller failed to restart on leader after database migration.")
            sys.exit(1)

        print("Cleaning up local database directories and stopping standby nodes...")
        for target_ip in ips[1:]:
            print(f"[{target_ip}] Aligning Linstor/DRBD state...")
            is_target_witness = (target_ip == WITNESS_IP)
            if not is_target_witness:
                run_checked_cmd(target_ip, "systemctl stop linstor-controller")
                run_checked_cmd(target_ip, "umount -l /var/lib/linstor || true")
                run_checked_cmd(target_ip, "rm -rf /var/lib/linstor/*")
            run_checked_cmd(target_ip, "drbdadm secondary linstor-db || true")

        print("Waiting for linstor-db DRBD replication to sync and reach UpToDate/Diskless status cluster-wide...")
        db_synced = False
        for i in range(120): # up to 4 minutes
            rc_stat, out_stat, _ = run_remote_spark(ips[0], "drbdadm status linstor-db")
            if rc_stat == 0:
                out_lower = out_stat.lower()
                if "inconsistent" not in out_lower and "sync" not in out_lower and ("uptodate" in out_lower or "diskless" in out_lower):
                    count_ok = out_lower.count("uptodate") + out_lower.count("diskless")
                    if count_ok >= len(ips):
                        db_synced = True
                        print("linstor-db is fully synchronized and UpToDate/Diskless on all nodes.")
                        break
            time.sleep(2)
        if not db_synced:
            print("[WARNING] linstor-db replication did not fully sync within timeout. Disk status:")
            rc_stat, out_stat, _ = run_remote_spark(ips[0], "drbdadm status linstor-db")
            print(out_stat)

        print("Writing storage pools config and spectrum configuration on all hosts...")
        for ip in ips:
            if ip == WITNESS_IP:
                storage_pool_json = {
                    "storage_pool_name": "default-pool",
                    "dfs_engine": "linstor",
                    "local_disks": [],
                    "storage_containers": []
                }
            else:
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

            controllers_line = ",".join(non_witness_ips)
            client_conf = f"[active]\ncontrollers = {controllers_line}\n"
            client_b64 = base64.b64encode(client_conf.encode('utf-8')).decode('utf-8')
            run_remote_spark(ip, f"mkdir -p /etc/linstor && echo {client_b64} | base64 -d > /etc/linstor/linstor-client.conf")

            seeds = ",".join(ips)
            spectrum_env = f"SPECTRUM_API_PORT=8443\nLOCAL_HYPERVISOR_IP={ip}\nCLUSTER_SEEDS={seeds}"
            env_b64 = base64.b64encode(spectrum_env.encode('utf-8')).decode('utf-8')
            run_remote_spark(ip, f"mkdir -p /etc/hci/spectrum && echo {env_b64} | base64 -d > /etc/hci/spectrum/spectrum.env")

        # Mounting storage volumes on all nodes in parallel (skipped for Linstor engine)
        pass

        # 5. Database Quorum Setup
        print("Creating ZooKeeper, ScyllaDB, and Aether volume directories on all nodes...")
        run_parallel_checked(ips, "mkdir -p /var/lib/hci/zookeeper/data /var/lib/hci/zookeeper/log /var/lib/hci/hydra/data /var/lib/hci/aether/volumes /var/lib/hci/aether/images /var/lib/hci/aether/nvram")
        
        # Copy Daruk proxy script to ScyllaDB volume directory (skipped on witness)
        print("Copying Daruk query proxy script to ScyllaDB volume directory on non-witness nodes...")
        run_parallel_checked(non_witness_ips, "mkdir -p /var/lib/hci/hydra/data && cp /usr/local/bin/daruk.py /var/lib/hci/hydra/data/daruk.py && chmod 644 /var/lib/hci/hydra/data/daruk.py")

        print("Writing dynamic ZooKeeper container configs on all hosts...")
        if len(ips) == 1:
            zoo_servers_env = ""
        else:
            zoo_servers_parts = []
            for i, ip in enumerate(ips, start=1):
                if i > 3:
                    zoo_servers_parts.append(f"server.{i}={ip}:2888:3888:observer;2181")
                else:
                    zoo_servers_parts.append(f"server.{i}={ip}:2888:3888;2181")
            zoo_servers_str = " ".join(zoo_servers_parts)
            zoo_servers_env = f' ZOO_SERVERS="{zoo_servers_str}"'

        for idx, ip in enumerate(ips):
            node_id = idx + 1
            peer_type_env = " ZOO_PEER_TYPE=observer" if node_id > 3 else ""
            zk_quad = (
                "[Unit]\n"
                "Description=ZooKeeper Cluster Consensus Service\n"
                "After=network.target\n\n"
                "[Service]\n"
                "Restart=always\n"
                "CPUWeight=100\n"
                "MemoryMax=512M\n"
                "MemoryHigh=400M\n\n"
                "[Container]\n"
                "Image=docker.io/library/zookeeper:3.9.2\n"
                "Network=host\n"
                "Volume=/var/lib/hci/zookeeper/data:/data:Z\n"
                "Volume=/var/lib/hci/zookeeper/log:/datalog:Z\n"
                f"Environment=ZOO_MY_ID={node_id}{zoo_servers_env}{peer_type_env} ZOO_4LW_COMMANDS_WHITELIST=*\n\n"
                "[Install]\n"
                "WantedBy=multi-user.target\n"
            )
            zk_b64 = base64.b64encode(zk_quad.encode()).decode()
            run_remote_spark(ip, f"mkdir -p /etc/containers/systemd && echo {zk_b64} | base64 -d > /etc/containers/systemd/zookeeper.container && systemctl daemon-reload")

        print("Starting ZooKeeper service in parallel on all hosts...")
        run_parallel_checked(ips, "systemctl restart zookeeper")
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

        def _wait_hydra_db_active(ip):
            for _ in range(40):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active hydra-db")
                if rc == 0 and out.strip() == "active":
                    return
                time.sleep(1)
            print(f"[ERROR] hydra-db failed to start on {ip}")
            sys.exit(1)

        def _wait_hydra_db_cql(ip):
            print(f"[{ip}] Waiting for ScyllaDB to listen on port 9042...")
            last_progress = None
            for i in range(600):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9042")
                if rc == 0 and "9042" in out:
                    return

                # Check and print bootstrap/repair progress every 10 seconds
                if i % 10 == 0:
                    progress = get_scylla_bootstrap_progress(ip)
                    if progress and progress != last_progress:
                        print(f"[{ip}] ScyllaDB Bootstrap Status: {progress}")
                        last_progress = progress
                time.sleep(1)
            print(f"[ERROR] ScyllaDB port 9042 timeout on {ip}")
            sys.exit(1)

        # ScyllaDB uses Raft to establish its internal "group0" management cluster on first
        # boot. Starting every node's hydra-db simultaneously races them all into "Discovering
        # group0..." with none willing to bootstrap it, deadlocking the whole cluster. The
        # leader must fully establish group0 (i.e. be listening on 9042) before any other
        # node starts, so the rest join an already-existing group0 instead of racing to create one.
        print(f"[{leader_ip}] Starting ScyllaDB Database Service on leader node (establishing Raft group0)...")
        run_checked_cmd(leader_ip, "systemctl restart hydra-db")
        _wait_hydra_db_active(leader_ip)
        _wait_hydra_db_cql(leader_ip)

        scylla_follower_ips = [ip for ip in non_witness_ips if ip != leader_ip]
        if scylla_follower_ips:
            print("Starting ScyllaDB Database Service on remaining non-witness nodes in parallel...")
            run_parallel_checked(scylla_follower_ips, "systemctl restart hydra-db")
            for ip in scylla_follower_ips:
                _wait_hydra_db_active(ip)

            print("Waiting for ScyllaDB to listen on port 9042 on remaining non-witness nodes...")
            for ip in scylla_follower_ips:
                _wait_hydra_db_cql(ip)

        print("Starting Daruk query proxy service on non-witness nodes...")
        run_parallel_checked(non_witness_ips, "systemctl restart daruk")
        print("Waiting for Daruk query proxy to listen on port 9043 on non-witness nodes...")
        for ip in non_witness_ips:
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
        print("Daruk query proxy is ready on all non-witness nodes.")

        # 6. Start Workload Services
        print("\n--- Phase 6: Starting Core HCI Services ---")
        services = ["spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha", "agahnim", "slate", "hylia"]
        
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
            print(f"Starting {svc} service in parallel across non-witness nodes...")
            run_parallel_checked(non_witness_ips, f"systemctl restart {svc}")
            for ip in non_witness_ips:
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
        print("Polling ScyllaDB Gossip Status until all active nodes are Up-Normal (UN)...")
        gossip_healthy = False
        for i in range(30):
            rc, out, _ = run_remote_spark(ips[0], "podman exec systemd-hydra-db nodetool status")
            if rc == 0:
                un_count = 0
                for line in out.splitlines():
                    if line.strip().startswith("UN"):
                        un_count += 1
                print(f"Gossip health check {i+1}/30: found {un_count}/{len(non_witness_ips)} nodes in UN state.")
                if un_count >= len(non_witness_ips):
                    gossip_healthy = True
                    break
            time.sleep(5)
            
        if not gossip_healthy:
            print("[ERROR] ScyllaDB Gossip ring failed to stabilize. nodetool status output:")
            rc, out, _ = run_remote_spark(ips[0], "podman exec systemd-hydra-db nodetool status")
            print(out)
            sys.exit(1)

        print("Checking ZooKeeper consensus and node states...")
        zk_healthy = True
        leaders = 0
        followers = 0
        for ip in ips:
            zk_cmd = "python3 -c \"import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1', 2181)); s.sendall(b'stat'); print(s.recv(1024).decode('utf-8', errors='ignore'))\""
            rc_zk, out_zk, _ = run_remote_spark(ip, zk_cmd)
            if rc_zk == 0 and "Mode:" in out_zk:
                mode = "unknown"
                for line in out_zk.splitlines():
                    if line.strip().startswith("Mode:"):
                        mode = line.split(":", 1)[1].strip()
                print(f"  [{ip}] ZooKeeper is active in mode: {mode}")
                if mode == "leader" or mode == "standalone":
                    leaders += 1
                elif mode == "follower":
                    followers += 1
            else:
                print(f"  [{ip}] [ERROR] ZooKeeper consensus check failed: {out_zk}")
                zk_healthy = False
        if not zk_healthy or leaders != 1 or followers != len(ips) - 1:
            print(f"[ERROR] ZooKeeper quorum is not healthy. Leaders: {leaders}, Followers: {followers}")
            sys.exit(1)

        print("Verifying Linstor satellite node connections...")
        linstor_healthy = False
        controllers_str = ",".join(non_witness_ips)
        for i in range(15):
            rc_l, out_l, _ = run_remote_spark(ips[0], f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor node list")
            if rc_l == 0:
                online_count = 0
                for line in out_l.splitlines():
                    if "Online" in line or "ONLINE" in line:
                        online_count += 1
                print(f"Linstor health check {i+1}/15: found {online_count}/{len(ips)} online nodes.")
                if online_count >= len(ips):
                    linstor_healthy = True
                    break
            time.sleep(3)
        if not linstor_healthy:
            print("[ERROR] Linstor satellites did not all reach ONLINE state. Status output:")
            rc_l, out_l, _ = run_remote_spark(ips[0], f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor node list")
            print(out_l)
            sys.exit(1)

        print("Verifying Spectrum Web UI reachability on port 8443 on non-witness nodes...")
        spectrum_healthy = True
        for ip in non_witness_ips:
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

        print("Running diagnostic verification checks using Mimir...")
        rc_m, out_m, _ = run_remote_spark(ips[0], "/usr/local/bin/mcli health_checks run_all")
        if rc_m != 0:
            print(f"[ERROR] Mimir health check execution failed.")
            sys.exit(1)
        fail_count = 0
        for line in out_m.splitlines():
            if "[ FAIL ]" in line:
                fail_count += 1
        if fail_count > 0:
            print(f"[ERROR] Mimir diagnostic checks found {fail_count} failures! Cluster is not healthy.")
            for line in out_m.splitlines():
                if "FAIL" in line:
                    print(line)
            sys.exit(1)
        else:
            print("Mimir diagnostics verified successfully (0 failures detected).")

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
        WITNESS_IP = get_witness_ip()
        non_witness_ips = [ip for ip in ips if ip != WITNESS_IP]
        print(f"Connecting to cluster nodes: {', '.join(ips)}")

        acquire_cluster_lock(ips)
        import atexit
        atexit.register(release_cluster_lock, ips)

        
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

        # Identify nodes in maintenance mode
        maintenance_ips = []
        for ip in ips:
            rc, _, _ = run_remote_spark(ip, "test -f /etc/hci/maintenance.state")
            if rc == 0:
                maintenance_ips.append(ip)
                print(f"[{ip}] Note: Host is currently in maintenance mode.")

        # 2. Start ZooKeeper Service
        print("\n--- Phase 1: Starting ZooKeeper Service ---")
        for ip in ips:
            print(f"[{ip}] Starting ZooKeeper service...")
            run_checked_cmd(ip, "systemctl restart zookeeper")
            
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
            if rc_s == 0 and ("mode: leader" in out_s.lower() or "mode: standalone" in out_s.lower()):
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
        for ip in non_witness_ips:
            print(f"[{ip}] Starting hydra-db systemd service...")
            run_checked_cmd(ip, "systemctl restart hydra-db")
            
        for ip in non_witness_ips:
            print(f"[{ip}] Waiting for hydra-db service to become active...")
            for _ in range(35):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active hydra-db")
                if rc == 0 and out.strip() == "active":
                    break
                time.sleep(1)
            else:
                print(f"[{ip}] ERROR: hydra-db service failed to start.")
                sys.exit(1)
                
        for ip in non_witness_ips:
            print(f"[{ip}] Waiting for ScyllaDB to start listening on port 9042...")
            last_progress = None
            for i in range(300):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9042")
                if rc == 0 and "9042" in out:
                    print(f"[{ip}] ScyllaDB is accepting database connections on port 9042.")
                    break
                
                # Check and print bootstrap/repair progress every 10 seconds
                if i % 10 == 0:
                    progress = get_scylla_bootstrap_progress(ip)
                    if progress and progress != last_progress:
                        print(f"[{ip}] ScyllaDB Bootstrap Status: {progress}")
                        last_progress = progress
                time.sleep(1)
            else:
                print(f"[{ip}] ERROR: ScyllaDB database connection port 9042 timeout.")
                sys.exit(1)

        # 4.5 Start Daruk Query Proxy
        for ip in non_witness_ips:
            print(f"[{ip}] Starting Daruk ScyllaDB query proxy...")
            run_checked_cmd(ip, "systemctl restart daruk")

        print("Waiting for Daruk query proxy to listen on port 9043 on non-witness nodes...")
        for ip in non_witness_ips:
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
        print("Daruk query proxy is ready on all non-witness nodes.")

        # 5. Start Aether Storage Service
        print("\n--- Phase 3: Starting Aether Storage Service ---")
        for ip in ips:
            print(f"[{ip}] Starting aether systemd service...")
            run_checked_cmd(ip, "systemctl restart aether")
            
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
                
        # Mounting storage volumes on all nodes (skipped for Linstor engine)
        pass

        # 6. Start remaining services
        print("\n--- Phase 4: Starting Core Workload & Coordination Services ---")
        services = ["spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha", "agahnim", "slate", "hylia"]
        if check_urbosa_enabled():
            services.append("urbosa")
        service_ports = {
            "spectrum": 8443,
            "vali": 9095,
            "catalyst": 9091,
            "agahnim": 8081,
            "slate": 443
        }
        
        for svc in services:
            for ip in non_witness_ips:
                if ip in maintenance_ips:
                    continue
                print(f"[{ip}] Starting systemd service: {svc}...")
                run_checked_cmd(ip, f"systemctl restart {svc}")
                
        for svc in services:
            for ip in non_witness_ips:
                if ip in maintenance_ips:
                    continue
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
                        
        # 7. Post-Start Health Verification Checks
        print("\n--- Phase 5: Cluster Health Verification ---")
        
        # A. ZooKeeper Consensus Check
        print("Checking ZooKeeper consensus quorum...")
        leaders = 0
        followers = 0
        zk_healthy = True
        for ip in ips:
            zk_cmd = "python3 -c \"import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1', 2181)); s.sendall(b'stat'); print(s.recv(1024).decode('utf-8', errors='ignore'))\""
            rc_zk, out_zk, _ = run_remote_spark(ip, zk_cmd)
            if rc_zk == 0 and "Mode:" in out_zk:
                mode = "unknown"
                for line in out_zk.splitlines():
                    if line.strip().startswith("Mode:"):
                        mode = line.split(":", 1)[1].strip()
                if mode == "leader" or mode == "standalone":
                    leaders += 1
                elif mode == "follower":
                    followers += 1
            else:
                zk_healthy = False
        if not zk_healthy or leaders != 1 or followers != len(ips) - 1:
            print(f"[ERROR] Cluster start verification failed: ZooKeeper quorum is not healthy. Leaders: {leaders}, Followers: {followers}")
            sys.exit(1)
        print("  ZooKeeper quorum is healthy.")

        # B. Wait for Mipha to promote linstor-db and start Linstor Controller on one of the nodes
        print("Waiting for linstor-controller to become active on one of the nodes...")
        controller_active_ip = None
        for _ in range(45):
            for ip in non_witness_ips:
                rc_c, out_c, _ = run_remote_spark(ip, "systemctl is-active linstor-controller")
                if rc_c == 0 and out_c.strip() == "active":
                    rc_p, out_p, _ = run_remote_spark(ip, "ss -tlnp | grep 3370")
                    if rc_p == 0 and "3370" in out_p:
                        controller_active_ip = ip
                        break
            if controller_active_ip:
                break
            time.sleep(1)
            
        if not controller_active_ip:
            print("[ERROR] Cluster start verification failed: Linstor Controller failed to start or become active on any node.")
            sys.exit(1)
            
        print(f"  Linstor Controller is active on {controller_active_ip}.")

        # C. Verify Linstor Satellites are online
        print("Verifying Linstor satellite node connections...")
        linstor_healthy = False
        controllers_str = ",".join(non_witness_ips)
        for i in range(15):
            rc_l, out_l, _ = run_remote_spark(controller_active_ip, f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor node list")
            if rc_l == 0:
                online_count = 0
                for line in out_l.splitlines():
                    if "Online" in line or "ONLINE" in line:
                        online_count += 1
                if online_count >= len(ips):
                    linstor_healthy = True
                    break
            time.sleep(2)
        if not linstor_healthy:
            print("[ERROR] Cluster start verification failed: Linstor satellites are not all online.")
            sys.exit(1)
        print("  All Linstor storage satellites are online.")

        # D. Verify DRBD Volume Replication Status
        print("Verifying DRBD volume replication status on all nodes...")
        drbd_healthy = True
        bad_vols = []
        for ip in ips:
            rc_st, out_st, _ = run_remote_spark(ip, "drbdadm status 2>/dev/null || true")
            if rc_st == 0:
                for line in out_st.splitlines():
                    if "connection:" in line:
                        parts = line.strip().split()
                        conn_state = "unknown"
                        for p in parts:
                            if p.startswith("connection:"):
                                conn_state = p.split(":", 1)[1]
                        if conn_state not in ["Connected", "SyncSource", "SyncTarget", "PausedSyncSource", "PausedSyncTarget", "VerifyS", "VerifyT"]:
                            drbd_healthy = False
                            bad_vols.append(f"{ip}: {line.strip()}")
        if not drbd_healthy:
            print(f"[ERROR] Cluster start verification failed: DRBD volumes are in an unhealthy replication state: {', '.join(bad_vols)}")
            sys.exit(1)
        print("  All DRBD replication rings are connected.")

        # E. Run Mimir diagnostic verification checks
        print("Running diagnostic verification checks using Mimir...")
        rc_m, out_m, _ = run_remote_spark(ips[0], "/usr/local/bin/mcli health_checks run_all")
        if rc_m != 0:
            print(f"[ERROR] Cluster start verification failed: Mimir health check execution failed.")
            sys.exit(1)
        if "[FAIL]" in out_m or "FAIL" in out_m:
            failed_checks = []
            for line in out_m.splitlines():
                if "[FAIL]" in line or "FAIL" in line:
                    failed_checks.append(line.strip())
            print(f"[ERROR] Cluster start verification failed: Mimir diagnostic checks failed:\n" + "\n".join(failed_checks))
            sys.exit(1)
        print("  All Mimir diagnostic checks passed successfully.")

        print("\n==========================================================")
        print("      HCI Cluster Started & Verified Successfully!       ")
        print("==========================================================")

    elif args.command == "stop":
        print("==========================================================")
        print("                 Stopping HCI Cluster                     ")
        print("==========================================================")
        
        ips = get_cluster_ips()
        WITNESS_IP = get_witness_ip()
        non_witness_ips = [ip for ip in ips if ip != WITNESS_IP]
        acquire_cluster_lock(ips)
        import atexit
        atexit.register(release_cluster_lock, ips)

        
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
            
        # 3. Stop workload and HA services in parallel (skipped on witness)
        print("\n--- Step 3: Stopping workload and HA services in parallel across non-witness nodes ---")
        workload_services = ["hylia", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha", "agahnim", "slate"]
        for svc in workload_services:
            print(f"Stopping systemd service '{svc}' in parallel across non-witness nodes...")
            run_parallel(non_witness_ips, f"systemctl stop {svc}")
            
        # 3.5. Wait for DRBD replication sync to finish
        print("\n--- Step 3.5: Ensuring all DRBD volumes finish syncing ---")
        for attempt in range(60): # Wait up to 120 seconds
            syncing = False
            for ip in ips:
                rc, stdout, _ = run_remote_spark(ip, "drbdsetup status --json")
                if rc == 0 and stdout.strip():
                    try:
                        data = json.loads(stdout)
                        for resource in data:
                            for conn in resource.get("connections", []):
                                for dev in conn.get("peer_devices", []):
                                    if dev.get("replication-state") in ("SyncTarget", "SyncSource"):
                                        syncing = True
                                        break
                    except Exception:
                        pass
            if not syncing:
                print("All DRBD resources are fully synced.")
                break
            print("Some DRBD volumes are still syncing, waiting 2 seconds...")
            time.sleep(2)
        else:
            print("Warning: Timeout waiting for DRBD resync to finish. Proceeding with shutdown.")
            
        # 4. Unmount default volumes in parallel (skipped on witness)
        print("\n--- Step 4: Unmounting default volumes in parallel across non-witness nodes ---")
        run_parallel(non_witness_ips, "umount -l /var/lib/hci/aether/volumes/default-vm-container || true")
        run_parallel(non_witness_ips, "umount -l /var/lib/hci/aether/volumes/default-image-container || true")
        run_parallel(non_witness_ips, "umount -l /var/lib/linstor || true")

        # 5. Stop storage and controller services in parallel
        print("\n--- Step 5: Stopping storage services in parallel ---")
        storage_non_witness = ["linstor-controller", "daruk"]
        if check_urbosa_enabled():
            storage_non_witness.insert(0, "urbosa")
        for svc in storage_non_witness:
            print(f"Stopping systemd service '{svc}' in parallel across non-witness nodes...")
            run_parallel(non_witness_ips, f"systemctl stop {svc}")
        print("Stopping systemd service 'aether' in parallel across all nodes...")
        run_parallel(ips, "systemctl stop aether")

        # 6. Bring down DRBD resources in parallel
        print("\n--- Step 6: Bringing down DRBD resources in parallel across all nodes ---")
        run_parallel(ips, "drbdadm down all || true")

        # 7. Stop database and coordination services in parallel
        print("\n--- Step 7: Stopping database and coordination services in parallel ---")
        print("Stopping systemd service 'hydra-db' in parallel across non-witness nodes...")
        run_parallel(non_witness_ips, "systemctl stop hydra-db")
        print("Stopping systemd service 'zookeeper' in parallel across all nodes...")
        run_parallel(ips, "systemctl stop zookeeper")
            
        # 8. Restart spark-daemon asynchronously in parallel
        print("\n--- Step 8: Restarting spark-daemon asynchronously in parallel ---")
        run_parallel(ips, "(sleep 1 && systemctl restart spark-daemon) >/dev/null 2>&1 < /dev/null &")
            
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

        print(f"Target cluster hosts: {', '.join(ips)}")

        acquire_cluster_lock(ips)
        import atexit
        atexit.register(release_cluster_lock, ips)

        run_destroy_flow(ips)

if __name__ == "__main__":
    main()
