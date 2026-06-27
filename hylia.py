#!/usr/bin/env python3
__build__ = "1.2.0-b4081"
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
import zipfile
import hashlib

def run_command_local(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return res.returncode, res.stdout.decode('utf-8', errors='ignore').strip(), res.stderr.decode('utf-8', errors='ignore').strip()

def run_remote_spark(ip, command, timeout=45):
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

def run_cql_query(cql_query):
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
        return -1, "", str(e)

def get_cluster_hosts():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            return cdata.get("hosts", [])
    except Exception:
        return []

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

def get_zookeeper_leader_ip():
    hosts = get_cluster_hosts()
    ips = [h.get("ip") for h in hosts if h.get("ip")] if hosts else ["10.10.102.120", "10.10.102.121", "10.10.102.122"]
    for ip in ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((ip, 2181))
            s.sendall(b"stat")
            resp = s.recv(1024).decode('utf-8', errors='ignore')
            s.close()
            if "mode: leader" in resp.lower() or "mode: standalone" in resp.lower():
                return ip
        except Exception:
            pass
    return None

def is_zookeeper_leader():
    return get_zookeeper_leader_ip() == LOCAL_IP

def log_upgrade(job_id, line):
    print(f"[Hylia] {line}")
    timestamp = int(time.time() * 1000)
    # Escape quotes
    escaped_line = line.replace("'", "''")
    cql = f"INSERT INTO hydra.hylia_logs (job_id, timestamp, log_line) VALUES ({job_id}, {timestamp}, '{escaped_line}');"
    run_cql_query(cql)

def validate_and_extract_zip(zip_path, extract_dir):
    if os.path.exists(extract_dir):
        import shutil
        try:
            shutil.rmtree(extract_dir)
        except Exception:
            pass
    os.makedirs(extract_dir, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        
    manifest_path = os.path.join(extract_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        raise Exception("manifest.json not found in update package.")
        
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
        
    components = manifest.get("components", {})
    for comp_name, comp_info in components.items():
        comp_file = comp_info.get("file")
        declared_hash = comp_info.get("sha256")
        
        file_path = os.path.join(extract_dir, comp_file)
        if not os.path.exists(file_path):
            raise Exception(f"Declared file '{comp_file}' for component '{comp_name}' is missing.")
            
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f_bin:
            while chunk := f_bin.read(8192):
                sha256.update(chunk)
        actual_hash = sha256.hexdigest()
        
        if actual_hash != declared_hash:
            raise Exception(f"Checksum verification failed for '{comp_file}'. Declared: {declared_hash}, Actual: {actual_hash}")
            
    changelog_file = manifest.get("changelog", "changelog.md")
    changelog_path = os.path.join(extract_dir, changelog_file)
    changelog_content = ""
    if os.path.exists(changelog_path):
        with open(changelog_path, "r", encoding="utf-8", errors="ignore") as f_ch:
            changelog_content = f_ch.read()
            
    return manifest, changelog_content

def get_service_build_number(target_path):
    if not os.path.exists(target_path):
        return "Not Installed"
    try:
        with open(target_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if "__build__" in line and "=" in line:
                    parts = line.split("=", 1)
                    val = parts[1].strip().strip("'\"")
                    return val
    except Exception:
        pass
    return "Unknown"

# Active set of jobs running on this host thread
running_jobs = set()

def hylia_rolling_upgrade(job_id):
    if job_id in running_jobs:
        return
    running_jobs.add(job_id)
    
    try:
        log_upgrade(job_id, "=== Initiating Hylia Rolling Upgrade Sequence ===")
        
        # 1. Fetch job data
        cql_job = f"SELECT JSON job_id, state, target_nodes, current_node, build_number, manifest_json, changelog_md FROM hydra.hylia_jobs WHERE job_id = {job_id};"
        rc_j, stdout_j, _ = run_cql_query(cql_job)
        if rc_j != 0 or not stdout_j:
            raise Exception("Failed to load upgrade job data from ScyllaDB.")
            
        job_data = json.loads(stdout_j.splitlines()[0])
        target_nodes = job_data.get("target_nodes", [])
        current_node_state = job_data.get("current_node")
        build_number = job_data.get("build_number", "Unknown")
        manifest = json.loads(job_data.get("manifest_json", "{}"))
        
        log_upgrade(job_id, f"Target version build: {build_number}")
        log_upgrade(job_id, f"Target nodes to upgrade: {', '.join(target_nodes)}")
        
        # Transition state to UPGRADING
        run_cql_query(f"UPDATE hydra.hylia_jobs SET state = 'UPGRADING' WHERE job_id = {job_id};")
        
        # Skip completed hosts if resuming
        start_index = 0
        if current_node_state:
            if current_node_state in target_nodes:
                start_index = target_nodes.index(current_node_state)
                log_upgrade(job_id, f"Resuming rolling upgrade starting at node {current_node_state}...")
            else:
                log_upgrade(job_id, f"Resuming; current node {current_node_state} not in target list. Starting from scratch.")
        
        for idx in range(start_index, len(target_nodes)):
            node_ip = target_nodes[idx]
            
            # Update ScyllaDB with current node progress
            run_cql_query(f"UPDATE hydra.hylia_jobs SET current_node = '{node_ip}' WHERE job_id = {job_id};")
            
            # Query hostname of node_ip
            hostname = "Unknown"
            rc_h, stdout_h, _ = run_cql_query(f"SELECT JSON hostname FROM hydra.nodes WHERE ip = '{node_ip}' ALLOW FILTERING;")
            if rc_h == 0 and stdout_h:
                try:
                    hostname = json.loads(stdout_h.splitlines()[0]).get("hostname")
                except Exception:
                    pass
            
            log_upgrade(job_id, f"--- Starting Upgrade Phase for Host {hostname} ({node_ip}) ---")
            
            # Step 1: Evacuate Host & Enter Maintenance
            log_upgrade(job_id, f"[{hostname}] Evacuating host VMs and entering maintenance mode...")
            payload_enter = {"hostname": hostname, "action": "enter", "force_stop": True}
            
            # Vali API runs on 127.0.0.1:9095, Vali's /api/v1/host/maintenance is exposed locally
            # We will trigger the API by calling Vali local endpoint or spark api
            rc_e, res_e, err_e = run_mtls_spark_api("127.0.0.1", "/api/v1/host/maintenance", payload_enter, method="POST")
            if rc_e != 0 or "error" in res_e:
                raise Exception(f"Failed to submit maintenance enter task to Vali: {res_e.get('error', err_e)}")
                
            maint_task_id = res_e.get("task_id")
            log_upgrade(job_id, f"[{hostname}] Maintenance task submitted (Task ID: {maint_task_id}). Waiting for evacuation...")
            
            maint_success = False
            for _ in range(150): # up to 5 minutes
                cql_check = f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{hostname}';"
                rc_c, stdout_c, _ = run_cql_query(cql_check)
                if rc_c == 0 and stdout_c:
                    try:
                        n_status = json.loads(stdout_c.splitlines()[0]).get("status")
                        if n_status == "IN_MAINTENANCE":
                            maint_success = True
                            break
                    except Exception:
                        pass
                time.sleep(2)
                
            if not maint_success:
                raise Exception(f"Host {hostname} failed to enter maintenance mode (evacuation timeout).")
            log_upgrade(job_id, f"[{hostname}] Successfully evacuated and entered maintenance mode.")
            
            # Step 2: Deploy Verified Files
            log_upgrade(job_id, f"[{hostname}] Deploying verified components...")
            extract_dir = "/tmp/yggdrasil_update" # Local extraction folder on leader
            
            # If the current leader is upgrading itself, we read from extract_dir.
            # If we are pushing files to a remote host, we can read them locally and push via base64 Spark CLI
            components = manifest.get("components", {})
            for comp_name, comp_info in components.items():
                comp_file = comp_info.get("file")
                target_path = comp_info.get("target_path", f"/usr/local/bin/{comp_name}")
                
                local_file_path = os.path.join(extract_dir, comp_file)
                if os.path.exists(local_file_path):
                    log_upgrade(job_id, f"[{hostname}] Transferring component '{comp_name}' to {target_path}...")
                    with open(local_file_path, "rb") as f_bin:
                        b64_data = base64.b64encode(f_bin.read()).decode("utf-8")
                        
                    # Split into chunks if too large (e.g. spectrum_server is ~280KB, base64 is ~380KB)
                    # We write it block-by-block remotely
                    remote_dir = os.path.dirname(target_path)
                    run_remote_spark(node_ip, f"mkdir -p {remote_dir} && rm -f {target_path}")
                    
                    chunk_size = 64000
                    for c_idx in range(0, len(b64_data), chunk_size):
                        sub_chunk = b64_data[c_idx:c_idx+chunk_size]
                        write_cmd = f"echo '{sub_chunk}' >> {target_path}.tmp"
                        rc_w, _, err_w = run_remote_spark(node_ip, write_cmd)
                        if rc_w != 0:
                            raise Exception(f"Failed to write file chunk to remote node: {err_w}")
                            
                    decode_cmd = f"cat {target_path}.tmp | base64 -d > {target_path} && rm -f {target_path}.tmp && chmod +x {target_path} || true"
                    rc_d, _, err_d = run_remote_spark(node_ip, decode_cmd)
                    if rc_d != 0:
                        raise Exception(f"Failed to decode base64 file remotely: {err_d}")
                        
            # If Spectrum is inside the package, rebuild the podman container
            if "spectrum" in components:
                log_upgrade(job_id, f"[{hostname}] Rebuilding Spectrum container on host...")
                build_cmd = (
                    "rm -rf /tmp/spectrum_build && mkdir -p /tmp/spectrum_build/static && "
                    "cp /usr/local/bin/spectrum_server /tmp/spectrum_build/server.py && "
                    "cp /usr/local/bin/Dockerfile /tmp/spectrum_build/Dockerfile && "
                    "cp -r /usr/local/bin/static/* /tmp/spectrum_build/static/ && "
                    "podman build -t localhost/spectrum:latest /tmp/spectrum_build && "
                    "systemctl restart spectrum"
                )
                # Note: /usr/local/bin/static/* and Dockerfile must be copied
                # We execute a build script on the target host
                rc_b, out_b, err_b = run_remote_spark(node_ip, build_cmd)
                if rc_b != 0:
                    log_upgrade(job_id, f"[{hostname}] Warning during Spectrum build: {err_b or out_b}")
                    
            log_upgrade(job_id, f"[{hostname}] All files successfully copied.")
            
            # Step 3: Reboot Host
            log_upgrade(job_id, f"[{hostname}] Initiating host reboot sequence...")
            run_remote_spark(node_ip, "reboot || true")
            time.sleep(10)
            
            log_upgrade(job_id, f"[{hostname}] Waiting for node to go offline...")
            for _ in range(60):
                rc_p, _, _ = run_remote_spark(node_ip, "echo 1")
                if rc_p != 0:
                    log_upgrade(job_id, f"[{hostname}] Node went offline.")
                    break
                time.sleep(2)
                
            log_upgrade(job_id, f"[{hostname}] Waiting for node to come back online...")
            online = False
            for _ in range(120):
                rc_p, _, _ = run_remote_spark(node_ip, "echo 1")
                if rc_p == 0:
                    online = True
                    log_upgrade(job_id, f"[{hostname}] Node is back online.")
                    break
                time.sleep(3)
                
            if not online:
                raise Exception(f"Node {hostname} did not return online after reboot.")
                
            log_upgrade(job_id, f"[{hostname}] Waiting 20 seconds for services to stabilize...")
            time.sleep(20)
            
            # Step 4: Leave Maintenance Mode
            log_upgrade(job_id, f"[{hostname}] Restoring node from maintenance mode...")
            payload_leave = {"hostname": hostname, "action": "leave"}
            rc_l, res_l, err_l = run_mtls_spark_api("127.0.0.1", "/api/v1/host/maintenance", payload_leave, method="POST")
            if rc_l != 0 or "error" in res_l:
                raise Exception(f"Failed to submit maintenance leave task: {res_l.get('error', err_l)}")
                
            leave_success = False
            for _ in range(60):
                cql_check = f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{hostname}';"
                rc_c, stdout_c, _ = run_cql_query(cql_check)
                if rc_c == 0 and stdout_c:
                    try:
                        n_status = json.loads(stdout_c.splitlines()[0]).get("status")
                        if n_status == "NORMAL":
                            leave_success = True
                            break
                    except Exception:
                        pass
                time.sleep(2)
                
            if not leave_success:
                raise Exception(f"Host {hostname} failed to leave maintenance mode.")
            log_upgrade(job_id, f"[{hostname}] Upgraded successfully and returned to normal service.")
            
        # Upgrade completed successfully!
        log_upgrade(job_id, "=== Rolling Upgrade Completed Successfully on all Nodes ===")
        run_cql_query(f"UPDATE hydra.hylia_jobs SET state = 'COMPLETED' WHERE job_id = {job_id};")
        
    except Exception as ex:
        log_upgrade(job_id, f"CRITICAL ERROR: Rolling Upgrade Failed: {ex}")
        run_cql_query(f"UPDATE hydra.hylia_jobs SET state = 'FAILED' WHERE job_id = {job_id};")
    finally:
        running_jobs.discard(job_id)

def hylia_loop():
    print("[Hylia] Daemon loop started.")
    while True:
        try:
            if is_zookeeper_leader():
                # Query upgrading jobs
                rc, stdout, _ = run_cql_query("SELECT JSON job_id, state FROM hydra.hylia_jobs;")
                if rc == 0 and stdout:
                    for line in stdout.splitlines():
                        if line.strip():
                            job = json.loads(line)
                            job_state = job.get("state")
                            job_id = job.get("job_id")
                            if job_state in ["STARTING", "UPGRADING"] and job_id not in running_jobs:
                                print(f"[Hylia] Found active job {job_id} in state {job_state}. Running rolling upgrade...")
                                threading.Thread(target=hylia_rolling_upgrade, args=(job_id,), daemon=True).start()
        except Exception as e:
            sys.stderr.write(f"[Hylia Loop Error] {e}\n")
        time.sleep(5)

if __name__ == "__main__":
    hylia_loop()
