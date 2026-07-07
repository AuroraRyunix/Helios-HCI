#!/usr/bin/env python3
import sys
import os
import json
import time
import socket
import urllib.request
import ssl
import threading
import uuid

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

def is_zookeeper_leader():
    return get_zookeeper_leader_ip() == LOCAL_IP

def call_catalyst_api(path, payload=None, method="GET"):
    import urllib.request
    import json
    url = f"http://127.0.0.1:9091{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=35) as response:
            if response.status == 204:
                return 204, None
            res = json.loads(response.read().decode("utf-8"))
            return response.status, res
    except Exception as e:
        return -1, str(e)

def insert_dagur_run(job_name, start_time, run_id, end_time, status, exit_code, output):
    clean_output = output.replace("'", "''").replace("\\", "\\\\")
    cql = f"""
    INSERT INTO hydra.dagur_runs (job_name, start_time, run_id, end_time, status, exit_code, output)
    VALUES ('{job_name}', {start_time}, {run_id}, {end_time}, '{status}', {exit_code}, '{clean_output}');
    """
    run_cql_query(cql)

def execute_dagur_job_thread(task_id, job_name, command):
    run_id = str(uuid.uuid4())
    start_time = int(time.time() * 1000)
    
    cql_start = f"""
    INSERT INTO hydra.dagur_runs (job_name, start_time, run_id, status, exit_code, output)
    VALUES ('{job_name}', {start_time}, {run_id}, 'RUNNING', -1, 'Job started...');
    """
    run_cql_query(cql_start)
    
    # Notify Catalyst we are processing
    call_catalyst_api("/api/v1/tasks/update", {
        "task_id": task_id,
        "status": "processing",
        "progress": 5
    }, method="POST")
    
    stop_progress_ticker = threading.Event()
    def progress_ticker():
        current_prog = 5
        while not stop_progress_ticker.wait(1.0):
            if current_prog < 95:
                current_prog += 10
                if current_prog > 95:
                    current_prog = 95
                call_catalyst_api("/api/v1/tasks/update", {
                    "task_id": task_id,
                    "status": "processing",
                    "progress": current_prog
                }, method="POST")
                
    ticker_thread = threading.Thread(target=progress_ticker)
    ticker_thread.start()
    
    try:
        exit_code, stdout, stderr = run_remote_spark("127.0.0.1", command)
        out_str = stdout + stderr
        status = 'SUCCESS' if exit_code == 0 else 'FAILED'
    except Exception as e:
        exit_code = -1
        out_str = f"Execution failed: {str(e)}"
        status = 'FAILED'
    finally:
        stop_progress_ticker.set()
        ticker_thread.join()
        
    end_time = int(time.time() * 1000)
    insert_dagur_run(job_name, start_time, run_id, end_time, status, exit_code, out_str)
    
    # Notify Catalyst of result
    status_str = "completed" if exit_code == 0 else "failed"
    call_catalyst_api("/api/v1/tasks/update", {
        "task_id": task_id,
        "status": status_str,
        "progress": 100,
        "error_msg": out_str if exit_code != 0 else ""
    }, method="POST")

def main():
    print("Dagur Catalyst task runner daemon started.")
    while True:
        try:
            if not is_zookeeper_leader():
                time.sleep(2)
                continue
                
            status, res = call_catalyst_api("/api/v1/queues/dagur")
            if status == 200 and res:
                task_id = res.get("task_id")
                action = res.get("action")
                payload = res.get("payload", {})
                
                job_name = payload.get("job_name")
                command = payload.get("command")
                
                print(f"[Dagur] Received task from Catalyst: {job_name} ({action})")
                t = threading.Thread(target=execute_dagur_job_thread, args=(task_id, job_name, command), daemon=True)
                t.start()
                
            elif status == 204:
                time.sleep(2)
            else:
                time.sleep(2)
        except Exception as e:
            sys.stderr.write(f"Error in Dagur loop: {e}\n")
            time.sleep(2)

if __name__ == "__main__":
    main()
