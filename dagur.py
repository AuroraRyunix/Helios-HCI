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

def run_cql_query(cql_query):
    import base64
    b64_query = base64.b64encode(cql_query.encode('utf-8')).decode('utf-8')
    cmd = f'echo {b64_query} | base64 -d | podman exec -i systemd-hydra-db cqlsh {LOCAL_IP}'
    rc, stdout, stderr = run_remote_spark("127.0.0.1", cmd)
    if rc == 0 and stdout:
        stdout = stdout.replace('\\\\', '\\')
    return rc, stdout, stderr

def is_zookeeper_leader():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        s.connect(("127.0.0.1", 2181))
        s.sendall(b"stat")
        resp = s.recv(1024).decode('utf-8', errors='ignore')
        s.close()
        return "mode: leader" in resp.lower()
    except Exception:
        return False

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
        "progress": 0
    }, method="POST")
    
    try:
        exit_code, stdout, stderr = run_remote_spark("127.0.0.1", command)
        out_str = stdout + stderr
        status = 'SUCCESS' if exit_code == 0 else 'FAILED'
    except Exception as e:
        exit_code = -1
        out_str = f"Execution failed: {str(e)}"
        status = 'FAILED'
        
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
