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
import queue
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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

# Initialize Database Schema
def init_db_schema():
    tasks_table = """
    CREATE TABLE IF NOT EXISTS hydra.catalyst_tasks (
        task_id uuid PRIMARY KEY,
        service text,
        action text,
        status text,
        payload text,
        progress int,
        error_msg text,
        created_at timestamp,
        updated_at timestamp
    );
    """
    run_cql_query(tasks_table)

# In-Memory Event Queues & Completion Sync
queues = {
    "vali": queue.Queue(),
    "dagur": queue.Queue(),
    "spark": queue.Queue()
}

task_events = {}
task_results = {}
lock = threading.Lock()

def submit_task_to_memory(service, task_data):
    if service in queues:
        queues[service].put(task_data)
        task_id = task_data["task_id"]
        with lock:
            task_events[task_id] = threading.Event()

# Scheduler Thread: reads hydra.dagur_schedules and submits execution tasks to Dagur
def scheduler_thread_loop():
    print("Catalyst scheduler thread started.")
    local_last_run = {}
    while True:
        try:
            if is_zookeeper_leader():
                cql = "SELECT JSON * FROM hydra.dagur_schedules;"
                rc, stdout, stderr = run_cql_query(cql)
                if rc == 0 and stdout:
                    schedules = []
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                schedules.append(json.loads(line))
                            except:
                                pass
                    
                    now = int(time.time())
                    for s in schedules:
                        if s.get("enabled", False):
                            name = s.get("job_name")
                            last_run = s.get("last_run_epoch", 0)
                            interval = s.get("interval_seconds", 3600)
                            command = s.get("command", "")
                            
                            if name in local_last_run and now - local_last_run[name] < interval:
                                continue
                                
                            if now - last_run >= interval:
                                print(f"[Scheduler] Triggering Dagur job: {name}...")
                                local_last_run[name] = now
                                cql_update = f"UPDATE hydra.dagur_schedules SET last_run_epoch = {now} WHERE job_name = '{name}';"
                                run_cql_query(cql_update)
                                
                                task_id = str(uuid.uuid4())
                                now_ms = int(time.time() * 1000)
                                payload = json.dumps({"job_name": name, "command": command})
                                
                                cql_insert = f"""
                                INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
                                VALUES ({task_id}, 'dagur', 'execute', 'pending', '{payload.replace("'", "''")}', 0, {now_ms}, {now_ms});
                                """
                                run_cql_query(cql_insert)
                                
                                submit_task_to_memory("dagur", {
                                    "task_id": task_id,
                                    "action": "execute",
                                    "payload": {"job_name": name, "command": command}
                                })
        except Exception as e:
            sys.stderr.write(f"Error in scheduler loop: {e}\n")
        time.sleep(10)

class CatalystAPIHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass # Prevent console clutter
        
    def send_json(self, status, data):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def do_GET(self):
        parts = self.path.split('/')
        
        # 1. GET /api/v1/queues/<service> (Long polling)
        if len(parts) == 5 and parts[3] == "queues":
            service = parts[4]
            if service in queues:
                try:
                    task = queues[service].get(timeout=30.0)
                    self.send_json(200, task)
                except queue.Empty:
                    self.send_response(204) # No Content
                    self.end_headers()
                return
            else:
                self.send_json(404, {"error": f"Unknown service queue: {service}"})
                return
                
        # 2. GET /api/v1/tasks/status/<task_id> (Long polling completion)
        elif len(parts) == 6 and parts[3] == "tasks" and parts[4] == "status":
            task_id = parts[5]
            event = None
            with lock:
                if task_id in task_events:
                    event = task_events[task_id]
            
            if event:
                finished = event.wait(timeout=30.0)
                if finished:
                    with lock:
                        result = task_results.get(task_id, {"status": "unknown"})
                        task_events.pop(task_id, None)
                        task_results.pop(task_id, None)
                    self.send_json(200, result)
                else:
                    self.send_response(204) # Timeout, retry
                    self.end_headers()
            else:
                # Fallback to DB query if not in memory (or completed earlier)
                cql = f"SELECT JSON status, error_msg FROM hydra.catalyst_tasks WHERE task_id = {task_id};"
                rc, stdout, _ = run_cql_query(cql)
                status_obj = None
                if rc == 0 and stdout:
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                status_obj = json.loads(line)
                                break
                            except:
                                pass
                if status_obj:
                    self.send_json(200, status_obj)
                else:
                    self.send_json(404, {"error": "Task not found"})
            return
            
        self.send_json(404, {"error": "Not Found"})

    def do_POST(self):
        # 1. POST /api/v1/tasks/submit
        if self.path == "/api/v1/tasks/submit":
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            try:
                payload = json.loads(post_data.decode('utf-8'))
                service = payload.get("service")
                action = payload.get("action")
                task_payload = payload.get("payload", {})
            except Exception as e:
                self.send_json(400, {"error": f"Invalid JSON payload: {str(e)}"})
                return
                
            if not service or not action:
                self.send_json(400, {"error": "service and action fields required"})
                return
                
            task_id = str(uuid.uuid4())
            now_ms = int(time.time() * 1000)
            payload_str = json.dumps(task_payload)
            
            cql = f"""
            INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
            VALUES ({task_id}, '{service}', '{action}', 'pending', '{payload_str.replace("'", "''")}', 0, {now_ms}, {now_ms});
            """
            run_cql_query(cql)
            
            task_data = {
                "task_id": task_id,
                "action": action,
                "payload": task_payload
            }
            submit_task_to_memory(service, task_data)
            
            self.send_json(200, {"task_id": task_id, "status": "pending"})
            return
            
        # 2. POST /api/v1/tasks/update
        elif self.path == "/api/v1/tasks/update":
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            try:
                payload = json.loads(post_data.decode('utf-8'))
                task_id = payload.get("task_id")
                status = payload.get("status")
                progress = payload.get("progress", 0)
                error_msg = payload.get("error_msg", "")
                result_data = payload.get("result", {})
            except Exception as e:
                self.send_json(400, {"error": f"Invalid JSON payload: {str(e)}"})
                return
                
            if not task_id or not status:
                self.send_json(400, {"error": "task_id and status fields required"})
                return
                
            now_ms = int(time.time() * 1000)
            escaped_error = error_msg.replace("'", "''")
            err_field = f", error_msg = '{escaped_error}'" if error_msg else ""
            
            cql = f"""
            UPDATE hydra.catalyst_tasks 
            SET status = '{status}', progress = {progress}, updated_at = {now_ms}{err_field} 
            WHERE task_id = {task_id};
            """
            run_cql_query(cql)
            
            if status in ["completed", "failed"]:
                with lock:
                    if task_id in task_events:
                        task_results[task_id] = {
                            "status": status,
                            "error_msg": error_msg,
                            "result": result_data
                        }
                        task_events[task_id].set()
            
            self.send_json(200, {"status": "ok"})
            return
            
        self.send_json(404, {"error": "Not Found"})

def recover_stuck_tasks():
    print("Catalyst recovering stuck tasks on startup...")
    cql = "SELECT JSON task_id, status FROM hydra.catalyst_tasks;"
    rc, stdout, stderr = run_cql_query(cql)
    if rc == 0 and stdout:
        stuck_tasks = []
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    t = json.loads(line)
                    status = t.get("status")
                    if status in ["pending", "processing"]:
                        stuck_tasks.append(t.get("task_id"))
                except:
                    pass
        for task_id in stuck_tasks:
            print(f"Aborting stuck task: {task_id}")
            now_ms = int(time.time() * 1000)
            update_cql = f"""
            UPDATE hydra.catalyst_tasks 
            SET status = 'failed', error_msg = 'Task aborted due to system daemon restart.', updated_at = {now_ms} 
            WHERE task_id = {task_id};
            """
            run_cql_query(update_cql)

def main():
    print("Catalyst task coordination service starting...")
    init_db_schema()
    recover_stuck_tasks()
    
    # Start scheduler thread
    t = threading.Thread(target=scheduler_thread_loop, daemon=True)
    t.start()
    
    server_address = ("0.0.0.0", 9091)
    httpd = ThreadingHTTPServer(server_address, CatalystAPIHandler)
    print("Catalyst API listening on port 9091")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
