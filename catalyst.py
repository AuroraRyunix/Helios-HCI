#!/usr/bin/env python3
import sys
import os
import re
import json
import time
import socket
import urllib.error
import urllib.request
import ssl
import threading
import uuid
import queue
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# The cluster's one CQL query layer. Fifteen files carried their own copy of this, most
# of them identical, and the guard against conditional statements had reached only three
# of them -- see helios_cql for what that cost.
from helios_cql import (  # noqa: F401  (re-exported for modules that import from here)
    ConditionalStatementError,
    cql_escape,
    cql_int,
    is_conditional_cql,
    run_conditional_cql_query,
    run_cql_query,
)

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

def spark_endpoint(ip):
    """Return (address, verify_identity) for an mTLS call to a spark-daemon.

    Node certificates carry `subjectAltName = IP:<node ip>` and nothing else, so a
    connection can only be tied to the node answering it when it is addressed by that
    same IP. Verification used to be off everywhere, which meant any certificate the
    cluster CA ever signed -- every node's own included -- satisfied a connection to any
    other node.

    Loopback is in no node's SAN. spark-daemon binds 0.0.0.0:9099, so this node's own
    address reaches the same listener and does verify; where that address is unknown the
    identity check is dropped rather than failing the call, since a loopback connection
    cannot be answered by another node in the first place.
    """
    if ip in ("127.0.0.1", "::1", "localhost"):
        if LOCAL_IP and LOCAL_IP not in ("127.0.0.1", "::1", "localhost"):
            return LOCAL_IP, True
        return ip, False
    return ip, True

def run_remote_spark(ip, command):
    ip, verify_identity = spark_endpoint(ip)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = verify_identity

    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=120) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)

DARUK_URL = "http://127.0.0.1:9043"














def run_lwt(endpoint, params, timeout=15):
    """Call one of Daruk's typed compare-and-swap endpoints.

    Returns `(ok, applied, current, error)`.

    `ok` is False only for a genuine failure: Daruk unreachable, a malformed request, a
    database error. A compare-and-swap that was *refused* is `(True, False, {...}, "")` --
    a lost race, not a failure. `current` carries the values that beat it, so the caller
    can say which scheduler already claimed the tick rather than "the update failed".

    There is deliberately no cqlsh fallback. That fallback keeps services working while
    Daruk is down, but it can only run statement text and cannot report whether a
    condition held; a claim that cannot be made conditional must not be made at all --
    running the job twice is worse than not running it this tick.
    """
    try:
        req = urllib.request.Request(
            f"{DARUK_URL}{endpoint}",
            data=json.dumps(params).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            res = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return False, False, {}, json.loads(e.read().decode("utf-8")).get("error", f"HTTP {e.code}")
        except Exception:
            return False, False, {}, f"HTTP {e.code}"
    except Exception as e:
        return False, False, {}, f"Daruk is not answering on {DARUK_URL}: {e}"
    if res.get("status") != "success":
        return False, False, {}, res.get("error", "compare-and-swap failed")
    return True, bool(res.get("applied")), res.get("current") or {}, ""


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

# Initialize Database Schema
def load_schema_module():
    """Import the ordered cluster schema, wherever this process is running from.

    On a host it sits in /usr/local/bin beside this file; inside the Spectrum container
    it is copied to /app. Neither location is importable by name from the other.
    """
    try:
        import helios_schema
        return helios_schema
    except ImportError:
        pass
    import importlib.util
    import os as _os
    for candidate in ("/usr/local/bin/helios_schema.py",
                      _os.path.join(_os.path.dirname(_os.path.abspath(__file__)),
                                    "helios_schema.py")):
        if not _os.path.exists(candidate):
            continue
        spec = importlib.util.spec_from_file_location("helios_schema", candidate)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    raise ImportError(
        "helios_schema.py was not found. The cluster schema cannot be applied without "
        "it; reinstall the Helios components.")


def init_db_schema():
    """Apply the cluster schema, which is shared and no longer this daemon's to define.

    hydra.catalyst_tasks used to be created here. It now lives in helios_schema with
    every other table, so two daemon versions cannot race to define it differently --
    the loser's CREATE TABLE IF NOT EXISTS is a silent no-op and it never finds out.

    Every daemon calls this. Applying happens behind a cluster lock, so concurrent
    starts are safe and no daemon depends on another having run first.

    The unguarded executor is handed over deliberately: the schema lock is an
    IF NOT EXISTS insert and a conditional delete, and helios_schema reads the [applied]
    verdict itself. It is the one caller allowed to run a conditional statement through
    the text path.
    """
    applied = load_schema_module().ensure_schema(run_conditional_cql_query, node_id=LOCAL_IP)
    if applied:
        print(f"[Catalyst] Applied schema migrations: {', '.join(applied)}")

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

def claim_scheduled_run(job_name, expected_last_run, now):
    """Take this tick of `job_name`, or report that somebody else already has it.

    The scheduler's clock is its lock. Reading `last_run_epoch`, deciding the job is due
    and writing the time back is a read-modify-write, and blind it submits the job once
    per scheduler that reaches the row -- two Dagur runs of the same backup, the same
    scrub, the same compaction, against the same volumes at the same moment.

    Two schedulers is not a hypothetical: is_zookeeper_leader() probes ZooKeeper's
    four-letter `stat` and, when the leader does not answer on 9091, falls back to "lowest
    node with 9091 open". A ZooKeeper that is slow, restarting or partitioned hands that
    answer to two nodes at once, and both then believe they are the only scheduler.

    Conditioning the clock write on the value that was read makes the claim and the clock
    one Paxos round, so exactly one caller proceeds. Returning False on a Daruk failure is
    the safe direction: a tick that is skipped runs on the next pass ten seconds later,
    and a tick that is run twice cannot be taken back.
    """
    ok, applied, current, error = run_lwt("/v1/schedule/claim-job", {
        "job_name": job_name,
        "last_run_epoch": now,
        "expected_last_run_epoch": expected_last_run,
    })
    if not ok:
        print(f"[Scheduler] Could not claim '{job_name}': {error}. Skipping this tick.")
        return False
    if not applied:
        print(f"[Scheduler] Job '{job_name}' was already claimed for this interval "
              f"(last_run_epoch is now {current.get('last_run_epoch')}). Skipping.")
        return False
    return True


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
                            # The value as the row holds it, nulls included, because that
                            # is what the compare-and-swap has to condition on. `.get(k, 0)`
                            # returns None for a column that exists and is null, so the
                            # arithmetic below needs its own coercion -- and used to raise
                            # TypeError on such a row, which the loop's except swallowed
                            # and which cost every *other* schedule that pass.
                            last_run_recorded = s.get("last_run_epoch")
                            last_run = last_run_recorded if isinstance(last_run_recorded, int) else 0
                            interval = s.get("interval_seconds") or 3600
                            command = s.get("command", "")

                            if name in local_last_run and now - local_last_run[name] < interval:
                                continue

                            if now - last_run >= interval:
                                # Claim the tick before doing anything with it. A refused
                                # claim means another scheduler got there first and is
                                # already submitting this job.
                                if not claim_scheduled_run(name, last_run_recorded, now):
                                    continue

                                print(f"[Scheduler] Triggering Dagur job: {name}...")
                                local_last_run[name] = now

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

    def setup(self):
        """Complete the TLS handshake in the worker thread.

        Same shape as spark-daemon's handler. Doing it here rather than by wrapping the
        listening socket means one slow or hostile client cannot stall every other
        connection during its handshake.
        """
        self.connection = self.server.ssl_context.wrap_socket(self.request, server_side=True)
        if self.timeout is not None:
            self.connection.settimeout(self.timeout)
        if self.disable_nagle_algorithm:
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        self.rfile = self.connection.makefile("rb", self.rbufsize)
        self.wfile = self.connection.makefile("wb", self.wbufsize)


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
            
            # Query DB first to see if task is already completed/failed in the database
            cql = f"SELECT JSON status, progress, error_msg FROM hydra.catalyst_tasks WHERE task_id = {task_id};"
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
            
            if status_obj and status_obj.get("status") in ["completed", "failed"]:
                with lock:
                    task_events.pop(task_id, None)
                    task_results.pop(task_id, None)
                self.send_json(200, status_obj)
                return
                
            # If not completed/failed in DB, proceed with memory event wait if present
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
    import time
    stdout = None
    for attempt in range(30):
        cql = "SELECT JSON task_id, status FROM hydra.catalyst_tasks;"
        rc, stdout, stderr = run_cql_query(cql)
        if rc == 0:
            break
        print(f"Waiting for database to become available to recover tasks (attempt {attempt+1}/30)...")
        time.sleep(2)
    else:
        print("Database not available. Skipping stuck tasks recovery.")
        return

    if stdout:
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
    
    # Mutual TLS, against the same cluster CA every other inter-node call uses.
    #
    # This API dispatched cluster work -- VM start, stop, migrate -- to anything that
    # could open a socket to port 9091. It bound 0.0.0.0 under Network=host and checked
    # neither a credential nor a source address, so on any network the cluster could
    # reach, it was an unauthenticated remote-control interface for every guest.
    #
    # CERT_REQUIRED is what closes it: a caller must present a certificate this cluster's
    # CA signed, which means a node, and the handshake fails before a request line is
    # ever parsed.
    ca_cert = "/etc/hci/spark/certs/ca.crt"
    node_cert = "/etc/hci/spark/certs/node.crt"
    node_key = "/etc/hci/spark/certs/node.key"
    for path in (ca_cert, node_cert, node_key):
        if not os.path.exists(path):
            print(f"[ERROR] Catalyst cannot start without {path}. The API would otherwise "
                  f"listen without authentication.")
            sys.exit(1)

    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(certfile=node_cert, keyfile=node_key)
    ssl_context.load_verify_locations(cafile=ca_cert)
    ssl_context.verify_mode = ssl.CERT_REQUIRED

    server_address = ("0.0.0.0", 9091)
    httpd = ThreadingHTTPServer(server_address, CatalystAPIHandler)
    httpd.ssl_context = ssl_context
    print("Catalyst API listening on port 9091 (mutual TLS)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
