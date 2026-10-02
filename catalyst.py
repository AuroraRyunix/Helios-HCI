#!/usr/bin/env python3
import sys
import os
import re
import json
import time
import socket
import helios_zk
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


def cluster_ips():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            return [h["ip"] for h in json.load(f).get("hosts", []) if h.get("ip")]
    except Exception:
        return [LOCAL_IP]


# -- Which Catalyst holds the queues, and which one runs the schedule ---------------------
#
# Two jobs, so two candidacies. They are deliberately not one: a node that happens to hold
# the queues has no reason to also be the node that submits every scheduled job, and
# funnelling both through one name is the coupling this replaces wearing a different hat.
#
# Both used to be `get_zookeeper_leader_ip() == LOCAL_IP` -- a string comparison against the
# node the *ZooKeeper ensemble* had elected for its own reasons. An ensemble election is a
# restart, a blip, a rolling upgrade, and it moved every leader-only workload in the cluster
# onto one node at once. Neither of these jobs has anything to do with who leads ZooKeeper.
#
# `dispatch` is the one with a visible consequence: the queues are in memory, so the node
# holding this candidacy is the node a submission has to reach and a worker has to poll.
# Everybody finds it the same way -- by reading what the winner published, not by probing.
_CANDIDACIES = {}
_CANDIDACY_LOCK = threading.Lock()


def candidacy(service):
    with _CANDIDACY_LOCK:
        existing = _CANDIDACIES.get(service)
        if existing is None:
            existing = helios_zk.cluster_candidacy(service, LOCAL_IP, hosts=cluster_ips())
            _CANDIDACIES[service] = existing
        return existing


def holds_dispatch():
    """True when this process is the one holding the queues."""
    return candidacy(helios_zk.SERVICE_CATALYST_DISPATCH).leading()


def dispatch_ip():
    """The address of the Catalyst holding the queues, or None.

    What every submitter needs and nobody could previously ask for. The winner publishes
    its own address in its ballot, so this is a read of a znode rather than a round of
    `stat` probes followed by a port check followed by a guess.
    """
    published = candidacy(helios_zk.SERVICE_CATALYST_DISPATCH).leader_identity()
    if not published:
        return None
    return published.decode("utf-8", "replace").strip() or None


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
    applied = schema_module().ensure_schema(run_conditional_cql_query, node_id=LOCAL_IP)
    if applied:
        print(f"[Catalyst] Applied schema migrations: {', '.join(applied)}")


_SCHEMA_MODULE = None


def schema_module():
    """The schema module, loaded once.

    It owns the task table's column list and the statements that write it, so every writer
    of a task row builds the same row. Four independent column lists is how
    `parent_task_id` came to live inside a JSON payload in one of them and nowhere else.
    """
    global _SCHEMA_MODULE
    if _SCHEMA_MODULE is None:
        _SCHEMA_MODULE = load_schema_module()
    return _SCHEMA_MODULE


# The component a task belongs to, when the submitter did not say. `service` is the queue
# that executes a task, which is not the same question -- a Hylia upgrade step runs on the
# `dagur` queue -- so defaulting one to the other is a last resort and not the intent.
DEFAULT_COMPONENT = "Catalyst"

# The last sequence id this process claimed per component, used only as the starting guess
# for the next claim. Wrong is free: the compare-and-swap refuses and hands back the right
# value, which costs one round trip and never a duplicate.
_SEQUENCE_HINTS = {}
_SEQUENCE_LOCK = threading.Lock()


def next_sequence_id(component):
    """The next sequence id for `component`, or None if one could not be claimed.

    None is not an error the submission should fail on. The number is how an operator orders
    a component's history; it is not how the cluster executes anything, and a task framework
    that refuses work because a counter was contended is worse than a task with no number.
    """
    with _SEQUENCE_LOCK:
        hint = _SEQUENCE_HINTS.get(component)
    claimed = schema_module().claim_task_sequence(run_lwt, component, expected=hint)
    if claimed is None:
        return None
    with _SEQUENCE_LOCK:
        _SEQUENCE_HINTS[component] = claimed
    return claimed

# In-Memory Event Queues & Completion Sync
#
# One entry per worker that long-polls /api/v1/queues/<name> on the node holding the dispatch
# candidacy. A name with no worker behind it is worse than a missing name: the
# submission succeeds, a row is written, and the task sits `pending` forever, which reads
# as "slow" rather than "nothing is going to happen". `spark` is exactly that today --
# nothing drains it -- and it is left in place only because removing a queue is a change
# to what a submission means, which belongs with whatever finally claims the name.
#
# `lanayru` is drained by the console backend rather than by a daemon of its own, because
# `lanayru.py` is a module that tier imports and the deploy needs everything it imports.
queues = {
    "vali": queue.Queue(),
    "dagur": queue.Queue(),
    "spark": queue.Queue(),
    "lanayru": queue.Queue()
}

task_events = {}
task_results = {}
lock = threading.Lock()

# Every task id this process has put on a queue. The recovery sweep replays `pending` rows
# the queues do not already hold, and without this it would replay the ones it had just
# queued itself -- a task is `pending` from the moment it is recorded until a worker picks it
# up, which is exactly the window the sweep runs in.
queued_task_ids = set()


def submit_task_to_memory(service, task_data):
    if service in queues:
        queues[service].put(task_data)
        task_id = task_data["task_id"]
        with lock:
            task_events[task_id] = threading.Event()
            queued_task_ids.add(task_id)

def claim_scheduled_run(job_name, expected_last_run, now):
    """Take this tick of `job_name`, or report that somebody else already has it.

    The scheduler's clock is its lock. Reading `last_run_epoch`, deciding the job is due
    and writing the time back is a read-modify-write, and blind it submits the job once
    per scheduler that reaches the row -- two Dagur runs of the same backup, the same
    scrub, the same compaction, against the same volumes at the same moment.

    Two schedulers is not a hypothetical, and it stays possible with a real election. A
    candidacy answers correctly when it answers, but the answer is read from the ensemble at
    some instant and acted on at a later one: a scheduler that reads "I lead", then stalls
    long enough to lose its session, is still inside the pass it started. The condition on
    the clock is what closes that window, and it is the only thing that can -- the claim and
    the clock are one Paxos round, so the second scheduler is told the tick is taken.

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
    scheduler = candidacy(helios_zk.SERVICE_CATALYST_SCHEDULER)
    while True:
        try:
            if scheduler.leading():
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
                                
                                # Recorded as Catalyst's own task even though Dagur runs it:
                                # the schedule is Catalyst's, and `service` already says who
                                # executes. `task_type` carries what the row otherwise could
                                # not -- every scheduled job has the action `execute`, so
                                # without it a component's history is a column of identical
                                # verbs.
                                run_cql_query(schema_module().task_insert_statement(
                                    task_id, "dagur", "execute", payload, now_ms,
                                    component=DEFAULT_COMPONENT,
                                    task_type="scheduled_job",
                                    sequence_id=next_sequence_id(DEFAULT_COMPONENT)))
                                
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
                if not holds_dispatch():
                    # The queues are in memory, so only the node holding the dispatch
                    # candidacy has anything in them. Answering 204 here would be
                    # indistinguishable from "nothing queued" and a worker polling the
                    # wrong node would wait forever on an empty queue while work piled up
                    # on the right one. Said as an error so the worker's log names it.
                    self.send_json(503, {"error": "this node does not hold the Catalyst "
                                                  "dispatch candidacy"})
                    return
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
            cql = (f"SELECT JSON status, progress, error_msg, parent_task_id, component, "
                   f"sequence_id, task_type, completed_at "
                   f"FROM hydra.catalyst_tasks WHERE task_id = {task_id};")
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
                if not isinstance(task_payload, dict):
                    task_payload = {}
                component = payload.get("component")
                task_type = payload.get("task_type") or payload.get("type")
                # A parent may be named at the top level or inside the payload. The payload
                # is where it has always lived -- mipha.py put it there because there was no
                # column -- and those submissions still arrive, so both are read and the
                # column is written from either.
                parent_task_id = payload.get("parent_task_id") or task_payload.get("parent_task_id")
            except Exception as e:
                self.send_json(400, {"error": f"Invalid JSON payload: {str(e)}"})
                return

            if not service or not action:
                self.send_json(400, {"error": "service and action fields required"})
                return

            task_id = str(uuid.uuid4())
            now_ms = int(time.time() * 1000)
            payload_str = json.dumps(task_payload)
            component = component or service or DEFAULT_COMPONENT

            run_cql_query(schema_module().task_insert_statement(
                task_id, service, action, payload_str, now_ms,
                component=component,
                task_type=task_type,
                sequence_id=next_sequence_id(component),
                parent_task_id=parent_task_id))

            # The row is written before anything is queued, and on every node whether or not
            # it holds the dispatch candidacy. A submission that reached the wrong Catalyst
            # used to be queued where nothing drained it and sat `pending` forever, which
            # reads as "slow". Recorded, it is work the dispatcher's sweep finds and replays
            # -- which is the whole point of persisting a task rather than noting it.
            if holds_dispatch():
                submit_task_to_memory(service, {
                    "task_id": task_id,
                    "action": action,
                    "payload": task_payload,
                })

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
            run_cql_query(schema_module().task_update_statement(
                task_id, status, progress, now_ms, error_msg=error_msg))

            if status in ["completed", "failed"]:
                with lock:
                    queued_task_ids.discard(task_id)
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

# -- Recovery ----------------------------------------------------------------------------
#
# What this used to do: at every start, on every node, mark every `pending` and every
# `processing` task `failed` with "Task aborted due to system daemon restart." That is data
# loss as designed behaviour, and it threw away exactly the tasks persistence exists to keep
# -- the ones that had not run yet. A task submitted a second before a restart was recorded,
# aborted, and never attempted.
#
# The two states are not the same thing and must not be handled the same way:
#
#   * `pending` means nothing has happened. No worker has seen it, so nothing in the cluster
#     has been touched and replaying it is simply doing what was asked. It is re-queued.
#
#   * `processing` means a worker had it. What it did before the dispatcher holding its
#     queue stopped is unknown, and the actions behind these rows are not replayable on a
#     guess -- a half-finished live migration replayed is a second migration of a guest that
#     may already be running somewhere else. So it is failed, with a reason that says it was
#     in flight rather than that a daemon restarted.
#
# Either way the task ends up in a state someone can act on. Nothing is dropped silently,
# which is the property that was missing.
IN_FLIGHT_REASON = ("Interrupted: this task was running when the Catalyst holding its queue "
                    "stopped, so how far it got is not recorded. Re-submit it after checking "
                    "what it had already changed.")


def read_open_tasks():
    """Every task that is still `pending` or `processing`, or None if the table could not
    be read. None and an empty list are different answers: recovery must not conclude there
    is nothing to replay because the database did not answer."""
    rc, stdout, _stderr = run_cql_query(
        "SELECT JSON task_id, service, action, status, payload FROM hydra.catalyst_tasks;")
    if rc != 0:
        return None
    open_tasks = []
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{") or not line.endswith("}"):
            continue
        try:
            row = json.loads(line)
        except ValueError:
            # A row that will not parse is a row nothing can decide about. Skipping it
            # leaves it exactly as it was, which is better than failing a task on the
            # strength of not having been able to read it.
            continue
        if row.get("status") in ("pending", "processing"):
            open_tasks.append(row)
    return open_tasks


def fail_task(task_id, reason):
    now_ms = int(time.time() * 1000)
    run_cql_query(schema_module().task_update_statement(
        task_id, "failed", 100, now_ms, error_msg=reason))


def recover_open_tasks(fail_in_flight=False):
    """Replay what can be replayed and fail the rest with a reason. Returns (requeued, failed).

    Runs on the node that has just taken the dispatch candidacy, not at start-up on every
    node. That is the difference between recovery and a cleanup pass: the queues live with
    the candidacy, so the process that can act on these rows is the one that just acquired
    it -- and running it on every node at start-up meant three nodes rewriting the same rows
    while two of them had nowhere to put the work.

    `fail_in_flight` is the whole reason this takes an argument, and it is True on exactly one
    pass: the first after acquiring the candidacy. A `processing` row means a worker had the
    task, and on that first pass the worker in question was talking to a dispatcher that has
    stopped -- so the row is a corpse. On every later pass it means a worker *here* is running
    it right now, and failing it would mean this sweep killing every live task in the cluster
    on a timer. The two readings of one status are a few seconds apart, which is why the
    caller has to say which one it means rather than this inferring it.
    """
    open_tasks = read_open_tasks()
    if open_tasks is None:
        print("[Catalyst] Task recovery skipped: the task table could not be read. "
              "Nothing has been failed or replayed.")
        return 0, 0

    requeued = failed = 0
    for row in open_tasks:
        task_id = row.get("task_id")
        service = row.get("service")
        status = row.get("status")
        if not task_id:
            continue

        if status == "processing":
            if fail_in_flight:
                fail_task(task_id, IN_FLIGHT_REASON)
                failed += 1
            continue

        if service not in queues:
            # A pending task for a name nothing drains would be replayed on every pass
            # forever. Failing it says what is wrong, which is the thing an operator can
            # act on; leaving it pending says "slow".
            fail_task(task_id, "No queue named '%s' exists, so this task has no worker and "
                               "cannot run." % service)
            failed += 1
            continue

        with lock:
            already = task_id in queued_task_ids
        if already:
            continue

        try:
            task_payload = json.loads(row.get("payload") or "{}")
        except ValueError:
            task_payload = {}
        if not isinstance(task_payload, dict):
            task_payload = {}
        submit_task_to_memory(service, {
            "task_id": task_id,
            "action": row.get("action"),
            "payload": task_payload,
        })
        requeued += 1

    return requeued, failed


# How often the dispatcher looks for pending work it is not already holding. Slow on purpose:
# a submission that reached this node is queued the moment it is recorded, so the sweep is
# for the submissions that did not -- a task recorded by a node that was not the dispatcher,
# and the backlog left by a dispatcher that stopped.
DISPATCH_SWEEP_SECONDS = 15


def dispatch_thread_loop():
    """Hold the dispatch candidacy, and replay the backlog whenever it is acquired."""
    print("Catalyst dispatch thread started.")
    dispatch = candidacy(helios_zk.SERVICE_CATALYST_DISPATCH)
    was_dispatcher = None
    last_sweep = 0.0
    just_acquired = False
    while True:
        try:
            leading = dispatch.leading()
            if leading != was_dispatcher:
                # Whether this node holds the queues decides whether any task in the cluster
                # moves, so the transition earns a line. An operator looking at a stuck task
                # needs to be able to tell "the worker is busy" from "the queues are
                # somewhere else".
                print("Catalyst dispatch: %s" % (
                    "holding the queues on this node" if leading
                    else "standing by, another node holds the queues"))
                sys.stdout.flush()
                was_dispatcher = leading
                if leading:
                    # Immediately, not on the next sweep: everything recorded under the
                    # previous dispatcher is waiting, and the window between acquiring the
                    # candidacy and replaying is a window where the cluster looks idle.
                    last_sweep = 0.0
                    just_acquired = True
                    with lock:
                        queued_task_ids.clear()

            if not leading:
                time.sleep(2)
                continue

            if time.time() - last_sweep >= DISPATCH_SWEEP_SECONDS:
                last_sweep = time.time()
                requeued, failed = recover_open_tasks(fail_in_flight=just_acquired)
                just_acquired = False
                if requeued or failed:
                    print(f"[Catalyst] Recovered tasks: {requeued} re-queued, "
                          f"{failed} failed with a reason.")
                    sys.stdout.flush()
        except Exception as e:
            sys.stderr.write(f"Error in Catalyst dispatch loop: {e}\n")
        time.sleep(2)


def main():
    print("Catalyst task coordination service starting...")
    init_db_schema()

    # Start scheduler thread
    t = threading.Thread(target=scheduler_thread_loop, daemon=True)
    t.start()

    # Hold the queues, and replay what the last holder left behind.
    threading.Thread(target=dispatch_thread_loop, daemon=True).start()

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
