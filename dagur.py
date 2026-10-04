#!/usr/bin/env python3
import sys
import os
import re
import json
import time
import socket
import helios_zk
import urllib.request
import ssl
import threading
import uuid

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

# How long a job's command may run when the task does not say. Scheduled maintenance --
# a scrub, a repair, a health sweep -- is measured in minutes, and the console now submits
# cluster-wide operations through this same queue. The task can raise or lower it with a
# `timeout` in its payload; this is only the number for a task that does not care.
DEFAULT_JOB_TIMEOUT = 3600

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

def run_remote_spark(ip, command, timeout=DEFAULT_JOB_TIMEOUT):
    """Run one command on a node and return (returncode, stdout, stderr).

    The timeout is passed to spark-daemon rather than left out. Omitting it does not mean
    "no limit": spark-daemon defaults an absent `timeout` to 45 seconds and kills the
    command there, so every job dagur has ever run was silently capped at forty-five
    seconds of work and anything longer came back as "Command timed out" from a daemon the
    caller never named. That is a control-plane number, and a scheduled job is not a
    control-plane call.

    The urllib read gets the same budget plus a margin, so the socket outlives the command
    it is waiting for -- otherwise a job that legitimately runs to its limit is reported as
    a transport failure rather than as its own exit code.
    """
    ip, verify_identity = spark_endpoint(ip)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = verify_identity

    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command, "timeout": timeout}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=timeout + 15) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)









# `get_zookeeper_leader_ip` used to live here, as one of nine copies of the same loop. Its
# only caller was the leadership gate below, and the gate no longer asks who leads the
# ensemble -- so the copy went with it. The question itself is still answerable, by
# `helios_zk.leader_ip`, for the callers that genuinely mean it.


_CANDIDACIES = {}
_CANDIDACY_LOCK = threading.Lock()


def candidacy(service):
    """One candidacy per job, kept for the life of the process.

    Draining Catalyst's `dagur` queue used to be `get_zookeeper_leader_ip() == LOCAL_IP`:
    whichever node the ZooKeeper ensemble had elected ran every leader-only workload in the
    cluster, and an ensemble election -- a restart, a blip, a rolling upgrade -- moved them
    all at once. Running maintenance jobs has nothing to do with leading ZooKeeper.
    """
    with _CANDIDACY_LOCK:
        existing = _CANDIDACIES.get(service)
        if existing is None:
            existing = helios_zk.cluster_candidacy(service, LOCAL_IP)
            _CANDIDACIES[service] = existing
        return existing


def get_catalyst_target_ip():
    """The address of the Catalyst holding the queues.

    This used to be loopback unconditionally, which was correct only because the worker and
    the queues were both pinned to the ZooKeeper leader. They are separately elected now, so
    the queue this worker drains and the Catalyst it reports results to may be on another
    node -- and both have to be the *same* node, or a result is reported to a process that
    is not waiting for it.
    """
    published = candidacy(helios_zk.SERVICE_CATALYST_DISPATCH).leader_identity()
    if published:
        address = published.decode("utf-8", "replace").strip()
        if address:
            return address
    # No published answer means an unreachable ensemble. Loopback is the honest fallback:
    # this node's own Catalyst is the only one it can be sure of, and the dispatch check on
    # the queue endpoint refuses the poll rather than serving an empty queue.
    return "127.0.0.1"


def call_catalyst_api(path, payload=None, method="GET"):
    """Call the Catalyst holding the queues, over mutual TLS.

    Catalyst dispatches cluster work and requires a certificate this cluster's CA signed.
    Loopback is reached by this node's own address because that is what its certificate
    names -- see spark_endpoint() for the same reasoning applied to spark-daemon.
    """
    import urllib.request
    import json
    address, verify_identity = spark_endpoint(get_catalyst_target_ip())
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                         cafile="/etc/hci/spark/certs/ca.crt")
    context.load_cert_chain(certfile="/etc/hci/spark/certs/node.crt",
                            keyfile="/etc/hci/spark/certs/node.key")
    context.check_hostname = verify_identity
    url = f"https://{address}:9091{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=35) as response:
            if response.status == 204:
                return 204, None
            res = json.loads(response.read().decode("utf-8"))
            return response.status, res
    except Exception as e:
        return -1, str(e)

def insert_dagur_run(job_name, start_time, run_id, end_time, status, exit_code, output):
    clean_output = output.replace("'", "''").replace("\\", "\\\\")
    job_name = cql_escape(job_name)
    status = cql_escape(status)
    cql = f"""
    INSERT INTO hydra.dagur_runs (job_name, start_time, run_id, end_time, status, exit_code, output)
    VALUES ('{job_name}', {start_time}, {run_id}, {end_time}, '{status}', {exit_code}, '{clean_output}');
    """
    run_cql_query(cql)

def execute_dagur_job_thread(task_id, job_name, command,
                             timeout=DEFAULT_JOB_TIMEOUT, reports_progress=False):
    run_id = str(uuid.uuid4())
    start_time = int(time.time() * 1000)
    job_name_for_cql = cql_escape(job_name)

    cql_start = f"""
    INSERT INTO hydra.dagur_runs (job_name, start_time, run_id, status, exit_code, output)
    VALUES ('{job_name_for_cql}', {start_time}, {run_id}, 'RUNNING', -1, 'Job started...');
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

    # The ticker is a guess -- it climbs to 95 in ten seconds and sits there however long
    # the job runs. That is honest enough for a job with nothing better to say, and it is
    # actively wrong for one that knows where it is: two writers on the same `progress`
    # column would have a real 20% overwritten by an invented 95% a second later. A task
    # that says it reports its own progress gets the column to itself.
    ticker_thread = None
    if not reports_progress:
        ticker_thread = threading.Thread(target=progress_ticker)
        ticker_thread.start()

    try:
        # The task id travels into the command's environment, so a command that wants to
        # report real progress can address the task it is running as. Nothing is required
        # to read it; a command that ignores it behaves exactly as before.
        exit_code, stdout, stderr = run_remote_spark(
            "127.0.0.1",
            f"CATALYST_TASK_ID={task_id} {command}" if task_id else command,
            timeout=timeout)
        out_str = stdout + stderr
        status = 'SUCCESS' if exit_code == 0 else 'FAILED'
    except Exception as e:
        exit_code = -1
        out_str = f"Execution failed: {str(e)}"
        status = 'FAILED'
    finally:
        stop_progress_ticker.set()
        if ticker_thread:
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
    worker = candidacy(helios_zk.SERVICE_DAGUR_QUEUE)
    was_worker = None
    while True:
        try:
            leading = worker.leading()
            if leading != was_worker:
                # The difference between "the job is slow" and "no runner is running
                # anywhere" is otherwise invisible from the console.
                print("Dagur Catalyst worker: %s" % (
                    "draining the queue, this node holds the dagur-queue candidacy" if leading
                    else "standing by, another node holds the dagur-queue candidacy"))
                sys.stdout.flush()
                was_worker = leading
            if not leading:
                time.sleep(2)
                continue
                
            status, res = call_catalyst_api("/api/v1/queues/dagur")
            if status == 200 and res:
                task_id = res.get("task_id")
                action = res.get("action")
                payload = res.get("payload", {})
                
                job_name = payload.get("job_name")
                command = payload.get("command")
                timeout = payload.get("timeout") or DEFAULT_JOB_TIMEOUT
                try:
                    timeout = max(1, int(timeout))
                except (TypeError, ValueError):
                    timeout = DEFAULT_JOB_TIMEOUT
                reports_progress = bool(payload.get("reports_progress"))

                print(f"[Dagur] Received task from Catalyst: {job_name} ({action})")
                t = threading.Thread(
                    target=execute_dagur_job_thread,
                    args=(task_id, job_name, command, timeout, reports_progress),
                    daemon=True)
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
