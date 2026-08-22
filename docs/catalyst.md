# Catalyst (Task Coordinator & Scheduler Daemon)

Catalyst is the task orchestrator, coordinator, and execution scheduler for the Helios-HCI cluster. It is the direct equivalent of Nutanix **Task Manager / Catalyst**. It manages the lifecycle of asynchronous cluster-wide tasks, exposes a centralized HTTP API for queuing and long-polling task updates, and coordinates background cron schedules via Dagur.

> [!NOTE]
> **Name Origin:** In chemical kinetics, a **catalyst** accelerates reactions without being consumed. Similarly, the **Catalyst** daemon coordinates and fast-tracks the execution of long-running asynchronous tasks (like VM creations, migrations, and maintenance checks) across the cluster, keeping core APIs non-blocking.

---

## Architecture & Features

- **Daemon Service**: Runs as a local Python service (`/usr/local/bin/catalyst`) managed by systemd (`catalyst.service`), binding to `127.0.0.1:9091`.
- **Task Schema & Persistence**: Tasks are persisted in the ScyllaDB table `hydra.catalyst_tasks`. This ensures tasks can be tracked across node failovers and server restarts.
- **Service Queues**: Distributes tasks to specialized background workers via in-memory queues:
  - `vali`: For VM scheduling, placement, load balancing, and maintenance migrations.
  - `dagur`: For cron scheduling and maintenance task execution.
  - `spark`: For node bootstrap and remote systemd control.
- **Task Long Polling**: Exposes endpoints for worker long-polling and client completion syncing, avoiding unnecessary database CPU polling overhead.
- **Cron Scheduler Thread**: Runs a background loop that evaluates clustered cron job definitions in `hydra.dagur_schedules` (maintained by Dagur) and dispatches execution tasks to the queue when intervals elapse.

---

## Claiming a Scheduler Tick

The scheduler thread reads `last_run_epoch`, decides a job is due, and writes the current
time back. That read-modify-write used to be blind, so it submitted the job **once per
scheduler that reached the row** — two Dagur runs of the same backup, the same scrub, the
same compaction, against the same volumes at the same moment.

Two schedulers is not a hypothetical. `is_zookeeper_leader()` probes ZooKeeper's four-letter
`stat` and, when the leader does not answer on port 9091, falls back to *"lowest node with
9091 open"*. A ZooKeeper that is slow, restarting or partitioned gives that answer to two
nodes at once, and both then believe they are the only scheduler.

`claim_scheduled_run()` takes the tick through Daruk's
[`POST /v1/schedule/claim-job`](./daruk.md#claiming-a-scheduler-tick), whose
`IF last_run_epoch = ?` makes the claim and the clock one Paxos round:

```
read hydra.dagur_schedules  →  job is due  →  claim the tick  →  submit to Dagur
                                                    │
                                                    └─ refused: another Catalyst
                                                       already has it. Skip.
```

Three behaviours the loop depends on:

* **The claim comes before the work.** Nothing is written to `hydra.catalyst_tasks` and
  nothing is queued until the tick is ours.
* **An unanswerable claim skips the tick.** If Daruk cannot be reached the job does *not*
  run: a skipped tick runs on the next pass ten seconds later, and a tick run twice cannot
  be taken back.
* **The expected clock is the value that was read, nulls included.** `last_run_epoch` is
  null — not `0` — for a schedule inserted without one, and `IF last_run_epoch = 0` does not
  match a null. (Reading it as `0` also used to raise `TypeError` inside the loop's
  `try`, which cost every *other* schedule that pass, silently.)

> [!NOTE]
> Spectrum runs its own copy of this loop over the same table, and Mimir runs the same shape
> over `hydra.mimir_schedules`. Both still write the clock blind and can race a Catalyst
> that is claiming correctly. `POST /v1/schedule/claim-check` exists for the Mimir side.

---

## API Endpoints Reference

Catalyst binds strictly to `127.0.0.1` and is accessed internally by Prism/Spectrum:

### 1. GET `/api/v1/queues/<service>`
Long-polls pending tasks from the specified service queue (e.g. `vali`, `dagur`). Blocks for up to 30 seconds if empty.
- **Response (200 OK)**: Task JSON payload.
- **Response (204 No Content)**: Queue is empty.

### 2. POST `/api/v1/tasks/submit`
Submits a new task to the queue and persists it as `pending` in ScyllaDB.
- **Request Body**:
  ```json
  {
    "service": "vali",
    "action": "migrate",
    "payload": {
      "vm_name": "server2022",
      "target_host": "10.10.102.122"
    }
  }
  ```
- **Response (200 OK)**:
  ```json
  {
    "task_id": "8f8b8a8b-1234-5678-abcd-ef1234567890",
    "status": "pending"
  }
  ```

### 3. GET `/api/v1/tasks/status/<task_id>`
Long-polls for completion or failure of a specific task. Blocks for up to 30 seconds if the task is still running.
- **Response (200 OK)**:
  ```json
  {
    "task_id": "8f8b8a8b-1234-5678-abcd-ef1234567890",
    "status": "completed",
    "progress": 100
  }
  ```

### 4. POST `/api/v1/tasks/update`
Allows system daemons and workers to update the progress, status, and optional error messages/results of a task.
- **Request Body**:
  ```json
  {
    "task_id": "8f8b8a8b-1234-5678-abcd-ef1234567890",
    "status": "processing",
    "progress": 50,
    "error_msg": "",
    "result": {}
  }
  ```
- **Response (200 OK)**:
  ```json
  {
    "status": "ok"
  }
  ```

---

## CLI Integration (`catcli`)

Administrators can use the `catcli` utility on the host console to interact directly with Catalyst:

```bash
# List all active and historical tasks
catcli list

# View the status of a specific task
catcli status <task_id>

# Submit a task to a service queue
catcli submit --service vali --action balance --payload '{}'

# Force a dns/ntp sync task
catcli sync

# Prune completed and failed tasks
catcli cleanup
```


---

## Technical Reference

For the internal code structure, class/function details, and execution flowcharts, see the [Technical Guide](./catalyst_technical.md).
