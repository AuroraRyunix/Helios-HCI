# Catalyst (Task Coordinator & Scheduler Daemon)

Catalyst is the task orchestrator, coordinator, and execution scheduler for the Helios-HCI cluster. It is the direct equivalent of Nutanix **Task Manager / Catalyst**. It manages the lifecycle of asynchronous cluster-wide tasks, exposes a centralized HTTP API for queuing and long-polling task updates, and coordinates background cron schedules via Dagur.

> [!NOTE]
> **Name Origin:** In chemical kinetics, a **catalyst** accelerates reactions without being consumed. Similarly, the **Catalyst** daemon coordinates and fast-tracks the execution of long-running asynchronous tasks (like VM creations, migrations, and maintenance checks) across the cluster, keeping core APIs non-blocking.

---

## Architecture & Features

- **Daemon Service**: Runs as a local Python service (`/usr/local/bin/catalyst`) managed by systemd (`catalyst.service`), binding to `127.0.0.1:9091`.
- **Task Schema & Persistence**: Tasks are persisted in the ScyllaDB table `hydra.catalyst_tasks`,
  with a parent, an owning component, a per-component sequence id and a completion time —
  see [The task tree](#the-task-tree) and [Recovery](#recovery).
- **Which node holds the queues**: the one holding the `catalyst-dispatch` candidacy, which is
  an election of its own and *not* the ZooKeeper ensemble's leader. The winner publishes its
  address, and every submitter and every worker resolves the queues by reading it. See
  [service_leadership.md](./service_leadership.md).
- **Service Queues**: Distributes tasks to specialized background workers via in-memory queues:
  - `vali`: For VM scheduling, placement, load balancing, and maintenance migrations.
  - `dagur`: For cron scheduling and maintenance task execution, and for any console
    operation whose code already exists on the host as a command — the Urbosa bootstrap
    and teardown, and hylia's package and upgrade entry points.
  - `lanayru`: For building and tearing down the guest Kubernetes cluster. Drained by the
    console backend rather than by a daemon of its own, because `lanayru.py`'s workers
    import half of `spectrum_server.py`.
  - `spark`: For node bootstrap and remote systemd control.
- **Task Long Polling**: Exposes endpoints for worker long-polling and client completion syncing, avoiding unnecessary database CPU polling overhead.
- **Cron Scheduler Thread**: Runs a background loop that evaluates clustered cron job definitions in `hydra.dagur_schedules` (maintained by Dagur) and dispatches execution tasks to the queue when intervals elapse.

---

## Claiming a Scheduler Tick

The scheduler thread reads `last_run_epoch`, decides a job is due, and writes the current
time back. That read-modify-write used to be blind, so it submitted the job **once per
scheduler that reached the row** — two Dagur runs of the same backup, the same scrub, the
same compaction, against the same volumes at the same moment.

Two schedulers is not a hypothetical, and it stays possible with a real election. The
scheduler now stands for the `catalyst-scheduler` candidacy, which answers correctly when it
answers — but the answer is read from the ensemble at one instant and acted on at a later one.
A scheduler that reads "I lead", then stalls long enough to lose its session, is still inside
the pass it started. (Before the candidacy it was worse: `is_zookeeper_leader()` probed
ZooKeeper's four-letter `stat` and, when the leader did not answer on port 9091, fell back to
*"lowest node with 9091 open"* — an answer a slow or partitioned ZooKeeper gave to two nodes at
the same time.)

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

## The task tree

A task used to be a flat row: a service, a verb, a status and two timestamps. Three things a
real task framework records were missing, and all three start at the table.

Measured against a Nutanix cluster's Ergon, read directly with `ecli task.list`:

| Ergon | Helios | Why it matters |
| :--- | :--- | :--- |
| Task UUID | `task_id` | was already there |
| **Parent Task UUID** | `parent_task_id` | a rolling upgrade *is* a tree — one job per node, each with steps — and was reported as a list of unrelated rows |
| Component (`Acropolis`, `Narsil`) | `component` | not the same as `service`: a Hylia upgrade step runs on the `dagur` queue, so `service` attributes it to the cron runner |
| Sequence-id (`255227`, per component) | `sequence_id` | two tasks created in the same millisecond had no defined order at all |
| Type (`kVmSetPowerState`) | `task_type` | every scheduled job has the action `execute`; the type is what says what each one did |
| Creation + completion, UTC | `created_at` + `completed_at` | `updated_at` moves on every progress report, so a completed task's duration was unknowable from the row |

Added by migration `0011-catalyst-task-tree`. Every column is nullable and nothing rewrites an
existing row: a task recorded before the migration keeps reading correctly with all of them
null, which is the truth about it. Backfilling a component or a sequence id would be inventing
a fact about work that has already happened.

**`parent_task_id` was a key inside the JSON `payload` before it was a column**, because there
was nowhere else to put it. Both are still read — column first — and will be for as long as the
thirty-day retention window holds rows written the old way.

### The sequence id

Per component, and claimed rather than incremented. `hydra.catalyst_task_sequence` holds one
row per component with the next number to hand out, and a submitter takes it through Daruk's
`POST /v1/catalyst/claim-sequence`, whose `IF next_sequence_id = ?` makes the read and the
write one Paxos round. A blind `n + 1` is a lost update, and a lost update here is two tasks
carrying the same number — the single property the column exists to provide.

A refused claim is retried with the value the refusal carries back. A claim that **cannot** be
made at all yields no number, and the task is submitted anyway: the number is how an operator
orders a component's history, not how the cluster executes anything, and a task framework that
refuses work because a counter was contended is worse than a task with no number.

Not a cluster-wide counter, deliberately. One sequence would have to be claimed by every
submitter in the cluster for every task, which is a permanently contended compare-and-swap on
the submission path.

---

## Recovery

What this replaces: `recover_stuck_tasks()` ran at every start, on every node, and marked every
`pending` and every `processing` row `failed` with *"Task aborted due to system daemon
restart."* A task submitted a second before a restart was recorded, aborted, and never
attempted. That is data loss, arrived at deliberately, in the function named after recovering
from it.

The two states are not the same thing and are no longer handled the same way:

* **`pending`** — nothing has happened. No worker has seen it, so nothing in the cluster has
  been touched and replaying it is simply doing what was asked. It is **re-queued**.
* **`processing`** — a worker had it. What it did before the dispatcher holding its queue
  stopped is not recorded, and the actions behind these rows are not replayable on a guess: a
  half-finished live migration replayed is a second migration of a guest that may already be
  running elsewhere. It is **failed, with a reason that says it was in flight** and that how
  far it got is unknown.
* **`pending` for a queue nothing drains** — failed, naming the missing queue. Left pending it
  would be replayed on every sweep forever and read as "slow".

Recovery runs on the node that has just **acquired** the dispatch candidacy, not at start-up on
every node, and then on a slow sweep while it holds it. That is the difference between recovery
and a cleanup pass: the queues live with the candidacy, so the process that can act on these
rows is the one that just took it.

The `processing` rule applies on **exactly one pass**: the first after acquiring the candidacy.
`processing` reads two ways a few seconds apart. On that first pass it means a worker was
talking to a dispatcher that has stopped, so the row is a corpse. On every later pass it means a
worker here is running the task right now — and failing those would be the sweep killing every
live task in the cluster on a fifteen-second timer. A `processing` row whose worker dies
*while* this node holds the queues is therefore left alone here and surfaces through Mimir's
stuck-task check instead; it is not this loop's to judge.

It also closes a gap that persistence did not previously pay for. A submission is recorded by
whichever Catalyst receives it, whether or not that node holds the queues — so a submission
that reaches the wrong node is now work the dispatcher's sweep finds and replays, where before
it was queued where nothing drained it and sat `pending` forever.

An unreadable task table fails nothing and replays nothing. "The database did not answer" and
"there is nothing to replay" are different facts, and acting on the first as though it were the
second fails every task in the cluster.

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

## A queue with no worker is worse than a missing queue

A service name is a queue, and a queue only moves if some daemon is long-polling
`/api/v1/queues/<name>` on the node holding leadership. Submitting to a name that is in the
dict and has nobody draining it writes the row, returns a task id, and then nothing ever
happens — and `pending` on the console's task ring is indistinguishable from a task that is
merely slow. Submitting to a name that is *not* in the dict is a `404` the caller can at
least report.

`spark` is currently in that first category: it is declared here and nothing polls it. It
is left in place because removing a queue changes what a submission means, which belongs
with whichever component finally claims the name.

The console's own guard is `SpectrumPhx.Catalyst.services/0`, which lists only the queues
that are drained and refuses everything else before a request is made.
`test_console_tasks.py` asserts that list against this daemon's queue dict and against the
daemons that poll them, so the three files remain one statement.

## Commands as tasks

`dagur`/`execute` is the general "do a thing to this cluster" task: dagur runs the command
on the leader through its spark-daemon and reports the exit code back here, so a non-zero
exit becomes a `failed` row carrying the command's own output. Two fields on the payload
matter for anything longer than a maintenance script:

* **`timeout`** — seconds the command may run for. spark-daemon applies **45 seconds** to a
  request that does not carry one and kills the command there, so a cluster-wide operation
  that omits it is capped at forty-five seconds and comes back as a timeout from a daemon
  the caller never mentioned.
* **`reports_progress`** — the command updates its own task, so dagur's ticker stands down.
  The ticker climbs to 95% in ten seconds and stays there, which is honest enough for a job
  with nothing better to say and actively wrong beside one that knows where it is: two
  writers on the same column have a true 20% overwritten by an invented 95% a second later.

Dagur also puts `CATALYST_TASK_ID` in the executed command's environment, which is how a
command can address the task it is running as. Nothing is required to read it.

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
