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
