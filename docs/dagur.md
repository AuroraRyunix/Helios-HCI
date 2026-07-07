# Dagur (Task Scheduler Daemon)

Dagur is the background central task runner and scheduler service for the HCI cluster. It is the direct equivalent of Nutanix **Chronos**.

> [!NOTE]
> **Name Origin:** In Norse mythology and the Icelandic language, **Dagur** translates directly to **Day** (representing time and daily schedules). It serves as the clustered cron manager to orchestrate time-based background tasks.

## Architecture & Lifecycle
- **Daemon Service**: Runs as a standalone python service (`/usr/local/bin/dagur`) managed by systemd (`dagur.service`).
- **Consensus Execution**: To prevent duplicate job runs, Dagur only executes tasks on the node elected as the ZooKeeper leader.
- **Autostart Constraint**: Dagur is a static systemd service that is dynamically started/stopped by Spark commands (`cluster start` / `cluster stop`) and does not auto-start on boot unless the cluster is online.

## Database Schema
Dagur relies on the following ScyllaDB tables in the `hydra` keyspace:
- `hydra.dagur_schedules`: Stores details of scheduled jobs, task types, cron expressions, intervals, and commands.
- `hydra.dagur_runs`: Logs the history of job execution runs, exit codes, status, and command output.

## Default Schedules
Dagur queries ScyllaDB and triggers the following default background maintenance jobs:

| Job Name | Task Type | Cron Expression | Interval | Command | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `mimir_diagnostics` | `mimir_health` | `0 * * * *` | 1 hour | `/usr/local/bin/mcli health_checks run_all` | Runs cluster health checks. |
| `storage_scrub` | `storage_scrub` | `0 */6 * * *` | 6 hours | `podman exec systemd-linstor-controller linstor resource list` | Verifies Linstor/DRBD storage volume state. |
| `db_compaction` | `db_compaction` | `0 */12 * * *` | 12 hours | `nodetool compact` | Compacts metadata database. |
| `storage_auto_heal` | `storage_auto_heal` | `0 1 * * *` | 24 hours | `N/A (Native)` | DRBD kernel replication natively handles replication synchronization. |

---

## Technical Execution & Scheduling Loop

Dagur operates as a distributed execution worker by doing the following:
1. **Quorum Check**: Queries ZooKeeper status to verify it is the active leader (only the leader executes jobs to prevent duplicate run conflicts).
2. **Long-poll Catalyst**: Performs a long-poll request to Catalyst on `GET /api/v1/queues/dagur`.
3. **Execute Job**: When Catalyst dispatches a task (scheduled by Catalyst's `scheduler_thread_loop` querying `hydra.dagur_schedules`), Dagur spawns a background thread `execute_dagur_job_thread` to execute the command using Spark mTLS on localhost.
4. **Progress Updates & Logging**:
   - Reports progress and status updates back to Catalyst via `POST /api/v1/tasks/update`.
   - Inserts run results, exit codes, and output logs into `hydra.dagur_runs`.

---

## Command Examples & Syntax

### A. Managing the Dagur Service
Control the cron daemon on the host:
```bash
# Check service status
systemctl status dagur

# Follow scheduling daemon logs
journalctl -u dagur -f --no-pager
```

### B. Querying Schedules and Runs in ScyllaDB
You can inspect active cron schedules and execution history directly using `cqlsh`:
```bash
# View all configured background schedules
podman exec -i systemd-hydra-db cqlsh 127.0.0.1 -e "SELECT schedule_name, cron_expr, command, enabled FROM hydra.dagur_schedules;"

# Query the last 5 execution runs logged by Dagur
podman exec -i systemd-hydra-db cqlsh 127.0.0.1 -e "SELECT run_id, schedule_name, status, exit_code, start_time, duration_ms FROM hydra.dagur_runs LIMIT 5;"
```

### C. Manually Adding a Scheduled Task
To register a new background task, insert a new row into the `dagur_schedules` table:
```bash
# Example: Add a daily backup sync task at 2:00 AM
podman exec -i systemd-hydra-db cqlsh 127.0.0.1 -e "INSERT INTO hydra.dagur_schedules (schedule_name, task_type, cron_expr, command, enabled) VALUES ('daily_backup', 'backup', '0 2 * * *', '/usr/local/bin/backup_sync.sh', true);"
```


---

## Technical Reference

For the internal code structure, class/function details, and execution flowcharts, see the [Technical Guide](./dagur_technical.md).
