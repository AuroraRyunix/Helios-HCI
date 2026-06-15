# Dagur (Task Scheduler Daemon)

Dagur is the background central task runner and scheduler service for the HCI cluster.

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
| `storage_scrub` | `storage_scrub` | `0 */6 * * *` | 6 hours | `podman exec systemd-aether gluster volume status` | Verifies GlusterFS volume state. |
| `db_compaction` | `db_compaction` | `0 */12 * * *` | 12 hours | `nodetool compact` | Compacts metadata database. |
| `storage_auto_heal` | `storage_auto_heal` | `* * * * *` | 1 minute | `/usr/local/bin/hci-auto-heal` | Fixes GlusterFS metadata mismatch attributes. |
