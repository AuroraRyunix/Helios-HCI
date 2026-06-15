# Mimir (Health Checker Daemon)

Mimir is the background cluster health diagnostics and checking service for the HCI cluster.

## Architecture & Lifecycle
- **Daemon Service**: Runs as a standalone python service (`/usr/local/bin/mimir`) managed by systemd (`mimir.service`).
- **Consensus Execution**: Mimir queries ZooKeeper status and only triggers checks on the node elected as the ZooKeeper leader to prevent concurrent execution.
- **Autostart Constraint**: Mimir is a static systemd service that is dynamically started/stopped by Spark commands (`cluster start` / `cluster stop`) and does not auto-start on boot unless the cluster is online.

## Database Schema
Mimir relies on the following ScyllaDB tables in the `hydra` keyspace:
- `hydra.mimir_schedules`: Stores details of scheduled diagnostic jobs, category parameters, enabled status, and last run timestamp.
- `hydra.mimir_results`: Stores history of Mimir health check diagnostic outputs, status (PASS, WARNING, FAIL), check name, and timestamps.

## Default Schedules
Mimir checks are triggered according to schedules defined in the database:

| Schedule Name | Category | Interval | Command Triggered | Description |
| :--- | :--- | :--- | :--- | :--- |
| `hourly_checks` | `all` | 1 hour | `/usr/local/bin/mcli health_checks run_all` | Runs all diagnostic health checks cluster-wide. |

The triggered execution calls `mcli` tool which performs node check evaluations (SSH connections, disk capacity, process health, mount checks, replica statuses) and records diagnostic output to `hydra.mimir_results`.
