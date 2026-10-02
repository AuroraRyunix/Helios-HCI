# Hylia (HA Rolling Upgrade & LCM) - Technical Documentation

This document details the internal technical structure, functions, flowcharts, and mindmaps of the Hylia rolling upgrade daemon.

## Technical Mindmap

```mermaid
mindmap
  root((Hylia Daemon))
    Consensus & HA
      ZooKeeper port 2181 mode leader checks
      Standby backup resumes state from ScyllaDB
    Database Schema
      hydra.hylia_jobs (upgrade state details)
      hydra.hylia_logs (job output log history)
    Upgrade Verification
      Checksum validation (manifest.json SHA-256)
      Extraction of update zip files
      get_service_build_number via __build__ flag
    Host Actions
      Enter Maintenance (live migration via Vali)
      Base64 chunk transfer & remote execution
      Spectrum container rebuilding
      Host reboot and connection polling
      Leave Maintenance
    Storage Guard
      verify_node_storage_health
      Asks sidon for capacity and its vdisk list
      Refuses a node whose store is unmounted, full, or holding a degraded vdisk
    Console Entry Points
      main(argv) - no arguments is the daemon
      --load-package validates, distributes, records the job
      --start-upgrade marks STARTING and watches to a verdict
      Progress reported to CATALYST_TASK_ID when run as a task
```

## Function & Logic Breakdown

### `run_command_local(cmd)`
- Executes shell commands on the local host namespace. Returns status and output buffers.

### `run_remote_spark(ip, command, timeout=45)`
- Runs shell commands on target cluster hosts using Spark's REST API port `9099` over TLS.
- Re-attempts execution up to 5 times (2s intervals) to prevent network hiccups during reboots.

### `run_mtls_spark_api(ip, path, payload, method="POST")`
- Directly submits REST HTTP request calls targeting Spark's local or peer API. Used to coordinate scheduler tasks.

### `run_cql_query(cql_query)`
- Runs queries in ScyllaDB (looks for the local Daruk proxy port `9043`, falling back to `podman exec systemd-hydra-db cqlsh`).

### `get_cluster_hosts()`
- Parses `/etc/hci/cluster.json` to resolve IP addresses of cluster nodes.

### `get_zookeeper_leader_ip()`
- Scans nodes on port `2181` to locate the active ZooKeeper consensus leader.

### `is_zookeeper_leader()`
- Compares ZooKeeper leader IP with local hypervisor IP.

### `log_upgrade(job_id, line)`
- Writes log messages to standard out.
- Appends log records directly to the `hydra.hylia_logs` ScyllaDB table under the active `job_id` so all nodes can coordinate logs.

### `validate_and_extract_zip(zip_path, extract_dir)`
- Wipes `/tmp/helios_update` and extracts update package files.
- Parses `manifest.json` and performs SHA-256 hash checksum tests on all included components.

### `get_service_build_number(target_path)`
- Scans component python file headers for a `__build__` string parameter value.

### `verify_node_storage_health(job_id, node_ip, hostname)`
- Health guard. Runs before entering maintenance and leaving.
- Asks the target's sidon for `capacity` and `list` over spark's mTLS API.
- Refuses when the daemon does not answer, when the extent store reports no capacity (which usually means it is not mounted), when it is ≥95% full, or when the node holds a vdisk marked degraded.
- **It no longer waits for a resync**, because there is nothing to resync: extent groups are immutable, so a returning node's copies are either correct or absent, and Purah restores absent ones in the background off the hot path. The DRBD version polled `drbdadm status` for minutes waiting for every peer to reach `UpToDate`.

### `hylia_rolling_upgrade(job_id)`
- Asynchronous orchestration thread:
  1. Loads manifest payload. Resolves whether it is a `FAST PATCH` (reboot not required) or `ROLLING REBOOT` upgrade.
  2. Transitions database job state to `UPGRADING`.
  3. Iterates over target hosts.
  4. Wait for other cluster hosts to reach a stable state `NORMAL`.
  5. Evacuates hosts by calling Vali's `/api/v1/host/maintenance` endpoint (`action="enter"`).
  6. Copies files via base64 encoded chunks.
  7. If Spectrum is upgraded, rebuilds the container on the target node.
  8. Triggers reboot and polls connection.
  9. Verifies storage health using `verify_node_storage_health()`.
  10. Clears maintenance mode (`action="leave"`).
  11. Transitions database job state to `COMPLETED` when all nodes finish.

### `hylia_loop()`
- Every 5 seconds, if it is the leader, queries `hydra.hylia_jobs`.
- If an active job is `STARTING`, starts the upgrade thread. If a job is `UPGRADING` and matches its node execution scope, resumes the execution thread (handles standby coordinator resume).

### `call_catalyst_api(path, payload=None, method="GET", address=None)` / `report_task_progress(...)`
- Mutual-TLS client for Catalyst on port 9091, with the same certificate material every
  other inter-node call uses. `get_catalyst_target_ip()` addresses the leader by its own
  IP rather than by loopback, because loopback is in no node's `subjectAltName` and the
  leader is usually this node.
- `report_task_progress` is a **no-op without `CATALYST_TASK_ID`** in the environment: run
  by hand there is no task to report against, and inventing one puts a row in the console's
  task log that no operator asked for. A failed report is swallowed — the exit code is the
  verdict that matters, and losing a progress tick must not fail an upgrade.

### `distribute_package(zip_path)`
- Copies a validated archive to every other node in base64 chunks over spark, then re-runs
  `validate_and_extract_zip` on the far side so a node never trusts an archive it has not
  checked itself.
- Moved here from `spectrum_server.py`, which had it only because that is where the upload
  landed; every line of it was already hylia's. A node that cannot be reached is collected
  and the rest continue — stopping at the first would leave the cluster worse off than
  finishing — and the list of failures is what makes `--load-package` exit non-zero.

### `load_package(zip_path)` / `start_upgrade(job_id)` / `main(argv)`
- The two Catalyst entry points and the argument dispatch. See
  [hylia.md](./hylia.md#the-two-console-entry-points) for what each guarantees and why.
- `main(argv)` with no arguments is `hylia_loop()`, unchanged: that is what
  `hylia.service` runs. An unknown option exits 2 rather than falling through to the
  daemon, so a typo in a task's command cannot start a second upgrade daemon on the leader.

### `upgrade_percent(job)`
- Whole nodes finished, as a percentage of the target list. A `current_node` outside that
  list contributes nothing: "we do not know where it is" and "it has just begun" are
  different states, and only one of them should be drawn as a bar that is about to move.
