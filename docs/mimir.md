# Mimir (Health Checker Daemon)

Mimir is the background cluster health diagnostics and checking service for the HCI cluster.

> [!NOTE]
> **Name Origin:** In Norse mythology, **Mímir** is a renowned figure of wisdom who guards the well of knowledge. After he is beheaded in the Æsir–Vanir War, Odin carries around Mímir's embalmed head to recite secrets and advise him. Here, **Mimir** acts as the wise diagnostic check engine (equivalent to Nutanix NCC), auditing cluster health and advising administrators on system status and failures.

## Architecture & Lifecycle
- **Daemon Service**: Runs as a standalone python service (`/usr/local/bin/mimir`) managed by systemd (`mimir.service`).
- **Consensus Execution**: Mimir queries ZooKeeper status and only triggers checks on the node elected as the ZooKeeper leader to prevent concurrent execution.
- **Autostart Constraint**: Mimir is a static systemd service that is dynamically started/stopped by Spark commands (`cluster start` / `cluster stop`) and does not auto-start on boot unless the cluster is online.
- **Certificate Survey (every node, every 15 minutes)**: the one check that is deliberately *not* behind the leader election above. Each node classifies every certificate under `/etc/hci/spark/certs` and `/root/.certs` and upserts the result into `hydra.mimir_results` as `mtls_cert_expiration` / `security.mtls.certs`, so the console shows a continuously refreshed answer rather than whatever the last leader-triggered fan-out left behind. The certificates are per-node, and the day they lapse is the day the leader-only fan-out stops being able to reach any node at all. `PASS` above 30 days, `WARN` inside 30, `FAIL` inside 7 or already expired; an expiry date that cannot be parsed is `WARN`, never `PASS`. See [mtls_lifecycle.md](./mtls_lifecycle.md) for the renewal path this check exists to prompt.

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

### Key Diagnostic Checks

- **SSH Known Hosts Seeding (`ssh_known_hosts_seeding`)**:
  - **Category**: `services`
  - **Description**: Verifies passwordless SSH connectivity and mutual host key trust between all nodes in the cluster. It runs `ssh -o BatchMode=yes -o ConnectTimeout=2 -o StrictHostKeyChecking=yes {node_ip} exit` for each node IP to guarantee that libvirt live migrations won't fail due to SSH host key verification prompts.

- **Certificate Seeding (`certs_seeding_check`)**:
  - **Category**: `services`
  - **Description**: Audits the presence, cryptographic integrity, and configuration of all SSL/TLS and mTLS certificates across three primary service scopes:
    1. **mTLS Client**: Client tools configuration files (`/root/.certs/ca.crt`, `client.crt`, `client.key`).
    2. **Spark Daemon**: Host agent configurations (`/etc/hci/spark/certs/ca.crt`, `node.crt`, `node.key`).
    3. **Spectrum Ingress**: Web console web server files (`/etc/hci/spectrum/certs/server.crt`, `server.key`).
  - **Checks Executed**:
    * **File Presence**: Confirms all CA certificates, client/server certificates, and private keys exist.
    * **Private Key Permissions**: Verifies private key file permissions are secure (restricted to owner only, i.e. `600` or `400`).
    * **Modulus Verification**: Checks that each private key matches its corresponding certificate by validating that their RSA/EC modulus matches using `openssl x509 -modulus` and `openssl pkey -modulus`.
    * **Signature Trust**: Verifies client and node certificates are properly signed by their respective CAs using `openssl verify`.

- **Spark Service Watchdog (`watchdog_daemon_status`)**:
  - **Category**: `services` (stored under `service.spark.watchdog`)
  - **Description**: Verifies that spark-daemon's background watchdog thread — the only thing on a host that restarts a cluster service after it fails — is actually running. `systemctl is-active spark-daemon` does not answer this: the daemon keeps serving its mTLS API whether or not that thread is alive, which is exactly the "host is up, host is not working" case this check exists for.
  - **How it decides**, since the loop publishes no heartbeat:
    * **Preconditions**: the autostart thread returns *permanently*, without retrying, if `/etc/hci/cluster.json` is missing or `/run/hci/cluster_operation.lock` is held when spark-daemon starts. A lock older than the daemon is therefore a `FAIL` that needs `systemctl restart spark-daemon`, not just a lock release. A lock taken *after* startup is the watchdog pausing by design; held over 15 minutes it warns, over an hour it fails as stale.
    * **Its own announcement**: `[WATCHDOG] Starting service health watchdog` in the journal for the current `InvocationID`. Only conclusive where the journal still holds an `[AUTOSTART]` line from the same window — after log rotation, absence of the line proves nothing and is reported as such.
    * **Its effect**: a unit the loop supervises that has been inactive for more than three of its 30-second passes, while ZooKeeper reports the cluster `started`. This is the only way to see a loop that is wedged rather than dead — its `systemctl start` and `podman` calls carry no timeout, so one hung call stops the thread forever without raising.
  - A stopped cluster, a maintenance window and a unit down for a few seconds are all `PASS`: the watchdog restarting nothing is correct in each.

- **DRS/Migration Storage Capacity Gate (`drs_storage_capacity_check`)**:
  - **Category**: `services` (stored under `service.vali.drs_storage_gate`)
  - **Description**: Vali refuses a migration whose target cannot hold the guest's disk by comparing the disk size against `get_storage_free_space(target)`. This check verifies the gate can actually answer, by calling vali's own function and comparing its result with the free capacity sidon's `capacity` op reports for this node's extent store. It imports the deployed `vali` rather than re-implementing the parser, so it follows a fix instead of rotting into a false alarm.
  - **Fails** when the gate reads materially *more* free space than the pool has — its failure mode is approval, not refusal, so a gate that cannot parse the listing waves every migration through. Also fails when a thin pool drops below 5% free, and warns below 15% or when provisioned volumes exceed the pool's total size.
  - Diskless pools are excluded: they report 2^63-1 bytes free and store nothing, so counting them would show unlimited headroom on a full node.

- **Migration Lock Auditor (`migration_lock_status`)**:
  - **Category**: `services` (stored under `service.vali.migration_locks`)
  - **Description**: `hydra.vms.status` is the per-VM migration lock, taken by daruk's `/v1/vm/migrate-lock` LWT and released by `migrate-commit` or `migrate-unlock`. The lock's job changed with the storage layer, and the change is worth stating precisely: it no longer stands between two migrations and a corrupted disk, because a vdisk has exactly one owner per epoch and a deposed owner's writes are refused by every replica. What it guards now is the *control plane* — two concurrent migrations of one VM racing each other's ownership CAS, with the loser leaving a VM defined on a host that does not own its disk.
  - **Reports**: a lock held with no migration task in flight (orphaned — it refuses every later migration *and* every delete of that VM until cleared, and the output names the `migrate-unlock` call that clears it); two migration tasks in flight for one VM; a migration that has been running past vali's own 10-minute timeout; and, from `virsh domjobinfo`, a live migration running on this host while the lock is **not** held, which is the direction that corrupts data.
  - `hydra.catalyst_tasks` is only scanned when a VM claims a migration or libvirt reports one — it is a full partition scan of a table with 30-day retention, and it should not run hourly on every node just to confirm nothing is happening.
  - libvirt is asked through `virsh`, not through `systemctl is-active libvirtd`: a modular or socket-activated host leaves that unit `inactive` while `virsh` works perfectly, and gating on it would report "no migration can be running here" on a host that is migrating right now.

- **Sidon Control-Socket Latency (`sidon_latency_check`)**:
  - **Category**: `storage`
  - **Description**: Every other storage check asks the daemon a question and believes the answer. This one times the question, with a `ping` on `/run/sidon/control.sock`. There is no controller to ask any more; the equivalent chokepoint is the local daemon's own socket, which every storage operation on this node goes through — attach, detach, drain, fence. A daemon answering in twenty seconds is about to stop answering, and everything queued behind it is already stalled.
  - `WARN` above 5s, `FAIL` above 20s or if there is no answer at all. The check is bounded so a wedged daemon cannot stall the diagnostic run.

- **Stuck Catalyst Tasks (`stuck_tasks_check`)**:
  - **Category**: `services` (stored under `service.catalyst.stuck_tasks`)
  - **Description**: Flags tasks in `hydra.catalyst_tasks` that have been `pending` or `processing` for over 10 minutes, naming each one and how long it has been in flight.
  - This check previously answered `PASS` on every cluster it ran on. `created_at` is a CQL `timestamp`, which `SELECT JSON` renders as `'2026-08-21 20:58:49.309Z'`; `int()` on that raises, the surrounding `except` swallowed it, and every in-flight task was skipped. On the reference cluster it went green while three Dagur tasks had been pending for over an hour. A task whose age cannot be read is now `WARN`, not silently treated as fresh.

> [!IMPORTANT]
> Every check must have an entry in `mcli`'s `CHECK_ID_TO_FUNC`, and its category must contain a dot. `hydra.mimir_results` is partitioned by `category`: a check missing from the map is stored under the *invoked* scope instead, which both duplicates it across scopes and puts it in the partition the legacy cleanup deletes at the end of the same run. An undotted category would be deleted for the same reason. `test_mimir_results.py` asserts both.

---

## Command Examples & Syntax

### A. Health Diagnostic CLI (`mcli`)
The `mcli` tool is executed locally to run health diagnostics and inspect cluster-wide results:
```bash
# Run all registered health diagnostics immediately
mcli health_checks run_all

# List all registered health checks and their description
mcli health_checks list

# Run a specific check category (e.g. storage)
mcli health_checks run --check storage_capacity_check
```

### B. Checking Health Check Results in ScyllaDB
You can query the results of the health runs directly using `cqlsh`:
```bash
# Query recent warnings or failures recorded by Mimir
podman exec -i systemd-hydra-db cqlsh 127.0.0.1 -e "SELECT category, check_name, node_ip, status, output FROM hydra.mimir_results WHERE status IN ('FAIL', 'WARNING') ALLOW FILTERING;"

# Check the execution details of the hourly health checks schedule
podman exec -i systemd-hydra-db cqlsh 127.0.0.1 -e "SELECT * FROM hydra.mimir_schedules WHERE schedule_name = 'hourly_checks';"
```


---

## Technical Reference

For the internal code structure, class/function details, and execution flowcharts, see the [Technical Guide](./mimir_technical.md).
