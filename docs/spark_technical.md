# Spark (Cluster Service & Bootstrap Manager) - Technical Documentation

This document details the internal technical structure, functions, flowcharts, and mindmaps of the Spark CLI manager (`spark.py`) and the Spark Daemon API (`spark_daemon_decoded.py`).

## Technical Mindmap

```mermaid
mindmap
  root((Spark System))
    Spark CLI (spark.py)
      Status checking
        Resolves active containers & PIDs
        Tests local TCP service listeners
        Queries maintenance status from /etc/hci/maintenance.state
      JSON output mode (--json)
      Service Controls
        Starts ZooKeeper & Spark Daemon
        Stops/restarts local cluster workloads
    Spark Daemon (spark_daemon_decoded.py)
      mTLS Web Server (Port 9099)
        Restricted via CA trust store validation
        Executes local commands via POST /api/v1/execute
      Consensus & Autostart
        Autostarts services sequentially on boot
        Checks local maintenance mode flags
        Verifies ZooKeeper cluster consensus state
```

## Function & Logic Breakdown - Spark CLI (`spark.py`)

### `check_tcp_port(port, local_ip=None)`
- Verifies socket connectivity on the target port locally.
- Attempts connecting to `127.0.0.1` first, then falls back to `local_ip`.

### `get_local_maintenance_status(ip_addr)`
- Checks if `/etc/hci/maintenance.state` exists.
- If not, queries node status (`IN_MAINTENANCE` or `ENTERING_MAINTENANCE`) in ScyllaDB using the Daruk query proxy, falling back to a containerized `cqlsh` shell script run.

### `show_status_json()` / `show_status()`
- Probes status of all core cluster services (`zookeeper`, `hydra-db`, `aether`, `spark-daemon`, `spectrum`, `bifrost`, `dagur`, `mimir`, `vali`, `catalyst`, `hylia`, `gatoway`, `logos`, `mipha`, `daruk`, `agahnim`, `slate`, `urbosa`).
- Checks if systemd status is `active`.
- Probes associated TCP ports (e.g. 2181 for ZooKeeper, 9042 for HydraDB, etc.).
- Resolves process PIDs (reads systemd `MainPID` properties for native helper daemons like `daruk` or fetches containerized process ID mappings using `podman top systemd-<service> hpid`).
- Prints a structured human-readable breakdown or JSON map.

---

## Function & Logic Breakdown - Spark Daemon (`spark_daemon_decoded.py`)

### mTLS Socket Listener
- Sets up an HTTPS server (`ThreadingHTTPServer`) on port `9099` running inside a privileged container mounting the host's `/usr/local/bin` and certificates.
- Binds SSL context:
  - Enforces `ssl.CERT_REQUIRED` to validate incoming client certificates.
  - Loads `/etc/hci/spark/certs/node.crt` and `/etc/hci/spark/certs/node.key`.
  - Configures trust validation using `/etc/hci/spark/certs/ca.crt`.

### API Endpoints
- **`POST /api/v1/execute`**: Extracts a JSON command payload, executes it using `subprocess.Popen` in the host context (via privileged mounts and systemd socket communication), and returns the exit status code, `stdout`, and `stderr` buffers.
- **`GET /api/v1/node/status`**: Returns local metadata, including IP, hostname, ZooKeeper leader state, and maintenance status.

### Workload Autostart Loop
- Runs in a separate daemon thread on service startup:
  1. Bootstraps the local ZooKeeper instance if it is down.
  2. Halts auto-start progression if `/etc/hci/maintenance.state` is present.
  3. Polls local ZooKeeper port `2181` until quorum mode is reached.
  4. Queries ZooKeeper `/cluster_state` node. If manual stop was requested, exits start sequence.
  5. Starts local cluster database, storage, and logic daemons sequentially (via systemd commands acting on containerized Quadlet service targets).

### What startup is allowed to clear

`check_cluster_and_autostart()` opens by clearing libvirt definitions this host is still
carrying. The premise is that a hypervisor is a stateless executor: Vali defines and starts
a workload when it schedules one here, so a definition present at startup is left over from
before and should go.

That premise is about a host that just **booted**. It does not hold when the daemon is
merely restarted, and spark-daemon is restarted on every `deploy_updates.py` rollout.

| | Cleared | Left alone |
| --- | --- | --- |
| Inactive (defined, not running) | undefined with `--nvram` | — |
| Running | — | untouched, and named in the log |

Until 2026-08-22 this walked `virsh list --all --name` and ran `virsh destroy` before the
undefine, so a rollout on a live host destroyed the guests it was running, deleted their
nvram, and left `hydra.vms` saying `Stopped` — which nothing brings back. It produced no
evidence: both streams go to `PIPE`, so no `virsh destroy` ever reached the journal, and
the only trace was a `machine-qemu-*.scope: Deactivated successfully` line that reads like
a guest shutting itself down.

At boot the two behave identically, because a freshly booted host has no running domains.
That is why the narrower version costs nothing where the reasoning applies.

Running guests are now listed rather than passed over silently — a restart that
deliberately leaves workloads up should say so, or the next person reads the same silence
and concludes nothing happened.

### The service lists name units that exist

`check_cluster_and_autostart()` and the 30-second watchdog carry explicit service lists.
Both named `aether`, which was removed with DRBD, so every watchdog cycle ran
`systemctl start aether` and logged `Unit aether.service not found` — on every node,
forever. `sidon` appeared in none of them, meaning the daemon that serves every guest disk
was not among the services autostart brought up or the watchdog kept up.

`AETHER_VOLUMES_ROOT` is *not* affected: the directory is still called that, and renaming
it is a data migration rather than a rename. Only the unit names were wrong.

`test_rollout_safety.py` covers both: that startup clears only inactive definitions and
never destroys a domain, and that no service list names a unit that no longer exists.
