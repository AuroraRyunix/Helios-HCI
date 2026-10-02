# Hylia (HA Rolling Upgrade & Life Cycle Management Service)

Hylia is the rolling upgrade and Life Cycle Management (LCM) daemon for the HCI cluster. It is the direct equivalent of Nutanix **Foundation** or **LCM**. It orchestrates zero-downtime, node-by-node rolling upgrades by leveraging Vali's native host maintenance APIs, verifies update packages via SHA-256 checksums, and features active-passive ZooKeeper-backed failover to resume upgrades when the leader node reboots.

> [!NOTE]
> **Name Origin:** In the Legend of Zelda, **Hylia** is the recurring goddess of protection, preservation, and rebirth who reincarnates across eras. In Helios-HCI, the **Hylia** daemon manages the rebirth (reboots) of hosts and preservation (migration of workloads) during zero-downtime cluster upgrades.

## Architecture & Lifecycle
- **Daemon Service**: Runs as a standalone python service (`/usr/local/bin/hylia`) managed by systemd (`hylia.service`).
- **Distributed State Persistence**: The upgrade state, target host lists, manifest data, and log runner buffer are stored in ScyllaDB (`hydra.hylia_jobs` and `hydra.hylia_logs`).
- **High-Availability Resume Hook**: When the ZooKeeper leader node reboots, the Hylia daemon on that node stops. A standby node gains ZooKeeper leadership, initializes its Hylia loop, detects the active upgrade job in the database, and seamlessly resumes orchestrating the upgrade from where it was interrupted.

## Rolling Upgrade Workflow
For each host in the target update node list, Hylia performs the following steps:
1. **Enter Maintenance Mode**: Submits a `/api/v1/host/maintenance` (`enter`) task to Vali. This triggers the native scheduler to live-migrate all running virtual machines to remaining nodes. Hylia loops and sleeps until the host status is verified as `IN_MAINTENANCE`.
2. **Deploy Updates**: Pushes verified component files to the target host's `/usr/local/bin/` via Spark API remote execution (using base64 file transfers).
3. **Reboot Host**: Triggers `reboot` on the target host.
4. **Wait Offline/Online**: Polls connection until the host goes offline, then comes back online and stabilizes.
5. **Leave Maintenance Mode**: Submits a `/api/v1/host/maintenance` (`leave`) task. Loops and sleeps until the host status returns to `NORMAL`, allowing VMs to be scheduled back onto the host.

---

## The two console entry points

`/usr/local/bin/hylia` with **no arguments** is the daemon, and always will be:
`hylia.service` runs it that way, and a CLI that changed the no-argument case would stop
every rolling upgrade in the fleet at the next deploy. Two subcommands exist beside it, and
each is one operation with an exit code, because each is run by a Catalyst task and dagur
reads the exit code and nothing else.

### `hylia --load-package <zip>`

Validates a staged upgrade archive, distributes it, and records the job it describes. This
is `/api/lcm/upload` minus the bytes: the console streams the archive to the leader's
spark-daemon (`POST /api/v1/lcm/package`, staged at `/tmp/helios_update.zip`) and then
submits this as a task, so what is left is the part that talks to every node — which is
exactly the part that had no business running on a web request thread.

It refuses before it destroys anything. `hylia_jobs` and `hylia_logs` are truncated only
after the archive has passed `validate_and_extract_zip`, which checks the signature over
the manifest *before* reading a single digest inside it. A missing or invalid package
therefore leaves the record of the package that **is** loaded untouched. A fan-out that
reached some nodes and not others is reported and exits non-zero: a package on two nodes
out of three is a rolling upgrade that dies on the third, hours later, with a confusing
error.

### `hylia --start-upgrade <job-id>`

Marks the loaded job `STARTING` and then **watches** it. It does not run the upgrade — the
daemon loop on the leader does, exactly as before, with its resume-after-reboot behaviour
untouched. The subcommand exists so there is one Catalyst task whose lifetime is the
upgrade's lifetime, which is what puts a rolling upgrade in the console's task ring and its
failure in the task log.

* Exit 0 only for `COMPLETED`; `FAILED` and a job nothing picks up are both non-zero.
* A job still `STARTING` after `STARTING_GRACE_SEC` is a failure with a sentence saying so.
  The daemon's loop runs every five seconds, so a minute is many chances; past that the
  honest report is that nothing is going to run this, not that the upgrade is slow.
* Progress is nodes finished, reported to the task named by `CATALYST_TASK_ID` — which
  dagur puts in the environment. The phase *within* a node is a guess and belongs on the
  page that can caveat it, not in a number a task reports.
* The job id is matched against a UUID before the first statement. It is interpolated into
  CQL and arrives from a web tier, and refusing it there is the difference between a
  rejected argument and an injected one.

Run by hand from a shell, both are ordinary commands: with no `CATALYST_TASK_ID` in the
environment, progress reporting is a no-op rather than a row in the console's task log that
no operator asked for.

---

## Command Examples & Syntax

### 1. Check Hylia Service Status
You can check if the Hylia daemon is active and running on a host:
```bash
systemctl status hylia
```

### 2. View Hylia Rolling Upgrade Logs
Monitor logs to track rolling upgrade progress and host transitions:
```bash
# View recent transition logs
journalctl -u hylia -n 50 --no-pager

# Follow logs in real-time
journalctl -u hylia -f
```

### 3. Check Active Upgrade Job in Database
Query ScyllaDB directly to check the current state of a cluster upgrade:
```bash
cqlsh -e "SELECT job_id, state, target_nodes, current_node, build_number FROM hydra.hylia_jobs;"
```


---

## Technical Reference

For the internal code structure, class/function details, and execution flowcharts, see the [Technical Guide](./hylia_technical.md).
