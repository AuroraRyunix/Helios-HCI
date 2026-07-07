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
