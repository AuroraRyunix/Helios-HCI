# Bifrost (Virtual IP Manager Daemon)

Bifrost is the floating Virtual IP (VIP) manager service for the HCI cluster. It is the direct equivalent of Nutanix **Vipmonitor**. It acts as a lightweight, consensus-aware IP router to ensure that the user-facing WebUI (Spectrum) is always reachable via a single, highly available virtual IP address.

> [!NOTE]
> **Name Origin:** In Norse mythology, **Bifröst** is the burning rainbow bridge that connects Midgard (the realm of humans) to Asgard (the realm of the gods). In Helios-HCI, the **Bifrost** daemon acts as a network bridge, dynamically routing management traffic to the active ZooKeeper leader node using a floating Virtual IP (VIP).

## Architecture & Lifecycle
- **Daemon Service**: Runs as a standalone python service (`/usr/local/bin/bifrost`) managed by systemd (`bifrost.service`).
- **WebUI-Aligned VIP Binding**: Bifrost queries ZooKeeper status and verifies WebUI (Spectrum port `8443`) responsiveness. The node elected as the ZooKeeper leader that is actively serving the WebUI binds the cluster VIP to its physical interface. If the leader node's WebUI is restarting or down, the VIP manager dynamically falls back to an active follower node running Spectrum, ensuring zero-downtime failover.
- **Autostart Constraint**: Bifrost is a static systemd service that is dynamically started/stopped by Spark commands (`cluster start` / `cluster stop`) and does not auto-start on boot unless the cluster is online.

## Technical Execution
Every 2 seconds, Bifrost checks the following state:
1. **Cluster Config**: Reads `/etc/hci/cluster.json` to resolve the VIP address and physical interface mapping (e.g. `ens192`).
2. **ZooKeeper & WebUI Leadership check**:
   - Queries all cluster nodes on ZooKeeper port `2181` to locate the current leader.
   - Verifies if the leader's Spectrum service is active on port `8443`.
   - If the leader's Spectrum service is down, it scans all other cluster hosts on port `8443` and selects the healthy candidate with the lowest IP address.
3. **Local Health Guard**:
   - Before binding the VIP, Bifrost runs a local health check `is_local_spectrum_listening()` by attempting to connect to `127.0.0.1:8443`.
   - If the local node is designated as the active leader candidate **and** local Spectrum is active, it binds the VIP:
     `ip addr add <vip>/24 dev <iface> label <iface>:vip`
   - It then broadcasts a Gratuitous ARP: `arping -U -c 3 -I <iface> <vip>` to flush client/switch ARP tables.
   - If the node is a follower, or if its local Spectrum service is down (e.g. during a reboot, bootstrap, or container rebuild), it immediately releases the VIP to allow another healthy node to bind it:
     `ip addr del <vip>/24 dev <iface> label <iface>:vip`

---

## Command Examples & Syntax

### 1. Check Bifrost Service Status
You can check if the Bifrost daemon is active and running on a host using systemd:
```bash
systemctl status bifrost
```

### 2. View Bifrost Transition Logs
Monitor logs to track VIP binding and release events:
```bash
# View recent transition logs
journalctl -u bifrost -n 30 --no-pager

# Follow logs in real-time
journalctl -u bifrost -f
```

### 3. Verify VIP Binding
Verify if the VIP address (e.g. `10.10.102.224`) is currently bound to the local network interface:
```bash
# Check ip address output for dev label :vip
ip addr show dev ens192
```

### 4. Manually Query ZK Leader IP
Query the current ZooKeeper leader IP as resolved by Bifrost's active failover logic:
```bash
# Query leader IP directly using python inside bifrost code context
python3 -c "import sys; sys.path.append('/usr/local/bin'); import bifrost; print(bifrost.get_zookeeper_leader_ip())"
```

