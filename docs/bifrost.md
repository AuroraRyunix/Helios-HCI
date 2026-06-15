# Bifrost (Virtual IP Manager Daemon)

Bifrost is the floating Virtual IP (VIP) manager service for the HCI cluster.

## Architecture & Lifecycle
- **Daemon Service**: Runs as a standalone python service (`/usr/local/bin/bifrost`) managed by systemd (`bifrost.service`).
- **Consensus VIP Binding**: Bifrost queries ZooKeeper status. The node elected as the ZooKeeper leader binds the cluster VIP to its physical interface. Non-leader hosts release the VIP.
- **Autostart Constraint**: Bifrost is a static systemd service that is dynamically started/stopped by Spark commands (`cluster start` / `cluster stop`) and does not auto-start on boot unless the cluster is online.

## Technical Execution
Every 2 seconds, Bifrost checks the following state:
1. **Cluster Config**: Reads `/etc/hci/cluster.json` to resolve the VIP address and physical interface mapping.
2. **ZooKeeper Leadership**: Calls the local ZooKeeper instance on port `2181` to check if the node is in `leader` mode.
3. **VIP Management**:
   - If leader and VIP is not bound, binds it: `ip addr add <vip>/24 dev <iface> label <iface>:vip`.
   - Broadcasts Gratuitous ARP: `arping -U -c 3 -I <iface> <vip>` to update network switches.
   - If follower and VIP is bound, releases it: `ip addr del <vip>/24 dev <iface> label <iface>:vip`.
