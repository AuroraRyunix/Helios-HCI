# Zookeeper (Distributed Configuration & Consensus Store)

Zookeeper provides highly reliable distributed coordination and consensus. It is used directly as **Zookeeper** in the Nutanix architecture.

> [!NOTE]
> **Name Origin:** In our stack, Zookeeper serves as the consensus engine for the **Odin** service wrapper. Just as Odin oversees the Norse gods from Asgard and maintains active consensus, Zookeeper coordinates active cluster leader elections and central state configuration records.

## Nutanix Role (Zookeeper)
In Nutanix, Zookeeper stores critical configuration state for the cluster, including node mappings, IP addresses, configured storage containers, and cluster topology. It runs on a subset of nodes (usually 3 or 5) to ensure high availability and uses Paxos-like consensus to resolve cluster state changes.

## Containerized HCI Approach
In our 3-node cluster, we run a **3-node ZooKeeper ensemble** using official ZooKeeper images in Podman containers across the hosts (`10.10.102.220`, `222`, `223`).
1. **Host Networking Mode**: To avoid overlay network overhead and complex container DNS resolution, Zookeeper containers run in `network=host` mode.
2. **Persistent Storage**: Zookeeper transactions and snapshots are written to host directories mounted into the container.
3. **Cluster Config**: Configured using standard environment variables or files mapped to the Zookeeper directory.

---

## Deployment & Configuration

### Ports Used (Host Network)
* `2181`: Client connections (used by Odin).
* `2888`: Follower connections to the Leader.
* `3888`: Leader election port.

### Directory Configuration on Host
* **Data Path**: `/var/lib/hci/zookeeper/data/`
* **Log Path**: `/var/lib/hci/zookeeper/log/`
* **Node ID File**: `/etc/hci/zookeeper/myid` (Contains a single integer: `1`, `2`, or `3`)

### Sample Podman Command (Run by Spark/Systemd)
```bash
podman run -d \
  --name zookeeper \
  --net=host \
  --restart=always \
  -v /var/lib/hci/zookeeper/data:/data:Z \
  -v /var/lib/hci/zookeeper/log:/datalog:Z \
  -e ZOO_MY_ID=1 \
  -e ZOO_SERVERS="server.1=10.10.102.220:2888:3888;2181 server.2=10.10.102.222:2888:3888;2181 server.3=10.10.102.223:2888:3888;2181" \
  zookeeper:3.9.2
```

*(Note: The `:Z` flag on volume mounts ensures correct SELinux context labeling on EL 10.2).*

---

## Technical Coordination & ZNode Registry

The cluster coordinators (Vali, Mipha, Bifrost) utilize the ZooKeeper ensemble for active leader elections and state lock coordination:
* **Vali Leader Election**: Uses ephemeral sequential znodes at `/vali/leader/lock-`. The node holding the lowest sequence number is elected as the active scheduler leader.
* **Mipha Coordinator**: Elects an active HA coordinator at `/mipha/leader/lock-` to monitor host heartbeats.
* **Bifrost VIP Floating**: Monitors `/vali/leader` to bind the floating Virtual IP address locally to the ZooKeeper leader node.
* **Cluster State**: Store the global cluster operational state at `/cluster_state` (can contain `started` or `stopped`).

---

## Command Examples & Verification

### A. Querying Ensemble Status (Four-Letter Words)
ZooKeeper supports simple network commands using four-letter words. You can query status and membership via netcat:
```bash
# Query server statistics, latency, and active mode (leader vs. follower)
echo stat | nc 127.0.0.1 2181

# Check client connections and active sessions
echo cons | nc 127.0.0.1 2181

# Verify server health state (should return 'imok')
echo ruok | nc 127.0.0.1 2181
```

### B. Interactive ZooKeeper Shell (`zkCli.sh`)
Use the interactive client tool inside the container to inspect znode trees and cluster states:
```bash
# Start the interactive ZK CLI session
podman exec -it systemd-zookeeper zkCli.sh -server 127.0.0.1:2181

# ZK Shell Command Examples:
# 1. List root-level znode paths
ls /

# 2. Query cluster state
get /cluster_state

# 3. View active Vali leader candidates
ls /vali/leader
```
