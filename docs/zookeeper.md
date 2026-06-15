# Zookeeper (Distributed Configuration & Consensus Store)

Zookeeper provides highly reliable distributed coordination and consensus. It is used directly as **Zookeeper** in the Nutanix architecture.

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
