# Cluster Management & Lifecycle Orchestration

This document details the lifecycle management, orchestration pathways, and operational syntax for bootstrapping, starting, stopping, and destroying the Helios-HCI cluster.

---

## 1. Overview of the `cluster` Utility

The `cluster` CLI utility (`/usr/local/bin/cluster`) is an administrative orchestration tool. Instead of interacting with individual nodes manually, administrators run `cluster` commands to distribute configurations and manage state across the entire hypervisor pool.

### Command Execution Route
1. The administrator runs the `cluster` CLI command on the local console.
2. The CLI calls the local `spark-daemon` on mTLS port `9099`.
3. The local `spark-daemon` acts as the coordinator, making concurrent mTLS calls to the `spark-daemon` instances on all peer nodes to distribute configuration scripts, synchronize states, and start/stop systemd workloads in parallel.

---

## 2. Command Reference & Syntax

### A. Cluster Creation (`cluster create`)
Bootstrap a new cluster across a set of physical hypervisor hosts.
```bash
# Syntax
cluster create -s <IP1,IP2,IP3> [-r <redundancy_factor>] [-v <virtual_ip>]

# Example: Create a 3-node cluster with Redundancy Factor 1 and VIP 10.10.102.240
cluster create -s 10.10.102.220,10.10.102.222,10.10.102.223 -r 1 -v 10.10.102.240
```
**Creation Workflow**:
1. Creates the cluster configuration file `/etc/hci/cluster.json` on all nodes.
2. Formats and claims raw disks $\ge 100\text{ GB}$ to construct the GlusterFS volumes (`default-vm-container` and `default-image-container`).
3. Writes `/etc/hci/aether/storage-pools.json` on each host.
4. Distributes environment parameters `/etc/hci/spectrum/spectrum.env`.
5. Starts the core storage layer (`Aether`) and mounts containers locally over loopback.
6. Starts ZooKeeper (`Odin`) and ScyllaDB (`HydraDB`) nodes to form the database ring.
7. Seeds the initial metadata schemas, user accounts, and default schedules.
8. Launches all application workloads (`spectrum`, `bifrost`, `dagur`, `mimir`, `vali`, `catalyst`, `gatoway`, `logos`).

### B. Cluster Status (`cluster status`)
Query cluster health and engine statistics.
```bash
# Check basic status (shows whether cluster is started/stopped and online hosts)
cluster status

# View verbose status (includes Gluster volume layouts, bricks, and detailed daemon states)
cluster status --verbose
```

### C. Cluster Startup (`cluster start`)
Resume cluster operations after the nodes have been powered on or stopped.
```bash
# Start all containerized workloads and host-level coordinators across all nodes
cluster start
```

### D. Cluster Stop (`cluster stop`)
Safely quiesce active virtual machines, unmount the filesystems, and put the services to rest.
```bash
# Stop all cluster services and unmount GlusterFS storage volumes
cluster stop
```

### E. Cluster Destruction (`cluster destroy`)
Wipe all databases, clear claimed disks, remove configuration parameters, and reset the hypervisor hosts to factory default.
```bash
# WARNING: Wipes all VM disks, metadata tables, and system configurations permanently
cluster destroy
```

---

## 3. High Availability (HA) Failover Logic

### A. Virtual IP (VIP) Failover via Bifrost
* The cluster utilizes a floating Virtual IP (VIP) managed by the **Bifrost** daemon.
* Bifrost monitors the ZooKeeper leadership. The node elected as the ZooKeeper leader binds the VIP interface locally.
* If the active leader goes offline, ZooKeeper consensus automatically triggers a new leader election. Bifrost on the newly elected leader host immediately claims the VIP using Gratuitous ARP (GARP) broadcasts, redirecting Spectrum Web Console traffic without manual intervention.

### B. VM High Availability (HA) Failover via Mipha
* **Active HA Orchestration**: High Availability is managed dynamically by the **Mipha** daemon. Mipha uses ZooKeeper to elect an active coordinator leader that monitors the cluster.
* **Host Crash Detection**: The active Mipha leader polls all cluster nodes every 10 seconds using both network pings (ICMP) and the Spark mTLS API (`9099`). If a host is unreachable on both paths for 3 consecutive polls (30 seconds), it is marked as `DOWN` in ScyllaDB.
* **Automatic Failover & Restart**: Mipha queries ScyllaDB for all virtual machines registered to the failed node, resets their database state, and submits automatic start tasks to the Catalyst task queue.
* **Optimal Scheduling**: The **Vali** scheduler picks up the tasks and immediately schedules the VMs to boot on the healthiest remaining hosts based on available RAM and DRS rules, restoring VM availability automatically.
