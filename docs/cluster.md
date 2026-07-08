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
Bootstrap a new cluster across a set of physical hosts.

#### Node Layout Overview
- **1-Node**: Single fully-provisioned hypervisor and storage node. No redundancy (RF=0 forced).
- **2-Node**: Both hosts are fully provisioned hypervisors and storage nodes. Requires a `--witness` node (see below) to achieve quorum.
- **3-Node**: All three hosts are fully provisioned hypervisors and storage nodes. Natural quorum is achieved with a 2/3 majority — **no witness needed**.
- **4+ Nodes**: ZooKeeper consensus quorum is maintained by the first 3 nodes as voting members; additional hosts are automatically configured as ZooKeeper observers to scale the cluster cleanly.

#### 2-Node Layout (Witness Node Required)
A **2-node cluster** cannot achieve quorum on its own — if one node goes down, the remaining node cannot safely determine whether it or the other node has the network partition. A lightweight **Witness Node** is required as a tie-breaker.

- **Role**: Serves as a quorum tie-breaker (ZooKeeper voter and DRBD diskless replica) to prevent split-brain. Does **not** run hypervisor or database workloads.
- **Provisioned Services**: Runs only `spark-daemon`, `zookeeper`, and `aether` (Linstor satellite, diskless).
- **Excluded Services**: Excludes `libvirtd`/`qemu`, `hydra-db`/ScyllaDB, Linstor controllers, `daruk`, and all scheduling/management workloads.
- **Storage**: No physical disk claiming or LVM pool provisioning. Linstor volumes are configured `--diskless` on the witness host.
- **Hardware**: Can be any minimal machine or VM — does not need storage or significant compute.

```bash
# Syntax
cluster create -s <IP1,IP2,...> [-r <redundancy_factor>] [-v <virtual_ip>] [--witness <witness_ip>]

# Example: 3-node cluster — all 3 are full hypervisors, quorum is natural
cluster create -s 10.10.102.220,10.10.102.222,10.10.102.223 -r 1 -v 10.10.102.240

# Example: 2-node cluster with a lightweight witness node for quorum
cluster create -s 10.10.102.220,10.10.102.222 -r 1 -v 10.10.102.240 --witness 10.10.102.223

# Example: 4-node cluster — first 3 are ZK voters, 4th is a ZK observer
cluster create -s 10.10.102.220,10.10.102.221,10.10.102.222,10.10.102.223 -r 2 -v 10.10.102.240
```
**Creation Workflow**:
1. Creates the cluster configuration file `/etc/hci/cluster.json` on all nodes.
2. Formats and claims raw disks $\ge 100\text{ GB}$ to construct the Aether storage resource pools (`default-vm-container` and `default-image-container`).
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

# View verbose status (includes storage resource layouts, node roles, and detailed daemon states)
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
# Stop all cluster services and unmount Aether storage volumes
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

---

## 4. Cluster Security & Trust Seeding

To guarantee passwordless SSH, secure inter-node KVM live migration, and encrypted mTLS command orchestration, the cluster configures and seeds security keys and certificates during bootstrapping.

### A. SSH Key Seeding and Keyscan Automation
During `cluster create` (orchestrated by `/usr/local/bin/provision.py`):
1. **Public Key Gathering**: Node 1 executes `ssh-keyscan` across all nodes (including their IP addresses and hostname formats like `Valkyrie-XXXXXX`) to capture host keys securely:
   ```bash
   ssh-keyscan -H -t rsa,ecdsa,ed25519 10.10.102.120 10.10.102.121 10.10.102.122 Valkyrie-51C2B5 Valkyrie-232EB8 Valkyrie-DB225F >> /root/.ssh/known_hosts
   ```
2. **Distribution**: These gathered keys are written to `/root/.ssh/known_hosts` on all cluster nodes. This prevents live migrations from failing due to SSH host key verification warnings when libvirt executes:
   ```bash
   virsh migrate --live ... qemu+ssh://root@<node_ip>/system
   ```

### B. mTLS Certificate Seeding & Locations
The provisioning engine generates and distributes TLS certificates signed by a custom cluster CA to enforce strict mTLS validation on port 9099.

Seeding paths:
* **Client mTLS Scope** (CLIs/tools):
  * `/root/.certs/ca.crt`: Custom cluster CA certificate
  * `/root/.certs/client.crt`: Client certificate for `valcli`/`mcli`
  * `/root/.certs/client.key`: Client private key (permission `600`)
* **Spark Daemon Scope** (Host Agent listener):
  * `/etc/hci/spark/certs/ca.crt`: Custom cluster CA certificate
  * `/etc/hci/spark/certs/node.crt`: Host agent node certificate
  * `/etc/hci/spark/certs/node.key`: Host agent private key (permission `600`)
* **Spectrum Ingress Scope** (Web interface / Traefik SSL):
  * `/etc/hci/spectrum/certs/server.crt`: Ingress SSL certificate
  * `/etc/hci/spectrum/certs/server.key`: Ingress SSL private key (permission `600`)

### C. Manual Trust Synchronization Commands
If a host key changes or a certificate needs manual synchronization, administrators can run:
```bash
# Scan and update keys for a host
ssh-keyscan -H -t rsa,ecdsa,ed25519 <node_ip> >> /root/.ssh/known_hosts
```


---

## Technical Reference

For the internal code structure, class/function details, and execution flowcharts, see the [Technical Guide](./cluster_technical.md).
