# Helios-HCI: Containerized Hyper-Converged Infrastructure Stack

Helios-HCI is a lightweight, containerized Hyper-Converged Infrastructure (HCI) software-defined datacenter stack inspired by Nutanix, built directly on Enterprise Linux (EL) 10.2 hypervisor hosts. 

It eliminates resource-heavy Controller VMs (CVMs) by co-locating metadata, storage, configuration, and orchestration daemons inside lightweight Podman containers and native systemd daemons directly on host kernels.

---

## 1. Component Mappings (Helios vs. Nutanix)

| Helios Service | Nutanix Equivalent | Technology Used | Description |
| :--- | :--- | :--- | :--- |
| [Valkyrie](./docs/valkyrie.md) | **AHV (Hypervisor)** | CentOS/RHEL KVM + libvirt | The physical host operating system. Runs VMs directly on host kernel. |
| [Spark](./docs/spark.md) | **Genesis** | Native Python mTLS daemon | Host-level bootstrap manager, systemd coordinator, and remote orchestrator. |
| [Odin](./docs/odin.md) / [ZooKeeper](./docs/zookeeper.md) | **Zeus (ZooKeeper)** | Podman + Apache ZooKeeper | Distributed consensus store for cluster metadata and active leader election. |
| [HydraDB](./docs/hydra.md) | **Medusa** | Podman + ScyllaDB (Cassandra) | Distributed metadata database for cluster configurations, VM state, and networks. |
| [Daruk](./docs/daruk.md) | **Medusa Proxy** | systemd + Python CQL Proxy | Persistent database query proxy shielding ScyllaDB from connection overhead. |
| [Aether](./docs/aether.md) | **Stargate** | Podman + GlusterFS + NFS-Ganesha | Software-defined distributed storage engine. Mounted locally on host loopback. |
| [Spectrum](./docs/spectrum.md) | **Prism** | Podman + Python Web Server | Web UI console and REST API manager for monitoring, VM operations, and tasks. |
| [Vali](./docs/vali.md) | **Acropolis VM Manager** | Native Python service | Dynamic VM placement scheduler, load balancer, and Distributed Resource Scheduler (DRS). |
| [Logos](./docs/logos.md) | **Arithmos** | Native Python collector | Distributed background telemetry agent collecting CPU, RAM, disk, and network stats. |
| [Dagur](./docs/dagur.md) | **Chronos** | Native Python service | Clustered cron task scheduler executing maintenance scripts and database tasks. |
| [Mimir](./docs/mimir.md) | **NCC (Health Checker)** | Native Python service | Background cluster diagnostics daemon executing periodic health checks. |
| [Mipha](./docs/mipha.md) | **Acropolis HA Manager** | Native Python service | High-Availability host liveness monitor and VM failover coordinator. |
| [Gatoway](./docs/gatoway.md) | **Flow** | Native Python service | Layer-2 VLAN network interface synchronization daemon. |
| [Urbosa](./docs/urbosa.md) | **Flow SDN** | Native Python service | Layer-3 software-defined overlay, distributed routing, and micro-segmentation daemon. |
| [Bifrost](./docs/bifrost.md) | **Vipmonitor** | Native Python service | Floating VIP manager daemon ensuring API access high availability. |

---

## 2. Command-Line Interface (CLI) Reference

Helios-HCI exposes several CLI utilities on host consoles to manage, monitor, and query cluster components.

### A. Genesis/Bootstrap Utility (`spark`)
Run on any hypervisor host to check local systemd services and container statuses:
```bash
# Check running status, main PIDs, and health of all local services
spark status

# Output local service health in machine-readable JSON format
spark status --json

# Start ZooKeeper and Spark-Daemon bootstrap processes locally
spark start

# Stop ZooKeeper and Spark-Daemon locally
spark stop

# Gracefully stop ALL containerized and native cluster workloads on this host
spark stop all
```

### B. Acropolis VM & Infrastructure Management (`valcli`)
The primary CLI for administrator operations, virtual machine control, storage benchmarking, and database diagnostics:
```bash
# VM Operations
valcli vm.list                     # List all virtual machines in the cluster
valcli vm.on <vm_name>             # Power ON a virtual machine on a scheduled host
valcli vm.off <vm_name>            # Power OFF (force destroy) a virtual machine
valcli vm.migrate <vm> <target_ip>  # Live-migrate a VM to another node IP
valcli vm.balance                  # Manually trigger memory load rebalancing (DRS)
valcli drs.status                  # View cluster load deviation and migration history

# Host & Maintenance
valcli host.list                   # List all hypervisor nodes and maintenance states
valcli host.maintenance.enter <IP> # Enter maintenance mode (live-evacuates active VMs)
valcli host.maintenance.leave <IP> # Exit maintenance mode

# Storage & Cleanup
valcli storage.list                # List Aether storage containers and paths
valcli storage.benchmark <name>    # Run a raw write/read performance benchmark
valcli storage.cleanup_orphaned    # Prune orphaned VM disk raw files and NVRAM files

# Scheduling & Diagnostics
valcli scheduler.list              # List scheduled cron jobs (Dagur)
valcli scheduler.trigger <name>    # Manually trigger execution of a scheduled job
valcli health.check                # Execute all health diagnostics checks (Mimir)
valcli db.print <table_name>       # Print contents of ScyllaDB metadata table as ASCII
valcli db.query "<cql_query>"      # Run a raw CQL query against the ScyllaDB cluster
```

### C. Task Coordination CLI (`catcli`)
Interacts with the Catalyst orchestrator to queue, monitor, and clean up async cluster tasks:
```bash
# List all historical tasks, actions, progress, and statuses
catcli list

# View JSON status detail of a specific task
catcli status <task_uuid>

# Submit a custom task action to a target service queue
catcli submit --service vali --action start --payload '{"name": "my-linux-vm"}'

# Synchronize host DNS, NTP, and timezone configurations from database settings
catcli sync

# Prune completed and failed tasks from ScyllaDB history
catcli cleanup
```

### D. NCC Diagnostics CLI (`mcli`)
Manually run and query diagnostic health check schedules:
```bash
# List all registered NCC health checks
mcli health_checks list

# Manually trigger all diagnostic checks immediately
mcli health_checks run_all
```

### E. Cluster Management CLI (`cluster`)
Orchestrate cluster-wide lifecycle commands:
```bash
# Bootstrap a 3-node cluster with virtual IP 10.10.102.240
cluster create -s 10.10.102.220,10.10.102.222,10.10.102.223 -r 1 -v 10.10.102.240

# Query cluster-wide status (verbose logs GlusterFS bricks and daemons)
cluster status --verbose

# Start all containerized and native services across the cluster
cluster start

# Stop all containerized and native services and unmount GlusterFS volumes
cluster stop

# Wipe cluster configurations, databases, and formats claimed drives
cluster destroy
```
For detailed creation workflows and HA failover policies, see [cluster.md](./docs/cluster.md). For virtual networking, subnets, and VLAN management, see [network.md](./docs/network.md).

---

## 3. Directory Layout (Configuration & Certs)

All configuration parameters and certificates reside under standardized directories:
* `/etc/hci/` - Root configuration directory.
* `/etc/hci/cluster.json` - Global host and cluster definition file.
* `/etc/hci/spectrum/spectrum.env` - Node IP and API configuration.
* `/etc/hci/spark/certs/` - Mutual TLS node certificates (`node.crt`, `node.key`, `ca.crt`) used by `spark-daemon` on port `9099`.
* `/root/.certs/` - Client Mutual TLS certificates used by administrative utilities (`client.crt`, `client.key`).
* `/var/lib/hci/aether/volumes/` - Mount directory where local virtual machine disk raw files are stored.

---

## 4. Cluster Network Architecture

Helios-HCI uses a lightweight, secure network layout for inter-node orchestration, consensus, and storage replication:

* **Localhost Bindings**: Internal service APIs (like Catalyst task queue on `9091` and Vali scheduler on `9095`) bind strictly to `127.0.0.1` to enforce local-only access.
* **Mutual TLS (mTLS) Mesh**: All cross-node administrative tasks and remote executions run securely over port `9099` via the **Spark Daemon**.
* **Consensus & Metadata Mesh**: Database gossip (ScyllaDB on `7000`) and consensus election (ZooKeeper on `2888`/`3888`) route over cluster-facing networks.
* **Floating Virtual IP (VIP)**: Managed dynamically by the **Bifrost** daemon, providing high-availability access to the Spectrum Web UI (`8443`).

### Cluster Network Flow Chart

```mermaid
flowchart TB
    subgraph Host1 [hci-node01]
        Spectrum1["Spectrum (WebUI/API)<br>Port 8443"]
        Catalyst1["Catalyst (Orchestrator)<br>Port 9091 (Localhost)"]
        Vali1["Vali (VM Scheduler/DRS)<br>Port 9095 (Localhost)"]
        Spark1["Spark Daemon (mTLS API)<br>Port 9099"]
        ZK1["ZooKeeper (Consensus)<br>Port 2181"]
        DB1["ScyllaDB (Metadata)<br>Port 9042"]
        Aether1["Aether (GlusterFS Storage)<br>Port 24007"]
    end

    subgraph Host2 [hci-node02]
        Spark2["Spark Daemon (mTLS API)<br>Port 9099"]
        ZK2["ZooKeeper (Consensus)<br>Port 2181"]
        DB2["ScyllaDB (Metadata)<br>Port 9042"]
        Aether2["Aether (GlusterFS Storage)<br>Port 24007"]
    end

    %% Internal service orchestration and query flows on Host1
    Spectrum1 -.->|Local API Calls / Command Exec (Port 9099)| Spark1
    Catalyst1 -.->|Submit Tasks / Local command (Port 9099)| Spark1
    Vali1 -.->|Schedule VM / Run command (Port 9099)| Spark1
    
    %% Direct TCP status checks
    Spectrum1 -.->|Check status (Port 2181)| ZK1
    Catalyst1 -.->|Check status (Port 2181)| ZK1
    Vali1 -.->|Check status (Port 2181)| ZK1

    %% Local database access via spark execution
    Spark1 -.->|Executes cqlsh (Port 9042)| DB1

    %% Inter-node replication and consensus (Cluster Mesh)
    DB1 <===>|ScyllaDB Gossip & Replication (Port 7000)| DB2
    ZK1 <===>|Consensus Election & Sync (Ports 2888/3888)| ZK2
    Aether1 <===>|GlusterFS Data Replication (Port 24007)| Aether2

    %% Remote orchestration and fallbacks
    Spark1 -.->|Orchestrate remote node (Port 9099)| Spark2
    Vali1 -.->|Remote VM Run (Port 9099)| Spark2
    DB1 -.->|cqlsh fallback query (Port 9042)| DB2
```

For a complete reference of network scopes, port allocations, and communication boundaries, see the [Network Architecture Documentation](./docs/network.md).

---

## 5. High-Availability & Robustness Enhancements

The stack has been enhanced with enterprise-grade resiliency and health-based routing:

* **Active WebUI VIP Failover**: The VIP manager (`bifrost`) evaluates active candidates on port `8443` (Spectrum) and enforces a local health guard. A node will only bind the VIP if it is the leader AND its local WebUI container is active and listening. If the local WebUI is down or bootstrapping, the VIP dynamically floats to a healthy node, ensuring zero client-facing downtime.
* **Database Connection Resilience**: The WebUI (`spectrum`) establishes keyspace connection checks. If the local ScyllaDB instance is bootstrapping or down, Spectrum reads `/etc/hci/cluster.json` and falls back to other online database hosts.
* **Task API Queue Cache**: If ScyllaDB encounters brief connection latency or quorum shifts, Spectrum serves Catalyst tasks from an in-memory fallback cache to prevent UI progress bars from flickering or resetting to grey.
* **Streamlined Reboot Coordination**: Graceful VM evacuation and host transitions are fully isolated. The reboot sequence relies on the prior maintenance phase to gracefully migrate VMs, leaving the host-level `spark-daemon` active to process remote hardware reboot calls reliably.


