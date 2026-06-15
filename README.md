# Helios-HCI: Containerized Hyper-Converged Infrastructure Stack

Helios-HCI is a lightweight, containerized Hyper-Converged Infrastructure (HCI) software-defined datacenter stack inspired by Nutanix, built directly on Enterprise Linux (EL) 10.2 hypervisor hosts. 

It eliminates resource-heavy Controller VMs (CVMs) by co-locating metadata, storage, configuration, and orchestration daemons inside lightweight Podman containers and native systemd daemons directly on host kernels.

---

## 1. Component Mappings (Helios vs. Nutanix)

| Helios Service | Nutanix Equivalent | Technology Used | Description |
| :--- | :--- | :--- | :--- |
| **Valkyrie** | **AHV (Hypervisor)** | CentOS/RHEL KVM + libvirt | The physical host operating system. Runs VMs directly on host kernel. |
| **Spark** | **Genesis** | Native Python mTLS daemon | Host-level bootstrap manager, systemd coordinator, and remote orchestrator. |
| **Odin** (ZooKeeper) | **Zeus (ZooKeeper)** | Podman + Apache ZooKeeper | Distributed consensus store for cluster metadata and active leader election. |
| **HydraDB** | **Medusa** | Podman + ScyllaDB (Cassandra) | Distributed metadata database for cluster configurations, VM state, and networks. |
| **Aether** | **Stargate** | Podman + GlusterFS + NFS-Ganesha | Software-defined distributed storage engine. Mounted locally on host loopback. |
| **Spectrum** | **Prism** | Podman + Python Web Server | Web UI console and REST API manager for monitoring, VM operations, and tasks. |
| **Vali** | **Acropolis VM Manager** | Native Python service | Dynamic VM placement scheduler, load balancer, and Distributed Resource Scheduler (DRS). |
| **Logos** | **Arithmos** | Native Python collector | Distributed background telemetry agent collecting CPU, RAM, disk, and network stats. |
| **Dagur** | **Scheduler** | Native Python service | Clustered cron task scheduler executing maintenance scripts and database tasks. |
| **Mimir** | **NCC (Health Checker)** | Native Python service | Background cluster diagnostics daemon executing periodic health checks. |

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
For detailed creation workflows and HA failover policies, see [cluster.md](file:///C:/Users/AuraFlight/Desktop/container-hci/docs/cluster.md).

---

## 3. Directory Layout (Configuration & Certs)

All configuration parameters and certificates reside under standardized directories:
* `/etc/hci/` - Root configuration directory.
* `/etc/hci/cluster.json` - Global host and cluster definition file.
* `/etc/hci/spectrum/spectrum.env` - Node IP and API configuration.
* `/etc/hci/spark/certs/` - Mutual TLS node certificates (`node.crt`, `node.key`, `ca.crt`) used by `spark-daemon` on port `9099`.
* `/root/.certs/` - Client Mutual TLS certificates used by administrative utilities (`client.crt`, `client.key`).
* `/var/lib/hci/aether/volumes/` - Mount directory where local virtual machine disk raw files are stored.
