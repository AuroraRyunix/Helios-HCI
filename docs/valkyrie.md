# Valkyrie (Hypervisor Host Layer)

Valkyrie is the foundation of our HCI cluster, serving as the hypervisor host operating system. It is the direct equivalent of Nutanix **AHV** (Acropolis Hypervisor).

> [!NOTE]
> **Name Origin:** In Norse mythology, the **Valkyries** ("choosers of the slain") are noble female figures who select who survives or perishes in battle, guiding them to Valhalla. In Helios-HCI, **Valkyrie** is the underlying host operating system that supports, hosts, and decides the placement/evacuation of the virtual machine workloads.

## Nutanix Role (AHV)
In Nutanix, AHV is a customized hypervisor based on CentOS/RHEL KVM. It runs virtual machines, hosts the Controller VM (CVM) which is granted direct control of local storage controllers via PCI passthrough, and accesses storage via a local NFS mount routed to the CVM.

## Containerized HCI Approach
In our architecture, the physical host OS (EL 10.2) is **Valkyrie**. 
Instead of running a separate, resource-heavy Controller VM (CVM):
1. **Direct KVM/libvirt on Host**: The EL 10.2 host runs the KVM kernel module and `libvirtd` / `virtqemud` directly.
2. **Co-located Native & Container Services**:
   - **Sidon** (Stargate equivalent): Runs natively on the host as a high-performance Rust service (`sidon.service`), managing local extent stores, journals, and NBD socket exports.
   - **HydraDB (ScyllaDB)**, **ZooKeeper**, and **Daruk**: Run as systemd-managed Podman Quadlet containers directly on the host.
   - **Spark Daemon**: Runs natively on the host (`:9099`), terminating mutual TLS and driving local libvirt/system operations.
3. **Direct Userspace Block I/O (NBD Sockets)**:
   - Sidon exports each virtual disk directly as a local UNIX domain socket at `/var/lib/hci/sidon/nbd/<vdisk>.sock`.
   - QEMU VMs attach directly to this socket using `qemu:blockdev` or NBD disk XML configs (`<disk type='network' device='disk'><source protocol='nbd'><socket type='unix' path='/var/lib/hci/sidon/nbd/<vdisk>.sock'/>...</disk>`).
   - All I/O is served in userspace with zero kernel driver dependencies. Secure Boot remains fully enabled.

---

## Host Configuration

### Required Host Services
- `libvirtd` (or modular daemons: `virtqemud`, `virtstoraged`, `virtnetworkd`, `virtnodedevd`)
- `podman` (container engine for Quadlet HCI services)
- `sidon.service` (distributed storage engine)
- `spark-daemon.service` (host orchestration and libvirt agent)

### Network Architecture
- **Management & Cluster Interface (`eth0` / `bond0`)**: Connects the hosts together for management, WebUI (`:443`), and cluster communication.
- **mTLS Orchestration Mesh (`:9099`)**: Mutual TLS secured channel used by Spark daemon for inter-node orchestration and task execution.
- **Storage Replication Mesh (`:9105`)**: High-throughput mutual TLS channel used by Sidon for journal replication, extent group transfer, epoch fencing, and live migration forwarding.
- **SDN Overlay / VM Bridges**: Managed by Gatoway (L2 VLAN sync) and Urbosa (L3 VXLAN overlay) connecting guest VM interfaces.

---

## Service Configuration File (`/etc/hci/cluster.json`)
The host references a global cluster configuration file to resolve peers:

```json
{
  "cluster_name": "aura-hci-01",
  "redundancy_factor": 2,
  "hosts": [
    {
      "node_id": 1,
      "ip": "10.10.102.220",
      "hostname": "hci-node01"
    },
    {
      "node_id": 2,
      "ip": "10.10.102.222",
      "hostname": "hci-node02"
    },
    {
      "node_id": 3,
      "ip": "10.10.102.223",
      "hostname": "hci-node03"
    }
  ]
}
```
