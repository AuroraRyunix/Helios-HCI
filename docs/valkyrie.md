# Valkyrie (Hypervisor Host Layer)

Valkyrie is the foundation of our HCI cluster, serving as the hypervisor host operating system. It is the direct equivalent of Nutanix **AHV** (Acropolis Hypervisor).

> [!NOTE]
> **Name Origin:** In Norse mythology, the **Valkyries** ("choosers of the slain") are noble female figures who select who survives or perishes in battle, guiding them to Valhalla. In Helios-HCI, **Valkyrie** is the underlying host operating system that supports, hosts, and decides the placement/evacuation of the virtual machine workloads.

## Nutanix Role (AHV)
In Nutanix, AHV is a customized hypervisor based on CentOS/RHEL KVM. It runs virtual machines, hosts the Controller VM (CVM) which is granted direct control of local storage controllers via PCI passthrough, and accesses storage via a local NFS mount routed to the CVM.

## Containerized HCI Approach
In our architecture, the physical host OS (EL 10.2) is **Valkyrie**. 
Instead of running a separate, resource-heavy CVM virtual machine:
1. **Direct KVM/libvirt on Host**: The EL 10.2 host runs the KVM kernel module and `libvirtd` / `virtqemud` directly.
2. **Co-located Container Services**: Services like storage (`Aether`), cluster state (`Odin`/`Zookeeper`), and metadata (`Hydra`) run as lightweight Podman containers directly on the host's kernel space.
3. **Internal Storage Mounting**:
   - `Aether` (Stargate) runs in a Podman container and exports storage via NFS (using NFS-Ganesha or standard NFS).
   - Valkyrie's host `libvirtd` mounts this NFS export locally over the loopback interface (`127.0.0.1` or a dedicated internal bridge IP).
   - QEMU VMs run on Valkyrie and use virtual disks stored in this NFS storage pool.

---

## Host Configuration

### Required Host Services
- `libvirtd` (or modular daemons: `virtqemud`, `virtstoraged`, `virtnetworkd`, `virtnodedevd`)
- `podman` (container engine)
- `rpcbind` / `nfs-utils` (to mount loopback NFS storage)

### Network Architecture
- **Management & Cluster Interface (`eth0` / `bond0`)**: Connects the hosts together. Standard host IPs (e.g., `10.10.102.220`, `222`, `223`).
- **Internal Storage Bridge (`virbr1` or Loopback)**: Dedicated path for the host hypervisor to talk to the local `Aether` storage daemon.

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
