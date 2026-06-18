# Aether (Distributed Storage I/O Engine - Linstor/DRBD)

Aether is the cluster storage controller and block path manager. It is the direct equivalent of Nutanix **Stargate**.

> [!NOTE]
> **Name Origin:** In Greek mythology, **Aether** is the personification of the bright upper sky and the air breathed by gods. Historically in physics, the *aether* was a hypothetical space-filling medium postulated to support the propagation of electromagnetic waves. In Helios-HCI, **Aether** refers to the distributed storage fabric (Linstor + DRBD) that spans all physical nodes to form a single, unified virtual storage medium.

---

## Nutanix Role (Stargate)
In Nutanix, Stargate is the core data-path service. All read and write operations from VMs are sent directly to Stargate. In our updated architecture, Aether bypasses FUSE filesystems completely, exposing virtual disks directly to QEMU as replicated DRBD block devices.

---

## Failures To Tolerate (FTT) & Replication

FTT defines the redundancy level of the cluster, mapping directly to Nutanix Redundancy Factors (RF). Under Linstor/DRBD, FTT dictates the auto-placement replication count of the DRBD resources:

*   **FTT = 0 (Redundancy Factor 1 / RF1)**:
    - **Replication count**: 1.
    - **Minimum hosts**: 1.
    - **Behavior**: Allocated only on the host running the VM. No network replication.
*   **FTT = 1 (Redundancy Factor 2 / RF2)**:
    - **Replication count**: 2.
    - **Minimum hosts**: 2.
    - **Behavior**: Replicated synchronously over the network between 2 hosts using DRBD. Survives 1 host failure.
*   **FTT = 2 (Redundancy Factor 3 / RF3)**:
    - **Replication count**: 3.
    - **Minimum hosts**: 3.
    - **Behavior**: Replicated synchronously over the network across 3 hosts using DRBD. Survives 2 simultaneous host failures.

---

## Underlying Storage Engine: Linstor + DRBD

To maximize storage I/O performance and support enterprise features, Aether runs **Linstor** and **DRBD** as the software-defined storage (SDS) replication engine:

### Linstor & DRBD Implementation
* **Host Storage Pools**: Host storage `/dev/sdb` is configured as an LVM-Thin Pool (`thin_pool_aether` inside `vg_aether`). Lvm-thin natively handles space-saving snapshots, thin provisioning, and high-performance block allocations.
* **Linstor Satellite**: Runs as a privileged Podman container on all nodes, communicating with the host kernel to provision block devices dynamically.
* **Linstor Controller**: Runs as a manager service on the leader node, keeping track of volume definitions, resource allocation, and replication targets.
* **Direct Block Access**: Instead of accessing a file on a shared mount, VMs are defined with direct block storage targets mapping to `/dev/drbd/by-res/<vm_name>/0`. This achieves near bare-metal I/O throughput.

---

## Data Write Path (RF2 / FTT=1)

```
[ Virtual Machine (VM) ]
           │
           │ (Direct Block I/O to /dev/drbd/by-res/test/0)
           ▼
[ DRBD Kernel Driver (Host Kernel) ]
           ├───► Local Writes to vg_aether/thin_pool_aether (LVM Thin)
           └───► Synchronous network replication (TCP/RDMA) to Peer Host
```
