# Aether (Distributed Storage I/O Engine)

Aether is the cluster storage controller and data path manager. It is the direct equivalent of Nutanix **Stargate**.

> [!NOTE]
> **Name Origin:** In Greek mythology, **Aether** is the personification of the bright upper sky and the air breathed by gods. Historically in physics, the *aether* was a hypothetical space-filling medium postulated to support the propagation of electromagnetic waves. In Helios-HCI, **Aether** refers to the distributed storage fabric (GlusterFS) that spans all physical nodes to form a single, unified virtual storage medium.

## Nutanix Role (Stargate)
In Nutanix, Stargate is the core data-path service. All read and write operations from VMs are sent directly to Stargate. It exposes standard storage protocols (NFS, iSCSI) to the hypervisor, handles caching, and performs synchronous remote replication (Redundancy Factor 2/3) before acknowledging writes.

---

## Failures To Tolerate (FTT) & Replication

FTT defines the redundancy level of the cluster, mapping directly to Nutanix Redundancy Factors (RF):

*   **FTT = 1 (Redundancy Factor 2 / RF2)**:
    *   **Data copies**: 2 copies of every block are kept in the cluster.
    *   **Failure tolerance**: The cluster can survive the complete loss of any **1 host** without data loss or downtime.
    *   **Minimum hosts**: 3 hosts (allows a majority quorum to exist during a single failure).
*   **FTT = 2 (Redundancy Factor 3 / RF3)**:
    *   **Data copies**: 3 copies of every block are kept in the cluster.
    *   **Failure tolerance**: The cluster can survive the simultaneous loss of any **2 hosts** without data loss.
    *   **Minimum hosts**: 5 hosts (or 3 hosts with degraded quorum/split-brain risk depending on the consensus mechanism).

### Storage Container Policies
Like Nutanix, FTT policies can be applied at the **Storage Container** level. A single physical storage pool can host multiple Storage Containers with different properties:
1.  `vms-rf2` container: Configured with `FTT=1` (2 replicas) for general-purpose VM storage.
2.  `vms-rf3` container: Configured with `FTT=2` (3 replicas) for mission-critical databases.
3.  `scratch-temp` container: Configured with `FTT=0` (1 replica, no replication) for throwaway data/caches.

---

## Underlying Distributed File System (DFS) Engine

To avoid building a distributed transport and consensus protocol from scratch, the `Aether` container packages and runs **GlusterFS** as the underlying software-defined storage and replication engine:

### GlusterFS Implementation
* **Architecture**: GlusterFS groups local drives/bricks across the nodes into unified cluster-wide volumes. These volumes correspond directly to our **Storage Containers**.
* **Storage Mount Mechanism**: Each host's modular `libvirtd` system mounts the local Aether container's GlusterFS volume via a loopback interface (`localhost:/<volume_name>`) mounted at `/var/lib/hci/aether/volumes/<volume_name>`.
* **Redundancy (FTT=1)**: Configured as a replicated volume across the three nodes (`replica 3` or `disperse` depending on the disk count and cluster settings), ensuring that any single host failure is tolerated without split-brain issues.
* **Performance Tuning**: Volumes are configured with high-performance flags, including client-side caching (`performance.cache-size 256MB`), write-behind buffering (`performance.write-behind on`), metadata prefetching (`performance.stat-prefetch on`), and optimized parallel client threads.

---

## Data Write Path (RF2 / FTT=1)

```
[ Virtual Machine (VM) ]
           │
           │ (NFS Write I/O to /aether-pool/container-01)
           ▼
[ Local Host Loopback (127.0.0.1) ]
           │
           ▼
[ Aether Container (Local Host) ]
  └───► [ DFS Client/Wrapper ]
            ├───► Writes to Local Brick (Host Disk)
            └───► Sync replicates over network to Peer Aether Node (Peer Disk)
```

---

## Sample Storage Configuration (`/etc/hci/aether/storage-pools.json`)

```json
{
  "storage_pool_name": "default-pool",
  "dfs_engine": "glusterfs",
  "local_disks": [
    {
      "device": "/dev/sdb",
      "role": "data",
      "fs_type": "xfs"
    }
  ],
  "storage_containers": [
    {
      "name": "default-vm-container",
      "path": "/default-pool/vms",
      "ftt": 1,
      "compression": "lz4",
      "quota_bytes": 0
    },
    {
      "name": "critical-db-container",
      "path": "/default-pool/db",
      "ftt": 2,
      "compression": "none",
      "quota_bytes": 0
    }
  ]
}
```
