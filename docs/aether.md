# Aether (Distributed Storage I/O Engine)

Aether is the cluster storage controller and data path manager. It is the direct equivalent of Nutanix **Stargate**.

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

## Underlying Distributed File System (DFS) Candidates

To avoid building a distributed transport and consensus protocol from scratch, the `Aether` container packages and runs a battle-tested open-source Distributed File System (DFS) acting as the underlying replication engine:

### Candidate 1: GlusterFS (File-Based, Lightweight)
*   **How it works**: GlusterFS groups local directories (bricks) across the hosts into cluster-wide "Volumes" (which represent our **Storage Containers**).
*   **NFS Presentation**: GlusterFS has a native NFS server built-in or can be exported via NFS-Ganesha.
*   **FTT Support**:
    *   `FTT=1` matches a Gluster `replica 3` volume (replicated across all 3 nodes) or `replica 3 arbiter 1` (replicates to 2 nodes, 3rd node acts as metadata tie-breaker to prevent split-brain).
*   **Resource footprint**: Low (~200MB RAM per host).

### Candidate 2: SeaweedFS (Cloud-Native, High Performance)
*   **How it works**: A Go-based distributed filesystem consisting of Master servers (consensus), Volume servers (data storage), and Filer servers (provides metadata and filesystem interface).
*   **NFS Presentation**: SeaweedFS exposes WebDAV/S3 natively and can mount via FUSE, which NFS-Ganesha then exports as NFS to the host.
*   **FTT Support**:
    *   Replication is set at the bucket or directory level (e.g. `001` for RF2, `002` for RF3).
*   **Resource footprint**: Extremely low (~50-100MB RAM per host).

### Candidate 3: Ceph / CephFS (Enterprise-Standard, Resource Intensive)
*   **How it works**: Object storage system (RADOS) with a file system layer (CephFS).
*   **NFS Presentation**: NFS-Ganesha can integrate natively with CephFS using the `libcephfs` FSAL.
*   **FTT Support**:
    *   Controlled by CRUSH map replication rules (e.g. `size 3, min_size 2` for FTT=1).
*   **Resource footprint**: High (~2-4GB RAM minimum per host for OSDs and Mons).

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
