# Helios Distributed File System (Sidon & Purah) — Technical Architecture Guide

## 1. Executive Overview & Design Motivation

The **Helios Distributed File System (DFS)** is an enterprise-grade, userspace software-defined storage engine written in Rust. It serves as the primary data path for virtual machine disks across the Helios HCI cluster.

Within the Helios HCI architecture, the storage tier directly mirrors the proven architecture of **Nutanix AOS**:
* **Sidon** is the per-node data-path engine (the equivalent of Nutanix **Stargate**). QEMU/KVM hypervisors communicate directly with the local Sidon instance via UNIX domain sockets over the Network Block Device (NBD) protocol.
* **Hydra (ScyllaDB)** maintains the cluster-wide distributed catalog and metadata map (the equivalent of Nutanix **Medusa**). It stores block-to-extent mapping tables and vdisk ownership records.
* **Daruk** is the local high-speed HTTP-to-CQL gateway proxying all metadata mutations through strict compare-and-swap (CAS) transactions.
* **Purah** is the leader-elected background curator embedded inside Sidon (the equivalent of Nutanix **Curator**). It handles asynchronous disk re-replication, mark-sweep garbage collection, background checksum scrubbing, and tiered data placement.
* **Ganon** is the comprehensive fault-injection and chaos validation test harness.

```
+-----------------------------------------------------------------------------+
|                            Valkyrie Host Node                               |
|                                                                             |
|  +---------------------------+       +-----------------------------------+  |
|  |     QEMU / KVM Guest      |       |          Spark Daemon             |  |
|  +-------------+-------------+       +-----------------+-----------------+  |
|                | NBD Unix Socket                       | mTLS (:9099)       |
|                v                                       v                    |
|  +-----------------------------------------------------------------------+  |
|  |                         SIDON (Rust Daemon)                           |  |
|  |                                                                       |  |
|  |  +--------------------+  +-------------------+  +------------------+  |  |
|  |  | NBD Engine / Switch|  | Commit Pipeline   |  | Purah Curator    |  |  |
|  |  +---------+----------+  +---------+---------+  +--------+---------+  |  |
|  |            |                       |                     |            |  |
|  +------------|-----------------------|---------------------|------------+  |
|               |                       |                     |               |
|               v                       v                     v               |
|        Local Journal               Peer Mesh             Daruk Gateway      |
|    (/var/lib/hci/sidon/...)      (mTLS :9105)         (http://127.0.0.1:9043)
|                                       |                     |               |
+---------------------------------------|---------------------|---------------+
                                        v                     v
                                 Remote Replicas         Hydra (ScyllaDB)
```

### Why Sidon Replaced Aether (Linstor / DRBD)

The previous storage substrate (**Aether**) relied on Linux kernel DRBD9 and Linstor. While functional for small setups, device-level synchronous kernel replication exposed fundamental architectural limits:
1. **The 191-Volume Port Ceiling:** DRBD requires dedicated TCP ports and standing kernel connections per replicated resource. The default allocation range capped a cluster at 191 virtual disks total.
2. **Kernel Overhead and Instability:** Every volume incurred kernel threads, minor numbers, and device nodes (`/dev/drbd/by-res/*`). Managing hundreds of block devices created severe latency spikes and udev contention.
3. **Dual-Primary Hazards and Fencing Limitations:** Under split-brain or network partitions, DRBD could only *infer* whether an isolated node was still writing. Determining safe promotion required complex out-of-band tie-breakers and risk of split-brain writes.
4. **Static Placement:** Rebalancing a DRBD volume required creating a new mirror on a remote node, performing full resynchronization, and tearing down the old mirror, putting heavy load on network links.
5. **Secure Boot Incompatibility:** DRBD required out-of-tree kernel modules (`kmod-drbd9x`), forcing hypervisors to disable Secure Boot or maintain custom MOK enrollments.

**Sidon eliminates every one of these problems:**
* **One Connection Per Node-Pair:** Sidon maintains a single persistent, multiplexed mutual TLS connection between any two physical nodes (on port `9105`), completely decoupling connection count from the number of VMs or disks.
* **100% Userspace:** Runs as a standard native binary (`sidon.service`). Zero kernel drivers or out-of-tree modules; Secure Boot remains fully enabled.
* **Extent-Based Immutability:** Disks are split into 1 MiB extents packaged into 4 MiB immutable sealed extent groups. Overwrites append new extents rather than mutating existing blocks in place.
* **Definitive Epoch Fencing:** Replica nodes fsync fencing epochs to disk. Stale writers are deterministically rejected with zero reliance on timing assumptions.

---

## 2. On-Disk Storage Engine & Hierarchy

### Disk Layout & Sibling Mounts

Sidon manages raw block devices and SSDs directly without requiring entries in `/etc/fstab`. During cluster bring-up, non-boot disks (NVMe, SATA SSDs) are formatted with XFS and auto-mounted by Sidon under:
`/var/lib/hci/sidon/disks/<filesystem-uuid>/`

The storage hierarchy within each mounted filesystem contains:
```
/var/lib/hci/sidon/
|-- nbd/                          # UNIX domain sockets exported to QEMU
|   |-- <vdisk-uuid>.sock
|-- journals/                     # Per-vdisk Write-Ahead Logs
|   |-- <vdisk-uuid>.jrn          # Active live journal segment
|   |-- <vdisk-uuid>.jrn.old      # Rotated journal segment being drained
|-- egroups/                      # Local extent groups
|   |-- <egroup-uuid>             # 4 MiB append-only data files
|-- disks/                        # Multi-disk sibling mounts
|   |-- <uuid-1>/egroups/
|   `-- <uuid-2>/egroups/
`-- fence/                        # On-disk persisted epoch fencing table
    `-- <vdisk-uuid>.epoch
```

### Extent Model & Extent Groups

* **Extent (1 MiB):** The atomic addressable data block. Every extent corresponds to a 1 MiB offset aligned within a virtual disk.
* **Extent Group (4 MiB):** An append-only physical file holding up to four 1 MiB extents. Extent groups undergo three lifecycle phases:
  1. **Open:** Actively receiving appends from the local drain engine.
  2. **Sealed:** Fully written (or closed on detach/takeover). Once sealed, the file is **strictly immutable**. Its contents are never altered.
  3. **Dead / Reclaimed:** Extents inside the group have been superseded by newer writes. Reclaimed by Purah's mark-sweep garbage collector.

### Extent Integrity & Self-Identification

Every stored extent contains an embedded footer containing:
1. **CRC32C Checksum:** Verifies physical block integrity against bit rot and disk corruption.
2. **Identity Metadata (`vdisk_id`, `extent_index`):** Ensures that the block physically located on disk belongs to the expected logical offset of the requesting virtual disk. Misdirected reads or stale metadata pointers are detected immediately at read time.

---

## 3. Data Path & Write Pipeline

Sidon enforces one fundamental performance invariant: **Guest writes never wait for metadata updates in Hydra.**

```
+------------------------------------------------------------------------+
|                          Guest Write Pipeline                          |
+------------------------------------------------------------------------+

  Guest (QEMU) writes 128 KiB
            |
            v
  [ NBD Unix Socket ]
            |
            v
  1. Append under Vdisk Lock (Local Journal)
     - Allocate contiguous sequence number (seq)
     - Append record to <vdisk>.jrn
     - Enqueue Ticket into Commit Queue
            |
            v
  2. Commit Batch (Unlocked Group Commit)
     - First waiting thread becomes Leader
     - Coalesce all waiting tickets (up to 32 MiB)
     +-----------------------------------+-------------------------------+
     |                                   |                               |
     v                                   v                               v
   Local Disk                   Replica 1 (TCP:9105)           Replica 2 (TCP:9105)
   fdatasync(<vdisk>.jrn)       OP_APPEND (APPEND_DEFER_SYNC)  OP_APPEND (APPEND_DEFER_SYNC)
                                OP_APPEND (Final + fdatasync)  OP_APPEND (Final + fdatasync)
     |                                   |                               |
     +-----------------------------------+-------------------------------+
                                         |
                                         v (All replicas confirmed durable)
  3. Publish under Vdisk Lock
     - Insert sequence ranges into in-memory Overlay Map
     - Mark tickets complete
            |
            v
  Acknowledge OK to Guest via NBD
```

### Group Commit Pipeline (`vdisk/commit.rs`)

To achieve maximum throughput under heavy guest queue depths, Sidon implements lock-free group commit:
1. **Append Phase:** Under the vdisk lock (lasting microseconds), the write is converted into journal records, assigned monotonically increasing sequence numbers, written to the local journal file, and a `Ticket` is pushed to the queue.
2. **Commit Phase (Unlocked):** The first writer to notice no active commit becomes the batch **Leader**. The leader pulls all tickets currently queued (up to `BATCH_MAX = 32 MiB`). It issues a single `fdatasync` to the local journal while simultaneously transmitting parallel `OP_APPEND` network requests to all remote replicas.
3. **`APPEND_DEFER_SYNC` Wire Optimization:** When transmitting a batch of records, intermediate records carry the `APPEND_DEFER_SYNC` flag. Remote replicas buffer writes without invoking `fdatasync`. Only the final record in the batch omits the flag, causing the replica to issue a single `fdatasync` covering the entire batch.
4. **Publish Phase:** Once the local sync and **every** replica acknowledge durability, the leader enters the vdisk lock, commits the sequence ranges into the in-memory `Overlay`, and releases all waiting writer tickets.

### Write-All Replication ($RF=2F+1$)

Sidon utilizes a **write-all, not quorum** journal replication model:
* A write is acknowledged to the guest if and only if it has reached durability on **100% of designated replicas**.
* If a single replica drops offline or fails its fsync, writes to that vdisk pause or return `EIO`.
* **The Safety Advantage:** Write-all replication provides a mathematically trivial, unbreakable fencing proof: Fencing **one** replica at epoch $e+1$ guarantees that a stale owner at epoch $e$ can never complete another write, because the old owner required all replicas to acknowledge. This eliminates the edge cases, multi-round Paxos leases, and split-brain windows inherent in quorum-based write protocols.
* **Rapid Self-Healing:** When a replica is lost, Purah automatically detects the degradation within ~3 seconds, allocates a spare healthy node, adds it to the write-all set, and restores write service.

### Journal Drain Mechanism

When the journal file reaches its configured high-water mark (`SIDON_HIGH_WATER`, default 64 MiB), a background drain thread triggers:
1. **Plan (under vdisk lock):** `<vdisk>.jrn` is renamed to `<vdisk>.jrn.old`. A new, empty `<vdisk>.jrn` is opened to accept live guest writes. The overlay ranges pointing into `.old` are frozen.
2. **Execute (unlocked):** The drain thread reads `.old`, synthesizes 1 MiB extents, appends them to open 4 MiB extent groups, replicates the extent groups to peers (`OP_EGROUP_PUT`), and syncs. Once confirmed on all replicas, the new block map entries are written to Hydra via Daruk CAS.
3. **Finish (under vdisk lock):** The drained ranges in the overlay are pruned, `<vdisk>.jrn.old` is unlinked, and replicas are notified via `OP_TRUNCATE_TO(seq)` to truncate journal records prior to the live sequence number.
4. **Hard Ceiling Protection:** If a guest writes faster than the drain can flush extents and the journal reaches `SIDON_HARD_CEILING` (default 128 MiB), writes pause until the drain clears space, preventing host disk exhaustion.

---

## 4. Peer Wire Protocol & Replication Mesh

All inter-node communication occurs over mutual TLS on port **9105**.

### Wire Framing Format

```
Request Header (44 bytes):
+-------------------+-------------------+-------------------+-------------------+
|  Magic (0x53445052 "SDPR") [u32]      | Opcode [u16]      | Flags [u16]       |
+-------------------+-------------------+-------------------+-------------------+
| Vdisk Len [u16]   | Padding [u16]     | Writer Epoch [u64]                    |
+---------------------------------------+---------------------------------------+
| Sequence Number [u64]                 | Offset [u64]                          |
+-------------------+-------------------+-------------------+-------------------+
| Payload Len [u32] | CRC32C [u32]      | Vdisk Name [Vdisk Len bytes]          |
+-------------------+-------------------+---------------------------------------+
| Data Payload [Payload Len bytes]                                              |
+-------------------------------------------------------------------------------+

Response Header (24 bytes):
+-------------------+-------------------+-------------------+-------------------+
|  Magic (0x53445052 "SDPR") [u32]      | Status [u16]      | Padding [u16]     |
+-------------------+-------------------+-------------------+-------------------+
| Epoch [u64]                           | Payload Len [u32] | CRC32C [u32]      |
+---------------------------------------+-------------------+-------------------+
| Data Payload [Payload Len bytes]                                              |
+-------------------------------------------------------------------------------+
```

The CRC32C covers the header, the vdisk name, and the payload. Any packet failing CRC validation triggers an immediate connection drop to avoid desynchronization hazards.

### Core Opcodes

| Opcode | Name | Purpose |
| :--- | :--- | :--- |
| `1` | `OP_PING` | Liveness health check between node pairs. |
| `2` | `OP_APPEND` | Replicate journal append records. Accepts `APPEND_DEFER_SYNC` (0x01). |
| `3` | `OP_FENCE` | Atomically persist and fsync a new fencing epoch to disk. |
| `4` | `OP_READ_TAIL` | Read active journal records during takeover recovery. |
| `5` | `OP_TRUNCATE` | Truncate entire journal on disk. |
| `6` | `OP_EGROUP_PUT`| Transmit 1 MiB extent or sealed 4 MiB extent group file to replica. |
| `7` | `OP_EGROUP_GET`| Read an extent group from remote replica during read-repair. |
| `8` | `OP_FORWARD_READ`| Relays guest read from non-owner to vdisk owner during migration. |
| `9` | `OP_FORWARD_WRITE`| Relays guest write from non-owner to vdisk owner during migration. |
| `10`| `OP_TRUNCATE_TO`| Safely trims drained prefix of journal up to sequence number `seq`. |
| `11`| `OP_EGROUP_DROP`| Requests replica to drop dead extent groups confirmed unreferenced. |
| `12`| `OP_RELEASE` | Asks vdisk owner to flush/drain journal and release socket for handover.|

---

## 5. Ownership, Epoch Fencing, and Live Migration Handover

### Single Writer & Fencing Model

Every vdisk has exactly one active owner node at any moment. Ownership is persisted in Hydra:
`hydra.dfs_vdisk_meta (vdisk_id, owner, epoch, replica_set, created_at)`

* Any transfer of ownership requires a Daruk CAS conditioned on **both** the previous `owner` and `epoch`.
* Replicas enforce the epoch fence: When a node is fenced at epoch $e+1$, it writes the epoch to `/var/lib/hci/sidon/fence/<vdisk>.epoch` and issues an `fdatasync`. Any subsequent append arriving with an epoch $\le e$ is instantly rejected with `ST_FENCED`.

### Live Migration Handover Protocol (`handover.rs`)

Live migrating a VM without dropping storage connections requires coordinating QEMU, libvirt, and Sidon without a single dropped I/O request. Sidon achieves this via the **Handover Switch**:

```
+-----------------------------------------------------------------------------+
|                      Live Migration Handover Sequence                       |
+-----------------------------------------------------------------------------+

  Source Host (Owner, Epoch e)                  Destination Host (Target)
  ============================                  =========================
  1. VM running, Sidon serves disk locally.
                                                2. Prepare Destination Storage:
                                                   Attach vdisk with `forward: true`.
                                                   Destination Sidon creates NBD socket
                                                   backed by Forwarding Switch.
                                                3. Vali executes `virsh migrate`.
                                                   QEMU connects to destination NBD socket.
                                                   Writes are forwarded to Source via
                                                   OP_FORWARD_WRITE.
  4. QEMU migration completes.
     Guest resumes on Destination!
                                                5. Takeover Triggered:
                                                   a. STALL: Destination switch halts
                                                      incoming guest I/O; waits for
                                                      in-flight requests to complete.
  6. Receive OP_RELEASE (12) <-------------------- b. RELEASE: Destination sends OP_RELEASE.
     - Flush active writes.
     - Execute journal drain.
     - Close local NBD socket.
     - Reply Released::Yes --------------------->
                                                   c. CLAIM & FENCE:
                                                      - CAS in Hydra: epoch e -> e+1.
                                                      - FENCE all replicas at epoch e+1.
                                                      - Adopt journal tail.
                                                   d. INSTALL:
                                                      - Mount local vdisk behind switch.
                                                      - Release STALL.
                                                6. Guest I/O flows locally! Zero drops.
```

If any failure occurs before step 5b, the owner is unchanged and forwarding continues safely. If a failure occurs after release, running takeover again completes the claim safely.

---

## 6. Purah Background Curator

Purah runs as a leader-elected background thread pool within Sidon, providing continuous autonomous health and storage maintenance:

### 1. Mark-Sweep Garbage Collection (`reclaim.rs`)
* **Zero Distributed Reference Counting:** Reference counters across clusters lead to desynchronization and data loss during unclean shutdowns. Purah uses pure mark-sweep.
* **The Two-Scan Safety Rule:** An extent group is pruned only if it is observed unreferenced on **two successive scans separated by a grace period** (default 10 minutes), and is not held by an attached vdisk, not open, and not young. This prevents reclaiming newly appended extents whose Hydra block-map updates are currently in flight.
* **Replica Drop Protocol:** Once a dead group is deleted locally, Purah transmits `OP_EGROUP_DROP` (11) to replicas, which verify the group is dead in Hydra before unlinking.

### 2. Autonomous Re-Replication (`replica.rs`)
* When Mipha marks a host `DOWN` or a replica reports persistent I/O errors, Purah selects a healthy replacement host from the cluster.
* **Write-All First Invariant:** The replacement node is added to the active write-all replication set **before** copying existing extents. This guarantees that new writes are immediately protected, avoiding synchronization gaps.
* Extent groups are then backfilled in the background.

### 3. Background Scrubbing
* Purah iteratively scans all sealed extent groups on disk.
* Because sealed groups are immutable, scrubbing requires no locking.
* Reads the file, computes CRC32C, and compares against the seal hash stored in `dfs_egroups`.
* If a corrupted extent is detected, Purah automatically executes **read-repair**, pulling a known-good copy from an uncorrupted replica (`OP_EGROUP_GET`) and rewriting the damaged block.

### 4. Heat Tracking & Storage Tiering (`heat.rs`, `tier.rs`)
* Tracks read and write frequencies per extent group in memory.
* Asynchronously flushes metrics to `hydra.dfs_egroup_access`.
* Supports operator-driven relocation of cold extent groups from fast NVMe storage to high-capacity SATA/spinning disks (`valcli storage.tier`, `valcli storage.move`).

---

## 7. Operator CLI & Diagnostic Commands

Storage operations are managed via the unified `valcli storage` CLI suite:

### Listing Virtual Disks
```bash
valcli storage.list
```
Displays all configured virtual disks, current owner node, active epoch, replication factor, journal size, and drain state.

### Inspecting Virtual Disk Details
```bash
valcli storage.status <vdisk-name>
```
Displays detailed metrics including:
* Active NBD socket path
* High-water mark and hard ceiling thresholds
* Number of allocated extents and sealed extent groups
* Replica node status and journal synchronization lag

### Triggering Manual Scrub
```bash
valcli storage.scrub [--repair]
```
Forces an immediate checksum audit of all local sealed extent groups.

### Triggering Extent Group Compaction
```bash
valcli storage.compact <vdisk-name>
```
Scans sparse extent groups (where dead extents exceed 50%), copies remaining live extents into a dense new extent group, updates Hydra mappings via CAS, and frees the sparse groups.

### Estimating Deduplication Savings
```bash
valcli storage.dedup.estimate <vdisk-name>
```
Performs a read-only cryptographic fingerprint scan of all live extents to report prospective deduplication space savings without altering on-disk structures.

---

## 8. Summary of Architectural Guarantees

| Invariant | Guarantee | Mechanism |
| :--- | :--- | :--- |
| **I-1: Zero Stale Writes** | A partitioned or delayed node can never commit writes after being deposed. | Replica-enforced on-disk fsynced epoch fencing (`OP_FENCE`). |
| **I-2: Immutability** | Sealed extent groups are never modified in place. | Append-only architecture; updates generate new extents and repoint metadata. |
| **I-3: Zero Uncommitted Reads**| Guest reads never observe uncommitted or un-replicated writes. | Writes are published to the overlay map only after full replica fsync confirmation. |
| **I-4: Data Before Metadata** | Extent bytes are durable on disk before any Hydra map row references them. | Strict pipeline sequencing: disk write $\to$ replica write $\to$ Daruk CAS metadata update. |
| **I-5: Safe Reclamation** | Live extents are never prematurely freed. | Two-scan mark-sweep with mandatory grace period; zero distributed refcounters. |
| **I-6: Hitless Live Migration**| No dropped I/O requests during live hypervisor migration. | Forwarding NBD switch with synchronized I/O stall and `OP_RELEASE` handover. |
