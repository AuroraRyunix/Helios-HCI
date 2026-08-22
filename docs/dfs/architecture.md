# Architecture

What the DFS is shaped like, and why that shape. The contract it must satisfy is in
[invariants.md](./invariants.md); the protocols that satisfy it are in
[data-path.md](./data-path.md) and [ownership.md](./ownership.md).

## 1. Why the current substrate cannot get there

DRBD's unit of replication is a *device*. Each replicated volume is a DRBD resource; each
resource is a standing connection between named peers; each connection needs a TCP port, a
kernel minor, its own worker threads, and RF−1 sockets per node. Four consequences, in
increasing order of severity:

1. **The port ceiling.** LINSTOR's `TcpPortAutoRange` default is 7700–7890: 191
   replicated volumes cluster-wide, regardless of node count. Verified on the reference
   cluster. Widening it is trivial and should happen regardless — but it buys one order
   of magnitude, not a shape change.
2. **Kernel-object cost per volume.** A thousand VM disks means a thousand DRBD minors
   and roughly two thousand standing TCP connections per node. The kernel will do it; it
   will not enjoy it, and neither will anyone debugging it.
3. **Placement is static.** A DRBD resource lives where it was created. Rebalancing means
   full-resource moves; there is no notion of moving a megabyte.
4. **Dual-primary is the only multi-attach story.** The golden-image case needs
   `allow-two-primaries`, which is precisely the option that let one VM run on two hosts
   and corrupt its own disk. The current code handles it carefully; a correct substrate
   would not offer the footgun at all.

The public Nutanix architecture solves all four the same way: replicate *extents*, not
devices. A vDisk is cut into extents grouped into extent groups; one Stargate per node
writes each group to two or three peers over a small number of shared channels; Medusa —
their modified Cassandra — holds only the map. Adding a VM adds metadata rows, not
network sessions.

Helios already holds the half of that which is usually gotten wrong: Daruk's typed
compare-and-swap endpoints, prepared statements at QUORUM with SERIAL, an ordered and
recorded schema, ring lifecycle behind a quorum gate, and a backup that has been restored.
What is missing is the data path. That is this design.

## 2. The shape

```
   guest (qemu 10.1)
      │  NBD over a unix socket, one per attached vdisk
      ▼
   Sidon (local, always)  ── owns this vdisk? ──no──►  forward to owner Sidon
      │ yes                                            (mTLS, port 9105)
      ├── journal append ──► RF replica Sidons (write-all, epoch-stamped)
      ├── drain (async) ──► extent groups on RF nodes' local XFS
      └── map commits ────► Daruk ► Hydra   (batched; 1 LWT per batch)

   ZooKeeper: liveness and Purah leader election only. Never in the data path.
   Hydra:     the map only. Guest bytes never pass through it.
```

Rules that define the shape:

- **qemu talks to the local Sidon, always.** Whether that Sidon owns the vdisk is
  invisible to the guest: a non-owner forwards to the owner over the peer channel. This
  single rule is what makes live migration require no qemu storage tricks at all
  (see [ownership.md §5](./ownership.md)) and what lets data locality be an optimisation
  instead of a correctness requirement.
- **One listener per node.** Sidon binds one mTLS port (9105, reserved in
  [decisions.md](./decisions.md)) using the cluster CA and per-node certificates with the
  IP SANs provisioning now issues. Peer connections are per node-pair, multiplexed:
  N nodes means N−1 standing connections per node — independent of VM count. This is the
  entire point.
- **Hydra is the map, never the bytes.** The strict tier boundary the console work
  established ("the web tier never touches the data path") has a storage-tier twin: the
  metadata tier never carries payloads. A `SELECT` returning guest data would be an
  architecture violation, not a bug.
- **No new consensus systems.** The cluster already runs two coordination mechanisms
  (ZooKeeper sessions; Scylla lightweight transactions). The DFS adds zero: ownership is
  a Hydra CAS, ordering within a vdisk comes from having a single writer, and replica
  agreement comes from write-all with epoch fencing. Every alternative design that added
  a Raft group per vdisk or an etcd was rejected for that reason alone.

## 3. The data model

| Object | Size | What it is |
|---|---|---|
| **vDisk** | up to TiB | What the guest sees. Sparse. Identified by UUID. Belongs to a storage container; the container's existing `ftt` column gives RF = ftt+1. |
| **Extent** | 1 MiB | A contiguous logical slice of a vDisk. The unit the map speaks in. |
| **Extent group (egroup)** | 4 MiB | The physical unit: one file on a node's local filesystem, replicated RF ways. Append-only while *open*; *sealed* egroups are immutable until GC. Holds extents, usually from one vDisk. |
| **Journal** | 64 MiB chunks | Per-vdisk replicated write-ahead log — special egroups with write-all semantics. Load-bearing, not an optimisation: see [data-path.md §2](./data-path.md). |
| **Map** | rows in Hydra | Two levels, as in the Nutanix design: vdisk block map (vdisk+extent-index → egroup+offset) and egroup map (egroup → replica locations, state, seal hash). |

Sealed-egroup immutability is the load-bearing simplification of the whole design.
Replication repair on immutable data is "compare lengths and checksums"; divergence is
only possible on open egroups and the journal, both of which are write-all and
epoch-fenced; snapshots become map copies with no data movement; and the scrubber can
verify a sealed egroup against its recorded hash with no locking at all.

## 4. Where the bytes physically live

Egroups and journal chunks are ordinary files under an XFS filesystem on a thin LV in
`vg_aether` — the same volume group and thin pool LINSTOR already draws from (verified on
the reference node: the pool owns the whole VG). Both substrates therefore coexist on the
same disks with no repartitioning during the migration years, at the cost of one honest
caveat: **capacity accounting must sum both consumers of the pool**, and the existing
free-space readers (the DRS gate, the storage page) must learn that before the first
byte of DFS data lands.

No LVM-per-egroup, no raw block devices, no custom on-disk format below the egroup file:
the filesystem gives us allocation, naming and fsync semantics that are boring and
correct, and boring is the highest compliment a storage substrate's bottom layer can be
paid.

## 5. Deliberately out of scope

Stated here so their absence is a decision, not an accident. None of these is in v1, and
several should never be built:

- **Deduplication, compression, erasure coding, tiering** — each multiplies the state
  space Ganon must cover. Revisit individually, after years of stable operation, if ever.
- **Multi-writer vdisks** (clustered guest filesystems) — unsupported, refused at attach.
  With one deliberate exception: an **immutable vdisk class** for golden images, served
  read-only from any replica without a lease. That replaces `allow-two-primaries`
  entirely, and it is *safer* than what it replaces because immutability is enforced by
  the map rather than promised by convention.
- **QoS / throttling** — until there is contention worth shaping.
- **A caching tier** — data locality (one replica on the writer's node) is the v1 cache.
- **Forking Scylla.** Nutanix modified Cassandra because 2011-era Cassandra lacked what
  they needed. Scylla 5.4 already provides lightweight transactions, and Helios's
  consistency discipline lives in Daruk — around the database, not inside it. Maintaining
  a C++ database fork is a treadmill with no payoff here; this is the recorded answer to
  "should we customise Scylla like Nutanix did Cassandra".

## 6. Implementation language

Rust. Not by taste: this is the one component where a stray pointer or a data race *is*
silent corruption discovered weeks later, the repo already ships a Rust daemon (Agahnim,
Tokio-based), and — verified — every node already carries the toolchain
(rustc/cargo 1.92, installed for Agahnim's on-node build). The deployment path for a Rust
daemon therefore already exists in `provision.py`.

Python remains what it is here: the control plane's language. Sidon's CLI glue and test
drivers may be Python; nothing on the byte path may be.
