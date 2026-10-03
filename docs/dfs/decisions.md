# Decisions

The ADR list. Every entry names the alternatives it beat and the reasoning, because a
decision whose alternatives are forgotten gets relitigated by whoever joins next — or
silently reversed by whoever implements next.

**D-1 — Build, not adopt.** Alternatives: Ceph RBD (removes the per-device model without
writing a storage system; MON/MGR/OSD operational weight; a second cluster to operate
inside the first), stay on LINSTOR with a widened port range (sufficient below ~1k
volumes; the shape remains device-per-connection). Decided by the owner with the trade
stated plainly. **And decided as a replacement, not an addition**: LINSTOR and DRBD are
removed once their disks are moved, because the cost being escaped is operating a storage
layer, and operating two is worse than operating either. The port-range widening survives
only as an interim measure for whatever runs before the cutover.

**D-2 — Rust for everything on the byte path.** Alternatives: C (the safety burden is
the whole objection), Go (GC pauses tolerable, but the io_uring/cgo story and the
absence of any Go in this repo both count against), Python (control plane only, never
bytes). Precedent: Agahnim. Verified: rustc/cargo 1.92 already on every node via the
existing provisioning package set, so the build-on-node path exists.

**D-3 — qemu attaches over NBD on a unix socket (v1).** Alternatives: iSCSI to
localhost (what Nutanix ships; drags in a whole target stack for no v1 gain),
vhost-user-blk (the v2 performance path — shared-memory rings; qemu 10.1 supports it,
adopt in milestone 9), ublk (kernel 6.12 has it; a real /dev node, but a kernel
dependency NBD avoids). NBD is qemu-native, needs no kernel module, and keeps v1 purely
in userspace. Verified: qemu 10.1.0 on the reference node.

**D-4 — Append-only egroups; sealed means immutable; redirect-on-write.** Alternative:
overwrite-in-place (no GC needed, but open files whose replicas can diverge in the
middle, and repair protocols for that divergence). Immutability makes repair a checksum
comparison, snapshots a map copy, and scrub lock-free. The price is GC, which is a
performance problem; divergence repair is a correctness problem; pay in performance.

**D-5 — The journal is write-all / read-one, not quorum.** The takeover proof
([ownership.md §3](./ownership.md)) is three lines *because* fencing one replica stops
the old owner and reading one replica sees every ack. Quorum journals buy availability
during single-replica loss and cost exactly that proof. DRBD protocol C users already
accept this trade, so it regresses nobody.

**D-6 — Ownership is a Hydra CAS plus replica-side epoch fencing; the lease is not the
safety mechanism.** Alternatives: lease-only (dies on clock skew and slow watchers —
the SIGSTOP scenario), per-vdisk Raft (a consensus instance per disk; operational
insanity at thousands), ZooKeeper ephemerals for ownership (couples the data path's
safety to ZK session semantics and adds a second authority beside Hydra's CAS). Replicas
rejecting stale epochs is the one mechanism that works when the deposed node is wedged,
lying, or living in the past.

**D-7 — Block-map writes are plain QUORUM, not LWT; drains commit through one
`drain_seq` CAS per batch.** Reasoning and the rejected reconciliation-on-takeover
alternative in [metadata.md §3–4](./metadata.md). The inviolable rule underneath both:
guest acknowledgement never waits on Hydra.

**D-8 — GC is Purah's mark-sweep with a two-scan grace; no reference counts, ever.**
Distributed refcounts are a standing bug class (every crash between data-op and
count-op; every clone doubling). The schema is forbidden a refcount column by
[metadata.md §1](./metadata.md); slow reclaim is the accepted price.

**D-9 — CRC32C, 32 KiB slices, algorithm byte in the footer.** Detection is the job,
not authentication; CRC32C is hardware-accelerated everywhere this runs; the algorithm
byte makes "stronger later" a value change, not a format migration.

**D-10 — RF comes from the existing `storage_containers.ftt` column (RF = ftt+1).** No
parallel policy object; the concept operators already have keeps meaning what it meant.

  Decided here, and then not done: `op_create` defaulted `rf` to 1 and read neither the
  container nor `cluster.json`, so every vdisk was single-copy no matter what either
  said. Implemented 2026-09-10, with `cluster.json`'s `redundancy_factor` as the fallback
  for a container that sets no `ftt` — which, until every caller passes a container, is
  most of them. The `+1` in this line is the entire defect: `ftt` counts failures
  survived and `dfs_vdisks.rf` counts copies, and dropping the conversion turns "survive
  one host loss" into "keep one copy" without failing anything visible.

**D-11 — One data port, 9105, mTLS with the cluster CA and per-node IP-SAN certs.**
Reserved here; lands in `network.md` at implementation. Peer connections are per
node-pair — the head count that was the whole complaint about the old substrate.

**D-12 — Immutable image class instead of any multi-writer mode.** Replaces
`allow-two-primaries` with a category that cannot express the hazard. General
multi-writer vdisks are refused at attach, indefinitely.

**D-13 — Egroups are files on XFS on a thin LV in `vg_aether`.** Alternatives: raw
block management (reinventing an allocator to save a filesystem's overhead), LVM-per-
egroup (metadata churn at 4 MiB granularity). Verified: the thin pool already owns the
whole VG, so both substrates coexist without repartitioning; the capacity-accounting
caveat is recorded in [architecture.md §4](./architecture.md) and lands on the readers
in milestone 7.

**D-14 — No Scylla fork.** The recorded answer to "customise Scylla like Nutanix did
Cassandra": Nutanix patched a 2011 Cassandra that lacked what they needed; Scylla 5.4
has LWT, and Helios's discipline lives in Daruk — around the database, not in it. A
C++ database fork is a permanent maintenance tax with no capability it buys here.

**D-15 — Harness before filesystem, calibrated on DRBD.** The methodological spine:
a harness debugged against the code it gates learns that code's blind spots. Ganon is
validated against a substrate known to be correct, gains standalone value attacking the
shipping product, and thereafter outranks the schedule ([milestones.md](./milestones.md),
final rule).

**D-16 — Names: Sidon, Purah, Ganon.** Sidon (data path — Mipha's sibling, as HA and
storage fencing genuinely are here), Purah (the scanner), Ganon (the recurring calamity
the kingdom prepares for). Rejected: Revali — completes the champions set but greps as a
substring of `revalidate`, which already appears in `spectrum_server.py` — and a name
you cannot grep for cleanly is a name that costs debugging time on every future search.
(The collision is with `revalidate`, not with `vali` itself; `vali` and `revali` are
distinct tokens.) **Aether is not renamed**: it keeps its name, its documents and its
meaning as the Linstor/DRBD substrate, and the two run side by side — reusing the name
would make every sentence in every older document ambiguous about which layer it meant.
`vg_aether` likewise stays, historical rather than descriptive, because renaming a
volume group under live data to fix an aesthetic is not a trade worth making. The CLI is
deliberately unnamed until milestone 7 rather than named badly now.

**D-17 — A single node is a supported topology, not a stepping stone.** The alternative,
adopted by most distributed stores, is to treat ftt=0 as a development mode: correctness
arguments assume peers, the single-node path is the one nobody soaks, and the smallest
deployment gets the least-tested code. Rejected because the reference cluster *is*
single-node and many installs will never be otherwise. Concretely this commits three
things: epoch fencing is never conditional on peer count (at RF1 it fences the previous
*process*, which is a real hazard, not a formality); ftt=0's durability limit is stated in
the invariant rather than hidden in a footnote; and Ganon gets a single-node soak tier
that runs because the configuration ships, not because the lab is small. See
[architecture.md §5](./architecture.md) and [ganon.md §5](./ganon.md).

**D-18 — An extent's identity lives on the block-map row, not on the vdisk reading it.**
The footer of every stored extent carries a checksum *and* an identity (which vdisk,
which extent index), because a correct checksum proves the bytes are undamaged and only
the identity proves they are the *right* bytes. Reads verified that identity against the
reading vdisk's own hash, which is the same thing exactly until extents are legitimately
shared — and a snapshot shares every one of them. The first snapshot ever taken returned
EIO on every read.

Alternatives: **check only the extent index** (keeps one field, throws away the half of
the guarantee that catches a misdirected read landing on the right offset of the wrong
disk — rejected, that is the case the identity exists for); **give the child the
parent's hash** (works until the child writes, at which point one vdisk's map holds
extents under two identities anyway, so it solves nothing and hides the problem);
**rewrite the footers on copy** (restores the invariant and makes a snapshot a data copy,
which is the entire thing a snapshot is not).

Decided: the map row records the hash the extent was written under, and the reader
verifies against that. Per row rather than per vdisk, because a clone genuinely holds a
mix — inherited extents keep the parent's stamp, and anything it rewrites takes its own.
What the check stops asserting is "the reader is the vdisk named in the footer", which
was only ever a proxy for "these are the right bytes" and becomes false the moment
sharing is legal. What it still asserts is unchanged: right extent, right index, right
writer, undamaged.

The migration needs no backfill. A row without the column was written by the vdisk that
owns it, so falling back to the reader's own hash is correct for every row predating
snapshots.

**D-19 — Snapshots need no reference counts, and that is the whole reason they are
cheap.** D-9 rejected refcounts for garbage collection on the grounds that a refcount is
a distributed counter with a crash window between every data operation and its count
operation. The bill for that decision was a full scan of `dfs_block_map` on every sweep.

Snapshots are where it pays back. Purah marks from the entire block map, so an extent
group referenced by a child is live whether or not the child is attached, whether or not
the parent still exists, and with no bookkeeping performed at snapshot time at all.
Taking a snapshot is a map copy and a class transition; deleting a parent is a row
delete. Neither touches a counter, because there is none to touch, and neither can leave
one wrong.

Had the design kept refcounts, every one of these would have needed a matching increment
or decrement, each with its own crash window, and the interesting bug would not be "the
snapshot is missing" but "the extents under a live snapshot were freed".

**D-20 — mutual TLS on 9105, in-process, with plain rustls.** The port carries guest
data: an `APPEND` payload is the literal bytes a VM just wrote. It also carries `FENCE`,
which is the part that decided this. Encryption alone would have left the fencing
mechanism open to anyone who could reach the port -- raise the epoch on a vdisk and every
replica refuses the real owner's writes -- so the requirement was authentication, and it
had to be mutual because both ends are peers rather than a client and a service.

Alternatives: **system OpenSSL via the `openssl` crate** (fewer vendored crates, and the
distro patches it; rejected for putting a C FFI on the byte path and coupling the build to
whichever OpenSSL the distro ships); **out-of-band encryption, WireGuard between nodes**
(keeps sidon at one dependency and moves peer identity into the kernel; rejected because
it adds an operational component and an ordering problem -- sidon must not start before
the tunnel, or the bind guard protects nothing); **routing replication through
spark-daemon's existing mTLS** (rejected outright: it would put a Python HTTP hop on every
guest write, breaking the one rule that acknowledgement never waits on anything slow).

Decided: `rustls` 0.21, matching agahnim, whose crates were already on every node.
**Plain rustls, not `tokio-rustls`** -- agahnim is async and sidon is not, and pulling in
a runtime for the transport would restructure the data path to solve a problem the data
path does not have. The certificates are the cluster's own, from Impa, so the storage tier
introduces no second credential to forget to renew.

Two things follow that are worth stating because both are easy to reverse by accident.
**Missing material is a refusal, never a downgrade**: a daemon that quietly serves guest
data in the clear because a file was absent is worse than one that will not start.
**Loopback stays plaintext**, because a connection that cannot leave the host cannot be
intercepted off it, and single-host multi-instance testing is how the protocol gets
exercised without three nodes. The rule lives in one function, `wire_policy`, consulted by
both the listener and the client -- the way two ends of a policy stop agreeing is by each
implementing it separately.

**D-21 — the peer list comes from `cluster.json`, not from the unit file.** The
alternative is generating `SIDON_PEERS=` into `sidon.service` at provision time, which is
one source of truth too many: adding or decommissioning a node rewrites `cluster.json` on
every host already, while a unit generated once describes the cluster as it was the day
the node was built. `SIDON_PEERS` still overrides, because tests need to name peers that
are in no cluster document. A single-host document binds loopback, since at ftt=0 there is
nothing to replicate to and demanding certificates to serve traffic that will never arrive
is a provisioning failure waiting to happen.

**D-22 — access data is approximate, aggregated in memory, and about extent groups rather
than part of them.** Tiering needs to know what is hot, and nothing recorded it:
`multi_disk.md` could place an extent group by free space, which needs no history, and
could not place one by temperature. `hydra.dfs_egroup_access` is the Nutanix
`medusa_extentgroupaccessdatamap` analog, and three alternatives were rejected on the way
to it.

**Per-access metadata writes** — the straightforward version, where a read records itself.
Rejected outright: it puts a metadata round trip on the read path, which is the one rule
the whole design does not bend (metadata.md §5). The counters accumulate in this process
and reach Hydra on a timer instead, which is why they are approximate: a crash loses
everything counted since the last flush and reopens the window.

**CQL counter columns** — what the data obviously is. Rejected because counters are not
idempotent under retry, so a timed-out flush may be applied twice, and because a counter
table cannot hold the non-counter columns the timestamps need. The flush writes *absolute*
totals instead, which makes it safe to fire and forget: lost changes nothing, duplicated
changes nothing. This is D-8's objection to distributed counters applied to a statistic,
and it is the reason the rows are keyed `((egroup_id), node)` — absolute writes to a shared
row would have each node clobber the others.

**Storing it in the sealed footer, beside the checksum and the identity.** Rejected on the
sealing invariant: a sealed extent group is immutable, which is what makes scrub lock-free
and repair a checksum comparison (D-4), and access data changes constantly. The access data
is *about* an extent group, not part of one — which is also what made this addable as a
migration rather than a format change.

What the trade commits to is a boundary, and it is worth stating because exceeding it is
easy and quiet: **approximate counters may decide where a copy of data goes, never whether
it exists.** Being wrong about placement costs a misplaced extent group and a later
migration. Using the same numbers as a reclamation input, an eviction trigger or a replica
decision would let a lost flush cost data, and that is refused.

Reporting only. The pass ranks and prints; nothing migrates an extent group on the strength
of a statistic nobody has read yet. A curator that started moving data the moment it could
measure temperature is how a tiering feature becomes the reason a node is busy.

**D-23 — the extent ID map is designed and reserved, not built.** Helios has two levels
where Nutanix has three: `dfs_block_map` points `(vdisk, extent_index)` straight at an
extent group, where their `medusa_vdiskblockmap` points at an *extent* and
`medusa_extentidmap` then points that extent at a group. The missing middle level is what
makes an extent an addressable thing several vdisks can reference by name, which is the
precondition for extent-granular clone divergence and the only thing that makes
deduplication expressible at all.

The schema is written down below. It takes **the next free migration id when it is built**
-- deliberately not a number reserved in advance. This was first written as `0013`, and the
task-tree and access-data migrations landed the same week and took 0011 through 0017, so
the reservation named an id that belonged to something else. A number held for a table that
does not exist yet is a collision waiting for the first migration that needs one. The table is deliberately *not* created yet, and that is the decision rather than an
omission: `multi_disk.md` already records what an empty table with a suggestive shape costs
— `dfs_egroup_replicas` sat in the schema with nothing writing to it, and every later
design had to begin by establishing that it was not a source of truth. One of those is
enough.

```
dfs_extent_id_map
  extent_id text PRIMARY KEY,     -- content- or birth-derived; see below
  egroup_id text, egroup_offset int, length int,
  vdisk_hash bigint,              -- the identity the footer was stamped with (D-18)
  created_at_ms bigint
```

and `dfs_block_map` grows one nullable column, `extent_id text`. **Null means the two-level
path**, so every vdisk that exists today keeps resolving reads exactly as it does now,
byte for byte, indefinitely. This is not a migration that rewrites the block map, and it
must never become one.

Two things make this larger than it looks, and they are the reason it is not being landed
on the strength of a flag.

*Purah's mark phase reads `egroup_id` off the block map.* With the indirection live, a
vdisk's rows name extents rather than groups, and a sweep that still marked from
`egroup_id` would see every one of those groups as unreferenced — and the two-scan grace
would delay the deletion of live data by ten minutes rather than prevent it. Marking has to
traverse both levels before the first row can carry an extent id, and that ordering is the
whole hazard: the flag protects the read path and does nothing for the curator, which runs
on a timer whether anyone opted in or not.

*Every path that copies or repoints a map row has to understand both shapes* —
`derive_child`, the drain's commit, resize, delete, and the heal's replica accounting. A
flag that covered the read path and missed one of those is precisely the half-migrated read
path this entry exists to avoid.

So the plan, in the order it has to happen: teach Purah to mark through both levels and
soak that against a cluster where no row has an extent id (a no-op change, fully testable
before anything depends on it); then the migration and a resolver that treats a null
`extent_id` as today's path; then writing extent ids behind a per-container opt-in, never a
cluster switch; then clone divergence, which is the first thing that *gains* anything.

**Dedup is not being built, and the reason is not difficulty.** On top of the extent id map
it needs a content hash as the extent id, a by-hash index to find candidates, and — this is
the part that decides it — some way to know when the last reference to a shared extent goes
away. D-8 forbids reference counts, so that answer has to be mark-sweep across every
generation, which is what Purah already does for extent groups and would now have to do at
1 MiB granularity instead of 4 MiB. Against that cost, the win on VM disks is identical OS
images, which clone-from-image already gets for free as a map copy sharing every extent
with its parent (D-19). Dedup would be buying back something never spent.
