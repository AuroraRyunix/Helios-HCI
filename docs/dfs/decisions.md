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

**D-23 — the extent ID map is built in three stages, and the order is the safety property.**
Helios has two levels where Nutanix has three: `dfs_block_map` points `(vdisk,
extent_index)` straight at an extent group, where their `medusa_vdiskblockmap` points at an
*extent* and `medusa_extentidmap` then points that extent at a group. The missing middle
level makes an extent an addressable thing several vdisks can reference by name. The
operator's procedure, including exactly what to observe between stages, is in
[extent_id_map.md](./extent_id_map.md); this entry is the reasoning.

The schema takes the two migration ids assigned for it, **`0020-dfs-extent-id-map`** and
**`0021-dfs-block-map-extent-id`**. An earlier version of this entry reserved `0013`, and
the task-tree and access-data migrations landed the same week and took 0011 through 0017;
the next version said "the next free id" and so named no number at all, which left the table
with nothing to be built against. A number held for a table that does not exist yet is a
collision waiting for the first migration that needs one, so ids are assigned when the work
is scheduled, and recorded here when they are.
The precedent this entry keeps citing, `dfs_egroup_replicas` -- a table that sat in the
schema with nothing writing to it -- has since been dropped (migration `0025`), so the
cost it describes is no longer a live example, only a recorded one.

```
dfs_extent_id_map                  -- 0020
  extent_id text PRIMARY KEY,      -- birth-derived; see the addendum on content-derived ids
  egroup_id text, egroup_offset int, length int,
  vdisk_hash bigint,               -- the identity the footer was stamped with (D-18)
  created_at_ms bigint

dfs_block_map ADD extent_id text   -- 0021, one bare ALTER (this ScyllaDB rejects IF NOT EXISTS)
```

**Null `extent_id` means the two-level path**, so every vdisk that exists today keeps
resolving reads exactly as it does now, byte for byte, indefinitely. A row fills `egroup_id`
or `extent_id`, never both. Neither migration rewrites a block-map row, and no later one may.
The table that `multi_disk.md` warned about -- an empty table with a suggestive shape, like
`dfs_egroup_replicas`, that every later design must first establish is not a source of truth
-- is avoided on purpose: a row of `dfs_extent_id_map` is reachable only through a block-map
row, and nothing but Purah's mark phase reads it without one.

**Why this could not be landed behind a flag.** Purah's mark phase reads `egroup_id` off the
block map and runs on a timer whether or not anyone opted in to anything. The first row that
names an extent instead of a group makes a sweep that only knows the old column see every
group behind it as unreferenced, and the two-scan grace delays that deletion by ten minutes
rather than preventing it. A flag guards the *read* path; the curator is not on it. So the
order is fixed, and it is the point of the entry:

1. **Stage 1 -- Purah marks through both levels. Built.** With no row naming an extent it is
   the scan it always was (the ledger says whether the column exists, so a cluster that has
   not migrated is never sent a column it lacks). With extent names it follows them, and it
   *fails closed*: an extent that cannot be followed aborts the sweep instead of being
   skipped, because skipping leaves the group behind it unmarked. It is proved by a
   randomized model -- any set of vdisks and extents, rows naming either level, orphan extent
   rows nobody references -- asserting the marked groups equal exactly what is reachable,
   in both directions. **This is the stage to roll out alone and let run through a full sweep
   cycle before anything else exists on the cluster.**
2. **Stage 2 -- the table, the column and the dormant read path. Built.** A vdisk resolves
   extent-named rows when it opens, but only when a returned row has no `egroup_id`, so every
   existing vdisk issues the one statement it always did. "Off by default per vdisk" is
   structural rather than a switch: the reader follows what the row says, a flag would have to
   agree with the rows, and a reader that disagreed with them returns the wrong vdisk's bytes.
   What stays off is the *writer*, and nothing in the tree writes an extent id; a test pins
   that.
3. **Stage 3 -- writing extent ids. Designed, not built.** The writers that must change, and
   the hazard each carries, are in [extent_id_map.md](./extent_id_map.md). They include the
   drain commit, which must null `extent_id` when it repoints a row; `derive_child`, which
   today refuses a row naming an extent, the safe direction; and a second Purah pass to
   reclaim extent rows orphaned by a vdisk delete. The per-vdisk opt-in for the writer needs a
   home on `dfs_vdisks`, which is a third migration id nobody has assigned.

What an operator must see between stages: before stage 2, no `purah: sweep failed` for two
sweep intervals plus the grace after every node's restart, and `referenced` unchanged;
before stage 3, an empty `dfs_extent_id_map`, no non-null `extent_id` anywhere, and a clean
sweep that has read the new column. The specifics are in the rollout document.

*A correction to this entry's original claim.* It said the middle level was what made
"extent-granular clone divergence" possible. It is not: a clone's rows are already a copy of
its parent's, one per 1 MiB extent index, and a write to either redirects and repoints only
that vdisk's own row, so clones already diverge extent by extent. What the middle level
buys is that **relocating an extent group costs one row, not a scan of every block map that
points at it** -- the thing tiering migration and compaction need and neither has yet --
and that an extent has a name, which is what dedup would need. Those are the reasons to build
it, and clone sharing is not one of them.

**Dedup is not being built, and the reason is not difficulty.** On top of the extent id map
it needs a content hash as the extent id, a by-hash index to find candidates, and -- this is
the part that decides it -- some way to know when the last reference to a shared extent goes
away. D-8 forbids reference counts, so that answer has to be mark-sweep across every
generation, which is what Purah already does for extent groups and would now have to do at
1 MiB granularity instead of 4 MiB. Against that cost, the win on VM disks is identical OS
images, which clone-from-image already gets for free as a map copy sharing every extent
with its parent (D-19). Dedup would be buying back something never spent.

**D-23 addendum -- dedup, revisited.** The paragraph above is the original position, and it
is kept as written because the addendum is only useful if it can be seen what it is
arguing with. The owner wants dedup. That is an input to this entry and not a measurement:
this addendum states what it costs and when it pays, and recommends, and the decision is the
owner's.

*The original argument, stated fairly.* Dedup earns its keep where the same bytes are written
many times independently. On VM disks the bulk of that is identical OS images, and a fleet
cloned from one template shares every extent of it as a map copy at no cost. So the
interesting question was never "does dedup save space" -- it does -- but "how much beyond
what clone-from-image already saves", and the original answer was "not enough to pay for the
machinery". That argument rested on a cost estimate, not a measurement, and the estimate
assumed the middle level did not exist.

*What has changed.* The middle level exists. An extent is now something a row can name, so
the first of dedup's three prerequisites is no longer missing, and Purah marks through it. Two
things the original did not count also cut the other way. Naming means that **with a
content-derived extent id the extent map is its own by-hash index**: finding a candidate is
`SELECT ... WHERE extent_id = <hash>`, so no separate index is needed. And relocating a shared
extent is one row, which makes compaction -- needed anyway -- affordable. What has *not*
changed is that a shared extent still has no reference count and no way to know its last
reference has gone except a full mark.

*What it would cost.*

- **Write amplification and metadata load.** Every extent a drain commits has to be hashed
  with a cryptographic hash (CRC32C is detection and nothing else, D-9) and looked up before
  it is written. `metadata.md` section 5 puts a loaded cluster at roughly 2 MiB/s of drain per
  vdisk across 1,000 vdisks: about **2,000 extents per second**, so ~2,000 point reads on a
  hash-partitioned table that cannot be batched with the vdisk's own partition, plus an insert
  for each miss. Racing drains writing identical content make "insert if absent" a
  lightweight transaction, and the cluster is budgeted for ~60 LWTs per second today. That is
  a thirty-fold increase in Paxos load, which is the figure to argue with. The alternative
  is to accept duplicates when two drains race, making the index a hint, which costs a little
  space and no correctness. None of it is on the guest's acknowledgement path (journal first),
  but it lengthens every drain, which lengthens the journal's high-water pressure.
- **Memory and index size.** At about 64 bytes per unique 1 MiB extent (32-byte hash, location,
  overhead) the index is **64 MiB per TiB of unique data**, or 64 GiB per PiB. In capacity terms
  that is 0.006% of the data and is not the cost. It matters as Scylla memory (bloom filters and
  key cache for a table with one row per unique extent) and, if held in Sidon instead, as a
  per-node resident set that must be rebuilt after a restart.
- **Garbage collection, which is where it actually hurts.** Liveness moves to the extent but
  deletion stays at the 4 MiB group, and a group holds up to four extents. A group is
  reclaimable only when *every* extent in it is dead, and under sharing the dead ones sit beside
  live ones from other vdisks. Space comes back only by compaction -- rewrite the live extents
  into new groups, repoint one extent row, let the old group die -- and compaction does not
  exist. Without it the headline dedup ratio overstates what is returned to the pool.
  Second, dedup creates a hazard the guards in `purah.rs` do not cover: **resurrection**. A
  drain that dedups against extent E re-references a group G that a sweep may already have
  observed unreferenced once. G is old and not held, so neither the young guard nor the held
  guard protects it, and a block row committed between the second scan's read and the delete
  loses live data. Closing it needs a lease on lookup (the lookup refreshes the extent row and
  the sweep honours it for the grace period), which is a new write on the path that was meant to
  save one. Third, the mark phase grows from "one row per extent index across all vdisks" to
  that plus "one row per unique extent".
- **Smaller costs that are real.** The footer stamps the writer's vdisk hash and the extent
  index (D-18); a shared extent is read by other vdisks at other indexes, so the identity check
  would have to key to the extent, not the reader. Per-container compression means two
  containers cannot share bytes stored differently. A shared extent lives on one group's
  replica set, so a clone on another node reads it remotely. Cross-tenant dedup is a timing
  side channel (a write that completes faster reveals the content exists), which matters if
  containers ever belong to different parties.
- **Granularity.** Extents are 1 MiB. Sub-extent duplicates -- the same 4 KiB filesystem
  blocks at different alignments inside different extents -- are invisible to it, so on data
  that is not a bit-identical image the hit rate will be well below what 4-16 KiB dedup
  advertises. That is reasoning from the geometry, not a measured rate.

*When it pays.* Not on what clone-from-image already shares. It pays on **identical bytes
written after the clone**: a thousand VMs cloned from one template each applying the same
patch set produce the same new extents a thousand times, and clone sharing cannot see that
because it ends at the moment of divergence. It also pays on full copies that were never
clones (the same installer run per VM, restored backups of similar machines). It does not pay
on databases, encrypted guests or compressed media, whose extents are unique or look random.
The index is nowhere near big enough to matter; what decides it is whether the LWT load, the
resurrection guard and a compaction pass that does not yet exist are worth the percentage the
cluster would save beyond what it already saves.

*Recommendation.* Do not build inline dedup on the drain. Build, in this order and each only
if the previous one earned it: (1) **a read-only estimator** -- a Purah pass in the style of
the heat ranking (D-22), measuring before moving -- that hashes sealed extents and reports how
many would be shared, split into "already shared by clone" and "would be new". It writes
nothing and needs none of stages 2 or 3 to exist. It turns the owner's wish into a number on
the owner's own data. (2) **Compaction**, which stage 3 and tiering need regardless. (3) Only
then, if (1) shows a worthwhile figure, **a background, post-process pass** rather than an
inline one: Purah finds duplicate sealed extents, creates or reuses an extent row, repoints the
referencing block rows, and the redundant copy dies by the ordinary sweep. That removes the LWT
load and the drain-path latency, and it keeps the resurrection window inside a pass that
already owns the grace logic. As a working threshold, a result under roughly 10-15% saved beyond
clone sharing does not justify a new failure class; that number is judgement, and the
estimator exists so that it can be replaced with a measurement.

*Built (D-32).* Steps (1) and (2) of that order exist: `valcli storage.dedup.estimate` and
`valcli storage.compact`, both operator-invoked. Step (3) is still not built, and the estimator is
what decides whether it should be.

**D-24 — erasure coding is not built, and on three nodes it should not be.** The question
that decides it is what a 2+1 stripe actually buys on the cluster Helios runs, so the
arithmetic comes first and the recommendation follows from it.

*What it buys.* A replica pair stores 2x. A 2+1 stripe -- two data groups and one parity
group, each on a different node -- stores 1.5x and survives the same single failure. On the
cold fraction `f` of the data, raw capacity per logical byte goes from 2 to `2 - 0.5f`, a
saving of `f/4` of the cluster, and never more than 25% even if everything were cold. At
`f` = 0.5 that is 12.5% of raw capacity. On three nodes 2+1 is the *only* stripe there is:
every shard needs its own node, so `k+m` cannot exceed 3, and `1+2` is just three-way
replication.

*What it costs, line by line.*

- **Write path: nothing.** The guest's write still lands in the replicated journal and is
  acknowledged there; encoding is a background job over groups that are already sealed.
  This is the one place erasure coding is cheap.
- **The encode itself.** Reading two 4 MiB groups (one local at best), computing parity,
  writing and verifying a third group elsewhere, recording the stripe, then dropping one
  replica of each data group: 12 MiB of I/O to turn 16 MiB of replicas into 12 MiB of stripe.
  A one-off 4 MiB saving per 8 MiB of data, paid in background I/O.
- **Degraded read.** A replica pair reads the surviving copy: one read, one hop. A stripe
  with a member lost must read the same byte range from *both* surviving shards, check both
  footers and combine them: two reads on two other nodes, latency the slower of the two.
  Healthy reads are not worse in bytes, but a stripe keeps one copy of each group instead of
  two, so on three nodes about two cold reads in three become network reads where an RF=2
  pair would have served them locally.
- **Rebuild, and the part that decides it.** Healing one lost shard reads two shards and
  writes one: 12 MiB of traffic per 4 MiB healed against 8 MiB for re-replicating a copy.
  Worse, **a stripe on three nodes has nowhere to heal to.** The shards must sit on three
  distinct nodes and after a node loss only two exist, so the stripe stays at zero
  redundancy until the node returns. An RF=2 pair loses a node and re-replicates onto the
  third -- about three seconds for the journal, measured on the test cluster -- and is back
  to full redundancy with two nodes left. That is the reason Nutanix's smallest EC-X stripe
  needs four nodes: the stripe is one node narrower than the cluster so that a rebuild has a
  home. On three nodes erasure coding does not merely trade capacity for read cost; it
  trades *self-healing* for capacity, on data the cluster claims `ftt=1` for.
- **Delete.** Today reclaiming a group is local: mark it dead, remove the file, no other
  node's agreement needed (Purah's `reclaim`). With a stripe it is not, because a parity
  group protects its other members and a dead member's bytes are still an input to
  reconstructing the live one. Deleting group G1 while G2 relies on `G1 xor P` silently
  removes G2's redundancy. A stripe with a dead member therefore has to be *undone* first --
  the live member re-replicated, the parity dropped -- as a coordinated three-node
  operation, or left in place until every member is dead. Either is new state, a new
  failure mode for the sweep, and a new Ganon scenario.
- **The immutability assumption holds, and that is not the problem.** Parity over sealed
  groups is computed once and stays valid, because sealed means immutable (D-4); mark-sweep
  never has to touch a group's bytes. What does not survive is the other assumption in D-8
  and I-7, that each group's fate is independent of every other group's. A stripe couples
  three groups' lifetimes, and the two-scan grace rule would have to be applied to the
  stripe rather than to the group.
- **Invariants.** I-6 says every live group has exactly RF verified replicas on distinct
  failure domains. That would need restating for stripe members, and scrub, which compares
  a group against its own seal hash, would need a parity check beside it.
- **D-22's boundary.** Access data may decide where a copy goes, never whether it exists
  (D-22). Encoding deletes a replica because a group *looks cold*, which is a decision about
  whether a copy exists, taken on an approximate statistic that a lost flush understates.
  A mistake would cost a degraded read rather than data, but the line was drawn on purpose
  and the choice would be either to amend D-22 explicitly or to select on something exact,
  such as the age since seal with no recorded access across two flush windows.

*Recommendation: do not build.* A 12 to 25 percent saving on the cold fraction does not pay
for the loss of self-healing at `ftt=1`, for a delete that is no longer local, and for a
codec, a stripe table and a Ganon matrix that would be the largest new surface in Sidon
since the journal. Compression is already built (migration `0008`) and takes bytes off the
same cold groups without any of that cost. No code was written for this decision, and none
should be: a codec with nothing wired to it is the same trap as the `dfs_egroup_replicas`
table, a thing of suggestive shape that every later design has to establish is not a source
of truth.

*What would change the answer.* Node count first. At four nodes 2+1 has a rebuild target
and the objection above disappears, leaving a saving of at most 25% on cold data. At five or
more nodes the case becomes real: 3+1 stores 1.33x (a third less than a replica pair, on the
cold fraction), and 2+2 gives `ftt=2` at 2x where replication needs 3x. A threshold stated as
a proposal for the owner to move: **at least five nodes, and a cold sealed fraction above
about half of used capacity, on a cluster where a saving of 15% of raw capacity is worth
more than the operating cost** -- that is 2+1 at 60% cold, or 3+1 and 2+2 at 45%. The cold
fraction is measurable now with `valcli storage.heat`, which is the first thing to do when
the node count changes. The prerequisites, in order, are the tiering migration job (copy,
repoint, delete, conditional on the epoch -- most of the machinery is shared), the D-22
amendment above, and a stripe-aware sweep designed *before* the codec. When it is built it
follows the pattern D-23 set: a stripe table that takes the next free migration id at that
time, a per-container opt-in, and no cluster-wide switch.

**D-25 — `vhost-user-blk` is designed and not built, and the next step is a measurement,
not code.** [vhost_user_blk.md](./vhost_user_blk.md) holds the reasoning. D-3 named it the
v2 performance path and `sidon.md` calls it deliberately last; this is the first time the
claim was checked against the code. Three findings, in order of consequence:

1. **Sidon serves one request at a time per connection**, so a guest's queue depth is
   flattened to 1 before any transport is involved, and the libvirt XML's `queues='N'
   iothread='1'` feeds N queues into one serial loop and one `Mutex<Vdisk>`. A faster
   transport does not change that. Concurrent requests do, and concurrent requests are the
   journal-ordering change the "deliberately last" warning is about, needed over NBD
   exactly as much as over vhost-user. *(Since built, over NBD: up to 32 requests in flight
   per connection and group commit of their syncs, [group_commit.md](./group_commit.md). On
   the test cluster 4 KiB writes at queue depth 16 went from 157 to about 2,275 IOPS with the
   queue-depth-1 figure unchanged -- so the "measure NBD first" step now has a fair baseline
   to measure against.)*
2. **The transport is a small part of the request.** An estimated ten microseconds of socket
   work sits beside a journal `fdatasync` and a sequential round trip to each replica on a
   write, and beside a 1 MiB read and checksum to serve 4 KiB on a read.
3. **Whether the EL10 `qemu-kvm` ships the device is unverified.** Upstream 10.1 has it,
   and D-3's "qemu 10.1 supports it" meant that. Red Hat's RHEL 10 documentation says it
   does not support a user-space vHost interface, which is a statement about support rather
   than about the build, and the check is three read-only commands that were not run
   because work on the cluster was limited to building under `/tmp`.

Recommendation: **do not build until a benchmark shows NBD is what limits a guest.** The
benchmark, its stages and its kill criteria are in the document; stage 1 compares the two
transports with no Sidon at all and stage 3 is the concurrency work that is the actual
prerequisite. Alternatives it beat: building it now because the design is clear (it
reorders the one thing the invariants rest on, to speed a path that is not the slow one),
and ublk (D-3's other alternative, a kernel dependency NBD avoids, and no better placed on
the concurrency question).

**D-26 — a disk is identified by what is written on it, and a move leaves the old copy to
the sweep.** Two choices built `multi_disk.md`'s placement and tiering, each with a rejected
alternative that looked simpler.

*Identity.* The claim script names each disk's directory after the kernel device it saw
(`disks/sdc`), kernel names follow probe order, and on one node the disk in the `sdc` role is
`/dev/sdb`. Keying anything on that name keys it on a guess. **Rejected: the directory name**
(the status quo); **the filesystem UUID from `blkid`**, which needs a mount-to-device-to-
`/dev/disk/by-uuid` mapping sidon has no other use for and which does not exist on a dev box;
**a column in Hydra**, which makes a node-local fact a cluster write for the reason option 3
in `multi_disk.md` already gave. **Taken:** a `disk.uid` file at the top of each filesystem,
written once. The name stays as a label.

*Deletion.* **Rejected: delete the source as the last step of the move**, which is what
"copy, verify, switch, delete" reads as. Every attached vdisk has its own `EgroupStore` with
its own index, so the switch is not visible to them, and a reader that resolved the old path
before the switch would meet `ENOENT` after the delete. **Taken:** the move never deletes; the
sweep removes the surplus copy after seeing it on two passes with the grace between (the
mark-sweep's own rule) and after proving it byte-identical to the one kept and that at least
two copies exist. A crash then needs no recovery at any point: there is one valid copy, or
two, never none.

*No map repoint.* `multi_disk.md` described the move as including "the map repoint conditional
on the epoch". The block map names a group, not a disk, and a group keeps its identity when it
changes disk, so nothing in Hydra changes: no migration, no epoch. The only thing repointed is
the node's own index. This is a correction to that sentence, not a new design.

*Opt-in.* The pass plans unless told to apply and nothing runs it on a timer, for D-22's
reason, which the ranking existing does not discharge: nobody has yet watched what it proposes
on mixed media. Heat chooses where a copy sits and never whether one exists, so nothing in the
pass deletes a group, shortens a replica set or touches the metadata that says one exists.

**D-27 — sidon mounts its own disks, by filesystem UUID, as siblings; nothing sidon owns is in
`/etc/fstab`; and no path is used whose disk is not provably there.** Two outages in one evening.
The toolkit wrote each extra disk to fstab without `nofail`, so a late disk failed
`local-fs.target` and two nodes came up with no network and no SSH. `nofail` fixed the boot and
made the next failure silent: the parent volume did not mount, nothing complained, and sidon wrote
extent groups to the root filesystem at the same path. Then, recovering, the parent was mounted
over a child that was *nested inside it*, which hid the child: it was still listed as mounted, the
path no longer reached it, and sixteen extent groups were unreachable. A real Nutanix CVM does
none of that: nothing storage-related is in its fstab, every parent is a plain directory, the disk
mounts are siblings, and they are named by serial. Mounting is the storage layer's job, and the
storage layer is the thing that knows what a missing disk means.

*Where the knowledge of "which devices are mine" lives.* `/etc/hci/sidon-disks`, one
`<filesystem-uuid> <journal|extent>` per line, written by the claim step and staged by the rollout,
read by sidon and by Mimir. It is configuration, so it is in `/etc/hci`, and sidon never rewrites
its own list of what it expects: a daemon that did could forget a disk that went missing.
**Rejected: fstab** (the status quo and both outages). **Discovery by scanning block devices for a
`disk.uid`**, which has to mount a disk to read the sentinel, and would adopt any foreign or cloned
disk that carried one. **A Hydra row**, for the reason option 1 in `multi_disk.md` gave: a
node-local fact made a cluster write, and a node must know its disks before Hydra is reachable.
**The serial**, which Nutanix uses: virtual disks on the lab nodes report none, and the filesystem
UUID exists before anything is mounted and changes on a reformat, which is a different disk and so
the right behaviour. This does not reopen D-26, which declined the filesystem UUID as a disk's
*identity*: identity is still `disk.uid`, and the UUID is only the address used to find and prove
the mount.

*Who mounts.* Sidon, at startup, and `sidon mounts apply`. **Rejected: a unit `ExecStartPre`**, since
the rollout does not write an existing node's unit file, so a step that lived there would never
reach a node that already exists while a step in the binary arrives with the binary. **Generated
`.mount` units**, which are fstab again with a different syntax and the same ability to fail a
dependency. **udev or automount**, which race and hide absence.

*The invariant.* A disk is present only if the directory is a mount point (its device differs from
its parent's), is the device carrying the manifest's UUID, and holds the `disk.uid` sentinel. All
by `stat`. **Rejected: `findmnt` and `mountpoint(1)`**, both of which still report a child covered
by a later mount of its parent as mounted (confirmed on a node with loop devices), and **the
sentinel alone**, which a file left on the root filesystem during an unmounted period satisfies.
It is checked at startup, at each journal attach, and at each extent-group create and move, not
once, because a disk can leave while sidon runs.

*Layout, and what each part does when its disk is absent.* `/var/lib/hci/sidon` is a plain
directory. `disks/<uuid>` are siblings. `nbd/` stays at `/var/lib/hci/sidon/nbd` on the root
filesystem because libvirt domain XML names those sockets; it holds no data and is recreated at
attach. The **journal volume** is the existing thin LV and holds `journal/`, `replica/`,
`replica-egroups/` and its own extent groups, which means an existing node moves no data. With it
absent sidon **refuses to start** (`Restart=always` retries, and each retry attempts the mount, so
a late disk recovers by itself): without the journal it cannot say what it has acknowledged, and
the alternative is journalling onto the root filesystem. **Rejected: the journal on the root
filesystem**, which is the silent hazard and a small filesystem a full store must not be able to
wedge. **A journal on every disk**, which is the "journal on the fastest disk" design that is
still unbuilt and would be a manifest role change plus a drain. An **extent disk** absent costs
that disk only: sidon starts, serves the others, places nothing on it, reports it in `capacity`
as `absent_disks`, and reads of its groups fail as referenced-but-absent.

*Existing nodes.* The rollout only stages the manifest. It mounts, unmounts and edits nothing,
because moving a mount under a running sidon is the thing this exists to prevent. The move happens
when sidon next starts, before anything is opened, or by `sidon mounts apply`, which refuses while
the control socket answers. It unmounts every mount under the root deepest first and never lazily,
in rounds so a covered child is recovered, then removes the sidon lines from fstab; it refuses on
EBUSY (nothing lost, resumes at the next start) and, before unmounting anything, on a mount the
manifest does not name. No data is copied. **Rejected: moving mounts during the rollout** (the
invariant above), **copying data to new volumes** (all risk, no benefit), and **rebuilding nodes**.
Until a node moves, a transitional repair keeps its old fstab lines from being able to fail the
boot.

*What it costs.* A disk attached after sidon starts is not used until sidon restarts. A missing
journal volume produces a log line every few seconds until it appears. And the first start after
the rollout is the one that moves the layout, so it belongs in the maintenance window the rolling
upgrade already opens.

**D-28 — two sites trust each other by a pinned site certificate on an import-only listener,
never by cross-signing.** The cluster CA is per cluster and its rule is "signed by our CA":
`client.crt` is identical on every node and `spark-daemon` on 9099 is a root-equivalent API.
Cross-signing, or putting a foreign CA in those trust stores, would make every node of the other
cluster a full member of this one. Alternatives: **cross-signing / merged trust bundles**
(rejected for that reason), **a separate listener that trusts the remote CA** (still trusts
every certificate that CA ever issues, including the identical `client.crt`), **a shared bearer
token** (replayable, a secret on the wire, needs its own rotation), **SSH or rsync** (the
fleet's SSH mesh is root on every node), **trusting the network** (a VPN is welcome and is not
an identity). Taken: a per-site key and certificate, authenticated by its SPKI SHA-256 pinned on
the other side by a human ceremony, mutual TLS on a port that serves only the replication
operations, with authorization (may push, a byte quota) held apart from authentication and the
target choosing local vdisk names itself. Pinning the key rather than the certificate lets a
renewal pass without re-pairing. Push only: a disaster-recovery site often cannot be dialled.
[replication.md](./replication.md) section 2. Not built.

**D-29 — replication ships the groups the target lacks and a verified map, not a log and not a
delta against a base snapshot.** Groups are immutable and snapshots share them, so what a target
is missing is exactly what was written since it last received anything, and asking the target
what it holds is correct whether or not an earlier snapshot still exists on either side.
Alternatives: **a change-tracking log** (a second source of truth about what changed, which every
crash must keep in step), **a delta against the last delivered snapshot** (breaks the moment
either side deletes it, and the target's own retention may have), **live extents only** (changes
offsets, so the remote file is no longer byte-identical and the footers' identity stamps stop
meaning anything). Taken: whole groups, with the waste of dead extents accepted and named; an
open group shipped as a prefix named `<id>~<length>`; verification at three levels the target
does *independently* of the sender (each referenced extent's footer, the seal hash, a SHA-256 per
group) plus a SHA-256 of the canonical map recomputed from the rows actually written.

**D-30 — a replicated snapshot becomes visible only by one compare-and-swap after everything
else is true, and a transfer resumes by verification rather than by trust.** The snapshot's row
is written `forming`, which nothing attaches or lists as a replica; the map follows, is read back
and checksummed, and a single `forming` to `immutable` flip is the whole of "visible", the same
shape a local snapshot already uses. Staged bytes resume on chunk boundaries but the end-of-group
digest is what is believed. Same-snapshot-twice is decided by the digest: equal is a no-op,
different is a hard refusal, because a source snapshot's name is immutable and the target never
overwrites. Replication never frees target space to fit itself. Open question to settle before
the transport: installed-but-unpublished groups are orphans to Purah and a transfer longer than
its grace can lose one; registering a job's groups as roots is a change in `purah.rs`.

**D-31 — bandwidth is limited at the sender, replication state has no tables until a second
site exists, and failover and failback are a design and not code.** A token bucket per site at
the sender, one transfer per node at a time; the target slows a sender by reading slowly.
`dfs_remote_sites` and `dfs_replication_sets` are specified and **not migrated** (ids `0033`,
`0034` reserved): two empty tables on every cluster is what `0025` removed. Failover is a clone
of the replicated members plus a VM definition that is not yet replicated, and failback is the
same replication reversed; neither can be exercised without a second site, and code written
against fakes would be fiction. [replication.md](./replication.md) sections 5, 7 and 8.

**D-32 — compaction rewrites live extents into a new group and repoints rows while the owner's
drains are held; the dedup estimator only reads; neither runs unattended.** D-23's addendum
asked for two things in order, a read-only estimator and compaction, and said the second is
needed "anyway". Both are built; the mechanism is [compaction.md](./compaction.md). What was
decided, with the alternative each choice beat:

*What a batch is.* Copy, verify, publish, then repoint -- `storage.move`'s discipline (D-26),
generalised from "copy a file" to "build a file from extents", and **never delete**: the old
group is left for the sweep's two-scan grace. That one rule is why a stop at any step needs no
recovery: once rows start moving, each names a location holding exactly its bytes, so every
prefix of the swaps is a valid map. **Rejected: a journal of the batch** (a second source of
truth about which rows moved, which every crash must keep in step; the map already says). One
ordering differs from a move on purpose: the new group is **registered in Hydra, already sealed,
before the rename**, so a crash between leaves a row the sweep reclaims and not a file nothing
names.

*The second writer.* [metadata.md](./metadata.md) section 3 says block-map rows are plain
writes because a partition has one writer, and prices a second at Paxos per row. Compaction is
the second writer, and a compare-and-swap does not remove the hazard: it protects against a
drain that has written, not one that has read the row and is about to. **Rejected: swaps with
no exclusion** (loses a guest write, or reverts one, in a window of milliseconds that no test
without a timing hook can find); **making compaction a drain run by the owner** (serialises
for free but handles neither the snapshots and images that share the extents nor any group whose
vdisks are not all one owner's); **claiming a detached vdisk before rewriting it** (a claim does
not stop another node attaching during the rewrite); **Paxos per row for the drain** (the
thirty-fold load D-23's addendum already costed). **Taken:** a row of a writable vdisk is
rewritten only while this node owns and has attached the vdisk and holds its **drain gate** (the
flag every drain sets, so no drain runs or starts; guest writes are not held); rows of an
immutable vdisk are rewritten freely; a group any other writable vdisk points into is skipped
whole. That makes the pass narrower than it could be -- a clone running on another node pins
its parent's groups -- and the narrowness is the price of not adding a writer the design cannot
exclude. The swap is conditional on group, offset and length anyway, so a drain that committed
first wins.

*All or nothing per group, and no partial credit.* A source group is moved whole or not at all:
the sweep frees a group only when all of it is dead, so moving some extents of a group frees
nothing and costs a copy. Sources are packed together only if they share a container and a
replica set. A shared extent (clone, snapshot) is copied once and every referrer's row is
swapped; an extent named through the middle level moves with **one** extent-map row, which is
the "relocation costs one row" D-23 promised. Missing a referrer is safe -- the pass never
frees, the mark phase still decides what is referenced -- so the live set may be incomplete and
the cost is space.

*No state.* No table, no migration, no id: the plan is recomputed from the map every run, and
convergence is structural (a new group is all live; a half-moved group is a smaller candidate).
Ids `0018`/`0019`/`0024`/`0026`-`0029`/`0033`/`0034` are untouched and `0035` is not taken.

*The cost it did not remove, and D-33 did.* A replica's copy of a group sat in its replica store
and nothing in Sidon removed one, whether the group was swept, compacted or deleted. Compaction
adds the live extents to each replica, so at ftt>=1 it moved space from the creator to its
replicas. **Resolved by D-33:** the sweep now asks every peer to drop its copy of a group it
reclaims, each replica re-checking Hydra and the map itself, and each node scans for orphaned
replica copies besides. Compaction's old groups are swept like any other, so at ftt>=1 the replicas
get the old group's space back after the sweep's two scans, by the amount the plan prints
(`freed_on_replicas_after_sweep`); a Rust test runs compact, sweep, sweep and lists the replica's
directory. It holds for replicas that run the D-33 build: an older replica refuses the request
and keeps its copy (safe, and reported), and a replica that was down is cleaned by its own scan.
The plan still prints the growth beside the saving, because until the sweep has run twice the
replicas hold both groups. This no longer argues against `--apply` at ftt>=1; it stays opt-in for
D-22's reason.

*Opt-in.* `storage.compact` plans unless `--apply`; a pass is bounded by groups, bytes, rate and
wall clock and says which bound it hit; nothing runs it on a timer, D-22's reason unchanged.

*The estimator.* Read-only by type (it is handed a reader and a store it only reads) and by
test (the production code contains no write). It hashes SHA-256 of each stored extent *as the
guest wrote it*, so a compressed and a plain copy match, and reports per container the bytes
already shared by clone (rows minus distinct stored extents), the bytes dedup would add (stored
extents whose content another holds), and the latter without all-zero extents, which a sparse
map handles without a hash index. **Rejected: a content-derived extent id or a by-hash table**
(that is dedup, which the addendum says to earn); **a setting** (none exists to toggle).
Sampling orders extents by a hash of their location and takes a prefix, so a pass cut short by
its time budget is still a uniform sample and says what it covered; a sample undercounts content
that exists twice and is fair for content that exists many times, which is the content that could
clear D-23's 10-15% bar. Duplicates that straddle two nodes are found by merging short digests
across nodes in `valcli`; one node alone cannot see them.

**D-33 — a replica drops a copy only when Hydra says the group is dead; the sweep asks, and a
scan finds what nobody asked about.** The sweep (I-7) freed a group on the node that created it
and nowhere else. Every other copy sits in `replica-egroups/` on the nodes that replicate the
group, nothing told them the group was gone, and nothing on them looked: on the test cluster, one
node still held 12.7 GiB of replica copies for groups the owner had long since swept. The cost was
real (half the raw space at RF 2 never came back) and it is why D-32 could not recommend
compaction at ftt>=1.

*Taken: two mechanisms, one rule.* (1) **The request.** When a sweep reclaims groups it asks
every peer, once per pass and with every group in one frame, to drop its copy (`OP_EGROUP_DROP`,
opcode 11). It asks *after* the row says `dead` and the owner's own copy is gone, and *before* the
row is deleted, because that row is what the replica checks; a stop after any step is finished by
the next sweep (`purah/reclaim.rs` header). (2) **The scan.** Each node's sweep also scans its own
`replica-egroups/` for copies whose group Hydra has no row for, or a `dead` row, and drops them
under the sweep's own rule: unreferenced, last written longer ago than the grace, and seen so on
two scans a grace apart. The scan is the backstop for everything the request cannot do: a replica
that was down when the owner swept, an owner that crashed between marking a group dead and asking,
an owner that predates the opcode, and the orphans already on disk before any of this existed.
Neither mechanism trusts the other and neither depends on it.

*What a replica checks, and why it is not an epoch.* A replica never takes the sender's word. For
each group it requires, from Hydra, that the row exists, says `dead`, and **names the sender as the
node that created it**; and then it reads the block map and the extent id map itself and refuses
any group something still points into (`referenced`, which the sender treats as an alarm: it keeps
the row `dead` as evidence and reports an anomaly, because a replica that kept the last copy of a
referenced group has saved it). A missing row, an `open` or `sealed` row, a different sender, a
name that is not a group id, and any failed read of Hydra are all refusals or errors that drop
nothing: silence is not permission. The request asked for an epoch fence; there is no epoch to
use. A group is not owned by a vdisk (clones and snapshots share one), so the epoch of whichever
vdisk wrote it would fence the wrong object. What a deposed or stale sender cannot do is make
Hydra say `dead`: that is a lightweight transaction conditional on the state it leaves, which a
node that cannot reach a quorum cannot perform, and it cannot make the block map stop pointing at
a group. The authority is therefore Hydra's row, re-read by the replica, plus the replica's own
look at the references -- two independent reads instead of one sender's claim.

*Rejected.* **The replica trusts the request** (a sender that marked a group dead wrongly -- a
drain committed a reference after its scans, the very race I-7 exists for -- would take the last
copy with it, because the owner has already deleted its own). **The owner asks only the replicas
of the vdisk that wrote the group** (the vdisk may be deleted, its replica set changes over time,
and a group outlives both; a peer that holds nothing answers in one directory lookup, so the owner
asks everyone). **The owner keeps the row until every replica has acknowledged** (it couples a
row's life to every peer being up, and an older replica never acknowledges, so it would leak rows
for ever; the scan makes the acknowledgement unnecessary). **A tombstone table** (a second table to
keep in step with the first, a migration, and nothing the `dead` row does not already say).
**Dropping on the replica's own scan alone** (correct, and two grace periods slower; kept as the
backstop, not the mechanism).

*Rolling upgrade.* The opcode is new, so an older replica answers "unknown opcode" with an empty
body and keeps its copy: space is not freed on it until it is upgraded, and nothing is lost. The
sender tells that from an answer (a new replica always answers `ST_OK` with a verdict per group,
even to refuse) and reports the peer as `unsupported` in `valcli storage.sweep`. An older *owner*
never asks; a new replica's own scan finds the copy after two scans past the grace. So any mix of
versions is safe, and a cluster that is fully upgraded drops its existing orphans on the first two
sweeps past the grace without anyone running anything.

*What it does not do.* A copy of a **live** group that its replica set no longer includes (a heal
replaced this node and the old copy was never removed) is left alone: Hydra says the group is live,
and whether this node should hold it is a question about the vdisk's replica set that this pass
does not ask (I-6 territory). The operator-visible number is `live` in the scan report.

*Open groups nothing holds (D-33, second part).* `open` means one thing: the append target of the
vdisk instance that created the group, in memory, on the node that created it (`Vdisk::open_eg`, or
the drain's own copy while a drain runs). Nothing adopts an open group -- a restarted daemon, a
detached vdisk or a failover starts a new group -- so a row stays `open` with nobody writing it
exactly when its writer is gone, and the sweep, which skipped every open group, never reclaimed any:
18 of them on one lab node after all its vdisks were deleted. The skip protected nothing the `held`
check did not already protect (a vdisk's `held_egroups` names its open group and every group a
running drain made, and the sweep checks `held` for every group whatever its state), so it is
removed. **Taken:** an open group is judged like any other -- unreferenced through both map levels,
not held, not young, seen so on two scans a grace apart, and the compare-and-swap to `dead` is
conditional on `open`, so a drain that seals it mid-pass wins -- and in addition it must be older
than `SIDON_PURAH_OPEN_ABANDON` (an hour; never less than twice the grace). The age is the third
guard and not the first: it covers the one window `held` cannot (a group created between the moment
the attached set was read and the moment the map was), and the attached set is now read under the
curator's lock, immediately before the scan, instead of before queueing for it. An unknown age
(`created_at_ms` unset) is never abandoned. **Rejected:** *asking each node whether a drain is
running for the vdisk* (an open group is only ever written by the node that created it, so only that
node can be asked, and that node is the sweep); *sealing abandoned groups instead of deleting them*
(an unreferenced group has no reason to exist, and sealing needs a hash of bytes nobody has
verified); *shortening or removing the age bound* (a free guard, and the only one that does not
depend on the attached set being read at the right moment). **Proof that a running drain's group is
safe:** `purah/reclaim/tests.rs` runs random histories of held sets, references and ages and checks
that nothing held or referenced is ever reclaimed and nothing is reclaimed on first sight, and
`vdisk.rs` runs a real vdisk with a drain parked at its commit and sweeps against its live
`held_egroups` past the bound and the grace, many times, before showing the same group go once the
vdisk is gone. **Not addressed:** an open group that rows *do* reference (its vdisk detached after
committing extents into it) stays open for ever and so is never scrubbed or compacted, which both
require a sealed group; sealing such a group needs the vdisk's drain excluded and is its own change.
