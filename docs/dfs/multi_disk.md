# Multiple disks per node

Every node in the test cluster has two empty 300 GB disks and uses one. `sdc` sits idle on
all three. This is the design for using them, and the reasoning for why the obvious answer
is the wrong one.

## The obvious answer is wrong

`vgextend vg_aether /dev/sdc` is one command and it would double the extent store tomorrow.
It is also the one option that must not be taken.

`vg_aether` holds a **thin pool**, and a thin pool spans its physical volumes. Adding a
second PV means an extent group can have its blocks on either disk, or both. Losing one
disk then takes the whole volume group with it — not half the extents, all of them,
because the pool metadata and the thin volume no longer have a complete backing store.

So pooling turns *"one disk died"* into *"this node's entire extent store died"*, and it
does so while making the failure **twice as likely**, because there are now two disks that
can kill it. That is a strictly worse position than using one disk and leaving the other
cold.

This is why Nutanix does not pool either. Each disk is a separate filesystem with its own
mount point and its own identity in the configuration, and Stargate places extent groups
across them in software. The failure domain is one disk, and losing one costs exactly the
extent groups that lived on it.

## What Helios has today

```
/dev/sdb ──▶ vg_aether ──▶ thin_pool_aether ──▶ sidon (150 GiB, XFS)
                                                  └─▶ /var/lib/hci/sidon
                                                        ├── egroups/   sealed extent groups
                                                        ├── journal/   per-vdisk write-ahead logs
                                                        └── nbd/       per-vdisk unix sockets
```

* `SIDON_ROOT` is a single path (`/var/lib/hci/sidon`), read once into `cfg.root`.
* `EgroupStore` is one directory; `path_for(id)` is `dir/{id}.eg`.
* `Purah::new(daruk, store, node, grace)` takes exactly **one** store.
* `op_capacity` runs `statfs` on that one filesystem.

Redundancy today is **per vdisk, across nodes**: `dfs_vdisks.replicas` names the nodes an
append must reach, and write-all means an append that misses one is not acknowledged.

`hydra.dfs_egroup_replicas` existed in the schema, with an `egroup_id`, `node`, `path` and
`state` — and **nothing wrote to it**. It was a table designed for a per-egroup placement
model that was never built, and every design that touched this layer had to begin by
establishing that it was not a source of truth. **It has been dropped (migration
`0025-drop-dfs-egroup-replicas`).**

It was removed rather than made true. Making it true means a metadata write per replica
per extent group, kept in step with the files on every node through every crash between
"file written" and "row written", and checked by a scrub against what is on disk. A table
that can lie is exactly what I-3 and I-7 keep out of Purah's mark phase, so the best it
could ever be is advice. The placement the system uses is per vdisk
(`dfs_vdisks.replicas`), and which node created a group, and in what state, is
`dfs_egroups`. Nothing read the table, so the mark phase — which reads `dfs_block_map` and
nothing else, and is tested to — is unaffected. If per-group placement is ever needed it
should arrive with the thing that writes it, in the same change.

## The design

### One disk, one store

Each disk gets its own filesystem and its own directory under the sidon root:

```
/var/lib/hci/sidon/
├── disks/
│   ├── d0/          ← /dev/sdb, own XFS
│   │   └── egroups/
│   └── d1/          ← /dev/sdc, own XFS
│       └── egroups/
├── journal/         ← on the fastest disk (see tiering)
└── nbd/             ← sockets, not data
```

`EgroupStore` becomes a set of stores. A disk that fails to mount is *absent*, not fatal:
the node keeps serving from the disks it has.

The directory names above are illustrative, and the real ones are worse than that. The claim
script names a disk's directory after the kernel device it saw (`disks/sdc`), kernel names
are assigned in probe order, and on one node the disk filling the `sdc` role is `/dev/sdb`:
the path already lies. So the name is a **label** and nothing is keyed on it.

### A disk is identified by what is written on it

Each disk's filesystem carries a small file at its top, `disk.uid`, holding a UUID that sidon
writes the first time it sees the disk (`create_new`, then fsync, so two threads discovering
one new disk cannot disagree). Re-mounting the disk under another name, or a reboot that
reorders the probe, changes the label and leaves the identity alone. Everything an operator is
shown about a disk, everything the surplus-copy ledger is keyed by and the name a manual move
takes is that identity; the label is shown beside it and accepted as a convenience.

Alternatives, and why not: the filesystem UUID from `blkid` needs a mapping from mount to
device to `/dev/disk/by-uuid` that sidon has no other reason to have, does not exist on a dev
box, and silently changes when a disk is re-formatted -- which is a different disk, so that part
is right, but it is also the part the identity file gives for free. A column in Hydra is
excluded by the same node-local argument as the group-to-disk map.

Three honest edges. A disk that will not take the file (read-only, full) is addressed by its
label and reported `uid_persisted: false`, because that identity is no more stable than the
name it came from. Two disks carrying one identity (a cloned disk image) are told apart in
memory by their label and reported the same way. And the identity lives at the top of the
*filesystem*, so on a node that predates `disks/` the first disk's is written in the sidon
root.

What the kernel currently says backs each mount (`/proc/self/mountinfo`) is reported as
`device`, purely so an operator can see the directory called `sdc` is `/dev/sdb`. Nothing
keys off it.

### Reads resolve without a schema change

A read must know which disk holds an extent group. Three ways to answer that, and the
cheapest correct one wins:

1. **Record `disk_id` in the metadata layer.** Correct, and wrong in a subtle way: which
   disk an egroup sits on is a *node-local* fact. Putting it in Hydra means every local
   placement decision becomes a cluster write, and a node that reorganises its own disks
   has to tell the cluster about it.
2. **Encode the disk in the egroup id.** Cheap to read, impossible to change: an egroup
   could never be moved between disks, which rules out tiering and rebalancing later.
3. **Build the map locally at startup.** Scan `disks/*/egroups/` once and hold
   `egroup_id → disk`. Placement stays node-local, egroups can move, and nothing new goes
   into Hydra.

**Take (3).** The scan is one `readdir` per disk at startup, and the map is small — an
egroup is 4 MiB, so a 300 GB disk holds ~75,000 of them, which is a few megabytes of
in-memory map.

This also gives disk-loss detection for free, and it falls out of a mechanism that already
exists. `referenced_egroups()` reads the whole block map to decide what is live. An egroup
that is **referenced but not in the local map** is one this node was supposed to have and
does not — which is precisely the state a dead disk produces. Purah already walks that set
for the mark-sweep; the same pass names the repair candidates.

### Placement: least-free-first

When sealing, pick the disk with the most free space. Self-balancing, needs no history, and
a disk added later fills preferentially until it matches the others — which is the
behaviour an operator expects after adding a disk.

Deliberately *not* round-robin: after adding a second disk, round-robin gives the empty
disk half the new writes and it stays permanently behind.

This is built (`pick_disk` in `sidon/src/extent/placement.rs`) and the observable half is
built with it: `valcli storage.list` prints one row per disk, and `valcli storage.placement`
lists which disk holds which extent groups, read from the directories rather than from the
index so it is still an answer when the index is what is in doubt.

When the owning vdisk's container names a tier (`SSD`, `HDD`, `NVME`), disks of that class
that still have more than 10% free are considered first, and if there are none the choice is
made across every disk. That fallback is the important half: until now the container's tier
was a label nothing read, and a container labelled `SSD` on a node with no SSD must still be
able to write. A disk whose class is not known never matches a preference.

A disk's class comes from an operator's `disk.tier` file if there is one, else the kernel's
rotational flag (looking through device-mapper to what backs it), else *unknown*. Virtual
disks are routinely reported as rotational whatever they sit on, which is why the file wins.

### Tiering, later and honestly

Nutanix tiers: oplog on SSD, extent store across SSD and HDD, and Curator migrates cold
extents down. Helios has a `tier` on the storage container (`SSD`/`HDD`/`NVME`) that is
**currently only a label** — nothing reads it for placement.

The staged version:

1. **Journal on the fastest disk.** The journal is the write path; every guest write lands
   there before it is acknowledged. This is the single highest-value use of an SSD and
   needs no migration machinery.
2. **Honour the container's tier when placing egroups.** A container marked `SSD` seals
   onto SSD-class disks where any exist.
3. **Migrate cold egroups down.** A Purah job, and the largest piece. Sealed groups are
   immutable, so moving one is a copy plus a map repoint plus a delete — no coordination
   with writers, which is what makes it tractable at all.

Only (1) is worth doing before there is mixed media to tier *onto*. The test nodes have
two identical 300 GB disks, so tiering has nothing to decide.

(2) and (3) both need something this design did not have when it was written: a record of
what is actually hot. Free-space placement needs no history, which is why it could be
specified above without one, and temperature placement is nothing *but* history. That gap is
now filled — `hydra.dfs_egroup_access` records per-extent-group read and write counts and
last-access times, and `valcli storage.heat` ranks them
([metadata.md §8](./metadata.md), D-22). It is a prerequisite rather than a companion: step
(3) cannot decide what to spill down without it, and step (2) cannot tell afterwards whether
it placed anything well.

The ranking itself reports and stops there. Nor should the heat data be read as more exact
than it is: the counters are aggregated in memory and flushed on a timer, so a crash loses a
window of them. That is fine for deciding which disk a copy should sit on and is explicitly
forbidden as an input to anything deciding whether a copy exists.

#### What is built of (2) and (3), and what is not

* **(2) is built** as the placement preference described under *Placement* above.
* **(3) is built as a mechanism and a policy, operator-invoked**: `valcli storage.tier`
  plans, `--apply` carries out, `valcli storage.move` relocates one named group. It is
  `purah-tier`, `purah-move` and `purah-placement` on the control socket and in
  `sidon/src/purah/tier.rs`.
* **(1) is designed and not built**, and is the first thing to build when a node has mixed
  media. The journal is opened by path at vdisk attach and a live journal cannot be relocated
  without a drain; it is not worth that until there is a faster disk to put it on.
* **Nothing runs (3) on a timer.** The ranking was shipped as reporting so that somebody would
  read it before anything acted on it (D-22), and that reasoning is not discharged by the
  ranking existing. Turning it on is a decision for after an operator has watched what the
  plan proposes on real mixed media.

**The migration is node-local, so there is no map repoint.** This document's earlier
description of (3) said "copy, repoint, delete, with the map repoint conditional on the
epoch". The repoint it meant is `dfs_block_map`, and a disk-to-disk move does not touch it:
that table says which extent *group* holds an extent and a group keeps its identity when it
changes disk. What changes is this node's own `group → disk` index, which is the reason
option 3 above was chosen. Consequently no epoch is involved, no migration was added, and
other nodes' copies of the same group are unaffected -- a group replicated to three nodes has
three independent placements. (`hydra.dfs_egroups.path` records where a group was *created*;
nothing reads it and a move deliberately does not update it.)

**The move.** Sealed groups only, checked against Hydra immediately beforehand:

1. The source is read and hashed, and refused if it does not match the seal hash Hydra
   recorded -- a damaged group is never copied to another disk, where the copy would carry a
   clean-looking checksum of its own.
2. It is copied to `<id>.eg.moving` on the destination. That name does not end in `.eg`, so
   no directory scan (startup, lookup, sweep) can mistake it for a group. The destination
   needs room for the group and as much again.
3. The copy is fsynced, the kernel's cache of it is dropped, and it is read back from the
   disk and must hash identically. (Without the cache drop, "verify" re-reads the pages that
   were just written and would pass a copy that never reached the platter.) The source must
   also be unchanged in length and mtime: sealed means immutable, so one that moved under the
   copy is not to be trusted.
4. The copy is renamed to `<id>.eg` (atomic within a directory) and the directory fsynced. A
   reader can never be pointed at a half-written file: the real name does not exist until the
   copy is whole and verified. A rename onto an existing file is refused, never done.
5. The store's index is switched to the new disk.

**The delete is not part of the move.** The old copy stays and Purah's sweep removes it after
it has been seen surplus on two passes with the grace between them -- the rule reclamation
already uses, so that this daemon has one answer to "when may bytes be deleted". Three
reasons, each a way to point a reader at a vanished file: every attached vdisk holds its own
`EgroupStore` with its own index, so switching Purah's does not switch theirs and a reader
that resolved the old path a moment ago still opens it; it makes a crash at any point
harmless without a journal; and deletion is the one irreversible step. Readers also survive
the old copy finally going: a `NotFound` on the indexed path drops that index entry and
re-resolves from the disks.

Crash at each point: before the rename, there is one valid copy and an invisible temporary
(removed by the sweep once older than the grace; only Purah starts moves, so age is a safe
test); after the rename and before the switch, two valid copies, either serves reads, and the
surplus is found by the next sweep; after the switch, the same. There is no point at which
neither exists. The removal itself refuses three things: removing a copy when fewer than two
exist, removing the copy the store resolves reads to, and removing either when the two are not
byte-identical (nobody knows which is right; both stay and it is reported every pass).
Reclaiming a dead group removes every local copy, including a surplus one.

**The policy** (`plan` in `tier.rs`, pure, tested over described disks): only disks of a known
class take part, and at least two distinct classes are needed, else the plan says there is
nothing to decide. When the fast tier is below 20% free it spills its coldest groups --
measured-cold before unmeasured, and unmeasured not at all if the tally was capped, because
part of those is a gap in the measurement -- until it is back to 30%; the gap is hysteresis.
Otherwise it promotes the hottest measured groups (heat above zero) from slower disks, never
past 25% free on the fast tier, so a promotion cannot be what the next pass undoes. Not both
in one pass. No move leaves its destination under 10% free; each pass is bounded in moves and
bytes; ties are broken by id so two calls plan the same set.

**What the test cluster can and cannot show.** Both disks on every node are identical VMDKs,
so there is no faster disk to promote onto and nothing to spill to: the policy correctly
plans nothing there, and no performance claim is made or possible. The policy is exercised by
unit tests over disks that do not exist; the mechanism is exercised on real files by the Rust
tests and, on a node, by `valcli storage.move`. The disk class on those VMs may well be
reported identically; setting a `disk.tier` file on one disk would let the policy be watched
making decisions, at the cost of those decisions being made up.

### Capacity

`op_capacity` sums across disks and reports per-disk. An operator needs to see one disk
filling faster than another, and a single total hides exactly that.

### Failure

A disk that disappears takes its extent groups. With RF ≥ 2 those are re-replicated from
the other nodes by the mechanism that already handles a lost node — the loss looks the
same from the cluster's side, it is just smaller.

**With RF=1 a disk loss is data loss**, exactly as it is today. Multi-disk does not change
that and must not be described as though it does.

## What this costs

Roughly, in `sidon`:

* `EgroupStore` → a collection of stores, each with its own root and free-space accounting.
* Startup scan building `egroup_id → disk`.
* Placement on seal.
* `op_capacity` summing and reporting per disk.
* Purah: treat referenced-but-absent as a repair candidate.
* Placement on seal by tier, once there is mixed media: built, and a no-op until a node has
  disks of different classes. The temperature input is [metadata.md §8](./metadata.md); the
  migration job is `purah/tier.rs`.
* Provisioning: claim *every* qualifying disk, one filesystem each, mounted under
  `disks/`, rather than one PV in a shared VG.

The provisioning half is the smaller piece and cannot land first: claiming both disks
before sidon can use the second one gains nothing and loses the guard that currently keeps
`sdc` untouched.

## Where this leaves the second disk today

Unused, deliberately. The three options were:

| | Failure domain | Capacity | Verdict |
|---|---|---|---|
| One disk per node (today) | 1 disk = node's store | 150 GiB usable | Coherent |
| `vgextend` into the pool | 1 disk = node's store, twice as likely | 300 GiB | **Worse than today** |
| One store per disk | 1 disk = that disk's egroups | 300 GiB | The design above |

Leaving `sdc` cold is not the best outcome, but it is strictly better than pooling, and the
work to do it properly is bounded and understood rather than urgent.
