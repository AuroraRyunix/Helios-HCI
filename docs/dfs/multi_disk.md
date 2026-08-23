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

`hydra.dfs_egroup_replicas` exists in the schema, with an `egroup_id`, `node`, `path` and
`state` — and **nothing writes to it**. There are no references to it anywhere in
`sidon/src`. It is a table designed for a per-egroup placement model that was never built.
Worth knowing before designing around it: it is not a source of truth, it is an empty
table with a suggestive shape.

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
