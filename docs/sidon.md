# Sidon — the storage data path

Sidon serves VM disks. A guest talks to the Sidon on its own host, always, over an NBD
unix socket; Sidon decides where the bytes actually go.

It replaced Aether (Linstor + DRBD) because DRBD replicates *devices*. Every replicated
volume was a standing connection between named peers, costing a TCP port, a kernel object,
its own threads and RF−1 connections per node — the right shape for a handful of HA
volumes and the wrong shape for one volume per VM disk. The visible symptom was a ceiling
of 191 replicated volumes on the default port range; widening the range only moved the
wall. Sidon holds **one connection per node pair**, whatever the disk count.

The design documents are in [dfs/](./dfs/README.md): [architecture](./dfs/architecture.md)
for the shape, [invariants](./dfs/invariants.md) for the contract everything else exists
to satisfy, [ownership](./dfs/ownership.md) for the fencing proof, and
[decisions](./dfs/decisions.md) for every choice and the alternatives it beat. This
document is the operator's view.

---

## 1. What a write actually does

```
guest write
   ↓  NBD, unix socket
journal append + fdatasync          ← on this node
   ↓  port 9105, in parallel
journal append + fdatasync          ← on every other replica
   ↓
acknowledged to the guest
```

The two journal appends really are concurrent, and so are the guest's writes. A write is
appended to the journal at once (its records are in the file, in the order writes arrived, and
nothing reads them yet), and then waits to be *committed* with whatever else is waiting: the
first waiting writer takes everything queued as one batch, issues one `fdatasync` of the journal
while one request per replica carries the batch's bytes in parallel, and only when this node and
every replica have it durable are the batch's writes made visible and acknowledged
(`sidon/src/vdisk/commit.rs`, [dfs/group_commit.md](./dfs/group_commit.md)). At queue depth one a
batch is one write and costs what a write always cost; at depth sixteen a batch is about sixteen,
so the sync and the round trip are shared. A guest write bigger than 1 MiB is several records and
one commit marker, kept together; only the last request of a batch is made durable before the
replica answers (the earlier ones carry `APPEND_DEFER_SYNC`; the last one's `fdatasync` takes them
with it). The guest is told nothing until the local sync has succeeded *and* every replica has
answered OK, so what is acknowledged is exactly what it always was: durable on every copy, and
visible to reads only from then on. A crash after some records but before the commit marker
leaves a group replay discards, the same as a crash between two records always did. Before 2026-10
the appends were serial -- a local `fdatasync` and then a round trip to each replica, per 1 MiB
record -- and, until the commit pipeline, a connection served one request at a time under the
vdisk lock, so a guest's queue depth bought nothing.

Nothing on that path touches Hydra. That is the design's one inviolable performance rule:
acknowledgement never waits on the metadata layer.

The journal is **write-all, not quorum**: an append that has not reached every replica is
not acknowledged, and the guest gets EIO. That is a deliberate trade. It costs
availability during single-replica loss, and it buys a three-line safety proof — fencing
one replica stops the old owner, because the old owner needed all of them; reading one
replica sees every acknowledged write, for the same reason. A quorum journal keeps writing
through a replica loss and turns both of those into multi-round protocols with corner
cases. DRBD protocol C users already accept this trade.

Losing a replica therefore stops writes until the set is restored. Purah notices within
seconds and re-replicates onto a spare — measured at about three seconds on the test
cluster — so the exposure is an interruption rather than an outage. With no spare node
available, writes stay refused and `valcli storage.list` says which vdisk and why.

At **ftt=0** there are no peers and the write-all set is just this node. Everything above
still holds; there is simply nothing to fence and nothing to re-replicate. A single-node
cluster is a supported topology, not a stepping stone — see
[architecture.md §5](./dfs/architecture.md).

## 2. Where the bytes live

A vdisk is cut into 1 MiB **extents**. Extents live in 4 MiB append-only **extent groups**,
which are ordinary files on XFS filesystems that sidon mounts itself under
`/var/lib/hci/sidon/disks/<filesystem-uuid>` — one on a thin LV in `vg_aether` (the journal
volume) and one per further disk. Nothing sidon owns is in `/etc/fstab`; see
[D-27](./dfs/decisions.md) and [multi_disk.md](./dfs/multi_disk.md).

When the journal reaches its high-water mark (64 MiB by default) a **drain** runs: each
touched extent is read, patched with the journal's newer bytes, and appended somewhere
new. The block map in Hydra is repointed, and only then is the journal allowed to forget.

The drain runs **beside** the guest, not in front of it. The write that crosses the
high-water mark starts the drain on a background thread and is acknowledged on its own
journal record; before 2026-10 it ran the drain inline and sat out the whole thing (about 3
seconds for 64 MiB), which is why a single large write measured a third of the streaming
rate. The mechanics, in `sidon/src/vdisk.rs`:

1. **Plan, under the vdisk lock.** The journal is *rotated*: `<vdisk>.jrn` is renamed
   `<vdisk>.jrn.old` and sealed, and a fresh `<vdisk>.jrn` takes new writes. The overlay
   ranges that point into the sealed file are frozen, and the block-map entries they touch are
   copied out.
2. **Run, unlocked.** The drain reads the sealed file, builds the extents, appends and
   replicates them, syncs, writes the map rows and makes the drain-commit CAS. Guest reads
   see the old map plus the overlay, which still holds every range being drained; guest
   writes go to the live file.
3. **Finish, under the lock.** The new map entries are applied, the overlay ranges still
   pointing into the sealed file are dropped (a range a newer write has covered points into
   the live file and stays), and the sealed file is deleted. The replicas are then told to
   drop the journal records *older than* the first live sequence number — by sequence, never
   wholesale, because they hold records acknowledged while the drain ran
   (`OP_TRUNCATE_TO`; a replica from before this change refuses it and keeps its whole
   journal, which is the safe way to be wrong).

Inside the run phase the extents go to the replicas through a pipeline: one thread and one
connection per replica, fed in order, so the drain reads and builds the next extent while the
last is on the wire. Every put to an extent group but its last carries `APPEND_DEFER_SYNC` (the
replica skips its fsync; the group's last put, which is synced, flushes the file), the drain
flushes its own copy of a full group while the replicas catch up, and it waits for every
replica's answer before it tells Hydra a group is sealed and before it writes a single map row.
Each drain logs one line, `drained N extent(s) in T ms (replicas …, this disk …, hydra …)`,
saying which of the three it spent its time on.

Guests are held back only at a **hard ceiling**, twice the high-water mark (128 MiB by
default): a write that finds the journal there waits, without the vdisk lock, for a drain to
make room, and fails with an error if the drain cannot run (a degraded vdisk) rather than
hang or let the journal fill the volume. A write admitted just under the ceiling may take the
journal past it by that one write. Anything that needs a *drained* vdisk — detach, seal,
snapshot, clone, `flush`, a replica heal — waits for a running drain and then drains whatever
is left under the lock (`vdisk::drain_all`, `lock_idle`), so what they hand back is as drained
as it ever was. A crash at any point leaves the sealed and live files; replay reads them in
that order. `valcli storage.list`/`status` report `draining`, `high_water` and `hard_ceiling`.

Two orderings are load-bearing and never depart from:

- **Extent bytes are durable before any map row points at them.** A crash between the two
  leaves orphaned bytes, which Purah sweeps. The reverse leaves a map pointing at bytes
  that do not exist, which is data loss.
- **The journal is not truncated until the map commit has applied.** A crash between the
  two replays records that are already drained, which is harmless.

Sealed extent groups are **immutable**, permanently. Overwrites are never in place: a
rewritten extent is appended somewhere new and the map is repointed. That single property
is what makes repair a checksum comparison rather than a divergence protocol, a snapshot a
map copy rather than a data copy, and scrub lock-free. The price is garbage collection —
paid deliberately, because GC is a performance problem and replica divergence is a
correctness problem.

Every stored extent carries a footer with its checksum **and its identity** (which vdisk,
which extent index). A correct checksum only proves the bytes are undamaged, not that they
are the right bytes; the identity is what makes a misdirected read self-evident.

## 3. Who owns a vdisk

Exactly one node serves a vdisk at a time. Ownership lives in Hydra as an `(owner, epoch)`
pair and moves only through a Daruk compare-and-swap conditioned on **both** halves —
conditioning on the owner alone would let a node that held the disk two takeovers ago
re-take it after a round trip it never noticed losing.

Every journal append carries its writer's epoch, and **every replica remembers the highest
epoch it has been fenced at, on disk, fsynced before the fence is acknowledged**. An
append below that is refused. That is the entire safety mechanism, and it works when the
deposed owner is wedged, lying about its own state, unreachable, or has no idea it was
deposed. The lease exists for orderly handover and to bound how long a loser keeps trying;
it is not what makes anything safe.

Taking over is four steps:

```
1. CAS ownership in Hydra: epoch e → e+1
2. FENCE every reachable replica at e+1   (in parallel — a failover has a time budget)
3. READ TAIL from one of them, replay it
4. Serve
```

Step 3 adopts the replica's journal **unconditionally**, including when it is shorter than
what is on local disk. "Shorter than what is here" is precisely the case where this node
owned the vdisk previously and its own file is a stale history — replaying that and
appending to it is how a journal ends up with a sequence hole.

**Forwarding.** A node that does not own a vdisk can still serve it, by relaying every
operation to the node that does. Correct and slower. This is what removes live migration's
cutover instant: a VM resumes on the destination before its storage has moved, and
ownership follows at leisure. It is deliberately not a special case — it is the same path
a post-failover VM uses before locality catches up, so it is exercised constantly.

## 4. Purah

The curator, running inside Sidon. Three jobs, all background, none on the guest's path:

- **Re-replication** — restore the replica count after a node is lost. The new member
  joins the write-all set *before* the backfill, never after: backfilling first leaves a
  window where a write lands on the old set and nothing later notices the hole.
- **Reclamation** — mark-sweep, with no reference counts anywhere. A refcount is a
  distributed counter with a crash window between every data operation and its count
  operation. An extent group is deleted only after being seen unreferenced **twice**, with
  a grace period between, and only if it is not open, not young, and not held by an
  attached vdisk. Each guard covers a different way live data looks like garbage — mostly
  the drain window, where bytes are durable but the map does not point at them yet.
- **Scrub** — recompute every sealed group's hash against the one recorded at seal time.
  Needs no lock, because sealed means immutable.
- **Access accounting** — tally how often each extent group is read and written, and flush
  the totals to `hydra.dfs_egroup_access` on its own, much shorter timer. Reads a node
  serves from its replica store to *another node's* vdisk are counted too: they never pass
  through `Vdisk::read`, so the replica store shares the same in-memory tally, with no
  metadata round trip on the read. On a separate
  thread from the sweep deliberately: a sweep stalled against an unreachable Hydra must not
  also stop the counters, because the ranking they feed is what an operator reaches for when
  they are trying to find out why a disk is busy.

- **Tiering** — move a sealed extent group between two disks of the same node on the
  strength of the heat ranking: copy, verify the copy against the group's seal hash, publish
  it by rename, switch, and leave the old copy for the sweep, which removes it after seeing
  it surplus on two passes with the grace between them. Operator-invoked and bounded; nothing
  runs it on a timer. See [dfs/multi_disk.md](./dfs/multi_disk.md).

The counters are recorded on the data path and are therefore **approximate** — they live in
the daemon between flushes, so a crash loses everything counted since the last one. They
decide where a copy of data goes, never whether it exists; the boundary and its reasoning are
in [dfs/metadata.md §8](./dfs/metadata.md) and D-22.

## 5. How many copies a vdisk gets

A vdisk is created with the number of copies the cluster's redundancy factor asks for.
That sentence was not true until recently, and the way it was false is worth keeping.

**Where the number lives.** `/etc/hci/cluster.json`, as `redundancy_factor`, written by
`cluster create -r` and copied to every host. It is not in ZooKeeper — `/cluster_state`
holds one word, `started` or `stopped` — and it is not
`hydra.cluster_settings.replication_factor`, which governs how many copies of the
*metadata* Scylla keeps and says nothing about a guest's disk. A storage container may
override it for its own vdisks with `hydra.storage_containers.ftt`, and that is what the
schema means when it records a vdisk's `rf` as copied from its container.

**The unit.** `redundancy_factor` and a container's `ftt` both count **failures
survived**. `dfs_vdisks.rf` counts **copies**. They differ by one:

| ftt | copies | meaning |
|-----|--------|---------|
| 0 | 1 | no replication — what a single-node cluster is created with |
| 1 | 2 | survive one host loss |
| 2 | 3 | survive two |

The count is clamped to the number of nodes that could hold a copy, so a single-node
cluster carrying a multi-node `cluster.json` keeps creating disks instead of refusing
every create. An explicit `rf` in the create request still wins over all of this, and so
does an explicit `replicas` list, which carries its own count.

**What went wrong.** `op_create` defaulted `rf` to 1, and no caller has ever sent one —
not vali, not the console, not the CLI, not the Elixir tier. So the default was the
policy: every vdisk on every cluster was single-copy whatever the operator had
configured. Nothing reported it, because every replication view compared a vdisk's
replica list against the `rf` on its own row, and the same defect had written that as 1
too. One replica, one requested, healthy. Purah's re-replication worked correctly the
whole time and had nothing to do, because one copy *was* the requested count.

**Seeing it.**

```bash
valcli storage.replication
```

Per vdisk: the copies policy asks for, the `rf` the vdisk itself asked for, and the
copies that exist. Three numbers rather than two, because the two states they separate
have different fixes. `degraded` means a disk lost copies it once had, and Purah restores
those by itself. `under-policy` means a disk never requested them, so there is nothing to
restore and no amount of healing will change it.

**Fixing an existing fleet.** Creates made before this change stay single-copy, and they
are not touched automatically. Purah re-replicates on the timer and on a failed write,
but only to replace a replica that stopped answering — an emergency, since write-all
means the guest is taking EIO until the set is restored. Topping up a disk that is
serving its guest perfectly well is not an emergency, and doing it on a timer would have
turned this change into an unannounced copy of every disk on the cluster at the next node
restart.

So it is opt-in and typed by hand:

```bash
valcli storage.replicate <vdisk_id>    # one vdisk, on its owner
valcli storage.replicate --all         # every vdisk this cluster owns
```

One copy per vdisk per run, so a disk that needs two more takes two runs. That bound is
deliberate: it keeps the amount of copying to something you can watch finish. Only
attached vdisks are eligible — re-replication runs on the owner, because the owner is the
node that has the data.

Changing the cluster's redundancy factor itself is a separate decision and is not made
here; these commands only make disks match whatever it is already set to. It is made with
`cluster add-node --node <ip> -r N`, see [cluster.md §F](./cluster.md); a cluster that was
created on one node carries 0 until someone does.

**A create always names a container.** The copy count comes from the container's `ftt`
before the cluster's factor, so a create that names no container, or one that matches no
row in `hydra.storage_containers`, gets no container policy at all and falls through to the
cluster factor — 0 on a cluster that was grown from one node. That is how both live vdisks
came to hold one copy on a cluster whose `default-pool` has `ftt=1`: they were in a
container called `default`, which is not a row, because the Phoenix tier's
`Spark.dfs_create/4` had no container parameter and `/api/vms/update`'s add-disk path
omitted it. Now:

* every create path names a container — `Spark.dfs_create/4` raises without one, VM disks
  and image uploads default to `default-pool` (`Containers.default_name/0`, equal to
  `helios_sidon.DEFAULT_CONTAINER` and to Sidon's own fallback, and a test holds the three
  together), and a disk added to a VM carries the container its entry names;
* each of them checks the container exists before creating anything, so a typo is refused
  with nothing to roll back;
* `test_vdisk_creates_name_a_container.py` finds the creates itself — it parses every Python
  module and Elixir source for a create — instead of listing known callers, which is how
  the old guard missed the add-disk path.

**The console shows all three numbers.** A vdisk holds some copies, was created asking for
`rf`, and the cluster policy asks for a third. The storage page's replica badge draws the
copies it holds against the larger of the last two, and says "asked 1, policy 2" when the
vdisk asked for less than policy. A vdisk that asked for one copy and holds one is
therefore **under-replicated**, not healthy: that match is the one every vdisk showed while
the defect was live, and one copy is what a node loss destroys. A vdisk that lost a copy it
asked for is told apart from one that never had it, because the first heals itself and the
second needs `valcli storage.replicate`.

## 6. Snapshots and clones

A snapshot copies the block map. It copies no data at all, and the number it reports for
bytes copied is zero because that is the honest figure.

```bash
valcli storage.snapshot <vdisk> <name>   # point-in-time, read-only
valcli storage.clone    <vdisk> <name>   # writable
valcli storage.children <vdisk>          # what was taken from it
valcli storage.snapshots <vdisk>         # the snapshots, who took each, what depends on it
valcli storage.rollback <vdisk> <snap>   # put a STOPPED VM's disk back, in place
```

Snapshots on a timer, with a retention policy, and rollback of a detached vdisk are built:
see [dfs/snapshots.md](./dfs/snapshots.md). Rolling back an attached one is a design, not
a feature: [dfs/rollback_attached.md](./dfs/rollback_attached.md).

Several vdisks can be snapshotted together as a crash-consistent set, by holding their guest
still, as a protection domain: [dfs/protection_domains.md](./dfs/protection_domains.md). Sending
snapshots to another site is a design ([dfs/replication.md](./dfs/replication.md)); Sidon's
`replicate` module holds its data plane (export a snapshot's missing extent groups, import them
with verification, publish the map atomically), tested against two directories on one machine and
reachable from nothing yet.

Sealed extent groups are immutable, so parent and child share every one of them and
neither can disturb the other: a write to either is redirect-on-write, appending
somewhere new and repointing only its own map. The cost is the number of extents in the
map, not the bytes on disk, so a snapshot of a terabyte costs what a snapshot of a
gigabyte costs.

**Nothing keeps a reference count**, and this is where that decision pays. Purah marks
from the whole of `dfs_block_map`, so a group the child points at is live whether or not
the child is attached, and whether or not the parent still exists. Deleting a parent that
has snapshots is safe and frees only what nothing else references — which looks alarming
until you have `storage.children` to show you what is holding it.

A clone is the same operation ending in a different class. That is what clone-from-image
is: a template is already immutable, so a fleet cloned from it shares its extents until
each VM writes its own. This is also the argument against deduplication in
[decisions.md](./dfs/decisions.md) — the win dedup is usually bought for is identical OS
images, and this has it without buying anything.

**Where the call has to go.** A writable parent must be attached on the node the request
reaches, because its journal has to be drained before its map is a complete answer and
only its owner can drain it. An immutable parent has no journal and any node can copy it.
`valcli` routes to the owner for you.

**What a crash leaves.** The child's row is written first in class `forming`, then the
map, then the class is set. Nothing attaches a `forming` vdisk, so an interrupted copy
leaves a row that says what it is rather than a disk that reads as half zeroes. Delete it
and take the snapshot again.

## 7. Operating it

```bash
valcli storage.list
```

Per-node extent store usage, and every vdisk with its owner, epoch and replica count. A
vdisk showing a short replica set is one node-loss from unavailable.

The Extent Store table has four states and none of them is "online with zero": `online`
(a real capacity), `not ready` (spark answered 503 or a capacity of zero: sidon is starting,
stopped, or has no mounted disk -- it binds its control socket only after its disks are
mounted, so during a restart the capacity is unknown, not zero), `unreachable` (nothing
answered) and `error` (any other refusal, with what it said). The Phoenix storage page says
the same in its stores banner.

```bash
valcli storage.heat        # or storage.heat 25 for a longer ranking
```

Per node: the hottest extent groups, the coldest ones, and how many have no access data at
all. Read and write counts with the window they were counted over, so the score can be
checked by hand rather than taken on trust. It flushes each node's in-memory tally before
ranking, so what comes back describes the node now and not as of the last timer tick.

Nothing here moves data; this is the input the tiering job reads.
`SIDON_ACCESS_FLUSH=0` turns the tally off entirely, counters included, in which case this
command has nothing to rank and says so.

```bash
valcli storage.benchmark default-pool
```

Creates a throwaway 256 MiB vdisk in the container, attaches it, measures it through the NBD
socket a guest would use, and detaches and deletes it (also on failure). **Before 2026-10 it
wrote 64 MiB in one request into a new 100 MiB vdisk and printed that single timing.** One
request is not a rate: it landed exactly on the journal's high-water mark, so the drain ran
inside that write and the printed ~14 MiB/s was a drain divided into 64 MiB. It now warms up,
then prints one labelled line per workload with MiB/s, IOPS and the per-request time:
1 MiB sequential writes at queue depth 1, 4 KiB synchronous writes at queue depths 1 (the
latency a guest's fsync sees) and 16, 1 MiB sequential reads, 1 MiB writes at queue depths 4 and
16, and 16 MiB writes. The vdisk is four times the journal high-water mark so drains happen during
the run. Because a write is acknowledged before the drain it triggered has finished, a write
line also shows the **sustained** rate (bytes over the time until those drains are done)
whenever a drain outlived it; that is the figure that stays true over a long stream, and the
acknowledged rate is the one a guest sees in a burst. A final line reads the sequential range
back and checks every byte, because a benchmark that is fast by losing data is worse than a
slow one. The 4 KiB writes are measured at queue depth 1 and at queue depth 16: the pair is the
measure of group commit ([dfs/group_commit.md](./dfs/group_commit.md)), the first being what one
synchronous writer sees and the second what a database or a guest filesystem journal gets when it
keeps writes in flight. It uses `qemu-img bench` (queue depth, millisecond timing) and `qemu-io`
(the read-back).

Each benchmark leaves its extent groups for Purah, so on a node with `SIDON_PURAH_INTERVAL=0`
(the test cluster) repeated runs accumulate garbage until a sweep is run.

```bash
valcli storage.placement [N]          # which disk of each node holds which extent groups
valcli storage.tier [--apply]         # plan, or with --apply make, disk-to-disk moves
valcli storage.move <egroup> <disk> <node>
valcli storage.compact [--apply]       # plan, or with --apply compact sparse sealed groups
valcli storage.dedup.estimate          # read-only: bytes dedup would share beyond clones
```

`storage.list` also prints one row per extent-store disk: its identity, the directory it is
mounted at, the device the kernel says backs it, its class, and how full it is. The identity
is what the disk carries on its own filesystem; the directory is only a label, and on one
node the two have already disagreed. `storage.placement` is read from the disks' directories
rather than from the daemon's index, and lists a group held twice on one node separately as a
surplus copy. `storage.tier` plans by default; **on the test nodes, whose two disks are
identical, it will say there is nothing to decide**, and `storage.move` is how the mechanism
is exercised there. Neither is a performance result.

`storage.compact` finds sealed groups that are mostly garbage (default: under 50% live) because a guest
rewrote what they held, copies the live extents into a new group, verifies and replicates it, repoints
the map by compare-and-swap, and leaves the old groups to the sweep. It plans unless `--apply`, is
bounded by groups, bytes, rate and time, and never runs on a timer. It leaves alone any group a writable
vdisk not attached on that node still points into, and it does not free a replica's copy of the old
group (nothing does yet), so the plan prints what it would add on replicas beside what it would free.
`storage.dedup.estimate` hashes a sample of sealed extents and reports, per container, what dedup would
share beyond what clones and snapshots already share; it writes nothing. Both are described in
[dfs/compaction.md](./dfs/compaction.md).

### Mounts

```bash
sidon mounts            # read only: is each disk in /etc/hci/sidon-disks mounted and proven?
sidon mounts apply      # sidon stopped: move an old layout, mount what is missing
```

A disk is trusted only when its directory is a mount, is the device carrying the recorded UUID,
and holds `disk.uid`. With the **journal volume** absent sidon does not start and retries; with an
**extent disk** absent it starts without it, and `valcli storage.list` lists the disk and why.
`apply` refuses while the control socket answers. A node still in the old layout (the volume
mounted at the root) is moved the next time sidon starts.

```bash
mcli health_checks storage
```

Seven checks: replica health, mount options, writability (aimed at the journal volume, never at
the root filesystem), fstab safety (now: no sidon mount in fstab at all), unreferenced
extent groups, replica counts, and control-socket latency. The latency one exists because
every other check asks the daemon a question and believes the answer; this one times the
question, and a control plane answering in twenty seconds is about to stop answering.

## 8. Ports, and the lack of them

Sidon adds **no client-facing TCP port**. 9105 is peer-to-peer and mutually
authenticated; nothing outside the cluster can speak to it.

- **Control** is a unix socket at `/run/sidon/control.sock`, reached from spark-daemon.
  Callers are authenticated once by the existing mutual-TLS mesh on 9099 rather than by a
  second credential nobody would rotate. Spectrum runs in a container and cannot reach the
  socket — by design; it asks spark.
- **Guests** attach over a per-vdisk unix socket under `/var/lib/hci/sidon/nbd/`,
  group-owned by `qemu`.
- **Replication** uses port **9105**, one connection per node pair.

**Replication is mutually authenticated**, against the cluster CA in
`/etc/hci/spark/certs` — the same material Impa already issues and renews, so the storage
tier introduces no second credential for someone to discover has expired.

Mutual, not server-only. Encrypting the bytes while accepting any connection would miss
the point: this port carries `FENCE` as well as `APPEND`, so an unauthenticated peer
could raise the epoch on a vdisk and make every replica refuse the real owner's writes.
The fencing proof assumes only cluster members can speak the protocol, and this is what
makes that true.

Loopback is the one exception and stays plaintext: a connection that cannot leave the
host cannot be intercepted off it, and it is how the protocol is exercised on a machine
with no certificates. Everything else is refused without the material rather than
downgraded — a daemon that quietly serves guest data in the clear because a file was
missing is worse than one that will not start.

The bind address and the peer list are read from `/etc/hci/cluster.json` rather than
configured into the unit. A one-host cluster binds loopback, because at ftt=0 there is
nothing to replicate to; a second host appearing in that document is all it takes.

## 9. Compression

Compression is a property of the **container**, not of a vdisk and not of the cluster. A
container is already the unit an operator reasons about for tier, quota and fault
tolerance, and it is the level where this trade-off is actually decided: a container of
golden images is written once and read forever and wants it on; one holding a database's
data files usually does not.

```
valcli storage.container.create templates --compression lz4
valcli storage.container.update default-pool --compression lz4
valcli storage.list                      # the setting is a column
```

The console's storage page has the same controls, and an image upload takes the container
it should land in — which matters more there than anywhere, since an ISO is the clearest
case of write-once-read-many.

### What it does, and when

An extent is compressed as it is **sealed into an extent group**, and an extent group is
never rewritten. Three consequences follow, and they are the whole reason this is safe to
change on a live container:

* Turning it on applies to what gets sealed **next**. Nothing already on disk is touched,
  so enabling it does not start a rewrite storm and does not reclaim anything by itself.
* Turning it off is equally undramatic. Already-compressed groups stay compressed and stay
  readable, because each extent's footer records what it actually is rather than what its
  container currently says.
* Sidon reads the setting when it **opens** a vdisk. A change therefore takes effect the
  next time that vdisk is attached, not mid-flight.

An operator who turns compression on and sees usage unchanged is seeing it work.

### What it costs to verify

The footer's checksum covers the bytes **as stored**. A scrub therefore verifies a
compressed extent group without decompressing any of it, and a repair copies a compressed
extent to a new replica as a byte copy — neither path has to know the codec. That is why
compression did not complicate Purah at all.

Incompressible data is stored verbatim. LZ4 on random bytes produces more than it
consumed, and a setting meant to save space must not be able to cost it.

### Reading old data

`COMP_NONE` is zero, which is what the reserved byte in every footer written before this
existed already contains. Those extents read back unchanged, with no backfill, no
migration and no version check. A container with no compression column — every container
that predates the setting — behaves exactly as it did.

## 10. What is not built

- **Rollback of an attached vdisk.** Scheduled snapshots with a retention policy, a read-only
  console view, and in-place rollback of a *detached* vdisk are built
  ([dfs/snapshots.md](./dfs/snapshots.md)); a clone still gives the old contents under a new
  name, and rollback puts the VM's own disk back. What is not built is rolling back a vdisk a
  guest is reading, because that changes the bytes under the guest's filesystem and no
  operation in Sidon can make the guest's caches true again. It is refused, and the design for
  doing it properly (stop the VM, roll back, start it, as one task tree) is
  [dfs/rollback_attached.md](./dfs/rollback_attached.md).
- **Replication to another site.** Designed in [dfs/replication.md](./dfs/replication.md)
  (D-28 to D-31). `sidon/src/replicate*` has the data plane and its tests, run against two
  directories on one machine; there is no transport, no listener and no TLS verifier, and no
  second cluster has ever been involved. Failover and failback are a design only.
- **Tiering, on a timer and on mixed media.** The disk-to-disk move and the pass that plans
  it from the heat ranking are built and operator-invoked
  ([dfs/multi_disk.md](./dfs/multi_disk.md)); what is not built is anything running that pass
  unattended, and any evidence it improves anything, because the test disks are identical.
  Putting the write-ahead journal on the fastest disk is designed and not built.
  Measuring first and moving later is the order on purpose: a curator that began migrating
  data the moment it could measure temperature would be acting on a ranking nobody had
  looked at.
- **The extent ID map, stage 3.** Helios has two map levels where Nutanix has three. Stages 1
  and 2 of D-23 are built: Purah's mark phase follows both levels, and migrations `0020` and
  `0021` add the table and the column, with a read path that stays dormant because nothing
  writes an extent id. **Stage 1 is the one to roll out first and let run through a full sweep
  cycle**; the procedure and what to observe between stages is
  [dfs/extent_id_map.md](./dfs/extent_id_map.md). Writing extent ids is designed there and not
  built.
- **Erasure coding** as a Purah job over cold sealed groups. Decided against, for now: on
  three nodes a 2+1 stripe saves at most 25% of the cold data's raw capacity, and costs the
  ability to re-replicate after a node loss, because the stripe has no spare node to heal
  onto. The arithmetic, and the node count that would change the answer, are D-24 in
  [decisions.md](./dfs/decisions.md). **Deduplication** was argued against there originally:
  the win on VM disks is identical OS images, which clone-from-image gets for free as a map
  copy. The D-23 addendum revisits that with the extent id map in hand, costs it, and
  recommends a read-only estimator before anything is built.
- **`vhost-user-blk`** beside NBD, deliberately last. Performance work reorders
  operations, and reordering is where invariants go to die. Designed, not built: the NBD
  transport is a small part of a request. (Sidon used to serve one request at a time per
  connection whatever the guest's queue depth; since the commit pipeline it serves up to 32 and
  shares their syncs, which was the prerequisite the transport could not supply.) D-25 and
  [dfs/vhost_user_blk.md](./dfs/vhost_user_blk.md) carry the design and the benchmark that
  would justify it.
