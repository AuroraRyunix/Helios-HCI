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
which are ordinary files on `/var/lib/hci/sidon` — one XFS filesystem on a thin LV in
`vg_aether`.

When the journal reaches its high-water mark (64 MiB by default) a **drain** runs: each
touched extent is read, patched with the journal's newer bytes, and appended somewhere
new. The block map in Hydra is repointed, and only then is the journal allowed to forget.

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

## 5. Snapshots and clones

A snapshot copies the block map. It copies no data at all, and the number it reports for
bytes copied is zero because that is the honest figure.

```bash
valcli storage.snapshot <vdisk> <name>   # point-in-time, read-only
valcli storage.clone    <vdisk> <name>   # writable
valcli storage.children <vdisk>          # what was taken from it
```

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

## 6. Operating it

```bash
valcli storage.list
```

Per-node extent store usage, and every vdisk with its owner, epoch and replica count. A
vdisk showing a short replica set is one node-loss from unavailable.

```bash
mcli health_checks storage
```

Seven checks: replica health, mount options, writability, fstab safety, unreferenced
extent groups, replica counts, and control-socket latency. The latency one exists because
every other check asks the daemon a question and believes the answer; this one times the
question, and a control plane answering in twenty seconds is about to stop answering.

## 7. Ports, and the lack of them

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

## 8. Compression

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

## 9. What is not built

- **Scheduled snapshots.** Taking one is a command; nothing takes them on a timer, prunes
  them by a retention policy, or presents them in the console. The mechanism is done and
  the policy around it is not.
- **Snapshot rollback.** A clone gives you the old contents under a new name, which is
  enough to recover data and not the same as putting a VM back. Rolling a vdisk *back* to
  a snapshot in place needs the ownership and epoch story thought through, because it
  changes what an attached guest is reading underneath itself.
- **Erasure coding** as a Purah job over cold
  sealed groups. **Deduplication** is argued against in
  [decisions.md](./dfs/decisions.md): the win on VM disks is identical OS images, which
  clone-from-image now gets for free as a map copy.
- **`vhost-user-blk`** beside NBD, deliberately last. Performance work reorders
  operations, and reordering is where invariants go to die.
