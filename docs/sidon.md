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

## 5. Operating it

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

## 6. Ports, and the lack of them

Sidon adds **no client-facing TCP port**.

- **Control** is a unix socket at `/run/sidon/control.sock`, reached from spark-daemon.
  Callers are authenticated once by the existing mutual-TLS mesh on 9099 rather than by a
  second credential nobody would rotate. Spectrum runs in a container and cannot reach the
  socket — by design; it asks spark.
- **Guests** attach over a per-vdisk unix socket under `/var/lib/hci/sidon/nbd/`,
  group-owned by `qemu`.
- **Replication** uses port **9105**, one connection per node pair.

> [!WARNING]
> The replication port carries guest data and **mTLS is not implemented yet**. Until it
> is, Sidon refuses to bind 9105 to anything but loopback — so a plaintext cluster data
> path cannot ship by someone forgetting. Multi-node replication needs that work finished
> first.

## 7. What is not built

- **mTLS on 9105**, as above.
- **Snapshots and clones.** The schema and the immutability rules are already in place —
  a snapshot is a map copy against a frozen parent — but nothing creates one yet.
- **Compression** at seal time, which is cheap because sealed groups are immutable and the
  footer already carries an algorithm byte. **Erasure coding** as a Purah job over cold
  sealed groups. **Deduplication** is argued against in
  [decisions.md](./dfs/decisions.md): the win on VM disks is identical OS images, which
  clone-from-image already gets for free as a map copy.
- **`vhost-user-blk`** beside NBD, deliberately last. Performance work reorders
  operations, and reordering is where invariants go to die.
