# Ring Lifecycle

How a node joins the Hydra (ScyllaDB) ring, how it leaves temporarily, and how it leaves
for good — and which of those Helios does for you.

This is the Nutanix **Cassandra ring detach / attach** lifecycle. Nutanix auto-detaches an
unhealthy CVM from its ring; Helios deliberately does less than that, and this document
says exactly how much less and why.

---

## 1. Two memberships, not one

Helios tracks a host twice, and the two records mean different things:

| | `hydra.nodes` | the ScyllaDB ring |
| :--- | :--- | :--- |
| Written by | Vali, Mipha | Scylla itself, via gossip |
| Means | "this host may be given VMs" | "this host holds replicas of the metadata" |
| Changed by | an `UPDATE` | `nodetool decommission` / `removenode` / a bootstrap |
| Reverts | instantly | never, without streaming data |

Marking a host `DOWN` removes it from the VM scheduler in one write. It removes it from
the ring not at all: the ring still assigns it token ranges, and every `QUORUM` read and
write still counts it toward the replicas it needs. On a three-node RF=3 cluster that is
the difference between *one node is down* and *the next node to go down takes the cluster
with it*.

Nothing reconciled the two, so a node replaced months ago could still be the reason a
maintenance request is refused, with nothing on any screen saying so. `cluster ring` now
prints both side by side.

---

## 2. The quorum gate

Entering maintenance stops the host's services, `hydra-db` among them. That stop used to
be unconditional.

On a three-node cluster at RF=3, with Daruk reading and writing at `QUORUM`, stopping the
**second** node leaves one replica of three against a quorum of two. The keyspace stops
answering — for everything, including the maintenance workflow that is halfway through
recording what it did. The cluster does not come back until that node does.

So before anything is stopped, Vali establishes how many replicas can actually answer.

### Deriving the rule

**The replication factor is read from the database, never assumed.**

```sql
SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'hydra';
```

`SimpleStrategy` gives `replication_factor` directly. `NetworkTopologyStrategy` spreads
the factor across datacenters and `QUORUM` is a majority of their *sum*, so the
per-datacenter values are added. `LocalStrategy` and `EverywhereStrategy` have no
replication factor at all.

If it cannot be read, the gate **refuses**. A plausible-looking default of 3 on a cluster
actually running RF=1 waves through the stop that takes the only copy of the metadata
offline — which is exactly the single-node case. `spectrum_server.get_actual_replication_factor()`
returns `"unknown"` for the same reason.

**The ring is read from `nodetool status`**, whose first column is two characters: `U`/`D`
for up or down, then `N`/`L`/`J`/`M` for normal, leaving, joining or moving. A member
counts as an available replica only when it is `UN`. `UJ` has not finished streaming in
and owns no complete range; `UL` is streaming out.

**The arithmetic**, for a host `T`:

```
required          = RF // 2 + 1                        # what Scylla demands at QUORUM
assigned          = min(RF, ring size)                 # replicas a partition actually has
unavailable_after = ring size - (members UN - 1)       # after T stops
replicas_after    = max(0, assigned - unavailable_after)

allowed           = replicas_after >= required
```

`unavailable_after` assumes every member that is not `UN` is a replica of the partition
we care about. `nodetool status` cannot say which nodes hold which partition, so this is
the worst case — and the worst case is the one that actually loses availability. The
cheap version of this check, *"are at least quorum-many nodes up right now"*, passes on
the exact cluster the gate exists for.

Two members are exempt:

* a host that is **not in the ring** — a three-node layout's witness runs no ScyllaDB, so
  draining it costs the ring nothing;
* a host that is **already not `UN`** — stopping it removes nothing that is answering.

### What it decides

| Cluster | RF | Quorum | Stopping one node |
| :--- | ---: | ---: | :--- |
| 3 nodes, all up | 3 | 2 | **allowed** — 2 replicas remain |
| 3 nodes, one already down | 3 | 2 | **refused** — 1 replica would remain |
| 2 nodes, all up | 2 | 2 | **refused** — RF=2 has no spare replica |
| 1 node | 1 | 1 | **refused** — the only replica is the only copy |
| 3 nodes at RF=1 | 1 | 1 | **refused** — each partition lives on exactly one node |
| 5 nodes, all up | 3 | 2 | **allowed** |
| 5 nodes, one already down | 3 | 2 | **refused** — both unavailable members could hold the same range |

A single-node cluster can therefore never enter maintenance mode, and that is correct
rather than a limitation to work around: there is no second copy of anything. `cluster
stop` is the operation for quiescing a whole single-node cluster.

### Where it runs

**Twice.**

1. In the API handler, before the host is claimed and before a single VM is migrated —
   so an evacuation that cannot end in maintenance is never started.
2. In the Catalyst task, immediately before the stop command is issued.

The second is the one that protects quorum. An evacuation can run for an hour, and the
ring the first check saw is not the ring that exists when the services finally stop. If
the second check refuses, the host is put back to `NORMAL`, the lock is released, and the
task fails loudly; the VMs have already moved, and DRS rebalances them.

---

## 3. The cluster maintenance lock

"Only one host in maintenance at a time, to preserve quorum" was a scan of every
`hydra.nodes` row followed by a write. Two hosts entering a second apart both read
*nobody is in maintenance* and both proceeded. A lightweight transaction is confined to
one partition, so a check spread across every node's row and a write to one of them were
never a single operation and never could be.

The exclusion now lives in **one row** that every contender conditions on.

```sql
CREATE TABLE hydra.cluster_locks (
    name          text PRIMARY KEY,
    holder        text,
    holder_token  text,
    reason        text,
    acquired_at_ms bigint
);
```

Added by `helios_schema.py` migration `0002-cluster-locks`. Taken through Daruk's typed
endpoints — `/v1/lock/acquire`, `/v1/lock/renew`, `/v1/lock/release` — so the CQL lives in
the proxy and the compare and the swap are one Paxos round at `SERIAL`.

### Holder, token, TTL

**`holder`** is the hostname, so a refusal can say who is draining. A refused
`IF NOT EXISTS` returns the whole existing row, so the error names the holder and its
reason without a second read.

**`holder_token`** identifies one *acquisition*, not one node, and every release and renew
conditions on it. Matching on `holder` alone lets a stale release from a node's earlier,
expired acquisition drop the lock that same node holds now — the flaw
[daruk.md](./daruk.md) records against the VM migration lock.

**The TTL is 300 seconds**, and it is not optional. A node that dies holding this lock
must not block maintenance for the whole cluster until somebody finds the row and deletes
it: on a cluster that cannot enter maintenance, nobody can replace the hardware that died.

Five minutes is far shorter than a maintenance window, so the lock is renewed:

* by **Vali's evacuation task**, once per migrated VM, for as long as the drain runs;
* by **Mipha's control loop**, every ten seconds, for as long as `hydra.nodes` reports the
  host in `ENTERING_MAINTENANCE`, `IN_MAINTENANCE` or `RECOVERING`.

Mipha renews on behalf of a host it does not speak for, so it reads the row and renews
conditionally on the token it read. If the lock changed hands in between, the renew is
refused rather than extending someone else's lock in the wrong host's name.

`RECOVERING` holds the lock deliberately. A host that has left maintenance but has not
finished resyncing its storage is not a replica anyone should count on yet.

> [!NOTE]
> Renewing rewrites **every** non-key column, `holder_token` included. A column left out
> keeps the original insert's TTL and expires first, after which the row is still alive —
> other cells are live — but no longer renewable or releasable by the host that holds it.
> Verified against Scylla 5.4: a renewed row still refuses a competing `IF NOT EXISTS`
> after the original insert marker has expired.

### Release

`leave` can arrive hours after `enter`, from a different Vali process after a leader
change, so the token from the acquisition is long gone. The row is read, the holder is
checked against the hostname, and the release conditions on the token that was read. If
the lock changed hands between the read and the release, the release is refused.

The lock is released when the host is **fully back** — services started, storage
resynced, `hydra.nodes` back to `NORMAL` — not when `leave` was requested. If the rejoin
fails, the lock stays held and Mipha keeps renewing it while the row says `RECOVERING`;
if the node is abandoned rather than fixed, the TTL frees it.

### Three layers, three jobs

| Layer | Prevents | Fails how |
| :--- | :--- | :--- |
| Quorum gate | stopping a replica the cluster cannot spare | 409, naming RF and the ring |
| Cluster lock | two hosts transitioning at once | 409, naming the holder |
| `/v1/node/maintenance` LWT | one host transitioning twice | 409, naming the host's state |

They are not redundant. The lock can expire while a host sits in maintenance; the quorum
gate still refuses the second host, because the first one's Scylla is `DN` in the ring.

---

## 4. Decommission — leaving for good

`cluster decommission --node <ip>` runs the preflight, prints the ordered sequence, and
**refuses when the destructive step would be unsafe**. It never runs `nodetool
decommission` or `nodetool removenode` itself.

### Why the streaming step is manual

`nodetool decommission` moves every token range the node owns to its replicas. It runs
for as long as that takes — hours on a real dataset — it cannot be undone, and it cannot
be re-run: a decommission interrupted half way leaves a node that is neither in the ring
nor out of it. That is an operator's decision made while watching it, not a side effect of
a CLI verb, and specifically not something a health check that has been failing for thirty
seconds should trigger. From a health check, a dead node and a partitioned one look
identical.

### The sequence

1. **Drain the host.** Maintenance mode migrates its VMs off. The quorum gate refuses this
   when the cluster cannot spare the replica — the same condition that makes step 4
   unsafe, caught before anything moves.
2. **Move its storage.** Nothing to do by hand in the normal case: Purah restores the
   replica count onto surviving nodes on its own, and `valcli storage.list` shows when it
   has finished. Confirm no vdisk still lists this node before continuing — a vdisk that
   does is one that has not been re-replicated yet, and removing the node makes it
   unavailable rather than degraded.
3. **Lower the replication factor** if it no longer fits the smaller ring, then
   `nodetool repair -pr hydra` on every remaining node. `ALTER KEYSPACE` changes the
   strategy only — existing data is not copied to the new replicas until a repair runs, so
   a cluster can report the new RF while a partition still lives on one node. *(Manual.)*
4. **Remove it from the ring.** `nodetool decommission` **on the node**, if it is running.
   If it is gone for good, `nodetool removenode <host-id>` **on a surviving node** instead,
   which rebuilds its ranges from the remaining replicas. The preflight prints whichever
   applies, with the host id — parsed out of `nodetool status` by shape, because `Load`
   occupies one field or two and every column after it shifts. *(Manual.)*
5. **`cluster decommission --node <ip> --finalize`.** Bookkeeping only, and only once the
   node is genuinely out of the ring: it rewrites `/etc/hci/cluster.json` on the surviving
   nodes, renumbers `node_id` (position identifies the witness in a three-node layout),
   and deletes the `hydra.nodes` row.
6. **ZooKeeper ensemble reconfiguration.** *(Manual: a voter that is gone still counts
   toward the ensemble's quorum until it is removed from every remaining host's config and
   they are restarted one at a time.)* There is no storage-side counterpart — a node with no
   vdisks listing it holds nothing the cluster needs.

### What the preflight blocks

* the replication factor cannot be read;
* after removal, `min(RF, remaining)` is below the quorum `RF` demands — lower RF first;
* another ring member is not `UN` — a decommission streams to its replicas, and with a
  replica unavailable the stream cannot complete and the data it carried is lost;
* VMs are still placed on the node;
* it is the only member of the ring — that is `cluster destroy`, not a decommission.

It warns, rather than blocking, when the removal leaves exactly quorum-many replicas: the
cluster survives the removal and will not survive the next failure.

---

## 5. Rejoin — coming back

`cluster rejoin --node <ip>` runs the preflight and prints the sequence; `--finalize`
performs the bookkeeping appropriate to the state it observes, so it is idempotent and
can be run at both ends of the sequence.

The dangerous part of a rejoin is not the ring operation. It is the data still on disk.

> [!WARNING]
> A node that was decommissioned and is then started again with its old commitlog and
> sstables either refuses to start or **re-introduces rows that were deleted while it was
> away**: its tombstones are older than `gc_grace` and its data is not, and Scylla cannot
> tell the difference. `/var/lib/hci/hydra/data` must be wiped before a decommissioned
> node rejoins. The preflight checks for it and blocks.

1. Confirm it is meant to come back as the same node; wipe its ScyllaDB data if it was
   decommissioned.
2. `cluster rejoin --node <ip> --finalize` — restores its `/etc/hci/cluster.json` entry on
   every node, so the seed list is right before it starts.
3. On the node: `systemctl start zookeeper hydra-db`. It bootstraps by streaming from the
   seeds. Watch `cluster ring` until it reports `UN`; a node sitting at `UJ` is still
   streaming, not broken.
4. Raise the replication factor back if it was lowered, then `nodetool repair -pr hydra`
   on every node. Bootstrapping streams the ranges the new node now owns; it does not
   reconcile what the survivors wrote while it was gone. *(Manual.)*
5. Storage needs nothing: Purah places new replicas onto the returned node as vdisks come
   to need them, and its old extent groups — if any survived — are immutable, so they are
   either correct or swept as orphans.
6. `cluster rejoin --node <ip> --finalize` again — registers it in `hydra.nodes` as
   `NORMAL`, but **only once it is `UN` in the ring**. Registering it while it is still
   bootstrapping hands it VMs it cannot run.

A node that was merely **down**, and never left the ring, does not rejoin: it is still a
member. Start `hydra-db`, let it catch up, repair. Mipha already does this half — see
below.

---

## 6. What Mipha does, and what it does not

Mipha's HA loop already marks a crashed host `DOWN`, fences it, fails its VMs over, and
runs the rejoin-and-resync sequence when it comes back.

It now also:

* **renews the cluster maintenance lock** for whichever host is transitioning, which is
  what lets the TTL stay at five minutes;
* **reports the ring state of a host it has just failed over** — whether it is still a
  ring member, what `nodetool` calls it, how many members are up, and the command that
  would detach it.

It does **not** detach anything. Automatic ring detach is the one piece of the Nutanix
behaviour deliberately not reproduced: `removenode` is irreversible, and thirty seconds of
failed health checks is a network partition as often as it is a dead node.

---

## 7. Related

* [hydra.md](./hydra.md) — the metadata layer and its replication
* [daruk.md](./daruk.md) — the typed compare-and-swap endpoints, including the lock
* [mipha.md](./mipha.md) — the HA coordinator
* [cluster.md](./cluster.md) — the `cluster` CLI
* [cluster_state.md](./cluster_state.md) — the ZooKeeper-backed membership, which is a
  third and separate thing again
