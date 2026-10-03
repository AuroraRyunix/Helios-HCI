# Metadata

The map in Hydra, the Daruk endpoints that mutate it, the exactly-once drain, and the
arithmetic showing Scylla never becomes the bottleneck. Guest bytes never appear in any
of this — the map says where data is, never what it is.

## 1. Tables (sketch — final shapes land as `helios_schema.py` migrations at build time)

```
dfs_vdisks
  vdisk_id uuid PRIMARY KEY,
  container text,               -- joins hydra.storage_containers; ftt+1 = RF
  size_bytes bigint,
  class text,                   -- 'rw' | 'immutable' (images)
  owner text, epoch bigint,     -- the CAS pair; see ownership.md
  drain_seq bigint,             -- the exactly-once counter; see §4
  journal_chunks list<uuid>,
  parent_vdisk uuid             -- snapshot chain; null for roots

dfs_block_map
  vdisk_id uuid, extent_index bigint,
  egroup_id uuid, egroup_offset int, length int, epoch bigint,
  PRIMARY KEY ((vdisk_id), extent_index)

dfs_egroups
  egroup_id uuid PRIMARY KEY,
  state text,                   -- open | sealed | dead
  replicas list<frozen<tuple<text, text>>>,   -- (node, path)
  size int, seal_hash text,
  vdisk_hint uuid               -- for Purah's scan locality; not authoritative
```

Design points that are decisions, not defaults:

- **Block map partitioned by vdisk, clustered by extent index.** The only hot reader is
  the vdisk's owner, whose lookups and drain commits become single-partition operations
  answered by clustering order — the same shape that fixed the metrics and dagur_runs
  scans in the console. A 1 TiB vdisk is ~1M rows in one partition: large but bounded,
  read once at open into the owner's cache, then maintained incrementally.
- **Map rows carry the writing epoch.** Not for the hot path — for takeover
  reconciliation and for Purah, which can recognise and discard rows a deposed drain
  wrote after losing a race it had not yet noticed (§4 makes this window tiny; the
  column makes it auditable).
- **Timestamps are `bigint` epoch-ms, ids are explicit.** The Daruk serializer now
  handles driver types, but the two prior tables (`cluster_locks`,
  `urbosa_transit_pool`) set the convention and consistency beats cleverness.
- **No inline refcounts anywhere** — GC is Purah's mark-sweep
  ([data-path.md §5](./data-path.md)). The schema must therefore never grow a
  `refcount` column; if one appears in review, the reviewer's job is to say no.

## 2. Daruk endpoints (extending `LWT_OPS`, the existing mechanism — never a second one)

| Endpoint | Statement shape | Consistency |
|---|---|---|
| `/v1/dfs/vdisk-create` | INSERT … IF NOT EXISTS (explicit columns — the `INSERT JSON IF NOT EXISTS` trap is documented in daruk_technical.md) | LWT |
| `/v1/dfs/claim` | UPDATE owner, epoch=epoch+1 IF owner=? AND epoch=? | LWT |
| `/v1/dfs/drain-commit` | UPDATE drain_seq=? IF drain_seq=? AND epoch=? | LWT |
| `/v1/dfs/egroup-state` | UPDATE state IF state=? (open→sealed, open→dead, sealed→dead) | LWT |
| block-map row batches | plain writes at QUORUM | see §3 |

Refused CAS returns `{"applied": false, "current": {...}}` at 200 — the established
contract; a lost claim names the actual owner.

## 3. Why block-map rows do not need LWT

Every LWT is a Paxos round; the block map takes thousands of writes per drain and would
melt. They are safe as plain QUORUM writes because of two facts that hold by
construction:

1. **Within an epoch there is one writer** — the owner serialises its own drains, so
   last-write-wins per row is simply "the write" per row.
2. **Across epochs, the drain-commit gate (§4) rejects the loser** before its batch is
   considered applied, and rows a zombie managed to land carry its stale epoch for
   reconciliation.

Single-writer-per-partition is the one arrangement under which LWW is not a euphemism
for data loss. The moment anyone proposes a second concurrent writer of a vdisk's map,
this section is the document that says the price is Paxos per row.

## 4. Exactly-once drain across ownership transfer

The named hard problem: a drain in flight when ownership moves must not commit twice,
half-commit, or commit after the new owner has replayed the same journal records.

**Chosen: the `drain_seq` CAS.** A drain batch is prepared (egroup bytes durable — data
before metadata), then committed by `drain-commit`: one LWT conditioned on both the
expected `drain_seq` *and* the caller's epoch. Outcomes:

- Old owner commits before the takeover's CAS: fine — the new owner reads a map that
  already includes the batch, and replays only journal records past the advanced
  watermark.
- Old owner tries to commit after: the epoch condition fails, `applied:false`, batch
  discarded. Its egroup bytes are orphans; Purah sweeps them. The journal records remain
  undrained from the map's perspective and the new owner drains them itself.
- Crash mid-prepare: nothing referenced anything; orphans and replay, as always.

Cost: **one LWT per batch** — thousands of guest writes amortise one Paxos round.

**Rejected: epoch-stamped rows with owner-side reconciliation on takeover** (new owner
scans for and repairs stale-epoch rows). Rejected because it turns takeover — the path
that runs during failures, under time pressure, least often exercised — into a repair
algorithm, where the chosen design makes takeover a reader. Correctness work belongs on
the always-exercised path; the epoch column stays as audit, not as mechanism.

## 5. Load arithmetic

The question "does the map melt Scylla" answered with numbers rather than adjectives.
Assume 1,000 active vdisks, an aggressive 500 sustained write IOPS each:

- **Journal path: zero metadata operations.** 500k IOPS touch Hydra not at all.
- **Drains:** 32 MiB high-water per vdisk → at ~2 MiB/s sustained per disk, a drain
  roughly every 16 s → ~60 batches/s cluster-wide → **60 LWTs/s** and perhaps 50k plain
  row upserts/s across the cluster, batched. Scylla on these nodes does an order of
  magnitude more before noticing; and it scales with the node count, which the load does
  too.
- **Reads:** the owner caches its partition; steady-state map reads are cache misses and
  takeovers only.

The system that dies at this layer dies because someone put a metadata op on the
per-write path. The design's one inviolable performance rule is that acknowledgement
never waits on Hydra.

## 6. Snapshots and clones (schema-ready now, built later)

Snapshot = freeze: the vdisk becomes an immutable parent, a new child vdisk with
`parent_vdisk` set takes the writes, reads walk the chain (child overlay → child map →
parent map → …), bounded by chain length and collapsed by Purah when chains grow long.
Clone-from-image is the same operation against a `class='immutable'` parent.

Nothing needs retrofitting for this later **because of two v1 decisions**: sealed
egroups are immutable (a parent's data cannot be scribbled on), and GC is mark-sweep
across *all* generations (a parent's egroups stay referenced by children with no
refcount bookkeeping to have gotten wrong). Those two lines are why snapshot support is
a feature and not a migration.

This is also what finally closes saga's honest caveat: with snapshots, guest data
backup becomes a map-copy plus extent export — backup of what the VMs actually contain,
not just of the metadata that finds them.

## 7. Schema change discipline

DFS tables arrive as ordered `helios_schema.py` migrations like everything else — with
one addition: any migration touching `dfs_*` must state in its description which
invariant from [invariants.md](./invariants.md) it serves or preserves. The checksum
mechanism already refuses edited-after-shipping migrations; this extends the same
discipline from "what changed" to "why it is allowed to".

## 8. Per-extent-group access data

`hydra.dfs_egroup_access` records how often each extent group is read and written and when
it was last touched. It is the Helios counterpart of Nutanix's
`medusa_extentgroupaccessdatamap`, and it exists for the same reason theirs does: tiering
has to know what is hot, and a curator has to have something to rank its work by.

```
dfs_egroup_access
  egroup_id text, node text,
  reads bigint, writes bigint,
  bytes_read bigint, bytes_written bigint,
  last_read_ms bigint, last_write_ms bigint,
  since_ms bigint, updated_at_ms bigint,
  PRIMARY KEY ((egroup_id), node)
```

Four design points, each of which is a decision:

- **Keyed per observer, `((egroup_id), node)`.** One row per node that saw the accesses,
  which makes every row single-writer. The flush writes *absolute* totals rather than
  increments, so a shared row would have each node clobber the others on every flush and
  an extent group read on three nodes would read as the heat seen by whichever flushed
  last. Reading one group's temperature stays a single-partition query, and the ranking
  pass sums the rows. It is also the shape the since-removed `dfs_egroup_replicas` had for
  facts about an extent group, per node.

- **Absolute totals, never increments and never a counter column.** CQL counters are not
  idempotent under retry — a timed-out write may be applied twice — and a read-modify-write
  would put a Hydra read in front of a Hydra write for every flush. Writing the whole total
  makes the flush idempotent, which is what lets it be fired and forgotten: a flush that is
  lost changes nothing, and one applied twice changes nothing. This is the same objection
  that forbids a refcount column (§1, D-8), applied to a statistic.

- **`since_ms` makes the totals readable.** They are not lifetime counts. They count from
  when the observing daemon opened its window, which is when it started. Heat is therefore
  a *rate* over `updated_at_ms − since_ms`, which is what a tiering decision wants anyway,
  and a restart resets the window visibly instead of silently losing counts.

- **It holds no references.** Nothing in this table is a pointer to data, so Purah's mark
  phase does not read it and nothing here can keep a dead extent group alive (I-7) or hide
  a live one (I-3). A row whose extent group has been reclaimed is a stale label, not a
  dangling pointer; the sweep deletes the partition when it deletes the group, and a
  leftover row is ignored by the ranking pass because no inventory lists it.

### What a crash loses

**The counters are approximate, and that is the trade rather than a defect.** Sidon
accumulates them in memory and a background thread flushes them to Hydra every 60 seconds
by default (`SIDON_ACCESS_FLUSH`; `0` turns the whole tally off, counters included). A
crash therefore loses every access counted since the last flush and reopens the window, so
the surviving row under-states the group by up to one interval and the new window starts
from zero.

That is acceptable for a specific reason, and the reason is the boundary of what this data
may ever be used for: **this data may decide where a copy of data goes, never whether it
exists.** The bytes are safe either way, so being wrong about placement costs a misplaced
extent group and a later migration, never a byte.
The moment anything proposes using access data to decide whether a copy *exists* — a
reclamation input, a replica count, an eviction — this paragraph is the one that says no,
because an approximate counter cannot carry a durability decision.

The same reasoning sets where recording happens. A read records the access after the bytes
are in hand: a hash lookup and four integer adds under a mutex that is never held across a
syscall. There is no metadata round trip on the read path, and there cannot be one — that
is the inviolable rule from §5 in the direction nobody thinks to check it.

Writes are counted on the drain, not on the guest's write. An extent group never sees a
guest write at all; the write reaches the journal and is acknowledged there. So a group's
write count is a count of drained extents landing in it, which is the honest meaning and
also the useful one: it identifies the groups still being appended to, and those are
exactly the ones a tiering pass must not move.

### Reading it

`purah-heat` on the control socket, `valcli storage.heat [N]` for an operator. It flushes
and then ranks, so the answer describes the node now rather than as of the last tick, and
it reports the window and the raw counters beside the score so the arithmetic can be
redone by hand.

The score is deliberately arithmetic rather than a tuned decay:

```
rate = accesses * 3_600_000 / window_ms
heat = rate / (1 + idle_hours)
```

A ranking used to argue for moving data has to be answerable when it says something
surprising, and "why does it think that is cold" must be answerable from the row.

Groups with **no** row are reported separately from cold ones. A group with a row and a low
score has been measured and found cold; a group with no row has not been measured. Both may
be fair to spill, and only `dropped` — the number of extent groups whose first access
arrived while the in-memory tally was at capacity — says which the operator is looking at.

**Nothing moves data on the strength of this.** The migration half of tiering is designed in
[multi_disk.md](./multi_disk.md) and not built; this is the input it was missing.
