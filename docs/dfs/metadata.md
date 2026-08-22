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
