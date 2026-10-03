# The extent ID map: staged rollout

The decision and its reasoning are **D-23** in [decisions.md](./decisions.md). This document
is the operator's half: what is built, in what order it must be deployed, and what you must
see between one step and the next before taking the next. The order is the safety property,
so it is written down as a procedure rather than left as a remark.

## What it is

Helios's block map has two levels: `hydra.dfs_block_map` points `(vdisk, extent_index)`
straight at an extent group. Nutanix has three. The middle level is a row per *extent* in
`hydra.dfs_extent_id_map` saying which group, offset and length it is, and a block-map row
may then name the extent (`dfs_block_map.extent_id`) instead of the group (`egroup_id`). A
row fills one column or the other, never both. **Null `extent_id` is the two-level path**,
and every vdisk that exists today keeps it indefinitely: nothing in this work rewrites an
existing block-map row.

## Why the order matters

Purah's mark phase decides which extent groups are live by reading the block map, and it
runs on a timer whether or not anyone has opted in to anything. A group it does not mark is a
group it may delete. The first block-map row that names an extent instead of a group would
make a sweep that only reads `egroup_id` see every group behind it as unreferenced, and the
two-scan grace would delay deleting live data by ten minutes, not prevent it. A flag on the
*read* path cannot help, because the sweep is not on the read path.

So the mark phase learns both levels first, and nothing may write an extent id until every
node's Purah has been running that version through a full sweep cycle.

## What is built

| Stage | State | What |
|-------|-------|------|
| 1 | **built** | Purah's mark phase traverses both levels. `sidon/src/extent_id_map.rs`. |
| 2 | **built, dormant** | Migrations `0020-dfs-extent-id-map` (the table) and `0021-dfs-block-map-extent-id` (the column); a vdisk whose block-map rows name extents resolves them on open (`sidon/src/extent_resolve.rs`). Nothing writes an extent id. |
| 3 | **designed, not built** | Writing extent ids and sharing extents between vdisks. See below. |

### Stage 1 -- the one to roll out first, alone

This is the stage to deploy and let run before anything else. It changes no data and no
schema, and with no row naming an extent it issues the statement it always did.

What it does differently, precisely:

* It reads `hydra.schema_migrations` once per sweep. If `0021-dfs-block-map-extent-id` is not
  recorded it scans `SELECT egroup_id FROM hydra.dfs_block_map`, exactly as before, and never
  names a column the cluster may not have. The ledger is read rather than the error from a
  failed `SELECT`, so the reclamation decision never depends on how ScyllaDB words a
  complaint.
* If the column exists it also reads `extent_id`. If any row names an extent it reads the
  extent map and marks the group each one points at.
* **It fails closed.** An extent a row names that the extent map does not hold, an extent
  whose own row names no group, an extent map that cannot be read, or extent names with
  `0020` unrecorded all *abort the sweep*. A sweep that errors reclaims nothing. The
  alternative, skipping what it cannot follow, leaves the groups behind it unmarked.

Deploy it the usual way and **restart sidon on each node**: the deploy installs the binary
and does not restart the daemon, so until you do, the old mark phase is still the one
running.

**Between stage 1 and anything else, observe, on every node:**

1. The sidon unit's log contains no `purah: sweep failed` line for at least **two full sweep
   intervals after the restart plus the grace**, which is 5 minutes and 10 minutes by default
   (`SIDON_PURAH_INTERVAL`, `SIDON_PURAH_GRACE`). That is the "full sweep cycle": long enough
   for a group seen unreferenced on the first scan to be reconsidered on a later one under the
   new code, so a regression in marking would have been able to act.
2. `valcli storage.cleanup_orphaned` reports `referenced` equal to what it reported before the
   restart, give or take vdisks created or deleted in the meantime. A drop in referenced
   groups with no deletion behind it is the signature of a mark phase that has stopped seeing
   something.
3. The set of groups it names as reclaimable is unchanged in kind: whatever was orphaned
   before is still the only thing listed.

If any of these is wrong, **stop**. Nothing later in this document is safe to do.

### Stage 2 -- the table and the dormant read path

Only after stage 1 has soaked. Applying the migrations is harmless in isolation: the table is
empty, the column is null on every row, and the read path is shape-driven.

*Shape-driven, not flagged.* `Vdisk::load_map` issues the same single `SELECT` as ever. Only
if a returned row has no `egroup_id` does it go to the extent map, and no row lacks one. A
flag would have to agree with the rows and a reader that disagreed with them would return
the wrong bytes; a row that says which level it uses cannot disagree with itself. What stays
off is the *writer*: nothing in this tree writes `extent_id`, so no vdisk is on the three-level
path and none can become so by accident.

**Observe after applying 0020 and 0021:**

1. Both ids appear in `hydra.schema_migrations`, and the next sweep still completes without
   `purah: sweep failed`. This is the first sweep that reads `extent_id`; it must finish.
2. `SELECT count(*) FROM hydra.dfs_extent_id_map` is `0`, and no `dfs_block_map` row has a
   non-null `extent_id`. If either is not true, something is writing that should not be.
3. Existing vdisks attach and read as before; there is no change to observe, which is the
   point.

Roll the binary and the migrations in either order -- stage 1 reads the ledger and tolerates
both -- but not the stage 3 writers (which do not exist) before both.

### Stage 3 -- designed, not built

Stage 3 is *not* a flag flip; it is new writers, and each is a place where the two shapes
can disagree. Before any of it, a finding that changes what stage 3 is for:

**Clones already share at extent granularity.** A clone's rows are a copy of its parent's
rows, one per 1 MiB extent index, and a write to either redirects into new storage and
repoints only that vdisk's own row. So "clone divergence one extent at a time" -- the benefit
D-23 originally listed -- is what the two-level map already does. What the middle level adds is
different, and smaller than the ADR first claimed:

* **Relocation costs one row.** Moving, compacting or erasure-coding an extent group today
  means finding every block-map row in every vdisk that points at it, which is a full scan of
  the map and a rewrite under each owner's epoch. With the middle level it is one extent row.
  That is the real prize. Compaction (D-32, [compaction.md](./compaction.md)) uses it: an extent
  named through the middle level is moved with one `extent-repoint` swap, whoever points at it.
* **An extent has a name**, which is a precondition for dedup (see the addendum to D-23).

Writers that would have to change, with the hazard each carries:

* **Drain commit** (`vdisk.rs`, `meta.rs::block_map_batches`). It writes extent rows first,
  then the block-map row, so the usual data-before-metadata ordering holds one level up. It
  must also write `extent_id = null` when it repoints a row a vdisk had previously named by
  extent: an `INSERT` lists only the columns it sets, so otherwise the old name survives on a
  row that now carries an `egroup_id`. Marking tolerates it (it marks both, which leaks and
  never loses) but it is a leak and a trap. That write cannot be added until `0021` is
  guaranteed on every node, which is why it is a stage 3 change.
* **`derive_child`** (`control.rs`). Today it fails with "block map row without egroup_id" on
  a row naming an extent. That is the safe direction and is left alone until stage 3 teaches
  it to copy `extent_id`.
* **Vdisk delete.** Deleting a vdisk's partition is unchanged, but it orphans extent rows.
  Purah then needs a second reclamation pass over `dfs_extent_id_map` with the same two-scan
  rule, marking from the block map. Until it exists, extent rows leak -- bytes of metadata, not
  data, and the reason stage 3 cannot start before it.
* **The per-vdisk opt-in.** Stage 2 is shape-driven and needs no switch. The *writer* needs
  one, per vdisk, and it needs a home: a column on `hydra.dfs_vdisks`. That is a migration
  beyond the two assigned (`0020`, `0021`), so it is deliberately not invented here; stage 3
  starts by being given an id.

None of this should start until stages 1 and 2 have run clean on the cluster.

## Where the code is

| Piece | File |
|-------|------|
| Mark phase through both levels, and its property tests | `sidon/src/extent_id_map.rs` |
| Reading a vdisk whose rows name extents | `sidon/src/extent_resolve.rs` |
| The one-line call into the mark phase | `Purah::referenced_egroups` in `sidon/src/purah.rs` |
| The two migrations | `helios_schema.py` (`0020`, `0021`) |
| Guards that keep the writer absent and the ids in agreement | `test_egroup_access_data.py` |
