# Compaction, and the dedup estimator

The decision and the alternatives it beat are **D-32** in [decisions.md](./decisions.md). This
document is the mechanism: what a pass does, in what order, why each step is safe to stop after,
what it refuses to touch, and what it does not do. Both passes are in
`sidon/src/purah/` (`compact.rs`, `dedup.rs`, `occupancy.rs`) and are operator-invoked. Nothing
runs either on a timer.

```bash
valcli storage.compact                       # plan: what would be copied, and what it would free
valcli storage.compact --apply               # do it, within the limits below
valcli storage.dedup.estimate [--sample F]   # how much dedup would share beyond clone sharing
```

## 1. Why compaction exists

Overwrites are redirect-on-write ([data-path.md](./data-path.md) section 2). The new bytes go
into a new extent and the old extent becomes garbage *inside a sealed group*. The sweep
reclaims a group only when **nothing** points into it, so a 4 MiB group that is three extents
dead and one alive keeps all four. A guest that rewrites a small working set over and over
leaves a trail of such groups, and today the only remedy is to delete the vdisk.

Compaction copies the live extents of mostly-dead groups into new groups, repoints the map,
and lets the sweep take the old groups. It is also the prerequisite D-23's addendum names for
dedup: without it, a dedup ratio overstates what is returned to the pool.

## 2. One batch, step by step

A **batch** turns one or more *source groups* into one new group. Sources are never split: a
group is moved whole or not at all, because a group is reclaimed only when *all* of it is dead
and half a move frees nothing. Sources are packed into one batch only if they agree on
container and replica set, since a new group has to be placed and replicated the way its
extents were.

| # | Step | If the process stops after it |
|---|------|-------------------------------|
| 0 | **Scan.** One pass over the block map (and the extent map if any row names an extent) gives, per sealed group this node created, which extents are live and which rows point at each. Candidates are groups below the threshold (default 50% live). Read-only. | Nothing changed. |
| 1 | **Read and verify** every live extent of every source: the group against its seal hash, each extent's footer (CRC, vdisk identity, index) against **every** row that points at it. A source that fails is left out; the pass is not the repair tool and does not launder damage into a new, clean-looking file. | Nothing changed. |
| 2 | **Stage** the new group as `<id>.eg.moving`, which no directory scan treats as a group. fsync, drop the cache, read it back from the disk, require the same hash. Same mechanics as `storage.move`. | A stale temporary, removed by the sweep's existing temporary reaper. |
| 3 | **Replicate** to the replica nodes of the vdisks that point at the extents, extent by extent at the same offsets (the drain's own `EGROUP_PUT`), then read the **whole group back** from each and compare. Write-all: one refusal or one mismatch abandons the batch and removes the temporary. | Replica copies of an id nothing references (the same orphan an abandoned drain leaves). |
| 4 | **Register** the group in Hydra, already `sealed`, with its hash (`egroup-create`, `IF NOT EXISTS`). Before the rename, so a crash between leaves a *row* the sweep reclaims rather than a *file* nothing names. | A sealed, unreferenced group row with no file: young, then swept after the grace. |
| 5 | **Publish** by rename and fsync of the directory. Re-hash the published file and read each extent through its footer. | A complete, registered, unreferenced group: swept like any orphan. |
| 6 | **Hold** the drains of every attached vdisk that points here (section 3) and check that each one's in-memory map still says what the scan saw. | Same. |
| 7 | **Repoint**, one compare-and-swap per row. A row of the block map: `block-map-repoint`, conditional on group, offset and length. An extent named through the middle level: **one** `extent-repoint` row, whoever points at it. After each applied swap the attached vdisk's in-memory entry is corrected. | Some rows moved, the rest not. See below. |

**The old group is never touched.** After step 7 begins, each row *individually* names a
location that holds exactly the bytes it should: the old group is intact and the new one is
verified. So a stop between any two statements leaves a map that reads correctly, and nothing
needs recovering. Re-running re-derives the plan from the map: rows already moved no longer
point at the old group, and a group left half-moved is simply a smaller candidate. There is no
journal and no state to resume from, which is why there is nothing to get out of step.

**Idempotence is convergence, not a flag.** A new group is all live, so it is never a candidate.
A group left partly moved has fewer live extents each run. The Rust tests run random maps
(shared extents, snapshots, an attached writable vdisk) to a fixed point and check that every
row reads the same bytes after every pass.

**An overwrite wins.** The swap's condition is "the row still points where the scan saw it". If
a drain has committed a newer extent for that index, the swap is refused, the newer row is left
alone, and the pass counts a `lost_race`. The copy of the old extent it made is dead on arrival
and the sweep removes it. Compaction can therefore never un-write a guest's write through the
condition.

## 3. The interleaving the condition cannot see, and how it is excluded

[metadata.md](./metadata.md) section 3 says block-map rows are plain writes because there is
**one writer per partition**, and that the moment anyone proposes a second, the price is Paxos
per row. Compaction is that second writer. A compare-and-swap protects against a drain that has
*already written* a row. It does not protect against a drain that has *read* a row, decided to
write it, and writes it a millisecond after the swap lands: the drain's plain write carries a
later timestamp, and the swap's effect is overwritten with data that was correct when the drain
read it and is stale now. The same interleaving the other way loses a guest write. The condition
cannot see it, so the pass excludes it instead:

* A row of an **immutable** vdisk (snapshot, image) is rewritten freely. Nothing writes those
  rows.
* A row of a **writable** vdisk is rewritten only while this node **owns the vdisk and has it
  attached**, and only inside a *hold*: the vdisk's own drain gate, the flag every drain sets.
  While it is held no drain is running and none can start (`kick`, the background drain and
  `drain_all` all see "a drain is running" and stand aside or wait). Guest writes are **not**
  held: they go to the journal as ever, and a writer at the hard ceiling waits for the gate,
  which is the length of a handful of metadata statements. The hold is released when the batch
  ends, on every path.
* A group that **any writable vdisk not attached here** still points at is skipped whole, and
  the plan says which vdisk. There is no way from this node to exclude that vdisk's drain, and
  moving "most of a group" frees nothing.
* A vdisk whose class is `forming` or `rolling_back` (a snapshot, clone or rollback in progress)
  blocks the group: its rows are being built or rewritten by someone else.
* A group named by a **drain on this node** (`drain_groups()`: the open group and any group a
  running drain created) is skipped, and so is any group younger than the sweep's grace, for
  the sweep's own reason: a drain writes a group before the rows that name its extents, so in
  that window extents that look dead are about to be live.

Before the first swap of a batch the pass checks, under the hold, that the vdisk's in-memory
map holds the location the scan saw for every extent it will move. A drain that finished since
the scan makes them differ; the batch is abandoned with no row moved and tried again next run.

## 4. Shared extents, snapshots and clones

A clone's rows are a copy of its parent's, so a shared extent is **one** stored extent with
several referrers, and it moves **once**: one copy in the new group, then each referrer's row is
swapped. Live bytes count a shared extent once. Every footer is verified against every referrer
(a clone reads an extent under the *parent's* identity, D-18), so a row that would not read
today is found before anything is copied.

Missing a referrer is safe. A snapshot taken between the scan and the swap copies either the
old or the new location; if the old one, its rows keep the old group referenced, the sweep
leaves the old group alone, and the cost is space, not data. This is the property that makes
extent-level garbage knowledge allowed to be incomplete: the pass never frees anything, and the
sweep's own mark phase still decides what is referenced.

## 5. Limits, pace, and the status line

| Limit | Default | Flag |
|-------|---------|------|
| a group is a candidate below this live fraction | 50% | `--threshold` |
| source groups per pass | 8 | `--max-groups` |
| live bytes copied per pass | 128 MiB | `--max-bytes` |
| bytes per second moved (read + local write + each replica) | 16 MiB/s | `--rate` |
| wall clock; no new batch starts after it | 40 s | `--seconds` |

The pass says which limit stopped it (`stopped_by`: `max_groups`, `max_bytes`, `time`,
`error`) and a re-run continues, because it recomputes from the map. The 40 seconds is short of
the 60 the control client waits, so the answer always arrives. The rate is paid *after* each
batch, with the token bucket the replication sender uses. The pass runs under the sweep's lock,
so the sweep cannot observe a group between registration and its first row; the lock is held
for at most the time budget plus one batch.

The first line of each node's output is the status line, for example:

```
n1: compaction plan: 3 group(s) below 50% live; this pass would copy 6291456 byte(s) into new
    group(s) and leave 18874368 byte(s) for the sweep; nothing was changed
n1: compaction: 2 batch(es) done, 7 row(s) repointed, 1 lost to an overwrite, 0 failed; the
    old group(s) are left for the sweep (12582912 byte(s) here once it has run)
```

A plan changes nothing: it takes no hold, makes no peer call, and issues no write. The next run
shows the previous run's status line as `previous_run`.

## 6. What it does not do

* **It does not reclaim space on replicas, and it adds some there.** A replica's copy of a
  group lives in its replica store, and nothing in Sidon removes one today, whether the group
  was swept, compacted or deleted with its vdisk. Compaction puts the live extents into a new
  group on every replica and frees nothing there. The plan prints `added_on_replicas` beside
  `freed_here_after_sweep` so that the net is visible, and until a replica-side reclaim exists
  (an opcode to drop a replica's copy, sent by the node that swept the group) a cluster at
  ftt>=1 trades space on the creator for space on its replicas. This is the largest limit and
  is why the command is opt-in.
* **It does not touch groups it cannot exclude a drain from** (section 3): writable vdisks not
  attached on this node, in particular a clone running on another node. Run it where the group
  lives, with the VM there; a node compacts only the groups it created.
* **It does not repair.** A source that fails its hash or a footer is left alone.
* **It does not move extents within a disk's tier policy beyond placement.** The new group goes
  where a new group goes: the container's preferred tier if the node has one, else most free.
* **It does not run itself.** D-22's reason applies unchanged: nobody has yet watched what it
  does on this cluster's data.

## 7. The dedup estimator

`valcli storage.dedup.estimate` is step one of D-23's addendum and nothing else: a Purah pass
that hashes sealed extents and reports, per container, what dedup *would* save beyond what
clone-from-image already saves. **It writes nothing but its report.** It is handed a reader
(`Rows`) and an extent store it only reads; it changes no extent id, adds no setting, builds no
index. A test asserts the production code contains no write.

What it reports, per container:

| Field | Meaning |
|-------|---------|
| `stored_extents`, `stored_bytes` | distinct (group, offset) locations the map points at, exact |
| `logical_bytes` | stored bytes times the rows pointing at them |
| `shared_by_clone_bytes` | `logical - stored`: sharing clones and snapshots already give for free |
| `would_share_bytes` | stored extents whose content another stored extent holds, redundant copies only |
| `would_share_bytes_excluding_zero` | the same without all-zero extents, which a sparse map handles |
| `fraction_of_stored` | `would_share / stored` |

Extents are hashed with SHA-256 of the extent **as the guest wrote it** (decoded), so a
compressed and an uncompressed copy of the same bytes match. Each node reads the groups it
created. With `digests` (which `valcli` asks for) a node also returns the first eight bytes of
each sampled hash, and `valcli` merges them across nodes to find duplicates that straddle two.

**Sampling.** Stored extents are ordered by a hash of their location and processed up to the
requested fraction, so any prefix is a uniform sample and a pass cut short by its time budget
(`--seconds`) is still a sample whose covered fraction is reported. Within a sample a duplicate
is seen only if *both* copies were sampled, so a sample **undercounts low-multiplicity
duplication** (content that exists twice) and is fair for **high-multiplicity** duplication
(a thousand VMs applying one patch). The scaled figure divides by the covered fraction, is
capped at what is stored, and is labelled an estimate. At `--sample 1` every sealed extent is
read and the figure is exact. The default is 10%, at 64 MiB/s.

**Reading the answer.** D-23's working threshold is roughly 10-15% beyond clone sharing. Below
it the estimate argues against building anything; above it, the next step is compaction at
scale (this document) and only then the background post-process pass. A figure is not a saving:
it is bytes that *could* be shared, and space returns only through compaction, subject to
section 6. The estimate says nothing about sub-extent duplicates (4 KiB blocks at other
alignments), which at 1 MiB granularity it cannot see.

## 8. Where it is

| Piece | File |
|-------|------|
| Which extents of which groups are live, through both map levels, failing closed | `sidon/src/purah/occupancy.rs` |
| Analysis, packing, one batch, the pass, the status line | `sidon/src/purah/compact.rs` |
| Failure-injection tests (stop at every step, concurrent overwrite, shared extents, replica refusal, random convergence) | `sidon/src/purah/compact/tests.rs` |
| The estimator and its tests | `sidon/src/purah/dedup.rs` |
| The model of Hydra and the fake daemon the tests use | `sidon/src/purah/testkit.rs` |
| The drain hold, the in-memory repoint, the group set a drain is making | `sidon/src/vdisk.rs` (`try_hold_drains`, `repoint_extent`, `drain_groups`) |
| Building a group out of bytes, with the move's verification | `sidon/src/extent/placement.rs` (`stage_new`) |
| The two compare-and-swaps | `daruk.py` (`/v1/dfs/block-map-repoint`, `/v1/dfs/extent-repoint`) |
| Control socket, peers and attached vdisks | `sidon/src/control.rs` (`purah-compact`, `purah-dedup`) |
| CLI, spark allow-list, Python client | `valcli.py`, `spark_daemon_decoded.py`, `helios_sidon.py` |
| Wiring and rule tests | `test_compaction.py` |

No migration was needed and none was added.
