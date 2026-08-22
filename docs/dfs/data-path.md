# The data path

The write path, the read path, and the background machinery. Everything here serves the
invariants file; where a mechanism exists because of a specific failure, the failure is
named.

## 1. Why there is a journal at all

The obvious design — redirect every guest write into an extent group directly — dies on
arithmetic. Extents are 1 MiB; guests write 4 KiB. Redirect-on-write at extent
granularity means a 1 MiB read-modify-write per 4 KiB guest write: 256× amplification.
Overwrite-in-place at sub-extent granularity avoids that but surrenders the crash-safety
and repair simplicity that immutable sealed egroups buy — every open egroup becomes a
mutable shared file whose replicas can diverge in the middle.

So the journal is not a performance optimisation to add later. It is the mechanism that
makes small writes both fast *and* provably recoverable, and every simpler design
collapses back into it. (This is the Nutanix OpLog, reached by the same forces.)

## 2. The write path

```
guest write (offset, len, bytes)
  1. owner Sidon builds a journal record:
       {vdisk_uuid, epoch, seq, offset, len, payload, crc32c(record)}
  2. append to the vdisk's active journal chunk on ALL RF replicas, fsync each
  3. all replicas acked  →  insert into the in-memory overlay index  →  ack the guest
```

- **Write-all, not quorum.** Every journal replica has every acknowledged record. This
  costs availability — one journal replica down pauses the vdisk until the journal is
  re-replicated to a healthy node (seconds, it is ≤64 MiB) — and buys the property the
  ownership proof rests on: *fencing any single replica is sufficient to stop the old
  owner acknowledging anything*, and *reading any single replica after fencing yields
  every acknowledged write*. Quorum journals make both statements false and the takeover
  protocol triple the size. DRBD protocol C users already live with exactly this
  availability trade, so it is not a regression.
- **Records ≤ 1 MiB.** Larger guest writes split into multiple records terminated by a
  commit marker; replay applies only marker-complete groups (invariant I-2's atomicity
  clause).
- **Sequence numbers** are per-vdisk, assigned by the owner, gap-free within an epoch.
  A replica seeing a gap refuses the append — a lost record must become a visible stall,
  never a silent hole replay will skip over.

### The drain

Asynchronously, and under backpressure when the journal passes its high-water mark:

1. Coalesce journal records into extent-sized runs (latest record wins per range).
2. Write the runs into **open egroups** on RF nodes: append-only, fsync, verify.
3. Seal any egroup reaching 4 MiB: write its footer (slice checksums), record its seal
   hash.
4. Commit the batch to Hydra: the moved block-map rows, egroup state changes, and one
   lightweight transaction on the vdisk's `drain_seq` — the exactly-once mechanism
   ([metadata.md §4](./metadata.md)).
5. Only after the map commit is durable: advance the journal's reclaim watermark.

Order is load-bearing at every step (invariant I-3): bytes, then map, then reclaim. A
crash at any point leaves either undrained journal (replayed) or orphaned egroup bytes
(scrubbed away) — never a map entry pointing at nothing.

### Failure during drain: redirect, never block

If an egroup replica write fails or times out, the drain does **not** wait for the node
to return. It abandons that egroup (marks it dead in the batch), allocates a fresh one on
healthy nodes, and writes there. Sealed-egroup immutability is what makes this cheap: the
half-written replica is garbage by construction, collected by Purah, and no repair
protocol for partially-diverged mutable files ever needs to exist. The data path's only
relationship with a dead node is a timeout.

## 3. The read path

Per range, in order:

1. **Overlay index** — undrained journal records (owner memory). Newest epoch wins.
2. **Block map** — extent index → egroup, offset. Cached; invalidated by drain commits.
3. **Replica choice** — local replica if the node holds one (data locality), else any
   replica the egroup map lists as verified; forwarded reads go to the owner, which
   applies the same rules.
4. **Verify** — slice checksum on every read (I-5). Mismatch → try next replica, rewrite
   the bad slice from a good one (read repair), count it for Mimir. All replicas bad →
   loud EIO, never bytes.

Read-after-write consistency is structural, not negotiated: one owner, one overlay, all
reads through it.

The overlay is bounded memory: when it exceeds its cap the drain is forced and the guest
write that would overflow it waits for drain progress — backpressure, not growth.

## 4. Checksums

- **Journal records**: CRC32C over the whole record, verified on every replay and every
  overlay-servicing read of the payload.
- **Egroups**: CRC32C per 32 KiB slice, stored in a footer that is itself checksummed;
  a whole-egroup hash recorded in the egroup map at seal time. Reads verify slices;
  scrub verifies sealed egroups against the seal hash end-to-end.
- CRC32C because it is hardware-accelerated everywhere this runs and detection (not
  authentication) is the job; the footer format carries an algorithm byte so a stronger
  hash is a new value, not a new format.
- Every record and extent carries its vdisk UUID inside the checksummed region — the
  cheap enforcement of isolation (I-8): a misdirected read fails verification rather
  than returning a neighbour's bytes.

## 5. Garbage collection: mark-sweep, no reference counts

Overwritten extents, abandoned egroups and drained journal chunks all become garbage.
The reclaimer is a Purah scan, not inline reference counting, and this is a considered
rejection: distributed refcounts are a bug class — every increment/decrement path, every
crash between data-op and count-op, every snapshot clone doubling counts — and the
codebase already chose scans over counters once (image reconciliation belongs in a job,
not a page load; same instinct).

Purah's sweep rule implements I-7 directly: an egroup is reclaimable only if
unreferenced by every map generation in **two consecutive scans** separated by more than
the maximum drain duration. Slow reclaim, immune to the scan/drain race by construction.

## 6. Numbers, and why these numbers

| Parameter | v1 value | Why |
|---|---|---|
| Extent | 1 MiB | Map granularity: 1 TiB vdisk = ~1M map rows worst case, fine for Scylla; smaller doubles map size for no locality gain. |
| Egroup | 4 MiB | Large enough that seals are rare and sequential I/O dominates; small enough that re-replicating one is sub-second. |
| Journal record cap | 1 MiB | One extent; above qemu's typical request size, so splits are rare. |
| Journal chunk | 64 MiB | Bounds replay time and per-vdisk pause on replica loss to seconds. |
| Drain high-water | 32 MiB | Half the chunk: drain runs continuously under load rather than in cliffs. |
| Slice | 32 KiB | Checksum overhead ~0.01%; verification cost invisible next to the I/O. |

All tunables; none sacred; every future change re-argued against the column it sits in.
