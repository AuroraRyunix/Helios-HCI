# Concurrent NBD requests and group commit (design, not built)

Sidon serves one request at a time per connection and holds the vdisk lock for the whole of
a write, fsync and replica round trip included. A guest that keeps eight or sixty-four small
writes in flight therefore gets the throughput of one. This document is the design for
removing that, and the reasoning for why it is a design and not part of the change that moved
the drain off the acknowledgement path and overlapped a write's local sync with its replicas
([sidon.md section 2](../sidon.md)). Nothing here is implemented.

## 1. What it would buy, and the evidence it is the next lever

Measured on the three-node test cluster (RF 2, `valcli storage.benchmark`, after the drain and
write-path changes; the lab's three virtual disks share one backing store, which is why the
large-block numbers are close to its ceiling and the small-block numbers are not):

| workload | result | what bounds it |
|---|---|---|
| 1 MiB writes, queue depth 1 | about 31 MiB/s, 32 ms each | one request at a time; a local and a remote flush per request |
| 1 MiB writes, queue depth 16 | about 40 MiB/s | the same -- depth only queues requests in the socket |
| 4 KiB writes, queue depth 1 | about 170 IOPS, 5.9 ms each | the flush: a bare 4 KiB `O_DSYNC` write is 3.2 ms on this disk |
| 16 MiB writes, queue depth 1 | about 43 MiB/s | the disks' flush bandwidth, which is what is left to hit |

The large-block rows are within a small factor of what the storage can do and need no help from
this. The small-block row is the one a database or a guest filesystem journal generates, and it
is paying one disk flush locally and one on the replica per write, for writes that are
independent of each other. With sixteen of them outstanding, one flush on each side could cover
all sixteen. That is group commit, and it is the only lever left that could plausibly be worth
a large factor on small writes; how large is a measurement to make once it is built, not a
number to promise here.

## 2. What stops it today

* **`nbd::serve` is a strict loop**: read a request, execute it, write the reply, read the next.
  qemu will keep many requests in flight if the server lets it (simple replies carry a handle,
  so completion order is free); this one answers them in arrival order, one at a time.
* **`write_through` holds `Arc<Mutex<Vdisk>>` across `append_group`**: sequence assignment, the
  journal write, the local `fdatasync`, every replica round trip, and the overlay insert. A
  second writer cannot even start its append until the first is acknowledged.
* **Records of one guest write must stay contiguous.** Replay applies a group only if its last
  record carries `FLAG_COMMIT`, and treats every record since the previous marker as part of it.
  Two multi-record writes interleaved in the journal would be applied as one group, or not at
  all.
* **A replica's journal must be in sequence order.** The replica appends bytes and does not
  reorder; `Journal::replay` refuses a sequence jump. Concurrent senders over one connection
  would arrive in whatever order they won the connection's mutex.
* **The overlay must be inserted in sequence order.** `Overlay::insert` makes the newest insert
  win a range, so for two overlapping writes the one with the higher sequence number must be
  inserted last, and only after it is durable -- the invariant that a read never sees a write
  that was not acknowledged.

## 3. Design

### 3.1 NBD: a reader, a bounded set of workers, a reply lock

`serve` becomes three roles on one connection: the existing loop reads requests and **drains a
write's payload from the socket before anything else**, exactly as now (the rule in the module
doc that stops a protocol desynchronisation); it then hands the request to a worker from a
bounded pool (32 per connection, with an in-flight byte budget so 32 requests of 64 MiB cannot
be held at once); a worker runs the request and writes its reply under a mutex on the socket.
Replies may complete out of order, which simple replies allow. `NBD_CMD_FLUSH` and FUA stay
no-ops for the reason they are today: a write's reply is sent only once it is durable on every
copy, and a flush only has to cover writes the guest has already seen replies to. (A flush
does not order against writes still in flight; the protocol does not ask it to.)
`NBD_CMD_DISC` waits for the pool to drain before the connection closes. Reads run concurrently with each other and with writes; the guest owns
the ordering of overlapping requests it keeps in flight, as it does with any disk.

### 3.2 Vdisk: three stages instead of one lock

```
 writer thread                     committer (one per vdisk)                writer thread
 -------------                     -------------------------                -------------
 1. append stage  (short lock)
    validate, reserve the write's
    whole record range (contiguous),
    assign sequence numbers, write
    its frames at the journal tail,
    queue a ticket          ------>  2. commit stage (no vdisk lock)
                                        take every queued ticket (a batch)
                                        ONE fdatasync of the journal, and ONE
                                        OP_APPEND per replica carrying the
                                        batch's frames concatenated, in parallel
                                        (the machinery `append_group` has today)
                                     3. publish stage (short lock)
                                        for each ticket in sequence order:
                                          overlay insert
                                        then complete the tickets  ------>  reply sent
```

* **Batching needs no timer.** The committer takes whatever is queued the moment the previous
  batch finishes. At queue depth 1 a batch is one ticket and the behaviour is today's; at depth
  16 the sixteen tickets that arrived while the last batch was flushing go together. Nothing
  waits to fill a batch, so latency at low depth does not get worse.
* **The wire protocol does not change.** A replica's `OP_APPEND` writes the bytes it is given
  and syncs; a payload that holds several framed records is already something it handles, since
  the owner has always sent whole frames and the replica never parses them. A batch is one
  larger payload. Mixed-version clusters therefore work in both directions.
* **A guest write's records stay together.** The append stage reserves a contiguous range under
  the append lock, and a write's frames are written as a block, so groups never interleave. A
  write larger than the record cap is still several records and one marker, in one ticket.
* **Replica order is journal order.** One committer per vdisk is the only sender of journal
  appends, in ticket order, so the replica sees the owner's bytes in the owner's order. The
  per-replica threads `append_group` already uses become the committer's, one per replica for
  the life of the vdisk instead of one set per write.
* **Overlay inserts happen in the publish stage, in sequence order, after the batch is
  durable.** That preserves both "a read never sees an unacknowledged write" and "the newest of
  two overlapping writes wins", and keeps the journal and the in-memory state agreeing about
  which one that is, which is what replay has to reproduce.

### 3.3 Failure

A batch fails as a unit: if the local sync or any replica fails, every ticket in it gets the
error, and **the vdisk is marked degraded and takes no further appends until it is healed or
re-attached.** That is deliberately stricter than today, where a failed write leaves the
vdisk usable. Today the failed write's records stay in the journal (and in the replicas'),
unacknowledged, followed by the next write's; replay would apply them with it. One failed
write is tolerable. A failed batch of sixteen followed by sixteen more is the same hazard
sixteen times over, and trimming the tail on the owner and on the replicas needs a
truncate-the-tail request that does not exist, so the design does not pretend to. A replica
failure already stops writes until the set is restored; this extends that to a local sync
failure, which is the right answer for a journal device that has just reported an error.

A stale-epoch answer deposes the owner exactly as now: the batch fails, the vdisk records that
it was deposed, and nothing is acknowledged.

### 3.4 The drain, and everything else that needs a quiet journal

Rotation (the first phase of a drain) and `Journal::replace` must not happen under an in-flight
batch. Rotation takes the append lock and waits until the committer has no batch outstanding,
which is a bounded wait of one flush, then rotates and releases both. `drain_all`, a replica
heal, a seal and a snapshot all go through the paths that already wait for the drain gate and
take the vdisk lock; they additionally quiesce the committer the same way.

The hard ceiling moves to the append stage: a writer that finds the journal at the ceiling waits
for the drain, without holding the append lock, as `write_through` does now.

### 3.5 Memory

The in-flight byte budget in 3.1 bounds what is queued. A ticket holds its frames until its
batch is durable, which is the same memory `append_group` already holds for a write, times the
number in flight.

## 4. Invariants, and where each is kept

| Invariant | Kept by |
|---|---|
| Acknowledged means durable on every replica | A ticket completes only in the publish stage, after the batch's local sync and every replica's OK. |
| An unacknowledged write is not visible | Overlay insert happens only in the publish stage. |
| A group is applied whole or not at all | Contiguous reservation; marker last; one ticket per guest write. |
| Replica journal equals the owner's | One committer, ticket order, bytes sent as written. |
| Partial write-all is an error | Batch fails as a unit; vdisk degraded; no append until healed. |
| Stale epoch deposes the owner | Replica status checked per batch, as per write now. |
| Journal forgets only after the map is repointed | Unchanged; rotation waits for the committer to be idle. |
| Newest of overlapping writes wins, after a crash too | Sequence-ordered publish; replay is in journal order. |

## 5. How it would be measured

`valcli storage.benchmark` reports a 4 KiB queue-depth-1 line and a 1 MiB queue-depth-16 line.
When this is built it should gain a 4 KiB queue-depth-16 line, and the claim to test is that it
is several times the queue-depth-1 IOPS (bounded by the flush rate on the batch, not by the
number of requests), while the queue-depth-1 line does not move.

## 6. Tests it would need

* Out-of-order replies: a read and a write in flight together; replies matched to handles.
* The payload-drain rule under concurrency: a failing request while others are in flight does
  not desynchronise the stream.
* One flush per batch, counted, for N concurrent writes; one flush for one write.
* Overlapping concurrent writes: the higher sequence number wins in memory and after replay.
* A multi-record write concurrent with others: groups stay contiguous in the journal and on the
  replicas, byte for byte.
* Local sync failure, replica failure and stale epoch mid-batch: every ticket in the batch
  errors, nothing is visible, the vdisk refuses further appends until healed.
* A drain started while batches are in flight: rotation lands between batches, and nothing
  acknowledged is lost across it (crash-recovery test, as for the drain).
* Mixed versions: a batch to a replica that predates this change.

## 7. Why this is not in the change that preceded it

It is not an addition to the write path; it replaces its concurrency model. The vdisk lock
stops being the thing that makes a write atomic, every path that takes it (write, drain, heal,
snapshot, detach) acquires a second condition to wait on, and the failure semantics change
(3.3). Each of those needs the tests above, written against failure injection, before the
change is safe to run on a cluster that holds anything. The measurements in section 1 are the
case for doing it; they are not a reason to do it without them.
