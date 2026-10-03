# Concurrent NBD requests and group commit (built)

A connection now serves up to 32 requests at once, and the writes that are in flight together
share one journal `fdatasync` here and one round trip to each replica. A write is still
acknowledged only when it is durable on every copy, and still invisible to readers until then.
This document is the design as it was built: what it buys (measured, including where it does
not), how it works, the failure semantics that were decided, and the test that holds each
invariant. The code is `sidon/src/nbd.rs` (the connection), `sidon/src/vdisk/commit.rs` (the
pipeline) and the `submit_write` / `lock_quiet` / `flush_through` functions in
`sidon/src/vdisk.rs`.

Before this change a connection was a strict loop (read a request, execute it, reply, read the
next) and `write_through` held the vdisk lock across the journal write, the local `fdatasync`,
every replica round trip and the overlay insert. A guest that kept sixteen small writes in
flight got the throughput of one.

## 1. What it buys, measured

Three-node test cluster, RF 2, the owner on one node and a replica on another. **The lab's three
virtual disks share one backing store**, so a bare fsynced write is about 3.2 ms and 64 MiB of
fsynced data is about 110 MB/s whatever Sidon does; the large-block rows are at that ceiling and
are not what this change is for. `qemu-img bench` over the vdisk's NBD socket, fresh vdisk per
run, the same script before (the build before this change, deployed) and after (this change,
deployed):

| workload | before | after | what bounds it now |
|---|---|---|---|
| 4 KiB writes, queue depth 1 | 167-172 IOPS, 5.9 ms | 171 IOPS, 5.9 ms | one local and one remote flush per write, as ever: **unchanged** |
| 4 KiB writes, queue depth 4 | 170 IOPS | 574 IOPS | 3.5 writes per batch; a batch costs 5.8 ms |
| 4 KiB writes, queue depth 16 | 157 IOPS | **2,275 IOPS** (14x) | 15.2 writes per batch; a batch costs 6.3 ms |
| 16 KiB writes, queue depth 16 | 143 IOPS (2.2 MiB/s) | 714 IOPS (11.2 MiB/s) | 10.9 per batch, 14.8 ms per batch |
| 64 KiB writes, queue depth 16 | 152 IOPS (9.5 MiB/s) | 554 IOPS (34.6 MiB/s) | 8.2 per batch, 14.5 ms per batch |
| 1 MiB writes, queue depth 1 | 22.9 MiB/s | 23.8 MiB/s | the flush bandwidth of the shared store |
| 1 MiB writes, queue depth 16 | 23.8 MiB/s | 39.8 MiB/s | the same; 6.4 writes (6.4 MiB) per batch |
| 16 MiB writes, queue depth 1 | 29.1 MiB/s | 38.6 MiB/s | the same (four requests; do not read much into it) |

`valcli storage.benchmark default-pool`, which gained a **4 KiB queue-depth-16 line** with this
change (its other lines are unchanged), before and after, one run each:

| line | before | after |
|---|---|---|
| sequential write 1M qd1 | 29.7 MiB/s | 30.3 MiB/s |
| sync write 4k qd1 | 170 IOPS | 170 IOPS |
| sequential read 1M qd1 | 1,371 MiB/s | 1,627 MiB/s |
| sync write 4k qd16 | (no line; 157 IOPS by `qemu-img`) | **2,201 IOPS** |
| write 1M qd4 | 30.0 MiB/s | 33.6 MiB/s |
| write 1M qd16 | 41.2 MiB/s | 52.4 MiB/s |
| large write 16M qd1 | 34.9 MiB/s | 40.7 MiB/s |

Its runs are a second or less, so its large-block figures move by tens of percent between
identical runs; the `qemu-img` table above is the steadier one, and the large-block rows are
not evidence of anything. One line needs a note: with the new phase placed *before* the read
phase, the read line measured about 790 MiB/s three times. Running the old phase list against
the new daemon gave 1,627 and 1,684, and repeating the read pattern on its own gave 1,300-1,500,
so the daemon is not slower to read; the 4 KiB qd16 phase now runs after the read so that line
stays comparable. Why a read straight after 1,600 small writes is slower was not run down.

How to read it:

* **The claim in the previous version of this document held**: the 4 KiB queue-depth-16 line is
  several times the queue-depth-1 line, bounded by the flush rate of a batch rather than by the
  number of requests (about 6 ms per batch whether it holds 1 write or 15), and the
  queue-depth-1 line did not move. 14x at depth 16 is what the arithmetic gives: 15 writes per
  6.3 ms batch is 2,400 per second.
* **It did not help, and was never going to, where the lab's storage is the limit.** 1 MiB and
  16 MiB writes are bound by the shared backing store's flush bandwidth, which batching does not
  raise. The modest gain at 1 MiB depth 16 is the local append no longer waiting behind the
  network, nothing more. A faster result there needs fewer physical writes per guest byte.
* **Depth 32 is depth 16.** qemu's NBD client keeps at most 16 requests in flight, so
  `qemu-img bench -d 32` is the same workload (the two rows differ by noise). Sidon admits 32.
* **Waiting for a burst is worth a factor of two on its own.** With no wait at all the first
  request of a burst is committed alone and the rest queue behind it, so the pipeline settles
  into alternating half-size batches: measured 7.8 writes per batch at depth 16 and 1,285 IOPS.
  Letting a leader wait up to 300 microseconds (never at queue depth 1, never longer than an
  eighth of what a commit has been costing) gave 15.3 per batch and 2,260 IOPS. 1,000
  microseconds gave the same batching as 300, so the shorter one is the default.
* **Reads are served by workers now.** A 4 KiB read at queue depth 1 takes about 53 microseconds
  end to end from `qemu-img` (1 MiB: 0.36-0.5 ms); at depth 16, 4 KiB reads run at about 100,000
  IOPS. There is no "before" for these on the same footing -- the old binary is gone from the
  cluster -- and a lone read does now cost two thread hand-offs (a few tens of microseconds)
  more than it did. Answering a lone read on the reader's thread was tried and dropped: it
  serialises a burst of reads behind the first and cost far more at depth 16 than it saved at
  depth 1.

## 2. How it is built

### 2.1 The connection (`nbd.rs`)

`serve` is three roles. **The reader** reads requests in order. For a write it reads the payload
first, completely, before anything else (the rule in the module doc that stops a protocol
desynchronisation), and then **submits the write to the backend before it reads the next
request** (`Backend::begin_write`). For a local vdisk submitting means appending the write's
records to the journal, so writes take their journal order from the order they arrived on the
socket. **Workers** run the rest: a write's worker waits for it to be durable on every copy; a
read's worker reads; a flush's worker waits for the barrier. They send their reply under a lock
on the socket, in completion order -- simple replies carry the request's handle, so that is
legal, and structured replies are still declined at the handshake. **A bounded set**: at most 32
requests in flight per connection (a worker is started when the depth first needs it, not up
front), and 128 MiB of write payload and read buffer in flight, so thirty-two 64 MiB writes
cannot all be held (a request larger than the budget still runs, alone).

* **FUA** needs nothing beyond the write: a write is not replied to until it is durable on
  every copy, which is all FUA asks for. It is no longer followed by a flush, which would now
  also wait for other connections' in-flight writes for no reason.
* **Flush** is a barrier: it returns when every write submitted before it has been committed.
  Because the reader submits writes in order, a flush read after a write covers it even if the
  guest did not wait for the write's reply. Writes submitted after the flush are not waited
  for, which is all the protocol asks. On a vdisk whose pipeline has stopped taking writes it is
  an error, because nothing can be made durable.
* **`NBD_CMD_DISC`** waits for the requests in flight to finish and be replied to.
* A failing request does not desynchronise the stream: a write's payload has been consumed
  before it is submitted, an error reply carries no payload, and an unknown command is refused
  and the stream carries on.
* The guest owns the ordering of overlapping requests it keeps in flight, as with any disk. What
  Sidon guarantees is that the order is *a defined one* (arrival order for writes) and that a
  reply means durable.

The accept loop still serves one connection at a time per export (unchanged; the handshake does
not advertise NBD's multi-connection flag). Forwarded I/O from a node that does not own the vdisk
still crosses one peer connection a request at a time, so a forwarding guest does not get this
(see section 6).

### 2.2 The pipeline (`vdisk/commit.rs`)

```
 NBD reader / writer thread                 whichever waiting writer leads            same thread
 --------------------------                 ------------------------------            -----------
 1. append stage (vdisk lock, short)
    validate, split into records,
    write them at the journal tail,
    assign sequence numbers, queue a
    ticket, in that order, atomically ----> 2. commit stage (no vdisk lock)
                                               take every queued ticket (a batch)
                                               ONE fdatasync of the journal, and, in parallel,
                                               ONE OP_APPEND per replica carrying the batch's
                                               frames (up to 4 MiB a request)
                                            3. publish stage (vdisk lock, short)
                                               overlay insert for each ticket in order, then
                                               complete the tickets ---------------> reply sent
```

* **No committer thread.** The first writer to find no commit in progress becomes the leader and
  does the batch's work on its own stack; the others wait on it. There is nothing per vdisk to
  start, stop or leak, and a batch's per-replica threads are scoped to it. Leadership is also
  available to anyone who needs the queue empty (a drain, a heal, a flush), so no ticket's
  progress depends on its own writer having started to wait. A panic in a commit completes its
  tickets with an error and releases the pipeline.
* **Batching needs no timer, except one.** The leader takes whatever is queued the moment the
  previous batch finishes. The exception is a short bounded wait, only when the queue is shorter
  than the last batch was: see section 1 for why, and `SIDON_COMMIT_LINGER_US` (default 300, 0
  turns it off) in section 5.
* **A guest write's records stay together.** The append stage is one critical section; a write
  larger than the record cap is several records and one commit marker, in one ticket, and tickets
  are batched whole. A ticket that cannot be fully appended is taken back out of the journal.
* **Replica order is journal order.** Appends happen under the vdisk lock in queue order; one
  leader at a time sends, in ticket order, over the one connection per replica; the replica
  appends bytes. `every_replica ... byte for byte` is tested under concurrency.
* **The overlay is inserted in sequence order, after the whole batch is durable**, in one
  critical section per batch, so a read sees all of a group or none of it, never a write that was
  not acknowledged, and of two overlapping writes the one with the higher sequence number wins.

### 2.3 Failure, and what was decided

A batch fails as a unit. If the local sync or any replica fails, **every write in the batch gets
the error**, nothing in the batch is published, and:

* **The vdisk is flagged degraded exactly as a replica failure always flagged it**, with the
  cause as its reason (the `list` and `status` ops report it); a stale-epoch answer deposes the owner
  (`deposed: replica ... is fenced at epoch ...`), as it did. **A local sync failure now also
  flags it**, which it did not before: a journal device that has just reported an error is not
  trusted with another write.
* **The pipeline fails closed.** No further write is appended: new writes get an error naming
  the earlier failure, and a flush is an error. Every write already queued *behind* the failed
  batch fails with it, and its records, which no replica has, are taken back out of the local
  journal (sequence numbers and all), so the journal ends where the failed batch ended.
* This is deliberately stricter than the single-write owner, which kept appending after a
  failure. A replica that missed a batch holds a journal that ends part-way through it (or, for a
  half-sent request, mid-record), and appending the next batch after that puts a hole in the
  middle that replay refuses (`sequence jumped`). One such hole was a rare accident; with sixteen
  writes in each batch it would be a routine one.
* **How it recovers** depends on the cause. A **replica** failure is repaired in place by the
  heal (`vdisk::recover`, called from `op_purah_heal` on the watcher's five-second tick, whether
  the replica was replaced or just stopped answering for a moment): each replica's journal is
  emptied (`OP_TRUNCATE`) and refilled with this node's (`OP_APPEND`), so every copy is
  byte-identical and the next append lands after the same tail everywhere, then writes resume.
  There is a window in which a replica holds less than this node (the same exposure as a heal
  onto a spare); there is no "truncate to here" request to do better. A **local** sync failure
  or a **deposed** owner is not repairable in place and says so; the vdisk has to be
  re-attached, which replays from the replicas (the failed batch's records are on at least one
  replica or on none, never a hole).
* What the guest sees: EIO (or EPERM for a refusal) on each write of the failed batch, as it saw
  for a single failed write. Writes in the failed batch **may reappear after a crash** if their
  records reached the journal and a commit marker did: the invariants permit an unacknowledged
  write to be applied or not (I-2), and the guest was told it failed. They never appear before
  one.
* Observed on the test cluster: a replica node was stopped for 25 seconds under a stream of
  4 KiB writes. Two writes returned EIO, the heal replaced the replica with the spare in the same
  second and re-synchronised the journals, the 498 acknowledged writes read back correct, and the
  owner's journal and the new replica's journal had the same checksum.

### 2.4 The drain and everything else that needs a still journal

The journal rotates (a drain's first step) and is replaced (a takeover) only when no write is
between its append and its commit. `lock_quiet` and `lock_idle` pause new appends, wait for the
pipeline to empty **without the vdisk lock** (the leader needs it to publish), and return with
the lock held; an overlay position can therefore never name a segment that a drain has since
deleted, and the leader can sync "the live segment" without caring which segment it was. The wait
is bounded by one flush because nothing joins the queue while it is paused. The background drain
plans through `lock_quiet`; `drain_all` (detach, seal, snapshot, clone, flush), a replica join
and the heal go through `lock_idle`, which also waits for a running drain. `plan_drain`,
`add_replica` and `fence_and_recover` refuse if anything is in flight, as a backstop.

The hard ceiling moves to the append stage: a writer that finds the journal at the ceiling waits
for the drain without holding any lock, as before. With several writers admitted at once the
journal can pass the ceiling by the writes admitted before the first reached it, bounded by the
connection's in-flight budget, where it used to be by one write.

### 2.5 Compatibility and rolling restarts

**The replica protocol did not change.** A batch is one `OP_APPEND` whose payload is several
whole journal frames; the replica appends bytes and never parses them, so the owner's
`APPEND_DEFER_SYNC` convention (all but the last request of a batch defer their sync, the last is
made durable before it is answered) is the same one a multi-record write already used. A replica
from before this change takes a batch (checked by a test that emulates one: no notion of
deferring, always syncs), and an old owner appends to a new replica as ever. Both directions
were exercised live during the rollout on the test cluster (new owner with an old replica, old
owner with a new replica) with the benchmark running. `OP_TRUNCATE` and `OP_APPEND` are the
existing opcodes; the repair uses nothing else. So the nodes can be restarted one at a time in
any order.

### 2.6 Memory

A ticket holds its frames until its batch is durable: the memory a write held before, times the
writes in flight, which the connection's 128 MiB budget bounds. A batch is at most 32 MiB (a
single larger write is its own batch); a request to a replica at most about 5 MiB.

## 3. The invariants, and the test that holds each

| Invariant | Kept by | Test |
|---|---|---|
| Acknowledged means durable on every replica | a ticket completes only after the batch's local sync and every replica's OK | `a_failed_local_sync_fails_every_write_in_the_batch...`, `a_replica_failure_mid_batch...`, `writes_in_flight_together_share_one_sync...` |
| An unacknowledged write is not visible | the overlay is inserted only in the publish stage | the three above (reads of the failed ranges are zero), `a_flush_does_not_return_while_an_earlier_write_is_still_between...` |
| A group is applied whole or not at all | contiguous reservation, marker last, one ticket per guest write | `many_concurrent_writers_leave_the_replicas_with_the_owners_journal_byte_for_byte`, `recovery_replays_acknowledged_groups_whole_and_never_a_torn_one` (a crash at every record boundary, one byte either side, and mid-record) |
| Replica journal equals the owner's | one leader, ticket order, bytes sent as written | `many_concurrent_writers...`, `a_batch_reaches_the_replica_as_whole_frames...` |
| Partial write-all is an error | the batch fails as a unit; the pipeline fails closed | `a_replica_failure_mid_batch...`, `with_two_replicas_a_repair_makes_both_identical_to_the_owner` |
| A failed batch fails every write in it, and those behind it | `Pipeline::fail` | `a_failed_local_sync...` (7 writes), `a_replica_failure_mid_batch...` (writes behind are taken back out of the journal: `next_seq` and length checked) |
| Stale epoch deposes the owner | replica status checked per request | `a_stale_epoch_deposes_the_owner_for_every_write_in_the_batch` (all refused, nothing visible, nothing on the new owner's journal, not repairable in place) |
| Newest of overlapping writes wins, after a crash too | queue order is sequence order; publish in order; replay is in journal order | `concurrent_writers_to_overlapping_ranges_apply_in_journal_order_in_memory_and_after_replay` (single-record and two-record writes; the journal's last group wins, whole, and replay agrees) |
| Flush covers every write submitted before it | `flush_through` waits for the ticket barrier | `a_flush_does_not_return_while...`, `a_flush_does_not_wait_for_writes_submitted_after_it` |
| Journal forgets only after the map is repointed; rotation never moves a file under a ticket | `lock_quiet` | `a_drain_waits_for_the_batch_in_flight_and_nothing_acknowledged_is_lost_across_it`, `writers_and_repeated_drains_together_lose_nothing...` |
| One sync and one round trip per batch; one for one write | the leader | `writes_in_flight_together_share_one_sync...` (16 writes, 2 syncs, 2 requests), `one_write_is_a_batch_of_one...` |
| A panic wedges nothing | `lead`'s guard | `a_panic_in_a_commit_fails_the_batch...` |
| Rollback of an unsent tail is exact | `Journal::mark` / `rollback` | `journal::tests::a_rollback_takes_back_a_group_and_its_sequence_numbers` and two more |
| Out-of-order replies match handles; the payload-drain rule under concurrency; disconnect and FUA; arrival order | `nbd.rs` | `nbd::tests::*` (a mock backend and a minimal NBD client), `a_guest_at_queue_depth_sixteen_gets_its_writes_committed_together`, `nbd_writes_to_one_range_are_applied_in_the_order_they_arrived` |
| A leader waits for a burst only when it should | `Pipeline::drive` | `a_leader_waits_for_the_rest_of_a_burst...`, `a_leader_does_not_wait_longer_than_the_linger...`, `queue_depth_one_never_waits` |

Fault injection is by holding or failing the local sync (`journal::testhook`), refusing or
delaying a replica's request (the replica test hook), and fencing a replica; the interesting
states -- a batch in flight with writers queued behind it, a drain wanting the journal still --
are reached on purpose by holding the first batch's sync until the test says so.

## 4. What changed from the design as first written

* **No committer thread and no long-lived per-replica threads**: a leader writer and threads
  scoped to the batch (section 2.2). The reasoning was lifecycle: nothing to start, stop or leak.
* **The append happens on the NBD reader's thread**, not a worker's, so arrival order is journal
  order and a flush covers the writes before it. The design had the worker do the whole request.
* **A replica-side break is repaired in place** (`recover`) rather than only by re-attaching;
  without it a replica's momentary failure would have wedged a running guest's vdisk until its VM
  restarted, which is worse than the single-write behaviour it replaced.
* **Queued-behind writes are taken back out of the journal** when a batch fails.
* **A short wait for a burst** (the design said no timer): without it the pipeline settled into
  half-size batches.
* **Writes admitted at the ceiling can overshoot it by the in-flight budget**, not by one write.

## 5. Operating it

* The daemon's `status` op (the API's `/api/v1/dfs/vdisk`, `op: status`) reports, per vdisk, a `commit` object: `batches`,
  `writes_committed` (so writes per batch is their ratio), `largest_batch`, `commit_ms` (so ms per
  batch), `in_flight`, `linger_us`, and `broken` with the reason when the pipeline has stopped
  taking writes. A vdisk that is `degraded` for a commit failure says why, and the watcher heals
  it within about five seconds when the cause is a replica.
* `SIDON_COMMIT_LINGER_US` (environment of the daemon; default 300, maximum 5000): the longest a
  leader waits for a burst to finish arriving, capped at an eighth of the recent commit time.
  Zero turns it off.
* Limits are constants in `nbd.rs` (`MAX_IN_FLIGHT` 32, `IN_FLIGHT_BYTES` 128 MiB) and
  `commit.rs` (`BATCH_MAX` 32 MiB, `SEND_MAX` 4 MiB).

## 6. Not done, and why

* **Overlapped commits.** One batch commits at a time. Two in flight (the next batch's sync
  while the previous one's replies are processed) could roughly double the rate again when a
  commit is much longer than a client's turnaround, but it needs the replica protocol to pipeline
  requests (the replica serves a connection one request at a time, and a client holds the
  connection for the round trip) and a rule for a failure of the earlier batch while the later one
  is half-sent. Measured, the linger recovers most of what it would give at depth 16.
* **Forwarded writes are still one at a time.** `Forwarder` relays over one peer connection a
  request at a time, and the owner's `serve_connection` handles a connection serially. A guest on
  a node that does not yet own its vdisk sees queue depth 1 until ownership follows.
* **Reads are serialised by the vdisk lock** (they hold it across the extent read). They run on
  workers, but one at a time per vdisk. Reading outside the lock is a separate change.
* **A peer call retries once on a transport failure**, and a retried append whose first
  attempt had reached the replica would leave a duplicate record there. Pre-existing and
  unchanged (a reply timeout is the only way to hit it, and the same hazard applied per write
  before); the repair above would clear it on the next break, but nothing detects it first.
* **Multiple NBD connections to one export** are still served one at a time.
