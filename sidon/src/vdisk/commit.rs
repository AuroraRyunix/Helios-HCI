//! The commit pipeline: how many guest writes share one `fdatasync` and one round trip.
//!
//! A write is three steps, and only the middle one is shared:
//!
//! 1. **Append** (under the vdisk lock, microseconds). Validate, split into records, write
//!    them at the journal tail, and queue a [`Ticket`]. The records are *in the file* but
//!    nothing has said they are durable, and nothing reads them: the overlay does not know
//!    about them yet. Doing this under the lock is what gives records of one guest write a
//!    contiguous run of sequence numbers and gives the queue the same order as the journal.
//! 2. **Commit** (no vdisk lock). Whichever waiting writer finds no commit in progress
//!    becomes the *leader*: it takes every queued ticket as one batch, `fdatasync`s the
//!    journal once while one `OP_APPEND` per replica carries the batch's bytes, in
//!    parallel. A writer that arrives while a batch is committing waits; the next leader
//!    takes everything that queued meanwhile. Nothing waits to fill a batch, so at queue
//!    depth one a batch is one ticket and the cost is what it always was.
//! 3. **Publish** (under the vdisk lock, microseconds). Only after the whole batch is
//!    durable here and on every replica: insert each ticket's records into the overlay, in
//!    ticket (= sequence) order, and release the writers. A read therefore never sees a
//!    write that has not been acknowledged, a group is visible whole or not at all, and of
//!    two overlapping writes the one with the higher sequence number is inserted last.
//!
//! There is no committer thread. A leader is just a writer doing the batch's work on its
//! own stack, which means no thread to start, stop or leak per vdisk, and a batch's
//! per-replica threads are scoped to it.
//!
//! ## Failure
//!
//! A batch fails as a unit. If the local sync or any replica fails, every ticket in it gets
//! the error, nothing in it is published, and the pipeline is **broken**: the vdisk is
//! flagged degraded (`Vdisk::degraded`, as a replica failure always did, and now a local
//! sync failure too), every ticket still queued behind the batch is failed with it (its
//! records, which no replica has, are taken back out of the local journal), and no further
//! write is appended until the pipeline is repaired. That is stricter than a single-write
//! owner needed to be, and deliberately: a replica that missed a batch holds a journal that
//! ends part-way, and appending the next batch after it would put a hole in the middle that
//! replay refuses. See [`recover`] for how a replica-side break is repaired in place (the
//! heal does it); a local or deposed break is not repairable in place.
//!
//! ## Rotation
//!
//! The journal rotates (a drain's first step) and is replaced (a takeover) only when no
//! ticket is outstanding: `lock_quiet` / `lock_idle` pause new appends, wait for the
//! queue to empty and return with the vdisk lock held. That is what lets the leader sync
//! "the live segment" without caring which segment its records went to, and what keeps an
//! overlay position from naming a segment that a drain has since deleted.

use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;

use super::*;

/// The most bytes of journal records sent to a replica in one `OP_APPEND`. A batch larger
/// than this is several requests, all but the last deferring their sync.
pub(super) const SEND_MAX: usize = 4 << 20;

/// The most bytes one leader takes into a batch (it always takes at least one ticket). A
/// bound on how long one commit can run and how much a failure takes with it, not a target:
/// batches are whatever queued while the last one flushed.
pub(super) const BATCH_MAX: usize = 32 << 20;

/// The longest a leader waits for a burst to finish arriving, in microseconds, from
/// `SIDON_COMMIT_LINGER_US` (default: see `DEFAULT_LINGER_US`). Zero turns waiting off.
///
/// Why there is a wait at all: with no wait the first request of a burst is committed alone
/// and the rest queue behind it, so a guest at queue depth 16 gets batches of about 8 -- two
/// half-size commits where one would do. Measured on the test cluster: 7.8 writes per batch
/// without it, 15.3 with 300 us, and twice the IOPS. The wait happens only when the queue is
/// shorter than the last batch was, so queue depth one never waits.
fn linger_from_env() -> Duration {
    let us = std::env::var("SIDON_COMMIT_LINGER_US")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_LINGER_US);
    Duration::from_micros(us.min(5000))
}

/// See `linger_from_env`.
pub(super) const DEFAULT_LINGER_US: u64 = 300;

/// Why the pipeline stopped taking writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Cause {
    /// A replica did not take a batch. Repairable in place: `recover`.
    Replica,
    /// The local `fdatasync` failed. A journal device that has just reported an error is
    /// not trusted again until the vdisk is re-attached and replayed.
    Local,
    /// A replica is fenced at a higher epoch: this owner has been deposed.
    Deposed,
    /// A commit panicked.
    Panic,
}

#[derive(Clone, Debug)]
pub(super) struct Broken {
    pub(super) cause: Cause,
    why: String,
    refused: bool,
}

impl Broken {
    pub(super) fn why(&self) -> &str {
        &self.why
    }

    fn error(&self, id: &str) -> Error {
        let m = format!(
            "vdisk {id} cannot take writes: an earlier commit failed ({}). Writes resume when \
             the vdisk is healed or re-attached.",
            self.why
        );
        if self.refused {
            Error::refused(m)
        } else {
            Error::io(m)
        }
    }
}

/// An error that went to one ticket, copied for the next. `Error` is not `Clone`.
fn dup(e: &Error) -> Error {
    match e {
        Error::Io(m) => Error::Io(m.clone()),
        Error::Corrupt(m) => Error::Corrupt(m.clone()),
        Error::Meta(m) => Error::Meta(m.clone()),
        Error::Refused(m) => Error::Refused(m.clone()),
    }
}

/// What a batch needs to be made durable, captured when its records were appended.
pub(super) struct Ctx {
    id: String,
    epoch: u64,
    file: Arc<File>,
    jpath: PathBuf,
    replicas: Vec<Arc<PeerClient>>,
}

/// One guest write that has been appended and is waiting to be made durable.
pub(super) struct Ticket {
    /// Each record exactly as written to the local journal, in order.
    frames: Vec<Vec<u8>>,
    /// What to put in the overlay once every copy is durable: offset, length, position.
    publish: Vec<(u64, u32, u64)>,
    bytes: usize,
    /// Where the journal ended before this ticket's records.
    mark: journal::Mark,
    ctx: Arc<Ctx>,
    result: Mutex<Option<Result<()>>>,
}

impl Ticket {
    fn set(&self, r: Result<()>) {
        *self.result.lock().unwrap_or_else(|p| p.into_inner()) = Some(r);
    }
    fn take(&self) -> Option<Result<()>> {
        self.result.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

#[derive(Default)]
struct QState {
    queue: VecDeque<Arc<Ticket>>,
    committing: bool,
    /// How many writes were outstanding (in the batch and queued behind it) when the last
    /// batch started: what a leader waits for the queue to grow back to, when it waits at all.
    expect: usize,
    /// Tickets appended and not yet completed (queued or in a batch).
    outstanding: usize,
    next_id: u64,
    /// Tickets completed, successfully or not. Tickets complete in id order, so "every
    /// ticket below `mark` is complete" is `done >= mark`.
    done: u64,
    /// Quiescers waiting for `outstanding == 0`; while non-zero no new append starts.
    paused: usize,
    broken: Option<Broken>,
}

pub(super) struct Pipeline {
    st: Mutex<QState>,
    cv: Condvar,
    /// The most a leader may wait for the rest of a burst to arrive; zero is no waiting.
    linger: Duration,
    /// A running average of how long a batch takes to make durable (microseconds, 0 until
    /// the first). A leader waits at most an eighth of it: on a disk that syncs in 6 ms a
    /// few hundred microseconds is noise, and on one that syncs in 150 us it would not be.
    commit_ema_us: AtomicU64,
    batches: AtomicU64,
    committed: AtomicU64,
    largest: AtomicU64,
    /// Time spent making batches durable, summed (microseconds), for `stats`: with the batch
    /// count it says what a commit costs and so whether batching is the thing worth tuning.
    commit_us: AtomicU64,
}

/// What the append stage may do right now.
pub(super) enum Gate {
    Open,
    /// A drain or a heal wants the journal still; wait for it and ask again.
    Paused,
    Broken(Error),
}

/// The outcome of the append stage.
pub(super) enum Append {
    /// Nothing to write (a zero-length request).
    Nothing,
    Queued(Arc<Ticket>),
    Paused(Arc<Pipeline>),
}

impl Pipeline {
    pub(super) fn new() -> Arc<Pipeline> {
        Pipeline::with_linger(linger_from_env())
    }

    pub(super) fn with_linger(linger: Duration) -> Arc<Pipeline> {
        Arc::new(Pipeline {
            st: Mutex::new(QState::default()),
            cv: Condvar::new(),
            linger,
            commit_ema_us: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            largest: AtomicU64::new(0),
            commit_us: AtomicU64::new(0),
        })
    }

    fn lock(&self) -> MutexGuard<'_, QState> {
        self.st.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(super) fn gate(&self, id: &str) -> Gate {
        let st = self.lock();
        if let Some(b) = &st.broken {
            return Gate::Broken(b.error(id));
        }
        if st.paused > 0 {
            return Gate::Paused;
        }
        Gate::Open
    }

    pub(super) fn broken(&self) -> Option<Broken> {
        self.lock().broken.clone()
    }

    pub(super) fn clear_broken(&self) {
        self.lock().broken = None;
    }

    pub(super) fn outstanding(&self) -> usize {
        self.lock().outstanding
    }

    pub(super) fn stats(&self) -> Value {
        let st = self.lock();
        json!({
            "batches": self.batches.load(Ordering::Relaxed),
            "writes_committed": self.committed.load(Ordering::Relaxed),
            "largest_batch": self.largest.load(Ordering::Relaxed),
            "commit_ms": self.commit_us.load(Ordering::Relaxed) / 1000,
            "linger_us": self.linger.as_micros() as u64,
            "in_flight": st.outstanding,
            "broken": st.broken.as_ref().map(|b| b.why.clone()),
        })
    }

    /// Queue a ticket. Called with the vdisk lock held, so queue order is journal order.
    /// Refuses if the pipeline broke since the caller looked.
    fn enqueue(
        &self,
        frames: Vec<Vec<u8>>,
        publish: Vec<(u64, u32, u64)>,
        mark: journal::Mark,
        ctx: Arc<Ctx>,
    ) -> Result<Arc<Ticket>> {
        let mut st = self.lock();
        if let Some(b) = &st.broken {
            return Err(b.error(&ctx.id));
        }
        let bytes = frames.iter().map(Vec::len).sum();
        let t = Arc::new(Ticket {
            frames,
            publish,
            bytes,
            mark,
            ctx,
            result: Mutex::new(None),
        });
        st.next_id += 1;
        st.outstanding += 1;
        st.queue.push_back(Arc::clone(&t));
        Ok(t)
    }

    /// Ask the append stage to hold off, until [`Pipeline::unpause`].
    pub(super) fn pause(&self) {
        self.lock().paused += 1;
    }

    pub(super) fn unpause(&self) {
        let mut st = self.lock();
        st.paused = st.paused.saturating_sub(1);
        drop(st);
        self.cv.notify_all();
    }

    /// Block until the append stage is not paused. Holds no lock of the vdisk's.
    pub(super) fn wait_unpaused(&self) {
        let mut st = self.lock();
        while st.paused > 0 {
            st = self.cv.wait(st).unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Block until nothing is outstanding, committing what is queued if nobody else is. The
    /// caller must not hold the vdisk lock: a commit needs it to publish.
    ///
    /// Driving the commit itself, rather than only waiting for the writers to, is what keeps
    /// progress from depending on any one writer: a caller that has submitted writes and not
    /// yet waited for them (the trim loop, or a connection's reader) cannot leave a drain
    /// waiting on a queue nobody is working through.
    pub(super) fn wait_empty(self: &Arc<Self>, handle: &Arc<Mutex<Vdisk>>) {
        self.drive(handle, false, |st| st.outstanding == 0, || false);
    }

    /// A position in the ticket sequence: every write appended before this call is below it.
    pub(super) fn barrier(&self) -> u64 {
        self.lock().next_id
    }

    /// Block until every ticket below `mark` has completed, committing if nobody else is.
    pub(super) fn wait_below(self: &Arc<Self>, handle: &Arc<Mutex<Vdisk>>, mark: u64) {
        self.drive(handle, false, |st| st.done >= mark, || false);
    }

    /// Wait for `ticket` to be made durable, committing a batch if nobody else is.
    fn wait(self: &Arc<Self>, handle: &Arc<Mutex<Vdisk>>, ticket: &Arc<Ticket>) -> Result<()> {
        let mut out: Option<Result<()>> = None;
        self.drive(handle, true, |_| false, || {
            out = ticket.take();
            out.is_some()
        });
        out.expect("drive returns only when the ticket has completed")
    }

    /// Run commits, or wait for whoever is running them, until `state_done` holds of the
    /// queue's state or `ticket_done` says the caller's ticket has completed.
    fn drive(
        self: &Arc<Self>,
        handle: &Arc<Mutex<Vdisk>>,
        may_linger: bool,
        state_done: impl Fn(&QState) -> bool,
        mut ticket_done: impl FnMut() -> bool,
    ) {
        let mut st = self.lock();
        loop {
            if state_done(&st) || ticket_done() {
                return;
            }
            if st.committing || st.queue.is_empty() {
                st = self.cv.wait(st).unwrap_or_else(|p| p.into_inner());
                continue;
            }
            st.committing = true;
            // A burst arrives over a few hundred microseconds, and the first request of it
            // would otherwise be committed alone while the rest queue behind it for the next
            // batch -- two half-size batches where one would do. If the last batch was
            // bigger than what has arrived so far, give the rest a moment. Never at queue
            // depth one (the last batch was one write, so there is nothing to wait for), and
            // never for longer than `linger`.
            let budget = self.linger_budget();
            if may_linger && !budget.is_zero() && st.queue.len() < st.expect {
                let deadline = Instant::now() + budget;
                while st.queue.len() < st.expect && Instant::now() < deadline {
                    drop(st);
                    std::thread::sleep(Duration::from_micros(25));
                    st = self.lock();
                }
            }
            st.expect = st.queue.len();
            let mut batch: Vec<Arc<Ticket>> = Vec::new();
            let mut bytes = 0usize;
            while let Some(front) = st.queue.front() {
                if !batch.is_empty() && bytes + front.bytes > BATCH_MAX {
                    break;
                }
                bytes += front.bytes;
                batch.push(st.queue.pop_front().expect("front just seen"));
            }
            drop(st);
            self.lead(handle, batch);
            st = self.lock();
        }
    }

    /// How long a leader may wait for a burst: `linger`, but no more than an eighth of what a
    /// commit has been costing.
    fn linger_budget(&self) -> Duration {
        let ema = self.commit_ema_us.load(Ordering::Relaxed);
        if ema == 0 {
            return self.linger;
        }
        self.linger.min(Duration::from_micros(ema / 8))
    }

    fn note_commit_time(&self, took: Duration) {
        let us = took.as_micros() as u64;
        let old = self.commit_ema_us.load(Ordering::Relaxed);
        let new = if old == 0 { us } else { (old * 3 + us) / 4 };
        self.commit_ema_us.store(new.max(1), Ordering::Relaxed);
        self.commit_us.fetch_add(us, Ordering::Relaxed);
    }

    fn lead(&self, handle: &Arc<Mutex<Vdisk>>, batch: Vec<Arc<Ticket>>) {
        struct Guard<'a> {
            pipe: &'a Pipeline,
            handle: &'a Arc<Mutex<Vdisk>>,
            batch: &'a [Arc<Ticket>],
            finished: bool,
        }
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                if !self.finished {
                    // A panic in a commit must not leave `committing` set for ever, with
                    // every writer behind it waiting on a leader that no longer exists.
                    self.pipe.fail(
                        self.handle,
                        self.batch,
                        Fault {
                            cause: Cause::Panic,
                            error: Error::io("the commit pipeline panicked".to_string()),
                            deposed: None,
                        },
                    );
                }
            }
        }
        let mut guard = Guard { pipe: self, handle, batch: &batch, finished: false };
        let n = batch.len() as u64;
        let started = Instant::now();
        let outcome = run_batch(&batch);
        self.note_commit_time(started.elapsed());
        match outcome {
            Ok(()) => {
                self.publish(handle, &batch);
                self.batches.fetch_add(1, Ordering::Relaxed);
                self.committed.fetch_add(n, Ordering::Relaxed);
                self.largest.fetch_max(n, Ordering::Relaxed);
            }
            Err(f) => self.fail(handle, &batch, f),
        }
        guard.finished = true;
    }

    /// The batch is durable everywhere: make it visible, then let its writers go. In that
    /// order, so that a write whose reply has been sent is a write a read can see.
    fn publish(&self, handle: &Arc<Mutex<Vdisk>>, batch: &[Arc<Ticket>]) {
        {
            let mut v = handle.lock().expect("vdisk mutex poisoned");
            for t in batch {
                for &(off, len, pos) in &t.publish {
                    v.overlay.insert(off, len, pos);
                }
            }
        }
        self.finish(batch, &[], None);
    }

    fn fail(&self, handle: &Arc<Mutex<Vdisk>>, batch: &[Arc<Ticket>], f: Fault) {
        let broken = Broken {
            cause: f.cause,
            why: f.error.to_string(),
            refused: matches!(f.error, Error::Refused(_)),
        };
        // Stop new appends, and take the tickets queued behind this batch: their records are
        // in the local journal after this batch's and no replica has them, so they cannot be
        // committed (a replica that missed this batch would have a hole before them).
        let behind: Vec<Arc<Ticket>> = {
            let mut st = self.lock();
            if st.broken.is_none() {
                st.broken = Some(broken);
            }
            st.queue.drain(..).collect()
        };
        {
            let mut v = handle.lock().unwrap_or_else(|p| p.into_inner());
            match &f.deposed {
                // Deposed is not an I/O problem to retry: somebody else owns this disk now.
                Some((node, fenced)) => {
                    v.degraded = Some(format!(
                        "deposed: replica {node} is fenced at epoch {fenced}, this owner holds {}",
                        v.epoch
                    ));
                }
                None => v.mark_degraded(f.error.to_string()),
            }
            // Nothing sent them anywhere, so nothing needs them: give the sequence numbers back.
            if let Some(first) = behind.first() {
                if let Err(e) = v.journal.rollback(first.mark) {
                    eprintln!(
                        "sidon: vdisk {}: could not take back the writes queued behind a failed \
                         commit: {e}",
                        v.id
                    );
                }
            }
        }
        self.finish(batch, &behind, Some(&f.error));
    }

    /// Complete tickets, then release everyone waiting for the pipeline to change.
    fn finish(&self, batch: &[Arc<Ticket>], behind: &[Arc<Ticket>], err: Option<&Error>) {
        for t in batch.iter().chain(behind) {
            t.set(match err {
                None => Ok(()),
                Some(e) => Err(dup(e)),
            });
        }
        let n = batch.len() + behind.len();
        let mut st = self.lock();
        st.outstanding -= n;
        st.done += n as u64;
        st.committing = false;
        drop(st);
        self.cv.notify_all();
    }
}

/// A write that has been appended and will be acknowledged by `wait`.
///
/// Dropping one without waiting waits for it: a ticket nobody is waiting on would sit in
/// the queue with no one to lead its commit.
pub struct Commit {
    inner: Option<(Arc<Pipeline>, Arc<Mutex<Vdisk>>, Arc<Ticket>)>,
}

impl Commit {
    pub(super) fn done() -> Commit {
        Commit { inner: None }
    }

    pub(super) fn queued(
        pipe: Arc<Pipeline>,
        handle: Arc<Mutex<Vdisk>>,
        ticket: Arc<Ticket>,
    ) -> Commit {
        Commit { inner: Some((pipe, handle, ticket)) }
    }

    /// Block until the write is durable on every copy and visible, or has failed.
    pub fn wait(mut self) -> Result<()> {
        match self.inner.take() {
            None => Ok(()),
            Some((p, h, t)) => p.wait(&h, &t),
        }
    }
}

impl Drop for Commit {
    fn drop(&mut self) {
        if let Some((p, h, t)) = self.inner.take() {
            let _ = p.wait(&h, &t);
        }
    }
}

// ---------------------------------------------------------------------------------
// The commit itself.
// ---------------------------------------------------------------------------------

struct Fault {
    cause: Cause,
    error: Error,
    /// The replica that fenced this owner, and the epoch it is fenced at.
    deposed: Option<(String, u64)>,
}

enum ReplicaFault {
    /// Fenced at a higher epoch: this owner has been deposed.
    Stale { node: String, fenced: u64 },
    Failed(Error),
    Panicked,
    /// Stopped because another copy had already failed; says nothing about this one.
    Abandoned,
}

/// A batch's records as the requests that carry them: whole frames, concatenated, up to
/// `SEND_MAX` each. The replica appends bytes and never parses them, so this needs nothing
/// of it that a one-record request did not -- a replica from before batching takes it.
fn messages(batch: &[Arc<Ticket>]) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    let mut cur: Vec<u8> = Vec::new();
    for t in batch {
        for f in &t.frames {
            if !cur.is_empty() && cur.len() + f.len() > SEND_MAX {
                out.push(std::mem::take(&mut cur));
            }
            cur.extend_from_slice(f);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Make one batch durable: this node's journal and every replica's, concurrently. Returns
/// only when all of them have answered.
///
/// The report, when more than one thing went wrong: deposed first (it is the one that
/// matters), then a replica failure, then a local one.
fn run_batch(batch: &[Arc<Ticket>]) -> std::result::Result<(), Fault> {
    let ctx = &batch[0].ctx;
    let msgs = messages(batch);
    let failed = AtomicBool::new(false);

    let (local, remote) = std::thread::scope(|scope| {
        let workers: Vec<_> = ctx
            .replicas
            .iter()
            .map(|replica| {
                let (msgs, failed, id, epoch) = (&msgs, &failed, ctx.id.as_str(), ctx.epoch);
                scope.spawn(move || send_all(replica, id, epoch, msgs, failed))
            })
            .collect();
        // The local fsync, with the replicas working through the same bytes.
        let local = Journal::sync_file(&ctx.file, &ctx.jpath);
        if local.is_err() {
            failed.store(true, Ordering::SeqCst);
        }
        let remote: Vec<std::result::Result<(), ReplicaFault>> = workers
            .into_iter()
            .map(|w| w.join().unwrap_or(Err(ReplicaFault::Panicked)))
            .collect();
        (local, remote)
    });

    for outcome in &remote {
        if let Err(ReplicaFault::Stale { node, fenced }) = outcome {
            return Err(Fault {
                cause: Cause::Deposed,
                error: Error::refused(format!(
                    "vdisk {} is no longer owned by this node: replica {node} is fenced at \
                     epoch {fenced} and refused a write at epoch {}",
                    ctx.id, ctx.epoch
                )),
                deposed: Some((node.clone(), *fenced)),
            });
        }
    }
    for outcome in remote {
        match outcome {
            Err(ReplicaFault::Failed(e)) => {
                return Err(Fault { cause: Cause::Replica, error: e, deposed: None })
            }
            Err(ReplicaFault::Panicked) => {
                return Err(Fault {
                    cause: Cause::Replica,
                    error: Error::io("a replication thread panicked".to_string()),
                    deposed: None,
                })
            }
            _ => {}
        }
    }
    local.map_err(|e| Fault { cause: Cause::Local, error: e, deposed: None })
}

/// Send a replica a batch's messages in order, one round trip each. Every message but the
/// last defers its sync: the last is synced before it is answered and takes the rest with
/// it, so by the time this returns Ok the whole batch is on that replica's disk.
fn send_all(
    replica: &PeerClient,
    vdisk: &str,
    epoch: u64,
    msgs: &[Vec<u8>],
    failed: &AtomicBool,
) -> std::result::Result<(), ReplicaFault> {
    let last = msgs.len().saturating_sub(1);
    for (i, m) in msgs.iter().enumerate() {
        if failed.load(Ordering::SeqCst) {
            return Err(ReplicaFault::Abandoned);
        }
        let resp = replica.call(&Request {
            opcode: peer::OP_APPEND,
            vdisk: vdisk.to_string(),
            epoch,
            seq: 0,
            offset: 0,
            flags: if i == last { 0 } else { peer::APPEND_DEFER_SYNC },
            data: m.clone(),
        });
        let fault = match resp {
            Err(e) => ReplicaFault::Failed(e),
            Ok(r) if r.status == peer::ST_STALE_EPOCH => {
                ReplicaFault::Stale { node: replica.node.clone(), fenced: r.epoch }
            }
            Ok(r) if !r.is_ok() => ReplicaFault::Failed(Error::io(format!(
                "replica {} refused a journal append for {vdisk} with status {}",
                replica.node, r.status
            ))),
            Ok(_) => continue,
        };
        failed.store(true, Ordering::SeqCst);
        return Err(fault);
    }
    Ok(())
}

// ---------------------------------------------------------------------------------
// The vdisk's side: the append stage, and repair.
// ---------------------------------------------------------------------------------

impl Vdisk {
    /// The append stage of a guest write. Called with the vdisk lock held.
    ///
    /// Writes the write's records at the journal tail and queues them; makes nothing
    /// durable and nothing visible. If any record cannot be written the ones already
    /// written are taken back, so a failed append leaves no partial group in the journal.
    pub(super) fn begin_append(
        &mut self,
        offset: u64,
        data: &[u8],
    ) -> Result<Append> {
        if self.class == CLASS_IMMUTABLE {
            return Err(Error::refused(format!(
                "vdisk {} is an immutable image and cannot be written",
                self.id
            )));
        }
        if data.is_empty() {
            return Ok(Append::Nothing);
        }
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| Error::refused("write offset overflows".to_string()))?;
        if end > self.size {
            return Err(Error::refused(format!(
                "write {offset}+{} runs past the end of vdisk {} ({} bytes)",
                data.len(),
                self.id,
                self.size
            )));
        }
        match self.commit.gate(&self.id) {
            Gate::Open => {}
            Gate::Paused => return Ok(Append::Paused(Arc::clone(&self.commit))),
            Gate::Broken(e) => return Err(e),
        }

        // Records of one guest write are contiguous in the journal, and only the last
        // carries the commit marker. Replay applies the group or none of it, so a crash
        // mid-write cannot expose a prefix.
        let mark = self.journal.mark();
        let epoch = self.epoch;
        let mut frames: Vec<Vec<u8>> = Vec::with_capacity(data.len() / MAX_RECORD + 1);
        let mut publish: Vec<(u64, u32, u64)> = Vec::with_capacity(frames.capacity());
        let mut pos = 0usize;
        while pos < data.len() {
            let n = MAX_RECORD.min(data.len() - pos);
            let last = pos + n == data.len();
            let flags = if last { FLAG_COMMIT } else { 0 };
            match self.journal.append_unsynced(epoch, offset + pos as u64, flags, &data[pos..pos + n]) {
                Ok(rec) => {
                    publish.push((rec.offset, rec.data_len, rec.data_pos));
                    frames.push(rec.framed);
                }
                Err(e) => {
                    if let Err(r) = self.journal.rollback(mark) {
                        // Cannot happen with a mark taken a moment ago; if it does the
                        // journal has a partial group at its tail and must not be extended.
                        self.degraded = Some(format!("journal tail could not be repaired: {r}"));
                    }
                    return Err(e);
                }
            }
            pos += n;
        }

        let (file, jpath) = self.journal.live_file();
        let ctx = Arc::new(Ctx {
            id: self.id.clone(),
            epoch,
            file,
            jpath,
            replicas: self.replicas.clone(),
        });
        match self.commit.enqueue(frames, publish, mark, ctx) {
            Ok(t) => Ok(Append::Queued(t)),
            Err(e) => {
                // The pipeline broke between the check and the queue. Nothing was sent, and
                // the lock has been held throughout, so the tail is exactly this write.
                let _ = self.journal.rollback(mark);
                Err(e)
            }
        }
    }

    /// Repair a pipeline that broke because a replica did not take a batch.
    ///
    /// Each replica's journal is emptied and refilled with this node's, so every copy is
    /// byte-identical again and the next append lands after the same tail everywhere. That
    /// leaves a window in which a replica holds less than this node (the same exposure as a
    /// heal onto a spare), and it is the only way back: the replica that missed a batch may
    /// hold a half of it, and this protocol has no "truncate to here" request.
    ///
    /// The caller holds the vdisk lock with nothing in flight (`lock_idle`). Returns whether
    /// a repair was made. A local or deposed break is not repairable in place and says so.
    pub(super) fn recover_commit(&mut self) -> Result<bool> {
        let broken = match self.commit.broken() {
            Some(b) => b,
            None => return Ok(false),
        };
        if broken.cause != Cause::Replica {
            return Err(Error::refused(format!(
                "vdisk {} cannot be repaired in place ({:?}: {}); it has to be re-attached",
                self.id, broken.cause, broken.why
            )));
        }
        if self.commit.outstanding() != 0 {
            return Err(Error::refused(format!(
                "vdisk {} has writes in flight; it cannot be repaired now",
                self.id
            )));
        }
        let journal = self.journal.read_all()?;
        for replica in &self.replicas {
            let resp = replica.call(&Request {
                opcode: peer::OP_TRUNCATE,
                vdisk: self.id.clone(),
                epoch: self.epoch,
                seq: 0,
                offset: 0,
                flags: 0,
                data: Vec::new(),
            })?;
            if !resp.is_ok() {
                return Err(Error::io(format!(
                    "replica {} would not empty its journal for {} (status {})",
                    replica.node, self.id, resp.status
                )));
            }
            let pieces: Vec<&[u8]> = journal.chunks(SEND_MAX).collect();
            for (i, piece) in pieces.iter().enumerate() {
                let resp = replica.call(&Request {
                    opcode: peer::OP_APPEND,
                    vdisk: self.id.clone(),
                    epoch: self.epoch,
                    seq: 0,
                    offset: 0,
                    flags: if i + 1 == pieces.len() { 0 } else { peer::APPEND_DEFER_SYNC },
                    data: piece.to_vec(),
                })?;
                if !resp.is_ok() {
                    return Err(Error::io(format!(
                        "replica {} refused its journal while it was being re-synchronised for {} \
                         (status {})",
                        replica.node, self.id, resp.status
                    )));
                }
            }
        }
        self.commit.clear_broken();
        self.degraded = None;
        eprintln!(
            "sidon: vdisk {}: re-synchronised {} replica journal(s) ({} bytes); writes resume",
            self.id,
            self.replicas.len(),
            journal.len()
        );
        Ok(true)
    }
}

/// Repair a vdisk whose commit pipeline broke on a replica, if it is broken. The heal calls
/// this. Waits for a running drain and for the pipeline to empty first (`lock_idle`).
pub fn recover(handle: &Arc<Mutex<Vdisk>>) -> Result<bool> {
    super::lock_idle(handle).recover_commit()
}

/// Whether a vdisk's commit pipeline has stopped taking writes, and why.
pub fn broken_reason(v: &Vdisk) -> Option<String> {
    v.commit.broken().map(|b| b.why)
}

#[cfg(test)]
impl Vdisk {
    /// A write made entirely under the caller's hold of the vdisk: append, commit the
    /// one-ticket batch on this thread, publish. For tests that stage a state by hand (a
    /// drain planned but not finished, say) and so cannot go through the handle, which a
    /// leader would need to lock.
    pub(super) fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let ticket = match self.begin_append(offset, data)? {
            Append::Queued(t) => t,
            Append::Nothing => return Ok(()),
            Append::Paused(_) => return Err(Error::io("paused".to_string())),
        };
        let pipe = Arc::clone(&self.commit);
        let batch = {
            let mut st = pipe.lock();
            st.queue.clear();
            st.committing = true;
            vec![ticket]
        };
        match run_batch(&batch) {
            Ok(()) => {
                for t in &batch {
                    for &(off, len, pos) in &t.publish {
                        self.overlay.insert(off, len, pos);
                    }
                }
                pipe.finish(&batch, &[], None);
                Ok(())
            }
            Err(f) => {
                let e = dup(&f.error);
                pipe.finish(&batch, &[], Some(&f.error));
                Err(e)
            }
        }
    }
}

#[cfg(test)]
impl Pipeline {
    /// Make the next leader behave as if the last batch had held `n` writes.
    pub(super) fn set_expect(&self, n: usize) {
        self.lock().expect = n;
    }

    pub(super) fn expect(&self) -> usize {
        self.lock().expect
    }

    /// Pretend commits have been taking this long, so a leader's wait is `linger` rather than
    /// a fraction of however fast this build host's disk happens to be.
    pub(super) fn set_commit_ema_us(&self, us: u64) {
        self.commit_ema_us.store(us, Ordering::Relaxed);
    }
}
