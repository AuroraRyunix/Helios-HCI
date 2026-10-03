//! Group commit: concurrent writes sharing a sync and a round trip, without giving up any of
//! what a single write promised.
//!
//! Every test here drives real files, real replica sockets and the real pipeline. The
//! interesting states -- a batch in flight, writers queued behind it, a drain that wants the
//! journal still -- are reached on purpose by *holding the local sync*: the first batch's
//! `fdatasync` does not return until the test says so, so the test can watch the queue fill.

use super::*;
use crate::nbd::{testclient, Export, LocalVdisk};
use std::thread::JoinHandle;

// ---- helpers -------------------------------------------------------------------------

/// Control over the local journal sync: how many times it has been entered, which calls are
/// held until `open`, and which one fails.
struct SyncGate {
    calls: Arc<AtomicUsize>,
    release: Arc<AtomicBool>,
}

fn gate_syncs(r: &Rig, hold: usize, fail_call: Option<usize>) -> SyncGate {
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(AtomicBool::new(false));
    let (c2, r2) = (Arc::clone(&calls), Arc::clone(&release));
    journal::testhook::set(
        &jpath(r),
        Some(Arc::new(move || {
            let n = c2.fetch_add(1, Ordering::SeqCst);
            if n < hold {
                let deadline = Instant::now() + Duration::from_secs(20);
                while !r2.load(Ordering::SeqCst) {
                    if Instant::now() > deadline {
                        return Err(Error::io("the test never released the sync".to_string()));
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            if fail_call == Some(n) {
                return Err(Error::io("injected local sync failure".to_string()));
            }
            Ok(())
        })),
    );
    SyncGate { calls, release }
}

impl SyncGate {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
    fn wait_entered(&self, n: usize) {
        wait_until("the local sync to be entered", || self.calls() >= n);
    }
    fn open(&self) {
        self.release.store(true, Ordering::SeqCst);
    }
}

fn spawn_write(r: &Rig, off: u64, data: Vec<u8>) -> JoinHandle<Result<()>> {
    let vd = Arc::clone(&r.vd);
    std::thread::spawn(move || write_through(&vd, off, &data))
}

fn outstanding(r: &Rig) -> usize {
    r.vd.lock().unwrap().commit.outstanding()
}

fn wait_outstanding(r: &Rig, n: usize) {
    wait_until("writes to queue behind the batch in flight", || outstanding(r) >= n);
}

struct Rec {
    seq: u64,
    off: u64,
    flags: u32,
    data: Vec<u8>,
    /// Byte offset in the journal at which this record ends.
    end: usize,
}

fn parse(bytes: &[u8]) -> Vec<Rec> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + journal::HEADER_LEN <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
        let end = pos + journal::HEADER_LEN + len;
        if end > bytes.len() {
            break;
        }
        out.push(Rec {
            seq: u64::from_le_bytes(bytes[pos + 8..pos + 16].try_into().unwrap()),
            off: u64::from_le_bytes(bytes[pos + 24..pos + 32].try_into().unwrap()),
            flags: u32::from_le_bytes(bytes[pos + 32..pos + 36].try_into().unwrap()),
            data: bytes[pos + journal::HEADER_LEN..end].to_vec(),
            end,
        });
        pos = end;
    }
    out
}

/// Records split into the groups the commit markers delimit. A trailing group with no marker
/// is returned too, so a test can see it.
fn groups(recs: &[Rec]) -> Vec<Vec<&Rec>> {
    let mut out: Vec<Vec<&Rec>> = Vec::new();
    let mut cur: Vec<&Rec> = Vec::new();
    for r in recs {
        cur.push(r);
        if r.flags & FLAG_COMMIT != 0 {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn local_journal(r: &Rig) -> Vec<u8> {
    std::fs::read(jpath(r)).unwrap()
}

const BIG: u64 = 64 * MIB as u64;
const BIGGER: u64 = 128 * MIB as u64;

// ================================================================================
// Batching
// ================================================================================

#[test]
fn writes_in_flight_together_share_one_sync_and_one_replica_round_trip() {
    let r = rig("gc-share", 1, BIG, BIGGER);
    let gate = gate_syncs(&r, 1, None);
    let appends = record_append_flags(&r.replicas[0]);

    // One write is committing (its sync is held); fifteen more arrive meanwhile.
    let mut writers = vec![spawn_write(&r, 0, fill(0, 4096))];
    gate.wait_entered(1);
    for i in 1..16u64 {
        writers.push(spawn_write(&r, i * 8192, fill(i as u8, 4096)));
    }
    wait_outstanding(&r, 16);
    gate.open();
    for w in writers {
        w.join().unwrap().expect("every write is acknowledged");
    }

    assert_eq!(gate.calls(), 2, "one sync for the first write, one for the fifteen behind it");
    assert_eq!(appends.lock().unwrap().len(), 2, "and one round trip to the replica for each");
    let stats = r.vd.lock().unwrap().commit.stats();
    assert_eq!(stats["batches"], 2);
    assert_eq!(stats["writes_committed"], 16);
    assert_eq!(stats["largest_batch"], 15);
    for i in 0..16u64 {
        assert_eq!(r.read(i * 8192, 4096), fill(i as u8, 4096), "write {i} is visible");
    }
    let local = local_journal(&r);
    assert_eq!(r.replicas[0].journal(), local, "the replica holds exactly the owner's journal");
    let recs = parse(&local);
    assert_eq!(recs.iter().map(|x| x.seq).collect::<Vec<_>>(), (0..16).collect::<Vec<_>>());
    assert!(recs.iter().all(|x| x.flags == FLAG_COMMIT), "sixteen single-record groups");
    journal::testhook::set(&jpath(&r), None);
}

#[test]
fn one_write_is_a_batch_of_one_and_costs_what_it_always_did() {
    let r = rig("gc-one", 1, BIG, BIGGER);
    let syncs = count_syncs(&r);
    let appends = record_append_flags(&r.replicas[0]);
    r.write(0, &fill(1, 4096)).unwrap();
    assert_eq!(syncs.load(Ordering::SeqCst), 1);
    assert_eq!(*appends.lock().unwrap(), vec![0], "one request, synced before it is answered");
    let stats = r.vd.lock().unwrap().commit.stats();
    assert_eq!((stats["batches"].as_u64(), stats["largest_batch"].as_u64()), (Some(1), Some(1)));
    journal::testhook::set(&jpath(&r), None);
}

#[test]
fn a_batch_reaches_the_replica_as_whole_frames_in_requests_a_replica_of_any_age_takes() {
    // The wire protocol did not change: a request's payload is journal bytes, and a batch is
    // one with several frames in it. Checked from the replica's side, for a replica that
    // ignores the defer-sync flag (every replica from before it) and writes what it is given.
    let r = rig("gc-wire", 1, BIG, BIGGER);
    let seen: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let (s2, store) = (Arc::clone(&seen), Arc::clone(&r.replicas[0].store));
    r.replicas[0].set_hook(Some(Arc::new(move |req| {
        if req.opcode != peer::OP_APPEND {
            return None;
        }
        s2.lock().unwrap().push(req.data.clone());
        // An old replica: no notion of deferring, always syncs.
        Some(match store.append_deferring(&req.vdisk, req.epoch, &req.data, false) {
            Ok(()) => Response::ok(Vec::new()),
            Err(_) => Response::err(peer::ST_IO, 0),
        })
    })));
    let gate = gate_syncs(&r, 1, None);
    let mut writers = vec![spawn_write(&r, 0, fill(0, 4096))];
    gate.wait_entered(1);
    for i in 1..6u64 {
        writers.push(spawn_write(&r, i * 4 * MIB as u64 % (12 * MIB as u64), fill(i as u8, (i as usize) * MIB)));
    }
    wait_outstanding(&r, 6);
    gate.open();
    for w in writers {
        w.join().unwrap().unwrap();
    }
    for msg in seen.lock().unwrap().iter() {
        let recs = parse(msg);
        assert!(!recs.is_empty());
        assert_eq!(recs.last().unwrap().end, msg.len(), "a request holds whole frames and nothing else");
        assert!(msg.len() <= 4 * MIB + MIB, "bounded: a request is at most {} bytes", 5 * MIB);
    }
    assert_eq!(r.replicas[0].journal(), local_journal(&r));
    journal::testhook::set(&jpath(&r), None);
}

#[test]
fn concurrent_writes_cost_a_few_batches_not_one_round_trip_each() {
    // Each side takes 60 ms per request. Sixteen writes made one at a time cost 16 x 60 ms;
    // sharing commits they cost a few. The bound is loose on purpose: it is the shape that
    // is under test, not the speed of the build host.
    let r = rig("gc-latency", 1, BIG, BIGGER);
    r.replicas[0].set_hook(Some(Arc::new(|req| {
        if req.opcode == peer::OP_APPEND {
            std::thread::sleep(Duration::from_millis(60));
        }
        None
    })));
    journal::testhook::set(&jpath(&r), Some(Arc::new(|| {
        std::thread::sleep(Duration::from_millis(60));
        Ok(())
    })));
    let t = Instant::now();
    let writers: Vec<_> = (0..16u64).map(|i| spawn_write(&r, i * 4096, fill(i as u8, 4096))).collect();
    for w in writers {
        w.join().unwrap().unwrap();
    }
    let took = t.elapsed();
    assert!(took < Duration::from_millis(16 * 60 * 2 / 3), "16 concurrent writes took {took:?}");
    journal::testhook::set(&jpath(&r), None);
}

// ================================================================================
// Failure: one batch fails, every write in it fails
// ================================================================================

#[test]
fn a_failed_local_sync_fails_every_write_in_the_batch_and_none_of_them_is_visible() {
    let r = rig("gc-sync-fails", 1, BIG, BIGGER);
    // The first batch (one write) syncs fine; the second (seven writes) does not.
    let gate = gate_syncs(&r, 1, Some(1));
    let first = spawn_write(&r, 0, fill(9, 4096));
    gate.wait_entered(1);
    let rest: Vec<_> = (1..8u64).map(|i| spawn_write(&r, i * 8192, fill(i as u8, 4096))).collect();
    wait_outstanding(&r, 8);
    gate.open();

    first.join().unwrap().expect("the batch that was synced is acknowledged");
    for (i, w) in rest.into_iter().enumerate() {
        let err = w.join().unwrap().expect_err("the batch it was in failed");
        assert!(err.to_string().contains("injected local sync failure"), "write {}: {err}", i + 1);
    }
    assert_eq!(r.read(0, 4096), fill(9, 4096), "what was acknowledged is there");
    for i in 1..8u64 {
        assert_eq!(r.read(i * 8192, 4096), vec![0u8; 4096], "write {i} was not acknowledged: not visible");
    }
    assert!(r.degraded().unwrap().contains("injected local sync failure"));
    journal::testhook::set(&jpath(&r), None);

    // The vdisk takes no further appends, and a flush cannot claim anything is durable.
    let err = r.write(0, &fill(1, 4096)).expect_err("fail closed");
    assert!(err.to_string().contains("cannot take writes"), "{err}");
    assert!(flush_through(&r.vd).is_err());
    // What was acknowledged is still readable.
    assert_eq!(r.read(0, 4096), fill(9, 4096));
}

#[test]
fn a_replica_failure_mid_batch_fails_the_whole_batch_and_the_writes_queued_behind_it() {
    let r = rig("gc-replica-fails", 1, BIG, BIGGER);
    // The replica refuses its first append, which is the first batch's.
    let arrivals = Arc::new(AtomicUsize::new(0));
    r.replicas[0].set_hook(Some(Arc::new(move |req| {
        (req.opcode == peer::OP_APPEND && arrivals.fetch_add(1, Ordering::SeqCst) == 0)
            .then(|| Response::err(peer::ST_IO, 0))
    })));
    let gate = gate_syncs(&r, 1, None);
    let first = spawn_write(&r, 0, fill(1, 4096));
    gate.wait_entered(1);
    let behind: Vec<_> = (1..4u64).map(|i| spawn_write(&r, i * 8192, fill(i as u8, 4096))).collect();
    wait_outstanding(&r, 4);
    gate.open();

    let err = first.join().unwrap().expect_err("write-all: the replica refused");
    assert!(err.to_string().contains("refused a journal append"), "{err}");
    for w in behind {
        w.join().unwrap().expect_err("queued behind a failed batch, so it fails with it");
    }
    for i in 0..4u64 {
        assert_eq!(r.read(i * 8192, 4096), vec![0u8; 4096], "nothing in the failed batch is visible");
    }
    assert!(!r.vd.lock().unwrap().needs_drain());
    assert!(r.degraded().unwrap().contains("refused a journal append"), "flagged as a replica failure");
    // The writes behind the batch were never sent anywhere, so their records are taken back
    // out of the journal: only the failed batch's own record is left.
    {
        let v = r.vd.lock().unwrap();
        assert_eq!(v.journal.next_seq(), 1);
        assert_eq!(v.journal.len(), (journal::HEADER_LEN + 4096) as u64);
    }
    journal::testhook::set(&jpath(&r), None);

    // Fail closed.
    let err = r.write(0, &fill(2, 4096)).expect_err("no appends until repaired");
    assert!(err.to_string().contains("cannot take writes"), "{err}");

    // The heal repairs it in place: the replica's journal is made the owner's again.
    assert!(recover(&r.vd).unwrap(), "a replica-side break is repairable");
    assert!(r.degraded().is_none());
    assert_eq!(r.replicas[0].journal(), local_journal(&r));
    r.write(0, &fill(3, 4096)).expect("writes resume");
    assert_eq!(r.read(0, 4096), fill(3, 4096));
    let recs = parse(&r.replicas[0].journal());
    assert_eq!(
        recs.iter().map(|x| x.seq).collect::<Vec<_>>(),
        vec![0, 1],
        "no duplicate and no gap in sequence across the failure and the repair"
    );
    assert_eq!(r.replicas[0].journal(), local_journal(&r));
}

#[test]
fn with_two_replicas_a_repair_makes_both_identical_to_the_owner() {
    let r = rig("gc-repair-two", 2, BIG, BIGGER);
    r.write(0, &fill(1, 4096)).unwrap();
    // r1 takes the next batch's first request and then fails the second.
    let arrivals = Arc::new(AtomicUsize::new(0));
    r.replicas[1].set_hook(Some(Arc::new(move |req| {
        (req.opcode == peer::OP_APPEND && arrivals.fetch_add(1, Ordering::SeqCst) == 1)
            .then(|| Response::err(peer::ST_IO, 0))
    })));
    let err = r.write(8192, &fill(2, 6 * MIB)).expect_err("two requests; the second fails on r1");
    assert!(err.to_string().contains("r1"), "{err}");
    // r0 took all of it, r1 a prefix: neither can be appended to as it is.
    assert!(r.replicas[1].journal().len() < r.replicas[0].journal().len());
    r.replicas[1].set_hook(None);

    assert!(recover(&r.vd).unwrap());
    let local = local_journal(&r);
    assert_eq!(r.replicas[0].journal(), local);
    assert_eq!(r.replicas[1].journal(), local);
    r.write(0, &fill(5, 4096)).unwrap();
    assert_eq!(r.replicas[0].journal(), local_journal(&r));
    assert_eq!(r.replicas[1].journal(), local_journal(&r));
}

#[test]
fn a_stale_epoch_deposes_the_owner_for_every_write_in_the_batch() {
    let r = rig("gc-deposed", 1, BIG, BIGGER);
    r.replicas[0].store.fence("vd", 9).unwrap();
    let gate = gate_syncs(&r, 1, None);
    let first = spawn_write(&r, 0, fill(1, 4096));
    gate.wait_entered(1);
    let rest: Vec<_> = (1..6u64).map(|i| spawn_write(&r, i * 8192, fill(i as u8, 4096))).collect();
    wait_outstanding(&r, 6);
    gate.open();

    let mut all = vec![first];
    all.extend(rest);
    for (i, w) in all.into_iter().enumerate() {
        let err = w.join().unwrap().expect_err("a deposed owner acknowledges nothing");
        assert!(matches!(err, Error::Refused(_)), "write {i}: {err:?}");
    }
    let why = r.degraded().unwrap();
    assert!(why.contains("deposed") && why.contains("fenced at epoch 9"), "{why}");
    for i in 0..6u64 {
        assert_eq!(r.read(i * 8192, 4096), vec![0u8; 4096]);
    }
    assert!(r.replicas[0].journal().is_empty(), "nothing reached the new owner's journal");
    journal::testhook::set(&jpath(&r), None);
    // Not something a heal can undo: somebody else owns the disk.
    let err = recover(&r.vd).expect_err("a deposed owner stays deposed");
    assert!(err.to_string().contains("re-attached"), "{err}");
    let err = r.write(0, &fill(2, 4096)).expect_err("and keeps being refused");
    assert!(matches!(err, Error::Refused(_)), "{err:?}");
}

// ================================================================================
// Order: the replica's journal is the owner's, and overlapping writes have an order
// ================================================================================

/// A small deterministic generator, so a failing run can be repeated.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

#[test]
fn many_concurrent_writers_leave_the_replicas_with_the_owners_journal_byte_for_byte() {
    let r = rig("gc-order", 2, 512 * MIB as u64, 1024 * MIB as u64);
    const THREADS: u64 = 6;
    const REGION: u64 = 5 * MIB as u64 / 2; // 2.5 MiB each
    let sizes = [512usize, 4096, 100_000, MIB + MIB / 2, 2 * MIB + MIB / 2];

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let vd = Arc::clone(&r.vd);
        handles.push(std::thread::spawn(move || {
            let mut rng = Lcg(t + 1);
            let mut last: Option<(usize, u8)> = None;
            for n in 0..20u8 {
                let size = sizes[(rng.next() % sizes.len() as u64) as usize];
                let seed = (t as u8) * 40 + n;
                write_through(&vd, t * REGION, &fill(seed, size)).unwrap();
                last = Some((size, seed));
            }
            last.unwrap()
        }));
    }
    let lasts: Vec<(usize, u8)> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    let local = local_journal(&r);
    for replica in &r.replicas {
        assert_eq!(replica.journal(), local, "a replica's journal is the owner's");
    }
    let recs = parse(&local);
    assert_eq!(recs.iter().map(|x| x.seq).collect::<Vec<_>>(), (0..recs.len() as u64).collect::<Vec<_>>());
    // Records of one guest write are contiguous: a group is consecutive megabytes of one
    // thread's region, ending in its commit marker. Two writes interleaved would break it.
    for g in groups(&recs) {
        assert!(g.last().unwrap().flags & FLAG_COMMIT != 0, "every group is terminated");
        let region = g[0].off / REGION;
        for (k, rec) in g.iter().enumerate() {
            assert_eq!(rec.off, g[0].off + (k * MIB) as u64, "records of one write are consecutive");
            assert_eq!(rec.off / REGION, region, "and belong to one writer");
            assert_eq!(rec.flags & FLAG_COMMIT != 0, k + 1 == g.len(), "only the last commits");
        }
    }
    for (t, (size, seed)) in lasts.iter().enumerate() {
        assert_eq!(r.read(t as u64 * REGION, *size as u32), fill(*seed, *size), "thread {t}'s last write");
    }
}

#[test]
fn concurrent_writers_to_overlapping_ranges_apply_in_journal_order_in_memory_and_after_replay() {
    for (round_len, label) in [(4096usize, "single record"), (MIB + MIB / 2, "two records")] {
        let r = rig(&format!("gc-overlap-{round_len}"), 1, BIG, BIGGER);
        let hydra = Hydra::start();
        let replay_dir = tmpdir(&format!("gc-overlap-replay-{round_len}"));
        let mut replayed = build(&replay_dir, &hydra, Vec::new(), BIG, BIGGER);
        for round in 0..12u8 {
            let writers: Vec<_> = (0..8u8)
                .map(|i| spawn_write(&r, 0, fill(round * 8 + i, round_len)))
                .collect();
            for w in writers {
                w.join().unwrap().unwrap();
            }
            // Which write came last, according to the journal the replica holds.
            let replica = r.replicas[0].journal();
            let recs = parse(&replica);
            let gs = groups(&recs);
            let winner = gs.last().unwrap()[0].data[0];

            let got = r.read(0, round_len as u32);
            assert_eq!(
                got,
                fill(winner, round_len),
                "{label}, round {round}: the write the journal ends with is the one that wins, whole"
            );
            // And a restart that replays that journal agrees.
            replayed.journal.replace(&replica).unwrap();
            replayed.overlay.clear();
            replayed.replay_journal().unwrap();
            assert_eq!(
                replayed.read(0, round_len as u32).unwrap(),
                got,
                "{label}, round {round}: replay reproduces the order"
            );
        }
        let _ = std::fs::remove_dir_all(&replay_dir);
    }
}

// ================================================================================
// Flush
// ================================================================================

#[test]
fn a_flush_does_not_return_while_an_earlier_write_is_still_between_its_append_and_its_commit() {
    let r = rig("gc-flush", 1, BIG, BIGGER);
    // The replica holds the write's request until released.
    let hold = Arc::new(AtomicBool::new(true));
    let h2 = Arc::clone(&hold);
    r.replicas[0].set_hook(Some(Arc::new(move |req| {
        if req.opcode == peer::OP_APPEND {
            let deadline = Instant::now() + Duration::from_secs(20);
            while h2.load(Ordering::SeqCst) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        None
    })));

    // Appended, not waited for: this is what an NBD reader does with a write.
    let pending = submit_write(&r.vd, 0, &fill(7, 4096)).unwrap();
    let vd = Arc::clone(&r.vd);
    let flushed = Arc::new(AtomicBool::new(false));
    let f2 = Arc::clone(&flushed);
    let flusher = std::thread::spawn(move || {
        let out = flush_through(&vd);
        f2.store(true, Ordering::SeqCst);
        out
    });
    std::thread::sleep(Duration::from_millis(300));
    assert!(!flushed.load(Ordering::SeqCst), "the flush waited for the write ahead of it");
    assert_eq!(r.read(0, 4096), vec![0u8; 4096], "and the write is still not visible");

    hold.store(false, Ordering::SeqCst);
    flusher.join().unwrap().expect("the flush covers a write that then succeeded");
    pending.wait().unwrap();
    assert_eq!(r.read(0, 4096), fill(7, 4096), "after the flush, the write it covered is durable and visible");
    // With nothing in flight a flush is immediate.
    let t = Instant::now();
    flush_through(&r.vd).unwrap();
    assert!(t.elapsed() < Duration::from_millis(1000));
}

#[test]
fn a_flush_does_not_wait_for_writes_submitted_after_it() {
    let r = rig("gc-flush-later", 0, BIG, BIGGER);
    r.write(0, &fill(1, 4096)).unwrap();
    // Nothing earlier is outstanding, so the barrier is already met however much comes after.
    let t = Instant::now();
    flush_through(&r.vd).unwrap();
    assert!(t.elapsed() < Duration::from_millis(1000));
}

// ================================================================================
// Crash recovery
// ================================================================================

#[test]
fn recovery_replays_acknowledged_groups_whole_and_never_a_torn_one() {
    // A series of acknowledged writes, some of several records, written concurrently so the
    // journal is whatever order the pipeline produced. Then a crash at every kind of place:
    // the journal cut at record boundaries, one byte either side of them, and mid-record.
    // What replay must give is exactly the groups whose marker survived, in order.
    let r = rig("gc-crash", 1, BIG, BIGGER);
    let specs: Vec<(u64, usize, u8)> = vec![
        (0, 4096, 1),
        (MIB as u64, MIB + MIB / 2, 2),
        (10, 512, 3),
        (4 * MIB as u64, 3 * MIB, 4),
        (100_000, 8192, 5),
        (9 * MIB as u64 + 5, 2 * MIB + 7, 6),
        (7 * MIB as u64, 4096, 7),
    ];
    // Sequentially submitted in this order, committed together where they can be.
    let commits: Vec<Commit> = specs
        .iter()
        .map(|(off, len, seed)| submit_write(&r.vd, *off, &fill(*seed, *len)).unwrap())
        .collect();
    for c in commits {
        c.wait().unwrap();
    }
    let bytes = local_journal(&r);
    assert_eq!(r.replicas[0].journal(), bytes);
    let recs = parse(&bytes);
    let gs = groups(&recs);
    assert_eq!(gs.len(), specs.len());
    let group_ends: Vec<usize> = gs.iter().map(|g| g.last().unwrap().end).collect();

    let hydra = Hydra::start();
    let torn_dir = tmpdir("gc-crash-replay");
    let mut torn = build(&torn_dir, &hydra, Vec::new(), BIG, BIGGER);
    let mut cuts: Vec<usize> = (0..bytes.len()).step_by(100_003).collect();
    for rec in &recs {
        for d in [-1i64, 0, 1, 39, 40, 41] {
            let c = rec.end as i64 + d;
            if c >= 0 && (c as usize) <= bytes.len() {
                cuts.push(c as usize);
            }
        }
    }
    cuts.push(bytes.len());
    cuts.sort_unstable();
    cuts.dedup();

    for cut in cuts {
        torn.journal.replace(&bytes[..cut]).unwrap();
        torn.overlay.clear();
        torn.replay_journal().unwrap();

        let mut model = Model::new();
        let mut kept = 0;
        for (i, (off, len, seed)) in specs.iter().enumerate() {
            if group_ends[i] <= cut {
                model.put(*off, &fill(*seed, *len));
                kept += 1;
            }
        }
        let got = torn.read(0, (12 * MIB) as u32).unwrap();
        assert!(
            got == model.slice(0, 12 * MIB),
            "a crash after {cut} of {} journal bytes must leave exactly the {kept} groups whose \
             marker is on disk",
            bytes.len()
        );
    }
    let _ = std::fs::remove_dir_all(&torn_dir);
}

// ================================================================================
// The drain and the pipeline
// ================================================================================

#[test]
fn a_drain_waits_for_the_batch_in_flight_and_nothing_acknowledged_is_lost_across_it() {
    let r = rig("gc-drain-waits", 1, MIB as u64, 64 * MIB as u64);
    let mut model = Model::new();
    r.write(0, &fill(1, 2 * MIB)).unwrap();
    model.put(0, &fill(1, 2 * MIB));
    r.settle();

    // A write is committing (its sync is held) when a drain wants to rotate the journal.
    let gate = gate_syncs(&r, 1, None);
    let writer = spawn_write(&r, 3 * MIB as u64, fill(2, 4096));
    gate.wait_entered(1);
    let vd = Arc::clone(&r.vd);
    let drain = std::thread::spawn(move || drain_all(&vd));
    std::thread::sleep(Duration::from_millis(300));
    assert!(!r.sealed_exists(), "the journal was not rotated under a batch in flight");
    assert!(!drain.is_finished(), "the drain is waiting for the batch, not skipping it");

    // A write that arrives now is held off rather than appended behind the drain's back.
    let late = spawn_write(&r, 5 * MIB as u64, fill(3, 4096));
    std::thread::sleep(Duration::from_millis(100));
    assert!(!late.is_finished());

    gate.open();
    writer.join().unwrap().unwrap();
    model.put(3 * MIB as u64, &fill(2, 4096));
    drain.join().unwrap().expect("the drain runs once the batch is out of the way");
    late.join().unwrap().unwrap();
    model.put(5 * MIB as u64, &fill(3, 4096));
    drain_all(&r.vd).unwrap();
    r.settle();

    assert_eq!(r.read(0, 8 * MIB as u32), model.slice(0, 8 * MIB), "every acknowledged write survived the rotation");
    assert!(!r.sealed_exists());
    journal::testhook::set(&jpath(&r), None);
}

#[test]
fn writers_and_repeated_drains_together_lose_nothing_and_leave_a_clean_replica_journal() {
    // A high-water mark of 1 MiB with 64 KiB writes: a drain is wanted every sixteen writes,
    // while four writers keep the queue busy. Each rotation has to land between batches.
    let r = rig("gc-drain-storm", 1, MIB as u64, 16 * MIB as u64);
    let vd = Arc::clone(&r.vd);
    let outcome = within(120, move || {
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let vd = Arc::clone(&vd);
            handles.push(std::thread::spawn(move || {
                let mut last = 0u8;
                for n in 0..60u8 {
                    // 64 KiB writes into the thread's own 1 MiB region, wrapping.
                    let off = t * MIB as u64 + (n as u64 % 16) * 65536;
                    last = n;
                    write_through(&vd, off, &fill(t as u8 * 60 + n, 65536)).unwrap();
                }
                last
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect::<Vec<u8>>()
    });
    assert_eq!(outcome, vec![59, 59, 59, 59]);
    drain_all(&r.vd).unwrap();
    r.settle();

    for t in 0..4u64 {
        // The last write to each 64 KiB slot of a region is the highest n with n % 16 == slot.
        for slot in 0..16u64 {
            let n = (0..60u64).filter(|n| n % 16 == slot).max().unwrap() as u8;
            assert_eq!(
                r.read(t * MIB as u64 + slot * 65536, 65536),
                fill(t as u8 * 60 + n, 65536),
                "thread {t} slot {slot}"
            );
        }
    }
    // What the replica still holds after all those truncations is a clean, gap-free stream.
    let recs = parse(&r.replicas[0].journal());
    for pair in recs.windows(2) {
        assert_eq!(pair[1].seq, pair[0].seq + 1, "no hole in the replica's journal");
    }
    let stats = r.vd.lock().unwrap().commit.stats();
    assert!(stats["batches"].as_u64().unwrap() <= stats["writes_committed"].as_u64().unwrap());
}

// ================================================================================
// Over NBD: queue depth shares commits
// ================================================================================

#[test]
fn a_guest_at_queue_depth_sixteen_gets_its_writes_committed_together() {
    let r = rig("gc-nbd-qd", 1, BIG, BIGGER);
    r.replicas[0].set_hook(Some(Arc::new(|req| {
        if req.opcode == peer::OP_APPEND {
            std::thread::sleep(Duration::from_millis(40));
        }
        None
    })));
    let syncs = Arc::new(AtomicUsize::new(0));
    let s2 = Arc::clone(&syncs);
    journal::testhook::set(&jpath(&r), Some(Arc::new(move || {
        s2.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(40));
        Ok(())
    })));

    let export = Arc::new(Export { backend: Arc::new(LocalVdisk(Arc::clone(&r.vd))), name: "vd".into() });
    let (mut c, server) = testclient::connect(export);
    let t = Instant::now();
    for i in 0..32u64 {
        c.write(i + 1, i * 8192, &fill(i as u8, 4096));
    }
    for _ in 0..32 {
        assert_eq!(c.recv().errno, 0);
    }
    let took = t.elapsed();
    // A flush behind them, and reads that see all of it.
    c.flush(100);
    assert_eq!(c.recv().errno, 0);
    for i in 0..32u64 {
        c.read(200 + i, i * 8192, 4096);
        let rep = c.recv();
        assert_eq!(rep.data, fill(i as u8, 4096), "write {i}");
    }
    c.disconnect();
    server.join().unwrap().unwrap();

    assert!(syncs.load(Ordering::SeqCst) <= 12, "32 writes took {} syncs", syncs.load(Ordering::SeqCst));
    assert!(took < Duration::from_millis(32 * 40 * 7 / 10), "32 writes at depth 32 took {took:?}");
    let recs = parse(&r.replicas[0].journal());
    assert_eq!(recs.len(), 32);
    // Arrival order is journal order.
    assert_eq!(recs.iter().map(|x| x.off).collect::<Vec<_>>(), (0..32).map(|i| i * 8192).collect::<Vec<u64>>());
    journal::testhook::set(&jpath(&r), None);
}

#[test]
fn nbd_writes_to_one_range_are_applied_in_the_order_they_arrived() {
    let r = rig("gc-nbd-order", 1, BIG, BIGGER);
    let export = Arc::new(Export { backend: Arc::new(LocalVdisk(Arc::clone(&r.vd))), name: "vd".into() });
    let (mut c, server) = testclient::connect(export);
    for i in 0..20u64 {
        c.write(i + 1, 0, &fill(i as u8, 4096));
    }
    for _ in 0..20 {
        assert_eq!(c.recv().errno, 0);
    }
    c.read(99, 0, 4096);
    assert_eq!(c.recv().data, fill(19, 4096), "the last write to arrive is the one that stands");
    c.disconnect();
    server.join().unwrap().unwrap();
}

// ================================================================================
// Waiting for the rest of a burst
// ================================================================================

#[test]
fn a_leader_waits_for_the_rest_of_a_burst_the_last_batch_says_to_expect() {
    // The last batch held three writes, so a leader that finds only one queued waits for the
    // others rather than committing it alone and leaving them for a second round.
    let r = rig("gc-linger", 1, BIG, BIGGER);
    {
        let mut v = r.vd.lock().unwrap();
        v.commit = commit::Pipeline::with_linger(Duration::from_secs(5));
        v.commit.set_expect(3);
    }
    let syncs = count_syncs(&r);
    let first = spawn_write(&r, 0, fill(1, 4096));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(syncs.load(Ordering::SeqCst), 0, "the leader is waiting, not committing one write");
    assert!(!first.is_finished());

    let t = Instant::now();
    let rest = [spawn_write(&r, 8192, fill(2, 4096)), spawn_write(&r, 16384, fill(3, 4096))];
    first.join().unwrap().unwrap();
    for w in rest {
        w.join().unwrap().unwrap();
    }
    assert!(t.elapsed() < Duration::from_secs(3), "it went as soon as the burst was in");
    assert_eq!(syncs.load(Ordering::SeqCst), 1, "one sync for the three");
    let stats = r.vd.lock().unwrap().commit.stats();
    assert_eq!((stats["batches"].as_u64(), stats["largest_batch"].as_u64()), (Some(1), Some(3)));
    journal::testhook::set(&jpath(&r), None);
}

#[test]
fn a_leader_does_not_wait_longer_than_the_linger_for_a_burst_that_does_not_come() {
    let r = rig("gc-linger-gives-up", 0, BIG, BIGGER);
    {
        let mut v = r.vd.lock().unwrap();
        v.commit = commit::Pipeline::with_linger(Duration::from_millis(200));
        v.commit.set_expect(8);
    }
    let t = Instant::now();
    r.write(0, &fill(1, 4096)).unwrap();
    let took = t.elapsed();
    assert!(took >= Duration::from_millis(190), "it waited for the burst: {took:?}");
    assert!(took < Duration::from_secs(2), "and then went without it: {took:?}");
    // The expectation is reset to what actually arrived, so the next leader does not wait for
    // a burst that was never there.
    assert_eq!(r.vd.lock().unwrap().commit.expect(), 1);
}

#[test]
fn queue_depth_one_never_waits() {
    // A wait of a second per write if one were wrongly taken: twenty of them would be twenty
    // seconds, where the writes themselves (no replica, one sync each) take a fraction of one.
    let r = rig("gc-linger-qd1", 0, BIG, BIGGER);
    {
        let mut v = r.vd.lock().unwrap();
        v.commit = commit::Pipeline::with_linger(Duration::from_secs(5));
        v.commit.set_commit_ema_us(8_000_000);
    }
    let t = Instant::now();
    for i in 0..20u64 {
        r.write(i * 4096, &fill(i as u8, 4096)).unwrap();
    }
    assert!(t.elapsed() < Duration::from_secs(8), "20 sequential writes took {:?}", t.elapsed());
    let stats = r.vd.lock().unwrap().commit.stats();
    assert_eq!((stats["batches"].as_u64(), stats["largest_batch"].as_u64()), (Some(20), Some(1)));
}

// ================================================================================
// A commit that panics
// ================================================================================

#[test]
fn a_panic_in_a_commit_fails_the_batch_and_the_writes_behind_it_and_wedges_nothing() {
    let r = rig("gc-panic", 0, BIG, BIGGER);
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let (e2, r2) = (Arc::clone(&entered), Arc::clone(&release));
    journal::testhook::set(
        &jpath(&r),
        Some(Arc::new(move || {
            e2.store(true, Ordering::SeqCst);
            while !r2.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(1));
            }
            panic!("injected panic in a commit");
        })),
    );
    // The first writer leads and panics in its sync; the second is queued behind it.
    let first = spawn_write(&r, 0, fill(1, 4096));
    wait_until("the sync to be entered", || entered.load(Ordering::SeqCst));
    let second = spawn_write(&r, 8192, fill(2, 4096));
    wait_outstanding(&r, 2);
    release.store(true, Ordering::SeqCst);

    assert!(first.join().is_err(), "the leader's own thread panicked");
    let err = second
        .join()
        .expect("the writer behind it did not panic")
        .expect_err("and was told its write failed rather than being left waiting");
    assert!(err.to_string().contains("panicked"), "{err}");
    journal::testhook::set(&jpath(&r), None);

    assert_eq!(outstanding(&r), 0, "nothing is left in flight");
    assert_eq!(r.read(0, 4096), vec![0u8; 4096]);
    assert_eq!(r.read(8192, 4096), vec![0u8; 4096]);
    let again = within(10, {
        let vd = Arc::clone(&r.vd);
        move || write_through(&vd, 0, &fill(3, 4096))
    });
    assert!(again.is_err(), "a pipeline that panicked fails closed");
}
