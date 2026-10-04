//! The switch a live migration hands a vdisk over through.
//!
//! A guest that has just resumed on a new host talks to that host's Sidon over an NBD
//! socket which was bound *before* the migration, with a forwarder behind it
//! ([`crate::peer::Forwarder`]): every request is relayed to the node that still owns the
//! disk, so the single writer never changes while two qemu processes have the export open.
//! Once the guest is running here, ownership follows. The NBD session qemu holds cannot be
//! reconnected for it, so the backend behind the session has to change under it. That is
//! all this type is: a backend that forwards until it is told to serve locally, and that
//! can be **stalled** so the change happens at an instant no request straddles.
//!
//! ## Why a stall and not a lock around each request
//!
//! The handover is: stop guest I/O here, ask the owner to drain and stop serving, win the
//! compare-and-swap in Hydra, fence the journal replicas, open the vdisk, install it, let
//! I/O go. A request admitted between "the owner stopped serving" and "the vdisk is
//! installed" would be forwarded to a node that no longer owns the disk, and the guest would
//! see an error for something that was merely in transit. So `stall()` first refuses to
//! admit new requests and then waits for those already admitted to finish -- a write that was
//! acknowledged by the owner is therefore on every replica before the owner is asked to
//! hand over, and a write that has not been admitted yet waits and is applied by the new
//! owner. Nothing is dropped and nothing is applied twice.
//!
//! `begin_write` keeps its contract through the switch: the append is begun, in the
//! reader's order, on whichever backend is current when the request is *admitted*, and the
//! stall waits for the returned closure to run, not merely for the call to return.

use std::sync::{Arc, Condvar, Mutex, OnceLock};

use crate::err::{Error, Result};
use crate::nbd::{Backend, Pending};

struct Gate {
    /// A handover is in progress: admit nothing new.
    stalled: bool,
    /// Requests admitted and not yet finished.
    inflight: usize,
}

pub struct Switch {
    forwarding: Arc<dyn Backend>,
    owned: OnceLock<Arc<dyn Backend>>,
    gate: Mutex<Gate>,
    cv: Condvar,
}

/// One admitted request. Dropping it is what the stall waits for.
struct Ticket<'a> {
    switch: &'a Switch,
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        let mut gate = self.switch.gate.lock().expect("switch gate poisoned");
        gate.inflight -= 1;
        self.switch.cv.notify_all();
    }
}

/// Held for the duration of a handover. Guest I/O resumes when it is dropped, whether the
/// handover succeeded or not.
pub struct Stall<'a> {
    switch: &'a Switch,
}

impl Drop for Stall<'_> {
    fn drop(&mut self) {
        let mut gate = self.switch.gate.lock().expect("switch gate poisoned");
        gate.stalled = false;
        self.switch.cv.notify_all();
    }
}

impl Switch {
    pub fn new(forwarding: Arc<dyn Backend>) -> Switch {
        Switch {
            forwarding,
            owned: OnceLock::new(),
            gate: Mutex::new(Gate { stalled: false, inflight: 0 }),
            cv: Condvar::new(),
        }
    }

    fn enter(&self) -> Ticket<'_> {
        let mut gate = self.gate.lock().expect("switch gate poisoned");
        while gate.stalled {
            gate = self.cv.wait(gate).expect("switch gate poisoned");
        }
        gate.inflight += 1;
        Ticket { switch: self }
    }

    fn route(&self) -> &dyn Backend {
        match self.owned.get() {
            Some(o) => o.as_ref(),
            None => self.forwarding.as_ref(),
        }
    }

    /// Stop admitting requests and wait for the admitted ones to finish.
    pub fn stall(&self) -> Stall<'_> {
        let mut gate = self.gate.lock().expect("switch gate poisoned");
        // Another handover on the same switch must finish first: two would each believe
        // they held the only quiet instant.
        while gate.stalled {
            gate = self.cv.wait(gate).expect("switch gate poisoned");
        }
        gate.stalled = true;
        while gate.inflight > 0 {
            gate = self.cv.wait(gate).expect("switch gate poisoned");
        }
        drop(gate);
        Stall { switch: self }
    }

    /// Serve from `owned` from now on. Takes the stall as proof the caller holds the quiet
    /// instant, so a switch cannot be flipped under a request.
    pub fn install(&self, _stall: &Stall<'_>, owned: Arc<dyn Backend>) -> Result<()> {
        self.owned
            .set(owned)
            .map_err(|_| Error::refused("this vdisk is already served locally".to_string()))
    }

    /// Whether the switch already serves the disk itself.
    pub fn is_owned(&self) -> bool {
        self.owned.get().is_some()
    }
}

impl Backend for Switch {
    fn size(&self) -> u64 {
        self.route().size()
    }
    fn read_only(&self) -> bool {
        self.route().read_only()
    }
    fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let _t = self.enter();
        self.route().read(offset, len)
    }
    fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        let _t = self.enter();
        self.route().write(offset, data)
    }
    fn begin_write<'a>(&'a self, offset: u64, data: Vec<u8>) -> Pending<'a> {
        let ticket = self.enter();
        let pending = self.route().begin_write(offset, data);
        Box::new(move || {
            let outcome = pending();
            drop(ticket);
            outcome
        })
    }
    fn flush(&self) -> Result<()> {
        let _t = self.enter();
        self.route().flush()
    }
    fn write_zeroes(&self, offset: u64, len: u64) -> Result<()> {
        let _t = self.enter();
        self.route().write_zeroes(offset, len)
    }
}

/// What Hydra says about a vdisk at the moment a handover reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ownership {
    /// The node serving the disk. Empty when nobody does.
    pub owner: String,
    pub epoch: i64,
}

/// What the owner answered when asked to let go.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Released {
    /// It drained its journal and stopped serving the disk.
    Yes,
    /// It does not serve the disk at all: it already let go, or it restarted and holds
    /// nothing. Proceeding is safe only because the fence that follows the claim is what
    /// stops a writer, not this answer.
    NotServing,
}

/// The steps of a handover that touch the outside world, each of which can fail on its own.
///
/// [`handover`] is the order they run in and what each failure leaves behind; the daemon
/// supplies the real ones (Hydra, the owner over the peer protocol, the journal replicas)
/// and the tests supply ones that fail on demand.
pub trait Steps {
    /// What winning the claim yields: the opened vdisk.
    type Opened;
    /// This node's name.
    fn node(&self) -> &str;
    /// Read the vdisk's owner and epoch from Hydra.
    fn read_ownership(&self) -> Result<Ownership>;
    /// Ask `owner` to drain its journal and stop serving. Any error means the owner is
    /// still serving (or cannot be proven not to be), so the handover ends right there.
    fn release(&self, owner: &str) -> Result<Released>;
    /// Win the compare-and-swap from `seen` to `seen.epoch + 1`, fence the journal replicas
    /// at the epoch won, and open the vdisk on that epoch.
    fn claim_and_open(&self, seen: &Ownership) -> Result<Self::Opened>;
    /// What serves guest I/O for an opened vdisk.
    fn backend(&self, opened: &Self::Opened) -> Arc<dyn Backend>;
}

/// A handover that completed.
pub struct Done<T> {
    pub opened: T,
    pub previous_owner: String,
    pub epoch: i64,
    pub released: bool,
}

pub enum Outcome<T> {
    /// A previous handover already installed the local vdisk; nothing was done.
    AlreadyOwned,
    Done(Done<T>),
}

/// What a failed handover left behind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Left {
    /// The owner still serves the disk and this node still forwards to it.
    Unchanged,
    /// The request to release went unanswered: the owner may or may not have let go. If it
    /// did, guest I/O on this node errors until the handover is run again.
    Unknown,
    /// The owner let go before the step failed, so guest I/O on this node errors until the
    /// handover is run again.
    OwnerReleased,
}

/// Which step failed, and what it left.
pub struct Failure {
    pub step: &'static str,
    pub left: Left,
    pub cause: Error,
}

impl Failure {
    /// The error an operator reads: the step by name, and what state it left things in.
    pub fn into_error(self, vdisk: &str) -> Error {
        let left = match self.left {
            Left::OwnerReleased => {
                "the owner had already let go of the disk, so I/O on this node fails until the \
                 handover is run again (it is safe to run again)"
            }
            Left::Unknown => {
                "the owner may or may not have let go; if it did, I/O on this node fails until \
                 the handover is run again (it is safe to run again)"
            }
            Left::Unchanged => {
                "nothing changed: the owner still serves the disk and this node still forwards to it"
            }
        };
        let msg = |m: String| format!("handover of {vdisk} failed at step '{}': {m}; {left}", self.step);
        match self.cause {
            Error::Io(m) => Error::Io(msg(m)),
            Error::Corrupt(m) => Error::Corrupt(msg(m)),
            Error::Meta(m) => Error::Meta(msg(m)),
            Error::Refused(m) => Error::Refused(msg(m)),
        }
    }
}

/// Turn a disk this node forwards into a disk this node owns, underneath the NBD session the
/// guest already has open. The second half of a live migration (docs/dfs/ownership.md §5).
///
/// 1. `stall`: guest I/O on this node is held at the switch, and what was admitted finishes,
///    so every write the owner acknowledged is on every replica before it is asked to let go.
/// 2. `release`: the owner drains and stops serving. A refusal or an unreachable owner ends
///    the handover here with nothing changed.
/// 3. `claim_and_open`: the compare-and-swap in Hydra (owner and epoch together, e to e+1),
///    the fence of every reachable journal replica at e+1, and the journal tail adopted.
/// 4. `install`: the opened vdisk becomes the backend behind the session; I/O resumes.
///
/// Dropping the stall on every path is what turns a failure into a pause: before step 2 the
/// forwarder carries on; after it, I/O errors (the owner said it is not serving) until a
/// rerun. A rerun reads Hydra afresh and continues from wherever the last run left it, and
/// never from a stale idea of it: the claim is conditional on what it just read.
///
/// What this guarantees, and what the fault-injection tests below pin:
/// - never two writers: the owner has stopped (or never served) before the claim is attempted,
///   and the claim's fence rejects an owner that did not stop;
/// - an acknowledged write is never lost: the stall waits for admitted writes, and the new
///   owner adopts the journal from a fenced replica;
/// - a stale claim loses: the compare-and-swap fails and nothing is installed.
pub fn handover<S: Steps>(switch: &Switch, steps: &S) -> std::result::Result<Outcome<S::Opened>, Failure> {
    let stall = switch.stall();
    // Re-checked under the stall: a second handover that waited behind the first finds the
    // work done instead of claiming the disk from the node that just claimed it.
    if switch.is_owned() {
        return Ok(Outcome::AlreadyOwned);
    }
    let fail = |step: &'static str, left: Left, cause: Error| Failure { step, left, cause };

    let after = |released: bool| if released { Left::OwnerReleased } else { Left::Unchanged };
    let seen = steps.read_ownership().map_err(|e| fail("read ownership", Left::Unchanged, e))?;

    // Skipped when Hydra already names this node (an earlier run won the claim and failed
    // after it) or names nobody: there is then no owner to ask.
    let mut released = false;
    if !seen.owner.is_empty() && seen.owner != steps.node() {
        match steps.release(&seen.owner) {
            Ok(Released::Yes) => released = true,
            Ok(Released::NotServing) => {}
            // A refusal is an answer: the owner still serves. Anything else is silence.
            Err(e @ Error::Refused(_)) => return Err(fail("release", Left::Unchanged, e)),
            Err(e) => return Err(fail("release", Left::Unknown, e)),
        }
    }

    let opened = steps
        .claim_and_open(&seen)
        .map_err(|e| fail("claim and fence", after(released), e))?;
    switch
        .install(&stall, steps.backend(&opened))
        .map_err(|e| fail("install", after(released), e))?;
    drop(stall);
    Ok(Outcome::Done(Done { opened, previous_owner: seen.owner, epoch: seen.epoch + 1, released }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    /// A backend that records every write it is given, in order, and can be told to hold
    /// writes until released.
    struct Recorder {
        name: &'static str,
        log: Arc<Mutex<Vec<(&'static str, u64)>>>,
        hold: Mutex<Option<mpsc::Receiver<()>>>,
        seen: AtomicUsize,
    }

    impl Recorder {
        fn new(name: &'static str, log: Arc<Mutex<Vec<(&'static str, u64)>>>) -> Recorder {
            Recorder { name, log, hold: Mutex::new(None), seen: AtomicUsize::new(0) }
        }
    }

    impl Backend for Recorder {
        fn size(&self) -> u64 {
            1 << 20
        }
        fn read_only(&self) -> bool {
            false
        }
        fn read(&self, _o: u64, len: u32) -> Result<Vec<u8>> {
            Ok(vec![0; len as usize])
        }
        fn write(&self, offset: u64, _data: &[u8]) -> Result<()> {
            self.seen.fetch_add(1, Ordering::SeqCst);
            if let Some(rx) = self.hold.lock().unwrap().take() {
                let _ = rx.recv();
            }
            self.log.lock().unwrap().push((self.name, offset));
            Ok(())
        }
        fn flush(&self) -> Result<()> {
            Ok(())
        }
        fn write_zeroes(&self, _o: u64, _l: u64) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn writes_go_to_the_forwarder_until_the_vdisk_is_installed_and_to_it_afterwards() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let fwd = Arc::new(Recorder::new("forward", Arc::clone(&log)));
        let own = Arc::new(Recorder::new("owner", Arc::clone(&log)));
        let sw = Switch::new(fwd);
        sw.write(0, b"a").unwrap();
        {
            let stall = sw.stall();
            sw.install(&stall, own).unwrap();
        }
        sw.write(4096, b"b").unwrap();
        assert_eq!(*log.lock().unwrap(), vec![("forward", 0), ("owner", 4096)]);
        assert!(sw.is_owned());
    }

    /// The property the handover rests on: a stall does not return while a write that was
    /// admitted is still running, so the owner is never asked to hand over under a write it
    /// has not acknowledged, and a write admitted after the stall began lands on the NEW
    /// backend, not the old one.
    #[test]
    fn a_stall_waits_for_an_admitted_write_and_holds_back_the_next() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let fwd = Arc::new(Recorder::new("forward", Arc::clone(&log)));
        let own = Arc::new(Recorder::new("owner", Arc::clone(&log)));
        let (release, hold) = mpsc::channel();
        *fwd.hold.lock().unwrap() = Some(hold);
        let sw = Arc::new(Switch::new(fwd.clone()));

        let w1 = {
            let sw = Arc::clone(&sw);
            std::thread::spawn(move || sw.write(0, b"first").unwrap())
        };
        while fwd.seen.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(2));
        }
        // The first write is admitted and parked inside the forwarder. Start the handover.
        let (stalled_tx, stalled_rx) = mpsc::channel();
        let handover = {
            let sw = Arc::clone(&sw);
            let own = own.clone();
            std::thread::spawn(move || {
                let stall = sw.stall();
                stalled_tx.send(()).unwrap();
                sw.install(&stall, own).unwrap();
            })
        };
        assert!(
            stalled_rx.recv_timeout(Duration::from_millis(150)).is_err(),
            "the stall returned while a write was still in flight"
        );
        // A second write, issued now, must wait for the handover and go to the new owner.
        let w2 = {
            let sw = Arc::clone(&sw);
            std::thread::spawn(move || sw.write(8192, b"second").unwrap())
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(own.seen.load(Ordering::SeqCst) == 0);
        release.send(()).unwrap();
        w1.join().unwrap();
        stalled_rx.recv_timeout(Duration::from_secs(5)).expect("stall completes once drained");
        handover.join().unwrap();
        w2.join().unwrap();
        assert_eq!(*log.lock().unwrap(), vec![("forward", 0), ("owner", 8192)]);
    }

    /// A stall that is dropped without installing anything (the handover failed) lets I/O
    /// continue against the forwarder, so a failed handover costs a pause, not the disk.
    #[test]
    fn a_failed_handover_resumes_forwarding() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let fwd = Arc::new(Recorder::new("forward", Arc::clone(&log)));
        let sw = Switch::new(fwd);
        drop(sw.stall());
        sw.write(512, b"x").unwrap();
        assert_eq!(*log.lock().unwrap(), vec![("forward", 512)]);
        assert!(!sw.is_owned());
    }

    #[test]
    fn a_second_install_is_refused() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let sw = Switch::new(Arc::new(Recorder::new("forward", Arc::clone(&log))));
        let stall = sw.stall();
        sw.install(&stall, Arc::new(Recorder::new("one", Arc::clone(&log)))).unwrap();
        assert!(sw.install(&stall, Arc::new(Recorder::new("two", log))).is_err());
    }

    /// begin_write holds its ticket until the returned closure has run, so a stall cannot
    /// slip between "append begun" and "append durable".
    #[test]
    fn a_begun_write_counts_as_in_flight_until_it_is_waited_for() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let sw = Arc::new(Switch::new(Arc::new(Recorder::new("forward", log))));
        let pending = sw.begin_write(0, vec![1]);
        let (tx, rx) = mpsc::channel();
        let s2 = Arc::clone(&sw);
        let t = std::thread::spawn(move || {
            let _stall = s2.stall();
            tx.send(()).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        pending().unwrap();
        rx.recv_timeout(Duration::from_secs(5)).expect("stall proceeds after the wait");
        t.join().unwrap();
    }
}

/// Fault injection for [`handover`]: a fake cluster in which every step can fail, and the
/// properties the handover is for checked after each failure and each retry.
#[cfg(test)]
mod fault_tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const ME: &str = "dest";
    const OLD: &str = "src";

    /// One vdisk on a cluster of two Sidons, as far as the handover can see it.
    struct World {
        owner: String,
        epoch: i64,
        /// The old owner's Sidon is serving the disk.
        owner_serving: bool,
        /// The epoch the journal replicas have been told to require. A writer holding a
        /// lower one is rejected, whatever it believes about itself.
        fenced: i64,
        /// Everything any writer was told succeeded: offset -> value.
        acked: BTreeMap<u64, u8>,
        /// What the disk holds.
        disk: BTreeMap<u64, u8>,
        /// Steps in the order they ran.
        log: Vec<String>,
        /// Set the moment two writers were both able to write.
        two_writers: bool,
        claims: usize,
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Fault {
        None,
        ReadOwnership,
        /// The owner answers "refused": it still serves.
        ReleaseRefused,
        /// No answer at all, and the owner did not act on the request.
        ReleaseUnreachable,
        /// The owner released and the reply was lost.
        ReleaseReplyLost,
        /// Another node wins the compare-and-swap first.
        ClaimLost,
        /// Hydra cannot be reached for the claim.
        ClaimHydraDown,
        /// The claim is won, then fencing or opening fails.
        FenceFails,
    }

    struct Fake {
        w: Arc<Mutex<World>>,
        fault: Mutex<Fault>,
        reads: AtomicUsize,
    }

    impl Fake {
        fn new(owner_serving: bool) -> Fake {
            Fake {
                w: Arc::new(Mutex::new(World {
                    owner: OLD.into(),
                    epoch: 4,
                    owner_serving,
                    fenced: 4,
                    acked: BTreeMap::new(),
                    disk: BTreeMap::new(),
                    log: Vec::new(),
                    two_writers: false,
                    claims: 0,
                })),
                fault: Mutex::new(Fault::None),
                reads: AtomicUsize::new(0),
            }
        }
        fn inject(&self, f: Fault) {
            *self.fault.lock().unwrap() = f;
        }
        /// The fault, once: a retry finds the system healthy.
        fn take(&self, want: Fault) -> bool {
            let mut f = self.fault.lock().unwrap();
            if *f == want {
                *f = Fault::None;
                true
            } else {
                false
            }
        }
        fn forwarder(&self) -> Arc<dyn Backend> {
            Arc::new(Via { w: Arc::clone(&self.w), local: false })
        }
    }

    /// A backend over the fake world: the forwarder (reaches the old owner, which writes at
    /// the epoch it holds) or the local vdisk (writes at the epoch won).
    struct Via {
        w: Arc<Mutex<World>>,
        local: bool,
    }

    impl Backend for Via {
        fn size(&self) -> u64 {
            1 << 20
        }
        fn read_only(&self) -> bool {
            false
        }
        fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
            let w = self.w.lock().unwrap();
            Ok(vec![*w.disk.get(&offset).unwrap_or(&0); len as usize])
        }
        fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
            let mut w = self.w.lock().unwrap();
            let value = data[0];
            if self.local {
                // A second writer is the old owner still being able to write.
                if w.owner_serving && w.fenced <= 4 {
                    w.two_writers = true;
                }
            } else {
                if !w.owner_serving {
                    return Err(Error::refused("src no longer owns the disk; forwarding target is stale"));
                }
                // The old owner appends at the epoch it holds, 4. Replicas fenced above that
                // reject it.
                if w.fenced > 4 {
                    return Err(Error::refused("stale epoch: rejected by a fenced replica"));
                }
            }
            w.disk.insert(offset, value);
            w.acked.insert(offset, value);
            Ok(())
        }
        fn flush(&self) -> Result<()> {
            Ok(())
        }
        fn write_zeroes(&self, _o: u64, _l: u64) -> Result<()> {
            Ok(())
        }
    }

    impl Steps for Fake {
        type Opened = i64;
        fn node(&self) -> &str {
            ME
        }
        fn read_ownership(&self) -> Result<Ownership> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.take(Fault::ReadOwnership) {
                return Err(Error::meta("hydra unreachable"));
            }
            let mut w = self.w.lock().unwrap();
            w.log.push("read".into());
            Ok(Ownership { owner: w.owner.clone(), epoch: w.epoch })
        }
        fn release(&self, owner: &str) -> Result<Released> {
            assert_eq!(owner, OLD);
            let mut w = self.w.lock().unwrap();
            w.log.push("release".into());
            drop(w);
            if self.take(Fault::ReleaseRefused) {
                return Err(Error::refused("a client is still connected"));
            }
            if self.take(Fault::ReleaseUnreachable) {
                return Err(Error::io("timed out"));
            }
            let mut w = self.w.lock().unwrap();
            let was = w.owner_serving;
            w.owner_serving = false;
            drop(w);
            if self.take(Fault::ReleaseReplyLost) {
                return Err(Error::io("reply lost"));
            }
            Ok(if was { Released::Yes } else { Released::NotServing })
        }
        fn claim_and_open(&self, seen: &Ownership) -> Result<i64> {
            let mut w = self.w.lock().unwrap();
            w.log.push("claim".into());
            if self.take(Fault::ClaimHydraDown) {
                return Err(Error::meta("hydra unreachable"));
            }
            if self.take(Fault::ClaimLost) {
                // Somebody else won between the read and the swap.
                w.owner = "third".into();
                w.epoch += 1;
                w.fenced = w.epoch;
            }
            // The compare-and-swap: owner and epoch together, as read.
            if w.owner != seen.owner || w.epoch != seen.epoch {
                return Err(Error::refused(format!(
                    "owned by {} at epoch {}; this node did not win the claim",
                    w.owner, w.epoch
                )));
            }
            w.claims += 1;
            w.owner = ME.into();
            w.epoch = seen.epoch + 1;
            // The fence reaches the replicas: the old owner is now rejected.
            w.fenced = w.epoch;
            w.log.push("fence".into());
            if self.take(Fault::FenceFails) {
                return Err(Error::io("a replica could not be reached to read the journal tail"));
            }
            Ok(w.epoch)
        }
        fn backend(&self, _opened: &i64) -> Arc<dyn Backend> {
            Arc::new(Via { w: Arc::clone(&self.w), local: true })
        }
    }

    fn rig(owner_serving: bool) -> (Fake, Arc<Switch>) {
        let f = Fake::new(owner_serving);
        let sw = Arc::new(Switch::new(f.forwarder()));
        (f, sw)
    }

    fn done(o: std::result::Result<Outcome<i64>, Failure>) -> Done<i64> {
        match o {
            Ok(Outcome::Done(d)) => d,
            Ok(Outcome::AlreadyOwned) => panic!("expected a completed handover"),
            Err(f) => panic!("handover failed at {}: {}", f.step, f.cause),
        }
    }

    fn failed(o: std::result::Result<Outcome<i64>, Failure>) -> Failure {
        match o {
            Err(f) => f,
            _ => panic!("expected a failure"),
        }
    }

    fn log(f: &Fake) -> Vec<String> {
        f.w.lock().unwrap().log.clone()
    }

    #[test]
    fn the_owner_lets_go_before_the_claim_is_attempted_and_the_fence_follows_the_claim() {
        let (f, sw) = rig(true);
        let d = done(handover(&sw, &f));
        assert!(d.released);
        assert_eq!(d.previous_owner, OLD);
        assert_eq!(d.epoch, 5);
        assert_eq!(log(&f), ["read", "release", "claim", "fence"]);
        let w = f.w.lock().unwrap();
        assert_eq!((w.owner.as_str(), w.epoch, w.fenced), (ME, 5, 5));
        assert!(sw.is_owned());
    }

    #[test]
    fn a_write_is_never_accepted_by_two_writers_at_once() {
        let (f, sw) = rig(true);
        sw.write(0, &[1]).unwrap();
        done(handover(&sw, &f));
        sw.write(4096, &[2]).unwrap();
        assert!(!f.w.lock().unwrap().two_writers);
        // And the model can see a violation: with the owner never asked to let go, a local
        // write would be a second writer. This is what the test above is not.
        let (g, sw2) = rig(true);
        let stall = sw2.stall();
        sw2.install(&stall, g.backend(&5)).unwrap();
        drop(stall);
        g.w.lock().unwrap().fenced = 4;
        sw2.write(0, &[1]).unwrap();
        assert!(g.w.lock().unwrap().two_writers, "the detector must be able to fire");
    }

    #[test]
    fn a_refused_release_changes_nothing_and_the_guest_keeps_running_on_the_forwarder() {
        let (f, sw) = rig(true);
        f.inject(Fault::ReleaseRefused);
        let e = failed(handover(&sw, &f));
        assert_eq!((e.step, e.left), ("release", Left::Unchanged));
        assert!(!sw.is_owned());
        sw.write(0, &[7]).expect("the owner still serves; I/O continues");
        let w = f.w.lock().unwrap();
        assert_eq!((w.owner.as_str(), w.epoch), (OLD, 4), "no epoch moved");
        assert_eq!(w.claims, 0);
        assert!(!log_has(&w, "claim"));
    }

    fn log_has(w: &World, what: &str) -> bool {
        w.log.iter().any(|l| l == what)
    }

    #[test]
    fn an_unreadable_hydra_ends_it_before_the_owner_is_asked_anything() {
        let (f, sw) = rig(true);
        f.inject(Fault::ReadOwnership);
        let e = failed(handover(&sw, &f));
        assert_eq!((e.step, e.left), ("read ownership", Left::Unchanged));
        assert_eq!(log(&f), Vec::<String>::new());
        sw.write(0, &[1]).unwrap();
    }

    #[test]
    fn an_unreachable_owner_is_reported_as_unknown_and_a_rerun_completes() {
        let (f, sw) = rig(true);
        f.inject(Fault::ReleaseUnreachable);
        let e = failed(handover(&sw, &f));
        assert_eq!((e.step, e.left), ("release", Left::Unknown));
        assert!(!sw.is_owned());
        let d = done(handover(&sw, &f));
        assert_eq!(d.epoch, 5, "the failed attempt moved nothing");
        assert_eq!(f.w.lock().unwrap().claims, 1);
    }

    /// The owner let go and its reply was lost. The destination cannot know, so it errors;
    /// guest I/O fails meanwhile (the owner says it is not serving), and the rerun finds the
    /// owner not serving, claims, and installs. No write was acknowledged during the gap.
    #[test]
    fn a_lost_release_reply_is_recoverable_and_loses_no_acknowledged_write() {
        let (f, sw) = rig(true);
        sw.write(0, &[1]).unwrap();
        f.inject(Fault::ReleaseReplyLost);
        let e = failed(handover(&sw, &f));
        assert_eq!(e.step, "release");
        assert!(sw.write(4096, &[2]).is_err(), "I/O errors until the rerun");
        let d = done(handover(&sw, &f));
        assert!(!d.released, "the owner had already stopped: NotServing");
        sw.write(8192, &[3]).unwrap();
        let w = f.w.lock().unwrap();
        for (off, v) in &w.acked {
            assert_eq!(w.disk.get(off), Some(v), "acknowledged write at {off} must be on the disk");
        }
        assert_eq!(w.acked.len(), 2, "the write that errored was never acknowledged");
        assert!(!w.two_writers);
    }

    #[test]
    fn losing_the_claim_installs_nothing_and_leaves_the_winner_in_charge() {
        let (f, sw) = rig(true);
        f.inject(Fault::ClaimLost);
        let e = failed(handover(&sw, &f));
        assert_eq!((e.step, e.left), ("claim and fence", Left::OwnerReleased));
        assert!(!sw.is_owned());
        let w = f.w.lock().unwrap();
        assert_eq!(w.owner, "third");
        assert_eq!(w.claims, 0, "this node never won");
        assert!(!w.two_writers);
        drop(w);
        // A rerun reads the new owner and asks it, not the old one: here the fake insists
        // on the old name, which is the point -- the claim is conditional on what was read.
    }

    #[test]
    fn hydra_down_at_the_claim_is_a_retry_not_a_loss() {
        let (f, sw) = rig(true);
        f.inject(Fault::ClaimHydraDown);
        let e = failed(handover(&sw, &f));
        assert_eq!((e.step, e.left), ("claim and fence", Left::OwnerReleased));
        assert_eq!(f.w.lock().unwrap().epoch, 4, "no epoch moved");
        let d = done(handover(&sw, &f));
        assert_eq!(d.epoch, 5);
    }

    /// The claim is won and then fencing fails. Hydra names this node at e+1, the replicas
    /// may or may not be fenced, and the old owner has already stopped. The rerun must not
    /// ask the owner (it is this node now) and must win again from the state it finds.
    #[test]
    fn a_failure_after_the_claim_is_retried_from_where_it_stopped() {
        let (f, sw) = rig(true);
        f.inject(Fault::FenceFails);
        let e = failed(handover(&sw, &f));
        assert_eq!((e.step, e.left), ("claim and fence", Left::OwnerReleased));
        {
            let w = f.w.lock().unwrap();
            assert_eq!((w.owner.as_str(), w.epoch), (ME, 5));
        }
        assert!(sw.write(0, &[1]).is_err(), "the old owner is fenced out; I/O errors until the rerun");
        let releases_before = log(&f).iter().filter(|l| *l == "release").count();
        let d = done(handover(&sw, &f));
        let releases_after = log(&f).iter().filter(|l| *l == "release").count();
        assert_eq!(releases_before, releases_after, "no one is asked to release a disk this node holds");
        assert_eq!(d.epoch, 6, "a re-claim moves the epoch again, and only ever upward");
        assert_eq!(d.previous_owner, ME);
        sw.write(0, &[2]).unwrap();
        assert!(!f.w.lock().unwrap().two_writers);
    }

    #[test]
    fn a_second_handover_after_a_completed_one_does_nothing() {
        let (f, sw) = rig(true);
        done(handover(&sw, &f));
        let reads = f.reads.load(Ordering::SeqCst);
        assert!(matches!(handover(&sw, &f), Ok(Outcome::AlreadyOwned)));
        assert_eq!(f.reads.load(Ordering::SeqCst), reads, "not even Hydra was asked");
        assert_eq!(f.w.lock().unwrap().epoch, 5);
    }

    #[test]
    fn two_handovers_at_once_claim_the_disk_once() {
        let (f, sw) = rig(true);
        let f = Arc::new(f);
        let outcomes: Vec<bool> = (0..4)
            .map(|_| {
                let (f, sw) = (Arc::clone(&f), Arc::clone(&sw));
                std::thread::spawn(move || matches!(handover(&sw, &*f), Ok(Outcome::Done(_))))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        assert_eq!(outcomes.iter().filter(|d| **d).count(), 1);
        let w = f.w.lock().unwrap();
        assert_eq!((w.claims, w.epoch), (1, 5));
    }

    /// Guest writes arrive continuously while the handover runs. Every one of them is either
    /// acknowledged and on the disk afterwards, or was never acknowledged; none errors,
    /// because the stall holds a request for the instant of change instead of failing it.
    #[test]
    fn writes_racing_the_handover_are_neither_lost_nor_failed() {
        let (f, sw) = rig(true);
        let f = Arc::new(f);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let errors = Arc::new(AtomicUsize::new(0));
        let writer = {
            let (sw, stop, errors) = (Arc::clone(&sw), Arc::clone(&stop), Arc::clone(&errors));
            std::thread::spawn(move || {
                let mut i: u64 = 0;
                while !stop.load(Ordering::SeqCst) {
                    if sw.write(i * 512, &[(i % 250) as u8 + 1]).is_err() {
                        errors.fetch_add(1, Ordering::SeqCst);
                    }
                    i += 1;
                }
                i
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(20));
        done(handover(&sw, &*f));
        std::thread::sleep(std::time::Duration::from_millis(20));
        stop.store(true, Ordering::SeqCst);
        let issued = writer.join().unwrap();
        assert!(issued > 2);
        assert_eq!(errors.load(Ordering::SeqCst), 0);
        let w = f.w.lock().unwrap();
        assert_eq!(w.acked.len() as u64, issued);
        for (off, v) in &w.acked {
            assert_eq!(w.disk.get(off), Some(v));
        }
        assert!(!w.two_writers);
    }

    /// The fence is what makes a deposed owner harmless: after the claim, a writer that still
    /// believes it owns the disk at the old epoch is rejected, whatever it thinks.
    #[test]
    fn the_old_owner_cannot_write_once_the_claim_has_fenced_it() {
        let (f, sw) = rig(true);
        // The owner never answers the release (it is wedged, not stopped).
        f.w.lock().unwrap().owner_serving = true;
        let fwd = Via { w: Arc::clone(&f.w), local: false };
        f.inject(Fault::ReleaseUnreachable);
        failed(handover(&sw, &f));
        assert!(fwd.write(0, &[9]).is_ok(), "still the owner at epoch 4: accepted");
        // A node that skips the release (treated as not serving) and claims: the fence stops
        // the wedged owner from this point.
        {
            let mut w = f.w.lock().unwrap();
            w.owner = ME.into();
            w.epoch = 5;
            w.fenced = 5;
        }
        assert!(fwd.write(512, &[9]).is_err(), "the replicas reject epoch 4");
        assert!(f.w.lock().unwrap().disk.get(&512).is_none());
    }

    #[test]
    fn a_failure_is_reported_with_its_step_and_what_it_left() {
        let (f, sw) = rig(true);
        f.inject(Fault::ReleaseRefused);
        let msg = failed(handover(&sw, &f)).into_error("vd1").to_string();
        assert!(msg.contains("vd1") && msg.contains("step 'release'"), "{msg}");
        assert!(msg.contains("still serves the disk"), "{msg}");
    }
}
