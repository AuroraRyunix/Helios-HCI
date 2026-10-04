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
