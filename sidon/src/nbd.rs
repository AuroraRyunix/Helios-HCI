//! The NBD server qemu attaches to.
//!
//! Fixed-newstyle handshake over a unix socket, simple replies only (a simple reply carries
//! the request's handle, so replies may complete in any order). NBD was chosen over
//! iSCSI-to-localhost (a whole target stack for no gain) and over ublk (a kernel
//! dependency): it is qemu-native, needs no kernel module, and keeps the entire data
//! path in userspace where it can be killed and restarted without touching the host.
//!
//! The rule that keeps this from desynchronising: **a WRITE's payload is drained from the
//! socket before any error is reported.** Replying early leaves the guest's bytes sitting
//! in the stream to be parsed as the next request header, which produces a protocol
//! failure attributed to whatever came after it.
//!
//! ## Many requests in flight
//!
//! One thread reads requests, in order, and a request is handed to a worker of its own (up
//! to [`MAX_IN_FLIGHT`] per connection, and a byte budget so thirty-two 64 MiB writes cannot
//! all be held); a worker runs it and sends its reply under a lock on the socket. So a slow
//! request does not hold up the ones behind it, and replies leave in completion order.
//!
//! What stays in order is the part that must: the reader reads a write's payload and then
//! *submits* the write to the backend ([`Backend::begin_write`]) before it reads the next
//! request. For a local vdisk submitting means appending the write's records to the
//! journal, so writes get their journal order from the order they arrived, and a flush --
//! which is read after every write that preceded it -- covers all of them. Waiting for the
//! write to be durable on every copy, which is the slow part, happens on the worker, and
//! writes that are waiting together are made durable together (`vdisk/commit.rs`).
//!
//! The guest owns the ordering of overlapping requests it keeps in flight, as with any disk;
//! what this guarantees is only that its order is *a* defined one (arrival order for writes)
//! and that a reply means durable. FUA needs nothing beyond that: a write is not replied to
//! until it is durable on every copy.

use std::io::{BufReader, BufWriter, Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};

use crate::err::{Error, Result};
use crate::vdisk::Vdisk;

const NBDMAGIC: u64 = 0x4e42_444d_4147_4943;
const IHAVEOPT: u64 = 0x4948_4156_454f_5054;
const REP_MAGIC: u64 = 0x0003_e889_0455_65a9;
const REQUEST_MAGIC: u32 = 0x2560_9513;
const SIMPLE_REPLY_MAGIC: u32 = 0x6744_6698;

const FLAG_FIXED_NEWSTYLE: u16 = 1;
const FLAG_NO_ZEROES: u16 = 2;

const OPT_EXPORT_NAME: u32 = 1;
const OPT_ABORT: u32 = 2;
const OPT_LIST: u32 = 3;
const OPT_INFO: u32 = 6;
const OPT_GO: u32 = 7;

const REP_ACK: u32 = 1;
const REP_SERVER: u32 = 2;
const REP_INFO: u32 = 3;
const REP_ERR_UNSUP: u32 = 0x8000_0001;
const REP_ERR_INVALID: u32 = 0x8000_0003;

const INFO_EXPORT: u16 = 0;
const INFO_BLOCK_SIZE: u16 = 3;

const TX_HAS_FLAGS: u16 = 1;
const TX_READ_ONLY: u16 = 2;
const TX_SEND_FLUSH: u16 = 4;
const TX_SEND_FUA: u16 = 8;
const TX_SEND_TRIM: u16 = 32;
const TX_SEND_WRITE_ZEROES: u16 = 64;

const CMD_READ: u16 = 0;
const CMD_WRITE: u16 = 1;
const CMD_DISC: u16 = 2;
const CMD_FLUSH: u16 = 3;
const CMD_TRIM: u16 = 4;
const CMD_CACHE: u16 = 5;
const CMD_WRITE_ZEROES: u16 = 6;

/// The force-unit-access flag on a write. Nothing here acts on it: a write is not answered
/// until it is durable on every copy, which is all FUA asks for.
#[cfg(test)]
const CMD_FLAG_FUA: u16 = 1;

/// How many requests one connection may have in flight, which is also the most threads it
/// uses. Threads are started as depth is actually used, not up front.
const MAX_IN_FLIGHT: usize = 32;

/// Bytes of write payload and read buffer a connection may hold at once. A request larger
/// than this still runs, alone.
const IN_FLIGHT_BYTES: u64 = 128 << 20;

/// Refuse absurd read lengths before allocating for them. NBD's own recommended maximum
/// is 32 MiB; a client asking for more is confused or hostile, and either way this
/// process should not try to satisfy it.
const MAX_IO: u32 = 64 << 20;

/// What an NBD export reads and writes.
///
/// Two implementations: a vdisk this node owns, and a forwarder relaying to the node that
/// does. The NBD layer cannot tell them apart, which is the point -- the guest always
/// talks to its local Sidon, and whether that Sidon happens to own the disk is not the
/// guest's problem and not this file's either.
pub trait Backend: Send + Sync {
    fn size(&self) -> u64;
    fn read_only(&self) -> bool;
    fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>>;
    /// Write `data`; returns once it is durable on every copy of the disk.
    fn write(&self, offset: u64, data: &[u8]) -> Result<()>;
    /// Start a write and return what waits for it.
    ///
    /// Called on the connection's reader thread, in request order, so what it does before
    /// returning fixes the order writes are applied in; the returned closure runs on a worker
    /// and returns when the write is durable on every copy. The default does all of it on
    /// the worker, which is right for a backend with no ordering of its own to preserve.
    fn begin_write<'a>(&'a self, offset: u64, data: Vec<u8>) -> Pending<'a> {
        Box::new(move || self.write(offset, &data))
    }
    /// Return once every write begun before this call is durable on every copy.
    fn flush(&self) -> Result<()>;
    fn write_zeroes(&self, offset: u64, len: u64) -> Result<()>;
}

/// A write that has been started: call it to wait for the write's outcome.
pub type Pending<'a> = Box<dyn FnOnce() -> Result<()> + Send + 'a>;

/// A vdisk served by the node that owns it.
pub struct LocalVdisk(pub Arc<Mutex<Vdisk>>);

impl Backend for LocalVdisk {
    fn size(&self) -> u64 {
        self.0.lock().expect("vdisk mutex poisoned").size
    }
    fn read_only(&self) -> bool {
        self.0.lock().expect("vdisk mutex poisoned").class == crate::meta::CLASS_IMMUTABLE
    }
    fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        self.0.lock().expect("vdisk mutex poisoned").read(offset, len)
    }
    fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        // Not `vdisk.write` under the lock: a write that fills the journal must start the
        // drain on its own thread and be acknowledged without it, and one that finds the
        // journal at its ceiling must wait for room *without* holding the lock the drain
        // needs. Both are `write_through`'s job.
        crate::vdisk::write_through(&self.0, offset, data)
    }
    fn begin_write<'a>(&'a self, offset: u64, data: Vec<u8>) -> Pending<'a> {
        // The append, here and now, in the reader's order; the wait is the worker's.
        match crate::vdisk::submit_write(&self.0, offset, &data) {
            Ok(commit) => Box::new(move || commit.wait()),
            Err(e) => Box::new(move || Err(e)),
        }
    }
    fn flush(&self) -> Result<()> {
        crate::vdisk::flush_through(&self.0)
    }
    fn write_zeroes(&self, offset: u64, len: u64) -> Result<()> {
        crate::vdisk::write_zeroes_through(&self.0, offset, len)
    }
}

pub struct Export {
    pub backend: Arc<dyn Backend>,
    pub name: String,
}

fn read_exact<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<()> {
    r.read_exact(buf).map_err(|e| Error::io(format!("nbd read: {e}")))
}

fn read_u16<R: Read>(r: &mut R) -> Result<u16> {
    let mut b = [0u8; 2];
    read_exact(r, &mut b)?;
    Ok(u16::from_be_bytes(b))
}

fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
    let mut b = [0u8; 4];
    read_exact(r, &mut b)?;
    Ok(u32::from_be_bytes(b))
}

fn read_u64<R: Read>(r: &mut R) -> Result<u64> {
    let mut b = [0u8; 8];
    read_exact(r, &mut b)?;
    Ok(u64::from_be_bytes(b))
}

fn transmission_flags(read_only: bool) -> u16 {
    let mut f = TX_HAS_FLAGS | TX_SEND_FLUSH | TX_SEND_FUA | TX_SEND_TRIM | TX_SEND_WRITE_ZEROES;
    if read_only {
        f |= TX_READ_ONLY;
    }
    f
}

fn send_option_reply<W: Write>(
    w: &mut W,
    option: u32,
    rep_type: u32,
    payload: &[u8],
) -> Result<()> {
    let mut head = Vec::with_capacity(20 + payload.len());
    head.extend_from_slice(&REP_MAGIC.to_be_bytes());
    head.extend_from_slice(&option.to_be_bytes());
    head.extend_from_slice(&rep_type.to_be_bytes());
    head.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    head.extend_from_slice(payload);
    w.write_all(&head).map_err(|e| Error::io(format!("nbd write: {e}")))?;
    Ok(())
}

fn export_info_payload(size: u64, read_only: bool) -> Vec<u8> {
    let mut p = Vec::with_capacity(12);
    p.extend_from_slice(&INFO_EXPORT.to_be_bytes());
    p.extend_from_slice(&size.to_be_bytes());
    p.extend_from_slice(&transmission_flags(read_only).to_be_bytes());
    p
}

fn block_size_payload() -> Vec<u8> {
    // minimum / preferred / maximum. The minimum is 1 because the journal records byte
    // ranges, not sectors; the preferred 4 KiB matches what guests actually issue.
    let mut p = Vec::with_capacity(14);
    p.extend_from_slice(&INFO_BLOCK_SIZE.to_be_bytes());
    p.extend_from_slice(&1u32.to_be_bytes());
    p.extend_from_slice(&4096u32.to_be_bytes());
    p.extend_from_slice(&(MAX_IO).to_be_bytes());
    p
}

/// Serve one client connection to completion.
pub fn serve(stream: UnixStream, export: &Export) -> Result<()> {
    // Two handles on one socket: the reader blocks on the next request while the writer
    // is still flushing the previous reply. `try_clone` shares the underlying descriptor,
    // which is what makes that safe rather than merely convenient.
    let peer = stream.try_clone().map_err(|e| Error::io(format!("nbd socket clone: {e}")))?;
    let mut reader = BufReader::new(peer);
    let mut writer = BufWriter::new(stream);

    let (size, read_only) = (export.backend.size(), export.backend.read_only());

    // ---- handshake ---------------------------------------------------------------
    let mut hello = Vec::with_capacity(18);
    hello.extend_from_slice(&NBDMAGIC.to_be_bytes());
    hello.extend_from_slice(&IHAVEOPT.to_be_bytes());
    hello.extend_from_slice(&(FLAG_FIXED_NEWSTYLE | FLAG_NO_ZEROES).to_be_bytes());
    writer.write_all(&hello)?;
    writer.flush()?;

    let client_flags = read_u32(&mut reader)?;
    let no_zeroes = client_flags & FLAG_NO_ZEROES as u32 != 0;

    loop {
        let magic = read_u64(&mut reader)?;
        if magic != IHAVEOPT {
            return Err(Error::refused(format!(
                "nbd client sent option magic {magic:#x}, expected IHAVEOPT"
            )));
        }
        let option = read_u32(&mut reader)?;
        let len = read_u32(&mut reader)?;
        if len > (1 << 20) {
            return Err(Error::refused(format!("nbd option {option} payload of {len} bytes")));
        }
        let mut data = vec![0u8; len as usize];
        read_exact(&mut reader, &mut data)?;

        match option {
            OPT_EXPORT_NAME => {
                // Old-style: no reply header, straight to the export tuple.
                let mut resp = Vec::with_capacity(10 + 124);
                resp.extend_from_slice(&size.to_be_bytes());
                resp.extend_from_slice(&transmission_flags(read_only).to_be_bytes());
                if !no_zeroes {
                    resp.extend_from_slice(&[0u8; 124]);
                }
                writer.write_all(&resp)?;
                writer.flush()?;
                break;
            }
            OPT_GO | OPT_INFO => {
                // Payload: u32 name length, name, u16 count, then that many u16 requests.
                // The requests are advisory; sending EXPORT and BLOCK_SIZE unconditionally
                // is permitted and saves parsing a list we would answer the same way.
                if data.len() < 4 {
                    send_option_reply(&mut writer, option, REP_ERR_INVALID, b"short payload")?;
                    writer.flush()?;
                    continue;
                }
                send_option_reply(
                    &mut writer,
                    option,
                    REP_INFO,
                    &export_info_payload(size, read_only),
                )?;
                send_option_reply(&mut writer, option, REP_INFO, &block_size_payload())?;
                send_option_reply(&mut writer, option, REP_ACK, &[])?;
                writer.flush()?;
                if option == OPT_GO {
                    break;
                }
            }
            OPT_LIST => {
                let name = export.name.as_bytes();
                let mut p = Vec::with_capacity(4 + name.len());
                p.extend_from_slice(&(name.len() as u32).to_be_bytes());
                p.extend_from_slice(name);
                send_option_reply(&mut writer, option, REP_SERVER, &p)?;
                send_option_reply(&mut writer, option, REP_ACK, &[])?;
                writer.flush()?;
            }
            OPT_ABORT => {
                send_option_reply(&mut writer, option, REP_ACK, &[])?;
                writer.flush()?;
                return Ok(());
            }
            _ => {
                // Structured replies and metadata contexts land here. Declining them is
                // a supported answer; the client falls back to simple replies.
                send_option_reply(&mut writer, option, REP_ERR_UNSUP, &[])?;
                writer.flush()?;
            }
        }
    }

    // ---- transmission ------------------------------------------------------------
    transmit(&mut reader, &Mutex::new(writer), export)
}

/// What a request will do once a worker has it.
enum Work<'a> {
    Write(Pending<'a>),
    Read,
    Flush,
    Zero,
    Nothing,
    /// Refused by the reader (after draining whatever payload it owned).
    Fail(Error),
}

struct Job<'a> {
    handle: u64,
    cmd: u16,
    offset: u64,
    length: u32,
    cost: u64,
    work: Work<'a>,
}

/// A count of requests in flight and the bytes they hold, with a limit on each.
struct Slots {
    state: Mutex<(usize, u64)>,
    cv: Condvar,
}

impl Slots {
    fn new() -> Slots {
        Slots { state: Mutex::new((0, 0)), cv: Condvar::new() }
    }

    /// Wait for room, take it, and say how many requests are now in flight.
    fn acquire(&self, cost: u64) -> usize {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        // The byte limit never stops a request that would be the only one.
        while s.0 >= MAX_IN_FLIGHT || (s.0 > 0 && s.1 + cost > IN_FLIGHT_BYTES) {
            s = self.cv.wait(s).unwrap_or_else(|p| p.into_inner());
        }
        s.0 += 1;
        s.1 += cost;
        s.0
    }

    fn release(&self, cost: u64) {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        s.0 -= 1;
        s.1 -= cost;
        drop(s);
        self.cv.notify_all();
    }

    fn wait_idle(&self) {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        while s.0 > 0 {
            s = self.cv.wait(s).unwrap_or_else(|p| p.into_inner());
        }
    }
}

/// The transmission phase: read requests in order, run them concurrently, reply as they
/// finish.
fn transmit(
    reader: &mut BufReader<UnixStream>,
    writer: &Mutex<BufWriter<UnixStream>>,
    export: &Export,
) -> Result<()> {
    let slots = Slots::new();
    let dead = AtomicBool::new(false);
    let (tx, rx) = mpsc::channel::<Job<'_>>();
    let rx = Mutex::new(rx);

    std::thread::scope(|scope| {
        let tx = tx;
        let mut workers = 0usize;
        let outcome = (|| -> Result<()> {
            loop {
                if dead.load(Ordering::SeqCst) {
                    return Err(Error::io("nbd: the client stopped reading replies".to_string()));
                }
                let magic = read_u32(reader)?;
                if magic != REQUEST_MAGIC {
                    return Err(Error::refused(format!(
                        "nbd request magic {magic:#x} is not a request; the stream is desynchronised"
                    )));
                }
                let _cmd_flags = read_u16(reader)?;
                let cmd = read_u16(reader)?;
                let handle = read_u64(reader)?;
                let offset = read_u64(reader)?;
                let length = read_u32(reader)?;

                if cmd == CMD_DISC {
                    // Let what is in flight finish and reply; the client asked to stop, not
                    // to abandon its writes.
                    slots.wait_idle();
                    writer.lock().unwrap_or_else(|p| p.into_inner()).flush().ok();
                    return Ok(());
                }

                let (cost, work) = match cmd {
                    CMD_WRITE => {
                        if length > MAX_IO {
                            return Err(Error::refused(format!(
                                "nbd write of {length} bytes is out of range"
                            )));
                        }
                        // Room first, then the payload: the budget is what bounds the memory
                        // held for requests, and a payload not yet read costs the socket
                        // buffer, not this process.
                        let cost = length as u64;
                        let n = slots.acquire(cost);
                        while workers < n {
                            workers += 1;
                            scope.spawn(|| worker(&rx, &slots, writer, export, &dead));
                        }
                        // A WRITE's payload belongs to this request whatever happens next.
                        let mut buf = vec![0u8; length as usize];
                        read_exact(reader, &mut buf)?;
                        // Appended here, in arrival order; made durable on a worker.
                        let pending = export.backend.begin_write(offset, buf);
                        tx.send(Job { handle, cmd, offset, length, cost, work: Work::Write(pending) })
                            .map_err(|_| Error::io("nbd: the workers have gone".to_string()))?;
                        continue;
                    }
                    CMD_READ if length > MAX_IO => (
                        0,
                        Work::Fail(Error::refused(format!(
                            "nbd read of {length} bytes is out of range"
                        ))),
                    ),
                    CMD_READ => (length as u64, Work::Read),
                    CMD_FLUSH => (0, Work::Flush),
                    CMD_TRIM | CMD_WRITE_ZEROES => (0, Work::Zero),
                    // A cache hint we honour by doing nothing, which is a complete
                    // implementation of a hint.
                    CMD_CACHE => (0, Work::Nothing),
                    other => (
                        0,
                        Work::Fail(Error::refused(format!("nbd command {other} is not supported"))),
                    ),
                };
                let n = slots.acquire(cost);
                while workers < n {
                    workers += 1;
                    scope.spawn(|| worker(&rx, &slots, writer, export, &dead));
                }
                tx.send(Job { handle, cmd, offset, length, cost, work })
                    .map_err(|_| Error::io("nbd: the workers have gone".to_string()))?;
            }
        })();
        // Closing the channel is what ends the workers; the scope then waits for them, so
        // every request already accepted is finished (and replied to, if the client is
        // still there) before this returns.
        drop(tx);
        outcome
    })
}

/// One worker: take a request, run it, reply, repeat until the reader is done.
fn worker(
    rx: &Mutex<mpsc::Receiver<Job<'_>>>,
    slots: &Slots,
    writer: &Mutex<BufWriter<UnixStream>>,
    export: &Export,
    dead: &AtomicBool,
) {
    loop {
        let job = {
            let rx = rx.lock().unwrap_or_else(|p| p.into_inner());
            match rx.recv() {
                Ok(j) => j,
                Err(_) => return,
            }
        };
        if run(export, writer, slots, job).is_err() {
            dead.store(true, Ordering::SeqCst);
        }
    }
}

/// Run one request, reply to it, and give back its slot. Errs only if the reply could not be
/// sent, which means the client is gone.
fn run(
    export: &Export,
    writer: &Mutex<BufWriter<UnixStream>>,
    slots: &Slots,
    job: Job<'_>,
) -> Result<()> {
    let backend = export.backend.as_ref();
    let outcome: Result<Option<Vec<u8>>> = match job.work {
        Work::Write(pending) => pending().map(|_| None),
        Work::Read => backend.read(job.offset, job.length).map(Some),
        Work::Flush => backend.flush().map(|_| None),
        Work::Zero => backend.write_zeroes(job.offset, job.length as u64).map(|_| None),
        Work::Nothing => Ok(None),
        Work::Fail(e) => Err(e),
    };
    if let Err(e) = &outcome {
        eprintln!(
            "sidon: nbd {} cmd={} off={} len={}: {e}",
            export.name, job.cmd, job.offset, job.length
        );
    }
    let sent = reply(writer, job.handle, outcome);
    slots.release(job.cost);
    sent
}

/// Send one simple reply, whole, under the socket lock so replies never interleave. An error
/// reply carries no payload, so the stream stays in step and the guest sees an I/O error on
/// this request only.
fn reply(
    writer: &Mutex<BufWriter<UnixStream>>,
    handle: u64,
    outcome: Result<Option<Vec<u8>>>,
) -> Result<()> {
    let mut head = Vec::with_capacity(16);
    head.extend_from_slice(&SIMPLE_REPLY_MAGIC.to_be_bytes());
    let data = match outcome {
        Ok(d) => {
            head.extend_from_slice(&0u32.to_be_bytes());
            d
        }
        Err(e) => {
            head.extend_from_slice(&e.errno().to_be_bytes());
            None
        }
    };
    head.extend_from_slice(&handle.to_be_bytes());
    let mut w = writer.lock().unwrap_or_else(|p| p.into_inner());
    w.write_all(&head)?;
    if let Some(d) = data {
        w.write_all(&d)?;
    }
    w.flush()?;
    Ok(())
}

#[cfg(test)]
pub mod testclient {
    //! A minimal NBD client for tests: enough of the protocol to handshake and to issue
    //! requests with several in flight, and to read replies in whatever order they come.
    use super::*;
    use std::collections::HashMap;

    pub struct Client {
        pub sock: UnixStream,
        reads: HashMap<u64, u32>,
    }

    #[derive(Debug)]
    pub struct Reply {
        pub handle: u64,
        pub errno: u32,
        pub data: Vec<u8>,
    }

    /// Serve `export` on one end of a socket pair, on a thread, and complete the handshake on
    /// the other.
    pub fn connect(export: Arc<Export>) -> (Client, std::thread::JoinHandle<Result<()>>) {
        let (a, b) = UnixStream::pair().expect("socket pair");
        let server = std::thread::spawn(move || serve(b, &export));
        let mut sock = a;
        sock.set_read_timeout(Some(std::time::Duration::from_secs(20))).ok();

        let mut hello = [0u8; 18];
        sock.read_exact(&mut hello).expect("server hello");
        assert_eq!(u64::from_be_bytes(hello[0..8].try_into().unwrap()), NBDMAGIC);
        sock.write_all(&3u32.to_be_bytes()).unwrap(); // fixed newstyle, no zeroes

        let mut opt = Vec::new();
        opt.extend_from_slice(&IHAVEOPT.to_be_bytes());
        opt.extend_from_slice(&OPT_GO.to_be_bytes());
        let name = b"test";
        let mut data = Vec::new();
        data.extend_from_slice(&(name.len() as u32).to_be_bytes());
        data.extend_from_slice(name);
        data.extend_from_slice(&0u16.to_be_bytes());
        opt.extend_from_slice(&(data.len() as u32).to_be_bytes());
        opt.extend_from_slice(&data);
        sock.write_all(&opt).unwrap();
        loop {
            let mut head = [0u8; 20];
            sock.read_exact(&mut head).expect("option reply");
            let rep_type = u32::from_be_bytes(head[12..16].try_into().unwrap());
            let len = u32::from_be_bytes(head[16..20].try_into().unwrap()) as usize;
            let mut payload = vec![0u8; len];
            sock.read_exact(&mut payload).unwrap();
            if rep_type == REP_ACK {
                break;
            }
            assert_eq!(rep_type, REP_INFO);
        }
        (Client { sock, reads: HashMap::new() }, server)
    }

    impl Client {
        pub fn send(&mut self, flags: u16, cmd: u16, handle: u64, offset: u64, length: u32, payload: &[u8]) {
            if cmd == CMD_READ {
                self.reads.insert(handle, length);
            }
            let mut req = Vec::with_capacity(28 + payload.len());
            req.extend_from_slice(&REQUEST_MAGIC.to_be_bytes());
            req.extend_from_slice(&flags.to_be_bytes());
            req.extend_from_slice(&cmd.to_be_bytes());
            req.extend_from_slice(&handle.to_be_bytes());
            req.extend_from_slice(&offset.to_be_bytes());
            req.extend_from_slice(&length.to_be_bytes());
            req.extend_from_slice(payload);
            self.sock.write_all(&req).expect("send request");
        }

        pub fn write(&mut self, handle: u64, offset: u64, data: &[u8]) {
            self.send(0, CMD_WRITE, handle, offset, data.len() as u32, data);
        }

        pub fn read(&mut self, handle: u64, offset: u64, len: u32) {
            self.send(0, CMD_READ, handle, offset, len, &[]);
        }

        pub fn flush(&mut self, handle: u64) {
            self.send(0, CMD_FLUSH, handle, 0, 0, &[]);
        }

        /// The next reply, whichever request it answers.
        pub fn recv(&mut self) -> Reply {
            let mut head = [0u8; 16];
            self.sock.read_exact(&mut head).expect("a reply");
            assert_eq!(u32::from_be_bytes(head[0..4].try_into().unwrap()), SIMPLE_REPLY_MAGIC);
            let errno = u32::from_be_bytes(head[4..8].try_into().unwrap());
            let handle = u64::from_be_bytes(head[8..16].try_into().unwrap());
            let mut data = Vec::new();
            if errno == 0 {
                if let Some(len) = self.reads.remove(&handle) {
                    data = vec![0u8; len as usize];
                    self.sock.read_exact(&mut data).expect("read payload");
                }
            }
            Reply { handle, errno, data }
        }

        pub fn disconnect(&mut self) {
            self.send(0, CMD_DISC, 0, 0, 0, &[]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transmission_flags_advertise_what_is_implemented() {
        let f = transmission_flags(false);
        assert!(f & TX_HAS_FLAGS != 0);
        assert!(f & TX_SEND_FLUSH != 0);
        assert!(f & TX_SEND_TRIM != 0);
        assert!(f & TX_SEND_WRITE_ZEROES != 0);
        assert_eq!(f & TX_READ_ONLY, 0);
        // An immutable vdisk must announce itself read-only, or a guest will try to
        // write and only discover the refusal one EPERM at a time.
        assert!(transmission_flags(true) & TX_READ_ONLY != 0);
    }

    #[test]
    fn export_info_is_twelve_bytes_big_endian() {
        let p = export_info_payload(0x1000, false);
        assert_eq!(p.len(), 12);
        assert_eq!(u16::from_be_bytes([p[0], p[1]]), INFO_EXPORT);
        assert_eq!(
            u64::from_be_bytes([p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9]]),
            0x1000
        );
    }

    #[test]
    fn block_size_info_is_ordered_min_preferred_max() {
        let p = block_size_payload();
        assert_eq!(p.len(), 14);
        let min = u32::from_be_bytes([p[2], p[3], p[4], p[5]]);
        let pref = u32::from_be_bytes([p[6], p[7], p[8], p[9]]);
        let max = u32::from_be_bytes([p[10], p[11], p[12], p[13]]);
        assert!(min <= pref && pref <= max, "{min} {pref} {max}");
    }

    // ---- many requests in flight --------------------------------------------------

    use super::testclient::connect;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant};

    type ReadFn = Box<dyn Fn(u64, u32) -> Result<Vec<u8>> + Send + Sync>;
    type WriteFn = Box<dyn Fn(u64, &[u8]) -> Result<()> + Send + Sync>;

    /// A backend whose every operation is a closure, and which notes the order writes were
    /// *begun* in (on the reader's thread) as distinct from the order they finished in.
    struct Mock {
        read_fn: ReadFn,
        write_fn: WriteFn,
        begun: Mutex<Vec<u64>>,
        flushes: AtomicUsize,
    }

    impl Mock {
        fn new(read_fn: ReadFn, write_fn: WriteFn) -> Arc<Mock> {
            Arc::new(Mock { read_fn, write_fn, begun: Mutex::new(Vec::new()), flushes: AtomicUsize::new(0) })
        }
    }

    impl Backend for Mock {
        fn size(&self) -> u64 {
            1 << 30
        }
        fn read_only(&self) -> bool {
            false
        }
        fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
            (self.read_fn)(offset, len)
        }
        fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
            (self.write_fn)(offset, data)
        }
        fn begin_write<'a>(&'a self, offset: u64, data: Vec<u8>) -> Pending<'a> {
            self.begun.lock().unwrap().push(offset);
            Box::new(move || (self.write_fn)(offset, &data))
        }
        fn flush(&self) -> Result<()> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn write_zeroes(&self, _offset: u64, _len: u64) -> Result<()> {
            Ok(())
        }
    }

    fn export_of(b: Arc<Mock>) -> Arc<Export> {
        Arc::new(Export { backend: b, name: "test".to_string() })
    }

    fn pattern(offset: u64, len: u32) -> Vec<u8> {
        (0..len).map(|i| (offset as u32).wrapping_add(i) as u8).collect()
    }

    #[test]
    fn replies_complete_out_of_order_and_each_carries_its_own_handle() {
        // A write and a read are held until the requests behind them have been answered. A
        // server that serves one request at a time cannot get past the first and the test
        // times out.
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (g_read, g_write) = (Arc::clone(&gate), Arc::clone(&gate));
        let mock = Mock::new(
            Box::new(move |off, len| {
                if off == 0 {
                    let (m, cv) = &*g_read;
                    let mut open = m.lock().unwrap();
                    while !*open {
                        open = cv.wait(open).unwrap();
                    }
                }
                Ok(pattern(off, len))
            }),
            Box::new(move |_, _| {
                let (m, cv) = &*g_write;
                let mut open = m.lock().unwrap();
                while !*open {
                    open = cv.wait(open).unwrap();
                }
                Ok(())
            }),
        );
        let (mut c, server) = connect(export_of(Arc::clone(&mock)));

        c.write(1, 8192, &[5u8; 512]); // held
        c.read(2, 0, 512); // held
        c.read(3, 4096, 512);
        c.flush(4);
        let first = c.recv();
        let second = c.recv();
        let mut early = vec![first.handle, second.handle];
        early.sort();
        assert_eq!(early, vec![3, 4], "the later requests were answered while the first two were held");
        assert!(first.errno == 0 && second.errno == 0);
        let read3 = if first.handle == 3 { &first } else { &second };
        assert_eq!(read3.data, pattern(4096, 512));

        let (m, cv) = &*gate;
        *m.lock().unwrap() = true;
        cv.notify_all();
        let mut late = vec![c.recv(), c.recv()];
        late.sort_by_key(|r| r.handle);
        assert_eq!(late.iter().map(|r| r.handle).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(late[1].data, pattern(0, 512));

        c.disconnect();
        server.join().unwrap().unwrap();
    }

    #[test]
    fn a_run_of_single_requests_with_a_write_now_and_then_stays_in_step() {
        // One request at a time, with a write now and then: every reply is matched to its
        // request and the stream stays in step.
        let mock = Mock::new(Box::new(|off, len| Ok(pattern(off, len))), Box::new(|_, _| Ok(())));
        let (mut c, server) = connect(export_of(mock));
        for i in 0..50u64 {
            c.read(i, i * 512, 256);
            let r = c.recv();
            assert_eq!((r.handle, r.errno), (i, 0));
            assert_eq!(r.data, pattern(i * 512, 256));
            if i % 10 == 0 {
                c.write(1000 + i, 0, &[1u8; 64]);
                assert_eq!(c.recv().handle, 1000 + i);
            }
        }
        c.disconnect();
        server.join().unwrap().unwrap();
    }

    #[test]
    fn writes_are_begun_in_arrival_order_whatever_order_they_finish_in() {
        // Later writes finish first (the earlier the offset, the longer it takes), but the
        // reader has submitted them in the order they arrived: that order is what a local
        // vdisk turns into journal order.
        let mock = Mock::new(
            Box::new(|_, _| Ok(Vec::new())),
            Box::new(|off, _| {
                std::thread::sleep(Duration::from_millis(40 * (8 - off / 4096)));
                Ok(())
            }),
        );
        let (mut c, server) = connect(export_of(Arc::clone(&mock)));
        for i in 0..8u64 {
            c.write(100 + i, i * 4096, &[i as u8; 512]);
        }
        let mut done = Vec::new();
        for _ in 0..8 {
            let r = c.recv();
            assert_eq!(r.errno, 0);
            done.push(r.handle);
        }
        assert_eq!(*mock.begun.lock().unwrap(), (0..8).map(|i| i * 4096).collect::<Vec<u64>>());
        assert_eq!(done.first(), Some(&107), "the shortest write finished first: replies are in completion order");
        let all: HashSet<u64> = done.into_iter().collect();
        assert_eq!(all, (100..108).collect::<HashSet<u64>>());
        c.disconnect();
        server.join().unwrap().unwrap();
    }

    #[test]
    fn a_request_that_fails_with_others_in_flight_leaves_the_stream_in_step() {
        // A write that fails, its payload already in the stream, between a slow write and a
        // read: the payload must have been consumed before the error was reported, or the
        // read after it parses guest bytes as a header.
        let mock = Mock::new(
            Box::new(|off, len| Ok(pattern(off, len))),
            Box::new(|off, data| {
                if off == 4096 {
                    return Err(Error::io("injected".to_string()));
                }
                std::thread::sleep(Duration::from_millis(150));
                assert!(!data.is_empty());
                Ok(())
            }),
        );
        let (mut c, server) = connect(export_of(mock));
        c.write(1, 0, &[0xAAu8; 8192]);
        c.write(2, 4096, &[0xBBu8; 8192]);
        c.read(3, 8192, 1024);
        let mut got = std::collections::HashMap::new();
        for _ in 0..3 {
            let r = c.recv();
            got.insert(r.handle, r);
        }
        assert_eq!(got[&1].errno, 0);
        assert_eq!(got[&2].errno, 5, "the failing write is an I/O error, to that request only");
        assert_eq!(got[&3].errno, 0);
        assert_eq!(got[&3].data, pattern(8192, 1024));
        // And the connection still works.
        c.read(4, 0, 16);
        assert_eq!(c.recv().data, pattern(0, 16));
        c.disconnect();
        server.join().unwrap().unwrap();
    }

    #[test]
    fn requests_run_concurrently_and_no_more_than_the_limit_at_once() {
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (r2, p2) = (Arc::clone(&running), Arc::clone(&peak));
        let mock = Mock::new(
            Box::new(move |off, len| {
                let now = r2.fetch_add(1, Ordering::SeqCst) + 1;
                p2.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(100));
                r2.fetch_sub(1, Ordering::SeqCst);
                Ok(pattern(off, len))
            }),
            Box::new(|_, _| Ok(())),
        );
        let (mut c, server) = connect(export_of(mock));
        let t = Instant::now();
        let n = 3 * MAX_IN_FLIGHT as u64;
        for i in 0..n {
            c.read(i, i * 512, 64);
        }
        for _ in 0..n {
            assert_eq!(c.recv().errno, 0);
        }
        let took = t.elapsed();
        let peak = peak.load(Ordering::SeqCst);
        assert!(peak > 8, "requests overlapped (peak {peak})");
        assert!(peak <= MAX_IN_FLIGHT, "a connection never runs more than {MAX_IN_FLIGHT} at once (peak {peak})");
        assert!(took < Duration::from_millis(100 * n / 4), "{n} reads of 100ms took {took:?}");
        c.disconnect();
        server.join().unwrap().unwrap();
    }

    #[test]
    fn a_disconnect_waits_for_the_writes_in_flight_and_they_are_answered() {
        let done = Arc::new(AtomicBool::new(false));
        let d2 = Arc::clone(&done);
        let mock = Mock::new(
            Box::new(|_, _| Ok(Vec::new())),
            Box::new(move |_, _| {
                std::thread::sleep(Duration::from_millis(250));
                d2.store(true, Ordering::SeqCst);
                Ok(())
            }),
        );
        let (mut c, server) = connect(export_of(mock));
        c.write(9, 0, &[1u8; 4096]);
        c.disconnect();
        let r = c.recv();
        assert_eq!((r.handle, r.errno), (9, 0));
        assert!(done.load(Ordering::SeqCst), "the reply left only after the write finished");
        server.join().unwrap().unwrap();
    }

    #[test]
    fn a_fua_write_is_not_answered_before_it_is_done() {
        let done = Arc::new(AtomicBool::new(false));
        let d2 = Arc::clone(&done);
        let mock = Mock::new(
            Box::new(|_, _| Ok(Vec::new())),
            Box::new(move |_, _| {
                std::thread::sleep(Duration::from_millis(100));
                d2.store(true, Ordering::SeqCst);
                Ok(())
            }),
        );
        let (mut c, server) = connect(export_of(mock));
        c.send(CMD_FLAG_FUA, CMD_WRITE, 5, 0, 4096, &[7u8; 4096]);
        let r = c.recv();
        assert_eq!((r.handle, r.errno), (5, 0));
        assert!(done.load(Ordering::SeqCst));
        c.disconnect();
        server.join().unwrap().unwrap();
    }

    #[test]
    fn an_unknown_command_is_refused_and_the_stream_carries_on() {
        let mock = Mock::new(Box::new(|off, len| Ok(pattern(off, len))), Box::new(|_, _| Ok(())));
        let (mut c, server) = connect(export_of(mock));
        c.send(0, 77, 1, 0, 0, &[]);
        let r = c.recv();
        assert_eq!((r.handle, r.errno), (1, 1), "EPERM for a refusal");
        c.read(2, 8, 8);
        assert_eq!(c.recv().data, pattern(8, 8));
        c.disconnect();
        server.join().unwrap().unwrap();
    }
}
