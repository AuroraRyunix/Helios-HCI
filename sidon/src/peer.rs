//! The replication path between nodes.
//!
//! One connection per node *pair*, not per vdisk — the whole complaint about the
//! substrate this replaces. A node with a thousand vdisks replicating to two peers holds
//! two connections, not two thousand.
//!
//! ## What makes this safe
//!
//! Every append carries the epoch its writer holds, and **every replica remembers the
//! highest epoch it has been fenced at, on disk**. An append whose epoch is below that is
//! refused. That single rule is the entire safety mechanism: it works when the deposed
//! owner is wedged, when it is lying about its own state, when it cannot be reached, and
//! when it has no idea it was deposed. The lease exists for orderly handover and to bound
//! how long a loser keeps trying; it is not what makes anything safe.
//!
//! Persisting the fence is not optional. A replica that forgot its fence across a restart
//! would accept a zombie's writes again, which is the exact failure the epoch exists to
//! prevent — so the epoch file is fsynced before a fence is acknowledged.
//!
//! ## Wire format
//!
//! ```text
//! request:  magic u32 | opcode u16 | flags u16 | vdisk_len u16 | pad u16
//!           epoch u64 | seq u64 | offset u64 | data_len u32 | crc u32
//!           vdisk[vdisk_len] | data[data_len]
//!
//! response: magic u32 | status u16 | pad u16 | epoch u64 | data_len u32 | crc u32
//!           data[data_len]
//! ```
//!
//! The CRC covers the header-without-crc, the vdisk name and the payload. A frame that
//! fails it is a desynchronised stream, not a bad request, so the connection is dropped
//! rather than answered — answering would let a shifted stream be interpreted as a
//! sequence of plausible commands.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::crc::crc32c;
use crate::err::{Error, Result};
use crate::tls::{self, TlsMaterial, Wire};

pub const MAGIC: u32 = 0x5344_5052; // "SDPR"
pub const REQ_HEADER: usize = 44;
pub const RESP_HEADER: usize = 24;

pub const OP_PING: u16 = 1;
pub const OP_APPEND: u16 = 2;
pub const OP_FENCE: u16 = 3;
pub const OP_READ_TAIL: u16 = 4;
pub const OP_TRUNCATE: u16 = 5;
pub const OP_EGROUP_PUT: u16 = 6;
pub const OP_EGROUP_GET: u16 = 7;
/// Guest I/O relayed from a node that does not own the vdisk to the node that does.
/// `offset` is the guest offset; for a read, `seq` carries the length.
pub const OP_FORWARD_READ: u16 = 8;
pub const OP_FORWARD_WRITE: u16 = 9;

pub const ST_OK: u16 = 0;
/// The caller's epoch is below the highest this replica has been fenced at. The response
/// carries the fenced epoch so the caller learns it has been deposed rather than merely
/// that something went wrong.
pub const ST_STALE_EPOCH: u16 = 1;
pub const ST_IO: u16 = 2;
pub const ST_NOT_FOUND: u16 = 3;
pub const ST_REFUSED: u16 = 4;

/// Refuse a frame that claims more payload than any legitimate one carries, before
/// allocating for it.
const MAX_FRAME: u32 = 80 << 20;

#[derive(Debug)]
pub struct Request {
    pub opcode: u16,
    /// The name this operation is about. A vdisk id for journal operations; an **extent
    /// group id** for OP_EGROUP_PUT and OP_EGROUP_GET. One name field rather than two,
    /// because no operation needs both, and a second field that is empty most of the time
    /// is a field somebody eventually fills in wrongly.
    pub vdisk: String,
    pub epoch: u64,
    /// Sequence number for journal operations; **byte length** for OP_EGROUP_GET, which
    /// has to say how much to read and carries no payload of its own.
    pub seq: u64,
    pub offset: u64,
    pub flags: u16,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    /// On ST_STALE_EPOCH, the epoch this replica is fenced at.
    pub epoch: u64,
    pub data: Vec<u8>,
}

impl Response {
    pub fn ok(data: Vec<u8>) -> Response {
        Response { status: ST_OK, epoch: 0, data }
    }
    pub fn err(status: u16, epoch: u64) -> Response {
        Response { status, epoch, data: Vec::new() }
    }
    pub fn is_ok(&self) -> bool {
        self.status == ST_OK
    }
}

fn encode_request(r: &Request) -> Vec<u8> {
    let name = r.vdisk.as_bytes();
    let mut out = Vec::with_capacity(REQ_HEADER + name.len() + r.data.len());
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&r.opcode.to_le_bytes());
    out.extend_from_slice(&r.flags.to_le_bytes());
    out.extend_from_slice(&(name.len() as u16).to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&r.epoch.to_le_bytes());
    out.extend_from_slice(&r.seq.to_le_bytes());
    out.extend_from_slice(&r.offset.to_le_bytes());
    out.extend_from_slice(&(r.data.len() as u32).to_le_bytes());
    let crc = crc32c(crc32c(crc32c(0, &out[..40]), name), &r.data);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(name);
    out.extend_from_slice(&r.data);
    out
}

fn read_exact<R: Read>(r: &mut R, n: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).map_err(|e| Error::io(format!("peer read: {e}")))?;
    Ok(buf)
}

fn decode_request<R: Read>(r: &mut R) -> Result<Request> {
    let head = read_exact(r, REQ_HEADER)?;
    if u32::from_le_bytes(head[0..4].try_into().unwrap()) != MAGIC {
        return Err(Error::corrupt("peer frame magic is wrong".to_string()));
    }
    let opcode = u16::from_le_bytes(head[4..6].try_into().unwrap());
    let flags = u16::from_le_bytes(head[6..8].try_into().unwrap());
    let vdisk_len = u16::from_le_bytes(head[8..10].try_into().unwrap()) as usize;
    let epoch = u64::from_le_bytes(head[12..20].try_into().unwrap());
    let seq = u64::from_le_bytes(head[20..28].try_into().unwrap());
    let offset = u64::from_le_bytes(head[28..36].try_into().unwrap());
    let data_len = u32::from_le_bytes(head[36..40].try_into().unwrap());
    let want_crc = u32::from_le_bytes(head[40..44].try_into().unwrap());
    if data_len > MAX_FRAME || vdisk_len > 512 {
        return Err(Error::corrupt(format!(
            "peer frame claims {data_len} bytes for a {vdisk_len}-byte name"
        )));
    }
    let name = read_exact(r, vdisk_len)?;
    let data = read_exact(r, data_len as usize)?;
    if crc32c(crc32c(crc32c(0, &head[..40]), &name), &data) != want_crc {
        return Err(Error::corrupt("peer frame failed its checksum".to_string()));
    }
    Ok(Request {
        opcode,
        vdisk: String::from_utf8_lossy(&name).to_string(),
        epoch,
        seq,
        offset,
        flags,
        data,
    })
}

fn encode_response(resp: &Response) -> Vec<u8> {
    let mut out = Vec::with_capacity(RESP_HEADER + resp.data.len());
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&resp.status.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&resp.epoch.to_le_bytes());
    out.extend_from_slice(&(resp.data.len() as u32).to_le_bytes());
    let crc = crc32c(crc32c(0, &out[..20]), &resp.data);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&resp.data);
    out
}

fn decode_response<R: Read>(r: &mut R) -> Result<Response> {
    let head = read_exact(r, RESP_HEADER)?;
    if u32::from_le_bytes(head[0..4].try_into().unwrap()) != MAGIC {
        return Err(Error::corrupt("peer response magic is wrong".to_string()));
    }
    let status = u16::from_le_bytes(head[4..6].try_into().unwrap());
    let epoch = u64::from_le_bytes(head[8..16].try_into().unwrap());
    let data_len = u32::from_le_bytes(head[16..20].try_into().unwrap());
    let want_crc = u32::from_le_bytes(head[20..24].try_into().unwrap());
    if data_len > MAX_FRAME {
        return Err(Error::corrupt(format!("peer response claims {data_len} bytes")));
    }
    let data = read_exact(r, data_len as usize)?;
    if crc32c(crc32c(0, &head[..20]), &data) != want_crc {
        return Err(Error::corrupt("peer response failed its checksum".to_string()));
    }
    Ok(Response { status, epoch, data })
}

// ---------------------------------------------------------------------------------
// The replica side: what this node stores on behalf of a vdisk it does not own.
// ---------------------------------------------------------------------------------

/// Where a replica keeps another node's journal, and the fence it is holding for it.
pub struct ReplicaStore {
    root: PathBuf,
    /// vdisk -> highest fenced epoch. Cached, but the file is the truth.
    fenced: Mutex<HashMap<String, u64>>,
}

impl ReplicaStore {
    pub fn new(root: &Path) -> Result<ReplicaStore> {
        std::fs::create_dir_all(root.join("replica"))?;
        std::fs::create_dir_all(root.join("replica-egroups"))?;
        Ok(ReplicaStore { root: root.to_path_buf(), fenced: Mutex::new(HashMap::new()) })
    }

    fn journal_path(&self, vdisk: &str) -> PathBuf {
        self.root.join("replica").join(format!("{vdisk}.jrn"))
    }

    fn epoch_path(&self, vdisk: &str) -> PathBuf {
        self.root.join("replica").join(format!("{vdisk}.epoch"))
    }

    fn egroup_path(&self, egroup: &str) -> PathBuf {
        self.root.join("replica-egroups").join(format!("{egroup}.eg"))
    }

    /// The highest epoch this replica has been fenced at, read from disk on first use.
    pub fn fenced_epoch(&self, vdisk: &str) -> u64 {
        let mut cache = self.fenced.lock().expect("fence mutex poisoned");
        if let Some(e) = cache.get(vdisk) {
            return *e;
        }
        let value = std::fs::read_to_string(self.epoch_path(vdisk))
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        cache.insert(vdisk.to_string(), value);
        value
    }

    /// Record a fence. Durable before it is acknowledged, and never decreasing.
    ///
    /// If this were cached-only, a replica restart would forget the fence and start
    /// accepting the deposed owner's appends again — which is the precise failure the
    /// epoch exists to prevent, arriving by way of a power cut instead of a bug.
    pub fn fence(&self, vdisk: &str, epoch: u64) -> Result<u64> {
        let mut cache = self.fenced.lock().expect("fence mutex poisoned");
        let current = match cache.get(vdisk) {
            Some(e) => *e,
            None => std::fs::read_to_string(self.epoch_path(vdisk))
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(0),
        };
        if epoch <= current {
            // A fence that would lower the bar is refused, not applied. Epochs never go
            // backwards, so this is either a retry or a stale actor; both are answered
            // with the epoch actually in force.
            cache.insert(vdisk.to_string(), current);
            return Ok(current);
        }
        let path = self.epoch_path(vdisk);
        let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(&path)?;
        file.write_all(epoch.to_string().as_bytes())?;
        file.sync_all()?;
        cache.insert(vdisk.to_string(), epoch);
        Ok(epoch)
    }

    /// Append a replicated journal record, refusing anything from a fenced-out epoch.
    pub fn append(&self, vdisk: &str, epoch: u64, record: &[u8]) -> Result<()> {
        let fenced = self.fenced_epoch(vdisk);
        if epoch < fenced {
            return Err(Error::refused(format!(
                "append at epoch {epoch} refused: this replica is fenced at {fenced}"
            )));
        }
        let path = self.journal_path(vdisk);
        let mut file = OpenOptions::new().append(true).create(true).open(&path)?;
        file.write_all(record)?;
        // The guest's write is acknowledged only after every replica has synced, so this
        // is on the critical path by design -- it is what "durable on RF nodes" means.
        file.sync_data()?;
        Ok(())
    }

    pub fn read_tail(&self, vdisk: &str) -> Result<Vec<u8>> {
        match File::open(self.journal_path(vdisk)) {
            Ok(mut f) => {
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)?;
                Ok(buf)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(Error::io(format!("replica journal unreadable: {e}"))),
        }
    }

    /// Drop a replicated journal after the owner has drained it.
    pub fn truncate(&self, vdisk: &str, epoch: u64) -> Result<()> {
        let fenced = self.fenced_epoch(vdisk);
        if epoch < fenced {
            return Err(Error::refused(format!(
                "truncate at epoch {epoch} refused: this replica is fenced at {fenced}"
            )));
        }
        match std::fs::OpenOptions::new().write(true).open(self.journal_path(vdisk)) {
            Ok(f) => {
                f.set_len(0)?;
                f.sync_all()?;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io(format!("replica journal truncate: {e}"))),
        }
    }

    pub fn put_egroup(&self, egroup: &str, offset: u64, data: &[u8]) -> Result<()> {
        let path = self.egroup_path(egroup);
        let mut file = OpenOptions::new().write(true).create(true).open(&path)?;
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.sync_data()?;
        Ok(())
    }

    pub fn get_egroup(&self, egroup: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
        use std::io::{Seek, SeekFrom};
        let mut file = File::open(self.egroup_path(egroup))
            .map_err(|e| Error::io(format!("replica extent group {egroup}: {e}")))?;
        let mut buf = vec![0u8; len];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buf)
            .map_err(|e| Error::corrupt(format!("replica extent group {egroup} short: {e}")))?;
        Ok(buf)
    }
}

/// What can answer guest I/O for a vdisk this node owns.
///
/// A trait so the peer listener does not have to know about the daemon's attach table:
/// peer.rs stays a transport and a replica store, and the thing that owns vdisks passes
/// itself in. Forwarded I/O is the only reason the two need to meet at all.
pub trait Owned: Send + Sync {
    /// Read from a vdisk this node owns, or None if it does not own it.
    fn owned_read(&self, vdisk: &str, offset: u64, len: u32) -> Option<Result<Vec<u8>>>;
    /// Write to a vdisk this node owns, or None if it does not own it.
    fn owned_write(&self, vdisk: &str, offset: u64, data: &[u8]) -> Option<Result<()>>;
}

/// Answer one request against the local replica store.
pub fn serve_request(store: &ReplicaStore, req: &Request) -> Response {
    match req.opcode {
        OP_PING => Response::ok(Vec::new()),
        OP_FENCE => match store.fence(&req.vdisk, req.epoch) {
            Ok(now) => Response { status: ST_OK, epoch: now, data: Vec::new() },
            Err(e) => {
                eprintln!("sidon: peer fence {}: {e}", req.vdisk);
                Response::err(ST_IO, 0)
            }
        },
        OP_APPEND => match store.append(&req.vdisk, req.epoch, &req.data) {
            Ok(()) => Response::ok(Vec::new()),
            Err(Error::Refused(_)) => {
                Response::err(ST_STALE_EPOCH, store.fenced_epoch(&req.vdisk))
            }
            Err(e) => {
                eprintln!("sidon: peer append {}: {e}", req.vdisk);
                Response::err(ST_IO, 0)
            }
        },
        OP_READ_TAIL => match store.read_tail(&req.vdisk) {
            Ok(data) => Response::ok(data),
            Err(_) => Response::err(ST_IO, 0),
        },
        OP_TRUNCATE => match store.truncate(&req.vdisk, req.epoch) {
            Ok(()) => Response::ok(Vec::new()),
            Err(Error::Refused(_)) => {
                Response::err(ST_STALE_EPOCH, store.fenced_epoch(&req.vdisk))
            }
            Err(_) => Response::err(ST_IO, 0),
        },
        OP_EGROUP_PUT => match store.put_egroup(&req.vdisk, req.offset, &req.data) {
            Ok(()) => Response::ok(Vec::new()),
            Err(_) => Response::err(ST_IO, 0),
        },
        OP_EGROUP_GET => match store.get_egroup(&req.vdisk, req.offset, req.seq as usize) {
            Ok(data) => Response::ok(data),
            Err(Error::Io(_)) => Response::err(ST_NOT_FOUND, 0),
            Err(_) => Response::err(ST_IO, 0),
        },
        other => {
            eprintln!("sidon: peer sent unknown opcode {other}");
            Response::err(ST_REFUSED, 0)
        }
    }
}

/// Answer a request, trying forwarded guest I/O first.
pub fn serve_with_owner(store: &ReplicaStore, owner: &dyn Owned, req: &Request) -> Response {
    match req.opcode {
        OP_FORWARD_READ => match owner.owned_read(&req.vdisk, req.offset, req.seq as u32) {
            Some(Ok(data)) => Response::ok(data),
            Some(Err(e)) => {
                eprintln!("sidon: forwarded read of {}: {e}", req.vdisk);
                Response::err(ST_IO, 0)
            }
            // Not the owner either. The forwarder was working from a stale map; answering
            // NOT_FOUND lets it re-read ownership rather than retrying into a node that
            // will never be able to help.
            None => Response::err(ST_NOT_FOUND, 0),
        },
        OP_FORWARD_WRITE => match owner.owned_write(&req.vdisk, req.offset, &req.data) {
            Some(Ok(())) => Response::ok(Vec::new()),
            Some(Err(e)) => {
                eprintln!("sidon: forwarded write to {}: {e}", req.vdisk);
                Response::err(ST_IO, 0)
            }
            None => Response::err(ST_NOT_FOUND, 0),
        },
        _ => serve_request(store, req),
    }
}

/// The listener. One thread per peer connection; a connection carries every vdisk this
/// pair replicates, which is the whole point of the shape.
pub fn listen(bind: &str, store: Arc<ReplicaStore>, owner: Arc<dyn Owned>) -> Result<()> {
    // Plaintext replication must never leave the machine.
    //
    // Anything that is not loopback gets mutual TLS against the cluster CA, and a missing
    // or unreadable certificate is a refusal to start rather than a fall back to
    // plaintext. The guard is on the bind rather than on an operator's memory: a daemon
    // that quietly serves guest data in the clear because a file was absent is worse than
    // one that does not start.
    //
    // Loopback stays plaintext on purpose. A connection that cannot leave the host cannot
    // be intercepted off it, and it is how the protocol and the state machine are
    // exercised on a machine with no certificates at all.
    let material = if tls::is_loopback(bind) { None } else { TlsMaterial::load_default() };
    let tls: Option<Arc<TlsMaterial>> = if tls::wire_policy(bind, material.is_some())? {
        material.map(Arc::new)
    } else {
        None
    };

    let listener = TcpListener::bind(bind)
        .map_err(|e| Error::io(format!("cannot bind peer port {bind}: {e}")))?;
    println!(
        "sidon: replication listener on {bind} ({})",
        if tls.is_some() { "mutual TLS" } else { "plaintext, loopback only" }
    );
    thread::spawn(move || {
        for conn in listener.incoming() {
            match conn {
                Ok(stream) => {
                    let store = Arc::clone(&store);
                    let owner = Arc::clone(&owner);
                    let tls = tls.clone();
                    thread::spawn(move || {
                        stream.set_nodelay(true).ok();
                        // The handshake runs inside the per-connection thread, so a peer
                        // that opens a socket and never speaks costs one thread rather
                        // than blocking the accept loop for everyone.
                        let wire: Box<dyn Wire> = match &tls {
                            Some(m) => match m.accept(stream) {
                                Ok(w) => w,
                                Err(e) => {
                                    eprintln!("sidon: peer handshake refused: {e}");
                                    return;
                                }
                            },
                            None => Box::new(stream),
                        };
                        if let Err(e) = serve_connection(wire, &store, owner.as_ref()) {
                            eprintln!("sidon: peer connection ended: {e}");
                        }
                    });
                }
                Err(e) => {
                    eprintln!("sidon: peer accept failed: {e}");
                    break;
                }
            }
        }
    });
    Ok(())
}

fn serve_connection(mut stream: Box<dyn Wire>, store: &ReplicaStore, owner: &dyn Owned) -> Result<()> {
    loop {
        // A decode failure is a desynchronised stream, so the connection is dropped
        // rather than answered: replying would let shifted bytes be read as a plausible
        // sequence of commands.
        let req = decode_request(&mut stream)?;
        let resp = serve_with_owner(store, owner, &req);
        stream
            .write_all(&encode_response(&resp))
            .map_err(|e| Error::io(format!("peer write: {e}")))?;
    }
}

// ---------------------------------------------------------------------------------
// The client side: one connection per peer, re-established on failure.
// ---------------------------------------------------------------------------------

pub struct PeerClient {
    pub node: String,
    addr: String,
    timeout: Duration,
    /// How many times to try. Two for bulk replication, where the common failure is a
    /// peer that restarted between two appends and a reconnect fixes it. **One** for
    /// fencing: a retry there buys nothing -- safety needs only one replica fenced,
    /// because an append needs all of them -- and costs a second full timeout against a
    /// peer that is wedged rather than gone, which is the exact case a failover is racing.
    attempts: u32,
    conn: Mutex<Option<Box<dyn Wire>>>,
    /// Loaded once per client rather than per connection: building a rustls config parses
    /// PEM and validates the key against the certificate, which is not work to repeat on
    /// every reconnect of a flapping peer.
    tls: Option<Arc<TlsMaterial>>,
}

impl PeerClient {
    pub fn new(node: &str, addr: &str, timeout: Duration) -> PeerClient {
        PeerClient::with_attempts(node, addr, timeout, 2)
    }

    /// A client that gives up after `attempts` tries. Used for fencing.
    pub fn with_attempts(node: &str, addr: &str, timeout: Duration, attempts: u32) -> PeerClient {
        // Same rule as the listener, from the other end: loopback is plaintext, anything
        // else needs the cluster CA. A client with no material for a routable peer is
        // built anyway and fails at `call`, because refusing to construct it would turn a
        // certificate problem into a daemon that will not start.
        let tls = if tls::is_loopback(addr) {
            None
        } else {
            TlsMaterial::load_default().map(Arc::new)
        };
        PeerClient {
            node: node.to_string(),
            addr: addr.to_string(),
            timeout,
            attempts: attempts.max(1),
            conn: Mutex::new(None),
            tls,
        }
    }

    /// Send one request. Reconnects once on a transport failure, because the common case
    /// is a peer that restarted between two appends rather than one that is gone.
    pub fn call(&self, req: &Request) -> Result<Response> {
        let mut guard = self.conn.lock().expect("peer conn mutex poisoned");
        let last = self.attempts - 1;
        for attempt in 0..self.attempts {
            if guard.is_none() {
                match self.dial() {
                    Ok(s) => {
                        *guard = Some(s);
                    }
                    Err(e) => {
                        if attempt == last {
                            return Err(Error::io(format!(
                                "peer {} at {} is unreachable: {e}", self.node, self.addr
                            )));
                        }
                        continue;
                    }
                }
            }
            let stream = guard.as_mut().expect("just connected");
            let framed = encode_request(req);
            let outcome = stream
                .write_all(&framed)
                .map_err(|e| Error::io(format!("peer write: {e}")))
                .and_then(|_| decode_response(stream));
            match outcome {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    *guard = None;
                    if attempt == last {
                        return Err(e);
                    }
                }
            }
        }
        Err(Error::io(format!("peer {} did not answer", self.node)))
    }

    /// Open one connection, wrapped in TLS unless the peer is on this machine.
    fn dial(&self) -> Result<Box<dyn Wire>> {
        // The same rule the listener applied, from the other end.
        tls::wire_policy(&self.addr, self.tls.is_some())?;
        let sock = TcpStream::connect(&self.addr)
            .map_err(|e| Error::io(format!(
                "peer {} at {} is unreachable: {e}", self.node, self.addr
            )))?;
        sock.set_read_timeout(Some(self.timeout)).ok();
        sock.set_write_timeout(Some(self.timeout)).ok();
        sock.set_nodelay(true).ok();
        match &self.tls {
            Some(m) => m.connect(tls::server_name_for(&self.addr)?, sock),
            None => Ok(Box::new(sock)),
        }
    }

    pub fn ping(&self) -> Result<()> {
        let resp = self.call(&Request {
            opcode: OP_PING,
            vdisk: String::new(),
            epoch: 0,
            seq: 0,
            offset: 0,
            flags: 0,
            data: Vec::new(),
        })?;
        if resp.is_ok() {
            Ok(())
        } else {
            Err(Error::io(format!("peer {} answered ping with status {}", self.node, resp.status)))
        }
    }
}

/// Serves a vdisk by relaying every operation to the node that owns it.
///
/// Correct and slower, which is the whole trade. A VM can resume on a destination host
/// before its storage has moved, and the destination takes ownership at leisure -- so
/// there is no instant at which storage must hand off synchronously with the guest, and
/// the migration window that dual-primary existed to cover simply does not occur.
pub struct Forwarder {
    pub vdisk: String,
    pub size: u64,
    pub read_only: bool,
    pub owner: Arc<PeerClient>,
}

impl Forwarder {
    fn relay(&self, opcode: u16, offset: u64, len: u64, data: Vec<u8>) -> Result<Vec<u8>> {
        let resp = self.owner.call(&Request {
            opcode,
            vdisk: self.vdisk.clone(),
            epoch: 0,
            seq: len,
            offset,
            flags: 0,
            data,
        })?;
        match resp.status {
            ST_OK => Ok(resp.data),
            // The owner moved. Surfaced rather than retried: this node's view of
            // ownership is stale, and the control plane has to re-resolve it -- retrying
            // against a node that has already said "not mine" is a loop.
            ST_NOT_FOUND => Err(Error::refused(format!(
                "{} no longer owns {}; this node's forwarding target is stale",
                self.owner.node, self.vdisk
            ))),
            other => Err(Error::io(format!(
                "owner {} answered forwarded I/O for {} with status {other}",
                self.owner.node, self.vdisk
            ))),
        }
    }
}

impl crate::nbd::Backend for Forwarder {
    fn size(&self) -> u64 {
        self.size
    }
    fn read_only(&self) -> bool {
        self.read_only
    }
    fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        self.relay(OP_FORWARD_READ, offset, len as u64, Vec::new())
    }
    fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.relay(OP_FORWARD_WRITE, offset, data.len() as u64, data.to_vec()).map(|_| ())
    }
    fn flush(&self) -> Result<()> {
        // The owner acknowledges a forwarded write only after its own journal sync, so
        // by the time a write returns here it is already durable on every replica. There
        // is nothing weaker to flush.
        Ok(())
    }
    fn write_zeroes(&self, offset: u64, len: u64) -> Result<()> {
        let mut remaining = len;
        let mut at = offset;
        while remaining > 0 {
            let chunk = remaining.min(1 << 20) as usize;
            self.write(at, &vec![0u8; chunk])?;
            at += chunk as u64;
            remaining -= chunk as u64;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("sidon-peer-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn a_request_round_trips_through_its_own_framing() {
        let req = Request {
            opcode: OP_APPEND,
            vdisk: "vm-disk0".to_string(),
            epoch: 7,
            seq: 3,
            offset: 4096,
            flags: 1,
            data: vec![9u8; 300],
        };
        let framed = encode_request(&req);
        let back = decode_request(&mut &framed[..]).unwrap();
        assert_eq!(back.opcode, OP_APPEND);
        assert_eq!(back.vdisk, "vm-disk0");
        assert_eq!(back.epoch, 7);
        assert_eq!(back.seq, 3);
        assert_eq!(back.offset, 4096);
        assert_eq!(back.data.len(), 300);
    }

    #[test]
    fn a_flipped_byte_is_a_desynchronised_stream_not_a_request() {
        let req = Request {
            opcode: OP_APPEND, vdisk: "vd".to_string(), epoch: 1, seq: 0,
            offset: 0, flags: 0, data: vec![1, 2, 3],
        };
        let mut framed = encode_request(&req);
        let last = framed.len() - 1;
        framed[last] ^= 0xFF;
        match decode_request(&mut &framed[..]) {
            Err(Error::Corrupt(_)) => {}
            other => panic!("expected a corruption error, got {other:?}"),
        }
    }

    #[test]
    fn a_fence_survives_the_replica_forgetting_everything() {
        // The whole point of persisting it: a restarted replica that forgot its fence
        // would accept the deposed owner's writes again.
        let dir = tmpdir("fence-persist");
        {
            let store = ReplicaStore::new(&dir).unwrap();
            assert_eq!(store.fence("vd", 5).unwrap(), 5);
        }
        let store = ReplicaStore::new(&dir).unwrap();
        assert_eq!(store.fenced_epoch("vd"), 5);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_fence_never_goes_backwards() {
        let dir = tmpdir("fence-monotonic");
        let store = ReplicaStore::new(&dir).unwrap();
        assert_eq!(store.fence("vd", 9).unwrap(), 9);
        assert_eq!(store.fence("vd", 4).unwrap(), 9, "a lower fence must not apply");
        assert_eq!(store.fenced_epoch("vd"), 9);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_fenced_out_epoch_cannot_append() {
        let dir = tmpdir("stale-append");
        let store = ReplicaStore::new(&dir).unwrap();
        store.append("vd", 3, b"before the fence").unwrap();
        store.fence("vd", 4).unwrap();
        match store.append("vd", 3, b"the zombie writes") {
            Err(Error::Refused(m)) => assert!(m.contains("fenced at 4"), "{m}"),
            other => panic!("expected refusal, got {other:?}"),
        }
        // The new owner's epoch is accepted.
        store.append("vd", 4, b"the new owner writes").unwrap();
        let tail = store.read_tail("vd").unwrap();
        assert!(tail.starts_with(b"before the fence"));
        assert!(!String::from_utf8_lossy(&tail).contains("zombie"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_serve_layer_reports_stale_epoch_with_the_fence_in_force() {
        let dir = tmpdir("serve-stale");
        let store = ReplicaStore::new(&dir).unwrap();
        store.fence("vd", 11).unwrap();
        let resp = serve_request(&store, &Request {
            opcode: OP_APPEND, vdisk: "vd".to_string(), epoch: 10, seq: 0,
            offset: 0, flags: 0, data: vec![1],
        });
        assert_eq!(resp.status, ST_STALE_EPOCH);
        // The caller learns which epoch deposed it, not merely that it failed.
        assert_eq!(resp.epoch, 11);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_equal_epoch_is_not_stale() {
        // The owner that *set* the fence must be able to write at it. Only strictly
        // older epochs are refused.
        let dir = tmpdir("equal-epoch");
        let store = ReplicaStore::new(&dir).unwrap();
        store.fence("vd", 6).unwrap();
        store.append("vd", 6, b"the owner at the fenced epoch").unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn truncate_is_fenced_too() {
        // A deposed owner draining on its own timetable must not be able to erase the
        // journal the new owner is about to replay.
        let dir = tmpdir("fenced-truncate");
        let store = ReplicaStore::new(&dir).unwrap();
        store.append("vd", 2, b"acknowledged data").unwrap();
        store.fence("vd", 3).unwrap();
        assert!(store.truncate("vd", 2).is_err());
        assert_eq!(store.read_tail("vd").unwrap(), b"acknowledged data");
        store.truncate("vd", 3).unwrap();
        assert!(store.read_tail("vd").unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_routable_bind_is_refused_when_there_is_no_tls_material() {
        // The rule itself is checked in tls.rs, without a socket. This checks that
        // `listen` consults it: the guard is only worth anything if the code path that
        // opens the port actually asks.
        let dir = tmpdir("no-certs");
        std::env::set_var("SIDON_CERT_DIR", dir.join("absent"));
        struct NoVdisks;
        impl Owned for NoVdisks {
            fn owned_read(&self, _v: &str, _o: u64, _l: u32) -> Option<Result<Vec<u8>>> { None }
            fn owned_write(&self, _v: &str, _o: u64, _d: &[u8]) -> Option<Result<()>> { None }
        }
        let store = Arc::new(ReplicaStore::new(&dir).unwrap());
        let outcome = listen("10.255.255.1:9105", Arc::clone(&store), Arc::new(NoVdisks));
        std::env::remove_var("SIDON_CERT_DIR");
        match outcome {
            Err(Error::Refused(m)) => assert!(m.contains("plaintext"), "{m}"),
            other => panic!("a routable bind without certificates must be refused, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
