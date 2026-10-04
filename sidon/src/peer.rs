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
use std::os::unix::fs::FileExt;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::crc::crc32c;
use crate::err::{Error, Result};
use crate::heat::AccessLog;
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
/// Drop the drained prefix of a replicated journal: every record older than `seq`, keeping
/// the rest. A separate opcode from OP_TRUNCATE (which empties the file) so that a replica
/// running an older build answers "unknown opcode" and keeps its journal, instead of
/// reading a new request as the old one and emptying a journal that holds acknowledged
/// writes made while the drain ran.
pub const OP_TRUNCATE_TO: u16 = 10;
/// Ask the node that owns a vdisk to hand it over: drain the journal into extent groups,
/// stop serving the disk and let go of its socket. Sent by the node that is about to take
/// the disk over (a live migration's destination), which holds its own guest's I/O stalled
/// while it waits. A node that does not know the opcode answers ST_REFUSED, so a handover
/// toward an older build fails before anything changed rather than half way.
pub const OP_RELEASE: u16 = 11;

/// Request flag on OP_APPEND and OP_EGROUP_PUT: this is not the last write of its group, so
/// the replica need not fsync it -- the group's last write, sent without the flag, is synced
/// and takes every earlier write to the file with it. For an append the group is one guest
/// write; for a put it is one extent group within a drain. A replica from before this flag
/// ignores it and syncs every write, which is slower and no less safe.
pub const APPEND_DEFER_SYNC: u16 = 1;

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
    /// The journal volume, not the sidon root: replica state is as durable as a journal and
    /// lives beside it, on the same mount.
    volume: PathBuf,
    /// vdisk -> highest fenced epoch. Cached, but the file is the truth.
    fenced: Mutex<HashMap<String, u64>>,
    /// The node's extent-group access tally, when the daemon has one to share.
    ///
    /// A read served from here never goes through `Vdisk::read`, which is where the tally
    /// is otherwise fed: the vdisk belongs to another node and this node only holds a copy.
    /// So the replica store feeds it itself. In memory and flushed on Purah's timer, like
    /// every other count in it, which is what makes this free -- an access is a hash lookup
    /// and four adds, with no round trip to Hydra on the read.
    access: Option<Arc<AccessLog>>,
}

impl ReplicaStore {
    pub fn new(volume: &Path) -> Result<ReplicaStore> {
        std::fs::create_dir_all(volume.join("replica"))?;
        std::fs::create_dir_all(volume.join("replica-egroups"))?;
        Ok(ReplicaStore {
            volume: volume.to_path_buf(),
            fenced: Mutex::new(HashMap::new()),
            access: None,
        })
    }

    /// Share the node's access tally, so the reads this replica serves count toward the
    /// heat of the extent groups it holds. Without it a group read mostly through its
    /// replicas ranks as cold on the node that serves it.
    pub fn with_access(mut self, access: Arc<AccessLog>) -> ReplicaStore {
        self.access = Some(access);
        self
    }

    fn journal_path(&self, vdisk: &str) -> PathBuf {
        self.volume.join("replica").join(format!("{vdisk}.jrn"))
    }

    fn epoch_path(&self, vdisk: &str) -> PathBuf {
        self.volume.join("replica").join(format!("{vdisk}.epoch"))
    }

    fn egroup_path(&self, egroup: &str) -> PathBuf {
        self.volume.join("replica-egroups").join(format!("{egroup}.eg"))
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
    #[cfg(test)]
    pub fn append(&self, vdisk: &str, epoch: u64, record: &[u8]) -> Result<()> {
        self.append_deferring(vdisk, epoch, record, false)
    }

    /// `append`, optionally leaving the fsync to a later record.
    ///
    /// With `defer_sync` the record is written and *not* made durable, and the reply says
    /// nothing about durability. The owner only does this for a record that is not the last
    /// of its group, and sends the last without the flag: that record's `sync_data` flushes
    /// every earlier write to the file, so by the time the owner hears an OK it needs --
    /// the one it acknowledges the guest on -- the whole group is on this disk. A replica
    /// that never sees the last record (the owner died) holds records that were never
    /// acknowledged and, with no commit marker, never applied.
    pub fn append_deferring(
        &self,
        vdisk: &str,
        epoch: u64,
        record: &[u8],
        defer_sync: bool,
    ) -> Result<()> {
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
        // is on the critical path by design -- it is what "durable on RF nodes" means. For
        // a record the owner marked deferred, the sync that matters is the last record's.
        if !defer_sync {
            file.sync_data()?;
        }
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

    /// Drop the drained *prefix* of a replicated journal: every record older than
    /// `keep_seq`, and nothing else.
    ///
    /// This is what a drain that ran beside guest writes has to ask for. `truncate` empties
    /// the file, which is right only when nothing was appended since the records being
    /// dropped -- and with the drain on its own thread, records acknowledged while it ran
    /// are in this file already and are the *only* copy of those writes that is not also
    /// in the owner's journal. Dropping them would be dropping acknowledged data from a
    /// replica, so the cut is made by sequence number, at a record boundary found by
    /// reading headers, and what follows it is carried over intact.
    ///
    /// The rewrite goes through a temporary file and a rename, so a crash leaves either the
    /// old journal (a superset, which replay handles because re-applying drained records is
    /// idempotent) or the new one -- never a journal with its middle missing.
    pub fn truncate_before(&self, vdisk: &str, epoch: u64, keep_seq: u64) -> Result<()> {
        let fenced = self.fenced_epoch(vdisk);
        if epoch < fenced {
            return Err(Error::refused(format!(
                "truncate at epoch {epoch} refused: this replica is fenced at {fenced}"
            )));
        }
        let path = self.journal_path(vdisk);
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(Error::io(format!("replica journal truncate: {e}"))),
        };
        let total = file.metadata()?.len();

        // Walk record headers to the first record at or after `keep_seq`.
        let mut pos = 0u64;
        let mut cut: Option<u64> = None;
        while pos + crate::journal::HEADER_LEN as u64 <= total {
            let mut head = [0u8; crate::journal::HEADER_LEN];
            file.read_exact_at(&mut head, pos)?;
            if u32::from_le_bytes(head[0..4].try_into().unwrap()) != crate::journal::MAGIC {
                break;
            }
            let data_len = u32::from_le_bytes(head[4..8].try_into().unwrap()) as u64;
            let seq = u64::from_le_bytes(head[8..16].try_into().unwrap());
            let end = pos + crate::journal::HEADER_LEN as u64 + data_len;
            if end > total {
                break;
            }
            if seq >= keep_seq {
                cut = Some(pos);
                break;
            }
            pos = end;
        }
        let cut = match cut {
            Some(c) => c,
            // Every record is older than the cut: all of it is drained.
            None if pos == total => total,
            // The walk stopped on something that is not a whole record, with no record at
            // or after the cut seen. Guessing which side of it the cut falls on is how an
            // acknowledged record gets dropped, so leave the file as it is.
            None => {
                return Err(Error::corrupt(format!(
                    "replica journal of {vdisk} is not a clean record stream at byte {pos}; \
                     not truncating it"
                )))
            }
        };
        if cut == 0 {
            return Ok(());
        }
        if cut == total {
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(0)?;
            f.sync_all()?;
            return Ok(());
        }
        let mut rest = vec![0u8; (total - cut) as usize];
        file.read_exact_at(&mut rest, cut)?;
        let tmp = path.with_extension("jrn.tmp");
        {
            let mut out = OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;
            out.write_all(&rest)?;
            out.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        File::open(path.parent().unwrap_or(Path::new(".")))?.sync_all()?;
        Ok(())
    }

    #[cfg(test)]
    pub fn put_egroup(&self, egroup: &str, offset: u64, data: &[u8]) -> Result<()> {
        self.put_egroup_deferring(egroup, offset, data, false)
    }

    /// `put_egroup`, optionally leaving the fsync to a later put to the same group.
    ///
    /// A drain appends a group's extents one after another and the owner will not point the
    /// block map at any of them until every one is durable. So all but the last put to a
    /// group can skip the sync: the last one's `sync_data` flushes the whole file, and the
    /// owner marks the drain committed only after that last reply. What this saves is a
    /// disk flush per extent, which on this hardware cost more than the transfer did.
    pub fn put_egroup_deferring(
        &self,
        egroup: &str,
        offset: u64,
        data: &[u8],
        defer_sync: bool,
    ) -> Result<()> {
        let path = self.egroup_path(egroup);
        let mut file = OpenOptions::new().write(true).create(true).open(&path)?;
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        if !defer_sync {
            file.sync_data()?;
        }
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
        // Counted after the bytes are in hand, so a read that failed is not recorded as an
        // access to a group nobody could read -- the same rule `Vdisk::read` follows.
        if let Some(access) = &self.access {
            access.record_read(egroup, len as u64, crate::meta::now_ms());
        }
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
    /// Drain and stop serving a vdisk this node owns, for a handover to the asking node.
    /// None if it does not serve the disk at all.
    fn owned_release(&self, _vdisk: &str) -> Option<Result<()>> {
        None
    }
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
        OP_APPEND => match store.append_deferring(
            &req.vdisk,
            req.epoch,
            &req.data,
            req.flags & APPEND_DEFER_SYNC != 0,
        ) {
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
        OP_TRUNCATE_TO => match store.truncate_before(&req.vdisk, req.epoch, req.seq) {
            Ok(()) => Response::ok(Vec::new()),
            Err(Error::Refused(_)) => {
                Response::err(ST_STALE_EPOCH, store.fenced_epoch(&req.vdisk))
            }
            Err(e) => {
                eprintln!("sidon: peer truncate-to {}: {e}", req.vdisk);
                Response::err(ST_IO, 0)
            }
        },
        OP_EGROUP_PUT => match store.put_egroup_deferring(
            &req.vdisk,
            req.offset,
            &req.data,
            req.flags & APPEND_DEFER_SYNC != 0,
        ) {
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
        OP_RELEASE => match owner.owned_release(&req.vdisk) {
            Some(Ok(())) => Response::ok(Vec::new()),
            Some(Err(e)) => {
                eprintln!("sidon: release of {} refused: {e}", req.vdisk);
                // The reason travels back: the asking node reports which step failed.
                Response { status: ST_REFUSED, epoch: 0, data: e.to_string().into_bytes() }
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
                        let handler = |req: &Request| serve_with_owner(&store, owner.as_ref(), req);
                        if let Err(e) = serve_connection(wire, &handler) {
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

/// A replication server on a loopback port, answering every request with `handler`, for
/// tests that need a real replica -- a real socket, real framing, a real `ReplicaStore`
/// behind it -- with the option of misbehaving: refusing, stalling, or recording what it
/// was sent. Returns the address to dial.
#[cfg(test)]
pub fn spawn_test_server(handler: Arc<dyn Fn(&Request) -> Response + Send + Sync>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback port");
    let addr = listener.local_addr().expect("local addr").to_string();
    thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(stream) = conn else { break };
            stream.set_nodelay(true).ok();
            let handler = Arc::clone(&handler);
            thread::spawn(move || {
                let h = |req: &Request| handler(req);
                let _ = serve_connection(Box::new(stream), &h);
            });
        }
    });
    addr
}

fn serve_connection(mut stream: Box<dyn Wire>, handler: &dyn Fn(&Request) -> Response) -> Result<()> {
    loop {
        // A decode failure is a desynchronised stream, so the connection is dropped
        // rather than answered: replying would let shifted bytes be read as a plausible
        // sequence of commands.
        let req = decode_request(&mut stream)?;
        let resp = handler(&req);
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

    /// A client for the same peer with its own connection, timeout and attempt count.
    ///
    /// A handover asks the owner to drain a journal, which takes as long as the journal is
    /// full and not as long as an append, and it must not share a connection with guest I/O
    /// that is being forwarded to the same peer.
    pub fn derive(&self, timeout: Duration, attempts: u32) -> PeerClient {
        PeerClient::with_attempts(&self.node, &self.addr, timeout, attempts)
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

    /// The property: a read this node serves to another node's vdisk is counted.
    ///
    /// Such a read reaches `ReplicaStore::get_egroup` and never `Vdisk::read`, which is the
    /// only other place the tally is fed -- so before the replica store shared the tally, a
    /// group read mostly through its replicas ranked as cold on the node that was serving
    /// it, which is the node a placement decision would be made on.
    #[test]
    fn a_read_served_to_another_nodes_vdisk_is_tallied() {
        let dir = tmpdir("replica-read-tally");
        let access = Arc::new(AccessLog::new(16, 0));
        let store = ReplicaStore::new(&dir).unwrap().with_access(Arc::clone(&access));
        store.put_egroup("eg-a", 0, &[7u8; 4096]).unwrap();

        // Through the serve layer, as a peer's request arrives.
        let resp = serve_request(&store, &Request {
            opcode: OP_EGROUP_GET,
            vdisk: "eg-a".to_string(),
            epoch: 1,
            seq: 4096,
            offset: 0,
            flags: 0,
            data: Vec::new(),
        });
        assert!(resp.is_ok());

        let sample = access.sample(10);
        assert_eq!(sample.rows.len(), 1);
        let (id, counts) = &sample.rows[0];
        assert_eq!(id, "eg-a");
        assert_eq!((counts.reads, counts.bytes_read), (1, 4096));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_replica_read_that_failed_is_not_an_access() {
        // The same rule Vdisk::read follows: a group nobody could read is not wanted-here
        // evidence, and counting it would rank a missing file as warm.
        let dir = tmpdir("replica-read-miss");
        let access = Arc::new(AccessLog::new(16, 0));
        let store = ReplicaStore::new(&dir).unwrap().with_access(Arc::clone(&access));
        assert!(store.get_egroup("absent", 0, 4096).is_err());
        assert!(access.sample(10).rows.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_replica_store_with_no_tally_still_serves_reads() {
        let dir = tmpdir("replica-read-untallied");
        let store = ReplicaStore::new(&dir).unwrap();
        store.put_egroup("eg-a", 0, b"bytes").unwrap();
        assert_eq!(store.get_egroup("eg-a", 0, 5).unwrap(), b"bytes");
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

    /// `n` framed journal records with sequence numbers `first..first+n`.
    fn records(first: u64, n: u64) -> Vec<Vec<u8>> {
        (first..first + n)
            .map(|s| crate::journal::Journal::encode(s, 1, s * 4096, 0, &[s as u8; 64]))
            .collect()
    }

    #[test]
    fn truncating_before_a_sequence_drops_only_the_drained_prefix() {
        // The point of the opcode: a drain that ran beside guest writes has records in the
        // replica's journal that it never saw. They are acknowledged data, and a replica
        // that emptied its journal would be the only place they were lost.
        let dir = tmpdir("truncate-before");
        let store = ReplicaStore::new(&dir).unwrap();
        let all = records(0, 6);
        for r in &all {
            store.append("vd", 1, r).unwrap();
        }
        store.truncate_before("vd", 1, 4).unwrap();
        let tail = store.read_tail("vd").unwrap();
        assert_eq!(tail, [all[4].clone(), all[5].clone()].concat());
        // No temporary file is left beside it.
        assert!(!store.journal_path("vd").with_extension("jrn.tmp").exists());
        // It is a normal journal still: the owner's next append lands after the kept records.
        let next = records(6, 1);
        store.append("vd", 1, &next[0]).unwrap();
        assert!(store.read_tail("vd").unwrap().ends_with(&next[0]));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn truncating_before_the_first_record_changes_nothing_and_past_the_last_empties_it() {
        let dir = tmpdir("truncate-before-edges");
        let store = ReplicaStore::new(&dir).unwrap();
        let all = records(10, 3);
        for r in &all {
            store.append("vd", 1, r).unwrap();
        }
        store.truncate_before("vd", 1, 0).unwrap();
        store.truncate_before("vd", 1, 10).unwrap();
        assert_eq!(store.read_tail("vd").unwrap(), all.concat());
        store.truncate_before("vd", 1, 13).unwrap();
        assert!(store.read_tail("vd").unwrap().is_empty());
        // And a vdisk this replica has no journal for is not an error.
        store.truncate_before("never-seen", 1, 5).unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn truncating_before_is_fenced_like_every_other_write_to_the_journal() {
        let dir = tmpdir("truncate-before-fenced");
        let store = ReplicaStore::new(&dir).unwrap();
        let all = records(0, 3);
        for r in &all {
            store.append("vd", 2, r).unwrap();
        }
        store.fence("vd", 3).unwrap();
        assert!(matches!(store.truncate_before("vd", 2, 2), Err(Error::Refused(_))));
        assert_eq!(store.read_tail("vd").unwrap(), all.concat());
        // Through the serve layer a deposed owner is told so, not that something broke.
        let resp = serve_request(&store, &Request {
            opcode: OP_TRUNCATE_TO, vdisk: "vd".to_string(), epoch: 2, seq: 2,
            offset: 0, flags: 0, data: Vec::new(),
        });
        assert_eq!((resp.status, resp.epoch), (ST_STALE_EPOCH, 3));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_journal_that_is_not_a_clean_record_stream_is_left_alone() {
        // Guessing where the cut falls inside bytes that are not records is how an
        // acknowledged one gets dropped, so the replica declines and the owner logs it.
        let dir = tmpdir("truncate-before-garbage");
        let store = ReplicaStore::new(&dir).unwrap();
        store.append("vd", 1, &records(0, 1)[0]).unwrap();
        store.append("vd", 1, b"this is not a journal record at all, just bytes").unwrap();
        let before = store.read_tail("vd").unwrap();
        assert!(store.truncate_before("vd", 1, 5).is_err());
        assert_eq!(store.read_tail("vd").unwrap(), before);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn deferring_the_sync_changes_when_bytes_are_durable_and_never_which_bytes_are_written() {
        // Through the serve layer, as a drain's pipeline sends them: three extents of one
        // group, the last without the flag.
        let dir = tmpdir("put-deferred");
        let store = ReplicaStore::new(&dir).unwrap();
        for (i, (chunk, flags)) in [(b"aaaa", APPEND_DEFER_SYNC), (b"bbbb", APPEND_DEFER_SYNC), (b"cccc", 0)]
            .into_iter()
            .enumerate()
        {
            let resp = serve_request(&store, &Request {
                opcode: OP_EGROUP_PUT, vdisk: "eg-a".to_string(), epoch: 1, seq: 0,
                offset: (i * 4) as u64, flags, data: chunk.to_vec(),
            });
            assert!(resp.is_ok());
        }
        assert_eq!(store.get_egroup("eg-a", 0, 12).unwrap(), b"aaaabbbbcccc");

        // And a deferred append is a whole record on disk all the same.
        let rec = records(0, 1).remove(0);
        let resp = serve_request(&store, &Request {
            opcode: OP_APPEND, vdisk: "vd".to_string(), epoch: 1, seq: 0,
            offset: 0, flags: APPEND_DEFER_SYNC, data: rec.clone(),
        });
        assert!(resp.is_ok());
        assert_eq!(store.read_tail("vd").unwrap(), rec);
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
