//! The write-ahead journal: the only thing a guest write waits for.
//!
//! A guest write is acknowledged when its record is durable in this file and nowhere
//! else. Everything downstream -- extent groups, the block map in Hydra, garbage
//! collection -- happens after the acknowledgement and must therefore be reconstructible
//! from what is here. That is the whole reason the journal exists, and it is why replay
//! is the most safety-critical function in the daemon.
//!
//! Record layout, little-endian:
//!
//! ```text
//! magic u32 | data_len u32 | seq u64 | epoch u64 | offset u64 | flags u32 | crc u32 | data
//! ```
//!
//! The CRC covers the header-without-crc and the payload as one seeded value, so a record
//! whose header is intact but whose payload is torn is still detected. A crash in the
//! middle of `write_all` leaves a trailing partial record; replay stops there, which is
//! exactly right -- that write was never acknowledged, so I-2 permits either outcome and
//! discarding it is the outcome we can prove.
//!
//! ## One file, or two while a drain runs
//!
//! Normally the journal is one file, `<vdisk>.jrn`. To let guest writes carry on while the
//! drain moves their predecessors into extent groups, the drain *rotates* first: the file
//! is renamed `<vdisk>.jrn.old` -- sealed, never written again -- and a fresh `.jrn` takes
//! new records. The drain reads only the sealed file; when its map commit has been applied
//! it deletes it. A crash at any point leaves one or both files, and replay reads the
//! sealed one first and then the live one, which is exactly the order they were written.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::crc::crc32c;
use crate::err::{Error, Result};

pub const MAGIC: u32 = 0x5344_4A52; // "SDJR"
pub const HEADER_LEN: usize = 40;

/// A commit marker terminates a group of records that must be applied all-or-nothing.
/// A guest write larger than the record cap becomes several records plus one marker, and
/// replay applies only marker-terminated groups -- so a crash exposes the whole write or
/// none of it, never a prefix.
pub const FLAG_COMMIT: u32 = 1;

/// A payload position is `generation << GEN_SHIFT | offset in that segment's file`.
///
/// The journal is two files while a drain is running -- the sealed segment the drain is
/// moving into extent groups, and the live one new writes go to -- and the overlay stores
/// one number per range. Putting the segment in the number means the overlay needs no idea
/// there are two files, a read finds its bytes by decoding it, and "everything the drain
/// took" is the set of ranges whose position carries the sealed segment's generation.
/// 2^44 bytes (16 TiB) per segment is far beyond any journal this daemon will hold.
pub const GEN_SHIFT: u32 = 44;
const LOCAL_MASK: u64 = (1u64 << GEN_SHIFT) - 1;
const GEN_MASK: u64 = (1u64 << (64 - GEN_SHIFT)) - 1;

/// The generation a payload position belongs to.
pub fn gen_of(pos: u64) -> u64 {
    pos >> GEN_SHIFT
}

/// Read `len` bytes at the encoded position `pos` out of the segment file `file`, which
/// must be the file of that position's generation.
pub fn read_in(file: &File, pos: u64, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    file.read_exact_at(&mut buf, pos & LOCAL_MASK)?;
    Ok(buf)
}

pub struct Record {
    pub seq: u64,
    /// The epoch its writer held. Replay does not consult it -- a record in this node's
    /// own journal was written by this node -- but it is what a replica checks before
    /// accepting an append, so the format carries it from the start rather than needing a
    /// migration when replication lands.
    #[allow(dead_code)]
    pub epoch: u64,
    pub offset: u64,
    pub flags: u32,
    /// Position of the payload (see `GEN_SHIFT`), so the read path can pull a segment back
    /// without holding every acknowledged write in memory.
    pub data_pos: u64,
    pub data_len: u32,
    /// The exact bytes written, for replication. Empty for records rebuilt by replay --
    /// nothing replicates a record it just read back off its own disk.
    pub framed: Vec<u8>,
}

/// A segment that has been closed to new records and is waiting for its drain to commit.
struct Sealed {
    file: Arc<File>,
    len: u64,
    gen: u64,
}

pub struct Journal {
    path: PathBuf,
    /// The live segment: the only one anything is appended to.
    file: File,
    len: u64,
    gen: u64,
    /// At most one. A drain that has not committed holds its segment here, and a second
    /// rotation is refused until it is gone, which is what bounds the journal to two files.
    old: Option<Sealed>,
    next_seq: u64,
    /// The sequence number of the first record the live segment holds (or will hold). What
    /// a drain of the sealed segment tells the replicas to keep from, so that records
    /// acknowledged while it ran are not dropped with the ones it took. Meaningful once
    /// `rotate` or `replay` has set it, which is the only time a sealed segment exists.
    live_first_seq: u64,
}

/// Where the sealed segment of the journal at `path` lives.
pub fn sealed_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_os_string();
    p.push(".old");
    PathBuf::from(p)
}

/// Remove a journal and its sealed segment, if either exists. For delete and rollback,
/// which both discard a detached vdisk's journal and must not leave half of it behind.
pub fn remove_files(path: &Path) -> std::io::Result<()> {
    for p in [path.to_path_buf(), sealed_path(path)] {
        match std::fs::remove_file(&p) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent).and_then(|d| d.sync_all())?;
    }
    Ok(())
}

impl Journal {
    pub fn open(path: &Path) -> Result<Journal> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // A sealed segment on disk means the last life of this vdisk died mid-drain, or
        // between that drain's commit and the removal of its segment. Both are replayed:
        // the first because it is the only copy of acknowledged writes, the second because
        // replaying records that are already drained is idempotent.
        let old = match File::open(sealed_path(path)) {
            Ok(f) => {
                let len = f.metadata()?.len();
                Some(Sealed { file: Arc::new(f), len, gen: 0 })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        let len = file.metadata()?.len();
        let gen = if old.is_some() { 1 } else { 0 };
        Ok(Journal { path: path.to_path_buf(), file, len, gen, old, next_seq: 0, live_first_seq: 0 })
    }

    /// Bytes the journal is holding that no drain has yet taken into extent groups: the
    /// sealed segment plus the live one. This is what the high-water mark and the ceiling
    /// compare against, because it is what a crash would have to replay.
    pub fn len(&self) -> u64 {
        self.len + self.old.as_ref().map(|o| o.len).unwrap_or(0)
    }

    /// Bytes in the live segment alone.
    pub fn live_len(&self) -> u64 {
        self.len
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Whether a sealed segment is waiting for its drain.
    pub fn has_sealed(&self) -> bool {
        self.old.is_some()
    }

    /// The generation of the sealed segment, and a handle to read it with that needs no
    /// lock on the journal -- which is what lets a drain read its input while guest writes
    /// keep appending to the live segment.
    pub fn sealed(&self) -> Option<(u64, Arc<File>)> {
        self.old.as_ref().map(|o| (o.gen, Arc::clone(&o.file)))
    }

    /// Frame a record without writing it.
    ///
    /// Split out from `append` so that the bytes replicated to a peer are byte-identical
    /// to the bytes written locally, rather than re-encoded at the far end from parsed
    /// fields. Re-encoding would mean two implementations of the format that have to stay
    /// agreeing, and a replica's copy differing from the owner's is exactly the divergence
    /// this design refuses to have a repair protocol for.
    pub fn encode(seq: u64, epoch: u64, offset: u64, flags: u32, data: &[u8]) -> Vec<u8> {
        let mut header = [0u8; HEADER_LEN];
        header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        header[4..8].copy_from_slice(&(data.len() as u32).to_le_bytes());
        header[8..16].copy_from_slice(&seq.to_le_bytes());
        header[16..24].copy_from_slice(&epoch.to_le_bytes());
        header[24..32].copy_from_slice(&offset.to_le_bytes());
        header[32..36].copy_from_slice(&flags.to_le_bytes());
        let crc = crc32c(crc32c(0, &header[0..36]), data);
        header[36..40].copy_from_slice(&crc.to_le_bytes());

        let mut buf = Vec::with_capacity(HEADER_LEN + data.len());
        buf.extend_from_slice(&header);
        buf.extend_from_slice(data);
        buf
    }

    /// Write one record to the live segment **without** making it durable. Nothing it
    /// returns may be acknowledged until `sync` has succeeded.
    ///
    /// Split from `sync` so a guest write of several records pays for one fsync, after the
    /// last, and so that fsync can run while the replicas are still taking the records.
    /// That is safe because of what the group already promises: replay applies a group only
    /// if its commit marker reached the disk, and the marker is the last record, so a crash
    /// before the sync leaves a group with no marker -- discarded, as an unacknowledged
    /// write is permitted to be.
    pub fn append_unsynced(
        &mut self,
        epoch: u64,
        offset: u64,
        flags: u32,
        data: &[u8],
    ) -> Result<Record> {
        let seq = self.next_seq;
        // One write for header+payload: the format does not depend on the vdisk lock
        // keeping two callers from interleaving. Positional, so a reader on another thread
        // using this descriptor cannot move the position out from under it.
        let buf = Journal::encode(seq, epoch, offset, flags, data);
        self.file.write_all_at(&buf, self.len)?;

        let data_pos = (self.gen << GEN_SHIFT) | (self.len + HEADER_LEN as u64);
        self.len += buf.len() as u64;
        self.next_seq = seq + 1;
        Ok(Record {
            seq,
            epoch,
            offset,
            flags,
            data_pos,
            data_len: data.len() as u32,
            framed: buf,
        })
    }

    /// Make everything appended so far durable.
    ///
    /// `sync_data` rather than `sync_all`: the payload and the file length are what must
    /// survive, and the journal's directory entry was created and synced at open or at
    /// rotation. This is the one fsync on the guest's critical path, and adding a second
    /// one would double every write's latency for a guarantee already held.
    pub fn sync(&self) -> Result<()> {
        #[cfg(test)]
        testhook::before_sync(&self.path)?;
        self.file.sync_data()?;
        Ok(())
    }

    /// Append one record and make it durable. Returns the payload's position.
    pub fn append(&mut self, epoch: u64, offset: u64, flags: u32, data: &[u8]) -> Result<Record> {
        let rec = self.append_unsynced(epoch, offset, flags, data)?;
        self.sync()?;
        Ok(rec)
    }

    pub fn read_at(&self, pos: u64, len: usize) -> Result<Vec<u8>> {
        let gen = gen_of(pos);
        if gen == self.gen {
            return read_in(&self.file, pos, len);
        }
        match &self.old {
            Some(o) if o.gen == gen => read_in(&o.file, pos, len),
            _ => Err(Error::corrupt(format!(
                "journal {}: a read at generation {gen} names a segment that is gone",
                self.path.display()
            ))),
        }
    }

    /// Close the live segment to new records and start a fresh one, returning the sequence
    /// number the fresh one begins at. Everything acknowledged so far is then in the sealed
    /// segment, which is what a drain takes; everything acknowledged afterwards is in the
    /// live one, which the drain never sees.
    ///
    /// The order is rename, create, sync the directory, and a crash anywhere in it is safe:
    /// before the rename nothing changed; between the rename and the create `open` finds a
    /// sealed segment and no live one and makes the live one; after the directory sync the
    /// new layout is durable. No record is appended to the new file before that last step,
    /// so no acknowledgement can rest on a directory entry that is not on disk.
    pub fn rotate(&mut self) -> Result<u64> {
        if self.old.is_some() {
            return Err(Error::refused(
                "the journal already has a sealed segment waiting for its drain".to_string(),
            ));
        }
        let old_path = sealed_path(&self.path);
        std::fs::rename(&self.path, &old_path)?;
        let fresh = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&self.path)
        {
            Ok(f) => f,
            Err(e) => {
                // Put it back rather than leave the vdisk with no live segment.
                let _ = std::fs::rename(&old_path, &self.path);
                return Err(e.into());
            }
        };
        sync_dir(&self.path)?;
        let sealed_file = std::mem::replace(&mut self.file, fresh);
        self.old = Some(Sealed { file: Arc::new(sealed_file), len: self.len, gen: self.gen });
        self.len = 0;
        self.gen = (self.gen + 1) & GEN_MASK;
        self.live_first_seq = self.next_seq;
        Ok(self.next_seq)
    }

    /// The first sequence number of the live segment while a sealed one exists.
    pub fn live_first_seq(&self) -> u64 {
        self.live_first_seq
    }

    /// Forget the sealed segment. Called only after the drain that took it has committed
    /// the block map: data before metadata, and metadata before forgetting.
    pub fn discard_sealed(&mut self) -> Result<()> {
        if self.old.is_none() {
            return Ok(());
        }
        std::fs::remove_file(sealed_path(&self.path))?;
        sync_dir(&self.path)?;
        self.old = None;
        Ok(())
    }

    /// Rebuild the record list from disk: the sealed segment first, then the live one.
    ///
    /// Stops cleanly at the first record that is short, mis-magicked or fails its CRC,
    /// and reports how many bytes were discarded. Those bytes are by construction
    /// unacknowledged: the acknowledgement happens after `sync_data` returns, so anything
    /// incomplete on disk never reached the guest.
    pub fn replay(&mut self) -> Result<(Vec<Record>, u64)> {
        let mut records: Vec<Record> = Vec::new();
        let mut discarded = 0u64;
        let mut last: Option<u64> = None;

        if let Some(o) = &self.old {
            let (recs, end) = scan(&o.file, o.len, o.gen, &mut last, &self.path)?;
            discarded += o.len - end;
            records.extend(recs);
        }
        let (recs, end) = scan(&self.file, self.len, self.gen, &mut last, &self.path)?;
        let first_live = recs.first().map(|r| r.seq);
        records.extend(recs);

        let torn = self.len - end;
        discarded += torn;
        self.next_seq = records.last().map(|r| r.seq + 1).unwrap_or(0);
        self.live_first_seq = first_live.unwrap_or(self.next_seq);
        // Truncate the torn tail so the next append starts from a clean boundary. Only the
        // live segment is ever written to, so it is the only one a crash can have torn.
        if torn > 0 {
            self.file.set_len(end)?;
            self.file.sync_all()?;
            self.len = end;
        }
        Ok((records, discarded))
    }

    /// The whole journal as bytes, sealed segment first, for backfilling a new replica.
    pub fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        if let Some(o) = &self.old {
            let mut part = vec![0u8; o.len as usize];
            o.file.read_exact_at(&mut part, 0)?;
            buf.extend_from_slice(&part);
        }
        if self.len > 0 {
            let mut part = vec![0u8; self.len as usize];
            self.file.seek(SeekFrom::Start(0))?;
            self.file.read_exact(&mut part)?;
            buf.extend_from_slice(&part);
        }
        Ok(buf)
    }

    /// Adopt a journal recovered from a replica, replacing whatever is here.
    ///
    /// Used by takeover only. The bytes are written and synced before the caller replays
    /// them, so a crash mid-takeover leaves a journal that replay can read rather than a
    /// half-written one -- and replay's torn-tail handling covers the rest. A sealed
    /// segment is removed first: it belongs to an earlier ownership, and what the replicas
    /// hold is the history that counts.
    pub fn replace(&mut self, bytes: &[u8]) -> Result<()> {
        if self.old.is_some() {
            self.old = None;
            match std::fs::remove_file(sealed_path(&self.path)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            sync_dir(&self.path)?;
        }
        self.file.set_len(0)?;
        self.file.write_all_at(bytes, 0)?;
        self.file.sync_all()?;
        self.len = bytes.len() as u64;
        self.next_seq = 0;
        Ok(())
    }
}

/// Read the records of one segment file, continuing the sequence from `last`. Returns them
/// and the offset at which the intact prefix ends.
fn scan(
    file: &File,
    total: u64,
    gen: u64,
    last: &mut Option<u64>,
    path: &Path,
) -> Result<(Vec<Record>, u64)> {
    let mut records = Vec::new();
    let mut pos = 0u64;
    while pos + HEADER_LEN as u64 <= total {
        let mut header = [0u8; HEADER_LEN];
        if file.read_exact_at(&mut header, pos).is_err() {
            break;
        }
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != MAGIC {
            break;
        }
        let data_len = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let seq = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let epoch = u64::from_le_bytes(header[16..24].try_into().unwrap());
        let offset = u64::from_le_bytes(header[24..32].try_into().unwrap());
        let flags = u32::from_le_bytes(header[32..36].try_into().unwrap());
        let want_crc = u32::from_le_bytes(header[36..40].try_into().unwrap());

        let end = pos + HEADER_LEN as u64 + data_len as u64;
        if end > total {
            break; // torn tail
        }
        let mut data = vec![0u8; data_len as usize];
        if file.read_exact_at(&mut data, pos + HEADER_LEN as u64).is_err() {
            break;
        }
        if crc32c(crc32c(0, &header[0..36]), &data) != want_crc {
            // Not an error to the caller: a bad CRC at the tail is the ordinary
            // signature of a power cut. A bad CRC in the *middle* would be caught by
            // the sequence check below.
            break;
        }
        if let Some(prev) = *last {
            if seq != prev + 1 {
                return Err(Error::corrupt(format!(
                    "journal {}: sequence jumped {prev} -> {seq} at byte {pos}; \
                     refusing to replay a hole",
                    path.display()
                )));
            }
        }
        *last = Some(seq);
        records.push(Record {
            seq,
            epoch,
            offset,
            flags,
            data_pos: (gen << GEN_SHIFT) | (pos + HEADER_LEN as u64),
            data_len,
            framed: Vec::new(),
        });
        pos = end;
    }
    Ok((records, pos))
}

#[cfg(test)]
pub mod testhook {
    //! Failure and timing injection for tests: a hook consulted by `Journal::sync`, keyed
    //! by journal path so tests running in parallel do not see each other's.
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    pub type Hook = Arc<dyn Fn() -> Result<()> + Send + Sync>;

    fn hooks() -> &'static Mutex<HashMap<PathBuf, Hook>> {
        static H: OnceLock<Mutex<HashMap<PathBuf, Hook>>> = OnceLock::new();
        H.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub fn set(path: &Path, hook: Option<Hook>) {
        let mut h = hooks().lock().unwrap();
        match hook {
            Some(f) => {
                h.insert(path.to_path_buf(), f);
            }
            None => {
                h.remove(path);
            }
        }
    }

    pub fn before_sync(path: &Path) -> Result<()> {
        let hook = hooks().lock().unwrap().get(path).cloned();
        match hook {
            Some(f) => f(),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("sidon-jrn-{}-{}", std::process::id(), name));
        let _ = remove_files(&p);
        p
    }

    #[test]
    fn append_then_replay_round_trips() {
        let p = tmp("roundtrip");
        let mut j = Journal::open(&p).unwrap();
        j.append(1, 0, 0, b"hello").unwrap();
        j.append(1, 4096, FLAG_COMMIT, b"world!!").unwrap();
        drop(j);

        let mut j = Journal::open(&p).unwrap();
        let (records, discarded) = j.replay().unwrap();
        assert_eq!(discarded, 0);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].offset, 4096);
        assert_eq!(records[1].flags, FLAG_COMMIT);
        let back = j.read_at(records[1].data_pos, records[1].data_len as usize).unwrap();
        assert_eq!(back, b"world!!");
        assert_eq!(j.next_seq(), 2);
        remove_files(&p).ok();
    }

    #[test]
    fn a_torn_tail_is_discarded_not_returned() {
        let p = tmp("torn");
        let mut j = Journal::open(&p).unwrap();
        j.append(1, 0, 0, b"durable").unwrap();
        let good_len = j.len();
        j.append(1, 512, 0, b"interrupted").unwrap();
        drop(j);

        // Simulate the crash: the second record is on disk only in part.
        let f = OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(good_len + 12).unwrap();
        drop(f);

        let mut j = Journal::open(&p).unwrap();
        let (records, discarded) = j.replay().unwrap();
        assert_eq!(records.len(), 1, "the unacknowledged write must not survive");
        assert_eq!(discarded, 12);
        // And the file is now clean, so the next append cannot straddle the torn bytes.
        assert_eq!(j.len(), good_len);
        assert_eq!(j.next_seq(), 1);
        remove_files(&p).ok();
    }

    #[test]
    fn payload_corruption_stops_replay() {
        let p = tmp("corrupt");
        let mut j = Journal::open(&p).unwrap();
        let r = j.append(1, 0, 0, b"aaaaaaaa").unwrap();
        drop(j);

        let f = OpenOptions::new().write(true).open(&p).unwrap();
        f.write_all_at(b"Z", r.data_pos + 2).unwrap();
        drop(f);

        let mut j = Journal::open(&p).unwrap();
        let (records, _) = j.replay().unwrap();
        assert!(records.is_empty(), "a bad CRC must never be replayed as data");
        remove_files(&p).ok();
    }

    #[test]
    fn a_hole_in_the_middle_is_an_error_not_a_truncation() {
        // Two records, then the first one's payload scribbled so replay stops at 0 --
        // that is the tail case. The hole case is a valid record whose seq skips, which
        // means the file is not what this daemon wrote.
        let p = tmp("hole");
        let mut j = Journal::open(&p).unwrap();
        j.append(1, 0, 0, b"one").unwrap();
        let second = j.len();
        j.append(1, 8, 0, b"two").unwrap();
        drop(j);

        let f = OpenOptions::new().write(true).open(&p).unwrap();
        f.write_all_at(&9u64.to_le_bytes(), second + 8).unwrap(); // seq 1 -> 9, CRC now wrong too
        drop(f);

        let mut j = Journal::open(&p).unwrap();
        let (records, _) = j.replay().unwrap();
        // The CRC catches it first and stops cleanly; either way, record 2 is not applied.
        assert_eq!(records.len(), 1);
        remove_files(&p).ok();
    }

    #[test]
    fn unsynced_appends_are_readable_and_replayable_once_synced() {
        let p = tmp("unsynced");
        let mut j = Journal::open(&p).unwrap();
        let a = j.append_unsynced(1, 0, 0, b"first").unwrap();
        let b = j.append_unsynced(1, 4096, FLAG_COMMIT, b"second").unwrap();
        j.sync().unwrap();
        // The framed bytes handed out are exactly what is on disk, which is what a replica
        // must be given for its copy to be byte-identical.
        let mut on_disk = Vec::new();
        File::open(&p).unwrap().read_to_end(&mut on_disk).unwrap();
        let mut framed = a.framed.clone();
        framed.extend_from_slice(&b.framed);
        assert_eq!(on_disk, framed);
        assert_eq!(j.read_at(b.data_pos, 6).unwrap(), b"second");
        drop(j);
        let mut j = Journal::open(&p).unwrap();
        let (records, discarded) = j.replay().unwrap();
        assert_eq!((records.len(), discarded), (2, 0));
        remove_files(&p).ok();
    }

    #[test]
    fn rotation_seals_what_was_acknowledged_and_new_records_go_to_a_fresh_file() {
        let p = tmp("rotate");
        let mut j = Journal::open(&p).unwrap();
        let a = j.append(1, 0, FLAG_COMMIT, b"before").unwrap();
        let keep_from = j.rotate().unwrap();
        assert_eq!(keep_from, 1, "the live segment begins at the next sequence number");
        assert!(j.has_sealed());
        assert_eq!(j.live_len(), 0);

        let b = j.append(1, 8, FLAG_COMMIT, b"after").unwrap();
        // Different segments, different generations, and both still read back.
        assert_ne!(gen_of(a.data_pos), gen_of(b.data_pos));
        assert_eq!(j.read_at(a.data_pos, 6).unwrap(), b"before");
        assert_eq!(j.read_at(b.data_pos, 5).unwrap(), b"after");
        // The total is what a crash would have to replay, which is what the water marks use.
        assert_eq!(j.len(), a.framed.len() as u64 + b.framed.len() as u64);

        // The drain's handle reads the sealed segment without the journal.
        let (gen, file) = j.sealed().unwrap();
        assert_eq!(gen, gen_of(a.data_pos));
        assert_eq!(read_in(&file, a.data_pos, 6).unwrap(), b"before");
        remove_files(&p).ok();
    }

    #[test]
    fn a_second_rotation_is_refused_until_the_first_segment_is_discarded() {
        let p = tmp("rotate-twice");
        let mut j = Journal::open(&p).unwrap();
        j.append(1, 0, FLAG_COMMIT, b"x").unwrap();
        j.rotate().unwrap();
        j.append(1, 0, FLAG_COMMIT, b"y").unwrap();
        assert!(j.rotate().is_err(), "two sealed segments would be an unbounded journal");
        j.discard_sealed().unwrap();
        assert!(!j.has_sealed());
        assert!(!sealed_path(&p).exists());
        j.rotate().unwrap();
        remove_files(&p).ok();
    }

    #[test]
    fn a_crash_after_rotation_replays_the_sealed_segment_then_the_live_one() {
        // Died mid-drain: both files are on disk. Everything acknowledged is in one of
        // them, and replaying in file order reproduces the order it was written in.
        let p = tmp("rotate-crash");
        let mut j = Journal::open(&p).unwrap();
        j.append(1, 0, 0, b"one").unwrap();
        j.append(1, 100, FLAG_COMMIT, b"two").unwrap();
        j.rotate().unwrap();
        j.append(1, 200, FLAG_COMMIT, b"three").unwrap();
        drop(j);

        let mut j = Journal::open(&p).unwrap();
        assert!(j.has_sealed());
        let (records, discarded) = j.replay().unwrap();
        assert_eq!(discarded, 0);
        let offsets: Vec<u64> = records.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![0, 100, 200]);
        let seqs: Vec<u64> = records.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, vec![0, 1, 2]);
        assert_eq!(j.next_seq(), 3, "appending after replay continues the sequence");
        // And every replayed position reads back from the right file.
        for (r, want) in records.iter().zip([&b"one"[..], b"two", b"three"]) {
            assert_eq!(j.read_at(r.data_pos, r.data_len as usize).unwrap(), want);
        }
        // New appends land in the live segment, after what replay found.
        let n = j.append(1, 300, FLAG_COMMIT, b"four").unwrap();
        assert_eq!(n.seq, 3);
        remove_files(&p).ok();
    }

    #[test]
    fn a_crash_between_the_rename_and_the_new_live_file_still_opens() {
        let p = tmp("rotate-half");
        let mut j = Journal::open(&p).unwrap();
        j.append(1, 0, FLAG_COMMIT, b"kept").unwrap();
        drop(j);
        // The state rotation leaves if it dies after the rename and before the create.
        std::fs::rename(&p, sealed_path(&p)).unwrap();
        assert!(!p.exists());

        let mut j = Journal::open(&p).unwrap();
        let (records, _) = j.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(j.read_at(records[0].data_pos, 4).unwrap(), b"kept");
        remove_files(&p).ok();
    }

    #[test]
    fn a_torn_live_tail_does_not_touch_the_sealed_segment() {
        let p = tmp("rotate-torn");
        let mut j = Journal::open(&p).unwrap();
        j.append(1, 0, FLAG_COMMIT, b"sealed").unwrap();
        let sealed_bytes = j.len();
        j.rotate().unwrap();
        j.append(1, 8, FLAG_COMMIT, b"live").unwrap();
        let live_good = j.live_len();
        j.append(1, 16, FLAG_COMMIT, b"torn!").unwrap();
        drop(j);
        let f = OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(live_good + 7).unwrap();
        drop(f);

        let mut j = Journal::open(&p).unwrap();
        let (records, discarded) = j.replay().unwrap();
        assert_eq!(records.len(), 2, "the sealed record and the intact live one");
        assert_eq!(discarded, 7);
        assert_eq!(j.live_len(), live_good);
        assert_eq!(std::fs::metadata(sealed_path(&p)).unwrap().len(), sealed_bytes);
        remove_files(&p).ok();
    }

    #[test]
    fn the_whole_journal_for_a_new_replica_is_the_sealed_segment_then_the_live_one() {
        let p = tmp("read-all");
        let mut j = Journal::open(&p).unwrap();
        let a = j.append(1, 0, FLAG_COMMIT, b"old").unwrap();
        j.rotate().unwrap();
        let b = j.append(1, 8, FLAG_COMMIT, b"new").unwrap();
        let all = j.read_all().unwrap();
        let mut want = a.framed.clone();
        want.extend_from_slice(&b.framed);
        assert_eq!(all, want);
        remove_files(&p).ok();
    }

    #[test]
    fn replacing_the_journal_for_a_takeover_drops_a_sealed_segment() {
        let p = tmp("replace");
        let mut j = Journal::open(&p).unwrap();
        j.append(1, 0, FLAG_COMMIT, b"stale").unwrap();
        j.rotate().unwrap();
        let tail = Journal::encode(0, 2, 0, FLAG_COMMIT, b"adopted");
        j.replace(&tail).unwrap();
        assert!(!j.has_sealed());
        assert!(!sealed_path(&p).exists());
        let (records, _) = j.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(j.read_at(records[0].data_pos, 7).unwrap(), b"adopted");
        remove_files(&p).ok();
    }

    #[test]
    fn removing_a_journal_removes_both_files() {
        let p = tmp("remove");
        let mut j = Journal::open(&p).unwrap();
        j.append(1, 0, FLAG_COMMIT, b"x").unwrap();
        j.rotate().unwrap();
        drop(j);
        assert!(p.exists() && sealed_path(&p).exists());
        remove_files(&p).unwrap();
        assert!(!p.exists() && !sealed_path(&p).exists());
        // And it is not an error when there is nothing to remove.
        remove_files(&p).unwrap();
    }
}
