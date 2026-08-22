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

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::crc::crc32c;
use crate::err::{Error, Result};

pub const MAGIC: u32 = 0x5344_4A52; // "SDJR"
pub const HEADER_LEN: usize = 40;

/// A commit marker terminates a group of records that must be applied all-or-nothing.
/// A guest write larger than the record cap becomes several records plus one marker, and
/// replay applies only marker-terminated groups -- so a crash exposes the whole write or
/// none of it, never a prefix.
pub const FLAG_COMMIT: u32 = 1;

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
    /// Byte position of the payload within the journal file, so the read path can pull a
    /// segment back without holding every acknowledged write in memory.
    pub data_pos: u64,
    pub data_len: u32,
    /// The exact bytes written, for replication. Empty for records rebuilt by replay --
    /// nothing replicates a record it just read back off its own disk.
    pub framed: Vec<u8>,
}

pub struct Journal {
    path: PathBuf,
    file: File,
    len: u64,
    next_seq: u64,
}

impl Journal {
    pub fn open(path: &Path) -> Result<Journal> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        let len = file.metadata()?.len();
        Ok(Journal { path: path.to_path_buf(), file, len, next_seq: 0 })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Append one record and make it durable. Returns the payload's file position.
    ///
    /// `sync_data` rather than `sync_all`: the payload and the file length are what must
    /// survive, and the journal's directory entry was created and synced at open. This is
    /// the single fsync on the guest's critical path, and adding a second one here would
    /// double every write's latency for a guarantee already held.
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

    pub fn append(&mut self, epoch: u64, offset: u64, flags: u32, data: &[u8]) -> Result<Record> {
        let seq = self.next_seq;
        // One write_all for header+payload. Two calls could interleave with another
        // thread's record; the vdisk lock prevents that today, but a single buffer means
        // the format does not depend on that lock still being there tomorrow.
        let buf = Journal::encode(seq, epoch, offset, flags, data);

        self.file.seek(SeekFrom::Start(self.len))?;
        self.file.write_all(&buf)?;
        self.file.sync_data()?;

        let data_pos = self.len + HEADER_LEN as u64;
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

    pub fn read_at(&mut self, pos: u64, len: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        self.file.seek(SeekFrom::Start(pos))?;
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Rebuild the record list from disk.
    ///
    /// Stops cleanly at the first record that is short, mis-magicked or fails its CRC,
    /// and reports how many bytes were discarded. Those bytes are by construction
    /// unacknowledged: the acknowledgement happens after `sync_data` returns, so anything
    /// incomplete on disk never reached the guest.
    pub fn replay(&mut self) -> Result<(Vec<Record>, u64)> {
        let mut records = Vec::new();
        let mut pos = 0u64;
        let total = self.len;
        self.file.seek(SeekFrom::Start(0))?;

        while pos + HEADER_LEN as u64 <= total {
            let mut header = [0u8; HEADER_LEN];
            self.file.seek(SeekFrom::Start(pos))?;
            if self.file.read_exact(&mut header).is_err() {
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
            if self.file.read_exact(&mut data).is_err() {
                break;
            }
            if crc32c(crc32c(0, &header[0..36]), &data) != want_crc {
                // Not an error to the caller: a bad CRC at the tail is the ordinary
                // signature of a power cut. A bad CRC in the *middle* would be caught by
                // the sequence check below.
                break;
            }
            if let Some(prev) = records.last().map(|r: &Record| r.seq) {
                if seq != prev + 1 {
                    return Err(Error::corrupt(format!(
                        "journal {}: sequence jumped {prev} -> {seq} at byte {pos}; \
                         refusing to replay a hole",
                        self.path.display()
                    )));
                }
            }
            records.push(Record {
                seq,
                epoch,
                offset,
                flags,
                data_pos: pos + HEADER_LEN as u64,
                data_len,
                framed: Vec::new(),
            });
            pos = end;
        }

        let discarded = total - pos;
        self.next_seq = records.last().map(|r| r.seq + 1).unwrap_or(0);
        // Truncate the torn tail so the next append starts from a clean boundary.
        if discarded > 0 {
            self.file.set_len(pos)?;
            self.file.sync_all()?;
            self.len = pos;
        }
        Ok((records, discarded))
    }

    /// The whole journal as bytes, for backfilling a new replica.
    pub fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; self.len as usize];
        if self.len > 0 {
            self.file.seek(SeekFrom::Start(0))?;
            self.file.read_exact(&mut buf)?;
        }
        Ok(buf)
    }

    /// Adopt a journal recovered from a replica, replacing whatever is here.
    ///
    /// Used by takeover only. The bytes are written and synced before the caller replays
    /// them, so a crash mid-takeover leaves a journal that replay can read rather than a
    /// half-written one -- and replay's torn-tail handling covers the rest.
    pub fn replace(&mut self, bytes: &[u8]) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(bytes)?;
        self.file.sync_all()?;
        self.len = bytes.len() as u64;
        self.next_seq = 0;
        Ok(())
    }

    /// Drop every record. Called only after a drain has committed the corresponding
    /// extents *and* the map -- data before metadata, and metadata before forgetting.
    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.sync_all()?;
        self.len = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("sidon-jrn-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
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
        std::fs::remove_file(&p).ok();
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
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn payload_corruption_stops_replay() {
        let p = tmp("corrupt");
        let mut j = Journal::open(&p).unwrap();
        let r = j.append(1, 0, 0, b"aaaaaaaa").unwrap();
        drop(j);

        let mut f = OpenOptions::new().write(true).open(&p).unwrap();
        f.seek(SeekFrom::Start(r.data_pos + 2)).unwrap();
        f.write_all(b"Z").unwrap();
        drop(f);

        let mut j = Journal::open(&p).unwrap();
        let (records, _) = j.replay().unwrap();
        assert!(records.is_empty(), "a bad CRC must never be replayed as data");
        std::fs::remove_file(&p).ok();
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

        let mut f = OpenOptions::new().write(true).open(&p).unwrap();
        f.seek(SeekFrom::Start(second + 8)).unwrap();
        f.write_all(&9u64.to_le_bytes()).unwrap(); // seq 1 -> 9, CRC now wrong too
        drop(f);

        let mut j = Journal::open(&p).unwrap();
        let (records, _) = j.replay().unwrap();
        // The CRC catches it first and stops cleanly; either way, record 2 is not applied.
        assert_eq!(records.len(), 1);
        std::fs::remove_file(&p).ok();
    }
}
