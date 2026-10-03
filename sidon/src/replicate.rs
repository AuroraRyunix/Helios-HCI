//! The data plane of replicating a snapshot to another site: what a manifest is, how a group
//! travels, and how it is checked on arrival.
//!
//! **Status: built and tested only against two directories on one machine standing in for two
//! sites.** There is no transport, no TLS verifier and no listener here, and nothing in the
//! daemon calls this module. The design is `docs/dfs/replication.md`, D-28 to D-31.
//!
//! A snapshot is a map plus immutable extent groups, so replicating it is sending the groups
//! the target lacks and then the map. The pieces:
//!
//! * [`Manifest`] -- the map rows and the groups they reference, with a canonical digest of
//!   the map that Python (`rauru_replication.py`) computes identically.
//! * [`Frame`] -- the stream a group travels in: begin, chunks each with its own crc32c, end
//!   carrying a SHA-256 of the bytes sent.
//! * [`verify_group`] -- the checks both ends make on a group's bytes. The importer makes them
//!   again on its own: it does not trust the sender.
//! * `export`, `import`, `throttle` -- the exporter, the importer (staging, verification,
//!   atomic publish through a [`import::MapSink`]) and the token bucket.
//!
//! Nothing here uses wall-clock time for anything but throttling, which takes an injected
//! clock, so two sites that disagree about the time cannot disagree about a snapshot.

pub mod export;
pub mod import;
pub mod throttle;

#[cfg(test)]
mod sim;

use std::io::{Read, Write};

use ring::digest;
use serde_json::{json, Value};

use crate::crc::crc32c;
use crate::err::{Error, Result};
use crate::extent::{verify_footer, FOOTER_LEN};

pub const FORMAT: u32 = 1;
/// Payload bytes per chunk, and the boundary a resumed transfer rounds down to.
pub const CHUNK: usize = 1 << 20;
/// No group this large is shipped: it is read whole into memory to be verified first.
pub const MAX_GROUP: u64 = 256 << 20;
const MAGIC: u32 = 0x5344_5250; // "SDRP"
const HEADER: usize = 24;
const MAX_FRAME_DATA: u32 = 8 << 20;

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let d = digest::digest(&digest::SHA256, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `crc32c:xxxxxxxx` over the bytes, the same spelling `EgroupStore::seal_hash` records.
pub fn seal_of(bytes: &[u8]) -> String {
    format!("crc32c:{:08x}", crc32c(0, bytes))
}

/// One `dfs_block_map` row, with the group named by its **target** id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapRow {
    pub extent_index: u64,
    pub group: String,
    pub offset: u32,
    pub length: u32,
    pub vdisk_hash: u64,
}

/// A group to be present on the target. `seal` is `crc32c:..` over `[0, length)`, or empty when
/// the builder could not know it (a prefix of a group still open at the source); then the group
/// is verified by its SHA-256 and by every referenced footer alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupRef {
    pub id: String,
    pub length: u64,
    pub seal: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub snapshot: String,
    pub size_bytes: u64,
    pub extent_bytes: u64,
    pub rows: Vec<MapRow>,
    pub groups: Vec<GroupRef>,
}

/// Names that end up in file paths. Nothing outside this set reaches the filesystem, because
/// the strings arrive from another site.
pub fn safe_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 200
        && !s.starts_with('.')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '~'))
}

impl Manifest {
    /// The text the map digest is taken over. The vdisk hash is signed because that is how
    /// Hydra stores it, and no timestamp or name is in it, so the same content has the same
    /// digest wherever and whenever it is computed.
    pub fn canonical(&self) -> String {
        let mut s = format!("helios-map-v{FORMAT}\nsize {}\nextent {}\n", self.size_bytes, self.extent_bytes);
        for r in &self.rows {
            s.push_str(&format!(
                "{} {} {} {} {}\n",
                r.extent_index, r.group, r.offset, r.length, r.vdisk_hash as i64
            ));
        }
        s
    }

    pub fn map_digest(&self) -> String {
        hex(&sha256(self.canonical().as_bytes()))
    }

    /// Refuse a manifest that could not describe a snapshot, or that names something unsafe.
    pub fn validate(&self) -> Result<()> {
        if !safe_name(&self.snapshot) {
            return Err(Error::refused(format!("snapshot name {:?} is not acceptable", self.snapshot)));
        }
        if self.extent_bytes == 0 {
            return Err(Error::refused("extent size is zero".to_string()));
        }
        let mut seen = std::collections::HashMap::new();
        for g in &self.groups {
            if !safe_name(&g.id) {
                return Err(Error::refused(format!("group id {:?} is not acceptable", g.id)));
            }
            if g.length == 0 || g.length > MAX_GROUP {
                return Err(Error::refused(format!("group {} has an unacceptable length {}", g.id, g.length)));
            }
            if seen.insert(g.id.as_str(), g.length).is_some() {
                return Err(Error::refused(format!("group {} is listed twice", g.id)));
            }
        }
        let mut previous: Option<u64> = None;
        for r in &self.rows {
            if previous.map_or(false, |p| r.extent_index <= p) {
                return Err(Error::refused(format!(
                    "rows are not strictly ordered by extent index at {}", r.extent_index)));
            }
            previous = Some(r.extent_index);
            let glen = *seen.get(r.group.as_str()).ok_or_else(|| {
                Error::refused(format!("row {} names group {}, which the manifest does not list",
                                       r.extent_index, r.group))
            })?;
            if r.offset as u64 + r.length as u64 + FOOTER_LEN as u64 > glen {
                return Err(Error::refused(format!(
                    "row {} reaches past the end of group {}", r.extent_index, r.group)));
            }
        }
        Ok(())
    }

    pub fn rows_for<'a>(&'a self, group: &str) -> Vec<&'a MapRow> {
        self.rows.iter().filter(|r| r.group == group).collect()
    }

    pub fn to_json(&self) -> Value {
        json!({
            "format": FORMAT,
            "snapshot": self.snapshot,
            "size_bytes": self.size_bytes,
            "extent_bytes": self.extent_bytes,
            "rows": self.rows.iter().map(|r| json!(
                [r.extent_index, r.group, r.offset, r.length, r.vdisk_hash as i64])).collect::<Vec<_>>(),
            "groups": self.groups.iter().map(|g| json!([g.id, g.length, g.seal])).collect::<Vec<_>>(),
            "map_sha256": self.map_digest(),
        })
    }

    /// Parse and validate. A manifest whose stated digest does not match its own rows is
    /// refused: it was damaged or edited in transit.
    pub fn from_json(v: &Value) -> Result<Manifest> {
        let bad = |what: &str| Error::refused(format!("manifest is malformed: {what}"));
        if v.get("format").and_then(Value::as_u64) != Some(FORMAT as u64) {
            return Err(bad("unknown format"));
        }
        let mut rows = Vec::new();
        for r in v.get("rows").and_then(Value::as_array).ok_or_else(|| bad("rows"))? {
            let a = r.as_array().filter(|a| a.len() == 5).ok_or_else(|| bad("a row"))?;
            rows.push(MapRow {
                extent_index: a[0].as_u64().ok_or_else(|| bad("extent index"))?,
                group: a[1].as_str().ok_or_else(|| bad("row group"))?.to_string(),
                offset: a[2].as_u64().ok_or_else(|| bad("offset"))? as u32,
                length: a[3].as_u64().ok_or_else(|| bad("length"))? as u32,
                vdisk_hash: a[4].as_i64().ok_or_else(|| bad("vdisk hash"))? as u64,
            });
        }
        let mut groups = Vec::new();
        for g in v.get("groups").and_then(Value::as_array).ok_or_else(|| bad("groups"))? {
            let a = g.as_array().filter(|a| a.len() == 3).ok_or_else(|| bad("a group"))?;
            groups.push(GroupRef {
                id: a[0].as_str().ok_or_else(|| bad("group id"))?.to_string(),
                length: a[1].as_u64().ok_or_else(|| bad("group length"))?,
                seal: a[2].as_str().ok_or_else(|| bad("group seal"))?.to_string(),
            });
        }
        let m = Manifest {
            snapshot: v.get("snapshot").and_then(Value::as_str).ok_or_else(|| bad("snapshot"))?.to_string(),
            size_bytes: v.get("size_bytes").and_then(Value::as_u64).ok_or_else(|| bad("size"))?,
            extent_bytes: v.get("extent_bytes").and_then(Value::as_u64).ok_or_else(|| bad("extent size"))?,
            rows,
            groups,
        };
        m.validate()?;
        if v.get("map_sha256").and_then(Value::as_str) != Some(m.map_digest().as_str()) {
            return Err(Error::corrupt("manifest map digest does not match its rows".to_string()));
        }
        Ok(m)
    }
}

/// What both ends check on a group's bytes: its length, its seal when there is one, and every
/// extent a row points into it, by the same footer check every read makes.
pub fn verify_group(g: &GroupRef, bytes: &[u8], rows: &[&MapRow]) -> Result<()> {
    if bytes.len() as u64 != g.length {
        return Err(Error::corrupt(format!(
            "group {} is {} bytes, the manifest says {}", g.id, bytes.len(), g.length)));
    }
    if !g.seal.is_empty() {
        let got = seal_of(bytes);
        if got != g.seal {
            return Err(Error::corrupt(format!(
                "group {} hashes {got}, expected {}", g.id, g.seal)));
        }
    }
    for r in rows {
        let start = r.offset as usize;
        let end = start + r.length as usize + FOOTER_LEN;
        if end > bytes.len() {
            return Err(Error::corrupt(format!(
                "group {} is shorter than the map claims for extent {}", g.id, r.extent_index)));
        }
        let (stored, footer) = bytes[start..end].split_at(r.length as usize);
        verify_footer(stored, footer, r.vdisk_hash, r.extent_index)?;
    }
    Ok(())
}

// -- frames ---------------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    /// A group follows, `length` bytes in all, starting at `start` (a resume).
    Begin { id: String, length: u64, start: u64 },
    Chunk { id: String, offset: u64, data: Vec<u8> },
    /// The SHA-256 of all `length` bytes of the group, not only those sent.
    End { id: String, sha256: [u8; 32] },
    Abort { id: String, reason: String },
}

const K_BEGIN: u8 = 1;
const K_CHUNK: u8 = 2;
const K_END: u8 = 3;
const K_ABORT: u8 = 4;

/// ```text
/// magic u32 | kind u8 | pad u8 | id_len u16 | offset u64 | data_len u32 | crc u32 | id | data
/// ```
/// The crc covers the header without itself, the id and the data. A frame that fails it is a
/// desynchronised stream, not a bad request, and is an error rather than something to skip.
pub fn write_frame(w: &mut dyn Write, f: &Frame) -> Result<()> {
    let (kind, id, offset, data): (u8, &str, u64, Vec<u8>) = match f {
        Frame::Begin { id, length, start } => (K_BEGIN, id, *start, length.to_le_bytes().to_vec()),
        Frame::Chunk { id, offset, data } => (K_CHUNK, id, *offset, data.clone()),
        Frame::End { id, sha256 } => (K_END, id, 0, sha256.to_vec()),
        Frame::Abort { id, reason } => (K_ABORT, id, 0, reason.as_bytes().to_vec()),
    };
    let mut h = [0u8; HEADER];
    h[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    h[4] = kind;
    h[6..8].copy_from_slice(&(id.len() as u16).to_le_bytes());
    h[8..16].copy_from_slice(&offset.to_le_bytes());
    h[16..20].copy_from_slice(&(data.len() as u32).to_le_bytes());
    let crc = crc32c(crc32c(crc32c(0, &h[..20]), id.as_bytes()), &data);
    h[20..24].copy_from_slice(&crc.to_le_bytes());
    w.write_all(&h)?;
    w.write_all(id.as_bytes())?;
    w.write_all(&data)?;
    Ok(())
}

/// The next frame, or `None` at a clean end of stream between frames. An end of stream inside a
/// frame is an error: that is a link that dropped.
pub fn read_frame(r: &mut dyn Read) -> Result<Option<Frame>> {
    let mut h = [0u8; HEADER];
    let mut got = 0;
    while got < HEADER {
        let n = r.read(&mut h[got..])?;
        if n == 0 {
            if got == 0 {
                return Ok(None);
            }
            return Err(Error::io("stream ended inside a frame header".to_string()));
        }
        got += n;
    }
    if u32::from_le_bytes(h[0..4].try_into().unwrap()) != MAGIC {
        return Err(Error::corrupt("frame magic is wrong: the stream is desynchronised".to_string()));
    }
    let kind = h[4];
    let id_len = u16::from_le_bytes(h[6..8].try_into().unwrap()) as usize;
    let offset = u64::from_le_bytes(h[8..16].try_into().unwrap());
    let data_len = u32::from_le_bytes(h[16..20].try_into().unwrap());
    let want = u32::from_le_bytes(h[20..24].try_into().unwrap());
    if data_len > MAX_FRAME_DATA || id_len > 255 {
        return Err(Error::corrupt("frame claims an impossible size".to_string()));
    }
    let mut id = vec![0u8; id_len];
    let mut data = vec![0u8; data_len as usize];
    r.read_exact(&mut id).map_err(|e| Error::io(format!("stream ended inside a frame: {e}")))?;
    r.read_exact(&mut data).map_err(|e| Error::io(format!("stream ended inside a frame: {e}")))?;
    if crc32c(crc32c(crc32c(0, &h[..20]), &id), &data) != want {
        return Err(Error::corrupt("frame checksum mismatch: the stream is desynchronised".to_string()));
    }
    let id = String::from_utf8(id).map_err(|_| Error::corrupt("frame id is not text".to_string()))?;
    let frame = match kind {
        K_BEGIN => {
            let length = data.get(..8).map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                .ok_or_else(|| Error::corrupt("begin frame without a length".to_string()))?;
            Frame::Begin { id, length, start: offset }
        }
        K_CHUNK => Frame::Chunk { id, offset, data },
        K_END => {
            let sha: [u8; 32] = data.as_slice().try_into()
                .map_err(|_| Error::corrupt("end frame without a digest".to_string()))?;
            Frame::End { id, sha256: sha }
        }
        K_ABORT => Frame::Abort { id, reason: String::from_utf8_lossy(&data).to_string() },
        other => return Err(Error::corrupt(format!("unknown frame kind {other}"))),
    };
    Ok(Some(frame))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Manifest {
        Manifest {
            snapshot: "web-disk0-dom-202610031200".to_string(),
            size_bytes: 1 << 30,
            extent_bytes: 1 << 20,
            rows: vec![
                MapRow { extent_index: 0, group: "eg-a".into(), offset: 0, length: 100, vdisk_hash: 7 },
                MapRow { extent_index: 1, group: "eg-a".into(), offset: 132, length: 100, vdisk_hash: 7 },
                MapRow { extent_index: 5, group: "eg-b~264".into(), offset: 0, length: 232, vdisk_hash: 1u64 << 63 },
            ],
            groups: vec![
                GroupRef { id: "eg-a".into(), length: 264, seal: "crc32c:deadbeef".into() },
                GroupRef { id: "eg-b~264".into(), length: 264, seal: String::new() },
            ],
        }
    }

    /// The same fixture and the same digest as `test_rauru_replication.py`. If either side
    /// changes how the map is canonicalised, one of the two fails, which is the point: a
    /// digest two languages disagree about turns every replication into a refusal.
    #[test]
    fn the_map_digest_is_the_one_python_computes() {
        assert_eq!(
            sample().map_digest(),
            "fbc5d009589663ef0dfe582e0059a5c785b4982b5f1f59ebceaaa3bfab887246"
        );
    }

    #[test]
    fn the_digest_names_no_time_and_no_snapshot_so_two_clocks_cannot_disagree_about_it() {
        let mut other = sample();
        other.snapshot = "renamed".to_string();
        assert_eq!(sample().map_digest(), other.map_digest());
    }

    #[test]
    fn a_manifest_survives_json_and_a_tampered_one_is_refused() {
        let m = sample();
        assert_eq!(Manifest::from_json(&m.to_json()).unwrap(), m);
        let mut v = m.to_json();
        v["rows"][1][3] = json!(101);
        assert!(matches!(Manifest::from_json(&v), Err(Error::Corrupt(_)) | Err(Error::Refused(_))));
    }

    #[test]
    fn names_that_could_leave_the_staging_directory_are_refused() {
        for bad in ["../x", "a/b", ".hidden", "", "a b", "x\0y"] {
            assert!(!safe_name(bad), "{bad:?}");
            let mut m = sample();
            m.groups[0].id = bad.to_string();
            assert!(m.validate().is_err(), "{bad:?}");
        }
        assert!(safe_name("eg-web-disk0-1a2b~264"));
    }

    #[test]
    fn rows_that_are_unordered_dangling_or_past_their_group_are_refused() {
        let mut m = sample();
        m.rows.swap(0, 1);
        assert!(m.validate().is_err());
        let mut m = sample();
        m.rows[2].group = "nowhere".into();
        assert!(m.validate().is_err());
        let mut m = sample();
        m.rows[1].offset = 200;
        assert!(m.validate().is_err());
    }

    #[test]
    fn a_frame_round_trips_and_a_flipped_byte_is_a_desynchronised_stream() {
        let frames = vec![
            Frame::Begin { id: "g".into(), length: 9, start: 4 },
            Frame::Chunk { id: "g".into(), offset: 4, data: vec![1, 2, 3, 4, 5] },
            Frame::End { id: "g".into(), sha256: [9u8; 32] },
            Frame::Abort { id: "g".into(), reason: "no".into() },
        ];
        let mut buf = Vec::new();
        for f in &frames {
            write_frame(&mut buf, f).unwrap();
        }
        let mut r = &buf[..];
        for f in &frames {
            assert_eq!(&read_frame(&mut r).unwrap().unwrap(), f);
        }
        assert!(read_frame(&mut r).unwrap().is_none());
        let mut bad = buf.clone();
        bad[HEADER + 3] ^= 0x40;
        assert!(matches!(read_frame(&mut &bad[..]), Err(Error::Corrupt(_))));
    }

    #[test]
    fn a_stream_cut_inside_a_frame_is_an_error_not_a_clean_end() {
        let mut buf = Vec::new();
        write_frame(&mut buf, &Frame::Chunk { id: "g".into(), offset: 0, data: vec![0; 100] }).unwrap();
        for cut in [3, HEADER, HEADER + 50] {
            assert!(read_frame(&mut &buf[..cut]).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn sha256_matches_the_published_vector() {
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
