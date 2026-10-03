//! The receiving half: preflight, stage, verify, install, and publish the map atomically.
//!
//! The rules, each of which exists because the alternative loses or fabricates data:
//!
//! * **Nothing from the wire is believed.** Names are checked before they touch a path, the
//!   manifest is validated, and every group is verified on arrival by the importer's own
//!   [`verify_group`] and a SHA-256 it computes itself.
//! * **A group is installed only whole and verified.** Bytes land in a `.part` file in staging;
//!   a mismatch deletes it. Resume is an optimisation: what is believed is the digest at the end.
//! * **A snapshot is visible only after everything else is true.** Publish writes the map under
//!   a `forming` state nothing lists, reads it back, checks its digest, and only then flips it
//!   to visible in one step ([`MapSink::make_visible`]).
//! * **The same snapshot twice is decided by its digest**: equal is a no-op, different is a
//!   refusal. A name is never overwritten.
//! * **Replication never frees target space to make room for itself.**

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::err::{Error, Result};

use super::{hex, read_frame, safe_name, sha256, verify_group, Frame, GroupRef, Manifest, MapRow, CHUNK};

/// Rows handed to the sink at a time, so a terabyte's map is not one statement.
pub const ROW_PAGE: usize = 10_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstalledMeta {
    pub length: u64,
    pub sha256: String,
    pub seal: String,
}

/// Where verified groups end up. The production implementation would place them on a disk of
/// the node's extent store and register them in Hydra; this module needs only these three.
pub trait GroupStore {
    /// Present only if the group is installed *and* its metadata agrees with its file.
    fn installed(&self, id: &str) -> Option<InstalledMeta>;
    fn free_bytes(&self) -> u64;
    /// Move a verified staged file into place. Atomic: the group is absent or whole.
    fn install(&mut self, id: &str, part: &Path, meta: &InstalledMeta) -> Result<()>;
}

/// Where the map goes. The production implementation writes `dfs_vdisks` and `dfs_block_map`
/// through Daruk; the order of calls below is what makes a half-published snapshot invisible.
pub trait MapSink {
    /// The map digest of a snapshot that is visible, if it is.
    fn visible(&self, snapshot: &str) -> Option<String>;
    /// Create (or reset) the snapshot in the state nothing lists. Idempotent.
    fn begin(&mut self, m: &Manifest) -> Result<()>;
    fn write_rows(&mut self, snapshot: &str, rows: &[MapRow]) -> Result<()>;
    fn read_back(&self, snapshot: &str) -> Result<Vec<MapRow>>;
    /// The single step after which the snapshot exists. Conditional on `begin`'s state.
    fn make_visible(&mut self, snapshot: &str) -> Result<()>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Have {
    Complete,
    Absent,
    /// Bytes already staged, rounded down to a chunk boundary: where a resume starts.
    Partial(u64),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Offer {
    AlreadyPresent,
    Proceed { have: Vec<(String, Have)>, need_bytes: u64 },
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReceiveReport {
    pub installed: Vec<String>,
    pub failed: Vec<(String, String)>,
    pub bytes: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Published {
    AlreadyVisible,
    Published,
}

type SpaceGuard = Box<dyn FnMut(u64) -> Result<()>>;

pub struct Importer<S: GroupStore> {
    pub store: S,
    staging: PathBuf,
    /// Called with the bytes about to be written. A hook for the one failure a test cannot
    /// otherwise cause on a healthy disk: the filesystem filling up mid-transfer.
    guard: Option<SpaceGuard>,
}

impl<S: GroupStore> Importer<S> {
    pub fn new(store: S, staging: &Path) -> Result<Importer<S>> {
        std::fs::create_dir_all(staging)?;
        Ok(Importer { store, staging: staging.to_path_buf(), guard: None })
    }

    pub fn with_space_guard(mut self, guard: SpaceGuard) -> Importer<S> {
        self.guard = Some(guard);
        self
    }

    fn job_dir(&self, job: &str) -> Result<PathBuf> {
        if !safe_name(job) {
            return Err(Error::refused(format!("job id {job:?} is not acceptable")));
        }
        Ok(self.staging.join(job))
    }

    fn part_path(&self, job: &str, id: &str) -> Result<PathBuf> {
        Ok(self.job_dir(job)?.join(format!("{id}.part")))
    }

    fn have_one(&self, job: &str, g: &GroupRef) -> Result<Have> {
        if let Some(meta) = self.store.installed(&g.id) {
            if meta.length == g.length && (g.seal.is_empty() || meta.seal == g.seal) {
                return Ok(Have::Complete);
            }
            // Same name, different content. A group id is unique to what was written into
            // it, so this is a collision or damage, and overwriting either would hide it.
            return Err(Error::refused(format!(
                "conflict: group {} is already here with different content", g.id)));
        }
        match std::fs::metadata(self.part_path(job, &g.id)?) {
            Ok(md) if md.len() > 0 && md.len() <= g.length => {
                let rounded = md.len() / CHUNK as u64 * CHUNK as u64;
                Ok(if rounded == 0 { Have::Absent } else { Have::Partial(rounded) })
            }
            _ => Ok(Have::Absent),
        }
    }

    /// Decide what to do with an offered snapshot. Errors are the refusals: a conflicting
    /// snapshot or group, a manifest that is not one, or not enough space.
    pub fn offer(&self, job: &str, m: &Manifest, sink: &dyn MapSink) -> Result<Offer> {
        m.validate()?;
        let digest = m.map_digest();
        match sink.visible(&m.snapshot) {
            Some(d) if d == digest => return Ok(Offer::AlreadyPresent),
            Some(_) => {
                return Err(Error::refused(format!(
                    "conflict: snapshot {} is already here with a different map", m.snapshot)))
            }
            None => {}
        }
        let mut have = Vec::new();
        let mut need = 0u64;
        for g in &m.groups {
            let h = self.have_one(job, g)?;
            match h {
                Have::Complete => {}
                Have::Absent => need += g.length,
                Have::Partial(staged) => need += g.length - staged,
            }
            have.push((g.id.clone(), h));
        }
        let free = self.store.free_bytes();
        // A fifth of a percent would be too little for a store that must keep serving guests,
        // and a fifth of the disk too much: five percent, as the rest of the daemon reserves.
        let usable = free - free / 20;
        if need > usable {
            return Err(Error::refused(format!(
                "no space: {need} bytes are needed and {usable} are usable of {free} free")));
        }
        Ok(Offer::Proceed { have, need_bytes: need })
    }

    /// Consume a stream of frames. A stream that ends inside a group is an error and leaves
    /// the staged bytes for a resume; a group that fails verification is reported in `failed`
    /// and its staged bytes are deleted, so the next attempt starts that group from zero.
    pub fn receive(&mut self, job: &str, m: &Manifest, r: &mut dyn Read) -> Result<ReceiveReport> {
        m.validate()?;
        std::fs::create_dir_all(self.job_dir(job)?)?;
        let mut report = ReceiveReport::default();
        struct Cur {
            id: String,
            group: GroupRef,
            file: Option<File>,
            next: u64,
            part: PathBuf,
        }
        let mut cur: Option<Cur> = None;
        loop {
            let frame = match read_frame(r)? {
                Some(f) => f,
                None => {
                    if let Some(c) = &cur {
                        return Err(Error::io(format!(
                            "the stream ended before group {} was complete", c.id)));
                    }
                    return Ok(report);
                }
            };
            match frame {
                Frame::Begin { id, length, start } => {
                    if let Some(c) = &cur {
                        return Err(Error::corrupt(format!(
                            "group {} began before {} ended", id, c.id)));
                    }
                    let group = m.groups.iter().find(|g| g.id == id).cloned().ok_or_else(|| {
                        Error::refused(format!("group {id} is not in the manifest"))
                    })?;
                    if length != group.length {
                        return Err(Error::refused(format!(
                            "group {id} is announced as {length} bytes, the manifest says {}",
                            group.length)));
                    }
                    let part = self.part_path(job, &id)?;
                    let file = if self.store.installed(&id).is_some() {
                        None // already here: its frames are read and ignored
                    } else if start == 0 {
                        Some(OpenOptions::new().read(true).write(true).create(true)
                            .truncate(true).open(&part)?)
                    } else {
                        let f = OpenOptions::new().read(true).write(true).open(&part).map_err(|_| {
                            Error::refused(format!("cannot resume {id} at {start}: nothing is staged"))
                        })?;
                        if f.metadata()?.len() < start {
                            return Err(Error::refused(format!(
                                "cannot resume {id} at {start}: only {} bytes are staged",
                                f.metadata()?.len())));
                        }
                        // Bytes past the resume point are untrusted; the sender is about to
                        // replace them.
                        f.set_len(start)?;
                        Some(f)
                    };
                    cur = Some(Cur { id, group, file, next: start, part });
                }
                Frame::Chunk { id, offset, data } => {
                    let c = cur.as_mut().filter(|c| c.id == id).ok_or_else(|| {
                        Error::corrupt(format!("a chunk of {id} arrived outside its group"))
                    })?;
                    if offset != c.next || offset + data.len() as u64 > c.group.length {
                        return Err(Error::corrupt(format!(
                            "chunk of {id} at {offset} is out of order or past the end")));
                    }
                    if let Some(file) = c.file.as_mut() {
                        if let Some(guard) = self.guard.as_mut() {
                            guard(data.len() as u64)?;
                        }
                        file.seek(SeekFrom::Start(offset))?;
                        file.write_all(&data)?;
                        report.bytes += data.len() as u64;
                    }
                    c.next += data.len() as u64;
                }
                Frame::End { id, sha256: stated } => {
                    let c = cur.take().filter(|c| c.id == id).ok_or_else(|| {
                        Error::corrupt(format!("the end of {id} arrived outside its group"))
                    })?;
                    let Some(file) = c.file else { continue };
                    let outcome = (|| -> Result<InstalledMeta> {
                        if c.next != c.group.length {
                            return Err(Error::corrupt(format!(
                                "{} ended at {} of {} bytes", c.id, c.next, c.group.length)));
                        }
                        file.sync_data()?;
                        let bytes = std::fs::read(&c.part)?;
                        let got = sha256(&bytes);
                        if got != stated {
                            return Err(Error::corrupt(format!(
                                "{} hashes {}, the sender says {}", c.id, hex(&got), hex(&stated))));
                        }
                        // The importer's own checks, whatever the sender claims to have done.
                        verify_group(&c.group, &bytes, &m.rows_for(&c.id))?;
                        Ok(InstalledMeta {
                            length: c.group.length,
                            sha256: hex(&got),
                            seal: super::seal_of(&bytes),
                        })
                    })();
                    match outcome {
                        Ok(meta) => {
                            self.store.install(&c.id, &c.part, &meta)?;
                            report.installed.push(c.id);
                        }
                        Err(e) => {
                            let _ = std::fs::remove_file(&c.part);
                            report.failed.push((c.id, e.to_string()));
                        }
                    }
                }
                Frame::Abort { id, reason } => {
                    cur = None;
                    report.failed.push((id, format!("the sender aborted: {reason}")));
                }
            }
        }
    }

    /// Write the map and make the snapshot visible, or leave it invisible. Every group must
    /// already be installed; the rows are read back and their digest recomputed before the
    /// last step, so a write Hydra lost is found here and not by a guest.
    pub fn publish(&mut self, m: &Manifest, sink: &mut dyn MapSink) -> Result<Published> {
        m.validate()?;
        let digest = m.map_digest();
        match sink.visible(&m.snapshot) {
            Some(d) if d == digest => return Ok(Published::AlreadyVisible),
            Some(_) => {
                return Err(Error::refused(format!(
                    "conflict: snapshot {} is already here with a different map", m.snapshot)))
            }
            None => {}
        }
        for g in &m.groups {
            match self.store.installed(&g.id) {
                Some(meta) if meta.length == g.length => {}
                _ => {
                    return Err(Error::refused(format!(
                        "cannot publish {}: group {} is not installed", m.snapshot, g.id)))
                }
            }
        }
        sink.begin(m)?;
        for page in m.rows.chunks(ROW_PAGE) {
            sink.write_rows(&m.snapshot, page)?;
        }
        let written = Manifest { rows: sink.read_back(&m.snapshot)?, ..m.clone() };
        if written.map_digest() != digest {
            return Err(Error::corrupt(format!(
                "the map of {} read back differently from what was written; it was not published",
                m.snapshot)));
        }
        sink.make_visible(&m.snapshot)?;
        Ok(Published::Published)
    }

    /// Delete staging directories untouched for `older_than`: the leftovers of jobs that
    /// never finished and will not. Returns how many were removed.
    pub fn reap_staging(&self, older_than: Duration) -> Result<usize> {
        let mut removed = 0;
        for entry in std::fs::read_dir(&self.staging)? {
            let entry = entry?;
            let age = entry.metadata()?.modified().ok()
                .and_then(|t| t.elapsed().ok()).unwrap_or(Duration::ZERO);
            if age >= older_than && std::fs::remove_dir_all(entry.path()).is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

/// A directory of installed groups, each `<id>.eg` with an `<id>.eg.meta` beside it. The
/// metadata is written after the data and is what makes a group count as installed, so a
/// crash between the two leaves a file that is simply re-received.
pub struct DirStore {
    root: PathBuf,
    capacity: Option<u64>,
}

impl DirStore {
    pub fn new(root: &Path, capacity: Option<u64>) -> Result<DirStore> {
        std::fs::create_dir_all(root)?;
        Ok(DirStore { root: root.to_path_buf(), capacity })
    }

    pub fn path_of(&self, id: &str) -> PathBuf {
        self.root.join(format!("{id}.eg"))
    }

    fn used(&self) -> u64 {
        std::fs::read_dir(&self.root).map(|rd| {
            rd.flatten().filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum()
        }).unwrap_or(0)
    }
}

impl GroupStore for DirStore {
    fn installed(&self, id: &str) -> Option<InstalledMeta> {
        let meta = std::fs::read_to_string(self.root.join(format!("{id}.eg.meta"))).ok()?;
        let mut lines = meta.lines();
        let length: u64 = lines.next()?.parse().ok()?;
        let sha = lines.next()?.to_string();
        let seal = lines.next()?.to_string();
        if std::fs::metadata(self.path_of(id)).ok()?.len() != length {
            return None;
        }
        Some(InstalledMeta { length, sha256: sha, seal })
    }

    fn free_bytes(&self) -> u64 {
        match self.capacity {
            Some(c) => c.saturating_sub(self.used()),
            None => u64::MAX / 4,
        }
    }

    fn install(&mut self, id: &str, part: &Path, meta: &InstalledMeta) -> Result<()> {
        std::fs::rename(part, self.path_of(id))?;
        let tmp = self.root.join(format!("{id}.eg.meta.tmp"));
        std::fs::write(&tmp, format!("{}\n{}\n{}\n", meta.length, meta.sha256, meta.seal))?;
        std::fs::rename(&tmp, self.root.join(format!("{id}.eg.meta")))?;
        if let Ok(dir) = File::open(&self.root) {
            let _ = dir.sync_all();
        }
        Ok(())
    }
}
