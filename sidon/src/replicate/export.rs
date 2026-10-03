//! The sending half: read a group whole, verify it, then stream it.
//!
//! A group is verified **before the first byte of it leaves**. Corruption at the source is
//! found here, where it can be reported against the source's own disk, instead of being sent
//! and refused at the far end of a slow link. Verification is the same check the target makes
//! on its own, by [`super::verify_group`].

use std::io::Write;

use crate::err::{Error, Result};
use crate::extent::EgroupStore;

use super::throttle::{Clock, TokenBucket};
use super::{sha256, verify_group, write_frame, Frame, GroupRef, Manifest, CHUNK, MAX_GROUP};

/// Where a group's bytes come from: `length` bytes from the start of the group.
pub trait GroupSource {
    fn read_group(&self, id: &str, length: u64) -> Result<Vec<u8>>;
}

/// The node's own extent store. The first `length` bytes of the file, which for a group still
/// open at the source is the immutable prefix the map references.
impl GroupSource for EgroupStore {
    fn read_group(&self, id: &str, length: u64) -> Result<Vec<u8>> {
        use std::io::Read;
        if length > MAX_GROUP {
            return Err(Error::refused(format!("group {id} is too large to ship")));
        }
        let path = self.path_for(id);
        let mut file = std::fs::File::open(&path)
            .map_err(|e| Error::io(format!("extent group {} unreadable: {e}", path.display())))?;
        let mut buf = vec![0u8; length as usize];
        file.read_exact(&mut buf).map_err(|e| {
            Error::corrupt(format!("extent group {id} is shorter than the manifest says: {e}"))
        })?;
        Ok(buf)
    }
}

/// One group to send. `source_id` is the id the source knows it by; `group.id` is the target's
/// (they differ for the prefix of a group that was open). `start` resumes a partial transfer.
pub struct Want {
    pub group: GroupRef,
    pub source_id: String,
    pub start: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ExportReport {
    pub groups: usize,
    pub bytes_sent: u64,
}

pub fn export_groups(
    manifest: &Manifest,
    src: &dyn GroupSource,
    wants: &[Want],
    out: &mut dyn Write,
    bucket: &mut TokenBucket,
    clock: &dyn Clock,
) -> Result<ExportReport> {
    manifest.validate()?;
    let mut report = ExportReport::default();
    for want in wants {
        let g = &want.group;
        if !manifest.groups.iter().any(|m| m == g) {
            return Err(Error::refused(format!("group {} is not in the manifest", g.id)));
        }
        if want.start > g.length || want.start % CHUNK as u64 != 0 {
            return Err(Error::refused(format!(
                "group {} cannot resume at {}: not a chunk boundary inside the group",
                g.id, want.start)));
        }
        let bytes = src.read_group(&want.source_id, g.length)?;
        verify_group(g, &bytes, &manifest.rows_for(&g.id)).map_err(|e| {
            Error::corrupt(format!("not sending {}: its source copy is damaged: {e}", g.id))
        })?;
        write_frame(out, &Frame::Begin { id: g.id.clone(), length: g.length, start: want.start })?;
        let mut offset = want.start as usize;
        while offset < bytes.len() {
            let end = (offset + CHUNK).min(bytes.len());
            bucket.take(clock, (end - offset) as u64);
            write_frame(out, &Frame::Chunk {
                id: g.id.clone(), offset: offset as u64, data: bytes[offset..end].to_vec() })?;
            report.bytes_sent += (end - offset) as u64;
            offset = end;
        }
        write_frame(out, &Frame::End { id: g.id.clone(), sha256: sha256(&bytes) })?;
        report.groups += 1;
    }
    out.flush()?;
    Ok(report)
}
