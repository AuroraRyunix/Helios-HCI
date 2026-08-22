//! Extent groups: append-only files that hold drained extents.
//!
//! An egroup is opened, appended to until it reaches its size, then **sealed**. Sealed
//! means immutable, permanently, and that single property is what buys most of the
//! design's simplicity: repair is a checksum comparison rather than a divergence
//! protocol, a snapshot is a map copy rather than a data copy, and scrub needs no lock
//! because nothing it reads can change underneath it.
//!
//! Overwrites therefore never happen in place. A rewritten extent is appended somewhere
//! new and the map is repointed -- redirect-on-write -- and the bytes it used to occupy
//! become garbage for the curator to sweep. That trade is deliberate: garbage collection
//! is a performance problem, and replica divergence is a correctness problem.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::crc::crc32c;
use crate::err::{Error, Result};

/// Each extent stored in an egroup is followed by a footer, so a read can tell that it
/// got the bytes it asked for and not a neighbour's.
///
/// ```text
/// crc u32 | algorithm u8 | reserved [u8;3] | vdisk_hash u64 | extent_index u64
/// ```
///
/// `vdisk_hash` and `extent_index` are what make a *misdirected* read self-evident: a
/// correct checksum only proves the bytes are undamaged, not that they are the right
/// bytes. This is invariant I-8's mechanism.
pub const FOOTER_LEN: usize = 32;
pub const ALGO_CRC32C: u8 = 1;

pub fn footer_for(data: &[u8], vdisk_hash: u64, extent_index: u64) -> [u8; FOOTER_LEN] {
    let mut f = [0u8; FOOTER_LEN];
    f[0..4].copy_from_slice(&crc32c(0, data).to_le_bytes());
    f[4] = ALGO_CRC32C;
    f[8..16].copy_from_slice(&vdisk_hash.to_le_bytes());
    f[16..24].copy_from_slice(&extent_index.to_le_bytes());
    f
}

pub fn verify_footer(
    data: &[u8],
    footer: &[u8],
    vdisk_hash: u64,
    extent_index: u64,
) -> Result<()> {
    if footer.len() < FOOTER_LEN {
        return Err(Error::corrupt("extent footer truncated".to_string()));
    }
    let algo = footer[4];
    if algo != ALGO_CRC32C {
        return Err(Error::corrupt(format!(
            "extent footer names checksum algorithm {algo}, which this build cannot verify"
        )));
    }
    let want = u32::from_le_bytes(footer[0..4].try_into().unwrap());
    let got = crc32c(0, data);
    if want != got {
        return Err(Error::corrupt(format!(
            "extent {extent_index}: checksum {got:#010x} does not match stored {want:#010x}"
        )));
    }
    let f_vdisk = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    let f_index = u64::from_le_bytes(footer[16..24].try_into().unwrap());
    if f_vdisk != vdisk_hash || f_index != extent_index {
        return Err(Error::corrupt(format!(
            "misdirected read: asked for vdisk {vdisk_hash:#x} extent {extent_index}, \
             found vdisk {f_vdisk:#x} extent {f_index}"
        )));
    }
    Ok(())
}

/// Stable 64-bit digest of a vdisk id, for the footer's identity field. FNV-1a: this is
/// a tag, not a security boundary, and it must produce the same value in every build.
pub fn vdisk_hash(id: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in id.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

pub struct EgroupStore {
    dir: PathBuf,
    egroup_bytes: u64,
}

pub struct OpenEgroup {
    pub id: String,
    pub file: File,
    pub size: u64,
}

impl EgroupStore {
    pub fn new(dir: &Path, egroup_bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(EgroupStore { dir: dir.to_path_buf(), egroup_bytes })
    }

    pub fn path_for(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.eg"))
    }

    pub fn egroup_bytes(&self) -> u64 {
        self.egroup_bytes
    }

    pub fn create(&self, id: &str) -> Result<OpenEgroup> {
        let path = self.path_for(id);
        let file = OpenOptions::new().read(true).write(true).create_new(true).open(&path)?;
        // Sync the directory so the new file's *name* survives a crash, not just its
        // contents. Without this an egroup can be referenced by a committed map row and
        // not exist after a power cut, which reads as data loss.
        File::open(&self.dir).and_then(|d| d.sync_all())?;
        Ok(OpenEgroup { id: id.to_string(), file, size: 0 })
    }

    /// Append one extent plus its footer. Returns the offset the extent starts at.
    pub fn append(
        &self,
        eg: &mut OpenEgroup,
        data: &[u8],
        vdisk_hash: u64,
        extent_index: u64,
    ) -> Result<u32> {
        let footer = footer_for(data, vdisk_hash, extent_index);
        let offset = eg.size;
        let mut buf = Vec::with_capacity(data.len() + FOOTER_LEN);
        buf.extend_from_slice(data);
        buf.extend_from_slice(&footer);
        eg.file.seek(SeekFrom::Start(offset))?;
        eg.file.write_all(&buf)?;
        eg.size += buf.len() as u64;
        Ok(offset as u32)
    }

    /// Make everything appended so far durable. Called once per drain batch, before the
    /// map that will point at it is written: data before metadata, always.
    pub fn sync(&self, eg: &mut OpenEgroup) -> Result<()> {
        eg.file.sync_data()?;
        Ok(())
    }

    pub fn is_full(&self, eg: &OpenEgroup) -> bool {
        eg.size >= self.egroup_bytes
    }

    /// Read one extent back and check it is the extent that was asked for.
    pub fn read_extent(
        &self,
        egroup_id: &str,
        offset: u32,
        length: u32,
        vdisk_hash: u64,
        extent_index: u64,
    ) -> Result<Vec<u8>> {
        let path = self.path_for(egroup_id);
        let mut file = File::open(&path).map_err(|e| {
            Error::io(format!("extent group {} unreadable: {e}", path.display()))
        })?;
        let mut buf = vec![0u8; length as usize + FOOTER_LEN];
        file.seek(SeekFrom::Start(offset as u64))?;
        file.read_exact(&mut buf).map_err(|e| {
            Error::corrupt(format!(
                "extent group {egroup_id} is shorter than the map claims \
                 (wanted {} bytes at {offset}): {e}",
                length as usize + FOOTER_LEN
            ))
        })?;
        let (data, footer) = buf.split_at(length as usize);
        verify_footer(data, footer, vdisk_hash, extent_index)?;
        Ok(data.to_vec())
    }

    /// The seal hash over a whole egroup file, recorded at seal time so scrub has
    /// something to compare against that was computed when the data was known good.
    pub fn seal_hash(&self, id: &str) -> Result<String> {
        let mut file = File::open(self.path_for(id))?;
        let mut crc = 0u32;
        let mut buf = vec![0u8; 1 << 16];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            crc = crc32c(crc, &buf[..n]);
        }
        Ok(format!("crc32c:{crc:08x}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("sidon-eg-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn append_and_read_back() {
        let dir = tmpdir("roundtrip");
        let store = EgroupStore::new(&dir, 1 << 20).unwrap();
        let mut eg = store.create("eg-test-1").unwrap();
        let vh = vdisk_hash("vd-1");
        let data = vec![0x5Au8; 4096];
        let off = store.append(&mut eg, &data, vh, 3).unwrap();
        store.sync(&mut eg).unwrap();
        let back = store.read_extent("eg-test-1", off, 4096, vh, 3).unwrap();
        assert_eq!(back, data);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corruption_is_an_error_not_a_value() {
        let dir = tmpdir("corrupt");
        let store = EgroupStore::new(&dir, 1 << 20).unwrap();
        let mut eg = store.create("eg-c").unwrap();
        let vh = vdisk_hash("vd-1");
        let off = store.append(&mut eg, &vec![7u8; 512], vh, 0).unwrap();
        store.sync(&mut eg).unwrap();
        drop(eg);

        let mut f = OpenOptions::new().write(true).open(store.path_for("eg-c")).unwrap();
        f.seek(SeekFrom::Start(off as u64 + 10)).unwrap();
        f.write_all(b"X").unwrap();
        drop(f);

        match store.read_extent("eg-c", off, 512, vh, 0) {
            Err(Error::Corrupt(_)) => {}
            other => panic!("expected a corruption error, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_misdirected_read_is_caught_even_though_the_checksum_is_fine() {
        let dir = tmpdir("misdirect");
        let store = EgroupStore::new(&dir, 1 << 20).unwrap();
        let mut eg = store.create("eg-m").unwrap();
        let vh = vdisk_hash("vd-1");
        let off = store.append(&mut eg, &vec![1u8; 256], vh, 42).unwrap();
        store.sync(&mut eg).unwrap();

        // Same bytes, same checksum, wrong extent: this is the failure a CRC alone
        // cannot see, and the reason the footer carries identity.
        match store.read_extent("eg-m", off, 256, vh, 43) {
            Err(Error::Corrupt(m)) => assert!(m.contains("misdirected"), "{m}"),
            other => panic!("expected a misdirect error, got {other:?}"),
        }
        match store.read_extent("eg-m", off, 256, vdisk_hash("vd-other"), 42) {
            Err(Error::Corrupt(m)) => assert!(m.contains("misdirected"), "{m}"),
            other => panic!("expected a misdirect error, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn vdisk_hash_is_stable_and_distinguishing() {
        assert_eq!(vdisk_hash("vd-1"), vdisk_hash("vd-1"));
        assert_ne!(vdisk_hash("vd-1"), vdisk_hash("vd-2"));
    }
}
