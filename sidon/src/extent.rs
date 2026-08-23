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
//! become garbage for Purah to sweep. That trade is deliberate: garbage collection
//! is a performance problem, and replica divergence is a correctness problem.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::crc::crc32c;
use crate::err::{Error, Result};

/// Each extent stored in an egroup is followed by a footer, so a read can tell that it
/// got the bytes it asked for and not a neighbour's.
///
/// ```text
/// crc u32 | algorithm u8 | codec u8 | reserved [u8;2] | vdisk_hash u64
///                             | extent_index u64 | plain_len u64
/// ```
///
/// The crc covers the bytes **as stored**, compressed or not, so a scrub can check an
/// extent group without decompressing any of it and a replica copy stays a byte copy.
/// `plain_len` is what the extent expands back to; it is meaningless when `codec` is
/// `COMP_NONE`, which is what every footer written before compression existed says.
///
/// `vdisk_hash` and `extent_index` are what make a *misdirected* read self-evident: a
/// correct checksum only proves the bytes are undamaged, not that they are the right
/// bytes. This is invariant I-8's mechanism.
pub const FOOTER_LEN: usize = 32;
pub const ALGO_CRC32C: u8 = 1;

/// Stored verbatim. Zero on purpose: every footer written before compression existed
/// already reads as this, so old extents need no backfill and no version check.
pub const COMP_NONE: u8 = 0;
pub const COMP_LZ4: u8 = 1;

pub fn footer_with(
    stored: &[u8],
    vdisk_hash: u64,
    extent_index: u64,
    codec: u8,
    plain_len: u64,
) -> [u8; FOOTER_LEN] {
    let mut f = [0u8; FOOTER_LEN];
    f[0..4].copy_from_slice(&crc32c(0, stored).to_le_bytes());
    f[4] = ALGO_CRC32C;
    f[5] = codec;
    f[8..16].copy_from_slice(&vdisk_hash.to_le_bytes());
    f[16..24].copy_from_slice(&extent_index.to_le_bytes());
    f[24..32].copy_from_slice(&plain_len.to_le_bytes());
    f
}

/// Compress an extent, or decline to.
///
/// Returns the bytes to store and the codec that describes them. Incompressible data is
/// stored verbatim rather than slightly larger: a codec that can inflate its input is a
/// codec that can overrun an extent group's budget on exactly the data that gains nothing
/// from it.
pub fn encode_extent(data: &[u8], compress: bool) -> (Vec<u8>, u8) {
    if !compress || data.is_empty() {
        return (data.to_vec(), COMP_NONE);
    }
    let packed = lz4_flex::block::compress(data);
    if packed.len() < data.len() {
        (packed, COMP_LZ4)
    } else {
        (data.to_vec(), COMP_NONE)
    }
}

/// Expand an extent back to what the guest wrote, per the codec its footer names.
///
/// An unknown codec is corruption, not a newer format to be tolerated: the alternative is
/// handing a guest bytes this build cannot prove are its own.
pub fn decode_extent(stored: &[u8], footer: &[u8]) -> Result<Vec<u8>> {
    if footer.len() < FOOTER_LEN {
        return Err(Error::corrupt("extent footer truncated".to_string()));
    }
    match footer[5] {
        COMP_NONE => Ok(stored.to_vec()),
        COMP_LZ4 => {
            let plain_len = u64::from_le_bytes(footer[24..32].try_into().unwrap()) as usize;
            lz4_flex::block::decompress(stored, plain_len).map_err(|e| {
                Error::corrupt(format!("extent will not decompress: {e}"))
            })
        }
        other => Err(Error::corrupt(format!(
            "extent footer names compression codec {other}, which this build cannot read"
        ))),
    }
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

/// One disk's worth of extent groups.
///
/// A disk is a filesystem, not a slice of a pooled one. Putting several disks into a
/// single LVM volume group would make one disk failure take the whole node's extent store
/// -- and make it twice as likely, because there would be two disks able to cause it. So
/// each disk is mounted on its own and the placement happens here, in software. See
/// docs/dfs/multi_disk.md.
pub struct Disk {
    /// Directory name under `disks/`, or "0" for a node that predates this.
    pub id: String,
    /// Where this disk's extent groups live.
    pub root: PathBuf,
}

/// Total and available bytes of the filesystem holding `path`.
pub fn disk_space(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    #[repr(C)]
    #[derive(Default)]
    struct StatVfs {
        f_bsize: u64,
        f_frsize: u64,
        f_blocks: u64,
        f_bfree: u64,
        f_bavail: u64,
        f_files: u64,
        f_ffree: u64,
        f_favail: u64,
        f_fsid: u64,
        f_flag: u64,
        f_namemax: u64,
        f_spare: [u32; 6],
    }
    extern "C" {
        fn statvfs(path: *const u8, buf: *mut StatVfs) -> i32;
    }
    let mut c_path: Vec<u8> = path.as_os_str().as_bytes().to_vec();
    c_path.push(0);
    let mut buf = StatVfs::default();
    let rc = unsafe { statvfs(c_path.as_ptr(), &mut buf) };
    if rc != 0 {
        return None;
    }
    let unit = if buf.f_frsize > 0 { buf.f_frsize } else { buf.f_bsize };
    Some((buf.f_blocks.saturating_mul(unit), buf.f_bavail.saturating_mul(unit)))
}

/// The disks this node has, in a stable order.
///
/// A node provisioned before multiple disks existed keeps its extent groups in
/// `<root>/egroups`, and that directory stays exactly where it is -- it becomes disk "0"
/// rather than being migrated, because moving a live extent store to gain a naming
/// convention is all risk and no benefit.
/// Whether `path` is the root of its own filesystem.
///
/// Compares the device id of the directory with its parent's, which is what `mountpoint`
/// does. A directory under `disks/` that is *not* a separate filesystem is a leftover --
/// a mount that failed at boot, or a directory created before the disk behind it was
/// ready -- and using it would put extent groups on the root filesystem, which is the one
/// place a full extent store must never be able to fill.
fn is_separate_filesystem(path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let here = match std::fs::metadata(path) {
        Ok(m) => m.dev(),
        Err(_) => return false,
    };
    match path.parent().and_then(|p| std::fs::metadata(p).ok()) {
        Some(parent) => here != parent.dev(),
        None => false,
    }
}

pub fn discover_disks(root: &Path) -> Vec<Disk> {
    discover_disks_with(root, &is_separate_filesystem)
}

/// `discover_disks`, with the "is this really its own disk" test injected.
///
/// The test is a parameter so it can be exercised without mounting filesystems. Nothing in
/// production passes anything but `is_separate_filesystem`.
pub fn discover_disks_with(root: &Path, is_own_filesystem: &dyn Fn(&Path) -> bool) -> Vec<Disk> {
    let mut disks = Vec::new();
    let disks_dir = root.join("disks");
    if disks_dir.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(&disks_dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        // Sorted so the order does not depend on readdir, which would make the disk a
        // request lands on vary between restarts for no reason.
        entries.sort();
        for name in entries {
            let mount = disks_dir.join(&name);
            if !is_own_filesystem(&mount) {
                // Skipped rather than used. Announced, because a disk silently missing
                // from the store is capacity an operator thinks they have.
                eprintln!(
                    "sidon: {} is not a mounted filesystem and will not be used as a disk; \
                     check that its device is mounted there",
                    mount.display()
                );
                continue;
            }
            disks.push(Disk {
                id: name.clone(),
                root: mount.join("egroups"),
            });
        }
    }

    // `<root>/egroups` is a disk when `<root>` is a filesystem of its own -- which it is
    // on every provisioned node, where vg_aether/sidon is mounted there -- or when it
    // already holds extent groups.
    //
    // The mount test rather than "does it contain data", because an empty filesystem is
    // still a disk: judging by content drops a freshly provisioned node's entire first
    // disk for the sole reason that nothing has been written to it yet. And the test is
    // needed at all because `<root>/egroups` is created unconditionally at startup, so on
    // a node where `<root>` is just a directory on `/`, counting it would place extent
    // groups on the root filesystem -- the one place a full extent store must never be
    // able to fill, because it would wedge the host.
    let legacy = root.join("egroups");
    let legacy_holds_data = std::fs::read_dir(&legacy)
        .map(|entries| {
            entries.flatten().any(|e| {
                e.file_name().to_string_lossy().ends_with(".eg")
            })
        })
        .unwrap_or(false);
    if is_own_filesystem(root) || legacy_holds_data {
        // First, so a node that predates `disks/` keeps serving from where its data is.
        disks.insert(0, Disk { id: "0".to_string(), root: legacy });
    } else if disks.is_empty() {
        // Nothing else to use. A dev box with no dedicated disk still needs somewhere.
        disks.push(Disk { id: "0".to_string(), root: legacy });
    }
    disks
}

pub struct EgroupStore {
    disks: Vec<Disk>,
    egroup_bytes: u64,
    /// Which disk holds each extent group.
    ///
    /// Built by scanning at startup and updated as groups are created. Node-local on
    /// purpose: which disk an egroup sits on is this node's business, and recording it in
    /// Hydra would turn every local placement decision into a cluster write and stop a
    /// group from ever being moved between disks.
    index: Mutex<HashMap<String, usize>>,
}

pub struct OpenEgroup {
    pub id: String,
    pub file: File,
    pub size: u64,
}

impl EgroupStore {
    /// A store over one directory.
    ///
    /// Test-only: production discovers its disks, and a constructor that quietly accepts
    /// a single path is how a multi-disk node ends up using one of them.
    #[cfg(test)]
    pub fn new(dir: &Path, egroup_bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        Self::open(vec![Disk { id: "0".to_string(), root: dir.to_path_buf() }], egroup_bytes)
    }

    /// A store over every disk this node has.
    ///
    /// Each disk directory is created if absent and scanned for the extent groups it
    /// already holds. A disk that cannot be read is *skipped rather than fatal*: a node
    /// with one failed disk must keep serving what its other disks hold, and the groups
    /// that went with it are recovered the same way a lost node's are -- they become
    /// referenced-but-absent, which is what Purah's repair pass is looking for.
    pub fn open(disks: Vec<Disk>, egroup_bytes: u64) -> Result<Self> {
        let mut usable = Vec::new();
        let mut index = HashMap::new();
        for disk in disks {
            if let Err(e) = std::fs::create_dir_all(&disk.root) {
                eprintln!(
                    "sidon: disk {} at {} is unusable and will be skipped: {e}",
                    disk.id,
                    disk.root.display()
                );
                continue;
            }
            let slot = usable.len();
            if let Ok(entries) = std::fs::read_dir(&disk.root) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if let Some(id) = name.strip_suffix(".eg") {
                        index.insert(id.to_string(), slot);
                    }
                }
            }
            usable.push(disk);
        }
        if usable.is_empty() {
            return Err(Error::io("no usable disk for the extent store".to_string()));
        }
        Ok(EgroupStore { disks: usable, egroup_bytes, index: Mutex::new(index) })
    }

    /// The disk a new extent group should go on: the one with the most room.
    ///
    /// Least-full-first rather than round-robin. After a disk is added, round-robin would
    /// give it an equal share of new writes and it would stay permanently behind the
    /// others; this fills it preferentially until it catches up, which is what an operator
    /// expects to happen after adding a disk.
    fn placement(&self) -> usize {
        let mut best = 0usize;
        let mut best_free = 0u64;
        for (i, disk) in self.disks.iter().enumerate() {
            let free = disk_space(&disk.root).map(|(_, avail)| avail).unwrap_or(0);
            if free > best_free {
                best_free = free;
                best = i;
            }
        }
        best
    }

    pub fn path_for(&self, id: &str) -> PathBuf {
        let file = format!("{id}.eg");
        if let Ok(index) = self.index.lock() {
            if let Some(&slot) = index.get(id) {
                return self.disks[slot].root.join(&file);
            }
        }
        // Not indexed. Look for it, so a group that appeared after startup -- a repair
        // copy from a peer, say -- is found rather than reported missing.
        for (slot, disk) in self.disks.iter().enumerate() {
            let candidate = disk.root.join(&file);
            if candidate.exists() {
                if let Ok(mut index) = self.index.lock() {
                    index.insert(id.to_string(), slot);
                }
                return candidate;
            }
        }
        // Genuinely absent. Answer with a path on the disk it would be created on, so the
        // caller's own open() reports the miss with a real filename in the error.
        self.disks[self.placement()].root.join(&file)
    }

    pub fn create(&self, id: &str) -> Result<OpenEgroup> {
        let slot = self.placement();
        let dir = &self.disks[slot].root;
        let path = dir.join(format!("{id}.eg"));
        let file = OpenOptions::new().read(true).write(true).create_new(true).open(&path)?;
        // Sync the directory so the new file's *name* survives a crash, not just its
        // contents. Without this an egroup can be referenced by a committed map row and
        // not exist after a power cut, which reads as data loss.
        File::open(dir).and_then(|d| d.sync_all())?;
        if let Ok(mut index) = self.index.lock() {
            index.insert(id.to_string(), slot);
        }
        Ok(OpenEgroup { id: id.to_string(), file, size: 0 })
    }

    /// Append one extent plus its footer, handing back the offset and the exact bytes
    /// written.
    ///
    /// The caller ships those bytes to the replicas rather than re-framing there, so every
    /// copy is byte-identical and repair stays a checksum comparison instead of a
    /// divergence protocol.
    pub fn append_framed(
        &self,
        eg: &mut OpenEgroup,
        data: &[u8],
        vdisk_hash: u64,
        extent_index: u64,
        compress: bool,
    ) -> Result<(u32, u32, Vec<u8>)> {
        let (stored, codec) = encode_extent(data, compress);
        let footer = footer_with(&stored, vdisk_hash, extent_index, codec, data.len() as u64);
        let offset = eg.size;
        let mut buf = Vec::with_capacity(stored.len() + FOOTER_LEN);
        buf.extend_from_slice(&stored);
        buf.extend_from_slice(&footer);
        eg.file.seek(SeekFrom::Start(offset))?;
        eg.file.write_all(&buf)?;
        eg.size += buf.len() as u64;
        // The stored length, which is what the block map has to record: a read seeks by it
        // and a compressed extent is not the size the guest thinks it is.
        Ok((offset as u32, stored.len() as u32, buf))
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
        let (stored, footer) = buf.split_at(length as usize);
        verify_footer(stored, footer, vdisk_hash, extent_index)?;
        decode_extent(stored, footer)
    }

    /// One extent plus its footer, exactly as stored.
    ///
    /// Deliberately unverified: this is for copying bytes to a new replica, and verifying
    /// here would mean a single damaged extent aborts a whole re-replication. The copy is
    /// verified where it is *used* -- every read checks the footer, on the local copy and
    /// on a replica's alike.
    pub fn read_extent_framed(&self, egroup_id: &str, offset: u32, length: u32) -> Result<Vec<u8>> {
        let path = self.path_for(egroup_id);
        let mut file = File::open(&path)
            .map_err(|e| Error::io(format!("extent group {} unreadable: {e}", path.display())))?;
        let mut buf = vec![0u8; length as usize + FOOTER_LEN];
        file.seek(SeekFrom::Start(offset as u64))?;
        file.read_exact(&mut buf).map_err(|e| {
            Error::corrupt(format!("extent group {egroup_id} is shorter than the map claims: {e}"))
        })?;
        Ok(buf)
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

    fn disk_at(parent: &PathBuf, name: &str) -> Disk {
        let root = parent.join("disks").join(name).join("egroups");
        std::fs::create_dir_all(&root).unwrap();
        Disk { id: name.to_string(), root }
    }

    #[test]
    fn the_parent_directory_is_not_a_disk_unless_it_holds_data() {
        // `<root>/egroups` is created unconditionally at startup. Counting an empty one as
        // a disk would place extent groups on the root filesystem -- the one place a full
        // extent store must never be able to fill, because it would wedge the host.
        let dir = tmpdir("discover-empty-parent");
        std::fs::create_dir_all(dir.join("egroups")).unwrap();
        std::fs::create_dir_all(dir.join("disks").join("d0").join("egroups")).unwrap();
        std::fs::create_dir_all(dir.join("disks").join("d1").join("egroups")).unwrap();

        // The parent is not its own filesystem here, so it must not be counted.
        let found = discover_disks_with(&dir, &|p: &Path| {
            p.file_name().map(|n| n == "d0" || n == "d1").unwrap_or(false)
        });
        assert_eq!(found.len(), 2, "the empty parent directory was counted as a disk");
        assert_eq!(found[0].id, "d0");
        assert_eq!(found[1].id, "d1");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_node_that_predates_disks_keeps_serving_from_where_its_data_is() {
        let dir = tmpdir("discover-legacy");
        let legacy = dir.join("egroups");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("eg-old.eg"), b"x").unwrap();
        std::fs::create_dir_all(dir.join("disks").join("d1").join("egroups")).unwrap();

        let found = discover_disks_with(&dir, &|_| true);
        assert_eq!(found.len(), 2);
        // First, so its extent groups are found without moving a live store to gain a
        // naming convention.
        assert_eq!(found[0].id, "0");
        assert_eq!(found[0].root, legacy);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_node_with_neither_still_gets_one_disk() {
        let dir = tmpdir("discover-bare");
        std::fs::create_dir_all(&dir).unwrap();
        let found = discover_disks_with(&dir, &|_| true);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].root, dir.join("egroups"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn discovery_order_does_not_depend_on_readdir() {
        let dir = tmpdir("discover-order");
        for name in ["d2", "d0", "d1"] {
            std::fs::create_dir_all(dir.join("disks").join(name).join("egroups")).unwrap();
        }
        // Only the disks/ entries are their own filesystems, so the ordering under test is
        // theirs alone.
        let ids: Vec<String> = discover_disks_with(&dir, &|p: &Path| {
            p.parent().map(|q| q.ends_with("disks")).unwrap_or(false)
        })
        .into_iter()
        .map(|d| d.id)
        .collect();
        assert_eq!(ids, vec!["d0", "d1", "d2"],
                   "the disk a write lands on would vary between restarts");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_disk_directory_that_is_not_a_mount_is_refused() {
        // A mount that failed at boot, or a directory made before its device was ready,
        // leaves an ordinary directory on the root filesystem. Using it would put extent
        // groups exactly where a full extent store must never be able to fill.
        let dir = tmpdir("discover-unmounted");
        std::fs::create_dir_all(dir.join("disks").join("d0").join("egroups")).unwrap();
        std::fs::create_dir_all(dir.join("disks").join("d1").join("egroups")).unwrap();

        // Only d1 is its own filesystem.
        let found = discover_disks_with(&dir, &|p: &Path| {
            p.file_name().map(|n| n == "d1").unwrap_or(false)
        });
        assert_eq!(found.len(), 1, "an unmounted disk directory was used");
        assert_eq!(found[0].id, "d1");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_extent_group_is_found_whichever_disk_holds_it() {
        let dir = tmpdir("multi-read");
        let disks = vec![disk_at(&dir, "d0"), disk_at(&dir, "d1")];
        let second = disks[1].root.clone();
        let store = EgroupStore::open(disks, 1 << 20).unwrap();

        // Placed by hand on the second disk, as a repair copy from a peer would be.
        std::fs::write(second.join("eg-elsewhere.eg"), b"payload").unwrap();

        assert_eq!(store.path_for("eg-elsewhere"), second.join("eg-elsewhere.eg"),
                   "a group on another disk was not found");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_group_written_to_one_disk_reads_back_from_it() {
        let dir = tmpdir("multi-roundtrip");
        let disks = vec![disk_at(&dir, "d0"), disk_at(&dir, "d1")];
        let store = EgroupStore::open(disks, 1 << 20).unwrap();
        let vh = vdisk_hash("vd-m");
        let mut eg = store.create("eg-m").unwrap();
        let data = vec![0x42u8; 2048];
        let (off, stored, _) = store.append_framed(&mut eg, &data, vh, 5, false).unwrap();
        store.sync(&mut eg).unwrap();

        assert_eq!(store.read_extent("eg-m", off, stored, vh, 5).unwrap(), data);
        // And it went to exactly one of them.
        let on_disk: Vec<_> = ["d0", "d1"].iter()
            .filter(|n| dir.join("disks").join(n).join("egroups").join("eg-m.eg").exists())
            .collect();
        assert_eq!(on_disk.len(), 1, "an extent group exists on more than one local disk");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_disk_that_cannot_be_used_is_skipped_rather_than_fatal() {
        // A node with one failed disk must keep serving what its other disks hold. The
        // groups that went with it become referenced-but-absent, which is what the sweep
        // reports.
        let dir = tmpdir("multi-degraded");
        let good = disk_at(&dir, "d0");
        // A file where a directory should be: create_dir_all on it fails.
        let bad_root = dir.join("disks").join("d1").join("egroups");
        std::fs::create_dir_all(bad_root.parent().unwrap()).unwrap();
        std::fs::write(&bad_root, b"not a directory").unwrap();

        let store = EgroupStore::open(
            vec![good, Disk { id: "d1".to_string(), root: bad_root }], 1 << 20).unwrap();
        // Still usable: the surviving disk takes the write.
        assert!(store.create("eg-survives").is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_store_with_no_usable_disk_is_an_error_not_an_empty_one() {
        let dir = tmpdir("multi-none");
        std::fs::create_dir_all(&dir).unwrap();
        let bad = dir.join("nope");
        std::fs::write(&bad, b"file").unwrap();
        match EgroupStore::open(vec![Disk { id: "d0".to_string(), root: bad }], 1 << 20) {
            Err(Error::Io(_)) => {}
            Err(other) => panic!("expected an io error, got {other:?}"),
            Ok(_) => panic!("a store with no usable disk was accepted"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn append_and_read_back() {
        let dir = tmpdir("roundtrip");
        let store = EgroupStore::new(&dir, 1 << 20).unwrap();
        let mut eg = store.create("eg-test-1").unwrap();
        let vh = vdisk_hash("vd-1");
        let data = vec![0x5Au8; 4096];
        let (off, _len, _) = store.append_framed(&mut eg, &data, vh, 3, false).unwrap();
        store.sync(&mut eg).unwrap();
        let back = store.read_extent("eg-test-1", off, 4096, vh, 3).unwrap();
        assert_eq!(back, data);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_compressed_extent_reads_back_as_what_was_written() {
        let dir = tmpdir("comp-roundtrip");
        let store = EgroupStore::new(&dir, 1 << 20).unwrap();
        let mut eg = store.create("eg-z").unwrap();
        let vh = vdisk_hash("vd-z");
        // Compressible on purpose: a guest filesystem is mostly zeroes and repetition,
        // which is the case compression exists for.
        let mut data = vec![0u8; 64 * 1024];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i / 512) as u8;
        }
        let (off, stored, _) = store.append_framed(&mut eg, &data, vh, 9, true).unwrap();
        store.sync(&mut eg).unwrap();

        assert!(
            (stored as usize) < data.len(),
            "compressible data was stored at {stored} bytes, no smaller than {}",
            data.len()
        );
        // Read by the *stored* length, which is what the block map records.
        let back = store.read_extent("eg-z", off, stored, vh, 9).unwrap();
        assert_eq!(back, data, "an extent did not survive a compress/decompress round trip");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn incompressible_data_is_stored_verbatim_rather_than_larger() {
        // LZ4 on random bytes produces more than it consumed. Storing that would let a
        // setting meant to save space cost it, and overrun an egroup's budget on exactly
        // the data that gains nothing.
        let mut x: u32 = 0x1234_5678;
        let data: Vec<u8> = (0..8192)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                (x & 0xFF) as u8
            })
            .collect();
        let (stored, codec) = encode_extent(&data, true);
        assert_eq!(codec, COMP_NONE, "random data was kept in compressed form");
        assert_eq!(stored.len(), data.len());
    }

    #[test]
    fn an_uncompressed_footer_still_reads_on_a_build_that_knows_compression() {
        // Every extent written before compression existed has codec 0 and plain_len 0.
        // Those must keep reading exactly as they did, with no migration.
        let dir = tmpdir("legacy");
        let store = EgroupStore::new(&dir, 1 << 20).unwrap();
        let mut eg = store.create("eg-l").unwrap();
        let vh = vdisk_hash("vd-l");
        let data = vec![0xABu8; 1024];
        let (off, stored, _) = store.append_framed(&mut eg, &data, vh, 1, false).unwrap();
        store.sync(&mut eg).unwrap();
        assert_eq!(stored as usize, data.len());
        assert_eq!(store.read_extent("eg-l", off, stored, vh, 1).unwrap(), data);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_checksum_covers_the_bytes_as_stored() {
        // So a scrub can verify an extent group without decompressing any of it, and a
        // replica copy stays a byte copy.
        let data = vec![3u8; 4096];
        let (stored, codec) = encode_extent(&data, true);
        assert_eq!(codec, COMP_LZ4);
        let footer = footer_with(&stored, 7, 2, codec, data.len() as u64);
        verify_footer(&stored, &footer, 7, 2).expect("footer must verify against stored bytes");
        assert_eq!(decode_extent(&stored, &footer).unwrap(), data);
    }

    #[test]
    fn an_unknown_codec_is_refused_rather_than_guessed_at() {
        let data = vec![1u8; 64];
        let mut footer = footer_with(&data, 1, 1, COMP_NONE, data.len() as u64);
        footer[5] = 77;
        match decode_extent(&data, &footer) {
            Err(Error::Corrupt(m)) => assert!(m.contains("77"), "{m}"),
            other => panic!("expected a corruption error, got {other:?}"),
        }
    }

    #[test]
    fn corruption_is_an_error_not_a_value() {
        let dir = tmpdir("corrupt");
        let store = EgroupStore::new(&dir, 1 << 20).unwrap();
        let mut eg = store.create("eg-c").unwrap();
        let vh = vdisk_hash("vd-1");
        let (off, _len, _) = store.append_framed(&mut eg, &vec![7u8; 512], vh, 0, false).unwrap();
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
        let (off, _len, _) = store.append_framed(&mut eg, &vec![1u8; 256], vh, 42, false).unwrap();
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
