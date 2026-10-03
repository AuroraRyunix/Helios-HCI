//! Which disk an extent group sits on, and moving one to another.
//!
//! Three things live here, kept out of `extent.rs` because they are about the *disks* and
//! not about the format of what is on them:
//!
//! 1. **Disk identity.** A disk is named by what is written on its filesystem, never by
//!    where the kernel happened to mount it.
//! 2. **The tier of a disk**, and the rule that picks one for a new group.
//! 3. **Moving a sealed group between two disks of the same node**, and cleaning up the
//!    copy it leaves behind.
//!
//! # Why the name of a disk is not its identity
//!
//! Disks were first named by kernel device name -- `disks/sdc`. Kernel names are assigned
//! in probe order, and probe order is not stable: on one node the disk filling the `sdc`
//! role is `/dev/sdb`, so the directory name already says something false. Anything keyed
//! by that name -- a placement record, an operator's note that "sdc is the slow one" -- is
//! keyed by a guess. So each filesystem carries its own identity, a file written once at
//! the top of it (`disk.uid`), and everything that needs to refer to a disk refers to
//! that. The directory name survives as a *label*, shown to an operator and accepted as a
//! convenience when naming a disk, but never written into anything that has to stay true.
//!
//! The identity travels with the filesystem. Re-cabling a disk, or a reboot that reorders
//! the probe, changes the label and leaves the identity alone, which is the whole point.
//!
//! # Why a move leaves the old copy behind
//!
//! A move is copy, verify, switch, delete -- and the delete is not part of the move. The
//! old copy is left where it is and is removed later by Purah's sweep under the same rule
//! that governs reclamation: seen as surplus on two consecutive passes with a grace period
//! between them. Three reasons, all of them ways a reader can be pointed at a file that is
//! about to vanish:
//!
//! - Every attached vdisk holds its *own* `EgroupStore` with its own index. Switching one
//!   store's index does not switch the others; a reader that resolved the old path a
//!   moment ago will still open it. Deleting immediately would hand that reader ENOENT.
//! - It makes a crash at any point harmless without a journal: before the switch there is
//!   one valid copy, after it there are two, and neither state needs recovering.
//! - Deletion is the one irreversible step. Putting it behind the same two-scan grace the
//!   sweep already trusts means there is exactly one rule in this daemon for "it is safe to
//!   remove bytes", rather than a second one invented for tiering.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde_json::{json, Value};

use super::{disk_space, Disk, EgroupStore};
use crate::crc::crc32c;
use crate::err::{Error, Result};

/// The file at the top of a disk's filesystem that says which disk it is.
pub const UID_FILE: &str = "disk.uid";
/// An operator's statement of what class of media a disk is, which wins over detection.
pub const TIER_FILE: &str = "disk.tier";
/// A copy in progress. Not ending in `.eg` is what keeps it out of every directory scan.
pub const MOVING_SUFFIX: &str = ".eg.moving";

/// What kind of media a disk is.
///
/// Ordered slowest to fastest, so `>` means "faster". `Unknown` is the lowest only so the
/// derive has something to order; the tiering policy does not treat it as slow, it treats
/// it as *not comparable* and leaves such a disk out entirely. A disk whose class could not
/// be established might be the fastest one on the node, and spilling data onto it as
/// though it were the slowest would be a guess presented as a policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Tier {
    Unknown,
    Hdd,
    Ssd,
    Nvme,
}

impl Tier {
    /// Accepts the container column's spelling (`SSD`, `HDD`, `NVME`) in any case.
    pub fn parse(s: &str) -> Tier {
        match s.trim().to_ascii_lowercase().as_str() {
            "hdd" => Tier::Hdd,
            "ssd" => Tier::Ssd,
            "nvme" => Tier::Nvme,
            _ => Tier::Unknown,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Tier::Unknown => "unknown",
            Tier::Hdd => "hdd",
            Tier::Ssd => "ssd",
            Tier::Nvme => "nvme",
        }
    }

    pub fn is_known(self) -> bool {
        self != Tier::Unknown
    }
}

// --- Identity -------------------------------------------------------------------------

fn valid_uid(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

fn fresh_uid() -> String {
    if let Ok(text) = std::fs::read_to_string("/proc/sys/kernel/random/uuid") {
        let text = text.trim().to_string();
        if valid_uid(&text) {
            return text;
        }
    }
    // No kernel uuid source: a time-and-pid tag. Not as good, still unique enough that two
    // disks on one node will not collide, which is all this identity has to guarantee.
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:x}-{:x}", nanos, std::process::id())
}

/// The identity of the disk whose extent groups are in `egroups_dir`, creating it the first
/// time the disk is seen. The second value is whether the identity is actually written to
/// the disk: when it is not (a read-only or full filesystem) the returned id is derived
/// from the label and *is* the name-keyed guess this module exists to avoid, so the caller
/// is told and reports it rather than presenting it as stable.
pub fn identify(egroups_dir: &Path, label: &str) -> (String, bool) {
    let top = match egroups_dir.parent() {
        Some(p) => p,
        None => return (format!("unpersisted:{label}"), false),
    };
    let file = top.join(UID_FILE);
    let read = |p: &Path| -> Option<String> {
        let text = std::fs::read_to_string(p).ok()?;
        let text = text.trim().to_string();
        if valid_uid(&text) { Some(text) } else { None }
    };
    if let Some(uid) = read(&file) {
        return (uid, true);
    }
    let candidate = fresh_uid();
    // create_new: two threads discovering the same new disk at once must not each write a
    // different identity. Whoever loses reads the winner's.
    let written = OpenOptions::new().write(true).create_new(true).open(&file).and_then(|mut f| {
        f.write_all(candidate.as_bytes())?;
        f.write_all(b"\n")?;
        f.sync_all()
    });
    match written {
        Ok(()) => (candidate, true),
        Err(_) => match read(&file) {
            Some(uid) => (uid, true),
            None => {
                eprintln!(
                    "sidon: cannot write an identity for the disk at {}; it will be known by \
                     its mount name, which is not stable across reboots",
                    top.display()
                );
                (format!("unpersisted:{label}"), false)
            }
        },
    }
}

// --- Tier detection -------------------------------------------------------------------

/// Whether a block device is solid-state, from its name and the kernel's rotational flag.
///
/// Pure so it can be tested without a sysfs. `nvme*` is believed on the name alone because
/// the kernel does not report NVMe as anything else.
pub fn tier_from_sysfs(device: &str, rotational: Option<&str>) -> Tier {
    if device.starts_with("nvme") {
        return Tier::Nvme;
    }
    match rotational.map(str::trim) {
        Some("0") => Tier::Ssd,
        Some("1") => Tier::Hdd,
        _ => Tier::Unknown,
    }
}

/// The device a mount point is mounted from, out of `/proc/self/mountinfo` text.
///
/// The last line naming `mount` wins, which is the one in force when something has been
/// mounted over another. Octal escapes (`\040` for a space) are undone because a path with
/// a space is written that way and would otherwise never compare equal.
pub fn parse_mountinfo_source(text: &str, mount: &Path) -> Option<String> {
    fn unescape(s: &str) -> String {
        let b = s.as_bytes();
        let mut out = Vec::with_capacity(b.len());
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'\\' && i + 3 < b.len() && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c)) {
                let v = (b[i + 1] - b'0') as u32 * 64 + (b[i + 2] - b'0') as u32 * 8 + (b[i + 3] - b'0') as u32;
                out.push(v as u8);
                i += 4;
            } else {
                out.push(b[i]);
                i += 1;
            }
        }
        String::from_utf8_lossy(&out).to_string()
    }
    let mut found = None;
    for line in text.lines() {
        let (left, right) = match line.split_once(" - ") {
            Some(v) => v,
            None => continue,
        };
        let fields: Vec<&str> = left.split(' ').collect();
        if fields.len() < 5 {
            continue;
        }
        if Path::new(&unescape(fields[4])) != mount {
            continue;
        }
        let mut rest = right.split(' ');
        let _fstype = rest.next();
        if let Some(source) = rest.next() {
            found = Some(unescape(source));
        }
    }
    found
}

/// What the kernel currently says backs `mount`. Informational: it is how an operator can
/// see that a directory called `sdc` is really `/dev/sdb`, and nothing keys off it.
pub fn mount_source(mount: &Path) -> Option<String> {
    let text = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    let canon = std::fs::canonicalize(mount).unwrap_or_else(|_| mount.to_path_buf());
    parse_mountinfo_source(&text, &canon)
}

fn rotational_of(sys_dir: &Path) -> Option<String> {
    for dir in [sys_dir.to_path_buf(), sys_dir.join("..")] {
        if let Ok(v) = std::fs::read_to_string(dir.join("queue/rotational")) {
            return Some(v);
        }
    }
    None
}

fn tier_of_device(name: &str, depth: u32) -> Tier {
    let sys = Path::new("/sys/class/block").join(name);
    if let Some(rot) = rotational_of(&sys) {
        // A device-mapper volume reports whatever the mapper reports, which on a thin
        // pool is not a statement about the disk under it. Look through to what backs it.
        if !name.starts_with("dm-") {
            return tier_from_sysfs(name, Some(&rot));
        }
    }
    if name.starts_with("dm-") && depth > 0 {
        let mut seen: Option<Tier> = None;
        if let Ok(entries) = std::fs::read_dir(sys.join("slaves")) {
            for e in entries.flatten() {
                let t = tier_of_device(&e.file_name().to_string_lossy(), depth - 1);
                seen = match seen {
                    None => Some(t),
                    Some(prev) if prev == t => Some(prev),
                    // Backed by media of different classes: no single honest answer.
                    Some(_) => return Tier::Unknown,
                };
            }
        }
        return seen.unwrap_or(Tier::Unknown);
    }
    tier_from_sysfs(name, None)
}

/// The class of media behind `mount`.
///
/// An operator's `disk.tier` file wins, because it is the only source that can be right on
/// virtual disks, which are routinely reported as rotational whatever they sit on. Then the
/// kernel's own flag. Otherwise `Unknown`, and unknown is a real answer: the policy treats
/// it as "no opinion", not as "slow".
pub fn detect_tier(mount: &Path) -> Tier {
    if let Ok(text) = std::fs::read_to_string(mount.join(TIER_FILE)) {
        let t = Tier::parse(&text);
        if t.is_known() {
            return t;
        }
    }
    let source = match mount_source(mount) {
        Some(s) => s,
        None => return Tier::Unknown,
    };
    let real = std::fs::canonicalize(&source).unwrap_or_else(|_| PathBuf::from(&source));
    match real.file_name().map(|n| n.to_string_lossy().to_string()) {
        Some(name) => tier_of_device(&name, 5),
        None => Tier::Unknown,
    }
}

// --- Choosing a disk for a new group --------------------------------------------------

/// What `pick_disk` needs to know about a disk.
#[derive(Clone, Copy, Debug)]
pub struct Room {
    pub tier: Tier,
    pub total: u64,
    pub avail: u64,
}

/// A disk that has less than this fraction free is not eligible for *preferred* placement.
/// A container asking for SSD must not be able to fill the only SSD to the last block just
/// because it asked first; below the floor its new groups spill to the other disks.
pub const PREFERRED_FLOOR_PERCENT: u64 = 10;

/// The slot a new extent group goes in.
///
/// With no preference this is the old rule, unchanged: most free space wins, first disk on
/// a tie. With one, disks of that class that still have room are considered first, and if
/// there are none the choice is made across every disk -- a container labelled `SSD` on a
/// node with no SSD must still be able to write, and until this reads the label at all it
/// has been exactly that.
pub fn pick_disk(rooms: &[Room], prefer: Option<Tier>) -> usize {
    let most_free = |eligible: &dyn Fn(&Room) -> bool| -> Option<usize> {
        let mut best: Option<(usize, u64)> = None;
        for (i, r) in rooms.iter().enumerate() {
            if !eligible(r) {
                continue;
            }
            if best.map(|(_, f)| r.avail > f).unwrap_or(true) {
                best = Some((i, r.avail));
            }
        }
        best.map(|(i, _)| i)
    };
    if let Some(want) = prefer.filter(|t| t.is_known()) {
        let preferred = most_free(&|r: &Room| {
            r.tier == want
                && r.avail > 0
                && r.avail.saturating_mul(100) > r.total.saturating_mul(PREFERRED_FLOOR_PERCENT)
        });
        if let Some(slot) = preferred {
            return slot;
        }
    }
    // The original rule, including its starting point: slot 0 when nothing reports any
    // free space at all.
    let mut best = 0usize;
    let mut best_free = 0u64;
    for (i, r) in rooms.iter().enumerate() {
        if r.avail > best_free {
            best_free = r.avail;
            best = i;
        }
    }
    best
}

/// The tier a container asks for, if it says. `None` for every way the answer can be
/// absent, which leaves placement exactly as it was.
pub fn container_tier(daruk: &crate::meta::Daruk, container: &str) -> Option<Tier> {
    let rows = daruk
        .query(&format!(
            "SELECT tier FROM hydra.storage_containers WHERE name = {}",
            crate::meta::cql_str(container)
        ))
        .ok()?;
    let tier = Tier::parse(rows.first()?.get("tier")?.as_str()?);
    if tier.is_known() { Some(tier) } else { None }
}

// --- The store's view of its disks ----------------------------------------------------

/// A copy that was verified and published, or the reason there was none.
#[derive(Debug)]
pub struct Moved {
    pub id: String,
    pub from: usize,
    pub to: usize,
    pub bytes: u64,
    pub hash: String,
}

/// A surplus copy: an extent group present on more than one disk, other than the one the
/// store currently resolves it to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stray {
    pub id: String,
    pub slot: usize,
}

/// Whole-file checksum in the exact format `seal_hash` records, plus the length.
fn file_crc(path: &Path) -> Result<(u32, u64)> {
    let mut file = File::open(path)?;
    let mut crc = 0u32;
    let mut len = 0u64;
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        crc = crc32c(crc, &buf[..n]);
        len += n as u64;
    }
    Ok((crc, len))
}

pub fn hash_string(crc: u32) -> String {
    format!("crc32c:{crc:08x}")
}

/// Ask the kernel to drop what it has cached of a file, so that reading it back afterwards
/// reads the disk and not the page cache. Without it "verify the copy" re-reads the very
/// pages that were just written and would pass for a copy that never reached the platter.
fn drop_cache(file: &File) {
    use std::os::unix::io::AsRawFd;
    extern "C" {
        fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
    }
    const POSIX_FADV_DONTNEED: i32 = 4;
    unsafe {
        posix_fadvise(file.as_raw_fd(), 0, 0, POSIX_FADV_DONTNEED);
    }
}

impl EgroupStore {
    /// Prefer disks of this class when placing new groups. See `pick_disk`.
    pub fn preferring(mut self, tier: Option<Tier>) -> Self {
        self.prefer = tier;
        self
    }

    pub fn disks(&self) -> &[Disk] {
        &self.disks
    }

    /// The disk to create a new group on.
    pub(super) fn pick_slot(&self) -> usize {
        let rooms: Vec<Room> = self
            .disks
            .iter()
            .map(|d| {
                let (total, avail) = disk_space(&d.root).unwrap_or((0, 0));
                Room { tier: d.tier, total, avail }
            })
            .collect();
        pick_disk(&rooms, self.prefer)
    }

    /// A disk named by its identity, or failing that by its label.
    pub fn resolve_disk(&self, name: &str) -> Option<usize> {
        self.disks
            .iter()
            .position(|d| d.uid == name)
            .or_else(|| self.disks.iter().position(|d| d.id == name))
    }

    fn file_name(id: &str) -> String {
        format!("{id}.eg")
    }

    /// Every disk that holds a file for this group, regardless of what the index says.
    pub fn copies(&self, id: &str) -> Vec<usize> {
        let name = Self::file_name(id);
        self.disks
            .iter()
            .enumerate()
            .filter(|(_, d)| d.root.join(&name).is_file())
            .map(|(i, _)| i)
            .collect()
    }

    /// Where this group actually is *now*, re-reading the disks rather than trusting the
    /// index, and correcting the index to match. The slot the store resolves reads to.
    pub fn locate(&self, id: &str) -> Option<usize> {
        let copies = self.copies(id);
        let indexed = self.index.lock().ok().and_then(|i| i.get(id).copied());
        let slot = match indexed {
            Some(s) if copies.contains(&s) => Some(s),
            _ => copies.last().copied(),
        };
        if let (Some(s), Ok(mut index)) = (slot, self.index.lock()) {
            index.insert(id.to_string(), s);
        }
        slot
    }

    /// Open a group's file for reading, surviving a stale index.
    ///
    /// Several stores in one process each hold their own index, and Purah's can move a
    /// group without telling the others. A reader whose index still names the old disk
    /// opens the old copy while it exists; once the sweep has removed it, this is what finds
    /// the new one instead of failing a guest read over a bookkeeping lag.
    pub(super) fn open_group(&self, id: &str) -> std::io::Result<(File, PathBuf)> {
        let path = self.path_for(id);
        match File::open(&path) {
            Ok(f) => Ok((f, path)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Ok(mut index) = self.index.lock() {
                    index.remove(id);
                }
                let again = self.path_for(id);
                File::open(&again).map(|f| (f, again)).map_err(|_| e)
            }
            Err(e) => Err(e),
        }
    }

    /// Phase one of a move: copy the group to a temporary name on `to` and prove the copy.
    ///
    /// The temporary name does not end in `.eg`, so no directory scan -- startup, `copies`,
    /// the sweep -- can mistake a half-written file for an extent group. Nothing reads it
    /// and nothing is repointed until it has been fsynced, dropped from cache, read back
    /// from the disk and found to hash the same as the source.
    pub fn stage_copy(&self, id: &str, from: usize, to: usize) -> Result<(PathBuf, u64, u32)> {
        if from == to || from >= self.disks.len() || to >= self.disks.len() {
            return Err(Error::refused(format!("extent group {id}: no move between disk slots {from} and {to}")));
        }
        let name = Self::file_name(id);
        let src_path = self.disks[from].root.join(&name);
        let dst_dir = &self.disks[to].root;
        let temp = dst_dir.join(format!("{id}{MOVING_SUFFIX}"));
        // A temporary left by an earlier attempt was never valid; start over rather than
        // appending to it.
        let _ = std::fs::remove_file(&temp);

        let mut src = File::open(&src_path)
            .map_err(|e| Error::io(format!("extent group {id}: source {} unreadable: {e}", src_path.display())))?;
        let before = src.metadata()?;
        let len = before.len();
        if let Some((_, avail)) = disk_space(dst_dir) {
            // Room for the copy and as much again: a move that leaves the destination with
            // nothing is a move that makes the next seal on it fail.
            if avail < len.saturating_mul(2) {
                return Err(Error::refused(format!(
                    "extent group {id}: destination disk {} has {avail} bytes free for a {len} byte group",
                    self.disks[to].uid
                )));
            }
        }

        let mut dst = OpenOptions::new().write(true).create_new(true).open(&temp)?;
        let mut crc = 0u32;
        let mut copied = 0u64;
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = src.read(&mut buf)?;
            if n == 0 {
                break;
            }
            crc = crc32c(crc, &buf[..n]);
            dst.write_all(&buf[..n])?;
            copied += n as u64;
        }
        dst.sync_all()?;

        // A sealed group cannot change, so a source that did is not a source to trust.
        let after = src.metadata()?;
        if copied != len || after.len() != len || after.modified().ok() != before.modified().ok() {
            let _ = std::fs::remove_file(&temp);
            return Err(Error::corrupt(format!(
                "extent group {id} changed while it was being copied; a sealed group must not"
            )));
        }

        drop_cache(&dst);
        let (back_crc, back_len) = file_crc(&temp)?;
        if back_crc != crc || back_len != len {
            let _ = std::fs::remove_file(&temp);
            return Err(Error::corrupt(format!(
                "extent group {id}: the copy on disk {} reads back as {} ({back_len} bytes), \
                 not the {} ({len} bytes) that was written; the source is untouched",
                self.disks[to].uid,
                hash_string(back_crc),
                hash_string(crc)
            )));
        }
        Ok((temp, len, crc))
    }

    /// Phase two: make the verified copy visible under its real name.
    ///
    /// A rename inside one directory is atomic, so the name either does not exist or names
    /// a complete, verified file -- no reader is ever pointed at a half-written one. The
    /// directory is fsynced so the name survives a power cut. After this there are two
    /// valid copies, and any crash from here on leaves a surplus, never a shortage.
    pub fn stage_publish(&self, id: &str, to: usize, temp: &Path) -> Result<()> {
        let dir = &self.disks[to].root;
        let final_path = dir.join(Self::file_name(id));
        if final_path.exists() {
            // Never replaced: an existing file here is either a surplus copy from an earlier
            // move, which the sweep owns, or something that should be looked at.
            let _ = std::fs::remove_file(temp);
            return Err(Error::refused(format!(
                "extent group {id} is already present on disk {}; it is removed by the sweep, \
                 not overwritten by a move",
                self.disks[to].uid
            )));
        }
        std::fs::rename(temp, &final_path)?;
        File::open(dir).and_then(|d| d.sync_all())?;
        Ok(())
    }

    /// Phase three: point this store's readers at the new copy.
    pub fn stage_switch(&self, id: &str, to: usize) {
        if let Ok(mut index) = self.index.lock() {
            index.insert(id.to_string(), to);
        }
    }

    /// Move a sealed extent group to another disk of this node.
    ///
    /// Copy, verify, publish, switch -- and *not* delete. See the module header for why the
    /// source is left in place for the sweep.
    ///
    /// `expected_hash` is the seal hash Hydra recorded when the group was known good. The
    /// source is checked against it before anything is written to the destination: a group
    /// that has already gone bad is not copied, because a copy would be a second damaged
    /// file with a clean-looking checksum of its own, and the repair path that would have
    /// noticed the first one compares against the recorded hash.
    pub fn move_group(&self, id: &str, to: usize, expected_hash: Option<&str>) -> Result<Moved> {
        let from = self
            .locate(id)
            .ok_or_else(|| Error::io(format!("extent group {id} is on none of this node's disks")))?;
        if from == to {
            return Err(Error::refused(format!("extent group {id} is already on that disk")));
        }
        if self.copies(id).len() > 1 {
            return Err(Error::refused(format!(
                "extent group {id} already has a surplus copy awaiting removal; it is moved after the sweep clears it"
            )));
        }
        if let Some(want) = expected_hash.filter(|h| !h.is_empty()) {
            let (crc, _) = file_crc(&self.disks[from].root.join(Self::file_name(id)))?;
            if hash_string(crc) != want {
                return Err(Error::corrupt(format!(
                    "extent group {id} hashes {}, sealed as {want}; refusing to copy damage to another disk",
                    hash_string(crc)
                )));
            }
        }
        let (temp, len, crc) = self.stage_copy(id, from, to)?;
        self.stage_publish(id, to, &temp)?;
        self.stage_switch(id, to);
        Ok(Moved { id: id.to_string(), from, to, bytes: len, hash: hash_string(crc) })
    }

    /// Groups present on more than one disk, other than on the disk reads resolve to.
    pub fn strays(&self) -> Vec<Stray> {
        let mut seen: HashMap<String, Vec<usize>> = HashMap::new();
        for (slot, disk) in self.disks.iter().enumerate() {
            if let Ok(entries) = std::fs::read_dir(&disk.root) {
                for e in entries.flatten() {
                    let name = e.file_name().to_string_lossy().to_string();
                    if let Some(id) = name.strip_suffix(".eg") {
                        seen.entry(id.to_string()).or_default().push(slot);
                    }
                }
            }
        }
        let mut out = Vec::new();
        let mut ids: Vec<_> = seen.into_iter().filter(|(_, v)| v.len() > 1).collect();
        ids.sort();
        for (id, slots) in ids {
            let keep = self.locate(&id).unwrap_or(slots[0]);
            for slot in slots {
                if slot != keep {
                    out.push(Stray { id: id.clone(), slot });
                }
            }
        }
        out
    }

    /// Remove one surplus copy, after proving it is surplus.
    ///
    /// Three refusals, each a way this could turn into data loss: fewer than two copies on
    /// disk (it would remove the only one), the surviving copy not being the one the store
    /// resolves reads to, and the two not being byte-identical -- in which case nobody
    /// knows which is right and neither is removed.
    pub fn remove_stray(&self, stray: &Stray) -> Result<u64> {
        let name = Self::file_name(&stray.id);
        let copies = self.copies(&stray.id);
        if copies.len() < 2 || !copies.contains(&stray.slot) {
            return Err(Error::refused(format!(
                "extent group {} is not held twice; refusing to remove a copy that may be the only one",
                stray.id
            )));
        }
        let keep = self.locate(&stray.id).filter(|k| *k != stray.slot).ok_or_else(|| {
            Error::refused(format!("extent group {}: the copy to keep is the one named for removal", stray.id))
        })?;
        let (kept_crc, kept_len) = file_crc(&self.disks[keep].root.join(&name))?;
        let path = self.disks[stray.slot].root.join(&name);
        let (crc, len) = file_crc(&path)?;
        if crc != kept_crc || len != kept_len {
            return Err(Error::corrupt(format!(
                "extent group {} differs between disk {} ({}) and disk {} ({}); neither copy removed",
                stray.id,
                self.disks[keep].uid,
                hash_string(kept_crc),
                self.disks[stray.slot].uid,
                hash_string(crc)
            )));
        }
        std::fs::remove_file(&path)?;
        Ok(len)
    }

    /// Remove a group everywhere it exists on this node. For reclamation, where the group is
    /// dead on the cluster and every local copy -- including a surplus one left by a move --
    /// has to go with it.
    pub fn remove_all(&self, id: &str) -> Result<()> {
        let name = Self::file_name(id);
        for disk in &self.disks {
            match std::fs::remove_file(disk.root.join(&name)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(Error::io(format!("removing {}: {e}", disk.root.join(&name).display()))),
            }
            let _ = std::fs::remove_file(disk.root.join(format!("{id}{MOVING_SUFFIX}")));
        }
        if let Ok(mut index) = self.index.lock() {
            index.remove(id);
        }
        Ok(())
    }

    /// Temporary copies older than `older_than`, with the disk they are on.
    ///
    /// A live move is seconds old at most; one that is old was interrupted. The caller must
    /// be the only thing that starts moves (Purah, under its own lock) for age to be a safe
    /// test, which is why this is not run from `open`: every vdisk opens a store of its own.
    pub fn stale_temporaries(&self, older_than: std::time::Duration) -> Vec<(usize, PathBuf)> {
        let mut out = Vec::new();
        for (slot, disk) in self.disks.iter().enumerate() {
            if let Ok(entries) = std::fs::read_dir(&disk.root) {
                for e in entries.flatten() {
                    let name = e.file_name().to_string_lossy().to_string();
                    if !name.ends_with(MOVING_SUFFIX) {
                        continue;
                    }
                    let age = e
                        .metadata()
                        .and_then(|m| m.modified())
                        .ok()
                        .and_then(|t| SystemTime::now().duration_since(t).ok());
                    if age.map(|a| a >= older_than).unwrap_or(false) {
                        out.push((slot, e.path()));
                    }
                }
            }
        }
        out
    }

    /// Which disk holds which extent groups, from the disks themselves.
    ///
    /// Read from the directories and not from the index: this is what an operator reaches
    /// for when something looks wrong, and an answer taken from the same bookkeeping that
    /// might be wrong would be no answer. `limit` bounds the ids listed per disk; the counts
    /// are always exact.
    pub fn placement_report(&self, limit: usize) -> Value {
        let mut disks = Vec::new();
        for (slot, disk) in self.disks.iter().enumerate() {
            let mut groups: Vec<(String, u64)> = Vec::new();
            let mut moving = 0usize;
            if let Ok(entries) = std::fs::read_dir(&disk.root) {
                for e in entries.flatten() {
                    let name = e.file_name().to_string_lossy().to_string();
                    if let Some(id) = name.strip_suffix(".eg") {
                        groups.push((id.to_string(), e.metadata().map(|m| m.len()).unwrap_or(0)));
                    } else if name.ends_with(MOVING_SUFFIX) {
                        moving += 1;
                    }
                }
            }
            groups.sort();
            let bytes: u64 = groups.iter().map(|g| g.1).sum();
            let count = groups.len();
            let space = disk_space(&disk.root);
            disks.push(json!({
                "slot": slot,
                "uid": disk.uid,
                "uid_persisted": disk.uid_persisted,
                "label": disk.id,
                "path": disk.root.to_string_lossy(),
                "device": mount_source(disk.root.parent().unwrap_or(&disk.root)),
                "tier": disk.tier.name(),
                "total_bytes": space.map(|s| json!(s.0)).unwrap_or(Value::Null),
                "available_bytes": space.map(|s| json!(s.1)).unwrap_or(Value::Null),
                "egroup_count": count,
                "egroup_bytes": bytes,
                "in_flight_copies": moving,
                "groups": groups.iter().take(limit).map(|(id, size)| json!({"egroup_id": id, "size": size})).collect::<Vec<_>>(),
                "groups_truncated": count > limit,
            }));
        }
        let strays: Vec<Value> = self
            .strays()
            .into_iter()
            .map(|s| json!({"egroup_id": s.id, "disk": self.disks[s.slot].uid, "label": self.disks[s.slot].id}))
            .collect();
        json!({
            "disks": disks,
            "surplus_copies": strays,
            "surplus_count": strays.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extent::{vdisk_hash, Disk, EgroupStore};

    fn tmpdir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("sidon-pl-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn disk_at(parent: &Path, label: &str, tier: Tier) -> Disk {
        let root = parent.join("disks").join(label).join("egroups");
        std::fs::create_dir_all(&root).unwrap();
        let (uid, persisted) = identify(&root, label);
        Disk { id: label.to_string(), root, uid, uid_persisted: persisted, tier }
    }

    /// A sealed group of `n` extents on the given disk, as the drain would have left it.
    fn seal_one(store: &EgroupStore, id: &str) -> (u32, u32, u64) {
        let vh = vdisk_hash("vd-t");
        let mut eg = store.create(id).unwrap();
        let (off, stored, _) = store.append_framed(&mut eg, &vec![0x5Au8; 8192], vh, 7, false).unwrap();
        store.sync(&mut eg).unwrap();
        (off, stored, vh)
    }

    fn two_disks(name: &str) -> (PathBuf, EgroupStore) {
        let dir = tmpdir(name);
        let disks = vec![disk_at(&dir, "d0", Tier::Unknown), disk_at(&dir, "d1", Tier::Unknown)];
        (dir.clone(), EgroupStore::open(disks, 1 << 20).unwrap())
    }

    // --- identity ---

    /// A disk keeps its identity when the directory it is mounted at changes.
    ///
    /// The kernel names disks in probe order, which is not stable: one node's `disks/sdc`
    /// is really `/dev/sdb`. Anything keyed by that name is keyed by a guess, so the
    /// identity has to be something the disk carries with it.
    #[test]
    fn a_disk_keeps_its_identity_when_its_mount_name_changes() {
        let dir = tmpdir("uid-rename");
        let first = dir.join("disks").join("sdc");
        std::fs::create_dir_all(first.join("egroups")).unwrap();
        let (uid, persisted) = identify(&first.join("egroups"), "sdc");
        assert!(persisted);

        // The same filesystem, mounted somewhere else after a reboot reordered the probe.
        let second = dir.join("disks").join("sdb");
        std::fs::rename(&first, &second).unwrap();
        let (again, _) = identify(&second.join("egroups"), "sdb");
        assert_eq!(uid, again, "re-mounting a disk under another name changed who it is");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn two_disks_never_share_an_identity() {
        let dir = tmpdir("uid-distinct");
        let a = disk_at(&dir, "d0", Tier::Unknown);
        let b = disk_at(&dir, "d1", Tier::Unknown);
        assert_ne!(a.uid, b.uid);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_disk_that_cannot_record_its_identity_says_so() {
        // Reporting the label as though it were stable would be the name-keyed guess this
        // exists to avoid, so the caller is told it is not persisted.
        let dir = tmpdir("uid-readonly");
        std::fs::create_dir_all(&dir).unwrap();
        // A directory standing where the file must go: create_new fails and the read does too.
        std::fs::create_dir_all(dir.join(UID_FILE)).unwrap();
        let root = dir.join("egroups");
        std::fs::create_dir_all(&root).unwrap();
        let (uid, persisted) = identify(&root, "sdz");
        assert!(!persisted);
        assert!(uid.starts_with("unpersisted:"), "{uid}");
        std::fs::remove_dir_all(&dir).ok();
    }

    // --- tier ---

    #[test]
    fn the_tier_a_container_names_is_read_in_any_case() {
        assert_eq!(Tier::parse("SSD"), Tier::Ssd);
        assert_eq!(Tier::parse("nvme"), Tier::Nvme);
        assert_eq!(Tier::parse(" Hdd\n"), Tier::Hdd);
        assert_eq!(Tier::parse("tape"), Tier::Unknown);
    }

    #[test]
    fn nvme_is_believed_by_name_and_everything_else_by_the_rotational_flag() {
        assert_eq!(tier_from_sysfs("nvme0n1", Some("1")), Tier::Nvme);
        assert_eq!(tier_from_sysfs("sdb", Some("0\n")), Tier::Ssd);
        assert_eq!(tier_from_sysfs("sdb", Some("1")), Tier::Hdd);
        assert_eq!(tier_from_sysfs("sdb", None), Tier::Unknown,
                   "no flag is not the same as a rotational disk");
    }

    #[test]
    fn the_mount_source_is_read_from_the_last_line_naming_the_mount() {
        let text = "\
36 35 8:16 / /var/lib/hci/sidon/disks/sdc rw,noatime - xfs /dev/sdb rw\n\
40 35 8:32 / /var/lib/hci/sidon rw - xfs /dev/mapper/vg_aether-sidon rw\n";
        assert_eq!(
            parse_mountinfo_source(text, Path::new("/var/lib/hci/sidon/disks/sdc")),
            Some("/dev/sdb".to_string()),
            "the directory called sdc is mounted from /dev/sdb on this node"
        );
        assert_eq!(parse_mountinfo_source(text, Path::new("/nowhere")), None);
    }

    #[test]
    fn an_operator_tier_file_wins_over_detection() {
        let dir = tmpdir("tier-file");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(TIER_FILE), "ssd\n").unwrap();
        assert_eq!(detect_tier(&dir), Tier::Ssd);
        std::fs::remove_dir_all(&dir).ok();
    }

    // --- choosing a disk ---

    fn room(tier: Tier, total: u64, avail: u64) -> Room {
        Room { tier, total, avail }
    }

    #[test]
    fn with_no_preference_the_emptiest_disk_wins() {
        let rooms = [room(Tier::Unknown, 100, 30), room(Tier::Unknown, 100, 70)];
        assert_eq!(pick_disk(&rooms, None), 1);
    }

    /// A container asking for a class that exists on the node lands on it, even when a disk
    /// of another class has more room.
    #[test]
    fn a_container_that_asks_for_ssd_is_placed_on_one() {
        let rooms = [room(Tier::Hdd, 1000, 900), room(Tier::Ssd, 1000, 400)];
        assert_eq!(pick_disk(&rooms, Some(Tier::Ssd)), 1);
    }

    /// A label on a container must never be able to stop it writing.
    ///
    /// The tier has been a label nothing read; on a node with no SSD the only acceptable
    /// reading of "SSD" is "somewhere", or reading it at all would turn a cosmetic setting
    /// into an outage on every test node, whose disks do not report a class.
    #[test]
    fn a_preference_nothing_can_satisfy_is_ignored() {
        let rooms = [room(Tier::Unknown, 100, 30), room(Tier::Unknown, 100, 70)];
        assert_eq!(pick_disk(&rooms, Some(Tier::Ssd)), 1);
    }

    /// A preferred disk that is nearly full stops being preferred.
    ///
    /// Otherwise the first container to ask for SSD fills the only one to the last block
    /// and every later write on the node lands on, or fails against, the same disk.
    #[test]
    fn a_nearly_full_preferred_disk_spills_to_the_others() {
        let rooms = [room(Tier::Hdd, 1000, 900), room(Tier::Ssd, 1000, 50)];
        assert_eq!(pick_disk(&rooms, Some(Tier::Ssd)), 0);
    }

    // --- moving ---

    /// A moved group reads back the same bytes from its new disk, and exists on both until
    /// the sweep removes the old copy.
    #[test]
    fn a_moved_group_reads_identically_from_the_other_disk() {
        let (dir, store) = two_disks("move-roundtrip");
        let (off, stored, vh) = seal_one(&store, "eg-a");
        let from = store.locate("eg-a").unwrap();
        let to = 1 - from;
        let before = store.read_extent("eg-a", off, stored, vh, 7).unwrap();
        let hash = store.seal_hash("eg-a").unwrap();

        let moved = store.move_group("eg-a", to, Some(&hash)).unwrap();
        assert_eq!((moved.from, moved.to), (from, to));
        assert_eq!(store.locate("eg-a"), Some(to));
        assert_eq!(store.read_extent("eg-a", off, stored, vh, 7).unwrap(), before,
                   "the extent differs on the disk it was moved to");
        assert_eq!(store.seal_hash("eg-a").unwrap(), hash, "a move changed the group's seal hash");
        assert_eq!(store.copies("eg-a").len(), 2,
                   "the move deleted the old copy, which is the sweep's job and not the move's");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A group already damaged is not carried to another disk.
    #[test]
    fn a_damaged_source_is_not_copied() {
        let (dir, store) = two_disks("move-damaged");
        seal_one(&store, "eg-bad");
        let from = store.locate("eg-bad").unwrap();
        let to = 1 - from;
        let wrong = "crc32c:deadbeef";
        match store.move_group("eg-bad", to, Some(wrong)) {
            Err(Error::Corrupt(_)) => {}
            other => panic!("expected a corruption refusal, got {other:?}"),
        }
        assert_eq!(store.copies("eg-bad"), vec![from], "a copy of a damaged group was made");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Crash after the temporary copy is written and before it is published: the old copy is
    /// the only valid one, the temporary is invisible to every scan, and the group still
    /// reads.
    #[test]
    fn a_crash_before_publication_leaves_the_old_copy_and_no_visible_new_one() {
        let (dir, store) = two_disks("crash-before-publish");
        let (off, stored, vh) = seal_one(&store, "eg-c1");
        let from = store.locate("eg-c1").unwrap();
        let to = 1 - from;
        let (temp, _, _) = store.stage_copy("eg-c1", from, to).unwrap();
        assert!(temp.exists());
        drop(store); // the process dies here

        let roots: Vec<Disk> = ["d0", "d1"].iter().map(|l| {
            let root = dir.join("disks").join(l).join("egroups");
            let (uid, p) = identify(&root, l);
            Disk { id: l.to_string(), root, uid, uid_persisted: p, tier: Tier::Unknown }
        }).collect();
        let reopened = EgroupStore::open(roots, 1 << 20).unwrap();
        assert_eq!(reopened.copies("eg-c1"), vec![from],
                   "a half-finished copy was visible as an extent group");
        assert_eq!(reopened.read_extent("eg-c1", off, stored, vh, 7).unwrap().len(), 8192);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Crash after publication and before anything is removed: two valid copies, either of
    /// which serves the read, and the surplus is identified rather than lost track of.
    #[test]
    fn a_crash_after_publication_leaves_two_good_copies_not_none() {
        let (dir, store) = two_disks("crash-after-publish");
        let (off, stored, vh) = seal_one(&store, "eg-c2");
        let from = store.locate("eg-c2").unwrap();
        let to = 1 - from;
        let (temp, _, _) = store.stage_copy("eg-c2", from, to).unwrap();
        store.stage_publish("eg-c2", to, &temp).unwrap();
        drop(store);

        let roots: Vec<Disk> = ["d0", "d1"].iter().map(|l| {
            let root = dir.join("disks").join(l).join("egroups");
            let (uid, p) = identify(&root, l);
            Disk { id: l.to_string(), root, uid, uid_persisted: p, tier: Tier::Unknown }
        }).collect();
        let reopened = EgroupStore::open(roots, 1 << 20).unwrap();
        assert_eq!(reopened.copies("eg-c2").len(), 2);
        assert_eq!(reopened.read_extent("eg-c2", off, stored, vh, 7).unwrap().len(), 8192,
                   "neither copy served the read after a crash between publish and switch");
        assert_eq!(reopened.strays().len(), 1, "the surplus copy was not identified");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A copy that did not survive the trip is refused and leaves nothing behind.
    #[test]
    fn a_copy_that_cannot_be_written_publishes_nothing() {
        let (dir, store) = two_disks("copy-verify");
        seal_one(&store, "eg-v");
        let from = store.locate("eg-v").unwrap();
        let to = 1 - from;
        // Make the destination unusable for the temporary: a directory standing in its place
        // cannot be removed as a file nor created over, so the copy cannot be written at all.
        let temp_path = store.disks()[to].root.join(format!("eg-v{MOVING_SUFFIX}"));
        std::fs::create_dir_all(temp_path.join("blocker")).unwrap();
        assert!(store.stage_copy("eg-v", from, to).is_err());
        assert_eq!(store.copies("eg-v"), vec![from], "a failed copy produced a visible group");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A move never overwrites a file that is already at the destination.
    #[test]
    fn a_move_does_not_replace_a_copy_already_there() {
        let (dir, store) = two_disks("move-no-clobber");
        seal_one(&store, "eg-n");
        let from = store.locate("eg-n").unwrap();
        let to = 1 - from;
        std::fs::write(store.disks()[to].root.join("eg-n.eg"), b"something else").unwrap();
        assert!(store.move_group("eg-n", to, None).is_err());
        assert_eq!(std::fs::read(store.disks()[to].root.join("eg-n.eg")).unwrap(), b"something else");
        std::fs::remove_dir_all(&dir).ok();
    }

    // --- the surplus copy ---

    /// A store whose index was not told about a move still reads, both while the old copy
    /// exists and after it has been removed.
    ///
    /// Every attached vdisk holds its own store. Purah moves a group in its store; the
    /// vdisk's index still names the old disk.
    #[test]
    fn a_reader_with_a_stale_index_survives_the_old_copy_going_away() {
        let (dir, mover) = two_disks("stale-reader");
        let (off, stored, vh) = seal_one(&mover, "eg-s");
        let roots: Vec<Disk> = ["d0", "d1"].iter().map(|l| {
            let root = dir.join("disks").join(l).join("egroups");
            let (uid, p) = identify(&root, l);
            Disk { id: l.to_string(), root, uid, uid_persisted: p, tier: Tier::Unknown }
        }).collect();
        let reader = EgroupStore::open(roots, 1 << 20).unwrap();
        assert_eq!(reader.read_extent("eg-s", off, stored, vh, 7).unwrap().len(), 8192);

        let from = mover.locate("eg-s").unwrap();
        mover.move_group("eg-s", 1 - from, None).unwrap();
        // The reader's index still says `from`, which still exists.
        assert_eq!(reader.read_extent("eg-s", off, stored, vh, 7).unwrap().len(), 8192);
        // Now the sweep removes the surplus.
        let stray = mover.strays().pop().unwrap();
        mover.remove_stray(&stray).unwrap();
        assert_eq!(reader.read_extent("eg-s", off, stored, vh, 7).unwrap().len(), 8192,
                   "a reader that resolved the old path failed once the old copy was removed");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The last copy of a group can never be removed as though it were surplus.
    #[test]
    fn the_only_copy_is_never_removed() {
        let (dir, store) = two_disks("only-copy");
        seal_one(&store, "eg-o");
        let slot = store.locate("eg-o").unwrap();
        let forged = Stray { id: "eg-o".to_string(), slot };
        assert!(store.remove_stray(&forged).is_err());
        assert_eq!(store.copies("eg-o"), vec![slot]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Two copies that differ are both kept: nobody knows which one is right.
    #[test]
    fn copies_that_differ_are_not_resolved_by_deleting_one() {
        let (dir, store) = two_disks("diverged");
        seal_one(&store, "eg-d");
        let from = store.locate("eg-d").unwrap();
        let to = 1 - from;
        std::fs::write(store.disks()[to].root.join("eg-d.eg"), b"a different history").unwrap();
        let strays = store.strays();
        assert_eq!(strays.len(), 1);
        match store.remove_stray(&strays[0]) {
            Err(Error::Corrupt(_)) => {}
            other => panic!("expected a refusal to pick a side, got {other:?}"),
        }
        assert_eq!(store.copies("eg-d").len(), 2, "a copy was removed though the two disagree");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reclaiming_a_group_removes_every_copy_of_it() {
        let (dir, store) = two_disks("remove-all");
        seal_one(&store, "eg-r");
        let from = store.locate("eg-r").unwrap();
        store.move_group("eg-r", 1 - from, None).unwrap();
        assert_eq!(store.copies("eg-r").len(), 2);
        store.remove_all("eg-r").unwrap();
        assert!(store.copies("eg-r").is_empty(), "a surplus copy outlived its group");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The report says which disk holds which group, read from the disks.
    #[test]
    fn placement_is_reported_from_the_disks_and_not_from_the_index() {
        let (dir, store) = two_disks("report");
        seal_one(&store, "eg-p");
        let at = store.locate("eg-p").unwrap();
        // Move the file behind the index's back.
        let other = 1 - at;
        std::fs::rename(
            store.disks()[at].root.join("eg-p.eg"),
            store.disks()[other].root.join("eg-p.eg"),
        ).unwrap();
        let report = store.placement_report(10);
        let holder = report["disks"][other]["groups"][0]["egroup_id"].as_str();
        assert_eq!(holder, Some("eg-p"), "the report followed the index instead of the disk");
        assert_eq!(report["disks"][at]["egroup_count"].as_u64(), Some(0));
        assert!(report["disks"][other]["uid"].as_str().is_some());
        std::fs::remove_dir_all(&dir).ok();
    }
}
