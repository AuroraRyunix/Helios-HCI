//! Which filesystems are sidon's, where they are mounted, and the refusal to use a path
//! whose filesystem is not there.
//!
//! # What went wrong, and what this changes
//!
//! The extent store used to be mounted by `/etc/fstab`, and one of its disks was mounted
//! *inside* another: `disks/sdc` under the volume at `/var/lib/hci/sidon`. Two outages came
//! of that in one evening.
//!
//! A disk that was late failed `local-fs.target` and the node booted to a console with no
//! network. `nofail` fixed the boot and made the failure silent: the parent volume did not
//! mount, nothing complained, and sidon wrote extent groups to the root filesystem at the
//! same path. Then, recovering, the parent was mounted over an already-mounted child, which
//! hid it -- `findmnt` still listed the child, the path no longer resolved to it, and
//! sixteen extent groups were unreachable. Only a `stat` of the path told the truth.
//!
//! So three rules, each of which would have prevented one of those:
//!
//! 1. **Nothing sidon-related is in `/etc/fstab`.** A missing disk cannot fail the boot
//!    because the boot does not know about it, so `nofail` is not needed and neither is the
//!    detector it forces. Mounting what sidon owns is sidon's job, and sidon is the thing
//!    that knows what an absent disk means.
//! 2. **No mount is nested inside another.** Every disk is a sibling under `<root>/disks/`,
//!    and `<root>` is a plain directory on the root filesystem, so no parent can shadow a
//!    child. (`<root>/nbd` stays where it is: libvirt domain XML names those sockets.)
//! 3. **A path backed by a disk is used only while that disk is provably there.** Proven by
//!    `stat`, never by a mount table: the directory's device number must differ from its
//!    parent's, must be the device number of the block device carrying the expected
//!    filesystem UUID, and the directory must hold the `disk.uid` sentinel. The sentinel
//!    alone proves nothing -- one left on the root filesystem during an unmounted period is
//!    exactly what a mount later covers -- and a mount alone proves nothing about *which*
//!    filesystem it is. All three together are what "present" means.
//!
//! # Where the knowledge of "which devices are mine" lives
//!
//! In `/etc/hci/sidon-disks`, one line per filesystem: `<filesystem-uuid> <journal|extent>`.
//! Written by the claim step (and staged on an existing node by the rollout), read by this
//! module, by the Mimir survey and by nothing else. It is keyed by *filesystem UUID*,
//! which exists before anything is mounted and survives a renumbered kernel device, and it
//! is the directory name too, so a path cannot say `sdc` about a disk the kernel calls
//! `sdb`. It lives in `/etc/hci` rather than in a place sidon writes because it is
//! configuration: an operator adding a disk by hand edits it, and a daemon that rewrote its
//! own list of what it expects would be able to forget a disk that went missing.
//!
//! Sidon does the mounting itself, at startup, rather than a unit `ExecStartPre`: the unit
//! file of an existing node is written only at provisioning and a rollout does not touch
//! it, so a step that lived in the unit would never reach a node that already exists,
//! while a step in the binary arrives with the binary. Doing it here also keeps the
//! knowledge of what is expected and the act of mounting it in one place, which is what
//! lets a missing disk be reported by the storage layer instead of inferred from outside.
//!
//! # The two roles
//!
//! `journal` is the volume that holds what must not be lost and is not extent data: the
//! write-ahead journals, and the replica state a node keeps for vdisks it does not own.
//! Without it sidon cannot say what it has acknowledged, so **sidon refuses to start**
//! rather than journal onto the root filesystem; `Restart=always` retries every few
//! seconds, and each retry attempts the mount again, so a late disk recovers by itself.
//! `extent` disks are capacity. One missing is *that disk*: sidon starts, serves what the
//! others hold, refuses to place anything on the absent one, and says so in `capacity`.
//! The journal volume also holds extent groups of its own, like any disk.
//!
//! # Moving an existing node
//!
//! A node built before this has the journal volume mounted *at* `<root>` with a disk
//! nested inside it. That layout is moved, once, **only while sidon is not running**: at
//! startup, before anything is opened, or by `sidon mounts apply`, which refuses if the
//! control socket answers. Every mount under `<root>` is unmounted deepest first, never
//! lazily, and the sidon lines come out of `/etc/fstab`. If anything is still using a
//! mount the unmount fails and the move stops, because a half-moved node is worse than a
//! node that has not moved. And it refuses before it unmounts anything if a filesystem
//! mounted under `<root>` is not in the manifest, since a disk the manifest does not name
//! would be orphaned. No data is copied: the volume's contents are simply reached at a
//! different path.

use std::ffi::{c_char, c_int, c_ulong, c_void, CString};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::err::{Error, Result};

pub const MANIFEST_DEFAULT: &str = "/etc/hci/sidon-disks";
pub const FSTAB_DEFAULT: &str = "/etc/fstab";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Journal,
    Extent,
}

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Role::Journal => "journal",
            Role::Extent => "extent",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub uuid: String,
    pub role: Role,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Manifest {
    pub entries: Vec<Entry>,
}

impl Manifest {
    pub fn journal(&self) -> Option<&Entry> {
        self.entries.iter().find(|e| e.role == Role::Journal)
    }

    pub fn has(&self, uuid: &str) -> bool {
        self.entries.iter().any(|e| e.uuid == uuid)
    }
}

/// A filesystem UUID as it may appear in the manifest. Strict because it becomes a
/// directory name under `<root>/disks/`: no separator, no dot, nothing that can climb out.
fn valid_uuid(s: &str) -> bool {
    !s.is_empty() && s.len() <= 64 && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

pub fn parse_manifest(text: &str) -> std::result::Result<Manifest, String> {
    let mut entries: Vec<Entry> = Vec::new();
    for (n, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            return Err(format!("line {}: expected `<filesystem-uuid> <journal|extent>`", n + 1));
        }
        if !valid_uuid(fields[0]) {
            return Err(format!("line {}: {:?} is not a filesystem uuid", n + 1, fields[0]));
        }
        let role = match fields[1] {
            "journal" => Role::Journal,
            "extent" => Role::Extent,
            other => return Err(format!("line {}: unknown role {other:?}", n + 1)),
        };
        if entries.iter().any(|e| e.uuid == fields[0]) {
            return Err(format!("line {}: {} is listed twice", n + 1, fields[0]));
        }
        entries.push(Entry { uuid: fields[0].to_string(), role });
    }
    if entries.iter().filter(|e| e.role == Role::Journal).count() > 1 {
        return Err("more than one journal volume is listed".to_string());
    }
    // Journal first, so it is mounted first and reported first; the rest keep file order.
    entries.sort_by_key(|e| if e.role == Role::Journal { 0 } else { 1 });
    Ok(Manifest { entries })
}

pub fn manifest_path() -> PathBuf {
    std::env::var("SIDON_DISKS_MANIFEST")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(MANIFEST_DEFAULT))
}

fn fstab_path() -> PathBuf {
    std::env::var("SIDON_FSTAB").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from(FSTAB_DEFAULT))
}

/// The manifest, `Ok(None)` when there is none (an unmanaged node: a development host, or
/// one the rollout has not reached), and an error when there is one that cannot be read.
/// Not an "unmanaged" fallback: a node that was told what its disks are and cannot read it
/// must not carry on as though it had been told nothing.
pub fn load_manifest() -> Result<Option<Manifest>> {
    let path = manifest_path();
    match std::fs::read_to_string(&path) {
        Ok(text) => parse_manifest(&text)
            .map(Some)
            .map_err(|e| Error::refused(format!("{} is not valid: {e}", path.display()))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::io(format!("cannot read {}: {e}", path.display()))),
    }
}

/// Where one disk is mounted. A sibling of every other, under a directory that is not
/// itself a mount.
pub fn mount_dir(root: &Path, uuid: &str) -> PathBuf {
    root.join("disks").join(uuid)
}

// --- The host, so the rules can be tested without mounting anything -------------------

/// Everything this module does to the machine. Real in production; a model of a mount
/// table in the tests, which is the only way to exercise a shadowed mount without root.
pub trait Host: Send + Sync {
    /// `st_dev` of a path, following symlinks. `None` when it does not exist.
    fn dev_of(&self, path: &Path) -> Option<u64>;
    /// The device number of the block device carrying this filesystem UUID.
    fn rdev_of_uuid(&self, uuid: &str) -> Option<u64>;
    /// The UUID of the block device with this device number.
    fn uuid_of_dev(&self, dev: u64) -> Option<String>;
    /// The `disk.uid` sentinel at the top of the filesystem `dir` resolves to.
    fn read_uid(&self, dir: &Path) -> Option<String>;
    /// Write the sentinel on a filesystem that is proven to be the right one.
    fn claim_uid(&self, dir: &Path) -> Option<String>;
    /// Every mount at or under `root`, deepest first, each with the device number the
    /// kernel lists for it. From the mount table, not from `stat`, because the point of
    /// asking is to find what to unmount, and a mount that something else covers is exactly
    /// the one a `stat` of its path cannot see.
    fn mounts_under(&self, root: &Path) -> Vec<(PathBuf, u64)>;
    fn mkdir(&self, dir: &Path) -> std::io::Result<()>;
    fn mount(&self, uuid: &str, target: &Path) -> std::result::Result<(), String>;
    fn umount(&self, target: &Path) -> std::result::Result<(), String>;
    fn reload_units(&self);
}

/// Whether `dir` is the root of its own filesystem, by comparing its device with its
/// parent's. This is what `mountpoint(1)` and `os.path.ismount` do, and it is the test the
/// shadowed-child outage needed: a child covered by a later mount of its parent resolves,
/// by path, to a directory on the parent's filesystem, and so reads as *not mounted*.
pub fn is_mount(host: &dyn Host, dir: &Path) -> bool {
    let here = match host.dev_of(dir) {
        Some(d) => d,
        None => return false,
    };
    match dir.parent().and_then(|p| host.dev_of(p)) {
        Some(parent) => here != parent,
        None => false,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Absence {
    /// A plain directory on the parent's filesystem: never mounted, unmounted since, or
    /// covered by a later mount of something above it.
    NotMounted,
    /// Mounted, and not the filesystem the manifest names for this path.
    WrongDevice,
    /// The filesystem with this UUID is not attached to the machine at all.
    NoDevice,
    /// The right filesystem is mounted but carries no `disk.uid`.
    NoSentinel,
    /// The mount was attempted and the kernel refused it.
    MountFailed(String),
}

impl std::fmt::Display for Absence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Absence::NotMounted => write!(f, "not mounted (a plain directory on the root filesystem)"),
            Absence::WrongDevice => write!(f, "something else is mounted there"),
            Absence::NoDevice => write!(f, "its device is not attached"),
            Absence::NoSentinel => write!(f, "mounted but carries no disk.uid"),
            Absence::MountFailed(m) => write!(f, "mount failed: {m}"),
        }
    }
}

/// Is the filesystem `uuid` the one mounted at `dir`? The disk's sentinel when it is.
///
/// Three independent proofs, all by `stat`. See the module comment for why none is enough
/// alone, and in particular why this never consults the mount table.
pub fn probe(host: &dyn Host, dir: &Path, uuid: &str) -> std::result::Result<String, Absence> {
    if !is_mount(host, dir) {
        // Said plainly: "its device is not attached" and "attached and not mounted" want
        // different responses from whoever reads them.
        return Err(if host.rdev_of_uuid(uuid).is_none() { Absence::NoDevice } else { Absence::NotMounted });
    }
    let here = host.dev_of(dir).ok_or(Absence::NotMounted)?;
    match host.rdev_of_uuid(uuid) {
        None => return Err(Absence::NoDevice),
        Some(want) if want != here => return Err(Absence::WrongDevice),
        Some(_) => {}
    }
    host.read_uid(dir).ok_or(Absence::NoSentinel)
}

/// What a disk's mount, tied to the filesystem it must be. Held by an extent store's
/// `Disk` so that "is it still there" can be asked again at the moment of a write, not
/// only once at startup: a disk can leave while sidon runs.
#[derive(Clone)]
pub struct MountGuard {
    pub dir: PathBuf,
    pub uuid: String,
    host: Arc<dyn Host>,
}

impl MountGuard {
    pub fn new(host: Arc<dyn Host>, dir: PathBuf, uuid: String) -> MountGuard {
        MountGuard { dir, uuid, host }
    }

    pub fn present(&self) -> bool {
        probe(self.host.as_ref(), &self.dir, &self.uuid).is_ok()
    }
}

pub fn real_host() -> Arc<dyn Host> {
    Arc::new(RealHost)
}

// --- The state of each disk -----------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    /// Already mounted and proven when looked at.
    Mounted(String),
    /// Mounted by this pass.
    JustMounted(String),
    Absent(Absence),
}

impl State {
    pub fn present(&self) -> bool {
        !matches!(self, State::Absent(_))
    }
}

pub struct Report {
    /// Mounts under `<root>` that were unmounted to leave the old layout, deepest first.
    pub moved: Vec<PathBuf>,
    /// Lines removed from fstab.
    pub fstab_removed: Vec<String>,
    pub disks: Vec<(Entry, State)>,
}

impl Report {
    pub fn journal_present(&self) -> bool {
        self.disks.iter().any(|(e, s)| e.role == Role::Journal && s.present())
    }
}

/// Mount what is not mounted. Never over something that is: a path that has a mount of the
/// wrong filesystem is reported, not covered, because covering is how a disk got hidden.
pub fn mount_all(host: &dyn Host, root: &Path, manifest: &Manifest) -> Vec<(Entry, State)> {
    let mut out = Vec::new();
    for entry in &manifest.entries {
        let dir = mount_dir(root, &entry.uuid);
        // A plain directory on the root filesystem is what a mountpoint is before it is
        // mounted, and what an absent disk leaves behind. Sidon writes nothing into it.
        if let Err(e) = host.mkdir(&dir) {
            out.push((entry.clone(), State::Absent(Absence::MountFailed(format!("mkdir: {e}")))));
            continue;
        }
        let state = match probe(host, &dir, &entry.uuid) {
            Ok(uid) => State::Mounted(uid),
            Err(Absence::NoSentinel) => match host.claim_uid(&dir) {
                Some(uid) => State::JustMounted(uid),
                None => State::Absent(Absence::NoSentinel),
            },
            Err(Absence::NotMounted) => match host.mount(&entry.uuid, &dir) {
                Err(m) => State::Absent(Absence::MountFailed(m)),
                Ok(()) => match probe(host, &dir, &entry.uuid) {
                    Ok(uid) => State::JustMounted(uid),
                    Err(Absence::NoSentinel) => match host.claim_uid(&dir) {
                        Some(uid) => State::JustMounted(uid),
                        None => State::Absent(Absence::NoSentinel),
                    },
                    Err(other) => State::Absent(other),
                },
            },
            Err(other) => State::Absent(other),
        };
        out.push((entry.clone(), state));
    }
    out
}

/// `/etc/fstab` with every line that mounts `root` or something under it removed, and the
/// lines removed. Everything else comes back byte for byte.
///
/// The test is on the mount point *field* and respects a path boundary: `/var/lib/hci/sidonia`
/// is not sidon's.
pub fn strip_sidon_lines(text: &str, root: &Path) -> (String, Vec<String>) {
    let root = root.to_string_lossy();
    let under = format!("{}/", root.trim_end_matches('/'));
    let mut kept = String::with_capacity(text.len());
    let mut removed = Vec::new();
    for line in text.split_inclusive('\n') {
        let body = line.trim_end_matches(['\n', '\r']);
        let trimmed = body.trim_start();
        let sidon = if trimmed.starts_with('#') {
            false
        } else {
            let f: Vec<&str> = trimmed.split_whitespace().collect();
            f.len() >= 2 && (f[1] == root || f[1].starts_with(&under))
        };
        if sidon {
            removed.push(body.to_string());
        } else {
            kept.push_str(line);
        }
    }
    (kept, removed)
}

/// The mounts under `root` that are not where the layout puts them, deepest first.
///
/// When `root` is itself a mount that is all of them: the old layout nests everything in
/// the volume, and whatever is under it comes off with it. When it is not, only the ones
/// that are not the manifest's own `<root>/disks/<uuid>` paths -- which is what a move that
/// stopped half way leaves behind, and lets the next start finish it rather than treat a
/// plain root as proof that there is nothing left to do.
fn old_layout_mounts(host: &dyn Host, root: &Path, manifest: &Manifest) -> Vec<(PathBuf, u64)> {
    let all = host.mounts_under(root);
    if is_mount(host, root) {
        return all;
    }
    let ours: Vec<PathBuf> = manifest.entries.iter().map(|e| mount_dir(root, &e.uuid)).collect();
    all.into_iter().filter(|(p, _)| !ours.contains(p)).collect()
}

/// Everything unmounted to leave must be something the manifest names, or moving it
/// orphans a disk. Judged by the device the mount table lists, so a mount that another one
/// covers is judged too.
fn known_to_the_manifest(host: &dyn Host, manifest: &Manifest, mount: &(PathBuf, u64)) -> Result<()> {
    match host.uuid_of_dev(mount.1) {
        Some(u) if manifest.has(&u) => Ok(()),
        Some(u) => Err(Error::refused(format!(
            "{} is mounted from filesystem {u}, which {} does not list; refusing to move the \
             layout, because that disk would stop being part of the store. Add it to the \
             manifest (role extent) or unmount it deliberately.",
            mount.0.display(),
            manifest_path().display()
        ))),
        None => Err(Error::refused(format!(
            "cannot tell which filesystem is mounted at {}; refusing to move the layout",
            mount.0.display()
        ))),
    }
}

/// Leave the old layout, if the node is in it. Returns what it unmounted.
///
/// Called only with sidon not running. See the module comment for what it refuses.
pub fn leave_old_layout(host: &dyn Host, root: &Path, manifest: &Manifest) -> Result<Vec<PathBuf>> {
    let mut pending = old_layout_mounts(host, root, manifest);
    if pending.is_empty() {
        return Ok(Vec::new());
    }

    // Judged before any is unmounted, so a refusal leaves everything as it was.
    for m in &pending {
        known_to_the_manifest(host, manifest, m)?;
    }

    // In rounds, deepest first, never lazily. A child that a later mount of its parent
    // covers cannot be unmounted by path until the cover is gone -- the kernel answers
    // EINVAL for it -- so that one waits for the next round, which is how the covered
    // child of the shadowing outage is recovered without being touched out of order. The
    // table is read again each round, because unmounting a cover is what reveals the mount
    // under it, and what is revealed is judged like the rest. Any other failure stops the
    // move: EBUSY means something is using the mount, and a lazy unmount would succeed
    // while it still did.
    let mut moved: Vec<PathBuf> = Vec::new();
    for _round in 0..8 {
        if pending.is_empty() {
            break;
        }
        let mut progress = false;
        for m in &pending {
            match host.umount(&m.0) {
                Ok(()) => {
                    moved.push(m.0.clone());
                    progress = true;
                }
                Err(e) if e.contains("Invalid argument") => {}
                Err(e) => {
                    return Err(Error::refused(format!(
                        "cannot unmount {} ({e}). The move to the new layout stops here; nothing \
                         has been lost, and it resumes when sidon next starts. Find what holds \
                         it with `fuser -vm {}`.",
                        m.0.display(),
                        m.0.display()
                    )))
                }
            }
        }
        if !progress {
            return Err(Error::refused(format!(
                "cannot unmount {}: no remaining mount can be reached by path",
                pending[0].0.display()
            )));
        }
        pending = old_layout_mounts(host, root, manifest);
        for m in &pending {
            known_to_the_manifest(host, manifest, m)?;
        }
    }
    if !pending.is_empty() {
        return Err(Error::refused(format!(
            "{} mounts under {} would not come off; refusing to continue",
            pending.len(),
            root.display()
        )));
    }
    if is_mount(host, root) {
        return Err(Error::refused(format!(
            "{} is still a mount after unmounting everything under it; refusing to continue",
            root.display()
        )));
    }

    Ok(moved)
}

/// Remove the sidon lines from fstab, keeping a copy. Failure is reported and survivable:
/// the mounts are already moved, and a stale line is undone by the next start.
fn rewrite_fstab(host: &dyn Host, fstab: &Path, root: &Path) -> Vec<String> {
    let text = match std::fs::read_to_string(fstab) {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };
    let (kept, removed) = strip_sidon_lines(&text, root);
    if removed.is_empty() {
        return removed;
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let backup = PathBuf::from(format!("{}.hci-bak.{stamp}", fstab.display()));
    let staged = PathBuf::from(format!("{}.hci-new", fstab.display()));
    let done = std::fs::copy(fstab, &backup)
        .and_then(|_| std::fs::write(&staged, kept.as_bytes()))
        .and_then(|_| std::fs::rename(&staged, fstab));
    match done {
        Ok(()) => {
            host.reload_units();
            removed
        }
        Err(e) => {
            let _ = std::fs::remove_file(&staged);
            eprintln!(
                "sidon: could not remove the sidon lines from {}: {e}. They are harmless to \
                 this boot's work and are removed the next time sidon starts.",
                fstab.display()
            );
            Vec::new()
        }
    }
}

/// Bring the node to the layout: leave the old one if need be, then mount every disk.
pub fn converge(host: &dyn Host, root: &Path, manifest: &Manifest, fstab: &Path) -> Result<Report> {
    let moved = leave_old_layout(host, root, manifest)?;
    // Whether or not anything was mounted: a boot where the old volume came up late leaves
    // the node with a plain root and an fstab that would mount the old layout at the next
    // boot, and nothing else would ever take the lines out.
    let fstab_removed = rewrite_fstab(host, fstab, root);
    let disks = mount_all(host, root, manifest);
    Ok(Report { moved, fstab_removed, disks })
}

/// What `Daemon::new` and `sidon mounts apply` call. `Ok(None)` is an unmanaged node.
pub fn prepare(root: &Path) -> Result<Option<Report>> {
    let manifest = match load_manifest()? {
        Some(m) => m,
        None => return Ok(None),
    };
    let report = converge(&RealHost, root, &manifest, &fstab_path())?;
    Ok(Some(report))
}

/// One line per disk, for a log or a terminal.
pub fn describe(report: &Report) -> Vec<String> {
    let mut lines = Vec::new();
    for m in &report.moved {
        lines.push(format!("left the old layout: unmounted {}", m.display()));
    }
    for l in &report.fstab_removed {
        lines.push(format!("removed from fstab: {l}"));
    }
    for (e, s) in &report.disks {
        lines.push(match s {
            State::Mounted(uid) => format!("{} {}: mounted, disk.uid {uid}", e.role.name(), e.uuid),
            State::JustMounted(uid) => format!("{} {}: mounted now, disk.uid {uid}", e.role.name(), e.uuid),
            State::Absent(why) => format!("{} {}: ABSENT, {why}", e.role.name(), e.uuid),
        });
    }
    lines
}

/// The journal volume's mount directory, proven present, or the reason it is not.
/// `Ok(None)` on an unmanaged node, where the root is the volume.
pub fn journal_volume_with(host: &dyn Host, root: &Path, manifest: Option<&Manifest>) -> Result<Option<PathBuf>> {
    let manifest = match manifest {
        Some(m) => m,
        None => return Ok(None),
    };
    let entry = manifest.journal().ok_or_else(|| {
        Error::refused(format!("{} names no journal volume", manifest_path().display()))
    })?;
    let dir = mount_dir(root, &entry.uuid);
    match probe(host, &dir, &entry.uuid) {
        Ok(_) => Ok(Some(dir)),
        Err(why) => Err(Error::refused(format!(
            "the journal volume {} is not available at {} ({why}); refusing to use the root \
             filesystem in its place",
            entry.uuid,
            dir.display()
        ))),
    }
}

pub fn journal_volume(root: &Path) -> Result<Option<PathBuf>> {
    let manifest = load_manifest()?;
    journal_volume_with(&RealHost, root, manifest.as_ref())
}

/// Where the journals, replica state and the journal volume's own extent groups live: the
/// proven journal volume on a managed node, the root on an unmanaged one. Asked again on
/// every use, because the answer can change while sidon runs.
pub fn volume_dir(root: &Path) -> Result<PathBuf> {
    Ok(journal_volume(root)?.unwrap_or_else(|| root.to_path_buf()))
}

pub fn journal_dir(root: &Path) -> Result<PathBuf> {
    Ok(volume_dir(root)?.join("journal"))
}

/// Disks the manifest names that are not usable now, as JSON for `capacity`.
pub fn absent_report(root: &Path) -> Vec<serde_json::Value> {
    let manifest = match load_manifest() {
        Ok(Some(m)) => m,
        _ => return Vec::new(),
    };
    let mut out = Vec::new();
    for e in &manifest.entries {
        if let Err(why) = probe(&RealHost, &mount_dir(root, &e.uuid), &e.uuid) {
            out.push(serde_json::json!({
                "uuid": e.uuid,
                "role": e.role.name(),
                "path": mount_dir(root, &e.uuid).to_string_lossy(),
                "reason": why.to_string(),
            }));
        }
    }
    out
}

/// Extent groups sitting on the root filesystem under the old extent directory: what
/// sidon wrote while the volume was not mounted. Reported, never touched.
pub fn strays_on_root(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    for dir in [root.join("egroups")] {
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for e in entries.flatten() {
                if e.file_name().to_string_lossy().ends_with(".eg") {
                    found.push(e.path());
                }
            }
        }
    }
    found
}

// --- The real machine -----------------------------------------------------------------

pub struct RealHost;

extern "C" {
    fn mount(
        source: *const c_char,
        target: *const c_char,
        fstype: *const c_char,
        flags: c_ulong,
        data: *const c_void,
    ) -> c_int;
    fn umount2(target: *const c_char, flags: c_int) -> c_int;
}

const MS_NOATIME: c_ulong = 1024;

fn cstr(p: &Path) -> std::result::Result<CString, String> {
    CString::new(p.as_os_str().as_bytes()).map_err(|e| e.to_string())
}

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

/// Mount points at or under `root` from `/proc/self/mountinfo` text, deepest first.
/// Used to know *what to unmount*; whether a path is mounted is never decided from it.
pub fn mountpoints_under(text: &str, root: &Path) -> Vec<(PathBuf, u64)> {
    let mut found: Vec<(PathBuf, u64)> = Vec::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split(' ').collect();
        if fields.len() < 5 {
            continue;
        }
        let mp = PathBuf::from(unescape(fields[4]));
        let dev = match fields[2].split_once(':') {
            Some((a, b)) => match (a.parse::<u64>(), b.parse::<u64>()) {
                (Ok(major), Ok(minor)) => makedev(major, minor),
                _ => continue,
            },
            None => continue,
        };
        if mp.starts_with(root) {
            found.push((mp, dev));
        }
    }
    // Deepest first; among equals, the later mount first, which is the one on top. A path
    // listed twice is two mounts, one over the other, and both are returned.
    found.reverse();
    found.sort_by_key(|(p, _)| std::cmp::Reverse(p.components().count()));
    found
}

/// The device number as `stat` reports it, from the `major:minor` the mount table lists.
fn makedev(major: u64, minor: u64) -> u64 {
    ((major & 0xffff_f000) << 32) | ((major & 0xfff) << 8) | ((minor & 0xffff_ff00) << 12) | (minor & 0xff)
}

impl Host for RealHost {
    fn dev_of(&self, path: &Path) -> Option<u64> {
        std::fs::metadata(path).ok().map(|m| m.dev())
    }

    fn rdev_of_uuid(&self, uuid: &str) -> Option<u64> {
        std::fs::metadata(Path::new("/dev/disk/by-uuid").join(uuid)).ok().map(|m| m.rdev())
    }

    fn uuid_of_dev(&self, dev: u64) -> Option<String> {
        for entry in std::fs::read_dir("/dev/disk/by-uuid").ok()?.flatten() {
            if std::fs::metadata(entry.path()).map(|m| m.rdev()).ok() == Some(dev) {
                return Some(entry.file_name().to_string_lossy().to_string());
            }
        }
        None
    }

    fn read_uid(&self, dir: &Path) -> Option<String> {
        let text = std::fs::read_to_string(dir.join(crate::extent::UID_FILE)).ok()?;
        let text = text.trim().to_string();
        if !text.is_empty() && text.len() <= 64 && text.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            Some(text)
        } else {
            None
        }
    }

    fn claim_uid(&self, dir: &Path) -> Option<String> {
        let label = dir.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
        let (uid, persisted) = crate::extent::identify(&dir.join("egroups"), &label);
        if persisted { Some(uid) } else { None }
    }

    fn mounts_under(&self, root: &Path) -> Vec<(PathBuf, u64)> {
        let text = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
        let canon = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
        mountpoints_under(&text, &canon)
    }

    fn mkdir(&self, dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)
    }

    fn mount(&self, uuid: &str, target: &Path) -> std::result::Result<(), String> {
        let source = cstr(&Path::new("/dev/disk/by-uuid").join(uuid))?;
        let target = cstr(target)?;
        let fstype = CString::new("xfs").map_err(|e| e.to_string())?;
        let rc = unsafe {
            mount(source.as_ptr(), target.as_ptr(), fstype.as_ptr(), MS_NOATIME, std::ptr::null())
        };
        if rc == 0 { Ok(()) } else { Err(std::io::Error::last_os_error().to_string()) }
    }

    fn umount(&self, target: &Path) -> std::result::Result<(), String> {
        let target = cstr(target)?;
        let rc = unsafe { umount2(target.as_ptr(), 0) };
        if rc == 0 { Ok(()) } else { Err(std::io::Error::last_os_error().to_string()) }
    }

    fn reload_units(&self) {
        // Best effort: it only refreshes systemd's idea of what fstab declares.
        let _ = std::process::Command::new("systemctl").arg("daemon-reload").status();
    }
}

// --- The `sidon mounts` command -------------------------------------------------------

/// `sidon mounts` (read only) and `sidon mounts apply`. Returns the exit status.
pub fn command(args: &[String]) -> i32 {
    let root = PathBuf::from(std::env::var("SIDON_ROOT").unwrap_or_else(|_| "/var/lib/hci/sidon".to_string()));
    let apply = args.first().map(String::as_str) == Some("apply");
    if !apply && !args.is_empty() {
        eprintln!("usage: sidon mounts [apply]");
        return 2;
    }
    let manifest = match load_manifest() {
        Ok(Some(m)) => m,
        Ok(None) => {
            println!(
                "no {}: this node is unmanaged, so sidon uses {} as it is. Nothing to do.",
                manifest_path().display(),
                root.display()
            );
            return 0;
        }
        Err(e) => {
            eprintln!("sidon: {e}");
            return 1;
        }
    };

    if !apply {
        let host = RealHost;
        if is_mount(&host, &root) {
            println!(
                "{} is itself a mount: this node is still in the old layout. Stop sidon and run \
                 `sidon mounts apply`, or start sidon, and it is moved.",
                root.display()
            );
        }
        let mut ok = true;
        for e in &manifest.entries {
            match probe(&host, &mount_dir(&root, &e.uuid), &e.uuid) {
                Ok(uid) => println!("{} {}: mounted, disk.uid {uid}", e.role.name(), e.uuid),
                Err(why) => {
                    ok = false;
                    println!("{} {}: ABSENT, {why}", e.role.name(), e.uuid);
                }
            }
        }
        return if ok { 0 } else { 1 };
    }

    // Refuse while sidon is serving. Moving a mount under it is the thing this exists to
    // never do; the control socket answering is the proof it is running.
    let socket = std::env::var("SIDON_CONTROL").unwrap_or_else(|_| "/run/sidon/control.sock".to_string());
    if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
        eprintln!(
            "sidon is running (its control socket {socket} answers). Refusing to change mounts \
             under it. Stop it first: systemctl stop sidon"
        );
        return 3;
    }
    match converge(&RealHost, &root, &manifest, &fstab_path()) {
        Ok(report) => {
            for line in describe(&report) {
                println!("{line}");
            }
            if report.journal_present() { 0 } else { 1 }
        }
        Err(e) => {
            eprintln!("sidon: {e}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    const ROOT: &str = "/var/lib/hci/sidon";
    const LV: &str = "dfa470d5-2350-4077-a8ed-7b942eb0cff0";
    const SDC: &str = "5c9e756d-a035-4de7-9dc0-11afcc82f0fe";

    /// A model of a machine's mount table, in mount order, with the same shadowing rules as
    /// a kernel: a path resolves to the most recently mounted mount that is a prefix of it,
    /// so mounting a parent over a child hides the child.
    #[derive(Default)]
    struct Model {
        mounts: Vec<(PathBuf, u64)>,
        devices: HashMap<String, u64>,
        /// (device the file is on, path relative to that mount) -> disk.uid
        uids: HashMap<(u64, PathBuf), String>,
        busy: HashSet<PathBuf>,
        log: Vec<String>,
        claimed: u32,
    }

    struct Fake(Mutex<Model>);

    impl Fake {
        fn new() -> Fake {
            Fake(Mutex::new(Model::default()))
        }
        fn attach(&self, uuid: &str, dev: u64) {
            self.0.lock().unwrap().devices.insert(uuid.to_string(), dev);
        }
        fn mount_at(&self, path: &str, dev: u64) {
            self.0.lock().unwrap().mounts.push((PathBuf::from(path), dev));
        }
        fn sentinel(&self, dev: u64, rel: &str, uid: &str) {
            self.0.lock().unwrap().uids.insert((dev, PathBuf::from(rel)), uid.to_string());
        }
        fn hold(&self, path: &str) {
            self.0.lock().unwrap().busy.insert(PathBuf::from(path));
        }
        fn log(&self) -> Vec<String> {
            self.0.lock().unwrap().log.clone()
        }
        fn mounted(&self) -> Vec<String> {
            self.0.lock().unwrap().mounts.iter().map(|(p, _)| p.to_string_lossy().to_string()).collect()
        }
    }

    fn resolve(m: &Model, p: &Path) -> (u64, PathBuf) {
        for (mp, dev) in m.mounts.iter().rev() {
            if p.starts_with(mp) {
                return (*dev, p.strip_prefix(mp).unwrap().to_path_buf());
            }
        }
        (1, p.to_path_buf())
    }

    impl Host for Fake {
        fn dev_of(&self, path: &Path) -> Option<u64> {
            Some(resolve(&self.0.lock().unwrap(), path).0)
        }
        fn rdev_of_uuid(&self, uuid: &str) -> Option<u64> {
            self.0.lock().unwrap().devices.get(uuid).copied()
        }
        fn uuid_of_dev(&self, dev: u64) -> Option<String> {
            self.0.lock().unwrap().devices.iter().find(|(_, d)| **d == dev).map(|(u, _)| u.clone())
        }
        fn read_uid(&self, dir: &Path) -> Option<String> {
            let m = self.0.lock().unwrap();
            let (dev, rel) = resolve(&m, dir);
            m.uids.get(&(dev, rel)).cloned()
        }
        fn claim_uid(&self, dir: &Path) -> Option<String> {
            let mut m = self.0.lock().unwrap();
            let (dev, rel) = resolve(&m, dir);
            m.claimed += 1;
            let uid = format!("claimed-{}", m.claimed);
            m.uids.insert((dev, rel), uid.clone());
            Some(uid)
        }
        fn mounts_under(&self, root: &Path) -> Vec<(PathBuf, u64)> {
            let m = self.0.lock().unwrap();
            let mut v: Vec<(PathBuf, u64)> =
                m.mounts.iter().rev().filter(|(p, _)| p.starts_with(root)).cloned().collect();
            v.sort_by_key(|(p, _)| std::cmp::Reverse(p.components().count()));
            v
        }
        fn mkdir(&self, _dir: &Path) -> std::io::Result<()> {
            Ok(())
        }
        fn mount(&self, uuid: &str, target: &Path) -> std::result::Result<(), String> {
            let mut m = self.0.lock().unwrap();
            let dev = *m.devices.get(uuid).ok_or("no such device")?;
            m.log.push(format!("mount {uuid} {}", target.display()));
            m.mounts.push((target.to_path_buf(), dev));
            Ok(())
        }
        fn umount(&self, target: &Path) -> std::result::Result<(), String> {
            let mut m = self.0.lock().unwrap();
            if m.busy.contains(target) {
                return Err("Device or resource busy".to_string());
            }
            // The kernel unmounts what the *path* resolves to. A mount covered by a later one
            // resolves to the cover, and that is only a mount point if the cover's own
            // path is this path.
            let top = m.mounts.iter().rposition(|(p, _)| target.starts_with(p));
            match top {
                Some(i) if m.mounts[i].0 == target => {
                    m.log.push(format!("umount {}", target.display()));
                    m.mounts.remove(i);
                    Ok(())
                }
                _ => Err("Invalid argument".to_string()),
            }
        }
        fn reload_units(&self) {}
    }

    fn manifest() -> Manifest {
        parse_manifest(&format!("{LV} journal\n{SDC} extent\n")).unwrap()
    }

    /// The machine as the existing nodes are: the volume mounted at the root and the second
    /// disk mounted inside it. Each carries its sentinel at the top of its own filesystem.
    fn old_layout() -> Fake {
        let f = Fake::new();
        f.attach(LV, 10);
        f.attach(SDC, 11);
        f.mount_at(ROOT, 10);
        f.mount_at(&format!("{ROOT}/disks/sdc"), 11);
        f.sentinel(10, "", "uid-lv");
        f.sentinel(11, "", "uid-sdc");
        f
    }

    fn tmp_fstab(name: &str, body: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("sidon-mounts-{}-{name}", std::process::id()));
        std::fs::write(&p, body).unwrap();
        p
    }

    /// Removes the table and the dated copy a rewrite keeps beside it.
    fn forget(fstab: &Path) {
        let dir = fstab.parent().unwrap();
        let stem = fstab.file_name().unwrap().to_string_lossy().to_string();
        for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            if e.file_name().to_string_lossy().starts_with(&stem) {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }

    // --- the manifest ---

    #[test]
    fn a_manifest_names_filesystems_by_uuid_and_the_journal_comes_first() {
        let m = parse_manifest(&format!("# written by the claim step\n{SDC} extent\n{LV} journal  # first disk\n")).unwrap();
        assert_eq!(m.entries[0], Entry { uuid: LV.to_string(), role: Role::Journal });
        assert_eq!(m.entries[1].uuid, SDC);
    }

    #[test]
    fn a_manifest_entry_cannot_name_a_path() {
        // The uuid becomes a directory under disks/, so one that can climb out is refused.
        assert!(parse_manifest("../../etc extent\n").is_err());
        assert!(parse_manifest("a/b extent\n").is_err());
        assert!(parse_manifest("..  extent\n").is_err());
    }

    #[test]
    fn a_manifest_that_is_ambiguous_is_refused_not_guessed() {
        assert!(parse_manifest(&format!("{LV} journal\n{LV} extent\n")).is_err());
        assert!(parse_manifest(&format!("{LV} journal\n{SDC} journal\n")).is_err());
        assert!(parse_manifest(&format!("{LV} volume\n")).is_err());
        assert!(parse_manifest(&format!("{LV}\n")).is_err());
    }

    // --- the layout ---

    #[test]
    fn every_disk_is_a_sibling_and_none_is_inside_another() {
        let m = manifest();
        let dirs: Vec<PathBuf> = m.entries.iter().map(|e| mount_dir(Path::new(ROOT), &e.uuid)).collect();
        for a in &dirs {
            assert_eq!(a.parent(), Some(Path::new("/var/lib/hci/sidon/disks")));
            for b in &dirs {
                if a != b {
                    assert!(!a.starts_with(b), "{} is nested inside {}", a.display(), b.display());
                }
            }
        }
    }

    #[test]
    fn a_mount_is_named_by_uuid_never_by_kernel_device() {
        let dir = mount_dir(Path::new(ROOT), SDC);
        assert!(dir.ends_with(SDC));
        assert!(!dir.to_string_lossy().contains("sd"), "{}", dir.display());
    }

    // --- presence ---

    #[test]
    fn a_mounted_disk_with_its_sentinel_is_present() {
        let f = Fake::new();
        f.attach(SDC, 11);
        f.mount_at(&format!("{ROOT}/disks/{SDC}"), 11);
        f.sentinel(11, "", "uid-sdc");
        assert_eq!(probe(&f, &mount_dir(Path::new(ROOT), SDC), SDC), Ok("uid-sdc".to_string()));
    }

    #[test]
    fn a_sentinel_left_on_the_root_filesystem_does_not_make_a_missing_disk_present() {
        // The disk was unmounted and a disk.uid sits in the plain directory. A check for
        // the file alone passes; the directory is not a mount, so this must not.
        let f = Fake::new();
        f.attach(SDC, 11);
        f.sentinel(1, &format!("{ROOT}/disks/{SDC}"), "leftover");
        assert_eq!(
            probe(&f, &mount_dir(Path::new(ROOT), SDC), SDC),
            Err(Absence::NotMounted),
            "a sentinel on the root filesystem was believed"
        );
    }

    #[test]
    fn the_real_disk_mounted_over_a_leftover_sentinel_reads_its_own() {
        let f = Fake::new();
        f.attach(SDC, 11);
        f.sentinel(1, &format!("{ROOT}/disks/{SDC}"), "leftover-on-root");
        f.sentinel(11, "", "uid-sdc");
        f.mount_at(&format!("{ROOT}/disks/{SDC}"), 11);
        assert_eq!(
            probe(&f, &mount_dir(Path::new(ROOT), SDC), SDC),
            Ok("uid-sdc".to_string()),
            "the disk's identity was taken from the file it covers"
        );
    }

    #[test]
    fn a_child_covered_by_a_later_mount_of_its_parent_reads_as_absent() {
        // Tonight's second outage. The child is in the mount table; the path no longer
        // resolves to it. Only a stat of the path tells the truth.
        let f = Fake::new();
        f.attach(LV, 10);
        f.attach(SDC, 11);
        let child = format!("{ROOT}/disks/{SDC}");
        f.mount_at(&child, 11);
        f.sentinel(11, "", "uid-sdc");
        f.mount_at(ROOT, 10); // the parent, mounted over the top of the child
        assert!(f.mounted().contains(&child), "the table still lists the child");
        assert_eq!(probe(&f, Path::new(&child), SDC), Err(Absence::NotMounted));
    }

    #[test]
    fn a_different_filesystem_at_the_path_is_wrong_not_present() {
        let f = Fake::new();
        f.attach(SDC, 11);
        f.attach(LV, 10);
        f.mount_at(&format!("{ROOT}/disks/{SDC}"), 10);
        f.sentinel(10, "", "uid-lv");
        assert_eq!(probe(&f, &mount_dir(Path::new(ROOT), SDC), SDC), Err(Absence::WrongDevice));
    }

    #[test]
    fn mounting_never_covers_something_already_mounted_there() {
        let f = Fake::new();
        f.attach(SDC, 11);
        f.attach(LV, 10);
        f.mount_at(&format!("{ROOT}/disks/{SDC}"), 10);
        let before = f.mounted();
        let states = mount_all(&f, Path::new(ROOT), &parse_manifest(&format!("{SDC} extent\n")).unwrap());
        assert!(matches!(states[0].1, State::Absent(Absence::WrongDevice)));
        assert_eq!(f.mounted(), before, "a mount was stacked over another");
    }

    #[test]
    fn a_disk_that_is_not_attached_is_absent_and_nothing_is_attempted() {
        let f = Fake::new();
        f.attach(LV, 10);
        f.sentinel(10, "", "uid-lv");
        let states = mount_all(&f, Path::new(ROOT), &manifest());
        assert!(states[0].1.present());
        assert_eq!(states[1].1, State::Absent(Absence::NoDevice));
        assert!(!f.log().iter().any(|l| l.contains(SDC)), "a mount of a missing device was tried");
    }

    #[test]
    fn the_journal_volume_missing_is_a_refusal_not_a_fallback_to_the_root() {
        let f = Fake::new();
        f.attach(SDC, 11);
        let m = manifest();
        let r = journal_volume_with(&f, Path::new(ROOT), Some(&m));
        assert!(matches!(r, Err(Error::Refused(_))), "{r:?}");
    }

    #[test]
    fn a_node_with_no_manifest_keeps_the_root_as_its_volume() {
        let f = Fake::new();
        assert!(matches!(journal_volume_with(&f, Path::new(ROOT), None), Ok(None)));
    }

    #[test]
    fn a_fresh_disk_gets_its_sentinel_once_its_filesystem_is_proven() {
        let f = Fake::new();
        f.attach(LV, 10);
        let m = parse_manifest(&format!("{LV} journal\n")).unwrap();
        let first = mount_all(&f, Path::new(ROOT), &m);
        assert!(matches!(first[0].1, State::JustMounted(_)));
        let second = mount_all(&f, Path::new(ROOT), &m);
        assert!(matches!(second[0].1, State::Mounted(_)), "a second pass mounted again: {:?}", second[0].1);
        assert_eq!(f.log().iter().filter(|l| l.starts_with("mount ")).count(), 1);
    }

    // --- leaving the old layout ---

    const REAL_FSTAB: &str = "\
UUID=2b665c02-9b7a-472e-bcef-1b41b6a6462f /                       xfs     defaults        0 0
UUID=b2cfead7-e59c-4152-98d7-a47e8f9abb7f /boot                   xfs     defaults        0 0
UUID=5f470b19-01b3-4ef5-8571-49bbeade4b97 none                    swap    defaults        0 0
UUID=dfa470d5-2350-4077-a8ed-7b942eb0cff0 /var/lib/hci/sidon xfs defaults,noatime,nofail,x-systemd.device-timeout=5s 0 0
UUID=5c9e756d-a035-4de7-9dc0-11afcc82f0fe /var/lib/hci/sidon/disks/sdc xfs defaults,noatime,nofail,x-systemd.device-timeout=5s 0 0
";

    #[test]
    fn the_sidon_lines_come_out_of_fstab_and_nothing_else_does() {
        let (kept, removed) = strip_sidon_lines(REAL_FSTAB, Path::new(ROOT));
        assert_eq!(removed.len(), 2);
        assert!(!kept.contains("/var/lib/hci/sidon"));
        for keep in REAL_FSTAB.lines().take(3) {
            assert!(kept.contains(keep), "{keep} was lost");
        }
        let (again, none) = strip_sidon_lines(&kept, Path::new(ROOT));
        assert!(none.is_empty());
        assert_eq!(again, kept, "a second pass changed the file");
    }

    #[test]
    fn a_path_that_merely_begins_with_the_root_is_not_sidons() {
        let text = "UUID=x /var/lib/hci/sidonia xfs defaults 0 0\n# /var/lib/hci/sidon commented\n";
        let (kept, removed) = strip_sidon_lines(text, Path::new(ROOT));
        assert!(removed.is_empty());
        assert_eq!(kept, text);
    }

    #[test]
    fn the_old_layout_is_left_children_first_and_ends_with_a_plain_root() {
        let f = old_layout();
        let fstab = tmp_fstab("old", REAL_FSTAB);
        let report = converge(&f, Path::new(ROOT), &manifest(), &fstab).unwrap();

        let log = f.log();
        let child = log.iter().position(|l| l.ends_with("disks/sdc") && l.starts_with("umount")).unwrap();
        let parent = log.iter().position(|l| *l == format!("umount {ROOT}")).unwrap();
        assert!(child < parent, "the parent was unmounted before the child it contains: {log:?}");

        assert!(!is_mount(&f, Path::new(ROOT)), "the root is still a mount, so a child can still be shadowed");
        assert!(report.journal_present());
        assert!(report.disks.iter().all(|(_, s)| s.present()), "{:?}", report.disks);
        assert_eq!(report.moved.len(), 2);

        let after = std::fs::read_to_string(&fstab).unwrap();
        assert!(!after.contains("/var/lib/hci/sidon"), "{after}");
        assert!(after.contains("/boot"));
        forget(&fstab);
    }

    #[test]
    fn the_node_where_the_second_disk_is_the_kernels_sdb_moves_the_same_way() {
        // `.43`: the fstab names the same UUID at the same path whatever the kernel calls
        // the device, because it was never keyed on the device.
        let fstab_43 = REAL_FSTAB.replace("UUID=5c9e756d-a035-4de7-9dc0-11afcc82f0fe", "/dev/sdb");
        let (_, removed) = strip_sidon_lines(&fstab_43, Path::new(ROOT));
        assert_eq!(removed.len(), 2, "a device-path line was not recognised as sidon's");
    }

    #[test]
    fn converging_twice_changes_nothing_the_second_time() {
        let f = old_layout();
        let fstab = tmp_fstab("twice", REAL_FSTAB);
        converge(&f, Path::new(ROOT), &manifest(), &fstab).unwrap();
        let mounts = f.mounted();
        let log_len = f.log().len();
        let again = converge(&f, Path::new(ROOT), &manifest(), &fstab).unwrap();
        assert!(again.moved.is_empty() && again.fstab_removed.is_empty());
        assert_eq!(f.mounted(), mounts);
        assert_eq!(f.log().len(), log_len, "the second pass touched a mount");
        forget(&fstab);
    }

    #[test]
    fn a_mount_something_is_using_stops_the_move_and_leaves_fstab_alone() {
        let f = old_layout();
        f.hold(ROOT);
        let fstab = tmp_fstab("busy", REAL_FSTAB);
        let r = converge(&f, Path::new(ROOT), &manifest(), &fstab);
        assert!(matches!(r, Err(Error::Refused(_))), "{:?}", r.err());
        assert_eq!(std::fs::read_to_string(&fstab).unwrap(), REAL_FSTAB, "fstab was edited by a move that did not finish");
        assert!(is_mount(&f, Path::new(ROOT)), "the busy mount was removed anyway");
        forget(&fstab);
    }

    #[test]
    fn a_mount_the_manifest_does_not_name_is_refused_before_anything_is_unmounted() {
        let f = old_layout();
        f.attach("aaaa-bbbb", 12);
        f.mount_at(&format!("{ROOT}/disks/sdd"), 12);
        let fstab = tmp_fstab("unknown", REAL_FSTAB);
        let r = converge(&f, Path::new(ROOT), &manifest(), &fstab);
        assert!(matches!(r, Err(Error::Refused(_))));
        assert!(f.log().is_empty(), "something was unmounted before the refusal: {:?}", f.log());
        forget(&fstab);
    }

    #[test]
    fn the_second_disk_absent_after_the_move_does_not_cost_the_journal_volume() {
        let f = Fake::new();
        f.attach(LV, 10);
        f.mount_at(ROOT, 10);
        f.sentinel(10, "", "uid-lv");
        let fstab = tmp_fstab("absent", "");
        let report = converge(&f, Path::new(ROOT), &manifest(), &fstab).unwrap();
        assert!(report.journal_present());
        assert!(!report.disks[1].1.present());
        forget(&fstab);
    }

    #[test]
    fn a_move_that_stopped_half_way_is_finished_by_the_next_start() {
        // The volume came off and the child that was under it did not (it was busy). The
        // root is a plain directory now, which a test of "is the root a mount" would read as
        // nothing left to do, leaving the child mounted at its old path and mounted again
        // at its new one.
        let f = Fake::new();
        f.attach(LV, 10);
        f.attach(SDC, 11);
        f.mount_at(&format!("{ROOT}/disks/sdc"), 11);
        f.sentinel(11, "", "uid-sdc");
        f.sentinel(10, "", "uid-lv");
        assert!(!is_mount(&f, Path::new(ROOT)));
        let fstab = tmp_fstab("half", "");
        let report = converge(&f, Path::new(ROOT), &manifest(), &fstab).unwrap();
        assert_eq!(report.moved, vec![PathBuf::from(format!("{ROOT}/disks/sdc"))]);
        assert!(!f.mounted().iter().any(|m| m.ends_with("disks/sdc")), "{:?}", f.mounted());
        assert!(report.disks.iter().all(|(_, s)| s.present()));
        forget(&fstab);
    }

    #[test]
    fn mountinfo_lists_deepest_first_and_only_what_is_under_the_root() {
        let text = "\
36 1 253:0 / / rw - xfs /dev/mapper/root rw
40 36 253:3 / /var/lib/hci/sidon rw - xfs /dev/mapper/vg_aether-sidon rw
41 40 8:32 / /var/lib/hci/sidon/disks/sdc rw - xfs /dev/sdb rw
42 36 8:1 / /var/lib/hci/sidonia rw - xfs /dev/sda1 rw
";
        let found = mountpoints_under(text, Path::new(ROOT));
        let paths: Vec<PathBuf> = found.iter().map(|(p, _)| p.clone()).collect();
        assert_eq!(paths, vec![PathBuf::from("/var/lib/hci/sidon/disks/sdc"), PathBuf::from(ROOT)]);
        assert_eq!(found[0].1, 8 * 256 + 32, "the device was not read from major:minor");
    }

    #[test]
    fn a_child_covered_by_its_parent_is_recovered_by_unmounting_the_cover_first() {
        // The state tonight left a node in: the child mounted first, the parent mounted over
        // it afterwards. The table lists both; the child's path no longer reaches it.
        let f = Fake::new();
        f.attach(LV, 10);
        f.attach(SDC, 11);
        f.mount_at(&format!("{ROOT}/disks/sdc"), 11);
        f.sentinel(11, "", "uid-sdc");
        f.mount_at(ROOT, 10);
        f.sentinel(10, "", "uid-lv");
        let fstab = tmp_fstab("covered", "");
        let report = converge(&f, Path::new(ROOT), &manifest(), &fstab).unwrap();
        assert!(report.disks.iter().all(|(_, s)| s.present()), "{:?}", report.disks);
        assert!(!is_mount(&f, Path::new(ROOT)));
        assert_eq!(report.moved.len(), 2, "the covered child was left mounted: {:?}", report.moved);
        forget(&fstab);
    }
}
