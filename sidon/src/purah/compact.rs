//! Compaction: copying the live extents out of sparse sealed groups into new ones.
//!
//! Guests overwrite, and an overwrite is redirect-on-write: the new bytes go to a new extent
//! and the old extent becomes garbage *inside a sealed group*. The sweep reclaims a group only
//! when nothing points at any of it, so a group that is three-quarters dead and one extent
//! alive keeps all four megabytes. This pass finds those groups and rewrites what is alive.
//! The design is D-32 in `docs/dfs/decisions.md` and `docs/dfs/compaction.md`; D-23's addendum
//! is why it exists at all (dedup's headline ratio overstates what is returned without it).
//!
//! # The shape of one batch
//!
//! Each step is the one `storage.move` uses (`extent/placement.rs`), generalised from "copy a
//! file" to "build a file from extents":
//!
//! 1. **Read and verify** every live extent of every source group: the group against its seal
//!    hash, each extent's footer against *every* row that points at it. A group that fails is
//!    left alone; this pass is not the repair tool and does not launder damage into a new file.
//! 2. **Stage** the new group under a temporary name that no directory scan will see, fsync,
//!    drop the cache, read it back, and require it to hash as what was written.
//! 3. **Replicate** to the nodes the referring vdisks replicate to, and read the whole group
//!    back from each. Write-all, like a drain: one refusal abandons the batch.
//! 4. **Register** the group in Hydra, already sealed, with its hash.
//! 5. **Publish** by rename, and prove the published file again.
//! 6. **Repoint** each row by compare-and-swap, one at a time, under the rules below.
//!
//! Nothing here deletes anything. The old group stays, unreferenced once its last row has
//! moved, and the sweep removes it under its two-scan grace like any other group. That is the
//! property that makes every crash harmless: after step 6 begins, each row individually names a
//! location that holds exactly the bytes it should (the old group is intact, the new one is
//! verified), so a stop *between any two statements* leaves a map that reads correctly.
//! Re-running recomputes the plan from the map, so it needs no journal and cannot repeat itself.
//!
//! # Why a row is only rewritten when the pass can exclude a drain
//!
//! `metadata.md` section 3 says block-map rows are written with plain statements because there
//! is one writer per partition, and that the moment anyone proposes a second, the price is
//! Paxos per row. This pass is that second writer. A compare-and-swap that lands between a
//! drain's decision to write a row and its write loses the guest's update, and the CAS cannot
//! see it coming: it is the one interleaving the condition does not cover. So a row of a
//! writable vdisk is touched only while this node **owns the vdisk and has it attached**, and
//! then only inside [`Hold`], which takes the vdisk's drain gate -- the flag every drain sets --
//! so no drain is running and none can begin until the rows are done. Rows of an immutable
//! vdisk (a snapshot, an image) are touched freely, since nothing writes them. A group that any
//! writable vdisk *not* attached here still points at is skipped whole: there is no way from
//! this node to exclude that vdisk's drain, and "most of the group" is no use to the sweep.
//!
//! # What this does not do
//!
//! Free anything itself. It never deletes: the old groups are left for the sweep, which frees
//! them here and, since D-33, asks every replica to drop its copy too, so at ftt>=1 the space
//! comes back on the replicas as well -- but only once **each replica runs a build that knows the
//! drop request**, and only after the sweep's two scans; until then compaction has added the new
//! group's bytes to every replica and freed nothing there. The plan reports what it would add on
//! replicas and what it would free there, so the net is visible. A replica from before D-33 keeps
//! its copy of an old group; that is safe, and it is the one case in which the replica figure is
//! growth with no matching saving.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::time::Duration;

use serde_json::{json, Value};

use super::occupancy::{self, GroupRow, Live, Occupancy, VdiskInfo};
use crate::err::{Error, Result};
use crate::extent::{verify_footer, EgroupStore, Tier};
use crate::extent_id_map::Rows;
use crate::meta::{json_params, Cas, Daruk};
use crate::replicate::seal_of;
use crate::replicate::throttle::{Clock, TokenBucket};

pub const DEFAULT_THRESHOLD: f64 = 0.5;
pub const MIN_THRESHOLD: f64 = 0.05;
pub const MAX_THRESHOLD: f64 = 0.95;
pub const DEFAULT_MAX_GROUPS: usize = 8;
pub const DEFAULT_MAX_BYTES: u64 = 128 << 20;
/// Bytes per second the pass may read and write, counting every copy it sends. Low on
/// purpose: the pass is background work and a guest is waiting on the same disks.
pub const DEFAULT_RATE: u64 = 16 << 20;
/// Short of the 60 seconds the control client waits, so the answer always arrives.
pub const DEFAULT_SECONDS: u64 = 40;
pub const TARGET_GROUP_BYTES: u64 = 4 << 20;
/// Entries per list in a report. The counts are exact; the lists are cut.
pub const LISTED: usize = 50;

#[derive(Clone, Debug)]
pub struct Options {
    pub apply: bool,
    /// A group is a candidate when less than this fraction of it is live.
    pub threshold: f64,
    /// At most this many source groups per pass.
    pub max_groups: usize,
    /// At most this many live bytes copied per pass.
    pub max_bytes: u64,
    /// Bytes per second, zero for unlimited.
    pub rate: u64,
    /// Wall-clock budget; no new batch starts after it.
    pub seconds: u64,
    /// How full a new group is packed.
    pub target_bytes: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            apply: false,
            threshold: DEFAULT_THRESHOLD,
            max_groups: DEFAULT_MAX_GROUPS,
            max_bytes: DEFAULT_MAX_BYTES,
            rate: DEFAULT_RATE,
            seconds: DEFAULT_SECONDS,
            target_bytes: TARGET_GROUP_BYTES,
        }
    }
}

// --- What the pass needs from the world -------------------------------------------------

/// Hydra, as far as this pass is concerned: reads, and typed compare-and-swaps.
pub trait Db: Rows {
    fn cas(&self, path: &str, params: Value) -> Result<Cas>;
}

impl Db for Daruk {
    fn cas(&self, path: &str, params: Value) -> Result<Cas> {
        Daruk::cas(self, path, params)
    }
}

/// An attached vdisk whose drains are held off, and whose in-memory map this pass may correct.
pub trait Hold {
    /// Whether the in-memory map still says what the scan said about one extent.
    fn has(&self, idx: u64, egroup: &str, offset: u32, length: u32) -> bool;
    /// Point one extent at its new copy; false if the map entry is no longer as expected.
    fn repoint(
        &self,
        idx: u64,
        egroup: &str,
        offset: u32,
        length: u32,
        to_group: &str,
        to_offset: u32,
    ) -> bool;
}

/// What the daemon provides: its peers, and its attached vdisks.
pub trait Env {
    /// Write a framed extent into a peer's replica store at `offset` of `group`.
    fn put(&self, node: &str, group: &str, offset: u64, data: &[u8], defer_sync: bool) -> Result<()>;
    /// Read bytes back from a peer's replica store.
    fn get(&self, node: &str, group: &str, offset: u64, len: usize) -> Result<Vec<u8>>;
    /// Whether this node owns `vdisk` and has it attached, so that it has an in-memory map.
    fn attached_here(&self, vdisk: &str) -> bool;
    /// Hold off the drains of an attached vdisk. An error means a drain is running or the
    /// vdisk is no longer attached; either way the caller leaves it alone.
    fn hold(&self, vdisk: &str) -> Result<Box<dyn Hold + '_>>;
    /// Extent groups a drain on this node is creating right now.
    fn drain_groups(&self) -> HashSet<String>;
}

/// A named point in a batch where a test may stop it, to prove what a crash there leaves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    Staged,
    Replicated,
    Registered,
    Published,
    Verified,
    Held,
    BeforeRepoint(usize),
    AfterRepoint(usize),
}

/// Production passes nothing, so each call is a no-op the compiler removes.
pub trait Probe {
    fn at(&self, _step: Step) -> Result<()> {
        Ok(())
    }
}

pub struct NoProbe;
impl Probe for NoProbe {}

// --- Planning ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Skip {
    NotLocal,
    DrainInFlight,
    Young,
    Contradictory,
    PastTheEnd,
    UnknownVdisk(String),
    UnusableClass { vdisk: String, class: String },
    WritableAndDetached(String),
}

impl Skip {
    pub fn kind(&self) -> &'static str {
        match self {
            Skip::NotLocal => "not-on-this-node",
            Skip::DrainInFlight => "drain-in-flight",
            Skip::Young => "too-young",
            Skip::Contradictory => "contradictory-rows",
            Skip::PastTheEnd => "extent-past-the-end",
            Skip::UnknownVdisk(_) => "unknown-vdisk",
            Skip::UnusableClass { .. } => "vdisk-being-formed",
            Skip::WritableAndDetached(_) => "writable-vdisk-not-attached-here",
        }
    }
}

impl fmt::Display for Skip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Skip::NotLocal => write!(f, "the group is on none of this node's disks"),
            Skip::DrainInFlight => write!(f, "a drain on this node is still writing it"),
            Skip::Young => write!(f, "younger than the grace period, so its map rows may not all be written yet"),
            Skip::Contradictory => write!(f, "the map's rows for it overlap or disagree about a length"),
            Skip::PastTheEnd => write!(f, "a row names an extent that runs past the end of the group"),
            Skip::UnknownVdisk(v) => write!(f, "vdisk {v} points into it and has no row in dfs_vdisks"),
            Skip::UnusableClass { vdisk, class } => write!(
                f,
                "vdisk {vdisk} is {class}, not rw or immutable; its map is being built or rewritten"
            ),
            Skip::WritableAndDetached(v) => write!(
                f,
                "writable vdisk {v} points into it and is not attached on this node, so a drain of it \
                 cannot be excluded while its rows are rewritten"
            ),
        }
    }
}

/// A sealed group worth rewriting.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub id: String,
    pub size: u64,
    pub live_bytes: u64,
    /// (offset, extent), ascending by offset.
    pub extents: Vec<(u32, Live)>,
    pub container: String,
    /// Peers that hold copies of the vdisks' extents, this node excluded.
    pub replicas: Vec<String>,
    pub hint: String,
    pub seal_hash: String,
    pub vdisks: BTreeSet<String>,
}

impl Candidate {
    pub fn fraction(&self) -> f64 {
        if self.size == 0 { 1.0 } else { self.live_bytes as f64 / self.size as f64 }
    }

    pub fn garbage(&self) -> u64 {
        self.size.saturating_sub(self.live_bytes)
    }

    pub fn shared_extents(&self) -> usize {
        self.extents.iter().filter(|(_, l)| l.referrers.len() > 1).count()
    }
}

/// Everything planning reads, so the policy can be exercised with no files and no Hydra.
pub struct Inputs<'a> {
    pub node: &'a str,
    pub groups: &'a [GroupRow],
    pub occ: &'a Occupancy,
    pub vdisks: &'a HashMap<String, VdiskInfo>,
    pub attached_here: &'a dyn Fn(&str) -> bool,
    pub in_flight: &'a HashSet<String>,
    /// The length of the group's file on this node's disks, or `None` if it is on none.
    pub local_len: &'a dyn Fn(&str) -> Option<u64>,
    pub now_ms: i64,
    pub grace_ms: i64,
    pub threshold: f64,
}

#[derive(Debug, Default)]
pub struct Analysis {
    pub candidates: Vec<Candidate>,
    pub skipped: Vec<(String, Skip)>,
    /// Sealed groups this node holds.
    pub sealed: usize,
    /// Groups with live data that is at or above the threshold: nothing to do.
    pub healthy: usize,
    /// Groups nothing points into at all. The sweep's, not this pass's.
    pub dead: usize,
    pub unsealed: usize,
}

pub fn analyze(inp: &Inputs) -> Analysis {
    let mut out = Analysis::default();
    for g in inp.groups {
        if g.state != "sealed" {
            out.unsealed += 1;
            continue;
        }
        out.sealed += 1;
        let Some(by_offset) = inp.occ.live.get(&g.id) else {
            out.dead += 1;
            continue;
        };
        let Some(file_len) = (inp.local_len)(&g.id) else {
            out.skipped.push((g.id.clone(), Skip::NotLocal));
            continue;
        };
        if inp.in_flight.contains(&g.id) {
            out.skipped.push((g.id.clone(), Skip::DrainInFlight));
            continue;
        }
        // The sweep's own young guard, for the sweep's own reason: a drain writes a group
        // before the map rows that name its extents, and a group seen in that window has
        // extents that look dead and are about to be live.
        if g.created_ms > 0 && inp.now_ms - g.created_ms < inp.grace_ms {
            out.skipped.push((g.id.clone(), Skip::Young));
            continue;
        }
        if inp.occ.conflicts.contains(&g.id) {
            out.skipped.push((g.id.clone(), Skip::Contradictory));
            continue;
        }
        let size = file_len.max(1);
        let live_bytes = inp.occ.live_bytes(&g.id);
        if live_bytes as f64 >= inp.threshold * size as f64 {
            out.healthy += 1;
            continue;
        }
        if by_offset.iter().any(|(o, l)| *o as u64 + l.framed() > size) {
            out.skipped.push((g.id.clone(), Skip::PastTheEnd));
            continue;
        }

        let mut vdisks: BTreeSet<String> = BTreeSet::new();
        for live in by_offset.values() {
            for r in &live.referrers {
                vdisks.insert(r.vdisk.clone());
            }
        }
        let mut verdict: Option<Skip> = None;
        let mut replicas: BTreeSet<String> = BTreeSet::new();
        let mut first_container: Option<String> = None;
        for v in &vdisks {
            let Some(info) = inp.vdisks.get(v) else {
                verdict = Some(Skip::UnknownVdisk(v.clone()));
                break;
            };
            if info.class != "rw" && info.class != "immutable" {
                verdict = Some(Skip::UnusableClass { vdisk: v.clone(), class: info.class.clone() });
                break;
            }
            if info.class == "rw" && !(inp.attached_here)(v) {
                verdict = Some(Skip::WritableAndDetached(v.clone()));
                break;
            }
            first_container.get_or_insert_with(|| info.container.clone());
            for r in &info.replicas {
                if r != inp.node {
                    replicas.insert(r.clone());
                }
            }
        }
        if let Some(v) = verdict {
            out.skipped.push((g.id.clone(), v));
            continue;
        }
        let container = inp
            .vdisks
            .get(&g.hint)
            .map(|i| i.container.clone())
            .or(first_container)
            .unwrap_or_default();
        out.candidates.push(Candidate {
            id: g.id.clone(),
            size,
            live_bytes,
            extents: by_offset.iter().map(|(o, l)| (*o, l.clone())).collect(),
            container,
            replicas: replicas.into_iter().collect(),
            hint: g.hint.clone(),
            seal_hash: g.seal_hash.clone(),
            vdisks,
        });
    }
    // Most garbage first, so a pass limited by count or bytes does the most good.
    out.candidates.sort_by(|a, b| {
        a.fraction()
            .partial_cmp(&b.fraction())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    });
    out
}

/// One new group and the sources it is built from.
#[derive(Clone, Debug)]
pub struct Bin {
    pub container: String,
    pub replicas: Vec<String>,
    pub sources: Vec<Candidate>,
    pub capacity: u64,
}

impl Bin {
    pub fn live_bytes(&self) -> u64 {
        self.sources.iter().map(|c| c.live_bytes).sum()
    }

    pub fn source_bytes(&self) -> u64 {
        self.sources.iter().map(|c| c.size).sum()
    }

    /// What the sweep gets back on this node once the sources are dead and the new group exists.
    pub fn net_freed(&self) -> u64 {
        self.source_bytes().saturating_sub(self.live_bytes())
    }
}

/// Pack candidates into new groups.
///
/// Sources are never split: a source group is moved whole or not at all, because a group is
/// reclaimed only when *all* of it is dead and half a move frees nothing. Groups are packed
/// together only when they agree on container and replica set, since a new group must be
/// placed and replicated the way its extents were, and merging two policies would make one of
/// them wrong.
pub fn pack(candidates: Vec<Candidate>, target: u64) -> Vec<Bin> {
    let mut keyed: BTreeMap<(String, Vec<String>), Vec<Candidate>> = BTreeMap::new();
    for c in candidates {
        keyed.entry((c.container.clone(), c.replicas.clone())).or_default().push(c);
    }
    let mut bins: Vec<Bin> = Vec::new();
    for ((container, replicas), mut group) in keyed {
        group.sort_by(|a, b| b.live_bytes.cmp(&a.live_bytes).then_with(|| a.id.cmp(&b.id)));
        let mut mine: Vec<Bin> = Vec::new();
        for c in group {
            let slot = mine.iter().position(|b| b.live_bytes() + c.live_bytes <= b.capacity);
            match slot {
                Some(i) => mine[i].sources.push(c),
                None => mine.push(Bin {
                    container: container.clone(),
                    replicas: replicas.clone(),
                    capacity: target.max(c.size),
                    sources: vec![c],
                }),
            }
        }
        bins.extend(mine);
    }
    for b in &mut bins {
        b.sources.sort_by(|a, c| a.id.cmp(&c.id));
    }
    bins.sort_by(|a, b| {
        b.net_freed()
            .cmp(&a.net_freed())
            .then_with(|| a.sources[0].id.cmp(&b.sources[0].id))
    });
    bins
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Cut {
    pub by_groups: bool,
    pub by_bytes: bool,
}

/// What a pass will do under its limits: the best bins first, each trimmed to fit.
pub fn select(bins: &[Bin], opts: &Options) -> (Vec<Bin>, Cut) {
    let mut out: Vec<Bin> = Vec::new();
    let mut cut = Cut::default();
    let mut groups = 0usize;
    let mut bytes = 0u64;
    for bin in bins {
        let mut sources: Vec<&Candidate> = bin.sources.iter().collect();
        sources.sort_by(|a, b| b.garbage().cmp(&a.garbage()).then_with(|| a.id.cmp(&b.id)));
        let mut taken: Vec<Candidate> = Vec::new();
        for c in sources {
            if groups + taken.len() >= opts.max_groups {
                cut.by_groups = true;
                continue;
            }
            if bytes + taken.iter().map(|t| t.live_bytes).sum::<u64>() + c.live_bytes > opts.max_bytes {
                cut.by_bytes = true;
                continue;
            }
            taken.push(c.clone());
        }
        if taken.is_empty() {
            continue;
        }
        groups += taken.len();
        bytes += taken.iter().map(|t| t.live_bytes).sum::<u64>();
        taken.sort_by(|a, b| a.id.cmp(&b.id));
        out.push(Bin {
            container: bin.container.clone(),
            replicas: bin.replicas.clone(),
            capacity: bin.capacity,
            sources: taken,
        });
    }
    (out, cut)
}

// --- Executing one batch ----------------------------------------------------------------

pub struct Ctx<'a, D: Db> {
    pub db: &'a D,
    pub store: &'a EgroupStore,
    pub node: &'a str,
    pub env: &'a dyn Env,
    pub probe: &'a dyn Probe,
    pub tier_of: &'a dyn Fn(&str) -> Option<Tier>,
    pub now_ms: i64,
}

#[derive(Debug, Default)]
pub struct BinResult {
    pub new_group: String,
    pub new_bytes: u64,
    pub replicas: Vec<String>,
    pub sources: Vec<String>,
    /// Sources that could not be verified and were left out, with why.
    pub unusable: Vec<(String, String)>,
    pub repointed: usize,
    /// Rows a compare-and-swap refused because they had changed since the scan: a guest had
    /// overwritten that extent. The copy of it is dead on arrival and the sweep removes it.
    pub lost_races: usize,
    pub anomalies: Vec<String>,
    /// Set when a statement to Hydra failed part way. Everything before it stands.
    pub stopped: Option<String>,
    pub freed_if_swept: u64,
    /// What the sources occupied, which is what each replica gets back when it drops its copy of
    /// them (a replica holds a whole copy of a group, dead extents included).
    pub source_bytes: u64,
}

struct Placed<'a> {
    src: &'a str,
    old: u32,
    new: u32,
    live: &'a Live,
}

/// Read every live extent of a source group, and prove the group and each extent.
fn read_source<D: Db>(ctx: &Ctx<D>, c: &Candidate) -> Result<Vec<(u32, Vec<u8>)>> {
    if ctx.store.copies(&c.id).len() != 1 {
        return Err(Error::refused(format!(
            "extent group {} is not on exactly one disk of this node (a move may have left a \
             surplus copy for the sweep)",
            c.id
        )));
    }
    let (len, hash) = ctx.store.group_len_and_hash(&c.id)?;
    if !c.seal_hash.is_empty() && hash != c.seal_hash {
        return Err(Error::corrupt(format!(
            "extent group {} hashes {hash}, sealed as {}; not copying from a damaged group \
             (scrub reports it)",
            c.id, c.seal_hash
        )));
    }
    let mut out = Vec::with_capacity(c.extents.len());
    for (offset, live) in &c.extents {
        if *offset as u64 + live.framed() > len {
            return Err(Error::corrupt(format!(
                "extent group {} is {len} bytes, shorter than the map says (extent at {offset})",
                c.id
            )));
        }
        let framed = ctx.store.read_extent_framed(&c.id, *offset, live.length)?;
        let (stored, footer) = framed.split_at(live.length as usize);
        // Every row that points here, not one of them: a clone reads this extent under the
        // parent's identity, and a footer that verifies for one reader and not another is a
        // map that does not mean what it says.
        for r in &live.referrers {
            verify_footer(stored, footer, r.vdisk_hash, r.idx)?;
        }
        out.push((*offset, framed));
    }
    Ok(out)
}

fn is_injected_or_meta(e: &Error) -> bool {
    matches!(e, Error::Meta(_))
}

/// Carry out one batch. `Err` means nothing the map points at was changed; a failure after the
/// first row moved is reported in the result as `stopped`.
pub fn execute_bin<D: Db>(ctx: &Ctx<D>, bin: &Bin, seq: u64) -> Result<BinResult> {
    // 1. Read and verify.
    let mut sources: Vec<(&Candidate, Vec<(u32, Vec<u8>)>)> = Vec::new();
    let mut unusable: Vec<(String, String)> = Vec::new();
    for c in &bin.sources {
        match read_source(ctx, c) {
            Ok(extents) => sources.push((c, extents)),
            Err(e) => unusable.push((c.id.clone(), e.to_string())),
        }
    }
    if sources.is_empty() {
        let why: Vec<String> = unusable.iter().map(|(i, e)| format!("{i}: {e}")).collect();
        return Err(Error::refused(format!(
            "no source group in this batch could be verified ({})",
            why.join("; ")
        )));
    }

    // Lay the new group out.
    let mut bytes: Vec<u8> = Vec::new();
    let mut placed: Vec<Placed> = Vec::new();
    for (c, extents) in &sources {
        for (old, framed) in extents {
            let live = &c.extents.iter().find(|(o, _)| o == old).expect("read from this list").1;
            placed.push(Placed { src: &c.id, old: *old, new: bytes.len() as u32, live });
            bytes.extend_from_slice(framed);
        }
    }
    let total = bytes.len() as u64;
    let hash = seal_of(&bytes);
    // Unique within a process by a counter and across restarts by the clock, so a batch that
    // died half way and a later one never ask for the same id.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let new_id = format!(
        "eg-compact-{:x}-{:x}-{:x}",
        ctx.now_ms.max(0) as u64,
        seq,
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let hint = sources[0].0.hint.clone();

    // 2. Stage locally.
    let slot = ctx.store.slot_preferring((ctx.tier_of)(&bin.container));
    let (temp, _crc) = ctx.store.stage_new(&new_id, slot, &bytes)?;
    ctx.probe.at(Step::Staged)?;
    let abandon = |e: Error| -> Error {
        let _ = std::fs::remove_file(&temp);
        e
    };

    // 3. Replicate, and read back from each.
    let mut offsets: Vec<(u64, usize, usize)> = Vec::with_capacity(placed.len());
    for p in &placed {
        let from = p.new as usize;
        offsets.push((p.new as u64, from, from + p.live.framed() as usize));
    }
    for node in &bin.replicas {
        for (i, (off, from, to)) in offsets.iter().enumerate() {
            let last = i + 1 == offsets.len();
            if let Err(e) = ctx.env.put(node, &new_id, *off, &bytes[*from..*to], !last) {
                return Err(abandon(Error::io(format!(
                    "replica {node} did not take extent group {new_id}: {e}; nothing was published"
                ))));
            }
        }
        match ctx.env.get(node, &new_id, 0, bytes.len()) {
            Ok(back) if back == bytes => {}
            Ok(back) => {
                return Err(abandon(Error::corrupt(format!(
                    "replica {node} returned {} bytes for extent group {new_id} that do not match the \
                     {} that were sent; nothing was published",
                    back.len(),
                    bytes.len()
                ))))
            }
            Err(e) => {
                return Err(abandon(Error::io(format!(
                    "replica {node} could not be read back for extent group {new_id}: {e}; nothing \
                     was published"
                ))))
            }
        }
    }
    ctx.probe.at(Step::Replicated)?;

    // 4. Register, already sealed. Before the rename, so that a crash between leaves a row
    //    the sweep reclaims, not a file nothing names.
    let path = ctx.store.disks()[slot].root.join(format!("{new_id}.eg"));
    let created = ctx.db.cas(
        "/v1/dfs/egroup-create",
        json_params(vec![
            ("egroup_id", json!(new_id)),
            ("state", json!("sealed")),
            ("node", json!(ctx.node)),
            ("path", json!(path.to_string_lossy())),
            ("size", json!(total as i64)),
            ("seal_hash", json!(hash)),
            ("vdisk_hint", json!(hint)),
            ("created_at_ms", json!(ctx.now_ms)),
        ]),
    );
    match created {
        Ok(c) if c.applied => {}
        Ok(_) => {
            return Err(abandon(Error::meta(format!(
                "extent group id {new_id} is already registered; refusing to reuse it"
            ))))
        }
        Err(e) => return Err(abandon(e)),
    }
    ctx.probe.at(Step::Registered)?;

    // 5. Publish by rename, and prove the published file.
    if let Err(e) = ctx.store.stage_publish(&new_id, slot, &temp) {
        return Err(abandon(e));
    }
    ctx.store.stage_switch(&new_id, slot);
    ctx.probe.at(Step::Published)?;
    let (len, again) = ctx.store.group_len_and_hash(&new_id)?;
    if len != total || again != hash {
        return Err(Error::corrupt(format!(
            "extent group {new_id} reads back as {again} ({len} bytes) after publication, not {hash} \
             ({total}); no row was repointed"
        )));
    }
    for p in &placed {
        let r = &p.live.referrers[0];
        ctx.store.read_extent(&new_id, p.new, p.live.length, r.vdisk_hash, r.idx)?;
    }
    ctx.probe.at(Step::Verified)?;

    // 6. Hold the drains of every attached vdisk that points here, then check that what each
    //    has in memory is what the scan saw, before the first row moves.
    let mut held: BTreeMap<String, Box<dyn Hold + '_>> = BTreeMap::new();
    let attached: BTreeSet<&str> = placed
        .iter()
        .flat_map(|p| p.live.referrers.iter().map(|r| r.vdisk.as_str()))
        .filter(|v| ctx.env.attached_here(v))
        .collect();
    for v in attached {
        let h = ctx.env.hold(v).map_err(|e| {
            Error::refused(format!("could not hold the drains of vdisk {v}: {e}; no row was repointed"))
        })?;
        held.insert(v.to_string(), h);
    }
    for p in &placed {
        for r in &p.live.referrers {
            if let Some(h) = held.get(&r.vdisk) {
                if !h.has(r.idx, p.src, p.old, p.live.length) {
                    return Err(Error::refused(format!(
                        "vdisk {} holds a different location for extent {} than the map did when it was \
                         scanned (a drain finished since); no row was repointed",
                        r.vdisk, r.idx
                    )));
                }
            }
        }
    }
    ctx.probe.at(Step::Held)?;

    let mut result = BinResult {
        new_group: new_id.clone(),
        new_bytes: total,
        replicas: bin.replicas.clone(),
        sources: sources.iter().map(|(c, _)| c.id.clone()).collect(),
        unusable,
        freed_if_swept: sources.iter().map(|(c, _)| c.size).sum::<u64>().saturating_sub(total),
        source_bytes: sources.iter().map(|(c, _)| c.size).sum::<u64>(),
        ..BinResult::default()
    };

    // 7. Repoint, one compare-and-swap at a time.
    let mut op = 0usize;
    'all: for p in &placed {
        let mut via: BTreeMap<&str, Vec<&occupancy::Referrer>> = BTreeMap::new();
        let mut direct: Vec<&occupancy::Referrer> = Vec::new();
        for r in &p.live.referrers {
            match &r.extent {
                Some(e) => via.entry(e.as_str()).or_default().push(r),
                None => direct.push(r),
            }
        }
        for r in direct {
            ctx.probe.at(Step::BeforeRepoint(op))?;
            let cas = ctx.db.cas(
                "/v1/dfs/block-map-repoint",
                json_params(vec![
                    ("vdisk_id", json!(r.vdisk)),
                    ("extent_index", json!(r.idx as i64)),
                    ("egroup_id", json!(new_id)),
                    ("egroup_offset", json!(p.new as i64)),
                    ("expected_egroup_id", json!(p.src)),
                    ("expected_egroup_offset", json!(p.old as i64)),
                    ("expected_length", json!(p.live.length as i64)),
                ]),
            );
            match cas {
                Err(e) => {
                    result.stopped = Some(e.to_string());
                    break 'all;
                }
                Ok(c) if c.applied => {
                    result.repointed += 1;
                    if let Some(h) = held.get(&r.vdisk) {
                        if !h.repoint(r.idx, p.src, p.old, p.live.length, &new_id, p.new) {
                            result.anomalies.push(format!(
                                "vdisk {} extent {}: the row moved but the in-memory map did not match",
                                r.vdisk, r.idx
                            ));
                        }
                    }
                }
                Ok(_) => result.lost_races += 1,
            }
            ctx.probe.at(Step::AfterRepoint(op))?;
            op += 1;
        }
        for (extent, referrers) in via {
            ctx.probe.at(Step::BeforeRepoint(op))?;
            let cas = ctx.db.cas(
                "/v1/dfs/extent-repoint",
                json_params(vec![
                    ("extent_id", json!(extent)),
                    ("egroup_id", json!(new_id)),
                    ("egroup_offset", json!(p.new as i64)),
                    ("expected_egroup_id", json!(p.src)),
                    ("expected_egroup_offset", json!(p.old as i64)),
                ]),
            );
            match cas {
                Err(e) => {
                    result.stopped = Some(e.to_string());
                    break 'all;
                }
                Ok(c) if c.applied => {
                    result.repointed += 1;
                    for r in referrers {
                        if let Some(h) = held.get(&r.vdisk) {
                            if !h.repoint(r.idx, p.src, p.old, p.live.length, &new_id, p.new) {
                                result.anomalies.push(format!(
                                    "vdisk {} extent {} (via {extent}): the row moved but the in-memory \
                                     map did not match",
                                    r.vdisk, r.idx
                                ));
                            }
                        }
                    }
                }
                Ok(_) => result.lost_races += 1,
            }
            ctx.probe.at(Step::AfterRepoint(op))?;
            op += 1;
        }
    }
    Ok(result)
}

// --- The pass ---------------------------------------------------------------------------

fn candidate_json(c: &Candidate) -> Value {
    json!({
        "egroup_id": c.id,
        "size_bytes": c.size,
        "live_bytes": c.live_bytes,
        "live_fraction": (c.fraction() * 1000.0).round() / 1000.0,
        "garbage_bytes": c.garbage(),
        "live_extents": c.extents.len(),
        "shared_extents": c.shared_extents(),
        "vdisks": c.vdisks.iter().cloned().collect::<Vec<_>>(),
        "container": c.container,
    })
}

fn bin_json(b: &Bin) -> Value {
    json!({
        "sources": b.sources.iter().map(|c| c.id.clone()).collect::<Vec<_>>(),
        "container": b.container,
        "replicas": b.replicas,
        "bytes_to_copy": b.live_bytes(),
        "source_bytes": b.source_bytes(),
        "freed_here_after_sweep": b.net_freed(),
        // Copies of the new group on peers, and what the peers get back when the sweep has
        // reclaimed the old groups and asked them to drop their copies (D-33). A replica holds a
        // whole copy of each source, so the saving per replica is the sources' size; it is an
        // estimate in that a replica that never held one (the replica set changed since the
        // group was written) frees nothing, and one running a build from before D-33 keeps it.
        "added_on_replicas": b.live_bytes() * b.replicas.len() as u64,
        "freed_on_replicas_after_sweep": b.source_bytes() * b.replicas.len() as u64,
        "net_freed_on_replicas": (b.source_bytes() * b.replicas.len() as u64)
            .saturating_sub(b.live_bytes() * b.replicas.len() as u64),
    })
}

pub fn status_line(r: &Value) -> String {
    let n = |k: &str| r[k].as_u64().unwrap_or(0);
    let pct = (r["threshold"].as_f64().unwrap_or(DEFAULT_THRESHOLD) * 100.0).round();
    let candidates = n("candidate_count");
    let applied = r["applied"].as_bool().unwrap_or(false);
    if !applied {
        if candidates == 0 {
            return format!(
                "compaction: no sealed group is below {pct:.0}% live ({} checked); nothing to do",
                n("sealed_groups")
            );
        }
        return format!(
            "compaction plan: {candidates} group(s) below {pct:.0}% live; this pass would copy {} \
             byte(s) into new group(s) and leave {} byte(s) here and {} on replicas for the \
             sweep; nothing was changed",
            n("selected_bytes_to_copy"),
            r["estimate"]["freed_here_after_sweep"].as_u64().unwrap_or(0),
            r["estimate"]["freed_on_replicas_after_sweep"].as_u64().unwrap_or(0)
        );
    }
    let done = r["executed"].as_array().map(Vec::len).unwrap_or(0);
    let failed = r["failed"].as_array().map(Vec::len).unwrap_or(0);
    format!(
        "compaction: {done} batch(es) done, {} row(s) repointed, {} lost to an overwrite, {failed} \
         failed; the old group(s) are left for the sweep ({} byte(s) here and {} on replicas once \
         it has run twice)",
        n("rows_repointed"),
        n("lost_races"),
        r["estimate"]["freed_here_after_sweep"].as_u64().unwrap_or(0),
        r["estimate"]["freed_on_replicas_after_sweep"].as_u64().unwrap_or(0)
    )
}

/// Plan, and with `opts.apply` carry out, one compaction pass.
#[allow(clippy::too_many_arguments)]
pub fn run<D: Db>(
    db: &D,
    store: &EgroupStore,
    node: &str,
    grace: Duration,
    env: &dyn Env,
    opts: &Options,
    probe: &dyn Probe,
    clock: &dyn Clock,
    tier_of: &dyn Fn(&str) -> Option<Tier>,
    now_ms: i64,
) -> Result<Value> {
    // Order matters here as it does in the sweep: the map first, then what Hydra says this
    // node holds, so a group made between the two reads is young and not misjudged.
    let occ = occupancy::scan(db)?;
    let groups = occupancy::groups_of(db, node)?;
    let vdisks = occupancy::vdisks(db)?;
    let in_flight = env.drain_groups();
    let started = clock.now();

    // The plan reads each candidate's length with a `stat`, not a read of the group.
    let local_len = |id: &str| -> Option<u64> {
        let slot = store.locate(id)?;
        std::fs::metadata(store.disks()[slot].root.join(format!("{id}.eg"))).ok().map(|m| m.len())
    };
    let attached = |v: &str| env.attached_here(v);

    let analysis = analyze(&Inputs {
        node,
        groups: &groups,
        occ: &occ,
        vdisks: &vdisks,
        attached_here: &attached,
        in_flight: &in_flight,
        local_len: &local_len,
        now_ms,
        grace_ms: grace.as_millis() as i64,
        threshold: opts.threshold,
    });
    let bins = pack(analysis.candidates.clone(), opts.target_bytes);
    let (selected, cut) = select(&bins, opts);

    let selected_sources: usize = selected.iter().map(|b| b.sources.len()).sum();
    let selected_bytes: u64 = selected.iter().map(Bin::live_bytes).sum();

    let mut executed: Vec<Value> = Vec::new();
    let mut failed: Vec<Value> = Vec::new();
    let mut lost = 0usize;
    let mut repointed = 0usize;
    let mut freed_if_swept = 0u64;
    let mut added_on_replicas = 0u64;
    let mut freed_on_replicas = 0u64;
    let mut anomalies: Vec<String> = Vec::new();
    let mut stopped_by: Option<&str> = if cut.by_groups {
        Some("max_groups")
    } else if cut.by_bytes {
        Some("max_bytes")
    } else {
        None
    };

    if opts.apply {
        let bucket = RefCell::new(TokenBucket::new(opts.rate, opts.rate.max(1)));
        let deadline = Duration::from_secs(opts.seconds.max(1));
        for (i, bin) in selected.iter().enumerate() {
            if clock.now().saturating_sub(started) >= deadline {
                stopped_by = Some("time");
                break;
            }
            let ctx = Ctx { db, store, node, env, probe, tier_of, now_ms };
            match execute_bin(&ctx, bin, i as u64) {
                Ok(r) => {
                    lost += r.lost_races;
                    repointed += r.repointed;
                    freed_if_swept += r.freed_if_swept;
                    added_on_replicas += r.new_bytes * r.replicas.len() as u64;
                    freed_on_replicas += r.source_bytes * r.replicas.len() as u64;
                    anomalies.extend(r.anomalies.iter().cloned());
                    let stop = r.stopped.clone();
                    executed.push(json!({
                        "new_group": r.new_group,
                        "bytes": r.new_bytes,
                        "sources": r.sources,
                        "replicas": r.replicas,
                        "rows_repointed": r.repointed,
                        "lost_races": r.lost_races,
                        "unusable_sources": r.unusable.iter()
                            .map(|(id, why)| json!({"egroup_id": id, "reason": why})).collect::<Vec<_>>(),
                        "stopped": r.stopped,
                    }));
                    // Pay for the bytes just moved before starting more: what keeps the pass
                    // from being a burst a guest has to wait out.
                    let sent = r.new_bytes * (1 + r.replicas.len() as u64) + r.new_bytes;
                    bucket.borrow_mut().take(clock, sent);
                    if stop.is_some() {
                        stopped_by = Some("error");
                        break;
                    }
                }
                Err(e) => {
                    failed.push(json!({
                        "sources": bin.sources.iter().map(|c| c.id.clone()).collect::<Vec<_>>(),
                        "error": e.to_string(),
                    }));
                    // Hydra unreachable is not a reason to try the next batch; anything else
                    // is about this batch's own sources or replicas.
                    if is_injected_or_meta(&e) {
                        stopped_by = Some("error");
                        break;
                    }
                }
            }
        }
    }

    let skipped_json: Vec<Value> = analysis
        .skipped
        .iter()
        .take(LISTED)
        .map(|(id, why)| json!({"egroup_id": id, "kind": why.kind(), "reason": why.to_string()}))
        .collect();
    let mut by_kind: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, why) in &analysis.skipped {
        *by_kind.entry(why.kind()).or_default() += 1;
    }

    let planned_freed: u64 = selected.iter().map(Bin::net_freed).sum();
    let planned_replica: u64 = selected.iter().map(|b| b.live_bytes() * b.replicas.len() as u64).sum();
    let planned_replica_freed: u64 =
        selected.iter().map(|b| b.source_bytes() * b.replicas.len() as u64).sum();
    let mut report = json!({
        "applied": opts.apply,
        "node": node,
        "threshold": opts.threshold,
        "limits": {
            "max_groups": opts.max_groups, "max_bytes": opts.max_bytes,
            "rate_bytes_per_second": opts.rate, "seconds": opts.seconds,
            "target_group_bytes": opts.target_bytes,
        },
        "sealed_groups": analysis.sealed,
        "block_rows": occ.block_rows,
        "extent_rows": occ.extent_rows_used,
        "healthy_groups": analysis.healthy,
        "wholly_dead_groups": analysis.dead,
        "candidate_count": analysis.candidates.len(),
        "candidates": analysis.candidates.iter().take(LISTED).map(candidate_json).collect::<Vec<_>>(),
        "skipped_count": analysis.skipped.len(),
        "skipped_by_kind": by_kind,
        "skipped": skipped_json,
        "plan": selected.iter().take(LISTED).map(bin_json).collect::<Vec<_>>(),
        "selected_groups": selected_sources,
        "selected_bytes_to_copy": selected_bytes,
        "executed": executed,
        "failed": failed,
        "rows_repointed": repointed,
        "lost_races": lost,
        "anomalies": anomalies,
        "stopped_by": stopped_by,
        "estimate": {
            "freed_here_after_sweep": if opts.apply { freed_if_swept } else { planned_freed },
            "added_on_replicas": if opts.apply { added_on_replicas } else { planned_replica },
            "freed_on_replicas_after_sweep":
                if opts.apply { freed_on_replicas } else { planned_replica_freed },
            "note": "Freed bytes come back on this node when the sweep has seen the old groups \
                     unreferenced on two passes, and on each replica when the sweep then asks it to \
                     drop its copy (D-33). Until that has happened, and for ever on a replica \
                     running a build from before D-33, the replica figure is growth.",
        },
    });
    let line = status_line(&report);
    report["status"] = json!(line);
    Ok(report)
}

// --- Tests ------------------------------------------------------------------------------

#[cfg(test)]
mod tests;
