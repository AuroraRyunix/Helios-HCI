//! A vdisk: the journal, the overlay, the extent map, and the drain that moves bytes
//! from the first to the third.
//!
//! The ordering rules enforced here are the ones that make the whole design defensible,
//! so they are stated once and never departed from:
//!
//! 1. **A write is acknowledged when its journal records are durable -- here and on every
//!    replica -- and not before.** Writes that are in flight together share one local
//!    `fdatasync` and one round trip per replica (group commit), and a write is visible to
//!    readers only once its whole batch is durable everywhere; what is acknowledged is
//!    unchanged. See `commit.rs`.
//! 2. **Extent bytes are durable before any map row points at them.** A crash between
//!    the two leaves orphaned bytes, which Purah sweeps. The reverse ordering
//!    leaves a map pointing at bytes that do not exist, which is data loss.
//! 3. **The journal is not truncated until the map commit has been applied.** A crash
//!    between the two replays records that are already drained, which is idempotent.
//! 4. **Nothing on the guest's write path talks to Hydra.**
//! 5. **The drain runs beside the guest, not in front of it.** A write that crosses the
//!    high-water mark does not wait for the drain: it starts one on a background thread
//!    and is acknowledged on its own journal record. Guests are held back only at the hard
//!    ceiling (twice the high-water mark), where waiting is the alternative to a journal
//!    that grows until the disk is full. See [`DrainGate`] and `write_through`.
//!
//! ## How a drain runs without stopping writes
//!
//! A drain has three phases, and only the first and last hold the vdisk lock:
//!
//! 1. **Plan** (locked, microseconds plus one rename). Rotate the journal: everything
//!    acknowledged so far is now in a sealed segment that nothing will ever append to, and
//!    new writes go to a fresh live segment. Freeze the overlay ranges that point into the
//!    sealed segment and copy out the block-map entries those ranges touch.
//! 2. **Run** (unlocked, seconds). Read the sealed segment, build the new extents, append
//!    them to an extent group, replicate them, sync, write the map rows and make the one
//!    drain-commit CAS in Hydra. Guest writes and reads proceed throughout: reads see the
//!    old map plus the overlay, which still holds every drained range.
//! 3. **Finish** (locked). Apply the new map entries, drop the overlay ranges that still
//!    point into the sealed segment -- the ones a newer write has covered point into the
//!    live segment and stay -- and delete the sealed segment. Rule 3 holds: the segment
//!    is deleted only after Hydra has the new map.
//!
//! The replicas are then told to drop the drained *prefix* of their journals by sequence
//! number, which leaves the records acknowledged while the drain ran.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::err::{Error, Result};
use crate::extent::{vdisk_hash, EgroupStore, OpenEgroup};
use crate::heat::AccessLog;
use crate::journal::{self, Journal, FLAG_COMMIT};
use crate::meta::{
    block_map_batches, cql_str, json_params, now_ms, Daruk, CLASS_IMMUTABLE, CLASS_RW,
    MAP_BATCH as MAP_ROWS_PER_BATCH,
};
use crate::overlay::Overlay;
use crate::peer::{self, PeerClient, Request};

mod commit;
pub use commit::{broken_reason, recover, Commit};

/// Guest writes larger than this become several journal records terminated by one commit
/// marker. Bounded so a single enormous write cannot pin an unbounded buffer.
pub const MAX_RECORD: usize = 1 << 20;

/// Rows per CQL batch when a drain commits its map. Large enough that a big drain is a
/// handful of round trips, small enough to stay clear of Scylla's batch size warnings.

#[derive(Clone, Debug)]
pub struct ExtentLoc {
    pub egroup_id: String,
    pub offset: u32,
    pub length: u32,
    /// The vdisk identity stamped into this extent's footer when it was written.
    ///
    /// Usually this vdisk's own, and for a snapshot or a clone it is the parent's: shared
    /// extents are written once, under whichever vdisk wrote them, and copying the map
    /// does not and must not rewrite them.
    ///
    /// Recorded per row rather than derived from the reader, because the footer check is
    /// "are these the bytes that were written here", and once extents are legitimately
    /// shared the reader's own identity is the wrong thing to compare against. Checking
    /// it against the reader is what made the first snapshot return EIO on every read --
    /// the guard firing correctly on a case that had not existed before.
    pub vdisk_hash: u64,
}

pub struct Vdisk {
    pub id: String,
    pub size: u64,
    pub epoch: u64,
    pub class: String,
    pub extent_bytes: u64,
    pub drain_seq: u64,
    /// How many copies this vdisk was created asking for.
    ///
    /// Carried on the open vdisk purely so `stats()` and the `list` op can report it
    /// beside the replica set, because "how many copies were asked for" and "how many
    /// exist" are different questions and nothing could answer the first one without a
    /// Hydra query of its own. Nothing on the write path reads it: the write-all set is
    /// `map_replicas`, and a number that disagreed with that list would be a durability
    /// claim rather than a durability mechanism.
    pub rf: u64,

    vh: u64,
    node: String,
    journal: Journal,
    overlay: Overlay,
    map: BTreeMap<u64, ExtentLoc>,
    store: Arc<EgroupStore>,
    /// The extent group the next drain appends to. `None` while a drain is running: the
    /// drain owns it for the duration and hands it back at its finish.
    open_eg: Option<OpenEgroup>,
    /// Journal size at which a write starts a background drain.
    high_water: u64,
    /// Journal size at which a write waits for a drain instead of being admitted. A write
    /// that was admitted just under it may take the journal past it by that one write.
    hard_ceiling: u64,
    /// Who is draining, and the means for others to wait on it. Shared with the drain
    /// thread and with whoever is waiting at the ceiling.
    gate: Arc<DrainGate>,
    /// Extent groups a running drain has created or is appending to. They are in no map
    /// until the drain finishes, and Purah's sweep must not read "in no map" as "unused".
    drain_held: Arc<Mutex<HashSet<String>>>,
    daruk: Daruk,
    /// The replica set exactly as the map records it, this node included.
    ///
    /// Distinct from `replicas` below, which holds only the peers this node dials -- a
    /// node does not replicate to itself over TCP. Conflating the two made the heal's
    /// compare-and-swap condition the wrong list, so it never matched and every heal
    /// backed itself out reporting a race that had not happened.
    map_replicas: Vec<String>,
    /// Whether extents sealed by this vdisk are compressed, from its container.
    ///
    /// Read once, when the vdisk is opened. Per-extent it would be a Hydra query on the
    /// drain path; per-cluster it would be the wrong unit, because the trade-off differs
    /// between a container of golden images and one holding a database's data files.
    /// Changing it takes effect the next time the vdisk is attached and never rewrites an
    /// extent group that already exists, which is what makes it safe to change on a live
    /// container.
    compress: bool,
    /// The peers an append must reach before it is acknowledged. Write-all, not quorum:
    /// the takeover proof in ownership.md is three lines *because* fencing one replica
    /// stops the old owner (it needed all of them) and reading one replica sees every
    /// acknowledged write. Quorum buys availability during single-replica loss and costs
    /// exactly that proof.
    replicas: Vec<Arc<PeerClient>>,
    /// Where accesses to extent groups are tallied, shared with every other vdisk on the
    /// node and with the thread that flushes it.
    ///
    /// Node-wide rather than per-vdisk because an extent group is shared: a golden image's
    /// groups are read by every clone of it, and a per-vdisk tally would have to be summed
    /// at flush time anyway to say anything true about the group.
    access: Arc<AccessLog>,
    /// Group commit: the queue of appended writes, the batch being made durable, and the
    /// state that stops appends when a batch failed. See `commit.rs`.
    commit: Arc<commit::Pipeline>,
    /// Set when a drain fails after its bytes are durable. Reads stay correct (the
    /// overlay still holds the newest data), but the journal must not be truncated and
    /// the condition has to be visible rather than retried into silence.
    pub degraded: Option<String>,
}

pub struct VdiskConfig {
    pub root: PathBuf,
    pub node: String,
    pub high_water: u64,
    /// The journal size at which writers wait for a drain. Zero means twice `high_water`.
    pub hard_ceiling: u64,
    /// The node's extent-group access tally. Carried in the config rather than passed
    /// separately so that every path which opens a vdisk gets the same one -- a vdisk
    /// opened with its own fresh tally would be invisible to the flusher, and the extents
    /// it is busiest on would read as the coldest on the node.
    pub access: Arc<AccessLog>,
}

impl Vdisk {
    /// Open a vdisk that already exists in the map. The caller must have won the
    /// ownership CAS first; `epoch` is what it won.
    pub fn open(
        id: &str,
        epoch: u64,
        cfg: &VdiskConfig,
        daruk: Daruk,
        replicas: Vec<Arc<PeerClient>>,
        map_replicas: Vec<String>,
    ) -> Result<Vdisk> {
        let rows = daruk.query(&format!(
            "SELECT vdisk_id, size_bytes, class, epoch, drain_seq, extent_bytes, egroup_bytes, \
             container, rf FROM hydra.dfs_vdisks WHERE vdisk_id = {}",
            cql_str(id)
        ))?;
        let row = rows
            .first()
            .ok_or_else(|| Error::refused(format!("vdisk {id} is not in the map")))?;

        let size = field_u64(row, "size_bytes")?;
        let extent_bytes = field_u64(row, "extent_bytes").unwrap_or(1 << 20).max(4096);
        let egroup_bytes = field_u64(row, "egroup_bytes").unwrap_or(4 << 20).max(extent_bytes);
        let class = row
            .get("class")
            .and_then(Value::as_str)
            .unwrap_or(CLASS_RW)
            .to_string();
        let drain_seq = field_u64(row, "drain_seq").unwrap_or(0);
        let container = row
            .get("container")
            .and_then(Value::as_str)
            .unwrap_or("default")
            .to_string();
        let compress = container_compresses(&daruk, &container);
        // A row written before rf existed has no value here, and one written by the
        // create-time default that this column was added to record has a 1. Neither is
        // worth failing an attach over -- the number is reported, never acted on -- so an
        // unreadable rf opens the vdisk as "one copy asked for", which is what such a row
        // is actually saying.
        let rf = field_u64(row, "rf").unwrap_or(1).max(1);

        let store = EgroupStore::open(
            crate::extent::discover_disks(&cfg.root), egroup_bytes)?
            .preferring(crate::extent::container_tier(&daruk, &container));
        // Proven present each time: attaching onto a journal directory that is really on
        // the root filesystem would acknowledge writes into a file the next mount hides.
        let journal_path = crate::mounts::journal_dir(&cfg.root)?.join(format!("{id}.jrn"));
        let journal = Journal::open(&journal_path)?;

        let mut v = Vdisk {
            id: id.to_string(),
            size,
            epoch,
            class,
            extent_bytes,
            drain_seq,
            rf,
            vh: vdisk_hash(id),
            node: cfg.node.clone(),
            overlay: Overlay::new(),
            map: BTreeMap::new(),
            store: Arc::new(store),
            open_eg: None,
            high_water: cfg.high_water,
            hard_ceiling: if cfg.hard_ceiling > 0 {
                cfg.hard_ceiling
            } else {
                cfg.high_water.saturating_mul(2)
            },
            gate: DrainGate::new(),
            drain_held: Arc::new(Mutex::new(HashSet::new())),
            daruk,
            replicas,
            map_replicas,
            compress,
            access: Arc::clone(&cfg.access),
            commit: commit::Pipeline::new(),
            degraded: None,
            journal,
        };

        v.load_map()?;
        // At ftt=0 the local journal is the only copy there is, so it is authoritative and
        // replayed here. With replicas it is *not*: this node may have owned the vdisk
        // before, in which case its file is a stale history from an earlier ownership
        // while the replicas hold what was actually acknowledged since. Replaying the
        // stale one and then appending to it is how a journal ends up with a sequence
        // hole -- which replay refuses, correctly, but only after the damage is on disk.
        // So with replicas, recovery waits for fence_and_recover().
        if v.replicas.is_empty() {
            let discarded = v.replay_journal()?;
            if discarded > 0 {
                eprintln!(
                    "sidon: vdisk {id}: discarded {discarded} bytes of unacknowledged journal tail"
                );
            }
        }
        Ok(v)
    }

    fn load_map(&mut self) -> Result<()> {
        let rows = self.daruk.query(&format!(
            "SELECT extent_index, egroup_id, egroup_offset, length, vdisk_hash FROM hydra.dfs_block_map \
             WHERE vdisk_id = {}",
            cql_str(&self.id)
        ))?;
        // Rows that name an extent instead of a group (D-23). None exist until something
        // writes one, and a vdisk with none issues no statement beyond the one above.
        let mut by_extent: Vec<u64> = Vec::new();
        for row in rows {
            let idx = field_u64(&row, "extent_index")?;
            let egroup_id = match row.get("egroup_id").and_then(Value::as_str) {
                Some(g) => g.to_string(),
                None => {
                    by_extent.push(idx);
                    continue;
                }
            };
            let offset = field_u64(&row, "egroup_offset")? as u32;
            let length = field_u64(&row, "length")? as u32;
            // Absent on rows written before the column existed. Those extents were
            // written by the vdisk that owns the row, so its own hash is the right
            // answer and the migration needs no backfill.
            // i64 on the wire; the round trip through two's complement is exact.
            let vh = row
                .get("vdisk_hash")
                .and_then(Value::as_i64)
                .map(|v| v as u64)
                .unwrap_or(self.vh);
            self.map.insert(idx, ExtentLoc { egroup_id, offset, length, vdisk_hash: vh });
        }
        if !by_extent.is_empty() {
            for (idx, r) in crate::extent_resolve::resolve(&self.daruk, &self.id, &by_extent)? {
                self.map.insert(
                    idx,
                    ExtentLoc {
                        egroup_id: r.egroup_id,
                        offset: r.offset,
                        length: r.length,
                        vdisk_hash: r.vdisk_hash.unwrap_or(self.vh),
                    },
                );
            }
        }
        Ok(())
    }

    /// Rebuild the overlay from the journal. Only complete, commit-terminated groups are
    /// applied: a trailing group without its marker is a write that was still in flight
    /// when the daemon died, and I-2 permits discarding it.
    fn replay_journal(&mut self) -> Result<u64> {
        let (records, discarded) = self.journal.replay()?;
        let mut pending: Vec<(u64, u32, u64)> = Vec::new();
        let mut applied = 0usize;
        for r in &records {
            pending.push((r.offset, r.data_len, r.data_pos));
            if r.flags & FLAG_COMMIT != 0 {
                for (off, len, pos) in pending.drain(..) {
                    self.overlay.insert(off, len, pos);
                    applied += 1;
                }
            }
        }
        if !pending.is_empty() {
            eprintln!(
                "sidon: vdisk {}: {} journal record(s) had no commit marker and were not applied",
                self.id,
                pending.len()
            );
        }
        if applied > 0 {
            eprintln!("sidon: vdisk {}: replayed {applied} journal record(s)", self.id);
        }
        Ok(discarded)
    }

    /// Read `len` bytes at `offset`. Extent store first, overlay on top: the overlay is
    /// by construction newer than anything drained.
    pub fn read(&mut self, offset: u64, len: u32) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(len as u64)
            .ok_or_else(|| Error::refused("read offset overflows".to_string()))?;
        if end > self.size {
            return Err(Error::refused(format!(
                "read {offset}+{len} runs past the end of vdisk {} ({} bytes)",
                self.id, self.size
            )));
        }

        // Unwritten ranges read as zeroes, which is what a sparse disk promises.
        let mut buf = vec![0u8; len as usize];

        let first = offset / self.extent_bytes;
        let last = (end - 1) / self.extent_bytes;
        // One clock read per guest read, not one per extent. The timestamp is what makes a
        // total readable as a rate and tells a tiering pass how long ago the last access
        // was; a few hundred microseconds of skew across the extents of one read is far
        // below the resolution any of that is used at.
        let seen_at = now_ms();
        for idx in first..=last {
            let loc = match self.map.get(&idx) {
                Some(l) => l.clone(),
                None => continue,
            };
            let extent = match self
                .store
                .read_extent(&loc.egroup_id, loc.offset, loc.length, loc.vdisk_hash, idx)
            {
                Ok(bytes) => bytes,
                Err(local) => {
                    // The local copy is damaged or missing. Ask a replica before giving up:
                    // this is the read-repair path, and it is the difference between one
                    // rotted extent costing a byte range and costing the disk. A replica's
                    // answer is verified against the same footer, so a second bad copy is
                    // refused too rather than quietly replacing a first.
                    match self.read_extent_from_replica(&loc, idx) {
                        Some(bytes) => {
                            eprintln!(
                                "sidon: vdisk {}: extent {idx} unreadable locally ({local}); \
                                 served from a replica",
                                self.id
                            );
                            bytes
                        }
                        None => return Err(local),
                    }
                }
            };
            // Counted here, after the bytes are in hand and before they are copied out, so
            // a read that failed on every copy is not recorded as an access to a group
            // nobody could read. A read served from a replica *is* one: the group was
            // wanted on this node, which is exactly the fact a placement decision needs.
            //
            // Four integer adds under a mutex held across no syscall. That is the entire
            // cost this adds to the read path, and it is the reason the tally is in memory
            // rather than in Hydra.
            self.access.record_read(&loc.egroup_id, extent.len() as u64, seen_at);
            let ext_start = idx * self.extent_bytes;
            let copy_start = offset.max(ext_start);
            let copy_end = end.min(ext_start + extent.len() as u64);
            if copy_end <= copy_start {
                continue;
            }
            let src = (copy_start - ext_start) as usize;
            let dst = (copy_start - offset) as usize;
            let n = (copy_end - copy_start) as usize;
            buf[dst..dst + n].copy_from_slice(&extent[src..src + n]);
        }

        for seg in self.overlay.overlapping(offset, end) {
            let copy_start = offset.max(seg.start);
            let copy_end = end.min(seg.end());
            if copy_end <= copy_start {
                continue;
            }
            let skip = copy_start - seg.start;
            let n = (copy_end - copy_start) as usize;
            let data = self.journal.read_at(seg.data_pos + skip, n)?;
            let dst = (copy_start - offset) as usize;
            buf[dst..dst + n].copy_from_slice(&data);
        }

        Ok(buf)
    }

    /// Record that a drain failed, and stop starting new ones.
    ///
    /// The writes it was draining are acknowledged and still readable from the overlay, and
    /// the journal still holds them: nothing is lost. What would be lost by carrying on is
    /// the operator's chance to notice -- retrying forever against a full disk or an
    /// unreachable Hydra is how a vdisk looks healthy until the journal volume fills.
    fn note_drain_failure(&mut self, e: &Error) {
        eprintln!("sidon: vdisk {}: drain failed: {e}", self.id);
        self.degraded = Some(e.to_string());
    }

    /// Whether the journal has reached the size at which a drain should be running.
    fn drain_wanted(&self) -> bool {
        self.journal.len() >= self.high_water && self.degraded.is_none()
    }

    /// May a write be taken now, or must it wait for a drain to make room?
    fn admit(&self) -> Admit {
        if self.journal.len() < self.hard_ceiling {
            return Admit::Go;
        }
        if self.gate.running() {
            return Admit::Wait;
        }
        if let Some(why) = &self.degraded {
            // Nothing is going to make room. Blocking would hang the guest on a condition
            // that never clears; admitting would grow the journal until the volume is full
            // and every vdisk on the node stops. An error now is the honest outcome, and it
            // clears itself the moment the cause does (a heal clears `degraded`).
            return Admit::Refuse(Error::io(format!(
                "vdisk {} has {} MiB of undrained journal, at its ceiling, and cannot drain: \
                 {why}. Refusing the write rather than growing the journal without bound.",
                self.id,
                self.journal.len() >> 20
            )));
        }
        Admit::StartDrain
    }

    /// Start a background drain if one is wanted and none is running. The caller holds the
    /// lock this vdisk is behind, which is what makes "none is running" stay true until the
    /// thread is spawned.
    fn kick(&self, handle: &Arc<Mutex<Vdisk>>) {
        if !self.drain_wanted() || !self.gate.try_begin() {
            return;
        }
        let handle = Arc::clone(handle);
        let gate = Arc::clone(&self.gate);
        let name = format!("drain-{}", self.id);
        if let Err(e) = std::thread::Builder::new()
            .name(name)
            .spawn(move || run_background_drain(handle, gate))
        {
            self.gate.finish();
            eprintln!("sidon: vdisk {}: could not start a drain thread: {e}", self.id);
        }
    }

    /// Which replicas are answering, and which are not.
    pub fn replica_health(&self) -> (Vec<String>, Vec<String>) {
        let mut up = Vec::new();
        let mut down = Vec::new();
        for replica in &self.replicas {
            match replica.ping() {
                Ok(()) => up.push(replica.node.clone()),
                Err(_) => down.push(replica.node.clone()),
            }
        }
        (up, down)
    }

    /// Bring a new replica up to date, then start writing to it.
    ///
    /// Order matters and is the opposite of the obvious one. The new node joins the
    /// write-all set **first**, so every append from this moment reaches it; only then is
    /// the history backfilled. Backfilling first and joining after leaves a window where
    /// a write lands on the old set and not the new member, and nothing afterwards would
    /// notice the hole -- the backfill has already run.
    ///
    /// A crash midway leaves a node holding a partial copy that the map does not list.
    /// That is garbage, not damage: nothing reads a replica the map does not name, and
    /// Purah sweeps what is left.
    pub fn add_replica(&mut self, client: Arc<PeerClient>) -> Result<usize> {
        if self.replicas.iter().any(|r| r.node == client.node) {
            return Ok(0);
        }
        // A drain in flight replicates its new extents to the set it was planned with. A
        // member that joined after that would be listed by the map as holding extents it
        // never received, which is a durability claim nothing could later notice was false.
        // The caller waits for the drain first (`lock_idle`); this is the backstop.
        if self.gate.running() {
            return Err(Error::refused(format!(
                "vdisk {} is draining; a replica cannot join until the drain has finished",
                self.id
            )));
        }
        // A batch in flight was sent to the set as it was when its records were appended. The
        // caller waits for the pipeline to empty (`lock_idle`); this is the backstop.
        if self.commit.outstanding() != 0 {
            return Err(Error::refused(format!(
                "vdisk {} has writes in flight; a replica cannot join until they are durable",
                self.id
            )));
        }
        let node = client.node.clone();
        self.replicas.push(client);

        // Every extent the map currently points at. Read locally and pushed as-is, so the
        // new copy is byte-identical rather than re-framed.
        let mut copied = 0usize;
        let entries: Vec<(u64, ExtentLoc)> =
            self.map.iter().map(|(i, l)| (*i, l.clone())).collect();
        for (idx, loc) in entries {
            let framed = match self.store.read_extent_framed(&loc.egroup_id, loc.offset, loc.length)
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    // A local extent that cannot be read is not something to paper over by
                    // silently shipping a shorter set. Undo the join and report it.
                    self.replicas.retain(|r| r.node != node);
                    return Err(Error::corrupt(format!(
                        "cannot re-replicate {}: extent {idx} is unreadable here ({e})",
                        self.id
                    )));
                }
            };
            self.replicate_extent_to(&node, &loc.egroup_id, loc.offset as u64, &framed)?;
            copied += 1;
        }

        // Then the journal: everything acknowledged but not yet drained.
        let journal = self.journal.read_all()?;
        if !journal.is_empty() {
            let replica = self
                .replicas
                .iter()
                .find(|r| r.node == node)
                .expect("just pushed")
                .clone();
            let resp = replica.call(&Request {
                opcode: peer::OP_APPEND,
                vdisk: self.id.clone(),
                epoch: self.epoch,
                seq: 0,
                offset: 0,
                flags: 0,
                data: journal,
            })?;
            if !resp.is_ok() {
                self.replicas.retain(|r| r.node != node);
                return Err(Error::io(format!(
                    "replica {node} refused the journal backfill for {} with status {}",
                    self.id, resp.status
                )));
            }
        }
        Ok(copied)
    }

    /// Drop a replica from the write-all set.
    ///
    /// Only ever after the map has been updated: a set that is narrower in memory than in
    /// the map means acknowledged writes are not reaching a node the map claims has them,
    /// which is a durability lie rather than a degraded state.
    pub fn remove_replica(&mut self, node: &str) {
        self.replicas.retain(|r| r.node != node);
    }

    /// The replica set as the map records it -- what a compare-and-swap on it must be
    /// conditioned against.
    pub fn map_replicas(&self) -> Vec<String> {
        self.map_replicas.clone()
    }

    /// Record a new set after the map has accepted it, so the two do not drift.
    pub fn set_map_replicas(&mut self, nodes: Vec<String>) {
        self.map_replicas = nodes;
        // Healed. Left set, the watcher would re-heal on every tick forever, and an
        // operator reading status would see a disk reported broken that is not. Not while
        // the commit pipeline is broken, though: a new member set does not make the replicas
        // that missed a batch consistent, and `recover` clears the flag once they are.
        if self.commit.broken().is_none() {
            self.degraded = None;
        }
    }

    fn replicate_extent_to(
        &self,
        node: &str,
        egroup_id: &str,
        offset: u64,
        framed: &[u8],
    ) -> Result<()> {
        let replica = self
            .replicas
            .iter()
            .find(|r| r.node == node)
            .ok_or_else(|| Error::refused(format!("{node} is not a replica of {}", self.id)))?;
        let resp = replica.call(&Request {
            opcode: peer::OP_EGROUP_PUT,
            vdisk: egroup_id.to_string(),
            epoch: self.epoch,
            seq: 0,
            offset,
            flags: 0,
            data: framed.to_vec(),
        })?;
        if !resp.is_ok() {
            return Err(Error::io(format!(
                "replica {node} refused extent group {egroup_id} with status {}",
                resp.status
            )));
        }
        Ok(())
    }

    /// Fetch one extent from whichever replica still has a good copy.
    ///
    /// Returns None when no replica could supply one that passes its footer, which keeps
    /// the caller's original local error as the thing reported -- "a replica also failed"
    /// is less useful to an operator than what went wrong here.
    fn read_extent_from_replica(&self, loc: &ExtentLoc, idx: u64) -> Option<Vec<u8>> {
        for replica in &self.replicas {
            let resp = match replica.call(&Request {
                opcode: peer::OP_EGROUP_GET,
                vdisk: loc.egroup_id.clone(),
                epoch: self.epoch,
                seq: (loc.length as usize + crate::extent::FOOTER_LEN) as u64,
                offset: loc.offset as u64,
                flags: 0,
                data: Vec::new(),
            }) {
                Ok(r) if r.is_ok() => r,
                _ => continue,
            };
            if resp.data.len() < loc.length as usize + crate::extent::FOOTER_LEN {
                continue;
            }
            let (data, footer) = resp.data.split_at(loc.length as usize);
            // Verified exactly as a local read is. A replica is not more trustworthy for
            // being remote, and accepting its bytes unchecked would turn one damaged copy
            // into a silently propagated one.
            if crate::extent::verify_footer(data, footer, loc.vdisk_hash, idx).is_ok() {
                // Expanded here, not by the replica: what travels between nodes is the
                // extent exactly as stored, so a repair copy stays a byte copy and the
                // footer that was checked is the footer that was written.
                return crate::extent::decode_extent(data, footer).ok();
            }
            eprintln!(
                "sidon: vdisk {}: replica {} also has a damaged copy of extent {idx}",
                self.id, replica.node
            );
        }
        None
    }

    /// Note that this vdisk cannot currently satisfy write-all, and why.
    ///
    /// Separate from the append path's error return because the two audiences differ: the
    /// guest gets EIO and can do nothing about it, while the curator needs a durable flag
    /// it can find on its next pass. Without this a node loss stops writes and nothing
    /// notices until the heal timer fires minutes later.
    pub fn mark_degraded(&mut self, detail: String) {
        if self.degraded.is_none() {
            eprintln!("sidon: vdisk {} is degraded: {detail}", self.id);
            self.degraded = Some(detail);
        }
    }

    /// Fence every reachable replica at this vdisk's epoch, and rebuild from one of them.
    ///
    /// Step 2 and 3 of the takeover in ownership.md. Every *reachable* replica is fenced
    /// so that step 3 can read any of them and so returning replicas rejoin already
    /// fenced; safety needs only one to have taken, because an append needs all of them.
    pub fn fence_and_recover(&mut self, fence_clients: &[Arc<PeerClient>]) -> Result<usize> {
        if fence_clients.is_empty() {
            return Ok(0);
        }
        // Fence every replica at once, not one after another.
        //
        // Found by testing: with serial fencing a takeover waits the full per-peer
        // timeout for each unreachable replica, so failing over away from a wedged host
        // in a three-replica set took twenty seconds before it did anything. HA has a
        // time budget and that spends all of it. Safety is unaffected either way -- an
        // append needs *every* replica, so fencing one is enough to stop the old owner --
        // which is exactly why the slow ones can be waited on in parallel and then
        // ignored.
        let mut handles = Vec::with_capacity(self.replicas.len());
        for replica in fence_clients {
            let replica = Arc::clone(replica);
            let vdisk = self.id.clone();
            let epoch = self.epoch;
            handles.push(std::thread::spawn(move || {
                let outcome = replica.call(&Request {
                    opcode: peer::OP_FENCE,
                    vdisk,
                    epoch,
                    seq: 0,
                    offset: 0,
                    flags: 0,
                    data: Vec::new(),
                });
                (replica, outcome)
            }));
        }

        let mut fenced = Vec::new();
        let mut unreachable = Vec::new();
        for handle in handles {
            match handle.join() {
                Ok((replica, Ok(resp))) if resp.is_ok() => fenced.push(replica),
                Ok((replica, Ok(resp))) => {
                    unreachable.push(format!("{} (status {})", replica.node, resp.status))
                }
                Ok((replica, Err(e))) => unreachable.push(format!("{}: {e}", replica.node)),
                // A panicked fence thread is not a fenced replica. Saying so beats
                // treating a crash as a success.
                Err(_) => unreachable.push("a fence thread panicked".to_string()),
            }
        }
        if fenced.is_empty() {
            return Err(Error::refused(format!(
                "no replica of {} could be fenced ({}), so the previous owner cannot be \
                 shown to have stopped writing",
                self.id,
                unreachable.join("; ")
            )));
        }
        if !unreachable.is_empty() {
            eprintln!(
                "sidon: vdisk {}: fenced {} replica(s); could not reach {}. Safe -- an \
                 append needs all of them -- but those will be fenced when they return.",
                self.id, fenced.len(), unreachable.join("; ")
            );
        }

        // Step 3: read the journal tail from the replicas just fenced. By write-all each
        // of them holds every acknowledged write, so any one is a complete history -- but
        // take the longest, because a replica that died mid-append has a torn tail and a
        // shorter file. They agree on every byte they share; only the end can differ.
        let mut best: Option<(String, Vec<u8>)> = None;
        for replica in &fenced {
            let resp = match replica.call(&Request {
                opcode: peer::OP_READ_TAIL,
                vdisk: self.id.clone(),
                epoch: self.epoch,
                seq: 0,
                offset: 0,
                flags: 0,
                data: Vec::new(),
            }) {
                Ok(r) if r.is_ok() => r,
                _ => continue,
            };
            let longer = best.as_ref().map(|(_, d)| resp.data.len() > d.len()).unwrap_or(true);
            if longer {
                best = Some((replica.node.clone(), resp.data));
            }
        }

        // Adopt it unconditionally, even when it is shorter than the local file or empty.
        // "Shorter than what is here" is precisely the stale-previous-ownership case: this
        // node's own journal is not evidence of anything once another node has owned the
        // disk, and an empty tail means the last owner drained everything, which is a fact
        // and not a failure to recover.
        let (from, tail) = match best {
            Some(v) => v,
            None => {
                return Err(Error::refused(format!(
                    "vdisk {} was fenced but no replica would return its journal, so the \
                     acknowledged history cannot be established",
                    self.id
                )))
            }
        };
        let bytes = tail.len();
        if self.commit.outstanding() != 0 {
            return Err(Error::refused(format!(
                "vdisk {} has writes in flight; its journal cannot be replaced now",
                self.id
            )));
        }
        self.journal.replace(&tail)?;
        self.overlay.clear();
        let discarded = self.replay_journal()?;
        eprintln!(
            "sidon: vdisk {}: adopted {bytes} byte(s) of journal from replica {from} \
             ({discarded} discarded as a torn tail)",
            self.id
        );
        Ok(fenced.len())
    }

    /// Whether anything is waiting to be drained: ranges in the overlay, or a sealed
    /// segment that a drain which failed left behind.
    pub fn needs_drain(&self) -> bool {
        !self.overlay.is_empty() || self.journal.has_sealed()
    }

    /// Drain everything, now, on this thread, and return with the journal empty.
    ///
    /// This is the synchronous drain that detach, seal, snapshot and flush rely on to hand
    /// back a fully drained vdisk, and it holds the vdisk lock throughout, exactly as the
    /// drain used to. It refuses if a background drain is running, because that one is
    /// moving the same journal; callers go through [`drain_all`], which waits for it first.
    ///
    /// It takes at most two rounds: one for a sealed segment a failed background drain left
    /// behind, one for the live segment. A vdisk it returns for is drained in the strong
    /// sense -- no overlay ranges, no sealed segment, an empty live one -- because nothing
    /// can append while this holds the lock.
    pub fn drain(&mut self) -> Result<()> {
        if self.gate.running() {
            return Err(Error::refused(format!(
                "vdisk {} has a drain running; wait for it before draining synchronously",
                self.id
            )));
        }
        for _ in 0..4 {
            let mut plan = match self.plan_drain()? {
                Some(p) => p,
                None => return Ok(()),
            };
            let outcome = plan.job.run();
            self.finish_drain(plan.sealed_gen, plan.job, outcome)?;
            truncate_replicas(&plan.replicas, &self.id, self.epoch, plan.keep_seq);
            if !self.journal.has_sealed() && self.journal.live_len() == 0 {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Phase one of a drain, under the lock: seal the journal, freeze what is to be moved,
    /// and hand it all to a job that can run without this vdisk.
    ///
    /// If a sealed segment is already waiting -- a drain failed after rotating, or the
    /// daemon died mid-drain and replay found both files -- that segment is what gets
    /// drained, and no second rotation is made. The frozen view is then whatever of its
    /// ranges is still newest, which is exactly what has to reach the extents: a range a
    /// later write has covered is that write's to move, in a later drain.
    fn plan_drain(&mut self) -> Result<Option<DrainPlan>> {
        if !self.journal.has_sealed() {
            if self.journal.live_len() == 0 {
                return Ok(None);
            }
            // Rotation moves the file a ticket's records are in. Callers hold the lock
            // from `lock_quiet`, so this is the backstop: a ticket outstanding here would
            // publish an overlay position into a segment the drain is about to delete.
            if self.commit.outstanding() != 0 {
                return Err(Error::refused(format!(
                    "vdisk {} has writes in flight; its journal cannot be rotated now",
                    self.id
                )));
            }
            self.journal.rotate()?;
        }
        let (sealed_gen, src) = self.journal.sealed().expect("rotated, or already sealed");
        let keep_seq = self.journal.live_first_seq();
        let frozen = self.overlay.filtered(|s| journal::gen_of(s.data_pos) == sealed_gen);

        let mut touched: Vec<u64> = Vec::new();
        for seg in frozen.iter() {
            let first = seg.start / self.extent_bytes;
            let last = (seg.end() - 1) / self.extent_bytes;
            for idx in first..=last {
                if touched.last() != Some(&idx) {
                    touched.push(idx);
                }
            }
        }
        // Segments come in ascending order and never overlap, so the list is sorted already
        // and only the same extent appearing twice in a row needs dropping -- which the
        // check above did. Dedup anyway: the guarantee is cheap and the cost of being wrong
        // is an extent written twice in one drain.
        touched.sort_unstable();
        touched.dedup();

        let locs: HashMap<u64, ExtentLoc> = touched
            .iter()
            .filter_map(|i| self.map.get(i).map(|l| (*i, l.clone())))
            .collect();

        if let Some(eg) = &self.open_eg {
            self.drain_held.lock().expect("held mutex poisoned").insert(eg.id.clone());
        }
        let replicas = self.replicas.clone();
        let job = DrainJob {
            id: self.id.clone(),
            node: self.node.clone(),
            epoch: self.epoch,
            vh: self.vh,
            compress: self.compress,
            extent_bytes: self.extent_bytes,
            size: self.size,
            store: Arc::clone(&self.store),
            daruk: self.daruk.clone(),
            replicas: replicas.clone(),
            access: Arc::clone(&self.access),
            src,
            src_gen: sealed_gen,
            frozen,
            indices: touched,
            locs,
            open_eg: self.open_eg.take(),
            drain_seq: self.drain_seq,
            held: Arc::clone(&self.drain_held),
            id_seed: self.journal.next_seq(),
            groups_made: 0,
            t_replicate: Duration::ZERO,
            t_local: Duration::ZERO,
            t_hydra: Duration::ZERO,
            pipe: ExtentPipe::none(),
        };
        Ok(Some(DrainPlan { job, replicas, keep_seq, sealed_gen }))
    }

    /// Phase three of a drain, under the lock: take what the job committed into the in-memory
    /// state, and let the journal forget it.
    ///
    /// On failure nothing is applied and nothing is forgotten. The sealed segment stays, the
    /// overlay still holds every range, and reads are unaffected -- the next drain starts
    /// from the same segment.
    fn finish_drain(
        &mut self,
        sealed_gen: u64,
        job: DrainJob,
        outcome: Result<Committed>,
    ) -> Result<()> {
        // The open group comes back whether the drain worked or not: extents already
        // appended to it by a drain that then failed are unreferenced garbage, and the next
        // drain appends after them.
        self.open_eg = job.open_eg;
        let result = match outcome {
            Err(e) => Err(e),
            Ok(done) => {
                for (idx, loc) in done.locs {
                    self.map.insert(idx, loc);
                }
                self.drain_seq = done.next_seq;
                for id in &done.sealed {
                    eprintln!("sidon: vdisk {}: sealed extent group {id}", self.id);
                }
                // Rule 3: only now may the journal forget -- here and, once the caller has
                // told them, on every replica. The ranges that go are those still pointing
                // into the sealed segment. A range a newer write covered points into the
                // live segment and stays: it is newer than anything the drain wrote.
                self.overlay.remove_where(|s| journal::gen_of(s.data_pos) == sealed_gen);
                if let Err(e) = self.journal.discard_sealed() {
                    // The map is committed and the overlay has let go of the segment, so
                    // nothing depends on the file any more. If it stays, replay will apply
                    // records that are already drained, which is idempotent; and the next
                    // drain finds it, plans nothing for it, and removes it again.
                    eprintln!(
                        "sidon: vdisk {}: could not remove the drained journal segment: {e}",
                        self.id
                    );
                }
                Ok(())
            }
        };
        // Whatever the outcome the job holds nothing now: groups it made are in the map (or
        // are garbage), and the open one is back in `open_eg`, which `held_egroups` reads.
        self.drain_held.lock().expect("held mutex poisoned").clear();
        result
    }

    /// Called at detach. Drains what is left so a clean shutdown leaves an empty journal.
    pub fn close(&mut self) -> Result<()> {
        if !self.needs_drain() {
            return Ok(());
        }
        self.drain()
    }

    /// Every extent group this vdisk is using right now: everything its map points at,
    /// plus the open group the next drain will append to.
    ///
    /// The sweep needs this because Hydra can be a moment behind the owner: a drain that
    /// has just repointed an extent leaves the previous group unreferenced in a stale
    /// read while this vdisk still has the new one only in memory.
    pub fn held_egroups(&self) -> HashSet<String> {
        let mut held: HashSet<String> =
            self.map.values().map(|l| l.egroup_id.clone()).collect();
        if let Some(eg) = &self.open_eg {
            held.insert(eg.id.clone());
        }
        // The groups a running drain has made or is appending to: in no map yet, and the
        // open group is out of `open_eg` while the drain has it.
        held.extend(self.drain_held.lock().expect("held mutex poisoned").iter().cloned());
        held
    }

    /// The extent groups an in-flight drain is making: the open group and any it created.
    ///
    /// Narrower than [`held_egroups`](Self::held_egroups), which also names everything the
    /// map points at. Compaction wants exactly these and not the others: a group a drain is
    /// still filling has bytes that no map row names *yet*, and the extents in it that look
    /// dead are about to be live.
    pub fn drain_groups(&self) -> HashSet<String> {
        let mut out: HashSet<String> =
            self.drain_held.lock().expect("held mutex poisoned").iter().cloned().collect();
        if let Some(eg) = &self.open_eg {
            out.insert(eg.id.clone());
        }
        out
    }

    /// Keep any drain of this vdisk from starting, without taking the vdisk lock.
    ///
    /// Compaction rewrites map rows of a vdisk that is attached and being written, and a
    /// drain writes the same rows with plain, unconditional statements. A compare-and-swap
    /// that lands between a drain's read of a row and its write of that row is the one
    /// interleaving the map's single-writer rule (`metadata.md` section 3) does not survive,
    /// so while the rows are rewritten there must be no drain of this vdisk in flight and
    /// none able to begin. This is the existing drain gate used as that exclusion: the same
    /// flag every drain sets, so `kick` and the background drain see a drain "running" and
    /// stand aside, and `drain_all` waits for it exactly as it waits for a real one.
    ///
    /// `None` when a drain is already running, in which case the caller leaves the vdisk
    /// alone and tries again another day. Guest writes are *not* held: they go to the
    /// journal as ever, and at the hard ceiling they wait for the gate to open, which is
    /// seconds at most because a hold covers a handful of metadata writes.
    pub fn try_hold_drains(&self) -> Option<DrainHold> {
        if self.gate.try_begin() {
            Some(DrainHold { gate: Arc::clone(&self.gate) })
        } else {
            None
        }
    }

    /// What this vdisk's in-memory map says about one extent.
    pub fn map_entry(&self, idx: u64) -> Option<ExtentLoc> {
        self.map.get(&idx).cloned()
    }

    /// Point one extent at a new copy of the same bytes, if it still points where the
    /// caller believes.
    ///
    /// The in-memory half of a compaction repoint, made after Hydra has accepted the row.
    /// Compare-and-set: an entry that no longer matches `expect` is left alone and `false`
    /// comes back, because a map entry that moved on is newer than anything the caller knew.
    /// Only the location changes. The footer was copied with the extent, so the identity it
    /// is read against (`vdisk_hash`) and the stored length are the same.
    pub fn repoint_extent(
        &mut self,
        idx: u64,
        expect: (&str, u32, u32),
        to_group: &str,
        to_offset: u32,
    ) -> bool {
        match self.map.get_mut(&idx) {
            Some(loc)
                if loc.egroup_id == expect.0 && loc.offset == expect.1 && loc.length == expect.2 =>
            {
                loc.egroup_id = to_group.to_string();
                loc.offset = to_offset;
                true
            }
            _ => false,
        }
    }

    pub fn stats(&self) -> Value {
        json!({
            "vdisk_id": self.id,
            "size_bytes": self.size,
            "class": self.class,
            "epoch": self.epoch,
            "drain_seq": self.drain_seq,
            "extent_bytes": self.extent_bytes,
            "journal_bytes": self.journal.len(),
            "draining": self.gate.running(),
            "high_water": self.high_water,
            "hard_ceiling": self.hard_ceiling,
            "overlay_segments": self.overlay.len(),
            "mapped_extents": self.map.len(),
            // The set as the map records it, this node included -- not `self.replicas`,
            // which is the peers this node dials and is therefore one short. Reporting
            // the dialled list under this name is how a reader counting replicas against
            // the redundancy factor concludes every vdisk is one copy down.
            "replicas": self.map_replicas.clone(),
            // Beside the set, never instead of it. `rf` is what was asked for and
            // `replicas` is what exists, and a reader that has only one of the two cannot
            // tell a vdisk that is short of its copies from one that never asked for any.
            "rf": self.rf,
            "peers": self.replicas.iter().map(|r| r.node.clone()).collect::<Vec<_>>(),
            "degraded": self.degraded,
            "commit": self.commit.stats(),
        })
    }
}

// ---------------------------------------------------------------------------------
// The drain, and who waits for it.
// ---------------------------------------------------------------------------------

/// How many extents of a drain may be queued for a replica's thread at once. Bounds the
/// memory a drain holds in flight while leaving room for it to build the next extent while
/// the last is on the wire.
const PIPELINE_DEPTH: usize = 4;

/// How long a write waits at the hard ceiling for a drain to make room before it gives up
/// and fails. Long enough to ride out a slow drain (a 128 MiB journal is seconds), short
/// enough that a wedged one surfaces as a guest I/O error instead of a hung VM.
const STALL_LIMIT: Duration = Duration::from_secs(120);

/// Whether a drain is running for one vdisk, and a way to wait for it to stop.
///
/// A flag and a counter under one mutex and one condition variable. The flag is what makes
/// "start a drain" idempotent -- a hundred writes crossing the high-water mark in the same
/// moment start one drain, not a hundred -- and what `drain` and `add_replica` check before
/// touching a journal a background drain is moving. The counter exists so that a waiter can
/// say "wake me when a drain *after this one* finishes" without missing a drain that
/// finished between its check and its wait.
pub struct DrainGate {
    state: Mutex<GateState>,
    cv: Condvar,
}

#[derive(Default)]
struct GateState {
    running: bool,
    finished: u64,
}

impl DrainGate {
    fn new() -> Arc<DrainGate> {
        Arc::new(DrainGate { state: Mutex::new(GateState::default()), cv: Condvar::new() })
    }

    /// Claim the right to run a drain. False if one is already running.
    fn try_begin(&self) -> bool {
        let mut s = self.state.lock().expect("gate mutex poisoned");
        if s.running {
            return false;
        }
        s.running = true;
        true
    }

    fn finish(&self) {
        let mut s = self.state.lock().expect("gate mutex poisoned");
        s.running = false;
        s.finished += 1;
        self.cv.notify_all();
    }

    pub fn running(&self) -> bool {
        self.state.lock().expect("gate mutex poisoned").running
    }

    fn finished(&self) -> u64 {
        self.state.lock().expect("gate mutex poisoned").finished
    }

    /// Block until no drain is running.
    fn wait_idle(&self) {
        let mut s = self.state.lock().expect("gate mutex poisoned");
        while s.running {
            s = self.cv.wait(s).expect("gate mutex poisoned");
        }
    }

    /// Block until a drain has finished since `seen`, or `timeout` passes.
    fn wait_progress(&self, seen: u64, timeout: Duration) {
        let s = self.state.lock().expect("gate mutex poisoned");
        if s.finished != seen {
            return;
        }
        let _ = self.cv.wait_timeout(s, timeout).expect("gate mutex poisoned");
    }
}

/// A reservation of a vdisk's drain gate, released when it is dropped. See
/// [`Vdisk::try_hold_drains`].
pub struct DrainHold {
    gate: Arc<DrainGate>,
}

impl Drop for DrainHold {
    fn drop(&mut self) {
        self.gate.finish();
    }
}

/// What a writer is told when it asks to write.
enum Admit {
    Go,
    /// At the ceiling, with no drain running and none failing: start one, then wait.
    StartDrain,
    /// At the ceiling with a drain running: wait for it.
    Wait,
    /// At the ceiling and nothing will make room.
    Refuse(Error),
}

/// Everything phase two needs, owned, so that it can run with no reference to the vdisk.
struct DrainJob {
    id: String,
    node: String,
    epoch: u64,
    vh: u64,
    compress: bool,
    extent_bytes: u64,
    size: u64,
    store: Arc<EgroupStore>,
    daruk: Daruk,
    /// The write-all set as it was when the drain was planned. A replica cannot join while
    /// a drain runs (`add_replica` refuses), so this is also the set at its finish.
    replicas: Vec<Arc<PeerClient>>,
    access: Arc<AccessLog>,
    /// The sealed journal segment, read by position, and the generation its positions carry.
    src: Arc<File>,
    src_gen: u64,
    /// The overlay ranges that point into the sealed segment, as they were at the plan.
    frozen: Overlay,
    /// The extents those ranges touch, ascending.
    indices: Vec<u64>,
    /// What the block map says about each of them, as it was at the plan.
    locs: HashMap<u64, ExtentLoc>,
    open_eg: Option<OpenEgroup>,
    drain_seq: u64,
    held: Arc<Mutex<HashSet<String>>>,
    id_seed: u64,
    groups_made: u64,
    /// Where the time went, for the one log line a drain writes. A drain that is slow is
    /// slow for one of three reasons (the replicas, this disk, or Hydra) and which one is
    /// the whole of the diagnosis.
    t_replicate: Duration,
    t_local: Duration,
    t_hydra: Duration,
    /// The threads that ship this drain's extents to the replicas. Empty until `run` starts
    /// them, and dropping it ends them.
    pipe: ExtentPipe,
}

/// Ships a drain's extents to the replicas on threads of their own, in order, so that the
/// drain can read and build the next extent while the last one is on the wire.
///
/// One thread and one bounded queue per replica. A replica's puts are therefore applied in
/// the order the drain made them, over one connection, which is the order a group is
/// appended in. Nothing about *when the drain may proceed* changes: it asks `flush` for the
/// moment every replica has answered everything sent so far, and does so before it tells
/// Hydra a group is sealed and before it writes a single map row.
struct ExtentPipe {
    senders: Vec<std::sync::mpsc::SyncSender<PipeMsg>>,
    /// The first failure any replica reported. Later puts to a failed replica are skipped
    /// rather than attempted, since the drain is already lost.
    failure: Arc<Mutex<Option<Error>>>,
    failed: Arc<AtomicBool>,
}

enum PipeMsg {
    Put { group: String, offset: u64, framed: Arc<Vec<u8>>, defer: bool },
    /// Answered once every message before it has been handled.
    Barrier(std::sync::mpsc::SyncSender<()>),
}

impl ExtentPipe {
    fn none() -> ExtentPipe {
        ExtentPipe {
            senders: Vec::new(),
            failure: Arc::new(Mutex::new(None)),
            failed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn new(replicas: &[Arc<PeerClient>], epoch: u64) -> ExtentPipe {
        let mut pipe = ExtentPipe::none();
        for replica in replicas {
            let (tx, rx) = std::sync::mpsc::sync_channel::<PipeMsg>(PIPELINE_DEPTH);
            pipe.senders.push(tx);
            let replica = Arc::clone(replica);
            let failure = Arc::clone(&pipe.failure);
            let failed = Arc::clone(&pipe.failed);
            std::thread::spawn(move || {
                for msg in rx {
                    let (group, offset, framed, defer) = match msg {
                        PipeMsg::Barrier(done) => {
                            let _ = done.send(());
                            continue;
                        }
                        PipeMsg::Put { group, offset, framed, defer } => (group, offset, framed, defer),
                    };
                    if failed.load(Ordering::SeqCst) {
                        continue;
                    }
                    let outcome = replica.call(&Request {
                        opcode: peer::OP_EGROUP_PUT,
                        vdisk: group.clone(),
                        epoch,
                        seq: 0,
                        offset,
                        flags: if defer { peer::APPEND_DEFER_SYNC } else { 0 },
                        data: framed.to_vec(),
                    });
                    let err = match outcome {
                        Err(e) => Some(e),
                        Ok(resp) if !resp.is_ok() => Some(Error::io(format!(
                            "replica {} refused extent group {group} at offset {offset} \
                             with status {}",
                            replica.node, resp.status
                        ))),
                        Ok(_) => None,
                    };
                    if let Some(e) = err {
                        let mut slot = failure.lock().expect("pipe mutex poisoned");
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                        failed.store(true, Ordering::SeqCst);
                    }
                }
            });
        }
        pipe
    }

    /// The first failure, if any replica has reported one.
    fn check(&self) -> Result<()> {
        if self.failed.load(Ordering::SeqCst) {
            let e = self.failure.lock().expect("pipe mutex poisoned").take();
            return Err(e.unwrap_or_else(|| {
                Error::io("a replica refused an extent earlier in this drain".to_string())
            }));
        }
        Ok(())
    }

    /// Queue one extent for every replica. Blocks only when a replica is `PIPELINE_DEPTH`
    /// extents behind, which is the drain running ahead of the network.
    fn put(&self, group: &str, offset: u64, framed: Vec<u8>, defer: bool) -> Result<()> {
        self.check()?;
        let framed = Arc::new(framed);
        for tx in &self.senders {
            tx.send(PipeMsg::Put {
                group: group.to_string(),
                offset,
                framed: Arc::clone(&framed),
                defer,
            })
            .map_err(|_| Error::io("a replication thread ended unexpectedly".to_string()))?;
        }
        Ok(())
    }

    /// Wait until every replica has answered everything sent so far; the first failure if any.
    fn flush(&self) -> Result<()> {
        for tx in &self.senders {
            let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
            tx.send(PipeMsg::Barrier(done_tx))
                .map_err(|_| Error::io("a replication thread ended unexpectedly".to_string()))?;
            done_rx
                .recv()
                .map_err(|_| Error::io("a replication thread ended unexpectedly".to_string()))?;
        }
        self.check()
    }
}

/// What a plan hands back: the job, and what the caller needs after it.
struct DrainPlan {
    job: DrainJob,
    replicas: Vec<Arc<PeerClient>>,
    /// First sequence number to keep when telling replicas to drop their drained prefix.
    keep_seq: u64,
    sealed_gen: u64,
}

/// A drain that committed: the map entries to apply and the counter it advanced to.
struct Committed {
    locs: Vec<(u64, ExtentLoc)>,
    sealed: Vec<String>,
    next_seq: u64,
}

impl DrainJob {
    fn extent_len(&self, index: u64) -> u64 {
        let start = index * self.extent_bytes;
        if start >= self.size {
            0
        } else {
            self.extent_bytes.min(self.size - start)
        }
    }

    /// Phase two: move the frozen ranges into extent groups and commit the map.
    ///
    /// Read-modify-write per extent, then redirect-on-write: the extent's current bytes
    /// are read, the frozen ranges are applied on top, and the result is appended somewhere
    /// new. The old location becomes garbage rather than being overwritten, because a
    /// sealed egroup is immutable and that is what makes repair and snapshots cheap.
    fn run(&mut self) -> Result<Committed> {
        let indices = std::mem::take(&mut self.indices);
        // Nothing the map needs to hear about: the sealed segment held no committed write
        // (a failed write's records, say). There is nothing to repoint, so no Hydra round
        // trip, and the counter does not move.
        if indices.is_empty() {
            return Ok(Committed { locs: Vec::new(), sealed: Vec::new(), next_seq: self.drain_seq });
        }

        let started = Instant::now();
        // The extent whose write ends the drain: its group's last write is synced on the
        // replicas whether or not the group is full.
        let last_idx = indices.iter().rev().find(|i| self.extent_len(**i) > 0).copied();
        self.pipe = ExtentPipe::new(&self.replicas, self.epoch);
        let mut new_rows: Vec<(u64, String, u32, u32, u64)> = Vec::with_capacity(indices.len());
        let mut new_locs: Vec<(u64, ExtentLoc)> = Vec::with_capacity(indices.len());
        let mut sealed: Vec<String> = Vec::new();

        for idx in indices {
            let ext_len = self.extent_len(idx) as usize;
            if ext_len == 0 {
                continue;
            }
            let ext_start = idx * self.extent_bytes;

            // Start from what is already stored, so a partial overwrite keeps the bytes
            // it did not touch.
            let mut buf = vec![0u8; ext_len];
            if let Some(loc) = self.locs.get(&idx) {
                let cur = self
                    .store
                    .read_extent(&loc.egroup_id, loc.offset, loc.length, loc.vdisk_hash, idx)?;
                let n = cur.len().min(ext_len);
                buf[..n].copy_from_slice(&cur[..n]);
            }

            for seg in self.frozen.overlapping(ext_start, ext_start + ext_len as u64) {
                let copy_start = ext_start.max(seg.start);
                let copy_end = (ext_start + ext_len as u64).min(seg.end());
                if copy_end <= copy_start {
                    continue;
                }
                if journal::gen_of(seg.data_pos) != self.src_gen {
                    return Err(Error::corrupt(format!(
                        "vdisk {}: a drain was handed a range from segment {} but reads segment {}",
                        self.id,
                        journal::gen_of(seg.data_pos),
                        self.src_gen
                    )));
                }
                let skip = copy_start - seg.start;
                let n = (copy_end - copy_start) as usize;
                let data = journal::read_in(&self.src, seg.data_pos + skip, n)?;
                let dst = (copy_start - ext_start) as usize;
                buf[dst..dst + n].copy_from_slice(&data);
            }

            let eg_id = self.ensure_open_egroup()?;
            let (offset, stored_len, framed) = {
                let store = &self.store;
                let eg = self.open_eg.as_mut().expect("ensure_open_egroup set it");
                store.append_framed(eg, &buf, self.vh, idx, self.compress)?
            };
            // The same bytes to every replica, extent plus footer. Without this a drained
            // extent exists once: the journal is replicated, so an un-drained write
            // survives a node loss, and draining it would *reduce* its durability. Data
            // that becomes less safe by being tidied up is not a tidy-up.
            //
            // Handed to a thread per replica rather than sent here, so the next extent is
            // read and built while this one is on the wire and on the replica's disk. The
            // writes to one group may skip their fsync but its last: that one flushes the
            // file, and nothing is committed until `replicas_caught_up` has seen every reply.
            let full = self
                .open_eg
                .as_ref()
                .map(|eg| self.store.is_full(eg))
                .unwrap_or(false);
            let t = Instant::now();
            self.pipe.put(&eg_id, offset as u64, framed, !(full || Some(idx) == last_idx))?;
            self.t_replicate += t.elapsed();
            // An append into this extent group, which is the only kind of write an extent
            // group ever takes -- the guest's write reached the journal and was
            // acknowledged there. So a group's write count is a count of drained extents
            // landing in it, which is the number that identifies the groups a tiering pass
            // must leave alone because they are still being appended to.
            self.access.record_write(&eg_id, stored_len as u64, now_ms());
            // The stored length, not the logical one: a read seeks by this, and a
            // compressed extent is not the size the guest thinks it wrote.
            new_rows.push((idx, eg_id.clone(), offset, stored_len, self.vh));
            new_locs.push((idx, ExtentLoc {
                egroup_id: eg_id,
                offset,
                length: stored_len,
                // Written here, so stamped with this vdisk's identity. A clone
                // that overwrites a shared extent lands here and takes its own,
                // which is why the hash belongs to the row and not to the vdisk.
                vdisk_hash: self.vh,
            }));

            if full {
                // The group is complete here and must be complete on every replica before
                // Hydra is told it is sealed. Flush this disk's copy first, while the
                // replicas are still taking theirs, and only then wait for them.
                let t = Instant::now();
                if let Some(eg) = self.open_eg.as_mut() {
                    self.store.sync(eg)?;
                }
                self.t_local += t.elapsed();
                self.replicas_caught_up()?;
                sealed.push(self.seal_open_egroup()?);
            }
        }

        // Rule 2: bytes durable before any row points at them -- here, with the replicas
        // still working through the tail of what they were sent, and then there.
        let t = Instant::now();
        if let Some(eg) = self.open_eg.as_mut() {
            self.store.sync(eg)?;
        }
        self.t_local += t.elapsed();
        self.replicas_caught_up()?;

        let t = Instant::now();
        for batch in block_map_batches(&self.id, self.epoch, &new_rows, MAP_ROWS_PER_BATCH) {
            self.daruk.query(&batch)?;
        }

        // The one lightweight transaction per drain. Conditioned on the epoch as well as
        // the counter, so a deposed owner cannot land a batch into a map that moved on.
        let next = self.drain_seq + 1;
        let cas = self.daruk.cas(
            "/v1/dfs/drain-commit",
            json_params(vec![
                ("vdisk_id", json!(self.id)),
                ("drain_seq", json!(next)),
                ("expected_drain_seq", json!(self.drain_seq)),
                ("expected_epoch", json!(self.epoch)),
            ]),
        )?;
        if !cas.applied {
            return Err(Error::refused(format!(
                "drain refused for vdisk {}: the map is at epoch {} drain_seq {}, this owner \
                 holds epoch {} drain_seq {}. This node no longer owns the disk.",
                self.id,
                cas.current_i64("epoch").unwrap_or(-1),
                cas.current_i64("drain_seq").unwrap_or(-1),
                self.epoch,
                self.drain_seq
            )));
        }
        self.t_hydra += t.elapsed();
        eprintln!(
            "sidon: vdisk {}: drained {} extent(s) in {} ms (replicas {} ms, this disk {} ms, \
             hydra {} ms)",
            self.id,
            new_locs.len(),
            started.elapsed().as_millis(),
            self.t_replicate.as_millis(),
            self.t_local.as_millis(),
            self.t_hydra.as_millis()
        );
        // Done with the replicas: let their threads end now rather than when the job drops.
        self.pipe = ExtentPipe::none();
        Ok(Committed { locs: new_locs, sealed, next_seq: next })
    }

    /// Wait until every replica has taken every extent sent so far, or report the first
    /// that did not. The point after which the replicas hold what the map is about to say
    /// they hold.
    fn replicas_caught_up(&mut self) -> Result<()> {
        let t = Instant::now();
        let r = self.pipe.flush();
        self.t_replicate += t.elapsed();
        r
    }

    fn ensure_open_egroup(&mut self) -> Result<String> {
        if let Some(eg) = &self.open_eg {
            if !self.store.is_full(eg) {
                return Ok(eg.id.clone());
            }
            self.replicas_caught_up()?;
            let id = self.seal_open_egroup()?;
            eprintln!("sidon: vdisk {}: sealed extent group {id}", self.id);
        }
        self.groups_made += 1;
        let id = format!(
            "eg-{}-{:x}",
            &self.id,
            now_ms() as u64 ^ self.id_seed ^ (self.groups_made << 48)
        );
        let t = Instant::now();
        let eg = self.store.create(&id)?;
        self.t_local += t.elapsed();
        // Held from the moment the group exists, before Hydra hears of it: the sweep reads
        // Hydra, and a group that is in no map and not in this set looks like an orphan.
        self.held.lock().expect("held mutex poisoned").insert(id.clone());
        let t = Instant::now();
        let cas = self.daruk.cas(
            "/v1/dfs/egroup-create",
            json_params(vec![
                ("egroup_id", json!(id)),
                ("state", json!("open")),
                ("node", json!(self.node)),
                ("path", json!(self.store.path_for(&id).to_string_lossy())),
                ("size", json!(0)),
                ("vdisk_hint", json!(self.id)),
                ("created_at_ms", json!(now_ms())),
            ]),
        )?;
        self.t_hydra += t.elapsed();
        if !cas.applied {
            return Err(Error::meta(format!(
                "extent group id {id} is already registered; refusing to reuse it"
            )));
        }
        self.open_eg = Some(eg);
        Ok(id)
    }

    fn seal_open_egroup(&mut self) -> Result<String> {
        let mut eg = self.open_eg.take().expect("caller checked");
        let t = Instant::now();
        self.store.sync(&mut eg)?;
        let hash = self.store.seal_hash(&eg.id)?;
        self.t_local += t.elapsed();
        let t = Instant::now();
        let cas = self.daruk.cas(
            "/v1/dfs/egroup-state",
            json_params(vec![
                ("egroup_id", json!(eg.id)),
                ("state", json!("sealed")),
                ("seal_hash", json!(hash)),
                ("size", json!(eg.size as i64)),
                ("expected_state", json!("open")),
            ]),
        )?;
        self.t_hydra += t.elapsed();
        if !cas.applied {
            return Err(Error::meta(format!(
                "extent group {} could not be sealed: it is in state {}",
                eg.id,
                cas.current_str("state")
            )));
        }
        Ok(eg.id)
    }
}

/// Tell every replica the drained prefix of its journal may go: records older than
/// `keep_seq`. Best effort -- a failure wastes disk on a replica and endangers nothing, because
/// the map already points at the drained extents and replaying a drained record is idempotent.
///
/// A replica running a build from before this opcode answers "refused" and keeps its whole
/// journal, which is the safe way for it to be wrong: it never empties a journal that holds
/// writes acknowledged while the drain ran.
fn truncate_replicas(replicas: &[Arc<PeerClient>], vdisk: &str, epoch: u64, keep_seq: u64) {
    for replica in replicas {
        match replica.call(&Request {
            opcode: peer::OP_TRUNCATE_TO,
            vdisk: vdisk.to_string(),
            epoch,
            seq: keep_seq,
            offset: 0,
            flags: 0,
            data: Vec::new(),
        }) {
            Ok(resp) if resp.is_ok() => {}
            Ok(resp) => eprintln!(
                "sidon: vdisk {vdisk}: replica {} did not drop its drained journal (status {})",
                replica.node, resp.status
            ),
            Err(e) => eprintln!(
                "sidon: vdisk {vdisk}: replica {} did not drop its drained journal: {e}",
                replica.node
            ),
        }
    }
}

/// The body of the drain thread. The gate was claimed by whoever spawned it, and is released
/// when this returns for any reason -- including a panic, which would otherwise leave every
/// writer at the ceiling waiting for a drain that no longer exists.
fn run_background_drain(handle: Arc<Mutex<Vdisk>>, gate: Arc<DrainGate>) {
    struct Release(Arc<DrainGate>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.finish();
        }
    }
    let _release = Release(gate);

    loop {
        let mut plan = {
            // No write may be between its append and its commit when the journal rotates:
            // `lock_quiet` returns with the lock held and nothing in flight.
            let mut v = lock_quiet(&handle);
            match v.plan_drain() {
                Ok(Some(p)) => p,
                Ok(None) => return,
                Err(e) => {
                    v.note_drain_failure(&e);
                    return;
                }
            }
        };

        // The long part, with no lock held: guest reads and writes carry on.
        let outcome = plan.job.run();

        let (again, committed) = {
            let mut v = handle.lock().expect("vdisk mutex poisoned");
            match v.finish_drain(plan.sealed_gen, plan.job, outcome) {
                Ok(()) => (v.drain_wanted(), true),
                Err(e) => {
                    v.note_drain_failure(&e);
                    (false, false)
                }
            }
        };
        if committed {
            // Outside the lock: a round trip per replica, and the replica rewrites the tail.
            // Safe there because the cut is by sequence number, so a record appended
            // meanwhile is simply after it.
            let (id, epoch) = {
                let v = handle.lock().expect("vdisk mutex poisoned");
                (v.id.clone(), v.epoch)
            };
            truncate_replicas(&plan.replicas, &id, epoch, plan.keep_seq);
        }
        if !again {
            return;
        }
    }
}

/// Lock a vdisk with no write between its append and its commit.
///
/// Rotating or replacing the journal moves the file that appended-but-uncommitted records
/// are in, and an overlay position that names a segment a drain then deletes reads garbage.
/// So the drain's plan step, a heal, a seal and a snapshot all start from here: new appends
/// are paused, the pipeline is waited on *without* the vdisk lock (the leader needs it to
/// publish), and the lock is taken again once it is empty. Returns with the lock held, and
/// since only a holder of the lock can append, it stays empty until the guard is dropped.
/// The wait is bounded by one flush, because nothing new joins the queue while it is paused.
pub fn lock_quiet(handle: &Arc<Mutex<Vdisk>>) -> MutexGuard<'_, Vdisk> {
    lock_when(handle, false)
}

/// Lock a vdisk once no background drain is running on it, and nothing is in flight.
///
/// Waits on the gate *without* the lock, because the drain needs the lock for its first and
/// last phases and a waiter holding it would be waiting on itself. Returns with the lock
/// held and the gate idle, and since a drain can only be started by a holder of the lock,
/// idle stays true until the guard is dropped.
pub fn lock_idle(handle: &Arc<Mutex<Vdisk>>) -> MutexGuard<'_, Vdisk> {
    lock_when(handle, true)
}

fn lock_when(handle: &Arc<Mutex<Vdisk>>, drain_idle: bool) -> MutexGuard<'_, Vdisk> {
    // Whether this call is holding appends off. It never does while waiting for a drain: the
    // drain may itself be waiting to rotate, and holding appends off for the length of a
    // drain would be the stall the background drain exists to avoid.
    let mut paused: Option<Arc<commit::Pipeline>> = None;
    loop {
        let v = handle.lock().expect("vdisk mutex poisoned");
        if drain_idle && v.gate.running() {
            if let Some(p) = paused.take() {
                p.unpause();
            }
            let gate = Arc::clone(&v.gate);
            drop(v);
            gate.wait_idle();
            continue;
        }
        if v.commit.outstanding() == 0 {
            if let Some(p) = paused.take() {
                p.unpause();
            }
            return v;
        }
        let pipe = Arc::clone(&v.commit);
        if paused.is_none() {
            pipe.pause();
            paused = Some(Arc::clone(&pipe));
        }
        drop(v);
        pipe.wait_empty(handle);
    }
}

/// Drain a vdisk completely, waiting for any background drain first. What detach, seal,
/// snapshot and flush call when they need a drained vdisk: it returns with the overlay
/// empty and the journal empty, or with the error that stopped it.
pub fn drain_all(handle: &Arc<Mutex<Vdisk>>) -> Result<()> {
    lock_idle(handle).close()
}

/// Append a guest write and queue it for the next commit, without waiting for it.
///
/// The write's records are in the journal, in the order writes were submitted, when this
/// returns; they are neither durable nor visible until [`Commit::wait`] returns Ok. A caller
/// that submits several writes before waiting on any gets them batched under one
/// `fdatasync` and one round trip per replica.
///
/// Admitted immediately unless the journal is at its hard ceiling, in which case it waits --
/// without holding the vdisk lock, so reads and the drain itself carry on -- for a drain to
/// make room. If the journal has passed the high-water mark, a background drain is started;
/// the write does not wait for it.
pub fn submit_write(handle: &Arc<Mutex<Vdisk>>, offset: u64, data: &[u8]) -> Result<Commit> {
    let deadline = Instant::now() + STALL_LIMIT;
    loop {
        let mut v = handle.lock().expect("vdisk mutex poisoned");
        match v.admit() {
            Admit::Go => match v.begin_append(offset, data)? {
                commit::Append::Queued(ticket) => {
                    let pipe = Arc::clone(&v.commit);
                    v.kick(handle);
                    drop(v);
                    return Ok(Commit::queued(pipe, Arc::clone(handle), ticket));
                }
                commit::Append::Nothing => return Ok(Commit::done()),
                commit::Append::Paused(pipe) => {
                    drop(v);
                    pipe.wait_unpaused();
                    continue;
                }
            },
            Admit::StartDrain => v.kick(handle),
            Admit::Wait => {}
            Admit::Refuse(e) => return Err(e),
        }
        let gate = Arc::clone(&v.gate);
        let seen = gate.finished();
        drop(v);
        if Instant::now() >= deadline {
            return Err(Error::io(format!(
                "timed out after {}s waiting for the journal to drain",
                STALL_LIMIT.as_secs()
            )));
        }
        gate.wait_progress(seen, Duration::from_millis(200));
    }
}

/// A guest write to a vdisk: [`submit_write`], then wait until it is durable on every copy.
pub fn write_through(handle: &Arc<Mutex<Vdisk>>, offset: u64, data: &[u8]) -> Result<()> {
    submit_write(handle, offset, data)?.wait()
}

/// NBD's flush: return once every write submitted before this call has been committed.
///
/// A write is acknowledged only when it is durable on every copy, so a flush has nothing of
/// its own to make durable -- what it adds is the *barrier*: it does not return while an
/// earlier write is still between its append and its commit, which is what a guest that
/// issued the flush behind its writes (without waiting for their replies) means by it.
/// Writes submitted after the call are not waited for. On a vdisk whose pipeline has
/// stopped taking writes it is an error, because nothing can be made durable now.
pub fn flush_through(handle: &Arc<Mutex<Vdisk>>) -> Result<()> {
    let (pipe, id) = {
        let v = handle.lock().expect("vdisk mutex poisoned");
        (Arc::clone(&v.commit), v.id.clone())
    };
    let mark = pipe.barrier();
    pipe.wait_below(handle, mark);
    match broken_reason_of(&pipe) {
        Some(why) => Err(Error::io(format!(
            "vdisk {id} cannot make writes durable: an earlier commit failed ({why})"
        ))),
        None => Ok(()),
    }
}

fn broken_reason_of(pipe: &commit::Pipeline) -> Option<String> {
    pipe.broken().map(|b| b.why().to_string())
}

/// `write_zeroes` in the same terms as [`write_through`]: a chunk at a time, each admitted
/// on its own, so a large trim cannot take the journal past the ceiling in one call.
pub fn write_zeroes_through(handle: &Arc<Mutex<Vdisk>>, offset: u64, len: u64) -> Result<()> {
    // A few chunks in flight at once so that their commits are shared; the first error is
    // reported once everything submitted has finished.
    const WINDOW: usize = 8;
    let mut remaining = len;
    let mut at = offset;
    let zeros = vec![0u8; MAX_RECORD];
    let mut pending: std::collections::VecDeque<Commit> = std::collections::VecDeque::new();
    let mut first: Option<Error> = None;
    while remaining > 0 && first.is_none() {
        let n = (MAX_RECORD as u64).min(remaining) as usize;
        match submit_write(handle, at, &zeros[..n]) {
            Ok(c) => pending.push_back(c),
            Err(e) => first = Some(e),
        }
        at += n as u64;
        remaining -= n as u64;
        while pending.len() >= WINDOW || (first.is_some() && !pending.is_empty()) {
            if let Err(e) = pending.pop_front().expect("non-empty").wait() {
                first.get_or_insert(e);
            }
        }
    }
    for c in pending {
        if let Err(e) = c.wait() {
            first.get_or_insert(e);
        }
    }
    match first {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Whether a container asks for its extents to be compressed.
///
/// Fails soft, deliberately. A container row that is missing, unreadable, or written by an
/// older schema means "not configured for compression", which is what every container did
/// before this existed. Refusing to open a vdisk because a *preference* could not be read
/// would turn a cosmetic gap into an outage, and the vdisk is perfectly serviceable either
/// way -- the footer records what each extent actually is, so a container that flips
/// between settings stays readable in both directions.
fn container_compresses(daruk: &Daruk, container: &str) -> bool {
    let rows = match daruk.query(&format!(
        "SELECT compression FROM hydra.storage_containers WHERE name = {}",
        cql_str(container)
    )) {
        Ok(rows) => rows,
        Err(_) => return false,
    };
    rows.first()
        .and_then(|r| r.get("compression"))
        .and_then(Value::as_str)
        .map(|s| {
            let s = s.trim().to_ascii_lowercase();
            s == "lz4" || s == "on" || s == "true"
        })
        .unwrap_or(false)
}

/// The fault tolerance a container asks its vdisks to survive, if it says.
///
/// `None` covers every way the answer can be absent -- no such container, an unreadable
/// Hydra, a row from before the column existed, a null -- and the caller falls back to
/// the cluster's own setting rather than to a number invented here. That distinction
/// matters more than it looks: a container whose ftt reads 0 has *chosen* not to
/// replicate, and collapsing that into "unknown" would quietly overrule an operator who
/// asked for a single copy on purpose.
///
/// Note the unit. This is a count of failures to survive, not a count of copies, which is
/// the same thing `cluster.json`'s `redundancy_factor` means and one less than the number
/// `dfs_vdisks.rf` holds. See `copies_for_ftt` in control.rs, where the conversion lives.
pub(crate) fn container_ftt(daruk: &Daruk, container: &str) -> Option<u64> {
    let rows = daruk
        .query(&format!(
            "SELECT ftt FROM hydra.storage_containers WHERE name = {}",
            cql_str(container)
        ))
        .ok()?;
    match rows.first()?.get("ftt") {
        Some(Value::Number(n)) if n.is_i64() => n.as_i64().filter(|v| *v >= 0).map(|v| v as u64),
        Some(Value::Number(n)) => n.as_u64(),
        _ => None,
    }
}

pub(crate) fn field_u64(row: &Value, name: &str) -> Result<u64> {
    match row.get(name) {
        Some(Value::Number(n)) if n.is_i64() => Ok(n.as_i64().unwrap_or(0).max(0) as u64),
        Some(Value::Number(n)) if n.is_u64() => Ok(n.as_u64().unwrap_or(0)),
        Some(Value::String(s)) => s
            .parse::<u64>()
            .map_err(|_| Error::meta(format!("column '{name}' is not a number: {s:?}"))),
        _ => Err(Error::meta(format!("row is missing numeric column '{name}'"))),
    }
}

#[cfg(test)]
mod tests {
    //! The vdisk with real files, real replication sockets and a stand-in for Hydra.
    //!
    //! Nothing here mocks the thing under test. The journal is a file on disk, the replicas
    //! are `ReplicaStore`s behind a real TCP listener speaking the real framing (with a hook
    //! that can refuse, stall or record), and Hydra is a small HTTP server that answers
    //! Daruk's requests from memory and can be told to hold or refuse the drain commit.
    //! Holding the commit is what makes the interesting states reachable on purpose: a drain
    //! that is *running* is a drain stopped at its commit, and the test can look around.

    use super::*;
    use crate::peer::{ReplicaStore, Response};
    use std::io::{Read, Write};
    use std::path::Path;
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const MIB: usize = 1 << 20;

    mod group_commit;

    fn tmpdir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("sidon-vdisk-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// Wait for `cond`, failing the test rather than hanging it.
    fn wait_until(what: &str, cond: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Run `f` on its own thread and fail the test if it has not returned in `secs` --
    /// which is how "the write waited for the drain" shows up when the drain is held.
    fn within<T: Send + 'static>(secs: u64, f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(Duration::from_secs(secs)).expect("timed out: the call did not return")
    }

    // ---- Hydra, in memory ---------------------------------------------------------

    #[derive(Default)]
    struct HydraState {
        /// What Hydra was asked, in order: "batch" for a block-map write, "commit" for an
        /// applied drain-commit, "commit-refused" for one it declined.
        log: Mutex<Vec<String>>,
        fail_commit: AtomicBool,
        fail_batch: AtomicBool,
        hold: Mutex<bool>,
        cv: Condvar,
        arrived: AtomicUsize,
        /// The journal whose sealed segment is probed on every commit request.
        probe: Mutex<Option<PathBuf>>,
        sealed_at_commit: Mutex<Vec<bool>>,
    }

    struct Hydra {
        addr: String,
        st: Arc<HydraState>,
    }

    impl Hydra {
        fn start() -> Hydra {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let st = Arc::new(HydraState::default());
            let st2 = Arc::clone(&st);
            std::thread::spawn(move || {
                for conn in listener.incoming() {
                    let Ok(mut s) = conn else { break };
                    let st = Arc::clone(&st2);
                    std::thread::spawn(move || {
                        // Request line and headers, then the body by Content-Length.
                        let mut head = Vec::new();
                        let mut byte = [0u8; 1];
                        while !head.ends_with(b"\r\n\r\n") {
                            if s.read(&mut byte).unwrap_or(0) == 0 {
                                return;
                            }
                            head.push(byte[0]);
                        }
                        let text = String::from_utf8_lossy(&head).to_string();
                        let path = text.split_whitespace().nth(1).unwrap_or("").to_string();
                        let len: usize = text
                            .lines()
                            .find_map(|l| {
                                let l = l.to_ascii_lowercase();
                                l.strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        let mut body = vec![0u8; len];
                        s.read_exact(&mut body).ok();
                        let out = Hydra::answer(&st, &path, &body).to_string();
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{out}",
                            out.len()
                        );
                        let _ = s.write_all(resp.as_bytes());
                    });
                }
            });
            Hydra { addr, st }
        }

        fn answer(st: &HydraState, path: &str, body: &[u8]) -> Value {
            match path {
                "/query" => {
                    let q = String::from_utf8_lossy(body);
                    if q.starts_with("BEGIN") && q.contains("dfs_block_map") {
                        if st.fail_batch.load(Ordering::SeqCst) {
                            return json!({"status": "error", "error": "injected batch failure"});
                        }
                        st.log.lock().unwrap().push("batch".to_string());
                    }
                    json!({"status": "success", "rows": []})
                }
                "/v1/dfs/egroup-state" => {
                    st.log.lock().unwrap().push("seal".to_string());
                    json!({"status": "success", "applied": true, "current": {}})
                }
                "/v1/dfs/drain-commit" => {
                    st.arrived.fetch_add(1, Ordering::SeqCst);
                    if let Some(p) = st.probe.lock().unwrap().as_ref() {
                        st.sealed_at_commit.lock().unwrap().push(journal::sealed_path(p).exists());
                    }
                    {
                        let mut held = st.hold.lock().unwrap();
                        while *held {
                            held = st.cv.wait(held).unwrap();
                        }
                    }
                    if st.fail_commit.load(Ordering::SeqCst) {
                        st.log.lock().unwrap().push("commit-refused".to_string());
                        return json!({"status": "success", "applied": false,
                                      "current": {"epoch": 9, "drain_seq": 0}});
                    }
                    st.log.lock().unwrap().push("commit".to_string());
                    json!({"status": "success", "applied": true, "current": {}})
                }
                _ => json!({"status": "success", "applied": true, "current": {}}),
            }
        }

        fn hold(&self, on: bool) {
            *self.st.hold.lock().unwrap() = on;
            self.st.cv.notify_all();
        }

        fn wait_for_commits(&self, n: usize) {
            wait_until("a drain-commit to reach Hydra", || self.st.arrived.load(Ordering::SeqCst) >= n);
        }

        fn log(&self) -> Vec<String> {
            self.st.log.lock().unwrap().clone()
        }
    }

    // ---- a replica that can misbehave ------------------------------------------------

    type Hook = Arc<dyn Fn(&peer::Request) -> Option<Response> + Send + Sync>;

    struct TestReplica {
        store: Arc<ReplicaStore>,
        client: Arc<PeerClient>,
        hook: Arc<Mutex<Option<Hook>>>,
    }

    impl TestReplica {
        fn start(dir: &Path, name: &str) -> TestReplica {
            let store = Arc::new(ReplicaStore::new(&dir.join(format!("replica-{name}"))).unwrap());
            let hook: Arc<Mutex<Option<Hook>>> = Arc::new(Mutex::new(None));
            let (s2, h2) = (Arc::clone(&store), Arc::clone(&hook));
            let addr = peer::spawn_test_server(Arc::new(move |req| {
                let hook = h2.lock().unwrap().clone();
                if let Some(h) = hook {
                    if let Some(resp) = h(req) {
                        return resp;
                    }
                }
                peer::serve_request(&s2, req)
            }));
            let client = Arc::new(PeerClient::new(name, &addr, Duration::from_secs(10)));
            TestReplica { store, client, hook }
        }

        fn set_hook(&self, h: Option<Hook>) {
            *self.hook.lock().unwrap() = h;
        }

        fn journal(&self) -> Vec<u8> {
            self.store.read_tail("vd").unwrap()
        }
    }

    /// (seq, flags) of every record in a journal byte stream.
    fn records_in(bytes: &[u8]) -> Vec<(u64, u32)> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos + journal::HEADER_LEN <= bytes.len() {
            let len = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
            let seq = u64::from_le_bytes(bytes[pos + 8..pos + 16].try_into().unwrap());
            let flags = u32::from_le_bytes(bytes[pos + 32..pos + 36].try_into().unwrap());
            out.push((seq, flags));
            pos += journal::HEADER_LEN + len;
        }
        out
    }

    // ---- the vdisk -------------------------------------------------------------------

    const SIZE: u64 = 16 << 20;

    struct Rig {
        dir: PathBuf,
        hydra: Hydra,
        replicas: Vec<TestReplica>,
        vd: Arc<Mutex<Vdisk>>,
    }

    fn build(dir: &Path, hydra: &Hydra, peers: Vec<Arc<PeerClient>>, high: u64, ceil: u64) -> Vdisk {
        let jpath = dir.join("journal").join("vd.jrn");
        let mut map_replicas = vec!["self".to_string()];
        map_replicas.extend(peers.iter().map(|p| p.node.clone()));
        Vdisk {
            id: "vd".to_string(),
            size: SIZE,
            epoch: 3,
            class: CLASS_RW.to_string(),
            extent_bytes: MIB as u64,
            drain_seq: 0,
            rf: map_replicas.len() as u64,
            vh: vdisk_hash("vd"),
            node: "self".to_string(),
            journal: Journal::open(&jpath).unwrap(),
            overlay: Overlay::new(),
            map: BTreeMap::new(),
            store: Arc::new(EgroupStore::new(&dir.join("egroups"), 4 << 20).unwrap()),
            open_eg: None,
            high_water: high,
            hard_ceiling: ceil,
            gate: DrainGate::new(),
            drain_held: Arc::new(Mutex::new(HashSet::new())),
            daruk: Daruk::new(&hydra.addr, Duration::from_secs(10)),
            map_replicas,
            compress: false,
            replicas: peers,
            access: Arc::new(AccessLog::new(64, 0)),
            commit: commit::Pipeline::new(),
            degraded: None,
        }
    }

    fn rig(name: &str, nreplicas: usize, high: u64, ceil: u64) -> Rig {
        let dir = tmpdir(name);
        let hydra = Hydra::start();
        *hydra.st.probe.lock().unwrap() = Some(dir.join("journal").join("vd.jrn"));
        let replicas: Vec<TestReplica> =
            (0..nreplicas).map(|i| TestReplica::start(&dir, &format!("r{i}"))).collect();
        let peers = replicas.iter().map(|r| Arc::clone(&r.client)).collect();
        let vd = Arc::new(Mutex::new(build(&dir, &hydra, peers, high, ceil)));
        Rig { dir, hydra, replicas, vd }
    }

    impl Rig {
        fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
            write_through(&self.vd, offset, data)
        }
        fn read(&self, offset: u64, len: u32) -> Vec<u8> {
            self.vd.lock().unwrap().read(offset, len).unwrap()
        }
        fn settle(&self) {
            let gate = Arc::clone(&self.vd.lock().unwrap().gate);
            wait_until("the drain to finish", || !gate.running());
        }
        fn running(&self) -> bool {
            self.vd.lock().unwrap().gate.running()
        }
        fn journal_len(&self) -> u64 {
            self.vd.lock().unwrap().journal.len()
        }
        fn sealed_exists(&self) -> bool {
            journal::sealed_path(&self.dir.join("journal").join("vd.jrn")).exists()
        }
        fn degraded(&self) -> Option<String> {
            self.vd.lock().unwrap().degraded.clone()
        }
    }

    impl Drop for Rig {
        fn drop(&mut self) {
            self.hydra.hold(false);
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn fill(seed: u8, n: usize) -> Vec<u8> {
        (0..n).map(|i| seed.wrapping_add((i % 251) as u8)).collect()
    }

    /// A model disk to compare reads against.
    struct Model(Vec<u8>);
    impl Model {
        fn new() -> Model {
            Model(vec![0u8; SIZE as usize])
        }
        fn put(&mut self, off: u64, data: &[u8]) {
            self.0[off as usize..off as usize + data.len()].copy_from_slice(data);
        }
        fn slice(&self, off: u64, len: usize) -> &[u8] {
            &self.0[off as usize..off as usize + len]
        }
    }

    // ================================================================================
    // The drain is out of the acknowledgement path
    // ================================================================================

    #[test]
    fn a_write_that_crosses_the_high_water_mark_is_acknowledged_while_its_drain_still_runs() {
        // The drain is held at its commit, so it cannot finish. If the write that started
        // it had to wait for it, this write would never return.
        let r = rig("ack-before-drain", 0, 2 * MIB as u64, 64 * MIB as u64);
        r.hydra.hold(true);
        let data = fill(1, 3 * MIB);
        r.write(0, &data).expect("acknowledged without waiting for the drain");

        r.hydra.wait_for_commits(1);
        assert!(r.running(), "the drain is running, stopped at its commit");
        assert!(r.vd.lock().unwrap().stats()["draining"].as_bool().unwrap());
        // And the guest carries on writing and reading through it.
        let more = fill(9, 4096);
        r.write(5 * MIB as u64, &more).expect("a second write during the drain");
        assert_eq!(r.read(0, MIB as u32), data[..MIB]);
        assert_eq!(r.read(5 * MIB as u64, 4096), more);

        r.hydra.hold(false);
        r.settle();
        assert!(!r.sealed_exists());
        assert_eq!(r.read(0, 3 * MIB as u32), data);
        assert_eq!(r.read(5 * MIB as u64, 4096), more);
    }

    #[test]
    fn writes_below_the_high_water_mark_start_no_drain() {
        let r = rig("below-high-water", 0, 8 * MIB as u64, 64 * MIB as u64);
        r.write(0, &fill(1, MIB)).unwrap();
        assert!(!r.running());
        assert_eq!(r.hydra.st.arrived.load(Ordering::SeqCst), 0);
        assert!(r.sealed_exists() == false);
    }

    #[test]
    fn reads_stay_correct_through_a_drain_including_ranges_written_while_it_ran() {
        let r = rig("read-during-drain", 0, 2 * MIB as u64, 64 * MIB as u64);
        let mut model = Model::new();
        r.hydra.hold(true);

        let a = fill(1, 3 * MIB);
        r.write(0, &a).unwrap();
        model.put(0, &a);
        r.hydra.wait_for_commits(1);

        // While the drain is stopped at its commit: overwrite the middle of a range it is
        // moving, and write somewhere it never saw.
        let b = fill(100, 700);
        r.write(1000, &b).unwrap();
        model.put(1000, &b);
        let c = fill(50, MIB / 2);
        r.write(6 * MIB as u64, &c).unwrap();
        model.put(6 * MIB as u64, &c);
        assert_eq!(r.read(0, 8 * MIB as u32), model.slice(0, 8 * MIB), "read during the drain");

        r.hydra.hold(false);
        r.settle();
        assert_eq!(r.read(0, 8 * MIB as u32), model.slice(0, 8 * MIB), "read after the drain");
        // What was written during the drain is still journalled, and is *not* lost with the
        // sealed segment: it is newer than anything the drain wrote.
        assert!(r.vd.lock().unwrap().needs_drain());

        drain_all(&r.vd).unwrap();
        let v = r.vd.lock().unwrap();
        assert!(!v.needs_drain());
        assert_eq!(v.journal.len(), 0);
        drop(v);
        // Now every byte comes from extent groups, and the overwritten middle is the new one.
        assert_eq!(r.read(0, 8 * MIB as u32), model.slice(0, 8 * MIB), "read from extents only");
    }

    #[test]
    fn the_journal_forgets_only_after_hydra_has_the_new_map() {
        let r = rig("forget-after-commit", 0, 2 * MIB as u64, 64 * MIB as u64);
        r.write(0, &fill(1, 3 * MIB)).unwrap();
        r.settle();

        // At the moment the commit reached Hydra the sealed segment was still on disk, and
        // the rows had already been written. Afterwards it is gone.
        assert_eq!(*r.hydra.st.sealed_at_commit.lock().unwrap(), vec![true]);
        assert_eq!(r.hydra.log(), vec!["batch".to_string(), "commit".to_string()]);
        assert!(!r.sealed_exists());
        assert_eq!(r.vd.lock().unwrap().drain_seq, 1);
    }

    #[test]
    fn a_refused_commit_forgets_nothing_and_stops_further_drains() {
        let r = rig("commit-refused", 0, 2 * MIB as u64, 64 * MIB as u64);
        r.hydra.st.fail_commit.store(true, Ordering::SeqCst);
        let data = fill(3, 3 * MIB);
        r.write(0, &data).unwrap();
        r.settle();

        // Deposed, as far as Hydra is concerned: flagged, and nothing was dropped.
        let why = r.degraded().expect("a refused drain degrades the vdisk");
        assert!(why.contains("no longer owns"), "{why}");
        assert!(r.sealed_exists(), "the journal must keep what Hydra does not have");
        assert_eq!(r.vd.lock().unwrap().drain_seq, 0);
        assert_eq!(r.read(0, 3 * MIB as u32), data, "reads are served from the overlay");

        // Degraded means no drain is retried into silence on every write.
        r.write(4 * MIB as u64, &fill(4, 1000)).unwrap();
        r.settle();
        assert_eq!(r.hydra.st.arrived.load(Ordering::SeqCst), 1);

        // But a synchronous drain (a detach, a flush) still tries, and succeeds once Hydra
        // agrees -- picking up the sealed segment the failed one left, then the live one.
        r.hydra.st.fail_commit.store(false, Ordering::SeqCst);
        drain_all(&r.vd).unwrap();
        assert!(!r.sealed_exists());
        let v = r.vd.lock().unwrap();
        assert_eq!((v.journal.len(), v.needs_drain()), (0, false));
        drop(v);
        assert_eq!(r.read(0, 3 * MIB as u32), data);
        assert_eq!(r.read(4 * MIB as u64, 1000), fill(4, 1000));
    }

    #[test]
    fn a_failure_writing_the_map_rows_forgets_nothing_either() {
        let r = rig("batch-fails", 0, 2 * MIB as u64, 64 * MIB as u64);
        r.hydra.st.fail_batch.store(true, Ordering::SeqCst);
        let data = fill(5, 3 * MIB);
        r.write(0, &data).unwrap();
        r.settle();
        assert!(r.degraded().unwrap().contains("injected batch failure"));
        assert!(r.sealed_exists());
        assert_eq!(r.hydra.st.arrived.load(Ordering::SeqCst), 0, "no commit without its rows");
        assert_eq!(r.read(0, 3 * MIB as u32), data);
    }

    #[test]
    fn a_crash_mid_drain_recovers_every_acknowledged_write() {
        let r = rig("crash-mid-drain", 0, 2 * MIB as u64, 64 * MIB as u64);
        let mut model = Model::new();
        // Everything acknowledged, in two generations: some before the drain was planned,
        // some after.
        let a = fill(1, 3 * MIB);
        {
            // No threads: write directly, then plan and run the drain by hand and stop it
            // before the finish -- the process dying after the extents were written and
            // before the journal was allowed to forget.
            let mut v = r.vd.lock().unwrap();
            v.write(0, &a).unwrap();
            model.put(0, &a);
            let mut plan = v.plan_drain().unwrap().expect("something to drain");
            let b = fill(60, 5000);
            v.write(2 * MIB as u64 + 7, &b).unwrap();
            model.put(2 * MIB as u64 + 7, &b);
            let committed = plan.job.run();
            assert!(committed.is_ok(), "the drain's bytes and commit went through");
            // ... and here the daemon dies: no finish_drain.
        }
        assert!(r.sealed_exists(), "the sealed segment is still on disk");

        // A new life. Same files, a map that never heard of the extents (the commit is
        // modelled as lost with the process), and no replicas, so open replays the journal.
        let hydra2 = Hydra::start();
        let mut v2 = build(&r.dir, &hydra2, Vec::new(), 2 * MIB as u64, 64 * MIB as u64);
        let discarded = v2.replay_journal().unwrap();
        assert_eq!(discarded, 0);
        assert!(v2.journal.has_sealed());
        let got = v2.read(0, 4 * MIB as u32).unwrap();
        assert_eq!(got, model.slice(0, 4 * MIB), "every acknowledged write came back");

        // And it can carry on: the next drain takes the sealed segment, then the live one.
        let h2 = Arc::new(Mutex::new(v2));
        drain_all(&h2).unwrap();
        let mut v2 = h2.lock().unwrap();
        assert_eq!((v2.journal.len(), v2.needs_drain()), (0, false));
        assert_eq!(v2.read(0, 4 * MIB as u32).unwrap(), model.slice(0, 4 * MIB));
    }

    // ---- backpressure ----------------------------------------------------------------

    #[test]
    fn a_writer_at_the_ceiling_waits_for_the_drain_and_the_journal_does_not_grow_past_it() {
        let r = rig("ceiling-waits", 0, MIB as u64, 3 * MIB as u64);
        r.hydra.hold(true);
        // The first two writes are admitted (the journal is under the ceiling when each
        // arrives); together they take it to the ceiling.
        r.write(0, &fill(1, 3 * MIB / 2)).unwrap();
        r.write(2 * MIB as u64, &fill(2, 3 * MIB / 2)).unwrap();
        r.hydra.wait_for_commits(1);
        let at_ceiling = r.journal_len();
        assert!(at_ceiling >= 3 * MIB as u64, "{at_ceiling}");

        // The third must wait for the held drain -- and must not have written anything.
        let vd = Arc::clone(&r.vd);
        let third = std::thread::spawn(move || write_through(&vd, 8 * MIB as u64, &fill(3, 4096)));
        std::thread::sleep(Duration::from_millis(400));
        assert!(!third.is_finished(), "a writer at the ceiling must wait");
        assert_eq!(r.journal_len(), at_ceiling, "and the journal must not have grown");

        r.hydra.hold(false);
        third.join().unwrap().expect("admitted once the drain made room");
        r.settle();
        assert_eq!(r.read(8 * MIB as u64, 4096), fill(3, 4096));
        assert!(r.journal_len() < at_ceiling);
    }

    #[test]
    fn at_the_ceiling_with_a_drain_that_cannot_run_the_write_fails_instead_of_hanging() {
        let r = rig("ceiling-refuses", 0, MIB as u64, 3 * MIB as u64);
        r.hydra.st.fail_commit.store(true, Ordering::SeqCst);
        r.write(0, &fill(1, 3 * MIB / 2)).unwrap();
        r.write(2 * MIB as u64, &fill(2, 3 * MIB / 2)).unwrap();
        r.settle();
        assert!(r.degraded().is_some());

        let err = within(10, {
            let vd = Arc::clone(&r.vd);
            move || write_through(&vd, 8 * MIB as u64, &fill(3, 4096))
        })
        .expect_err("nothing will make room");
        assert!(err.to_string().contains("ceiling"), "{err}");
        // Nothing of the refused write is visible.
        assert_eq!(r.read(8 * MIB as u64, 4096), vec![0u8; 4096]);
    }

    #[test]
    fn a_large_trim_is_admitted_a_chunk_at_a_time_so_it_cannot_overshoot_the_ceiling() {
        let r = rig("trim-chunks", 0, MIB as u64, 3 * MIB as u64);
        r.write(0, &fill(1, 8 * MIB)).unwrap();
        r.settle();

        // Sample the journal while a trim four times the ceiling runs.
        let peak = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let sampler = {
            let (vd, peak, stop) = (Arc::clone(&r.vd), Arc::clone(&peak), Arc::clone(&stop));
            std::thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    let len = vd.lock().unwrap().journal.len();
                    peak.fetch_max(len, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
        };
        write_zeroes_through(&r.vd, 0, 8 * MIB as u64).unwrap();
        stop.store(true, Ordering::SeqCst);
        sampler.join().unwrap();
        r.settle();

        assert_eq!(r.read(0, 8 * MIB as u32), vec![0u8; 8 * MIB]);
        // Admitted just under the ceiling, so it can overshoot by one chunk and no more.
        let peak = peak.load(Ordering::SeqCst);
        assert!(peak <= 3 * MIB as u64 + MIB as u64 + 4096, "the journal reached {peak} bytes");
    }

    // ---- callers that need a drained vdisk -------------------------------------------

    #[test]
    fn drain_all_waits_for_a_drain_in_flight_and_returns_a_fully_drained_vdisk() {
        let r = rig("drain-all", 0, 2 * MIB as u64, 64 * MIB as u64);
        let mut model = Model::new();
        r.hydra.hold(true);
        let a = fill(1, 3 * MIB);
        r.write(0, &a).unwrap();
        model.put(0, &a);
        r.hydra.wait_for_commits(1);
        let b = fill(8, 3000);
        r.write(7 * MIB as u64, &b).unwrap();
        model.put(7 * MIB as u64, &b);

        let vd = Arc::clone(&r.vd);
        let all = std::thread::spawn(move || drain_all(&vd));
        std::thread::sleep(Duration::from_millis(300));
        assert!(!all.is_finished(), "it must wait for the running drain, not race it");

        r.hydra.hold(false);
        within(20, move || all.join().unwrap()).expect("drained");
        let v = r.vd.lock().unwrap();
        assert_eq!(v.journal.len(), 0);
        assert!(!v.needs_drain());
        assert!(!v.gate.running());
        drop(v);
        assert!(!r.sealed_exists());
        assert_eq!(r.read(0, 8 * MIB as u32), model.slice(0, 8 * MIB));
    }

    #[test]
    fn a_synchronous_drain_refuses_to_run_beside_a_background_one() {
        let r = rig("no-two-drains", 0, 2 * MIB as u64, 64 * MIB as u64);
        r.hydra.hold(true);
        r.write(0, &fill(1, 3 * MIB)).unwrap();
        r.hydra.wait_for_commits(1);
        let err = r.vd.lock().unwrap().drain().expect_err("two drains would move one journal twice");
        assert!(err.to_string().contains("running"), "{err}");
        r.hydra.hold(false);
        r.settle();
    }

    #[test]
    fn a_replica_cannot_join_while_a_drain_runs() {
        // The drain replicates to the set it was planned with. A member that joined after
        // would be listed by the map as holding extents it never received.
        let r = rig("join-during-drain", 0, 2 * MIB as u64, 64 * MIB as u64);
        let late = TestReplica::start(&r.dir, "late");
        r.hydra.hold(true);
        r.write(0, &fill(1, 3 * MIB)).unwrap();
        r.hydra.wait_for_commits(1);
        let err = r.vd.lock().unwrap().add_replica(Arc::clone(&late.client)).expect_err("refused");
        assert!(err.to_string().contains("draining"), "{err}");
        assert!(r.vd.lock().unwrap().replicas.is_empty());

        r.hydra.hold(false);
        // The heal's own path waits for the drain, then joins -- and the new member gets
        // every extent the finished drain committed, and the journal.
        let mut v = lock_idle(&r.vd);
        let copied = v.add_replica(Arc::clone(&late.client)).unwrap();
        assert_eq!(copied, 3, "the three extents the drain wrote");
        drop(v);
        let groups: Vec<_> = r.vd.lock().unwrap().map.values().map(|l| l.egroup_id.clone()).collect();
        for g in groups {
            assert!(late.store.get_egroup(&g, 0, 16).is_ok(), "replica has {g}");
        }
    }

    #[test]
    fn extent_groups_a_running_drain_has_made_are_held_against_the_sweep() {
        let r = rig("held-by-drain", 0, 2 * MIB as u64, 64 * MIB as u64);
        r.hydra.hold(true);
        r.write(0, &fill(1, 3 * MIB)).unwrap();
        r.hydra.wait_for_commits(1);
        // The group exists on disk and in Hydra's egroup table but is in no map yet and is
        // not `open_eg` (the drain has it): the sweep must still be told it is in use.
        let held = r.vd.lock().unwrap().held_egroups();
        assert_eq!(held.len(), 1, "{held:?}");
        assert!(held.iter().next().unwrap().starts_with("eg-vd-"));
        r.hydra.hold(false);
        r.settle();
        // Afterwards it is the open group, still held, and now also in the map.
        let held_after = r.vd.lock().unwrap().held_egroups();
        assert_eq!(held, held_after);
    }

    // ---- compaction's hooks into the vdisk --------------------------------------------

    #[test]
    fn a_hold_keeps_drains_off_without_stopping_the_guest_and_lets_them_resume() {
        let r = rig("compaction-hold", 0, 2 * MIB as u64, 64 * MIB as u64);
        let hold = r.vd.lock().unwrap().try_hold_drains().expect("no drain was running");
        assert!(
            r.vd.lock().unwrap().try_hold_drains().is_none(),
            "two holds, or a hold beside a drain, would be two writers of the block map"
        );
        // Well past the high-water mark, and still acknowledged: only the drain is held off.
        r.write(0, &fill(1, 3 * MIB)).unwrap();
        assert!(r.running(), "a hold is a drain in the gate's eyes");
        assert!(r.journal_len() >= 3 * MIB as u64, "a drain ran while the gate was held");
        assert_eq!(r.read(0, 16), fill(1, 16));
        drop(hold);
        assert!(!r.running());
        // The next write finds the journal over the mark and starts the drain again.
        r.write(3 * MIB as u64, &fill(2, 4096)).unwrap();
        r.settle();
        assert!(r.journal_len() < 3 * MIB as u64, "drains did not resume after the hold");
    }

    #[test]
    fn an_extent_is_repointed_only_if_it_still_points_where_the_caller_believes() {
        let r = rig("compaction-repoint", 0, 2 * MIB as u64, 64 * MIB as u64);
        r.write(0, &fill(1, 3 * MIB)).unwrap();
        r.settle();
        let mut v = r.vd.lock().unwrap();
        let loc = v.map_entry(0).expect("extent 0 was drained");
        assert!(!v.repoint_extent(0, ("eg-elsewhere", loc.offset, loc.length), "eg-new", 0));
        assert!(!v.repoint_extent(0, (&loc.egroup_id, loc.offset + 1, loc.length), "eg-new", 0));
        assert_eq!(v.map_entry(0).unwrap().egroup_id, loc.egroup_id, "a refused repoint moved the entry");
        assert!(v.repoint_extent(0, (&loc.egroup_id, loc.offset, loc.length), "eg-new", 64));
        let after = v.map_entry(0).unwrap();
        assert_eq!((after.egroup_id.as_str(), after.offset), ("eg-new", 64));
        // The footer travelled with the bytes, so identity and stored length are unchanged.
        assert_eq!((after.length, after.vdisk_hash), (loc.length, loc.vdisk_hash));
        assert!(!v.repoint_extent(99, ("x", 0, 0), "y", 0), "an extent that is not mapped cannot be repointed");
    }

    #[test]
    fn the_groups_a_drain_is_making_are_named_apart_from_everything_the_map_points_at() {
        let r = rig("compaction-drain-groups", 0, 2 * MIB as u64, 64 * MIB as u64);
        r.hydra.hold(true);
        r.write(0, &fill(1, 3 * MIB)).unwrap();
        r.hydra.wait_for_commits(1);
        let (making, held) = {
            let v = r.vd.lock().unwrap();
            (v.drain_groups(), v.held_egroups())
        };
        assert_eq!(making.len(), 1, "{making:?}");
        assert!(held.is_superset(&making));
        r.hydra.hold(false);
        r.settle();
    }

    // ---- replicas see the drain by sequence, not wholesale ----------------------------

    #[test]
    fn a_drain_trims_replica_journals_by_sequence_and_keeps_writes_made_while_it_ran() {
        let r = rig("replica-trim", 1, 2 * MIB as u64, 64 * MIB as u64);
        r.hydra.hold(true);
        r.write(0, &fill(1, 3 * MIB)).unwrap(); // seqs 0..=2
        r.hydra.wait_for_commits(1);
        r.write(5 * MIB as u64, &fill(2, 1000)).unwrap(); // seq 3, while the drain is held
        assert_eq!(records_in(&r.replicas[0].journal()).len(), 4);

        r.hydra.hold(false);
        r.settle();
        // The drain's three records are gone from the replica; the one acknowledged while it
        // ran is not. Emptying the file here would be dropping an acknowledged write from the
        // only other copy of it.
        wait_until("the replica to drop its drained prefix", || {
            records_in(&r.replicas[0].journal()).len() == 1
        });
        assert_eq!(records_in(&r.replicas[0].journal()), vec![(3, FLAG_COMMIT)]);
        // The extents reached the replica before the commit.
        let groups: Vec<_> = r.vd.lock().unwrap().map.values().map(|l| l.egroup_id.clone()).collect();
        assert!(!groups.is_empty());
        for g in groups {
            assert!(r.replicas[0].store.get_egroup(&g, 0, 16).is_ok());
        }
    }

    #[test]
    fn a_replica_that_does_not_know_the_new_opcode_keeps_its_journal() {
        // An older replica answers "refused" to OP_TRUNCATE_TO. The drain carries on, and the
        // replica's journal is untouched -- a superset, which replay handles.
        let r = rig("old-replica", 1, 2 * MIB as u64, 64 * MIB as u64);
        r.replicas[0].set_hook(Some(Arc::new(|req| {
            (req.opcode == peer::OP_TRUNCATE_TO).then(|| Response::err(peer::ST_REFUSED, 0))
        })));
        r.write(0, &fill(1, 3 * MIB)).unwrap();
        r.settle();
        std::thread::sleep(Duration::from_millis(200));
        assert!(!r.sealed_exists(), "the drain itself completed");
        assert_eq!(records_in(&r.replicas[0].journal()).len(), 3, "and the replica kept its records");
        assert!(r.degraded().is_none(), "a replica that cannot trim is not a failed drain");
    }

    #[test]
    fn a_drain_that_cannot_reach_a_replica_with_its_extents_does_not_commit() {
        // Draining must not make data less durable than the journal was: an extent that is on
        // one node only, with the journal that backed it about to be forgotten, is a copy lost.
        let r = rig("replica-extent-fails", 1, 2 * MIB as u64, 64 * MIB as u64);
        r.replicas[0].set_hook(Some(Arc::new(|req| {
            (req.opcode == peer::OP_EGROUP_PUT).then(|| Response::err(peer::ST_IO, 0))
        })));
        let data = fill(7, 3 * MIB);
        r.write(0, &data).unwrap();
        r.settle();
        assert!(r.degraded().is_some());
        assert_eq!(r.hydra.st.arrived.load(Ordering::SeqCst), 0, "no commit");
        assert!(r.sealed_exists());
        assert_eq!(r.read(0, 3 * MIB as u32), data);
    }

    // ================================================================================
    // The local sync and the replicas work together
    // ================================================================================

    fn jpath(r: &Rig) -> PathBuf {
        r.dir.join("journal").join("vd.jrn")
    }

    /// Count the local journal's syncs.
    fn count_syncs(r: &Rig) -> Arc<AtomicUsize> {
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = Arc::clone(&n);
        journal::testhook::set(&jpath(r), Some(Arc::new(move || {
            n2.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })));
        n
    }

    /// Record (flags) of every journal append a replica is sent.
    fn record_append_flags(replica: &TestReplica) -> Arc<Mutex<Vec<u16>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s2 = Arc::clone(&seen);
        replica.set_hook(Some(Arc::new(move |req| {
            if req.opcode == peer::OP_APPEND {
                s2.lock().unwrap().push(req.flags);
            }
            None
        })));
        seen
    }

    #[test]
    fn the_local_sync_runs_while_the_replica_is_being_written_not_before_it() {
        // Each side is made to wait for the other: the local sync will not return until the
        // replica has been contacted, and the replica will not answer until the local sync has
        // started. Whichever order a serial owner used -- sync and then the replica, or the
        // replica and then the sync -- one side would wait out its deadline and fail the
        // write. The only way through is both being in flight at once.
        let r = rig("overlap-sync", 1, 64 * MIB as u64, 128 * MIB as u64);
        let contacted = Arc::new(AtomicBool::new(false));
        let syncing = Arc::new(AtomicBool::new(false));
        let (c2, s2) = (Arc::clone(&contacted), Arc::clone(&syncing));
        r.replicas[0].set_hook(Some(Arc::new(move |req| {
            if req.opcode != peer::OP_APPEND {
                return None;
            }
            c2.store(true, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(5);
            while !s2.load(Ordering::SeqCst) {
                if Instant::now() > deadline {
                    return Some(Response::err(peer::ST_IO, 0));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            None
        })));
        let s3 = Arc::clone(&syncing);
        journal::testhook::set(&jpath(&r), Some(Arc::new(move || {
            s3.store(true, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(5);
            while !contacted.load(Ordering::SeqCst) {
                if Instant::now() > deadline {
                    return Err(Error::io(
                        "the replica was not contacted while the local sync was pending".to_string(),
                    ));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        })));

        r.write(0, &fill(1, 4096)).expect("the sync and the round trip ran together");
        assert_eq!(r.read(0, 4096), fill(1, 4096));
        assert_eq!(records_in(&r.replicas[0].journal()), vec![(0, FLAG_COMMIT)]);
        journal::testhook::set(&jpath(&r), None);
    }

    #[test]
    fn the_next_record_is_written_locally_while_the_previous_one_is_still_with_the_replica() {
        // The replica holds its answer to the first record until the owner's own journal
        // already contains the second. A strictly serial owner -- record, replicate,
        // record -- never gets there, and the replica gives up and refuses.
        let r = rig("pipeline", 1, 64 * MIB as u64, 128 * MIB as u64);
        let jp = jpath(&r);
        let arrivals = Arc::new(AtomicUsize::new(0));
        r.replicas[0].set_hook(Some(Arc::new(move |req| {
            if req.opcode != peer::OP_APPEND || arrivals.fetch_add(1, Ordering::SeqCst) != 0 {
                return None;
            }
            let two_records = 2 * (journal::HEADER_LEN + MIB) as u64;
            let deadline = Instant::now() + Duration::from_secs(5);
            while std::fs::metadata(&jp).map(|m| m.len()).unwrap_or(0) < two_records {
                if Instant::now() > deadline {
                    return Some(Response::err(peer::ST_IO, 0));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            None
        })));

        let data = fill(2, 3 * MIB);
        r.write(0, &data).expect("the local append ran ahead of the replica's answer");
        assert_eq!(r.read(0, 3 * MIB as u32), data);
    }

    #[test]
    fn every_replica_gets_the_owners_journal_byte_for_byte_and_the_commit_marker_comes_last() {
        let r = rig("replica-bytes", 2, 64 * MIB as u64, 128 * MIB as u64);
        r.write(0, &fill(1, 5 * MIB)).unwrap();
        let local = std::fs::read(jpath(&r)).unwrap();
        for replica in &r.replicas {
            let theirs = replica.journal();
            assert_eq!(theirs, local, "a replica's journal is the owner's");
            let recs = records_in(&theirs);
            assert_eq!(
                recs,
                vec![(0, 0), (1, 0), (2, 0), (3, 0), (4, FLAG_COMMIT)],
                "in sequence, and only the last record commits the group"
            );
        }
    }

    #[test]
    fn a_group_is_synced_once_locally_and_only_its_last_record_is_synced_on_the_replica() {
        let r = rig("one-sync", 1, 64 * MIB as u64, 128 * MIB as u64);
        let syncs = count_syncs(&r);
        let flags = record_append_flags(&r.replicas[0]);

        r.write(0, &fill(1, 5 * MIB)).unwrap();
        assert_eq!(syncs.load(Ordering::SeqCst), 1, "one fsync for five records");
        // Five 1 MiB records go as two requests (a request carries up to 4 MiB of whole
        // frames): the first defers its sync, the last is made durable before it is answered.
        assert_eq!(
            *flags.lock().unwrap(),
            vec![peer::APPEND_DEFER_SYNC, 0],
            "the replica may defer all but the last, which it must make durable before answering"
        );

        // A single-record write has nothing to defer.
        flags.lock().unwrap().clear();
        r.write(8 * MIB as u64, &fill(2, 4096)).unwrap();
        assert_eq!(syncs.load(Ordering::SeqCst), 2);
        assert_eq!(*flags.lock().unwrap(), vec![0]);
        journal::testhook::set(&jpath(&r), None);
    }

    #[test]
    fn without_replicas_a_group_is_still_synced_once_after_its_last_record() {
        let r = rig("one-sync-local", 0, 64 * MIB as u64, 128 * MIB as u64);
        let syncs = count_syncs(&r);
        r.write(0, &fill(1, 3 * MIB)).unwrap();
        assert_eq!(syncs.load(Ordering::SeqCst), 1);
        journal::testhook::set(&jpath(&r), None);

        // And what is on disk replays as one complete group.
        let mut j = Journal::open(&jpath(&r)).unwrap();
        let (recs, _) = j.replay().unwrap();
        assert_eq!(recs.iter().map(|x| x.flags).collect::<Vec<_>>(), vec![0, 0, FLAG_COMMIT]);
    }

    #[test]
    fn a_group_missing_its_commit_marker_is_not_applied_on_replay() {
        // The crash the one-sync policy trades on: records on disk, marker not. Replay must
        // discard the lot -- never expose a prefix of a guest write.
        let r = rig("no-marker", 1, 64 * MIB as u64, 128 * MIB as u64);
        let data = fill(4, 3 * MIB);
        r.write(0, &data).unwrap();
        let whole = r.replicas[0].journal();
        let rec_len = journal::HEADER_LEN + MIB;

        let hydra = Hydra::start();
        let dir2 = tmpdir("no-marker-replay");
        let mut torn = build(&dir2, &hydra, Vec::new(), 64 * MIB as u64, 128 * MIB as u64);
        torn.journal.replace(&whole[..2 * rec_len]).unwrap();
        assert_eq!(torn.replay_journal().unwrap(), 0);
        assert!(!torn.needs_drain(), "two records of a three-record group are not a write");
        assert_eq!(torn.read(0, 3 * MIB as u32).unwrap(), vec![0u8; 3 * MIB]);

        let mut whole_vd = build(&tmpdir("no-marker-whole"), &hydra, Vec::new(), 64 * MIB as u64, 128 * MIB as u64);
        whole_vd.journal.replace(&whole).unwrap();
        whole_vd.replay_journal().unwrap();
        assert_eq!(whole_vd.read(0, 3 * MIB as u32).unwrap(), data);
    }

    #[test]
    fn a_replica_failure_fails_the_write_flags_the_vdisk_and_shows_nothing_to_readers() {
        let r = rig("replica-fails", 1, 64 * MIB as u64, 128 * MIB as u64);
        let arrivals = Arc::new(AtomicUsize::new(0));
        r.replicas[0].set_hook(Some(Arc::new(move |req| {
            (req.opcode == peer::OP_APPEND && arrivals.fetch_add(1, Ordering::SeqCst) == 1)
                .then(|| Response::err(peer::ST_IO, 0))
        })));

        // Six records are two requests; the second is the one that fails.
        let err = r.write(0, &fill(1, 6 * MIB)).expect_err("write-all: one replica failing fails it");
        assert!(err.to_string().contains("refused a journal append"), "{err}");
        assert!(r.degraded().unwrap().contains("refused a journal append"));
        // The records that did go in are not a write the guest was told about.
        assert!(!r.vd.lock().unwrap().needs_drain());
        assert_eq!(r.read(0, 6 * MIB as u32), vec![0u8; 6 * MIB]);
    }

    #[test]
    fn with_two_replicas_either_one_failing_fails_the_write() {
        for bad in 0..2 {
            let r = rig(&format!("one-of-two-{bad}"), 2, 64 * MIB as u64, 128 * MIB as u64);
            r.replicas[bad].set_hook(Some(Arc::new(|req| {
                (req.opcode == peer::OP_APPEND).then(|| Response::err(peer::ST_IO, 0))
            })));
            let err = r.write(0, &fill(1, 2 * MIB)).expect_err("a partial write-all is an error");
            assert!(err.to_string().contains(&format!("replica r{bad}")), "{err}");
            assert!(r.degraded().is_some());
            assert_eq!(r.read(0, 2 * MIB as u32), vec![0u8; 2 * MIB]);
        }
    }

    #[test]
    fn a_replica_that_cannot_be_reached_fails_the_write_too() {
        let r = rig("unreachable", 0, 64 * MIB as u64, 128 * MIB as u64);
        r.vd.lock().unwrap().replicas.push(Arc::new(PeerClient::new(
            "dead",
            "127.0.0.1:1",
            Duration::from_secs(2),
        )));
        let err = r.write(0, &fill(1, 4096)).expect_err("no replica, no acknowledgement");
        assert!(err.to_string().contains("unreachable"), "{err}");
        assert!(r.degraded().is_some());
        assert_eq!(r.read(0, 4096), vec![0u8; 4096]);
    }

    #[test]
    fn a_fenced_replica_deposes_the_owner_and_the_write_says_so() {
        // The vdisk writes at epoch 3; the replica has been fenced at 9 by a new owner.
        let r = rig("deposed", 1, 64 * MIB as u64, 128 * MIB as u64);
        r.replicas[0].store.fence("vd", 9).unwrap();
        let err = r.write(0, &fill(1, 2 * MIB)).expect_err("a deposed owner acknowledges nothing");
        assert!(matches!(err, Error::Refused(_)), "{err:?}");
        assert!(err.to_string().contains("no longer owned"), "{err}");
        let why = r.degraded().unwrap();
        assert!(why.contains("deposed") && why.contains("fenced at epoch 9"), "{why}");
        assert_eq!(r.read(0, 2 * MIB as u32), vec![0u8; 2 * MIB]);
        // And it did not reach the new owner's journal.
        assert!(r.replicas[0].journal().is_empty());
    }

    #[test]
    fn a_local_sync_failure_is_an_error_even_though_the_replicas_took_the_records() {
        let r = rig("local-sync-fails", 1, 64 * MIB as u64, 128 * MIB as u64);
        journal::testhook::set(&jpath(&r), Some(Arc::new(|| {
            Err(Error::io("injected local sync failure".to_string()))
        })));
        let err = r.write(0, &fill(1, 2 * MIB)).expect_err("not durable here, so not acknowledged");
        assert!(err.to_string().contains("injected local sync failure"), "{err}");
        // Not durable here means not visible here, whatever the replicas hold.
        assert!(!r.vd.lock().unwrap().needs_drain());
        assert_eq!(r.read(0, 2 * MIB as u32), vec![0u8; 2 * MIB]);
        // A journal device that has just reported an error is not trusted with another write
        // until the vdisk is re-attached: it is flagged, and takes no more appends.
        assert!(r.degraded().unwrap().contains("injected local sync failure"));
        journal::testhook::set(&jpath(&r), None);
        let again = r.write(0, &fill(2, 4096)).expect_err("the pipeline fails closed");
        assert!(again.to_string().contains("cannot take writes"), "{again}");
        assert_eq!(r.read(0, 4096), vec![0u8; 4096]);
        // And it is not one a replica repair can fix.
        let why = recover(&r.vd).expect_err("a local fault needs a re-attach");
        assert!(why.to_string().contains("re-attached"), "{why}");
    }

    #[test]
    fn a_slow_replica_costs_the_write_its_own_time_and_no_more() {
        // A replica that takes 500 ms per record and a local sync that takes 500 ms: serial
        // would be over 1000 ms for a single record; overlapped is about 500.
        let r = rig("slow-replica", 1, 64 * MIB as u64, 128 * MIB as u64);
        r.replicas[0].set_hook(Some(Arc::new(|req| {
            if req.opcode == peer::OP_APPEND {
                std::thread::sleep(Duration::from_millis(500));
            }
            None
        })));
        journal::testhook::set(&jpath(&r), Some(Arc::new(|| {
            std::thread::sleep(Duration::from_millis(500));
            Ok(())
        })));
        let t = Instant::now();
        r.write(0, &fill(1, 4096)).unwrap();
        let took = t.elapsed();
        assert!(took >= Duration::from_millis(500), "{took:?}");
        assert!(took < Duration::from_millis(850), "serial would be 1000ms+, took {took:?}");
        journal::testhook::set(&jpath(&r), None);
    }

    // ================================================================================
    // The drain ships its extents through a pipeline
    // ================================================================================

    /// Make a replica apply every request for real and note, after it has been applied,
    /// which journal-or-extent writes it took, in Hydra's event log -- so one ordered list
    /// holds both what the replica durably took and what Hydra was then told.
    fn log_puts(replica: &TestReplica, hydra: &Hydra) {
        let store = Arc::clone(&replica.store);
        let st = Arc::clone(&hydra.st);
        replica.set_hook(Some(Arc::new(move |req| {
            if req.opcode != peer::OP_EGROUP_PUT {
                return None;
            }
            let resp = peer::serve_request(&store, req);
            st.log.lock().unwrap().push(format!("put:{}", req.flags));
            Some(resp)
        })));
    }

    #[test]
    fn every_extent_is_on_every_replica_before_a_group_is_sealed_and_before_any_map_row() {
        let r = rig("pipeline-order", 2, 2 * MIB as u64, 64 * MIB as u64);
        for replica in &r.replicas {
            log_puts(replica, &r.hydra);
        }
        // Six extents: the first four fill a 4 MiB group, which is sealed; two are left in
        // the next, which is not.
        r.write(0, &fill(1, 6 * MIB)).unwrap();
        r.settle();

        let log = r.hydra.log();
        // Each put appears once per replica; a replica's puts are in order and so are the two
        // replicas', but they interleave, so compare as a count per stage.
        let seal = log.iter().position(|e| e == "seal").expect("the full group was sealed");
        let batch = log.iter().position(|e| e == "batch").expect("rows written");
        let commit = log.iter().position(|e| e == "commit").expect("committed");
        let puts_before_seal = log[..seal].iter().filter(|e| e.starts_with("put:")).count();
        assert_eq!(puts_before_seal, 4 * 2, "all four extents of the group, on both replicas, before the seal");
        let puts_before_rows = log[..batch].iter().filter(|e| e.starts_with("put:")).count();
        assert_eq!(puts_before_rows, 6 * 2, "all six extents on both replicas before any map row");
        assert!(log[batch..].iter().all(|e| !e.starts_with("put:")), "{log:?}");
        assert!(seal < batch && batch < commit, "{log:?}");

        // And both replicas have the bytes -- the whole of each group the map names.
        let groups: Vec<_> = r.vd.lock().unwrap().map.values().map(|l| l.egroup_id.clone()).collect();
        for replica in &r.replicas {
            for g in &groups {
                assert!(replica.store.get_egroup(g, 0, 16).is_ok());
            }
        }
    }

    #[test]
    fn only_the_last_write_to_each_group_is_synced_on_the_replica() {
        let r = rig("pipeline-flags", 1, 2 * MIB as u64, 64 * MIB as u64);
        log_puts(&r.replicas[0], &r.hydra);
        r.write(0, &fill(1, 6 * MIB)).unwrap();
        r.settle();
        let puts: Vec<String> = r.hydra.log().into_iter().filter(|e| e.starts_with("put:")).collect();
        let d = peer::APPEND_DEFER_SYNC;
        // Group one: three deferred and the fourth, which fills it, synced. Group two: one
        // deferred and the last of the drain, synced.
        let want: Vec<String> = [d, d, d, 0, d, 0].iter().map(|f| format!("put:{f}")).collect();
        assert_eq!(puts, want);
    }

    #[test]
    fn a_replica_refusing_an_extent_stops_the_drain_before_the_group_is_sealed_or_any_row_written() {
        let r = rig("pipeline-refused", 1, 2 * MIB as u64, 64 * MIB as u64);
        let seen = Arc::new(AtomicUsize::new(0));
        r.replicas[0].set_hook(Some(Arc::new(move |req| {
            (req.opcode == peer::OP_EGROUP_PUT && seen.fetch_add(1, Ordering::SeqCst) == 1)
                .then(|| Response::err(peer::ST_IO, 0))
        })));
        let data = fill(2, 6 * MIB);
        r.write(0, &data).unwrap();
        r.settle();

        assert!(r.degraded().unwrap().contains("refused extent group"), "{:?}", r.degraded());
        let log = r.hydra.log();
        assert!(!log.iter().any(|e| e == "seal" || e == "batch" || e == "commit"), "{log:?}");
        assert!(r.sealed_exists(), "and the journal still holds it all");
        assert_eq!(r.read(0, 6 * MIB as u32), data);
    }

    #[test]
    fn a_drain_to_a_dead_replica_fails_cleanly_instead_of_hanging() {
        let r = rig("pipeline-dead", 0, 2 * MIB as u64, 64 * MIB as u64);
        r.vd.lock().unwrap().replicas.push(Arc::new(PeerClient::new(
            "dead",
            "127.0.0.1:1",
            Duration::from_secs(2),
        )));
        // No replica to take the journal either, so write through the lock-free path the
        // heal uses: put the records in directly and drain.
        {
            let mut v = r.vd.lock().unwrap();
            let saved = std::mem::take(&mut v.replicas);
            v.write(0, &fill(3, 3 * MIB)).unwrap();
            v.replicas = saved;
        }
        let err = within(20, {
            let vd = Arc::clone(&r.vd);
            move || drain_all(&vd)
        })
        .expect_err("a drain that cannot reach its replica must not commit");
        assert!(err.to_string().contains("unreachable"), "{err}");
        assert!(r.vd.lock().unwrap().journal.has_sealed());
    }
}
