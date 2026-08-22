//! A vdisk: the journal, the overlay, the extent map, and the drain that moves bytes
//! from the first to the third.
//!
//! The ordering rules enforced here are the ones that make the whole design defensible,
//! so they are stated once and never departed from:
//!
//! 1. **A write is acknowledged when its journal record is durable, and not before.**
//! 2. **Extent bytes are durable before any map row points at them.** A crash between
//!    the two leaves orphaned bytes, which Purah sweeps. The reverse ordering
//!    leaves a map pointing at bytes that do not exist, which is data loss.
//! 3. **The journal is not truncated until the map commit has been applied.** A crash
//!    between the two replays records that are already drained, which is idempotent.
//! 4. **Nothing on the guest's write path talks to Hydra.**

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::err::{Error, Result};
use crate::extent::{vdisk_hash, EgroupStore, OpenEgroup};
use crate::journal::{Journal, FLAG_COMMIT};
use crate::meta::{block_map_batches, cql_str, json_params, now_ms, Daruk};
use crate::overlay::Overlay;
use crate::peer::{self, PeerClient, Request};

/// Guest writes larger than this become several journal records terminated by one commit
/// marker. Bounded so a single enormous write cannot pin an unbounded buffer.
pub const MAX_RECORD: usize = 1 << 20;

/// Rows per CQL batch when a drain commits its map. Large enough that a big drain is a
/// handful of round trips, small enough to stay clear of Scylla's batch size warnings.
const MAP_ROWS_PER_BATCH: usize = 100;

#[derive(Clone, Debug)]
pub struct ExtentLoc {
    pub egroup_id: String,
    pub offset: u32,
    pub length: u32,
}

pub struct Vdisk {
    pub id: String,
    pub size: u64,
    pub epoch: u64,
    pub class: String,
    pub extent_bytes: u64,
    pub drain_seq: u64,

    vh: u64,
    node: String,
    journal: Journal,
    overlay: Overlay,
    map: BTreeMap<u64, ExtentLoc>,
    store: EgroupStore,
    open_eg: Option<OpenEgroup>,
    high_water: u64,
    daruk: Daruk,
    /// The replica set exactly as the map records it, this node included.
    ///
    /// Distinct from `replicas` below, which holds only the peers this node dials -- a
    /// node does not replicate to itself over TCP. Conflating the two made the heal's
    /// compare-and-swap condition the wrong list, so it never matched and every heal
    /// backed itself out reporting a race that had not happened.
    map_replicas: Vec<String>,
    /// The peers an append must reach before it is acknowledged. Write-all, not quorum:
    /// the takeover proof in ownership.md is three lines *because* fencing one replica
    /// stops the old owner (it needed all of them) and reading one replica sees every
    /// acknowledged write. Quorum buys availability during single-replica loss and costs
    /// exactly that proof.
    replicas: Vec<Arc<PeerClient>>,
    /// Set when a drain fails after its bytes are durable. Reads stay correct (the
    /// overlay still holds the newest data), but the journal must not be truncated and
    /// the condition has to be visible rather than retried into silence.
    pub degraded: Option<String>,
}

pub struct VdiskConfig {
    pub root: PathBuf,
    pub node: String,
    pub high_water: u64,
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
            "SELECT vdisk_id, size_bytes, class, epoch, drain_seq, extent_bytes, egroup_bytes \
             FROM hydra.dfs_vdisks WHERE vdisk_id = {}",
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
            .unwrap_or("rw")
            .to_string();
        let drain_seq = field_u64(row, "drain_seq").unwrap_or(0);

        let store = EgroupStore::new(&cfg.root.join("egroups"), egroup_bytes)?;
        let mut journal = Journal::open(&cfg.root.join("journal").join(format!("{id}.jrn")))?;

        let mut v = Vdisk {
            id: id.to_string(),
            size,
            epoch,
            class,
            extent_bytes,
            drain_seq,
            vh: vdisk_hash(id),
            node: cfg.node.clone(),
            overlay: Overlay::new(),
            map: BTreeMap::new(),
            store,
            open_eg: None,
            high_water: cfg.high_water,
            daruk,
            replicas,
            map_replicas,
            degraded: None,
            journal: Journal::open(&cfg.root.join("journal").join(format!("{id}.jrn")))?,
        };
        // `journal` above was opened twice during construction; keep the first handle and
        // drop the duplicate so there is exactly one writer to the file.
        std::mem::swap(&mut v.journal, &mut journal);
        drop(journal);

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
            "SELECT extent_index, egroup_id, egroup_offset, length FROM hydra.dfs_block_map \
             WHERE vdisk_id = {}",
            cql_str(&self.id)
        ))?;
        for row in rows {
            let idx = field_u64(&row, "extent_index")?;
            let egroup_id = row
                .get("egroup_id")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::meta("block map row without egroup_id".to_string()))?
                .to_string();
            let offset = field_u64(&row, "egroup_offset")? as u32;
            let length = field_u64(&row, "length")? as u32;
            self.map.insert(idx, ExtentLoc { egroup_id, offset, length });
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

    pub fn extent_len(&self, index: u64) -> u64 {
        let start = index * self.extent_bytes;
        if start >= self.size {
            0
        } else {
            self.extent_bytes.min(self.size - start)
        }
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
        for idx in first..=last {
            let loc = match self.map.get(&idx) {
                Some(l) => l.clone(),
                None => continue,
            };
            let extent = match self
                .store
                .read_extent(&loc.egroup_id, loc.offset, loc.length, self.vh, idx)
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

    /// Append a guest write to the journal and acknowledge it. The only durability call
    /// on this path is the journal's own `sync_data`.
    pub fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        if self.class == "immutable" {
            return Err(Error::refused(format!(
                "vdisk {} is an immutable image and cannot be written",
                self.id
            )));
        }
        if data.is_empty() {
            return Ok(());
        }
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| Error::refused("write offset overflows".to_string()))?;
        if end > self.size {
            return Err(Error::refused(format!(
                "write {offset}+{} runs past the end of vdisk {} ({} bytes)",
                data.len(),
                self.id,
                self.size
            )));
        }

        // Split oversized writes, marking only the final record. Replay applies the group
        // or none of it, so a crash mid-split cannot expose a prefix of a guest write.
        let mut chunks: Vec<(u64, &[u8])> = Vec::new();
        let mut pos = 0usize;
        while pos < data.len() {
            let n = MAX_RECORD.min(data.len() - pos);
            chunks.push((offset + pos as u64, &data[pos..pos + n]));
            pos += n;
        }
        let last = chunks.len() - 1;
        let mut written: Vec<(u64, u32, u64)> = Vec::with_capacity(chunks.len());
        for (i, (off, chunk)) in chunks.into_iter().enumerate() {
            let flags = if i == last { FLAG_COMMIT } else { 0 };
            let rec = self.journal.append(self.epoch, off, flags, chunk)?;
            // Every replica, before the guest hears anything. A partial write-all is not
            // an acknowledged write: if any replica refuses or cannot be reached, this
            // returns an error and the guest sees EIO, which is the honest outcome --
            // acknowledging on a subset would mean the takeover proof's "read one replica
            // sees every acknowledged write" is false.
            self.replicate(&rec.framed)?;
            written.push((off, rec.data_len, rec.data_pos));
        }
        // Overlay updates only after every record is durable, so a partially written
        // group never becomes visible to a read.
        for (off, len, pos) in written {
            self.overlay.insert(off, len, pos);
        }

        if self.journal.len() >= self.high_water && self.degraded.is_none() {
            if let Err(e) = self.drain() {
                // The write is already acknowledged and still readable from the overlay.
                // Record the failure, stop draining, and let the operator see it rather
                // than retrying forever against a full disk or an unreachable Hydra.
                self.degraded = Some(e.to_string());
                eprintln!("sidon: vdisk {}: drain failed: {e}", self.id);
            }
        }
        Ok(())
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

    /// Ship one extent (payload plus footer) to every replica.
    fn replicate_extent(&mut self, egroup_id: &str, offset: u64, framed: &[u8]) -> Result<()> {
        for replica in &self.replicas {
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
                    "replica {} refused extent group {egroup_id} at offset {offset} \
                     with status {}",
                    replica.node, resp.status
                )));
            }
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
            if crate::extent::verify_footer(data, footer, self.vh, idx).is_ok() {
                return Some(data.to_vec());
            }
            eprintln!(
                "sidon: vdisk {}: replica {} also has a damaged copy of extent {idx}",
                self.id, replica.node
            );
        }
        None
    }

    /// Ship one framed journal record to every replica and wait for all of them.
    fn replicate(&mut self, framed: &[u8]) -> Result<()> {
        if self.replicas.is_empty() {
            return Ok(());
        }
        for replica in &self.replicas {
            let resp = replica.call(&Request {
                opcode: peer::OP_APPEND,
                vdisk: self.id.clone(),
                epoch: self.epoch,
                seq: 0,
                offset: 0,
                flags: 0,
                data: framed.to_vec(),
            })?;
            if resp.status == peer::ST_STALE_EPOCH {
                // Deposed. Not an I/O problem to retry -- somebody else owns this disk
                // now, and the correct behaviour is to stop, loudly and immediately.
                self.degraded = Some(format!(
                    "deposed: replica {} is fenced at epoch {}, this owner holds {}",
                    replica.node, resp.epoch, self.epoch
                ));
                return Err(Error::refused(format!(
                    "vdisk {} is no longer owned by this node: replica {} is fenced at \
                     epoch {} and refused a write at epoch {}",
                    self.id, replica.node, resp.epoch, self.epoch
                )));
            }
            if !resp.is_ok() {
                return Err(Error::io(format!(
                    "replica {} refused a journal append for {} with status {}",
                    replica.node, self.id, resp.status
                )));
            }
        }
        Ok(())
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

    /// Tell every replica the journal has been drained and may be dropped.
    fn replicate_truncate(&mut self) -> Result<()> {
        for replica in &self.replicas {
            // A failure here wastes disk on a replica; it does not endanger data, because
            // the map already points at the drained extents. Logged, not fatal.
            if let Err(e) = replica.call(&Request {
                opcode: peer::OP_TRUNCATE,
                vdisk: self.id.clone(),
                epoch: self.epoch,
                seq: 0,
                offset: 0,
                flags: 0,
                data: Vec::new(),
            }) {
                eprintln!(
                    "sidon: vdisk {}: replica {} did not drop its drained journal: {e}",
                    self.id, replica.node
                );
            }
        }
        Ok(())
    }

    /// Every acknowledged write is already durable, so a flush has nothing left to do.
    /// It is not a lie by omission: the journal calls `sync_data` before the write is
    /// acknowledged, which is strictly stronger than what a flush would promise.
    pub fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn write_zeroes(&mut self, offset: u64, len: u64) -> Result<()> {
        let mut remaining = len;
        let mut at = offset;
        let zeros = vec![0u8; MAX_RECORD];
        while remaining > 0 {
            let n = (MAX_RECORD as u64).min(remaining) as usize;
            self.write(at, &zeros[..n])?;
            at += n as u64;
            remaining -= n as u64;
        }
        Ok(())
    }

    pub fn needs_drain(&self) -> bool {
        !self.overlay.is_empty()
    }

    /// Move everything in the overlay into extent groups and commit the map.
    ///
    /// Read-modify-write per extent, then redirect-on-write: the extent's current bytes
    /// are read, the overlay is applied on top, and the result is appended somewhere new.
    /// The old location becomes garbage rather than being overwritten, because a sealed
    /// egroup is immutable and that is what makes repair and snapshots cheap.
    pub fn drain(&mut self) -> Result<()> {
        if self.overlay.is_empty() {
            return Ok(());
        }

        let mut touched: HashSet<u64> = HashSet::new();
        for seg in self.overlay.iter() {
            let first = seg.start / self.extent_bytes;
            let last = (seg.end() - 1) / self.extent_bytes;
            for idx in first..=last {
                touched.insert(idx);
            }
        }
        let mut indices: Vec<u64> = touched.into_iter().collect();
        indices.sort_unstable();

        let mut new_rows: Vec<(u64, String, u32, u32)> = Vec::with_capacity(indices.len());
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
            if let Some(loc) = self.map.get(&idx).cloned() {
                let cur = self
                    .store
                    .read_extent(&loc.egroup_id, loc.offset, loc.length, self.vh, idx)?;
                let n = cur.len().min(ext_len);
                buf[..n].copy_from_slice(&cur[..n]);
            }

            for seg in self.overlay.overlapping(ext_start, ext_start + ext_len as u64) {
                let copy_start = ext_start.max(seg.start);
                let copy_end = (ext_start + ext_len as u64).min(seg.end());
                if copy_end <= copy_start {
                    continue;
                }
                let skip = copy_start - seg.start;
                let n = (copy_end - copy_start) as usize;
                let data = self.journal.read_at(seg.data_pos + skip, n)?;
                let dst = (copy_start - ext_start) as usize;
                buf[dst..dst + n].copy_from_slice(&data);
            }

            let eg_id = self.ensure_open_egroup()?;
            let (offset, framed) = {
                let store = &self.store;
                let eg = self.open_eg.as_mut().expect("ensure_open_egroup set it");
                store.append_framed(eg, &buf, self.vh, idx)?
            };
            // The same bytes to every replica, extent plus footer. Without this a drained
            // extent exists once: the journal is replicated, so an un-drained write
            // survives a node loss, and draining it would *reduce* its durability. Data
            // that becomes less safe by being tidied up is not a tidy-up.
            self.replicate_extent(&eg_id, offset as u64, &framed)?;
            new_rows.push((idx, eg_id.clone(), offset, ext_len as u32));
            new_locs.push((idx, ExtentLoc { egroup_id: eg_id, offset, length: ext_len as u32 }));

            let full = self
                .open_eg
                .as_ref()
                .map(|eg| self.store.is_full(eg))
                .unwrap_or(false);
            if full {
                sealed.push(self.seal_open_egroup()?);
            }
        }

        // Rule 2: bytes durable before any row points at them.
        if let Some(eg) = self.open_eg.as_mut() {
            self.store.sync(eg)?;
        }

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
        self.drain_seq = next;

        for (idx, loc) in new_locs {
            self.map.insert(idx, loc);
        }
        for id in sealed {
            eprintln!("sidon: vdisk {}: sealed extent group {id}", self.id);
        }

        // Rule 3: only now is the journal allowed to forget -- here and on every replica.
        self.overlay.clear();
        self.journal.reset()?;
        self.replicate_truncate()?;
        Ok(())
    }

    fn ensure_open_egroup(&mut self) -> Result<String> {
        if let Some(eg) = &self.open_eg {
            if !self.store.is_full(eg) {
                return Ok(eg.id.clone());
            }
            let id = self.seal_open_egroup()?;
            eprintln!("sidon: vdisk {}: sealed extent group {id}", self.id);
        }
        let id = format!("eg-{}-{:x}", &self.id, now_ms() as u64 ^ self.journal.next_seq());
        let eg = self.store.create(&id)?;
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
        self.store.sync(&mut eg)?;
        let hash = self.store.seal_hash(&eg.id)?;
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
        if !cas.applied {
            return Err(Error::meta(format!(
                "extent group {} could not be sealed: it is in state {}",
                eg.id,
                cas.current_str("state")
            )));
        }
        Ok(eg.id)
    }

    /// Called at detach. Drains what is left so a clean shutdown leaves an empty journal.
    pub fn close(&mut self) -> Result<()> {
        if self.overlay.is_empty() {
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
        held
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
            "overlay_segments": self.overlay.len(),
            "mapped_extents": self.map.len(),
            "replicas": self.replicas.iter().map(|r| r.node.clone()).collect::<Vec<_>>(),
            "degraded": self.degraded,
        })
    }
}

fn field_u64(row: &Value, name: &str) -> Result<u64> {
    match row.get(name) {
        Some(Value::Number(n)) if n.is_i64() => Ok(n.as_i64().unwrap_or(0).max(0) as u64),
        Some(Value::Number(n)) if n.is_u64() => Ok(n.as_u64().unwrap_or(0)),
        Some(Value::String(s)) => s
            .parse::<u64>()
            .map_err(|_| Error::meta(format!("column '{name}' is not a number: {s:?}"))),
        _ => Err(Error::meta(format!("row is missing numeric column '{name}'"))),
    }
}
