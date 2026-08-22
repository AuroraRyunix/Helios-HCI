//! A vdisk: the journal, the overlay, the extent map, and the drain that moves bytes
//! from the first to the third.
//!
//! The ordering rules enforced here are the ones that make the whole design defensible,
//! so they are stated once and never departed from:
//!
//! 1. **A write is acknowledged when its journal record is durable, and not before.**
//! 2. **Extent bytes are durable before any map row points at them.** A crash between
//!    the two leaves orphaned bytes, which the curator sweeps. The reverse ordering
//!    leaves a map pointing at bytes that do not exist, which is data loss.
//! 3. **The journal is not truncated until the map commit has been applied.** A crash
//!    between the two replays records that are already drained, which is idempotent.
//! 4. **Nothing on the guest's write path talks to Hydra.**

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use crate::err::{Error, Result};
use crate::extent::{vdisk_hash, EgroupStore, OpenEgroup};
use crate::journal::{Journal, FLAG_COMMIT};
use crate::meta::{block_map_batches, cql_str, json_params, now_ms, Daruk};
use crate::overlay::Overlay;

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
            degraded: None,
            journal: Journal::open(&cfg.root.join("journal").join(format!("{id}.jrn")))?,
        };
        // `journal` above was opened twice during construction; keep the first handle and
        // drop the duplicate so there is exactly one writer to the file.
        std::mem::swap(&mut v.journal, &mut journal);
        drop(journal);

        v.load_map()?;
        let discarded = v.replay_journal()?;
        if discarded > 0 {
            eprintln!(
                "sidon: vdisk {id}: discarded {discarded} bytes of unacknowledged journal tail"
            );
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
            let extent = self
                .store
                .read_extent(&loc.egroup_id, loc.offset, loc.length, self.vh, idx)?;
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
            let offset = {
                let store = &self.store;
                let eg = self.open_eg.as_mut().expect("ensure_open_egroup set it");
                store.append(eg, &buf, self.vh, idx)?
            };
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

        // Rule 3: only now is the journal allowed to forget.
        self.overlay.clear();
        self.journal.reset()?;
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

/// Where a vdisk's files live, for callers that need to clean up after a delete.
pub fn vdisk_paths(root: &Path, id: &str) -> (PathBuf, PathBuf) {
    (root.join("journal").join(format!("{id}.jrn")), root.join("egroups"))
}
