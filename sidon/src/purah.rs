//! Purah.
//!
//! Three jobs, all of them background, none of them on the guest's path: reclaim extent
//! groups nothing points at, verify sealed groups against the hash taken when they were
//! known good, and (when peers exist) restore the replica count after a node is lost.
//!
//! **Reclamation is mark-sweep, and there are no reference counts anywhere.** A refcount
//! is a distributed counter with a crash window between every data operation and its
//! count operation, and every clone is an opportunity to double or forget one. The schema
//! is forbidden a refcount column for exactly this reason. Mark-sweep pays for that with
//! a full scan of the block map, which is why this runs on an interval measured in
//! minutes and never in response to a delete.
//!
//! The safety rule the whole sweep rests on: **an extent group is deleted only after it
//! has been observed unreferenced twice, with a grace period between the observations,
//! and only if it is not open, not young, and not held by an attached vdisk.** Each of
//! those guards a different way a live group can look like garbage:
//!
//! - *Twice, with a gap*: a drain writes egroup bytes before it commits the map rows that
//!   point at them (data before metadata). A single scan landing in that window sees a
//!   group nothing references. It is not garbage; it is thirty milliseconds from being
//!   referenced.
//! - *Not young*: the same window, for a group created between two scans.
//! - *Not open*: an open group is the drain's current target.
//! - *Not held*: a vdisk attached here has map entries in memory that may be ahead of a
//!   stale read.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::err::{Error, Result};
use crate::extent::EgroupStore;
use crate::meta::{cql_str, json_params, Daruk};

pub struct Purah {
    daruk: Daruk,
    store: EgroupStore,
    node: String,
    grace: Duration,
    /// egroup_id -> when it was first seen unreferenced. Cleared the moment a group is
    /// seen referenced again, so a reused or re-referenced group starts its grace over.
    unreferenced_since: HashMap<String, Instant>,
}

#[derive(Debug, Default)]
pub struct SweepReport {
    pub egroups_known: usize,
    pub egroups_referenced: usize,
    pub candidates: usize,
    pub reclaimed: Vec<String>,
    pub bytes_reclaimed: u64,
    pub skipped_young: usize,
    pub skipped_open: usize,
    pub skipped_held: usize,
    pub skipped_grace: usize,
}

#[derive(Debug, Default)]
pub struct ScrubReport {
    pub checked: usize,
    pub skipped_unsealed: usize,
    pub missing: Vec<String>,
    pub mismatched: Vec<String>,
}

impl Purah {
    pub fn new(daruk: Daruk, store: EgroupStore, node: &str, grace: Duration) -> Purah {
        Purah {
            daruk,
            store,
            node: node.to_string(),
            grace,
            unreferenced_since: HashMap::new(),
        }
    }

    /// Every extent group id the block map currently points at, across all vdisks.
    ///
    /// A full scan of `dfs_block_map`. That is the cost of not keeping reference counts,
    /// and it is paid deliberately -- see the module header. It must be read *before* the
    /// egroup inventory, so that a group created between the two reads appears in the
    /// inventory as unreferenced-and-young rather than being missed entirely.
    fn referenced_egroups(&self) -> Result<HashSet<String>> {
        let rows = self
            .daruk
            .query("SELECT egroup_id FROM hydra.dfs_block_map")?;
        let mut out = HashSet::new();
        for row in rows {
            if let Some(id) = row.get("egroup_id").and_then(Value::as_str) {
                out.insert(id.to_string());
            }
        }
        Ok(out)
    }

    fn my_egroups(&self) -> Result<Vec<(String, String, i64, i64)>> {
        let rows = self.daruk.query(&format!(
            "SELECT egroup_id, state, created_at_ms, size FROM hydra.dfs_egroups WHERE node = {} ALLOW FILTERING",
            cql_str(&self.node)
        ))?;
        let mut out = Vec::new();
        for row in rows {
            let id = match row.get("egroup_id").and_then(Value::as_str) {
                Some(v) => v.to_string(),
                None => continue,
            };
            let state = row
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            let created = row.get("created_at_ms").and_then(Value::as_i64).unwrap_or(0);
            let size = row.get("size").and_then(Value::as_i64).unwrap_or(0);
            out.push((id, state, created, size));
        }
        Ok(out)
    }

    /// One mark-sweep pass. `held` is the set of extent groups attached vdisks are using
    /// right now, which the caller supplies because only it knows what is attached.
    pub fn sweep(&mut self, held: &HashSet<String>, now_ms: i64) -> Result<SweepReport> {
        let mut report = SweepReport::default();

        // Order matters: references first. See referenced_egroups().
        let referenced = self.referenced_egroups()?;
        let inventory = self.my_egroups()?;
        report.egroups_known = inventory.len();

        let grace_ms = self.grace.as_millis() as i64;
        let now = Instant::now();
        let mut still_unreferenced: HashMap<String, Instant> = HashMap::new();

        for (id, state, created_at_ms, size) in inventory {
            if referenced.contains(&id) {
                report.egroups_referenced += 1;
                // Seen referenced: any grace it had accumulated is void.
                continue;
            }
            if state == "open" {
                report.skipped_open += 1;
                continue;
            }
            if held.contains(&id) {
                report.skipped_held += 1;
                continue;
            }
            if created_at_ms > 0 && now_ms - created_at_ms < grace_ms {
                report.skipped_young += 1;
                // Still record the observation so its grace can start ticking.
                let first = self.unreferenced_since.get(&id).copied().unwrap_or(now);
                still_unreferenced.insert(id, first);
                continue;
            }

            let first_seen = match self.unreferenced_since.get(&id) {
                Some(t) => *t,
                None => {
                    // First observation. It gets no further than this on this pass --
                    // this is the second half of the two-scan rule.
                    still_unreferenced.insert(id, now);
                    report.skipped_grace += 1;
                    continue;
                }
            };
            if now.duration_since(first_seen) < self.grace {
                still_unreferenced.insert(id, first_seen);
                report.skipped_grace += 1;
                continue;
            }

            report.candidates += 1;
            match self.reclaim(&id, &state) {
                Ok(()) => {
                    report.reclaimed.push(id);
                    report.bytes_reclaimed += size.max(0) as u64;
                }
                Err(e) => {
                    eprintln!("purah: could not reclaim extent group {id}: {e}");
                    still_unreferenced.insert(id, first_seen);
                }
            }
        }

        self.unreferenced_since = still_unreferenced;
        Ok(report)
    }

    /// Mark dead in the map, then remove the file. That order, always: a file removed
    /// before the map forgets it is a map row pointing at nothing, which reads as data
    /// loss. A row marked dead whose file still exists is a wasted block and a warning.
    fn reclaim(&self, id: &str, current_state: &str) -> Result<()> {
        let cas = self.daruk.cas(
            "/v1/dfs/egroup-state",
            json_params(vec![
                ("egroup_id", json!(id)),
                ("state", json!("dead")),
                ("seal_hash", json!("")),
                ("size", json!(0)),
                ("expected_state", json!(current_state)),
            ]),
        )?;
        if !cas.applied {
            return Err(Error::refused(format!(
                "extent group {id} changed state to {} while it was being reclaimed",
                cas.current_str("state")
            )));
        }
        let path = self.store.path_for(id);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::io(format!("removing {}: {e}", path.display()))),
        }
        self.daruk.query(&format!(
            "DELETE FROM hydra.dfs_egroups WHERE egroup_id = {}",
            cql_str(id)
        ))?;
        Ok(())
    }

    /// Recompute every sealed group's hash and compare it with the one recorded at seal
    /// time. Sealed means immutable, so any difference is damage -- there is no benign
    /// reason for one of these to change.
    ///
    /// Scrub needs no lock precisely because of that immutability, which is one of the
    /// things sealing buys.
    pub fn scrub(&self) -> Result<ScrubReport> {
        let mut report = ScrubReport::default();
        for (id, state, _created, _size) in self.my_egroups()? {
            if state != "sealed" {
                report.skipped_unsealed += 1;
                continue;
            }
            let rows = self.daruk.query(&format!(
                "SELECT seal_hash FROM hydra.dfs_egroups WHERE egroup_id = {}",
                cql_str(&id)
            ))?;
            let recorded = rows
                .first()
                .and_then(|r| r.get("seal_hash"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if recorded.is_empty() {
                continue;
            }
            if !self.store.path_for(&id).exists() {
                report.missing.push(id);
                continue;
            }
            let actual = self.store.seal_hash(&id)?;
            report.checked += 1;
            if actual != recorded {
                eprintln!(
                    "purah: SCRUB FAILURE: extent group {id} hashes {actual}, sealed as {recorded}"
                );
                report.mismatched.push(id);
            }
        }
        Ok(report)
    }

}

impl SweepReport {
    pub fn to_json(&self) -> Value {
        json!({
            "egroups_known": self.egroups_known,
            "egroups_referenced": self.egroups_referenced,
            "candidates": self.candidates,
            "reclaimed": self.reclaimed,
            "bytes_reclaimed": self.bytes_reclaimed,
            "skipped_open": self.skipped_open,
            "skipped_held": self.skipped_held,
            "skipped_young": self.skipped_young,
            "skipped_awaiting_grace": self.skipped_grace,
        })
    }
}

impl ScrubReport {
    pub fn to_json(&self) -> Value {
        json!({
            "checked": self.checked,
            "skipped_unsealed": self.skipped_unsealed,
            "missing": self.missing,
            "mismatched": self.mismatched,
            "clean": self.missing.is_empty() && self.mismatched.is_empty(),
        })
    }
}
