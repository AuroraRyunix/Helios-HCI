//! Reclaiming the copies a node holds on behalf of other nodes (D-33).
//!
//! The sweep frees a group on the node that created it. Every other copy -- the ones in
//! `replica-egroups/` on the nodes that replicate it -- used to stay for ever, because
//! nothing told those nodes the group was gone and nothing on them looked. This is both
//! halves of fixing that:
//!
//! * [`ReplicaReaper::drop_declared_dead`] answers the owner's `OP_EGROUP_DROP`: "I have
//!   declared these groups dead, drop your copies."
//! * [`ReplicaReaper::scan`] is the backstop that needs no message: it finds replica copies
//!   whose group Hydra no longer knows, and drops them under the sweep's own two-scan rule.
//!   It covers a replica that was down when the owner swept, an owner that crashed between
//!   marking a group dead and asking, an old owner that never asks, and every orphan that
//!   was left before any of this existed.
//!
//! **A replica never takes the sender's word for it.** The bytes on a replica are, for a
//! group the owner has lost its own copy of, the only copy there is, so the question "is this
//! group dead" is asked of Hydra by the replica itself, and the answer must be an affirmative
//! one:
//!
//! 1. the group's row exists, says `dead`, and names the sender as the node that created it
//!    (only the creator reclaims a group; a different node asking is refused);
//! 2. nothing in the block map, and nothing in the extent id map, points into the group.
//!
//! A row that is missing, or `open`, or `sealed`, is a refusal. A Hydra read that fails is
//! an error and drops nothing: this module never reads silence as permission.
//!
//! There is no vdisk epoch on a group, because a group is not owned by a vdisk -- clones and
//! snapshots share one -- so the epoch of whichever vdisk happened to write it would fence
//! the wrong thing. What a deposed or stale sender cannot do is make Hydra say `dead`: that
//! is a lightweight transaction (`egroup-state`, conditional on the state it leaves) which a
//! node that cannot reach a quorum cannot perform, and the replica then looks at the block map
//! again itself, so even a sender that marked a group dead wrongly -- a drain committed a
//! reference after its scans -- does not take the last copy with it.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::err::{Error, Result};
use crate::extent_id_map::{referenced_egroups, Rows};
use crate::meta::cql_str;
use crate::peer::{valid_group_id, ReplicaStore};

/// What a replica decided about one group it was asked to drop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The copy existed, was proved dead and was removed; this many bytes were freed.
    Dropped(u64),
    /// There was no copy here. The goal state, so it counts as success.
    Absent,
    /// There is a copy and it was kept, for the stated reason.
    Refused(String),
    /// There is a copy and it was kept because the block map still points into the group,
    /// which means the sender's two scans missed a reference. Distinct from a refusal because
    /// the sender must not erase the evidence (it leaves the row `dead`).
    Referenced,
}

pub fn verdicts_to_json(verdicts: &[(String, Outcome)]) -> Value {
    let rows: Vec<Value> = verdicts
        .iter()
        .map(|(id, outcome)| match outcome {
            Outcome::Dropped(bytes) => json!({"id": id, "outcome": "dropped", "bytes": bytes}),
            Outcome::Absent => json!({"id": id, "outcome": "absent"}),
            Outcome::Refused(why) => json!({"id": id, "outcome": "refused", "reason": why}),
            Outcome::Referenced => json!({"id": id, "outcome": "referenced"}),
        })
        .collect();
    json!({ "results": rows })
}

/// Parse a replica's answer. Anything unrecognised is an error and not a guess, so a reply
/// from some future build is never counted as a drop it did not report.
pub fn verdicts_from_json(doc: &Value) -> Result<Vec<(String, Outcome)>> {
    let rows = doc
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::io("a drop answer has no 'results'".to_string()))?;
    let mut out = Vec::new();
    for row in rows {
        let id = row
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::io("a drop verdict names no group".to_string()))?
            .to_string();
        let outcome = match row.get("outcome").and_then(Value::as_str) {
            Some("dropped") => Outcome::Dropped(row.get("bytes").and_then(Value::as_u64).unwrap_or(0)),
            Some("absent") => Outcome::Absent,
            Some("referenced") => Outcome::Referenced,
            Some("refused") => Outcome::Refused(
                row.get("reason").and_then(Value::as_str).unwrap_or("no reason given").to_string(),
            ),
            other => return Err(Error::io(format!("a drop verdict for {id} says {other:?}"))),
        };
        out.push((id, outcome));
    }
    Ok(out)
}

/// What Hydra says about one group, as far as a replica needs it.
enum Row {
    Missing,
    Present { state: String, node: String },
}

fn row_of<D: Rows>(db: &D, id: &str) -> Result<Row> {
    let rows = db.rows(&format!(
        "SELECT state, node FROM hydra.dfs_egroups WHERE egroup_id = {}",
        cql_str(id)
    ))?;
    Ok(match rows.first() {
        None => Row::Missing,
        Some(r) => Row::Present {
            state: r.get("state").and_then(Value::as_str).unwrap_or("").to_string(),
            node: r.get("node").and_then(Value::as_str).unwrap_or("").to_string(),
        },
    })
}

/// What one orphan scan found.
#[derive(Debug, Default)]
pub struct ScanReport {
    /// Replica copies on this node, and the bytes they occupy.
    pub scanned: usize,
    pub bytes_held: u64,
    /// Copies whose group Hydra still lists as live: left alone, whatever this node's place
    /// in that group's replica set.
    pub live: usize,
    /// Copies of a group Hydra does not list, too recently written to judge.
    pub young: usize,
    /// Copies of a dead or unlisted group seen for the first time, or within the grace of
    /// the first sighting.
    pub awaiting_grace: usize,
    pub dropped: Vec<String>,
    pub bytes_dropped: u64,
    /// Things that should not be and that this scan will not resolve by deleting: a group the
    /// block map points into whose row is missing or dead, and removals that failed.
    pub anomalies: Vec<String>,
}

impl ScanReport {
    pub fn to_json(&self) -> Value {
        json!({
            "scanned": self.scanned,
            "bytes_held": self.bytes_held,
            "live": self.live,
            "young": self.young,
            "awaiting_grace": self.awaiting_grace,
            "dropped": self.dropped,
            "bytes_dropped": self.bytes_dropped,
            "anomalies": self.anomalies,
        })
    }
}

/// The replica-side reclaimer. One per daemon, because the two-scan ledger must outlive a
/// scan (a fresh one each time could drop on first sight).
pub struct ReplicaReaper {
    grace: Duration,
    /// group id -> when this node first saw its copy orphaned. Rebuilt at the end of every
    /// scan from what that scan still found orphaned, so a copy that turns out live, or is
    /// referenced, or disappears, starts over if it ever reappears.
    seen: Mutex<HashMap<String, Instant>>,
}

impl ReplicaReaper {
    pub fn new(grace: Duration) -> ReplicaReaper {
        ReplicaReaper { grace, seen: Mutex::new(HashMap::new()) }
    }

    /// Answer a drop request from `from`. See the module header for what is checked.
    ///
    /// One Hydra scan of the block map serves the whole request, and only if this node holds
    /// a copy of at least one named group: a node with nothing to drop does no work.
    pub fn drop_declared_dead<D: Rows>(
        &self,
        db: &D,
        store: &ReplicaStore,
        from: &str,
        ids: &[String],
    ) -> Result<Vec<(String, Outcome)>> {
        let mut out: Vec<(String, Outcome)> = Vec::new();
        let mut held: Vec<&String> = Vec::new();
        for id in ids {
            if !valid_group_id(id) {
                out.push((id.clone(), Outcome::Refused("not an extent group id".to_string())));
            } else if !store.has_egroup(id) {
                out.push((id.clone(), Outcome::Absent));
            } else {
                held.push(id);
            }
        }
        if held.is_empty() {
            return Ok(out);
        }

        // The rows first, then the references, so the references are the freshest thing read.
        let mut declared: Vec<&String> = Vec::new();
        for id in held {
            match row_of(db, id)? {
                Row::Missing => out.push((
                    id.clone(),
                    Outcome::Refused("Hydra has no row for it; an orphan is found by the scan, not dropped on request".to_string()),
                )),
                Row::Present { state, node } => {
                    if state != "dead" {
                        out.push((id.clone(), Outcome::Refused(format!("Hydra says {state}, not dead"))));
                    } else if node != from {
                        out.push((
                            id.clone(),
                            Outcome::Refused(format!("created by {node}, and only its creator may declare it dead; {from} asked")),
                        ));
                    } else {
                        declared.push(id);
                    }
                }
            }
        }
        if declared.is_empty() {
            return Ok(out);
        }
        let referenced = referenced_egroups(db)?;
        for id in declared {
            if referenced.contains(id) {
                eprintln!(
                    "purah: {from} declared extent group {id} dead but the block map still points into it; \
                     this replica keeps its copy"
                );
                out.push((id.clone(), Outcome::Referenced));
                continue;
            }
            match store.remove_egroup(id) {
                Ok(Some(bytes)) => out.push((id.clone(), Outcome::Dropped(bytes))),
                Ok(None) => out.push((id.clone(), Outcome::Absent)),
                Err(e) => out.push((id.clone(), Outcome::Refused(format!("could not remove it: {e}")))),
            }
        }
        Ok(out)
    }

    /// Find replica copies of groups that no longer exist, and drop those that have been
    /// orphaned for two scans, at least the grace apart.
    ///
    /// A copy is an orphan when Hydra has no row for its group, or the row says `dead`, **and**
    /// nothing references it, **and** it was last written longer ago than the grace. It is
    /// removed on the first scan at which it has *also* been seen orphaned for the grace
    /// (so: not on first sight, whatever its age).
    ///
    /// Why a row-less group can be dead and not merely new: a drain registers a group's row
    /// before it sends the first extent, a heal copies into groups whose rows exist, and the
    /// one writer that sends before it registers -- compaction -- registers within one pass,
    /// seconds, long inside the grace the age test and the second sighting each impose.
    ///
    /// `now` is injected so that a test can place the second scan beyond the grace without
    /// sleeping.
    ///
    /// On any error nothing is dropped and the ledger is left as it was.
    pub fn scan<D: Rows>(&self, db: &D, store: &ReplicaStore, now: Instant) -> Result<ScanReport> {
        let mut report = ScanReport::default();
        let files = store.list_egroups()?;
        report.scanned = files.len();
        report.bytes_held = files.iter().map(|f| f.size).sum();
        if files.is_empty() {
            *self.seen.lock().expect("reaper ledger poisoned") = HashMap::new();
            return Ok(report);
        }

        // References first, then the rows: a group registered and pointed at between the two
        // reads shows up as live, never as an orphan. The files were listed before either, and
        // a file that appears after the listing is simply not judged this time.
        let referenced = referenced_egroups(db)?;
        let rows = db.rows("SELECT egroup_id, state FROM hydra.dfs_egroups")?;
        let mut state_of: HashMap<String, String> = HashMap::new();
        for r in &rows {
            if let Some(id) = r.get("egroup_id").and_then(Value::as_str) {
                state_of.insert(
                    id.to_string(),
                    r.get("state").and_then(Value::as_str).unwrap_or("").to_string(),
                );
            }
        }

        let mut previous = self.seen.lock().expect("reaper ledger poisoned").clone();
        let mut next: HashMap<String, Instant> = HashMap::new();
        for file in files {
            let id = file.id;
            let listed = state_of.get(&id);
            let orphaned = match listed {
                None => true,
                Some(state) => state == "dead",
            };
            if !orphaned {
                report.live += 1;
                continue;
            }
            if referenced.contains(&id) {
                // The map points into a group Hydra has no live row for. Deleting the copy
                // would make a bad state unrecoverable, so it is reported and left.
                report.anomalies.push(format!(
                    "{id}: the block map points into it but Hydra has {}; the copy is kept",
                    if listed.is_some() { "it dead" } else { "no row for it" }
                ));
                continue;
            }
            if file.age < self.grace {
                report.young += 1;
                continue;
            }
            let first = previous.remove(&id).unwrap_or(now);
            if now.saturating_duration_since(first) < self.grace {
                report.awaiting_grace += 1;
                next.insert(id, first);
                continue;
            }
            match store.remove_egroup(&id) {
                Ok(bytes) => {
                    report.bytes_dropped += bytes.unwrap_or(0);
                    report.dropped.push(id);
                }
                Err(e) => {
                    report.anomalies.push(format!("{id}: could not be removed: {e}"));
                    next.insert(id, first);
                }
            }
        }
        *self.seen.lock().expect("reaper ledger poisoned") = next;
        Ok(report)
    }
}
