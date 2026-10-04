//! The mark-sweep pass itself, written against [`Db`] and [`ReplicaPeers`] so that it can be
//! run against a model of Hydra with failure injected, which the daemon's concrete client does
//! not allow. `Purah::sweep` is a thin caller of [`sweep_pass`]; the rule it implements, and
//! why each guard exists, is in the header of `purah.rs`.
//!
//! What this file adds to that rule is the order of a reclaim, because since D-33 a reclaim
//! has a step that crosses nodes:
//!
//! 1. compare-and-swap the group's row to `dead` (Hydra now says it is gone);
//! 2. remove this node's copies;
//! 3. ask every peer to drop its replica copy, **while the row still says `dead`** -- that row
//!    is what a replica checks, so it must still be there;
//! 4. delete the row (and the access data).
//!
//! A stop after any step is safe. After 1 the group is a dead row with a file, which the next
//! sweep takes through the same steps again (the CAS is conditional on the state it *leaves*,
//! and `dead` -> `dead` is allowed here as it always was). After 3 the replicas are done and
//! the row is a leftover the next sweep removes. A replica that could not be reached in 3 is
//! not retried by the owner: the row goes in 4 regardless, and that replica's own orphan scan
//! (`replica.rs`) finds the copy, because a copy with no row is exactly what it looks for.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::compact::Db;
use super::replica::Outcome;
use super::SweepReport;
use crate::err::{Error, Result};
use crate::extent::EgroupStore;
use crate::heat::AccessLog;
use crate::meta::{cql_str, json_params};
use crate::peer::MAX_DROP_IDS;

/// A peer's answer to "drop your copies of these groups".
pub enum PeerAnswer {
    /// A build from before the opcode. Nothing was dropped and nothing will be.
    Unsupported,
    Verdicts(Vec<(String, Outcome)>),
}

/// The other nodes, as the sweep sees them.
pub trait ReplicaPeers {
    /// Every node that might hold a replica copy of a group this node created. All of them,
    /// not the replica set of a vdisk: the vdisk that wrote a group may be long deleted, and
    /// a node that holds nothing answers in one directory lookup.
    fn nodes(&self) -> Vec<String>;
    fn drop_groups(&self, node: &str, from: &str, ids: &[String]) -> Result<PeerAnswer>;
}

/// What asking one peer to drop its copies came to, for the report.
#[derive(Debug, Default, Clone)]
pub struct PeerDropReport {
    pub node: String,
    pub dropped: usize,
    pub bytes: u64,
    pub absent: usize,
    pub refused: usize,
    /// The first few reasons a peer refused, so that an operator is told why a copy stayed.
    pub refusals: Vec<String>,
    /// Groups a peer said the block map still points into.
    pub referenced: Vec<String>,
    pub unsupported: bool,
    pub error: Option<String>,
}

impl PeerDropReport {
    pub fn to_json(&self) -> Value {
        json!({
            "node": self.node,
            "dropped": self.dropped,
            "bytes": self.bytes,
            "absent": self.absent,
            "refused": self.refused,
            "refusals": self.refusals,
            "unsupported": self.unsupported,
            "error": self.error,
        })
    }
}

/// Ask every peer to drop its copies of `ids`. Never fails: whatever happens is in the
/// report, and a peer that cannot be asked is left to its own scan.
pub fn push_drops(peers: &dyn ReplicaPeers, from: &str, ids: &[String]) -> Vec<PeerDropReport> {
    let mut reports = Vec::new();
    for node in peers.nodes() {
        let mut r = PeerDropReport { node: node.clone(), ..PeerDropReport::default() };
        for chunk in ids.chunks(MAX_DROP_IDS) {
            match peers.drop_groups(&node, from, chunk) {
                Ok(PeerAnswer::Unsupported) => {
                    r.unsupported = true;
                    break;
                }
                Ok(PeerAnswer::Verdicts(verdicts)) => {
                    for (id, outcome) in verdicts {
                        match outcome {
                            Outcome::Dropped(bytes) => {
                                r.dropped += 1;
                                r.bytes += bytes;
                            }
                            Outcome::Absent => r.absent += 1,
                            Outcome::Referenced => {
                                r.refused += 1;
                                r.referenced.push(id.clone());
                                if r.refusals.len() < 5 {
                                    r.refusals.push(format!("{id}: the block map still references it"));
                                }
                            }
                            Outcome::Refused(why) => {
                                r.refused += 1;
                                if r.refusals.len() < 5 {
                                    r.refusals.push(format!("{id}: {why}"));
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    r.error = Some(e.to_string());
                    break;
                }
            }
        }
        reports.push(r);
    }
    reports
}

/// Everything a pass needs from the daemon, bundled so the call reads as what it is.
pub struct Pass<'a, D: Db> {
    pub db: &'a D,
    pub store: &'a EgroupStore,
    pub node: &'a str,
    pub grace: Duration,
    pub access: &'a AccessLog,
    pub peers: &'a dyn ReplicaPeers,
}

fn my_egroups<D: Db>(db: &D, node: &str) -> Result<Vec<(String, String, i64, i64)>> {
    let rows = db.rows(&format!(
        "SELECT egroup_id, state, created_at_ms, size FROM hydra.dfs_egroups WHERE node = {} ALLOW FILTERING",
        cql_str(node)
    ))?;
    let mut out = Vec::new();
    for row in rows {
        let id = match row.get("egroup_id").and_then(Value::as_str) {
            Some(v) => v.to_string(),
            None => continue,
        };
        let state = row.get("state").and_then(Value::as_str).unwrap_or("unknown").to_string();
        let created = row.get("created_at_ms").and_then(Value::as_i64).unwrap_or(0);
        let size = row.get("size").and_then(Value::as_i64).unwrap_or(0);
        out.push((id, state, created, size));
    }
    Ok(out)
}

/// Steps 1 and 2: mark dead in the map, then remove the files. That order, always: a file
/// removed before the map forgets it is a map row pointing at nothing, which reads as data
/// loss. A row marked dead whose file still exists is a wasted block and a warning.
fn mark_dead_and_remove_local<D: Db>(p: &Pass<D>, id: &str, current_state: &str) -> Result<()> {
    let cas = p.db.cas(
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
    // Every copy on this node, including a surplus one a disk-to-disk move left behind.
    p.store.remove_all(id)
}

/// Step 4: the row, and the access data about the group.
fn forget_group<D: Db>(p: &Pass<D>, id: &str) -> Result<()> {
    p.db.rows(&format!("DELETE FROM hydra.dfs_egroups WHERE egroup_id = {}", cql_str(id)))?;
    // The access data is about this extent group, so it dies with it -- the whole
    // partition, every node's row, because the group is gone everywhere and not only
    // here. Last, after the row the group is actually described by, and best-effort:
    // nothing reads these rows except the ranking pass, and a leftover one ranks a
    // group that no inventory lists, which the pass ignores. Failing the reclaim over
    // it would be letting a statistic block the reclamation of disk.
    if let Err(e) = p.db.rows(&format!(
        "DELETE FROM hydra.dfs_egroup_access WHERE egroup_id = {}",
        cql_str(id)
    )) {
        eprintln!("purah: access data for reclaimed extent group {id} could not be deleted: {e}");
    }
    p.access.forget(id);
    Ok(())
}

/// One mark-sweep pass. `held` is the set of extent groups attached vdisks are using right
/// now, which the caller supplies because only it knows what is attached. `ledger` is the
/// curator's memory of when each group was first seen unreferenced; `now` is the instant to
/// measure it against, injected so a test can put the second scan beyond the grace.
pub fn sweep_pass<D: Db>(
    p: &Pass<D>,
    ledger: &mut HashMap<String, Instant>,
    held: &HashSet<String>,
    now_ms: i64,
    now: Instant,
) -> Result<SweepReport> {
    let mut report = SweepReport { grace_seconds: p.grace.as_secs(), ..SweepReport::default() };

    // Order matters: references first. See `referenced_egroups`.
    let referenced = crate::extent_id_map::referenced_egroups(p.db)?;
    let inventory = my_egroups(p.db, p.node)?;
    report.egroups_known = inventory.len();

    let grace_ms = p.grace.as_millis() as i64;
    let mut still_unreferenced: HashMap<String, Instant> = HashMap::new();
    // Marked dead and gone from this node, awaiting the replicas and then the row.
    let mut pending: Vec<(String, u64, Instant)> = Vec::new();

    for (id, state, created_at_ms, size) in inventory {
        if referenced.contains(&id) {
            report.egroups_referenced += 1;
            // Referenced and recorded here, so it should be on one of this node's
            // disks. If it is on none of them the bytes are gone locally -- a disk
            // that failed, was unmounted, or never came back after a reboot.
            if !p.store.path_for(&id).exists() {
                report.missing.push(id.clone());
            }
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
            let first = ledger.get(&id).copied().unwrap_or(now);
            still_unreferenced.insert(id, first);
            continue;
        }

        let first_seen = match ledger.get(&id) {
            Some(t) => *t,
            None => {
                // First observation. It gets no further than this on this pass --
                // this is the second half of the two-scan rule.
                still_unreferenced.insert(id, now);
                report.skipped_grace += 1;
                continue;
            }
        };
        if now.saturating_duration_since(first_seen) < p.grace {
            still_unreferenced.insert(id, first_seen);
            report.skipped_grace += 1;
            continue;
        }

        report.candidates += 1;
        match mark_dead_and_remove_local(p, &id, &state) {
            Ok(()) => pending.push((id, size.max(0) as u64, first_seen)),
            Err(e) => {
                eprintln!("purah: could not reclaim extent group {id}: {e}");
                still_unreferenced.insert(id, first_seen);
            }
        }
    }

    // Step 3, once for the whole pass: one request per peer, however many groups, because a
    // replica reads the block map once per request.
    let mut keep_row: HashSet<String> = HashSet::new();
    if !pending.is_empty() {
        let ids: Vec<String> = pending.iter().map(|(id, _, _)| id.clone()).collect();
        report.replica_drops = push_drops(p.peers, p.node, &ids);
        for drop in &report.replica_drops {
            for id in &drop.referenced {
                keep_row.insert(id.clone());
            }
        }
    }

    for (id, size, first_seen) in pending {
        if keep_row.contains(&id) {
            // A replica found a reference this node's scans missed. The local copy is gone and
            // cannot be restored from here, but the replica kept its own, and the row stays
            // `dead` as the evidence: the next sweep sees the group referenced and reports its
            // file missing, which is true and which nothing else would say.
            report.anomalies.push(format!(
                "extent group {id} was reclaimed here but a replica reports the block map still points into it; \
                 the replica copy was kept and the row left"
            ));
            continue;
        }
        match forget_group(p, &id) {
            Ok(()) => {
                report.reclaimed.push(id);
                report.bytes_reclaimed += size;
            }
            Err(e) => {
                eprintln!("purah: could not reclaim extent group {id}: {e}");
                still_unreferenced.insert(id, first_seen);
            }
        }
    }

    *ledger = still_unreferenced;
    Ok(report)
}

#[cfg(test)]
mod tests;
