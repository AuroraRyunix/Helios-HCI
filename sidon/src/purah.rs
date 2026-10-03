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
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::err::{Error, Result};
use crate::extent::EgroupStore;
use crate::heat::{heat_score, AccessLog, Counts};
use crate::meta::{access_batches, cql_str, json_params, Daruk, ACCESS_BATCH};

pub mod tier;

pub struct Purah {
    daruk: Daruk,
    store: EgroupStore,
    node: String,
    grace: Duration,
    /// egroup_id -> when it was first seen unreferenced. Cleared the moment a group is
    /// seen referenced again, so a reused or re-referenced group starts its grace over.
    unreferenced_since: HashMap<String, Instant>,
    /// The node's extent-group access tally, shared with every attached vdisk.
    ///
    /// Purah is both its writer and its reader: it flushes the in-memory counts to Hydra
    /// and it is the pass that ranks them. That is the same arrangement the sweep already
    /// has -- the curator is the only thing that reads the whole map -- and it keeps the
    /// scoring formula in one place, which matters because a ranking that two components
    /// computed differently would be two different answers to "is this hot".
    access: Arc<AccessLog>,
    /// Surplus copies a disk-to-disk move left behind, and when each was first seen. Swept
    /// under the same two-scan grace as an unreferenced group; see `tier.rs`.
    strays: tier::StrayLedger,
}

#[derive(Debug, Default)]
pub struct SweepReport {
    pub egroups_known: usize,
    pub egroups_referenced: usize,
    /// Extent groups this node is recorded as holding, which are still referenced, and
    /// whose file is on none of its disks.
    ///
    /// This is what a failed disk looks like from here. The sweep already reads the whole
    /// block map to decide what is live and already walks everything this node owns, so
    /// naming the ones that have gone costs one `exists()` per referenced group and needs
    /// no new bookkeeping -- which is the reason the disk a group lives on is a node-local
    /// fact rather than a column in Hydra.
    ///
    /// Reads of these do not fail while a replica holds them: `read_extent_from_replica`
    /// already falls back. What was missing was anyone *saying* so, and a silently
    /// half-empty node is the failure that gets noticed at the worst moment.
    pub missing: Vec<String>,
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

/// What one heat pass found, for whoever is deciding where extent groups should live.
#[derive(Debug, Default)]
pub struct HeatReport {
    /// Extent groups on this node, hottest first.
    pub hot: Vec<Value>,
    /// Extent groups on this node that have an access row, coldest first.
    pub cold: Vec<Value>,
    /// This node's extent groups with no access row at all.
    ///
    /// The coldest class there is, and kept separate from `cold` rather than folded in with
    /// a score of zero, because the two mean different things. A group with a row and a low
    /// score has been measured and found cold; a group with no row has not been measured --
    /// nothing has read or written it since any daemon that holds a replica of it last
    /// started, or the tally was at capacity when it was first touched. A tiering pass may
    /// legitimately spill both, but an operator reading this needs to know which it is
    /// looking at, and `dropped` is what distinguishes the two causes.
    pub unobserved: Vec<String>,
    /// How many there were before the list was truncated to the requested limit. A
    /// truncated list that did not say so would read as "three extent groups have never
    /// been touched" on a node where three thousand have, which is the difference between a
    /// quiet corner of a disk and a node whose tally is not working.
    pub unobserved_count: usize,
    pub inventory: usize,
    pub observed: usize,
    pub tracked_in_memory: usize,
    pub dropped: u64,
}

/// What one flush of the in-memory tally wrote.
#[derive(Debug, Default)]
pub struct AccessFlushReport {
    pub rows: usize,
    pub statements: usize,
    pub since_ms: i64,
    pub updated_at_ms: i64,
    pub dropped: u64,
}

impl Purah {
    pub fn new(
        daruk: Daruk,
        store: EgroupStore,
        node: &str,
        grace: Duration,
        access: Arc<AccessLog>,
    ) -> Purah {
        Purah {
            daruk,
            store,
            node: node.to_string(),
            grace,
            unreferenced_since: HashMap::new(),
            access,
            strays: tier::StrayLedger::default(),
        }
    }

    /// Every extent group id the block map currently points at, across all vdisks.
    ///
    /// A full scan of `dfs_block_map`. That is the cost of not keeping reference counts,
    /// and it is paid deliberately -- see the module header. It must be read *before* the
    /// egroup inventory, so that a group created between the two reads appears in the
    /// inventory as unreferenced-and-young rather than being missed entirely.
    ///
    /// Through **both** map levels (D-23): a row naming an extent keeps alive the group that
    /// extent's row names. On a cluster where nothing names an extent this is the scan it
    /// always was; see `extent_id_map::referenced_egroups`.
    fn referenced_egroups(&self) -> Result<HashSet<String>> {
        crate::extent_id_map::referenced_egroups(&self.daruk)
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
                // Referenced and recorded here, so it should be on one of this node's
                // disks. If it is on none of them the bytes are gone locally -- a disk
                // that failed, was unmounted, or never came back after a reboot.
                if !self.store.path_for(&id).exists() {
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
        // The copy a move left behind is surplus bytes, not garbage: it is removed by the
        // same two-scan rule, on the same cadence, so there is one answer in this daemon to
        // "when may bytes be deleted".
        self.reap_strays();
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
        // Every copy on this node, including a surplus one a disk-to-disk move left behind.
        self.store.remove_all(id)?;
        self.daruk.query(&format!(
            "DELETE FROM hydra.dfs_egroups WHERE egroup_id = {}",
            cql_str(id)
        ))?;
        // The access data is about this extent group, so it dies with it -- the whole
        // partition, every node's row, because the group is gone everywhere and not only
        // here. Last, after the row the group is actually described by, and best-effort:
        // nothing reads these rows except the ranking pass, and a leftover one ranks a
        // group that no inventory lists, which the pass ignores. Failing the reclaim over
        // it would be letting a statistic block the reclamation of disk.
        if let Err(e) = self.daruk.query(&format!(
            "DELETE FROM hydra.dfs_egroup_access WHERE egroup_id = {}",
            cql_str(id)
        )) {
            eprintln!("purah: access data for reclaimed extent group {id} could not be deleted: {e}");
        }
        self.access.forget(id);
        Ok(())
    }

    /// Write the in-memory access tally to Hydra.
    ///
    /// The whole reason the data path can afford to record anything: the counts accumulate
    /// in this process and reach Hydra on a timer, so no read ever waits on a metadata
    /// write. What that costs is exactness -- a crash loses everything counted since the
    /// last flush and reopens the window -- and the trade is stated in
    /// [heat.rs](./heat.rs) and in `docs/dfs/metadata.md`: this data decides *where* to put
    /// a copy of bytes that are safe either way, so being wrong about it costs a misplaced
    /// extent group and a later migration, never data.
    ///
    /// Absolute totals, so the write is idempotent and nothing is waiting on it. A flush
    /// that fails is not retried here; the next tick carries the same numbers plus whatever
    /// arrived since.
    pub fn flush_access(&self, now_ms: i64) -> Result<AccessFlushReport> {
        let sample = self.access.sample(now_ms);
        let mut report = AccessFlushReport {
            rows: sample.rows.len(),
            statements: 0,
            since_ms: sample.since_ms,
            updated_at_ms: sample.until_ms,
            dropped: sample.dropped,
        };
        if sample.rows.is_empty() {
            return Ok(report);
        }
        let batches = access_batches(
            &self.node,
            sample.since_ms,
            sample.until_ms,
            &sample.rows,
            ACCESS_BATCH,
        );
        report.statements = batches.len();
        for batch in batches {
            self.daruk.query(&batch)?;
        }
        Ok(report)
    }

    /// Every node's access rows, summed per extent group.
    ///
    /// Summed rather than read per node because an extent group is shared -- a golden
    /// image's groups are read by every clone of it, on whichever node that clone is
    /// attached -- so one node's view of a template is the view of however many clones
    /// happen to live there. The window each node reports is its own, so the merged row
    /// keeps the widest of them: a rate computed over the longest window any observer
    /// measured is the conservative reading, and under-stating heat spills something busy
    /// at worst to a slower disk, while over-stating it fills the fast one with cold data.
    fn access_rows(&self) -> Result<HashMap<String, (Counts, i64, i64)>> {
        let rows = self.daruk.query(
            "SELECT egroup_id, reads, writes, bytes_read, bytes_written, last_read_ms, \
             last_write_ms, since_ms, updated_at_ms FROM hydra.dfs_egroup_access",
        )?;
        let mut out: HashMap<String, (Counts, i64, i64)> = HashMap::new();
        for row in rows {
            let id = match row.get("egroup_id").and_then(Value::as_str) {
                Some(v) => v.to_string(),
                None => continue,
            };
            let num = |name: &str| row.get(name).and_then(Value::as_i64).unwrap_or(0);
            let counts = Counts {
                reads: num("reads").max(0) as u64,
                writes: num("writes").max(0) as u64,
                bytes_read: num("bytes_read").max(0) as u64,
                bytes_written: num("bytes_written").max(0) as u64,
                last_read_ms: num("last_read_ms"),
                last_write_ms: num("last_write_ms"),
            };
            let since = num("since_ms");
            let updated = num("updated_at_ms");
            match out.get_mut(&id) {
                Some((acc, acc_since, acc_updated)) => {
                    acc.merge(&counts);
                    *acc_since = (*acc_since).min(since);
                    *acc_updated = (*acc_updated).max(updated);
                }
                None => {
                    out.insert(id, (counts, since, updated));
                }
            }
        }
        Ok(out)
    }

    /// Rank this node's extent groups by how hot they are.
    ///
    /// The pass the access data exists for, and the one the tiering design in
    /// `docs/dfs/multi_disk.md` is blocked on: placing extent groups by temperature needs
    /// something that says which are hot, and until this there was nothing that did.
    ///
    /// **It only reports.** Nothing here moves, copies or deletes a byte, and that is
    /// deliberate rather than unfinished -- migrating a sealed group between disks is a
    /// copy, a map repoint and a delete, and the first thing that work needs is a ranking
    /// an operator has looked at and agreed with. A curator that started moving data on the
    /// strength of a statistic nobody had read yet is how a tiering pass becomes the reason
    /// a node is busy.
    ///
    /// Scoped to this node's inventory, because placement is a node-local decision
    /// (`multi_disk.md`, option 3): which disk a group sits on is not in Hydra and should
    /// not be, so the node that holds a group is the only one that can act on its
    /// temperature.
    pub fn heat(&self, limit: usize, now_ms: i64) -> Result<HeatReport> {
        let access = self.access_rows()?;
        let inventory = self.my_egroups()?;
        let mut report = HeatReport {
            inventory: inventory.len(),
            tracked_in_memory: self.access.tracked(),
            ..HeatReport::default()
        };
        let mut scored: Vec<(f64, Value)> = Vec::new();
        for (id, state, _created, size) in inventory {
            // Copied out of the map before the match, so the arm that records an unobserved
            // group may take ownership of `id`. Matching on `access.get(&id)` directly keeps
            // the borrow of `id` alive for the whole match and forbids that.
            let found = access.get(&id).copied();
            let (counts, since, updated) = match found {
                Some(v) => v,
                None => {
                    report.unobserved.push(id);
                    continue;
                }
            };
            let score = heat_score(&counts, since, updated, now_ms);
            scored.push((
                score,
                json!({
                    "egroup_id": id,
                    "state": state,
                    "size": size,
                    "reads": counts.reads,
                    "writes": counts.writes,
                    "bytes_read": counts.bytes_read,
                    "bytes_written": counts.bytes_written,
                    "last_access_ms": counts.last_access_ms(),
                    "idle_ms": now_ms.saturating_sub(counts.last_access_ms()).max(0),
                    // The window the totals cover, so the score can be recomputed by hand
                    // from the row. A ranking whose arithmetic cannot be checked is a
                    // ranking an operator has to take on trust, and this one is going to
                    // be used to argue for moving data.
                    "window_ms": updated.saturating_sub(since).max(0),
                    "heat": score,
                }),
            ));
        }
        report.observed = scored.len();
        // Total order, and ties broken by id. A ranking that reordered equal entries
        // between two calls would make "the coldest ten" a different ten each time it was
        // read, which is not something to migrate data on.
        scored.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1["egroup_id"].as_str().cmp(&b.1["egroup_id"].as_str()))
        });
        let limit = limit.max(1);
        report.hot = scored.iter().take(limit).map(|(_, v)| v.clone()).collect();
        report.cold = scored.iter().rev().take(limit).map(|(_, v)| v.clone()).collect();
        report.unobserved_count = report.unobserved.len();
        report.unobserved.sort();
        report.unobserved.truncate(limit);
        report.dropped = self.access.dropped();
        Ok(report)
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
            // Named, not just counted: an operator needs to know which extent groups went
            // with a disk, and a bare number cannot be acted on.
            "missing": self.missing,
            "missing_count": self.missing.len(),
        })
    }
}

impl HeatReport {
    pub fn to_json(&self) -> Value {
        json!({
            "hot": self.hot,
            "cold": self.cold,
            "unobserved": self.unobserved,
            "unobserved_count": self.unobserved_count,
            "inventory": self.inventory,
            "observed": self.observed,
            "tracked_in_memory": self.tracked_in_memory,
            // Non-zero means this node's tally hit its cap and some extent groups were
            // never counted at all, so part of `unobserved` is a measurement gap rather
            // than cold data. Reported beside the ranking because the ranking is wrong in a
            // specific way when it is set, and silently wrong rankings are what get acted
            // on.
            "dropped": self.dropped,
        })
    }
}

impl AccessFlushReport {
    pub fn to_json(&self) -> Value {
        json!({
            "rows": self.rows,
            "statements": self.statements,
            "since_ms": self.since_ms,
            "updated_at_ms": self.updated_at_ms,
            "window_ms": self.updated_at_ms.saturating_sub(self.since_ms).max(0),
            "dropped": self.dropped,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A truncated list of never-touched extent groups still reports how many there were.
    ///
    /// The property, not the formatting: a node where three thousand extent groups have
    /// never been read must not report the same thing as one where three have. The first is
    /// a tally that is not working or a disk full of cold data worth spilling; the second is
    /// a quiet corner. Only the count separates them once the list is cut to a limit.
    #[test]
    fn a_truncated_unobserved_list_still_says_how_many_there_were() {
        let report = HeatReport {
            unobserved: vec!["eg-a".into(), "eg-b".into()],
            unobserved_count: 3_000,
            inventory: 3_010,
            observed: 10,
            ..HeatReport::default()
        };
        let json = report.to_json();
        assert_eq!(json["unobserved"].as_array().map(|a| a.len()), Some(2));
        assert_eq!(json["unobserved_count"].as_u64(), Some(3_000));
    }

    /// A capped tally is reported beside the ranking it makes wrong.
    ///
    /// When `dropped` is non-zero the ranking describes part of the node, and part of
    /// `unobserved` is a measurement gap rather than cold data. A report that carried the
    /// ranking without the caveat would be acted on as though it were complete.
    #[test]
    fn the_ranking_carries_its_own_caveat() {
        let report = HeatReport { dropped: 42, ..HeatReport::default() };
        assert_eq!(report.to_json()["dropped"].as_u64(), Some(42));
    }

    /// The flush reports the window its totals cover, not just the totals.
    ///
    /// Without both ends of the window a total is uninterpretable: a hundred reads means
    /// something different over a minute than over a week, and the two extent groups being
    /// compared may have been known to the daemon for different lengths of time.
    #[test]
    fn a_flush_report_states_the_window_it_counted_over() {
        let report = AccessFlushReport {
            rows: 3,
            statements: 1,
            since_ms: 1_000,
            updated_at_ms: 61_000,
            dropped: 0,
        };
        let json = report.to_json();
        assert_eq!(json["window_ms"].as_i64(), Some(60_000));
        assert_eq!(json["rows"].as_u64(), Some(3));
    }

    /// A clock that went backwards reports a window of zero, never a negative one.
    ///
    /// NTP stepping a node's clock back between the window opening and a flush is the
    /// ordinary way this happens, and a negative window divided into a total is a heat score
    /// with the wrong sign -- which would sort the busiest extent group on the node to the
    /// coldest end of the ranking.
    #[test]
    fn a_backwards_clock_cannot_make_a_negative_window() {
        let report = AccessFlushReport {
            since_ms: 10_000,
            updated_at_ms: 5_000,
            ..AccessFlushReport::default()
        };
        assert_eq!(report.to_json()["window_ms"].as_i64(), Some(0));
    }
}
