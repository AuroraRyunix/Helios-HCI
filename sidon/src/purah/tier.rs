//! Tiering: moving extent groups between a node's disks on the strength of the heat ranking.
//!
//! This is the half of tiering that is safe to build, and the rules it keeps are the ones
//! the design documents set down before there was anything to break them:
//!
//! * **Heat decides where a copy sits, never whether one exists.** The counters are
//!   approximate and a crash loses a window of them (decisions.md, D-22). Nothing here
//!   deletes a group, shortens a replica set or reads a statistic as a reason to stop
//!   holding bytes. A wrong ranking costs a group on the wrong disk and a later move.
//! * **Sealed groups only.** A sealed group is immutable and its footer format is fixed, so
//!   the file is copied byte for byte and the copy hashes the same as the original.
//! * **Move is copy, verify, publish, switch -- and not delete.** The old copy outlives
//!   the move and is removed by the sweep under its two-scan grace. See `extent/placement.rs`
//!   for why, and `StrayLedger` below for the rule.
//! * **Opt-in and bounded.** Nothing here runs on a timer. The ranking this consumes was
//!   shipped as reporting only so that an operator would read it before anything acted on
//!   it, and a curator that moved data the moment it could measure is how a tiering feature
//!   becomes the reason a node is busy. `purah-tier` plans by default and moves only when
//!   told to, a bounded number of groups at a time.
//!
//! # What this cannot demonstrate on the test cluster
//!
//! Every node there has two identical disks, so there is no faster disk to promote onto and
//! no slower one to spill to. The policy is exercised by unit tests over described disks;
//! the *mechanism* (copy, verify, publish, switch, surplus removal) is exercised on real
//! files by `purah-move`, which relocates one named group to a named disk. Neither is a
//! performance result, and none is claimed.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::Purah;
use crate::err::{Error, Result};
use crate::extent::{EgroupStore, Tier};
use crate::heat::heat_score;
use crate::meta::cql_str;

/// The fast tier is considered under pressure below this fraction free, and starts spilling
/// its coldest groups to slower disks.
pub const DEMOTE_BELOW_FREE: f64 = 0.20;
/// Spilling stops once the fast tier is back to this much free. The gap between the two is
/// the hysteresis: a tier that demoted at 20% and stopped at 20% would demote one group
/// every pass for ever.
pub const TARGET_FREE: f64 = 0.30;
/// Promotion will not fill the fast tier past this fraction free. Above the demotion
/// threshold on purpose, so a promotion can never be what the next pass has to undo.
pub const PROMOTE_FLOOR_FREE: f64 = 0.25;
/// No move leaves its destination disk with less than this fraction free.
pub const DESTINATION_RESERVE: f64 = 0.10;

pub const DEFAULT_MAX_MOVES: usize = 8;
pub const DEFAULT_MAX_BYTES: u64 = 256 << 20;

#[derive(Clone, Debug)]
pub struct PlanDisk {
    pub slot: usize,
    pub uid: String,
    pub label: String,
    pub tier: Tier,
    pub total: u64,
    pub avail: u64,
}

#[derive(Clone, Debug)]
pub struct PlanGroup {
    pub id: String,
    pub slot: usize,
    pub size: u64,
    /// `None` is a group nothing has measured, which is not the same as a cold one.
    pub heat: Option<f64>,
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_moves: usize,
    pub max_bytes: u64,
    /// The access tally hit its cap, so a group with no row may simply have been missed.
    pub measurement_incomplete: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Hot data onto a faster disk.
    Promote,
    /// Cold data off a fast disk that is running short of room.
    Demote,
}

#[derive(Clone, Debug)]
pub struct Planned {
    pub id: String,
    pub from: usize,
    pub to: usize,
    pub size: u64,
    pub direction: Direction,
    pub heat: Option<f64>,
}

#[derive(Debug, Default)]
pub struct Plan {
    pub moves: Vec<Planned>,
    /// Why the plan is what it is, including why it is empty. An empty plan with no reason
    /// is indistinguishable from a pass that did not run.
    pub notes: Vec<String>,
}

fn free_fraction(avail: u64, total: u64) -> f64 {
    if total == 0 { 0.0 } else { avail as f64 / total as f64 }
}

/// What to move, from a description of the disks and the groups on them. Pure: no files, no
/// Hydra, which is what lets the policy be tested on disks that do not exist.
pub fn plan(disks: &[PlanDisk], groups: &[PlanGroup], limits: Limits) -> Plan {
    let mut out = Plan::default();

    // Only disks whose class is actually known take part. A disk of unknown class might be
    // the fastest on the node, and treating it as the slowest would spill hot data onto it
    // on the strength of nothing.
    let known: Vec<&PlanDisk> = disks.iter().filter(|d| d.tier.is_known() && d.total > 0).collect();
    let unknown = disks.len() - known.len();
    if unknown > 0 {
        out.notes.push(format!(
            "{unknown} disk(s) have no known class and take no part; set it with a disk.tier file \
             if the kernel cannot say"
        ));
    }
    let mut tiers: Vec<Tier> = known.iter().map(|d| d.tier).collect();
    tiers.sort();
    tiers.dedup();
    if tiers.len() < 2 {
        out.notes.push(
            "every disk with a known class is the same class, so there is no faster or slower \
             disk for a group to move to; tiering has nothing to decide"
                .to_string(),
        );
        return out;
    }
    let fast_tier = *tiers.last().expect("two tiers");
    let is_fast = |slot: usize| known.iter().any(|d| d.slot == slot && d.tier == fast_tier);
    let fast: Vec<&PlanDisk> = known.iter().copied().filter(|d| d.tier == fast_tier).collect();
    let slow: Vec<&PlanDisk> = known.iter().copied().filter(|d| d.tier != fast_tier).collect();
    let fast_total: u64 = fast.iter().map(|d| d.total).sum();

    let mut avail: HashMap<usize, u64> = known.iter().map(|d| (d.slot, d.avail)).collect();
    let total_of: HashMap<usize, u64> = known.iter().map(|d| (d.slot, d.total)).collect();
    let fast_free = |avail: &HashMap<usize, u64>| -> f64 {
        free_fraction(fast.iter().map(|d| avail[&d.slot]).sum(), fast_total)
    };
    // Room on `dest` for a group of `size` that still leaves its reserve.
    let fits = |avail: &HashMap<usize, u64>, dest: usize, size: u64| -> bool {
        let left = avail[&dest].saturating_sub(size);
        avail[&dest] >= size && free_fraction(left, total_of[&dest]) >= DESTINATION_RESERVE
    };
    let roomiest = |avail: &HashMap<usize, u64>, among: &[&PlanDisk], size: u64| -> Option<usize> {
        among
            .iter()
            .filter(|d| fits(avail, d.slot, size))
            .max_by_key(|d| (avail[&d.slot], std::cmp::Reverse(d.slot)))
            .map(|d| d.slot)
    };

    let mut moved_bytes = 0u64;
    let within_budget = |moves: usize, bytes: u64, next: u64| -> bool {
        moves < limits.max_moves && bytes.saturating_add(next) <= limits.max_bytes
    };

    // Demotion first. A fast tier that is nearly full is a problem now; a hot group on a
    // slow disk is only a missed opportunity.
    if fast_free(&avail) < DEMOTE_BELOW_FREE {
        let mut cold: Vec<&PlanGroup> = groups.iter().filter(|g| is_fast(g.slot)).collect();
        // Measured-and-cold before unmeasured. Both may go, but a group that was counted and
        // found quiet is better evidence than one nobody counted -- and when the tally was
        // capped the unmeasured ones are not offered at all, since part of them is a gap in
        // the measurement and not a cold group.
        cold.retain(|g| g.heat.is_some() || !limits.measurement_incomplete);
        cold.sort_by(|a, b| {
            let rank = |g: &PlanGroup| g.heat.map(|h| (0u8, h)).unwrap_or((1u8, 0.0));
            let (ra, ha) = rank(a);
            let (rb, hb) = rank(b);
            ra.cmp(&rb)
                .then(ha.partial_cmp(&hb).unwrap_or(std::cmp::Ordering::Equal))
                .then_with(|| a.id.cmp(&b.id))
        });
        for g in cold {
            if fast_free(&avail) >= TARGET_FREE {
                break;
            }
            if !within_budget(out.moves.len(), moved_bytes, g.size) {
                out.notes.push("the per-pass limit was reached before the fast tier recovered".to_string());
                break;
            }
            match roomiest(&avail, &slow, g.size) {
                Some(dest) => {
                    *avail.get_mut(&g.slot).expect("known slot") += g.size;
                    *avail.get_mut(&dest).expect("known slot") -= g.size;
                    moved_bytes += g.size;
                    out.moves.push(Planned {
                        id: g.id.clone(),
                        from: g.slot,
                        to: dest,
                        size: g.size,
                        direction: Direction::Demote,
                        heat: g.heat,
                    });
                }
                None => {
                    out.notes.push("no slower disk has room to take a group".to_string());
                    break;
                }
            }
        }
        if !out.moves.is_empty() {
            // Not both in one pass: a promotion would spend the room a demotion just made.
            out.notes.push(format!(
                "the fast tier is below {:.0}% free, so cold groups are spilled and nothing is promoted",
                DEMOTE_BELOW_FREE * 100.0
            ));
            return out;
        }
    }

    // Promotion: the hottest measured groups that are on a slow disk. A group with no heat
    // is never promoted -- there is no evidence it would be read.
    let mut hot: Vec<&PlanGroup> = groups
        .iter()
        .filter(|g| !is_fast(g.slot) && known.iter().any(|d| d.slot == g.slot))
        .filter(|g| g.heat.map(|h| h > 0.0).unwrap_or(false))
        .collect();
    hot.sort_by(|a, b| {
        b.heat
            .partial_cmp(&a.heat)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    });
    for g in hot {
        if !within_budget(out.moves.len(), moved_bytes, g.size) {
            out.notes.push("the per-pass limit was reached".to_string());
            break;
        }
        let after = free_fraction(
            fast.iter().map(|d| avail[&d.slot]).sum::<u64>().saturating_sub(g.size),
            fast_total,
        );
        if after < PROMOTE_FLOOR_FREE {
            out.notes.push(format!(
                "promotion stops where the fast tier would fall below {:.0}% free",
                PROMOTE_FLOOR_FREE * 100.0
            ));
            break;
        }
        if let Some(dest) = roomiest(&avail, &fast, g.size) {
            *avail.get_mut(&g.slot).expect("known slot") += g.size;
            *avail.get_mut(&dest).expect("known slot") -= g.size;
            moved_bytes += g.size;
            out.moves.push(Planned {
                id: g.id.clone(),
                from: g.slot,
                to: dest,
                size: g.size,
                direction: Direction::Promote,
                heat: g.heat,
            });
        } else {
            break;
        }
    }
    if out.moves.is_empty() {
        out.notes.push("every group is already where the ranking would put it".to_string());
    }
    out
}

// --- The surplus copy and its grace -----------------------------------------------------

/// Which surplus copies have been seen, and since when.
///
/// The same rule the mark-sweep applies to an unreferenced group, applied to a copy a move
/// left behind: it is removed only after it has been observed surplus on two passes with at
/// least the grace period between them. Not because a surplus copy is dangerous to keep --
/// it is just disk -- but because removing bytes is the one irreversible step, and this
/// daemon should have one rule for when that is allowed, not a second invented for tiering.
/// A restart empties the ledger, so a restart restarts the grace: the safe direction.
#[derive(Default)]
pub struct StrayLedger {
    first_seen: HashMap<(String, String), Instant>,
}

#[derive(Debug, Default)]
pub struct ReapReport {
    /// Surplus copies seen this pass that have not yet earned removal.
    pub awaiting_grace: usize,
    pub removed: Vec<String>,
    pub bytes_freed: u64,
    /// Copies that were not removed and why. Anything here is worth reading: it is either
    /// two files that disagree or a filesystem that would not let one go.
    pub refused: Vec<(String, String)>,
    pub temporaries_removed: usize,
}

impl StrayLedger {
    pub fn step(&mut self, store: &EgroupStore, now: Instant, grace: Duration) -> ReapReport {
        let mut report = ReapReport::default();
        let mut next: HashMap<(String, String), Instant> = HashMap::new();
        for stray in store.strays() {
            let uid = store.disks()[stray.slot].uid.clone();
            let key = (stray.id.clone(), uid);
            let first = match self.first_seen.get(&key) {
                Some(t) => *t,
                None => {
                    // First sighting: the first of the two observations, and no further.
                    next.insert(key, now);
                    report.awaiting_grace += 1;
                    continue;
                }
            };
            if now.duration_since(first) < grace {
                next.insert(key, first);
                report.awaiting_grace += 1;
                continue;
            }
            match store.remove_stray(&stray) {
                Ok(bytes) => {
                    report.removed.push(stray.id.clone());
                    report.bytes_freed += bytes;
                }
                Err(e) => {
                    eprintln!("purah: surplus copy of extent group {} not removed: {e}", stray.id);
                    report.refused.push((stray.id.clone(), e.to_string()));
                    next.insert(key, first);
                }
            }
        }
        self.first_seen = next;

        // A copy that was being written when the daemon died. Never valid, never indexed,
        // and only safe to judge by age because Purah is the only thing that starts moves.
        for (_, path) in store.stale_temporaries(grace) {
            if std::fs::remove_file(&path).is_ok() {
                report.temporaries_removed += 1;
            }
        }
        report
    }
}

// --- The pass ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct TierOptions {
    pub apply: bool,
    pub max_moves: usize,
    pub max_bytes: u64,
}

impl Default for TierOptions {
    fn default() -> Self {
        TierOptions { apply: false, max_moves: DEFAULT_MAX_MOVES, max_bytes: DEFAULT_MAX_BYTES }
    }
}

fn move_json(store: &EgroupStore, m: &Planned) -> Value {
    json!({
        "egroup_id": m.id,
        "direction": match m.direction { Direction::Promote => "promote", Direction::Demote => "demote" },
        "from": store.disks()[m.from].uid,
        "from_label": store.disks()[m.from].id,
        "to": store.disks()[m.to].uid,
        "to_label": store.disks()[m.to].id,
        "bytes": m.size,
        "heat": m.heat,
    })
}

impl Purah {
    fn plan_disks(&self) -> Vec<PlanDisk> {
        self.store
            .disks()
            .iter()
            .enumerate()
            .map(|(slot, d)| {
                let (total, avail) = crate::extent::disk_space(&d.root).unwrap_or((0, 0));
                PlanDisk { slot, uid: d.uid.clone(), label: d.id.clone(), tier: d.tier, total, avail }
            })
            .collect()
    }

    /// Hydra's record of a group, for the checks that must hold immediately before bytes are
    /// copied: it is sealed, and what hash it was sealed with.
    fn sealed_hash(&self, id: &str) -> Result<String> {
        let rows = self.daruk.query(&format!(
            "SELECT state, seal_hash FROM hydra.dfs_egroups WHERE egroup_id = {}",
            cql_str(id)
        ))?;
        let row = rows
            .first()
            .ok_or_else(|| Error::refused(format!("extent group {id} is not recorded in Hydra")))?;
        let state = row.get("state").and_then(Value::as_str).unwrap_or("unknown");
        if state != "sealed" {
            return Err(Error::refused(format!(
                "extent group {id} is {state}; only a sealed group is immutable and may be moved"
            )));
        }
        Ok(row.get("seal_hash").and_then(Value::as_str).unwrap_or("").to_string())
    }

    fn move_checked(&self, id: &str, to: usize) -> Result<Value> {
        let hash = self.sealed_hash(id)?;
        let moved = self.store.move_group(id, to, Some(&hash))?;
        Ok(json!({
            "egroup_id": moved.id,
            "from": self.store.disks()[moved.from].uid,
            "to": self.store.disks()[moved.to].uid,
            "bytes": moved.bytes,
            "hash": moved.hash,
            // The old copy stays until the sweep has seen it surplus twice.
            "old_copy_remains_until_swept": true,
        }))
    }

    /// Plan, and with `apply`, carry out, the moves the heat ranking argues for.
    pub fn tier(&mut self, opts: &TierOptions, now_ms: i64) -> Result<Value> {
        let access = self.access_rows()?;
        let inventory = self.my_egroups()?;
        let mut groups = Vec::new();
        let mut skipped_unsealed = 0usize;
        let mut absent = 0usize;
        for (id, state, _created, size) in inventory {
            if state != "sealed" {
                skipped_unsealed += 1;
                continue;
            }
            let slot = match self.store.locate(&id) {
                Some(s) => s,
                None => {
                    absent += 1;
                    continue;
                }
            };
            let heat = access
                .get(&id)
                .map(|(counts, since, updated)| heat_score(counts, *since, *updated, now_ms));
            groups.push(PlanGroup { id, slot, size: size.max(0) as u64, heat });
        }
        let dropped = self.access.dropped();
        let disks = self.plan_disks();
        let limits = Limits {
            max_moves: opts.max_moves.max(1),
            max_bytes: opts.max_bytes,
            measurement_incomplete: dropped > 0,
        };
        let planned = plan(&disks, &groups, limits);

        let mut executed = Vec::new();
        let mut failed = Vec::new();
        if opts.apply {
            for m in &planned.moves {
                match self.move_checked(&m.id, m.to) {
                    Ok(v) => executed.push(v),
                    Err(e) => {
                        eprintln!("purah: tier move of {} failed: {e}", m.id);
                        failed.push(json!({"egroup_id": m.id, "error": e.to_string()}));
                    }
                }
            }
        }
        Ok(json!({
            "applied": opts.apply,
            "disks": disks.iter().map(|d| json!({
                "uid": d.uid, "label": d.label, "tier": d.tier.name(),
                "total_bytes": d.total, "available_bytes": d.avail,
            })).collect::<Vec<_>>(),
            "considered": groups.len(),
            "skipped_unsealed": skipped_unsealed,
            "absent_locally": absent,
            "planned": planned.moves.iter().map(|m| move_json(&self.store, m)).collect::<Vec<_>>(),
            "executed": executed,
            "failed": failed,
            "notes": planned.notes,
            // Said beside the plan because the plan is wrong in a specific way when set.
            "dropped": dropped,
            "limits": {"max_moves": limits.max_moves, "max_bytes": limits.max_bytes},
        }))
    }

    /// Move one named sealed group to one named disk.
    ///
    /// The mechanism with no policy in front of it: what an operator reaches for to rebalance
    /// by hand, and what exercises copy, verify, publish and switch on a node whose disks are
    /// identical and so give the policy nothing to decide.
    pub fn move_one(&mut self, id: &str, disk: &str) -> Result<Value> {
        let to = self
            .store
            .resolve_disk(disk)
            .ok_or_else(|| Error::refused(format!("this node has no disk {disk:?}; see `purah-placement`")))?;
        self.move_checked(id, to)
    }

    pub fn placement(&self, limit: usize) -> Value {
        self.store.placement_report(limit)
    }

    /// Remove surplus copies and abandoned temporaries that have waited out the grace.
    pub(super) fn reap_strays(&mut self) -> ReapReport {
        let report = self.strays.step(&self.store, Instant::now(), self.grace);
        if !report.removed.is_empty() {
            println!(
                "purah: removed {} surplus extent-group cop{} left by a move, {} bytes",
                report.removed.len(),
                if report.removed.len() == 1 { "y" } else { "ies" },
                report.bytes_freed
            );
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disk(slot: usize, tier: Tier, total: u64, avail: u64) -> PlanDisk {
        PlanDisk { slot, uid: format!("uid-{slot}"), label: format!("d{slot}"), tier, total, avail }
    }

    fn group(id: &str, slot: usize, size: u64, heat: Option<f64>) -> PlanGroup {
        PlanGroup { id: id.to_string(), slot, size, heat }
    }

    fn limits() -> Limits {
        Limits { max_moves: 100, max_bytes: u64::MAX, measurement_incomplete: false }
    }

    const GIB: u64 = 1 << 30;

    /// Identical disks give the policy nothing to decide.
    ///
    /// This is the test cluster: two disks of the same class. A tiering pass that moved
    /// anything there would be shuffling data between equals on the strength of a ranking,
    /// which is churn with a risk attached and no benefit to claim.
    #[test]
    fn disks_of_one_class_are_never_tiered() {
        let disks = [disk(0, Tier::Ssd, 100 * GIB, 5 * GIB), disk(1, Tier::Ssd, 100 * GIB, 90 * GIB)];
        let groups = [group("eg-hot", 0, GIB, Some(900.0)), group("eg-cold", 0, GIB, Some(0.0))];
        let p = plan(&disks, &groups, limits());
        assert!(p.moves.is_empty(), "groups were moved between disks of the same class");
        assert!(!p.notes.is_empty(), "an empty plan gave no reason");
    }

    /// A disk whose class is not known is left out, not assumed slow.
    #[test]
    fn a_disk_of_unknown_class_takes_no_part() {
        let disks = [disk(0, Tier::Ssd, 100 * GIB, 5 * GIB), disk(1, Tier::Unknown, 100 * GIB, 90 * GIB)];
        let groups = [group("eg-a", 0, GIB, Some(0.0))];
        let p = plan(&disks, &groups, limits());
        assert!(p.moves.is_empty(), "data was spilled onto a disk nobody knows the speed of");
    }

    /// Hot groups on the slow disk move to the fast one, hottest first, and groups nothing
    /// has measured are never promoted.
    #[test]
    fn the_hottest_measured_groups_are_promoted_and_unmeasured_ones_are_not() {
        let disks = [disk(0, Tier::Hdd, 100 * GIB, 50 * GIB), disk(1, Tier::Ssd, 100 * GIB, 80 * GIB)];
        let groups = [
            group("eg-warm", 0, GIB, Some(10.0)),
            group("eg-hot", 0, GIB, Some(500.0)),
            group("eg-never", 0, GIB, None),
            group("eg-zero", 0, GIB, Some(0.0)),
        ];
        let p = plan(&disks, &groups, limits());
        let ids: Vec<&str> = p.moves.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["eg-hot", "eg-warm"]);
        assert!(p.moves.iter().all(|m| m.direction == Direction::Promote && m.to == 1));
    }

    /// A promotion must not be the thing the next pass undoes.
    #[test]
    fn promotion_stops_before_it_would_force_a_demotion() {
        // Fast tier is 100 GiB with 26 free: one GiB is fine (25 left, exactly the floor),
        // a second would leave 24 -- under the floor -- and the next pass would spill it.
        let disks = [disk(0, Tier::Hdd, 100 * GIB, 50 * GIB), disk(1, Tier::Ssd, 100 * GIB, 26 * GIB)];
        let groups = [group("eg-1", 0, GIB, Some(9.0)), group("eg-2", 0, GIB, Some(8.0))];
        let p = plan(&disks, &groups, limits());
        assert_eq!(p.moves.len(), 1, "promotion filled the fast tier past the point of demoting");
    }

    /// A fast disk that is nearly full spills its coldest groups, coldest first, and
    /// promotes nothing in the same pass.
    #[test]
    fn a_fast_tier_short_of_room_spills_its_coldest_groups() {
        let disks = [disk(0, Tier::Ssd, 100 * GIB, 10 * GIB), disk(1, Tier::Hdd, 100 * GIB, 80 * GIB)];
        let groups = [
            group("eg-busy", 0, GIB, Some(300.0)),
            group("eg-quiet", 0, GIB, Some(0.5)),
            group("eg-idle", 0, GIB, Some(0.0)),
            group("eg-hot-on-slow", 1, GIB, Some(900.0)),
        ];
        let p = plan(&disks, &groups, limits());
        assert!(p.moves.iter().all(|m| m.direction == Direction::Demote), "promoted while spilling");
        assert_eq!(p.moves[0].id, "eg-idle");
        assert_eq!(p.moves[1].id, "eg-quiet");
        assert!(p.moves.iter().all(|m| m.id != "eg-hot-on-slow"));
    }

    /// Spilling stops when the fast tier has recovered, not when the groups run out.
    #[test]
    fn spilling_stops_at_the_target_and_not_before_or_after() {
        // 100 GiB fast tier with 19 free; it needs 11 more to reach 30%.
        let disks = [disk(0, Tier::Ssd, 100 * GIB, 19 * GIB), disk(1, Tier::Hdd, 1000 * GIB, 900 * GIB)];
        let groups: Vec<PlanGroup> =
            (0..40).map(|i| group(&format!("eg-{i:02}"), 0, GIB, Some(i as f64))).collect();
        let p = plan(&disks, &groups, limits());
        assert_eq!(p.moves.len(), 11);
    }

    /// When the tally was capped, groups with no row are not offered as cold.
    ///
    /// Part of them is a gap in the measurement. Spilling a busy group because the counter
    /// ran out of room to count it is the failure D-22's caveat is about.
    #[test]
    fn unmeasured_groups_are_not_spilled_when_the_tally_was_capped() {
        let disks = [disk(0, Tier::Ssd, 100 * GIB, 10 * GIB), disk(1, Tier::Hdd, 100 * GIB, 80 * GIB)];
        let groups = [group("eg-unknown", 0, GIB, None), group("eg-known-cold", 0, GIB, Some(0.0))];
        let mut l = limits();
        l.measurement_incomplete = true;
        let p = plan(&disks, &groups, l);
        let ids: Vec<&str> = p.moves.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["eg-known-cold"]);
    }

    /// Measured-cold goes before unmeasured, so a counted group is preferred evidence.
    #[test]
    fn measured_cold_groups_are_spilled_before_unmeasured_ones() {
        let disks = [disk(0, Tier::Ssd, 100 * GIB, 19 * GIB), disk(1, Tier::Hdd, 1000 * GIB, 900 * GIB)];
        let mut groups = vec![group("eg-aaa-unmeasured", 0, GIB, None)];
        groups.extend((0..11).map(|i| group(&format!("eg-m{i:02}"), 0, GIB, Some(1.0 + i as f64))));
        let p = plan(&disks, &groups, limits());
        assert!(p.moves.iter().all(|m| m.id != "eg-aaa-unmeasured"),
                "an unmeasured group went ahead of measured cold ones");
    }

    /// No move may leave its destination with less than the reserve.
    #[test]
    fn a_move_never_fills_its_destination() {
        let disks = [disk(0, Tier::Ssd, 100 * GIB, 5 * GIB), disk(1, Tier::Hdd, 100 * GIB, 11 * GIB)];
        let groups: Vec<PlanGroup> =
            (0..10).map(|i| group(&format!("eg-{i}"), 0, GIB, Some(i as f64))).collect();
        let p = plan(&disks, &groups, limits());
        // 11 GiB free on 100 with a 10% reserve leaves room for exactly one GiB.
        assert_eq!(p.moves.len(), 1, "a destination was filled past its reserve");
    }

    /// Per-pass limits bound the work.
    #[test]
    fn the_per_pass_limits_are_honoured() {
        let disks = [disk(0, Tier::Ssd, 100 * GIB, 1 * GIB), disk(1, Tier::Hdd, 1000 * GIB, 900 * GIB)];
        let groups: Vec<PlanGroup> =
            (0..40).map(|i| group(&format!("eg-{i:02}"), 0, GIB, Some(i as f64))).collect();
        let mut l = limits();
        l.max_moves = 3;
        assert_eq!(plan(&disks, &groups, l).moves.len(), 3);
        let mut l = limits();
        l.max_bytes = 2 * GIB;
        assert_eq!(plan(&disks, &groups, l).moves.len(), 2);
    }

    /// The same inputs give the same plan.
    ///
    /// Moves are made on the strength of this, so a plan that reordered equal entries
    /// between two calls would move a different set each time it was asked.
    #[test]
    fn equal_heat_is_ordered_by_id() {
        let disks = [disk(0, Tier::Hdd, 100 * GIB, 50 * GIB), disk(1, Tier::Ssd, 100 * GIB, 80 * GIB)];
        let groups = [group("eg-b", 0, GIB, Some(4.0)), group("eg-a", 0, GIB, Some(4.0))];
        let p = plan(&disks, &groups, limits());
        assert_eq!(p.moves[0].id, "eg-a");
    }

    // --- the surplus copy and its grace ---

    use crate::extent::{vdisk_hash, Disk};

    fn tmpdir(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("sidon-tier-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn store_with_a_moved_group(name: &str) -> (std::path::PathBuf, EgroupStore) {
        let dir = tmpdir(name);
        let disks: Vec<Disk> = ["d0", "d1"]
            .iter()
            .map(|l| {
                let root = dir.join("disks").join(l).join("egroups");
                std::fs::create_dir_all(&root).unwrap();
                Disk::identified(l.to_string(), root)
            })
            .collect();
        let store = EgroupStore::open(disks, 1 << 20).unwrap();
        let mut eg = store.create("eg-x").unwrap();
        store.append_framed(&mut eg, &vec![1u8; 4096], vdisk_hash("vd"), 0, false).unwrap();
        store.sync(&mut eg).unwrap();
        let from = store.locate("eg-x").unwrap();
        store.move_group("eg-x", 1 - from, None).unwrap();
        (dir, store)
    }

    /// A surplus copy is not removed the first time it is seen, nor before the grace.
    #[test]
    fn a_surplus_copy_survives_its_first_sighting_and_the_grace() {
        let (dir, store) = store_with_a_moved_group("grace");
        let mut ledger = StrayLedger::default();
        let grace = Duration::from_secs(600);
        let t0 = Instant::now();

        let r = ledger.step(&store, t0, grace);
        assert!(r.removed.is_empty(), "removed on first sight, which is the one observation");
        assert_eq!(store.copies("eg-x").len(), 2);

        // A second sighting inside the grace is still too early.
        let r = ledger.step(&store, t0 + Duration::from_secs(300), grace);
        assert!(r.removed.is_empty(), "removed before the grace had elapsed");
        assert_eq!(store.copies("eg-x").len(), 2);

        // Seen twice with the grace between: now it goes, and the group still exists.
        let r = ledger.step(&store, t0 + Duration::from_secs(601), grace);
        assert_eq!(r.removed, vec!["eg-x".to_string()]);
        assert_eq!(store.copies("eg-x").len(), 1, "the sweep removed the surplus and the group with it");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A fresh ledger -- a restarted daemon -- starts the grace over.
    #[test]
    fn a_restart_restarts_the_grace() {
        let (dir, store) = store_with_a_moved_group("restart-grace");
        let grace = Duration::from_secs(600);
        let t0 = Instant::now();
        let mut first = StrayLedger::default();
        first.step(&store, t0, grace);
        let mut after_restart = StrayLedger::default();
        let r = after_restart.step(&store, t0 + Duration::from_secs(5000), grace);
        assert!(r.removed.is_empty(), "a restart skipped the observation that had not happened yet");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A copy that disagrees with the one kept is reported and left alone, every pass.
    #[test]
    fn a_surplus_copy_that_differs_is_reported_not_removed() {
        let (dir, store) = store_with_a_moved_group("differs");
        let stray = store.strays().pop().unwrap();
        std::fs::write(store.disks()[stray.slot].root.join("eg-x.eg"), b"drifted").unwrap();
        let mut ledger = StrayLedger::default();
        let grace = Duration::from_secs(1);
        let t0 = Instant::now();
        ledger.step(&store, t0, grace);
        let r = ledger.step(&store, t0 + Duration::from_secs(10), grace);
        assert!(r.removed.is_empty());
        assert_eq!(r.refused.len(), 1);
        assert_eq!(store.copies("eg-x").len(), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A temporary left by a crashed copy is cleaned up once it is old, and not before.
    #[test]
    fn an_abandoned_temporary_is_removed_when_old() {
        let (dir, store) = store_with_a_moved_group("temporary");
        let temp = store.disks()[0].root.join("eg-dead.eg.moving");
        std::fs::write(&temp, b"half a group").unwrap();
        let mut ledger = StrayLedger::default();
        // Younger than the grace: could be a move in flight.
        let r = ledger.step(&store, Instant::now(), Duration::from_secs(3600));
        assert_eq!(r.temporaries_removed, 0);
        assert!(temp.exists());
        // Zero grace treats everything as old.
        let r = ledger.step(&store, Instant::now(), Duration::from_secs(0));
        assert_eq!(r.temporaries_removed, 1);
        assert!(!temp.exists());
        std::fs::remove_dir_all(&dir).ok();
    }
}
