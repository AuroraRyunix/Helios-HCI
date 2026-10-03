//! The middle map level (D-23), stage 1: Purah's mark phase learns to traverse it.
//!
//! Helios's map has had two levels, `(vdisk, extent_index) -> extent group`. D-23 adds a
//! third, `(vdisk, extent_index) -> extent -> extent group`, where the middle row lives in
//! `hydra.dfs_extent_id_map` and several vdisks may name the same extent. A block-map row
//! says which level it uses by which column it fills: `egroup_id` is the two-level path
//! every vdisk has today, `extent_id` is the three-level one.
//!
//! [`referenced_egroups`] is Purah's mark phase. It runs on a timer whether or not anyone
//! has opted in to anything, and an extent group it does not mark is an extent group the
//! sweep will delete. That is why it learned the extent level *first*: with no row naming an
//! extent it is the old scan, and the first row that does is already understood. Reading a
//! vdisk through the middle level is `extent_resolve.rs`, which may not be used until this
//! has been deployed and has run a full sweep cycle.
//!
//! It reads through [`Rows`], not through [`Daruk`] directly, which is what lets the
//! property test drive the real statements and the real decisions against a model of the
//! database instead of against a copy of their logic.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::err::{Error, Result};
use crate::meta::Daruk;

/// The migration that creates `hydra.dfs_extent_id_map`. Kept equal to the id in
/// `helios_schema.py` by a test on the Python side.
pub const TABLE_MIGRATION: &str = "0020-dfs-extent-id-map";
/// The migration that adds `dfs_block_map.extent_id`.
pub const COLUMN_MIGRATION: &str = "0021-dfs-block-map-extent-id";

/// What the mark phase has always read.
pub const LEGACY_BLOCK_MAP_SCAN: &str = "SELECT egroup_id FROM hydra.dfs_block_map";
/// What it reads once the column exists.
pub const BLOCK_MAP_SCAN: &str = "SELECT egroup_id, extent_id FROM hydra.dfs_block_map";
pub const EXTENT_MAP_SCAN: &str = "SELECT extent_id, egroup_id FROM hydra.dfs_extent_id_map";
pub const LEDGER_SCAN: &str = "SELECT id FROM hydra.schema_migrations";

/// A thing that answers CQL with rows. [`Daruk`] is the only production implementation.
pub trait Rows {
    fn rows(&self, cql: &str) -> Result<Vec<Value>>;
}

impl Rows for Daruk {
    fn rows(&self, cql: &str) -> Result<Vec<Value>> {
        self.query(cql)
    }
}

fn text(row: &Value, name: &str) -> Option<String> {
    row.get(name).and_then(Value::as_str).map(str::to_string)
}

/// Which levels the schema has, read from the migration ledger.
///
/// The ledger and not the error text of a failed `SELECT`. The alternative is to run the
/// extended statement and fall back to the old one when it complains the column is
/// missing, which makes the reclamation decision depend on how ScyllaDB words a
/// complaint and on every other failure not being mistaken for that one. A fallback that
/// is wrong in the permissive direction is a sweep that does not see extent references,
/// and that is the one mistake this module exists to rule out. Reading the ledger has no
/// such direction: either the migration is recorded, in which case the column exists and
/// every failure from here on aborts the sweep, or it is not, in which case no writer has
/// had a table to put an extent id in.
fn applied_levels<D: Rows>(db: &D) -> Result<(bool, bool)> {
    let ids: HashSet<String> = db
        .rows(LEDGER_SCAN)?
        .iter()
        .filter_map(|r| text(r, "id"))
        .collect();
    Ok((ids.contains(COLUMN_MIGRATION), ids.contains(TABLE_MIGRATION)))
}

/// The groups a set of block-map rows keep alive, through either level.
///
/// `rows` are `(egroup_id, extent_id)` straight off the map. `extents` maps an extent id to
/// the group its row names, `None` for a row that names none. Returns the marked groups, or
/// the extent ids that could not be followed.
///
/// A row that fills *both* columns marks both. Nothing legitimate writes one, but the
/// drain's insert lists only the columns it sets, so a row repointed by a drain after it was
/// extent-named still carries the old `extent_id`; marking the union keeps a group alive
/// that perhaps need not be, which costs space, where marking one side would cost data.
///
/// An extent that cannot be followed is an error and never a skip. Skipping would leave
/// its group unmarked, and an unmarked group is a group the sweep may delete.
pub fn mark(
    rows: &[(Option<String>, Option<String>)],
    extents: &HashMap<String, Option<String>>,
) -> std::result::Result<HashSet<String>, Vec<String>> {
    let mut marked = HashSet::new();
    let mut dangling: Vec<String> = Vec::new();
    for (egroup, extent) in rows {
        if let Some(g) = egroup {
            marked.insert(g.clone());
        }
        if let Some(e) = extent {
            match extents.get(e) {
                Some(Some(g)) => {
                    marked.insert(g.clone());
                }
                _ => dangling.push(e.clone()),
            }
        }
    }
    if dangling.is_empty() {
        Ok(marked)
    } else {
        dangling.sort();
        dangling.dedup();
        Err(dangling)
    }
}

/// Every extent group the map points at, across all vdisks, through both levels.
///
/// Read *before* the egroup inventory, for the reason Purah documents: a group created
/// between the two reads then shows as unreferenced-and-young instead of being missed. The
/// extent map is read after the block map and not before, for the same reason one level
/// down: an extent a drain adds between the two scans is named by no row this scan saw, so
/// nothing is lost by its absence, whereas the other order would find a block row naming an
/// extent the earlier scan never saw and abort a sweep over a race.
pub fn referenced_egroups<D: Rows>(db: &D) -> Result<HashSet<String>> {
    let (has_column, has_table) = applied_levels(db)?;
    let scan = if has_column { BLOCK_MAP_SCAN } else { LEGACY_BLOCK_MAP_SCAN };
    let block_rows: Vec<(Option<String>, Option<String>)> = db
        .rows(scan)?
        .iter()
        .map(|r| (text(r, "egroup_id"), text(r, "extent_id")))
        .collect();

    let mut wanted: HashSet<&str> = HashSet::new();
    for (_, extent) in &block_rows {
        if let Some(e) = extent {
            wanted.insert(e.as_str());
        }
    }

    let mut extents: HashMap<String, Option<String>> = HashMap::new();
    if !wanted.is_empty() {
        if !has_table {
            return Err(Error::meta(format!(
                "the block map names extents but {TABLE_MIGRATION} is not recorded as applied; \
                 refusing to mark from a map whose middle level cannot be read"
            )));
        }
        for r in db.rows(EXTENT_MAP_SCAN)? {
            if let Some(id) = text(&r, "extent_id") {
                extents.insert(id, text(&r, "egroup_id"));
            }
        }
    }

    mark(&block_rows, &extents).map_err(|d| {
        let sample: Vec<&str> = d.iter().take(5).map(String::as_str).collect();
        Error::meta(format!(
            "{} extent id(s) named by the block map have no usable row in the extent id map \
             (first: {sample:?}); refusing to mark, because every group behind them would \
             look unreferenced",
            d.len()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cell::Cell;

    /// A small deterministic generator. The crate has no `rand`, and a property test that
    /// cannot be replayed from its seed is a test whose failure nobody can investigate.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }

    /// The database as the mark phase sees it: a ledger, a block map and an extent map.
    struct Model {
        ledger: Vec<&'static str>,
        /// (vdisk, index, egroup_id, extent_id)
        block: Vec<(String, u64, Option<String>, Option<String>)>,
        /// extent_id -> (egroup_id, offset, length)
        extents: Vec<(String, Option<String>, u32, u32)>,
        fail_extent_scan: Cell<bool>,
        statements: std::cell::RefCell<Vec<String>>,
    }

    impl Rows for Model {
        fn rows(&self, cql: &str) -> Result<Vec<Value>> {
            self.statements.borrow_mut().push(cql.to_string());
            let has_col = self.ledger.contains(&COLUMN_MIGRATION);
            if cql == LEDGER_SCAN {
                return Ok(self.ledger.iter().map(|i| json!({ "id": i })).collect());
            }
            if cql == LEGACY_BLOCK_MAP_SCAN {
                return Ok(self.block.iter().map(|b| json!({ "egroup_id": b.2 })).collect());
            }
            if cql == BLOCK_MAP_SCAN {
                assert!(has_col, "read a column the ledger says does not exist");
                return Ok(self
                    .block
                    .iter()
                    .map(|b| json!({ "egroup_id": b.2, "extent_id": b.3 }))
                    .collect());
            }
            if cql == EXTENT_MAP_SCAN {
                if self.fail_extent_scan.get() {
                    return Err(Error::meta("daruk refused statement: timeout"));
                }
                return Ok(self
                    .extents
                    .iter()
                    .map(|e| json!({ "extent_id": e.0, "egroup_id": e.1 }))
                    .collect());
            }
            panic!("the model was asked a statement it does not know: {cql}");
        }
    }

    const BOTH: [&str; 2] = [TABLE_MIGRATION, COLUMN_MIGRATION];

    /// Build a random world and the set of groups that are *actually reachable* in it,
    /// computed from the choices made while building rather than from the rows afterwards,
    /// so the expectation does not share any logic with the code under test.
    fn world(seed: u64, levels: &[&'static str], allow_extents: bool) -> (Model, HashSet<String>) {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let groups: Vec<String> = (0..(3 + rng.below(20))).map(|i| format!("eg-{i}")).collect();

        // Extents: each names one group. Some are never referenced by any vdisk, and their
        // groups must stay unmarked unless something else reaches them.
        let mut extents = Vec::new();
        if allow_extents {
            for i in 0..rng.below(25) {
                let g = groups[rng.below(groups.len() as u64) as usize].clone();
                extents.push((format!("ex-{i}"), Some(g), 0u32, 1024u32));
            }
        }

        let mut block = Vec::new();
        let mut reachable = HashSet::new();
        for v in 0..rng.below(8) {
            let vdisk = format!("vd-{v}");
            for idx in 0..rng.below(30) {
                let via_extent = allow_extents && !extents.is_empty() && rng.below(2) == 0;
                if via_extent {
                    let e = &extents[rng.below(extents.len() as u64) as usize];
                    reachable.insert(e.1.clone().unwrap());
                    block.push((vdisk.clone(), idx, None, Some(e.0.clone())));
                } else {
                    let g = groups[rng.below(groups.len() as u64) as usize].clone();
                    reachable.insert(g.clone());
                    block.push((vdisk.clone(), idx, Some(g), None));
                }
            }
        }
        let model = Model {
            ledger: levels.to_vec(),
            block,
            extents,
            fail_extent_scan: Cell::new(false),
            statements: Default::default(),
        };
        (model, reachable)
    }

    /// The first generated world in which some block row names an extent, so a test about
    /// extents cannot pass vacuously because its seed happened to build a two-level map.
    fn world_using_extents(levels: &[&'static str]) -> (Model, HashSet<String>) {
        (1u64..)
            .map(|seed| world(seed, levels, true))
            .find(|(m, _)| m.block.iter().any(|b| b.3.is_some()))
            .unwrap()
    }

    /// Marking equals exactly what is reachable through either level.
    ///
    /// The property the sweep's safety rests on, stated both ways. A group reachable
    /// through an extent that is not marked is live data the sweep may delete; a group
    /// marked that nothing reaches -- an extent row no block row names -- is a leak that
    /// can never be reclaimed. The first is data loss and the second is the mark phase
    /// quietly becoming "keep everything", so both directions are asserted.
    #[test]
    fn marked_groups_are_exactly_those_reachable_through_either_level() {
        let mut used_extents = 0;
        for seed in 1..=600u64 {
            let (model, reachable) = world(seed, &BOTH, true);
            if model.block.iter().any(|b| b.3.is_some()) {
                used_extents += 1;
            }
            let marked = referenced_egroups(&model)
                .unwrap_or_else(|e| panic!("seed {seed}: a well-formed map aborted the sweep: {e}"));
            assert_eq!(marked, reachable, "seed {seed}");
        }
        assert!(used_extents > 300, "the generator stopped exercising the extent level");
    }

    /// With no row naming an extent, marking is the old scan, whatever the schema says.
    ///
    /// Stage 1 of D-23 has to be deployable on its own, and "behaves exactly as before"
    /// has to hold on every cluster state it can meet: before either migration, after the
    /// table but before the column, and after both with an empty extent map. The first
    /// asserts the old statement is what is issued, so a cluster that has not migrated is
    /// never sent a column it does not have.
    #[test]
    fn with_no_extent_rows_marking_is_the_old_scan_on_every_schema_state() {
        let states: [&[&'static str]; 4] = [&[], &[TABLE_MIGRATION], &[COLUMN_MIGRATION], &BOTH];
        for seed in 1..=200u64 {
            for levels in states {
                let (model, reachable) = world(seed, levels, false);
                let marked = referenced_egroups(&model).unwrap();
                assert_eq!(marked, reachable, "seed {seed}, ledger {levels:?}");
                let issued = model.statements.borrow();
                assert!(!issued.iter().any(|s| s == EXTENT_MAP_SCAN),
                        "read the extent map though nothing names an extent");
                if !levels.contains(&COLUMN_MIGRATION) {
                    assert!(issued.iter().any(|s| s == LEGACY_BLOCK_MAP_SCAN));
                    assert!(!issued.iter().any(|s| s.contains("extent_id")));
                }
            }
        }
    }

    /// A block row naming an extent the extent map does not hold aborts the mark.
    ///
    /// Aborting, not skipping: skipping leaves the group behind that row unmarked, and an
    /// unmarked group is one the sweep is allowed to delete. A sweep that errors reclaims
    /// nothing, which is the safe failure.
    #[test]
    fn an_extent_that_cannot_be_followed_aborts_rather_than_being_skipped() {
        let (mut model, _) = world(7, &BOTH, true);
        model.block.push(("vd-x".into(), 0, None, Some("ex-gone".into())));
        let err = referenced_egroups(&model).unwrap_err().to_string();
        assert!(err.contains("ex-gone"), "{err}");
    }

    /// An extent whose own row names no group is as unfollowable as a missing one.
    #[test]
    fn an_extent_row_with_no_group_is_not_a_reference_to_nothing() {
        let (mut model, _) = world(7, &BOTH, true);
        model.extents.push(("ex-null".into(), None, 0, 0));
        model.block.push(("vd-x".into(), 0, None, Some("ex-null".into())));
        assert!(referenced_egroups(&model).is_err());
    }

    /// A failed read of the extent map aborts the mark instead of yielding the groups the
    /// block map named directly, which is a smaller set and so a more dangerous answer.
    #[test]
    fn a_failed_extent_map_scan_never_yields_a_partial_answer() {
        let (model, _) = world_using_extents(&BOTH);
        assert!(model.block.iter().any(|b| b.3.is_some()), "pick a seed that uses extents");
        model.fail_extent_scan.set(true);
        assert!(referenced_egroups(&model).is_err());
    }

    /// Extent rows with the table unrecorded is a map that cannot be marked, not a map
    /// with nothing in it. Someone wrote an extent id without the ledger saying the table
    /// exists, and the right response to a state that should be impossible is to stop
    /// deleting.
    #[test]
    fn extent_names_without_the_table_migration_abort_the_mark() {
        let (model, _) = world_using_extents(&[COLUMN_MIGRATION]);
        assert!(referenced_egroups(&model).is_err());
    }

    /// A row that fills both columns keeps both groups alive.
    #[test]
    fn a_row_naming_both_levels_marks_both() {
        let mut extents = HashMap::new();
        extents.insert("ex-1".to_string(), Some("eg-b".to_string()));
        let rows = vec![(Some("eg-a".to_string()), Some("ex-1".to_string()))];
        let marked = mark(&rows, &extents).unwrap();
        assert!(marked.contains("eg-a") && marked.contains("eg-b"));
    }

    /// The ledger ids are the ones the schema declares. The Python side asserts the same
    /// strings against `helios_schema.MIGRATIONS`.
    #[test]
    fn migration_ids_are_the_assigned_ones() {
        assert!(TABLE_MIGRATION.starts_with("0020-"));
        assert!(COLUMN_MIGRATION.starts_with("0021-"));
    }
}
