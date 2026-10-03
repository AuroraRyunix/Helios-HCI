//! What the block map says is alive inside each extent group.
//!
//! The mark phase asks one question of the map -- is anything pointing at this group? -- and
//! that is all reclamation needs. Compaction and the dedup estimator ask a finer one: *which
//! extents* of this group are pointed at, and by whom. The answer is the same scan read for a
//! different purpose, so it lives here once and both passes consume it, rather than each
//! growing a second reading of the block map that could disagree with the first about what is
//! live.
//!
//! Two properties are carried over from `extent_id_map.rs` deliberately, because this is the
//! same traversal and a pass that was laxer than the mark phase would be the one that acts on
//! a map the sweep refuses to trust:
//!
//! * **Both map levels.** A row naming an extent keeps alive the location that extent's row
//!   names, and the referrer recorded is the *block row*, because that is what has an index and
//!   an owner.
//! * **Fails closed.** An extent a row names that the extent map does not hold, or whose own
//!   row names no group, aborts the scan. Skipping it would report a live extent as dead, and a
//!   pass that rewrites groups on the strength of "dead" is a pass that loses data.
//!
//! Everything here is a pure function of rows, read through [`Rows`], so the passes are
//! tested against a model of the database and not against a copy of their own logic.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::Value;

use crate::err::{Error, Result};
use crate::extent::{vdisk_hash, FOOTER_LEN};
use crate::extent_id_map::{applied_levels, Rows, TABLE_MIGRATION};
use crate::meta::cql_str;

pub const BLOCK_ROWS_LEGACY: &str =
    "SELECT vdisk_id, extent_index, egroup_id, egroup_offset, length, vdisk_hash \
     FROM hydra.dfs_block_map";
pub const BLOCK_ROWS: &str =
    "SELECT vdisk_id, extent_index, egroup_id, egroup_offset, length, vdisk_hash, extent_id \
     FROM hydra.dfs_block_map";
pub const EXTENT_ROWS: &str =
    "SELECT extent_id, egroup_id, egroup_offset, length, vdisk_hash FROM hydra.dfs_extent_id_map";
pub const VDISK_ROWS: &str =
    "SELECT vdisk_id, class, container, owner, replicas FROM hydra.dfs_vdisks";

fn text(row: &Value, name: &str) -> Option<String> {
    row.get(name).and_then(Value::as_str).map(str::to_string)
}

fn uint(row: &Value, name: &str) -> Option<u64> {
    match row.get(name) {
        Some(Value::Number(n)) => n.as_i64().map(|v| v.max(0) as u64).or_else(|| n.as_u64()),
        _ => None,
    }
}

/// One `dfs_block_map` row.
#[derive(Clone, Debug)]
pub struct BlockRow {
    pub vdisk: String,
    pub idx: u64,
    pub egroup: Option<String>,
    pub offset: Option<u32>,
    pub length: Option<u32>,
    pub vdisk_hash: Option<u64>,
    pub extent: Option<String>,
}

/// One `dfs_extent_id_map` row.
#[derive(Clone, Debug)]
pub struct ExtentRow {
    #[allow(dead_code)]
    pub id: String,
    pub egroup: Option<String>,
    pub offset: Option<u32>,
    pub length: Option<u32>,
    pub vdisk_hash: Option<u64>,
}

/// Something that points at a stored extent: a block-map row, and the extent it names if it
/// names one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Referrer {
    pub vdisk: String,
    pub idx: u64,
    /// The identity the extent's footer was stamped with, which is what a read of this row
    /// compares the footer against (D-18). Resolved the way `Vdisk::load_map` resolves it, so
    /// "does this footer verify for this referrer" has the answer a guest read would get.
    pub vdisk_hash: u64,
    /// `Some` when the row reaches the extent through the middle level.
    pub extent: Option<String>,
}

/// One stored extent that something points at.
#[derive(Clone, Debug, Default)]
pub struct Live {
    /// The stored length, which is what the map records and what a read seeks by.
    pub length: u32,
    pub referrers: Vec<Referrer>,
}

impl Live {
    /// Bytes the extent occupies in its group: the stored extent and its footer.
    pub fn framed(&self) -> u64 {
        self.length as u64 + FOOTER_LEN as u64
    }

}

/// Which extents of which groups the map points at.
#[derive(Debug, Default)]
pub struct Occupancy {
    /// group -> offset -> the extent stored there.
    pub live: BTreeMap<String, BTreeMap<u32, Live>>,
    /// Groups whose rows cannot all be right: two lengths at one offset, or extents that
    /// overlap. A valid map has neither. Nothing is done to such a group; it is reported.
    pub conflicts: BTreeSet<String>,
    pub block_rows: usize,
    pub extent_rows_used: usize,
}

impl Occupancy {
    /// Bytes of a group that something points at.
    pub fn live_bytes(&self, group: &str) -> u64 {
        self.live
            .get(group)
            .map(|m| m.values().map(Live::framed).sum())
            .unwrap_or(0)
    }
}

/// Group the rows by the extent they reach.
///
/// `extents` is keyed by extent id. Returns an error naming the extents that could not be
/// followed, never a partial answer: see the module header.
pub fn build(rows: &[BlockRow], extents: &HashMap<String, ExtentRow>) -> Result<Occupancy> {
    let mut occ = Occupancy { block_rows: rows.len(), ..Occupancy::default() };
    let mut dangling: BTreeSet<String> = BTreeSet::new();
    let mut used: BTreeSet<&str> = BTreeSet::new();

    for row in rows {
        if let Some(group) = &row.egroup {
            let (Some(offset), Some(length)) = (row.offset, row.length) else {
                return Err(Error::meta(format!(
                    "vdisk {} extent {} names group {group} but not where in it",
                    row.vdisk, row.idx
                )));
            };
            let vh = row.vdisk_hash.unwrap_or_else(|| vdisk_hash(&row.vdisk));
            add(
                &mut occ,
                group,
                offset,
                length,
                Referrer { vdisk: row.vdisk.clone(), idx: row.idx, vdisk_hash: vh, extent: None },
            );
        }
        if let Some(extent) = &row.extent {
            let followed = extents.get(extent).and_then(|e| match (&e.egroup, e.offset, e.length) {
                (Some(g), Some(o), Some(l)) => Some((g.clone(), o, l, e.vdisk_hash)),
                _ => None,
            });
            match followed {
                Some((group, offset, length, evh)) => {
                    used.insert(extent.as_str());
                    // The extent row's identity, as the reader uses it; the block row's own
                    // `vdisk_hash` is not consulted for an extent-named row.
                    let vh = evh.unwrap_or_else(|| vdisk_hash(&row.vdisk));
                    add(
                        &mut occ,
                        &group,
                        offset,
                        length,
                        Referrer {
                            vdisk: row.vdisk.clone(),
                            idx: row.idx,
                            vdisk_hash: vh,
                            extent: Some(extent.clone()),
                        },
                    );
                }
                None => {
                    dangling.insert(extent.clone());
                }
            }
        }
    }
    if !dangling.is_empty() {
        let sample: Vec<&str> = dangling.iter().take(5).map(String::as_str).collect();
        return Err(Error::meta(format!(
            "{} extent id(s) named by the block map have no usable row in the extent id map \
             (first: {sample:?}); refusing to say what is live, because every extent behind \
             them would look dead",
            dangling.len()
        )));
    }
    occ.extent_rows_used = used.len();

    // Extents that overlap cannot both be right.
    for (group, by_offset) in &occ.live {
        let mut end = 0u64;
        for (offset, live) in by_offset {
            if (*offset as u64) < end {
                occ.conflicts.insert(group.clone());
            }
            end = end.max(*offset as u64 + live.framed());
        }
    }
    Ok(occ)
}

fn add(occ: &mut Occupancy, group: &str, offset: u32, length: u32, referrer: Referrer) {
    let slot = occ.live.entry(group.to_string()).or_default().entry(offset).or_default();
    if slot.referrers.is_empty() {
        slot.length = length;
    } else if slot.length != length {
        occ.conflicts.insert(group.to_string());
    }
    slot.referrers.push(referrer);
}

/// Read the block map, and the extent map if anything names an extent, and group them.
///
/// The ledger decides which statement is sent, exactly as the mark phase does, so a cluster
/// that has not applied `0021` is never sent a column it lacks.
pub fn scan<D: Rows>(db: &D) -> Result<Occupancy> {
    let (has_column, has_table) = applied_levels(db)?;
    let statement = if has_column { BLOCK_ROWS } else { BLOCK_ROWS_LEGACY };
    let mut rows = Vec::new();
    for r in db.rows(statement)? {
        let Some(vdisk) = text(&r, "vdisk_id") else { continue };
        let Some(idx) = uint(&r, "extent_index") else { continue };
        rows.push(BlockRow {
            vdisk,
            idx,
            egroup: text(&r, "egroup_id"),
            offset: uint(&r, "egroup_offset").map(|v| v as u32),
            length: uint(&r, "length").map(|v| v as u32),
            vdisk_hash: r.get("vdisk_hash").and_then(Value::as_i64).map(|v| v as u64),
            extent: text(&r, "extent_id"),
        });
    }
    let mut extents: HashMap<String, ExtentRow> = HashMap::new();
    if rows.iter().any(|r| r.extent.is_some()) {
        if !has_table {
            return Err(Error::meta(format!(
                "the block map names extents but {TABLE_MIGRATION} is not recorded as applied; \
                 refusing to say what is live from a map whose middle level cannot be read"
            )));
        }
        for r in db.rows(EXTENT_ROWS)? {
            if let Some(id) = text(&r, "extent_id") {
                extents.insert(
                    id.clone(),
                    ExtentRow {
                        id,
                        egroup: text(&r, "egroup_id"),
                        offset: uint(&r, "egroup_offset").map(|v| v as u32),
                        length: uint(&r, "length").map(|v| v as u32),
                        vdisk_hash: r.get("vdisk_hash").and_then(Value::as_i64).map(|v| v as u64),
                    },
                );
            }
        }
    }
    build(&rows, &extents)
}

/// What the vdisk table says about a vdisk, for deciding what may be done to its rows.
#[derive(Clone, Debug)]
pub struct VdiskInfo {
    pub class: String,
    pub container: String,
    #[allow(dead_code)]
    pub owner: String,
    pub replicas: Vec<String>,
}

pub fn vdisks<D: Rows>(db: &D) -> Result<HashMap<String, VdiskInfo>> {
    let mut out = HashMap::new();
    for r in db.rows(VDISK_ROWS)? {
        let Some(id) = text(&r, "vdisk_id") else { continue };
        out.insert(
            id,
            VdiskInfo {
                class: text(&r, "class").unwrap_or_default(),
                container: text(&r, "container").unwrap_or_default(),
                owner: text(&r, "owner").unwrap_or_default(),
                replicas: r
                    .get("replicas")
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
                    .unwrap_or_default(),
            },
        );
    }
    Ok(out)
}

/// A `dfs_egroups` row.
#[derive(Clone, Debug)]
pub struct GroupRow {
    pub id: String,
    pub state: String,
    pub created_ms: i64,
    #[allow(dead_code)]
    pub size: u64,
    pub seal_hash: String,
    /// The vdisk that was draining when the group was made, which is the one that knows what
    /// container its bytes belong to.
    pub hint: String,
}

/// The groups Hydra records this node as holding.
pub fn groups_of<D: Rows>(db: &D, node: &str) -> Result<Vec<GroupRow>> {
    let mut out = Vec::new();
    for r in db.rows(&format!(
        "SELECT egroup_id, state, created_at_ms, size, seal_hash, vdisk_hint \
         FROM hydra.dfs_egroups WHERE node = {} ALLOW FILTERING",
        cql_str(node)
    ))? {
        let Some(id) = text(&r, "egroup_id") else { continue };
        out.push(GroupRow {
            id,
            state: text(&r, "state").unwrap_or_else(|| "unknown".to_string()),
            created_ms: r.get("created_at_ms").and_then(Value::as_i64).unwrap_or(0),
            size: uint(&r, "size").unwrap_or(0),
            seal_hash: text(&r, "seal_hash").unwrap_or_default(),
            hint: text(&r, "vdisk_hint").unwrap_or_default(),
        });
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn direct(vdisk: &str, idx: u64, group: &str, offset: u32, length: u32) -> BlockRow {
        BlockRow {
            vdisk: vdisk.into(),
            idx,
            egroup: Some(group.into()),
            offset: Some(offset),
            length: Some(length),
            vdisk_hash: Some(vdisk_hash(vdisk)),
            extent: None,
        }
    }

    fn named(vdisk: &str, idx: u64, extent: &str) -> BlockRow {
        BlockRow {
            vdisk: vdisk.into(),
            idx,
            egroup: None,
            offset: None,
            length: None,
            vdisk_hash: None,
            extent: Some(extent.into()),
        }
    }

    /// A clone's rows are a copy of its parent's, so the extent they share is one extent with
    /// two referrers -- not two extents, and not one that counts twice toward a group's live
    /// bytes. Counting a shared extent once is what makes "live fraction" mean something.
    #[test]
    fn a_shared_extent_is_one_extent_with_every_referrer() {
        let rows = vec![
            direct("parent", 0, "eg-a", 0, 1000),
            direct("clone", 0, "eg-a", 0, 1000),
            direct("snap", 0, "eg-a", 0, 1000),
            direct("parent", 1, "eg-a", 1032, 500),
        ];
        let occ = build(&rows, &HashMap::new()).unwrap();
        let group = &occ.live["eg-a"];
        assert_eq!(group.len(), 2);
        assert_eq!(group[&0].referrers.len(), 3);
        assert_eq!(occ.live_bytes("eg-a"), 1000 + 32 + 500 + 32);
        assert!(occ.conflicts.is_empty());
    }

    /// An extent reached through the middle level is live in the group its extent row names,
    /// and the referrer is the block row that reaches it.
    #[test]
    fn a_named_extent_is_live_where_its_row_says() {
        let mut extents = HashMap::new();
        extents.insert(
            "ex-1".to_string(),
            ExtentRow {
                id: "ex-1".into(),
                egroup: Some("eg-b".into()),
                offset: Some(2048),
                length: Some(700),
                vdisk_hash: Some(77),
            },
        );
        let rows = vec![named("v1", 3, "ex-1"), named("v2", 3, "ex-1")];
        let occ = build(&rows, &extents).unwrap();
        let live = &occ.live["eg-b"][&2048];
        assert_eq!(live.length, 700);
        assert_eq!(live.referrers.len(), 2);
        assert!(live.referrers.iter().all(|r| r.vdisk_hash == 77 && r.extent.as_deref() == Some("ex-1")));
        assert_eq!(occ.extent_rows_used, 1);
    }

    /// The fail-closed rule, shared with the mark phase: an extent that cannot be followed
    /// aborts the answer. A partial one would call a live extent dead.
    #[test]
    fn an_extent_that_cannot_be_followed_aborts_rather_than_being_skipped() {
        let rows = vec![direct("v", 0, "eg-a", 0, 10), named("v", 1, "ex-missing")];
        assert!(build(&rows, &HashMap::new()).is_err());
        let mut extents = HashMap::new();
        extents.insert(
            "ex-nogroup".to_string(),
            ExtentRow { id: "ex-nogroup".into(), egroup: None, offset: None, length: None, vdisk_hash: None },
        );
        assert!(build(&[named("v", 1, "ex-nogroup")], &extents).is_err());
    }

    /// Two lengths at one offset, or overlapping extents, cannot both be a valid map. The
    /// group is flagged and left alone rather than "fixed" by picking one.
    #[test]
    fn rows_that_disagree_about_a_group_flag_it() {
        let rows = vec![direct("a", 0, "eg-x", 0, 100), direct("b", 0, "eg-x", 0, 200)];
        assert!(build(&rows, &HashMap::new()).unwrap().conflicts.contains("eg-x"));
        let rows = vec![direct("a", 0, "eg-y", 0, 100), direct("b", 1, "eg-y", 64, 100)];
        assert!(build(&rows, &HashMap::new()).unwrap().conflicts.contains("eg-y"));
        let rows = vec![direct("a", 0, "eg-z", 0, 100), direct("b", 1, "eg-z", 132, 100)];
        assert!(build(&rows, &HashMap::new()).unwrap().conflicts.is_empty());
    }

    /// A row from before the identity column existed reads under its own vdisk's hash.
    #[test]
    fn a_row_with_no_recorded_identity_reads_under_its_own_vdisks() {
        let mut row = direct("old", 0, "eg-a", 0, 10);
        row.vdisk_hash = None;
        let occ = build(&[row], &HashMap::new()).unwrap();
        assert_eq!(occ.live["eg-a"][&0].referrers[0].vdisk_hash, vdisk_hash("old"));
    }
}
