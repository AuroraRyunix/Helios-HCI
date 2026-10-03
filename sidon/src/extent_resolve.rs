//! Reading a vdisk whose map rows name extents (D-23, stage 2).
//!
//! `Vdisk::load_map` calls [`resolve`] only for the block-map rows that carry no
//! `egroup_id`. A vdisk whose rows all name groups -- every vdisk that exists today --
//! never reaches this file and issues exactly the one statement it always did, which is
//! what keeps the two-level path byte for byte what it was.
//!
//! The read side is shape-driven rather than flag-driven on purpose. A flag would have to
//! agree with the rows, and a reader that disagreed with them is a reader that returns the
//! wrong vdisk's bytes; a row that says which level it uses cannot disagree with itself.
//! What stays off by default is the *writer*: nothing in this tree writes an extent id.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::err::{Error, Result};
use crate::extent_id_map::Rows;
use crate::meta::cql_str;

fn text(row: &Value, name: &str) -> Option<String> {
    row.get(name).and_then(Value::as_str).map(str::to_string)
}

/// Where one extent lives, as the extent id map records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedExtent {
    pub egroup_id: String,
    pub offset: u32,
    pub length: u32,
    /// `None` when the row carries no identity, in which case the caller's own applies.
    pub vdisk_hash: Option<u64>,
}

fn number(row: &Value, name: &str) -> Result<u64> {
    match row.get(name) {
        Some(Value::Number(n)) if n.is_i64() => Ok(n.as_i64().unwrap_or(0).max(0) as u64),
        Some(Value::Number(n)) if n.is_u64() => Ok(n.as_u64().unwrap_or(0)),
        _ => Err(Error::meta(format!("extent id map row is missing numeric column '{name}'"))),
    }
}

/// Resolve the extents a vdisk's map names rather than points at.
///
/// Called by `Vdisk::load_map` only for the indexes whose block-map row had no `egroup_id`.
/// It re-reads the vdisk's partition to learn the extent id for each, and refuses a row that
/// fills both columns: a reader that picked one would be guessing which of two writers was
/// last, and an extent-granular clone is exactly where the wrong guess returns another
/// vdisk's bytes.
pub fn resolve<D: Rows>(
    db: &D,
    vdisk_id: &str,
    indexes: &[u64],
) -> Result<Vec<(u64, ResolvedExtent)>> {
    let rows = db.rows(&format!(
        "SELECT extent_index, egroup_id, extent_id FROM hydra.dfs_block_map WHERE vdisk_id = {}",
        cql_str(vdisk_id)
    ))?;
    let wanted: HashSet<u64> = indexes.iter().copied().collect();
    let mut named: Vec<(u64, String)> = Vec::new();
    for row in &rows {
        let idx = number(row, "extent_index")?;
        if !wanted.contains(&idx) {
            continue;
        }
        if text(row, "egroup_id").is_some() {
            return Err(Error::corrupt(format!(
                "vdisk {vdisk_id} extent {idx} names both an extent group and an extent id"
            )));
        }
        let extent = text(row, "extent_id").ok_or_else(|| {
            Error::meta(format!("vdisk {vdisk_id} extent {idx} names neither a group nor an extent"))
        })?;
        named.push((idx, extent));
    }
    if named.len() != wanted.len() {
        return Err(Error::meta(format!(
            "vdisk {vdisk_id}: {} block-map row(s) vanished between two reads of its partition",
            wanted.len() - named.len()
        )));
    }

    let mut ids: Vec<&str> = named.iter().map(|(_, e)| e.as_str()).collect();
    ids.sort_unstable();
    ids.dedup();
    let mut found: HashMap<String, ResolvedExtent> = HashMap::new();
    for chunk in ids.chunks(100) {
        let list = chunk.iter().map(|e| cql_str(e)).collect::<Vec<_>>().join(", ");
        let rows = db.rows(&format!(
            "SELECT extent_id, egroup_id, egroup_offset, length, vdisk_hash \
             FROM hydra.dfs_extent_id_map WHERE extent_id IN ({list})"
        ))?;
        for row in rows {
            let (Some(id), Some(egroup_id)) = (text(&row, "extent_id"), text(&row, "egroup_id"))
            else {
                continue;
            };
            let vdisk_hash = row.get("vdisk_hash").and_then(Value::as_i64).map(|v| v as u64);
            found.insert(
                id,
                ResolvedExtent {
                    egroup_id,
                    offset: number(&row, "egroup_offset")? as u32,
                    length: number(&row, "length")? as u32,
                    vdisk_hash,
                },
            );
        }
    }

    let mut out = Vec::with_capacity(named.len());
    for (idx, extent) in named {
        match found.get(&extent) {
            Some(r) => out.push((idx, r.clone())),
            None => {
                return Err(Error::corrupt(format!(
                    "vdisk {vdisk_id} extent {idx} names extent {extent}, which the extent id \
                     map does not hold"
                )))
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A vdisk's block-map partition and the extent id map, as the resolver sees them.
    struct Model {
        /// (vdisk, index, egroup_id, extent_id)
        block: Vec<(String, u64, Option<String>, Option<String>)>,
        /// (extent_id, egroup_id, offset, length)
        extents: Vec<(String, Option<String>, u32, u32)>,
    }

    impl Rows for Model {
        fn rows(&self, cql: &str) -> Result<Vec<Value>> {
            if cql.starts_with("SELECT extent_index, egroup_id, extent_id") {
                return Ok(self
                    .block
                    .iter()
                    .filter(|b| cql.ends_with(&cql_str(&b.0)))
                    .map(|b| json!({ "extent_index": b.1, "egroup_id": b.2, "extent_id": b.3 }))
                    .collect());
            }
            if cql.starts_with("SELECT extent_id, egroup_id, egroup_offset") {
                return Ok(self
                    .extents
                    .iter()
                    .filter(|e| cql.contains(&cql_str(&e.0)))
                    .map(|e| {
                        json!({ "extent_id": e.0, "egroup_id": e.1,
                                "egroup_offset": e.2, "length": e.3 })
                    })
                    .collect());
            }
            panic!("the model was asked a statement it does not know: {cql}");
        }
    }

    /// Deterministic generator; the crate has no `rand`.
    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n.max(1)
        }
    }

    fn model(seed: u64) -> Model {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let extents: Vec<_> = (0..(1 + rng.below(25)))
            .map(|i| {
                (
                    format!("ex-{i}"),
                    Some(format!("eg-{}", rng.below(9))),
                    (rng.below(4) * 1024) as u32,
                    1024u32,
                )
            })
            .collect();
        let mut block = Vec::new();
        for v in 0..6u64 {
            for idx in 0..rng.below(30) {
                let e = &extents[rng.below(extents.len() as u64) as usize];
                block.push((format!("vd-{v}"), idx, None, Some(e.0.clone())));
            }
        }
        Model { block, extents }
    }

    /// Resolving a vdisk's extent-named rows yields what the extent map says, per row.
    ///
    /// Several vdisks name the same extents, which is the whole point of the middle level,
    /// so the property is per vdisk and per index: each row must come back as the group,
    /// offset and length of the extent *it* names and not of a neighbour's.
    #[test]
    fn a_vdisk_resolves_to_exactly_the_groups_its_extents_name() {
        for seed in 1..=300u64 {
            let model = model(seed);
            for vd in 0..6 {
                let vdisk = format!("vd-{vd}");
                let named: Vec<(u64, String)> = model
                    .block
                    .iter()
                    .filter(|b| b.0 == vdisk)
                    .map(|b| (b.1, b.3.clone().unwrap()))
                    .collect();
                let idx: Vec<u64> = named.iter().map(|n| n.0).collect();
                if idx.is_empty() {
                    continue;
                }
                let got = resolve(&model, &vdisk, &idx).unwrap();
                assert_eq!(got.len(), named.len(), "seed {seed}");
                for (i, loc) in got {
                    let ext = &named.iter().find(|n| n.0 == i).unwrap().1;
                    let want = model.extents.iter().find(|e| &e.0 == ext).unwrap();
                    assert_eq!(Some(&loc.egroup_id), want.1.as_ref(), "seed {seed}");
                    assert_eq!(loc.offset, want.2, "seed {seed}");
                    assert_eq!(loc.length, want.3, "seed {seed}");
                }
            }
        }
    }

    /// A row naming both a group and an extent is refused at read time, not resolved by
    /// preferring one. Which of two writers came last is not something a reader can know.
    #[test]
    fn a_row_naming_both_levels_is_refused_by_the_reader() {
        let mut model = model(3);
        model.extents.push(("ex-z".into(), Some("eg-0".into()), 0, 10));
        model.block.push(("vd-both".into(), 0, Some("eg-1".into()), Some("ex-z".into())));
        assert!(resolve(&model, "vd-both", &[0]).is_err());
    }

    /// An extent the map does not hold is an error the guest sees as EIO, never zeroes.
    #[test]
    fn resolving_an_unknown_extent_is_corruption_not_a_hole() {
        let mut model = model(3);
        model.block.push(("vd-lost".into(), 4, None, Some("ex-missing".into())));
        match resolve(&model, "vd-lost", &[4]) {
            Err(Error::Corrupt(_)) => {}
            other => panic!("expected corruption, got {other:?}"),
        }
    }
}
