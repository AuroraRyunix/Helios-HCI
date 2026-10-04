//! The production [`MapSink`]: a snapshot's map written to Hydra through Daruk, in the order that
//! keeps a half-published snapshot invisible.
//!
//! **Status: exercised only against an in-memory stand-in for Hydra.** Nothing in the daemon
//! constructs this yet; there is no listener and no second site. The statements it issues are
//! the ones the rest of Sidon already issues (`block_map_batches`, `/v1/dfs/vdisk-create`,
//! `/v1/dfs/vdisk-class`), which is why a stand-in can be honest about them, but a real Scylla
//! has not seen this module.
//!
//! The order is the whole point:
//!
//! 1. `begin` creates the vdisk row in class `forming` (an existing `forming` row is reused and
//!    its map cleared, so a crashed publish is simply redone). Nothing lists or attaches a
//!    `forming` vdisk.
//! 2. `write_rows` appends the map in `MAP_BATCH`-sized single-partition batches.
//! 3. `read_back` returns what Hydra now holds, so the importer can recompute the digest and
//!    catch a lost write here rather than in a guest.
//! 4. `make_visible` is one compare-and-swap, `forming` to `immutable`. A snapshot is visible if
//!    and only if that flip applied.
//!
//! A name that is already `immutable` is never touched: `begin` refuses it.

use serde_json::{json, Value};

use crate::err::{Error, Result};
use crate::meta::{block_map_batches, cql_str, now_ms, Cas, Daruk, MAP_BATCH};

use super::import::MapSink;
use super::{Manifest, MapRow};

/// The class a replica is in while its map is being written, and the one it is flipped to.
pub const FORMING: &str = "forming";
pub const VISIBLE_CLASS: &str = "immutable";

/// The two things this sink needs from Hydra. `Daruk` is the production implementation.
pub trait Hydra {
    fn query(&self, cql: &str) -> Result<Vec<Value>>;
    fn cas(&self, path: &str, params: Value) -> Result<Cas>;
}

impl Hydra for Daruk {
    fn query(&self, cql: &str) -> Result<Vec<Value>> {
        Daruk::query(self, cql)
    }
    fn cas(&self, path: &str, params: Value) -> Result<Cas> {
        Daruk::cas(self, path, params)
    }
}

pub struct HydraMapSink<H: Hydra> {
    hydra: H,
    container: String,
}

impl<H: Hydra> HydraMapSink<H> {
    pub fn new(hydra: H, container: &str) -> Self {
        HydraMapSink { hydra, container: container.to_string() }
    }

    fn vdisk_row(&self, snapshot: &str) -> Result<Option<Value>> {
        let rows = self.hydra.query(&format!(
            "SELECT vdisk_id, class, size_bytes, extent_bytes FROM hydra.dfs_vdisks WHERE vdisk_id = {};",
            cql_str(snapshot)
        ))?;
        Ok(rows.into_iter().next())
    }

    fn map_rows(&self, snapshot: &str) -> Result<Vec<MapRow>> {
        let rows = self.hydra.query(&format!(
            "SELECT extent_index, egroup_id, egroup_offset, length, vdisk_hash \
             FROM hydra.dfs_block_map WHERE vdisk_id = {};",
            cql_str(snapshot)
        ))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let int = |k: &str| {
                r.get(k).and_then(Value::as_i64).ok_or_else(|| {
                    Error::meta(format!("block-map row of {snapshot} has no integer {k}"))
                })
            };
            out.push(MapRow {
                extent_index: int("extent_index")? as u64,
                group: r
                    .get("egroup_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::meta(format!("block-map row of {snapshot} names no group")))?
                    .to_string(),
                offset: int("egroup_offset")? as u32,
                length: int("length")? as u32,
                // Stored signed; the top bit set is a legitimate hash and must come back as one.
                vdisk_hash: int("vdisk_hash")? as u64,
            });
        }
        out.sort_by_key(|r| r.extent_index);
        Ok(out)
    }
}

fn class_of(row: &Value) -> String {
    row.get("class").and_then(Value::as_str).unwrap_or("").to_string()
}

impl<H: Hydra> MapSink for HydraMapSink<H> {
    /// The digest of a snapshot that is visible. A Hydra that cannot be asked answers `None`,
    /// which is safe: the caller then calls `begin`, which asks again and refuses a visible name.
    fn visible(&self, snapshot: &str) -> Option<String> {
        let row = self.vdisk_row(snapshot).ok()??;
        if class_of(&row) != VISIBLE_CLASS {
            return None;
        }
        let rows = self.map_rows(snapshot).ok()?;
        let size = row.get("size_bytes").and_then(Value::as_i64)? as u64;
        let extent_bytes = row.get("extent_bytes").and_then(Value::as_i64)? as u64;
        Some(
            Manifest { snapshot: snapshot.to_string(), size_bytes: size, extent_bytes, rows, groups: vec![] }
                .map_digest(),
        )
    }

    fn begin(&mut self, m: &Manifest) -> Result<()> {
        match self.vdisk_row(&m.snapshot)? {
            Some(row) => {
                let class = class_of(&row);
                if class != FORMING {
                    return Err(Error::refused(format!(
                        "{} already exists in class {class:?}; a replica's name is never overwritten",
                        m.snapshot
                    )));
                }
                // A publish that did not finish: start its map again from nothing.
                self.hydra.query(&format!(
                    "DELETE FROM hydra.dfs_block_map WHERE vdisk_id = {};",
                    cql_str(&m.snapshot)
                ))?;
                Ok(())
            }
            None => {
                let made = self.hydra.cas(
                    "/v1/dfs/vdisk-create",
                    json!({
                        "vdisk_id": m.snapshot,
                        "container": self.container,
                        "size_bytes": m.size_bytes,
                        "class": FORMING,
                        "owner": "",
                        "epoch": 0,
                        "drain_seq": 0,
                        "extent_bytes": m.extent_bytes,
                        "egroup_bytes": 4194304,
                        "created_at_ms": now_ms(),
                        "replicas": [],
                        "rf": 1,
                        "parent_vdisk": "",
                    }),
                )?;
                if !made.applied {
                    // Someone created it between our read and our create: begin again, which
                    // takes the other branch.
                    return Err(Error::meta(format!(
                        "{} appeared while it was being created; retry", m.snapshot)));
                }
                Ok(())
            }
        }
    }

    fn write_rows(&mut self, snapshot: &str, rows: &[MapRow]) -> Result<()> {
        let tuples: Vec<(u64, String, u32, u32, u64)> = rows
            .iter()
            .map(|r| (r.extent_index, r.group.clone(), r.offset, r.length, r.vdisk_hash))
            .collect();
        for statement in block_map_batches(snapshot, 0, &tuples, MAP_BATCH) {
            self.hydra.query(&statement)?;
        }
        Ok(())
    }

    fn read_back(&self, snapshot: &str) -> Result<Vec<MapRow>> {
        self.map_rows(snapshot)
    }

    fn make_visible(&mut self, snapshot: &str) -> Result<()> {
        let flip = self.hydra.cas(
            "/v1/dfs/vdisk-class",
            json!({"vdisk_id": snapshot, "class": VISIBLE_CLASS, "expected_class": FORMING}),
        )?;
        if flip.applied || flip.current_str("class") == VISIBLE_CLASS {
            // Applied now, or applied by an earlier attempt whose answer was lost.
            return Ok(());
        }
        Err(Error::meta(format!(
            "{snapshot} could not be made visible: it is in class {}", flip.current_str("class"))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct World {
        /// vdisk id -> (class, size, extent_bytes)
        vdisks: BTreeMap<String, (String, i64, i64)>,
        /// vdisk id -> extent_index -> (group, offset, length, hash as stored)
        map: BTreeMap<String, BTreeMap<i64, (String, i64, i64, i64)>>,
        statements: Vec<String>,
        fail_flip: bool,
        lose_a_row: bool,
        flip_applies_but_answer_is_lost: bool,
    }

    /// Hydra in memory. It understands exactly the statements this module and `meta` write.
    struct FakeHydra(RefCell<World>);

    fn between<'a>(s: &'a str, open: &str, close: &str) -> &'a str {
        let a = s.find(open).unwrap() + open.len();
        let b = s[a..].find(close).unwrap() + a;
        &s[a..b]
    }

    impl Hydra for FakeHydra {
        fn query(&self, cql: &str) -> Result<Vec<Value>> {
            let mut w = self.0.borrow_mut();
            w.statements.push(cql.split_whitespace().take(3).collect::<Vec<_>>().join(" "));
            if cql.starts_with("BEGIN UNLOGGED BATCH") {
                for stmt in cql.split("INSERT INTO hydra.dfs_block_map").skip(1) {
                    let values = between(stmt, "VALUES (", ")");
                    let f: Vec<&str> = values.split(", ").collect();
                    let id = f[0].trim_matches('\'').to_string();
                    w.map.entry(id).or_default().insert(
                        f[1].parse().unwrap(),
                        (f[2].trim_matches('\'').to_string(), f[3].parse().unwrap(),
                         f[4].parse().unwrap(), f[6].parse().unwrap()),
                    );
                }
                return Ok(vec![]);
            }
            if cql.starts_with("DELETE FROM hydra.dfs_block_map") {
                let id = between(cql, "vdisk_id = '", "'");
                w.map.remove(id);
                return Ok(vec![]);
            }
            if cql.starts_with("SELECT vdisk_id, class") {
                let id = between(cql, "vdisk_id = '", "'");
                return Ok(w.vdisks.get(id).map(|(c, s, e)| {
                    vec![json!({"vdisk_id": id, "class": c, "size_bytes": s, "extent_bytes": e})]
                }).unwrap_or_default());
            }
            if cql.starts_with("SELECT extent_index") {
                let id = between(cql, "vdisk_id = '", "'").to_string();
                let lose = w.lose_a_row;
                let mut rows: Vec<Value> = w.map.get(&id).into_iter().flat_map(|m| m.iter()).map(
                    |(i, (g, o, l, h))| json!({"extent_index": i, "egroup_id": g,
                        "egroup_offset": o, "length": l, "vdisk_hash": h})).collect();
                if lose {
                    rows.pop();
                }
                return Ok(rows);
            }
            panic!("the fake does not know: {cql}");
        }

        fn cas(&self, path: &str, p: Value) -> Result<Cas> {
            let mut w = self.0.borrow_mut();
            w.statements.push(path.to_string());
            let id = p["vdisk_id"].as_str().unwrap().to_string();
            match path {
                "/v1/dfs/vdisk-create" => {
                    if let Some((class, _, _)) = w.vdisks.get(&id) {
                        return Ok(Cas { applied: false, current: json!({"class": class}) });
                    }
                    w.vdisks.insert(id, (p["class"].as_str().unwrap().to_string(),
                        p["size_bytes"].as_i64().unwrap(), p["extent_bytes"].as_i64().unwrap()));
                    Ok(Cas { applied: true, current: Value::Null })
                }
                "/v1/dfs/vdisk-class" => {
                    if w.fail_flip {
                        return Err(Error::meta("injected failure at the flip".to_string()));
                    }
                    let expected = p["expected_class"].as_str().unwrap().to_string();
                    let new = p["class"].as_str().unwrap().to_string();
                    let lost = w.flip_applies_but_answer_is_lost;
                    let entry = w.vdisks.get_mut(&id).unwrap();
                    if entry.0 == expected {
                        entry.0 = new;
                        if lost {
                            w.flip_applies_but_answer_is_lost = false;
                            return Err(Error::meta("the answer was lost".to_string()));
                        }
                        Ok(Cas { applied: true, current: Value::Null })
                    } else {
                        Ok(Cas { applied: false, current: json!({"class": entry.0}) })
                    }
                }
                other => panic!("the fake does not know {other}"),
            }
        }
    }

    const VH: u64 = 0x8000_0000_0000_00AB;

    fn manifest(name: &str, rows: usize) -> Manifest {
        Manifest {
            snapshot: name.to_string(),
            size_bytes: 1 << 30,
            extent_bytes: 1 << 20,
            rows: (0..rows as u64)
                .map(|i| MapRow { extent_index: i * 2, group: "g1".into(), offset: (i * 4096) as u32,
                                  length: 4096, vdisk_hash: VH })
                .collect(),
            groups: vec![],
        }
    }

    fn sink() -> HydraMapSink<FakeHydra> {
        HydraMapSink::new(FakeHydra(RefCell::new(World::default())), "default")
    }

    fn publish(s: &mut HydraMapSink<FakeHydra>, m: &Manifest) -> Result<()> {
        s.begin(m)?;
        s.write_rows(&m.snapshot, &m.rows)?;
        let back = Manifest { rows: s.read_back(&m.snapshot)?, ..m.clone() };
        if back.map_digest() != m.map_digest() {
            return Err(Error::corrupt("read-back differs".to_string()));
        }
        s.make_visible(&m.snapshot)
    }

    #[test]
    fn a_published_snapshot_reads_back_with_the_same_digest_including_the_top_bit_hash() {
        let mut s = sink();
        let m = manifest("snap-a", 250); // three batches
        publish(&mut s, &m).unwrap();
        assert_eq!(s.visible("snap-a"), Some(m.map_digest()));
        assert_eq!(s.read_back("snap-a").unwrap(), m.rows);
    }

    #[test]
    fn nothing_is_visible_until_the_one_flip() {
        let mut s = sink();
        let m = manifest("snap-b", 10);
        s.begin(&m).unwrap();
        s.write_rows("snap-b", &m.rows).unwrap();
        assert_eq!(s.visible("snap-b"), None, "written but not flipped");
        s.make_visible("snap-b").unwrap();
        assert!(s.visible("snap-b").is_some());
    }

    #[test]
    fn a_publish_that_died_before_the_flip_is_redone_from_an_empty_map() {
        let mut s = sink();
        let m = manifest("snap-c", 10);
        s.begin(&m).unwrap();
        s.write_rows("snap-c", &m.rows[..4]).unwrap(); // dies here
        publish(&mut s, &m).unwrap();
        assert_eq!(s.read_back("snap-c").unwrap(), m.rows, "no leftover from the first attempt");
        assert!(s.visible("snap-c").is_some());
    }

    #[test]
    fn a_visible_name_is_never_overwritten() {
        let mut s = sink();
        publish(&mut s, &manifest("snap-d", 5)).unwrap();
        let err = s.begin(&manifest("snap-d", 6)).unwrap_err();
        assert!(format!("{err}").contains("never overwritten"), "{err}");
        assert_eq!(s.read_back("snap-d").unwrap().len(), 5, "the map was not touched");
    }

    #[test]
    fn a_flip_that_failed_leaves_the_snapshot_invisible_and_a_retry_completes_it() {
        let mut s = sink();
        let m = manifest("snap-e", 5);
        s.hydra.0.borrow_mut().fail_flip = true;
        assert!(publish(&mut s, &m).is_err());
        assert_eq!(s.visible("snap-e"), None);
        s.hydra.0.borrow_mut().fail_flip = false;
        publish(&mut s, &m).unwrap();
        assert!(s.visible("snap-e").is_some());
    }

    #[test]
    fn a_lost_row_is_caught_at_read_back_and_the_snapshot_stays_invisible() {
        let mut s = sink();
        let m = manifest("snap-f", 5);
        s.hydra.0.borrow_mut().lose_a_row = true;
        assert!(publish(&mut s, &m).is_err());
        assert_eq!(s.visible("snap-f"), None);
        assert!(!s.hydra.0.borrow().statements.iter().any(|x| x == "/v1/dfs/vdisk-class"),
                "the flip must not be attempted over a map that does not match");
    }

    #[test]
    fn the_flip_is_idempotent_when_an_earlier_answer_was_lost() {
        let mut s = sink();
        let m = manifest("snap-g", 3);
        s.hydra.0.borrow_mut().flip_applies_but_answer_is_lost = true;
        assert!(publish(&mut s, &m).is_err(), "the caller saw an error");
        // Hydra did apply it: the snapshot is visible, and publishing again is a refusal of the
        // name by `begin`, which the importer's `visible()` check answers first in real use.
        assert!(s.visible("snap-g").is_some());
        assert_eq!(s.visible("snap-g"), Some(m.map_digest()));
        s.make_visible("snap-g").unwrap();
    }

    #[test]
    fn the_order_of_calls_is_create_batches_read_flip() {
        let mut s = sink();
        publish(&mut s, &manifest("snap-h", 150)).unwrap();
        let order: Vec<String> = s.hydra.0.borrow().statements.iter()
            .filter(|x| x.starts_with("/v1") || x.starts_with("BEGIN") || x.starts_with("SELECT extent"))
            .cloned().collect();
        assert_eq!(order, vec!["/v1/dfs/vdisk-create", "BEGIN UNLOGGED BATCH", "BEGIN UNLOGGED BATCH",
                               "SELECT extent_index, egroup_id,", "/v1/dfs/vdisk-class"]);
    }
}
