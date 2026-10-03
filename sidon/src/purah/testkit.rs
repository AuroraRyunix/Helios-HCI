//! A model of Hydra, a real extent store and a fake daemon, for the passes that read the block
//! map and rewrite or hash extent groups.
//!
//! Nothing here is the thing under test. The extent store is real files with real footers; what
//! is modelled is the database (rows, and compare-and-swaps that really compare) and the
//! daemon's peers and attached vdisks, each of which can be told to fail. That is what lets a
//! test stop a batch between any two statements and then ask what the map says.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;

use serde_json::{json, Value};

use super::compact::{Db, Env, Hold};
use super::occupancy::{BLOCK_ROWS, EXTENT_ROWS, VDISK_ROWS};
use crate::err::{Error, Result};
use crate::extent::{vdisk_hash, EgroupStore};
use crate::extent_id_map::{Rows, LEDGER_SCAN};
use crate::meta::{cql_str, Cas};

#[derive(Clone, Debug)]
pub struct MRow {
    pub vdisk: String,
    pub idx: u64,
    pub egroup: Option<String>,
    pub offset: u32,
    pub length: u32,
    pub vhash: u64,
    pub extent: Option<String>,
}

#[derive(Clone, Debug)]
pub struct MExt {
    pub egroup: String,
    pub offset: u32,
    pub length: u32,
    pub vhash: u64,
}

#[derive(Clone, Debug)]
pub struct MVdisk {
    pub class: String,
    pub container: String,
    pub replicas: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct MGroup {
    pub state: String,
    pub node: String,
    pub created_ms: i64,
    pub size: u64,
    pub seal_hash: String,
    pub hint: String,
}

pub type CasHook = Box<dyn FnMut(&mut State, &str, &Value)>;

#[derive(Default)]
pub struct State {
    pub block: Vec<MRow>,
    pub extents: BTreeMap<String, MExt>,
    pub vdisks: BTreeMap<String, MVdisk>,
    pub egroups: BTreeMap<String, MGroup>,
    /// Every compare-and-swap issued, as `path`, in order.
    pub cas_log: Vec<String>,
    /// Fail the nth compare-and-swap to this path (0-based) with a metadata error.
    pub fail_cas: Option<(String, usize)>,
    /// Called before each compare-and-swap is evaluated, with the state it will see: how a
    /// test makes a drain land between a scan and a repoint.
    pub before_cas: Option<CasHook>,
}

pub struct Model {
    pub st: RefCell<State>,
}

impl Model {
    pub fn new() -> Model {
        Model { st: RefCell::new(State::default()) }
    }
}

impl Rows for Model {
    fn rows(&self, cql: &str) -> Result<Vec<Value>> {
        let st = self.st.borrow();
        if cql == LEDGER_SCAN {
            return Ok(vec![
                json!({"id": crate::extent_id_map::TABLE_MIGRATION}),
                json!({"id": crate::extent_id_map::COLUMN_MIGRATION}),
            ]);
        }
        if cql == BLOCK_ROWS {
            return Ok(st
                .block
                .iter()
                .map(|r| {
                    json!({
                        "vdisk_id": r.vdisk, "extent_index": r.idx,
                        "egroup_id": r.egroup,
                        "egroup_offset": if r.egroup.is_some() { json!(r.offset) } else { Value::Null },
                        "length": if r.egroup.is_some() { json!(r.length) } else { Value::Null },
                        "vdisk_hash": r.vhash as i64,
                        "extent_id": r.extent,
                    })
                })
                .collect());
        }
        if cql == EXTENT_ROWS {
            return Ok(st
                .extents
                .iter()
                .map(|(id, e)| {
                    json!({"extent_id": id, "egroup_id": e.egroup, "egroup_offset": e.offset,
                           "length": e.length, "vdisk_hash": e.vhash as i64})
                })
                .collect());
        }
        if cql == VDISK_ROWS {
            return Ok(st
                .vdisks
                .iter()
                .map(|(id, v)| {
                    json!({"vdisk_id": id, "class": v.class, "container": v.container,
                           "owner": "", "replicas": v.replicas})
                })
                .collect());
        }
        if cql.starts_with("SELECT egroup_id, state, created_at_ms, size, seal_hash, vdisk_hint") {
            return Ok(st
                .egroups
                .iter()
                .filter(|(_, g)| cql.contains(&cql_str(&g.node)))
                .map(|(id, g)| {
                    json!({"egroup_id": id, "state": g.state, "created_at_ms": g.created_ms,
                           "size": g.size, "seal_hash": g.seal_hash, "vdisk_hint": g.hint})
                })
                .collect());
        }
        panic!("the model was asked a statement it does not know: {cql}");
    }
}

fn s(p: &Value, k: &str) -> String {
    p[k].as_str().unwrap_or_else(|| panic!("{k} missing in {p}")).to_string()
}

fn n(p: &Value, k: &str) -> u64 {
    p[k].as_i64().unwrap_or_else(|| panic!("{k} missing in {p}")) as u64
}

impl Db for Model {
    fn cas(&self, path: &str, p: Value) -> Result<Cas> {
        let mut st = self.st.borrow_mut();
        let nth = st.cas_log.iter().filter(|x| x.as_str() == path).count();
        st.cas_log.push(path.to_string());
        if let Some(mut hook) = st.before_cas.take() {
            hook(&mut st, path, &p);
            st.before_cas = Some(hook);
        }
        if let Some((fp, fnth)) = &st.fail_cas {
            if fp == path && *fnth == nth {
                return Err(Error::meta("injected: hydra unreachable".to_string()));
            }
        }
        let refused = |current: Value| Ok(Cas { applied: false, current });
        match path {
            "/v1/dfs/egroup-create" => {
                let id = s(&p, "egroup_id");
                if st.egroups.contains_key(&id) {
                    return refused(Value::Null);
                }
                st.egroups.insert(
                    id,
                    MGroup {
                        state: s(&p, "state"),
                        node: s(&p, "node"),
                        created_ms: p["created_at_ms"].as_i64().unwrap_or(0),
                        size: n(&p, "size"),
                        seal_hash: s(&p, "seal_hash"),
                        hint: s(&p, "vdisk_hint"),
                    },
                );
                Ok(Cas { applied: true, current: Value::Null })
            }
            "/v1/dfs/block-map-repoint" => {
                let (v, idx) = (s(&p, "vdisk_id"), n(&p, "extent_index"));
                let want = (s(&p, "expected_egroup_id"), n(&p, "expected_egroup_offset") as u32,
                            n(&p, "expected_length") as u32);
                let Some(row) = st.block.iter_mut().find(|r| r.vdisk == v && r.idx == idx) else {
                    return refused(Value::Null);
                };
                if row.egroup.as_deref() != Some(want.0.as_str()) || row.offset != want.1 || row.length != want.2 {
                    return refused(json!({"egroup_id": row.egroup, "egroup_offset": row.offset}));
                }
                row.egroup = Some(s(&p, "egroup_id"));
                row.offset = n(&p, "egroup_offset") as u32;
                Ok(Cas { applied: true, current: Value::Null })
            }
            "/v1/dfs/extent-repoint" => {
                let id = s(&p, "extent_id");
                let Some(e) = st.extents.get_mut(&id) else { return refused(Value::Null) };
                if e.egroup != s(&p, "expected_egroup_id") || e.offset != n(&p, "expected_egroup_offset") as u32 {
                    return refused(json!({"egroup_id": e.egroup}));
                }
                e.egroup = s(&p, "egroup_id");
                e.offset = n(&p, "egroup_offset") as u32;
                Ok(Cas { applied: true, current: Value::Null })
            }
            other => panic!("the model has no compare-and-swap {other}"),
        }
    }
}

/// Deterministic, non-repeating-looking bytes: a seed per extent keeps two extents distinct.
pub fn data(seed: u8, len: usize) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed).wrapping_add((i >> 8) as u8)).collect()
}

pub struct Rig {
    pub dir: PathBuf,
    pub store: EgroupStore,
    pub model: Model,
    /// What a read of (vdisk, index) must return.
    pub truth: RefCell<HashMap<(String, u64), Vec<u8>>>,
    pub node: String,
}

impl Rig {
    pub fn new(name: &str) -> Rig {
        let mut dir = std::env::temp_dir();
        dir.push(format!("sidon-pass-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&dir);
        let store = EgroupStore::new(&dir.join("egroups"), 4 << 20).unwrap();
        Rig { dir, store, model: Model::new(), truth: RefCell::new(HashMap::new()), node: "n1".to_string() }
    }

    pub fn vdisk(&self, id: &str, class: &str, replicas: &[&str]) {
        self.model.st.borrow_mut().vdisks.insert(
            id.to_string(),
            MVdisk {
                class: class.to_string(),
                container: "pool".to_string(),
                replicas: replicas.iter().map(|r| r.to_string()).collect(),
            },
        );
    }

    /// A sealed group written by `writer`, holding one extent per `(index, bytes)`. Returns
    /// `(index, offset, stored_length)` for each. Nothing points at it yet.
    pub fn group(&self, id: &str, writer: &str, extents: &[(u64, Vec<u8>)]) -> Vec<(u64, u32, u32)> {
        let vh = vdisk_hash(writer);
        let mut eg = self.store.create(id).unwrap();
        let mut out = Vec::new();
        for (idx, bytes) in extents {
            let (off, len, _) = self.store.append_framed(&mut eg, bytes, vh, *idx, false).unwrap();
            out.push((*idx, off, len));
        }
        self.store.sync(&mut eg).unwrap();
        let hash = self.store.seal_hash(id).unwrap();
        let size = std::fs::metadata(self.store.path_for(id)).unwrap().len();
        self.model.st.borrow_mut().egroups.insert(
            id.to_string(),
            MGroup {
                state: "sealed".into(),
                node: self.node.clone(),
                created_ms: 0,
                size,
                seal_hash: hash,
                hint: writer.to_string(),
            },
        );
        out
    }

    /// A block-map row, with the bytes a read through it must return.
    pub fn point(&self, vdisk: &str, idx: u64, group: &str, loc: (u32, u32), writer: &str, bytes: &[u8]) {
        self.model.st.borrow_mut().block.push(MRow {
            vdisk: vdisk.into(),
            idx,
            egroup: Some(group.into()),
            offset: loc.0,
            length: loc.1,
            vhash: vdisk_hash(writer),
            extent: None,
        });
        self.truth.borrow_mut().insert((vdisk.to_string(), idx), bytes.to_vec());
    }

    /// A block-map row that reaches its extent through the extent id map.
    pub fn point_via(&self, vdisk: &str, idx: u64, extent: &str, bytes: &[u8]) {
        self.model.st.borrow_mut().block.push(MRow {
            vdisk: vdisk.into(),
            idx,
            egroup: None,
            offset: 0,
            length: 0,
            vhash: 0,
            extent: Some(extent.into()),
        });
        self.truth.borrow_mut().insert((vdisk.to_string(), idx), bytes.to_vec());
    }

    pub fn extent_row(&self, id: &str, group: &str, loc: (u32, u32), writer: &str) {
        self.model.st.borrow_mut().extents.insert(
            id.to_string(),
            MExt { egroup: group.into(), offset: loc.0, length: loc.1, vhash: vdisk_hash(writer) },
        );
    }

    /// Every row reads back, from wherever it points now, as the bytes it is supposed to.
    pub fn assert_every_row_reads_correctly(&self) {
        let st = self.model.st.borrow();
        for r in &st.block {
            let (group, off, len, vh) = match (&r.egroup, &r.extent) {
                (Some(g), _) => (g.clone(), r.offset, r.length, r.vhash),
                (None, Some(e)) => {
                    let x = &st.extents[e];
                    (x.egroup.clone(), x.offset, x.length, x.vhash)
                }
                _ => panic!("a row that names nothing"),
            };
            let got = self
                .store
                .read_extent(&group, off, len, vh, r.idx)
                .unwrap_or_else(|e| panic!("{}:{} no longer reads from {group}@{off}: {e}", r.vdisk, r.idx));
            assert_eq!(
                &got,
                &self.truth.borrow()[&(r.vdisk.clone(), r.idx)],
                "{}:{} reads the wrong bytes from {group}@{off}",
                r.vdisk,
                r.idx
            );
        }
    }

    /// The groups any row points into, directly or through an extent.
    pub fn referenced_groups(&self) -> HashSet<String> {
        let st = self.model.st.borrow();
        let mut out = HashSet::new();
        for r in &st.block {
            if let Some(g) = &r.egroup {
                out.insert(g.clone());
            }
            if let Some(e) = &r.extent {
                out.insert(st.extents[e].egroup.clone());
            }
        }
        out
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

type MemMap = Rc<RefCell<HashMap<u64, (String, u32, u32)>>>;

#[derive(Default)]
pub struct FakeEnv {
    pub peers: RefCell<HashMap<(String, String), Vec<u8>>>,
    pub refuse: RefCell<HashSet<String>>,
    pub corrupt_readback: RefCell<HashSet<String>>,
    pub attached: RefCell<HashMap<String, MemMap>>,
    pub busy: RefCell<HashSet<String>>,
    pub drain: RefCell<HashSet<String>>,
    pub puts: Cell<usize>,
    pub holds: Cell<usize>,
}

impl FakeEnv {
    /// Attach a vdisk here with an in-memory map taken from the model's rows.
    pub fn attach(&self, rig: &Rig, vdisk: &str) -> MemMap {
        let mut m = HashMap::new();
        for r in rig.model.st.borrow().block.iter().filter(|r| r.vdisk == vdisk) {
            if let Some(g) = &r.egroup {
                m.insert(r.idx, (g.clone(), r.offset, r.length));
            }
        }
        let m = Rc::new(RefCell::new(m));
        self.attached.borrow_mut().insert(vdisk.to_string(), Rc::clone(&m));
        m
    }
}

pub struct FakeHold {
    map: MemMap,
    held: Rc<Cell<usize>>,
}

impl Hold for FakeHold {
    fn has(&self, idx: u64, egroup: &str, offset: u32, length: u32) -> bool {
        self.map.borrow().get(&idx) == Some(&(egroup.to_string(), offset, length))
    }

    fn repoint(&self, idx: u64, egroup: &str, offset: u32, length: u32, to_group: &str, to_offset: u32) -> bool {
        let mut m = self.map.borrow_mut();
        if m.get(&idx) == Some(&(egroup.to_string(), offset, length)) {
            m.insert(idx, (to_group.to_string(), to_offset, length));
            true
        } else {
            false
        }
    }
}

impl Drop for FakeHold {
    fn drop(&mut self) {
        self.held.set(self.held.get().saturating_sub(1));
    }
}

impl Env for FakeEnv {
    fn put(&self, node: &str, group: &str, offset: u64, data: &[u8], _defer: bool) -> Result<()> {
        if self.refuse.borrow().contains(node) {
            return Err(Error::io(format!("{node} refused")));
        }
        self.puts.set(self.puts.get() + 1);
        let mut peers = self.peers.borrow_mut();
        let file = peers.entry((node.to_string(), group.to_string())).or_default();
        let end = offset as usize + data.len();
        if file.len() < end {
            file.resize(end, 0);
        }
        file[offset as usize..end].copy_from_slice(data);
        Ok(())
    }

    fn get(&self, node: &str, group: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
        let peers = self.peers.borrow();
        let file = peers
            .get(&(node.to_string(), group.to_string()))
            .ok_or_else(|| Error::io("no such replica group".to_string()))?;
        let mut out = file[offset as usize..(offset as usize + len).min(file.len())].to_vec();
        if self.corrupt_readback.borrow().contains(node) && !out.is_empty() {
            out[0] ^= 0xFF;
        }
        Ok(out)
    }

    fn attached_here(&self, vdisk: &str) -> bool {
        self.attached.borrow().contains_key(vdisk)
    }

    fn hold(&self, vdisk: &str) -> Result<Box<dyn Hold + '_>> {
        if self.busy.borrow().contains(vdisk) {
            return Err(Error::refused(format!("a drain of {vdisk} is running")));
        }
        let map = self
            .attached
            .borrow()
            .get(vdisk)
            .cloned()
            .ok_or_else(|| Error::refused(format!("{vdisk} is not attached")))?;
        self.holds.set(self.holds.get() + 1);
        Ok(Box::new(FakeHold { map, held: Rc::new(Cell::new(1)) }))
    }

    fn drain_groups(&self) -> HashSet<String> {
        self.drain.borrow().clone()
    }
}
