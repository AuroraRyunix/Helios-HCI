//! Reclamation across nodes (D-33), against a model of Hydra, real extent files on the owner
//! and real replica directories on the peers, with failure injected at every step.
//!
//! The properties, in the order they matter: a replica never drops a copy of a group that is
//! live in Hydra, whoever asks and however old the copy; the owner's own guards (two scans,
//! open, held, young) are exactly as they were; a stop at any step of a reclaim leaves a
//! state the next sweep finishes; and every copy that is dead is eventually dropped -- by the
//! owner's request when the replica was reachable, by the replica's own scan when it was not.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

use super::*;
use crate::purah::replica::{verdicts_from_json, verdicts_to_json, ReplicaReaper};
use crate::purah::testkit::{data, MGroup, MRow, Model, Rig};
use crate::peer::ReplicaStore;

const NOW_MS: i64 = 1_000_000_000_000;
const GRACE: Duration = Duration::from_secs(600);
const OPEN_ABANDON: Duration = Duration::from_secs(3600);
const HOUR_MS: i64 = 3_600_000;

fn later(base: Instant, secs: u64) -> Instant {
    base + Duration::from_secs(secs)
}

/// Make a replica file look as if it was last written `secs` ago.
fn written(dir: &PathBuf, id: &str, secs: u64) {
    let path = dir.join("replica-egroups").join(format!("{id}.eg"));
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_modified(SystemTime::now() - Duration::from_secs(secs)).unwrap();
}

struct Replica {
    dir: PathBuf,
    store: ReplicaStore,
    reaper: ReplicaReaper,
}

impl Replica {
    fn new(base: &PathBuf, name: &str) -> Replica {
        let dir = base.join(format!("replica-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        Replica { store: ReplicaStore::new(&dir).unwrap(), dir, reaper: ReplicaReaper::new(GRACE) }
    }

    fn put(&self, id: &str, bytes: &[u8], age_secs: u64) {
        self.store.put_egroup(id, 0, bytes).unwrap();
        written(&self.dir, id, age_secs);
    }

    fn has(&self, id: &str) -> bool {
        self.store.has_egroup(id)
    }
}

type Hook = Box<dyn FnMut(&Model, &str, &[String])>;

/// The peers of the owner under test: each a real replica directory, reading the same model of
/// Hydra the owner does, and each able to be down, to be an older build, or to be watched.
struct FakePeers<'a> {
    model: &'a Model,
    replicas: BTreeMap<String, Replica>,
    down: RefCell<HashSet<String>>,
    old: RefCell<HashSet<String>>,
    calls: RefCell<Vec<(String, Vec<String>)>>,
    on_call: RefCell<Option<Hook>>,
}

impl<'a> FakePeers<'a> {
    fn new(rig: &'a Rig, names: &[&str]) -> FakePeers<'a> {
        let mut replicas = BTreeMap::new();
        for n in names {
            replicas.insert(n.to_string(), Replica::new(&rig.dir, n));
        }
        FakePeers {
            model: &rig.model,
            replicas,
            down: RefCell::new(HashSet::new()),
            old: RefCell::new(HashSet::new()),
            calls: RefCell::new(Vec::new()),
            on_call: RefCell::new(None),
        }
    }

    fn r(&self, n: &str) -> &Replica {
        &self.replicas[n]
    }
}

impl ReplicaPeers for FakePeers<'_> {
    fn nodes(&self) -> Vec<String> {
        self.replicas.keys().cloned().collect()
    }

    fn drop_groups(&self, node: &str, from: &str, ids: &[String]) -> Result<PeerAnswer> {
        self.calls.borrow_mut().push((node.to_string(), ids.to_vec()));
        if let Some(hook) = self.on_call.borrow_mut().as_mut() {
            hook(self.model, node, ids);
        }
        if self.down.borrow().contains(node) {
            return Err(Error::io(format!("peer {node} is unreachable")));
        }
        if self.old.borrow().contains(node) {
            return Ok(PeerAnswer::Unsupported);
        }
        let rep = &self.replicas[node];
        let verdicts = rep.reaper.drop_declared_dead(self.model, &rep.store, from, ids)?;
        // Through the wire document, so the codec is exercised on every call.
        Ok(PeerAnswer::Verdicts(verdicts_from_json(&verdicts_to_json(&verdicts))?))
    }
}

struct World<'a> {
    rig: &'a Rig,
    peers: &'a FakePeers<'a>,
    access: AccessLog,
    ledger: HashMap<String, Instant>,
    base: Instant,
}

impl<'a> World<'a> {
    fn new(rig: &'a Rig, peers: &'a FakePeers<'a>) -> World<'a> {
        World { rig, peers, access: AccessLog::new(1000, 0), ledger: HashMap::new(), base: Instant::now() }
    }

    fn sweep_at(&mut self, secs: u64, held: &HashSet<String>) -> SweepReport {
        let pass = Pass {
            db: &self.rig.model,
            store: &self.rig.store,
            node: "n1",
            grace: GRACE,
            access: &self.access,
            peers: self.peers,
            open_abandon: OPEN_ABANDON,
        };
        sweep_pass(&pass, &mut self.ledger, held, NOW_MS, later(self.base, secs)).unwrap()
    }

    /// The two scans that make a group reclaimable: first sight, then past the grace.
    fn two_sweeps(&mut self) -> SweepReport {
        let first = self.sweep_at(0, &HashSet::new());
        assert!(first.reclaimed.is_empty(), "first sight must reclaim nothing: {first:?}");
        self.sweep_at(601, &HashSet::new())
    }
}

/// A sealed group on the owner, a copy of it on each named replica, and nothing pointing at it.
fn garbage(rig: &Rig, peers: &FakePeers, id: &str, on: &[&str]) {
    let bytes = data(7, 4096);
    rig.group(id, "v1", &[(0, bytes.clone())]);
    for n in on {
        peers.r(n).put(id, &bytes, 3 * 3600);
    }
}

fn state_of(rig: &Rig, id: &str) -> Option<String> {
    rig.model.st.borrow().egroups.get(id).map(|g| g.state.clone())
}

// --- The owner: the order of a reclaim ----------------------------------------------------

#[test]
fn a_reclaim_asks_each_replica_while_the_row_still_says_dead_and_only_then_deletes_it() {
    let rig = Rig::new("order");
    let peers = FakePeers::new(&rig, &["n2", "n3"]);
    garbage(&rig, &peers, "eg-a", &["n2", "n3"]);
    let seen: std::rc::Rc<RefCell<Vec<(String, Option<String>, bool)>>> = Default::default();
    {
        let seen = seen.clone();
        let dir = rig.dir.clone();
        *peers.on_call.borrow_mut() = Some(Box::new(move |model, node, _ids| {
            let row = model.st.borrow().egroups.get("eg-a").map(|g| g.state.clone());
            let local_gone = !dir.join("egroups").join("eg-a.eg").exists();
            seen.borrow_mut().push((node.to_string(), row, local_gone));
        }));
    }
    let mut w = World::new(&rig, &peers);
    let r = w.two_sweeps();

    assert_eq!(r.reclaimed, vec!["eg-a".to_string()], "{r:?}");
    let seen = seen.borrow();
    assert_eq!(seen.len(), 2, "one request per peer: {seen:?}");
    for (node, row, local_gone) in seen.iter() {
        assert_eq!(row.as_deref(), Some("dead"), "{node} was asked before the row said dead");
        assert!(local_gone, "{node} was asked before this node's own copy was removed");
    }
    assert_eq!(state_of(&rig, "eg-a"), None, "the row is deleted last");
    assert!(!peers.r("n2").has("eg-a") && !peers.r("n3").has("eg-a"));
    let drops = &r.replica_drops;
    assert_eq!(drops.len(), 2);
    assert!(drops.iter().all(|d| d.dropped == 1 && d.bytes > 0 && !d.unsupported && d.error.is_none()));
}

#[test]
fn one_request_per_peer_carries_every_group_reclaimed_in_the_pass() {
    let rig = Rig::new("batch");
    let peers = FakePeers::new(&rig, &["n2"]);
    for id in ["eg-1", "eg-2", "eg-3"] {
        garbage(&rig, &peers, id, &["n2"]);
    }
    let mut w = World::new(&rig, &peers);
    let r = w.two_sweeps();
    assert_eq!(r.reclaimed.len(), 3);
    let calls = peers.calls.borrow();
    assert_eq!(calls.len(), 1, "a replica reads the block map once per request: {calls:?}");
    assert_eq!(calls[0].1.len(), 3);
}

#[test]
fn nothing_is_asked_of_a_peer_on_first_sight_or_inside_the_grace() {
    let rig = Rig::new("first-sight");
    let peers = FakePeers::new(&rig, &["n2"]);
    garbage(&rig, &peers, "eg-a", &["n2"]);
    let mut w = World::new(&rig, &peers);

    let first = w.sweep_at(0, &HashSet::new());
    assert_eq!(first.skipped_grace, 1);
    let inside = w.sweep_at(300, &HashSet::new());
    assert_eq!(inside.skipped_grace, 1, "still inside the grace");
    assert!(peers.calls.borrow().is_empty(), "no peer is asked about a group that has not earned reclamation");
    assert!(peers.r("n2").has("eg-a"));
    assert!(rig.store.path_for("eg-a").exists());
    assert_eq!(state_of(&rig, "eg-a").as_deref(), Some("sealed"));
}

#[test]
fn a_referenced_group_is_never_reclaimed_or_asked_about() {
    let rig = Rig::new("referenced");
    let peers = FakePeers::new(&rig, &["n2"]);
    let bytes = data(1, 4096);
    let loc = rig.group("eg-live", "v1", &[(0, bytes.clone())]);
    rig.point("v1", 0, "eg-live", (loc[0].1, loc[0].2), "v1", &bytes);
    peers.r("n2").put("eg-live", &bytes, 9999);
    let mut w = World::new(&rig, &peers);
    w.two_sweeps();
    let r = w.sweep_at(5000, &HashSet::new());
    assert_eq!(r.egroups_referenced, 1);
    assert!(r.reclaimed.is_empty());
    assert!(peers.calls.borrow().is_empty());
    assert!(peers.r("n2").has("eg-live"));
    rig.assert_every_row_reads_correctly();
}

#[test]
fn held_open_and_young_groups_are_left_alone_exactly_as_before() {
    let rig = Rig::new("guards");
    let peers = FakePeers::new(&rig, &["n2"]);
    garbage(&rig, &peers, "eg-held", &["n2"]);
    garbage(&rig, &peers, "eg-open", &["n2"]);
    garbage(&rig, &peers, "eg-young", &["n2"]);
    {
        let mut st = rig.model.st.borrow_mut();
        st.egroups.get_mut("eg-open").unwrap().state = "open".into();
        st.egroups.get_mut("eg-young").unwrap().created_ms = NOW_MS - 1000;
    }
    let held: HashSet<String> = ["eg-held".to_string()].into_iter().collect();
    let mut w = World::new(&rig, &peers);
    w.sweep_at(0, &held);
    let r = w.sweep_at(601, &held);
    assert!(r.reclaimed.is_empty(), "{r:?}");
    assert_eq!(r.skipped_held, 1);
    assert_eq!(r.skipped_open, 1);
    assert_eq!(r.skipped_young, 1);
    assert!(peers.calls.borrow().is_empty());
    for id in ["eg-held", "eg-open", "eg-young"] {
        assert!(rig.store.path_for(id).exists() && peers.r("n2").has(id), "{id}");
    }
}

#[test]
fn a_restart_restarts_the_grace() {
    let rig = Rig::new("restart");
    let peers = FakePeers::new(&rig, &["n2"]);
    garbage(&rig, &peers, "eg-a", &["n2"]);
    let mut w = World::new(&rig, &peers);
    w.sweep_at(0, &HashSet::new());
    // The daemon restarts: the ledger is memory only.
    w.ledger.clear();
    let r = w.sweep_at(601, &HashSet::new());
    assert!(r.reclaimed.is_empty(), "a first sight after a restart is still a first sight");
    assert!(peers.r("n2").has("eg-a"));
}

// --- The owner: stopping at any step ------------------------------------------------------

#[test]
fn a_failed_dead_marking_touches_nothing_anywhere() {
    let rig = Rig::new("cas-fails");
    let peers = FakePeers::new(&rig, &["n2"]);
    garbage(&rig, &peers, "eg-a", &["n2"]);
    rig.model.st.borrow_mut().fail_cas = Some(("/v1/dfs/egroup-state".into(), 0));
    let mut w = World::new(&rig, &peers);
    let r = w.two_sweeps();
    assert!(r.reclaimed.is_empty());
    assert!(peers.calls.borrow().is_empty(), "a group Hydra was not told is dead is not declared dead to a replica");
    assert!(rig.store.path_for("eg-a").exists() && peers.r("n2").has("eg-a"));
    assert_eq!(state_of(&rig, "eg-a").as_deref(), Some("sealed"));
}

#[test]
fn a_state_that_changed_under_the_sweep_wins() {
    // The group was sealed when the sweep read it and is something else by the time it marks it.
    let rig = Rig::new("raced");
    let peers = FakePeers::new(&rig, &["n2"]);
    garbage(&rig, &peers, "eg-a", &["n2"]);
    let mut w = World::new(&rig, &peers);
    w.sweep_at(0, &HashSet::new());
    rig.model.st.borrow_mut().before_cas = Some(Box::new(|st, _path, _p| {
        st.egroups.get_mut("eg-a").unwrap().state = "open".into();
    }));
    let r = w.sweep_at(601, &HashSet::new());
    assert!(r.reclaimed.is_empty());
    assert!(peers.calls.borrow().is_empty());
    assert!(rig.store.path_for("eg-a").exists() && peers.r("n2").has("eg-a"));
    assert_eq!(state_of(&rig, "eg-a").as_deref(), Some("open"));
}

#[test]
fn a_crash_after_the_dead_marking_is_finished_by_the_next_sweep() {
    // Stop after step 1: the row says dead, nothing else was done. Modelled by failing the
    // first peer request outright and the row delete, then letting the next sweep run.
    let rig = Rig::new("crash-after-dead");
    let peers = FakePeers::new(&rig, &["n2"]);
    garbage(&rig, &peers, "eg-a", &["n2"]);
    peers.down.borrow_mut().insert("n2".into());
    rig.model.st.borrow_mut().fail_delete = true;
    let mut w = World::new(&rig, &peers);
    let r = w.two_sweeps();
    assert!(r.reclaimed.is_empty(), "the row could not be deleted, so it is not reclaimed yet");
    assert_eq!(state_of(&rig, "eg-a").as_deref(), Some("dead"));
    assert!(!rig.store.path_for("eg-a").exists(), "the local copy went in step 2");
    assert!(peers.r("n2").has("eg-a"));

    // Everything recovers. The dead row is a candidate again: it needs the same two scans.
    peers.down.borrow_mut().clear();
    rig.model.st.borrow_mut().fail_delete = false;
    let again = w.sweep_at(1300, &HashSet::new());
    let again2 = w.sweep_at(2000, &HashSet::new());
    let done = if again.reclaimed.is_empty() { again2 } else { again };
    assert_eq!(done.reclaimed, vec!["eg-a".to_string()], "{done:?}");
    assert_eq!(state_of(&rig, "eg-a"), None);
    assert!(!peers.r("n2").has("eg-a"), "the replica drops it now that the row is still dead when it asks");
}

#[test]
fn a_failed_row_delete_after_the_replicas_dropped_is_retried_and_idempotent() {
    let rig = Rig::new("delete-fails");
    let peers = FakePeers::new(&rig, &["n2"]);
    garbage(&rig, &peers, "eg-a", &["n2"]);
    rig.model.st.borrow_mut().fail_delete = true;
    let mut w = World::new(&rig, &peers);
    let r = w.two_sweeps();
    assert!(r.reclaimed.is_empty());
    assert!(!peers.r("n2").has("eg-a"), "step 3 happened");
    assert_eq!(state_of(&rig, "eg-a").as_deref(), Some("dead"));

    rig.model.st.borrow_mut().fail_delete = false;
    let mut done = w.sweep_at(1300, &HashSet::new());
    if done.reclaimed.is_empty() {
        done = w.sweep_at(2000, &HashSet::new());
    }
    assert_eq!(done.reclaimed, vec!["eg-a".to_string()]);
    let last = peers.calls.borrow().last().cloned().unwrap();
    assert_eq!(last.1, vec!["eg-a".to_string()]);
    assert_eq!(done.replica_drops[0].absent, 1, "asking again finds the work done");
}

// --- The owner: peers that cannot or will not -----------------------------------------------

#[test]
fn a_peer_that_is_down_costs_nothing_here_and_its_copy_is_found_by_its_own_scan() {
    let rig = Rig::new("peer-down");
    let peers = FakePeers::new(&rig, &["n2", "n3"]);
    garbage(&rig, &peers, "eg-a", &["n2", "n3"]);
    peers.down.borrow_mut().insert("n3".into());
    let mut w = World::new(&rig, &peers);
    let r = w.two_sweeps();

    assert_eq!(r.reclaimed, vec!["eg-a".to_string()], "the owner finishes whatever a peer does");
    assert_eq!(state_of(&rig, "eg-a"), None);
    assert!(!peers.r("n2").has("eg-a"));
    assert!(peers.r("n3").has("eg-a"), "the unreachable peer still holds its copy");
    let n3 = r.replica_drops.iter().find(|d| d.node == "n3").unwrap();
    assert!(n3.error.is_some());

    // n3 comes back. Its own scan finds a copy with no row: first sight, then past the grace.
    peers.down.borrow_mut().clear();
    let n3 = peers.r("n3");
    let t0 = Instant::now();
    let first = n3.reaper.scan(&rig.model, &n3.store, t0).unwrap();
    assert!(first.dropped.is_empty() && first.awaiting_grace == 1, "{first:?}");
    let second = n3.reaper.scan(&rig.model, &n3.store, later(t0, 601)).unwrap();
    assert_eq!(second.dropped, vec!["eg-a".to_string()]);
    assert!(!n3.has("eg-a"));
}

#[test]
fn an_older_replica_keeps_its_copy_and_is_reported_as_older() {
    let rig = Rig::new("old-replica");
    let peers = FakePeers::new(&rig, &["n2", "n3"]);
    garbage(&rig, &peers, "eg-a", &["n2", "n3"]);
    peers.old.borrow_mut().insert("n3".into());
    let mut w = World::new(&rig, &peers);
    let r = w.two_sweeps();
    assert_eq!(r.reclaimed.len(), 1);
    assert!(peers.r("n3").has("eg-a"), "an older replica does not drop, and loses nothing by it");
    assert!(!peers.r("n2").has("eg-a"));
    assert!(r.replica_drops.iter().find(|d| d.node == "n3").unwrap().unsupported);
    assert!(r.to_json()["replica_drops"].as_array().unwrap().len() == 2);
}

#[test]
fn a_replica_that_finds_the_block_map_pointing_into_a_dead_group_keeps_it_and_the_row_stays() {
    // The owner's two scans missed a reference (the race I-7 exists for): a clone's rows point
    // into the group by the time the replica looks. The replica holds the only copy now.
    let rig = Rig::new("late-reference");
    let peers = FakePeers::new(&rig, &["n2"]);
    garbage(&rig, &peers, "eg-a", &["n2"]);
    let mut w = World::new(&rig, &peers);
    w.sweep_at(0, &HashSet::new());
    *peers.on_call.borrow_mut() = Some(Box::new(|model, _node, _ids| {
        model.st.borrow_mut().block.push(MRow {
            vdisk: "clone".into(),
            idx: 0,
            egroup: Some("eg-a".into()),
            offset: 0,
            length: 100,
            vhash: 1,
            extent: None,
        });
    }));
    let r = w.sweep_at(601, &HashSet::new());

    assert!(peers.r("n2").has("eg-a"), "the last copy survives");
    assert!(r.reclaimed.is_empty(), "{r:?}");
    assert_eq!(state_of(&rig, "eg-a").as_deref(), Some("dead"), "the evidence is left in place");
    assert_eq!(r.anomalies.len(), 1, "{r:?}");
    assert_eq!(r.replica_drops[0].referenced, vec!["eg-a".to_string()]);
}

#[test]
fn a_group_nobody_replicated_costs_the_peers_nothing() {
    let rig = Rig::new("no-copies");
    let peers = FakePeers::new(&rig, &["n2"]);
    garbage(&rig, &peers, "eg-a", &[]);
    let mut w = World::new(&rig, &peers);
    let r = w.two_sweeps();
    assert_eq!(r.reclaimed.len(), 1);
    assert_eq!(r.replica_drops[0].absent, 1);
    // A peer with nothing to drop never reads the block map.
    let statements = rig.model.st.borrow().statements.clone();
    assert!(
        !statements.iter().any(|s| s.starts_with("SELECT state, node FROM hydra.dfs_egroups")),
        "{statements:?}"
    );
}

// --- The replica: it checks for itself ----------------------------------------------------

fn ask(rig: &Rig, rep: &Replica, from: &str, ids: &[&str]) -> Vec<(String, Outcome)> {
    let ids: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
    rep.reaper.drop_declared_dead(&rig.model, &rep.store, from, &ids).unwrap()
}

fn setup_replica(name: &str) -> (Rig, Replica) {
    let rig = Rig::new(name);
    let rep = Replica::new(&rig.dir, "n2");
    (rig, rep)
}

#[test]
fn a_copy_of_a_live_group_is_never_dropped_whoever_asks() {
    let (rig, rep) = setup_replica("live-refused");
    let bytes = data(3, 4096);
    for (id, state) in [("eg-sealed", "sealed"), ("eg-open", "open")] {
        rig.group(id, "v1", &[(0, bytes.clone())]);
        rig.model.st.borrow_mut().egroups.get_mut(id).unwrap().state = state.into();
        rep.put(id, &bytes, 99_999);
    }
    for who in ["n1", "n3", "anyone"] {
        for v in ask(&rig, &rep, who, &["eg-sealed", "eg-open"]) {
            assert!(matches!(v.1, Outcome::Refused(_)), "{who}: {v:?}");
        }
    }
    assert!(rep.has("eg-sealed") && rep.has("eg-open"));
}

#[test]
fn a_group_with_no_row_is_not_dropped_on_request() {
    let (rig, rep) = setup_replica("no-row");
    rep.put("eg-nowhere", b"bytes", 99_999);
    let v = ask(&rig, &rep, "n1", &["eg-nowhere"]);
    assert!(matches!(v[0].1, Outcome::Refused(_)), "{v:?}");
    assert!(rep.has("eg-nowhere"), "an orphan goes through the scan's two-scan rule, not a request");
}

#[test]
fn only_the_creator_may_declare_a_group_dead() {
    let (rig, rep) = setup_replica("wrong-sender");
    let bytes = data(4, 4096);
    rig.group("eg-a", "v1", &[(0, bytes.clone())]);
    rig.model.st.borrow_mut().egroups.get_mut("eg-a").unwrap().state = "dead".into();
    rep.put("eg-a", &bytes, 99_999);
    let v = ask(&rig, &rep, "n3", &["eg-a"]);
    match &v[0].1 {
        Outcome::Refused(why) => assert!(why.contains("only its creator"), "{why}"),
        other => panic!("{other:?}"),
    }
    assert!(rep.has("eg-a"));
    let v = ask(&rig, &rep, "n1", &["eg-a"]);
    assert!(matches!(v[0].1, Outcome::Dropped(4096)), "{v:?}");
    assert!(!rep.has("eg-a"));
}

#[test]
fn a_dead_group_the_map_still_points_into_is_kept_and_reported() {
    let (rig, rep) = setup_replica("dead-but-referenced");
    let bytes = data(5, 4096);
    let loc = rig.group("eg-a", "v1", &[(0, bytes.clone())]);
    rig.point("v1", 0, "eg-a", (loc[0].1, loc[0].2), "v1", &bytes);
    rig.model.st.borrow_mut().egroups.get_mut("eg-a").unwrap().state = "dead".into();
    rep.put("eg-a", &bytes, 99_999);
    let v = ask(&rig, &rep, "n1", &["eg-a"]);
    assert_eq!(v[0].1, Outcome::Referenced);
    assert!(rep.has("eg-a"));
}

#[test]
fn a_group_referenced_only_through_the_extent_map_is_kept() {
    let (rig, rep) = setup_replica("via-extent");
    let bytes = data(6, 4096);
    let loc = rig.group("eg-a", "v1", &[(0, bytes.clone())]);
    rig.extent_row("x1", "eg-a", (loc[0].1, loc[0].2), "v1");
    rig.point_via("v1", 0, "x1", &bytes);
    rig.model.st.borrow_mut().egroups.get_mut("eg-a").unwrap().state = "dead".into();
    rep.put("eg-a", &bytes, 99_999);
    let v = ask(&rig, &rep, "n1", &["eg-a"]);
    assert_eq!(v[0].1, Outcome::Referenced);
    assert!(rep.has("eg-a"));
}

#[test]
fn asking_twice_is_the_same_as_asking_once() {
    let (rig, rep) = setup_replica("idempotent");
    let bytes = data(8, 4096);
    rig.group("eg-a", "v1", &[(0, bytes.clone())]);
    rig.model.st.borrow_mut().egroups.get_mut("eg-a").unwrap().state = "dead".into();
    rep.put("eg-a", &bytes, 99_999);
    assert!(matches!(ask(&rig, &rep, "n1", &["eg-a"])[0].1, Outcome::Dropped(_)));
    assert_eq!(ask(&rig, &rep, "n1", &["eg-a"])[0].1, Outcome::Absent);
    assert_eq!(ask(&rig, &rep, "n1", &["eg-never-held"])[0].1, Outcome::Absent);
}

#[test]
fn a_name_that_is_not_a_group_id_never_reaches_the_filesystem() {
    let (rig, rep) = setup_replica("traversal");
    let outside = rig.dir.join("replica-n2").join("victim.eg");
    std::fs::write(&outside, b"not a replica group").unwrap();
    // `replica-egroups/../victim` would be this file.
    let v = ask(&rig, &rep, "n1", &["../victim", "a/b", "", ".hidden", "x..y"]);
    for (_, o) in &v {
        assert!(matches!(o, Outcome::Refused(_)), "{v:?}");
    }
    assert!(outside.exists());
}

#[test]
fn hydra_being_unreachable_drops_nothing() {
    let (rig, rep) = setup_replica("hydra-down");
    let bytes = data(9, 4096);
    rig.group("eg-a", "v1", &[(0, bytes.clone())]);
    rig.model.st.borrow_mut().egroups.get_mut("eg-a").unwrap().state = "dead".into();
    rep.put("eg-a", &bytes, 99_999);
    for statement in ["SELECT state, node FROM hydra.dfs_egroups", "FROM hydra.dfs_block_map"] {
        rig.model.st.borrow_mut().fail_read_containing = Some(statement.into());
        let ids = vec!["eg-a".to_string()];
        assert!(rep.reaper.drop_declared_dead(&rig.model, &rep.store, "n1", &ids).is_err(), "{statement}");
        assert!(rep.has("eg-a"), "{statement}: silence is not permission");
    }
}

#[test]
fn an_unreadable_block_map_drops_nothing_even_for_a_dead_row() {
    let (rig, rep) = setup_replica("map-unreadable");
    let bytes = data(9, 4096);
    rig.group("eg-a", "v1", &[(0, bytes.clone())]);
    rig.model.st.borrow_mut().egroups.get_mut("eg-a").unwrap().state = "dead".into();
    rep.put("eg-a", &bytes, 99_999);
    rig.model.st.borrow_mut().fail_read_containing = Some("dfs_block_map".into());
    let ids = vec!["eg-a".to_string()];
    assert!(rep.reaper.drop_declared_dead(&rig.model, &rep.store, "n1", &ids).is_err());
    assert!(rep.has("eg-a"));
}

// --- The replica's own scan ---------------------------------------------------------------

fn scan(rig: &Rig, rep: &Replica, base: Instant, secs: u64) -> crate::purah::replica::ScanReport {
    rep.reaper.scan(&rig.model, &rep.store, later(base, secs)).unwrap()
}

#[test]
fn an_orphan_goes_on_the_second_scan_past_the_grace_and_not_before() {
    let (rig, rep) = setup_replica("orphan");
    rep.put("eg-orphan", b"twelve bytes", 3 * 3600);
    let t0 = Instant::now();
    let a = scan(&rig, &rep, t0, 0);
    assert!(a.dropped.is_empty() && a.awaiting_grace == 1, "{a:?}");
    let b = scan(&rig, &rep, t0, 300);
    assert!(b.dropped.is_empty() && b.awaiting_grace == 1, "inside the grace: {b:?}");
    assert!(rep.has("eg-orphan"));
    let c = scan(&rig, &rep, t0, 601);
    assert_eq!(c.dropped, vec!["eg-orphan".to_string()]);
    assert_eq!(c.bytes_dropped, 12);
    assert!(!rep.has("eg-orphan"));
}

#[test]
fn a_young_copy_is_not_judged_however_many_times_it_is_seen() {
    // The compaction window: bytes replicated, the row not registered yet.
    let (rig, rep) = setup_replica("young");
    rep.put("eg-new", b"bytes", 5);
    let t0 = Instant::now();
    for secs in [0, 700, 1400] {
        let r = scan(&rig, &rep, t0, secs);
        assert!(r.dropped.is_empty() && r.young == 1, "{r:?}");
    }
    assert!(rep.has("eg-new"));
}

#[test]
fn a_copy_whose_group_is_live_in_hydra_is_kept_whatever_its_age_and_however_often_seen() {
    let (rig, rep) = setup_replica("live-scan");
    let bytes = data(1, 4096);
    for (id, state) in [("eg-sealed", "sealed"), ("eg-open", "open")] {
        rig.group(id, "v1", &[(0, bytes.clone())]);
        rig.model.st.borrow_mut().egroups.get_mut(id).unwrap().state = state.into();
        rep.put(id, &bytes, 99_999);
    }
    let t0 = Instant::now();
    for secs in [0, 700, 1400, 9000] {
        let r = scan(&rig, &rep, t0, secs);
        assert!(r.dropped.is_empty(), "{r:?}");
        assert_eq!(r.live, 2);
    }
    assert!(rep.has("eg-sealed") && rep.has("eg-open"));
}

#[test]
fn a_row_that_appears_between_the_two_scans_voids_the_first_sighting() {
    let (rig, rep) = setup_replica("registered-between");
    let bytes = data(2, 4096);
    rep.put("eg-late", &bytes, 99_999);
    let t0 = Instant::now();
    let a = scan(&rig, &rep, t0, 0);
    assert_eq!(a.awaiting_grace, 1);
    // Registered in Hydra after the first scan, as compaction does.
    rig.model.st.borrow_mut().egroups.insert(
        "eg-late".into(),
        MGroup { state: "sealed".into(), node: "n1".into(), created_ms: 0, size: 4096, seal_hash: String::new(), hint: String::new() },
    );
    let b = scan(&rig, &rep, t0, 601);
    assert!(b.dropped.is_empty() && b.live == 1, "{b:?}");
    // And if it ever becomes an orphan again it starts over.
    rig.model.st.borrow_mut().egroups.remove("eg-late");
    let c = scan(&rig, &rep, t0, 700);
    assert!(c.dropped.is_empty() && c.awaiting_grace == 1, "{c:?}");
    assert!(rep.has("eg-late"));
}

#[test]
fn a_referenced_copy_with_no_live_row_is_reported_and_kept() {
    let (rig, rep) = setup_replica("scan-referenced");
    let bytes = data(3, 4096);
    rig.model.st.borrow_mut().block.push(MRow {
        vdisk: "v1".into(), idx: 0, egroup: Some("eg-mystery".into()), offset: 0, length: 10, vhash: 1, extent: None,
    });
    rep.put("eg-mystery", &bytes, 99_999);
    let t0 = Instant::now();
    for secs in [0, 601, 1500] {
        let r = scan(&rig, &rep, t0, secs);
        assert!(r.dropped.is_empty(), "{r:?}");
        assert_eq!(r.anomalies.len(), 1, "{r:?}");
    }
    assert!(rep.has("eg-mystery"), "the last copy of a referenced group is never deleted");
}

#[test]
fn a_dead_row_counts_as_an_orphan_and_goes_under_the_same_rule() {
    let (rig, rep) = setup_replica("scan-dead-row");
    let bytes = data(4, 4096);
    rig.group("eg-dead", "v1", &[(0, bytes.clone())]);
    rig.model.st.borrow_mut().egroups.get_mut("eg-dead").unwrap().state = "dead".into();
    rep.put("eg-dead", &bytes, 99_999);
    let t0 = Instant::now();
    assert!(scan(&rig, &rep, t0, 0).dropped.is_empty());
    assert_eq!(scan(&rig, &rep, t0, 601).dropped, vec!["eg-dead".to_string()]);
}

#[test]
fn an_unreadable_hydra_drops_nothing_and_does_not_advance_the_clock() {
    let (rig, rep) = setup_replica("scan-hydra-down");
    rep.put("eg-orphan", b"bytes", 99_999);
    let t0 = Instant::now();
    assert_eq!(scan(&rig, &rep, t0, 0).awaiting_grace, 1);
    for needle in ["dfs_block_map", "SELECT egroup_id, state FROM hydra.dfs_egroups", "schema_migrations"] {
        rig.model.st.borrow_mut().fail_read_containing = Some(needle.into());
        assert!(rep.reaper.scan(&rig.model, &rep.store, later(t0, 601)).is_err(), "{needle}");
        assert!(rep.has("eg-orphan"), "{needle}");
    }
    // Hydra back: the first sighting at t0 still counts, because a failed scan changed nothing.
    rig.model.st.borrow_mut().fail_read_containing = None;
    assert_eq!(scan(&rig, &rep, t0, 601).dropped, vec!["eg-orphan".to_string()]);
}

#[test]
fn an_empty_replica_directory_reads_nothing_from_hydra() {
    let (rig, rep) = setup_replica("scan-empty");
    let r = scan(&rig, &rep, Instant::now(), 0);
    assert_eq!(r.scanned, 0);
    assert!(rig.model.st.borrow().statements.is_empty());
}

#[test]
fn files_that_are_not_groups_are_never_offered_for_removal() {
    let (rig, rep) = setup_replica("scan-strangers");
    let dir = rep.dir.join("replica-egroups");
    std::fs::write(dir.join("notes.txt"), b"x").unwrap();
    std::fs::write(dir.join("eg-a.eg.tmp"), b"x").unwrap();
    std::fs::create_dir(dir.join("subdir.eg")).unwrap();
    rep.put("eg-real", b"bytes", 99_999);
    let t0 = Instant::now();
    scan(&rig, &rep, t0, 0);
    let r = scan(&rig, &rep, t0, 601);
    assert_eq!(r.dropped, vec!["eg-real".to_string()]);
    assert!(dir.join("notes.txt").exists() && dir.join("eg-a.eg.tmp").exists() && dir.join("subdir.eg").exists());
}

/// A deterministic generator: the crate has no `rand`, and a property test that cannot be
/// replayed from its seed is one whose failure nobody can investigate.
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

/// Whatever Hydra and the block map do between scans, the scan never removes a copy it has not
/// seen orphaned -- no live row, no reference -- on two scans a grace apart, and never one
/// that is young. Random states, random mutations, many seeds.
#[test]
fn random_histories_never_drop_a_copy_that_was_live_at_either_sighting() {
    for seed in 1..=40u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let (rig, rep) = setup_replica(&format!("random-{seed}"));
        let ids: Vec<String> = (0..16).map(|i| format!("eg-{seed}-{i}")).collect();
        let mut young: HashSet<String> = HashSet::new();
        for id in &ids {
            let is_young = rng.below(4) == 0;
            rep.put(id, &data(1, 64), if is_young { 5 } else { 99_999 });
            if is_young {
                young.insert(id.clone());
            }
        }
        let t0 = Instant::now();
        // What each scan saw as orphaned, to judge the next one.
        let mut since: HashMap<String, u64> = HashMap::new();
        let mut clock = 0u64;
        for _ in 0..8 {
            // Mutate the world.
            for id in &ids {
                match rng.below(6) {
                    0 => {
                        rig.model.st.borrow_mut().egroups.insert(
                            id.clone(),
                            MGroup { state: ["sealed", "open", "dead"][rng.below(3) as usize].into(), node: "n1".into(), created_ms: 0, size: 64, seal_hash: String::new(), hint: String::new() },
                        );
                    }
                    1 => {
                        rig.model.st.borrow_mut().egroups.remove(id);
                    }
                    2 => rig.model.st.borrow_mut().block.push(MRow {
                        vdisk: "v".into(), idx: rng.below(1000), egroup: Some(id.clone()), offset: 0, length: 1, vhash: 1, extent: None,
                    }),
                    3 => rig.model.st.borrow_mut().block.retain(|r| r.egroup.as_deref() != Some(id.as_str())),
                    _ => {}
                }
            }
            clock += [10, 300, 601, 900][rng.below(4) as usize];
            let (state, referenced) = {
                let st = rig.model.st.borrow();
                let state: HashMap<String, String> = st.egroups.iter().map(|(k, g)| (k.clone(), g.state.clone())).collect();
                let referenced: HashSet<String> = st.block.iter().filter_map(|r| r.egroup.clone()).collect();
                (state, referenced)
            };
            let orphaned_now: HashSet<String> = ids
                .iter()
                .filter(|id| !referenced.contains(*id) && state.get(*id).map(|s| s == "dead").unwrap_or(true))
                .cloned()
                .collect();
            let before: HashSet<String> = ids.iter().filter(|id| rep.has(id)).cloned().collect();
            let r = scan(&rig, &rep, t0, clock);
            let dropped: HashSet<String> = r.dropped.iter().cloned().collect();
            for id in &dropped {
                assert!(before.contains(id));
                assert!(orphaned_now.contains(id), "seed {seed}: {id} dropped but it was live or referenced: row {:?}", state.get(id));
                assert!(!young.contains(id), "seed {seed}: {id} was young");
                // First seen orphaned at least a grace ago, and orphaned at every scan since.
                let first = since.get(id).unwrap_or_else(|| panic!("seed {seed}: {id} dropped on first sight"));
                assert!(clock - first >= 600, "seed {seed}: {id} dropped {}s after first sight", clock - first);
            }
            since.retain(|id, _| orphaned_now.contains(id));
            for id in &orphaned_now {
                since.entry(id.clone()).or_insert(clock);
            }
        }
    }
}

// --- Abandoned open groups ------------------------------------------------------------------

/// An `open` group the owner has: unreferenced, created `age_ms` ago, with a copy on each peer.
fn open_group(rig: &Rig, peers: &FakePeers, id: &str, age_ms: i64, on: &[&str]) {
    garbage(rig, peers, id, on);
    let mut st = rig.model.st.borrow_mut();
    let g = st.egroups.get_mut(id).unwrap();
    g.state = "open".into();
    g.created_ms = NOW_MS - age_ms;
}

#[test]
fn an_open_group_nothing_holds_is_reclaimed_after_two_scans_and_its_replicas_with_it() {
    let rig = Rig::new("open-abandoned");
    let peers = FakePeers::new(&rig, &["n2", "n3"]);
    open_group(&rig, &peers, "eg-orphaned-open", 2 * HOUR_MS, &["n2", "n3"]);
    let mut w = World::new(&rig, &peers);

    let first = w.sweep_at(0, &HashSet::new());
    assert!(first.reclaimed.is_empty(), "first sight reclaims nothing: {first:?}");
    assert_eq!(first.skipped_grace, 1);
    let second = w.sweep_at(601, &HashSet::new());

    assert_eq!(second.reclaimed, vec!["eg-orphaned-open".to_string()], "{second:?}");
    assert_eq!(second.reclaimed_abandoned_open, vec!["eg-orphaned-open".to_string()]);
    assert!(!rig.store.path_for("eg-orphaned-open").exists());
    assert_eq!(state_of(&rig, "eg-orphaned-open"), None);
    assert!(!peers.r("n2").has("eg-orphaned-open") && !peers.r("n3").has("eg-orphaned-open"));
    assert_eq!(second.to_json()["reclaimed_abandoned_open"].as_array().unwrap().len(), 1);
}

#[test]
fn an_open_group_younger_than_the_bound_is_never_reclaimed_however_many_scans_see_it() {
    let rig = Rig::new("open-young");
    let peers = FakePeers::new(&rig, &["n2"]);
    // Past the grace, short of the bound: a vdisk that has been writing for half an hour.
    open_group(&rig, &peers, "eg-open", HOUR_MS / 2, &["n2"]);
    let mut w = World::new(&rig, &peers);
    for secs in [0, 601, 2000, 4000] {
        let r = w.sweep_at(secs, &HashSet::new());
        assert!(r.reclaimed.is_empty(), "{secs}: {r:?}");
        assert_eq!(r.skipped_open, 1, "{secs}");
    }
    assert!(rig.store.path_for("eg-open").exists() && peers.r("n2").has("eg-open"));
    assert_eq!(state_of(&rig, "eg-open").as_deref(), Some("open"));
}

#[test]
fn an_open_group_a_running_drain_holds_is_never_reclaimed_whatever_its_age() {
    // The drain's group is in `held` from before the first byte until it is sealed or the vdisk
    // is gone: `Vdisk::held_egroups` names the open group and every group a running drain made.
    let rig = Rig::new("open-held");
    let peers = FakePeers::new(&rig, &["n2"]);
    open_group(&rig, &peers, "eg-draining", 48 * HOUR_MS, &["n2"]);
    let held: HashSet<String> = ["eg-draining".to_string()].into_iter().collect();
    let mut w = World::new(&rig, &peers);
    for secs in [0, 601, 5000, 90_000] {
        let r = w.sweep_at(secs, &held);
        assert!(r.reclaimed.is_empty(), "{secs}: {r:?}");
        assert_eq!(r.skipped_held, 1);
    }
    assert!(rig.store.path_for("eg-draining").exists() && peers.r("n2").has("eg-draining"));
    assert!(peers.calls.borrow().is_empty());
}

#[test]
fn a_group_that_becomes_held_between_the_scans_starts_over() {
    // Seen unheld on the first scan; a vdisk is then attached and holds it. The second scan sees it
    // held and must not reclaim, and the first sighting no longer counts afterwards.
    let rig = Rig::new("open-held-between");
    let peers = FakePeers::new(&rig, &["n2"]);
    open_group(&rig, &peers, "eg-open", 3 * HOUR_MS, &["n2"]);
    let mut w = World::new(&rig, &peers);
    w.sweep_at(0, &HashSet::new());
    let held: HashSet<String> = ["eg-open".to_string()].into_iter().collect();
    let held_scan = w.sweep_at(601, &held);
    assert!(held_scan.reclaimed.is_empty() && held_scan.skipped_held == 1, "{held_scan:?}");
    // Released again: a fresh first sighting, so nothing yet.
    let after = w.sweep_at(700, &HashSet::new());
    assert!(after.reclaimed.is_empty(), "{after:?}");
    assert!(rig.store.path_for("eg-open").exists());
}

#[test]
fn an_open_group_the_map_points_into_is_never_reclaimed() {
    // A drain committed rows into it and the vdisk went away: referenced, so live.
    let rig = Rig::new("open-referenced");
    let peers = FakePeers::new(&rig, &["n2"]);
    let bytes = data(2, 4096);
    let loc = rig.group("eg-open-live", "v1", &[(0, bytes.clone())]);
    rig.point("v1", 0, "eg-open-live", (loc[0].1, loc[0].2), "v1", &bytes);
    {
        let mut st = rig.model.st.borrow_mut();
        let g = st.egroups.get_mut("eg-open-live").unwrap();
        g.state = "open".into();
        g.created_ms = NOW_MS - 48 * HOUR_MS;
    }
    peers.r("n2").put("eg-open-live", &bytes, 99_999);
    let mut w = World::new(&rig, &peers);
    for secs in [0, 601, 5000] {
        let r = w.sweep_at(secs, &HashSet::new());
        assert!(r.reclaimed.is_empty(), "{r:?}");
        assert_eq!(r.egroups_referenced, 1);
    }
    rig.assert_every_row_reads_correctly();
    assert!(peers.r("n2").has("eg-open-live"));
}

#[test]
fn a_group_sealed_by_a_drain_while_the_sweep_runs_is_not_taken() {
    // The only race the guards leave: between the inventory and the compare-and-swap a drain seals
    // the group. The swap is conditional on the state the sweep saw, so it is refused.
    let rig = Rig::new("open-sealed-under-us");
    let peers = FakePeers::new(&rig, &["n2"]);
    open_group(&rig, &peers, "eg-open", 3 * HOUR_MS, &["n2"]);
    let mut w = World::new(&rig, &peers);
    w.sweep_at(0, &HashSet::new());
    rig.model.st.borrow_mut().before_cas = Some(Box::new(|st, _p, _x| {
        st.egroups.get_mut("eg-open").unwrap().state = "sealed".into();
    }));
    let r = w.sweep_at(601, &HashSet::new());
    assert!(r.reclaimed.is_empty(), "{r:?}");
    assert!(rig.store.path_for("eg-open").exists() && peers.r("n2").has("eg-open"));
    assert!(peers.calls.borrow().is_empty());
    assert_eq!(state_of(&rig, "eg-open").as_deref(), Some("sealed"));
}

#[test]
fn an_open_group_of_unknown_age_is_never_called_abandoned() {
    let rig = Rig::new("open-unknown-age");
    let peers = FakePeers::new(&rig, &["n2"]);
    open_group(&rig, &peers, "eg-open", 0, &["n2"]);
    rig.model.st.borrow_mut().egroups.get_mut("eg-open").unwrap().created_ms = 0;
    let mut w = World::new(&rig, &peers);
    for secs in [0, 601, 5000] {
        let r = w.sweep_at(secs, &HashSet::new());
        assert!(r.reclaimed.is_empty() && r.skipped_open == 1, "{r:?}");
    }
    assert!(rig.store.path_for("eg-open").exists());
}

#[test]
fn the_age_test_is_a_bound_and_not_a_guess() {
    let h = Duration::from_secs(3600);
    assert!(!abandoned(0, NOW_MS, h));
    assert!(!abandoned(NOW_MS - HOUR_MS + 1, NOW_MS, h));
    assert!(abandoned(NOW_MS - HOUR_MS, NOW_MS, h));
    // A clock that went backwards makes a group look younger, never older.
    assert!(!abandoned(NOW_MS + HOUR_MS, NOW_MS, h));
    assert!(!abandoned(i64::MAX, i64::MIN, h));
}

/// Whatever mix of open and sealed groups, held sets and references the passes see, a group is
/// reclaimed only if at that pass it was unreferenced, not held, and (when open) old enough --
/// and had been so at an earlier pass at least a grace before. In particular a group in `held`
/// at the pass that would reclaim it is never reclaimed: that is the running drain.
#[test]
fn random_histories_never_reclaim_what_a_drain_holds_or_the_map_points_at() {
    // The property is only worth anything if the histories reach the interesting cases.
    let (mut reclaimed_total, mut reclaimed_open) = (0usize, 0usize);
    for seed in 1..=40u64 {
        let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
        let rig = Rig::new(&format!("open-random-{seed}"));
        let peers = FakePeers::new(&rig, &["n2"]);
        let ids: Vec<String> = (0..12).map(|i| format!("eg-r{seed}-{i}")).collect();
        for id in &ids {
            garbage(&rig, &peers, id, &["n2"]);
            let mut st = rig.model.st.borrow_mut();
            let g = st.egroups.get_mut(id).unwrap();
            g.state = ["open", "sealed"][rng.below(2) as usize].into();
            g.created_ms = NOW_MS - [HOUR_MS / 2, 2 * HOUR_MS, 30 * HOUR_MS][rng.below(3) as usize];
        }
        let mut w = World::new(&rig, &peers);
        let mut since: HashMap<String, u64> = HashMap::new();
        let mut clock = 0u64;
        for _ in 0..8 {
            for id in &ids {
                match rng.below(8) {
                    0 => rig.model.st.borrow_mut().block.push(MRow {
                        vdisk: "v".into(), idx: rng.below(1000), egroup: Some(id.clone()), offset: 0, length: 1, vhash: 1, extent: None,
                    }),
                    1 => rig.model.st.borrow_mut().block.retain(|r| r.egroup.as_deref() != Some(id.as_str())),
                    _ => {}
                }
            }
            let held: HashSet<String> = ids.iter().filter(|_| rng.below(4) == 0).cloned().collect();
            clock += [10, 300, 601, 900][rng.below(4) as usize];
            let referenced: HashSet<String> =
                rig.model.st.borrow().block.iter().filter_map(|r| r.egroup.clone()).collect();
            let states: HashMap<String, (String, i64)> = rig
                .model
                .st
                .borrow()
                .egroups
                .iter()
                .map(|(k, g)| (k.clone(), (g.state.clone(), g.created_ms)))
                .collect();
            let r = w.sweep_at(clock, &held);
            reclaimed_total += r.reclaimed.len();
            reclaimed_open += r.reclaimed_abandoned_open.len();
            for id in &r.reclaimed {
                assert!(!held.contains(id), "seed {seed}: {id} was held by a drain and was reclaimed");
                assert!(!referenced.contains(id), "seed {seed}: {id} was referenced and was reclaimed");
                let (state, created) = &states[id];
                if state == "open" {
                    assert!(abandoned(*created, NOW_MS, OPEN_ABANDON), "seed {seed}: {id} open and too young");
                }
                let first = since.get(id).unwrap_or_else(|| panic!("seed {seed}: {id} reclaimed on first sight"));
                assert!(clock - first >= 600, "seed {seed}: {id} reclaimed {}s after first sight", clock - first);
            }
            // The oracle of "seen reclaimable": unreferenced, not held, and old enough if open.
            let reclaimable_now: HashSet<String> = ids
                .iter()
                .filter(|id| {
                    // Already reclaimed in an earlier pass: gone, so nothing to be seen about.
                    let Some((state, created)) = states.get(*id) else { return false };
                    !referenced.contains(*id)
                        && !held.contains(*id)
                        && (state != "open" || abandoned(*created, NOW_MS, OPEN_ABANDON))
                })
                .cloned()
                .collect();
            since.retain(|id, _| reclaimable_now.contains(id));
            for id in &reclaimable_now {
                since.entry(id.clone()).or_insert(clock);
            }
        }
    }
    assert!(reclaimed_total > 20, "the histories reclaimed only {reclaimed_total} group(s)");
    assert!(reclaimed_open > 5, "the histories reclaimed only {reclaimed_open} open group(s)");
}
