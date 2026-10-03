//! Compaction against a model of Hydra and real extent files.
//!
//! The properties, in the order they matter: nothing a guest can read changes whatever stops
//! the pass and wherever; a pass that cannot be sure leaves the map alone; a second run does
//! nothing the first did not leave undone; and an overwrite that lands mid-pass wins.

use std::collections::HashSet;
use std::time::Duration;

use super::*;
use crate::purah::testkit::{data, FakeEnv, Rig};
use crate::replicate::throttle::tests::FakeClock;

const NOW: i64 = 1_000_000_000;
const LEN: usize = 16 * 1024;

fn opts() -> Options {
    Options { apply: true, rate: 0, ..Options::default() }
}

/// A threshold above the half-live state a partly finished pass leaves behind, so a rerun
/// has something to finish.
fn t6() -> Options {
    Options { threshold: 0.6, ..opts() }
}

fn go(rig: &Rig, env: &FakeEnv, o: &Options) -> Value {
    run(&rig.model, &rig.store, "n1", Duration::ZERO, env, o, &NoProbe, &FakeClock::new(), &|_| None, NOW)
        .unwrap()
}

struct Crash(Step);
impl Probe for Crash {
    fn at(&self, s: Step) -> Result<()> {
        if s == self.0 { Err(Error::io("injected crash".to_string())) } else { Ok(()) }
    }
}

fn files(rig: &Rig) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(rig.dir.join("egroups"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    v.sort();
    v
}

/// `eg-a` holds four extents of `v1`, of which only index 0 is still the live one: the guest
/// has since rewritten 1..3, and those rewrites live in the full group `eg-b`.
fn sparse(rig: &Rig, vdisk: &str) -> (Vec<(u64, u32, u32)>, Vec<Vec<u8>>) {
    let old: Vec<Vec<u8>> = (0..4).map(|i| data(10 + i as u8, LEN)).collect();
    let new: Vec<Vec<u8>> = (0..4).map(|i| data(50 + i as u8, LEN)).collect();
    let a = rig.group("eg-a", vdisk, &(0..4).map(|i| (i as u64, old[i].clone())).collect::<Vec<_>>());
    let b = rig.group("eg-b", vdisk, &(1..4).map(|i| (i as u64, new[i].clone())).collect::<Vec<_>>());
    rig.point(vdisk, 0, "eg-a", (a[0].1, a[0].2), vdisk, &old[0]);
    for i in 1..4usize {
        rig.point(vdisk, i as u64, "eg-b", (b[i - 1].1, b[i - 1].2), vdisk, &new[i]);
    }
    (a, old)
}

fn rows_of(rig: &Rig) -> Vec<(String, u64, Option<String>, u32)> {
    rig.model.st.borrow().block.iter().map(|r| (r.vdisk.clone(), r.idx, r.egroup.clone(), r.offset)).collect()
}

// --- The ordinary case -------------------------------------------------------------------

#[test]
fn a_sparse_group_is_rewritten_and_every_row_still_reads_the_same_bytes() {
    let rig = Rig::new("basic");
    rig.vdisk("v1", "rw", &["n1", "n2"]);
    let (_a, _) = sparse(&rig, "v1");
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    let r = go(&rig, &env, &opts());

    assert_eq!(r["candidate_count"], 1, "{r}");
    assert_eq!(r["executed"].as_array().unwrap().len(), 1, "{r}");
    assert_eq!(r["rows_repointed"], 1);
    rig.assert_every_row_reads_correctly();

    let st = rig.model.st.borrow();
    let moved = st.block.iter().find(|x| x.idx == 0).unwrap();
    let new_id = moved.egroup.clone().unwrap();
    assert!(new_id.starts_with("eg-compact-"), "{new_id}");
    // Registered sealed, with the hash of the file that was published.
    let g = &st.egroups[&new_id];
    assert_eq!(g.state, "sealed");
    assert_eq!(g.seal_hash, rig.store.seal_hash(&new_id).unwrap());
    // On the replica, byte for byte.
    let on_peer = env.peers.borrow()[&("n2".to_string(), new_id.clone())].clone();
    assert_eq!(on_peer, std::fs::read(rig.store.path_for(&new_id)).unwrap());
    // And the old group is still there, for the sweep.
    assert!(rig.store.path_for("eg-a").exists());
    assert!(!rig.referenced_groups().contains("eg-a"));
}

#[test]
fn nothing_is_deleted_by_the_pass() {
    let rig = Rig::new("nodelete");
    rig.vdisk("v1", "rw", &["n1"]);
    sparse(&rig, "v1");
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    let before = files(&rig);
    go(&rig, &env, &opts());
    let after = files(&rig);
    for f in &before {
        assert!(after.contains(f), "{f} was removed");
    }
    assert!(!rig.model.st.borrow().egroups["eg-a"].state.eq("dead"));
}

#[test]
fn a_plan_changes_nothing_anywhere() {
    let rig = Rig::new("plan");
    rig.vdisk("v1", "rw", &["n1", "n2"]);
    sparse(&rig, "v1");
    let env = FakeEnv::default();
    let mem = env.attach(&rig, "v1");
    let mem_before = mem.borrow().clone();
    let (rows, fs) = (rows_of(&rig), files(&rig));
    let r = go(&rig, &env, &Options { apply: false, ..opts() });
    assert_eq!(r["candidate_count"], 1);
    assert_eq!(r["selected_groups"], 1);
    assert!(r["status"].as_str().unwrap().contains("nothing was changed"), "{r}");
    assert_eq!(rows_of(&rig), rows);
    assert_eq!(files(&rig), fs);
    assert!(rig.model.st.borrow().cas_log.is_empty(), "a plan issued a statement that writes");
    assert_eq!(env.puts.get(), 0);
    assert_eq!(env.holds.get(), 0);
    assert_eq!(*mem.borrow(), mem_before);
}

#[test]
fn a_second_run_has_nothing_to_do() {
    let rig = Rig::new("idempotent");
    rig.vdisk("v1", "rw", &["n1"]);
    sparse(&rig, "v1");
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    go(&rig, &env, &opts());
    let calls = rig.model.st.borrow().cas_log.len();
    let again = go(&rig, &env, &opts());
    assert_eq!(again["candidate_count"], 0, "{again}");
    assert_eq!(rig.model.st.borrow().cas_log.len(), calls);
    rig.assert_every_row_reads_correctly();
}

#[test]
fn full_groups_and_dead_groups_are_left_alone() {
    let rig = Rig::new("healthy");
    rig.vdisk("v1", "rw", &["n1"]);
    let d: Vec<Vec<u8>> = (0..4).map(|i| data(i, LEN)).collect();
    let full = rig.group("eg-full", "v1", &(0..4).map(|i| (i as u64, d[i].clone())).collect::<Vec<_>>());
    for (i, (_, o, l)) in full.iter().enumerate() {
        rig.point("v1", i as u64, "eg-full", (*o, *l), "v1", &d[i]);
    }
    rig.group("eg-dead", "v1", &[(9, data(9, LEN))]);
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    let r = go(&rig, &env, &opts());
    assert_eq!(r["candidate_count"], 0);
    assert_eq!(r["healthy_groups"], 1);
    assert_eq!(r["wholly_dead_groups"], 1, "a group nothing points at is the sweep's");
    assert!(rig.model.st.borrow().cas_log.is_empty());
}

// --- Shared extents ----------------------------------------------------------------------

#[test]
fn a_shared_extent_moves_for_every_vdisk_that_points_at_it() {
    let rig = Rig::new("shared");
    rig.vdisk("v1", "rw", &["n1"]);
    rig.vdisk("snap", "immutable", &["n1"]);
    rig.vdisk("img-clone", "immutable", &["n1"]);
    let (a, old) = sparse(&rig, "v1");
    for who in ["snap", "img-clone"] {
        rig.point(who, 0, "eg-a", (a[0].1, a[0].2), "v1", &old[0]);
    }
    let env = FakeEnv::default();
    let mem = env.attach(&rig, "v1");
    let r = go(&rig, &env, &opts());
    assert_eq!(r["rows_repointed"], 3, "{r}");
    let st = rig.model.st.borrow();
    let targets: HashSet<_> = st.block.iter().filter(|x| x.idx == 0).map(|x| x.egroup.clone().unwrap()).collect();
    assert_eq!(targets.len(), 1, "the three rows do not agree where the extent is");
    assert!(targets.iter().next().unwrap().starts_with("eg-compact-"));
    // The attached vdisk's own memory followed, so a read does not go back to the old group.
    assert_eq!(mem.borrow()[&0].0, *targets.iter().next().unwrap());
    drop(st);
    rig.assert_every_row_reads_correctly();
    // One copy of the extent in the new group, not three.
    let new_id = rig.model.st.borrow().block[0].egroup.clone().unwrap();
    assert_eq!(std::fs::metadata(rig.store.path_for(&new_id)).unwrap().len(), (LEN + 32) as u64);
}

#[test]
fn a_writable_vdisk_that_is_not_attached_here_keeps_the_whole_group_where_it_is() {
    let rig = Rig::new("detached");
    rig.vdisk("v1", "rw", &["n1"]);
    rig.vdisk("clone-elsewhere", "rw", &["n1"]);
    let (a, old) = sparse(&rig, "v1");
    rig.point("clone-elsewhere", 0, "eg-a", (a[0].1, a[0].2), "v1", &old[0]);
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    let rows = rows_of(&rig);
    let r = go(&rig, &env, &opts());
    assert_eq!(r["candidate_count"], 0);
    assert_eq!(r["skipped_by_kind"]["writable-vdisk-not-attached-here"], 1, "{r}");
    assert_eq!(rows_of(&rig), rows);
    assert!(rig.model.st.borrow().cas_log.is_empty());
}

#[test]
fn a_vdisk_that_is_still_being_formed_or_rolled_back_blocks_the_group() {
    for class in ["forming", "rolling_back"] {
        let rig = Rig::new(&format!("class-{class}"));
        rig.vdisk("v1", "rw", &["n1"]);
        rig.vdisk("child", class, &["n1"]);
        let (a, old) = sparse(&rig, "v1");
        rig.point("child", 0, "eg-a", (a[0].1, a[0].2), "v1", &old[0]);
        let env = FakeEnv::default();
        env.attach(&rig, "v1");
        let r = go(&rig, &env, &opts());
        assert_eq!(r["skipped_by_kind"]["vdisk-being-formed"], 1, "{class}: {r}");
        assert!(rig.model.st.borrow().cas_log.is_empty());
    }
}

#[test]
fn rows_naming_an_extent_move_with_one_statement_and_every_referrer_follows() {
    let rig = Rig::new("via");
    rig.vdisk("v1", "rw", &["n1"]);
    rig.vdisk("img", "immutable", &["n1"]);
    let old: Vec<Vec<u8>> = (0..4).map(|i| data(70 + i as u8, LEN)).collect();
    let a = rig.group("eg-a", "v1", &(0..4).map(|i| (i as u64, old[i].clone())).collect::<Vec<_>>());
    rig.extent_row("ex-1", "eg-a", (a[0].1, a[0].2), "v1");
    rig.point_via("v1", 0, "ex-1", &old[0]);
    rig.point_via("img", 0, "ex-1", &old[0]);
    let env = FakeEnv::default();
    let mem = env.attach(&rig, "v1");
    mem.borrow_mut().insert(0, ("eg-a".to_string(), a[0].1, a[0].2));
    let r = go(&rig, &env, &opts());
    assert_eq!(r["rows_repointed"], 1, "one extent row, not one per referrer: {r}");
    let st = rig.model.st.borrow();
    assert_eq!(st.cas_log.iter().filter(|p| p.as_str() == "/v1/dfs/extent-repoint").count(), 1);
    assert_eq!(st.cas_log.iter().filter(|p| p.as_str() == "/v1/dfs/block-map-repoint").count(), 0);
    let new_id = st.extents["ex-1"].egroup.clone();
    assert!(new_id.starts_with("eg-compact-"));
    assert_eq!(mem.borrow()[&0].0, new_id);
    drop(st);
    rig.assert_every_row_reads_correctly();
}

// --- Guards ------------------------------------------------------------------------------

#[test]
fn a_group_a_drain_is_still_writing_is_not_touched() {
    let rig = Rig::new("drain");
    rig.vdisk("v1", "rw", &["n1"]);
    sparse(&rig, "v1");
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    env.drain.borrow_mut().insert("eg-a".to_string());
    let r = go(&rig, &env, &opts());
    assert_eq!(r["skipped_by_kind"]["drain-in-flight"], 1, "{r}");
    assert!(rig.model.st.borrow().cas_log.is_empty());
}

#[test]
fn a_group_younger_than_the_grace_period_is_not_touched() {
    let rig = Rig::new("young");
    rig.vdisk("v1", "rw", &["n1"]);
    sparse(&rig, "v1");
    rig.model.st.borrow_mut().egroups.get_mut("eg-a").unwrap().created_ms = NOW - 1000;
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    let r = run(&rig.model, &rig.store, "n1", Duration::from_secs(600), &env, &opts(), &NoProbe,
                &FakeClock::new(), &|_| None, NOW)
        .unwrap();
    assert_eq!(r["skipped_by_kind"]["too-young"], 1, "{r}");
    assert!(rig.model.st.borrow().cas_log.is_empty());
}

#[test]
fn an_open_group_is_never_a_candidate() {
    let rig = Rig::new("open");
    rig.vdisk("v1", "rw", &["n1"]);
    sparse(&rig, "v1");
    rig.model.st.borrow_mut().egroups.get_mut("eg-a").unwrap().state = "open".into();
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    let r = go(&rig, &env, &opts());
    assert_eq!(r["candidate_count"], 0);
    assert!(rig.model.st.borrow().cas_log.is_empty());
}

#[test]
fn a_vdisk_whose_drain_is_running_is_left_alone_before_any_row_moves() {
    let rig = Rig::new("busy");
    rig.vdisk("v1", "rw", &["n1"]);
    sparse(&rig, "v1");
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    env.busy.borrow_mut().insert("v1".to_string());
    let rows = rows_of(&rig);
    let r = go(&rig, &env, &opts());
    assert_eq!(r["failed"].as_array().unwrap().len(), 1, "{r}");
    assert_eq!(rows_of(&rig), rows);
    let st = rig.model.st.borrow();
    assert!(!st.cas_log.iter().any(|p| p.contains("repoint")), "{:?}", st.cas_log);
}

#[test]
fn a_vdisk_whose_memory_has_moved_on_since_the_scan_is_left_alone() {
    let rig = Rig::new("stale-mem");
    rig.vdisk("v1", "rw", &["n1"]);
    sparse(&rig, "v1");
    let env = FakeEnv::default();
    let mem = env.attach(&rig, "v1");
    // A drain finished after the scan: memory already names a different place for extent 0.
    mem.borrow_mut().insert(0, ("eg-b".to_string(), 0, 1));
    let rows = rows_of(&rig);
    let r = go(&rig, &env, &opts());
    assert_eq!(r["failed"].as_array().unwrap().len(), 1, "{r}");
    assert_eq!(rows_of(&rig), rows);
    assert!(!rig.model.st.borrow().cas_log.iter().any(|p| p.contains("repoint")));
}

// --- A guest overwrites mid-pass ---------------------------------------------------------

#[test]
fn an_overwrite_that_lands_before_the_swap_wins() {
    let rig = Rig::new("overwrite");
    rig.vdisk("v1", "rw", &["n1"]);
    let (_a, _) = sparse(&rig, "v1");
    let env = FakeEnv::default();
    let mem = env.attach(&rig, "v1");
    // Between the scan and the swap the owner's drain repoints extent 0 at newer data.
    rig.model.st.borrow_mut().before_cas = Some(Box::new(|st, path, _| {
        if path == "/v1/dfs/block-map-repoint" {
            let row = st.block.iter_mut().find(|r| r.idx == 0).unwrap();
            row.egroup = Some("eg-b".to_string());
            row.offset = 0;
        }
    }));
    let r = go(&rig, &env, &opts());
    assert_eq!(r["lost_races"], 1, "{r}");
    assert_eq!(r["rows_repointed"], 0);
    let st = rig.model.st.borrow();
    let row = st.block.iter().find(|x| x.idx == 0).unwrap();
    assert_eq!(row.egroup.as_deref(), Some("eg-b"), "the pass overwrote a newer row");
    // The memory was not touched for a row that did not move.
    assert_eq!(mem.borrow()[&0].0, "eg-a");
}

// --- Crash safety ------------------------------------------------------------------------

fn three_ops(rig: &Rig) {
    rig.vdisk("v1", "rw", &["n1", "n2"]);
    rig.vdisk("snap", "immutable", &["n1", "n2"]);
    let (a, old) = sparse(rig, "v1");
    // Extent 3 is live in the sparse group too, via a second row; and the snapshot shares 0.
    rig.point("snap", 0, "eg-a", (a[0].1, a[0].2), "v1", &old[0]);
    rig.point("v2-detached-ok", 3, "eg-a", (a[3].1, a[3].2), "v1", &old[3]);
}

#[test]
fn a_stop_at_any_step_leaves_a_map_that_reads_correctly_and_a_rerun_finishes() {
    let steps = [
        Step::Staged,
        Step::Replicated,
        Step::Registered,
        Step::Published,
        Step::Verified,
        Step::Held,
        Step::BeforeRepoint(0),
        Step::AfterRepoint(0),
        Step::BeforeRepoint(1),
        Step::AfterRepoint(1),
        Step::BeforeRepoint(2),
        Step::AfterRepoint(2),
    ];
    for step in steps {
        let rig = Rig::new(&format!("crash-{step:?}").replace(['(', ')'], "-"));
        three_ops(&rig);
        rig.vdisk("v2-detached-ok", "immutable", &["n1", "n2"]);
        let env = FakeEnv::default();
        env.attach(&rig, "v1");
        let before = rows_of(&rig);

        let crashed = run(&rig.model, &rig.store, "n1", Duration::ZERO, &env, &t6(), &Crash(step),
                          &FakeClock::new(), &|_| None, NOW)
            .unwrap();
        assert_eq!(crashed["failed"].as_array().unwrap().len(), 1, "{step:?}: {crashed}");

        // Whatever stopped it: every row reads, from wherever it now points.
        rig.assert_every_row_reads_correctly();
        if matches!(step, Step::Staged | Step::Replicated | Step::Registered | Step::Published
                          | Step::Verified | Step::Held | Step::BeforeRepoint(0)) {
            assert_eq!(rows_of(&rig), before, "{step:?}: a row moved before the first swap");
        }
        // A group that is registered is sealed and complete; a half-built one is never visible
        // under a name a scan treats as a group.
        for f in files(&rig) {
            assert!(f.ends_with(".eg") || f.ends_with(".eg.moving"), "{f}");
        }
        for (id, g) in rig.model.st.borrow().egroups.iter() {
            if id.starts_with("eg-compact-") {
                assert_eq!(g.state, "sealed");
            }
        }

        // A restart forgets memory, so the rerun gets a freshly attached vdisk.
        let env2 = FakeEnv::default();
        env2.attach(&rig, "v1");
        let r = go(&rig, &env2, &t6());
        assert_eq!(r["failed"].as_array().unwrap().len(), 0, "{step:?} rerun: {r}");
        rig.assert_every_row_reads_correctly();
        assert!(
            !rig.referenced_groups().contains("eg-a"),
            "{step:?}: the old group is still referenced after a clean rerun"
        );
        // And once more: nothing left to do.
        assert_eq!(go(&rig, &env2, &t6())["candidate_count"], 0, "{step:?}");
    }
}

#[test]
fn a_statement_to_hydra_that_fails_part_way_stops_the_pass_with_progress_kept() {
    let rig = Rig::new("hydra-fails");
    three_ops(&rig);
    rig.vdisk("v2-detached-ok", "immutable", &["n1", "n2"]);
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    rig.model.st.borrow_mut().fail_cas = Some(("/v1/dfs/block-map-repoint".to_string(), 1));
    let r = go(&rig, &env, &t6());
    assert_eq!(r["stopped_by"], "error", "{r}");
    assert_eq!(r["rows_repointed"], 1);
    rig.assert_every_row_reads_correctly();
    rig.model.st.borrow_mut().fail_cas = None;
    let env2 = FakeEnv::default();
    env2.attach(&rig, "v1");
    go(&rig, &env2, &t6());
    assert!(!rig.referenced_groups().contains("eg-a"));
    rig.assert_every_row_reads_correctly();
}

// --- Replicas and sources ----------------------------------------------------------------

#[test]
fn a_replica_that_refuses_stops_the_batch_before_anything_is_published() {
    let rig = Rig::new("refuse");
    rig.vdisk("v1", "rw", &["n1", "n2", "n3"]);
    sparse(&rig, "v1");
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    env.refuse.borrow_mut().insert("n3".to_string());
    let (rows, fs) = (rows_of(&rig), files(&rig));
    let r = go(&rig, &env, &opts());
    assert_eq!(r["failed"].as_array().unwrap().len(), 1, "{r}");
    assert_eq!(rows_of(&rig), rows);
    assert_eq!(files(&rig), fs, "a file (even a temporary one) was left behind");
    assert!(!rig.model.st.borrow().egroups.keys().any(|k| k.starts_with("eg-compact-")));
    assert!(rig.model.st.borrow().cas_log.is_empty());
}

#[test]
fn a_replica_whose_copy_does_not_read_back_identically_stops_the_batch() {
    let rig = Rig::new("readback");
    rig.vdisk("v1", "rw", &["n1", "n2"]);
    sparse(&rig, "v1");
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    env.corrupt_readback.borrow_mut().insert("n2".to_string());
    let (rows, fs) = (rows_of(&rig), files(&rig));
    let r = go(&rig, &env, &opts());
    assert_eq!(r["failed"].as_array().unwrap().len(), 1, "{r}");
    assert_eq!(rows_of(&rig), rows);
    assert_eq!(files(&rig), fs);
}

#[test]
fn a_damaged_source_group_is_not_copied() {
    let rig = Rig::new("damaged");
    rig.vdisk("v1", "rw", &["n1"]);
    let (a, _) = sparse(&rig, "v1");
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().write(true).open(rig.store.path_for("eg-a")).unwrap();
        f.seek(SeekFrom::Start(a[0].1 as u64 + 5)).unwrap();
        f.write_all(b"X").unwrap();
    }
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    let rows = rows_of(&rig);
    let r = go(&rig, &env, &opts());
    assert_eq!(r["failed"].as_array().unwrap().len(), 1, "{r}");
    assert!(r["failed"][0]["error"].as_str().unwrap().contains("sealed as"), "{r}");
    assert_eq!(rows_of(&rig), rows);
    assert!(rig.model.st.borrow().cas_log.is_empty());
}

#[test]
fn an_extent_whose_footer_does_not_match_a_referrer_stops_the_batch() {
    let rig = Rig::new("misdirected");
    rig.vdisk("v1", "rw", &["n1"]);
    rig.vdisk("other", "immutable", &["n1"]);
    let (a, old) = sparse(&rig, "v1");
    // `other` reads this extent at index 5, but the footer says index 0: a misdirected row.
    rig.point("other", 5, "eg-a", (a[0].1, a[0].2), "v1", &old[0]);
    let env = FakeEnv::default();
    env.attach(&rig, "v1");
    let rows = rows_of(&rig);
    let r = go(&rig, &env, &opts());
    assert_eq!(r["failed"].as_array().unwrap().len(), 1, "{r}");
    assert_eq!(rows_of(&rig), rows);
}

// --- Packing, limits and pace ------------------------------------------------------------

fn three_vdisk_world(rig: &Rig) {
    for (i, v) in ["v1", "v2", "v3"].iter().enumerate() {
        // A different replica set each, so the three can never be merged into one batch.
        let peers = ["n2", "n3", "n4"][i];
        rig.vdisk(v, "rw", &["n1", peers]);
        let ga = format!("eg-a{i}");
        let gb = format!("eg-b{i}");
        let old: Vec<Vec<u8>> = (0..4).map(|k| data((i * 10 + k) as u8, LEN)).collect();
        let new: Vec<Vec<u8>> = (0..4).map(|k| data((100 + i * 10 + k) as u8, LEN)).collect();
        let a = rig.group(&ga, v, &(0..4).map(|k| (k as u64, old[k].clone())).collect::<Vec<_>>());
        let b = rig.group(&gb, v, &(1..4).map(|k| (k as u64, new[k].clone())).collect::<Vec<_>>());
        rig.point(v, 0, &ga, (a[0].1, a[0].2), v, &old[0]);
        for k in 1..4usize {
            rig.point(v, k as u64, &gb, (b[k - 1].1, b[k - 1].2), v, &new[k]);
        }
    }
}

#[test]
fn the_limits_bound_what_one_pass_does_and_say_which_one_bit() {
    let rig = Rig::new("limits");
    three_vdisk_world(&rig);
    let env = FakeEnv::default();
    for v in ["v1", "v2", "v3"] {
        env.attach(&rig, v);
    }
    let r = go(&rig, &env, &Options { max_groups: 2, ..opts() });
    assert_eq!(r["selected_groups"], 2, "{r}");
    assert_eq!(r["stopped_by"], "max_groups");
    assert_eq!(r["executed"].as_array().unwrap().len(), 2);

    let rig = Rig::new("limits-bytes");
    three_vdisk_world(&rig);
    let env = FakeEnv::default();
    for v in ["v1", "v2", "v3"] {
        env.attach(&rig, v);
    }
    let r = go(&rig, &env, &Options { max_bytes: (LEN + 32) as u64 + 1, ..opts() });
    assert_eq!(r["selected_groups"], 1, "{r}");
    assert_eq!(r["stopped_by"], "max_bytes");
}

#[test]
fn the_pass_is_paced_by_the_bytes_it_moves() {
    let rig = Rig::new("pace");
    three_vdisk_world(&rig);
    let env = FakeEnv::default();
    for v in ["v1", "v2", "v3"] {
        env.attach(&rig, v);
    }
    let clock = FakeClock::new();
    // 64 KiB per second against about 16 KiB x (local copy + one peer + the source read) per
    // batch: the pass must have waited.
    let o = Options { rate: 64 * 1024, ..opts() };
    run(&rig.model, &rig.store, "n1", Duration::ZERO, &env, &o, &NoProbe, &clock, &|_| None, NOW).unwrap();
    assert!(clock.slept.get() >= Duration::from_millis(500), "slept only {:?}", clock.slept.get());

    let rig = Rig::new("pace-free");
    three_vdisk_world(&rig);
    let env = FakeEnv::default();
    for v in ["v1", "v2", "v3"] {
        env.attach(&rig, v);
    }
    let clock = FakeClock::new();
    run(&rig.model, &rig.store, "n1", Duration::ZERO, &env, &opts(), &NoProbe, &clock, &|_| None, NOW)
        .unwrap();
    assert_eq!(clock.slept.get(), Duration::ZERO);
}

#[test]
fn no_new_batch_starts_after_the_time_budget() {
    let rig = Rig::new("budget");
    three_vdisk_world(&rig);
    let env = FakeEnv::default();
    for v in ["v1", "v2", "v3"] {
        env.attach(&rig, v);
    }
    let clock = FakeClock::new();
    let o = Options { rate: 1024, seconds: 1, ..opts() };
    let r = run(&rig.model, &rig.store, "n1", Duration::ZERO, &env, &o, &NoProbe, &clock, &|_| None, NOW)
        .unwrap();
    assert_eq!(r["executed"].as_array().unwrap().len(), 1, "{r}");
    assert_eq!(r["stopped_by"], "time");
    rig.assert_every_row_reads_correctly();
}

#[test]
fn sparse_groups_with_the_same_policy_share_a_new_group_and_others_do_not() {
    let mk = |id: &str, live: u64, container: &str, replicas: &[&str]| Candidate {
        id: id.into(),
        size: 1000,
        live_bytes: live,
        extents: Vec::new(),
        container: container.into(),
        replicas: replicas.iter().map(|s| s.to_string()).collect(),
        hint: String::new(),
        seal_hash: String::new(),
        vdisks: BTreeSet::new(),
    };
    let bins = pack(
        vec![
            mk("a", 200, "pool", &["n2"]),
            mk("b", 200, "pool", &["n2"]),
            mk("c", 200, "pool", &["n3"]),
            mk("d", 200, "other", &["n2"]),
        ],
        1000,
    );
    assert_eq!(bins.len(), 3, "{bins:?}");
    let together = bins.iter().find(|b| b.sources.len() == 2).expect("a and b share one");
    assert_eq!(together.sources.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(), ["a", "b"]);
    // A source is never split, and a new group is never packed past its capacity.
    let big = pack(vec![mk("a", 600, "p", &[]), mk("b", 600, "p", &[])], 1000);
    assert_eq!(big.len(), 2);
}

// --- Convergence over random maps --------------------------------------------------------

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

#[test]
fn random_maps_converge_without_ever_changing_what_a_read_returns() {
    for seed in 1..=25u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let rig = Rig::new(&format!("rand-{seed}"));
        rig.vdisk("live", "rw", &["n1", "n2"]);
        rig.vdisk("snap", "immutable", &["n1", "n2"]);
        rig.vdisk("snap2", "immutable", &["n1", "n2"]);
        let mut counter = 0u8;
        let mut idx = 0u64;
        for g in 0..(2 + rng.below(5)) {
            let n = 1 + rng.below(4) as usize;
            let exts: Vec<(u64, Vec<u8>)> = (0..n)
                .map(|k| {
                    counter = counter.wrapping_add(1);
                    (idx + k as u64, data(counter, 2048 + (rng.below(3) as usize) * 1024))
                })
                .collect();
            let id = format!("eg-r{g}");
            let locs = rig.group(&id, "live", &exts);
            // Each extent is live for some subset of the three vdisks, or for none.
            for (k, (_, off, len)) in locs.iter().enumerate() {
                let who: Vec<&str> = ["live", "snap", "snap2"]
                    .into_iter()
                    .filter(|_| rng.below(3) == 0)
                    .collect();
                for w in who {
                    // The same index in every sharer, as a clone's copied rows have.
                    let ix = idx + k as u64;
                    rig.point(w, ix, &id, (*off, *len), "live", &exts[k].1);
                }
            }
            idx += 8;
        }
        let env = FakeEnv::default();
        let mem = env.attach(&rig, "live");
        let rows_total = rig.model.st.borrow().block.len();
        let threshold = 0.3 + (rng.below(5) as f64) / 10.0;
        for _ in 0..8 {
            let r = go(&rig, &env, &Options { threshold, ..opts() });
            assert!(r["anomalies"].as_array().unwrap().is_empty(), "seed {seed}: {r}");
            assert_eq!(r["failed"].as_array().unwrap().len(), 0, "seed {seed}: {r}");
            rig.assert_every_row_reads_correctly();
            assert_eq!(rig.model.st.borrow().block.len(), rows_total);
            // The attached vdisk's memory is the map, always.
            for row in rig.model.st.borrow().block.iter().filter(|x| x.vdisk == "live") {
                assert_eq!(
                    mem.borrow()[&row.idx],
                    (row.egroup.clone().unwrap(), row.offset, row.length),
                    "seed {seed}: memory and the map disagree"
                );
            }
            if r["candidate_count"] == 0 {
                break;
            }
        }
        let last = go(&rig, &env, &Options { apply: false, threshold, ..opts() });
        assert_eq!(last["candidate_count"], 0, "seed {seed} did not converge: {last}");
    }
}
