//! Two sites on one machine.
//!
//! **This is a simulation.** "Site A" is a real `EgroupStore` in one temporary directory holding
//! real extent groups written through the real append path; "site B" is a `DirStore` in another
//! with its own staging directory and an in-memory map sink. The two share a process and a clock
//! and nothing between them is a network, so what these tests show is that the protocol's logic
//! is right: what is shipped, what is refused, and that a snapshot is never visible before it is
//! whole. They show nothing about a real link, TLS, a real Hydra, or two machines' clocks.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use crate::err::{Error, Result};
use crate::extent::EgroupStore;

use super::export::{export_groups, ExportReport, GroupSource, Want};
use super::import::{DirStore, Have, Importer, InstalledMeta, MapSink, Offer, Published, GroupStore};
use super::throttle::tests::FakeClock;
use super::throttle::TokenBucket;
use super::*;

fn scratch(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("sidon-repl-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// A map sink that remembers what was written and can be made to fail at a named step.
#[derive(Default)]
struct MemSink {
    visible: HashMap<String, String>,
    forming: HashMap<String, Vec<MapRow>>,
    fail_at: Option<&'static str>,
    drop_a_row_on_read_back: bool,
    calls: Vec<String>,
}

impl MemSink {
    fn step(&mut self, name: &'static str) -> Result<()> {
        self.calls.push(name.to_string());
        if self.fail_at == Some(name) {
            return Err(Error::meta(format!("injected failure at {name}")));
        }
        Ok(())
    }
}

impl MapSink for MemSink {
    fn visible(&self, snapshot: &str) -> Option<String> {
        self.visible.get(snapshot).cloned()
    }
    fn begin(&mut self, m: &Manifest) -> Result<()> {
        self.step("begin")?;
        self.forming.insert(m.snapshot.clone(), Vec::new());
        Ok(())
    }
    fn write_rows(&mut self, snapshot: &str, rows: &[MapRow]) -> Result<()> {
        self.step("write_rows")?;
        self.forming.get_mut(snapshot).unwrap().extend_from_slice(rows);
        Ok(())
    }
    fn read_back(&self, snapshot: &str) -> Result<Vec<MapRow>> {
        let mut rows = self.forming.get(snapshot).cloned().unwrap_or_default();
        if self.drop_a_row_on_read_back {
            rows.pop();
        }
        Ok(rows)
    }
    fn make_visible(&mut self, snapshot: &str) -> Result<()> {
        self.step("make_visible")?;
        let digest = Manifest {
            snapshot: snapshot.to_string(), size_bytes: self.size_of(snapshot), extent_bytes: 1 << 20,
            rows: self.forming[snapshot].clone(), groups: vec![],
        }
        .map_digest();
        self.visible.insert(snapshot.to_string(), digest);
        Ok(())
    }
}

impl MemSink {
    fn size_of(&self, _snapshot: &str) -> u64 {
        SIZE
    }
}

const SIZE: u64 = 1 << 30;
const VH: u64 = 0x8000_0000_0000_00AB; // the top bit set, the case that wraps if mishandled

/// Site A: real extent groups.
struct SiteA {
    dir: PathBuf,
    store: EgroupStore,
    /// target id -> source id
    source_ids: HashMap<String, String>,
}

impl SiteA {
    fn new(name: &str) -> SiteA {
        let dir = scratch(&format!("{name}-a"));
        SiteA { store: EgroupStore::new(&dir.join("egroups"), 1 << 30).unwrap(), dir, source_ids: HashMap::new() }
    }

    /// Write one extent group holding the given extents; returns its rows (source ids) and size.
    fn put(&self, gid: &str, extents: &[(u64, usize)]) -> (Vec<MapRow>, u64) {
        let mut eg = self.store.create(gid).unwrap();
        let mut rows = Vec::new();
        for (index, len) in extents {
            let data: Vec<u8> = (0..*len).map(|i| (i as u64 * 31 + index * 7) as u8).collect();
            let (off, stored, _) = self.store.append_framed(&mut eg, &data, VH, *index, false).unwrap();
            rows.push(MapRow { extent_index: *index, group: gid.to_string(), offset: off, length: stored, vdisk_hash: VH });
        }
        self.store.sync(&mut eg).unwrap();
        (rows, eg.size)
    }

    fn bytes(&self, gid: &str) -> Vec<u8> {
        std::fs::read(self.store.path_for(gid)).unwrap()
    }

    /// A manifest from groups: `(source id, rows, size, sealed)`. A sealed group ships whole under
    /// its own id with its seal; one still open ships its prefix as `<id>~<len>` with none.
    fn manifest(&mut self, snapshot: &str, parts: &[(&str, Vec<MapRow>, u64, bool)]) -> Manifest {
        let mut rows = Vec::new();
        let mut groups = Vec::new();
        for (gid, grows, size, sealed) in parts {
            let target = if *sealed { gid.to_string() } else { format!("{gid}~{size}") };
            let seal = if *sealed { seal_of(&self.bytes(gid)) } else { String::new() };
            self.source_ids.insert(target.clone(), gid.to_string());
            groups.push(GroupRef { id: target.clone(), length: *size, seal });
            for r in grows {
                rows.push(MapRow { group: target.clone(), ..r.clone() });
            }
        }
        rows.sort_by_key(|r| r.extent_index);
        Manifest { snapshot: snapshot.to_string(), size_bytes: SIZE, extent_bytes: 1 << 20, rows, groups }
    }

    fn wants(&self, have: &[(String, Have)], m: &Manifest) -> Vec<Want> {
        have.iter().filter_map(|(id, h)| {
            let group = m.groups.iter().find(|g| &g.id == id).unwrap().clone();
            let start = match h {
                Have::Complete => return None,
                Have::Absent => 0,
                Have::Partial(n) => *n,
            };
            Some(Want { source_id: self.source_ids[id].clone(), group, start })
        }).collect()
    }
}

impl GroupSource for SiteA {
    fn read_group(&self, id: &str, length: u64) -> Result<Vec<u8>> {
        self.store.read_group(id, length)
    }
}

/// Site B.
struct SiteB {
    root: PathBuf,
    importer: Importer<DirStore>,
    sink: MemSink,
}

impl SiteB {
    fn new(name: &str, capacity: Option<u64>) -> SiteB {
        let root = scratch(&format!("{name}-b"));
        let importer = Importer::new(DirStore::new(&root.join("groups"), capacity).unwrap(), &root.join("staging")).unwrap();
        SiteB { root, importer, sink: MemSink::default() }
    }

    /// The same directories after a restart: nothing but what is on disk survives.
    fn restarted(self, capacity: Option<u64>) -> SiteB {
        let importer = Importer::new(DirStore::new(&self.root.join("groups"), capacity).unwrap(), &self.root.join("staging")).unwrap();
        SiteB { root: self.root, importer, sink: self.sink }
    }

    /// Reads an extent through the real read path, the one a guest's read takes.
    fn read_extent(&self, group: &str, row: &MapRow) -> Vec<u8> {
        let store = EgroupStore::new(&self.root.join("groups"), 1 << 30).unwrap();
        store.read_extent(group, row.offset, row.length, row.vdisk_hash, row.extent_index).unwrap()
    }
}

fn expected(index: u64, len: usize) -> Vec<u8> {
    (0..len).map(|i| (i as u64 * 31 + index * 7) as u8).collect()
}

/// One whole attempt: offer, send what is missing, receive, publish.
fn replicate(a: &SiteA, b: &mut SiteB, m: &Manifest, job: &str) -> Result<(ExportReport, Published)> {
    let have = match b.importer.offer(job, m, &b.sink)? {
        Offer::AlreadyPresent => return Ok((ExportReport::default(), Published::AlreadyVisible)),
        Offer::Proceed { have, .. } => have,
    };
    let clock = FakeClock::new();
    let mut stream = Vec::new();
    let report = export_groups(m, a, &a.wants(&have, m), &mut stream, &mut TokenBucket::unlimited(), &clock)?;
    let received = b.importer.receive(job, m, &mut &stream[..])?;
    if let Some((id, why)) = received.failed.first() {
        return Err(Error::corrupt(format!("{id}: {why}")));
    }
    let published = b.importer.publish(m, &mut b.sink)?;
    Ok((report, published))
}

#[test]
fn a_snapshot_arrives_whole_and_is_readable_through_the_ordinary_read_path() {
    let mut a = SiteA::new("whole");
    let (r1, s1) = a.put("eg-1", &[(0, 5000), (1, 7000)]);
    let (r2, s2) = a.put("eg-2", &[(2, 300_000)]);
    let m = a.manifest("snap-1", &[("eg-1", r1, s1, true), ("eg-2", r2, s2, true)]);
    let mut b = SiteB::new("whole", None);
    let (report, published) = replicate(&a, &mut b, &m, "job1").unwrap();
    assert_eq!(published, Published::Published);
    assert_eq!(report.bytes_sent, s1 + s2);
    assert!(b.sink.visible("snap-1").is_some());
    for r in &m.rows {
        let len = if r.extent_index == 0 { 5000 } else if r.extent_index == 1 { 7000 } else { 300_000 };
        assert_eq!(b.read_extent(&r.group, r), expected(r.extent_index, len), "extent {}", r.extent_index);
    }
}

#[test]
fn a_second_snapshot_sends_only_the_groups_the_target_lacks() {
    // The property that makes replication useful: groups are immutable and shared, so what is
    // missing is exactly what was written since.
    let mut a = SiteA::new("delta");
    let (r1, s1) = a.put("eg-1", &[(0, 4000), (1, 4000)]);
    let m1 = a.manifest("snap-1", &[("eg-1", r1.clone(), s1, true)]);
    let mut b = SiteB::new("delta", None);
    replicate(&a, &mut b, &m1, "j1").unwrap();

    let (r2, s2) = a.put("eg-2", &[(1, 9000)]); // extent 1 rewritten: redirect-on-write
    let mut rows = vec![r1[0].clone()];
    rows.extend(r2.clone());
    let m2 = a.manifest("snap-2", &[("eg-1", rows[..1].to_vec(), s1, true), ("eg-2", r2, s2, true)]);
    let (report, _) = replicate(&a, &mut b, &m2, "j2").unwrap();
    assert_eq!(report.groups, 1);
    assert_eq!(report.bytes_sent, s2, "the group the target already held was sent again");
    assert!(b.sink.visible("snap-2").is_some());
    // And the first snapshot is unaffected.
    assert_eq!(b.read_extent("eg-1", &r1[1]), expected(1, 4000));
}

#[test]
fn the_delta_needs_no_earlier_snapshot_to_exist() {
    // Deleting the first snapshot on either side must not change what the second needs, which
    // is why the target is asked what it holds instead of being sent a difference.
    let mut a = SiteA::new("nobase");
    let (r1, s1) = a.put("eg-1", &[(0, 4000)]);
    let m1 = a.manifest("snap-1", &[("eg-1", r1.clone(), s1, true)]);
    let mut b = SiteB::new("nobase", None);
    replicate(&a, &mut b, &m1, "j1").unwrap();
    b.sink.visible.clear();
    b.sink.forming.clear();
    let (r2, s2) = a.put("eg-2", &[(1, 100)]);
    let m2 = a.manifest("snap-2", &[("eg-1", r1, s1, true), ("eg-2", r2, s2, true)]);
    let (report, _) = replicate(&a, &mut b, &m2, "j2").unwrap();
    assert_eq!(report.bytes_sent, s2);
}

#[test]
fn a_group_still_open_at_the_source_ships_as_a_prefix_under_its_own_name() {
    let mut a = SiteA::new("open");
    let (r1, s1) = a.put("eg-open", &[(0, 4000)]);
    let m1 = a.manifest("snap-1", &[("eg-open", r1.clone(), s1, false)]);
    assert_eq!(m1.groups[0].id, format!("eg-open~{s1}"));
    let mut b = SiteB::new("open", None);
    replicate(&a, &mut b, &m1, "j1").unwrap();
    assert_eq!(b.read_extent(&m1.groups[0].id, &m1.rows[0]), expected(0, 4000));
}

#[test]
fn a_dropped_link_resumes_from_the_last_whole_chunk_and_not_from_zero() {
    let mut a = SiteA::new("drop");
    // About 2.6 MiB in one group: three chunks.
    let (r, s) = a.put("eg-big", &[(0, 900_000), (1, 900_000), (2, 900_000)]);
    let m = a.manifest("snap-1", &[("eg-big", r, s, true)]);
    assert!(s as usize > 2 * CHUNK);
    let mut b = SiteB::new("drop", None);

    let have = match b.importer.offer("job", &m, &b.sink).unwrap() { Offer::Proceed { have, .. } => have, _ => panic!() };
    // A writer that fails after the first chunk and a few bytes of the second.
    struct Dropping { buf: Vec<u8>, limit: usize }
    impl Write for Dropping {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            if self.buf.len() + data.len() > self.limit {
                let keep = self.limit - self.buf.len();
                self.buf.extend_from_slice(&data[..keep]);
                return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "link dropped"));
            }
            self.buf.extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let id_len = m.groups[0].id.len();
    let mut link = Dropping { buf: Vec::new(), limit: (24 + id_len + 8) + (24 + id_len + CHUNK) + 100 };
    let clock = FakeClock::new();
    assert!(export_groups(&m, &a, &a.wants(&have, &m), &mut link, &mut TokenBucket::unlimited(), &clock).is_err());
    // What arrived is a stream cut inside a frame.
    assert!(b.importer.receive("job", &m, &mut &link.buf[..]).is_err());
    assert!(b.sink.visible("snap-1").is_none(), "nothing is visible after a drop");

    // After a restart, the offer reports where to resume.
    let mut b = b.restarted(None);
    let have = match b.importer.offer("job", &m, &b.sink).unwrap() { Offer::Proceed { have, .. } => have, _ => panic!() };
    assert_eq!(have[0].1, Have::Partial(CHUNK as u64));
    let mut stream = Vec::new();
    let report = export_groups(&m, &a, &a.wants(&have, &m), &mut stream, &mut TokenBucket::unlimited(), &clock).unwrap();
    assert_eq!(report.bytes_sent, s - CHUNK as u64, "the whole group was sent again");
    let received = b.importer.receive("job", &m, &mut &stream[..]).unwrap();
    assert_eq!(received.installed.len(), 1);
    assert_eq!(b.importer.publish(&m, &mut b.sink).unwrap(), Published::Published);
    assert_eq!(b.read_extent("eg-big", &m.rows[2]), expected(2, 900_000));
}

#[test]
fn a_torn_staged_prefix_is_caught_by_the_end_digest_and_that_group_restarts() {
    // Resume is an optimisation and never a trust: a prefix damaged by a crash passes the
    // chunk-boundary rounding and must be caught by the digest of the whole.
    let mut a = SiteA::new("torn");
    let (r, s) = a.put("eg-big", &[(0, 900_000), (1, 900_000), (2, 900_000)]);
    let m = a.manifest("snap-1", &[("eg-big", r, s, true)]);
    let mut b = SiteB::new("torn", None);
    let part = b.root.join("staging").join("job");
    std::fs::create_dir_all(&part).unwrap();
    let mut staged = a.bytes("eg-big");
    staged.truncate(CHUNK + 10);
    staged[100] ^= 0xFF; // damage inside the kept chunk
    std::fs::write(part.join(format!("{}.part", m.groups[0].id)), &staged).unwrap();

    let have = match b.importer.offer("job", &m, &b.sink).unwrap() { Offer::Proceed { have, .. } => have, _ => panic!() };
    assert_eq!(have[0].1, Have::Partial(CHUNK as u64));
    let clock = FakeClock::new();
    let mut stream = Vec::new();
    export_groups(&m, &a, &a.wants(&have, &m), &mut stream, &mut TokenBucket::unlimited(), &clock).unwrap();
    let received = b.importer.receive("job", &m, &mut &stream[..]).unwrap();
    assert!(received.installed.is_empty());
    assert_eq!(received.failed.len(), 1);
    assert!(b.importer.store.installed(&m.groups[0].id).is_none());
    // Its staged bytes are gone, so the next attempt starts that group from zero and succeeds.
    let (report, published) = replicate(&a, &mut b, &m, "job").unwrap();
    assert_eq!(published, Published::Published);
    assert_eq!(report.bytes_sent, s);
}

#[test]
fn a_byte_flipped_in_transit_installs_nothing() {
    let mut a = SiteA::new("flip");
    let (r, s) = a.put("eg-1", &[(0, 5000)]);
    let m = a.manifest("snap-1", &[("eg-1", r, s, true)]);
    let mut b = SiteB::new("flip", None);
    let have = match b.importer.offer("job", &m, &b.sink).unwrap() { Offer::Proceed { have, .. } => have, _ => panic!() };
    let mut stream = Vec::new();
    export_groups(&m, &a, &a.wants(&have, &m), &mut stream, &mut TokenBucket::unlimited(), &FakeClock::new()).unwrap();
    let at = stream.len() / 2;
    stream[at] ^= 0x10;
    assert!(matches!(b.importer.receive("job", &m, &mut &stream[..]), Err(Error::Corrupt(_))));
    assert!(b.importer.store.installed("eg-1").is_none());
    assert!(b.sink.visible("snap-1").is_none());
}

#[test]
fn damage_at_the_source_is_found_before_anything_is_sent() {
    let mut a = SiteA::new("srcbad");
    let (r, s) = a.put("eg-1", &[(0, 5000)]);
    let m = a.manifest("snap-1", &[("eg-1", r, s, true)]);
    let mut bad = a.bytes("eg-1");
    bad[10] ^= 0x01;
    std::fs::write(a.store.path_for("eg-1"), bad).unwrap();
    let mut out = Vec::new();
    let err = export_groups(&m, &a, &a.wants(&[("eg-1".into(), Have::Absent)], &m), &mut out,
                            &mut TokenBucket::unlimited(), &FakeClock::new()).unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    assert!(err.to_string().contains("not sending"));
    assert!(out.is_empty(), "bytes of a damaged group left the building");
}

#[test]
fn the_importer_checks_the_footers_itself_and_does_not_take_the_senders_word() {
    // A sender that is wrong or hostile sends damaged bytes with a SHA-256 that matches them.
    // The digest alone would accept that; the importer's own footer check must not.
    let mut a = SiteA::new("lies");
    let (r, s) = a.put("eg-1", &[(0, 5000)]);
    let m = a.manifest("snap-1", &[("eg-1", r, s, false)]); // no seal: only footers can catch it
    let mut bytes = a.bytes("eg-1");
    bytes[20] ^= 0xFF;
    let g = &m.groups[0];
    let mut stream = Vec::new();
    write_frame(&mut stream, &Frame::Begin { id: g.id.clone(), length: g.length, start: 0 }).unwrap();
    write_frame(&mut stream, &Frame::Chunk { id: g.id.clone(), offset: 0, data: bytes.clone() }).unwrap();
    write_frame(&mut stream, &Frame::End { id: g.id.clone(), sha256: sha256(&bytes) }).unwrap();
    let mut b = SiteB::new("lies", None);
    let received = b.importer.receive("job", &m, &mut &stream[..]).unwrap();
    assert!(received.installed.is_empty());
    assert!(received.failed[0].1.contains("checksum"), "{:?}", received.failed);
}

#[test]
fn a_stream_naming_a_group_the_manifest_does_not_list_is_refused() {
    let mut a = SiteA::new("alien");
    let (r, s) = a.put("eg-1", &[(0, 100)]);
    let m = a.manifest("snap-1", &[("eg-1", r, s, true)]);
    let mut stream = Vec::new();
    write_frame(&mut stream, &Frame::Begin { id: "eg-evil".into(), length: 10, start: 0 }).unwrap();
    let mut b = SiteB::new("alien", None);
    assert!(matches!(b.importer.receive("job", &m, &mut &stream[..]), Err(Error::Refused(_))));
}

fn publish_ready(name: &str) -> (SiteA, SiteB, Manifest) {
    let mut a = SiteA::new(name);
    let (r1, s1) = a.put("eg-1", &[(0, 4000), (1, 4000)]);
    let m = a.manifest("snap-1", &[("eg-1", r1, s1, true)]);
    let mut b = SiteB::new(name, None);
    let have = match b.importer.offer("job", &m, &b.sink).unwrap() { Offer::Proceed { have, .. } => have, _ => panic!() };
    let mut stream = Vec::new();
    export_groups(&m, &a, &a.wants(&have, &m), &mut stream, &mut TokenBucket::unlimited(), &FakeClock::new()).unwrap();
    b.importer.receive("job", &m, &mut &stream[..]).unwrap();
    (a, b, m)
}

#[test]
fn a_snapshot_is_not_visible_at_any_point_before_the_last_step() {
    for step in ["begin", "write_rows", "make_visible"] {
        let (_a, mut b, m) = publish_ready(&format!("crash-{step}"));
        b.sink.fail_at = Some(step);
        assert!(b.importer.publish(&m, &mut b.sink).is_err(), "{step}");
        assert!(b.sink.visible("snap-1").is_none(), "visible after a failure at {step}");
        // Retrying completes it, from whatever state the failure left.
        b.sink.fail_at = None;
        assert_eq!(b.importer.publish(&m, &mut b.sink).unwrap(), Published::Published, "{step}");
        assert!(b.sink.visible("snap-1").is_some());
        // And a second publish of the same map is a no-op.
        assert_eq!(b.importer.publish(&m, &mut b.sink).unwrap(), Published::AlreadyVisible);
    }
}

#[test]
fn a_map_that_reads_back_differently_from_what_was_written_is_never_made_visible() {
    let (_a, mut b, m) = publish_ready("readback");
    b.sink.drop_a_row_on_read_back = true;
    let err = b.importer.publish(&m, &mut b.sink).unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    assert!(!b.sink.calls.contains(&"make_visible".to_string()));
    assert!(b.sink.visible("snap-1").is_none());
}

#[test]
fn publish_refuses_while_any_group_is_not_installed() {
    let mut a = SiteA::new("missing");
    let (r1, s1) = a.put("eg-1", &[(0, 100)]);
    let (r2, s2) = a.put("eg-2", &[(1, 100)]);
    let m = a.manifest("snap-1", &[("eg-1", r1, s1, true), ("eg-2", r2, s2, true)]);
    let mut b = SiteB::new("missing", None);
    let err = b.importer.publish(&m, &mut b.sink).unwrap_err();
    assert!(matches!(err, Error::Refused(_)));
    assert!(b.sink.calls.is_empty(), "the map was touched before its data was here");
}

#[test]
fn the_same_snapshot_offered_twice_is_a_no_op_and_a_different_one_under_its_name_is_refused() {
    let mut a = SiteA::new("twice");
    let (r1, s1) = a.put("eg-1", &[(0, 4000)]);
    let m = a.manifest("snap-1", &[("eg-1", r1.clone(), s1, true)]);
    let mut b = SiteB::new("twice", None);
    replicate(&a, &mut b, &m, "j1").unwrap();
    assert_eq!(b.importer.offer("j2", &m, &b.sink).unwrap(), Offer::AlreadyPresent);
    let (report, published) = replicate(&a, &mut b, &m, "j2").unwrap();
    assert_eq!((report.bytes_sent, published), (0, Published::AlreadyVisible));

    // The same name with a different map: the source recreated it, or something is damaged.
    let (r2, s2) = a.put("eg-2", &[(0, 4100)]);
    let other = a.manifest("snap-1", &[("eg-2", r2, s2, true)]);
    let err = b.importer.offer("j3", &other, &b.sink).unwrap_err();
    assert!(err.to_string().contains("conflict"), "{err}");
}

#[test]
fn a_group_id_already_held_with_different_content_is_a_conflict_and_is_never_overwritten() {
    let mut a = SiteA::new("collide");
    let (r1, s1) = a.put("eg-1", &[(0, 4000)]);
    let m = a.manifest("snap-1", &[("eg-1", r1, s1, true)]);
    let mut b = SiteB::new("collide", None);
    b.importer.store.install("eg-1", &{
        let p = b.root.join("x.part");
        std::fs::write(&p, b"something else entirely").unwrap();
        p
    }, &InstalledMeta { length: 23, sha256: "x".into(), seal: "crc32c:00000000".into() }).unwrap();
    let err = b.importer.offer("job", &m, &b.sink).unwrap_err();
    assert!(err.to_string().contains("conflict"), "{err}");
    assert_eq!(std::fs::read(b.importer.store.path_of("eg-1")).unwrap(), b"something else entirely");
}

#[test]
fn a_target_without_room_refuses_before_a_byte_is_sent() {
    let mut a = SiteA::new("space");
    let (r, s) = a.put("eg-1", &[(0, 50_000)]);
    let m = a.manifest("snap-1", &[("eg-1", r, s, true)]);
    let b = SiteB::new("space", Some(s - 1));
    let err = b.importer.offer("job", &m, &b.sink).unwrap_err();
    assert!(err.to_string().contains("no space"), "{err}");
    // It must not have tried to make room.
    assert_eq!(std::fs::read_dir(b.root.join("groups")).unwrap().count(), 0);
}

#[test]
fn running_out_of_space_mid_transfer_pauses_it_with_staging_kept_and_it_resumes_later() {
    let mut a = SiteA::new("enospc");
    let (r, s) = a.put("eg-big", &[(0, 900_000), (1, 900_000), (2, 900_000)]);
    let m = a.manifest("snap-1", &[("eg-big", r, s, true)]);
    let b = SiteB::new("enospc", None);
    let root = b.root.clone();
    let mut full = Importer::new(DirStore::new(&root.join("groups"), None).unwrap(), &root.join("staging")).unwrap()
        .with_space_guard({
            let mut written = 0u64;
            Box::new(move |n| {
                written += n;
                if written > CHUNK as u64 {
                    Err(Error::io("No space left on device (os error 28)".to_string()))
                } else { Ok(()) }
            })
        });
    let have = match full.offer("job", &m, &b.sink).unwrap() { Offer::Proceed { have, .. } => have, _ => panic!() };
    let mut stream = Vec::new();
    export_groups(&m, &a, &a.wants(&have, &m), &mut stream, &mut TokenBucket::unlimited(), &FakeClock::new()).unwrap();
    let err = full.receive("job", &m, &mut &stream[..]).unwrap_err();
    assert_eq!(err.errno(), 28, "{err}");
    assert!(b.sink.visible("snap-1").is_none());

    // Space is found; the same directories resume from the whole chunk that made it.
    let mut b = b.restarted(None);
    let (report, published) = replicate(&a, &mut b, &m, "job").unwrap();
    assert_eq!(published, Published::Published);
    assert!(report.bytes_sent < s, "the resume started over");
}

#[test]
fn sending_is_limited_to_the_rate_the_site_was_given() {
    let mut a = SiteA::new("rate");
    let (r, s) = a.put("eg-big", &[(0, 900_000), (1, 900_000), (2, 900_000)]);
    let m = a.manifest("snap-1", &[("eg-big", r, s, true)]);
    let clock = FakeClock::new();
    let mut out = Vec::new();
    let mut bucket = TokenBucket::new(CHUNK as u64, CHUNK as u64);
    export_groups(&m, &a, &a.wants(&[("eg-big".into(), Have::Absent)], &m), &mut out, &mut bucket, &clock).unwrap();
    // 2.6 MiB at 1 MiB/s with a one-chunk burst: at least the time for what the burst did not cover.
    let floor = (s as f64 - CHUNK as f64) / CHUNK as f64 - 0.05;
    assert!(clock.slept.get().as_secs_f64() >= floor, "{:?} < {floor}", clock.slept.get());
}

#[test]
fn nothing_here_reads_the_wall_clock_so_two_sites_that_disagree_cannot_disagree_about_a_snapshot() {
    let source = include_str!("../replicate.rs").to_string()
        + include_str!("export.rs") + include_str!("import.rs");
    // Test code above the first `#[cfg(test)]` is the production surface.
    for file in [include_str!("../replicate.rs"), include_str!("export.rs"), include_str!("import.rs")] {
        let production = file.split("#[cfg(test)]").next().unwrap();
        assert!(!production.contains("SystemTime"), "wall clock in production code");
        assert!(!production.contains("UNIX_EPOCH"));
    }
    assert!(!source.is_empty());
}

#[test]
fn abandoned_staging_is_reaped_but_a_recent_job_is_not() {
    let b = SiteB::new("reap", None);
    std::fs::create_dir_all(b.root.join("staging").join("dead-job")).unwrap();
    assert_eq!(b.importer.reap_staging(Duration::from_secs(3600)).unwrap(), 0);
    assert_eq!(b.importer.reap_staging(Duration::ZERO).unwrap(), 1);
}

use std::time::Duration;
