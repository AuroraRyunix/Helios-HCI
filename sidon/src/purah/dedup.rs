//! The dedup estimator: how many bytes would be shared beyond what clone-from-image shares.
//!
//! This is step one of D-23's addendum and nothing more. It turns "the owner wants dedup" into
//! a number measured on the owner's own data, so that the decision to build the machinery (a
//! content-derived id, a lookup on every drain, a resurrection guard) can be made against a
//! figure instead of a hope. **It writes nothing.** It takes a [`Rows`], which has no way to
//! write, and an extent store it only reads, and its one output is a report. It does not change
//! an extent id, does not add a setting, and does not dedup anything.
//!
//! # What is counted
//!
//! The map's rows are first reduced to *stored extents* -- distinct (group, offset) locations.
//! A clone's rows are a copy of its parent's, so a thousand clones of an image are a thousand
//! rows per stored extent, and that sharing is already free: it is reported as
//! `shared_by_clone_bytes` and is not dedup's to claim. What is left is the stored extents, and
//! the question is how many of *those* hold bytes another one holds. They are hashed (SHA-256 of
//! the extent as the guest wrote it, so a compressed and an uncompressed copy of the same bytes
//! match) and the redundant ones are `would_share_bytes`: what an extent-granular dedup would
//! return beyond what clones already return.
//!
//! All-zero extents are counted apart. A guest that trims or zero-fills writes them, they are
//! duplicates of each other by construction, and the remedy for them is a sparse map rather than
//! a hash index, so folding them into the headline would flatter dedup with something it is not
//! needed for.
//!
//! # Sampling, and what it does and does not say
//!
//! Stored extents are ordered by a hash of their location and processed in that order, up to a
//! requested fraction. That makes any *prefix* a uniform sample, so a pass cut short by its time
//! budget is still a sample, and the report states the fraction it actually covered. Within a
//! sample a duplicate is seen only if both copies were sampled, so a sample **undercounts
//! low-multiplicity duplication** (two copies of one extent) and is fair on high-multiplicity
//! duplication (a thousand VMs applying one patch). The scaled figure divides by the covered
//! fraction and is labelled an estimate; at a fraction of 1.0 it is exact. The decision this
//! feeds turns on a figure of roughly ten to fifteen percent, and the content that could reach
//! that on VM disks is the high-multiplicity kind.
//!
//! A node reads only the groups it created, so with `digests` it also returns a short digest of
//! each sampled extent, which a caller can merge across nodes to see duplicates that straddle
//! two of them. The digests are the first eight bytes of the SHA-256: enough to count, not
//! enough to be a content address, and nothing here is one.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use serde_json::{json, Value};

use super::occupancy::{self, Live};
use crate::err::Result;
use crate::extent::{decode_extent, verify_footer, EgroupStore};
use crate::extent_id_map::Rows;
use crate::replicate::throttle::{Clock, TokenBucket};
use crate::replicate::{hex, sha256};

pub const DEFAULT_SAMPLE: f64 = 0.1;
pub const DEFAULT_RATE: u64 = 64 << 20;
pub const DEFAULT_SECONDS: u64 = 40;
/// Digests returned per container when asked, so the answer stays small enough to move.
pub const MAX_DIGESTS: usize = 200_000;

#[derive(Clone, Debug)]
pub struct Options {
    /// Fraction of stored extents to hash, in (0, 1].
    pub sample: f64,
    pub seconds: u64,
    /// Bytes per second read, zero for unlimited.
    pub rate: u64,
    pub digests: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options { sample: DEFAULT_SAMPLE, seconds: DEFAULT_SECONDS, rate: DEFAULT_RATE, digests: false }
    }
}

/// A uniform 64-bit position for a location, deterministic across runs and nodes.
fn position(group: &str, offset: u32) -> u64 {
    // FNV-1a, then a finaliser, so that nearby offsets in one group do not land together.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in group.as_bytes().iter().chain(offset.to_le_bytes().iter()) {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

fn fraction_of(pos: u64) -> f64 {
    pos as f64 / u64::MAX as f64
}

#[derive(Default)]
struct Tally {
    // Exact, from the map alone.
    locations: u64,
    stored_bytes: u64,
    rows: u64,
    logical_bytes: u64,
    // From the sample.
    sampled: u64,
    sampled_bytes: u64,
    unreadable: u64,
    unique_bytes: u64,
    zero_bytes: u64,
    zero_dup_bytes: u64,
    contents: HashMap<[u8; 32], u32>,
    digests: Vec<(String, u64)>,
}

struct Item<'a> {
    group: &'a str,
    offset: u32,
    live: &'a Live,
    container: String,
    pos: u64,
}

/// Estimate, from this node's sealed groups.
pub fn run<D: Rows>(
    db: &D,
    store: &EgroupStore,
    node: &str,
    clock: &dyn Clock,
    opts: &Options,
) -> Result<Value> {
    let occ = occupancy::scan(db)?;
    let groups = occupancy::groups_of(db, node)?;
    let vdisks = occupancy::vdisks(db)?;
    let sample = opts.sample.clamp(0.0001, 1.0);

    let mut tallies: BTreeMap<String, Tally> = BTreeMap::new();
    let mut items: Vec<Item> = Vec::new();
    let mut not_local = 0u64;
    for g in groups.iter().filter(|g| g.state == "sealed") {
        let Some(by_offset) = occ.live.get(&g.id) else { continue };
        if store.locate(&g.id).is_none() {
            not_local += 1;
            continue;
        }
        for (offset, live) in by_offset {
            // The container of the vdisk that wrote the group, else of whoever points at it.
            let container = vdisks
                .get(&g.hint)
                .or_else(|| live.referrers.iter().find_map(|r| vdisks.get(&r.vdisk)))
                .map(|v| v.container.clone())
                .unwrap_or_else(|| "unknown".to_string());
            let t = tallies.entry(container.clone()).or_default();
            t.locations += 1;
            t.stored_bytes += live.length as u64;
            t.rows += live.referrers.len() as u64;
            t.logical_bytes += live.length as u64 * live.referrers.len() as u64;
            items.push(Item { group: &g.id, offset: *offset, live, container, pos: position(&g.id, *offset) });
        }
    }
    items.sort_by_key(|i| i.pos);

    let started = clock.now();
    let deadline = Duration::from_secs(opts.seconds.max(1));
    let mut bucket = TokenBucket::new(opts.rate, opts.rate.max(1));
    let mut covered = sample;
    let mut cut_by_time = false;
    for item in &items {
        if fraction_of(item.pos) > sample {
            break;
        }
        if clock.now().saturating_sub(started) >= deadline {
            covered = fraction_of(item.pos);
            cut_by_time = true;
            break;
        }
        let t = tallies.get_mut(&item.container).expect("counted above");
        t.sampled += 1;
        t.sampled_bytes += item.live.length as u64;
        let framed = match store.read_extent_framed(item.group, item.offset, item.live.length) {
            Ok(f) => f,
            Err(_) => {
                t.unreadable += 1;
                continue;
            }
        };
        let (stored, footer) = framed.split_at(item.live.length as usize);
        let r = &item.live.referrers[0];
        let plain = match verify_footer(stored, footer, r.vdisk_hash, r.idx)
            .and_then(|_| decode_extent(stored, footer))
        {
            Ok(p) => p,
            Err(_) => {
                t.unreadable += 1;
                continue;
            }
        };
        bucket.take(clock, framed.len() as u64);
        let digest = sha256(&plain);
        let zero = plain.iter().all(|b| *b == 0);
        let seen = t.contents.entry(digest).or_insert(0);
        *seen += 1;
        if *seen == 1 {
            t.unique_bytes += item.live.length as u64;
            if zero {
                t.zero_bytes += item.live.length as u64;
            }
        } else if zero {
            t.zero_dup_bytes += item.live.length as u64;
        }
        if opts.digests && t.digests.len() < MAX_DIGESTS {
            t.digests.push((hex(&digest[..8]), item.live.length as u64));
        }
    }
    let exact = covered >= 1.0 - f64::EPSILON && !cut_by_time;

    let mut containers = Vec::new();
    let mut totals = (0u64, 0u64, 0u64, 0u64);
    for (name, t) in &tallies {
        let observed_dup = t.sampled_bytes.saturating_sub(t.unique_bytes);
        let nonzero_dup = observed_dup.saturating_sub(t.zero_dup_bytes);
        let scale = |v: u64| -> u64 {
            if exact || covered <= 0.0 { v } else { ((v as f64) / covered).round().min(t.stored_bytes as f64) as u64 }
        };
        let would_share = scale(observed_dup);
        let would_share_nonzero = scale(nonzero_dup);
        let shared_by_clone = t.logical_bytes.saturating_sub(t.stored_bytes);
        totals.0 += t.stored_bytes;
        totals.1 += would_share;
        totals.2 += would_share_nonzero;
        totals.3 += shared_by_clone;
        let mut entry = json!({
            "container": name,
            "stored_extents": t.locations,
            "stored_bytes": t.stored_bytes,
            "logical_bytes": t.logical_bytes,
            "shared_by_clone_bytes": shared_by_clone,
            "sampled_extents": t.sampled,
            "sampled_bytes": t.sampled_bytes,
            "unreadable_extents": t.unreadable,
            "distinct_contents_in_sample": t.contents.len(),
            "observed_duplicate_bytes": observed_dup,
            "would_share_bytes": would_share,
            "would_share_bytes_excluding_zero": would_share_nonzero,
            "zero_extent_bytes_in_sample": t.zero_bytes + t.zero_dup_bytes,
            "fraction_of_stored": ratio(would_share, t.stored_bytes),
            "fraction_of_stored_excluding_zero": ratio(would_share_nonzero, t.stored_bytes),
        });
        if opts.digests {
            entry["digests"] = json!(t.digests.iter().map(|(h, b)| json!([h, b])).collect::<Vec<_>>());
        }
        containers.push(entry);
    }

    let mut report = json!({
        "node": node,
        "sample_requested": sample,
        "sample_covered": (covered * 10_000.0).round() / 10_000.0,
        "exact": exact,
        "cut_by_time": cut_by_time,
        "groups_not_on_this_node": not_local,
        "containers": containers,
        "total": {
            "stored_bytes": totals.0,
            "would_share_bytes": totals.1,
            "would_share_bytes_excluding_zero": totals.2,
            "shared_by_clone_bytes": totals.3,
            "fraction_of_stored": ratio(totals.1, totals.0),
            "fraction_of_stored_excluding_zero": ratio(totals.2, totals.0),
        },
        "caveats": [
            "Counted over the extent groups this node created; duplicates that straddle two nodes are \
             only visible when digests from every node are merged.",
            "Within a sample a duplicate is seen only if both copies were sampled, so a sample \
             undercounts low-multiplicity duplication. At a covered fraction of 1.0 the figure is exact.",
            "Extent granularity only (1 MiB): duplicates at other alignments, or inside an extent, \
             are invisible to it.",
            "A figure is not a saving: space comes back only through compaction, which does not return \
             anything on replicas today.",
        ],
        "writes": "nothing",
    });
    let line = status_line(&report);
    report["status"] = json!(line);
    Ok(report)
}

fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 { 0.0 } else { ((a as f64 / b as f64) * 10_000.0).round() / 10_000.0 }
}

pub fn status_line(r: &Value) -> String {
    let total = &r["total"];
    let stored = total["stored_bytes"].as_u64().unwrap_or(0);
    let share = total["would_share_bytes_excluding_zero"].as_u64().unwrap_or(0);
    let pct = total["fraction_of_stored_excluding_zero"].as_f64().unwrap_or(0.0) * 100.0;
    format!(
        "dedup estimate ({}): {share} of {stored} stored byte(s), {pct:.1}%, would be shared beyond \
         clone sharing (zero-filled extents excluded); sampled {:.1}% of extents; nothing was written",
        if r["exact"].as_bool().unwrap_or(false) { "exact" } else { "sampled" },
        r["sample_covered"].as_f64().unwrap_or(0.0) * 100.0
    )
}

/// Merge the digests several nodes returned for one container into a cluster-wide figure.
/// Kept here so the arithmetic that valcli repeats has a tested original.
pub fn merge_digests(per_node: &[Vec<(String, u64)>]) -> (u64, u64) {
    let mut seen: HashSet<&str> = HashSet::new();
    let (mut total, mut unique) = (0u64, 0u64);
    for node in per_node {
        for (h, bytes) in node {
            total += bytes;
            if seen.insert(h.as_str()) {
                unique += bytes;
            }
        }
    }
    (total, unique)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::purah::testkit::{data, Rig};
    use crate::replicate::throttle::tests::FakeClock;

    const LEN: usize = 8192;

    /// Four vdisks, each having written two extents; one extent is the same bytes everywhere
    /// (the "patch"), one is unique to its vdisk.
    fn world(name: &str) -> Rig {
        let rig = Rig::new(name);
        for v in ["a", "b", "c", "d"] {
            rig.vdisk(v, "rw", &["n1"]);
            let patch = data(200, LEN);
            let own = data(v.as_bytes()[0], LEN);
            let locs = rig.group(&format!("eg-{v}"), v, &[(0, patch.clone()), (1, own.clone())]);
            rig.point(v, 0, &format!("eg-{v}"), (locs[0].1, locs[0].2), v, &patch);
            rig.point(v, 1, &format!("eg-{v}"), (locs[1].1, locs[1].2), v, &own);
        }
        rig
    }

    fn full(rig: &Rig) -> Value {
        run(&rig.model, &rig.store, "n1", &FakeClock::new(), &Options { sample: 1.0, ..Options::default() })
            .unwrap()
    }

    #[test]
    fn identical_content_in_different_extents_is_counted_once_and_the_rest_would_be_shared() {
        let rig = world("dedup-basic");
        let r = full(&rig);
        let c = &r["containers"][0];
        assert_eq!(r["exact"], true, "{r}");
        assert_eq!(c["stored_extents"], 8);
        assert_eq!(c["distinct_contents_in_sample"], 5, "four unique plus one shared: {c}");
        // Three of the four copies of the patch are redundant: 3 extents' worth.
        let stored_len = c["stored_bytes"].as_u64().unwrap() / 8;
        assert_eq!(c["would_share_bytes"].as_u64().unwrap(), 3 * stored_len, "{c}");
        assert_eq!(c["shared_by_clone_bytes"], 0);
    }

    /// Sharing a clone already provides is not dedup's to claim.
    #[test]
    fn clone_sharing_is_reported_apart_and_not_counted_as_duplication() {
        let rig = Rig::new("dedup-clones");
        rig.vdisk("img", "immutable", &["n1"]);
        let bytes = data(9, LEN);
        let locs = rig.group("eg-img", "img", &[(0, bytes.clone())]);
        for v in ["img", "c1", "c2", "c3"] {
            rig.point(v, 0, "eg-img", (locs[0].1, locs[0].2), "img", &bytes);
        }
        let r = full(&rig);
        let c = &r["containers"][0];
        assert_eq!(c["stored_extents"], 1);
        assert_eq!(c["would_share_bytes"], 0, "{c}");
        assert_eq!(c["shared_by_clone_bytes"].as_u64().unwrap(), 3 * c["stored_bytes"].as_u64().unwrap());
    }

    #[test]
    fn zero_filled_extents_are_split_out_of_the_headline() {
        let rig = Rig::new("dedup-zero");
        rig.vdisk("v", "rw", &["n1"]);
        let z = vec![0u8; LEN];
        let locs = rig.group("eg-z", "v", &[(0, z.clone()), (1, z.clone()), (2, z.clone())]);
        for (i, l) in locs.iter().enumerate() {
            rig.point("v", i as u64, "eg-z", (l.1, l.2), "v", &z);
        }
        let c = &full(&rig)["containers"][0];
        assert!(c["would_share_bytes"].as_u64().unwrap() > 0);
        assert_eq!(c["would_share_bytes_excluding_zero"], 0, "{c}");
    }

    #[test]
    fn the_same_bytes_stored_compressed_and_plain_still_match() {
        let rig = Rig::new("dedup-codec");
        rig.vdisk("v", "rw", &["n1"]);
        let bytes = vec![7u8; LEN];
        let mut eg = rig.store.create("eg-c").unwrap();
        let vh = crate::extent::vdisk_hash("v");
        let (o0, l0, _) = rig.store.append_framed(&mut eg, &bytes, vh, 0, true).unwrap();
        let (o1, l1, _) = rig.store.append_framed(&mut eg, &bytes, vh, 1, false).unwrap();
        rig.store.sync(&mut eg).unwrap();
        rig.model.st.borrow_mut().egroups.insert(
            "eg-c".into(),
            crate::purah::testkit::MGroup {
                state: "sealed".into(), node: "n1".into(), created_ms: 0, size: 0,
                seal_hash: String::new(), hint: "v".into(),
            },
        );
        rig.point("v", 0, "eg-c", (o0, l0), "v", &bytes);
        rig.point("v", 1, "eg-c", (o1, l1), "v", &bytes);
        let c = &full(&rig)["containers"][0];
        assert_eq!(c["distinct_contents_in_sample"], 1, "{c}");
        assert!(c["would_share_bytes"].as_u64().unwrap() > 0);
    }

    /// The pass is a report. The model it reads accepts no write, and it sets nothing.
    #[test]
    fn the_estimator_writes_nothing() {
        let rig = world("dedup-readonly");
        let (before_rows, before_files) = (
            rig.model.st.borrow().block.len(),
            std::fs::read_dir(rig.dir.join("egroups")).unwrap().count(),
        );
        let r = full(&rig);
        assert_eq!(r["writes"], "nothing");
        assert!(rig.model.st.borrow().cas_log.is_empty());
        assert_eq!(rig.model.st.borrow().block.len(), before_rows);
        assert_eq!(std::fs::read_dir(rig.dir.join("egroups")).unwrap().count(), before_files);
    }

    #[test]
    fn a_sample_covers_the_fraction_it_says_and_is_labelled_inexact() {
        let rig = Rig::new("dedup-sample");
        rig.vdisk("v", "rw", &["n1"]);
        for g in 0..40u8 {
            let b = data(g % 4, LEN);
            let id = format!("eg-s{g}");
            let l = rig.group(&id, "v", &[(0, b.clone())]);
            rig.point("v", g as u64, &id, (l[0].1, l[0].2), "v", &b);
        }
        let r = run(&rig.model, &rig.store, "n1", &FakeClock::new(),
                    &Options { sample: 0.5, ..Options::default() })
            .unwrap();
        assert_eq!(r["exact"], false);
        let c = &r["containers"][0];
        assert_eq!(c["stored_extents"], 40, "map-level counts are exact whatever the sample");
        let sampled = c["sampled_extents"].as_u64().unwrap();
        assert!(sampled > 0 && sampled < 40, "{sampled}");
        assert!(r["status"].as_str().unwrap().contains("sampled"));
    }

    #[test]
    fn a_pass_cut_short_by_its_budget_reports_what_it_covered() {
        let rig = world("dedup-budget");
        let clock = FakeClock::new();
        // Pace to a crawl so the second item finds the budget spent.
        let r = run(&rig.model, &rig.store, "n1", &clock,
                    &Options { sample: 1.0, seconds: 1, rate: 100, digests: false })
            .unwrap();
        assert_eq!(r["cut_by_time"], true, "{r}");
        assert_eq!(r["exact"], false);
        assert!(r["sample_covered"].as_f64().unwrap() < 1.0);
    }

    #[test]
    fn digests_merge_across_nodes_into_what_either_alone_would_miss() {
        let a = vec![("aa".to_string(), 10u64), ("bb".to_string(), 10)];
        let b = vec![("aa".to_string(), 10u64), ("cc".to_string(), 10)];
        let (total, unique) = merge_digests(&[a, b]);
        assert_eq!((total, unique), (40, 30));
    }

    #[test]
    fn digests_are_returned_only_when_asked() {
        let rig = world("dedup-digests");
        assert!(full(&rig)["containers"][0].get("digests").is_none());
        let r = run(&rig.model, &rig.store, "n1", &FakeClock::new(),
                    &Options { sample: 1.0, digests: true, ..Options::default() })
            .unwrap();
        assert_eq!(r["containers"][0]["digests"].as_array().unwrap().len(), 8);
    }

    #[test]
    fn an_unreadable_extent_is_counted_and_does_not_fail_the_pass() {
        let rig = world("dedup-unreadable");
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new().write(true).open(rig.store.path_for("eg-a")).unwrap();
            f.seek(SeekFrom::Start(3)).unwrap();
            f.write_all(b"X").unwrap();
        }
        let r = full(&rig);
        assert_eq!(r["containers"][0]["unreadable_extents"], 1, "{r}");
    }
}
