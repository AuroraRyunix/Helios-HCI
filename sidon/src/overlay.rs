//! The overlay: which journal record currently holds the newest bytes for each range.
//!
//! Kept as a map of **non-overlapping** segments. Overlap is resolved at insert time
//! rather than at read time, which is the decision this whole module turns on: a reader
//! that had to reconcile competing segments would need their ordering to be total and
//! correct on every path, and "the newest write wins" would become a property of the
//! read code rather than a property of the data structure. Here, a segment's presence
//! *is* the claim that it is newest for its range.
//!
//! Only positions and lengths live here. The bytes stay in the journal file, so a vdisk
//! that has absorbed gigabytes of writes between drains costs index, not payload.

use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Seg {
    pub start: u64,
    pub len: u32,
    /// Byte position of these bytes inside the journal file.
    pub data_pos: u64,
}

impl Seg {
    pub fn end(&self) -> u64 {
        self.start + self.len as u64
    }
}

#[derive(Default)]
pub struct Overlay {
    segs: BTreeMap<u64, Seg>,
}

impl Overlay {
    pub fn new() -> Self {
        Overlay { segs: BTreeMap::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.segs.is_empty()
    }

    pub fn len(&self) -> usize {
        self.segs.len()
    }

    pub fn clear(&mut self) {
        self.segs.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item = &Seg> {
        self.segs.values()
    }

    /// Record that `[start, start+len)` now lives at `data_pos` in the journal, evicting
    /// whatever previously covered any part of that range.
    pub fn insert(&mut self, start: u64, len: u32, data_pos: u64) {
        if len == 0 {
            return;
        }
        let end = start + len as u64;

        // Everything that could overlap starts at or after the last segment beginning at
        // or before `start`. Anything earlier than that ends before it, by the invariant.
        let first = self
            .segs
            .range(..=start)
            .next_back()
            .map(|(k, _)| *k)
            .unwrap_or(0);

        let touched: Vec<u64> = self
            .segs
            .range(first..end)
            .filter(|(_, s)| s.end() > start)
            .map(|(k, _)| *k)
            .collect();

        for key in touched {
            let old = self.segs.remove(&key).expect("key came from this map");
            // Left remainder: the part of the old segment before the new write.
            if old.start < start {
                self.segs.insert(
                    old.start,
                    Seg {
                        start: old.start,
                        len: (start - old.start) as u32,
                        data_pos: old.data_pos,
                    },
                );
            }
            // Right remainder: the part after. Its bytes are further into the same
            // journal record, so the position advances by exactly what was consumed.
            if old.end() > end {
                let skip = end - old.start;
                self.segs.insert(
                    end,
                    Seg {
                        start: end,
                        len: (old.end() - end) as u32,
                        data_pos: old.data_pos + skip,
                    },
                );
            }
        }

        self.segs.insert(start, Seg { start, len, data_pos });
    }

    /// Every segment overlapping `[start, end)`, in ascending order.
    pub fn overlapping(&self, start: u64, end: u64) -> Vec<Seg> {
        if end <= start {
            return Vec::new();
        }
        let first = self.segs.range(..=start).next_back().map(|(k, _)| *k).unwrap_or(0);
        self.segs
            .range(first..end)
            .map(|(_, s)| *s)
            .filter(|s| s.end() > start)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn covered(o: &Overlay) -> Vec<(u64, u64, u64)> {
        o.iter().map(|s| (s.start, s.len as u64, s.data_pos)).collect()
    }

    #[test]
    fn disjoint_writes_coexist() {
        let mut o = Overlay::new();
        o.insert(0, 512, 100);
        o.insert(4096, 512, 200);
        assert_eq!(covered(&o), vec![(0, 512, 100), (4096, 512, 200)]);
    }

    #[test]
    fn a_full_overwrite_replaces() {
        let mut o = Overlay::new();
        o.insert(0, 512, 100);
        o.insert(0, 512, 900);
        assert_eq!(covered(&o), vec![(0, 512, 900)]);
    }

    #[test]
    fn a_wider_write_swallows_what_it_covers() {
        let mut o = Overlay::new();
        o.insert(100, 10, 1);
        o.insert(200, 10, 2);
        o.insert(300, 10, 3);
        o.insert(50, 400, 999);
        assert_eq!(covered(&o), vec![(50, 400, 999)]);
    }

    #[test]
    fn a_narrow_write_splits_and_keeps_both_remainders() {
        let mut o = Overlay::new();
        o.insert(0, 1000, 500); // journal payload at 500..1500
        o.insert(400, 100, 7000);
        // Left keeps its original position; right advances by what the hole consumed.
        assert_eq!(
            covered(&o),
            vec![(0, 400, 500), (400, 100, 7000), (500, 500, 1000)]
        );
    }

    #[test]
    fn partial_overlaps_trim_the_right_side() {
        let mut o = Overlay::new();
        o.insert(0, 100, 10);
        o.insert(50, 100, 20);
        assert_eq!(covered(&o), vec![(0, 50, 10), (50, 100, 20)]);
    }

    #[test]
    fn partial_overlaps_trim_the_left_side() {
        let mut o = Overlay::new();
        o.insert(50, 100, 20);
        o.insert(0, 60, 10);
        // The survivor's payload starts 10 bytes into the older record.
        assert_eq!(covered(&o), vec![(0, 60, 10), (60, 90, 30)]);
    }

    #[test]
    fn segments_never_overlap_under_random_writes() {
        // A deterministic pseudo-random soak: the invariant the read path depends on is
        // that no two segments intersect, whatever order writes arrive in.
        let mut o = Overlay::new();
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        for i in 0..2000u64 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let start = state % 8192;
            let len = 1 + (state >> 32) % 512;
            o.insert(start, len as u32, i * 4096);
        }
        let segs: Vec<Seg> = o.iter().copied().collect();
        for w in segs.windows(2) {
            assert!(
                w[0].end() <= w[1].start,
                "segments {:?} and {:?} overlap",
                w[0],
                w[1]
            );
            assert!(w[0].len > 0);
        }
    }

    #[test]
    fn overlapping_query_finds_straddlers() {
        let mut o = Overlay::new();
        o.insert(0, 4096, 0);
        let hits = o.overlapping(1000, 1004);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].start, 0);
        assert!(o.overlapping(8192, 9000).is_empty());
        assert!(o.overlapping(100, 100).is_empty());
    }
}
