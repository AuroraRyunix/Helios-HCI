//! Per-extent-group access accounting: how often each group is read and written, and
//! when it was last touched.
//!
//! This is the thing a tiering decision is made from. Without it, "keep the hot extents
//! on the fast disk" has no operand -- the placement design in `docs/dfs/multi_disk.md`
//! can pick a disk by free space, which needs no history, and cannot pick one by
//! temperature, which needs this.
//!
//! Three properties decide the shape, and all three are about not making the guest pay.
//!
//! 1. **Nothing here touches Hydra.** Recording an access is a hash lookup and four
//!    integer adds under a mutex that is never held across a syscall. A metadata round
//!    trip per read would be a write to the map on the read path, which is the one thing
//!    the whole design forbids.
//! 2. **The counters are approximate, and that is the trade, not a defect.** They live in
//!    this process until a background thread flushes them, so a crash loses everything
//!    since the last flush and resets the window. Heat data decides where to *put* a copy
//!    of data that exists either way; being wrong about it costs a misplaced extent group
//!    and a later migration, never a byte. Paying for exactness would mean paying on the
//!    read path, which is the only place the cost would be unaffordable.
//! 3. **It is about extent groups, not part of them.** A sealed extent group is immutable
//!    and its footer format is fixed; nothing here is written into one. That is also why
//!    this could be added at all rather than being a format migration.
//!
//! The counters are **totals since a window opened**, not since the group was created and
//! not since the last flush. The window opens when this daemon starts, and `since_ms`
//! records when, so a reader can turn a total into a rate. Per-flush deltas would make
//! every reading a sixty-second spike; lifetime totals would need a read-modify-write
//! against Hydra on every flush, which is the counter problem this avoids entirely -- the
//! flush writes absolute values, so a lost or duplicated flush changes nothing.

use std::collections::HashMap;
use std::sync::Mutex;

/// What is known about one extent group's accesses, within one window, as seen by one
/// node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub reads: u64,
    pub writes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub last_read_ms: i64,
    pub last_write_ms: i64,
}

impl Counts {
    /// The later of the two timestamps, or 0 if neither has happened.
    ///
    /// Ranking asks "when was this last touched", and a group that is read constantly and
    /// never written is as hot as one that is written constantly -- a tiering pass that
    /// only looked at writes would spill a golden image nobody has written since it was
    /// made and every VM on the cluster reads.
    pub fn last_access_ms(&self) -> i64 {
        self.last_read_ms.max(self.last_write_ms)
    }

    pub fn accesses(&self) -> u64 {
        self.reads.saturating_add(self.writes)
    }

    pub fn merge(&mut self, other: &Counts) {
        self.reads = self.reads.saturating_add(other.reads);
        self.writes = self.writes.saturating_add(other.writes);
        self.bytes_read = self.bytes_read.saturating_add(other.bytes_read);
        self.bytes_written = self.bytes_written.saturating_add(other.bytes_written);
        self.last_read_ms = self.last_read_ms.max(other.last_read_ms);
        self.last_write_ms = self.last_write_ms.max(other.last_write_ms);
    }
}

/// One flush's worth of rows, plus the window they were counted over.
pub struct Sample {
    pub since_ms: i64,
    pub until_ms: i64,
    /// Extent groups whose first access arrived while the table was at capacity, and were
    /// therefore not counted at all. Reported rather than silently absent: an unobserved
    /// extent group and a cold one look identical in the table, and only this number can
    /// tell the operator which they are looking at.
    pub dropped: u64,
    pub rows: Vec<(String, Counts)>,
}

/// The in-memory accumulator, shared by every vdisk attached to this node.
///
/// One lock for the whole node rather than one per vdisk. The critical section is a hash
/// lookup and some addition, with no allocation after an extent group's first access and
/// no I/O ever, so it is shorter than the per-vdisk lock the caller is already holding.
/// Per-vdisk accumulators would have to be merged at flush time anyway, because an extent
/// group is shared between a parent and every snapshot of it -- the heat of a golden
/// image is the heat of all the clones reading it, and splitting it per vdisk would hide
/// exactly that.
pub struct AccessLog {
    inner: Mutex<Inner>,
    /// How many extent groups may be tracked at once.
    ///
    /// A guard, not a policy. One node's inventory is bounded by its disks -- an extent
    /// group is 4 MiB, so a 300 GB disk holds about 75,000 of them -- and a few hundred
    /// thousand `Counts` is a few megabytes. The cap exists so that a bug elsewhere
    /// producing unbounded extent-group ids cannot turn heat accounting into the reason a
    /// node runs out of memory, which would be a curator killing the data path.
    capacity: usize,
}

struct Inner {
    counts: HashMap<String, Counts>,
    since_ms: i64,
    dropped: u64,
}

impl AccessLog {
    pub fn new(capacity: usize, now_ms: i64) -> AccessLog {
        AccessLog {
            inner: Mutex::new(Inner {
                counts: HashMap::new(),
                since_ms: now_ms,
                dropped: 0,
            }),
            capacity: capacity.max(1),
        }
    }

    /// A read served from this extent group.
    ///
    /// Counted per extent, not per guest read: one guest read spanning three extents in
    /// two groups is two accesses, which is the number that matters to whoever is
    /// deciding which disk a group should sit on.
    pub fn record_read(&self, egroup_id: &str, bytes: u64, now_ms: i64) {
        self.with(egroup_id, |c| {
            c.reads = c.reads.saturating_add(1);
            c.bytes_read = c.bytes_read.saturating_add(bytes);
            c.last_read_ms = c.last_read_ms.max(now_ms);
        });
    }

    /// An extent appended into this extent group by a drain.
    ///
    /// The guest's write path never touches an extent group -- it reaches the journal and
    /// is acknowledged -- so a group's write count is a count of drained extents landing
    /// in it. That is the honest meaning and it is the useful one: it is the number that
    /// says which groups are being appended to now, and those are the ones a tiering pass
    /// must not move.
    pub fn record_write(&self, egroup_id: &str, bytes: u64, now_ms: i64) {
        self.with(egroup_id, |c| {
            c.writes = c.writes.saturating_add(1);
            c.bytes_written = c.bytes_written.saturating_add(bytes);
            c.last_write_ms = c.last_write_ms.max(now_ms);
        });
    }

    fn with(&self, egroup_id: &str, f: impl FnOnce(&mut Counts)) {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            // A poisoned heat counter must not take down the data path. The panic that
            // poisoned it has already been reported by whoever panicked; losing access
            // statistics is the correct amount of consequence for it.
            Err(_) => return,
        };
        // The known case first and returning, rather than one `match` over `get_mut`: the
        // mutable borrow a `match` takes lives for the whole match, so the arm that needs to
        // insert cannot. This shape is also the common one -- an extent group's first access
        // happens once and every later one takes the early return.
        if let Some(c) = inner.counts.get_mut(egroup_id) {
            f(c);
            return;
        }
        if inner.counts.len() >= self.capacity {
            inner.dropped = inner.dropped.saturating_add(1);
            return;
        }
        let mut c = Counts::default();
        f(&mut c);
        inner.counts.insert(egroup_id.to_string(), c);
    }

    /// Everything counted so far, without resetting anything.
    ///
    /// Non-destructive on purpose. The flush writes absolute totals, so a flush that fails
    /// or is retried costs nothing -- the next one carries the same numbers plus whatever
    /// happened since. Draining the table into the flush would make a failed write a hole
    /// in the history and a duplicated one a double count, which is the whole class of
    /// problem that made distributed refcounts unacceptable here (D-8).
    pub fn sample(&self, now_ms: i64) -> Sample {
        let inner = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut rows: Vec<(String, Counts)> =
            inner.counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
        // Sorted so that two flushes of the same table produce the same statements, which
        // is what makes a diff of the flush readable and a test of it deterministic.
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        Sample {
            since_ms: inner.since_ms,
            until_ms: now_ms,
            dropped: inner.dropped,
            rows,
        }
    }

    /// Forget one extent group, because it no longer exists.
    ///
    /// Called when Purah reclaims a group. Without it a reclaimed id would keep its entry
    /// for the life of the daemon and keep being flushed, which is how a table of facts
    /// about extent groups fills up with facts about extent groups that are gone.
    pub fn forget(&self, egroup_id: &str) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.counts.remove(egroup_id);
        }
    }

    /// How many first accesses were thrown away because the table was full.
    ///
    /// A separate accessor rather than reading it off a `Sample`, so that reporting it does
    /// not have to copy every row to get at one number.
    pub fn dropped(&self) -> u64 {
        self.inner.lock().map(|i| i.dropped).unwrap_or(0)
    }

    pub fn tracked(&self) -> usize {
        self.inner.lock().map(|i| i.counts.len()).unwrap_or(0)
    }
}

/// How hot one extent group is, as one number, from totals and the window they cover.
///
/// Accesses per hour, discounted by how long ago the last one was:
///
/// ```text
///     rate = accesses * 3_600_000 / window_ms
///     heat = rate / (1 + idle_hours)
/// ```
///
/// Deliberately arithmetic an operator can redo by hand from the columns in the table,
/// rather than an exponential decay with a tuned half-life. The ranking this feeds is a
/// *suggestion* about where to put a copy of data that is safe either way, so an
/// explainable formula beats a defensible one: when the pass proposes moving something
/// surprising, the question "why does it think that is cold" has to be answerable from the
/// row.
///
/// A window of zero or less scores zero rather than dividing by it: it means the row was
/// written in the same millisecond its window opened, so there is no rate to compute yet.
/// Scoring it infinite would make the newest extent group on the node the hottest thing on
/// it, every time a daemon restarted.
pub fn heat_score(counts: &Counts, since_ms: i64, updated_at_ms: i64, now_ms: i64) -> f64 {
    let window_ms = updated_at_ms.saturating_sub(since_ms);
    if window_ms <= 0 {
        return 0.0;
    }
    let rate = counts.accesses() as f64 * 3_600_000.0 / window_ms as f64;
    let idle_ms = now_ms.saturating_sub(counts.last_access_ms()).max(0);
    let idle_hours = idle_ms as f64 / 3_600_000.0;
    rate / (1.0 + idle_hours)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_and_a_write_are_counted_separately_and_both_set_the_clock() {
        let log = AccessLog::new(16, 1_000);
        log.record_read("eg-a", 4096, 2_000);
        log.record_write("eg-a", 8192, 3_000);
        let sample = log.sample(4_000);
        assert_eq!(sample.rows.len(), 1);
        let (id, c) = &sample.rows[0];
        assert_eq!(id, "eg-a");
        assert_eq!((c.reads, c.writes), (1, 1));
        assert_eq!((c.bytes_read, c.bytes_written), (4096, 8192));
        assert_eq!(c.last_read_ms, 2_000);
        assert_eq!(c.last_write_ms, 3_000);
        assert_eq!(c.last_access_ms(), 3_000);
        assert_eq!((sample.since_ms, sample.until_ms), (1_000, 4_000));
    }

    /// The property: sampling does not reset the counters.
    ///
    /// The flush writes absolute totals, so a flush that fails must be able to be retried
    /// and must not have taken the numbers with it. A destructive sample turns one
    /// unreachable Daruk into a permanent hole in the history.
    #[test]
    fn sampling_leaves_the_counters_where_they_were() {
        let log = AccessLog::new(16, 0);
        log.record_read("eg-a", 1, 10);
        let first = log.sample(20);
        let second = log.sample(30);
        assert_eq!(first.rows[0].1.reads, 1);
        assert_eq!(second.rows[0].1.reads, 1);
        log.record_read("eg-a", 1, 40);
        assert_eq!(log.sample(50).rows[0].1.reads, 2);
        // And the window never moves, because the totals never restart.
        assert_eq!(log.sample(50).since_ms, 0);
    }

    /// At capacity, an unknown extent group is dropped and *counted*, never allowed to
    /// grow the table.
    ///
    /// The number is the point. An extent group with no row and an extent group that was
    /// never observed look identical to a ranking pass, so the only thing that can tell an
    /// operator "this table is not describing your whole node" is this counter.
    #[test]
    fn a_full_table_drops_new_groups_visibly_and_keeps_counting_known_ones() {
        let log = AccessLog::new(2, 0);
        log.record_read("eg-a", 1, 1);
        log.record_read("eg-b", 1, 2);
        log.record_read("eg-c", 1, 3);
        let sample = log.sample(4);
        assert_eq!(sample.rows.len(), 2);
        assert_eq!(sample.dropped, 1);
        assert!(sample.rows.iter().all(|(id, _)| id != "eg-c"));
        // A group already in the table is still counted while the table is full.
        log.record_read("eg-a", 1, 5);
        assert_eq!(log.sample(6).rows[0].1.reads, 2);
    }

    #[test]
    fn forgetting_a_reclaimed_group_removes_it_from_the_next_flush() {
        let log = AccessLog::new(16, 0);
        log.record_read("eg-a", 1, 1);
        log.record_read("eg-b", 1, 1);
        log.forget("eg-a");
        let ids: Vec<String> = log.sample(2).rows.into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec!["eg-b".to_string()]);
    }

    /// Counters are totals over a window, so a short window and a long one with the same
    /// total are not the same temperature.
    #[test]
    fn heat_is_a_rate_rather_than_a_total() {
        let c = Counts { reads: 60, last_read_ms: 3_600_000, ..Counts::default() };
        // 60 accesses in one hour, measured at the instant of the last one.
        let hour = heat_score(&c, 0, 3_600_000, 3_600_000);
        // The same 60 accesses spread over ten hours is a tenth as hot.
        let c_slow = Counts { reads: 60, last_read_ms: 36_000_000, ..Counts::default() };
        let ten_hours = heat_score(&c_slow, 0, 36_000_000, 36_000_000);
        assert!((hour - 60.0).abs() < 1e-6, "{hour}");
        assert!((ten_hours - 6.0).abs() < 1e-6, "{ten_hours}");
    }

    /// Idleness cools a group even though its totals never change.
    ///
    /// This is the half that makes the ranking usable for tiering. A group read a thousand
    /// times last week and not since is not hot, and a formula built only on totals would
    /// insist that it is -- and would keep insisting for as long as the daemon stayed up.
    #[test]
    fn a_group_that_has_not_been_touched_for_hours_cools_off() {
        let c = Counts { reads: 60, last_read_ms: 3_600_000, ..Counts::default() };
        let fresh = heat_score(&c, 0, 3_600_000, 3_600_000);
        let three_hours_later = heat_score(&c, 0, 3_600_000, 3_600_000 + 3 * 3_600_000);
        assert!(three_hours_later < fresh);
        // 60/hour, idle three hours: 60 / (1 + 3).
        assert!((three_hours_later - 15.0).abs() < 1e-6, "{three_hours_later}");
    }

    /// A row whose window has no duration scores zero, not infinity.
    ///
    /// Every daemon restart creates one of these for the first extent group it touches,
    /// and a divide-by-zero reading as "hottest on the node" would make a restart look
    /// like a workload.
    #[test]
    fn a_zero_length_window_is_not_infinitely_hot() {
        let c = Counts { reads: 5, last_read_ms: 1_000, ..Counts::default() };
        assert_eq!(heat_score(&c, 1_000, 1_000, 1_000), 0.0);
        assert_eq!(heat_score(&c, 2_000, 1_000, 2_000), 0.0);
    }

    /// Two nodes' rows for one extent group add up, and the clock takes the later of them.
    ///
    /// An extent group is shared by a parent and every snapshot of it, and reads of it can
    /// be served on any node holding a replica. Ranking one node's view of a golden image
    /// would say it is cold on the node where no clone happens to be attached.
    #[test]
    fn per_node_rows_merge_into_one_temperature() {
        let mut a = Counts { reads: 10, last_read_ms: 500, ..Counts::default() };
        let b = Counts { reads: 4, writes: 1, last_read_ms: 900, last_write_ms: 950,
                         ..Counts::default() };
        a.merge(&b);
        assert_eq!(a.accesses(), 15);
        assert_eq!(a.last_access_ms(), 950);
    }
}
