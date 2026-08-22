//! The ack journal, and the verdict it makes possible.
//!
//! Kept by Ganon, on the machine driving the test, never on the system under test. Every
//! write is logged *issued* before the request goes out and *acked* after the reply comes
//! back, so a write that was in flight when the target died is known to be exactly that:
//! either outcome is legal, and a verifier that did not know this would either miss real
//! losses or invent false ones.
//!
//! The rule the verdict applies, per block:
//!
//! - the newest **acked** generation must be what a read returns, **or**
//! - a generation that was **issued but never acked** may appear instead, **or**
//! - if nothing was ever acked, zeroes are legal.
//!
//! Anything else -- an older generation than one already acknowledged, a generation never
//! issued at all, a foreign offset, a failed checksum -- is a violation with evidence.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use crate::stamp::{self, Read as StampRead};

#[derive(Default)]
pub struct AckJournal {
    acked: HashMap<u64, u64>,
    in_flight: HashMap<u64, Vec<u64>>,
    file: Option<File>,
}

#[derive(Debug)]
pub struct Violation {
    pub block: u64,
    pub offset: u64,
    pub detail: String,
}

#[derive(Debug, Default)]
pub struct Verdict {
    pub blocks_checked: u64,
    pub blocks_acked: u64,
    pub blocks_from_inflight: u64,
    pub blocks_zeroed: u64,
    pub violations: Vec<Violation>,
}

impl Verdict {
    pub fn clean(&self) -> bool {
        self.violations.is_empty()
    }
}

impl AckJournal {
    /// Open (and replay) a journal file so a scenario can survive Ganon itself being
    /// restarted between the write phase and the verify phase.
    pub fn open(path: &Path) -> Result<AckJournal, String> {
        let mut j = AckJournal::default();
        if path.exists() {
            let f = File::open(path).map_err(|e| format!("ack journal: {e}"))?;
            for line in BufReader::new(f).lines() {
                let line = line.map_err(|e| e.to_string())?;
                let mut parts = line.split_whitespace();
                match (parts.next(), parts.next(), parts.next()) {
                    (Some("I"), Some(b), Some(g)) => {
                        if let (Ok(b), Ok(g)) = (b.parse(), g.parse()) {
                            j.in_flight.entry(b).or_default().push(g);
                        }
                    }
                    (Some("A"), Some(b), Some(g)) => {
                        if let (Ok(b), Ok(g)) = (b.parse::<u64>(), g.parse::<u64>()) {
                            j.acked.insert(b, g);
                            if let Some(v) = j.in_flight.get_mut(&b) {
                                v.retain(|x| *x != g);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("ack journal: {e}"))?;
        j.file = Some(f);
        Ok(j)
    }

    pub fn in_memory() -> AckJournal {
        AckJournal::default()
    }

    /// Log before the request leaves. `sync_all` on every line is deliberate: the whole
    /// value of this file is that it is more durable than the thing being tested.
    pub fn issue(&mut self, block: u64, generation: u64) {
        self.in_flight.entry(block).or_default().push(generation);
        if let Some(f) = self.file.as_mut() {
            let _ = writeln!(f, "I {block} {generation}");
            let _ = f.sync_all();
        }
    }

    pub fn ack(&mut self, block: u64, generation: u64) {
        self.acked.insert(block, generation);
        if let Some(v) = self.in_flight.get_mut(&block) {
            v.retain(|x| *x != generation);
        }
        if let Some(f) = self.file.as_mut() {
            let _ = writeln!(f, "A {block} {generation}");
            let _ = f.sync_all();
        }
    }

    pub fn acked_generation(&self, block: u64) -> Option<u64> {
        self.acked.get(&block).copied()
    }

    pub fn was_issued(&self, block: u64, generation: u64) -> bool {
        self.in_flight
            .get(&block)
            .map(|v| v.contains(&generation))
            .unwrap_or(false)
    }

    /// Judge one block that was read back.
    pub fn judge(&self, block: u64, offset: u64, data: &[u8], verdict: &mut Verdict) {
        verdict.blocks_checked += 1;
        let acked = self.acked_generation(block);
        match stamp::parse(data, offset) {
            StampRead::Zeroed => {
                if let Some(g) = acked {
                    verdict.violations.push(Violation {
                        block,
                        offset,
                        detail: format!(
                            "block reads as never-written, but generation {g} was acknowledged \
                             to the writer. An acknowledged write is gone."
                        ),
                    });
                } else {
                    verdict.blocks_zeroed += 1;
                }
            }
            StampRead::Damaged(why) => verdict.violations.push(Violation {
                block,
                offset,
                detail: format!("unreadable or corrupt: {why}"),
            }),
            StampRead::Valid(s) => {
                if Some(s.generation) == acked {
                    verdict.blocks_acked += 1;
                } else if self.was_issued(block, s.generation) {
                    // A write that was in flight when the world ended. Legal.
                    verdict.blocks_from_inflight += 1;
                } else if let Some(g) = acked {
                    if s.generation < g {
                        verdict.violations.push(Violation {
                            block,
                            offset,
                            detail: format!(
                                "time travel: generation {} was returned after generation {g} \
                                 had been acknowledged",
                                s.generation
                            ),
                        });
                    } else {
                        verdict.violations.push(Violation {
                            block,
                            offset,
                            detail: format!(
                                "generation {} was never issued for this block (newest \
                                 acknowledged is {g})",
                                s.generation
                            ),
                        });
                    }
                } else {
                    verdict.violations.push(Violation {
                        block,
                        offset,
                        detail: format!(
                            "generation {} appeared although nothing was ever written here",
                            s.generation
                        ),
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_for(gen: u64, offset: u64) -> Vec<u8> {
        stamp::build(0xABCD, offset, gen, 0)
    }

    #[test]
    fn the_acked_generation_is_accepted() {
        let mut j = AckJournal::in_memory();
        j.issue(0, 1);
        j.ack(0, 1);
        let mut v = Verdict::default();
        j.judge(0, 0, &block_for(1, 0), &mut v);
        assert!(v.clean(), "{:?}", v.violations);
        assert_eq!(v.blocks_acked, 1);
    }

    #[test]
    fn an_unacked_in_flight_generation_is_also_legal() {
        let mut j = AckJournal::in_memory();
        j.issue(0, 1);
        j.ack(0, 1);
        j.issue(0, 2); // never acked: the crash happened here
        let mut v = Verdict::default();
        j.judge(0, 0, &block_for(2, 0), &mut v);
        assert!(v.clean(), "{:?}", v.violations);
        assert_eq!(v.blocks_from_inflight, 1);
        // ...and so is the older, acknowledged one.
        let mut v2 = Verdict::default();
        j.judge(0, 0, &block_for(1, 0), &mut v2);
        assert!(v2.clean(), "{:?}", v2.violations);
    }

    #[test]
    fn losing_an_acknowledged_write_is_a_violation() {
        let mut j = AckJournal::in_memory();
        j.issue(0, 1);
        j.ack(0, 1);
        let mut v = Verdict::default();
        j.judge(0, 0, &vec![0u8; stamp::BLOCK], &mut v);
        assert_eq!(v.violations.len(), 1);
        assert!(v.violations[0].detail.contains("acknowledged write is gone"));
    }

    #[test]
    fn time_travel_is_a_violation() {
        let mut j = AckJournal::in_memory();
        j.issue(0, 1);
        j.ack(0, 1);
        j.issue(0, 2);
        j.ack(0, 2);
        let mut v = Verdict::default();
        j.judge(0, 0, &block_for(1, 0), &mut v);
        assert_eq!(v.violations.len(), 1);
        assert!(v.violations[0].detail.contains("time travel"), "{:?}", v.violations);
    }

    #[test]
    fn a_generation_nobody_wrote_is_a_violation() {
        let mut j = AckJournal::in_memory();
        j.issue(0, 1);
        j.ack(0, 1);
        let mut v = Verdict::default();
        j.judge(0, 0, &block_for(99, 0), &mut v);
        assert_eq!(v.violations.len(), 1);
        assert!(v.violations[0].detail.contains("never issued"));
    }

    #[test]
    fn zeroes_where_nothing_was_written_are_fine() {
        let j = AckJournal::in_memory();
        let mut v = Verdict::default();
        j.judge(7, 7 * 4096, &vec![0u8; stamp::BLOCK], &mut v);
        assert!(v.clean());
        assert_eq!(v.blocks_zeroed, 1);
    }

    #[test]
    fn a_journal_survives_a_restart_of_the_harness() {
        let mut p = std::env::temp_dir();
        p.push(format!("ganon-ackj-{}", std::process::id()));
        let _ = std::fs::remove_file(&p);
        {
            let mut j = AckJournal::open(&p).unwrap();
            j.issue(3, 1);
            j.ack(3, 1);
            j.issue(3, 2);
        }
        let j = AckJournal::open(&p).unwrap();
        assert_eq!(j.acked_generation(3), Some(1));
        assert!(j.was_issued(3, 2));
        assert!(!j.was_issued(3, 1), "an acked generation is no longer in flight");
        std::fs::remove_file(&p).ok();
    }
}
