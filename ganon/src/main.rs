//! Ganon: the fault-injection harness.
//!
//! Built before the filesystem it gates, and calibrated against DRBD first. If Ganon
//! reports a violation against DRBD protocol C, then either Ganon is wrong or -- more
//! interestingly -- DRBD as deployed here is; either way the harness is measured against
//! ground truth before it is allowed to judge new code.
//!
//! Usage:
//!
//! ```text
//! ganon write   --target <spec> --journal <path> --blocks N --generation G [--seed S]
//! ganon verify  --target <spec> --journal <path> --blocks N
//! ganon soak    --target <spec> --journal <path> --blocks N --rounds R
//! ganon corrupt --file <path> --offset N [--bytes N]
//! ```
//!
//! `<spec>` is a device path (`/dev/drbd/by-res/x/0`) or `nbd:<socket>:<export>`. A
//! scenario never knows which it got.
//!
//! Process-level injection -- kill, SIGSTOP, partition, kernel death -- is driven by the
//! runner around Ganon rather than by Ganon itself, so that the same verdict engine
//! judges a crash it caused and a crash it merely observed.

mod journal;
mod stamp;
mod target;

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use journal::{AckJournal, Verdict};
use target::Block;

const VDISK_TAG: u64 = 0x5349_444F_4E00_0001;

struct Args {
    cmd: String,
    target: Option<String>,
    journal: Option<PathBuf>,
    file: Option<PathBuf>,
    blocks: u64,
    generation: u64,
    rounds: u64,
    seed: u64,
    offset: u64,
    bytes: usize,
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let cmd = it.next().ok_or_else(|| "no subcommand".to_string())?;
    let mut a = Args {
        cmd,
        target: None,
        journal: None,
        file: None,
        blocks: 256,
        generation: 1,
        rounds: 4,
        seed: 0x2545_F491_4F6C_DD1D,
        offset: 0,
        bytes: 1,
    };
    while let Some(flag) = it.next() {
        let val = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--target" => a.target = Some(val),
            "--journal" => a.journal = Some(PathBuf::from(val)),
            "--file" => a.file = Some(PathBuf::from(val)),
            "--blocks" => a.blocks = val.parse().map_err(|_| "--blocks")?,
            "--generation" => a.generation = val.parse().map_err(|_| "--generation")?,
            "--rounds" => a.rounds = val.parse().map_err(|_| "--rounds")?,
            "--seed" => a.seed = val.parse().map_err(|_| "--seed")?,
            "--offset" => a.offset = val.parse().map_err(|_| "--offset")?,
            "--bytes" => a.bytes = val.parse().map_err(|_| "--bytes")?,
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(a)
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn open_journal(a: &Args) -> Result<AckJournal, String> {
    match &a.journal {
        Some(p) => AckJournal::open(p),
        None => Err("--journal is required".to_string()),
    }
}

fn open_target(a: &Args) -> Result<Box<dyn Block>, String> {
    let spec = a.target.as_ref().ok_or_else(|| "--target is required".to_string())?;
    target::open(spec)
}

/// Write one stamped generation across `blocks`, logging issue and ack around every
/// request. Write errors are *not* failures: a target that dies mid-run is the scenario.
fn cmd_write(a: &Args) -> Result<i32, String> {
    let mut t = open_target(a)?;
    let mut j = open_journal(a)?;
    let capacity = t.size() / stamp::BLOCK as u64;
    let blocks = a.blocks.min(capacity);
    if blocks == 0 {
        return Err(format!("{} is too small to hold a block", t.describe()));
    }
    println!("ganon: writing generation {} to {} blocks of {}", a.generation, blocks, t.describe());

    let mut errors = 0u64;
    for b in 0..blocks {
        let offset = b * stamp::BLOCK as u64;
        let data = stamp::build(VDISK_TAG, offset, a.generation, now_ns());
        j.issue(b, a.generation);
        match t.write_at(offset, &data) {
            Ok(()) => j.ack(b, a.generation),
            Err(e) => {
                errors += 1;
                if errors <= 3 {
                    eprintln!("ganon: write to block {b} failed: {e}");
                }
                // Left in flight on purpose: either outcome is legal for this block now.
            }
        }
    }
    if let Err(e) = t.flush() {
        eprintln!("ganon: flush failed: {e}");
    }
    println!("ganon: {} block(s) written, {errors} error(s)", blocks - errors);
    Ok(0)
}

fn cmd_verify(a: &Args) -> Result<i32, String> {
    let mut t = open_target(a)?;
    let j = open_journal(a)?;
    let capacity = t.size() / stamp::BLOCK as u64;
    let blocks = a.blocks.min(capacity);
    let mut v = Verdict::default();

    for b in 0..blocks {
        let offset = b * stamp::BLOCK as u64;
        match t.read_at(offset, stamp::BLOCK) {
            Ok(data) => j.judge(b, offset, &data, &mut v),
            Err(e) => {
                // An unreadable range is a verdict too. At ftt>=1 after a single failure
                // this is a durability violation; at ftt=0 it is the honest consequence
                // of having one copy, and the operator is owed the distinction.
                v.blocks_checked += 1;
                v.violations.push(journal::Violation {
                    block: b,
                    offset,
                    detail: format!("range unreadable: {e}"),
                });
            }
        }
    }
    report(&v, &t.describe());
    Ok(if v.clean() { 0 } else { 1 })
}

/// Overwrite the same blocks generation after generation, verifying between rounds.
/// The point is redirect-on-write churn: every round leaves the previous extents as
/// garbage and repoints the map, which is where an off-by-one in the drain shows up.
fn cmd_soak(a: &Args) -> Result<i32, String> {
    let mut t = open_target(a)?;
    let mut j = open_journal(a)?;
    let capacity = t.size() / stamp::BLOCK as u64;
    let blocks = a.blocks.min(capacity);
    let mut state = a.seed | 1;
    let mut worst = 0;

    for round in 1..=a.rounds {
        // Random order, so a bug that only appears when extents are touched out of
        // sequence is reachable. The seed is printed, so it replays.
        let mut order: Vec<u64> = (0..blocks).collect();
        for i in (1..order.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            order.swap(i, (state % (i as u64 + 1)) as usize);
        }
        for b in order {
            let offset = b * stamp::BLOCK as u64;
            let data = stamp::build(VDISK_TAG, offset, round, now_ns());
            j.issue(b, round);
            match t.write_at(offset, &data) {
                Ok(()) => j.ack(b, round),
                Err(e) => eprintln!("ganon: round {round} block {b}: {e}"),
            }
        }
        t.flush().ok();

        let mut v = Verdict::default();
        for b in 0..blocks {
            let offset = b * stamp::BLOCK as u64;
            match t.read_at(offset, stamp::BLOCK) {
                Ok(data) => j.judge(b, offset, &data, &mut v),
                Err(e) => {
                    v.blocks_checked += 1;
                    v.violations.push(journal::Violation {
                        block: b,
                        offset,
                        detail: format!("range unreadable: {e}"),
                    });
                }
            }
        }
        println!(
            "ganon: round {round}/{}: {} blocks, {} violation(s)",
            a.rounds,
            v.blocks_checked,
            v.violations.len()
        );
        if !v.clean() {
            report(&v, &t.describe());
            worst = 1;
            break;
        }
    }
    println!("ganon: seed {} (pass --seed {} to replay)", a.seed, a.seed);
    Ok(worst)
}

/// Flip bytes in a file directly, underneath whatever is serving it. This is the bitrot
/// injector: it exists to prove that a corrupted replica is reported as an error and
/// never returned as data.
fn cmd_corrupt(a: &Args) -> Result<i32, String> {
    use std::io::{Read, Seek, SeekFrom, Write};
    let path = a.file.as_ref().ok_or_else(|| "--file is required".to_string())?;
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    let mut buf = vec![0u8; a.bytes];
    f.seek(SeekFrom::Start(a.offset)).map_err(|e| e.to_string())?;
    f.read_exact(&mut buf).map_err(|e| e.to_string())?;
    for b in buf.iter_mut() {
        *b ^= 0xFF;
    }
    f.seek(SeekFrom::Start(a.offset)).map_err(|e| e.to_string())?;
    f.write_all(&buf).map_err(|e| e.to_string())?;
    f.sync_all().map_err(|e| e.to_string())?;
    println!(
        "ganon: inverted {} byte(s) at offset {} of {}",
        a.bytes,
        a.offset,
        path.display()
    );
    Ok(0)
}

fn report(v: &Verdict, target: &str) {
    println!("--- ganon verdict: {target} ---");
    println!(
        "  {} blocks checked: {} at the acknowledged generation, {} from in-flight writes, \
         {} never written",
        v.blocks_checked, v.blocks_acked, v.blocks_from_inflight, v.blocks_zeroed
    );
    if v.violations.is_empty() {
        // Never "no violations exist". The strongest available claim is about the
        // histories actually injected, and anything stronger is marketing.
        println!("  NO VIOLATION OBSERVED under the injected history.");
    } else {
        println!("  {} VIOLATION(S):", v.violations.len());
        for x in v.violations.iter().take(20) {
            println!("    block {} (offset {}): {}", x.block, x.offset, x.detail);
        }
        if v.violations.len() > 20 {
            println!("    ... and {} more", v.violations.len() - 20);
        }
    }
}

fn usage() -> i32 {
    eprintln!(
        "ganon <write|verify|soak|corrupt> [flags]\n\
         \n\
         \x20 --target <spec>     device path, or nbd:<socket>:<export>\n\
         \x20 --journal <path>    ack journal; keep it off the system under test\n\
         \x20 --blocks N          how many 4 KiB blocks to cover (default 256)\n\
         \x20 --generation G      generation to stamp (write)\n\
         \x20 --rounds R          overwrite rounds (soak, default 4)\n\
         \x20 --seed S            shuffle seed, printed on every run so it replays\n\
         \x20 --file <path>       file to damage (corrupt)\n\
         \x20 --offset N          byte offset to damage (corrupt)\n\
         \x20 --bytes N           how many bytes to invert (corrupt, default 1)"
    );
    2
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("ganon: {e}");
            std::process::exit(usage());
        }
    };
    let result = match args.cmd.as_str() {
        "write" => cmd_write(&args),
        "verify" => cmd_verify(&args),
        "soak" => cmd_soak(&args),
        "corrupt" => cmd_corrupt(&args),
        _ => {
            std::process::exit(usage());
        }
    };
    match result {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("ganon: {e}");
            std::process::exit(2);
        }
    }
}

/// Kept so the module tree is exercised from an integration angle too.
#[allow(dead_code)]
fn _target_path_is_a_path(p: &Path) -> bool {
    p.is_absolute()
}
