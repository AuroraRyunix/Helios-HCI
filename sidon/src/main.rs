//! Sidon: the per-node data-path daemon.
//!
//! Two listening surfaces, and **neither of them is a TCP port**:
//!
//! - a control socket at `/run/sidon/control.sock`, spoken by spark-daemon on this host.
//!   Cluster-facing control therefore arrives over the existing mutual-TLS mesh on 9099
//!   and is translated locally, so adding a storage tier adds no new authenticated
//!   surface, no new certificate, and nothing new to firewall.
//! - one NBD socket per attached vdisk, which qemu opens directly.
//!
//! A peer data port is reserved for replication (9105) and is not opened here: at ftt=0
//! there is no peer to talk to, and a port that binds before it has a purpose is a port
//! somebody has to explain.

mod control;
mod crc;
mod err;
mod extent;
mod journal;
mod meta;
mod nbd;
mod overlay;
mod peer;
mod purah;
mod vdisk;

use std::path::PathBuf;
use std::time::Duration;

use crate::control::{Daemon, DaemonConfig};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_bytes(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

/// This node's name, matching what LINSTOR and the rest of the stack already use as a
/// node identity so the map's `owner` column means the same thing everywhere.
/// Parse "node=host:port,node=host:port" into pairs.
///
/// A malformed entry is dropped with a warning rather than failing startup: a daemon that
/// refuses to boot because one peer address has a typo cannot serve the disks it owns
/// locally either, and those are unaffected by a peer it cannot name.
fn parse_peers(spec: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for entry in spec.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        match entry.split_once('=') {
            Some((node, addr)) if !node.is_empty() && addr.contains(':') => {
                out.push((node.trim().to_string(), addr.trim().to_string()));
            }
            _ => eprintln!("sidon: ignoring malformed SIDON_PEERS entry {entry:?}"),
        }
    }
    out
}

fn node_name() -> String {
    if let Ok(n) = std::env::var("SIDON_NODE") {
        return n;
    }
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn main() {
    let cfg = DaemonConfig {
        root: PathBuf::from(env_or("SIDON_ROOT", "/var/lib/hci/sidon")),
        control_socket: PathBuf::from(env_or("SIDON_CONTROL", "/run/sidon/control.sock")),
        daruk_addr: env_or("SIDON_DARUK", "127.0.0.1:9043"),
        node: node_name(),
        // 64 MiB of journal before a drain. Large enough that a burst of guest writes
        // never waits on one, small enough that replay after a crash is seconds.
        high_water: env_bytes("SIDON_HIGH_WATER", 64 << 20),
        daruk_timeout: Duration::from_secs(env_bytes("SIDON_DARUK_TIMEOUT", 15)),
        // Purah runs every 5 minutes and will not reclaim anything it has not seen
        // unreferenced for 10 -- and never on first sight, whatever the grace, because
        // the two-scan rule is separate from it. Generous on purpose: reclaiming late
        // costs disk, reclaiming early costs data, and those are not comparable.
        purah_interval: Duration::from_secs(env_bytes("SIDON_PURAH_INTERVAL", 300)),
        purah_grace: Duration::from_secs(env_bytes("SIDON_PURAH_GRACE", 600)),
        // Where this node listens for replication, and how to reach the others.
        //
        // SIDON_PEERS is "node=host:port,node=host:port". Generated from cluster.json on
        // a real cluster; set by hand to run several instances on one host, which is how
        // the replication semantics are tested without needing several hosts.
        peer_bind: env_or("SIDON_PEER_BIND", "127.0.0.1:9105"),
        peers: parse_peers(&env_or("SIDON_PEERS", "")),
        // 20s suits a bulk append to a busy peer. Fencing during a takeover gets its
        // own, shorter budget: the whole point of that path is to be fast, and a
        // replica that has not answered in five seconds is not going to make the
        // difference between a safe failover and an unsafe one -- an append needs all
        // of them, so fencing any one is already sufficient.
        peer_timeout: Duration::from_secs(env_bytes("SIDON_PEER_TIMEOUT", 20)),
        fence_timeout: Duration::from_secs(env_bytes("SIDON_FENCE_TIMEOUT", 5)),
    };

    println!(
        "sidon: node={} root={} control={} daruk={} peer_bind={} peers=[{}]",
        cfg.node,
        cfg.root.display(),
        cfg.control_socket.display(),
        cfg.daruk_addr,
        cfg.peer_bind,
        cfg.peers.iter().map(|(n, a)| format!("{n}@{a}")).collect::<Vec<_>>().join(" ")
    );

    let daemon = match Daemon::new(cfg) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("sidon: cannot start: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = daemon.run() {
        eprintln!("sidon: control loop exited: {e}");
        std::process::exit(1);
    }
}
