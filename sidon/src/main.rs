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
mod extent_id_map;
mod extent_resolve;
mod heat;
mod journal;
mod meta;
mod nbd;
mod overlay;
mod peer;
mod purah;
mod tls;
mod vdisk;

use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;

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

/// `/etc/hci/cluster.json`, or `None` when it cannot be read.
fn cluster_document() -> Option<Value> {
    let path = std::env::var("SIDON_CLUSTER_JSON")
        .unwrap_or_else(|_| "/etc/hci/cluster.json".to_string());
    let text = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&text).ok()
}

/// Every other node in the cluster, as `(name, "ip:port")`.
///
/// Derived rather than configured. The alternative is generating `SIDON_PEERS=` into the
/// unit file at provision time, which is one source of truth too many: membership changes
/// -- a node added, a node decommissioned -- rewrite `cluster.json` on every host
/// already, and a unit file generated once would describe the cluster as it was on the
/// day the node was built.
///
/// `SIDON_PEERS` still wins when set, because the tests need to name peers that are not
/// in any cluster document.
fn peers_from_cluster(me: &str, port: u16) -> Vec<(String, String)> {
    let doc = match cluster_document() {
        Some(d) => d,
        None => return Vec::new(),
    };
    let hosts = match doc.get("hosts").and_then(Value::as_array) {
        Some(h) => h,
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    for host in hosts {
        let name = host.get("hostname").and_then(Value::as_str).unwrap_or("").trim();
        let ip = host.get("ip").and_then(Value::as_str).unwrap_or("").trim();
        if name.is_empty() || ip.is_empty() || name == me {
            continue;
        }
        out.push((name.to_string(), format!("{ip}:{port}")));
    }
    out
}

/// The fault tolerance this cluster was created with, from `cluster.json`.
///
/// This is where the redundancy factor actually lives. It is not in ZooKeeper --
/// `/cluster_state` holds one word, `started` or `stopped` -- and it is not a row in
/// Hydra: `cluster_settings.replication_factor` is the *keyspace's* replication, which
/// governs copies of the metadata and says nothing about copies of a guest's disk.
/// `cluster.json` is written by `cluster create -r`, copied to every host, and is the
/// same document this daemon already reads to find its peers.
///
/// `SIDON_CLUSTER_FTT` overrides it so the replication semantics can be exercised with
/// several instances on one host, which is the only way they are tested today.
fn cluster_ftt() -> Option<u64> {
    if let Ok(v) = std::env::var("SIDON_CLUSTER_FTT") {
        return v.trim().parse().ok();
    }
    // Not `as_u64` alone: a document written by hand with `"redundancy_factor": 1.0`, or
    // with the number quoted, should still be read rather than silently becoming "the
    // cluster did not say" and dropping every create back to a single copy.
    let doc = cluster_document()?;
    match doc.get("redundancy_factor")? {
        Value::Number(n) if n.is_u64() => n.as_u64(),
        Value::Number(n) if n.is_i64() => n.as_i64().filter(|v| *v >= 0).map(|v| v as u64),
        Value::Number(n) => n.as_f64().filter(|v| *v >= 0.0).map(|v| v as u64),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// What to bind the replication port to.
///
/// This node's own address from `cluster.json`, so peers can reach it -- but loopback
/// when the cluster has one host, because at ftt=0 there is nothing to replicate to and
/// binding a routable port would demand certificates to serve traffic that will never
/// arrive.
fn peer_bind_address(me: &str, port: u16) -> String {
    let doc = match cluster_document() {
        Some(d) => d,
        None => return format!("127.0.0.1:{port}"),
    };
    let hosts = doc.get("hosts").and_then(Value::as_array).cloned().unwrap_or_default();
    if hosts.len() < 2 {
        return format!("127.0.0.1:{port}");
    }
    for host in &hosts {
        if host.get("hostname").and_then(Value::as_str).map(str::trim) == Some(me) {
            if let Some(ip) = host.get("ip").and_then(Value::as_str) {
                return format!("{}:{port}", ip.trim());
            }
        }
    }
    // In a multi-node document but not in it by name. Refusing to guess: binding
    // loopback here keeps this node serving its own guests while making it obvious in
    // `peers` that nothing can reach it.
    eprintln!(
        "sidon: this node ({me}) is not listed in the cluster document, so the \
         replication port stays on loopback and no peer can reach it"
    );
    format!("127.0.0.1:{port}")
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
    // Resolved before the config, because the bind address and the peer list are both
    // "everyone in the cluster document except me" and need to know which one is me.
    let node = node_name();
    let cfg = DaemonConfig {
        root: PathBuf::from(env_or("SIDON_ROOT", "/var/lib/hci/sidon")),
        control_socket: PathBuf::from(env_or("SIDON_CONTROL", "/run/sidon/control.sock")),
        daruk_addr: env_or("SIDON_DARUK", "127.0.0.1:9043"),
        node: node.clone(),
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
        peer_bind: match std::env::var("SIDON_PEER_BIND") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => peer_bind_address(&node, 9105),
        },
        peers: match std::env::var("SIDON_PEERS") {
            Ok(v) if !v.trim().is_empty() => parse_peers(&v),
            _ => peers_from_cluster(&node, 9105),
        },
        // 20s suits a bulk append to a busy peer. Fencing during a takeover gets its
        // own, shorter budget: the whole point of that path is to be fast, and a
        // replica that has not answered in five seconds is not going to make the
        // difference between a safe failover and an unsafe one -- an append needs all
        // of them, so fencing any one is already sufficient.
        peer_timeout: Duration::from_secs(env_bytes("SIDON_PEER_TIMEOUT", 20)),
        fence_timeout: Duration::from_secs(env_bytes("SIDON_FENCE_TIMEOUT", 5)),
        // How many copies a create makes when nothing in the request says. Read here
        // rather than per-create so it is printed at startup beside the peer list: the
        // two together are this node's placement policy, and an operator wondering why a
        // vdisk has one copy should be able to answer it from the log.
        cluster_ftt: cluster_ftt(),
        // How often the extent-group access tally reaches Hydra, and how many groups it
        // may hold.
        //
        // Sixty seconds, which is two orders of magnitude more often than the sweep and
        // for the opposite reason: the sweep is slow because reclaiming early costs data,
        // while the only thing a slow flush buys here is a bigger window of counts to lose
        // in a crash. Zero turns the tally off, counters included, for an operator who
        // wants the read path to do nothing it does not have to -- and `purah-heat` then
        // ranks nothing, which is the honest consequence rather than a silent one.
        //
        // 200,000 groups is a generous cap on a node whose disks hold about 75,000 per
        // 300 GB. It is there so that a bug producing unbounded extent-group ids cannot
        // make heat accounting the reason a node runs out of memory.
        access_flush: Duration::from_secs(env_bytes("SIDON_ACCESS_FLUSH", 60)),
        access_capacity: env_bytes("SIDON_ACCESS_CAPACITY", 200_000) as usize,
    };

    println!(
        "sidon: node={} root={} control={} daruk={} peer_bind={} peers=[{}] ftt={}",
        cfg.node,
        cfg.root.display(),
        cfg.control_socket.display(),
        cfg.daruk_addr,
        cfg.peer_bind,
        cfg.peers.iter().map(|(n, a)| format!("{n}@{a}")).collect::<Vec<_>>().join(" "),
        // "unset" and "0" print differently on purpose. One of them is a cluster that
        // asked for no replication; the other is a node that could not read the document
        // saying what it asked for, and those want different responses from whoever is
        // reading this line.
        match cfg.cluster_ftt {
            Some(f) => f.to_string(),
            None => "unset".to_string(),
        }
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
