//! The control plane: newline-delimited JSON over a unix socket.
//!
//! Deliberately not an HTTP server on a port. Everything that reaches this socket has
//! already crossed the cluster's trust boundary at spark-daemon's mutual-TLS API on 9099,
//! so authentication happens once, in the place that already does it, rather than being
//! reimplemented here with a second certificate and a second thing to get wrong. Unix
//! permissions carry the local half.
//!
//! Ownership is claimed here and nowhere else: a vdisk is opened only after this node has
//! won the compare-and-swap in Hydra, and the epoch it won is the epoch every journal
//! record it writes will carry.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};

use crate::err::{Error, Result};
use crate::meta::{
    block_map_batches, cql_str, json_params, now_ms, Daruk, CLASS_FORMING, CLASS_IMMUTABLE,
    CLASS_RW, MAP_BATCH,
};
use crate::heat::AccessLog;
use crate::nbd::{self, Export, LocalVdisk};
use crate::peer::{self, Forwarder, Owned, PeerClient, ReplicaStore};
use crate::extent::vdisk_hash;
use crate::vdisk::field_u64;
use crate::purah::Purah;
use crate::vdisk::{Vdisk, VdiskConfig};

pub struct DaemonConfig {
    pub root: PathBuf,
    pub control_socket: PathBuf,
    pub daruk_addr: String,
    pub node: String,
    pub high_water: u64,
    pub daruk_timeout: Duration,
    pub purah_interval: Duration,
    pub purah_grace: Duration,
    pub peer_bind: String,
    pub peers: Vec<(String, String)>,
    pub peer_timeout: Duration,
    pub fence_timeout: Duration,
    /// The cluster's `redundancy_factor` from `cluster.json`, when it could be read.
    ///
    /// The fallback for a create whose container says nothing, and the reason a vdisk
    /// gets more than one copy at all. `None` is "the document did not say", which is not
    /// the same as 0 and must not be treated as it: 0 is a cluster created with `-r 0` and
    /// meaning it, while `None` is a node that could not read its own configuration, and
    /// only the first of those is an instruction.
    pub cluster_ftt: Option<u64>,
    /// How often the in-memory extent-group access tally is written to Hydra. Zero turns
    /// the tally off entirely, counters and all.
    pub access_flush: Duration,
    /// How many extent groups the tally may track at once. A guard against unbounded
    /// growth, not a policy -- see `heat.rs`.
    pub access_capacity: usize,
}

struct Attached {
    /// None when this node is forwarding rather than owning. Everything that reaches into
    /// the vdisk -- status, flush, drain, the held-egroup set -- is therefore an owner-only
    /// operation, and says so rather than inventing an answer for a disk it does not have.
    vdisk: Option<Arc<Mutex<Vdisk>>>,
    socket: PathBuf,
    stop: Arc<AtomicBool>,
    forwarding_to: Option<String>,
}

pub struct Daemon {
    cfg: DaemonConfig,
    attached: Mutex<HashMap<String, Attached>>,
    purah_state: Mutex<Purah>,
    /// One client per peer, shared by every vdisk that replicates to it -- the whole
    /// point of the shape: connections scale with the node count, not the disk count.
    peers: HashMap<String, Arc<PeerClient>>,
    /// A second client per peer, with a shorter timeout, used only for fencing during a
    /// takeover. A client owns its socket timeouts, so the fast path and the bulk path
    /// cannot share one -- and a takeover inheriting the replication timeout is what made
    /// failing over away from a wedged host take twenty seconds.
    fence_peers: HashMap<String, Arc<PeerClient>>,
    /// What this node stores on behalf of vdisks it does not own.
    replica_store: Arc<ReplicaStore>,
    /// Where every attached vdisk tallies its extent-group accesses, and where the flusher
    /// thread and the ranking pass read them from. One per node: an extent group is shared
    /// between a parent and every snapshot of it, so its temperature is the sum of what
    /// every vdisk on the node does to it.
    access: Arc<AccessLog>,
}

/// How many copies a vdisk should be created with, given a fault tolerance and the number
/// of nodes that could hold one.
///
/// The whole reason this is a named function rather than two characters at the call site
/// is that the conversion is where the bug was. `ftt` counts *failures survived* --
/// `cluster.json`'s `redundancy_factor` and a container's `ftt` both mean that, and a
/// single-node cluster is created with 0 -- while `dfs_vdisks.rf` counts *copies*. They
/// differ by exactly one, and the column being called rf while the setting is called
/// redundancy_factor is enough to make reading one straight into the other look correct.
/// It is not: it turns "survive one host loss" into "keep one copy", which is the
/// opposite instruction, and it does so without failing anything an operator would see.
///
/// Clamped to the nodes available rather than refused. A cluster with fewer hosts than its
/// ftt asks for is a real and supported state -- one node at ftt=1 is what every
/// single-node deployment looks like after `cluster.json` is copied from a bigger one --
/// and refusing every create there would take a cluster that works today and stop it. The
/// shortfall is visible in the recorded rf, which is the honest place for it.
fn copies_for_ftt(ftt: u64, nodes: usize) -> usize {
    let asked = usize::try_from(ftt.saturating_add(1)).unwrap_or(usize::MAX);
    asked.min(nodes.max(1)).max(1)
}

impl Daemon {
    pub fn new(cfg: DaemonConfig) -> Result<Arc<Daemon>> {
        std::fs::create_dir_all(cfg.root.join("journal"))?;
        std::fs::create_dir_all(cfg.root.join("egroups"))?;
        std::fs::create_dir_all(cfg.root.join("nbd"))?;
        if let Some(parent) = cfg.control_socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let store = crate::extent::EgroupStore::open(
            crate::extent::discover_disks(&cfg.root), 0)?;
        // Built before Purah and before any vdisk, because both hold a handle to it and
        // there must be exactly one: a second tally would be a set of counters nothing
        // flushes, and the extent groups counted into it would rank as the coldest on the
        // node precisely because they are the busiest.
        let access = Arc::new(AccessLog::new(cfg.access_capacity, crate::meta::now_ms()));
        let purah = Purah::new(
            Daruk::new(&cfg.daruk_addr, cfg.daruk_timeout),
            store,
            &cfg.node,
            // Zero means zero. The safe default lives in main.rs where the environment is
            // read; silently substituting it here would make an explicit
            // SIDON_PURAH_GRACE=0 do something other than what it says, which is worse
            // than letting an operator who asked for no grace have none.
            cfg.purah_grace,
            Arc::clone(&access),
        );
        let replica_store = Arc::new(ReplicaStore::new(&cfg.root)?);
        let mut peers = HashMap::new();
        let mut fence_peers = HashMap::new();
        for (node, addr) in &cfg.peers {
            peers.insert(
                node.clone(),
                Arc::new(PeerClient::new(node, addr, cfg.peer_timeout)),
            );
            fence_peers.insert(
                node.clone(),
                Arc::new(PeerClient::with_attempts(node, addr, cfg.fence_timeout, 1)),
            );
        }
        Ok(Arc::new(Daemon {
            cfg,
            attached: Mutex::new(HashMap::new()),
            purah_state: Mutex::new(purah),
            peers,
            fence_peers,
            replica_store,
            access,
        }))
    }

    fn daruk(&self) -> Daruk {
        Daruk::new(&self.cfg.daruk_addr, self.cfg.daruk_timeout)
    }

    /// Nodes that could hold a copy: this one, plus every peer the cluster document named.
    ///
    /// The peer list rather than the peers that are answering. Placement is a durability
    /// decision and a node being down for ten minutes is not a reason to create every
    /// vdisk in that window at a lower redundancy than the cluster asked for -- the
    /// create would succeed, the number would be wrong forever, and nothing afterwards
    /// re-reads it.
    fn placement_nodes(&self) -> usize {
        1 + self.cfg.peers.len()
    }

    /// The replica count a create should use when the request does not say.
    ///
    /// Container first, cluster second, one copy last. The container is first because
    /// that is what the schema says this number is -- migration 0006 records rf as
    /// "copied from the container's ftt" -- and because it is the only one of the two
    /// that can express "these particular disks are scratch". The cluster's
    /// `redundancy_factor` catches every vdisk whose container says nothing, which today
    /// is any create that omits the container and lands on Sidon's unmatched "default".
    ///
    /// One copy is the last resort and not a policy: it is what this node falls back to
    /// when it can read neither Hydra nor its own cluster document, and it is deliberately
    /// the same thing that used to happen unconditionally. Guessing higher would place
    /// replicas on nodes whose membership we just failed to establish.
    fn default_copies(&self, container: &str) -> usize {
        let ftt = crate::vdisk::container_ftt(&self.daruk(), container).or(self.cfg.cluster_ftt);
        match ftt {
            Some(ftt) => copies_for_ftt(ftt, self.placement_nodes()),
            None => {
                eprintln!(
                    "sidon: neither container '{container}' nor the cluster document names a \
                     redundancy factor, so this vdisk is being created with one copy"
                );
                1
            }
        }
    }

    fn vdisk_cfg(&self) -> VdiskConfig {
        VdiskConfig {
            root: self.cfg.root.clone(),
            node: self.cfg.node.clone(),
            high_water: self.cfg.high_water,
            access: Arc::clone(&self.access),
        }
    }

    pub fn run(self: &Arc<Self>) -> Result<()> {
        // A stale socket file from a killed daemon would make bind fail forever. Removing
        // it is safe because a *live* daemon holding it would have been caught by the
        // systemd unit's own start limit, not by a second process reaching this line.
        let _ = std::fs::remove_file(&self.cfg.control_socket);
        let listener = UnixListener::bind(&self.cfg.control_socket).map_err(|e| {
            Error::io(format!(
                "cannot bind control socket {}: {e}",
                self.cfg.control_socket.display()
            ))
        })?;
        std::fs::set_permissions(&self.cfg.control_socket, std::fs::Permissions::from_mode(0o600))?;
        peer::listen(
            &self.cfg.peer_bind,
            Arc::clone(&self.replica_store),
            Arc::clone(self) as Arc<dyn Owned>,
        )?;
        self.start_purah();
        println!("sidon: control socket ready");

        for conn in listener.incoming() {
            match conn {
                Ok(stream) => {
                    let me = Arc::clone(self);
                    thread::spawn(move || {
                        if let Err(e) = me.serve_control(stream) {
                            eprintln!("sidon: control connection: {e}");
                        }
                    });
                }
                Err(e) => eprintln!("sidon: control accept failed: {e}"),
            }
        }
        Ok(())
    }

    fn serve_control(self: &Arc<Self>, stream: UnixStream) -> Result<()> {
        let peer = stream.try_clone()?;
        let reader = BufReader::new(peer);
        let mut writer = stream;
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let response = match serde_json::from_str::<Value>(&line) {
                Ok(req) => match self.dispatch(&req) {
                    Ok(v) => {
                        let mut out = json!({"ok": true});
                        if let (Some(o), Some(m)) = (out.as_object_mut(), v.as_object()) {
                            for (k, val) in m {
                                o.insert(k.clone(), val.clone());
                            }
                        }
                        out
                    }
                    // The error *kind* is carried alongside the message: a caller
                    // retrying a Meta failure is sensible, retrying a Refused is a loop.
                    Err(e) => json!({
                        "ok": false,
                        "error": e.to_string(),
                        "kind": match e {
                            Error::Io(_) => "io",
                            Error::Corrupt(_) => "corrupt",
                            Error::Meta(_) => "meta",
                            Error::Refused(_) => "refused",
                        }
                    }),
                },
                Err(e) => json!({"ok": false, "error": format!("malformed request: {e}"), "kind": "refused"}),
            };
            writeln!(writer, "{response}")?;
            writer.flush()?;
        }
        Ok(())
    }

    fn dispatch(self: &Arc<Self>, req: &Value) -> Result<Value> {
        let op = req
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::refused("request has no 'op'".to_string()))?;
        match op {
            "ping" => Ok(json!({"node": self.cfg.node})),
            "create" => self.op_create(req),
            "attach" => self.op_attach(req),
            "detach" => self.op_detach(req),
            "delete" => self.op_delete(req),
            "list" => self.op_list(),
            "status" => self.op_status(req),
            "flush" => self.op_flush(req),
            "seal" => self.op_seal(req),
            "snapshot" => self.op_snapshot(req),
            "clone" => self.op_clone(req),
            "resize" => self.op_resize(req),
            "capacity" => self.op_capacity(),
            "peers" => self.op_peers(),
            "purah-heal" => self.op_purah_heal(req),
            "purah-sweep" => self.op_purah_sweep(),
            "purah-scrub" => self.op_purah_scrub(),
            "purah-heat" => self.op_purah_heat(req),
            other => Err(Error::refused(format!("unknown op '{other}'"))),
        }
    }

    fn op_create(&self, req: &Value) -> Result<Value> {
        let id = str_field(req, "vdisk_id")?;
        let size = u64_field(req, "size_bytes")?;
        if size == 0 {
            return Err(Error::refused("size_bytes must be greater than zero".to_string()));
        }
        let container = req.get("container").and_then(Value::as_str).unwrap_or("default");
        let class = req.get("class").and_then(Value::as_str).unwrap_or(CLASS_RW);
        if class != CLASS_RW && class != CLASS_IMMUTABLE {
            return Err(Error::refused(format!(
                "class '{class}' is neither 'rw' nor 'immutable'"
            )));
        }
        let extent_bytes = req.get("extent_bytes").and_then(Value::as_u64).unwrap_or(1 << 20);
        let egroup_bytes = req.get("egroup_bytes").and_then(Value::as_u64).unwrap_or(4 << 20);

        // Replica placement. Explicit `replicas` wins; otherwise this node plus as many
        // peers as `rf` calls for, in the order they were configured. Deliberately simple:
        // a real placement policy (racks, free space, locality) belongs in Vali, which
        // already places VMs, rather than in the daemon serving the bytes.
        //
        // `rf` used to default to 1 here, and because no caller has ever sent one, that
        // default *was* the policy: every vdisk on every cluster was created single-copy
        // whatever the operator had configured, and nothing said so. The cluster's
        // redundancy factor was consulted by the console, by the keyspace, and by the
        // capacity arithmetic -- everywhere except the one place that decides how many
        // copies of a guest's disk exist. Purah re-replicated correctly the whole time and
        // had nothing to do, because one copy was the requested count.
        //
        // An explicit `rf` still wins. A caller that names a number has a reason, and the
        // only two callers that ever will are the replication test harness and an operator
        // overriding policy for one disk.
        //
        // So does an explicit `replicas` list, and it carries the count with it: a caller
        // that names the nodes has already said how many copies it wants, and deriving a
        // larger number from policy would refuse a request that names a perfectly valid
        // set. That is not hypothetical -- it is what tools/tls_replication_check.sh does.
        let explicit_replicas = req.get("replicas").and_then(Value::as_array);
        let rf = match (req.get("rf").and_then(Value::as_u64), &explicit_replicas) {
            (Some(rf), _) => rf.max(1) as usize,
            (None, Some(list)) => list.len().max(1),
            (None, None) => self.default_copies(container),
        };
        let replicas: Vec<String> = match explicit_replicas {
            Some(list) => list.iter().filter_map(Value::as_str).map(str::to_string).collect(),
            None => {
                let mut chosen = vec![self.cfg.node.clone()];
                for (node, _) in self.cfg.peers.iter() {
                    if chosen.len() >= rf {
                        break;
                    }
                    if node != &self.cfg.node {
                        chosen.push(node.clone());
                    }
                }
                chosen
            }
        };
        if replicas.len() < rf {
            return Err(Error::refused(format!(
                "vdisk {id} asks for rf={rf} but only {} node(s) are available. Creating it \
                 anyway would record a durability guarantee the cluster cannot keep.",
                replicas.len()
            )));
        }

        let cas = self.daruk().cas(
            "/v1/dfs/vdisk-create",
            json_params(vec![
                ("vdisk_id", json!(id)),
                ("container", json!(container)),
                ("size_bytes", json!(size as i64)),
                ("class", json!(class)),
                ("owner", json!("")),
                ("epoch", json!(0)),
                ("drain_seq", json!(0)),
                ("extent_bytes", json!(extent_bytes as i64)),
                ("egroup_bytes", json!(egroup_bytes as i64)),
                ("created_at_ms", json!(now_ms())),
                ("replicas", json!(replicas)),
                ("rf", json!(rf as i64)),
            ]),
        )?;
        if !cas.applied {
            return Err(Error::refused(format!("vdisk {id} already exists")));
        }
        Ok(json!({
            "vdisk_id": id, "size_bytes": size, "class": class,
            "replicas": replicas, "rf": rf,
        }))
    }

    fn op_attach(self: &Arc<Self>, req: &Value) -> Result<Value> {
        let id = str_field(req, "vdisk_id")?;
        {
            let attached = self.attached.lock().expect("attached mutex poisoned");
            if let Some(a) = attached.get(&id) {
                // Idempotent: a retried attach returns the socket it already has rather
                // than bumping the epoch, which would fence the qemu currently using it.
                return Ok(json!({
                    "vdisk_id": id,
                    "socket": a.socket.to_string_lossy(),
                    "already_attached": true
                }));
            }
        }

        let daruk = self.daruk();
        let rows = daruk.query(&format!(
            "SELECT owner, epoch, replicas, size_bytes, class FROM hydra.dfs_vdisks \
             WHERE vdisk_id = {}",
            cql_str(&id)
        ))?;
        let row = rows
            .first()
            .ok_or_else(|| Error::refused(format!("vdisk {id} does not exist")))?;
        // A vdisk still being built out of a parent's map is not servable, and the class
        // says so. Attaching one would present a disk whose extents are half copied --
        // which reads as zeroes where the copy has not reached, and is indistinguishable
        // from a disk that was legitimately never written.
        if row.get("class").and_then(Value::as_str) == Some(CLASS_FORMING) {
            return Err(Error::refused(format!(
                "vdisk {id} is still being formed from its parent and cannot be attached. \
                 If no snapshot or clone is in progress, this one did not finish and the \
                 vdisk should be deleted."
            )));
        }
        let cur_owner = row.get("owner").and_then(Value::as_str).unwrap_or("").to_string();
        let cur_epoch = row.get("epoch").and_then(Value::as_i64).unwrap_or(0);
        let new_epoch = cur_epoch + 1;

        // Forwarding mode: serve the disk without taking it.
        //
        // What a live migration uses. The guest resumes on this host and its I/O is
        // relayed to whoever still owns the disk, so there is no instant where storage
        // must hand over synchronously with the VM. Ownership follows later, at leisure.
        if req.get("forward").and_then(Value::as_bool).unwrap_or(false) {
            if cur_owner.is_empty() || cur_owner == self.cfg.node {
                return Err(Error::refused(format!(
                    "vdisk {id} is owned by {}; forwarding needs another node to forward to",
                    if cur_owner.is_empty() { "nobody" } else { "this node" }
                )));
            }
            let owner_client = self.peers.get(&cur_owner).ok_or_else(|| {
                Error::refused(format!(
                    "vdisk {id} is owned by {cur_owner}, which this daemon has no address for"
                ))
            })?;
            let size = row.get("size_bytes").and_then(Value::as_i64).unwrap_or(0).max(0) as u64;
            let class = row.get("class").and_then(Value::as_str).unwrap_or(CLASS_RW);
            let backend = Arc::new(Forwarder {
                vdisk: id.clone(),
                size,
                read_only: class == CLASS_IMMUTABLE,
                owner: Arc::clone(owner_client),
            });
            let socket = self.serve_socket(&id, backend)?;
            self.attached.lock().expect("attached mutex poisoned").insert(
                id.clone(),
                Attached {
                    vdisk: None,
                    socket: socket.0.clone(),
                    stop: socket.1,
                    forwarding_to: Some(cur_owner.clone()),
                },
            );
            return Ok(json!({
                "vdisk_id": id,
                "socket": socket.0.to_string_lossy(),
                "forwarding_to": cur_owner,
                "owner_epoch": cur_epoch,
            }));
        }

        // The claim is conditional on *both* the owner and the epoch as they were read.
        // Conditioning on owner alone would let a node that held this disk two takeovers
        // ago re-take it after a round trip it never noticed losing.
        let cas = daruk.cas(
            "/v1/dfs/claim",
            json_params(vec![
                ("vdisk_id", json!(id)),
                ("owner", json!(self.cfg.node)),
                ("epoch", json!(new_epoch)),
                ("expected_owner", json!(cur_owner)),
                ("expected_epoch", json!(cur_epoch)),
            ]),
        )?;
        if !cas.applied {
            return Err(Error::refused(format!(
                "vdisk {id} is owned by {} at epoch {}; this node did not win the claim",
                cas.current_str("owner"),
                cas.current_i64("epoch").unwrap_or(-1)
            )));
        }

        // The replica set is whatever the map says, minus this node: a vdisk does not
        // replicate to itself over TCP.
        let replica_nodes: Vec<String> = row
            .get("replicas")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
            .unwrap_or_default();
        let mut replicas = Vec::new();
        for node in &replica_nodes {
            if node == &self.cfg.node {
                continue;
            }
            match self.peers.get(node) {
                Some(client) => replicas.push(Arc::clone(client)),
                None => {
                    return Err(Error::refused(format!(
                        "vdisk {id} replicates to {node}, which this daemon has no address \
                         for. Refusing to serve it: attaching without every replica would \
                         acknowledge writes that are not on the nodes the map claims."
                    )))
                }
            }
        }

        let mut fence_clients = Vec::new();
        for node in &replica_nodes {
            if node != &self.cfg.node {
                if let Some(c) = self.fence_peers.get(node) {
                    fence_clients.push(Arc::clone(c));
                }
            }
        }

        let mut vdisk = Vdisk::open(
            &id, new_epoch as u64, &self.vdisk_cfg(), self.daruk(), replicas,
            replica_nodes.clone(),
        )?;
        // Steps 2 and 3 of the takeover: fence every reachable replica at the epoch just
        // won, then rebuild from one of them. Done before a single byte is served, so a
        // guest never reads a state the previous owner could still add to.
        let fenced = vdisk.fence_and_recover(&fence_clients)?;
        let vdisk = Arc::new(Mutex::new(vdisk));

        let (socket, stop) = self.serve_socket(&id, Arc::new(LocalVdisk(Arc::clone(&vdisk))))?;
        self.attached.lock().expect("attached mutex poisoned").insert(
            id.clone(),
            Attached {
                vdisk: Some(vdisk),
                socket: socket.clone(),
                stop,
                forwarding_to: None,
            },
        );

        Ok(json!({
            "vdisk_id": id,
            "socket": socket.to_string_lossy(),
            "epoch": new_epoch,
            "previous_owner": cur_owner,
            "replicas": replica_nodes,
            "replicas_fenced": fenced,
        }))
    }

    /// The vdisk this node owns under `id`, or a refusal that says why not.
    ///
    /// Two different "no"s, kept apart: not attached at all, versus attached in
    /// forwarding mode. The second is a normal state -- a VM that migrated here before
    /// its storage did -- and an operator seeing "not attached" for a disk that is
    /// visibly serving I/O would reasonably conclude something is broken.
    fn owned_vdisk(&self, id: &str) -> Result<Arc<Mutex<Vdisk>>> {
        let map = self.attached.lock().expect("attached mutex poisoned");
        let attached = map
            .get(id)
            .ok_or_else(|| Error::refused(format!("vdisk {id} is not attached")))?;
        match &attached.vdisk {
            Some(v) => Ok(Arc::clone(v)),
            None => Err(Error::refused(format!(
                "vdisk {id} is being forwarded to {}, not owned here. Take ownership \
                 before asking this node to act on it.",
                attached.forwarding_to.as_deref().unwrap_or("another node")
            ))),
        }
    }

    /// Bind the per-vdisk NBD socket and start serving `backend` on it.
    fn serve_socket(
        &self,
        id: &str,
        backend: Arc<dyn nbd::Backend>,
    ) -> Result<(PathBuf, Arc<AtomicBool>)> {
        let socket = self.cfg.root.join("nbd").join(format!("{id}.sock"));
        let _ = std::fs::remove_file(&socket);
        let listener = UnixListener::bind(&socket)
            .map_err(|e| Error::io(format!("cannot bind {}: {e}", socket.display())))?;

        // qemu runs as the `qemu` user, so the socket has to be group-owned by it: 0660
        // on a root:root socket is 0000 as far as qemu is concerned, and the VM fails to
        // start with a permission error that names the socket rather than the reason.
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o660))?;
        if let Some(gid) = group_id("qemu") {
            if let Err(e) = std::os::unix::fs::chown(&socket, None, Some(gid)) {
                eprintln!("sidon: could not give group qemu access to {}: {e}", socket.display());
            }
        } else {
            eprintln!("sidon: no 'qemu' group on this host; {} stays root-only", socket.display());
        }

        let stop = Arc::new(AtomicBool::new(false));
        let export = Export { backend, name: id.to_string() };
        let stop_thread = Arc::clone(&stop);
        thread::spawn(move || {
            for conn in listener.incoming() {
                if stop_thread.load(Ordering::SeqCst) {
                    break;
                }
                match conn {
                    Ok(s) => {
                        if let Err(e) = nbd::serve(s, &export) {
                            // A guest closing its disk shows up as a read error on the
                            // next header; that is a disconnect, not a fault.
                            eprintln!("sidon: nbd session for {} ended: {e}", export.name);
                        }
                    }
                    Err(e) => {
                        eprintln!("sidon: nbd accept failed: {e}");
                        break;
                    }
                }
            }
        });
        Ok((socket, stop))
    }

    fn op_detach(&self, req: &Value) -> Result<Value> {
        let id = str_field(req, "vdisk_id")?;
        let attached = {
            let mut map = self.attached.lock().expect("attached mutex poisoned");
            map.remove(&id)
        };
        let a = attached.ok_or_else(|| Error::refused(format!("vdisk {id} is not attached")))?;

        // Drain before releasing: a clean detach should leave an empty journal so the
        // next open has nothing to replay. A failure here is reported, not swallowed --
        // the data is still safe in the journal, but somebody needs to know.
        let drain_result = match &a.vdisk {
            Some(handle) => handle.lock().expect("vdisk mutex poisoned").close(),
            // Forwarding: nothing local to drain. The owner still holds the journal, and
            // draining is its business.
            None => Ok(()),
        };

        a.stop.store(true, Ordering::SeqCst);
        // Unblock the accept loop by connecting to it once, then remove the socket.
        let _ = UnixStream::connect(&a.socket);
        let _ = std::fs::remove_file(&a.socket);

        match drain_result {
            Ok(()) => Ok(json!({"vdisk_id": id, "drained": true})),
            Err(e) => Ok(json!({
                "vdisk_id": id,
                "drained": false,
                "warning": format!("detached, but the final drain failed: {e}")
            })),
        }
    }

    fn op_delete(&self, req: &Value) -> Result<Value> {
        let id = str_field(req, "vdisk_id")?;
        if self.attached.lock().expect("attached mutex poisoned").contains_key(&id) {
            return Err(Error::refused(format!(
                "vdisk {id} is attached; detach it before deleting"
            )));
        }
        let daruk = self.daruk();
        // Map rows first, then the vdisk row. The reverse order would leave orphaned map
        // rows pointing into egroups with no vdisk to explain them, which is exactly the
        // state Purah cannot distinguish from a bug.
        daruk.query(&format!(
            "DELETE FROM hydra.dfs_block_map WHERE vdisk_id = {}",
            cql_str(&id)
        ))?;
        daruk.query(&format!(
            "DELETE FROM hydra.dfs_vdisks WHERE vdisk_id = {}",
            cql_str(&id)
        ))?;
        let journal = self.cfg.root.join("journal").join(format!("{id}.jrn"));
        let _ = std::fs::remove_file(&journal);
        // Extent groups are left for Purah: they may be shared with snapshots, and
        // deleting shared data because one referrer went away is the bug refcounts exist
        // to cause. Mark-sweep reclaims them when nothing points at them.
        Ok(json!({"vdisk_id": id, "deleted": true, "egroups": "left for purah"}))
    }

    /// Freeze a vdisk: rw -> immutable, permanently.
    ///
    /// Drains first, so everything the writer put there is in extent groups before the
    /// class changes -- an immutable vdisk whose journal still held un-drained writes
    /// would be frozen around data it could no longer drain, since the drain itself is a
    /// write path. Then detaches: an immutable vdisk has no owner and no epoch, and any
    /// node may serve reads from it.
    fn op_seal(&self, req: &Value) -> Result<Value> {
        let id = str_field(req, "vdisk_id")?;
        let vdisk = self.owned_vdisk(&id)?;
        {
            let mut v = vdisk.lock().expect("vdisk mutex poisoned");
            if v.class == CLASS_IMMUTABLE {
                return Ok(json!({"vdisk_id": id, "class": CLASS_IMMUTABLE, "already_sealed": true}));
            }
            v.close()?;
        }
        let cas = self.daruk().cas(
            "/v1/dfs/vdisk-seal",
            json_params(vec![
                ("vdisk_id", json!(id)),
                ("expected_class", json!(CLASS_RW)),
            ]),
        )?;
        if !cas.applied {
            return Err(Error::refused(format!(
                "vdisk {id} could not be sealed: its class is {}",
                cas.current_str("class")
            )));
        }
        self.op_detach(req)?;
        Ok(json!({"vdisk_id": id, "class": CLASS_IMMUTABLE, "sealed": true}))
    }

    /// Point-in-time copy of a vdisk, as an immutable child.
    ///
    /// The whole operation is a map copy. Sealed extent groups are immutable, so parent
    /// and snapshot can share every one of them and neither can disturb the other: a
    /// write to the parent is redirect-on-write, appending somewhere new and repointing
    /// only the parent's own map. Nothing copies a byte of data, so this is O(extents in
    /// the map) rather than O(bytes on disk), and a snapshot of a terabyte costs the same
    /// as a snapshot of a gigabyte.
    ///
    /// Nothing here touches reference counts, because there are none. Purah marks from
    /// `dfs_block_map` in its entirety, so a group referenced by the snapshot's rows is
    /// live whether or not the snapshot is attached, and whether or not the parent still
    /// exists. That is the property that makes snapshots nearly free here and it is worth
    /// naming: the decision not to keep refcounts is what paid for this.
    fn op_snapshot(&self, req: &Value) -> Result<Value> {
        self.derive_child(req, CLASS_IMMUTABLE)
    }

    /// Writable copy of a vdisk.
    ///
    /// Identical to a snapshot except for the class the child ends up in. Safe for the
    /// same reason: both sides redirect their writes into new extents and neither ever
    /// writes into a shared sealed group. This is what clone-from-image is -- an image is
    /// already immutable, so a fleet of VMs cloned from one template shares its extents
    /// until each writes, and deduplication would be buying back something never spent.
    fn op_clone(&self, req: &Value) -> Result<Value> {
        self.derive_child(req, CLASS_RW)
    }

    fn derive_child(&self, req: &Value, class: &str) -> Result<Value> {
        let parent_id = str_field(req, "vdisk_id")?;
        let child_id = str_field(req, "child_id")?;
        if child_id == parent_id {
            return Err(Error::refused(
                "a vdisk cannot be its own snapshot".to_string(),
            ));
        }
        let daruk = self.daruk();

        let rows = daruk.query(&format!(
            // The parent's `rf` is deliberately not read. It records the policy the parent
            // was created under, and the child is created under the policy in force now.
            "SELECT class, size_bytes, container, extent_bytes, egroup_bytes, replicas \
             FROM hydra.dfs_vdisks WHERE vdisk_id = {}",
            cql_str(&parent_id)
        ))?;
        let parent = rows
            .first()
            .ok_or_else(|| Error::refused(format!("vdisk {parent_id} does not exist")))?;
        let parent_class = parent.get("class").and_then(Value::as_str).unwrap_or(CLASS_RW);

        // A writable parent has a journal, and a journal is data the map does not know
        // about yet. Draining it is what makes the copied map a complete answer -- and
        // only the owner can drain, so a writable parent has to be attached here.
        //
        // An immutable parent needs none of this: it has no journal, its map is final,
        // and any node may copy it. That is the clone-from-image case, and it is the
        // common one.
        match parent_class {
            CLASS_IMMUTABLE => {}
            CLASS_RW => {
                let vdisk = self.owned_vdisk(&parent_id).map_err(|_| {
                    Error::refused(format!(
                        "vdisk {parent_id} is writable, so it has to be drained before it can \
                         be copied, and only the node that owns it can do that. Attach it \
                         here first, or seal it."
                    ))
                })?;
                let mut v = vdisk.lock().expect("vdisk mutex poisoned");
                if v.needs_drain() {
                    v.drain()?;
                }
            }
            other => {
                return Err(Error::refused(format!(
                    "vdisk {parent_id} has class '{other}' and cannot be copied"
                )))
            }
        }

        let size = parent.get("size_bytes").and_then(Value::as_i64).unwrap_or(0).max(0);
        let container = parent.get("container").and_then(Value::as_str).unwrap_or("default");
        let extent_bytes = parent.get("extent_bytes").and_then(Value::as_i64).unwrap_or(1 << 20);
        let egroup_bytes = parent.get("egroup_bytes").and_then(Value::as_i64).unwrap_or(4 << 20);
        let replicas: Vec<String> = parent
            .get("replicas")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
            .unwrap_or_else(|| vec![self.cfg.node.clone()]);

        // The child is a new vdisk, so it is created under the policy in force now rather
        // than the one its parent was made under. Inheriting the parent's `rf` is what
        // this line used to do, and with every parent on the cluster recorded at 1 it
        // meant a clone could never be more durable than the defect that made its parent
        // -- the single-copy default propagating itself one generation at a time.
        //
        // What the child *does* inherit is the replica set, because the extents are
        // shared: the copies that exist are exactly where the parent's are, and placing
        // the child elsewhere would mean copying every byte, which is the one cost a
        // snapshot exists not to pay.
        //
        // So a child can be born naming more copies than it has, which `op_create`
        // refuses outright for a fresh vdisk. The asymmetry is deliberate. A fresh create
        // chooses its own nodes and has no excuse for a set that is short; a clone's
        // placement is decided by where its parent's extents already sit. Recording the
        // shortfall is the honest option -- it is what `valcli storage.replication` reads,
        // and nothing anywhere grows the set on the strength of it.
        let rf = match req.get("rf").and_then(Value::as_u64) {
            Some(rf) => rf.max(1) as i64,
            None => self.default_copies(container) as i64,
        };

        // The child row goes in first, as CLASS_FORMING, and the map follows.
        //
        // Neither order is free of a crash window, so the question is which leftover a
        // later reader can name. Map-rows-first leaves a block map with no vdisk to
        // explain it, which is the one state Purah cannot tell from a bug. Row-first at
        // the final class would leave an attachable disk whose extents are half copied.
        // A row in a class nothing will attach is neither: it says exactly what it is.
        let cas = daruk.cas(
            "/v1/dfs/vdisk-create",
            json_params(vec![
                ("vdisk_id", json!(child_id)),
                ("container", json!(container)),
                ("size_bytes", json!(size)),
                ("class", json!(CLASS_FORMING)),
                ("owner", json!("")),
                ("epoch", json!(0)),
                ("drain_seq", json!(0)),
                ("extent_bytes", json!(extent_bytes)),
                ("egroup_bytes", json!(egroup_bytes)),
                ("created_at_ms", json!(now_ms())),
                ("replicas", json!(replicas)),
                ("rf", json!(rf)),
                ("parent_vdisk", json!(parent_id)),
            ]),
        )?;
        if !cas.applied {
            return Err(Error::refused(format!("vdisk {child_id} already exists")));
        }

        let map_rows = daruk.query(&format!(
            "SELECT extent_index, egroup_id, egroup_offset, length, vdisk_hash FROM hydra.dfs_block_map \
             WHERE vdisk_id = {}",
            cql_str(&parent_id)
        ))?;
        let mut copied: Vec<(u64, String, u32, u32, u64)> = Vec::with_capacity(map_rows.len());
        for row in map_rows {
            let idx = field_u64(&row, "extent_index")?;
            let egroup_id = row
                .get("egroup_id")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::meta("block map row without egroup_id".to_string()))?
                .to_string();
            let offset = field_u64(&row, "egroup_offset")? as u32;
            let length = field_u64(&row, "length")? as u32;
            // Carried through, never recomputed. These extents keep the identity the
            // parent wrote them under; rewriting them to say "the child wrote this"
            // would be a lie, and copying them so it were true would make a snapshot a
            // data copy, which is the entire thing it is not.
            let vh = row
                .get("vdisk_hash")
                .and_then(Value::as_i64)
                .map(|v| v as u64)
                .unwrap_or_else(|| vdisk_hash(&parent_id));
            copied.push((idx, egroup_id, offset, length, vh));
        }
        let extents = copied.len();
        for batch in block_map_batches(&child_id, 0, &copied, MAP_BATCH) {
            daruk.query(&batch)?;
        }

        // Only now is it a disk. Conditional on the class this call put there, so two
        // concurrent snapshots into the same name cannot both believe they finished.
        let done = daruk.cas(
            "/v1/dfs/vdisk-class",
            json_params(vec![
                ("vdisk_id", json!(child_id)),
                ("class", json!(class)),
                ("expected_class", json!(CLASS_FORMING)),
            ]),
        )?;
        if !done.applied {
            return Err(Error::refused(format!(
                "vdisk {child_id} was built but its class could not be set: it is {}",
                done.current_str("class")
            )));
        }

        Ok(json!({
            "vdisk_id": child_id,
            "parent_vdisk": parent_id,
            "class": class,
            "size_bytes": size,
            "extents": extents,
            "bytes_copied": 0,
        }))
    }

    /// Grow a vdisk. Refuses to shrink, always.
    ///
    /// A vdisk is sparse and the map is keyed by extent index, so growing needs no data
    /// movement: the new range simply has no map entries and reads as zeroes, which is
    /// exactly what a freshly grown disk should contain.
    fn op_resize(&self, req: &Value) -> Result<Value> {
        let id = str_field(req, "vdisk_id")?;
        let new_size = u64_field(req, "size_bytes")?;
        let vdisk = self.owned_vdisk(&id)?;
        let mut v = vdisk.lock().expect("vdisk mutex poisoned");
        if v.class == CLASS_IMMUTABLE {
            return Err(Error::refused(format!("vdisk {id} is immutable and cannot be resized")));
        }
        if new_size == v.size {
            return Ok(json!({"vdisk_id": id, "size_bytes": v.size, "unchanged": true}));
        }
        if new_size < v.size {
            return Err(Error::refused(format!(
                "refusing to shrink vdisk {id} from {} to {new_size} bytes: everything past                  the new end would be discarded, which no guest filesystem survives",
                v.size
            )));
        }
        let cas = self.daruk().cas(
            "/v1/dfs/vdisk-resize",
            json_params(vec![
                ("vdisk_id", json!(id)),
                ("size_bytes", json!(new_size as i64)),
                ("expected_size_bytes", json!(v.size as i64)),
            ]),
        )?;
        if !cas.applied {
            return Err(Error::refused(format!(
                "vdisk {id} was resized by someone else: the map says {} bytes, this caller                  read {}",
                cas.current_i64("size_bytes").unwrap_or(-1),
                v.size
            )));
        }
        v.size = new_size;
        // Connected guests keep the size they were told at handshake; libvirt's
        // blockresize is what makes qemu re-read it. New connections see it immediately.
        Ok(json!({"vdisk_id": id, "size_bytes": new_size}))
    }

    /// What this node's extent store holds and how much room is left.
    ///
    /// Read from the filesystem rather than summed from the map, deliberately. The map
    /// says how many bytes vdisks *claim*; the filesystem says how many are actually
    /// consumed, and those differ by every sparse hole, every extent group not yet
    /// reclaimed, and every footer. A capacity gate that refuses a VM needs the second
    /// number -- the DRS gate failing open for a year was exactly this distinction going
    /// unnoticed.
    /// What this node's extent store holds, per disk and in total.
    ///
    /// The per-disk breakdown is the point rather than a nicety: with more than one disk
    /// an operator needs to see one of them filling faster than the others, and a single
    /// total is precisely what hides that. The summed fields are kept at the top level
    /// because every existing reader -- Spectrum's pool cards, valcli's extent-store
    /// table, the capacity gate -- was written against them.
    ///
    /// A disk whose filesystem will not answer `statvfs` is reported with nulls rather
    /// than zeroes. Zero capacity and unknown capacity are different statements, and only
    /// one of them means "full".
    fn op_capacity(&self) -> Result<Value> {
        let disks = crate::extent::discover_disks(&self.cfg.root);
        let mut per_disk = Vec::new();
        let mut total = 0u64;
        let mut avail = 0u64;
        let mut used = 0u64;
        let mut groups = 0u64;
        let mut readable = 0usize;

        for disk in &disks {
            let space = crate::extent::disk_space(&disk.root);
            let mut disk_used = 0u64;
            let mut disk_groups = 0u64;
            if let Ok(entries) = std::fs::read_dir(&disk.root) {
                for entry in entries.flatten() {
                    if let Ok(meta) = entry.metadata() {
                        if meta.is_file() {
                            disk_used += meta.len();
                            disk_groups += 1;
                        }
                    }
                }
            }
            used += disk_used;
            groups += disk_groups;
            match space {
                Some((t, a)) => {
                    total += t;
                    avail += a;
                    readable += 1;
                    per_disk.push(json!({
                        "id": disk.id,
                        "path": disk.root.to_string_lossy(),
                        "total_bytes": t,
                        "available_bytes": a,
                        "egroup_bytes": disk_used,
                        "egroup_count": disk_groups,
                    }));
                }
                None => per_disk.push(json!({
                    "id": disk.id,
                    "path": disk.root.to_string_lossy(),
                    "total_bytes": Value::Null,
                    "available_bytes": Value::Null,
                    "egroup_bytes": disk_used,
                    "egroup_count": disk_groups,
                })),
            }
        }

        if readable == 0 {
            return Err(Error::io(
                "no disk in the extent store would report its capacity".to_string()));
        }

        let mut journal = 0u64;
        if let Ok(entries) = std::fs::read_dir(self.cfg.root.join("journal")) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    journal += meta.len();
                }
            }
        }

        Ok(json!({
            "node": self.cfg.node,
            // The first disk's path, for readers that show one. The full list is `disks`.
            "path": disks.first().map(|d| d.root.to_string_lossy().to_string())
                .unwrap_or_default(),
            "total_bytes": total,
            "available_bytes": avail,
            "egroup_bytes": used,
            "egroup_count": groups,
            "journal_bytes": journal,
            "disks": per_disk,
            "disk_count": disks.len(),
        }))
    }

    /// Which peers this node can reach right now.
    ///
    /// Reachability is not safety -- an append needs every replica, and an unreachable
    /// one fails the write rather than being skipped -- but an operator looking at a
    /// vdisk that will not accept writes needs to see which peer is down without reading
    /// a log.
    fn op_peers(&self) -> Result<Value> {
        let mut out = Vec::new();
        for (node, client) in self.peers.iter() {
            let (reachable, detail) = match client.ping() {
                Ok(()) => (true, String::new()),
                Err(e) => (false, e.to_string()),
            };
            out.push(json!({"node": node, "reachable": reachable, "detail": detail}));
        }
        out.sort_by(|a, b| a["node"].as_str().cmp(&b["node"].as_str()));
        Ok(json!({"node": self.cfg.node, "peers": out}))
    }

    fn op_list(&self) -> Result<Value> {
        let map = self.attached.lock().expect("attached mutex poisoned");
        let mut out = Vec::new();
        for (id, a) in map.iter() {
            match &a.vdisk {
                Some(handle) => {
                    let v = handle.lock().expect("vdisk mutex poisoned");
                    // The replica set is listed here rather than left to a per-vdisk
                    // `status` call. A console rendering a storage page wants the replica
                    // count of every vdisk at once, and asking one question per vdisk
                    // turns one page load into an N+1 fan-out over mTLS.
                    out.push(json!({
                        "vdisk_id": id,
                        "socket": a.socket.to_string_lossy(),
                        "epoch": v.epoch,
                        "size_bytes": v.size,
                        "degraded": v.degraded,
                        "class": v.class,
                        "replicas": v.map_replicas(),
                        // Listed beside the set for the same reason `status` carries it:
                        // a page that renders the replica count against the cluster's
                        // redundancy factor is comparing what exists to what policy asks
                        // for, and neither of those is what this vdisk asked for.
                        "rf": v.rf,
                        "role": "owner",
                    }));
                }
                None => out.push(json!({
                    "vdisk_id": id,
                    "socket": a.socket.to_string_lossy(),
                    "role": "forwarding",
                    "forwarding_to": a.forwarding_to,
                })),
            }
        }
        Ok(json!({"attached": out}))
    }

    fn op_status(&self, req: &Value) -> Result<Value> {
        let id = str_field(req, "vdisk_id")?;
        let vdisk = self.owned_vdisk(&id)?;
        let v = vdisk.lock().expect("vdisk mutex poisoned");
        Ok(v.stats())
    }

    fn op_flush(&self, req: &Value) -> Result<Value> {
        let id = str_field(req, "vdisk_id")?;
        let vdisk = self.owned_vdisk(&id)?;
        let mut v = vdisk.lock().expect("vdisk mutex poisoned");
        if v.needs_drain() {
            v.drain()?;
        }
        Ok(v.stats())
    }
}

/// Resolve a group name to its gid by reading `/etc/group`.
///
/// Parsed directly rather than through libc's `getgrnam`: this daemon has no C
/// dependencies and adding one for a four-field colon-separated lookup would be the most
/// expensive line in the build. Hosts using LDAP or SSSD for the *qemu* group do not
/// exist -- it is created by the qemu package, locally, at install time.
fn group_id(name: &str) -> Option<u32> {
    let content = std::fs::read_to_string("/etc/group").ok()?;
    for line in content.lines() {
        let mut fields = line.split(':');
        if fields.next() == Some(name) {
            return fields.nth(1).and_then(|gid| gid.parse().ok());
        }
    }
    None
}

impl Daemon {
    /// Extent groups every attached vdisk is using, gathered under their locks.
    fn held_egroups(&self) -> HashSet<String> {
        let map = self.attached.lock().expect("attached mutex poisoned");
        let mut held = HashSet::new();
        for a in map.values() {
            // A forwarded vdisk's extents are held on the owner, not here, and the sweep
            // on this node has no business protecting them.
            if let Some(handle) = &a.vdisk {
                let v = handle.lock().expect("vdisk mutex poisoned");
                held.extend(v.held_egroups());
            }
        }
        held
    }

    /// Restore the replica count on every vdisk this node owns.
    ///
    /// Purah's re-replication, driven from the owner because the owner is the node that
    /// has the data. Only owned vdisks: a forwarding node has nothing to copy, and a node
    /// that merely holds a replica is not entitled to rewrite the set.
    ///
    /// Two things can make a vdisk short of its copies, and by default this heals only
    /// one of them. A replica that stopped answering is an *emergency*: the journal is
    /// write-all, so the guest is taking EIO until the set is restored, and that is what
    /// the five-second watcher and the timer both call this for. A vdisk that simply
    /// never asked for enough copies is not an emergency, and until the create-time
    /// default was fixed that described every vdisk on the cluster -- healing it on the
    /// timer would have turned a metadata fix into an unannounced full-cluster data copy
    /// the first time a node restarted.
    ///
    /// So `restore_rf` is opt-in and reaches this only from an operator typing it. It
    /// adds one replica per vdisk per call, the same as the emergency path: growing a set
    /// by one node is a bounded amount of copying that can be watched, and an operator
    /// topping up a fleet would rather run this four times than start something they
    /// cannot see the end of.
    fn op_purah_heal(&self, req: &Value) -> Result<Value> {
        let restore_rf = req.get("restore_rf").and_then(Value::as_bool).unwrap_or(false);
        // Narrows the pass to one vdisk. Without it, an operator asking to top up a single
        // disk would top up every disk this node owns and be shown a report about one of
        // them -- which is the opposite of making the copying deliberate. Never used by
        // the emergency paths: a write failing is a fact about the node, and healing only
        // the vdisk that happened to notice first would leave its neighbours to discover
        // the same dead replica one guest at a time.
        let only = req.get("vdisk_id").and_then(Value::as_str);
        let owned: Vec<(String, Arc<Mutex<Vdisk>>)> = {
            let map = self.attached.lock().expect("attached mutex poisoned");
            map.iter()
                .filter(|(id, _)| only.map(|want| want == id.as_str()).unwrap_or(true))
                .filter_map(|(id, a)| a.vdisk.as_ref().map(|v| (id.clone(), Arc::clone(v))))
                .collect()
        };
        if owned.is_empty() {
            if let Some(want) = only {
                return Err(Error::refused(format!(
                    "vdisk {want} is not owned by this node, so it has no data here to copy \
                     from. Re-replication runs on the owner."
                )));
            }
        }

        let mut healed = Vec::new();
        let mut degraded = Vec::new();
        for (id, handle) in owned {
            let (before, down, epoch, want) = {
                let v = handle.lock().expect("vdisk mutex poisoned");
                let (_up, down) = v.replica_health();
                // The map's set, this node included -- the CAS is conditioned on what the
                // map holds, not on the subset this node happens to dial.
                (v.map_replicas(), down, v.epoch, v.rf as usize)
            };
            // Short of what it asked for, counting only the members that answer: a set of
            // two with one unreachable node is one copy short whichever of the two reasons
            // put it there, and healing it once should not leave it still short.
            let short = restore_rf && before.len().saturating_sub(down.len()) < want;
            if down.is_empty() && !short {
                continue;
            }

            // A spare is a configured peer that is answering and is not already a member.
            let mut spare = None;
            for (node, client) in self.peers.iter() {
                if before.contains(node) || node == &self.cfg.node {
                    continue;
                }
                if client.ping().is_ok() {
                    spare = Some((node.clone(), Arc::clone(client)));
                    break;
                }
            }
            let (spare_node, spare_client) = match spare {
                Some(v) => v,
                None => {
                    // Nothing to heal onto. Reported rather than retried silently: a
                    // cluster that cannot restore its redundancy is a fact an operator
                    // needs, and the vdisk keeps working in the meantime.
                    degraded.push(json!({
                        "vdisk_id": id, "unreachable": down,
                        "detail": "no spare node is available to re-replicate onto",
                    }));
                    continue;
                }
            };

            // The map first, then the data. The CAS is conditional on the set that was
            // read and on the epoch, so a deposed owner loses this race rather than
            // rewriting the durability guarantee of a disk it no longer owns.
            let mut after: Vec<String> = before.iter().filter(|n| !down.contains(n)).cloned().collect();
            after.push(spare_node.clone());

            let copied = {
                let mut v = handle.lock().expect("vdisk mutex poisoned");
                match v.add_replica(Arc::clone(&spare_client)) {
                    Ok(n) => n,
                    Err(e) => {
                        degraded.push(json!({
                            "vdisk_id": id, "unreachable": down,
                            "detail": format!("re-replication onto {spare_node} failed: {e}"),
                        }));
                        continue;
                    }
                }
            };

            // A failed CAS and a refused CAS get the same treatment. An error here used
            // to propagate and abort the loop, which left the new member in the write-all
            // set while the map did not list it -- safe, since writes reaching more nodes
            // than the map claims is not a durability lie, but inconsistent and forgotten
            // on the next restart. Back it out either way.
            let cas = match self.daruk().cas(
                "/v1/dfs/set-replicas",
                json_params(vec![
                    ("vdisk_id", json!(id)),
                    ("replicas", json!(after)),
                    ("expected_replicas", json!(before)),
                    ("expected_epoch", json!(epoch as i64)),
                ]),
            ) {
                Ok(c) => c,
                Err(e) => {
                    let mut v = handle.lock().expect("vdisk mutex poisoned");
                    v.remove_replica(&spare_node);
                    degraded.push(json!({
                        "vdisk_id": id,
                        "detail": format!("could not record the new replica set ({e}); backed out"),
                    }));
                    continue;
                }
            };
            if !cas.applied {
                // Somebody else changed the set, or this node was deposed. Back the new
                // member out of the write-all set rather than acknowledging writes to a
                // node the map does not list.
                let mut v = handle.lock().expect("vdisk mutex poisoned");
                v.remove_replica(&spare_node);
                degraded.push(json!({
                    "vdisk_id": id,
                    "detail": "the replica set changed underneath this heal; backed out",
                }));
                continue;
            }

            {
                let mut v = handle.lock().expect("vdisk mutex poisoned");
                for lost in &down {
                    v.remove_replica(lost);
                }
                v.set_map_replicas(after.clone());
            }
            healed.push(json!({
                "vdisk_id": id, "replaced": down, "with": spare_node,
                "extents_copied": copied,
            }));
        }
        Ok(json!({"healed": healed, "degraded": degraded}))
    }

    fn op_purah_sweep(&self) -> Result<Value> {
        // The curator state -- which groups have been seen unreferenced, and since when --
        // lives across sweeps, so it is held by the daemon rather than rebuilt per call.
        // Two consecutive observations is the rule; a fresh Purah each time would reset
        // that and could reclaim on first sight.
        let held = self.held_egroups();
        let mut purah = self.purah_state.lock().expect("purah mutex poisoned");
        let report = purah.sweep(&held, now_ms())?;
        Ok(report.to_json())
    }

    fn op_purah_scrub(&self) -> Result<Value> {
        let purah = self.purah_state.lock().expect("purah mutex poisoned");
        Ok(purah.scrub()?.to_json())
    }

    /// The hottest and coldest extent groups this node holds.
    ///
    /// Flushes first, then ranks. Without that an operator who has just run a workload and
    /// typed this would be shown the tally as of the last timer tick, concluded the busiest
    /// extent groups on the node were cold, and been right about the table and wrong about
    /// the disk. The flush is the cheap half -- one statement per fifty extent groups -- and
    /// the read it precedes is a full scan either way.
    ///
    /// A flush that fails does not fail the pass: the ranking is still the best answer
    /// available and saying nothing would be worse than saying something slightly stale.
    /// The report carries `window_ms` per row so the staleness is visible rather than
    /// implied.
    fn op_purah_heat(&self, req: &Value) -> Result<Value> {
        let limit = req
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(20)
            .clamp(1, 1000) as usize;
        let purah = self.purah_state.lock().expect("purah mutex poisoned");
        let now = now_ms();
        let flushed = match purah.flush_access(now) {
            Ok(r) => r.to_json(),
            Err(e) => {
                eprintln!("purah: heat flush before ranking failed: {e}");
                json!({"error": e.to_string()})
            }
        };
        let mut report = purah.heat(limit, now)?.to_json();
        if let Some(map) = report.as_object_mut() {
            map.insert("flushed".to_string(), flushed);
        }
        Ok(report)
    }

    /// The background loop. Sweeps, then scrubs, forever, logging anything it finds.
    pub fn start_purah(self: &Arc<Self>) {
        // Heal promptly when a write fails, not only on the timer.
        //
        // Write-all means an unreachable replica stops writes: the guest gets EIO until
        // the set is restored. That is the design's trade, and it is the right one -- a
        // quorum journal would keep writing and cost the three-line takeover proof -- but
        // waiting a full timer tick to *notice* turns a node loss into a multi-minute
        // write outage for no reason. A degraded vdisk is a signal, so the loop watches
        // for one and heals on the spot.
        let watcher = Arc::clone(self);
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(5));
            let degraded = {
                let map = watcher.attached.lock().expect("attached mutex poisoned");
                map.values().any(|a| {
                    a.vdisk.as_ref().map(|v| {
                        v.lock().expect("vdisk mutex poisoned").degraded.is_some()
                    }).unwrap_or(false)
                })
            };
            if degraded {
                // No `restore_rf`: this fires on a write failing, and the answer to that
                // is to replace what stopped answering, not to start copying disks that
                // are serving their guests perfectly well.
                if let Err(e) = watcher.op_purah_heal(&json!({})) {
                    eprintln!("purah: prompt re-replication failed: {e}");
                }
            }
        });

        // The heat tally, on its own timer.
        //
        // Separate from the sweep deliberately, and far more often. The sweep is minutes
        // because reclaiming late costs disk and reclaiming early costs data; the tally is
        // a statistic nobody waits on, and the only thing a long interval buys is a larger
        // window of counts to lose in a crash. One statement per fifty extent groups per
        // minute is not a load worth economising on.
        //
        // Its own thread rather than a step in the sweep loop for the same reason: a sweep
        // that is slow or stalled against an unreachable Hydra must not also stop the
        // counters reaching it, because the pass that ranks them is the one an operator
        // runs when they are trying to find out why a disk is busy.
        let flusher = Arc::clone(self);
        let flush_every = flusher.cfg.access_flush;
        if flush_every.is_zero() {
            println!(
                "sidon: extent-group access tally disabled (SIDON_ACCESS_FLUSH=0); \
                 purah-heat will rank nothing and tiering has no input"
            );
        } else {
            thread::spawn(move || loop {
                thread::sleep(flush_every);
                let purah = flusher.purah_state.lock().expect("purah mutex poisoned");
                match purah.flush_access(now_ms()) {
                    Ok(r) => {
                        if r.dropped > 0 {
                            // Said once per flush and not swallowed: a capped tally means
                            // the ranking is describing part of the node, and a ranking that
                            // silently describes part of a node is the one that gets acted
                            // on.
                            eprintln!(
                                "purah: the extent-group access tally is at capacity; \
                                 {} group(s) have gone uncounted",
                                r.dropped
                            );
                        }
                    }
                    // Nothing waits on this. Hydra being unreachable costs a statistic, and
                    // the next tick carries the same totals plus whatever arrived since --
                    // the flush writes absolute values, so a missed one leaves no hole.
                    Err(e) => eprintln!("purah: access tally flush failed: {e}"),
                }
                drop(purah);
            });
        }

        let me = Arc::clone(self);
        let interval = me.cfg.purah_interval;
        if interval.is_zero() {
            // Reclamation and scrub off, redundancy watching still on. They answer to
            // different concerns -- one is about disk, the other about surviving a node
            // loss -- and an operator who turns off the garbage collector has not asked
            // to stop restoring replicas.
            println!("sidon: purah sweep/scrub disabled (interval 0); the redundancy watcher still runs");
            return;
        }

        thread::spawn(move || loop {
            thread::sleep(interval);
            match me.op_purah_sweep() {
                Ok(r) => {
                    let reclaimed = r.get("reclaimed").and_then(Value::as_array)
                        .map(|a| a.len()).unwrap_or(0);
                    if reclaimed > 0 {
                        println!("purah: reclaimed {reclaimed} extent group(s), {} bytes",
                                 r.get("bytes_reclaimed").and_then(Value::as_u64).unwrap_or(0));
                    }
                }
                // Hydra being unreachable is not a reason to stop curating forever; the
                // next tick tries again. Reclamation is allowed to be late.
                Err(e) => eprintln!("purah: sweep failed: {e}"),
            }
            match me.op_purah_heal(&json!({})) {
                Ok(r) => {
                    let healed = r.get("healed").and_then(Value::as_array).map(|a| a.len()).unwrap_or(0);
                    let degraded = r.get("degraded").and_then(Value::as_array).map(|a| a.len()).unwrap_or(0);
                    if healed > 0 || degraded > 0 {
                        println!("purah: re-replicated {healed} vdisk(s), {degraded} still degraded");
                    }
                }
                Err(e) => eprintln!("purah: re-replication failed: {e}"),
            }
            match me.op_purah_scrub() {
                Ok(r) => {
                    if r.get("clean").and_then(Value::as_bool) != Some(true) {
                        eprintln!("purah: SCRUB FOUND DAMAGE: {r}");
                    }
                }
                Err(e) => eprintln!("purah: scrub failed: {e}"),
            }
        });
    }
}

/// Guest I/O arriving from a node that forwarded it here.
///
/// Answers only for vdisks this node is actually serving. Returning None rather than an
/// error when it is not the owner is the distinction that matters: the forwarder learns
/// its map is stale and re-reads ownership, instead of retrying against a node that can
/// never help it.
impl Owned for Daemon {
    fn owned_read(&self, vdisk: &str, offset: u64, len: u32) -> Option<Result<Vec<u8>>> {
        let handle = {
            let map = self.attached.lock().expect("attached mutex poisoned");
            map.get(vdisk).and_then(|a| a.vdisk.clone())?
        };
        let mut v = handle.lock().expect("vdisk mutex poisoned");
        Some(v.read(offset, len))
    }

    fn owned_write(&self, vdisk: &str, offset: u64, data: &[u8]) -> Option<Result<()>> {
        let handle = {
            let map = self.attached.lock().expect("attached mutex poisoned");
            map.get(vdisk).and_then(|a| a.vdisk.clone())?
        };
        let mut v = handle.lock().expect("vdisk mutex poisoned");
        Some(v.write(offset, data))
    }
}

fn str_field(req: &Value, name: &str) -> Result<String> {
    req.get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| Error::refused(format!("request is missing '{name}'")))
}

fn u64_field(req: &Value, name: &str) -> Result<u64> {
    req.get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::refused(format!("request is missing numeric '{name}'")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_lookup_finds_a_real_group_and_misses_a_fake_one() {
        // root:x:0: is present on every Linux this runs on.
        if std::path::Path::new("/etc/group").exists() {
            assert_eq!(group_id("root"), Some(0));
            assert_eq!(group_id("definitely-not-a-group-9f2a"), None);
        }
    }

    #[test]
    fn field_extraction_rejects_empties() {
        let v = json!({"vdisk_id": "", "size_bytes": 10});
        assert!(str_field(&v, "vdisk_id").is_err());
        assert!(str_field(&v, "missing").is_err());
        assert_eq!(u64_field(&v, "size_bytes").unwrap(), 10);
        assert!(u64_field(&v, "vdisk_id").is_err());
    }

    /// A fault tolerance is not a copy count, and reading one as the other is what made
    /// every vdisk on the cluster single-copy.
    ///
    /// `cluster.json` says `redundancy_factor: 1` and a container says `ftt: 1`, and both
    /// mean "survive one host loss", which takes two copies. The column that records the
    /// answer is called `rf` and holds a number of copies. Nothing about either name says
    /// they differ, and the whole defect fits in the one that got dropped.
    #[test]
    fn a_fault_tolerance_of_one_asks_for_two_copies() {
        assert_eq!(copies_for_ftt(1, 3), 2);
        assert_eq!(copies_for_ftt(2, 3), 3);
    }

    /// ftt=0 is an operator's decision, not a missing value.
    ///
    /// It is what `cluster create` writes for a single-node cluster, and it means one
    /// copy. Worth pinning separately because it is the one input where reading the ftt
    /// straight into rf gives an answer that is wrong in the other direction -- zero
    /// copies -- and a guard that only ever tested ftt=1 would not have seen it.
    #[test]
    fn no_fault_tolerance_still_means_one_copy_rather_than_none() {
        assert_eq!(copies_for_ftt(0, 1), 1);
        assert_eq!(copies_for_ftt(0, 3), 1);
    }

    /// A cluster smaller than its own redundancy factor keeps working.
    ///
    /// This is not a corner case: it is any single-node deployment whose `cluster.json`
    /// was copied from a larger one, and a create that refused there would take a cluster
    /// that serves guests today and stop it doing so. The clamp is what makes the
    /// shortfall a recorded number instead of an outage.
    #[test]
    fn a_cluster_cannot_be_asked_for_more_copies_than_it_has_nodes() {
        assert_eq!(copies_for_ftt(1, 1), 1);
        assert_eq!(copies_for_ftt(5, 2), 2);
        // Even with the node count nonsensically zero, a vdisk has one copy: itself. A
        // zero here would be a create that records a durability claim of "no copies".
        assert_eq!(copies_for_ftt(2, 0), 1);
    }

    /// An ftt large enough to overflow the `+1` must not wrap to zero copies.
    ///
    /// `redundancy_factor` is operator-typed and parsed from a JSON document, so nothing
    /// upstream bounds it. Saturating rather than wrapping means a preposterous value
    /// clamps to the node count like any other too-large one.
    #[test]
    fn an_absurd_fault_tolerance_clamps_instead_of_wrapping() {
        assert_eq!(copies_for_ftt(u64::MAX, 3), 3);
    }
}
