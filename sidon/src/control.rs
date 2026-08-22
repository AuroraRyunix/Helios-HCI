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
use crate::meta::{cql_str, json_params, now_ms, Daruk};
use crate::nbd::{self, Export, LocalVdisk};
use crate::peer::{self, Forwarder, Owned, PeerClient, ReplicaStore};
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
}

impl Daemon {
    pub fn new(cfg: DaemonConfig) -> Result<Arc<Daemon>> {
        std::fs::create_dir_all(cfg.root.join("journal"))?;
        std::fs::create_dir_all(cfg.root.join("egroups"))?;
        std::fs::create_dir_all(cfg.root.join("nbd"))?;
        if let Some(parent) = cfg.control_socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let store = crate::extent::EgroupStore::new(&cfg.root.join("egroups"), 0)?;
        let purah = Purah::new(
            Daruk::new(&cfg.daruk_addr, cfg.daruk_timeout),
            store,
            &cfg.node,
            // Zero means zero. The safe default lives in main.rs where the environment is
            // read; silently substituting it here would make an explicit
            // SIDON_PURAH_GRACE=0 do something other than what it says, which is worse
            // than letting an operator who asked for no grace have none.
            cfg.purah_grace,
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
        }))
    }

    fn daruk(&self) -> Daruk {
        Daruk::new(&self.cfg.daruk_addr, self.cfg.daruk_timeout)
    }

    fn vdisk_cfg(&self) -> VdiskConfig {
        VdiskConfig {
            root: self.cfg.root.clone(),
            node: self.cfg.node.clone(),
            high_water: self.cfg.high_water,
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
            "resize" => self.op_resize(req),
            "capacity" => self.op_capacity(),
            "peers" => self.op_peers(),
            "purah-heal" => self.op_purah_heal(),
            "purah-sweep" => self.op_purah_sweep(),
            "purah-scrub" => self.op_purah_scrub(),
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
        let class = req.get("class").and_then(Value::as_str).unwrap_or("rw");
        if class != "rw" && class != "immutable" {
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
        let rf = req.get("rf").and_then(Value::as_u64).unwrap_or(1).max(1) as usize;
        let replicas: Vec<String> = match req.get("replicas").and_then(Value::as_array) {
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
            let class = row.get("class").and_then(Value::as_str).unwrap_or("rw");
            let backend = Arc::new(Forwarder {
                vdisk: id.clone(),
                size,
                read_only: class == "immutable",
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
            if v.class == "immutable" {
                return Ok(json!({"vdisk_id": id, "class": "immutable", "already_sealed": true}));
            }
            v.close()?;
        }
        let cas = self.daruk().cas(
            "/v1/dfs/vdisk-seal",
            json_params(vec![
                ("vdisk_id", json!(id)),
                ("expected_class", json!("rw")),
            ]),
        )?;
        if !cas.applied {
            return Err(Error::refused(format!(
                "vdisk {id} could not be sealed: its class is {}",
                cas.current_str("class")
            )));
        }
        self.op_detach(req)?;
        Ok(json!({"vdisk_id": id, "class": "immutable", "sealed": true}))
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
        if v.class == "immutable" {
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
    fn op_capacity(&self) -> Result<Value> {
        let root = self.cfg.root.join("egroups");
        let (total, avail) = statfs(&root)?;
        let mut used = 0u64;
        let mut groups = 0u64;
        if let Ok(entries) = std::fs::read_dir(&root) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_file() {
                        used += meta.len();
                        groups += 1;
                    }
                }
            }
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
            "path": root.to_string_lossy(),
            "total_bytes": total,
            "available_bytes": avail,
            "egroup_bytes": used,
            "egroup_count": groups,
            "journal_bytes": journal,
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
                    out.push(json!({
                        "vdisk_id": id,
                        "socket": a.socket.to_string_lossy(),
                        "epoch": v.epoch,
                        "size_bytes": v.size,
                        "degraded": v.degraded,
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
    fn op_purah_heal(&self) -> Result<Value> {
        let owned: Vec<(String, Arc<Mutex<Vdisk>>)> = {
            let map = self.attached.lock().expect("attached mutex poisoned");
            map.iter()
                .filter_map(|(id, a)| a.vdisk.as_ref().map(|v| (id.clone(), Arc::clone(v))))
                .collect()
        };

        let mut healed = Vec::new();
        let mut degraded = Vec::new();
        for (id, handle) in owned {
            let (before, down, epoch) = {
                let v = handle.lock().expect("vdisk mutex poisoned");
                let (_up, down) = v.replica_health();
                // The map's set, this node included -- the CAS is conditioned on what the
                // map holds, not on the subset this node happens to dial.
                (v.map_replicas(), down, v.epoch)
            };
            if down.is_empty() {
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
                if let Err(e) = watcher.op_purah_heal() {
                    eprintln!("purah: prompt re-replication failed: {e}");
                }
            }
        });

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
            match me.op_purah_heal() {
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

/// Total and available bytes of the filesystem holding `path`.
///
/// `statvfs` through a direct syscall rather than a crate: it is one call with a
/// well-known struct layout, and pulling in a libc binding for it would be the only C
/// dependency in the daemon.
fn statfs(path: &std::path::Path) -> Result<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    #[repr(C)]
    #[derive(Default)]
    struct StatVfs {
        f_bsize: u64,
        f_frsize: u64,
        f_blocks: u64,
        f_bfree: u64,
        f_bavail: u64,
        f_files: u64,
        f_ffree: u64,
        f_favail: u64,
        f_fsid: u64,
        f_flag: u64,
        f_namemax: u64,
        f_spare: [u32; 6],
    }
    extern "C" {
        fn statvfs(path: *const u8, buf: *mut StatVfs) -> i32;
    }
    let mut c_path: Vec<u8> = path.as_os_str().as_bytes().to_vec();
    c_path.push(0);
    let mut buf = StatVfs::default();
    // SAFETY: c_path is NUL-terminated and buf is a correctly sized, owned struct.
    let rc = unsafe { statvfs(c_path.as_ptr(), &mut buf) };
    if rc != 0 {
        return Err(Error::io(format!("statvfs({}) failed", path.display())));
    }
    let unit = if buf.f_frsize > 0 { buf.f_frsize } else { buf.f_bsize };
    Ok((buf.f_blocks.saturating_mul(unit), buf.f_bavail.saturating_mul(unit)))
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
}
