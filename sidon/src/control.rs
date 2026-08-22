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
use crate::nbd::{self, Export};
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
}

struct Attached {
    vdisk: Arc<Mutex<Vdisk>>,
    socket: PathBuf,
    stop: Arc<AtomicBool>,
}

pub struct Daemon {
    cfg: DaemonConfig,
    attached: Mutex<HashMap<String, Attached>>,
    purah_state: Mutex<Purah>,
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
        Ok(Arc::new(Daemon {
            cfg,
            attached: Mutex::new(HashMap::new()),
            purah_state: Mutex::new(purah),
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
            ]),
        )?;
        if !cas.applied {
            return Err(Error::refused(format!("vdisk {id} already exists")));
        }
        Ok(json!({"vdisk_id": id, "size_bytes": size, "class": class}))
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
            "SELECT owner, epoch FROM hydra.dfs_vdisks WHERE vdisk_id = {}",
            cql_str(&id)
        ))?;
        let row = rows
            .first()
            .ok_or_else(|| Error::refused(format!("vdisk {id} does not exist")))?;
        let cur_owner = row.get("owner").and_then(Value::as_str).unwrap_or("").to_string();
        let cur_epoch = row.get("epoch").and_then(Value::as_i64).unwrap_or(0);
        let new_epoch = cur_epoch + 1;

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

        let vdisk = Vdisk::open(&id, new_epoch as u64, &self.vdisk_cfg(), self.daruk())?;
        let vdisk = Arc::new(Mutex::new(vdisk));

        let socket = self.cfg.root.join("nbd").join(format!("{id}.sock"));
        let _ = std::fs::remove_file(&socket);
        let listener = UnixListener::bind(&socket)
            .map_err(|e| Error::io(format!("cannot bind {}: {e}", socket.display())))?;
        // qemu runs as the `qemu` user, so the socket has to be group-owned by it: 0660
        // on a root:root socket is 0000 as far as qemu is concerned, and the VM fails to
        // start with a permission error that names the socket rather than the reason.
        // Group ownership plus 0660 is what grants qemu access without handing the world
        // a writable block device.
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o660))?;
        if let Some(gid) = group_id("qemu") {
            if let Err(e) = std::os::unix::fs::chown(&socket, None, Some(gid)) {
                eprintln!("sidon: could not give group qemu access to {}: {e}", socket.display());
            }
        } else {
            eprintln!("sidon: no 'qemu' group on this host; {} stays root-only", socket.display());
        }

        let stop = Arc::new(AtomicBool::new(false));
        let export = Export { vdisk: Arc::clone(&vdisk), name: id.clone() };
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

        self.attached.lock().expect("attached mutex poisoned").insert(
            id.clone(),
            Attached { vdisk, socket: socket.clone(), stop },
        );

        Ok(json!({
            "vdisk_id": id,
            "socket": socket.to_string_lossy(),
            "epoch": new_epoch,
            "previous_owner": cur_owner,
        }))
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
        let drain_result = {
            let mut v = a.vdisk.lock().expect("vdisk mutex poisoned");
            v.close()
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

    fn op_list(&self) -> Result<Value> {
        let map = self.attached.lock().expect("attached mutex poisoned");
        let mut out = Vec::new();
        for (id, a) in map.iter() {
            let v = a.vdisk.lock().expect("vdisk mutex poisoned");
            out.push(json!({
                "vdisk_id": id,
                "socket": a.socket.to_string_lossy(),
                "epoch": v.epoch,
                "size_bytes": v.size,
                "degraded": v.degraded,
            }));
        }
        Ok(json!({"attached": out}))
    }

    fn op_status(&self, req: &Value) -> Result<Value> {
        let id = str_field(req, "vdisk_id")?;
        let map = self.attached.lock().expect("attached mutex poisoned");
        let a = map
            .get(&id)
            .ok_or_else(|| Error::refused(format!("vdisk {id} is not attached")))?;
        let v = a.vdisk.lock().expect("vdisk mutex poisoned");
        Ok(v.stats())
    }

    fn op_flush(&self, req: &Value) -> Result<Value> {
        let id = str_field(req, "vdisk_id")?;
        let vdisk = {
            let map = self.attached.lock().expect("attached mutex poisoned");
            map.get(&id)
                .map(|a| Arc::clone(&a.vdisk))
                .ok_or_else(|| Error::refused(format!("vdisk {id} is not attached")))?
        };
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
            let v = a.vdisk.lock().expect("vdisk mutex poisoned");
            held.extend(v.held_egroups());
        }
        held
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
        let me = Arc::clone(self);
        let interval = me.cfg.purah_interval;
        if interval.is_zero() {
            println!("sidon: purah is disabled (interval 0)");
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
