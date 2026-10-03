//! Rolling a detached vdisk back to one of its own snapshots, in place.
//!
//! A clone gives the old contents under a new name. That recovers data and does not put a
//! VM back: the VM's disk is still the vdisk it was defined with, and every reference to it
//! -- the libvirt domain, the `<vm>-disk<n>` convention, the block map row -- names the
//! original. Rolling back replaces the *contents* of that vdisk with a snapshot's, so
//! nothing that points at it has to be told.
//!
//! ## Why only while it is detached
//!
//! An attached vdisk is being read by a guest whose page cache, filesystem journal and
//! in-flight writes all assume the bytes under them change only when the guest changes them.
//! Swapping the map beneath it is not a write the guest issued; its filesystem would meet
//! blocks that contradict its own metadata. That case needs the guest to stop (or be told),
//! and the reasoning for doing it properly is in `docs/dfs/rollback_attached.md`. This
//! module refuses it instead of approximating it.
//!
//! ## The ownership dance, and why the order is what it is
//!
//! ```text
//! 1. class  rw -> rolling-back     nothing attaches a disk in this class
//! 2. claim  (owner, e) -> (me, e+1) the epoch moves; no replica may mistake pre-rollback
//!                                   state for current
//! 3. fence + truncate every replica at e+1, ALL of them, or stop
//! 4. delete the map, write the snapshot's map at epoch e+1
//! 5. class  rolling-back -> rw
//! ```
//!
//! Step 1 precedes step 2 on purpose. An attach that raced this call has already read the
//! row and holds an `(owner, epoch)` pair; it will try to claim with that pair. Whichever of
//! the two claims reaches Daruk first wins and the other is refused by the compare-and-swap,
//! so a lost race here is an error the caller sees and not two owners. The class flip is
//! what stops an attach that *starts* after step 2 from claiming `e+2` and serving a map
//! this call has half written.
//!
//! Step 3 requires every replica and not a quorum. A replica that misses the fence and the
//! truncate keeps a journal tail from before the rollback, and the next attach adopts the
//! longest tail it can read and replays it over the map. Replayed over the restored disk,
//! that is the old data returning, silently, on a vdisk the operator was told was rolled
//! back. Write-all is already how the journal works; this is the same rule applied to its
//! destruction.
//!
//! A crash anywhere after step 1 leaves the vdisk in `rolling-back`, which nothing attaches
//! and which a second rollback to the same snapshot completes: every step is a pure function
//! of the snapshot, so repeating one is harmless. That is what `resumed` means below.

use std::sync::Arc;

use serde_json::{json, Value};

use super::{str_field, Daemon};
use crate::err::{Error, Result};
use crate::meta::{
    block_map_batches, cql_str, json_params, now_ms, CLASS_FORMING, CLASS_IMMUTABLE, CLASS_RW,
    MAP_BATCH,
};
use crate::peer::{self, PeerClient, Request};

/// The class a vdisk holds while its map is being replaced.
///
/// Not `forming`: that class tells an operator "delete this, a snapshot did not finish",
/// which for a vdisk whose data is only partly restored is the one piece of advice that
/// destroys the snapshot's other half. This one says what is happening and that running the
/// rollback again is the way out.
pub const CLASS_ROLLING_BACK: &str = "rolling-back";

/// What a vdisk's row says about it, as far as a rollback is concerned.
#[derive(Debug, Clone, PartialEq)]
pub struct Facts {
    pub class: String,
    pub owner: String,
    pub epoch: i64,
    pub size_bytes: i64,
    pub extent_bytes: i64,
    pub parent_vdisk: String,
}

impl Facts {
    pub fn from_row(row: &Value) -> Facts {
        let text = |k: &str| row.get(k).and_then(Value::as_str).unwrap_or("").to_string();
        let num = |k: &str, default: i64| row.get(k).and_then(Value::as_i64).unwrap_or(default);
        Facts {
            class: row.get("class").and_then(Value::as_str).unwrap_or(CLASS_RW).to_string(),
            owner: text("owner"),
            epoch: num("epoch", 0),
            size_bytes: num("size_bytes", 0),
            extent_bytes: num("extent_bytes", 1 << 20),
            parent_vdisk: text("parent_vdisk"),
        }
    }
}

/// What a permitted rollback will do.
#[derive(Debug, PartialEq)]
pub struct Plan {
    /// The vdisk was already `rolling-back`: an earlier attempt did not finish and this
    /// one completes it.
    pub resumed: bool,
    /// The epoch the rollback claims: one above whatever the row holds.
    pub new_epoch: i64,
}

/// Every reason a rollback is refused, decided from rows and from whether this node is
/// serving the vdisk. Pure on purpose: the refusals are the safety of the operation, and a
/// refusal that can only be exercised against a live Hydra is a refusal nobody tests.
pub fn plan(
    vdisk_id: &str,
    vdisk: &Facts,
    snapshot_id: &str,
    snapshot: &Facts,
    this_node: &str,
    attached_here: bool,
) -> Result<Plan> {
    if vdisk_id == snapshot_id {
        return Err(Error::refused(
            "a vdisk cannot be rolled back to itself".to_string(),
        ));
    }
    if attached_here {
        return Err(Error::refused(format!(
            "vdisk {vdisk_id} is attached on this node, so a guest may be reading it. Rolling \
             it back would change the bytes under that guest's filesystem; stop the VM and \
             detach the disk first."
        )));
    }
    let resumed = match vdisk.class.as_str() {
        CLASS_RW => false,
        CLASS_ROLLING_BACK => true,
        CLASS_IMMUTABLE => {
            return Err(Error::refused(format!(
                "vdisk {vdisk_id} is immutable and has no contents to roll back; clone it \
                 to get a writable disk"
            )))
        }
        CLASS_FORMING => {
            return Err(Error::refused(format!(
                "vdisk {vdisk_id} is still being formed from its parent and has nothing to \
                 roll back"
            )))
        }
        other => {
            return Err(Error::refused(format!(
                "vdisk {vdisk_id} has class '{other}' and cannot be rolled back"
            )))
        }
    };
    if snapshot.class != CLASS_IMMUTABLE {
        return Err(Error::refused(format!(
            "{snapshot_id} is class '{}', not a snapshot: only an immutable copy is a fixed \
             point to roll back to",
            snapshot.class
        )));
    }
    // A snapshot of some other disk is a valid immutable vdisk and would restore without
    // complaint, which is what makes this a check rather than an assumption. Rolling a VM's
    // disk back to a different VM's snapshot is a reimage, and a reimage should be asked for
    // by name, not arrived at by a typo in a snapshot id.
    if snapshot.parent_vdisk != vdisk_id {
        let from = if snapshot.parent_vdisk.is_empty() {
            "no vdisk".to_string()
        } else {
            format!("'{}'", snapshot.parent_vdisk)
        };
        return Err(Error::refused(format!(
            "{snapshot_id} was taken from {from}, not from {vdisk_id}"
        )));
    }
    // Only the owner has the journal that must be discarded, and only the owner's node can
    // delete it. An unowned disk (never attached) has no journal at all.
    if !vdisk.owner.is_empty() && vdisk.owner != this_node {
        return Err(Error::refused(format!(
            "vdisk {vdisk_id} was last owned by {}; send the rollback there, because that \
             node holds its journal",
            vdisk.owner
        )));
    }
    if vdisk.extent_bytes != snapshot.extent_bytes {
        return Err(Error::refused(format!(
            "{snapshot_id} uses {}-byte extents and {vdisk_id} uses {}: the map indices would \
             not mean the same offsets",
            snapshot.extent_bytes, vdisk.extent_bytes
        )));
    }
    // A vdisk is grow-only, so it can only be larger than its own snapshot. Smaller means
    // the rows disagree about which disk this is, and restoring would leave map entries
    // past the end of the disk.
    if vdisk.size_bytes < snapshot.size_bytes {
        return Err(Error::refused(format!(
            "{vdisk_id} is {} bytes and its snapshot {snapshot_id} is {}; a vdisk never \
             shrinks, so these rows disagree",
            vdisk.size_bytes, snapshot.size_bytes
        )));
    }
    Ok(Plan { resumed, new_epoch: vdisk.epoch + 1 })
}

impl Daemon {
    /// `{"op": "rollback", "vdisk_id": ..., "snapshot_id": ..., "keep_as": optional}`.
    ///
    /// `keep_as` names an immutable copy of the vdisk's *current* map, taken before anything
    /// is destroyed. A rollback is the one operation here that throws away data the guest
    /// wrote, and an operator who picked the wrong snapshot should be one command from
    /// getting it back. It copies the map and nothing else, so it costs what any snapshot
    /// costs; what it preserves is the drained state, because a detached vdisk's journal is
    /// by definition not part of any map and is discarded below.
    pub(super) fn op_rollback(&self, req: &Value) -> Result<Value> {
        let id = str_field(req, "vdisk_id")?;
        let snap = str_field(req, "snapshot_id")?;
        let keep_as = req.get("keep_as").and_then(Value::as_str).filter(|s| !s.is_empty());
        let daruk = self.daruk();

        let read = |vdisk: &str| -> Result<Facts> {
            let rows = daruk.query(&format!(
                "SELECT class, owner, epoch, size_bytes, extent_bytes, parent_vdisk \
                 FROM hydra.dfs_vdisks WHERE vdisk_id = {}",
                cql_str(vdisk)
            ))?;
            rows.first()
                .map(Facts::from_row)
                .ok_or_else(|| Error::refused(format!("vdisk {vdisk} does not exist")))
        };
        let vdisk = read(&id)?;
        let snapshot = read(&snap)?;
        let attached_here = self.attached.lock().expect("attached mutex poisoned").contains_key(&id);
        let plan = plan(&id, &vdisk, &snap, &snapshot, &self.cfg.node, attached_here)?;
        if let Some(name) = keep_as {
            if name == id || name == snap {
                return Err(Error::refused(format!("keep_as '{name}' is already a vdisk in this rollback")));
            }
            if read(name).is_ok() {
                return Err(Error::refused(format!("keep_as '{name}' already exists")));
            }
        }

        // Every replica has to be reachable *and named*: see the module header, step 3.
        let replica_rows = daruk.query(&format!(
            "SELECT replicas FROM hydra.dfs_vdisks WHERE vdisk_id = {}",
            cql_str(&id)
        ))?;
        let replica_nodes: Vec<String> = replica_rows
            .first()
            .and_then(|r| r.get("replicas"))
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
            .unwrap_or_default();
        let mut clients: Vec<Arc<PeerClient>> = Vec::new();
        for node in replica_nodes.iter().filter(|n| **n != self.cfg.node) {
            match self.fence_peers.get(node) {
                Some(c) => clients.push(Arc::clone(c)),
                None => {
                    return Err(Error::refused(format!(
                        "vdisk {id} replicates to {node}, which this daemon has no address \
                         for. Its journal could not be discarded, and a journal left behind \
                         is replayed over the restored disk at the next attach."
                    )))
                }
            }
        }

        // Step 1. Skipped on a resume, where the class already says what is going on.
        if !plan.resumed {
            let flip = daruk.cas(
                "/v1/dfs/vdisk-class",
                json_params(vec![
                    ("vdisk_id", json!(id)),
                    ("class", json!(CLASS_ROLLING_BACK)),
                    ("expected_class", json!(CLASS_RW)),
                ]),
            )?;
            if !flip.applied {
                return Err(Error::refused(format!(
                    "vdisk {id} changed class while the rollback was starting: it is now {}",
                    flip.current_str("class")
                )));
            }
        }

        // Step 2. Conditional on both halves of the pair, like every claim.
        let claim = daruk.cas(
            "/v1/dfs/claim",
            json_params(vec![
                ("vdisk_id", json!(id)),
                ("owner", json!(self.cfg.node)),
                ("epoch", json!(plan.new_epoch)),
                ("expected_owner", json!(vdisk.owner)),
                ("expected_epoch", json!(vdisk.epoch)),
            ]),
        )?;
        if !claim.applied {
            if !plan.resumed {
                // Nothing has been touched, so put the class back. Left as it was it would
                // strand a healthy disk in a state nothing attaches.
                let _ = daruk.cas(
                    "/v1/dfs/vdisk-class",
                    json_params(vec![
                        ("vdisk_id", json!(id)),
                        ("class", json!(CLASS_RW)),
                        ("expected_class", json!(CLASS_ROLLING_BACK)),
                    ]),
                );
            }
            return Err(Error::refused(format!(
                "vdisk {id} is owned by {} at epoch {}: something attached it while the \
                 rollback was starting, and nothing was changed",
                claim.current_str("owner"),
                claim.current_i64("epoch").unwrap_or(-1)
            )));
        }
        let epoch = plan.new_epoch as u64;

        // The copy of what is about to be lost, before any of it is.
        let kept = match keep_as {
            Some(name) => Some(self.keep_current_map(&id, name, &vdisk)?),
            None => None,
        };

        // Step 3.
        self.discard_journals(&id, epoch, &clients)?;

        // Step 4. The snapshot's map is read after the journals are gone, so nothing between
        // reading it and writing it can change which disk is being restored.
        let rows = daruk.query(&format!(
            "SELECT extent_index, egroup_id, egroup_offset, length, vdisk_hash \
             FROM hydra.dfs_block_map WHERE vdisk_id = {}",
            cql_str(&snap)
        ))?;
        let mut restored: Vec<(u64, String, u32, u32, u64)> = Vec::with_capacity(rows.len());
        for row in &rows {
            restored.push(map_row(row, &snap)?);
        }
        // Rows first would leave two generations interleaved if the delete then failed; the
        // delete first leaves an empty map and a class that says the disk is mid-rollback.
        daruk.query(&format!(
            "DELETE FROM hydra.dfs_block_map WHERE vdisk_id = {}",
            cql_str(&id)
        ))?;
        for batch in block_map_batches(&id, epoch, &restored, MAP_BATCH) {
            daruk.query(&batch)?;
        }

        // Step 5.
        let done = daruk.cas(
            "/v1/dfs/vdisk-class",
            json_params(vec![
                ("vdisk_id", json!(id)),
                ("class", json!(CLASS_RW)),
                ("expected_class", json!(CLASS_ROLLING_BACK)),
            ]),
        )?;
        if !done.applied {
            return Err(Error::refused(format!(
                "vdisk {id} was restored but its class could not be set back to rw: it is {}",
                done.current_str("class")
            )));
        }

        Ok(json!({
            "vdisk_id": id,
            "snapshot_id": snap,
            "epoch": epoch,
            "previous_epoch": vdisk.epoch,
            "extents": restored.len(),
            "resumed": plan.resumed,
            "kept_as": kept,
            "bytes_copied": 0,
        }))
    }

    /// Fence every replica at `epoch` and empty its copy of the journal, then do the same
    /// to this node's own. Any failure is the whole operation's failure -- see the module
    /// header for why a partial discard is worse than none.
    fn discard_journals(&self, id: &str, epoch: u64, clients: &[Arc<PeerClient>]) -> Result<()> {
        for client in clients {
            for opcode in [peer::OP_FENCE, peer::OP_TRUNCATE] {
                let verb = if opcode == peer::OP_FENCE { "fence" } else { "truncate" };
                let resp = client
                    .call(&Request {
                        opcode,
                        vdisk: id.to_string(),
                        epoch,
                        seq: 0,
                        offset: 0,
                        flags: 0,
                        data: Vec::new(),
                    })
                    .map_err(|e| {
                        Error::refused(format!(
                            "replica {} did not {verb} vdisk {id}: {e}. The rollback is not \
                             complete and the vdisk is left in class '{CLASS_ROLLING_BACK}'; \
                             run it again once the replica is back.",
                            client.node
                        ))
                    })?;
                if !resp.is_ok() {
                    return Err(Error::refused(format!(
                        "replica {} refused to {verb} vdisk {id} (status {}). The rollback is \
                         not complete and the vdisk is left in class '{CLASS_ROLLING_BACK}'.",
                        client.node, resp.status
                    )));
                }
            }
        }
        // This node's copies: the owner's own journal, and any replica copy it holds from an
        // earlier ownership by another node.
        self.replica_store.fence(id, epoch)?;
        self.replica_store.truncate(id, epoch)?;
        match std::fs::remove_file(self.cfg.root.join("journal").join(format!("{id}.jrn"))) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io(format!("could not remove the local journal of {id}: {e}"))),
        }
    }

    /// An immutable copy of `id`'s current map under `name`: the same forming -> copy ->
    /// immutable sequence a snapshot uses, so an interrupted one leaves a row that says
    /// `forming` instead of a disk that reads as half zeroes.
    fn keep_current_map(&self, id: &str, name: &str, current: &Facts) -> Result<String> {
        let daruk = self.daruk();
        let rows = daruk.query(&format!(
            "SELECT container, egroup_bytes, replicas FROM hydra.dfs_vdisks WHERE vdisk_id = {}",
            cql_str(id)
        ))?;
        let row = rows
            .first()
            .ok_or_else(|| Error::refused(format!("vdisk {id} does not exist")))?;
        let replicas: Vec<String> = row
            .get("replicas")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
            .unwrap_or_else(|| vec![self.cfg.node.clone()]);
        let cas = daruk.cas(
            "/v1/dfs/vdisk-create",
            json_params(vec![
                ("vdisk_id", json!(name)),
                ("container", json!(row.get("container").and_then(Value::as_str).unwrap_or("default"))),
                ("size_bytes", json!(current.size_bytes)),
                ("class", json!(CLASS_FORMING)),
                ("owner", json!("")),
                ("epoch", json!(0)),
                ("drain_seq", json!(0)),
                ("extent_bytes", json!(current.extent_bytes)),
                ("egroup_bytes", json!(row.get("egroup_bytes").and_then(Value::as_i64).unwrap_or(4 << 20))),
                ("created_at_ms", json!(now_ms())),
                ("replicas", json!(replicas)),
                ("rf", json!(replicas.len().max(1) as i64)),
                ("parent_vdisk", json!(id)),
            ]),
        )?;
        if !cas.applied {
            return Err(Error::refused(format!("keep_as '{name}' already exists")));
        }
        let rows = daruk.query(&format!(
            "SELECT extent_index, egroup_id, egroup_offset, length, vdisk_hash \
             FROM hydra.dfs_block_map WHERE vdisk_id = {}",
            cql_str(id)
        ))?;
        let mut copied = Vec::with_capacity(rows.len());
        for r in &rows {
            copied.push(map_row(r, id)?);
        }
        for batch in block_map_batches(name, 0, &copied, MAP_BATCH) {
            daruk.query(&batch)?;
        }
        let done = daruk.cas(
            "/v1/dfs/vdisk-class",
            json_params(vec![
                ("vdisk_id", json!(name)),
                ("class", json!(CLASS_IMMUTABLE)),
                ("expected_class", json!(CLASS_FORMING)),
            ]),
        )?;
        if !done.applied {
            return Err(Error::refused(format!(
                "{name} was built but its class could not be set: it is {}",
                done.current_str("class")
            )));
        }
        Ok(name.to_string())
    }
}

/// One block-map row as `block_map_batches` takes it. The vdisk hash is carried through
/// from the row and defaults to the *source* vdisk's, never the destination's: these
/// extents were written under the source's identity and a footer check against any other
/// would fail every read.
fn map_row(row: &Value, source: &str) -> Result<(u64, String, u32, u32, u64)> {
    let idx = crate::vdisk::field_u64(row, "extent_index")?;
    let egroup = row
        .get("egroup_id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::meta("block map row without egroup_id".to_string()))?
        .to_string();
    let offset = crate::vdisk::field_u64(row, "egroup_offset")? as u32;
    let length = crate::vdisk::field_u64(row, "length")? as u32;
    let vh = row
        .get("vdisk_hash")
        .and_then(Value::as_i64)
        .map(|v| v as u64)
        .unwrap_or_else(|| crate::extent::vdisk_hash(source));
    Ok((idx, egroup, offset, length, vh))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(class: &str, owner: &str, parent: &str) -> Facts {
        Facts {
            class: class.to_string(),
            owner: owner.to_string(),
            epoch: 7,
            size_bytes: 10 << 20,
            extent_bytes: 1 << 20,
            parent_vdisk: parent.to_string(),
        }
    }

    fn snap_of(parent: &str) -> Facts {
        facts(CLASS_IMMUTABLE, "", parent)
    }

    #[test]
    fn a_detached_vdisk_rolls_back_to_its_own_snapshot_one_epoch_higher() {
        let got = plan("vm-disk0", &facts("rw", "node-a", ""), "vm-disk0-s1", &snap_of("vm-disk0"), "node-a", false)
            .unwrap();
        assert_eq!(got, Plan { resumed: false, new_epoch: 8 });
    }

    /// The property the whole design leans on: a guest is never left reading under a swap.
    #[test]
    fn an_attached_vdisk_is_refused_loudly_and_says_why() {
        let err = plan("vm-disk0", &facts("rw", "node-a", ""), "s", &snap_of("vm-disk0"), "node-a", true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("attached"), "{err}");
        assert!(err.contains("guest"), "{err}");
    }

    /// A snapshot of someone else's disk is a perfectly valid immutable vdisk, so only the
    /// lineage check stands between a typo and a reimage.
    #[test]
    fn a_snapshot_of_a_different_vdisk_is_not_a_rollback_target() {
        let err = plan("vm-disk0", &facts("rw", "", ""), "other-s1", &snap_of("other-disk0"), "node-a", false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("other-disk0"), "{err}");
    }

    #[test]
    fn an_image_with_no_parent_is_not_a_snapshot_of_anything() {
        assert!(plan("vm-disk0", &facts("rw", "", ""), "img", &snap_of(""), "node-a", false).is_err());
    }

    #[test]
    fn a_writable_copy_is_not_a_fixed_point() {
        assert!(plan("vm-disk0", &facts("rw", "", ""), "clone", &facts("rw", "", "vm-disk0"), "node-a", false)
            .is_err());
    }

    #[test]
    fn only_the_node_that_holds_the_journal_may_discard_it() {
        let err = plan("vm-disk0", &facts("rw", "node-b", ""), "s", &snap_of("vm-disk0"), "node-a", false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("node-b"), "{err}");
    }

    #[test]
    fn a_vdisk_nobody_ever_attached_has_no_owner_and_rolls_back_from_anywhere() {
        assert!(plan("vm-disk0", &facts("rw", "", ""), "s", &snap_of("vm-disk0"), "node-a", false).is_ok());
    }

    /// The way out of a crash in the middle: the class says it was interrupted and running
    /// the same rollback again is accepted, rather than the disk being stranded.
    #[test]
    fn an_interrupted_rollback_can_be_run_again() {
        let got = plan("vm-disk0", &facts(CLASS_ROLLING_BACK, "node-a", ""), "s", &snap_of("vm-disk0"), "node-a", false)
            .unwrap();
        assert!(got.resumed);
    }

    #[test]
    fn an_immutable_or_forming_vdisk_has_nothing_to_roll_back() {
        for class in [CLASS_IMMUTABLE, CLASS_FORMING, "nonsense"] {
            assert!(plan("vm-disk0", &facts(class, "", ""), "s", &snap_of("vm-disk0"), "node-a", false).is_err(), "{class}");
        }
    }

    #[test]
    fn extent_sizes_must_agree_or_the_map_indices_mean_different_offsets() {
        let mut s = snap_of("vm-disk0");
        s.extent_bytes = 4 << 20;
        assert!(plan("vm-disk0", &facts("rw", "", ""), "s", &s, "node-a", false).is_err());
    }

    #[test]
    fn a_vdisk_smaller_than_its_snapshot_means_the_rows_disagree() {
        let mut s = snap_of("vm-disk0");
        s.size_bytes = 20 << 20;
        assert!(plan("vm-disk0", &facts("rw", "", ""), "s", &s, "node-a", false).is_err());
    }

    #[test]
    fn a_vdisk_grown_since_the_snapshot_keeps_its_size() {
        let mut v = facts("rw", "", "");
        v.size_bytes = 20 << 20;
        assert!(plan("vm-disk0", &v, "s", &snap_of("vm-disk0"), "node-a", false).is_ok());
    }

    #[test]
    fn a_half_restored_vdisk_holds_a_class_of_its_own() {
        // `forming` tells an operator to delete the vdisk, which for a disk whose data is
        // only partly restored destroys the snapshot's other half; `rw` and `immutable`
        // are attachable. Attach refuses this class by name, so it must not collide.
        for other in [CLASS_RW, CLASS_IMMUTABLE, CLASS_FORMING] {
            assert_ne!(CLASS_ROLLING_BACK, other);
        }
    }
}
