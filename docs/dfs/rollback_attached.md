# Rolling back an attached vdisk (design, not built)

[snapshots.md section 5](./snapshots.md) builds rollback for a **detached** vdisk and refuses
the attached case. This document is the reasoning for the attached case: what makes it
different, what the ownership and epoch story has to be, and the one design that survives it.
Nothing here is implemented. It is written down so the refusal is a decision with a stated
way forward and not an omission.

## 1. What is different when something is reading

A detached rollback swaps a map nobody is looking at. An attached one swaps a map underneath
a guest, and a guest is not a client of the disk that can be told to re-read it. Three things
inside the guest assume the disk changes only when the guest changes it:

* **The page cache and buffer cache** hold blocks the snapshot may have replaced. A cached
  block is served without a read, so the guest would act on a mixture of old cached blocks
  and new on-disk ones.
* **The filesystem's own metadata** (allocation bitmaps, the journal, inode tables) is a
  consistent set of blocks. A rollback restores a consistent set from one moment, but the
  guest's in-memory view of it is from another. The first write that trusts the stale view
  corrupts the restored one.
* **In-flight and queued I/O** was issued against the old contents. A write acknowledged
  before the swap and a write issued after it land on different generations of the disk with
  no ordering between them that the guest would recognise.

None of these is a Sidon problem and none has a Sidon fix. There is no operation Sidon can
perform on the extent map that makes a running guest's caches true again; qemu has no NBD
"your disk changed under you" message that a guest kernel acts on. So the question is not how
to swap safely under a guest. It is what must be true of the guest first.

## 2. The ownership and epoch story

The epoch is what makes any version of this safe against *stale actors*, and it is worth
being exact about what it does and does not do here.

**What the epoch does.** A rollback is an ownership change in the sense that matters: it
claims `(owner, e) -> (this node, e+1)` with the same compare-and-swap an attach uses, and
fences every replica at `e+1`. From that instant a replica refuses journal appends and extent
writes from any epoch below `e+1`. Anything that held the disk at `e` is a stale actor: a
qemu on a host that was partitioned away, a Sidon that lost its lease without noticing, a
zombie writer from a fenced host. Its next append meets a rejection and becomes `EIO` to its
guest instead of a write into the restored disk. This is invariant I-4 ([invariants.md](./invariants.md))
doing the same job it does for a failover, and it is why the detached rollback bumps the
epoch even though "nothing is attached": *detached* is a statement about the nodes that
answered, and the epoch is the statement that holds for the ones that did not.

**What the epoch does not do.** It does not tell a live, *current* owner's guest that its
disk changed. If the rollback is performed **by the owner itself**, at its own epoch `e`,
the sequence is: bump to `e+1` in Hydra, fence replicas at `e+1`, swap the map. The live
`Vdisk` object in memory still holds epoch `e`. Its next journal append is rejected by the
replicas it just fenced, and the guest sees `EIO` on a disk that is, from the guest's side,
perfectly healthy. The epoch converts a silent corruption into a loud I/O error, which is
the right failure, but it is a failure, and it is not a rollback.

Conversely, if the owner re-adopts `e+1` in memory and keeps serving, the guest keeps running
against a map it did not write with caches it did not invalidate: the case of section 1, with
the fencing nodding it through.

**Forwarding.** A vdisk attached in forwarding mode on another node (a guest that has
live-migrated and whose storage has not followed, [ownership.md section 5](./ownership.md))
is *attached*, even though the owner's `list` shows nothing for the guest. The detached
rollback finds it by asking every node; any attached-case design inherits that requirement.

## 3. The design: stop, roll back, start, as one task tree

The one approach that is sound is to make the guest not be reading, and then do the detached
rollback. Concretely, Vali owns it, because Vali owns VM lifecycle:

```
parent: snapshot_rollback (vm, vdisk, snapshot)
  1. stop the VM            graceful shutdown with a deadline, then destroy
  2. detach its vdisks      so the owner drains its journal
  3. verify detached        every node's `list`, the same check the detached path makes
  4. rollback               the built operation: class flip, claim e+1, fence + truncate
                            every replica, map swap, class back to rw
  5. start the VM           only if it was running when the request began
```

Every child is a row in `hydra.catalyst_tasks` with the parent and component every task
carries, so the console shows where it stopped. Ownership and epochs need nothing new: by
step 4 the situation is exactly the detached one, with the epoch bump doing what
section 2 says it does.

The rules that make it a design and not a script:

* **It never proceeds past a failed stop.** If the VM cannot be stopped inside the deadline,
  the rollback does not start. Destroying a guest to make a rollback possible is a decision
  for a person; the default is to fail and say so.
* **It never auto-starts after a failed rollback.** A vdisk left in `rolling-back` is
  unattachable by design, and Vali must not try to start a VM against it. Re-running the
  rollback resumes it ([snapshots.md](./snapshots.md)).
* **It keeps the safety copy by default**, for the same reason the detached path does: the
  operation discards what the guest wrote.
* **It is one request with one verdict**, not three commands an operator has to remember to
  order. The failure the detached-only refusal exists to prevent is an operator who rolls back
  while the VM is still up; making the safe sequence one command is what removes the reason
  to try.

## 4. Alternatives considered

**Freeze, swap, thaw (no shutdown).** Suspend the guest (`virsh suspend`) or freeze its
filesystems through the guest agent, swap the map, resume. This fixes in-flight I/O and
fixes nothing else: the page cache and in-memory filesystem state are still from before the
swap. A guest resumed over a swapped disk is the corruption of section 1 with a pause in it.
Rejected, and not a close call.

**Swap, then tell the guest to drop its caches.** `echo 3 > /proc/sys/vm/drop_caches` drops
clean caches only, says nothing about a mounted filesystem's in-memory metadata, and requires
guest cooperation that a cluster operator cannot assume across guest operating systems.
Rejected.

**Present the snapshot as a new disk and hot-swap the block device in qemu**
(`blockdev-reopen`). The guest's view of *a device* changes, which a guest handles as a
hot-unplug and replug if it was told, and as a corrupted disk if it was not. It also needs a
second vdisk identity, which is a clone, which is the thing rollback exists to avoid.
Rejected.

**Roll back the owner at its own epoch without bumping.** Would leave no way to tell
pre-rollback replica state from post-rollback state, which is the property the whole
operation rests on. Rejected without discussion.

## 5. Open questions for whoever builds it

* **The shutdown deadline.** Per VM, per cluster, or per request? A database guest and a
  throwaway guest want different answers, and the existing power-off path has its own
  timeout behaviour that this should reuse and not reinvent.
* **A VM with several disks.** Rolling one disk back and not its siblings produces a guest
  whose disks are from different moments. The detached path is per vdisk and says nothing;
  the VM-level wrapper almost certainly wants "all of this VM's disks to snapshots taken
  together", which the policy does not yet guarantee (it snapshots per vdisk, minutes apart).
  A consistency group is the missing concept, and it is not a small one.
* **A VM that is not Vali's.** A vdisk with no `<vm>-disk<n>` owner has nothing for Vali to
  stop. It stays detached-only.
* **Whether the console grows the button.** It should not until the confirmation says what is
  destroyed and names the safety copy that is being taken.
