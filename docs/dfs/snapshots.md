# Scheduled snapshots, retention, and rollback

Taking a snapshot has been a command since Sidon had them ([sidon.md section 6](../sidon.md)):
a map copy, zero bytes moved. What a command is not is a backup tier. This document is the
part around it: what takes snapshots on a timer, what stops them accumulating, how a person
sees them, and how a stopped VM's disk is put back to one.

Three things are built and one is designed:

| | State |
| :-- | :-- |
| Scheduled snapshots with a retention policy | **Built.** A Dagur job; policy in Hydra. |
| `valcli storage.*` views, and a read-only console page | **Built.** |
| In-place rollback of a **detached** vdisk | **Built.** Sidon op `rollback`. |
| In-place rollback of an **attached** vdisk | **Designed, not built:** [rollback_attached.md](./rollback_attached.md). |

## 1. Scheduling uses what already exists

There is no new daemon. The cluster already has a clustered cron: Dagur runs jobs from
`hydra.dagur_schedules`, Catalyst claims each tick exactly once cluster-wide
(`/v1/schedule/claim-job`), and the job runs on the node holding the `dagur-queue`
candidacy ([../service_leadership.md](../service_leadership.md)). The policy is one more
row in that table:

```
snapshot_policy   0 * * * *   3600s   /usr/local/bin/valcli storage.snapshot-run
```

It is seeded by the console's bootstrap with `IF NOT EXISTS`, which runs on every start of
every node, so an existing cluster gains the job on its next rollout without a repair writer.
It is enabled from the start and does nothing until a policy exists: a run with no policy
reads one table, prints that there is nothing to do, and exits 0.

**Leader-only work, and why nothing here elects a leader.** The job inherits its single-run
guarantee from Dagur: the tick is claimed once, and the node running it is the
`dagur-queue` candidacy holder, a per-service election in `helios_zk.py`. The code in
`helios_snapshots.py` never asks who leads ZooKeeper and never compares an address to
anything; `test_snapshot_policy.TheWiring` fails if it starts to. An operator running
`valcli storage.snapshot-run` by hand concurrently with the job is a supported accident
rather than a prevented one: snapshot names have minute resolution, so both compute the
same name and the second is refused by Sidon as already existing, and a double prune
deletes an already-absent row, which is a no-op.

**Granularity.** The job fires hourly, so that is the shortest interval a policy can honestly
promise. `validate_policy` refuses less. A snapshot is due when the newest *policy*
snapshot is within five minutes of a full interval old, because the scheduler fires at its
interval give or take a poll, and without the slack a one-hour policy would snapshot every
other run.

## 2. A run is a task

Dagur's own task is the parent. Under it, every snapshot taken, every snapshot pruned and
every rollback is a child row in `hydra.catalyst_tasks`, written with
`helios_schema.task_insert_statement`, so each carries the parent, the component
(`Catalyst`, matching the scheduled job that parents it) and a per-component sequence. A
failure is a `failed` row in the console's task ring with the daemon's own message, and the
job exits non-zero, which is what makes Dagur record the run itself as failed.

What is *not* a failure, and is never silent either:

* A vdisk that is not attached is **skipped**, with the reason printed and counted. A stopped
  VM writes nothing, so its newest snapshot is still a true picture of it. Sidon cannot
  snapshot a detached writable vdisk anyway (the journal has to be drained by the owner).
* A snapshot another run took in the same minute is reported as already existing.

Failing to *record* a task never fails the work: the snapshot either happened or it did not.

A snapshot is crash-consistent, as it always was: Sidon drains the owner's journal, then
copies the map. It is not application-consistent; nothing here quiesces a guest.

## 3. The policy

Two tables, one ALTER-free migration each (`0022`, `0023`; both `CREATE TABLE IF NOT EXISTS`,
which this ScyllaDB accepts, unlike `ADD IF NOT EXISTS`).

`hydra.dfs_snapshot_policies`, keyed `((scope), target)`:

| scope | target | meaning |
| :-- | :-- | :-- |
| `cluster` | `*` | the default |
| `container` | a container name | overrides the default for that container |
| `vdisk` | a vdisk id | overrides both for one disk |

The narrowest row wins, **including a disabled one**: a `vdisk` row with `enabled = false` is
how one disk is exempted from a cluster-wide policy. Falling through a disabled row to the
broader one would make exemption inexpressible. No rows at all means nothing is snapshotted;
it is opt-in, because a snapshot pins extent groups and a cluster that begins keeping history
nobody asked for fills its stores with it.

`hydra.dfs_snapshot_index`, keyed `((vdisk_id), created_at_ms DESC, snapshot_id)`, records
which snapshots exist, when, and who took them: `policy`, `manual` or `pre-rollback`. Lineage
(`dfs_vdisks.parent_vdisk`) already says what came from what; it cannot say whether a person
or a policy made it, and retention may only touch the latter. A snapshot with no index row
is *unindexed* and is never pruned, so the failure mode of a missing row is a snapshot kept,
never one lost.

```bash
valcli storage.snapshot-policy.set cluster --every-hours 24 --keep 7
valcli storage.snapshot-policy.set container:fast --every-hours 4 --keep 12
valcli storage.snapshot-policy.set vdisk:scratch-disk0 --disable     # exempt
valcli storage.snapshot-policy                                       # what is set, what it covers
valcli storage.snapshots <vdisk>                                     # who took each, what depends on it
valcli storage.snapshot-run --dry-run                                # what a run would do
```

## 4. Retention, and how it never prunes what something depends on

Retention keeps the newest `keep` *policy* snapshots per vdisk and deletes older ones, unless:

1. **A vdisk was derived from it.** Every vdisk named as another's `parent_vdisk` is a
   snapshot someone cloned (or snapshotted). Deleting it would not lose data, because Purah
   marks from every block-map row and the child's own rows keep its extent groups alive
   ([D-19](./decisions.md)). It would lose *lineage*: `storage.children` would stop saying
   where the child came from, and a child with no recorded parent is indistinguishable from
   one that never had any.
2. **A node is serving it.** Each node is asked what it has attached; Sidon refuses to delete
   one it serves itself, but only on that node.
3. **It is not the policy's.** `manual` and `pre-rollback` snapshots are never candidates,
   however old.

How (1) is checked: the plan is made from one listing of `dfs_vdisks`, and then **the listing
is read again, immediately before each delete**, because a clone can be made in between. A
snapshot spared this way is reported with the names of its children. What remains is the
window between that second read and Sidon's delete: Sidon's `delete` does not itself check
for children, so a clone created inside that window leaves its parent row dangling. The data
is safe (it is the lineage record that is lost) and the window is milliseconds, but it is a
window, and closing it needs a conditional delete in Sidon; it is recorded in TODO.md.

A spared snapshot does **not** count against `keep` (the window is "the newest N the policy
took", so a pinned old snapshot costs one extra rather than displacing a recent one) and
sparing it never causes a younger one to be deleted instead.

Two further rules keep a bad day from becoming a worse one:

* **Retention does not run on a vdisk whose snapshot failed this run.** A cluster that cannot
  take snapshots must not also be shedding the ones it has.
* **Retention does not run while any node is unreachable**, because it might be the one
  serving a snapshot.

Deleting a snapshot frees only what nothing else references; Purah reclaims it after its
usual two-scan grace ([invariants.md](./invariants.md), I-7). Pruning is therefore not
instantaneous space recovery.

## 5. Rollback of a detached vdisk

```bash
valcli storage.rollback <vdisk> <snapshot> [--no-keep]
```

Puts the vdisk's *contents* back to a snapshot of itself, in place, so everything that refers
to the vdisk (the libvirt domain, the `<vm>-disk<n>` convention, the row) stays valid. That is
what a clone cannot do.

### Refused unless nothing can be reading it

A guest reading a disk assumes the bytes change only when it changes them. A rollback is not a
write the guest issued, so under a running guest the filesystem meets blocks that contradict
its own metadata. Rollback is therefore refused, loudly, unless the disk is detached, and the
refusal is made in two independent places because neither can see everything:

* **The control plane** (`valcli`) refuses if the VM that owns the disk (by the
  `<vm>-disk<n>` convention) is not `Stopped` or is mid-operation, if **any** node reports the
  vdisk attached in any role (it asks every node; Sidon only knows its own), or if a node
  cannot be asked, because "I could not check" is not "it is detached". It also refuses a
  snapshot that is not of this vdisk.
* **Sidon** (`rollback`) refuses if it is serving the vdisk itself, if the vdisk was last
  owned by a different node (the node that holds its journal must do this), if the class is
  not `rw`, if the target is not an immutable snapshot whose `parent_vdisk` is this vdisk
  (a snapshot of someone else's disk is a perfectly valid immutable vdisk and would restore
  without complaint; the lineage check is what stands between a typo and a reimage), or if
  extent sizes disagree.

A vdisk whose name does not follow `<vm>-disk<n>` has no VM to consult; the attach check and
Sidon's own refusals still apply. That is the honest limit of the control-plane check.

### What it does, in this order

```
1. class  rw -> rolling-back            nothing attaches a disk in this class
2. claim  (owner, e) -> (this node, e+1)   the epoch bump
3. keep   an immutable copy of the current map, if asked      (default: yes)
4. fence + truncate every replica at e+1; ALL of them, or stop
5. delete the vdisk's map; write the snapshot's map at epoch e+1
6. class  rolling-back -> rw
```

**The epoch is bumped** by the same compare-and-swap an attach uses (`IF owner = ? AND epoch
= ?`), so a rollback that raced an attach is decided by Daruk: one claim wins and the other is
refused, never two owners. The bump is what lets replicas tell the difference between before
and after: every replica is fenced at `e+1` and refuses journal appends from any lower epoch,
so a deposed owner that somehow still believed it held the disk is rejected like any other
stale actor ([ownership.md](./ownership.md)).

**Why every replica and not a quorum, and why the journals are destroyed.** A detached vdisk
normally has an empty journal, because detach drains. If the final drain failed it does not,
and a replica that was unreachable keeps whatever tail it had. At the next attach the owner
adopts the longest tail it can read and replays it over the map. After a rollback that replay
is the *old data coming back*, silently, on a disk the operator was told had been rolled
back. So the rollback fences and truncates every replica's journal, and if one cannot be
reached it fails and leaves the vdisk in `rolling-back`. This is the journal's write-all rule
applied to the journal's destruction.

**Why the class flip comes before the claim.** An attach that raced this call already holds an
`(owner, epoch)` pair and will claim with it; whichever claim reaches Daruk first wins. The
class flip is what stops an attach that *starts after* the claim from claiming `e+2` and
serving a map that is half written. Attach refuses `rolling-back` by name.

**Why `rolling-back` and not `forming`.** `forming` tells an operator to delete the vdisk,
which is exactly the wrong advice for a disk whose data is half restored and whose snapshot
is the other half.

**A crash is recoverable.** After step 1 every step is a pure function of the snapshot, so
repeating the same rollback completes it: the plan treats `rolling-back` as a resume. Between
the map delete and the map write the disk is empty and unattachable, and says why.

**The safety copy.** A rollback is the one operation here that discards what a guest wrote.
Unless `--no-keep`, step 3 takes an immutable copy of the *drained* current state, named
`<vdisk>-pre-rollback-<UTC minute>`, indexed as `pre-rollback` so retention never touches it.
Rolling back to it undoes the rollback. It captures the drained state only: a detached
vdisk's journal is by definition not part of any map, and is discarded in step 4.

What the old extent groups do afterwards: nothing. They are unreferenced by this vdisk's map,
and Purah reclaims whatever no snapshot or clone still references. Snapshots newer than the
one rolled back to remain valid immutable copies of the old timeline and age out under
retention like any other.

### Not guaranteed

* There is no check that the VM's guest is *shut down cleanly*, only that it is not running.
  A VM that was powered off uncleanly is rolled back like any other; the snapshot is
  crash-consistent and the guest will recover as it would from a power cut.
* The control-plane VM check trusts `hydra.vms.state`. A stale `Stopped` on a VM that is in
  fact running is caught only by the attach check (the qemu process holds an NBD attachment),
  which is why both exist.

## 6. The console

`/storage/vdisks/:vdisk_id/snapshots` (linked from each writable vdisk on `/storage`) shows a
vdisk's snapshots read-only: who took each, when, its size, why retention will leave it
alone (a vdisk was derived from it), and the policy that governs the disk, including
exemption. It offers no button. A rollback destroys what a guest wrote and deserves its own
confirmation flow, and the console is being brought to parity separately; until then the
mutating half stays on `valcli`.

## 7. Tests

`test_snapshot_policy.py` asserts the properties above without a cluster (the decisions are
pure functions and the runner is exercised against a fake that speaks the same statements and
Sidon operations): retention never deletes a clone's parent, a manual snapshot, an attached
snapshot, or anything on a run that failed to take one; the pre-delete re-read catches a
clone made after the plan; a failure is a failed task and a non-zero exit while a skip is
neither; a rollback is refused for a running VM, for a disk any node serves, and for a node
that cannot be asked, *before* Sidon is called. Sidon's own refusals are the pure `plan`
function in `sidon/src/control/rollback.rs`, tested in place. The console is covered by
`snapshots_test.exs` and `snapshots_live_test.exs`.

The end-to-end path (Sidon claiming, fencing replicas and swapping a real map under a real
Hydra) is **not** covered by an automated test; see TODO.md.
