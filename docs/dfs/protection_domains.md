# Protection domains

A protection domain is a named group of VMs and vdisks that is snapshotted **together**, under
one policy. It is the part of Nutanix's Cerebro that Helios had only as a per-vdisk rule
([snapshots.md](./snapshots.md)), and the unit the replication design
([replication.md](./replication.md)) attaches to.

| | State |
| :-- | :-- |
| Domains, membership, scheduled sets, retention of sets, restore of a set | **Built.** `rauru_protection.py`; tables `0030`-`0032`; `valcli storage.domain*`. |
| A crash-consistent cut across a VM's disks, by suspending the guest | **Built and unit-tested against a fake. Never run against a real guest.** |
| A timer that calls it | **Wired, never run against a real cluster.** The Rauru daemon runs one domain pass after each snapshot-policy pass (`rauru.run_everything`); an enabled domain is the opt-in, and with none enabled the pass is one table read. A failure in the domain pass is logged and does not lose the policy's result. `valcli storage.domain.run` is the same pass by hand. |
| Application-consistent sets | **Not possible without an agent in the guest. Out of scope.** |

## 1. Why a per-disk snapshot is not enough

A VM with three disks, snapshotted per vdisk by the policy job, gets three snapshots minutes
apart. Rolling the VM back to them puts a database on disk 1 beside a log on disk 2 that is
older or newer than it. Each snapshot is individually valid and the set of them is a state the
machine was never in.

Sidon cannot fix this from below. A snapshot of one vdisk drains its owner's journal and copies
its map; there is no operation that snapshots several vdisks at one instant, and adding one
would need a barrier across vdisks that may be owned by different nodes. What *can* hold the
disks of a VM still is the thing that writes to them.

## 2. The consistency story

**The target is crash consistency across the disks**: the set is a state the machine could have
been left in by a power cut at one instant.

The mechanism, per VM: suspend its vCPUs (`virsh suspend`), snapshot every disk, resume. The
control plane does this through the typed `POST /api/v1/vm/<name>/power` on the host the VM runs
on, with the two actions `suspend` and `resume` added to spark-daemon's allow-list.

Why that is crash-consistent:

* Every write the guest *completed* before the suspend is in every disk's snapshot, because
  Sidon drains the owner's journal first and a write is acknowledged only once it is in the
  journal.
* A write in flight at the suspend may be in some snapshots and not in others. That is exactly
  what a power cut produces. The guest cannot have ordered two writes that were both
  outstanding; it orders with a flush, and a flush it saw complete is before the suspend. So the
  set contains a prefix of the guest's ordering plus an arbitrary subset of what was concurrent,
  and the guest's filesystem journal does the recovery it always does.
* While the vCPUs are stopped the guest issues nothing new, so the first and the last snapshot of
  the set describe the same moment up to that in-flight ambiguity. The set records the measured
  spread (`cut_end_ms - cut_start_ms`) and how long the guest was held (`paused_ms`) so this is
  a number and not a claim.

### What it does not guarantee

* **It is not application-consistent.** Nothing flushes a guest page cache, freezes a filesystem
  or quiesces a database. That needs an agent inside the guest (qemu-guest-agent `fsfreeze` is
  the obvious one) and none is deployed. A restored guest recovers as from a power cut: a
  journalling filesystem is fine, a database recovers from its own write-ahead log, an
  application that keeps state only in memory loses it. This is the same guarantee a snapshot
  of one disk always had; the domain extends it across disks and does not strengthen it.
* **A pause is visible to the guest.** Its clock jumps forward by the pause when it resumes, and
  network peers may time out a connection that was idle for longer than their patience. The
  pause is therefore bounded (`max_pause_seconds`, default 30, at most 300), and a set that
  cannot finish inside it is abandoned, the guest resumed at once, and the set recorded as a
  failure. It is never a quietly inconsistent success.
* **The pause is as long as the snapshots are.** A snapshot copies a block map: about one row per
  MiB of written extent, in batches of 100. A large disk is tens of thousands of statements.
  Nothing has measured this on a real cluster. A domain whose disks are large enough that the
  default budget cannot be met will fail every run with a message naming the limit, which is the
  intended way to find out; the remedies are a larger limit or `--quiesce none`.
* **A single-vdisk snapshot is not atomic across a drain.** `derive_child` in
  `sidon/src/control.rs` drains the journal and then reads the map after releasing the vdisk
  lock; a write-triggered drain can land between ([TODO.md](../../TODO.md), found by the
  snapshots work and not fixed). Under a barrier this reduces to the in-flight ambiguity above,
  because a drain can only contain writes that arrived after the first drain, and those are by
  construction the in-flight ones. **Without a barrier (`quiesce none`) it is a genuine hazard**
  and is part of why that mode's sets say `none`.

### How far the barrier reaches: `quiesce`

| `quiesce` | What is paused | What the set can claim |
| :-- | :-- | :-- |
| `none` | nothing | `none`: no instant, with the measured spread recorded |
| `vm` (default) | each VM with more than one disk, for its own disks only | `crash:vm`: a VM's disks share a cut; VMs are skewed by the time between groups |
| `domain` | every VM in the domain at once | `crash:domain`: one cut for everything |

A single-disk VM is never paused: one snapshot is already one instant. The `consistency` a set
records is *computed from what was actually held still*, not copied from the policy, and it can
be weaker: a bare `vdisk:` member has no known VM, so under `domain` it is snapshotted inside
the pause window but is not itself held still, and the set says `none`. Nothing in the data
model lets a bare vdisk borrow a VM's cut, and the convention that links `<vm>-disk<n>` to a VM
is a naming convention, not a fact.

## 3. A set is whole or it is not a set

If any member cannot be captured the whole set is **failed** and the member snapshots already
taken are deleted. An operator who wants VMs protected independently puts them in separate
domains. The alternative, a "partial" set that restores some disks, is a state that looks like a
backup and cannot be restored consistently, and `restore` would have to second-guess it.

What is a skip and what is a failure:

| Situation | Outcome |
| :-- | :-- |
| A VM member that is not running | **Skipped**, with the reason. A detached writable vdisk cannot be snapshotted (the owner has to drain), and nothing is writing. |
| A running VM with a disk no node serves | **Failure.** Capturing its other disks would produce a set silently missing one. |
| A VM mid-operation (migrating) | **Failure.** It is never paused. |
| A member that no longer exists | Skipped, loudly. |
| Pause refused, or a snapshot refused | **Failure**; the guests are resumed and what was taken is deleted. |
| The pause budget exhausted | **Failure**; resumed at once. |
| A guest that cannot be resumed | **Failure named STRANDED**, row left `taking`, recovery finds it. |

## 4. Schema

Migrations `0030`-`0032`, all `CREATE TABLE IF NOT EXISTS` (this ScyllaDB rejects
`ADD IF NOT EXISTS`, and none of these needs an ALTER).

```
dfs_protection_domains         name PK; enabled, interval_seconds, keep_last,
                               quiesce, max_pause_seconds, created_at_ms, updated_at_ms
dfs_protection_domain_members  ((domain), kind, name); added_at_ms          kind = vm | vdisk
dfs_protection_sets            ((domain), taken_at_ms DESC, set_id)
                               origin (policy|manual), state (taking|complete|failed),
                               consistency, quiesce, started/finished_at_ms,
                               cut_start_ms, cut_end_ms, paused_ms,
                               members (JSON), paused_vms (JSON), error
```

A VM member is resolved to its vdisks **when a set is taken**, by the same rule the rest of the
tree uses (`<vm>-disk<n>`, count from `disks_list`, absent or `NONE` is one), so a disk added
later is in the next set without anyone editing membership.

`members` is a JSON column and not a table because it is written once and read whole. While a
set is `taking`, `paused_vms` names the guests currently suspended and `cut_start_ms` is the
heartbeat; that is what recovery reads. Nothing here references data: member snapshots are
ordinary immutable vdisks that Purah marks through their block-map rows (I-3, I-7).

Member snapshots are indexed in `dfs_snapshot_index` with origin `domain`. The per-vdisk
retention deletes only `policy` rows, so a vdisk policy can never prune a member out from under a
set. (The console's snapshot page maps an unknown origin to "unindexed"; teaching it `domain` is
in TODO.md.)

## 5. A run

```
recover        resume any guest a dead run left suspended
for each enabled domain:
  take a set if the newest complete policy set is older than the interval
  retention: only if this run's set did not fail and every node answered
```

Taking a set: plan (pure), write the `taking` row, then per group **record the guests about to be
paused and a heartbeat, pause, snapshot every target, resume (in a `finally`)**, then write the
`complete` row. The row is written *before* each pause so a run that dies with a guest suspended
leaves a row saying which guest and where. The resume cannot be skipped by anything that goes
wrong in between; a resume that fails after three attempts raises a failure that outranks the
one in flight.

Two runs in the same minute name the same set and the second is a no-op, so a scheduled run and
an operator's cannot pause a guest twice.

**Recovery.** A `taking` set whose heartbeat is older than ten minutes (longer than the pause
ceiling plus a Sidon call, so a live run is never mistaken for a dead one) belongs to a run that
died: its guests are resumed, its snapshots deleted and the row marked failed. The Rauru daemon
calls `recover(force=True)` at start-up, when it knows no run of its own is in flight;
`valcli storage.domain.recover --force` is the operator's equivalent. Resuming a guest that a live
set is still holding would turn that set into one taken from a running guest, and nothing would
say so, which is why the unforced path waits.

## 6. Retention

The rules of [snapshots.md section 4](./snapshots.md), applied to a set as a unit. Only
**complete sets the policy took** are candidates; the newest `keep` are kept; an older set is
spared, **whole**, if any member snapshot has a vdisk derived from it, is attached on a node, or
is **pinned** (the hook replication uses so a set not yet delivered to a remote site is never
deleted to save space). A set is deleted whole or not at all: the check for children is made for
every member before the first delete, and again immediately before, because half a set cannot
be restored and looks complete. A failed run, or a node that does not answer, stops pruning for
that pass. Failed rows older than the newest five are dropped; the leftovers of a failed set
whose cleanup could not reach a node are retried on every run so a failure cannot pin extent
groups indefinitely.

## 7. Restore

`valcli storage.domain.restore <domain> <set>` puts every disk of a complete set back, in place,
and is **refused unless every member could be rolled back** ([snapshots.md section 5](./snapshots.md):
VM stopped, vdisk detached, every node answering). Every member is checked first and none is
touched unless all pass. Past that point the rollbacks are sequential and **not atomic**: if one
fails, `RestoreIncomplete` names what was restored and what was not, and running the same restore
again finishes it. Each rollback keeps a pre-rollback copy unless `--no-keep`.

## 8. Interplay with the per-vdisk policy

A vdisk in an enabled domain is skipped by `snapshot-run` ("left to its protection domain"), or
it would be snapshotted twice an interval, once consistently and once not. The check is
`rauru_protection.claimed_vdisks`, called lazily and swallowing import and query errors, so a node
or cluster without domains behaves as before.

## 9. Commands

```bash
valcli storage.domain.create web --every-hours 24 --keep 7 --quiesce vm --max-pause 30
valcli storage.domain.add web vm:web-frontend vm:web-db vdisk:scratch-disk0
valcli storage.domain.snapshot web          # a manual set, never pruned
valcli storage.domain                       # domains, members, newest set
valcli storage.domain.sets web              # every set: state, consistency, members
valcli storage.domain.run [--dry-run]       # the scheduled pass
valcli storage.domain.restore web web-202610031200 [--no-keep]
valcli storage.domain.recover [--force]
valcli storage.domain.delete web [--with-sets]
```

## 10. The interface for the Rauru daemon

Everything is in `rauru_protection.py`, which imports `helios_snapshots` and nothing that is a
daemon. The daemon builds an `Env` and calls:

| Call | Returns | Raises |
| :-- | :-- | :-- |
| `Env(query, dfs, vm_power, nodes=..., lwt=..., now_ms=..., say=..., parent_task_id=...)` | the effects, injected | |
| `Runner(env, helios_schema, dry_run=False)` | a runner | |
| `runner.recover(force=False)` | `Summary` (`resumed`, `failures`) | `RuntimeError` if Hydra cannot be read |
| `runner.run()` | `Summary` (`taken`, `pruned`, `skipped`, `spared`, `failures`, `resumed`); `ok` is false on any failure | `RuntimeError` if Hydra cannot be read |
| `runner.snapshot_domain(name)` | the set row | `DomainRefused` (no such domain, nothing to capture), `RuntimeError` (failed; message is the reason) |
| `runner.restore_set(domain, set_id, keep=True)` | vdisk ids restored | `DomainRefused` (nothing started), `RestoreIncomplete` (`done`, `remaining`) |
| `runner.delete_set(domain, set_id)` | None | `DomainRefused` (a member has children), `RuntimeError` |
| `runner.create_domain / add_member / remove_member / delete_domain` | | `DomainError` (bad input), `DomainRefused` |
| `runner.pinned_sets(domain)` | `{set_id: reason}` the retention must not delete. **Override this** with the sets not yet replicated | |
| `rauru_protection.run_command(argv, env, schema, out)` | exit status | |

`vm_power(host_ip, vm_name, action) -> (rc, body, err)`; the body must carry libvirt's `state`
after the call, which is what is trusted. The daemon is also what should call `recover(force=True)`
once at start-up and `run()` on its timer.

## 11. Tests

`test_rauru_protection.py`, against a fake that speaks the same CQL (it parses `VALUES`
strictly, so a builder whose columns and values disagree fails), the same Sidon operations and the
same power calls. The properties: the barrier brackets the snapshots in order; a failed snapshot,
a refused second pause, an exhausted budget and a failed resume each leave the guest resumed or
loudly stranded; a failed set deletes what it took; the consistency recorded is the consistency
achieved; retention deletes a set whole or not at all and respects children, attachment, origin
and pins; a dead run is recovered and a live one is not; a restore refuses before it starts.

**Not covered, and why it matters.** No test has paused a real guest. `virsh suspend` against a
domain whose disks are NBD exports of Sidon has not been run: whether in-flight requests
complete cleanly, how long a real snapshot takes under a barrier, and whether Mipha's health
checks object to a paused domain are all open. The pause window is the thing to measure first.
