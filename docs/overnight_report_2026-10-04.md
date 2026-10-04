# Overnight report, 2026-10-04

Branch `overnight/2026-10-04`. A running log: each item says what was found, what changed, what
was tested (and whether the test fails on the old code), which docs moved, and the status.

Baseline before any change (main at 0a6fb6f), on the container this ran in: Python 1755 tests with
8 failing and Rust 371 passing. The 8 Python failures are in `test_sidon_owns_its_mounts` (6 failures,
2 errors: the claim-step script reports "no additional empty disk to claim" in this container) and are
not caused by this work; `test_registry_override` needs `paramiko`, which was installed for the run.
Elixir/Erlang are not installed in the container, so no Elixir code was touched and `mix test` was not
run. One Rust test, `group_commit::with_two_replicas_a_repair_makes_both_identical_to_the_owner`,
failed once in the full parallel run and passed on every rerun alone (a timing-dependent test; see
"found but not fixed").

## Items

### A. Live migration of VMs with Sidon-served disks: done in code and unit tests, needs a live check

*Root cause.* `vali.py`'s migrate branch ran `virsh migrate` with nothing attached on the destination,
so the destination qemu opened an NBD socket no Sidon served. Start and HA attach before defining the
domain; migration never did.

*Change.*
- Merged `wip/live-migration-sidon` (not rewritten). Resolved a wire-opcode clash: main had taken 11 for
  `OP_EGROUP_DROP`, so `OP_RELEASE` is 12.
- `sidon/src/handover.rs`: `handover::handover` is a function of an injectable `Steps` trait (stall,
  read ownership, release, claim and open, install); failures are step-named (`Failure`, `Left`).
  `control.rs`'s `op_takeover` is a thin wrapper over it. The owner's release now removes the disk from the
  attached table before draining and puts it back if the drain fails or a client appears
  (`SIDON_RELEASE_WAIT`, 30 s).
- `vali.py`: attach each disk on the destination (data disks `forward: true`, images plain) before
  `virsh migrate`; the migrate command gets a 3600 s timeout (the daemon's default of 45 s would kill any
  long migration, a second latent bug); on failure the forwarders it created are detached (never images);
  after the commit, `takeover` per disk, retried 3 times, reported by step with the command to finish it.
- `spark_daemon_decoded.py`: `takeover` in `DFS_VDISK_OPS`, with a 300 s control-socket timeout; callers can
  no longer set `timeout`/`socket_path` through the payload.
- `valcli.py`: `storage.takeover <vdisk> <node>`.
- Docs: `docs/dfs/ownership.md` §5, `docs/vali.md` (migration section, stale `--unsafe` text), `docs/sidon.md`
  (handover, restart note), D-34 in `docs/dfs/decisions.md`, TODO.md.

*Tests.* Rust: 15 new handover tests (fault injection for every step; never two writers; no acknowledged
write lost; stale epoch rejected; idempotent retry; concurrent handovers; writes racing the handover).
Mutating `handover` to skip the release makes 9 of them fail. Python: `test_live_migration.py`, 20 tests
over the real migrate branch with a recording cluster; 15 of them fail against the old `vali.py`.

*Not done.* Sidon re-attach on start (not simple or safe without a Hydra-backed test; documented that a VM
must be moved or stopped before its node's Sidon is restarted). The owner's release path against a real
journal has no unit test.

Status: **done in code; live verification needed**.

### Cluster stop was slow and could look stuck: fixed in code and unit tests, needs a live check

*Root cause (by reading the code; not measured on a cluster).* `converge_to_desired_state` read the
units' states once per pass and acted on that snapshot. A stop goes in reverse dependency order, but
Sidon, Daruk and the database each waited on a dependent the snapshot still showed active, so they
were only stopped in the 2nd, 3rd and 4th pass, and passes were `ZK_DRIFT_CHECK_INTERVAL` (30 s) apart
because nothing woke the loop: roughly 90 s of idle waiting before any stop time. Every `systemctl
stop` was also run one after another, and `cluster stop` shut guests down one at a time with a
5 s window each (guests that needed longer were powered off). A start had the same one-layer-per-30 s
shape. A dependent in `deactivating` was also treated as not holding its requirement.

*Change.* Tier by tier within one pass (concurrent `systemctl` per tier, states updated after each
tier, ports re-read), a 3 s retry interval after a pass that left work (`ZK_CONVERGE_RETRY_INTERVAL`),
`deactivating`/`activating` count as still holding, the "waiting on" log line is printed once per
change. `cluster stop` stops all guests together against one 20 s window (`stop_vms_together`).
Files: `spark_daemon_decoded.py`, `cluster_new.py`, `docs/cluster_state.md`.

*Tests.* `test_cluster_declarative.py`: the old test `test_the_stop_order_is_the_start_order_inverted`
pinned the slow behaviour (one `systemctl stop sidon` per pass) and was rewritten; new tests for a
failing stop keeping what it uses up, a deactivating dependent holding its requirement, concurrency,
and the retry interval. `test_cluster_stop.py`: 7 tests. Both fail against the old code.

*Not changed deliberately.* `drain_local_storage` still detaches vdisks one after another (each is a
journal drain; parallelising IO-heavy drains is a decision for someone watching a real node).
Stop/start semantics (order, gates, error latching, state store stopped last) are unchanged.

Status: **done in code; live verification needed**.

(Further items are added below as they are finished.)

## Needs live verification

- A. Live migration, on the lab after a rollout:
  `ssh -i ~/.ssh/id_rsa_hci root@10.10.102.41 valcli vm.migrate <vm> 10.10.102.42` (or `host.maintenance.enter`).
  Pass: the task succeeds, `valcli vm.list` shows the VM on the target, and
  `valcli storage.list` shows the vdisk owned by the target node at an epoch one higher; on the source
  `journalctl -u sidon | grep 'released to another node'`; on the target `grep 'took over from'`.
  If `.42`/`.43` (TCG) refuse the migration for CPU/accelerator reasons the error comes from libvirt and
  names no NBD socket. A failed takeover is finished with `valcli storage.takeover <vdisk> <target-node>`.

- Cluster stop, on the lab: `time valcli cluster stop` (or `cluster stop`) on .41 with at least one running
  guest. Pass: it finishes well inside a minute for the services (it used to need several 30 s waits),
  `cluster status` shows every service DOWN, no `last_error` is printed, and the guests are `shut off`
  (a guest that ignores ACPI is powered off after 20 s). Then `cluster start` must still bring everything
  back in order (hydra-db, daruk, then the rest): the start path shares the loop that was changed.

## Decisions for the owner

(none yet)

## Found but not fixed

- Rust test `vdisk::tests::group_commit::with_two_replicas_a_repair_makes_both_identical_to_the_owner` failed
  once in a full parallel `cargo test` and passed on every rerun alone; timing-dependent.
- `test_sidon_owns_its_mounts` fails 8 ways in this container (environment), unrelated to this work.
