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

(Further items are added below as they are finished.)

## Needs live verification

- A. Live migration, on the lab after a rollout:
  `ssh -i ~/.ssh/id_rsa_hci root@10.10.102.41 valcli vm.migrate <vm> 10.10.102.42` (or `host.maintenance.enter`).
  Pass: the task succeeds, `valcli vm.list` shows the VM on the target, and
  `valcli storage.list` shows the vdisk owned by the target node at an epoch one higher; on the source
  `journalctl -u sidon | grep 'released to another node'`; on the target `grep 'took over from'`.
  If `.42`/`.43` (TCG) refuse the migration for CPU/accelerator reasons the error comes from libvirt and
  names no NBD socket. A failed takeover is finished with `valcli storage.takeover <vdisk> <target-node>`.

## Decisions for the owner

(none yet)

## Found but not fixed

- Rust test `vdisk::tests::group_commit::with_two_replicas_a_repair_makes_both_identical_to_the_owner` failed
  once in a full parallel `cargo test` and passed on every rerun alone; timing-dependent.
- `test_sidon_owns_its_mounts` fails 8 ways in this container (environment), unrelated to this work.
