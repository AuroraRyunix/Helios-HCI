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

### C. test2 hangs at the boot options screen: generator fixed and tested, needs a live boot

*Root cause (by reading the code and firmware behaviour; not reproduced on hardware).* Vali's
`generate_vm_xml` expressed the boot order as `<os><boot dev='cdrom'/><boot dev='hd'/></os>`. That
is a legacy BIOS setting: OVMF, the UEFI firmware that the console's form defaults to, ignores it and
boots in its own default order, so a VM created with an ISO and an empty disk did not reliably boot
the CD-ROM and could stop at the firmware boot menu. The console tier's own builder was worse: it
emitted only the one `boot_device` entry (no fallback) and interpolated the value into the XML
unescaped. The Phoenix form's default (`boot_device` empty) and its `iso`/`disks_list` strings were
checked by reading `form.ex`/`vm.ex`: the image reaches `iso` as a comma-separated list and disks as
`size:container:bus`; nothing was found wrong there. Separately, the Sidon disk path ignores the bus
chosen for a disk (always virtio): noted under "found but not fixed".

*Change.* `helios_sidon.boot_orders` plans per-device orders; `disk_xml`/`cdrom_xml` take `boot_order`;
Vali and the console tier's builder emit `<boot order='N'/>` and no `<os><boot>`; NIC boot is
supported for `network`. Semantics are documented in the new `docs/vm_lifecycle.md` (Boot order).

*Tests.* `test_vm_boot_order.py`, 14 tests (ISO plus empty disk, `cdrom`, `hd`, `network`, UEFI and BIOS,
empty drives, several disks, none, markup injection); 8 fail against the old Vali.

Status: **done in code; live verification needed**.

### G. Hydra replication factor consistency: done (code and tests); nothing live to check beyond a read

*Audit.* Places that pick or compare a keyspace RF: `helios_cql.metadata_replication_factor` (2F+1,
the rule), Mimir's audit and Phoenix's Settings (already the rule), but `spectrum_server.init_db` created
the keyspace at a flat `min(3, nodes)` and its start-up reconcile defaulted a missing setting to a flat 3,
so a five-node `ftt=2` cluster (needs 5) was lowered to 3 at every start; `cluster add-node` printed
`min(3, nodes)` as the factor to ALTER to. Guest-data copy counts (`ftt + 1`) in valcli/spectrum are a
different quantity and correct.

*Change.* `helios_cql.default_metadata_replication_factor` (the rule, floored at `min(3, nodes)` so a default
never lowers a live database) and `two_replica_warning`; used by `init_db` (create and reconcile),
`cluster create`, `cluster add-node` advice and the `remove-node` plan. A warning is printed whenever the
database would be at two replicas. The two-node tie-breaker decision is not made. Docs:
`docs/ring_lifecycle.md`, `docs/cluster.md`.

*Tests.* `test_metadata_replication_rule.py` (+7). The init/reconcile and CLI call sites are asserted as
source text because `spectrum_server` opens a database on import.

Status: **done**.

### D. Host maintenance flow: rule defined, bugs fixed, documented and tested; needs a live check

*What was wrong (by reading the code).* Three definitions of what maintenance keeps running: Vali stopped
every unit but ZooKeeper (hydra-db, daruk and sidon included, not hylia); spark's boot-time maintenance branch
stopped a different list, started zookeeper and hydra-db, and its watchdog restarted zookeeper, hydra-db and
sidon (which that branch had just stopped); the zk reconcile loop ignores the desired state. Hence the mixture
the owner saw. Also: (1) **nothing took a host from RECOVERING to NORMAL after `leave`** (the lock was released at
RECOVERING and the row stayed there; Mipha's rejoin sets NORMAL only for hosts that were DOWN; Hylia waits for NORMAL
for 120 s and then fails the upgrade); (2) `leave` ran on any host, including a NORMAL one (restarting its services and
setting it RECOVERING); (3) the leave start list omitted Hylia; (4) a marker file that could not be written still
left the host recorded as IN_MAINTENANCE; (5) a stop that spark refused was logged and the task still reported success.

*Change.* The rule (see `docs/maintenance.md`): kept up = spark-daemon, zookeeper, hydra-db, daruk, sidon, hylia;
stopped = the other 14 managed units. One column (`maintenance: keep`) in `MANAGED_SERVICES`; spark's autostart and
watchdogs derive from it, Vali's stop list is held equal by a test, `cluster status` labels what is up on purpose or
should have been stopped. `handle_maintenance_request` is now a function (it was a block in the HTTP handler) so every
transition is testable. New `finish_maintenance_exit`: wait for every service UP, then RECOVERING to NORMAL by
compare-and-swap, then release the lock; on timeout the host stays RECOVERING with the lock held and the task names the
services. `leave` is refused with 409 unless the host is IN_MAINTENANCE, ENTERING_MAINTENANCE or RECOVERING.
Docs: new `docs/maintenance.md`; `cluster.md`, `spark.md` and the architecture guide corrected.

*Tests.* `test_maintenance_flow.py`, 37 tests: the rule's single source, the status labels, every enter and leave
transition and failure with the cluster stubbed (quorum, lock, claim, evacuation, marker, stop, services not up, retry,
a status changed meanwhile), and the boot path. The behavioural ones fail against the old Vali (it never wrote NORMAL).

*Not done.* Enforcing the rule continuously (the loop stays out of maintenance, so an operator can still start a unit
by hand; `cluster status` says when one is up that should not be). The quorum gate is kept although the database now
stays up (a maintenance host is the one that is rebooted and upgraded); whether to drop it is an owner decision.

Status: **done in code; live verification needed**.

### E. Host degraded / quarantined / rejoin: documented, two gaps fixed; needs a live check of the rejoin

*Deliverable.* `docs/host_states.md`: every state, what sets and clears it, what each stops (placement, DRS, migration,
maintenance entry, HA), thresholds with a verdict, how split brain is avoided, how a host rejoins, and an executable
transition table.

*Gaps found and fixed (by reading).* (1) A host marked DOWN while only partitioned still has the qemu processes of
guests that were restarted elsewhere; after it came back nothing stopped them or removed their libvirt definitions and
vdisk attachments. The leader now runs `reconcile_returning_host` before starting the host's services: destroys and
undefines domains Hydra places on another host (or none), detaches `<vm>-disk<N>` vdisks for such guests, leaves
alone anything placed here or unknown, and does nothing if Hydra or the domain list cannot be read. New typed endpoint
`GET /api/v1/host/domains`. (2) A quarantine lifted on the first clean probe, so a flapping host oscillated between
DEGRADED and NORMAL at probe speed; it now needs 6 clean passes in a row (`quarantine_lift_after_clean_passes`).

*Left (listed in the document).* No operator override from another host (`valcli host.clear` would be the addition);
the DOWN rejoin writes NORMAL after 10 s without checking services (placement still refuses a host with a service down);
the leader's 30 s detection window is short but safe because the fence precedes the failover.

*Tests.* `test_host_states.py`, 31 tests: the plan and its I/O with stubs (including unreadable inputs), hysteresis,
and a decision table over `self_fence_decide`. The reconcile and hysteresis tests fail on the old code (the functions
did not exist; the old loop lifted on one pass).

Status: **done in code; live verification needed for the rejoin**.

### F. Automatic VM migration as one story: done; tests; documented

*Found.* A manual or DRS migration checked only the target's own maintenance flag, so it could be aimed at a host
recorded DEGRADED, FENCED, DOWN or RECOVERING (start-time placement already refused them). DRS's cooldown lived in
process memory (lost at every Vali restart or leader change) and nothing stopped it picking the guest it had just moved.
*Change.* `host_ineligible_reason` is the one landing test (state NORMAL, answering, all services UP, not in maintenance;
unreadable inputs refuse); the migrate task uses it. DRS reads `hydra.vali_drs_history` for the global 300 s cooldown and
a 30-minute per-guest cooldown. Documented in `docs/vali.md` ("One path for moving a guest").
*Tests.* `test_automatic_migration.py`, 20 tests; 7 fail against the old Vali.
*Left, as decisions.* `select_best_start_host` falls back to the first healthy host when no host has the memory (an
overcommit); CPU/accelerator compatibility is libvirt's refusal.
Status: **done**.

### B. Editing VMs, the running-VM change list and ESXi parity: stopped-VM edit built and tested; live changes built (commands and XML tested, not run on a host)

*Environment.* Elixir/Erlang were not installed; Erlang/OTP 25 came from apt and Elixir 1.17.3 from its release archive, so `mix test`
ran: 934 tests at the baseline, 972 now, 0 failures.

*Stopped VM (console).* `/vms/<name>/edit` is the creation page in edit mode: `Form.from_vm`, `Vms.update_vm/2`, `Vms.edit_errors/2`.
Everything but the name is editable; disks only grow, keep their container, and only the last can be removed; storage is changed in the
order that leaves least behind and the row is written by compare-and-swap on the VM still being stopped. The VM's page offers Edit only
when stopped. Network (PXE) is offered as a boot device again. Tests: `vms_edit_test.exs` (29), `edit_live_test.exs` (9), one updated.
*Not exercised against a cluster:* the row's compare-and-swap (statement and parameter order are asserted).

*Running VM.* New: spark `POST /api/v1/vm/<name>/live` (`plan_live_change`, a pure planner; `--live --config` for everything except the
memory balloon), Vali task `live_change` (status lock, storage and network preparation, row update), `POST /api/v1/vms/live`,
`valcli vm.live`. Ops: vCPU hot-add, memory balloon, CD-ROM insert/change/eject, NIC add/remove/link, disk add/grow/remove. Create-time
vCPU headroom behind the cluster setting `vm_hotplug_headroom` (default off, unchanged XML). A real find on the way: the legacy
`/api/vms/update` drives CD-ROMs through a stale path, writes unescaped values into CQL, and ignores every `virsh` return code.
*Tests:* `test_vm_live_change.py` (30), `test_vm_live_vali.py` (40, including an integer column that would have been written as a string).
*Not verified, and said so in `docs/vm_lifecycle.md`:* that `update-device` swaps an NBD-backed CD-ROM, that `blockresize` makes qemu re-read an
NBD disk's size, and vCPU headroom on a real guest. Memory hot-add was deliberately not built (a balloon above the booted memory boots the
guest with the larger amount if it has no balloon driver); it is in the plan.

*Documents.* `docs/vm_lifecycle.md`: the edit rules, the live operations, the attribute-by-attribute table against ESXi (the ESXi column is from
general knowledge, labelled as unverified), and a prioritised parity plan (nine items with effort and dependencies).

Status: **done in code and tests; live verification needed**.

### H. Per-service review: document written (read-only pass), one high-severity finding fixed and verified, the rest listed

`docs/service_review.md` (about 1250 lines): every service's entry points, endpoints and callers, Hydra/ZooKeeper state, leadership,
failure behaviour, tests, and dead code/duplication/risks with evidence. It was produced by a read-only subagent; I re-read the claim
below before acting on it, and spot-checked nothing else, so treat the rest as a lead list with evidence to check, not as verified.
*Logos question:* each node writes only its own host's rows (keyed `(node_ip, timestamp)`), no election, no duplicate writes; the exception is a
missing `LOCAL_HYPERVISOR_IP`, which collapses every node onto 127.0.0.1. `docs/logos.md` is stale (it describes 30 s polling through Spark).
*Fixed:* Mipha's storage-fence rung posted `/v1/dfs/claim` (a Daruk route) to spark-daemon, which has no `/v1` routes, so the rung could never
confirm a fence. It now uses `run_lwt`; `test_fencing.py` pins it (the old tests patched the call, which is why nothing saw it).
*Listed, not fixed (see the document):* the shell-string guard covers six files only (`valcli.py:3050` builds `systemctl restart bifrost` as a
shell string); a second hard-coded watchdog list in spark-daemon with no ordering; protection-domain snapshots are never scheduled; Phoenix submits
tasks to the ZooKeeper leader not the Catalyst queue holder; a Phoenix page with no Slate rule; Catalyst's scheduler queues without checking
`holds_dispatch()`, and `/api/v1/tasks/status/<id>` splices an unvalidated id into CQL; about 30 disagreeing service lists; dead code with greps.
Status: **document done; one fix; remainder open**.

### I. Sidon / DFS open items: three built and tested, the rest documented

*Done (D-35, 31 Rust tests added or revived since the baseline of 371, 402 pass).* (1) **Idempotent appends**: a retried append is recognised by the replica
(bytes equal the journal's tail, via its own record or one tail comparison after a restart) and not written twice; the fence is still checked
first. (2) **Seal on detach**: `drain_and_seal`, used by detach and the handover's release; stays open (and the vdisk usable) if Hydra refuses; an empty
group is left to the sweep. (3) **`SIDON_HARD_CEILING`** with a startup warning when the journal volume could not hold eight busy vdisks' ceilings, and a
ceiling at or below the high-water mark replaced and said so.
*Documented, not built:* overlapped commits (the safe shape and the proof required first are in `docs/dfs/group_commit.md` section 6);
re-attach of NBD sockets at start (needs the claim, fence and journal recovery at start-up, which is not simple; the restart rule is documented);
`vhost-user-blk` already has its measurement plan in `docs/dfs/vhost_user_blk.md` section 5, unchanged. `docs/dfs/compaction.md` was re-read against
D-33: no stale text found. Reads outside the vdisk lock: not attempted.
*Not run:* the new seal and retry paths against a live cluster (they are covered by the fake-Hydra rig and the replica store tests).
Status: **done for the three; the rest not built**.

### K (part). valcli reaches a live peer when this node's database is down

`valcli` could run no command that reads the cluster on a node whose `hydra-db` was down, including the ones needed
to see why. `run_cql_query_via_peers` (aliased as `run_cql_query`, so the one-query-layer guard still holds) asks each other node in the cluster
document through its spark daemon after a local failure that is about reaching the database; a rejected statement is not
retried elsewhere. 10 tests. Documented in `docs/valcli_technical.md`.
Status: **done**.

### J. Rauru growth stages: three parts built and tested in isolation, nothing wired

Built (D-36): the pinned-key TLS verifier (`sidon/src/replicate/site_tls.rs`, 11 tests, a real TLS handshake over
loopback between throwaway certificates; I mutated the pin check and four tests failed, as they should); the Hydra-backed map sink
(`hydra_sink.rs`, 8 tests) against an in-memory Hydra I wrote, so it proves the order and the crash behaviour, not that Scylla accepts the
statements; and the failover and failback state machine as a pure function (`rauru_failover.py`, 17 tests).
*Not built:* the listener and the wire operations, the group store, the Purah root registration, the state tables, the driver.
*Wiring:* none of this is a service, so nothing was added to the service checklist; `docs/rauru.md` says so.
*Surprise worth knowing:* rustls 0.21 still sends a certificate request when the verifier names no root subjects (its newer
documentation says it does not); a test pins that anonymous dialling is refused.
*Needs the second cluster:* everything above running against a real remote.
Status: **built in isolation; the real work (a second site) not possible here**.

(Further items are added below as they are finished.)

## Needs live verification

- B. Stopped-VM edit: in the console, create a VM with an ISO, stop it, open Edit, grow the disk, add a second disk, change vCPU, save; start it.
  Pass: `valcli vm.list` shows the new vCPU; `valcli storage.list` shows the grown and the new vdisk; the guest boots with them. Remove the second
  disk again: its vdisk is gone from `storage.list`.
- B. Live changes (set `INSERT INTO hydra.cluster_settings (key, value) VALUES ('vm_hotplug_headroom', 'true');`, restart the VM first for vCPUs):
  `valcli vm.live <vm> vcpus 4` (pass: `nproc` in the guest shows 4 after onlining; `virsh dumpxml --inactive` shows `current='4'`);
  `valcli vm.live <vm> cdrom 0 <image>` then `eject` (pass: the guest sees the medium change; `virsh domblklist` shows the socket);
  `valcli vm.live <vm> disk attach 5` then `disk resize 1 10` (pass: `lsblk` in the guest shows the disk and then 10G: if the guest keeps the
  old size, `blockresize` does not refresh NBD disks: report it) then `disk detach 1 --confirm-delete`.

- E. Rejoin reconcile, on the lab: with a VM running on `.43`, stop spark-daemon on `.43` (not the VM) until Mipha marks it DOWN
  and restarts the VM elsewhere, then start spark-daemon again. Pass: the leader's log (`journalctl -u mipha | grep rejoining`)
  shows `destroy <vm>` and `undefine <vm>` for `.43`, and `virsh list --all` on `.43` no longer shows the VM; a guest placed on `.43`
  by Hydra is untouched.

- D. Maintenance, on the lab: `valcli host.maintenance.enter <host>` (a host with at least one running VM), then
  `cluster status`. Pass: the task succeeds; the host shows `[IN MAINTENANCE]`; ZooKeeper, HydraDB, Daruk, Sidon,
  Hylia and Spark are UP and labelled `(kept up in maintenance)`; the other 14 units are DOWN; nothing is labelled
  `expected to be stopped`. Then `valcli host.maintenance.leave <host>`: pass = the task succeeds, `valcli host.list`
  shows NORMAL (it used to stay RECOVERING) and `hydra.cluster_locks` is empty. Also run `leave` on a NORMAL host:
  pass = a 409 saying there is nothing to leave.

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

- C. Boot order, on the lab: create a VM in the console with the lab's ISO and a new empty disk (default
  boot device), start it, open its console. Pass: it boots the installer without stopping at the boot
  options screen. `ssh root@<host> virsh dumpxml <vm> | grep -n "boot order"` shows `order='1'` on the
  cdrom and `order='2'` on the disk, and no `<boot dev=` under `<os>`. Then set the boot device to `hd`
  and check the order swaps.

## Decisions for the owner

- **vCPU headroom default** (`vm_hotplug_headroom`): off, because it changes guest-visible hardware (the guest sees up to 4x its vCPUs as
  possible-but-offline). Turn on for the lab to exercise hot-add, or make it the default?
- **Rename and removal from the middle** need a stable id per VM and per disk instead of ids derived from names and positions: a
  migration of every vdisk id. Worth doing, and when? (parity plan item 7)

- Memory overcommit on placement: `select_best_start_host` returns the first healthy host when no host has enough free
  memory, so a start or evacuation can land on a host that cannot hold the guest. Keep, or refuse?
- A `valcli host.clear <host>` to clear a DEGRADED/FENCED status from anywhere (today only `mipha --clear-self-fence` on the
  host itself does)?

- **What a host in maintenance keeps running** (item D). I decided it so the flow is consistent: kept up =
  spark-daemon, zookeeper, hydra-db, daruk, sidon, hylia; stopped = everything else (Mipha and Logos included).
  It is one column in `MANAGED_SERVICES`; say if it should differ (the likeliest argument is Sidon: kept so vdisks
  with a replica on the host stay fully replicated, but a disk or kernel job on the host needs it stopped by hand).
- Keep the quorum gate for maintenance entry although the database now stays up? Kept (rolling upgrades restart it).
- (Not mine to decide, untouched) the console Quadlet --privileged question; a two-node tie-breaker; dedup beyond
  the estimator; erasure coding; component naming.

## Found but not fixed

- Rust test `vdisk::tests::group_commit::with_two_replicas_a_repair_makes_both_identical_to_the_owner` failed
  once in a full parallel `cargo test` and passed on every rerun alone; timing-dependent.
- The Sidon disk path in `generate_vm_xml` ignores the bus recorded for a disk (every disk is virtio,
  `vdX`), although the form offers other buses. Not changed: it is a behaviour decision (and the editing
  flow in item B has to say what a bus change means).
- `test_sidon_owns_its_mounts` fails 8 ways in this container (environment), unrelated to this work.
- The rollout (`deploy_updates.py`) never writes the Sidon unit file, so a change to the unit's text reaches a node only through
  provisioning; every other service's unit is written by both. Not changed: it needs the unit text moved into one place first.
- `static/app.js` still names Sidon's metrics `aether_*` (`aether_storage_pools`, `aether_heal_pending`, `aether_split_brain`, ...).
  They are the metric names the exporter emits, so renaming is a coordinated change in the exporter and the page, and component
  naming is the owner's decision.
- The Rust tests that build certificates (`replicate::site_tls`) need the `openssl` command, and fail loudly without it rather than
  skipping.
- Items from the earlier sections of this report that are listed as "not built" (re-attach of NBD sockets at Sidon start, overlapped
  commits, `vhost-user-blk`, the replication listener and driver) stay open; see each section for what is missing.
- Mimir's checks (quarantine age, maintenance consistency, the replication-factor rule) and the Phoenix items in TODO.md under "console lost
  function" were not reached; nothing was changed there.

### Second audit pass (read-only agents over every service; findings checked before acting)

Fixed (tests added): Catalyst and Vali pick up renewed certificates (impa restarted only spark-daemon); `lanayru` destroy deleted every VM
whose name merely started with the cluster name, vdisks included; `impa --rotate-ca --nodes` is refused; and the items listed in
`docs/service_review.md` 4.1/4.2.
Found and **not fixed** (each is a design or a larger piece of work; none is a one-line change):
- **Lanayru cannot produce a working guest cluster, and reports success.** The overlay branch runs only for a segment id starting with `ov-`, which nothing
  produces; the VM disk XML is hand-built (a block device pointing at a socket) instead of `sidon.disk_xml`; every `virsh define/start`, cloud-init write and
  `genisoimage` result is unchecked; steps 4 to 6 are log lines and sleeps with no code behind them; the cluster is then marked `active`. Also: cloud-init
  hard-codes gateway `172.16.10.254` for nodes on `172.16.11.x`; static node IPs sit inside the DHCP range; VNIs and subnets are hard-coded with no uniqueness
  check, so a second deploy collides; `network_id` is stored as a name so Vali regenerates the VM on `virbr0`; the DHCP-refresh submission lacks `service` and is
  refused. Needs the owner's decision on whether Lanayru is a supported feature before it is rebuilt.
- Urbosa: a tier-1 router namespace gets its uplink only on the node holding the VIP, so north-south traffic from other nodes is broken or random; per-host dnsmasq
  with no shared lease store; veth names exceed 15 characters for VNI >= 10,000,000 (no range check); the VXLAN MTU is set to 1500 and silently rejected on a 1500 underlay.
- Console token: it comes from the unauthenticated WebSocket query string and reaches a CQL statement unescaped (injection when the cqlsh fallback runs);
  `console_sessions` has no TTL and nothing deletes rows; Traefik's access log may record the token in the URL, which undoes Agahnim's redaction.
- Logos: host throughput now counts physical NICs only (it summed every interface, so the same bytes were counted three or four times; fixed, tested). Still open: one
  failing table fails the whole batch including host CPU and memory samples.
- Impa: rollback restores node certificates but not the staged CA directory; a failed rotation deletes the staged new CA key so it can only be rolled back.
- Sidon (9105) and Agahnim load their certificates once at start; see `docs/mtls_lifecycle.md`.
- Agahnim: an `accept()` error ends the whole accept loop; no timeout on the first read; the whole `Sec-WebSocket-Protocol` list is echoed.
- Hard-coded guest root password in `lanayru.py` (two places, in git history): needs rotating and moving to a generated per-cluster secret. Value deliberately not repeated here.
