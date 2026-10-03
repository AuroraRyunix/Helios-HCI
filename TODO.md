# Helios-HCI Roadmap & Technical Debt

Living backlog. Merges [docs/audit_findings.md](./docs/audit_findings.md) and
[docs/add_ons_design.md](./docs/add_ons_design.md) with a full source sweep of the storage,
networking, and deployment layers performed on **2026-08-17**.

Items are grouped by severity, then subsystem, and cite `file:line` so they can be verified against
the current tree. See [docs/history/walkthrough.md](./docs/history/walkthrough.md) for older fixes.

---

## Fixed on 2026-08-17

Recorded because several of these contradicted existing documentation, and one was documented as
already-fixed when it had never worked.

**Data loss / cluster-down**
* Mipha split-brain now decides **per resource** from DRBD role and device holders, never from
  cluster-wide ZooKeeper leadership, and never discards on a resource whose device is in use
  (`mipha.py`). The old behaviour made a node running a VM discard its own live writes.
  `docs/audit_findings.md` §5.A explicitly recommended the approach that was the bug — corrected in place.
* `cluster destroy` no longer appends `/dev/sdb` unconditionally after the safety scan
  (`cluster_new.py`, `spark_daemon_decoded.py`). The mountpoint guard is now an exclusion of *any*
  mounted filesystem rather than an allowlist of six system paths, candidates are cross-checked against
  `/etc/hci/aether/storage-pools.json`, and the plan is printed per device before anything is destroyed.
* `spark_daemon_decoded.py` no longer broadcasts one node's device names to every host during destroy.
* Dual-primary closed: `--allow-two-primaries` removed from VM disk create/update, DRBD promotion on VM
  start is a checked step that aborts the start on failure (`vali.py`), and the migration window is
  opened and closed around `/api/vms/migrate` instead of being permanent.
* Gatoway no longer tears down every `br-vlan-*` when a database read fails. Failure is now distinct
  from empty, the deletion pass is skipped on an untrustworthy read, and a bridge with enslaved
  interfaces is never deleted.
* Hylia's deploy is now staged, checksum-verified on the node, atomically renamed into place, and rolled
  back from a backup on failure. The old path `rm -f`'d the target *before* writing, with a trailing
  `|| true` that made the error check unreachable — a dropped call could leave `spark-daemon` missing,
  destroying the channel needed to push the fix.

**Security**
* VM names are validated at every endpoint that reaches a shell or CQL (`spectrum_server.py`, `vali.py`).
  They were previously unvalidated while usernames and timezones were not.
* Session tokens are validated against the generated format before entering CQL, pre-auth
  (`spectrum_server.py`); logout now also evicts the session cache.
* Update manifest `target_path`, `file`, `sha256`, and `changelog` are validated before use (`hylia.py`);
  `target_path` previously went straight into a root shell string from an unhashed manifest, and
  `changelog` was an arbitrary file read whose contents reached the database and WebUI.
* Urbosa firewall fields are strictly validated before reaching `iptables` under `shell=True`.
* Update download now requires a well-formed SHA-256 and an `https://` URL from an allowlisted host,
  re-checked after redirects (`spectrum_server.py`).
* `check_updates.py` escapes all values from the update server; `size` is coerced rather than
  interpolated as a bare literal.
* Image CQL escaping applied at five sites (`spectrum_server.py`); image block device is `0660 root:qemu`
  rather than `0666`.
* `deploy_updates.py` verifies SSH host keys instead of `AutoAddPolicy`, with
  `HELIOS_SSH_TRUST_NEW_HOSTS=1` as an explicit first-contact opt-in.

## Fixed on 2026-10-04: `valcli storage.list` showed a restarting node as online with 0.0 GiB

While sidon is down or starting its control socket does not exist, so spark answers the capacity
request with `503 {"error": ..., "kind": "io"}`. `valcli`'s `run_mtls_spark_api` returns an HTTP
error body with rc 0 (so a 409 keeps its explanation), and `cmd_storage_list` read
`total_bytes or 0` out of that dict: "online, 0.0 GiB". Reproduced on the lab by stopping sidon on
one node. Sidon, spark and the Phoenix page were not at fault (sidon fails the request outright
rather than answering zero; Phoenix already treated errors as unreachable). `valcli` now reads the
HTTP status (`run_mtls_spark_api_full`, `extent_store_row`) and shows `not ready` / `unreachable` /
`error` / `online`; Phoenix distinguishes a 503 node as "starting" in the stores banner.
`test_storage_list_not_ready.py`, `index_live_test.exs`. Not changed: `hylia`/`vali` read the same
body but already fail closed on a zero capacity (the log message for a starting node is generic).

## P2 — Two-node clusters have no quorum tie-breaker (2026-10-03)

**The witness node is gone from the docs because it is gone from the code.** `docs/cluster.md`,
`docs/cluster_technical.md` and `docs/ring_lifecycle.md` described a diskless third host, a
ZooKeeper voter and nothing else, auto-flagged by position in a three-node layout. It was a
**two-node quorum tie-breaker** added in July 2026 (commits `6cd254c` to `6ebbc6c`). The code has no
`--witness` flag and no `is_witness` field, and the running three-node cluster has HydraDB, Daruk and
Sidon on all three hosts, so the docs now say every host is a full member. Three stale comments and
a test were reworded: the behaviour they described -- a host that is not in the ScyllaDB ring holds
no replicas, so stopping it costs the ring nothing -- is generic and never depended on a witness.

I could not find the commit that removed it, so **it is not known whether that was deliberate**.
The need it served is still real for one case: **a two-node cluster has a two-member ZooKeeper
ensemble, which needs both nodes up and tolerates no failure.** Nothing in the toolkit offers a
tie-breaker for that. Whether to bring one back, and as what (a ZooKeeper-only third host is the
natural shape now that DRBD's odd-voter requirement is gone), is a decision for the owner.

**Also found while correcting `docs/cluster.md`:** `cluster create` still starts the application
daemons by hand in its phase 6, which `cluster start` stopped doing when it became declarative. The
same duplication, and the same risk -- it is how `cluster start` came to restart `aether` for
months after the unit was deleted. And the volume group is still named `vg_aether`, a name left over
from the DRBD design that nothing but history explains.

## P1 — The Phoenix console lost function in the port (2026-10-02, restored 2026-10-03; needs a pass in a real browser)

Reported from using it. The pages exist and render, which is what "every page is Phoenix"
measured; it did not measure whether each page can still *do* what its predecessor did. The
instruction for all of these is **port the old one faithfully first**, then improve.

* [x] **Settings is read-only for things that used to be editable**, the VIP among them. *Done: cluster name, VIP, subnet and replication factor are inputs, and saving applies to every host as the old save did (resolv.conf, chrony, timezone, cluster.json, bifrost restart, ALTER KEYSPACE + repair, scrub schedule). Operator accounts can be created and re-passworded. SSL upload, node add/remove, maintenance operations and language/theme from the old Settings page are still not ported.* A
  settings page that displays a value it will not let you change is worse than no page,
  because it looks like the feature was removed rather than the form.
* [x] **"Policies" is a page nobody can explain.** *Done: it was a panel on Settings, now removed; `/policies` shows snapshot policies, protection domains, container policies and the security policy, read-only. What it should be was inferred from the schema, not specified.* Work out what it was meant to be against
  `static/settings.html`, and either port that faithfully or delete it. An unexplainable
  page is a bug regardless of which way it resolves.
* [x] **VM create lost most of its options.** *Done, except Secure Boot and PXE boot, which Vali does not implement and so are not offered (see docs/console.md).* The old form carried the full set; the new one
  carries a fraction, so a VM that needs anything beyond the defaults cannot be created
  from the console at all.
* [x] **Disks and CD-ROM are a single text box each.** *Done: repeatable rows for disks, CD-ROMs and NICs.* They used to be repeatable rows you could
  add to and remove from, which is how a multi-disk VM gets built. A comma-separated string
  is not a replacement for a list, and the backend already takes a list -- `disks_list` is
  parsed from one.
* [x] **The storage page is a regression** on the one it replaced. *Done: re-laid-out; the data was already all there.* Treat the old page as the
  specification.
* [x] **Front page uses about 55-60% of the window width.** *Done: the 1280px column is gone on every page.* There is screen real estate going
  spare on a dashboard whose whole job is density.

**And one that is not a regression but a new capability (still open, blocked on D-23; deliberately not built here)** the storage layer can nearly
support: **dedup**. Container compression shipped in `0008`; dedup is the obvious next
property an operator would expect beside it. It is *not* a settings toggle away, and the
reason is recorded in **D-23**: dedup needs the extent id map, the middle level between the
block map and extent groups that makes an extent an addressable thing several vdisks can
reference. That level now exists (stages 1 and 2, below), so the naming prerequisite is met;
what is not met is everything D-23's **addendum** costs out -- an inline dedup would put
roughly thirty times today's lightweight-transaction load on the drain, needs a lease to close
a resurrection window the sweep's guards do not cover, and returns space only through a
compaction pass that does not exist. The owner wants dedup; the addendum's recommendation is
a **read-only estimator first** (a Purah pass that hashes sealed extents and reports how many
would be shared beyond what clone-from-image already shares), then compaction, then a
background post-process pass, each only if the last earned it. The settings toggle stays last.
**Update (D-32):** the estimator and compaction now exist -- `valcli storage.dedup.estimate` and
`valcli storage.compact` -- both operator-invoked, neither on a timer, and neither enables dedup or adds
a setting. The next step is to *run the estimator on real data* and compare it with D-23's 10-15% bar;
the background post-process pass stays unbuilt until that number says it should exist.

**Coordination and tasks (2026-10-02)**

Four changes that came out of reading a running Nutanix cluster rather than guessing at it.
The notes themselves are deliberately not in this repository.

* **Leadership is per job, not per ensemble.** Ten leader-only jobs decided they were the
  one by comparing their own address to the ZooKeeper *ensemble* leader's
  (`vali.py` was literally `get_zookeeper_leader_ip() == LOCAL_IP`). The ensemble elects for
  its own reasons, so every one of them relocated together whenever it did, onto a single
  node. Each now holds its own candidacy under `/helios/leaders/<service>` -- a persistent
  parent, one ephemeral sequential ballot per candidate, lowest counter leads -- so failover
  is a property of the session rather than of anyone noticing. One election per *job*:
  `vali`'s DRS loop and its queue worker are not the same question. Four of the nine private
  `stat` probe loops were deleted outright, their only caller having been the placement gate.
  `helios_zk.leader_ip` stays, because "which node leads the ensemble" is still a real
  question that `mipha` and `cluster status` ask. See
  [docs/service_leadership.md](docs/service_leadership.md).
* **Tasks outlive the leader.** `recover_stuck_tasks()` aborted every pending task at
  startup, which is data loss written down as recovery. Tasks now carry a parent, an owning
  component and a per-component sequence (migrations `0011-catalyst-task-parent` through `0016-catalyst-task-sequence`), and recovery
  re-queues what was pending and fails what was in flight with a reason saying its progress
  is unrecorded. `component` is not `service`: a Hylia upgrade step runs on the `dagur`
  queue, and recording it as a Dagur task loses who asked for it.
* **The reconcile loop is told rather than asking.** `helios_zk` discarded the only frames a
  watch could arrive on, so the loop re-read `/cluster_state` on a timer -- a declaration
  nobody hears for half a minute, which is why `cluster start` drove services by hand. There
  is now a frame demultiplexer, watches that re-arm across a reconnect, and a loop that
  blocks on an event. Confirmed against the live ensemble on 3.9.2, including the case that
  matters: a `getData` on a missing path registers no watch, and `/cluster_state` does not
  exist on a cluster that has never been started.
* **`cluster start` names no services.** It drove ScyllaDB and the core services through
  numbered phases and then waited for convergence anyway -- two actors per service, which is
  the flapping a cold start shows, and the hand-written list is what kept restarting `aether`
  after the unit was deleted with DRBD. From 43 occurrences of 18 distinct service names to
  none, asserted by a test. What moved into the loop is a declared service table with units,
  what each requires, and readiness meaning *answering* rather than `active`. systemd keeps
  lifecycle; no supervisor was built, because systemd already is one -- the one place this
  deliberately diverges from how Nutanix does it.
* **Extent groups record how hot they are** (migration `0017-egroup-access-data`), which
  `docs/dfs/multi_disk.md` needs before tiering can decide what to spill down. Counters are
  absolute totals per observer, so a flush is safe to fire and forget. The data may decide
  *where a copy of bytes goes, never whether it exists*.

  **The extent id map** -- the middle level Nutanix has and Helios does not -- is **D-23**,
  and is staged because it cannot be landed behind a read-path flag: Purah's mark phase
  derives liveness from the block map, and the first row naming an extent instead of a group
  makes a sweep see every live group as unreferenced, with the two-scan grace delaying that by
  ten minutes rather than preventing it. Built, in this order:
  * **Stage 1 (built, undeployed): Purah marks through both levels**, fails closed on an
    extent it cannot follow, and is proved by a randomized model. **This is the one to roll out
    first, alone, and let run through a full sweep cycle** on every node (restart sidon: the
    deploy does not) before anything else on the list; what to observe is in
    [docs/dfs/extent_id_map.md](./docs/dfs/extent_id_map.md).
  * **Stage 2 (built, undeployed, dormant): migrations `0020-dfs-extent-id-map` and
    `0021-dfs-block-map-extent-id`** and a read path that resolves extent-named rows only
    when a row has no `egroup_id`. Nothing writes an extent id, and a test pins that.
  * **Stage 3 (designed, not built): writing extent ids.** Needs a third migration id for the
    per-vdisk writer opt-in on `dfs_vdisks` (not assigned), a drain commit that nulls
    `extent_id` on repoint, `derive_child` taught the new shape (it refuses it today, the safe
    direction), and a Purah pass reclaiming extent rows orphaned by a vdisk delete.
  * **Corrected in D-23:** the middle level is not what lets clones diverge per extent -- they
    already do. Its value is that relocating an extent group costs one row instead of a scan of
    every block map, which tiering and compaction will need.
  * **Found, not fixed:** the drain's `INSERT` lists only the columns it sets, so once a row has
    ever named an extent a repoint leaves the old `extent_id` behind; marking tolerates it (it
    marks both) but it must be nulled in stage 3. Not fixable earlier, because naming the column
    in the drain before `0021` is on every node would break drains.

* **Extent groups have a deliberate disk, and can be moved between a node's disks.** Disks are
  identified by a `disk.uid` file on their own filesystem rather than by `disks/sdc`, a kernel
  name that already names the wrong device on one node. Placement prefers the container's tier
  where a node has a disk of that class, else most-free-first as before. `valcli storage.list`
  shows each disk, `storage.placement` lists which holds which group, and `storage.tier` /
  `storage.move` move sealed groups: copy, verify against the seal hash, publish by rename,
  switch -- and the old copy is left for the sweep's two-scan grace (D-26). Operator-invoked;
  nothing runs it on a timer. The test nodes' disks are identical, so there the policy plans
  nothing and the mechanism is exercised by `storage.move`; no performance claim is made.
  **Not built**: the journal on the fastest disk (a live journal cannot be relocated without a
  drain, and there is no faster disk to put it on), any unattended tiering, and anything
  showing tiering improves a workload.
  **Known gap**: `hydra.dfs_egroups.path` records where a group was created and is stale after
  a move; nothing reads it, and fixing it would turn a node-local fact into a cluster write.
  **Known gap**: `capacity` counts a copy in progress and a surplus copy as extent groups,
  which is true of the bytes and misleading as a count until the sweep clears them.


* **Extent-group compaction and a read-only dedup estimator** (D-32,
  [docs/dfs/compaction.md](./docs/dfs/compaction.md)). `valcli storage.compact` finds sealed groups
  below a live fraction (default 50%), copies their live extents into a new group (verified against the
  seal hash and every referrer's footer, replicated and read back, registered, published by rename),
  repoints the rows by compare-and-swap and leaves the old group to the sweep. Plans unless `--apply`;
  bounded by groups, bytes, rate and time; never on a timer. A writable vdisk's rows are rewritten only
  while this node owns the vdisk, has it attached and holds its drain gate; a group a writable vdisk not
  attached here points into is skipped whole. `valcli storage.dedup.estimate` hashes a sample of sealed
  extents and reports per container what dedup would share beyond clone sharing; it writes nothing. Two
  new Daruk swaps (`/v1/dfs/block-map-repoint`, `/v1/dfs/extent-repoint`); **no migration**. Rust tests
  stop a batch at every step, overwrite mid-pass, share extents across clones and snapshots, refuse at
  a replica, and run random maps to a fixed point; `test_compaction.py` pins the wiring and the rules.
  **Not built / known:**
  * **Replica copies of a dead group are never reclaimed** -- not by the sweep, not by compaction, not
    when a vdisk is deleted -- as far as the code shows (`replica-egroups/` has no remover). Compaction
    therefore *adds* its live bytes on every replica and frees nothing there; the plan prints both
    numbers. A replica-side drop (an opcode sent by the node that swept a group) is the fix and is the
    thing to build before recommending `--apply` at ftt>=1.
  * Groups a writable vdisk not attached on the creating node still points into (a clone on another
    node, a detached VM) are skipped; a safe way to exclude that vdisk's drain from here does not exist.
  * Nothing has run on the test cluster: the passes are proved against a model of Hydra and real extent
    files, not against live nodes, and the lead integrates the live check.
  * Found while reading, not fixed: the sweep skips every `open` group (`skipped_open`), so an open group
    a crashed drain abandoned, and which no row references, appears never to be reclaimed.

**Cluster state (2026-08-17, later session)**
* **ZooKeeper-backed cluster state shipped.** Desired state lives at `/cluster_state`;
  each node's spark-daemon publishes an ephemeral `/helios/nodes/<ip>` znode every 5s and
  converges local services toward the desired state. `cluster status` reads that tree in
  one connection and renders locally (adds `--json`), falling back to the direct mTLS
  probe with an explicit notice. `cluster start` now waits for the cluster to report its
  own convergence, printing which services are still pending. See
  [docs/cluster_state.md](./docs/cluster_state.md).
* **`helios_zk.py` (new)**: minimal stdlib ZooKeeper 3.x wire-protocol client, since the
  repo has no third-party dependencies and the existing code only spoke the read-only
  four-letter-word commands. Wired into all five deployment paths.
* **Crash-looping services no longer report UP.** `hylia` had failed 31 consecutive times
  while `systemctl is-active` returned `active` during each restart window, so status
  sampled it as healthy. Restart counts are now published and a unit that is active with
  no main PID after repeated restarts reports `FLAPPING`.
* **Autostart deadlock broken.** Cluster state lives in ZooKeeper, but autostart read it,
  failed when ZooKeeper was down, defaulted to "stopped", and then stopped ZooKeeper --
  a latch that never reopened. ZooKeeper is now treated as infrastructure: started
  unconditionally, removed from every stop list, and "unreadable" is no longer conflated
  with "stopped".
* **The abandoned Quadlet migration reverted.** Eleven daemons pointed at
  `localhost/helios-base:latest`, an image no commit ever built, so none could start. The
  migration had also dropped the maintenance interlock from nine units and most cgroup
  limits, and never updated `spark.py`. `deploy_updates.py` carried the same broken
  definitions and would have re-broken every node on the next rollout.
* **Secure Boot pre-flight.** `provision.py` now refuses to provision a host with Secure
  Boot enabled (DRBD is an out-of-tree module), checked before the node is modified, and
  `modprobe drbd` no longer hides failure behind `|| true`.
* **CRLF corruption of every deployed script fixed** at three layers (`.gitattributes`,
  `sync_provision.py` normalization, `write_file` normalization).

**Follow-up pass (2026-08-18)**
* **RF changes now replicate.** `ALTER KEYSPACE` only changes the replication strategy;
  existing data is not copied to new replicas until a repair runs, so the cluster reported
  full fault tolerance while a partition still lived on one node. Both ALTER sites now go
  through `alter_keyspace_rf()`, which runs `nodetool repair -pr hydra` in the background
  whenever the factor increases (or when the previous factor could not be determined).
  `get_actual_replication_factor()` no longer returns a reassuring `"3"` on error -- it
  returns `"unknown"`, which is visibly wrong in the UI, which is the point.
* **`mipha --auto-heal` implemented** and the Dagur cron repointed at it. Runs the slow
  storage work that must not sit in a 10-second liveness loop: `drbdadm verify` scrubs,
  Linstor pool usage with thin-pool metadata pressure, and under-replicated resource
  detection. Chosen over a new daemon so the DRBD logic keeps a single owner and no fifth
  deployment list is created. Note the pre-existing `insert_storage_auto_heal` was defined
  but **never executed**, so the job had never been registered at all -- it was not failing
  nightly, it did not exist. Now wired up, with a migration for any cluster carrying the
  old `/usr/local/bin/hci-auto-heal` command.
* **Convergence is now continuous.** The reconcile loop re-asserts desired state every 30s,
  comparing actual against desired and acting only on drifted units -- so the steady state
  costs one batched `systemctl is-active` and issues no commands. Verified: a service
  stopped out from under the daemon returns in ~25s, a service started while the desired
  state is `stopped` is re-stopped in ~30s, and a quiet cluster logs nothing.

**Testing pass (2026-08-19)**
* **`cluster destroy` disk safety proven on hardware.** A real XFS filesystem with data was mounted
  on a spare disk and both versions of the discovery logic run in plan-only mode: the old code listed
  it for wiping, the new code skipped it with `mounted at /srv/backup`.
* **Image upload fixed -- it had never worked.** It failed with `ENOENT` on the DRBD device because
  Spectrum runs in a container that mounts no `/dev`; the `test -b` probe passed only because it runs
  on the host via spark-daemon. Mounting `/dev` into the web tier would have been the wrong fix.
  `POST /api/v1/storage/device/write` now streams the body onto the device from spark-daemon, and
  Spectrum opens neither a device nor a staging file -- the same split as Stargate rather than Prism
  owning the data path. Verified byte-identical on the device.
* **CI is green** on all four jobs, after two real failures: a stray carriage return that stopped the
  workflow parsing, and Elixir 1.20 formatter output that 1.17 rejects. Both now guarded by tests.

**Correctness bugs found while fixing the above**
* **The migration lock had never worked.** `vali.py` wrote `SET status = 'migrating'` to a column that
  does not exist on `hydra.vms` (there is only `state`), and the read side `vm_data.get("status", "")`
  was therefore always `""`. Both directions dead; the write's return value was unchecked. Column added
  to the schema and both `ALTER` blocks; the write is now checked. `docs/audit_findings.md` listed this
  lock as already fixed.
* **`submit_and_wait_task` gave live migration a ~6 s ceiling**, and its `204` branch `continue`d without
  sleeping, burning the entire poll budget instantly. Both fixed; migration and power operations now have
  named, realistic timeouts (`vali.py`).
* **spark-daemon's cluster-create path was hard-broken.** The embedded `disk_claim_script` had a
  pre-existing `IndentationError`, and `handle_cluster_create` dispatches it to every node then
  JSON-parses stdout — so it raised "returned invalid json" every time. Only the `cluster` CLI path worked.
* **Hylia's reboot pre-flight raised `TypeError` on every multi-node upgrade**, after files were already
  deployed, because it indexed a list of IP strings as dicts.
* **Lanayru was never deployed.** It had no `*_B64` constant, no deploy block, and no package entry, while
  `spectrum_server.py` imports it at runtime. Now embedded in `provision.py`, added to
  `sync_provision.py`, `create_upgrade_zip.py`, `check_updates.py` inventory, the Spectrum build context,
  and the `Dockerfile` (which lacked `COPY lanayru.py`, so `/api/lanayru/*` raised `ModuleNotFoundError`
  in any container built from it).
* **Daruk ran stale code after an LCM patch.** The unit executes the copy inside the DB volume, but LCM
  only replaced `/usr/local/bin/daruk.py`. An `ExecStartPre` now refreshes it on every start.
* `sync_provision.py` covered 21 of 24 constants and only warned on missing *files*. It now covers 25/25,
  detects drift in both directions, resolves paths from its own location, and aborts **before writing**
  rather than leaving a half-synced `provision.py`.
* `create_upgrade_zip.py` gained the four missing components and now stamps file modes into the archive
  (`zipfile.write` had been shipping `mcli`/`mcli-runner`/`catcli` without `+x`); `ZIP_NAME` derives from
  `VERSION`.
* Bifrost: `current_prefixlen` was never actually `global`, so shutdown released the VIP with a hardcoded
  `/24` and silently failed on other prefixes. VIP presence is matched exactly rather than by substring.
  The health guard checks the client-facing port. Candidate sort is numeric.
* Urbosa: `pgrep` inside `ip netns exec` matched processes from other namespaces (`ip netns exec` swaps
  only the *network* namespace), so the T1 DHCP server never started. Firewall rules now live in a
  flushed-and-rebuilt `URBOSA-FWD` chain with deterministic ordering. Four remaining substring address
  comparisons made exact — the segment-gateway one was live, reading `10.0.0.1` as present when the
  interface held `10.0.0.10`. `urbosa_bootstrap.py --cleanup` updated for the new chain.
* Gatoway re-resolves the uplink interface every pass instead of once at startup.

---

## P1 — Open security items

* ~~The update chain still has no signature.~~ **Resolved (2026-08-20)**: detached Ed25519 signatures
  over the release document and the package manifest, verified against a key pinned at provision time
  (`/etc/hci/keys/release_ed25519.pub`). Asymmetric rather than a shared-secret MAC because every node
  must verify, so a MAC would let any compromised node forge releases for the whole fleet.
  `check-updates` now reads version, URL, digest and changelog *only* from the signed payload; hylia
  refuses an unsigned package before it reads a digest, which is the only anchor the manual-upload path
  has; and `/api/lcm/upgrade/download` takes the URL and digest from the verified row rather than the
  caller's POST body, which was a way to route around the check entirely. Fails closed, verified live
  against the real update server. The transition escape hatch is deliberately awkward and never accepts
  a *bad* signature. See [docs/update_signing.md](docs/update_signing.md).
  **Outstanding**: `RELEASE_PUBKEY_PEM` in `provision.py` ships empty -- generate a release keypair and
  paste in the public half, or update checks stay failed-closed.
* **Unsandboxed root command execution.** `/api/v1/execute` runs caller-supplied strings via
  `shell=True` as root. **In progress**: the typed API in [docs/spark_api.md](./docs/spark_api.md)
  covers 28 paths. **149 call sites remain**, down from 194: `cluster_new.py` 58, `hylia.py` 38,
  `spectrum_server.py` 34, `vali.py` 15, `mipha.py` 3, `dagur.py` 1.

  Those figures are counted, by one stated rule, and the rule matters because the last two
  recordings were both wrong. A **call site** is a call to `run_remote_spark` or to any function
  that takes a command as a parameter and forwards it to one -- `run_parallel`,
  `run_checked_cmd`, `run_parallel_checked`; `run_linstor_cmd` before it -- not counting the
  forwarding call inside the wrapper, which is the wrapper. The figure of 105 missed
  `cluster_new.py` and `dagur.py` entirely. The 184 that replaced it had the total nearly right
  and the split badly wrong: when it was written `hylia.py` held 37 and was recorded as 22, and
  `spectrum_server.py` held 36 and was recorded as 64. The total is what gets quoted; the split is what anyone
  planning the next family actually reads.

  **Two families are finished.** Counted as shell strings rather than call sites, because a
  string in a loop over every node is one place to get wrong and many invocations:

  * **systemd unit control** -- **48** strings, not the ~15 estimated here. 44 are gone: 42 that
    were handed to spark-daemon, and two local `subprocess.run("systemctl restart ...",
    shell=True)` in `spectrum_server.py` that became argv lists on the way past. `GET`/`POST`
    `/api/v1/host/units`, with unit names matched against `MANAGED_UNITS` -- every unit
    `provision.py` installs, plus `chronyd`, `libvirtd` and `virtqemud`, the three host units the
    stack drives without owning. `ignore_failed` is the `|| true` these strings carried and
    `detach` is the backgrounded subshell a node needed to restart the daemon answering the
    request. Four strings remain and each has a reason recorded in the test: printed advice to
    an operator; a line inside the base64 wipe script `cluster destroy` runs (a
    filesystem-family call site, and it moves when that does); hylia restarting itself locally
    with a constant command; and `mipha.legacy_spark_fence`, which is reached only when a host
    answered 404 to the typed fence and therefore must not require a typed endpoint of its own.
  * **network probing** -- **11** strings, all gone. `GET /api/v1/host/listeners` (with `?port=`,
    answering a boolean, because `ss -tlnp | grep 9042` also matches a peer address of
    10.0.90.42, a queue depth and another process's pid), `GET /api/v1/host/interfaces`, and a
    `cidr` field on `/api/v1/host/network`'s addresses so the console stops re-reading
    `ip addr show` through a shell for a string it was handed the parts of. `mipha.ping_host`
    went to argv in passing -- it interpolated an address out of `cluster.json` straight into a
    root shell.

  `test_spark_shell_calls.py` asserts the property rather than the change -- no caller builds a
  shell string in either family -- reading string literals out of the AST, so an f-string is as
  visible as a plain one and the comments explaining what each call site used to be are not
  mistaken for the thing they replaced. Its allow-list checks found two dead unit names in
  hylia's restart map (`aether`, removed with DRBD; `spark`, which was never the unit name --
  it is `spark-daemon`), both of which had been failing silently as unknown units, and a whole
  stale phase in `cluster start` that restarted `aether` and then exited when it did not come
  up. `cluster create` lost that phase when the unit was deleted; `cluster start` kept it, so
  every `cluster start` since has failed on a service that does not exist.

  The remaining families: filesystem operations (the largest, and needing careful path
  allowlisting), `podman`/container operations, `virsh`, base64 file transfer in the LCM paths,
  `podman exec` for cqlsh (which belongs behind Daruk rather than Spark), process control
  (`pkill`/`pgrep`, the half of the legacy fence that is not a unit action), journal reading,
  and the dagur scheduled-job path, which is the one place a free command string is the feature.
  This is a programme of work rather than a task: each family needs an endpoint designed,
  validated and tested.
* ~~Catalyst/Vali internal APIs are reachable on the LAN with no auth.~~ **Resolved (2026-08-20)**:
  both now require mutual TLS against the cluster CA, with `CERT_REQUIRED`, and neither starts if its
  certificates are absent -- falling back to plain HTTP when something is already wrong would reopen
  the hole at the worst moment. The handshake runs per connection in the worker thread, so one slow
  client cannot stall the listener. Every caller moved with them, including several that were not
  obvious: `spectrum_server.py` had seven submission sites, and `vali.py` and `valcli.py` had their
  own Catalyst clients. `test_internal_api_auth.py` asserts the property rather than the change -- no
  daemon calls an internal control API over plain HTTP, both listeners demand a client certificate and
  pin the CA -- and it is what found the three callers that had been missed.
  Verified live for Catalyst end to end: plain HTTP refused, TLS without a client certificate refused
  at the handshake, and a cluster-signed certificate reaching the handler; the Spectrum container
  authenticates with its own `client.crt`.
* ~~No mTLS certificate renewal.~~ **Resolved (2026-08-20)**: `impa`
  (status/plan/renew/rollback/selftest) drives renewal and CA rotation over SSH rather than mTLS,
  deliberately -- it has to work once the certificates it repairs have expired, which is exactly when
  port 9099 is what is broken. CA rotation is a three-pass trust/present/prune ordering, asserted
  before a byte is written. Mimir now surveys both certificate directories on every node every 15
  minutes (PASS >30d, WARN <30d, FAIL <7d; an unparseable date is WARN, never PASS -- the previous
  check returned PASS when it could not parse one). Hostname verification is **on**: the certificates
  already carried an IP SAN, and provisioning now adds loopback, localhost, the hostname and the VIP,
  plus `serverAuth,clientAuth` on node certs and `clientAuth` only on `client.crt`, which sits on every
  node and was previously valid as a server certificate too. CA validity went from 3650 to 7300 days --
  **the CA and every leaf previously expired on the same day**, so a leaf renewed near that date would
  outlive its issuer and fail cluster-wide. See [docs/mtls_lifecycle.md](docs/mtls_lifecycle.md).
  **Outstanding**: the floating VIP cannot be identity-bound without regenerating certificates (any
  node may answer it), and `spectrum_phx/lib/spectrum_phx/spark.ex` is now the last client that accepts
  any cluster-signed certificate for any node.
* ~~Spectrum runs `--privileged` with `Network=host`.~~ **Resolved (2026-08-20)**: now
  `DropCapability=ALL` + `NoNewPrivileges=true`. `--privileged` was granting the most exposed component
  in the cluster the full capability set, SELinux confinement off, and podman's whole `/dev` -- a
  process in that container could open `/dev/sda` read-write, confirmed on the live node. It bought
  nothing: a container's `/dev` carries device nodes but not udev's subdirectories, so the
  `/dev/drbd/by-res/...` paths the code actually uses were never present. That, not a missing
  permission, is why image upload failed with ENOENT. Verified live -- `CapEff` is zero, `/dev/sda` is
  gone, every console endpoint still serves -- and guarded by tests in `test_deployment_manifest.py`.
  **Outstanding**: `aether` keeps `--privileged` and now documents why (it loads the DRBD module and
  drives device-mapper). Scoping it to explicit `AddDevice`/`AddCapability` grants needs an audit of
  what the Linstor satellite actually calls; guessed wrong it fails as silent storage corruption.

---

## P1 — Metadata layer (Daruk as a Medusa Store)

* ~~ZooKeeper observers cannot be promoted, so quorum is bound to three arbitrary nodes.~~
  **Resolved (2026-09-10)**: `reconfigEnabled=true` ships in all three Quadlet writers and
  `deploy_updates.py` adds it to nodes that already exist, and `cluster zk-promote` /
  `cluster zk-demote` move the vote between members in one `reconfig` -- no restart, and no instant
  at which two members disagree about who votes. The first three nodes are still the *starting* set,
  which remains right: observers scale reads without slowing writes.

  The refusals are the feature. ZooKeeper commits any membership for which a quorum of the old and of
  the new configuration exists at that instant, which includes handing a vote to a node that is not
  answering and taking three voters down to one -- both leave a cluster the next single failure
  finishes off, and both report success. So a change is refused unless *every* voter in the new set is
  answering (which also gives the new configuration its quorum), at least a quorum of the current
  voters is answering, there is exactly one leader among them, and the result still has three voters.
  An even count warns rather than refuses. The plan is submitted as `reconfig -v <version>` against
  the version `/zookeeper/config` reported, so a membership that moved in between is refused rather
  than overwritten, and the result is read back and compared instead of trusted from the client's exit
  status. `--replacing` makes a swap one operation: on a three-voter ensemble it is the only legal
  change, and it is what moves the role off a permanently-failed node -- which stays an observer, in
  the ring and in `cluster.json`. `cluster decommission --finalize` hands the departing voter's vote to
  a live observer the same way, and falls back to the old rewrite-and-roll where there is none, which
  on three nodes is always.

  **The `zoo.cfg.dynamic` interaction turned out to be the opposite of the worry.** With reconfig on,
  ZooKeeper moves the `server.N` lines into `zoo.cfg.dynamic.<version>` and points `zoo.cfg` at it --
  both in `/conf`, which is in the container: the image declares volumes for `/data`, `/datalog` and
  `/logs` and no other, and its entrypoint regenerates `zoo.cfg` from `ZOO_SERVERS` whenever the file
  is absent, which for a recreated container is every start. Persisting the dynamic file does not help
  and makes it worse: ZooKeeper names it from the path of the *static* config
  (`QuorumPeer.makeDynamicConfigFilename`), so the first committed reconfiguration writes it back
  beside `/conf/zoo.cfg` wherever `dynamicConfigFile` pointed, leaving a membership file on a volume
  that outlives the pointer naming it. So the dynamic config is deliberately left ephemeral: the pair
  is lost together and a restarted node re-derives everything from its unit rather than reading a
  stale half. **ZooKeeper owns membership while it is running; the Quadlet owns it across a restart.**
  A provisioner rewriting `zoo.cfg` is therefore not fighting ZooKeeper -- it is what rebuilds what
  ZooKeeper was running -- and what would fight it is a unit disagreeing with the live ensemble, so
  every path that reconfigures also rewrites the units from the membership it read back, without
  restarting anything. Asking for the role a member already has is a repair rather than a no-op, which
  is how a node that was down during a change is brought into line when it returns.
  `standaloneEnabled` stays `true`: with three or more participants it has no effect, and the only
  thing `false` buys is reconfiguring below two voters, which `zk-demote` refuses anyway.
  See [docs/zookeeper.md](docs/zookeeper.md#changing-which-nodes-vote); 31 tests in
  `test_zk_reconfig.py`.
  **Outstanding**: enabling reconfiguration widens what an unauthenticated client on 2181 can do --
  ZooKeeper 3.9 runs no ACL check on the `reconfig` operation, so anything that reaches the client
  port can change the ensemble. That port already takes unauthenticated writes and has
  `4lw.commands.whitelist=*`, so this widens an existing exposure rather than creating one, but it is
  another reason the 2181 boundary has to stay a network boundary. Nothing yet reports a unit that has
  drifted from the live configuration until someone runs one of these commands; `mimir` is where that
  check belongs.

* ~~Catalyst double-claims scheduled jobs.~~ **Resolved (2026-08-21)**: `claim_scheduled_run()` takes
  the tick with `IF last_run_epoch = ?` through Daruk's `/v1/schedule/claim-job` before anything is
  written or queued; a refusal or an unreachable Daruk skips the tick, because a skipped tick runs ten
  seconds later and a doubled one cannot be taken back. A latent crash went with it: the column exists
  and is null on a schedule that has never run, so `now - None` raised inside the loop's `try` and one
  such row silently cost every *other* schedule that pass.
* ~~`run_cql_query` cannot report a failed LWT.~~ **Resolved (2026-08-21)**: it now raises on a
  statement carrying an `IF` clause, before any I/O, so the class cannot come back silently. The keyword
  is matched *outside quoted literals* -- Dagur writes arbitrary job stdout into `dagur_runs`, and a raw
  text match would discard a run record because a health check printed the word "if". DDL is exempt
  (`CREATE TABLE IF NOT EXISTS` is not a compare-and-swap, and refusing it would stop the daemons
  booting). The remaining conditional statements on the text path are seed and one-time-repair
  `IF NOT EXISTS` writes where `applied: false` means "already in the desired state", plus the schema
  runner's own lock, which parses `[applied]` positionally and cannot use a typed endpoint because it
  runs before the schema exists.
* ~~Daruk silently downgrades writes from QUORUM to ONE.~~ **Resolved (2026-08-19)**: reads may
  still degrade, since stale data is recoverable; mutations, DDL and lightweight transactions now
  surface the failure. Retrying a write at ONE during a partition is how two nodes come to believe
  they own the same VM. The trigger was also narrowed from substring matching on
  "unavailable"/"timeout"/"active" to driver exception types -- "active" alone matched a wide range
  of unrelated errors. Ten classifier cases unit-tested; verified live.
* ~~No compare-and-swap on ownership.~~ **Resolved (2026-08-20)**: Daruk gained typed LWT endpoints
  (`/v1/vm/claim`, `/release`, `/set-state`, `/migrate-lock`, `/migrate-unlock`, `/migrate-commit`,
  `/create`, `/node/maintenance`) backed by prepared statements at QUORUM + SERIAL, with a fixed
  operation table so no caller text can reach a statement. Ten ownership-critical writes migrated:
  Vali now claims a VM *before* promoting its disk rather than recording placement after boot, the
  read-then-write migration lock became one Paxos round, and three reconcile loops no longer unplace a
  VM that has legitimately moved. A refused CAS returns 200 with `applied: false` and the *current*
  values, so a caller can name the actual owner -- a lost race and a failure are never collapsed.
  Verified against live Scylla, which settled three things that were not assumable: the driver renames
  `[applied]` to `applied` inside rows, `was_applied` is single-use, and **`INSERT ... JSON ? IF NOT
  EXISTS` silently ignores the condition and overwrites the row**.
  **Outstanding**: the migration lock has no holder identity or expiry, so a late cleanup from a failed
  attempt can release a *later* migration's lock. Cross-host maintenance exclusion is still
  read-then-write and an LWT cannot fix it (it spans partitions); it needs a single-row cluster lock,
  which belongs with the schema-ownership item.
* ~~Scheduled major compaction is an anti-pattern.~~ **Resolved (2026-08-19)**: the 12-hourly
  `nodetool compact` job is disabled, with a migration for existing clusters. Disabled rather than
  deleted so an operator can see it and re-enable deliberately.
* ~~Schema is scattered and unversioned.~~ **Resolved (2026-08-20)**: `helios_schema.py` holds one
  ordered migration list, recorded in `hydra.schema_migrations` behind a TTL-bounded LWT lock, with
  checksums that refuse a migration edited after it shipped. All five daemons now call
  `ensure_schema()` at startup and define no tables of their own; the lock makes concurrent starts
  safe, so none of them depends on another having run first.
  Two bugs surfaced only by running it against a real database rather than a fake. The LWT parser
  compared the whole output line to "True", which matches cqlsh's single-column form and nothing else,
  so a *successful* lock acquisition read as a lost race and the runner returned still holding the
  lock. Then the daemons turned out not to use cqlsh at all -- they proxy to Daruk, which returns row
  values joined by spaces with no column names, so both parsers had to learn that shape too. Verified
  live: dropping the ledger and restarting catalyst, vali and the console applies both migrations,
  records them, releases the lock, and leaves every seeding step working.
  **Deliberately not moved**: the `ALTER TABLE ... ADD` statements. Scylla errors when the column
  exists, so they are idempotent only because that error is swallowed at the call site; inside a
  migration, where a failed statement aborts the run, they would break every restart after the first.
  Making them migrations needs an add-column-if-absent step that consults `system_schema.columns`.
* ~~No ring lifecycle management.~~ **Resolved (2026-08-20)**: maintenance entry is gated on quorum,
  with the replication factor read from `system_schema.keyspaces` rather than assumed, and the gate
  refusing outright if it cannot be read. It runs twice -- once in the API handler and again
  immediately before the database is stopped -- because an evacuation can take an hour and the ring
  the first check saw is not the ring at stop time. Cross-host exclusion is now a lock row taken with
  `IF NOT EXISTS`, carrying a holder token so a previous holder cannot release the current one and a
  TTL so a host that dies does not wedge maintenance for the cluster. `cluster ring`,
  `cluster decommission` and `cluster rejoin` provide the preflight, the ordered plan and the
  bookkeeping. See [docs/ring_lifecycle.md](docs/ring_lifecycle.md).
  Verified live: the single-node cluster correctly refused to enter maintenance, reading RF and the
  ring from the real database.
  **Deliberately manual**: `nodetool decommission` and `removenode` stream data, run unbounded and
  cannot be re-run, so an interrupted one leaves a node neither in nor out of the ring. Automatic
  detach of an unhealthy node is also not implemented -- from a health check that has failed for
  thirty seconds, a dead node and a partitioned one are indistinguishable.
* ~~No way to grow a cluster.~~ **Resolved (2026-08-23)**: `cluster add-node` brings a provisioned,
  enrolled machine in, in the order identity -> membership -> consensus -> storage -> scheduling, and
  is resumable because a join that fails part-way leaves the node in `cluster.json` and out of the
  ring. Deliberately not `cluster create` with one more address: create claims disks, and `wipefs -a`
  against nodes already serving guests is not a recoverable mistake. `cluster decommission --finalize`
  is the counterpart and now shrinks the ZooKeeper ensemble as part of the removal. Verified live by
  taking a one-node cluster to three. See [docs/cluster.md](docs/cluster.md).

Suggested shape: keep `/query` working, add typed endpoints (`/v1/vm/claim`, `/v1/vm/migrate-lock`)
backed by prepared LWT statements, migrate invariant-critical writes first, move schema ownership into
Daruk, then gate raw CQL behind an explicit admin path (`valcli db.query` is a legitimate feature).
This composes with the Phoenix rewrite — Xandra gives prepared statements and LWT natively.

---

## P2 — Storage / DFS

* **Sidon write path, what the 2026-10-03 change did not do.** The drain now runs on its own
  thread, a write's local sync overlaps its replicas, and the drain ships extents through a
  pipeline ([docs/sidon.md](docs/sidon.md) section 2). Left, in order of value:
  * **Concurrent NBD requests and group commit -- built (2026-10-04)**, see
    [docs/dfs/group_commit.md](docs/dfs/group_commit.md). Up to 32 requests in flight per
    connection; writes in flight together share one journal sync and one replica round trip; a
    failed batch fails every write in it and the vdisk fails closed until the heal re-synchronises
    the replicas. 4 KiB writes at queue depth 16 went from 157 to about 2,275 IOPS on the test
    cluster, queue depth 1 unchanged (171 IOPS); large blocks did not move much because the lab's
    shared store is the limit there. `valcli storage.benchmark` gained the 4 KiB queue-depth-16
    line. What it left, in order of value:
    * **Overlapped commits.** One batch commits at a time. A second batch's sync overlapping the
      first's replies could roughly double depth-16 again, but needs the replica protocol to
      pipeline requests and a rule for a failure of the earlier batch while the later one is
      half-sent. The 300 microsecond wait for a burst already recovers most of it.
    * **Reads outside the vdisk lock.** They run on workers now but still one at a time per
      vdisk, because `Vdisk::read` holds the lock across the extent read. A guest at depth 16 on a
      read-heavy workload is bounded by that, not by the transport.
    * **Forwarded writes are still one at a time** (`Forwarder` and `serve_connection` are
      serial), so a guest on a node that does not yet own its vdisk sees queue depth 1.
    * **`PeerClient::call` retries once on a transport failure, including for an append.** A retry
      after a reply timeout whose first attempt had reached the replica leaves a duplicate record
      in the replica's journal, which replay refuses on takeover. Pre-existing, rare (it needs a
      reply to be lost, not a connection to be refused), unchanged by group commit; the fix is an
      append that is idempotent (a sequence the replica checks) or a no-retry append with the
      repair path as the answer.
    * **Several NBD connections to one export** are still served one at a time.
  * **The test cluster is at its storage ceiling for large blocks.** Its three virtual disks
    share one backing store (a bare 64 MiB fsynced write is about 110 MB/s, and the owner's and
    a replica's flushes contend), and each guest MiB costs four physical MiB (journal and
    extent group, on two nodes). The sustained rate (about 20 MiB/s) is within a small factor
    of that. A faster result needs fewer physical writes per guest byte -- the journal on a
    separate or faster device is the lever -- not more code on the data path.
  * **The hard ceiling is hard-wired to twice `SIDON_HIGH_WATER`.** `VdiskConfig::hard_ceiling`
    exists and is zero in production; there is no environment variable for it. The journal
    volume now has to be sized for *two* high-water marks per busy vdisk, which nothing checks.
  * **Replicas from before `OP_TRUNCATE_TO` never trim their journals.** An old replica refuses
    the opcode (safely: it keeps its whole journal), so until every node runs the new build the
    replicas' journals only grow. Nothing to do but finish the rollout; the first trim after it
    drops everything older than the live sequence number.
  * **A replica creates its journal file without syncing the directory** (`ReplicaStore::append`).
    Found while reading it, pre-existing and unchanged: the file's `sync_data` does not
    promise the directory entry, so the first acknowledged append after a power cut relies on
    the filesystem persisting it with the data. XFS and ext4 do; POSIX does not say they must.
  * **A failed batch's own records still stay in the journals, and replay may apply them.** The
    commit pipeline now fails closed (no write is appended after a failed batch until the heal has
    made the replicas' journals the owner's again), so the hazard this bullet used to describe --
    a next write carrying a failed one into replay, or a replica gaining a hole -- is gone, and the
    writes queued behind a failed batch are taken back out of the journal. What remains is
    permitted by I-2 and visible only across a restart: a write the guest was told failed can
    appear after a crash if its records and marker reached a journal. Cleaning that up would need
    a "truncate to here" request on the replica protocol.
  * **`storage.benchmark` leaves garbage on a node with Purah's sweep off** (the test cluster
    runs `SIDON_PURAH_INTERVAL=0`). Each run leaves roughly 250 MiB of extent groups per copy
    until `purah-sweep` has seen them unreferenced twice.
* **The single-copy vdisks, and what is still owed on the live cluster (2026-10-03).** The code
  paths are closed; the cluster is not, because nothing was deployed. Both live vdisks are in a
  container called `default` (not a row) and hold one copy, and `cluster.json` says
  `redundancy_factor: 0` on three nodes. In this order, by hand: `cluster add-node --node <member>
  -r 1` (writes the factor; see [docs/cluster.md](docs/cluster.md)), restart sidon on each node
  one at a time (it reads `cluster.json` at start, and the binary change below needs the restart
  anyway), then `valcli storage.replicate --all`. Closed in code: every create names a container
  that exists (`Spark.dfs_create/4` raises without one; `/api/vms/update` add-disk passes one;
  Sidon's own fallback is now `default-pool`), `add-node` warns and takes `-r`, the replica
  store tallies the reads it serves, the console shows requested against held against policy,
  and `dfs_egroup_replicas` is dropped (migration `0025`).
* **A replica's `replica-egroups/` files are never reclaimed.** Purah sweeps the groups
  `dfs_egroups` records for *this* node, in `egroups/`. A copy pushed to a peer lands in that
  peer's `replica-egroups/`, which nothing lists, so when the owner reclaims a group the peers
  keep their copies for good. Found while deciding what to do with `dfs_egroup_replicas`; not
  fixed because the right sweep is a replica-side one that marks from `dfs_block_map` (I-7),
  which wants a design, not a patch.
* **`purah-heat` ranks only groups this node created.** The reads a replica now serves are
  tallied and flushed to `dfs_egroup_access`, but the ranking is scoped to the node's own
  `dfs_egroups` inventory, so a group held only as a replica does not appear in that node's
  ranking; it surfaces through the owner's merged rows. Fine until tiering acts per node.
* **The default container is spelled in more places than the three a test holds equal.**
  `vali.py` and `valcli.py` each carry their own `"default-pool"` fallback instead of reading
  `helios_sidon.DEFAULT_CONTAINER`, and Daruk's `vdisk-create` allow-list still defaults the
  column to `"default"` (Sidon always sends one now, so it is unreachable from Sidon).

* ~~191 replicated volumes, cluster-wide, whatever the node count.~~ **Resolved
  (2026-08-22)**: the ceiling was LINSTOR's `TcpPortAutoRange` default of `7700-7890` -- one
  port per DRBD resource, so about nine VMs per host at twenty nodes, surfacing as a confusing
  LINSTOR error at create time rather than as "out of capacity". Widening the range was the
  interim measure and was never needed: the underlying problem was that DRBD replicates
  *devices*, so each volume cost a kernel object, its own threads and RF-1 standing connections
  per node. Sidon replicates extents and holds one connection per node **pair**, whatever the
  disk count. See [docs/sidon.md](docs/sidon.md).
* **`hydra` uses SimpleStrategy, so all three replicas can land in one rack.** Replication factor is
  `min(3, node_count)`, which is correct, but `SimpleStrategy` places replicas by token order alone and
  knows nothing about racks or datacentres. On any cluster spanning more than one rack a single rack
  loss can take the whole metadata layer with it -- and the metadata layer is what makes the
  surviving extent groups identifiable. An extent group without the block map is four megabytes
  of unlabelled bytes, so this is not merely a control-plane outage.
  `NetworkTopologyStrategy` with a rack-aware snitch is the standard answer. This wants deciding
  *before* anyone racks a second cabinet: changing the strategy later requires a full repair, and until
  that repair completes the cluster is running with replicas it believes exist and does not.

* ~~Mipha's HA failover write is unconditional.~~ **Resolved (2026-08-21)**: placement is released
  through `/v1/vm/release` conditioned on the dead host, so a VM already recovered elsewhere is skipped
  rather than clobbered -- and skipped means *not restarted*, which is the point.
  A worse defect was found beside it: `UPDATE hydra.nodes SET status = 'DOWN' WHERE ip = ?` is
  **rejected outright by Scylla** (`ip` is not the partition key) and the return code was never read, so
  a host that died had never once been marked down and Vali kept scheduling onto it. It now goes through
  `/v1/node/maintenance` keyed on `hostname` and conditioned on the status that pass read.
* ~~VM delete can orphan a running guest.~~ **Resolved (2026-08-21)**: the row is read first (so "no
  such VM" is decided before any conditional write -- `UPDATE ... IF status != ?` *applies* against a
  nonexistent row and would invent a stub), then the migration lock is taken, the placement re-read
  under it, and the state set with `IF host_ip = ?`. Every refusal restores the state, releases the lock
  conditionally, and returns the row intact naming the current host. Verified live: a delete against a
  stale placement was refused by a real Scylla LWT with the row surviving and nothing destroyed.
* ~~Lanayru clears the migration lock it never held.~~ **Resolved (2026-08-21)**: the guest is
  recorded through `/v1/vm/set-state` conditioned on the host, which never names `status` at all.
  Separately, its registration `INSERT` named six columns `hydra.vms` does not have, so Scylla rejected
  it and **no Lanayru control node had ever been recorded**; it now uses `/v1/vm/create` with the real
  columns, moved ahead of the Linstor resource so a name collision costs a refused deployment rather
  than another VM's disk.
* ~~`/api/cluster/metrics` scans the whole metrics table on every poll.~~ **Resolved (2026-08-21)**:
  one bounded `WHERE node_ip = ? LIMIT 40` per node, answered directly by the `timestamp DESC`
  clustering order, with a `metrics_unavailable` key so a node nobody could ask is not drawn as a node
  that reported nothing. `dagur_runs` is read one partition per job and merged newest-first. Verified
  live: `logos_metrics` held 2,879 rows for a single node and the endpoint now returns 40 from one
  statement; `/api/dagur/runs` previously returned 100 rows of which 61 were one job.
* ~~`GET /api/images` writes.~~ **Resolved (2026-08-21)**: the directory scan and its `INSERT` are
  gone, columns are named rather than `*`, and an unreadable catalogue answers 503 instead of an empty
  list. The reconciliation was dropped rather than moved to a job: upload writes a DRBD device, not that
  directory, so the scan only ever caught files nobody registered and recorded them with a `path` no
  LINSTOR resource backed -- and the container sees only its own host's volumes, so two nodes disagreed
  about the cluster catalogue.
* ~~`/api/images/delete` always answers 200.~~ **Resolved (2026-08-21)**: the backing store is removed
  first and checked, and only then the row; a failure returns 500 with the row intact. A `/dev/drbd`
  path is never `rm`'d -- that deletes a udev symlink and leaves the resource holding storage on every
  node -- it goes through `resource-definition delete`, with "already gone" tolerated. Paths outside the
  allowed roots are refused, and an unknown image is 404 rather than 200.
* **`catalyst_tasks` has no time-ordered clustering key** (`task_id` is the whole primary key), so
  "the most recent N tasks" is not answerable server-side and every read is a full scan.
  **Mitigated, and the rest declined deliberately (2026-08-21)**: migration `0003-bound-task-history`
  sets a 30-day TTL, so the scan no longer walks a table that grows forever.
  The remaining half -- a companion table keyed by time bucket, dual-written by Catalyst -- is **not**
  being built. With the retention window the table holds a few thousand rows and the console caps its
  render at 200; a dual-write would trade a bounded scan for a new failure mode where a task exists in
  one table and not the other, and a reader that has to reconcile them. That is a worse system for this
  volume. Revisit if task rates make the scan measurable, which is the condition that would justify it.
* ~~`mimir_results` accumulates duplicate rows.~~ **Resolved (2026-08-20)**: results are stored under
  the check's *own* category from `CHECK_ID_TO_FUNC`, not the category that was invoked, so a check
  always lands in the same row however it is run and a re-run updates rather than duplicates. This also
  makes the column mean what its name says -- grouping by it after a `run_all` previously yielded one
  bucket, which is why the old console carried a hardcoded check-name list that had drifted from this
  map. Legacy partitions are shed on the next run, discriminated by real categories containing a dot
  where invocation scopes do not, with a guard so a future dotted scope cannot delete live rows. A test
  asserts every check the runner reports has a category -- it found eight that did not, each of which
  would have kept duplicating. Verified live: the `all` partition is gone and no check name appears in
  more than one partition.
* ~~No maintenance-mode quorum gate.~~ **Resolved (2026-08-20)** as part of the ring lifecycle work
  above -- the gate reads the replication factor from `system_schema.keyspaces`, refuses if it cannot
  read it, and runs again immediately before the database is stopped rather than only at API entry.
* ~~No out-of-band fencing path.~~ **Resolved (2026-08-21)**: `mipha.py` runs a four-rung fence
  ladder -- self, spark, BMC, storage -- and **every rung reads back the state it claims to have
  produced**. "Could not tell" is recorded as failure. An unconfirmed fence marks the host `DOWN` (so
  Vali stops placing there) but releases nothing and restarts nothing, and the Catalyst task is created
  *before* the fence so the refusal is visible; the next pass retries, so failover resumes by itself
  once an operator acts.
  Three bugs went with it: `ssh_fence_host` sent a shell string whose every clause ended in `|| true`,
  the caller discarded that status anyway, and the fence only ran `if ping_ok` -- so a host that had
  gone silent, the exact case fencing exists for, was assumed dead on no evidence.
  BMC credentials live in `/etc/hci/fencing.json` (0600, root, every host), **not** in ScyllaDB: a
  fence is needed precisely when things have failed and the database is often part of what failed, and
  anything in `hydra` is readable by the web tier. The password goes through `IPMI_PASSWORD`, never
  argv, since `/proc/*/cmdline` is world-readable. Provisioning now creates the file and installs
  `ipmitool`.
  Storage fencing rests on **DRBD quorum**, which the failed host's own kernel enforces without its
  userspace. Three simpler approaches were tried and rejected as ineffective -- disconnecting on the
  survivors leaves the old Primary writing locally, `linstor resource delete` needs the satellite on
  the dead node, and a local promotion proves nothing across a connection that no longer exists. This
  needed a new endpoint: `drbdsetup status --json` reports `"quorum": true` both when a majority is
  held and when quorum is off entirely, verified live.
  **Residual unsafe cases are enumerated in [docs/fencing.md](docs/fencing.md) §8** rather than papered
  over -- chiefly: no BMC plus unarmed quorum plus an unreachable host confirms nothing (default is to
  refuse the failover, an availability failure, not a safety one), and two-node clusters have no
  storage fence at all because there is no third vote.
* ~~No automated self-fencing on partial failure.~~ **Resolved (2026-08-21)**: a watchdog on *every*
  host probes libvirt, the DRBD control plane and per-resource serviceability. Each probe returns
  ok/failed/**unknown**, and unknown never escalates to the destructive tier.
  The distinction that matters: libvirt dying is **quarantine only** (`DEGRADED`, which Vali's existing
  `status != NORMAL` filter already excludes) because qemu keeps running when libvirtd dies --
  destroying working guests would be a self-inflicted outage and failing them over while they still
  write would be the corruption. Only a Primary that genuinely cannot serve I/O fences. A failed disk
  *with* a healthy peer deliberately does not trigger: DRBD 9 goes diskless-client and the guest never
  notices.
  Anti-flap: three consecutive passes, one good pass resets, 180s startup grace, maintenance exempt,
  never on a single-node cluster. A fence that did not fully take publishes `DEGRADED`, not `FENCED` --
  only a verified fence may claim the status that makes the leader skip its own ladder.
  Verified live where a single node allows it, including that a `systemctl is-active libvirtd` probe
  would have false-positived on Rocky 10, which uses `virtqemud`.
* ~~No backup / disaster recovery.~~ **Resolved (2026-08-21)**: `saga` captures the `hydra` keyspace
  whole, the LINSTOR controller database (on the controller node only) and `/etc/hci`, with the cluster
  CA opt-in. ZooKeeper state is deliberately **not** captured -- `/helios/nodes/*` is ephemeral and
  republished in seconds, and restoring a stale `stopped` would hold down a cluster being brought up.
  A target on the same filesystem as the database is refused: a backup stored on the disk it protects
  is not a backup. A missing target directory is refused rather than created, because an unmounted
  mount point looks exactly like a missing directory. Snapshots are cleared in a `finally` on every
  path -- a snapshot is hardlinks, so it costs nothing and then costs everything by pinning SSTables
  against compaction.
  The restore was **demonstrated, not just documented**: three rows, drop the table, recreate it (new
  uuid, stale directory left on disk), restore, rows return. Saga resolves the live directory through
  `system_schema.tables.id`; globbing `<table>-*` would copy into a directory Scylla has forgotten and
  report success while the data never appears.
  Retention keeps N *healthy* artefacts -- three bad nights must not evict the last good backup -- and
  a node only prunes its own. The nightly schedule ships **enabled** even though a fresh cluster has no
  target, so it fails once a day with a message naming the fix; a disabled schedule is silent, which is
  what "no backup/DR" looked like.
  **Explicitly not covered**: guest data inside DRBD volumes. DRBD protects against a host failing and
  nothing else -- a synchronous replica of a corrupted block is a corrupted block.
* ~~Live migration still passes `--unsafe`.~~ **Resolved (2026-08-19)**: removed. It was only needed
  while VM disks carried `--allow-two-primaries` permanently; that window is now scoped to the
  migration itself, so libvirt's coherence check is the one we want.
---

## P2 — Networking

* ~~Urbosa leaks every resource it creates.~~ **Resolved (2026-08-21)**: reclamation is split into
  observation, a pure plan, and execution, with `urbosa --reclaim` reporting and removing nothing until
  `--apply`. Refusals are as much the output as removals: a bridge with a guest tap, a namespace holding
  something Urbosa did not create, or anything unreadable counts as busy. The prerequisite was that a
  failed desired-state read used to return an empty list -- which would have made the collector delete
  the entire overlay on a database blip.
  The real inventory was larger than recorded: the router-level and per-segment `dnsmasq` instances were
  also never reclaimed, and a segment re-attached to a different T1 stayed wired to the old router
  permanently. Verified live against a built orphan set, with a live bridge correctly refused.
* ~~Transit /30 allocation collides.~~ **Resolved (2026-08-21)**: allocations are recorded in
  `hydra.urbosa_transit_pool` (migration `0004`) and claimed with `IF NOT EXISTS` keyed on the slot, so
  two routers racing for one subnet resolve rather than both taking it. The hashed value is kept as the
  *preferred* slot, so an upgrading cluster keeps its current addressing wherever that slot is free and
  only the colliding minority move. Fail-closed while the migration is absent: transit links are left
  untouched rather than falling back to the colliding hash.
* ~~VXLAN head-end replication overhead.~~ **Assessed and kept, deliberately (2026-08-21)**, with the
  reasoning in `docs/urbosa.md` §5. Multicast is the only change that removes the replication and needs
  IGMP snooping and PIM on the physical fabric -- unavailable in the target environments, and
  unavailable is not a trade-off but an overlay that does not pass traffic. The fix worth having removes
  the *cause* (flooding as discovery) via EVPN with ARP suppression, which is already scoped as the
  Scale-Out Urbosa add-on.
  One claim here was **wrong and is withdrawn**: FDB flood entries do not accumulate, because
  `bridge fdb append` is idempotent for a given (MAC, dst) pair -- verified on the live node. The real
  leak was *stale* entries for hosts removed from the cluster, and that is fixed.
* ~~Bifrost split-brain fallback.~~ **Resolved (2026-08-19)**: when ZooKeeper names a leader that is
  not serving, Bifrost no longer elects a replacement by sort order -- a second election that can
  disagree with the ensemble's, and in a partition each side would pick the lowest candidate it can
  see. It releases the VIP instead: briefly unreachable is visible and recoverable, duplicated is not.
* ~~A VLAN id has no uniqueness constraint.~~ **Resolved (2026-09-10)**: `hydra.gatoway_vlan_claims`
  (migration `0009`), keyed by the VLAN id and claimed with `IF NOT EXISTS`, so two creates racing for
  VLAN 100 resolve rather than both taking it. Wired into create, delete and re-tag in both consoles;
  the read-then-refuse check stays in front of it because it gives the better message. Written up under
  the Phoenix rewrite below and in `docs/gatoway.md` §2C.
* **A VNI has no uniqueness constraint either, and no check at all.** `hydra.urbosa_segments` is keyed
  by `segment_id`, so nothing stops two overlay segments declaring VNI 5001 -- and unlike the VLAN case
  there is not even an advisory read: `/api/urbosa/segments/create` validates the CIDR, the gateway and
  the DHCP range and then inserts whatever VNI it was handed. Two segments on one VNI put their frames
  on the same VXLAN interface, which is the overlay's version of the broadcast-domain merge the VLAN
  claim now prevents. The fix is the same shape and can reuse it directly: a claim table keyed by
  `vni`, taken before the segment row and released with it. It is a bigger job than the VLAN one only
  because the segment create goes through a Catalyst task rather than writing inline, so the claim and
  the release have to be part of what the task does -- the console returns before the row exists.
---

## P3 — Code health

* ~~`mcli-runner`'s certificate check returns PASS when it cannot parse an expiry date.~~
  **Resolved (2026-08-20)**: both certificate checks now go through Mimir's survey rather than a second
  implementation, so there is one parser and one verdict. An unreadable date is WARN -- "I could not
  check this" and "this is fine" are different answers, and the old code gave the second for both. It
  also looked only at `client.crt`, ignoring the node and CA certificates in `/etc/hci/spark/certs`.
  The sibling ingress check had the same defect and the same locale-dependent `strptime`, and was fixed
  with it. Verified live, which caught a second bug in the fix: `mimir` is deployed as
  `/usr/local/bin/mimir` with no `.py` suffix, and `spec_from_file_location` returns `None` for an
  extensionless path, so the loader needs an explicit `SourceFileLoader`.
* ~~`hydra.vali_tasks` is a dead table.~~ **Corrected and documented (2026-08-20)**: nothing *writes*
  it, but it is not unreferenced -- `valcli`'s cleanup reads and deletes from it and `mcli` checks it
  exists, so dropping it would break both. They simply always find it empty. `docs/vali.md` and the
  master architecture guide both described it as the live task queue, which was the actual harm; both
  now say what it is, and record that the real queue is Catalyst's in-process `queue.Queue` and does
  not survive a restart, so a task accepted and not yet run is lost rather than resumed.
* ~~`vali.evacuate_host_thread` is dead code.~~ **Resolved (2026-08-20)**: removed. It was a complete
  second copy of the maintenance-enter sequence, including the unconditional database stop the quorum
  gate now prevents -- a way back into the bug for anyone who wired it up. Deleted rather than left
  gated, because two implementations of one sequence drift.
* **`run_cql_query` is copy-pasted into at least six files** (~40 lines each, including the cqlsh
  fallback). This is why the CQL-injection items had to be fixed at each call site — there is no single
  query layer to parameterize. The Daruk work above is the structural fix.
* **`spectrum_server.py` is two if/elif chains** — 7,300+ lines, 95 API paths, `do_GET` ~1,800 lines and
  `do_POST` ~3,150 lines, with routing, auth, validation, and shell-outs interleaved.
* ~~13 unsupervised background threads in Spectrum.~~ **Resolved (2026-08-21)**: the three
  long-running loops now start under `supervise()`, which restarts a loop that raises, with exponential
  backoff capped so a permanently-failing loop is not a restart storm, and which leaves a loop that
  *returns* alone because returning is a decision to stop. The failure this addresses is not a crash: a
  bare daemon thread that raises prints a traceback nobody tails and then stops existing, the process
  keeps serving, and reconciliation or metrics collection is silently gone. The remaining
  `threading.Thread` calls are per-request workers with a caller waiting on the result, which is a
  different thing; a test asserts `main()` starts none of them bare.
  Two dead scheduler loops went with it. `mimir_scheduler_loop` and `dagur_scheduler_loop` had zero
  references but were still 75 lines carrying the blind `last_run_epoch` write -- a way back into the
  double-submission bug for anyone who re-wired them. Deleted rather than left commented.
* ~~`check_updates.py` can report "update available" forever.~~ **Resolved (2026-08-21)**: an
  unreadable current version returns `None` rather than silently leaving the fallback build in place,
  and `None` is treated as *not comparable* -- `update_available` is false and the reason is recorded
  where the console shows it. Same rule per component: `"N/A"` means the node could not be asked and is
  excluded, while `"Not Installed"` and `"Unknown"` are real answers and still compared.
* ~~Proposed Mimir/mcli diagnostic checks not implemented.~~ **Resolved (2026-08-21)**: four
  implemented (`watchdog_daemon_status`, `linstor_latency_check`, `drs_storage_capacity_check`,
  `migration_lock_status`); four already existed; one declined with reasoning recorded in
  `audit_findings.md` §13 A4 -- `fencing_access_check` inspects files that never existed and would FAIL
  permanently on every cluster, and the fencing mechanism it assumed has since been built differently.
  Two of them immediately found real faults. `drs_storage_capacity_check` FAILs on the reference
  cluster because `vali.get_linstor_free_space()` returned a hardcoded 999999 MiB against a real 306951
  -- **the migration storage gate refused nothing, on every cluster, for as long as it existed**. Both
  it and `get_vm_disk_size()` now return unknown, and the gate refuses on unknown. And
  `stuck_tasks_check` had answered PASS everywhere it ever ran: `created_at` is a CQL timestamp, `int()`
  raised on every row, and a bare `except` swallowed it. It now finds three genuinely stuck tasks.
  A third pre-existing defect surfaced: `hylia_status` is built as `results[f"{svc}_status"]`, which the
  guarding test's regex could not see, so it had no category entry -- every run wrote it to the invoked
  scope's partition and the legacy cleanup deleted it seconds later, in the same run. It has never
  appeared in either console.
* ~~Air-gap / private registry hardcoding.~~ **Resolved (2026-08-21)**: every third-party image lives
  in one `IMAGES` catalogue and is resolved through `--registry` / `HELIOS_REGISTRY`, which replaces the
  *registry host* and keeps the repository path and tag -- what a mirror, skopeo or a pull-through cache
  assumes. `Dockerfile` takes a `BASE_IMAGE` build arg, matching what `spectrum_phx/Dockerfile` already
  did. `deploy_updates.py` carries a second copy of three Quadlet bodies, so it has the same resolver
  and a test asserts the two catalogues cannot drift -- an update writing a different image than
  provisioning did would silently downgrade a service. That test also caught the Linstor controller
  resolving under the `aether` key: the same image today, so it worked by coincidence and would have
  broken the moment either moved.
## Missing tooling / process

* **Only `deploy_updates.py` ever writes `/etc/hci/zookeeper/logback.xml`.** `provision.py`'s
  Quadlet mounts that path into the container and nothing in the provisioning path creates it, so a
  cluster that has been provisioned and never had a rollout run against it bind-mounts a file that
  does not exist -- podman creates a directory there, and the quietened logging config that mount
  exists to install is not what the container reads. `cluster create` and `spark_daemon_decoded.py`
  rewrite the unit without the mount at all, so the three writers disagree about it, which is the
  shape of divergence `test_zk_probe_storm.py` was written to catch and does not: it asserts the
  string appears in each file, not that the file it names is ever written. Noted 2026-09-10 while
  adding `reconfigEnabled` to the same units. The fix is for provisioning to ship the config the way
  the rollout does -- an embedded payload in `provision.py` and a `sync_provision.py` mapping entry --
  after which all three writers can mount it.

* ~~No top-level `LICENSE` file.~~ **Resolved**: Business Source License 1.1, converting to
  MPL-2.0 on 2030-08-19. MPL rather than Apache-2.0 because the BSL covenants require a
  GPLv2-compatible Change License, and Apache-2.0 is compatible with GPLv3 but not GPLv2.
  Third-party components itemised in [THIRD_PARTY_LICENSES.md](./THIRD_PARTY_LICENSES.md).
* ~~Vendored frontend libraries lack bundled attribution.~~ **Resolved**: noVNC's MPL-2.0 text and
  pako's MIT text are now bundled. Noted while doing so: `static/spice-html5/` is 2.4 MB of LGPL-3.0
  that **no served page loads** — only `src/lz_decompress.c` is used, compiled to WebAssembly during
  the image build. Removing the unreferenced JavaScript would shrink the copyleft surface to that one
  file.
* ~~No CI/CD.~~ **Resolved**: `.github/workflows/ci.yml` byte-compiles every Python module, runs
  `test_hylia.py` and `test_deployment_manifest.py`, builds and tests the Elixir app pinned to the
  release image's toolchain (1.17.3/OTP 27.1.2) with `mix hex.audit`, checks `agahnim`, and builds the
  Spectrum container image — the only step that evaluates `runtime.exs`.
* ~~No `requirements.txt`.~~ **Resolved**: paramiko and cassandra-driver pinned.
* ~~No regression test for the deployment manifest.~~ **Resolved**: `test_deployment_manifest.py`
  asserts `*_B64`/mapping coverage in both directions, upgrade-package vs LCM-inventory parity, that
  every embedded `*_script` literal compiles, and that no embedded source carries CRLF. That last
  assertion failed on its first run and caught 17 files that had drifted back to CRLF.
  **Extended 2026-08-22** with the fifth and sixth hand-maintained inventories in this
  path: the Spectrum image's build context against the Dockerfile's own `COPY` lines, and
  the Rust crates the rollout builds against the `Cargo.toml` files in the tree. Both
  found live breakage — see the two entries below.
* ~~Nothing checked that a name a component reads is bound anywhere.~~ **Resolved
  (2026-08-22)**: `test_unbound_names.py`. Removing a feature means cutting a region out
  of a file, and a region has two ends, so a cut sized to the feature routinely takes a
  definition that outlived it. None of that is visible at import — Python resolves a
  global when the line runs — so the file parses, imports, deploys, starts, serves every
  request that does not touch the missing name, and raises `NameError` on the one that
  does. It found six: `spark_daemon_decoded.py` had lost the allowlists behind four
  endpoints and the assignment feeding `host/capabilities`' own return value; `mipha.py`
  had lost `LOCAL_IP`, which `spark_endpoint` reads, so every mTLS call it made would
  have raised on the first fence; and `valcli.py` called three functions that exist in no
  module.

  The checker is deliberately over-permissive: a conditional assignment counts as a
  binding. It cannot prove a name is *always* bound; it proves a name is bound **nowhere**,
  which is what this class of edit actually produces.
* ~~The rollout could not ship the storage daemon.~~ **Resolved (2026-08-22)**: only
  `agahnim` was built on the node, so `sidon` could be changed in the repository and reach
  a running cluster by no route except reprovisioning — the one component where that is
  the least acceptable answer. All three crates now go, with every `.rs` under `src/`
  rather than a named list, and a `sidon` that fails to build stops that node's update
  instead of leaving the old one running behind a rollout that reported success.
* ~~The Spectrum image had silently stopped being rebuilt.~~ **Resolved (2026-08-22)**:
  its Dockerfile gained `COPY lanayru.py` and `COPY helios_sidon.py`; the upload list
  gained neither, so `podman build` failed with "no such file or directory" on every
  rollout — and the script printed that, continued, restarted spectrum onto the image
  already running, and reported success. A build failure is now fatal.
* ~~`spark status` reported the storage daemon DOWN on a healthy node.~~ **Fixed
  (2026-08-22).** It probed TCP 3366 for Sidon -- the LINSTOR *satellite* port, belonging
  to the thing Sidon replaced. Nothing has listened there since, so the check could only
  ever fail. Sidon has no client-facing TCP port at all, so the probe is now its control
  socket, and it sends a ping rather than only connecting: a socket file outlives the
  process that made it.
* ~~Eleven native services reported no PID, and could read as FLAPPING.~~ **Fixed
  (2026-08-22).** spark-daemon called `spark-daemon`, `bifrost`, `dagur`, `mimir`, `vali`,
  `catalyst`, `hylia`, `gatoway`, `logos`, `mipha` and `agahnim` "containerized" and asked
  `podman top systemd-<name>` about each, which fails -- they are native systemd units.
  Empty PIDs are not only cosmetic: a unit with no PID and NRestarts at or above the flap
  threshold is reported FLAPPING, so a healthy service that had restarted a few times read
  as crash-looping. The split is now derived from one list of the four real containers.
* ~~`catcli list` crashed on every row that existed.~~ **Fixed (2026-08-22).**
  `created_at` is a CQL `timestamp` and Daruk serialises it as an ISO-8601 string; the
  code divided it by 1000 assuming epoch milliseconds and raised `TypeError`. It had only
  ever been exercised against an empty task table. Both forms are accepted now, because
  `log_catalyst_task` genuinely writes both depending on the path.
* ~~No VM could be started at all.~~ **Fixed (2026-08-22).** spark-daemon's service
  inventory still listed `aether` after the unit was removed, so every node reported
  `Aether: DOWN` forever -- and `vali.select_best_start_host()` skips any host where
  *all* services are not UP. Creates worked; starts refused with "No active hypervisor
  host has sufficient memory" on a host with 9 GB free, because the loop passes over an
  ineligible host and the caller's only message is about memory. The symptom named the
  wrong subsystem entirely. `test_service_inventory.py` now compares the inventory
  against the units the toolkit installs, in both directions, and refuses any name the
  toolkit removes.
* ~~The Phoenix console was deployed by hand.~~ **Resolved (2026-08-22)**:
  `deploy_updates.py` tars its build context, builds the image, installs the Quadlet from
  `spectrum_phx/quadlet/` rather than a duplicated string, and restarts the unit.
  `SECRET_KEY_BASE` is read from whichever node already has one and reused, because it
  must be identical cluster-wide — with per-node secrets, Slate moving a request to a
  different backend logs the operator out — and regenerating it on each rollout would do
  that to every live session.
* ~~The signed upgrade package cannot carry a binary.~~ **Resolved for the Rust services
  (2026-08-22): it carries sources and hylia builds them.** Signing a tarball of Rust a
  reader can audit is a claim about the code; signing an ELF is a claim about whoever's
  machine produced it. The cost is stated rather than hidden -- every node needs a
  toolchain and the build takes minutes -- and the ordering makes it survivable: nothing
  touches the live binary until the new one has compiled, so a failure is a node that did
  not update rather than a node without storage.

  Reproducibility is the whole argument, so it is pinned and tested: entries sorted, mtime
  and ownership zeroed, CRLF normalised, and no filename in the gzip header. That last one
  was a real defect the test caught and an ad-hoc check missed, because building to the
  same path twice hides it. `Cargo.lock` is committed for all three crates and hylia
  builds `--locked`; without it the signature would cover this repository's code and not
  the two hundred-odd crates compiled in beside it.

  **Still open, and different in kind: the console's container image.** It pulls base
  images from a public registry at build time, so putting it in a signed package means
  either vendoring those bases or admitting the build is not hermetic.
* ~~`provision.py` does not know about the Phoenix console.~~ **Done (2026-10-03):** provisioning now
  installs the Quadlet, builds the image and writes the environment file on every node, and `cluster
  create` starts it in Phase 6 (it is a `MANAGED_SERVICES` entry). The history below is why the secret
  is decided where it is. *Half done (2026-08-22).*
  Provisioning now decides the one thing only it can: `SECRET_KEY_BASE`, generated once
  per cluster and written identically to every node. It has to be the same everywhere --
  a session cookie signed on one node must verify on the others, or Slate routing to a
  different backend logs the operator out -- and rotating it later invalidates every live
  session, so cluster creation is the only moment to choose it.

  Provisioning deliberately does **not** install the unit or build the image. The Quadlet
  carries `ConditionPathExists` on that env file, so it stays cleanly inactive until the
  first `deploy_updates.py` run builds the image; installing a unit whose `Pull=never`
  image does not exist yet would give a fresh cluster a start-failure loop instead.
* ~~`podman build` cannot be run from a clean checkout.~~ **Resolved (2026-08-20)**: the Dockerfile
  copies `spectrum_server.py` and renames it on the way in, so the build works from the tree as checked
  out. The rename indirection is gone rather than worked around -- it had four touchpoints
  (`provision.py`, `deploy_updates.py` twice, and `hylia.py`'s rebuild), all staging the file under the
  other name. The in-image layout is unchanged, so `CMD` and every path inside the container stay as
  they were. Verified by building the image from a clean checkout on the test node.
## Design / future work

### Phoenix LiveView rewrite of Spectrum

A strangler migration, documented in [docs/spectrum_phx.md](docs/spectrum_phx.md).
`spectrum-phx` runs beside the Python tier on port 8444.

**Every page is now served by Phoenix (2026-09-10).** Slate routes the pages to 8444 and
everything else -- the whole HTTP API, the guest console, Spectrum's own assets -- to 8443.
The `:legacy` machinery in the navigation table stays, because the routing split it belongs
to is still carrying those; what changed is that no page points at the Python tier.

**Still on the Python tier, and the reason each is:**

* **The guest console** (`/vnc_auto.html`). Needs the WebAssembly/SPICE work below.
* **The whole HTTP API.** Nothing in the console calls it any more, but `mcli`, `valcli`
  and the deployment tooling do.

* ~~**Five controls**, deliberately not wired to buttons in the rebuilt pages.~~
  **Resolved (2026-09-10): all five are Catalyst tasks.** Starting an upgrade and loading a
  package (LCM), deploying and destroying Kubernetes (Lanayru), and building or tearing
  down the overlay (Settings) are wired, and each runs where a worker can report on it.

  Submission is one path, `SpectrumPhx.Catalyst`, posting to the ZooKeeper leader over
  mutual TLS -- Catalyst's queues are `queue.Queue` objects inside that process, so a
  console that wrote the `catalyst_tasks` row itself would produce a task that is listed,
  never runs and never fails. The three files that have to agree on which queues exist and
  which are drained (`catalyst.py`, the daemons that poll them, and the console's service
  list) are asserted against each other by `test_console_tasks.py`.

  Four of them are `dagur`/`execute` -- a command on the leader, its exit code the verdict:
  `urbosa-bootstrap`, `urbosa-bootstrap --cleanup`, `hylia --load-package` and
  `hylia --start-upgrade`. Kubernetes has its own `lanayru` queue drained by the console
  backend under `supervise()`, because `lanayru.py`'s workers import half of
  `spectrum_server.py` and cannot be a host CLI without moving all of it first.

  Three things this turned up on the way:

  * `dagur` sent spark-daemon no `timeout`, and an absent `timeout` is not "no limit" --
    the daemon applies 45 seconds and kills the command. Every scheduled job the cluster
    has ever run was capped there, and one that exceeded it was recorded FAILED with a
    timeout from a daemon the caller never mentioned. The limit now travels with the task.
  * `Settings.read_only` was merged over a seed that did not contain `urbosa_enabled`, and
    the reduce only accepts a key the seed already has -- so the settings page reported the
    overlay as disabled on every render, whatever the row said.
  * `/api/settings/update` wrote the `urbosa_enabled` row, tried to submit the bootstrap,
    printed the failure to a log and answered 200. `set_urbosa_enabled/2` puts the row back
    and reports the failure, because a row saying the overlay is up with no bootstrap
    behind it is what Lanayru's pre-flight and every deploy then read.

  Documented in [docs/spectrum_phx.md](docs/spectrum_phx.md) §7e, with the per-component
  halves in [docs/catalyst.md](docs/catalyst.md), [docs/dagur.md](docs/dagur.md),
  [docs/hylia.md](docs/hylia.md), [docs/lanayru.md](docs/lanayru.md) and
  [docs/spark_api.md](docs/spark_api.md).

  **Left alone:** the Python tier's `/api/lanayru/deploy` and `/api/lanayru/destroy` still
  spawn a thread on whichever node served the request. They are the old console's path, and
  they are why the queue worker reuses Catalyst's task id rather than minting one -- the
  two paths write the same table.

**A VLAN id now has a uniqueness constraint (2026-09-10).** `hydra.gatoway_networks` is
keyed by `net_id`, so nothing in that table stopped two networks claiming VLAN 100. Both
consoles read the existing networks and refused a duplicate, which catches the mistake an
operator actually makes and cannot serialise against a concurrent create: a read followed
by a write is two operations, and two creates a millisecond apart both read "VLAN 100 is
free". Gatoway builds one `br-vlan-100` either way, so the guests of both networks end up
in the same broadcast domain and neither operator is told.

`hydra.gatoway_vlan_claims` (migration `0009-vlan-claims`) is keyed by the VLAN id, which
is the only thing two racing creates share and therefore the only thing an `IF NOT EXISTS`
can serialise them on. The advisory check stays -- it gives the better message -- and the
claim is the backstop behind it. Every path that assigns a VLAN takes it and every path
that gives one up releases it: create, delete and re-tag, in both consoles, through
Daruk's `/v1/network/claim-vlan`, `release-vlan` and `reclaim-vlan` on the Python side and
`Hydra.apply_lwt_row/3` on the Phoenix side. Order is the part that matters: the claim is
taken before the network row is written and released after it is removed, because the
other way round reopens the window it exists to close. A claim left behind by a create
that could not finish is given back on the failure path, and one left by a create that
died outright is taken over by the next create of that VLAN once it is older than five
minutes -- a VLAN nothing can ever use again is a worse failure than the duplicate. The
migration claims the VLANs a cluster already has and reports any duplicates by name
rather than failing or fixing them itself. Documented in
[docs/gatoway.md](docs/gatoway.md#c-vlan-uniqueness).

That work turned up one thing that had never fired: four daemons handed `ensure_schema`
the *guarded* `run_cql_query`, which refuses the conditional statements the schema lock is
made of. It was invisible because `ensure_schema` returns before taking the lock when
nothing is pending, so it would have surfaced on the first day a migration was added, as
every daemon failing at once. All five call sites now pass `run_conditional_cql_query`,
and `test_vlan_claims.SchemaExecutorTests` reads the call sites to keep it that way.

**Ported and verified against the live cluster:** authentication (shared `pbkdf2_sha256` hashes
and `hydra.sessions` with the Python tier, enforced once via a router `live_session`), cluster
overview, hosts, VM list/create/detail with disk allocation through the typed Linstor endpoints,
storage fabric, images, tasks, metrics, health. Navigation is one list checked against the router
by `navigation_test.exs`.

**Image upload is done and verified on hardware.** A custom `Phoenix.LiveView.UploadWriter`
pushes each chunk onto an open request to spark-daemon, so nothing is spooled in the web
tier. Verified end to end: 8 MiB written to a DRBD device and compared byte for byte,
`root:qemu 0660`, demoted to Secondary, volume defined at exactly 8192 KiB, and the
cancelled and truncated paths leaving no resource behind.

**Still on the Python tier**, counted against `static/*.html` on 2026-08-23: `hardware`,
`networking`, `sdn`, `settings`, `lcm`, `lanayru`, and the console (`vnc_auto.html`). Phoenix
carries ten routes against the Python tier's fourteen pages. `sdn.html` (875 lines) and
`settings.html` (792 lines) are the two large ones and between them account for most of the
remaining work.

**Costs still outstanding:** `hylia.py` (738 lines) and `lanayru.py` (468 lines) are imported as
Python modules by Spectrum and have no Elixir counterpart; they must be reimplemented, shelled
out to, or kept behind a port before those routes can move. `create_upgrade_zip.py` and hylia's
deploy path still know only about the Python image.

This rewrite addresses none of the P0 items and only part of P1: the DFS, networking, and LCM
defects live in `mipha`, `gatoway`, `urbosa`, `hylia`, `vali`, and `cluster_new`.

### Multiple disks per node

Every node has two 300 GB disks and uses one; `sdc` is idle on all three. The design is in
[docs/dfs/multi_disk.md](docs/dfs/multi_disk.md), and its first conclusion is that the easy
answer is the wrong one: `vgextend` into the existing thin pool makes one disk failure take
the node's whole extent store, and doubles the chance of it happening. That is worse than
using one disk.

The design is one filesystem per disk with placement in software -- which is what Nutanix
does and for the same reason. Work is bounded: `EgroupStore` becomes a set, a startup scan
builds `egroup_id -> disk`, placement picks least-free-first on seal, `op_capacity` sums and
reports per disk, and Purah treats referenced-but-absent as a repair candidate. No schema
change: which disk holds an egroup is a node-local fact, and `referenced_egroups()` already
produces the set that identifies a dead disk's losses.

`hydra.dfs_egroup_replicas` was in the schema, with `egroup_id`/`node`/`path`/`state`, and was
written by nothing. It was dropped by migration `0025`; see
[docs/dfs/multi_disk.md](docs/dfs/multi_disk.md) for why it was removed rather than made true.

Provisioning must not land first -- claiming both disks before sidon can use the second one
gains nothing and removes the guard currently keeping `sdc` untouched.

**Built (D-27): sidon mounts its own disks.** Nothing sidon owns is in `/etc/fstab`; the disks are
siblings at `/var/lib/hci/sidon/disks/<filesystem-uuid>`, named in `/etc/hci/sidon-disks`, and sidon
refuses any path whose disk is not provably mounted (journal volume absent: no start; extent disk
absent: that disk only). See [docs/dfs/multi_disk.md](docs/dfs/multi_disk.md). Still open:

* **Move the existing nodes.** The rollout only stages the manifest. Each node moves to the new
  layout the next time sidon starts, or by `sidon mounts apply` with sidon stopped. Until then
  Mimir reports a WARN per node, and `RECONCILE_SIDON_FSTAB` (a transitional guard that keeps the
  old fstab lines from failing the boot) must stay. Delete it once every node has moved.
* **A disk attached after sidon starts is not used until sidon restarts.** `sidon mounts apply`
  refuses while sidon runs, deliberately; mounting a *missing* disk (not moving one) under a live
  sidon would be safe but Purah's store is built once at start and would not see it.
* **Absent disk, then back.** Purah's repair may recreate a missing disk's groups from replicas, so
  a returning disk can hold duplicates. The stray sweep is meant to resolve them; nothing has
  exercised that path.
* **The rollout never writes or enables `sidon.service`** (only provisioning does). This design
  does not depend on it, which is why sidon does the mounting, but it means a unit change cannot
  reach an existing node.
* **`cluster create` after `destroy` used to leave no volume for sidon** (it re-claimed the first
  disk and ran a `mount` with no fstab line). Fixed with the carve/claim/stage step; it has been
  exercised only through unit tests and loop devices, not by a destroy and create on the cluster.

### The SPICE console, decided and built -- but unverified on hardware

The goal was ESXi-class console performance in a browser. SPICE had been half-present for a
long time: the vendored client was in the tree and compiled to WebAssembly on *every*
rollout, `agahnim` bridged TCP to WebSocket protocol-agnostically, and Spectrum's WebSocket
proxy was already parameterised by `console_type` and already refused a protocol mismatch
rather than downgrading. What was missing sat at the two ends -- no domain was ever given a
SPICE device, and no page loaded the client -- and both were gated on one unmade decision.

**Decided and built 2026-10-02: SPICE is per-VM, VNC stays the default.** The graphics device
lives in the domain XML and is therefore per-domain by construction, so a cluster-wide switch
would still have to rewrite every domain and restart every guest before it meant anything --
a per-VM, restart-requiring change wearing a toggle's clothes. The proxy was already per-VM,
so the stored column was the only part that did not exist. Reasoning in
[docs/console.md](docs/console.md).

`hydra.vms.graphics` (migration `0010-vm-graphics`; null means VNC, no row rewritten) is read
by both XML builders, which emit exactly **one** `<graphics>` device -- not both, which
`docs/vali.md` had claimed for a long time and no builder ever did. `static/spice_auto.html`
loads the vendored client through the same token exchange and proxy as the VNC page. The
duplicate console button is gone: there were two in `static/app.js`, and the second --
labelled for the SPICE client -- opened the VNC page.

Two choices that look wrong and are not. The SPICE video adapter stays **VirtIO rather than
QXL**, because `docs/vali.md` records QXL's BIOS ROM files being absent from the EL 10.2
repositories, and a video model whose ROM is missing is a domain that will not start; the
client needs the SPICE display *channel*, not QXL hardware. And image compression is **off**,
because the client decodes LZ while QEMU's default is `auto_glz`, a different format.

Found on the way and fixed: **the Phoenix VM page had no console button at all.** Every page
is Phoenix now, so the only console buttons in the tree were the two legacy ones -- an
operator on the current console had no way to open a guest console.

**Outstanding, and why this is not finished:**

* **No SPICE console has been watched working against a guest.** The cluster was unreachable
  when this was built. It is verified by `test_vm_graphics.py` (28) and `vm_graphics_test.exs`
  (10), and by checking every client API used against `main.js`'s exports -- not on hardware.
  The cursor is the path to watch, since its encoder had never produced valid output.
* **Whether these hosts can serve SPICE at all is open.** Red Hat deprecated QXL and then
  removed SPICE from the EL virtualisation stack, and these hosts are built from EL 10.2
  repositories. libvirt refuses a domain whose graphics type QEMU lacks, so the VM never
  starts. `/api/v1/host/capabilities` now reports a `graphics` list read from
  `virsh domcapabilities`; **check it first**, because if it does not contain `spice` then the
  right answer is to retire the vendored client and stop building the wasm on every rollout,
  not to finish this.
* The console ports still take no password and listen on `0.0.0.0` -- unchanged from what VNC
  has always done here, so SPICE matches it rather than worsening it, but the port is
  reachable directly on the LAN, bypassing the ticket exchange.

~~**The cursor encoder in the vendored client is broken.**~~ **Fixed 2026-09-10.**
`create_rgba_png` in `spice-html5/src/png.js` put BFINAL in bit 7 of the deflate header
instead of bit 0, and wrote a stored block's LEN/NLEN big-endian -- what a DataView does
unless asked otherwise, and the opposite of what deflate wants. Either alone was fatal. The
FIXME the file carried about libpng rejecting its output was this, and it is gone.

Worth keeping: the obvious check does not see this bug. Byte-swapping *both* LEN and NLEN
preserves their one's-complement relationship, so zlib's stored-block length check still
passes and the block merely declares a length that is not there. A test asserting NLEN is
the complement of LEN would have gone green on the broken file, and so would one asserting
the header byte, once either half was fixed. `test_spice_cursor_png.py` therefore asserts
nothing about the bytes: it runs the real `png.js` under node, inflates the IDAT with
Python's `zlib`, and compares the pixels that come back against the ones that went in. It
fails on the shipped file and on each half-fix, and it skips where node is absent.

### Built: the extent-based store (Sidon), with Hydra as the metadata layer

**Decision taken 2026-08-22, built the same day.** The reasoning is in
[docs/dfs/](docs/dfs/README.md) — architecture, the invariant contract, the journal/drain
data path, epoch-fenced ownership, the metadata schema with exactly-once drain, the Ganon
harness, milestones with gates and abandonment values, and the ADR list with every
rejected alternative. The operator's view is [docs/sidon.md](docs/sidon.md).

Shipped: the journal and drain, extent groups with checksummed and identity-stamped
footers, write-all journal replication, replica-side epoch fencing persisted across
restarts, ownership transfer with recovery from a replica's journal, forwarding for
non-owners, extent replication with read repair, and Purah's re-replication, mark-sweep
reclamation and scrub. LINSTOR and DRBD are gone from the tree.

The re-replication in that list was never the defect, and it is worth being exact about
what was. It restored a replica set correctly the whole time and simply never had cause
to run: `op_create` defaulted `rf` to 1 and no caller has ever sent one — not vali, not
the console, not the CLI, not the Elixir tier — so every vdisk on every cluster was
created single-copy whatever the operator had configured, and one copy *was* the
requested count. Nothing reported it either, because every replication view compared a
vdisk's replica list against the `rf` on its own row, which the same default had written
as 1. One replica, one requested, healthy.

**Fixed 2026-09-10.** A create now takes its count from the container's `ftt`, falling
back to `cluster.json`'s `redundancy_factor`, converting the fault tolerance to copies
(`ftt + 1`) and clamping to the nodes that could hold one; a clone takes the policy in
force now rather than inheriting its parent's, which is what propagated the single-copy
default one generation at a time. An explicit `rf` or `replicas` in the request still
wins. `valcli storage.replication` is the view whose absence hid all of this: policy,
requested, actual, side by side. Vdisks created before the fix are left alone — restoring
a replica that stopped answering is an emergency and stays automatic, while topping one
up to a factor it never asked for is a bulk data copy and is `valcli storage.replicate`,
opt-in and one copy per vdisk per run.

Ganon was built first and calibrated against DRBD, as designed. That calibration produced
the finding worth keeping: the same corruption injected under both substrates is *served
as data* by DRBD and refused with EIO by Sidon.

**Left, in the order it probably matters:**

* ~~mTLS on the replication port.~~ **Built 2026-08-22.** `rustls` 0.21 over the
  existing blocking socket, mutual against the cluster CA in `/etc/hci/spark/certs`.
  Plain rustls rather than `tokio-rustls`, which agahnim uses: this daemon's byte path is
  blocking threads and std, and an async runtime for the transport would restructure the
  data path to solve a problem it does not have. The crates were already on the nodes.

  Verified on one host with two instances bound to its real address, so the traffic was
  genuinely TLS rather than loopback-exempt: the peers completed a mutual handshake, an
  RF2 vdisk replicated and the guest's bytes landed in the replica's journal, a plaintext
  client got a TLS fatal alert, a client presenting no certificate got
  `TLSV13_ALERT_CERTIFICATE_REQUIRED`, and the node's own certificate completed the
  handshake as the positive control.

  The bind address and peer list now come from `cluster.json` rather than the unit file,
  so membership changes do not need the unit regenerating. Still untested across real
  hosts, which needs the other nodes.
* **Multi-host soaks.** Everything above was verified with several daemon instances on one
  machine, which proves the protocol and the state machine and cannot prove independence
  from one machine: the instances share a clock and a page cache. Real hosts are what
  settle clock skew, genuine partitions, and the kernel-death injector — which needs
  `kernel.sysrq` widened on a node somebody is willing to lose.
* **A cluster-wide replica check before a rolling reboot.** `hylia` verifies the node it
  is about to take down — daemon answering, store mounted with room, no degraded vdisk —
  and nothing verifies the *cluster*. Rebooting the node holding the last reachable
  replica of a vdisk still makes it unavailable. It needs more than one node to write or
  to test.
* ~~Leftover LINSTOR logical volumes on upgraded nodes.~~ **Reported, not removed
  (2026-08-22).** The DRBD teardown unmounts, downs the resources, unloads the module and
  removes the packages, and deliberately does not touch the backing volumes: one may be a
  VM disk whose guest was never migrated, and a rollout is not the place to decide that.
  It now *names* them instead -- LINSTOR suffixed every volume it created with `_00000`,
  so they are identifiable -- with sizes and the `lvremove` line, because nothing else
  reports them anywhere and they share the thin pool with the extent store. That is how
  they sat unnoticed on the test node for four days after the tree was clean; those four
  (`img-test`, `linstor-db`, `scratchtest`, `test-disk0`) have since been removed by hand.
* ~~Snapshots and clones.~~ **Built 2026-08-22.** A map copy, sharing every extent with
  the parent; zero bytes copied, and the cost is the number of extents rather than the
  size of the disk. `valcli storage.snapshot|clone|children`. Mark-sweep needed no change
  at all -- it marks from the whole block map, so a child's references keep extents alive
  whether or not it is attached, which is the refcount decision paying for itself.

  Two guards fired on the way, and one had never fired before: the footer identity check
  refused every snapshot read (fixed by recording, per block-map row, which vdisk wrote
  the extent), and `class` came back from Daruk as `field_2_` because namedtuple renames
  Python keywords -- so *every* sealed image had been loading as writable and the
  immutability check had never once run.

  Scheduled snapshots with a retention policy, in-place rollback of a detached vdisk and a
  read-only console view are built since -- see the next entry.
* **Scheduled snapshots, retention and rollback.** Built; [docs/dfs/snapshots.md](docs/dfs/snapshots.md).
  [Rauru](docs/rauru.md) runs the policy hourly (it was a Dagur job; the seed is gone and the
  console deletes the old row on start); policy is in
  `hydra.dfs_snapshot_policies` (migration `0022`) and provenance in `hydra.dfs_snapshot_index`
  (`0023`); each snapshot, prune and rollback is a Catalyst task with component `Rauru`. Rollback is Sidon's new
  `rollback` op and is refused unless the vdisk is detached. Open around it:
  * **Rollback of an attached vdisk** is a design, not a feature
    ([docs/dfs/rollback_attached.md](docs/dfs/rollback_attached.md)): stop the VM, roll back,
    start it, as one task tree in Vali. Needs a shutdown-deadline policy first.
  * **The rollback path has no live test.** The refusals are unit-tested (`plan` in
    `sidon/src/control/rollback.rs`) and the control-plane half is tested against a fake, but
    nothing has claimed, fenced replicas and swapped a real map on a cluster. Run it on the test
    cluster, including killing Sidon between the class flip and the map write, before trusting
    the resume path.
  * **Pruning has a millisecond window.** Retention re-reads `dfs_vdisks` immediately before each
    delete, but Sidon's `delete` does not itself refuse a snapshot that has children, so a clone
    made between that read and the delete orphans its `parent_vdisk` (the data is safe; the
    lineage record is lost). Closing it wants a conditional delete in Sidon.
  * **A snapshot is not atomic across a drain.** `derive_child` (`sidon/src/control.rs`) drains the
    owner's journal and then reads the block map *after* releasing the vdisk lock, so a
    write-triggered drain at the high-water mark can land its batches between the two and the
    copy can contain part of a drain. Each extent is individually valid; the set is not
    necessarily prefix-closed. Holding the lock across the map read fixes it. Found while
    writing this, not fixed: it is in a function other work is editing, and it predates the
    schedule, which only makes it likelier to matter.
  * **Consistency group: resolved by protection domains**
    ([docs/dfs/protection_domains.md](docs/dfs/protection_domains.md)) for the case that matters:
    a VM's disks are snapshotted inside one guest pause and the set says what it achieved. A
    per-vdisk policy is still per vdisk; a disk in an enabled domain is left to it.
  * **The `<vm>-disk<n>` convention is the only link from a vdisk to its VM**, and the rollback
    VM-state check depends on it; a vdisk named otherwise is checked by attachment alone.
  * **The console is read-only** and policy is set only with `valcli`.
  * **Policy interval is hourly at best**, because the Dagur job is. A finer one needs the job
    interval lowered, not the policy table changed.
  * **Task rows carry component `Catalyst`**, matching the scheduled job that parents them. If
    these should have a component of their own, that is the owner's naming call.
* **Protection domains: built, never run against a real guest.**
  [docs/dfs/protection_domains.md](docs/dfs/protection_domains.md); `rauru_protection.py`,
  migrations `0030`-`0032`, `valcli storage.domain*`. Open:
  * **No test has paused a real guest.** `virsh suspend` on a domain whose disks are Sidon NBD
    exports has not been run: whether in-flight requests complete cleanly, how long a real set
    takes under the barrier (a snapshot copies a map, about a row per MiB), and whether Mipha's
    health checks object to a paused domain are all unmeasured. Measure the pause first; the
    default 30 s limit is a guess.
  * **Nothing runs it on a timer.** The Rauru daemon is the caller (`run()` on its timer,
    `recover(force=True)` once at start-up, `pinned_sets` overridden). Until then
    `valcli storage.domain.run`, or a Dagur row, is the trigger.
  * **The console shows origin `domain` as "unindexed".** `spectrum_phx/lib/spectrum_phx/snapshots.ex`
    maps unknown origins to `:unindexed`; it needs a `domain` clause (and the template one).
    Left alone because the console is another agent's file.
  * **`derive_child` is not atomic across a drain** (see the snapshots entry): under a guest
    pause this reduces to the in-flight ambiguity a power cut has anyway, but with
    `--quiesce none` it is a real hazard, and is why such sets are labelled `none`.
  * **Application consistency** needs an agent in the guest (qemu-guest-agent `fsfreeze`) and
    none is deployed. Out of scope; the doc says so on its first screen.
  * **A bare `vdisk:` member cannot borrow a VM's cut.** The `<vm>-disk<n>` convention is a name,
    not a fact; such a set says `none`.
* **Replicating snapshots to another site: designed, data plane simulated.**
  [docs/dfs/replication.md](docs/dfs/replication.md), D-28 to D-31. Built:
  `sidon/src/replicate*` (manifest, framed groups, export with verification, importer with
  staging and atomic publish, token bucket; 32 tests) and `rauru_replication.py` (manifest from
  Hydra rows, delta, resumable driver; 37 tests). **All of it ran only against two directories on
  one machine and fakes.** Not built, and each is a real piece of work:
  * the transport, the pinned-SPKI TLS verifier and the import-only listener (D-28);
  * the `offer`/`group`/`publish` wire operations and their entry in spark's `DFS_VDISK_OPS`;
  * a Hydra-backed `MapSink` and a `GroupStore` over `EgroupStore` that registers in `dfs_egroups`
    (needs an adopt-a-file method in `extent.rs`);
  * **Purah must treat a job's installed-but-unpublished groups as roots**: its grace is 600 s and
    a long transfer outlasts it. Settle this before the transport (`purah.rs`);
  * the state tables `dfs_remote_sites` and `dfs_replication_sets`, ids `0033`/`0034` reserved and
    deliberately not migrated until a second site exists (a table nothing exercises is what
    `0025` removed);
  * pairing, site certificate issuance and renewal in Impa, quota, bandwidth schedules;
  * **failover and failback**: a design only, because they cannot be exercised without a second
    site. A VM definition is not in the replicated data at all.
* **Compression at seal time.** The cheap one: sealed groups are immutable and the footer
  already carries an algorithm byte, so it is off the write path entirely.
* **Erasure coding**, as a Purah job over cold sealed groups — **decided against on three
  nodes** (D-24 in [decisions.md](docs/dfs/decisions.md)): 2+1 saves at most 25% of the cold
  data and cannot heal after a node loss because it has no spare node. Revisit at five or
  more nodes with a cold sealed fraction above about half of used capacity; measure the
  fraction with `valcli storage.heat`. **Deduplication is argued against** there too.
* **`vhost-user-blk`** beside NBD, deliberately last: performance work reorders
  operations, and reordering is where invariants go to die. **Designed, not built**
  ([docs/dfs/vhost_user_blk.md](docs/dfs/vhost_user_blk.md), D-25): the first step is a
  benchmark, and the first thing it is likely to find is that the transport is not the limit.
  Open prerequisite: on an EL10 node, run `/usr/libexec/qemu-kvm -device help | grep -i
  vhost-user` to find out whether the shipped qemu has the device at all.
* **Sidon serves one NBD request at a time per connection.** The serve loop reads a request,
  executes it and replies before reading the next, behind one `Mutex<Vdisk>`, so a guest's
  queue depth and the `queues='N'` the libvirt XML asks for are flattened to 1. Making the
  backend concurrent is a journal-ordering change (gap-free sequences, replica order) and
  needs its own Ganon scenario; it is the real prerequisite for any transport work.
* **A 4 KiB read of drained data reads and checksums the whole 1 MiB extent**
  (`extent.rs::read_extent`), 256x amplification, and opens the group file each time.
* **Journal replication is sequential across replicas**, one mutex-guarded connection per
  peer shared by every vdisk, although `docs/sidon.md` section 1 draws the replica appends
  as parallel. Either the code or the diagram is wrong; at RF=2 it is one replica and the
  difference is invisible, at RF=3 it is a second serial round trip per write.
* **A drain runs inline in `Vdisk::write`** once the journal passes its high-water mark, with
  the vdisk mutex held, so the write that crosses it waits for the whole drain.

### Scale-out add-ons (blueprints only)

* **Helios Portal** — multi-cluster control plane (Prism Central analog): aggregation service, federated
  Prometheus metrics, federated Loki logs, cross-cluster Hylia LCM staging.
* **Helios Files** — scale-out NFS/SMB add-on on a Linstor/DRBD HA volume, orchestrated by Vali with
  Mipha-driven failover.
* **Helios Horizon** — AD-integrated VDI/application streaming via Apache Guacamole (`guacd`).
* **Scale-Out Urbosa** — FRRouting BGP EVPN control plane with per-host ARP suppression, resolving the
  head-end-replication and FDB-leak items above.

## Service wiring (found by `test_service_wiring.py`)

Everything `KNOWN_GAPS` and `EXCLUDED` in that test records as a gap rather than a decision. Each
entry fails the test the moment it is fixed, so it has to be deleted here too.

* **Rauru has not been run against a cluster.** It is wired and its loop is tested with fakes, and
  `rauru --check` and `systemd-analyze verify` were run on a node's own Python, but a real start
  against Hydra, the `rauru-snapshots` election and one scheduled run need a cluster. The commands
  to run after `cluster create` are in [docs/rauru.md](docs/rauru.md).
* **The rollout never writes or enables sidon's unit.** `provision.py` is the only writer;
  `deploy_updates.py` builds and installs the binary and deliberately does not restart it. A node
  whose sidon unit is missing or stale cannot be repaired by a rollout. Belongs with the change
  to how sidon's disks are mounted, which edits the same code.
* **The rollout writes a different *Python* console Quadlet from provisioning.** (The Phoenix
  console's Quadlet has a single source, `spectrum_phx/quadlet/`, is the hardened form, and is installed
  by provisioning and the rollout alike -- see [docs/spectrum_phx.md](docs/spectrum_phx.md) section 0.) `deploy_updates.py`'s
  `spectrum_container_content` still has `PodmanArgs=--privileged`; `provision.py`'s drops all
  capabilities and sets `NoNewPrivileges`, with the reasoning. A node provisioned today is put back
  to the privileged console by its next rollout. They also disagree on the maintenance condition
  and volume labels (and slate's Quadlet differs in `:ro,z` and `[Install]`). Which is canonical
  is the owner's call, and the change restarts the console on every node.
* **`mcli-runner`'s maintenance residue probe names six services** of those vali stops when a host
  enters maintenance, so a seventh left running is not reported. Which of the rest may
  legitimately stay up needs a cluster to judge.
* **`static/app.js` `getCheckCategory` still carries `aether_*` and `linstor_latency_check`.**
  They are check names, not services, so the wiring test does not look at them.
* **The watchdog and the boot-time start list do not supervise slate, agahnim or hylia.** The
  reconcile loop (`MANAGED_SERVICES`) and `Restart=always` do, and `test_mimir_checks` pins that
  slate and hylia are not in the watchdog's set. Recorded as an exemption, not a decision to
  remove the older lists.
