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

* **The update chain still has no signature.** Hardened but not solved: `check_updates.py` still takes
  `download_url` and its `sha256` from the same response, and the manifest still declares the hashes for
  its own contents. Real integrity needs a detached signature over the manifest verified against a key
  pinned at provision time. Everything else in the update path is now defence-in-depth around this gap.
* **Unsandboxed root command execution.** `/api/v1/execute` runs caller-supplied strings via
  `shell=True` as root. **In progress**: the typed API in [docs/spark_api.md](./docs/spark_api.md)
  covers 22 endpoints and `spectrum_server.py` is down from 79 raw calls to 45. `/api/v1/execute`
  goes when that reaches zero across `spectrum_server.py` (45), `vali.py` (29), `hylia.py` (23) and
  `mipha.py` (8). The v2 gap list in that doc names what each remaining group needs.
* **Catalyst/Vali internal APIs are reachable on the LAN with no auth.** `catalyst.py` (`9091`) and
  `vali.py` (`9095`) bind `0.0.0.0` under `Network=host` and check neither source IP nor credential.
  Input validation now closes the injection paths; the authorization gap is untouched.
* **No mTLS certificate renewal.** Certificates under `/etc/hci/spark/certs/` and `/root/.certs/` are
  generated once at provision time. Expiry silently freezes all inter-node orchestration.
  `run_remote_spark` also sets `check_hostname = False`, so any valid node cert can impersonate any node.
* **Spectrum runs `--privileged` with `Network=host`** (`provision.py`). Worth scoping to explicit device
  and capability grants.

---

## P1 — Metadata layer (Daruk as a Medusa Store)

Daruk is already mapped to Nutanix's "Medusa Proxy" and is already the single per-node choke point every
daemon's `run_cql_query` talks to — but it is a pass-through that executes raw CQL text. Scylla already
provides the storage-engine work Nutanix hand-built into Cassandra; what is missing is the consistency
discipline, the ring lifecycle, and the store abstraction.

* ~~Daruk silently downgrades writes from QUORUM to ONE.~~ **Resolved (2026-08-19)**: reads may
  still degrade, since stale data is recoverable; mutations, DDL and lightweight transactions now
  surface the failure. Retrying a write at ONE during a partition is how two nodes come to believe
  they own the same VM. The trigger was also narrowed from substring matching on
  "unavailable"/"timeout"/"active" to driver exception types -- "active" alone matched a wide range
  of unrelated errors. Ten classifier cases unit-tested; verified live.
* **No compare-and-swap on ownership.** `IF NOT EXISTS` is used in 11 places, all seeding static rows.
  VM ownership, the migration lock, and task claiming are all blind writes. `UPDATE hydra.vms SET
  host_ip = ? WHERE name = ? IF host_ip = ?` would make the dual-primary scenario structurally impossible
  rather than defended-against. The migration lock in particular is still read-then-write and therefore
  racy even now that its column exists.
* ~~Scheduled major compaction is an anti-pattern.~~ **Resolved (2026-08-19)**: the 12-hourly
  `nodetool compact` job is disabled, with a migration for existing clusters. Disabled rather than
  deleted so an operator can see it and re-enable deliberately.
* **Schema is scattered and unversioned.** 38 `CREATE TABLE IF NOT EXISTS` across five daemons with no
  migration system, so two daemon versions can race to define different schemas.
* **No ring lifecycle management.** Nutanix auto-detaches an unhealthy node from the ring. Helios has no
  quorum gate on maintenance entry and no decommission/rejoin sequencing.

Suggested shape: keep `/query` working, add typed endpoints (`/v1/vm/claim`, `/v1/vm/migrate-lock`)
backed by prepared LWT statements, migrate invariant-critical writes first, move schema ownership into
Daruk, then gate raw CQL behind an explicit admin path (`valcli db.query` is a legitimate feature).
This composes with the Phoenix rewrite — Xandra gives prepared statements and LWT natively.

---

## P2 — Storage / DFS

* **No maintenance-mode quorum gate.** Entering maintenance stops the local `hydra-db` unconditionally.
  On a 1- or 2-node cluster with QUORUM this can freeze all queries cluster-wide.
* **No out-of-band fencing path.** `mipha.py` `ssh_fence_host` relies entirely on Spark's mTLS API. If a
  node's software hangs while the box stays powered, there is no IPMI/PDU STONITH fallback.
* **No automated self-fencing on partial failure.** Mipha's ping-based liveness reports a host healthy
  while its storage or database daemon is dead.
* **No backup / disaster recovery.** Nothing snapshots the `hydra` keyspace or Linstor metadata to an
  external target.
* ~~Live migration still passes `--unsafe`.~~ **Resolved (2026-08-19)**: removed. It was only needed
  while VM disks carried `--allow-two-primaries` permanently; that window is now scoped to the
  migration itself, so libvirt's coherence check is the one we want.
---

## P2 — Networking

* **Urbosa leaks every resource it creates.** T1 namespaces, `br-ov-*`, `vxlan-*`, veths, T0 return
  routes, and FDB peer entries are never removed when the database row disappears — a deleted tenant
  segment keeps forwarding. Needs an ownership/GC model; deliberately not attempted in the fix pass.
* **Transit /30 allocation collides.** `md5(router_id)[:4] % 16384` with no collision check and no
  persistence: first collision at ~178 routers, 413 collisions at 4000. Needs a persisted pool.
* **VXLAN head-end replication overhead.** Static flood entries replicate every broadcast to every peer.
  "Scale-Out Urbosa" below is the designed fix.
* ~~Bifrost split-brain fallback.~~ **Resolved (2026-08-19)**: when ZooKeeper names a leader that is
  not serving, Bifrost no longer elects a replacement by sort order -- a second election that can
  disagree with the ensemble's, and in a partition each side would pick the lowest candidate it can
  see. It releases the VIP instead: briefly unreachable is visible and recoverable, duplicated is not.
---

## P3 — Code health

* **`run_cql_query` is copy-pasted into at least six files** (~40 lines each, including the cqlsh
  fallback). This is why the CQL-injection items had to be fixed at each call site — there is no single
  query layer to parameterize. The Daruk work above is the structural fix.
* **`spectrum_server.py` is two if/elif chains** — 7,300+ lines, 95 API paths, `do_GET` ~1,800 lines and
  `do_POST` ~3,150 lines, with routing, auth, validation, and shell-outs interleaved.
* **13 unsupervised background threads in Spectrum**, all `daemon=True` with no restart semantics.
* **`check_updates.py` can report "update available" forever.** When hylia is unreadable the current
  version is a fabricated constant, and the component loop substitutes a fallback for `Unknown`/`N/A`
  versions — neither can ever equal the target. The fix is for hylia to record the build it actually
  installed into `hydra.lcm_inventory` at deploy time, which needs a schema and UI change.
* **Proposed Mimir/mcli diagnostic checks not implemented** (`docs/audit_findings.md` §13). Several map
  directly to items above — `drbd_split_brain_check`, `mtls_cert_expiry_warning`, `scylladb_quorum_safety`
  — and would give early warning. A `replication_factor_vs_repair` check would surface the RF gap above.
* **Air-gap / private registry hardcoding.** Images are pinned to `docker.io`/`quay.io` with no
  `--registry` override, blocking air-gapped provisioning.

---

## Missing tooling / process

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
* **`podman build` cannot be run from a clean checkout** — the Dockerfile expects `server.py`, which is
  produced by renaming `spectrum_server.py` during provisioning (see `docs/AGENTS.md` §7).

---

## Design / future work

### Under evaluation: Phoenix LiveView rewrite of Spectrum

Spectrum is **already containerized** (`Dockerfile` → `localhost/spectrum:latest`, Quadlet in
`provision.py`), so containerization is not part of the work. Scope: 7,300+ lines of Python across 95 API
paths plus 21,484 lines of first-party frontend (`static/app.js` alone is 10,685). Vendored noVNC,
spice-html5, and pako stay; `agahnim` already owns the console WebSocket, so the hardest realtime piece is
out of scope.

Buys: pushed diffs instead of REST polling for task/DRS/metrics state; OTP supervision for the 13
unsupervised threads; prepared statements via Xandra removing the CQL-injection class structurally;
better fan-out for per-node mTLS calls.

Costs: `hylia.py` (738 lines) and `lanayru.py` (468 lines) are imported as Python modules by Spectrum, so
they must be reimplemented, shelled out to, or kept behind a port. The build becomes multi-stage
Elixir + assets, which reworks `create_upgrade_zip.py` and hylia's deploy path.

Recommended shape: strangler, not big-bang — Traefik already fronts everything, so routing a path prefix
to a second backend is config. Start with the hardest-polling dashboards. Do the injection and metadata
work first. Note this rewrite addresses none of the P0 items and only part of P1: the DFS, networking, and
LCM defects live in `mipha`, `gatoway`, `urbosa`, `hylia`, `vali`, and `cluster_new`.

### Scale-out add-ons (blueprints only)

* **Helios Portal** — multi-cluster control plane (Prism Central analog): aggregation service, federated
  Prometheus metrics, federated Loki logs, cross-cluster Hylia LCM staging.
* **Helios Files** — scale-out NFS/SMB add-on on a Linstor/DRBD HA volume, orchestrated by Vali with
  Mipha-driven failover.
* **Helios Horizon** — AD-integrated VDI/application streaming via Apache Guacamole (`guacd`).
* **Scale-Out Urbosa** — FRRouting BGP EVPN control plane with per-host ARP suppression, resolving the
  head-end-replication and FDB-leak items above.
