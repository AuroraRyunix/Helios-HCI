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
  covers 22 endpoints. **The remaining count was wrong, and larger than recorded**: 184 call sites,
  not 105, because `cluster_new.py` (62) and `dagur.py` (1) were never counted -- they reach the same
  shell through their own copies of `run_remote_spark`. Current: `spectrum_server.py` 64,
  `cluster_new.py` 62, `vali.py` 28, `hylia.py` 22, `mipha.py` 7, `dagur.py` 1. They group into roughly
  ten endpoint families -- systemd unit control (~15), filesystem operations (~22, needing careful path
  allowlisting), network probing via `ss`/`ip` (~12), linstor/drbd (~14, mostly already typed), virsh
  (~4), and `podman exec` for cqlsh (~8, which belongs behind Daruk rather than Spark). This is a
  programme of work rather than a task: each family needs an endpoint designed, validated and tested.
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

* **Catalyst double-claims scheduled jobs.** `catalyst.py:237` reads `last_run_epoch`, decides a job is
  due, and writes it back blind. Two Catalyst instances that both believe they hold leadership -- which
  `is_zookeeper_leader()` permits, since it probes ZooKeeper's four-letter `stat` and falls back to
  "lowest node with 9091 open" -- both submit the same Dagur job. `IF last_run_epoch = ?` fixes it, and
  the Daruk LWT endpoints added for VM ownership are the pattern to follow.
* **`run_cql_query` cannot report a failed LWT.** It renders the rejection row as the string
  `"False 10.10.102.41"` with `rc=0`, which is indistinguishable from success. Every caller still using
  it for a conditional write is silently treating lost races as wins. This is why the typed endpoints
  return `(ok, applied, current, error)` instead, and it is the argument for retiring raw `/query`.

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

Suggested shape: keep `/query` working, add typed endpoints (`/v1/vm/claim`, `/v1/vm/migrate-lock`)
backed by prepared LWT statements, migrate invariant-critical writes first, move schema ownership into
Daruk, then gate raw CQL behind an explicit admin path (`valcli db.query` is a legitimate feature).
This composes with the Phoenix rewrite — Xandra gives prepared statements and LWT natively.

---

## P2 — Storage / DFS

* **Mipha's HA failover write is unconditional.** `mipha.py:1143` resets `state='Stopped', host_ip=''`
  on every VM of a fenced host. It is guarded by SSH fencing and three consecutive failures, but a VM
  already recovered elsewhere is clobbered by a late write. `IF host_ip = '<dead ip>'` scopes it to the
  host that actually died.
* **VM delete can orphan a running guest.** `spectrum_server.py` reads `host_ip`, destroys the domain
  there, then deletes the row unconditionally. A VM that migrated in between is destroyed nowhere and
  its row disappears, leaving a guest running that nothing knows about.
* **Lanayru clears the migration lock it never held.** `lanayru.py:295` writes `hydra.vms SET
  status='running'` blind -- `status` is the migration-lock column.

* **`/api/cluster/metrics` scans the whole metrics table on every poll.** `SELECT JSON * FROM
  hydra.logos_metrics` with no `WHERE` and no `LIMIT` -- roughly 8,600 live rows on a 3-node
  cluster at a 30s cadence with a 24h TTL -- then discards all but 40 samples per host in the
  browser, once per open tab. Same shape in `dagur_runs`, where a bare `LIMIT 100` returns
  whatever 100 rows the coordinator reached first, not the 100 most recent. Fixed in the Phoenix
  port; still live on 8443.
* **`GET /api/images` writes.** It scans
  `/var/lib/hci/aether/volumes/default-image-container` and inserts catalogue rows for files it
  finds, so a page load mutates the database. If the catalogue is meant to be reconciled with the
  filesystem, that belongs in a Dagur job.
* **`/api/images/delete` always answers 200.** The row is deleted first, and both the
  `resource-definition delete` and the fan-out `rm -f {path}` (unescaped, straight into a root
  shell) are unchecked. A failed LINSTOR delete leaves storage allocated that nothing in the UI
  can ever see again, and the operator is told it worked.
* **`catalyst_tasks` has no time-ordered clustering key** (`task_id` is the whole primary key), so
  "the most recent N tasks" is not answerable server-side and every read is a full scan.
  **Partly mitigated (2026-08-20)**: migration `0003-bound-task-history` sets a 30-day TTL, so the scan
  no longer walks a table that grows forever. That bounds the cost; it does not make the query indexed.
  The real fix is a companion table keyed by time bucket that Catalyst writes alongside, which is a
  dual-write and belongs with the work that migrates the readers. Note that setting a table property is
  idempotent, unlike `ALTER ... ADD`, which is why this one could be a migration at all -- verified
  against real Scylla.
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
* ~~`podman build` cannot be run from a clean checkout.~~ **Resolved (2026-08-20)**: the Dockerfile
  copies `spectrum_server.py` and renames it on the way in, so the build works from the tree as checked
  out. The rename indirection is gone rather than worked around -- it had four touchpoints
  (`provision.py`, `deploy_updates.py` twice, and `hylia.py`'s rebuild), all staging the file under the
  other name. The in-image layout is unchanged, so `CMD` and every path inside the container stay as
  they were. Verified by building the image from a clean checkout on the test node.
## Design / future work

### In progress: Phoenix LiveView rewrite of Spectrum

Underway as a strangler migration, documented in [docs/spectrum_phx.md](docs/spectrum_phx.md).
`spectrum-phx` runs beside the Python tier on port 8444; Slate still routes to 8443, so nothing
is cut over yet.

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

**Still on the Python tier:** networks, snapshots, console proxy, and cluster lifecycle.

**Costs still outstanding:** `hylia.py` (738 lines) and `lanayru.py` (468 lines) are imported as
Python modules by Spectrum and have no Elixir counterpart; they must be reimplemented, shelled
out to, or kept behind a port before those routes can move. `create_upgrade_zip.py` and hylia's
deploy path still know only about the Python image.

This rewrite addresses none of the P0 items and only part of P1: the DFS, networking, and LCM
defects live in `mipha`, `gatoway`, `urbosa`, `hylia`, `vali`, and `cluster_new`.

### Scale-out add-ons (blueprints only)

* **Helios Portal** — multi-cluster control plane (Prism Central analog): aggregation service, federated
  Prometheus metrics, federated Loki logs, cross-cluster Hylia LCM staging.
* **Helios Files** — scale-out NFS/SMB add-on on a Linstor/DRBD HA volume, orchestrated by Vali with
  Mipha-driven failover.
* **Helios Horizon** — AD-integrated VDI/application streaming via Apache Guacamole (`guacd`).
* **Scale-Out Urbosa** — FRRouting BGP EVPN control plane with per-host ARP suppression, resolving the
  head-end-replication and FDB-leak items above.
