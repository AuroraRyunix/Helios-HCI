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

* **ZooKeeper observers cannot be promoted, so quorum is bound to three arbitrary nodes.**
  `provision.py` makes the first three provisioned nodes voters and everything above node 3 an
  observer (`ZOO_PEER_TYPE=observer if idx > 3`). That part is right -- observers scale reads without
  slowing writes. The gap is that there is no way to change the set afterwards: lose two of those three
  and cluster coordination stops with every other node healthy and idle, and a decommissioned voter
  takes its vote with it.
  ZooKeeper has supported dynamic reconfiguration since 3.5 and the deployed version is **3.9.2**, so
  the mechanism is present and merely switched off -- `/conf/zoo.cfg` sets `standaloneEnabled=true` and
  never sets `reconfigEnabled`, so `reconfig` is refused. Verified on the live node.
  Needs: `reconfigEnabled=true` in the Quadlet config, a `cluster zk-promote` / `zk-demote` path that
  refuses any change that would lose quorum mid-reconfiguration, promotion wired into
  `cluster decommission` so removing a voter hands its vote on, and the dynamic config file
  (`zoo.cfg.dynamic`) accounted for in provisioning -- once reconfig is enabled, ZooKeeper owns
  membership and a provisioner that rewrites `zoo.cfg` wholesale will fight it.
  Nutanix migrates the ZooKeeper role off a permanently-failed node; this is the same idea.

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

Suggested shape: keep `/query` working, add typed endpoints (`/v1/vm/claim`, `/v1/vm/migrate-lock`)
backed by prepared LWT statements, migrate invariant-critical writes first, move schema ownership into
Daruk, then gate raw CQL behind an explicit admin path (`valcli db.query` is a legitimate feature).
This composes with the Phoenix rewrite — Xandra gives prepared statements and LWT natively.

---

## P2 — Storage / DFS

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
* ~~`provision.py` does not know about the Phoenix console.~~ **Half done (2026-08-22).**
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

  Still open around it: scheduled snapshots with a retention policy, rollback in place
  (a clone recovers data; it is not the same as putting a VM back), and a console view.
* **Compression at seal time.** The cheap one: sealed groups are immutable and the footer
  already carries an algorithm byte, so it is off the write path entirely.
* **Erasure coding**, as a Purah job over cold sealed groups — after the above, not
  before. **Deduplication is argued against** in [decisions.md](docs/dfs/decisions.md).
* **`vhost-user-blk`** beside NBD, deliberately last: performance work reorders
  operations, and reordering is where invariants go to die.

### Scale-out add-ons (blueprints only)

* **Helios Portal** — multi-cluster control plane (Prism Central analog): aggregation service, federated
  Prometheus metrics, federated Loki logs, cross-cluster Hylia LCM staging.
* **Helios Files** — scale-out NFS/SMB add-on on a Linstor/DRBD HA volume, orchestrated by Vali with
  Mipha-driven failover.
* **Helios Horizon** — AD-integrated VDI/application streaming via Apache Guacamole (`guacd`).
* **Scale-Out Urbosa** — FRRouting BGP EVPN control plane with per-host ARP suppression, resolving the
  head-end-replication and FDB-leak items above.
