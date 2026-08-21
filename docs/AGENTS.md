# AGENTS.md — Technical Reference for AI Coding Agents

This document is written for an AI agent (or a new engineer) making code changes in this repository. It complements [docs/hci_master_architecture_guide.md](./hci_master_architecture_guide.md) (the deepest existing reference) and [docs/audit_findings.md](./audit_findings.md) (the known-issues backlog) rather than duplicating them — read those two first for depth, this file for orientation and repo-specific mechanics.

---

## 1. Repo layout

```
Helios-HCI/
├── *.py, catcli, mcli, mcli-runner, nodetool, allssh   # Flat root: every daemon and CLI (see §2)
├── agahnim/           # Rust console-proxy sidecar (Tokio); builds with `cargo build --release`
├── slate_config/       # Traefik ("Slate") config: traefik.yml (static), dynamic.yml (dynamic routing)
├── static/             # WebUI frontend served by spectrum_server.py; no build step
│   ├── novnc/          # Vendored noVNC (VNC-in-browser client)
│   ├── spice-html5/     # Vendored SPICE-HTML5 client
│   └── vendor/pako/     # Vendored pako (zlib) JS library
├── docs/               # This file, architecture/deployment/setup docs, per-daemon narrative +
│   │                    #   technical doc pairs, and docs/history/ (superseded changelogs)
├── Dockerfile           # Builds the Spectrum container image — see §6, this is NOT run directly
│                        #   against the files in this repo as checked out
└── test_hylia.py        # The only automated test in the repo (see §5)
```

There is no `requirements.txt` anywhere in the tree. Every root Python script assumes its third-party imports (`paramiko`, the `cassandra-driver` package imported as `cassandra` in `daruk.py`, etc.) are already installed on the EL10.2 host image it runs on.

## 2. Daemon / CLI map

Every service below is named after its Nutanix analog (see the table in the top-level [README.md](../README.md) §4 for the full mapping). Files are root-level `*.py` unless noted:

| File | Role |
| :--- | :--- |
| `spectrum_server.py` | Main WebUI/REST API backend ("Spectrum"). Serves `static/`, handles VM/storage/task/auth logic. Largest file in the repo (~340KB). |
| `provision.py` | Cluster bootstrap/provisioner CLI. Embeds base64 copies of ~19 other scripts to push to new nodes and writes the Podman Quadlet unit files (~1.3MB, mostly base64 payload). |
| `sync_provision.py` | Regenerates those base64 payloads from the live source files — see §4. |
| `spark.py` / `spark_daemon_decoded.py` | `spark` is the host CLI (`status`/`start`/`stop`/`restart`); `spark_daemon_decoded.py` is the mTLS daemon listening on port `9099` that actually executes remote commands, owns the boot autostart sequence, and runs the service-health watchdog loop (see §3). |
| `deploy_updates.py` | Rolling update deployer, drives nodes over `paramiko`/SSH. |
| `mipha.py` | HA monitor / VM failover coordinator; also resolves DRBD split-brain and manages `linstor-db` HA promotion. |
| `valcli.py` | Primary admin CLI — VM ops, storage benchmarking, DRS, DB queries (see README §5.B for the full command list, verified against `valcli.py`'s `cmd ==` dispatch). |
| `vali.py` | VM scheduler/DRS daemon (Acropolis-equivalent); HTTP API on port `9095`. |
| `urbosa.py` / `urbosa_bootstrap.py` | L3 SDN overlay daemon + bootstrap script. |
| `gatoway.py` | L2 VLAN/bridge sync daemon. |
| `bifrost.py` | Floating VIP manager. |
| `catalyst.py` / `catcli` | Task orchestrator daemon (HTTP API on port `9091`) + its CLI. |
| `dagur.py` | Cron/scheduled-task daemon, reads job definitions from the `hydra.dagur_schedules` ScyllaDB table. |
| `daruk.py` | ScyllaDB CQL query proxy; runs as a plain `systemd` unit (not a Quadlet) that execs `podman exec` into the ScyllaDB container. |
| `mimir.py` / `mcli` / `mcli-runner` | Health-check daemon + CLI + runner helper. Mimir also surveys this node's mTLS certificate expiry every 15 minutes, independently of the leader-only schedule. |
| `impa.py` | mTLS certificate lifecycle: `status` / `plan` / `renew` / `rollback` / `selftest` for the cluster CA and the certificates it signs. Runs on the host holding `ca.key` and drives peers over SSH, not over mTLS, because renewal has to work once the certificates it repairs have expired. See [mtls_lifecycle.md](./mtls_lifecycle.md). |
| `logos.py` | Metrics/telemetry collector. |
| `lanayru.py` | Guest-Kubernetes ("Lanayru") deployer, split out of `spectrum_server.py`. |
| `check_updates.py`, `create_upgrade_zip.py`, `deploy_updates.py` | Update pipeline: check for a newer version → build an upgrade zip → push it to nodes. |
| `push_to_github.py` | Manual GitHub Contents-API uploader (reads `GITHUB_TOKEN` from the environment). |
| `test_hylia.py` | `unittest` suite for `hylia.py`. |
| `saga.py` | Metadata backup/restore (`backup`/`list`/`verify`/`restore`/`prune`/`snapshots`/`target`). Snapshots the `hydra` keyspace with `nodetool snapshot`, captures the Linstor controller DB and `/etc/hci`, and archives them to an operator-supplied external target. Talks to `cqlsh`/`nodetool` directly rather than through Daruk, because the restore path must work when the metadata layer is what is broken. `valcli backup.*` are pass-throughs. See [backup_restore.md](./backup_restore.md). |
| `nodetool` | Thin wrapper: `podman exec`s into the ScyllaDB container to run the real `nodetool`. |
| `allssh` | Fan-out mTLS command executor across all cluster nodes. |
| `agahnim/` (Rust) | WebSocket console-proxy sidecar for VNC/SPICE, runs as a native `systemd` unit built from source on each node (**not** a Quadlet container — see the note in §3). |

## 3. How the system actually starts

Boot is owned by `spark-daemon` (`spark_daemon_decoded.py`), not by plain `systemd` dependency ordering. Its `main()` runs an `[AUTOSTART]` sequence before ever calling `serve_forever()`:

1. Clean up any stale local libvirt VM state.
2. Bail out early if `/run/hci/cluster_operation.lock` exists (an operation like `cluster create`/`stop` is in progress) or `/etc/hci/cluster.json` is missing (unprovisioned host — leave workloads stopped).
3. If `/etc/hci/maintenance.state` exists, start only ZooKeeper/database/storage-tier services and keep compute workloads down.
4. Start local `zookeeper`, then poll it (`stat` 4-letter word over TCP `2181`) until it reports `follower`/`leader`/`standalone` mode (quorum established).
5. Query the ZooKeeper znode `/cluster_state`. If it isn't `"started"`, leave the rest of the stack stopped.
6. If it is `"started"`, start every remaining service (`hydra-db`, `daruk`, `aether`, `spectrum`, `bifrost`, `dagur`, `mimir`, `vali`, `catalyst`, `gatoway`, `logos`, `mipha`, and `urbosa` if SDN is enabled) and attempt a local settings sync.

After autostart, the same function drops into a `while True` **watchdog loop** (`sleep(30)`) that re-checks quorum/cluster-state and calls `systemctl start <svc>` on anything not `active`/`activating` — this is the current (systemd-unit-level, not HTTP-health-level) answer to "what restarts a hung daemon."

All of this happens per-node — there is no separate orchestrator process deciding what runs where; every `spark-daemon` independently converges its own host to the same target state by reading shared ZooKeeper/ScyllaDB state.

**Known inconsistency to watch for**: the PID-resolution helper inside `spark_daemon_decoded.py` (used for `spark status`) classifies `agahnim` under `container_svcs` and resolves its PID via `podman top systemd-agahnim hpid` — but `agahnim` is actually deployed as a plain `systemd` unit (`/etc/systemd/system/agahnim.service`, built with `cargo build --release` directly on the node), not a Podman Quadlet. That `podman top` call will silently fail (wrapped in a broad `except`) and report an empty PID list for `agahnim` even while it's running fine. Don't use this function as evidence that `agahnim` is a container.

## 4. State flow

```
Client → Slate (Traefik, :443) → Spectrum (spectrum_server.py, WebUI/API)
                                        │
                                        ▼
                          Catalyst task queue (catalyst.py, :9091)
                                        │
                        ┌───────────────┼────────────────┐
                        ▼               ▼                ▼
                    Vali (:9095)     Dagur           Mipha / Bifrost / ...
                  (VM scheduling)  (cron jobs)      (HA / VIP / networking)
                        │
                        ▼
              spark-daemon (mTLS, :9099) on the target host
                        │
                        ▼
              libvirt/KVM, DRBD/Linstor, ScyllaDB (via Daruk proxy)
```

* **ScyllaDB** (`hydra-db`, via the `daruk.py` CQL proxy) and **ZooKeeper** (`zookeeper`) are the two sources of truth: ScyllaDB holds cluster/VM/task/scheduling state (keyspace `hydra`), ZooKeeper holds leader election and small coordination znodes (e.g. `/cluster_state`).
* Long-running operations (VM migrate, host maintenance, etc.) are queued as **Catalyst tasks**; Catalyst's HTTP API (`:9091`) is polled by long-poll `GET /api/v1/queues/<service>` clients and workers, and tasks are persisted to ScyllaDB for status tracking (`catcli status <task_uuid>`).
* **Vali** (`:9095`) is the VM placement/DRS scheduler; it runs its own periodic DRS loop plus an HTTP API, and calls into `spark-daemon` on remote hosts to actually execute `virsh`/libvirt commands.
* Note: Catalyst (`:9091`) and Vali (`:9095`) both run in Quadlet containers with `Network=host` and bind `0.0.0.0` directly — they are reachable from the wider cluster network, not restricted to loopback, and have no request-level authentication (see [docs/audit_findings.md](./audit_findings.md) and [TODO.md](../TODO.md)).

## 5. Build / test commands that actually exist

* **Run the test suite**: `python -m unittest discover -p 'test_*.py'`, or one file at a time, e.g. `python -m unittest test_hylia` or `python -m unittest test_saga_backup` (46 tests covering backup/restore, retention and artefact integrity). The root `test_*.py` files are plain `unittest`; several use hand-written fakes rather than mocking, so they run with no cluster and no third-party packages.
* **Syntax-check a script after editing it**: `python -m py_compile <file>.py` (no linter or formatter config exists in the repo).
* **Build the Rust console proxy**: `cd agahnim && cargo build --release` (this is exactly what `provision.py` does remotely on each node — there's no prebuilt binary checked in).
* **Build the Spectrum container image**: see §6 — do not `podman build` straight off the checked-out `Dockerfile` in this repo root; it expects a `server.py` that only exists in the temporary build context `provision.py`/`deploy_updates.py` construct.
* There is no CI (no `.github/workflows`) — all of the above are run manually today.

## 6. The `provision.py` / `sync_provision.py` / `*_B64` embedding relationship

`provision.py` is a single ~1.3MB file that is mostly base64-encoded copies of 21 other repo files (`catcli`, `catalyst.py`, `vali.py`, `valcli.py`, `dagur.py`, `mimir.py`, `cluster_new.py`, `spark.py`, `spark_daemon_decoded.py`, `spectrum_server.py`, `Dockerfile`, `gatoway.py`, `urbosa.py`, `logos.py`, `mipha.py`, `urbosa_bootstrap.py`, `daruk.py`, `bifrost.py`, `mcli`, `mcli-runner`, `hylia.py` — see the `mapping` dict at the top of `sync_provision.py` for the authoritative list), stored as `SOMETHING_B64 = "<base64>"` string constants. During provisioning, `provision.py` base64-decodes these constants and writes the resulting files out to new cluster nodes.

**Never hand-edit the `*_B64` strings inside `provision.py` directly.** Instead:
1. Edit the real source file (e.g. `vali.py`).
2. Run `python sync_provision.py` from the repo root. It reads the mapping table at the top of `sync_provision.py` (var name → source file path), re-encodes each source file, and does an in-place regex replacement of the matching `VAR_NAME = "..."` constant in `provision.py`, then writes `provision.py` back out.

`sync_provision.py` used to also contain a large block (roughly its first 260 lines, since removed) of one-time "inject this declaration/deploy-block into `provision.py` if it's missing" patches, written against an older, pre-Quadlet shape of `provision.py`. Those blocks were all inert no-ops against current `provision.py` (their guard markers, or in one case their target text, no longer match anything in the file) and have been deleted — the file now does only the base64 re-sync loop.

## 7. The Dockerfile / `server.py` indirection

`Dockerfile` (repo root) does `COPY server.py .` and `CMD ["python", "-u", "server.py"]` — but there is **no `server.py` file in this repo**. This is intentional, not broken: both `provision.py` (~line 1062) and `deploy_updates.py` (~line 626-703) copy `spectrum_server.py` into a temporary build context directory (e.g. `/tmp/spectrum_build/`) **renamed to `server.py`**, alongside the `Dockerfile` (also decoded from its own `SPECTRUM_DOCKERFILE_B64` constant) and `hylia.py`, before running `podman build` against that temp directory. The resulting image is tagged `spectrum:latest`, and the build context's `server.py`/`Dockerfile`/`static/` are also copied to `/usr/local/bin/` on the host (as `/usr/local/bin/spectrum_server`) for the Quadlet container to mount and exec directly.

If you need to test the Dockerfile build locally, you must replicate this rename step yourself — `podman build .` from the repo root as checked out will fail with a missing `server.py`.

## 8. Known invariants / edge cases

Pulled from [docs/audit_findings.md](./audit_findings.md) and [docs/history/walkthrough.md](./history/walkthrough.md) — see those documents and [TODO.md](../TODO.md) for the full, current list. Highlights an agent should know before touching HA/quorum code:

* **Quorum math is size-sensitive.** ZooKeeper's voting ensemble is capped at 3 members by `cluster_new.py`/`provision.py` (nodes beyond the 3rd are configured as `ZOO_PEER_TYPE=observer`); ScyllaDB's `daruk.py` proxy runs under `ConsistencyLevel.QUORUM` by default but now falls back to `ConsistencyLevel.ONE` if a query fails due to unavailable nodes (see `daruk.py` around line 67).
* **DRBD split-brain resolution is ZooKeeper-leader-driven**: `mipha.py` uses ZK leadership as the tie-breaker for who force-demotes and reconnects with `--discard-my-data` when a resource is `StandAlone`. The `linstor-db` HA volume additionally falls back to `drbdadm primary --force linstor-db` if normal promotion fails because the previous leader is unreachable.
* **Bifrost's VIP fallback is still IP-sort-based** (`candidates.sort(); return candidates[0]`) when ZooKeeper consensus is unavailable — this is a known open split-brain risk, not yet fixed (see TODO.md).
* **Hardcoded IP fallback arrays were removed**, not just narrowed: `bifrost.py`, `catalyst.py`, `dagur.py`, `mimir.py`, `spectrum_server.py`, `valcli.py`, `vali.py`, and `mipha.py` all now fall back to a `LOCAL_IP`/`127.0.0.1`-based array instead of a hardcoded `10.10.102.x` list when `/etc/hci/cluster.json` can't be read.
* **Internal legacy codename**: "Yggdrasil" still appears in `test_hylia.py` (its temp directory names, e.g. `/tmp/yggdrasil_test_env`) and as local variable names in `provision.py`'s Hylia deployment block (`yggdrasil_cli`, `yggdrasil_svc`, around line 915). It refers to what is now called Hylia — harmless, but don't be confused into thinking there's a separate "Yggdrasil" component.
