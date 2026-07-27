# Local Development Setup Guide

This covers what you need on a dev machine to work on this repo, and what actually works standalone versus what requires a real (or single-node) cluster.

---

## 1. Prerequisites

* **Python 3.** No version is pinned anywhere in the repo (no `requirements.txt`, no `pyproject.toml`). The one place a specific version is named is `Dockerfile`, which builds the Spectrum container image `FROM docker.io/library/python:3.11-slim` — treat 3.11 as a reasonable baseline. In production, scripts run under whatever Python 3 ships on the EL10.2 host image.
* **Python dependencies**: not pinned anywhere. Reading the imports across the root scripts, you'll need at least:
  * `paramiko` (used by `deploy_updates.py` and parts of `provision.py`'s `RemoteNode` SSH wrapper)
  * `cassandra-driver` (imported as `from cassandra.cluster import Cluster` in `daruk.py`)
  * everything else used (`http.server`, `subprocess`, `socket`, `ssl`, `threading`, `zipfile`, `hashlib`, `json`, etc.) is Python stdlib.
  
  This gap is tracked in [TODO.md](../TODO.md) — there is currently no reliable, reproducible way to install matching dependencies on a fresh machine other than reading the `import` statements yourself.
* **Rust toolchain** (stable, 2021 edition) if you're touching `agahnim/` — it's a small single-file Tokio project (`agahnim/src/main.rs`, `agahnim/Cargo.toml`). Build/check it with:
  ```bash
  cd agahnim
  cargo build --release      # matches exactly what provision.py runs on each node
  cargo check                # faster, for iterating without a full build
  ```
* **Podman** (with the Quadlet/`podman-system-generator` support your distro ships) only if you actually intend to test `.container` unit files locally — this repo's Quadlet units assume EL10.2-style paths (`/etc/containers/systemd/`) and several assume they're running on a real hypervisor host (libvirt socket mounts, `/var/lib/hci/...` paths), so testing them meaningfully generally means testing against a real or VM-based EL10.2 node, not a laptop container runtime.
* **libvirt/QEMU/DRBD/Linstor** are only needed if you're standing up an actual cluster node — see the Quick Start in the top-level [README.md](../README.md) §2 and the Secure Boot warning at the top of that file (DRBD is an out-of-tree kernel module).

## 2. What actually runs standalone

* **`test_hylia.py`** — the one thing in this repo that runs cleanly with zero external services:
  ```bash
  python -m unittest test_hylia.py
  ```
  This exercises `hylia.py`'s update-package validation logic (`validate_and_extract_zip`: manifest parsing, SHA-256 checksum verification, corrupt-zip rejection) using temp files under `/tmp/yggdrasil_test_env` / `/tmp/yggdrasil_test_extract` (see [docs/AGENTS.md](./AGENTS.md) for the "Yggdrasil" legacy-codename note) — no network, database, or ZooKeeper dependency.
* **Syntax-checking any script you edit**:
  ```bash
  python -m py_compile <file>.py
  ```
  There is no linter or formatter configured in the repo (no `.flake8`, `pyproject.toml`, `ruff.toml`, etc.) — `py_compile` is the only automated correctness check available short of the unit tests above.
* **`sync_provision.py`**, after editing one of the embedded daemons/CLIs (see [docs/AGENTS.md](./AGENTS.md) §6):
  ```bash
  python sync_provision.py
  ```
  This only touches local files (reads the source file you edited, rewrites the matching `*_B64` constant in `provision.py`) — no cluster access required. Run `python -m py_compile provision.py` afterward to make sure the rewrite didn't corrupt anything.

## 3. What does *not* run standalone

Almost every other daemon connects to something at import time or immediately in `main()`:

* **`spectrum_server.py`**: its `main()` calls `init_db()` on startup, which expects a reachable database path (normally the local Daruk proxy at `http://127.0.0.1:9043/query`, itself backed by ScyllaDB). Running it on a bare dev machine will fail or hang at startup unless you have at least a local ScyllaDB + Daruk proxy (or a full/single-node cluster) available.
* **`daruk.py`**: connects to ScyllaDB at module scope (`connect_db()`, with a 30-attempt/2-second retry loop) — needs a real ScyllaDB instance reachable at the detected local IP.
* **`vali.py`**, `catalyst.py`, `mimir.py`, `dagur.py`, etc.: all read `/etc/hci/cluster.json` and/or talk to ZooKeeper/ScyllaDB/`spark-daemon` on startup or on their first scheduler tick.

Practically, meaningful local iteration on these means either: (a) reading/editing the code and relying on `py_compile` + targeted unit tests, or (b) standing up a real (ideally single-node, `redundancy_factor=0` or `1`) Helios-HCI cluster per the Quick Start in the README and iterating against it via `deploy_updates.py`/`sync_provision.py`, since there is no lightweight local-mock mode built into any of these daemons.

## 4. Known gaps for a new contributor

* No `requirements.txt` (see above).
* No CI (no `.github/workflows`) — nothing runs automatically on push/PR; the checks in this guide are the same ones you'd need to run by hand before shipping a change.
* No linter/formatter configuration.
* `provision.py` and `deploy_updates.py` are the two scripts that actually touch a live cluster over SSH — be deliberate about pointing `HELIOS_STATIC_IPS`/`HELIOS_NODES` at a real target before running them.
