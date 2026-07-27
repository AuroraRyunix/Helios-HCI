# Deployment Model

Helios-HCI is deployed as a **self-hosted, on-premises cluster** — there is no managed hosting provider or single public URL for the stack itself in this codebase; each cluster is a set of EL10.2 hypervisor hosts an operator provisions and runs on their own hardware/network (see the Quick Start in the top-level [README.md](../README.md) §2). The rest of this document describes the actual mechanism: how services are packaged and started on a node, and how updates are rolled out.

---

## 1. Podman Quadlets

Almost every Helios daemon runs as a **Podman Quadlet** — a `.container` unit file under `/etc/containers/systemd/` that `podman-system-generator` turns into a regular `systemd` unit (`systemd-<name>.service`) at boot. `provision.py` is what writes these files (grep it for `node.write_file("/etc/containers/systemd/...")` to see the exact unit content it generates). The Quadlet-managed services, verified against `provision.py`'s unit-writing code, are:

```
spark-daemon, bifrost, dagur, mimir, vali, gatoway, urbosa, logos, mipha,
catalyst, hylia, zookeeper, hydra-db, aether, spectrum, slate, linstor-controller
```

(`urbosa` is only deployed if SDN overlay is enabled for the cluster.)

Every Quadlet-managed Python daemon runs the same local image, `localhost/helios-base:latest`, with `Network=host` and a read-only bind-mount of `/usr/local/bin` (where `provision.py` wrote the actual `.py`/CLI files) so the container executes the script directly off the host filesystem rather than baking it into the image. Individual units add whatever extra mounts/capabilities they need — e.g. `vali.container` mounts `/var/run/libvirt/libvirt-sock`, `gatoway.container` adds `CAP_NET_ADMIN`, `hylia.container` bind-mounts the whole host (`/:/host:rw`) and the systemd D-Bus socket so it can drive host reboots/service management during rolling upgrades.

**Two services are intentionally *not* Quadlets** — they run as plain native `systemd` units instead:
* **`agahnim`** (`/etc/systemd/system/agahnim.service`) — the Rust console-proxy binary is `cargo build --release`'d directly on the node during provisioning and installed to `/usr/local/bin/agahnim`; the unit just execs it.
* **`daruk`** (`/etc/systemd/system/daruk.service`) — the ScyllaDB CQL proxy runs natively and `podman exec`s *into* the `systemd-hydra-db` container to reach `cqlsh`/the ScyllaDB Python driver, rather than running as its own container.

The Spectrum WebUI container image itself (`spectrum:latest` / `localhost/spectrum:latest`) is the one component actually built from a `Dockerfile` with `podman build` — see [docs/AGENTS.md](./AGENTS.md) §7 for the `server.py` copy-rename step this depends on.

## 2. Update rollout

There are two related but independent code paths for shipping updates — they share the same component list but are not literally chained (`deploy_updates.py` does not consume the zip that `create_upgrade_zip.py` produces):

1. **Self-service / Hylia-driven path**: `check_updates.py` polls `https://updates-helios.zerotwo.cloud/api/v1/releases/latest` for a `latest_version`/component-version manifest, compares it against the currently-installed `hylia.py`'s `__build__` string (and each node's installed component versions), and reports whether an update is available. `create_upgrade_zip.py` builds the actual upgrade artifact: it copies every component in its `components_map` (all the root daemons/CLIs, matching the `sync_provision.py` embedding list) into a build directory, computes a SHA-256 per file, writes a `manifest.json` (`build`, `changelog`, per-component `sha256`/`target_path`/`version`, and a `min_hylia_version` gate) plus a `changelog.md`, and zips it up as `upgrade_<version>.zip`. `hylia.py` (the LCM daemon) is what actually validates and extracts a zip like this on a target node — checksum-verifying every entry against the manifest before installing it (see `test_hylia.py` for the exact validation contract, e.g. `validate_and_extract_zip` rejecting a zip whose manifest checksum doesn't match).
2. **Direct operator rollout**: `deploy_updates.py` is a standalone `paramiko`/SSH tool run from an operator's workstation. It reads `HELIOS_NODES` (comma-separated IPs) and `HELIOS_PASSWORD` (or an SSH key at `~/.ssh/id_rsa_hci`) from the environment, connects to every node, and pushes the current local copies of the daemons/CLIs plus the Spectrum container build context directly over SFTP — including the `server.py` rename step described in [docs/AGENTS.md](./AGENTS.md) §7 — restarting the relevant Quadlet/systemd units afterward. It supports a `--fast` flag to skip slower steps. This path does not read or produce the `create_upgrade_zip.py` zip at all.

## 3. Ingress

**Slate** (Traefik, configured by `slate_config/traefik.yml` and `slate_config/dynamic.yml`) is the sole externally-facing ingress, terminating all client traffic — WebUI, REST API, and VNC/SPICE console WebSocket traffic — on port `443`, and reverse-proxying it same-origin to Spectrum (`spectrum_server.py`) and Agahnim (the console proxy) on their internal ports. No other service in the component list is meant to be exposed directly to end users; see [docs/AGENTS.md](./AGENTS.md) §4 for the caveat that Catalyst (`:9091`) and Vali (`:9095`) are nonetheless reachable on the cluster network today (Quadlet `Network=host`, no loopback restriction, no auth) — this is an open item in [TODO.md](../TODO.md), not an intended second ingress path.

For the full network flow (ports, mTLS mesh, ScyllaDB/ZooKeeper cluster-facing traffic), see the Cluster Network Architecture section and flowchart in the top-level [README.md](../README.md) §7, and [docs/network.md](./network.md).
