# Spectrum (Phoenix)

The Phoenix rewrite of Spectrum, the Helios HCI web console.

This is a **strangler migration**, not a replacement. The Python
`spectrum_server.py` keeps running on port 8443 and keeps serving the console;
this app runs beside it on port 8444 and takes over routes as they are ported.
Nothing in `spectrum_phx/` modifies, restarts, or depends on the Python tier.

- Container image: `localhost/spectrum-phx:latest` (built from `Dockerfile`)
- Systemd unit: `spectrum-phx.service` (from `quadlet/spectrum-phx.container`)
- Container name: `spectrum-phx`
- Listener: `0.0.0.0:8444` (HTTP), host network namespace

---

## Local development

```sh
mix setup            # deps.get + assets.setup + assets.build
mix phx.server       # http://localhost:4000
```

`PORT` defaults to `4000` in dev and test, and to `8443` in prod — see
"Environment variables" below for why prod differs.

The app reads `/etc/hci/cluster.json` and `/etc/hci/spectrum/spectrum.env` at
boot. Neither exists on a workstation; `SpectrumPhx.Cluster.Config` logs that
and falls back to an empty configuration, so the app still boots. Spark calls
and Hydra queries will fail against a workstation, by design.

```sh
mix precommit        # compile --warnings-as-errors, deps.unlock --unused, format, test
```

---

## Building the image

```sh
podman build -t localhost/spectrum-phx:latest spectrum_phx/
```

Two stages:

| Stage   | Image                                                        | Why |
| ------- | ------------------------------------------------------------ | --- |
| build   | `docker.io/hexpm/elixir:1.17.3-erlang-27.1.2-alpine-3.20.3`   | Verified to pull on the target nodes. |
| runtime | `docker.io/library/alpine:3.20.3`                             | `mix release` bundles ERTS, so no Erlang install is needed. An `erlang:...-alpine` runtime would ship a second, unused copy of the same VM. |

**The two Alpine versions must stay in lockstep.** The release carries ERTS
binaries linked against the builder's musl and OpenSSL. Landing them on a
different Alpine minor is how you get a crypto NIF that will not load.

The build needs network access to hex.pm, github.com (the `heroicons` and
`daisyui` deps are git checkouts, and the tailwind/esbuild standalone binaries
are downloaded) and registry.npmjs.org. Layers are ordered so that editing
`lib/` or `assets/` re-runs neither `deps.get` nor the tailwind/esbuild
download.

For an air-gapped node, build once somewhere with network and ship the image:

```sh
podman save localhost/spectrum-phx:latest -o spectrum-phx.tar
# copy, then on the node:
podman load -i spectrum-phx.tar
```

### Running the image directly

```sh
podman run --rm --network host \
  -e SECRET_KEY_BASE="$(openssl rand -base64 48 | tr -d '[:space:]')" \
  -e PORT=8444 \
  -e PHX_HOST=localhost \
  localhost/spectrum-phx:latest
```

The image sets `PHX_SERVER=true`, so no `bin/server` wrapper is needed. Its
default user is the unprivileged `spectrum` (uid 10001); the deployed Quadlet
overrides that with `User=root` for one specific reason (see "Privileges").

---

## Environment variables

Read at boot by `config/runtime.exs`, never baked into the image.

| Variable | Required | Default | Notes |
| --- | --- | --- | --- |
| `SECRET_KEY_BASE` | **yes**, in prod | — | Signs the session cookie. Must be **identical on every node**: Slate can land the next request from one browser on a different hypervisor. Boot raises loudly if unset. |
| `PORT` | no | `8443` prod, `4000` dev/test | The Quadlet sets `8444` so this can run beside the Python tier. |
| `PHX_HOST` | no | `localhost` | Host used for URL generation and as the first entry of the Origin allow-list. Set it to the cluster VIP or its DNS name. |
| `PHX_SERVER` | no | set to `true` in the image | Gates whether the release starts the web server, so `bin/spectrum_phx eval` can run without binding a port. |
| `PHX_BIND_IP` | no | `0.0.0.0` | The container uses `Network=host`, so this binds the hypervisor's real interfaces. Set `127.0.0.1` to make the port reachable only via Slate. |
| `PHX_EXTRA_ORIGINS` | no | — | Comma-separated extra hosts for LiveView's Origin check (VIP, node IPs, alternate DNS names). |
| `PHX_CHECK_ORIGIN` | no | — | `false` to disable the Origin check, `true` to compare against the endpoint URL only, or a comma-separated override list. |
| `SPECTRUM_TLS_PORT` | no | — | Set to enable a second, TLS-terminating listener. See "Cutting over". |
| `SPECTRUM_TLS_CERT` | no | `/etc/hci/spectrum/certs/server.crt` | Only read when `SPECTRUM_TLS_PORT` is set. Boot raises if the file is missing. |
| `SPECTRUM_TLS_KEY` | no | `/etc/hci/spectrum/certs/server.key` | Same. |
| `DNS_CLUSTER_QUERY` | no | — | Enables `DNSCluster`, and with it Erlang distribution (see `rel/env.sh.eex`). Not needed today. |
| `RELEASE_DISTRIBUTION` | no | `none` | Set by `rel/env.sh.eex`. Keeps EPMD off the host network. |
| `RELEASE_DIST_PORT` | no | `9199` | Only used when distribution is enabled. |

On a node these live in `/etc/hci/spectrum/spectrum-phx.env`, mode 0600, which
the Quadlet reads via `EnvironmentFile=`:

```sh
SECRET_KEY_BASE=...
PHX_HOST=hci.example.internal
PHX_EXTRA_ORIGINS=10.0.0.50,10.0.0.11,10.0.0.12,10.0.0.13
```

Generate the secret **once per cluster** and copy the same value to every node:

```sh
openssl rand -base64 48 | tr -d '[:space:]'
```

---

## Deploying alongside the existing Spectrum

Nothing in `provision.py` or `deploy_updates.py` knows about this app yet
(see "Wiring still to be done"). Until it does, per node:

```sh
# 1. Get the image onto the node (build there, or podman load a saved tar)
podman build -t localhost/spectrum-phx:latest /tmp/spectrum_phx_build

# 2. Per-cluster secrets and identity
install -d -m 0755 /etc/hci/spectrum
cat > /etc/hci/spectrum/spectrum-phx.env <<'EOF'
SECRET_KEY_BASE=<the same value on every node>
PHX_HOST=<cluster VIP or DNS name>
PHX_EXTRA_ORIGINS=<node IPs, comma separated>
EOF
chmod 600 /etc/hci/spectrum/spectrum-phx.env

# 3. The unit
cp quadlet/spectrum-phx.container /etc/containers/systemd/spectrum-phx.container
systemctl daemon-reload
systemctl start spectrum-phx

# 4. Check
systemctl status spectrum-phx
journalctl -u spectrum-phx -f
curl -sS http://127.0.0.1:8444/robots.txt
```

The Python `spectrum.service` is untouched throughout. Both containers can be
running at the same time; they share `/etc/hci/spectrum`, `/etc/hci`,
`/root/.certs` and `/var/lib/hci/aether/volumes` read-side with no conflict.

To roll back, `systemctl stop spectrum-phx` and delete the unit file. Slate was
never pointed at it, so there is nothing else to undo.

### Reaching it through Slate

Slate currently routes everything to the Python backend:

```yaml
# slate_config/dynamic.yml
spectrum-backend:
  loadBalancer:
    servers:
      - url: "https://127.0.0.1:8443"
    serversTransport: insecureTransport
```

To send *some* traffic here while leaving the rest alone, add a service and a
higher-priority router for the ported paths only:

```yaml
http:
  routers:
    spectrum-phx:
      rule: "PathPrefix(`/hosts`) || PathPrefix(`/vms`)"
      priority: 100
      entryPoints: [websecure]
      service: spectrum-phx-backend
      tls: {}
  services:
    spectrum-phx-backend:
      loadBalancer:
        servers:
          - url: "http://127.0.0.1:8444"
```

Note the scheme: `http://`, not `https://`. See below.

### Cutting over

The Python backend terminates TLS itself, which is why Slate dials
`https://127.0.0.1:8443` with `insecureSkipVerify`. This app serves **plaintext
HTTP** by default. There are two ways to finish the migration:

1. **Point Slate at HTTP.** Change the `spectrum-backend` service URL to
   `http://127.0.0.1:8444`. Simplest. Traffic between Slate and the app is
   plaintext on loopback of the same host, which is not buying an attacker
   anything they could not already get from the host.

2. **Terminate TLS here too.** Set `SPECTRUM_TLS_PORT=8443` in
   `spectrum-phx.env` after the Python container has been stopped; the app then
   serves the same `/etc/hci/spectrum/certs/server.{crt,key}` pair on the same
   port, and `dynamic.yml` needs no change at all. `config/runtime.exs` raises
   at boot if the certificate or key is unreadable.

`force_ssl` is set at compile time in `config/prod.exs` with
`rewrite_on: [:x_forwarded_proto]`, so plain HTTP behind Slate does not
redirect-loop — Traefik sets that header. It also excludes the hosts
`localhost` and `127.0.0.1`, which is what lets the health probe use plain HTTP.

---

## Privileges

`quadlet/spectrum-phx.container` deliberately does **not** carry
`PodmanArgs=--privileged`, which the Python `spectrum.container` does.

The app makes outbound TLS calls, reads config and certificates, and binds one
port above 1024. It never manipulates the host — no devices, no kernel modules,
no mounts, no namespaces, no writes to `/sys`. So:

- `DropCapability=ALL` — zero capabilities, including podman's default set.
  Reading `/root/.certs/client.key` (mode 0600, owner root) needs none of them,
  because the process runs as uid 0 and matches the file owner.
- `NoNewPrivileges=true`
- `ReadOnly=true` — nothing is written inside the image. `RELEASE_TMP` points at
  `/tmp`, which podman backs with a tmpfs. This is load-bearing: the release
  start script writes its resolved `sys.config` there on every boot.
- `User=root` — the one concession. `/root/.certs/client.key` is 0600 and
  owned by root on the host, so an unprivileged uid cannot read it. The image's
  own default user is `spectrum` (uid 10001).
- **SELinux confinement stays on.** This is the thing `--privileged` quietly
  turns off, and the reason the Python container appears to need it: a confined
  `container_t` cannot read `/etc/hci` (`etc_t`) or `/root/.certs`
  (`admin_home_t`). The Quadlet mounts them `ro,z` instead, which is exactly
  what `deploy_updates.py` already does for the Python container.

If a cluster cannot tolerate relabelling `/etc/hci` and `/root/.certs`, drop the
`,z` from those two mounts and add `SecurityLabelDisable=true`. That is still
far weaker than `--privileged`: capabilities, device access, seccomp and the
masked `/proc` paths all stay enforced.

`/var/lib/hci/aether/volumes` is mounted `rslave` and is deliberately **not**
relabelled — it holds live VM disk images carrying `svirt_image_t`, and
rewriting those to `container_file_t` would stop libvirt opening them. Nothing
ported so far touches that path.

### Erlang distribution

`rel/env.sh.eex` sets `RELEASE_DISTRIBUTION=none` unless `DNS_CLUSTER_QUERY` is
set. With `Network=host`, the default `sname` distribution would start EPMD on
`0.0.0.0:4369` plus a listener on a random ephemeral port, on the node's real
interfaces including the storage network, guarded only by a cookie baked into
the image. Nothing in this app clusters over distribution.

That does mean `bin/spectrum_phx remote` will not attach. To debug, restart the
container with distribution on:

```sh
systemctl stop spectrum-phx
podman run --rm -it --network host \
  --env-file /etc/hci/spectrum/spectrum-phx.env \
  -e RELEASE_DISTRIBUTION=name -e RELEASE_NODE=spectrum_phx@127.0.0.1 \
  -v /etc/hci:/etc/hci:ro -v /root/.certs:/root/.certs:ro \
  localhost/spectrum-phx:latest /app/bin/spectrum_phx remote
```

---

## What is and is not ported

Ported so far (this list moves fast — check `lib/spectrum_phx_web/router.ex`):

| Route | Module | Replaces |
| --- | --- | --- |
| `/` | `SpectrumPhxWeb.Cluster.OverviewLive` | cluster dashboard |
| `/hosts` | `SpectrumPhxWeb.Cluster.HostsLive` | host list |
| `/vms` | `SpectrumPhxWeb.Vms.IndexLive` | VM list |
| `/vms/new` | `SpectrumPhxWeb.Vms.NewLive` | VM creation |
| `/vms/:name` | `SpectrumPhxWeb.Vms.ShowLive` | VM detail |

Supporting layers: `SpectrumPhx.Cluster.Config` (`/etc/hci/cluster.json`),
`SpectrumPhx.Hydra` (ScyllaDB via Xandra, prepared statements only),
`SpectrumPhx.Spark` (mTLS control plane on :9099), `SpectrumPhx.Zk`
(ZooKeeper-backed cluster state), `SpectrumPhx.Vms`.

**Not** ported, and still served only by the Python tier:

- Authentication and sessions — this app has no login. Do not expose port 8444
  outside the host until that lands. `PHX_BIND_IP=127.0.0.1` is a reasonable
  interim guard.
- VNC/SPICE console proxying — still Agahnim on :8081, routed by Slate.
- Storage, networking, images/ISOs, backups, users, cluster lifecycle, updates,
  alerting, and everything else `spectrum_server.py` serves.
- The whole JSON API surface (`/api/...`). The `:api` pipeline exists in the
  router but is unused.
- `/var/lib/hci/aether/volumes` is mounted but nothing reads or writes it yet.

Because auth is not ported, the two tiers do **not** share sessions. A user
logged into the Python console is anonymous here and vice versa.

---

## Wiring still to be done

`provision.py`, `deploy_updates.py` and `sync_provision.py` have not been
touched — building and deploying this image is manual until they are updated.
In outline, they need:

- `provision.py`: a `SPECTRUM_PHX_QUADLET` entry alongside `QUADLETS["spectrum"]`,
  a build step for `localhost/spectrum-phx:latest`, generation of
  `/etc/hci/spectrum/spectrum-phx.env` with a cluster-wide `SECRET_KEY_BASE`,
  and `systemctl start spectrum-phx`.
- `deploy_updates.py`: the same unit content in its own
  `spectrum_phx_container_content`, plus upload/rebuild/restart steps that
  mirror the existing Spectrum ones.
- `sync_provision.py`: this app is a directory tree, not a single file, so it
  does not fit the existing base64-constant mapping. Either ship the image as a
  tar (like `traefik.tar`) or upload the source tree with
  `node.upload_directory`.
