# Spark Typed API (v1)

The contract that replaces `POST /api/v1/execute` with per-domain endpoints.

## Why

`spark-daemon` is the only component permitted to act on a hypervisor, and today its main
entry point is `/api/v1/execute`, which runs a caller-supplied string through a shell as
root. `spectrum_server.py` alone makes **79 raw execute calls against 15 typed ones**,
shelling out for `virsh` (12), `ip` (5), `rm` (4), `drbdadm` (4), plus `podman`, `reboot`
and `mkdir`.

That single fact explains a large share of this project's defect history: every shell
string is an injection sink, which is why VM names, image filenames, session tokens and
update-server values each had to be patched separately; and the web tier ends up
re-implementing host orchestration, which is why it grew to 95 endpoints.

Spark already has the right shape -- 22 typed endpoints, and `forward_to_vali()` already
brokers VM power/migrate/balance and host maintenance through to Vali. The work is to
finish that pattern and stop routing around it.

## Design rules

1. **Model domain operations, not shell verbs.** There is no `/exec/rm` or `/exec/mkdir`.
   A file removal is an implementation detail of a domain operation, never an endpoint.
   Exposing the verbs would reproduce `/execute` with extra steps.
2. **No caller-supplied command fragments.** Parameters are values (a VM name, a resource
   name, a path from a fixed allowlist), never flags or shell text.
3. **Validate at the boundary.** Names match `\A[A-Za-z0-9][A-Za-z0-9_.-]{0,62}\z`. Paths
   must resolve under an allowlisted root. Reject rather than sanitize.
4. **Structured responses.** Return parsed JSON, not captured stdout. A caller that has to
   regex stdout is still coupled to the command.
5. **`/api/v1/execute` stays** during migration, and shrinks as call sites move. It is not
   removed until the raw-call count reaches zero.

## Endpoints

All are mTLS on `:9099`, JSON in and out. Errors return
`{"error": "<message>"}` with a 4xx/5xx status.

### VM (libvirt)

| Method | Path | Body / Query | Returns |
| :-- | :-- | :-- | :-- |
| GET | `/api/v1/vm/{name}/interfaces` | -- | `{"interfaces":[{"mac","type","source","model"}]}` |
| GET | `/api/v1/vm/{name}/console` | -- | `{"graphics":"vnc"\|"spice","port":int,"listen":str}` |
| GET | `/api/v1/vm/{name}/info` | -- | `{"state","vcpus","memory_kib","autostart"}` |
| POST | `/api/v1/vm/define` | `{"name","xml_b64"}` | `{"defined":true}` |
| POST | `/api/v1/vm/undefine` | `{"name","keep_nvram":bool}` | `{"undefined":true}` |
| POST | `/api/v1/vm/{name}/power` | `{"action":"start"\|"destroy"\|"reboot"\|"shutdown"\|"reset"}` | `{"state":str}` |

`xml_b64` is base64 so domain XML never passes through a shell. The daemon decodes it to a
temp file and calls `virsh define` on that path.

### Storage (DRBD / Linstor / block devices)

| Method | Path | Body / Query | Returns |
| :-- | :-- | :-- | :-- |
| GET | `/api/v1/storage/drbd/status` | `?resource=` (optional) | parsed `drbdsetup status --json` |
| POST | `/api/v1/storage/drbd/role` | `{"resource","role":"primary"\|"secondary","force":bool}` | `{"role":str}` |
| GET | `/api/v1/storage/device` | `?path=` | `{"exists":bool,"is_block":bool,"size_bytes":int}` |
| POST | `/api/v1/storage/device/prepare` | `{"path","owner","mode"}` | `{"prepared":true}` |
| POST | `/api/v1/storage/device/flush` | `{"path"}` | `{"flushed":true}` |
| GET | `/api/v1/storage/container/mounted` | `?path=` | `{"mounted":bool}` |
| POST | `/api/v1/storage/container/ensure` | `{"name"}` | `{"path":str,"created":bool}` |

`path` must resolve under `/dev/drbd/` or `/var/lib/hci/aether/`. `owner` is an allowlist
(`root:qemu`, `root:root`), `mode` an octal string from an allowlist. Promotion returns the
resulting role so a caller can detect the peer already holding Primary -- the condition
that previously allowed a VM to start twice.

### Host

| Method | Path | Body / Query | Returns |
| :-- | :-- | :-- | :-- |
| GET | `/api/v1/host/network` | -- | `{"default_interface","default_gateway","addresses":[...]}` |
| GET | `/api/v1/host/memory` | -- | `{"total_mb","used_mb","free_mb","available_mb"}` |
| GET | `/api/v1/host/disks` | -- | parsed `lsblk -J` |
| GET | `/api/v1/host/capabilities` | -- | `{"kvm":bool,"drbd_module":bool,"secure_boot":bool}` |
| GET | `/api/v1/host/dhcp-leases` | -- | `{"leases":[{"mac","ip","hostname","expires"}]}` |
| POST | `/api/v1/host/reboot` | `{"confirm":true}` | `{"rebooting":true}` |

### Database (ScyllaDB via the hydra-db container)

| Method | Path | Body / Query | Returns |
| :-- | :-- | :-- | :-- |
| GET | `/api/v1/db/ring` | -- | `{"nodes":[{"address","status","state","load","tokens"}]}` |
| POST | `/api/v1/db/repair` | `{"keyspace":"hydra","primary_range":bool}` | `{"started":true}` |

`repair` runs asynchronously and returns immediately; it can take a long time on a large
keyspace and must not block an HTTP request.

## Migration

Each call site moves from `run_remote_spark(ip, "<shell string>")` to
`run_mtls_spark_api(ip, "<path>", payload, method=...)`. The raw-call count is the metric:
79 today in `spectrum_server.py`. `/api/v1/execute` is removed when it reaches zero across
`spectrum_server.py`, `vali.py`, `hylia.py`, `mipha.py` and `cluster_new.py`.

Note this work is not contingent on the Phoenix rewrite: it improves the Python tier
directly, and the Elixir client consumes the same contract.

## Known gaps (v2 candidates)

Identified while migrating `spectrum_server.py`; these call sites deliberately still use
`/api/v1/execute` because no typed endpoint covers them. Listed with the count of raw
calls each would retire.

| Missing endpoint | Raw calls | Why it is needed |
| :-- | --: | :-- |
| `GET /api/v1/vms` (list defined domains) | 3 | The reconcile loop must *enumerate* locally-defined VMs to find ones the database assigns elsewhere. A per-name lookup cannot substitute: a 404 conflates "not defined" with a transient error. |
| `GET /api/v1/vm/{name}/stats` | 2 | `cpu.time`, `balloon.rss`, `block.*` counters. `/vm/{name}/info` carries none of them. |
| VM media (CD-ROM change/eject) | 10 | `virsh change-media`, `qemu-monitor-command`. The largest single group remaining. |
| VM device hotplug (attach/detach NIC and disk) | 5 | `virsh attach-device`, `detach-interface`, `attach-disk`, `detach-disk`. |
| `POST /api/v1/vm/{name}/disk/resize` | 1 | `virsh blockresize`. |
| `drbdadm resize` on `/storage/drbd/` | 1 | `/storage/drbd/role` covers only primary/secondary. |
| Linstor resource operations | 3 | `resource-definition`/`volume-definition`/`resource create`. Blocks moving VM disk allocation out of the web tier. |
| `GET /api/v1/host/cpu` | 1 | Core count and model. |
| `GET /api/v1/host/ping` | 2 | A liveness probe for the reboot task. Currently `echo 1`, and not migrated because `run_mtls_spark_api` has a 120s timeout against `run_remote_spark`'s 60s, which would change reboot detection timing. |

Two shape ambiguities in v1 worth tightening when these land: the element shape of
`/host/network`'s `addresses` array was left unspecified (a caller needing a per-interface
CIDR had to keep shelling out), and `/host/disks` returning `{"blockdevices": [...]}`
follows `lsblk -J` rather than being stated in the contract.

Remaining by design: `rm`, `echo >`, `mkdir` and base64-decode calls in the LCM
file-transfer and config-sync paths. Exposing file verbs as endpoints would reproduce
`/execute` with a JSON wrapper.

## Related

* [spark.md](./spark.md) / [spark_technical.md](./spark_technical.md) -- the daemon
* [cluster_state.md](./cluster_state.md) -- the ZooKeeper-backed state model
* [../TODO.md](../TODO.md) -- "Unsandboxed root command execution" is the item this retires
