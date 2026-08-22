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
3. **Validate at the boundary.** Names match `\A[A-Za-z0-9][A-Za-z0-9_.-]{0,62}\Z`
   (`\Z`, not `$` -- `$` also matches before a trailing newline). Paths must resolve under
   an allowlisted root. Reject rather than sanitize.

   One carve-out is required and was found during implementation: `/dev/drbd/by-res/<res>/<vol>`
   is a symlink to `/dev/drbdNNNN`, so a plain `realpath`-must-be-under-an-allowed-root rule
   rejects *every* DRBD device and makes the storage endpoints unusable. The rule is
   therefore: the literal path must be under an allowed root **and** the realpath must be
   under an allowed root, *except* that a literal path under `/dev/drbd/` may resolve to
   `\A/dev/drbd[0-9]+\Z`. An aether-rooted symlink pointing at `/etc/shadow` is still rejected.
4. **Structured responses.** Return parsed JSON, not captured stdout. A caller that has to
   regex stdout is still coupled to the command.

   Note the two pass-through documents keep their upstream shape: `/storage/drbd/status`
   returns the top-level **array** from `drbdsetup status --json`, and `/host/disks` returns
   the `{"blockdevices": [...]}` **object** from `lsblk -J`.

5. **Error codes.** `400` for a rejected parameter, `404` for an unknown domain or resource,
   `409` when an operation did not take. A `409` still carries the state key so the caller
   learns the actual value: `/storage/drbd/role` returns `{"role": "<actual>", "error": ...}`
   and names the peer when one holds Primary; `/vm/{name}/power` returns
   `{"state": "<actual>", "error": ...}`.
6. **`/api/v1/execute` stays** during migration, and shrinks as call sites move. It is not
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
| POST | `/api/v1/storage/device/write` | `?device=` + **raw body** | `{"written":int}` |
| POST | `/api/v1/storage/device/flush` | `{"path"}` | `{"flushed":true}` |
| GET | `/api/v1/storage/container/mounted` | `?path=` | `{"mounted":bool}` |
| POST | `/api/v1/storage/container/ensure` | `{"name"}` | `{"path":str,"created":bool}` |
| GET | `/api/v1/storage/linstor/resources` | `?resource=` (optional) | `{"resources":[{"name","size_kib","size_gib","nodes","device_path"}]}` |
| POST | `/api/v1/storage/linstor/resource` | `{"resource", "size_gib"\|"size_kib", "nodes"?, "storage_pool"?, "allow_two_primaries"?}` | `{"resource","created":bool,"size_kib","device_path",...}` |
| POST | `/api/v1/storage/linstor/resource/delete` | `{"resource"}` | `{"resource","deleted":bool}` |
| GET | `/api/v1/storage/drbd/options` | `?resource=` | `{"resource","options":{...}}` |
| POST | `/api/v1/host/fence` | `{"confirm":true}` | 200 with a verification report, or 409 |

`path` must resolve under `/dev/drbd/` or `/var/lib/hci/aether/`. `owner` is an allowlist
(`root:qemu`, `root:root`), `mode` an octal string from an allowlist. Promotion returns the
resulting role so a caller can detect the peer already holding Primary -- the condition
that previously allowed a VM to start twice.

`device/write` streams the request body straight onto the device. It exists because the
web tier must not touch the data path at all. Opening a DRBD device from the console
failed with `ENOENT` and image upload had never worked in this deployment: a container's
`/dev` carries device nodes but not udev's subdirectories, so `/dev/drbd/by-res/<res>/0`
-- the form every code path uses -- does not exist inside it. Note this was never a
permissions problem, which is why running the container `--privileged` did not fix it and
could not have. Mounting `/dev` into the web tier would have been the wrong fix -- Spark owns
host storage, the way Stargate rather than Prism owns it on Nutanix. Spectrum receives the
upload and proxies the bytes here, so it needs neither `/dev` nor a storage mount.

A short write returns 400 with the byte count rather than 200, so a client that
disconnects mid-upload cannot leave a truncated image registered as valid.

Note the daemon holds the device open for the life of the write request. A caller that
abandons an upload must close the connection *before* trying to delete the resource, or
LINSTOR refuses with "resource is still in use" and the rollback leaks the storage it was
meant to reclaim. Release is asynchronous, so the delete also has to be retried; see
`SpectrumPhx.Images.rollback_upload/1`.

`drbd/options` exists because `drbdsetup status --json` reports `"quorum": true` both
when a majority is genuinely held **and when quorum is switched off entirely** -- verified
on a live cluster, where every resource reported `"quorum": true` in status while
`drbdsetup show` said `"quorum": "off"`. Storage fencing rests on quorum being armed, so
it has to read the configured options rather than the status.

`host/fence` asks a host to take itself out of service and **reads back what it produced**:
no guest process, no open DRBD device, no resource held Primary. It returns that report
rather than a bare success, because the previous fence was a shell string whose every
clause ended in `|| true` and whose exit status the caller discarded -- so a host that had
gone silent, the exact case fencing exists for, was recorded as fenced on no evidence.
A fence that cannot be confirmed is a failure; see [fencing.md](./fencing.md).

`linstor/resource` creates a resource definition, a volume definition, placement on every
node and the DRBD options as **one idempotent operation**. The four commands are
meaningless apart, so exposing them separately would move the sequencing bug into every
caller. `created: false` means the resource was already there and was adopted, which is
what makes a retry after a timeout safe. A resource that exists at a *different* size is a
`409` carrying the actual size, never a silent reuse -- that is precisely how a new VM ends
up attached to a deleted VM's disk.

Size is given in exactly one unit, and sending both is a `400` rather than a precedence
rule. VM disks are whole GiB (`size_gib`). Images are not -- an ISO is whatever size it is
-- so they use `size_kib`, which is rounded up to LINSTOR's 4 KiB alignment before use; an
unaligned request would otherwise be stored larger than asked for, and the next idempotent
create would reject its own retry as a size mismatch.

`allow_two_primaries` defaults to false and is validated as a strict JSON boolean, so a
truthy string cannot turn it on. It is correct for a golden image, which guests on several
hosts attach read-only at the same time and which is written exactly once by the upload
that creates it. It is never correct for a VM disk: dual-primary there is what let one VM
run on two hosts and corrupt itself.

### Host

| Method | Path | Body / Query | Returns |
| :-- | :-- | :-- | :-- |
| GET | `/api/v1/host/network` | -- | `{"default_interface","default_gateway","addresses":[{"interface","family","address","prefixlen","scope"}]}` |
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
| ~~Linstor resource operations~~ | ~~3~~ | **Done.** `POST /storage/linstor/resource`, `/resource/delete` and `GET /storage/linstor/resources`. See the correction below: the figure of 3 was wrong. |
| `GET /api/v1/host/cpu` | 1 | Core count and model. |
| `GET /api/v1/host/ping` | 2 | A liveness probe for the reboot task. Currently `echo 1`, and not migrated because `run_mtls_spark_api` has a 120s timeout against `run_remote_spark`'s 60s, which would change reboot detection timing. |

Two shape ambiguities in v1 worth tightening when these land: the element shape of
`/host/network`'s `addresses` array was left unspecified (a caller needing a per-interface
CIDR had to keep shelling out), and `/host/disks` returning `{"blockdevices": [...]}`
follows `lsblk -J` rather than being stated in the contract.

Remaining by design: `rm`, `echo >`, `mkdir` and base64-decode calls in the LCM
file-transfer and config-sync paths. Exposing file verbs as endpoints would reproduce
`/execute` with a JSON wrapper.

### Correction: `/execute` usage is undercounted

The headline figure of 79 raw calls counts `run_remote_spark` call sites in
`spectrum_server.py`. It misses Linstor entirely: those go through a separate
`run_linstor_cmd` wrapper, which builds `podman exec ... linstor <args>` and *then* hands
it to `run_remote_spark`, so its **21 call sites** never appear in the count. This gap was
listed as worth 3 calls; the real figure is an order of magnitude higher.

Two consequences worth carrying:

* The migration metric should count wrappers that reach `/api/v1/execute`, not only direct
  callers. Any future wrapper hides its call sites the same way.
* `/api/v1/execute` cannot be removed on the strength of the direct-call count alone.

Also unlike the other pass-through endpoints, the Linstor responses are **normalised** by
the daemon rather than returned verbatim. `drbdsetup status --json` and `lsblk -J` have
stable shapes; the Linstor client renames its own keys between output versions
(`rsc_dfns`/`rsc_name`/`vlm_size` versus `resource_definitions`/`name`/`size_kib`), so a
pass-through would push a version dependency onto every caller. An unrecognised document
yields an empty list rather than a guess.

Still requiring `/execute` after this work: `volume-definition set-size` (disk resize) and
the dual-primary toggle around live migration.

## Related

* [spark.md](./spark.md) / [spark_technical.md](./spark_technical.md) -- the daemon
* [cluster_state.md](./cluster_state.md) -- the ZooKeeper-backed state model
* [../TODO.md](../TODO.md) -- "Unsandboxed root command execution" is the item this retires
