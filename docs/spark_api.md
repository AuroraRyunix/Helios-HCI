# Spark Typed API (v1)

The contract that replaces `POST /api/v1/execute` with per-domain endpoints.

## Why

`spark-daemon` is the only component permitted to act on a hypervisor, and today its main
entry point is `/api/v1/execute`, which runs a caller-supplied string through a shell as
root. `spectrum_server.py` alone makes **79 raw execute calls against 15 typed ones**,
shelling out for `virsh` (12), `ip` (5), `rm` (4), plus `podman`, `reboot`
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

   One carve-out used to be required and no longer is. `/dev/drbd/by-res/<res>/<vol>` was
   a symlink to `/dev/drbdNNNN`, so a plain realpath-under-an-allowed-root rule rejected
   *every* DRBD device; the exception that allowed it was the only place a device node was
   reachable at all. There are no device nodes in the allow-list now -- a vdisk is a unix
   socket under `/var/lib/hci/sidon/nbd/` -- so the rule is
   therefore: the literal path must be under an allowed root **and** the realpath must be
   under an allowed root, with no exception. A symlink under one of those roots pointing
   at `/etc/shadow` is still rejected.
4. **Structured responses.** Return parsed JSON, not captured stdout. A caller that has to
   regex stdout is still coupled to the command.

   Note the pass-through document keeps its upstream shape: `/host/disks` returns
   the `{"blockdevices": [...]}` **object** from `lsblk -J`.

5. **Error codes.** `400` for a rejected parameter, `404` for an unknown domain or resource,
   `409` when an operation did not take. A `409` still carries the state key so the caller
   learns the actual value: a refused `attach` returns `409` with the host that owns the
   vdisk named in the body
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

### Storage (Sidon vdisks, files and block devices)

| Method | Path | Body / Query | Returns |
| :-- | :-- | :-- | :-- |
| POST | `/api/v1/dfs/vdisk` | `{"op": ..., "vdisk_id"?: ..., ...}` | whatever Sidon answers |
| POST | `/api/v1/dfs/write` | `?vdisk=` + **raw body** | `{"written":int}` |
| GET | `/api/v1/storage/device` | `?path=` | `{"exists":bool,"is_block":bool,"size_bytes":int}` |
| POST | `/api/v1/storage/device/prepare` | `{"path","owner","mode"}` | `{"prepared":true}` |
| POST | `/api/v1/storage/device/write` | `?device=` + **raw body** | `{"written":int}` |
| POST | `/api/v1/storage/device/flush` | `{"path"}` | `{"flushed":true}` |
| GET | `/api/v1/storage/container/mounted` | `?path=` | `{"mounted":bool}` |
| POST | `/api/v1/storage/container/ensure` | `{"name"}` | `{"path":str,"created":bool}` |
| POST | `/api/v1/host/fence` | `{"confirm":true}` | 200 with a verification report, or 409 |

`path` must resolve under `/var/lib/hci/aether/`, `/var/lib/hci/sidon/` or
`/var/lib/hci/images/`. `owner` is an allowlist (`root:qemu`, `root:root`), `mode` an
octal string from an allowlist.

#### `/api/v1/dfs/vdisk`

Fronts Sidon's unix control socket. An **allow-list**, not a pass-through: forwarding
whatever arrives would make this endpoint exactly as powerful as the socket it fronts,
which is the reason for fronting it. Two groups:

| Group | Operations | `vdisk_id` |
| :-- | :-- | :-- |
| Per vdisk | `create` `attach` `detach` `delete` `status` `flush` `seal` `resize` | required |
| Per node | `list` `ping` `capacity` `peers` `purah-sweep` `purah-scrub` `purah-heal` | not taken |

The split is load-bearing and is pinned by `test_dfs_endpoint.py`. It was once written as
"everything except `list` and `ping` needs a `vdisk_id`", which refused `capacity`,
`peers` and the three Purah jobs outright -- and since everything that asks a node how
much room it has goes through here, and every caller's behaviour on no answer is to be
cautious, that silently made hylia refuse every maintenance exit, made vali's migration
capacity gate refuse every migration, and made the console render a cluster with no
storage in it. Nothing raised anywhere.

`attach` is the ownership operation and the one whose refusal matters: it wins the
`(owner, epoch)` compare-and-swap in Hydra and fences every replica at the new epoch, so
a `409` means another host holds the disk and the body names it. `409` means the answer
will not change on a retry; `503` means it might.

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

Note the daemon keeps the vdisk attached for the life of the write request. A caller that
abandons an upload must close the connection *before* trying to delete it, or the delete
is refused and the rollback leaks the storage it was meant to reclaim. Release is
asynchronous, so the delete also has to be retried; see
`SpectrumPhx.Images.rollback_upload/1`.

`/api/v1/dfs/write` is the same idea as `/storage/device/write` and a smaller thing to
trust: the device form takes a path and checks it against an allow-list, while this one
takes a *vdisk name* and derives the socket itself, so a caller cannot name a file at all.

`host/fence` asks a host to take itself out of service and **reads back what it produced**:
no guest process, no vdisk still attached. It returns that report
rather than a bare success, because the previous fence was a shell string whose every
clause ended in `|| true` and whose exit status the caller discarded -- so a host that had
gone silent, the exact case fencing exists for, was recorded as fenced on no evidence.
A fence that cannot be confirmed is a failure; see [fencing.md](./fencing.md).

`create` allocates a vdisk, and it is a metadata operation: a row and a block map, sparse,
with nothing written until a guest writes. It returns in milliseconds, where the LINSTOR
placement it replaced built a kernel object on every node and needed a four-minute
timeout. Size is in bytes, with no alignment to round to -- the DRBD path rounded to whole
KiB and then to DRBD's own 4 KiB, which made an idempotent retry compare unequal and
reject itself as a size conflict.

An existing vdisk of the same name is refused rather than adopted. Adoption was safe when
a create was idempotent-by-adoption; refusing is safer, because a create that adopts is
also a rollback that deletes someone else's live disk.

`seal` is what replaced `--allow-two-primaries`. That flag existed because a golden image
is attached read-only by guests on several hosts at once, and DRBD required each of those
hosts to hold Primary in order to read -- exactly the state that corrupts a device the
moment anything writes. A sealed vdisk cannot reach it: it is permanently immutable, reads
need no lease, and writes are refused by class at the NBD layer. The seal drains first,
because the drain is itself a write path and a vdisk frozen around an undrained journal
could never finish draining it.

| GET | `/api/v1/host/capabilities` | -- | `{"kvm":bool,"secure_boot":bool}` |
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
| ~~`drbdadm resize`~~ | ~~1~~ | **Gone with DRBD.** A vdisk is sparse and its map is keyed by extent index, so growing one changes a recorded size and nothing else; `resize` on `/api/v1/dfs/vdisk` covers it, and only qemu needs telling afterwards. |
| ~~Linstor resource operations~~ | ~~3~~ | **Gone with LINSTOR.** Every storage operation goes through `/api/v1/dfs/vdisk`, and the figure of 3 was wrong in a way worth keeping — see the correction below. |
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

The headline figure of 79 raw calls counted `run_remote_spark` call sites in
`spectrum_server.py`. It missed LINSTOR entirely: those went through a separate
`run_linstor_cmd` wrapper, which built `podman exec ... linstor <args>` and *then* handed
it to `run_remote_spark`, so its **21 call sites** never appeared in the count. The gap
was listed as worth 3 calls; the real figure was an order of magnitude higher.

Both the wrapper and the calls are gone, and the lesson is the part worth keeping:

* The migration metric must count wrappers that reach `/api/v1/execute`, not only direct
  callers. Any future wrapper hides its call sites the same way.
* `/api/v1/execute` cannot be removed on the strength of the direct-call count alone.

The normalisation argument outlived the thing it was about. `lsblk -J` has a stable shape
and is passed through; the LINSTOR client renamed its own keys between output versions
(`rsc_dfns`/`rsc_name`/`vlm_size` versus `resource_definitions`/`name`/`size_kib`), so
those responses had to be normalised or every caller inherited a version dependency.
Sidon's control socket answers a JSON document this repository defines, which removes the
question rather than answering it — the shape cannot drift out from under a caller,
because nothing upstream owns it.

Nothing storage-related requires `/execute` any more.

## Related

* [spark.md](./spark.md) / [spark_technical.md](./spark_technical.md) -- the daemon
* [cluster_state.md](./cluster_state.md) -- the ZooKeeper-backed state model
* [../TODO.md](../TODO.md) -- "Unsandboxed root command execution" is the item this retires
