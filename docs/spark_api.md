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

Spark already has the right shape -- **28 typed paths**, counted off the router in
`spark_daemon_decoded.py` rather than off the table below, which had drifted behind it --
and `forward_to_vali()` already brokers VM power/migrate/balance and host maintenance
through to Vali. The work is to finish that pattern and stop routing around it.

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
| POST | `/api/v1/lcm/package` | **raw body** (no parameters) | `{"path":str,"written":int}` |

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
| Per node | `list` `ping` `capacity` `peers` `purah-sweep` `purah-scrub` `purah-heal` `purah-heat` `purah-tier` `purah-move` `purah-placement` `purah-compact` `purah-dedup` | not taken |

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

`/api/v1/lcm/package` takes that one step further and has **no parameters at all**. It
streams the body into one fixed staging path, `/tmp/helios_update.zip`, which is what
`hylia --load-package` reads; with nothing in the request to point elsewhere, a compromised
console cannot use it to place bytes anywhere on a hypervisor. It exists for the same
reason the vdisk write does: the console tier receives the operator's upload and must not
hold it — it is a container whose disk exists for an application, not for an archive that
may be hundreds of megabytes — so it proxies the bytes to the daemon that is native to the
host and that the loader runs on. That has to be the **ZooKeeper leader**, because that is
where the Catalyst task dispatches.

The body is written to `<path>.part` and renamed at the end, so a request that dies half
way leaves nothing at the path the loader looks in. A truncated archive there does fail
validation — as "not a zip file", which sends an operator looking at the package rather
than at the transfer. A short body is reported with what arrived and what was promised, for
the same reason the vdisk write is: the caller knows what it sent and is the only one that
can tell a truncated upload from a client that hung up.

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

### Host

| Method | Path | Body / Query | Returns |
| :-- | :-- | :-- | :-- |
| GET | `/api/v1/host/cpu` | -- | `{"model","cores","physical_cores","sockets","load_average"}` |
| GET | `/api/v1/host/memory` | -- | parsed `/proc/meminfo` |
| GET | `/api/v1/host/disks` | -- | the `{"blockdevices":[...]}` object from `lsblk -J` |
| GET | `/api/v1/host/network` | -- | `{"default_interface","default_gateway","addresses":[...]}` |
| GET | `/api/v1/host/interfaces` | -- | `{"interfaces":[{"name","mac","operstate","virtual","addresses"}]}` |
| GET | `/api/v1/host/listeners` | `?port=` (optional) | `{"listeners":[...]}`, or `{"port","listening","listeners"}` |
| GET | `/api/v1/host/units` | `?units=a,b` (optional) | `{"units":[{"unit","load_state","active_state","sub_state","unit_file_state","active"}]}` |
| POST | `/api/v1/host/units` | `{"action","units","detach"?,"ignore_failed"?}` | `{"action","units","ok"}` |
| GET | `/api/v1/host/capabilities` | -- | `{"kvm":bool,"secure_boot":bool}` |
| GET | `/api/v1/host/dhcp-leases` | -- | `{"leases":[{"mac","ip","hostname","expires"}]}` |
| POST | `/api/v1/host/reboot` | `{"confirm":true}` | `{"rebooting":true}` |

#### `/api/v1/host/units`

Unit control is the one place where design rule 1 has to be read carefully. "Model domain
operations, not shell verbs" rules out `/exec/rm`, but starting and stopping a service *is*
a domain operation on a host -- it is what `cluster start`, a maintenance window and a
rolling upgrade are made of -- so it earns an endpoint the way `/host/reboot` does.

What makes it safe is not the shape of the verb, it is the allow-list behind the unit
name. `MANAGED_UNITS` in `spark_daemon_decoded.py` is every unit `provision.py` installs,
plus `chronyd`, `libvirtd` and `virtqemud` -- three host units the stack drives without
owning. A name outside it is refused with `400` and a message naming it; nothing is
stripped, normalised or dropped. `zookeeper.service` is refused too: one spelling per
unit, or the allow-list has to be checked in two forms and the second is the one an edit
forgets.

`action` is one of `start` `stop` `restart` `enable` `disable` `daemon-reload`, and
`daemon-reload` is the only one that takes no `units`. The whole list goes to systemd in
one transaction rather than one call per unit, because systemd orders a transaction by the
units' own dependencies and fifteen sequential stops in the caller's order is a different
operation.

Two flags carry idioms the shell strings used to spell out:

* `ignore_failed: true` is `|| true`. The failure is still reported in the body; it is
  just answered `200` instead of `409`. Stopping a service that is not running is the
  usual case on a clean host.
* `detach: true` is `(sleep 1 && systemctl restart <unit>) >/dev/null 2>&1 < /dev/null &`.
  It answers immediately and acts afterwards, and it exists because `spark-daemon` is in
  the list: restarting it inline means systemd kills the process partway through writing
  the reply, and the caller sees a connection reset it cannot tell from a node that has
  gone away. A detached call returns `{"detached": true}` and reads nothing back; a caller
  that wants the outcome asks `GET /api/v1/host/units` afterwards.

A failed action answers `409` and carries `states` -- the per-unit read-back -- so the
caller learns which unit is in what state rather than only that something failed.

The read side uses `systemctl show` rather than `systemctl is-active`, and that is not a
detail. `is-active a b c` prints three bare words and leaves the caller matching answers
to units by line number, so a single missing line shifts every unit's state onto its
neighbour. `show` prints `Id=` with each block, distinguishes "inactive" from "there is no
such unit", and the response carries `active` as a boolean so that the comparison against
the literal `"active"` happens once here instead of at every call site.

#### `/api/v1/host/listeners` and `/api/v1/host/interfaces`

`listeners` parses `ss -ltnp` into `{"protocol","address","port","process","pid"}`, with
`port` as a **number**. Every caller this replaced wrote `ss -tlnp | grep <port>` and read
a match as "the service is up" -- but `grep 9042` also matches a peer address of
10.0.90.42, a queue depth, and another process's pid, so the check could pass on a node
where nothing had bound the port at all. `?port=` answers `{"listening": bool}` directly,
because the caller doing the scanning was the thing that was wrong.

`interfaces` reads `/sys/class/net` and `ip -j addr` and returns every interface with a
`virtual` flag. The flag is by name -- `lo`, `virbr*`, `br-*`, `vxlan*`, `veth*`, `vnet*`,
`macvtap*` -- because that is the rule it replaces, a `find /sys/class/net` carrying
exactly those `-not -name` clauses, shelled out to every node in the cluster. Asking sysfs
whether an interface has a backing device would answer differently for a bond or a bridge,
which are genuine uplinks. Every interface is reported and the caller filters; the daemon
says what is on the host, and which of those a user may pick is the console's business.

`/api/v1/host/network`'s `addresses` elements are
`{"interface","family","address","prefixlen","cidr","scope"}`. `cidr` is new, and it
closes the gap that used to be listed below: the console was handed `address` and
`prefixlen` and then ran `ip addr show <iface> | grep 'inet '` through a shell to get the
joined string back.

### Database (ScyllaDB via the hydra-db container)

| Method | Path | Body / Query | Returns |
| :-- | :-- | :-- | :-- |
| GET | `/api/v1/db/ring` | -- | `{"nodes":[{"address","status","state","load","tokens"}]}` |
| POST | `/api/v1/db/repair` | `{"keyspace":"hydra","primary_range":bool}` | `{"started":true}` |

`repair` runs asynchronously and returns immediately; it can take a long time on a large
keyspace and must not block an HTTP request.

### Cluster lifecycle

| Method | Path | Body | Returns |
| :-- | :-- | :-- | :-- |
| POST | `/api/v1/cluster/state` | `{"desired":"started"\|"stopped","stop_state_store":bool}` | `{"desired","state_store_stopped"}` |

The endpoint `cluster start` and `cluster stop` are built out of, and it takes **no service
list** -- deliberately, because a caller that could name a service would be a second place
the start ordering lives, and the copy nobody reads is the one that goes stale. It starts
the store the desired state lives in, writes the state, and returns; each node's reconcile
loop decides what to start or stop, in what order, and when it is finished. See
[cluster_state.md](./cluster_state.md#3-one-actor-per-service).

`stop_state_store` is the one thing a reconcile loop cannot do for itself: stop the store
it reads its instructions from. It is accepted only alongside `desired: "stopped"`, and
`cluster stop` sends it after every node reports it has nothing left running.

## Migration

Each call site moves from `run_remote_spark(ip, "<shell string>")` to
`run_mtls_spark_api(ip, "<path>", payload, method=...)`. The raw-call count is the metric,
and it is counted rather than estimated -- see the correction below for what estimating it
cost. `/api/v1/execute` is removed when the count reaches zero across `spectrum_server.py`,
`cluster_new.py`, `vali.py`, `hylia.py`, `mipha.py` and `dagur.py`.

**149 call sites remain**, down from 194: `cluster_new.py` 58, `hylia.py` 38,
`spectrum_server.py` 34, `vali.py` 15, `mipha.py` 3, `dagur.py` 1. Two families are finished --
systemd unit control and network probing -- and `test_spark_shell_calls.py` is what keeps
them finished: it asserts that no caller builds a shell string in either family, reading
string literals out of the AST so that an f-string is as visible as a plain one and a
comment quoting the command it replaced is not mistaken for the command.

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
| ~~`GET /api/v1/host/cpu`~~ | ~~1~~ | **Landed.** Core count, model and load average, read from `/proc`. |
| `GET /api/v1/host/ping` | 2 | A liveness probe for the reboot task. Currently `echo 1`, and not migrated because `run_mtls_spark_api` has a 120s timeout against `run_remote_spark`'s 60s, which would change reboot detection timing. |
| Process control (`pkill`, `pgrep`) | 2 | `mipha.legacy_spark_fence` and the reading back that proves it took. A name-pattern kill needs the same treatment a unit name got: an allow-list, not a string. Note the fence's `systemctl` clause is deliberately **not** migrated with the rest of its family: that function is reached only when a host answered 404 to the typed fence, so a daemon old enough to reach it is old enough to 404 on `/host/units` as well, and a compatibility path that requires the endpoint it exists to be compatible without fails silently. |
| Journal reading (`journalctl -u <unit>`) | 1 | `cluster_new` reads hydra-db's bootstrap progress out of the journal. Adjacent to unit control and deliberately not folded into it: reading a unit's log is a different operation from acting on the unit, with a different answer shape. |

One shape ambiguity in v1 is left: `/host/disks` returns `{"blockdevices": [...]}` because
`lsblk -J` does, which follows upstream rather than being stated in the contract. The other
one is closed -- `/host/network`'s `addresses` elements are specified above and now carry
`cidr`, which is the field the caller who "had to keep shelling out" was shelling out for.

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

The correction needed a correction of its own. The next figure recorded -- 184 call sites,
split `spectrum_server.py` 64, `cluster_new.py` 62, `vali.py` 28, `hylia.py` 22,
`mipha.py` 7, `dagur.py` 1 -- had the right total to within five and the wrong split by a
long way: `hylia.py` had 37 and not 22, and `spectrum_server.py` 36 and not 64. A total
that is nearly right hides a per-file breakdown that is not, and the breakdown is what
anyone planning the next family reads.

So the count is now produced the same way every time, and stated so it can be reproduced:
a **call site** is a call to `run_remote_spark`, or to any function that takes a command as
a parameter and forwards it to one -- `run_parallel`, `run_checked_cmd` and
`run_parallel_checked` in `cluster_new.py` today, `run_linstor_cmd` before it -- excluding
the forwarding call inside the wrapper itself, which is the wrapper and not a site.

Also worth writing down, because it will happen again: two of the four call sites removed
from `hylia.py` came back as one. Splitting a chained shell string moves the clauses that
have a typed endpoint and leaves the ones that do not, and where those alternate, the
remainder becomes more than one call. `podman rm -f systemd-spectrum` sat between a stop
and a start; the two service clauses left the shell and the container clause became a call
site of its own. The number went down by two rather than three, and that is the right
trade.

## Related

* [spark.md](./spark.md) / [spark_technical.md](./spark_technical.md) -- the daemon
* [cluster_state.md](./cluster_state.md) -- the ZooKeeper-backed state model
* [../TODO.md](../TODO.md) -- "Unsandboxed root command execution" is the item this retires
