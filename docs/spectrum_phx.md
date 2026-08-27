# Spectrum (Phoenix) — the console migration

**Nutanix analog:** Prism Element, same as [Spectrum](./spectrum.md).
**Runtime:** Podman + Elixir/Phoenix LiveView release. **Unit:** `spectrum-phx.service`. **Port:** 8444.

This document covers *what the Phoenix console is and why it behaves the way it does*.
For build, toolchain and local-development instructions see
[`spectrum_phx/README.md`](../spectrum_phx/README.md). For the Python console it is
replacing, see [spectrum.md](./spectrum.md) and [spectrum_technical.md](./spectrum_technical.md).

---

## 1. It is a strangler migration, not a rewrite-and-switch

The Python `spectrum_server.py` keeps running on 8443. `spectrum-phx` runs beside it on
8444 with a different unit name, container name and image. Nothing under `spectrum_phx/`
modifies, restarts, or depends on the Python tier.

**Both tiers now serve the live console.** [Slate](./slate.md) routes the rebuilt pages to
Phoenix and everything else — the pages not yet rebuilt, the whole HTTP API, the Python
tier's own assets — to Python. The two halves of that statement are
`slate_config/dynamic.yml` and `SpectrumPhxWeb.Layouts.nav_items/0`; `test_console_routing.py`
asserts they agree, because when they do not the failure is a link in the navigation bar
that 404s, or a page that quietly loads from the tier it was supposed to replace.

The split is written as "Phoenix owns these paths, everything else is still Python" rather
than the reverse. A catch-all pointing at the new tier would hand it every API endpoint and
unported page too, and each would fail the moment it was missed.

Routes move over one at a time, and each one is expected to be *more correct* than what it
replaced, not merely prettier. Section 6 lists what the old pages were getting wrong.

## 2. The console never touches the data path

This is the single hardest architectural rule here, and it was learned the expensive way.

The web tier does not open block devices, does not mount storage, and does not stage files
on a storage mount. Everything goes through [Spark](./spark.md)'s typed API
([spark_api.md](./spark_api.md)) or through a read of [HydraDB](./hydra.md). Prism does not
reach into Stargate on Nutanix, and Spectrum does not reach into [Sidon](./sidon.md) here.

The failure that established the rule: image upload called `test -b` on a device path and
then opened it. `test -b` ran *on the host* through spark-daemon and passed; the open ran
*inside the container* and failed with `ENOENT` -- a container's `/dev` has device nodes
but not udev's subdirectories, so `/dev/drbd/by-res/<res>/0` was simply not there. The
upload had never worked, and no amount of privilege would have made it. The first
attempted fix — staging the file on a storage mount — is the same mistake wearing a
different hat: it is still the web tier writing cluster storage. The correct shape is
`POST /api/v1/dfs/write`, where Spark owns the storage and the console streams bytes to
it.

The storage layer changed underneath this rule and did not weaken it. A vdisk is reached
over a unix socket under `/var/lib/hci/sidon/nbd/`, which this container also does not
have and also should not get. What did improve is the endpoint: the device form took a
path and checked it against an allow-list, while the vdisk form takes a *name* and derives
the socket itself, so a caller cannot name a file at all.

## 3. Pages

| Route | View | Source of truth |
|---|---|---|
| `/` | `Cluster.OverviewLive` | ZooKeeper desired state + per-node ephemeral znodes ([cluster_state.md](./cluster_state.md)) |
| `/hosts` | `Cluster.HostsLive` | ZooKeeper + `Spark.host_disks/1` |
| `/vms`, `/vms/new`, `/vms/:name` | `Vms.*Live` | `hydra.vms`, Spark VM and DFS endpoints |
| `/storage` | `Storage.IndexLive` | Sidon's `capacity`, `list` and `peers` per node, plus `lsblk` |
| `/images` | `Images.IndexLive` | `hydra.valhalla_images`, Sidon vdisks |
| `/tasks` | `Tasks.IndexLive` | `hydra.catalyst_tasks` |
| `/metrics` | `Metrics.IndexLive` | `hydra.logos_metrics` |
| `/health` | `Health.IndexLive` | `hydra.mimir_results`, `hydra.dagur_schedules` |
| `/hardware` | `Hardware.IndexLive` | Spark's `host/cpu`, `host/memory`, `host/disks`, `host/network` per node |
| `/sdn` | `Sdn.IndexLive` | the five `hydra.urbosa_*` tables, plus Spark's tunnel status |

Navigation lives in one list, `SpectrumPhxWeb.Layouts.nav_items/0`, and it spans both tiers
while the migration is in progress. Each entry carries the tier that serves it:

| Tier | Entries | Rendered as |
|---|---|---|
| `:live` | the table above | `navigate` — live navigation within this application |
| `:legacy` | Networking, LCM, Lanayru, Settings | `href` to `/<page>.html` — an ordinary link to the Python tier |

A `:legacy` entry has to be a plain link: live navigation asks *this* router for the page,
and it has no route for one the other tier serves. As each page is rebuilt its entry moves
to `:live` and loses the `.html` suffix; when none are left, the split and the routing rule
that implements it go away together.

Nothing in the compiler notices when a page is renamed, so
`test/spectrum_phx_web/navigation_test.exs` walks the list against the real router. The two
tiers are asserted differently on purpose: a `:live` entry must resolve to 200 when signed
in and redirect to `/login` when signed out, while a `:legacy` entry must **404** here —
requiring it to be routable is what would silently pass if somebody moved an entry to
`:live` without building the page.

## 4. Authentication

Sessions live in `hydra.sessions`, referenced by an opaque 64-hex-character token held in a
signed, HTTP-only cookie. Passwords use the same `pbkdf2_sha256$100000$<salt>$<digest>`
format the Python tier writes, so both consoles authenticate the same accounts during the
migration and no account has to be migrated or duplicated.

### One sign-in covers both tiers

The two tiers always shared the session *store* — identical `hydra.sessions` rows, and both
generate the token as 64 lowercase hex characters. What they did not share was the cookie:
this application keeps the token inside its own signed session cookie, and the Python tier
looks for a bare `session_id`. With a navigation bar spanning both, that meant half the
links bounced the operator to a login page.

So the token is written to both and read from both. `UserAuth.log_in_user/2` sets
`session_id` alongside the signed session (HTTP-only, Secure, `SameSite=Lax`) and
`log_out_user/1` clears it; `fetch_current_user/2` falls back to that cookie and **adopts**
the token into this application's session when it finds one there. The adoption is the part
that matters: LiveView mounts are handed the session, not the request's cookies, so leaving
the token in the cookie alone would authenticate the page that renders the socket and then
refuse the socket itself — a dashboard that appears and vanishes a moment later.

The cookie is unsigned because the other tier expects the raw token. That costs nothing:
the token is opaque, server-side, and revoked by deleting its row. This goes away with the
last `:legacy` entry in the navigation table.

Two further properties worth stating explicitly:

- **The token's format is validated before it reaches a query.** The Python tier
  concatenated a header, cookie or query-parameter value straight into CQL.
- **Authentication is enforced once, in the router**, by a single `live_session` with an
  `on_mount` hook, rather than repeated inside each view — which is the form of that check
  that eventually gets forgotten in one view. LiveDashboard sits behind the same gate.

An unknown username still runs a dummy verification, so a failed login costs the same time
whether or not the account exists. A database that cannot be reached is reported as
`:database_unavailable`, distinctly from `:invalid_credentials` — an operator locked out by
a ScyllaDB outage should not be told their password is wrong.

## 5. How pages stay current

Every view subscribes to a PubSub topic on connect and *also* schedules a server-side
refresh. Nothing polls from the browser.

That distinction is the whole point. The old console had every open browser tab issuing its
own unbounded scan; the Phoenix views read once on the server and fan the result out to
every connected socket. One operator with a page open costs one read, not one read per tab.

Refresh intervals are **self-rescheduling** — the next tick is scheduled when the previous
read returns, not on a fixed timer. A fixed interval queues ticks faster than a degraded
database can serve them, which is exactly when you least want a backlog. `broadcast/1` on
each context is the seam for a real watcher; once one exists the intervals can go.

### Unknown is never drawn as healthy

The rule enforced across every page: a value that could not be read renders as *unknown*,
never as zero, empty, or green. Concretely — a node whose Sidon did not answer renders
differently from one that answered with nothing; an extent store reporting no capacity gets
no usage bar rather than a reassuring 0%, and says the likely reason (it is not mounted); a
node with no telemetry says "unknown, not zero"; a vdisk on a partially-readable cluster is
`:unknown` rather than `:ok`, and its `under_replicated?` is `nil` rather than `false`; an
empty `mimir_results` says diagnostics have not run rather than showing green.

One distinction is new and worth stating on its own: a replication link that is down is
reported as **writes being refused**, not as reduced redundancy. The journal is write-all,
so an append that has not reached every replica is not acknowledged — the guests on that
vdisk are taking EIO now, which is a different operational situation from "fewer copies
than we would like".

## 6. Where the old pages disagreed with the database

These were found by reading the schema against the templates, and each one is a case where
the previous console displayed something confidently false.

**Health**

- `mimir_results.category` is the *invocation scope*, not the check kind — it is the
  partition key, and `mcli health_checks run_all` writes the literal `all` for every row.
  Grouping by it yields one bucket. `app.js` worked around this with a hardcoded name list
  that had drifted from the `checks_map` in `mcli`: two storage checks were being shown as
  services, and seven service checks as hardware.
- The same check can appear in `mimir_results` twice with different statuses, because
  running one category after a `run_all` writes a second row and nothing removes the first.
  The old page rendered both.
- `mimir_schedules` has no `cron_expression` column; the old page hardcoded `0 * * * *` as
  its "Cron Spec". Two independent schedulers can run Mimir, and the cron actually lives in
  `dagur_schedules`.

**Tasks**

- `progress = 100` does not mean success. Every failure path in `log_catalyst_task` writes
  `progress = 100` alongside `status = 'failed'`.
- `catalyst_tasks` has no time-ordered clustering key, so "the most recent N" is not
  answerable server-side and every read is a full scan.

**Metrics**

- `logos_metrics` has no disk-usage column — it records `disk_iops` and
  `disk_bandwidth_kbps`. The old page labelled its chart "Disk" while plotting IOPS.
- `/api/cluster/metrics` ran `SELECT JSON * FROM hydra.logos_metrics` with no `WHERE` and no
  `LIMIT` — roughly 8,600 rows on a three-node cluster — then discarded all but 40 samples
  per host in the browser, from every open tab. Reads are now per-node, bounded, and
  answered directly by the clustering order.

**Storage and images**

- The storage page's pools never came from `/api/storage/*`. `/api/storage/disks`
  synthesises fake disks from `hydra.vms` rows; it is a VM-volume list, not block devices.
  The "Physical Disks" panel was pool rows whose *name string* began with `"Physical Disk"`.
- Pool health was a substring test (`"ok" in state.lower()`) with nothing about the
  replicas anywhere — which is precisely why a resource missing a replica rendered
  identically to a healthy one. Both the parser and the substring test are gone with
  LINSTOR: capacity now comes from `statfs` on the extent store's own filesystem, in
  bytes, with no sentinel values to filter and no human table to split.
- `GET /api/images` *writes*: it scans a directory and inserts rows for files it finds.
  Reconciling the catalogue with the filesystem is a job, not a page load.
- `/api/images/delete` always answered 200 — the row was deleted first, and both the
  `resource-definition delete` and a fan-out `rm -f` were unchecked. A failed delete left
  storage allocated that nothing in the UI could ever see again, and told the operator it
  had worked. The order is now inverted (backing store first, checked), and deletion is
  confirmed server-side through a distinct named event rather than a client-only dialog.

## 7. Image upload

Uploading is the one page that both reads and writes the data path, so it is worth stating
how it avoids doing so from here.

The browser's chunks arrive over the LiveView channel and are pushed, one at a time, onto
an HTTP request that is already open to spark-daemon's `POST /storage/device/write`.
Nothing is spooled: LiveView's default writer would buffer the whole file to a temporary
file first, which for an install ISO is gibibytes of the web tier's disk and puts this tier
straight back on the data path. Memory use is one chunk regardless of image size, and
because the send blocks until the socket accepts it, a slow host slows the browser rather
than filling this node.

The sequence, in order:

1. `prepare_upload/2` -- resource definition, a volume definition sized in KiB from the
   file, placement on every node, `--allow-two-primaries` (correct only for images), then
   poll until the device appears and promote this node to Primary, **checking the role that
   comes back**. A promotion that did not take means a peer still holds Primary, and
   writing from a Secondary is the split-brain the check exists to prevent.
2. Stream the chunks.
3. `finish_upload/2` -- verify the byte count against the declared size, set `root:qemu`
   `0660` (not `0666`: world-writable lets any local user corrupt the golden image every VM
   is cloned from), flush, demote.
4. `register/1` -- the catalogue row, **last**, because a row is a claim that the image is
   usable.

Two things about it are less obvious than they look, and both were established by running
it against real hardware rather than by reasoning:

**The preparation runs on the first chunk, not in the writer's `init/1`.** `init/1` is
called inside the upload channel's `join`, and the browser joins with the socket's default
ten-second timeout, then *rejoins* if it expires -- which would run the whole preparation
a second time and leak the first attempt's connection. Creating a vdisk is metadata work
that returns in milliseconds, where the LINSTOR placement it replaced built a kernel object
on every node and could use most of that budget by itself; the reasoning holds anyway,
because "usually fits" is not a bound. `write_chunk/2` is bounded by `:chunk_timeout`,
which `allow_upload/3` accepts as an option, so the work sits under a limit the application
controls rather than one the browser picked.

**Rollback closes the connection before it deletes the vdisk, and retries.** spark-daemon
keeps the vdisk attached for the life of the write request, so an abandoned upload -- a
cancelled transfer, a truncated body, a browser that vanished -- leaves it in use for a
moment after this tier has given up on it. Deleting first fails, and the first version of
this code gave up there, leaving storage held on every replica. Ordering the teardown
(detach, then delete) and retrying for a few seconds is what makes the rollback actually
reclaim anything.

**An image is sealed, not left writable.** The last step before the catalogue row makes
the vdisk permanently immutable. This is what replaced `--allow-two-primaries`, which
existed because guests on several hosts attach a golden image read-only at the same time
and DRBD required each of those hosts to hold Primary in order to read -- exactly the state
that corrupts a device the moment anything writes. A sealed vdisk cannot reach it: reads
need no lease and writes are refused by class at the NBD layer. A seal that fails is a
failed upload, because an unsealed template is one attach away from changing what every VM
cloned from it boots.

## 7a. Hardware

The physical inventory. Four reads per node — CPU, memory, disks, network — in parallel,
each kept or lost on its own: a host whose `lsblk` fails still reports its processors, and
a host that answers nothing still appears, marked unreachable. An inventory that silently
omits a machine is worse than one that admits it could not reach it.

Porting it **removed** an `/api/v1/execute` call site. The Python page reads the processor
by sending `nproc; grep -m1 "model name" /proc/cpuinfo` through the general
run-this-string-as-root endpoint — the last open P1 security item in
[TODO.md](../TODO.md) — for a fact that is two file reads. Spark grew a typed
`/api/v1/host/cpu` instead, reporting logical cores, physical cores and sockets
separately: a 2-socket 8-core host with hyper-threading and a 32-thread single socket both
answer 32 to `nproc` and are not the same machine.

Disks are listed once. `lsblk -J` nests partitions under their disk and the obvious
rendering counts the same platter three times, so partitions are folded into a count and
the mountpoints they carry.

It refreshes every two minutes. Hardware does not change between two page loads, and each
refresh is four reads against every node.

## 7b. SDN

Urbosa's five tables are flat and join by uuid — a segment names a T1, a T1 names a T0, a
guest names a segment. `SpectrumPhx.Sdn` assembles them into the tree they describe and
`SpectrumPhxWeb.Sdn.Topology` computes the diagram's geometry from it.

That is the difference from the page it replaces, which drew a **fixed** diagram: the
boxes were in the markup and the data was written into them, so it could only show the
shape somebody anticipated. A cluster with two Tier-0s had nowhere to put the second, and
a segment whose router had been deleted had nowhere to appear at all — which is the one
shape an operator most needs to see, because it is a guest that cannot reach anything.

Anything dangling is kept and drawn detached, in its own band with no edge to it. Edges
are elbows rather than diagonals: through four bands of boxes a diagonal crosses whatever
is between them and the eye cannot follow which line entered which box. Guests are dots
along the bottom of their segment, hollow when not running — a segment with four stopped
guests and one with none are different situations.

Selecting a box filters the tables beneath it, which is the reason to draw it rather than
tabulate it.

The logical tree and the tunnels are read on separate clocks (15s and 30s). T0/T1/segments
are rows somebody created; the tunnels are what those rows are carried over, and they fail
independently — a perfectly configured segment on a host whose tunnels are down is
unreachable, and nothing in the logical tree shows it.

A firewall action the table cannot parse renders as `unknown`, never as `allow`. A table
that guesses permissive when it cannot read a row is worse than one that admits it.

## 8. What is not done yet

Four pages are still served by `spectrum_server.py` on 8443, reached from the navigation
bar as `:legacy` entries: **Networking, LCM, Lanayru and Settings**. The guest console
(`/vnc_auto.html`) and the whole HTTP API are there too.

`hylia.py` and `lanayru.py` are imported as Python modules by Spectrum and have no Elixir
counterpart, so the routes that use them cannot move until they are reimplemented, shelled
out to, or kept behind a port.

Three things disappear when the last page moves: the `:legacy` half of the navigation
table, the `phoenix-ui` router in `slate_config/dynamic.yml` (the catch-all can point here
instead), and the shared `session_id` cookie in section 4.

Upload does not write a `catalyst_tasks` row, unlike the Python endpoint. LiveView reports
progress on the page itself, which is better for the operator watching it happen; the cost
is that an upload is not visible from `/tasks`.

## 9. See also

- [`spectrum_phx/README.md`](../spectrum_phx/README.md) — toolchain, build, local development
- [spark_api.md](./spark_api.md) — the typed API the console calls
- [cluster_state.md](./cluster_state.md) — the ZooKeeper model the overview reads
- [deployment.md](./deployment.md) — Quadlet deployment and rollout
