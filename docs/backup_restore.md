# Backup and Disaster Recovery (Saga)

What a Helios cluster cannot rebuild by itself, how it is captured, and — the half that
is usually missing — how it is put back.

> [!WARNING]
> **This does not back up guest data.** Nothing here copies a byte out of a DRBD
> volume. A VM's disk lives in `vg_aether/thin_pool_aether` and is replicated by DRBD
> across hosts; that protects it against a host dying, and against nothing else. If a
> guest's filesystem is corrupted, or someone deletes the VM, or the storage tier is
> lost on every replica at once, Saga does not help. Back guests up from inside the
> guest, or with a product that does volume-level backup. Read [§7](#7-what-this-does-not-cover)
> before you rely on any of this.

> [!NOTE]
> **Name origin:** In Norse mythology, **Sága** is the goddess associated with
> record and recollection, who drinks daily with Odin at Sökkvabekkr; a *saga* is the
> written account of what happened. The Nutanix analog is **Cerebro**, the DR and
> replication service.

---

## 1. Why: the pile of anonymous block devices

The storage layer is the part that already survives. Lose a host at FTT=1 and every
DRBD resource is still intact on its peer. What is *not* replicated anywhere outside
the metadata layer is the answer to "which resource is which":

| Held in | What it is | Reconstructible? |
| :--- | :--- | :--- |
| `hydra.vms` | Every VM: vCPU, memory, firmware, boot device, network, host assignment | **No** |
| `hydra.vm_nvram` | Each UEFI guest's NVRAM variables — boot order, Secure Boot state | **No** |
| `hydra.storage_containers` | Container name, tier, quota, FTT | **No** |
| `hydra.gatoway_networks`, `hydra.urbosa_*` | VLANs, overlay segments, routers, firewall rules, transit /30 allocations | **No** |
| `hydra.users` | Console logins and their password hashes | **No** |
| LINSTOR controller DB | Each resource's port, minor, node-id, placement and volume definitions | **No** in practice |
| `/etc/hci/cluster.json` | Node identities, redundancy factor, VIP, cluster name | **No** |
| `/var/lib/hci/certs_staging/ca.key` | The cluster CA. Exists on exactly one host | Only by re-issuing every certificate in the cluster |

Lose the `hydra` keyspace and you have `/dev/drbd1000`, `/dev/drbd1001`, `/dev/drbd1002`
and no statement anywhere about what any of them is.

---

## 2. What is backed up, and what deliberately is not

Saga captures four things and refuses to capture a fifth.

### 2.1 The `hydra` keyspace — captured

Via `nodetool snapshot`, which is the standard Cassandra/Scylla mechanism: it flushes
memtables and hardlinks the SSTables into
`<data>/data/<keyspace>/<table>-<uuid>/snapshots/<tag>/`, giving a consistent
point-in-time set for that node at effectively zero cost.

The whole keyspace is captured, not a chosen subset. Some tables in it are logs rather
than records — `dagur_runs`, `catalyst_tasks`, `logos_metrics`, `console_metrics`,
`urbosa_tunnel_metrics`, `hylia_logs` — and several carry a `default_time_to_live` so
they are bounded anyway. Selecting at *backup* time would mean deciding once, for
every future restore, that a table is never wanted; selecting at *restore* time
(`--tables`) costs nothing and can be decided when you know what went wrong.

The keyspace definition is captured alongside it (`meta/schema.cql`, from
`DESCRIBE KEYSPACE`), and so is the migration ledger `hydra.schema_migrations`
(`meta/schema_migrations.json`) — see [§5.2](#52-the-schema-check-and-why-it-refuses).

### 2.2 The LINSTOR controller database — captured

Via `linstor controller backupdb`, which writes a consistent zip of the H2 database.

This one is *technically* reconstructible and practically is not. The LVM metadata on
each host says which logical volumes exist, and each DRBD resource's on-disk metadata
carries its own node-ids — so a determined operator could reverse-engineer the resource
definitions. That is an archaeology project measured in days, per resource, with a live
risk of attaching the wrong replica set to the wrong volume. Treat it as unrecoverable.

Two properties worth knowing:

* It is captured **only on the node running the controller**, because that is the only
  node it exists on. Every other node's artefact records
  `contains_linstor_db: false` and says why. `saga list` flags a backup round where no
  node captured it.
* `linstor controller backupdb` writes into the controller's own `/var/lib/linstor`,
  which in Helios is the `linstor-db` DRBD volume — i.e. it writes the backup onto the
  very volume being backed up. Saga moves it off and deletes the original in the same
  step. Left alone, this grows the HA volume by a copy of itself on every run.

### 2.3 `/etc/hci` — captured

`cluster.json` is the point: it names the hosts, holds `redundancy_factor`, the VIP and
the cluster name, and none of that is derivable from `hydra.nodes` (which knows hostname,
IP, status and maintenance flag, and nothing else). It is also *circular* — you need it
before you can reach Hydra at all — so it must be readable straight out of the artefact
without a working cluster.

The rest of the tree (`spectrum/`, `slate/`, `aether/`, `hydra/`, `zookeeper/`) is
regenerated by `provision.py` on a rebuild, so it is captured for diagnosis rather than
for restore.

**Private keys are skipped by default.** Any file named `*.key` under `/etc/hci` is left
out unless `--include-ca` is given, and the manifest lists what was skipped. An artefact
sitting on an NFS export that quietly contains every node's TLS key is a different kind
of object from one that contains configuration, and the difference should be a decision
rather than an accident.

### 2.4 The cluster CA — captured only when asked

`--include-ca` adds `/var/lib/hci/certs_staging` (`ca.key`, `ca.crt`, `ca.srl` and the
per-node material) and stops skipping `*.key` under `/etc/hci`.

The CA is the most fragile single thing in the cluster: it exists on the first host in
`cluster.json` and nowhere else (see [mtls_lifecycle.md](./mtls_lifecycle.md) §1). Losing
it does not stop a running cluster — the leaf certificates keep working until they
expire — but you can no longer add a node or renew anything without a cluster-wide
re-issue with `impa renew --ca`, which touches every host at once.

So it is *reconstructible*, at the cost of an outage-adjacent operation on every node
simultaneously. That is why it is offered and why it is off by default: an artefact
carrying `ca.key` is a credential that impersonates any node in the cluster, and where
that artefact ends up is a decision the operator has to make deliberately. When it is
included the manifest records `contains_ca: true` and `saga list` prints a `CA` flag.

Artefacts and manifests are always written mode `0600` regardless — even without the CA
they carry `hydra.users` password hashes and `hydra.sessions` tokens.

### 2.5 ZooKeeper — deliberately NOT captured

`/cluster_state` holds one word, `started` or `stopped`, and an operator retypes it with
`cluster start`. Everything under `/helios/nodes/<ip>` is **ephemeral** by construction:
its lifetime is bound to the publishing `spark-daemon`'s session, and it is republished
within about five seconds of that daemon starting. See
[cluster_state.md](./cluster_state.md) §2.

Capturing it would be worse than useless twice over. It would imply the tree is a system
of record when it is a live view, and restoring a stale `stopped` into a cluster you are
trying to bring up would actively hold it down.

**A backup that captures the wrong set is worse than none**, because it converts "we
have no DR" into "we have DR" in everyone's head. The table above is the claim this tool
makes; it does not make a larger one.

---

## 3. What an operator must provide

**There is no external storage in Helios.** No NFS export, no S3 endpoint, no tape.
This is the part you have to supply, and Saga refuses to invent it:

* There is **no default target**. A default would silently put backups on the boot
  disk.
* A target that does not exist is **refused, not created**. A mount point with nothing
  mounted on it is indistinguishable from a missing directory, and quietly creating it
  is how a root filesystem fills with backups nobody can find.
* A target on the **same filesystem as the Scylla data directory** is refused. A backup
  stored on the disk it protects survives exactly the failures that were never going to
  destroy the data anyway, and competes for the space whose exhaustion stops the
  cluster. `--allow-same-filesystem` overrides it; the artefact then records
  `target_on_data_filesystem: true` and `saga list` marks it `LOCAL`.

What to provide: a directory, mounted on **every node**, backed by storage outside the
cluster. An NFS export, a CIFS share, or an external disk — anything the kernel will
mount. Something like:

```bash
# On each node, /etc/fstab:
nas.example.net:/export/helios-backups  /mnt/helios-backups  nfs  _netdev,noatime  0 0

mkdir -p /mnt/helios-backups && mount -a
valcli backup.target /mnt/helios-backups
```

Every node needs it because every node writes its own artefact (see
[§4.2](#42-a-multi-node-cluster-is-a-set-of-artefacts-not-one)).

The target path is stored in `hydra.cluster_settings` under the key `saga_target`,
alongside `dns_servers` and `urbosa_enabled` — no new table, no migration.
`--target` and the `SAGA_TARGET` environment variable override it, and both
short-circuit the database read entirely, because a restore has to work on a cluster
whose metadata layer is the thing that is broken.

---

## 4. Taking a backup

```bash
valcli backup.target /mnt/helios-backups     # once
valcli backup.run --all-nodes                # every node, in parallel
valcli backup.list
valcli backup.verify                         # the newest artefact
```

`saga` is the same tool without the CLI wrapper, and is what the host-level and restore
work uses.

### 4.1 What one run does

1. Validate the target (exists, writable, not the database's own disk).
2. Read `hydra.schema_migrations` and `DESCRIBE KEYSPACE hydra` — **before** the
   snapshot, so what is recorded describes the shape the SSTables are about to be
   written in. A failure here stops the run: an artefact whose table shape is unknown
   is one no restore can check.
3. `nodetool snapshot -t saga-<round> hydra`.
4. Gather the snapshot's files, `/etc/hci`, the LINSTOR database (if the controller is
   here), and the generated `meta/` files.
5. Write `<target>/saga-<cluster>-<node>-<stamp>.tar.gz.partial`, fsync, rename to the
   final name, write the `.manifest.json` sidecar.
6. **Always** `nodetool clearsnapshot -t saga-<round> hydra`, on every path including
   every failure path.
7. Apply retention.

Step 6 is the one that matters for disk space. A snapshot is hardlinks, so it costs
nothing when taken and then costs everything: it pins every SSTable it references
against deletion, so compaction can no longer release the space those files occupy. A
tool that leaks a snapshot on each failure fills the disk the database lives on. Saga
clears its own snapshot in a `finally`, and `saga snapshots --prune` sweeps any tag
matching `saga-*` left behind by a run that was killed outright.

`saga snapshots` also *reports* snapshots it will not touch. Scylla writes a
`pre-drop-<epoch>` auto-snapshot every time a table is dropped and never clears it —
ten of them were sitting on the test cluster holding 452 KiB. Those are the last copy of
a table somebody dropped, so clearing them automatically would be the tool destroying
data. They are listed, with the command to clear them by hand.

### 4.2 A multi-node cluster is a set of artefacts, not one

`nodetool snapshot` is per node, and a node's snapshot holds only the SSTables for the
token ranges it replicates. At RF=3 on three nodes every node holds everything and any
single artefact is complete — but RF is **not** fixed at 3 (see [hydra.md](./hydra.md)
§2); a single-node cluster runs RF=1 and a two-node cluster may run RF=2.

So the unit of recovery is a **round**: one artefact from each node, taken close
together. `--all-nodes` runs them in parallel under a shared round tag, and `saga list`
groups by that tag and reports how many of `cluster.json`'s nodes are represented.

At restore, every node's artefact is loaded with `nodetool refresh --load-and-stream`,
which hands any SSTable whose token range this node does not own to the node that does.
The union of the replicas is the whole keyspace, and duplicate rows across replicas are
harmless because writes are idempotent by (key, timestamp).

**Consistency across nodes is per-node, not cluster-wide.** Each node's snapshot is a
point in time; the round spans a window of a few seconds. That is the standard
Cassandra property, and last-write-wins reconciliation means the merged result is a
valid state rather than a torn one — but a VM created *during* the window may be in one
node's artefact and not another's. It exists after the restore, because the write
survives on whichever replica captured it. In-flight lightweight transactions do not
survive at all: `system.paxos` is not captured.

`--all-nodes` fans out over spark-daemon's mTLS API rather than `allssh`, because
`allssh` prints every node's output and then exits 0 regardless — a scheduled backup
driven through it would report SUCCESS on the night every node failed. Saga collects
per-node exit codes and returns non-zero if any node failed.

### 4.3 The artefact

```
saga-<cluster>-<node>-<YYYYmmddTHHMMSSZ>.tar.gz
saga-<cluster>-<node>-<YYYYmmddTHHMMSSZ>.tar.gz.manifest.json
```

Inside:

```
manifest.json                     the same document, so the archive is self-describing
meta/schema.cql                   DESCRIBE KEYSPACE hydra
meta/schema_migrations.json       hydra.schema_migrations, id -> checksum
meta/nodetool-status.txt          the ring as it stood (diagnostic)
etc-hci/…                         /etc/hci, minus *.key unless --include-ca
ca/…                              only with --include-ca
linstor/linstordb.zip             only on the controller node
scylla/<table>/…                  the snapshot's SSTable files, per table
```

The manifest records every member's size and sha256 individually; the sidecar
additionally records the finished archive's size and sha256. `saga verify` checks all
of it — a truncated archive fails on the archive digest *and* on the members that are
no longer there, and a swapped member fails on its own digest even at identical size.

This detects corruption, truncation and bit-rot. It does **not** detect a determined
tamperer, who can rewrite the sidecar as easily as the archive. The repo has
`helios_sig.py` for signature verification and this deliberately does not pretend to be
one; signing artefacts is future work.

Cluster and node names are folded to `[A-Za-z0-9_]` in the filename (so `hci-01`
becomes `hci_01` and `10.10.102.41` becomes `10_10_102_41`) because `-` is the field
separator. Anything that does not parse as this exact shape is never treated as an
artefact — the target may be a shared export holding other people's files, and
retention must never consider one.

---

## 5. Restoring

### 5.1 The sequence, in order

**Case A — a table was dropped, truncated, or corrupted; the cluster is otherwise fine.**

```bash
valcli backup.verify saga-hci_01-10_10_102_41-20260821T225436Z.tar.gz
valcli backup.restore saga-hci_01-10_10_102_41-20260821T225436Z.tar.gz --tables vms,vm_nvram
```

Restore **merges**; it does not replace. Rows that exist now and are newer than the
backup's win on timestamp. If you need exactly the state of the backup, `TRUNCATE` the
table first — and understand that this is itself the destructive step.

On a multi-node cluster, run the restore for **each node's artefact from that round**.
They can all be run from one node; `--load-and-stream` routes each SSTable to its owner.

**Case B — total loss of the metadata layer; the DRBD volumes survived.**

1. **Get `cluster.json` back first.** `saga restore --extract-only /tmp/recover <artefact>`
   unpacks the archive and touches nothing. `etc-hci/cluster.json` is in there. Every
   later step needs it.
2. **Rebuild the nodes** with `provision.py` / `cluster create`, using that
   `cluster.json`'s node identities, redundancy factor and VIP. Do not invent new ones:
   the DRBD resources on disk are addressed by node-id.
3. **Restore the LINSTOR controller database** — by hand, deliberately, because the
   automated version would mean stopping storage on a live cluster:
   ```bash
   systemctl stop linstor-controller
   unzip -o /tmp/recover/linstor/linstordb.zip -d /var/lib/linstor/
   systemctl start linstor-controller
   podman exec -e LS_CONTROLLERS=<node-ip> systemd-aether linstor node list
   podman exec -e LS_CONTROLLERS=<node-ip> systemd-aether linstor resource list
   ```
   The zip contains `linstordb.mv.db` and nothing else. Restore it on the node that will
   run the controller.
4. **Let the daemons create the schema.** Start `hydra-db`; Spectrum applies
   `helios_schema.py`'s migrations behind the cluster lock. The schema comes from the
   *build you are running*, not from the backup — `meta/schema.cql` in the artefact is
   there for diagnosis and for tables `helios_schema.py` does not know about.
5. **Restore the data**, one artefact per node in the round:
   ```bash
   valcli backup.restore <artefact>
   ```
6. **Restore the CA** if the host holding it was lost and the artefact has it
   (`ca/` members, `contains_ca: true`): copy `ca.key`, `ca.crt`, `ca.srl` into
   `/var/lib/hci/certs_staging/` on the first host in `cluster.json`, `chmod 600` the
   key, and run `impa selftest`. If the artefact does **not** have it, you are re-issuing:
   `impa renew --ca` across every node.
7. `cluster start`, then `valcli vm.list` and `valcli storage.list` and check that what
   they say matches what `linstor resource list` and `drbdadm status` say.

**After any restore of `hydra.sessions`**, existing console session tokens from the
backup come back with it. They are rows with no TTL. Clear them if the backup is old
enough that someone's session should not be resurrected.

### 5.2 The schema check, and why it refuses

Before loading anything, Saga compares the artefact's `schema_migrations` against the
live cluster's, and **refuses on any difference**:

```
Error: schema mismatch between the artefact and this cluster:
  - the backup was taken with migrations this cluster has not applied: 0005-imaginary.
    Restoring would load SSTables for tables that do not exist here.
Deploy the build the backup was taken with and run the restore again, or pass --force
if you have decided this is safe.
```

This is the failure with no symptom. Scylla accepts SSTables written against a different
table definition; the columns that moved simply read wrong afterwards, and nothing
anywhere says so. It is the same reasoning `helios_schema.py` uses when it raises
`SchemaDivergence` on an edited migration.

Three shapes, all refused:

* **the cluster is ahead** — migrations applied here that the backup predates. The
  tables changed shape since the snapshot.
* **the cluster is behind** — the backup carries migrations this cluster has not
  applied. Deploy the matching build first; that is usually the right fix after a
  rebuild, because step 4 above gives you the *current* build's schema.
* **same id, different checksum** — one of the two clusters ran an edited migration.

`--force` proceeds and prints the whole difference first.

### 5.3 Table directories, and the trap in them

A table's on-disk directory is `<table>-<uuid-without-dashes>`, where the uuid is the
table's id in `system_schema.tables`. **Dropping and recreating a table changes that
id, and leaves the old directory behind.** The live test cluster carries four
`schema_migrations-*` directories and three `cluster_locks-*`, one live in each case.

Saga resolves the directory by querying `system_schema.tables` for the current id.
Globbing for `<table>-*` and taking the first match — the obvious implementation —
copies into a directory Scylla has forgotten about, `nodetool refresh` finds nothing,
and the restore reports success while the data never appears.

`schema.cql` and `manifest.json` inside a Scylla snapshot directory are not SSTables and
are not staged into `upload/`; `refresh` would ignore them and leave them there
permanently, where they are indistinguishable from an SSTable a refresh failed to
consume. Anything genuinely left in `upload/` after a refresh is reported as a warning,
because the rows it holds are not in the table.

---

## 6. Scheduling and retention

### 6.1 The schedule

One row in `hydra.dagur_schedules`, registered the same way `mimir_diagnostics`,
`storage_scrub` and `orphaned_disks_cleanup` are:

| Job name | Task type | Cron | Interval | Command |
| :--- | :--- | :--- | :--- | :--- |
| `metadata_backup` | `backup` | `30 1 * * *` | 86400 | `/usr/local/bin/saga backup --all-nodes` |

Catalyst's scheduler thread claims each tick with
[`POST /v1/schedule/claim-job`](./daruk.md#claiming-a-scheduler-tick) and dispatches it
to Dagur on the ZooKeeper leader, which fans it out to the other nodes. See
[dagur.md](./dagur.md).

**It is enabled by default even though a fresh cluster has no target configured**, and
therefore fails every night with:

```
Error: no backup target is configured. Set one with `valcli backup.target <dir>` ...
```

That is the intent. A cluster with no backups should say so once a day in
`valcli scheduler.history`, loudly, in a sentence that says what to do about it. A
schedule disabled by default would be silent, and silence is what "no backup /
disaster recovery" looked like in the first place.

> [!IMPORTANT]
> Dagur executes a scheduled command through `run_remote_spark("127.0.0.1", command)`
> without a `timeout` in the payload, so spark-daemon applies its **45-second default**.
> A metadata backup of the live single-node test cluster (35 tables, 691 SSTable files,
> ~1 MiB compressed) takes about **6.6 seconds** end to end, and `--all-nodes` runs
> peers in parallel rather than in series so a three-node round costs roughly the
> slowest node. A cluster whose metadata has grown far beyond that will hit the ceiling
> and the run will be recorded as FAILED even though the artefacts were written. The
> fix is one line in `dagur.py` — pass a `timeout` in the execute payload — and is
> noted rather than made here.

### 6.2 Retention

Defaults: `saga_keep = 7`, `saga_keep_days = 30` (both `hydra.cluster_settings` keys,
both overridable per run with `--keep` / `--keep-days`). Retention runs at the end of
every backup; `valcli backup.prune --dry-run` previews it.

An artefact is deleted only when it is outside **both** limits: not among the newest
`keep`, *and* older than `keep_days`. So `keep_days=0` gives count-only retention, and
the default keeps a month of nightly backups plus at least the last seven whatever
their age.

Four rules, each closing a way this goes wrong:

* **`keep` is a floor that age can never override.** A policy where "everything is
  older than 30 days" means "delete everything" empties the target during exactly the
  quiet month when nobody is watching.
* **A corrupt artefact never occupies one of the `keep` slots.** Otherwise three bad
  nights push the last good backup out of a `keep=3` window and what is being kept is
  three copies of nothing. A corrupt artefact is still removed once it ages past
  `keep_days` — it is reported as unusable in `saga list` long before then.
* **A node only ever deletes its own artefacts.** The target is shared. A node applying
  its own keep count across every node's files would delete a peer's backups using a
  number the peer never agreed to.
* **Abandoned `.partial` files are swept after 24 hours**, and only then — a younger one
  may belong to a backup another node is still writing.

An in-progress archive is written as `.partial` and renamed only once complete and
flushed. A half-written file carrying the final name is indistinguishable from a good
backup, and the next retention pass would count it as one of the N it is keeping.

Sizing: the live single-node cluster's whole `hydra` keyspace is 2.51 MB on disk and
compresses to about 1 MiB, so 30 nightly artefacts is ~30 MiB per node. Metadata does
not grow like guest data; `catalyst_tasks` carries a 30-day TTL (migration
`0003-bound-task-history`) and the metrics tables carry 24-hour TTLs.

---

## 7. What this does not cover

Stated plainly, because "backup" is a word that invites people to assume more:

* **Guest data is not backed up.** Nothing here reads a DRBD volume. A VM's disk is
  replicated by DRBD, which protects against a host failing and against nothing else —
  not against a guest filesystem corrupting, not against a VM being deleted, not
  against ransomware inside a guest, not against the storage tier being lost on every
  replica. There is no snapshot, no changed-block tracking, no off-site volume
  replication. LINSTOR's own `linstor backup ship` / `linstor snapshot` can do that
  against an S3 remote and Helios does not currently drive it.
* **VM images in the Valhalla catalogue** (`hydra.valhalla_images` → Linstor `img-*`
  resources) are metadata-only here. The rows come back; the image contents do not.
* **The backup is not application-consistent for guests.** It never touches them, so
  the question does not arise — but do not read "consistent point-in-time snapshot" as
  saying anything about what is inside a VM.
* **Not cluster-wide-consistent across nodes.** See
  [§4.2](#42-a-multi-node-cluster-is-a-set-of-artefacts-not-one). Per-node
  point-in-time, cluster-wide within a window of seconds. In-flight lightweight
  transactions (`system.paxos`) are not captured.
* **`system_auth` is not captured** — only the `hydra` keyspace. Helios does not enable
  Scylla authentication; if that changes, this list changes with it.
* **Artefacts are not signed and not encrypted.** Integrity checking detects corruption
  and truncation, not tampering. The artefact contains `hydra.users` password hashes
  and session tokens by construction, and the cluster CA when `--include-ca` is used;
  protecting the target is the operator's job.
* **The restore is not automated end to end.** Rebuilding nodes, restoring the LINSTOR
  database and restoring the CA are documented manual steps, because each of them means
  stopping something on a live cluster and none of them should happen because a cron
  job decided to.
* **Nothing tests the backups but you.** `valcli backup.verify` proves the artefact is
  intact, not that it restores. Restore something on purpose, occasionally.

---

## 8. Command reference

| Command | What it does |
| :--- | :--- |
| `valcli backup.target [<dir>]` | Show or set the target (`saga_target` in `hydra.cluster_settings`) |
| `valcli backup.run [--all-nodes] [--include-ca] [--allow-same-filesystem]` | Take a backup |
| `valcli backup.list` | Artefacts at the target, their health and flags, grouped into rounds |
| `valcli backup.verify [<file>]` | Full integrity check against the manifest (default: newest) |
| `valcli backup.restore [<file>] [--tables a,b] [--force] [--extract-only <dir>]` | Restore |
| `valcli backup.prune [--dry-run] [--keep N] [--keep-days D]` | Apply retention now |
| `saga snapshots [--prune]` | List Scylla snapshots; clear leftover `saga-*` tags |

Flags in `saga list`:

| Flag | Meaning |
| :--- | :--- |
| `LINSTOR` | This artefact contains the LINSTOR controller database |
| `CA` | This artefact contains the cluster CA private key |
| `LOCAL` | Written to the same filesystem as the database — not protected against losing that disk |
| `PARTIAL` | An interrupted run; not a backup |

---

## 9. Verified behaviour

Everything below was run against the live single-node test cluster (Scylla 5.4.0,
LINSTOR 1.31.0, `hydra` at RF=1) on 2026-08-21.

* **Backup.** 35 tables, 691–853 SSTable files, ~1 MiB compressed, **6.6 s** wall clock
  including the LINSTOR database. `nodetool listsnapshots` shows zero `saga-*` tags
  afterwards.
* **Target refusal.** With `saga_target` on the same filesystem as
  `/var/lib/hci/hydra/data`, `saga backup` refuses and exits 1;
  `--allow-same-filesystem` proceeds and the manifest records
  `target_on_data_filesystem: true`.
* **Restore, destructively demonstrated.** On a throwaway `saga_demo` keyspace: three
  rows inserted, backed up, `DROP TABLE`, table recreated (**new uuid**, old directory
  left behind on disk), `saga restore` → all three rows read back. The stale directory
  was untouched; `upload/` drained to empty; a second restore of the same artefact was
  a no-op merge.
* **Schema refusal.** An artefact whose manifest claims a migration the cluster has not
  applied verifies clean and is **refused** at restore, before anything is loaded.
* **Corruption detection.** An artefact truncated from 1 237 296 to 900 000 bytes is
  reported as truncated on the archive digest, on the archive size, on the gzip stream
  ending early, and on each of the 400+ members no longer present.
* **Retention.** Against a target holding five artefacts for this node (one of them
  truncated, the newest) and one for a peer, `--keep 2 --keep-days 30` retained the two
  newest *healthy* artefacts plus the corrupt one (inside the age window) plus the
  peer's, and pruned the two oldest. The peer's artefact was never a candidate.
* **Cleanup.** The `saga_demo` keyspace, its `pre-drop-*` auto-snapshots and its data
  directory were removed afterwards. Ten pre-existing `pre-drop-*` snapshots in `hydra`
  (452 KiB, from other work) were left alone — Saga reports them and does not clear
  what it did not create.

---

## 10. Related

* [hydra.md](./hydra.md) — the keyspace this protects, and its replication factor
* [aether.md](./aether.md) — LINSTOR/DRBD, and why guest data is not in scope here
* [cluster_state.md](./cluster_state.md) — why ZooKeeper is not backed up
* [dagur.md](./dagur.md) — the scheduler that runs the nightly job
* [mtls_lifecycle.md](./mtls_lifecycle.md) — the CA, where it lives, and `impa renew --ca`
* [ring_lifecycle.md](./ring_lifecycle.md) — quorum, and what "the cluster is degraded" means
