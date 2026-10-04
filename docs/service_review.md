# Service review

A read-only review of every service in this repository: what it is for, what it serves, who
calls it, what it owns, who leads it, what happens when its dependencies go away, which tests
cover it, and where the code is dead, duplicated or inconsistent. Every claim carries a
`file:line` taken from the tree this document was written against. Where something was not
verified it says **not checked**.

## 0. Method and limits

* Read in full or in the relevant part: `README.md`, `docs/README.md`,
  `docs/service_leadership.md`, `docs/vali.md`, `docs/mipha.md`, `docs/cluster_state.md`,
  `docs/spark_api.md`, `docs/logos.md`, and the code of every service listed below.
* **(Written before `docs/maintenance.md` landed on the overnight branch; that document now exists and supersedes this review's maintenance remarks.)** The reviewer's tree had no `docs/maintenance.md`: `ls docs/` has no such file and
  `git log --all -- docs/maintenance.md` returns nothing. The host-maintenance flow was
  therefore reviewed from code: `vali.py:2491-2640` (enter/leave), `vali.py:2233-2300` (quorum
  gate and lock), `valcli.py:2884-3010`, `spark_daemon_decoded.py:2815-2882` (leave fallback),
  `spark_daemon_decoded.py:1139,1269,4902,5009` (the `/etc/hci/maintenance.state` readers),
  `mipha.py:528,1681-1790`, `hylia.py:844,1028`.
* "Callers" were found by text search over `*.py`, `mcli`, `mcli-runner`, `catcli`, `allssh`,
  `spectrum_phx/lib`, `static/*`. A URL assembled at run time can be missed; one such case was
  found and is noted (`static/app.js:572`, `/api/maintenance/${type}`).
* The test suite was **not run**. Test coverage statements come from reading which test files
  load or name the module.
* Not reviewed: `urbosa.py`, `impa.py`, `saga.py`, `deploy_updates.py`, `check_updates.py`,
  `create_upgrade_zip.py`, `provision.py` (not touched). Sidon was reviewed at the surface
  (`main.rs`, `control.rs`, `peer.rs`, `tls.rs`, plus greps), not through the extent, journal
  or Purah internals. `spectrum_server.py` was reviewed for its route table, loops and wiring,
  not each handler's business logic.

---

## 1. The three questions

### 1.1 Logos: does every node write the same rows?

**No. Each node writes only its own host's rows. There is no election and no duplicate write,
with one configuration exception.**

* `logos.py:158-281` is one loop per node. It resolves `local_ip = get_local_ip()`
  (`logos.py:160`, read from `LOCAL_HYPERVISOR_IP` in `/etc/hci/spectrum/spectrum.env`,
  `logos.py:22-31`) and every statement it builds uses that address as the partition key.
* Every 5 seconds (`logos.py:281`) it samples `/proc/stat`, `/proc/meminfo`, `/proc/cpuinfo`,
  `/proc/diskstats`, `/proc/net/dev` of **its own host** and writes, in one logged `BEGIN
  BATCH ... APPLY BATCH` (`logos.py:266-270`):
  * one row in `hydra.logos_metrics`
    (`node_ip, timestamp, cpu_pct, mem_pct, mem_total_kb, cpu_cores, disk_iops,
    disk_bandwidth_kbps, net_rx_kbps, net_tx_kbps`; `logos.py:255-261`), keyed
    `PRIMARY KEY (node_ip, timestamp)`, clustering `timestamp DESC`, `default_time_to_live =
    86400` (`helios_schema.py:223`);
  * zero or more rows in `hydra.urbosa_tunnel_metrics`, one per `vxlan*`, `br-ov*`, `veth*`
    interface **on this host** (`logos.py:241-249`, interface filter `logos.py:150`), keyed
    `((node_ip, interface_name), timestamp)`, same 24 h TTL (`helios_schema.py:233`).
* Two nodes can never write the same `(node_ip, timestamp)` unless they resolve the same
  `node_ip`. That happens only if `LOCAL_HYPERVISOR_IP` is absent: `logos.py:22` defaults to
  `127.0.0.1`, so every node lacking the key writes under the partition `127.0.0.1` and their
  samples interleave in one partition. Nothing in Logos warns about it (Vali does warn for the
  same condition, `vali.py:1985-1988`).
* The statement is also built by text interpolation, not bound parameters (`logos.py:241-263`);
  the interface names come from `/proc/net/dev`, not from a caller, so this is not an injection
  path, only a style inconsistency.
* The intent is written down in a test: `test_service_wiring.py:965` lists `logos` under
  `NO_ELECTION`: "every node reports its own, so there is nothing to elect".
* Readers: Spectrum `/api/cluster/metrics` (`spectrum_server.py:4228`), `spectrum_server.py`
  cluster monitor, the Phoenix `Metrics` module (`spectrum_phx/lib/spectrum_phx/metrics.ex:49`,
  bounded per-node read), `spark_daemon_decoded.py:3144,3204` (tunnel metrics).
* Docs are stale: `docs/logos.md:34` says the interval is 30 s (code: 5 s, `logos.py:281`),
  `docs/logos.md:35-37,51,63` describe polling Spark, a "Logos API instance" and a Spark mesh
  route. `logos.py` has no HTTP server, no Spark call and no peer traffic.

### 1.2 Typed-wrapper bypass audit (`valcli.py`, `spark_daemon_decoded.py`)

What `test_spark_shell_calls.py` actually guards:

* Files scanned: only `CALLERS = [spectrum_server.py, cluster_new.py, vali.py, hylia.py,
  mipha.py, dagur.py]` (`test_spark_shell_calls.py:48-51`). **`valcli.py` is not on the list.**
  Neither are `mimir.py`, `catalyst.py`, `lanayru.py`, `rauru*.py`, `urbosa_bootstrap.py`,
  `mcli`, `mcli-runner`, `bifrost.py`, `gatoway.py`, or `spark_daemon_decoded.py` itself.
* Families scanned: exactly two, `systemctl\s+\S` (`:58`) and `ss|netstat -`, `ip route|addr|
  link|neigh`, `/sys/class/net` (`:62-66`). Nothing checks `virsh`, `rm`, `mkdir`, `cp`,
  `podman`, `reboot`, `tar`, `nc`, `nodetool`, `qemu-img`, `genisoimage`, `base64 -d >` writes.
* Mechanism: string *literals* from the AST (f-strings with `{}`), allow-list
  `KNOWN_SHELL_COMMANDS` (`:79`). A command assembled from variables
  (`cmd = "sys" + "temctl ..."`, or a `%` / `.format` template whose literal lacks the
  `systemctl x` shape) is invisible to it.

Running the test's own scanner over the files it does not cover
(`literal_strings` + `FAMILIES`) finds these call sites that reach spark `/api/v1/execute`:

| Where | What | Family |
| :--- | :--- | :--- |
| `valcli.py:3050-3054` | `cmd_cluster_vip_set` writes `cluster.json` with `echo <b64> \| base64 -d > ... && systemctl restart bifrost` to every host through `run_remote_spark` | systemctl, in an uncovered file |
| `valcli.py:2531-2537` | `/usr/local/bin/mcli-runner --category all` (local `subprocess.run(..., shell=True)` or remote execute) | shell, uncovered family |
| `lanayru.py:135-140` | six `ip link/addr` commands through `run_remote_spark` | network, uncovered file |
| `urbosa_bootstrap.py:141,186` | `systemctl stop/disable/enable/start urbosa` plus `ip netns` loop, through execute | systemctl and network, uncovered file |
| `mimir.py:482-483` | `/usr/local/bin/mcli health_checks <category>` built from a Hydra row, through execute | shell |
| `spectrum_server.py:1807,1916` | Dagur job whose command is `printf '{"op":"purah-scrub"}' \| nc -U /run/sidon/control.sock`, i.e. a raw call to Sidon's control socket that bypasses the `/api/v1/dfs/vdisk` allow-list; the row is rewritten on every Spectrum start | shell, covered file but not the `nc` family |

Other shell-string execute call sites in files that **are** on the list, because their family
is not covered (AST count of `run_remote_spark`/`run_parallel` calls with a command argument):
`spectrum_server.py` 37, `cluster_new.py` 57, `hylia.py` 38, `vali.py` 15 (`free -m`,
`test -e /dev/kvm`, `systemd-detect-virt`, `virsh ... undefine`, `rm -f /var/lib/hci/aether/
nvram/...`, and the maintenance marker `mkdir -p /etc/hci && touch /etc/hci/maintenance.state`
at `vali.py:1865` and `rm -f` at `:1904`, `nodetool status` at `:2171`), `lanayru.py` 22
(`qemu-img`, `genisoimage`, `virsh define/start/destroy/undefine`, `rm -rf`), `mipha.py` 3
(`pgrep -a qemu`, `nodetool status`, the legacy fence), `valcli.py` 2, `dagur.py` 1 (the
generic job runner, by design).

`spark_daemon_decoded.py` itself runs 41 `subprocess.*(shell=True)` calls locally (AST count).
That is the daemon acting on its own host, not a bypass, but three places are worth a decision:

* `spark_daemon_decoded.py:2873` runs a hard-coded `systemctl start zookeeper hydra-db sidon
  spectrum spectrum-phx bifrost dagur mimir rauru vali catalyst gatoway logos mipha daruk
  agahnim slate` in the maintenance-leave fallback. It omits `hylia` and `urbosa` that
  `MANAGED_SERVICES` (`:632-659`) declares, and the branch writes nothing to Hydra
  (`:2868-2882`), so `hydra.nodes.status` is not moved by the fallback. **Not checked
  end-to-end.**
* `spark_daemon_decoded.py:3517-3594` (`handle_cluster_create`) and `:3640-3660`
  (`handle_cluster_destroy`) restart units and grep `ss -tlnp` through shell strings.
* `spark_daemon_decoded.py:5001-5055`: a second reconciler (see risk R-2).

Typed endpoints that nobody calls are listed per service below (spark section 3.2) and in
section 4.3.

### 1.3 Unused code and duplicated logic

Both are itemised in section 4. Headlines, each with its grep:

* **Duplicated across files** (AST-normalised function bodies compared): `spark_endpoint` in
  12 files, `run_remote_spark` in 12 files (10 shapes), `run_lwt` in 5, `candidacy()` wrapper in 7,
  `get_catalyst_target_ip` in 6 variants, `get_zookeeper_leader_ip` in 6 definitions in 5 files
  (`spectrum_server.py` defines it twice), `parse_nodetool_status` in 4, `parse_json_rows` in 4,
  `check_urbosa_enabled` in 3, `spark_local_ip` in 2, `get_local_ip` in 6 definitions of 4 shapes, **about 30
  literal lists of service names**, three `x // 2 + 1` quorum helpers plus six inline uses,
  and `generate_vm_xml` twice (one copy has no production caller).
* **Dead**: five spark endpoints with no caller, 17 Phoenix `Spark` wrappers with no caller,
  the Catalyst `spark` queue, a Daruk operation, two Python CLI helpers, a Rust module behind
  `#[allow(dead_code)]`, and one whole Python module (`rauru_replication.py`) imported only by
  its own test.

---

## 2. Cross-cutting map

### 2.1 Ports and transports

| Port | Service | Transport | Bound to | Evidence |
| :--- | :--- | :--- | :--- | :--- |
| 9099 | spark-daemon | HTTPS, mTLS (`CERT_REQUIRED`) | all | `spark_daemon_decoded.py:45,5059-5129` |
| 9095 | vali | HTTPS, mTLS | `0.0.0.0` | `vali.py:2679` (docs/vali.md still says "listening locally") |
| 9091 | catalyst | HTTPS, mTLS | `0.0.0.0` | `catalyst.py:830` |
| 9043 | daruk | plain HTTP, no auth | `127.0.0.1` | `daruk.py:942` |
| 9042 | Hydra (ScyllaDB) | CQL | host network | `spark_daemon_decoded.py:633` |
| 2181 | ZooKeeper | ZK wire protocol | host network | `helios_zk.py` |
| 9105 | sidon peers | custom frames, mTLS off loopback, **refuses plaintext off loopback** | node address | `sidon/src/peer.rs:762-781`, `tls.rs:59-72` |
| unix | sidon control | line JSON | `/run/sidon/control.sock`, mode 0600 | `sidon/src/control.rs:281` |
| unix | sidon NBD | one socket per attached vdisk, 0660 group `qemu` | `/var/lib/hci/sidon/nbd/` | `control.rs:678-684` |
| 8443 | spectrum | HTTPS | all | `spectrum_server.py:42,8765` |
| 8444 | spectrum_phx | plain HTTP | `127.0.0.1` | `spectrum_phx/quadlet/spectrum-phx.container:55,64` |
| 8089 | console token verifier (inside spectrum) | plain TCP, no auth | `127.0.0.1` | `spectrum_server.py:8640-8647`, dialled by `agahnim/src/main.rs:250` |
| 8081 | agahnim | WebSocket proxy, TLS if certs exist, else plain | `0.0.0.0` | `agahnim/src/main.rs:49-94` |
| 443 | slate (ingress) | TLS | all | `slate_config/traefik.yml` |

### 2.2 Leadership (`helios_zk.py:1404-1414`, `docs/service_leadership.md`)

| Election name | Stands | Used at |
| :--- | :--- | :--- |
| `catalyst-dispatch` | every catalyst | `catalyst.py:166,176,752`; read by `dagur.py:139`, `hylia.py:1167`, `mipha.py:224`, `vali.py:367`, `spectrum_server.py:8584` |
| `catalyst-scheduler` | every catalyst | `catalyst.py:350` |
| `vali-queue` | every vali | `vali.py:1991` |
| `vali-drs` | every vali | `vali.py:814` |
| `dagur-queue` | every dagur | `dagur.py:262` |
| `lanayru-queue` | every spectrum backend | `spectrum_server.py:3077` |
| `mimir-schedules` | every mimir | `mimir.py:431` |
| `hylia-upgrades` | every hylia | `hylia.py:1463` |
| `mipha-ha` | every mipha | `mipha.py:1999` |
| `bifrost-vip` | every bifrost, only while locally healthy | `bifrost.py:118,254-259` |
| `rauru-snapshots` | every rauru | `rauru.py:282` |

No election: sidon, daruk, spark-daemon, logos, gatoway, agahnim, slate, spectrum_phx,
spectrum (except `lanayru-queue`), urbosa (`test_service_wiring.py:952-966`). The 11 constants
are all used; none is orphaned.

### 2.3 ZooKeeper paths in use

| Path | Type | Writer | Readers |
| :--- | :--- | :--- | :--- |
| `/helios/leaders/<service>/n_<seq>` | ephemeral sequential | `helios_zk.Candidacy` (`helios_zk.py:1058-1414`) | the election peers; `/helios/leaders/catalyst-dispatch` is read by 6 daemons for the queue holder |
| `/helios/nodes/<ip>` | ephemeral | each spark-daemon, every 5 s (`spark_daemon_decoded.py:592,703-736`) | `cluster_new.py:272-304`, `spectrum_phx/lib/spectrum_phx/zk/state.ex:23` |
| `/cluster_state` | persistent | spark-daemon on `POST /api/v1/cluster/state` (`:888-893`) | each spark reconcile loop (`:1117,1132`), `cluster_new.py:235`, Phoenix `zk/state.ex:24` |
| `/zookeeper/config` | ZK built-in | ZooKeeper | `cluster_new.py:1360-1372` (ensemble reconfig) |

No service other than these touches ZooKeeper. `saga` deliberately backs up none of it
(`docs/cluster_state.md` section 7).

### 2.4 Hydra tables: who writes what

Counts are statement occurrences found by regex over each file (`INSERT/UPDATE/DELETE/TRUNCATE`
and `FROM hydra.<t>`); writes made through Daruk typed endpoints or through
`helios_schema` statement builders are attributed separately.

| Service | Writes (direct CQL) | Via Daruk `/v1/...` or builders |
| :--- | :--- | :--- |
| vali | `nodes`, `vali_drs_status`, `vali_drs_history`, `vm_nvram`, `gatoway_networks` | `vm/claim,release,migrate-*`, `node/maintenance`, `lock/*`; `catalyst_tasks` via Catalyst |
| catalyst | none | `catalyst_tasks` via `helios_schema.task_*_statement`, `schedule/claim-job`, `catalyst/claim-sequence` |
| mipha | `nodes` (2), `catalyst_tasks` progress (8) | `node/maintenance`, `lock/renew`, `vm/release`, `dfs/claim` (broken, see R-1) |
| dagur | `dagur_runs` | none |
| mimir | `mimir_results`, `mimir_schedules` | none (`schedule/claim-check` exists and is not used, R-5) |
| hylia | `hylia_jobs`, `hylia_logs` | none |
| rauru / snapshots / protection | `dfs_snapshot_*`, `dfs_protection_*` (statement builders) | `catalyst/claim-sequence` |
| logos | `logos_metrics`, `urbosa_tunnel_metrics` | none |
| gatoway | none (read only) | none |
| spectrum | 21 tables incl. `users`, `sessions`, `storage_containers`, `dagur_schedules`, `urbosa_*`, `valhalla_images`, `vms` | `vm/*`, `network/*-vlan` |
| lanayru | `lanayru_clusters`, `lanayru_k8s_state`, `urbosa_segments`, `vm_nvram`, `vms` | `vm/create`, `vm/set-state` |
| spark-daemon | `vm_nvram` (every 5 s file scan, `:5083-5112`) | none |
| sidon | `dfs_*` | `dfs/*` (11 operations, see 3.1) |
| valcli | `dagur_runs`, `mimir_results`, `valhalla_images`, `vali_drs_history`, `vali_tasks` (cleanup only) | none |

---

## 3. Services

Format per service: purpose, entry points, endpoints and callers, config, state, leadership,
failure behaviour, tests, findings. Finding ids (R-n) refer to section 4.2.

### 3.1 sidon (Rust, `sidon/`)

**Purpose.** Per-node storage data path: serves vdisks to qemu over NBD unix sockets
(journal, overlay, extent store), replicates journal appends write-all to peers, and runs
Purah, the background curator (sweep, scrub, heal, heat, tier, compact, dedup). Placement and
ownership metadata live in Hydra through Daruk.

**Entry points.**

* `sidon/src/main.rs:186` `main`: `sidon mounts [apply]` subcommand (`main.rs:190-191`,
  `mounts::command`) and otherwise `Daemon::new` + `Daemon::run` (`control.rs:270-304`).
* Threads: accept loop on the control socket (main thread); one thread per control
  connection (`control.rs:294`); one listener thread per attached vdisk plus one per NBD
  connection (`control.rs:693`, `nbd.rs`); peer listener thread and one thread per peer
  connection (`peer.rs:787,794`); Purah degraded-watcher every 5 s (`control.rs:1647`); access
  tally flusher every `SIDON_ACCESS_FLUSH` s (`control.rs:1687`); sweep/heal/scrub loop every
  `SIDON_PURAH_INTERVAL` s (`control.rs:1724-1765`).

**Control socket** (`/run/sidon/control.sock`, mode 0600, line-delimited JSON; reply
`{"ok":true,...}` or `{"ok":false,"error":...,"kind":"io|corrupt|meta|refused"}`,
`control.rs:305-345`). Dispatch table `control.rs:350-377`. The only caller is spark-daemon
(directly through `helios_sidon.py`, or via `POST /api/v1/dfs/vdisk`, whose allow-list is
`spark_daemon_decoded.py:4356-4376`) plus the raw `nc -U` Dagur job in R-8.

| op | Fields | Does | Callers found |
| :--- | :--- | :--- | :--- |
| `ping` | - | `{node}` | `spark.py:38` |
| `create` | `vdisk_id, size_bytes, [container, class, extent_bytes, egroup_bytes, rf, replicas]` | metadata-only vdisk create (`control.rs:381`) | `spectrum_server.py:6079`, `lanayru.py:209`, `valcli.py:894`, Phoenix `spark.ex` `dfs_create` |
| `attach` | `vdisk_id, [forward]` | win `(owner, epoch)` CAS, fence replicas, open NBD socket (`:473`) | `vali.py:1493,1521`, `spectrum_server.py:6083,6383,6862`, `lanayru.py:219`, `valcli.py:904`, `images.ex:624` |
| `detach` | `vdisk_id` | close NBD, drain (`:716`) | `vali.py:1497,1531`, `cluster_new.py:618`, `valcli.py:935,2375,2519`, `spectrum_server.py:1161,1374,6185`, `lanayru.py:507`, `spark_daemon_decoded.py:866,2531` |
| `delete` | `vdisk_id` | delete map (`:751`) | `valcli.py:937,2383,2523`, `spectrum_server.py:1162,1375`, `lanayru.py:508`, `helios_snapshots.py:636`, `rauru_protection.py:919`, Phoenix `vms.ex:535,741` |
| `list` | - | attached vdisks and role (`:1195`) | `helios_snapshots.py:465`, `mipha.py:741`, `cluster_new.py:623`, `hylia.py:687`, Phoenix |
| `status` | `vdisk_id` | one vdisk (`:1233`) | `valcli.py:817` only (Phoenix `dfs_status` has no caller) |
| `flush` | `vdisk_id` | drain journal (`:1240`) | **none found** (`helios_sidon.flush`, Phoenix `dfs_flush` unused) |
| `seal` | `vdisk_id` | immutable (`:787`) | `spectrum_server.py:6156`, `images.ex:626` |
| `snapshot` | `vdisk_id, child_id` | map copy, class immutable (`:828`) | `helios_snapshots.py:581`, `rauru_protection.py:868`, `valcli.py:980` |
| `clone` | `vdisk_id, child_id, [rf]` | map copy, class rw (`:839`) | `valcli.py:980` only |
| `rollback` | `vdisk_id, snapshot_id, [keep_as]` | in-place rollback of a detached vdisk (`control/rollback.rs`) | `helios_snapshots.py:703` |
| `resize` | `vdisk_id, size_bytes` | grow (`:1017`) | `spectrum_server.py:6820` (Phoenix `dfs_resize` has no caller) |
| `capacity` | - | per-disk and total space (`:1074`) | `vali.py:887`, `hylia.py:666`, `valcli.py:604`, `cluster_new.py:621`, `spectrum_server.py:2497,3597`, `spark_daemon_decoded.py:3020`, `mipha.py:1949` |
| `peers` | - | peer table and reachability (`:1182`) | `cluster_new.py:622`, Phoenix `storage.ex:273` |
| `purah-heal` | `[restore_rf, vdisk_id]` | re-replicate (`:1302`) | `valcli.py:1537` |
| `purah-sweep` | - | reclaim unreferenced groups (`:1474`) | `valcli.py:2149`; also the timer |
| `purah-scrub` | - | re-hash sealed groups (`:1505`) | `valcli.py:2200`, `mipha.py:1905-1925` (nightly), the timer, and the Dagur `nc -U` job |
| `purah-heat` | `[limit]` | rank groups by access (`:1522`) | `valcli.py:1992` |
| `purah-placement` | `[limit]` | disk-to-group map (`:1545`) | `valcli.py:1614` |
| `purah-tier` | `[apply, max_bytes, ...]` | plan or apply disk moves (`:1554`) | `valcli.py:1662` |
| `purah-move` | `egroup_id, disk` | move one group (`:1577`) | `valcli.py:1707` |
| `purah-compact` | `[apply, threshold, ...]` | compact sparse groups (`:1587`) | `valcli.py:1725` |
| `purah-dedup` | `[sample, rate_bytes_per_second, digests]` | read-only estimate (`:1618`) | `valcli.py:1822` |

Spark forwards all 24 (`DFS_VDISK_OPS` 11 + `DFS_NODE_OPS` 13, `spark_daemon_decoded.py:4356-
4376`): **all 24 control ops are exposed**. `docs/spark_api.md` ("Per vdisk" row) lists only
`create attach detach delete status flush seal resize`, so `snapshot`, `clone`, `rollback` are
undocumented there.

**Peer protocol** (`sidon/src/peer.rs:55-84`, port 9105, frame = 44-byte header + vdisk name +
payload, CRC32C over all of it, desync means drop the connection, `peer.rs:20-38`). Status
codes `ST_OK 0, ST_STALE_EPOCH 1, ST_IO 2, ST_NOT_FOUND 3, ST_REFUSED 4` (`:97-104`). The only
peers are other sidon daemons; nothing else speaks it.

| Opcode | Name | Handler | Meaning |
| :--- | :--- | :--- | :--- |
| 1 | `PING` | `peer.rs:647` | liveness |
| 2 | `APPEND` | `:655` | journal append at the writer's epoch; stale epoch gives `ST_STALE_EPOCH` with the fence |
| 3 | `FENCE` | `:648` | persist a higher epoch (fsynced before ack) |
| 4 | `READ_TAIL` | `:670` | read the replicated journal (takeover) |
| 5 | `TRUNCATE` | `:674` | empty a journal at an epoch |
| 6 | `EGROUP_PUT` | `:691` | store a replica extent group |
| 7 | `EGROUP_GET` | `:700` | read a replica extent group |
| 8 | `FORWARD_READ` | `:715` | guest read relayed to the owning node |
| 9 | `FORWARD_WRITE` | `:726` | guest write relayed to the owning node |
| 10 | `TRUNCATE_TO` | `:681` | drop the drained prefix, keep the tail |
| 11 | `EGROUP_DROP` | `:734` | "I declare these groups dead", replica re-checks Hydra and drops only what that proves; max 256 ids |

Unknown opcodes answer `ST_REFUSED` (`:705-708`), which is how a mixed-version rollout stays
safe (`peer.rs:62-84`). Sidon has no HTTP surface and no ZooKeeper dependency (`grep -rln
"2181\|zookeeper" sidon/src` is empty).

**Config.** Env (all read in `main.rs:198-255` unless noted): `SIDON_ROOT`, `SIDON_CONTROL`,
`SIDON_DARUK`, `SIDON_HIGH_WATER`, `SIDON_DARUK_TIMEOUT`, `SIDON_PURAH_INTERVAL`,
`SIDON_PURAH_GRACE`, `SIDON_PURAH_OPEN_ABANDON`, `SIDON_PEER_BIND`, `SIDON_PEERS`,
`SIDON_PEER_TIMEOUT`, `SIDON_FENCE_TIMEOUT`, `SIDON_CLUSTER_FTT`, `SIDON_ACCESS_FLUSH`,
`SIDON_ACCESS_CAPACITY`, `SIDON_NODE`, `SIDON_CLUSTER_JSON` (`main.rs:78`), `SIDON_CERT_DIR`
(`tls.rs:46`), `SIDON_DISKS_MANIFEST` (`mounts.rs:162`), `SIDON_FSTAB` (`mounts.rs:168`),
`SIDON_COMMIT_LINGER_US` (`vdisk/commit.rs:70`). Files: `/etc/hci/cluster.json` (peers, ftt,
bind address), `/etc/hci/sidon-disks`, `/etc/hci/spark/certs/{ca.crt,node.crt,node.key}`.
`provision.py:1738-1741` sets only `SIDON_ROOT`, `SIDON_CONTROL`, `SIDON_DARUK`.

**State.** Hydra via Daruk: `dfs_vdisks`, `dfs_block_map`, `dfs_egroups`, `dfs_extent_id_map`,
`dfs_egroup_access`, `dfs_snapshot_*` readers, `storage_containers` (ftt). Daruk operations
used (grep of `sidon/src`): `dfs/vdisk-create, claim, drain-commit, vdisk-resize, set-replicas,
egroup-create, vdisk-class, vdisk-seal, egroup-state, block-map-repoint, extent-repoint`. ZK:
none. Local: journal, overlay, extent groups, replica store, fence epochs under `SIDON_ROOT`.

**Leadership.** None. Ownership of a vdisk is `(owner, epoch)` on the vdisk row
(`test_service_wiring.py:956-958`).

**Failure behaviour.**

* Daruk or Hydra down: create, attach, claim, drain-commit fail with `kind:"meta"`; the
  Purah loops log and retry next tick (`control.rs:1742-1744`). I/O on already-attached vdisks
  continues from the journal until the high-water mark.
* Peer down: write-all means an unreachable replica stops writes and the guest sees EIO; a
  degraded watcher triggers heal within 5 s (`control.rs:1640-1665`). This is the documented
  trade (comment at `control.rs:1640-1645`).
* Certificates missing off loopback: refuses to start the peer listener
  (`tls.rs:59-72`, `peer.rs:762-781`). Single-host clusters bind loopback
  (`main.rs:145-170`).
* ZooKeeper down: no effect.

**Tests.** `cargo test` in `sidon/` (371 `#[test]`, of which `control.rs` 6, `peer.rs` 22,
`control/rollback.rs` 13); Python: `test_dfs_endpoint.py` (the spark allow-list),
`test_sidon_fstab_boot.py`, `test_sidon_mount_survey.py`, `test_sidon_owns_its_mounts.py`,
`test_vdisk_replication_factor.py`, `test_storage_sweep.py`, `test_compaction.py`. The 24-op
control dispatch has 6 direct tests in `control.rs`; coverage of the individual ops through
other modules was **not checked**. `ganon/` (14 tests) is the fault-injection harness.

**Findings.** R-12 (`#[allow(dead_code)] mod replicate`), R-13 (stale module doc),
R-8 (raw control-socket caller), plus scrub cadence R-14.

### 3.2 spark-daemon (`spark_daemon_decoded.py`) and the cluster CLI (`cluster_new.py`)

**Purpose.** spark-daemon is the only component allowed to act on a hypervisor: typed host
endpoints, a raw `execute` escape hatch, VM and storage brokers, forwarding to Vali, and the
ZooKeeper-backed publish/reconcile loops. `cluster` is the operator CLI for create, status,
start, stop, destroy, ring and ensemble operations.

**Entry points (spark-daemon).** `main()` `spark_daemon_decoded.py:5059`; `ThreadingHTTPServer`
subclass `SecureHTTPServer` (`:4833`) on `:9099`, mTLS (`:5059-5129`). Daemon threads
(`:5074-5122`): `check_cluster_and_autostart` (autostart, then a 30 s watchdog, `:4838-5055`),
`settings_sync_loop` (60 s, `:564-571`), NVRAM watcher (5 s, `:5083-5112`), `zk_publisher_loop`
(5 s, `:703`), `zk_reconcile_loop` (`:1061`, watch on `/cluster_state`, drift check 30 s, re-read
300 s: `:592-598`).

**Endpoints.** Router: `do_GET` `:2887`, `do_POST` `:2917`, typed routers `route_typed_get`
`:3908`, `route_typed_post` `:3971`.

| Method | Path | Does | Callers |
| :--- | :--- | :--- | :--- |
| GET | `/api/v1/cluster/status` | zk/state/storage summary | `cluster_new.py:3054` (fallback path) |
| GET | `/api/v1/node/status` | `build_node_status()` (`:1208`) | `vali.py:669,1314,1343,1628`, `mipha.py:1173,2063`, `hylia.py:986`, `spectrum_server.py:2446`, Phoenix `spark.ex:58` |
| GET | `/api/v1/node/binary-version` | sha/version of a path | `check_updates.py:36,246`, `spectrum_server.py:3233,5121,5312,5405` |
| GET | `/api/v1/vm/drs` | forward to Vali `/api/v1/drs/status` | `valcli.py:363`, `spectrum_server.py:3525` |
| GET | `/api/v1/hosts` | forward to Vali `/api/v1/hosts` | `valcli.py:2757,2886,2940` |
| GET | `/api/v1/urbosa/tunnels/metrics`, `/status` | Hydra read | `spectrum_server.py:3947,3956`, Phoenix `spark.ex:336` |
| POST | `/api/v1/execute` | `subprocess.run(shell=True)` as root | 13 clients (section 4.3 D-3) |
| POST | `/api/v1/cluster/start`, `/stop` | `declare_cluster_state` fan-out | **none** |
| POST | `/api/v1/cluster/create`, `/destroy` | duplicate of the CLI flows | **none** |
| POST | `/api/v1/cluster/sync-settings` | `sync_cluster_settings_local` | **none** |
| POST | `/api/v1/cluster/state` | write `/cluster_state` (and `stop_state_store`) | `cluster_new.py:500` |
| POST | `/api/v1/vm/power`, `/vm/migrate`, `/vm/balance` | forward to Vali (`vms/power` etc., plural) | `valcli.py:320,331,342,353`, `spectrum_server.py:6583,6629,6650` |
| POST | `/api/v1/host/maintenance` | forward to Vali `hosts/maintenance`; Vali-down fallback `:2815-2882` | `valcli.py:2908-3009`, `hylia.py:844,1028`, `spectrum_server.py:8141,8240,8336` |
| GET | `/api/v1/vm/{n}/interfaces`, `/console`, `/info` | `virsh domiflist/dumpxml/dominfo` | `/interfaces`: `spectrum_server.py:1642,2870`; `/console`: `spectrum_server.py:4631,4761`; `/info`: **none** (Phoenix wrappers `spark.ex:97-103` are unused) |
| POST | `/api/v1/vm/{n}/power` | `virsh start/destroy/reboot/shutdown/reset/suspend/resume` | `valcli.py:1281`, `spectrum_server.py:1305,8486`, `rauru_protection.py:494` (pause) |
| POST | `/api/v1/vm/define` | `virsh define` from base64 XML | **none** (only the unused Phoenix wrapper, `spark.ex:111`) |
| POST | `/api/v1/vm/undefine` | `virsh undefine` | `spectrum_server.py:1310,3458,4015,8471,8488` |
| GET | `/api/v1/storage/device`, POST `.../device/prepare`, `/write`, `/flush` | device probing/preparing | **none** (only unused Phoenix wrappers, `/write` not even wrapped) |
| GET | `/api/v1/storage/container/mounted`, POST `.../container/ensure` | container mountpoint | `spectrum_server.py:802,6790` |
| POST | `/api/v1/dfs/vdisk` | allow-listed Sidon control op | 8 files, see 3.1 |
| POST | `/api/v1/dfs/write` | raw body to a vdisk | `spectrum_server.py:6130`, Phoenix `images.ex:642-692` |
| POST | `/api/v1/lcm/package` | raw body to `/tmp/helios_update.zip` | Phoenix `spark.ex:403` |
| GET | `/api/v1/host/cpu`, `/memory`, `/disks`, `/network`, `/capabilities`, `/units`, `/listeners`, `/interfaces`, `/dhcp-leases` | typed host reads | `spectrum_server.py`, Phoenix `spark.ex`, `vali.py:200,1213`, `cluster_new.py:757,922,986,2490`, `mipha.py:683,725`, `hylia.py:135` |
| POST | `/api/v1/host/units` | `systemctl` on allow-listed units | `cluster_new.py`, `vali.py:200`, `mipha.py:683`, `hylia.py:135`, `spectrum_server.py:513`, Phoenix `settings/apply.ex:325` |
| POST | `/api/v1/host/reboot` | reboot (`confirm:true`) | `spectrum_server.py:8299` (Phoenix wrapper unused) |
| POST | `/api/v1/host/fence` | fence and read back | `mipha.py:699,1323` |
| GET | `/api/v1/db/ring`, POST `/db/repair` | `nodetool status`, background `nodetool repair` | `spectrum_server.py:3571,706`, Phoenix `spark.ex:355,364` |

Counted from the routers: 30 typed paths (15 GET, 15 POST incl. the three `{name}` routes);
`docs/spark_api.md` says 28.

**Config.** No environment variables. Files: `/etc/hci/spark/certs/{ca,node}.crt,node.key`,
`/etc/hci/cluster.json`, `/etc/hci/maintenance.state`, `/etc/hci/spectrum/spectrum-phx.env`
(`:276`), `/run/hci/cluster_operation.lock`, `/run/hci/mipha-self-fence.json` (`:2403`),
`/var/lib/hci/aether/nvram` (`:5090`). Constants: `ZK_*` `:586-600`, `ALLOWED_PATH_ROOTS`
`:1505`, `MANAGED_SERVICES` `:632`, `MANAGED_UNITS` `:1537`.

**State.** ZooKeeper: owns `/helios/nodes/<ip>` and writes `/cluster_state`. Hydra: writes
`hydra.vm_nvram`; reads `cluster_settings`, `nodes`, `urbosa_*`.

**Leadership.** Every node; no election.

**Failure behaviour.** ZooKeeper down: publisher and reconciler retry, the mTLS API keeps
serving, `cluster status` falls back to direct probes (`docs/cluster_state.md` section 5).
Hydra down: settings sync and tunnel reads fail with 500, nothing else. Vali down:
`forward_to_vali` returns 500, except the maintenance-leave fallback (`:2815-2882`). Peer
down: `run_remote_spark` returns `(-1, "", error)`.

**Tests.** `test_spark_daemon.py`, `test_dfs_endpoint.py`, `test_spark_shell_calls.py`,
`test_run_argv_shape.py`, `test_cluster_declarative.py`, `test_zk_watches.py`,
`test_zk_reconfig.py`, `test_storage_prep_timeout.py`, `test_no_secure_boot_gate.py`,
`test_node_identity.py`, `test_sidon_*`. Cross-cutting static checks also load it:
`test_service_wiring.py`, `test_unbound_names.py`, `test_internal_api_auth.py`. The
Vali-down maintenance fallback and the 30 s autostart watchdog have no direct test that I
found (**not checked**: `grep -ln "check_cluster_and_autostart" test_*.py` was not run).

**cluster CLI** (`cluster_new.py:2405` `main`). Commands: `create status start stop destroy
ring decommission rejoin add-node zk-promote zk-demote` (`:2424`). Flags `-s -r -v --verbose
--json --node --replacing -y --finalize`. Dispatch lines: `create :2441`, `status :3007`,
`start :3085`, `stop :3209`, `destroy :3305`, `ring :3640`, `decommission :3675`, `rejoin :3934`.
`start`/`stop` name no services and only call `POST /api/v1/cluster/state` (`:485-506`);
`status` reads ZooKeeper (`:272-304`) and falls back to `GET /api/v1/cluster/status` through
the VIP (`make_request :2303`). Config: `/etc/hci/cluster.json`, `/root/.certs/*`,
`/etc/hci/spectrum/spectrum.env`, no environment variables. Hydra: reads/writes
`cluster_settings`, `nodes`, `vms`. Leadership: none; it takes a per-node
`/run/hci/cluster_operation.lock` file (`:1022-1033`) rather than a cluster lock. Tests:
`test_cluster_declarative.py`, `test_add_node_redundancy.py`, `test_destroy_confirmation.py`,
`test_status_without_a_cluster.py`, `test_replication_topology.py`, `test_ring_lifecycle.py`,
`test_zk_reconfig.py`, `test_create_progress.py`, `test_create_health_findings.py`,
`test_storage_prep_timeout.py`, `test_multi_disk.py`.

**Findings.** R-2 (second reconciler), R-3 (`cluster_new.py` TLS), R-9 (dead endpoints),
D-1 (duplicate create/destroy), D-5 (service lists), D-2 and 4.1 item 8 (`run_mtls_spark_api` defined twice,
`:377` and `:470`; the later one wins and has a 30 s timeout, not 120 s), `:5106,5108` write the
literal characters `\\n` (an escape left over from the "decoded" copy), `:3044-3046` Gluster-
era `volume name:` / `options reconfigured:` filter that can no longer match.

### 3.3 vali (`vali.py`)

**Purpose.** VM placement, power, migration, DRS and host-maintenance orchestration.

**Entry points.** `main()` `vali.py:2651`: `init_db_schema` (`:515`), thread
`queue_thread_loop` (`:1975`, drains Catalyst's `vali` queue, per-task thread), thread
`drs_thread_loop` (`:814`, every 30 s), mTLS `ThreadingHTTPServer` on `0.0.0.0:9095`
(`:2663-2680`).

**Endpoints** (all mTLS; handler `ValiAPIHandler`, `:2373`).

| Method | Path | Does | Callers |
| :--- | :--- | :--- | :--- |
| GET | `/api/v1/hosts` | `hydra.nodes` rows | spark forward (`spark_daemon_decoded.py:2901`), `mipha.py:325` (`check_vali_health`) |
| GET | `/api/v1/drs/status` | balance score, last 15 migrations | spark forward from `GET /api/v1/vm/drs` |
| POST | `/api/v1/vms/power` | submit `vali` task, wait up to 60 polls | spark forward |
| POST | `/api/v1/vms/migrate` | submit and wait up to 300 polls | spark forward |
| POST | `/api/v1/vms/balance` | start `run_drs_loop` on **this node** in a thread (`:2486-2489`) | spark forward |
| POST | `/api/v1/hosts/maintenance` | enter: quorum gate, cluster lock, claim `ENTERING_MAINTENANCE`, submit task; leave: submit task | spark forward |

The worker also handles Catalyst `vali` actions (`process_queue_task :1401`): start, stop,
reboot, shutdown, reset, migrate, `host_maintenance_enter`, `host_maintenance_leave`.

**Config.** `LOCAL_HYPERVISOR_IP` from `/etc/hci/spectrum/spectrum.env` (warns if absent,
`:1985-1988`); `/etc/hci/spark/certs/*` (refuses to start without them, `:2663-2678`);
`/etc/hci/cluster.json`; `/etc/hci/maintenance.state` written and removed through spark
`execute` (`:1865,1904`).

**State.** Hydra: `nodes`, `vali_drs_*`, `vm_nvram`, `gatoway_networks`, `cluster_locks`
(DDL `:2099`), plus Daruk `vm/*`, `node/maintenance`, `lock/*`. `hydra.vali_tasks` is created
and never written (`docs/vali.md` "vestigial", only `valcli.py:3145-3158` touches it).
ZooKeeper: `vali-queue`, `vali-drs`.

**Leadership.** Two candidacies. Every other path (HTTP) runs on whichever node receives it.

**Failure behaviour.** ZooKeeper unreachable: `leading()` is False, so neither loop acts.
Catalyst unreachable: the worker loop logs and sleeps 2 s (`:2055-2062`); HTTP handlers return
500 with the Catalyst error. Daruk down: placement claims fail, tasks fail with the claim error.
A host in maintenance whose Vali fell over is handled by spark's fallback, not by Vali.

**Tests.** `test_daruk_lwt.py`, `test_drs_storage_gate.py`, `test_image_boot.py`,
`test_vm_graphics.py`, `test_ring_lifecycle.py`, `test_catalyst_task_tree.py`,
`test_replication_topology.py`, `test_console_tasks.py`. `queue_thread_loop`,
`drs_thread_loop`, `process_queue_task` are only named by `test_service_wiring.py`
(static) in my search.

**Findings.** R-6 (balance bypasses `vali-drs`), `docs/vali.md` says Vali listens locally
(code: mTLS on all interfaces), `vali.py:856` `hostname_for_ip` has no caller, D-4
`generate_vm_xml`, D-5 service lists at `:1868,1914`, 15 shell-string execute sites (1.2),
`/var/lib/hci/aether/nvram` paths (`:1707`).

### 3.4 catalyst (`catalyst.py`)

**Purpose.** Task coordinator: records every task in `hydra.catalyst_tasks`, holds the
in-memory per-service queues, and runs the Dagur schedule clock.

**Entry points.** `main()` `catalyst.py:795`: `scheduler_thread_loop` (`:347`, every 10 s while
leading `catalyst-scheduler`), `dispatch_thread_loop` (`:749`, holds `catalyst-dispatch`, replays
`pending` rows every 15 s), mTLS `ThreadingHTTPServer` on `0.0.0.0:9091` (`:830`).

**Endpoints** (`CatalystAPIHandler`, `:419`).

| Method | Path | Does | Callers |
| :--- | :--- | :--- | :--- |
| GET | `/api/v1/queues/<service>` | long-poll one task (30 s); **503** if this node does not hold `catalyst-dispatch` (`:452-460`); 404 unknown queue | `vali.py:2009`, `dagur.py:279`, `spectrum_server.py:3094` |
| GET | `/api/v1/tasks/status/<task_id>` | DB first, then in-memory event (30 s) | `vali.py:467`, `valcli.py:2841-2882`, `spectrum_server.py:3019` |
| POST | `/api/v1/tasks/submit` | insert row, queue if holding dispatch (`:527-578`) | `vali.py:457,805,2598,2625`, `mipha.py:368`, `lanayru.py:398`, `spectrum_server.py:828,5193,5869,5892,7924,7969,8037`, Phoenix `catalyst.ex:161` |
| POST | `/api/v1/tasks/update` | status/progress/error update, wake waiters (`:580-620`) | `vali.py`, `dagur.py:199,213,253`, `hylia.py:1220`, `spectrum_server.py:3024-3052` |

Queues (`catalyst.py:285-290`): `vali`, `dagur`, `spark`, `lanayru`. Drainers: vali
(`vali.py:2009`), dagur (`dagur.py:279`), the spectrum backend (`spectrum_server.py:3094`).
Nobody submits to or drains `spark` (R-10).

**Config.** `/etc/hci/spark/certs/*` (exits if absent, `:817-824`), `/etc/hci/cluster.json`,
`LOCAL_HYPERVISOR_IP`. No environment variables.

**State.** Hydra: `catalyst_tasks`, `catalyst_task_sequence` (Daruk), reads `dagur_schedules`.
Daruk: `schedule/claim-job`, `catalyst/claim-sequence`. ZooKeeper: two candidacies.

**Failure behaviour.** ZooKeeper down: neither candidacy leads; every submission is still
recorded and sits `pending` until a dispatcher exists (`:557-575`), the worker polls get 503.
Hydra/Daruk down: `run_cql_query` falls to `cqlsh` through `podman exec`; if that fails the
submit still answers 200 with a task id (`:557-566`, return code unchecked). A restarted
dispatcher fails `processing` rows once with a reason and re-queues `pending` ones
(`:675-742`).

**Tests.** `test_catalyst_task_tree.py`, `test_conditional_writes.py`,
`test_service_leadership.py`, `test_console_tasks.py`, `test_zk_watches.py`.

**Findings.** R-4 (scheduler vs dispatch), R-7 (unvalidated `task_id` in CQL, `:479`), R-10
(dead `spark` queue), `catalyst.py:169` `dispatch_ip` has no caller, submit ignores the result
of the row insert (`:557-566`).

### 3.5 mipha (`mipha.py`)

**Purpose.** HA: monitors hosts, fences and fails VMs over, runs a per-host self-fence
watchdog, and runs the nightly storage auto-heal.

**Entry points.** `main()` `mipha.py:1991`; thread `self_fence_loop` (`:1531`, every node),
then the HA loop (10 s) gated by `mipha-ha` (`:1999`). CLI subcommands (`:2425-2436`):
`--auto-heal` (`run_auto_heal :1973`, the Dagur `storage_auto_heal` job,
`spectrum_server.py:1812`), `--fence-status`, `--clear-self-fence [--force]`. No HTTP.

**Calls out to.** spark `/api/v1/node/status`, `/host/units`, `/host/fence`, `execute`
(`pgrep -a qemu`, `nodetool status`), Vali `/api/v1/hosts` (health probe), Catalyst submit,
Daruk `node/maintenance`, `lock/renew`, `vm/release`, and (see R-1) `dfs/claim`.

**Config.** `/etc/hci/fencing.json` (`:433`), `/run/hci/mipha-self-fence.json` (`:438`),
`/etc/hci/maintenance.state` (`:528`), `LOCAL_HYPERVISOR_IP`, IPMI password injected into
the process environment around a BMC call (`:825-844`). Not `os.environ` reads otherwise.

**State.** Hydra: `nodes` (status DOWN/RECOVERING/NORMAL/FENCED/DEGRADED), `catalyst_tasks`
progress, `cluster_locks` renew, `dfs_vdisks` read (storage fence). ZK: `mipha-ha`.

**Failure behaviour.** ZooKeeper unreachable: not leading, monitor idle, self-fence loop still
runs. Hydra unreachable during storage fence: "nothing can be fenced" (`:897-899`), failover
refused. Daruk down: conditional status moves fail and are logged, failover continues
(`docs/mipha.md` section 2B).

**Tests.** `test_fencing.py`, `test_mipha_orphaned_quarantine.py`; static: `test_service_wiring.py`.

**Findings.** **R-1** (storage fence calls a Daruk path on spark), R-11 (rejoin writes
`hydra.nodes` unconditionally, `:2093,2136`, while the DOWN write is conditional,
`:2191,2238`), `mipha.py:31` `run_command_local` has no caller (also `hylia.py:31`),
`mipha.py:175-185` `is_zookeeper_leader` vs mcli-runner's unused copy, D-2 copies.

### 3.6 bifrost (`bifrost.py`)

**Purpose.** Binds the cluster VIP on exactly one healthy node.

**Entry point.** `main()` `bifrost.py:212`, single loop, 2 s. No HTTP, no CLI. SIGTERM/SIGINT
handler `:135`.

**Behaviour.** Reads `/etc/hci/cluster.json` (`vip`, `hosts`), finds the local interface by
matching a host address (`get_local_net_info :68`), stands for `bifrost-vip` only while
`127.0.0.1:443`, `:8443` and `:8444` all accept (`is_local_stack_healthy`), withdraws
otherwise; binds with `ip addr add` and 3 gratuitous ARPs (`:265-270`), releases with `ip addr
del` (`:274`). All via `subprocess.run(shell=True)` locally (6 sites).

**Config.** `/etc/hci/cluster.json`; no environment. **State.** none in Hydra; ZK
`bifrost-vip`.

**Failure behaviour.** ZooKeeper unreachable: `leading()` False, the VIP is released (safe
direction, `docs/service_leadership.md`). `cluster.json` missing or no `vip`: idle. No
interface match: idle.

**Tests.** `test_console_routing.py` (health-guard ports, static), `test_service_wiring.py`,
`test_phoenix_console_install.py`. I found no test that drives `main`, the candidacy or the
bind/release commands (**not checked** beyond grep for `is_local_stack_healthy|vip_candidacy`).

**Findings.** R-15 (SIGTERM releases the address but neither resigns nor closes the ballot,
`bifrost.py:135-150`: the ballot lingers up to the 15 s session timeout,
`helios_zk.py:1255`, so a clean stop of the VIP holder leaves the address unbound until it
expires), hard-coded fallback interface `ens192` (`:83`, same in `gatoway.py:49`,
`spectrum_server.py:2898,3864`, `vali.py:1204`, `urbosa.py:1571`).

### 3.7 dagur (`dagur.py`)

**Purpose.** Runs Catalyst's `dagur` tasks (scheduled jobs and one-off maintenance commands)
as shell commands on its own node through its spark-daemon, recording runs.

**Entry point.** `main()` `dagur.py:260`: loop gated by `dagur-queue` (`:262`), long-polls
`/api/v1/queues/dagur`, one thread per task (`execute_dagur_job_thread :187`). No HTTP, no CLI.

**Behaviour.** Writes `hydra.dagur_runs` (start row `RUNNING`, final row), updates Catalyst
progress (a fake 5..95 ticker unless `reports_progress`, `:205-228`), runs
`CATALYST_TASK_ID=<id> <command>` via `/api/v1/execute` on `127.0.0.1` with timeout from the
payload (default 3600 s, `:32,67`).

**Config.** `LOCAL_HYPERVISOR_IP`; certificates `/etc/hci/spark/certs`. **State.** Hydra
`dagur_runs` (no TTL, `helios_schema.py:214`); reads nothing else. ZK `dagur-queue`.

**Failure behaviour.** Not leading: sleeps. Catalyst 503/204/error: sleep 2 s. Spark down: the
run is recorded `FAILED` with the error. Daruk down: the run row is lost (return code of
`run_cql_query` ignored, `:185,196`).

**Tests.** No dedicated test. `test_console_tasks.py` calls `execute_dagur_job_thread`;
`test_conditional_writes.py`, `test_service_wiring.py`, `test_spark_shell_calls.py` cover its
CQL/wiring statically.

**Findings.** `job_name` is interpolated into CQL unescaped (`dagur.py:183,194`; the value
originates from Spectrum, which validates it with `_DAGUR_JOB_NAME_RE`,
`spectrum_server.py:950`, but a Catalyst submitter other than Spectrum is not constrained);
`spectrum_server.py:8600-8632` contains a second `insert_dagur_run` and
`execute_dagur_job_thread` with no caller (D-6); the `dagur_schedules.cron_expression` column
is written (`spectrum_server.py:1801-1840,5778`, Phoenix `settings/apply.ex:51`) and read only
for display (`spectrum_phx/lib/spectrum_phx/health.ex:57,452`), the scheduler honours
`interval_seconds` alone (`catalyst.py:378`).

### 3.8 lanayru (`lanayru.py` plus the worker loop in `spectrum_server.py`)

**Purpose.** Kubernetes engine: deploy and destroy a cluster (VMs, overlay segment,
cloud-init).

**Entry points.** `lanayru.py` has no `main`: it is a library of two workers
(`deploy_lanayru_worker :39`, `destroy_lanayru_worker :430`) imported by Spectrum
(`spectrum_server.py:2975-2982`). The queue worker is `lanayru_queue_loop`
(`spectrum_server.py:3057`), started under `supervise("lanayru_tasks", ...)` (`:8747`),
gated by `lanayru-queue`. There are two ways to start a deploy: Catalyst task (Phoenix
`lanayru.ex`) and the older Python endpoints `POST /api/lanayru/deploy`
(`spectrum_server.py:6214`) and `/destroy` (`:6263`), which still launch a bare thread
(`:6250-6258,6280`) and are called by `static/lanayru.html` only.

**Calls.** 22 shell-string `run_remote_spark` calls (1.2), Catalyst submit (`lanayru.py:398`),
`sidon_call create/attach/detach/delete` (`:209,219,507-508`), Daruk `vm/create`, `vm/set-state`.

**State.** Hydra `lanayru_clusters`, `lanayru_k8s_state`, `urbosa_segments`, `vms`,
`vm_nvram` (writes); reads `urbosa_t0_routers`, `urbosa_t1_routers`. ZK: `lanayru-queue`
(from Spectrum).

**Failure behaviour.** The worker marks a failed task `failed` with the exception text
(`spectrum_server.py:3040-3053`); a crash inside the worker threads is covered by the
supervisor only for the queue loop itself. Hydra or Urbosa absent: the deploy aborts with a
log line (`lanayru.py:84-110`).

**Tests.** `test_console_tasks.py` (queue worker, task ids), `test_conditional_writes.py`,
`test_vlan_claims.py`, `test_vdisk_creates_name_a_container.py`. No test of the remote command
sequence.

**Findings.** Two start paths (above); every command through `execute`; `lanayru.py:229`
hard-codes `/var/lib/hci/images/cirros.img`.

### 3.9 mimir (`mimir.py`), `mcli`, `mcli-runner`

**Purpose.** Health checks. `mimir` is the per-node daemon (certificate and Sidon-mount
surveys on every node, schedule trigger on the leader). `mcli` is the fan-out CLI. `mcli-runner`
is the per-node check engine (3587 lines; `CHECK_ID_TO_FUNC` in `mcli:21-100` maps 66 checks).

**Entry points.** `mimir.py:426` `main`, loop every 60 s: every node publishes
`security.mtls.certs` (`CERT_SURVEY_INTERVAL 900`) and `storage.sidon.mounts`
(`STORAGE_SURVEY_INTERVAL 300`) into `hydra.mimir_results`; the `mimir-schedules` leader
reads `hydra.mimir_schedules` and runs `mcli health_checks run_all|<category>` through spark
`execute` (`:478-483`). `mcli`: `status`, `health_checks run_all|services|hardware|storage|list`
(`mcli:608-640`). `mcli-runner --category <c>` is run on each node by `mcli`
(`mcli:240`), by `valcli health.check` (`valcli.py:2531-2537`), and as a Dagur job
(`spectrum_server.py:1801`, `mimir_diagnostics`). No HTTP.

**Config.** `CERT_WARN_DAYS 30`, `CERT_FAIL_DAYS 7`, `/etc/hci/spark/certs`, `/root/.certs`,
`/etc/hci/sidon-disks`, `/etc/hci/cluster.json`; no environment variables.

**State.** `hydra.mimir_results` PK `(category, check_name, node_ip)` (written by mimir and
mcli); `hydra.mimir_schedules` (seeded `hourly_checks` by Spectrum, `spectrum_server.py:1853`;
interval hard-coded by name, `mimir.py:470`). ZK: `mimir-schedules`.

**Failure behaviour.** ZK down: schedules not triggered, surveys continue. Hydra down:
survey writes lost silently (`run_cql_query` result unchecked, `mimir.py:403-424`).

**Tests.** `test_mimir_checks.py`, `test_mimir_results.py`, `test_candidacy_not_rebound.py`,
`test_sidon_mount_survey.py`, `test_create_health_findings.py`, `test_metadata_replication_rule.py`.

**Findings.** R-5 (blind schedule write, null `last_run_epoch`, unused Daruk claim), R-16
(health checks scheduled twice), `mcli-runner:14` `is_zookeeper_leader` has no caller,
`valcli.py:710` lists a `cron_expression` column for `mimir_schedules` that the schema
(`helios_schema.py:225`) does not have, `mcli:143` is a private `run_cql_query` that discards
rows (`"success"` only) and still shells out to `cqlsh` with `shell=True`.

### 3.10 rauru (`rauru.py`, `rauru_protection.py`, `rauru_replication.py`, `helios_snapshots.py`)

**Purpose.** Snapshot policy and (designed) protection and replication.

**Entry point.** `rauru.py:260` `main`: `--check` validates imports/config without a network
(`:231-257`), otherwise `Daemon.step` loop (`:183`) gated by `rauru-snapshots`, one pass of
`helios_snapshots.Runner(...).run()` per interval (`:158`), back-off while Hydra is not ready.
No HTTP. `valcli` runs the same code for `storage.snapshot-run` and the `storage.domain.*`
commands.

**Calls.** spark `/api/v1/dfs/vdisk` (`rauru.py:96-108`, `snapshot`, `delete`, `list`,
`rollback`), Daruk `catalyst/claim-sequence`, Hydra via `helios_cql`. **State.** Hydra
`dfs_snapshot_policies`, `dfs_snapshot_index`, `dfs_protection_domains`,
`dfs_protection_domain_members`, `dfs_protection_sets`. ZK `rauru-snapshots`. Config:
`LOCAL_HYPERVISOR_IP`; `/root/.certs/*`.

**Failure behaviour.** Starts before any cluster exists and never exits on a missing
dependency, it says what it waits for and backs off 5..300 s (`rauru.py:11-17,71-73,125`). A node
that cannot establish leadership does nothing.

**Tests.** `test_rauru.py`, `test_rauru_protection.py`, `test_rauru_replication.py`,
`test_snapshot_policy.py`.

**Findings.** **R-17**: the daemon never runs protection-domain sets. `rauru.py:158` calls only
`helios_snapshots.Runner`; `rauru_protection.Runner` is constructed only from
`rauru_protection.run_command` (`rauru_protection.py:1142`), reached from
`valcli.cmd_storage_domain` (`valcli.py:1298`). `helios_snapshots.py:492-524` only *skips*
vdisks claimed by an enabled domain. So scheduled domain snapshots never happen unless
someone runs `valcli storage.domain.run` (the "A timer that calls it: Not wired" row of
`docs/dfs/protection_domains.md:12` is still true), and the comment at `valcli.py:1287`
("which the Rauru daemon imports too") is wrong. D-7: `rauru_replication.py` is imported only
by `test_rauru_replication.py` (`grep -ln rauru_replication *.py`), is not in the deploy
lists, and `sidon/src/replicate` is behind `#[allow(dead_code)]`; `docs/dfs/replication.md`
says so.

### 3.11 hylia (`hylia.py`)

**Purpose.** LCM: validates and stages an update package, rolling-upgrades the nodes.

**Entry points.** `hylia.py:1483` `main`: no args runs the daemon (`hylia_loop :1461`, every
5 s, gated by `hylia-upgrades`, starts `hylia_rolling_upgrade` in a thread for `STARTING` or
`UPGRADING` jobs not in `running_jobs`); `--load-package <zip>` and `--start-upgrade <job-id>`
are what Catalyst/Dagur tasks run. No HTTP.

**Calls.** spark `execute` (38 shell sites incl. `cargo build`, `tar`, `base64 -d >`,
`reboot`), `/host/units`, `/host/maintenance` (`:844,1028`), `/dfs/vdisk` `capacity`/`list`
(`:666,687`), `/node/status` (`:986`), Catalyst. **State.** Hydra `hylia_jobs`, `hylia_logs`;
reads `nodes`. Files: `/var/lib/hylia/upgrade_rebooted_<job>`, `/tmp/helios_update.zip`.
Env: `CATALYST_TASK_ID` (`:1213`). ZK `hylia-upgrades`.

**Failure behaviour.** Resume after a leader change is by job state in Hydra
(`docs/service_leadership.md`). Hydra down: loop logs and retries. A node that fails its
storage guard blocks its own maintenance exit.

**Tests.** `test_hylia.py`, `test_update_signature.py`, `test_source_components.py`,
`test_console_tasks.py`.

**Findings.** `hylia.py:168` is an unreachable `return None` after `return`;
`hylia.py:31` `run_command_local` has no caller; `get_zookeeper_leader_ip` only feeds the
catalyst fallback; local restart uses `nohup sh -c 'sleep 2 && systemctl restart hylia'`
(`:1123`, an allow-listed exemption in `test_spark_shell_calls.py:79-105`); `docs/spark_api.md`
`lcm/package` flow depends on the leader being the node the Catalyst task runs on.

### 3.12 logos (`logos.py`)

See 1.1. **Purpose** per-node telemetry. **Entry** `logos.py:158` `main`, one loop, no
threads, no HTTP. **Config** `LOCAL_HYPERVISOR_IP`. **State** `logos_metrics`,
`urbosa_tunnel_metrics`. **Leadership** none, by design. **Failure** Hydra/Daruk down:
`run_cql_query` falls to `cqlsh`; on failure it prints and tries again in 5 s
(`logos.py:270-281`), no queueing, so the gap is lost. The `local_ip` argument passed to
`run_cql_query` (`:270`) is ignored by `helios_cql` (`*args`), the query always goes to the
local Daruk. **Tests** none dedicated; `test_cql_layer.py`, `test_service_wiring.py`,
`test_unbound_names.py`. `test_spectrum_data_layer.py:200-267` covers the *read* side.
**Findings** unused imports `socket`, `base64`, `subprocess` (`logos.py:5,7,8`, one reference
each); `spectrum_server.py:1897-1898` still runs `ALTER TABLE ... ADD mem_total_kb / cpu_cores`
on every start although `helios_schema.py:223` already defines both columns (errors are
swallowed); stale `docs/logos.md`.

### 3.13 gatoway (`gatoway.py`)

**Purpose.** Per-node L2/VLAN bridge builder for `hydra.gatoway_networks`.

**Entry** `gatoway.py:main` (loop 5 s). No HTTP. Reads `cluster_settings.gato_enabled`
(`:142-152`) and `gatoway_networks`; creates `br-vlan-<id>` bridges and `<uplink>.<id>`
sub-interfaces with `ip link` through local `shell=True`; deletes bridges for removed
networks only when no guest port remains (`gatoway.py:225-246`). **Config** none; uplink from
`ip route show` (default `ens192`, `:49`). **State** read only.
**Leadership** none (each node builds its own). **Failure** an unreadable or unparseable
network table skips reconcile and cleanup rather than treating it as empty (`gatoway.py:65-100`):
good. **Tests** none dedicated (`grep -lE "get_db_networks|is_gato_enabled" test_*.py` is
empty); `test_cql_layer.py`, `test_service_wiring.py`. **Findings** `gato_enabled` is parsed by
substring `"true"` in the output line, as is `urbosa_enabled` in three other places (D-9);
`gatoway.py:22` is another local `run_cmd` shell helper; VLAN claims are a Daruk concern
(`/v1/network/claim-vlan`, `spectrum_server.py`), not Gatoway's.

### 3.14 ingress and console proxies (`slate_config/`, `agahnim/`)

**Slate** is the ingress on :443 (`slate_config/traefik.yml`, `dynamic.yml`). Routing:

| Router (priority) | Match | Backend |
| :--- | :--- | :--- |
| `console-ws` (300) | `Path(/api/vms/console/ws)` | agahnim `http://127.0.0.1:8081` |
| `phoenix-ui` (200) | exact pages `/ /login /logout /hosts /vms /tasks /metrics /health /storage /policies /images /hardware /sdn /networking /settings /lcm /lanayru /favicon.ico /robots.txt` or prefix `/vms/ /live/ /assets/ /fonts/ /images/` | spectrum-phx `http://127.0.0.1:8444` |
| `webui` (100) | `PathPrefix(/)` | spectrum `https://127.0.0.1:8443`, `insecureSkipVerify` |

**Agahnim** (`agahnim/src/main.rs`, 328 lines, no tests): `main :49-50` binds `0.0.0.0:<arg 1 or
8081>`; per connection it peeks the first byte, terminates TLS with
`/etc/hci/spectrum/certs/server.{crt,key}` if present (else plain), upgrades to WebSocket,
takes `?token=` from the query, asks Spectrum's verifier on `127.0.0.1:8089` (`OK|host|port`),
then pipes bytes to the guest's VNC/SPICE port. A non-WebSocket request gets a static "Console
Authorized" page (certificate-trust helper for SPICE). Called by Slate only
(`provision.py:1669` `ExecStart=/usr/local/bin/agahnim 8081`).

**Failure.** Verifier down: connection refused, no console. Certificates missing: plain.
**Tests** `test_console_routing.py` (Slate rules, static YAML), `test_service_wiring.py`.
Agahnim has zero Rust tests (`grep -rc "#\[test\]" agahnim/src` is 0).

**Findings.** R-18 (Phoenix `/storage/vdisks/:id/snapshots` is not routed to Phoenix),
R-19 (Agahnim logs the console token, `main.rs:247,326`, and listens on all interfaces plain),
`spectrum_phx/config/runtime.exs:22-28` default port comment contradicts the live 8444.

### 3.15 spectrum (`spectrum_server.py`, :8443)

**Purpose.** The Python console backend: REST API, sessions and users, image and VM
operations, SDN, LCM, console-token verifier, and the `lanayru-queue` worker.

**Entry points.** `main()` `spectrum_server.py:8737`: supervised loops (`supervise :8693`)
`db_reconcile` (`:8409`), `metrics_and_cluster_monitor` (`:2381`), `internal_token_verifier`
(`:8640`, 127.0.0.1:8089), `lanayru_tasks` (`:3057`); `init_ssl`, `init_db` (`:1772`, creates
keyspace, applies `helios_schema`, seeds default rows), `ThreadingHTTPServer` on `:8443`
(`:8765`). Handler `SpectrumHandler :3108`, `do_GET :3122`, `do_POST :4947`; no PUT or DELETE.
Auth guard on every `/api/` path except `/api/login` and `/api/auth/check`; CSRF
Origin/Referer check on POST (`:4947-4967`).

**Endpoints** (callers: the legacy pages in `static/`, `app.js` unless noted; **Phoenix never
calls these**, `grep '"/api/[a-z]' spectrum_phx/lib` excluding `/api/v1` is empty; `valcli`
calls the ones marked V).

GET: `/api/auth/check`, `/api/lcm/upgrade/check`, `/api/lcm/inventory`,
`/api/lcm/upgrade/status`, `/api/settings`, `/api/users`, `/api/vms` (V), `/api/vms/drs`,
`/api/networks`, `/api/lanayru/checks|status|cluster/info` (`lanayru.html`),
`/api/host/interfaces`, `/api/urbosa/t0|t1|segments|firewall|tunnels/metrics|tunnels/status`,
`/api/status` (+`vnc_auto.html`), `/api/metrics/history`, `/api/cluster/metrics`
(`metrics.html`), `/api/cluster/nodes/hardware` (`hardware.html`), `/api/mimir/results`,
`/api/mimir/schedules`, `/api/dagur/schedules`, `/api/dagur/runs`, `/api/catalyst/tasks`,
`/api/storage/containers` (V), `/api/images`, `/api/storage/disks`,
`/api/vms/console/ping|token|ws`, and static files for any other path (`:4896`).

POST: `/api/login`, `/api/auth/logout`, `/api/auth/change-password`,
`/api/lcm/upload|upgrade/check|upgrade/download|upgrade/start|upgrade/abort`,
`/api/catalyst/tasks/cleanup`, `/api/cluster/nodes/add|remove`, `/api/settings/update`,
`/api/settings/ssl/update`, `/api/users/create|delete|change-password`,
`/api/images/upload*`, `/api/images/delete`, `/api/vms/console/metrics`,
`/api/lanayru/deploy|destroy`, `/api/vms/create|update|delete` (V),
`/api/vms/cdrom|power|balance`, `/api/vms/migrate` (**no caller**),
`/api/storage/containers/create|update|delete` (V), `/api/networks/create|update|delete`,
`/api/urbosa/{t0,t1,segments,firewall}/{create,update,delete}`,
`/api/mimir/schedule/update`, `/api/mimir/run`,
`/api/maintenance/rebalance|cleanup|dbcleanup` (dynamic URL, `static/app.js:572`),
`/api/dagur/schedule/update|trigger` (V), `/api/host/maintenance`, `/api/host/reboot`.
(Route table extracted by script from `spectrum_server.py:3122-8160`; the caller column was
computed by substring search over `static/`, `valcli.py`, `mcli*`, `check_updates.py`,
`saga.py`.)

**Config.** `LOCAL_HYPERVISOR_IP` from env (`:781`) and `/etc/hci/spectrum/spectrum.env`
(`:171`), `/etc/hci/cluster.json` (11 reads), `/root/.certs/*`, `/etc/hci/spectrum/certs`,
`/etc/hci/aether/storage-pools.json`, `UPDATE_HOST_ALLOWLIST` (`:162`).

**State.** Hydra 21 tables (2.4); Daruk `vm/*`, `network/*-vlan`. ZK `lanayru-queue`.

**Failure behaviour.** Hydra down: `init_db` retries 15 times, then serves with the
schema flag unset; handlers return 500s; the supervised loops restart with 5 s to 300 s
back-off (`:8693-8735`). Catalyst down: submissions return 500 with the error. Spark down:
per-node calls return `-1` and the pages degrade.

**Tests.** `test_spectrum_data_layer.py`, `test_status_payload.py`, `test_auth_stale_token.py`,
`test_console_token_errors.py`, `test_console_tasks.py`, `test_thread_supervision.py`,
`test_storage_containers.py`, `test_vlan_claims.py`, `test_vm_graphics.py`,
`test_internal_api_auth.py`. The route table as a whole is not tested; **not checked**
which of the ~100 routes have direct tests (test files mention about 20 of the paths as strings; whether they exercise the handlers is unverified).

**Findings.** D-4 (dead `generate_vm_xml`, `get_cpu_pct`, `get_cpu_info`, `get_mem_stats`,
`parse_free_m_all`), D-6, double definition of `get_zookeeper_leader_ip` (`:1496` and `:8495`,
the later wins), 37 shell-string execute sites, `aether` path leftovers
(`:780,1098,1468,1784,2140,4575,6508`) and stale check names `aether_volume`, `aether_status`, `aether_peers` (`:4165`), `/api/maintenance/rebalance` runs a command that only echoes
(`:7948-7950`), `/api/vms/migrate` has no caller, `docs/spectrum.md` route statements **not
checked**.

### 3.16 spectrum_phx (Elixir, :8444)

**Purpose.** The rebuilt LiveView console. Reads Hydra directly with bound parameters
(Xandra, `hydra.ex`), reads ZooKeeper (`zk/client.ex`, `zk/state.ex`), calls spark for host
actions and Catalyst for work.

**Entry points.** `SpectrumPhx.Application` (`application.ex:9-38`): Telemetry, DNSCluster,
`Cluster.Config` (cached `cluster.json`), `Zk.Client`, `Hydra` (lazy, `sync_connect: false`),
PubSub, `Endpoint`. Router (`spectrum_phx_web/router.ex:21-76`): `GET/POST /login`,
`DELETE/GET /logout`, LiveViews `/`, `/hosts`, `/vms`, `/vms/new`, `/vms/:name`, `/tasks`,
`/metrics`, `/health`, `/storage`, `/policies`, `/storage/vdisks/:vdisk_id/snapshots`,
`/images`, `/hardware`, `/sdn`, `/networking`, `/lcm`, `/lanayru`, `/settings`, plus
`/dev/dashboard` in dev. No JSON API. Called by browsers through Slate only.

**Config** (`config/runtime.exs`): `PHX_SERVER`, `PORT` (8444 in the quadlet), `PHX_BIND_IP`
(127.0.0.1 in the quadlet), `SECRET_KEY_BASE`, `PHX_HOST`, `PHX_CHECK_ORIGIN`,
`PHX_EXTRA_ORIGINS`, `DNS_CLUSTER_QUERY`, `SPECTRUM_TLS_PORT`, `SPECTRUM_TLS_CERT`,
`SPECTRUM_TLS_KEY`, plus `/etc/hci/spectrum/spectrum-phx.env`.

**State.** Reads most Hydra tables; writes via its own modules (accounts, settings, images,
snapshots/policies). ZK: reads `/helios/nodes`, `/cluster_state`. Leadership none.

**Failure behaviour.** Boots with ScyllaDB or ZooKeeper down and reports them down; spark
probes run with `retry: false` where a page load must not block
(`spark.ex:37-55`).

**Tests.** ExUnit: 25 files in `spectrum_phx/test/spectrum_phx/`, a navigation test, auth
test, LiveView tests per area (`spectrum_phx/test/spectrum_phx_web/live/*`).

**Findings.** **R-20**: `SpectrumPhx.Catalyst.leader_ip/0` (`catalyst.ex:194-209`) resolves at
run time to `Zk.State.leader_ip/0` (`zk/state.ex:99-114`), which returns the node whose status
document says `zk_leader == true`: the **ZooKeeper ensemble leader**, not the
`catalyst-dispatch` holder. `catalyst.ex:183-190` and `docs/service_leadership.md` both say it
submits to the local or configured node. Submissions to a non-dispatch Catalyst are recorded
and replayed by the 15 s sweep, so work still runs but with up to that delay. Also D-10: 17
`SpectrumPhx.Spark` functions have no caller in `lib/` or `test/`: `vm_interfaces :97`,
`vm_console :100`, `vm_info :103`, `vm_define :111`, `vm_undefine :116`, `vm_power :121`,
`dfs_status :160`, `dfs_resize :267`, `dfs_flush :272`, `device_info :277`,
`device_prepare :280`, `device_flush :289`, `container_mounted :292`, `container_ensure :300`,
`dhcp_leases :339`, `host_reboot :342`, `execute_all :61`. Callers of `Spark.execute`:
`settings/apply.ex:318`, `images.ex:245`, `lcm/package_upload_writer.ex:250`, and
`Catalyst.run_on_leader` (dagur tasks).

### 3.17 valcli (`valcli.py`, 3691 lines)

**Purpose.** Operator CLI. **Entry** `main :3464`, 60 subcommands dispatched by `if/elif`
(`:3475-3688`), `print_usage :3384`. Version string `v1.2.0`. No HTTP server.

**Commands and what they call.** `vm.*`, `drs.status`, `host.*` use spark typed endpoints
(`/api/v1/vm/*`, `/api/v1/hosts`, `/api/v1/host/maintenance`) and Catalyst
`/api/v1/tasks/status` for waiting (`:2839-2882`). `vm.create|delete|edit` call Spectrum
`https://127.0.0.1:8443` (`run_spectrum_api :2660`, pinned to
`/etc/hci/spectrum/certs/server.crt`). `storage.*` call `/api/v1/dfs/vdisk` (24 ops) and Hydra
through `helios_cql`. `scheduler.*` read `dagur_schedules`/`dagur_runs`; `scheduler.trigger`
goes through Spectrum (`:2750`). `health.check` fans out `mcli-runner`. `backup.*` run `saga`
(`:3336-3380`). `system.cleanup` prunes `dagur_runs`, `mimir_results`, `vali_tasks`.
`cluster.vip.set` rewrites `cluster.json` everywhere and restarts bifrost (`:3024-3057`).
`storage.domain*` runs `rauru_protection` in-process.

**Callers.** Operators; Dagur seeds `valcli system.cleanup` and `valcli storage.cleanup_orphaned`
(`spectrum_server.py:1822,1834`); `CATALYST_TASK_ID` is read for task parenting (`:1083,1297`).
**Config.** `CATALYST_TASK_ID`; `/etc/hci/cluster.json`; `/root/.certs/*`; `SAGA_TARGET` is
saga's. **State.** none of its own. **Leadership** none. **Failure** each command reports the
spark/Hydra error and exits non-zero; `--all` for host maintenance iterates hosts one by one
and is expected to be refused after the first on a multi-node cluster by the single
maintenance lock and the quorum gate (`vali.py:2233-2300`); not run.

**Tests.** `test_storage_benchmark.py`, `test_storage_list_not_ready.py`,
`test_storage_sweep.py`, `test_compaction.py`, `test_egroup_access_data.py`,
`test_vdisk_replication_factor.py`, `test_snapshot_policy.py`, `test_rauru_protection.py`,
`test_multi_disk.py`, `test_storage_containers.py`. No test for the host/VM/scheduler/health
commands.

**Findings.** R-21 (`valcli` is outside `test_spark_shell_calls.CALLERS`, and
`cmd_cluster_vip_set` builds `systemctl restart bifrost` as a shell string); every `def` in
`valcli.py` is referenced at least once; `valcli.py:710` lists a nonexistent column;
`valcli.py:1351` `_copies_for_ftt` duplicates `copies_for_ftt` in
`sidon/src/control.rs:131`; `valcli.py:2777` is a third byte-identical copy of
`get_zookeeper_leader_ip` (D-2).

### 3.18 daruk (`daruk.py`)

**Purpose.** The cluster's CQL gateway: a local HTTP proxy to Hydra with 28 typed
compare-and-swap operations. Runs *inside the Hydra container*
(`provision.py:1377,1545`: `podman exec systemd-hydra-db python3 /var/lib/scylla/daruk.py`).

**Entry.** `daruk.py:run :941`; connects at import (`connect_db :26`, 30 tries x 2 s, then
`RuntimeError`, systemd restarts it); `HTTPServer` (single threaded) on `127.0.0.1:9043`
(`:942`).

**Endpoints.**

| Method | Path | Does | Callers |
| :--- | :--- | :--- | :--- |
| POST | `/query` | run any CQL; reads degrade to consistency ONE, writes never (`:866-934`); rows returned as dicts, `SELECT JSON` as one `json` field | every Python daemon through `helios_cql.py:140-175`, `mcli:143` |
| POST | `/v1/vm/claim, release, set-state, migrate-lock, migrate-unlock, migrate-commit, create` | typed VM ownership CAS | `vali.py`, `mipha.py`, `spectrum_server.py`, `lanayru.py`, `mcli-runner` |
| POST | `/v1/schedule/claim-job` | Dagur tick claim | `catalyst.py` |
| POST | `/v1/schedule/claim-check` | Mimir tick claim | **none** (test only: `test_conditional_writes.py`) |
| POST | `/v1/catalyst/claim-sequence` | task sequence ids | `helios_schema.claim_task_sequence`, used by `catalyst.py:267`, `helios_snapshots.py:382` |
| POST | `/v1/node/maintenance` | conditional node status | `vali.py`, `mipha.py` |
| POST | `/v1/lock/acquire, renew, release` | cluster lock with TTL | `vali.py`, `mipha.py` |
| POST | `/v1/network/claim-vlan, release-vlan, reclaim-vlan` | VLAN claims | `spectrum_server.py` |
| POST | `/v1/dfs/vdisk-create, claim, drain-commit, vdisk-resize, set-replicas, egroup-create, vdisk-class, vdisk-seal, egroup-state, block-map-repoint, extent-repoint` | vdisk map | `sidon` only (`mipha.py:923` calls `dfs/claim` on the wrong port, R-1) |

**Config.** Local address by UDP-connect trick (`:8-19`), Hydra seed = that address. No
environment. **State** none. **Leadership** none, one per node.

**Failure.** Hydra down at start: up to 60 s of retry, then exit. During operation: 400 with
the driver error; CAS failures never retried at weaker consistency. Because the server is
single-threaded, one slow query holds every other client, including Sidon's 15 s
`SIDON_DARUK_TIMEOUT`.

**Tests.** `test_daruk_lwt.py`, `test_daruk_column_names.py`, `test_conditional_writes.py`,
`test_vlan_claims.py`, `test_ring_lifecycle.py`.

**Findings.** R-5, R-22 (single-threaded, unauthenticated plain HTTP on loopback),
`/v1/schedule/claim-check` unused.

---

## 4. Findings

### 4.1 Low-risk fixes (safe to do with a test)

> **Status, second pass (2026-10-04).** Done, each with a test that fails on the old code: 1, 2, 3, 4, 5, 6, 7, 8, 9 (the
> scanner now covers `valcli`, `mimir`, `catalyst`, `lanayru`, `urbosa_bootstrap` and `rauru`; the unit-control call sites
> moved to the typed endpoint; the remaining network commands are listed with reasons in `test_spark_shell_calls.py`),
> 10 (except `catalyst.dispatch_ip`, kept because it is the published way to find the queue holder, and `hylia`'s
> dead `return None`), 12, 13, 14. Item 11 (documentation) is partly done: `docs/rauru.md`, `docs/dfs/protection_domains.md`,
> `docs/maintenance.md`, `docs/host_states.md`; `docs/logos.md`, `docs/vali.md`, `docs/spark_api.md` and `docs/service_leadership.md`
> still need a read against the code.

1. **Validate `task_id` in `GET /api/v1/tasks/status/<id>`** (`catalyst.py:446-479`). It is
   spliced into CQL as `task_id = {task_id}`. Reject anything that is not a UUID with 400.
   Test: a path with `; DROP` or a quote returns 400 and issues no query.
2. **Remove the dead `parts` of `handle_cluster_status`** (`spark_daemon_decoded.py:3044-3055`,
   the `volume name:` filter). Test: `GET /api/v1/cluster/status` output is unchanged for a
   sidon-only cluster.
3. **Fix `\\n` in the NVRAM watcher** (`spark_daemon_decoded.py:5106,5108`) so errors end in a
   newline. Test: stub `sys.stderr`.
4. **Escape or validate `job_name` in `dagur.py:183,194`** with `cql_escape`
   (`helios_cql.py:65`) or the Spectrum regex. Test: a name with `'` does not break the
   statement.
5. **Make Mimir's schedule claim a compare-and-swap** (`mimir.py:469-478`) using the existing
   `/v1/schedule/claim-check` (`daruk.py:335`), coerce a null `last_run_epoch` to 0 as
   `catalyst.py:376-381` does. Test: a null row does not raise; a lost claim skips the run.
6. **Guard `submit_task_to_memory` in the scheduler loop** with `holds_dispatch()`
   (`catalyst.py:410`), as the submit handler does (`:569`); the dispatcher sweep already
   replays the recorded row. Test: scheduler leading, dispatch not leading, queue stays empty.
7. **Delete the duplicate `spectrum_server.get_zookeeper_leader_ip` at `:1496`** (the later
   definition at `:8495` is what runs). Test: `test_zk_probe_storm.py` still passes.
8. **Delete the duplicate `run_mtls_spark_api` at `spark_daemon_decoded.py:377`** (the one at
   `:470` shadows it) or reconcile the 120 s and 30 s timeouts deliberately.
9. **Add `valcli.py`, `mimir.py`, `catalyst.py`, `lanayru.py`, `urbosa_bootstrap.py`,
   `rauru.py` to `CALLERS`** in `test_spark_shell_calls.py:48`, and move the 5 call sites in
   1.2 (plus the `KNOWN_SHELL_COMMANDS` entries they need) before turning it on. Add a family
   for `nc -U /run/sidon`.
10. **Remove `mcli-runner:14 is_zookeeper_leader`, `mipha.py:31` and `hylia.py:31`
    `run_command_local`, `hylia.py:168` dead `return None`, `catalyst.py:169 dispatch_ip`,
    `vali.py:856 hostname_for_ip`, `logos.py` unused imports.** Each has no caller (4.3).
11. **Fix `docs/logos.md`, `docs/vali.md` (listens on all interfaces, mTLS), `docs/spark_api.md`
    (`snapshot`, `clone`, `rollback`, 30 typed paths), `docs/service_leadership.md` (Phoenix
    leader resolution, R-20), `valcli.py:1287` comment.**
12. **Add `/storage/vdisks/` to the `phoenix-ui` prefix list** in
    `slate_config/dynamic.yml` and a case to `test_console_routing.py` (R-18).
13. **Warn in Logos when `LOCAL_HYPERVISOR_IP` is absent** (`logos.py:22`), as Vali does
    (`vali.py:1985`), or refuse to write under `127.0.0.1`.
14. **Resign the ballot on SIGTERM in bifrost** (`bifrost.py:135-150`: call the candidacy's
    `withdraw()`), so the address does not wait out the 15 s session.

### 4.2 Risks and inconsistencies (need a decision)

> **Status, second pass (2026-10-04).** *Fixed:* R-1 (earlier), R-2 (boot autostart and the watchdog now run the declared-table
> reconcile pass, serialised with the ZooKeeper loop; the Mimir watchdog check follows), R-3 (the cluster CLI verifies the
> daemon and no longer carries one machine's path; `HCI_CERT_DIR` relocates it), R-4 and 4.1(6) (a scheduler that is not the
> dispatcher no longer queues), R-5, R-7, R-10 (the `spark` queue is gone and a submission naming a service with no queue is
> refused unrecorded), R-13, R-15, R-17 (Rauru runs the protection-domain pass; **never run live**), R-18 (and every Phoenix
> route is now checked against the ingress rule), R-19 (the console token is redacted in Agahnim's log; the loopback verifier
> on 8089 is still unauthenticated), R-20 (Phoenix resolves the `catalyst-dispatch` election holder, not the ZooKeeper leader),
> R-22 (Daruk serves one thread per request). *Left open, with reasons:* R-6 (low), R-8 and R-14 (scrub cadence is a decision),
> R-9 (the unused cluster create/destroy endpoints are 300+ lines with their own service lists; removing them is a deletion
> that deserves its own review), R-11, R-12 (`#[allow(dead_code)]` stays while the replication pieces are not called),
> R-16 (the duplicate hourly health check is harmless, and the console's Health page reads the Dagur run record), R-21,
> R-23, R-24, R-25.

* **R-1 (high) Mipha's storage-epoch fence appears to call a path Spark does not serve.**
  **FIXED 2026-10-04** (the claim now goes through `run_lwt` to Daruk; `test_fencing.py` pins it). Original finding: `mipha.py:923` posts `/v1/dfs/claim` with `run_mtls_spark_api_full("127.0.0.1", ...)`, i.e.
  `https://<local>:9099/v1/dfs/claim` (`mipha.py:127-168`). `/v1/dfs/claim` is a Daruk path
  (`daruk.py:539`, on `127.0.0.1:9043`); `grep -n '"/v1/' spark_daemon_decoded.py` is empty and
  spark's routers have no `/v1/` route, so it answers 404 with no body and mipha reads that as
  "claim refused" (`:931-939`). `docs/mipha.md` and the code comment (`mipha.py:873-895`) call
  this rung "the only one that has to succeed for a failover to be safe". Tests patch
  `run_mtls_spark_api_full` with a fake claim (`test_fencing.py:408`) so they cannot see it.
  **Not run against a live cluster.** Decision: send it to Daruk with `run_lwt`
  (`mipha.py:1705`), which the rest of the file already uses, and add a test that asserts the
  path is a route of the target service.
* **R-2 (medium) Two actors drive services after boot.** `docs/cluster_state.md` section 3 says
  one actor per service. `check_cluster_and_autostart` still starts services itself
  (autostart loop `spark_daemon_decoded.py:4973-4983`) and then runs a 30 s watchdog (`:5001-5055`) with its
  own 13-service list (`:5043`), no dependency order, a `zkCli.sh` text parse through
  `podman exec` (`:5030`), and `systemctl start` for anything not `active` or `activating`.
  `deactivating` is not in that set, which is the exact window `docs/cluster_state.md`
  ("A unit that is mid-transition is not drift") explains makes a start cancel a pending
  stop. The list omits `spectrum-phx`, `slate`, `agahnim`, `hylia` that `MANAGED_SERVICES`
  declares.
* **R-3 (medium) The cluster CLI does not verify the server certificate.**
  `cluster_new.py:664,710` use `ssl._create_unverified_context()` for `run_remote_spark` and
  `run_mtls_spark_api_full`, with a developer path baked in
  (`C:/Users/AuraFlight/.hci_temp_certs/...`, `:655,701`). `mipha.py:73` explicitly refuses this
  approach. Every other client verifies through `spark_endpoint` (`cluster_new.py:2258`).
* **R-4 (medium) Catalyst scheduler and dispatcher can be different nodes.**
  `catalyst.py:410` queues the scheduled Dagur task in the scheduler node's in-memory queue
  without checking `holds_dispatch()`, unlike `:569`. If `catalyst-scheduler` and
  `catalyst-dispatch` are held by different nodes, the row is replayed by the dispatcher's sweep
  (so it runs), but the scheduler node keeps an orphan entry and an orphan `task_events` entry
  that nothing drains (`:303-309`); if that node later becomes dispatcher, the same job can run
  again from the stale queue entry (`queued_task_ids` is cleared on acquisition, `:776`, the
  queues are not). Plausible from the code, **not reproduced**.
* **R-5 (medium) Mimir's schedule is not a compare-and-swap.** `mimir.py:478` updates
  `last_run_epoch` blindly; `daruk.py:335` `/v1/schedule/claim-check` exists for exactly this
  and has no caller. `mimir.py:469` `s.get("last_run_epoch", 0)` returns `None` for a null
  column, raising in `now - last_run`, which the loop's `except` swallows for the whole
  schedule pass. The interval is hard-coded by name (`:470`), unlike Dagur's
  `interval_seconds`.
* **R-6 (low) `POST /api/v1/vms/balance` ignores `vali-drs`.** `vali.py:2486-2489` runs
  `run_drs_loop` on whichever Vali received the call (spark forwards to its own node,
  `spark_daemon_decoded.py:2775`), concurrently with the leader's 30 s pass. DRS migrations go
  through Catalyst, so duplicates are caught by VM migrate locks, but the pass itself is not
  single-instance.
* **R-7 (low, mTLS-gated) CQL injection via URL** (`catalyst.py:479`), see 4.1 item 1.
* **R-8 (low) Raw control-socket access outside the typed API.** A seeded Dagur job writes
  JSON to `/run/sidon/control.sock` with `nc -U` (`spectrum_server.py:1807,1916`), and the
  row is re-written on every Spectrum start. It bypasses `DFS_NODE_OPS`
  (`spark_daemon_decoded.py:4358`). `purah-scrub` is therefore triggered by four independent
  things: the Sidon timer (`control.rs:1724-1765`, each `SIDON_PURAH_INTERVAL`, default 300 s),
  Dagur every 6 h, Mipha nightly (`mipha.py:1905`), and operators. Whether the 5-minute
  scrub is incremental was **not checked**.
* **R-9 (low) Five spark endpoints and the `device/*` family have no caller**:
  `/api/v1/cluster/start`, `/stop`, `/create`, `/destroy`, `/sync-settings`
  (`spark_daemon_decoded.py:2921-2946`), `/api/v1/vm/define` and `/storage/device*`
  (only unused Phoenix wrappers). The create/destroy handlers are 300+ lines of a second
  implementation of the CLI flows (`handle_cluster_create :3294`, `handle_cluster_destroy
  :3609`) and contain their own service lists.
* **R-10 (low) Catalyst has a `spark` queue** (`catalyst.py:288`) that nothing submits to and
  nothing drains. A submission would be re-queued by the sweep forever instead of failing
  (`catalyst.py:714-720` fails only unknown queue names).
* **R-11 (low) Unconditional `hydra.nodes` writes in mipha's rejoin path**
  (`mipha.py:2093,2136`) while the DOWN transition is conditional (`:2191,2238`);
  `docs/mipha.md` section 2B describes only the conditional write.
* **R-12 (low) `sidon/src/main.rs:29` `#[allow(dead_code)] mod replicate`** silences the
  compiler for `replicate::{export,import,sim,verify_group,write_frame,read_frame,safe_name}`:
  no caller outside `src/replicate` (`grep -rn "replicate::export\|replicate::import" sidon/src`
  finds only `src/replicate`); the `throttle`, `sha256`, `hex`, `seal_of` items are used by
  Purah. Matches `docs/dfs/replication.md`; the attribute hides regressions in the used parts.
* **R-13 (low) Stale text**: `sidon/src/main.rs:11-13` says the peer port "is not opened
  here" but `control.rs:282-286` opens it; `main.rs:52` comment about LINSTOR.
* **R-14 (decision) Scrub cadence**: see R-8.
* **R-15 (low) Bifrost SIGTERM does not resign**: `bifrost.py:135-150`, 15 s lingering ballot,
  `helios_zk.py:1255`.
* **R-16 (low) Health checks are scheduled twice**: Dagur's `mimir_diagnostics`
  (`spectrum_server.py:1801`, hourly, `mcli health_checks run_all`) and Mimir's
  `hourly_checks` (`spectrum_server.py:1853`, `mimir.py:482`, same command, hourly).
* **R-17 (medium) Protection-domain snapshots are never scheduled** (3.10): `rauru.py:158`.
* **R-18 (medium) A Phoenix page is unreachable through Slate.** `router.ex:61` and the link at
  `storage/index_live.ex:656` use `/storage/vdisks/:id/snapshots`; `dynamic.yml` routes only
  the exact path `/storage` and the prefixes `/vms/ /live/ /assets/ /fonts/ /images/`, so the
  request falls to the `webui` catch-all (Python). `test_console_routing.py` checks the
  navigation table only. Dispatching through Slate was **not run**.
* **R-19 (low) Agahnim logs the console token** (`agahnim/src/main.rs:247,326`, "Verifying token '{}'"
  and "Tearing down connection for token '{}'") and the verifier on 8089 is unauthenticated
  loopback TCP (`spectrum_server.py:8645`).
* **R-20 (medium) Phoenix submits to the ZooKeeper leader**, not the queue holder (3.16).
* **R-21 (low) Shell-string coverage gap**: 1.2.
* **R-22 (medium) Daruk is a single-threaded plain-HTTP server** (`daruk.py:942`): a slow
  query serialises every daemon; Sidon's metadata calls time out at 15 s.
* **R-23 (low) `aether` path names persist** for live directories (`/var/lib/hci/aether/
  volumes`, `/nvram`, `/etc/hci/aether/storage-pools.json`) in `spectrum_server.py:780,1098,1468,1784`,
  `vali.py:1707`, `lanayru.py:344`, `spark_daemon_decoded.py:1505,5090`. `ALLOWED_PATH_ROOTS`
  still lists `/var/lib/hci/aether/` (`:1505`). Whether those directories still exist on a
  Sidon node was **not checked**.
* **R-24 (low) Naming drift in the maintenance flow**: spark `/api/v1/host/maintenance`
  forwards to Vali `/api/v1/hosts/maintenance`, `/api/v1/vm/power` to `/api/v1/vms/power`
  (`spark_daemon_decoded.py:2933-2943`), and the Vali-down fallback delegates to a remote
  spark with the singular path (`:2846`). Works, but there are two spellings per resource.
* **R-25 (low) `valcli host.maintenance.enter --all`** is advertised (`valcli.py:3397`) but a
  multi-node cluster refuses all but the first by design (cluster maintenance lock and quorum
  gate). Not run.

### 4.3 Dead code candidates (with the grep that shows no caller)

Python (function defined, no other reference in non-test `*.py`, `mcli*`, `spectrum_phx/lib`,
`static/`; run from the repo root):

| Item | Evidence |
| :--- | :--- |
| `spectrum_server.py:2122 generate_vm_xml` (161 lines) | `grep -n "generate_vm_xml" spectrum_server.py test_*.py`: def and `test_vm_graphics.py:72` only; `vali.py:971` has the live copy |
| `spectrum_server.py:2006 get_cpu_pct`, `:2030 get_cpu_info`, `:2037 get_mem_stats`, `:2060 parse_free_m_all` | `grep -rn "get_cpu_pct\|get_cpu_info\|get_mem_stats\|parse_free_m_all" --include=*.py --include=*.ex --include=*.js .` returns only the defs |
| `spectrum_server.py:8600-8632 insert_dagur_run`, `execute_dagur_job_thread` | no caller in `spectrum_server.py`; `dagur.py` has its own (`test_console_tasks.py` exercises dagur's) |
| `spectrum_server.py:1496 get_zookeeper_leader_ip` (first def) | shadowed by `:8495` |
| `spark_daemon_decoded.py:377 run_mtls_spark_api` (first def) | shadowed by `:470` |
| `spark_daemon_decoded.py:409 execute_checked`, `:739 read_desired_cluster_state`, `:2564 _first_key` | `grep -c` one occurrence each, no test |
| `spark_daemon_decoded.py:2921-2946` handlers `handle_cluster_start/stop/create/destroy`, `handle_sync_settings` | `grep -rIn "cluster/start\|cluster/stop\|cluster/create\|cluster/destroy\|cluster/sync-settings" . --exclude=provision.py` finds only the router and docs |
| `cluster_new.py:626 get_dfs_engine`, `:617 sidon_detach_cmd`, `:1051 run_checked_cmd` | `grep -n "get_dfs_engine()\|sidon_detach_cmd\|run_checked_cmd(" cluster_new.py` shows definitions only; `spark.py:160` has the same unused `get_dfs_engine` |
| `catalyst.py:169 dispatch_ip` | one occurrence |
| `catalyst.py:288 "spark": queue.Queue()` | `grep -rn 'queues/spark\|"service": "spark"' --include=*.py --include=*.ex .` empty |
| `vali.py:856 hostname_for_ip` | one occurrence |
| `mipha.py:31`, `hylia.py:31 run_command_local` | each defined once, never called |
| `mcli-runner:14 is_zookeeper_leader` | `grep -n is_zookeeper_leader mcli-runner` shows the def only |
| `hylia.py:168` `return None` after `return` | read it |
| `helios_sidon.py:286 move_egroup`, `:304 dedup_estimate`, `:462 NbdWriter.write_at`, `:317 flush` (and `helios_sidon.snapshot/clone` besides tests) | valcli calls ops with raw dicts instead |
| `helios_schema.py:885 task_read_statement` | one occurrence |
| `logos.py` imports `base64`, `socket`, `subprocess` | one occurrence each |
| `rauru_replication.py` (404 lines) | `grep -ln rauru_replication *.py` is `test_rauru_replication.py` only; not in `deploy_updates.py`, `check_updates.py`, `create_upgrade_zip.py` |
| Daruk `/v1/schedule/claim-check` | `grep -rn "claim-check" . --include=*.py` outside `daruk.py` and tests is empty |
| `hydra.vali_tasks` | created `helios_schema.py:238`; written nowhere; read/deleted only by `valcli.py:3144-3158` |
| `dagur_schedules.cron_expression` | display only (3.7) |

Elixir: `SpectrumPhx.Spark` functions listed in 3.16 (`grep -rn "<name>" spectrum_phx/lib
spectrum_phx/test | grep -v spark.ex` returns nothing for each).

Rust: `sidon/src/main.rs:29` `mod replicate` items listed in R-12.

Spectrum routes with no caller found: `POST /api/vms/migrate`
(`grep -rIn "api/vms/migrate" static valcli.py spectrum_phx/lib` is empty). Routes reachable
only from legacy pages now routed elsewhere by Slate (`/settings.html`, `/index.html`, ...):
`/api/maintenance/*`, the `/api/urbosa/*` set, `/api/lanayru/*`; whether those pages are still
linked to **was not checked**.

### 4.4 Duplicated logic (itemised)

* **D-1** `handle_cluster_create/destroy` in spark (`:3294,3609`) vs `cluster_new.py`
  create/destroy (`:2441,3305`), each with its own service list.
* **D-2** Function bodies compared by AST (identical groups): `spark_endpoint` x12
  (`catalyst.py:44`, `dagur.py:47`, `mimir.py:41`, `vali.py:102`, `mipha.py:50`, `hylia.py:35`,
  `rauru.py:86`, `spectrum_server.py:319`, `valcli.py:39`, `urbosa_bootstrap.py:39`,
  `cluster_new.py:2258`, `spark_daemon_decoded.py:249`, ...); `run_remote_spark` x12 in 10
  distinct shapes (`cluster_new.py:653` and `:687` also repeat the certificate search); `run_lwt`
  x5 (`catalyst.py:95`, `spectrum_server.py:555` identical, `vali.py:297`, `mipha.py:1705`,
  `helios_snapshots.py:328`); `candidacy()` wrapper x7 (`catalyst.py:155`, `dagur.py:114`,
  `mimir.py:87`, `vali.py:348`, `hylia.py:173`, `mipha.py:232`, `spectrum_server.py:8548`)
  although `helios_zk.cluster_candidacy` (`helios_zk.py:1379`) exists; `get_catalyst_target_ip`
  x4 (`dagur.py:130`, `hylia.py:1158`, `vali.py:358`, `spectrum_server.py:8567`) plus
  `mipha.py:217 catalyst_target_ip` and `catalyst.py:169 dispatch_ip`; `get_zookeeper_leader_ip` x6 definitions (`mipha.py:250` and
  `vali.py:374` 46 lines each, `valcli.py:2777` and `spectrum_server.py:8495` identical
  48 lines, `hylia.py:162`, `spectrum_server.py:1496`); `parse_nodetool_status` x4
  (`vali.py:2134` = `mipha.py:1826`, `cluster_new.py:1128`, `spark_daemon_decoded.py:1950`)
  plus `mcli-runner:279 parse_nodetool_load`; `parse_json_rows` x4 (`mcli-runner:508`,
  `spectrum_server.py:967`, `helios_snapshots.py:82`, `mipha.py:851`); `check_urbosa_enabled`
  x3 (`cluster_new.py:2217`, `spark_daemon_decoded.py:461`, `spark.py:120`) and the substring
  `"true" in line.lower()` probe in `gatoway.py:142`, `spectrum_server.py:6230`, `lanayru`;
  `spark_local_ip` x2 (`cluster_new.py:2227`, `spark_daemon_decoded.py:216`);
  `load_helios_zk` x2 (`cluster_new.py:250`, `mcli-runner:326`); `get_local_ip` in 4 shapes
  (`urbosa.py:41`, `urbosa_bootstrap.py:23`, `daruk.py:8` identical; `logos.py:22`,
  `mcli:105`, `mcli-runner:222`); `get_cluster_hosts` x4 (`vali.py:583`, `mipha.py:297`,
  `hylia.py:143` identical, `mcli-runner:242`); `get_dfs_engine` x4 (`cluster_new.py:626`,
  `spark.py:160`, `vali.py:73`, `helios_sidon.py:50`).
* **D-3** `/api/v1/execute` clients: `catalyst.py:70`, `dagur.py:86`, `hylia.py:61`,
  `mimir.py:67`, `mipha.py:99`, `vali.py:127`, `valcli.py:66`, `urbosa_bootstrap.py:65`,
  `mcli:129`, `cluster_new.py:668`, `spark_daemon_decoded.py:329`, `spectrum_server.py:346`,
  `saga.py:935`, Phoenix `spark.ex:26`. Certificate paths differ: `/etc/hci/spark/certs/*`
  (daemons) vs `/root/.certs/*` (CLIs, Spectrum, Phoenix).
* **D-4** `generate_vm_xml`: `vali.py:971` (313 lines, live) and `spectrum_server.py:2122`
  (161 lines, no caller); `test_vm_graphics.py:58-74` exists only to keep the two in step.
* **D-5** Service lists (AST scan for literal collections with at least 5 declared unit names):
  `spark_daemon_decoded.py` 605, 632 (`MANAGED_SERVICES`), 1299, 1537 (`MANAGED_UNITS`),
  3548, 3640, 4894, 4904, 4970, 4975, 5043, plus the shell string at 2873; `cluster_new.py`
  245 (`SERVICE_DISPLAY_ORDER`), 2514, 2853, 3347; `hylia.py:1073`; `mipha.py:2107`;
  `vali.py:1868,1914`; `mcli` `checks_map` (`mcli:183`); `mcli-runner` 166, 1242, 1264,
  1859, 2684; `spark.py` 16, 176, 285, 397, 445. They disagree: the autostart list at `:5043`
  has 13 units, the destroy list at `:3640` 19, the leave fallback at `:2873` 17 (no `hylia`,
  `urbosa`). The test guards only against *unknown* names
  (`test_spark_shell_calls.py:268`), not against missing ones.
* **D-6** Dagur run recording: `dagur.py:179-228` and `spectrum_server.py:8600-8632`.
* **D-7** `copies_for_ftt`: `sidon/src/control.rs:131` and `valcli.py:1351`.
* **D-8** Quorum arithmetic (`n // 2 + 1`): `cluster_new.py:1421 quorum_size` (ZooKeeper
  voters) and `:2178 quorum_of` (Hydra RF), `vali.py:2182` (same as `quorum_of`),
  and inline in `mcli-runner:2383,2386,2416,2451,2548,2551`.
* **D-9** Cluster-setting boolean probes: `gatoway.py:142`, `spark_daemon_decoded.py:461`,
  `cluster_new.py:2217`, `spark.py:120`, `spectrum_server.py:6230`, all by substring match.
* **D-10** Phoenix `Spark` wrappers duplicating live spark clients in Python (3.16).
* **D-11** Daruk unit file text written twice in `provision.py` (`:1360-1380` and
  `:1530-1548`); not touched.
* **D-12** Health checks run by both Dagur and Mimir (R-16); `purah-scrub` by four triggers (R-8).
