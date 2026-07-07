# Helios-HCI Code Audit Findings & Architectural Gaps

This document outlines critical issues, edge cases, and design bottlenecks identified during the deep audit of the Helios-HCI codebase.

---

## 1. Quorum & Scale Boundaries ($N=1$, $N=2$, $N=40$)

### A. ZooKeeper Ensemble Scale Overload
*   **Location:** `cluster_new.py` (lines 661–670) and `provision.py`
*   **The Issue:** The provisioner writes all node IPs directly into the ZooKeeper server list (`ZOO_SERVERS`), meaning every node in the cluster joins the voting ring. 
*   **Node Count Impact:**
    *   **$N=2$ (Fragile Quorum):** ZK requires $(2/2)+1 = 2$ nodes for quorum. If 1 node fails, ZooKeeper loses consensus and shuts down. The remaining node becomes completely inoperable as all daemons fail their leader checks.
    *   **$N \ge 7$ (e.g., 40 nodes):** ZooKeeper's consensus protocol (Zab) suffers heavy latency when the voting ensemble grows beyond 5 or 7 nodes. Having 40 voting members will cause high latency, synchronization timeouts, and split-brain elections.
*   **Recommendation:** Limit the ZooKeeper voting ensemble to a maximum of 3 or 5 designated nodes, and configure the rest of the cluster nodes as ZooKeeper **Observers** (which receive state updates but do not participate in voting).

### B. ScyllaDB Hardcoded QUORUM Consistency
*   **Location:** `daruk.py` (line 25)
*   **The Issue:** The Medusa DB proxy forces database query execution to use `ConsistencyLevel.QUORUM`.
*   **Node Count Impact:**
    *   **$N=2$ (Zero Tolerance):** The `hydra` keyspace uses SimpleStrategy with a replication factor equal to the node count (`desired_rf = min(3, node_count)` = 2). A write or read under `QUORUM` requires $(2/2)+1 = 2$ active nodes. If a single node goes down, **every database query fails**, rendering the surviving node useless even though its container databases are active.
*   **Recommendation:** Implement dynamic consistency configuration. For 2-node clusters, fallback to `ConsistencyLevel.LOCAL_ONE` or `ConsistencyLevel.ONE` when a node goes down, or require the user to configure a replication factor of 1 with a secondary standby disk model.

### C. Hardcoded IP Fallback Arrays
*   **Location:** `vali.py` (line 207) and `mipha.py`
*   **The Issue:** When retrieving cluster hosts from `/etc/hci/cluster.json` fails, the scripts fall back to a hardcoded IP array:
    `ips = ["10.10.102.220", "10.10.102.222", "10.10.102.223"]`
*   **Impact:** If the cluster runs on a different subnet, has 1 node, or has 40 nodes, the fallback causes connection timeouts or queries invalid nodes.
*   **Recommendation:** Replace hardcoded fallbacks with localhost queries or raise a clear configuration error if `cluster.json` cannot be loaded.

---

## 2. High Availability & Partition Failures

### A. Linstor Database HA Promotion Locks
*   **Location:** `mipha.py` (lines 155–173)
*   **The Issue:** When ZooKeeper leader election changes, the new leader coordinates with other hosts to release `linstor-db` by executing a remote release script. However, this is gated by `ping_host(ip)`.
*   **Impact:** If the previous leader node is partitioned (network split, not pingable) but still powered on, it will keep `linstor-db` promoted as `Primary` and mounted. The new leader will skip the remote release command, attempt to run `drbdadm primary linstor-db`, and fail due to split-brain locking. The database volume will fail to mount on the new leader, halting all cluster management.
*   **Recommendation:** Force-fence the unresponsive node via IPMI/PDU (STONITH) before attempting to promote the DRBD resource locally, or use `drbdadm primary --force linstor-db` after a timeout.

### B. Bifrost Split-Brain VIP Conflict
*   **Location:** `bifrost.py` (lines 63–79)
*   **The Issue:** If ZooKeeper consensus is lost (e.g. partition in a 2-node cluster), `get_zookeeper_leader_ip()` returns `None`. Bifrost then falls back to sorting all nodes with an active port 8443 (Spectrum) open and binds the VIP to the first one:
    `candidates.sort(); return candidates[0]`
*   **Impact:** Both sides of a partitioned cluster will see their own Spectrum portal as active. If the partition splits the hosts, both nodes could bind the VIP locally, causing MAC address conflicts and routing issues.
*   **Recommendation:** If ZooKeeper consensus is lost, Bifrost should immediately release the VIP and log a consensus warning rather than falling back to IP sorting.

---

## 3. Upgrade Safety Gaps

### A. Hylia Pre-Flight Storage Check Gaps
*   **Location:** `hylia.py` (lines 470–530)
*   **The Issue:** During rolling upgrades, Hylia evacuates a host's VMs and triggers a reboot. It checks if the host's storage volumes are synchronized *after* the reboot, but not *before*.
*   **Impact:** If the cluster storage is already degraded (e.g., Node 3's disk is broken, so Node 2 contains the only healthy copy of a VM's data) and Hylia reboots Node 2, the VM's storage becomes completely unavailable, causing VM crash or data corruption.
*   **Recommendation:** Implement pre-flight checks in Hylia that verify that **all** DRBD resources across the **entire cluster** are fully synchronized and healthy before taking a node offline.

---

## 4. Performance & Threading Scale

### A. Sequential DRS Metrics Polling
*   **Location:** `vali.py` (lines 532–558)
*   **The Issue:** The DRS scheduler loops through all cluster nodes sequentially to fetch CPU/RAM usage.
*   **Impact:** Gathering CPU metrics takes ~0.7 seconds per node due to remote execution handshakes and `time.sleep(0.2)` measurements. In a 40-node cluster, this loop blocks the scheduler thread for ~28 seconds. If any node is slow or unresponsive, connection timeouts will cause the loop to exceed the DRS run window.
*   **Recommendation:** Query host utilization metrics in parallel using Python's `concurrent.futures.ThreadPoolExecutor`.

---

## 5. Aether (Storage & DRBD) Reliability Hellholes

### A. Split-Brain StandAlone Deadlocks
*   **Location:** `mipha.py` (lines 52–68)
*   **The Issue:** When a DRBD resource enters `StandAlone` (due to split-brain data divergence), Mipha's auto-resolution check only triggers connection resets with `--discard-my-data` if the local node role is `Secondary`.
*   **Impact:** If a network partition occurs while VMs are active on both nodes, both nodes will have promoted their local resources to `Primary`. Since both nodes report `role == "Primary"`, neither will match the resolution condition (`role != "Primary"`), leaving the DRBD volumes permanently stuck in `StandAlone` and replication suspended until manual operator intervention.
*   **Recommendation:** Enhance `resolve_drbd_standalone` to check if a split-brain is active, determine which node has the latest writes (or let ZooKeeper designate the leader's storage as correct), force-demote the other node to `Secondary`, and run the connection reset.

### B. Blocking Sequential Linstor Command Hangs
*   **Location:** `spectrum_server.py` (lines 265–287)
*   **The Issue:** `run_linstor_cmd` loops through all cluster node IPs sequentially to attempt a `podman exec` call inside the `systemd-aether` container.
*   **Impact:** If a node is completely offline, `run_remote_spark` will wait for the full default socket timeout (45 seconds). When creating, deleting, or resizing VM disks, multiple Linstor commands are executed sequentially. If any node is unresponsive, VM operations will block for several minutes, causing task timeouts in Catalyst or browser gateways.
*   **Recommendation:** Fast-fail node connections by pinging them first, or query only the active Linstor Controller IP (resolved via ZooKeeper) rather than iterating through all nodes.

---

## 6. Spark Bootstrap & Security Risks

### A. mTLS Certificate Expiration Freeze
*   **Location:** `/etc/hci/spark/certs/` and `/root/.certs/`
*   **The Issue:** Spark's mTLS authentication CA, node, and client certificates are generated during initial provisioning with a fixed lifetime but no automated renewal daemon or renew script.
*   **Impact:** When the certificates expire, all inter-node communication (which relies entirely on Spark port 9099) will fail, freezing all orchestration, monitoring, and failover operations.
*   **Recommendation:** Implement an automated renewal task or cron script that checks certificate validity and rotates them.

### B. Unsandboxed Root Command Execution
*   **Location:** `spark_daemon_decoded.py` (POST `/api/v1/execute` handler)
*   **The Issue:** Spark executes incoming command string parameters as `root` directly in the host namespace via `subprocess.Popen(shell=True)`.
*   **Impact:** Any command injection vulnerability in Spectrum's Web API or Vali's VM task scheduler can immediately escalate to complete root host compromise across all cluster hypervisors.
*   **Recommendation:** Implement a strict command whitelist or parameter-based wrapper rather than executing arbitrary shell command strings.

---

## 7. Vali (Scheduler & DRS) Constraints

### A. Lack of Storage Capacity Gates in DRS
*   **Location:** `vali.py` (lines 587–620)
*   **The Issue:** Vali's DRS balancer only evaluates memory and CPU usage when selecting a target host for VM live migration.
*   **Impact:** Since VM thin pool storage (/dev/sdb) is local to each host and managed via DRBD, if a VM is migrated to a host with an almost full storage pool (even if CPU/RAM load is low), the VM's disk write operations will freeze, crashing the guest file system.
*   **Recommendation:** Add storage pool capacity checks to the DRS host evaluation matrix.

### B. Concurrent Migration Collisions
*   **Location:** `vali.py`
*   **The Issue:** DRS migrations run asynchronously. While a live migration is in progress (`virsh migrate --live ...`), there is no migration lock set in the database.
*   **Impact:** If a DRS run initiates a migration and another scheduling task (e.g. manual VM move or Hylia node evacuation) executes concurrently, libvirt and QEMU will conflict, resulting in corrupted VM descriptors or duplicate running guest processes (split-brain execution).
*   **Recommendation:** Implement a global `is_migrating` lock column in `hydra.vms` database table.

---

## 8. Runtime Service Watchdog / Auto-Restart Absence

### A. Missing Service Watchdog
*   **Location:** Host Systemd Services and Container Quadlets
*   **The Issue:** While native systemd units define `Restart=always`, there is no cluster-wide or local node-level watchdog daemon to monitor the health of Spark, Vali, Catalyst, or containerized services (`zookeeper`, `hydra-db`, `aether`).
*   **Impact:** If a daemon hangs, deadlocks, enters a `failed` state due to start-limit-burst, or becomes unresponsive without exiting its process, the systemd state remains "active" but the cluster service is dead. Nothing detects or restarts these silent failures, causing cluster operations to freeze.
*   **Recommendation (Spark as "Genesis" Orchestrator):**
    1. **Disable Service Autostart in Systemd**: All native Python and containerized services (except `spark-daemon` and `zookeeper`) should be configured as **disabled** in systemd by default (`systemctl disable`). Systemd should *never* start them directly on boot.
    2. **Orchestrate Startups via Spark**: The `spark-daemon` (acting as the True Genesis equivalent) should own the cluster startup loop. When booting, Spark polls ZooKeeper consensus:
       - If ZooKeeper is standalone or the local node is elected leader, Spark starts the core database (`hydra-db`), local storage (`aether`), and leader-specific management daemons (`catalyst`, `vali`, `mipha`, `bifrost`).
       - If the local node is a follower, Spark only starts the database, storage, metrics collector (`logos`), and standby daemons, keeping management daemons stopped.
    3. **Background Watchdog Loops**: Spark should spawn a background watchdog thread running periodic HTTP health and responsiveness checks on local services (e.g. GET `/api/status` on Spectrum, `/api/v1/hosts` on Vali, TCP port probes, and `podman ps` checks). If a service becomes unresponsive or enters a degraded state, Spark performs remediation (e.g., stops/starts the unit, clears transient lockfiles, or alerts the cluster Catalyst coordinator).


---

## 9. Maintenance Mode Operational Bottlenecks

### A. Database Freeze on 1-Node and 2-Node Clusters
*   **Location:** `vali.py` (lines 1404–1414)
*   **The Issue:** When a host enters maintenance mode, all local services—including the `hydra-db` (ScyllaDB) container—are shut down.
*   **Impact:** 
    *   **$N=2$:** When Node A enters maintenance, its ScyllaDB instance stops. The cluster has only 1 active database node left. Since consistency is hardcoded to `QUORUM`, any query submitted on the surviving Node B will immediately fail, freezing the entire cluster.
    *   **$N=1$:** Entering maintenance stops the only database node, crashing the cluster entirely.
*   **Recommendation:** Gating maintenance mode to require $N \ge 3$ active nodes, or dynamically altering the DB consistency level/replication factor during maintenance transitions.

---

## 10. Aether/Linstor Storage Auto-Heal Deficiencies

### A. Lack of Active Storage Auto-Heal
*   **Location:** `mipha.py` and `spectrum_server.py`
*   **The Issue:** Aether has no automated recovery loops for physical disk errors, LVM thin pool exhaustion, or DRBD metadata corruption.
*   **Impact:** If a drive fails or falls offline, or if a DRBD resource enters a degraded state, Aether does not attempt to reconstruct or migrate replica pools to surviving hosts. The storage remains degraded until manually resolved by an operator.
*   **Recommendation:** Introduce a dedicated Storage Health Monitor to automatically trigger Linstor volume re-creation or replica moves when hardware faults are detected.

---

## 11. Missing Core Cluster Services

### A. Backup & Disaster Recovery (DR) Manager
*   **The Issue:** There is no service or utility to back up the cluster database (`hydra` keyspace) and Linstor configuration metadata to an external target.
*   **Impact:** If ScyllaDB data corruption occurs or DRBD metadata is wiped across multiple nodes, the cluster cannot be restored, leading to complete VM data loss.
*   **Recommendation:** Create a backup daemon that periodically snapshots the ScyllaDB database and copies it to a configured external NFS share or S3 target.

