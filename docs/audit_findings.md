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

### D. Hardcoded Subnet Masks for Virtual IP (VIP)
*   **Location:** `bifrost.py` (lines 130, 189, 198)
*   **The Issue:** Bifrost hardcodes `/24` subnet masks when adding or deleting the virtual IP (VIP) to physical network interfaces (`ip addr add {vip}/24 ...`).
*   **Impact:** If the cluster network uses a different size (e.g. `/22` or `/16`), binding the VIP with `/24` will break routing to other hosts outside the `/24` range or cause IP subnet overlaps.
*   **Recommendation:** Dynamically fetch the subnet mask of the host interface or read the netmask configuration from `/etc/hci/cluster.json` and apply it to the VIP binding command.

### E. Fragile Default Interface Parsing
*   **Location:** `gatoway.py` (lines 68–77)
*   **The Issue:** The helper function `get_default_interface` performs a flat space split on the entire stdout of `ip route show | grep default` without handling multi-line outputs.
*   **Impact:** If a host has multiple default routes configured, the function returns the interface associated with the first occurrence of the `"dev"` token in the combined output string. If this is a lower-priority metric route, the VLAN bridge will bind to the incorrect physical interface, breaking external VM connectivity.
*   **Recommendation:** Iterate through the lines of the default route output, parse them individually, and select the interface belonging to the active default route with the lowest metric.


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

### C. Chicken-and-Egg Fencing Failure (Spark-Only Fallback)
*   **Location:** `mipha.py` (lines 504–516)
*   **The Issue:** The SSH fencing helper function (`ssh_fence_host`) does not use actual SSH. Instead, it relies entirely on Spark Daemon's port 9099 mTLS execution API to stop libvirt and kill QEMU guest processes.
*   **Impact:** If a node's software crashes (e.g. `spark-daemon` hangs, database freezes, or network partition occurs), Spark will be unresponsive on port 9099. Because passwordless root SSH keys are deleted from `/root/.ssh/` post-installation for security hardening, the coordinator has no alternative pathway (like passwordless SSH or IPMI power controls) to execute the fence. The fence fails, causing Vali to restart VMs on surviving hosts while they are still running on the unresponsive host. This dual-Primary execution will instantly corrupt VM virtual disks.
*   **Recommendation (Fencing Keys in Cluster Bootstrap):** 
    During the `cluster create` bootstrap sequence (managed by `cluster_new.py`), instead of completely deleting all inter-node passwordless SSH keys, the system should generate and distribute a dedicated, restricted SSH key pair (e.g. `/root/.ssh/id_rsa_fencing`) for the HA/fencing daemons:
    1.  **Restrict Authorized Keys Options**: Configure `/root/.ssh/authorized_keys` on each node to restrict the key's execution privileges (e.g., using `command="/usr/local/bin/fence_node"` and restricting the source IP to cluster nodes only). This prevents the key from being used for arbitrary root command execution while preserving passwordless access for host fencing.
    2.  **IPMI/PDU Out-of-Band Fallback**: Implement out-of-band STONITH commands in `cluster create` (registering IPMI credentials in ScyllaDB) so that if the network-level SSH fence fails, the coordinator can directly power-cycle the server hardware.



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

### B. ScyllaDB Database Ring Membership on Maintenance
*   **Location:** `vali.py` and `catalyst.py`
*   **The Issue:** When a host enters maintenance, its ScyllaDB database container (`hydra-db`) is simply stopped. The remaining database nodes continue to treat it as an active member of the token ring, accumulating hinted handoffs (mutations) locally. If the maintenance is long-running (exceeding ScyllaDB's maximum hint window, typically 3 hours), the node becomes heavily out of sync, requiring a manual `nodetool repair` on recovery to prevent data consistency gaps.
*   **Impact:** Stopping a node without decommissioning degrades query availability. For larger clusters (e.g. $N \ge 4$), the cluster remains vulnerable to subsequent node failures while a node is down.
*   **Recommendation (ScyllaDB Ring Decommission/Rejoin via Catalyst Tasks):**
    During host maintenance, upgrade, or failure-induced quarantine events, Catalyst should coordinate ring membership modifications under specific constraints:
    1.  **Fast Rolling Upgrade vs. Long-Term Maintenance**:
        - **Short-Term reboots (e.g. Hylia software updates < 3 hours)**: The node should **not** be decommissioned. Decommissioning streams gigabytes of data partitions across the network, generating heavy load. For quick rolling updates, let the node go offline, reboot, and catch up using ScyllaDB's built-in **hinted handoff** mechanism.
        - **Long-Term maintenance or Hardware repairs**: Execute a full decommission to maintain cluster resilience.
    2.  **Auto-Quarantine Trigger (Mipha Integration)**:
        - If Mipha detects host software or hardware faults (Node Quarantine), it must automatically trigger the `host_maintenance_enter` Catalyst task, which handles VM evacuations and initiates the database decommissioning sequence to isolate the faulty node cleanly.
    3.  **Single-Node ($N=1$) and Two-Node ($N=2$) Exclusions**:
        - Decommissioning is mathematically impossible on a 1-node cluster (ScyllaDB requires at least one surviving node).
        - Decommissioning a node in a 2-node cluster drops the active node count to 1. Since keyspace replication is RF=2, the surviving node will fail `QUORUM` checks.
        - Therefore, the decommission task must be **bypassed** unless the active node count *before* entering maintenance is **at least 3** ($N_{active} \ge 3$).
    4.  **Auto-Join on Reconnection**:
        - When the host leaves maintenance or resolves its fault, Catalyst starts the ScyllaDB container in join mode, allowing it to bootstrap, stream its assigned tokens, and transition back to `NORMAL`.



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

---

## 12. Host Isolation & Self-Fencing Design Recommendations

### A. Lack of Automated Host Isolation
*   **The Issue:** The cluster has no logic to handle nodes experiencing partial software failures (e.g. ScyllaDB crashes, Linstor Satellite hangs, or physical disk I/O errors) while network connectivity and SSH remain active.
*   **Impact:** If a host's storage or database daemon crashes, running VMs on that host will freeze or experience read/write failures. Because the host's operating system is still online, Mipha's ping checks see the host as healthy and do not trigger a failover, causing VMs to remain locked in a broken state indefinitely.
*   **Recommendation (Software Failure Isolation & Self-Fencing):**
    1.  **Software Fault Isolation & Local Self-Fencing (Watchdog-Initiated)**: If Spark's local watchdog thread detects that local database or storage services have failed, or are caught in a **restart/crash loop** (restarting repeatedly and failing again more than 5 times in 10 minutes, hitting systemd start-limit limits):
        - Spark should attempt basic remediation (clearing locks, flushing sockets).
        - If remediation fails, Spark triggers a **Host Isolation / Quarantine** state, notifying Catalyst to gracefully evacuate local VMs.
        - If communication is lost (Catalyst or ZooKeeper unreachable), the Spark daemon must execute **Self-Fencing** by immediately pausing or force-stopping all local VMs. This acts as a critical safety gate: it ensures local VM instances are dead/suspended before the cluster attempts to spawn them elsewhere, preventing filesystem corruption from concurrent double-writes.
    2.  **Cluster-Level Failover & Fencing (Mipha HA Coordinator)**: If Mipha detects that a host is unresponsive (or reporting critical crash loops):
        - Mipha transitions the host status to `DEGRADED` in ScyllaDB.
        - Vali stops scheduling new workloads to the host.
        - Catalyst queues a task to evacuate all VMs from the degraded host and decommissions the database node from the ring (if $N_{active} \ge 3$).
        - **Failover Boot Sequence**: Once Mipha confirms the degraded host is offline or fenced (either verified via Spark, or after executing an out-of-band IPMI STONITH power-off), Mipha coordinates with Vali and Catalyst to **safely spawn the VMs on healthy nodes**.
    3.  **Self-Fencing on Quorum Loss (Network Partition)**: If a host's local Spark daemon detects that it has lost connection to ZooKeeper consensus (or cannot contact ScyllaDB seeds) for more than 30 seconds:
        - The host must assume it is partitioned. To prevent split-brain writes on DRBD storage, it must automatically demote its local DRBD storage resources to `Secondary` and suspend all local running virtual machines.


---

## 13. Proposed Mimir/Mcli Diagnostic Health Checks

To proactively detect and surface the architectural gaps identified in this audit, Mimir's health checking system (`mcli-runner` / `mcli`) should be extended with the following diagnostic checks:

### A. Core services checks (Category: `services`)
1.  **ZooKeeper Ensemble Scale Check (`zookeeper_ring_scale`)**:
    *   **Logic**: Query the ZooKeeper server list configuration. If the number of voting nodes exceeds 5 or 7, trigger a warning recommending that secondary nodes be configured as ZK Observers.
2.  **ScyllaDB 2-Node Quorum Check (`scylladb_quorum_safety`)**:
    *   **Logic**: Query host counts and keyspace replication factor. If the cluster has exactly 2 nodes and SimpleStrategy replication is RF=2, verify database consistency level. If `QUORUM` is enforced, flag a critical warning indicating zero tolerance for node failure.
3.  **mTLS Certificate Expiration Alert (`mtls_cert_expiry_warning`)**:
    *   **Logic**: Parse CA, node, and client certificates (`/etc/hci/spark/certs/node.crt`) and compute the remaining lifetime. Flag a warning if the remaining lifetime is less than 30 days.
4.  **Fencing SSH Keys & Out-of-Band Setup Check (`fencing_access_check`)**:
    *   **Logic**: Verify that the dedicated fencing SSH key pair (`id_rsa_fencing`) exists on the host, that `/root/.ssh/authorized_keys` restricts execution to `/usr/local/bin/fence_node`, and that registered IPMI IP addresses are pingable.
5.  **Service Watchdog Active Check (`watchdog_daemon_status`)**:
    *   **Logic**: Query the status of Spark's background watchdog thread. Flag a failure if the watchdog loop has crashed or is not executing health probes.

### B. Storage engine checks (Category: `storage`)
1.  **DRBD Split-Brain Dual-Primary Check (`drbd_split_brain_check`)**:
    *   **Logic**: Parse `drbdsetup status --json`. If any replication volumes are in `StandAlone` state while their roles are both `Primary`, trigger a critical failure alarm.
2.  **Linstor Controller Query Latency Check (`linstor_latency_check`)**:
    *   **Logic**: Execute a lightweight Linstor CLI query (e.g., `linstor node list`) and measure the execution time. If it exceeds 5 seconds, flag a latency warning indicating a possible database blocking hang or offline node.

### C. Scheduler & DRS checks (Category: `drs`)
1.  **DRS Storage Capacity Gate Check (`drs_storage_capacity_check`)**:
    *   **Logic**: Verify that Vali's DRS balancer checks Linstor thin pool storage capacity before initiating migrations, alerting if DRS is running without storage capacity validation.
2.  **Concurrent Migration Lock Auditor (`migration_lock_status`)**:
    *   **Logic**: Audit the ScyllaDB active VM tables to ensure that no live migrations are running concurrently without corresponding locks, flagging collisions.

---

## 14. Additional Hardcoded Constraints & Startup SPOFs

### A. Air-Gap Registry Hardcoding
*   **Location:** `provision.py` (lines 88, 109, 129, 153, 204)
*   **The Issue:** Container image source registries are hardcoded to public hosts (`docker.io` and `quay.io`) for core system components (ScyllaDB, ZooKeeper, Linstor, Traefik).
*   **Impact:** Enterprise installations are frequently deployed in secure, air-gapped data centers without external internet access. The lack of a configuration parameter to prefix custom local registries prevents container pulls and boots during provisioning.
*   **Recommendation:** Add a registry configuration variable or CLI flag (e.g. `--registry <local-registry-ip>`) to prefix container images during Quadlet configuration generation.

### B. Daruk Startup Connection SPOF
*   **Location:** `daruk.py` (lines 21–25)
*   **The Issue:** Daruk connects to the local ScyllaDB node at module load time (import phase) with a single connection attempt.
*   **Impact:** During cluster boot, if the `daruk` service starts before ScyllaDB completes its replay log checks and opens port 9042, Daruk crashes and exits. Because all other services (Spectrum, Vali, Catalyst, Mimir) query ScyllaDB through the Daruk HTTP proxy, a Daruk startup failure cascades into a complete cluster orchestration freeze.
*   **Recommendation:** Wrap Daruk's connection initialization in a retry loop with exponential backoff on startup, allowing it to wait for ScyllaDB to stabilize and begin responding.




