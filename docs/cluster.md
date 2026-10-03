# Cluster Management & Lifecycle Orchestration

This document details the lifecycle management, orchestration pathways, and operational syntax for bootstrapping, starting, stopping, and destroying the Helios-HCI cluster.

---

## 1. Overview of the `cluster` Utility

The `cluster` CLI utility (`/usr/local/bin/cluster`) is an administrative orchestration tool. Instead of interacting with individual nodes manually, administrators run `cluster` commands to distribute configurations and manage state across the entire hypervisor pool.

### Command Execution Route
1. The administrator runs the `cluster` CLI command on the local console.
2. The CLI calls the local `spark-daemon` on mTLS port `9099`.
3. The local `spark-daemon` acts as the coordinator, making concurrent mTLS calls to the `spark-daemon` instances on all peer nodes to distribute configuration scripts, synchronize states, and start/stop systemd workloads in parallel.

---

## 2. Command Reference & Syntax

### A. Cluster Creation (`cluster create`)
Bootstrap a new cluster across a set of physical hosts.

#### Layouts
Every host is a **full member**: a hypervisor, a storage node, a ScyllaDB (Hydra) node, and
it runs every control-plane daemon. There is no lightweight or diskless role.
- **1 node**: no replication; the redundancy factor is forced to 0.
- **2 nodes**: both hosts are ZooKeeper voters, so the ensemble needs **both** up -- it has no
  tie-breaker and tolerates no failure. See *What there is not* below.
- **3 nodes**: all three vote in ZooKeeper, which tolerates the loss of one host. With `-r 1`
  every vdisk asks for two copies, on any two of the three.
- **4+ Nodes**: ZooKeeper consensus quorum is maintained by the first 3 nodes as voting members, and additional hosts are automatically configured as observers to scale the cluster cleanly. That is the starting set, not a fixed one — `cluster zk-promote` / `zk-demote` (section H) move the vote between members afterwards.

#### What there is not: a witness node
Earlier versions of this document described a diskless third host acting as a quorum
tie-breaker for two-node clusters (July 2026, commits `6cd254c` to `6ebbc6c`). **That mode does
not exist in the code**: there is no `--witness` flag and no `is_witness` field in
`cluster.json`, and a three-node cluster runs HydraDB, Daruk and Sidon on all three hosts.
The need it served was real -- an even ensemble tolerates nothing, and DRBD wanted an odd
number of voters per volume -- but only the first half survives DRBD. A two-node cluster
currently has no tie-breaker; whether to bring one back is a decision, recorded in
[TODO.md](../TODO.md).

```bash
# Syntax
cluster create -s <IP1,IP2,IP3,...> [-r <redundancy_factor>] [-v <virtual_ip>]

# Example: a 3-node cluster; every host is a full member and all three vote
cluster create -s 10.10.102.220,10.10.102.222,10.10.102.223 -r 1 -v 10.10.102.240
```
**Creation Workflow** (the phases `cluster create` prints):
1. **Connectivity and pre-checks.** `spark-daemon` answers on every host over mTLS, port conflicts
   are reported, and any running cluster services are stopped so the bootstrap starts clean.
   There is no Secure Boot check: Sidon loads no kernel module.
2. **Hostnames and cluster setup.** Resolves each hostname and writes `/etc/hci/cluster.json`
   (hosts, node ids, redundancy factor, VIP) on every node.
3. **Disk scan, and the extent store's volumes.** Finds an empty disk of at least 100 GB on each
   host and builds the thin-provisioned LVM pool (the volume group is still named `vg_aether`, a
   name left over from the DRBD design). Then, on every host and in this order, it carves the
   `vg_aether/sidon` volume and gives it a filesystem, registers every further empty disk, and
   writes `/etc/hci/sidon-disks`, which names each filesystem by UUID. `cluster destroy` removes the
   volume group, so a create that stopped at the pool would leave sidon nothing to journal to. None
   of it is mounted and nothing is written to `/etc/fstab`: sidon mounts what the file names when it
   starts ([D-27](./dfs/decisions.md)).
4. **Coordination, metadata and storage.** Writes the per-host configuration, starts ZooKeeper and
   records the desired state `started`, starts ScyllaDB (waiting for it to listen on 9042) and
   starts Daruk (waiting for 9043). Only then does it start sidon, which mounts the disks, and
   verify that each node's extent store answers with capacity, waiting up to 90 seconds because
   `systemctl restart` returns before the control socket exists. A disk it could not mount is
   printed as a warning; a node where sidon never answers is shown what sidon itself reports
   (unit state, journal tail, mounts). The order is the one `cluster start` follows: sidon keeps
   vdisk ownership in Hydra, so it comes up behind it. Long steps print what they are doing and,
   every ten seconds, which nodes they are still waiting on.
6. **Core services.** Starts the application daemons. This phase still starts services by hand,
   which `cluster start` no longer does; converting it is recorded in [TODO.md](../TODO.md).
7. **Liveness and health.** Verifies that every Sidon peer is reachable and that Spectrum
   answers on 8443.

(Phase 5 no longer exists: it was the DRBD/Linstor bring-up.)

### B. Cluster Status (`cluster status`)
Query cluster health and engine statistics.
```bash
# Check basic status (shows whether cluster is started/stopped and online hosts)
cluster status

# View verbose status (includes storage resource layouts, node roles, and detailed daemon states)
cluster status --verbose
```

### C. Cluster Startup (`cluster start`)
Resume cluster operations after the nodes have been powered on or stopped.
```bash
# Record the desired state on every node and watch them converge
cluster start
```
This names no service. It records `started` on each node, and each node's reconcile loop
starts what it manages in dependency order and publishes what it achieved, including why
anything refused. The ordering lives with the loop, in one place —
[cluster_state.md](./cluster_state.md#3-one-actor-per-service).

### D. Cluster Stop (`cluster stop`)
Safely quiesce active virtual machines and put the services to rest.
```bash
# Shut guests down, declare 'stopped', then quiesce the state store last
cluster stop
```
The same shape inverted: guests first (which host a guest runs on is cluster state, not
node state), then `stopped` is declared and each node stops what it manages in reverse
order, draining its storage journals before the storage daemon goes. ZooKeeper is stopped
afterwards, by an explicit call, because the loop cannot take away the store it reads its
instructions from.

### E. Ring Inspection (`cluster ring`)
Print the ScyllaDB (Hydra) ring beside the cluster's own membership. The two are separate
records and drift apart silently — a node marked `DOWN` in `hydra.nodes` is still a ring
member holding replicas, and a ring member with no `hydra.nodes` row is a node the cluster
has forgotten but `QUORUM` has not.
```bash
cluster ring
```
Shows the keyspace replication factor, what `QUORUM` therefore requires, each member's
up/normal state and host ID, and which side of the two memberships each host is on.

### F. Node Addition (`cluster add-node`)
Bring an already-provisioned, already-enrolled machine into an existing cluster. This is
deliberately **not** `cluster create` with one more address: create claims disks, and
`wipefs -a` against nodes that are already serving guests is not a recoverable mistake.

The node must be prepared first. `add-node` refuses a machine that cannot answer over
mTLS, and refuses one that already carries ScyllaDB data.
```bash
# 1. On the workstation: install the stack on the new machine
python provision.py --join --hosts 10.10.102.43

# 2. Issue it a certificate this cluster's CA signed
impa enroll --node 10.10.102.43

# 3. On any existing node: bring it in
cluster add-node --node 10.10.102.43
```

The order inside step 3 is not arbitrary, and each step exists because skipping it fails
silently rather than loudly:

| Order | Step | Why it is where it is |
| --- | --- | --- |
| 0 | **Identity** | The node is told its own address in `/etc/hci/spectrum/spectrum.env` before it is given any cluster responsibility. See below — this one is not local. |
| 1 | **Membership** | Every node's `cluster.json` learns the new address first, so anything reading the host list mid-restart sees the intended membership rather than a half-written one. |
| 2 | **Consensus** | The ZooKeeper ensemble is rewritten and restarted one node at a time, oldest first, which keeps a quorum of the *previous* ensemble alive throughout. |
| 3 | **Storage** | ScyllaDB is seeded from the existing cluster and the tool waits for the ring to report the node `UN`. |
| 4 | **Scheduling** | Only now is the node written into `hydra.nodes`. Registering it earlier hands it VMs it cannot yet run. |

`add-node` is **resumable**. A join that fails part-way leaves the node in `cluster.json`
and out of the ring, and re-running finishes from there rather than refusing because the
address is already in the config.

#### Adding a node does not change the redundancy factor
`cluster create` defaults `redundancy_factor` to 1 and forces it to 0 for a one-node
cluster, which is right at the time: there is nowhere to put a second copy. Nothing
revisited it as the cluster grew, so a cluster created on one node and grown to three kept
`redundancy_factor: 0` for as long as it lived, every new vdisk was created with **one
copy**, and `valcli storage.replication` read the 0 back as the operator's decision.

The factor is a replication policy, so `add-node` does not change it on its own. What it
does instead:

* **Warns, loudly, at the end of every join** (including a resumed one) when the cluster
  now has two or more nodes and the factor is 0 or missing, and prints the command that
  settles it:
  ```bash
  cluster add-node --node 10.10.102.43 -r 1
  ```
* **Takes `-r N` explicitly.** On a join it writes `redundancy_factor` into `cluster.json`
  with the new membership, in the same write to every node; the factor is checked against
  the grown cluster's size *before* anything is changed, and refused rather than clamped.
* **Settles an existing cluster.** Given a node that is already a live member, `-r N`
  joins nothing and changes only the factor. That is the way out for a cluster grown
  before this existed. It is idempotent.

Two things it deliberately does not do. Sidon reads `cluster.json` when it starts, so the
new factor reaches *creates* only after sidon is restarted on each node, one at a time;
the command says so. And disks that already exist keep the `rf` they were created with —
`valcli storage.replication` lists them and `valcli storage.replicate --all` tops them up,
one copy per run. `valcli storage.replication` also prints a note when it sees factor 0 on
a multi-node cluster.

#### Why identity comes first
A node reads its own IP from `LOCAL_HYPERVISOR_IP` in `/etc/hci/spectrum/spectrum.env`.
Eleven Python modules and the Phoenix console read that key, and every one of them falls
back to `127.0.0.1` when it is missing.

That fallback is not a degraded mode — it is a cluster-wide outage waiting for an
election. Vali's Catalyst queue worker runs **only** on the node holding the `vali-queue` candidacy ([service_leadership.md](./service_leadership.md)), and decides
whether it is the leader by comparing the leader's address against its own. A node that
believes it is `127.0.0.1` can never match, so it never drains the queue; and because the
leader is the only worker, leadership landing on such a node stops every VM power,
migrate and DRS task **in the whole cluster**. Each one still returns, eventually, as a
timeout — which looks exactly like a slow cluster rather than a broken one.

`provision.py` writes this file, `add-node` writes it again so the join is self-sufficient
against nodes built by an older toolkit, and `deploy_updates.py` repairs any node whose
copy does not name it. Vali warns on startup if it comes up without an address, and logs
whenever it takes or gives up the worker role.

### G. Node Decommission (`cluster decommission`)
Preflight and plan the permanent removal of a node from the ring. Prints the ordered
sequence and refuses when the destructive step would be unsafe. It never runs
`nodetool decommission` or `nodetool removenode` itself — those stream data, run
unbounded, and cannot be undone.
```bash
# Check and print the sequence
cluster decommission --node 10.10.102.223

# After the node is genuinely out of the ring: rewrites cluster.json on the
# survivors, deletes the hydra.nodes row, and shrinks the ZooKeeper ensemble
cluster decommission --node 10.10.102.223 --finalize
```

The ensemble shrink is not bookkeeping and is not optional. Until it runs, every
survivor's `zookeeper.container` still lists the departed node and still counts it
towards quorum — so a three-entry ensemble with two live members needs *both* of them,
leaving the cluster less fault-tolerant than the two-node cluster it has actually become.

Where there is a live observer to take the departing node's vote, `--finalize` hands it
over deliberately: one `reconfig`, no restart, and no instant at which the members
disagree about who votes. Where there is not — which on a three-node cluster is always,
because every node already votes — it falls back to rewriting the units and restarting
the survivors one at a time, so a quorum of the previous ensemble stays alive throughout.
That fallback is not a lesser path; removing a node from three genuinely does leave a
two-voter ensemble, and no reconfiguration can make that untrue.

Surviving members keep the ZooKeeper ids they already hold, and a removal simply leaves a
gap in the numbering. A member's id is its identity — it must match the `server.<id>`
entry every other member holds for it, and the image writes it into the data directory as
`myid` — so renumbering the members after the departed one would hand each of them an
identity that no longer matches their own data. Ids are read back from the units rather
than assumed; a node whose unit cannot be read is never given a guessed one, because
inventing an id for a node that already has one is how two members come to claim the same
identity. When that happens the ensemble is left alone and the manual steps are printed.

### H. Moving the ZooKeeper vote (`cluster zk-promote` / `zk-demote`)
Change which nodes form the consensus quorum, without changing which nodes are in the
cluster. The first three nodes provisioned are the voters and everything above them is an
observer; these are how that set is changed on purpose rather than as a side effect of a
removal.
```bash
# 10.10.102.43 takes over voting from a node that is never coming back
cluster zk-promote --node 10.10.102.43 --replacing 10.10.102.223
```
`--replacing` makes it one atomic reconfiguration rather than two steps with a weakened
ensemble in between, and on a three-voter ensemble it is the only legal form. The replaced
node stays in the ensemble as an observer and stays in the ring — moving the ZooKeeper
role off a node is not the same operation as removing the node.

Any change that would leave a voter which is not answering, or fewer than three voters, or
that is asked for while the ensemble has no single leader, is refused rather than
performed. The reasoning, the refusal table and the `zoo.cfg.dynamic` interaction that
makes the Quadlet the durable record of membership are in
[zookeeper.md](./zookeeper.md#changing-which-nodes-vote).

### I. Node Rejoin (`cluster rejoin`)
Preflight and plan bringing a node back. Checks that a previously decommissioned node has
had its ScyllaDB data wiped — rejoining with it resurrects rows deleted while the node was
away — and restores its cluster metadata.
```bash
cluster rejoin --node 10.10.102.223
cluster rejoin --node 10.10.102.223 --finalize
```

Both sequences, and the quorum gate that governs maintenance mode, are documented in
[ring_lifecycle.md](./ring_lifecycle.md).

### J. Cluster Destruction (`cluster destroy`)
Wipe all databases, clear claimed disks, remove configuration parameters, and reset the hypervisor hosts to factory default.
```bash
# WARNING: Wipes all VM disks, metadata tables, and system configurations permanently
cluster destroy          # asks you to type the word `destroy` first
cluster destroy --yes    # skips the prompt, for a script that really means it
```

It asks because there is nothing to undo afterwards: every VM is stopped and undefined, the
LVM pool and disk signatures are wiped, every mount under `/var/lib/hci/sidon` is taken off
(deepest first) and `/etc/hci/sidon-disks` is removed, the ZooKeeper and Hydra data and the sidon
extent store are deleted, and `/etc/hci/cluster.json` is removed -- which is why `cluster create`
needs `-s` again afterwards. The prompt wants the word `destroy` rather than `y`, because
`y` is what a finger types when it is expecting a different question.

Run without a terminal and without `--yes` -- a pipe, a cron job, a closed stdin -- it
**refuses**. Nobody answering is never read as consent. Declining, or Ctrl+C at the prompt,
exits non-zero before the cluster lock is taken and before any phase has started.

---

## 3. High Availability (HA) Failover Logic

### A. Virtual IP (VIP) Failover via Bifrost
* The cluster utilizes a floating Virtual IP (VIP) managed by the **Bifrost** daemon.
* Bifrost stands for the `bifrost-vip` candidacy while this node is serving the ingress port, and the node holding it binds the VIP interface locally. See [service_leadership.md](./service_leadership.md).
* If the active leader goes offline, ZooKeeper consensus automatically triggers a new leader election. Bifrost on the newly elected leader host immediately claims the VIP using Gratuitous ARP (GARP) broadcasts, redirecting Spectrum Web Console traffic without manual intervention.

### B. VM High Availability (HA) Failover via Mipha
* **Active HA Orchestration**: High Availability is managed dynamically by the **Mipha** daemon. Mipha uses ZooKeeper to elect an active coordinator leader that monitors the cluster.
* **Host Crash Detection**: The active Mipha leader polls all cluster nodes every 10 seconds using both network pings (ICMP) and the Spark mTLS API (`9099`). If a host is unreachable on both paths for 3 consecutive polls (30 seconds), it is marked as `DOWN` in ScyllaDB.
* **Automatic Failover & Restart**: Mipha queries ScyllaDB for all virtual machines registered to the failed node, resets their database state, and submits automatic start tasks to the Catalyst task queue.
* **Optimal Scheduling**: The **Vali** scheduler picks up the tasks and immediately schedules the VMs to boot on the healthiest remaining hosts based on available RAM and DRS rules, restoring VM availability automatically.

### C. Maintenance Mode and Quorum
Entering maintenance mode stops the host's `hydra-db` along with everything else, so it is
refused when the remaining ScyllaDB replicas could not form a quorum without it — derived
from the keyspace's actual replication factor and the actual ring, not assumed. Only one
host may transition at a time, enforced by a single-row lock in `hydra.cluster_locks` with
a holder token and a TTL rather than by a scan of node rows. A single-node cluster can
never enter maintenance mode; `cluster stop` is the operation for quiescing one. See
[ring_lifecycle.md](./ring_lifecycle.md).

---

## 4. Cluster Security & Trust Seeding

To guarantee passwordless SSH, secure inter-node KVM live migration, and encrypted mTLS command orchestration, the cluster configures and seeds security keys and certificates during bootstrapping.

### A. SSH Key Seeding and Keyscan Automation
During `cluster create` (orchestrated by `/usr/local/bin/provision.py`):
1. **Public Key Gathering**: Node 1 executes `ssh-keyscan` across all nodes (including their IP addresses and hostname formats like `Valkyrie-XXXXXX`) to capture host keys securely:
   ```bash
   ssh-keyscan -H -t rsa,ecdsa,ed25519 10.10.102.120 10.10.102.121 10.10.102.122 Valkyrie-51C2B5 Valkyrie-232EB8 Valkyrie-DB225F >> /root/.ssh/known_hosts
   ```
2. **Distribution**: These gathered keys are written to `/root/.ssh/known_hosts` on all cluster nodes. This prevents live migrations from failing due to SSH host key verification warnings when libvirt executes:
   ```bash
   virsh migrate --live ... qemu+ssh://root@<node_ip>/system
   ```

### B. mTLS Certificate Seeding & Locations
The provisioning engine generates and distributes TLS certificates signed by a custom cluster CA to enforce strict mTLS validation on port 9099.

Seeding paths:
* **Client mTLS Scope** (CLIs/tools):
  * `/root/.certs/ca.crt`: Custom cluster CA certificate
  * `/root/.certs/client.crt`: Client certificate for `valcli`/`mcli`
  * `/root/.certs/client.key`: Client private key (permission `600`)
* **Spark Daemon Scope** (Host Agent listener):
  * `/etc/hci/spark/certs/ca.crt`: Custom cluster CA certificate
  * `/etc/hci/spark/certs/node.crt`: Host agent node certificate
  * `/etc/hci/spark/certs/node.key`: Host agent private key (permission `600`)
* **Spectrum Ingress Scope** (Web interface / Traefik SSL):
  * `/etc/hci/spectrum/certs/server.crt`: Ingress SSL certificate
  * `/etc/hci/spectrum/certs/server.key`: Ingress SSL private key (permission `600`)

### C. Manual Trust Synchronization Commands
If a host key changes or a certificate needs manual synchronization, administrators can run:
```bash
# Scan and update keys for a host
ssh-keyscan -H -t rsa,ecdsa,ed25519 <node_ip> >> /root/.ssh/known_hosts
```


---

## Technical Reference

For the internal code structure, class/function details, and execution flowcharts, see the [Technical Guide](./cluster_technical.md).
