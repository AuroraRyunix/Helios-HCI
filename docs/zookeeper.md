# Zookeeper (Distributed Configuration & Consensus Store)

Zookeeper provides highly reliable distributed coordination and consensus. It is used directly as **Zookeeper** in the Nutanix architecture.

> [!NOTE]
> **Name Origin:** In our stack, Zookeeper serves as the consensus engine for the **Odin** service wrapper. Just as Odin oversees the Norse gods from Asgard and maintains active consensus, Zookeeper coordinates active cluster leader elections and central state configuration records.

## Nutanix Role (Zookeeper)
In Nutanix, Zookeeper stores critical configuration state for the cluster, including node mappings, IP addresses, configured storage containers, and cluster topology. It runs on a subset of nodes (usually 3 or 5) to ensure high availability and uses Paxos-like consensus to resolve cluster state changes.

## Containerized HCI Approach
In our 3-node cluster, we run a **3-node ZooKeeper ensemble** using official ZooKeeper images in Podman containers across the hosts (`10.10.102.220`, `222`, `223`).
1. **Host Networking Mode**: To avoid overlay network overhead and complex container DNS resolution, Zookeeper containers run in `network=host` mode.
2. **Persistent Storage**: Zookeeper transactions and snapshots are written to host directories mounted into the container.
3. **Cluster Config**: Configured using standard environment variables or files mapped to the Zookeeper directory.

---

## Deployment & Configuration

### Ports Used (Host Network)
* `2181`: Client connections (used by Odin).
* `2888`: Follower connections to the Leader.
* `3888`: Leader election port.

### Directory Configuration on Host
* **Data Path**: `/var/lib/hci/zookeeper/data/`
* **Log Path**: `/var/lib/hci/zookeeper/log/`
* **Node ID File**: `/etc/hci/zookeeper/myid` (Contains a single integer: `1`, `2`, or `3`)

### Sample Podman Command (Run by Spark/Systemd)
```bash
podman run -d \
  --name zookeeper \
  --net=host \
  --restart=always \
  -v /var/lib/hci/zookeeper/data:/data:Z \
  -v /var/lib/hci/zookeeper/log:/datalog:Z \
  -e ZOO_MY_ID=1 \
  -e ZOO_SERVERS="server.1=10.10.102.220:2888:3888;2181 server.2=10.10.102.222:2888:3888;2181 server.3=10.10.102.223:2888:3888;2181" \
  zookeeper:3.9.2
```

*(Note: The `:Z` flag on volume mounts ensures correct SELinux context labeling on EL 10.2).*

---

## Technical Coordination & ZNode Registry

The cluster coordinators (Vali, Mipha, Bifrost) utilize the ZooKeeper ensemble for active leader elections and state lock coordination:
* **Vali Leader Election**: Uses ephemeral sequential znodes at `/vali/leader/lock-`. The node holding the lowest sequence number is elected as the active scheduler leader.
* **Mipha Coordinator**: Elects an active HA coordinator at `/mipha/leader/lock-` to monitor host heartbeats.
* **Bifrost VIP Floating**: Monitors `/vali/leader` to bind the floating Virtual IP address locally to the ZooKeeper leader node.
* **Cluster State**: Store the global cluster operational state at `/cluster_state` (can contain `started` or `stopped`).

---

## Command Examples & Verification

### A. Querying Ensemble Status (Four-Letter Words)
ZooKeeper supports simple network commands using four-letter words. You can query status and membership via netcat:
```bash
# Query server statistics, latency, and active mode (leader vs. follower)
echo stat | nc 127.0.0.1 2181

# Check client connections and active sessions
echo cons | nc 127.0.0.1 2181

# Verify server health state (should return 'imok')
echo ruok | nc 127.0.0.1 2181
```

### Who asks which node is the leader, and how often

Nine daemons need to know which node leads the ensemble: `vali`, `catalyst`, `mimir`,
`dagur`, `valcli`, `mipha`, `bifrost`, `hylia` and `spectrum_server`. Each used to carry
its own copy of the same `stat` probe loop and run it on its own timer -- vali's queue
worker every two seconds, and every call into Catalyst again on top.

That summed to roughly **eleven journal lines a second on a completely idle cluster**, and
it had been doing it since the cluster was built: 151 MB of journal, and enough journald
work to be the second-largest CPU consumer on two of three nodes. Nothing was wrong with
any individual copy. There were just nine of them, none cached.

Two things changed, because either alone leaves the cost in place:

**The probe is shared and cached.** `helios_zk.leader_ip(ips)` is the one implementation,
cached for `LEADER_CACHE_SECONDS` (5s). Leadership changes only at an election, so a
caller acting on a five-second-old answer is in the same position as one whose probe raced
an election -- which every caller already tolerates. `None` is cached too: a cluster
mid-election is the worst moment to add probe load to.

`helios_zk.server_mode(ip)` is the uncached form, for asking about one *named* server
rather than finding the leader. `mipha.is_zookeeper_leader(ip)` uses it.

The fallbacks stayed where they were. What each daemon does when the leader is missing or
not serving differs deliberately -- `bifrost` refuses to elect a replacement independently,
because in a partition both sides would choose the lowest candidate they can see and both
would bind the VIP -- and consolidating that would be consolidating reasoning, not code.

**ZooKeeper no longer logs the probes.** Caching lowers the rate but cannot reach zero:
each daemon *process* holds its own cache and there are a couple of dozen across a cluster,
so the floor is set by how many processes exist. The probes were never the expense -- a TCP
connect and a nine-byte write -- but two INFO lines about each one is. `zookeeper_config/logback.xml`
sets `org.apache.zookeeper.server.NIOServerCnxn` and `.server.command` to `WARN` and leaves
everything else at `INFO`, because elections, quorum changes, session commits and connection
errors are what this log is for. It is vendored from the pinned 3.9.2 image and mounted over
the container's own; both are pinned in the same toolkit.

Each unit also carries `LogRateLimitIntervalSec=10s` / `LogRateLimitBurst=100` -- a ceiling
for whatever the next mistake turns out to be, generous enough for a real election's burst.

`test_zk_probe_storm.py` holds all of it: no daemon may open its own connection to 2181,
the cache must be honoured, the config must ship, and every writer of the unit must mount
it and set the limit.

### Restarting the ensemble

Never all at once. `deploy_updates.py` reconciles `zookeeper.container` and reloads systemd
but deliberately **does not** restart ZooKeeper, because the rollout runs against every node
in parallel and restarting the consensus layer everywhere simultaneously is how a rollout
takes quorum away. Restart followers first and the leader last -- restarting the leader
forces an election, and doing the followers afterwards would force a second -- waiting for
each node to report a mode again before touching the next:

```bash
# On any node, one at a time, checking in between:
systemctl restart zookeeper
(echo stat; sleep 0.3) | nc 127.0.0.1 2181 | grep '^Mode:'
```

`cluster add-node` does exactly this when it rewrites the ensemble.

### B. Interactive ZooKeeper Shell (`zkCli.sh`)
Use the interactive client tool inside the container to inspect znode trees and cluster states:
```bash
# Start the interactive ZK CLI session
podman exec -it systemd-zookeeper zkCli.sh -server 127.0.0.1:2181

# ZK Shell Command Examples:
# 1. List root-level znode paths
ls /

# 2. Query cluster state
get /cluster_state

# 3. View active Vali leader candidates
ls /vali/leader
```
