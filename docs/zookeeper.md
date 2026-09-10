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

### Changing which nodes vote

The first three nodes provisioned are the ensemble's voters and everything above node
three is an observer. That part is right — observers scale reads without joining the
write quorum, so a five-node cluster still needs two failures to lose consensus rather
than three. What was missing was any way to change the set on purpose: lose two of those
three and cluster coordination stops with every other node healthy and idle.

```bash
# Move the ZooKeeper role off a node that is never coming back. One reconfiguration:
# 10.10.102.43 starts voting and 10.10.102.223 stops, and no member ever holds a
# different view of who votes.
cluster zk-promote --node 10.10.102.43 --replacing 10.10.102.223

# The same sentence from the other end.
cluster zk-demote --node 10.10.102.223 --replacing 10.10.102.43
```

`--replacing` is not decoration. On a three-voter ensemble it is the *only* legal change:
promoting without it makes four voters of which one is the node being replaced, and
demoting without it makes two.

The demoted node stays in the ensemble as an observer. It is not removed from the ring,
it is not removed from `cluster.json`, and it serves reads again the moment it comes back
— **moving the role is not the same operation as removing the node**, and neither implies
the other.

#### What it refuses, and why that is the point

ZooKeeper will commit any membership for which a quorum of the old *and* of the new
configuration is available at that instant. That includes handing a vote to a node which
is not answering, and taking three voters down to one. Both leave a cluster the next
single failure finishes off, and both report success. The judgement is Helios's:

| Refused | Because |
| --- | --- |
| Any new voter that is not answering | A vote held by a node that is down is counted in every quorum and cast in none of them. Requiring *every* new voter to be live also gives the new configuration its quorum, which is the second half of what ZooKeeper needs to commit. |
| A result with fewer than three voters | An ensemble whose quorum is its entire membership tolerates no failure at all. Two voters need both; one is not an ensemble. |
| No single leader among the current voters | Zero means mid-election or no quorum; more than one means a partition. Either is the worst possible moment to change the membership out from under it. |
| Fewer than a quorum of the current voters answering | The configuration being left has to be able to commit the change. Checking first turns a hang into a sentence. |
| A member that is not in the ensemble | Promotion moves a vote between members that already exist. Adding or removing one is `cluster add-node` / `cluster decommission`. |

An **even** number of voters is a warning rather than a refusal: four voters tolerate the
same single failure three do and put a fourth node in every quorum, but refusing it
outright would make the swap above impossible to express.

Taken together: a quorum of the old configuration is live, *all* of the new configuration
is live, the change is one `reconfig` rather than a rewrite plus a rolling restart, and
nothing is restarted at all. There is no instant at which the ensemble is without a
quorum and none at which two members disagree about who votes.

The version is a guard too. The plan is made against the configuration version
`/zookeeper/config` reported, and submitted as `reconfig -v <version>`, so a membership
that moved between reading and writing is refused rather than overwritten. And the result
is not taken from the client's exit status: the membership is read back and compared with
what was asked for, because the ensemble agreeing is a stronger claim than a client
reporting success.

#### `zoo.cfg.dynamic`, and why the Quadlet is still the source of truth

This is the part that does not announce itself.

With `reconfigEnabled=true`, ZooKeeper splits its configuration on first boot: the
`server.N` lines move out of `zoo.cfg` into `zoo.cfg.dynamic.<version>`, `zoo.cfg` gains a
`dynamicConfigFile=` pointer, and the original is kept as `zoo.cfg.bak`. From then on the
ensemble owns its own membership and writes a new dynamic file on every committed
`reconfig`.

All of that happens in `/conf`, and `/conf` is in the container. The image declares
volumes for `/data`, `/datalog` and `/logs` and not for the configuration directory, and
its entrypoint regenerates `zoo.cfg` from `ZOO_SERVERS` **whenever the file is absent** —
which, for a container that is recreated on restart, is every start. So a reconfiguration
survives exactly as long as the container does.

The instinct is to persist the dynamic file. That does not work: ZooKeeper names it from
the path of the *static* config file (`QuorumPeer.makeDynamicConfigFilename`), so the first
committed reconfiguration writes it back beside `/conf/zoo.cfg` wherever `dynamicConfigFile`
originally pointed. A dynamic file on a volume would outlive the pointer that names it —
a second source of truth for membership, which is worse than none.

So the conclusion is the other way round, and it is deliberate:

> **ZooKeeper owns the membership while it is running. The Quadlet owns it across a
> restart.** The dynamic configuration is left ephemeral on purpose, because the static
> file goes with it and a restarted node therefore re-derives everything from its unit
> rather than reading a stale half of a pair.

Which means a provisioner that rewrites `zoo.cfg` does **not** fight ZooKeeper — it is the
thing that rebuilds what ZooKeeper was running. What would fight it is a unit that says
something different from the live ensemble, so the invariant every path maintains is:

* `cluster zk-promote` / `zk-demote` reconfigure the ensemble, then rewrite every member's
  unit from the membership they read back, and restart nothing;
* `cluster decommission --finalize` does the same when it can hand a vote on, and falls
  back to the rewrite-and-roll when it cannot;
* `cluster create` and `cluster add-node` rewrite the units and restart, which makes the
  units authoritative again — correct, because they were in agreement when it started;
* `deploy_updates.py` appends `ZOO_CFG_EXTRA` to the `Environment=` line and touches
  nothing else on it. `ZOO_MY_ID` is the node's identity and `ZOO_SERVERS` is the
  ensemble, and a rollout running on every node at once has no business with either.

Asking for the role a member already has is therefore a repair rather than a no-op: the
ensemble needs nothing and the units are rewritten to match it. That is how a node that
was down during an earlier change is brought into line when it comes back — and it needs
to be, because until then its unit describes the membership it left, and starting
ZooKeeper there brings up a member that believes it still holds the role it was relieved
of. (It would learn better from the leader, which carries a higher configuration version;
the point is not to depend on that.)

#### Enabling it

`reconfigEnabled` reaches a node in the `Environment=` line, as
`ZOO_CFG_EXTRA=reconfigEnabled=true` — the image appends every whitespace-separated entry
of that variable to the generated `zoo.cfg` verbatim. All three unit writers set it
(`cluster_new.py`, `provision.py`, `spark_daemon_decoded.py`) and `deploy_updates.py`
adds it to a node that already exists.

It has to be the same on every member. The setting is read by whichever server becomes
leader, so a half-enabled ensemble answers differently depending on an election. The
commands check the **running** server's `/conf/zoo.cfg` rather than the unit for exactly
this reason: after a rollout the unit has the setting and the container does not, because
the rollout deliberately does not restart the consensus layer.

`standaloneEnabled` is left at `true`. With three or more participants it has no effect —
ZooKeeper is in replicated mode either way — and the only thing setting it to `false`
would buy is the ability to reconfigure below two voters, which `zk-demote` refuses on
purpose. It would also change how a single-node install boots, for nothing.

One thing worth being explicit about: enabling reconfiguration widens what an
unauthenticated client on port 2181 can do. ZooKeeper 3.9 performs no ACL check on the
`reconfig` operation itself, so anything that can reach the client port can change the
ensemble. That port already accepts unauthenticated writes and has `4lw.commands.whitelist=*`,
so this is a widening of an existing exposure rather than a new one — but it is one more
reason the 2181 boundary is a network boundary and has to stay one.

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
