# Host maintenance

Maintenance mode empties a host of guests so it can be worked on, without taking it out of the
cluster's metadata and storage planes. This document is the whole flow: what a host in maintenance
runs and why, every step in order, what each failure leaves behind and how it recovers, and what
happens if the host reboots meanwhile. The code is `vali.py` (`handle_maintenance_request`, the
`host_maintenance_enter` / `host_maintenance_leave` tasks, `finish_maintenance_exit`),
`spark_daemon_decoded.py` (the `maintenance` column of `MANAGED_SERVICES`, autostart and watchdog) and
`mipha.py` (lock renewal); `test_maintenance_flow.py` runs every transition with the cluster stubbed.

## What a host in maintenance runs

| Unit | In maintenance | Why |
|---|---|---|
| `spark-daemon` | **up** | The agent that is told to leave. Not a managed service: the reconcile loop does not touch it. |
| `zookeeper` | **up** | A voter in the ensemble that holds the desired cluster state and every leader election. Stopping it shrinks the ensemble's margin for no benefit. |
| `hydra-db` | **up** | A replica of the metadata. The cluster's quorum margin is made of these. |
| `daruk` | **up** | The only way anything on the host, the leave sequence included, reaches the database. |
| `sidon` | **up** | Holds replicas of other hosts' vdisks. Stopping it degrades every vdisk with a copy here (writes need every replica) and gains nothing when the point is to empty the host of guests. The guests are gone, so no NBD socket here is in use. |
| `hylia` | **up** | The rolling upgrade's orchestrator. An upgrade is what puts a host in maintenance in the first place, so stopping it would stop the operation that asked for the maintenance. |
| `vali`, `catalyst`, `mipha`, `bifrost`, `dagur`, `mimir`, `rauru`, `logos`, `gatoway`, `urbosa` | stopped | Everything that places, schedules, restarts, serves the VIP for, collects for or acts on guests and the cluster. A host being drained must not take part in HA decisions, hold the VIP, or accept work. |
| `spectrum`, `spectrum-phx`, `slate`, `agahnim` | stopped | The console and its proxies. They are reachable through the other hosts. |

The rule is one declared column, `"maintenance": "keep"` on the rows of `MANAGED_SERVICES`
(hydra-db, daruk, sidon, hylia) plus the two units named in `MAINTENANCE_UNMANAGED_KEPT`
(zookeeper, spark-daemon). Everything else derives from it: spark's autostart stops
`maintenance_stopped_units()` and starts and watches `maintenance_watchdog_units()`; Vali's
`MAINTENANCE_STOP_UNITS` is held equal to the complement by a test; `cluster status` prints
`(kept up in maintenance)` for a unit the node says is kept and `(expected to be stopped in
maintenance)` for any other unit that is up on a host in maintenance, which is how a stop that did not
finish shows itself.

Before this was one rule, Vali stopped everything but ZooKeeper, spark's boot path stopped a
different list and started the database, and its watchdog restarted the database and Sidon that Vali
had just stopped. The mixture an operator saw (ZooKeeper, HydraDB, Daruk, Spark and Hylia up, Sidon
down) was an accident of those lists.

**Why the quorum gate still exists** although the database stays up: maintenance is the state in
which a host is rebooted, upgraded and has its services restarted, and a rolling upgrade restarts
`hydra-db` on it. The gate asks whether the cluster could lose this host's replica *now*, so that
entering maintenance is never what makes a later restart fatal. See [ring_lifecycle.md](./ring_lifecycle.md).

## Entering

```
POST /api/v1/hosts/maintenance {hostname, action: "enter", force_stop}
 1. read hydra.nodes for the host (404 if unknown; 409 if it has no address)
 2. QUORUM GATE   check_stop_preserves_quorum        -> 409 reason "quorum"
 3. LOCK          hydra.cluster_locks (TTL 5 min)    -> 409 reason "locked", naming the holder
 4. CLAIM         LWT NORMAL -> ENTERING_MAINTENANCE -> 409 (lock released) if not NORMAL
 5. submit Catalyst task host_maintenance_enter      -> on failure: back to NORMAL, lock released

host_maintenance_enter (Vali's queue worker)
 6. EVACUATE      every Running VM on the host: select_best_start_host, migrate; the lock is renewed
                  per VM. No target or a failed migration: force_stop stops the VM, otherwise the
                  enter FAILS naming the VM and the host goes back to NORMAL, lock released
 7. QUORUM RE-CHECK the evacuation can take an hour; a replica may have died meanwhile -> NORMAL, lock
                  released, nothing stopped
 8. MARK          hydra.nodes IN_MAINTENANCE, lock renewed (Mipha renews it from now on)
 9. MARKER        /etc/hci/maintenance.state on the host. If it cannot be written -> NORMAL, lock
                  released, nothing stopped
10. STOP          the non-kept units, detached (Vali is one of them and cannot wait for its own
                  stop). If spark refuses, the task FAILS and names `valcli host.maintenance.leave`
```

Guests that migrate use the storage handover described in [vali.md](./vali.md) and
[dfs/ownership.md](./dfs/ownership.md) §5.

**Failure at each step** (the property to hold: a failed enter leaves the host NORMAL, the lock free
and nothing stopped, except the last row):

| Fails at | Host left | Lock | Recovery |
|---|---|---|---|
| 1 to 4 | unchanged | free | none needed; the 409 says why |
| 5 | NORMAL | released | retry |
| 6 | NORMAL (some VMs may already have moved; DRS rebalances) | released | fix the cause or use `force_stop`, retry |
| 7 | NORMAL | released | retry when the ring is whole |
| 9 | NORMAL | released | retry |
| 10 | IN_MAINTENANCE with some services still up | held | `leave`, or stop the units listed by `cluster status` |
| the task itself dies (Vali restart, Catalyst loss) | ENTERING_MAINTENANCE | held, renewed by Mipha | `valcli host.maintenance.leave <host>`: ENTERING is a leavable state precisely for this |

## Leaving

```
POST /api/v1/hosts/maintenance {hostname, action: "leave"}
 1. the host must be IN_MAINTENANCE, ENTERING_MAINTENANCE or RECOVERING -> otherwise 409 "not_in_maintenance"
    (it used to run on a NORMAL host: restart its services and set it RECOVERING)

host_maintenance_leave
 2. remove /etc/hci/maintenance.state     (spark's reconcile loop now acts on the desired state again)
 3. start ZooKeeper, the kept units, then everything else (idempotent; a failure is logged)
 4. wait 10 s, set the row RECOVERING     (not schedulable yet)
 5. FINISH        poll the host's status for up to 180 s until every service is UP, then
                  LWT RECOVERING -> NORMAL (a status somebody changed meanwhile, such as FENCED, wins),
                  then release the lock
 6. a Mimir health check and a DRS rebalance as subtasks
```

Step 5 did not exist: nothing took the host from RECOVERING to NORMAL (the lock was released and the row
stayed RECOVERING), so a placed VM never came back to the host and a rolling upgrade waiting for NORMAL
timed out. If the services do not all come up the task **fails naming them**, the host stays RECOVERING,
the lock stays held (Mipha renews it for as long as the row says RECOVERING), and running leave again
retries from step 2: every step is idempotent. If the host is abandoned the lock's TTL frees it.

## The lock

`hydra.cluster_locks`, name `cluster-maintenance`, a holder (the hostname) and a holder token, TTL 300 s.
One host transitions at a time cluster-wide; the claim is one Paxos round, not a scan of node rows. It is
renewed by the evacuation task (per VM) and by Mipha's leader loop for as long as the host's row is
ENTERING_MAINTENANCE, IN_MAINTENANCE or RECOVERING, so the TTL can stay short. A leave that arrives hours
later reads the row and releases on the token it read. See [ring_lifecycle.md](./ring_lifecycle.md).

## If the host reboots in maintenance

`/etc/hci/maintenance.state` survives the reboot, and spark's autostart checks it before anything
else: it stops `maintenance_stopped_units()`, starts ZooKeeper and the kept units, and runs the
maintenance watchdog (every 30 s it restarts a kept unit that is not active). The reconcile loop
ignores the desired cluster state while the marker exists, Mipha exempts the host from failure
detection, and the row still says IN_MAINTENANCE, so nothing is scheduled onto it. `leave` brings it back.

## Reading the state

* `valcli host.list` / the console show the row status.
* `cluster status` shows `[IN MAINTENANCE]` on the host and labels each unit that is up there.
* `hydra.cluster_locks` shows who holds the lock, and `valcli`'s error on a second enter names the holder.

## What was decided here

The rule above is the author's call (the owner asked for Spark, Hylia, Sidon, Mipha and Logos to be
decided): Sidon and Hylia are kept, Mipha and Logos are stopped. It is one column to change if it should
differ. Decision record: TODO.md "Maintenance flow".
