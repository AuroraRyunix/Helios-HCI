# Host maintenance

Maintenance mode empties a host of guests and fully evacuates its storage and service roles so physical hardware (SSDs, memory, boards) can be safely serviced or the host rebooted, without risking cluster corruption or uncoordinated operations. This document describes the whole flow: what a host in maintenance runs and why, every step in order, what each failure leaves behind and how it recovers, and what happens if the host reboots meanwhile. The code is `vali.py` (`handle_maintenance_request`, the `host_maintenance_enter` / `host_maintenance_leave` tasks, `finish_maintenance_exit`), `spark_daemon_decoded.py` (the `MANAGED_SERVICES` inventory, autostart and watchdog), `valcli.py` (`wait_for_catalyst_task`), and `mipha.py` (lock renewal); `test_maintenance_flow.py` runs every transition with the cluster stubbed.

## What a host in maintenance runs

In hyperconverged infrastructure (such as Nutanix AOS), node maintenance means the physical node is taken down or serviced. Hence, all cluster services, database replicas, and storage engines are fully stopped and evacuated from the node:

| Unit | In maintenance | Why |
|---|---|---|
| `spark-daemon` | **up** | The local host agent that executes node lifecycle tasks and receives the leave instruction. |
| `hydra-db`, `daruk` | stopped | Drained and stopped. ScyllaDB on this node does not accept queries during physical maintenance. |
| `sidon` | stopped | Drained and stopped. Storage journals are flushed and extents sealed prior to stopping. |
| `zookeeper` | stopped | Local ZooKeeper container is stopped; consensus is sustained by remaining ensemble quorum. |
| `vali`, `catalyst`, `mipha`, `bifrost`, `dagur`, `mimir`, `rauru`, `logos`, `gatoway`, `urbosa`, `hylia` | stopped | All scheduling, HA, VIP, task management, and monitoring roles on this host are silenced. |
| `spectrum`, `spectrum-phx`, `slate`, `agahnim` | stopped | Console and HTTP reverse proxies are stopped; access is served via healthy peer nodes. |

**The Quorum Gate:**
Before a host enters maintenance and stops `hydra-db` / `sidon`, Vali checks quorum (`check_stop_preserves_quorum`). The cluster must retain sufficient replicas to survive the loss of the host. If quorum cannot be maintained, maintenance mode entry is refused.

Upon leaving maintenance (`valcli host.maintenance.leave <host>`), the host agent removes the `/etc/hci/maintenance.state` marker, bootstraps core consensus/storage services (`zookeeper` -> `hydra-db` -> `daruk` -> `sidon`), brings up the remaining cluster services, verifies that every service is `UP` via `services_down_on`, transitions the node status to `NORMAL`, releases the cluster maintenance lock, and triggers background DRS rebalancing and health checks.

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
