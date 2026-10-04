# Rauru (Snapshot and Data-Protection Manager)

Rauru is the daemon that owns snapshots and, later, the rest of data protection. Its Nutanix
counterpart is **Cerebro**, the snapshot, protection-domain and replication service.

> [!NOTE]
> **Name Origin:** Rauru is the first sage of Hyrule in the Zelda series, the one who founded
> the kingdom and keeps it intact across time. Like every daemon here, the name is the owner's.

## What it does today, and what it is meant to become

**Today it does one thing:** it runs the snapshot policy. On its interval
(`helios_snapshots.RUN_INTERVAL_SECONDS`, one hour) it takes the snapshots the policy in
`hydra.dfs_snapshot_policies` says are due and prunes the ones retention says are no longer kept,
by calling `helios_snapshots.Runner.run()`. The policy, the retention rules and how a run never
prunes what something depends on are in [dfs/snapshots.md](./dfs/snapshots.md). That work used to
be a Dagur job; Rauru took it over and the job is gone.

**It is meant to grow** into replication and disaster recovery: protection domains, replicating
snapshots to another cluster, and recovering a workload from them. That is designed in
[dfs/replication.md](./dfs/replication.md) and **none of it runs**. What runs is the daemon, the
election it works under and the one job. Beside it, built and tested in isolation but called by
nothing (so no new unit, and nothing new in the service checklist): the replication planner
(`rauru_replication.py`), the failover and failback decision logic (`rauru_failover.py`), and in
Sidon the pinned-key verifier and the Hydra map sink for a replicated snapshot. The second
cluster these would talk to does not exist, which is why no driver was written.

## Architecture and lifecycle

- **A native systemd unit** running `/usr/local/bin/rauru` (never `rauru.py`), written by
  `provision.py` and by the rollout, and declared in `MANAGED_SERVICES` with `requires: daruk`.
  Everything it reads or writes in Hydra goes through Daruk, so it is not started before Daruk
  answers. It does not require Sidon: it reaches storage through spark-daemon's API on whichever
  node owns a vdisk, and says so when that fails.
- **It starts correctly before there is a cluster.** `systemctl enable rauru` runs it at boot,
  which is before ZooKeeper, Hydra and Daruk exist, and on a node that never joins a cluster they
  never will. It therefore never exits because a dependency is missing. It waits, with exponential
  backoff (5 s doubling to 5 min, jittered), and says what it is waiting for once per distinct
  reason rather than every pass. A daemon that crashed instead would be restarted by systemd
  every few seconds forever, which looks like a running service in every listing.
- **The work is behind a per-service election.** Rauru stands for `rauru-snapshots`
  (`helios_zk.SERVICE_RAURU_SNAPSHOTS`) and only the node holding the lowest ballot runs the
  policy. It does not compare an address to the ZooKeeper ensemble leader; see
  [service_leadership.md](./service_leadership.md). A node that cannot establish that it leads
  does nothing, and does not even probe Hydra. Leadership moving causes at most one extra run,
  which is harmless: whether a snapshot is due is decided from the age of the newest snapshot, not
  from when the last run happened.
- **Its tasks carry component `Rauru`.** Each snapshot taken, each snapshot pruned and each
  rollback is a row in `hydra.catalyst_tasks`, with `component = 'Rauru'`, `service = 'rauru'` and
  the per-component sequence number. They have no parent task (the Dagur task that used to be
  their parent no longer exists), so they appear as top-level Rauru tasks in the console.
- **`valcli storage.snapshot-run`** remains as the manual command and runs the same code, so an
  operator can ask for a pass (`--dry-run` to see what it would do) without waiting for the hour.
  A manual run beside Rauru's is a supported accident: snapshot names have minute resolution, so
  the second is refused by Sidon as already existing, and a double prune deletes an absent row.
- **It does not run in maintenance.** The unit carries
  `ConditionPathExists=!/etc/hci/maintenance.state`, like the other daemons that do cluster work,
  and `cluster stop` stops it through the declared table.

## Checking that it would start

```bash
/usr/local/bin/rauru --check
```

Imports every module Rauru uses, validates its configuration (the election name, that tasks are
written as `Rauru`, that the policy tables' migrations exist) and exits 0 or 1. It opens no
sockets, so it works on a node with no cluster, and it names the module that is missing rather
than printing a traceback. `systemd-analyze verify /etc/systemd/system/rauru.service` checks the
unit.

After `cluster create`:

```bash
systemctl is-enabled rauru && systemctl status rauru --no-pager
journalctl -u rauru --no-pager | tail -20
# who holds the election (one ballot per node that stands, the lowest leads)
podman exec -it systemd-zookeeper bin/zkCli.sh -server 127.0.0.1:2181 ls /helios/leaders/rauru-snapshots
cluster status                       # Rauru should be UP on every node
valcli storage.snapshot-policy       # an enabled policy is what makes a run do anything
valcli storage.snapshot-run --dry-run
```

With no policy enabled a run reads one table, says so and finishes; that is the expected first
log line on a new cluster. A run that took or pruned something leaves `Rauru` rows in the console's
task list.

## Where it is registered

A service is written down in a dozen files and the unit text exists twice. `test_service_wiring.py`
reads `MANAGED_SERVICES` and fails if any service is missing from any registry it applies to, so
Rauru was added by following that test, not a list. See [deployment.md](./deployment.md).

## Moved from Dagur

The console's bootstrap used to seed a Dagur job named `snapshot_policy`. It no longer does, and
on every start it deletes that row if it is still the seeded one
(`DELETE ... IF command = '/usr/local/bin/valcli storage.snapshot-run'`), so a cluster that already
had the job does not run the policy twice. A job an operator repurposed under that name is left
alone. During a rolling upgrade a node still on the old console can re-seed the row; the next
start of an upgraded console removes it again.
