# Which node runs a leader-only job

Several things in this cluster must happen **once**: one node drains Catalyst's `vali` queue,
one node submits the scheduled Dagur jobs, one node monitors the others for HA, one node holds
the VIP. Deciding which node that is used to be one line, repeated in eight daemons:

```python
def is_zookeeper_leader():
    return get_zookeeper_leader_ip() == LOCAL_IP
```

It is one question with three things wrong with it, and they are the same thing.

**It is the wrong question.** The ZooKeeper ensemble elects a leader for the ensemble's own
reasons: a member restarts, a link blips, a rolling upgrade restarts ZooKeeper on every node in
turn. None of those events has anything to do with which node should be running DRS. The
ensemble re-elected twice in one afternoon recently and relocated every leader-only workload in
the cluster each time.

**It funnels everything onto one node.** Because every daemon asked the same question, every
daemon got the same answer. One node ran the queue worker, the scheduler, the HA monitor, the
upgrade driver and the health checks, and held the VIP — so the busiest node in the cluster was
also the only one doing coordination work, and the only one whose failure moved all of it at
once.

**It is a string comparison against a cached probe.** `leader_ip` caches for five seconds, by
design, because the uncached version had ZooKeeper answering eleven `stat` probes a second
forever (see [zookeeper.md](./zookeeper.md)). So the answer was true or false *some seconds
after the fact*, and two nodes could believe it simultaneously — which is how two schedulers
submitting one backup became a real failure mode rather than a theoretical one.

---

## What replaces it

The standard ZooKeeper election recipe, one election per job, in `helios_zk`:

```
/helios/leaders/<service>/          persistent parent, one per job
/helios/leaders/<service>/n_0000000003    ephemeral sequential ballot, one per candidate
```

Each candidate creates one **ephemeral sequential** child and writes its own address into it.
The lowest counter leads. Nobody announces anything and nobody times anyone out: the ballot is
bound to the session, so a process that dies, hangs past its session timeout, or is partitioned
away stops leading because its ballot is *gone*, not because somebody noticed.

A follower finds the leader by **reading** the winning ballot's contents, which is how a
submitter resolves "where are Catalyst's queues" without probing anything.

`helios_zk.Election` is the recipe. `helios_zk.Candidacy` is what a long-lived daemon wraps
around it, and it exists because of the failure the recipe alone does not cover: a daemon runs
for months and its session does not. Three rules make it safe to call from a two-second loop.

* **False means "I could not establish that I lead."** Never an exception, and never True on a
  guess. The thing on the other side of the answer is a queue being drained, and two drainers
  is worse than none.
* **A lost ballot is reported as not-leading *before* a new one is created.** A stale candidate
  must never act in the window where it has stopped leading. Standing again puts it at the back
  of the queue, which is where a returning candidate belongs.
* **A reconnect takes a new session rather than resuming the old one.** Resuming would bring
  the old ballot back and leave one process holding two.

A failed connect is retried on a timer rather than on every pass: a down ensemble is exactly
the moment every daemon in the cluster is looping, and the last thing it needs is nine
processes opening sockets as fast as their loops allow.

---

## The services

One name per *job*, not per daemon. Two leader-only loops in one process take two candidacies,
because funnelling them through one name rebuilds the coupling this replaces.

| Service | Who stands | What it decides |
| :--- | :--- | :--- |
| `catalyst-dispatch` | every `catalyst` | Which node's in-memory queues are *the* queues. Everyone else reads this one's published address to find out where to submit and where to poll. |
| `catalyst-scheduler` | every `catalyst` | Which node evaluates `hydra.dagur_schedules` and submits the due jobs. |
| `vali-queue` | every `vali` | Which node drains Catalyst's `vali` queue — VM power, migration, placement. |
| `vali-drs` | every `vali` | Which node runs the DRS rebalancing pass. |
| `dagur-queue` | every `dagur` | Which node drains the `dagur` queue and runs maintenance commands. |
| `lanayru-queue` | every Spectrum backend | Which node drains the `lanayru` queue (Kubernetes deploy/destroy). |
| `mimir-schedules` | every `mimir` | Which node triggers the health-check schedule. |
| `hylia-upgrades` | every `hylia` | Which node drives a rolling upgrade. |
| `mipha-ha` | every `mipha` | Which node watches the other hosts and orchestrates failover. |
| `bifrost-vip` | every `bifrost` | Which node binds the cluster VIP. |

The names are in `helios_zk` as constants, because the string is the contract between the
daemon that stands and anything reading `/helios/leaders` to find out who won — and a typo in
one copy is a second election nobody notices.

### Bifrost is the one that changed behaviour, not just mechanism

The VIP used to go to the ensemble leader *if* that node was also serving port 443, and
otherwise nowhere. Picking a replacement by sort order was deliberately refused, because that
is a second independent election that can disagree with the ensemble's and put the same address
on both sides of a partition. The cost was that one node's Traefik being down took the console
offline cluster-wide while two healthy nodes watched.

With a candidacy, local health is the **entry condition**: a node stands only while it is
serving 443 and both console backends, and withdraws when it stops. So the lowest ballot is by
construction a node that can serve clients, and there is exactly one of them because the
ensemble assigned the counters. The safe direction is unchanged — a candidacy that cannot
establish that it leads reports False, so an unreachable ensemble releases the VIP rather than
binding it on a guess.

---

## The handover window

Rolling this change out means a window in which some nodes decide by candidacy and some still
decide by address comparison, so two nodes can believe they are the queue worker at once. That
is worth being explicit about, because "two nodes drain one queue" is the failure this whole
area exists to prevent.

Nothing duplicates in that window, for three separate reasons, and none of them is timing:

* **A queue hands each task to exactly one poller.** The queues are `queue.Queue` objects
  inside one Catalyst process, and `get()` removes the task. Two workers polling the same
  Catalyst split the work; they do not both get the same task.
* **Only one Catalyst serves a queue at all.** `GET /api/v1/queues/<service>` answers `503` on
  a node that does not hold the `catalyst-dispatch` candidacy, rather than `204`. Answering
  "nothing queued" would be indistinguishable from an empty queue, and a worker polling the
  wrong node would wait forever while work piled up on the right one.
* **Every schedule tick is still claimed with a compare-and-swap.** A candidacy answers
  correctly at the instant it is read and the pass acts later, so an election is not and never
  was sufficient here. `IF last_run_epoch = ?` is — see
  [daruk.md](./daruk.md#claiming-a-scheduler-tick).

The one job where two holders would genuinely conflict is the VIP, and that one cannot overlap
for a different reason: binding an address twice is visible as an address conflict, and both the
old logic and the new one release rather than bind when they cannot establish that they should
hold it. Upgrade `bifrost` on all nodes in the same rollout rather than leaving it half-rolled
for long.

---

## What deliberately did *not* move

`helios_zk.leader_ip` stays, and so does every caller that genuinely means *"which node leads
the ZooKeeper ensemble"*. That is a real question with real answers:

* `cluster status` and `valcli` **report** it. An operator asking which node is the ensemble
  leader wants the ensemble leader.
* `mipha`'s self-fence stops ZooKeeper on a host that has just admitted it cannot serve
  storage, so that a healthy node takes over coordination. "Should I hand ensemble leadership
  off" is a question about the ensemble.
* `mipha`'s failover waits for the ensemble to settle before proceeding. That is a liveness
  check on consensus, not a placement decision.
* Every `get_catalyst_target_ip` keeps the probe as its **fallback** for an unreachable
  ensemble, where no candidacy has published anything and a submission still has to go
  somewhere.

What went away is the *placement* use, and with it four of the nine private copies of the probe
loop — `catalyst`, `dagur`, `mimir` and `bifrost` do not ask the ensemble anything any more.

`spectrum_phx`'s `Catalyst.leader_ip/0` also did not move. It is an Elixir process that cannot
open a ZooKeeper session with the client in this repo, and it already carries a `:catalyst_ip`
override and a standing TODO for leader resolution. Submissions from the Phoenix console
therefore still go to the configured or local Catalyst; a submission that lands on the wrong
node is now *recorded* and replayed by the dispatcher's sweep rather than lost, which is why
this is a gap and no longer a defect. See [catalyst.md](./catalyst.md#recovery).

---

## Reading the current state

```bash
# Who is standing for what, and who won.
podman exec -it systemd-zookeeper bin/zkCli.sh -server 127.0.0.1:2181 \
  ls /helios/leaders
podman exec -it systemd-zookeeper bin/zkCli.sh -server 127.0.0.1:2181 \
  ls /helios/leaders/vali-queue
podman exec -it systemd-zookeeper bin/zkCli.sh -server 127.0.0.1:2181 \
  get /helios/leaders/vali-queue/n_0000000000
```

Each daemon also logs its transitions, which is the difference an operator needs between "the
worker is busy" and "no worker is running anywhere":

```
Vali Catalyst worker: draining the queue, this node holds the vali-queue candidacy
Catalyst dispatch: standing by, another node holds the queues
```

An empty `/helios/leaders/<service>` means nothing is standing: either no daemon of that kind
is running, or none of them can reach the ensemble. Both are outages of that job, and neither
is silent in the journal.
