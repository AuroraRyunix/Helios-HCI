# Host states

What a host can be, what puts it there, what each state stops, what brings it back, and what a
host that comes back still has to put right. The state is `hydra.nodes.status` (with
`maintenance_mode` beside it); "unreachable" is not a stored state but what the leader observes
before it writes DOWN. Code: `mipha.py` (watchdog, fencing ladder, rejoin), `vali.py` (maintenance,
placement), `spark_daemon_decoded.py` (the marker files). Tests: `test_host_states.py`,
`test_maintenance_flow.py`, `test_fencing.py`, `test_mipha_orphaned_quarantine.py`.
Maintenance has its own document: [maintenance.md](./maintenance.md).

## The states

| State | Set by | Meaning | Cleared by |
|---|---|---|---|
| `NORMAL` | initial; every path below | Takes guests. | — |
| `DEGRADED` | the host's own Mipha watchdog (**only** it writes it; also published when a self-fence did not fully take) | **Quarantined**: its storage or libvirt has failed three probes in a row. It keeps the guests it has and receives no new ones. | the same watchdog, after **6 consecutive clean passes** (60 s); or `clear_orphaned_quarantine` if the process that set it is gone (every rollout restarts Mipha); or `mipha --clear-self-fence` on the host |
| `FENCED` | the host's own watchdog, after a self-fence that **took** | Its guests were stopped, every vdisk it served was given up, and ZooKeeper leadership released when three or more nodes. The leader restarts its VMs elsewhere. | **an operator**: `mipha --clear-self-fence` on that host (`auto_recover_after_clean_seconds` is 0 by default, on purpose: a host that destroyed its own guests had a real fault) |
| `DOWN` | the Mipha **leader**, after the host failed 3 consecutive polls (10 s apart) on both ICMP and spark-daemon, and the fencing ladder allowed the failover | Presumed dead. Its VMs were released and restarted elsewhere. | the leader: when spark-daemon answers again it runs the rejoin sequence |
| `ENTERING_MAINTENANCE` | `POST /hosts/maintenance` (enter) | Being drained. | the enter task (to IN_MAINTENANCE, or back to NORMAL on failure); `leave` if the task died |
| `IN_MAINTENANCE` | the enter task | Emptied of guests; the metadata and storage planes stay up. | `leave` |
| `RECOVERING` | the leader's rejoin of a DOWN host; the leave task of a maintenance host | Back, not yet trusted: no placement. | the rejoin (NORMAL after its services start) or the leave task (NORMAL once every service is UP) |
| *unreachable* | observed | spark-daemon not answering. After three polls the leader marks DOWN (unless the host is FENCED/in maintenance, which it skips). | — |

## What each state stops

| | Placement of a new or restarted guest (start, HA restart, evacuation) | DRS | Migration onto it (manual, DRS) | Entering maintenance | HA failover of its guests |
|---|---|---|---|---|---|
| NORMAL | yes | yes | yes | yes | only if it goes DOWN |
| DEGRADED | **no** | **no** (not a target, not a source of balance) | **no** | no (the claim is `NORMAL -> ENTERING_MAINTENANCE`) | no: it keeps its guests running |
| FENCED | no | no | no | no | **yes**: the leader skips its own ladder and restarts the guests |
| DOWN | no | no | no | no | done at the transition |
| RECOVERING | no | no | no | no | no |
| ENTERING/IN_MAINTENANCE | no | no | no | no | not performed: Mipha exempts it |

Placement, DRS and migration all use the same test (`vali.host_ineligible_reason`): the recorded
state is NORMAL **and** the host answers with every managed service UP and not in maintenance. A
database that cannot be read, or a host that does not answer, is a reason to refuse and never a
pass. Until this was shared, a manual or DRS migration read only the daemon's own maintenance flag
and could be aimed at a DEGRADED, FENCED or DOWN host.

## Thresholds and timers

| What | Value | Sane? |
|---|---|---|
| Self-fence probe interval | 10 s | Yes. |
| Consecutive failed probes to quarantine or fence | 3 (30 s) | Yes: a Sidon restart or a slow virsh is a blip, not three. |
| Startup grace before any self-fence | 180 s | Yes; covers Sidon's journal replay. |
| An `unknown` probe | never counts | Right: it must not destroy guests. |
| A quarantine lifts after | 6 clean passes in a row (60 s) | New. It used to lift on the first clean pass, so a host whose storage came and went flipped between DEGRADED and NORMAL at probe speed, schedulable in each NORMAL window. |
| `auto_recover_after_clean_seconds` for FENCED | 0 (operator only) | Deliberate. |
| Leader polls for DOWN | every 10 s, 3 failures (30 s) | Short: a partition of 30 s fails a host over. Safe only because the ladder fences before moving anything (below); raising it trades recovery time for fewer needless failovers. Not changed. |
| Maintenance lock TTL | 300 s, renewed | See maintenance.md. |

## Split brain: what keeps two copies of a guest from running

Failover restarts a guest on another host while the original host may still be running it (a
partition looks exactly like a death). Three layers, in order:

1. **The fencing ladder** (docs/fencing.md): self-fence (the host says it stopped), spark, BMC power-off,
   and the **storage rung** — the new owner's claim of the guest's vdisks raises their epoch and every journal
   replica refuses anything lower. That last rung is exact and needs no cooperation from the old host.
   `unconfirmed_fence_policy: block` (default) refuses the failover if no rung confirmed.
2. **Conditional placement writes**: `release_orphaned_vm` and the start's claim are compare-and-swaps on
   `host_ip`; a guest already recovered elsewhere is not started twice.
3. **The old host cannot write**: a surviving qemu on it has its vdisks fenced out and sees I/O errors.

What was *not* handled: when the old host comes back, those surviving qemu processes (writing nothing, never
useful again), their libvirt definitions and the NBD sockets Sidon kept for them were left for ever, and a stale
*running* domain could be mistaken for the guest by anything that listed libvirt. **Now**: before a rejoining
host's services are started, the leader reconciles it against Hydra (`mipha.reconcile_returning_host`):

* every domain Hydra places on **another host, or on none**, is destroyed if live and undefined;
* every `<vm>-disk<N>` vdisk attached on the host for such a guest is detached;
* a domain Hydra places **here** is left alone, and so is one Hydra has no row for (not a cluster VM);
* nothing is destroyed if the host's domain list or Hydra cannot be read.

## How a host rejoins

* **Mipha leader, host was DOWN** (spark answering again): `DOWN -> RECOVERING`; reconcile the host's guests
  (above); start `zookeeper, hydra-db, daruk, sidon` and every other unit; poll status for up to 60s verifying every
  service is UP; `RECOVERING -> NORMAL`. If services fail to become UP, the host remains in `RECOVERING` rather than
  prematurely becoming schedulable. Storage needs no resync: extent groups are immutable and Purah restores replica
  counts in the background.
* **ZooKeeper** rejoins by starting (the ensemble's voters are static, in the Quadlet). **ScyllaDB** rejoins by
  gossip unless the operator ran `nodetool removenode`; the leader prints the ring candidate but never removes a
  node (see ring_lifecycle.md).
* **FENCED** is never rejoined by the leader (it never stopped answering): the host's operator runs
  `mipha --clear-self-fence`, which also restarts ZooKeeper if the fence stopped it. The fence marker is on tmpfs,
  so a reboot ends it.
* **DEGRADED** clears itself (6 clean passes), or is cleared as an orphan, or by the same command.

## What the operator sees

`valcli host.list` (status and maintenance flag), the console's hosts page, `cluster status` (the node block:
`[IN MAINTENANCE]`, `[STALE]`, per-unit state and, in maintenance, whether a unit is up on purpose), and
`mipha --fence-status` on a host for the self-fence report.

## State-transition table (executable: `test_host_states.py`)

| From | Event | To | By |
|---|---|---|---|
| NORMAL | 3 failed storage or libvirt probes | DEGRADED | host watchdog |
| NORMAL | 3 passes with an unserviceable vdisk and a healthy peer | FENCED (DEGRADED if the fence did not take) | host watchdog |
| NORMAL | the same with no peer answering | DEGRADED | host watchdog |
| DEGRADED | 6 consecutive clean passes | NORMAL | host watchdog |
| DEGRADED | a clean host, quarantine not remembered | NORMAL | orphan reconcile |
| FENCED | `mipha --clear-self-fence` | NORMAL | operator |
| NORMAL / DEGRADED | 3 failed leader polls and the ladder allows | DOWN | leader |
| DOWN | spark answers again | RECOVERING, then NORMAL | leader |
| NORMAL | enter | ENTERING_MAINTENANCE, then IN_MAINTENANCE | vali |
| any maintenance state, RECOVERING | leave | RECOVERING, then NORMAL | vali |
| in maintenance | any probe failure | unchanged (exempt) | host watchdog |
| single-node cluster | any probe failure | unchanged (nowhere to go) | host watchdog |

## Found, not fixed

* The leader does not monitor *itself* (it filters its own address out); the host watchdog is the only thing that
  can quarantine it, and a fenced leader releases ZooKeeper leadership so another node takes over.
* There is no way to clear a non-NORMAL status from another host or from the console; the host's own `mipha
  --clear-self-fence` is the override. A `valcli host.clear <host>` that does it over spark is the natural
  addition.
* The rejoin writes NORMAL without verifying services (above).
* `RECOVERING` left by a maintenance `leave` that timed out is retried with `leave`; one left by the DOWN rejoin
  has no retry (the leader only acts on DOWN).
