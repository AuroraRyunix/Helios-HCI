# Ganon — the fault-injection harness

Built first, trusted first, and the acceptance gate for everything after it. A storage
system's tests can only be as honest as the thing that attacks it, so the attacker is
milestone 1 and the filesystem is not.

Ganon plays the Calamity: its job is to be the recurring disaster the kingdom claims to
survive, on a schedule, with receipts.

## 1. Why the harness precedes the filesystem

Two reasons, one methodological and one immediate:

- **A harness debugged against the code it gates proves nothing.** If Ganon and Sidon
  are written together, every ambiguous result becomes "fix whichever is easier", and
  the harness quietly learns the implementation's blind spots. Ganon is therefore
  validated against **DRBD protocol C** — an implementation two decades of production
  says is correct. If Ganon reports a violation there, Ganon is wrong (or, more
  interestingly, DRBD-as-deployed-here is); either way the harness is calibrated against
  ground truth before it ever judges new code.
- **It has standalone value now.** The current product's storage path — DRBD, the
  fencing ladder, the quorum gate — has never faced a systematic attacker. Milestone 1
  will find things regardless of whether the DFS is ever built; the session that
  produced this design found the storage capacity gate had never refused anything, by
  looking. Ganon is that looking, mechanised.

## 2. The stamp: self-describing blocks

Every 4 KiB block Ganon writes is a stamp:

```
magic | vdisk_uuid | offset | generation | wall_ns | payload_crc32c | stamp_crc32c
```

- `offset` is where the block *believes* it lives — a misdirected write or read is
  self-evident (attacks I-8).
- `generation` is a per-offset monotonic counter from Ganon's own write log — staleness
  and time-travel are self-evident (attacks I-2).
- The whole stamp is checksummed — torn or corrupted blocks are self-evident (I-2, I-5).

Ganon keeps its own **ack journal** on the machine driving the test (not on the system
under test): every write attempt is logged *issued* before the syscall and *acked* after.
Crash-in-flight writes are therefore known to Ganon as "either outcome legal", which is
exactly I-2's contract.

## 3. The verdict

After any scenario, Ganon reads everything and asserts, per offset:

1. The generation found is the **latest acked**, or a **later issued-but-unacked** one.
   Anything else — an older generation, a never-issued generation, a foreign offset's
   stamp — is a verdict of *history violation*, with the offset, the found stamp, and
   the relevant slice of the ack journal printed. A verdict names evidence, not vibes.
2. During-scenario reads (Ganon reads while injecting) obey the same rule at read time,
   which is what catches windows that heal before the final sweep.
3. Unreadable ranges are verdicts too (I-1): an acked write behind EIO after a
   single-failure scenario at ftt=1 is a durability violation, not an excuse.

## 4. The injectors

Each maps to the failure it forges, and greyness is deliberate — clean failures are the
easy case and real clusters do not fail cleanly:

| Injector | Mechanism | The real-world event it forges |
|---|---|---|
| Process kill | `kill -9` on the daemon / qemu | crash mid-anything |
| Process **stall** | `SIGSTOP … SIGCONT` | the wedged-but-alive host, the fencing work's hardest case; a stalled owner resuming with stale state is the sharpest I-4 attack there is |
| Kernel death | `echo c > /proc/sysrq-trigger` | host loss with no goodbye (on an expendable node) |
| Partition | nftables drop, **including asymmetric** (A sees B, B drops A) and partial (data port only, Hydra still reachable — and the reverse) | every split-brain precondition; the asymmetric cases are where lease-based designs die |
| Disk misbehaviour | `dm-flakey` (fails after N s), `dm-dust` (bad sectors), `dm-delay` (slow disk ≠ dead disk) | dying disks, and the grey zone where a disk answers slowly enough to stall but not to fail |
| Power-loss proxy | `dm-flakey` with `drop_writes` across a remount | the lying-fsync window, to the extent software can forge it (invariants.md is explicit that a physical power-cut rig eventually supersedes this) |
| On-disk corruption | flip bytes in a replica file / DRBD backing LV directly | bitrot; the scrub-and-read-repair attack (I-5) |
| Clock skew | `date -s` jumps on one node | every "the lease hasn't expired by *my* clock" argument |
| Space exhaustion | fill the backing filesystem | ENOSPC mid-drain, the error path nobody tests |

## 5. Scenario × invariant

The matrix is the test plan; each cell is a named scenario with a seed, so every failure
replays. The load-bearing rows:

| Scenario | Attacks | DRBD adapter | Sidon adapter |
|---|---|---|---|
| Kill writer mid-burst, restart, verify | I-1, I-2 | ✓ | ✓ (journal replay) |
| Kill one replica mid-burst, verify, heal, verify | I-1, I-6 | ✓ (resync) | ✓ (redirect + Purah) |
| Partition writer from a replica, keep writing, heal | I-2, I-4 | ✓ (quorum armed) | ✓ |
| **Stall owner, force takeover, resume the zombie, let it try** | **I-4** | primary switch + resume | fence + epoch rejection — *the* scenario |
| Asymmetric partition during takeover | I-4 | ~ (where expressible) | ✓ |
| Corrupt a replica, read everything | I-5 | ✗ — DRBD without integrity options is *expected to fail this*, which is itself a finding to document, not a harness bug | ✓ |
| Kill during drain; kill during replay; kill between data-durable and map-commit | I-3 | n/a | ✓ |
| Ownership transfer under load ×100, no faults | I-2, I-4 | primary switches | takeovers |
| ENOSPC during drain | I-3 | ~ | ✓ |
| Two Ganons race to attach one vdisk | I-4 | dual-primary refusal | lease refusal |

## 6. Adapters

Ganon speaks to a block device and to a control surface; both are traits:

- **Block**: open/read/write/flush against a device path or NBD socket. DRBD: the
  `/dev/drbd/by-res/...` path. Sidon: the NBD unix socket.
- **Control**: create/attach/transfer-ownership/fence/replica-locations, so scenarios are
  written once. DRBD: LINSTOR + drbdadm. Sidon: the Daruk endpoints.

Scenario definitions never mention which substrate they run against; the matrix marks
the cells that only one substrate can express.

## 7. Where it runs

- **CI tier**: single-node, minutes — kill/restart/verify, corruption/read-repair, the
  deterministic subset. Green is a merge gate for every data-path change, enforced the
  same way the rest of CI now is (discovery, not enumeration).
- **Soak tier**: multi-node, hours to days, randomised scenario schedule from a printed
  seed. Runs on the test cluster; a soak failure ships the seed, the ack journal and the
  verdict as one artefact, because a distributed-storage bug report without a replay
  recipe is a rumour.
- Implementation language: Rust, sharing the stamp/verify code with nothing — Ganon
  deliberately does not link Sidon's libraries, so a shared bug cannot vouch for itself.
  (The one exception forever: the stamp format spec lives in this file, and both
  implement it from the document.)

## 8. What Ganon cannot prove

Restated from invariants.md so no green run overclaims: firmware that lies about
durability, Byzantine replicas, and Scylla's own promises are outside its reach.
Ganon's verdict is "no violation observed under these injected histories" — which is the
strongest statement available, and the design treats anything stronger as marketing.
