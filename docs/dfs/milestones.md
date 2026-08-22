# Milestones

The build order. Each step has a gate (which Ganon scenarios must pass before the next
begins) and an abandonment value — what the work is worth if the project stops there.
A plan whose steps are only valuable if all of them happen is a bet, not a plan.

Precedent worth remembering: this codebase has migrated storage substrates before —
Aether replaced an earlier GlusterFS-based design (README §history). The coexistence
pattern below is the same one that worked then.

## 0. Design sign-off — *this directory*

The documents are the deliverable. Review ends when every "named hard problem"
(exactly-once drain, takeover proof, GC race) has a chosen solution and a rejected
alternative on record.

## 1. Ganon vs DRBD

Build the harness ([ganon.md](./ganon.md)); calibrate it against the substrate two
decades of production says is correct. Run the full applicable matrix against the
*current* product: DRBD volumes, the fencing ladder, the quorum gate.

- **Gate:** Ganon's verdicts on DRBD match DRBD's documented semantics — including the
  expected corruption-detection failure, which must be reported as a substrate property,
  not a harness bug.
- **Abandonment value: high.** A systematic attacker for the shipping product, wired
  into CI, plus whatever it finds. This milestone is worth funding even if the word
  "Sidon" is never spoken again.

## 2. Map and endpoints, no data path

The `dfs_*` migrations in `helios_schema.py`, the Daruk `LWT_OPS` entries
([metadata.md §2](./metadata.md)), and simulation tests driving claim / drain-commit /
egroup-state through the real Daruk against real Scylla — the `test_daruk_lwt.py`
pattern, which exists precisely because LWT behaviour was not assumable last time
(`INSERT JSON IF NOT EXISTS` silently not being a CAS, the driver renaming `[applied]`).

- **Gate:** the exactly-once drain and takeover CAS sequences proven at the metadata
  layer alone: two simulated owners race claims, race drain-commits, race across an
  epoch bump — one winner every time, losers named.
- **Abandonment value: moderate.** The endpoints generalise (epoch-fenced ownership is
  useful beyond storage) and the simulation suite documents Scylla's LWT semantics
  further.

## 3. The journal vertical slice

Sidon serves one vdisk over an NBD unix socket: append to a replicated journal
(write-all, epoch-stamped), ack, replay on restart. No drain, no egroups — reads come
entirely from the overlay. First single-replica, then RF2 across two nodes.

- **Gate:** Ganon kill/restart/verify and kill-one-replica scenarios green at RF2. qemu
  boots a real guest from it (slowly; nobody cares yet).
- **Abandonment value: low** — and that is fine. This is the first step that only pays
  as part of the whole, which is why two milestones of standalone value precede it.

## 4. The extent store

Drain, open/sealed egroups, checksummed reads, redirect-on-replica-failure, the
data-before-metadata ordering throughout.

- **Gate:** kill-during-drain, kill-during-replay, kill-between-data-and-map, ENOSPC —
  the I-3 rows of the matrix. Plus a full read-back of a multi-GiB vdisk with every
  slice verified.

## 5. Ownership transfer and fencing

The takeover protocol of [ownership.md §2](./ownership.md), planned and unplanned, and
the immutable image class.

- **Gate:** the stalled-zombie scenario — SIGSTOP the owner, take over, SIGCONT, watch
  every zombie write get rejected — plus asymmetric partitions during takeover, ×100
  transfer soak under load. This is the milestone that deletes the dual-primary class,
  so its gate is the harshest.

## 6. Purah

Re-replication after loss, mark-sweep GC with the two-scan grace, background scrub
against seal hashes, locality rebuild after ownership moves.

- **Gate:** corrupt-and-scrub, kill-replica-and-converge (I-6), and a GC soak proving
  reclaimed space never intersects referenced data across ownership churn (I-7).

## 7. Integration

Vali places new vdisks and drives migration through forwarding-then-takeover; Mipha's
ladder gains the exact storage fence and its residual-unsafe list shrinks accordingly;
Spectrum (both consoles) shows DFS vdisks; capacity readers learn that the thin pool now
has two consumers; saga gains extent-aware guest-data backup via snapshots. mcli gets
`dfs_*` health checks — under the category-map discipline, with dotted categories, since
the test that guards that map will refuse anything else.

- **Gate:** a VM lives its whole lifecycle — create, boot, migrate, snapshot, host
  failover, delete — on DFS storage through the normal UI, with Ganon's CI tier green
  throughout.

## 8. Migration from DRBD

Per-disk, reversible until the final cutover: create the DFS vdisk, mirror from the DRBD
device (qemu drive-mirror for live VMs; direct copy for stopped ones), verify by full
checksum comparison, cut the VM's XML over, leave the DRBD resource intact until the
operator confirms, then release it. New VMs default to DFS at the operator's chosen
moment, not before.

- **Gate:** round-trip a real VM DRBD→DFS→boot→verify, and the reverse path documented
  and tested — a migration that cannot retreat is a hostage situation.
- LINSTOR remains for whatever chooses it, indefinitely; the Gluster precedent says the
  long tail is long.

The switch already has a name and a config key. `cluster.json` carries `"dfs_engine": "linstor"` on every cluster this provisioner has ever built, and `get_dfs_engine()` is defined in `cluster_new.py`, `mipha.py`, `spark.py` and `vali.py` — four copies, each hardcoded to `return "linstor"`, each called by nothing. It is the vestige of the GlusterFS transition, left behind when that migration finished. This milestone is what it was for: consolidate the four copies into one reader of the existing key, then let it return something else. Worth knowing before someone deletes it as dead code — it is dead, but it is dead in exactly the shape this needs.

## 9. Performance — deliberately last

vhost-user-blk beside NBD, drain tuning, read-ahead, locality policy. Every earlier
milestone accepts "correct and unremarkable" throughput on purpose: performance work
reorders operations, and reordering is where invariants go to die — so it happens only
inside a fence of green Ganon soaks, one change at a time.

- **Gate:** the same matrix, unchanged. Performance never gets its own, weaker gate.

## The rule that spans all of it

No milestone's code merges while any Ganon scenario it claims to satisfy is red, and no
scenario is weakened to make a milestone close. The harness outranks the schedule —
that is the entire reason it was built first.
