# Invariants

The contract. Every protocol in the other documents exists to satisfy a line in this
file, every Ganon scenario exists to attack one, and a proposed change that cannot say
which invariant it preserves — and how — is not reviewable.

The characteristic failure of a distributed store is not a crash. It is a read returning
something that was never written, on one extent, weeks after the partition that caused it
healed. That class of bug does not appear in review and does not appear in unit tests.
It appears here, as a violated invariant, or it appears at a customer.

## The write contract

**I-1 — Durability.** A write that has been acknowledged to the guest is returned by
every subsequent read of that range (until overwritten), across any combination of
failures the container's `ftt` claims to tolerate: any single node/disk loss at ftt=1,
any two at ftt=2. "Acknowledged" means the NBD reply left Sidon; there is no weaker ack.

**I-2 — Legal history.** A read of any range returns, for each atomicity unit, either
the most recently acknowledged write of that unit or — during the window where a write
was issued but never acknowledged — that unacknowledged write in full. Never a third
value, never a torn unit, never a stale value after a newer acknowledged one has been
read (no time travel within a vdisk).

The atomicity unit is **one journal record** (a single guest write up to the record size
cap). A guest write large enough to split into multiple records is made atomic by a
commit marker: replay applies only complete marker-terminated groups, so a crash exposes
all-or-nothing, never a prefix. This is stronger than the 4 KiB-sector atomicity a
physical disk promises, and it costs one marker record.

**I-3 — Map integrity (data before metadata).** The map never references bytes that are
not durable on every replica it names, and never references space that has been
reclaimed. Operationally: journal and egroup writes are fsynced on all replicas *before*
the map row that points at them is committed; GC reclaims an egroup only after the map
dereference is itself durable and I-7 holds. A crash between data-durable and
map-committed leaks orphaned bytes — harmless, found by scrub — and never the reverse.

## The ownership contract

**I-4 — Single writer.** At most one epoch is ever *active* per vdisk, and only the
epoch's holder can get writes acknowledged. Concretely: replicas reject journal appends
and egroup writes stamped with an epoch lower than the highest they have fenced, so two
Sidons can never both acknowledge writes for one vdisk concurrently — a partition turns
the loser's writes into rejected requests, not into a diverged replica. This is the
invariant Ganon attacks hardest, because its violation is the dual-primary corruption
class this design exists to delete. The proof obligation is in
[ownership.md §3](./ownership.md).

## The integrity contract

**I-5 — No silent corruption.** No read returns data that fails its checksum. A mismatch
triggers repair from another replica; if no replica verifies, the read fails loudly with
a distinct error. There is no code path that returns unverified bytes, including during
repair, replay and forwarding.

**I-6 — Convergence.** After Purah has completed a pass with the cluster stable, every
live egroup has exactly RF verified replicas on distinct failure domains (nodes now;
racks when the topology work lands). Degradation is a state the system reports and
repairs, never one it forgets.

**I-7 — GC safety.** Space is reclaimed only if unreferenced by *every* map generation —
current and all snapshots — in two consecutive Purah scans separated by more than the
maximum drain duration. The two-scan grace period is what makes mark-sweep immune to the
race where a drain commits a reference between scan and sweep.

**I-8 — Isolation.** No read of vdisk A ever returns bytes written to vdisk B. Every
journal record and every extent carries the vdisk UUID it belongs to, verified on read —
so an offset-arithmetic bug or a misdirected write surfaces as a checksum-class failure
(I-5), not as a tenant reading a neighbour's disk.

## What software verification cannot reach

Stated so the limits are chosen rather than discovered:

- **A lying fsync.** If the kernel, a firmware write cache, or a virtualised test disk
  acknowledges durability it does not provide, I-1 and I-3 rest on a false premise no
  userspace assertion can detect. Mitigation: `dm-flakey`-backed power-loss simulation in
  Ganon as a proxy now; a physical power-cut rig before the first production customer,
  not before.
- **Byzantine replicas.** A replica that actively lies (returns wrong data with a valid
  recomputed checksum) is outside the model. mTLS peer authentication bounds this to
  "a compromised cluster node", which is already game over for stronger reasons.
- **Scylla itself.** The map's durability is Hydra's QUORUM promise. That is a dependency
  taken with eyes open — it is the same promise the whole management plane already rests
  on, now load-bearing for storage. The saga backup and restore path is what bounds the
  blast radius of being wrong.
