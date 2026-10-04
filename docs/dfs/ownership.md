# Ownership, epochs, and fencing

How exactly one Sidon writes a vdisk at a time, why a partition produces rejected
requests instead of corrupted disks, and what this deletes from the existing HA design.

## 1. The model

Every vdisk has, in Hydra:

- an **owner** — the node whose Sidon serves its I/O, and
- an **epoch** — a monotonically increasing integer, bumped by every ownership change,
  never reused, never decremented.

Ownership changes only through a Daruk lightweight transaction:
`IF owner = <expected> AND epoch = <expected>` → set new owner, epoch+1. The CAS is the
same machinery `/v1/vm/claim` already uses, applied to the disk instead of the VM — and
losing the race returns the actual owner, exactly as VM claims do.

Every journal append and every egroup write carries the writer's epoch. **Replicas
remember the highest epoch they have seen fenced for each vdisk and reject anything
lower.** That replica-side check, not the lease, is the safety mechanism; the lease
exists for orderly handover and to bound how long a deposed owner keeps trying.

## 2. Takeover

Whether planned (migration) or unplanned (owner died):

```
1. CAS ownership in Hydra: epoch e → e+1, owner → me.
2. FENCE: send fence(vdisk, e+1) to every reachable journal replica.
   Each replica records e+1 and thereafter rejects appends with epoch ≤ e.
3. READ TAIL: read the journal from any one fenced replica; rebuild the overlay index.
4. Serve. First drain re-replicates the journal locally if this node holds no replica.
```

## 3. Why this is safe — the short proof

The claim (invariant I-4): after takeover completes, every write the old owner ever
acknowledged is visible to the new owner, and the old owner can never acknowledge
another.

- Journal appends require **all** RF replicas to acknowledge (write-all,
  [data-path.md §2](./data-path.md)). Therefore any single replica contains every
  acknowledged write.
- Step 2 fences at least one replica the old owner needs (it needs *all* of them), so
  from that moment the old owner cannot complete — hence cannot acknowledge — any new
  append. It does not matter that the old owner is unreachable, wedged, or convinced it
  is still the owner: its next append meets a rejection, turns into EIO to its guest, and
  its guest is being restarted elsewhere anyway.
- Step 3 reads a fenced replica, which by the first point holds every acknowledged
  write. Nothing acknowledged is lost; nothing unacknowledged is promised (I-2 permits
  either outcome for in-flight writes).

Three lines, and every line leans on write-all. This is why the journal is not quorum:
with quorum replication both the "fence one suffices" and the "read one suffices" halves
become multi-round protocols with corner cases, and the proof stops fitting in a review.

Liveness note: step 2 fences *every reachable* replica so step 3 can pick any and so
returning replicas rejoin pre-fenced; safety needs only one.

## 4. What this deletes from the existing design

The fencing work in `mipha.py` (docs/fencing.md) built a four-rung ladder — self, spark,
BMC, storage — because with DRBD the storage layer can only *infer* that a dead host has
stopped writing (quorum arithmetic, and only where quorum is armed; two-node clusters
have no storage fence at all).

Under Sidon, the storage fence is exact and universal: **deposing a host is a metadata
CAS plus one fence message, and the deposed host's writes are rejected by every peer that
matters, with no cooperation from the host itself.** The residual-unsafe cases enumerated
in fencing.md §8 — no BMC plus unarmed quorum, the two-node gap — cease to exist for
DFS-backed disks. The BMC rung remains worth having for the *host* (a wedged kernel
still burns CPU and holds the VIP), but data safety no longer depends on it. Mipha's
ladder shrinks to: fence the disks (always works), then optionally power off (hygiene).

The migration-lock limitation recorded in daruk.md — no holder identity, a late cleanup
can unlock a later migration — is also structurally closed here: epochs are the holder
identity, and there is nothing a stale holder can do that a replica will accept.

## 5. Live migration

Because the guest always talks to its local Sidon and non-owners forward
([architecture.md §2](./architecture.md)), storage needs nothing from qemu:

```
0. The destination attaches the vdisk in forwarding mode (attach, forward=true). Its Sidon
   binds the NBD socket the destination domain names; every request on it is relayed to
   the node that still owns the disk. Without this the destination qemu fails to start:
   the socket does not exist. The writer is still the source.
1. VM memory migrates (virsh migrate, unchanged). Two qemu processes may have the
   export open; only the source writes.
2. The guest resumes on the destination. Its I/O is forwarded to the source owner.
   Correct, slower, running.
3. The destination runs takeover (below), still with the guest running.
4. Locality rebuilds in the background: Purah notes the owner holds no local replica
   and migrates one over, egroup by egroup, off the hot path.
```

Step 2 is the design's quiet win: there is no cutover instant where storage must
hand off synchronously with the VM, so the migration windows that DRBD dual-primary
existed to cover simply do not occur. Forwarding mode is also, deliberately, not a
special case — it is the same path a post-failover VM uses before locality catches up,
so it is exercised constantly rather than only during migrations.

### The takeover

The NBD session the guest holds on the destination cannot be reconnected for it, so the
backend behind the session changes under it: a switch that forwards until a local vdisk is
installed. The change has to happen at an instant no request straddles, so it is a *stall*
(admit nothing new, let the admitted finish), not a lock around every request.

```
stall    guest I/O on the destination is held; admitted requests finish
release  ask the owner to drain its journal, stop serving, drop its socket (OP_RELEASE)
claim    CAS owner and epoch together, e -> e+1                       (§2 step 1)
fence    fence every reachable journal replica at e+1; adopt the tail (§2 steps 2-3)
install  the opened vdisk becomes the backend behind the session; the stall is released
```

Why this is safe, in terms of the invariants:

- **Never two writers.** The owner has stopped serving (or never did) before the claim is
  attempted, and the claim's fence makes any owner that did not stop harmless (§3). The release
  also refuses while the owner's own client is still connected, and takes the disk out of its
  attached table *before* it drains, so a forwarded request arriving afterwards is answered
  "not served here" and does not land behind the drain.
- **An acknowledged write is never lost.** The stall waits for admitted writes before the owner is
  asked to drain, so everything it acknowledged is on every replica; the new owner adopts the
  journal from a fenced replica (§3).
- **A stale claim loses.** The compare-and-swap is conditioned on the owner and epoch the
  handover just read, so a node that lost the race installs nothing.

Failure, and what it leaves:

| Step that failed | State left | Recovery |
|---|---|---|
| read ownership; release refused | nothing changed: the owner serves, the destination forwards | vali detaches the destination forwarder (migration failed) or the operator retries |
| release unanswered | unknown: the owner may have let go | rerun `takeover` |
| claim, fence or open (owner already released) | destination I/O errors (the owner no longer serves) | rerun `takeover`; it skips the release if Hydra names this node and re-claims from what it reads |
| a concurrent second takeover | the second waits behind the stall and finds the disk owned | none needed |

Failure *before* the migration moves the guest is the common case and the cheap one: the
destination's forwarders are detached, the source never changed owner, no epoch moved. After
the guest has moved there is nothing to roll back to — the guest runs on the destination — so
the migration commits the placement, retries the takeover, and if it still fails reports the
step by name together with the command that finishes it (`valcli storage.takeover <vdisk>
<node>`).

## 6. Immutable images

Golden images get a distinct vdisk class: sealed at creation, no journal, no owner, no
epoch. Any Sidon serves reads from any verified replica; writes are refused by class at
the NBD layer. This replaces `allow-two-primaries` — the option that caused the
corruption the fencing work spent weeks defending against — with a category that cannot
express the hazard. VM clone-from-image is a map copy against the immutable parent
(snapshot machinery, [metadata.md §6](./metadata.md)), not a byte copy.

## 7. When Hydra is unreachable

The honest degradation table, decided rather than discovered:

| Condition | Behaviour |
|---|---|
| Hydra down, steady state | I/O continues indefinitely: journal appends and reads touch no metadata. Drains queue; the journal fills; at the high-water mark writes backpressure. Bounded by journal size, not by a lease timer. |
| Hydra down, owner dies | The vdisk pauses: takeover needs the CAS. This is the correct outcome — promoting an owner without the CAS is exactly the split-brain machinery elsewhere; a paused disk recovers, a forked one does not. |
| Lease expiry with Hydra down | Nothing. The lease is not the fence; replicas are. A deposed owner is stopped by fencing regardless of what it believes about its lease, so lease staleness during an outage costs availability decisions only, never safety. |

That last row is the difference between this design and every lease-only system: the
answer to "what if the clock skews / the lease server blips / the old owner doesn't
notice" is always the same — the replicas were already told, and they do not negotiate.
