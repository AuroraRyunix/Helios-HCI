# Replicating snapshots to another site

**Status: designed. The data plane's building blocks are built and tested against a local
simulation of a second site (section 9); no network transport of group bytes, no listener and no driver
against a real remote exists, and no second cluster has ever been involved. Three more pieces are
built and tested in isolation (the pinned-key verifier, the Hydra map sink and the failover state
machine, section 9); none is called by any daemon.** Decisions: D-28 to D-31 in
[decisions.md](./decisions.md). The unit being replicated is a *snapshot set* from a
[protection domain](./protection_domains.md).

Nutanix's Cerebro calls this asynchronous replication. Helios's version is simpler than it
sounds, because of two facts the design already paid for: a snapshot is **a map plus immutable
extent groups**, and **extent groups are never modified once sealed**. Replication is therefore
"send the groups the remote does not have, then send the map". There is no log to ship, no
change tracking and no base snapshot to keep.

## 1. Vocabulary

* **Site**: one Helios cluster, with its own CA, Hydra and Sidons.
* **Source** / **target**: the site a snapshot is pushed from / to. Push only (D-28).
* **Group**: an extent group file, `<id>.eg`, extents each followed by a 32-byte footer.
* **Manifest**: one snapshot's map rows, the groups they reference, and the digests that verify
  them.
* **Staging**: a target-local directory where received bytes wait until verified.

## 2. How two sites trust each other (D-28)

The cluster CA is per cluster, and its trust rule is "signed by our CA": `client.crt` is
identical on every node and `spark-daemon` on 9099 is a root-equivalent API
([mtls_lifecycle.md](../mtls_lifecycle.md)). Anything that adds a foreign CA to those trust
stores, which is what cross-signing is, makes every node of the other cluster a full member of
this one. Rejected outright.

**Taken: a pinned site certificate on a separate, import-only listener.**

* Each site has one **site key pair** and certificate (`CN=HCI-Site-<site id>`, serverAuth and
  clientAuth). It is a different credential from `client.crt` and `node.crt`, kept on every node
  in its own directory, and it is never usable against 9099 or 9105.
* A site is authenticated by its certificate's **SPKI SHA-256**, pinned in a per-site record on
  the other side. TLS is mutual; the verifier ignores the chain and the name and accepts a peer
  iff its leaf SPKI hash is in the pin set. Expiry is checked with a one-day leeway either
  side, so ordinary clock skew does not break a link, and a skew beyond it is an error that
  names the skew.
* **Pairing is a human ceremony**: each side prints a bundle (site id, endpoints, certificate,
  fingerprint); the operator carries it to the other side and confirms the fingerprint by typing
  it back, as with SSH host keys. Nothing is trusted on first use silently.
* **Rotation without re-pairing**: the pin is the *key*, so a renewed certificate on the same key
  needs no action. A new key is introduced by importing it as a second pin before it is used,
  then retiring the first.
* **Authorization is separate from authentication.** Each remote-site record carries what that
  site may do (`may_push`, a byte quota). The listener serves only the replication operations in
  section 5; it has no attach, delete, claim or CQL. The target **chooses the local vdisk
  name** (`replica_vdisk_name`, deterministic, site-qualified) and never takes one from the
  wire, so a compromised or buggy source can fill its quota with immutable snapshots under its
  own prefix and can touch nothing else.
* **Revocation** is deleting the pin; every new connection checks it.

Rejected alongside cross-signing: a shared bearer token (replayable, needs a rotation story, a
secret on the wire); SSH or rsync (the fleet's SSH mesh is root on every node, so crossing sites
with it is root across sites); trusting the network (a VPN is welcome and orthogonal, but a
network position is not an identity).

## 3. What is shipped (D-29)

A manifest per snapshot:

```
snapshot, size_bytes, extent_bytes
rows:   [(extent_index, group, offset, length, vdisk_hash)]    sorted by extent_index
groups: [(id, length, seal)]     seal = "crc32c:xxxxxxxx" over bytes [0, length)
map_sha256                       over the canonical text of size, extent size and rows
```

Rows are exactly `dfs_block_map` rows, `vdisk_hash` signed as Hydra stores it, because the
footer inside each extent was stamped with the *writing* vdisk's identity and the bytes cannot
change. A snapshot with a row naming an `extent_id` (D-23 stage 3, not built) is refused for now.

**The delta is against what the target holds, not against a previous snapshot.** The target is
asked about every group of the manifest and answers per group: *complete* (same id, length and
seal), *absent*, *partial* (some staged bytes), or *conflict* (same id, different content). Only
the absent and partial ones are sent. This works without a shared base because groups are
immutable and snapshots share them: a later snapshot references mostly the groups an earlier one
did, so what is missing is what was written since, plus the group that was open. A deleted
earlier snapshot, a first replication and a re-replication after the remote lost data are the
same operation.

**Whole groups, not extents.** A sealed group may hold dead extents the guest has since
overwritten; shipping it ships that garbage. Shipping only live extents would change offsets,
break byte identity and make the remote a different file; the cost is waste proportional to
churn, to be measured and paid by compaction later if it matters. Groups are sent as stored,
so compressed groups are small on the wire.

**Open groups.** The group a vdisk is currently appending to is `open`: no seal hash, still
growing. Its bytes below the highest referenced offset are immutable and durable (the map is
written after the bytes are synced), so the manifest ships that **prefix** and names it
`<id>~<length>` on the target, where it is a complete, sealed, immutable file of its own. A later
snapshot that references the same group grown or sealed is a different target group and re-sends
the prefix: at most one group per vdisk per snapshot, a few MiB. Seeding the new file from the
old prefix locally is an optimisation not worth its state.

**Cost of asking.** The have-query lists every group: about 100 bytes each, 26 MB for a
terabyte, streamed. A cheaper "only groups newer than the last delivered set" shortcut is unsafe,
because the target's own retention may have reclaimed the old ones, so it is not taken.

## 4. How a transfer completes, resumes, and never shows half a snapshot (D-30)

```
offered -> negotiating -> sending -> publishing -> visible            (or failed / paused)
```

1. **Offer.** The source offers a manifest. The key is (source site, snapshot, `map_sha256`). The
   target answers *already present* (visible with the same digest: nothing to do), *busy* (a job
   for the key exists: join it), *conflict* (the name exists with a different digest: refuse; a
   source snapshot's name is immutable, so a different manifest means deletion and recreation
   or corruption, and the target never overwrites), or *proceed* with the have-list.
2. **Space preflight.** The target sums what it still needs and refuses before any byte flows if
   it does not fit with a reserve. Replication never deletes target data to make room.
3. **Send.** Each missing group is read whole at the source and *verified before the first byte
   leaves*: every referenced extent's footer (crc, vdisk hash, index) and, for sealed groups,
   the group's seal hash against Hydra's. A group that fails is not sent, and the job fails
   naming it. Frames carry a per-chunk crc32c; the group ends with a SHA-256 of what was sent.
4. **Stage and verify.** The target writes to `staging/<job>/<id>.part`. At the end of a group it
   checks length, SHA-256, the seal hash, and **independently** every referenced extent's footer
   against the row that points at it: it does not trust the sender. Only then is the file renamed
   into the extent store with its metadata. Any mismatch deletes the `.part`; the group restarts
   from zero once.
5. **Publish.** Only when every group is installed: the vdisk row is written in class `forming`
   (nothing attaches it, and listings of replicas show only `immutable`), the map rows follow in
   batches, the rows are **read back and their `map_sha256` recomputed** (a lost write is
   caught here, not by a guest), and then one compare-and-swap flips `forming` to `immutable`.
   A snapshot is visible if and only if that flip happened. A crash anywhere before it leaves a
   `forming` row, which is a statement of what it is; re-running publish rewrites the same rows.
   A set of several vdisks becomes visible as a set only after every member is.
6. **Resume.** After a drop, the source re-negotiates. A partial group reports its staged length
   rounded down to a chunk boundary; the target truncates to it and the source resumes from
   there. Resume is an optimisation, never a trust: the end-of-group digest verifies the whole
   file, so a torn tail is caught and costs one restart of that group.

**Installed but unpublished groups** are orphans to the target's existing mark-sweep, and are
reclaimed after its two-scan grace. That makes a failed job cost space, never correctness, but
it has a sharp edge: Purah's grace is 600 seconds by default, a long transfer can outlast it, and
a group installed early could be reclaimed before the map that references it is published. The
design answer is that a job registers its group ids as roots for Purah's mark phase; that is a
change to `purah.rs`, which this work does not make, and it is the first thing to settle before
the transport is built. Publishing each vdisk as soon as its own groups are in narrows the
window meanwhile.

### Failure modes

| Failure | What happens |
| :-- | :-- |
| The link drops mid-transfer | Staged bytes stay. The job is `paused`; the next attempt resumes from the chunk boundary. Nothing is visible. |
| Torn staged tail after a crash | Caught by the end-of-group digest; that group restarts. |
| A byte flips in transit | The chunk crc fails; the stream is treated as desynchronised and dropped, as in `peer.rs`. |
| Source data is already corrupt | Caught by the exporter's footer and seal checks; the group is not sent and the job fails naming it. |
| The target runs out of space | Before sending: refused with the numbers. Mid-transfer: `paused: no space`, staging kept, resumes when space exists. Stale staging is reaped after 48 hours. |
| The same snapshot is offered twice | Same digest: *already present* or *busy*, no work. Different digest: *conflict*, hard refusal. |
| The source node dies mid-export | Any node holding the group yields identical bytes, and the SHA-256 proves it. The job resumes from another. |
| The target node dies mid-import | Its staging is lost; the job state is in Hydra and another node restarts from the have-list. |
| Clocks disagree | Nothing in the protocol is ordered by wall time. Sets are identified by id and digest, never compared by timestamp across sites. Certificate expiry has a one-day leeway and a larger skew is reported as skew. RPO is measured by the source against its own clock. |
| A site is compromised | It can fill its quota with immutable replicas under its own prefix. It cannot name a vdisk, attach, delete or read. |

## 5. Bandwidth (D-31)

A per-site rate limit applied at the **sender** as a token bucket over payload bytes, with a
burst of one chunk; the target may impose its own accept rate by simply reading slower, which
TCP turns into back-pressure. One transfer per source node at a time, so replication cannot starve
guest I/O on the local disks. Time-of-day windows are designed (a list of rate overrides per hour
of week) and not built.

## 6. The operations

Between sites, over the pinned listener, and only these:

```
offer(manifest)        -> already_present | busy | conflict | proceed(have, need_bytes)
group(id, ...)         -> a framed stream (begin, chunks, end), many groups per connection
publish(snapshot)      -> visible | refused(reason)
status(job)            -> state, bytes, last error
```

The listener runs in Sidon, so bytes never pass through Python (D-20's rule). The Rauru daemon
decides *what* to replicate and *when*; Sidon moves it.

## 7. State the daemon keeps

Not built, and deliberately without migrations: two tables would sit empty on every production
cluster until a second site exists, which is the pattern migration `0025` cleaned up. When the
first real link is built they are created together with the code that exercises them, in the ids
reserved for this work (`0033`, `0034`):

```
dfs_remote_sites       site_id PK; name, endpoints, pinned_spki (set), may_push,
                       quota_bytes, bandwidth_bps, enabled
dfs_replication_sets   ((site_id), set_id): domain, state, map digest per snapshot,
                       bytes_sent, attempts, last_error, updated_at_ms
```

`rauru_replication.StateStore` is the interface; the tests use an in-memory one.

## 8. Disaster-recovery orchestration: the decision logic is built, the driver is not

`rauru_failover.py` is the state machine below as a pure function: `step(state, event)` returns the
next state and the *actions* a driver would carry out (clone members, rebuild the definition, start,
stop, reverse-replicate, final delta, verify), or refuses with a reason. It performs nothing, and no
driver exists, because a driver cannot be exercised without a second site and one written against
fakes would be fiction. What is pinned is what can be wrong without a second site: the order of
the steps (failback stops the guest, ships the last changes and proves they are visible before it
starts anywhere), and the refusals (nothing automatic; no failover while the original site reports
the guest running; an unreachable site needs the risk accepted by name; both sites running the guest
is a `split_brain` state that only an explicit `resolve` leaves). The states are `replicating`,
`running_at_target`, `failback_syncing`, `failback_ready`, `cutting_over`, `split_brain`.
What the design commits to, so the replication above does not foreclose it:

* A replica is an ordinary immutable vdisk under a site-qualified name, with provenance in the
  state table. **Failover** is a clone of each member (a map copy, zero bytes), a VM definition
  rebuilt from the set's recorded members, and a start; the replica itself is never made writable.
* The replicated **set** is the unit, and its recorded consistency (`crash:domain` and so on)
  travels with it, so a failover knows what it is promising.
* A VM definition is not in the data being replicated. Replicating VM metadata and networks is its
  own design.
* **Failback** is replication in the other direction of what changed while running at the
  target, and is the same machinery with the roles swapped.
* Split-brain (both sites running the guest) cannot be prevented by software that can only reach
  one site; it needs a human decision or a third witness, and the design should say so rather than
  hide it.

## 9. What is built, and how much of it was exercised

In `sidon/src/replicate*` (32 tests) and `rauru_replication.py` (37 tests). **Everything below ran only
against two directories on one machine standing in for two sites**, with real extent groups made
by the real `EgroupStore`:

| Piece | Where |
| :-- | :-- |
| Manifest, canonical map digest, validation | `replicate.rs`, `rauru_replication.py` (a shared digest vector) |
| Frames with per-chunk crc, the group stream | `replicate.rs` |
| Export: verify then stream, throttled | `replicate/export.rs`, `replicate/throttle.rs` |
| Import: preflight, stage, verify, install; atomic publish through a sink | `replicate/import.rs` |
| Delta planning, job state machine, resumable driver | `rauru_replication.py` |

Built afterwards, each tested only in isolation and called by nothing:

| Piece | Where | What was exercised |
| :-- | :-- | :-- |
| Pinned-key verifier for both ends of a connection (SPKI SHA-256 pin set, one-day leeway, skew reported as skew, handshake signature still verified) | `sidon/src/replicate/site_tls.rs` (11 tests) | A loopback TLS connection between throwaway certificates made with the `openssl` command: both pinned works; an unpinned server is refused by the dialler; an unpinned or anonymous client is refused by the listener; a certificate renewed on the same key still works; a second pin works during an overlap; a revoked pin refuses the next connection; the fingerprint equals openssl's own; mutating the pin check makes four tests fail |
| Hydra-backed `MapSink`: vdisk row `forming`, map in batches, read back, one class compare-and-swap to `immutable` | `sidon/src/replicate/hydra_sink.rs` (8 tests) | An in-memory Hydra that understands exactly the statements and endpoints involved (`block_map_batches`, `/v1/dfs/vdisk-create`, `/v1/dfs/vdisk-class`); **no real Scylla** |
| The decision logic of failover and failback | `rauru_failover.py` (17 tests) | Pure function; no effects |

The verifier is in Rust because the listener is (D-20's rule that bytes do not pass through
Python), and because Python's `ssl` cannot ask a peer for a certificate without also validating its
chain, which is exactly what a pin replaces.

Not built: the listener and the wire operations that use these configs, the `GroupStore` over
`EgroupStore` that registers groups in `dfs_egroups`, the Purah root registration, the state
tables, pairing, site certificate issuance, any scheduling, and the driver that feeds
`rauru_failover.step` and carries out its actions.
