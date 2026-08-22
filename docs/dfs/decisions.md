# Decisions

The ADR list. Every entry names the alternatives it beat and the reasoning, because a
decision whose alternatives are forgotten gets relitigated by whoever joins next — or
silently reversed by whoever implements next.

**D-1 — Build, not adopt.** Alternatives: Ceph RBD (removes the per-device model without
writing a storage system; MON/MGR/OSD operational weight; a second cluster to operate
inside the first), stay on LINSTOR with a widened port range (sufficient below ~1k
volumes; the shape remains device-per-connection). Decided by the owner with the trade
stated plainly. **And decided as a replacement, not an addition**: LINSTOR and DRBD are
removed once their disks are moved, because the cost being escaped is operating a storage
layer, and operating two is worse than operating either. The port-range widening survives
only as an interim measure for whatever runs before the cutover.

**D-2 — Rust for everything on the byte path.** Alternatives: C (the safety burden is
the whole objection), Go (GC pauses tolerable, but the io_uring/cgo story and the
absence of any Go in this repo both count against), Python (control plane only, never
bytes). Precedent: Agahnim. Verified: rustc/cargo 1.92 already on every node via the
existing provisioning package set, so the build-on-node path exists.

**D-3 — qemu attaches over NBD on a unix socket (v1).** Alternatives: iSCSI to
localhost (what Nutanix ships; drags in a whole target stack for no v1 gain),
vhost-user-blk (the v2 performance path — shared-memory rings; qemu 10.1 supports it,
adopt in milestone 9), ublk (kernel 6.12 has it; a real /dev node, but a kernel
dependency NBD avoids). NBD is qemu-native, needs no kernel module, and keeps v1 purely
in userspace. Verified: qemu 10.1.0 on the reference node.

**D-4 — Append-only egroups; sealed means immutable; redirect-on-write.** Alternative:
overwrite-in-place (no GC needed, but open files whose replicas can diverge in the
middle, and repair protocols for that divergence). Immutability makes repair a checksum
comparison, snapshots a map copy, and scrub lock-free. The price is GC, which is a
performance problem; divergence repair is a correctness problem; pay in performance.

**D-5 — The journal is write-all / read-one, not quorum.** The takeover proof
([ownership.md §3](./ownership.md)) is three lines *because* fencing one replica stops
the old owner and reading one replica sees every ack. Quorum journals buy availability
during single-replica loss and cost exactly that proof. DRBD protocol C users already
accept this trade, so it regresses nobody.

**D-6 — Ownership is a Hydra CAS plus replica-side epoch fencing; the lease is not the
safety mechanism.** Alternatives: lease-only (dies on clock skew and slow watchers —
the SIGSTOP scenario), per-vdisk Raft (a consensus instance per disk; operational
insanity at thousands), ZooKeeper ephemerals for ownership (couples the data path's
safety to ZK session semantics and adds a second authority beside Hydra's CAS). Replicas
rejecting stale epochs is the one mechanism that works when the deposed node is wedged,
lying, or living in the past.

**D-7 — Block-map writes are plain QUORUM, not LWT; drains commit through one
`drain_seq` CAS per batch.** Reasoning and the rejected reconciliation-on-takeover
alternative in [metadata.md §3–4](./metadata.md). The inviolable rule underneath both:
guest acknowledgement never waits on Hydra.

**D-8 — GC is Purah's mark-sweep with a two-scan grace; no reference counts, ever.**
Distributed refcounts are a standing bug class (every crash between data-op and
count-op; every clone doubling). The schema is forbidden a refcount column by
[metadata.md §1](./metadata.md); slow reclaim is the accepted price.

**D-9 — CRC32C, 32 KiB slices, algorithm byte in the footer.** Detection is the job,
not authentication; CRC32C is hardware-accelerated everywhere this runs; the algorithm
byte makes "stronger later" a value change, not a format migration.

**D-10 — RF comes from the existing `storage_containers.ftt` column (RF = ftt+1).** No
parallel policy object; the concept operators already have keeps meaning what it meant.

**D-11 — One data port, 9105, mTLS with the cluster CA and per-node IP-SAN certs.**
Reserved here; lands in `network.md` at implementation. Peer connections are per
node-pair — the head count that was the whole complaint about the old substrate.

**D-12 — Immutable image class instead of any multi-writer mode.** Replaces
`allow-two-primaries` with a category that cannot express the hazard. General
multi-writer vdisks are refused at attach, indefinitely.

**D-13 — Egroups are files on XFS on a thin LV in `vg_aether`.** Alternatives: raw
block management (reinventing an allocator to save a filesystem's overhead), LVM-per-
egroup (metadata churn at 4 MiB granularity). Verified: the thin pool already owns the
whole VG, so both substrates coexist without repartitioning; the capacity-accounting
caveat is recorded in [architecture.md §4](./architecture.md) and lands on the readers
in milestone 7.

**D-14 — No Scylla fork.** The recorded answer to "customise Scylla like Nutanix did
Cassandra": Nutanix patched a 2011 Cassandra that lacked what they needed; Scylla 5.4
has LWT, and Helios's discipline lives in Daruk — around the database, not in it. A
C++ database fork is a permanent maintenance tax with no capability it buys here.

**D-15 — Harness before filesystem, calibrated on DRBD.** The methodological spine:
a harness debugged against the code it gates learns that code's blind spots. Ganon is
validated against a substrate known to be correct, gains standalone value attacking the
shipping product, and thereafter outranks the schedule ([milestones.md](./milestones.md),
final rule).

**D-16 — Names: Sidon, Purah, Ganon.** Sidon (data path — Mipha's sibling, as HA and
storage fencing genuinely are here), Purah (the scanner), Ganon (the recurring calamity
the kingdom prepares for). Rejected: Revali — completes the champions set but greps as a
substring of `revalidate`, which already appears in `spectrum_server.py` — and a name
you cannot grep for cleanly is a name that costs debugging time on every future search.
(The collision is with `revalidate`, not with `vali` itself; `vali` and `revali` are
distinct tokens.) **Aether is not renamed**: it keeps its name, its documents and its
meaning as the Linstor/DRBD substrate, and the two run side by side — reusing the name
would make every sentence in every older document ambiguous about which layer it meant.
`vg_aether` likewise stays, historical rather than descriptive, because renaming a
volume group under live data to fix an aesthetic is not a trade worth making. The CLI is
deliberately unnamed until milestone 7 rather than named badly now.

**D-17 — A single node is a supported topology, not a stepping stone.** The alternative,
adopted by most distributed stores, is to treat ftt=0 as a development mode: correctness
arguments assume peers, the single-node path is the one nobody soaks, and the smallest
deployment gets the least-tested code. Rejected because the reference cluster *is*
single-node and many installs will never be otherwise. Concretely this commits three
things: epoch fencing is never conditional on peer count (at RF1 it fences the previous
*process*, which is a real hazard, not a formality); ftt=0's durability limit is stated in
the invariant rather than hidden in a footnote; and Ganon gets a single-node soak tier
that runs because the configuration ships, not because the lab is small. See
[architecture.md §5](./architecture.md) and [ganon.md §5](./ganon.md).
