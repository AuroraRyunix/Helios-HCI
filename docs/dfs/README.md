# The Helios DFS — design documents

**Status: built and running.** Journal replication, replica-side epoch fencing, ownership
transfer, forwarding, extent replication with read repair, and Purah's re-replication,
reclamation and scrub are all implemented and exercised. Ganon is built and calibrated
against DRBD. LINSTOR and DRBD have been removed from the tree entirely.

Verified with several daemon instances on one host, which proves the protocol and the
state machine but not independence from a single machine -- the instances share a clock
and a page cache. Multi-host soaks are what settle that.

Snapshots and clones are built: a map copy sharing every extent with the parent, zero
bytes copied, and no reference counting anywhere -- see D-18 and D-19 in
[decisions.md](./decisions.md).

Replication is mutually authenticated against the cluster CA -- see D-20 in
[decisions.md](./decisions.md).

Not built: scheduled snapshots and rollback, compression, erasure coding, and
`vhost-user-blk`.

The documents remain the specification -- where code and document disagree, that is a bug
in one of them and the disagreement is the finding.

## What this is

A replacement storage substrate for VM disks: extent-based, replicated at the extent-group
level by a per-node data-path daemon, with the placement map held in Hydra through Daruk's
compare-and-swap discipline. The Nutanix analog is Stargate + Medusa, from their public
architecture documentation.

It exists because DRBD replicates *devices*: every replicated volume is a standing
connection between named peers, costing a TCP port, a kernel object, its own threads and
RF−1 connections per node. That is the right shape for a handful of HA volumes and the
wrong shape for one volume per VM disk — the visible symptom is the 191-port ceiling
recorded in TODO.md, but widening the range only moves the wall.

## Components and names

| Name | Role | Nutanix analog |
|---|---|---|
| **Sidon** | Per-node data-path daemon (Rust). qemu talks to the local Sidon, always; Sidon forwards to the vdisk owner when it is not the owner itself. | Stargate |
| **Hydra** (existing) | Holds the extent map — *which* extent group lives *where*. Never holds guest bytes. | Medusa |
| **Daruk** (existing) | The typed compare-and-swap gateway every metadata mutation goes through. | Medusa proxy |
| **Purah** | Background curator role inside Sidon, leader-elected: re-replication, mark-sweep GC, scrub. | Curator |
| **Ganon** | The fault-injection harness. Its job is to be the recurring calamity the cluster claims to survive, and it is built and trusted *before* any data-path code exists. | — |

Sidon is Mipha's brother in the source material, which is apt rather than cute: HA
failover and the storage data path are genuinely siblings here — fencing becomes a
property of the data path itself (see [ownership.md](./ownership.md)).

**Sidon, Purah and Ganon are settled** — these are the names, they are what the code will be called, and later documents may use them without hedging. Three further questions:

- *Aether keeps its name and its meaning.* It is the Linstor/DRBD substrate, and it is not renamed into this. The two run side by side through the migration and Aether stays afterwards for anything that chooses it, exactly as the GlusterFS-to-Aether transition went. Reusing the name would have made every sentence in every older document ambiguous about which storage layer it meant.
- *The volume group stays `vg_aether`.* Both substrates carve from the same thin pool, so the name is now historical rather than descriptive. Renaming a VG under live data to fix a naming aesthetic is not a trade worth making; the capacity consequence is real and is handled in [architecture.md](./architecture.md) §4.
- *The CLI has no name yet.* The pattern is `valcli` / `mcli` / `catcli`, and none of the obvious derivations are good. It is not needed until milestone 7, so it is left blank rather than filled in badly — an unnamed slot is honest; a placeholder that leaks into code is not.

## Read these in order

1. [architecture.md](./architecture.md) — the shape, the data model, what is deliberately out.
2. [invariants.md](./invariants.md) — the contract. Everything else exists to satisfy this file.
3. [data-path.md](./data-path.md) — journal, drain, extent store, checksums, GC.
4. [ownership.md](./ownership.md) — leases, epochs, fencing, live migration.
5. [metadata.md](./metadata.md) — schema, Daruk endpoints, exactly-once drain, load arithmetic.
6. [ganon.md](./ganon.md) — the harness, and why it runs against DRBD first.
7. [milestones.md](./milestones.md) — build order, gates, and what each step is worth if abandoned.
8. [decisions.md](./decisions.md) — the ADR list: every choice, its alternatives, and why.

## The one-paragraph version

A vDisk is cut into 1 MiB extents living in 4 MiB append-only extent groups, replicated
`ftt+1` ways as plain files on each node's local filesystem. Guest writes land first in a
per-vdisk replicated journal (write-all, epoch-fenced) and are acknowledged; a background
drain coalesces them into extent groups and commits the map to Hydra in batches, one
lightweight transaction per batch. Exactly one Sidon owns a vdisk at a time, held as a
lease with a monotonically increasing epoch, and replicas reject writes from stale epochs
— which is what makes split-brain a *rejected request* instead of a corrupted disk.
Everything the map claims is durable before the map claims it.
