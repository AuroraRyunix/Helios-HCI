# vhost-user-blk beside NBD

A design, not a plan to build. [decisions.md](./decisions.md) D-25 records the verdict and
this document holds the reasoning and the measurements that would change it.

**The verdict in one paragraph.** Do not build it until a benchmark shows the NBD transport
is what limits a guest. Reading `sidon/src/nbd.rs` and the path behind it, the transport is
almost certainly not the limit: a request costs on the order of ten microseconds of socket
work, and it is then handed to a path that costs hundreds -- a journal `fdatasync`, a
sequential round trip to each replica, or a 1 MiB read and checksum to serve 4 KiB. More
importantly, Sidon serves **one request at a time per connection**, so whatever queue depth
the guest runs, the data path sees one. Replacing the transport does not change that, and
the work that would -- concurrent requests into one vdisk -- is the work that touches the
journal's ordering rules, and it is needed *with or without* vhost-user. Do that first, over
NBD, which already pipelines. Then measure.

**Update.** The concurrency prerequisite named here is built, over NBD:
[group_commit.md](./group_commit.md). A connection now runs up to 32 requests at once and writes
that are in flight together share one journal sync and one replica round trip, so the "effective
queue depth is 1" and "no pipelining" findings below describe the code as it was when this was
written. What remains of the argument -- that the transport is a small part of a request, and that
a benchmark of NBD against a null backend should come before any vhost-user code -- is unchanged,
and is now answerable, because queue depth finally reaches the data path.

Everything numeric below that is not a measurement from this repository is labelled an
estimate. There are no benchmark results in the tree, and this document does not invent
any.

---

## 1. What NBD costs per request today

The path, from `nbd::serve`:

```
qemu NBD client ──unix socket──▶ BufReader::read (one syscall for header, usually the
                                  payload too for a 4 KiB write)
                                 allocate payload Vec (write) / result Vec (read)
                                 Backend::write | read   ← takes the vdisk Mutex
                                 BufWriter: header (+ data) , flush
                  ◀─────────────  one write syscall, or two for a reply over 8 KiB
```

What that adds up to, and what it is not:

- **Syscalls and wakeups.** One `read` and one or two `write`s per request, plus the wakeup
  of the thread blocked in `read` and of qemu's NBD reader on the other side. Estimate: on
  the order of ten microseconds of CPU and latency per request, spread over both processes.
  This is the cost vhost-user-blk can remove.
- **Copies.** A write is copied socket-to-`Vec`, then `Vec`-to-journal (`journal.append`),
  and again into `framed.to_vec()` for each replica. A read is built in a zeroed `Vec`,
  filled from an extent `Vec` (itself a copy of a `pread` buffer, plus a decode copy), and
  then copied into the socket. vhost-user removes the two socket copies and lets the backend
  read and write guest memory directly. It does **not** remove the journal, replica or
  extent copies, which are Sidon's own.
- **No pipelining.** `serve` reads a request, executes it, writes the reply, and only then
  reads the next. NBD itself permits many requests in flight on one connection (that is what
  the handle field is for) and qemu's client will issue them, but Sidon never has more than
  one in progress. The libvirt XML asks for `queues='N' iothread='1'` -- a multiqueue
  virtio-blk device with its own I/O thread -- and every one of those queues drains into one
  socket and then into one `Mutex<Vdisk>`. The effective queue depth is 1.
- **One connection at a time.** The accept loop in `control.rs::serve_socket` serves a
  connection inline, so a second connection to the same export waits for the first to end.
  The handshake does not advertise NBD's multi-connection flag either.

Set against what the request then does:

| Step | Cost, by construction | Where |
|---|---|---|
| Write: local journal `fdatasync` | the disk's sync latency | `journal.rs::append` |
| Write: per replica, sequentially | one TLS round trip plus the remote `fdatasync`, **in series** across replicas, behind one mutex-guarded connection per peer | `vdisk.rs::replicate`, `peer.rs::call` |
| Write: crossing the high-water mark | a whole drain, inline, while the vdisk mutex is held and the guest waits | `vdisk.rs::write` |
| Read of 4 KiB from drained data | a `File::open`, a 1 MiB `pread`, a CRC32C over all 1 MiB, a decode copy, then a 4 KiB copy out | `extent.rs::read_extent` |
| Read from the overlay | a journal `read_at` per segment | `vdisk.rs::read` |

Two consequences. First, for a write the transport is a small fraction of the latency floor
(about two `fdatasync`s and a network round trip at RF=2), so even a transport costing
nothing changes a guest's write latency by single-digit percent. Second, for a read of
drained data the dominant cost is amplification inside Sidon -- 256 times the bytes
requested for a random 4 KiB read -- which is a defect of the extent store's read granularity
and not a property of the transport.

Because the request loop is serial, throughput is bounded by `1 / latency` regardless of
transport: at a 1 ms write that is 1,000 IOPS from a guest asking for 32 in flight. That
ceiling is the first number the benchmark below measures, because it is the number that
decides whether any transport work is premature.

## 2. What vhost-user-blk would remove

The guest sees the same virtio-blk device. The virtqueues live in memory shared between
qemu and the backend, and the backend (Sidon) is notified through eventfds rather than
reading a stream:

- the per-request socket `read`/`write` pair, replaced by an eventfd kick and a used-ring
  update, and batchable (several descriptors per wakeup, polling for the hot case);
- the two socket copies, replaced by direct reads and writes of guest memory;
- qemu's block-layer and NBD client work per request, because qemu is no longer in the data
  path at all once the queues are set up;
- the single connection, replaced by N virtqueues, which is the part that only pays off if
  the backend actually runs them concurrently.

What it removes nothing of: the journal append, the `fdatasync`, the replica round trips,
the overlay, the extent read and its checksum, the vdisk mutex, the drain stall. Those are
the cost of the guarantees in [invariants.md](./invariants.md), and a transport swap leaves
every one of them in place.

## 3. What it could endanger

Each of these is an invariant or an ordering rule that the NBD path satisfies by being
simple. They are listed with what would have to be proven before anything shipped.

**Ack after journal (I-1).** I-1 defines acknowledgement as "the NBD reply left Sidon".
Under vhost-user the equivalent is the used-ring entry becoming visible to the guest. The
rule is unchanged -- publish it only after every replica has made the record durable -- but
the code that publishes it moves from a function that returns a `Result` to a completion
callback, and a completion path that can fire early is exactly the bug class this rule
exists to forbid. Any batching of used-ring updates must happen strictly after the
durability point of every request in the batch.

**Journal ordering and gap-free sequences (I-2, data-path.md §2).** Replicas refuse a
sequence gap. Today there is one writer thread per vdisk, so sequence order is arrival
order and the question never arises. Running several virtqueues concurrently means
assigning sequence numbers and shipping records to replicas in a single agreed order while
appends overlap -- a group-commit pipeline. That is the actual hazard in this work, and it
is *not specific to vhost-user*: it is the price of any queue depth above one, over NBD
too. It is the reordering the "deliberately last" note in `sidon.md` is warning about.

**Flush semantics.** Today `flush` is a no-op that is correct because every acknowledged
write is already durable. For virtio-blk the same is achieved by not advertising
`VIRTIO_BLK_F_FLUSH` and presenting the device as write-through (`config-wce` off), which
tells the guest there is nothing to flush. That is the simplest correct answer and it should
be the only one: advertising a write-back cache would invite the guest to rely on barriers
the backend would then have to implement.

**Epoch fencing (I-4).** The fence itself is replica-side and unchanged. What changes is
the local behaviour of a deposed owner: today every request after deposition returns EIO
through `degraded`, request by request. A vhost-user backend must do the same by completing
descriptors with `VIRTIO_BLK_S_IOERR`, and must not simply stop servicing the ring, because
a ring nobody services wedges the guest instead of failing it.

**Reconnect and replay.** A vhost-user connection is stateful. If Sidon restarts, qemu can
reconnect and, with in-flight tracking, resubmit descriptors the old backend had accepted
but not completed. A resubmitted write can land after later writes to the same range. That
is legal under I-2 (the write was never acknowledged, and the guest issued it concurrently
with its successors), but it is a behaviour Ganon would need a scenario for, because the
NBD path cannot produce it: a dead socket loses every in-flight request and qemu reports the
error.

**Shared guest memory (I-8, and a new trust boundary).** The backend maps the guest's RAM.
Sidon is a root daemon with a network listener; today a bug in it can corrupt storage, and
with vhost-user a bug in descriptor parsing can also write into guest memory. Every
descriptor address and length must be bounds-checked against the memory table, and
one-process-many-guests means one process holds mappings of unrelated VMs. A per-vdisk
worker process would contain that and would also fork Sidon's one-daemon structure, which is
a decision for the owner, not for this document.

**Live migration.** Helios removes migration's cutover instant by forwarding: the
destination's local Sidon serves the disk immediately and ownership follows (ownership.md).
With vhost-user-blk, qemu can migrate a guest whose disk is a vhost-user device only if the
backend supports dirty-page logging, because the backend writes read data straight into
guest memory and qemu must be told which pages it dirtied. A backend that does not log, on a
build that does not refuse, would migrate a guest with silently stale pages. Whether the
shipped qemu refuses is part of the verification in section 4.

**The `Backend` trait.** `read` returns an owned `Vec<u8>`. A zero-copy read needs a
`read_into(&mut [u8])` variant, and that is a change inside `vdisk.rs`, one of the files
under concurrent edit. It belongs in its own module and its own change, after the benchmark.

## 4. What qemu and libvirt need

What is established, and what is not:

| Question | Answer | Status |
|---|---|---|
| Does upstream QEMU 10.1 have the device? | Yes: `vhost-user-blk-pci`, plus `qemu-storage-daemon --export type=vhost-user-blk` as a reference backend. | Known upstream behaviour. |
| Is the reference node's QEMU 10.1.0? | Yes, recorded in D-3. | Verified earlier. |
| Is the device compiled into the *shipped* `qemu-kvm` on the EL10 nodes? | **Not established.** Red Hat's RHEL 10 virtualization documentation states that RHEL 10 does not support a user-space vHost interface. That is a statement about what Red Hat supports and not necessarily about what is built, and a Red Hat bug record shows `vhost-user-blk-pci` present in an earlier RHEL `qemu-kvm` build, but neither answers the question for this package. D-3's "qemu 10.1 supports it" was about upstream and should not have been read as this. | **Unverified.** |
| Does libvirt express it? | libvirt has a `<disk type='vhostuser'>` form with a `<source type='unix' .../>`, believed to be in libvirt 7.1 and later, with a reconnect option on the source. | **Unverified against the node's libvirt**; the documentation fetched while writing this was inconclusive. |
| Does it need shared guest memory? | Yes. qemu must back all guest RAM with shared memory (memfd or hugepages) so the backend can map it; in libvirt, `<memoryBacking>` with a memfd source and shared access. | Documented upstream. |

The check that settles the packaging question is read-only and takes a minute; it was not run
for this document because work on the cluster was restricted to building under `/tmp`:

```
/usr/libexec/qemu-kvm -device help 2>&1 | grep -i vhost-user
/usr/libexec/qemu-kvm -device vhost-user-blk-pci,help
rpm -q qemu-kvm libvirt; command -v qemu-storage-daemon
```

If `vhost-user-blk-pci` is absent, the question is closed for this fleet until the package
changes, and the rest of this document is moot. If it is present, the libvirt form still has
to be proven on a throwaway domain before anything is designed around it.

What attaching it costs the platform, beyond Sidon:

- **Guest memory must be shared-backed on every VM that uses it**, a change to `mipha`'s and
  `lanayru`'s domain XML and to anything that sizes or places memory. It is per-domain, so
  it can be opt-in, but a VM migrated between a vhost-user host and a NBD host changes shape.
- **qemu's block layer is out of the picture for that disk.** Anything implemented there --
  throttling, block jobs, qemu-side snapshots and mirrors, NBD export of a disk -- stops
  applying. Helios does storage movement in Sidon, so this is a short list today, and it
  must be checked against what the console and `valcli` promise before it is called short.
- **SELinux and the socket.** The backend socket needs a label qemu's domain may connect to,
  and the `chgrp qemu` arrangement `provision.py` makes for `nbd/` would need its twin.
- **A second code path in `helios_sidon.py`** emitting a different `<disk>` element, chosen
  per container, with NBD remaining the default and the fallback.

## 5. The benchmark plan

The aim is to be able to kill the work with numbers, so each stage has a stated result that
ends it.

**Stage 0 -- is the device even there.** The three commands in section 4. A no ends it.

**Stage 1 -- the transport ceiling, with no Sidon.** Export a null block device both ways
from the same process and drive each from a guest with `fio` (`direct=1`, `ioengine=libaio`
or `io_uring`):

- NBD over a unix socket from `qemu-storage-daemon` with a `null-co` blockdev;
- `vhost-user-blk` over a unix socket from the same daemon and blockdev.

If `qemu-storage-daemon` is not packaged, a stub `Backend` that returns zeroes behind the
existing `nbd::serve` is the NBD half, and a minimal vhost-user stub is the other half and is
therefore real work; in that case stop here and report that the comparison needs a build
decision before it can be made.

Matrix: 4 KiB random read and random write at queue depth 1, 8 and 32; `numjobs` 1 and 4;
128 KiB sequential at queue depth 8. Record IOPS, p50 / p99 / p99.9 completion latency, and
host CPU per I/O (`pidstat` on the backend and on the qemu process, divided by IOPS). This
bounds what a transport swap could ever give: the null-backend difference *is* the entire
prize.

**Stage 2 -- how far real Sidon is from that ceiling.** The same guest workloads against a
real vdisk over NBD, at RF=1 and RF=2, on the test cluster. Two numbers decide the work:

- *Queue-depth scaling.* IOPS at queue depth 1 versus 32. If they are equal, the serial
  request loop is the limit and no transport changes it.
- *The latency breakdown.* Time in `Backend::write` and `Backend::read` against time in
  `serve` outside them, from timestamps added around those calls in a scratch build. This
  is the measurement of "what the transport costs" that the estimate in section 1 stands
  in for.

**Stage 3 -- fix what stage 2 finds, over NBD.** If queue-depth scaling is flat, make the
backend handle concurrent requests (and advertise the multi-connection flag if that proves
useful) as its own change, with a Ganon scenario for the journal ordering in section 3. Then
repeat stage 2. This is not part of the vhost-user decision; it is the work that makes the
decision possible, and it is likely to take most of the available gain.

**Decision rule.** Proposed thresholds, for the owner to adjust before the numbers exist and
not after:

- **Do not build** if, on real Sidon after stage 3, the NBD transport accounts for under 20%
  of end-to-end p50 latency at queue depth 1 and the null-backend vhost-user ceiling is under
  1.5 times the real-Sidon IOPS at queue depth 32. The transport is not what limits the
  guest.
- **Worth designing in earnest** only if the null-backend vhost-user ceiling is at least 2
  times the null NBD ceiling *and* real Sidon over NBD is within 2 times of its own null
  NBD ceiling at queue depth 32, with transport CPU a double-digit share of host CPU per
  I/O. That combination says Sidon has caught up to the transport and the transport is now
  the next wall.
- Anything in between is a no for now and a re-measure after the next change to the data
  path.

If the answer is yes, the build starts with the `Backend` trait's zero-copy read, a
vhost-user frontend as a new module that only translates descriptors into `Backend` calls
and owns no ordering of its own, per-container opt-in, NBD as the default and the fallback,
and a Ganon adapter for the new transport before the first guest touches it -- the same
order that built Sidon: harness before data path.
