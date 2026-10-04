# Vali (VM Manager & Scheduler Service)

Vali is the standalone VM management, placement scheduling, and DRS (load balancing) coordinator for the HCI cluster. It is the direct equivalent of Nutanix **Acropolis (AHV VM Management)**.

> [!NOTE]
> **Name Origin:** A dual-purpose name:
> 1. In Norse mythology, **Váli** is a son of Odin destined to survive Ragnarok and avenge the death of his brother, representing how Vali avenges host resource imbalances by dynamically migrating and scheduling virtual machines.
> 2. It is also short for **Revali**, the Rito Champion in *The Legend of Zelda: Breath of the Wild* known for *Revali's Gale* (an upward draft that launches the hero into the air), representing the dynamic flight, placement, and live migration of virtual machines across hypervisor nodes.

## Architecture & Lifecycle
- **Daemon Service**: Runs as a standalone python service (`/usr/local/bin/vali`) listening locally on port `9095`. Managed by systemd (`vali.service`).
- **Leader Election**: There are two, one per job, and they are application-level. Vali stands for `vali-queue` (draining Catalyst's `vali` queue) and for `vali-drs` (the rebalancing pass) as separate candidacies: an ephemeral sequential ballot under `/helios/leaders/<service>`, lowest counter wins. They are deliberately two, because there is no reason the node draining the queue should also be the node running DRS.
  There used to be no election at all. Vali asked which node the **ZooKeeper ensemble itself** had elected (by sending `stat` to each host's port 2181) and compared that address with its own — so an ensemble election, which is a restart or a blip or a rolling upgrade, moved both of these and every other leader-only workload in the cluster onto one node at once. See [service_leadership.md](./service_leadership.md).
- **Knowing its own address**: Vali reads `LOCAL_HYPERVISOR_IP` from `/etc/hci/spectrum/spectrum.env` and falls back to `127.0.0.1` when the key is absent. What that costs has changed. A ballot does not need to know its own address to be the lowest one, so such a node *can* now win — what it loses instead is the ability to be **found**, because the address it publishes in its ballot is how a submitter resolves the queue holder. (Before the candidacies it could never match the comparison at all, so it never became the worker, and leadership landing there stopped every VM power, migrate and DRS task in the cluster — each returning as a timeout, which reads as a slow cluster rather than a broken one.) Vali still warns loudly on startup if it has no address, and logs each time it takes or gives up the worker role. See [cluster.md](./cluster.md#why-identity-comes-first).
- **Autostart Constraint**: Vali is a static systemd service that is dynamically started/stopped by Spark commands (`cluster start` / `cluster stop`) and does not auto-start on boot unless the cluster is online.

## Database Schema
Vali relies on a task queue table in ScyllaDB (`hydra` keyspace):
```sql
CREATE TABLE IF NOT EXISTS hydra.vali_tasks (
    task_id uuid PRIMARY KEY,
    vm_name text,
    action text,         -- 'start', 'stop', 'reboot', 'shutdown', 'reset', 'migrate'
    status text,         -- 'pending', 'processing', 'completed', 'failed'
    target_host text,    -- target IP for migration or explicit start (optional)
    created_at bigint,
    updated_at bigint,
    error_msg text
);
```

## Communication Routing & Security
To keep the Spectrum container boundaries secure, Spectrum is not allowed to communicate directly with Vali. Instead, all actions are routed as follows:
1. Spectrum calls the local `spark-daemon` on `127.0.0.1:9099` via mTLS.
2. The local `spark-daemon` forwards the request locally to `vali` on `127.0.0.1:9095`.
3. Vali executes the task and returns a synchronous response.

> **`hydra.vali_tasks` is vestigial.** The table is created and nothing ever writes
> to it. Dispatch is Catalyst's in-process `queue.Queue`, which does not survive a
> Catalyst restart -- a task accepted and not yet run is lost rather than resumed.
> The table is kept rather than dropped because `valcli`'s cleanup reads it and
> `mcli` checks it exists; both simply always find it empty.

```
[ Spectrum Container ] 
       │ (Secure mTLS)
       ▼
[ spark-daemon (Port 9099) ] (Local Host Daemon)
       │ (Local Forwarding)
       ▼
[ Vali Daemon (Port 9095) ] (Local Host Daemon)
```

## VM Placement & Scheduling (Task Processing)
When the Vali Leader processes a `start` task from the queue:
1. It queries available memory across all online nodes in the cluster.
2. It filters out nodes without sufficient memory to accommodate the VM configuration.
3. It selects the candidate node with the least used memory (dynamic scheduling).
4. **It claims the VM for that node** through Daruk's `POST /v1/vm/claim`, which writes
   `state = 'Running'` and `host_ip` only if the row still holds the placement Vali read
   when it decided to start. A VM another host already owns is refused here, and the error
   names that host.
5. It compiles the VM's XML and calls the target node's `spark-daemon` `/api/v1/execute` to define and start the VM.
6. If the vdisk attach or the start itself fails, the claim is given back — conditionally,
   so a VM something else has taken in the meantime is left alone, and any vdisk already
   attached on this attempt is detached again.

### Placement is a compare-and-swap, not a write

The claim used to be the *last* step of a start: the resources were promoted, the domain
defined and booted, and only then was `host_ip` written unconditionally. Two callers racing
on the same VM — a manual start against a DRS placement, or a start issued while a stale
`host_ip` was still in the row — both reached that point and both wrote, so two qemu
processes ended up on the same disk.

Claiming first means one of them is turned away before anything touches the disks. The
vdisk attach that follows is the second gate, not the only one — and it is the stronger of
the two: attaching wins the ownership compare-and-swap in Hydra and fences every journal
replica at the new epoch, so a previous owner that is wedged, lying about its state, or
unreachable cannot complete another write. `drbdadm primary` could only *infer* that the
peer had stopped, and only where quorum was armed. A refused attach names the host that
actually holds the disk.

The same applies in reverse: `stop` releases the placement conditionally on Vali still being
the host of record. An unconditional release frees the row of a VM a concurrent migration had
already moved, and the next start boots a second copy of a guest that is still running.

> [!NOTE]
> These calls go to Daruk's typed endpoints and have **no `cqlsh` fallback**. That fallback
> can only run statement text and cannot report whether a condition held; an ownership write
> that cannot be made conditional must not be made at all. A start with Daruk down is
> refused, loudly.

## Distributed Resource Scheduler (DRS)
The Vali Leader runs a periodic DRS loop (every 30 seconds):
1. **Load Evaluation**: It checks memory utilization percentages across all active hypervisor nodes.
2. **Overload Trigger**: A host is considered overloaded if its memory usage exceeds `85%` or if its usage is more than `15%` higher than the average cluster node utilization.
3. **Rebalancing Action**: If an overloaded node is detected, Vali selects a running VM on that host and queues a `migrate` task to live-migrate it to the node with the highest available memory.
4. **Live Migration**: Vali executes live migrations via libvirt:
   `virsh -c qemu:///system migrate --live --persistent --undefinesource <vm_name> qemu+ssh://root@<target_ip>/system tcp://<target_ip>` (after the storage steps below; `--unsafe` is not used)
    And updates the VM's `host_ip` in ScyllaDB on completion. To enable compatibility during live migrations, VM guest CPUs are defined with `<cpu mode='host-model'/>` when running under KVM.

### One path for moving a guest

Manual migration, DRS, evacuation (maintenance) and the HA restart all end in a migrate or a start, so
they share what makes a move safe:

* **Where it may land** — `host_ineligible_reason(ip)`: the host's recorded state is `NORMAL` (not
  DEGRADED, FENCED, DOWN, RECOVERING or any maintenance state) *and* it answers with every managed
  service UP and not in maintenance. Starts (HA restart, evacuation's start fallback) choose their host
  with `select_best_start_host`, which applies the same two tests; migrations apply them to the named
  target, then the memory gate (the guest's RAM against the target's free memory) and the storage gate (the
  guest's disk against the target's free extent-store space, refused when either is unknown).
* **One migration per guest** — the `status` lock (`/v1/vm/migrate-lock`), taken before anything is touched
  and released on every path.
* **Not too often** — DRS acts only when the load deviation exceeds 0.15 and a move improves it by 0.03,
  at most one move per 300 s *cluster-wide*, and never picks a guest that moved in the last 30 minutes. Both
  limits read `hydra.vali_drs_history`, which every successful migration writes (manual and evacuation moves
  included), so a Vali restart or a change of leader does not forget them. An "aggressive" run is the
  operator's explicit request and ignores the history.
* **Visible** — a migration is a Catalyst task; its error names the step that failed (see below).

Not unified: the "fallback to any online host" in `select_best_start_host` returns the first healthy host when
no host has the memory a guest needs (an overcommit); whether that is wanted is a decision, recorded in
TODO.md. CPU and accelerator compatibility between hosts is left to libvirt's own refusal at `virsh migrate`.

### What a migration does to the storage

The destination qemu opens an NBD socket per disk as the migration starts, so the destination's
Sidon must be serving those sockets before `virsh migrate` runs. (Until this was wired the migration
attached nothing and every `host.maintenance.enter`, evacuation and DRS move failed with
`Failed to connect ... .sock: No such file or directory`; the start and HA paths attached before
they defined the domain and the migration did not.) The order is:

1. **pre-clean** the stale definition on the target and restore the NVRAM;
2. **attach** each disk the domain XML names on the target (`domain_vdisk_ids`, the rule
   `generate_vm_xml` uses): data disks in forwarding mode, so the writer stays the source; image
   (CD-ROM) vdisks ordinarily;
3. **`virsh migrate`**, with a long timeout (the daemon's default of 45 s would kill any migration
   that takes longer to copy memory);
4. **commit** the placement and drop the lock, conditionally (`/v1/vm/migrate-commit`);
5. **takeover** on the target for each data disk (Sidon: stall, release by the old owner, claim and
   fence, install — [dfs/ownership.md §5](./dfs/ownership.md)), retried three times.

Failure, by step; every error message names the step:

| Fails at | Left behind | What vali does |
|---|---|---|
| attach | nothing on the source; earlier forwarders on the target | detaches the forwarders it created, aborts, releases the lock |
| virsh migrate | forwarders on the target; the source owns every disk and no epoch moved | detaches the forwarders it created (never images — they are shared), releases the lock |
| commit | the guest runs on the target, the row says the source | reports it as a record to repair, not a failed migration |
| takeover | the guest runs on the target; its disks are served through the forwarder, or fail until retried | the task fails with `step 'takeover'`, the reason, and `valcli storage.takeover <vdisk> <node>` (safe to repeat) |

An attach that would make the target the *owner* (Hydra names no other owner) is refused: the guest
is running on the source, and taking the disk would fence the source out.

Not a storage matter, but the same boundary: cross-host live migration also needs the two hosts to
agree on CPU and accelerator. The lab's `.41` has KVM and `.42`/`.43` are nested TCG, so a migration
between them can be refused by libvirt for that reason; that is separate from the storage steps above
and is reported by libvirt's own error.

Restarting a node's Sidon removes the sockets of the disks it serves (it does not re-attach on start):
move or stop every VM on a node before restarting its Sidon.

### The migration lock

The `status` column on `hydra.vms` holds a transient lifecycle lock (`migrating`), distinct
from `state` (`Running`/`Stopped`). It is taken through Daruk's `POST /v1/vm/migrate-lock`,
which is `UPDATE ... SET status = 'migrating' ... IF status != 'migrating'` — the condition
and the write are one Paxos round.

This replaces a read of `status` followed by a separate write, which two callers could both
pass. That mattered because live migration used to be the window in which DRBD
dual-primary was open, and two concurrent migrations of one VM was disk corruption. The
corruption is gone — a vdisk has one owner per epoch and a deposed owner's writes are
refused by every replica — but the lock is not redundant, because what it now prevents is
two migrations racing each other's ownership CAS and leaving a VM defined on a host that
does not own its disk. A failed lock aborts the migration; it is never logged and stepped
over.

On success the hand-over runs through `POST /v1/vm/migrate-commit`, which moves `host_ip` to
the target *and* clears the lock in a single round, conditional on both the source host and
the lock still belonging to this migration. On failure the lock is released conditionally,
so a late cleanup from a failed attempt cannot unlock a migration that has since started.

A null `status` satisfies `!= 'migrating'`, which matters because `status` is null for every
VM that has never migrated — the common case is the null case. This is verified against a
live Scylla; see [docs/daruk_technical.md](./daruk_technical.md).

> [!NOTE]
> The "is it running?" gate reads the `state` column. It previously read `status` — the lock
> column — and compared it to `"running"`, so it raised `AttributeError` on the `None` of a
> VM that had never migrated, and every first migration failed with
> `'NoneType' object has no attribute 'lower'` reported as the task error.

## VM Display and Video Configuration Standards
To ensure compatibility across all hypervisor nodes:
- **Video Model**: VMs default to the high-performance **VirtIO** graphics adapter with VGA compatibility (`<model type='virtio' heads='1' primary='yes' vga='on'/>`) for both UEFI and BIOS virtual machines to utilize hardware-accelerated guest OS graphics rendering (such as WDDM drivers). Other video drivers like `qxl` are avoided because of missing BIOS ROM files on standard EL 10.2 hypervisor repositories.
- **Console Protocol**: Chosen per VM and recorded in `hydra.vms.graphics`; null means VNC, so every VM predating the column keeps the console it had. VNC is the default and SPICE is opt-in. Exactly one `<graphics>` device is emitted -- this entry previously claimed both protocols ran at once, which no builder has ever done, and two graphics servers per guest would double a listening surface that takes no password in order to save a restart. Because the device is part of the domain definition, switching a VM's console takes effect on its next start, not on save. A SPICE VM also gets a VirtIO-serial `spicevmc` channel targeting `com.redhat.spice.0`, and keeps the VirtIO video adapter for the reason in the entry above. Image compression is set to `off` rather than the `lz` this entry used to describe: the vendored client decodes LZ, but QEMU's default is `auto_glz`, and GLZ is a different format. See [console.md](console.md).
- **Explicit Boot Devices**: The generated XML gives each boot device its own order (`<boot order='N'/>` on the disk, the CD-ROMs and, for network boot, the NIC) and carries no domain-level `<os><boot dev=...>` list, which UEFI firmware ignores. The CD-ROM is first when there is an image (or `boot_device` is `cdrom`); the disk stays in the list after it. See [vm_lifecycle.md](./vm_lifecycle.md#boot-order).
- **UEFI Boot Menu**: Boot menu options are enabled via `<bootmenu enable='yes' timeout='3000'/>` allowing direct boot path configuration.


---

## VM Disk Management & Resizing

To ensure guest OS virtual machines correctly recognize disk capacity increases (both while running and during initial boot), Vali orchestrates storage synchronization and device mapping resize operations.

### A. Guest VM Boot Disk Synchronization
This step no longer exists, and the reason is worth recording. A DRBD volume was a kernel
block device with a size the kernel had to be told about, and a stopped VM's device sat
`Secondary` where it could not be resized at all — so the power-on path had to prepend
`drbdadm primary` and `drbdadm resize` before libvirt could define the domain, or the guest
installer would see the old capacity.

A vdisk has no kernel object and no size the host has to agree with. It is sparse, and its
block map is keyed by extent index, so a grown range simply has no entries yet and reads as
zeroes. The recorded size is the whole of it, and a VM starting after a resize gets the new
size because that is what the record says.

### B. Live VM Disk Resizing
When a VM is running (`state = 'Running'`) and its disk is resized using `valcli vm.edit` or the Spectrum API:
1. **Vdisk resize**: the recorded size is raised through sidon's `resize` op. Nothing is
   resized underneath it — the vdisk is sparse and the map is keyed by extent index, so the
   new range has no entries and reads as zeroes until something writes there.
2. **Device Prefix Resolution**: The hypervisor dynamically resolves the correct disk prefix (`vd` for VirtIO controllers vs `sd` for SATA/SCSI controllers) based on the configured bus type in the VM metadata, avoiding hardcoded device guesses.
3. **QEMU Notification**: Finally, the hypervisor sends a live block-resize notification to QEMU via libvirt:
   ```bash
   virsh -c qemu:///system blockresize <vm_name> <target_dev> <new_size_in_kb>
   ```
   For example:
   ```bash
   virsh -c qemu:///system blockresize server2022 vda 130000000
   ```

---

### A. Managing VMs via `valcli`
The `valcli` CLI tool provides VM status management, power controls, and live migration:
```bash
# List all virtual machines in the cluster
valcli vm.list

# Power ON a virtual machine
valcli vm.on my-linux-vm

# Power OFF a virtual machine
valcli vm.off my-linux-vm

# Manually migrate a running VM to another cluster host IP address
valcli vm.migrate my-linux-vm 10.10.102.222

# Trigger a manual cluster memory load rebalancing check
valcli vm.balance

# View cluster load metrics and recent DRS migration events
valcli drs.status

# Place a node into maintenance mode (evacuates all running VMs to other hosts)
valcli host.maintenance.enter hci-node01

# Place a node into maintenance mode and force stop any VMs that cannot migrate
valcli host.maintenance.enter hci-node01 --force

# Restore a node from maintenance mode, starting services and re-syncing volumes
valcli host.maintenance.leave hci-node01
```

Entering maintenance is a claim on the host, taken through Daruk's
`POST /v1/node/maintenance` as `IF status = 'NORMAL'`. A host that is already transitioning
is refused with `409` instead of starting a second evacuation of the same VMs. Only that
first transition is conditional: the ones that follow are made by the workflow that already
holds the claim, and `hydra.nodes.status` is also written by Mipha's health loop, so
conditioning them too would wedge a host the first time the two crossed.

> [!WARNING]
> The cross-host rule — only one host in maintenance at a time, to preserve quorum — is
> still a read of every node row followed by a write, and two hosts entering concurrently
> can both pass it. Closing that needs a single-row cluster-wide lock, which needs a table
> that does not exist yet.

### B. Live Migration Command Syntax (libvirt)
To execute manual VM live migrations outside `valcli` (useful for troubleshooting):
```bash
# Live migrate 'my-linux-vm' to host 10.10.102.223. The destination's Sidon must already serve each
# disk's NBD socket (attach with forward=true on that node), and `valcli storage.takeover` finishes it:
# prefer `valcli vm.migrate`, which does all of it.
virsh -c qemu:///system migrate --live --persistent --undefinesource my-linux-vm qemu+ssh://root@10.10.102.223/system tcp://10.10.102.223
```

### C. Direct Database Task Querying
To check pending VM placement and migration tasks queued by Catalyst/Vali:
```bash
# Query tasks database table using cqlsh
podman exec -i systemd-hydra-db cqlsh 127.0.0.1 -e "SELECT task_id, vm_name, action, status FROM hydra.vali_tasks;"

# Query catalyst tasks for host reboot or maintenance operations
podman exec -i systemd-hydra-db cqlsh 127.0.0.1 -e "SELECT task_id, service, action, status, progress FROM hydra.catalyst_tasks;"

# Check host status and maintenance mode flags
podman exec -i systemd-hydra-db cqlsh 127.0.0.1 -e "SELECT hostname, ip, status, maintenance_mode FROM hydra.nodes;"
```



---

## Technical Reference

For the internal code structure, class/function details, and execution flowcharts, see the [Technical Guide](./vali_technical.md).
