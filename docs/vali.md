# Vali (VM Manager & Scheduler Service)

Vali is the standalone VM management, placement scheduling, and DRS (load balancing) coordinator for the HCI cluster. It is the direct equivalent of Nutanix **Acropolis (AHV VM Management)**.

> [!NOTE]
> **Name Origin:** A dual-purpose name:
> 1. In Norse mythology, **Váli** is a son of Odin destined to survive Ragnarok and avenge the death of his brother, representing how Vali avenges host resource imbalances by dynamically migrating and scheduling virtual machines.
> 2. It is also short for **Revali**, the Rito Champion in *The Legend of Zelda: Breath of the Wild* known for *Revali's Gale* (an upward draft that launches the hero into the air), representing the dynamic flight, placement, and live migration of virtual machines across hypervisor nodes.

## Architecture & Lifecycle
- **Daemon Service**: Runs as a standalone python service (`/usr/local/bin/vali`) listening locally on port `9095`. Managed by systemd (`vali.service`).
- **Leader Election**: All Vali instances run ZooKeeper leader election using ephemeral sequential nodes at `/vali/leader`. The elected Leader is responsible for consuming tasks and executing DRS checks.
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
3. Vali queues the task in `hydra.vali_tasks` and polls the database for task completion, returning a synchronous response once processed.

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
4. It compiles the VM's XML and calls the target node's `spark-daemon` `/api/v1/execute` to define and start the VM.
5. It updates the VM record state to `Running` and `host_ip` to the chosen hypervisor node.

## Distributed Resource Scheduler (DRS)
The Vali Leader runs a periodic DRS loop (every 30 seconds):
1. **Load Evaluation**: It checks memory utilization percentages across all active hypervisor nodes.
2. **Overload Trigger**: A host is considered overloaded if its memory usage exceeds `85%` or if its usage is more than `15%` higher than the average cluster node utilization.
3. **Rebalancing Action**: If an overloaded node is detected, Vali selects a running VM on that host and queues a `migrate` task to live-migrate it to the node with the highest available memory.
4. **Live Migration**: Vali executes live migrations via libvirt:
   `virsh -c qemu:///system migrate --live --persistent --undefinesource --unsafe <vm_name> qemu+ssh://root@<target_ip>/system`
    And updates the VM's `host_ip` in ScyllaDB on completion. To enable compatibility during live migrations, VM guest CPUs are defined with `<cpu mode='host-model'/>` when running under KVM.

## VM Display and Video Configuration Standards
To ensure compatibility across all hypervisor nodes:
- **Video Model**: VMs use the standard VGA video model (`<model type='vga' vram='16384' heads='1' primary='yes'/>`). Other video drivers like `qxl` are avoided because QEMU ROM files (such as `vgabios-qxl.bin`) are missing on standard EL 10.2 hypervisor repositories. The VGA BIOS binary `/usr/share/seavgabios/vgabios-stdvga.bin` is pre-installed on every hypervisor node.
- **Dual Display Console**: Both VNC and SPICE graphic displays are enabled concurrently with automatic ports, offering high performance and smooth VM console interactivity. A VirtIO-serial spicevmc channel target is mapped to `com.redhat.spice.0` for SPICE guest communication.
- **Explicit Boot Devices**: The generated XML explicitly specifies both CD-ROM (`<boot dev='cdrom'/>`) and Hard Disk (`<boot dev='hd'/>`) boot elements to prevent guest boot loops after OS installations.
- **UEFI Boot Menu**: Boot menu options are enabled via `<bootmenu enable='yes' timeout='3000'/>` allowing direct boot path configuration.


---

## VM Disk Management & Resizing

To ensure guest OS virtual machines correctly recognize disk capacity increases (both while running and during initial boot), Vali orchestrates storage synchronization and device mapping resize operations.

### A. Guest VM Boot Disk Synchronization
If a virtual machine's disk is resized while the VM is stopped, the underlying DRBD block device exists in a `Secondary` role on the host hypervisor and cannot be directly queried or updated by the kernel automatically. To resolve this:
1. During the VM power-on sequence, Vali parses the disk definitions from ScyllaDB.
2. Vali prepends commands to promote (`drbdadm primary`) and sync/resize (`drbdadm resize`) the DRBD resource definition on the destination host before the VM is defined and booted:
   ```bash
   drbdadm primary res-img-virtio-win && drbdadm resize res-img-virtio-win
   ```
3. This updates the physical block device capacity in the host kernel before libvirt defines the domain, guaranteeing that the guest OS installer (e.g. Windows Server) detects the full resized capacity immediately upon boot.

### B. Live VM Disk Resizing
When a VM is running (`state = 'Running'`) and its disk is resized using `valcli vm.edit` or the Spectrum API:
1. **Host Block Device Resize**: The hypervisor first updates the host kernel's block size mapping by running:
   ```bash
   drbdadm resize res-img-virtio-win
   ```
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

### B. Live Migration Command Syntax (libvirt)
To execute manual VM live migrations outside `valcli` (useful for troubleshooting):
```bash
# Live migrate 'my-linux-vm' to host 10.10.102.223 securely without shared storage requirement checks
virsh -c qemu:///system migrate --live --persistent --undefinesource --unsafe my-linux-vm qemu+ssh://root@10.10.102.223/system
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

