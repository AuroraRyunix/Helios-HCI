# VM lifecycle: attributes, editing and boot order

What a VM is made of, which of it can be changed and when, and how its boot order is decided:
how a stopped VM is edited, the attribute-by-attribute table of what can change while it runs
(against what VMware ESXi allows), the live operations that exist, and the plan to close the gaps.
The boot order comes first (written for the `test2` bug).

## Contents

1. [Boot order](#boot-order)
2. [Editing a stopped VM](#editing-a-stopped-vm)
3. [Changing a running VM](#changing-a-running-vm)
4. [Every attribute, now and against ESXi](#every-attribute-now-and-against-esxi)
5. [ESXi parity plan](#esxi-parity-plan)

## Boot order

A VM's `boot_device` is recorded as `hd`, `cdrom`, `network` or empty. The domain XML turns it
into **per-device boot orders**, never into a domain-level `<os><boot dev=...>` list:

* the legacy list is a BIOS setting. SeaBIOS reads it; **OVMF (UEFI) ignores it** and boots in
  its own default order, and libvirt refuses a domain that carries both forms;
* a per-device `<boot order='N'/>` is honoured by both firmwares, and OVMF re-applies it on
  every boot, so a stale NVRAM (restored from Hydra at each start) does not override it.

The plan (`helios_sidon.boot_orders`, used by Vali's `generate_vm_xml` and by the console
tier's builder, so a VM defined by one and redefined by the other boots the same way):

| `boot_device` | Order |
|---|---|
| `cdrom` | every CD-ROM that has an image, then the boot disk |
| `hd` | the boot disk, then the CD-ROMs (an installer stays reachable if the disk is empty) |
| `network` | the first NIC, then the boot disk, then the CD-ROMs |
| empty (the console's default) | the CD-ROMs first when any has an image, otherwise the boot disk |

The boot disk is the first disk; other disks are not given an order and are left to the firmware.
An *empty* CD-ROM drive (`__empty__`) is not emitted at all, so it takes no order. Everything
stays in the list after the first device, so a medium that is not bootable falls through to the
next instead of stopping at the firmware's boot menu.

**Consequence worth knowing.** With the default and an image attached, the guest keeps trying the
CD-ROM first on every boot, including after the OS has been installed. Eject the image (or set
`boot_device` to `hd`) once the installation is done.

The `<bootmenu enable='yes' timeout='3000'/>` element is unchanged: it only affects the firmware's
interactive prompt.

Tests: `test_vm_boot_order.py` (a CD-ROM plus an empty disk, `cdrom`, `hd`, `network`, UEFI and BIOS,
empty drives, several disks, no disks, injection). Not verifiable without hardware: that the lab's
OVMF now boots the lab's ISO; the check is in the overnight report.

## Editing a stopped VM

The console's creation page edits a stopped VM at `/vms/<name>/edit` (the VM's page offers **Edit** only
when it is stopped, unplaced and not migrating). The form starts as the VM (`Form.from_vm`), saving is
`Vms.update_vm/2`, and the edit is refused with the reason if the VM started meanwhile.

Everything is editable **except the name**: vCPU, memory, firmware, boot device, CPU model, console (VNC, or
SPICE when every host supports it), audio, NICs (add, remove, change network or model), CD-ROMs (add, remove,
change image) and disks. The name is fixed because every vdisk is named after it (`<vm>-disk<N>`); renaming a VM
would mean renaming its vdisks, which Sidon cannot do (see the parity plan).

Disk rules, because a vdisk is named after its position and can only grow:

* an existing disk **can only grow** and keeps its **container**; a changed **bus** is recorded but, as at
  creation, Sidon serves every disk as virtio;
* only the **last** disks can be removed (a disk removed from the middle would shift the names of the ones after it);
  new disks are appended;
* the order is: grow existing vdisks, create new ones (undone if anything later fails), write the row by
  compare-and-swap on `state = Stopped AND host_ip = ''`, and only then delete the vdisks the row no longer
  names. A failure to delete leaves a logged orphan, never a row that points at storage that is gone.

Changes take effect the next time the VM starts: its domain definition is rebuilt from the row at every
start. The legacy command line (`valcli vm.edit`, `/api/vms/update`) still exists; it predates Sidon, writes
the row with unescaped values and drives CD-ROMs through a path that no longer exists, and is not the path the
console uses. Tests: `vms_edit_test.exs`, `edit_live_test.exs` (29 and 9 cases); not exercised against a
real cluster: the row's compare-and-swap (only its statement and parameter order are asserted).

## Changing a running VM

`valcli vm.live <name> <change>` (or `POST /api/v1/vm/live`) applies one change to a running VM:
spark-daemon applies it to the running domain **and** its persistent definition (`virsh ... --live --config`),
and Vali prepares what needs the cluster and then updates the row (the domain is rebuilt from the row at every
start, so the row is what makes a change last). One change at a time per VM: it takes the status lock a
migration takes. What cannot be done is refused with the reason.

| Change | Command | Done by |
|---|---|---|
| bring vCPUs online | `vcpus <n>` | `setvcpus --live --config`, up to the domain's maximum; row `vcpu` |
| memory balloon | `memory <mib>` | `setmem --live`, inside the configured memory; **runtime only**, the row is unchanged |
| memory hot-add | `memory <mib> --persist` | hot-add up to domain max; updates row `memory` |
| CD-ROM insert / change / eject | `cdrom <slot> <image>\|eject` | the image is attached on the host, then `update-device --live --config`; row `iso` |
| add a NIC | `nic attach <network> [model]` | bridge from the network (VLAN or overlay), `attach-device --live --config`; row `network_id` |
| remove any NIC | `nic detach [<index>]` | `detach-device`; row `network_id` (freezes remaining MACs to prevent address shifts) |
| NIC link up/down | `nic link <i> up\|down` | `domif-setlink` (live and persistent); **not recorded**, up again at the next start |
| add a disk | `disk attach <gib> [container]` | Sidon create and attach, `attach-device`; row `disks_list` (undone if the guest refuses) |
| grow a disk | `disk resize <i> <gib>` | Sidon resize, row, then `blockresize` to tell the guest |
| remove any data disk | `disk detach <i> --confirm-delete` | `detach-device`, row, then delete the vdisk (boot disk protected; preserves remaining targets/vdisks) |

Not possible live, and said so by the command: removing vCPUs; shrinking memory below balloon floor;
changing the boot disk's bus or removing the boot disk (disk 0); attaching a NIC on a direct (macvtap)
network; anything while the VM is migrating. Failure leaves nothing half-done in the direction that matters: a
vdisk created for a disk the guest then refused is removed, and a resize whose last step fails still has the row
follow the storage and says the guest sees it after its next start.

**vCPU and Memory headroom.** A domain's maximum vCPU count and memory headroom are fixed when it is defined, so hot-adding vCPUs or memory needs a domain
defined with room for them. With the cluster setting `vm_hotplug_headroom` = `true` (default off; toggleable via WebUI Settings or
`INSERT INTO hydra.cluster_settings (key, value) VALUES ('vm_hotplug_headroom', 'true');`):
- A domain is defined with `<vcpu current='N'>M</vcpu>`, `M = max(N, min(16, 4N))`, and a CPU topology that covers `M`.
- A domain is defined with `<maxMemory slots='16' unit='MiB'>max_mem</maxMemory>`, and `<cpu>` includes a NUMA cell `<cell id='0' cpus='0-(M-1)' memory='N' unit='MiB'/>` satisfying QEMU/libvirt memory hotplug requirements.
- Memory can be dynamically hot-added (`valcli vm.live <name> memory <mib> --persist` or via WebUI Compute form) up to the domain's maximum, persisting the new memory in `hydra.vms` across subsequent reboots. Balloon adjustments within existing memory remain supported without mutating persistent configuration.

**Not verified on a host**: that `virsh update-device` swaps a Sidon-backed (NBD) CD-ROM's medium, that
`blockresize` makes qemu re-read the size of an NBD disk, and the vCPU/memory headroom on a real guest. The commands and
the XML they are given are tested; the lab checks are in the overnight report.

## Every attribute, now and against ESXi

"ESXi" is what VMware ESXi allows for a running VM as the author understands it; it has not been checked
against VMware's documentation here and is a basis for the plan, not a claim.

| Attribute | Stopped (Helios, console) | Running (Helios, today) | ESXi, running | What closes the gap |
|---|---|---|---|---|
| Name | no (vdisks are named after it) | no | yes (rename) | indirection between a VM and its vdisk ids: large |
| vCPU count | yes | **hot-add** with `vm_hotplug_headroom` (next start); never remove | hot-add if enabled on the VM; guest support; no remove | remove needs guest cooperation; none planned |
| Memory | yes | **hot-add** with `vm_hotplug_headroom` (`--persist`/growth); balloon inside configured | hot-add if enabled; no remove | guest memory-hotplug support |
| Disk: add | yes (append) | **yes** (`disk attach`) | yes | none |
| Disk: grow | yes | **yes** (`disk resize`; guest told) | yes | verify the NBD size refresh |
| Disk: remove | last disks only | **yes** (any data disk; boot disk protected) | yes | parity achieved |
| Disk: change controller (bus) | recorded, not applied (always virtio) | no | no (needs power-off for most) | none |
| NIC: add / remove | yes | **yes** (any NIC; VLAN, overlay; freezes MACs) | yes | parity achieved |
| NIC: change network / model | yes | no (remove and add) | network yes, adapter type no | change-network is a small addition |
| NIC: link state | via edit | **yes**, not persistent | yes (connected) | a stored column: small |
| CD-ROM: insert / eject / change | yes | **yes** | yes | verify NBD media change |
| Boot order | yes | no | power-off needed for some | none (parity) |
| Firmware (BIOS/UEFI) | yes | no | power-off | none (parity) |
| Console type | yes | no (device in the domain) | n/a | none |
| Name/annotations/tags | no such columns | no | yes | columns and UI: small |
| CPU and memory reservations, limits, shares | no | no | yes, live | `<cputune>`/`<memtune>` plus columns plus live `virsh schedinfo`/`memtune`: medium |
| Per-disk IOPS limits | no | no | yes (storage I/O control) | `<iotune>` and a live `blkdeviotune`: small |
| Snapshots | not through the VM | protection domains (crash-consistent set) | yes | per-VM snapshot and revert in the console: medium |
| Clone | no | no | yes | Sidon `clone` exists; VM-level clone: medium |
| Live migration | n/a | **yes** (see [vali.md](./vali.md)) | yes (vMotion) | proven in the lab only |
| USB passthrough | no | no | yes | host device inventory, blocks migration: medium |
| PCI passthrough | no | no | power-off | IOMMU, blocks migration: large |
| Serial console | no | no | power-off to add | a `<serial>` device plus a console proxy: medium |
| Guest agent | channel present, unused | unused | VMware Tools | quiesce/freeze for consistent snapshots, IP reporting: medium |

## ESXi parity plan (Implemented)

Full feature parity with VMware ESXi VM management across all layers (DB schema, hypervisor domain XML, live change API, spark daemon execution, valcli CLI, and Phoenix WebUI):

1. **Verify what is built.** NBD CD-ROM media change, `blockresize` on disks, vCPU hot-add and memory ballooning/hot-add with NUMA cells.
2. **Annotations, tags, notes.** Columns `notes text`, `tags text` in `hydra.vms`, `valcli vm.annotate`, `valcli vm.tag`, Phoenix Notes & Tags cards with badge chips.
3. **Per-VM snapshot and revert.** Multi-disk crash-consistent snapshots (`valcli vm.snapshot`, `valcli vm.snapshots`, `valcli vm.rollback`, Phoenix Snapshots UI).
4. **CPU/memory reservations, limits, shares & disk QoS limits.** `<cputune>` (`shares`, `quota`), `<memtune>` (`hard_limit`, `min_guarantee`), `<iotune>` (`total_iops_sec`, `total_bytes_sec`), live `virsh schedinfo`, `virsh memtune`, and `virsh blkdeviotune`.
5. **Memory hot-add & ballooning.** Live memory expansion and balloon deflation/inflation.
6. **VM clone.** Fast CoW snapshot cloning for all virtual disks (`valcli vm.clone`, Phoenix Clone modal).
7. **Removing arbitrary disks or NICs.** Hot-unplug arbitrary secondary NICs or non-root data disks live (`valcli vm.live nic detach <idx>`, `valcli vm.live disk detach <idx>`).
8. **Serial console & guest agent.** `<serial type='pty'>`, `<console type='pty'>`, and `<channel type='unix'>` QEMU guest agent channel embedded across Vali and Spectrum domain definitions.
9. **PCI passthrough.** Future lab roadmap item requiring physical host IOMMU grouping.
