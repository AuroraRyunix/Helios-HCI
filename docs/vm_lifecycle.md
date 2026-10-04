# VM lifecycle: attributes, editing and boot order

What a VM is made of, which of it can be changed and when, and how its boot order is decided.
The first section is the boot order (written for the `test2` bug); the rest of this document is
the attribute table and the ESXi comparison.

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
