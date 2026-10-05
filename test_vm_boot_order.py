#!/usr/bin/env python3
"""What boots first, and why a VM given an install image used to stop at the boot menu.

The domain XML named the boot order with `<os><boot dev='cdrom'/><boot dev='hd'/></os>`.
That is a legacy BIOS setting: SeaBIOS reads it, and OVMF (the UEFI firmware, which is what
the console's create form defaults to) does not -- it honours only per-device boot orders, and
otherwise boots in the firmware's own default order. A VM created with an ISO and an empty
disk therefore did not reliably boot the CD-ROM, and when the firmware found nothing it liked
first it stopped at its boot options screen.

Every disk and CD-ROM that takes part now carries its own `<boot order='N'/>`, and the
domain-level list is gone (libvirt refuses a domain that has both). The semantics are
Sidon's `boot_orders`, shared by Vali and the console tier's own builder, and written down in
docs/vm_lifecycle.md.

Run with:  python -m unittest test_vm_boot_order
"""

import importlib.util
import os
import sys
import unittest
import xml.etree.ElementTree as ET
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name):
    spec = importlib.util.spec_from_file_location(
        "boot_order_" + name.replace(".py", ""), os.path.join(HERE, name))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


vali = load("vali.py")
spectrum = load("spectrum_server.py") if False else None  # opens a database on import; see below


def build(firmware="uefi", disks="20GB:default-pool:virtio", iso="", boot_device=""):
    with mock.patch.object(vali, "run_remote_spark", lambda ip, cmd, timeout=None: (1, "", "")), \
            mock.patch.object(vali, "get_network_by_id", lambda n: None):
        return vali.generate_vm_xml("vm", 2048, 2, firmware, disks, iso, boot_device)


def device_orders(xml):
    """{('disk'|'cdrom'|'interface', target-or-index): order} for every device that has one."""
    root = ET.fromstring(xml)
    found = {}
    for disk in root.iter("disk"):
        boot = disk.find("boot")
        if boot is not None:
            found[(disk.get("device"), disk.find("target").get("dev"))] = int(boot.get("order"))
    for index, nic in enumerate(root.iter("interface")):
        boot = nic.find("boot")
        if boot is not None:
            found[("interface", index)] = int(boot.get("order"))
    return found


def by_kind(xml):
    orders = device_orders(xml)
    return {
        "disk": sorted(v for (k, _), v in orders.items() if k == "disk"),
        "cdrom": sorted(v for (k, _), v in orders.items() if k == "cdrom"),
        "nic": sorted(v for (k, _), v in orders.items() if k == "interface"),
    }


class TheDomainNamesItsBootOrderPerDevice(unittest.TestCase):
    def test_there_is_no_domain_level_boot_list_for_either_firmware(self):
        """The legacy list is what UEFI ignored, and libvirt refuses it beside per-device
        orders."""
        for firmware in ("uefi", "bios"):
            for boot_device in ("", "cdrom", "hd"):
                root = ET.fromstring(build(firmware, iso="ubuntu.iso", boot_device=boot_device))
                self.assertEqual(root.find("os").findall("boot"), [],
                                 "%s/%s still carries <os><boot>" % (firmware, boot_device))

    def test_a_new_vm_with_an_image_and_an_empty_disk_boots_the_cd_first(self):
        """The reported case: created in the console with an ISO, default boot device."""
        for firmware in ("uefi", "bios"):
            orders = device_orders(build(firmware, iso="ubuntu.iso"))
            self.assertEqual(orders[("cdrom", "sda")], 1)
            self.assertEqual(orders[("disk", "vda")], 2,
                             "the disk follows the CD, so a CD that is not bootable falls "
                             "through to it instead of stopping at the menu")

    def test_boot_from_cdrom_is_the_same_as_the_default_when_there_is_an_image(self):
        self.assertEqual(device_orders(build(iso="ubuntu.iso", boot_device="cdrom")),
                         device_orders(build(iso="ubuntu.iso", boot_device="")))

    def test_boot_from_hd_puts_the_disk_first_and_keeps_the_cd_as_the_fallback(self):
        orders = device_orders(build(iso="ubuntu.iso", boot_device="hd"))
        self.assertEqual(orders[("disk", "vda")], 1)
        self.assertEqual(orders[("cdrom", "sda")], 2)

    def test_no_image_means_the_disk_boots(self):
        for boot_device in ("", "hd", "cdrom"):
            orders = by_kind(build(boot_device=boot_device))
            self.assertEqual(orders, {"disk": [1], "cdrom": [], "nic": []}, boot_device)

    def test_only_the_first_disk_is_the_boot_disk(self):
        xml = build(disks="20GB:default-pool:virtio,50GB:default-pool:virtio")
        self.assertEqual(device_orders(xml), {("disk", "vda"): 1})

    def test_an_empty_drive_is_not_a_boot_device(self):
        orders = device_orders(build(iso="__empty__,ubuntu.iso"))
        self.assertEqual(list(orders.values()).count(1), 1)
        self.assertEqual(sorted(orders.values()), [1, 2])

    def test_a_vm_with_no_disks_boots_its_cd(self):
        self.assertEqual(by_kind(build(disks="NONE", iso="ubuntu.iso")),
                         {"disk": [], "cdrom": [1], "nic": []})

    def test_network_boot_is_first_when_asked_for(self):
        orders = device_orders(build(iso="ubuntu.iso", boot_device="network"))
        self.assertEqual(orders[("interface", 0)], 1)
        self.assertEqual(orders[("disk", "vda")], 2)
        self.assertEqual(orders[("cdrom", "sda")], 3)

    def test_orders_are_distinct_and_start_at_one(self):
        for boot_device in ("", "cdrom", "hd", "network"):
            values = sorted(device_orders(build(iso="a.iso,b.iso", boot_device=boot_device)).values())
            self.assertEqual(values, list(range(1, len(values) + 1)), boot_device)

    def test_a_boot_device_cannot_inject_markup(self):
        """It used to be interpolated into the XML in the console tier's builder."""
        xml = build(boot_device="hd'/><evil/><x a='")
        self.assertEqual(ET.fromstring(xml).findall(".//evil"), [])

    def test_the_uefi_loader_and_nvram_are_unchanged(self):
        root = ET.fromstring(build("uefi", iso="ubuntu.iso"))
        self.assertIsNotNone(root.find("os/loader"))
        self.assertIsNotNone(root.find("os/nvram"))
        self.assertIsNone(ET.fromstring(build("bios")).find("os/loader"))


class TheSharedPlanner(unittest.TestCase):
    def setUp(self):
        self.sidon = vali.sidon_module()

    def test_the_plan_for_each_choice(self):
        plan = self.sidon.boot_orders
        self.assertEqual(plan("", 1, 1), {"disk": {0: 2}, "cdrom": {0: 1}, "nic": {}})
        self.assertEqual(plan("cdrom", 1, 2), {"disk": {0: 3}, "cdrom": {0: 1, 1: 2}, "nic": {}})
        self.assertEqual(plan("hd", 2, 1), {"disk": {0: 1}, "cdrom": {0: 2}, "nic": {}})
        self.assertEqual(plan("", 1, 0), {"disk": {0: 1}, "cdrom": {}, "nic": {}})
        self.assertEqual(plan("network", 1, 1, 1), {"disk": {0: 2}, "cdrom": {0: 3}, "nic": {0: 1}})

    def test_the_console_tiers_builder_uses_the_same_planner(self):
        """spectrum_server opens a database on import, so its builder is read as text."""
        with open(os.path.join(HERE, "spectrum_server.py"), encoding="utf-8") as handle:
            source = handle.read()
        body = source[source.index("def generate_vm_xml"):source.index("def generate_vm_xml") + 9000]
        self.assertIn("module.boot_orders(", body)
        self.assertIn("boot_order=orders[\"disk\"]", body)
        self.assertIn("boot_order=orders[\"cdrom\"]", body)
        self.assertIn("<bootmenu enable='yes' timeout='3000'/>", body)
        self.assertNotIn("<boot dev=", body)

    def test_valcli_create_defaults_to_automatic_boot_device(self):
        """valcli vm.create must not hard-code boot_device to 'hd' so ISOs can boot first."""
        with open(os.path.join(HERE, "valcli.py"), encoding="utf-8") as handle:
            source = handle.read()
        body = source[source.index("def cmd_vm_create"):source.index("def cmd_vm_create") + 2000]
        self.assertIn('boot_device = ""', body)
        self.assertNotIn('boot_device = "hd"', body)


if __name__ == "__main__":
    unittest.main()
