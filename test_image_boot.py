#!/usr/bin/env python3
"""Booting a VM from an image, which is the only way to install a guest OS.

An image is an ordinary vdisk that was written once and sealed. Sealing detaches it --
deliberately, since an immutable vdisk should not keep the writer's attachment -- so an
image is *always* detached at rest and has no NBD socket until something attaches it.

Nothing did. The domain XML named `/var/lib/hci/sidon/nbd/img-<slug>.sock` and the start
path attached only the VM's own data disks, so every VM with an ISO failed with
"Cannot access storage file ...: No such file or directory" -- which reads like a missing
file rather than an unattached disk.

The XML was wrong too, in a way that predates that. Under DRBD an image really was a block
device at `/dev/drbd/by-res/img-<slug>/0`, so the CD-ROM was emitted as `type='block'` with
`<source dev=...>`. When images moved to Sidon the *path* was changed to a socket and the
device type was not, and qemu cannot open a unix socket as a block device.

Run with:  python -m unittest test_image_boot
"""

import ast
import io
import os
import re
import unittest

import helios_sidon

HERE = os.path.dirname(os.path.abspath(__file__))
SPECTRUM = os.path.join(HERE, "spectrum_server.py")
VALI = os.path.join(HERE, "vali.py")


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def spectrum_slugify():
    """`slugify_image_name` lifted out of spectrum_server.py, which cannot be imported."""
    tree = ast.parse(read(SPECTRUM), filename=SPECTRUM)
    fn = next((n for n in tree.body
               if isinstance(n, ast.FunctionDef) and n.name == "slugify_image_name"), None)
    assert fn is not None, "spectrum_server.py no longer defines slugify_image_name"
    scope = {}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), SPECTRUM, "exec"), scope)
    return scope["slugify_image_name"]


NAMES = [
    "debian-edu-13.4.0-amd64-netinst.iso",
    "ubuntu-24.04.1-live-server-amd64.iso",
    "CirrOS 0.6.2 (test).img",
    "win2022.qcow2",
    "a.iso",
    "----weird----name----.iso",
    "MiXeD CaSe With Spaces.ISO",
    "no-extension",
]


class TheSlugIsOneString(unittest.TestCase):
    """Upload derives the vdisk id, and so do start and delete. They must agree."""

    def test_image_vdisk_id_matches_the_upload_path(self):
        slugify = spectrum_slugify()
        for name in NAMES:
            self.assertEqual(
                helios_sidon.image_vdisk_id(name),
                "img-%s" % slugify(name),
                f"{name!r} is stored under one id by upload and looked up under another "
                f"at boot, so the guest is pointed at a vdisk nobody created")

    def test_the_real_image_resolves_to_the_id_on_the_node(self):
        """The id this produced for the image that exposed the bug."""
        self.assertEqual(
            helios_sidon.image_vdisk_id("debian-edu-13.4.0-amd64-netinst.iso"),
            "img-debian-edu-13-4-0-amd64-neti")


class TheCdromIsAnNbdExport(unittest.TestCase):
    def test_cdrom_xml_is_a_network_disk_not_a_block_device(self):
        xml = helios_sidon.cdrom_xml("img-x", "a")
        self.assertIn("type='network'", xml)
        self.assertIn("device='cdrom'", xml)
        self.assertIn("protocol='nbd'", xml)
        self.assertIn("transport='unix'", xml)
        self.assertIn("<readonly/>", xml)
        self.assertNotIn(
            "type='block'", xml,
            "a unix socket cannot be opened as a block device")
        self.assertNotIn("<source dev=", xml)

    def test_the_export_is_named_by_vdisk_id(self):
        """NBD addresses an export by name; a path in that field names nothing."""
        xml = helios_sidon.cdrom_xml("img-debian-edu-13-4-0-amd64-neti", "b")
        self.assertIn("name='img-debian-edu-13-4-0-amd64-neti'", xml)
        self.assertIn("img-debian-edu-13-4-0-amd64-neti.sock", xml)
        self.assertIn("dev='sdb'", xml)

    def test_neither_xml_builder_still_emits_a_block_cdrom(self):
        for path in (SPECTRUM, VALI):
            source = read(path)
            self.assertNotIn(
                "<disk type='block' device='cdrom'>", source,
                f"{os.path.basename(path)} still emits a CD-ROM as a block device, which "
                f"cannot open a Sidon socket")

    def test_no_builder_still_looks_for_a_dev_path_in_the_catalogue(self):
        """That lookup accepted a row only if its path contained "/dev/", so it stopped
        matching when images became sockets and silently never fired again."""
        for path in (SPECTRUM, VALI):
            source = read(path)
            self.assertNotIn(
                'if "/dev/" in line:', source,
                f"{os.path.basename(path)} still filters catalogue rows on a DRBD path")


class TheStartPathAttachesImages(unittest.TestCase):
    def test_vali_attaches_image_vdisks_before_defining_the_domain(self):
        source = read(VALI)
        self.assertIn(
            "image_vdisk_id(spec)", source,
            "vali no longer resolves image vdisk ids in the start path")

        attach = source.index('"op": "attach", "vdisk_id": img_id')
        define = source.index("virsh -c qemu:///system define")
        self.assertLess(
            attach, define,
            "the image is attached after the domain is defined, so the socket still does "
            "not exist when libvirt opens it")

    def test_a_failed_image_attach_does_not_detach_shared_images(self):
        """An immutable image may be serving another guest's CD-ROM. Rolling back this
        VM's data disks is right; ejecting a disc from a running VM is not."""
        source = read(VALI)
        start = source.index('"op": "attach", "vdisk_id": img_id')
        end = source.index("cmd = f\"{restore_cmd}", start)
        rollback = source[start:end]
        self.assertIn("for done in promoted:", rollback)
        self.assertNotIn(
            '"op": "detach", "vdisk_id": img_id', rollback,
            "the failure path detaches the image it just failed on, which can eject a "
            "disc from a different running VM")


if __name__ == "__main__":
    unittest.main()
