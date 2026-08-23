#!/usr/bin/env python3
"""Every empty disk becomes an extent store, and none of them share a failure domain.

Each node had two 300 GB disks and used one. The obvious fix -- `vgextend vg_aether
/dev/sdc` -- is the one that must not be taken: `vg_aether` holds a **thin pool**, a thin
pool spans its physical volumes, and losing one disk would then take the whole volume
group. That converts "one disk died" into "this node's entire extent store died", *and*
doubles the chance of it, because two disks could now cause it.

So each disk is its own filesystem and sidon places extent groups across them in software,
which is what Nutanix does and for the same reason. The design is in
docs/dfs/multi_disk.md.

These tests guard the two things that would quietly undo it: a claim script that stops
skipping a disk it must not touch, and the two copies of that script drifting apart.

Run with:  python -m unittest test_multi_disk
"""

import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def claim_script(name, const):
    match = re.search(r'%s = r"""(.*?)"""' % const, read(name), re.S)
    assert match, "%s does not define %s" % (name, const)
    return match.group(1)


class NothingPoolsTheDisks(unittest.TestCase):
    """The failure domain is one disk. Pooling would make it the node."""

    def test_no_second_physical_volume_is_ever_added(self):
        for name in ("provision.py", "deploy_updates.py", "cluster_new.py"):
            self.assertNotIn(
                "vgextend", read(name),
                "%s extends the volume group, which makes one disk failure take the "
                "node's whole extent store" % name)

    def test_additional_disks_get_their_own_filesystem(self):
        script = claim_script("provision.py", "CLAIM_EXTRA_DISKS")
        self.assertIn("mkfs.xfs", script)
        self.assertIn("/var/lib/hci/sidon/disks", script)


class TheClaimGuardsHold(unittest.TestCase):
    """A claimed disk is wiped, so these guards are the entire safety story."""

    def setUp(self):
        self.script = claim_script("provision.py", "CLAIM_EXTRA_DISKS")

    def test_a_disk_already_in_a_volume_group_is_skipped(self):
        """That is the first disk. Reformatting it would destroy the extent store."""
        self.assertIn("pvs --noheadings -o pv_name", self.script)
        self.assertIn("claimed_pvs", self.script)

    def test_a_mounted_disk_is_skipped(self):
        self.assertIn("lsblk -n -o MOUNTPOINT", self.script)

    def test_a_partitioned_disk_is_skipped(self):
        """The OS disk carries partitions; nothing else needs to be known about it."""
        self.assertIn("lsblk -n -o TYPE", self.script)
        self.assertIn("grep -qx part", self.script)

    def test_a_small_disk_is_skipped(self):
        self.assertIn("100000000000", self.script)

    def test_a_disk_that_already_has_a_filesystem_is_never_reformatted(self):
        """Re-running a rollout must not wipe a disk that is already an extent store."""
        self.assertIn('if ! blkid "$dev"', self.script)

    def test_it_mounts_by_uuid(self):
        """Device names are not stable across reboots; a disk mounted by /dev/sdc can come
        back as something else and take another disk's place in the store."""
        self.assertIn("blkid -s UUID -o value", self.script)
        self.assertIn("UUID=$uuid", self.script)


class BothDeploymentPathsClaimIdentically(unittest.TestCase):
    def test_the_two_copies_are_the_same_script(self):
        """One claims disks for a new node, the other reaches nodes that already exist. A
        difference means a disk laid out one way on some nodes and another way on the
        rest."""
        self.assertEqual(
            claim_script("provision.py", "CLAIM_EXTRA_DISKS"),
            claim_script("deploy_updates.py", "CLAIM_EXTRA_DISKS"),
            "the claim script has drifted between provisioning and the rollout")


class SidonSpansThemInSoftware(unittest.TestCase):
    def setUp(self):
        self.extent = read(os.path.join("sidon", "src", "extent.rs"))

    def test_the_store_is_built_from_discovered_disks(self):
        self.assertIn("pub fn discover_disks(", self.extent)
        self.assertIn("pub fn open(disks: Vec<Disk>", self.extent)
        for name in ("sidon/src/vdisk.rs", "sidon/src/control.rs"):
            self.assertIn(
                "discover_disks", read(name),
                "%s still builds a store over a single directory" % name)

    def test_placement_is_least_full_first(self):
        """Round-robin would give a newly added disk an equal share of new writes, so it
        would stay permanently behind the others."""
        self.assertIn("fn placement(", self.extent)
        self.assertIn("best_free", self.extent)

    def test_the_root_filesystem_is_not_treated_as_a_disk(self):
        """`<root>/egroups` is created unconditionally at startup. Counting an empty one
        would put extent groups on the root filesystem, which a full extent store must
        never be able to fill."""
        self.assertIn("legacy_holds_data", self.extent)

    def test_an_unmounted_disk_directory_is_refused(self):
        """A mount that failed at boot leaves an ordinary directory on the root
        filesystem. Using it puts extent groups where a full store could wedge the host."""
        self.assertIn("fn is_separate_filesystem(", self.extent)
        self.assertIn("is not a mounted filesystem and will not be used", self.extent)

    def test_a_lost_disk_is_reported_rather_than_silent(self):
        purah = read(os.path.join("sidon", "src", "purah.rs"))
        self.assertIn("pub missing: Vec<String>", purah)
        self.assertIn('"missing_count"', purah)

    def test_capacity_reports_each_disk(self):
        control = read(os.path.join("sidon", "src", "control.rs"))
        self.assertIn('"disks": per_disk', control)
        self.assertIn('"disk_count"', control)

    def test_an_unreadable_disk_reports_unknown_not_zero(self):
        """Zero capacity and unknown capacity are different statements, and only one of
        them means full."""
        control = read(os.path.join("sidon", "src", "control.rs"))
        self.assertIn('"total_bytes": Value::Null', control)


if __name__ == "__main__":
    unittest.main()
