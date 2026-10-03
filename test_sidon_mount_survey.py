#!/usr/bin/env python3
"""A missing disk is reported by the layer that owns it, at the right severity.

The sidon mounts used to need `nofail`: without it a data disk that was late or renumbered
failed `local-fs.target` and dropped the node into emergency mode with no network and no SSH,
and two of three nodes did exactly that. But nofail traded a loud failure for a silent one,
and for a storage system the silent one is worse. A node whose volume did not mount booted
perfectly, and sidon wrote extent groups to the *root filesystem* at the same path -- smaller,
slower, un-replicated, and invisible the moment the real volume mounted underneath them.

That is not hypothetical. After an emergency-mode boot all three nodes came up with
`/var/lib/hci/sidon` unmounted, 202 extent groups sat unreachable on an active LV, and the
only symptom anywhere was NBD reads failing for one image. systemd was content: a nofail
mount that did not happen is not a failure.

Nothing sidon owns is in fstab any more (D-27), so there is no `nofail` and no declared mount
for systemd to be content about. What is declared is /etc/hci/sidon-disks, by filesystem UUID,
and sidon itself refuses a path whose disk is not there. This survey is the independent
witness. The property asserted is the same as before -- the condition is *reported*, at the
right severity: the journal volume missing is a FAIL, extra capacity missing is a WARN -- but
what it reads is the manifest, and what it believes is `stat`, never a mount table and never a
sentinel file alone.

Run with:  python -m unittest test_sidon_mount_survey
"""

import importlib.util
import io
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))

LV = "dfa470d5-2350-4077-a8ed-7b942eb0cff0"
SDC = "5c9e756d-a035-4de7-9dc0-11afcc82f0fe"

MANIFEST_BOTH = """\
# <filesystem-uuid> <journal|extent>
%s extent
%s journal  # written by the claim step
""" % (SDC, LV)

# What a node built before the manifest still declares, off 10.10.102.41.
FSTAB_OLD = """\
UUID=aaa /                            xfs  defaults                          0 0
UUID=bbb /var/lib/hci/sidon           xfs  defaults,noatime,nofail           0 0
UUID=ccc /var/lib/hci/sidon/disks/sdc xfs  defaults,noatime,nofail           0 0
"""

FSTAB_NONE = """\
UUID=aaa / xfs defaults 0 0
"""


def load_mimir():
    spec = importlib.util.spec_from_file_location(
        "mimir_under_test", os.path.join(HERE, "mimir.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class Machine(unittest.TestCase):
    """A node's files and mounts, without a node."""

    def setUp(self):
        self.mimir = load_mimir()
        self.manifest = MANIFEST_BOTH
        self.fstab = FSTAB_NONE
        self.mounted = set()
        self.absent_because = {}
        real_open = io.open

        def fake_open(path, *args, **kwargs):
            if path == self.mimir.SIDON_DISKS_MANIFEST:
                if self.manifest is None:
                    raise FileNotFoundError(path)
                return io.StringIO(self.manifest)
            if path == "/etc/fstab":
                return io.StringIO(self.fstab)
            return real_open(path, *args, **kwargs)

        glob = self.mimir.sidon_declared_disks.__globals__
        glob["open"] = fake_open
        self.addCleanup(glob.pop, "open", None)

        patch = mock.patch.object(
            self.mimir, "sidon_disk_absence",
            side_effect=lambda uuid, root=self.mimir.SIDON_ROOT: self.absent_because.get(uuid))
        patch.start()
        self.addCleanup(patch.stop)
        ismount = mock.patch.object(os.path, "ismount", side_effect=lambda p: p in self.mounted)
        ismount.start()
        self.addCleanup(ismount.stop)

    def gone(self, uuid, why="not mounted (a plain directory on the root filesystem)"):
        self.absent_because[uuid] = why


class TheSurveyReadsTheManifestRatherThanAssuming(Machine):
    """A node with two spare disks declares two more of these than a node with none, so a
    fixed list would be wrong on both."""

    def test_it_finds_every_declared_disk_with_the_journal_volume_first(self):
        self.assertEqual(self.mimir.sidon_declared_disks(),
                         [(LV, "journal"), (SDC, "extent")])

    def test_comments_and_lines_that_are_not_disks_are_ignored(self):
        self.manifest = MANIFEST_BOTH + "# aaaa-bbbb extent  commented out\nnonsense\nxxxx volume\n"
        self.assertEqual(self.mimir.sidon_declared_disks(), [(LV, "journal"), (SDC, "extent")])

    def test_everything_present_passes(self):
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "PASS")
        self.assertIn(LV, output)
        self.assertIn(SDC, output)

    def test_the_journal_volume_missing_is_a_failure(self):
        """The case that actually happened. Sidon used to keep writing, to the wrong
        filesystem; it now refuses to start, and this says why it is not running."""
        self.gone(LV)
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "FAIL")
        self.assertIn(LV, output)
        self.assertIn("journal", output)
        self.assertIn("root filesystem", output, "the report does not say where writes would have gone")

    def test_a_missing_data_disk_is_only_a_warning(self):
        """The store is smaller than intended rather than misplaced, which is a different
        severity -- and `.43` was in exactly this state with nobody told."""
        self.gone(SDC)
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "WARN")
        self.assertIn(SDC, output)

    def test_both_missing_is_a_failure_naming_both(self):
        self.gone(LV)
        self.gone(SDC, "its device is not attached")
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "FAIL")
        self.assertIn(LV, output)
        self.assertIn(SDC, output)
        self.assertIn("its device is not attached", output)

    def test_the_report_no_longer_blames_nofail(self):
        """There is no nofail to blame: a node with a missing disk boots because the boot
        does not know about the disk, and the report must not send an operator to fstab."""
        self.gone(SDC)
        _, output = self.mimir.survey_sidon_mounts()
        self.assertNotIn("nofail", output)
        self.assertNotIn("/etc/fstab and NOT", output)

    def test_a_manifest_with_no_journal_volume_is_a_failure(self):
        """Sidon refuses to start on one, so the survey must not call it healthy."""
        self.manifest = "%s extent\n" % SDC
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "FAIL")
        self.assertIn("journal", output)

    def test_a_host_that_declares_nothing_is_reported_not_passed(self):
        """A single-filesystem host is valid, and still worth saying out loud -- silently
        passing would make "no disks declared" indistinguishable from "all mounted"."""
        self.manifest = None
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "WARN")
        self.assertIn("root filesystem", output)


class AnUnmovedNodeIsNotReportedBroken(Machine):
    """The rollout stages the manifest and sidon moves the layout when it next starts. In
    between, a node is working and must not be reported as failed for being old."""

    def test_the_old_layout_with_a_manifest_is_a_warning_that_says_it_moves_on_restart(self):
        self.mounted = {self.mimir.SIDON_ROOT}
        self.gone(LV)       # not yet at its new path -- it is at the old one
        self.gone(SDC)
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "WARN", output)
        self.assertIn("next time it starts", output)

    def test_a_node_the_rollout_has_not_reached_is_judged_by_its_fstab_as_before(self):
        self.manifest = None
        self.fstab = FSTAB_OLD
        self.mounted = {"/var/lib/hci/sidon", "/var/lib/hci/sidon/disks/sdc"}
        self.assertEqual(self.mimir.survey_sidon_mounts()[0], "PASS")

    def test_and_keeps_its_severities(self):
        self.manifest = None
        self.fstab = FSTAB_OLD
        self.mounted = {"/var/lib/hci/sidon/disks/sdc"}
        self.assertEqual(self.mimir.survey_sidon_mounts()[0], "FAIL")
        self.mounted = {"/var/lib/hci/sidon"}
        self.assertEqual(self.mimir.survey_sidon_mounts()[0], "WARN")


class PresenceIsJudgedByStatNotByAFileOrATable(unittest.TestCase):
    """The outage that shadowed a mount: `findmnt` listed it and the path no longer reached
    it. And the leftover that a mount later covers: a sentinel on the root filesystem."""

    def setUp(self):
        self.mimir = load_mimir()
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.mount = os.path.join(self.root, "disks", SDC)
        os.makedirs(self.mount)

    def stat_as(self, here, wanted):
        real_stat = os.stat

        def fake(path, *a, **k):
            if path == self.mount:
                return mock.Mock(st_dev=here)
            if path == os.path.join("/dev/disk/by-uuid", SDC):
                return mock.Mock(st_rdev=wanted)
            return real_stat(path, *a, **k)

        return mock.patch.object(os, "stat", side_effect=fake)

    def test_a_sentinel_left_on_the_root_filesystem_does_not_make_a_missing_disk_present(self):
        with io.open(os.path.join(self.mount, "disk.uid"), "w") as handle:
            handle.write("left-behind\n")
        with mock.patch.object(os.path, "exists", return_value=True):
            why = self.mimir.sidon_disk_absence(SDC, root=self.root)
        self.assertIsNotNone(why, "a sentinel on the root filesystem was believed")
        self.assertIn("not mounted", why)

    def test_a_disk_whose_device_is_not_attached_says_so(self):
        why = self.mimir.sidon_disk_absence(SDC, root=self.root)
        self.assertIn("device is not attached", why)

    def test_a_mount_of_the_right_device_with_its_sentinel_is_present(self):
        with io.open(os.path.join(self.mount, "disk.uid"), "w") as handle:
            handle.write("uid\n")
        with mock.patch.object(os.path, "ismount", return_value=True), self.stat_as(11, 11):
            self.assertIsNone(self.mimir.sidon_disk_absence(SDC, root=self.root))

    def test_a_different_filesystem_mounted_there_is_not_the_disk(self):
        with io.open(os.path.join(self.mount, "disk.uid"), "w") as handle:
            handle.write("uid\n")
        with mock.patch.object(os.path, "ismount", return_value=True), self.stat_as(10, 11):
            self.assertIn("something else", self.mimir.sidon_disk_absence(SDC, root=self.root))

    def test_the_right_filesystem_without_the_sentinel_is_not_yet_a_sidon_disk(self):
        with mock.patch.object(os.path, "ismount", return_value=True), self.stat_as(11, 11):
            self.assertIn("disk.uid", self.mimir.sidon_disk_absence(SDC, root=self.root))

    def test_the_survey_never_consults_findmnt(self):
        source = io.open(os.path.join(HERE, "mimir.py"), encoding="utf-8").read()
        body = source[source.index("def sidon_disk_absence"):source.index("def sidon_fstab_mounts")]
        self.assertNotIn("findmnt", body)


class TheCheckIsPublishedAndRunsEverywhere(unittest.TestCase):
    def setUp(self):
        with io.open(os.path.join(HERE, "mimir.py"), encoding="utf-8") as handle:
            self.source = handle.read()

    def test_it_is_wired_into_the_daemon_loop(self):
        self.assertIn("publish_sidon_mount_survey()", self.source,
                      "the survey exists and nothing calls it")
        self.assertIn("STORAGE_SURVEY_INTERVAL", self.source)

    def test_it_runs_on_every_node_not_only_the_schedule_leader(self):
        """A volume that failed to mount is a fact about one host. Asking the leader would
        miss precisely the node with the problem."""
        body = self.source[self.source.index("def main("):]
        call = body.index("publish_sidon_mount_survey()")
        leader_gate = body.find("schedules.leading()")
        if leader_gate != -1:
            self.assertLess(
                call, leader_gate,
                "the mount survey is behind the schedule-leader gate, so two of three "
                "nodes would never report their own mounts")

    def test_it_lands_in_the_table_the_console_already_renders(self):
        self.assertIn("hydra.mimir_results", self.source)
        self.assertIn("STORAGE_CHECK_NAME", self.source)
        self.assertIn("STORAGE_CHECK_CATEGORY", self.source)


if __name__ == "__main__":
    unittest.main()
