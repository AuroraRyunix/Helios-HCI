#!/usr/bin/env python3
"""`nofail` bought a boot and owed a detector.

The sidon mounts need `nofail`: without it a data disk that is late or renumbered fails
`local-fs.target` and drops the node into emergency mode with no network and no SSH, and two
of three nodes did exactly that. But nofail trades a loud failure for a silent one, and for a
storage system the silent one is worse. A node whose volume did not mount boots perfectly,
and sidon writes extent groups to the *root filesystem* at the same path -- smaller, slower,
un-replicated, and invisible the moment the real volume mounts underneath them.

That is not hypothetical. After an emergency-mode boot all three nodes came up with
`/var/lib/hci/sidon` unmounted, 202 extent groups sat unreachable on an active LV, and the
only symptom anywhere was NBD reads failing for one image. systemd was content: a nofail
mount that did not happen is not a failure.

So the property asserted here is that the condition is *reported*, and reported at the right
severity -- the extent store root missing is a FAIL, extra capacity missing is a WARN.

Run with:  python -m unittest test_sidon_mount_survey
"""

import importlib.util
import io
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

FSTAB_BOTH = """\
# /etc/fstab
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


class TheSurveyReadsFstabRatherThanAssuming(unittest.TestCase):
    """A node with two spare disks declares two more of these than a node with none, so a
    fixed list would be wrong on both."""

    def setUp(self):
        self.mimir = load_mimir()
        self.table = FSTAB_BOTH
        real_open = io.open

        def fake_open(path, *args, **kwargs):
            if path == "/etc/fstab":
                return io.StringIO(self.table)
            return real_open(path, *args, **kwargs)

        self.mimir.sidon_fstab_mounts.__globals__["open"] = fake_open
        self.addCleanup(
            self.mimir.sidon_fstab_mounts.__globals__.__setitem__, "open", real_open)

    def set_mounted(self, mounted):
        self.mimir.survey_sidon_mounts.__globals__["os"] = type(
            "FakeOs", (), {"path": type("P", (), {"ismount": staticmethod(
                lambda p: p in mounted)})})

    def test_it_finds_both_declared_mounts(self):
        self.assertEqual(
            self.mimir.sidon_fstab_mounts(),
            ["/var/lib/hci/sidon", "/var/lib/hci/sidon/disks/sdc"])

    def test_comments_and_unrelated_filesystems_are_ignored(self):
        self.table = FSTAB_BOTH + "# /var/lib/hci/sidon/disks/sdd commented out\n"
        self.assertNotIn("/var/lib/hci/sidon/disks/sdd", self.mimir.sidon_fstab_mounts())
        self.assertNotIn("/", self.mimir.sidon_fstab_mounts())

    def test_everything_mounted_passes(self):
        self.set_mounted({"/var/lib/hci/sidon", "/var/lib/hci/sidon/disks/sdc"})
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "PASS")
        self.assertIn("/var/lib/hci/sidon", output)

    def test_the_extent_store_root_missing_is_a_failure(self):
        """The case that actually happened. sidon keeps writing, to the wrong filesystem."""
        self.set_mounted({"/var/lib/hci/sidon/disks/sdc"})
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "FAIL")
        self.assertIn("/var/lib/hci/sidon", output)
        self.assertIn("root filesystem", output)
        self.assertIn("nofail", output, "the report does not say why systemd was quiet")

    def test_a_missing_data_disk_is_only_a_warning(self):
        """The store is smaller than intended rather than misplaced, which is a different
        severity -- and `.43` was in exactly this state with nobody told."""
        self.set_mounted({"/var/lib/hci/sidon"})
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "WARN")
        self.assertIn("/var/lib/hci/sidon/disks/sdc", output)

    def test_both_missing_is_a_failure_naming_both(self):
        self.set_mounted(set())
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "FAIL")
        self.assertIn("/var/lib/hci/sidon/disks/sdc", output)

    def test_a_host_that_declares_none_is_reported_not_passed(self):
        """A single-filesystem host is valid, and still worth saying out loud -- silently
        passing would make "no volumes declared" indistinguishable from "all mounted"."""
        self.table = FSTAB_NONE
        self.set_mounted(set())
        status, output = self.mimir.survey_sidon_mounts()
        self.assertEqual(status, "WARN")
        self.assertIn("root filesystem", output)


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
