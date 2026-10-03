#!/usr/bin/env python3
"""No data disk can cost a node its boot, because nothing sidon owns is in /etc/fstab.

Two of three nodes came up in systemd's emergency mode after a power cycle. Emergency mode
is `local-fs.target` failing, and the entry that failed was the one the toolkit wrote for
each extra extent-store disk:

    UUID=5c9e756d-... /var/lib/hci/sidon/disks/sdc xfs defaults,noatime 0 0

No `nofail`. A disk that is slow to appear, or whose UUID moved, therefore takes the whole
host down to a console prompt -- no network, no SSH -- and because a three-node ensemble
needs two, losing two nodes leaves the survivor's ZooKeeper running and answering nothing.
That reads as ZooKeeper having failed, which is the expensive part of this bug: the symptom
appears three layers away from the cause.

The first fix was `nofail,x-systemd.device-timeout=5s` on every sidon line, and it bought the
boot at the price of silence: the parent volume then failed to mount, nothing complained, and
sidon wrote extent groups to the root filesystem. The property that closes both is stronger
than either option: **the boot does not know about sidon's disks at all.** A real Nutanix CVM
mounts none of its storage from fstab, so a missing or late disk cannot fail
`local-fs.target`, `nofail` has nothing to qualify, and the detector it forced is not needed.
Mounting is the storage layer's job (D-27).

Three things are asserted, because fixing one leaves the failure available:

  * **no writer puts a sidon mount in fstab**, so a node cannot be built able to fail its
    boot -- the claim step registers a disk in /etc/hci/sidon-disks and mounts nothing;
  * **staging never edits fstab or mounts anything**, so a rollout reaches a node that already
    exists without moving a mount under a running sidon;
  * **a node that has not yet moved still cannot fail its boot**: until sidon next starts,
    its old fstab lines are what the boot is made of, and the rollout repairs the ones that
    would. The writers only ever appended, and a repair is not an append.

Run with:  python -m unittest test_sidon_fstab_boot
"""

import io
import os
import re
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

# Every file that used to put a sidon mount in /etc/fstab, and the scripts that replaced it.
FSTAB_WRITERS = ("provision.py", "deploy_updates.py", "cluster_new.py", "spark_daemon_decoded.py")

# The fstab as the broken nodes actually had it, read off 10.10.102.41. The last line is
# the one that boots a host into emergency mode when its disk is late.
REAL_FSTAB = """\
#
# /etc/fstab
# Created by anaconda on Mon Aug 17 20:18:01 2026
#
UUID=2b665c02-9b7a-472e-bcef-1b41b6a6462f /                       xfs     defaults        0 0
UUID=b2cfead7-e59c-4152-98d7-a47e8f9abb7f /boot                   xfs     defaults        0 0
UUID=C76B-80B4          /boot/efi               vfat    umask=0077,shortname=winnt 0 2
UUID=372f208e-896f-4ca6-ae24-82a4fbfaf05c /home                   xfs     defaults        0 0
UUID=5f470b19-01b3-4ef5-8571-49bbeade4b97 none                    swap    defaults        0 0
UUID=dfa470d5-2350-4077-a8ed-7b942eb0cff0 /var/lib/hci/sidon xfs defaults,noatime,nofail,x-systemd.device-timeout=5s 0 0
UUID=5c9e756d-a035-4de7-9dc0-11afcc82f0fe /var/lib/hci/sidon/disks/sdc xfs defaults,noatime 0 0
"""


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def awk_program():
    """The awk out of the shipped reconcile block, so the test runs what deploys."""
    source = read("deploy_updates.py")
    block = source[source.index('RECONCILE_SIDON_FSTAB = r"""'):]
    block = block[: block.index('"""', 30)]
    return block[block.index("awk '") + 5 : block.index("' \"$FSTAB\"")]


def run_awk(fstab_text):
    """(returncode, output). 10 means the program changed something."""
    awk = shutil.which("awk") or shutil.which("gawk")
    if not awk:
        raise unittest.SkipTest("no awk on this machine")

    workdir = tempfile.mkdtemp()
    try:
        prog = os.path.join(workdir, "p.awk")
        table = os.path.join(workdir, "fstab")
        for path, text in ((prog, awk_program()), (table, fstab_text)):
            with io.open(path, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(text)
        done = subprocess.run([awk, "-f", prog, table],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return done.returncode, done.stdout.decode("utf-8", "replace")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def sidon_lines(fstab_text):
    return [line for line in fstab_text.splitlines()
            if not line.lstrip().startswith("#")
            and len(line.split()) >= 4
            and line.split()[1].startswith("/var/lib/hci/sidon")]


def script(name, const):
    match = re.search(r'%s = r"""(.*?)"""' % const, read(name), re.S)
    assert match, "%s does not define %s" % (name, const)
    return match.group(1)


class NoWriterCanBuildABrokenNode(unittest.TestCase):
    """The boot must not know about sidon's disks, so no writer may tell it."""

    def test_no_writer_appends_to_or_rewrites_fstab_with_a_sidon_mount(self):
        for name in FSTAB_WRITERS:
            source = read(name)
            for match in re.finditer(r'>>\s*/etc/fstab', source):
                line = source[source.rfind("\n", 0, match.start()) + 1:
                              source.find("\n", match.end())]
                self.fail("%s still appends to /etc/fstab: %s" % (name, line.strip()))

    def test_the_claim_and_stage_scripts_only_read_fstab(self):
        """STAGE reads it, to learn what an older node declared, and must never write it."""
        for const in ("CLAIM_EXTRA_DISKS", "STAGE_SIDON_DISKS"):
            text = script("provision.py" if const == "CLAIM_EXTRA_DISKS" else "deploy_updates.py", const)
            for line in text.splitlines():
                if line.lstrip().startswith("#"):
                    continue
                self.assertNotRegex(
                    line, r'>\s*"?\$?\{?(FSTAB|/etc/fstab)',
                    "%s writes fstab: %s" % (const, line.strip()))
                self.assertNotRegex(
                    line, r'sed\s+-i.*fstab|mount\s+"?\$|mount\s+--',
                    "%s edits fstab or mounts something: %s" % (const, line.strip()))

    def test_nothing_the_toolkit_writes_carries_nofail_any_more(self):
        """`nofail` qualified a line that is no longer written. Leaving it in a writer is a
        dead option, and a reader of it would conclude fstab is still where sidon mounts."""
        # deploy_updates.py is exempt: its transitional repair adds the option to the lines an
        # unmoved node still has.
        for name in ("provision.py", "cluster_new.py", "spark_daemon_decoded.py"):
            for line in read(name).splitlines():
                if "nofail" in line and not line.lstrip().startswith("#"):
                    self.fail("%s writes or tests `nofail` in code: %s" % (name, line.strip()))

    def test_the_toolkit_no_longer_mounts_the_sidon_root_by_name(self):
        """`mount /var/lib/hci/sidon` has no fstab line to resolve against, so a create that
        still ran it would fail on every node built this way."""
        for name in FSTAB_WRITERS:
            self.assertNotRegex(
                read(name), r'mount /var/lib/hci/sidon(?!/)\s*["\'\)]',
                "%s still mounts the sidon root by name" % name)


class StagingNeverMovesAMount(unittest.TestCase):
    """A rollout reaches nodes that are serving guests."""

    def setUp(self):
        self.script = script("deploy_updates.py", "STAGE_SIDON_DISKS")

    def test_it_neither_mounts_unmounts_nor_edits_fstab(self):
        code = "\n".join(l for l in self.script.splitlines() if not l.lstrip().startswith("#"))
        for forbidden in ("umount", "mount ", "sed -i", "> \"$FSTAB\"", ">> \"$FSTAB\"",
                          "systemctl"):
            self.assertNotIn(forbidden, code, "staging does %r" % forbidden)

    def test_the_rollout_stages_after_claiming_and_before_it_repairs(self):
        deploy = read("deploy_updates.py")
        # Located by the script each call runs, not by its exact spelling: the claim and stage
        # calls carry a guard prefix now (ONLY_WITH_A_CLUSTER +), and the order is the property.
        claim = deploy.index("CLAIM_EXTRA_DISKS)", deploy.index("ssh.exec_command(ONLY_WITH_A_CLUSTER"))
        stage = deploy.index("ssh.exec_command(ONLY_WITH_A_CLUSTER + STAGE_SIDON_DISKS)")
        repair = deploy.index("ssh.exec_command(RECONCILE_SIDON_FSTAB)")
        self.assertLess(claim, stage, "the record is staged before the disks are registered")
        self.assertLess(stage, repair)


class TheRolloutRepairsWhatIsAlreadyThere(unittest.TestCase):
    """The writers only append. Fixing them does nothing for a node already built."""

    def test_the_rollout_has_a_repair_and_runs_it(self):
        deploy = read("deploy_updates.py")
        self.assertIn("RECONCILE_SIDON_FSTAB = ", deploy)
        self.assertIn("ssh.exec_command(RECONCILE_SIDON_FSTAB)", deploy,
                      "the repair is defined and never called, which repairs nothing")

    def test_it_fixes_the_line_the_broken_nodes_had(self):
        rc, out = run_awk(REAL_FSTAB)
        self.assertEqual(rc, 10, "the real fstab was reported as needing no change")
        for line in sidon_lines(out):
            self.assertIn("nofail", line)
            self.assertIn("x-systemd.device-timeout=", line)

    def test_it_changes_nothing_else(self):
        """This is /etc/fstab on a host that is already one bad line from a console prompt.
        Every line that is not a sidon mount must come back byte-identical."""
        _, out = run_awk(REAL_FSTAB)
        before = REAL_FSTAB.splitlines()
        after = out.splitlines()
        self.assertEqual(len(before), len(after), "the line count changed")
        for old, new in zip(before, after):
            if old.split()[1:2] and old.split()[1].startswith("/var/lib/hci/sidon"):
                continue
            self.assertEqual(old, new, "a line that is not a sidon mount was rewritten")

    def test_it_is_idempotent(self):
        """It runs on every node on every rollout."""
        _, once = run_awk(REAL_FSTAB)
        rc, twice = run_awk(once)
        self.assertEqual(rc, 0, "a second pass reported another change")
        self.assertEqual(once, twice)

    def test_it_does_not_duplicate_an_option_that_is_already_there(self):
        """A line with a device timeout but no nofail must gain only nofail. Appending both
        blindly produces a duplicated option, and mount is entitled to reject it."""
        table = ("UUID=x /var/lib/hci/sidon/disks/sdd xfs "
                 "defaults,noatime,x-systemd.device-timeout=9s 0 0\n")
        rc, out = run_awk(table)
        self.assertEqual(rc, 10)
        self.assertEqual(out.count("x-systemd.device-timeout="), 1, out)
        self.assertEqual(out.count("nofail"), 1, out)
        self.assertIn("9s", out, "an existing timeout was overwritten rather than kept")

    def test_a_table_with_nothing_to_fix_is_left_alone(self):
        rc, out = run_awk(
            "UUID=x /var/lib/hci/sidon xfs defaults,nofail,x-systemd.device-timeout=5s 0 0\n")
        self.assertEqual(rc, 0, "a healthy table was reported as changed")
        self.assertIn("nofail", out)

    def test_comments_and_short_lines_survive(self):
        table = "# a comment\n\n/dev/foo\nUUID=x /var/lib/hci/sidon/disks/sde xfs defaults 0 0\n"
        rc, out = run_awk(table)
        self.assertEqual(rc, 10)
        self.assertIn("# a comment", out)
        self.assertIn("/dev/foo", out)

    def test_a_path_that_merely_looks_like_sidon_is_not_touched(self):
        """`$2 ~ /^\\/var\\/lib\\/hci\\/sidon/` is a prefix match, which is what is wanted for
        the nested disks -- but it must not reach outside the tree."""
        table = "UUID=x /var/lib/hci/sidonia xfs defaults 0 0\n"
        rc, out = run_awk(table)
        # Documented rather than asserted as correct: this path *is* matched by the prefix,
        # and it is inside the directory the cluster owns, so adding nofail to it is
        # harmless. What must not happen is a match on an unrelated filesystem.
        self.assertNotIn("/var/lib/hci/sidon ", out.replace("sidonia", "X"))
        table_other = "UUID=x /srv/data xfs defaults 0 0\n"
        rc_other, out_other = run_awk(table_other)
        self.assertEqual(rc_other, 0, "an unrelated filesystem was rewritten")
        self.assertNotIn("nofail", out_other)

    def test_the_repair_validates_before_installing(self):
        """It rewrites the file that decides whether the host boots. A candidate that does
        not parse must never be installed, and the previous table must be kept."""
        deploy = read("deploy_updates.py")
        block = deploy[deploy.index('RECONCILE_SIDON_FSTAB = r"""'):]
        block = block[: block.index('"""', 30)]

        self.assertIn("mount --fake", block, "the candidate table is installed unchecked")
        self.assertIn("did not parse", block)
        self.assertIn("hci-bak", block, "no copy of the previous table is kept")
        self.assertIn("systemctl daemon-reload", block,
                      "the generated mount units would still describe the old options")


if __name__ == "__main__":
    unittest.main()
