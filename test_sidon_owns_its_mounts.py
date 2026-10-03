#!/usr/bin/env python3
"""Sidon mounts its own disks, by UUID, as siblings, and uses none that is not there.

Two outages in one evening came from how the extent store was mounted. A late data disk
failed `local-fs.target` because the toolkit wrote it to /etc/fstab without `nofail`, and
`nofail` then made the next failure silent: the parent volume did not mount, sidon wrote
extent groups to the root filesystem at the same path, and mounting the parent over a child
that was nested inside it hid the child -- `findmnt` still listed it, the path no longer
reached it, and sixteen extent groups were unreachable.

A real Nutanix CVM does none of that. Nothing storage-related is in its fstab, every parent
directory is a plain directory, the disk mounts are siblings named by serial, and mounting is
the storage layer's job. D-27 makes Helios do the same, and this file holds the properties of
the toolkit half of that. The properties of sidon's half -- that a path whose disk is not
mounted is refused, that a sentinel left on the root filesystem proves nothing, that a
shadowed child is recovered, that the old layout is left children first and only while sidon
is not running -- are Rust tests in `sidon/src/mounts.rs` and `sidon/src/extent.rs`, run on a
node, and were exercised against real loop-mounted XFS (docs/dfs/multi_disk.md).

What is asserted here:

  * the **claim** step registers a disk by filesystem UUID and mounts nothing, writes nothing
    to fstab, never adopts a filesystem that is not XFS, and is idempotent;
  * the **stage** step -- the one a rollout runs on a node that already has the old layout --
    builds /etc/hci/sidon-disks from the volume the toolkit carved and from the fstab lines
    the old layout left, *converges* a record that is wrong rather than appending to it, and
    never mounts, unmounts or edits fstab, because a rollout must not move a mount under a
    running sidon;
  * **destroy** takes mounts off deepest first and forgets the record, and never removes a
    tree across a mount;
  * sidon's **source** keeps the one rule that matters: every path that is backed by a disk
    is asked of the mount layer, none is joined onto the root.

Run with:  python -m unittest test_sidon_owns_its_mounts
"""

import io
import os
import re
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

LV = "dfa470d5-2350-4077-a8ed-7b942eb0cff0"
SDC = "5c9e756d-a035-4de7-9dc0-11afcc82f0fe"
OTHER = "0f0f0f0f-1111-2222-3333-444444444444"

# The fstab from a node built before D-27, as it was read off 10.10.102.41.
REAL_FSTAB = """\
#
# /etc/fstab
#
UUID=2b665c02-9b7a-472e-bcef-1b41b6a6462f /                       xfs     defaults        0 0
UUID=b2cfead7-e59c-4152-98d7-a47e8f9abb7f /boot                   xfs     defaults        0 0
UUID=5f470b19-01b3-4ef5-8571-49bbeade4b97 none                    swap    defaults        0 0
UUID=%s /var/lib/hci/sidon xfs defaults,noatime,nofail,x-systemd.device-timeout=5s 0 0
UUID=%s /var/lib/hci/sidon/disks/sdc xfs defaults,noatime,nofail,x-systemd.device-timeout=5s 0 0
""" % (LV, SDC)

# `.43` differs: the disk filling the sdc role is the kernel's /dev/sdb, and its line says so.
FSTAB_43 = REAL_FSTAB.replace("UUID=" + SDC, "/dev/sdb")


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def script(name, const):
    match = re.search(r'%s = r"""(.*?)"""' % const, read(name), re.S)
    assert match, "%s does not define %s" % (name, const)
    return match.group(1)


def a_bash():
    """A bash that can run the shipped scripts, or None. Not WSL's: it cannot see our paths."""
    candidates = [shutil.which("bash"),
                  r"C:\Program Files\Git\bin\bash.exe",
                  r"C:\Program Files\Git\usr\bin\bash.exe"]
    for candidate in candidates:
        if not candidate or "System32" in candidate:
            continue
        try:
            done = subprocess.run([candidate, "-c", "command -v awk >/dev/null && echo ok"],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
        except (OSError, subprocess.SubprocessError):
            continue
        if done.stdout.strip() == b"ok":
            return candidate
    return None


STUBS = {
    # blkid DEV | blkid -s UUID|TYPE -o value DEV, against ./devs: "<dev> <uuid> <type>".
    "blkid": r'''#!/bin/bash
dev="${@: -1}"
row="$(awk -v d="$dev" '$1 == d { print; exit }' devs 2>/dev/null)"
[ -n "$row" ] || exit 2
case "$*" in
  *"-s UUID"*) echo "$row" | awk '{ print $2 }' ;;
  *"-s TYPE"*) echo "$row" | awk '{ print $3 }' ;;
esac
exit 0
''',
    # ./disks: "<name> <bytes> <mountpoint or -> <partitions: part|none>"
    "lsblk": r'''#!/bin/bash
last="${@: -1}"
case "$*" in
  "-b -n -o NAME,SIZE,TYPE") awk '{ print $1, $2, "disk" }' disks ;;
  "-n -o MOUNTPOINT "*) awk -v n="${last#/dev/}" '$1 == n && $3 != "-" { print $3 }' disks ;;
  "-n -o TYPE "*) echo disk; awk -v n="${last#/dev/}" '$1 == n && $4 == "part" { print "part" }' disks ;;
esac
exit 0
''',
    "pvs": "#!/bin/bash\nexit 0\n",
    "mkfs.xfs": r'''#!/bin/bash
dev="${@: -1}"
echo "mkfs.xfs $dev" >> calls.log
echo "$dev 99999999-0000-0000-0000-$(printf '%012d' $RANDOM) xfs" >> devs
''',
    "mount": "#!/bin/bash\necho \"mount $*\" >> calls.log\n",
    "umount": "#!/bin/bash\necho \"umount $*\" >> calls.log\n",
    "systemctl": "#!/bin/bash\necho \"systemctl $*\" >> calls.log\n",
    "chgrp": "#!/bin/bash\nexit 1\n",
}


class OnANode(unittest.TestCase):
    """A directory standing in for a node: ./etc, ./var, ./devs, ./disks and stubbed tools."""

    def setUp(self):
        self.bash = a_bash()
        if not self.bash:
            self.skipTest("no bash with awk on this machine")
        self.node = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.node, ignore_errors=True)
        os.makedirs(os.path.join(self.node, "bin"))
        for name, body in STUBS.items():
            self.write(os.path.join("bin", name), body)
        self.write("devs", "")
        self.write("disks", "")
        self.write("calls.log", "")

    def write(self, rel, text):
        path = os.path.join(self.node, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with io.open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)

    def write_fstab(self, text):
        """The fstab as the node had it, its mount points moved under the stand-in root the
        same way run_script moves the script's. Nothing else about a line changes."""
        self.fstab = text.replace("/var/lib/hci/sidon", "./var/lib/hci/sidon")
        self.write("etc/fstab", self.fstab)

    def read_node(self, rel):
        try:
            with io.open(os.path.join(self.node, rel), encoding="utf-8") as handle:
                return handle.read()
        except FileNotFoundError:
            return None

    def attach(self, dev, uuid, fstype="xfs"):
        self.write("devs", (self.read_node("devs") or "") + "%s %s %s\n" % (dev, uuid, fstype))

    def run_script(self, text):
        """Run a shipped script with its absolute paths pointed into the stand-in node."""
        text = (text.replace("/etc/hci/sidon-disks", "./etc/hci/sidon-disks")
                    .replace("mkdir -p /etc/hci", "mkdir -p ./etc/hci")
                    .replace("/etc/fstab", "./etc/fstab")
                    .replace("/var/lib/hci/sidon", "./var/lib/hci/sidon"))
        text = 'export PATH="$PWD/bin:$PATH"\n' + text
        done = subprocess.run([self.bash, "-c", text], cwd=self.node,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        return done.returncode, done.stdout.decode("utf-8", "replace"), done.stderr.decode("utf-8", "replace")

    def manifest(self):
        text = self.read_node("etc/hci/sidon-disks")
        if text is None:
            return None
        return [tuple(l.split()[:2]) for l in text.splitlines()
                if l.strip() and not l.startswith("#")]

    def calls(self):
        return self.read_node("calls.log").splitlines()


class TheClaimStepRegistersAndMountsNothing(OnANode):
    def setUp(self):
        super().setUp()
        self.script = script("provision.py", "CLAIM_EXTRA_DISKS")
        self.write("disks", "\n".join([
            "sda 500000000000 / part",             # the system disk
            "sdb 300000000000 - none",             # empty, already XFS
            "sdc 300000000000 - none",             # somebody's ext4
            "sdd 10000000000 - none",              # too small
            "sde 300000000000 - part",             # carries partitions
            "sdf 300000000000 - none",             # blank
            "sdg 300000000000 /mnt/data none",     # mounted somewhere
        ]) + "\n")
        self.attach("/dev/sdb", SDC)
        self.attach("/dev/sdc", OTHER, "ext4")
        self.fstab_before = "UUID=x / xfs defaults 0 0\n"
        self.write_fstab(self.fstab_before)

    def test_a_disk_is_registered_by_filesystem_uuid_as_an_extent_disk(self):
        rc, out, err = self.run_script(self.script)
        self.assertEqual(rc, 0, err)
        self.assertIn((SDC, "extent"), self.manifest())

    def test_it_mounts_nothing_and_writes_nothing_to_fstab(self):
        """A line in fstab is how one late disk failed local-fs.target, and a mount from
        here is a mount sidon did not make -- which is how a disk ends up nested."""
        self.run_script(self.script)
        self.assertFalse([c for c in self.calls() if c.startswith(("mount", "umount", "systemctl"))],
                         self.calls())
        self.assertEqual(self.read_node("etc/fstab"), self.fstab_before)

    def test_only_a_blank_disk_is_formatted(self):
        self.run_script(self.script)
        formatted = [c for c in self.calls() if c.startswith("mkfs.xfs")]
        self.assertEqual(formatted, ["mkfs.xfs /dev/sdf"], "a disk that was not blank was wiped")

    def test_a_filesystem_that_is_not_xfs_is_left_alone_and_not_adopted(self):
        rc, out, _ = self.run_script(self.script)
        self.assertNotIn(OTHER, (self.read_node("etc/hci/sidon-disks") or ""))
        self.assertIn("skipped /dev/sdc", out)

    def test_in_use_small_and_partitioned_disks_are_not_candidates(self):
        self.run_script(self.script)
        registered = self.read_node("etc/hci/sidon-disks")
        self.assertEqual(len(self.manifest()), 2, registered)   # sdb and the freshly formatted sdf

    def test_running_it_again_adds_nothing(self):
        self.run_script(self.script)
        once = self.read_node("etc/hci/sidon-disks")
        self.run_script(self.script)
        self.assertEqual(self.read_node("etc/hci/sidon-disks"), once,
                         "a second pass changed the record")


class TheStageStepBuildsTheRecordFromWhatTheNodeAlreadyHas(OnANode):
    """What a rollout runs on a node that is serving guests."""

    def setUp(self):
        super().setUp()
        self.script = script("deploy_updates.py", "STAGE_SIDON_DISKS")

    def test_a_fresh_node_records_the_carved_volume_as_the_journal_volume(self):
        self.attach("/dev/vg_aether/sidon", LV)
        rc, out, err = self.run_script(self.script)
        self.assertEqual(rc, 0, err)
        self.assertEqual(self.manifest(), [(LV, "journal")])

    def test_the_real_fstab_of_an_existing_node_is_read_and_never_written(self):
        self.attach("/dev/vg_aether/sidon", LV)
        self.write_fstab(REAL_FSTAB)
        rc, out, err = self.run_script(self.script)
        self.assertEqual(rc, 0, err)
        self.assertEqual(self.manifest(), [(LV, "journal"), (SDC, "extent")])
        self.assertEqual(self.read_node("etc/fstab"), self.fstab,
                         "staging edited fstab: a rollout must not move a mount")
        self.assertFalse(self.calls(), "staging mounted, unmounted or reloaded something: %s" % self.calls())

    def test_the_old_fstab_alone_is_enough_when_the_volume_is_not_visible(self):
        """The volume's device may not be up yet when a rollout runs. The lines the old
        layout left are a second source, and they must carry the journal volume too."""
        self.write_fstab(REAL_FSTAB)
        rc, _, err = self.run_script(self.script)
        self.assertEqual(rc, 0, err)
        self.assertEqual(self.manifest(), [(LV, "journal"), (SDC, "extent")])

    def test_the_node_where_the_second_disk_is_the_kernels_sdb_resolves_the_same_way(self):
        """`.43`: the line names /dev/sdb, which is the disk filling the sdc role. The record
        is by UUID, so the name the kernel happens to give it stops mattering."""
        self.attach("/dev/vg_aether/sidon", LV)
        self.attach("/dev/sdb", SDC)
        self.write_fstab(FSTAB_43)
        rc, _, err = self.run_script(self.script)
        self.assertEqual(rc, 0, err)
        self.assertEqual(self.manifest(), [(LV, "journal"), (SDC, "extent")])

    def test_staging_twice_is_a_no_op(self):
        self.attach("/dev/vg_aether/sidon", LV)
        self.write_fstab(REAL_FSTAB)
        self.run_script(self.script)
        once = self.read_node("etc/hci/sidon-disks")
        rc, out, _ = self.run_script(self.script)
        self.assertTrue(out.splitlines()[0].endswith("ok"), out)
        self.assertEqual(self.read_node("etc/hci/sidon-disks"), once)

    def test_a_record_that_is_wrong_is_repaired_and_not_merely_appended_to(self):
        """The bug class the writers had twice: they only ever appended, so a node that
        already had the wrong state kept it. Here the volume is recorded as an extent disk
        and a stale journal volume is listed; staging must correct both."""
        self.attach("/dev/vg_aether/sidon", LV)
        self.write("etc/hci/sidon-disks", "%s extent\n%s journal\n" % (LV, OTHER))
        rc, _, err = self.run_script(self.script)
        self.assertEqual(rc, 0, err)
        self.assertEqual(self.manifest(), [(LV, "journal"), (OTHER, "extent")])

    def test_a_disk_registered_by_hand_survives(self):
        self.attach("/dev/vg_aether/sidon", LV)
        self.write("etc/hci/sidon-disks", "%s extent\n" % OTHER)
        self.run_script(self.script)
        self.assertIn((OTHER, "extent"), self.manifest())

    def test_nothing_that_cannot_be_a_directory_name_is_written(self):
        """The UUID becomes a directory under disks/. Anything that could climb out is
        dropped here as well as refused by sidon."""
        self.attach("/dev/vg_aether/sidon", LV)
        self.write("etc/hci/sidon-disks", "../../etc extent\na/b extent\n")
        self.run_script(self.script)
        self.assertEqual(self.manifest(), [(LV, "journal")])

    def test_a_node_with_no_volume_gets_no_record_rather_than_an_empty_one(self):
        """An empty record would make sidon refuse to start for want of a journal volume."""
        rc, out, _ = self.run_script(self.script)
        self.assertEqual(rc, 0)
        self.assertIsNone(self.read_node("etc/hci/sidon-disks"))
        self.assertIn("no journal volume", out)

    def test_the_socket_directory_libvirt_names_is_kept_where_it_is(self):
        """The domain XML of existing VMs references /var/lib/hci/sidon/nbd/<vdisk>.sock."""
        self.attach("/dev/vg_aether/sidon", LV)
        self.run_script(self.script)
        self.assertTrue(os.path.isdir(os.path.join(self.node, "var", "lib", "hci", "sidon", "nbd")))


class DestroyTakesMountsOffInTheRightOrderAndForgetsThem(unittest.TestCase):
    def test_the_two_copies_of_the_teardown_are_the_same_command(self):
        import cluster_new
        import spark_daemon_decoded
        self.assertEqual(cluster_new.SIDON_TEARDOWN, spark_daemon_decoded.SIDON_TEARDOWN)

    def test_it_unmounts_deepest_first_and_repeats_for_a_mount_that_was_covered(self):
        import cluster_new
        t = cluster_new.SIDON_TEARDOWN
        self.assertIn("sort -r", t, "children must come off before the parent that holds them")
        self.assertIn("for i in 1 2 3 4 5", t, "a covered mount appears only when its cover is gone")
        self.assertIn("/proc/self/mountinfo", t)
        self.assertNotIn("findmnt", t)

    def test_it_forgets_the_record_and_the_old_fstab_lines(self):
        import cluster_new
        self.assertIn("rm -f /etc/hci/sidon-disks", cluster_new.SIDON_TEARDOWN)
        self.assertIn("/etc/fstab", cluster_new.SIDON_TEARDOWN)

    def test_it_never_replaces_fstab_with_an_empty_file(self):
        import cluster_new
        self.assertIn("-s /etc/fstab.hci-new", cluster_new.SIDON_TEARDOWN)

    def test_no_tree_is_removed_across_a_mount(self):
        """If an unmount failed, `rm -rf` of the sidon root would delete data on the disk
        still mounted under it."""
        for name in ("cluster_new.py", "spark_daemon_decoded.py"):
            for line in read(name).splitlines():
                if "rm -rf" in line and "/var/lib/hci/sidon" in line:
                    self.assertIn("--one-file-system", line, "%s: %s" % (name, line.strip()))

    def test_nothing_lazily_unmounts_the_sidon_root_by_name_any_more(self):
        for name in ("cluster_new.py", "spark_daemon_decoded.py"):
            self.assertNotRegex(read(name), r'umount -l /var/lib/hci/sidon[ "|;]',
                                "%s unmounts the root, which is a plain directory now" % name)

    def test_stopping_the_cluster_does_not_unmount_under_a_running_sidon(self):
        source = read("spark_daemon_decoded.py")
        body = source[source.index("def drain_local_storage"):source.index("def write_desired_cluster_state")]
        self.assertNotIn("umount", body)


class SidonKeepsTheOneRuleThatMatters(unittest.TestCase):
    """Source-level properties; the behaviour is in the Rust tests."""

    SRC = os.path.join("sidon", "src")

    def test_no_path_backed_by_a_disk_is_joined_onto_the_root(self):
        """The journal, the replica state and every extent group live on a mount. A
        `root.join("journal")` is the bug: it is the root filesystem when the mount is gone."""
        for dirpath, _, files in os.walk(os.path.join(HERE, self.SRC)):
            for name in files:
                if not name.endswith(".rs") or name == "mounts.rs":
                    continue
                text = read(os.path.join(os.path.relpath(dirpath, HERE), name))
                code = text.split("#[cfg(test)]")[0]
                for bad in ('root.join("journal")', 'root.join("replica")',
                            'root.join("replica-egroups")', 'root.join("egroups")'):
                    for line in code.splitlines():
                        # The one exemption: `discover_disks_with`, which only an unmanaged
                        # node (no /etc/hci/sidon-disks) ever reaches, and which keeps the
                        # old behaviour for it and for development hosts.
                        if line.strip() == 'let legacy = root.join("egroups");':
                            continue
                        if bad in line and not line.lstrip().startswith("//"):
                            self.fail("%s joins %s onto the root: %s" % (name, bad, line.strip()))

    def test_the_daemon_converges_the_mounts_before_it_opens_anything(self):
        control = read(os.path.join(self.SRC, "control.rs"))
        new = control[control.index("pub fn new(cfg: DaemonConfig)"):]
        self.assertLess(new.index("crate::mounts::prepare"), new.index("create_dir_all"))

    def test_a_missing_journal_volume_stops_the_daemon(self):
        control = read(os.path.join(self.SRC, "control.rs"))
        self.assertIn("report.journal_present()", control)
        self.assertIn("will not start", control)

    def test_an_extent_group_is_not_created_under_a_mount_that_has_gone(self):
        extent = read(os.path.join(self.SRC, "extent.rs"))
        create = extent[extent.index("pub fn create(&self"):]
        self.assertLess(create.index(".present()"), create.index("OpenOptions::new()"))

    def test_the_move_between_disks_checks_both_ends_are_mounted(self):
        placement = read(os.path.join(self.SRC, "extent", "placement.rs"))
        copy = placement[placement.index("pub fn stage_copy"):placement.index("pub fn stage_publish")]
        self.assertIn(".present()", copy)

    def test_the_command_that_moves_the_layout_refuses_a_running_sidon(self):
        mounts = read(os.path.join(self.SRC, "mounts.rs"))
        self.assertIn("UnixStream::connect", mounts)
        self.assertIn("Refusing to change mounts", mounts)
        self.assertIn('Some("mounts")', read(os.path.join(self.SRC, "main.rs")))

    def test_the_old_layout_is_never_left_with_a_lazy_unmount(self):
        mounts = read(os.path.join(self.SRC, "mounts.rs"))
        self.assertIn("umount2(target.as_ptr(), 0)", mounts)

    def test_every_mount_is_proven_by_stat_and_never_by_the_mount_table(self):
        mounts = read(os.path.join(self.SRC, "mounts.rs"))
        probe = mounts[mounts.index("pub fn probe("):mounts.index("/// What a disk's mount")]
        self.assertNotIn("mountinfo", probe)
        self.assertNotIn("findmnt", mounts.split("#[cfg(test)]")[0].replace("`findmnt`", ""))


if __name__ == "__main__":
    unittest.main()
