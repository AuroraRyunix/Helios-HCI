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

    def test_it_names_a_disk_by_uuid_and_mounts_nothing(self):
        """Device names are not stable across reboots; a disk named /dev/sdc can come back
        as something else and take another disk's place in the store. The disk is recorded
        by filesystem UUID and sidon mounts it -- the claim step mounts nothing and writes
        nothing to fstab (test_sidon_owns_its_mounts.py runs it to prove so)."""
        self.assertIn("blkid -s UUID -o value", self.script)
        self.assertIn("/etc/hci/sidon-disks", self.script)
        self.assertNotIn("UUID=$uuid", self.script)


class EveryDeploymentPathClaimsIdentically(unittest.TestCase):
    """Provisioning claims disks for a new node, the rollout reaches nodes that already
    exist, and `cluster create` rebuilds the volume after a destroy. A difference between any
    two means a disk laid out one way on some nodes and another way on the rest."""

    COPIES = {
        "CLAIM_EXTRA_DISKS": ("provision.py", "deploy_updates.py", "cluster_new.py",
                              "spark_daemon_decoded.py"),
        "STAGE_SIDON_DISKS": ("provision.py", "deploy_updates.py", "cluster_new.py",
                              "spark_daemon_decoded.py"),
        "CARVE_SIDON_VOLUME": ("provision.py", "cluster_new.py", "spark_daemon_decoded.py"),
    }

    def test_the_copies_are_the_same_script(self):
        for const, files in self.COPIES.items():
            first = claim_script(files[0], const)
            for other in files[1:]:
                self.assertEqual(
                    first, claim_script(other, const),
                    "%s has drifted between %s and %s" % (const, files[0], other))

    def test_the_create_path_prepares_storage_before_it_starts_sidon(self):
        """`cluster destroy` removes the volume group, and a create that only re-claimed the
        first disk left a thin pool and no volume for sidon -- and the `mount` that followed
        had no fstab line to resolve."""
        for name in ("cluster_new.py", "spark_daemon_decoded.py"):
            source = read(name)
            carve = source.index("shell_script_command(CARVE_SIDON_VOLUME)")
            claim = source.index("shell_script_command(CLAIM_EXTRA_DISKS)")
            stage = source.index("shell_script_command(STAGE_SIDON_DISKS)")
            self.assertLess(carve, claim, name)
            self.assertLess(claim, stage, name)


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
        # The rule itself moved to placement.rs when it learned to prefer a disk class; with
        # no preference it is unchanged.
        self.assertIn("best_free", read(os.path.join("sidon", "src", "extent", "placement.rs")))

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


def rust_source(*parts):
    return read(os.path.join("sidon", "src", *parts))


def rust_function(source, signature):
    """The text of one Rust function, from its signature to the next item at the same
    indent. Enough to assert what a function does *not* contain."""
    start = source.index(signature)
    indent = len(source[:start].rsplit("\n", 1)[-1])
    end = re.search(r"\n%s\}" % (" " * indent), source[start + len(signature):])
    return source[start:start + len(signature) + (end.end() if end else len(source))]


class ADiskIsKeyedByWhatItCarries(unittest.TestCase):
    """`disks/sdc` already names the wrong device on one node.

    Kernel names are assigned in probe order, so a directory called `sdc` is a guess about
    which disk it is. Placement and anything an operator is told about a disk must not
    depend on that guess, or the day the probe reorders a node's disks they are attributed
    to each other.
    """

    def setUp(self):
        self.placement = rust_source("extent", "placement.rs")
        self.extent = rust_source("extent.rs")

    def test_a_disk_has_an_identity_that_is_not_its_directory_name(self):
        self.assertIn("pub uid: String", self.extent)
        self.assertIn("pub fn identify(", self.placement)
        self.assertIn('UID_FILE: &str = "disk.uid"', self.placement)

    def test_the_identity_is_written_on_the_disk_not_derived_from_the_label(self):
        body = rust_function(self.placement, "pub fn identify(")
        self.assertIn("create_new", body)
        self.assertIn("sync_all", body)

    def test_a_disk_that_cannot_hold_its_identity_says_it_is_not_stable(self):
        self.assertIn("uid_persisted", self.extent)
        self.assertIn("unpersisted:", self.placement)

    def test_the_surplus_copy_ledger_is_keyed_by_identity(self):
        tier = rust_source("purah", "tier.rs")
        body = rust_function(tier, "pub fn step(")
        self.assertIn(".uid.clone()", body)
        self.assertNotIn(".id.clone(), ", body.split("let key")[0])

    def test_a_disk_is_named_to_the_mover_by_identity_first(self):
        body = rust_function(self.placement, "pub fn resolve_disk(")
        self.assertLess(body.index("d.uid == name"), body.index("d.id == name"))

    def test_capacity_reports_the_identity_beside_the_label_and_the_device(self):
        control = rust_source("control.rs")
        for field in ('"uid"', '"label"', '"device"', '"tier"'):
            self.assertIn(field, rust_function(control, "fn op_capacity("))


class TheRolloutShipsEverySourceFile(unittest.TestCase):
    """A module that is a directory used to be skipped by the rollout.

    `deploy_updates.upload_crate` listed `src/` without descending, so `src/extent/placement.rs`
    would never have reached a node. The failure is a compile error *on the node*, after the
    script has reported the upload as done -- which is the way a toolkit gap turns into a
    hand-patched node. The script is run at import, so the functions are lifted out of its
    source rather than imported.
    """

    def test_a_nested_module_is_uploaded_with_its_directory(self):
        import tempfile

        source = read("deploy_updates.py")
        namespace = {"os": os}
        for name in ("put_text_file", "mkdir_p", "upload_crate"):
            match = re.search(r"^def %s\(.*?(?=^\S)" % name, source, re.S | re.M)
            self.assertIsNotNone(match, name)
            exec(match.group(0), namespace)

        class FakeSftp:
            def __init__(self):
                self.dirs, self.files = [], {}

            def mkdir(self, path):
                self.dirs.append(path)

            def open(self, path, mode):
                files = self.files

                class Handle:
                    def __enter__(self_inner):
                        return self_inner

                    def __exit__(self_inner, *exc):
                        return False

                    def write(self_inner, data):
                        files[path] = data

                return Handle()

        class FakeSsh:
            def exec_command(self, command):
                pass

        with tempfile.TemporaryDirectory() as root:
            for rel in ("Cargo.toml", "src/main.rs", "src/extent.rs",
                        "src/extent/placement.rs", "src/purah/tier.rs", "src/notes.txt"):
                full = os.path.join(root, "crate", rel)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as handle:
                    handle.write("// %s\r\n" % rel)
            sftp = FakeSftp()
            build = namespace["upload_crate"](sftp, FakeSsh(), root, "crate")
            self.assertIn(build + "/src/extent/placement.rs", sftp.files,
                          "a module in a subdirectory was not sent to the node")
            self.assertIn(build + "/src/purah/tier.rs", sftp.files)
            self.assertIn(build + "/src/main.rs", sftp.files)
            self.assertNotIn(build + "/src/notes.txt", sftp.files)
            self.assertIn(build + "/src/extent", sftp.dirs)


class AMoveNeverDeletes(unittest.TestCase):
    """Moving is copy, verify, switch -- and the delete is somebody else's job.

    Every attached vdisk holds its own store with its own index, so a reader may have
    resolved the old path a moment before the switch. The old copy is removed by the sweep,
    under the same two-scan grace as reclamation, which keeps one rule in this daemon for
    when bytes may be deleted.
    """

    def setUp(self):
        self.placement = rust_source("extent", "placement.rs")
        self.tier = rust_source("purah", "tier.rs")
        self.purah = rust_source("purah.rs")

    def test_the_move_itself_contains_no_delete(self):
        body = rust_function(self.placement, "pub fn move_group(")
        self.assertNotIn("remove_file", body)
        self.assertNotIn("remove_all", body)

    def test_the_copy_is_published_by_rename_after_it_is_verified(self):
        copy_body = rust_function(self.placement, "pub fn stage_copy(")
        self.assertIn("file_crc(&temp)", copy_body)
        self.assertIn("sync_all", copy_body)
        publish = rust_function(self.placement, "pub fn stage_publish(")
        self.assertIn("std::fs::rename", publish)

    def test_a_copy_in_progress_cannot_be_mistaken_for_an_extent_group(self):
        """Every directory scan keys on the `.eg` suffix."""
        self.assertIn('MOVING_SUFFIX: &str = ".eg.moving"', self.placement)
        self.assertFalse(".eg.moving".endswith(".eg"))

    def test_the_surplus_copy_is_removed_by_the_sweep_under_the_grace(self):
        sweep = rust_function(self.purah, "pub fn sweep(")
        self.assertIn("reap_strays", sweep)
        step = rust_function(self.tier, "pub fn step(")
        self.assertIn("first_seen", step)
        self.assertIn("< grace", step)

    def test_removing_a_surplus_copy_refuses_to_remove_the_only_one(self):
        body = rust_function(self.placement, "pub fn remove_stray(")
        self.assertIn("copies.len() < 2", body)

    def test_reclaiming_a_group_removes_every_local_copy(self):
        reclaim = rust_function(rust_source("purah", "reclaim.rs"), "fn mark_dead_and_remove_local<")
        self.assertIn("remove_all", reclaim)

    def test_a_reader_with_a_stale_index_is_not_failed_by_a_move(self):
        extent = rust_source("extent.rs")
        for name in ("pub fn read_extent(", "pub fn read_extent_framed(", "pub fn seal_hash("):
            self.assertIn("open_group", rust_function(extent, name), name)


class TieringHonoursTheRulesTheDocumentsSetDown(unittest.TestCase):
    """Heat decides where a copy sits, never whether one exists."""

    def setUp(self):
        self.tier = rust_source("purah", "tier.rs")

    def test_tiering_never_deletes_a_group_or_touches_the_metadata_that_says_it_exists(self):
        # Production code only. What it may remove is an abandoned temporary and a surplus
        # copy that the grace and a byte-for-byte comparison have already cleared; it never
        # reaches the statements or the helper that reclaim a group.
        production = self.tier.split("#[cfg(test)]")[0]
        for forbidden in ("DELETE FROM", "egroup-state", "remove_all"):
            self.assertNotIn(
                forbidden, production,
                "tiering performs %r; heat may decide where a copy goes and never whether it exists"
                % forbidden)

    def test_only_a_sealed_group_is_moved_and_it_is_checked_against_hydra_first(self):
        body = rust_function(self.tier, "fn sealed_hash(")
        self.assertIn('state != "sealed"', body)
        move = rust_function(self.tier, "fn move_checked(")
        self.assertLess(move.index("sealed_hash"), move.index("move_group"))

    def test_nothing_runs_tiering_on_a_timer(self):
        """The ranking was shipped as reporting so that somebody would read it before
        anything acted on it. A pass on a timer would be the thing that decision refused."""
        control = rust_source("control.rs")
        loop_body = rust_function(control, "pub fn start_purah(")
        self.assertNotIn("op_purah_tier", loop_body)
        self.assertNotIn(".tier(", loop_body)

    def test_the_pass_plans_unless_told_to_apply(self):
        body = rust_function(rust_source("control.rs"), "fn op_purah_tier(")
        self.assertIn('unwrap_or(false)', body)

    def test_disks_of_unknown_class_take_no_part(self):
        self.assertIn("is_known()", self.tier)

    def test_unmeasured_groups_are_not_spilled_when_the_tally_was_capped(self):
        self.assertIn("measurement_incomplete", self.tier)

    def test_the_container_tier_is_now_read_when_placing(self):
        vdisk = rust_source("vdisk.rs")
        self.assertIn("container_tier(", vdisk)
        self.assertIn(".preferring(", vdisk)

    def test_the_cli_exposes_placement_tiering_and_the_manual_move(self):
        valcli = read("valcli.py")
        for command in ("storage.placement", "storage.tier", "storage.move"):
            self.assertIn('cmd == "%s"' % command, valcli)
        for op in ("purah-placement", "purah-tier", "purah-move"):
            self.assertIn('"op": "%s"' % op, valcli)

    def test_no_schema_change_was_needed(self):
        """Which disk holds a group is node-local and is not in Hydra, so moving one is not
        a metadata write. If a migration ever appears for this, the design has changed."""
        import helios_schema
        ids = [m["id"] for m in helios_schema.MIGRATIONS]
        self.assertFalse([i for i in ids if i.startswith(("0018", "0019"))],
                         "a migration was added for disk placement, which is node-local")


if __name__ == "__main__":
    unittest.main()
