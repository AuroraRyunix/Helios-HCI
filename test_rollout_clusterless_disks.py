#!/usr/bin/env python3
"""The rollout must leave a clusterless node's disks alone.

After `cluster destroy` a node has blank disks and no /etc/hci/cluster.json. A rollout onto
it ran the disk-claim step, which formats and mounts every blank disk of 100 GB or more that is
not already an LVM physical volume -- and on a destroyed node that is *every* data disk, because
destroy had just removed the volume group. So `sdb` and `sdc` were both formatted XFS, mounted
under /var/lib/hci/sidon/disks/, and listed in a staged manifest.

`cluster create` then looks for an empty disk to carve the journal volume from, and skips any
disk with a mount point. It would have found none and stopped with "No empty disk >= 100GB
found", on a node the owner had every reason to believe was clean.

The claim step is right for what it was written for: reaching a node that already has a cluster
and a journal volume, where a further disk really is "additional". The mistake was running it on
a node that has neither. The guard is a prefix at the two call sites rather than a change to the
scripts, because the scripts are asserted byte-identical across four files and this is a fact
about the rollout alone.

Run with:  python -m unittest test_rollout_clusterless_disks
"""

import ast
import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


class _Constants(object):
    """Module-level string constants read out of deploy_updates.py without importing it.

    Importing that file is not safe: it is a script, and it opens by asking "Enter cluster node
    IPs" on stdin, so a test that imported it hung until the harness killed it -- which also meant
    a test suite could, in principle, start a rollout. Parsing the source reads exactly the text
    that ships and runs nothing.
    """

    def __init__(self, source):
        for node in ast.parse(source).body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1 \
                    and isinstance(node.targets[0], ast.Name):
                try:
                    setattr(self, node.targets[0].id, ast.literal_eval(node.value))
                except (ValueError, SyntaxError):
                    pass


def load_deploy():
    return _Constants(read("deploy_updates.py"))


class TheGuardDecidesOnTheNodesState(unittest.TestCase):
    """Three states, because the first version of the guard knew only two of them.

    It asked for cluster.json alone. `cluster create` writes that file in its second phase,
    before the disk phase, so a create that failed in phase 3 left a config on every node and
    no volume group on two of them -- and a rollout before the retry would have claimed those
    blank disks and broken the retry in the same way. The state in which a further disk is
    genuinely "additional" is a cluster *and* a prepared extent store.
    """

    def setUp(self):
        self.bash = shutil.which("bash")
        if not self.bash:
            self.skipTest("no bash on this machine")
        self.deploy = load_deploy()
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, True)

    def run_guard(self, config_exists, volume_group_exists):
        config = os.path.join(self.dir, "cluster.json")
        if config_exists:
            with io.open(config, "w", encoding="utf-8") as handle:
                handle.write("{}")
        # The real guard text, with only the config path substituted and `vgs` supplied as a
        # function: this machine has no LVM, and what is being tested is the decision.
        guard = self.deploy.ONLY_WITH_A_CLUSTER.replace(
            "/etc/hci/cluster.json", config.replace("\\", "/"))
        vgs = "vgs() { return %d; }\n" % (0 if volume_group_exists else 5)
        script = vgs + guard + 'echo "CLAIM RAN"\n'
        done = subprocess.run([self.bash, "-c", script], stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        return done.returncode, done.stdout.decode("utf-8", "replace")

    def test_a_destroyed_node_is_left_alone(self):
        """No config, no volume group: what `cluster destroy` leaves."""
        rc, out = self.run_guard(config_exists=False, volume_group_exists=False)
        self.assertEqual(rc, 0, "leaving the disks alone must not look like a failure")
        self.assertNotIn("CLAIM RAN", out)
        self.assertIn("left alone", out, "it should say why nothing happened")

    def test_a_node_whose_create_failed_before_its_disks_is_left_alone(self):
        """A config and no volume group: exactly what a create that died in its disk phase
        leaves. This is the case the first version of the guard got wrong."""
        rc, out = self.run_guard(config_exists=True, volume_group_exists=False)
        self.assertEqual(rc, 0)
        self.assertNotIn("CLAIM RAN", out,
                         "a node with a config but no extent store had its blank disks claimed")

    def test_a_volume_group_without_a_config_is_left_alone(self):
        rc, out = self.run_guard(config_exists=False, volume_group_exists=True)
        self.assertNotIn("CLAIM RAN", out)

    def test_a_node_with_a_cluster_and_an_extent_store_is_reached(self):
        """The case the claim step exists for: a further disk on an established node."""
        rc, out = self.run_guard(config_exists=True, volume_group_exists=True)
        self.assertEqual(rc, 0)
        self.assertIn("CLAIM RAN", out,
                      "the guard blocks a node that really does have a cluster and an extent store")


class BothDiskStepsAreGuarded(unittest.TestCase):
    def setUp(self):
        self.source = read("deploy_updates.py")

    def test_the_claim_step_is_prefixed(self):
        self.assertIn("ONLY_WITH_A_CLUSTER + CLAIM_EXTRA_DISKS", self.source,
                      "the rollout claims a clusterless node's blank disks")

    def test_the_stage_step_is_prefixed(self):
        """Staging writes /etc/hci/sidon-disks. On a node with no cluster it recorded a
        journal volume that no longer existed, which sidon would then have refused to start
        without."""
        self.assertIn("ONLY_WITH_A_CLUSTER + STAGE_SIDON_DISKS", self.source)

    def test_no_unguarded_call_remains(self):
        for script in ("CLAIM_EXTRA_DISKS", "STAGE_SIDON_DISKS"):
            for line in self.source.splitlines():
                if "exec_command(" in line and script in line:
                    self.assertIn("ONLY_WITH_A_CLUSTER", line,
                                  "an unguarded %s call: %s" % (script, line.strip()))

    def test_the_shared_scripts_are_not_the_place_for_it(self):
        """They are asserted identical across four files, and provisioning and create must
        still claim blank disks -- on those paths a blank disk is exactly what is wanted."""
        for name in ("provision.py", "cluster_new.py", "spark_daemon_decoded.py"):
            self.assertNotIn("ONLY_WITH_A_CLUSTER", read(name),
                             "%s carries a rollout-only guard" % name)


if __name__ == "__main__":
    unittest.main()
