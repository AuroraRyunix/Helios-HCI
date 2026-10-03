#!/usr/bin/env python3
"""A cluster grown from one node must not keep a replication policy made for one node.

`cluster create` forces `redundancy_factor` to 0 for a single-node cluster, correctly: there
is nowhere to put a second copy. Nothing in `cluster add-node` revisited it, so a cluster
created on one node and grown to three kept 0 for as long as it lived, every new vdisk was
created with one copy, and every tool read the 0 back as the operator's decision. The live
test cluster reported "Cluster redundancy factor 0" for exactly this reason.

The factor is a replication policy, so `add-node` does not change it on its own. What these
tests hold is the other half: that the staleness is never silent, that the operator has an
explicit way to settle it, and that nothing is half-written when the answer is no.

Run with:  python -m unittest test_add_node_redundancy
"""

import contextlib
import importlib.util
import io
import os
import sys
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_cluster():
    spec = importlib.util.spec_from_file_location(
        "cluster_add_node_under_test", os.path.join(HERE, "cluster_new.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules["cluster_add_node_under_test"] = module
    spec.loader.exec_module(module)
    return module


cluster = load_cluster()


def args(**overrides):
    base = {"node": "10.0.0.3", "redundancy_factor": None}
    base.update(overrides)
    return types.SimpleNamespace(**base)


class AGrownClusterIsToldItsPolicyIsStale(unittest.TestCase):

    def lines(self, factor, nodes, **extra):
        config = {"hosts": [{}] * nodes}
        if factor is not None:
            config["redundancy_factor"] = factor
        config.update(extra)
        return cluster.redundancy_factor_warning(config, nodes, "10.0.0.3")

    def test_factor_zero_on_a_cluster_of_three_warns(self):
        text = "\n".join(self.lines(0, 3))
        self.assertIn("redundancy_factor 0", text)
        self.assertIn("ONE copy", text)

    def test_the_warning_carries_the_command_that_settles_it(self):
        """Not 'raise the factor' but the exact line to type, so the operator does not have
        to know which tool owns it."""
        self.assertIn("cluster add-node --node 10.0.0.3 -r 1", "\n".join(self.lines(0, 3)))

    def test_a_missing_factor_is_the_same_state(self):
        """Sidon falls back to one copy for a document that says nothing, so a cluster whose
        file lost the key has a policy nobody chose."""
        self.assertTrue(self.lines(None, 3))

    def test_the_warning_says_the_policy_was_left_alone(self):
        self.assertIn("has not been changed", "\n".join(self.lines(0, 3)))

    def test_it_says_sidon_has_to_be_restarted(self):
        """Sidon reads cluster.json at start. Rewriting the file without saying so leaves
        an operator with a corrected document and the old behaviour."""
        self.assertIn("restart sidon", "\n".join(self.lines(0, 3)))

    def test_a_cluster_that_chose_replication_is_not_nagged(self):
        for factor in (1, 2):
            self.assertEqual(self.lines(factor, 3), [])

    def test_a_single_node_cluster_at_zero_is_correct_and_silent(self):
        self.assertEqual(self.lines(0, 1), [])


class TheFactorIsOnlyChangedWhenAskedFor(unittest.TestCase):

    def test_a_factor_the_cluster_cannot_satisfy_is_refused_not_clamped(self):
        """A clamped value is written to cluster.json and read back later as a decision."""
        self.assertIsNotNone(cluster.redundancy_factor_refusal(2, 2))
        self.assertIsNotNone(cluster.redundancy_factor_refusal(-1, 3))
        self.assertIsNone(cluster.redundancy_factor_refusal(1, 2))
        self.assertIsNone(cluster.redundancy_factor_refusal(0, 1))

    def test_add_node_only_touches_the_factor_when_the_flag_is_given(self):
        source = open(os.path.join(HERE, "cluster_new.py"), encoding="utf-8").read()
        body = source[source.index("def cmd_add_node("):]
        body = body[:body.index("\ndef ", 10)]
        self.assertEqual(
            body.count('config["redundancy_factor"] ='), 1,
            "add-node assigns the factor somewhere other than the explicit flag")
        self.assertIn("if args.redundancy_factor is not None:\n"
                      '        config["redundancy_factor"] = args.redundancy_factor', body)

    def test_the_factor_is_validated_before_anything_is_changed(self):
        source = open(os.path.join(HERE, "cluster_new.py"), encoding="utf-8").read()
        body = source[source.index("def cmd_add_node("):]
        body = body[:body.index("\ndef ", 10)]
        self.assertLess(body.index("redundancy_factor_refusal("),
                        body.index("write_zookeeper_ensemble"),
                        "a refused factor would be discovered after the ensemble was rewritten")

    def test_the_warning_is_printed_by_add_node_even_when_resuming(self):
        source = open(os.path.join(HERE, "cluster_new.py"), encoding="utf-8").read()
        body = source[source.index("def cmd_add_node("):]
        body = body[:body.index("\ndef ", 10)]
        self.assertIn("redundancy_factor_warning(config, len(ips), target)", body)


class SettingTheFactorOnAnExistingMember(unittest.TestCase):
    """`add-node --node <member> -r N` is the way out for a cluster that was grown before
    this existed: nothing is joined, only the policy is settled."""

    def setUp(self):
        self.written = []
        self.originals = (cluster.cluster_hosts_config, cluster.write_cluster_config)
        self.config = {"cluster_name": "hci-01", "redundancy_factor": 0,
                       "hosts": [{"ip": "10.0.0.1"}, {"ip": "10.0.0.2"}, {"ip": "10.0.0.3"}]}
        cluster.cluster_hosts_config = lambda: dict(self.config)
        cluster.write_cluster_config = self.record

    def tearDown(self):
        cluster.cluster_hosts_config, cluster.write_cluster_config = self.originals

    def record(self, ips, config):
        self.written.append((list(ips), config))
        return []

    def run_it(self, factor):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = cluster.set_redundancy_factor(
                "10.0.0.3", factor, ["10.0.0.1", "10.0.0.2", "10.0.0.3"])
        return rc, out.getvalue()

    def test_it_rewrites_cluster_json_on_every_node_and_nothing_else(self):
        rc, _ = self.run_it(1)
        self.assertEqual(rc, 0)
        ips, config = self.written[0]
        self.assertEqual(ips, ["10.0.0.1", "10.0.0.2", "10.0.0.3"])
        self.assertEqual(config["redundancy_factor"], 1)
        self.assertEqual(config["hosts"], self.config["hosts"])
        self.assertEqual(config["cluster_name"], "hci-01")

    def test_it_is_idempotent(self):
        self.run_it(1)
        self.config["redundancy_factor"] = 1
        rc, _ = self.run_it(1)
        self.assertEqual(rc, 0)
        self.assertEqual(self.written[0][1], self.written[1][1])

    def test_it_says_what_it_did_not_do(self):
        _, out = self.run_it(1)
        self.assertIn("Restart it on each node", out)
        self.assertIn("Existing vdisks keep the rf", out)

    def test_a_factor_beyond_the_cluster_writes_nothing(self):
        rc, out = self.run_it(3)
        self.assertEqual(rc, 1)
        self.assertEqual(self.written, [])
        self.assertIn("3 node(s)", out)

    def test_a_node_that_cannot_be_written_is_a_failure(self):
        cluster.write_cluster_config = lambda ips, config: ["10.0.0.2"]
        rc, out = self.run_it(1)
        self.assertEqual(rc, 1)
        self.assertIn("10.0.0.2", out)


class CreateTellsYouGrowingDoesNotRaiseIt(unittest.TestCase):
    def test_the_single_node_message_names_the_command(self):
        source = open(os.path.join(HERE, "cluster_new.py"), encoding="utf-8").read()
        self.assertIn("Adding nodes later will not raise it", source)


if __name__ == "__main__":
    unittest.main()
