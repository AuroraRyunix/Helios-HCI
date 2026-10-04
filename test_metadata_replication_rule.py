#!/usr/bin/env python3
"""The hydra keyspace needs 2F+1 replicas to survive F failures, not F+1.

The keyspace is read and written at QUORUM, a strict majority. At RF=2 a majority is both replicas,
so losing either stops every read and write in the cluster. Three places derived the "right" factor
as ftt + 1 -- Phoenix's Settings page, Mimir's replication audit, and the guidance they gave -- so the
Settings page flagged a correct RF=3 as a mismatch and offered 2; an operator took the offer, and a
maintenance request was then refused because stopping either node would have stopped the database.

Run with:  python -m unittest test_metadata_replication_rule
"""

import importlib.util
import io
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

CASES = [
    # (ftt, nodes, expected)
    (1, 3, 3), (1, 2, 2), (1, 1, 1), (0, 3, 1), (0, 1, 1),
    (2, 3, 3), (2, 5, 5), (2, 1, 1), (3, 5, 5), (3, 9, 7),
]


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def load_cql():
    spec = importlib.util.spec_from_file_location("helios_cql_rule", os.path.join(HERE, "helios_cql.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TheRule(unittest.TestCase):
    def test_the_table(self):
        rule = load_cql().metadata_replication_factor
        for ftt, nodes, expected in CASES:
            self.assertEqual(rule(ftt, nodes), expected, "ftt=%s nodes=%s" % (ftt, nodes))

    def test_ftt_one_on_three_nodes_survives_losing_one(self):
        rule = load_cql().metadata_replication_factor
        rf = rule(1, 3)
        quorum = rf // 2 + 1
        self.assertGreaterEqual(rf - 1, quorum, "RF=%d cannot lose a node and still reach quorum" % rf)

    def test_the_old_formula_would_have_been_unsafe(self):
        rf = 1 + 1  # ftt + 1
        self.assertLess(rf - 1, rf // 2 + 1, "ftt + 1 is not majority-safe, which is the bug")

    def test_unreadable_input_is_the_conservative_single_copy_not_an_exception(self):
        rule = load_cql().metadata_replication_factor
        self.assertEqual(rule(None, 3), 1)
        self.assertEqual(rule("x", "y"), 1)


class MimirUsesTheSameRule(unittest.TestCase):
    def test_the_audit_expects_two_f_plus_one(self):
        text = read("mcli-runner")
        self.assertIn("expected_rf = min(node_count, 2 * ftt + 1)", text)
        self.assertNotIn("min(node_count, ftt + 1)", text)

    def test_the_audit_fails_a_factor_that_loses_quorum_with_one_failure(self):
        text = read("mcli-runner")
        self.assertIn("(rf - ftt) < (rf // 2 + 1)", text)
        self.assertIn("critical = True", text[text.index("(rf - ftt) < (rf // 2 + 1)"):][:400])

    def test_the_inline_expression_matches_the_shared_rule_over_the_whole_table(self):
        rule = load_cql().metadata_replication_factor
        for ftt, nodes, expected in CASES:
            inline = min(nodes, 2 * ftt + 1) if (nodes > 1 and ftt > 0) else 1
            self.assertEqual(inline, rule(ftt, nodes))


class PhoenixAgrees(unittest.TestCase):
    def test_the_settings_page_derives_the_same_number(self):
        text = read("spectrum_phx/lib/spectrum_phx/settings.ex")
        self.assertIn("2 * ftt + 1", text)
        self.assertNotIn("(cluster[:redundancy_factor] || 0) + 1", text)

    def test_the_page_refuses_to_lower_below_it(self):
        text = read("spectrum_phx/lib/spectrum_phx/settings.ex")
        self.assertIn("factor < safe", text)


if __name__ == "__main__":
    unittest.main()


class TheDefaultAndTheWarning(unittest.TestCase):
    """Every place that picks a factor for the keyspace agrees with the rule, and a two-replica
    database is announced where the operator is looking."""

    def test_the_default_is_the_rule_with_a_floor_of_three_for_a_real_cluster(self):
        cql = load_cql()
        cases = [(1, 3, 3), (2, 5, 5), (0, 3, 3), (None, 3, 3), (1, 2, 2), (1, 1, 1),
                 (0, 1, 1), (2, 3, 3), (3, 9, 7)]
        for ftt, nodes, expected in cases:
            self.assertEqual(cql.default_metadata_replication_factor(ftt, nodes), expected,
                             "ftt=%s nodes=%s" % (ftt, nodes))

    def test_a_cluster_that_asked_for_more_than_three_is_not_held_at_three(self):
        """spectrum_server created the keyspace at min(3, nodes) and reconciled to 3 at every
        start, so a five-node ftt=2 cluster -- which needs five replicas for QUORUM to survive two
        losses -- had its keyspace lowered to three."""
        self.assertEqual(load_cql().default_metadata_replication_factor(2, 5), 5)

    def test_the_default_never_drops_below_the_rule(self):
        cql = load_cql()
        for ftt in range(0, 5):
            for nodes in range(1, 10):
                self.assertGreaterEqual(cql.default_metadata_replication_factor(ftt, nodes),
                                        cql.metadata_replication_factor(ftt, nodes))
                self.assertLessEqual(cql.default_metadata_replication_factor(ftt, nodes), nodes)

    def test_only_two_replicas_draw_the_warning(self):
        cql = load_cql()
        self.assertTrue(cql.two_replica_warning(2))
        for other in (0, 1, 3, 5, None):
            self.assertEqual(cql.two_replica_warning(other), [])
        text = " ".join(cql.two_replica_warning(2))
        self.assertIn("EITHER node", text)
        self.assertIn("tie-breaker", text)

    def test_the_console_tier_asks_the_shared_default_and_not_a_flat_three(self):
        text = read("spectrum_server.py")
        self.assertIn("desired_rf = default_metadata_replication_factor(cluster_fault_tolerance(), node_count)", text)
        self.assertNotIn("desired_rf = min(3, node_count)", text)
        self.assertNotIn("configured_rf = 3\n", text)

    def test_the_cli_advice_and_creation_use_it(self):
        text = read("cluster_new.py")
        self.assertIn("wanted = default_metadata_replication_factor(", text)
        self.assertNotIn("% min(3, len(ips)))", text)
        self.assertIn("two_replica_warning(default_metadata_replication_factor(rf, len(ips)))", text)
        self.assertIn("two_replica_warning(min(replication_factor, remaining))", text)

    def test_a_two_node_create_warns_and_a_three_node_create_does_not(self):
        cql = load_cql()
        self.assertTrue(cql.two_replica_warning(cql.default_metadata_replication_factor(1, 2)))
        self.assertFalse(cql.two_replica_warning(cql.default_metadata_replication_factor(1, 3)))
        # One failure tolerated on one node is not a two-replica cluster either.
        self.assertFalse(cql.two_replica_warning(cql.default_metadata_replication_factor(0, 1)))
