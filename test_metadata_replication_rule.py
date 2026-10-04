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
