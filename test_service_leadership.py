#!/usr/bin/env python3
"""Which node runs a leader-only job is a question per job.

Helios answered it by comparing addresses: `vali` decided it was the Catalyst queue worker
when `helios_zk.leader_ip(ips) == LOCAL_IP`. Three things are wrong with that, and they are
the same thing. The ensemble elects a leader for its own reasons -- a restart, a blip, a
rolling upgrade, twice in one afternoon -- and *every* leader-only workload in the cluster
relocates when it does. One node runs all of them. And "am I the leader" is a string
comparison against a cached probe, so it is true or false some seconds after the fact.

This is the standard ZooKeeper recipe instead: a persistent parent per service, one
ephemeral sequential child per candidate, lowest counter wins. Nobody announces and nobody
times anyone out -- the ballot is tied to the session, so a process that dies, hangs past
its session, or is partitioned away stops leading because its node is gone.

Run with:  python -m unittest test_service_leadership
"""

import importlib.util
import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_helios_zk():
    spec = importlib.util.spec_from_file_location(
        "helios_zk_leadership", os.path.join(HERE, "helios_zk.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeEnsemble(object):
    """Just enough ZooKeeper: sequential creates, children, get, delete."""

    def __init__(self):
        self.nodes = {}
        self.counters = {}
        self.fail_with = None

    def ensure_path(self, path):
        self.nodes.setdefault(path, b"")

    def create(self, path, data=b"", ephemeral=False, makepath=False, sequential=False):
        if sequential:
            parent, prefix = path.rsplit("/", 1)
            seq = self.counters.get(parent, 0)
            self.counters[parent] = seq + 1
            path = "%s/%s%010d" % (parent, prefix, seq)
        self.nodes[path] = data
        return path

    def get_children(self, path):
        if self.fail_with:
            raise self.fail_with
        prefix = path.rstrip("/") + "/"
        return [k[len(prefix):] for k in self.nodes
                if k.startswith(prefix) and "/" not in k[len(prefix):]]

    def get(self, path):
        if self.fail_with:
            raise self.fail_with
        return self.nodes[path]

    def delete(self, path, version=-1):
        self.nodes.pop(path, None)

    def expire(self, ballot_path):
        """What a lost session does: the ephemeral node simply stops existing."""
        self.nodes.pop(ballot_path, None)


class TheBallotDecidesIt(unittest.TestCase):
    def setUp(self):
        self.zk = load_helios_zk()
        self.ensemble = FakeEnsemble()

    def stand(self, who):
        election = self.zk.Election(self.ensemble, "catalyst", identity=who)
        election.stand()
        return election

    def test_exactly_one_candidate_leads(self):
        a, b, c = self.stand(b"a"), self.stand(b"b"), self.stand(b"c")
        self.assertEqual([a.is_leader(), b.is_leader(), c.is_leader()], [True, False, False])

    def test_the_first_to_stand_wins(self):
        """Not an arbitrary winner: the counter is the ensemble's, so the order is the
        order the ballots were actually created in."""
        first, second = self.stand(b"first"), self.stand(b"second")
        self.assertTrue(first.is_leader())
        self.assertFalse(second.is_leader())

    def test_a_lost_session_hands_over_without_anyone_being_told(self):
        """The case the address comparison could never handle. Nothing times out, nothing
        notices -- the node is gone, so the next counter is lowest."""
        a, b = self.stand(b"a"), self.stand(b"b")
        self.assertTrue(a.is_leader())

        self.ensemble.expire("%s/%s" % (a.parent, a.ballot))

        self.assertFalse(a.is_leader(), "a still claims to lead with no ballot")
        self.assertTrue(b.is_leader())

    def test_standing_again_puts_a_candidate_at_the_back(self):
        a, b = self.stand(b"a"), self.stand(b"b")
        self.ensemble.expire("%s/%s" % (a.parent, a.ballot))
        a.ballot = None
        a.stand()

        self.assertTrue(b.is_leader(), "the survivor lost leadership to a returning node")
        self.assertFalse(a.is_leader())

    def test_a_follower_reads_the_leader_rather_than_probing_for_it(self):
        a, b = self.stand(b"10.0.0.1"), self.stand(b"10.0.0.2")
        self.assertEqual(b.leader_identity(), b"10.0.0.1")
        self.assertEqual(a.leader_identity(), b"10.0.0.1")

    def test_resigning_passes_it_on(self):
        a, b = self.stand(b"a"), self.stand(b"b")
        a.resign()
        self.assertFalse(a.is_leader())
        self.assertTrue(b.is_leader())

    def test_a_candidate_that_never_stood_does_not_lead(self):
        election = self.zk.Election(self.ensemble, "catalyst", identity=b"x")
        self.assertFalse(election.is_leader())
        self.assertIsNone(election.leader_identity())

    def test_an_unreachable_ensemble_means_not_leading(self):
        """"I cannot tell" has to read as "not the leader". The alternative is two nodes
        draining one queue, which is worse than neither draining it."""
        a = self.stand(b"a")
        self.assertTrue(a.is_leader())

        self.ensemble.fail_with = self.zk.ZKError(-1, "connection lost")
        self.assertFalse(a.is_leader())
        self.assertIsNone(a.leader_identity())

    def test_a_programming_error_is_not_swallowed(self):
        """The catches are narrow on purpose.

        leader_identity first shipped calling `get()` as though it returned (data, stat).
        It returns bytes, so the unpack raised ValueError -- and a blanket `except
        Exception` turned that into None, which looked exactly like an empty ballot. The
        live test caught it; the catch is what hid it.
        """
        a = self.stand(b"a")
        self.ensemble.fail_with = ValueError("not a ZooKeeper problem")
        with self.assertRaises(ValueError):
            a.is_leader()


class TheBallotNamesSortNumerically(unittest.TestCase):
    def setUp(self):
        self.zk = load_helios_zk()

    def test_the_lowest_counter_wins_not_the_lowest_string(self):
        """ZooKeeper pads to ten digits, which is only unambiguous until the counter passes
        it. String order would then invert, and the cluster would change leader because a
        number got longer."""
        self.assertEqual(
            self.zk.lowest_ballot(["n_9999999999", "n_10000000000"]), "n_9999999999")

    def test_unrelated_children_are_ignored(self):
        self.assertEqual(
            self.zk.lowest_ballot(["lock", "n_0000000005", "notes"]), "n_0000000005")

    def test_nobody_standing(self):
        self.assertIsNone(self.zk.lowest_ballot([]))
        self.assertIsNone(self.zk.lowest_ballot(["lock", "notes"]))


class TheFlagsAreABitmask(unittest.TestCase):
    def setUp(self):
        self.zk = load_helios_zk()

    def test_ephemeral_sequential_is_three(self):
        self.assertEqual(self.zk.EPHEMERAL | self.zk.SEQUENTIAL, 3)

    def test_create_still_defaults_to_persistent(self):
        source = io.open(os.path.join(HERE, "helios_zk.py"), encoding="utf-8").read()
        signature = re.search(r"def create\(self, path, data=b\"\", ([^)]*)\)", source)
        self.assertTrue(signature)
        self.assertIn("ephemeral=False", signature.group(1))
        self.assertIn("sequential=False", signature.group(1))


if __name__ == "__main__":
    unittest.main()
