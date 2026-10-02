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


class ACandidacyOutlivesItsSession(unittest.TestCase):
    """The half of the failure that is silent.

    A daemon runs for months; its ZooKeeper session does not. The ballot is ephemeral, so when
    the session goes the candidacy goes with it -- and a process that does not notice is not
    merely wrong for a moment, it never leads again for as long as it runs. The address
    comparison this replaces was at least self-healing in that one respect: it started
    answering True again when ZooKeeper came back.

    What is asserted is the order of events, not just the end state. A candidate that has lost
    its ballot must report not-leading *before* it stands again, because the alternative is a
    stale candidate acting in the window where it has stopped leading.
    """

    def setUp(self):
        self.zk = load_helios_zk()
        self.ensemble = FakeEnsemble()
        self.opened = []
        self.clock = [1000.0]

    def candidate(self, who, fail_connect=False):
        def connect():
            if fail_connect:
                raise self.zk.ZKError(-1, "could not connect to any ZooKeeper host")
            self.opened.append(who)
            return self.ensemble
        return self.zk.Candidacy(
            "catalyst-dispatch", identity=who, connect=connect,
            now=lambda: self.clock[0])

    def test_it_stands_on_the_first_question_rather_than_needing_to_be_told(self):
        a = self.candidate(b"a")
        self.assertTrue(a.leading())
        self.assertEqual(self.opened, [b"a"])

    def test_exactly_one_candidate_leads(self):
        a, b = self.candidate(b"a"), self.candidate(b"b")
        self.assertEqual([a.leading(), b.leading()], [True, False])

    def test_a_lost_ballot_is_reported_as_not_leading_before_a_new_one_is_created(self):
        a, b = self.candidate(b"a"), self.candidate(b"b")
        self.assertTrue(a.leading())
        self.assertTrue(b.leading() is False)

        # What a lost session does: the ephemeral node simply stops existing.
        self.ensemble.expire("/helios/leaders/catalyst-dispatch/n_0000000000")

        self.assertFalse(a.leading(), "a claimed to lead with no ballot")
        self.assertTrue(b.leading(), "the survivor did not take over")

    def test_it_stands_again_afterwards_rather_than_never_leading_again(self):
        a = self.candidate(b"a")
        self.assertTrue(a.leading())
        self.ensemble.expire("/helios/leaders/catalyst-dispatch/n_0000000000")

        self.assertFalse(a.leading())
        # The retry timer has to pass; a down ensemble must not be reconnected to on every
        # pass of a two-second loop.
        self.clock[0] += self.zk.CANDIDACY_RETRY_SECONDS + 1

        self.assertTrue(a.leading(), "the only candidate in the cluster never stood again")

    def test_a_reconnect_takes_a_new_session_rather_than_resuming_the_old_one(self):
        """Resuming would bring the old ballot back and leave one process holding two of
        them: harmless for correctness, permanent as a leak, and it makes the lowest ballot
        belong to a candidacy nobody is maintaining."""
        a = self.candidate(b"a")
        a.leading()
        self.ensemble.expire("/helios/leaders/catalyst-dispatch/n_0000000000")
        a.leading()
        self.clock[0] += self.zk.CANDIDACY_RETRY_SECONDS + 1
        a.leading()

        self.assertEqual(self.opened, [b"a", b"a"], "the session was resumed, not replaced")
        ballots = self.ensemble.get_children("/helios/leaders/catalyst-dispatch")
        self.assertEqual(len(ballots), 1, "the process is holding two ballots")

    def test_an_ensemble_it_cannot_reach_means_not_leading(self):
        a = self.candidate(b"a", fail_connect=True)
        self.assertFalse(a.leading())
        self.assertIsNone(a.leader_identity())

    def test_a_failed_connect_is_retried_on_a_timer_and_not_on_every_pass(self):
        """A down ensemble is the moment every daemon in the cluster is looping, and the last
        thing it needs is nine processes opening sockets as fast as their loops allow."""
        attempts = []

        def connect():
            attempts.append(self.clock[0])
            raise self.zk.ZKError(-1, "connection refused")

        a = self.zk.Candidacy("catalyst-dispatch", identity=b"a", connect=connect,
                              now=lambda: self.clock[0])
        for _ in range(10):
            a.leading()
        self.assertEqual(len(attempts), 1)

        self.clock[0] += self.zk.CANDIDACY_RETRY_SECONDS + 1
        a.leading()
        self.assertEqual(len(attempts), 2)

    def test_a_follower_can_read_who_leads_without_standing_for_the_job(self):
        """How a submitter finds the node holding a queue: by reading the winner's published
        address, not by probing for it and not by standing to drain it."""
        leader = self.candidate(b"10.0.0.1")
        self.assertTrue(leader.leading())

        observer = self.candidate(b"")
        self.assertEqual(observer.leader_identity(), b"10.0.0.1")
        self.assertEqual(
            self.ensemble.get_children("/helios/leaders/catalyst-dispatch"),
            ["n_0000000000"],
            "reading who leads created a ballot")

    def test_withdrawing_hands_it_on_and_allows_standing_again(self):
        """Bifrost's case: a candidate whose fitness is conditional on something local must
        stop being a candidate rather than win and decline, or the address sits with a node
        that cannot serve it."""
        a, b = self.candidate(b"a"), self.candidate(b"b")
        self.assertTrue(a.leading())

        a.withdraw()
        self.assertTrue(b.leading())
        self.assertFalse(a.leading() and b.leading(), "both nodes hold it")

        self.assertTrue(b.leading())

    def test_the_published_identity_is_cached_rather_than_read_per_submission(self):
        reads = []
        real_children = self.ensemble.get_children

        def counting(path):
            reads.append(path)
            return real_children(path)

        self.ensemble.get_children = counting
        leader = self.candidate(b"10.0.0.1")
        leader.leading()

        observer = self.candidate(b"")
        observer.leader_identity()
        before = len(reads)
        for _ in range(20):
            observer.leader_identity()
        self.assertEqual(len(reads), before,
                         "twenty submissions produced twenty reads of the ballot")


class TheServiceNamesAreOnePerJob(unittest.TestCase):
    """Each distinct leader-only job gets its own name. Funnelling them through one is the
    coupling this whole change removes, arrived at from the other direction."""

    def setUp(self):
        self.zk = load_helios_zk()

    def test_no_two_jobs_share_a_name(self):
        names = [value for key, value in vars(self.zk).items()
                 if key.startswith("SERVICE_")]
        self.assertEqual(sorted(names), sorted(set(names)))
        self.assertGreaterEqual(len(names), 8)

    def test_every_name_is_a_single_path_element(self):
        """The name becomes a znode under /helios/leaders. A slash in it would put a
        service's ballots under a parent nobody else looks in."""
        for key, value in vars(self.zk).items():
            if not key.startswith("SERVICE_"):
                continue
            self.assertNotIn("/", value, key)
            self.assertTrue(value.strip() == value and value, key)


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
