#!/usr/bin/env python3
"""Rauru starts before there is a cluster to start against, and must not crash-loop for it.

`systemctl enable rauru` runs it at boot, and boot is before ZooKeeper, Hydra and Daruk exist --
on a node that has never been in a cluster, they never will. A daemon that exited when its
database was absent would be restarted by systemd every few seconds for as long as that stayed
true: a crash loop that looks like a running service in every status listing. So the properties
asserted here are about behaviour while things are missing, not about the policy it runs (that
is `test_snapshot_policy`).

Run with:  python -m unittest test_rauru
"""

import importlib
import io
import os
import re
import sys
import unittest
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
rauru = importlib.import_module("rauru")


class FakeElection(object):
    def __init__(self, leads=True):
        self.leads = leads

    def leading(self):
        return self.leads


class Clock(object):
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class Summary(object):
    def __init__(self, ok=True):
        self.ok = ok
        self.failures = [] if ok else [("vm-disk0", "refused")]


def make(leads=True, ready=(True, ""), run=None, interval=3600):
    said = []
    clock = Clock()
    calls = {"run": 0, "ready": 0}

    def ready_probe():
        calls["ready"] += 1
        return ready

    def runner():
        calls["run"] += 1
        if run is not None:
            return run()
        return Summary()

    daemon = rauru.Daemon(FakeElection(leads), ready_probe, runner, interval, clock=clock,
                          say=said.append, backoff=rauru.Backoff(rand=lambda: 0.5))
    return daemon, clock, calls, said


class ItWaitsForWhatIsMissingInsteadOfDying(unittest.TestCase):

    def test_hydra_being_down_is_a_wait_with_a_reason_and_not_an_exception(self):
        daemon, _, calls, said = make(ready=(False, "Daruk is not answering"))
        delay = daemon.step()
        self.assertGreater(delay, 0)
        self.assertEqual(calls["run"], 0, "it must not run the policy against a database that is not there")
        self.assertTrue(any("Daruk is not answering" in line for line in said))

    def test_the_wait_backs_off_and_is_capped(self):
        daemon, _, _, _ = make(ready=(False, "down"))
        delays = [daemon.step() for _ in range(12)]
        self.assertLess(delays[0], delays[3])
        self.assertLessEqual(max(delays), rauru.BACKOFF_MAX_SECONDS * 1.25 + 1)

    def test_the_same_reason_is_not_logged_every_pass(self):
        """A node with no cluster would otherwise write the same line for as long as it lives."""
        daemon, _, _, said = make(ready=(False, "down"))
        for _ in range(10):
            daemon.step()
        self.assertEqual(len([line for line in said if "waiting" in line]), 1)

    def test_a_cluster_that_cannot_be_read_is_a_wait_not_a_crash(self):
        def unreadable():
            raise RuntimeError("could not read the cluster: no such table")

        daemon, _, calls, said = make(run=unreadable)
        self.assertGreater(daemon.step(), 0)
        self.assertEqual(calls["run"], 1)
        self.assertTrue(any("no such table" in line for line in said))

    def test_nothing_the_loop_body_raises_escapes_it(self):
        def explode():
            raise ValueError("a bug in the policy")

        daemon, _, _, said = make(run=explode)
        self.assertGreater(daemon.step(), 0)
        self.assertTrue(any("a bug in the policy" in line for line in said))

    def test_an_election_that_raises_is_survived_too(self):
        class Broken(object):
            def leading(self):
                raise OSError("ZooKeeper is not there")

        daemon, _, _, _ = make()
        daemon.election = Broken()
        self.assertGreater(daemon.step(), 0)

    def test_recovery_clears_the_backoff(self):
        state = {"up": False}
        daemon, _, calls, _ = make()
        daemon.ready = lambda: (state["up"], "down")
        for _ in range(5):
            daemon.step()
        self.assertGreater(daemon.backoff.failures, 0)
        state["up"] = True
        daemon.step()
        self.assertEqual(daemon.backoff.failures, 0)
        self.assertEqual(calls["run"], 1)


class TheWorkIsBehindTheElection(unittest.TestCase):

    def test_a_node_that_does_not_lead_runs_nothing_and_does_not_even_probe_hydra(self):
        daemon, _, calls, said = make(leads=False)
        daemon.step()
        self.assertEqual((calls["run"], calls["ready"]), (0, 0))
        self.assertTrue(any("standing by" in line for line in said))

    def test_the_leader_runs_once_per_interval_and_not_on_every_pass(self):
        daemon, clock, calls, _ = make(interval=3600)
        daemon.step()
        self.assertEqual(calls["run"], 1)
        clock.now += 60
        daemon.step()
        self.assertEqual(calls["run"], 1)
        clock.now += 3600
        daemon.step()
        self.assertEqual(calls["run"], 2)

    def test_losing_the_election_stops_the_runs(self):
        daemon, clock, calls, _ = make(interval=10)
        daemon.step()
        daemon.election.leads = False
        clock.now += 100
        daemon.step()
        self.assertEqual(calls["run"], 1)

    def test_a_run_with_failures_is_said_and_still_rescheduled(self):
        daemon, clock, calls, said = make(run=lambda: Summary(ok=False), interval=3600)
        daemon.step()
        self.assertTrue(any("failure" in line for line in said))
        clock.now += 10
        daemon.step()
        self.assertEqual(calls["run"], 1)


class ItCanBeAskedWhetherItWouldStart(unittest.TestCase):

    def test_check_passes_on_this_tree(self):
        self.assertEqual(rauru.check(), [])

    def test_check_exits_zero_and_says_what_it_validated(self):
        out = io.StringIO()
        with redirect_stdout(out):
            code = rauru.main(["--check"])
        self.assertEqual(code, 0)
        self.assertIn("configuration ok", out.getvalue())
        self.assertIn("rauru-snapshots", out.getvalue())

    def test_check_exits_one_and_names_a_module_that_cannot_be_imported(self):
        saved = dict(rauru.IMPORT_ERRORS)
        rauru.IMPORT_ERRORS["helios_snapshots"] = "No module named 'helios_snapshots'"
        out = io.StringIO()
        try:
            with redirect_stdout(out):
                code = rauru.main(["--check"])
        finally:
            rauru.IMPORT_ERRORS.clear()
            rauru.IMPORT_ERRORS.update(saved)
        self.assertEqual(code, 1)
        self.assertIn("helios_snapshots", out.getvalue())

    def test_check_does_not_touch_the_network(self):
        import socket
        real = socket.socket

        def refuse(*args, **kwargs):
            raise AssertionError("--check opened a socket")

        socket.socket = refuse
        try:
            self.assertEqual(rauru.check(), [])
        finally:
            socket.socket = real


class ItIsWiredToTheNamesTheRestOfTheClusterUses(unittest.TestCase):

    def test_its_tasks_carry_the_rauru_component(self):
        import helios_snapshots
        self.assertEqual(helios_snapshots.TASK_COMPONENT, "Rauru")
        self.assertEqual(rauru.TASK_COMPONENT, helios_snapshots.TASK_COMPONENT)

    def test_it_stands_in_its_own_election_named_in_helios_zk(self):
        import helios_zk
        self.assertEqual(helios_zk.SERVICE_RAURU_SNAPSHOTS, "rauru-snapshots")
        source = open(os.path.join(HERE, "rauru.py"), encoding="utf-8").read()
        self.assertIn("helios_zk.SERVICE_RAURU_SNAPSHOTS", source)
        # The question removed from ten places for being the wrong one.
        for needle in ("leader_ip", "is_zookeeper_leader", "get_zookeeper_leader"):
            self.assertNotIn(needle, source)

    def test_the_unit_runs_the_installed_name_and_not_the_source_file(self):
        provision = open(os.path.join(HERE, "provision.py"), encoding="utf-8").read()
        self.assertIn("ExecStart=/usr/local/bin/rauru\n", provision)
        self.assertNotRegex(provision, r"ExecStart=/usr/local/bin/rauru\.py")


if __name__ == "__main__":
    unittest.main()
