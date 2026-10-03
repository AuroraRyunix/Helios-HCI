#!/usr/bin/env python3
"""cluster create must not look stuck, and must start sidon after the metadata layer.

Two things came out of one pasted run: Phase 3 printed a line and then said nothing for a
minute, and Phase 4 reported "sidon did not answer on its control socket" for a sidon that was
healthy (the probe ran the instant `systemctl restart` returned). Also, sidon was started before
ZooKeeper, the reverse of the order MANAGED_SERVICES declares.

Run with:  python -m unittest test_create_progress
"""

import ast
import contextlib
import importlib.util
import io
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def source():
    with io.open(os.path.join(HERE, "cluster_new.py"), encoding="utf-8") as handle:
        return handle.read()


def load():
    spec = importlib.util.spec_from_file_location("cluster_new_under_test",
                                                  os.path.join(HERE, "cluster_new.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class Clock(object):
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.t += seconds


class RunParallelSaysWhatItIsDoing(unittest.TestCase):
    def setUp(self):
        self.m = load()
        self.original = self.m.run_remote_spark
        self.addCleanup(setattr, self.m, "run_remote_spark", self.original)

    def run_it(self, fake, **kwargs):
        self.m.run_remote_spark = fake
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            results = self.m.run_parallel(["10.0.0.1", "10.0.0.2"], "x", **kwargs)
        return results, out.getvalue()

    def test_a_label_announces_the_step_and_each_node_finishing(self):
        results, out = self.run_it(lambda ip, cmd, timeout=None: (0, "", ""), label="Doing a thing")
        self.assertIn("Doing a thing on 2 node(s)", out)
        self.assertIn("[10.0.0.1] Doing a thing: done", out)
        self.assertIn("[10.0.0.2] Doing a thing: done", out)
        self.assertEqual(set(results), {"10.0.0.1", "10.0.0.2"})

    def test_a_failure_is_reported_as_one(self):
        results, out = self.run_it(lambda ip, cmd, timeout=None: (1, "", "boom"), label="Doing a thing")
        self.assertIn("FAILED", out)

    def test_without_a_label_it_is_silent(self):
        results, out = self.run_it(lambda ip, cmd, timeout=None: (0, "", ""))
        self.assertEqual(out, "")

    def test_a_slow_node_gets_a_heartbeat_naming_only_the_nodes_still_running(self):
        import threading
        release = threading.Event()

        def fake(ip, cmd, timeout=None):
            if ip == "10.0.0.2":
                release.wait(5)
            return 0, "", ""

        calls = {"n": 0}

        def fake_now():
            # Every poll of the clock advances time by 6s, so a 10s heartbeat fires quickly.
            calls["n"] += 1
            if calls["n"] > 8:
                release.set()
            return calls["n"] * 6.0

        results, out = self.run_it(fake, label="Slow step", heartbeat=10, now=fake_now)
        beats = [l for l in out.splitlines() if "still working on" in l]
        self.assertTrue(beats, "a slow step printed no heartbeat:\n" + out)
        for line in beats:
            self.assertIn("10.0.0.2", line)
            self.assertNotIn("10.0.0.1", line, "the heartbeat names a node that already finished")


class SidonWaitIsNotASingleProbe(unittest.TestCase):
    def setUp(self):
        self.m = load()

    def test_it_keeps_asking_until_sidon_answers(self):
        clock = Clock()
        answers = [(1, "", "No such file"), (1, "", "No such file"), (0, '{"total_bytes": 5}', "")]
        seen = []

        def runner(ip, cmd, **kw):
            seen.append(ip)
            return answers.pop(0)

        rc, out, err = self.m.wait_for_sidon_capacity(
            "10.0.0.1", runner=runner, sleep=clock.sleep, now=clock.now, say=lambda *_: None)
        self.assertEqual(rc, 0)
        self.assertEqual(len(seen), 3)

    def test_it_gives_up_after_the_timeout_and_returns_the_failure(self):
        clock = Clock()
        rc, out, err = self.m.wait_for_sidon_capacity(
            "10.0.0.1", runner=lambda ip, cmd, **kw: (1, "", "No such file"),
            timeout=30, interval=2, sleep=clock.sleep, now=clock.now, say=lambda *_: None)
        self.assertEqual(rc, 1)
        self.assertGreaterEqual(clock.t, 30)

    def test_a_long_wait_says_so(self):
        clock = Clock()
        said = []
        self.m.wait_for_sidon_capacity(
            "10.0.0.1", runner=lambda ip, cmd, **kw: (1, "", "x"),
            timeout=35, interval=2, sleep=clock.sleep, now=clock.now, say=said.append)
        self.assertTrue(said, "a 35s wait printed nothing")


class FailureReportsEvidence(unittest.TestCase):
    def test_it_asks_the_node_and_indents_the_answer(self):
        m = load()
        asked = []

        def runner(ip, cmd, **kw):
            asked.append(cmd)
            return 0, "unit: failed  restarts: 4\n--- journal ---\nboom", ""

        text = m.describe_sidon_failure("10.0.0.1", runner=runner)
        self.assertIn("unit: failed", text)
        self.assertIn("boom", text)
        for needle in ("systemctl is-active sidon", "journalctl -u sidon", "sidon mounts"):
            self.assertIn(needle, asked[0])

    def test_the_old_guess_is_gone(self):
        self.assertNotIn("refuses to start while its journal volume is not mounted", source())


def phase4_calls():
    """The create function's source order of the Phase 4 steps, by what they run."""
    text = source()
    start = text.index("--- Phase 4: Starting the coordination, metadata and storage services")
    end = text.index("--- Phase 6: Starting Core HCI Services")
    body = text[start:end]
    return {
        "zookeeper": body.index("Starting ZooKeeper service"),
        "scylla": body.index("start_scylla_in_order(ips)"),
        "daruk": body.index("Daruk query proxy is ready"),
        "sidon": body.index('unit_action_checked(ips, "restart", ["sidon"])'),
        "verify": body.index("wait_for_sidon_capacity(ip)"),
    }


class SidonStartsBehindTheMetadataLayer(unittest.TestCase):
    def test_order(self):
        p = phase4_calls()
        self.assertLess(p["zookeeper"], p["scylla"])
        self.assertLess(p["scylla"], p["daruk"])
        self.assertLess(p["daruk"], p["sidon"], "sidon is started before Daruk is ready")
        self.assertLess(p["sidon"], p["verify"])

    def test_the_declared_service_table_agrees(self):
        spec = importlib.util.spec_from_file_location(
            "sd_under_test", os.path.join(HERE, "spark_daemon_decoded.py"))
        sd = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = sd
        spec.loader.exec_module(sd)
        table = {e["unit"]: e for e in sd.MANAGED_SERVICES}
        self.assertIn("daruk", table["sidon"]["requires"])


class TheLongDiskStepsAreLabelled(unittest.TestCase):
    def test_every_storage_prep_call_in_the_cli_has_a_label(self):
        tree = ast.parse(source())
        found = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) in (
                    "run_parallel", "run_parallel_checked"):
                kws = {k.arg for k in node.keywords}
                if "timeout" in kws:
                    found += 1
                    self.assertIn("label", kws, "line %d runs a minutes-long step silently" % node.lineno)
        self.assertGreaterEqual(found, 4)  # the four disk calls, plus the checked wrapper forwarding its own


if __name__ == "__main__":
    unittest.main()
