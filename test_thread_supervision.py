#!/usr/bin/env python3
"""Tests for the background-loop supervisor in spectrum_server.

Every long-running loop in the console was a bare `daemon=True` thread. A daemon thread
that raises prints a traceback to a log nobody tails and then stops existing -- the
process keeps serving requests, so nothing looks broken, and whatever that thread did is
silently gone. Reconciliation stops. Metrics stop being collected. The console looks
fine.

`supervise()` is extracted and exercised on its own rather than by importing
`spectrum_server`, which starts a web server and opens a database on import.

Run with:  python -m unittest test_thread_supervision
"""

import ast
import io
import os
import threading
import time
import traceback
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_supervise():
    """Compile just the supervise() function out of spectrum_server.py."""
    source = io.open(os.path.join(HERE, "spectrum_server.py"), encoding="utf-8").read()
    tree = ast.parse(source)
    fn = next((n for n in tree.body
               if isinstance(n, ast.FunctionDef) and n.name == "supervise"), None)
    assert fn is not None, "supervise() not found in spectrum_server.py"
    # The supervisor reports failures to stderr, which is right in production and noise
    # here -- these tests deliberately make loops fail. Both reporters are stubbed; the
    # tests assert on behaviour, not on output.
    quiet_traceback = type("quiet", (), {"print_exc": staticmethod(lambda *a, **k: None)})
    namespace = {
        "threading": threading, "time": time, "traceback": quiet_traceback,
        "print": lambda *a, **k: None,
    }
    module = ast.Module(body=[fn], type_ignores=[])
    exec(compile(module, "<supervise>", "exec"), namespace)
    return namespace["supervise"]


supervise = load_supervise()


def wait_until(predicate, timeout=3.0, interval=0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class SupervisorTests(unittest.TestCase):
    def test_a_loop_that_raises_is_restarted(self):
        # The whole point. Without this the thread is gone and the process looks healthy.
        runs = []

        def flaky():
            runs.append(1)
            if len(runs) < 3:
                raise RuntimeError("boom")
            time.sleep(10)

        supervise("flaky", flaky, restart_delay=0.01, max_restart_delay=0.05)
        self.assertTrue(wait_until(lambda: len(runs) >= 3),
                        "the loop was not restarted after raising")

    def test_a_loop_that_returns_is_not_restarted(self):
        # Returning is a decision to stop. Restarting it would be an infinite loop the
        # author did not ask for.
        runs = []
        supervise("finishes", lambda: runs.append(1), restart_delay=0.01)
        time.sleep(0.3)
        self.assertEqual(len(runs), 1)

    def test_backoff_grows_so_a_permanent_failure_is_not_a_restart_storm(self):
        # A loop that cannot start at all -- an unreachable database at boot -- must keep
        # trying without burying the log or burning a core.
        stamps = []

        def always_fails():
            stamps.append(time.time())
            raise RuntimeError("nope")

        supervise("always", always_fails, restart_delay=0.02, max_restart_delay=0.4)
        wait_until(lambda: len(stamps) >= 4, timeout=5.0)
        self.assertGreaterEqual(len(stamps), 4)

        gaps = [b - a for a, b in zip(stamps, stamps[1:])]
        # Each wait should be no shorter than the one before, allowing for scheduling
        # jitter. Asserting exact doubling would make this a clock test.
        self.assertGreater(gaps[-1], gaps[0],
                           "the delay between restarts did not grow: %r" % (gaps,))

    def test_backoff_is_capped(self):
        stamps = []

        def always_fails():
            stamps.append(time.time())
            raise RuntimeError("nope")

        supervise("capped", always_fails, restart_delay=0.05, max_restart_delay=0.1)
        wait_until(lambda: len(stamps) >= 5, timeout=5.0)
        gaps = [b - a for a, b in zip(stamps, stamps[1:])]
        self.assertLess(max(gaps), 1.0, "the delay exceeded its cap: %r" % (gaps,))

    def test_the_supervisor_thread_is_a_daemon(self):
        # Otherwise the process cannot exit while a loop is sleeping between restarts.
        thread = supervise("daemonic", lambda: time.sleep(10), restart_delay=0.01)
        self.assertTrue(thread.daemon)

    def test_the_thread_is_named_after_the_loop(self):
        # So a stack dump names the loop rather than "Thread-7".
        thread = supervise("reconcile", lambda: time.sleep(10), restart_delay=0.01)
        self.assertIn("reconcile", thread.name)


class WiringTests(unittest.TestCase):
    """main() must actually use it."""

    def setUp(self):
        self.source = io.open(os.path.join(HERE, "spectrum_server.py"),
                              encoding="utf-8").read()
        tree = ast.parse(self.source)
        self.main = next(n for n in tree.body
                         if isinstance(n, ast.FunctionDef) and n.name == "main")

    def test_main_starts_its_loops_through_the_supervisor(self):
        supervised = {
            node.args[0].value
            for node in ast.walk(self.main)
            if isinstance(node, ast.Call)
            and getattr(node.func, "id", None) == "supervise"
            and node.args and isinstance(node.args[0], ast.Constant)
        }
        self.assertEqual(
            supervised,
            {"db_reconcile", "metrics_and_cluster_monitor", "internal_token_verifier"})

    def test_main_starts_no_bare_background_thread(self):
        # A new loop added as threading.Thread(...) would reintroduce the silent death.
        bare = [node for node in ast.walk(self.main)
                if isinstance(node, ast.Call)
                and getattr(getattr(node.func, "value", None), "id", None) == "threading"
                and getattr(node.func, "attr", None) == "Thread"]
        self.assertEqual(bare, [], "main() starts a thread outside the supervisor")

    def test_the_console_carries_no_scheduler_of_its_own(self):
        # Catalyst owns both schedules and now claims each tick with a compare-and-swap.
        # The console's copies were dead code -- commented out at the call site but still
        # present, complete with the blind `last_run_epoch` write, which is a way back
        # into the double-submission bug for anyone who re-wires them. Deleted, not gated.
        for loop in ("mimir_scheduler_loop", "dagur_scheduler_loop"):
            self.assertNotIn("def " + loop, self.source,
                             loop + " is still defined in the console")


if __name__ == "__main__":
    unittest.main()
