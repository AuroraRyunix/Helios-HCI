#!/usr/bin/env python3
"""Small defects found reading every service, each pinned.

Run with:  python -m unittest test_service_hardening
"""

import importlib.util
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name, alias):
    spec = importlib.util.spec_from_file_location(alias, os.path.join(HERE, name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read(name):
    with open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


class DagurRunRowsSurviveAQuoteInTheJobName(unittest.TestCase):
    def test_both_statements_escape_the_name(self):
        dagur = load("dagur.py", "dagur_h")
        seen = []
        dagur.run_cql_query = lambda cql, *a, **k: seen.append(cql) or (0, "", "")
        dagur.call_catalyst_api = lambda *a, **k: (200, {})
        dagur.insert_dagur_run("it's", 1, "r", 2, "SUCCESS", 0, "out")
        self.assertIn("VALUES ('it''s',", seen[0])
        seen.clear()
        try:
            dagur.execute_dagur_job_thread("t", "it's", "true")
        except Exception:
            pass
        self.assertTrue(seen, "the RUNNING row was never written")
        self.assertIn("VALUES ('it''s',", seen[0])


class TheNvramWatcherEndsItsErrorsWithANewline(unittest.TestCase):
    def test_no_literal_backslash_n_inside_the_messages(self):
        text = read("spark_daemon_decoded.py")
        for line in re.findall(r'.*\[NVRAM Watcher\] Error.*', text):
            self.assertNotIn("\\\\n", line, line)
            self.assertIn('\\n")', line, line)


class TheClusterStatusHasNoGlusterRemnant(unittest.TestCase):
    def test_the_volume_name_filter_is_gone(self):
        self.assertNotIn('"volume name:"', read("spark_daemon_decoded.py"))


class NoDaemonDefinesTheSameTopLevelNameTwice(unittest.TestCase):
    """The later definition silently replaces the earlier one, so a reader fixing the first is
    fixing nothing. Two of these existed (a 120 s and a 30 s `run_mtls_spark_api`, and two
    `get_zookeeper_leader_ip`), and the one that ran was the one nobody was looking at."""

    def test_no_repeated_function_or_class(self):
        import ast
        import collections
        import glob
        repeated = {}
        for path in sorted(glob.glob(os.path.join(HERE, "*.py"))):
            name = os.path.basename(path)
            if name.startswith("test_") or name == "provision.py":
                continue
            tree = ast.parse(read(name), filename=name)
            counts = collections.Counter(
                n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)))
            for symbol, n in counts.items():
                if n > 1:
                    repeated.setdefault(name, []).append(symbol)
        self.assertEqual(repeated, {})


class LogosWritesOnlyUnderAnAddressThatNamesTheNode(unittest.TestCase):
    def test_loopback_and_empty_are_not_an_identity(self):
        logos = load("logos.py", "logos_h")
        self.assertFalse(logos.has_node_identity("127.0.0.1"))
        self.assertFalse(logos.has_node_identity(""))
        self.assertFalse(logos.has_node_identity(None))
        self.assertTrue(logos.has_node_identity("10.10.102.41"))

    def test_the_write_is_behind_the_check(self):
        src = read("logos.py")
        self.assertLess(src.index("if not has_node_identity(local_ip):"),
                        src.index("rc, out, err = run_cql_query(combined_cql, local_ip)"))


class BifrostResignsItsBallotOnSigterm(unittest.TestCase):
    def setUp(self):
        self.bifrost = load("bifrost.py", "bifrost_h")

    def test_the_ballot_is_withdrawn(self):
        calls = []

        class C(object):
            def withdraw(self):
                calls.append("withdraw")
        self.assertTrue(self.bifrost.withdraw_ballot(C()))
        self.assertEqual(calls, ["withdraw"])

    def test_a_ballot_that_cannot_be_resigned_does_not_hold_the_process_up(self):
        import threading
        import time
        release = threading.Event()

        class Stuck(object):
            def withdraw(self):
                release.wait(10)
        started = time.time()
        self.assertFalse(self.bifrost.withdraw_ballot(Stuck(), wait=0.2))
        self.assertLess(time.time() - started, 2)
        release.set()

    def test_an_error_in_withdraw_is_not_raised_into_the_handler(self):
        class Broken(object):
            def withdraw(self):
                raise RuntimeError("no session")
        self.assertTrue(self.bifrost.withdraw_ballot(Broken()))

    def test_the_signal_handler_calls_it_before_releasing_the_address(self):
        src = read("bifrost.py")
        handler = src[src.index("def signal_handler"):src.index("def is_local_ingress_listening")]
        self.assertLess(handler.index("withdraw_ballot("), handler.index("ip addr del"))


if __name__ == "__main__":
    unittest.main()
