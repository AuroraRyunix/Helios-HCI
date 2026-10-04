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


if __name__ == "__main__":
    unittest.main()
