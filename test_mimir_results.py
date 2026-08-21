#!/usr/bin/env python3
"""Tests for how health-check results are stored and classified.

Two defects, both of which made the console show something confidently wrong.

`hydra.mimir_results` is partitioned by `category`, and `mcli` wrote the category that
was *invoked* rather than the check's own. `run_all` therefore wrote every check under
'all' while `health_checks storage` wrote the same checks under 'storage' -- two rows for
one check, with different statuses and timestamps, and nothing removing either. It also
made the column a lie: grouping by it after a run_all yields one bucket, which is why the
old web console carried its own hardcoded check-name list, which had drifted.

And `mcli-runner` had a second, weaker implementation of the certificate-expiry check
writing the same row: it looked at one file instead of two directories, and answered PASS
when it could not parse the date.

Run with:  python -m unittest test_mimir_results
"""

import importlib.util
import io
import os
import re
import unittest
from importlib.machinery import SourceFileLoader

HERE = os.path.dirname(os.path.abspath(__file__))


def load_script(name, path):
    """Load a deployed CLI, which has no .py suffix.

    An explicit SourceFileLoader is required: importlib infers a loader from the file
    extension, and `spec_from_file_location` on an extensionless path returns None. This
    is the same trap the runner itself hit.
    """
    loader = SourceFileLoader(name, os.path.join(HERE, path))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def read(name):
    return io.open(os.path.join(HERE, name), encoding="utf-8").read()


def reported_checks(runner_source):
    """Every check name `mcli-runner` writes into its results dict.

    Not all of them are literals. The per-service loop writes `results[f"{svc}_status"]`
    for each name in its `svcs` list, and a scan for quoted keys cannot see those -- which
    is how `hylia_status` came to be the one check with no entry in CHECK_ID_TO_FUNC. It
    was written to the invoked scope's partition and deleted again by the
    legacy-partition cleanup seconds later, in the same run, on every run, and never
    appeared in either console.
    """
    reported = set(re.findall(r'results\["([a-z0-9_.-]+)"\]', runner_source))
    for match in re.finditer(r'results\[f"\{(\w+)\}_status"\]', runner_source):
        loop_var = match.group(1)
        # The list the loop iterates, declared as `svcs = [...]` above it.
        for names in re.findall(r"^\s*%ss?\s*=\s*\[(.*?)\]" % loop_var,
                                runner_source[:match.start()], re.S | re.M):
            for svc in re.findall(r'"([a-z0-9_.-]+)"', names):
                reported.add(svc + "_status")
    # The same loop appends this one conditionally.
    if 'svcs.append("urbosa")' in runner_source:
        reported.add("urbosa_status")
    return reported


class CategoryPartitionTests(unittest.TestCase):
    """`category` is the partition key, so what goes in it decides duplication."""

    def setUp(self):
        self.mcli = read("mcli")
        m = re.search(r"CHECK_ID_TO_FUNC = \{(.*?)\n\}", self.mcli, re.S)
        self.assertIsNotNone(m, "CHECK_ID_TO_FUNC not found")
        self.map = dict(re.findall(r'"([a-z0-9_.-]+)"\s*:\s*"([^"]+)"', m.group(1)))

    def test_results_are_stored_under_the_checks_own_category(self):
        # The insert must not use the invoked category, or the same check lands in a
        # different partition depending on how it was run.
        insert = re.search(
            r"INSERT INTO hydra\.mimir_results[^;]*?VALUES \('\{([a-z_]+)\}'", self.mcli, re.S)
        self.assertIsNotNone(insert, "the mimir_results insert was not found")
        self.assertEqual(
            insert.group(1), "stored_category",
            "the insert still interpolates the invoked category into the partition key")

    def test_every_mapped_category_is_dotted(self):
        # The legacy-partition cleanup uses "contains a dot" to tell a real category from
        # an invocation scope. If a real category were undotted, that delete would remove
        # live rows.
        undotted = sorted(k for k, v in self.map.items() if "." not in v)
        self.assertEqual(undotted, [], "these categories would be mistaken for a scope")

    def test_the_legacy_cleanup_is_guarded(self):
        # A future invocation scope containing a dot must not make the delete live.
        self.assertRegex(
            self.mcli,
            r'if "\." not in category:\s*\n\s*run_cql_query\(\s*\n?\s*local_ip,\s*\n?\s*'
            r'f?"DELETE FROM hydra\.mimir_results WHERE category',
            "the legacy-partition delete is missing its guard")

    def test_the_map_covers_the_checks_the_runner_reports(self):
        # A check missing from the map falls back to the invoked category, which
        # reintroduces the duplication for that check alone -- the hardest kind to spot.
        missing = sorted(reported_checks(read("mcli-runner")) - set(self.map))
        self.assertEqual(missing, [], "these checks have no category and would duplicate")


class CertificateClassificationTests(unittest.TestCase):
    """An unparseable date is not a healthy certificate."""

    @classmethod
    def setUpClass(cls):
        cls.runner = load_script("mcli_runner_under_test", "mcli-runner")

    def test_a_missing_certificate_fails(self):
        status, message = self.runner.classify_certificate(
            os.path.join(HERE, "does-not-exist.crt"), "Test Certificate")
        self.assertEqual(status, "FAIL")
        self.assertIn("not found", message)

    def test_an_unreadable_certificate_warns_rather_than_passes(self):
        # The whole point. The check this replaced answered PASS on a parse failure, so a
        # node whose locale could not read the date reported a healthy certificate.
        unreadable = os.path.join(HERE, "test_mimir_results_fixture.crt")
        io.open(unreadable, "w", encoding="utf-8").write("not a certificate\n")
        try:
            status, message = self.runner.classify_certificate(unreadable, "Test Certificate")
        finally:
            os.remove(unreadable)
        self.assertEqual(status, "WARN", message)
        self.assertNotEqual(status, "PASS")

    def test_the_source_carries_no_pass_on_parse_error(self):
        source = read("mcli-runner")
        self.assertNotIn("parse error", source,
                         "a PASS-on-parse-error branch is still present")

    def test_the_survey_is_delegated_not_duplicated(self):
        # Two implementations writing one row is how the two disagreed. mcli-runner must
        # defer to mimir's survey rather than carry its own copy.
        source = read("mcli-runner")
        self.assertIn("survey_mtls_certs", source)
        self.assertNotIn("MTLS Client Certificate is valid", source,
                         "the duplicate certificate check is still here")

    def test_a_loader_failure_warns_rather_than_passes(self):
        # If mimir cannot be loaded at all, the honest answer is "unknown".
        original = self.runner.load_mimir_module
        self.runner.load_mimir_module = lambda: None
        try:
            status, message = self.runner.survey_certs_via_mimir()
            self.assertEqual(status, "WARN", message)
            status, message = self.runner.classify_certificate(__file__, "Anything")
            self.assertEqual(status, "WARN", message)
        finally:
            self.runner.load_mimir_module = original

    def test_the_loader_uses_an_explicit_source_loader(self):
        # mimir is deployed as /usr/local/bin/mimir with no .py suffix, and importlib
        # infers a loader from the extension -- spec_from_file_location returns None for
        # an extensionless path, and the failure reads as "'NoneType' has no attribute
        # 'loader'". Verified against the live node.
        source = read("mcli-runner")
        self.assertIn("SourceFileLoader", source)


if __name__ == "__main__":
    unittest.main()
