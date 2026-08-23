#!/usr/bin/env python3
"""One CQL query layer, and the guard that only three files used to have.

`run_cql_query` was defined fifteen times across fifteen files. Most copies were
byte-identical; the differences were what mattered. Catalyst, Dagur and Mipha had grown a
guard that refuses conditional statements, and the other twelve never got it -- so twelve
files could hand a compare-and-swap to a path that reports a lost race as a success.

Daruk's `/query` renders a rejected lightweight transaction as its row of values joined by
spaces and returns rc=0, which is indistinguishable from a write that applied. Four of
those twelve files contained lightweight transactions.

These tests assert the property rather than the change: there is one definition, every
user imports it, the guard is on by default, and the callers that legitimately need a
conditional statement ask for the unguarded path by name.

Run with:  python -m unittest test_cql_layer
"""

import ast
import io
import os
import unittest

import helios_cql

HERE = os.path.dirname(os.path.abspath(__file__))

# Every file that used to carry its own copy.
FORMER_COPIES = [
    "catalyst.py", "check_updates.py", "cluster_new.py", "dagur.py", "gatoway.py",
    "hylia.py", "logos.py", "mimir.py", "mipha.py", "spark_daemon_decoded.py",
    "spectrum_server.py", "urbosa.py", "urbosa_bootstrap.py", "valcli.py", "vali.py",
]


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def toplevel_defs(name):
    tree = ast.parse(read(name), filename=name)
    out = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            out.add(node.name)
    return out


class ThereIsExactlyOneQueryLayer(unittest.TestCase):
    def test_no_module_defines_its_own_run_cql_query(self):
        offenders = [name for name in FORMER_COPIES
                     if "run_cql_query" in toplevel_defs(name)]
        self.assertEqual(
            offenders, [],
            "a copy of the query layer has come back, which is how the conditional-"
            "statement guard came to exist in three files out of fifteen: %s" % offenders)

    def test_no_module_defines_its_own_guard(self):
        for symbol in ("is_conditional_cql", "ConditionalStatementError",
                       "run_conditional_cql_query", "cql_escape", "cql_int"):
            offenders = [name for name in FORMER_COPIES if symbol in toplevel_defs(name)]
            self.assertEqual(
                offenders, [], "%s is defined outside helios_cql in %s" % (symbol, offenders))

    def test_every_former_copy_imports_it_instead(self):
        for name in FORMER_COPIES:
            self.assertIn(
                "from helios_cql import", read(name),
                "%s uses the query layer but does not import it" % name)


class TheGuardIsOnByDefault(unittest.TestCase):
    def test_a_compare_and_swap_is_refused(self):
        for statement in (
            "INSERT INTO hydra.t (a) VALUES (1) IF NOT EXISTS;",
            "UPDATE hydra.t SET a = 1 WHERE b = 2 IF a = 0;",
            "DELETE FROM hydra.t WHERE a = 1 IF owner = 'x';",
            "BEGIN BATCH INSERT INTO hydra.t (a) VALUES (1) IF NOT EXISTS; APPLY BATCH;",
        ):
            self.assertTrue(helios_cql.is_conditional_cql(statement), statement)
            with self.assertRaises(helios_cql.ConditionalStatementError):
                helios_cql.run_cql_query(statement)

    def test_ddl_is_not_a_compare_and_swap(self):
        """`CREATE TABLE IF NOT EXISTS` carries nothing a caller needs to read, and
        refusing it would break every schema path."""
        for statement in (
            "CREATE TABLE IF NOT EXISTS hydra.t (a int PRIMARY KEY);",
            "CREATE KEYSPACE IF NOT EXISTS hydra WITH replication = {'class': 'SimpleStrategy'};",
            "ALTER TABLE hydra.t ADD b text;",
            "DROP TABLE IF EXISTS hydra.t;",
        ):
            self.assertFalse(helios_cql.is_conditional_cql(statement), statement)

    def test_the_word_if_inside_a_literal_does_not_trip_it(self):
        """Job output, task errors and operator commands all end up inside CQL literals,
        and any of them can contain the word "if". Refusing an ordinary INSERT because a
        job printed "check if the volume is mounted" would be its own outage."""
        statement = (
            "INSERT INTO hydra.dagur_runs (job_name, output) "
            "VALUES ('backup', 'check if the volume is mounted');")
        self.assertFalse(helios_cql.is_conditional_cql(statement))

    def test_a_doubled_quote_is_an_escaped_quote_not_a_closing_one(self):
        statement = (
            "INSERT INTO hydra.dagur_runs (job_name, output) "
            "VALUES ('backup', 'it''s fine, verify if needed');")
        self.assertFalse(helios_cql.is_conditional_cql(statement))

    def test_a_plain_write_passes_the_guard(self):
        self.assertFalse(
            helios_cql.is_conditional_cql("INSERT INTO hydra.t (a) VALUES (1);"))

    def test_the_refusal_says_what_to_use_instead(self):
        with self.assertRaises(helios_cql.ConditionalStatementError) as caught:
            helios_cql.run_cql_query("UPDATE hydra.t SET a = 1 IF b = 2;")
        message = str(caught.exception)
        self.assertIn("run_conditional_cql_query", message)
        self.assertIn("run_lwt", message)


class TheUnguardedPathIsAskedForByName(unittest.TestCase):
    """Two legitimate kinds of caller, and both say so at the call site."""

    def test_idempotent_seeding_uses_it(self):
        # Bootstrap runs on every node; a lost race means the row is already there.
        spectrum = read("spectrum_server.py")
        self.assertIn("run_conditional_cql_query(insert_default)", spectrum)
        self.assertIn("run_conditional_cql_query(insert_default_network)", read("vali.py"))

    def test_a_caller_that_reads_the_verdict_uses_it(self):
        # urbosa's own comment: trusting the return code "is how two routers end up on
        # one /30".
        urbosa = read("urbosa.py")
        self.assertIn("rc, stdout, stderr = run_conditional_cql_query(", urbosa)
        self.assertIn("lwt_was_applied(stdout)", urbosa)

    def test_no_seeding_call_site_still_uses_the_guarded_path(self):
        spectrum = read("spectrum_server.py")
        for seed in ("insert_diagnostics", "insert_storage_scrub", "insert_db_compaction",
                     "insert_mimir_default", "insert_metadata_backup"):
            self.assertNotIn(
                "run_cql_query(%s)" % seed, spectrum,
                "%s is IF NOT EXISTS and would raise at bootstrap" % seed)


class ItIsShippedLikeEveryOtherSharedModule(unittest.TestCase):
    """A module every daemon imports has to reach the node before they restart."""

    def test_the_rollout_uploads_it(self):
        deploy = read("deploy_updates.py")
        self.assertIn('put_text_file(sftp, "helios_cql.py", "/usr/local/bin/helios_cql.py")', deploy)
        self.assertIn('("helios_cql.py", "helios_cql.py")', deploy)

    def test_the_spectrum_image_carries_it(self):
        self.assertIn("COPY helios_cql.py .", read("Dockerfile"))

    def test_provisioning_writes_it_to_both_places(self):
        provision = read("provision.py")
        self.assertIn('node.write_file("/usr/local/bin/helios_cql.py"', provision)
        self.assertIn('node.write_file("/tmp/spectrum_build/helios_cql.py"', provision)

    def test_it_is_embedded_from_the_current_source(self):
        self.assertIn('"HELIOS_CQL_B64": "helios_cql.py"', read("sync_provision.py"))


class TheModuleStandsAlone(unittest.TestCase):
    def test_it_imports_nothing_outside_the_standard_library(self):
        """Every daemon imports this, including ones that run before anything is
        installed. A third-party dependency here would be a bootstrap ordering problem."""
        tree = ast.parse(read("helios_cql.py"), filename="helios_cql.py")
        stdlib = {"base64", "json", "re", "socket", "subprocess", "urllib", "urllib.request"}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.assertIn(alias.name.split(".")[0], stdlib, alias.name)
            elif isinstance(node, ast.ImportFrom) and node.module:
                self.assertIn(node.module.split(".")[0], stdlib, node.module)


if __name__ == "__main__":
    unittest.main()
