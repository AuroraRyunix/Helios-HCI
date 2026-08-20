#!/usr/bin/env python3
"""Tests for the recorded schema migration runner.

Every assertion here is a way the previous arrangement -- 38 independent
`CREATE TABLE IF NOT EXISTS` statements across five daemons -- could and did go wrong
silently: two daemons migrating at once, a migration applied twice, a migration edited
after it shipped, a crashed migrator wedging the cluster, or a lock released by someone
who no longer held it.

Run with:  python -m unittest test_helios_schema
"""

import unittest

import helios_schema as schema


class FakeDatabase:
    """A cqlsh-shaped stand-in: takes CQL text, returns (rc, stdout, stderr).

    It is not a CQL engine. It records statements and answers the two reads the runner
    actually makes -- the applied-migrations select and the lock LWT -- which is all
    that is needed to drive every branch.
    """

    def __init__(self, applied=None, lock_taken_by=None, fail_on=None):
        self.statements = []
        self.applied = dict(applied or {})
        self.lock_holder = lock_taken_by
        self.fail_on = fail_on

    def __call__(self, cql):
        self.statements.append(cql)

        if self.fail_on and self.fail_on in cql:
            return 1, "", "injected failure"

        if cql.startswith("SELECT id, checksum FROM hydra.schema_migrations"):
            rows = ["\n id | checksum", "----+---------"]
            rows += [" %s | %s" % (k, v) for k, v in self.applied.items()]
            rows.append("\n(%d rows)" % len(self.applied))
            return 0, "\n".join(rows), ""

        if "INSERT INTO hydra.schema_lock" in cql:
            if self.lock_holder is None:
                self.lock_holder = "self"
                return 0, "\n [applied]\n-----------\n      True\n", ""
            return 0, "\n [applied] | name\n-----------+------\n     False | x\n", ""

        if "DELETE FROM hydra.schema_lock" in cql:
            self.lock_holder = None
            return 0, "\n [applied]\n-----------\n      True\n", ""

        if cql.startswith("INSERT INTO hydra.schema_migrations"):
            # Record it the way the real table would, so a second ensure_schema is a
            # no-op the way it would be in production.
            for migration in schema.MIGRATIONS:
                if schema.quote(migration["id"]) in cql:
                    self.applied[migration["id"]] = schema.checksum(migration)
        return 0, "", ""

    def ddl(self):
        return [s for s in self.statements if s.startswith("CREATE TABLE")]


class BaselineTests(unittest.TestCase):
    def test_the_baseline_covers_every_table_the_daemons_declared(self):
        statements = schema.MIGRATIONS[0]["statements"]
        tables = {s.split()[5] for s in statements}
        # If a daemon gains a table and the baseline is not updated, the daemon's own
        # CREATE is gone and the table never exists. 31 is the deduplicated count taken
        # from the five daemons.
        self.assertEqual(len(tables), 31)
        self.assertEqual(len(statements), len(tables), "a table is declared twice")

    def test_every_baseline_statement_is_idempotent(self):
        # Adoption of an existing cluster depends on this: the baseline runs against a
        # database that already has all 31 tables and must change nothing.
        for statement in schema.MIGRATIONS[0]["statements"]:
            self.assertIn("IF NOT EXISTS", statement)
            self.assertTrue(statement.rstrip().endswith(";"), statement[:60])

    def test_migration_ids_are_unique_and_ordered(self):
        ids = [m["id"] for m in schema.MIGRATIONS]
        self.assertEqual(len(ids), len(set(ids)), "duplicate migration id")
        self.assertEqual(ids, sorted(ids), "migrations are not in id order")


class ChecksumTests(unittest.TestCase):
    def test_checksum_ignores_whitespace_but_not_content(self):
        a = {"id": "x", "statements": ["CREATE TABLE  a ( b int );"]}
        b = {"id": "x", "statements": ["CREATE TABLE a ( b int );"]}
        c = {"id": "x", "statements": ["CREATE TABLE a ( b text );"]}
        self.assertEqual(schema.checksum(a), schema.checksum(b))
        self.assertNotEqual(schema.checksum(a), schema.checksum(c))

    def test_editing_an_applied_migration_raises(self):
        # The failure this catches: someone fixes a typo in a migration that has already
        # run on half the fleet. Both halves then believe they are current.
        applied = {schema.MIGRATIONS[0]["id"]: "0" * 64}
        with self.assertRaises(schema.SchemaDivergence) as caught:
            schema.pending(applied)
        self.assertIn("Add a new migration instead", str(caught.exception))

    def test_a_correctly_recorded_migration_is_not_pending(self):
        applied = {m["id"]: schema.checksum(m) for m in schema.MIGRATIONS}
        self.assertEqual(schema.pending(applied), [])


class OutputParsingTests(unittest.TestCase):
    def test_parse_applied_ignores_cqlsh_furniture(self):
        stdout = (
            "\n id           | checksum\n"
            "--------------+----------\n"
            " 0001-baseline | abc123\n"
            " 0002-thing    | def456\n"
            "\n(2 rows)\n")
        self.assertEqual(schema.parse_applied(stdout),
                         {"0001-baseline": "abc123", "0002-thing": "def456"})

    def test_parse_applied_on_empty_table(self):
        self.assertEqual(schema.parse_applied("\n id | checksum\n----+----\n\n(0 rows)\n"), {})
        self.assertEqual(schema.parse_applied(""), {})
        self.assertEqual(schema.parse_applied(None), {})

    def test_lwt_applied_reads_the_marker_not_the_exit_code(self):
        # A rejected LWT exits zero. Reading [applied] is the only way to tell "I took
        # the lock" from "someone else holds it".
        self.assertTrue(schema.lwt_applied("\n [applied]\n-----------\n      True\n"))
        self.assertFalse(schema.lwt_applied("\n [applied] | holder\n---+---\n False | b\n"))

    def test_lwt_applied_is_false_when_the_marker_is_missing(self):
        # Guessing "applied" here would let two daemons migrate at once.
        self.assertFalse(schema.lwt_applied(""))
        self.assertFalse(schema.lwt_applied("some unexpected output"))
        self.assertFalse(schema.lwt_applied(None))


class QuotingTests(unittest.TestCase):
    def test_embedded_quotes_are_doubled(self):
        self.assertEqual(schema.quote("a'b"), "'a''b'")
        self.assertEqual(schema.quote("plain"), "'plain'")


class EnsureSchemaTests(unittest.TestCase):
    def test_a_fresh_cluster_applies_and_records_every_migration(self):
        db = FakeDatabase()
        applied = schema.ensure_schema(db, node_id="10.0.0.1", now_ms=1)

        self.assertEqual(applied, [m["id"] for m in schema.MIGRATIONS])
        self.assertGreaterEqual(len(db.ddl()), 31)
        self.assertTrue(any("INSERT INTO hydra.schema_migrations" in s
                            for s in db.statements))

    def test_a_second_run_does_nothing(self):
        db = FakeDatabase()
        schema.ensure_schema(db, node_id="10.0.0.1", now_ms=1)
        before = len(db.statements)

        self.assertEqual(schema.ensure_schema(db, node_id="10.0.0.1", now_ms=2), [])
        # Only the bookkeeping CREATEs and the select run on a no-op pass. In
        # particular the lock is never taken, so a healthy restart cannot block a peer.
        self.assertFalse(any("INSERT INTO hydra.schema_lock" in s
                             for s in db.statements[before:]))

    def test_losing_the_lock_race_returns_without_migrating(self):
        # The other node is mid-migration. Blocking here would turn its crash into this
        # node's hang, so this returns and lets the next start pick things up.
        db = FakeDatabase(lock_taken_by="10.0.0.2")
        self.assertEqual(schema.ensure_schema(db, node_id="10.0.0.1", now_ms=1), [])
        self.assertFalse(any(s.startswith("CREATE TABLE IF NOT EXISTS hydra.vms")
                             for s in db.statements))

    def test_the_lock_is_released_even_when_a_migration_fails(self):
        db = FakeDatabase(fail_on="hydra.vms")
        with self.assertRaises(schema.SchemaError):
            schema.ensure_schema(db, node_id="10.0.0.1", now_ms=1)
        self.assertTrue(any("DELETE FROM hydra.schema_lock" in s for s in db.statements),
                        "a failed migration left the lock held")

    def test_the_lock_carries_a_ttl(self):
        # Without it, a daemon killed mid-migration wedges every other node forever and
        # there is nobody to clear the row by hand.
        db = FakeDatabase()
        schema.ensure_schema(db, node_id="10.0.0.1", now_ms=1)
        lock = next(s for s in db.statements if "INSERT INTO hydra.schema_lock" in s)
        self.assertIn("USING TTL", lock)
        self.assertIn("IF NOT EXISTS", lock)

    def test_the_lock_is_released_conditionally(self):
        # An unconditional delete would let a node whose TTL had expired release the
        # lock another node has since taken, allowing two migrators at once.
        db = FakeDatabase()
        schema.ensure_schema(db, node_id="10.0.0.1", now_ms=1)
        release = next(s for s in db.statements if "DELETE FROM hydra.schema_lock" in s)
        self.assertIn("IF holder =", release)
        self.assertIn("'10.0.0.1'", release)

    def test_a_database_that_cannot_be_reached_raises_rather_than_reporting_success(self):
        db = FakeDatabase(fail_on="schema_migrations")
        with self.assertRaises(schema.SchemaError):
            schema.ensure_schema(db, node_id="10.0.0.1", now_ms=1)

    def test_a_malformed_execute_is_reported_clearly(self):
        with self.assertRaises(schema.SchemaError) as caught:
            schema.ensure_schema(lambda cql: "not a tuple", node_id="x", now_ms=1)
        self.assertIn("must return (rc, stdout, stderr)", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
