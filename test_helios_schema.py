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

    def test_lwt_applied_reads_the_first_column_of_a_multi_column_row(self):
        """Captured verbatim from cqlsh against Scylla 5.4.

        A rejected LWT returns the conditioned columns beside [applied]; an accepted one
        returns them as null. An earlier version compared the whole stripped line to
        "True", which matched only the single-column case -- so every successful lock
        acquisition was read as a lost race and the caller returned while holding the
        lock it had just taken. The TTL was the only thing that eventually freed it.
        """
        accepted = "\n".join([
            "",
            " [applied] | name | acquired_at | holder",
            "-----------+------+-------------+--------",
            "      True | null |        null |   null",
            "",
        ])
        rejected = "\n".join([
            "",
            " [applied] | name         | acquired_at                     | holder",
            "-----------+--------------+---------------------------------+--------",
            "     False | hydra-schema | 2026-08-20 10:00:14.701000+0000 | 10.0.0.1",
            "",
        ])
        self.assertTrue(schema.lwt_applied(accepted))
        self.assertFalse(schema.lwt_applied(rejected))

    def test_lwt_applied_ignores_the_header_and_rule(self):
        # "[applied]" appears in the header; the rule is dashes and pluses. Neither is a
        # row, and treating either as one would answer from the wrong line.
        single_column = "\n".join(
            ["", " [applied]", "-----------", "      True", "", "(1 rows)", ""])
        self.assertTrue(schema.lwt_applied(single_column))

    def test_both_parsers_handle_daruk_output_as_well_as_cqlsh(self):
        """The daemons do not use cqlsh; they proxy to Daruk.

        Daruk's /query returns decoded row values joined by a space, with no column names
        and no [applied] marker. Reading only the cqlsh form meant a successful lock
        acquisition looked like a lost race -- so the runner returned holding the lock it
        had just taken -- and every applied migration looked unapplied, so it would try
        to reapply the whole list on every start. Verified against a real daemon.
        """
        self.assertTrue(schema.lwt_applied("True null null null"))
        self.assertFalse(schema.lwt_applied("False 10.10.102.41"))
        daruk_rows = "\n".join(["0001-baseline abc123", "0002-cluster-locks def456"])
        self.assertEqual(
            schema.parse_applied(daruk_rows),
            {"0001-baseline": "abc123", "0002-cluster-locks": "def456"})

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


class BackfillStepTests(unittest.TestCase):
    """A migration may carry a step that derives rows from rows that already exist.

    `CREATE TABLE` is not enough for a constraint added to a cluster that is already
    running: a table keyed by VLAN id constrains nothing until the VLANs already in use
    are in it. That step has to read, so it cannot be a statement in a list, and it has to
    be able to find something an operator must decide about -- which a migration running
    at daemon start must never decide by itself.
    """

    def with_only(self, migration):
        saved = schema.MIGRATIONS
        schema.MIGRATIONS = [migration]
        self.addCleanup(setattr, schema, "MIGRATIONS", saved)

    def migration(self, backfill):
        return {
            "id": "9999-test",
            "statements": ["CREATE TABLE IF NOT EXISTS hydra.scratch ( a int PRIMARY KEY );"],
            "backfill": backfill,
        }

    def test_a_backfill_runs_and_what_it_finds_reaches_the_reporter(self):
        seen = []

        def backfill(execute, report, now_ms):
            execute("SELECT JSON * FROM hydra.scratch;")
            report("two networks carry VLAN 100")

        self.with_only(self.migration(backfill))
        db = FakeDatabase()
        applied = schema.ensure_schema(db, node_id="10.0.0.1", now_ms=1, report=seen.append)
        self.assertEqual(applied, ["9999-test"])
        self.assertEqual(seen, ["two networks carry VLAN 100"])

    def test_a_backfill_that_raises_leaves_the_migration_unrecorded(self):
        # So the next daemon start repeats it, rather than recording a migration that did
        # half its work. The statements before it are idempotent, which is what makes
        # repeating them free.
        def backfill(execute, report, now_ms):
            raise schema.SchemaError("the network table could not be read")

        self.with_only(self.migration(backfill))
        db = FakeDatabase()
        with self.assertRaises(schema.SchemaError):
            schema.ensure_schema(db, node_id="10.0.0.1", now_ms=1)
        self.assertFalse(
            any("INSERT INTO hydra.schema_migrations" in s for s in db.statements),
            "a migration whose backfill failed was recorded as applied")
        self.assertTrue(any("DELETE FROM hydra.schema_lock" in s for s in db.statements),
                        "a failed backfill left the cluster lock held")

    def test_a_backfill_does_not_run_again_once_the_migration_is_recorded(self):
        runs = []

        def backfill(execute, report, now_ms):
            runs.append(now_ms)

        self.with_only(self.migration(backfill))
        db = FakeDatabase()
        schema.ensure_schema(db, node_id="10.0.0.1", now_ms=1)
        schema.ensure_schema(db, node_id="10.0.0.1", now_ms=2)
        self.assertEqual(runs, [1])

    def test_a_backfill_changes_the_migration_checksum(self):
        # Not to protect the backfill's body -- only its name is hashed -- but so that
        # attaching one to a migration that has already shipped is refused rather than
        # silently skipped on every cluster that already ran it.
        plain = {"id": "x", "statements": ["CREATE TABLE a ( b int );"]}
        with_step = dict(plain, backfill=lambda execute, report, now_ms: None)
        self.assertNotEqual(schema.checksum(plain), schema.checksum(with_step))

    # The checksums hydra.schema_migrations holds on the test cluster, read out of it on
    # 2026-09-10. They are frozen: a migration that has been applied anywhere can never
    # hash differently again, whatever else the runner grows. Changing how `checksum`
    # works -- adding the backfill key was such a change -- must leave every one of these
    # exactly as it is, or every daemon raises SchemaDivergence on its next start and the
    # cluster does not come back.
    RECORDED = {
        "0001-baseline":
            "f5ff9871b3f1c6ba1759a1ce7961d6525658f5935b9a5f9861268447443f36dd",
        "0002-cluster-locks":
            "4f9cc45f93b888ceacb9039c1e2ece3a9a63687c918c8a10375e134b80af192f",
        "0003-bound-task-history":
            "aec6f8441d6effd20286d9fb26ca1feb0bea216b31493140fba85f5e6bf0387a",
        "0004-urbosa-transit-pool":
            "7f7839db97e9b0d25e21d689ae01f242786f6222f341ef954841a71c0c48b9f4",
        "0005-dfs-extent-store":
            "fd24e4f0070710b275691f71a343656810ff24900874730ff60f84fd49bbc28d",
        "0006-dfs-replication":
            "54f4801fcf38b24c4a6b5dd00263c0d50ce41fe7d2868c449a7fa3457d7530cd",
        "0007-dfs-snapshots":
            "11beb5977ba67ac24da62f6a7eeaf846514c93458e53af8b4cc0cb1d20bda195",
        "0008-container-compression":
            "d9befd898db1e73fc2eaa71845afff85eaf4b5ddb51772e927315c6fcb3af2fd",
    }

    def test_every_migration_that_has_shipped_still_hashes_the_way_it_was_recorded(self):
        by_id = {m["id"]: m for m in schema.MIGRATIONS}
        for migration_id, recorded in self.RECORDED.items():
            self.assertIn(migration_id, by_id, "a shipped migration was removed")
            self.assertEqual(
                schema.checksum(by_id[migration_id]), recorded,
                f"{migration_id} no longer hashes to what every cluster recorded")


if __name__ == "__main__":
    unittest.main()
