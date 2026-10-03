#!/usr/bin/env python3
"""`hydra.dfs_egroup_replicas` was a table nothing wrote, and it is gone.

It sat in the schema with a suggestive shape -- an extent group, a node, a path, a state --
and every design that touched Sidon's storage began by establishing that it was not a source
of truth (docs/dfs/multi_disk.md). Two things could have been done about it: make it true,
or remove it. Making it true means a metadata write per replica per extent group, kept in
step with the files on every node through every crash between "file written" and "row
written", and checked by a scrub against disk. A table that can lie is exactly what
invariants I-3 and I-7 keep out of Purah's mark phase, so the best it could ever be is
advice. The placement the system uses is per vdisk (`dfs_vdisks.replicas`), and which node
created a group is `dfs_egroups`.

These tests hold the removal, and the two things that make it safe: nothing reads the table,
and the mark phase reads the block map and nothing else.

Run with:  python -m unittest test_egroup_replicas_removed
"""

import glob
import io
import os
import unittest

import helios_schema as schema

HERE = os.path.dirname(os.path.abspath(__file__))
TABLE = "dfs_egroup_replicas"


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def product_sources():
    """Everything that runs: Python, Rust and Elixir, not tests, docs or the schema ledger."""
    patterns = ("*.py", os.path.join("sidon", "src", "*.rs"),
                os.path.join("spectrum_phx", "lib", "**", "*.ex"))
    for pattern in patterns:
        for path in glob.glob(os.path.join(HERE, pattern), recursive=True):
            name = os.path.basename(path)
            if name.startswith("test_") or name == "helios_schema.py":
                continue
            yield path


class TheTableIsDroppedByAMigration(unittest.TestCase):

    def test_a_migration_drops_it_and_does_only_that(self):
        drops = [m for m in schema.MIGRATIONS
                 if any("DROP TABLE" in s and TABLE in s for s in m["statements"])]
        self.assertEqual(len(drops), 1)
        self.assertEqual(drops[0]["statements"],
                         ["DROP TABLE IF EXISTS hydra.%s;" % TABLE])

    def test_the_drop_is_idempotent(self):
        """It runs on clusters that have the table and on fresh ones that have just created
        it in 0006; both must end the same way, and a re-run must not fail."""
        for migration in schema.MIGRATIONS:
            for statement in migration["statements"]:
                if "DROP TABLE" in statement:
                    self.assertIn("IF EXISTS", statement)

    def test_it_runs_after_the_migration_that_created_it(self):
        ids = [m["id"] for m in schema.MIGRATIONS]
        created = next(m["id"] for m in schema.MIGRATIONS
                       if any("CREATE TABLE" in s and TABLE in s for s in m["statements"]))
        dropped = next(m["id"] for m in schema.MIGRATIONS
                       if any("DROP TABLE" in s and TABLE in s for s in m["statements"]))
        self.assertLess(ids.index(created), ids.index(dropped))

    def test_the_migration_that_created_it_was_not_rewritten(self):
        """An applied migration's text is its checksum. Editing 0006 to omit the table would
        make every live cluster refuse to start; the removal is a new migration."""
        by_id = {m["id"]: m for m in schema.MIGRATIONS}
        self.assertEqual(
            schema.checksum(by_id["0006-dfs-replication"]),
            "54f4801fcf38b24c4a6b5dd00263c0d50ce41fe7d2868c449a7fa3457d7530cd")

    def test_the_id_is_the_reserved_one(self):
        self.assertIn("0025-drop-dfs-egroup-replicas", [m["id"] for m in schema.MIGRATIONS])


class NothingReadsOrWritesIt(unittest.TestCase):

    def test_no_daemon_references_the_table(self):
        """Dropping a table something still reads turns a stale answer into an error."""
        for path in product_sources():
            self.assertNotIn(TABLE, read(path),
                             "%s still refers to hydra.%s" % (os.path.basename(path), TABLE))


class PurahMarksFromTheBlockMapAlone(unittest.TestCase):
    """I-3 and I-7: liveness comes from what the map points at, never from a table that
    describes where copies are. Stated here because it is what made removing the table
    safe, and it must stay true of whatever replaces it."""

    def test_the_mark_phase_reads_only_the_block_map(self):
        source = read(os.path.join(HERE, "sidon", "src", "purah.rs"))
        body = source[source.index("fn referenced_egroups"):]
        body = body[:body.index("\n    }\n")]
        self.assertIn("hydra.dfs_block_map", body)
        for other in ("dfs_egroups", "dfs_egroup_access", TABLE, "dfs_vdisks"):
            self.assertNotIn(other, body,
                             "the mark phase derives liveness from %s" % other)


if __name__ == "__main__":
    unittest.main()
