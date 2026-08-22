#!/usr/bin/env python3
"""A CQL column whose name is a Python keyword must survive the proxy.

`hydra.dfs_vdisks` has a column called `class`. The Cassandra driver returns rows as
`namedtuple`s built with `rename=True`, and `namedtuple` will not accept a Python
keyword as a field name -- it replaces it positionally, so `class` arrived as
`field_2_`.

Nothing failed. Every reader asked for `"class"`, got nothing, and used its default of
`rw`. So a sealed image loaded as writable, and `Vdisk::write`'s immutability check --
the thing that replaced DRBD's `--allow-two-primaries` for golden templates -- could
never fire. An attached image accepted writes and served them back. That was found by a
snapshot test asserting a read-only snapshot refuses a write, which it did not.

`class` is not special. Every one of these is a legal CQL identifier and a Python
keyword, and each would be renamed the same way at whatever position it happened to
occupy.

Run with:  python -m unittest test_daruk_column_names
"""

import ast
import io
import keyword
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
DARUK = os.path.join(HERE, "daruk.py")
SCHEMA = os.path.join(HERE, "helios_schema.py")

# `CREATE TABLE ... ( <name> <type>, ... )` and `ALTER TABLE ... ADD <name> <type>`.
CREATE_RE = re.compile(r"CREATE TABLE IF NOT EXISTS\s+\S+\s*\((.*?)\)\s*;", re.S)
ALTER_RE = re.compile(r"ALTER TABLE\s+\S+\s+ADD\s+([A-Za-z_][A-Za-z0-9_]*)\s")


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def declared_columns():
    """Every column name the schema declares, from CREATE and ALTER alike."""
    source = read(SCHEMA)
    names = set(ALTER_RE.findall(source))
    for body in CREATE_RE.findall(source):
        for part in body.split(","):
            part = part.strip()
            if not part or part.upper().startswith("PRIMARY KEY"):
                continue
            head = part.split()[0]
            if head.startswith("("):
                continue
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", head):
                names.add(head)
    return names


class KeywordColumnTests(unittest.TestCase):
    def test_the_schema_really_does_use_a_python_keyword_as_a_column(self):
        """If this ever stops being true the rest of the file is guarding nothing, and
        that is worth knowing rather than quietly passing."""
        offenders = sorted(n for n in declared_columns() if keyword.iskeyword(n))
        self.assertIn(
            "class", offenders,
            "hydra.dfs_vdisks no longer has a `class` column; this file's premise has "
            "changed and its reasoning should be re-read rather than deleted")

    def test_daruk_keys_rows_by_the_result_sets_column_names(self):
        """The fix, asserted on the source rather than on a live cluster.

        A namedtuple's `_fields` is the wrong source for a column name, and it is wrong
        only for keyword columns -- which is why it went unnoticed. `column_names` on the
        result set is what the server actually sent back.
        """
        source = read(DARUK)
        self.assertIn(
            "column_names", source,
            "daruk.py no longer reads the result set's column_names; if rows are keyed "
            "by namedtuple fields again, every keyword-named column silently becomes "
            "field_N_ and every reader of it silently gets its default")

        # And the namedtuple paths must come after it, as fallbacks rather than the
        # first thing tried.
        by_names = source.index("column_names")
        for fallback in ("_asdict", "_fields"):
            self.assertGreater(
                source.index(fallback), by_names,
                f"daruk.py consults `{fallback}` before the result set's column names, "
                f"which puts the renaming bug back")

    def test_the_renaming_this_guards_against_is_real(self):
        """Not a hypothetical. `namedtuple(rename=True)` is what the driver uses."""
        import collections

        row = collections.namedtuple("Row", ["vdisk_id", "size_bytes", "class"], rename=True)
        self.assertNotIn("class", row._fields)
        self.assertEqual(row._fields[2], "_2")

    def test_every_keyword_column_is_listed_here_deliberately(self):
        """A new keyword-named column is fine -- daruk handles it now -- but it is worth
        being told about, because anything that reads rows *without* going through daruk
        inherits the same trap."""
        known = {"class"}
        found = {n for n in declared_columns() if keyword.iskeyword(n)}
        surprising = sorted(found - known)
        self.assertEqual(
            surprising, [],
            "these columns are Python keywords and are new since this test was written: "
            f"{surprising}. Daruk handles them; anything reading Scylla directly may not.")


if __name__ == "__main__":
    unittest.main()
