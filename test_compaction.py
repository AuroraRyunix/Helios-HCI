#!/usr/bin/env python3
"""Compaction and the dedup estimator: how they are wired, and the rules they must keep.

The behaviour is proved in Rust (`sidon/src/purah/compact/tests.rs`, `dedup.rs`) against a model
of Hydra and real extent files, with failure injected at every step. What cannot be proved there
is that the operator can reach it, and that nobody has quietly made it do more than D-32 allows.
Those are the things pinned here, read statically in the way `test_multi_disk.py` reads the
tiering pass:

  * the control socket, spark-daemon's allow-list, `helios_sidon` and `valcli` all know the two
    operations, and the allow-list agrees with what sidon dispatches;
  * compaction plans unless told to apply, and nothing runs it, or the estimator, on a timer;
  * the estimator writes nothing, and compaction deletes nothing;
  * the two Daruk compare-and-swaps are conditional on what the scan saw;
  * no migration was needed, so none was added;
  * `valcli`'s argument parsing refuses what it does not understand, and its cross-node merge
    counts a duplicate that straddles two nodes once.

Run with:  python -m unittest test_compaction
"""

import ast
import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def read(*parts):
    with io.open(os.path.join(HERE, *parts), encoding="utf-8") as handle:
        return handle.read()


def rust(*parts):
    return read("sidon", "src", *parts)


def rust_function(source, signature):
    start = source.index(signature)
    indent = len(source[:start].rsplit("\n", 1)[-1])
    end = re.search(r"\n%s\}" % (" " * indent), source[start + len(signature):])
    return source[start:start + len(signature) + (end.end() if end else len(source))]


def production(source):
    """Everything before the unit tests, which are allowed to name what the code may not."""
    return source.split("#[cfg(test)]")[0]


def load_valcli(names):
    """Compile only the named top-level functions out of valcli.py, which does real work at import."""
    tree = ast.parse(read("valcli.py"))
    body = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    missing = set(names) - {n.name for n in body}
    assert not missing, "valcli.py no longer defines %s" % sorted(missing)
    ns = {}
    exec(compile(ast.Module(body=body, type_ignores=[]), "valcli.py", "exec"), ns)
    return ns


class TheOperatorCanReachBoth(unittest.TestCase):
    OPS = ("purah-compact", "purah-dedup")

    def test_sidon_dispatches_both(self):
        control = rust("control.rs")
        for op in self.OPS:
            self.assertIn('"%s" =>' % op, control)

    def test_spark_forwards_both_as_node_operations(self):
        daemon = read("spark_daemon_decoded.py")
        node_ops = daemon[daemon.index("DFS_NODE_OPS"):daemon.index("DFS_OPS =")]
        vdisk_ops = daemon[daemon.index("DFS_VDISK_OPS"):daemon.index("DFS_NODE_OPS")]
        for op in self.OPS:
            self.assertIn('"%s"' % op, node_ops)
            self.assertNotIn('"%s"' % op, vdisk_ops,
                             "%s takes no vdisk; listing it there has spark refuse it for a missing id" % op)

    def test_the_python_client_has_a_function_for_each(self):
        client = read("helios_sidon.py")
        self.assertIn('call("purah-compact", apply=bool(apply), **kw)', client)
        self.assertIn('call("purah-dedup", **kw)', client)

    def test_valcli_exposes_both_commands_and_their_ops(self):
        valcli = read("valcli.py")
        for command in ("storage.compact", "storage.dedup.estimate"):
            self.assertIn('cmd == "%s"' % command, valcli)
            self.assertIn("valcli %s" % command, valcli, "%s is not in the help text" % command)
        self.assertIn('"op": "purah-compact"', valcli)
        self.assertIn('"op": "purah-dedup"', valcli)


class NothingRunsThemUnattended(unittest.TestCase):
    """D-22's reason, applied again: nobody has yet watched what these do on this cluster's data."""

    def test_the_background_loop_calls_neither(self):
        loop = rust_function(rust("control.rs"), "pub fn start_purah(")
        for forbidden in ("compact", "dedup"):
            self.assertNotIn(forbidden, loop)

    def test_compaction_plans_unless_told_to_apply(self):
        body = rust_function(rust("control.rs"), "fn op_purah_compact(")
        self.assertIn('Value::as_bool).unwrap_or(false)', body)
        self.assertIn("apply:", body)
        self.assertIn("pub apply: bool", rust("purah", "compact.rs"))
        self.assertIn("apply: false", rust("purah", "compact.rs"))

    def test_valcli_plans_unless_given_apply(self):
        ns = load_valcli(("_compact_request",))
        self.assertFalse(ns["_compact_request"]([])["apply"])
        self.assertTrue(ns["_compact_request"](["--apply"])["apply"])

    def test_the_command_is_not_a_cron_or_dagur_job(self):
        for name in ("dagur.py", "mipha.py", "hylia.py", "lanayru.py"):
            text = read(name)
            self.assertNotIn("storage.compact", text)
            self.assertNotIn("purah-compact", text)


class TheEstimatorWritesNothing(unittest.TestCase):
    def setUp(self):
        self.dedup = production(rust("purah", "dedup.rs"))

    def test_it_is_handed_a_reader_and_not_something_that_can_write(self):
        self.assertIn("pub fn run<D: Rows>(", self.dedup)
        self.assertNotIn("Db", self.dedup.replace("DEDUP", ""))
        for forbidden in ("cas(", "remove_file", "rename", "create(", "DELETE FROM", "INSERT", "UPDATE", "write_all",
                          "stage_", "OpenOptions"):
            self.assertNotIn(forbidden, self.dedup, "the estimator performs %r" % forbidden)

    def test_it_does_not_take_the_curators_lock(self):
        body = rust_function(rust("control.rs"), "fn op_purah_dedup(")
        self.assertNotIn("purah_state", body)

    def test_it_changes_no_extent_id_and_adds_no_setting_or_migration(self):
        import helios_schema
        ids = [m["id"] for m in helios_schema.MIGRATIONS]
        self.assertEqual([i for i in ids if i.startswith(("0018", "0019", "0024", "0026", "0027", "0028",
                                                          "0029", "0033", "0034", "0035"))], [],
                         "a migration appeared for compaction or the estimator, which need none")
        for name in ("helios_schema.py", "valcli.py", "helios_sidon.py"):
            self.assertNotRegex(read(name), r"dedup(_enabled|\.enable|_mode)", name)


class CompactionDeletesNothing(unittest.TestCase):
    def setUp(self):
        self.compact = production(rust("purah", "compact.rs"))

    def test_the_old_group_is_left_to_the_sweep(self):
        # What it may remove is its own temporary after a failed batch; it never reaches the
        # statements or helpers that reclaim a group.
        for forbidden in ("DELETE FROM", '"dead"', "remove_all", "remove_stray", "egroup-state"):
            self.assertNotIn(forbidden, self.compact, "compaction performs %r" % forbidden)
        self.assertIn("remove_file(&temp)", self.compact)

    def test_the_order_of_a_batch_is_the_safety_property(self):
        body = rust_function(self.compact, "pub fn execute_bin<")
        order = [body.index(marker) for marker in (
            "read_source(ctx, c)", "stage_new(", "Step::Replicated", "/v1/dfs/egroup-create",
            "stage_publish(", "Step::Verified", "ctx.env.hold(", "/v1/dfs/block-map-repoint")]
        self.assertEqual(order, sorted(order),
                         "bytes verified, staged, replicated, registered, published, then rows moved")

    def test_only_a_sealed_group_is_a_candidate_and_young_ones_are_skipped(self):
        analyze = rust_function(self.compact, "pub fn analyze(")
        self.assertIn('g.state != "sealed"', analyze)
        self.assertIn("inp.grace_ms", analyze)
        self.assertIn("in_flight.contains", analyze)

    def test_a_writable_vdisk_is_only_rewritten_while_its_drains_are_held(self):
        analyze = rust_function(self.compact, "pub fn analyze(")
        self.assertIn("WritableAndDetached", analyze)
        vdisk = rust("vdisk.rs")
        self.assertIn("pub fn try_hold_drains(", vdisk)
        self.assertIn("self.gate.try_begin()", rust_function(vdisk, "pub fn try_hold_drains("))
        env = rust_function(rust("control.rs"), "fn hold(")
        self.assertIn("try_hold_drains()", env)

    def test_it_runs_under_the_sweeps_lock(self):
        body = rust_function(rust("control.rs"), "fn op_purah_compact(")
        self.assertIn("purah_state.lock()", body)


class TheDarukSwapsAreConditional(unittest.TestCase):
    """Read from the source: importing daruk.py wants a Cassandra driver this suite does not."""

    @classmethod
    def setUpClass(cls):
        source = read("daruk.py")
        cls.source = source
        start = source.index('"/v1/dfs/block-map-repoint": {')
        mid = source.index('"/v1/dfs/extent-repoint": {')
        end = source.index("\n}\n", mid)
        cls.block = source[start:mid]
        cls.extent = source[mid:end]

    def cql(self, block):
        text = block[block.index('"cql": ('):block.index('"binds"')]
        return "".join(re.findall(r'"([^"]*)"', text)[1:])

    def test_a_block_map_row_moves_only_if_it_is_still_where_the_scan_saw_it(self):
        cql = self.cql(self.block)
        for column in ("egroup_id", "egroup_offset", "length"):
            self.assertIn("%s = ?" % column, cql.split(" IF ")[1])
        # Only the location is written: identity, length and epoch are untouched.
        self.assertEqual(cql.split(" SET ")[1].split(" WHERE ")[0], "egroup_id = ?, egroup_offset = ?")

    def test_an_extent_row_moves_only_if_it_is_still_where_the_scan_saw_it(self):
        condition = self.cql(self.extent).split(" IF ")[1]
        self.assertIn("egroup_id = ?", condition)
        self.assertIn("egroup_offset = ?", condition)

    def test_every_expected_value_is_required_and_has_no_default(self):
        seen = 0
        for block in (self.block, self.extent):
            for line in block.splitlines():
                if '"expected_' in line and '"type"' in line:
                    seen += 1
                    self.assertIn('"required": True', line)
                    self.assertNotIn('"default"', line)
        self.assertEqual(seen, 5)

    def test_the_rust_side_names_the_same_paths(self):
        compact = rust("purah", "compact.rs")
        for path in ("/v1/dfs/block-map-repoint", "/v1/dfs/extent-repoint", "/v1/dfs/egroup-create"):
            self.assertIn('"%s"' % path, compact)
            self.assertIn('"%s"' % path, self.source)


class ValcliParsesStrictlyAndMergesAcrossNodes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ns = load_valcli(("_compact_request", "_dedup_request", "_merge_dedup"))

    def test_limits_are_passed_through_as_numbers(self):
        request = self.ns["_compact_request"](["--apply", "--threshold", "0.4", "--max-groups", "3",
                                               "--max-bytes", "1000", "--seconds", "20", "--rate", "5"])
        self.assertEqual(request, {"op": "purah-compact", "apply": True, "threshold": 0.4,
                                   "max_groups": 3, "max_bytes": 1000, "seconds": 20,
                                   "rate_bytes_per_second": 5})

    def test_an_unknown_or_malformed_argument_is_an_error_and_not_ignored(self):
        for bad in (["--aplly"], ["--threshold"], ["--threshold", "lots"], ["--max-groups", "1.5"], ["extra"]):
            with self.assertRaises(SystemExit):
                self.ns["_compact_request"](bad)

    def test_the_sample_is_a_fraction(self):
        self.assertEqual(self.ns["_dedup_request"](["--sample", "0.25"])["sample"], 0.25)
        for bad in (["--sample", "0"], ["--sample", "1.5"], ["--sample", "x"], ["--nope"]):
            with self.assertRaises(SystemExit):
                self.ns["_dedup_request"](bad)

    def test_a_duplicate_that_straddles_two_nodes_is_counted_once_across_them(self):
        def node(digests, stored):
            return {"sample_covered": 1.0, "exact": True, "containers": [{
                "container": "pool", "stored_bytes": stored, "stored_extents": len(digests),
                "logical_bytes": stored, "shared_by_clone_bytes": 0, "sampled_bytes": stored,
                "sampled_extents": len(digests), "digests": digests}]}
        a = node([["aa", 100], ["bb", 100]], 200)
        b = node([["aa", 100], ["cc", 100]], 200)
        merged = self.ns["_merge_dedup"]([a, b])["containers"][0]
        self.assertEqual(merged["stored_bytes"], 400)
        self.assertEqual(merged["would_share_bytes"], 100, "the extent held on both nodes is one redundant copy")
        self.assertTrue(merged["merged_across_nodes"])

    def test_a_node_that_returned_no_digests_is_said_to_be_unmerged(self):
        a = {"sample_covered": 1.0, "exact": True, "containers": [{
            "container": "pool", "stored_bytes": 10, "stored_extents": 1, "logical_bytes": 10,
            "shared_by_clone_bytes": 0, "sampled_bytes": 10, "sampled_extents": 1}]}
        self.assertFalse(self.ns["_merge_dedup"]([a])["containers"][0]["merged_across_nodes"])

    def test_a_sampled_figure_is_scaled_by_the_smallest_fraction_any_node_covered(self):
        def node(covered):
            return {"sample_covered": covered, "exact": False, "containers": [{
                "container": "pool", "stored_bytes": 1000, "stored_extents": 10,
                "logical_bytes": 1000, "shared_by_clone_bytes": 0, "sampled_bytes": 200,
                "sampled_extents": 2, "digests": [["aa", 100], ["aa", 100]]}]}
        merged = self.ns["_merge_dedup"]([node(0.5), node(0.25)])
        self.assertEqual(merged["covered"], 0.25)
        # Four sampled copies of one extent: three redundant, 300 bytes, over 0.25 (stored is 2000).
        self.assertEqual(merged["containers"][0]["would_share_bytes"], 1200)


class TheDocumentsExistAndAreLinked(unittest.TestCase):
    def test_the_design_document_exists_and_is_linked_from_the_index(self):
        self.assertTrue(os.path.exists(os.path.join(HERE, "docs", "dfs", "compaction.md")))
        self.assertIn("compaction.md", read("docs", "dfs", "README.md"))
        self.assertIn("compaction.md", read("README.md"))

    def test_the_decision_is_recorded(self):
        decisions = read("docs", "dfs", "decisions.md")
        self.assertIn("**D-32", decisions)
        self.assertIn("compaction.md", decisions)

    def test_the_spark_api_table_lists_the_new_node_operations(self):
        api = read("docs", "spark_api.md")
        self.assertIn("`purah-compact`", api)
        self.assertIn("`purah-dedup`", api)


if __name__ == "__main__":
    unittest.main()
