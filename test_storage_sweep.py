#!/usr/bin/env python3
"""`valcli storage.sweep` and `storage.scrub`: how they are wired, and what they tell the operator.

The reclaimer itself is proved in Rust. What this pins is that an operator can reach it from a
command line without a hand-built curl, and that its answer explains itself: the sweep is slow
on purpose (a group goes only after two scans, a grace period apart, have found nothing pointing
at it), so the commonest honest answer is "nothing reclaimed, N waiting", and an output that
printed only a zero would be read as a broken command.

Run with:  python -m unittest test_storage_sweep
"""

import ast
import contextlib
import io
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def read(*parts):
    with io.open(os.path.join(HERE, *parts), encoding="utf-8") as handle:
        return handle.read()


FUNCTIONS = ("_sweep_mib", "_grace_phrase", "sweep_lines", "cmd_storage_sweep", "scrub_lines",
             "cmd_storage_scrub", "cmd_storage_cleanup_orphaned")


def load_valcli(**overrides):
    """Compile only the sweep functions out of valcli.py, which does real work at import."""
    tree = ast.parse(read("valcli.py"))
    body = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in FUNCTIONS]
    missing = set(FUNCTIONS) - {n.name for n in body}
    assert not missing, "valcli.py no longer defines %s" % sorted(missing)
    ns = {"sys": sys}
    ns.update(overrides)
    exec(compile(ast.Module(body=body, type_ignores=[]), "valcli.py", "exec"), ns)
    return ns


def run(ns, name, *args):
    out = io.StringIO()
    code = None
    with contextlib.redirect_stdout(out):
        try:
            ns[name](*args)
        except SystemExit as exit_:
            code = exit_.code
    return out.getvalue(), code


def cluster(answers):
    """A cluster of nodes whose spark API answers with `answers[ip]` for any request."""
    hosts = [{"ip": ip, "hostname": "n%d" % (i + 1)} for i, ip in enumerate(sorted(answers))]
    asked = []

    def spark(ip, path, request):
        asked.append((ip, path, dict(request)))
        value = answers[ip]
        if isinstance(value, Exception):
            return 1, None, str(value)
        return 0, value, ""

    return {"_storage_hosts": lambda: hosts, "run_mtls_spark_api": spark}, asked


IDLE = {"egroups_known": 10, "egroups_referenced": 10, "candidates": 0, "reclaimed": [],
        "bytes_reclaimed": 0, "skipped_open": 0, "skipped_held": 0, "skipped_young": 0,
        "skipped_awaiting_grace": 0, "missing": [], "missing_count": 0}


class TheOperatorCanReachThem(unittest.TestCase):
    def test_sidon_dispatches_both_operations(self):
        control = read("sidon", "src", "control.rs")
        for op in ("purah-sweep", "purah-scrub"):
            self.assertIn('"%s" =>' % op, control)

    def test_spark_forwards_them_as_node_operations(self):
        daemon = read("spark_daemon_decoded.py")
        node_ops = daemon[daemon.index("DFS_NODE_OPS"):daemon.index("DFS_OPS =")]
        for op in ("purah-sweep", "purah-scrub"):
            self.assertIn('"%s"' % op, node_ops)

    def test_the_python_client_has_a_function_for_each(self):
        client = read("helios_sidon.py")
        self.assertRegex(client, r'def sweep\(\*\*kw\):[\s\S]*?call\("purah-sweep"')
        self.assertRegex(client, r'def scrub\(\*\*kw\):[\s\S]*?call\("purah-scrub"')

    def test_valcli_dispatches_and_documents_both_commands(self):
        valcli = read("valcli.py")
        self.assertIn('elif cmd == "storage.sweep":', valcli)
        self.assertIn('elif cmd == "storage.scrub":', valcli)
        self.assertIn("valcli storage.sweep ", valcli)
        self.assertIn("valcli storage.scrub ", valcli)

    def test_the_old_name_is_the_same_command(self):
        """The daily Dagur job runs `storage.cleanup_orphaned`; it must do what `sweep` does."""
        ns, _ = cluster({"10.0.0.1": dict(IDLE)})
        env = load_valcli(**ns)
        out_old, _ = run(env, "cmd_storage_cleanup_orphaned")
        out_new, _ = run(env, "cmd_storage_sweep")
        self.assertEqual(out_old, out_new)

    def test_the_command_sends_the_sweep_op_to_every_node(self):
        ns, asked = cluster({"10.0.0.1": dict(IDLE), "10.0.0.2": dict(IDLE)})
        run(load_valcli(**ns), "cmd_storage_sweep")
        self.assertEqual([a[0] for a in asked], ["10.0.0.1", "10.0.0.2"])
        for _, path, request in asked:
            self.assertEqual(path, "/api/v1/dfs/vdisk")
            self.assertEqual(request, {"op": "purah-sweep"})


class TheSweepOutputExplainsItself(unittest.TestCase):
    def test_a_group_awaiting_its_second_scan_is_said_to_be_waiting_and_not_lost(self):
        body = dict(IDLE, skipped_awaiting_grace=4, grace_seconds=600)
        ns, _ = cluster({"10.0.0.1": body})
        out, _ = run(load_valcli(**ns), "cmd_storage_sweep")
        self.assertIn("awaiting a second scan: 4 group(s)", out)
        self.assertIn("4 group(s) are awaiting a second scan", out)
        self.assertIn("10 minute(s)", out)
        self.assertIn("Run this again", out)

    def test_nothing_waiting_says_so(self):
        ns, _ = cluster({"10.0.0.1": dict(IDLE)})
        out, _ = run(load_valcli(**ns), "cmd_storage_sweep")
        self.assertIn("Nothing is waiting for a second scan.", out)
        self.assertNotIn("awaiting a second scan:", out)

    def test_candidates_reclaimed_groups_and_bytes_are_all_shown(self):
        body = dict(IDLE, candidates=3, reclaimed=["a", "b", "c"], bytes_reclaimed=3 * 1024 * 1024)
        lines = load_valcli()["sweep_lines"]("n1", body)
        text = "\n".join(lines)
        self.assertIn("3 candidate(s)", text)
        self.assertIn("reclaimed 3 group(s), 3.0 MiB freed here", text)

    def test_what_kept_a_group_is_named(self):
        body = dict(IDLE, skipped_open=2, skipped_held=1, skipped_young=5)
        text = "\n".join(load_valcli()["sweep_lines"]("n1", body))
        self.assertIn("2 open", text)
        self.assertIn("1 held by a vdisk attached here", text)
        self.assertIn("5 too young", text)

    def test_a_group_whose_file_is_gone_is_a_warning_with_its_name(self):
        body = dict(IDLE, missing=["eg-x", "eg-y"], missing_count=2)
        text = "\n".join(load_valcli()["sweep_lines"]("n1", body))
        self.assertIn("WARNING: 2 referenced group(s) have no file", text)
        self.assertIn("eg-x", text)

    def test_an_older_daemons_answer_is_shown_without_inventing_fields(self):
        text = "\n".join(load_valcli()["sweep_lines"]("n1", dict(IDLE)))
        self.assertNotIn("replica", text)
        self.assertNotIn("abandoned", text)
        self.assertIn("the grace period", text)

    def test_replica_drops_and_orphans_are_reported_when_the_daemon_sends_them(self):
        body = dict(
            IDLE,
            replica_drops=[
                {"node": "n2", "dropped": 3, "bytes": 2 * 1024 * 1024, "absent": 1, "refused": 0},
                {"node": "n3", "unsupported": True},
                {"node": "n4", "error": "unreachable"},
            ],
            replica_orphans={"scanned": 9, "dropped": ["g1"], "bytes_dropped": 1048576,
                             "awaiting_grace": 2, "anomalies": ["g9 is referenced and has no row"]},
        )
        text = "\n".join(load_valcli()["sweep_lines"]("n1", body))
        self.assertIn("replica n2: dropped 3 copy(ies), 2.0 MiB", text)
        self.assertIn("replica n3: runs a sidon older than replica reclamation", text)
        self.assertIn("replica n4: could not be asked (unreachable)", text)
        self.assertIn("dropped 1 orphan(s), 1.0 MiB; 2 awaiting a second scan", text)
        self.assertIn("ANOMALY: g9", text)

    def test_an_anomaly_the_sweep_would_not_resolve_by_deleting_is_shown(self):
        body = dict(IDLE, anomalies=["extent group eg-x was reclaimed here but a replica reports ..."])
        text = "\n".join(load_valcli()["sweep_lines"]("n1", body))
        self.assertIn("ANOMALY: extent group eg-x", text)

    def test_one_node_failing_does_not_hide_the_others(self):
        ns, _ = cluster({"10.0.0.1": RuntimeError("tls handshake failed"), "10.0.0.2": dict(IDLE)})
        out, _ = run(load_valcli(**ns), "cmd_storage_sweep")
        self.assertIn("[n1] sweep failed: tls handshake failed", out)
        self.assertIn("n2: 10 extent group(s)", out)

    def test_no_node_answering_is_said_plainly(self):
        ns, _ = cluster({"10.0.0.1": RuntimeError("down")})
        out, _ = run(load_valcli(**ns), "cmd_storage_sweep")
        self.assertIn("No node answered", out)


class ScrubReportsDamageAndExitsNonZero(unittest.TestCase):
    CLEAN = {"checked": 7, "skipped_unsealed": 2, "missing": [], "mismatched": [], "clean": True}

    def test_a_clean_cluster_exits_zero(self):
        ns, asked = cluster({"10.0.0.1": dict(self.CLEAN)})
        out, code = run(load_valcli(**ns), "cmd_storage_scrub")
        self.assertIsNone(code)
        self.assertIn("7 sealed group(s) re-hashed", out)
        self.assertIn("clean", out)
        self.assertEqual(asked[0][2], {"op": "purah-scrub"})

    def test_damage_is_named_and_the_exit_code_says_so(self):
        body = dict(self.CLEAN, mismatched=["eg-bad"], clean=False)
        ns, _ = cluster({"10.0.0.1": body})
        out, code = run(load_valcli(**ns), "cmd_storage_scrub")
        self.assertEqual(code, 1)
        self.assertIn("DAMAGED: eg-bad", out)

    def test_a_missing_group_is_damage(self):
        body = dict(self.CLEAN, missing=["eg-gone"], clean=False)
        ns, _ = cluster({"10.0.0.1": body})
        out, code = run(load_valcli(**ns), "cmd_storage_scrub")
        self.assertEqual(code, 1)
        self.assertIn("MISSING: eg-gone", out)

    def test_no_node_answering_is_a_failure(self):
        ns, _ = cluster({"10.0.0.1": RuntimeError("down")})
        out, code = run(load_valcli(**ns), "cmd_storage_scrub")
        self.assertEqual(code, 1)


def rust(*parts):
    return read("sidon", "src", *parts)


def body(source, signature):
    """The text of one Rust function, from its signature to the closing brace at its indent."""
    start = source.index(signature)
    indent = len(source[:start].rsplit("\n", 1)[-1])
    end = re.search(r"\n%s\}" % (" " * indent), source[start + len(signature):])
    return source[start:start + len(signature) + (end.end() if end else len(source))]


class ReplicaReclamationKeepsItsRules(unittest.TestCase):
    """The behaviour is proved in Rust (`purah/reclaim/tests.rs`, `peer.rs`); these pin the shape
    that proof assumes, so that a refactor cannot move a step without a test noticing (D-33)."""

    def test_the_drop_opcode_is_new_and_unique(self):
        peer = rust("peer.rs")
        ops = re.findall(r"pub const (OP_[A-Z_]+): u16 = (\d+);", peer)
        numbers = [n for _, n in ops]
        self.assertEqual(len(numbers), len(set(numbers)), "two opcodes share a number: %s" % ops)
        self.assertIn(("OP_EGROUP_DROP", "11"), ops)
        # Every older opcode keeps its number: a replica from before must still read them.
        old = dict(ops)
        for name, number in (("OP_PING", "1"), ("OP_APPEND", "2"), ("OP_TRUNCATE_TO", "10")):
            self.assertEqual(old[name], number)

    def test_an_unknown_opcode_is_still_refused_not_answered(self):
        """The rolling-upgrade guarantee: an older replica refuses what it does not know."""
        serve = body(rust("peer.rs"), "pub fn serve_request(")
        self.assertIn("other => {", serve)
        self.assertIn("ST_REFUSED", serve[serve.index("other => {"):])
        self.assertNotIn("OP_EGROUP_DROP", serve, "the replica-only dispatch must not handle the drop")

    def test_a_transport_with_nothing_to_verify_refuses_the_drop(self):
        peer = rust("peer.rs")
        self.assertRegex(peer, r"fn drop_replica_groups\([^)]*\) -> Option<Result<serde_json::Value>> \{\s*None\s*\}")

    def test_a_replica_checks_the_row_the_sender_and_the_map_before_it_removes_anything(self):
        replica = rust("purah", "replica.rs")
        drop = body(replica, "pub fn drop_declared_dead<")
        removal = drop.index("store.remove_egroup")
        for needle in ('state != "dead"', "node != from", "referenced_egroups(db)", "referenced.contains(id)"):
            self.assertIn(needle, drop)
            self.assertLess(drop.index(needle), removal, "%s must come before the removal" % needle)

    def test_the_scan_needs_two_sightings_age_and_no_reference(self):
        scan = body(rust("purah", "replica.rs"), "pub fn scan<")
        removal = scan.index("store.remove_egroup")
        for needle in ("referenced.contains(&id)", "file.age < self.grace",
                       "saturating_duration_since(first) < self.grace"):
            self.assertIn(needle, scan)
            self.assertLess(scan.index(needle), removal)

    def test_the_owner_asks_while_the_row_is_dead_and_deletes_it_afterwards(self):
        sweep = body(rust("purah", "reclaim.rs"), "pub fn sweep_pass<")
        self.assertLess(sweep.index("mark_dead_and_remove_local(p"), sweep.index("push_drops("))
        self.assertLess(sweep.index("push_drops("), sweep.index("forget_group(p, &id)"))

    def test_the_mark_dead_comes_before_any_file_is_removed(self):
        mark = body(rust("purah", "reclaim.rs"), "fn mark_dead_and_remove_local<")
        self.assertLess(mark.index("/v1/dfs/egroup-state"), mark.index("remove_all"))

    def test_the_sweep_takes_the_attached_set_under_the_curators_lock(self):
        sweep = body(rust("control.rs"), "fn op_purah_sweep(")
        self.assertLess(sweep.index("purah_state.lock()"), sweep.index("self.held_egroups()"))

    def test_the_daemon_answers_the_drop_and_reports_the_orphan_scan(self):
        control = rust("control.rs")
        self.assertIn("fn drop_replica_groups(&self, from: &str, ids: &[String])", control)
        self.assertIn('report["replica_orphans"]', control)

    def test_the_decision_is_recorded_and_the_rolling_upgrade_is_documented(self):
        decisions = read("docs", "dfs", "decisions.md")
        self.assertIn("**D-33", decisions)
        entry = decisions[decisions.index("**D-33"):]
        self.assertIn("Rolling upgrade", entry)
        self.assertIn("unknown opcode", entry)


class StorageListIsUntouchedByThisChange(unittest.TestCase):
    def test_the_sweep_functions_do_not_call_into_storage_list(self):
        source = read("valcli.py")
        start = source.index("def sweep_lines")
        end = source.index("def format_size")
        self.assertIsNone(re.search(r"cmd_storage_list", source[start:end]))


if __name__ == "__main__":
    unittest.main()
