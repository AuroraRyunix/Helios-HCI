#!/usr/bin/env python3
"""A snapshot policy that deletes the wrong snapshot, or runs where nobody can see it, is
worse than no policy.

Snapshots existed as a command. Putting them on a timer adds two ways to lose data that a
command never had, and this file exists to hold both shut:

  * **Retention deleting something that mattered.** A policy prunes without a person
    watching, so every reason a snapshot must survive has to be a property of the code and
    not of the operator's care: a snapshot a clone was made from, one a person took by hand,
    one a node is serving, and -- the quiet one -- every snapshot there is, on a run that
    could not take a new one.
  * **A failure nobody sees.** A scheduled run has no terminal. The failure has to land in
    the task table and the job's exit status, and an *ordinary* skip (a stopped VM has
    nothing new to capture) must not be mistaken for one.

And rollback, which is the one operation here that destroys what a guest wrote: it must
refuse while anything could be reading the disk, and the refusal has to come from the
control plane as well as from Sidon, because Sidon can only see its own node.

Nothing here needs a cluster. The decisions are pure functions, and `Runner` is exercised
against a fake that speaks the same statements and the same Sidon operations.

Run with:  python -m unittest test_snapshot_policy
"""

import importlib.util
import io
import json
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name, alias):
    spec = importlib.util.spec_from_file_location(alias, os.path.join(HERE, name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read(*parts):
    with io.open(os.path.join(HERE, *parts), encoding="utf-8") as handle:
        return handle.read()


SNAP = load("helios_snapshots.py", "helios_snapshots_under_test")
SCHEMA = load("helios_schema.py", "helios_schema_under_snapshot_test")

HOUR_MS = 3600 * 1000
DAY_MS = 24 * HOUR_MS
NOW = 1_800_000_000_000  # a round-ish instant; nothing depends on it being real


def policy(scope=SNAP.SCOPE_CLUSTER, target="*", enabled=True, every_h=24, keep=3):
    return {"scope": scope, "target": target, "enabled": enabled,
            "interval_seconds": every_h * 3600, "keep_last": keep}


def snap(sid, age_h, origin=SNAP.ORIGIN_POLICY):
    return {"snapshot_id": sid, "created_at_ms": NOW - age_h * HOUR_MS, "origin": origin}


class WhichPolicyGovernsADisk(unittest.TestCase):
    """Narrowest wins, and a disabled narrow row is how one disk opts out."""

    def test_a_vdisk_policy_beats_a_container_policy_beats_the_cluster_default(self):
        rows = [policy(keep=1), policy("container", "pool", keep=2),
                policy("vdisk", "vm-disk0", keep=3)]
        self.assertEqual(SNAP.effective_policy(rows, "vm-disk0", "pool")["keep_last"], 3)
        self.assertEqual(SNAP.effective_policy(rows, "other-disk0", "pool")["keep_last"], 2)
        self.assertEqual(SNAP.effective_policy(rows, "other-disk0", "elsewhere")["keep_last"], 1)

    def test_a_disabled_narrow_policy_exempts_the_disk_instead_of_falling_through(self):
        # If a disabled row were skipped, the cluster default would reach the disk it was
        # written to exclude, and there would be no way to say "not this one".
        rows = [policy(keep=5), policy("vdisk", "scratch-disk0", enabled=False)]
        self.assertIsNone(SNAP.effective_policy(rows, "scratch-disk0", "pool"))
        self.assertIsNotNone(SNAP.effective_policy(rows, "vm-disk0", "pool"))

    def test_with_no_policy_nothing_is_snapshotted(self):
        # Opt-in: a snapshot pins extent groups, and history nobody asked for fills a store.
        self.assertIsNone(SNAP.effective_policy([], "vm-disk0", "pool"))


class APolicyThatCouldNotBeKeptIsRefusedWhenItIsWritten(unittest.TestCase):

    def test_an_interval_shorter_than_the_job_that_keeps_it_is_refused(self):
        with self.assertRaises(SNAP.PolicyError):
            SNAP.validate_policy("cluster", "*", 600, 3)

    def test_keeping_none_is_refused_because_it_would_delete_what_it_just_took(self):
        with self.assertRaises(SNAP.PolicyError):
            SNAP.validate_policy("cluster", "*", 86400, 0)

    def test_a_target_that_is_not_a_name_never_reaches_statement_text(self):
        # These statements are built by interpolation; the name check is the guard.
        with self.assertRaises(SNAP.PolicyError):
            SNAP.policy_insert_statement("vdisk", "x'; DROP TABLE hydra.vms; --", True, 86400, 3, NOW)
        with self.assertRaises(SNAP.PolicyError):
            SNAP.policy_delete_statement("container", "a b")

    def test_the_cluster_scope_has_one_fixed_target(self):
        statement = SNAP.policy_insert_statement("cluster", "anything", True, 86400, 3, NOW)
        self.assertIn("'cluster', '*'", statement)


class WhenASnapshotIsDue(unittest.TestCase):

    def test_a_disk_never_snapshotted_is_due(self):
        self.assertTrue(SNAP.is_due(policy(), None, NOW))

    def test_the_hourly_tick_does_not_miss_by_the_few_seconds_the_scheduler_drifts(self):
        # Without slack the newest snapshot is 3599.8s old when the next tick arrives and a
        # one-hour policy would snapshot every other run.
        p = policy(every_h=1)
        self.assertTrue(SNAP.is_due(p, NOW - HOUR_MS + 2000, NOW))
        self.assertFalse(SNAP.is_due(p, NOW - HOUR_MS // 2, NOW))

    def test_a_policy_is_not_satisfied_until_the_interval_has_passed(self):
        self.assertFalse(SNAP.is_due(policy(every_h=24), NOW - 23 * HOUR_MS, NOW))
        self.assertTrue(SNAP.is_due(policy(every_h=24), NOW - 24 * HOUR_MS, NOW))

    def test_two_runs_in_one_minute_compute_the_same_snapshot_name(self):
        # So the second is refused as already existing rather than creating a near-duplicate.
        self.assertEqual(SNAP.snapshot_name("vm-disk0", NOW), SNAP.snapshot_name("vm-disk0", NOW + 20000))

    def test_a_vdisk_id_too_long_to_name_a_snapshot_from_is_refused_not_truncated(self):
        with self.assertRaises(SNAP.PolicyError):
            SNAP.snapshot_name("v" * 60, NOW)


class WhatRetentionMayDelete(unittest.TestCase):
    """Every reason a snapshot survives is a property of plan_retention, not of care."""

    def test_the_newest_keep_last_policy_snapshots_survive_and_older_ones_go(self):
        snaps = [snap("s1", 1), snap("s2", 25), snap("s3", 49), snap("s4", 73)]
        got = SNAP.plan_retention(snaps, 2, referenced=set(), attached={})
        self.assertEqual(got.keep, ["s1", "s2"])
        self.assertEqual(got.prune, ["s3", "s4"])

    def test_a_snapshot_a_person_took_is_never_the_policys_to_delete(self):
        snaps = [snap("auto-new", 1), snap("by-hand", 500, SNAP.ORIGIN_MANUAL),
                 snap("before-rollback", 600, SNAP.ORIGIN_PRE_ROLLBACK)]
        got = SNAP.plan_retention(snaps, 1, referenced=set(), attached={})
        self.assertEqual(got.prune, [])

    def test_a_snapshot_a_clone_was_made_from_is_never_pruned(self):
        # The data would survive (Purah marks from every block-map row), but the clone
        # would lose the only record of where it came from.
        snaps = [snap("new", 1), snap("old", 100), snap("older", 200)]
        got = SNAP.plan_retention(snaps, 1, referenced={"old"}, attached={})
        self.assertEqual(got.prune, ["older"])
        self.assertIn("old", got.protected)
        self.assertIn("derived", got.protected["old"])

    def test_a_snapshot_a_node_is_serving_is_never_pruned(self):
        snaps = [snap("new", 1), snap("old", 100)]
        got = SNAP.plan_retention(snaps, 1, referenced=set(), attached={"old": "node-b (owner)"})
        self.assertEqual(got.prune, [])
        self.assertIn("node-b", got.protected["old"])

    def test_sparing_one_snapshot_does_not_cost_a_younger_one_its_place(self):
        # A pinned old snapshot is one extra, not one displaced: the window is the newest N
        # the policy took, and nothing cascades.
        snaps = [snap("n1", 1), snap("n2", 25), snap("pinned", 100), snap("old", 200)]
        got = SNAP.plan_retention(snaps, 2, referenced={"pinned"}, attached={})
        self.assertEqual(got.keep, ["n1", "n2"])
        self.assertEqual(got.prune, ["old"])

    def test_a_keep_of_zero_still_keeps_the_newest(self):
        got = SNAP.plan_retention([snap("only", 1)], 0, referenced=set(), attached={})
        self.assertEqual((got.keep, got.prune), (["only"], []))


class FakeCluster(object):
    """Speaks the statements the runner issues and the Sidon operations it calls."""

    def __init__(self):
        self.policies = []
        self.vdisks = {}      # id -> row
        self.index = []       # rows
        self.vms = {}         # name -> row
        self.attached = {"10.0.0.1": [], "10.0.0.2": []}   # ip -> [{vdisk_id, role}]
        self.down = set()
        self.statements = []
        self.calls = []       # (ip, payload)
        self.snapshot_error = None
        self.on_children_read = None
        self.clock = NOW
        self.seq = 0

    nodes = [{"hostname": "node-a", "ip": "10.0.0.1"}, {"hostname": "node-b", "ip": "10.0.0.2"}]

    def add_vdisk(self, vid, cls="rw", owner="node-a", container="pool", parent="", created=0):
        self.vdisks[vid] = {"vdisk_id": vid, "class": cls, "owner": owner, "epoch": 3,
                            "container": container, "parent_vdisk": parent,
                            "size_bytes": 1 << 30, "created_at_ms": created}

    def attach(self, vid, ip="10.0.0.1", role="owner"):
        self.attached[ip].append({"vdisk_id": vid, "role": role})

    # the Env ---------------------------------------------------------------------------
    def query(self, cql):
        self.statements.append(cql)
        out = []
        if "FROM hydra.dfs_snapshot_policies" in cql:
            out = self.policies
        elif "FROM hydra.dfs_vdisks" in cql:
            if self.on_children_read and "parent_vdisk FROM" in cql and "WHERE" not in cql:
                self.on_children_read(self)
            out = list(self.vdisks.values())
        elif cql.startswith("SELECT") and "FROM hydra.dfs_snapshot_index" in cql:
            wanted = re.search(r"vdisk_id = '([^']+)'", cql).group(1)
            out = [r for r in self.index if r["vdisk_id"] == wanted]
        elif "FROM hydra.vms" in cql:
            wanted = re.search(r"name = '([^']+)'", cql).group(1)
            out = [self.vms[wanted]] if wanted in self.vms else []
        elif cql.startswith("INSERT INTO hydra.dfs_snapshot_index"):
            m = re.search(r"VALUES \('([^']+)', (\d+), '([^']+)', '([^']+)'\)", cql)
            self.index.append({"vdisk_id": m.group(1), "created_at_ms": int(m.group(2)),
                               "snapshot_id": m.group(3), "origin": m.group(4)})
        elif cql.startswith("DELETE FROM hydra.dfs_snapshot_index"):
            sid = re.search(r"snapshot_id = '([^']+)'", cql).group(1)
            self.index = [r for r in self.index if r["snapshot_id"] != sid]
        return 0, "\n".join(json.dumps(r) for r in out), ""

    def dfs(self, ip, payload):
        self.calls.append((ip, dict(payload)))
        op = payload["op"]
        if ip in self.down:
            return -1, {}, "connection refused"
        if op == "list":
            return 0, {"attached": list(self.attached[ip])}, ""
        if op == "snapshot":
            if self.snapshot_error:
                return -1, {"error": self.snapshot_error}, self.snapshot_error
            self.add_vdisk(payload["child_id"], "immutable", "", parent=payload["vdisk_id"],
                           created=self.clock)
            return 0, {"vdisk_id": payload["child_id"]}, ""
        if op == "delete":
            self.vdisks.pop(payload["vdisk_id"], None)
            return 0, {"deleted": True}, ""
        if op == "rollback":
            return 0, {"epoch": 4, "previous_epoch": 3, "extents": 10,
                       "kept_as": payload.get("keep_as")}, ""
        return -1, {}, "unsupported"

    def env(self, parent=None):
        said = []
        env = SNAP.Env(self.query, self.dfs, nodes=lambda: self.nodes,
                       lwt=lambda endpoint, params: (True, True, {}, ""),
                       now_ms=lambda: self.clock, say=said.append, parent_task_id=parent)
        env.said = said
        return env

    def task_statements(self):
        return [s for s in self.statements if "hydra.catalyst_tasks" in s]


class ARunTakesWhatIsDue(unittest.TestCase):

    def setUp(self):
        self.c = FakeCluster()
        self.c.policies = [policy(keep=2, every_h=24)]
        self.c.add_vdisk("vm-disk0")
        self.c.attach("vm-disk0")

    def run_it(self, parent="11111111-2222-3333-4444-555555555555"):
        runner = SNAP.Runner(self.c.env(parent), SCHEMA)
        return runner.run(), runner

    def test_an_attached_disk_is_snapshotted_on_the_node_that_owns_it(self):
        summary, _ = self.run_it()
        self.assertEqual(len(summary.taken), 1)
        ip, payload = [c for c in self.c.calls if c[1]["op"] == "snapshot"][0]
        self.assertEqual(ip, "10.0.0.1")
        self.assertEqual(payload["vdisk_id"], "vm-disk0")

    def test_the_snapshot_is_indexed_as_the_policys_own(self):
        self.run_it()
        self.assertEqual([r["origin"] for r in self.c.index], [SNAP.ORIGIN_POLICY])

    def test_a_disk_nothing_is_writing_to_is_skipped_loudly_and_is_not_a_failure(self):
        self.c.attached["10.0.0.1"] = []
        summary, runner = self.run_it()
        self.assertTrue(summary.ok)
        self.assertEqual([v for v, _ in summary.skipped], ["vm-disk0"])
        self.assertTrue(any("skipped vm-disk0" in line for line in runner.env.said))

    def test_a_disk_still_inside_its_interval_is_left_alone(self):
        self.c.add_vdisk("vm-disk0-auto-recent", "immutable", "", parent="vm-disk0")
        self.c.index.append({"vdisk_id": "vm-disk0", "created_at_ms": NOW - HOUR_MS,
                             "snapshot_id": "vm-disk0-auto-recent", "origin": SNAP.ORIGIN_POLICY})
        summary, _ = self.run_it()
        self.assertEqual(summary.taken, [])

    def test_with_no_policy_the_run_reads_one_table_and_touches_nothing(self):
        self.c.policies = []
        summary, runner = self.run_it()
        self.assertTrue(summary.ok)
        self.assertEqual(self.c.calls, [])
        self.assertTrue(any("No snapshot policy" in line for line in runner.env.said))

    def test_images_and_snapshots_are_not_snapshotted(self):
        self.c.add_vdisk("img-ubuntu", "immutable", "")
        self.c.add_vdisk("clone-forming", "forming", "")
        self.run_it()
        targets = [p["vdisk_id"] for _ip, p in self.c.calls if p["op"] == "snapshot"]
        self.assertEqual(targets, ["vm-disk0"])


class AFailureIsVisibleAndStopsRetention(unittest.TestCase):

    def setUp(self):
        self.c = FakeCluster()
        self.c.policies = [policy(keep=1, every_h=24)]
        self.c.add_vdisk("vm-disk0")
        self.c.attach("vm-disk0")
        # Two old snapshots that retention would happily delete on a good run.
        for sid, age in (("vm-disk0-auto-a", 100), ("vm-disk0-auto-b", 200)):
            self.c.add_vdisk(sid, "immutable", "", parent="vm-disk0")
            self.c.index.append({"vdisk_id": "vm-disk0", "created_at_ms": NOW - age * HOUR_MS,
                                 "snapshot_id": sid, "origin": SNAP.ORIGIN_POLICY})

    def test_a_refused_snapshot_is_a_failed_task_and_a_failed_run(self):
        self.c.snapshot_error = "vdisk vm-disk0 is writable and not attached here"
        runner = SNAP.Runner(self.c.env("11111111-2222-3333-4444-555555555555"), SCHEMA)
        summary = runner.run()
        self.assertFalse(summary.ok)
        failed = [s for s in self.c.task_statements() if "'failed'" in s]
        self.assertEqual(len(failed), 1)
        self.assertIn("is writable", failed[0])

    def test_a_cluster_that_cannot_snapshot_does_not_also_shed_the_snapshots_it_has(self):
        self.c.snapshot_error = "no space"
        SNAP.Runner(self.c.env(), SCHEMA).run()
        self.assertEqual([p for _ip, p in self.c.calls if p["op"] == "delete"], [])
        self.assertIn("vm-disk0-auto-a", self.c.vdisks)

    def test_an_unreachable_node_stops_pruning_because_it_might_be_serving_a_snapshot(self):
        self.c.down.add("10.0.0.2")
        # Due, so a snapshot is taken -- but nothing is deleted.
        SNAP.Runner(self.c.env(), SCHEMA).run()
        self.assertEqual([p for _ip, p in self.c.calls if p["op"] == "delete"], [])

    def test_on_a_good_run_the_old_ones_are_pruned_and_unindexed(self):
        summary = SNAP.Runner(self.c.env(), SCHEMA).run()
        self.assertTrue(summary.ok)
        self.assertEqual(sorted(summary.pruned), ["vm-disk0-auto-a", "vm-disk0-auto-b"])
        self.assertNotIn("vm-disk0-auto-a", [r["snapshot_id"] for r in self.c.index])


class RetentionChecksForClonesAtTheLastMoment(unittest.TestCase):
    """The plan is made from a listing; a clone can be made after it and before the delete."""

    def test_a_clone_made_after_the_plan_spares_the_snapshot(self):
        c = FakeCluster()
        c.policies = [policy(keep=1, every_h=24)]
        c.add_vdisk("vm-disk0")
        c.attach("vm-disk0")
        c.add_vdisk("vm-disk0-auto-old", "immutable", "", parent="vm-disk0")
        c.index.append({"vdisk_id": "vm-disk0", "created_at_ms": NOW - 300 * HOUR_MS,
                        "snapshot_id": "vm-disk0-auto-old", "origin": SNAP.ORIGIN_POLICY})
        c.index.append({"vdisk_id": "vm-disk0", "created_at_ms": NOW - 200 * HOUR_MS,
                        "snapshot_id": "vm-disk0-auto-mid", "origin": SNAP.ORIGIN_POLICY})
        c.add_vdisk("vm-disk0-auto-mid", "immutable", "", parent="vm-disk0")
        calls = {"n": 0}

        def clone_appears(cluster):
            # The first scan is the run's own; the second is the pre-delete look.
            calls["n"] += 1
            if calls["n"] == 2:
                cluster.add_vdisk("restored-copy", "rw", "", parent="vm-disk0-auto-old")

        c.on_children_read = clone_appears
        summary = SNAP.Runner(c.env(), SCHEMA).run()
        self.assertIn("vm-disk0-auto-old", c.vdisks)
        self.assertIn("vm-disk0-auto-old", [s for s, _ in summary.spared])
        self.assertNotIn("vm-disk0-auto-old", summary.pruned)


class ARunIsATaskWithAParent(unittest.TestCase):

    def test_each_snapshot_is_a_child_of_the_dagur_task_that_ran_it(self):
        c = FakeCluster()
        c.policies = [policy()]
        c.add_vdisk("vm-disk0")
        c.attach("vm-disk0")
        parent = "11111111-2222-3333-4444-555555555555"
        SNAP.Runner(c.env(parent), SCHEMA).run()
        inserts = [s for s in c.task_statements() if s.startswith("INSERT")]
        self.assertEqual(len(inserts), 1)
        self.assertIn(parent, inserts[0])
        self.assertIn("'Rauru'", inserts[0])
        self.assertIn("'snapshot_policy'", inserts[0])
        # The same helper every other writer uses, so the row has the columns a reader needs.
        self.assertIn("parent_task_id", inserts[0])
        self.assertIn("sequence_id", inserts[0])

    def test_a_task_table_that_cannot_be_written_does_not_fail_a_good_snapshot(self):
        c = FakeCluster()
        c.policies = [policy()]
        c.add_vdisk("vm-disk0")
        c.attach("vm-disk0")
        real = c.query

        def query(cql):
            if "hydra.catalyst_tasks" in cql:
                return 1, "", "unavailable"
            return real(cql)

        env = c.env()
        env.query = query
        summary = SNAP.Runner(env, SCHEMA).run()
        self.assertTrue(summary.ok)
        self.assertEqual(len(summary.taken), 1)


class RollbackRefusals(unittest.TestCase):
    """The control plane's half of 'only while detached'."""

    def setUp(self):
        self.c = FakeCluster()
        self.c.add_vdisk("vm-disk0")
        self.c.add_vdisk("vm-disk0-s1", "immutable", "", parent="vm-disk0")
        self.c.vms["vm"] = {"name": "vm", "state": "Stopped", "status": ""}

    def roll(self, **kw):
        return SNAP.Runner(self.c.env(), SCHEMA).rollback("vm-disk0", "vm-disk0-s1", **kw)

    def rolled_back(self):
        return [p for _ip, p in self.c.calls if p["op"] == "rollback"]

    def test_a_stopped_vm_with_a_detached_disk_is_rolled_back_on_the_node_that_owned_it(self):
        self.c.vdisks["vm-disk0"]["owner"] = "node-b"
        body = self.roll()
        self.assertEqual(body["epoch"], 4)
        ip, payload = [c for c in self.c.calls if c[1]["op"] == "rollback"][0]
        self.assertEqual(ip, "10.0.0.2")
        self.assertTrue(payload["keep_as"].startswith("vm-disk0-pre-rollback-"))

    def test_a_running_vm_refuses_even_when_no_node_reports_the_disk(self):
        # The VM record is the control plane's own knowledge, and it is a different source
        # from any node's attach table: either alone can be stale.
        self.c.vms["vm"]["state"] = "Running"
        with self.assertRaises(SNAP.RollbackRefused) as ctx:
            self.roll()
        self.assertIn("stop the VM", str(ctx.exception))
        self.assertEqual(self.rolled_back(), [])

    def test_a_disk_any_node_is_serving_refuses_even_if_the_vm_says_stopped(self):
        self.c.attach("vm-disk0", "10.0.0.2", role="forwarding")
        with self.assertRaises(SNAP.RollbackRefused) as ctx:
            self.roll()
        self.assertIn("attached", str(ctx.exception))
        self.assertEqual(self.rolled_back(), [])

    def test_a_node_that_cannot_be_asked_is_a_refusal_not_a_yes(self):
        self.c.down.add("10.0.0.2")
        with self.assertRaises(SNAP.RollbackRefused):
            self.roll()
        self.assertEqual(self.rolled_back(), [])

    def test_a_snapshot_of_some_other_disk_is_refused_before_sidon_is_asked(self):
        self.c.add_vdisk("other-s1", "immutable", "", parent="other-disk0")
        with self.assertRaises(SNAP.RollbackRefused):
            SNAP.Runner(self.c.env(), SCHEMA).rollback("vm-disk0", "other-s1")
        self.assertEqual(self.rolled_back(), [])

    def test_the_copy_taken_before_the_rollback_is_indexed_so_retention_never_touches_it(self):
        self.roll()
        self.assertEqual([r["origin"] for r in self.c.index], [SNAP.ORIGIN_PRE_ROLLBACK])

    def test_a_rollback_sidon_refuses_is_reported_with_sidons_words_and_a_failed_task(self):
        real = self.c.dfs

        def dfs(ip, payload):
            if payload["op"] == "rollback":
                return -1, {"error": "replica node-c did not fence"}, "x"
            return real(ip, payload)

        env = self.c.env()
        env.dfs = dfs
        with self.assertRaises(RuntimeError) as ctx:
            SNAP.Runner(env, SCHEMA).rollback("vm-disk0", "vm-disk0-s1")
        self.assertIn("did not fence", str(ctx.exception))
        self.assertTrue(any("'failed'" in s for s in self.c.task_statements()))
        self.assertEqual(self.c.index, [])


class TheCommandLine(unittest.TestCase):
    """valcli is where an operator meets all of this."""

    @classmethod
    def setUpClass(cls):
        import sys
        if HERE not in sys.path:
            sys.path.insert(0, HERE)
        cls.valcli = load("valcli.py", "valcli_under_snapshot_test")

    def test_a_refusal_the_daemon_sends_as_an_http_409_is_an_error_not_a_success(self):
        # run_mtls_spark_api answers a 409 with rc 0 and the explanation in the body, so a
        # caller testing only rc reported "Snapshot created" for one Sidon had declined.
        valcli = self.valcli
        original = valcli.run_mtls_spark_api
        valcli.run_mtls_spark_api = lambda ip, path, payload: (
            0, {"error": "vdisk x is writable and not attached here", "kind": "refused"}, "")
        try:
            rc, body, err = valcli._dfs_call("10.0.0.1", {"op": "snapshot"})
        finally:
            valcli.run_mtls_spark_api = original
        self.assertNotEqual(rc, 0)
        self.assertIn("not attached here", err)

    def test_storage_snapshot_reports_a_declined_snapshot_as_declined(self):
        valcli = self.valcli
        saved = (valcli.run_cql_query, valcli.run_mtls_spark_api)
        valcli.run_cql_query = lambda cql, *a, **k: (
            0, json.dumps({"vdisk_id": "vm-disk0", "owner": "", "class": "rw"}), "")
        valcli.run_mtls_spark_api = lambda ip, path, payload: (0, {"error": "declined"}, "")
        out = io.StringIO()
        try:
            import contextlib
            with contextlib.redirect_stdout(out):
                with self.assertRaises(SystemExit):
                    valcli.cmd_storage_derive("vm-disk0", "snap", "snapshot")
        finally:
            valcli.run_cql_query, valcli.run_mtls_spark_api = saved
        self.assertIn("declined", out.getvalue())
        self.assertNotIn("created", out.getvalue())

    def test_policy_targets_are_parsed_from_the_three_spellings(self):
        parse = self.valcli._parse_policy_target
        self.assertEqual(parse("cluster"), ("cluster", "*"))
        self.assertEqual(parse("container:pool"), ("container", "pool"))
        self.assertEqual(parse("vdisk:vm-disk0"), ("vdisk", "vm-disk0"))
        self.assertEqual(parse("vdisk:"), (None, None))
        self.assertEqual(parse("rack:3"), (None, None))

    def test_the_commands_are_dispatched_and_listed_in_usage(self):
        source = read("valcli.py")
        for command in ("storage.snapshots", "storage.snapshot-policy",
                        "storage.snapshot-policy.set", "storage.snapshot-policy.delete",
                        "storage.snapshot-run", "storage.rollback"):
            self.assertIn('cmd == "%s"' % command, source, command)
            self.assertIn("valcli %s" % command, source, command)


class TheWiring(unittest.TestCase):
    """The ways this has been built and then not reachable before."""

    def test_the_two_migrations_come_after_the_existing_ones_in_id_order(self):
        ids = [m["id"] for m in SCHEMA.MIGRATIONS]
        self.assertEqual(ids, sorted(ids))
        self.assertIn("0022-snapshot-policies", ids)
        self.assertIn("0023-snapshot-index", ids)

    def test_they_use_no_alter_so_the_unsupported_if_not_exists_form_cannot_recur(self):
        by_id = dict((m["id"], m) for m in SCHEMA.MIGRATIONS)
        for name in ("0022-snapshot-policies", "0023-snapshot-index"):
            for statement in by_id[name]["statements"]:
                self.assertTrue(statement.upper().startswith("CREATE TABLE IF NOT EXISTS"), statement)
                self.assertNotIn("ALTER", statement.upper())

    def test_the_dagur_job_is_gone_and_existing_clusters_lose_it_so_the_policy_runs_once(self):
        """Rauru runs the policy now. A Dagur job that also runs it would take every snapshot
        twice a day's worth of runs, and a cluster that already has the seeded row keeps it
        unless something removes it -- seeding with IF NOT EXISTS never repairs a node."""
        source = read("spectrum_server.py")
        self.assertNotIn("VALUES ('snapshot_policy'", source)
        self.assertNotIn("insert_snapshot_policy", source)
        self.assertIn("DELETE FROM hydra.dagur_schedules WHERE job_name = 'snapshot_policy' ", source)
        self.assertIn("IF command = '/usr/local/bin/valcli storage.snapshot-run';", source)
        self.assertIn("run_conditional_cql_query(retire_snapshot_policy_job)", source)

    def test_rollback_is_reachable_through_spark_not_just_implemented_in_sidon(self):
        # A Sidon op the spark allow-list does not name is refused as unsupported, which is
        # how `capacity` once existed and could not be called.
        self.assertRegex(read("spark_daemon_decoded.py"), r'DFS_VDISK_OPS = \([^)]*"rollback"')
        self.assertIn('"rollback" => self.op_rollback(req)', read("sidon", "src", "control.rs"))
        self.assertIn("def rollback(", read("helios_sidon.py"))

    def test_attach_refuses_a_vdisk_whose_map_is_half_replaced(self):
        source = read("sidon", "src", "control.rs")
        self.assertIn("rollback::CLASS_ROLLING_BACK", source)

    def test_the_policy_never_asks_who_leads_zookeeper(self):
        # That question was removed from ten places for being the wrong one. Leadership here
        # is Dagur's dagur-queue candidacy, inherited by running as its job.
        source = read("helios_snapshots.py")
        for needle in ("leader_ip", "is_zookeeper_leader", "get_zookeeper_leader"):
            self.assertNotIn(needle, source)

    def test_the_new_module_is_in_every_list_that_ships_a_module(self):
        for name in ("sync_provision.py", "deploy_updates.py", "create_upgrade_zip.py",
                     "check_updates.py"):
            self.assertIn("helios_snapshots", read(name), name)


if __name__ == "__main__":
    unittest.main()
