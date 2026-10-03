#!/usr/bin/env python3
"""A protection domain that leaves a guest suspended, or calls a torn set whole, is worse than
the per-disk snapshots it replaces.

Snapshotting a VM's disks one at a time gives a restore whose disks never coexisted. A domain
fixes that by holding the guest still while every disk is snapshotted, and that is also what
makes it dangerous, so this file holds four properties shut:

  * **A guest is never left suspended.** Whatever fails between the pause and the resume -- a
    snapshot, the clock running out, the second VM refusing to pause -- the guest is resumed,
    and a guest that cannot be resumed is a loud failure with a recovery that finds it.
  * **A set is whole or it is not a set.** A member that cannot be captured fails the set and
    the snapshots already taken are deleted; retention deletes a set entirely or not at all.
  * **The consistency a set claims is the consistency it got.** A set taken without a barrier
    says so, and so does one whose members are not all held still.
  * **Nothing here deletes what something depends on**, the rule helios_snapshots established
    for one snapshot, applied to a set of them.

Nothing needs a cluster: the decisions are pure functions and `Runner` is run against a fake
that speaks the same statements, the same Sidon operations and the same power calls.

Run with:  python -m unittest test_rauru_protection
"""

import importlib.util
import io
import json
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import helios_snapshots as SNAP  # noqa: E402
import helios_schema as SCHEMA  # noqa: E402
import rauru_protection as RP  # noqa: E402


def read(*parts):
    with io.open(os.path.join(HERE, *parts), encoding="utf-8") as handle:
        return handle.read()


HOUR_MS = 3600 * 1000
NOW = 1_800_000_000_000


def parse_values(statement):
    """The literals of an INSERT's VALUES list, in order. Strict on purpose: a statement this
    cannot read is a statement Scylla would refuse, and the fake must not be kinder."""
    body = statement[statement.index("VALUES (") + len("VALUES ("):].rstrip().rstrip(";")
    assert body.endswith(")"), statement
    body = body[:-1]
    values, i = [], 0
    while i < len(body):
        ch = body[i]
        if ch in " ,":
            i += 1
        elif ch == "'":
            j, text = i + 1, []
            while True:
                if body[j] == "'":
                    if j + 1 < len(body) and body[j + 1] == "'":
                        text.append("'")
                        j += 2
                        continue
                    break
                text.append(body[j])
                j += 1
            values.append("".join(text))
            i = j + 1
        else:
            j = i
            while j < len(body) and body[j] not in ",":
                j += 1
            token = body[i:j].strip()
            values.append(None if token == "null" else token == "true" if token in ("true", "false")
                          else int(token))
            i = j
    return values


def columns(statement):
    match = re.search(r"INSERT INTO \S+ \(([^)]*)\)", statement)
    return [c.strip() for c in match.group(1).split(",")]


class FakeCluster(object):
    """Hydra, two Sidon nodes and two hypervisors, as far as the runner can tell."""

    nodes = [{"hostname": "node-a", "ip": "10.0.0.1"}, {"hostname": "node-b", "ip": "10.0.0.2"}]

    def __init__(self):
        self.clock = NOW
        self.domains = {}
        self.members = []          # {domain, kind, name}
        self.sets = []             # decoded-as-stored rows (members/paused_vms stay JSON text)
        self.vdisks = {}
        self.index = []
        self.vms = {}
        self.attached = {"10.0.0.1": [], "10.0.0.2": []}
        self.libvirt = {}          # vm -> "running" | "paused"
        self.down = set()
        self.log = []              # ordered ("suspend"|"resume"|"snapshot"|"delete", name)
        self.snapshot_ms = 1000    # how long one Sidon snapshot takes
        self.fail_snapshot = set()
        self.fail_power = {}       # (vm, action) -> remaining failures (None = forever)
        self.unreachable_sets_read = False
        self.statements = []

    # building the world
    def add_vm(self, name, disks=1, host="10.0.0.1", state="Running", status=""):
        self.vms[name] = {"name": name, "state": state, "status": status, "host_ip": host,
                          "disks_list": ",".join("10G:pool" for _ in range(disks))}
        self.libvirt[name] = "running" if state == "Running" else "shut off"
        for index in range(disks):
            vid = "%s-disk%d" % (name, index)
            self.add_vdisk(vid)
            if state == "Running":
                self.attach(vid, host)

    def add_vdisk(self, vid, cls="rw", owner="node-a", parent=""):
        self.vdisks[vid] = {"vdisk_id": vid, "class": cls, "owner": owner, "epoch": 3,
                            "container": "pool", "parent_vdisk": parent,
                            "size_bytes": 1 << 30, "created_at_ms": 0}

    def attach(self, vid, ip="10.0.0.1", role="owner"):
        self.attached[ip].append({"vdisk_id": vid, "role": role})

    def add_domain(self, name="web", members=(), enabled=True, every_h=24, keep=3,
                   quiesce=RP.QUIESCE_VM, max_pause=30):
        self.domains[name] = {"name": name, "enabled": enabled, "interval_seconds": every_h * 3600,
                              "keep_last": keep, "quiesce": quiesce, "max_pause_seconds": max_pause,
                              "created_at_ms": 0, "updated_at_ms": 0}
        for spec in members:
            kind, member = RP.parse_member(spec)
            self.members.append({"domain": name, "kind": kind, "name": member})

    def stored_sets(self, domain="web"):
        out = []
        for row in self.sets:
            if row["domain"] == domain:
                out.append(RP.normalise_set(row))
        return out

    # the Env
    def query(self, cql):
        self.statements.append(cql)
        out = []
        if cql.startswith("SELECT") and "FROM hydra.dfs_protection_domains" in cql:
            out = list(self.domains.values())
        elif cql.startswith("SELECT") and "FROM hydra.dfs_protection_domain_members" in cql:
            wanted = re.search(r"domain = '([^']+)'", cql)
            out = [m for m in self.members if not wanted or m["domain"] == wanted.group(1)]
        elif cql.startswith("SELECT") and "FROM hydra.dfs_protection_sets" in cql:
            wanted = re.search(r"domain = '([^']+)'", cql).group(1)
            out = [r for r in self.sets if r["domain"] == wanted]
        elif cql.startswith("INSERT INTO hydra.dfs_protection_sets"):
            row = dict(zip(columns(cql), parse_values(cql)))
            self.sets = [r for r in self.sets if not (
                r["domain"] == row["domain"] and r["taken_at_ms"] == row["taken_at_ms"]
                and r["set_id"] == row["set_id"])]
            self.sets.append(row)
        elif cql.startswith("DELETE FROM hydra.dfs_protection_sets"):
            sid = re.search(r"set_id = '([^']+)'", cql).group(1)
            taken = int(re.search(r"taken_at_ms = (\d+)", cql).group(1))
            self.sets = [r for r in self.sets if not (r["set_id"] == sid and r["taken_at_ms"] == taken)]
        elif cql.startswith("INSERT INTO hydra.dfs_protection_domains"):
            row = dict(zip(columns(cql), parse_values(cql)))
            self.domains[row["name"]] = row
        elif cql.startswith("INSERT INTO hydra.dfs_protection_domain_members"):
            row = dict(zip(columns(cql), parse_values(cql)))
            self.members.append({"domain": row["domain"], "kind": row["kind"], "name": row["name"]})
        elif cql.startswith("DELETE FROM hydra.dfs_protection_domain_members"):
            name = re.search(r"name = '([^']+)'", cql).group(1)
            self.members = [m for m in self.members if m["name"] != name]
        elif cql.startswith("DELETE FROM hydra.dfs_protection_domains"):
            self.domains.pop(re.search(r"name = '([^']+)'", cql).group(1), None)
        elif cql.startswith("SELECT") and "FROM hydra.dfs_snapshot_policies" in cql:
            out = []
        elif cql.startswith("SELECT") and "FROM hydra.dfs_vdisks" in cql:
            out = list(self.vdisks.values())
        elif cql.startswith("SELECT") and "FROM hydra.dfs_snapshot_index" in cql:
            wanted = re.search(r"vdisk_id = '([^']+)'", cql).group(1)
            out = [r for r in self.index if r["vdisk_id"] == wanted]
        elif cql.startswith("SELECT") and "FROM hydra.vms" in cql:
            wanted = re.search(r"name = '([^']+)'", cql)
            out = [r for r in self.vms.values() if not wanted or r["name"] == wanted.group(1)]
        elif cql.startswith("INSERT INTO hydra.dfs_snapshot_index"):
            row = dict(zip(columns(cql), parse_values(cql)))
            self.index.append(row)
        elif cql.startswith("DELETE FROM hydra.dfs_snapshot_index"):
            sid = re.search(r"snapshot_id = '([^']+)'", cql).group(1)
            self.index = [r for r in self.index if r["snapshot_id"] != sid]
        return 0, "\n".join(json.dumps(r) for r in out), ""

    def dfs(self, ip, payload):
        op = payload["op"]
        if ip in self.down:
            return -1, {}, "connection refused"
        if op == "list":
            return 0, {"attached": list(self.attached[ip])}, ""
        if op == "snapshot":
            vid = payload["vdisk_id"]
            if vid in self.fail_snapshot:
                return -1, {"error": "journal drain failed"}, "journal drain failed"
            self.log.append(("snapshot", vid))
            self.clock += self.snapshot_ms
            self.add_vdisk(payload["child_id"], "immutable", "", parent=vid)
            return 0, {"vdisk_id": payload["child_id"]}, ""
        if op == "delete":
            name = payload["vdisk_id"]
            if name not in self.vdisks:
                return -1, {"error": "vdisk %s does not exist" % name}, ""
            self.log.append(("delete", name))
            self.vdisks.pop(name)
            return 0, {"deleted": True}, ""
        if op == "rollback":
            self.log.append(("rollback", payload["vdisk_id"]))
            return 0, {"epoch": 4, "previous_epoch": 3, "extents": 10,
                       "kept_as": payload.get("keep_as")}, ""
        return -1, {}, "unsupported"

    def vm_power(self, host_ip, vm, action):
        key = (vm, action)
        if key in self.fail_power:
            left = self.fail_power[key]
            if left is None or left > 0:
                if left is not None:
                    self.fail_power[key] = left - 1
                return 0, {"state": self.libvirt[vm], "error": "virsh %s failed" % action}, ""
        self.log.append((action, vm))
        self.libvirt[vm] = "paused" if action == "suspend" else "running"
        return 0, {"state": self.libvirt[vm]}, ""

    def env(self, parent=None):
        said = []
        env = RP.Env(self.query, self.dfs, self.vm_power, nodes=lambda: self.nodes,
                     lwt=lambda endpoint, params: (True, True, {}, ""),
                     now_ms=lambda: self.clock, say=said.append, parent_task_id=parent)
        env.said = said
        return env

    def runner(self, dry_run=False):
        return RP.Runner(self.env(), SCHEMA, dry_run=dry_run)

    def suspended(self):
        return sorted(v for v, s in self.libvirt.items() if s == "paused")

    def order(self):
        return [f"{a}:{b}" for a, b in self.log]


def web_cluster(quiesce=RP.QUIESCE_VM, **kw):
    c = FakeCluster()
    c.add_vm("web", disks=2)
    c.add_vm("db", disks=2)
    c.add_domain("web", ["vm:web", "vm:db"], quiesce=quiesce, **kw)
    return c


class WhatIsPausedAndWhatConsistencyThatBuys(unittest.TestCase):
    """The plan decides the barrier, and the consistency a set may claim follows from it."""

    def plan(self, c, quiesce, members=None):
        members = members or [(m["kind"], m["name"]) for m in c.members]
        runner = c.runner()
        owners, _where, unreachable = runner.owners_and_unreachable()
        return RP.plan_set(members, runner.vms(), runner.vdisks(), owners, unreachable,
                           quiesce, NOW)

    def test_per_vm_quiesce_holds_each_vm_for_its_own_disks_and_claims_only_that(self):
        plan = self.plan(web_cluster(), RP.QUIESCE_VM)
        self.assertEqual([[t["vdisk"] for t in g.targets] for g in plan.groups],
                         [["db-disk0", "db-disk1"], ["web-disk0", "web-disk1"]])
        self.assertEqual([[p["name"] for p in g.pause] for g in plan.groups], [["db"], ["web"]])
        # Two groups means two instants: honest consistency is per VM, not per domain.
        self.assertEqual(plan.consistency, RP.CONSISTENCY_VM)

    def test_domain_quiesce_holds_every_vm_at_once_and_shares_one_cut(self):
        plan = self.plan(web_cluster(), RP.QUIESCE_DOMAIN)
        self.assertEqual(len(plan.groups), 1)
        self.assertEqual(sorted(p["name"] for p in plan.groups[0].pause), ["db", "web"])
        self.assertEqual(plan.consistency, RP.CONSISTENCY_DOMAIN)

    def test_no_quiesce_pauses_nothing_and_does_not_claim_a_point_in_time(self):
        plan = self.plan(web_cluster(), RP.QUIESCE_NONE)
        self.assertEqual([g.pause for g in plan.groups], [[]])
        self.assertEqual(plan.consistency, RP.CONSISTENCY_NONE)

    def test_a_single_disk_vm_needs_no_barrier_because_one_snapshot_is_one_instant(self):
        c = FakeCluster()
        c.add_vm("solo", disks=1)
        c.add_domain("d", ["vm:solo"])
        plan = self.plan(c, RP.QUIESCE_VM)
        self.assertEqual(plan.groups[0].pause, [])
        self.assertEqual(plan.consistency, RP.CONSISTENCY_DOMAIN)

    def test_a_bare_vdisk_in_a_domain_barrier_is_not_held_still_and_the_set_says_so(self):
        # Nothing says which VM writes a bare vdisk, so the pause cannot cover it. Claiming
        # the domain's cut for it would be a lie in exactly the field meant to prevent one.
        c = web_cluster()
        c.add_vdisk("scratch")
        c.attach("scratch")
        plan = self.plan(c, RP.QUIESCE_DOMAIN,
                         [("vm", "web"), ("vm", "db"), ("vdisk", "scratch")])
        self.assertEqual(plan.consistency, RP.CONSISTENCY_NONE)

    def test_a_vm_added_a_disk_later_is_in_the_next_set_without_editing_membership(self):
        c = web_cluster()
        c.vms["web"]["disks_list"] += ",10G:pool"
        c.add_vdisk("web-disk2")
        c.attach("web-disk2")
        plan = self.plan(c, RP.QUIESCE_VM)
        self.assertIn("web-disk2", [t["vdisk"] for t in plan.targets])


class WhatCannotBeCapturedAndWhatIsMerelyNotRunning(unittest.TestCase):

    def plan(self, c, quiesce=RP.QUIESCE_VM):
        return WhatIsPausedAndWhatConsistencyThatBuys.plan(self, c, quiesce)

    def test_a_stopped_vm_is_skipped_and_is_not_a_failure(self):
        c = web_cluster()
        c.vms["db"]["state"] = "Stopped"
        for vid in ("db-disk0", "db-disk1"):
            c.attached["10.0.0.1"] = [a for a in c.attached["10.0.0.1"] if a["vdisk_id"] != vid]
        plan = self.plan(c)
        self.assertEqual(plan.failures, [])
        self.assertEqual([label for label, _ in plan.skipped], ["vm:db"])
        self.assertEqual([t["vm"] for t in plan.targets], ["web", "web"])

    def test_a_running_vm_with_a_disk_nobody_serves_fails_the_set_instead_of_omitting_the_disk(self):
        # Capturing the other disk would produce a set that silently lacks one: the very
        # inconsistency a domain exists to prevent.
        c = web_cluster()
        c.attached["10.0.0.1"] = [a for a in c.attached["10.0.0.1"] if a["vdisk_id"] != "db-disk1"]
        plan = self.plan(c)
        self.assertEqual([label for label, _ in plan.failures], ["vm:db"])
        self.assertIn("db-disk1", plan.failures[0][1])

    def test_a_vm_mid_migration_is_never_paused(self):
        c = web_cluster()
        c.vms["web"]["status"] = "migrating"
        plan = self.plan(c)
        self.assertEqual([label for label, _ in plan.failures], ["vm:web"])

    def test_a_node_that_did_not_answer_is_named_when_it_could_hold_the_disk(self):
        c = web_cluster()
        c.attached["10.0.0.1"] = []
        c.down.add("10.0.0.2")
        plan = self.plan(c)
        self.assertTrue(any("node-b" in reason for _l, reason in plan.failures))

    def test_a_member_that_names_nothing_is_skipped_loudly(self):
        c = web_cluster()
        c.add_domain("ghost", ["vm:gone"])
        plan = WhatIsPausedAndWhatConsistencyThatBuys.plan(self, c, RP.QUIESCE_VM,
                                                           [("vm", "gone")])
        self.assertEqual(plan.skipped[0], ("vm:gone", "no such VM"))
        self.assertEqual(plan.failures, [])


class AGuestIsNeverLeftSuspended(unittest.TestCase):
    """The property the whole feature can be judged by."""

    def take(self, c, origin=RP.SET_ORIGIN_MANUAL):
        runner = c.runner()
        summary = RP.Summary()
        ok = runner.take_set(summary, c.domains["web"], origin)
        return ok, summary

    def test_the_barrier_brackets_the_snapshots_in_order(self):
        c = web_cluster(RP.QUIESCE_DOMAIN)
        ok, _ = self.take(c)
        self.assertTrue(ok)
        order = c.order()
        suspends = [i for i, x in enumerate(order) if x.startswith("suspend")]
        snaps = [i for i, x in enumerate(order) if x.startswith("snapshot")]
        resumes = [i for i, x in enumerate(order) if x.startswith("resume")]
        self.assertEqual((len(suspends), len(snaps), len(resumes)), (2, 4, 2))
        self.assertLess(max(suspends), min(snaps))
        self.assertLess(max(snaps), min(resumes))
        self.assertEqual(c.suspended(), [])

    def test_a_failed_snapshot_resumes_the_guest_and_deletes_what_was_taken(self):
        c = web_cluster(RP.QUIESCE_DOMAIN)
        c.fail_snapshot.add("web-disk1")
        ok, summary = self.take(c)
        self.assertFalse(ok)
        self.assertEqual(c.suspended(), [])
        # Whole or not at all: the member snapshots taken before the failure are gone.
        self.assertEqual([v for v in c.vdisks if "-dom-" in v], [])
        row = c.stored_sets()[0]
        self.assertEqual(row["state"], RP.SET_FAILED)
        self.assertIn("web-disk1", row["error"])
        self.assertEqual([r for r in c.index if r["origin"] == RP.ORIGIN_DOMAIN], [])

    def test_a_second_vm_that_will_not_pause_resumes_the_first(self):
        c = web_cluster(RP.QUIESCE_DOMAIN)
        c.fail_power[("web", "suspend")] = None
        ok, summary = self.take(c)
        self.assertFalse(ok)
        self.assertEqual(c.suspended(), [])
        # No snapshot was attempted: capturing an unpaused VM would downgrade the set silently.
        self.assertFalse([x for x in c.order() if x.startswith("snapshot")])

    def test_running_out_of_pause_budget_abandons_the_set_and_resumes_at_once(self):
        c = web_cluster(RP.QUIESCE_DOMAIN, max_pause=3)
        c.snapshot_ms = 2000     # four snapshots at two seconds against a three second limit
        ok, summary = self.take(c)
        self.assertFalse(ok)
        self.assertEqual(c.suspended(), [])
        self.assertIn("limit", summary.failures[0][1])
        self.assertEqual([v for v in c.vdisks if "-dom-" in v], [])

    def test_a_guest_that_will_not_resume_is_a_loud_failure_that_names_it(self):
        c = web_cluster(RP.QUIESCE_VM)
        c.fail_power[("db", "resume")] = None
        ok, summary = self.take(c)
        self.assertFalse(ok)
        self.assertEqual(c.suspended(), ["db"])
        self.assertIn("STRANDED", summary.failures[0][1])
        self.assertIn("db", summary.failures[0][1])

    def test_a_resume_that_works_on_a_later_attempt_is_not_a_failure(self):
        c = web_cluster(RP.QUIESCE_VM)
        c.fail_power[("db", "resume")] = 2
        ok, _ = self.take(c)
        self.assertTrue(ok)
        self.assertEqual(c.suspended(), [])

    def test_a_plan_that_cannot_work_pauses_nothing_at_all(self):
        c = web_cluster()
        c.attached["10.0.0.1"] = [a for a in c.attached["10.0.0.1"] if a["vdisk_id"] != "db-disk1"]
        ok, summary = self.take(c)
        self.assertFalse(ok)
        self.assertEqual([x for x in c.order() if x.split(":")[0] in ("suspend", "snapshot")], [])
        # The attempt is on record, so `sets` can say why there is no set.
        self.assertEqual(c.stored_sets()[0]["state"], RP.SET_FAILED)

    def test_the_run_is_recorded_before_the_first_pause_so_a_dead_run_can_be_found(self):
        c = web_cluster(RP.QUIESCE_DOMAIN)
        seen = []
        original = c.vm_power

        def watching(host, vm, action):
            if action == "suspend":
                taking = [s for s in c.stored_sets() if s["state"] == RP.SET_TAKING]
                seen.append((vm, [p["name"] for s in taking for p in s["paused_vms"]]))
            return original(host, vm, action)
        c.vm_power = watching
        self.take(c)
        self.assertTrue(seen)
        for _vm, recorded in seen:
            self.assertEqual(sorted(recorded), ["db", "web"])

    def test_two_runs_in_one_minute_pause_the_guests_once(self):
        c = web_cluster(RP.QUIESCE_DOMAIN)
        self.take(c)
        before = len([x for x in c.order() if x.startswith("suspend")])
        ok, summary = self.take(c)
        self.assertTrue(ok)
        self.assertEqual(len([x for x in c.order() if x.startswith("suspend")]), before)


class ASetSaysWhatItGot(unittest.TestCase):

    def test_a_complete_set_records_members_cut_spread_and_how_long_guests_were_held(self):
        c = web_cluster(RP.QUIESCE_DOMAIN)
        c.snapshot_ms = 500
        runner = c.runner()
        entry = runner.snapshot_domain("web")
        self.assertEqual(entry["state"], RP.SET_COMPLETE)
        self.assertEqual(entry["consistency"], RP.CONSISTENCY_DOMAIN)
        self.assertEqual(sorted(m["vdisk"] for m in entry["members"]),
                         ["db-disk0", "db-disk1", "web-disk0", "web-disk1"])
        self.assertTrue(all(m["status"] == "taken" for m in entry["members"]))
        # Four snapshots of 500 ms: the cut spans the first start to the last end.
        self.assertEqual(entry["cut_end_ms"] - entry["cut_start_ms"], 2000)
        self.assertEqual(entry["paused_ms"], 2000)
        self.assertEqual(entry["paused_vms"], [])

    def test_a_set_taken_without_a_barrier_is_labelled_so_in_the_row_the_operator_reads(self):
        c = web_cluster(RP.QUIESCE_NONE)
        entry = c.runner().snapshot_domain("web")
        self.assertEqual(entry["consistency"], RP.CONSISTENCY_NONE)
        self.assertEqual(entry["paused_ms"], 0)

    def test_member_snapshots_are_indexed_as_the_domains_not_the_policys(self):
        # Per-vdisk retention deletes only `policy` rows, so a domain's snapshots can never be
        # pruned out from under a set by a vdisk policy.
        c = web_cluster()
        c.runner().snapshot_domain("web")
        self.assertEqual(set(r["origin"] for r in c.index), {RP.ORIGIN_DOMAIN})
        self.assertEqual(len(c.index), 4)
        self.assertNotIn(SNAP.ORIGIN_POLICY, set(r["origin"] for r in c.index))

    def test_the_statements_written_name_every_column_with_one_value(self):
        # The fake parses VALUES strictly, so this fails if a builder's columns and values
        # ever disagree -- the kind of mistake Scylla reports only when the statement runs.
        statement = RP.set_row_statement({
            "domain": "web", "taken_at_ms": NOW, "set_id": "web-x", "origin": "policy",
            "state": "complete", "consistency": "crash:vm", "quiesce": "vm",
            "started_at_ms": NOW, "members": [{"snapshot": "it's"}], "paused_vms": []})
        self.assertEqual(len(columns(statement)), len(parse_values(statement)))
        self.assertEqual(json.loads(parse_values(statement)[12])[0]["snapshot"], "it's")

    def test_a_domain_with_nothing_running_takes_no_set_and_says_why(self):
        c = FakeCluster()
        c.add_vm("db", disks=2, state="Stopped")
        c.add_domain("web", ["vm:db"])
        runner = c.runner()
        summary = RP.Summary()
        self.assertTrue(runner.take_set(summary, c.domains["web"], RP.SET_ORIGIN_POLICY))
        self.assertEqual(summary.taken, [])
        self.assertEqual(c.stored_sets(), [])
        self.assertIn("Stopped", summary.skipped[0][1])
        with self.assertRaises(RP.DomainRefused):
            c.runner().snapshot_domain("web")


class WhenASetIsDue(unittest.TestCase):

    def test_a_failed_set_does_not_satisfy_the_schedule(self):
        c = web_cluster(RP.QUIESCE_DOMAIN, every_h=24)
        c.sets.append({"domain": "web", "taken_at_ms": NOW - HOUR_MS, "set_id": "web-old",
                       "origin": "policy", "state": "failed", "members": "[]", "paused_vms": "[]"})
        c.attach("web-disk0", "10.0.0.1")
        summary = c.runner().run()
        self.assertEqual(len(summary.taken), 1)

    def test_a_manual_set_does_not_stand_in_for_the_scheduled_one(self):
        c = web_cluster(RP.QUIESCE_DOMAIN, every_h=24)
        c.runner().snapshot_domain("web")
        c.clock += 2 * HOUR_MS
        summary = c.runner().run()
        self.assertEqual(len(summary.taken), 1)

    def test_a_disabled_domain_is_left_alone(self):
        c = web_cluster(RP.QUIESCE_DOMAIN)
        c.domains["web"]["enabled"] = False
        summary = c.runner().run()
        self.assertEqual((summary.taken, c.order()), ([], []))

    def test_a_run_in_the_interval_takes_nothing(self):
        c = web_cluster(RP.QUIESCE_DOMAIN, every_h=24)
        c.runner().run()
        c.clock += 2 * HOUR_MS
        before = len(c.log)
        summary = c.runner().run()
        self.assertEqual(summary.taken, [])
        self.assertEqual(len(c.log), before)


def rset(sid, age_h, snaps, origin=RP.SET_ORIGIN_POLICY, state=RP.SET_COMPLETE):
    return {"set_id": sid, "taken_at_ms": NOW - age_h * HOUR_MS, "origin": origin, "state": state,
            "members": [{"vdisk": s.split("-dom-")[0], "snapshot": s, "status": "taken"}
                        for s in snaps]}


class WhatRetentionMayDeleteOfASet(unittest.TestCase):
    """Every reason a snapshot survives, applied to a unit of several."""

    def plan(self, sets, keep=1, referenced=(), attached=None, pinned=None):
        return RP.plan_set_retention(sets, keep, set(referenced), attached or {}, pinned)

    def test_the_newest_complete_policy_sets_are_kept_and_older_ones_go(self):
        got = self.plan([rset("s1", 1, ["a-dom-1"]), rset("s2", 25, ["a-dom-2"]),
                         rset("s3", 49, ["a-dom-3"])], keep=1)
        self.assertEqual((got.keep, got.prune), (["s1"], ["s2", "s3"]))

    def test_one_member_with_a_clone_spares_the_whole_set(self):
        # Pruning half a set leaves something that cannot be restored and looks complete.
        sets = [rset("s1", 1, ["a-dom-1", "b-dom-1"]),
                rset("s2", 25, ["a-dom-2", "b-dom-2"])]
        got = self.plan(sets, keep=1, referenced={"b-dom-2"})
        self.assertEqual(got.prune, [])
        self.assertIn("b-dom-2", got.protected["s2"])

    def test_a_member_a_node_is_serving_spares_the_whole_set(self):
        sets = [rset("s1", 1, ["a-dom-1"]), rset("s2", 25, ["a-dom-2", "b-dom-2"])]
        got = self.plan(sets, keep=1, attached={"a-dom-2": "node-a (reader)"})
        self.assertEqual(got.prune, [])

    def test_a_set_something_has_pinned_is_never_deleted(self):
        # The hook replication uses: a set not yet delivered to a remote site is the only
        # copy of that backup, and deleting it to save space would be deleting the backup.
        sets = [rset("s1", 1, ["a-dom-1"]), rset("s2", 25, ["a-dom-2"])]
        got = self.plan(sets, keep=1, pinned={"s2": "not yet replicated to site-b"})
        self.assertEqual(got.prune, [])
        self.assertEqual(got.protected["s2"], "not yet replicated to site-b")

    def test_manual_failed_and_taking_sets_are_never_the_policys_to_delete(self):
        sets = [rset("s1", 1, ["a-dom-1"]),
                rset("m", 99, ["a-dom-9"], origin=RP.SET_ORIGIN_MANUAL),
                rset("f", 99, [], state=RP.SET_FAILED),
                rset("t", 99, ["a-dom-8"], state=RP.SET_TAKING)]
        got = self.plan(sets, keep=1)
        self.assertEqual(got.prune, [])

    def test_a_protected_old_set_does_not_displace_a_recent_one(self):
        sets = [rset("s1", 1, ["a-dom-1"]), rset("s2", 25, ["a-dom-2"]),
                rset("s3", 49, ["a-dom-3"])]
        got = self.plan(sets, keep=1, referenced={"a-dom-2"})
        self.assertEqual((got.keep, got.prune), (["s1"], ["s3"]))


class RetentionAsRun(unittest.TestCase):

    def cluster_with_sets(self, n=3):
        c = web_cluster(RP.QUIESCE_DOMAIN, every_h=24, keep=1)
        for _ in range(n):
            c.runner().take_set(RP.Summary(), c.domains["web"], RP.SET_ORIGIN_POLICY)
            c.clock += 25 * HOUR_MS
        return c

    def test_a_run_prunes_old_sets_whole_and_leaves_no_orphaned_member(self):
        c = self.cluster_with_sets(3)
        c.runner().run()
        sets = [s for s in c.stored_sets() if s["state"] == RP.SET_COMPLETE]
        # One kept from before the run plus the one this run took: keep=1 applies after.
        self.assertEqual(len(sets), 1)
        live = set(m["snapshot"] for s in sets for m in s["members"])
        existing = set(v for v in c.vdisks if "-dom-" in v)
        self.assertEqual(existing, live)
        self.assertEqual(set(r["snapshot_id"] for r in c.index), live)

    def test_a_clone_made_after_the_plan_stops_the_delete_before_any_member_goes(self):
        c = self.cluster_with_sets(2)
        runner = c.runner()
        oldest = sorted(c.stored_sets(), key=lambda s: s["taken_at_ms"])[0]
        target = oldest["members"][0]["snapshot"]
        c.add_vdisk("restored-clone", parent=target)
        before = set(c.vdisks)
        with self.assertRaises(RP.DomainRefused):
            runner.delete_set("web", oldest["set_id"])
        self.assertEqual(set(c.vdisks), before)
        self.assertTrue(any(s["set_id"] == oldest["set_id"] for s in c.stored_sets()))

    def test_a_run_that_could_not_take_its_own_set_sheds_nothing(self):
        c = self.cluster_with_sets(3)
        before = set(c.vdisks)
        c.fail_snapshot.add("web-disk0")
        summary = c.runner().run()
        self.assertFalse(summary.ok)
        self.assertEqual(summary.pruned, [])
        self.assertTrue(before <= set(c.vdisks))

    def test_a_node_that_does_not_answer_stops_pruning_because_it_may_hold_a_member(self):
        c = self.cluster_with_sets(3)
        c.down.add("10.0.0.2")
        summary = c.runner().run()
        self.assertEqual(summary.pruned, [])

    def test_a_failed_sets_leftovers_are_retried_so_it_cannot_pin_extents_forever(self):
        c = web_cluster(RP.QUIESCE_DOMAIN, every_h=24, keep=2)
        c.add_vdisk("web-disk0-dom-leftover", "immutable", "", parent="web-disk0")
        c.sets.append({
            "domain": "web", "taken_at_ms": NOW - HOUR_MS, "set_id": "web-failed",
            "origin": "policy", "state": "failed", "paused_vms": "[]",
            "members": json.dumps([{"vm": "web", "vdisk": "web-disk0",
                                    "snapshot": "web-disk0-dom-leftover", "status": "taken"}])})
        c.runner().run()
        self.assertNotIn("web-disk0-dom-leftover", c.vdisks)

    def test_old_failed_rows_are_dropped_but_the_recent_ones_stay_to_explain_themselves(self):
        c = web_cluster(RP.QUIESCE_DOMAIN, every_h=24, keep=2)
        for i in range(RP.FAILED_ROWS_KEPT + 3):
            c.sets.append({"domain": "web", "taken_at_ms": NOW - (i + 1) * HOUR_MS,
                           "set_id": "web-f%d" % i, "origin": "policy", "state": "failed",
                           "paused_vms": "[]", "members": "[]"})
        c.runner().run()
        failed = [s for s in c.stored_sets() if s["state"] == RP.SET_FAILED]
        self.assertEqual(len(failed), RP.FAILED_ROWS_KEPT)


class ARunThatDiesIsRecovered(unittest.TestCase):

    def dead_run(self, c, age_ms):
        """A `taking` row whose run is gone, with `web` still suspended."""
        c.libvirt["web"] = "paused"
        c.add_vdisk("web-disk0-dom-half", "immutable", "", parent="web-disk0")
        c.sets.append({
            "domain": "web", "taken_at_ms": c.clock - age_ms, "set_id": "web-dead",
            "origin": "policy", "state": "taking", "started_at_ms": c.clock - age_ms,
            "cut_start_ms": c.clock - age_ms,
            "paused_vms": json.dumps([{"name": "web", "host_ip": "10.0.0.1"}]),
            "members": json.dumps([{"vm": "web", "vdisk": "web-disk0",
                                    "snapshot": "web-disk0-dom-half", "status": "taken"}])})

    def test_a_guest_a_dead_run_left_suspended_is_resumed_and_its_snapshots_removed(self):
        c = web_cluster()
        self.dead_run(c, RP.STALE_TAKING_MS + 1000)
        summary = c.runner().recover()
        self.assertEqual(summary.resumed, ["web"])
        self.assertEqual(c.suspended(), [])
        self.assertNotIn("web-disk0-dom-half", c.vdisks)
        self.assertEqual(c.stored_sets()[0]["state"], RP.SET_FAILED)

    def test_a_run_still_inside_its_budget_is_not_mistaken_for_a_dead_one(self):
        # Resuming a guest a live set is holding would turn that set into one taken from a
        # guest that was not held still, and nothing would say so.
        c = web_cluster()
        self.dead_run(c, 60 * 1000)
        summary = c.runner().recover()
        self.assertEqual(summary.resumed, [])
        self.assertEqual(c.suspended(), ["web"])

    def test_a_caller_that_knows_no_run_is_in_flight_can_force_the_recovery(self):
        c = web_cluster()
        self.dead_run(c, 60 * 1000)
        c.runner().recover(force=True)
        self.assertEqual(c.suspended(), [])

    def test_every_scheduled_run_begins_by_looking_for_stranded_guests(self):
        c = web_cluster()
        self.dead_run(c, RP.STALE_TAKING_MS + 1000)
        c.runner().run()
        self.assertEqual(c.suspended(), [])

    def test_a_guest_that_still_will_not_resume_keeps_the_failure_visible(self):
        c = web_cluster()
        self.dead_run(c, RP.STALE_TAKING_MS + 1000)
        c.fail_power[("web", "resume")] = None
        summary = c.runner().recover()
        self.assertFalse(summary.ok)
        self.assertEqual(c.stored_sets()[0]["state"], RP.SET_TAKING)


class RestoringASetIsAllOrNothingUntilItStarts(unittest.TestCase):

    def stopped_cluster(self):
        c = web_cluster(RP.QUIESCE_DOMAIN)
        entry = c.runner().snapshot_domain("web")
        for vm in ("web", "db"):
            c.vms[vm]["state"] = "Stopped"
        c.attached = {"10.0.0.1": [], "10.0.0.2": []}
        return c, entry

    def test_every_member_is_checked_first_and_none_is_touched_if_one_is_refused(self):
        c, entry = self.stopped_cluster()
        c.vms["db"]["state"] = "Running"
        with self.assertRaises(RP.DomainRefused) as ctx:
            c.runner().restore_set("web", entry["set_id"])
        self.assertIn("nothing was restored", str(ctx.exception))
        self.assertEqual([x for x in c.order() if x.startswith("rollback")], [])

    def test_a_restore_puts_every_disk_of_the_set_back(self):
        c, entry = self.stopped_cluster()
        done = c.runner().restore_set("web", entry["set_id"])
        self.assertEqual(sorted(done), ["db-disk0", "db-disk1", "web-disk0", "web-disk1"])

    def test_a_restore_that_stops_halfway_says_what_is_done_and_what_is_not(self):
        c, entry = self.stopped_cluster()
        original, calls = c.dfs, []

        def flaky(ip, payload):
            if payload["op"] == "rollback":
                calls.append(payload["vdisk_id"])
                if len(calls) == 3:
                    return -1, {"error": "replica unreachable"}, "replica unreachable"
            return original(ip, payload)
        c.dfs = flaky
        with self.assertRaises(RP.RestoreIncomplete) as ctx:
            c.runner().restore_set("web", entry["set_id"])
        self.assertEqual(len(ctx.exception.done), 2)
        self.assertEqual(len(ctx.exception.remaining), 2)
        self.assertIn("again", str(ctx.exception))

    def test_a_set_that_failed_cannot_be_restored(self):
        c = web_cluster()
        c.sets.append({"domain": "web", "taken_at_ms": NOW, "set_id": "web-f", "origin": "manual",
                       "state": "failed", "members": "[]", "paused_vms": "[]"})
        with self.assertRaises(RP.DomainRefused):
            c.runner().restore_set("web", "web-f")


class ADiskInADomainIsNotSnapshottedTwice(unittest.TestCase):

    def test_the_per_vdisk_policy_leaves_a_domains_disks_to_the_domain(self):
        c = web_cluster()
        c.add_vdisk("loose")
        c.attach("loose")
        original = c.query
        policy = {"scope": "cluster", "target": "*", "enabled": True,
                  "interval_seconds": 86400, "keep_last": 3}

        def with_policy(cql):
            if "FROM hydra.dfs_snapshot_policies" in cql:
                return 0, json.dumps(policy), ""
            return original(cql)
        c.query = with_policy
        runner = SNAP.Runner(c.env(), SCHEMA)
        summary = runner.run()
        taken = [t for t in summary.taken]
        self.assertEqual(len(taken), 1)
        self.assertTrue(taken[0].startswith("loose-auto-"))

    def test_a_disabled_domain_does_not_claim_its_disks(self):
        c = web_cluster()
        c.domains["web"]["enabled"] = False
        self.assertEqual(RP.claimed_vdisks(c.query), set())

    def test_a_cluster_without_the_tables_still_snapshots_on_policy(self):
        self.assertEqual(RP.claimed_vdisks(lambda cql: (1, "", "unconfigured table")), set())


class Administration(unittest.TestCase):

    def test_names_that_are_not_names_never_reach_statement_text(self):
        for bad in ("a b", "x'; DROP TABLE hydra.vms; --", "", "-lead"):
            with self.assertRaises(RP.DomainError):
                RP.domain_insert_statement(bad, True, 86400, 3, "vm", 30, NOW, NOW)
            with self.assertRaises(RP.DomainError):
                RP.parse_member("vm:" + bad)

    def test_a_policy_that_could_not_be_kept_is_refused_when_written(self):
        for args in ((600, 3, "vm", 30), (86400, 0, "vm", 30), (86400, 3, "pause", 30),
                     (86400, 3, "vm", 0), (86400, 3, "vm", 3600)):
            with self.assertRaises(RP.DomainError, msg=str(args)):
                RP.validate_policy(*args)

    def test_a_member_must_be_spelled_with_its_kind(self):
        self.assertEqual(RP.parse_member("vm:web"), ("vm", "web"))
        self.assertEqual(RP.parse_member("vdisk:web-disk0"), ("vdisk", "web-disk0"))
        for bad in ("web", "rack:3", "vm:"):
            with self.assertRaises(RP.DomainError):
                RP.parse_member(bad)

    def test_a_member_that_does_not_exist_is_refused_when_added(self):
        c = FakeCluster()
        c.add_domain("web")
        with self.assertRaises(RP.DomainRefused):
            c.runner().add_member("web", "vm", "nope")
        with self.assertRaises(RP.DomainRefused):
            c.runner().add_member("nodomain", "vm", "x")

    def test_an_image_cannot_be_a_member_because_only_writable_disks_are_snapshotted(self):
        c = FakeCluster()
        c.add_domain("web")
        c.add_vdisk("img", "immutable", "")
        with self.assertRaises(RP.DomainRefused):
            c.runner().add_member("web", "vdisk", "img")

    def test_deleting_a_domain_that_holds_sets_needs_to_say_so(self):
        c = web_cluster(RP.QUIESCE_DOMAIN)
        c.runner().snapshot_domain("web")
        with self.assertRaises(RP.DomainRefused):
            c.runner().delete_domain("web")
        self.assertTrue(c.domains)
        c.runner().delete_domain("web", with_sets=True)
        self.assertEqual((c.domains, c.members, c.sets), ({}, [], []))
        self.assertEqual([v for v in c.vdisks if "-dom-" in v], [])

    def test_a_member_snapshot_name_too_long_is_refused_not_truncated(self):
        with self.assertRaises(RP.DomainError):
            RP.member_snapshot_name("v" * 60, NOW)

    def test_the_command_line_round_trips_through_the_runner(self):
        c = web_cluster()
        c.domains.clear()
        c.members.clear()
        lines = []
        env = c.env()
        run = lambda *argv: RP.run_command(["valcli"] + list(argv), env, SCHEMA, lines.append)
        self.assertEqual(run("storage.domain.create", "web", "--every-hours", "24", "--keep", "5",
                             "--quiesce", "domain"), 0)
        self.assertEqual(run("storage.domain.add", "web", "vm:web", "vm:db"), 0)
        self.assertEqual(run("storage.domain.snapshot", "web"), 0)
        self.assertTrue(any("crash:domain" in line for line in lines))
        self.assertTrue(any("Crash-consistent only" in line for line in lines))
        self.assertEqual(run("storage.domain"), 0)
        self.assertEqual(run("storage.domain.sets", "web"), 0)
        self.assertEqual(run("storage.domain.create", "web", "--every-hours", "x"), 1)
        self.assertEqual(run("storage.domain.snapshot", "nope"), 1)

    def test_a_run_with_a_failure_exits_non_zero_and_a_skip_does_not(self):
        c = web_cluster(RP.QUIESCE_DOMAIN)
        lines = []
        env = c.env()
        self.assertEqual(RP.run_command(["valcli", "storage.domain.run"], env, SCHEMA, lines.append), 0)
        c.clock += 25 * HOUR_MS
        c.fail_snapshot.add("web-disk0")
        self.assertEqual(RP.run_command(["valcli", "storage.domain.run"], env, SCHEMA, lines.append), 1)


class TheWiring(unittest.TestCase):

    def test_the_migrations_are_in_my_assigned_range_and_in_id_order(self):
        ids = [m["id"] for m in SCHEMA.MIGRATIONS]
        self.assertEqual(ids, sorted(ids))
        mine = [i for i in ids if i.startswith(("0030", "0031", "0032"))]
        self.assertEqual(mine, ["0030-protection-domains", "0031-protection-domain-members",
                                "0032-protection-sets"])

    def test_they_are_create_table_only_so_scylla_3_0_8_accepts_them(self):
        by_id = dict((m["id"], m) for m in SCHEMA.MIGRATIONS)
        for name in ("0030-protection-domains", "0031-protection-domain-members",
                     "0032-protection-sets"):
            for statement in by_id[name]["statements"]:
                self.assertTrue(statement.startswith("CREATE TABLE IF NOT EXISTS"), statement)
                self.assertNotIn("ALTER", statement.upper())

    def test_the_columns_the_runner_writes_exist_in_the_tables_it_writes_them_to(self):
        by_id = dict((m["id"], m) for m in SCHEMA.MIGRATIONS)
        wrote = RP.set_row_statement({"domain": "d", "taken_at_ms": 1, "set_id": "s"})
        ddl = by_id["0032-protection-sets"]["statements"][0]
        for column in columns(wrote):
            self.assertRegex(ddl, r"\b%s\b" % column)
        domain_ddl = by_id["0030-protection-domains"]["statements"][0]
        for column in columns(RP.domain_insert_statement("d", True, 86400, 3, "vm", 30, 1, 1)):
            self.assertRegex(domain_ddl, r"\b%s\b" % column)

    def test_valcli_dispatches_the_domain_commands_and_builds_the_power_call(self):
        source = read("valcli.py")
        self.assertIn('cmd.startswith("storage.domain")', source)
        self.assertIn("rauru_protection.run_command", source)
        self.assertIn("/api/v1/vm/%s/power", source)

    def test_the_barrier_is_reachable_through_spark_not_just_implemented_here(self):
        # An action the allow-list does not name is refused, which is how a Sidon op once
        # existed and could not be called.
        self.assertRegex(read("spark_daemon_decoded.py"),
                         r'VM_POWER_ACTIONS = \([^)]*"suspend"[^)]*"resume"')

    def test_the_module_is_in_every_list_that_ships_a_module(self):
        for name in ("sync_provision.py", "deploy_updates.py", "create_upgrade_zip.py",
                     "check_updates.py", "provision.py"):
            self.assertIn("rauru_protection", read(name), name)

    def test_it_never_asks_who_leads_zookeeper(self):
        source = read("rauru_protection.py")
        for needle in ("leader_ip", "is_zookeeper_leader", "get_zookeeper_leader"):
            self.assertNotIn(needle, source)

    def test_it_touches_no_service_registry_and_does_not_import_the_daemon(self):
        source = read("rauru_protection.py")
        self.assertNotIn("import rauru\n", source)
        self.assertNotIn("from rauru ", source)


if __name__ == "__main__":
    unittest.main()
