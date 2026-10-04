#!/usr/bin/env python3
"""Automatic VM migration as one story: DRS, evacuation, manual moves and HA restarts.

They end in the same two operations -- a migrate task or a start task -- and the safety of both
rests on where the guest is allowed to land and how often it is allowed to move. Before this:

* a manual or DRS migration checked only the target's own maintenance flag, so it could be aimed
  at a host recorded as DEGRADED, FENCED, DOWN or RECOVERING (what a quarantine and a failover
  write); start-time placement refused those. Now both use one test, `host_ineligible_reason`;
* DRS's cooldown lived in the process's memory, so a restart or a change of Vali leader forgot it,
  and nothing stopped it picking the guest it had just moved: a pair of hosts could trade one VM.
  Both now read hydra.vali_drs_history, which every successful migration writes.

Run with:  python -m unittest test_automatic_migration
"""

import importlib.util
import io
import json
import os
import sys
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))


def load_vali():
    spec = importlib.util.spec_from_file_location("vali_automatic_migration", os.path.join(HERE, "vali.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass
    return module


vali = load_vali()
A, B = "10.0.0.1", "10.0.0.2"


def healthy():
    return {"maintenance_status": "NORMAL", "services": {"Vali": {"status": "UP"}}}


class TheOneTestForALandingHost(unittest.TestCase):
    def reason(self, node_rows, status=None, rc=0):
        with mock.patch.object(vali, "run_mtls_spark_api",
                               lambda ip, path, payload=None, method="POST": (rc, status if status is not None else healthy(), "")):
            return vali.host_ineligible_reason(B, node_rows)

    def test_a_normal_answering_host_is_eligible(self):
        self.assertIsNone(self.reason({B: {"status": "NORMAL", "maintenance_mode": False}}))

    def test_a_host_with_no_row_but_a_healthy_daemon_is_eligible(self):
        self.assertIsNone(self.reason({}))

    def test_every_state_a_quarantine_a_failover_or_maintenance_writes_is_refused(self):
        for state in ("DEGRADED", "FENCED", "DOWN", "RECOVERING", "IN_MAINTENANCE", "ENTERING_MAINTENANCE"):
            with self.subTest(state=state):
                why = self.reason({B: {"status": state, "maintenance_mode": False}})
                self.assertIn(state, why)

    def test_the_maintenance_flag_alone_is_enough(self):
        self.assertIsNotNone(self.reason({B: {"status": "NORMAL", "maintenance_mode": True}}))

    def test_an_unreadable_database_is_a_reason_and_not_a_pass(self):
        self.assertIn("could not be read", self.reason(None))

    def test_a_host_that_does_not_answer_is_refused(self):
        self.assertIn("not answering", self.reason({}, rc=-1, status={}))

    def test_a_host_with_a_service_down_is_refused_and_the_service_is_named(self):
        down = {"maintenance_status": "NORMAL", "services": {"Vali": {"status": "UP"}, "Sidon": {"status": "DOWN"}}}
        self.assertIn("Sidon", self.reason({}, status=down))

    def test_a_host_the_daemon_says_is_in_maintenance_is_refused(self):
        self.assertIn("maintenance", self.reason({}, status={"maintenance_status": "IN_MAINTENANCE", "services": {}}))


class ManualMigrationUsesIt(unittest.TestCase):
    """The migrate task, against a target recorded as DEGRADED."""

    def run_migrate(self, node_status):
        events = []
        rows = {"name": "web", "host_ip": A, "state": "Running", "ram": 1024,
                "disks_list": "a:default:virtio", "iso": ""}

        def cql(query):
            if "FROM hydra.nodes" in query:
                return 0, json.dumps({"ip": B, "status": node_status, "maintenance_mode": False}), ""
            return 0, "", ""

        patches = {
            "get_node_ip": lambda h: h, "get_vm_xml_specs": lambda n: dict(rows),
            "run_cql_query": cql,
            "run_mtls_spark_api": lambda ip, path, payload=None, method="POST": (0, healthy(), ""),
            "get_node_utilization": lambda ip, fetch_cpu=False: (0, 0, 16000, 1000),
            "get_vm_disk_size": lambda n: 100, "get_storage_free_space": lambda ip: 100000,
            "run_lwt": lambda ep, params, timeout=15: events.append(ep) or (True, True, {}, ""),
            "run_remote_spark": lambda ip, cmd, timeout=None: events.append("remote") or (0, "", ""),
            "run_mtls_spark_api_full": lambda *a, **k: events.append("dfs") or (200, {"forwarding_to": "x"}, ""),
        }
        started = [mock.patch.object(vali, n, f) for n, f in patches.items()]
        for p in started:
            p.start()
        try:
            with mock.patch("sys.stdout", io.StringIO()), mock.patch.object(vali.time, "sleep", lambda s: None):
                result = vali.process_queue_task({"task_id": "t", "vm_name": "web", "action": "migrate",
                                                  "target_host": B, "payload": {}})
        finally:
            for p in started:
                p.stop()
        return result, events

    def test_a_degraded_target_is_refused_before_anything_is_touched(self):
        (ok, detail), events = self.run_migrate("DEGRADED")
        self.assertFalse(ok)
        self.assertIn("cannot take guests", detail)
        self.assertIn("DEGRADED", detail)
        self.assertEqual(events, [], "no lock, no attach, no migrate")

    def test_a_fenced_target_is_refused(self):
        (ok, detail), _ = self.run_migrate("FENCED")
        self.assertFalse(ok)
        self.assertIn("FENCED", detail)

    def test_a_normal_target_proceeds(self):
        (ok, detail), events = self.run_migrate("NORMAL")
        self.assertTrue(ok, detail)
        self.assertIn("/v1/vm/migrate-lock", events)


class DrsDoesNotThrash(unittest.TestCase):
    def setUp(self):
        self.submitted = []
        self.history = []
        self.now = time.time()
        vm_rows = [{"name": "big", "host_ip": A, "memory": 1000, "state": "Running"},
                   {"name": "small", "host_ip": A, "memory": 900, "state": "Running"}]

        def cql(query):
            if "FROM hydra.cluster_settings" in query:
                return 0, "", ""
            if "FROM hydra.nodes" in query:
                return 0, "\n".join(json.dumps({"ip": ip, "status": "NORMAL", "maintenance_mode": False})
                                    for ip in (A, B)), ""
            if "FROM hydra.vali_drs_history" in query:
                return 0, "\n".join(json.dumps(h) for h in self.history), ""
            if "FROM hydra.vms" in query:
                return 0, "\n".join(json.dumps(v) for v in vm_rows), ""
            return 0, "", ""

        loads = {A: (0.8, 0.8, 10000, 8000), B: (0.1, 0.1, 10000, 1000)}
        patches = {
            "get_cluster_hosts": lambda: [{"ip": A}, {"ip": B}],
            "run_cql_query": cql,
            "run_mtls_spark_api": lambda ip, path, payload=None, method="POST": (0, healthy(), ""),
            "get_node_utilization": lambda ip, fetch_cpu=False: loads[ip],
            "call_catalyst_api": lambda path, payload=None, method="GET": self.submitted.append(payload) or (200, {}),
        }
        for name, fn in patches.items():
            p = mock.patch.object(vali, name, fn)
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(setattr, vali, "last_migration_time", 0.0)
        vali.last_migration_time = 0.0
        quiet = mock.patch("sys.stdout", io.StringIO())
        quiet.start()
        self.addCleanup(quiet.stop)

    def moved(self, name, seconds_ago):
        return {"vm_name": name, "event_time": int((self.now - seconds_ago) * 1000)}

    def test_with_nothing_recent_it_moves_a_guest(self):
        vali.run_drs_loop()
        self.assertEqual(len(self.submitted), 1)
        self.assertEqual(self.submitted[0]["payload"]["target_host"], B)

    def test_a_guest_that_just_moved_is_not_chosen_again(self):
        self.history = [self.moved("big", 600)]
        # The cooldown is the global one's 300 s: 600 s ago is past it, so only the per-guest rule is in play.
        vali.run_drs_loop()
        chosen = [s["payload"]["vm_name"] for s in self.submitted]
        self.assertEqual(chosen, ["small"], "the guest moved ten minutes ago must be left where it is")

    def test_if_every_candidate_moved_recently_nothing_moves(self):
        self.history = [self.moved("big", 600), self.moved("small", 700)]
        vali.run_drs_loop()
        self.assertEqual(self.submitted, [])

    def test_the_cooldown_comes_from_the_history_so_a_restart_does_not_forget_it(self):
        self.history = [self.moved("someone-else", 60)]
        vali.last_migration_time = 0.0          # a fresh process
        vali.run_drs_loop()
        self.assertEqual(self.submitted, [], "a migration a minute ago, by anyone, holds the cooldown")

    def test_a_long_ago_migration_does_not(self):
        self.history = [self.moved("big", 7200)]
        vali.run_drs_loop()
        self.assertEqual(len(self.submitted), 1)

    def test_an_unreadable_history_falls_back_to_the_process_cooldown(self):
        with mock.patch.object(vali, "recent_drs_migrations", lambda now, window=1800: {}):
            vali.last_migration_time = self.now - 10
            vali.run_drs_loop()
        self.assertEqual(self.submitted, [])

    def test_an_aggressive_run_is_the_operators_explicit_ask_and_ignores_the_history(self):
        self.history = [self.moved("big", 30), self.moved("small", 30)]
        vali.run_drs_loop(aggressive=True)
        self.assertEqual(len(self.submitted), 1)

    def test_the_history_reader_only_returns_what_is_inside_the_window(self):
        self.history = [self.moved("old", 5000), self.moved("fresh", 100), {"vm_name": "bad", "event_time": "x"}]
        got = vali.recent_drs_migrations(self.now)
        self.assertEqual(list(got), ["fresh"])


class StartTimePlacementAlreadyExcludedThem(unittest.TestCase):
    """Evacuation and HA restart choose their host with select_best_start_host. Pinned, because
    the story is only one story while that stays true."""

    def test_a_degraded_host_is_never_chosen_to_start_a_guest(self):
        def cql(query):
            return 0, "\n".join(json.dumps({"ip": ip, "status": s, "maintenance_mode": False})
                                for ip, s in ((A, "DEGRADED"), (B, "NORMAL"))), ""

        with mock.patch.object(vali, "run_cql_query", cql), \
                mock.patch.object(vali, "get_cluster_hosts", lambda: [{"ip": A}, {"ip": B}]), \
                mock.patch.object(vali, "run_mtls_spark_api", lambda ip, path, payload=None, method="POST": (0, healthy(), "")), \
                mock.patch.object(vali, "get_node_utilization", lambda ip, fetch_cpu=False: (0, 0, 16000, 1000)):
            self.assertEqual(vali.select_best_start_host(1024), B)


if __name__ == "__main__":
    unittest.main()
