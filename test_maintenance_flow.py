#!/usr/bin/env python3
"""The maintenance flow, state by state, with the cluster stubbed.

What a host in maintenance keeps running was defined three ways that disagreed: Vali stopped
every unit but ZooKeeper, Spark's autostart-in-maintenance stopped a different list and started
the database, and its watchdog restarted the database and the storage daemon the first list had
just stopped. A host "in maintenance" therefore showed ZooKeeper, HydraDB, Daruk, Spark and Hylia
UP and Sidon DOWN, by accident. The rule is now one declared column (`maintenance` in spark's
MANAGED_SERVICES) and everything else derives from it or is held to it by the tests below.

The flow (docs/maintenance.md): request, gate, lock, claim, evacuate, quorum re-check, marker,
stop; and on the way out: marker removed, services started, RECOVERING, services verified UP,
NORMAL, lock released. Each failure leaves the host where the document says.

Run with:  python -m unittest test_maintenance_flow
"""

import importlib.util
import io
import json
import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name, alias):
    spec = importlib.util.spec_from_file_location(alias, os.path.join(HERE, name))
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass
    return module


vali = load("vali.py", "vali_maintenance_flow")
spark = load("spark_daemon_decoded.py", "spark_maintenance_flow")
cli = load("cluster_new.py", "cluster_maintenance_flow")


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


class TheRuleIsOneColumn(unittest.TestCase):
    KEPT = {"hydra-db", "daruk", "sidon", "hylia"}

    def test_the_declared_table_keeps_exactly_the_documented_units(self):
        self.assertEqual(set(spark.maintenance_kept_units()), self.KEPT)
        self.assertEqual(set(spark.MAINTENANCE_UNMANAGED_KEPT), {"zookeeper", "spark-daemon"})

    def test_every_managed_unit_is_either_kept_or_stopped_never_both_never_neither(self):
        managed = [e["unit"] for e in spark.MANAGED_SERVICES]
        kept, stopped = spark.maintenance_kept_units(), spark.maintenance_stopped_units()
        self.assertEqual(sorted(kept + stopped), sorted(managed))
        self.assertEqual(set(kept) & set(stopped), set())

    def test_vali_stops_what_the_table_says_and_nothing_else(self):
        self.assertEqual(set(vali.MAINTENANCE_STOP_UNITS), set(spark.maintenance_stopped_units()))
        self.assertEqual(len(vali.MAINTENANCE_STOP_UNITS), len(set(vali.MAINTENANCE_STOP_UNITS)))

    def test_nothing_kept_is_stopped_by_maintenance(self):
        for unit in self.KEPT | {"zookeeper", "spark-daemon"}:
            self.assertNotIn(unit, vali.MAINTENANCE_STOP_UNITS)

    def test_the_watchdog_restarts_what_is_kept_in_start_order(self):
        order = spark.maintenance_watchdog_units()
        self.assertEqual(order[0], "zookeeper")
        self.assertEqual(set(order), {"zookeeper"} | self.KEPT)
        # Daruk after the database it fronts, and Sidon after Daruk.
        self.assertLess(order.index("hydra-db"), order.index("daruk"))
        self.assertLess(order.index("daruk"), order.index("sidon"))

    def test_the_hardcoded_lists_that_disagreed_are_gone(self):
        src = read("spark_daemon_decoded.py")
        self.assertNotIn('for svc in ["zookeeper", "hydra-db", "sidon"]:', src)
        self.assertEqual(src.count("for svc in maintenance_watchdog_units():"), 3,
                         "the autostart start loop and both watchdogs use the one list")
        self.assertIn("for svc in maintenance_stopped_units():", src)

    def test_a_service_row_says_whether_it_is_kept(self):
        self.assertIn('"kept_in_maintenance": svc in kept_in_maintenance', read("spark_daemon_decoded.py"))


class TheStatusLabelsWhatIsUpOnPurpose(unittest.TestCase):
    def block(self, maint, **rows):
        services = {name: dict({"status": "UP", "pids": [1], "restarts": 0}, **extra)
                    for name, extra in rows.items()}
        return cli.render_node_block("10.0.0.2", {
            "hostname": "n2", "maintenance_status": maint, "ts": 10 ** 12, "services": services,
        }, use_color=False)

    def test_a_kept_unit_is_labelled_in_maintenance(self):
        text = self.block("IN_MAINTENANCE", HydraDB={"kept_in_maintenance": True})
        self.assertIn("kept up in maintenance", text)

    def test_a_unit_that_should_have_stopped_is_called_out(self):
        text = self.block("IN_MAINTENANCE", Vali={"kept_in_maintenance": False})
        self.assertIn("expected to be stopped in maintenance", text)

    def test_a_normal_host_gets_no_label(self):
        text = self.block("NORMAL", HydraDB={"kept_in_maintenance": True},
                          Vali={"kept_in_maintenance": False})
        self.assertNotIn("maintenance)", text)


class Cluster:
    """The database row, the lock, the host's marker file and Catalyst, in memory."""

    def __init__(self, status="NORMAL"):
        self.status = status
        self.lock = None
        self.marker = False
        self.events = []
        self.vms = []
        self.quorum = (True, "RF=3, 3 of 3 up")
        self.lock_taken_elsewhere = None
        self.catalyst_ok = True
        self.status_after_leave = None
        self.node_status_replies = []        # popped per poll; the last one repeats
        self.marker_write_rc = 0
        self.unit_action_ok = True
        self.migrations_ok = True
        self.submitted = []
        self.history = []

    # -- stubs ---------------------------------------------------------------------------

    def cql(self, query):
        self.events.append(("cql", query))
        if "SELECT JSON hostname, ip, status" in query:
            return 0, json.dumps({"hostname": "n2", "ip": "10.0.0.2", "status": self.status,
                                  "maintenance_mode": self.status == "IN_MAINTENANCE"}), ""
        if "SELECT JSON name, host_ip, state, memory FROM hydra.vms" in query:
            return 0, "\n".join(json.dumps(v) for v in self.vms), ""
        if "UPDATE hydra.nodes SET status = 'NORMAL'" in query:
            self.status = "NORMAL"
        elif "SET status = 'IN_MAINTENANCE'" in query:
            self.status = "IN_MAINTENANCE"
        elif "SET status = 'RECOVERING'" in query:
            self.status = "RECOVERING"
        return 0, "", ""

    def lwt(self, endpoint, params, timeout=15):
        self.events.append(("lwt", endpoint, dict(params)))
        if endpoint == "/v1/node/maintenance":
            if self.status != params.get("expected_status"):
                return True, False, {"status": self.status}, ""
            self.status = params["status"]
            return True, True, {}, ""
        return True, True, {}, ""

    def acquire(self, hostname, reason):
        self.events.append(("lock-acquire", hostname))
        if self.lock_taken_elsewhere:
            return True, "", {"holder": self.lock_taken_elsewhere, "reason": "draining"}, ""
        self.lock = {"holder": hostname, "holder_token": "tok"}
        return True, "tok", {}, ""

    def release(self, token):
        self.events.append(("lock-release", token))
        self.lock = None
        return True

    def release_for_host(self, hostname):
        self.events.append(("lock-release-host", hostname))
        self.lock = None
        return True

    def renew(self, hostname, token, reason):
        self.events.append(("lock-renew", hostname))
        return True

    def catalyst(self, path, payload=None, method="GET"):
        self.events.append(("catalyst", path, payload))
        if path == "/api/v1/tasks/submit":
            self.submitted.append(payload)
            return (200, {"task_id": "T1"}) if self.catalyst_ok else (500, {"error": "down"})
        return 200, {}

    def remote(self, ip, command, timeout=None):
        self.events.append(("remote", ip, command))
        if "touch /etc/hci/maintenance.state" in command:
            if self.marker_write_rc == 0:
                self.marker = True
            return self.marker_write_rc, "", "read-only file system" if self.marker_write_rc else ""
        if "rm -f /etc/hci/maintenance.state" in command:
            self.marker = False
        return 0, "", ""

    def units(self, ip, action, units, detach=False, ignore_failed=False):
        self.events.append(("units", action, list(units), detach))
        return (True, "") if self.unit_action_ok else (False, "spark refused")

    def node_status(self, ip, path, payload=None, method="POST"):
        if path == "/api/v1/node/status":
            self.events.append(("status-poll",))
            reply = self.node_status_replies[0] if len(self.node_status_replies) == 1 else (
                self.node_status_replies.pop(0) if self.node_status_replies else
                {"maintenance_status": "NORMAL", "services": {"Vali": {"status": "UP"}}})
            return 0, reply, ""
        return 0, {}, ""

    def submit_and_wait(self, service, action, payload, timeout_polls=3, parent_task_id=None):
        self.events.append(("subtask", service, action))
        return True, "", {}

    def best_host(self, memory):
        return "10.0.0.3" if self.migrations_ok else None

    def has(self, *prefix):
        return [e for e in self.events if e[:len(prefix)] == prefix]


def patch_vali(c):
    patches = {
        "run_cql_query": c.cql, "run_lwt": c.lwt, "acquire_maintenance_lock": c.acquire,
        "release_maintenance_lock": c.release, "release_maintenance_lock_for_host": c.release_for_host,
        "renew_maintenance_lock": c.renew, "call_catalyst_api": c.catalyst,
        "run_remote_spark": c.remote, "spark_unit_action": c.units,
        "run_mtls_spark_api": c.node_status, "submit_and_wait_task": c.submit_and_wait,
        "select_best_start_host": c.best_host,
        "check_stop_preserves_quorum": lambda ip: c.quorum,
        "read_maintenance_lock": lambda: c.lock,
    }
    started = []
    for name, fn in patches.items():
        p = mock.patch.object(vali, name, fn)
        p.start()
        started.append(p)
    return started


class ValiTestCase(unittest.TestCase):
    status = "NORMAL"

    def setUp(self):
        self.c = Cluster(self.status)
        for p in patch_vali(self.c):
            self.addCleanup(p.stop)
        quiet = mock.patch("sys.stdout", io.StringIO())
        quiet.start()
        self.addCleanup(quiet.stop)
        nap = mock.patch.object(vali.time, "sleep", lambda s: None)
        nap.start()
        self.addCleanup(nap.stop)

    def request(self, action, **extra):
        replies = []
        vali.handle_maintenance_request(
            dict({"hostname": "n2", "action": action}, **extra),
            lambda status, body: replies.append((status, body)))
        self.assertEqual(len(replies), 1, replies)
        return replies[0]

    def task(self, action, **payload):
        body = dict({"hostname": "n2", "target_ip": "10.0.0.2", "lock_token": "tok"}, **payload)
        return vali.process_queue_task({"task_id": "T1", "vm_name": "", "action": action,
                                        "payload": body})


class TheRequestToEnter(ValiTestCase):
    def test_a_normal_host_is_claimed_and_the_task_submitted(self):
        status, body = self.request("enter")
        self.assertEqual((status, body["status"]), (200, "transitioning"))
        self.assertEqual(self.c.status, "ENTERING_MAINTENANCE")
        self.assertEqual(self.c.submitted[0]["action"], "host_maintenance_enter")
        self.assertEqual(self.c.submitted[0]["payload"]["lock_token"], "tok")
        self.assertIsNotNone(self.c.lock, "the lock travels with the task")

    def test_the_quorum_gate_refuses_before_anything_is_taken(self):
        self.c.quorum = (False, "stopping n2 leaves 1 of 3 replicas")
        status, body = self.request("enter")
        self.assertEqual((status, body["reason"]), (409, "quorum"))
        self.assertEqual(self.c.has("lock-acquire"), [])
        self.assertEqual(self.c.status, "NORMAL")

    def test_another_host_holding_the_lock_refuses_with_its_name(self):
        self.c.lock_taken_elsewhere = "n3"
        status, body = self.request("enter")
        self.assertEqual((status, body["reason"], body["holder"]), (409, "locked", "n3"))
        self.assertEqual(self.c.has("lwt"), [], "no claim without the lock")

    def test_a_host_that_is_not_normal_is_refused_and_the_lock_given_back(self):
        self.c.status = "DEGRADED"
        status, body = self.request("enter")
        self.assertEqual(status, 409)
        self.assertIn("DEGRADED", body["error"])
        self.assertIsNone(self.c.lock)
        self.assertEqual(self.c.status, "DEGRADED", "a refused request changes nothing")

    def test_a_catalyst_that_will_not_take_the_task_puts_everything_back(self):
        self.c.catalyst_ok = False
        status, body = self.request("enter")
        self.assertEqual(status, 500)
        self.assertEqual(self.c.status, "NORMAL")
        self.assertIsNone(self.c.lock)

    def test_an_unknown_host_and_a_bad_action(self):
        replies = []
        with mock.patch.object(vali, "run_cql_query", lambda q: (0, "", "")):
            vali.handle_maintenance_request({"hostname": "ghost", "action": "enter"},
                                            lambda s, b: replies.append(s))
        self.assertEqual(replies, [404])
        self.assertEqual(self.request("sideways")[0], 400)

    def test_the_request_names_what_it_needs(self):
        replies = []
        vali.handle_maintenance_request({"hostname": "n2"}, lambda s, b: replies.append(s))
        self.assertEqual(replies, [400])


class TheTaskThatEnters(ValiTestCase):
    status = "ENTERING_MAINTENANCE"

    def setUp(self):
        super().setUp()
        self.c.lock = {"holder": "n2", "holder_token": "tok"}

    def test_an_empty_host_enters_stops_the_declared_units_and_keeps_the_rest(self):
        ok, detail = self.task("host_maintenance_enter")
        self.assertTrue(ok, detail)
        self.assertEqual(self.c.status, "IN_MAINTENANCE")
        self.assertTrue(self.c.marker)
        stop = self.c.has("units", "stop")[0]
        self.assertEqual(set(stop[2]), set(spark.maintenance_stopped_units()))
        self.assertTrue(stop[3], "detached: vali is in the list and cannot wait for its own stop")
        self.assertIsNotNone(self.c.lock, "the lock stays held for the whole window")

    def test_the_marker_is_written_before_anything_is_stopped(self):
        self.task("host_maintenance_enter")
        kinds = [e[0] for e in self.c.events]
        marker_at = next(i for i, e in enumerate(self.c.events)
                         if e[0] == "remote" and "touch /etc/hci/maintenance.state" in e[2])
        stop_at = kinds.index("units")
        self.assertLess(marker_at, stop_at)

    def test_the_quorum_is_checked_again_after_the_evacuation(self):
        self.c.quorum = (False, "a second replica went down while the host was draining")
        ok, detail = self.task("host_maintenance_enter")
        self.assertFalse(ok)
        self.assertIn("a second replica went down", detail)
        self.assertEqual(self.c.status, "NORMAL", "a failed enter leaves the host NORMAL")
        self.assertIsNone(self.c.lock)
        self.assertFalse(self.c.marker)
        self.assertEqual(self.c.has("units"), [], "nothing was stopped")

    def test_a_vm_that_cannot_be_moved_fails_the_enter_and_names_it(self):
        self.c.vms = [{"name": "web", "host_ip": "10.0.0.2", "state": "Running", "memory": 1024}]
        self.c.migrations_ok = False
        ok, detail = self.task("host_maintenance_enter", force_stop=False)
        self.assertFalse(ok)
        self.assertIn("web", detail)
        self.assertEqual(self.c.status, "NORMAL")
        self.assertIsNone(self.c.lock)
        self.assertEqual(self.c.has("units"), [])
        self.assertFalse(self.c.marker)

    def test_a_vm_is_force_stopped_when_asked_and_there_is_nowhere_to_put_it(self):
        self.c.vms = [{"name": "web", "host_ip": "10.0.0.2", "state": "Running", "memory": 1024}]
        self.c.migrations_ok = False
        ok, detail = self.task("host_maintenance_enter", force_stop=True)
        self.assertTrue(ok, detail)
        self.assertEqual(self.c.has("subtask", "vali", "stop") and 1, 1)
        self.assertEqual(self.c.status, "IN_MAINTENANCE")

    def test_a_marker_that_cannot_be_written_abandons_the_enter(self):
        self.c.marker_write_rc = 1
        ok, detail = self.task("host_maintenance_enter")
        self.assertFalse(ok)
        self.assertIn("marker", detail)
        self.assertEqual(self.c.status, "NORMAL")
        self.assertIsNone(self.c.lock)
        self.assertEqual(self.c.has("units"), [], "nothing is stopped on a host that does not know")

    def test_a_stop_that_spark_refuses_is_reported_and_the_way_back_is_named(self):
        self.c.unit_action_ok = False
        ok, detail = self.task("host_maintenance_enter")
        self.assertFalse(ok)
        self.assertIn("valcli host.maintenance.leave n2", detail)
        self.assertEqual(self.c.status, "IN_MAINTENANCE", "the VMs are already gone; leave undoes it")

    def test_an_invalid_hostname_gives_the_lock_back(self):
        ok, _ = vali.process_queue_task({"task_id": "T", "vm_name": "", "action": "host_maintenance_enter",
                                         "payload": {"hostname": "bad name;", "target_ip": "10.0.0.2",
                                                     "lock_token": "tok"}})
        self.assertFalse(ok)
        self.assertEqual(self.c.has("lock-release"), [("lock-release", "tok")])


class TheRequestToLeave(ValiTestCase):
    def test_a_host_that_is_not_in_maintenance_has_nothing_to_leave(self):
        """It used to run the whole sequence on a healthy host: services restarted, the row set
        RECOVERING, and a lock released that somebody else might hold."""
        status, body = self.request("leave")
        self.assertEqual((status, body["reason"]), (409, "not_in_maintenance"))
        self.assertEqual(self.c.submitted, [])
        self.assertEqual(self.c.status, "NORMAL")

    def test_every_state_a_transition_can_be_stuck_in_can_be_left(self):
        for state in ("IN_MAINTENANCE", "ENTERING_MAINTENANCE", "RECOVERING"):
            with self.subTest(state=state):
                self.c.status = state
                self.c.submitted.clear()
                status, _ = self.request("leave")
                self.assertEqual(status, 200)
                self.assertEqual(self.c.submitted[0]["action"], "host_maintenance_leave")

    def test_a_degraded_or_down_host_is_not_a_maintenance_host(self):
        for state in ("DEGRADED", "DOWN", "FENCED"):
            with self.subTest(state=state):
                self.c.status = state
                self.assertEqual(self.request("leave")[0], 409)


class TheTaskThatLeaves(ValiTestCase):
    status = "IN_MAINTENANCE"

    def setUp(self):
        super().setUp()
        self.c.marker = True
        fast = mock.patch.multiple(vali, MAINTENANCE_EXIT_WAIT_SECONDS=0.05,
                                   MAINTENANCE_EXIT_POLL_SECONDS=0.01)
        fast.start()
        self.addCleanup(fast.stop)

    def up(self):
        return {"maintenance_status": "NORMAL",
                "services": {"Vali": {"status": "UP"}, "HydraDB": {"status": "UP"}}}

    def test_a_clean_exit_ends_normal_with_the_lock_released(self):
        self.c.node_status_replies = [self.up()]
        ok, detail = self.task("host_maintenance_leave")
        self.assertTrue(ok, detail)
        self.assertEqual(self.c.status, "NORMAL", "nothing used to take the host from RECOVERING")
        self.assertFalse(self.c.marker)
        self.assertEqual(self.c.has("lock-release-host"), [("lock-release-host", "n2")])
        self.assertEqual(self.c.has("subtask", "vali", "balance") and 1, 1)

    def test_the_marker_goes_first_then_the_units_start_then_recovering_then_normal(self):
        self.c.node_status_replies = [self.up()]
        self.task("host_maintenance_leave")
        marker_removed = next(i for i, e in enumerate(self.c.events)
                              if e[0] == "remote" and "rm -f /etc/hci/maintenance.state" in e[2])
        started = next(i for i, e in enumerate(self.c.events) if e[:2] == ("units", "start"))
        recovering = next(i for i, e in enumerate(self.c.events)
                          if e[0] == "cql" and "SET status = 'RECOVERING'" in e[1])
        normal = next(i for i, e in enumerate(self.c.events)
                      if e[0] == "lwt" and e[2].get("status") == "NORMAL")
        self.assertLess(marker_removed, started)
        self.assertLess(started, recovering)
        self.assertLess(recovering, normal)

    def test_the_normal_transition_is_conditional_on_recovering(self):
        self.c.node_status_replies = [self.up()]
        self.task("host_maintenance_leave")
        lwt = [e for e in self.c.events if e[0] == "lwt" and e[2].get("status") == "NORMAL"][0]
        self.assertEqual(lwt[2]["expected_status"], "RECOVERING")

    def test_every_managed_unit_and_zookeeper_are_started(self):
        self.c.node_status_replies = [self.up()]
        self.task("host_maintenance_leave")
        started = set(self.c.has("units", "start")[0][2])
        self.assertEqual(started, {"zookeeper"} | {e["unit"] for e in spark.MANAGED_SERVICES})

    def test_a_host_whose_services_do_not_come_up_stays_recovering_and_keeps_the_lock(self):
        self.c.node_status_replies = [{"maintenance_status": "NORMAL",
                                       "services": {"Vali": {"status": "DOWN"}, "Mipha": {"status": "UP"}}}]
        self.c.lock = {"holder": "n2", "holder_token": "tok"}
        ok, detail = self.task("host_maintenance_leave")
        self.assertFalse(ok)
        self.assertIn("Vali", detail)
        self.assertNotIn("Mipha", detail)
        self.assertIn("valcli host.maintenance.leave n2", detail)
        self.assertEqual(self.c.status, "RECOVERING")
        self.assertIsNotNone(self.c.lock, "Mipha keeps renewing it while the row says RECOVERING")
        self.assertEqual(self.c.has("subtask"), [], "no health check or rebalance for a host that is not back")

    def test_a_failed_exit_is_retryable_and_the_retry_completes_it(self):
        self.c.node_status_replies = [{"maintenance_status": "NORMAL",
                                       "services": {"Vali": {"status": "DOWN"}}}]
        self.c.lock = {"holder": "n2", "holder_token": "tok"}
        self.assertFalse(self.task("host_maintenance_leave")[0])
        # The retry is accepted because RECOVERING is a leavable state ...
        self.assertEqual(self.request("leave")[0], 200)
        # ... and now the services are up.
        self.c.node_status_replies = [self.up()]
        ok, detail = self.task("host_maintenance_leave")
        self.assertTrue(ok, detail)
        self.assertEqual(self.c.status, "NORMAL")
        self.assertIsNone(self.c.lock)

    def test_a_status_somebody_else_set_meanwhile_is_not_overwritten(self):
        """Mipha fencing the host while it recovers must win over this task's NORMAL."""
        self.c.node_status_replies = [self.up()]
        real = self.c.lwt

        def fenced(endpoint, params, timeout=15):
            if params.get("status") == "NORMAL":
                self.c.status = "FENCED"
            return real(endpoint, params, timeout)

        with mock.patch.object(vali, "run_lwt", fenced):
            ok, _ = self.task("host_maintenance_leave")
        self.assertTrue(ok)
        self.assertEqual(self.c.status, "FENCED")

    def test_a_host_whose_status_cannot_be_read_is_not_declared_back(self):
        with mock.patch.object(vali, "run_mtls_spark_api", lambda *a, **k: (-1, {}, "unreachable")):
            ok, detail = self.task("host_maintenance_leave")
        self.assertFalse(ok)
        self.assertIn("could not be read", detail)
        self.assertEqual(self.c.status, "RECOVERING")


class AHostThatRebootsInMaintenance(unittest.TestCase):
    """Spark's autostart comes back into maintenance with the same rule, from the same table."""

    def test_the_boot_path_stops_starts_and_watches_the_declared_lists(self):
        src = read("spark_daemon_decoded.py")
        branch = src[src.index('if os.path.exists("/etc/hci/maintenance.state"):\n        print("[AUTOSTART] Host is in maintenance mode.'):]
        branch = branch[:branch.index("# 1. Start ZooKeeper unconditionally")]
        self.assertIn("for svc in maintenance_stopped_units():", branch)
        self.assertIn("for svc in maintenance_watchdog_units():", branch)
        self.assertNotIn('"sidon"', branch, "the boot path used to stop sidon and then watch it")


if __name__ == "__main__":
    unittest.main()
