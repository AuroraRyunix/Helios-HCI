#!/usr/bin/env python3
"""Host states: what sets each, what each stops, and what a host that comes back still holds.

docs/host_states.md is the table; this runs it. Two things here are new behaviour:

* **A host that rejoins is reconciled against Hydra before its services start.** A host marked
  DOWN while it was only partitioned still has its guests' qemu processes running. They were
  restarted elsewhere, the new owner fenced their vdisks (so they can write nothing), and nothing
  would ever stop them or remove their definitions. The leader now destroys and undefines every
  domain Hydra places on another host (or on none), detaches the vdisks of guests that live
  elsewhere, and leaves alone anything Hydra places here and anything it has no row for.
* **A quarantine is lifted only after a stretch of clean passes**, so a host whose storage comes
  and goes does not flip between DEGRADED and NORMAL at probe speed.

Run with:  python -m unittest test_host_states
"""

import importlib.util
import io
import json
import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))


def load_mipha():
    spec = importlib.util.spec_from_file_location("mipha_host_states", os.path.join(HERE, "mipha.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mipha = load_mipha()
IP = "10.0.0.2"


class TheStaleGuestPlan(unittest.TestCase):
    ROWS = {
        "mine": {"host_ip": IP, "state": "Running"},
        "moved": {"host_ip": "10.0.0.3", "state": "Running"},
        "unplaced": {"host_ip": "", "state": "Stopped"},
        "nulled": {"host_ip": None, "state": "Stopped"},
    }

    def plan(self, *domains):
        return mipha.stale_guest_plan([{"name": n, "state": s} for n, s in domains], self.ROWS, IP)

    def test_a_running_guest_placed_elsewhere_is_destroyed_and_its_definition_removed(self):
        plan = self.plan(("moved", "running"))
        self.assertEqual((plan["destroy"], plan["undefine"]), (["moved"], ["moved"]))

    def test_a_stopped_stale_definition_is_only_undefined(self):
        plan = self.plan(("moved", "shut off"))
        self.assertEqual((plan["destroy"], plan["undefine"]), ([], ["moved"]))

    def test_a_guest_placed_here_is_left_alone(self):
        plan = self.plan(("mine", "running"))
        self.assertEqual((plan["destroy"], plan["undefine"], plan["left"]), ([], [], ["mine"]))

    def test_a_guest_nobody_owns_is_a_ghost_while_a_restart_is_pending(self):
        for name in ("unplaced", "nulled"):
            plan = self.plan((name, "running"))
            self.assertEqual(plan["destroy"], [name], name)

    def test_a_domain_hydra_does_not_know_is_not_ours_to_touch(self):
        plan = self.plan(("hand-made", "running"))
        self.assertEqual((plan["destroy"], plan["undefine"], plan["left"]), ([], [], ["hand-made"]))

    def test_every_live_state_is_destroyed_not_only_running(self):
        for state in ("running", "paused", "in shutdown", "crashed", "idle"):
            self.assertEqual(self.plan(("moved", state))["destroy"], ["moved"], state)

    def test_only_a_guests_own_disks_are_detached(self):
        attached = [{"vdisk_id": "moved-disk0"}, {"vdisk_id": "moved-disk1"},
                    {"vdisk_id": "mine-disk0"}, {"vdisk_id": "img-ubuntu"},
                    {"vdisk_id": "stranger-disk0"}, {"vdisk_id": "unplaced-disk0"}]
        self.assertEqual(mipha.stale_vdisks(attached, self.ROWS, IP),
                         ["moved-disk0", "moved-disk1", "unplaced-disk0"])


class Host:
    """The returning host's spark-daemon, as the leader sees it."""

    def __init__(self, domains, attached=()):
        self.domains = domains
        self.attached = list(attached)
        self.calls = []
        self.listing_status = 200

    def request(self, ip, path, payload=None, method="POST"):
        self.calls.append((path, payload))
        assert ip == IP
        if path == "/api/v1/host/domains":
            return self.listing_status, {"domains": self.domains}, "" if self.listing_status == 200 else "down"
        if path == "/api/v1/dfs/vdisk" and payload.get("op") == "list":
            return 200, {"attached": self.attached}, ""
        return 200, {}, ""


class TheReturningHost(unittest.TestCase):
    ROWS = TheStaleGuestPlan.ROWS

    def reconcile(self, host, rows="default"):
        rows = self.ROWS if rows == "default" else rows
        with mock.patch("sys.stdout", io.StringIO()):
            return mipha.reconcile_returning_host("n2", IP, request=host.request, read_rows=lambda: rows)

    def test_it_destroys_then_undefines_then_detaches(self):
        host = Host([{"name": "moved", "state": "running"}, {"name": "mine", "state": "running"}],
                    attached=[{"vdisk_id": "moved-disk0"}, {"vdisk_id": "mine-disk0"}])
        done = self.reconcile(host)
        self.assertEqual(done, ["destroy moved", "undefine moved", "detach moved-disk0"])
        self.assertIn(("/api/v1/vm/moved/power", {"action": "destroy"}), host.calls)
        self.assertIn(("/api/v1/vm/undefine", {"name": "moved", "keep_nvram": False}), host.calls)
        self.assertIn(("/api/v1/dfs/vdisk", {"op": "detach", "vdisk_id": "moved-disk0"}), host.calls)
        self.assertFalse([c for c in host.calls if "mine" in str(c) and "list" not in str(c)
                          and c[0] != "/api/v1/host/domains"], "a guest placed here is untouched")

    def test_an_unreadable_database_destroys_nothing(self):
        host = Host([{"name": "moved", "state": "running"}])
        self.assertIsNone(self.reconcile(host, rows=None))
        self.assertEqual([c[0] for c in host.calls], ["/api/v1/host/domains"])

    def test_a_host_that_cannot_list_its_domains_is_not_reconciled(self):
        host = Host([])
        host.listing_status = 503
        self.assertIsNone(self.reconcile(host))
        self.assertEqual(len(host.calls), 1)

    def test_a_failed_destroy_is_reported_and_the_rest_still_runs(self):
        host = Host([{"name": "moved", "state": "running"}], attached=[{"vdisk_id": "moved-disk0"}])
        real = host.request

        def failing(ip, path, payload=None, method="POST"):
            if path.endswith("/power"):
                return 409, {}, "busy"
            return real(ip, path, payload, method)

        with mock.patch("sys.stdout", io.StringIO()):
            done = mipha.reconcile_returning_host("n2", IP, request=failing, read_rows=lambda: self.ROWS)
        self.assertEqual(done[0], "destroy moved (failed: busy)")
        self.assertIn("detach moved-disk0", done)

    def test_the_rejoin_sequence_runs_it_before_the_services_are_started(self):
        src = open(os.path.join(HERE, "mipha.py"), encoding="utf-8").read()
        rejoin = src[src.index("# A1. Set host status to RECOVERING"):]
        self.assertLess(rejoin.index("reconcile_returning_host(hostname, ip)"),
                        rejoin.index("# B. Start all hypervisor services on the returning host"))


class AQuarantineIsLiftedSlowly(unittest.TestCase):
    def test_it_takes_the_whole_stretch_of_clean_passes(self):
        state = {}
        results = [mipha.quarantine_may_lift(state, True, 3) for _ in range(3)]
        self.assertEqual(results, [False, False, True])

    def test_one_bad_pass_starts_the_count_again(self):
        state = {}
        mipha.quarantine_may_lift(state, True, 3)
        mipha.quarantine_may_lift(state, True, 3)
        self.assertFalse(mipha.quarantine_may_lift(state, False, 3))
        self.assertFalse(mipha.quarantine_may_lift(state, True, 3))
        self.assertEqual(state["clean_passes"], 1)

    def test_a_flapping_host_is_never_lifted(self):
        state = {}
        lifted = [mipha.quarantine_may_lift(state, clean, 6)
                  for clean in [True, True, False] * 10]
        self.assertFalse(any(lifted))

    def test_the_default_is_a_minute_at_the_default_interval(self):
        settings = mipha.DEFAULT_FENCING_CONFIG["self_fence"]
        self.assertEqual(settings["quarantine_lift_after_clean_passes"] * settings["interval_seconds"], 60)

    def test_the_loop_uses_it(self):
        src = open(os.path.join(HERE, "mipha.py"), encoding="utf-8").read()
        self.assertIn("quarantine_may_lift(\n                        SELF_FENCE_STATE, local_health_is_clean(probe),", src)


class TheSelfFenceDecisionTable(unittest.TestCase):
    """probe + circumstances -> none / quarantine / fence, for every row of docs/host_states.md."""

    def decide(self, probe, counters=None, hosts=3, uptime=1000, maintenance=False, peer=True,
               enabled=True, passes=1):
        config = {"self_fence": {"enabled": enabled, "threshold": 3, "grace_seconds": 180}}
        counters = {} if counters is None else counters
        with mock.patch.object(mipha, "host_is_in_maintenance", lambda: maintenance), \
                mock.patch.object(mipha, "healthy_peer_exists", lambda hosts=None: peer):
            result = None
            for _ in range(passes):
                result = mipha.self_fence_decide(
                    probe, counters, config, [{"ip": str(i)} for i in range(hosts)], uptime)
            return result[0]

    @staticmethod
    def probe(libvirt="ok", storage="ok", unserviceable=()):
        return {"libvirt": libvirt, "storage": storage, "unserviceable": list(unserviceable),
                "detail": {"libvirt": "x", "storage": "y"}}

    BAD_DISK = [{"resource": "v-disk0", "cause": "drain-failed", "detail": "d"}]

    def test_a_healthy_host_does_nothing(self):
        self.assertEqual(self.decide(self.probe(), passes=5), "none")

    def test_one_or_two_failed_probes_are_a_blip(self):
        for passes in (1, 2):
            self.assertEqual(self.decide(self.probe(storage="failed"), passes=passes), "none")

    def test_three_failed_storage_probes_quarantine(self):
        self.assertEqual(self.decide(self.probe(storage="failed"), passes=3), "quarantine")

    def test_three_failed_libvirt_probes_quarantine_and_do_not_fence(self):
        self.assertEqual(self.decide(self.probe(libvirt="failed"), passes=3), "quarantine")

    def test_unknown_never_counts(self):
        self.assertEqual(self.decide(self.probe(libvirt="unknown", storage="unknown"), passes=10), "none")

    def test_an_unserviceable_vdisk_for_three_passes_fences_when_a_peer_can_take_the_guests(self):
        self.assertEqual(self.decide(self.probe(unserviceable=self.BAD_DISK), passes=3), "fence")

    def test_the_same_with_no_peer_answering_only_quarantines(self):
        self.assertEqual(self.decide(self.probe(unserviceable=self.BAD_DISK), peer=False, passes=3),
                         "quarantine")

    def test_a_host_in_maintenance_is_exempt(self):
        self.assertEqual(self.decide(self.probe(unserviceable=self.BAD_DISK), maintenance=True, passes=9), "none")

    def test_a_single_node_cluster_never_self_fences(self):
        self.assertEqual(self.decide(self.probe(unserviceable=self.BAD_DISK), hosts=1, passes=9), "none")

    def test_nothing_fires_inside_the_startup_grace(self):
        self.assertEqual(self.decide(self.probe(unserviceable=self.BAD_DISK), uptime=60, passes=9), "none")

    def test_it_can_be_switched_off(self):
        self.assertEqual(self.decide(self.probe(unserviceable=self.BAD_DISK), enabled=False, passes=9), "none")

    def test_a_clean_pass_wipes_the_history(self):
        counters = {}
        self.decide(self.probe(storage="failed"), counters=counters, passes=2)
        self.decide(self.probe(), counters=counters)
        self.assertEqual(self.decide(self.probe(storage="failed"), counters=counters, passes=2), "none")


class TheAnnouncement(unittest.TestCase):
    def test_a_fence_that_took_is_fenced_and_one_that_did_not_is_degraded(self):
        state = mipha.SELF_FENCE_STATE
        saved = dict(state)
        try:
            state["report"] = {"fenced": True}
            self.assertEqual(mipha.self_fence_announcement(), "FENCED")
            state["report"] = {"fenced": False}
            self.assertEqual(mipha.self_fence_announcement(), "DEGRADED")
        finally:
            state.clear()
            state.update(saved)


class TheSparkEndpointsItUses(unittest.TestCase):
    def test_the_domain_list_is_a_typed_endpoint_and_parses_two_word_states(self):
        spec = importlib.util.spec_from_file_location("spark_host_states", os.path.join(HERE, "spark_daemon_decoded.py"))
        spark = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(spark)
        text = " Id   Name    State\n-----------------------\n 1    a       running\n -    b       shut off\n"
        self.assertEqual(spark.parse_virsh_list(text),
                         [{"name": "a", "state": "running"}, {"name": "b", "state": "shut off"}])
        src = open(os.path.join(HERE, "spark_daemon_decoded.py"), encoding="utf-8").read()
        self.assertIn('if path == "/api/v1/host/domains":', src)


if __name__ == "__main__":
    unittest.main()
