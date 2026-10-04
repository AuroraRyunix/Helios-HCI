#!/usr/bin/env python3
"""A quarantine that outlived the process that set it.

Mipha quarantines a host whose storage keeps failing its probe by writing DEGRADED into
hydra.nodes, and lifts it when the same process sees the host healthy again -- remembering the
quarantine in memory. Every rollout restarts Mipha. A host quarantined while its sidon was being
restarted stayed DEGRADED for hours after storage came back, which refuses it new placement and, as
a result, as a maintenance target ("cool, degraded host, can't enter maintenance mode either").

Run with:  python -m unittest test_mipha_orphaned_quarantine
"""

import importlib.util
import io
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load():
    spec = importlib.util.spec_from_file_location("mipha_orphan_under_test", os.path.join(HERE, "mipha.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class Rig(object):
    def __init__(self, status, quarantined=False, fence_active=False):
        self.m = load()
        self.announced = []
        self.m.local_hostname = lambda: "host-a"
        self.m.run_cql_query = lambda cql: (
            (0, json.dumps({"status": status}), "") if status is not None else (1, "", "down"))
        self.m.announce_node_status = lambda target: self.announced.append(target) or True
        self.m.self_fence_is_active = lambda: fence_active
        self.m.SELF_FENCE_STATE["quarantined"] = quarantined


class OrphanedQuarantineIsCleared(unittest.TestCase):
    def test_a_degraded_host_this_process_did_not_quarantine_is_returned_to_normal(self):
        rig = Rig("DEGRADED")
        self.assertTrue(rig.m.clear_orphaned_quarantine())
        self.assertEqual(rig.announced, ["NORMAL"])

    def test_a_quarantine_this_process_is_holding_is_left_alone(self):
        rig = Rig("DEGRADED", quarantined=True)
        self.assertFalse(rig.m.clear_orphaned_quarantine())
        self.assertEqual(rig.announced, [])

    def test_an_active_fence_is_left_alone(self):
        rig = Rig("DEGRADED", fence_active=True)
        self.assertFalse(rig.m.clear_orphaned_quarantine())
        self.assertEqual(rig.announced, [])

    def test_other_statuses_are_never_touched(self):
        for status in ("NORMAL", "FENCED", "MAINTENANCE", "OFFLINE"):
            rig = Rig(status)
            self.assertFalse(rig.m.clear_orphaned_quarantine(), status)
            self.assertEqual(rig.announced, [], status)

    def test_an_unreadable_row_changes_nothing(self):
        rig = Rig(None)
        self.assertFalse(rig.m.clear_orphaned_quarantine())
        self.assertEqual(rig.announced, [])


class TheWatchdogLoopUsesIt(unittest.TestCase):
    def test_a_clean_pass_checks_for_an_orphan_on_a_schedule(self):
        with io.open(os.path.join(HERE, "mipha.py"), encoding="utf-8") as handle:
            text = handle.read()
        loop = text[text.index("def self_fence_loop():"):text.index("def report_fence_status():")]
        self.assertIn("elif local_health_is_clean(probe):", loop)
        self.assertIn("clear_orphaned_quarantine()", loop)
        self.assertIn("since_orphan_check = ORPHAN_CHECK_EVERY", loop,
                      "the first clean pass after a restart should look, not wait a minute")

    def test_a_healthy_probe_is_defined_like_the_fences_own_recovery(self):
        rig = Rig("NORMAL")
        clean = {"unserviceable": [], "libvirt": "ok", "storage": "ok"}
        self.assertTrue(rig.m.local_health_is_clean(clean))
        self.assertFalse(rig.m.local_health_is_clean(dict(clean, storage="failed")))
        self.assertFalse(rig.m.local_health_is_clean(dict(clean, libvirt="failed")))
        self.assertFalse(rig.m.local_health_is_clean(dict(clean, unserviceable=["x"])))


if __name__ == "__main__":
    unittest.main()
