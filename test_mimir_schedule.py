#!/usr/bin/env python3
"""Mimir's schedule is claimed with a compare-and-swap, not written blind.

Run with:  python -m unittest test_mimir_schedule
"""

import importlib.util
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load():
    spec = importlib.util.spec_from_file_location("mimir_s", os.path.join(HERE, "mimir.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TheClaim(unittest.TestCase):
    def setUp(self):
        self.m = load()
        self.sent = []

    def lwt(self, result):
        def fake(endpoint, params, timeout=15):
            self.sent.append((endpoint, params))
            return result
        self.m.run_lwt = fake

    def test_it_is_conditional_on_what_was_read(self):
        self.lwt((True, True, {}, ""))
        self.assertTrue(self.m.claim_schedule("hourly_checks", 100, 200))
        self.assertEqual(self.sent, [("/v1/schedule/claim-check", {
            "schedule_name": "hourly_checks", "last_run_epoch": 200, "expected_last_run_epoch": 100})])

    def test_a_null_column_is_claimed_as_null(self):
        self.lwt((True, True, {}, ""))
        self.m.claim_schedule("daily", None, 200)
        self.assertIsNone(self.sent[0][1]["expected_last_run_epoch"])

    def test_a_lost_race_is_a_skip(self):
        self.lwt((True, False, {"last_run_epoch": 150}, ""))
        self.assertFalse(self.m.claim_schedule("hourly_checks", 100, 200))

    def test_an_unreachable_daruk_is_a_skip_not_a_run(self):
        self.lwt((False, False, {}, "Daruk is not answering"))
        self.assertFalse(self.m.claim_schedule("hourly_checks", 100, 200))


class TheInterval(unittest.TestCase):
    def test_a_row_may_say_its_own_and_the_name_is_the_fallback(self):
        m = load()
        self.assertEqual(m.schedule_interval({"schedule_name": "x", "interval_seconds": 120}), 120)
        self.assertEqual(m.schedule_interval({"schedule_name": "hourly_checks"}), 3600)
        self.assertEqual(m.schedule_interval({"schedule_name": "other", "interval_seconds": 0}), 86400)


class TheLoopNoLongerWritesTheClockBlind(unittest.TestCase):
    def test_no_plain_update_of_last_run_epoch(self):
        with open(os.path.join(HERE, "mimir.py"), encoding="utf-8") as h:
            src = h.read()
        self.assertNotIn("UPDATE hydra.mimir_schedules SET last_run_epoch", src)
        self.assertNotIn('s.get("last_run_epoch", 0)', src)


if __name__ == "__main__":
    unittest.main()
