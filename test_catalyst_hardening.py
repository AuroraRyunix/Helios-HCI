#!/usr/bin/env python3
"""Catalyst's HTTP edge: ids are checked before they reach a statement, and a scheduled task is
only queued by the node that holds the dispatch candidacy.

Run with:  python -m unittest test_catalyst_hardening
"""

import importlib.util
import json
import os
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))


def load():
    spec = importlib.util.spec_from_file_location("catalyst_h", os.path.join(HERE, "catalyst.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TheStatusPathTakesOnlyAUuid(unittest.TestCase):
    def setUp(self):
        self.c = load()
        self.statements = []

        def fake(cql, *a, **k):
            self.statements.append(cql)
            return 0, "", ""
        self.c.run_cql_query = fake
        class Plain(self.c.CatalystAPIHandler):
            # The real handler completes an mTLS handshake in setup(); the routes are what is
            # under test here, so the same routes are served over plain loopback.
            def setup(self):
                self.connection = self.request
                self.rfile = self.connection.makefile("rb", self.rbufsize)
                self.wfile = self.connection.makefile("wb", self.wbufsize)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Plain)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def get(self, path):
        try:
            with urllib.request.urlopen(self.base + path, timeout=5) as r:
                return r.status
        except urllib.error.HTTPError as e:
            return e.code

    def test_anything_that_is_not_a_uuid_is_refused_and_asks_nothing(self):
        for bad in ("1;DROP", "abc", "%27%20OR%201%3D1", "123e4567-e89b-12d3-a456-42661417400g",
                    "123e4567-e89b-12d3-a456-4266141740000"):
            self.assertEqual(self.get("/api/v1/tasks/status/" + bad), 400, bad)
        self.assertEqual(self.statements, [], "a malformed id reached the database layer")

    def test_a_uuid_is_queried_as_a_bare_literal(self):
        task = "123e4567-e89b-12d3-a456-426614174000"
        self.c.task_events[task] = threading.Event()
        self.c.task_events[task].set()
        self.get("/api/v1/tasks/status/" + task)
        self.assertTrue(self.statements)
        self.assertIn("task_id = %s;" % task, self.statements[0])


class AScheduledTaskIsQueuedOnlyWhereTheQueuesAre(unittest.TestCase):
    def setUp(self):
        self.c = load()
        self.c.queues["dagur"] = __import__("queue").Queue()

    def task(self):
        return {"task_id": "t-1", "action": "execute", "payload": {}}

    def test_a_scheduler_that_is_not_the_dispatcher_leaves_the_replay_to_the_dispatcher(self):
        self.c.holds_dispatch = lambda: False
        self.assertFalse(self.c.queue_scheduled_task("dagur", self.task()))
        self.assertTrue(self.c.queues["dagur"].empty(), "an orphan queue entry nothing would drain")
        self.assertNotIn("t-1", self.c.queued_task_ids)

    def test_the_dispatcher_queues_it(self):
        self.c.holds_dispatch = lambda: True
        self.assertTrue(self.c.queue_scheduled_task("dagur", self.task()))
        self.assertEqual(self.c.queues["dagur"].get_nowait()["task_id"], "t-1")
        self.assertIn("t-1", self.c.queued_task_ids)

    def test_the_scheduler_loop_uses_it(self):
        with open(os.path.join(HERE, "catalyst.py"), encoding="utf-8") as h:
            src = h.read()
        loop = src[src.index("Triggering Dagur job"):src.index("class CatalystAPIHandler")]
        self.assertIn("queue_scheduled_task(", loop)
        self.assertNotIn("submit_task_to_memory(", loop)


if __name__ == "__main__":
    unittest.main()
