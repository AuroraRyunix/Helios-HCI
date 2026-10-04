#!/usr/bin/env python3
"""`cluster stop` shuts the guests down together and not one after another.

It used to ask each guest to shut down, poll it five times a second apart, and destroy it if
it was still running -- then move to the next guest. A dozen guests that ignore the request
cost a minute of nothing, and a guest that needed ten seconds to go down cleanly never got
them. Now every guest is asked at once, polled together against one deadline, and the ones
still running when it passes are powered off together.

Run with:  python -m unittest test_cluster_stop
"""

import importlib.util
import os
import sys
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_cli():
    spec = importlib.util.spec_from_file_location(
        "cluster_new_under_test", os.path.join(HERE, "cluster_new.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules["cluster_new_under_test"] = module
    spec.loader.exec_module(module)
    return module


cli = load_cli()


class Host:
    """Guests that shut down after a given number of polls (None: never)."""

    def __init__(self, shut_after):
        self.shut_after = dict(shut_after)
        self.polls = {}
        self.commands = []
        self.lock = threading.Lock()

    def run(self, ip, command, timeout=None):
        verb, _, name = command.partition(" ")[2].partition(" ")
        name = name.strip("'")
        with self.lock:
            self.commands.append((ip, command))
            if command.startswith("virsh domstate"):
                self.polls[name] = self.polls.get(name, 0) + 1
                after = self.shut_after.get(name)
                down = after is not None and self.polls[name] >= after
                return 0, "shut off" if down else "running", ""
        return 0, "", ""


class StopVmsTogether(unittest.TestCase):
    def stop(self, vms, shut_after, grace=0.5):
        host = Host(shut_after)
        rows = []
        destroyed = cli.stop_vms_together(
            [{"name": n, "host_ip": ip, "state": "Running"} for n, ip in vms],
            grace=grace, runner=host.run, sleep=lambda s: time.sleep(0.01),
            update_row=rows.append)
        return host, rows, destroyed

    def verbs(self, host):
        return [c.split()[1] for _, c in host.commands]

    def test_every_guest_is_asked_to_shut_down_before_any_is_polled(self):
        host, _, _ = self.stop([("a", "10.0.0.1"), ("b", "10.0.0.2"), ("c", "10.0.0.1")],
                               {"a": 1, "b": 1, "c": 1})
        verbs = self.verbs(host)
        last_shutdown = max(i for i, v in enumerate(verbs) if v == "shutdown")
        first_poll = min(i for i, v in enumerate(verbs) if v == "domstate")
        self.assertEqual(verbs.count("shutdown"), 3)
        self.assertLess(last_shutdown, first_poll)

    def test_a_guest_that_shuts_down_in_time_is_not_powered_off(self):
        host, rows, destroyed = self.stop([("a", "10.0.0.1"), ("b", "10.0.0.2")], {"a": 2, "b": 1})
        self.assertEqual(destroyed, [])
        self.assertNotIn("destroy", self.verbs(host))
        self.assertEqual(sorted(rows), ["a", "b"])

    def test_one_deadline_covers_all_guests_and_the_stragglers_are_powered_off_together(self):
        vms = [("g%d" % i, "10.0.0.1") for i in range(6)]
        start = time.time()
        host, rows, destroyed = self.stop(vms, {}, grace=0.3)
        elapsed = time.time() - start
        self.assertEqual(sorted(destroyed), sorted(n for n, _ in vms))
        self.assertEqual(self.verbs(host).count("destroy"), 6)
        self.assertLess(elapsed, 2.0, "the grace period must be shared, not per guest")
        self.assertEqual(sorted(rows), sorted(n for n, _ in vms))

    def test_only_the_guest_that_ignores_the_request_is_destroyed(self):
        host, _, destroyed = self.stop([("good", "10.0.0.1"), ("stuck", "10.0.0.1")],
                                       {"good": 1}, grace=0.2)
        self.assertEqual(destroyed, ["stuck"])

    def test_a_guest_without_a_host_is_skipped_and_nothing_to_do_is_quiet(self):
        host, rows, destroyed = self.stop([("a", "")], {})
        self.assertEqual((host.commands, rows, destroyed), ([], [], []))

    def test_names_are_quoted_for_the_remote_shell(self):
        host, _, _ = self.stop([("odd name;rm", "10.0.0.1")], {"odd name;rm": 1})
        self.assertTrue(all("'odd name;rm'" in c for _, c in host.commands), host.commands)

    def test_the_stop_command_uses_it(self):
        src = open(os.path.join(HERE, "cluster_new.py"), encoding="utf-8").read()
        self.assertIn("stop_vms_together(running_vms)", src)
        self.assertNotIn("Poll up to 5 seconds", src)


if __name__ == "__main__":
    unittest.main()
