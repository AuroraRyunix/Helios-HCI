#!/usr/bin/env python3
"""Three things a freshly created cluster's own health check reported, found by running create.

  * the hydra keyspace was replicated correctly and the check said it could not read it, because
    it recognised only SimpleStrategy's spelling of the factor;
  * nodes could not ssh to each other, because only provisioning seeds known_hosts and create did
    not;
  * the service watchdog was not running, because spark-daemon's startup thread returned when it
    found no cluster (every destroy restarts the daemon) and the watchdog is the tail of that
    thread.

Run with:  python -m unittest test_create_health_findings
"""

import ast
import importlib.util
import io
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


class ReplicationCheckReadsBothStrategies(unittest.TestCase):
    def pattern(self):
        """The factor pattern, taken from the check itself so this cannot drift from it."""
        for node in ast.walk(ast.parse(read("mcli-runner"))):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "findall" and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and "\d+" in str(node.args[0].value)):
                return node.args[0].value
        self.fail("the replication check no longer extracts factors with findall")

    def factors(self, text):
        return [int(n) for n in re.findall(self.pattern(), text)]

    def test_network_topology_strategy(self):
        out = "{'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '3'}"
        self.assertEqual(self.factors(out), [3])

    def test_simple_strategy(self):
        out = "{'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '3'}"
        self.assertEqual(self.factors(out), [3])

    def test_a_different_factor_is_not_three(self):
        out = "{'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '1'}"
        self.assertNotIn(3, self.factors(out))


class PeerHostKeysAreSeededIdempotently(unittest.TestCase):
    def setUp(self):
        self.bash = shutil.which("bash")
        if not self.bash:
            self.skipTest("no bash on this machine")
        spec = importlib.util.spec_from_file_location("cn_hk", os.path.join(HERE, "cluster_new.py"))
        self.m = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = self.m
        spec.loader.exec_module(self.m)
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, True)

    def run_once(self, ips):
        command = self.m.peer_host_key_command(ips).replace("/root/.ssh", self.dir.replace("\\", "/"))
        # Stand-ins for the two tools: keygen succeeds when the file already names the host,
        # keyscan prints a line naming it.
        prelude = ('ssh-keygen() { grep -q "$2" "$4"; }\n'
                   'ssh-keyscan() { echo "scanned-$2"; }\n')
        done = subprocess.run([self.bash, "-c", prelude + command],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertEqual(done.returncode, 0, done.stderr.decode("utf-8", "replace"))
        with io.open(os.path.join(self.dir, "known_hosts"), encoding="utf-8") as handle:
            return handle.read().split()

    def test_every_peer_is_added(self):
        lines = self.run_once(["10.0.0.1", "10.0.0.2", "10.0.0.3"])
        self.assertEqual(sorted(lines), ["scanned-10.0.0.1", "scanned-10.0.0.2", "scanned-10.0.0.3"])

    def test_running_it_again_adds_nothing(self):
        self.run_once(["10.0.0.1", "10.0.0.2"])
        again = self.run_once(["10.0.0.1", "10.0.0.2"])
        self.assertEqual(len(again), 2, "a second run duplicated entries")

    def test_an_existing_entry_is_never_replaced(self):
        with io.open(os.path.join(self.dir, "known_hosts"), "w", encoding="utf-8") as handle:
            handle.write("10.0.0.1 pinned-key\n")
        lines = self.run_once(["10.0.0.1", "10.0.0.2"])
        self.assertIn("pinned-key", lines)
        self.assertNotIn("scanned-10.0.0.1", lines)

    def test_create_runs_it_before_the_disk_phase(self):
        text = read("cluster_new.py")
        seed = text.index("peer_host_key_command(ips)", text.index("Phase 2: Hostname"))
        self.assertLess(seed, text.index("--- Phase 3: Dynamic Disk Scan"))


class AutostartNeverEndsBeforeTheWatchdog(unittest.TestCase):
    def function(self):
        for node in ast.walk(ast.parse(read("spark_daemon_decoded.py"))):
            if isinstance(node, ast.FunctionDef) and node.name == "check_cluster_and_autostart":
                return node
        self.fail("check_cluster_and_autostart is gone")

    def test_it_has_no_early_return(self):
        """The watchdog is the end of this function. A `return` anywhere ahead of it is a way for
        a host to run without one, which is what happened after every destroy."""
        returns = [n.lineno for n in ast.walk(self.function()) if isinstance(n, ast.Return)]
        self.assertEqual(returns, [], "check_cluster_and_autostart returns at lines %s" % returns)

    def test_it_waits_for_a_cluster_instead(self):
        body = ast.get_source_segment(read("spark_daemon_decoded.py"), self.function())
        self.assertIn('os.path.exists("/etc/hci/cluster.json")', body)
        self.assertLess(body.index("time.sleep(10)"), body.index("Starting service health watchdog..."))


class DestroyWaitsForTheDaemonItRestarted(unittest.TestCase):
    """`cluster create` straight after `cluster destroy` failed on two nodes with "Remote end
    closed connection", because destroy returned while the detached spark-daemon restart was
    still in flight."""

    def load(self):
        spec = importlib.util.spec_from_file_location("cn_wait", os.path.join(HERE, "cluster_new.py"))
        m = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = m
        spec.loader.exec_module(m)
        return m

    def clock(self):
        state = {"t": 0.0}
        return state, (lambda s: state.__setitem__("t", state["t"] + s)), (lambda: state["t"])

    def test_a_daemon_that_drops_out_and_returns_is_waited_for(self):
        m = self.load()
        state, sleep, now = self.clock()
        # After the grace period the daemon is mid-restart: not answering, one stray answer
        # (a connection accepted just before the process exits), not answering, then back.
        script = {"10.0.0.1": [False, False, True, False, True, True, True]}

        def probe(ip):
            seq = script[ip]
            return seq.pop(0) if len(seq) > 1 else seq[0]

        missing = m.wait_for_spark_daemons(["10.0.0.1"], probe=probe, sleep=sleep, now=now,
                                           say=lambda *_: None)
        self.assertEqual(missing, [])
        self.assertLessEqual(len(script["10.0.0.1"]), 1, "it stopped probing before the daemon was back")

    def test_one_answer_is_not_enough(self):
        m = self.load()
        state, sleep, now = self.clock()
        answers = iter([True, False, True, True])
        missing = m.wait_for_spark_daemons(["10.0.0.1"], probe=lambda ip: next(answers),
                                           sleep=sleep, now=now, say=lambda *_: None)
        self.assertEqual(missing, [])

    def test_a_daemon_that_never_returns_is_reported(self):
        m = self.load()
        state, sleep, now = self.clock()
        missing = m.wait_for_spark_daemons(["10.0.0.1", "10.0.0.2"],
                                           probe=lambda ip: ip == "10.0.0.1",
                                           timeout=20, sleep=sleep, now=now, say=lambda *_: None)
        self.assertEqual(missing, ["10.0.0.2"])

    def test_destroy_uses_it_after_launching_the_restarts(self):
        text = read("cluster_new.py")
        phase = text.index("--- Phase 7: Restarting spark-daemon Services")
        self.assertLess(text.index('["spark-daemon"], detach=True', phase),
                        text.index("wait_for_spark_daemons(ips)", phase))


class ScyllaStartsOneNodeAtATimeSeedFirst(unittest.TestCase):
    """Starting all three at once left two nodes stuck in Raft group 0 on some runs: the seed could
    not translate their Raft ids to addresses, and they never listened."""

    def load(self):
        spec = importlib.util.spec_from_file_location("cn_scylla", os.path.join(HERE, "cluster_new.py"))
        m = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = m
        spec.loader.exec_module(m)
        return m

    def run_it(self, ips, listening_after=None, active=True):
        m = self.load()
        events = []
        listening_after = listening_after or {}
        started = set()

        def restart(ip):
            events.append(("restart", ip))
            started.add(ip)

        def listening(ip):
            if ip not in started:
                return False
            events.append(("listening?", ip))
            return True

        result = m.start_scylla_in_order(
            ips, restart=restart, is_active=lambda ip: active, listening=listening,
            progress=lambda ip: None, sleep=lambda s: None, say=lambda *_: None, listen_seconds=5)
        return result, events

    def test_each_node_is_listening_before_the_next_is_started(self):
        result, events = self.run_it(["10.0.0.1", "10.0.0.2", "10.0.0.3"])
        self.assertIsNone(result)
        restarts = [e for e in events if e[0] == "restart"]
        self.assertEqual([ip for _, ip in restarts], ["10.0.0.1", "10.0.0.2", "10.0.0.3"],
                         "the seed (first address) must be started first")
        for ip, nxt in (("10.0.0.1", "10.0.0.2"), ("10.0.0.2", "10.0.0.3")):
            self.assertLess(events.index(("listening?", ip)), events.index(("restart", nxt)),
                            "%s was started before %s was listening" % (nxt, ip))

    def test_a_node_that_never_listens_stops_the_sequence_and_is_named(self):
        m = self.load()
        restarted = []
        result = m.start_scylla_in_order(
            ["10.0.0.1", "10.0.0.2"], restart=restarted.append, is_active=lambda ip: True,
            listening=lambda ip: ip == "10.0.0.1", progress=lambda ip: None,
            sleep=lambda s: None, say=lambda *_: None, listen_seconds=3)
        self.assertIn("10.0.0.2", result)
        self.assertEqual(restarted, ["10.0.0.1", "10.0.0.2"])

    def test_a_unit_that_does_not_become_active_is_named(self):
        result, _ = self.run_it(["10.0.0.1"], active=False)
        self.assertIn("hydra-db failed to start on 10.0.0.1", result)

    def test_create_no_longer_restarts_it_everywhere_at_once(self):
        text = read("cluster_new.py")
        body = text[text.index("--- Phase 4: Starting the coordination"):text.index("--- Phase 6: Starting Core")]
        self.assertNotIn('unit_action_checked(ips, "restart", ["hydra-db"])', body)
        self.assertIn("start_scylla_in_order(ips)", body)


class TheDaemonLogIsLineBuffered(unittest.TestCase):
    def test_stdout_and_stderr_are_reconfigured_before_anything_prints(self):
        text = read("spark_daemon_decoded.py")
        head = text[:text.index("def ")]
        self.assertIn("reconfigure(line_buffering=True)", head)
        self.assertIn("sys.stdout", head)
        self.assertIn("sys.stderr", head)


class CreateGivesTheChecksTimeToSettle(unittest.TestCase):
    def load(self):
        spec = importlib.util.spec_from_file_location("cn_settle", os.path.join(HERE, "cluster_new.py"))
        m = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = m
        spec.loader.exec_module(m)
        return m

    def run_it(self, outputs, attempts=4):
        m = self.load()
        queue = list(outputs)
        sleeps = []
        result = m.run_health_checks_settled(
            "10.0.0.1", runner=lambda: (0, queue.pop(0) if len(queue) > 1 else queue[0], ""),
            attempts=attempts, interval=20, sleep=sleeps.append, say=lambda *_: None)
        return result, sleeps

    def test_a_check_that_settles_is_not_reported(self):
        (ok, out, failing), sleeps = self.run_it(["[ FAIL ] vip", "[ PASS ] vip"])
        self.assertTrue(ok)
        self.assertEqual(failing, [])
        self.assertEqual(sleeps, [20])

    def test_a_check_that_never_settles_is_reported_after_the_last_attempt(self):
        (ok, out, failing), sleeps = self.run_it(["[ FAIL ] vip"], attempts=3)
        self.assertEqual(len(failing), 1)
        self.assertEqual(sleeps, [20, 20], "it should wait between attempts and not after the last")

    def test_a_clean_first_run_does_not_wait(self):
        (ok, out, failing), sleeps = self.run_it(["[ PASS ] all"])
        self.assertEqual((failing, sleeps), ([], []))

    def test_a_run_that_cannot_execute_is_not_retried(self):
        m = self.load()
        calls = []
        ok, _, _ = m.run_health_checks_settled(
            "10.0.0.1", runner=lambda: calls.append(1) or (1, "", "boom"),
            sleep=lambda s: None, say=lambda *_: None)
        self.assertFalse(ok)
        self.assertEqual(len(calls), 1)

    def test_create_uses_it(self):
        self.assertIn("run_health_checks_settled(ips[0])", read("cluster_new.py"))


if __name__ == "__main__":
    unittest.main()
