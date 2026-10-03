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


if __name__ == "__main__":
    unittest.main()
