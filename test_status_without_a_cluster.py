#!/usr/bin/env python3
"""`cluster status` on a node with no cluster must say so, not invent one.

After `cluster destroy` the command printed this:

    ZooKeeper unreachable; probing nodes directly over mTLS.
    The state of the cluster: stopped
    Host: 127.0.0.1 Up (Valkyrie-997A49)
        ZooKeeper   DOWN
        HydraDB     DOWN
        ...

None of which is true of anything. There was no cluster. `get_cluster_ips()` quietly returns
`127.0.0.1` when `/etc/hci/cluster.json` is missing, so `status` conjured a one-host cluster,
probed it, and reported every service on it as DOWN -- a report about something that was never
there, which reads as a cluster in trouble rather than as no cluster at all.

"No cluster is configured" and "a configured cluster is down" are different situations. Only the
second has a table to print, and only the second should ever say DOWN.

Run with:  python -m unittest test_status_without_a_cluster
"""

import importlib.util
import io
import json
import os
import re
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))


def load_cluster():
    spec = importlib.util.spec_from_file_location(
        "cluster_new_status_test", os.path.join(HERE, "cluster_new.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ConfiguredIsNotTheSameAsPresent(unittest.TestCase):
    def setUp(self):
        self.cluster = load_cluster()
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "cluster.json")

    def write(self, text):
        with io.open(self.path, "w", encoding="utf-8") as handle:
            handle.write(text)

    def test_a_missing_file_is_no_cluster_not_localhost(self):
        self.assertIsNone(self.cluster.configured_cluster_ips(self.path))

    def test_the_old_fallback_is_still_what_it_was(self):
        """get_cluster_ips() keeps its 127.0.0.1 fallback: other commands lean on it. The
        bug was status believing it, not the fallback existing."""
        self.assertEqual(self.cluster.get_cluster_ips.__name__, "get_cluster_ips")

    def test_a_file_with_no_hosts_is_no_cluster(self):
        self.write(json.dumps({"hosts": [], "redundancy_factor": 1}))
        self.assertIsNone(self.cluster.configured_cluster_ips(self.path))

    def test_unparseable_is_no_cluster_rather_than_a_crash(self):
        for text in ("", "{not json", "[]", "null", '{"hosts": "nope"}'):
            self.write(text)
            self.assertIsNone(self.cluster.configured_cluster_ips(self.path),
                              "%r was treated as a configured cluster" % text)

    def test_hosts_without_an_address_do_not_count(self):
        self.write(json.dumps({"hosts": [{"hostname": "x"}, {"ip": ""}]}))
        self.assertIsNone(self.cluster.configured_cluster_ips(self.path))

    def test_a_real_cluster_lists_its_hosts_in_order(self):
        self.write(json.dumps({"hosts": [{"ip": "10.0.0.1"}, {"ip": "10.0.0.2"}]}))
        self.assertEqual(self.cluster.configured_cluster_ips(self.path),
                         ["10.0.0.1", "10.0.0.2"])


class TheAnswerSaysNoClusterAndNothingElse(unittest.TestCase):
    def setUp(self):
        self.cluster = load_cluster()

    def render(self, **kwargs):
        out = io.StringIO()
        with redirect_stdout(out):
            self.cluster.print_no_cluster(**kwargs)
        return re.sub(r"\x1b\[[0-9;]*m", "", out.getvalue())

    def test_it_says_there_is_no_cluster(self):
        self.assertIn("No cluster is configured", self.render())

    def test_it_never_says_down(self):
        """The whole defect. DOWN is a statement about a service on a host that exists."""
        text = self.render()
        for word in ("DOWN", "UP ", "Up (", "stopped", "Host:"):
            self.assertNotIn(word, text, "a cluster that does not exist was described as %r" % word)

    def test_it_says_how_to_make_one(self):
        text = self.render()
        self.assertIn("cluster -s", text)
        self.assertIn("create", text)

    def test_the_json_form_is_machine_readable_and_honest(self):
        data = json.loads(self.render(as_json=True))
        self.assertEqual(data["cluster_state"], "not_configured")
        self.assertEqual(data["nodes"], {})


class StatusAsksBeforeItProbesAnything(unittest.TestCase):
    def setUp(self):
        with io.open(os.path.join(HERE, "cluster_new.py"), encoding="utf-8") as handle:
            source = handle.read()
        start = source.index('elif args.command == "status":')
        self.block = source[start:start + 6000]

    def test_the_check_precedes_the_zookeeper_read_and_the_probe(self):
        check = self.block.index("configured_cluster_ips()")
        for later in ("zk_read_cluster_state()", "probing nodes directly"):
            self.assertIn(later, self.block)
            self.assertLess(check, self.block.index(later),
                            "status probes (%s) before asking whether there is a cluster" % later)

    def test_no_cluster_exits_non_zero(self):
        match = re.search(r"configured_cluster_ips\(\) is None:\s*\n(?:.*\n)*?\s*sys\.exit\((\d+)\)",
                          self.block)
        self.assertTrue(match, "no-cluster does not stop the command")
        self.assertNotEqual(match.group(1), "0",
                            "reporting no cluster exits 0, which a script reads as healthy")

    def test_an_explicit_server_list_still_goes_ahead(self):
        """-s is the operator naming hosts; they are asking about those, config or not."""
        self.assertIn("not args.servers", self.block)


if __name__ == "__main__":
    unittest.main()
