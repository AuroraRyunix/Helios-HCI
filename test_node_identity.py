#!/usr/bin/env python3
"""A node has to know its own address, and every path that builds one must say so.

`/etc/hci/spectrum/spectrum.env` is where a node learns what IP it is. Eleven Python
modules and the Phoenix console read `LOCAL_HYPERVISOR_IP` out of it, and every one of
them falls back to `127.0.0.1` when the key is absent.

That fallback is not a degraded mode, it is a cluster outage waiting for an election.
`vali`'s Catalyst queue worker runs only on the ZooKeeper leader, and it decides whether
it *is* the leader by comparing the leader's address with its own. A node that believes
it is `127.0.0.1` can never match, so it never drains the queue -- and because the leader
is the only worker, leadership landing on such a node stops every VM power, migrate and
DRS task in the cluster. Each one still returns, eventually, as a timeout, which is
indistinguishable from a slow cluster.

Two writers used to disagree about this file: `provision.py` wrote `SPECTRUM_HOST`,
`SPECTRUM_PORT` and `SPECTRUM_LOG_LEVEL`, and `cluster create` wrote `SPECTRUM_API_PORT`,
`LOCAL_HYPERVISOR_IP` and `CLUSTER_SEEDS` over the top of it. Create ran second, so a
created cluster worked and hid the fault; `cluster add-node` runs only the first, so
every node ever *added* came up anonymous. Nothing read any of the other five keys.

Run with:  python -m unittest test_node_identity
"""

import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

ENV_PATH = "/etc/hci/spectrum/spectrum.env"
IDENTITY = "LOCAL_HYPERVISOR_IP"

# Every file that writes the env file, and the fragment each one uses to name the target.
WRITERS = ("provision.py", "cluster_new.py", "spark_daemon_decoded.py")


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def written_env_content(name):
    """The content each writer actually puts in the env file.

    Only the written literals, never the surrounding comments -- the comments name the
    keys that were removed, and matching those would make this test contradict itself.
    """
    source = read(name)
    literals = re.findall(r'spectrum_env\s*=\s*f?["\'](.*?)["\']', source, re.S)
    literals += re.findall(
        r'write_file\(\s*["\']%s["\']\s*,\s*f?["\'](.*?)["\']' % re.escape(ENV_PATH),
        source, re.S)
    return " ".join(literals)


class EveryWriterNamesTheNode(unittest.TestCase):
    """The one key anything reads has to be in everything anyone writes."""

    def test_each_writer_emits_the_address(self):
        for name in WRITERS:
            self.assertIn(
                ENV_PATH, read(name),
                "%s no longer writes the env file; this test needs updating" % name)
            self.assertIn(
                IDENTITY, written_env_content(name),
                "%s writes %s without %s, so a node built by that path cannot recognise "
                "itself as the ZooKeeper leader and will never drain the Catalyst queue"
                % (name, ENV_PATH, IDENTITY))

    def test_no_writer_reintroduces_a_key_nothing_reads(self):
        """The disagreement between the two writers was the bug. Keys with no reader are
        how they drifted apart in the first place."""
        for name in WRITERS:
            content = written_env_content(name)
            for dead in ("SPECTRUM_API_PORT", "CLUSTER_SEEDS", "SPECTRUM_LOG_LEVEL",
                         "SPECTRUM_HOST", "SPECTRUM_PORT"):
                self.assertNotIn(
                    dead, content,
                    "%s writes %s, which nothing anywhere reads" % (name, dead))


class AddingANodeGivesItAnIdentity(unittest.TestCase):
    def test_add_node_writes_the_address_itself(self):
        """`provision.py --join` writes it, but a node provisioned by an older toolkit has
        the version that carried no address, and the join has to be self-sufficient."""
        source = read("cluster_new.py")
        start = source.index("def cmd_add_node(")
        body = source[start:source.index("\ndef ", start + 10)]
        self.assertIn(IDENTITY, body)
        self.assertIn(ENV_PATH, body)

    def test_it_happens_before_the_ring_join(self):
        """A node that does not know itself should not be given cluster responsibilities."""
        source = read("cluster_new.py")
        start = source.index("def cmd_add_node(")
        body = source[start:source.index("\ndef ", start + 10)]
        self.assertLess(
            body.index(IDENTITY), body.index("write_zookeeper_ensemble"),
            "the node joins the ensemble before it is told its own address")


class TheRolloutRepairsNodesBuiltBeforeTheFix(unittest.TestCase):
    def test_the_rollout_sets_the_address_on_every_node(self):
        source = read("deploy_updates.py")
        self.assertIn(
            'want="%s=%%s"' % IDENTITY, source,
            "the rollout does not repair a node whose env file does not name it, so "
            "nodes added by the old add-node path stay anonymous forever")

    def test_the_repair_leaves_other_lines_alone(self):
        """A rollout runs against live clusters; it fixes the key, it does not take over
        the file."""
        source = read("deploy_updates.py")
        self.assertIn('grep -v "^%s=" "$env"' % IDENTITY, source)


class TheFailureCannotBeSilent(unittest.TestCase):
    def test_vali_warns_when_it_does_not_know_its_address(self):
        source = read("vali.py")
        self.assertIn('if LOCAL_IP == "127.0.0.1":', source)
        self.assertIn("never act as the Catalyst queue worker", source)

    def test_vali_reports_whether_it_is_the_worker(self):
        """"The worker is busy" and "no worker is running anywhere" look identical from
        the outside, and only one of them is an outage."""
        source = read("vali.py")
        self.assertIn("was_worker", source)
        self.assertIn("standing by", source)

    def test_valis_unit_does_not_buffer_its_output(self):
        """Its diagnostics sat in a 4 KB buffer for the life of the process, which is how
        a worker that never claimed a task left no trace in the journal."""
        for name in ("deploy_updates.py", "provision.py"):
            source = read(name)
            unit = source[source.index("Description=Vali Audit Log"):]
            unit = unit[:unit.index("[Install]")]
            self.assertIn(
                "PYTHONUNBUFFERED=1", unit,
                "%s installs vali with a block-buffered stdout" % name)


if __name__ == "__main__":
    unittest.main()
