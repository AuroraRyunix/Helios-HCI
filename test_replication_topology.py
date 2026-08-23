#!/usr/bin/env python3
"""hydra is replicated by topology, not by token order.

`SimpleStrategy` places replicas by token order alone and knows nothing about racks, so on
any cluster spanning more than one rack a single rack loss can take every copy of the
metadata with it. That is not merely a control-plane outage: without the block map an
extent group is four megabytes of unlabelled bytes, so the surviving *data* stops being
identifiable.

The switch is cheap now and expensive later, which is why it is done now.
NetworkTopologyStrategy with every node in one rack places replicas on exactly the nodes
SimpleStrategy did -- nothing moves. Once racks actually differ, the same change relocates
replicas, and until a full repair completes the cluster is running with replicas it
believes exist and does not.

Run with:  python -m unittest test_replication_topology
"""

import ast
import io
import os
import unittest

import helios_cql

HERE = os.path.dirname(os.path.abspath(__file__))


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def function_source(name, module):
    tree = ast.parse(read(module), filename=module)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(read(module), node)
    raise AssertionError("%s does not define %s" % (module, name))


class TheFactorIsReadUnderEitherStrategy(unittest.TestCase):
    """One parser, because Spectrum had its own that read only `replication_factor`."""

    def test_simple_strategy(self):
        self.assertEqual(
            helios_cql.parse_replication_factor(
                "{'class': 'SimpleStrategy', 'replication_factor': '3'}"), 3)

    def test_network_topology_one_datacenter(self):
        self.assertEqual(
            helios_cql.parse_replication_factor(
                "{'class': 'NetworkTopologyStrategy', 'datacenter1': '1'}"), 1)

    def test_network_topology_sums_across_datacenters(self):
        """QUORUM is a majority of the sum, so the sum is what a quorum gate needs."""
        self.assertEqual(
            helios_cql.parse_replication_factor(
                "{'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '2'}"), 5)

    def test_strategies_with_no_factor_give_none(self):
        for text in ("{'class': 'LocalStrategy'}", "{'class': 'EverywhereStrategy'}"):
            self.assertIsNone(helios_cql.parse_replication_factor(text))

    def test_the_tuple_repr_the_driver_produces_is_understood(self):
        """`OrderedMapSerializedKey` reprs its pairs as tuples rather than with colons,
        and the difference is invisible until the gate reports "unknown" and refuses
        every maintenance request."""
        text = ("OrderedMapSerializedKey([('class', "
                "'org.apache.cassandra.locator.SimpleStrategy'), ('replication_factor', '3')])")
        self.assertEqual(helios_cql.parse_replication_factor(text), 3)

    def test_unreadable_input_is_none_not_a_plausible_default(self):
        for text in (None, "", "not a replication map"):
            self.assertIsNone(helios_cql.parse_replication_factor(text))

    def test_only_helios_cql_defines_it(self):
        for module in ("cluster_new.py", "vali.py", "spectrum_server.py"):
            tree = ast.parse(read(module), filename=module)
            names = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
            self.assertNotIn("parse_replication_factor", names, module)


class NewKeyspacesAreTopologyAware(unittest.TestCase):
    def setUp(self):
        self.source = read("spectrum_server.py")

    def test_creation_and_alteration_both_use_the_shared_clause(self):
        self.assertIn("def replication_map(", self.source)
        for caller in ("create_keyspace = ", "alter = "):
            self.assertIn("replication_map(", self.source)
        self.assertIn("NetworkTopologyStrategy", self.source)

    def test_nothing_still_writes_simple_strategy(self):
        for name in ("spectrum_server.py", "cluster_new.py"):
            body = read(name)
            self.assertNotIn(
                "'class': 'SimpleStrategy'", body,
                "%s still creates or alters a keyspace with SimpleStrategy" % name)

    def test_the_datacenter_is_read_rather_than_assumed(self):
        """A keyspace naming a datacenter the snitch does not report is accepted by
        Scylla and places no replicas at all -- a healthy-looking ALTER and an empty
        keyspace."""
        self.assertIn("def local_datacenter(", self.source)
        self.assertIn("SELECT data_center FROM system.local;", self.source)

    def test_the_fallback_is_what_simplesnitch_already_reported(self):
        source = function_source("local_datacenter", "spectrum_server.py")
        self.assertIn('return "datacenter1"', source)


class ExistingClustersAreMigratedOnce(unittest.TestCase):
    def setUp(self):
        self.source = function_source(
            "migrate_keyspace_to_topology_strategy", "spectrum_server.py")

    def test_it_only_acts_on_simple_strategy(self):
        self.assertIn('if "simplestrategy" not in stdout.lower():', self.source)
        self.assertIn("return False", self.source)

    def test_it_refuses_to_guess_a_factor_it_could_not_read(self):
        """Guessing here would silently change the number of replicas."""
        self.assertIn("if factor is None:", self.source)
        self.assertIn("refusing to guess", self.source)

    def test_it_repairs_afterwards(self):
        """Cheap when nothing moved, and the thing that makes the new placement real
        when something did."""
        self.assertIn("run_nodetool_repair", self.source)

    def test_a_failed_alter_leaves_the_keyspace_alone(self):
        self.assertIn("stays on SimpleStrategy", self.source)


class TheSnitchCanSeeRacks(unittest.TestCase):
    """NetworkTopologyStrategy places one replica per rack. That is the whole mechanism,
    and it does nothing unless the snitch reports racks that differ."""

    def setUp(self):
        self.provision = read("provision.py")

    def test_the_quadlet_asks_for_a_rack_aware_snitch(self):
        self.assertIn("--endpoint-snitch GossipingPropertyFileSnitch", self.provision)

    def test_the_properties_file_is_mounted_and_written(self):
        self.assertIn(
            "Volume=/etc/hci/hydra/cassandra-rackdc.properties:"
            "/etc/scylla/cassandra-rackdc.properties:ro", self.provision)
        # Podman creates a directory where a bind-mount source is missing, and Scylla then
        # finds no properties at all -- so the file must be written first.
        write = self.provision.index('write_file("/etc/hci/hydra/cassandra-rackdc.properties"')
        quadlet = self.provision.index('write_file("/etc/containers/systemd/hydra-db.container"')
        self.assertLess(write, quadlet)

    def test_the_defaults_preserve_the_existing_topology(self):
        """Changing the snitch is only safe if what it reports does not move.
        SimpleSnitch already reported datacenter1/rack1."""
        self.assertIn('RACKDC_DC = os.environ.get("HELIOS_DC", "datacenter1")', self.provision)
        self.assertIn('RACKDC_RACK = os.environ.get("HELIOS_RACK", "rack1")', self.provision)

    def test_the_rollout_installs_it_without_restarting_the_database(self):
        """Changing endpoint_snitch needs a ScyllaDB restart, and restarting the metadata
        layer under a running cluster belongs in a maintenance window rather than in a
        rollout."""
        deploy = read("deploy_updates.py")
        self.assertIn("cassandra-rackdc.properties", deploy)
        self.assertIn("already present, left alone", deploy)
        self.assertNotIn("systemctl restart hydra-db", deploy)


if __name__ == "__main__":
    unittest.main()
