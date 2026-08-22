#!/usr/bin/env python3
"""Tests for the ring quorum gate and the cluster maintenance lock.

The failure these exist to prevent is a host entering maintenance mode and stopping its
ScyllaDB when the cluster could not spare that replica. On three nodes at RF=3 with
QUORUM reads and writes, the second stop leaves one replica of three against a quorum of
two and the metadata layer stops answering for the whole cluster -- including for the
maintenance workflow that is halfway through recording what it did.

Every assertion below corresponds to a way that can still happen: a gate that reads a
plausible default replication factor instead of the real one, a gate that counts nodes
that are up rather than replicas that can answer, two hosts that both pass a
read-then-write exclusion, a lock that a dead node keeps forever, and a release that
drops somebody else's lock.

`FakeScyllaSession` is the one from test_daruk_lwt.py, extended with the TTL and delete
forms the lock uses. Its behaviour was read off a live Scylla 5.4 through Daruk -- see
the class docstring for the specific observations, including the one that decides whether
a renewed lock is still exclusive.

Run with:  python -m unittest test_ring_lifecycle
"""

import importlib.util
import json
import os
import re
import sys
import threading
import types
import unittest
import urllib.error
import urllib.request
from http.server import HTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))


# -- a stand-in for Scylla ---------------------------------------------------------------

_UPDATE_RE = re.compile(
    r"\AUPDATE\s+(?P<table>[\w.]+)\s*(?:USING\s+TTL\s+\?\s*)?SET\s+(?P<sets>.+?)"
    r"\s+WHERE\s+(?P<key>\w+)\s*=\s*\?"
    r"(?:\s+IF\s+(?P<conditions>.+))?\Z", re.S)
_INSERT_RE = re.compile(
    r"\AINSERT\s+INTO\s+(?P<table>[\w.]+)\s*\((?P<columns>[^)]*)\)\s*"
    r"VALUES\s*\([^)]*\)\s+IF\s+NOT\s+EXISTS(?:\s+USING\s+TTL\s+\?)?\Z", re.S)
_DELETE_RE = re.compile(
    r"\ADELETE\s+FROM\s+(?P<table>[\w.]+)\s+WHERE\s+(?P<key>\w+)\s*=\s*\?"
    r"\s+IF\s+(?P<conditions>.+)\Z", re.S)
_ASSIGNMENT_RE = re.compile(r"(\w+)\s*=\s*\?")
_CONDITION_RE = re.compile(r"(\w+)\s*(!=|=)\s*\?")


class FakeResultSet:
    """The two attributes `_split_lwt_result` reads, and nothing else."""

    def __init__(self, column_names, rows):
        self.column_names = column_names
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class FakeStatement:
    def __init__(self, cql):
        self.cql = cql
        self.consistency_level = None
        self.serial_consistency_level = None


class FakeScyllaSession:
    """Lightweight-transaction semantics, as observed against a live Scylla 5.4.

      * A rejected `IF NOT EXISTS` returns the whole existing row, which is what lets the
        caller name the host that holds the lock.
      * A rejected `IF col = ?` returns `[applied] = false` plus the conditioned column
        as it stands now.
      * `DELETE ... IF holder_token = <concrete>` against an absent row does NOT apply.
      * A row whose insert marker has expired but whose cells were renewed still exists,
        so a competing `IF NOT EXISTS` is still refused. That was checked directly on the
        live cluster, because the opposite would make every renewed lock quietly takeable.

    `expire(name)` models the TTL running out on the whole row -- what happens when the
    node holding the lock dies and stops renewing it.
    """

    def __init__(self):
        self.store = {}
        self.prepared = []

    # -- driver surface ------------------------------------------------------------
    def prepare(self, cql):
        self.prepared.append(cql)
        return FakeStatement(cql)

    def execute(self, statement, parameters=None):
        cql = getattr(statement, "cql", None) or str(statement)
        params = list(parameters or ())
        text = cql.strip()
        insert = _INSERT_RE.match(text)
        if insert:
            return self._insert_if_not_exists(insert, params)
        delete = _DELETE_RE.match(text)
        if delete:
            return self._conditional_delete(delete, params)
        update = _UPDATE_RE.match(text)
        if update:
            return self._conditional_update(update, text, params)
        raise AssertionError(f"FakeScyllaSession was handed a statement it cannot run: {cql}")

    # -- test helpers --------------------------------------------------------------
    def row(self, table, key):
        return self.store.get(table, {}).get(key)

    def put(self, table, key, **columns):
        row = self.store.setdefault(table, {}).setdefault(key, {})
        row.update(columns)
        return row

    def expire(self, table, key):
        """The TTL ran out: the row is gone, the way a dead holder's lock goes."""
        self.store.get(table, {}).pop(key, None)

    # -- statement execution -------------------------------------------------------
    def _conditional_update(self, match, text, params):
        table = self.store.setdefault(match.group("table"), {})
        key_column = match.group("key")
        set_columns = _ASSIGNMENT_RE.findall(match.group("sets"))
        conditions = _CONDITION_RE.findall(match.group("conditions") or "")

        # A leading `USING TTL ?` consumes the first bound value.
        offset = 1 if "USING TTL ?" in text.split("SET", 1)[0] else 0
        set_values = params[offset:offset + len(set_columns)]
        key_value = params[offset + len(set_columns)]
        condition_values = params[offset + len(set_columns) + 1:]

        if not conditions:
            row = table.setdefault(key_value, {key_column: key_value})
            row.update(dict(zip(set_columns, set_values)))
            return FakeResultSet(None, [])

        row = table.get(key_value) or {}
        current = [row.get(column) for column, _operator in conditions]
        applied = all(
            (row.get(column) == expected) if operator == "=" else (row.get(column) != expected)
            for (column, operator), expected in zip(conditions, condition_values))
        if applied:
            written = table.setdefault(key_value, {key_column: key_value})
            written.update(dict(zip(set_columns, set_values)))
        names = ["[applied]"] + [column for column, _operator in conditions]
        return FakeResultSet(names, [tuple([applied] + current)])

    def _conditional_delete(self, match, params):
        table = self.store.setdefault(match.group("table"), {})
        conditions = _CONDITION_RE.findall(match.group("conditions"))
        key_value = params[0]
        condition_values = params[1:]

        row = table.get(key_value) or {}
        current = [row.get(column) for column, _operator in conditions]
        applied = bool(row) and all(
            (row.get(column) == expected) if operator == "=" else (row.get(column) != expected)
            for (column, operator), expected in zip(conditions, condition_values))
        if applied:
            table.pop(key_value, None)
        names = ["[applied]"] + [column for column, _operator in conditions]
        return FakeResultSet(names, [tuple([applied] + current)])

    def _insert_if_not_exists(self, match, params):
        table = self.store.setdefault(match.group("table"), {})
        columns = [column.strip() for column in match.group("columns").split(",")]
        key_value = params[0]
        names = ["[applied]"] + columns
        existing = table.get(key_value)
        if existing is not None:
            return FakeResultSet(names, [tuple([False] + [existing.get(c) for c in columns])])
        table[key_value] = dict(zip(columns, params))
        return FakeResultSet(names, [tuple([True] + [None] * len(columns))])


SESSION = FakeScyllaSession()


def install_fake_driver():
    """Put a fake cassandra-driver in sys.modules so daruk.py can be imported at all."""
    cassandra = types.ModuleType("cassandra")

    class ConsistencyLevel:
        ONE = 1
        QUORUM = 4
        SERIAL = 8

    class Unavailable(Exception):
        pass

    class ReadTimeout(Exception):
        pass

    class OperationTimedOut(Exception):
        pass

    cassandra.ConsistencyLevel = ConsistencyLevel
    cassandra.Unavailable = Unavailable
    cassandra.ReadTimeout = ReadTimeout
    cassandra.OperationTimedOut = OperationTimedOut

    cluster_module = types.ModuleType("cassandra.cluster")

    class NoHostAvailable(Exception):
        pass

    class Cluster:
        def __init__(self, contact_points=None, *args, **kwargs):
            self.contact_points = contact_points

        def connect(self):
            return SESSION

    cluster_module.Cluster = Cluster
    cluster_module.NoHostAvailable = NoHostAvailable

    query_module = types.ModuleType("cassandra.query")

    class SimpleStatement:
        def __init__(self, text, consistency_level=None):
            self.cql = text
            self.consistency_level = consistency_level

    query_module.SimpleStatement = SimpleStatement

    cassandra.cluster = cluster_module
    cassandra.query = query_module
    sys.modules["cassandra"] = cassandra
    sys.modules["cassandra.cluster"] = cluster_module
    sys.modules["cassandra.query"] = query_module


def load_module(alias, filename):
    spec = importlib.util.spec_from_file_location(alias, os.path.join(HERE, filename))
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


install_fake_driver()
daruk = load_module("daruk_ring_under_test", "daruk.py")
vali = load_module("vali_ring_under_test", "vali.py")
mipha = load_module("mipha_ring_under_test", "mipha.py")
cluster = load_module("cluster_ring_under_test", "cluster_new.py")
import helios_schema


# -- one Daruk, served over HTTP for the whole run ---------------------------------------

_server = HTTPServer(("127.0.0.1", 0), daruk.CQLProxyHandler)
DARUK_TEST_URL = "http://127.0.0.1:%d" % _server.server_address[1]
threading.Thread(target=_server.serve_forever, daemon=True).start()

LOCKS = "hydra.cluster_locks"


def call(endpoint, params):
    """POST to Daruk and return (http_status, decoded_body)."""
    request = urllib.request.Request(
        DARUK_TEST_URL + endpoint,
        data=json.dumps(params).encode("utf-8"),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))


# Ring output exactly as `nodetool status` prints it on the live cluster, header and
# trailing note included, because the parser has to survive both.
def host_id_for(index):
    return f"3cf7d7ff-0549-4977-9595-3e15301300{index:02d}"


def nodetool_status(*rows, load="2.38 MB"):
    body = "\n".join(
        f"{marker}  {address}  {load}    256          ?       "
        f"{host_id_for(index)}  rack1"
        for index, (marker, address) in enumerate(rows))
    return (
        "Datacenter: datacenter1\n"
        "=======================\n"
        "Status=Up/Down\n"
        "|/ State=Normal/Leaving/Joining/Moving\n"
        "--  Address       Load       Tokens       Owns    Host ID                               Rack\n"
        + body +
        "\n\nNote: Non-system keyspaces don't have the same replication settings, "
        "effective ownership information is meaningless\n")


THREE_UP = nodetool_status(("UN", "10.0.0.1"), ("UN", "10.0.0.2"), ("UN", "10.0.0.3"))
THREE_ONE_DOWN = nodetool_status(("UN", "10.0.0.1"), ("DN", "10.0.0.2"), ("UN", "10.0.0.3"))
SINGLE_NODE = nodetool_status(("UN", "10.10.102.41"),)


class RingTestCase(unittest.TestCase):
    def setUp(self):
        SESSION.store.clear()
        SESSION.prepared.clear()
        vali.DARUK_URL = DARUK_TEST_URL
        mipha.DARUK_URL = DARUK_TEST_URL
        vali._cluster_locks_table_ready = True   # the DDL is not a statement the fake runs


# -- reading the ring --------------------------------------------------------------------

class NodetoolParsingTests(RingTestCase):
    def test_the_header_and_the_trailing_note_are_not_ring_members(self):
        # The status output carries five header lines and a paragraph of prose. A parser
        # that took any whitespace-split line would report a ring of ten members, and the
        # quorum arithmetic would come out generous in exactly the wrong direction.
        members = vali.parse_nodetool_status(THREE_UP)
        self.assertEqual([m["address"] for m in members],
                         ["10.0.0.1", "10.0.0.2", "10.0.0.3"])

    def test_only_up_and_normal_counts_as_an_available_replica(self):
        # UJ is still streaming in and owns no complete range yet; UL is streaming out.
        # Counting either as a replica overstates what survives the next stop.
        ring = nodetool_status(("UN", "10.0.0.1"), ("UJ", "10.0.0.2"),
                               ("UL", "10.0.0.3"), ("DN", "10.0.0.4"))
        available = {m["address"]: m["available"] for m in vali.parse_nodetool_status(ring)}
        self.assertEqual(available, {"10.0.0.1": True, "10.0.0.2": False,
                                     "10.0.0.3": False, "10.0.0.4": False})

    def test_every_daemon_parses_the_ring_identically(self):
        # The gate refuses in Vali, Mipha reports on it, and the CLI plans against it. If
        # they disagree about who is up, the operator is told one thing and the cluster
        # does another.
        for module in (vali, mipha, cluster):
            members = module.parse_nodetool_status(THREE_ONE_DOWN)
            self.assertEqual([m["available"] for m in members], [True, False, True],
                             module.__name__)


class ReplicationFactorTests(RingTestCase):
    def test_simple_strategy_is_read_from_the_flattened_row(self):
        # Daruk flattens result rows into space-joined str(value), so what reaches the
        # caller is a stringified dict, not a mapping.
        text = "{'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '3'}"
        self.assertEqual(vali.parse_replication_factor(text), 3)

    def test_the_drivers_own_map_repr_is_read_too(self):
        # OrderedMapSerializedKey reprs its pairs as tuples, not with colons. A parser
        # that only knew the colon form would report "unknown" here -- and the gate turns
        # "unknown" into "refuse", so every maintenance request would fail with a message
        # about an unreadable database on a cluster that is perfectly healthy.
        text = ("OrderedMapSerializedKey([('class', "
                "'org.apache.cassandra.locator.SimpleStrategy'), ('replication_factor', '3')])")
        self.assertEqual(vali.parse_replication_factor(text), 3)
        self.assertEqual(cluster.parse_replication_factor(text), 3)

    def test_cqlsh_output_is_read_too(self):
        # The fallback path prints the map instead, and the gate has to work on both or
        # it stops working exactly when Daruk is down.
        text = "{'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'}"
        self.assertEqual(vali.parse_replication_factor(text), 1)

    def test_network_topology_factors_are_summed(self):
        # QUORUM is a majority of the total replica count, not of one datacenter's.
        text = ("{'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', "
                "'dc1': '3', 'dc2': '2'}")
        self.assertEqual(vali.parse_replication_factor(text), 5)

    def test_strategies_without_a_replication_factor_give_none(self):
        self.assertIsNone(vali.parse_replication_factor(
            "{'class': 'org.apache.cassandra.locator.LocalStrategy'}"))
        self.assertIsNone(vali.parse_replication_factor(
            "{'class': 'org.apache.cassandra.locator.EverywhereStrategy'}"))
        self.assertIsNone(vali.parse_replication_factor(""))
        self.assertIsNone(vali.parse_replication_factor(None))

    def test_the_replication_factor_is_never_defaulted(self):
        # spectrum_server's get_actual_replication_factor returns "unknown" rather than a
        # plausible 3 for the same reason: a guess here waves through the stop that takes
        # the last copy of the metadata offline.
        vali.run_cql_query = lambda cql, *a, **k: (1, "", "connection refused")
        try:
            self.assertIsNone(vali.get_hydra_replication_factor())
        finally:
            del vali.run_cql_query


# -- the quorum gate ---------------------------------------------------------------------

class QuorumArithmeticTests(RingTestCase):
    def test_quorum_is_a_strict_majority_of_the_replication_factor(self):
        self.assertEqual([vali.quorum_of(rf) for rf in (1, 2, 3, 4, 5)], [1, 2, 2, 3, 3])

    def test_a_healthy_three_node_rf3_cluster_may_lose_one(self):
        members = vali.parse_nodetool_status(THREE_UP)
        allowed, reason, facts = vali.evaluate_stop(members, 3, "10.0.0.2")
        self.assertTrue(allowed, reason)
        self.assertEqual(facts["replicas_after"], 2)
        self.assertEqual(facts["required"], 2)

    def test_the_second_node_is_refused(self):
        # The failure this whole change exists for. One node is already down; stopping
        # another leaves one replica of three against a quorum of two, and the cluster
        # stops answering -- including for the maintenance workflow itself.
        members = vali.parse_nodetool_status(THREE_ONE_DOWN)
        allowed, reason, facts = vali.evaluate_stop(members, 3, "10.0.0.3")
        self.assertFalse(allowed)
        self.assertEqual(facts["replicas_after"], 1)
        self.assertIn("QUORUM", reason)

    def test_a_single_node_cluster_can_never_enter_maintenance(self):
        # RF=1 has no fault tolerance at all: the one replica is the only copy. This is
        # the live test cluster, and refusing is the correct answer -- 'cluster stop' is
        # the operation for taking a whole single-node cluster down.
        members = vali.parse_nodetool_status(SINGLE_NODE)
        allowed, reason, facts = vali.evaluate_stop(members, 1, "10.10.102.41")
        self.assertFalse(allowed)
        self.assertEqual(facts["replicas_after"], 0)
        self.assertIn("RF=1", reason)

    def test_rf2_cannot_spare_a_node_either(self):
        # QUORUM at RF=2 is 2, so both replicas must answer. A gate that assumed "more
        # than one node means one is spare" would wave this through.
        members = vali.parse_nodetool_status(
            nodetool_status(("UN", "10.0.0.1"), ("UN", "10.0.0.2")))
        allowed, _reason, _facts = vali.evaluate_stop(members, 2, "10.0.0.1")
        self.assertFalse(allowed)

    def test_rf1_on_a_multi_node_ring_is_refused(self):
        # RF=1 spread over three nodes means each partition lives on exactly one of them.
        # Stopping any node makes a third of the metadata unreadable, even though two
        # nodes are still up -- which is why the gate counts replicas, not nodes.
        members = vali.parse_nodetool_status(THREE_UP)
        allowed, _reason, _facts = vali.evaluate_stop(members, 1, "10.0.0.2")
        self.assertFalse(allowed)

    def test_a_witness_node_is_not_a_ring_member_and_may_be_stopped(self):
        # Three-node layouts run no ScyllaDB on the witness, so it holds no replicas and
        # draining it costs the ring nothing. A gate keyed on "is this host in
        # hydra.nodes" would have refused it forever.
        members = vali.parse_nodetool_status(THREE_UP)
        allowed, reason, facts = vali.evaluate_stop(members, 3, "10.0.0.9")
        self.assertTrue(allowed)
        self.assertFalse(facts["ring_member"])
        self.assertIn("not a member", reason)

    def test_stopping_a_node_that_is_already_down_changes_nothing(self):
        members = vali.parse_nodetool_status(THREE_ONE_DOWN)
        allowed, reason, _facts = vali.evaluate_stop(members, 3, "10.0.0.2")
        self.assertTrue(allowed)
        self.assertIn("already unavailable", reason)

    def test_a_bigger_ring_than_rf_still_refuses_once_two_are_missing(self):
        # Five nodes at RF=3 with one already down: the two unavailable members could
        # both be replicas of the same partition, which is the case worth refusing on.
        ring = vali.parse_nodetool_status(nodetool_status(
            ("UN", "10.0.0.1"), ("DN", "10.0.0.2"), ("UN", "10.0.0.3"),
            ("UN", "10.0.0.4"), ("UN", "10.0.0.5")))
        self.assertFalse(vali.evaluate_stop(ring, 3, "10.0.0.4")[0])
        healthy = vali.parse_nodetool_status(nodetool_status(
            ("UN", "10.0.0.1"), ("UN", "10.0.0.2"), ("UN", "10.0.0.3"),
            ("UN", "10.0.0.4"), ("UN", "10.0.0.5")))
        self.assertTrue(vali.evaluate_stop(healthy, 3, "10.0.0.4")[0])


class QuorumGateRefusalTests(RingTestCase):
    """`check_stop_preserves_quorum` is what the maintenance path actually calls."""

    def drive(self, replication, ring, ring_rc=0):
        vali.run_cql_query = lambda cql, *a, **k: (0, replication, "")
        vali.run_remote_spark = lambda ip, cmd: (ring_rc, ring, "")

    def tearDown(self):
        for name in ("run_cql_query", "run_remote_spark"):
            if name in vars(vali):
                del vars(vali)[name]

    def test_it_allows_a_stop_the_cluster_can_absorb(self):
        self.drive("{'class': 'SimpleStrategy', 'replication_factor': '3'}", THREE_UP)
        allowed, reason = vali.check_stop_preserves_quorum("10.0.0.2")
        self.assertTrue(allowed, reason)

    def test_it_refuses_a_stop_that_would_break_quorum(self):
        self.drive("{'class': 'SimpleStrategy', 'replication_factor': '3'}", THREE_ONE_DOWN)
        allowed, reason = vali.check_stop_preserves_quorum("10.0.0.3")
        self.assertFalse(allowed)
        self.assertIn("would leave", reason)

    def test_an_unreadable_replication_factor_refuses(self):
        self.drive("", THREE_UP)
        allowed, reason = vali.check_stop_preserves_quorum("10.0.0.2")
        self.assertFalse(allowed)
        self.assertIn("replication factor", reason)

    def test_an_unreadable_ring_refuses(self):
        # Not "assume it is fine". The gate is asked precisely when the database is about
        # to be taken away, and the cluster it cannot see may already be one node short.
        self.drive("{'class': 'SimpleStrategy', 'replication_factor': '3'}",
                   "nodetool: connection refused", ring_rc=1)
        allowed, reason = vali.check_stop_preserves_quorum("10.0.0.2")
        self.assertFalse(allowed)
        self.assertIn("Refusing", reason)


# -- the lock endpoints ------------------------------------------------------------------

class LockEndpointTests(RingTestCase):
    def lock_params(self, holder, token, ttl=300):
        return {"name": "cluster-maintenance", "holder": holder, "holder_token": token,
                "reason": f"maintenance entry on {holder}", "acquired_at_ms": 1,
                "ttl_seconds": ttl}

    def test_two_hosts_racing_for_the_lock_and_only_one_wins(self):
        # The bug this replaces: "only one host in maintenance at a time" scanned every
        # hydra.nodes row and then wrote. Two hosts a second apart both read "nobody" and
        # both went on to stop their local ScyllaDB.
        _s1, first = call("/v1/lock/acquire", self.lock_params("node01", "t1"))
        _s2, second = call("/v1/lock/acquire", self.lock_params("node02", "t2"))
        self.assertTrue(first["applied"])
        self.assertFalse(second["applied"], "two hosts both entered maintenance")
        self.assertEqual(second["current"]["holder"], "node01")
        self.assertEqual(SESSION.row(LOCKS, "cluster-maintenance")["holder"], "node01")

    def test_the_refusal_names_the_holder_so_the_error_can_too(self):
        call("/v1/lock/acquire", self.lock_params("node01", "t1"))
        _status, body = call("/v1/lock/acquire", self.lock_params("node02", "t2"))
        self.assertEqual(body["current"]["reason"], "maintenance entry on node01")

    def test_a_refused_acquisition_is_http_200_not_an_error(self):
        # A lost race is not an outage. Answering 4xx here would make a correctly refused
        # second maintenance request look like a broken cluster.
        call("/v1/lock/acquire", self.lock_params("node01", "t1"))
        status, body = call("/v1/lock/acquire", self.lock_params("node02", "t2"))
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "success")

    def test_a_lock_held_by_a_dead_node_expires(self):
        # Without a TTL a node that dies holding this blocks maintenance for the whole
        # cluster forever -- and on a cluster that cannot enter maintenance, nobody can
        # replace the hardware that died.
        call("/v1/lock/acquire", self.lock_params("node01", "t1"))
        SESSION.expire(LOCKS, "cluster-maintenance")      # node01 stopped renewing
        _status, body = call("/v1/lock/acquire", self.lock_params("node02", "t2"))
        self.assertTrue(body["applied"])
        self.assertEqual(SESSION.row(LOCKS, "cluster-maintenance")["holder"], "node02")

    def test_the_acquire_statement_carries_a_ttl_and_is_conditional(self):
        op = daruk.LWT_OPS["/v1/lock/acquire"]
        self.assertIn("IF NOT EXISTS", op["cql"])
        self.assertIn("USING TTL ?", op["cql"])
        self.assertIn("ttl_seconds", op["binds"])

    def test_releasing_a_lock_you_do_not_hold_does_not_release_it(self):
        # The late-cleanup failure: a release from an attempt that already lost the lock
        # must not drop the lock the host that is draining right now depends on.
        call("/v1/lock/acquire", self.lock_params("node01", "t1"))
        _status, body = call("/v1/lock/release",
                             {"name": "cluster-maintenance", "holder_token": "t2"})
        self.assertFalse(body["applied"])
        self.assertEqual(SESSION.row(LOCKS, "cluster-maintenance")["holder"], "node01")

        _status, mine = call("/v1/lock/release",
                             {"name": "cluster-maintenance", "holder_token": "t1"})
        self.assertTrue(mine["applied"])
        self.assertIsNone(SESSION.row(LOCKS, "cluster-maintenance"))

    def test_a_release_is_conditional_on_the_acquisition_not_the_holder(self):
        # node01 takes the lock, its TTL runs out, node01 takes it again. A release left
        # over from the first acquisition matches on holder and would drop the second.
        call("/v1/lock/acquire", self.lock_params("node01", "t1"))
        SESSION.expire(LOCKS, "cluster-maintenance")
        call("/v1/lock/acquire", self.lock_params("node01", "t2"))
        _status, body = call("/v1/lock/release",
                             {"name": "cluster-maintenance", "holder_token": "t1"})
        self.assertFalse(body["applied"], "a stale release dropped a live lock")
        self.assertEqual(SESSION.row(LOCKS, "cluster-maintenance")["holder_token"], "t2")

    def test_releasing_a_lock_nobody_holds_does_not_apply(self):
        _status, body = call("/v1/lock/release",
                             {"name": "cluster-maintenance", "holder_token": "t1"})
        self.assertFalse(body["applied"])

    def test_only_the_holder_can_renew(self):
        call("/v1/lock/acquire", self.lock_params("node01", "t1"))
        _s1, theirs = call("/v1/lock/renew", self.lock_params("node02", "t2"))
        _s2, mine = call("/v1/lock/renew", self.lock_params("node01", "t1"))
        self.assertFalse(theirs["applied"])
        self.assertTrue(mine["applied"])
        self.assertEqual(SESSION.row(LOCKS, "cluster-maintenance")["holder"], "node01")

    def test_renew_rewrites_the_token_so_it_cannot_outlive_the_row(self):
        # Every non-key column has to be rewritten on renew. A column left out keeps the
        # original insert's TTL and expires first -- after which the row is still alive
        # but no longer renewable or releasable by the host that holds it.
        op = daruk.LWT_OPS["/v1/lock/renew"]
        assignments = op["cql"].split("SET", 1)[1].split("WHERE", 1)[0]
        for column in ("holder", "holder_token", "reason", "acquired_at_ms"):
            self.assertIn(column, assignments, f"renew does not refresh {column}")

    def test_a_token_may_not_be_null(self):
        # `IF holder_token = null` applies against a row that does not exist and creates a
        # partial one, so a caller that omitted its token would "release" a lock into a
        # half-written row that then blocks everyone.
        status, body = call("/v1/lock/release",
                            {"name": "cluster-maintenance", "holder_token": None})
        self.assertEqual(status, 400)
        self.assertIn("holder_token", body["error"])

    def test_a_misspelt_parameter_is_refused_rather_than_defaulted(self):
        status, body = call("/v1/lock/acquire", {
            "name": "cluster-maintenance", "holder": "node01", "holder_tokn": "t1",
            "acquired_at_ms": 1, "ttl_seconds": 300})
        self.assertEqual(status, 400)
        self.assertIn("holder_tokn", body["error"])


# -- the callers -------------------------------------------------------------------------

class ValiLockClientTests(RingTestCase):
    def test_acquiring_returns_a_token_and_a_second_host_gets_the_holder(self):
        ok, token, current, error = vali.acquire_maintenance_lock("node01", "draining")
        self.assertTrue(ok)
        self.assertTrue(token)
        self.assertEqual((current, error), ({}, ""))

        ok2, token2, current2, error2 = vali.acquire_maintenance_lock("node02", "draining")
        self.assertTrue(ok2, "a lost race is not a failure")
        self.assertEqual(token2, "")
        self.assertEqual(current2["holder"], "node01")
        self.assertEqual(error2, "")

    def test_daruk_being_down_is_an_error_and_never_a_held_lock(self):
        # Reporting success here would let a host drain with nothing excluding a second.
        vali.DARUK_URL = "http://127.0.0.1:1"
        try:
            ok, token, _current, error = vali.acquire_maintenance_lock("node01", "draining")
        finally:
            vali.DARUK_URL = DARUK_TEST_URL
        self.assertFalse(ok)
        self.assertEqual(token, "")
        self.assertTrue(error)

    def test_leave_releases_the_lock_it_finds_rather_than_one_it_remembers(self):
        # `leave` arrives hours after `enter`, from a process that never held the token.
        _ok, token, _current, _error = vali.acquire_maintenance_lock("node01", "draining")
        vali.run_cql_query = lambda cql, *a, **k: (
            0, json.dumps(SESSION.row(LOCKS, "cluster-maintenance") or {}), "")
        try:
            self.assertTrue(vali.release_maintenance_lock_for_host("node01"))
            self.assertIsNone(SESSION.row(LOCKS, "cluster-maintenance"))
        finally:
            del vali.run_cql_query
        self.assertTrue(token)

    def test_leave_does_not_release_a_lock_another_host_holds(self):
        vali.acquire_maintenance_lock("node01", "draining")
        vali.run_cql_query = lambda cql, *a, **k: (
            0, json.dumps(SESSION.row(LOCKS, "cluster-maintenance") or {}), "")
        try:
            self.assertFalse(vali.release_maintenance_lock_for_host("node02"))
        finally:
            del vali.run_cql_query
        self.assertEqual(SESSION.row(LOCKS, "cluster-maintenance")["holder"], "node01")


class MiphaRenewalTests(RingTestCase):
    """Mipha renews the lock while a host is transitioning, so the TTL can stay short."""

    def read_lock_via(self, module):
        module.run_cql_query = lambda cql, *a, **k: (
            0, json.dumps(SESSION.row(LOCKS, "cluster-maintenance") or {}), "")

    def tearDown(self):
        if "run_cql_query" in vars(mipha):
            del vars(mipha)["run_cql_query"]

    def test_it_renews_the_lock_of_the_host_that_holds_it(self):
        vali.acquire_maintenance_lock("node01", "draining")
        self.read_lock_via(mipha)
        self.assertTrue(mipha.renew_maintenance_lock_for("node01"))

    def test_it_will_not_renew_a_lock_held_by_a_different_host(self):
        # Renewing on hostname alone would let Mipha keep a lock alive in the name of a
        # host that lost it, so the host that holds it now could be released by the loser.
        vali.acquire_maintenance_lock("node01", "draining")
        self.read_lock_via(mipha)
        self.assertFalse(mipha.renew_maintenance_lock_for("node02"))

    def test_there_is_nothing_to_renew_when_the_lock_has_expired(self):
        vali.acquire_maintenance_lock("node01", "draining")
        SESSION.expire(LOCKS, "cluster-maintenance")
        self.read_lock_via(mipha)
        self.assertFalse(mipha.renew_maintenance_lock_for("node01"))

    def test_recovering_hosts_still_hold_the_lock(self):
        # A host that has left maintenance but not finished resyncing its storage is not
        # a replica anyone should count on, so no second host may start draining yet.
        self.assertIn("RECOVERING", mipha.MAINTENANCE_LOCK_STATES)
        self.assertIn("IN_MAINTENANCE", mipha.MAINTENANCE_LOCK_STATES)
        self.assertIn("ENTERING_MAINTENANCE", mipha.MAINTENANCE_LOCK_STATES)


# -- the table and the endpoint table ----------------------------------------------------

class SchemaTests(RingTestCase):
    def test_the_lock_table_is_a_migration_not_another_daemon_create(self):
        ids = [m["id"] for m in helios_schema.MIGRATIONS]
        self.assertIn("0002-cluster-locks", ids)
        self.assertEqual(ids, sorted(ids), "migrations are not in id order")
        self.assertEqual(len(ids), len(set(ids)))

    def test_the_baseline_migration_was_not_edited(self):
        # Editing an applied migration leaves clusters with different schemas that both
        # believe they are current; helios_schema raises on it, which is only useful if
        # new tables are appended instead.
        baseline = helios_schema.MIGRATIONS[0]
        self.assertEqual(baseline["id"], "0001-baseline")
        self.assertEqual(len(baseline["statements"]), 31)

    def test_vali_bootstraps_the_exact_statement_the_migration_declares(self):
        # Vali creates this table lazily because nothing runs the migrations yet. The two
        # texts drifting apart is how a column ends up on some nodes and not others.
        migration = next(m for m in helios_schema.MIGRATIONS
                         if m["id"] == "0002-cluster-locks")
        self.assertEqual(" ".join(vali.CLUSTER_LOCKS_DDL.split()),
                         " ".join(migration["statements"][0].split()))


class EndpointTableTests(RingTestCase):
    def test_every_lock_operation_is_conditional(self):
        for path in ("/v1/lock/acquire", "/v1/lock/renew", "/v1/lock/release"):
            self.assertRegex(daruk.LWT_OPS[path]["cql"], r"\bIF\b", path)

    def test_placeholder_count_matches_the_bind_count(self):
        for path, op in daruk.LWT_OPS.items():
            self.assertEqual(op["cql"].count("?"), len(op["binds"]), path)

    def test_every_bound_name_is_either_a_parameter_or_server_owned(self):
        for path, op in daruk.LWT_OPS.items():
            known = set(op["params"]) | set(op.get("fixed", {}))
            for name in op["binds"]:
                self.assertIn(name, known, f"{path}: bind '{name}'")

    def test_lock_statements_are_prepared_at_serial_consistency(self):
        # Without SERIAL the compare and the swap are not one Paxos round, and two hosts
        # can both take the lock.
        call("/v1/lock/acquire", {"name": "cluster-maintenance", "holder": "node01",
                                  "holder_token": "t1", "acquired_at_ms": 1,
                                  "ttl_seconds": 300})
        statement = daruk._lwt_statement(daruk.LWT_OPS["/v1/lock/acquire"]["cql"])
        self.assertEqual(statement.serial_consistency_level, daruk.ConsistencyLevel.SERIAL)
        self.assertEqual(statement.consistency_level, daruk.ConsistencyLevel.QUORUM)

    def test_the_lock_row_holds_no_column_json_cannot_encode(self):
        # A refused IF NOT EXISTS returns the whole row, and make_serializable passes a
        # driver datetime straight through -- json.dumps would then raise on exactly the
        # response that tells a caller who holds the lock. Hence bigint milliseconds.
        self.assertIn("acquired_at_ms", daruk.LWT_OPS["/v1/lock/acquire"]["cql"])
        self.assertNotIn("acquired_at ", daruk.LWT_OPS["/v1/lock/acquire"]["cql"])


# -- decommission planning ---------------------------------------------------------------

class DecommissionPlanningTests(RingTestCase):
    def test_the_cli_agrees_with_the_daemons_about_quorum(self):
        for rf in (1, 2, 3, 4, 5):
            self.assertEqual(cluster.quorum_of(rf), vali.quorum_of(rf))

    def test_a_host_id_is_parsed_so_removenode_can_be_offered(self):
        # `nodetool removenode` takes the host id, not the address, and it is the one
        # argument a decommission plan cannot get wrong. Reading it positionally returned
        # the `Owns` column -- a literal "?" -- because Load is printed as two
        # whitespace-separated fields, "2.38 MB". Caught on the live cluster, not here,
        # which is why this asserts the value and not merely that it is truthy.
        members = cluster.parse_nodetool_status(THREE_ONE_DOWN)
        dead = next(m for m in members if m["address"] == "10.0.0.2")
        self.assertEqual(dead["host_id"], host_id_for(1))
        self.assertEqual(dead["load"], "2.38 MB")
        self.assertFalse(dead["available"])

    def test_the_host_id_survives_an_unknown_load(self):
        # A node that has not reported its load prints a bare "?" there, shifting every
        # later column by one.
        members = cluster.parse_nodetool_status(
            nodetool_status(("UN", "10.0.0.1"), load="?"))
        self.assertEqual(members[0]["host_id"], host_id_for(0))

    def test_every_daemon_finds_the_same_host_id(self):
        for module in (vali, mipha, cluster):
            members = module.parse_nodetool_status(THREE_ONE_DOWN)
            self.assertEqual([m["host_id"] for m in members],
                             [host_id_for(i) for i in range(3)], module.__name__)


if __name__ == "__main__":
    unittest.main()
