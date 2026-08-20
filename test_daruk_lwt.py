#!/usr/bin/env python3
"""Tests for Daruk's typed compare-and-swap endpoints and the callers that use them.

The failure these exist to prevent is two hosts concluding they own the same VM and both
starting it, which puts two qemu processes on one raw DRBD device and corrupts the guest's
disk. Every assertion below corresponds to a way that can still happen: a claim that is
not conditional, a refusal a caller mistakes for success, a migration lock two callers can
both take, a cleanup that releases somebody else's lock.

`FakeScyllaSession` stands in for the database. Its behaviour was read off a live Scylla
5.4 through Daruk rather than assumed -- see the class docstring for the specific
observations it encodes.

Run with:  python -m unittest test_daruk_lwt
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
    r"\AUPDATE\s+(?P<table>[\w.]+)\s+SET\s+(?P<sets>.+?)"
    r"\s+WHERE\s+(?P<key>\w+)\s*=\s*\?"
    r"(?:\s+IF\s+(?P<conditions>.+))?\Z", re.S)
_INSERT_RE = re.compile(
    r"\AINSERT\s+INTO\s+(?P<table>[\w.]+)\s*\((?P<columns>[^)]*)\)\s*"
    r"VALUES\s*\([^)]*\)\s+IF\s+NOT\s+EXISTS\Z", re.S)
_ASSIGNMENT_RE = re.compile(r"(\w+)\s*=\s*\?")
_CONDITION_RE = re.compile(r"(\w+)\s*(!=|=)\s*\?")


class FakeResultSet:
    """The two attributes `_split_lwt_result` reads, and nothing else.

    `column_names` deliberately stays readable after iteration, and there is no
    `was_applied`: the driver's version of that property is only readable *before* the
    result set is consumed, and Daruk must not depend on it.
    """

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

      * A rejected statement returns `[applied] = false` followed by the conditioned
        columns as they stand now. That is what lets a caller name the winner.
      * An applied statement returns `[applied] = true` followed by the same columns
        holding their pre-image, which a caller must not mistake for the new state.
      * A condition satisfied by nulls applies against a row that does not exist yet, and
        creates it -- `IF status != 'migrating'` on an unknown VM writes a stub row.
      * `IF col = ''` does *not* match a row whose column is null, so it fails on an
        absent row.
      * `INSERT ... IF NOT EXISTS` that is refused returns the whole existing row.
      * A statement with no IF clause returns no rows and no `[applied]` column at all.
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
        insert = _INSERT_RE.match(cql.strip())
        if insert:
            return self._insert_if_not_exists(insert, params)
        update = _UPDATE_RE.match(cql.strip())
        if update:
            return self._conditional_update(update, params)
        raise AssertionError(f"FakeScyllaSession was handed a statement it cannot run: {cql}")

    # -- test helpers --------------------------------------------------------------
    def row(self, table, key):
        return self.store.get(table, {}).get(key)

    def put(self, table, key, **columns):
        row = self.store.setdefault(table, {}).setdefault(key, {})
        row.update(columns)
        return row

    # -- statement execution -------------------------------------------------------
    def _conditional_update(self, match, params):
        table = self.store.setdefault(match.group("table"), {})
        key_column = match.group("key")
        set_columns = _ASSIGNMENT_RE.findall(match.group("sets"))
        conditions = _CONDITION_RE.findall(match.group("conditions") or "")

        set_values = params[:len(set_columns)]
        key_value = params[len(set_columns)]
        condition_values = params[len(set_columns) + 1:]

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
    """Put a fake cassandra-driver in sys.modules so daruk.py can be imported at all.

    daruk.py connects at import time. The alternative to faking the driver is not testing
    the endpoint table, the binding, or the result parsing -- which is all of the logic.
    """
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
daruk = load_module("daruk_under_test", "daruk.py")
vali = load_module("vali_under_test", "vali.py")
spectrum = load_module("spectrum_under_test", "spectrum_server.py")


# -- one Daruk, served over HTTP for the whole run ---------------------------------------

_server = HTTPServer(("127.0.0.1", 0), daruk.CQLProxyHandler)
DARUK_TEST_URL = "http://127.0.0.1:%d" % _server.server_address[1]
threading.Thread(target=_server.serve_forever, daemon=True).start()

VMS = "hydra.vms"
NODES = "hydra.nodes"


def call(endpoint, params, url=DARUK_TEST_URL):
    """POST to Daruk and return (http_status, decoded_body)."""
    request = urllib.request.Request(
        url + endpoint,
        data=json.dumps(params).encode("utf-8"),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))


class LwtTestCase(unittest.TestCase):
    def setUp(self):
        SESSION.store.clear()
        SESSION.prepared.clear()
        vali.DARUK_URL = DARUK_TEST_URL
        spectrum.DARUK_URL = DARUK_TEST_URL

    def given_vm(self, name, **columns):
        columns.setdefault("host_ip", "")
        columns.setdefault("state", "Stopped")
        return SESSION.put(VMS, name, name=name, **columns)


# -- the endpoint table itself -----------------------------------------------------------

class EndpointTableTests(LwtTestCase):
    """The table is the security boundary and the correctness boundary at once."""

    def test_every_operation_is_conditional(self):
        # An endpoint here without an IF clause is a blind write wearing the name of a
        # compare-and-swap, and every caller would treat its applied=true as a claim.
        for path, op in daruk.LWT_OPS.items():
            self.assertRegex(op["cql"], r"\bIF\b", path)

    def test_every_bound_name_is_either_a_parameter_or_server_owned(self):
        # A bind naming something that is neither would raise KeyError at request time --
        # on the ownership path, in production, and only for that one endpoint.
        for path, op in daruk.LWT_OPS.items():
            known = set(op["params"]) | set(op.get("fixed", {}))
            for name in op["binds"]:
                self.assertIn(name, known, f"{path}: bind '{name}'")

    def test_placeholder_count_matches_the_bind_count(self):
        for path, op in daruk.LWT_OPS.items():
            self.assertEqual(op["cql"].count("?"), len(op["binds"]), path)

    def test_parameter_types_are_ones_the_coercer_understands(self):
        for path, op in daruk.LWT_OPS.items():
            for name, spec in op["params"].items():
                self.assertIn(spec["type"], ("text", "int", "bool"), f"{path}.{name}")

    def test_the_migration_lock_value_is_not_a_caller_parameter(self):
        # If a caller could choose the lock value, two callers could each "hold" a
        # different lock on the same VM and both migrate it.
        for path in ("/v1/vm/migrate-lock", "/v1/vm/migrate-unlock", "/v1/vm/migrate-commit"):
            self.assertEqual(daruk.LWT_OPS[path]["fixed"]["lock"], daruk.MIGRATION_LOCK)
            self.assertNotIn("lock", daruk.LWT_OPS[path]["params"])

    def test_statements_are_prepared_at_serial_consistency(self):
        # Without SERIAL the compare and the swap are not one Paxos round and the endpoint
        # is back to being the blind write it replaced.
        self.given_vm("web-01")
        call("/v1/vm/claim", {"name": "web-01", "host_ip": "10.0.0.1"})
        statement = daruk._lwt_statement(daruk.LWT_OPS["/v1/vm/claim"]["cql"])
        self.assertEqual(statement.serial_consistency_level, daruk.ConsistencyLevel.SERIAL)
        self.assertEqual(statement.consistency_level, daruk.ConsistencyLevel.QUORUM)

    def test_a_statement_is_prepared_once_and_reused(self):
        self.given_vm("web-01")
        before = len(SESSION.prepared)
        for _ in range(3):
            call("/v1/vm/claim", {"name": "web-01", "host_ip": "10.0.0.1",
                                  "expected_host_ip": "10.0.0.1"})
        self.assertLessEqual(len(SESSION.prepared) - before, 1)


# -- request validation ------------------------------------------------------------------

class ParameterValidationTests(LwtTestCase):
    def test_a_misspelt_parameter_is_refused_rather_than_defaulted(self):
        # This is the whole reason unknown keys are an error: "expcted_host_ip" would
        # otherwise fall back to the default of "" and turn the compare-and-swap into an
        # unconditional claim, which is the bug the endpoint exists to remove.
        status, body = call("/v1/vm/claim", {
            "name": "web-01", "host_ip": "10.0.0.1", "expcted_host_ip": "10.0.0.2"})
        self.assertEqual(status, 400)
        self.assertIn("expcted_host_ip", body["error"])

    def test_a_missing_required_parameter_is_refused(self):
        status, body = call("/v1/vm/claim", {"name": "web-01"})
        self.assertEqual(status, 400)
        self.assertIn("host_ip", body["error"])

    def test_release_has_no_default_expected_owner(self):
        # A release that matches any owner is exactly the blind write that frees a live
        # VM's row for a second host to claim.
        status, body = call("/v1/vm/release", {"name": "web-01"})
        self.assertEqual(status, 400)
        self.assertIn("expected_host_ip", body["error"])

    def test_types_are_enforced(self):
        for params, expected in (
                ({"name": "web-01", "host_ip": 42}, "host_ip"),
                ({"name": 7, "host_ip": "10.0.0.1"}, "name")):
            status, body = call("/v1/vm/claim", params)
            self.assertEqual(status, 400, params)
            self.assertIn(expected, body["error"])

    def test_a_boolean_is_not_accepted_where_an_integer_is_due(self):
        # isinstance(True, int) is True in Python, so an unguarded check would register a
        # VM with one vcpu because someone sent `true`.
        status, body = call("/v1/vm/create", {"name": "web-01", "vcpu": True})
        self.assertEqual(status, 400)
        self.assertIn("vcpu", body["error"])

    def test_caller_supplied_cql_is_not_a_parameter(self):
        # The endpoints must never take statement text. There is no key that would accept
        # it, so it arrives as an unknown parameter.
        status, body = call("/v1/vm/claim", {"cql": "DROP KEYSPACE hydra"})
        self.assertEqual(status, 400)
        self.assertIn("cql", body["error"])

    def test_a_body_that_is_not_an_object_is_refused(self):
        status, _body = call("/v1/vm/claim", ["web-01", "10.0.0.1"])
        self.assertEqual(status, 400)

    def test_an_unknown_endpoint_is_not_routed(self):
        request = urllib.request.Request(
            DARUK_TEST_URL + "/v1/vm/nonesuch", data=b"{}",
            headers={"Content-Type": "application/json"})
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request, timeout=10)
        self.assertEqual(caught.exception.code, 404)

    def test_null_is_accepted_only_where_the_column_may_be_null(self):
        # `IF host_ip = null` is a real condition -- it matches a row whose column was
        # never written -- so nullable parameters must survive binding.
        self.given_vm("web-01")
        SESSION.store[VMS]["web-01"].pop("host_ip")
        status, body = call("/v1/vm/claim", {
            "name": "web-01", "host_ip": "10.0.0.1", "expected_host_ip": None})
        self.assertEqual(status, 200)
        self.assertTrue(body["applied"])
        # ... but a parameter the statement writes may not be null.
        status, body = call("/v1/vm/claim", {"name": "web-01", "host_ip": None})
        self.assertEqual(status, 400)


# -- result parsing ----------------------------------------------------------------------

class ResultParsingTests(LwtTestCase):
    def test_a_statement_that_ran_unconditionally_is_reported_not_assumed_applied(self):
        # Scylla accepts "INSERT INTO t JSON ? IF NOT EXISTS", ignores the condition, and
        # returns no [applied] column while overwriting the row. Treating a result with no
        # [applied] column as success would report a compare-and-swap that never happened.
        blind = FakeResultSet(None, [])
        with self.assertRaises(RuntimeError):
            daruk._split_lwt_result(blind)

    def test_applied_and_current_are_split_off_the_first_column(self):
        rejected = FakeResultSet(["[applied]", "host_ip"], [(False, "10.0.0.2")])
        applied, current = daruk._split_lwt_result(rejected)
        self.assertFalse(applied)
        self.assertEqual(current, {"host_ip": "10.0.0.2"})

    def test_more_than_one_row_is_refused(self):
        malformed = FakeResultSet(["[applied]", "host_ip"], [(True, "a"), (True, "b")])
        with self.assertRaises(RuntimeError):
            daruk._split_lwt_result(malformed)

    def test_current_values_are_only_returned_when_the_race_was_lost(self):
        # On success the driver echoes the *pre-image* of the conditioned columns. Handing
        # that back as "current" invites a caller to read it as the new state.
        self.given_vm("web-01", host_ip="")
        _status, body = call("/v1/vm/claim", {"name": "web-01", "host_ip": "10.0.0.1"})
        self.assertTrue(body["applied"])
        self.assertNotIn("current", body)


# -- VM ownership ------------------------------------------------------------------------

class VmOwnershipTests(LwtTestCase):
    def test_claiming_an_unowned_vm_succeeds(self):
        self.given_vm("web-01", host_ip="")
        status, body = call("/v1/vm/claim", {"name": "web-01", "host_ip": "10.0.0.1"})
        self.assertEqual(status, 200)
        self.assertTrue(body["applied"])
        self.assertEqual(SESSION.row(VMS, "web-01")["host_ip"], "10.0.0.1")
        self.assertEqual(SESSION.row(VMS, "web-01")["state"], "Running")

    def test_claiming_an_owned_vm_is_refused_and_names_the_owner(self):
        # The refusal has to carry the owner: "someone else has it" with no name leaves an
        # operator to guess, and leaves the caller unable to say where the VM is running.
        self.given_vm("web-01", host_ip="10.0.0.9", state="Running")
        status, body = call("/v1/vm/claim", {"name": "web-01", "host_ip": "10.0.0.1"})
        self.assertEqual(status, 200, "a lost race is not an HTTP error")
        self.assertFalse(body["applied"])
        self.assertEqual(body["current"]["host_ip"], "10.0.0.9")
        self.assertEqual(SESSION.row(VMS, "web-01")["host_ip"], "10.0.0.9")

    def test_reclaiming_a_vm_this_host_already_owns_succeeds(self):
        # A retried start must not be refused just because the first attempt got as far as
        # writing the row.
        self.given_vm("web-01", host_ip="10.0.0.1", state="Running")
        status, body = call("/v1/vm/claim", {
            "name": "web-01", "host_ip": "10.0.0.1", "expected_host_ip": "10.0.0.1"})
        self.assertEqual(status, 200)
        self.assertTrue(body["applied"])

    def test_only_one_of_two_hosts_racing_from_the_same_read_wins(self):
        # Both callers read host_ip = "" and both decide to start the VM. This is the
        # dual-primary scenario: without the condition both writes land and two qemu
        # processes open the same DRBD device.
        self.given_vm("web-01", host_ip="")
        _s1, first = call("/v1/vm/claim", {
            "name": "web-01", "host_ip": "10.0.0.1", "expected_host_ip": ""})
        _s2, second = call("/v1/vm/claim", {
            "name": "web-01", "host_ip": "10.0.0.2", "expected_host_ip": ""})
        self.assertTrue(first["applied"])
        self.assertFalse(second["applied"])
        self.assertEqual(second["current"]["host_ip"], "10.0.0.1")

    def test_releasing_a_placement_this_host_does_not_hold_is_refused(self):
        # This is the reconciler's failure mode: a VM that migrated away still leaves a
        # local libvirt trace, and an unconditional release would unplace a live VM.
        self.given_vm("web-01", host_ip="10.0.0.9", state="Running")
        status, body = call("/v1/vm/release", {
            "name": "web-01", "expected_host_ip": "10.0.0.1"})
        self.assertEqual(status, 200)
        self.assertFalse(body["applied"])
        self.assertEqual(body["current"]["host_ip"], "10.0.0.9")
        self.assertEqual(SESSION.row(VMS, "web-01")["host_ip"], "10.0.0.9")

    def test_releasing_a_placement_this_host_holds_succeeds(self):
        self.given_vm("web-01", host_ip="10.0.0.1", state="Running")
        _status, body = call("/v1/vm/release", {
            "name": "web-01", "expected_host_ip": "10.0.0.1"})
        self.assertTrue(body["applied"])
        self.assertEqual(SESSION.row(VMS, "web-01")["host_ip"], "")
        self.assertEqual(SESSION.row(VMS, "web-01")["state"], "Stopped")

    def test_writing_state_for_a_vm_that_moved_away_is_refused(self):
        self.given_vm("web-01", host_ip="10.0.0.9", state="Running")
        _status, body = call("/v1/vm/set-state", {
            "name": "web-01", "state": "Stopped", "expected_host_ip": "10.0.0.1"})
        self.assertFalse(body["applied"])
        self.assertEqual(SESSION.row(VMS, "web-01")["state"], "Running")

    def test_a_placement_that_was_never_written_can_still_be_claimed_and_released(self):
        # `host_ip` is null, not "", for a row nothing has ever placed. `IF host_ip = ''`
        # does not match a null, so a caller that coerced the value it read to "" would
        # have every start of such a VM refused as "owned by another host".
        self.given_vm("web-01")
        SESSION.store[VMS]["web-01"].pop("host_ip")
        _status, claim = call("/v1/vm/claim", {
            "name": "web-01", "host_ip": "10.0.0.1", "expected_host_ip": None})
        self.assertTrue(claim["applied"])
        self.given_vm("web-02")
        SESSION.store[VMS]["web-02"].pop("host_ip")
        _status, release = call("/v1/vm/release", {
            "name": "web-02", "expected_host_ip": None})
        self.assertTrue(release["applied"])

    def test_a_claim_does_not_conjure_a_vm_that_does_not_exist(self):
        # `IF host_ip = ''` does not match an absent row, so an unknown VM is refused
        # rather than half-created. (`expected_host_ip: null` would match and create one,
        # which is why callers read the row before claiming it.)
        status, body = call("/v1/vm/claim", {"name": "ghost", "host_ip": "10.0.0.1"})
        self.assertEqual(status, 200)
        self.assertFalse(body["applied"])
        self.assertIsNone(SESSION.row(VMS, "ghost"))


# -- migration lock ----------------------------------------------------------------------

class MigrationLockTests(LwtTestCase):
    def test_the_lock_can_be_taken_when_free(self):
        self.given_vm("web-01", host_ip="10.0.0.1", state="Running")
        _status, body = call("/v1/vm/migrate-lock", {"name": "web-01"})
        self.assertTrue(body["applied"])
        self.assertEqual(SESSION.row(VMS, "web-01")["status"], daruk.MIGRATION_LOCK)

    def test_the_lock_is_free_when_status_has_never_been_written(self):
        # `status` is null for every VM that has never migrated, so the common case is the
        # null case. If a null did not satisfy `!=` the first migration of every VM would
        # be refused.
        self.given_vm("web-01", host_ip="10.0.0.1", state="Running")
        self.assertNotIn("status", SESSION.row(VMS, "web-01"))
        _status, body = call("/v1/vm/migrate-lock", {"name": "web-01"})
        self.assertTrue(body["applied"])

    def test_the_lock_cannot_be_taken_twice(self):
        # Two concurrent live migrations of one VM, and live migration is exactly the
        # window in which DRBD dual-primary is open.
        self.given_vm("web-01", host_ip="10.0.0.1", state="Running")
        _s1, first = call("/v1/vm/migrate-lock", {"name": "web-01"})
        _s2, second = call("/v1/vm/migrate-lock", {"name": "web-01"})
        self.assertTrue(first["applied"])
        self.assertFalse(second["applied"])
        self.assertEqual(second["current"]["status"], daruk.MIGRATION_LOCK)

    def test_releasing_a_lock_you_do_not_hold_changes_nothing(self):
        self.given_vm("web-01", host_ip="10.0.0.1", state="Running", status="running")
        _status, body = call("/v1/vm/migrate-unlock", {"name": "web-01"})
        self.assertFalse(body["applied"])
        self.assertEqual(body["current"]["status"], "running")

    def test_a_cleanup_that_arrives_after_the_lock_was_released_is_a_no_op(self):
        # A failed attempt's cleanup can arrive long after the fact. Because the release
        # is conditional on the lock being held, a second one cannot write over whatever
        # status the VM has settled into.
        #
        # Note the limit of this: the lock is a bare value with no holder identity, so if
        # a *second* migration has taken it by then, the stale cleanup does match and does
        # release it. Giving the lock a holder token is the fix; see docs/daruk.md.
        self.given_vm("web-01", host_ip="10.0.0.1", state="Running")
        call("/v1/vm/migrate-lock", {"name": "web-01"})
        _status, first = call("/v1/vm/migrate-unlock", {"name": "web-01"})
        self.assertTrue(first["applied"])
        SESSION.put(VMS, "web-01", status="something-else")
        _status, stale = call("/v1/vm/migrate-unlock", {"name": "web-01"})
        self.assertFalse(stale["applied"])
        self.assertEqual(SESSION.row(VMS, "web-01")["status"], "something-else")

    def test_committing_a_migration_needs_the_source_host_and_the_lock(self):
        self.given_vm("web-01", host_ip="10.0.0.1", state="Running", status="running")
        _status, without_lock = call("/v1/vm/migrate-commit", {
            "name": "web-01", "host_ip": "10.0.0.2", "expected_host_ip": "10.0.0.1"})
        self.assertFalse(without_lock["applied"], "commit must require the lock")
        self.assertEqual(SESSION.row(VMS, "web-01")["host_ip"], "10.0.0.1")

        call("/v1/vm/migrate-lock", {"name": "web-01"})
        _status, wrong_source = call("/v1/vm/migrate-commit", {
            "name": "web-01", "host_ip": "10.0.0.2", "expected_host_ip": "10.0.0.7"})
        self.assertFalse(wrong_source["applied"], "commit must require the source host")
        self.assertEqual(wrong_source["current"]["host_ip"], "10.0.0.1")

        _status, good = call("/v1/vm/migrate-commit", {
            "name": "web-01", "host_ip": "10.0.0.2", "expected_host_ip": "10.0.0.1"})
        self.assertTrue(good["applied"])
        row = SESSION.row(VMS, "web-01")
        self.assertEqual(row["host_ip"], "10.0.0.2")
        self.assertEqual(row["status"], daruk.UNLOCKED_STATUS)

    def test_the_hand_over_moves_the_host_and_drops_the_lock_together(self):
        # Two statements would leave a window in which the VM is placed on the target with
        # the lock still held, or unlocked while still recorded on the source.
        self.given_vm("web-01", host_ip="10.0.0.1", state="Running")
        call("/v1/vm/migrate-lock", {"name": "web-01"})
        call("/v1/vm/migrate-commit", {
            "name": "web-01", "host_ip": "10.0.0.2", "expected_host_ip": "10.0.0.1"})
        cql = daruk.LWT_OPS["/v1/vm/migrate-commit"]["cql"]
        self.assertIn("host_ip = ?", cql)
        self.assertIn("status = ?", cql)
        self.assertIn("AND", cql)


# -- VM registration ---------------------------------------------------------------------

class VmCreateTests(LwtTestCase):
    def test_a_new_vm_is_registered(self):
        status, body = call("/v1/vm/create", {"name": "web-01", "vcpu": 2, "memory": 2048})
        self.assertEqual(status, 200)
        self.assertTrue(body["applied"])
        self.assertEqual(SESSION.row(VMS, "web-01")["vcpu"], 2)

    def test_a_duplicate_name_does_not_overwrite_a_live_vm(self):
        # INSERT is an upsert in CQL: the unconditional version reset a running VM's
        # host_ip to "", after which the next start put a second copy of it on another
        # host against the same disks.
        self.given_vm("web-01", host_ip="10.0.0.9", state="Running", vcpu=8)
        status, body = call("/v1/vm/create", {"name": "web-01", "vcpu": 1, "memory": 512})
        self.assertEqual(status, 200)
        self.assertFalse(body["applied"])
        self.assertEqual(body["current"]["host_ip"], "10.0.0.9")
        row = SESSION.row(VMS, "web-01")
        self.assertEqual(row["host_ip"], "10.0.0.9")
        self.assertEqual(row["vcpu"], 8)

    def test_the_insert_lists_its_columns(self):
        # "INSERT INTO hydra.vms JSON ? IF NOT EXISTS" is accepted by Scylla and then runs
        # unconditionally: no [applied] column, and the row is overwritten. The column
        # form is the only one that actually compares and swaps.
        self.assertNotIn("JSON", daruk.LWT_OPS["/v1/vm/create"]["cql"])
        for column in ("name", "host_ip", "state"):
            self.assertIn(column, daruk.LWT_OPS["/v1/vm/create"]["cql"])


# -- host maintenance --------------------------------------------------------------------

class NodeMaintenanceTests(LwtTestCase):
    def test_a_host_enters_maintenance_once(self):
        SESSION.put(NODES, "node01", hostname="node01", status="NORMAL", maintenance_mode=False)
        enter = {"hostname": "node01", "status": "ENTERING_MAINTENANCE",
                 "maintenance_mode": False, "expected_status": "NORMAL"}
        _s1, first = call("/v1/node/maintenance", enter)
        _s2, second = call("/v1/node/maintenance", enter)
        self.assertTrue(first["applied"])
        self.assertFalse(second["applied"], "a second enter would start a second evacuation")
        self.assertEqual(second["current"]["status"], "ENTERING_MAINTENANCE")


# -- what the callers do with the answer -------------------------------------------------

class ClientContractTests(LwtTestCase):
    """`run_lwt` is duplicated in vali.py and spectrum_server.py; both are checked."""

    def clients(self):
        return (("vali", vali.run_lwt), ("spectrum_server", spectrum.run_lwt))

    def test_a_lost_race_is_not_reported_as_an_error(self):
        # Getting this wrong is what turns a correctly refused claim into a task failure,
        # and -- in the other direction -- a lost race into a second VM start.
        self.given_vm("web-01", host_ip="10.0.0.9", state="Running")
        for name, run_lwt in self.clients():
            ok, applied, current, error = run_lwt(
                "/v1/vm/claim", {"name": "web-01", "host_ip": "10.0.0.1"})
            self.assertTrue(ok, name)
            self.assertFalse(applied, name)
            self.assertEqual(current.get("host_ip"), "10.0.0.9", name)
            self.assertEqual(error, "", name)

    def test_a_won_race_reports_applied_with_no_current_values(self):
        self.given_vm("web-01", host_ip="")
        for name, run_lwt in self.clients():
            SESSION.put(VMS, "web-01", host_ip="")
            ok, applied, current, error = run_lwt(
                "/v1/vm/claim", {"name": "web-01", "host_ip": "10.0.0.1"})
            self.assertTrue(ok, name)
            self.assertTrue(applied, name)
            self.assertEqual(current, {}, name)
            self.assertEqual(error, "", name)

    def test_a_rejected_request_is_an_error_not_a_lost_race(self):
        for name, run_lwt in self.clients():
            ok, applied, _current, error = run_lwt("/v1/vm/claim", {"name": "web-01"})
            self.assertFalse(ok, name)
            self.assertFalse(applied, name)
            self.assertIn("host_ip", error, name)

    def test_daruk_being_down_is_an_error_and_never_an_applied_claim(self):
        # There is no cqlsh fallback on this path on purpose: an ownership write that
        # cannot be made conditional must not be made at all.
        for name, run_lwt in self.clients():
            module = vali if name == "vali" else spectrum
            module.DARUK_URL = "http://127.0.0.1:1"
            try:
                ok, applied, _current, error = run_lwt(
                    "/v1/vm/claim", {"name": "web-01", "host_ip": "10.0.0.1"}, timeout=2)
            finally:
                module.DARUK_URL = DARUK_TEST_URL
            self.assertFalse(ok, name)
            self.assertFalse(applied, name)
            self.assertTrue(error, name)


class ReconcilerTests(LwtTestCase):
    """`reconcile_local_vm` is Spectrum's write-back of what libvirt reports."""

    def test_a_vm_that_moved_away_keeps_its_placement(self):
        # The reconciler read host_ip == its own address, then the VM was migrated. Its
        # write must not land: clearing host_ip here unplaces a running VM, and the next
        # start boots a second copy of it.
        self.given_vm("web-01", host_ip="10.0.0.9", state="Running")
        self.assertFalse(spectrum.reconcile_local_vm("web-01", "10.0.0.1", "Stopped"))
        row = SESSION.row(VMS, "web-01")
        self.assertEqual(row["host_ip"], "10.0.0.9")
        self.assertEqual(row["state"], "Running")

    def test_a_vm_this_node_still_owns_is_written_back(self):
        self.given_vm("web-01", host_ip="10.0.0.1", state="Running")
        self.assertTrue(spectrum.reconcile_local_vm("web-01", "10.0.0.1", "Stopped"))
        row = SESSION.row(VMS, "web-01")
        self.assertEqual(row["host_ip"], "")
        self.assertEqual(row["state"], "Stopped")

    def test_a_running_vm_has_its_state_recorded_without_touching_placement(self):
        self.given_vm("web-01", host_ip="10.0.0.1", state="Stopped")
        self.assertTrue(spectrum.reconcile_local_vm("web-01", "10.0.0.1", "Running"))
        row = SESSION.row(VMS, "web-01")
        self.assertEqual(row["state"], "Running")
        self.assertEqual(row["host_ip"], "10.0.0.1")


class ValiClaimReleaseTests(LwtTestCase):
    """`release_failed_claim` runs after a start that could not be completed."""

    def test_a_failed_start_gives_the_placement_back(self):
        # Without this the VM is recorded as Running on a host it never booted on, and
        # every later start is refused because the row says someone owns it.
        self.given_vm("web-01", host_ip="10.0.0.1", state="Running")
        vali.release_failed_claim("web-01", "10.0.0.1")
        self.assertEqual(SESSION.row(VMS, "web-01")["host_ip"], "")

    def test_a_failed_start_does_not_disturb_a_placement_that_moved_on(self):
        self.given_vm("web-01", host_ip="10.0.0.9", state="Running")
        vali.release_failed_claim("web-01", "10.0.0.1")
        self.assertEqual(SESSION.row(VMS, "web-01")["host_ip"], "10.0.0.9")


if __name__ == "__main__":
    unittest.main()
