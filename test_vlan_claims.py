#!/usr/bin/env python3
"""Tests for the VLAN uniqueness constraint, which until now was a read and a hope.

`hydra.gatoway_networks` is keyed by `net_id`, so a VLAN id is an ordinary column and two
networks on VLAN 100 are perfectly legal as far as the database is concerned. Both
consoles read the table and refuse a clash before writing. That check is worth keeping --
it names the offending network and it catches the mistake an operator actually makes --
but a read followed by a write is two operations, and two creates a millisecond apart both
read "VLAN 100 is free" and both write it. The result is two networks in one broadcast
domain and neither operator told.

`hydra.gatoway_vlan_claims` is keyed by the VLAN id, which is the only thing two racing
creates share, and therefore the only thing an `IF NOT EXISTS` can serialise them on.

The tests that matter here are the concurrent ones. **The sequential case already passed
before any of this existed**, which is exactly why the bug survived: a test that creates
VLAN 100 twice in a row is answered by the advisory check and never reaches the claim at
all. So `ConcurrentCreateTests` stages the interleaving explicitly -- both creates finish
reading the network table before either writes -- and asserts that the advisory check did
*not* fire, because a run where it did would prove nothing about the claim.

The second thing being tested throughout is that a claim is never left behind. A stranded
claim makes its VLAN unusable forever, which is a worse failure than the duplicate the
claim prevents, so every path that takes one has to give it back: the create that cannot
write its row, the delete, and the edit that re-tags a network onto a different VLAN.

Run with:  python -m unittest test_vlan_claims
"""

import ast
import io
import json
import os
import re
import threading
import time
import unittest

# Installs the fake cassandra-driver, loads daruk.py and spectrum_server.py against it,
# and serves one Daruk over HTTP for the whole run. The claim endpoints are exercised
# through that Daruk rather than through a stub, so the operation table, the parameter
# binding and the [applied] parsing are all under test.
import test_daruk_lwt as base

import helios_schema as schema

HERE = os.path.dirname(os.path.abspath(__file__))

SESSION = base.SESSION
spectrum = base.spectrum
call = base.call

NETWORKS = "hydra.gatoway_networks"
CLAIMS = "hydra.gatoway_vlan_claims"

PHYSICAL_DIRECT = "7a68e0d6-11f8-4e89-9430-b3b44b8bc438"


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


# -- the network table, shared with the Daruk the claims go through ----------------------

_SELECT_NETWORKS = re.compile(
    r"\ASELECT JSON .*? FROM hydra\.gatoway_networks"
    r"(?:\s+WHERE\s+net_id\s*=\s*(?P<id>[0-9a-fA-F-]+))?\s*;\Z", re.S)
_INSERT_NETWORK = re.compile(
    r"\AINSERT INTO hydra\.gatoway_networks \(net_id, name, type, vlan_id\) VALUES "
    r"\((?P<id>[0-9a-fA-F-]+), '(?P<name>.*)', '(?P<type>.*)', (?P<vlan>\w+)\);\Z", re.S)
_DELETE_NETWORK = re.compile(
    r"\ADELETE FROM hydra\.gatoway_networks WHERE net_id = (?P<id>[0-9a-fA-F-]+);\Z")
_UPDATE_NETWORK = re.compile(
    r"\AUPDATE hydra\.gatoway_networks SET name = '(?P<name>.*)', vlan_id = (?P<vlan>\w+) "
    r"WHERE net_id = (?P<id>[0-9a-fA-F-]+);\Z", re.S)


class NetworkTable:
    """`run_cql_query` over the same store Daruk's fake session holds.

    One store rather than two, because the whole question is whether the claim table and
    the network table agree: a fake where the console writes to one and the claim to
    another can only ever confirm that each half works on its own.

    `pause_on_scan` is a barrier tripped by the unqualified read of the network table --
    the read both consoles do to decide whether a VLAN is free. Holding both creates there
    is how the race is staged rather than hoped for.
    """

    def __init__(self):
        self.statements = []
        self.lock = threading.Lock()
        self.pause_on_scan = None
        self.fail_on = None
        # The claim table as it stood at the instant the network row was written or
        # removed. Order is the property that matters on both paths and neither return
        # value shows it: the claim must exist before the row does, and must outlive it.
        self.claims_when_row_written = None
        self.claims_when_row_deleted = None

    def rows(self):
        return SESSION.store.setdefault(NETWORKS, {})

    def given_network(self, net_id, name, kind="vlan", vlan_id=None):
        self.rows()[net_id] = {
            "net_id": net_id, "name": name, "type": kind, "vlan_id": vlan_id}
        return net_id

    def given_claim(self, vlan_id, net_id, name="held", claimed_at_ms=1):
        # Epoch by default, so a claim built here is old enough to be taken over if its
        # network turns out not to exist. Freshness has its own test.
        SESSION.store.setdefault(CLAIMS, {})[vlan_id] = {
            "vlan_id": vlan_id, "net_id": net_id, "name": name,
            "claimed_at_ms": claimed_at_ms}
        return vlan_id

    def claims(self):
        return SESSION.store.setdefault(CLAIMS, {})

    def matching(self, pattern):
        rx = re.compile(pattern, re.S | re.I)
        with self.lock:
            return [s for s in self.statements if rx.search(s)]

    def __call__(self, cql, *args, **kwargs):
        with self.lock:
            self.statements.append(cql)
        if self.fail_on and re.search(self.fail_on, cql, re.S | re.I):
            return 1, "", "injected failure"

        statement = cql.strip()

        match = _SELECT_NETWORKS.match(statement)
        if match:
            with self.lock:
                if match.group("id"):
                    found = self.rows().get(match.group("id"))
                    rows = [found] if found else []
                else:
                    rows = list(self.rows().values())
            answer = "\n".join(json.dumps(row) for row in rows)
            if match.group("id") is None and self.pause_on_scan is not None:
                # Both creates have now read the table and neither has written. This is
                # the interleaving the bug lives in.
                self.pause_on_scan.wait()
            return 0, answer, ""

        match = _INSERT_NETWORK.match(statement)
        if match:
            vlan = match.group("vlan")
            with self.lock:
                self.claims_when_row_written = dict(self.claims())
                self.rows()[match.group("id")] = {
                    "net_id": match.group("id"),
                    "name": match.group("name"),
                    "type": match.group("type"),
                    "vlan_id": None if vlan == "null" else int(vlan),
                }
            return 0, "", ""

        match = _UPDATE_NETWORK.match(statement)
        if match:
            vlan = match.group("vlan")
            with self.lock:
                row = self.rows().setdefault(match.group("id"), {"net_id": match.group("id")})
                row["name"] = match.group("name")
                row["vlan_id"] = None if vlan == "null" else int(vlan)
            return 0, "", ""

        match = _DELETE_NETWORK.match(statement)
        if match:
            with self.lock:
                self.claims_when_row_deleted = dict(self.claims())
                self.rows().pop(match.group("id"), None)
            return 0, "", ""

        if statement.startswith("SELECT JSON name, network_id FROM hydra.vms"):
            return 0, "", ""

        return 0, "", ""


class FakeHeaders(dict):
    def get(self, key, default=None):
        for existing, value in self.items():
            if existing.lower() == str(key).lower():
                return value
        return default

    def __contains__(self, key):
        return any(existing.lower() == str(key).lower() for existing in self.keys())


def drive(path, body):
    """One request through the real handler; returns (status, body).

    A local copy rather than the one in test_spectrum_data_layer, deliberately: that file
    loads its own spectrum_server against the real driver, and these tests need the copy
    loaded against the fake session that Daruk is also serving. Two modules, one file, and
    only one of them has a database.
    """
    handler = object.__new__(spectrum.SpectrumHandler)
    handler.path = path
    handler.client_address = ("127.0.0.1", 54321)
    payload = json.dumps(body).encode("utf-8")
    headers = FakeHeaders()
    headers["Content-Length"] = str(len(payload))
    handler.headers = headers
    handler.rfile = io.BytesIO(payload)

    captured = {}

    def send_json(status, data):
        captured["status"] = status
        captured["body"] = data

    handler.send_json = send_json
    handler.do_POST()
    return captured.get("status"), captured.get("body")


class VlanClaimTestCase(unittest.TestCase):
    def setUp(self):
        SESSION.store.clear()
        SESSION.prepared.clear()
        self.hydra = NetworkTable()
        self._saved_query = spectrum.run_cql_query
        self._saved_url = spectrum.DARUK_URL
        spectrum.run_cql_query = self.hydra
        spectrum.DARUK_URL = base.DARUK_TEST_URL

    def tearDown(self):
        spectrum.run_cql_query = self._saved_query
        spectrum.DARUK_URL = self._saved_url


# -- the endpoints themselves ------------------------------------------------------------

class DarukClaimEndpointTests(VlanClaimTestCase):
    def claim(self, vlan, net_id, name="net"):
        return call("/v1/network/claim-vlan",
                    {"vlan_id": vlan, "net_id": net_id, "name": name, "claimed_at_ms": 1})

    def test_a_free_vlan_is_claimed(self):
        status, body = self.claim(100, "aaa")
        self.assertEqual(status, 200)
        self.assertTrue(body["applied"])
        self.assertEqual(SESSION.row(CLAIMS, 100)["net_id"], "aaa")

    def test_a_second_claim_on_the_same_vlan_is_refused_and_names_the_holder(self):
        self.claim(100, "aaa", name="production")
        status, body = self.claim(100, "bbb", name="staging")
        self.assertEqual(status, 200, "a lost race is not an error")
        self.assertFalse(body["applied"])
        self.assertEqual(body["current"]["net_id"], "aaa")
        self.assertEqual(body["current"]["name"], "production")
        self.assertEqual(SESSION.row(CLAIMS, 100)["net_id"], "aaa")

    def test_the_claim_lists_its_columns(self):
        # "INSERT INTO ... JSON ? IF NOT EXISTS" is accepted by Scylla and then runs
        # unconditionally: no [applied] column, and the existing row overwritten. Here
        # that would hand VLAN 100 to the second caller and report it as a win.
        cql = base.daruk.LWT_OPS["/v1/network/claim-vlan"]["cql"]
        self.assertNotIn("JSON", cql)
        self.assertIn("IF NOT EXISTS", cql)
        for column in ("vlan_id", "net_id", "name", "claimed_at_ms"):
            self.assertIn(column, cql)

    def test_a_release_only_lands_for_the_network_that_holds_the_claim(self):
        # A release matching on the VLAN alone would let a late cleanup from a create that
        # failed drop the claim a later create legitimately holds.
        self.claim(100, "aaa")
        _status, body = call("/v1/network/release-vlan", {"vlan_id": 100, "net_id": "bbb"})
        self.assertFalse(body["applied"])
        self.assertEqual(body["current"]["net_id"], "aaa")
        self.assertIsNotNone(SESSION.row(CLAIMS, 100))

        _status, body = call("/v1/network/release-vlan", {"vlan_id": 100, "net_id": "aaa"})
        self.assertTrue(body["applied"])
        self.assertIsNone(SESSION.row(CLAIMS, 100))

    def test_releasing_a_claim_that_is_not_there_is_refused_with_a_null_holder(self):
        # The shape that lets "somebody else holds it" be told from "there was nothing to
        # release" -- read off a live Scylla, where a DELETE ... IF against a missing row
        # answers [applied] = false with every conditioned column null. A caller that read
        # that as a lost race would turn an ordinary second delete into a conflict.
        _status, body = call("/v1/network/release-vlan", {"vlan_id": 100, "net_id": "aaa"})
        self.assertFalse(body["applied"])
        self.assertIsNone(body["current"]["net_id"])

    def test_a_takeover_is_conditional_on_the_claim_it_read(self):
        self.claim(100, "gone")
        _status, body = call("/v1/network/reclaim-vlan", {
            "vlan_id": 100, "net_id": "new", "name": "new", "claimed_at_ms": 2,
            "expected_net_id": "somebody-else"})
        self.assertFalse(body["applied"], "two callers finding one orphan must not both win")

        _status, body = call("/v1/network/reclaim-vlan", {
            "vlan_id": 100, "net_id": "new", "name": "new", "claimed_at_ms": 2,
            "expected_net_id": "gone"})
        self.assertTrue(body["applied"])
        self.assertEqual(SESSION.row(CLAIMS, 100)["net_id"], "new")

    def test_a_takeover_will_not_default_its_expectation(self):
        # A default would match the claim whose net_id has never been written and turn the
        # takeover into an unconditional seizure of whatever is there.
        status, body = call("/v1/network/reclaim-vlan", {
            "vlan_id": 100, "net_id": "new", "claimed_at_ms": 2})
        self.assertEqual(status, 400)
        self.assertIn("expected_net_id", body["error"])


# -- creating through the console --------------------------------------------------------

class SequentialCreateTests(VlanClaimTestCase):
    """The case that already passed, kept because it is the message an operator sees."""

    def create(self, name, vlan=None, kind="vlan"):
        payload = {"name": name, "type": kind}
        if vlan is not None:
            payload["vlan_id"] = vlan
        return drive("/api/networks/create", payload)

    def test_a_network_on_a_free_vlan_is_created_and_holds_the_claim(self):
        status, body = self.create("production", 100)
        self.assertEqual(status, 201)
        self.assertEqual(self.hydra.claims()[100]["net_id"], body["net_id"])

    def test_the_claim_is_taken_before_the_network_row_is_written(self):
        # The claim is what decides the race, so anything written before it is written on
        # the strength of a read that may already be stale -- and Gatoway polls the
        # network table every five seconds, so a row that exists for a moment is a network
        # that may be configured on every host.
        self.create("production", 100)
        self.assertIn(
            100, self.hydra.claims_when_row_written,
            "the network row was written before the VLAN was claimed")

    def test_a_direct_network_claims_nothing(self):
        # It has no tag to collide with, which is why hydra.gatoway_networks stores a null
        # vlan_id for every one of them.
        status, _body = self.create("flat", kind="direct")
        self.assertEqual(status, 201)
        self.assertEqual(self.hydra.claims(), {})

    def test_the_advisory_check_still_answers_first_and_names_the_clash(self):
        self.hydra.given_network("11111111-1111-1111-1111-111111111111", "production",
                                 vlan_id=100)
        status, body = self.create("staging", 100)
        self.assertEqual(status, 400)
        self.assertIn("already assigned to network 'production'", body["error"])

    def test_a_claim_the_console_cannot_see_still_refuses_the_create(self):
        # The claim firing on its own is the whole point: the network table says VLAN 100
        # is free, and it is not. That is what a concurrent create looks like from the
        # loser's side, one round trip later.
        self.hydra.given_network("22222222-2222-2222-2222-222222222222", "production",
                                 vlan_id=None)
        self.hydra.given_claim(100, "22222222-2222-2222-2222-222222222222", "production")
        status, body = self.create("staging", 100)
        self.assertEqual(status, 409)
        self.assertIn("VLAN 100", body["error"])
        self.assertIn("production", body["error"])
        self.assertIn("broadcast domain", body["error"])

    def test_a_create_that_cannot_write_its_row_gives_the_claim_back(self):
        # Without this the VLAN is spoken for by a network that does not exist, and the
        # operator -- who was told only that the write failed -- has no reason to suspect
        # that retrying with the same VLAN cannot work.
        self.hydra.fail_on = r"INSERT INTO hydra\.gatoway_networks"
        status, _body = self.create("production", 100)
        self.assertEqual(status, 500)
        self.assertEqual(self.hydra.claims(), {},
                         "the claim outlived the create that took it")

        self.hydra.fail_on = None
        status, _body = self.create("production", 100)
        self.assertEqual(status, 201, "VLAN 100 was left unusable by the failed create")

    def test_a_claim_stranded_by_a_create_that_never_finished_is_taken_over(self):
        # The failure a release cannot cover: the node dies between claiming and writing
        # the row. Left alone that VLAN is unusable for good, which is worse than the
        # duplicate the claim prevents, so a create that loses to a claim whose network
        # does not exist takes it over.
        self.hydra.given_claim(100, "33333333-3333-3333-3333-333333333333", "half-created")
        status, body = self.create("production", 100)
        self.assertEqual(status, 201)
        self.assertEqual(self.hydra.claims()[100]["net_id"], body["net_id"])

    def test_a_claim_younger_than_the_grace_period_is_left_alone(self):
        # Otherwise the repair breaks what it protects: for the few milliseconds between a
        # create's claim and its row, its network legitimately does not exist, and a
        # second create checking in that window would take the claim from a create that is
        # still running -- and both would write a network on VLAN 100.
        self.hydra.given_claim(100, "66666666-6666-6666-6666-666666666666", "in flight",
                               claimed_at_ms=int(time.time() * 1000))
        status, body = self.create("staging", 100)
        self.assertEqual(status, 409)
        self.assertIn("has not finished writing its row", body["error"])
        self.assertEqual(self.hydra.claims()[100]["name"], "in flight")

    def test_a_claim_is_not_taken_over_when_the_holder_cannot_be_looked_up(self):
        # Guessing here takes a VLAN away from a network that may be live, which is worse
        # than either failure it is choosing between.
        holder = "44444444-4444-4444-4444-444444444444"
        self.hydra.given_claim(100, holder, "production")
        self.hydra.fail_on = r"WHERE net_id = " + holder
        status, body = self.create("staging", 100)
        self.assertEqual(status, 409)
        self.assertIn("could not be read", body["error"])
        self.assertEqual(self.hydra.claims()[100]["net_id"], holder)


class ConcurrentCreateTests(VlanClaimTestCase):
    """Two creates that both read "VLAN 100 is free", which is the actual bug.

    The sequential version of this passes with no claim table at all: the second create
    reads the first one's row and the advisory check refuses it. Staging the interleaving
    is therefore not decoration -- it is the only arrangement in which the claim is the
    thing being tested.
    """

    def race(self, vlan, names=("alpha", "bravo")):
        self.hydra.pause_on_scan = threading.Barrier(len(names), timeout=15)
        results = {}

        def create(name):
            results[name] = drive(
                "/api/networks/create", {"name": name, "type": "vlan", "vlan_id": vlan})

        threads = [threading.Thread(target=create, args=(name,)) for name in names]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        self.assertFalse(any(thread.is_alive() for thread in threads),
                         "a create never returned; the barrier did not release")
        return results

    def test_only_one_of_two_racing_creates_gets_the_vlan(self):
        results = self.race(100)
        statuses = sorted(status for status, _body in results.values())
        self.assertEqual(statuses, [201, 409])

        tagged = [row for row in self.hydra.rows().values() if row.get("vlan_id") == 100]
        self.assertEqual(len(tagged), 1, "two networks were written onto VLAN 100")
        self.assertEqual(self.hydra.claims()[100]["net_id"], tagged[0]["net_id"])

    def test_the_race_really_was_a_race(self):
        # Belt and braces on the test itself. If either create had reached the network
        # table after the other had written to it, the advisory check would have refused
        # it and the claim would never have been consulted -- and this file would be
        # asserting nothing at all. Both reads happen before either write.
        self.race(100)
        order = self.hydra.statements
        scans = [i for i, s in enumerate(order)
                 if s.strip() == "SELECT JSON * FROM hydra.gatoway_networks;"]
        writes = [i for i, s in enumerate(order)
                  if s.strip().startswith("INSERT INTO hydra.gatoway_networks")]
        self.assertEqual(len(scans), 2)
        self.assertEqual(len(writes), 1)
        self.assertTrue(all(scan < writes[0] for scan in scans),
                        "one create read the table after the other had written to it")

    def test_the_loser_is_told_about_the_vlan_and_not_about_a_write(self):
        # A backstop that fires with an illegible error is a backstop that gets removed.
        # Which of the two the loser sees depends on whether the winner had written its
        # row yet, and both are about the VLAN rather than about the database.
        results = self.race(100)
        loser = [body for status, body in results.values() if status == 409]
        self.assertEqual(len(loser), 1)
        message = loser[0]["error"]
        self.assertIn("VLAN 100", message)
        self.assertTrue(
            "already assigned" in message or "has not finished writing its row" in message,
            message)

    def test_the_vlan_is_claimed_once_however_many_creates_race(self):
        results = self.race(200, names=("a", "b", "c", "d"))
        statuses = sorted(status for status, _body in results.values())
        self.assertEqual(statuses, [201, 409, 409, 409])
        self.assertEqual(len(self.hydra.claims()), 1)


# -- deleting and re-tagging -------------------------------------------------------------

class ReleaseTests(VlanClaimTestCase):
    def given_tagged_network(self, name="production", vlan=100):
        status, body = drive("/api/networks/create",
                             {"name": name, "type": "vlan", "vlan_id": vlan})
        self.assertEqual(status, 201)
        return body["net_id"]

    def test_deleting_a_network_frees_its_vlan(self):
        net_id = self.given_tagged_network()
        status, _body = drive("/api/networks/delete", {"net_id": net_id})
        self.assertEqual(status, 200)
        self.assertEqual(self.hydra.claims(), {})

        status, _body = drive("/api/networks/create",
                              {"name": "second", "type": "vlan", "vlan_id": 100})
        self.assertEqual(status, 201, "VLAN 100 stayed claimed after its network was gone")

    def test_the_claim_is_released_after_the_row_is_gone_and_not_before(self):
        # Releasing first leaves a window in which the network still exists and its VLAN
        # is free, so a create racing the delete takes VLAN 100 while a network still
        # carries it -- the duplicate the claim exists to prevent, reintroduced here.
        net_id = self.given_tagged_network()
        drive("/api/networks/delete", {"net_id": net_id})
        self.assertIn(
            100, self.hydra.claims_when_row_deleted,
            "the claim was given back before the row was gone, so a create racing this "
            "delete could take VLAN 100 from a network that still had it")
        self.assertEqual(self.hydra.claims(), {})
        self.assertIsNone(self.hydra.rows().get(net_id))

    def test_a_delete_whose_network_cannot_be_read_is_refused_rather_than_stranding_it(self):
        net_id = self.given_tagged_network()
        self.hydra.fail_on = r"SELECT JSON net_id, vlan_id FROM hydra\.gatoway_networks"
        status, body = drive("/api/networks/delete", {"net_id": net_id})
        self.assertEqual(status, 503)
        self.assertIn("VLAN claim", body["error"])
        self.assertIn(net_id, self.hydra.rows())

    def test_a_network_id_that_is_not_a_uuid_is_refused(self):
        # The id is interpolated into the statement, and the cqlsh fallback executes
        # `;`-separated statements.
        status, _body = drive("/api/networks/delete",
                              {"net_id": "1; DROP TABLE hydra.gatoway_networks--"})
        self.assertEqual(status, 400)

    def test_retagging_a_network_claims_the_new_vlan_and_frees_the_old(self):
        # Without this an edit is the way round the constraint: create on a free VLAN,
        # then edit onto the one somebody else already has.
        net_id = self.given_tagged_network(vlan=100)
        status, _body = drive("/api/networks/update",
                              {"net_id": net_id, "name": "production", "vlan_id": 200})
        self.assertEqual(status, 200)
        self.assertEqual(sorted(self.hydra.claims()), [200])
        self.assertEqual(self.hydra.claims()[200]["net_id"], net_id)

    def test_retagging_onto_a_claimed_vlan_is_refused_and_the_old_claim_survives(self):
        net_id = self.given_tagged_network(vlan=100)
        self.hydra.given_network("55555555-5555-5555-5555-555555555555", "other",
                                 vlan_id=None)
        self.hydra.given_claim(300, "55555555-5555-5555-5555-555555555555", "other")
        status, body = drive("/api/networks/update",
                             {"net_id": net_id, "name": "production", "vlan_id": 300})
        self.assertEqual(status, 409)
        self.assertIn("VLAN 300", body["error"])
        self.assertEqual(self.hydra.claims()[100]["net_id"], net_id,
                         "a refused re-tag dropped the claim the network still holds")

    def test_a_rename_that_leaves_the_vlan_alone_touches_no_claim(self):
        net_id = self.given_tagged_network(vlan=100)
        before = dict(self.hydra.claims()[100])
        status, _body = drive("/api/networks/update",
                              {"net_id": net_id, "name": "renamed", "vlan_id": 100})
        self.assertEqual(status, 200)
        self.assertEqual(self.hydra.claims()[100], before)

    def test_a_retag_that_cannot_be_written_gives_the_new_claim_back(self):
        net_id = self.given_tagged_network(vlan=100)
        self.hydra.fail_on = r"UPDATE hydra\.gatoway_networks"
        status, _body = drive("/api/networks/update",
                              {"net_id": net_id, "name": "production", "vlan_id": 200})
        self.assertEqual(status, 500)
        self.assertEqual(sorted(self.hydra.claims()), [100],
                         "the re-tag that failed kept VLAN 200")


# -- adopting a cluster that already has networks ----------------------------------------

class BackfillDatabase:
    """The three statements `backfill_vlan_claims` issues, in the shapes Daruk returns."""

    def __init__(self, networks, claims=None):
        self.networks = list(networks)
        self.claims = dict(claims or {})
        self.statements = []
        self.fail_read = False
        self.corrupt_read = False

    def __call__(self, cql):
        self.statements.append(cql)

        if cql.startswith("SELECT JSON net_id, name, vlan_id FROM hydra.gatoway_networks"):
            if self.fail_read:
                return 1, "", "Hydra is unavailable"
            if self.corrupt_read:
                return 0, '{"net_id": "a", "name": "production", "vlan_id": }', ""
            return 0, "\n".join(json.dumps(row) for row in self.networks), ""

        if cql.startswith("INSERT INTO " + CLAIMS):
            vlan = int(re.search(r"VALUES \((\d+),", cql).group(1))
            net_id, name = re.findall(r"'((?:[^']|'')*)'", cql)[:2]
            if vlan in self.claims:
                held = self.claims[vlan]
                return 0, "False %d %s %s 1" % (vlan, held["net_id"], held["name"]), ""
            self.claims[vlan] = {"net_id": net_id, "name": name.replace("''", "'")}
            return 0, "True", ""

        if cql.startswith("SELECT JSON net_id, name FROM " + CLAIMS):
            vlan = int(re.search(r"vlan_id = (\d+)", cql).group(1))
            held = self.claims.get(vlan)
            return 0, json.dumps(held) if held else "", ""

        return 0, "", ""


class BackfillTests(unittest.TestCase):
    """A cluster adopting the claim table already has networks on VLANs."""

    def backfill(self, networks, claims=None):
        db = BackfillDatabase(networks, claims)
        notes = []
        schema.backfill_vlan_claims(db, notes.append, 1)
        return db, notes

    def test_every_vlan_in_use_ends_up_claimed(self):
        # Without this the first create after the migration wins a claim on a VLAN that is
        # visibly in use, which is the duplicate this table exists to prevent arrived at
        # from the other direction.
        db, notes = self.backfill([
            {"net_id": "a", "name": "production", "vlan_id": 100},
            {"net_id": "b", "name": "staging", "vlan_id": 200},
        ])
        self.assertEqual(sorted(db.claims), [100, 200])
        self.assertEqual(db.claims[100]["name"], "production")
        self.assertEqual(notes, [])

    def test_a_direct_network_carries_no_vlan_and_claims_nothing(self):
        # The shape of the seeded Physical-Direct row on the live cluster: vlan_id null.
        db, notes = self.backfill([
            {"net_id": PHYSICAL_DIRECT, "name": "Physical-Direct", "vlan_id": None},
        ])
        self.assertEqual(db.claims, {})
        self.assertEqual(notes, [])

    def test_existing_duplicates_are_reported_rather_than_failing_the_migration(self):
        # A cluster with two networks on VLAN 100 is *why* this table is being added.
        # Refusing to migrate would leave exactly that cluster with no constraint and no
        # way to get one, and it would do it at daemon start.
        db, notes = self.backfill([
            {"net_id": "a", "name": "production", "vlan_id": 100},
            {"net_id": "b", "name": "staging", "vlan_id": 100},
        ])
        self.assertEqual(len(notes), 1)
        self.assertIn("VLAN 100", notes[0])
        self.assertIn("production", notes[0])
        self.assertIn("staging", notes[0])
        self.assertIn("Re-tag or delete", notes[0])
        self.assertEqual(len(db.claims), 1)

    def test_a_duplicate_is_not_resolved_by_deleting_anything(self):
        # Both are configuration changes that take a guest's network away, and a migration
        # running at daemon start is the worst possible place to make one.
        db, _notes = self.backfill([
            {"net_id": "a", "name": "production", "vlan_id": 100},
            {"net_id": "b", "name": "staging", "vlan_id": 100},
        ])
        writes = [s for s in db.statements
                  if re.match(r"\s*(delete|update)\b", s, re.I)]
        self.assertEqual(writes, [])

    def test_the_winner_is_the_same_on_every_node(self):
        # Every node runs this and they must not disagree about who holds VLAN 100.
        forwards, _ = self.backfill([
            {"net_id": "a", "name": "production", "vlan_id": 100},
            {"net_id": "b", "name": "staging", "vlan_id": 100},
        ])
        backwards, _ = self.backfill([
            {"net_id": "b", "name": "staging", "vlan_id": 100},
            {"net_id": "a", "name": "production", "vlan_id": 100},
        ])
        self.assertEqual(forwards.claims[100]["net_id"], backwards.claims[100]["net_id"])

    def test_running_it_twice_claims_nothing_new_and_reports_nothing(self):
        networks = [{"net_id": "a", "name": "production", "vlan_id": 100}]
        first, _notes = self.backfill(networks)
        db = BackfillDatabase(networks, first.claims)
        notes = []
        schema.backfill_vlan_claims(db, notes.append, 2)
        self.assertEqual(db.claims, first.claims)
        self.assertEqual(notes, [], "a re-run reported a network as clashing with itself")

    def test_an_unreadable_network_table_leaves_the_migration_unrecorded(self):
        db = BackfillDatabase([])
        db.fail_read = True
        with self.assertRaises(schema.SchemaError):
            schema.backfill_vlan_claims(db, lambda note: None, 1)

    def test_a_row_that_does_not_parse_is_not_treated_as_no_rows(self):
        # A partial set is worse than no set: acting on it looks like the missing networks
        # do not exist, and their VLANs would be handed to the next create.
        db = BackfillDatabase([])
        db.corrupt_read = True
        with self.assertRaises(schema.SchemaError):
            schema.backfill_vlan_claims(db, lambda note: None, 1)


class MigrationTests(unittest.TestCase):
    def migration(self):
        found = [m for m in schema.MIGRATIONS if m["id"] == "0009-vlan-claims"]
        self.assertEqual(len(found), 1)
        return found[0]

    def test_the_claim_table_is_keyed_by_the_vlan_id(self):
        # The whole point. A lightweight transaction cannot span partitions, so an
        # exclusion between two creates has to live in a row they both condition on, and
        # a VLAN id is the only thing they share.
        statement = self.migration()["statements"][0]
        self.assertIn("vlan_id int PRIMARY KEY", statement)
        self.assertIn("IF NOT EXISTS", statement)

    def test_the_claim_statements_agree_across_the_tiers(self):
        # A claim the migration made must be releasable by a delete the console makes, so
        # the column names cannot drift between helios_schema, Daruk and the Elixir tier.
        columns = ("vlan_id", "net_id", "name", "claimed_at_ms")
        migration = self.migration()["statements"][0]
        daruk_claim = base.daruk.LWT_OPS["/v1/network/claim-vlan"]["cql"]
        elixir = read(os.path.join(HERE, "spectrum_phx", "lib", "spectrum_phx", "networking.ex"))
        for column in columns:
            self.assertIn(column, migration)
            self.assertIn(column, daruk_claim)
        self.assertIn("hydra.gatoway_vlan_claims", elixir)
        for column in columns:
            self.assertIn(column, elixir)

    def test_adding_a_backfill_did_not_change_any_recorded_checksum(self):
        # Every cluster already carries checksums for 0001-0008. If hashing had changed
        # for migrations without a backfill, every one of them would raise
        # SchemaDivergence on the next start and no daemon would come up.
        for migration in schema.MIGRATIONS:
            if migration.get("backfill") is not None:
                continue
            digest = schema.checksum({"id": migration["id"],
                                      "statements": migration["statements"]})
            self.assertEqual(digest, schema.checksum(migration))


class SchemaExecutorTests(unittest.TestCase):
    """Every daemon must hand `ensure_schema` an executor that can run a conditional.

    `run_cql_query` refuses one, correctly, because it cannot report whether the condition
    held -- and helios_schema's own cluster lock is an IF NOT EXISTS insert. Four daemons
    passed the guarded executor, and it never showed: `ensure_schema` returns before it
    takes the lock when nothing is pending, so on a cluster with an up-to-date schema the
    call never reaches a conditional statement. It surfaces on the first day a migration
    is added, as every daemon failing at once.
    """

    CALLERS = ("catalyst.py", "check_updates.py", "lanayru.py", "spectrum_server.py",
               "vali.py")

    def executors(self, filename):
        tree = ast.parse(read(os.path.join(HERE, filename)), filename=filename)
        found = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
            if name != "ensure_schema" or not node.args:
                continue
            first = node.args[0]
            found.append(first.id if isinstance(first, ast.Name) else ast.dump(first))
        return found

    def test_every_caller_passes_the_unguarded_executor(self):
        for filename in self.CALLERS:
            executors = self.executors(filename)
            self.assertTrue(executors, f"{filename} no longer calls ensure_schema")
            for executor in executors:
                self.assertEqual(
                    executor, "run_conditional_cql_query",
                    f"{filename} hands ensure_schema an executor that refuses the "
                    f"conditional statements the schema lock is made of")


if __name__ == "__main__":
    unittest.main()
