#!/usr/bin/env python3
"""Tests for Urbosa's resource reclamation and its transit /30 allocation.

Two failures these exist to prevent, pulling in opposite directions.

The first is the leak. Deleting a segment or a router from the database used to
delete nothing from the host: the namespace kept routing, the bridge kept
forwarding, the tunnel kept carrying the VNI. A segment an operator removed for
a reason went on passing traffic until the machine was rebooted.

The second is the cure being worse. A collector that reads "absent from the
database" as "delete it" will, on the day the database is briefly unreachable,
delete every bridge on the host and take the NIC out of every running VM. Every
refusal asserted below is a specific way that can happen: a bridge with a guest
tap on it, a bridge holding an address another component put there, a namespace
with a process in it that Urbosa did not start, and any observation that could
not be made at all.

Between them sits the transit pool. `md5(router_id)[:4] % 16384` handed two
routers the same /30 with no check anywhere, and the Tier-0 then had routes to
two tenants' subnets pointing at one next hop - silent misrouting, first
expected at around 150 routers. The allocation is recorded now, so the tests
below check the two properties a recorded allocation has to have: two routers
never share one, and a slot is still the same slot after a restart.

`FakeHost` answers the commands the reclaimer runs with output transcribed from
a live Helios node - iproute2 6.17, kernel 6.12 - including the details that
decide the outcome: `ip link show master` exiting 255 for a missing bridge and
printing `[]` for an empty one, `bridge fdb` flagging flood entries `self`, and
`link_netnsid` naming the namespace a veth's far end landed in.

Run with:  python -m unittest test_urbosa_reclaim
"""

import contextlib
import importlib.util
import io
import json
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_module(alias, filename):
    spec = importlib.util.spec_from_file_location(alias, os.path.join(HERE, filename))
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


urbosa = load_module("urbosa_under_test", "urbosa.py")


# -- a stand-in for the host's network stack ---------------------------------------------

class FakeNetns:
    def __init__(self, nsid=None, links=None, procs=None, routes=None):
        self.nsid = nsid
        self.links = list(links or ["lo"])
        # pid -> argv, exactly as /proc/<pid>/cmdline splits
        self.procs = dict(procs or {})
        self.routes = list(routes or [])


class FakeHost:
    """The subset of iproute2 and bridge(8) that Urbosa's reclaimer invokes.

    Output shapes come from a live node rather than from the manual pages,
    because the shapes are what the parsers get wrong:

      * `ip -json -o link show master br-ov-99999` prints `[]` and exits 0 for a
        bridge with nothing attached, and exits 255 with a message on stderr for
        a bridge that does not exist. The reclaimer has to tell those apart:
        empty permits deletion, missing-or-unreadable must not.
      * `ip netns list` prints `ns-t1-aabbccdd (id: 0)` only for namespaces that
        have been assigned an nsid, which happens when a link's peer is moved
        into them.
      * `bridge -json fdb show dev vxlan-N` returns the head-end flood list as
        entries with mac 00:00:00:00:00:00 and flags ["self"], alongside the
        bridge's own permanent MAC entries, which carry "master" instead.

    Every command that changes something is recorded in `executed` and applied
    to the model, so a test can assert both that the right command ran and that
    the object is really gone. An unrecognised command raises rather than
    returning success: a new command in urbosa.py should fail loudly here.
    """

    def __init__(self):
        self.links = {}
        self.netns = {}
        self.executed = []
        self.unreadable = set()

    # -- building a host -------------------------------------------------------
    def add_link(self, name, kind, master=None, netnsid=None, addrs=()):
        self.links[name] = {"kind": kind, "master": master, "netnsid": netnsid,
                            "addrs": list(addrs)}
        return self

    def add_netns(self, name, **kwargs):
        self.netns[name] = FakeNetns(**kwargs)
        return self

    def ports_of(self, bridge):
        return sorted(n for n, a in self.links.items() if a.get("master") == bridge)

    # -- the command surface ---------------------------------------------------
    def run(self, cmd):
        cmd = cmd.strip()
        for pattern, handler in self._ROUTES:
            match = re.match(pattern, cmd)
            if match:
                return handler(self, match, cmd)
        raise AssertionError(f"FakeHost was handed a command it cannot run: {cmd}")

    def argv(self, pid):
        for ns in self.netns.values():
            if str(pid) in ns.procs:
                return list(ns.procs[str(pid)])
        return []

    # -- reads -----------------------------------------------------------------
    def _netns_list(self, _match, _cmd):
        lines = []
        for name in sorted(self.netns):
            nsid = self.netns[name].nsid
            lines.append(f"{name} (id: {nsid})" if nsid is not None else name)
        return 0, "\n".join(lines), ""

    def _link_show_all(self, _match, _cmd):
        entries = []
        for name, attrs in self.links.items():
            entry = {"ifname": name, "linkinfo": {"info_kind": attrs["kind"]}}
            if attrs.get("master"):
                entry["master"] = attrs["master"]
            if attrs.get("netnsid") is not None:
                entry["link_netnsid"] = attrs["netnsid"]
            entries.append(entry)
        return 0, json.dumps(entries), ""

    def _link_show_master(self, match, _cmd):
        bridge = match.group("bridge")
        if bridge not in self.links or bridge in self.unreadable:
            return 255, "", f'Error: argument "{bridge}" is wrong: Device does not exist'
        return 0, json.dumps([{"ifname": p} for p in self.ports_of(bridge)]), ""

    def _addr_show(self, match, _cmd):
        name = match.group("name")
        if name not in self.links or name in self.unreadable:
            return 255, "", f'Device "{name}" does not exist.'
        # Every device that is up carries a kernel-assigned link-local address,
        # whether or not anybody configured anything. Observed on a live node:
        # treating it as a configured address refused to reclaim any bridge at
        # all, so the fake produces it too.
        addrs = [{"family": "inet6", "local": "fe80::a46a:67ff:fe8a:381c",
                  "prefixlen": 64, "scope": "link", "protocol": "kernel_ll"}]
        addrs += [{"family": "inet", "local": a, "prefixlen": 24, "scope": "global"}
                  for a in self.links[name]["addrs"]]
        return 0, json.dumps([{"ifname": name, "addr_info": addrs}]), ""

    def _ns_link_show(self, match, _cmd):
        ns = self.netns.get(match.group("ns"))
        if ns is None or match.group("ns") in self.unreadable:
            return 255, "", "Cannot open network namespace"
        return 0, json.dumps([{"ifname": n} for n in ns.links]), ""

    def _ns_pids(self, match, _cmd):
        ns = self.netns.get(match.group("ns"))
        if ns is None:
            return 255, "", "Cannot open network namespace"
        return 0, "\n".join(sorted(ns.procs)), ""

    def _ns_route_show(self, match, _cmd):
        ns = self.netns.get(match.group("ns"))
        if ns is None:
            return 255, "", "Cannot open network namespace"
        return 0, json.dumps([{"dst": d, "gateway": g} for d, g in ns.routes]), ""

    def _fdb_show(self, match, _cmd):
        device = match.group("dev")
        if device not in self.links:
            return 255, "", f'Cannot find device "{device}"'
        entries = [{"mac": "3e:bb:c1:a8:17:7b", "flags": [],
                    "master": self.links[device].get("master"), "state": "permanent"}]
        for dst in self.links[device].get("flood", []):
            entries.append({"mac": "00:00:00:00:00:00", "dst": dst,
                            "flags": ["self"], "state": "permanent"})
        for mac, dst in self.links[device].get("learned", []):
            entries.append({"mac": mac, "dst": dst, "flags": ["self"], "state": "stale"})
        return 0, json.dumps(entries), ""

    # -- writes ----------------------------------------------------------------
    @staticmethod
    def _peer_of(name):
        """The other end of a veth, by the naming Urbosa creates them with."""
        for left, right in (("veth-ov-", "veth-t1-"), ("t0-", "t1-")):
            if name.startswith(left):
                return right + name[len(left):]
            if name.startswith(right):
                return left + name[len(right):]
        return None

    def _link_delete(self, match, cmd):
        self.executed.append(cmd)
        name = match.group("name")
        if name not in self.links:
            return 1, "", f'Cannot find device "{name}"'
        del self.links[name]
        # Deleting a veth takes its peer with it, wherever that peer lives. That
        # is what made the reclaimer's second proposal for a pair report
        # "Cannot find device" on the live node.
        peer = self._peer_of(name)
        if peer:
            self.links.pop(peer, None)
            for ns in self.netns.values():
                if peer in ns.links:
                    ns.links.remove(peer)
        return 0, "", ""

    def _link_set(self, match, cmd):
        self.executed.append(cmd)
        if match.group("name") not in self.links:
            return 255, "", f'Cannot find device "{match.group("name")}"'
        return 0, "", ""

    def _netns_del(self, match, cmd):
        self.executed.append(cmd)
        self.netns.pop(match.group("ns"), None)
        return 0, "", ""

    def _kill(self, match, cmd):
        self.executed.append(cmd)
        for ns in self.netns.values():
            ns.procs.pop(match.group("pid"), None)
        return 0, "", ""

    def _fdb_del(self, match, cmd):
        self.executed.append(cmd)
        link = self.links.get(match.group("dev"))
        if link and match.group("dst") in link.get("flood", []):
            link["flood"].remove(match.group("dst"))
        return 0, "", ""

    def _route_del(self, match, cmd):
        self.executed.append(cmd)
        ns = self.netns.get(match.group("ns"))
        if ns:
            ns.routes = [r for r in ns.routes
                         if r != (match.group("dst"), match.group("gateway"))]
        return 0, "", ""

    _ROUTES = [
        (r"^ip netns list$", _netns_list),
        (r"^ip -d -json link show$", _link_show_all),
        (r"^ip -json -o link show master (?P<bridge>\S+)$", _link_show_master),
        (r"^ip -json addr show (?P<name>\S+)$", _addr_show),
        (r"^ip -json -n (?P<ns>\S+) link show$", _ns_link_show),
        (r"^ip netns pids (?P<ns>\S+)$", _ns_pids),
        (r"^ip -json -n (?P<ns>\S+) route show$", _ns_route_show),
        (r"^bridge -json fdb show dev (?P<dev>\S+)$", _fdb_show),
        (r"^ip link (?:delete|del) (?P<name>\S+)$", _link_delete),
        (r"^ip link set (?P<name>\S+) (?:up|down)$", _link_set),
        (r"^ip netns del (?P<ns>\S+)$", _netns_del),
        (r"^kill -9 (?P<pid>\d+)$", _kill),
        (r"^bridge fdb del \S+ dev (?P<dev>\S+) dst (?P<dst>\S+)$", _fdb_del),
        (r"^ip netns exec (?P<ns>\S+) ip route del (?P<dst>\S+) via (?P<gateway>\S+)$", _route_del),
    ]


# -- a stand-in for the database ---------------------------------------------------------

_SELECT_RE = re.compile(r"^SELECT JSON (?P<columns>.+?) FROM (?P<table>[\w.]+)"
                        r"(?P<where>.*?);$", re.S)
_INSERT_RE = re.compile(r"^INSERT INTO (?P<table>[\w.]+) \((?P<columns>[^)]*)\) VALUES "
                        r"\((?P<values>.+)\) IF NOT EXISTS;$", re.S)
_DELETE_RE = re.compile(r"^DELETE FROM (?P<table>[\w.]+) WHERE subnet_index = (?P<index>\d+) "
                        r"IF router_id = '(?P<router>[^']+)';$", re.S)


class FakeHydra:
    """Scylla through Daruk, in the shape urbosa's run_cql_query hands back.

    Two details decide these tests:

      * `SELECT JSON` arrives as one JSON object per line, because Daruk names
        that column `json` and run_cql_query returns it verbatim.
      * A lightweight transaction arrives as its row values joined by spaces,
        with `[applied]` first and no column names at all - so a refusal reads
        "False 41 0f0f-...". The return code is 0 either way: a rejected LWT
        executed perfectly well, which is exactly why an allocator that trusts
        the return code hands the same slot to two routers.
    """

    def __init__(self):
        self.pool = {}          # subnet_index -> {"router_id":..., "node_id":...}
        self.settings = {}
        self.failing = set()
        self.statements = []
        self.refuse_next_insert = False

    def query(self, cql, *_args, **_kwargs):
        cql = " ".join(cql.split())
        self.statements.append(cql)

        insert = _INSERT_RE.match(cql)
        if insert:
            return self._insert(insert)
        delete = _DELETE_RE.match(cql)
        if delete:
            return self._delete(delete)
        select = _SELECT_RE.match(cql)
        if select:
            return self._select(select)
        if cql.startswith("SELECT value FROM hydra.cluster_settings WHERE key = "):
            key = cql.split("'")[1]
            return 0, self.settings.get(key, ""), ""
        raise AssertionError(f"FakeHydra was handed a statement it cannot run: {cql}")

    # -- statements ------------------------------------------------------------
    def _select(self, match):
        table = match.group("table")
        if table in self.failing:
            return 1, "", f"NoHostAvailable: unable to complete the operation against {table}"
        rows = []
        if table == "hydra.urbosa_transit_pool":
            rows = [{"subnet_index": i, "router_id": r["router_id"]}
                    for i, r in sorted(self.pool.items())]
        elif table == "hydra.cluster_settings":
            wanted = re.findall(r"'([^']+)'", match.group("where"))
            rows = [{"key": k, "value": v} for k, v in sorted(self.settings.items())
                    if not wanted or k in wanted]
        elif table in self.tables:
            rows = self.tables[table]
        return 0, "\n".join(json.dumps(row) for row in rows), ""

    def _insert(self, match):
        if match.group("table") in self.failing:
            return 1, "", "NoHostAvailable"
        columns = [c.strip() for c in match.group("columns").split(",")]
        values = [v.strip().strip("'") for v in match.group("values").split(",")]
        row = dict(zip(columns, values))
        index = int(row["subnet_index"])
        if self.refuse_next_insert:
            # Another node won this slot between our read and our write.
            self.refuse_next_insert = False
            self.pool.setdefault(index, {"router_id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
                                         "node_id": "10.0.0.9"})
        if index in self.pool:
            # The refusal shape observed live: [applied] first, then the row that
            # beat us, flattened to space-joined values with no column names.
            existing = self.pool[index]
            return 0, (f"False {index} 1 {existing['node_id']} "
                       f"{existing['router_id']}"), ""
        self.pool[index] = {"router_id": row["router_id"],
                            "node_id": row.get("node_id", "")}
        return 0, "True", ""

    def _delete(self, match):
        index = int(match.group("index"))
        existing = self.pool.get(index)
        if existing is None or existing["router_id"] != match.group("router"):
            return 0, f"False {(existing or {}).get('router_id', 'null')}", ""
        del self.pool[index]
        # A successful conditional delete still returns the conditioned column
        # beside [applied] - "True aaaa" on the live cluster, not a bare "True".
        return 0, f"True {existing['router_id']}", ""

    tables = {}


# -- shared fixtures ---------------------------------------------------------------------

T0_ID = "aabbccdd-1111-2222-3333-444455556666"
T1_ID = "11223344-5555-6666-7777-888899990000"
T0_NS = "ns-t0-aabbccdd"
T1_NS = "ns-t1-11223344"


def t0_router(router_id=T0_ID):
    return {"router_id": router_id, "name": "edge", "uplink_interface": "ens192",
            "uplink_ip": "10.10.102.90/24", "gateway_ip": "10.10.102.1"}


def t1_router(router_id=T1_ID, t0_link_id=T0_ID, dhcp=False):
    return {"router_id": router_id, "name": "tenant", "t0_link_id": t0_link_id,
            "dhcp_enabled": dhcp}


def segment(vni, t1_link_id=T1_ID, cidr=None, dhcp=False):
    return {"segment_id": f"0000{vni}-0000-0000-0000-000000000000", "name": f"seg{vni}",
            "vni": vni, "t1_link_id": t1_link_id,
            "subnet_cidr": cidr or f"10.0.{vni % 250}.0/24",
            "gateway_ip": f"10.0.{vni % 250}.1", "dhcp_enabled": dhcp,
            "dhcp_start": f"10.0.{vni % 250}.10" if dhcp else None,
            "dhcp_end": f"10.0.{vni % 250}.100" if dhcp else None}


class UrbosaTestCase(unittest.TestCase):
    """Patches the module's two doors to the outside world: shell and database."""

    def setUp(self):
        self.host = FakeHost()
        self.db = FakeHydra()
        self.log = io.StringIO()

        self._saved = {name: getattr(urbosa, name)
                       for name in ("run_cmd", "run_cql_query", "run_conditional_cql_query",
                                    "read_proc_argv", "is_leader")}
        urbosa.run_cmd = self.host.run
        urbosa.run_cql_query = self.db.query
        # The transit pool's claim and release are lightweight transactions, and they read
        # the [applied] verdict themselves -- so they go through the unguarded path.
        # Stubbing only run_cql_query left the real one in place and reached Hydra.
        urbosa.run_conditional_cql_query = self.db.query
        urbosa.read_proc_argv = self.host.argv
        urbosa.is_leader = lambda: True

        urbosa.db_read_errors.clear()
        urbosa.transit_announced.clear()
        urbosa.reclaim_refusals_logged.clear()

        stack = contextlib.ExitStack()
        stack.enter_context(contextlib.redirect_stdout(self.log))
        self.addCleanup(stack.close)
        self.addCleanup(self._restore)

    def _restore(self):
        for name, value in self._saved.items():
            setattr(urbosa, name, value)

    def plan(self, t0_routers=(), t1_routers=(), segments=(), node_ips=None,
             leader=True, pool=None, t0_routes=None):
        desired = urbosa.build_desired_state(
            list(t0_routers), list(t1_routers), list(segments), node_ips, leader,
            transit_pool=pool, t0_routes=t0_routes)
        inventory = urbosa.collect_inventory(desired)
        return urbosa.plan_reclamation(desired, inventory)

    def targets(self, actions):
        return [a.target for a in actions]

    def refused(self, refusals):
        return [r.target for r in refusals]


# -- the transit pool --------------------------------------------------------------------

class TransitAllocationTests(UrbosaTestCase):

    def colliding_router_ids(self):
        """Two router ids the old derived scheme maps to the same /30.

        Found by search rather than hardcoded so the pair stays valid if the
        preferred-slot function is ever changed; the point of the test is that
        such a pair exists at all, which is the defect.
        """
        seen = {}
        for i in range(1, 5000):
            router_id = f"{i:08x}-0000-0000-0000-000000000000"
            index = urbosa.preferred_transit_index(router_id)
            if index in seen:
                return seen[index], router_id
            seen[index] = router_id
        self.fail("no hash collision found in 5000 candidate router ids")

    def test_the_derived_scheme_really_did_collide(self):
        # The defect itself, pinned: two different routers, one transit /30,
        # both ends of both links addressed identically, and no check anywhere.
        first, second = self.colliding_router_ids()
        self.assertNotEqual(first, second)
        self.assertEqual(urbosa.preferred_transit_index(first),
                         urbosa.preferred_transit_index(second))

    def test_two_routers_that_collide_are_given_different_subnets(self):
        first, second = self.colliding_router_ids()
        first_slot = urbosa.claim_transit_index(first, "10.0.0.1")
        second_slot = urbosa.claim_transit_index(second, "10.0.0.1")

        self.assertIsNotNone(first_slot)
        self.assertIsNotNone(second_slot)
        self.assertNotEqual(first_slot, second_slot)

        first_addrs = urbosa.transit_addresses(first_slot)
        second_addrs = urbosa.transit_addresses(second_slot)
        self.assertEqual(set(first_addrs) & set(second_addrs), set())

    def test_an_allocation_survives_a_restart(self):
        slot = urbosa.claim_transit_index(T1_ID, "10.0.0.1")
        self.assertIsNotNone(slot)

        # A restart keeps nothing but the database. Everything the process was
        # holding goes, and the allocation has to come back out of the pool.
        urbosa.transit_announced.clear()
        urbosa.db_read_errors.clear()
        before = len([s for s in self.db.statements if s.startswith("INSERT")])

        again = urbosa.claim_transit_index(T1_ID, "10.0.0.1")
        after = len([s for s in self.db.statements if s.startswith("INSERT")])

        self.assertEqual(slot, again)
        self.assertEqual(before, after, "a restart re-claimed a slot it already owned")

    def test_the_preferred_slot_keeps_a_cluster_on_its_existing_addressing(self):
        # An upgrade must not renumber transit links that are up and carrying
        # traffic, so a free preferred slot is taken as-is.
        expected = urbosa.preferred_transit_index(T1_ID)
        self.assertEqual(urbosa.claim_transit_index(T1_ID, "10.0.0.1"), expected)

    def test_a_lost_race_does_not_hand_out_an_owned_slot(self):
        # Two nodes claiming at once: the loser is told so by [applied]=False and
        # has to take a different slot. Trusting the return code here is how the
        # same /30 reaches two routers.
        self.db.refuse_next_insert = True
        slot = urbosa.claim_transit_index(T1_ID, "10.0.0.1")

        self.assertIsNotNone(slot)
        self.assertNotEqual(slot, urbosa.preferred_transit_index(T1_ID))
        self.assertEqual(self.db.pool[slot]["router_id"], T1_ID)

    def test_a_database_failure_yields_no_allocation_rather_than_a_derived_one(self):
        self.db.failing.add("hydra.urbosa_transit_pool")
        self.assertIsNone(urbosa.claim_transit_index(T1_ID, "10.0.0.1"))
        self.assertIn("Transit links are left exactly as they are", self.log.getvalue())

    def test_a_router_id_that_is_not_a_uuid_is_never_interpolated_into_cql(self):
        self.assertIsNone(urbosa.claim_transit_index("1; DROP TABLE hydra.nodes--", "10.0.0.1"))
        self.assertFalse([s for s in self.db.statements if "DROP TABLE" in s])

    def test_releasing_a_slot_reassigned_to_someone_else_does_nothing(self):
        slot = urbosa.claim_transit_index(T1_ID, "10.0.0.1")
        other = "99999999-0000-0000-0000-000000000000"
        self.db.pool[slot] = {"router_id": other, "node_id": "10.0.0.2"}

        self.assertFalse(urbosa.release_transit_index(slot, T1_ID))
        self.assertEqual(self.db.pool[slot]["router_id"], other)

    def test_releasing_a_slot_this_router_still_owns_frees_it(self):
        slot = urbosa.claim_transit_index(T1_ID, "10.0.0.1")
        self.assertTrue(urbosa.release_transit_index(slot, T1_ID))
        self.assertNotIn(slot, self.db.pool)

    def test_the_claim_statement_uses_types_daruk_can_hand_back(self):
        # Checked against the live cluster: with `router_id uuid` and
        # `allocated_at timestamp`, a REFUSED claim returns the winning row, and
        # Daruk's make_serializable passes driver UUID and datetime objects
        # through untouched - so json.dumps raises and the refusal arrives as
        # "Object of type UUID is not JSON serializable". The one response that
        # says somebody else won the slot was the one response that could not be
        # read. hydra.cluster_locks carries the same scar.
        urbosa.claim_transit_index(T1_ID, "10.0.0.1")
        insert = [s for s in self.db.statements if s.startswith("INSERT")][0]
        self.assertIn(f"'{T1_ID}'", insert)
        self.assertIn("allocated_at_ms", insert)
        self.assertNotIn("toTimestamp", insert)

    def test_the_lwt_result_shapes_seen_on_the_live_cluster_are_read_correctly(self):
        # Flattened by run_cql_query to space-joined values, [applied] first.
        self.assertIs(urbosa.lwt_was_applied("True"), True)
        self.assertIs(urbosa.lwt_was_applied("True aaaa"), True)
        self.assertIs(urbosa.lwt_was_applied("False 777 1 10.10.102.41 aaaa"), False)
        self.assertIs(urbosa.lwt_was_applied(""), False)
        # cqlsh's rendering, for the fallback path that bypasses Daruk.
        self.assertIs(urbosa.lwt_was_applied(
            " [applied] | router_id\n-----------+----------\n     False | aaaa\n"), False)
        self.assertIs(urbosa.lwt_was_applied(
            " [applied]\n-----------\n      True\n"), True)

    def test_slot_arithmetic_covers_the_transit_range_exactly(self):
        self.assertEqual(urbosa.transit_addresses(0),
                         ("100.64.0.1/30", "100.64.0.2/30", "100.64.0.0/30"))
        self.assertEqual(urbosa.transit_addresses(urbosa.TRANSIT_SLOTS - 1),
                         ("100.64.255.253/30", "100.64.255.254/30", "100.64.255.252/30"))
        with self.assertRaises(ValueError):
            urbosa.transit_addresses(urbosa.TRANSIT_SLOTS)


# -- identifying orphans -----------------------------------------------------------------

class OrphanIdentificationTests(UrbosaTestCase):

    def test_a_deleted_segment_takes_its_bridge_tunnel_and_veth_with_it(self):
        self.host.add_link("br-ov-10001", "bridge")
        self.host.add_link("vxlan-10001", "vxlan", master="br-ov-10001")
        self.host.add_link("veth-ov-10001", "veth", master="br-ov-10001", netnsid=0)
        self.host.add_netns(T1_NS, nsid=0, links=["lo", "veth-t1-10001"])

        actions, refusals = self.plan(t1_routers=[t1_router()], segments=[])

        self.assertEqual(refusals, [])
        self.assertEqual(sorted(self.targets(actions)),
                         ["br-ov-10001", "veth-ov-10001", "vxlan-10001"])
        for action in actions:
            self.assertIn("no segment with VNI 10001", action.reason)

    def test_a_segment_that_still_exists_is_left_completely_alone(self):
        self.host.add_link("br-ov-10001", "bridge")
        self.host.add_link("vxlan-10001", "vxlan", master="br-ov-10001")
        self.host.add_link("veth-ov-10001", "veth", master="br-ov-10001", netnsid=0)
        self.host.add_netns(T1_NS, nsid=0, links=["lo", "veth-t1-10001"])

        actions, refusals = self.plan(t1_routers=[t1_router()], segments=[segment(10001)])

        self.assertEqual(actions, [])
        self.assertEqual(refusals, [])

    def test_an_orphan_namespace_is_reclaimed_and_its_dnsmasq_stopped_first(self):
        self.host.add_netns(
            "ns-t1-deadbeef", links=["lo", "veth-t1-10001"],
            procs={"4242": ["/usr/sbin/dnsmasq", "--bind-interfaces",
                            "--interface=veth-t1-10001"]})

        actions, refusals = self.plan(t1_routers=[t1_router()], segments=[])

        self.assertEqual(refusals, [])
        self.assertEqual(self.targets(actions), ["ns-t1-deadbeef"])
        # `ip netns del` unlinks the name and stops nothing. A dnsmasq left
        # running keeps the namespace and its interfaces alive with no name left
        # to reach them by, which is a worse leak than the one being fixed.
        self.assertEqual(actions[0].commands,
                         ["kill -9 4242", "ip netns del ns-t1-deadbeef"])

    def test_a_namespace_belonging_to_a_live_router_is_not_touched(self):
        self.host.add_netns(T1_NS, links=["lo", "t1-11223344"])
        actions, refusals = self.plan(t1_routers=[t1_router()], segments=[])
        self.assertEqual(actions, [])
        self.assertEqual(refusals, [])

    def test_namespaces_urbosa_did_not_name_are_never_candidates(self):
        # Podman, CNI and an operator's scratch namespace all live in the same
        # list. Matching a prefix rather than the whole name would put them in
        # scope; the eight-hex-character suffix is what makes the name ours.
        for name in ("netns-9f2c", "cni-1234abcd", "ns-t1-notahex", "ns-t2-aabbccdd",
                     "ns-t1-aabbccddee"):
            self.host.add_netns(name, links=["lo"])
        actions, refusals = self.plan(t1_routers=[], segments=[])
        self.assertEqual(actions, [])
        self.assertEqual(refusals, [])

    def test_a_tier0_namespace_on_a_node_without_the_vip_is_reclaimed(self):
        # Active-passive: two hosts holding the same uplink IP on the physical
        # network is the failure this prevents.
        self.host.add_netns(T0_NS, links=["lo", "mv-t0-aabbccdd"])
        self.host.add_link("mv-t0-aabbccdd", "macvlan")

        actions, _refusals = self.plan(t0_routers=[t0_router()], leader=False)

        self.assertIn(T0_NS, self.targets(actions))
        self.assertIn("mv-t0-aabbccdd", self.targets(actions))

    def test_a_half_created_transit_veth_in_the_root_namespace_is_swept_up(self):
        # `ip link add` succeeded, `ip link set netns` did not. Both ends are
        # moved in the same breath they are created, so a root-namespace half is
        # always wreckage.
        self.host.add_link("t1-11223344", "veth")
        actions, _refusals = self.plan(t1_routers=[t1_router()], segments=[])
        self.assertEqual(self.targets(actions), ["t1-11223344"])

    def test_both_ends_of_an_abandoned_transit_pair_are_removed_by_one_deletion(self):
        # Observed on the live node: proposing both ends produced one deletion
        # and one "Cannot find device", because deleting either end takes its
        # peer with it. A log full of false alarms is a log nobody reads.
        self.host.add_link("t0-11223344", "veth")
        self.host.add_link("t1-11223344", "veth")

        actions, _refusals = self.plan(t1_routers=[], segments=[])

        self.assertEqual(len(actions), 1)
        urbosa.execute_plan(actions, [], dry_run=False, run=self.host.run)
        self.assertNotIn("failed", self.log.getvalue())

    def test_a_segment_moved_to_another_router_has_its_veth_rebuilt(self):
        # The veth is only ever created when the host side is missing, so a
        # re-attached segment stayed wired to the old router forever.
        self.host.add_link("br-ov-10001", "bridge")
        self.host.add_link("veth-ov-10001", "veth", master="br-ov-10001", netnsid=7)
        self.host.add_netns("ns-t1-99999999", nsid=7, links=["lo", "veth-t1-10001"])
        self.host.add_netns(T1_NS, nsid=0, links=["lo"])

        actions, _refusals = self.plan(
            t1_routers=[t1_router(), t1_router("99999999-0000-0000-0000-000000000000")],
            segments=[segment(10001)])

        self.assertEqual(self.targets(actions), ["veth-ov-10001"])
        self.assertIn("now attached to ns-t1-11223344", actions[0].reason)


# -- refusing to touch anything live -----------------------------------------------------

class LiveResourceTests(UrbosaTestCase):

    def test_a_bridge_with_a_guest_tap_is_refused(self):
        # The whole point. vnet0 is a running VM's NIC; deleting the bridge
        # unplugs it, and no amount of leaked kernel objects is worse than that.
        self.host.add_link("br-ov-10001", "bridge")
        self.host.add_link("vxlan-10001", "vxlan", master="br-ov-10001")
        self.host.add_link("vnet0", "tun", master="br-ov-10001")

        actions, refusals = self.plan(segments=[])

        self.assertEqual(actions, [])
        self.assertEqual(self.refused(refusals), ["br-ov-10001", "vxlan-10001"])
        self.assertIn("vnet0", refusals[0].reason)

    def test_a_bridge_holding_a_host_address_is_refused(self):
        # Lanayru puts 172.16.10.250/24 on br-ov-10010 directly and removes only
        # the database row on teardown. Urbosa never addresses a host bridge, so
        # an address here belongs to something Urbosa cannot reason about.
        self.host.add_link("br-ov-10010", "bridge", addrs=["172.16.10.250"])

        actions, refusals = self.plan(segments=[])

        self.assertEqual(actions, [])
        self.assertIn("172.16.10.250", refusals[0].reason)

    def test_the_kernels_link_local_address_is_not_read_as_a_configured_one(self):
        # Found on the live node: the first version of the guard above refused
        # every bridge on the host, because an up bridge always has an fe80::
        # address. A guard that never lets anything through is not a guard.
        self.host.add_link("br-ov-10001", "bridge")
        actions, refusals = self.plan(segments=[])
        self.assertEqual(self.targets(actions), ["br-ov-10001"])
        self.assertEqual(refusals, [])

    def test_a_bridge_whose_ports_cannot_be_listed_is_refused(self):
        self.host.add_link("br-ov-10001", "bridge")
        self.host.unreadable.add("br-ov-10001")

        actions, refusals = self.plan(segments=[])

        self.assertEqual(actions, [])
        self.assertIn("could not be listed", refusals[0].reason)

    def test_a_namespace_holding_an_interface_urbosa_did_not_create_is_refused(self):
        self.host.add_netns("ns-t1-deadbeef", links=["lo", "vnet3"])
        actions, refusals = self.plan(t1_routers=[], segments=[])
        self.assertEqual(actions, [])
        self.assertIn("vnet3", refusals[0].reason)

    def test_a_namespace_running_a_process_urbosa_did_not_start_is_refused(self):
        self.host.add_netns("ns-t1-deadbeef", links=["lo"],
                            procs={"991": ["/usr/sbin/sshd", "-D"]})
        actions, refusals = self.plan(t1_routers=[], segments=[])
        self.assertEqual(actions, [])
        self.assertIn("991", refusals[0].reason)

    def test_a_namespace_holding_a_live_segments_gateway_is_refused(self):
        # The router row is gone but a segment attached to it is not. Deleting
        # the namespace strips the default gateway from a segment that is still
        # configured and still carrying traffic; that is the operator's call.
        self.host.add_netns("ns-t1-deadbeef", links=["lo", "veth-t1-10001"])
        self.host.add_link("br-ov-10001", "bridge")
        self.host.add_link("veth-ov-10001", "veth", master="br-ov-10001", netnsid=0)

        actions, refusals = self.plan(t1_routers=[], segments=[segment(10001)])

        self.assertEqual(actions, [])
        self.assertIn("VNI 10001 is still configured", refusals[0].reason)

    def test_a_namespace_whose_contents_cannot_be_read_is_refused(self):
        self.host.add_netns("ns-t1-deadbeef", links=["lo"])
        self.host.unreadable.add("ns-t1-deadbeef")
        actions, refusals = self.plan(t1_routers=[], segments=[])
        self.assertEqual(actions, [])
        self.assertIn("could not be read", refusals[0].reason)

    def test_a_device_wearing_our_name_but_not_our_type_is_refused(self):
        self.host.add_link("br-ov-10001", "vlan")
        actions, refusals = self.plan(segments=[])
        self.assertEqual(actions, [])
        self.assertIn("not a bridge", refusals[0].reason)

    def test_a_refusal_is_explained_once_and_not_on_every_pass(self):
        self.host.add_link("br-ov-10001", "bridge")
        self.host.add_link("vnet0", "tun", master="br-ov-10001")
        actions, refusals = self.plan(segments=[])

        urbosa.execute_plan(actions, refusals, dry_run=False, run=self.host.run)
        urbosa.execute_plan(actions, refusals, dry_run=False, run=self.host.run)

        self.assertEqual(self.log.getvalue().count("REFUSING to remove"), 1)


# -- the dry run -------------------------------------------------------------------------

class DryRunTests(UrbosaTestCase):

    def orphaned_host(self):
        self.host.add_link("br-ov-10001", "bridge")
        self.host.add_link("vxlan-10001", "vxlan", master="br-ov-10001")
        self.host.add_netns("ns-t1-deadbeef", links=["lo"], procs={"77": ["/usr/sbin/dnsmasq"]})

    def test_the_dry_run_removes_nothing(self):
        self.orphaned_host()
        actions, refusals = self.plan(t1_routers=[], segments=[])
        self.assertTrue(actions, "the fixture should have produced something to remove")

        performed = urbosa.execute_plan(actions, refusals, dry_run=True, run=self.host.run)

        self.assertEqual(performed, [])
        self.assertEqual(self.host.executed, [])
        self.assertIn("br-ov-10001", self.host.links)
        self.assertIn("ns-t1-deadbeef", self.host.netns)

    def test_the_dry_run_prints_the_commands_it_would_have_run(self):
        self.orphaned_host()
        actions, refusals = self.plan(t1_routers=[], segments=[])
        urbosa.execute_plan(actions, refusals, dry_run=True, run=self.host.run)

        output = self.log.getvalue()
        self.assertIn("would remove", output)
        self.assertIn("ip link delete br-ov-10001", output)
        self.assertIn("ip netns del ns-t1-deadbeef", output)

    def test_applying_the_same_plan_does_remove_it(self):
        self.orphaned_host()
        actions, refusals = self.plan(t1_routers=[], segments=[])

        performed = urbosa.execute_plan(actions, refusals, dry_run=False, run=self.host.run)

        self.assertEqual(len(performed), len(actions))
        self.assertNotIn("br-ov-10001", self.host.links)
        self.assertNotIn("vxlan-10001", self.host.links)
        self.assertNotIn("ns-t1-deadbeef", self.host.netns)

    def test_a_failed_command_does_not_stop_the_rest_of_the_plan(self):
        # The host moves under a plan: something else removed the bridge between
        # the observation and the deletion. That action fails; the namespace it
        # has nothing to do with must still be reclaimed.
        self.orphaned_host()
        actions, refusals = self.plan(t1_routers=[], segments=[])
        self.assertIn("br-ov-10001", self.targets(actions))
        del self.host.links["br-ov-10001"]

        performed = urbosa.execute_plan(actions, refusals, dry_run=False, run=self.host.run)

        self.assertNotIn("br-ov-10001", self.targets(performed))
        self.assertIn("ns-t1-deadbeef", self.targets(performed))
        self.assertNotIn("ns-t1-deadbeef", self.host.netns)
        self.assertIn("failed", self.log.getvalue())


# -- flood entries and return routes ------------------------------------------------------

class MeshReclamationTests(UrbosaTestCase):

    def vxlan_with_flood(self, *peers):
        self.host.add_link("br-ov-10001", "bridge")
        self.host.add_link("vxlan-10001", "vxlan", master="br-ov-10001")
        self.host.links["vxlan-10001"]["flood"] = list(peers)

    def test_a_decommissioned_peer_stops_receiving_every_broadcast(self):
        self.vxlan_with_flood("10.0.0.2", "10.0.0.9")
        actions, _refusals = self.plan(t1_routers=[t1_router()], segments=[segment(10001)],
                                       node_ips=["10.0.0.1", "10.0.0.2"])

        self.assertEqual(self.targets(actions), ["vxlan-10001 -> 10.0.0.9"])
        self.assertIn("not a member of hydra.nodes", actions[0].reason)

    def test_a_live_peer_keeps_its_flood_entry(self):
        self.vxlan_with_flood("10.0.0.2")
        actions, _refusals = self.plan(t1_routers=[t1_router()], segments=[segment(10001)],
                                       node_ips=["10.0.0.1", "10.0.0.2"])
        self.assertEqual(actions, [])

    def test_flood_entries_are_untouched_when_the_node_list_could_not_be_read(self):
        # An unreadable hydra.nodes arriving as "no peers" would prune the entire
        # mesh and silently isolate every segment on the host.
        self.vxlan_with_flood("10.0.0.2", "10.0.0.9")
        actions, _refusals = self.plan(t1_routers=[t1_router()], segments=[segment(10001)],
                                       node_ips=None)
        self.assertEqual(actions, [])

    def test_learned_unicast_entries_are_never_touched(self):
        # Deleting one blackholes a live VM until the fabric relearns it. Only
        # the all-zero-MAC head-end entries are ours.
        self.vxlan_with_flood("10.0.0.2")
        self.host.links["vxlan-10001"]["learned"] = [("52:54:00:aa:bb:cc", "10.0.0.9")]
        actions, _refusals = self.plan(t1_routers=[t1_router()], segments=[segment(10001)],
                                       node_ips=["10.0.0.1", "10.0.0.2"])
        self.assertEqual(actions, [])

    def test_a_return_route_to_a_deleted_segment_is_withdrawn(self):
        self.host.add_netns(T0_NS, links=["lo", "mv-t0-aabbccdd", "t0-11223344"], routes=[
            ("default", "10.10.102.1"),
            ("10.0.1.0/24", "100.64.0.2"),
            ("10.0.2.0/24", "100.64.0.2"),
        ])
        pool = {0: T1_ID}
        routes = urbosa.plan_t0_routes([t0_router()], [t1_router()],
                                       [segment(10001, cidr="10.0.1.0/24")], pool)

        actions, _refusals = self.plan(t0_routers=[t0_router()], t1_routers=[t1_router()],
                                       segments=[segment(10001, cidr="10.0.1.0/24")],
                                       pool=pool, t0_routes=routes)

        self.assertEqual(self.targets(actions), [f"{T0_NS}: 10.0.2.0/24 via 100.64.0.2"])

    def test_the_uplink_default_route_is_never_withdrawn(self):
        self.host.add_netns(T0_NS, links=["lo", "mv-t0-aabbccdd"], routes=[
            ("default", "10.10.102.1"),
        ])
        routes = urbosa.plan_t0_routes([t0_router()], [], [], {})
        actions, _refusals = self.plan(t0_routers=[t0_router()], t1_routers=[],
                                       segments=[], pool={}, t0_routes=routes)
        self.assertEqual(actions, [])

    def test_return_routes_are_not_pruned_when_a_transit_slot_is_unknown(self):
        # Without every transit address, "which routes should exist" cannot be
        # answered, and pruning against a partial answer deletes live paths.
        self.host.add_netns(T0_NS, links=["lo", "t0-11223344"], routes=[
            ("10.0.1.0/24", "100.64.0.2"),
        ])
        routes = urbosa.plan_t0_routes([t0_router()], [t1_router()], [segment(10001)], {})
        self.assertIsNone(routes[T0_NS])

        actions, _refusals = self.plan(t0_routers=[t0_router()], t1_routers=[t1_router()],
                                       segments=[segment(10001)], pool={}, t0_routes=routes)
        self.assertEqual(actions, [])


# -- reservations and the end-to-end pass -------------------------------------------------

class ReclaimPassTests(UrbosaTestCase):

    def test_a_reservation_for_a_deleted_router_is_released(self):
        self.db.pool[5] = {"router_id": T1_ID, "node_id": "10.0.0.1"}
        actions, _refusals = self.plan(t1_routers=[], segments=[], pool={5: T1_ID}, leader=True)

        self.assertEqual(self.targets(actions), ["slot 5 (100.64.0.20/30)"])
        urbosa.execute_plan(actions, [], dry_run=False, run=self.host.run)
        self.assertNotIn(5, self.db.pool)

    def test_a_reservation_for_a_live_router_is_kept(self):
        actions, _refusals = self.plan(t1_routers=[t1_router()], segments=[],
                                       pool={5: T1_ID}, leader=True)
        self.assertEqual(actions, [])

    def test_only_the_vip_holder_releases_reservations(self):
        # Every node runs the reclaimer. The pool is cluster-wide state, and one
        # writer for it is enough.
        actions, _refusals = self.plan(t1_routers=[], segments=[], pool={5: T1_ID},
                                       leader=False)
        self.assertEqual(actions, [])

    def test_nothing_is_reclaimed_when_the_desired_state_cannot_be_read(self):
        # The failure mode that makes a collector dangerous: a database blip
        # reading as "the operator deleted everything".
        self.host.add_link("br-ov-10001", "bridge")
        self.host.add_netns(T1_NS, links=["lo"])
        self.db.failing.add("hydra.urbosa_segments")

        actions, refusals = urbosa.reclaim_once(dry_run=False)

        self.assertEqual(actions, [])
        self.assertEqual(refusals, [])
        self.assertEqual(self.host.executed, [])
        self.assertIn("could not be read in full", self.log.getvalue())

    def test_a_full_pass_reclaims_a_deleted_segment_end_to_end(self):
        FakeHydra.tables = {
            "hydra.urbosa_t0_routers": [],
            "hydra.urbosa_t1_routers": [t1_router()],
            "hydra.urbosa_segments": [],
            "hydra.nodes": [{"ip": "10.0.0.1"}],
        }
        self.addCleanup(lambda: setattr(FakeHydra, "tables", {}))

        self.host.add_link("br-ov-10001", "bridge")
        self.host.add_link("vxlan-10001", "vxlan", master="br-ov-10001")
        self.host.add_netns(T1_NS, links=["lo"])

        actions, refusals = urbosa.reclaim_once(dry_run=False)

        self.assertEqual(refusals, [])
        self.assertEqual(sorted(self.targets(actions)), ["br-ov-10001", "vxlan-10001"])
        self.assertNotIn("br-ov-10001", self.host.links)
        self.assertIn(T1_NS, self.host.netns, "the live router's namespace was removed")

    def test_the_reclaim_settings_default_to_enabled_and_wet(self):
        self.assertEqual(urbosa.get_reclaim_settings(), (True, False))

    def test_the_reclaim_settings_can_be_switched_off(self):
        self.db.settings["urbosa_gc_enabled"] = "false"
        self.db.settings["urbosa_gc_dry_run"] = "true"
        self.assertEqual(urbosa.get_reclaim_settings(), (False, True))


# -- DHCP servers ------------------------------------------------------------------------

class DhcpReclamationTests(UrbosaTestCase):

    def test_a_dhcp_server_for_a_deleted_segment_is_stopped(self):
        self.host.add_netns(T1_NS, links=["lo", "veth-t1-10002"], procs={
            "500": ["/usr/sbin/dnsmasq", "--bind-interfaces", "--interface=veth-t1-10001"],
            "501": ["/usr/sbin/dnsmasq", "--bind-interfaces", "--interface=veth-t1-10002"],
        })

        actions, _refusals = self.plan(
            t1_routers=[t1_router()], segments=[segment(10002, dhcp=True)])

        self.assertEqual(self.targets(actions), [f"{T1_NS}: pid 500 on veth-t1-10001"])

    def test_a_dhcp_server_a_segment_still_wants_is_left_running(self):
        self.host.add_netns(T1_NS, links=["lo", "veth-t1-10001"], procs={
            "500": ["/usr/sbin/dnsmasq", "--interface=veth-t1-10001"]})
        actions, _refusals = self.plan(
            t1_routers=[t1_router()], segments=[segment(10001, dhcp=True)])
        self.assertEqual(actions, [])

    def test_the_router_level_dhcp_server_stops_when_dhcp_is_turned_off(self):
        self.host.add_netns(T1_NS, links=["lo"], procs={
            "500": ["/usr/sbin/dnsmasq", "--interface=lo"]})

        running, _refusals = self.plan(t1_routers=[t1_router(dhcp=True)], segments=[])
        self.assertEqual(running, [])

        stopped, _refusals = self.plan(t1_routers=[t1_router(dhcp=False)], segments=[])
        self.assertEqual(self.targets(stopped), [f"{T1_NS}: pid 500 on lo"])

    def test_an_interface_name_is_matched_whole(self):
        # --interface=veth-t1-10 must not be read as --interface=veth-t1-100.
        self.assertEqual(urbosa.dnsmasq_interface(["dnsmasq", "--interface=veth-t1-10"]),
                         "veth-t1-10")
        self.assertIsNone(urbosa.dnsmasq_interface(["dnsmasq", "--bind-interfaces"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
