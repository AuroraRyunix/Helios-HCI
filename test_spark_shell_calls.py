#!/usr/bin/env python3
"""No caller builds a shell string to control a systemd unit or probe the network.

`/api/v1/execute` runs whatever it is handed through a shell, as root, on a hypervisor.
The way out of that is one typed endpoint per family of thing callers were asking for,
and the way to know a family is *finished* is not to count the call sites that moved --
it is to assert that none are left.

So this reads the tree rather than a running cluster. Two families are covered:

  * systemd unit control, behind `POST/GET /api/v1/host/units`;
  * network probing, behind `GET /api/v1/host/listeners`, `GET /api/v1/host/interfaces`
    and the `addresses` array of the `/api/v1/host/network` that already existed.

The scan is over string *literals* in the caller files, taken from the AST rather than
by reading lines, for two reasons. Comments in this repository quote the commands they
replaced -- a line-based scan would flag every explanation of why a shell string is gone
as though it were still there. And an f-string is not a `Constant`, so
`f"systemctl restart {svc}"` is invisible to anything that only looks at plain string
nodes; it is exactly the spelling that hides a call site.

Two more assertions sit alongside the scan, because the allow-list is the thing that
makes the endpoint safe and an allow-list nobody checks drifts:

  * every unit the deployment toolkit installs is one spark-daemon will act on, and
  * every list of service names in a caller names only units that exist.

The second is what would have caught `aether` and `spark` sitting in hylia's restart
map. `aether.service` was deleted with DRBD and Spark's unit has always been
`spark-daemon`, so both restarts had been failing as unknown units, silently, for as
long as they had been written that way.

Run with:  python -m unittest test_spark_shell_calls
"""

import ast
import importlib.util
import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

# Every file that holds a client for spark-daemon. A name here that is not a file is a
# typo, and a typo silently switches a caller off -- see test_internal_api_auth, where
# exactly that left one caller dialling plain HTTP after the port was hardened.
CALLERS = [
    "spectrum_server.py", "cluster_new.py", "vali.py", "hylia.py", "mipha.py",
    "dagur.py", "valcli.py", "mimir.py", "catalyst.py", "lanayru.py",
    "urbosa_bootstrap.py", "rauru.py",
]

DAEMON = "spark_daemon_decoded.py"
PROVISION = "provision.py"

# systemctl followed by something: the bare word `"systemctl"` is the head of an argv
# list, which is the shape this is trying to reach, not the one it is looking for.
UNIT_COMMAND_RE = re.compile(r"systemctl\s+\S")

# `ss`/`netstat` with a flag, `ip` with a subcommand, and the sysfs directory the console
# used to enumerate interfaces out of with `find`.
NETWORK_COMMAND_RE = re.compile(
    r"(?:\bss|\bnetstat)\s+-"
    r"|\bip\s+(?:-\S+\s+)*(?:route|addr|link|neigh)\b"
    r"|/sys/class/net")

FAMILIES = (
    ("systemd unit control", UNIT_COMMAND_RE),
    ("network probing", NETWORK_COMMAND_RE),
)

# Commands in these two families that are deliberately still written as shell text. Each
# needs a reason: an unexplained exemption is how the next one gets waved through.
#
# The key is the command, not the whole literal that carries it, because one of them is a
# single line inside a several-hundred-line script. A literal is only excused once every
# match in it has been accounted for by an entry here, so a second command smuggled into
# the same string is still reported.
KNOWN_SHELL_COMMANDS = {
    ("cluster_new.py", "systemctl stop sidon || true"):
        "A line inside the base64-encoded wipe script `cluster destroy` runs on each "
        "node. The script is one /execute call site belonging to the filesystem family, "
        "which is a separate piece of work; the stop rides along inside it and moves "
        "when that call site does.",
    ("cluster_new.py", "systemctl start zookeeper hydra-db"):
        "Printed advice. It is the instruction an operator is given when a cluster "
        "cannot be recovered automatically, not a command this file runs.",
    ("cluster_new.py", "systemctl is-active sidon 2>&1"):
        "Part of `describe_sidon_failure`, a read-only probe that runs only after sidon has "
        "failed to answer and prints the node's own account (unit state, restart count, "
        "journal tail, mounts) instead of a guess. It is one multi-command evidence string, "
        "not unit control, so the units endpoint does not fit it.",
    ("cluster_new.py", "systemctl show -p NRestarts --value sidon 2>&1"):
        "The restart count in the same evidence probe; see the entry above.",
    ("hylia.py", "nohup sh -c 'sleep 2 && systemctl restart hylia' > /dev/null 2>&1 &"):
        "Hylia restarting itself, locally, with a constant command -- not a call into "
        "spark-daemon at all. The nohup and the ampersand are what let the replacement "
        "process outlive the one being killed, which a thread inside this process "
        "cannot do.",
    ("mipha.py", "systemctl stop libvirtd virtqemud || true; "):
        "`legacy_spark_fence`, which is reached only when the far side answered 404 to "
        "the typed fence -- a daemon that predates the typed endpoints predates "
        "/api/v1/host/units too. Migrating the compatibility path to the endpoint it "
        "exists to be compatible without would fail silently, because a 404 on the unit "
        "call reads exactly like a fence that was attempted.",
    ("lanayru.py", "systemctl restart systemd-networkd"):
        "A line of cloud-init user data that runs inside the guest being created, not on a "
        "hypervisor and not through spark-daemon.",
    ("lanayru.py", "ip link add name br-ov-10010 type bridge || true"):
        "Host gateway bridges for the default overlay segments. There is no typed endpoint for "
        "creating a link or an address (the host/network endpoint is read-only), and one is a "
        "design of its own: which links may be created, with which names. Fixed text, no "
        "caller-supplied value. Listed in docs/service_review.md.",
    ("lanayru.py", "ip addr add 172.16.10.250/24 dev br-ov-10010 || true"): "See the entry above.",
    ("lanayru.py", "ip link set br-ov-10010 up || true"): "See the entry above.",
    ("lanayru.py", "ip link add name br-ov-10011 type bridge || true"): "See the entry above.",
    ("lanayru.py", "ip addr add 172.16.11.250/24 dev br-ov-10011 || true"): "See the entry above.",
    ("lanayru.py", "ip link set br-ov-10011 up || true"): "See the entry above.",
    ("urbosa_bootstrap.py", "ip netns show"):
        "Urbosa's teardown walks the namespaces and links it created and deletes them. No typed "
        "endpoint exists for namespace or link management; the only interpolated part is the "
        "firewall-rule text built from Hydra rows, which is in a different family. The unit "
        "control that used to share this string now goes through the units endpoint.",
    ("urbosa_bootstrap.py", "ip netns pids"): "See the entry above.",
    ("urbosa_bootstrap.py", "ip netns del"): "See the entry above.",
    ("urbosa_bootstrap.py", "ip -o link show"): "See the entry above.",
    ("urbosa_bootstrap.py", "ip link del"): "See the entry above.",
}


def unexplained(name, text, pattern):
    """True when `text` matches the family pattern somewhere no exemption accounts for."""
    remaining = text
    for exempt_name, command in KNOWN_SHELL_COMMANDS:
        if exempt_name == name:
            remaining = remaining.replace(command, " ")
    return pattern.search(remaining) is not None


def read(name):
    path = os.path.join(HERE, name)
    if not os.path.exists(path):
        return None
    with io.open(path, encoding="utf-8", errors="ignore") as handle:
        return handle.read()


def load_daemon():
    """Import spark_daemon_decoded.py by path.

    It is not a package and its filename is not an identifier, so a plain import will not
    reach it. Everything at module level is definitions; the server only starts under
    `if __name__ == "__main__"`.
    """
    spec = importlib.util.spec_from_file_location(
        "spark_daemon_under_test", os.path.join(HERE, DAEMON))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


daemon = load_daemon()


def literal_strings(source):
    """Every string literal in the source that is not a docstring, as (line, text).

    An f-string is returned with `{}` where its expressions were, so
    `f"systemctl restart {svc}"` reads as `systemctl restart {}` -- the command is still
    visible, and the interpolation is the part that made it worth finding.
    """
    tree = ast.parse(source)

    skip = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)):
            body = getattr(node, "body", None)
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                skip.add(id(body[0].value))
        if isinstance(node, ast.JoinedStr):
            # The pieces are reported as part of the whole f-string, not on their own.
            for part in ast.walk(node):
                if part is not node:
                    skip.add(id(part))

    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr) and id(node) not in skip:
            found.append((node.lineno, "".join(
                part.value if isinstance(part, ast.Constant) and isinstance(part.value, str)
                else "{}"
                for part in node.values)))
        elif (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and id(node) not in skip):
            found.append((node.lineno, node.value))
    return found


def unit_collections(source, units):
    """Literal collections of strings that are lists of service names.

    A collection counts as one when at least three of its members are units this cluster
    manages -- enough that it is a service list and not, say, a list of table columns
    that happens to contain the word `slate`. Dict values count as well as list
    elements, because the map that carried the two dead names was a dict.
    """
    tree = ast.parse(source)
    collections = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            members = node.elts
        elif isinstance(node, ast.Dict):
            members = node.values
        else:
            continue
        values = [member.value for member in members
                  if isinstance(member, ast.Constant) and isinstance(member.value, str)]
        if not values or len(values) != len(members):
            continue
        if len([value for value in values if value in units]) >= 3:
            collections.append((node.lineno, values))
    return collections


class CallerScanTests(unittest.TestCase):
    def test_every_caller_named_here_is_a_file(self):
        absent = [name for name in CALLERS if read(name) is None]
        self.assertEqual(
            absent, [],
            "CALLERS names files that are not in the repository, so they are not being "
            "checked at all: %s" % absent)

    def test_no_caller_builds_a_shell_string_for_these_families(self):
        offenders = []
        for name in CALLERS:
            for line, text in literal_strings(read(name)):
                for family, pattern in FAMILIES:
                    if unexplained(name, text, pattern):
                        offenders.append(
                            "%s:%d (%s): %s" % (name, line, family,
                                                " ".join(text.split())[:100]))
        self.assertEqual(
            offenders, [],
            "these build a shell string for a family that has a typed endpoint; use it "
            "instead of /api/v1/execute:\n  " + "\n  ".join(offenders))

    def test_every_exemption_is_still_present_and_still_needed(self):
        """An exemption for a command nobody writes any more is a stale note that hides
        the next one. It goes with the call site, not after it."""
        unused = []
        for (name, command), reason in KNOWN_SHELL_COMMANDS.items():
            source = read(name)
            if source is None or command not in source:
                unused.append("%s: %s" % (name, command[:70]))
            self.assertTrue(reason.strip(), "%s has an exemption with no reason" % name)
        self.assertEqual(
            unused, [],
            "these exemptions name commands that are no longer in the tree: %s" % unused)


class UnitAllowListTests(unittest.TestCase):
    """The allow-list is what makes /api/v1/host/units safe, so it is checked against
    the thing that decides what units exist: the deployment toolkit."""

    def test_every_unit_the_toolkit_installs_is_one_the_daemon_will_act_on(self):
        source = read(PROVISION)
        self.assertIsNotNone(source, PROVISION + " is missing")
        installed = set(re.findall(r"([A-Za-z][A-Za-z0-9_-]{0,40})\.(?:service|container)",
                                   source))
        missing = sorted(installed - daemon.MANAGED_UNITS)
        self.assertEqual(
            missing, [],
            "provision.py installs these units and spark-daemon refuses to act on them, "
            "so nothing can start or stop them through the typed API: %s" % missing)

    def test_the_only_extra_units_are_the_host_ones_we_drive_but_do_not_install(self):
        source = read(PROVISION)
        installed = set(re.findall(r"([A-Za-z][A-Za-z0-9_-]{0,40})\.(?:service|container)",
                                   source))
        extra = sorted(daemon.MANAGED_UNITS - installed)
        self.assertEqual(
            extra, ["chronyd", "libvirtd", "virtqemud"],
            "the allow-list has grown a unit the toolkit does not install and that is "
            "not one of the three host units the stack drives (chrony for the time "
            "source the console configures, libvirt's two for fencing): %s" % extra)

    def test_no_caller_keeps_a_list_of_services_that_do_not_exist(self):
        offenders = []
        for name in CALLERS:
            for line, values in unit_collections(read(name), daemon.MANAGED_UNITS):
                unknown = sorted(set(value for value in values
                                     if value not in daemon.MANAGED_UNITS))
                if unknown:
                    offenders.append("%s:%d names %s" % (name, line, unknown))
        self.assertEqual(
            offenders, [],
            "these service lists name units this cluster does not have, so those "
            "entries do nothing and say nothing when they do it:\n  "
            + "\n  ".join(offenders))


class UnitValidationTests(unittest.TestCase):
    """Reject rather than sanitize. A refused name is a bug report."""

    def test_a_unit_outside_the_allow_list_is_refused(self):
        for candidate in ("sshd", "firewalld", "systemd-journald", "aether", "spark"):
            unit, error = daemon.validate_unit(candidate)
            self.assertIsNone(unit, candidate)
            self.assertIn(candidate, error)

    def test_a_managed_unit_passes_unchanged(self):
        self.assertEqual(daemon.validate_unit("zookeeper"), ("zookeeper", None))

    def test_the_suffixed_spelling_is_refused_rather_than_normalised(self):
        """Two spellings for one unit means the allow-list is checked in two forms, and
        the second form is the one an edit forgets."""
        unit, error = daemon.validate_unit("zookeeper.service")
        self.assertIsNone(unit)
        self.assertIsNotNone(error)

    def test_shell_metacharacters_do_not_survive_the_allow_list(self):
        for candidate in ("zookeeper; reboot", "zookeeper && rm -rf /", "$(reboot)",
                          "zookeeper\nreboot", "-h"):
            unit, error = daemon.validate_unit(candidate)
            self.assertIsNone(unit, candidate)
            self.assertIsNotNone(error, candidate)

    def test_non_strings_are_refused(self):
        for candidate in (None, 1, True, ["zookeeper"], {"unit": "zookeeper"}):
            unit, error = daemon.validate_unit(candidate)
            self.assertIsNone(unit, candidate)
            self.assertIsNotNone(error, candidate)

    def test_one_unknown_name_refuses_the_whole_list(self):
        """Filtering would turn "restart these five services" into "restart four of
        them" and say nothing about the fifth."""
        units, error = daemon.validate_units(["zookeeper", "aether", "vali"])
        self.assertIsNone(units)
        self.assertIn("aether", error)

    def test_a_good_list_keeps_the_caller_order(self):
        units, error = daemon.validate_units(["vali", "zookeeper", "sidon"])
        self.assertIsNone(error)
        self.assertEqual(units, ["vali", "zookeeper", "sidon"])

    def test_a_bare_string_is_not_a_list_of_one(self):
        units, error = daemon.validate_units("zookeeper")
        self.assertIsNone(units)
        self.assertIn("not a string", error)

    def test_an_empty_list_is_refused(self):
        units, error = daemon.validate_units([])
        self.assertIsNone(units)
        self.assertIsNotNone(error)

    def test_the_list_is_bounded(self):
        units, error = daemon.validate_units(
            ["zookeeper"] * (daemon.MAX_UNITS_PER_REQUEST + 1))
        self.assertIsNone(units)
        self.assertIn("at most", error)

    def test_only_the_listed_verbs_are_actions(self):
        for candidate in ("mask", "isolate", "kill", "poweroff", "--version", "", None):
            action, error = daemon.validate_unit_action(candidate)
            self.assertIsNone(action, candidate)
            self.assertIsNotNone(error, candidate)
        for candidate in daemon.UNIT_ACTIONS:
            self.assertEqual(daemon.validate_unit_action(candidate), (candidate, None))

    def test_the_argv_never_becomes_more_than_one_command(self):
        """The point of the whole exercise: whatever a unit name contains, it is one
        argv element and the verb is another."""
        argv_pieces = []

        def fake_run_argv(argv, timeout=None):
            argv_pieces.append(list(argv))
            return 0, "", ""

        original = daemon.run_argv
        daemon.run_argv = fake_run_argv
        try:
            daemon.run_unit_action("restart", ["zookeeper", "vali"])
        finally:
            daemon.run_argv = original
        self.assertEqual(argv_pieces,
                         [["systemctl", "restart", "--", "zookeeper", "vali"]])

    def test_daemon_reload_takes_no_units(self):
        argv_pieces = []

        def fake_run_argv(argv, timeout=None):
            argv_pieces.append(list(argv))
            return 0, "", ""

        original = daemon.run_argv
        daemon.run_argv = fake_run_argv
        try:
            daemon.run_unit_action("daemon-reload", [])
        finally:
            daemon.run_argv = original
        self.assertEqual(argv_pieces, [["systemctl", "daemon-reload"]])


class PortValidationTests(unittest.TestCase):
    def test_the_decimal_string_form_is_accepted(self):
        """It arrives in a query string, where every value is text."""
        self.assertEqual(daemon.validate_port("9042"), (9042, None))

    def test_out_of_range_and_non_numeric_are_refused(self):
        for candidate in (0, -1, 65536, "", "9042; reboot", "0x2382", None, True, 1.5):
            port, error = daemon.validate_port(candidate)
            self.assertIsNone(port, candidate)
            self.assertIsNotNone(error, candidate)


class ListenerParsingTests(unittest.TestCase):
    SAMPLE = (
        "State  Recv-Q Send-Q  Local Address:Port  Peer Address:Port Process\n"
        "LISTEN 0      4096          0.0.0.0:9042       10.0.90.42:*"
        " users:((\"scylla\",pid=9042,fd=12))\n"
        "LISTEN 0      128              [::]:22             [::]:*\n"
        "LISTEN 0      511                 *:8443              *:*"
        " users:((\"python3\",pid=99,fd=7))\n"
    )

    def test_the_header_row_is_not_a_listener(self):
        self.assertEqual(len(daemon.parse_ss_listeners(self.SAMPLE)), 3)

    def test_ports_are_numbers_and_addresses_lose_their_brackets(self):
        listeners = daemon.parse_ss_listeners(self.SAMPLE)
        self.assertEqual([entry["port"] for entry in listeners], [9042, 22, 8443])
        self.assertEqual([entry["address"] for entry in listeners],
                         ["0.0.0.0", "::", "*"])

    def test_the_process_and_pid_are_read_where_ss_gives_them(self):
        listeners = daemon.parse_ss_listeners(self.SAMPLE)
        self.assertEqual(listeners[0]["process"], "scylla")
        self.assertEqual(listeners[0]["pid"], 9042)
        self.assertIsNone(listeners[1]["process"])

    def test_a_port_number_appearing_elsewhere_is_not_a_listener(self):
        """The reason the endpoint exists. `ss -tlnp | grep 3370` matched the peer
        address 10.0.90.42 and the pid 9042 in the row above as readily as a bound
        port, and every caller read a match as "the service is up"."""
        ports = [entry["port"] for entry in daemon.parse_ss_listeners(self.SAMPLE)]
        self.assertNotIn(3370, ports)
        self.assertNotIn(90, ports)


class UnitStateParsingTests(unittest.TestCase):
    SAMPLE = (
        "Id=zookeeper.service\n"
        "LoadState=loaded\n"
        "ActiveState=active\n"
        "SubState=running\n"
        "UnitFileState=enabled\n"
        "\n"
        "Id=vali.service\n"
        "LoadState=loaded\n"
        "ActiveState=activating\n"
        "SubState=start\n"
        "UnitFileState=\n"
    )

    def test_states_are_keyed_by_the_unit_systemd_named(self):
        """Not by request order. `systemctl is-active a b c` answers with three bare
        words and leaves the caller matching them to units by line number, so one
        missing line shifts every unit's state onto its neighbour."""
        states = daemon.parse_systemctl_show(self.SAMPLE)
        self.assertEqual([state["unit"] for state in states], ["zookeeper", "vali"])

    def test_active_is_derived_once_rather_than_compared_at_every_call_site(self):
        states = daemon.parse_systemctl_show(self.SAMPLE)
        self.assertIs(states[0]["active"], True)
        self.assertIs(states[1]["active"], False)

    def test_an_empty_property_reads_as_unknown_rather_than_blank(self):
        states = daemon.parse_systemctl_show(self.SAMPLE)
        self.assertEqual(states[1]["unit_file_state"], "unknown")


class AddressParsingTests(unittest.TestCase):
    SAMPLE = (
        '[{"ifname":"ens192","addr_info":['
        '{"family":"inet","local":"10.10.102.41","prefixlen":24,"scope":"global"}]}]'
    )

    def test_the_cidr_is_given_rather_than_left_to_the_caller(self):
        """The console rebuilt it by running `ip addr show <iface> | grep 'inet '`
        through a shell, having already been handed the two halves."""
        addresses = daemon.parse_ip_addr_json(self.SAMPLE)
        self.assertEqual(addresses[0]["cidr"], "10.10.102.41/24")

    def test_an_address_without_a_prefix_has_no_cidr(self):
        addresses = daemon.parse_ip_addr_json(
            '[{"ifname":"lo","addr_info":[{"family":"inet","local":"127.0.0.1"}]}]')
        self.assertIsNone(addresses[0]["cidr"])


class InterfaceClassificationTests(unittest.TestCase):
    def test_the_interfaces_the_console_never_offered_are_marked_virtual(self):
        for name in ("lo", "virbr0", "br-abc123", "vxlan100", "veth9f", "vnet3",
                     "macvtap0"):
            self.assertTrue(daemon.is_virtual_interface(name), name)

    def test_a_real_uplink_is_not(self):
        for name in ("ens192", "eth0", "eno1", "bond0", "enp3s0f1"):
            self.assertFalse(daemon.is_virtual_interface(name), name)


class RouteTests(unittest.TestCase):
    """Every typed route this work added must actually be dispatched. A handler with no
    route is a 404 that reads as "this node is too old for the endpoint", which is the
    one failure every caller of these has a quiet fallback for."""

    def test_the_new_endpoints_are_routed(self):
        source = read(DAEMON)
        for path, handler in (
                ("/api/v1/host/units", "handle_host_units"),
                ("/api/v1/host/units", "handle_host_units_action"),
                ("/api/v1/host/listeners", "handle_host_listeners"),
                ("/api/v1/host/interfaces", "handle_host_interfaces")):
            with self.subTest(path=path, handler=handler):
                self.assertIn('"%s"' % path, source)
                self.assertIn("def %s(" % handler, source)
                self.assertIn("self.%s(" % handler, source)

    def test_every_path_branch_says_it_handled_the_request(self):
        """`/api/v1/host/cpu` was written as the `if` half of an if/elif pair and so
        answered 200 and then fell through to do_GET's 404, which was written onto the
        end of the response it had already sent.

        Read from the AST: a branch that tests `path == "<literal>"` must end in
        `return True`, whatever the handler inside it is called."""
        tree = ast.parse(read(DAEMON))
        offenders = []
        for router in ("route_typed_get", "route_typed_post"):
            function = next(node for node in ast.walk(tree)
                            if isinstance(node, ast.FunctionDef) and node.name == router)
            for node in ast.walk(function):
                if not isinstance(node, ast.If):
                    continue
                test = node.test
                if not (isinstance(test, ast.Compare)
                        and isinstance(test.ops[0], ast.Eq)
                        and isinstance(test.comparators[0], ast.Constant)
                        and str(test.comparators[0].value).startswith("/api/")):
                    continue
                last = node.body[-1]
                returns_true = (isinstance(last, ast.Return)
                                and isinstance(last.value, ast.Constant)
                                and last.value.value is True)
                if not returns_true:
                    offenders.append("%s: %s" % (router, test.comparators[0].value))
        self.assertEqual(
            offenders, [],
            "these path branches do not end in `return True`, so the request falls "
            "through and a second response is written after the first: %s" % offenders)


if __name__ == "__main__":
    unittest.main()
