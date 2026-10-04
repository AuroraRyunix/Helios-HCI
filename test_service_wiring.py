#!/usr/bin/env python3
"""A daemon is registered by hand in a dozen files. This is what stops one being forgotten.

Adding a service to this stack means writing its name into a hard-coded list in `provision.py`,
`deploy_updates.py`, `cluster_new.py`, `spark.py`, `spark_daemon_decoded.py`, `mcli`,
`mcli-runner`, `hylia.py`, `vali.py`, `mipha.py`, `check_updates.py`, `create_upgrade_zip.py`,
`sync_provision.py`, `static/app.js`, the Phoenix console and `helios_zk.py` -- and writing its
systemd unit twice, once in `provision.py` and once in `deploy_updates.py`. Nothing connected
any of those to each other, and each omission has already cost something:

  * `cluster start` restarted the `aether` unit for months after it was deleted with DRBD,
    because one copy of the list was never told. A name in a list that nothing installs is a
    start that fails on every node, every time.
  * Two of three nodes could not be created over a Secure Boot check that three other files
    had dropped. Nobody could say which files "all the places" were.
  * `cluster destroy` and `cleanup_services` went on stopping a service list that predated
    `slate` and `agahnim`, so a destroyed cluster left both running.

So the question "where does a service have to be written down" is answered here, by a test,
instead of by whoever last added one remembering. The single source of truth for which
services exist is `MANAGED_SERVICES` in `spark_daemon_decoded.py` -- the declared table the
reconcile loop uses to start and stop the cluster in dependency order. Every service in it
must appear in every registry that applies to it.

Registries do not all apply to every service, and the rule is written down rather than
implied. A service has a KIND (what it is made of); a registry applies to the kinds that
could possibly belong in it. Anything narrower than that is an EXEMPTION on one registry,
and every exemption carries its reason. An exemption nobody needs any more fails the test
too, so the table of decisions cannot outlive the decisions.

Three things are asserted besides presence:

  * **Nothing names a unit that does not exist.** The `aether` bug, generalised: every name in
    a service list, and every unit handed to `systemctl` in a command string, must be a unit
    that something installs.
  * **The two copies of a daemon's unit text are identical.** `provision.py` is one
    self-contained file that embeds its payloads and is run from anywhere, so the unit text
    cannot be imported from a shared module without changing what provisioning is. The copies
    are asserted equal instead, which is the difference between a rollout that converges a
    node and one that quietly downgrades it.
  * **A daemon's unit starts it properly**: it runs `/usr/local/bin/<name>` (never `<name>.py`),
    restarts on failure, is installable, and does not buffer its stdout out of the journal.

Nothing here imports the modules under test: they open sockets, rewrite `provision.py` at
import time or are 1.6 MB of base64. Everything is read statically.

Run with:  python -m unittest test_service_wiring
"""

import ast
import glob
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def read(*parts):
    with open(os.path.join(HERE, *parts), "r", encoding="utf-8", newline="") as handle:
        return handle.read().replace("\r\n", "\n")


_TREES = {}


def tree(path):
    if path not in _TREES:
        _TREES[path] = ast.parse(read(path))
    return _TREES[path]


# -- the declared table: the one source of truth ---------------------------------------------

def declared_services():
    for node in tree("spark_daemon_decoded.py").body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "MANAGED_SERVICES":
            return ast.literal_eval(node.value)
    raise AssertionError("MANAGED_SERVICES is gone from spark_daemon_decoded.py, and with it "
                         "the one place this test learns which services exist")


TABLE = declared_services()
UNITS = tuple(entry["unit"] for entry in TABLE)
DISPLAY = dict((entry["unit"], entry["display"]) for entry in TABLE)
# A service that is declared but can be switched off by a cluster setting. Where a list builds
# itself around such a service -- `if check_urbosa_enabled(): services.append("urbosa")` -- the
# name is conditional by design, so it only has to appear somewhere in the same function.
SETTING_GATED = frozenset(entry["unit"] for entry in TABLE if entry.get("setting"))

# Units the stack drives or reports without owning them as a daemon of its own. They are
# real units, so a list naming them is not naming a ghost, but they are not in the table:
# ZooKeeper is the store the desired state lives in (started before convergence begins),
# spark-daemon is the process running the loop, and the rest are host units.
INFRASTRUCTURE_UNITS = frozenset((
    "zookeeper", "spark-daemon", "libvirtd", "virtqemud", "virtnetworkd", "chronyd",
))
INFRASTRUCTURE_DISPLAY = frozenset(("ZooKeeper", "Spark", "Libvirtd"))


# -- what each service is made of, and so which registries could ever hold it ----------------
#
#   script       ships a Python script that lands in /usr/local/bin and is listed in the
#                inventories that carry scripts (LCM, the upgrade package, embedded payloads)
#   native-unit  has a systemd unit of its own, written by provision.py and by the rollout
#   quadlet      runs as a container, so its unit is generated from a .container file
#   rust-crate   is a compiled crate built on the node by cargo, not a script
#
# `hydra-db`, `slate` and `agahnim` are the cases the owner named, and they are absent from the
# registries that ship Python scripts for different reasons, which is why the kinds differ:
# hydra-db (ScyllaDB) and slate (Traefik) are third-party container images -- there is no
# file of ours to ship. `agahnim` and `sidon` are Rust crates, shipped as sources and built
# on the node (`SOURCE_COMPONENTS` / `RUST_CRATES`), so they appear in those registries
# instead. `spectrum` is both: a Python script run inside a container of our own building.
DAEMON = frozenset(("script", "native-unit"))
KINDS = {
    "hydra-db": frozenset(("quadlet",)),
    "daruk": DAEMON,   # a script, but run inside the hydra-db container by `podman exec`
    "sidon": frozenset(("rust-crate", "native-unit")),
    "spectrum": frozenset(("script", "quadlet")),
    "slate": frozenset(("quadlet",)),
    "spectrum-phx": frozenset(("quadlet",)),
    "agahnim": frozenset(("rust-crate", "native-unit")),
    "catalyst": DAEMON,
    "vali": DAEMON,
    "bifrost": DAEMON,
    "dagur": DAEMON,
    "mimir": DAEMON,
    "logos": DAEMON,
    "mipha": DAEMON,
    "gatoway": DAEMON,
    "urbosa": DAEMON,
    "hylia": DAEMON,
    "rauru": DAEMON,
}

# The file a script service is embedded from, where it is not simply `<unit>.py`, and the
# path it is installed to, where that is not simply `/usr/local/bin/<unit>`.
SCRIPT_SOURCE = {"spectrum": "spectrum_server.py"}
SCRIPT_TARGET = {"daruk": "daruk.py", "spectrum": "spectrum_server"}


def script_source(unit):
    return SCRIPT_SOURCE.get(unit, unit + ".py")


def script_target(unit):
    return SCRIPT_TARGET.get(unit, unit)


def has(kind):
    return lambda unit: kind in KINDS.get(unit, ())


def everyone(unit):
    return True


# -- the lists in Python files ---------------------------------------------------------------

class Site(object):
    """One literal in a source file that names services, found by walking the syntax tree.

    A site is identified by where it lives -- the file, the enclosing function, the name it is
    assigned to and which of those it is -- and not by its line number, so it survives the file
    growing. `strings` are the exact string constants the literal holds (list elements, dict
    keys and values) and `tokens` are the whitespace-separated words of a command string, for the
    `"systemctl start a b c"` form. `function_strings` is every string constant anywhere in the
    enclosing function: where a name is added conditionally, this is where it will be.
    """

    def __init__(self, path, function, target, ordinal, lineno, strings, tokens, function_strings):
        self.path = path
        self.function = function
        self.target = target
        self.ordinal = ordinal
        self.lineno = lineno
        self.strings = strings
        self.tokens = tokens
        self.function_strings = function_strings

    @property
    def key(self):
        return (self.path, self.function, self.target, self.ordinal)

    def where(self):
        return "%s:%d (%s, %s #%d)" % (self.path, self.lineno, self.function, self.target, self.ordinal)


def flavour_name(unit, flavour):
    """How a service is spelled in a given kind of list."""
    if flavour == "display":
        return DISPLAY[unit]
    if flavour == "status":
        return unit + "_status"
    return unit


class _Walker(ast.NodeVisitor):
    def __init__(self, path):
        self.path = path
        self.scope = []
        self.assigned = []
        self.found = []
        self.counts = {}

    def _function_strings(self, node):
        found = set()
        for child in ast.walk(node):
            if isinstance(child, ast.Constant) and isinstance(child.value, str):
                found.add(child.value)
        return found

    def _enter(self, node):
        self.scope.append((node.name, node))
        self.generic_visit(node)
        self.scope.pop()

    visit_FunctionDef = _enter
    visit_AsyncFunctionDef = _enter
    visit_ClassDef = _enter

    def _where(self):
        names = [name for name, _ in self.scope]
        function = ".".join(names) or "<module>"
        # The strings of the innermost enclosing function, or the whole module at top level.
        innermost = None
        for name, node in reversed(self.scope):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                innermost = node
                break
        return function, innermost

    def visit_Assign(self, node):
        target = node.targets[0]
        self.assigned.append(target.id if isinstance(target, ast.Name) else "<expr>")
        self.generic_visit(node)
        self.assigned.pop()

    def visit_AnnAssign(self, node):
        self.assigned.append(node.target.id if isinstance(node.target, ast.Name) else "<expr>")
        self.generic_visit(node)
        self.assigned.pop()

    def _record(self, node, strings, tokens):
        function, innermost = self._where()
        target = self.assigned[-1] if self.assigned else "<expr>"
        base = (self.path, function, target)
        ordinal = self.counts.get(base, 0)
        self.counts[base] = ordinal + 1
        scope_strings = self._function_strings(innermost if innermost is not None else tree(self.path))
        self.found.append(Site(self.path, function, target, ordinal, node.lineno,
                               set(strings), set(tokens), scope_strings))

    def _consider(self, node, strings):
        if self._looks_like_a_service_list(strings):
            self._record(node, strings, ())

    @staticmethod
    def _looks_like_a_service_list(strings):
        # Five or more of the declared names, in any spelling a list uses. Three would catch
        # `["catalyst", "spark-daemon"]`, which is a pair and not an inventory; five is a list
        # that was clearly meant to say "the services".
        as_units = sum(1 for unit in UNITS if unit in strings)
        as_display = sum(1 for unit in UNITS if DISPLAY[unit] in strings)
        as_status = sum(1 for unit in UNITS if unit + "_status" in strings)
        return max(as_units, as_display, as_status) >= 5

    def visit_List(self, node):
        self._consider(node, [e.value for e in node.elts
                              if isinstance(e, ast.Constant) and isinstance(e.value, str)])
        self.generic_visit(node)

    visit_Tuple = visit_List
    visit_Set = visit_List

    def visit_Dict(self, node):
        strings = [k.value for k in list(node.keys) + list(node.values)
                   if isinstance(k, ast.Constant) and isinstance(k.value, str)]
        self._consider(node, strings)
        self.generic_visit(node)

    COMMAND_RE = re.compile(r"systemctl\s+(?:start|stop|restart|enable|disable)\b")

    def visit_Constant(self, node):
        if isinstance(node.value, str) and self.COMMAND_RE.search(node.value):
            words = node.value.split()
            if sum(1 for unit in UNITS if unit in words) >= 5:
                self._record(node, (), words)


def python_sources():
    """Every Python program in the repository that is not a test: daemons, CLIs, scripts."""
    paths = [os.path.basename(p) for p in glob.glob(os.path.join(HERE, "*.py"))]
    paths = [p for p in paths if not p.startswith("test_")]
    for name in sorted(os.listdir(HERE)):
        full = os.path.join(HERE, name)
        if "." in name or not os.path.isfile(full):
            continue
        with open(full, "rb") as handle:
            if handle.read(64).startswith(b"#!/usr/bin/env python"):
                paths.append(name)
    return sorted(paths)


_SITES = {}


def sites(path):
    if path not in _SITES:
        walker = _Walker(path)
        walker.visit(tree(path))
        _SITES[path] = walker.found
    return _SITES[path]


def find_site(path, function, target, nth):
    matches = [s for s in sites(path)
               if s.function.endswith(function) and s.target == target and s.ordinal == nth]
    return matches[0] if matches else None


class Registry(object):
    """A list in a Python file that has to name every service it applies to.

    `flavour` is how the list spells a service: its unit name, its display name, or the
    `<unit>_status` check id. `applies` says which services could belong in it at all -- that
    is the kind-level rule. `exempt` is `{unit: reason}` for a service that could belong and
    deliberately does not here; the reason is the point of the entry, and an entry whose
    service is in fact present is reported as stale.
    """

    def __init__(self, path, function, target, nth=0, flavour="unit", applies=everyone,
                 exempt=None, name=None):
        self.path = path
        self.function = function
        self.target = target
        self.nth = nth
        self.flavour = flavour
        self.applies = applies
        self.exempt = exempt or {}
        self.name = name or "%s %s.%s" % (path, function, target)


# What an exemption has to say, once, so it is not repeated at every site that shares it.
HYLIA_THROUGH_MAINTENANCE = (
    "hylia is the one daemon that runs through a maintenance window: it drives the rolling "
    "upgrade that puts hosts in and out of maintenance, and its unit carries no "
    "ConditionPathExists=!/etc/hci/maintenance.state, so entering maintenance never stops it "
    "and leaving it has nothing to start")
KEPT_UP_IN_MAINTENANCE = (
    "kept up in maintenance on purpose (the `maintenance: keep` column of MANAGED_SERVICES, "
    "docs/maintenance.md): the metadata and storage planes stay so the cluster's margins hold")
SETTING_NOT_READ_HERE = (
    "urbosa is switched on by the `urbosa_enabled` setting; this code does not read that "
    "setting, so naming the unit would start it on a cluster where it is off")
SUPERVISED_BY_THE_RECONCILE_LOOP = (
    "supervised by the declared-table reconcile loop (MANAGED_SERVICES) and systemd's "
    "Restart=always, not by this older fixed list; test_mimir_checks pins that slate and hylia "
    "are not in the watchdog's set")
STARTED_EARLIER_IN_THIS_FUNCTION = (
    "started and verified by name in an earlier phase of this same function, before the "
    "workload services this list covers")

LEGACY_WATCHDOG = {
    "slate": SUPERVISED_BY_THE_RECONCILE_LOOP,
    "agahnim": SUPERVISED_BY_THE_RECONCILE_LOOP,
    "hylia": SUPERVISED_BY_THE_RECONCILE_LOOP,
    "spectrum-phx": SUPERVISED_BY_THE_RECONCILE_LOOP,
}

REGISTRIES = [
    # -- the inventories that ship Python scripts ------------------------------------------
    # Only services that have a script: see KINDS for why hydra-db, slate, sidon and agahnim
    # are not in these.
    Registry("check_updates.py", "collect_inventory", "components_paths", flavour="component",
             applies=has("script")),
    Registry("create_upgrade_zip.py", "<module>", "components_map", flavour="component",
             applies=has("script")),
    Registry("spectrum_server.py", "do_GET", "components_paths", flavour="component",
             applies=has("script")),

    # -- the fast-patch restart map ----------------------------------------------------------
    Registry("hylia.py", "hylia_rolling_upgrade", "service_components", exempt={
        "spectrum-phx": ("the Phoenix console is not a component of the signed upgrade package: "
                         "its image is built from a source tree the package does not carry (see "
                         "TODO.md, the console image is not hermetic), so no fast patch changes "
                         "it and only deploy_updates.py rebuilds and restarts it"),
        "sidon": ("a fast patch must not restart the storage daemon: that detaches every "
                  "vdisk on the node, so a new sidon takes effect on the reboot the full "
                  "upgrade performs (see deploy_updates.py, `sidon is installed but not "
                  "restarted here`)"),
        "hylia": ("hylia is the orchestrator running this very code; it is restarted after "
                  "the job is recorded complete, not as part of it"),
    }),

    # -- what `cluster` and `spark` report and operate on ------------------------------------
    Registry("cluster_new.py", "<module>", "SERVICE_DISPLAY_ORDER", flavour="display"),
    Registry("cluster_new.py", "main", "cleanup_services", name="cluster create: stop everything first"),
    Registry("cluster_new.py", "main", "services", nth=0, name="cluster create: start the workloads",
             exempt={
                 "hydra-db": STARTED_EARLIER_IN_THIS_FUNCTION,
                 "daruk": STARTED_EARLIER_IN_THIS_FUNCTION,
                 "sidon": STARTED_EARLIER_IN_THIS_FUNCTION,
             }),
    Registry("cluster_new.py", "main", "services", nth=1, name="cluster destroy: stop everything"),
    Registry("spark.py", "show_status_json", "services"),
    Registry("spark.py", "show_status_json", "svc_map", flavour="display"),
    Registry("spark.py", "show_status", "services"),
    Registry("spark.py", "show_status", "svc_map", flavour="display"),
    Registry("spark.py", "main", "services", name="spark stop all"),

    # -- spark-daemon ------------------------------------------------------------------------
    Registry("spark_daemon_decoded.py", "build_node_status", "services"),
    Registry("spark_daemon_decoded.py", "build_node_status", "svc_map", flavour="display"),
    Registry("spark_daemon_decoded.py", "<module>", "MANAGED_UNITS",
             name="spark-daemon's allow-list of units it will act on"),
    Registry("spark_daemon_decoded.py", "handle_cluster_status", "svc_list", flavour="display"),
    Registry("spark_daemon_decoded.py", "forward_to_vali", "start_cmd",
             name="spark-daemon: start local services on leaving maintenance without vali",
             exempt={"hylia": HYLIA_THROUGH_MAINTENANCE, "urbosa": SETTING_NOT_READ_HERE}),
    Registry("spark_daemon_decoded.py", "handle_cluster_create", "services",
             name="spark-daemon: cluster create", exempt={
                 "hydra-db": STARTED_EARLIER_IN_THIS_FUNCTION,
                 "daruk": STARTED_EARLIER_IN_THIS_FUNCTION,
                 "sidon": STARTED_EARLIER_IN_THIS_FUNCTION,
                 "urbosa": ("a cluster being created has no `urbosa_enabled` setting to read "
                            "yet: the console seeds it false, so urbosa is off on a new cluster"),
             }),
    Registry("spark_daemon_decoded.py", "handle_cluster_destroy", "services",
             name="spark-daemon: cluster destroy"),
    Registry("spark_daemon_decoded.py", "check_cluster_and_autostart", "services_to_stop", nth=0,
             name="autostart: no cluster document, stop the workloads"),
    # The host-in-maintenance branch used to carry a list here. It stops
    # maintenance_stopped_units() now -- derived from the `maintenance` column of MANAGED_SERVICES,
    # which test_maintenance_flow holds against vali's MAINTENANCE_STOP_UNITS.
    Registry("spark_daemon_decoded.py", "check_cluster_and_autostart", "services_to_stop", nth=1,
             name="autostart: cluster is stopped, stop the workloads"),
    Registry("spark_daemon_decoded.py", "check_cluster_and_autostart", "services", nth=0,
             name="autostart: start the local workloads", exempt=LEGACY_WATCHDOG),
    Registry("spark_daemon_decoded.py", "check_cluster_and_autostart", "services", nth=1,
             name="watchdog: restart failed workloads", exempt=LEGACY_WATCHDOG),

    # -- the daemons that start and stop other hosts' services ---------------------------------
    Registry("vali.py", "<module>", "MAINTENANCE_STOP_UNITS", name="vali: enter maintenance",
             exempt={
                 "hydra-db": KEPT_UP_IN_MAINTENANCE,
                 "daruk": KEPT_UP_IN_MAINTENANCE,
                 "sidon": KEPT_UP_IN_MAINTENANCE,
                 "hylia": HYLIA_THROUGH_MAINTENANCE,
             }),
    Registry("mipha.py", "main", "start_units", name="mipha: rejoin a returned host",
             exempt={"urbosa": SETTING_NOT_READ_HERE}),

    # -- mcli and mcli-runner --------------------------------------------------------------------
    Registry("mcli", "<module>", "CHECK_ID_TO_FUNC", flavour="status"),
    Registry("mcli", "update_progress", "checks_map", flavour="status"),
    Registry("mcli-runner", "<module>", "WATCHDOG_SERVICES",
             name="mcli-runner: the units spark-daemon's watchdog restarts", exempt=LEGACY_WATCHDOG),
    Registry("mcli-runner", "check_services", "svcs", name="mcli-runner: per-service status checks"),
    Registry("mcli-runner", "check_services", "managed", name="mcli-runner: flapping check"),
]

# Lists that look like a service inventory and are not one, each with why. A list that is not
# here and is not a Registry fails `test_every_service_list_is_accounted_for`: a new list of
# services is either one that must be kept complete, or one somebody has decided need not be.
EXCLUDED = {
    ("spark.py", "check_any_cluster_service_active", "services", 0):
        ("an existence probe, not an inventory: it asks whether any one of these is active to "
         "decide whether `spark stop` should refuse. A service missing from it cannot make that "
         "answer wrong while the cluster is up, because every other service is up with it"),
    ("provision.py", "provision_single_node", "stale_quadlets", 0):
        ("units that USED to be Quadlets and whose leftover .container files are deleted. A "
         "native-unit service must not be added to it; it is a list of the past, not of services"),
    ("mcli-runner", "check_services", "services_to_check", 0):
        ("NOT widened, and that is a gap rather than a decision: this residue probe names six of "
         "the units vali stops when a host enters maintenance, so a seventh left running is not "
         "reported. Widening it changes what turns a maintenance check FAIL, and which of the "
         "others may legitimately stay up cannot be judged without a cluster. Recorded in TODO.md"),
}


# -- the registries that are not Python lists ------------------------------------------------

class TextRegistry(object):
    """A fixed pattern that has to match for every service it applies to.

    `pattern` is a format string with `{unit}`, `{name}` (the unit's script name) and
    `{source}` available, searched in `path` with DOTALL off. Used for the registries that are
    code and not literals: a `write_file(...)` call, a shell string, a JavaScript array.
    """

    def __init__(self, name, path, pattern, applies, exempt=None, scope=None):
        self.name = name
        self.path = path
        self.pattern = pattern
        self.applies = applies
        self.exempt = exempt or {}
        self.scope = scope

    def text(self):
        text = read(self.path)
        if self.scope:
            return "\n".join(match.group(1) for match in re.finditer(self.scope, text, re.S))
        return text

    def missing(self, unit):
        pattern = self.pattern.format(unit=re.escape(unit), name=re.escape(script_target(unit)),
                                      source=re.escape(script_source(unit)))
        return re.search(pattern, self.text(), re.M) is None


def enable_pattern():
    return r"systemctl\s+enable\s+(?:--now\s+)?(?:[a-z0-9-]+\s+)*{unit}(?![A-Za-z0-9_.-])"


QUADLET_UNITS_ARE_GENERATED = (
    "a Quadlet's unit is generated from its .container file and a generated unit cannot be "
    "enabled -- its [Install] section is what the generator acts on (deploy_updates.py says so "
    "beside the slate restart)")

TEXT_REGISTRIES = [
    # provision.py -------------------------------------------------------------------------------
    TextRegistry("provision.py writes the script", "provision.py",
                 r'write_file\("/usr/local/bin/{name}",',
                 applies=has("script"),
                 exempt={"spectrum": ("the console is not written to /usr/local/bin by "
                                      "write_file: it goes into the image's build context")}),
    TextRegistry("provision.py writes the systemd unit", "provision.py",
                 r'write_file\("/etc/systemd/system/{unit}\.service"', applies=has("native-unit")),
    TextRegistry("provision.py enables the unit", "provision.py", enable_pattern(),
                 applies=has("native-unit")),
    TextRegistry("provision.py writes the Quadlet", "provision.py",
                 r'write_file\("/etc/containers/systemd/{unit}\.container"', applies=has("quadlet")),
    # deploy_updates.py -- the rollout converges a node that already exists -----------------------
    TextRegistry("deploy_updates.py uploads the script", "deploy_updates.py",
                 r'(?:put_text_file|sftp\.put)\([^)\n]*"/usr/local/bin/{name}"',
                 applies=lambda u: has("script")(u) and u != "spectrum",
                 exempt={}),
    TextRegistry("deploy_updates.py writes the systemd unit", "deploy_updates.py",
                 r'sftp\.open\("/etc/systemd/system/{unit}\.service", "w"\)',
                 applies=has("native-unit"),
                 exempt={"sidon": ("KNOWN GAP, see KNOWN_GAPS: the rollout never writes sidon's "
                                   "unit; provision.py is the only writer")}),
    TextRegistry("deploy_updates.py enables the unit, so a node without one gains it",
                 "deploy_updates.py", enable_pattern(), applies=has("native-unit"),
                 exempt={"sidon": ("KNOWN GAP, see KNOWN_GAPS: installed but neither enabled nor "
                                   "restarted by the rollout, which is deliberate for the restart "
                                   "and unexamined for the enable")}),
    TextRegistry("deploy_updates.py makes the script executable", "deploy_updates.py",
                 r'chmod \+x[^"\n]*/usr/local/bin/{name}(?![A-Za-z0-9_.-])',
                 applies=lambda u: has("script")(u) and has("native-unit")(u) and u != "daruk",
                 exempt={}),
    TextRegistry("deploy_updates.py builds the crate", "deploy_updates.py",
                 r'\(\s*"{unit}",\s*"{unit}"\s*\)', applies=has("rust-crate")),
    TextRegistry("create_upgrade_zip.py packages the crate and names its unit",
                 "create_upgrade_zip.py", r'"unit":\s*"{unit}"', applies=has("rust-crate")),
    # sync_provision.py -----------------------------------------------------------------------------
    TextRegistry("sync_provision.py embeds the source", "sync_provision.py",
                 r'"[A-Z_]+_B64":\s*"{source}"', applies=has("script")),
    # the console ------------------------------------------------------------------------------------
    TextRegistry("app.js lists the service status checks", "static/app.js", r"'{unit}_status'",
                 applies=everyone, scope=r"const serviceChecks = \[(.*?)\];"),
    TextRegistry("app.js gives them friendly names", "static/app.js", r"'{unit}_status':",
                 applies=everyone, scope=r"const serviceFriendlyNames = \{(.*?)\};"),
    TextRegistry("app.js groups the LCM components", "static/app.js", r'"{unit}"',
                 applies=lambda u: has("script")(u), scope=r"const COMPONENT_GROUPS = \{(.*?)\};",
                 exempt={}),
    TextRegistry("health.ex classifies the service checks", "spectrum_phx/lib/spectrum_phx/health.ex",
                 r"(?<![A-Za-z0-9_-]){unit}_status(?![A-Za-z0-9_-])", applies=everyone,
                 scope=r"@service_checks ~w\((.*?)\)"),
    TextRegistry("mcli lists the available checks", "mcli", r'"{unit}_status"', applies=everyone,
                 scope=r"def show_list\(\):(.*?)for func, check_id, desc in checks"),
]

# Service names that appear in these lists only as history. A cluster upgraded from the DRBD era
# still has rows keyed by them, and dropping the name would file a historical row under "other".
HISTORICAL_CHECK_IDS = {
    "spectrum_phx/lib/spectrum_phx/health.ex": frozenset(("aether_status",)),
}


# -- the exceptions that are gaps, not decisions ------------------------------------------------
#
# Each entry is something this test found and did not fix, with why. The check says whether the
# gap still exists; the moment it does not, the entry fails and has to be deleted, so the list
# is a record of what is currently wrong and never of what once was.

def _unit_texts(path):
    """{unit: [unit file text, ...]} for every native unit file embedded in a Python source."""
    found = {}
    for node in ast.walk(tree(path)):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        text = node.value
        if not text.lstrip().startswith("[Unit]") or "[Service]" not in text:
            continue
        match = re.search(r"^ExecStart=(.*)$", text, re.M)
        if not match:
            continue
        command = match.group(1).strip()
        if "daruk.py" in command:
            unit = "daruk"
        else:
            executable = command.split()[0]
            unit = os.path.basename(executable)
        found.setdefault(unit, []).append(text)
    return found


def _spectrum_quadlet_pair():
    provision = None
    for node in tree("provision.py").body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "QUADLETS":
            provision = ast.literal_eval(node.value)["spectrum"]
    deploy = None
    for node in tree("deploy_updates.py").body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "spectrum_container_content":
            deploy = ast.literal_eval(node.value)
    return provision, deploy


KNOWN_GAPS = {
    "sidon is not installed by the rollout's unit writers": (
        "provision.py is the only place sidon's unit is written, and deploy_updates.py neither "
        "writes nor enables it (it only builds and installs the binary, and deliberately does not "
        "restart it). A node whose sidon unit is missing or stale cannot be repaired by a rollout. "
        "Left alone because the rollout's sidon handling and the unit's mount handling are being "
        "changed together in provision.py and deploy_updates.py by someone else; it belongs in "
        "that change. Recorded in TODO.md.",
        lambda: "sidon" not in _unit_texts("deploy_updates.py"),
    ),
    "the rollout writes a different console Quadlet from the one provisioning writes": (
        "deploy_updates.py's spectrum_container_content still carries `PodmanArgs=--privileged`, "
        "while provision.py's has `DropCapability=ALL` and `NoNewPrivileges=true` and explains why "
        "the privileged form was removed. A node provisioned today runs the hardened console and "
        "is put back to the privileged one by the next rollout. The two also disagree about the "
        "maintenance condition and the volume labels. Which is canonical is the owner's decision, "
        "and the change restarts the console on every node, so it is recorded rather than made. "
        "Recorded in TODO.md.",
        lambda: (lambda pair: pair[0] is not None and pair[0] != pair[1])(_spectrum_quadlet_pair()),
    ),
}

# -- hosts' own units that a command string may name ---------------------------------------------

HOST_UNITS = frozenset((
    "podman", "firewalld", "systemd-networkd", "systemd-resolved", "sshd",
))

# Units that no longer exist, which a command may still name -- but only to stop, disable or
# clear them, because cleaning up after a unit that is gone is exactly the one reason to say
# its name. Starting, enabling or restarting one is the `aether` bug.
REMOVED_UNITS = frozenset(("aether", "linstor-controller", "linstor-satellite", "hydra-db-proxy",
                           "helios-config-syncer", "odin", "spark"))
TEARDOWN_VERBS = frozenset(("stop", "disable", "reset-failed", "mask", "kill", "is-active", "status"))


def installed_units():
    """Every unit provision.py or the rollout writes a unit file for."""
    names = set()
    for path in ("provision.py", "deploy_updates.py"):
        text = read(path)
        names.update(re.findall(r"/etc/systemd/system/([A-Za-z0-9_.-]+?)\.service", text))
        names.update(re.findall(r"/etc/containers/systemd/([A-Za-z0-9_.-]+?)\.container", text))
    return names


COMMAND_RE = re.compile(
    r"systemctl\s+(?:--\S+\s+)*(start|stop|restart|enable|disable|reset-failed|is-active|"
    r"is-enabled|mask|unmask|reload|status)\s+([^\n\"']*)")
UNIT_WORD_RE = re.compile(r"^[a-z][a-z0-9-]*(?:\.service)?$")


def systemctl_mentions(path):
    """[(lineno, verb, unit)] for every unit a string constant hands to systemctl."""
    out = []
    docstrings = set()
    for node in ast.walk(tree(path)):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            docstrings.add(id(node.value))
    for node in ast.walk(tree(path)):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        if id(node) in docstrings:
            continue
        for match in COMMAND_RE.finditer(node.value):
            verb, rest = match.group(1), match.group(2)
            for word in re.split(r"\s+", rest.strip()):
                # A shell word that ends the command, or one that is not a plain unit name:
                # a redirect, a pipe, a variable, a placeholder, a flag.
                if not word or word[0] in "&|;<>2$-{(" or "{" in word or "$" in word:
                    break
                if not UNIT_WORD_RE.match(word):
                    break
                out.append((node.lineno, verb, re.sub(r"\.service$", "", word)))
    return out


# =============================================================================================
# The tests
# =============================================================================================

class TheTableIsTheSourceOfTruth(unittest.TestCase):

    def test_every_declared_service_has_a_recorded_kind(self):
        """A new service has to be classified before it can be registered anywhere.

        The kind is what decides which registries apply, so a service with none would be
        silently excused from the lot -- which is the failure this file exists to prevent.
        """
        self.assertEqual(sorted(set(UNITS) - set(KINDS)), [],
                         "these services are declared in MANAGED_SERVICES but have no KIND in "
                         "test_service_wiring.py, so no registry knows whether to expect them")

    def test_no_kind_describes_a_service_that_is_not_declared(self):
        self.assertEqual(sorted(set(KINDS) - set(UNITS)), [],
                         "KINDS names services MANAGED_SERVICES does not declare")

    def test_a_service_that_can_be_switched_off_is_still_declared(self):
        # Urbosa is the only one today; the point is that "off" is a state the table reports.
        self.assertTrue(SETTING_GATED <= set(UNITS))

    def test_the_table_is_readable_and_unique(self):
        self.assertEqual(len(UNITS), len(set(UNITS)))
        self.assertEqual(len(DISPLAY.values()), len(set(DISPLAY.values())))


class EveryServiceIsInEveryRegistryThatApplies(unittest.TestCase):

    def test_the_python_lists(self):
        for registry in REGISTRIES:
            with self.subTest(registry.name):
                site = find_site(registry.path, registry.function, registry.target, registry.nth)
                self.assertIsNotNone(
                    site, "%s no longer has a list %s in %s (#%d). If it moved, update the "
                          "REGISTRIES entry; if it was deleted, delete the entry"
                    % (registry.path, registry.target, registry.function, registry.nth))
                missing = []
                for unit in UNITS:
                    if not registry.applies(unit) or unit in registry.exempt:
                        continue
                    name = flavour_name(unit, registry.flavour)
                    present = name in site.strings or name in site.tokens
                    if not present and unit in SETTING_GATED:
                        present = name in site.function_strings
                    if not present:
                        missing.append(name)
                self.assertEqual(
                    missing, [],
                    "%s is missing %s. Add it, or if it genuinely does not belong, record why in "
                    "that registry's `exempt` in test_service_wiring.py" % (site.where(), missing))

    def test_an_exemption_that_is_no_longer_needed_is_removed(self):
        """A recorded decision that nobody needs is a decision nobody remembers making."""
        for registry in REGISTRIES:
            site = find_site(registry.path, registry.function, registry.target, registry.nth)
            if site is None:
                continue
            for unit in registry.exempt:
                name = flavour_name(unit, registry.flavour)
                with self.subTest("%s / %s" % (registry.name, unit)):
                    present = name in site.strings or name in site.tokens
                    self.assertFalse(
                        present,
                        "%s now lists %s, so the exemption for it in test_service_wiring.py is "
                        "stale and should be deleted" % (site.where(), name))

    def test_the_other_registries(self):
        for registry in TEXT_REGISTRIES:
            with self.subTest(registry.name):
                self.assertTrue(read(registry.path), registry.path)
                missing = [unit for unit in UNITS
                           if registry.applies(unit) and unit not in registry.exempt
                           and registry.missing(unit)]
                self.assertEqual(
                    missing, [],
                    "%s: %s has no entry for %s. Add it, or record why it does not belong in "
                    "that TextRegistry's `exempt` in test_service_wiring.py"
                    % (registry.path, registry.name, missing))

    def test_a_text_exemption_that_is_no_longer_needed_is_removed(self):
        for registry in TEXT_REGISTRIES:
            for unit in registry.exempt:
                if not registry.applies(unit):
                    continue
                with self.subTest("%s / %s" % (registry.name, unit)):
                    self.assertTrue(
                        registry.missing(unit),
                        "%s now has an entry for %s, so its exemption in test_service_wiring.py "
                        "is stale" % (registry.name, unit))

    def test_every_service_list_is_accounted_for(self):
        """A list of services nobody registered is one nobody will remember to extend.

        Every literal in a non-test Python program that names five or more declared services is
        either a Registry above -- which will then be kept complete -- or in EXCLUDED with the
        reason it is not an inventory. This is what makes a *new* list impossible to forget: the
        day someone writes `services = ["logos", "mipha", ...]` for a new code path, this fails
        until they choose.
        """
        registered = set((r.path, r.function.split(".")[-1], r.target, r.nth) for r in REGISTRIES)
        excluded = set(EXCLUDED)
        unaccounted = []
        for path in python_sources():
            for site in sites(path):
                leaf = site.function.split(".")[-1]
                if (site.path, leaf, site.target, site.ordinal) in registered:
                    continue
                if (site.path, site.function, site.target, site.ordinal) in excluded:
                    continue
                if (site.path, leaf, site.target, site.ordinal) in excluded:
                    continue
                unaccounted.append(site.where())
        self.assertEqual(
            unaccounted, [],
            "these lists name five or more services and are neither kept complete nor recorded as "
            "something else. Add each to REGISTRIES (so the next service is required in it) or to "
            "EXCLUDED with the reason it is not an inventory")

    def test_every_registered_list_still_exists(self):
        missing = [r.name for r in REGISTRIES
                   if find_site(r.path, r.function, r.target, r.nth) is None]
        self.assertEqual(missing, [])
        for key in EXCLUDED:
            path, function, target, nth = key
            with self.subTest(key):
                self.assertIsNotNone(
                    find_site(path, function.split(".")[-1], target, nth),
                    "EXCLUDED names a list that is gone: %r" % (key,))


class NoListNamesAUnitThatDoesNotExist(unittest.TestCase):
    """The `aether` bug, generalised.

    `cluster start` went on restarting a unit that had been deleted with DRBD, because one
    copy of the list was never told. Every other copy of that list was right, so no test of a
    single copy could have failed.
    """

    @staticmethod
    def _known():
        return set(UNITS) | INFRASTRUCTURE_UNITS

    def test_unit_lists_name_only_units_that_exist(self):
        known = self._known()
        offences = []
        for registry in REGISTRIES:
            if registry.flavour != "unit":
                continue
            site = find_site(registry.path, registry.function, registry.target, registry.nth)
            if site is None:
                continue
            for element in sorted(site.strings | site.tokens):
                if re.match(r"^[a-z][a-z0-9-]*$", element) and element not in known \
                        and element not in ("systemctl", "start", "stop", "restart", "enable",
                                            "disable"):
                    offences.append("%s names %r" % (site.where(), element))
        self.assertEqual(offences, [])

    def test_display_lists_name_only_services_that_exist(self):
        known = set(DISPLAY.values()) | INFRASTRUCTURE_DISPLAY
        offences = []
        for registry in REGISTRIES:
            if registry.flavour != "display":
                continue
            site = find_site(registry.path, registry.function, registry.target, registry.nth)
            if site is None:
                continue
            for element in sorted(site.strings):
                if re.match(r"^[A-Z][A-Za-z]+$", element) and element not in known:
                    offences.append("%s names %r" % (site.where(), element))
        self.assertEqual(offences, [])

    def test_check_id_lists_name_only_services_that_exist(self):
        """A `<unit>_status` check id for a unit that was removed is a check that can never pass.

        Only ids whose unit is a *removed* one are refused: the lists also hold checks that are
        not about a unit at all (`vip_binding_status`, `watchdog_daemon_status`), and a rule that
        guessed which was which would be wrong in both directions.
        """
        offences = []
        for registry in REGISTRIES:
            if registry.flavour != "status":
                continue
            site = find_site(registry.path, registry.function, registry.target, registry.nth)
            for element in sorted(site.strings):
                if element.endswith("_status") and element[:-len("_status")] in REMOVED_UNITS:
                    offences.append("%s names %r" % (site.where(), element))
        self.assertEqual(offences, [])

    def test_the_console_and_the_phoenix_lists_name_only_services_that_exist(self):
        """JavaScript and Elixir lists, where the same mistake was made in a different language.

        The console listed `aether_status` -- a check mcli-runner has never written since DRBD
        went -- and no `sidon_status`, which it does write, so the one service every guest disk
        depends on had no row in the console's service health view.
        """
        offences = []
        checks = (
            ("static/app.js", r"const serviceChecks = \[(.*?)\];"),
            ("static/app.js", r"const serviceFriendlyNames = \{(.*?)\};"),
            ("spectrum_phx/lib/spectrum_phx/health.ex", r"@service_checks ~w\((.*?)\)"),
        )
        for path, scope in checks:
            allowed = HISTORICAL_CHECK_IDS.get(path, frozenset())
            for block in re.findall(scope, read(path), re.S):
                for check_id in re.findall(r"[A-Za-z0-9_-]+_status", block):
                    unit = check_id[:-len("_status")]
                    if unit in REMOVED_UNITS and check_id not in allowed:
                        offences.append("%s names the check %r, whose service was removed"
                                        % (path, check_id))
        self.assertEqual(offences, [])

    def test_the_phoenix_display_order_names_the_services_the_daemon_reports(self):
        order = re.search(r"@service_display_order ~w\((.*?)\)", read(
            "spectrum_phx/lib/spectrum_phx/zk/state.ex"), re.S).group(1).split()
        known = set(DISPLAY.values()) | INFRASTRUCTURE_DISPLAY
        self.assertEqual([name for name in order if name not in known], [],
                         "the Phoenix console orders service names spark-daemon does not report "
                         "(`Aether` was the storage daemon before Sidon replaced it)")
        self.assertEqual([DISPLAY[u] for u in UNITS if DISPLAY[u] not in order], [],
                         "spark-daemon reports services the Phoenix console has no place for")

    def test_no_command_starts_enables_or_restarts_a_unit_nothing_installs(self):
        """A `systemctl` string is a list the AST walk cannot see.

        Names are checked against what is declared, what provisioning or the rollout writes a
        unit file for, and the host's own units. A removed unit may be named to clean up after it
        and nowhere else.
        """
        # `installed_units` reads paths out of strings, and the cleanup that deletes a removed
        # unit's file names that path too -- so a removed unit has to be taken back out.
        known = (set(UNITS) | INFRASTRUCTURE_UNITS | HOST_UNITS | installed_units()) - REMOVED_UNITS
        offences = []
        for path in python_sources():
            for lineno, verb, unit in systemctl_mentions(path):
                if unit in known:
                    continue
                if unit in REMOVED_UNITS and verb in TEARDOWN_VERBS:
                    continue
                offences.append("%s:%d `systemctl %s %s`" % (path, lineno, verb, unit))
        self.assertEqual(offences, [],
                         "a unit handed to systemctl that no declared service, installed unit or "
                         "host unit accounts for -- a ghost like `aether`, or a host unit missing "
                         "from HOST_UNITS in test_service_wiring.py")


class ElectionNames(unittest.TestCase):
    """A service that does work on one node at a time stands in an election, and says so.

    The name is the contract between the daemon that stands and anyone reading
    `/helios/leaders` to find out who won, so it lives in `helios_zk` beside the others and not
    as a string at the call site (docs/service_leadership.md).
    """

    NO_ELECTION = {
        "hydra-db": "ScyllaDB coordinates itself: gossip and lightweight transactions, not a leader",
        "daruk": "a stateless per-node query proxy; every node runs one and uses its own",
        "sidon": ("a per-node storage daemon: ownership of a vdisk is recorded on the vdisk, "
                  "with an epoch, rather than elected"),
        "spectrum": ("the console is stateless behind the VIP; the one leader-only job it hosts is "
                     "named for the job, `lanayru-queue`, and is not named for the daemon"),
        "slate": "a per-node Traefik; every node routes for itself",
        "spectrum-phx": "the console is stateless behind the VIP, like the Python tier it sits beside",
        "agahnim": "a per-node console proxy",
        "logos": "per-node telemetry: every node reports its own, so there is nothing to elect",
        "gatoway": "per-node bridge synchronisation: each node builds its own bridges",
        "urbosa": ("acts for whichever node holds the VIP, which `bifrost-vip` elects; it has no "
                   "election of its own"),
    }

    def _constants(self):
        text = read("helios_zk.py")
        return dict(re.findall(r'^(SERVICE_[A-Z_]+)\s*=\s*"([a-z-]+)"', text, re.M))

    def test_every_service_that_leads_has_an_election_name_beside_the_others(self):
        constants = self._constants()
        missing = []
        for unit in UNITS:
            if unit in self.NO_ELECTION:
                continue
            owned = [name for name in constants if name.startswith("SERVICE_" + unit.upper().replace("-", "_") + "_")]
            if not owned:
                missing.append(unit)
        self.assertEqual(
            missing, [],
            "no SERVICE_<NAME>_... election constant in helios_zk.py for %s. Add one, or record in "
            "ElectionNames.NO_ELECTION why the service has nothing to elect" % missing)

    def test_an_election_name_starts_with_the_service_that_stands_in_it(self):
        constants = self._constants()
        wrong = [(name, value) for name, value in constants.items()
                 if not value.startswith(name[len("SERVICE_"):].split("_")[0].lower() + "-")]
        self.assertEqual(wrong, [])

    def test_a_no_election_reason_is_never_out_of_date(self):
        constants = self._constants()
        stale = [unit for unit in self.NO_ELECTION
                 if any(n.startswith("SERVICE_" + unit.upper().replace("-", "_") + "_") for n in constants)]
        self.assertEqual(stale, [], "these services now stand in an election; drop the exemption")


class TheTwoCopiesOfAUnitAreOneUnit(unittest.TestCase):
    """provision.py and deploy_updates.py each write the unit, and a rollout overwrites it.

    Wherever the copies differ the rollout *rewrites* the unit as the other file's text, so a node
    provisioned one way runs another way after its first upgrade. The daruk unit was the live
    example: provisioning writes an `ExecStartPre` that copies the new daruk.py into the database
    volume on every start, so that an LCM patch which replaces only /usr/local/bin/daruk.py is
    actually run; the rollout wrote a unit without it, and every node that had ever been rolled
    out kept running the old copy after a fast patch.
    """

    def test_every_native_unit_is_written_identically_by_both(self):
        provision = _unit_texts("provision.py")
        deploy = _unit_texts("deploy_updates.py")
        differing = []
        for unit in UNITS:
            if not has("native-unit")(unit):
                continue
            if unit == "sidon":
                continue    # in KNOWN_GAPS: the rollout has no copy to compare
            self.assertIn(unit, provision, "provision.py writes no unit for %s" % unit)
            self.assertIn(unit, deploy, "deploy_updates.py writes no unit for %s" % unit)
            for copy in provision[unit] + deploy[unit]:
                if copy != provision[unit][0]:
                    differing.append(unit)
                    break
        self.assertEqual(differing, [],
                         "these units are written differently by provision.py and deploy_updates.py")

    def test_no_unit_is_written_by_the_rollout_that_provisioning_does_not_know(self):
        provision = _unit_texts("provision.py")
        extra = sorted(set(_unit_texts("deploy_updates.py")) - set(provision))
        self.assertEqual(extra, [])


class ADaemonsUnitStartsIt(unittest.TestCase):

    def _daemon_units(self):
        provision = _unit_texts("provision.py")
        for unit in UNITS:
            if has("script")(unit) and has("native-unit")(unit) and unit != "daruk":
                self.assertIn(unit, provision, "no unit text for %s" % unit)
                yield unit, provision[unit][0]

    def test_it_runs_the_installed_script_and_not_the_source_file(self):
        """Units run /usr/local/bin/<name>; nothing there is called <name>.py."""
        for unit, text in self._daemon_units():
            with self.subTest(unit):
                self.assertRegex(text, r"(?m)^ExecStart=/usr/local/bin/%s$" % re.escape(unit))

    def test_it_is_restarted_if_it_dies_and_started_at_boot(self):
        for unit, text in self._daemon_units():
            with self.subTest(unit):
                self.assertRegex(text, r"(?m)^Restart=always$")
                self.assertRegex(text, r"(?m)^WantedBy=multi-user\.target$")

    def test_its_output_reaches_the_journal(self):
        """Python block-buffers stdout when it is not a terminal, which under systemd it never is.

        Without PYTHONUNBUFFERED a daemon's startup line and its diagnostics sit in a 4 KB
        buffer, so a daemon that is working looks identical to one that is stuck. vali's unit
        carries a comment about the hours that cost; the rest were written before it.
        """
        for unit, text in self._daemon_units():
            with self.subTest(unit):
                self.assertRegex(text, r"(?m)^Environment=PYTHONUNBUFFERED=1$")

    def test_it_is_bounded_so_a_runaway_one_cannot_starve_the_host(self):
        for unit, text in self._daemon_units():
            with self.subTest(unit):
                self.assertRegex(text, r"(?m)^MemoryMax=\d+[MG]$")


class TheDeclaredGapsAreStillGaps(unittest.TestCase):
    """What this file found and did not fix, kept honest.

    An entry here is not an excuse: it is a thing that is wrong now, with the reason it was left.
    The moment the underlying problem is fixed the entry fails, and has to be deleted.
    """

    def test_each_gap_still_exists(self):
        for name, (reason, still_a_gap) in sorted(KNOWN_GAPS.items()):
            with self.subTest(name):
                self.assertTrue(still_a_gap(),
                                "this gap is closed: delete %r from KNOWN_GAPS (and from TODO.md)" % name)
                self.assertGreater(len(reason), 80, "a gap is recorded with the reason it was left")


if __name__ == "__main__":
    unittest.main()
