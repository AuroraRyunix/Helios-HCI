#!/usr/bin/env python3
"""`cluster start` and `cluster stop` declare, and the reconcile loop decides.

The CLI used to start ZooKeeper, then ScyllaDB, then Daruk, then thirteen more units by
hand -- and then wait for each node's reconcile loop to converge the *same* services
toward the desired state it had recorded in its first phase. Two actors drove every
service on a cold start, which is what the flapping was, and the ordering existed in two
places, which is what let one of them rot:

    `cluster start` restarted `aether` for months after the unit was deleted with DRBD.
    Every start failed on a service that does not exist, unnoticed, because nothing read
    that list but the CLI and the reconcile loop did not care what was in it.

So the property asserted here is the one whose violation produced that bug: **the CLI's
start and stop paths name no service at all.** A name that is not there cannot go stale.
The ordering it used to carry is asserted where it now lives -- in the declared service
table the loop walks -- along with the two things that make a failure legible: a
per-service `last_error` published in the node's status, and a `retry` flag that is the
node's own answer to "am I done?", so the CLI loops on the agent's judgement instead of
re-deriving convergence.

Run with:  python -m unittest test_cluster_declarative
"""

import ast
import io
import os
import re
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
CLI = os.path.join(HERE, "cluster_new.py")
SPARK = os.path.join(HERE, "spark_daemon_decoded.py")


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def load(path, functions=(), constants=(), scope=None):
    """Compile named functions and module constants out of a module that acts at import."""
    tree = ast.parse(read(path), filename=path)
    body = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in functions:
            body.append(node)
        elif isinstance(node, ast.Assign):
            names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if any(name in constants for name in names):
                body.append(node)
    found = set()
    for node in body:
        if isinstance(node, ast.FunctionDef):
            found.add(node.name)
        else:
            found.update(t.id for t in node.targets if isinstance(t, ast.Name))
    missing = (set(functions) | set(constants)) - found
    assert not missing, "%s no longer defines %s" % (os.path.basename(path), sorted(missing))
    scope = dict(scope or {})
    exec(compile(ast.Module(body=body, type_ignores=[]), path, "exec"), scope)
    return scope


def service_table():
    """The declared inventory, read out of the reconcile loop's own table."""
    source = read(SPARK)
    start = source.index("MANAGED_SERVICES = (")
    end = source.index("\n)", start) + 2
    return ast.literal_eval(source[start:end].split("=", 1)[1].strip())


def command_block(command):
    """The source of one `elif args.command == "..."` branch of the CLI's main()."""
    source = read(CLI)
    start = source.index('elif args.command == "%s":' % command)
    rest = source[start + 10:]
    offsets = [rest.index(marker) for marker in ('elif args.command ==', '\nif __name__')
               if marker in rest]
    return source[start:start + 10 + min(offsets)]


def code_only(text):
    """Code, not commentary. The comments necessarily name the services that were being
    started by hand, because that is what they explain."""
    return "\n".join(line for line in text.splitlines()
                     if not line.strip().startswith("#"))


def quoted_strings(text):
    """Every string literal in a block of code."""
    found = set()
    # The block is one `elif` arm lifted out of main(), so it is given an `if` to hang off.
    body = "\n".join("    " + line for line in text.replace("elif", "if", 1).splitlines())
    for node in ast.walk(ast.parse("if True:\n" + body)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            found.add(node.value)
    return found


class TheStartAndStopPathsNameNoService(unittest.TestCase):
    """Zero service names in the CLI's start and stop paths.

    Not a style rule. `aether` was a service name in this file, and the only thing that
    read it was this file, so nothing noticed when the unit it named stopped existing.
    """

    # Every unit the reconcile loop manages, plus the two it deliberately does not: the
    # store the desired state lives in and the daemon running the loop. Naming either in
    # the start path is how the CLI ends up driving lifecycle again.
    UNITS = sorted([entry["unit"] for entry in service_table()] +
                   ["zookeeper", "spark-daemon", "aether"])

    def blocks(self):
        return {"start": code_only(command_block("start")),
                "stop": code_only(command_block("stop"))}

    def test_no_service_is_named(self):
        for name, block in self.blocks().items():
            named = sorted(quoted_strings(block) & set(self.UNITS))
            self.assertEqual(
                named, [],
                "`cluster %s` names %s. A service named here is an ordering kept in two "
                "places, and the copy the reconcile loop does not read is the one that "
                "goes stale." % (name, named))

    def test_no_unit_lifecycle_is_driven_from_the_cli(self):
        for name, block in self.blocks().items():
            for forbidden in ("systemctl", "unit_action", "unit_is_active"):
                self.assertNotIn(
                    forbidden, block,
                    "`cluster %s` drives systemd units itself (%s); starting and stopping "
                    "units is the reconcile loop's job" % (name, forbidden))

    def test_both_paths_declare_a_desired_state_and_then_watch(self):
        for name, expected in (("start", "started"), ("stop", "stopped")):
            block = self.blocks()[name]
            self.assertIn("declare_cluster_state", block)
            self.assertIn(expected, quoted_strings(block))
            self.assertIn("wait_until_error_or_done", block)


class TheOrderingLivesInTheLoop(unittest.TestCase):
    """ZooKeeper -> ScyllaDB -> everything else did not evaporate when the phases went.

    The CLI's phases were not only duplication: Phase 2 waited for port 9042 before
    starting anything that stores data, and `systemctl is-active hydra-db` goes true tens
    of seconds before ScyllaDB answers there. Removing the phases without moving that gate
    would have replaced a redundant wait with no wait at all.
    """

    def setUp(self):
        self.table = service_table()
        self.by_unit = {entry["unit"]: entry for entry in self.table}

    def test_the_state_store_is_not_in_the_inventory(self):
        """ZooKeeper holds the desired state, so it cannot be converged toward it."""
        self.assertNotIn("zookeeper", self.by_unit)
        self.assertNotIn("spark-daemon", self.by_unit)

    def test_the_database_is_first_and_is_gated_on_answering(self):
        self.assertEqual(self.table[0]["unit"], "hydra-db")
        self.assertEqual(self.by_unit["hydra-db"]["ready_port"], 9042,
                         "without a readiness port the gate is on `active`, which is true "
                         "long before ScyllaDB accepts a query")
        self.assertEqual(self.by_unit["daruk"]["requires"], ("hydra-db",))
        self.assertEqual(self.by_unit["daruk"]["ready_port"], 9043)

    def test_everything_else_waits_for_the_metadata_path(self):
        for entry in self.table:
            if entry["unit"] in ("hydra-db", "daruk"):
                continue
            self.assertIn("daruk", entry["requires"],
                          "%s is started with no gate on the metadata path being "
                          "answerable" % entry["unit"])


class TheLoopConvergesInDependencyOrder(unittest.TestCase):
    """The gates are gates: a pass does what it can and the node says it is not done."""

    ORDER = ["hydra-db", "daruk", "sidon"]
    TABLE = ({"unit": "hydra-db", "requires": (), "ready_port": 9042},
             {"unit": "daruk", "requires": ("hydra-db",), "ready_port": 9043},
             {"unit": "sidon", "requires": ("daruk",), "drain_before_stop": True})

    def converge(self, states, desired="started", ports=(), failing=(), stale=None):
        """One pass over `states`, returning (issued commands, scope)."""
        issued = []
        drained = []

        class FakeCompleted:
            def __init__(self, out=b"", rc=0, err=b""):
                self.stdout = out
                self.returncode = rc
                self.stderr = err

        class FakeSubprocess:
            DEVNULL = -3
            PIPE = -1

            @staticmethod
            def run(command, shell=False, stdout=None, stderr=None):
                if command.startswith("systemctl is-active"):
                    return FakeCompleted("\n".join(states).encode())
                if command.startswith("ss -ltn"):
                    rows = ["State Recv-Q Send-Q Local-Address:Port Peer"]
                    rows += ["LISTEN 0 100 0.0.0.0:%d *:*" % port for port in ports]
                    return FakeCompleted("\n".join(rows).encode())
                issued.append(command)
                for name in failing:
                    if command.endswith(" " + name):
                        return FakeCompleted(rc=1, err=b"Job for %s failed" % name.encode())
                return FakeCompleted()

        scope = load(
            SPARK,
            functions=("converge_to_desired_state", "_converge_locked", "unit_active_states", "service_entry",
                       "service_is_disabled", "service_is_ready", "listening_ports",
                       "convergence_gate", "run_unit_commands"),
            scope={"MANAGED_SERVICE_ORDER": list(self.ORDER),
                   "CONVERGE_LOCK": __import__("threading").Lock(),
                   "CONVERGE_PARALLELISM": 8,
                   "UNIT_DOWN_STATES": ("inactive", "failed", ""),
                   "MANAGED_SERVICES": self.TABLE,
                   "SERVICE_SETTING_PROBES": {},
                   "SERVICE_ERRORS": dict(stale or {}),
                   "drain_local_storage": lambda: drained.append(True),
                   "subprocess": FakeSubprocess,
                   "print": lambda *a, **k: None})
        scope["converge_to_desired_state"](desired)
        self.drained = drained
        return issued, scope

    def test_nothing_is_started_before_the_database(self):
        issued, _ = self.converge(["inactive", "inactive", "inactive"])
        self.assertEqual(
            issued, ["systemctl start hydra-db"],
            "the services that store data were started alongside the database rather "
            "than after it")

    def test_an_active_database_that_is_not_answering_is_not_a_started_database(self):
        issued, _ = self.converge(["active", "inactive", "inactive"], ports=())
        self.assertEqual(
            issued, [],
            "hydra-db reports active tens of seconds before ScyllaDB accepts a "
            "connection on 9042; starting Daruk into that window is the wait the CLI's "
            "Phase 2 existed to do")

    def test_a_ready_database_releases_the_next_service(self):
        issued, _ = self.converge(["active", "inactive", "inactive"], ports=(9042,))
        self.assertEqual(issued, ["systemctl start daruk"])

    def test_a_node_that_still_has_work_says_so(self):
        _, scope = self.converge(["active", "inactive", "inactive"], ports=(9042,))
        self.assertTrue(scope["CONVERGE_RETRY"],
                        "the CLI loops on this flag; a node that stops asking before it "
                        "is finished ends the wait early")

    def test_a_converged_node_stops_asking(self):
        _, scope = self.converge(["active", "active", "active"], ports=(9042, 9043))
        self.assertFalse(scope["CONVERGE_RETRY"])

    def test_a_refused_start_is_latched_and_not_retried_forever(self):
        _, scope = self.converge(["inactive", "inactive", "inactive"], failing=("hydra-db",))
        self.assertIn("hydra-db", scope["SERVICE_ERRORS"])
        self.assertIn("Job for hydra-db failed", scope["SERVICE_ERRORS"]["hydra-db"])
        self.assertFalse(scope["CONVERGE_RETRY"],
                         "a service that refused to start is reported, not waited on")

    def test_a_service_that_came_up_clears_its_error(self):
        _, scope = self.converge(["active", "active", "active"], ports=(9042, 9043))
        self.assertEqual(scope["SERVICE_ERRORS"], {})

    def test_a_reason_does_not_outlive_the_problem_it_described(self):
        """systemd restarts things on its own. A service that refused once and is running
        now must not keep a reason attached to it: the CLI aborts a start at the first
        published error, so a stale one would stop every later start on a healthy
        cluster."""
        failed, _ = self.converge(["inactive", "inactive", "inactive"], failing=("hydra-db",))
        self.assertTrue(failed)
        _, scope = self.converge(["active", "active", "active"], ports=(9042, 9043),
                                 stale={"hydra-db": "Job for hydra-db failed"})
        self.assertEqual(scope["SERVICE_ERRORS"], {})

    def test_the_stop_order_is_the_start_order_inverted(self):
        issued, _ = self.converge(["active", "active", "active"], desired="stopped")
        self.assertEqual(
            issued, ["systemctl stop sidon", "systemctl stop daruk", "systemctl stop hydra-db"],
            "each service is stopped only after the ones that use it, and the whole chain "
            "in one pass: this used to stop one layer per pass, and the passes were a "
            "drift interval (30 s) apart, which is why `cluster stop` was slow")

    def test_a_service_that_will_not_stop_keeps_what_it_uses_up(self):
        """The reason the order exists. With sidon refusing to stop, Daruk and the database
        must stay: stopping them under a storage daemon that is still serving is the
        failure the gates are there to prevent."""
        issued, scope = self.converge(["active", "active", "active"], desired="stopped",
                                      failing=("sidon",))
        self.assertEqual(issued, ["systemctl stop sidon"])
        self.assertIn("sidon", scope["SERVICE_ERRORS"])

    def test_a_service_still_going_down_keeps_what_it_uses_up(self):
        """`deactivating` is not down. Sidon draining its journals for half a minute is
        still using Daruk, and Daruk was being stopped under it."""
        issued, _ = self.converge(["deactivating", "active", "active"], desired="stopped")
        self.assertEqual(issued, [], "Daruk was stopped while Sidon was still shutting down")

    def test_a_pass_that_left_work_asks_to_be_run_again_soon(self):
        """After the stops above nothing is left to stop, but the pass cannot know that
        until it has looked again, so it says it has work: the loop then runs the next pass
        in seconds and not after the drift interval."""
        _, scope = self.converge(["active", "active", "active"], desired="stopped")
        self.assertTrue(scope["CONVERGE_RETRY"])

    def test_storage_is_drained_before_it_is_stopped(self):
        # States are in the order being converged, which for a stop is the inverse: sidon
        # first.
        self.converge(["active", "active", "active"], desired="stopped")
        self.assertTrue(
            self.drained,
            "draining the journals before the storage daemon stops ran from `cluster "
            "stop`, so it happened only when an operator typed that command")


class EveryServiceRowCarriesItsReason(unittest.TestCase):
    """A start that cannot explain a failure is the thing being replaced.

    Failures ride in the published status, not in a journal on three hosts: every service
    row carries `last_error`, and the node publishes `retry` -- its own answer to whether
    it is finished -- alongside them.
    """

    def status_rows(self):
        """Every dict literal build_node_status() writes into result["services"]."""
        tree = ast.parse(read(SPARK))
        builder = next(node for node in ast.walk(tree)
                       if isinstance(node, ast.FunctionDef) and node.name == "build_node_status")
        rows = []
        for node in ast.walk(builder):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict):
                keys = [k.value for k in node.value.keys if isinstance(k, ast.Constant)]
                if "status" in keys and "pids" in keys:
                    rows.append(keys)
        return rows

    def test_every_row_has_a_last_error(self):
        rows = self.status_rows()
        self.assertTrue(rows, "build_node_status no longer publishes service rows")
        for keys in rows:
            self.assertIn("last_error", keys,
                          "a service row without last_error means a caller watching this "
                          "node has to be told to go and read a journal")

    def test_the_reason_describes_an_attempt_that_was_made(self):
        """`systemctl show -p Result` keeps saying `exit-code` until the unit is started
        again, so a cluster start that aborts at the first published error would abort on a
        unit that failed hours earlier and never on anything the start did."""
        reason = load(SPARK, functions=("service_last_error",))["service_last_error"]
        self.assertEqual(
            reason(latched=None, state="failed", result="exit-code", converging=True), "",
            "a stale systemd Result was published while the node was still converging")
        self.assertEqual(
            reason(latched=None, state="inactive", result="exit-code", converging=False), "",
            "a cleanly stopped unit was reported as an error because Result outlived the "
            "failure it described")
        self.assertIn(
            "exit-code",
            reason(latched=None, state="failed", result="exit-code", converging=False),
            "a unit that is failed now, on a node that has finished converging, explains "
            "itself to nobody")
        self.assertEqual(
            reason(latched="Job for vali.service failed", state="failed",
                   result="exit-code", converging=True),
            "Job for vali.service failed",
            "the loop's own reason is the one that describes an attempt somebody made")

    def test_the_node_publishes_whether_it_is_done(self):
        source = read(SPARK)
        self.assertIn('result["retry"] = ', source)
        self.assertIn('result["disabled_services"] = ', source)

    def test_a_disabled_service_is_reported_rather_than_omitted(self):
        """`cim_service: []` in Nutanix's own status: an empty PID list is information.

        It is published apart from `services` on purpose -- vali.select_best_start_host()
        requires every entry there to be UP, so a row that can never be UP makes every
        host ineligible for placement. That is exactly what the stale `aether` entry did.
        """
        table = {entry["unit"]: entry for entry in service_table()}
        self.assertEqual(table["urbosa"].get("setting"), "urbosa_enabled",
                         "a settings-gated service must be expressible as disabled, not "
                         "deleted from the inventory")


class TheWaitObservesAndAbortsOnAPublishedError(unittest.TestCase):
    """What `cluster start` does after it has declared the state: watch, and stop at the
    first published error rather than waiting out a timeout to say nothing useful."""

    IPS = ["10.0.0.1", "10.0.0.2"]

    def node(self, services, retry=False, maintenance="NORMAL"):
        return {"hostname": "n", "ts": 10 ** 10, "retry": retry,
                "maintenance_status": maintenance, "services": services}

    def wait(self, passes, op="start"):
        """Run the wait over a scripted sequence of published states."""
        printed = []
        reads = []

        class FakeTime:
            """A clock that only moves when the wait sleeps, so a wait that never
            finishes ends as a timeout rather than as a hung test."""
            now = [0.0]

            @classmethod
            def time(cls):
                return cls.now[0]

            @classmethod
            def sleep(cls, seconds):
                cls.now[0] += max(seconds, 1)

        def zk_read_cluster_state():
            reads.append(True)
            state = passes[min(len(reads) - 1, len(passes) - 1)]
            return None if state is None else {"nodes": state, "desired": "x", "via": "zk"}

        scope = load(
            CLI,
            functions=("wait_until_error_or_done", "published_service_errors",
                       "print_cluster_table", "render_node_block", "service_is_compliant"),
            constants=("SERVICE_DISPLAY_ORDER", "EXPECTED_SERVICES", "NODE_STALE_AFTER",
                       "UNREPORTED_NODE_ATTEMPTS", "GREEN", "RED", "YELLOW", "BOLD",
                       "GRAY", "RESET"),
            scope={"time": FakeTime, "zk_read_cluster_state": zk_read_cluster_state,
                   "print": lambda *a, **k: printed.append(" ".join(str(x) for x in a))})
        done = scope["wait_until_error_or_done"](self.IPS, op=op, timeout=30, poll=0)
        self.printed = "\n".join(printed)
        self.reads = len(reads)
        return done

    def up(self, pids=(1,)):
        return {name: {"status": "UP", "pids": list(pids), "restarts": 0, "last_error": ""}
                for name in ("HydraDB", "Daruk", "Sidon", "Spectrum")}

    def test_a_cluster_that_is_up_is_done(self):
        self.assertTrue(self.wait([{ip: self.node(self.up()) for ip in self.IPS}]))

    def test_compliance_is_pids_not_status(self):
        """A unit with Restart=always reports `active` during every restart window, so a
        service that has never once stayed up answers "started" as often as not."""
        services = self.up()
        services["Spectrum"] = {"status": "UP", "pids": [], "restarts": 7, "last_error": ""}
        self.assertFalse(self.wait([{ip: self.node(services) for ip in self.IPS}]))
        self.assertIn("Spectrum", self.printed)

    def test_a_stop_is_the_same_test_inverted(self):
        gone = {name: {"status": "DOWN", "pids": [], "restarts": 0, "last_error": ""}
                for name in self.up()}
        self.assertTrue(self.wait([{ip: self.node(gone) for ip in self.IPS}], op="stop"))
        still_there = dict(gone)
        still_there["Sidon"] = {"status": "UP", "pids": [42], "restarts": 0, "last_error": ""}
        self.assertFalse(self.wait([{ip: self.node(still_there) for ip in self.IPS}],
                                   op="stop"))

    def test_a_published_error_stops_the_wait_at_once(self):
        services = self.up()
        services["Sidon"] = {"status": "DOWN", "pids": [], "restarts": 2,
                             "last_error": "Job for sidon.service failed"}
        state = {ip: self.node(services) for ip in self.IPS}
        self.assertFalse(self.wait([state]))
        self.assertEqual(self.reads, 1,
                         "the wait kept polling after a node had already said why it "
                         "could not converge")
        self.assertIn("Job for sidon.service failed", self.printed)

    def test_the_table_it_prints_is_the_whole_inventory(self):
        services = self.up()
        services["Sidon"] = {"status": "DOWN", "pids": [], "restarts": 0,
                             "last_error": "Job for sidon.service failed"}
        self.wait([{ip: self.node(services) for ip in self.IPS}])
        for name in ("HydraDB", "Daruk", "Sidon", "Spectrum"):
            self.assertIn(name, self.printed,
                          "a service with no PIDs was omitted from the table; an empty "
                          "PID list is information, a missing row is not")

    def test_a_node_that_never_reports_is_retried_and_then_tolerated(self):
        partial = {self.IPS[0]: self.node(self.up())}
        self.assertTrue(self.wait([partial]),
                        "one unreachable node aborted an operation the rest of the "
                        "cluster completed")
        self.assertGreaterEqual(self.reads, 10,
                                "the missing node was abandoned without being retried")
        self.assertIn(self.IPS[1], self.printed)

    def test_a_node_in_maintenance_is_not_waited_on(self):
        state = {self.IPS[0]: self.node(self.up()),
                 self.IPS[1]: self.node(self.up(pids=()), maintenance="IN_MAINTENANCE")}
        self.assertTrue(self.wait([state]))

    def test_a_node_that_says_it_has_work_left_is_waited_on(self):
        busy = {ip: self.node(self.up(), retry=True) for ip in self.IPS}
        done = {ip: self.node(self.up()) for ip in self.IPS}
        self.assertTrue(self.wait([busy, done]))
        self.assertEqual(self.reads, 2,
                         "the wait finished while a node was still reporting work to do")


if __name__ == "__main__":
    unittest.main()


class TheLoopDoesNotIdleBetweenTiers(unittest.TestCase):
    """Why `cluster stop` and `cluster start` were slow: one pass per layer, and the pass
    interval is the drift interval."""

    def setUp(self):
        self.src = read(SPARK)

    def test_a_pass_with_work_left_is_followed_by_a_short_wait(self):
        self.assertIn("ZK_CONVERGE_RETRY_INTERVAL", self.src)
        self.assertIn("woken.wait(ZK_CONVERGE_RETRY_INTERVAL if retry_soon else "
                      "ZK_DRIFT_CHECK_INTERVAL)", self.src)
        interval = int(re.search(r"^ZK_CONVERGE_RETRY_INTERVAL = (\d+)", self.src, re.M).group(1))
        drift = int(re.search(r"^ZK_DRIFT_CHECK_INTERVAL = (\d+)", self.src, re.M).group(1))
        self.assertLess(interval * 5, drift)

    def test_independent_units_are_acted_on_together(self):
        """The leaf services of a stop depend on nothing among themselves; each blocks until
        its unit is down, so one after the other costs the sum of their stop times."""
        calls = []
        lock = threading.Lock()
        running = {"now": 0, "peak": 0}

        class Done:
            returncode = 0
            stderr = b""

        class FakeSubprocess:
            DEVNULL = -3
            PIPE = -1

            @staticmethod
            def run(command, shell=False, stdout=None, stderr=None):
                with lock:
                    running["now"] += 1
                    running["peak"] = max(running["peak"], running["now"])
                time.sleep(0.05)
                with lock:
                    running["now"] -= 1
                    calls.append(command)
                return Done()

        scope = load(SPARK, functions=("run_unit_commands",),
                     scope={"CONVERGE_PARALLELISM": 8, "subprocess": FakeSubprocess})
        results = scope["run_unit_commands"]("stop", ["a", "b", "c", "d"])
        self.assertEqual(sorted(results), ["a", "b", "c", "d"])
        self.assertEqual(sorted(calls), ["systemctl stop %s" % u for u in "abcd"])
        self.assertGreater(running["peak"], 1, "the units were stopped one at a time")

    def test_a_failure_names_its_unit(self):
        class Fail:
            returncode = 1
            stderr = b"Job for b failed"

        class Done:
            returncode = 0
            stderr = b""

        class FakeSubprocess:
            DEVNULL = -3
            PIPE = -1

            @staticmethod
            def run(command, shell=False, stdout=None, stderr=None):
                return Fail() if command.endswith(" b") else Done()

        scope = load(SPARK, functions=("run_unit_commands",),
                     scope={"CONVERGE_PARALLELISM": 8, "subprocess": FakeSubprocess})
        results = scope["run_unit_commands"]("stop", ["a", "b"])
        self.assertEqual(results["a"][0], 0)
        self.assertEqual(results["b"], (1, "Job for b failed"))
