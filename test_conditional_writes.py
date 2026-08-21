#!/usr/bin/env python3
"""Tests for the ownership writes that were still unconditional after the first CAS pass.

Four failures, one shape. Something reads a value, decides it is allowed to act, and
writes back without checking that the value is still there:

  * Catalyst reads `last_run_epoch`, decides a scheduled job is due, and writes the
    current time back. Two Catalyst instances that both believe they hold leadership --
    which `is_zookeeper_leader()` permits, since it probes ZooKeeper's four-letter `stat`
    and falls back to "lowest node with 9091 open" -- both submit the same Dagur job.
  * Mipha's HA failover resets `state='Stopped', host_ip=''` on every VM of a fenced host.
    A VM already recovered elsewhere is unplaced by the late write, and the start task
    that follows boots a second copy of it against the same DRBD device.
  * Lanayru writes `hydra.vms SET status='running'` on a VM it has just created. `status`
    is the migration-lock column, and Lanayru has never held that lock.
  * `run_cql_query` renders a rejected lightweight transaction as the string
    "False 10.10.102.41" with rc=0, indistinguishable from success, so any caller using it
    for a conditional write reads every lost race as a win.

The fake Scylla is `test_daruk_lwt.FakeScyllaSession`, imported rather than copied. Its
behaviour was read off a live Scylla 5.4 through Daruk, and two files encoding those
observations separately would eventually encode them differently.

Run with:  python -m unittest test_conditional_writes
"""

import ast
import inspect
import io
import json
import os
import re
import sys
import textwrap
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# Installs the fake cassandra-driver, loads daruk.py against it, and serves one Daruk over
# HTTP for the whole run. Importing it is what makes the modules below importable at all.
import test_daruk_lwt as base

SESSION = base.SESSION
daruk = base.daruk
call = base.call
DARUK_TEST_URL = base.DARUK_TEST_URL

VMS = base.VMS
NODES = base.NODES
DAGUR_SCHEDULES = "hydra.dagur_schedules"
MIMIR_SCHEDULES = "hydra.mimir_schedules"

# lanayru.py does `from spectrum_server import ...` inside its worker, so the name has to
# resolve to the module that was loaded against the fake driver rather than to a second,
# real-driver import of the same file.
sys.modules["spectrum_server"] = base.spectrum
spectrum = base.spectrum

catalyst = base.load_module("catalyst_under_test", "catalyst.py")
mipha = base.load_module("mipha_under_test", "mipha.py")
dagur = base.load_module("dagur_under_test", "dagur.py")
lanayru = base.load_module("lanayru_under_test", "lanayru.py")


_DML_START = re.compile(r"\A\s*(?:insert|update|delete|begin)\b", re.I)


def cql_statements_in(target):
    """Every string literal in `target`'s source that reads as a CQL write.

    Read off the AST rather than grepped out of the text, because the comments explaining
    these fixes quote the statements they replaced -- a substring search finds the
    explanation and reports the bug as still present.
    """
    source = textwrap.dedent(inspect.getsource(target))
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            text = node.value
        elif isinstance(node, ast.JoinedStr):
            text = "".join(
                part.value if isinstance(part, ast.Constant) and isinstance(part.value, str)
                else "?" for part in node.values)
        else:
            continue
        if _DML_START.match(text):
            found.append(" ".join(text.split()))
    return found


class _StopLoop(Exception):
    """Raised from the fake clock to end a daemon's `while True` after one pass."""


class FakeClock:
    """Enough of the `time` module to run one iteration of a scheduler loop.

    The loops under test end with `time.sleep(...)` *outside* their try/except, so raising
    there is the one way to leave them after exactly one pass without adding a test-only
    exit to production code.
    """

    def __init__(self, now):
        self.now = now
        self.slept = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        raise _StopLoop()


class ConditionalWriteTestCase(unittest.TestCase):
    def setUp(self):
        SESSION.store.clear()
        SESSION.prepared.clear()
        catalyst.DARUK_URL = DARUK_TEST_URL
        mipha.DARUK_URL = DARUK_TEST_URL
        spectrum.DARUK_URL = DARUK_TEST_URL
        # These daemons narrate what they are doing, and a test run is not the place for
        # it. Capturing rather than discarding also keeps the provisioner's log off a
        # Windows console, whose cp1252 codec raises on the emoji in its success message
        # and would otherwise turn a successful deployment into a caught exception.
        self._stdout, sys.stdout = sys.stdout, io.StringIO()
        self.addCleanup(self.restore_stdout)

    def restore_stdout(self):
        self.captured_stdout = sys.stdout.getvalue()
        sys.stdout = self._stdout

    def given_vm(self, name, **columns):
        columns.setdefault("host_ip", "")
        columns.setdefault("state", "Stopped")
        return SESSION.put(VMS, name, name=name, **columns)

    def given_job(self, job_name, **columns):
        columns.setdefault("enabled", True)
        columns.setdefault("interval_seconds", 3600)
        columns.setdefault("command", "/usr/local/bin/mcli health_checks run_all")
        return SESSION.put(DAGUR_SCHEDULES, job_name, job_name=job_name, **columns)


# -- Catalyst: two schedulers, one job ----------------------------------------------------

class CatalystScheduleClaimTests(ConditionalWriteTestCase):
    def test_a_due_job_can_be_claimed(self):
        self.given_job("storage_scrub", last_run_epoch=1000)
        self.assertTrue(catalyst.claim_scheduled_run("storage_scrub", 1000, 5000))
        self.assertEqual(SESSION.row(DAGUR_SCHEDULES, "storage_scrub")["last_run_epoch"], 5000)

    def test_two_instances_racing_from_the_same_read_produce_one_winner(self):
        # Both Catalysts read last_run_epoch = 1000, both compute that the job is due, and
        # both try to take the tick. Unconditionally, both writes land and Dagur runs the
        # same backup twice against the same volumes.
        self.given_job("storage_scrub", last_run_epoch=1000)
        first = catalyst.claim_scheduled_run("storage_scrub", 1000, 5000)
        second = catalyst.claim_scheduled_run("storage_scrub", 1000, 5000)
        self.assertTrue(first)
        self.assertFalse(second, "the second scheduler must not also submit the job")
        self.assertEqual(SESSION.row(DAGUR_SCHEDULES, "storage_scrub")["last_run_epoch"], 5000)

    def test_a_schedule_whose_clock_was_never_written_is_still_claimable_once(self):
        # `last_run_epoch` is null, not 0, for a schedule inserted without one. `IF
        # last_run_epoch = 0` would not match a null, so every such job would be refused
        # forever -- and a claim that defaulted the expected value to 0 would be blind.
        self.given_job("helios_update_check")
        self.assertNotIn("last_run_epoch", SESSION.row(DAGUR_SCHEDULES, "helios_update_check"))
        self.assertTrue(catalyst.claim_scheduled_run("helios_update_check", None, 5000))
        self.assertFalse(catalyst.claim_scheduled_run("helios_update_check", None, 5000))

    def test_daruk_being_unreachable_skips_the_tick_rather_than_running_it(self):
        # A tick that is skipped runs on the next pass ten seconds later. A tick that is
        # run twice cannot be taken back, so an unanswerable claim must fail closed.
        self.given_job("storage_scrub", last_run_epoch=1000)
        catalyst.DARUK_URL = "http://127.0.0.1:1"
        try:
            self.assertFalse(catalyst.claim_scheduled_run("storage_scrub", 1000, 5000))
        finally:
            catalyst.DARUK_URL = DARUK_TEST_URL
        self.assertEqual(SESSION.row(DAGUR_SCHEDULES, "storage_scrub")["last_run_epoch"], 1000)

    def test_the_claim_is_conditional_on_the_value_that_was_read(self):
        # Not merely "on some value": a claim conditioned on anything else would apply
        # against a row another scheduler had already advanced.
        self.given_job("storage_scrub", last_run_epoch=4000)
        self.assertFalse(catalyst.claim_scheduled_run("storage_scrub", 1000, 5000))
        self.assertEqual(SESSION.row(DAGUR_SCHEDULES, "storage_scrub")["last_run_epoch"], 4000)


class CatalystSchedulerLoopTests(ConditionalWriteTestCase):
    """The loop itself, run one pass at a time against the fake database."""

    def run_one_pass(self, now):
        """Run scheduler_thread_loop until its first sleep, returning what it submitted."""
        submitted = []
        statements = []

        def fake_run_cql_query(cql_query, *args, **kwargs):
            statements.append(cql_query)
            if cql_query.strip().lower().startswith("select"):
                rows = SESSION.store.get(DAGUR_SCHEDULES, {}).values()
                return 0, "\n".join(json.dumps(row) for row in rows), ""
            return 0, "", ""

        real_time = catalyst.time
        real_run_cql = catalyst.run_cql_query
        real_submit = catalyst.submit_task_to_memory
        real_leader = catalyst.is_zookeeper_leader
        catalyst.time = FakeClock(now)
        catalyst.run_cql_query = fake_run_cql_query
        catalyst.submit_task_to_memory = lambda service, task: submitted.append((service, task))
        catalyst.is_zookeeper_leader = lambda: True
        stderr, sys.stderr = sys.stderr, io.StringIO()
        try:
            catalyst.scheduler_thread_loop()
        except _StopLoop:
            pass
        finally:
            captured, sys.stderr = sys.stderr, stderr
            catalyst.time = real_time
            catalyst.run_cql_query = real_run_cql
            catalyst.submit_task_to_memory = real_submit
            catalyst.is_zookeeper_leader = real_leader
        self.assertEqual(captured.getvalue(), "", "the loop swallowed an exception")
        return submitted, statements

    def test_a_due_job_is_submitted_once_and_the_second_scheduler_submits_nothing(self):
        # Two Catalyst processes, one database. Both see the same due job in the same
        # second; only the one that wins the compare-and-swap may queue it for Dagur.
        self.given_job("storage_scrub", last_run_epoch=1000, interval_seconds=3600)
        first, _ = self.run_one_pass(now=99000)
        second, _ = self.run_one_pass(now=99000)
        self.assertEqual([task["payload"]["job_name"] for _service, task in first],
                         ["storage_scrub"])
        self.assertEqual(second, [], "the losing scheduler must not queue the job")

    def test_a_job_that_is_not_due_is_left_alone(self):
        self.given_job("storage_scrub", last_run_epoch=98000, interval_seconds=3600)
        submitted, _statements = self.run_one_pass(now=99000)
        self.assertEqual(submitted, [])
        self.assertEqual(SESSION.row(DAGUR_SCHEDULES, "storage_scrub")["last_run_epoch"], 98000)

    def test_a_disabled_job_is_never_claimed(self):
        self.given_job("db_compaction", last_run_epoch=1000, enabled=False)
        submitted, _statements = self.run_one_pass(now=99000)
        self.assertEqual(submitted, [])
        self.assertEqual(SESSION.row(DAGUR_SCHEDULES, "db_compaction")["last_run_epoch"], 1000)

    def test_a_null_clock_does_not_take_the_whole_pass_down_with_it(self):
        # `s.get("last_run_epoch", 0)` returns None for a column that exists and is null,
        # and `now - None` raised TypeError inside the loop's try -- so one such row cost
        # every other schedule that pass, silently.
        self.given_job("helios_update_check")
        self.given_job("storage_scrub", last_run_epoch=1000)
        submitted, _statements = self.run_one_pass(now=99000)
        self.assertEqual(
            sorted(task["payload"]["job_name"] for _service, task in submitted),
            ["helios_update_check", "storage_scrub"])

    def test_the_loop_no_longer_writes_the_clock_through_the_query_path(self):
        # The whole point: the only write to last_run_epoch is the compare-and-swap.
        source = inspect.getsource(catalyst.scheduler_thread_loop)
        self.assertNotIn("last_run_epoch =", source)
        self.assertIn("claim_scheduled_run", source)


# -- Mipha: failover must not clobber a VM that was already recovered ---------------------

class MiphaFailoverTests(ConditionalWriteTestCase):
    def test_a_vm_still_on_the_dead_host_is_released(self):
        self.given_vm("web-01", host_ip="10.0.0.9", state="Running")
        self.assertTrue(mipha.release_orphaned_vm("web-01", "10.0.0.9"))
        row = SESSION.row(VMS, "web-01")
        self.assertEqual(row["host_ip"], "")
        self.assertEqual(row["state"], "Stopped")

    def test_a_vm_already_recovered_elsewhere_is_not_clobbered(self):
        # The VM list was read while the guest was still on the dead host; by the time the
        # reset runs it has been started on a survivor. The unconditional write unplaced a
        # running VM, and the start task that followed booted a second copy of it against
        # the same DRBD device.
        self.given_vm("web-01", host_ip="10.0.0.2", state="Running")
        self.assertFalse(mipha.release_orphaned_vm("web-01", "10.0.0.9"))
        row = SESSION.row(VMS, "web-01")
        self.assertEqual(row["host_ip"], "10.0.0.2")
        self.assertEqual(row["state"], "Running")

    def test_a_refusal_stops_the_failover_from_starting_the_vm_again(self):
        # The return value is the gate on submitting the start task, so False has to mean
        # "not ours to recover" and never "carry on".
        self.given_vm("web-01", host_ip="10.0.0.2", state="Running")
        self.assertIs(mipha.release_orphaned_vm("web-01", "10.0.0.9"), False)

    def test_daruk_being_unreachable_does_not_release_and_does_not_start(self):
        # No cqlsh fallback on this path: a write that cannot be made conditional must not
        # be made. We no longer know who owns the VM, and guessing is how both sides of a
        # partition come to own the same guest.
        self.given_vm("web-01", host_ip="10.0.0.9", state="Running")
        mipha.DARUK_URL = "http://127.0.0.1:1"
        try:
            self.assertFalse(mipha.release_orphaned_vm("web-01", "10.0.0.9"))
        finally:
            mipha.DARUK_URL = DARUK_TEST_URL
        self.assertEqual(SESSION.row(VMS, "web-01")["host_ip"], "10.0.0.9")

    def test_a_vm_that_no_longer_exists_is_not_conjured_back(self):
        # `IF host_ip = '<dead ip>'` does not match an absent row, so a VM deleted during
        # the failover is refused rather than half-created.
        self.assertFalse(mipha.release_orphaned_vm("ghost", "10.0.0.9"))
        self.assertIsNone(SESSION.row(VMS, "ghost"))

    def test_the_failover_loop_no_longer_holds_a_blind_reset(self):
        # The HA control loop lives in mipha.main().
        for statement in cql_statements_in(mipha.main):
            self.assertNotIn("hydra.vms", statement,
                             "the failover loop still writes hydra.vms directly")
        self.assertIn("release_orphaned_vm", inspect.getsource(mipha.main))

    def test_marking_a_host_down_is_keyed_on_the_partition_key(self):
        # This was `UPDATE hydra.nodes SET status = 'DOWN' WHERE ip = ...`, and `ip` is a
        # plain column: Scylla rejected the statement outright ("Cannot execute this query
        # as it might involve data filtering") and nothing read the rc, so a dead host was
        # never actually marked DOWN and Vali kept scheduling onto it.
        for statement in cql_statements_in(mipha.main):
            if "hydra.nodes" in statement:
                self.assertNotIn("WHERE ip = ", statement)
        SESSION.put(NODES, "node09", hostname="node09", status="NORMAL", maintenance_mode=False)
        _status, body = call("/v1/node/maintenance", {
            "hostname": "node09", "status": "DOWN", "maintenance_mode": False,
            "expected_status": "NORMAL"})
        self.assertTrue(body["applied"])
        self.assertEqual(SESSION.row(NODES, "node09")["status"], "DOWN")

    def test_marking_a_host_down_does_not_drag_it_out_of_maintenance(self):
        # The failover decision was made before the operator touched the host. Conditioning
        # on the status this pass read means the later change wins.
        SESSION.put(NODES, "node09", hostname="node09", status="ENTERING_MAINTENANCE",
                    maintenance_mode=True)
        _status, body = call("/v1/node/maintenance", {
            "hostname": "node09", "status": "DOWN", "maintenance_mode": False,
            "expected_status": "NORMAL"})
        self.assertFalse(body["applied"])
        self.assertEqual(body["current"]["status"], "ENTERING_MAINTENANCE")


# -- Lanayru: the migration lock is not its to release ------------------------------------

class LanayruDeploymentTests(ConditionalWriteTestCase):
    """`deploy_lanayru_worker` with everything outside Hydra stubbed out."""

    def setUp(self):
        super().setUp()
        self.spark_commands = []
        self.linstor_commands = []
        self.tasks = []
        self._patched = {}
        self.patch(spectrum, "run_remote_spark",
                   lambda ip, cmd: (self.spark_commands.append((ip, cmd)), (0, "", ""))[1])
        self.patch(spectrum, "run_linstor_cmd",
                   lambda args: (self.linstor_commands.append(args), (0, "", ""))[1])
        self.patch(spectrum, "log_catalyst_task",
                   lambda *args, **kwargs: self.tasks.append((args, kwargs)))
        self.patch(spectrum, "get_cluster_nodes",
                   lambda: [{"ip": "10.0.0.1", "hostname": "node01"}])
        self.patch(spectrum, "get_catalyst_target_ip", lambda: "")
        self.patch(spectrum, "run_cql_query", self.record_cql)
        # ensure_schema would run the real schema against the real proxy; the tables the
        # deployment writes to are seeded by the fake session instead.
        self.patch(lanayru, "load_schema_module",
                   lambda: types.SimpleNamespace(ensure_schema=lambda execute, **kw: []))
        self.patch(lanayru, "time", FakeSleepless())
        self.cql_statements = []

    def tearDown(self):
        for (owner, name), value in self._patched.items():
            setattr(owner, name, value)
        super().tearDown()

    def patch(self, owner, name, value):
        self._patched.setdefault((owner, name), getattr(owner, name))
        setattr(owner, name, value)

    def record_cql(self, cql_query, *args, **kwargs):
        self.cql_statements.append(cql_query)
        return 0, "", ""

    def deploy(self, cluster_name="lke", control_nodes=1):
        lanayru.deploy_lanayru_worker(
            task_id="task-1", cluster_name=cluster_name, control_nodes=control_nodes,
            overlay_segment_id="vlan-0", created_at=0)

    def test_a_control_node_is_registered_with_the_columns_hydra_vms_actually_has(self):
        # The old INSERT named uuid, vcpus, ram, guest_ip, network_name and created_at --
        # none of which exist -- so Scylla rejected it and no Lanayru VM was ever recorded.
        self.deploy()
        row = SESSION.row(VMS, "lke-control-01")
        self.assertIsNotNone(row, "the control node was never registered")
        self.assertEqual(row["vcpu"], 2)
        self.assertEqual(row["memory"], 4096)
        self.assertEqual(row["host_ip"], "10.0.0.1")
        self.assertEqual(row["disks_list"], "lke-control-01-disk0")

    def test_the_migration_lock_column_is_never_written_by_a_deployment(self):
        # `status` is the migration lock, and 'running' is its *released* value. Lanayru
        # has never held that lock, so writing it released a lock belonging to whatever
        # live migration was in flight -- after which a second migration was free to start.
        self.deploy()
        row = SESSION.row(VMS, "lke-control-01")
        self.assertIsNone(row["status"])
        self.assertEqual(row["state"], "Running",
                         "the power state belongs in `state`, which is what was meant")

    def test_the_deployment_no_longer_writes_hydra_vms_as_statement_text(self):
        # Both writes -- the registration and the "it is running now" -- are typed
        # endpoints. Nothing in the deployment assembles CQL against hydra.vms any more,
        # so nothing in it can name `status`.
        for statement in cql_statements_in(lanayru.deploy_lanayru_worker):
            self.assertNotIn("hydra.vms", statement)

    def test_a_name_collision_is_refused_before_any_storage_is_built(self):
        # INSERT is an upsert in CQL. Unconditionally, deploying a cluster whose name
        # collided with a live VM reset that VM's placement onto this target host, and the
        # image copy that followed wrote over its disk.
        self.given_vm("lke-control-01", host_ip="10.0.0.9", state="Running", vcpu=8)
        self.deploy()
        row = SESSION.row(VMS, "lke-control-01")
        self.assertEqual(row["host_ip"], "10.0.0.9")
        self.assertEqual(row["vcpu"], 8)
        self.assertEqual(self.linstor_commands, [],
                         "storage was built for a VM whose name was already taken")
        self.assertEqual(self.tasks[-1][0][2], "failed")

    def test_a_migration_lock_held_by_somebody_else_survives_a_set_state(self):
        # The write Lanayru makes now is /v1/vm/set-state, whose statement does not name
        # `status` at all, so a migration in flight keeps its lock.
        self.given_vm("web-01", host_ip="10.0.0.1", state="Stopped",
                      status=daruk.MIGRATION_LOCK)
        _status, body = call("/v1/vm/set-state", {
            "name": "web-01", "state": "Running", "expected_host_ip": "10.0.0.1"})
        self.assertTrue(body["applied"])
        row = SESSION.row(VMS, "web-01")
        self.assertEqual(row["state"], "Running")
        self.assertEqual(row["status"], daruk.MIGRATION_LOCK,
                         "set-state released a migration lock it does not hold")

    def test_recording_the_running_state_is_conditional_on_the_placement(self):
        assignment = daruk.LWT_OPS["/v1/vm/set-state"]["cql"].split(" WHERE ")[0]
        self.assertNotIn("status", assignment)
        self.assertIn("IF host_ip = ?", daruk.LWT_OPS["/v1/vm/set-state"]["cql"])


class FakeSleepless:
    """`time` with sleeps removed, so a provisioner's pacing does not pace the test."""

    def sleep(self, seconds):
        return None

    def time(self):
        return 1_700_000_000.0


# -- run_cql_query refuses what it cannot report ------------------------------------------

class ConditionalStatementGuardTests(ConditionalWriteTestCase):
    """The guard is in every daemon that owns a copy of run_cql_query."""

    def modules(self):
        return (("catalyst", catalyst), ("mipha", mipha), ("dagur", dagur))

    def test_a_conditional_update_is_refused_rather_than_silently_mis_read(self):
        # Daruk's /query renders a rejected LWT as "False 10.10.102.41" with rc=0, which is
        # indistinguishable from a successful write. Running it and reading the answer is
        # the bug; refusing to run it is the fix.
        statement = ("UPDATE hydra.vms SET host_ip = '' WHERE name = 'web-01' "
                     "IF host_ip = '10.10.102.41';")
        for name, module in self.modules():
            with self.assertRaises(module.ConditionalStatementError, msg=name):
                module.run_cql_query(statement)

    def test_an_insert_if_not_exists_is_refused(self):
        statement = ("INSERT INTO hydra.cluster_locks (name, holder) "
                     "VALUES ('cluster-maintenance', 'node01') IF NOT EXISTS;")
        for name, module in self.modules():
            with self.assertRaises(module.ConditionalStatementError, msg=name):
                module.run_cql_query(statement)

    def test_a_conditional_delete_is_refused(self):
        statement = "DELETE FROM hydra.cluster_locks WHERE name = 'x' IF holder = 'node01';"
        for name, module in self.modules():
            with self.assertRaises(module.ConditionalStatementError, msg=name):
                module.run_cql_query(statement)

    def test_the_refusal_names_the_statement_and_points_somewhere(self):
        statement = "UPDATE hydra.vms SET state = 'X' WHERE name = 'web-01' IF host_ip = 'a';"
        with self.assertRaises(catalyst.ConditionalStatementError) as caught:
            catalyst.run_cql_query(statement)
        message = str(caught.exception)
        self.assertIn("hydra.vms", message)
        self.assertIn("/v1/", message)

    def test_ordinary_writes_are_not_conditional(self):
        for name, module in self.modules():
            for statement in (
                    "SELECT JSON * FROM hydra.dagur_schedules;",
                    "UPDATE hydra.vms SET state = 'Running' WHERE name = 'web-01';",
                    "INSERT INTO hydra.dagur_runs (job_name, status) VALUES ('a', 'b');",
                    "DELETE FROM hydra.vms WHERE name = 'web-01';"):
                self.assertFalse(module.is_conditional_cql(statement), f"{name}: {statement}")

    def test_ddl_is_not_a_compare_and_swap(self):
        # Every daemon applies the shared schema, and "CREATE TABLE IF NOT EXISTS" carries
        # nothing a caller needs to read. Refusing it would stop the daemons from starting.
        for name, module in self.modules():
            self.assertFalse(module.is_conditional_cql(
                "CREATE TABLE IF NOT EXISTS hydra.vms ( name text PRIMARY KEY );"), name)
            self.assertFalse(module.is_conditional_cql(
                "CREATE KEYSPACE IF NOT EXISTS hydra WITH replication = {'x': 1};"), name)

    def test_the_word_if_inside_a_quoted_value_is_not_a_condition(self):
        # Dagur writes the stdout of arbitrary jobs into hydra.dagur_runs. A guard that
        # matched the raw text would refuse a run record because a health check printed
        # "check if the volume is mounted", and the run would vanish instead.
        statements = (
            "INSERT INTO hydra.dagur_runs (job_name, output) "
            "VALUES ('scrub', 'check if the volume is mounted');",
            "UPDATE hydra.catalyst_tasks SET error_msg = 'failed: if you see this, retry' "
            "WHERE task_id = 1;",
            "INSERT INTO hydra.dagur_runs (job_name, output) "
            "VALUES ('scrub', 'it''s unclear if this applied');",
        )
        for name, module in self.modules():
            for statement in statements:
                self.assertFalse(module.is_conditional_cql(statement), f"{name}: {statement}")

    def test_a_condition_after_a_quoted_value_is_still_found(self):
        # ... and blanking the literals must not blank the clause that follows one.
        statement = ("UPDATE hydra.dagur_schedules SET command = 'run --if-needed' "
                     "WHERE job_name = 'scrub' IF command = 'old';")
        for name, module in self.modules():
            self.assertTrue(module.is_conditional_cql(statement), name)

    def test_the_schema_bootstrap_keeps_its_escape_hatch(self):
        # helios_schema takes its lock with IF NOT EXISTS and reads the [applied] verdict
        # itself, and it runs before the schema exists -- so Daruk cannot have a typed
        # endpoint for it. It is handed the unguarded executor on purpose.
        self.assertIn("run_conditional_cql_query",
                      inspect.getsource(catalyst.init_db_schema))
        self.assertFalse(hasattr(catalyst.run_conditional_cql_query, "__wrapped__"))

    def test_no_daemon_still_builds_a_conditional_statement_as_text(self):
        # A caller that ran a conditional statement through the query path and then read
        # the answer is the original bug. None of these daemons builds one any more, so
        # the guard has nothing legitimate to refuse -- it is there for the next change.
        for name, module in (("catalyst", catalyst), ("mipha", mipha),
                             ("dagur", dagur), ("lanayru", lanayru)):
            for statement in cql_statements_in(module):
                self.assertFalse(catalyst.is_conditional_cql(statement),
                                 f"{name} still builds: {statement}")


# -- the endpoints themselves --------------------------------------------------------------

class ScheduleEndpointTests(ConditionalWriteTestCase):
    def test_both_schedule_tables_have_a_claim_endpoint(self):
        # mimir_schedules carries the same read-decide-write, in three other daemons. The
        # endpoint exists so those fixes are a call, not a second mechanism.
        self.assertIn("/v1/schedule/claim-job", daruk.LWT_OPS)
        self.assertIn("/v1/schedule/claim-check", daruk.LWT_OPS)

    def test_the_table_is_not_a_caller_parameter(self):
        # A table name that arrives in a request is caller-supplied CQL, which is what the
        # typed endpoints exist to prevent.
        for path in ("/v1/schedule/claim-job", "/v1/schedule/claim-check"):
            self.assertNotIn("table", daruk.LWT_OPS[path]["params"])

    def test_the_expected_clock_has_no_default(self):
        # A default would match every schedule whose clock is null and turn the claim back
        # into the blind write it replaced.
        for path in ("/v1/schedule/claim-job", "/v1/schedule/claim-check"):
            spec = daruk.LWT_OPS[path]["params"]["expected_last_run_epoch"]
            self.assertTrue(spec["required"])
            self.assertTrue(spec["nullable"])
            self.assertNotIn("default", spec)

    def test_a_check_schedule_claim_is_refused_twice(self):
        SESSION.put(MIMIR_SCHEDULES, "hourly_checks",
                    schedule_name="hourly_checks", last_run_epoch=1000)
        _s1, first = call("/v1/schedule/claim-check", {
            "schedule_name": "hourly_checks", "last_run_epoch": 5000,
            "expected_last_run_epoch": 1000})
        _s2, second = call("/v1/schedule/claim-check", {
            "schedule_name": "hourly_checks", "last_run_epoch": 6000,
            "expected_last_run_epoch": 1000})
        self.assertTrue(first["applied"])
        self.assertFalse(second["applied"])
        self.assertEqual(second["current"]["last_run_epoch"], 5000)

    def test_a_boolean_clock_is_refused(self):
        # isinstance(True, int) is True in Python; an unguarded bind would claim the tick
        # for epoch 1.
        status, body = call("/v1/schedule/claim-job", {
            "job_name": "scrub", "last_run_epoch": True, "expected_last_run_epoch": 0})
        self.assertEqual(status, 400)
        self.assertIn("last_run_epoch", body["error"])

    def test_a_misspelt_expected_clock_is_refused_rather_than_defaulted(self):
        status, body = call("/v1/schedule/claim-job", {
            "job_name": "scrub", "last_run_epoch": 5000, "expcted_last_run_epoch": 1000})
        self.assertEqual(status, 400)
        self.assertIn("expcted_last_run_epoch", body["error"])


if __name__ == "__main__":
    unittest.main()
