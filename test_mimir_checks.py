#!/usr/bin/env python3
"""Tests for the diagnostic checks proposed in docs/audit_findings.md section 13.

Four checks, each of which answers a question no other check here answers:

  * `watchdog_daemon_status` -- spark-daemon keeps answering its API whether or not the
    thread that restarts failed services is alive, so `systemctl is-active spark-daemon`
    is green on a host where nothing self-heals.
  * `sidon_latency_check` -- every other storage check asks the daemon a question
    and believes the answer. This one times the question.
  * `drs_storage_capacity_check` -- vali's storage gate returns a hard-coded 999999 MiB
    when it cannot parse the pool listing, which is larger than any VM disk, so an
    unparseable listing does not make the gate refuse: it makes the gate approve.
  * `migration_lock_status` -- a migration lock left held refuses every later migration
    and delete of that VM; a migration running *without* the lock leaves storage
    dual-primary open with nothing preventing a second one.

Each check is tested three ways, because these are the three ways a health check goes
wrong: it must fire on the fault, stay quiet on the healthy case, and answer WARN -- never
PASS -- when it cannot determine the answer. That last one is not hypothetical here: a
certificate check in this codebase answered PASS when it could not parse a date.

Run with:  python -m unittest test_mimir_checks
"""

import importlib.util
import io
import os
import re
import unittest
from importlib.machinery import SourceFileLoader

HERE = os.path.dirname(os.path.abspath(__file__))


def load_script(name, path):
    """Load a deployed CLI, which has no .py suffix.

    An explicit SourceFileLoader is required: importlib infers a loader from the file
    extension, and `spec_from_file_location` on an extensionless path returns None.
    """
    loader = SourceFileLoader(name, os.path.join(HERE, path))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


runner = load_script("mcli_runner_checks", "mcli-runner")


def healthy_watchdog_facts(**overrides):
    facts = {
        "unit": {"LoadState": "loaded", "ActiveState": "active", "SubState": "running",
                 "InvocationID": "abc", "MainPID": "1234"},
        "started_ago": 172800.0,
        "maintenance": False,
        "cluster_json": True,
        "lock_present": False, "lock_age": None, "lock_predates_start": None,
        "cluster_state": "started", "cluster_state_detail": "started",
        "journal_covers_start": True, "journal_detail": "",
        "watchdog_announced": True, "watchdog_errors": 0,
        "supervised": ["hydra-db", "vali"], "down_units": [], "unreadable_units": [],
    }
    facts.update(overrides)
    return facts


def healthy_gate_facts(**overrides):
    facts = {
        "local_ip": "10.0.0.1", "node_name": "node-a",
        "gate_value": 299760, "gate_error": "",
        "pools": [{"name": "default-pool", "node": "node-a", "kind": "LVM_THIN",
                   "free_kib": 314318732, "total_kib": 314413056}],
        "linstor_error": "", "provisioned_kib": 26308608,
    }
    facts.update(overrides)
    return facts


def healthy_lock_facts(**overrides):
    facts = {
        "vms": [{"name": "web01", "host_ip": "10.0.0.1", "state": "Running",
                 "status": None}],
        "vms_error": "", "tasks": None, "tasks_error": "", "tasks_scanned": False,
        "libvirt_reachable": True, "hypervisor_present": True, "domains": ["web01"],
        "domains_error": "", "migrating_domains": [],
    }
    facts.update(overrides)
    return facts


def runner_source():
    """mcli-runner's text, for the checks asserted structurally rather than by call."""
    return io.open(os.path.join(HERE, "mcli-runner"), encoding="utf-8").read()


class WatchdogTests(unittest.TestCase):
    """spark-daemon's watchdog publishes no heartbeat, so it is inferred. Carefully."""

    def test_a_healthy_host_passes(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts())
        self.assertEqual(status, "PASS", message)
        self.assertIn("hydra-db", message)

    def test_a_dead_spark_daemon_fails_and_says_so(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            unit={"LoadState": "loaded", "ActiveState": "failed", "SubState": "failed"}))
        self.assertEqual(status, "FAIL", message)
        self.assertIn("systemctl start spark-daemon", message)

    def test_a_lock_held_since_before_startup_fails(self):
        # The autostart thread returns immediately when the lock exists at startup and
        # never retries, so the watchdog loop is dead for the life of the process.
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            lock_present=True, lock_age=200000.0, lock_predates_start=True))
        self.assertEqual(status, "FAIL", message)
        self.assertIn("systemctl restart spark-daemon", message)

    def test_a_lock_taken_after_startup_is_not_a_fault(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            lock_present=True, lock_age=30.0, lock_predates_start=False))
        self.assertEqual(status, "PASS", message)
        self.assertIn("by design", message)

    def test_a_lock_held_for_hours_fails(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            lock_present=True, lock_age=runner.CLUSTER_OP_LOCK_FAIL + 60,
            lock_predates_start=False))
        self.assertEqual(status, "FAIL", message)
        self.assertIn(runner.CLUSTER_OP_LOCK, message)

    def test_a_retained_journal_without_the_announcement_fails(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            journal_covers_start=True, watchdog_announced=False))
        self.assertEqual(status, "FAIL", message)
        self.assertIn("before reaching the loop", message)

    def test_a_rotated_journal_is_not_treated_as_evidence(self):
        # The reference cluster: spark-daemon has been up for days and journald has
        # vacuumed its startup. Absence of the announcement proves nothing there, and a
        # check that read it as proof would call a healthy node broken.
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            journal_covers_start=False, watchdog_announced=False))
        self.assertEqual(status, "PASS", message)
        self.assertIn("no longer covers", message)

    def test_a_long_down_supervised_unit_fails(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            down_units=[{"name": "vali", "active_state": "failed",
                         "sub_state": "failed", "down_for": 900.0}]))
        self.assertEqual(status, "FAIL", message)
        self.assertIn("vali", message)
        self.assertIn("systemctl status vali", message)

    def test_a_briefly_down_unit_does_not_fire(self):
        # Inside the window the watchdog needs to notice and restart it, a down unit is
        # the watchdog working, not the watchdog broken.
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            down_units=[{"name": "vali", "active_state": "inactive",
                         "sub_state": "dead", "down_for": 5.0}]))
        self.assertEqual(status, "PASS", message)

    def test_a_stopped_cluster_is_not_a_watchdog_failure(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            cluster_state="stopped",
            down_units=[{"name": "vali", "active_state": "inactive",
                         "sub_state": "dead", "down_for": 90000.0}]))
        self.assertEqual(status, "PASS", message)
        self.assertIn("by design", message)

    def test_unreadable_systemctl_warns_rather_than_passes(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts(unit={}))
        self.assertEqual(status, "WARN", message)

    def test_an_unreadable_journal_warns(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            journal_covers_start=None, journal_detail="No journal files were found"))
        self.assertEqual(status, "WARN", message)

    def test_an_unreadable_cluster_state_warns(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            cluster_state=None, cluster_state_detail="zkCli.sh exited 1"))
        self.assertEqual(status, "WARN", message)

    def test_unreadable_units_warn(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            unreadable_units=["mipha"]))
        self.assertEqual(status, "WARN", message)
        self.assertIn("mipha", message)

    def test_findings_inside_the_startup_grace_are_downgraded(self):
        # The autostart sequence polls with sleeps; a node 20 seconds into it has not
        # necessarily reached the loop, so a FAIL there would fire on every reboot.
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            started_ago=20.0, journal_covers_start=True, watchdog_announced=False))
        self.assertEqual(status, "WARN", message)
        self.assertIn("not yet", message)

    def test_watchdog_exceptions_warn(self):
        status, message = runner.classify_watchdog(healthy_watchdog_facts(
            watchdog_errors=7))
        self.assertEqual(status, "WARN", message)
        self.assertIn("7", message)

    def test_maintenance_uses_the_smaller_supervised_set(self):
        # In maintenance spark-daemon supervises only consensus and storage; reporting
        # the compute services as unrestarted there would fire on every window.
        self.assertEqual(runner.WATCHDOG_MAINTENANCE_SERVICES,
                         ["zookeeper", "hydra-db", "sidon"])
        for name in ("spectrum", "vali", "catalyst"):
            self.assertNotIn(name, runner.WATCHDOG_MAINTENANCE_SERVICES)

    def test_the_supervised_list_matches_the_loop_it_describes(self):
        # This check is about the watchdog's own behaviour, so a unit the loop does not
        # touch must not appear: reporting zookeeper here would blame the watchdog for
        # something it was never asked to restart.
        for name in ("zookeeper", "libvirtd", "slate", "hylia"):
            self.assertNotIn(name, runner.WATCHDOG_SERVICES)
        for name in ("hydra-db", "spectrum", "vali", "catalyst", "mipha"):
            self.assertIn(name, runner.WATCHDOG_SERVICES)


class SidonLatencyTests(unittest.TestCase):
    """Every other storage check believes the answer; this one times the question.

    Rewritten from a check that timed `linstor node list` against a controller. There is
    no controller now, and the equivalent -- the local daemon's control socket -- is what
    every operation on this node goes through, so a slow one stalls attach, detach, drain
    and migration alike.
    """

    def test_the_thresholds_are_ordered_and_bounded(self):
        self.assertLess(runner.SIDON_LATENCY_WARN, runner.SIDON_LATENCY_FAIL)
        # The timeout has to exceed the fail threshold, or a call that should be reported
        # as "slow" is reported as "did not answer", which reads as a dead daemon.
        self.assertGreater(runner.SIDON_LATENCY_TIMEOUT, runner.SIDON_LATENCY_FAIL)

    def test_the_check_pings_rather_than_doing_work(self):
        # A ping costs the daemon nothing, so what is measured is the daemon's ability to
        # answer at all -- not how long some particular operation happens to take.
        source = runner_source()
        block = source[source.index('update_current_check(category, "sidon_latency_check")'):]
        block = block[:block.index('results["sidon_latency_check"]')]
        self.assertIn("ping", block)
        self.assertIn("control.sock", block)
        self.assertIn("SIDON_LATENCY_TIMEOUT", block)

    def test_a_timeout_is_a_failure_not_a_hang(self):
        # The whole reason this check exists is that a blocked storage control plane
        # stalls everything queued behind it. Waiting forever to find that out would make
        # the diagnostic run the next thing stalled.
        source = runner_source()
        block = source[source.index('update_current_check(category, "sidon_latency_check")'):]
        block = block[:block.index('results["sidon_latency_check"]')]
        self.assertIn('status = "FAIL"', block)


class DrsStorageGateTests(unittest.TestCase):
    """The gate's failure mode is approval, not refusal, so silence is not safety."""

    @staticmethod
    def facts(gate_value=150056, available=157345660928, total=160982630400, **over):
        base = {"local_ip": "10.0.0.1", "gate_value": gate_value, "gate_error": "",
                "capacity": {"ok": True, "total_bytes": total,
                             "available_bytes": available, "egroup_count": 12},
                "capacity_error": ""}
        base.update(over)
        return base

    def test_a_working_gate_with_headroom_passes(self):
        status, message = runner.classify_drs_storage_gate(self.facts())
        self.assertEqual(status, "PASS", message)

    def test_the_fail_open_sentinel_is_caught(self):
        # The original defect: the gate reported 999999 MiB whenever it could not parse a
        # listing, which is larger than any VM disk, so it refused nothing.
        status, message = runner.classify_drs_storage_gate(self.facts(gate_value=999999))
        self.assertEqual(status, "FAIL", message)
        self.assertIn("999999", message)

    def test_any_over_reading_gate_fails_not_just_the_known_sentinel(self):
        # Hard-coding 999999 would go quiet the moment the fallback value changed.
        status, message = runner.classify_drs_storage_gate(self.facts(gate_value=4000000))
        self.assertEqual(status, "FAIL", message)

    def test_a_gate_that_cannot_read_free_space_warns_rather_than_passes(self):
        # None is the correct answer to "I cannot tell", and vali refuses migrations on
        # it -- safe. But a gate that can never answer is a gate nobody is checking.
        status, message = runner.classify_drs_storage_gate(self.facts(gate_value=None))
        self.assertEqual(status, "WARN", message)
        self.assertIn("None", message)

    def test_an_unreadable_extent_store_warns_rather_than_passes(self):
        status, message = runner.classify_drs_storage_gate(
            self.facts(capacity=None, capacity_error="sidon did not answer"))
        self.assertEqual(status, "WARN", message)
        self.assertIn("not the same as it having room", message)

    def test_a_gate_that_could_not_be_exercised_warns(self):
        status, message = runner.classify_drs_storage_gate(
            self.facts(gate_error="vali is not deployed on this node"))
        self.assertEqual(status, "WARN", message)

    def test_a_gate_disagreeing_with_the_filesystem_warns(self):
        # Either it refuses migrations that would fit, or it approves ones that will not.
        status, message = runner.classify_drs_storage_gate(self.facts(gate_value=10))
        self.assertEqual(status, "WARN", message)

    def test_small_drift_is_not_a_disagreement(self):
        # Free space moves between the two reads. A check that fired on every byte of
        # drift would fire constantly and be ignored.
        status, _message = runner.classify_drs_storage_gate(self.facts(gate_value=150000))
        self.assertEqual(status, "PASS")


class MigrationLockTests(unittest.TestCase):
    def test_a_quiet_cluster_passes(self):
        status, message = runner.classify_migration_locks(healthy_lock_facts())
        self.assertEqual(status, "PASS", message)

    def test_an_orphaned_lock_fails_and_names_the_release(self):
        status, message = runner.classify_migration_locks(healthy_lock_facts(
            vms=[{"name": "web01", "host_ip": "10.0.0.1", "state": "Running",
                  "status": "migrating"}],
            tasks=[], tasks_scanned=True))
        self.assertEqual(status, "FAIL", message)
        self.assertIn("migrate-unlock", message)
        self.assertIn("web01", message)

    def test_a_live_migration_with_its_lock_held_passes(self):
        status, message = runner.classify_migration_locks(healthy_lock_facts(
            vms=[{"name": "web01", "host_ip": "10.0.0.1", "state": "Running",
                  "status": "migrating"}],
            tasks=[{"task_id": "t-1", "service": "vali", "action": "migrate",
                    "status": "processing", "payload": '{"vm_name": "web01"}',
                    "created_at": "2026-08-21 20:58:49.309Z"}],
            tasks_scanned=True,
            migrating_domains=[{"name": "web01", "job_type": "Unbounded",
                                "operation": "Outgoing migration"}]))
        # The task's created_at is in the past, so this asserts the age path too.
        self.assertIn(status, ("PASS", "WARN"), message)
        self.assertNotEqual(status, "FAIL", message)

    def test_a_migration_running_without_the_lock_fails(self):
        # The dangerous one: the disk can be claimed and nothing would refuse a second
        # migration of the same disk.
        status, message = runner.classify_migration_locks(healthy_lock_facts(
            migrating_domains=[{"name": "web01", "job_type": "Unbounded",
                                "operation": "Outgoing migration"}]))
        self.assertEqual(status, "FAIL", message)
        self.assertIn("dual-primary", message)

    def test_a_migration_of_an_unknown_vm_fails(self):
        status, message = runner.classify_migration_locks(healthy_lock_facts(
            migrating_domains=[{"name": "ghost", "job_type": "Unbounded",
                                "operation": "Incoming migration"}]))
        self.assertEqual(status, "FAIL", message)
        self.assertIn("ghost", message)

    def test_two_migrations_of_one_vm_fail(self):
        status, message = runner.classify_migration_locks(healthy_lock_facts(
            vms=[{"name": "web01", "host_ip": "10.0.0.1", "state": "Running",
                  "status": "migrating"}],
            tasks=[{"task_id": "t-1", "action": "migrate", "status": "processing",
                    "payload": '{"vm_name": "web01"}', "created_at": 1},
                   {"task_id": "t-2", "action": "migrate", "status": "pending",
                    "payload": '{"vm_name": "web01"}', "created_at": 2}],
            tasks_scanned=True))
        self.assertEqual(status, "FAIL", message)
        self.assertIn("t-2", message)

    def test_a_stuck_migration_warns(self):
        import time as _time
        old_ms = int((_time.time() - runner.MIGRATION_STUCK_AFTER * 3) * 1000)
        status, message = runner.classify_migration_locks(healthy_lock_facts(
            vms=[{"name": "web01", "host_ip": "10.0.0.1", "state": "Running",
                  "status": "migrating"}],
            tasks=[{"task_id": "t-1", "action": "migrate", "status": "processing",
                    "payload": '{"vm_name": "web01"}', "created_at": old_ms}],
            tasks_scanned=True))
        self.assertEqual(status, "WARN", message)
        self.assertIn("stuck", message)

    def test_an_unreadable_vm_table_warns_rather_than_passes(self):
        status, message = runner.classify_migration_locks(healthy_lock_facts(
            vms=None, vms_error="cqlsh exited 1"))
        self.assertEqual(status, "WARN", message)
        self.assertNotEqual(status, "PASS")

    def test_a_lock_with_an_unreadable_task_table_warns(self):
        status, message = runner.classify_migration_locks(healthy_lock_facts(
            vms=[{"name": "web01", "host_ip": "10.0.0.1", "state": "Running",
                  "status": "migrating"}],
            tasks=None, tasks_error="cqlsh exited 1", tasks_scanned=True))
        self.assertEqual(status, "WARN", message)

    def test_an_uninspectable_hypervisor_warns(self):
        status, message = runner.classify_migration_locks(healthy_lock_facts(
            libvirt_reachable=False, hypervisor_present=True, domains=None,
            domains_error="failed to connect to the hypervisor"))
        self.assertEqual(status, "WARN", message)

    def test_no_hypervisor_at_all_is_not_an_unknown(self):
        # With no libvirt daemon and no socket, no live migration can be running here, so
        # the local half of the audit is satisfied rather than merely unchecked.
        status, message = runner.classify_migration_locks(healthy_lock_facts(
            libvirt_reachable=False, hypervisor_present=False, domains=None,
            domains_error="failed to connect"))
        self.assertEqual(status, "PASS", message)

    def test_an_unrecognised_lock_value_warns(self):
        status, message = runner.classify_migration_locks(healthy_lock_facts(
            vms=[{"name": "web01", "host_ip": "10.0.0.1", "state": "Running",
                  "status": "cloning"}]))
        self.assertEqual(status, "WARN", message)
        self.assertIn("cloning", message)

    def test_the_lock_values_match_daruks(self):
        # These are daruk's constants. A different value here would make the check audit
        # a lock that does not exist.
        daruk = read("daruk.py")
        self.assertIn('MIGRATION_LOCK = "%s"' % runner.MIGRATION_LOCK_VALUE, daruk)
        self.assertIn('UNLOCKED_STATUS = "%s"' % runner.MIGRATION_UNLOCKED_VALUE, daruk)

    def test_the_task_table_is_only_scanned_when_something_claims_a_migration(self):
        # A full scan of a table with 30-day retention, run hourly on every node, to
        # confirm that nothing is migrating is the unbounded read this codebase has had
        # to remove elsewhere.
        calls = []

        def fake_run_cqlsh(cql):
            calls.append(cql)
            if "hydra.vms" in cql:
                return 0, '{"name": "web01", "host_ip": "", "state": "Stopped", ' \
                          '"status": null}', ""
            return 0, "", ""

        original_cqlsh = runner.run_cqlsh
        original_cmd = runner.run_cmd
        runner.run_cqlsh = fake_run_cqlsh
        runner.run_cmd = lambda cmd: (0, "", "")
        try:
            facts = runner.collect_migration_lock_facts()
        finally:
            runner.run_cqlsh = original_cqlsh
            runner.run_cmd = original_cmd
        self.assertFalse(facts["tasks_scanned"])
        self.assertEqual([c for c in calls if "catalyst_tasks" in c], [])


class HelperTests(unittest.TestCase):
    def test_a_cql_timestamp_string_parses(self):
        # `SELECT JSON` renders a timestamp column as a string, not a number. int() on it
        # raises, and a check that swallows the exception stops examining the rows it was
        # written to examine -- which is how a stuck-task check goes permanently quiet.
        parsed = runner.parse_cql_timestamp_ms("2026-08-21 20:58:49.309Z")
        self.assertEqual(parsed, 1787345929309)

    def test_a_cql_timestamp_without_millis_parses(self):
        self.assertEqual(runner.parse_cql_timestamp_ms("2026-08-21 20:58:49Z"),
                         1787345929000)

    def test_an_epoch_number_still_parses(self):
        self.assertEqual(runner.parse_cql_timestamp_ms(1787345929309), 1787345929309)
        self.assertEqual(runner.parse_cql_timestamp_ms("1787345929309"), 1787345929309)

    def test_an_unparseable_timestamp_is_none_not_zero(self):
        # None means "unknown"; zero would mean "1970", which reads as infinitely stuck.
        self.assertIsNone(runner.parse_cql_timestamp_ms("not a timestamp"))
        self.assertIsNone(runner.parse_cql_timestamp_ms(None))
        self.assertIsNone(runner.parse_cql_timestamp_ms(""))

    def test_monotonic_conversion_handles_missing_values(self):
        self.assertIsNone(runner.seconds_since_monotonic(None, 100.0))
        self.assertIsNone(runner.seconds_since_monotonic("0", 100.0))
        self.assertIsNone(runner.seconds_since_monotonic("nonsense", 100.0))
        self.assertAlmostEqual(runner.seconds_since_monotonic("60000000", 100.0), 40.0)

    def test_an_unreadable_uptime_yields_none(self):
        # `uptime=None` means "read it from the system", so this has to stub the reader
        # rather than pass None and hope. Passing None asserted a platform accident: on a
        # host with /proc/uptime the read succeeds and a number comes back, so the test
        # passed only where the reader could not work and failed in CI.
        saved = runner.monotonic_uptime
        runner.monotonic_uptime = lambda: None
        try:
            self.assertIsNone(runner.seconds_since_monotonic("1000000"))
        finally:
            runner.monotonic_uptime = saved

    def test_migration_tasks_only_match_in_flight_migrations_of_that_vm(self):
        tasks = [
            {"task_id": "a", "action": "migrate", "status": "completed",
             "payload": '{"vm_name": "web01"}', "created_at": 1},
            {"task_id": "b", "action": "start", "status": "processing",
             "payload": '{"vm_name": "web01"}', "created_at": 1},
            {"task_id": "c", "action": "migrate", "status": "processing",
             "payload": '{"vm_name": "db01"}', "created_at": 1},
            {"task_id": "d", "action": "migrate", "status": "pending",
             "payload": '{"vm_name": "web01"}', "created_at": 1},
        ]
        matched = runner.migration_tasks_for(tasks, "web01")
        self.assertEqual([t["task_id"] for t in matched], ["d"])

    def test_a_slow_call_is_abandoned_rather_than_hanging_the_run(self):
        # mcli allows a remote node 90 seconds for the whole runner. vali's storage gate
        # carries its own 120-second timeout, so calling it unbounded would lose every
        # check on that node rather than just this one.
        import time as _time

        value, error = runner.call_with_timeout(lambda: _time.sleep(5) or 1, 0.05)
        self.assertIsNone(value)
        self.assertIn("did not return", error)

    def test_a_raising_call_reports_the_error_not_a_value(self):
        def boom():
            raise RuntimeError("no controller")

        value, error = runner.call_with_timeout(boom, 5)
        self.assertIsNone(value)
        self.assertIn("no controller", error)

    def test_a_normal_call_returns_its_value(self):
        value, error = runner.call_with_timeout(lambda arg: arg * 2, 5, 21)
        self.assertEqual(value, 42)
        self.assertEqual(error, "")

    def test_a_gate_that_does_not_answer_warns_rather_than_passes(self):
        """vali present but its reader returning None must not read as healthy.

        The gate refuses on None, which is safe -- but a gate that can never answer is a
        gate nobody is checking, and a PASS here would be the check going quiet about
        exactly the condition it exists to catch.
        """
        facts = {"local_ip": "10.0.0.1", "gate_value": None, "gate_error": "",
                 "capacity": {"ok": True, "total_bytes": 1 << 40,
                              "available_bytes": 1 << 39, "egroup_count": 4},
                 "capacity_error": ""}
        status, message = runner.classify_drs_storage_gate(facts)
        self.assertEqual(status, "WARN", message)

    def test_a_report_prints_its_evidence_even_when_passing(self):
        # "PASS" with nothing behind it is indistinguishable from a check that did not
        # run, which is how a hollow check hides.
        message = runner.compose_report("headline", [], [], ["saw this", "and this"])
        self.assertIn("saw this", message)
        self.assertNotIn("headline", message)

    def test_a_report_leads_with_the_failures(self):
        message = runner.compose_report("headline", ["broken"], ["odd"], ["seen"])
        self.assertTrue(message.startswith("headline:\n- broken"))
        self.assertIn("Also noted", message)


class StuckTaskAgeTests(unittest.TestCase):
    """`stuck_tasks_check` answered PASS on every cluster it has ever run on."""

    def test_the_int_cast_on_a_timestamp_column_is_gone(self):
        # `created_at` is a CQL timestamp, rendered by SELECT JSON as
        # '2026-08-21 20:58:49.309Z'. int() raised ValueError on every row, the bare
        # except swallowed it, and the check reported "no stuck tasks" while three had
        # been pending for over an hour on the reference cluster.
        source = read("mcli-runner")
        self.assertNotIn("int(created_at)", source)
        self.assertIn("parse_cql_timestamp_ms(created_at)", source)

    def test_an_unreadable_age_does_not_read_as_not_stuck(self):
        source = read("mcli-runner")
        stuck = source.split('update_current_check(category, "stuck_tasks_check")', 1)[1]
        stuck = stuck.split('results["stuck_tasks_check"] = {\n            "status": "PASS"', 1)[0]
        self.assertIn("unreadable", stuck,
                      "a task whose age cannot be read is silently treated as fresh")


class WiringTests(unittest.TestCase):
    """A check nobody runs, or that lands in the wrong partition, is not a check."""

    def setUp(self):
        self.mcli = read("mcli")
        self.runner_source = read("mcli-runner")
        mapping = re.search(r"CHECK_ID_TO_FUNC = \{(.*?)\n\}", self.mcli, re.S)
        self.assertIsNotNone(mapping)
        self.map = dict(re.findall(r'"([a-z0-9_.-]+)"\s*:\s*"([^"]+)"', mapping.group(1)))
        self.new_checks = ["watchdog_daemon_status", "drs_storage_capacity_check",
                           "migration_lock_status", "sidon_latency_check"]

    def test_each_new_check_has_a_dotted_category(self):
        # A check missing from the map falls back to the invoked category, which puts it
        # in a different partition depending on how it was run. An undotted one would be
        # removed by the legacy-partition cleanup as if it were an invocation scope.
        for name in self.new_checks:
            self.assertIn(name, self.map)
            self.assertIn(".", self.map[name], name)

    def test_each_new_check_is_produced_by_the_runner(self):
        for name in self.new_checks:
            self.assertIn('results["%s"]' % name, self.runner_source, name)

    def test_each_new_check_is_listed_by_health_checks_list(self):
        for name in self.new_checks:
            self.assertIn(name, self.mcli.split("def show_list", 1)[1], name)

    def test_every_check_is_listed_in_the_scope_that_runs_it(self):
        """mcli's per-scope lists must agree with which runner function defines a check.

        The lists drive the progress display, and a name in the wrong one means the bar
        for `mcli health_checks storage` never advances past a check it is running. The
        same drift, in the console's copy of these lists, is what section 13's own
        grouping bug came from.
        """
        bounds = {}
        for func in ("check_services", "check_storage", "check_hardware"):
            match = re.search(r"^def %s\(" % func, self.runner_source, re.M)
            self.assertIsNotNone(match, func)
            bounds[func] = match.start()
        order = sorted(bounds, key=lambda f: bounds[f])
        limits = []
        for index, func in enumerate(order):
            end = bounds[order[index + 1]] if index + 1 < len(order) \
                else len(self.runner_source)
            limits.append((func, bounds[func], end))

        def attribute(name, position):
            for func, start, end in limits:
                if start <= position < end:
                    defined_in[name] = func
                    return

        defined_in = {}
        for match in re.finditer(r'results\["([a-z0-9_.-]+)"\]', self.runner_source):
            attribute(match.group(1), match.start())
        # The per-service loop writes results[f"{svc}_status"]. A scan for quoted keys
        # cannot see those, which is how hylia_status came to be listed nowhere at all.
        for match in re.finditer(r'results\[f"\{(\w+)\}_status"\]', self.runner_source):
            for names in re.findall(r"^\s*%ss?\s*=\s*\[(.*?)\]" % match.group(1),
                                    self.runner_source[:match.start()], re.S | re.M):
                for svc in re.findall(r'"([a-z0-9_.-]+)"', names):
                    attribute(svc + "_status", match.start())
        if 'svcs.append("urbosa")' in self.runner_source:
            attribute("urbosa_status", self.runner_source.index('svcs.append("urbosa")'))

        scope_for_func = {"check_services": "services", "check_storage": "storage",
                          "check_hardware": "hardware"}
        listed = {}
        for scope in ("services", "storage", "hardware"):
            block = re.search(r'"%s": \[(.*?)\]' % scope, self.mcli, re.S)
            self.assertIsNotNone(block, scope)
            for name in re.findall(r'"([a-z0-9_.-]+)"', block.group(1)):
                listed.setdefault(name, []).append(scope)

        misplaced = []
        for name, func in sorted(defined_in.items()):
            expected = scope_for_func[func]
            if listed.get(name) != [expected]:
                misplaced.append("%s: defined in %s, listed under %s"
                                 % (name, func, listed.get(name) or "nothing"))
        self.assertEqual(misplaced, [])


if __name__ == "__main__":
    unittest.main()
