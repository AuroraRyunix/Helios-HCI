#!/usr/bin/env python3
"""Tests for host fencing, the failover gate, and self-fencing.

The failure these exist to prevent is the one that turns HA into data loss: Mipha
restarts a VM on a healthy host while the original is still running on the failed one and
still has the same DRBD device open. Two qemu processes on one raw block device destroy
the filesystem inside it within seconds, and no later repair recovers it.

The guard is "fence first, then fail over", and every assertion below corresponds to a
way that guard used to be hollow:

  * a fence command whose every clause ended in `|| true`, so its exit status was 0
    whatever happened, read as proof that no guest was left running;
  * a fence result the caller discarded entirely, and a fence that was only attempted
    when the host still answered ping -- so a host that had gone silent was assumed dead
    on no evidence at all;
  * `ipmitool chassis power off` returning 0 for a command a BMC accepted and never
    carried out;
  * a storage "fence" that looked at DRBD's per-device quorum flag, which reads true both
    when the majority is held and when quorum is switched off altogether;
  * a self-fence that fires on one slow probe and evacuates a working host, or that kills
    running guests because libvirtd crashed underneath them.

Run with:  python -m unittest test_fencing
"""

import importlib.util
import io
import json
import json
import os
import sys
import tempfile
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_module(alias, filename):
    spec = importlib.util.spec_from_file_location(alias, os.path.join(HERE, filename))
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


mipha = load_module("mipha_fencing_under_test", "mipha.py")
daemon = load_module("spark_daemon_fencing_under_test", "spark_daemon_decoded.py")


def base_config(**overrides):
    """The built-in defaults, with the top-level keys a test wants changed."""
    return mipha._merge_defaults(mipha.DEFAULT_FENCING_CONFIG, overrides)


def probe(libvirt="ok", storage="ok", unserviceable=(), detail=None):
    return {"libvirt": libvirt, "storage": storage,
            "unserviceable": list(unserviceable), "detail": detail or {}}


def unserviceable(cause="drain-failed", resource="vm-disk0"):
    return {"resource": resource, "cause": cause,
            "detail": f"{resource}: drain failed, journal is not emptying"}


def vdisk_rows(*rows):
    """A `SELECT JSON` result the way run_cql_query hands it back."""
    return "\n".join(json.dumps(row) for row in rows)


def owned(vdisk_id="vm-disk0", owner="node-b", epoch=4):
    return {"vdisk_id": vdisk_id, "owner": owner, "epoch": epoch}


class FenceTestCase(unittest.TestCase):
    """Patch/restore helper, and a captured stdout so the suite stays readable."""

    def setUp(self):
        super().setUp()
        self._patched = {}
        self._stdout = sys.stdout
        sys.stdout = io.StringIO()
        self.addCleanup(self.restore_stdout)
        mipha.FENCE_LEDGER.clear()
        self.patch(mipha, "host_is_in_maintenance", lambda: False)

    def restore_stdout(self):
        self.output = sys.stdout.getvalue()
        sys.stdout = self._stdout

    def tearDown(self):
        for (owner, name), value in self._patched.items():
            setattr(owner, name, value)
        mipha.FENCE_LEDGER.clear()
        super().tearDown()

    def patch(self, owner, name, value):
        self._patched.setdefault((owner, name), getattr(owner, name))
        setattr(owner, name, value)


# -- the gate ----------------------------------------------------------------------------

class FailoverGateTests(FenceTestCase):
    """A fence that cannot be confirmed does not permit a failover."""

    def unconfirmed(self):
        result = mipha.FenceResult("node-b", "10.0.0.2")
        result.record(mipha.FENCE_METHOD_SPARK, False, "spark-daemon did not answer")
        result.record(mipha.FENCE_METHOD_BMC, False, "no BMC entry for node-b")
        result.record(mipha.FENCE_METHOD_STORAGE, False, "quorum is off")
        return result

    def test_an_unconfirmed_fence_blocks_the_failover(self):
        allowed, why = mipha.failover_permitted(self.unconfirmed(), base_config())
        self.assertFalse(allowed)
        # The reason has to name every rung that was tried, because the operator's next
        # action -- power the box off, buy a BMC, turn on quorum -- depends on which.
        self.assertIn("spark", why)
        self.assertIn("bmc", why)
        self.assertIn("quorum is off", why)

    def test_a_confirmed_fence_permits_the_failover(self):
        result = mipha.FenceResult("node-b", "10.0.0.2")
        result.record(mipha.FENCE_METHOD_SPARK, False, "spark-daemon did not answer")
        result.record(mipha.FENCE_METHOD_BMC, True, "the BMC reports the chassis off")
        allowed, why = mipha.failover_permitted(result, base_config())
        self.assertTrue(allowed)
        self.assertIn(mipha.FENCE_METHOD_BMC, why)
        self.assertEqual(result.method, mipha.FENCE_METHOD_BMC)

    def test_an_operator_can_opt_into_failing_over_unfenced(self):
        # Deliberately available and deliberately not the default: some clusters would
        # rather risk it than lose HA entirely, but they have to say so.
        allowed, why = mipha.failover_permitted(
            self.unconfirmed(), base_config(unconfirmed_fence_policy="failover"))
        self.assertTrue(allowed)
        self.assertIn("not proven", why)

    def test_the_default_with_no_configuration_at_all_is_to_block(self):
        config, warnings = mipha.load_fencing_config(
            os.path.join(tempfile.gettempdir(), "helios-fencing-does-not-exist.json"))
        self.assertEqual(config["unconfirmed_fence_policy"], "block")
        self.assertEqual(warnings, [])
        allowed, _why = mipha.failover_permitted(self.unconfirmed(), config)
        self.assertFalse(allowed)

    def test_an_unrecognised_policy_is_read_as_block(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "fencing.json")
            with open(path, "w") as handle:
                json.dump({"unconfirmed_fence_policy": "yolo"}, handle)
            self.patch(mipha, "fencing_config_is_private", lambda _p: True)
            config, warnings = mipha.load_fencing_config(path)
        self.assertEqual(config["unconfirmed_fence_policy"], "block")
        self.assertTrue(any("not understood" in w for w in warnings))


# -- the in-band (spark) rung --------------------------------------------------------------

class SparkFenceTests(FenceTestCase):

    def test_a_daemon_that_reports_the_fence_did_not_take_is_not_confirmed(self):
        self.patch(mipha, "run_mtls_spark_api_full",
                   lambda *a, **k: (409, {"fenced": False, "qemu_pids": [4211],
                                          "detail": "the fence did not take -- guest "
                                                    "processes still running: [4211]"}, ""))
        confirmed, detail = mipha.spark_fence_host("10.0.0.2")
        self.assertFalse(confirmed)
        self.assertIn("4211", detail)

    def test_a_daemon_that_reports_nothing_is_left_is_confirmed(self):
        self.patch(mipha, "run_mtls_spark_api_full",
                   lambda *a, **k: (200, {"fenced": True, "detail": "nothing remains"}, ""))
        confirmed, detail = mipha.spark_fence_host("10.0.0.2")
        self.assertTrue(confirmed)
        self.assertIn("nothing remains", detail)

    def test_a_request_that_never_lands_is_not_a_fence(self):
        # The whole point of an out-of-band path: a host wedged enough to need fencing is
        # exactly the host that will not answer the request to fence itself.
        self.patch(mipha, "run_mtls_spark_api_full",
                   lambda *a, **k: (0, {}, "timed out"))
        confirmed, detail = mipha.spark_fence_host("10.0.0.2")
        self.assertFalse(confirmed)
        self.assertIn("timed out", detail)

    def test_a_legacy_daemon_falls_back_and_still_verifies(self):
        calls = []

        def fake_remote(ip, command):
            calls.append(command)
            if command.startswith("pgrep"):
                return 1, "", ""       # pgrep exits 1 when nothing matched
            return 0, "", ""

        self.patch(mipha, "run_mtls_spark_api_full", lambda *a, **k: (404, {}, "HTTP 404"))
        self.patch(mipha, "run_remote_spark", fake_remote)
        self.patch(mipha, "run_mtls_spark_api", lambda *a, **k: (0, {"attached": []}, ""))
        confirmed, detail = mipha.spark_fence_host("10.0.0.2")
        self.assertTrue(confirmed)
        self.assertIn("serving no vdisk", detail)
        self.assertTrue(any(c.startswith("systemctl stop libvirtd") for c in calls))

    def test_the_legacy_commands_exit_status_is_not_evidence(self):
        # This is the original bug, written down. `... || true; pkill -9 qemu || true`
        # exits 0 on a host where the pkill did nothing at all, and rc == 0 was the only
        # thing the old fence checked.
        def fake_remote(ip, command):
            if command.startswith("pgrep"):
                return 0, "4211 /usr/libexec/qemu-kvm -name guest=web01\n", ""
            return 0, "", ""

        self.patch(mipha, "run_mtls_spark_api_full", lambda *a, **k: (404, {}, "HTTP 404"))
        self.patch(mipha, "run_remote_spark", fake_remote)
        confirmed, detail = mipha.spark_fence_host("10.0.0.2")
        self.assertFalse(confirmed)
        self.assertIn("still running", detail)

    def test_a_host_that_still_holds_its_disks_is_not_fenced(self):
        self.patch(mipha, "run_mtls_spark_api_full", lambda *a, **k: (404, {}, "HTTP 404"))
        self.patch(mipha, "run_remote_spark",
                   lambda ip, command: (1, "", "") if command.startswith("pgrep") else (0, "", ""))
        self.patch(mipha, "run_mtls_spark_api",
                   lambda *a, **k: (0, {"attached": [{"vdisk_id": "vm-disk0"}]}, ""))
        confirmed, detail = mipha.spark_fence_host("10.0.0.2")
        self.assertFalse(confirmed)
        self.assertIn("vm-disk0", detail)

    def test_storage_state_that_cannot_be_read_is_not_confirmed(self):
        self.patch(mipha, "run_mtls_spark_api_full", lambda *a, **k: (404, {}, "HTTP 404"))
        self.patch(mipha, "run_remote_spark",
                   lambda ip, command: (1, "", "") if command.startswith("pgrep") else (0, "", ""))
        self.patch(mipha, "run_mtls_spark_api", lambda *a, **k: (-1, "", "connection reset"))
        confirmed, detail = mipha.spark_fence_host("10.0.0.2")
        self.assertFalse(confirmed)
        self.assertIn("not proven", detail)


# -- the out-of-band (BMC) rung ------------------------------------------------------------

class BmcFenceTests(FenceTestCase):

    def entry_config(self, **entry):
        record = {"address": "10.9.0.2", "username": "helios-fence",
                  "password": "s3cret", "power_off_timeout_seconds": 6}
        record.update(entry)
        return base_config(bmc={"defaults": {"interface": "lanplus"},
                                "hosts": {"node-b": record}})

    def test_a_host_with_no_bmc_entry_is_not_powered_off(self):
        confirmed, detail = mipha.bmc_fence_host("node-b", "10.0.0.2", base_config())
        self.assertFalse(confirmed)
        self.assertIn("not configured", detail)

    def test_a_missing_ipmitool_is_not_a_power_off(self):
        self.patch(mipha.shutil, "which", lambda _name: None)
        confirmed, detail = mipha.bmc_fence_host("node-b", "10.0.0.2", self.entry_config())
        self.assertFalse(confirmed)
        self.assertIn("ipmitool is not installed", detail)

    def test_a_bmc_that_never_reports_off_is_not_confirmed(self):
        # ipmitool returns 0 for a chassis command the BMC accepted. Accepting is not
        # doing, and this is the case the whole rung exists to distinguish.
        self.patch(mipha.shutil, "which", lambda _name: "/usr/bin/ipmitool")
        self.patch(mipha, "time", types.SimpleNamespace(
            time=lambda: next(self.clock), sleep=lambda _s: None))
        self.clock = iter([0.0] + [float(n) for n in range(1, 40)])

        def fake_argv(argv, timeout=45):
            if argv[-1] == "status":
                return 0, "Chassis Power is on\n", ""
            return 0, "Chassis Power Control: Down/Off\n", ""

        self.patch(mipha, "run_argv_local", fake_argv)
        confirmed, detail = mipha.bmc_fence_host("node-b", "10.0.0.2", self.entry_config())
        self.assertFalse(confirmed)
        self.assertIn("never reported the chassis off", detail)

    def test_a_bmc_that_reports_off_is_confirmed(self):
        self.patch(mipha.shutil, "which", lambda _name: "/usr/bin/ipmitool")
        seen = []

        def fake_argv(argv, timeout=45):
            seen.append(argv)
            if argv[-1] == "status":
                return 0, "Chassis Power is off\n", ""
            return 0, "Chassis Power Control: Down/Off\n", ""

        self.patch(mipha, "run_argv_local", fake_argv)
        confirmed, detail = mipha.bmc_fence_host("node-b", "10.0.0.2", self.entry_config())
        self.assertTrue(confirmed)
        self.assertIn("powered off", detail)
        self.assertIn(["chassis", "power", "off"], [argv[-3:] for argv in seen])

    def test_an_ipmitool_that_cannot_reach_the_bmc_is_not_a_power_off(self):
        self.patch(mipha.shutil, "which", lambda _name: "/usr/bin/ipmitool")
        self.patch(mipha, "run_argv_local",
                   lambda argv, timeout=45: (1, "", "Error: Unable to establish IPMI v2 "
                                                     "/ RMCP+ session"))
        confirmed, detail = mipha.bmc_fence_host("node-b", "10.0.0.2", self.entry_config())
        self.assertFalse(confirmed)
        self.assertIn("Unable to establish", detail)

    def test_the_password_never_reaches_the_command_line(self):
        # /proc/<pid>/cmdline is world readable, so a password on argv is a password every
        # account on the host can read while the fence runs.
        self.patch(mipha.shutil, "which", lambda _name: "/usr/bin/ipmitool")
        observed = {}

        def fake_argv(argv, timeout=45):
            observed["argv"] = argv
            observed["env"] = os.environ.get("IPMI_PASSWORD")
            return 0, "Chassis Power is off\n", ""

        self.patch(mipha, "run_argv_local", fake_argv)
        mipha.bmc_fence_host("node-b", "10.0.0.2", self.entry_config())
        self.assertNotIn("s3cret", " ".join(observed["argv"]))
        self.assertIn("-E", observed["argv"])
        self.assertEqual(observed["env"], "s3cret")
        # ... and the process environment is put back afterwards.
        self.assertIsNone(os.environ.get("IPMI_PASSWORD"))

    def test_a_world_readable_config_does_not_supply_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "fencing.json")
            with open(path, "w") as handle:
                json.dump({"bmc": {"hosts": {"node-b": {"address": "10.9.0.2",
                                                        "username": "u",
                                                        "password": "p"}}}}, handle)
            self.patch(mipha, "fencing_config_is_private", lambda _p: False)
            config, warnings = mipha.load_fencing_config(path)
        self.assertIsNone(mipha.bmc_entry_for("node-b", "10.0.0.2", config))
        self.assertTrue(any("readable by accounts other than root" in w for w in warnings))

    def test_a_root_only_config_does_supply_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "fencing.json")
            with open(path, "w") as handle:
                json.dump({"bmc": {"defaults": {"interface": "lanplus"},
                                   "hosts": {"node-b": {"address": "10.9.0.2",
                                                        "username": "u",
                                                        "password": "p"}}}}, handle)
            self.patch(mipha, "fencing_config_is_private", lambda _p: True)
            config, warnings = mipha.load_fencing_config(path)
        entry = mipha.bmc_entry_for("node-b", "10.0.0.2", config)
        self.assertEqual(entry["address"], "10.9.0.2")
        # Per-host values inherit the defaults block rather than replacing it.
        self.assertEqual(entry["interface"], "lanplus")
        self.assertEqual(warnings, [])

    def test_a_password_file_that_anyone_can_read_is_refused(self):
        self.patch(mipha, "fencing_config_is_private", lambda _p: False)
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write("s3cret\n")
            path = handle.name
        self.addCleanup(os.remove, path)
        password, error = mipha._bmc_password({"password_file": path})
        self.assertIsNone(password)
        self.assertIn("chmod 600", error)


# -- the storage rung ----------------------------------------------------------------------

class StorageFenceTests(FenceTestCase):
    """The rung that used to be an inference and is now an action.

    With DRBD this read quorum and argued that a host which could not see a majority was
    already failing its own I/O. It now raises the epoch on every vdisk the dead host
    owns, which every replica then enforces without the dead host's cooperation.
    """

    HOSTS = [{"hostname": "node-a", "ip": "10.0.0.1"},
             {"hostname": "node-b", "ip": "10.0.0.2"},
             {"hostname": "node-c", "ip": "10.0.0.3"}]

    def setUp(self):
        super().setUp()
        self.claims = []
        self.patch(mipha, "sidon_module", lambda: types.SimpleNamespace())
        self.patch(mipha, "local_hostname", lambda: "node-a")

    def arrange_map(self, rows, rc=0):
        self.patch(mipha, "run_cql_query",
                   lambda *a, **k: (rc, vdisk_rows(*rows), ""))

    def arrange_claims(self, responder):
        def fake_claim(ip, path, payload, method="POST"):
            self.claims.append(payload)
            return responder(payload)

        self.patch(mipha, "run_mtls_spark_api_full", fake_claim)

    def test_raising_the_epoch_on_every_owned_vdisk_is_a_fence(self):
        self.arrange_map([owned("vm-disk0", "node-b", 4), owned("vm-disk1", "node-b", 1)])
        self.arrange_claims(lambda payload: (0, {"applied": True}, ""))
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(confirmed, detail)
        self.assertIn("2 vdisk(s)", detail)
        # Conditioned on the owner and epoch that were read, and set one past them.
        self.assertEqual([c["expected_epoch"] for c in self.claims], [4, 1])
        self.assertEqual([c["epoch"] for c in self.claims], [5, 2])
        self.assertEqual({c["expected_owner"] for c in self.claims}, {"node-b"})

    def test_a_host_that_owns_nothing_is_already_fenced(self):
        self.arrange_map([owned("vm-disk0", "node-c", 2)])
        self.arrange_claims(lambda payload: (0, {"applied": True}, ""))
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(confirmed)
        self.assertIn("owns no vdisk", detail)
        self.assertEqual(self.claims, [], "nothing should have been claimed")

    def test_a_claim_lost_to_a_third_host_still_fences_the_dead_one(self):
        # Somebody else got there first. The dead host is off the vdisk either way, which
        # is the only thing this rung is asserting.
        self.arrange_map([owned("vm-disk0", "node-b", 4)])
        self.arrange_claims(
            lambda payload: (0, {"applied": False,
                                 "current": {"owner": "node-c", "epoch": 5}}, ""))
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(confirmed, detail)

    def test_a_claim_that_leaves_the_dead_host_owning_it_is_not_a_fence(self):
        self.arrange_map([owned("vm-disk0", "node-b", 4)])
        self.arrange_claims(
            lambda payload: (0, {"applied": False,
                                 "current": {"owner": "node-b", "epoch": 4}}, ""))
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(confirmed)
        self.assertIn("vm-disk0", detail)

    def test_one_unfenced_vdisk_out_of_two_blocks_the_assertion(self):
        self.arrange_map([owned("vm-disk0", "node-b", 4), owned("vm-disk1", "node-b", 1)])
        self.arrange_claims(
            lambda payload: (0, {"applied": True}, "") if payload["vdisk_id"] == "vm-disk0"
            else (0, {"applied": False, "current": {"owner": "node-b", "epoch": 1}}, ""))
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(confirmed, "a partial fence is not a fence")
        self.assertIn("vm-disk1", detail)

    def test_a_map_that_cannot_be_read_is_not_a_fence(self):
        self.arrange_map([], rc=1)
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(confirmed)
        self.assertIn("could not be read", detail)

    def test_a_host_matched_by_ip_is_fenced_too(self):
        # hydra.dfs_vdisks records whatever the owner called itself. Both spellings have
        # to fence, or a cluster that mixes them silently fences nothing.
        self.arrange_map([owned("vm-disk0", "10.0.0.2", 7)])
        self.arrange_claims(lambda payload: (0, {"applied": True}, ""))
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(confirmed, detail)
        self.assertEqual(self.claims[0]["expected_owner"], "10.0.0.2")

    def test_without_helios_sidon_nothing_can_be_fenced(self):
        self.patch(mipha, "sidon_module", lambda: None)
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(confirmed)
        self.assertIn("helios_sidon", detail)


# -- the ladder and its ledger -------------------------------------------------------------

class FenceLadderTests(FenceTestCase):

    HOSTS = [{"hostname": "node-a", "ip": "10.0.0.1"},
             {"hostname": "node-b", "ip": "10.0.0.2"}]

    def setUp(self):
        super().setUp()
        self.spark_calls = []
        self.bmc_calls = []
        self.storage_calls = []
        self.patch(mipha, "load_fencing_config", lambda path=None: (base_config(), []))

    def arrange(self, spark=(False, "no answer"), bmc=(False, "no BMC entry"),
                storage=(False, "hydra is unreachable")):
        def fake_spark(ip):
            self.spark_calls.append(ip)
            return spark

        def fake_bmc(hostname, ip, config):
            self.bmc_calls.append(hostname)
            return bmc

        def fake_storage(hostname, ip, hosts):
            self.storage_calls.append(hostname)
            return storage

        self.patch(mipha, "spark_fence_host", fake_spark)
        self.patch(mipha, "bmc_fence_host", fake_bmc)
        self.patch(mipha, "storage_fence_assert", fake_storage)

    def test_the_ladder_stops_at_the_first_rung_that_confirms(self):
        # Storage is first now: it is the only rung that works unconditionally, so a
        # confirmed epoch bump means the other two are never needed for safety.
        self.arrange(storage=(True, "2 vdisk(s) moved past node-b's epoch"))
        result = mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(result.confirmed)
        self.assertEqual(result.method, mipha.FENCE_METHOD_STORAGE)
        self.assertEqual(self.spark_calls, [])
        self.assertEqual(self.bmc_calls, [])

    def test_the_ladder_escalates_when_a_rung_cannot_confirm(self):
        # Storage could not confirm -- which now means Hydra itself is unreachable -- so
        # the hygiene rungs are tried in turn.
        self.arrange(spark=(True, "nothing is left running"))
        result = mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(result.confirmed)
        self.assertEqual(result.method, mipha.FENCE_METHOD_SPARK)
        self.assertEqual([step["method"] for step in result.steps],
                         [mipha.FENCE_METHOD_STORAGE, mipha.FENCE_METHOD_SPARK])

    def test_every_rung_failing_is_an_unconfirmed_fence(self):
        self.arrange()
        result = mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(result.confirmed)
        self.assertEqual(result.method, mipha.FENCE_METHOD_NONE)

    def test_a_rung_that_raises_is_a_failure_not_a_crash(self):
        def explode(ip):
            raise RuntimeError("ipmitool segfaulted")

        self.arrange(bmc=(True, "the BMC reports the chassis off"))
        self.patch(mipha, "spark_fence_host", explode)
        result = mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(result.confirmed)
        # storage could not confirm, spark raised, BMC carried it -- and the raise is
        # recorded as a failed rung rather than taking the whole ladder down.
        self.assertIn("RuntimeError", result.steps[1]["detail"])

    def test_a_host_already_fenced_is_not_fenced_again(self):
        self.arrange(bmc=(True, "the BMC reports the chassis off"))
        first = mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        second = mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(first.confirmed and second.confirmed)
        self.assertEqual(second.method, mipha.FENCE_METHOD_BMC)
        # Powering off a chassis that is already off, or killing guests that are already
        # dead, costs a failover the one thing it does not have.
        self.assertEqual(self.bmc_calls, ["node-b"])
        self.assertEqual(second.steps[0]["method"], "ledger")

    def test_a_fence_that_failed_is_retried_rather_than_remembered(self):
        self.arrange()
        mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertEqual(self.spark_calls, ["10.0.0.2", "10.0.0.2"])

    def test_a_host_that_comes_back_forgets_its_fence(self):
        self.arrange(spark=(True, "nothing is left running"))
        mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(mipha.clear_fence_record("10.0.0.2"))
        mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertEqual(self.spark_calls, ["10.0.0.2", "10.0.0.2"])

    def test_a_self_fenced_host_is_confirmed_without_being_touched(self):
        self.arrange()
        result = mipha.fence_host("node-b", "10.0.0.2", self.HOSTS,
                                  db_status=mipha.NODE_STATUS_FENCED)
        self.assertTrue(result.confirmed)
        self.assertEqual(result.method, mipha.FENCE_METHOD_SELF)
        self.assertEqual(self.spark_calls, [])


# -- self-fencing --------------------------------------------------------------------------

class SelfFenceDecisionTests(FenceTestCase):

    HOSTS = [{"hostname": "node-a", "ip": "10.0.0.1"},
             {"hostname": "node-b", "ip": "10.0.0.2"},
             {"hostname": "node-c", "ip": "10.0.0.3"}]

    def setUp(self):
        super().setUp()
        self.counters = {}
        self.patch(mipha, "healthy_peer_exists", lambda hosts=None: True)

    def decide(self, current, uptime=10000, config=None, hosts=None):
        return mipha.self_fence_decide(current, self.counters, config or base_config(),
                                       self.HOSTS if hosts is None else hosts, uptime)

    def test_a_single_transient_failure_does_not_self_fence(self):
        action, _reason = self.decide(probe(unserviceable=[unserviceable()]))
        self.assertEqual(action, "none")
        self.assertEqual(self.counters["unserviceable"], 1)

    def test_two_failures_still_do_not_self_fence(self):
        for _ in range(2):
            action, _reason = self.decide(probe(unserviceable=[unserviceable()]))
        self.assertEqual(action, "none")

    def test_one_good_pass_wipes_the_history(self):
        # The blip case in full: a failure, a recovery, then another failure must not add
        # up to a fence.
        self.decide(probe(unserviceable=[unserviceable()]))
        self.decide(probe(unserviceable=[unserviceable()]))
        action, _reason = self.decide(probe())
        self.assertEqual(action, "none")
        self.assertEqual(self.counters["unserviceable"], 0)
        action, _reason = self.decide(probe(unserviceable=[unserviceable()]))
        self.assertEqual(action, "none")

    def test_three_consecutive_drain_failures_self_fence(self):
        for _ in range(3):
            action, reason = self.decide(probe(unserviceable=[unserviceable()]))
        self.assertEqual(action, "fence")
        self.assertIn("cannot serve I/O", reason)

    def test_a_failed_drain_with_no_peer_only_quarantines(self):
        # DRBD had a shortcut here: losing quorum *was* the majority test, so a node that
        # lost it knew some other set of nodes held it and could fence itself without
        # checking whether any peer was reachable. A failed drain proves nothing of the
        # kind -- the journal may be un-drainable because the extent store is full, which
        # says nothing about whether anywhere else can run these guests. So the peer check
        # now applies to every local storage fault, and with no peer the honest outcome is
        # to keep the guests running here rather than kill them for nothing.
        self.patch(mipha, "healthy_peer_exists", lambda hosts=None: False)
        for _ in range(3):
            action, reason = self.decide(probe(unserviceable=[unserviceable()]))
        self.assertEqual(action, "quarantine")
        self.assertIn("no peer is answering", reason)

    def test_a_local_storage_fault_with_nowhere_to_go_only_quarantines(self):
        self.patch(mipha, "healthy_peer_exists", lambda hosts=None: False)
        for _ in range(3):
            action, reason = self.decide(
                probe(unserviceable=[unserviceable(cause="no-data")]))
        self.assertEqual(action, "quarantine")
        self.assertIn("no peer is answering", reason)

    def test_a_local_storage_fault_with_a_peer_fences(self):
        for _ in range(3):
            action, _reason = self.decide(
                probe(unserviceable=[unserviceable(cause="no-data")]))
        self.assertEqual(action, "fence")

    def test_dead_libvirt_quarantines_instead_of_killing_working_guests(self):
        # qemu keeps running when libvirtd dies, so the guests are fine and only their
        # management is lost. Destroying them would be a self-inflicted outage, and
        # failing them over while they are still writing would be the corruption.
        for _ in range(5):
            action, reason = self.decide(
                probe(libvirt="failed", detail={"libvirt": "connection refused"}))
        self.assertEqual(action, "quarantine")
        self.assertIn("libvirt", reason)

    def test_a_probe_that_cannot_reach_a_verdict_never_escalates(self):
        for _ in range(10):
            action, _reason = self.decide(probe(libvirt="unknown", storage="unknown"))
        self.assertEqual(action, "none")

    def test_a_single_node_cluster_never_self_fences(self):
        # There is nowhere for the guests to be restarted, so a fence here is a pure
        # outage with no safety benefit whatsoever.
        for _ in range(5):
            action, reason = self.decide(probe(unserviceable=[unserviceable()]),
                                         hosts=[{"hostname": "solo", "ip": "10.0.0.1"}])
        self.assertEqual(action, "none")
        self.assertIn("single-node", reason)

    def test_a_host_in_maintenance_is_exempt(self):
        self.patch(mipha, "host_is_in_maintenance", lambda: True)
        for _ in range(5):
            action, reason = self.decide(probe(unserviceable=[unserviceable()]))
        self.assertEqual(action, "none")
        self.assertIn("maintenance", reason)

    def test_nothing_self_fences_during_the_startup_grace_period(self):
        for _ in range(5):
            action, reason = self.decide(probe(unserviceable=[unserviceable()]), uptime=5)
        self.assertEqual(action, "none")
        self.assertIn("grace", reason)

    def test_self_fencing_can_be_switched_off(self):
        config = base_config(self_fence={"enabled": False})
        for _ in range(5):
            action, _reason = self.decide(probe(unserviceable=[unserviceable()]),
                                          config=config)
        self.assertEqual(action, "none")

    def test_recovery_leaves_quarantine(self):
        for _ in range(3):
            self.decide(probe(storage="failed"))
        action, _reason = self.decide(probe())
        self.assertEqual(action, "none")
        self.assertEqual(self.counters["storage"], 0)


class SelfFenceAnnouncementTests(FenceTestCase):
    """FENCED is a claim the leader acts on, so only a fence that took may publish it."""

    def setUp(self):
        super().setUp()
        self.patch(mipha, "SELF_FENCE_STATE", dict(mipha.SELF_FENCE_STATE))

    def test_a_fence_that_took_publishes_fenced(self):
        mipha.SELF_FENCE_STATE["report"] = {"fenced": True}
        self.assertEqual(mipha.self_fence_announcement(), mipha.NODE_STATUS_FENCED)

    def test_a_fence_that_did_not_take_publishes_degraded(self):
        # Publishing FENCED here would make the leader skip its own ladder and restart
        # guests that are demonstrably still running on this host.
        mipha.SELF_FENCE_STATE["report"] = {"fenced": False,
                                            "qemu_pids": [4211],
                                            "detail": "guest processes still running"}
        self.assertEqual(mipha.self_fence_announcement(), mipha.NODE_STATUS_DEGRADED)

    def test_a_fence_with_no_report_at_all_publishes_degraded(self):
        mipha.SELF_FENCE_STATE["report"] = {}
        self.assertEqual(mipha.self_fence_announcement(), mipha.NODE_STATUS_DEGRADED)


class SelfFenceProbeTests(FenceTestCase):
    """`probe_local_health` has to distinguish "broken" from "could not tell"."""

    def arrange_sidon(self, attached=None, raises=None):
        def list_attached(timeout=15):
            if raises:
                raise raises
            return {"attached": attached or []}

        self.patch(mipha, "sidon_module",
                   lambda: types.SimpleNamespace(list_attached=list_attached))

    def test_sidon_not_answering_is_a_storage_failure(self):
        self.arrange_sidon(raises=RuntimeError("control socket is unreachable"))
        self.patch(mipha.shutil, "which", lambda _n: None)
        current = mipha.probe_local_health()
        self.assertEqual(current["storage"], "failed")
        self.assertIn("unreachable", current["detail"]["storage"])

    def test_a_node_without_helios_sidon_is_unknown_not_failed(self):
        # A node that has not been updated yet must not read as a node whose storage has
        # died -- "unknown" is the verdict that never escalates to killing guests.
        self.patch(mipha, "sidon_module", lambda: None)
        self.patch(mipha.shutil, "which", lambda _n: None)
        current = mipha.probe_local_health()
        self.assertEqual(current["storage"], "unknown")

    def test_a_missing_virsh_is_unknown_rather_than_failed(self):
        self.arrange_sidon()
        self.patch(mipha.shutil, "which", lambda _n: None)
        current = mipha.probe_local_health()
        self.assertEqual(current["libvirt"], "unknown")

    def test_a_degraded_vdisk_is_collected_as_unserviceable(self):
        self.arrange_sidon(attached=[
            {"vdisk_id": "vm-disk0", "degraded": "drain failed: No space left on device"},
            {"vdisk_id": "vm-disk1", "degraded": None},
        ])
        self.patch(mipha.shutil, "which", lambda _n: "/usr/bin/virsh")
        self.patch(mipha, "run_argv_local", lambda argv, timeout=45: (0, "", ""))
        current = mipha.probe_local_health()
        self.assertEqual(current["storage"], "ok")
        self.assertEqual([u["resource"] for u in current["unserviceable"]], ["vm-disk0"])
        self.assertEqual(current["unserviceable"][0]["cause"], "drain-failed")

    def test_serving_healthy_vdisks_is_ok_with_nothing_unserviceable(self):
        self.arrange_sidon(attached=[{"vdisk_id": "vm-disk0", "degraded": None}])
        self.patch(mipha.shutil, "which", lambda _n: "/usr/bin/virsh")
        self.patch(mipha, "run_argv_local", lambda argv, timeout=45: (0, "", ""))
        current = mipha.probe_local_health()
        self.assertEqual(current["storage"], "ok")
        self.assertEqual(current["unserviceable"], [])


# -- the daemon side: a fence that reads back what it did ----------------------------------

class DaemonFenceTests(FenceTestCase):
    """spark-daemon's local fence: stop the guests, release the vdisks, read it back."""

    def setUp(self):
        super().setUp()
        self.patch(daemon, "write_fence_marker", lambda reason, report: True)
        self.patch(daemon, "time", types.SimpleNamespace(sleep=lambda _s: None,
                                                         time=lambda: 0.0))
        self.commands = []
        self.detached = []

    def arrange(self, running_domains=(), qemu_after=(), attached_before=(),
                attached_after=None, detach_fails=()):
        states = [list(attached_before),
                  list(attached_before if attached_after is None else attached_after)]

        def fake_argv(argv, timeout=45):
            self.commands.append(argv)
            if argv[:2] == ["virsh", "-c"] and "list" in argv:
                return 0, "\n".join(running_domains), ""
            if argv[:2] == ["virsh", "-c"] and "destroy" in argv:
                return 0, "Domain destroyed", ""
            if argv[0] == "mountpoint":
                return 1, "", ""
            return 0, "", ""

        def list_attached(timeout=15):
            current = states.pop(0) if states else []
            return {"attached": [{"vdisk_id": v} for v in current]}

        def detach(vdisk_id, timeout=30):
            self.detached.append(vdisk_id)
            if vdisk_id in detach_fails:
                raise RuntimeError("vdisk %s refused to detach" % vdisk_id)
            return {"vdisk_id": vdisk_id}

        self.patch(daemon, "run_argv", fake_argv)
        self.patch(daemon, "qemu_process_ids", lambda: list(qemu_after))
        self.patch(daemon, "load_sidon_module",
                   lambda: types.SimpleNamespace(list_attached=list_attached, detach=detach))

    def test_a_fence_is_confirmed_only_when_nothing_is_left(self):
        self.arrange(running_domains=["web01"], qemu_after=[],
                     attached_before=["vm-disk0"], attached_after=[])
        report = daemon.fence_this_host()
        self.assertTrue(report["fenced"], report)
        self.assertEqual(self.detached, ["vm-disk0"])
        self.assertEqual(report["held_vdisks"], [])

    def test_a_host_with_nothing_running_fences_cleanly(self):
        self.arrange()
        report = daemon.fence_this_host()
        self.assertTrue(report["fenced"], report)
        self.assertEqual(self.detached, [])

    def test_a_vdisk_still_served_afterwards_means_the_fence_did_not_take(self):
        # The read-back is the point: a detach that was issued and did not take must not
        # be reported as a fence, or the leader restarts guests on another host while this
        # one is still serving their disks.
        self.arrange(attached_before=["vm-disk0"], attached_after=["vm-disk0"],
                     detach_fails=("vm-disk0",))
        report = daemon.fence_this_host()
        self.assertFalse(report["fenced"])
        self.assertEqual(report["held_vdisks"], ["vm-disk0"])
        self.assertIn("still serving", report["detail"])

    def test_a_surviving_guest_process_means_the_fence_did_not_take(self):
        self.arrange(running_domains=["web01"], qemu_after=[4211],
                     attached_before=[], attached_after=[])
        report = daemon.fence_this_host()
        self.assertFalse(report["fenced"])
        self.assertIn("still running", report["detail"])

    def test_sidon_not_answering_is_not_a_fence(self):
        def explode(timeout=15):
            raise RuntimeError("control socket is unreachable")

        self.arrange()
        self.patch(daemon, "load_sidon_module",
                   lambda: types.SimpleNamespace(list_attached=explode, detach=None))
        report = daemon.fence_this_host()
        self.assertNotIn("fenced", report.get("detail", "").split(" -- ")[0].lower()[:6])
        self.assertIn("did not answer", report["detail"])


class DaemonOptionsEndpointTests(FenceTestCase):
    """The options endpoint exists so a caller can tell quorum-off from quorum-held."""

