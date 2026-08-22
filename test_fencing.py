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


def probe(libvirt="ok", drbd_control="ok", unserviceable=(), detail=None):
    return {"libvirt": libvirt, "drbd_control": drbd_control,
            "unserviceable": list(unserviceable), "detail": detail or {}}


def unserviceable(cause="quorum-lost", resource="vm-disk0"):
    return {"resource": resource, "cause": cause,
            "detail": f"{resource}/0 is Primary without quorum"}


def resource(name="vm-disk0", role="Secondary", devices=None, connections=None, **extra):
    entry = {"name": name, "role": role,
             "devices": devices if devices is not None else [],
             "connections": connections if connections is not None else []}
    entry.update(extra)
    return entry


def device(volume=0, quorum=True, disk_state="UpToDate", open_=False):
    return {"volume": volume, "quorum": quorum, "disk-state": disk_state, "open": open_}


def connection(name="node-b", state="Connecting", peer_devices=None):
    return {"name": name, "connection": state,
            "peer_devices": peer_devices if peer_devices is not None else []}


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
        self.patch(mipha, "run_mtls_spark_api",
                   lambda *a, **k: (0, [resource(role="Secondary",
                                                 devices=[device(open_=False)])], ""))
        confirmed, detail = mipha.spark_fence_host("10.0.0.2")
        self.assertTrue(confirmed)
        self.assertIn("legacy", detail)
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
                   lambda *a, **k: (0, [resource(role="Primary",
                                                 devices=[device(open_=True)])], ""))
        confirmed, detail = mipha.spark_fence_host("10.0.0.2")
        self.assertFalse(confirmed)
        self.assertIn("Primary", detail)

    def test_storage_state_that_cannot_be_read_is_not_confirmed(self):
        self.patch(mipha, "run_mtls_spark_api_full", lambda *a, **k: (404, {}, "HTTP 404"))
        self.patch(mipha, "run_remote_spark",
                   lambda ip, command: (1, "", "") if command.startswith("pgrep") else (0, "", ""))
        self.patch(mipha, "run_mtls_spark_api", lambda *a, **k: (-1, {}, "connection reset"))
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

class QuorumArithmeticTests(FenceTestCase):
    """`quorum majority` is a proof; `quorum 1` is a coin toss dressed as one."""

    def test_majority_with_io_error_arms_the_fence(self):
        armed, why = mipha.quorum_arms_the_fence(
            {"quorum": "majority", "on-no-quorum": "io-error"}, 3)
        self.assertTrue(armed, why)

    def test_suspend_io_also_arms_it(self):
        # A suspended writer is not writing. The guest hangs instead of erroring, which
        # is a different user experience and the same safety property.
        armed, _why = mipha.quorum_arms_the_fence(
            {"quorum": "all", "on-no-quorum": "suspend-io"}, 2)
        self.assertTrue(armed)

    def test_quorum_off_does_not_arm_it(self):
        # This is what LINSTOR writes for a single-replica resource, and what the live
        # test cluster actually has -- so the storage rung correctly refuses there.
        armed, why = mipha.quorum_arms_the_fence(
            {"quorum": "off", "on-no-quorum": "io-error"}, 1)
        self.assertFalse(armed)
        self.assertIn("quorum is off", why)

    def test_a_quorum_both_sides_can_hold_does_not_arm_it(self):
        armed, why = mipha.quorum_arms_the_fence(
            {"quorum": "1", "on-no-quorum": "io-error"}, 3)
        self.assertFalse(armed)
        self.assertIn("both sides", why)

    def test_a_numeric_majority_arms_it(self):
        armed, _why = mipha.quorum_arms_the_fence(
            {"quorum": "2", "on-no-quorum": "io-error"}, 3)
        self.assertTrue(_why == "" and armed)

    def test_a_policy_that_keeps_writing_does_not_arm_it(self):
        armed, why = mipha.quorum_arms_the_fence({"quorum": "majority"}, 3)
        self.assertFalse(armed)
        self.assertIn("on-no-quorum", why)

    def test_unreadable_options_are_never_assumed_safe(self):
        armed, why = mipha.quorum_arms_the_fence(None, 3)
        self.assertFalse(armed)
        self.assertIn("could not be read", why)


class StorageFenceTests(FenceTestCase):

    HOSTS = [{"hostname": "node-a", "ip": "10.0.0.1"},
             {"hostname": "node-b", "ip": "10.0.0.2"},
             {"hostname": "node-c", "ip": "10.0.0.3"}]

    def arrange(self, status, options):
        self.patch(mipha, "_drbd_status_from",
                   lambda ip: status.get(ip) if isinstance(status, dict) else status)
        self.patch(mipha, "_drbd_options_from", lambda ip, res: options)

    def quorate_resource(self, peer_state="Connecting"):
        return resource(name="vm-disk0", role="Primary",
                        devices=[device(quorum=True)],
                        connections=[connection("node-b", peer_state),
                                     connection("node-c", "Connected")])

    def test_a_quorate_survivor_with_a_disconnected_peer_is_a_fence(self):
        self.arrange({"10.0.0.1": [self.quorate_resource()], "10.0.0.3": []},
                     {"quorum": "majority", "on-no-quorum": "io-error"})
        self.patch(mipha, "LOCAL_IP", "10.0.0.1")
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(confirmed, detail)
        self.assertIn("already failing", detail)

    def test_quorum_switched_off_is_not_a_fence(self):
        # Reading only the device's `quorum: true` flag would have called this confirmed.
        # It is true here because quorum is not enforced at all, not because a majority is
        # held -- and the partitioned host is writing away regardless.
        self.arrange({"10.0.0.1": [self.quorate_resource()], "10.0.0.3": []},
                     {"quorum": "off", "on-no-quorum": "io-error"})
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(confirmed)
        self.assertIn("quorum is off", detail)

    def test_a_peer_that_is_still_connected_is_not_cut_off(self):
        self.arrange({"10.0.0.1": [self.quorate_resource(peer_state="Connected")],
                      "10.0.0.3": []},
                     {"quorum": "majority", "on-no-quorum": "io-error"})
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(confirmed)
        self.assertIn("still Connected", detail)

    def test_a_survivor_that_does_not_hold_quorum_is_not_a_fence(self):
        # If we are the minority, the other side may be the one still serving. Failing
        # over here would be the split-brain, not the cure for it.
        losing = resource(name="vm-disk0", role="Secondary",
                          devices=[device(quorum=False)],
                          connections=[connection("node-b", "Connecting")])
        self.arrange({"10.0.0.1": [losing], "10.0.0.3": []},
                     {"quorum": "majority", "on-no-quorum": "io-error"})
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(confirmed)
        self.assertIn("does not hold quorum", detail)

    def test_options_that_cannot_be_read_are_not_a_fence(self):
        self.arrange({"10.0.0.1": [self.quorate_resource()], "10.0.0.3": []}, None)
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(confirmed)
        self.assertIn("could not be read", detail)

    def test_a_host_that_shares_no_storage_gives_no_evidence(self):
        # Deliberately not "confirmed": no shared disk means no corruption from *this*
        # resource set, but it also means DRBD has nothing to say about whether the host
        # stopped. Silence is not proof.
        self.arrange({"10.0.0.1": [resource(name="other", role="Primary",
                                            devices=[device()],
                                            connections=[connection("node-c", "Connected")])],
                      "10.0.0.3": []},
                     {"quorum": "majority", "on-no-quorum": "io-error"})
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(confirmed)
        self.assertIn("no evidence", detail)

    def test_one_unarmed_resource_out_of_two_blocks_the_assertion(self):
        # Partial safety is not safety: the VM on the unarmed resource is the one that
        # gets two writers.
        armed = self.quorate_resource()
        unarmed = resource(name="vm-disk1", role="Primary", devices=[device(quorum=True)],
                           connections=[connection("node-b", "Connecting")])

        def options_for(_ip, res):
            if res == "vm-disk1":
                return {"quorum": "off", "on-no-quorum": "io-error"}
            return {"quorum": "majority", "on-no-quorum": "io-error"}

        self.patch(mipha, "_drbd_status_from",
                   lambda ip: [armed, unarmed] if ip == "10.0.0.1" else [])
        self.patch(mipha, "_drbd_options_from", options_for)
        confirmed, detail = mipha.storage_fence_assert("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(confirmed)
        self.assertIn("vm-disk1", detail)

    def test_no_surviving_host_is_not_a_fence(self):
        confirmed, detail = mipha.storage_fence_assert(
            "node-b", "10.0.0.2", [{"hostname": "node-b", "ip": "10.0.0.2"}])
        self.assertFalse(confirmed)
        self.assertIn("no surviving host", detail)


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
                storage=(False, "quorum is off")):
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
        self.arrange(spark=(True, "nothing is left running"))
        result = mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(result.confirmed)
        self.assertEqual(result.method, mipha.FENCE_METHOD_SPARK)
        self.assertEqual(self.bmc_calls, [])
        self.assertEqual(self.storage_calls, [])

    def test_the_ladder_escalates_when_a_rung_cannot_confirm(self):
        self.arrange(storage=(True, "quorum held here"))
        result = mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(result.confirmed)
        self.assertEqual(result.method, mipha.FENCE_METHOD_STORAGE)
        self.assertEqual([step["method"] for step in result.steps],
                         [mipha.FENCE_METHOD_SPARK, mipha.FENCE_METHOD_BMC,
                          mipha.FENCE_METHOD_STORAGE])

    def test_every_rung_failing_is_an_unconfirmed_fence(self):
        self.arrange()
        result = mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertFalse(result.confirmed)
        self.assertEqual(result.method, mipha.FENCE_METHOD_NONE)

    def test_a_rung_that_raises_is_a_failure_not_a_crash(self):
        def explode(ip):
            raise RuntimeError("ipmitool segfaulted")

        self.arrange(storage=(True, "quorum held here"))
        self.patch(mipha, "spark_fence_host", explode)
        result = mipha.fence_host("node-b", "10.0.0.2", self.HOSTS)
        self.assertTrue(result.confirmed)
        self.assertIn("RuntimeError", result.steps[0]["detail"])

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

class UnserviceableResourceTests(FenceTestCase):

    def test_primary_without_quorum_is_unserviceable(self):
        bad, cause, detail = mipha.resource_is_unserviceable(
            resource(role="Primary", devices=[device(quorum=False)]))
        self.assertTrue(bad)
        self.assertEqual(cause, "quorum-lost")
        self.assertIn("without quorum", detail)

    def test_a_secondary_is_never_unserviceable(self):
        # A Secondary is not writing, so it has nothing to be fenced off.
        bad, _cause, _detail = mipha.resource_is_unserviceable(
            resource(role="Secondary", devices=[device(quorum=False)]))
        self.assertFalse(bad)

    def test_a_failed_disk_with_a_healthy_peer_keeps_serving(self):
        # DRBD 9 turns the local node into a diskless client and reads over the network.
        # The guest never notices, and fencing it would be an outage we caused.
        bad, _cause, _detail = mipha.resource_is_unserviceable(resource(
            role="Primary",
            devices=[device(disk_state="Failed")],
            connections=[connection("node-b", "Connected",
                                    [{"volume": 0, "peer-disk-state": "UpToDate"}])]))
        self.assertFalse(bad)

    def test_a_failed_disk_with_no_healthy_peer_is_unserviceable(self):
        bad, cause, _detail = mipha.resource_is_unserviceable(resource(
            role="Primary",
            devices=[device(disk_state="Failed")],
            connections=[connection("node-b", "Connecting",
                                    [{"volume": 0, "peer-disk-state": "DUnknown"}])]))
        self.assertTrue(bad)
        self.assertEqual(cause, "no-data")

    def test_forced_io_failures_are_unserviceable(self):
        bad, cause, _detail = mipha.resource_is_unserviceable(
            resource(role="Primary", devices=[device()], **{"force-io-failures": True}))
        self.assertTrue(bad)
        self.assertEqual(cause, "io-failures")


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

    def test_three_consecutive_quorum_losses_self_fence(self):
        for _ in range(3):
            action, reason = self.decide(probe(unserviceable=[unserviceable()]))
        self.assertEqual(action, "fence")
        self.assertIn("quorum", reason)

    def test_quorum_loss_does_not_wait_for_a_healthy_peer(self):
        # Losing quorum *is* the majority test. If this node lost it, some other set of
        # nodes holds it, whether or not they are answering us right now.
        self.patch(mipha, "healthy_peer_exists", lambda hosts=None: False)
        for _ in range(3):
            action, _reason = self.decide(probe(unserviceable=[unserviceable()]))
        self.assertEqual(action, "fence")

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
            action, _reason = self.decide(probe(libvirt="unknown", drbd_control="unknown"))
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
            self.decide(probe(drbd_control="failed"))
        action, _reason = self.decide(probe())
        self.assertEqual(action, "none")
        self.assertEqual(self.counters["drbd_control"], 0)


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

    def test_drbdsetup_failing_on_a_host_with_resources_is_a_failure(self):
        self.patch(mipha, "get_all_drbd_resources", lambda: ["vm-disk0"])
        self.patch(mipha.shutil, "which", lambda _n: None)
        self.patch(mipha, "run_argv_local",
                   lambda argv, timeout=45: (1, "", "drbdsetup: cannot open netlink"))
        current = mipha.probe_local_health()
        self.assertEqual(current["drbd_control"], "failed")

    def test_drbdsetup_failing_on_a_host_with_no_resources_is_unknown(self):
        # A node that simply does not back any DRBD resource must not read as a node
        # whose storage stack has died.
        self.patch(mipha, "get_all_drbd_resources", lambda: [])
        self.patch(mipha.shutil, "which", lambda _n: None)
        self.patch(mipha, "run_argv_local", lambda argv, timeout=45: (1, "", "no such module"))
        current = mipha.probe_local_health()
        self.assertEqual(current["drbd_control"], "unknown")

    def test_a_missing_virsh_is_unknown_rather_than_failed(self):
        self.patch(mipha, "get_all_drbd_resources", lambda: [])
        self.patch(mipha.shutil, "which", lambda _n: None)
        self.patch(mipha, "run_argv_local", lambda argv, timeout=45: (0, "[]", ""))
        current = mipha.probe_local_health()
        self.assertEqual(current["libvirt"], "unknown")

    def test_unserviceable_resources_are_collected_from_the_status_document(self):
        self.patch(mipha, "get_all_drbd_resources", lambda: ["vm-disk0"])
        self.patch(mipha.shutil, "which", lambda _n: "/usr/bin/virsh")
        status = json.dumps([resource(name="vm-disk0", role="Primary",
                                      devices=[device(quorum=False)])])

        def fake_argv(argv, timeout=45):
            if argv[0] == "drbdsetup":
                return 0, status, ""
            return 0, "", ""

        self.patch(mipha, "run_argv_local", fake_argv)
        current = mipha.probe_local_health()
        self.assertEqual(current["libvirt"], "ok")
        self.assertEqual(current["drbd_control"], "ok")
        self.assertEqual([item["cause"] for item in current["unserviceable"]],
                         ["quorum-lost"])


# -- the daemon side: a fence that reads back what it did ----------------------------------

class DaemonFenceTests(FenceTestCase):

    def setUp(self):
        super().setUp()
        self.patch(daemon, "write_fence_marker", lambda reason, report: True)
        self.patch(daemon, "time", types.SimpleNamespace(sleep=lambda _s: None,
                                                         time=lambda: 0.0))
        self.commands = []

    def arrange(self, running_domains=(), qemu_after=(), resources_before=(),
                resources_after=None):
        self.states = [list(resources_before),
                       list(resources_before if resources_after is None else resources_after)]

        def fake_argv(argv, timeout=45):
            self.commands.append(argv)
            if argv[:2] == ["virsh", "-c"] and "list" in argv:
                return 0, "\n".join(running_domains), ""
            if argv[:2] == ["virsh", "-c"] and "destroy" in argv:
                return 0, "Domain destroyed", ""
            if argv[0] == "mountpoint":
                return 1, "", ""
            return 0, "", ""

        self.patch(daemon, "run_argv", fake_argv)
        self.patch(daemon, "qemu_process_ids", lambda: list(qemu_after))
        self.patch(daemon, "drbd_local_resources",
                   lambda: self.states.pop(0) if self.states else [])

    def test_a_fence_is_confirmed_only_when_nothing_is_left(self):
        self.arrange(running_domains=["web01"], qemu_after=[],
                     resources_before=[("vm-disk0", "Primary", [])],
                     resources_after=[("vm-disk0", "Secondary", [])])
        report = daemon.fence_this_host()
        self.assertTrue(report["fenced"], report)
        self.assertEqual(report["primary_resources"], [])
        self.assertIn(["virsh", "-c", "qemu:///system", "destroy", "web01"], self.commands)

    def test_a_surviving_guest_process_means_the_fence_did_not_take(self):
        self.arrange(qemu_after=[4211],
                     resources_before=[("vm-disk0", "Secondary", [])])
        report = daemon.fence_this_host()
        self.assertFalse(report["fenced"])
        self.assertEqual(report["qemu_pids"], [4211])
        self.assertIn("still running", report["detail"])

    def test_a_resource_that_refused_to_demote_means_the_fence_did_not_take(self):
        self.arrange(resources_before=[("vm-disk0", "Primary", [])],
                     resources_after=[("vm-disk0", "Primary", ["0"])])
        report = daemon.fence_this_host()
        self.assertFalse(report["fenced"])
        self.assertEqual(report["primary_resources"], ["vm-disk0"])
        self.assertIn("vm-disk0/0", report["open_devices"])

    def test_unreadable_drbd_state_is_not_a_fence(self):
        self.arrange(resources_before=[])
        self.patch(daemon, "drbd_local_resources", lambda: None)
        report = daemon.fence_this_host()
        self.assertFalse(report["fenced"])
        self.assertIn("did not answer", report["detail"])

    def test_demotion_is_checked_and_never_forced(self):
        # `drbdadm secondary --force` past a process that still holds the device would
        # not make that process stop writing; it would only stop us finding out.
        self.arrange(resources_before=[("vm-disk0", "Primary", [])],
                     resources_after=[("vm-disk0", "Secondary", [])])
        daemon.fence_this_host()
        demotions = [argv for argv in self.commands if argv[:2] == ["drbdadm", "secondary"]]
        self.assertEqual(demotions, [["drbdadm", "secondary", "vm-disk0"]])

    def test_a_host_with_nothing_running_fences_cleanly(self):
        self.arrange(resources_before=[])
        report = daemon.fence_this_host()
        self.assertTrue(report["fenced"])


class DaemonOptionsEndpointTests(FenceTestCase):
    """The options endpoint exists so a caller can tell quorum-off from quorum-held."""

    def test_drbdsetup_show_is_parsed_into_the_options_object(self):
        shown = [{"resource": "vm-disk0",
                  "options": {"quorum": "majority", "on-no-quorum": "io-error"}}]
        self.patch(daemon, "run_argv", lambda argv, timeout=45: (0, json.dumps(shown), ""))
        captured = {}

        handler = daemon.SparkDaemonHandler.__new__(daemon.SparkDaemonHandler)
        handler.query_param = lambda parsed, key: "vm-disk0"
        handler.send_json_response = lambda status, body: captured.update(
            {"status": status, "body": body})
        handler.reject = lambda message, status=400: captured.update(
            {"status": status, "body": {"error": message}})
        handler.handle_storage_drbd_options(types.SimpleNamespace(query=""))

        self.assertEqual(captured["status"], 200)
        self.assertEqual(captured["body"]["options"]["quorum"], "majority")


if __name__ == "__main__":
    unittest.main()
