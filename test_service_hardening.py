#!/usr/bin/env python3
"""Small defects found reading every service, each pinned.

Run with:  python -m unittest test_service_hardening
"""

import importlib.util
import os
import re
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name, alias):
    spec = importlib.util.spec_from_file_location(alias, os.path.join(HERE, name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read(name):
    with open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


class DagurRunRowsSurviveAQuoteInTheJobName(unittest.TestCase):
    def test_both_statements_escape_the_name(self):
        dagur = load("dagur.py", "dagur_h")
        seen = []
        dagur.run_cql_query = lambda cql, *a, **k: seen.append(cql) or (0, "", "")
        dagur.call_catalyst_api = lambda *a, **k: (200, {})
        dagur.insert_dagur_run("it's", 1, "r", 2, "SUCCESS", 0, "out")
        self.assertIn("VALUES ('it''s',", seen[0])
        seen.clear()
        try:
            dagur.execute_dagur_job_thread("t", "it's", "true")
        except Exception:
            pass
        self.assertTrue(seen, "the RUNNING row was never written")
        self.assertIn("VALUES ('it''s',", seen[0])


class TheNvramWatcherEndsItsErrorsWithANewline(unittest.TestCase):
    def test_no_literal_backslash_n_inside_the_messages(self):
        text = read("spark_daemon_decoded.py")
        for line in re.findall(r'.*\[NVRAM Watcher\] Error.*', text):
            self.assertNotIn("\\\\n", line, line)
            self.assertIn('\\n")', line, line)


class TheClusterStatusHasNoGlusterRemnant(unittest.TestCase):
    def test_the_volume_name_filter_is_gone(self):
        self.assertNotIn('"volume name:"', read("spark_daemon_decoded.py"))


class NoDaemonDefinesTheSameTopLevelNameTwice(unittest.TestCase):
    """The later definition silently replaces the earlier one, so a reader fixing the first is
    fixing nothing. Two of these existed (a 120 s and a 30 s `run_mtls_spark_api`, and two
    `get_zookeeper_leader_ip`), and the one that ran was the one nobody was looking at."""

    def test_no_repeated_function_or_class(self):
        import ast
        import collections
        import glob
        repeated = {}
        for path in sorted(glob.glob(os.path.join(HERE, "*.py"))):
            name = os.path.basename(path)
            if name.startswith("test_") or name == "provision.py":
                continue
            tree = ast.parse(read(name), filename=name)
            counts = collections.Counter(
                n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)))
            for symbol, n in counts.items():
                if n > 1:
                    repeated.setdefault(name, []).append(symbol)
        self.assertEqual(repeated, {})


class LogosWritesOnlyUnderAnAddressThatNamesTheNode(unittest.TestCase):
    def test_loopback_and_empty_are_not_an_identity(self):
        logos = load("logos.py", "logos_h")
        self.assertFalse(logos.has_node_identity("127.0.0.1"))
        self.assertFalse(logos.has_node_identity(""))
        self.assertFalse(logos.has_node_identity(None))
        self.assertTrue(logos.has_node_identity("10.10.102.41"))

    def test_the_write_is_behind_the_check(self):
        src = read("logos.py")
        self.assertLess(src.index("if not has_node_identity(local_ip):"),
                        src.index("rc, out, err = run_cql_query(combined_cql, local_ip)"))


class BifrostResignsItsBallotOnSigterm(unittest.TestCase):
    def setUp(self):
        self.bifrost = load("bifrost.py", "bifrost_h")

    def test_the_ballot_is_withdrawn(self):
        calls = []

        class C(object):
            def withdraw(self):
                calls.append("withdraw")
        self.assertTrue(self.bifrost.withdraw_ballot(C()))
        self.assertEqual(calls, ["withdraw"])

    def test_a_ballot_that_cannot_be_resigned_does_not_hold_the_process_up(self):
        import threading
        import time
        release = threading.Event()

        class Stuck(object):
            def withdraw(self):
                release.wait(10)
        started = time.time()
        self.assertFalse(self.bifrost.withdraw_ballot(Stuck(), wait=0.2))
        self.assertLess(time.time() - started, 2)
        release.set()

    def test_an_error_in_withdraw_is_not_raised_into_the_handler(self):
        class Broken(object):
            def withdraw(self):
                raise RuntimeError("no session")
        self.assertTrue(self.bifrost.withdraw_ballot(Broken()))

    def test_the_signal_handler_calls_it_before_releasing_the_address(self):
        src = read("bifrost.py")
        handler = src[src.index("def signal_handler"):src.index("def is_local_ingress_listening")]
        self.assertLess(handler.index("withdraw_ballot("), handler.index("ip addr del"))


class TheWatchdogCheckAgreesWithTheDeclaredTable(unittest.TestCase):
    """mcli-runner's idea of what the watchdog restarts must be what it restarts. It was
    thirteen names and a three-name maintenance list after the watchdog had become the
    declared-table pass, so a dead Phoenix or Slate was not seen and a maintenance host's
    Daruk and Hylia were not checked."""

    def test_the_lists(self):
        import ast
        daemon = load("spark_daemon_decoded.py", "spark_h")
        tree = ast.parse(read("mcli-runner"))
        found = {}
        for node in tree.body:
            if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name) \
                    and node.targets[0].id in ("WATCHDOG_SERVICES", "WATCHDOG_MAINTENANCE_SERVICES"):
                found[node.targets[0].id] = ast.literal_eval(node.value)
        declared = [e["unit"] for e in daemon.MANAGED_SERVICES if "setting" not in e]
        self.assertEqual(sorted(found["WATCHDOG_SERVICES"]), sorted(declared))
        self.assertEqual(sorted(found["WATCHDOG_MAINTENANCE_SERVICES"]),
                         sorted(daemon.maintenance_watchdog_units()))


class TheClusterCliVerifiesTheDaemonItCalls(unittest.TestCase):
    def test_no_unverified_context_and_no_one_machines_path(self):
        src = read("cluster_new.py")
        self.assertNotIn("_create_unverified_context", src)
        self.assertNotIn("AuraFlight", src)

    def test_without_the_ca_it_says_so_and_does_not_connect(self):
        import tempfile
        cluster = load("cluster_new.py", "cluster_h")
        os.environ["HCI_CERT_DIR"] = tempfile.mkdtemp()
        self.addCleanup(os.environ.pop, "HCI_CERT_DIR", None)
        opened = []
        # urllib.request is one shared module: patch it for this test only.
        patcher = mock.patch.object(cluster.urllib.request, "urlopen",
                                    lambda *a, **k: opened.append(a))
        patcher.start()
        self.addCleanup(patcher.stop)
        rc, out, err = cluster.run_remote_spark("10.0.0.2", "true")
        self.assertEqual(rc, -1)
        self.assertIn("ca.crt", err)
        status, body, err = cluster.run_mtls_spark_api_full("10.0.0.2", "/api/v1/host/units")
        self.assertEqual(status, 0)
        self.assertIn("ca.crt", err)
        self.assertEqual(opened, [])


class AgahnimDoesNotLogTheConsoleToken(unittest.TestCase):
    def test_every_log_line_that_names_the_token_redacts_it(self):
        src = read("agahnim/src/main.rs")
        for line in src.splitlines():
            if "println!" in line and "token" in line.lower() and "{}" in line and "token" in line.split("println!")[1]:
                if "Missing token" in line:
                    continue
                self.assertTrue("redact(" in line or "token" not in line.split('",', 1)[-1],
                                "a log line prints a console token in the clear: " + line.strip())

    def test_the_redaction_exists_and_is_tested(self):
        src = read("agahnim/src/main.rs")
        self.assertIn("fn redact(token: &str)", src)
        self.assertIn("a_log_line_never_carries_the_whole_token", src)


class LanayruDestroyTouchesOnlyItsOwnGuests(unittest.TestCase):
    def test_exact_names_only(self):
        lanayru = load("lanayru.py", "lanayru_h")
        self.assertTrue(lanayru.owns_vm("web", "web-control-01"))
        self.assertTrue(lanayru.owns_vm("web", "web-control-03"))
        for other in ("webapp-1", "web", "web-control-1", "web-control-010", "web-controller-01",
                      "xweb-control-01", "", None):
            self.assertFalse(lanayru.owns_vm("web", other), other)

    def test_a_name_with_regex_characters_is_literal(self):
        lanayru = load("lanayru.py", "lanayru_h2")
        self.assertFalse(lanayru.owns_vm("a.c", "abc-control-01"))
        self.assertTrue(lanayru.owns_vm("a.c", "a.c-control-01"))

    def test_the_delete_loop_uses_it(self):
        self.assertNotIn("vm_name.startswith(cluster_name)", read("lanayru.py"))


class TheAPIServersReloadRenewedCertificates(unittest.TestCase):
    """impa renews certificates and restarts only spark-daemon. Catalyst and Vali built their TLS
    context once, so they kept presenting the old certificate from memory until it expired."""

    def make_certs(self, directory, name):
        import subprocess
        key, crt = os.path.join(directory, name + ".key"), os.path.join(directory, name + ".crt")
        subprocess.run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
                        "-nodes", "-keyout", key, "-out", crt, "-subj", "/CN=" + name, "-days", "2"],
                       check=True, stdin=subprocess.DEVNULL, capture_output=True)
        return crt, key

    def test_a_changed_file_rebuilds_the_context_and_a_bad_one_keeps_the_old(self):
        import shutil
        import tempfile
        if shutil.which("openssl") is None:
            self.skipTest("openssl is needed to make throwaway certificates")
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        for name in ("catalyst.py", "vali.py"):
            crt, key = self.make_certs(directory, "one")
            module = load(name, "reload_" + name[:-3])
            ctx = module.ReloadingServerContext(crt, key, crt)
            first = ctx._context
            ctx._refresh()
            self.assertIs(ctx._context, first, "rebuilt with nothing changed")
            crt2, key2 = self.make_certs(directory, "two")
            shutil.copy(crt2, crt)
            shutil.copy(key2, key)
            os.utime(crt, ns=(1, 2 * 10 ** 18))
            ctx._refresh()
            self.assertIsNot(ctx._context, first, name + " kept the old certificate in memory")
            kept = ctx._context
            with open(crt, "w") as handle:
                handle.write("not a certificate")
            os.utime(crt, ns=(1, 3 * 10 ** 18))
            ctx._refresh()
            self.assertIs(ctx._context, kept, "a corrupt file replaced the working context")

    def test_both_servers_use_it(self):
        for name in ("catalyst.py", "vali.py"):
            self.assertIn("ssl_context = ReloadingServerContext(", read(name))


class LogosCountsEachByteOnce(unittest.TestCase):
    def test_only_physical_nics_count_toward_host_throughput(self):
        import tempfile
        logos = load("logos.py", "logos_net")
        root = tempfile.mkdtemp()
        sysfs = os.path.join(root, "sys")
        for iface, physical in (("eth0", True), ("eth0.20", False), ("br0", False), ("tap1", False)):
            os.makedirs(os.path.join(sysfs, iface))
            if physical:
                os.makedirs(os.path.join(sysfs, iface, "device"))
        header = "h1\nh2\n"
        row = "%s: %d 0 0 0 0 0 0 0 %d 0 0 0 0 0 0 0\n"
        proc = os.path.join(root, "dev")
        with open(proc, "w") as handle:
            handle.write(header + row % ("lo", 999, 999) + row % ("eth0", 100, 200)
                         + row % ("eth0.20", 100, 200) + row % ("br0", 100, 200) + row % ("tap1", 100, 200))
        self.assertEqual(logos.get_net_stats(proc, sysfs), (100, 200))

    def test_when_sysfs_cannot_say_nothing_is_zeroed(self):
        import tempfile
        logos = load("logos.py", "logos_net2")
        root = tempfile.mkdtemp()
        proc = os.path.join(root, "dev")
        with open(proc, "w") as handle:
            handle.write("h1\nh2\n" + "eth0: 5 0 0 0 0 0 0 0 7 0 0 0 0 0 0 0\n")
        self.assertEqual(logos.get_net_stats(proc, os.path.join(root, "absent")), (5, 7))


class TheQueryLayerDoesNotSayAStatementTwice(unittest.TestCase):
    def setUp(self):
        import io
        import json
        import socket
        import urllib.error
        import urllib.request
        self.cql = load("helios_cql.py", "cql_h")
        self.fallbacks = []
        self.cql._cqlsh_fallback = lambda q: self.fallbacks.append(q) or (0, "fallback", "")
        self.errors = (io, json, socket, urllib.error)
        self.urlopen = urllib.request.urlopen
        self.addCleanup(setattr, urllib.request, "urlopen", self.urlopen)

    def raising(self, exc):
        import urllib.request

        def fake(*a, **k):
            raise exc
        urllib.request.urlopen = fake

    def test_a_database_refusal_is_returned_and_not_retried_through_cqlsh(self):
        io, json, socket, urlerr = self.errors
        self.raising(urlerr.HTTPError("u", 400, "Bad", {}, io.BytesIO(json.dumps({"error": "Unavailable"}).encode())))
        rc, out, err = self.cql.run_cql_query("UPDATE hydra.t SET a = 1 WHERE b = 2;")
        self.assertEqual((rc, err), (1, "Unavailable"))
        self.assertEqual(self.fallbacks, [])

    def test_a_timeout_after_the_request_was_sent_is_not_retried(self):
        io, json, socket, urlerr = self.errors
        self.raising(socket.timeout("timed out"))
        rc, out, err = self.cql.run_cql_query("INSERT INTO hydra.t (a) VALUES (1);")
        self.assertEqual(rc, 1)
        self.assertEqual(self.fallbacks, [])

    def test_a_daruk_that_is_not_there_still_falls_back(self):
        io, json, socket, urlerr = self.errors
        self.raising(urlerr.URLError(ConnectionRefusedError(111, "refused")))
        rc, out, err = self.cql.run_cql_query("SELECT now() FROM system.local;")
        self.assertEqual((rc, out), (0, "fallback"))
        self.assertEqual(len(self.fallbacks), 1)


class SystemctlShowValuesAreAttributedToTheRightUnit(unittest.TestCase):
    def test_blank_separated_blocks(self):
        daemon = load("spark_daemon_decoded.py", "spark_show")
        out = "0\n\n3\n\n0\n"
        self.assertEqual(daemon.show_values_by_unit(out, ["a", "b", "c"]), {"a": "0", "b": "3", "c": "0"})

    def test_a_count_that_does_not_match_attributes_nothing(self):
        daemon = load("spark_daemon_decoded.py", "spark_show2")
        self.assertEqual(daemon.show_values_by_unit("0\n\n3\n", ["a", "b", "c"]), {})
        self.assertEqual(daemon.show_values_by_unit("", ["a"]), {})

    def test_the_status_publisher_uses_it_for_restarts_and_results(self):
        src = read("spark_daemon_decoded.py")
        self.assertEqual(src.count("show_values_by_unit("), 3)  # the definition and two uses


class AnUnknownDesiredStateStopsNothingAtBoot(unittest.TestCase):
    def test_autostart_has_an_unknown_state_that_changes_nothing(self):
        src = read("spark_daemon_decoded.py")
        body = src[src.index("def check_cluster_and_autostart"):src.index("Autostart completed successfully")]
        self.assertIn('cluster_state = "unknown"', body)
        self.assertNotIn('cluster_state = "stopped"\n    if quorum_established', body)
        self.assertLess(body.index('if cluster_state == "unknown":'),
                        body.index('converge_to_desired_state("stopped", full=True)\n    else:'))


class SidonKeepsRunningInMaintenance(unittest.TestCase):
    def test_its_unit_has_no_maintenance_condition(self):
        src = read("provision.py")
        unit = src[src.index("Description=Sidon DFS Data Path Daemon"):]
        unit = unit[:unit.index("[Service]")]
        self.assertNotIn("ConditionPathExists=!/etc/hci/maintenance.state", unit)
        self.assertIn("ConditionPathExists=/etc/hci/cluster.json", unit)


class SpectrumKeepsTryingToSeedItsDatabase(unittest.TestCase):
    """spectrum_server.py must not be imported by a test, so the function is compiled out of the
    source with a fake environment."""

    def build(self, attempts):
        import ast
        import threading
        import time as real_time
        src = read("spectrum_server.py")
        tree = ast.parse(src)
        wanted = {"init_db"}
        body = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in wanted]
        scope = {"INIT_DB_RETRY_SECONDS": 0.01, "_INIT_DB_RETRY_THREAD": [None],
                 "threading": threading, "time": real_time, "print": lambda *a, **k: None}
        calls = []

        def once():
            calls.append(1)
            return attempts[min(len(calls) - 1, len(attempts) - 1)]
        scope["_init_db_once"] = once
        exec(compile(ast.Module(body=body, type_ignores=[]), "spectrum_server.py", "exec"), scope)
        return scope, calls

    def test_a_first_failure_starts_a_thread_that_runs_until_it_succeeds(self):
        import time
        scope, calls = self.build([False, False, True])
        self.assertFalse(scope["init_db"]())
        deadline = time.time() + 3
        while len(calls) < 3 and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(len(calls), 3)
        self.assertIsNotNone(scope["_INIT_DB_RETRY_THREAD"][0])

    def test_a_first_success_starts_nothing(self):
        scope, calls = self.build([True])
        self.assertTrue(scope["init_db"]())
        self.assertIsNone(scope["_INIT_DB_RETRY_THREAD"][0])
        self.assertEqual(len(calls), 1)

    def test_the_attempt_has_no_loop_of_its_own(self):
        src = read("spectrum_server.py")
        once = src[src.index("def _init_db_once"):src.index("def init_db")]
        self.assertNotIn("for i in range(15)", once)


class ValcliDoesNotReportARefusalAsSuccess(unittest.TestCase):
    def setUp(self):
        self.valcli = load("valcli.py", "valcli_refusal")

    def test_the_classifier(self):
        refused = self.valcli.spark_call_refused
        self.assertIsNone(refused(0, {"ok": True}))
        self.assertEqual(refused(0, {"error": "vdisk is attached"}), "vdisk is attached")
        self.assertEqual(refused(-1, {}, "timed out"), "timed out")
        self.assertEqual(refused(-1, {"error": "x"}, "y"), "x")
        self.assertEqual(refused(-1, {}, ""), "no answer")

    def test_both_delete_commands_use_it(self):
        src = read("valcli.py")
        self.assertEqual(src.count("spark_call_refused(rc_del, body_del, err_del)"), 2)

    def test_a_refused_image_delete_leaves_the_catalogue_row(self):
        src = read("valcli.py")
        body = src[src.index("Deleting vdisk '{vdisk_id}'"):src.index("# 4. Delete from ScyllaDB")]
        self.assertIn("The image's catalogue entry was left in place.", body)


class HyliaCanFinishAnUpgrade(unittest.TestCase):
    def test_the_post_reboot_check_names_services_the_status_document_has(self):
        src = read("hylia.py")
        self.assertIn('critical_services = ["ZooKeeper", "HydraDB", "Sidon", "Spark"]', src)
        self.assertNotIn('"Aether"', src)
        daemon = read("spark_daemon_decoded.py")
        self.assertIn('"Sidon"', daemon)

    def test_builds_are_given_longer_than_the_daemons_default(self):
        src = read("hylia.py")
        self.assertIn("BUILD_TIMEOUT_SECONDS = 1800", src)
        self.assertEqual(src.count("timeout=BUILD_TIMEOUT_SECONDS"), 2)
        self.assertIn("set -o pipefail; cd '{work}' && cargo build", src)

    def test_the_spectrum_image_build_gets_every_module_the_dockerfile_copies(self):
        import re
        dockerfile = read("Dockerfile")
        modules = set(re.findall(r"^COPY (helios_\w+)\.py \.", dockerfile, re.M))
        src = read("hylia.py")
        loop = re.search(r"for m in ([\w ]+); do", src).group(1).split()
        self.assertTrue(modules <= set(loop), modules - set(loop))


class EveryImportedModuleIsPartOfTheUpgrade(unittest.TestCase):
    def test_helios_cql_and_schema_ship_and_are_inventoried(self):
        for name in ("create_upgrade_zip.py", "check_updates.py"):
            src = read(name)
            for component in ('"helios-cql"', '"helios-schema"'):
                self.assertIn(component, src, name)


class TheAuthCheckAlwaysAnswersOnce(unittest.TestCase):
    def test_both_branches_answer_and_return(self):
        src = read("spectrum_server.py")
        block = src[src.index('if path == "/api/auth/check":'):src.index('elif path == "/api/lcm/upgrade/check"')]
        self.assertIn("self.send_json(401", block)
        self.assertIn("return", block)


class MiphaDoesNotRepeatAFailoverThatFinished(unittest.TestCase):
    def test_guests_left_on(self):
        mipha = load("mipha.py", "mipha_h")
        rows = {"a": {"host_ip": "10.0.0.2", "state": "Running"}, "b": {"host_ip": "", "state": "Stopped"}}
        self.assertTrue(mipha.guests_left_on("10.0.0.2", lambda: rows))
        self.assertFalse(mipha.guests_left_on("10.0.0.9", lambda: rows))
        rows["a"]["state"] = "Stopped"
        self.assertFalse(mipha.guests_left_on("10.0.0.2", lambda: rows))
        self.assertTrue(mipha.guests_left_on("10.0.0.2", lambda: None), "an unreadable table is not 'finished'")

    def test_the_loop_skips_a_down_host_with_nothing_left(self):
        src = read("mipha.py")
        self.assertIn('already_failed_over = (db_status == "DOWN" and not guests_left_on(ip))', src)
        self.assertIn("and not already_failed_over)", src)


class AMigrationThatAlreadyAppliedItsColumnIsNotAFailure(unittest.TestCase):
    def setUp(self):
        self.schema = load("helios_schema.py", "schema_h")

    def run_with(self, statement, rc, err):
        return self.schema._run(lambda s: (rc, "", err), statement)

    def test_an_add_of_a_column_that_exists_is_success(self):
        self.assertEqual(self.run_with(
            "ALTER TABLE hydra.t ADD c text;", 2,
            "Invalid column name c because it conflicts with an existing column")[0], 0)
        self.assertEqual(self.run_with(
            "alter table hydra.t add c text", 2, "Column c already exists")[0], 0)

    def test_every_other_failure_still_raises(self):
        for statement, err in (
                ("ALTER TABLE hydra.t ADD c text;", "no viable alternative at input 'IF'"),
                ("ALTER TABLE hydra.t ADD c text;", "Unavailable"),
                ("CREATE TABLE hydra.t (a int PRIMARY KEY);", "already exists"),
                ("INSERT INTO hydra.t (a) VALUES (1);", "conflicts with an existing column")):
            with self.assertRaises(self.schema.SchemaError):
                self.run_with(statement, 2, err)


class CandidaciesDialTheLocalZooKeeperFirst(unittest.TestCase):
    def test_order(self):
        zk = load("helios_zk.py", "zk_h")
        c = zk.cluster_candidacy("svc", "10.0.0.3", hosts=["10.0.0.1", "10.0.0.2", "10.0.0.3"])
        self.assertEqual(list(c.hosts), ["10.0.0.3", "10.0.0.1", "10.0.0.2"])

    def test_a_node_not_in_the_list_keeps_the_document_order(self):
        zk = load("helios_zk.py", "zk_h2")
        c = zk.cluster_candidacy("svc", "10.0.0.9", hosts=["10.0.0.1", "10.0.0.2"])
        self.assertEqual(list(c.hosts), ["10.0.0.1", "10.0.0.2"])


class AFencedMiphaLeaderStandsDown(unittest.TestCase):
    def test_the_loop_withdraws_before_it_checks_leadership(self):
        src = read("mipha.py")
        loop = src[src.index("ballot_withdrawn = False\n\n    while True:"):]
        self.assertLess(loop.index("if self_fence_is_active():"), loop.index("if not monitor.leading():"))
        self.assertIn("monitor.withdraw()", loop)
        self.assertIn("ballot_withdrawn = False\n\n            # 1. Leadership Check", loop)


class TheNvramBackupDeletesTheLocalCopyOnlyAfterItIsSaved(unittest.TestCase):
    def run_script(self, daruk_ok, cqlsh_rc):
        import base64
        import json
        import re
        import subprocess
        import tempfile
        import types
        vali = load("vali.py", "vali_nvram")
        command = vali.get_nvram_backup_cmd("vm1", delete_local=True)
        code = base64.b64decode(re.search(r"b64decode\('([^']+)'", command).group(1)).decode()
        directory = tempfile.mkdtemp()
        path = os.path.join(directory, "vm1_vars.fd")
        with open(path, "wb") as handle:
            handle.write(b"x" * 100)
        target_template = path.replace("\\", "/").replace("vm1_vars.fd", "{vm_name}_vars.fd")
        code = code.replace("/var/lib/hci/aether/nvram/{vm_name}_vars.fd", target_template)
        calls = []

        class Resp(object):
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                return json.dumps({"status": "success" if daruk_ok else "error"}).encode()

        def fake_urlopen(*a, **k):
            return Resp()

        def fake_run(argv, **k):
            calls.append((argv, k))
            return types.SimpleNamespace(returncode=cqlsh_rc)
        import urllib.request
        from unittest import mock
        with mock.patch.object(urllib.request, "urlopen", fake_urlopen), \
                mock.patch.object(subprocess, "run", fake_run):
            exec(compile(code, "nvram", "exec"), {"__name__": "x"})
        return os.path.exists(path), calls

    def test_daruk_took_it(self):
        kept, calls = self.run_script(True, 1)
        self.assertFalse(kept)
        self.assertEqual(calls, [])

    def test_daruk_refused_and_cqlsh_took_it_by_stdin(self):
        kept, calls = self.run_script(False, 0)
        self.assertFalse(kept)
        argv, kwargs = calls[0]
        self.assertIsInstance(argv, list, "no shell string: the statement is too long for one")
        self.assertIn(b"INSERT INTO hydra.vm_nvram", kwargs["input"])

    def test_nobody_took_it_so_the_local_copy_stays(self):
        kept, calls = self.run_script(False, 1)
        self.assertTrue(kept)


class AFailedVirshListDoesNotUnplaceVms(unittest.TestCase):
    def test_both_page_handlers_reconcile_only_after_libvirt_answered(self):
        src = read("spectrum_server.py")
        self.assertEqual(src.count("virsh_read_ok = False"), 2)
        self.assertEqual(src.count("if is_local and virsh_read_ok:"), 2)
        self.assertEqual(src.count("virsh_read_ok = True"), 2)


class TheSettingsSaveReallyUpdatesClusterJson(unittest.TestCase):
    def test_the_command_the_console_builds_runs_and_merges(self):
        import base64
        import json
        import re
        import subprocess
        import tempfile
        src = read("spectrum_server.py")
        block = src[src.index("update_json_cmd = ("):src.index("rc_json, _, _ = run_remote_spark")]
        directory = tempfile.mkdtemp()
        path = os.path.join(directory, "cluster.json")
        with open(path, "w") as handle:
            json.dump({"hosts": [{"ip": "10.0.0.1"}], "vip": "old"}, handle)
        updates_b64 = base64.b64encode(json.dumps({"vip": "10.0.0.45", "cluster_name": "it's \"x\""}).encode()).decode()
        scope = {"updates_b64": updates_b64}
        exec(block.replace("rc_json", "_"), scope)
        command = scope["update_json_cmd"].replace("/etc/hci/cluster.json", path)
        import shutil
        if not shutil.which("bash"):
            self.skipTest("bash is not installed on this system")
        done = subprocess.run(["bash", "-c", command], capture_output=True, stdin=subprocess.DEVNULL)
        self.assertEqual(done.returncode, 0, done.stderr)
        merged = json.load(open(path))
        self.assertEqual(merged["vip"], "10.0.0.45")
        self.assertEqual(merged["cluster_name"], "it's \"x\"")
        self.assertEqual(merged["hosts"], [{"ip": "10.0.0.1"}], "existing keys survive")

    def test_only_the_keys_sent_touch_dns_ntp_and_timezone(self):
        src = read("spectrum_server.py")
        for flag in ("touch_dns", "touch_ntp", "touch_tz"):
            self.assertIn("if %s" % flag, src)


if __name__ == "__main__":
    unittest.main()
