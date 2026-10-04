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


if __name__ == "__main__":
    unittest.main()
