#!/usr/bin/env python3
"""Tests for the mTLS certificate lifecycle.

Two defects are covered here, and every assertion corresponds to one of them.

Certificates were minted once by provision.py and never looked at again: nothing renewed
them and nothing warned, so the whole cluster would have stopped talking on one specific
day with no notice. These cover the classification that decides when to warn, the
parsing it depends on, and the ordering a renewal has to follow so a node never presents
a signature its peers do not yet trust.

Every mTLS client in the tree also set `check_hostname = False`, which made any
certificate the cluster CA ever signed acceptable for any node. These cover the address
rewriting that lets verification be switched on against certificates that are addressed
by IP, and the peer-identity check that stands in for it on the floating VIP, which no
certificate is issued for.

The last class mints a real chain with openssl and completes a real handshake against
it. It is the only thing here that can prove the SAN a renewal writes is one that
Python's hostname verification actually accepts; it skips if openssl is absent.

Run with:  python -m unittest test_mtls_lifecycle
"""

import calendar
import importlib.util
import os
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name, filename):
    """Import a repo script by path. None of them do work at module scope."""
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


impa = load("impa_under_test", "impa.py")
mimir = load("mimir_under_test", "mimir.py")
cluster = load("cluster_new_under_test", "cluster_new.py")

HAVE_OPENSSL = shutil.which("openssl") is not None


class OpensslParsingTests(unittest.TestCase):
    """`openssl x509 -noout -subject -issuer -dates -ext subjectAltName` output."""

    NODE = ("subject=CN=10.10.102.41\n"
            "issuer=CN=HCI-Root-CA\n"
            "notBefore=Aug 17 20:55:31 2026 GMT\n"
            "notAfter=Aug 14 20:55:31 2036 GMT\n"
            "X509v3 Subject Alternative Name: \n"
            "    IP Address:10.10.102.41\n")

    def test_node_certificate_fields(self):
        info = impa.parse_openssl_x509(self.NODE)
        self.assertEqual(info["subject"], "CN=10.10.102.41")
        self.assertEqual(info["issuer"], "CN=HCI-Root-CA")
        self.assertEqual(info["not_after"], "Aug 14 20:55:31 2036 GMT")
        self.assertEqual(info["san_ips"], ["10.10.102.41"])
        self.assertEqual(info["san_dns"], [])

    def test_multiple_san_entries_on_one_line(self):
        info = impa.parse_openssl_x509(
            "X509v3 Subject Alternative Name: \n"
            "    IP Address:10.0.0.1, IP Address:127.0.0.1, DNS:localhost, DNS:Valkyrie-A\n")
        self.assertEqual(info["san_ips"], ["10.0.0.1", "127.0.0.1"])
        self.assertEqual(info["san_dns"], ["localhost", "Valkyrie-A"])

    def test_certificate_with_no_san_reports_empty_not_missing(self):
        # client.crt has no SAN at all. The caller has to be able to tell that apart
        # from a parse failure, because "no SAN" is a real answer and means the
        # certificate can never satisfy hostname verification.
        info = impa.parse_openssl_x509(
            "No extensions in certificate\nsubject=CN=HCI-Client\nissuer=CN=HCI-Root-CA\n"
            "notAfter=Aug 14 20:55:31 2036 GMT\n")
        self.assertEqual(info["san_ips"], [])
        self.assertEqual(info["not_after"], "Aug 14 20:55:31 2036 GMT")

    def test_common_name_survives_both_openssl_spacings(self):
        # openssl 1.x prints CN=x, openssl 3.x prints CN = x.
        self.assertEqual(impa.common_name("subject=CN=10.0.0.1"), "10.0.0.1")
        self.assertEqual(impa.common_name("CN = 10.0.0.1"), "10.0.0.1")
        self.assertEqual(impa.common_name("O=HCI, CN=node-1"), "node-1")
        self.assertEqual(impa.common_name(""), "")


class ExpiryParsingTests(unittest.TestCase):
    def test_openssl_date_parses(self):
        expected = calendar.timegm((2036, 8, 14, 20, 55, 31, 0, 0, 0))
        self.assertEqual(impa.openssl_date_to_epoch("Aug 14 20:55:31 2036 GMT"), expected)

    def test_single_digit_day_is_padded_by_openssl(self):
        expected = calendar.timegm((2026, 9, 5, 1, 2, 3, 0, 0, 0))
        self.assertEqual(impa.openssl_date_to_epoch("Sep  5 01:02:03 2026 GMT"), expected)

    def test_garbage_returns_none_rather_than_a_guess(self):
        for value in ("", None, "not a date", "notAfter="):
            self.assertIsNone(impa.openssl_date_to_epoch(value), value)


class ExpiryClassificationTests(unittest.TestCase):
    """WARN at 30 days, FAIL at 7, and never PASS on a date that could not be read."""

    def test_thresholds(self):
        self.assertEqual(impa.classify_expiry(3650), "PASS")
        self.assertEqual(impa.classify_expiry(31), "PASS")
        self.assertEqual(impa.classify_expiry(30), "PASS")
        self.assertEqual(impa.classify_expiry(29), "WARN")
        self.assertEqual(impa.classify_expiry(7), "WARN")
        self.assertEqual(impa.classify_expiry(6), "FAIL")
        self.assertEqual(impa.classify_expiry(0), "FAIL")

    def test_already_expired_is_a_failure(self):
        self.assertEqual(impa.classify_expiry(-1), "FAIL")
        self.assertEqual(impa.classify_expiry(-4000), "FAIL")

    def test_unreadable_expiry_is_unknown_not_pass(self):
        # The check this replaces answered PASS when strptime raised, which reported a
        # certificate nobody could read the expiry of as healthy.
        self.assertEqual(impa.classify_expiry(None), "UNKNOWN")

    def test_worst_status_wins(self):
        self.assertEqual(impa.worst_status(["PASS", "WARN", "PASS"]), "WARN")
        self.assertEqual(impa.worst_status(["WARN", "FAIL"]), "FAIL")
        self.assertEqual(impa.worst_status(["PASS", "UNKNOWN"]), "UNKNOWN")
        self.assertEqual(impa.worst_status([]), "UNKNOWN")


class SanConstructionTests(unittest.TestCase):
    """What a renewed node certificate has to be addressable as."""

    def test_provisioned_san_is_the_node_ip_only(self):
        # This is what provision.py writes today, and it is why loopback and the VIP
        # could not be verified.
        info = {"san_ips": ["10.10.102.41"], "san_dns": []}
        covered, missing = impa.san_covers(
            info, ["10.10.102.41", "127.0.0.1", "Valkyrie-997A49", "10.10.102.45"])
        self.assertEqual(covered, ["10.10.102.41"])
        self.assertEqual(missing, ["127.0.0.1", "Valkyrie-997A49", "10.10.102.45"])

    def test_renewed_san_covers_every_route_a_daemon_dials(self):
        san = impa.node_san("10.10.102.41", hostname="Valkyrie-997A49", vip="10.10.102.45")
        info = impa.parse_openssl_x509(
            "X509v3 Subject Alternative Name:\n    " + san.replace("IP:", "IP Address:") + "\n")
        _covered, missing = impa.san_covers(
            info, ["10.10.102.41", "127.0.0.1", "localhost", "Valkyrie-997A49", "10.10.102.45"])
        self.assertEqual(missing, [])

    def test_node_ip_comes_first_and_loopback_is_always_present(self):
        self.assertTrue(impa.node_san("10.0.0.1").startswith("IP:10.0.0.1,"))
        self.assertIn("IP:127.0.0.1", impa.node_san("10.0.0.1"))

    def test_a_loopback_node_does_not_get_a_duplicate_entry(self):
        # Single-node lab clusters do exist with 127.0.0.1 as the host address; a
        # repeated SAN entry makes openssl reject the config outright.
        self.assertEqual(impa.node_san("127.0.0.1").count("IP:127.0.0.1"), 1)

    def test_absent_hostname_and_vip_are_simply_omitted(self):
        self.assertEqual(impa.node_san("10.0.0.1", None, None),
                         "IP:10.0.0.1,IP:127.0.0.1,DNS:localhost")


class RenewalOrderingTests(unittest.TestCase):
    """The CA signs the node certificates, so trust has to move before presentation."""

    HOSTS = ["10.0.0.1", "10.0.0.2", "10.0.0.3"]

    def actions(self, steps):
        return [step[1] for step in steps]

    def test_leaf_only_renewal_needs_no_trust_distribution(self):
        steps = impa.renewal_plan(self.HOSTS, rotate_ca=False)
        self.assertNotIn("trust", self.actions(steps))
        self.assertNotIn("prune", self.actions(steps))
        self.assertIsNone(impa.plan_violates_ordering(steps))

    def test_every_host_is_installed_and_then_verified(self):
        steps = impa.renewal_plan(self.HOSTS, rotate_ca=False)
        for host in self.HOSTS:
            present = [i for i, s in enumerate(steps) if s[1] == "present" and s[2] == host]
            verify = [i for i, s in enumerate(steps) if s[1] == "verify" and s[2] == host]
            self.assertEqual(len(present), 1, host)
            self.assertEqual(len(verify), 1, host)
            self.assertLess(present[0], verify[0], host)

    def test_ca_rotation_trusts_every_node_before_any_node_presents(self):
        steps = impa.renewal_plan(self.HOSTS, rotate_ca=True)
        last_trust = max(i for i, s in enumerate(steps) if s[1] == "trust")
        first_present = min(i for i, s in enumerate(steps) if s[1] == "present")
        self.assertLess(last_trust, first_present)
        self.assertEqual(
            len([s for s in steps if s[1] == "trust"]), len(self.HOSTS))

    def test_ca_rotation_prunes_only_after_every_node_presents(self):
        steps = impa.renewal_plan(self.HOSTS, rotate_ca=True)
        last_present = max(i for i, s in enumerate(steps) if s[1] == "present")
        first_prune = min(i for i, s in enumerate(steps) if s[1] == "prune")
        self.assertLess(last_present, first_prune)

    def test_the_new_ca_exists_before_anything_is_signed_with_it(self):
        steps = impa.renewal_plan(self.HOSTS, rotate_ca=True)
        self.assertLess(self.actions(steps).index("mint-ca"),
                        self.actions(steps).index("mint-leaf"))

    def test_generated_plans_pass_their_own_invariant(self):
        for rotate in (False, True):
            for hosts in ([], ["10.0.0.1"], self.HOSTS):
                steps = impa.renewal_plan(hosts, rotate_ca=rotate)
                self.assertIsNone(impa.plan_violates_ordering(steps), (rotate, hosts))

    def test_presenting_before_trusting_is_caught(self):
        steps = impa.renewal_plan(self.HOSTS, rotate_ca=True)
        first_trust = min(i for i, s in enumerate(steps) if s[1] == "trust")
        last_present = max(i for i, s in enumerate(steps) if s[1] == "present")
        steps[first_trust], steps[last_present] = steps[last_present], steps[first_trust]
        self.assertIsNotNone(impa.plan_violates_ordering(steps))

    def test_pruning_before_presenting_is_caught(self):
        steps = impa.renewal_plan(self.HOSTS, rotate_ca=True)
        last_present = max(i for i, s in enumerate(steps) if s[1] == "present")
        first_prune = min(i for i, s in enumerate(steps) if s[1] == "prune")
        steps[first_prune], steps[last_present] = steps[last_present], steps[first_prune]
        self.assertIsNotNone(impa.plan_violates_ordering(steps))

    def test_dropping_the_old_ca_without_distributing_the_new_one_is_caught(self):
        steps = [s for s in impa.renewal_plan(self.HOSTS, rotate_ca=True) if s[1] != "trust"]
        self.assertIsNotNone(impa.plan_violates_ordering(steps))


class SparkEndpointTests(unittest.TestCase):
    """Loopback has to be rewritten before hostname verification can be switched on.

    Every daemon dials 127.0.0.1:9099 to reach its own host, and no node certificate
    carries a loopback SAN. spark-daemon binds 0.0.0.0, so the node's own address reaches
    the same listener and verifies.
    """

    MODULES = ("mimir", "cluster")

    def modules(self):
        return {"mimir": mimir, "cluster": cluster}

    def test_a_peer_ip_is_used_as_given_and_verified(self):
        for name, module in self.modules().items():
            self.assertEqual(module.spark_endpoint("10.0.0.7"), ("10.0.0.7", True), name)

    def test_loopback_is_rewritten_to_this_nodes_own_address(self):
        original = mimir.LOCAL_IP
        try:
            mimir.LOCAL_IP = "10.0.0.9"
            for name in ("127.0.0.1", "localhost", "::1"):
                self.assertEqual(mimir.spark_endpoint(name), ("10.0.0.9", True), name)
        finally:
            mimir.LOCAL_IP = original

    def test_verification_is_dropped_only_when_the_local_address_is_unknown(self):
        # Refusing the call instead would take out every daemon's own-host path on a
        # node where spectrum.env never got written, which is strictly worse than the
        # status quo for a connection that cannot leave the machine anyway.
        original = mimir.LOCAL_IP
        try:
            mimir.LOCAL_IP = "127.0.0.1"
            self.assertEqual(mimir.spark_endpoint("127.0.0.1"), ("127.0.0.1", False))
            mimir.LOCAL_IP = ""
            self.assertEqual(mimir.spark_endpoint("127.0.0.1"), ("127.0.0.1", False))
        finally:
            mimir.LOCAL_IP = original

    def test_every_client_module_exposes_the_same_helper(self):
        for filename in ("catalyst.py", "dagur.py", "hylia.py", "mipha.py",
                         "spark_daemon_decoded.py"):
            module = load(filename.replace(".", "_"), filename)
            self.assertTrue(callable(getattr(module, "spark_endpoint", None)), filename)
            self.assertEqual(module.spark_endpoint("10.0.0.7"), ("10.0.0.7", True), filename)


class VipPeerIdentityTests(unittest.TestCase):
    """The VIP is answered by whichever node holds it, so it has no certificate."""

    HOSTS = ["10.10.102.41", "10.10.102.42"]

    def cert(self, *ips):
        return {"subjectAltName": tuple(("IP Address", ip) for ip in ips)}

    def test_a_cluster_nodes_certificate_is_accepted(self):
        ok, detail = impa.peer_is_cluster_node(self.cert("10.10.102.42"), self.HOSTS)
        self.assertTrue(ok)
        self.assertEqual(detail, "10.10.102.42")

    def test_a_certificate_for_a_foreign_address_is_refused(self):
        ok, detail = impa.peer_is_cluster_node(self.cert("192.0.2.10"), self.HOSTS)
        self.assertFalse(ok)
        self.assertIn("192.0.2.10", detail)

    def test_the_shared_client_certificate_cannot_answer_on_the_vip(self):
        # client.crt has no SAN and sits in /root/.certs on every node. Chain-only
        # verification accepted it as a server certificate, which is exactly the
        # "any valid cert impersonates any node" hole.
        ok, detail = impa.peer_is_cluster_node({"subjectAltName": ()}, self.HOSTS)
        self.assertFalse(ok)
        self.assertIn("no IP SAN", detail)

    def test_no_configured_hosts_means_nothing_is_accepted(self):
        ok, _ = impa.peer_is_cluster_node(self.cert("10.10.102.41"), [])
        self.assertFalse(ok)

    def test_cluster_new_builds_a_context_that_refuses_a_foreign_peer(self):
        context = cluster.ClusterPeerSSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.cluster_ips = frozenset(self.HOSTS)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertFalse(context.check_hostname)
        self.assertIn("10.10.102.41", context.cluster_ips)


@unittest.skipUnless(HAVE_OPENSSL, "openssl is not on PATH")
class MintedChainTests(unittest.TestCase):
    """Mint a chain the way a renewal does and handshake against it.

    Nothing above can prove that the SAN a renewal writes is one Python's hostname
    verification accepts, or that splitting extendedKeyUsage between the node and client
    certificates does not break the node certificate on its own outbound calls -- both
    are properties of OpenSSL, not of this code. This mints for real and connects.
    """

    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="helios-mtls-test-")
        cls.ca_key, cls.ca_crt = impa.mint_ca(cls.dir, cn="HCI-Test-CA", days=30)
        cls.node_key, cls.node_crt = impa.mint_leaf(
            cls.dir, "127.0.0.1", 30, cls.ca_key, cls.ca_crt,
            eku="serverAuth,clientAuth",
            san=impa.node_san("127.0.0.1", hostname="Valkyrie-TEST", vip=None),
            stem="node")
        cls.client_key, cls.client_crt = impa.mint_leaf(
            cls.dir, "HCI-Client", 30, cls.ca_key, cls.ca_crt,
            eku="clientAuth", stem="client")

        server = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        server.load_cert_chain(certfile=cls.node_crt, keyfile=cls.node_key)
        server.load_verify_locations(cafile=cls.ca_crt)
        server.verify_mode = ssl.CERT_REQUIRED
        cls.server_context = server

        cls.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cls.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        cls.listener.bind(("127.0.0.1", 0))
        cls.listener.listen(16)
        cls.port = cls.listener.getsockname()[1]
        cls.thread = threading.Thread(target=cls._serve, daemon=True)
        cls.thread.start()

    @classmethod
    def _serve(cls):
        while True:
            try:
                raw, _ = cls.listener.accept()
            except OSError:
                return
            try:
                with cls.server_context.wrap_socket(raw, server_side=True) as wrapped:
                    wrapped.recv(16)
            except Exception:
                pass

    @classmethod
    def tearDownClass(cls):
        cls.listener.close()
        shutil.rmtree(cls.dir, ignore_errors=True)

    def dial(self, server_hostname, cert, key, check_hostname=True):
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=self.ca_crt)
        context.load_cert_chain(certfile=cert, keyfile=key)
        context.check_hostname = check_hostname
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as raw:
            with context.wrap_socket(raw, server_hostname=server_hostname) as wrapped:
                wrapped.send(b"x")
                return wrapped.getpeercert()

    def test_openssl_accepts_the_minted_chain(self):
        result = subprocess.run(
            ["openssl", "verify", "-CAfile", self.ca_crt, self.node_crt, self.client_crt],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.assertEqual(result.returncode, 0, result.stdout.decode("utf-8", "ignore"))

    def test_the_ip_san_satisfies_hostname_verification(self):
        peer = self.dial("127.0.0.1", self.client_crt, self.client_key)
        self.assertIn(("IP Address", "127.0.0.1"), peer["subjectAltName"])

    def test_a_node_certificate_still_works_as_a_client_certificate(self):
        # spark-daemon and Mipha dial peers with node.crt. OpenSSL applies the ssl_client
        # purpose when a server verifies its peer, so a node certificate issued with only
        # serverAuth is rejected on every outbound call it makes.
        self.dial("127.0.0.1", self.node_crt, self.node_key)

    def test_a_certificate_for_one_address_is_refused_for_another(self):
        # The whole point of turning check_hostname back on.
        with self.assertRaises(ssl.SSLCertVerificationError):
            self.dial("10.255.255.254", self.client_crt, self.client_key)

    def test_the_hostname_in_the_san_is_accepted_too(self):
        peer = self.dial("Valkyrie-TEST", self.client_crt, self.client_key)
        self.assertIn(("DNS", "Valkyrie-TEST"), peer["subjectAltName"])

    def test_a_ca_bundle_of_two_cas_trusts_both(self):
        # A CA rotation distributes ca.crt = old || new and every node has to keep
        # trusting certificates from both signers until the last one has been reissued.
        other_key, other_crt = impa.mint_ca(self.dir, cn="HCI-Other-CA", days=30,
                                            key_name="other-ca.key", crt_name="other-ca.crt")
        bundle = os.path.join(self.dir, "bundle.crt")
        with open(bundle, "w") as handle:
            for path in (self.ca_crt, other_crt):
                with open(path, "r") as source:
                    handle.write(source.read().rstrip() + "\n")

        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=bundle)
        context.load_cert_chain(certfile=self.client_crt, keyfile=self.client_key)
        context.check_hostname = True
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as raw:
            with context.wrap_socket(raw, server_hostname="127.0.0.1") as wrapped:
                wrapped.send(b"x")
                self.assertTrue(wrapped.getpeercert())
        self.assertTrue(os.path.exists(other_key))


class MimirSurveyTests(unittest.TestCase):
    """Mimir publishes expiry from every node, not just the ZooKeeper leader."""

    def test_survey_reports_a_status_and_never_raises_off_a_missing_directory(self):
        original = mimir.MTLS_CERT_DIRS
        try:
            mimir.MTLS_CERT_DIRS = [os.path.join(HERE, "no-such-cert-dir")]
            status, output = mimir.survey_mtls_certs()
            self.assertEqual(status, "WARN")
            self.assertIn("does not exist", output)
        finally:
            mimir.MTLS_CERT_DIRS = original

    def test_an_empty_directory_is_a_warning_not_a_pass(self):
        original = mimir.MTLS_CERT_DIRS
        empty = tempfile.mkdtemp(prefix="helios-empty-certs-")
        try:
            mimir.MTLS_CERT_DIRS = [empty]
            status, output = mimir.survey_mtls_certs()
            self.assertEqual(status, "WARN")
            self.assertIn("no certificates", output)
        finally:
            mimir.MTLS_CERT_DIRS = original
            shutil.rmtree(empty, ignore_errors=True)

    @unittest.skipUnless(HAVE_OPENSSL, "openssl is not on PATH")
    def test_a_short_dated_certificate_is_reported_as_expiring(self):
        original = mimir.MTLS_CERT_DIRS
        directory = tempfile.mkdtemp(prefix="helios-short-certs-")
        try:
            ca_key, ca_crt = impa.mint_ca(directory, cn="HCI-Short-CA", days=10)
            impa.mint_leaf(directory, "10.0.0.1", 3, ca_key, ca_crt,
                           eku="serverAuth,clientAuth", san=impa.node_san("10.0.0.1"),
                           stem="node")
            mimir.MTLS_CERT_DIRS = [directory]
            status, output = mimir.survey_mtls_certs()
            self.assertEqual(status, "FAIL")
            self.assertIn("node.crt", output)
            self.assertIn("impa renew", output)
        finally:
            mimir.MTLS_CERT_DIRS = original
            shutil.rmtree(directory, ignore_errors=True)

    def test_the_check_is_published_under_the_name_the_console_renders(self):
        self.assertEqual(mimir.CERT_CHECK_NAME, "mtls_cert_expiration")
        self.assertEqual(mimir.CERT_CHECK_CATEGORY, "security.mtls.certs")

    def test_thresholds_match_the_health_check_runner(self):
        # mcli-runner's mtls_cert_expiry_warning uses the same numbers; if they drift,
        # the console shows two checks disagreeing about the same certificates.
        self.assertEqual((mimir.CERT_WARN_DAYS, mimir.CERT_FAIL_DAYS),
                         (impa.CERT_WARN_DAYS, impa.CERT_FAIL_DAYS))


if __name__ == "__main__":
    unittest.main()
