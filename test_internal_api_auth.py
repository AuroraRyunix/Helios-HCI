#!/usr/bin/env python3
"""Tests that the internal control APIs are not reachable without a certificate.

`catalyst.py` (9091) and `vali.py` (9095) bind 0.0.0.0 under `Network=host`. Catalyst
dispatches cluster work -- VM start, stop, migrate -- and both checked neither a
credential nor a source address, so on any network the cluster could reach they were
unauthenticated remote-control interfaces for every guest.

These are source-level assertions rather than live ones: the property is "no caller
speaks plain HTTP to these ports and no server accepts an unauthenticated client", and
that is a property of the tree, not of one running cluster. A single call site left on
http:// is both a broken caller and, if the server were ever relaxed, a hole.

Run with:  python -m unittest test_internal_api_auth
"""

import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

# Every daemon that might talk to Catalyst or Vali.
CALLERS = [
    "spectrum_server.py", "vali.py", "mipha.py", "dagur.py", "mimir.py",
    "lanayru.py", "catalyst.py", "cluster_new.py", "spark_daemon_decoded.py",
    "valcli.py", "hylia.py",
]

INTERNAL_PORTS = ("9091", "9095")


def read(name):
    path = os.path.join(HERE, name)
    if not os.path.exists(path):
        return None
    return io.open(path, encoding="utf-8").read()


class PlainHttpTests(unittest.TestCase):
    def test_no_daemon_calls_an_internal_api_over_plain_http(self):
        offenders = []
        for name in CALLERS:
            source = read(name)
            if source is None:
                continue
            for number, line in enumerate(source.splitlines(), 1):
                if "http://" not in line:
                    continue
                if any(port in line for port in INTERNAL_PORTS):
                    offenders.append("%s:%d: %s" % (name, number, line.strip()[:90]))
        self.assertEqual(
            offenders, [],
            "these call an internal control API over plain HTTP:\n  " + "\n  ".join(offenders))


class ServerTests(unittest.TestCase):
    """The listeners themselves must demand a client certificate."""

    def _asserts_mutual_tls(self, name):
        source = read(name)
        self.assertIsNotNone(source, name + " is missing")
        self.assertIn("ssl.CERT_REQUIRED", source,
                      name + " does not require a client certificate")
        self.assertIn("load_verify_locations", source,
                      name + " does not pin the cluster CA, so it would trust any CA")
        self.assertIn("Purpose.CLIENT_AUTH", source,
                      name + " does not build a server-side context")
        return source

    def test_catalyst_requires_a_client_certificate(self):
        source = self._asserts_mutual_tls("catalyst.py")
        # The handshake must happen per connection in the worker thread, not on the
        # listening socket, or one slow client stalls every other connection.
        self.assertIn("wrap_socket(self.request, server_side=True)", source)

    def test_catalyst_refuses_to_start_without_its_certificates(self):
        # Falling back to plain HTTP when a certificate is missing would reopen the hole
        # precisely when something is already wrong.
        source = read("catalyst.py")
        self.assertRegex(
            source,
            r"if not os\.path\.exists\(path\):[\s\S]{0,300}?sys\.exit\(1\)",
            "catalyst does not exit when its certificates are absent")


class ContextTests(unittest.TestCase):
    def test_callers_present_a_certificate_rather_than_only_trusting_the_ca(self):
        # load_verify_locations alone authenticates the *server*. Without a cert chain
        # the caller has nothing to present and the handshake fails at the far end.
        for name in ("dagur.py", "lanayru.py", "spectrum_server.py"):
            source = read(name)
            if source is None or not any(p in source for p in INTERNAL_PORTS):
                continue
            with self.subTest(caller=name):
                self.assertIn("load_cert_chain", source,
                              name + " builds no client certificate chain")


if __name__ == "__main__":
    unittest.main()
