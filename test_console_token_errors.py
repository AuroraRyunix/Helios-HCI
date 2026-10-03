#!/usr/bin/env python3
"""The console page said "Authentication Failed" for every reason a console could not open.

GET /api/vms/console/token answered 500 "Could not resolve VM console port" for a VM that was not
running, for one that had just started and whose graphics device was not listening yet, and for a
protocol mismatch; vnc_auto.html turned all of them into "Authentication Failed", which sent people
to check their login. Now the endpoint says which it is, waits briefly for a VM that is recorded as
running, and the page reports the server's reason and keeps "Authentication Failed" for a real 401.

Run with:  python -m unittest test_console_token_errors
"""

import io
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def handler():
    text = read("spectrum_server.py")
    start = text.index('elif path == "/api/vms/console/token":')
    return text[start:text.index('elif path == "/api/vms/console/ws":', start)]


class TheEndpointSaysWhy(unittest.TestCase):
    def test_a_vm_that_is_not_running_is_a_409_naming_the_fix(self):
        body = handler()
        self.assertIn("is not running; start it to open its console", body)
        self.assertIn("self.send_json(409", body)

    def test_a_protocol_mismatch_is_a_409_naming_both_protocols(self):
        self.assertIn("console, not {console_type}", handler())

    def test_a_running_vm_without_a_console_is_a_503_not_a_generic_500(self):
        body = handler()
        self.assertIn("self.send_json(503", body)
        self.assertNotIn("self.send_json(500", body)

    def test_a_just_started_vm_gets_a_few_attempts_and_others_get_one(self):
        body = handler()
        self.assertIn('attempts = 6 if recorded_state == "running" else 1', body)
        self.assertIn("time.sleep(2)", body)


class ThePageReportsTheReason(unittest.TestCase):
    def test_only_a_401_is_called_an_authentication_failure(self):
        page = read("static/vnc_auto.html")
        self.assertIn("err.status === 401", page)
        self.assertIn('showVncDisconnected("Console Unavailable", err.message)', page)
        self.assertNotIn("Failed to fetch VNC console credentials", page)

    def test_the_servers_message_reaches_the_page(self):
        page = read("static/vnc_auto.html")
        self.assertIn("body.error", page)


if __name__ == "__main__":
    unittest.main()
