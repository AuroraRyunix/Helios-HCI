#!/usr/bin/env python3
"""A stale token must not outrank a valid one.

The console page puts whatever the browser's localStorage holds into ?token=. After the cluster was
rebuilt that is a token from a session that no longer exists, and is_authenticated took the first
token it found -- the query parameter, ahead of the cookie -- and stopped, so a request that also
carried a perfectly good session_id cookie was refused as signed out ("Authentication Failed" on the
guest console). It now tries every token the request offers and accepts the first live session.

spectrum_server.py opens a database and binds a socket when imported, so the two functions are
extracted and run against stubs.

Run with:  python -m unittest test_auth_stale_token
"""

import ast
import http.cookies
import io
import os
import re
import time
import unittest
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
GOOD = "a" * 64
STALE = "b" * 64


def load_functions(db):
    with io.open(os.path.join(HERE, "spectrum_server.py"), encoding="utf-8") as handle:
        source = handle.read()
    tree = ast.parse(source)
    wanted = {"is_authenticated", "_session_user", "is_valid_session_token"}
    pieces = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            pieces.append(ast.get_source_segment(source, node))
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "_SESSION_TOKEN_RE":
            pieces.append(ast.get_source_segment(source, node))

    def run_cql_query(cql):
        for token, user in db.items():
            if "'%s'" % token in cql:
                return 0, "username\n-------\n%s\n\n(1 rows)" % user, ""
        return 0, "username\n-------\n\n(0 rows)", ""

    namespace = {"SESSION_CACHE": {}, "SESSION_CACHE_TTL": 10.0, "time": time, "http": http,
                 "re": re, "urllib": urllib, "run_cql_query": run_cql_query}
    exec("\n\n".join(pieces), namespace)
    return namespace


class Handler(object):
    def __init__(self, path="/api/vms/console/token?name=x&type=vnc", cookie=None, bearer=None):
        self.path = path
        self.client_address = ("10.0.0.9", 55555)
        self.headers = {"X-Forwarded-For": "1.1.1.1"}
        if cookie:
            self.headers["Cookie"] = "session_id=" + cookie
        if bearer:
            self.headers["Authorization"] = "Bearer " + bearer


class AStaleTokenDoesNotOutrankAValidOne(unittest.TestCase):
    def setUp(self):
        self.ns = load_functions({GOOD: "alice"})

    def check(self, handler):
        ok = self.ns["is_authenticated"](handler)
        return ok, getattr(handler, "current_user", None)

    def test_a_stale_query_token_with_a_good_cookie_is_accepted(self):
        handler = Handler(path="/api/vms/console/token?name=x&type=vnc&token=" + STALE, cookie=GOOD)
        self.assertEqual(self.check(handler), (True, "alice"))

    def test_a_good_query_token_with_a_stale_cookie_is_accepted(self):
        handler = Handler(path="/api/x?token=" + GOOD, cookie=STALE)
        self.assertEqual(self.check(handler), (True, "alice"))

    def test_an_empty_query_token_falls_through_to_the_cookie(self):
        handler = Handler(path="/api/x?name=a&token=", cookie=GOOD)
        self.assertEqual(self.check(handler), (True, "alice"))

    def test_a_stale_bearer_with_a_good_cookie_is_accepted(self):
        self.assertEqual(self.check(Handler(cookie=GOOD, bearer=STALE)), (True, "alice"))


class TheRefusalsAreStillRefusals(unittest.TestCase):
    def setUp(self):
        self.ns = load_functions({GOOD: "alice"})

    def test_only_stale_tokens_are_refused(self):
        handler = Handler(path="/api/x?token=" + STALE, cookie="c" * 64)
        self.assertFalse(self.ns["is_authenticated"](handler))

    def test_no_token_at_all_is_refused(self):
        self.assertFalse(self.ns["is_authenticated"](Handler(path="/api/x")))

    def test_a_malformed_token_never_reaches_the_database(self):
        queried = []
        ns = load_functions({})
        original = ns["run_cql_query"]
        ns["run_cql_query"] = lambda cql: queried.append(cql) or original(cql)
        handler = Handler(path="/api/x?token=%27%20OR%201%3D1--", cookie="' OR 1=1--")
        self.assertFalse(ns["is_authenticated"](handler))
        self.assertEqual(queried, [], "an invalid token was interpolated into a query")

    def test_loopback_without_proxy_headers_is_still_local_admin(self):
        handler = Handler(path="/api/x")
        handler.client_address = ("127.0.0.1", 1)
        handler.headers = {}
        self.assertTrue(self.ns["is_authenticated"](handler))
        self.assertEqual(handler.current_user, "local-admin")


if __name__ == "__main__":
    unittest.main()
