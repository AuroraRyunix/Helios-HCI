#!/usr/bin/env python3
"""`valcli storage.list` must never draw a node whose sidon is not answering as online.

During a rolling restart of sidon the listing once showed a node as "online, 0.0 GiB". The
cause was not in sidon and not in the timing of anything: while the daemon starts its control
socket does not exist (it is bound only after the disks are mounted), spark answers the
capacity request with HTTP 503 and `{"error": ..., "kind": "io"}`, `run_mtls_spark_api`
returns that body with rc 0 -- deliberately, so a 409 refusal keeps its explanation -- and the
table builder read `total_bytes` out of it with `or 0`. An error document is a dict, so it
passed the "is this an answer" test.

These tests drive the real `cmd_storage_list` with the transport stubbed at `urlopen`, so
they exercise exactly the path that produced the lie and do not depend on how the helper
that fixed it is named.

Run with:  python -m unittest test_storage_list_not_ready
"""

import contextlib
import importlib.util
import io
import json
import os
import sys
import unittest
import urllib.error
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name, alias):
    if HERE not in sys.path:
        sys.path.insert(0, HERE)
    spec = importlib.util.spec_from_file_location(alias, os.path.join(HERE, name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


valcli = load("valcli.py", "valcli_under_not_ready_test")

GIB = 1024 ** 3


class _Response(io.BytesIO):
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def capacity_document(total=450 * GIB):
    return {"ok": True, "node": "node-a", "total_bytes": total,
            "available_bytes": total // 2, "egroup_count": 3, "journal_bytes": 0,
            "disks": [], "absent_disks": []}


def answer_200(document):
    return lambda *a, **k: _Response(json.dumps(document).encode())


def answer_http(code, document):
    def fail(*a, **k):
        raise urllib.error.HTTPError("https://x", code, "x", {},
                                     io.BytesIO(json.dumps(document).encode()))
    return fail


def answer_dead(*a, **k):
    raise urllib.error.URLError("connection refused")


def extent_store_table(urlopen):
    """Run the real command and return the printed Extent Store section."""
    out = io.StringIO()
    with mock.patch.object(valcli, "run_cql_query", return_value=(0, "", "")), \
            mock.patch.object(valcli.ssl, "create_default_context"), \
            mock.patch.object(valcli.urllib.request, "urlopen", side_effect=urlopen), \
            contextlib.redirect_stdout(out):
        valcli.cmd_storage_list()
    text = out.getvalue()
    return text.split("=== Extent Store ===", 1)[1].split("=== Vdisks ===", 1)[0]


class ExtentStoreState(unittest.TestCase):
    def test_sidon_down_is_not_ready_never_online_with_zero(self):
        # What spark sends while sidon has no control socket: observed on the lab.
        table = extent_store_table(answer_http(503, {
            "error": "sidon control socket /run/sidon/control.sock is unreachable: "
                     "[Errno 2] No such file or directory",
            "kind": "io"}))
        self.assertNotIn("online", table)
        self.assertNotIn("0.0 GiB", table)
        self.assertIn("not ready", table)

    def test_a_node_that_answers_nothing_is_unreachable(self):
        table = extent_store_table(answer_dead)
        self.assertIn("unreachable", table)
        self.assertNotIn("online", table)

    def test_an_answer_with_no_capacity_is_not_ready_not_zero(self):
        table = extent_store_table(answer_200({"ok": True, "node": "node-a",
                                               "total_bytes": 0, "available_bytes": 0}))
        self.assertNotIn("online", table)
        self.assertNotIn("0.0 GiB", table)
        self.assertIn("not ready", table)

    def test_a_refusal_is_an_error_with_what_it_said(self):
        table = extent_store_table(answer_http(409, {"error": "refused: nope",
                                                     "kind": "refused"}))
        self.assertNotIn("online", table)
        self.assertIn("refused: nope", table)

    def test_a_real_answer_is_still_online_with_its_real_capacity(self):
        table = extent_store_table(answer_200(capacity_document()))
        self.assertIn("online", table)
        self.assertIn("450.0 GiB", table)
        self.assertNotIn("not ready", table)


class Wiring(unittest.TestCase):
    def test_storage_list_reads_the_status_not_only_the_body(self):
        # The 503 body is a dict. Anything that decides "answered" from the body alone is the
        # defect again.
        with io.open(os.path.join(HERE, "valcli.py"), encoding="utf-8") as handle:
            source = handle.read()
        start = source.index("def cmd_storage_list")
        body = source[start:source.index("def cmd_db_print")]
        self.assertIn("run_mtls_spark_api_full", body)
        self.assertNotIn('body.get("total_bytes") or 0', body)

    def test_phoenix_keeps_a_starting_node_apart_from_an_unreachable_one(self):
        with io.open(os.path.join(HERE, "spectrum_phx", "lib", "spectrum_phx", "storage.ex"),
                     encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn("node_status({503", source)
        self.assertIn(":starting", source)


if __name__ == "__main__":
    unittest.main()
