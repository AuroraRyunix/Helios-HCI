#!/usr/bin/env python3
"""Tests for the Python console's data layer: bounded reads, and deletes that check.

Every assertion here corresponds to a way the old code told an operator something false:

  * a page load that answered with a full table scan, or with `LIMIT n` and no `WHERE` --
    which is not "the most recent n" of anything, it is the first n rows in token order;
  * a `GET` that wrote catalogue rows for files it happened to find on one node;
  * a delete that removed the database row first and then fired unchecked commands at the
    storage, so a failure left storage allocated with nothing in the UI pointing at it,
    and the operator was told it had worked;
  * a VM delete that read a placement, destroyed a guest at the address it used to be,
    and deleted the row regardless -- leaving a guest running that nothing knows about;
  * an update check that treated "this node's version could not be read" as "this node's
    version is old", and therefore offered an update no update could clear.

`FakeHydra` stands in for run_cql_query. It records every statement, which is what lets a
test assert on the *shape* of a read (bounded, per-partition) and on the *order* of a
write sequence, not just on its result.

Run with:  python -m unittest test_spectrum_data_layer
"""

import importlib.util
import io
import json
import os
import re
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_module(filename, name):
    """Import a file by path.

    Neither of these is a package, and `check-updates` is installed under a name Python
    cannot import at all. Everything at module level in both is definitions or small
    constant assignments; neither starts a server outside `__main__`.
    """
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


spectrum = load_module("spectrum_server.py", "spectrum_under_test")
check_updates = load_module("check_updates.py", "check_updates_under_test")


# -- a stand-in for Hydra ----------------------------------------------------------------

class FakeHydra:
    """Records statements and answers reads from fixtures.

    `rows_for` maps a regex to the rows a matching SELECT returns; `fail_on` maps a regex
    to the error a matching statement fails with. A statement that matches nothing
    succeeds and returns no rows, which is what an INSERT or DELETE does.
    """

    def __init__(self):
        self.statements = []
        self.rows_for = []
        self.fail_on = []

    def returns(self, pattern, rows):
        # Prepended, so a test can override an answer its setUp arranged.
        self.rows_for.insert(0, (re.compile(pattern, re.S | re.I), rows))
        return self

    def fails(self, pattern, error="Hydra is unavailable"):
        self.fail_on.insert(0, (re.compile(pattern, re.S | re.I), error))
        return self

    def __call__(self, cql, *args, **kwargs):
        self.statements.append(cql)
        for pattern, error in self.fail_on:
            if pattern.search(cql):
                return 1, "", error
        for pattern, rows in self.rows_for:
            if pattern.search(cql):
                return 0, "\n".join(json.dumps(row) for row in rows), ""
        return 0, "", ""

    def matching(self, pattern):
        rx = re.compile(pattern, re.S | re.I)
        return [s for s in self.statements if rx.search(s)]

    def writes(self):
        """Every statement that mutates. CREATE/ALTER are schema, not data."""
        return [s for s in self.statements
                if re.match(r"\s*(insert|update|delete)\b", s, re.I)]

    def index_of(self, pattern):
        rx = re.compile(pattern, re.S | re.I)
        for i, statement in enumerate(self.statements):
            if rx.search(statement):
                return i
        return -1


class Recorder:
    """A callable that records its calls and replays canned results."""

    def __init__(self, default=(0, "", "")):
        self.calls = []
        self.default = default
        self.rules = []

    def when(self, pattern, result):
        self.rules.append((re.compile(pattern, re.S | re.I), result))
        return self

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        joined = " ".join(str(a) for a in args)
        for pattern, result in self.rules:
            if pattern.search(joined):
                return result
        return self.default

    def arguments(self):
        return [" ".join(str(a) for a in args) for args, _ in self.calls]


class FakeHeaders(dict):
    """http.client.HTTPMessage's two behaviours that the handlers use."""

    def get(self, key, default=None):
        for existing, value in self.items():
            if existing.lower() == str(key).lower():
                return value
        return default

    def __contains__(self, key):
        return any(existing.lower() == str(key).lower() for existing in self.keys())


def drive(method, path, body=None, headers=None):
    """Run one request through the real handler and return (status, body).

    The handler is built without BaseHTTPRequestHandler.__init__, which would try to read
    a socket. Everything the routing and the auth guard touch is set explicitly; a
    request from 127.0.0.1 with no proxy headers authenticates as local-admin, which is
    this console's own rule.
    """
    handler = object.__new__(spectrum.SpectrumHandler)
    handler.path = path
    handler.client_address = ("127.0.0.1", 54321)
    payload = b"" if body is None else json.dumps(body).encode("utf-8")
    hdrs = FakeHeaders(headers or {})
    hdrs["Content-Length"] = str(len(payload))
    handler.headers = hdrs
    handler.rfile = io.BytesIO(payload)

    captured = {}

    def send_json(status, data):
        captured["status"] = status
        captured["body"] = data

    handler.send_json = send_json
    getattr(handler, method)()
    return captured.get("status"), captured.get("body")


class SpectrumTestCase(unittest.TestCase):
    """Restores every module global a test replaces."""

    PATCHED = ("run_cql_query", "run_lwt", "run_remote_spark", "sidon_call",
               "run_mtls_spark_api", "get_cluster_nodes", "log_catalyst_task",
               "invalidate_status_cache", "invalidate_tasks_cache")

    def setUp(self):
        self._saved = {name: getattr(spectrum, name) for name in self.PATCHED}
        self.hydra = FakeHydra()
        spectrum.run_cql_query = self.hydra
        spectrum.log_catalyst_task = lambda *a, **k: ("task-id", 0)
        spectrum.invalidate_status_cache = lambda *a, **k: None
        spectrum.invalidate_tasks_cache = lambda *a, **k: None
        # Anything a test does not deliberately arrange must not reach a real cluster.
        spectrum.run_remote_spark = Recorder((0, "", ""))
        spectrum.sidon_call = Recorder((True, {}))
        spectrum.run_mtls_spark_api = Recorder((0, {}, ""))
        spectrum.run_lwt = Recorder((True, True, {}, ""))

    def tearDown(self):
        for name, value in self._saved.items():
            setattr(spectrum, name, value)


# -- metrics -----------------------------------------------------------------------------

class MetricsReadTests(SpectrumTestCase):
    """`/api/cluster/metrics` reads one bounded partition per node.

    hydra.logos_metrics is PRIMARY KEY (node_ip, timestamp) clustered timestamp DESC with
    a 24h TTL, and logos.py writes a row per node every 30 seconds. The old endpoint ran
    `SELECT JSON * FROM hydra.logos_metrics` -- no WHERE, no LIMIT -- on every poll of
    every open tab, and the browser then kept 40 samples per host.
    """

    def setUp(self):
        super().setUp()
        spectrum.get_cluster_nodes = lambda: [
            {"hostname": "node1", "ip": "10.10.102.41"},
            {"hostname": "node2", "ip": "10.10.102.42"},
        ]
        self.hydra.returns(r"FROM hydra\.logos_metrics", [
            {"node_ip": "10.10.102.41", "timestamp": "2026-08-21 10:00:00.000+0000",
             "cpu_pct": 12.0, "mem_pct": 40.0},
        ])

    def test_one_query_per_node(self):
        rows, unread = spectrum.read_node_metrics()
        reads = self.hydra.matching(r"FROM hydra\.logos_metrics")
        self.assertEqual(len(reads), 2)
        self.assertEqual(unread, [])
        self.assertEqual(len(rows), 2)

    def test_every_read_is_a_single_partition_with_a_limit(self):
        spectrum.read_node_metrics()
        for cql in self.hydra.matching(r"FROM hydra\.logos_metrics"):
            self.assertRegex(cql, r"WHERE\s+node_ip\s*=\s*'\d+\.\d+\.\d+\.\d+'")
            self.assertRegex(cql, r"LIMIT\s+\d+")

    def test_the_unbounded_scan_is_gone(self):
        spectrum.read_node_metrics()
        for cql in self.hydra.matching(r"FROM hydra\.logos_metrics"):
            self.assertNotIn("SELECT JSON *", cql)
            self.assertIn("WHERE", cql.upper())

    def test_the_limit_is_what_the_page_actually_draws(self):
        # metrics.html slices each host's series to its last 40 points. Reading more than
        # that is rows fetched out of Hydra to be discarded in JavaScript.
        spectrum.read_node_metrics()
        for cql in self.hydra.matching(r"FROM hydra\.logos_metrics"):
            self.assertIn(f"LIMIT {spectrum.METRICS_SAMPLES_PER_NODE}", cql)

    def test_columns_are_named_rather_than_starred(self):
        spectrum.read_node_metrics()
        cql = self.hydra.matching(r"FROM hydra\.logos_metrics")[0]
        for column in ("node_ip", "timestamp", "cpu_pct", "mem_pct", "disk_iops",
                       "net_rx_kbps"):
            self.assertIn(column, cql)

    def test_a_node_whose_partition_cannot_be_read_is_named_not_dropped(self):
        # An unread node is not a node that reported nothing, and must not be drawn as
        # one. The caller gets its address so it can say so.
        self.hydra.fails(r"node_ip = '10\.10\.102\.42'", "read timeout")
        rows, unread = spectrum.read_node_metrics()
        self.assertEqual(unread, ["10.10.102.42"])
        self.assertEqual([r["node_ip"] for r in rows], ["10.10.102.41"])

    def test_a_node_address_that_is_not_an_address_is_not_interpolated(self):
        # Addresses come out of cluster.json and hydra.nodes and go back into CQL as a
        # partition key. run_cql_query() falls back to piping statement text into cqlsh
        # when Daruk is down, where a ';' would be a second statement.
        spectrum.get_cluster_nodes = lambda: [
            {"hostname": "evil", "ip": "10.0.0.1'; DROP KEYSPACE hydra; --"},
            {"hostname": "node1", "ip": "10.10.102.41"},
        ]
        spectrum.read_node_metrics()
        reads = self.hydra.matching(r"FROM hydra\.logos_metrics")
        self.assertEqual(len(reads), 1)
        self.assertNotIn("DROP KEYSPACE", reads[0])

    def test_the_endpoint_reports_which_nodes_it_could_not_read(self):
        self.hydra.fails(r"node_ip = '10\.10\.102\.42'", "read timeout")
        status, body = drive("do_GET", "/api/cluster/metrics")
        self.assertEqual(status, 200)
        self.assertEqual(body["metrics_unavailable"], ["10.10.102.42"])


class DagurRunTests(SpectrumTestCase):
    """`dagur_runs` is read per job, and "recent" means recent.

    The table is PRIMARY KEY (job_name, start_time) clustered start_time DESC. The old
    `SELECT JSON * FROM hydra.dagur_runs LIMIT 100` had no WHERE, so its rows were the
    first 100 the coordinator reached in token order -- one busy job could fill the whole
    answer while another job's runs never appeared.
    """

    def setUp(self):
        super().setUp()
        self.hydra.returns(r"FROM hydra\.dagur_schedules", [
            {"job_name": "mimir_diagnostics"},
            {"job_name": "storage_scrub"},
        ])
        self.hydra.returns(r"job_name = 'mimir_diagnostics'", [
            {"job_name": "mimir_diagnostics", "start_time": 300, "status": "SUCCESS"},
            {"job_name": "mimir_diagnostics", "start_time": 100, "status": "SUCCESS"},
        ])
        self.hydra.returns(r"job_name = 'storage_scrub'", [
            {"job_name": "storage_scrub", "start_time": 200, "status": "FAILED"},
        ])

    def test_one_bounded_read_per_job(self):
        runs, ok = spectrum.read_dagur_runs()
        self.assertTrue(ok)
        reads = self.hydra.matching(r"FROM hydra\.dagur_runs")
        self.assertEqual(len(reads), 2)
        for cql in reads:
            self.assertRegex(cql, r"WHERE\s+job_name\s*=\s*'[A-Za-z0-9_.-]+'")
            self.assertRegex(cql, r"LIMIT\s+\d+")

    def test_the_result_is_ordered_by_recency_across_jobs(self):
        runs, _ = spectrum.read_dagur_runs()
        self.assertEqual([r["start_time"] for r in runs], [300, 200, 100])

    def test_the_order_holds_for_the_timestamps_cql_actually_returns(self):
        # `SELECT JSON` renders a timestamp column as a string, not a number. Sorting
        # those as they arrive sorts strings; mixing them with an int raises. The live
        # cluster returns exactly this shape.
        self.hydra.returns(r"job_name = 'mimir_diagnostics'", [
            {"job_name": "mimir_diagnostics", "start_time": "2026-08-18 20:58:32.922Z"},
        ])
        self.hydra.returns(r"job_name = 'storage_scrub'", [
            {"job_name": "storage_scrub", "start_time": "2026-08-20 08:59:44.893Z"},
        ])
        runs, _ = spectrum.read_dagur_runs()
        self.assertEqual([r["job_name"] for r in runs],
                         ["storage_scrub", "mimir_diagnostics"])

    def test_an_unreadable_timestamp_does_not_raise(self):
        self.hydra.returns(r"job_name = 'storage_scrub'",
                           [{"job_name": "storage_scrub", "start_time": None}])
        runs, ok = spectrum.read_dagur_runs()
        self.assertTrue(ok)
        self.assertEqual(spectrum.cql_timestamp_ms(None), 0.0)
        self.assertEqual(spectrum.cql_timestamp_ms("not a timestamp"), 0.0)

    def test_the_total_is_capped(self):
        runs, _ = spectrum.read_dagur_runs(per_job=2, cap=1)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["start_time"], 300)

    def test_a_job_name_that_is_not_an_identifier_is_skipped(self):
        self.hydra.returns(r"FROM hydra\.dagur_schedules",
                           [{"job_name": "x'; DROP KEYSPACE hydra; --"}])
        spectrum.read_dagur_runs()
        self.assertEqual(self.hydra.matching(r"FROM hydra\.dagur_runs"), [])

    def test_an_unreadable_schedule_list_is_not_an_empty_history(self):
        self.hydra.fails(r"FROM hydra\.dagur_schedules")
        runs, ok = spectrum.read_dagur_runs()
        self.assertFalse(ok)
        self.assertEqual(runs, [])
        status, _ = drive("do_GET", "/api/dagur/runs")
        self.assertEqual(status, 503)


# -- the image catalogue ------------------------------------------------------------------

class ImageListTests(SpectrumTestCase):
    """`GET /api/images` is a read.

    It used to scan /var/lib/hci/aether/volumes/default-image-container and INSERT a
    catalogue row for every image-looking file, so opening the page wrote to the
    database -- from every tab, on every refresh.
    """

    def setUp(self):
        super().setUp()
        self.hydra.returns(r"FROM hydra\.valhalla_images", [
            {"name": "test.iso", "filename": "test.iso", "size_bytes": 10,
             "type": "iso", "path": "/var/lib/hci/sidon/nbd/img-test.sock", "created_at": 1},
        ])

    def test_the_page_load_writes_nothing(self):
        status, body = drive("do_GET", "/api/images")
        self.assertEqual(status, 200)
        self.assertEqual([i["name"] for i in body["images"]], ["test.iso"])
        self.assertEqual(self.hydra.writes(), [])

    def test_the_filesystem_is_not_scanned(self):
        # The scan is gone, not merely conditional on the directory existing. A listdir
        # here would still be this tier inspecting one node's storage on a page load.
        listed = []
        real_listdir = os.listdir
        os.listdir = lambda p: (listed.append(p), real_listdir(p))[1]
        try:
            drive("do_GET", "/api/images")
        finally:
            os.listdir = real_listdir
        self.assertEqual(
            [p for p in listed if "aether" in str(p) or "image-container" in str(p)], [])

    def test_an_unreadable_catalogue_is_not_an_empty_one(self):
        self.hydra.fails(r"FROM hydra\.valhalla_images", "no replicas available")
        status, body = drive("do_GET", "/api/images")
        self.assertEqual(status, 503)
        self.assertIn("no replicas available", body["error"])


class ImagePathTests(unittest.TestCase):
    """Which paths an image row may direct a delete at.

    The old code ran `rm -f {path}` with the database value interpolated straight into a
    root shell. Quoting alone is not the guard -- a correctly quoted `rm -f /etc` is
    still `rm -f /etc`. The path has to be one of the two shapes an image can have.
    """

    def test_a_vdisk_is_removed_through_sidon(self):
        self.assertEqual(
            spectrum.image_backing_kind("/var/lib/hci/sidon/nbd/img-test.sock"), "vdisk")

    def test_a_staged_file_under_the_container_root_is_a_file(self):
        self.assertEqual(
            spectrum.image_backing_kind(
                "/var/lib/hci/aether/volumes/default-image-container/test.iso"),
            "file")

    def test_a_path_outside_the_allowed_roots_is_refused(self):
        for path in ("/etc/passwd",
                     "/var/lib/hci/aether/volumes-elsewhere/x.iso",
                     "/dev/sda",
                     "relative.iso",
                     "/var/lib/hci/aether/volumes",
                     "/var/lib/hci/sidon/nbd/"):
            self.assertIsNone(spectrum.image_backing_kind(path), path)

    def test_a_traversal_segment_is_refused_even_under_an_allowed_root(self):
        self.assertIsNone(spectrum.image_backing_kind(
            "/var/lib/hci/aether/volumes/default-image-container/../../../etc/shadow"))

    def test_a_null_byte_is_refused(self):
        self.assertIsNone(spectrum.image_backing_kind(
            "/var/lib/hci/aether/volumes/x.iso\x00.png"))

    def test_a_non_string_is_refused(self):
        for value in (None, 12, [], {}):
            self.assertIsNone(spectrum.image_backing_kind(value))


class ImageDeleteTests(SpectrumTestCase):
    """The backing store goes first, checked, and the row only goes after it."""

    VDISK_ROW = {"name": "scratch.iso",
                 "path": "/var/lib/hci/sidon/nbd/img-scratch.sock"}
    FILE_ROW = {"name": "staged.iso",
                "path": "/var/lib/hci/aether/volumes/default-image-container/staged.iso"}

    def catalogue(self, row):
        self.hydra.returns(r"FROM hydra\.valhalla_images", [row])

    def test_a_failed_vdisk_delete_keeps_the_row_and_says_so(self):
        # This is the defect: the row used to be deleted first, so a storage failure left
        # the image allocated with nothing in the UI pointing at it -- and the operator
        # was told the delete worked.
        self.catalogue(self.VDISK_ROW)
        spectrum.sidon_call = Recorder((False, "vdisk is attached and still serving reads"))
        status, body = spectrum.delete_catalogue_image("scratch.iso")
        self.assertEqual(status, 500)
        self.assertIn("still serving", body["error"])
        self.assertEqual(body["catalogue_row"], "kept")
        self.assertEqual(self.hydra.matching(r"DELETE FROM hydra\.valhalla_images"), [])

    def test_the_backing_store_is_removed_before_the_row(self):
        self.catalogue(self.VDISK_ROW)
        storage = Recorder((True, {}))
        spectrum.sidon_call = storage
        status, _ = spectrum.delete_catalogue_image("scratch.iso")
        self.assertEqual(status, 200)
        # detach then delete: two calls, in that order.
        self.assertEqual([c[0][0] for c in storage.calls], ["detach", "delete"])
        self.assertEqual(storage.calls[-1][1]["vdisk_id"], "img-scratch")
        self.assertGreaterEqual(self.hydra.index_of(r"DELETE FROM hydra\.valhalla_images"), 0)

    def test_a_vdisk_is_never_removed_with_rm(self):
        # `rm` on the NBD socket removes a socket and leaves every byte of the image
        # -- and the storage it holds on every node -- allocated.
        self.catalogue(self.VDISK_ROW)
        remote = Recorder((0, "", ""))
        spectrum.run_remote_spark = remote
        spectrum.delete_catalogue_image("scratch.iso")
        self.assertEqual([a for a in remote.arguments() if "rm " in a], [])

    def test_a_vdisk_that_is_already_gone_is_not_a_failure(self):
        self.catalogue(self.VDISK_ROW)
        spectrum.sidon_call = Recorder((False, "refused: vdisk img-scratch does not exist"))
        status, _ = spectrum.delete_catalogue_image("scratch.iso")
        self.assertEqual(status, 200)
        self.assertEqual(len(self.hydra.matching(r"DELETE FROM hydra\.valhalla_images")), 1)

    def test_a_staged_file_is_removed_on_every_node_with_a_quoted_path(self):
        self.catalogue(self.FILE_ROW)
        self.hydra.returns(r"FROM hydra\.nodes",
                           [{"ip": "10.10.102.41"}, {"ip": "10.10.102.42"}])
        remote = Recorder((0, "", ""))
        spectrum.run_remote_spark = remote
        status, _ = spectrum.delete_catalogue_image("staged.iso")
        self.assertEqual(status, 200)
        self.assertEqual(len(remote.calls), 2)
        for command in remote.arguments():
            self.assertIn("rm -f -- ", command)

    def test_a_node_that_refuses_the_removal_keeps_the_row(self):
        # A copy left behind on one node is what the next upload of the same name
        # collides with, so it is not a partial success.
        self.catalogue(self.FILE_ROW)
        self.hydra.returns(r"FROM hydra\.nodes",
                           [{"ip": "10.10.102.41"}, {"ip": "10.10.102.42"}])
        spectrum.run_remote_spark = Recorder((0, "", "")).when(
            r"10\.10\.102\.42", (1, "", "Read-only file system"))
        status, body = spectrum.delete_catalogue_image("staged.iso")
        self.assertEqual(status, 500)
        self.assertIn("10.10.102.42", body["error"])
        self.assertEqual(self.hydra.matching(r"DELETE FROM hydra\.valhalla_images"), [])

    def test_a_row_pointing_outside_the_allowed_roots_is_refused_not_deleted(self):
        self.catalogue({"name": "odd.iso", "path": "/etc/passwd"})
        remote = Recorder((0, "", ""))
        spectrum.run_remote_spark = remote
        status, body = spectrum.delete_catalogue_image("odd.iso")
        self.assertEqual(status, 500)
        self.assertIn("/etc/passwd", body["error"])
        self.assertEqual(remote.calls, [])
        self.assertEqual(self.hydra.matching(r"DELETE FROM hydra\.valhalla_images"), [])

    def test_an_unknown_image_is_not_a_silent_success(self):
        self.hydra.returns(r"SELECT JSON name, path FROM hydra\.valhalla_images", [])
        status, body = spectrum.delete_catalogue_image("never-existed.iso")
        self.assertEqual(status, 404)
        self.assertIn("never-existed.iso", body["error"])
        self.assertEqual(self.hydra.writes(), [])

    def test_an_unreadable_catalogue_does_not_delete_anything(self):
        self.hydra.fails(r"SELECT JSON name, path FROM hydra\.valhalla_images")
        status, _ = spectrum.delete_catalogue_image("scratch.iso")
        self.assertEqual(status, 503)
        self.assertEqual(self.hydra.writes(), [])

    def test_a_row_with_no_path_is_deletable(self):
        # Rows the old directory scan wrote never recorded a path. There is nothing to
        # remove, and saying so beats inventing a path to delete.
        self.catalogue({"name": "ghost.iso", "path": ""})
        status, _ = spectrum.delete_catalogue_image("ghost.iso")
        self.assertEqual(status, 200)
        self.assertEqual(len(self.hydra.matching(r"DELETE FROM hydra\.valhalla_images")), 1)

    def test_the_endpoint_reports_the_failure_rather_than_answering_200(self):
        self.catalogue(self.VDISK_ROW)
        spectrum.sidon_call = Recorder((False, "sidon is not answering"))
        status, body = drive("do_POST", "/api/images/delete", {"name": "scratch.iso"})
        self.assertEqual(status, 500)
        self.assertIn("not answering", body["error"])


# -- VM delete ----------------------------------------------------------------------------

class VmDeleteTests(SpectrumTestCase):
    """A VM's row is only removed once its guest is provably gone from the host of record.

    The old sequence read host_ip, destroyed the domain there, and deleted the row
    unconditionally. A VM that migrated in between was destroyed nowhere and its row
    disappeared anyway, leaving a guest running that nothing in the cluster knows about.
    """

    VM = {"name": "web-01", "host_ip": "10.10.102.41", "state": "Running",
          "status": None, "disks_list": "disk0", "disk_path": ""}

    def setUp(self):
        super().setUp()
        self.hydra.returns(r"FROM hydra\.vms", [dict(self.VM)])

    def lwt(self, **overrides):
        """A run_lwt stand-in whose per-endpoint answers a test can override."""
        answers = {
            "/v1/vm/migrate-lock": (True, True, {}, ""),
            "/v1/vm/migrate-unlock": (True, True, {}, ""),
            "/v1/vm/set-state": (True, True, {}, ""),
        }
        answers.update(overrides)
        recorder = Recorder((True, True, {}, ""))
        for endpoint, answer in answers.items():
            recorder.when(re.escape(endpoint), answer)
        spectrum.run_lwt = recorder
        return recorder

    def endpoints_called(self, recorder):
        return [args[0] for args, _ in recorder.calls]

    def test_a_vm_that_has_moved_is_not_deleted_and_its_row_survives(self):
        # The compare-and-swap on `IF host_ip = ?` is refused: something moved the VM
        # between the read and the write. Nothing is destroyed and the row stays.
        recorder = self.lwt(**{"/v1/vm/set-state":
                               (True, False, {"host_ip": "10.10.102.42"}, "")})
        spark = Recorder((0, {}, ""))
        spectrum.run_mtls_spark_api = spark
        status, body = spectrum.delete_vm("web-01")
        self.assertEqual(status, 409)
        self.assertIn("10.10.102.42", body["error"])
        self.assertEqual(body["record"], "kept")
        self.assertEqual(self.hydra.matching(r"DELETE FROM hydra\.vms"), [])
        self.assertEqual(spark.calls, [])
        self.assertIn("/v1/vm/migrate-unlock", self.endpoints_called(recorder))

    def test_a_migration_in_flight_blocks_the_delete(self):
        recorder = self.lwt(**{"/v1/vm/migrate-lock":
                               (True, False, {"status": "migrating"}, "")})
        spark = Recorder((0, {}, ""))
        spectrum.run_mtls_spark_api = spark
        status, body = spectrum.delete_vm("web-01")
        self.assertEqual(status, 409)
        self.assertIn("migrating", body["error"])
        self.assertEqual(self.hydra.matching(r"DELETE FROM hydra\.vms"), [])
        self.assertEqual(spark.calls, [])

    def test_a_daruk_that_cannot_answer_does_not_become_a_delete(self):
        # run_lwt reports a genuine failure as ok=False. Without the lock there is no way
        # to stop a migration racing the delete, so the delete does not proceed.
        self.lwt(**{"/v1/vm/migrate-lock": (False, False, {}, "Daruk is not answering")})
        status, body = spectrum.delete_vm("web-01")
        self.assertEqual(status, 503)
        self.assertEqual(self.hydra.matching(r"DELETE FROM hydra\.vms"), [])

    def test_an_unknown_vm_is_404_and_takes_no_lock(self):
        # `UPDATE ... IF status != ?` applies against a row that does not exist and
        # creates a partial one, so migrate-lock on an unknown name would invent a VM.
        self.hydra.returns(r"FROM hydra\.vms", [])
        recorder = self.lwt()
        status, body = spectrum.delete_vm("no-such-vm")
        self.assertEqual(status, 404)
        self.assertEqual(recorder.calls, [])
        self.assertEqual(self.hydra.writes(), [])

    def test_an_unreadable_vms_table_is_not_a_missing_vm(self):
        self.hydra.fails(r"FROM hydra\.vms")
        recorder = self.lwt()
        status, _ = spectrum.delete_vm("web-01")
        self.assertEqual(status, 503)
        self.assertEqual(recorder.calls, [])

    def test_an_invalid_name_never_reaches_a_query(self):
        status, _ = spectrum.delete_vm("web 01; DROP KEYSPACE hydra")
        self.assertEqual(status, 400)
        self.assertEqual(self.hydra.statements, [])

    def test_a_destroy_that_fails_keeps_the_row_and_restores_the_state(self):
        recorder = self.lwt()
        spectrum.run_mtls_spark_api = Recorder((0, {}, "")).when(
            r"/power", (-1, {"error": "Failed to destroy domain: internal error"}, ""))
        status, body = spectrum.delete_vm("web-01")
        self.assertEqual(status, 500)
        self.assertIn("internal error", body["error"])
        self.assertEqual(self.hydra.matching(r"DELETE FROM hydra\.vms"), [])
        # The state written while the delete was running is put back.
        states = [kwargs_or_args[1].get("state")
                  for kwargs_or_args, _ in recorder.calls
                  if kwargs_or_args[0] == "/v1/vm/set-state"]
        self.assertEqual(states, [spectrum.VM_DELETING_STATE, "Running"])

    def test_a_domain_that_is_already_undefined_is_not_a_failure(self):
        self.lwt()
        spectrum.run_mtls_spark_api = Recorder((0, {}, "")).when(
            r"/power", (-1, {"error": "error: failed to get domain 'web-01': not found"}, ""))
        status, _ = spectrum.delete_vm("web-01")
        self.assertEqual(status, 200)

    def test_a_failed_disk_delete_keeps_the_row(self):
        # Same reasoning as the image catalogue: unchecked, the row goes and the LINSTOR
        # resources stay, holding storage nothing in the UI can reach.
        self.lwt()
        spectrum.sidon_call = Recorder((False, "vdisk is attached"))
        status, body = spectrum.delete_vm("web-01")
        self.assertEqual(status, 500)
        self.assertIn("is attached", body["error"])
        self.assertEqual(self.hydra.matching(r"DELETE FROM hydra\.vms"), [])

    def test_the_happy_path_destroys_on_the_host_of_record_then_deletes(self):
        recorder = self.lwt()
        spark = Recorder((0, {}, ""))
        spectrum.run_mtls_spark_api = spark
        storage = Recorder((True, {}))
        spectrum.sidon_call = storage
        status, _ = spectrum.delete_vm("web-01")
        self.assertEqual(status, 200)
        # The destroy went to the host the compare-and-swap confirmed.
        self.assertTrue(all("10.10.102.41" in a for a in spark.arguments()))
        self.assertIn("web-01-disk0", [c[1].get("vdisk_id") for c in storage.calls])
        self.assertEqual(len(self.hydra.matching(r"DELETE FROM hydra\.vms")), 1)
        # The row carried the lock, so there is nothing left to unlock.
        self.assertNotIn("/v1/vm/migrate-unlock", self.endpoints_called(recorder))

    def test_the_placement_is_re_read_under_the_lock(self):
        # A migration may have committed between the first read and the lock. The
        # placement used for the destroy is the one read after the lock was taken.
        reads = []
        original = self.hydra.__call__

        def counting(cql, *a, **k):
            if re.search(r"SELECT JSON name, host_ip", cql, re.I):
                reads.append(cql)
            return original(cql, *a, **k)

        spectrum.run_cql_query = counting
        self.lwt()
        spectrum.delete_vm("web-01")
        self.assertEqual(len(reads), 2)

    def test_a_vm_with_no_host_of_record_destroys_nothing(self):
        # Hydra places it nowhere. Guessing a host would destroy a same-named guest
        # belonging to nobody.
        row = dict(self.VM)
        row["host_ip"] = ""
        self.hydra.returns(r"FROM hydra\.vms", [row])
        self.lwt()
        spark = Recorder((0, {}, ""))
        spectrum.run_mtls_spark_api = spark
        status, _ = spectrum.delete_vm("web-01")
        self.assertEqual(status, 200)
        self.assertEqual([a for a in spark.arguments() if "/power" in a], [])

    def test_the_endpoint_passes_the_refusal_through(self):
        self.lwt(**{"/v1/vm/set-state": (True, False, {"host_ip": "10.10.102.42"}, "")})
        status, body = drive("do_POST", "/api/vms/delete", {"name": "web-01"})
        self.assertEqual(status, 409)
        self.assertIn("10.10.102.42", body["error"])


# -- the update check ----------------------------------------------------------------------

class CurrentVersionTests(unittest.TestCase):
    """Reading this node's build, and admitting when it could not be read."""

    def hylia(self, contents):
        directory = tempfile.mkdtemp()
        # SourceFileLoader may leave a __pycache__ beside the fixture.
        self.addCleanup(shutil.rmtree, directory, True)
        path = os.path.join(directory, "hylia")
        with open(path, "w", encoding="utf-8") as f:
            f.write(contents)
        return path

    def test_a_build_tag_is_read(self):
        path = self.hylia('__build__ = "1.2.7-b9001"\n')
        self.assertEqual(check_updates.read_current_version(path), "1.2.7-b9001")

    def test_a_module_that_will_not_execute_falls_back_to_reading_the_tag(self):
        path = self.hylia('__build__ = "1.2.7-b9001"\nthis is not python\n')
        self.assertEqual(check_updates.read_current_version(path), "1.2.7-b9001")

    def test_an_unreadable_hylia_is_none_not_a_version(self):
        # Neither executable nor parseable: the version is genuinely unknown, and the
        # answer says so rather than defaulting to the pre-build-tag fallback.
        path = self.hylia("this is not python and has no build tag\n")
        self.assertIsNone(check_updates.read_current_version(path))

    def test_a_hylia_that_is_neither_installed_nor_importable_is_none(self):
        # The installed layout is a file at HYLIA_PATH; the container layout is an
        # importable module beside this script. With neither available the version is
        # unknown, and the old code's answer was FALLBACK_BUILD -- a version string,
        # unequal to every release, forever.
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        missing = os.path.join(directory, "hylia")

        previous = sys.modules.get("hylia", "absent")
        sys.modules["hylia"] = None          # makes `import hylia` raise ImportError

        def restore():
            if previous == "absent":
                sys.modules.pop("hylia", None)
            else:
                sys.modules["hylia"] = previous

        self.addCleanup(restore)

        result = check_updates.read_current_version(missing)
        self.assertIsNone(result)
        self.assertNotEqual(result, check_updates.FALLBACK_BUILD)


class UpdateDecisionTests(unittest.TestCase):
    """Unknown never counts as a mismatch.

    Every comparison in the check is an inequality against a release version, so any
    stand-in value for "could not read" is unequal to the release forever: the console
    offers an update that installing cannot clear, because the next run cannot read the
    version it just installed either.
    """

    def test_an_unreadable_current_version_does_not_report_an_update(self):
        available, notes = check_updates.decide_update_available(
            "1.3.0-b5000", None, {}, {})
        self.assertFalse(available)
        self.assertTrue(notes)
        self.assertIn("could not be read", notes[0])

    def test_the_fallback_build_is_not_substituted_for_an_unreadable_version(self):
        # The specific old behaviour: current_version stayed at FALLBACK_BUILD, which is
        # unequal to every release the server will ever publish.
        available, _ = check_updates.decide_update_available(
            check_updates.FALLBACK_BUILD, None, {}, {})
        self.assertFalse(available)

    def test_a_matching_version_reports_no_update(self):
        available, notes = check_updates.decide_update_available(
            "1.3.0-b5000", "1.3.0-b5000", {}, {})
        self.assertFalse(available)
        self.assertEqual(notes, [])

    def test_a_differing_version_still_reports_an_update(self):
        available, notes = check_updates.decide_update_available(
            "1.3.0-b5000", "1.2.9-b4999", {}, {})
        self.assertTrue(available)
        self.assertEqual(notes, [])

    def test_a_component_that_could_not_be_asked_is_not_a_mismatch(self):
        inventory = {"node1": {"ip": "10.10.102.41",
                               "versions": {"spark": check_updates.VERSION_UNREADABLE}}}
        available, notes = check_updates.decide_update_available(
            "1.3.0-b5000", "1.3.0-b5000", {"spark": "1.3.0-b5000"}, inventory)
        self.assertFalse(available)
        self.assertIn("node1/spark", notes[0])

    def test_a_component_that_answered_and_differs_is_a_mismatch(self):
        inventory = {"node1": {"ip": "10.10.102.41",
                               "versions": {"spark": "1.2.9-b4999"}}}
        available, notes = check_updates.decide_update_available(
            "1.3.0-b5000", "1.3.0-b5000", {"spark": "1.3.0-b5000"}, inventory)
        self.assertTrue(available)
        self.assertEqual(notes, [])

    def test_a_component_that_is_not_installed_is_still_a_mismatch(self):
        # "Not Installed" is an answer about the component, unlike "N/A".
        inventory = {"node1": {"ip": "10.10.102.41",
                               "versions": {"spark": "Not Installed"}}}
        available, _ = check_updates.decide_update_available(
            "1.3.0-b5000", "1.3.0-b5000", {"spark": "1.3.0-b5000"}, inventory)
        self.assertTrue(available)

    def test_an_untagged_component_is_compared_as_the_pre_tag_build(self):
        inventory = {"node1": {"ip": "10.10.102.41", "versions": {"spark": "Unknown"}}}
        available, _ = check_updates.decide_update_available(
            "1.3.0-b5000", "1.3.0-b5000",
            {"spark": check_updates.FALLBACK_BUILD}, inventory)
        self.assertFalse(available)


if __name__ == "__main__":
    unittest.main()
