#!/usr/bin/env python3
"""`valcli storage.benchmark`: steady-state numbers, not one timing.

The benchmark this replaced wrote 64 MiB in a single request into a brand-new 100 MiB vdisk
and printed qemu-io's own line. 64 MiB landed exactly on Sidon's journal high-water mark, so
the drain ran inside that one write and the figure it printed (~14 MiB/s) was a drain divided
into 64 MiB. It said nothing about what a guest streaming data, or issuing small synchronous
writes, would see.

What these tests hold: each workload is its own labelled line; the vdisk is several times the
high-water mark so drains happen during the run; ranges do not overlap, so no phase measures
another's leftovers; a write phase can report its sustained rate once the drain it caused has
finished; and the throwaway vdisk is created, detached and deleted on every path.

Run with:  python -m unittest test_storage_benchmark
"""

import ast
import io
import os
import re
import subprocess
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
VALCLI = os.path.join(HERE, "valcli.py")

NAMES = {
    "MIB", "BENCH_VDISK_BYTES", "BENCH_WARMUP", "BENCH_PHASES", "BENCH_PATTERN",
    "_bench_run", "_bench_settle", "_bench_line", "_bench_verify", "cmd_storage_benchmark",
}

# Sidon's default journal high-water mark (SIDON_HIGH_WATER), which the vdisk must exceed by
# enough that drains are part of the run.
HIGH_WATER = 64 * 1024 * 1024


def load():
    """Compile just the benchmark out of valcli.py, which does real work at import."""
    with io.open(VALCLI, encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=VALCLI)
    body, got = [], set()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in NAMES:
            body.append(node)
            got.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in NAMES:
                    body.append(node)
                    got.add(target.id)
    missing = NAMES - got
    assert not missing, "valcli.py no longer defines %s" % sorted(missing)
    ns = {"subprocess": subprocess, "re": re, "time": time, "os": os, "json": __import__("json")}
    exec(compile(ast.Module(body=body, type_ignores=[]), VALCLI, "exec"), ns)
    return ns


class Completed(object):
    def __init__(self, stdout="", returncode=0, stderr=""):
        self.stdout, self.returncode, self.stderr = stdout, returncode, stderr


class Layout(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.b = load()

    def test_the_vdisk_is_several_times_the_journal_high_water_mark(self):
        # Smaller than that and the whole run fits in the journal: it measures the journal and
        # nothing else, which is the defect this exists to fix.
        self.assertGreaterEqual(self.b["BENCH_VDISK_BYTES"], 4 * HIGH_WATER)

    def test_the_workloads_the_operator_asked_for_each_have_a_line(self):
        labels = [p[0] for p in self.b["BENCH_PHASES"]]
        wanted = {
            "sequential write at 1M blocks": lambda p: p[1] == "write" and p[2] == 1 << 20 and p[4] == 1,
            "sync-heavy 4k writes": lambda p: p[1] == "write" and p[2] == 4096 and p[4] == 1,
            "sequential read": lambda p: p[1] == "read",
            "writes at a queue depth above 1": lambda p: p[1] == "write" and p[4] > 1,
        }
        for what, test in wanted.items():
            self.assertTrue(any(test(p) for p in self.b["BENCH_PHASES"]), what)
        self.assertEqual(len(labels), len(set(labels)), "every line is separately labelled")

    def test_at_least_two_queue_depths_are_measured(self):
        depths = {p[4] for p in self.b["BENCH_PHASES"] if p[1] == "write" and p[2] == 1 << 20}
        self.assertGreaterEqual(len(depths - {1}), 2)

    def test_no_phase_touches_another_phases_range_and_all_fit_in_the_vdisk(self):
        size = self.b["BENCH_VDISK_BYTES"]
        writes = [self.b["BENCH_WARMUP"]] + [p for p in self.b["BENCH_PHASES"] if p[1] == "write"]
        spans = []
        for label, _direction, block, count, _depth, offset in writes:
            end = offset + block * count
            self.assertLessEqual(end, size, "%s runs past the end of the vdisk" % label)
            spans.append((offset, end, label))
        spans.sort()
        for (s1, e1, l1), (s2, _e2, l2) in zip(spans, spans[1:]):
            self.assertLessEqual(e1, s2, "%s overlaps %s" % (l1, l2))

    def test_the_read_phase_reads_exactly_what_the_sequential_write_phase_wrote(self):
        phases = self.b["BENCH_PHASES"]
        seq = [p for p in phases if p[1] == "write" and p[2] == 1 << 20 and p[4] == 1][0]
        read = [p for p in phases if p[1] == "read"][0]
        self.assertEqual((read[2], read[3], read[5]), (seq[2], seq[3], seq[5]))


class Run(unittest.TestCase):
    def setUp(self):
        self.b = load()
        self.calls = []
        self.reply = Completed("Run completed in 1.500 seconds.\n")
        outer = self

        def fake_run(cmd, **kwargs):
            outer.calls.append(cmd)
            return outer.reply
        self.b["subprocess"] = type("S", (), {"run": staticmethod(fake_run), "PIPE": -1, "STDOUT": -2})

    def test_a_write_run_names_its_size_depth_count_offset_and_pattern(self):
        seconds = self.b["_bench_run"]("nbd+unix:///v?socket=/s", "write", 4096, 400, 8, 1 << 20)
        self.assertEqual(seconds, 1.5)
        cmd = self.calls[0]
        self.assertEqual(cmd[:3], ["qemu-img", "bench", "-f"])
        flags = dict(zip(cmd, cmd[1:]))
        self.assertEqual((flags["-s"], flags["-S"], flags["-c"], flags["-d"], flags["-o"]),
                         ("4096", "4096", "400", "8", str(1 << 20)))
        self.assertIn("-w", cmd)
        self.assertEqual(flags["--pattern"], str(self.b["BENCH_PATTERN"]))
        self.assertEqual(cmd[-1], "nbd+unix:///v?socket=/s")

    def test_a_read_run_does_not_write(self):
        self.b["_bench_run"]("nbd+unix:///v?socket=/s", "read", 1 << 20, 96, 1, 0)
        self.assertNotIn("-w", self.calls[0])

    def test_a_failed_run_is_no_result_not_a_zero(self):
        self.reply = Completed("", returncode=1, stderr="Could not open image")
        self.assertIsNone(self.b["_bench_run"]("u", "write", 4096, 1, 1, 0))
        self.reply = Completed("no timing line here\n")
        self.assertIsNone(self.b["_bench_run"]("u", "write", 4096, 1, 1, 0))


class Lines(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.b = load()

    def test_a_depth_one_line_gives_rate_iops_and_latency(self):
        line = self.b["_bench_line"]("sync write 4k qd1", 4096, 400, 1, 2.0)
        self.assertIn("sync write 4k qd1", line)
        self.assertIn("200.0 IOPS", line)
        self.assertIn("5.00 ms/op latency", line)
        self.assertIn("0.8 MiB/s", line)

    def test_a_deeper_line_does_not_call_the_completion_interval_a_latency(self):
        line = self.b["_bench_line"]("write 1M qd16", 1 << 20, 32, 16, 1.0)
        self.assertIn("32.0 MiB/s", line)
        self.assertIn("completion interval", line)
        self.assertNotIn("latency", line)

    def test_the_sustained_rate_appears_only_when_a_drain_outlived_the_write(self):
        quiet = self.b["_bench_line"]("w", 1 << 20, 96, 1, 3.0, settle=0.01)
        self.assertNotIn("sustained", quiet)
        tail = self.b["_bench_line"]("w", 1 << 20, 96, 1, 3.0, settle=1.0)
        self.assertIn("sustained incl. drain tail: 24.0 MiB/s (+1.00s)", tail)

    def test_no_timing_is_a_line_that_says_so(self):
        self.assertIn("no result", self.b["_bench_line"]("w", 1 << 20, 1, 1, None))


class Settle(unittest.TestCase):
    def setUp(self):
        self.b = load()

    def _with(self, answers):
        seen = []

        def api(ip, path, payload, method="POST"):
            seen.append(payload)
            return answers.pop(0) if answers else (0, {"draining": False}, "")
        self.b["run_mtls_spark_api"] = api
        self.b["time"] = type("T", (), {
            "monotonic": staticmethod(time.monotonic), "sleep": staticmethod(lambda s: None)})
        return seen

    def test_it_waits_while_sidon_reports_a_drain_and_stops_when_it_does_not(self):
        seen = self._with([(0, {"draining": True}, ""), (0, {"draining": True}, ""),
                           (0, {"draining": False}, "")])
        self.b["_bench_settle"]("bench-temp-x")
        self.assertEqual(len(seen), 3)
        self.assertTrue(all(p == {"op": "status", "vdisk_id": "bench-temp-x"} for p in seen))

    def test_a_daemon_that_predates_the_field_has_nothing_to_wait_for(self):
        # Before the drain had its own thread it ran inside the write, so by the time the
        # write returned it was already over.
        seen = self._with([(0, {"journal_bytes": 5}, "")])
        self.b["_bench_settle"]("v")
        self.assertEqual(len(seen), 1)

    def test_a_failing_status_call_does_not_hang_the_benchmark(self):
        seen = self._with([(-1, {}, "unreachable")])
        self.b["_bench_settle"]("v")
        self.assertEqual(len(seen), 1)


class Flow(unittest.TestCase):
    """The throwaway-vdisk lifecycle, with the API and qemu stubbed out."""

    def setUp(self):
        self.b = load()
        self.ops = []
        self.attach_ok = True
        self.out = []
        outer = self

        def api(ip, path, payload, method="POST"):
            outer.ops.append(payload["op"])
            op = payload["op"]
            if op == "attach":
                if not outer.attach_ok:
                    return (1, {"error": "no"}, "refused")
                return (0, {"socket": "/run/x.sock"}, "")
            if op == "status":
                return (0, {"draining": False}, "")
            if op == "create":
                outer.created = payload
            return (0, {"ok": True}, "")

        def fake_run(cmd, **kwargs):
            outer.ops.append(cmd[0])
            if cmd[0] == "qemu-img":
                return Completed("Run completed in 0.500 seconds.\n")
            return Completed("read 100/100 bytes\n")
        self.b["run_mtls_spark_api"] = api
        self.b["default_container"] = lambda: "default-pool"
        self.b["subprocess"] = type("S", (), {"run": staticmethod(fake_run), "PIPE": -1, "STDOUT": -2})
        self.b["time"] = type("T", (), {
            "monotonic": staticmethod(time.monotonic), "sleep": staticmethod(lambda s: None)})
        self.b["print"] = lambda *a, **k: outer.out.append(" ".join(str(x) for x in a))

    def test_it_creates_a_vdisk_of_the_benchmark_size_runs_every_phase_and_cleans_up(self):
        self.b["cmd_storage_benchmark"]("default-pool")
        self.assertEqual(self.created["size_bytes"], self.b["BENCH_VDISK_BYTES"])
        # create, attach, then the work, then detach before delete.
        self.assertEqual(self.ops[0], "create")
        self.assertEqual(self.ops[1], "attach")
        self.assertEqual(self.ops[-2:], ["detach", "delete"])
        text = "\n".join(self.out)
        for phase in self.b["BENCH_PHASES"]:
            if phase[1] == "write" or phase[1] == "read":
                self.assertIn(phase[0], text)
        self.assertIn("read-back verify", text)
        self.assertIn("ok", text.split("read-back verify")[1].splitlines()[0])

    def test_the_warm_up_is_run_but_not_reported(self):
        self.b["cmd_storage_benchmark"]("default-pool")
        text = "\n".join(self.out)
        self.assertNotIn(self.b["BENCH_WARMUP"][0] + " ", text.split("Results")[1])
        # One qemu-img for the warm-up plus one per phase.
        self.assertEqual(self.ops.count("qemu-img"), 1 + len(self.b["BENCH_PHASES"]))

    def test_a_write_phase_is_followed_by_a_settle_and_a_read_phase_is_not(self):
        self.b["cmd_storage_benchmark"]("default-pool")
        phases = self.b["BENCH_PHASES"]
        after_warmup = self.ops[self.ops.index("qemu-img") + 1:]
        expected = []
        for p in phases:
            expected.append("qemu-img")
            if p[1] == "write":
                expected.append("status")
        # qemu-io (the read-back) comes after, then cleanup.
        self.assertEqual(after_warmup[:len(expected)], expected)

    def test_a_vdisk_that_will_not_attach_is_still_deleted(self):
        self.attach_ok = False
        self.b["cmd_storage_benchmark"]("default-pool")
        self.assertEqual(self.ops[-2:], ["detach", "delete"])
        self.assertNotIn("qemu-img", self.ops)

    def test_a_failure_in_the_middle_still_detaches_and_deletes(self):
        def boom(*a, **k):
            raise OSError("qemu-img vanished")
        self.b["_bench_run"] = boom
        self.b["cmd_storage_benchmark"]("default-pool")
        self.assertEqual(self.ops[-2:], ["detach", "delete"])
        self.assertTrue(any("Error during benchmark" in line for line in self.out))

    def test_a_vdisk_that_cannot_be_created_ends_it_without_touching_anything_else(self):
        outer = self

        def api(ip, path, payload, method="POST"):
            outer.ops.append(payload["op"])
            return (1, {"error": "no room"}, "")
        self.b["run_mtls_spark_api"] = api
        self.b["cmd_storage_benchmark"]("default-pool")
        self.assertEqual(self.ops, ["create"])


if __name__ == "__main__":
    unittest.main()
