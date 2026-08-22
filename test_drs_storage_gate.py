#!/usr/bin/env python3
"""Tests for Vali's DRS storage-capacity gate.

The gate refuses a migration when the target cannot hold the VM's disk. It refused
nothing, on every cluster, for as long as it existed.

Two fallbacks caused it. The free-space reader parsed the *human* storage-pool table by
scanning cells for one containing "/", which matched `vg_aether/thin_pool_aether` -- the
backing volume group, printed before the capacity columns -- and fell through to a
hardcoded 999999 MiB. `get_vm_disk_size()` fell through to 51200. Since the gate only
refuses when the disk is larger than the free space, a free-space fallback of 999999
approved every migration.

Both return `None` for "unknown" now, and the gate refuses on unknown rather than
guessing. That property is what these tests exist to hold, and it survived the storage
layer being replaced underneath them: the readers ask Sidon instead of a LINSTOR
controller, and the shape of the answer -- a real number, or None, never a guess -- is
the same.

Run with:  python -m unittest test_drs_storage_gate
"""

import importlib.util
import io
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_vali():
    spec = importlib.util.spec_from_file_location(
        "vali_under_test", os.path.join(HERE, "vali.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules["vali_under_test"] = module
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass
    return module


vali = load_vali()


class FreeSpaceTests(unittest.TestCase):
    """`get_storage_free_space` returns MiB, or None. Never a guess."""

    def setUp(self):
        self._saved = vali.run_mtls_spark_api

    def tearDown(self):
        vali.run_mtls_spark_api = self._saved

    def _answer(self, rc, body):
        vali.run_mtls_spark_api = lambda ip, path, payload, method="POST": (rc, body, "")

    def test_it_reads_the_real_capacity(self):
        self._answer(0, {"ok": True, "total_bytes": 160982630400,
                         "available_bytes": 157345660928, "egroup_count": 12})
        self.assertEqual(vali.get_storage_free_space("10.0.0.1"), 150056)

    def test_a_node_that_does_not_answer_is_unknown(self):
        self._answer(-1, {})
        self.assertIsNone(vali.get_storage_free_space("10.0.0.1"))

    def test_an_unparseable_answer_is_unknown_not_enormous(self):
        # The whole defect in one line: anything other than a number must be None, or the
        # gate approves migrations onto hosts that cannot hold the disk.
        for body in ({"ok": True}, {"available_bytes": "lots"}, "not-a-dict", None):
            with self.subTest(body=body):
                self._answer(0, body)
                self.assertIsNone(vali.get_storage_free_space("10.0.0.1"))

    def test_a_full_node_is_zero_free_and_therefore_unknown(self):
        # Zero available is reported as None rather than 0. Both refuse -- the gate treats
        # unknown and "no room" identically -- and a node reporting exactly zero is far
        # more likely to be answering wrongly than to be exactly full.
        self._answer(0, {"ok": True, "total_bytes": 1024, "available_bytes": 0})
        self.assertIsNone(vali.get_storage_free_space("10.0.0.1"))

    def test_it_asks_the_node_being_measured(self):
        # The reader this replaced had to filter a cluster-wide listing down to the target
        # node, because `storage-pool list` returns every node's pools. Getting that wrong
        # measured the wrong host's free space. Asking the host directly cannot.
        seen = {}

        def capture(ip, path, payload, method="POST"):
            seen["ip"] = ip
            seen["op"] = payload.get("op")
            return 0, {"ok": True, "total_bytes": 100, "available_bytes": 100}, ""

        vali.run_mtls_spark_api = capture
        vali.get_storage_free_space("10.0.0.2")
        self.assertEqual(seen["ip"], "10.0.0.2")
        self.assertEqual(seen["op"], "capacity")

    def test_the_old_fallbacks_are_gone(self):
        source = io.open(os.path.join(HERE, "vali.py"), encoding="utf-8").read()
        self.assertNotIn("return 999999", source,
                         "the free-space fallback that disabled the gate is still here")
        self.assertNotIn("return 51200", source,
                         "the disk-size fallback is still here")


class DiskSizeTests(unittest.TestCase):
    """`get_vm_disk_size` returns MiB from the map, or None."""

    def setUp(self):
        self._saved = (vali.get_vm_xml_specs, vali.run_cql_query, vali.sidon_module)
        vali.get_vm_xml_specs = lambda name: {"disks_list": "20G"}
        vali.sidon_module = lambda: type("M", (), {
            "vdisk_id_for": staticmethod(lambda vm, idx: "%s-disk%d" % (vm, idx))})()

    def tearDown(self):
        vali.get_vm_xml_specs, vali.run_cql_query, vali.sidon_module = self._saved

    def test_it_reads_the_size_from_the_map(self):
        vali.run_cql_query = lambda cql, *a, **k: (
            0, json.dumps({"size_bytes": 21474836480}), "")
        self.assertEqual(vali.get_vm_disk_size("web-01"), 20480)

    def test_a_vdisk_with_no_row_is_unknown(self):
        vali.run_cql_query = lambda cql, *a, **k: (0, "", "")
        self.assertIsNone(vali.get_vm_disk_size("web-01"))

    def test_an_unreadable_map_is_unknown(self):
        vali.run_cql_query = lambda cql, *a, **k: (1, "", "connection refused")
        self.assertIsNone(vali.get_vm_disk_size("web-01"))

    def test_a_vm_with_no_disks_is_unknown(self):
        vali.get_vm_xml_specs = lambda name: {"disks_list": ""}
        vali.run_cql_query = lambda cql, *a, **k: (0, "", "")
        self.assertIsNone(vali.get_vm_disk_size("web-01"))


class GateTests(unittest.TestCase):
    """Unknown must refuse, not approve.

    Asserted against the source rather than by running a migration: the gate is a few
    lines inside a long function that talks to libvirt, Hydra and three daemons, and the
    property worth guarding -- the guards precede the comparison -- is structural.
    """

    def _gate_source(self):
        source = io.open(os.path.join(HERE, "vali.py"), encoding="utf-8").read()
        start = source.index("# Storage capacity gate")
        return source[start:start + source[start:].index("Take the migration lock")]

    def test_the_gate_refuses_when_the_disk_size_is_unknown(self):
        gate = self._gate_source()
        self.assertIn("disk_size is None", gate)
        self.assertIn("Refusing to migrate", gate)

    def test_the_gate_refuses_when_free_space_is_unknown(self):
        gate = self._gate_source()
        self.assertIn("target_free is None", gate)
        # Two distinct refusals, not one shared message: the operator needs to know which
        # input could not be read.
        self.assertEqual(gate.count("Refusing to migrate"), 2)

    def test_the_refusals_come_before_the_comparison(self):
        # If the comparison ran first, `None < int` would raise TypeError inside the
        # caller's try and the migration would fail with an unrelated message.
        gate = self._gate_source()
        disk_guard = gate.index("disk_size is None")
        free_guard = gate.index("target_free is None")
        comparison = gate.index("target_free < disk_size")
        self.assertLess(disk_guard, comparison)
        self.assertLess(free_guard, comparison)


if __name__ == "__main__":
    unittest.main()
