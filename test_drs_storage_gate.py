#!/usr/bin/env python3
"""Tests for Vali's DRS storage-capacity gate.

The gate refuses a migration when the target cannot hold the VM's disk. It refused
nothing, on every cluster, for as long as it has existed.

Two fallbacks caused it. `get_linstor_free_space()` parsed the *human* storage-pool table
by scanning cells for one containing "/", which matched `vg_aether/thin_pool_aether` --
the backing volume group, printed before the capacity columns -- and fell through to a
hardcoded 999999 MiB. `get_vm_disk_size()` fell through to 51200. Since the gate only
refuses when the disk is larger than the free space, a free-space fallback of 999999
approved every migration.

Both now return `None` for "unknown", and the gate refuses on unknown rather than
guessing. Measured against the real cluster: the pool has 306951 MiB free, not 999999.

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

# Captured verbatim from `linstor -m --output-version v1 storage-pool list` on a live
# cluster. The diskless pool really does report INT64_MAX.
LIVE_POOLS = json.dumps([[
    {"storage_pool_name": "default-pool", "node_name": "node01",
     "provider_kind": "LVM_THIN", "free_capacity": 314318732,
     "total_capacity": 419430400},
    {"storage_pool_name": "DfltDisklessStorPool", "node_name": "node01",
     "provider_kind": "DISKLESS", "free_capacity": 9223372036854775807,
     "total_capacity": 9223372036854775807},
]])


class FreeSpaceTests(unittest.TestCase):
    def setUp(self):
        self._saved = (vali.run_remote_spark, vali.hostname_for_ip)
        # LINSTOR keys pools by node name; the caller passes an address. cluster.json is
        # the only place holding both, and there is no cluster.json in a test.
        vali.hostname_for_ip = lambda ip: {"10.0.0.1": "node01",
                                           "10.0.0.2": "node02"}.get(ip)

    def tearDown(self):
        vali.run_remote_spark, vali.hostname_for_ip = self._saved

    def _answer(self, rc, stdout):
        vali.run_remote_spark = lambda ip, cmd: (rc, stdout, "")

    def test_it_reads_the_real_capacity(self):
        # 314318732 KiB / 1024 = 306951 MiB. The old parser said 999999.
        self._answer(0, LIVE_POOLS)
        self.assertEqual(vali.get_linstor_free_space("10.0.0.1"), 306951)

    def test_the_diskless_pool_is_skipped(self):
        # INT64_MAX free. Counting it makes a full fabric look empty.
        self._answer(0, LIVE_POOLS)
        self.assertLess(vali.get_linstor_free_space("10.0.0.1"), 10 ** 9)

    def test_an_unreadable_pool_list_is_unknown_not_enormous(self):
        for rc, out in ((1, ""), (0, ""), (0, "not json"), (0, "[]")):
            with self.subTest(rc=rc, out=out[:12]):
                self._answer(rc, out)
                self.assertIsNone(vali.get_linstor_free_space("10.0.0.1"))

    def test_it_reports_the_smallest_backed_pool_on_that_node(self):
        # A target is only as big as its tightest pool.
        self._answer(0, json.dumps([[
            {"storage_pool_name": "a", "node_name": "node01",
             "provider_kind": "LVM_THIN", "free_capacity": 4194304},
            {"storage_pool_name": "b", "node_name": "node01",
             "provider_kind": "LVM_THIN", "free_capacity": 1048576},
        ]]))
        self.assertEqual(vali.get_linstor_free_space("10.0.0.1"), 1024)

    def test_another_nodes_pools_are_not_counted(self):
        # `storage-pool list` returns the whole cluster. Taking the minimum across all of
        # it would refuse a migration to a target with room because some *other* node is
        # full -- and on a single-node cluster that mistake is invisible.
        self._answer(0, json.dumps([[
            {"storage_pool_name": "default-pool", "node_name": "node01",
             "provider_kind": "LVM_THIN", "free_capacity": 314318732},
            {"storage_pool_name": "default-pool", "node_name": "node02",
             "provider_kind": "LVM_THIN", "free_capacity": 1048576},
        ]]))
        self.assertEqual(vali.get_linstor_free_space("10.0.0.1"), 306951)
        self.assertEqual(vali.get_linstor_free_space("10.0.0.2"), 1024)

    def test_a_node_linstor_did_not_report_on_is_unknown(self):
        # Not "no space" and not "plenty": no rows for a node means LINSTOR did not
        # answer for it, which is the case that must refuse rather than guess.
        self._answer(0, json.dumps([[
            {"storage_pool_name": "default-pool", "node_name": "node02",
             "provider_kind": "LVM_THIN", "free_capacity": 1048576},
        ]]))
        self.assertIsNone(vali.get_linstor_free_space("10.0.0.1"))

    def test_an_unmappable_address_is_unknown(self):
        # If the target is not in cluster.json there is no node name to filter on, so
        # there is no honest answer.
        self._answer(0, LIVE_POOLS)
        self.assertIsNone(vali.get_linstor_free_space("10.9.9.9"))

    def test_the_old_fallbacks_are_gone(self):
        source = io.open(os.path.join(HERE, "vali.py"), encoding="utf-8").read()
        self.assertNotIn("return 999999", source,
                         "the free-space fallback that disabled the gate is still here")
        self.assertNotIn("return 51200", source,
                         "the disk-size fallback is still here")

    def test_it_asks_for_machine_readable_output(self):
        # The human table's column order is what the old parser tripped over.
        source = io.open(os.path.join(HERE, "vali.py"), encoding="utf-8").read()
        self.assertIn("--output-version v1", source)


class GateTests(unittest.TestCase):
    """Unknown must refuse, not approve."""

    def setUp(self):
        self.saved = (vali.get_vm_disk_size, vali.get_linstor_free_space)

    def tearDown(self):
        vali.get_vm_disk_size, vali.get_linstor_free_space = self.saved

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
