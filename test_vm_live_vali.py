#!/usr/bin/env python3
"""Live changes to a running VM, Vali's half: the preparation the cluster has to do, and the VM's row.

spark-daemon applies a change to the running domain and its persistent definition
(test_vm_live_change.py). Before it, Vali takes the VM's status lock (one change at a time, and
never during a migration), prepares storage and networking (a new vdisk, an attached image, the
bridge for a network); after it, Vali writes the row, because the domain is rebuilt from the row at
every start. Each of these runs here against a recording cluster.

Also here: the create-time XML that makes vCPU hot-add possible at all. A domain's maximum vCPU
count is fixed when it is defined, so headroom is reserved at define time, behind the cluster
setting `vm_hotplug_headroom`; off, the XML is what it always was.

Run with:  python -m unittest test_vm_live_vali
"""

import importlib.util
import io
import json
import os
import sys
import unittest
import xml.etree.ElementTree as ET
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name, alias):
    spec = importlib.util.spec_from_file_location(alias, os.path.join(HERE, name))
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass
    return module


vali = load("vali.py", "vali_live_change")
valcli = load("valcli.py", "valcli_live_change")
HOST = "10.0.0.2"


class World:
    def __init__(self, **row):
        self.row = {"name": "web", "host_ip": HOST, "state": "Running", "vcpu": 2, "memory": 2048,
                    "disks_list": "10GB:default-pool:virtio,20GB:default-pool:virtio",
                    "iso": "rocky.iso", "network_id": '["net-prod:virtio"]', "disk_size": 10}
        self.row.update(row)
        self.events = []
        self.spark_error = None
        self.sidon_errors = {}
        self.locked = False
        self.lock_busy = False
        self.updates = []

    def vm(self, name):
        return dict(self.row)

    def lwt(self, endpoint, params, timeout=15):
        self.events.append(("lwt", endpoint))
        if endpoint == "/v1/vm/migrate-lock":
            if self.lock_busy:
                return True, False, {"status": "migrating"}, ""
            self.locked = True
        elif endpoint == "/v1/vm/migrate-unlock":
            self.locked = False
        return True, True, {}, ""

    def spark(self, ip, path, payload=None, method="POST", timeout=120):
        self.events.append(("spark", ip, path, payload))
        if path.endswith("/live"):
            if self.spark_error:
                return 409, {"error": self.spark_error, "applied": []}, ""
            return 200, {"applied": ["x"]}, ""
        if path == "/api/v1/dfs/vdisk":
            op = payload["op"]
            self.events.append(("sidon", op, payload.get("vdisk_id"), {k: v for k, v in payload.items() if k not in ("op", "vdisk_id")}))
            if (op, payload.get("vdisk_id")) in self.sidon_errors or op in self.sidon_errors:
                return 409, {"error": self.sidon_errors.get((op, payload.get("vdisk_id"))) or self.sidon_errors[op]}, ""
            return 200, {}, ""
        return 200, {}, ""

    def cql(self, query):
        self.updates.append(query)
        return 0, "", ""

    def net(self, net_id):
        return {"net-prod": {"type": "vlan", "vlan_id": 100},
                "net-overlay": {"type": "overlay", "vni": 5000},
                "net-direct": {"type": "direct"}}.get(net_id)

    def kinds(self, *k):
        return [e for e in self.events if e[0] in k]

    def row_update(self):
        for q in self.updates:
            if q.startswith("UPDATE hydra.vms"):
                return q
        return None


class LiveCase(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.start()

    def start(self):
        for name, fn in {"get_vm_xml_specs": self.w.vm, "run_lwt": self.w.lwt,
                         "run_mtls_spark_api_full": self.w.spark, "run_cql_query": self.w.cql,
                         "get_network_by_id": self.w.net}.items():
            p = mock.patch.object(vali, name, fn)
            p.start()
            self.addCleanup(p.stop)

    def change(self, **payload):
        with mock.patch("sys.stderr", io.StringIO()):
            return vali.run_live_change("web", payload)

    def spark_calls(self):
        return [e for e in self.w.events if e[0] == "spark" and e[2].endswith("/live")]


class TheEnvelope(LiveCase):
    def test_a_stopped_vm_is_refused_and_pointed_at_the_console(self):
        self.w.row["state"] = "Stopped"
        ok, msg = self.change(op="vcpus", count=4)
        self.assertFalse(ok)
        self.assertIn("edited in the console", msg)
        self.assertEqual(self.w.events, [])

    def test_a_busy_vm_is_refused_before_anything_is_touched(self):
        self.w.lock_busy = True
        ok, msg = self.change(op="vcpus", count=4)
        self.assertFalse(ok)
        self.assertIn("busy", msg)
        self.assertEqual(self.spark_calls(), [])

    def test_the_lock_is_taken_first_and_always_released(self):
        self.change(op="vcpus", count=4)
        lwts = [e[1] for e in self.w.kinds("lwt")]
        self.assertEqual(lwts, ["/v1/vm/migrate-lock", "/v1/vm/migrate-unlock"])
        self.assertFalse(self.w.locked)
        self.w.spark_error = "boom"
        self.change(op="vcpus", count=5)
        self.assertFalse(self.w.locked, "a failed change gives the lock back")

    def test_an_unknown_op_is_refused(self):
        self.assertIn("op must be one of", self.change(op="reboot")[1])

    def test_the_change_goes_to_the_host_the_vm_runs_on(self):
        self.change(op="vcpus", count=4)
        self.assertEqual(self.spark_calls()[0][1:3], (HOST, "/api/v1/vm/web/live"))


class VCpus(LiveCase):
    def test_it_is_applied_then_recorded(self):
        ok, msg = self.change(op="vcpus", count=4)
        self.assertTrue(ok, msg)
        self.assertEqual(self.spark_calls()[0][3], {"op": "vcpus", "count": 4})
        self.assertIn("vcpu = 4", self.w.row_update())

    def test_a_refusal_from_the_host_is_the_answer_and_the_row_is_untouched(self):
        self.w.spark_error = "6 vCPUs is above this domain's maximum of 4"
        ok, msg = self.change(op="vcpus", count=6)
        self.assertFalse(ok)
        self.assertIn("maximum of 4", msg)
        self.assertIsNone(self.w.row_update())


class Memory(LiveCase):
    def test_the_balloon_is_runtime_only_and_the_record_is_not_changed(self):
        ok, msg = self.change(op="memory", mib=1024)
        self.assertTrue(ok, msg)
        self.assertIsNone(self.w.row_update())
        self.assertIn("unchanged", msg)


class CdRom(LiveCase):
    def test_an_image_is_attached_on_the_host_then_inserted_then_recorded(self):
        ok, msg = self.change(op="cdrom", slot=0, image="tools.iso")
        self.assertTrue(ok, msg)
        order = [e[0] + ":" + (e[1] if e[0] == "sidon" else "") for e in self.w.events if e[0] in ("sidon", "spark") and (e[0] == "sidon" or e[2].endswith("/live"))]
        self.assertEqual(order, ["sidon:attach", "spark:"])
        body = self.spark_calls()[0][3]
        self.assertEqual((body["target"], body["image_vdisk_id"].startswith("img-")), ("sda", True))
        self.assertIn("iso = 'tools.iso'", self.w.row_update())

    def test_ejecting_records_an_empty_drive_and_attaches_nothing(self):
        ok, _ = self.change(op="cdrom", slot=0, image=None)
        self.assertTrue(ok)
        self.assertEqual(self.w.kinds("sidon"), [])
        self.assertEqual(self.spark_calls()[0][3]["image_vdisk_id"], None)
        self.assertIn("iso = ''", self.w.row_update())

    def test_a_second_drive_pads_the_empty_ones_before_it(self):
        self.w.row["iso"] = ""
        self.change(op="cdrom", slot=1, image="tools.iso")
        self.assertIn("iso = '__empty__,tools.iso'", self.w.row_update())

    def test_an_image_that_cannot_be_attached_stops_before_the_guest_is_touched(self):
        self.w.sidon_errors["attach"] = "no such vdisk"
        ok, msg = self.change(op="cdrom", slot=0, image="tools.iso")
        self.assertFalse(ok)
        self.assertIn("step 'attach image", msg)
        self.assertEqual(self.spark_calls(), [])

    def test_a_bad_slot_or_name(self):
        for slot in (-1, 26, "0", True):
            self.assertFalse(self.change(op="cdrom", slot=slot, image=None)[0], slot)
        self.assertFalse(self.change(op="cdrom", slot=0, image="a b;c")[0])


class Nics(LiveCase):
    def test_attach_resolves_the_bridge_and_gives_the_nic_the_address_the_domain_would(self):
        ok, msg = self.change(op="nic", action="attach", network_id="net-prod", model="e1000")
        self.assertTrue(ok, msg)
        body = self.spark_calls()[0][3]
        self.assertEqual((body["bridge"], body["model"]), ("br-vlan-100", "e1000"))
        self.assertEqual(body["mac"], vali.vm_nic_mac("web", 1))
        self.assertEqual(json.loads(self.w.row_update().split("network_id = '")[1].split("' WHERE")[0]),
                         ["net-prod:virtio", "net-prod:e1000"])

    def test_an_overlay_network_uses_its_vni_bridge(self):
        self.change(op="nic", action="attach", network_id="net-overlay")
        self.assertEqual(self.spark_calls()[0][3]["bridge"], "br-ov-5000")

    def test_a_direct_network_and_an_unknown_one_cannot_be_attached_live(self):
        self.assertIn("macvtap", self.change(op="nic", action="attach", network_id="net-direct")[1])
        self.assertIn("does not exist", self.change(op="nic", action="attach", network_id="ghost")[1])
        self.assertEqual(self.spark_calls(), [])

    def test_only_the_last_nic_can_be_removed(self):
        self.w.row["network_id"] = '["a:virtio","b:virtio"]'
        ok, msg = self.change(op="nic", action="detach", index=0)
        self.assertFalse(ok)
        self.assertIn("only the last NIC", msg)
        ok, msg = self.change(op="nic", action="detach", index=-1)
        self.assertTrue(ok, msg)
        self.assertEqual(self.spark_calls()[-1][3]["mac"], vali.vm_nic_mac("web", 1))
        self.assertIn('["a:virtio"]', self.w.row_update())

    def test_link_state_is_applied_and_said_not_to_persist(self):
        ok, msg = self.change(op="nic", action="link", index=0, state="down")
        self.assertTrue(ok)
        self.assertIn("up again after the VM next starts", msg)
        self.assertIsNone(self.w.row_update())

    def test_the_mac_is_the_one_generate_vm_xml_uses(self):
        import hashlib
        h = hashlib.md5(b"web_nic_0").hexdigest()
        self.assertEqual(vali.vm_nic_mac("web", 0), "52:54:00:%s:%s:%s" % (h[0:2], h[2:4], h[4:6]))


class Disks(LiveCase):
    def test_attach_creates_attaches_then_tells_the_guest_then_records(self):
        ok, msg = self.change(op="disk", action="attach", size_gib=50, container="fast")
        self.assertTrue(ok, msg)
        sidon = [(e[1], e[2]) for e in self.w.kinds("sidon")]
        self.assertEqual(sidon, [("create", "web-disk2"), ("attach", "web-disk2")])
        self.assertEqual(self.w.kinds("sidon")[0][3], {"size_bytes": 50 * 1024 ** 3, "container": "fast"})
        body = self.spark_calls()[0][3]
        self.assertEqual((body["vdisk_id"], body["target"]), ("web-disk2", "vdc"))
        self.assertIn("50GB:fast:virtio", self.w.row_update())

    def test_a_guest_that_refuses_the_disk_gives_the_vdisk_back(self):
        self.w.spark_error = "no free slot"
        ok, msg = self.change(op="disk", action="attach", size_gib=5)
        self.assertFalse(ok)
        self.assertIn("removed", msg)
        self.assertEqual([(e[1], e[2]) for e in self.w.kinds("sidon")][-2:],
                         [("detach", "web-disk2"), ("delete", "web-disk2")])
        self.assertIsNone(self.w.row_update())

    def test_a_vdisk_that_cannot_be_created_changes_nothing(self):
        self.w.sidon_errors["create"] = "no space"
        ok, msg = self.change(op="disk", action="attach", size_gib=5)
        self.assertFalse(ok)
        self.assertIn("step 'create web-disk2' failed: no space", msg)
        self.assertEqual(self.spark_calls(), [])

    def test_grow_extends_storage_then_the_record_then_tells_the_guest(self):
        ok, msg = self.change(op="disk", action="resize", index=1, size_gib=40)
        self.assertTrue(ok, msg)
        self.assertEqual([(e[1], e[2]) for e in self.w.kinds("sidon")], [("resize", "web-disk1")])
        self.assertEqual(self.spark_calls()[0][3], {"op": "disk", "action": "resize", "target": "vdb", "size_bytes": 40 * 1024 ** 3})
        self.assertIn("40GB:default-pool:virtio", self.w.row_update())

    def test_growing_the_boot_disk_updates_the_recorded_size_too(self):
        self.change(op="disk", action="resize", index=0, size_gib=30)
        self.assertIn("disk_size = 30", self.w.row_update())

    def test_a_shrink_and_a_no_op(self):
        self.assertIn("can only grow", self.change(op="disk", action="resize", index=1, size_gib=5)[1])
        self.assertTrue(self.change(op="disk", action="resize", index=1, size_gib=20)[0])
        self.assertEqual(self.w.kinds("sidon"), [])

    def test_a_guest_that_was_not_told_still_has_its_record_follow_the_storage(self):
        self.w.spark_error = "not supported by this block driver"
        ok, msg = self.change(op="disk", action="resize", index=1, size_gib=40)
        self.assertFalse(ok)
        self.assertIn("record updated", msg)
        self.assertIn("40GB", self.w.row_update())

    def test_removal_needs_confirmation_and_only_takes_the_last_data_disk(self):
        self.assertIn("confirm_delete", self.change(op="disk", action="detach", index=1)[1])
        self.assertIn("boot disk", self.change(op="disk", action="detach", index=0, confirm_delete=True)[1])
        self.w.row["disks_list"] = "10GB:default-pool:virtio,20GB:default-pool:virtio,30GB:default-pool:virtio"
        self.assertIn("only the last disk", self.change(op="disk", action="detach", index=1, confirm_delete=True)[1])
        self.assertEqual(self.spark_calls(), [])

    def test_removal_detaches_from_the_guest_then_writes_the_row_then_deletes_the_vdisk(self):
        ok, msg = self.change(op="disk", action="detach", index=1, confirm_delete=True)
        self.assertTrue(ok, msg)
        order = []
        for e in self.w.events:
            if e[0] == "spark" and e[2].endswith("/live"):
                order.append("guest")
            elif e[0] == "sidon":
                order.append(e[1])
        self.assertEqual(order, ["guest", "detach", "delete"])
        self.assertIn("disks_list = '10GB:default-pool:virtio'", self.w.row_update())

    def test_a_vdisk_left_behind_is_reported_and_is_not_a_failure(self):
        self.w.sidon_errors["delete"] = "still attached elsewhere"
        ok, msg = self.change(op="disk", action="detach", index=1, confirm_delete=True)
        self.assertTrue(ok)
        self.assertIn("orphan", msg)


class TheRowIsOnlyEverWrittenThroughOneDoor(unittest.TestCase):
    def test_only_the_columns_a_live_change_owns_can_be_written(self):
        with self.assertRaises(ValueError):
            vali._set_row("web", host_ip="10.0.0.9")
        with self.assertRaises(ValueError):
            vali._set_row("web", state="Stopped")

    def test_values_are_escaped_and_integers_are_coerced(self):
        seen = []
        with mock.patch.object(vali, "run_cql_query", lambda q: seen.append(q) or (0, "", "")):
            vali._set_row("we'b", iso="a'b", vcpu="4")
        self.assertEqual(seen, ["UPDATE hydra.vms SET iso = 'a''b', vcpu = 4 WHERE name = 'we''b';"])
        self.assertIn("iso = 'a''b'", seen[0])
        self.assertIn("name = 'we''b'", seen[0])


class TheEntryPoints(unittest.TestCase):
    def test_the_api_validates_before_it_submits(self):
        src = open(os.path.join(HERE, "vali.py"), encoding="utf-8").read()
        body = src[src.index('elif self.path == "/api/v1/vms/live":'):]
        body = body[:body.index('elif self.path == "/api/v1/vms/balance":')]
        self.assertLess(body.index("is_valid_object_name(name)"), body.index("submit_and_wait_task"))
        self.assertIn("LIVE_CHANGE_OPS", body)
        self.assertIn('"live_change"', body)

    def test_the_task_dispatches_to_the_runner(self):
        src = open(os.path.join(HERE, "vali.py"), encoding="utf-8").read()
        self.assertIn('elif action == "live_change":\n            return run_live_change(vm_name, payload)', src)


class TheCli(unittest.TestCase):
    def test_every_form_builds_the_change_it_names(self):
        build = valcli.live_change_from_args
        self.assertEqual(build(["vcpus", "4"]), {"op": "vcpus", "count": 4})
        self.assertEqual(build(["memory", "1024"]), {"op": "memory", "mib": 1024})
        self.assertEqual(build(["cdrom", "0", "tools.iso"]), {"op": "cdrom", "slot": 0, "image": "tools.iso"})
        self.assertEqual(build(["cdrom", "1", "eject"]), {"op": "cdrom", "slot": 1, "image": None})
        self.assertEqual(build(["nic", "attach", "net-1"]), {"op": "nic", "action": "attach", "network_id": "net-1"})
        self.assertEqual(build(["nic", "attach", "net-1", "e1000"])["model"], "e1000")
        self.assertEqual(build(["nic", "link", "0", "down"]), {"op": "nic", "action": "link", "index": 0, "state": "down"})
        self.assertEqual(build(["disk", "attach", "50", "fast"]),
                         {"op": "disk", "action": "attach", "size_gib": 50, "container": "fast"})
        self.assertEqual(build(["disk", "resize", "1", "40"]), {"op": "disk", "action": "resize", "index": 1, "size_gib": 40})
        self.assertEqual(build(["disk", "detach", "2", "--confirm-delete"]),
                         {"op": "disk", "action": "detach", "index": 2, "confirm_delete": True})

    def test_a_typo_is_an_error_and_never_a_different_change(self):
        for words in ([], ["vcpu", "4"], ["vcpus"], ["vcpus", "four"], ["disk", "detach", "2"],
                      ["disk", "detach", "2", "--confirm"], ["nic", "link", "0", "sideways"], ["cdrom", "0"]):
            with self.subTest(words=words):
                with self.assertRaises(SystemExit):
                    valcli.live_change_from_args(words)


class HeadroomAtDefineTime(unittest.TestCase):
    """The maximum vCPU count is fixed when the domain is defined."""

    def build(self, **kw):
        with mock.patch.object(vali, "run_remote_spark", lambda ip, cmd, timeout=None: (1, "", "")), \
                mock.patch.object(vali, "get_network_by_id", lambda n: None):
            return vali.generate_vm_xml("vm", 2048, 2, "uefi", "10GB:default-pool:virtio", "", **kw)

    def test_without_headroom_the_xml_is_what_it_always_was(self):
        xml = self.build()
        root = ET.fromstring(xml)
        self.assertEqual(root.find("vcpu").text, "2")
        self.assertIsNone(root.find("vcpu").get("current"))
        self.assertEqual(root.find("cpu/topology").get("cores"), "2")

    def test_with_headroom_the_maximum_is_reserved_and_the_current_count_stays(self):
        root = ET.fromstring(self.build(hotplug_max_vcpus=vali.hotplug_vcpu_limit(2)))
        self.assertEqual(root.find("vcpu").text, "8")
        self.assertEqual(root.find("vcpu").get("current"), "2")
        self.assertEqual(root.find("cpu/topology").get("cores"), "8", "the topology covers the maximum")

    def test_the_limit(self):
        limit = vali.hotplug_vcpu_limit
        self.assertEqual([limit(n) for n in (1, 2, 4, 8, 16, 32)], [4, 8, 16, 16, 16, 32])

    def test_the_setting_is_read_from_the_cluster_settings_and_defaults_to_off(self):
        def reading(out, rc=0):
            return lambda q: (rc, out, "")

        with mock.patch.object(vali, "run_cql_query", reading("value\n-----\ntrue\n\n(1 rows)")):
            self.assertTrue(vali.hotplug_headroom_enabled())
        with mock.patch.object(vali, "run_cql_query", reading("value\n-----\nfalse\n")):
            self.assertFalse(vali.hotplug_headroom_enabled())
        with mock.patch.object(vali, "run_cql_query", reading("")):
            self.assertFalse(vali.hotplug_headroom_enabled())
        with mock.patch.object(vali, "run_cql_query", reading("true", rc=1)):
            self.assertFalse(vali.hotplug_headroom_enabled())

    def test_the_start_path_asks_for_headroom_only_when_the_setting_is_on(self):
        src = open(os.path.join(HERE, "vali.py"), encoding="utf-8").read()
        self.assertIn("hotplug_max_vcpus=hotplug_vcpu_limit(vcpu) if hotplug_headroom_enabled() else None", src)
        self.assertIn("hotplug_max_memory=hotplug_memory_limit(memory) if hotplug_headroom_enabled() else None", src)

    def test_memory_headroom_emits_max_memory_and_current_memory(self):
        xml_without = self.build()
        root_without = ET.fromstring(xml_without)
        self.assertIsNone(root_without.find("maxMemory"))
        self.assertEqual(root_without.find("memory").text, "2048")
        self.assertEqual(root_without.find("currentMemory").text, "2048")

        limit = vali.hotplug_memory_limit(2048)
        self.assertEqual(limit, 8192)
        xml_with = self.build(hotplug_max_memory=limit)
        root_with = ET.fromstring(xml_with)
        max_mem = root_with.find("maxMemory")
        self.assertIsNotNone(max_mem)
        self.assertEqual(max_mem.text, "8192")
        self.assertEqual(max_mem.get("slots"), "16")
        self.assertEqual(max_mem.get("unit"), "MiB")
        self.assertEqual(root_with.find("memory").text, "2048")
        self.assertEqual(root_with.find("currentMemory").text, "2048")

    def test_the_memory_limit(self):
        limit = vali.hotplug_memory_limit
        self.assertEqual([limit(m) for m in (1024, 2048, 4096, 32768, 65536, 131072, 262144)],
                         [4096, 8192, 16384, 131072, 131072, 131072, 262144])


if __name__ == "__main__":
    unittest.main()

