#!/usr/bin/env python3
"""Live changes to a running VM: what spark runs, and what it refuses.

Nothing a caller sends is interpolated into a command, and every change goes to the running
domain *and* its persistent definition (`--live --config`), so the two cannot disagree after the
next start. `plan_live_change` is a pure function of the request and what libvirt reports, so the
whole matrix of what is possible, what is refused and why is tested without a hypervisor.

Run with:  python -m unittest test_vm_live_change
"""

import importlib.util
import os
import unittest
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))


def load_daemon():
    spec = importlib.util.spec_from_file_location(
        "spark_live_under_test", os.path.join(HERE, "spark_daemon_decoded.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


daemon = load_daemon()

RUNNING = {"state": "running", "vcpu_current": 2, "vcpu_max": 8, "mem_max_kib": 8 * 1024 * 1024}


def plan(payload, info=None, name="web-01"):
    return daemon.plan_live_change(name, payload, info or RUNNING)


def argv(step):
    return " ".join(step["argv"])


class TheGate(unittest.TestCase):
    def test_a_stopped_vm_is_not_changed_live(self):
        steps, error = plan({"op": "vcpus", "count": 4}, dict(RUNNING, state="shut off"))
        self.assertIsNone(steps)
        self.assertIn("shut off", error)

    def test_an_unknown_op_and_a_bad_name_are_refused(self):
        self.assertIn("op must be one of", plan({"op": "reboot"})[1])
        self.assertIn("Invalid VM name", plan({"op": "vcpus", "count": 3}, name="a; rm -rf /")[1])


class VCpus(unittest.TestCase):
    def test_hot_add_within_the_maximum_applies_live_and_persistently(self):
        steps, error = plan({"op": "vcpus", "count": 4})
        self.assertIsNone(error)
        self.assertEqual(len(steps), 1)
        self.assertEqual(steps[0]["argv"][-5:], ["setvcpus", "web-01", "4", "--live", "--config"])
        self.assertIn("--live", steps[0]["argv"])
        self.assertIn("--config", steps[0]["argv"])

    def test_the_same_count_is_a_no_op(self):
        self.assertEqual(plan({"op": "vcpus", "count": 2}), ([], None))

    def test_removing_vcpus_is_refused_and_says_to_stop_the_vm(self):
        steps, error = plan({"op": "vcpus", "count": 1})
        self.assertIsNone(steps)
        self.assertIn("not supported", error)
        self.assertIn("stop the VM", error)

    def test_above_the_maximum_names_the_maximum_and_the_way_to_raise_it(self):
        steps, error = plan({"op": "vcpus", "count": 9})
        self.assertIsNone(steps)
        self.assertIn("maximum of 8", error)
        self.assertIn("vm_hotplug_headroom", error)

    def test_garbage_counts(self):
        for count in (0, -1, "4", 4.5, True, None):
            with self.subTest(count=count):
                self.assertIsNone(plan({"op": "vcpus", "count": count})[0])

    def test_unreadable_counts_are_a_refusal_not_a_guess(self):
        self.assertIn("could not be read", plan({"op": "vcpus", "count": 3}, dict(RUNNING, vcpu_max=None))[1])


class Memory(unittest.TestCase):
    def test_a_change_within_the_maximum_is_the_balloon_in_kib_and_live_only(self):
        steps, error = plan({"op": "memory", "mib": 4096})
        self.assertIsNone(error)
        self.assertEqual(steps[0]["argv"][-4:], ["setmem", "web-01", str(4096 * 1024), "--live"])
        self.assertNotIn("--config", steps[0]["argv"],
                         "the definition is rebuilt from the VM's record at every start; a --config "
                         "here would claim a persistence it does not have")

    def test_shrinking_the_balloon_is_allowed(self):
        self.assertIsNone(plan({"op": "memory", "mib": 512})[1])

    def test_above_the_maximum_is_refused_with_the_maximum(self):
        steps, error = plan({"op": "memory", "mib": 16384})
        self.assertIsNone(steps)
        self.assertIn("maximum of 8192 MiB", error)

    def test_below_the_floor_is_refused(self):
        self.assertIsNone(plan({"op": "memory", "mib": 64})[0])

    def test_garbage(self):
        for mib in ("2048", 2048.0, True, None, -5):
            self.assertIsNone(plan({"op": "memory", "mib": mib})[0], mib)


class CdRom(unittest.TestCase):
    def test_inserting_an_image_uses_the_same_xml_the_domain_is_created_from(self):
        steps, error = plan({"op": "cdrom", "target": "sda", "image_vdisk_id": "img-rocky-10"})
        self.assertIsNone(error)
        root = ET.fromstring(steps[0]["xml"])
        self.assertEqual(root.get("device"), "cdrom")
        self.assertEqual(root.find("target").get("dev"), "sda")
        self.assertEqual(root.find("source").get("name"), "img-rocky-10")
        self.assertIsNotNone(root.find("readonly"))
        self.assertEqual(steps[0]["argv"][-4:], ["web-01", daemon.LIVE_XML_PATH, "--live", "--config"])
        self.assertIn("update-device", steps[0]["argv"])
        self.assertIn("--live", steps[0]["argv"])
        self.assertIn("--config", steps[0]["argv"])

    def test_ejecting_has_no_source(self):
        steps, error = plan({"op": "cdrom", "target": "sdb", "image_vdisk_id": None})
        self.assertIsNone(error)
        root = ET.fromstring(steps[0]["xml"])
        self.assertIsNone(root.find("source"))
        self.assertEqual(root.find("target").get("dev"), "sdb")

    def test_only_cd_drives_and_valid_images(self):
        for target in ("vda", "sda1", "../sda", None, 5):
            self.assertIsNone(plan({"op": "cdrom", "target": target, "image_vdisk_id": "img-x"})[0], target)
        self.assertIn("Invalid image", plan({"op": "cdrom", "target": "sda", "image_vdisk_id": "a b"})[1])


class Nic(unittest.TestCase):
    MAC = "52:54:00:aa:bb:cc"

    def test_attach_builds_a_bridge_interface(self):
        steps, error = plan({"op": "nic", "action": "attach", "mac": self.MAC, "bridge": "br-vlan-100", "model": "e1000"})
        self.assertIsNone(error)
        root = ET.fromstring(steps[0]["xml"])
        self.assertEqual((root.get("type"), root.find("mac").get("address")), ("bridge", self.MAC))
        self.assertEqual(root.find("source").get("bridge"), "br-vlan-100")
        self.assertEqual(root.find("model").get("type"), "e1000")
        self.assertIn("attach-device", steps[0]["argv"])
        self.assertIn("--config", steps[0]["argv"])

    def test_detach_names_the_interface_by_mac(self):
        steps, _ = plan({"op": "nic", "action": "detach", "mac": self.MAC})
        self.assertEqual(ET.fromstring(steps[0]["xml"]).find("mac").get("address"), self.MAC)
        self.assertIn("detach-device", steps[0]["argv"])

    def test_link_state_is_set_now_and_in_the_definition(self):
        steps, error = plan({"op": "nic", "action": "link", "mac": self.MAC, "state": "down"})
        self.assertIsNone(error)
        self.assertEqual(len(steps), 2)
        self.assertNotIn("--config", steps[0]["argv"])
        self.assertIn("--config", steps[1]["argv"])
        self.assertTrue(all("down" in step["argv"] for step in steps))

    def test_nothing_unvalidated_reaches_a_command_or_the_xml(self):
        bad = [
            {"action": "attach", "mac": "not-a-mac", "bridge": "br0"},
            {"action": "attach", "mac": self.MAC, "bridge": "br0'/><evil/>"},
            {"action": "attach", "mac": self.MAC, "bridge": "a" * 16},
            {"action": "attach", "mac": self.MAC, "bridge": "br0", "model": "ne2000"},
            {"action": "link", "mac": self.MAC, "state": "sideways"},
            {"action": "wiggle", "mac": self.MAC},
        ]
        for payload in bad:
            with self.subTest(payload=payload):
                self.assertIsNone(plan(dict(payload, op="nic"))[0])


class Disk(unittest.TestCase):
    def test_attach_uses_the_domains_own_disk_builder(self):
        steps, error = plan({"op": "disk", "action": "attach", "vdisk_id": "web-01-disk1", "target": "vdb"})
        self.assertIsNone(error)
        root = ET.fromstring(steps[0]["xml"])
        self.assertEqual(root.find("target").get("dev"), "vdb")
        self.assertEqual(root.find("source").get("protocol"), "nbd")
        self.assertEqual(root.find("source").get("name"), "web-01-disk1")
        self.assertEqual(root.find("driver").get("queues"), "2", "queues follow the live vCPU count")

    def test_the_boot_disk_cannot_be_attached_over_or_removed(self):
        self.assertIsNone(plan({"op": "disk", "action": "attach", "vdisk_id": "x", "target": "vda"})[0])
        self.assertIn("boot disk", plan({"op": "disk", "action": "detach", "target": "vda"})[1])

    def test_detach_by_target(self):
        steps, _ = plan({"op": "disk", "action": "detach", "target": "vdc"})
        self.assertEqual(ET.fromstring(steps[0]["xml"]).find("target").get("dev"), "vdc")
        self.assertIn("detach-device", steps[0]["argv"])

    def test_resize_tells_qemu_in_kib_and_touches_no_xml(self):
        steps, error = plan({"op": "disk", "action": "resize", "target": "vda", "size_bytes": 20 * 1024 ** 3})
        self.assertIsNone(error)
        self.assertEqual(steps[0]["argv"][-4:], ["blockresize", "web-01", "vda", str(20 * 1024 ** 2)])
        self.assertIsNone(steps[0]["xml"])

    def test_resize_garbage(self):
        for size in (0, 1000, 1024 * 1024 + 1, "20G", True, None):
            self.assertIsNone(plan({"op": "disk", "action": "resize", "target": "vdb", "size_bytes": size})[0], size)


class RunningAStep(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.files = []

        def fake_run(argv, timeout=45):
            self.calls.append(list(argv))
            for part in argv:
                if part.endswith(".xml"):
                    with open(part, encoding="utf-8") as handle:
                        self.files.append((part, handle.read()))
            return (self.rc, "", self.err)

        self.rc, self.err = 0, ""
        self.saved = daemon.run_argv
        daemon.run_argv = fake_run

    def tearDown(self):
        daemon.run_argv = self.saved

    def test_the_xml_goes_through_a_temporary_file_that_is_removed(self):
        steps, _ = plan({"op": "nic", "action": "detach", "mac": "52:54:00:aa:bb:cc"})
        ok, detail = daemon.run_live_step(steps[0])
        self.assertTrue(ok, detail)
        path, content = self.files[0]
        self.assertIn("52:54:00:aa:bb:cc", content)
        self.assertFalse(os.path.exists(path))
        self.assertNotIn(daemon.LIVE_XML_PATH, self.calls[0])

    def test_a_virsh_refusal_comes_back_as_text(self):
        self.rc, self.err = 1, "error: Requested operation is not valid"
        steps, _ = plan({"op": "vcpus", "count": 4})
        ok, detail = daemon.run_live_step(steps[0])
        self.assertFalse(ok)
        self.assertIn("not valid", detail)

    def test_domain_info_reads_state_vcpus_and_memory(self):
        outputs = {
            "dominfo": "Id: 3\nName: web-01\nState: running\nCPU(s): 2\nMax memory: 8388608 KiB\nUsed memory: 2097152 KiB\n",
            "vcpucount": "8\n",
        }
        daemon.run_argv = lambda argv, timeout=45: (0, outputs[argv[3]], "")
        info = daemon.read_live_domain_info("web-01")
        self.assertEqual(info, {"state": "running", "vcpu_current": 2, "vcpu_max": 8, "mem_max_kib": 8388608})

    def test_an_absent_domain_is_none(self):
        daemon.run_argv = lambda argv, timeout=45: (1, "", "error: failed to get domain")
        self.assertIsNone(daemon.read_live_domain_info("ghost"))


class TheEndpoint(unittest.TestCase):
    def test_it_is_a_typed_post_route_with_a_validated_name(self):
        with open(os.path.join(HERE, "spark_daemon_decoded.py"), encoding="utf-8") as handle:
            src = handle.read()
        self.assertIn('segments[4] == "live"', src)
        self.assertIn("self.handle_vm_live(name)", src)
        self.assertIn('"/api/v1/vm/live"', src)  # forwarded to Vali from the CLI side


if __name__ == "__main__":
    unittest.main()
