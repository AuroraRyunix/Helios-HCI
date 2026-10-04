#!/usr/bin/env python3
"""Tests for the storage steps of a live migration.

`valcli host.maintenance.enter` (and every migrate, evacuate and DRS move) failed with

    qemu-kvm: -blockdev {"driver":"nbd","server":{"type":"unix","path":".../<vdisk>.sock"}...}:
    Failed to connect ... No such file or directory

because the migration defined nothing on the destination and attached nothing either: the
destination qemu opened an NBD socket that no Sidon was serving. The start and HA paths
attach before they define the domain; the migration never did.

The migration now attaches every disk on the target in forwarding mode (the writer stays the
source), runs `virsh migrate`, and then has the target take the disks over. These tests run
the real `process_queue_task` migrate branch against a recording stand-in for the cluster.

Run with:  python -m unittest test_live_migration
"""

import ast
import importlib.util
import io
import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))


def load_vali():
    spec = importlib.util.spec_from_file_location(
        "vali_live_migration", os.path.join(HERE, "vali.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules["vali_live_migration"] = module
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass
    return module


vali = load_vali()

SRC, DST = "10.0.0.1", "10.0.0.2"


class Cluster:
    """A recording stand-in for everything the migration talks to."""

    def __init__(self):
        self.events = []
        self.dfs = {}          # (op, vdisk) -> list of canned responses, consumed in order
        self.migrate_rc = 0
        self.timeouts = {}

    def respond(self, op, vdisk, *answers):
        self.dfs[(op, vdisk)] = list(answers)

    # -- stubs -----------------------------------------------------------------------------

    def dfs_call(self, ip, path, payload=None, method="POST", timeout=120):
        op, vdisk = payload["op"], payload["vdisk_id"]
        extra = {k: v for k, v in payload.items() if k not in ("op", "vdisk_id")}
        self.events.append(("dfs", ip, op, vdisk, extra))
        self.timeouts[(op, vdisk)] = timeout
        queue = self.dfs.get((op, vdisk))
        if queue:
            answer = queue.pop(0) if len(queue) > 1 else queue[0]
            return answer
        if op == "attach" and extra.get("forward"):
            return 200, {"vdisk_id": vdisk, "forwarding_to": "src"}, ""
        if op == "takeover":
            return 200, {"vdisk_id": vdisk, "epoch": 5, "previous_owner": "src"}, ""
        return 200, {"vdisk_id": vdisk}, ""

    def remote(self, ip, command, timeout=None):
        if "virsh -c qemu:///system migrate" in command:
            self.events.append(("migrate", ip, timeout))
            return self.migrate_rc, "", "error: migration failed" if self.migrate_rc else ""
        self.events.append(("remote", ip, command.split()[0] if command.split() else ""))
        return 0, "", ""

    def lwt(self, endpoint, params, timeout=15):
        self.events.append(("lwt", endpoint))
        return True, True, {}, ""

    def kinds(self, *names):
        return [e for e in self.events if e[0] in names]


class MigrateBranch(unittest.TestCase):
    def setUp(self):
        self.c = Cluster()
        row = {"name": "web", "host_ip": SRC, "state": "Running", "ram": 1024,
               "disks_list": "a:default:virtio,b:default:virtio", "iso": "ubuntu.iso"}
        patches = {
            "get_node_ip": lambda h: h,
            "get_vm_xml_specs": lambda n: dict(row),
            "run_mtls_spark_api": lambda ip, path, payload=None, method="POST": (
                0, {"maintenance_status": "NORMAL", "services": {}}, ""),
            "get_node_utilization": lambda ip, fetch_cpu=False: (0, 0, 16000, 1000),
            "get_vm_disk_size": lambda n: 100,
            "get_storage_free_space": lambda ip: 100000,
            "run_lwt": self.c.lwt,
            "run_remote_spark": self.c.remote,
            "run_mtls_spark_api_full": self.c.dfs_call,
            "run_cql_query": lambda q: (0, "", ""),
        }
        for name, fn in patches.items():
            p = mock.patch.object(vali, name, fn)
            p.start()
            self.addCleanup(p.stop)
        sleep = mock.patch.object(vali.time, "sleep", lambda s: None)
        sleep.start()
        self.addCleanup(sleep.stop)

    def migrate(self):
        out = io.StringIO()
        err = io.StringIO()
        with mock.patch("sys.stdout", out), mock.patch("sys.stderr", err):
            return vali.process_queue_task({
                "task_id": "t1", "vm_name": "web", "action": "migrate",
                "target_host": DST, "payload": {}})

    def dfs_events(self):
        return [e for e in self.c.events if e[0] == "dfs"]

    def index(self, wanted):
        for i, e in enumerate(self.c.events):
            if e[:1] == wanted[:1] and all(w == x for w, x in zip(wanted, e)):
                return i
        self.fail("no event %r in %r" % (wanted, self.c.events))

    # -- the fix ---------------------------------------------------------------------------

    def test_the_target_is_given_every_disk_before_the_guest_is_moved(self):
        ok, detail = self.migrate()
        self.assertTrue(ok, detail)
        attach_a = self.index(("dfs", DST, "attach", "web-disk0"))
        attach_b = self.index(("dfs", DST, "attach", "web-disk1"))
        migrate = self.index(("migrate", SRC))
        self.assertLess(attach_a, migrate)
        self.assertLess(attach_b, migrate)

    def test_data_disks_are_attached_forwarding_and_images_are_not(self):
        self.migrate()
        attaches = {e[3]: e[4] for e in self.dfs_events() if e[2] == "attach"}
        self.assertEqual(attaches["web-disk0"], {"forward": True})
        self.assertEqual(attaches["web-disk1"], {"forward": True})
        image = [k for k in attaches if k.startswith("img-")]
        self.assertEqual(len(image), 1)
        self.assertEqual(attaches[image[0]], {}, "an immutable image has no owner to forward to")

    def test_the_target_takes_each_disk_over_after_the_guest_has_moved(self):
        ok, detail = self.migrate()
        self.assertTrue(ok, detail)
        migrate = self.index(("migrate", SRC))
        for disk in ("web-disk0", "web-disk1"):
            self.assertGreater(self.index(("dfs", DST, "takeover", disk)), migrate)
        self.assertEqual(
            [e for e in self.dfs_events() if e[2] == "takeover" and e[1] != DST], [],
            "the takeover runs on the node the guest now runs on")

    def test_the_placement_is_committed_before_the_takeover(self):
        self.migrate()
        self.assertLess(self.index(("lwt", "/v1/vm/migrate-commit")),
                        self.index(("dfs", DST, "takeover", "web-disk0")))

    def test_the_migration_is_given_far_longer_than_the_daemon_default(self):
        self.migrate()
        timeout = [e for e in self.c.events if e[0] == "migrate"][0][2]
        self.assertGreaterEqual(timeout, 600, "the daemon's default of 45 s would kill a migration "
                                              "of any guest that takes longer than that to copy")

    def test_a_takeover_is_given_longer_than_sidons_own_handover_budget(self):
        self.migrate()
        self.assertGreaterEqual(self.c.timeouts[("takeover", "web-disk0")], 150)

    # -- failure before the guest moves: nothing is left behind -----------------------------

    def test_a_failed_virsh_migrate_detaches_the_forwarders_and_moves_nothing(self):
        self.c.migrate_rc = 1
        ok, detail = self.migrate()
        self.assertFalse(ok)
        detached = [e[3] for e in self.dfs_events() if e[2] == "detach"]
        self.assertEqual(sorted(detached), ["web-disk0", "web-disk1"])
        self.assertTrue(all(e[1] == DST for e in self.dfs_events() if e[2] == "detach"))
        self.assertEqual([e for e in self.dfs_events() if e[2] == "takeover"], [],
                         "no handover for a guest that did not move")
        self.assertEqual(self.c.kinds("lwt") and [e[1] for e in self.c.kinds("lwt")].count(
            "/v1/vm/migrate-commit"), 0, "the placement must not move")
        self.assertIn("/v1/vm/migrate-unlock", [e[1] for e in self.c.kinds("lwt")])
        self.assertIn("step 'virsh migrate'", detail)
        self.assertIn("no epoch moved", detail)

    def test_an_image_is_never_detached_by_a_failed_migration(self):
        self.c.migrate_rc = 1
        self.migrate()
        self.assertEqual([e for e in self.dfs_events()
                          if e[2] == "detach" and e[3].startswith("img-")], [],
                         "an image may be the CD-ROM of a guest that is running fine")

    def test_a_refused_attach_names_the_step_and_the_reason_and_undoes_the_first(self):
        self.c.respond("attach", "web-disk1", (409, {"error": "refused: vdisk web-disk1 is owned "
                                                    "by nobody this daemon knows"}, ""))
        ok, detail = self.migrate()
        self.assertFalse(ok)
        self.assertIn("attach web-disk1 on the target", detail)
        self.assertIn("owned by nobody this daemon knows", detail)
        self.assertEqual([e[3] for e in self.dfs_events() if e[2] == "detach"], ["web-disk0"])
        self.assertEqual(self.c.kinds("migrate"), [], "the guest is not moved onto missing disks")

    def test_an_attach_that_would_take_the_disk_from_a_running_source_is_refused(self):
        self.c.respond("attach", "web-disk0", (200, {"vdisk_id": "web-disk0", "epoch": 9}, ""))
        ok, detail = self.migrate()
        self.assertFalse(ok)
        self.assertIn("would own the disk", detail)
        self.assertEqual(self.c.kinds("migrate"), [])

    def test_a_forwarder_that_was_already_there_is_not_detached_by_the_cleanup(self):
        self.c.respond("attach", "web-disk0", (200, {"vdisk_id": "web-disk0",
                                                     "already_attached": True,
                                                     "forwarding_to": "src"}, ""))
        self.c.migrate_rc = 1
        self.migrate()
        self.assertEqual([e[3] for e in self.dfs_events() if e[2] == "detach"], ["web-disk1"])

    # -- failure after the guest moved: it is reported, not rolled back ----------------------

    def test_a_failed_takeover_is_retried_and_then_reported_with_the_way_to_finish_it(self):
        self.c.respond("takeover", "web-disk0", (409, {"error": "handover of web-disk0 failed at "
                       "step 'release': refused: a client is still connected"}, ""))
        ok, detail = self.migrate()
        self.assertFalse(ok)
        tries = [e for e in self.dfs_events() if e[2] == "takeover" and e[3] == "web-disk0"]
        self.assertEqual(len(tries), 3)
        self.assertIn("step 'takeover'", detail)
        self.assertIn("a client is still connected", detail)
        self.assertIn("valcli storage.takeover", detail)
        self.assertIn("running there", detail)
        self.assertEqual([e for e in self.dfs_events() if e[2] == "detach"], [],
                         "the guest runs on the target; its sockets must stay")
        self.assertIn("/v1/vm/migrate-commit", [e[1] for e in self.c.kinds("lwt")],
                      "the placement records where the guest actually is")

    def test_a_takeover_that_works_on_the_second_try_is_a_success(self):
        self.c.respond("takeover", "web-disk0",
                       (503, {"error": "sidon did not answer"}, ""),
                       (200, {"vdisk_id": "web-disk0", "epoch": 5}, ""))
        ok, detail = self.migrate()
        self.assertTrue(ok, detail)

    def test_one_disk_failing_does_not_stop_the_others_being_taken_over(self):
        self.c.respond("takeover", "web-disk0", (409, {"error": "nope"}, ""))
        ok, detail = self.migrate()
        self.assertFalse(ok)
        self.assertTrue([e for e in self.dfs_events() if e[2] == "takeover" and e[3] == "web-disk1"])
        self.assertIn("web-disk0", detail)
        self.assertNotIn("web-disk1:", detail)


class DomainDiskIds(unittest.TestCase):
    """The set a migration attaches is the set the domain XML names."""

    def test_the_ids_follow_the_xml_rule(self):
        self.assertEqual(vali.domain_vdisk_ids("vm", "a:c:virtio,b:c:sata"), ["vm-disk0", "vm-disk1"])
        self.assertEqual(vali.domain_vdisk_ids("vm", "NONE"), [])
        self.assertEqual(vali.domain_vdisk_ids("vm", ""), ["vm-disk0"],
                         "generate_vm_xml gives a VM with no disks_list its first disk")

    def test_images_skip_empty_drives(self):
        self.assertEqual(vali.domain_image_ids("__empty__"), [])
        self.assertEqual(vali.domain_image_ids(""), [])
        self.assertEqual(len(vali.domain_image_ids("a.iso,__empty__,b.iso")), 2)


class SparkAndCli(unittest.TestCase):
    def test_spark_forwards_takeover_with_a_timeout_that_outlasts_the_handover(self):
        src = Path(os.path.join(HERE, "spark_daemon_decoded.py")).read_text(encoding="utf-8")
        tree = ast.parse(src)
        values = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in ("DFS_VDISK_OPS", "DFS_OP_TIMEOUTS"):
                        values[target.id] = ast.literal_eval(node.value)
        self.assertIn("takeover", values["DFS_VDISK_OPS"])
        self.assertGreaterEqual(values["DFS_OP_TIMEOUTS"]["takeover"], 150)

    def test_a_caller_cannot_pick_the_socket_or_the_timeout(self):
        src = Path(os.path.join(HERE, "spark_daemon_decoded.py")).read_text(encoding="utf-8")
        self.assertIn('("op", "timeout", "socket_path")', src)

    def test_run_remote_spark_passes_a_timeout_to_the_daemon(self):
        seen = {}

        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                return json.dumps({"returncode": 0, "stdout": "", "stderr": ""}).encode()

        def fake_open(req, context=None, timeout=None):
            seen["body"] = json.loads(req.data.decode())
            seen["wait"] = timeout
            return Resp()

        with mock.patch.object(vali.urllib.request, "urlopen", fake_open), \
                mock.patch.object(vali.ssl, "create_default_context") as ctx:
            ctx.return_value = mock.MagicMock()
            vali.run_remote_spark("10.0.0.9", "true", timeout=900)
            self.assertEqual(seen["body"]["timeout"], 900)
            self.assertGreater(seen["wait"], 900)
            vali.run_remote_spark("10.0.0.9", "true")
            self.assertNotIn("timeout", seen["body"])

    def test_valcli_has_a_way_to_finish_a_handover_by_hand(self):
        src = Path(os.path.join(HERE, "valcli.py")).read_text(encoding="utf-8")
        self.assertIn('cmd == "storage.takeover"', src)
        self.assertIn('{"op": "takeover", "vdisk_id": vdisk_id}', src)


if __name__ == "__main__":
    unittest.main()
