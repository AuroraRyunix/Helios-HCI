#!/usr/bin/env python3
"""Replication that sends the wrong thing, or reports a snapshot delivered that is not, is a
backup that fails the day it is needed.

**These tests run against a fake remote.** There is no second site, no transport and no TLS
anywhere in this repository; the fake implements the same interface a real remote would and can
drop the link, run out of space and be offered the same snapshot twice. What they establish is
that the control-plane logic is right -- what is shipped, what is refused, when a transfer is
called visible, what retention is told not to delete -- and nothing about a real link. The bytes
themselves are moved and verified by Sidon's `replicate` module, tested in Rust against two
directories standing in for two sites.

The map digest is the one thing both languages must compute identically, so the fixture and the
expected value here are the ones in `sidon/src/replicate.rs`, and a test reads that file to prove
the constant is the same.

Run with:  python -m unittest test_rauru_replication
"""

import io
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import rauru_replication as RR  # noqa: E402

TOP_BIT = -(1 << 63)   # a signed vdisk hash with the top bit set, as Hydra returns it


def fixture():
    return {
        "format": 1, "snapshot": "web-disk0-dom-202610031200",
        "size_bytes": 1 << 30, "extent_bytes": 1 << 20,
        "rows": [
            {"extent_index": 0, "group": "eg-a", "offset": 0, "length": 100, "vdisk_hash": 7},
            {"extent_index": 1, "group": "eg-a", "offset": 132, "length": 100, "vdisk_hash": 7},
            {"extent_index": 5, "group": "eg-b~264", "offset": 0, "length": 232,
             "vdisk_hash": TOP_BIT},
        ],
        "groups": [{"id": "eg-a", "length": 264, "seal": "crc32c:deadbeef"},
                   {"id": "eg-b~264", "length": 264, "seal": ""}],
    }


def block_rows():
    return [
        {"extent_index": 0, "egroup_id": "eg-a", "egroup_offset": 0, "length": 100, "vdisk_hash": 7},
        {"extent_index": 1, "egroup_id": "eg-a", "egroup_offset": 132, "length": 100, "vdisk_hash": 7},
        {"extent_index": 5, "egroup_id": "eg-b", "egroup_offset": 0, "length": 232,
         "vdisk_hash": TOP_BIT},
    ]


EGROUPS = {"eg-a": {"state": "sealed", "size": 264, "seal_hash": "crc32c:deadbeef"},
           "eg-b": {"state": "open", "size": 9999, "seal_hash": ""}}


class TheMapDigestIsTheOneSidonComputes(unittest.TestCase):

    def test_the_digest_matches_the_constant_the_rust_test_asserts(self):
        with io.open(os.path.join(HERE, "sidon", "src", "replicate.rs"), encoding="utf-8") as h:
            source = h.read()
        self.assertIn(RR.map_digest(fixture()), source)

    def test_it_names_no_time_and_no_snapshot(self):
        other = dict(fixture(), snapshot="renamed")
        self.assertEqual(RR.map_digest(fixture()), RR.map_digest(other))

    def test_a_changed_row_changes_it(self):
        other = fixture()
        other["rows"][1]["length"] = 101
        self.assertNotEqual(RR.map_digest(fixture()), RR.map_digest(other))


class BuildingAManifest(unittest.TestCase):

    def test_a_sealed_group_ships_whole_and_an_open_one_ships_its_prefix_under_its_own_name(self):
        manifest, sources = RR.build_manifest("snap", 1 << 30, 1 << 20, block_rows(), EGROUPS)
        ids = [g["id"] for g in manifest["groups"]]
        self.assertEqual(ids, ["eg-a", "eg-b~264"])
        sealed, prefix = manifest["groups"]
        self.assertEqual((sealed["length"], sealed["seal"]), (264, "crc32c:deadbeef"))
        # The open group's length is what the map references, not the file's current size,
        # which is still growing and 9999 here to show it is not used.
        self.assertEqual((prefix["length"], prefix["seal"]), (264, ""))
        self.assertEqual(sources, {"eg-a": "eg-a", "eg-b~264": "eg-b"})

    def test_it_is_the_same_manifest_the_rust_side_fixture_describes(self):
        manifest, _ = RR.build_manifest(fixture()["snapshot"], 1 << 30, 1 << 20, block_rows(), EGROUPS)
        self.assertEqual(manifest["rows"], fixture()["rows"])
        self.assertEqual(RR.map_digest(manifest), RR.map_digest(fixture()))

    def test_a_row_naming_an_extent_id_is_refused_because_that_level_is_not_replicated(self):
        rows = block_rows()
        rows[0]["extent_id"] = "x-1"
        with self.assertRaises(RR.ReplicationError):
            RR.build_manifest("snap", 1, 1, rows, EGROUPS)

    def test_a_group_that_is_missing_or_dead_cannot_be_replicated(self):
        for egroups in ({"eg-a": EGROUPS["eg-a"]}, dict(EGROUPS, **{"eg-b": {"state": "dead", "size": 1}})):
            with self.assertRaises(RR.ReplicationError):
                RR.build_manifest("snap", 1, 1, block_rows(), egroups)

    def test_a_row_reaching_past_its_sealed_group_is_refused(self):
        with self.assertRaises(RR.ReplicationError):
            RR.build_manifest("snap", 1, 1, block_rows(),
                              dict(EGROUPS, **{"eg-a": {"state": "sealed", "size": 200, "seal_hash": "x"}}))

    def test_names_that_could_leave_a_directory_are_refused(self):
        for bad in ("../x", "a/b", ".h", "", "a b"):
            manifest = fixture()
            manifest["groups"][0]["id"] = bad
            with self.assertRaises(RR.ReplicationError, msg=repr(bad)):
                RR.validate_manifest(manifest)

    def test_unordered_dangling_or_overlong_rows_are_refused(self):
        manifest = fixture()
        manifest["rows"].reverse()
        with self.assertRaises(RR.ReplicationError):
            RR.validate_manifest(manifest)
        manifest = fixture()
        manifest["rows"][2]["group"] = "nowhere"
        with self.assertRaises(RR.ReplicationError):
            RR.validate_manifest(manifest)


class TheLocalNameIsChosenHere(unittest.TestCase):

    def test_a_replica_is_named_from_the_site_and_the_snapshot_and_is_a_valid_vdisk_id(self):
        name = RR.replica_vdisk_name("0a1b2c3d-4e5f", "web-disk0-dom-202610031200")
        self.assertTrue(name.startswith("r-0a1b2c3d-"))
        self.assertLessEqual(len(name), 63)
        self.assertRegex(name, r"\A[A-Za-z0-9][A-Za-z0-9_.-]{0,62}\Z")

    def test_two_long_names_sharing_a_prefix_still_differ_after_truncation(self):
        a = RR.replica_vdisk_name("site", "x" * 80 + "a")
        b = RR.replica_vdisk_name("site", "x" * 80 + "b")
        self.assertNotEqual(a, b)
        self.assertLessEqual(len(a), 63)

    def test_two_sites_cannot_collide_on_one_snapshot_name(self):
        self.assertNotEqual(RR.replica_vdisk_name("aaaaaaaa", "snap"),
                            RR.replica_vdisk_name("bbbbbbbb", "snap"))

    def test_a_name_off_the_wire_that_is_not_a_name_is_refused_not_sanitised(self):
        for bad in ("../x", "a b", "", "x/y"):
            with self.assertRaises(RR.ReplicationError):
                RR.replica_vdisk_name("site", bad)


class WhatTheDeltaSends(unittest.TestCase):

    def test_only_what_the_remote_lacks_is_sent(self):
        delta = RR.plan_delta(fixture(), {"eg-a": ("complete",), "eg-b~264": ("absent",)})
        self.assertEqual(delta.ship, [("eg-b~264", 0)])
        self.assertEqual((delta.bytes_needed, delta.complete), (264, ["eg-a"]))

    def test_a_group_the_remote_did_not_mention_is_treated_as_absent(self):
        self.assertEqual([g for g, _ in RR.plan_delta(fixture(), {}).ship], ["eg-a", "eg-b~264"])

    def test_a_resume_starts_on_a_chunk_boundary_whatever_the_remote_says(self):
        manifest = fixture()
        manifest["groups"][0]["length"] = 3 * RR.CHUNK
        manifest["rows"] = [r for r in manifest["rows"] if r["group"] != "eg-a"]
        got = RR.plan_delta(manifest, {"eg-a": ("partial", RR.CHUNK + 5)})
        self.assertEqual(got.ship[0], ("eg-a", RR.CHUNK))
        self.assertEqual(got.bytes_needed, 2 * RR.CHUNK + 264)

    def test_a_claim_of_more_staged_bytes_than_the_group_has_cannot_skip_the_group(self):
        manifest = fixture()
        manifest["groups"][0]["length"] = 2 * RR.CHUNK
        manifest["rows"] = [r for r in manifest["rows"] if r["group"] != "eg-a"]
        got = RR.plan_delta(manifest, {"eg-a": ("partial", 10 * RR.CHUNK)})
        self.assertEqual(got.ship[0][1], 2 * RR.CHUNK)


class FakeRemote(RR.Remote):
    """A remote that holds groups and snapshots and fails on request."""

    def __init__(self, free=10 ** 12):
        self.groups = {}        # id -> bytes held (complete)
        self.staged = {}        # id -> bytes staged
        self.visible = {}       # snapshot -> digest
        self.free = free
        self.drop_after = None  # bytes after which the next send drops the link
        self.damage_next = False
        self.log = []

    def offer(self, job, manifest_json):
        import json
        m = json.loads(manifest_json)
        digest = m["map_sha256"]
        self.log.append(("offer", m["snapshot"]))
        if m["snapshot"] in self.visible:
            if self.visible[m["snapshot"]] == digest:
                return ("already_present",)
            raise RR.Conflict("snapshot %s is here with a different map" % m["snapshot"])
        have, need = {}, 0
        for gid, length, _seal in m["groups"]:
            if gid in self.groups:
                have[gid] = ("complete",)
            elif self.staged.get(gid):
                staged = self.staged[gid] // RR.CHUNK * RR.CHUNK
                have[gid] = ("partial", staged) if staged else ("absent",)
                need += length - staged
            else:
                have[gid] = ("absent",)
                need += length
        if need > self.free:
            raise RR.NoSpace("%d bytes needed, %d free" % (need, self.free))
        return ("proceed", have)

    def send(self, job, manifest, wants):
        total = 0
        lengths = dict((g["id"], g["length"]) for g in manifest["groups"])
        for gid, start in wants:
            self.log.append(("send", gid, start))
            if self.damage_next:
                self.damage_next = False
                self.staged.pop(gid, None)
                raise RR.GroupDamaged("group %s did not verify" % gid)
            sendable = lengths[gid] - start
            if self.drop_after is not None and total + sendable > self.drop_after:
                kept = max(0, self.drop_after - total)
                self.staged[gid] = start + kept
                self.drop_after = None
                raise RR.LinkDropped("connection reset after %d bytes" % (total + kept))
            total += sendable
            self.groups[gid] = lengths[gid]
            self.free -= sendable
            self.staged.pop(gid, None)
        return total

    def publish(self, job, manifest_json):
        import json
        m = json.loads(manifest_json)
        for gid, _length, _seal in m["groups"]:
            if gid not in self.groups:
                raise RR.ReplicationError("group %s is not installed" % gid)
        if m["snapshot"] in self.visible:
            return "already_visible"
        self.visible[m["snapshot"]] = m["map_sha256"]
        return "published"


def big_manifest(extra=0):
    manifest = fixture()
    manifest["groups"][0]["length"] = 3 * RR.CHUNK + extra
    manifest["rows"] = [r for r in manifest["rows"] if r["group"] != "eg-a"] + []
    manifest["rows"].insert(0, {"extent_index": 0, "group": "eg-a", "offset": 0, "length": 100,
                                "vdisk_hash": 7})
    return manifest


class DrivingATransferToVisible(unittest.TestCase):

    def setUp(self):
        self.remote = FakeRemote()
        self.store = RR.MemoryStateStore()
        self.r = RR.Replicator(self.remote, self.store)

    def test_a_first_replication_sends_everything_and_ends_visible(self):
        result = self.r.replicate("site-b", "set-1", fixture())
        self.assertEqual(result.state, RR.VISIBLE)
        self.assertEqual(result.bytes_sent, 528)
        self.assertEqual(self.store.get("site-b", "set-1", fixture()["snapshot"])["state"], RR.VISIBLE)

    def test_the_same_snapshot_offered_again_does_no_work(self):
        self.r.replicate("site-b", "set-1", fixture())
        before = list(self.remote.log)
        result = self.r.replicate("site-b", "set-1", fixture())
        self.assertEqual((result.state, result.bytes_sent), (RR.VISIBLE, 0))
        self.assertEqual(self.remote.log, before, "a delivered snapshot was offered again")

    def test_a_remote_that_already_has_it_from_another_job_is_not_sent_it_again(self):
        self.r.replicate("site-b", "set-1", fixture())
        fresh = RR.Replicator(self.remote, RR.MemoryStateStore())
        result = fresh.replicate("site-b", "set-9", fixture())
        self.assertEqual((result.state, result.bytes_sent), (RR.VISIBLE, 0))
        self.assertEqual([e for e in self.remote.log if e[0] == "send"].__len__(), 2)

    def test_a_second_snapshot_ships_only_the_groups_the_first_did_not_carry(self):
        self.r.replicate("site-b", "set-1", fixture())
        second = fixture()
        second["snapshot"] = "web-disk0-dom-202610031300"
        second["groups"].append({"id": "eg-c", "length": 300, "seal": "crc32c:00000001"})
        second["rows"].append({"extent_index": 9, "group": "eg-c", "offset": 0, "length": 200,
                               "vdisk_hash": 7})
        result = self.r.replicate("site-b", "set-2", second)
        self.assertEqual(result.bytes_sent, 300)

    def test_a_dropped_link_pauses_and_the_next_attempt_resumes_from_the_chunk(self):
        manifest = big_manifest()
        self.remote.drop_after = RR.CHUNK + 1234
        result = RR.Replicator(self.remote, self.store, max_attempts=1).replicate("site-b", "set-1", manifest)
        self.assertEqual(result.state, RR.PAUSED)
        self.assertEqual(self.store.get("site-b", "set-1", manifest["snapshot"])["state"], RR.PAUSED)
        self.assertNotIn(manifest["snapshot"], self.remote.visible)
        again = self.r.replicate("site-b", "set-1", manifest)
        self.assertEqual(again.state, RR.VISIBLE)
        resumed = [e for e in self.remote.log if e[0] == "send" and e[1] == "eg-a"][-1]
        self.assertEqual(resumed[2], RR.CHUNK, "the retry started over instead of resuming")

    def test_a_link_that_drops_once_is_retried_within_the_same_call(self):
        manifest = big_manifest()
        self.remote.drop_after = RR.CHUNK + 5
        result = self.r.replicate("site-b", "set-1", manifest)
        self.assertEqual((result.state, result.attempts), (RR.VISIBLE, 2))

    def test_a_remote_out_of_space_pauses_and_nothing_is_freed_to_fit(self):
        self.remote.free = 100
        result = self.r.replicate("site-b", "set-1", fixture())
        self.assertEqual(result.state, RR.PAUSED)
        self.assertIn("no space", result.error)
        self.assertEqual(result.attempts, 1, "waiting a second does not make room")
        self.assertEqual(self.remote.visible, {})
        self.remote.free = 10 ** 9
        self.assertEqual(self.r.replicate("site-b", "set-1", fixture()).state, RR.VISIBLE)

    def test_a_different_snapshot_under_a_delivered_name_is_a_permanent_conflict(self):
        self.r.replicate("site-b", "set-1", fixture())
        other = fixture()
        other["rows"][0]["length"] = 99
        result = RR.Replicator(self.remote, RR.MemoryStateStore()).replicate("site-b", "set-2", other)
        self.assertEqual(result.state, RR.FAILED)
        self.assertIn("different map", result.error)

    def test_a_group_that_fails_verification_is_retried_once_and_a_persistent_one_fails(self):
        self.remote.damage_next = True
        self.assertEqual(self.r.replicate("site-b", "set-1", fixture()).state, RR.VISIBLE)

        class Always(FakeRemote):
            def send(self, job, manifest, wants):
                raise RR.GroupDamaged("source copy of %s is damaged" % wants[0][0])
        result = RR.Replicator(Always(), RR.MemoryStateStore(), max_attempts=2).replicate(
            "site-b", "set-1", fixture())
        self.assertEqual(result.state, RR.FAILED)

    def test_a_visible_transfer_never_becomes_anything_else(self):
        record = RR.advance({}, RR.OFFERED)
        for state in (RR.SENDING, RR.PUBLISHING, RR.VISIBLE):
            record = RR.advance(record, state)
        with self.assertRaises(RR.ReplicationError):
            RR.advance(record, RR.PAUSED)

    def test_a_failed_transfer_is_retried_only_by_going_back_to_offered(self):
        record = RR.advance(RR.advance({}, RR.OFFERED), RR.FAILED)
        with self.assertRaises(RR.ReplicationError):
            RR.advance(record, RR.PUBLISHING)
        self.assertEqual(RR.advance(record, RR.OFFERED)["state"], RR.OFFERED)

    def test_the_job_id_is_stable_so_a_restart_finds_its_staged_bytes(self):
        self.assertEqual(RR.job_id("s", "set", "snap", "abc"), RR.job_id("s", "set", "snap", "abc"))
        self.assertNotEqual(RR.job_id("s", "set", "snap", "abc"), RR.job_id("s", "set", "snap", "abd"))


class ASetIsVisibleOnlyWhenEveryMemberIs(unittest.TestCase):

    def test_one_member_failing_leaves_the_set_not_visible_and_the_others_delivered(self):
        remote = FakeRemote()
        store = RR.MemoryStateStore()
        a = fixture()
        b = fixture()
        b["snapshot"] = "db-disk0-dom-202610031200"
        b["groups"][0] = {"id": "eg-z", "length": 10 ** 8, "seal": "crc32c:00000002"}
        b["rows"] = [r for r in b["rows"] if r["group"] != "eg-a"]
        b["rows"].insert(0, {"extent_index": 0, "group": "eg-z", "offset": 0, "length": 100,
                             "vdisk_hash": 7})
        remote.free = 10 ** 6
        state, results = RR.Replicator(remote, store).replicate_set("site-b", "set-1", [a, b])
        self.assertEqual(state, RR.PAUSED)
        self.assertEqual(results[a["snapshot"]].state, RR.VISIBLE)
        self.assertEqual(results[b["snapshot"]].state, RR.PAUSED)

    def test_all_members_visible_is_a_visible_set(self):
        state, _ = RR.Replicator(FakeRemote(), RR.MemoryStateStore()).replicate_set(
            "site-b", "set-1", [fixture()])
        self.assertEqual(state, RR.VISIBLE)


class WhatRetentionIsToldNotToDelete(unittest.TestCase):

    def test_a_set_not_yet_delivered_is_pinned_and_a_delivered_one_is_not(self):
        store = RR.MemoryStateStore()
        store.put("site-b", "set-1", "snap-a", {"state": RR.VISIBLE})
        store.put("site-b", "set-2", "snap-b", {"state": RR.PAUSED})
        pinned = RR.pinned_sets(store, "site-b", {"set-1": ["snap-a"], "set-2": ["snap-b"],
                                                  "set-3": ["snap-c"]})
        self.assertEqual(sorted(pinned), ["set-2", "set-3"])
        self.assertIn("site-b", pinned["set-2"])

    def test_a_set_with_one_member_delivered_and_one_not_is_still_pinned(self):
        store = RR.MemoryStateStore()
        store.put("site-b", "set-1", "a", {"state": RR.VISIBLE})
        self.assertEqual(list(RR.pinned_sets(store, "site-b", {"set-1": ["a", "b"]})), ["set-1"])

    def test_the_pins_plug_into_the_set_retention_that_already_exists(self):
        import rauru_protection as RP
        sets = [{"set_id": "s1", "taken_at_ms": 2, "origin": "policy", "state": "complete",
                 "members": [{"snapshot": "a-1"}]},
                {"set_id": "s2", "taken_at_ms": 1, "origin": "policy", "state": "complete",
                 "members": [{"snapshot": "a-2"}]}]
        store = RR.MemoryStateStore()
        pinned = RR.pinned_sets(store, "site-b", {"s2": ["a-2"]})
        plan = RP.plan_set_retention(sets, 1, set(), {}, pinned)
        self.assertEqual(plan.prune, [])


class TheWiring(unittest.TestCase):

    def test_the_module_is_a_library_it_imports_no_daemon_and_holds_no_candidacy(self):
        with io.open(os.path.join(HERE, "rauru_replication.py"), encoding="utf-8") as h:
            source = h.read()
        for needle in ("import rauru\n", "from rauru ", "candidacy", "is_zookeeper_leader",
                       "leader_ip", "time.time", "datetime"):
            self.assertNotIn(needle, source.replace("holds no candidacy", ""), needle)

    def test_no_migration_creates_replication_tables_until_a_second_site_exists(self):
        import helios_schema
        for m in helios_schema.MIGRATIONS:
            self.assertFalse(re.match(r"003[3-9]", m["id"]), m["id"])


if __name__ == "__main__":
    unittest.main()
