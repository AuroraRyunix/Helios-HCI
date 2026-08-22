#!/usr/bin/env python3
"""Tests for Saga, the metadata backup/restore tool.

Every assertion here corresponds to a way a backup system ships broken, and most of
them to a way this one nearly did:

  * Retention that can delete the last good artefact. A count-based window that lets
    corrupt files occupy its slots, or an age-based window with no floor, empties the
    target during exactly the quiet month where nobody is looking.
  * A failed backup that reports success. The two halves are an artefact left at the
    final name after a half-finished write, and a run that dies after the snapshot and
    leaves it pinned on the data disk.
  * A restore into a schema that is not the one the SSTables were written against.
    Scylla accepts them; the columns that moved simply read wrong afterwards, and
    nothing anywhere says so.
  * An artefact that has rotted since it was written and is trusted anyway.

`FakeShell` stands in for every external command. Its default responses are the real
output observed on a live single-node cluster (Scylla 5.4.0, LINSTOR 1.31.0) -- the
`podman inspect` mount list, the `nodetool listsnapshots` column layout whose size
fields carry a space, cqlsh's `SELECT JSON` framing with its replication-factor warning
block. Where a test needs a command to fail, it overrides just that one.

Run with:  python -m unittest test_saga_backup
"""

import contextlib
import io
import json
import os
import shutil
import tarfile
import tempfile
import time
import unittest

import saga


# ---------------------------------------------------------------------------------
# A stand-in for the host
# ---------------------------------------------------------------------------------

SCYLLA_MOUNTS = json.dumps([
    {"Type": "bind", "Source": "/var/lib/hci/hydra/data", "Destination": "/var/lib/scylla"},
])

# cqlsh prints a warning block before the rows on a cluster running below the
# recommended replication factor -- which every single-node Helios cluster does. The
# JSON-line filter has to survive it.
CQLSH_WARNING = "Warnings :\nUsing Replication Factor replication_factor=1 lower than " \
                "the minimum_replication_factor_warn_threshold=3 is not recommended.\n\n"

MIGRATIONS_OUT = CQLSH_WARNING + """
 [json]
------------------------------------------------------------
 {"id": "0001-baseline", "checksum": "aaaa"}
 {"id": "0002-cluster-locks", "checksum": "bbbb"}

(2 rows)
"""

KEYSPACES_OUT = """
 [json]
-------
 {"keyspace_name": "hydra", "replication": {"class": "SimpleStrategy", "replication_factor": "1"}}

(1 rows)
"""

TABLES_OUT = """
 [json]
-------
 {"table_name": "vms", "id": "5d7b1420-9a7e-11f1-b832-ba30b24e98e1"}
 {"table_name": "vm_nvram", "id": "5e15a670-9a7e-11f1-b832-ba30b24e98e1"}

(2 rows)
"""

SETTINGS_OUT = """
 [json]
-------
 {"key": "saga_target", "value": "/mnt/backup"}

(1 rows)
"""

DESCRIBE_OUT = """
CREATE KEYSPACE hydra WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE hydra.vms (
    name text PRIMARY KEY,
    vcpu int
);
"""

# Verbatim shape from `nodetool listsnapshots` on the live node. The size columns
# contain a space, which is why only the first three fields can be read positionally.
LISTSNAPSHOTS_OUT = """Snapshot Details:
Snapshot name          Keyspace name Column family name  True size Size on disk
pre-drop-1787345836577 hydra         urbosa_transit_pool 80 KB     80 KB
saga-20260822T101500Z  hydra         vms                 40 KB     40 KB

Total TrueDiskSpaceUsed: 452 KiB

"""

NODETOOL_STATUS_OUT = """Datacenter: datacenter1
=======================
UN  10.10.102.41  2.51 MB    256          ?       3cf7d7ff-0549-4977-9595-3e1530130b08  rack1
"""


def classify(argv):
    """Which external command this is, in terms the fake can answer.

    Two shapes for nodetool: `/usr/local/bin/nodetool ...` when the repo's wrapper is
    installed, and `podman exec -i systemd-hydra-db nodetool ...` when it is not. Saga
    picks between them at run time, so the fake has to recognise both.
    """
    argv = list(argv)
    if not argv:
        return "unknown"
    if argv[0].endswith("nodetool"):
        return _nodetool_kind(argv)
    if argv[0] == "podman":
        if "inspect" in argv:
            return "inspect"
        if "ps" in argv:
            return "ps"
        if "linstor" in argv:
            return "linstor"
        if "cqlsh" in argv:
            return _cql_kind(argv)
        if "nodetool" in argv:
            return _nodetool_kind(argv)
    return "unknown"


def _cql_kind(argv):
    statement = argv[-1]
    if "DESCRIBE" in statement:
        return "describe"
    if "schema_migrations" in statement:
        return "migrations"
    if "system_schema.keyspaces" in statement:
        return "keyspaces"
    if "system_schema.tables" in statement:
        return "tables"
    if "cluster_settings" in statement:
        return "settings"
    return "cql"


def _nodetool_kind(argv):
    for word in ("clearsnapshot", "listsnapshots", "snapshot", "status", "refresh"):
        if word in argv:
            return word
    return "nodetool"


DEFAULTS = {
    "inspect": (0, SCYLLA_MOUNTS, ""),
    "ps": (0, "systemd-zookeeper\nsystemd-hydra-db\nsystemd-aether\n", ""),
    "describe": (0, DESCRIBE_OUT, ""),
    "migrations": (0, MIGRATIONS_OUT, ""),
    "keyspaces": (0, KEYSPACES_OUT, ""),
    "tables": (0, TABLES_OUT, ""),
    "settings": (0, SETTINGS_OUT, ""),
    "cql": (0, "", ""),
    "snapshot": (0, "Requested creating snapshot(s) for [hydra]\n", ""),
    "clearsnapshot": (0, "Requested clearing snapshot(s)\n", ""),
    "listsnapshots": (0, LISTSNAPSHOTS_OUT, ""),
    "status": (0, NODETOOL_STATUS_OUT, ""),
    "refresh": (0, "", ""),
    "linstor": (0, "SUCCESS: Database backup created\n", ""),
    "unknown": (0, "", ""),
}


class FakeShell:
    """Answers with observed output, records every call, and fails where told to."""

    def __init__(self, **overrides):
        self.calls = []
        self.overrides = dict(overrides)

    def run(self, argv, timeout=None, stdin_data=None):
        argv = list(argv)
        self.calls.append(argv)
        kind = classify(argv)
        return self.overrides.get(kind, DEFAULTS.get(kind, (0, "", "")))

    def kinds(self):
        return [classify(argv) for argv in self.calls]


# ---------------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------------

def make_entry(node, epoch, healthy=True, partial=False, reason=""):
    stamp = saga.utc_stamp(epoch)
    name = saga.artefact_name("hci-01", node, stamp)
    if partial:
        name = name[: -len(saga.ARCHIVE_SUFFIX)] + saga.PARTIAL_SUFFIX
    return {
        "cluster": "hci_01",
        "node": saga.sanitize(node),
        "stamp": stamp,
        "epoch": epoch,
        "name": name,
        "path": "/mnt/backup/" + name,
        "partial": partial,
        "healthy": healthy,
        "health_reason": reason,
        "bytes": 1024,
    }


class TempTree(unittest.TestCase):
    """A scratch host: a data directory, an /etc/hci, and a backup target."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="saga-test-")
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.data = os.path.join(self.root, "scylla")
        self.target = os.path.join(self.root, "target")
        self.etc = os.path.join(self.root, "etc-hci")
        for path in (self.data, self.target, self.etc):
            os.makedirs(path)

        self.cluster_json = os.path.join(self.etc, "cluster.json")
        with open(self.cluster_json, "w") as handle:
            json.dump({"cluster_name": "hci-01", "redundancy_factor": 0,
                       "vip": "10.10.102.45",
                       "hosts": [{"node_id": 1, "ip": "10.10.102.41",
                                  "hostname": "Valkyrie-997A49"}]}, handle)
        os.makedirs(os.path.join(self.etc, "spark", "certs"))
        with open(os.path.join(self.etc, "spark", "certs", "node.key"), "w") as handle:
            handle.write("-----BEGIN PRIVATE KEY-----\nnot really\n")
        with open(os.path.join(self.etc, "spark", "certs", "ca.crt"), "w") as handle:
            handle.write("-----BEGIN CERTIFICATE-----\nnot really\n")

    def plant_snapshot(self, tag, tables=("vms", "vm_nvram"), files=2):
        """The directory layout `nodetool snapshot` leaves behind, as observed:
        <data>/data/<ks>/<table>-<uuid-without-dashes>/snapshots/<tag>/"""
        for index, table in enumerate(tables):
            uuid = "%032x" % (0xABCDEF + index)
            snap = os.path.join(self.data, "data", "hydra",
                                "%s-%s" % (table, uuid), "snapshots", tag)
            os.makedirs(snap, exist_ok=True)
            for number in range(files):
                with open(os.path.join(snap, "me-%d-big-Data.db" % number), "w") as handle:
                    handle.write("sstable %s %d\n" % (table, number))

    def build_saga(self, shell=None, address="10.10.102.41"):
        instance = saga.Saga(shell=shell or FakeShell(), address=address,
                             etc_hci=self.etc, certs_staging=os.path.join(self.root, "nope"),
                             cluster_json=self.cluster_json)
        instance._data_dir = self.data
        return instance

    def quiet_backup(self, instance, **kwargs):
        kwargs.setdefault("allow_same_filesystem", True)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            result = instance.backup(self.target, **kwargs)
        return result, buffer.getvalue()


# ---------------------------------------------------------------------------------
# Artefact naming
# ---------------------------------------------------------------------------------

class ArtefactNamingTest(unittest.TestCase):

    def test_name_round_trips(self):
        name = saga.artefact_name("hci-01", "10.10.102.41", "20260822T101500Z")
        fields = saga.parse_artefact_name(name)
        self.assertIsNotNone(fields)
        self.assertEqual(fields["cluster"], "hci_01")
        self.assertEqual(fields["node"], "10_10_102_41")
        self.assertEqual(fields["stamp"], "20260822T101500Z")

    def test_the_stamp_is_read_as_utc(self):
        """Retention arithmetic is in epoch seconds. Parsing the stamp as local time
        would shift every age by the host's UTC offset -- and by a different amount
        either side of a DST boundary, so the same artefact would age at two speeds."""
        epoch = 1787_400_000
        self.assertEqual(saga.parse_stamp(saga.utc_stamp(epoch)), epoch)

    def test_foreign_files_are_never_artefacts(self):
        """The target may be a shared NFS export. Anything the parser accepts is
        something retention may later delete, so it accepts only our own shape."""
        for name in ("README.txt", "backup.tar.gz", "saga-hci_01-10_10_102_41.tar.gz",
                     "saga-hci_01-10_10_102_41-notastamp.tar.gz",
                     "saga-hci_01-10_10_102_41-20260822T101500Z.tar",
                     "linstordb.zip"):
            with self.subTest(name=name):
                self.assertIsNone(saga.parse_artefact_name(name))

    def test_an_impossible_stamp_is_rejected(self):
        self.assertIsNone(saga.parse_artefact_name(
            "saga-hci_01-10_10_102_41-20261322T101500Z.tar.gz"))


# ---------------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------------

class RetentionTest(unittest.TestCase):

    NOW = 1_787_500_000

    def entries(self, count, node="10.10.102.41", spacing=86400, healthy=True,
                start_age_days=0):
        return [make_entry(node,
                           self.NOW - int((start_age_days * 86400) + index * spacing),
                           healthy=healthy)
                for index in range(count)]

    def test_prunes_by_count_and_keeps_the_newest(self):
        entries = self.entries(10)
        retained, removals = saga.retention_plan(entries, "10.10.102.41", keep=3,
                                                 keep_days=0, now_epoch=self.NOW)
        self.assertEqual(len(retained), 3)
        self.assertEqual(len(removals), 7)
        kept = {e["epoch"] for e in retained}
        self.assertEqual(kept, {e["epoch"] for e in entries[:3]},
                         "the three artefacts kept must be the three newest")
        self.assertLess(max(pair[0]["epoch"] for pair in removals), min(kept))

    def test_the_newest_survive_even_when_everything_is_past_the_age_limit(self):
        """The failure this exists for: a cluster nobody backed up for a year, where
        an age-only policy wakes up and deletes the entire target."""
        entries = self.entries(5, start_age_days=400)
        retained, removals = saga.retention_plan(entries, "10.10.102.41", keep=3,
                                                 keep_days=30, now_epoch=self.NOW)
        self.assertEqual(len(retained), 3)
        self.assertEqual(len(removals), 2)
        self.assertTrue(all(pair[0]["epoch"] < min(e["epoch"] for e in retained)
                            for pair in removals))

    def test_nothing_inside_the_age_window_is_removed(self):
        entries = self.entries(5)
        retained, removals = saga.retention_plan(entries, "10.10.102.41", keep=2,
                                                 keep_days=30, now_epoch=self.NOW)
        self.assertEqual(removals, [])
        self.assertEqual(len(retained), 5)

    def test_a_corrupt_artefact_does_not_occupy_a_retention_slot(self):
        """Three bad nights must not push the last good backup out of a keep=2 window.
        A corrupt file is not a backup, and counting it as one is how a target ends up
        holding nothing but rubble."""
        good_old = make_entry("10.10.102.41", self.NOW - 40 * 86400)
        good_older = make_entry("10.10.102.41", self.NOW - 41 * 86400)
        bad = [make_entry("10.10.102.41", self.NOW - day * 86400, healthy=False,
                          reason="size mismatch") for day in (1, 2, 3)]
        retained, _removals = saga.retention_plan(bad + [good_old, good_older],
                                                  "10.10.102.41", keep=2, keep_days=30,
                                                  now_epoch=self.NOW)
        kept = {e["epoch"] for e in retained}
        self.assertIn(good_old["epoch"], kept)
        self.assertIn(good_older["epoch"], kept)

    def test_a_corrupt_artefact_is_still_removed_once_it_ages_out(self):
        bad = make_entry("10.10.102.41", self.NOW - 90 * 86400, healthy=False,
                         reason="no manifest")
        good = self.entries(2)
        _retained, removals = saga.retention_plan(good + [bad], "10.10.102.41",
                                                  keep=2, keep_days=30,
                                                  now_epoch=self.NOW)
        self.assertEqual([pair[0]["name"] for pair in removals], [bad["name"]])
        self.assertIn("unusable", removals[0][1])

    def test_a_node_never_prunes_a_peers_artefacts(self):
        """The target is shared. A node applying its own keep count across every node's
        artefacts would delete a peer's backups using a number the peer never agreed
        to."""
        mine = self.entries(2, node="10.10.102.41")
        theirs = self.entries(9, node="10.10.102.42", start_age_days=200)
        retained, removals = saga.retention_plan(mine + theirs, "10.10.102.41", keep=1,
                                                 keep_days=0, now_epoch=self.NOW)
        removed = {pair[0]["node"] for pair in removals}
        self.assertEqual(removed, {"10_10_102_41"})
        self.assertEqual(len([e for e in retained if e["node"] == "10_10_102_42"]), 9)

    def test_abandoned_partials_are_swept_but_fresh_ones_are_left(self):
        """A .partial is an interrupted run. Sweeping a young one would delete the
        artefact a backup running right now is still writing."""
        stale = make_entry("10.10.102.41", self.NOW - 3 * 86400, partial=True)
        fresh = make_entry("10.10.102.41", self.NOW - 60, partial=True)
        retained, removals = saga.retention_plan([stale, fresh], "10.10.102.41",
                                                 keep=3, keep_days=30,
                                                 now_epoch=self.NOW)
        self.assertEqual([pair[0]["name"] for pair in removals], [stale["name"]])
        self.assertIn("abandoned partial", removals[0][1])
        self.assertEqual([e["name"] for e in retained], [fresh["name"]])

    def test_keeping_nothing_is_refused(self):
        """A retention policy that is allowed to keep zero artefacts is a delete
        command with a schedule attached."""
        for keep in (0, -1, None):
            with self.subTest(keep=keep):
                with self.assertRaises(ValueError):
                    saga.retention_plan(self.entries(3), "10.10.102.41", keep=keep,
                                        keep_days=30, now_epoch=self.NOW)


class RetentionOnDiskTest(TempTree):
    """Retention against real files, including the sidecars."""

    def test_pruning_removes_the_manifest_with_the_archive(self):
        now = int(time.time())
        for age_days in (1, 2, 3, 40, 41):
            stamp = saga.utc_stamp(now - age_days * 86400)
            archive = os.path.join(self.target,
                                   saga.artefact_name("hci-01", "10.10.102.41", stamp))
            source = os.path.join(self.root, "payload-%d" % age_days)
            with open(source, "w") as handle:
                handle.write("payload %d" % age_days)
            saga.write_artefact([("meta/x.txt", source)], archive,
                                {"cluster_name": "hci-01"})

        entries = saga.scan_target(self.target)
        self.assertEqual(len(entries), 5)
        self.assertTrue(all(e["healthy"] for e in entries))

        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            saga.apply_retention(entries, "10.10.102.41", keep=2, keep_days=30,
                                 now_epoch=now)

        left = sorted(os.listdir(self.target))
        self.assertEqual(len(saga.scan_target(self.target)), 3)
        self.assertFalse([n for n in left if n.endswith(saga.MANIFEST_SUFFIX)
                          and n[: -len(saga.MANIFEST_SUFFIX)] + saga.ARCHIVE_SUFFIX
                          not in left],
                         "a manifest was left behind without its archive")

    def test_an_archive_whose_size_changed_reads_as_unhealthy(self):
        stamp = saga.utc_stamp()
        archive = os.path.join(self.target,
                               saga.artefact_name("hci-01", "10.10.102.41", stamp))
        source = os.path.join(self.root, "payload")
        with open(source, "w") as handle:
            handle.write("payload")
        saga.write_artefact([("meta/x.txt", source)], archive, {})
        with open(archive, "ab") as handle:
            handle.write(b"junk appended by something else")
        healthy, reason = saga.quick_health(archive)
        self.assertFalse(healthy)
        self.assertIn("manifest says", reason)


# ---------------------------------------------------------------------------------
# Artefact integrity
# ---------------------------------------------------------------------------------

class ArtefactIntegrityTest(TempTree):

    def build(self, payload=b"a" * 4096, name="scylla/vms/me-1-big-Data.db"):
        source = os.path.join(self.root, "payload.bin")
        with open(source, "wb") as handle:
            handle.write(payload)
        archive = os.path.join(
            self.target, saga.artefact_name("hci-01", "10.10.102.41", saga.utc_stamp()))
        manifest = saga.write_artefact([(name, source)], archive,
                                       {"cluster_name": "hci-01", "keyspace": "hydra"})
        return archive, manifest, source

    def test_a_freshly_written_artefact_verifies(self):
        archive, manifest, _source = self.build()
        found, problems = saga.verify_artefact(archive)
        self.assertEqual(problems, [])
        self.assertEqual(found["archive"]["sha256"], manifest["archive"]["sha256"])

    def test_the_partial_name_is_never_left_at_the_final_name(self):
        """A half-written archive carrying the final name is indistinguishable from a
        good backup, and the next retention pass counts it as one of the N it keeps."""
        archive = os.path.join(
            self.target, saga.artefact_name("hci-01", "10.10.102.41", saga.utc_stamp()))
        missing = os.path.join(self.root, "does-not-exist")
        with open(missing, "w") as handle:
            handle.write("briefly")
        os.remove(missing)
        with self.assertRaises(OSError):
            saga.write_artefact([("scylla/vms/gone", missing)], archive, {})
        self.assertFalse(os.path.exists(archive))
        self.assertEqual(saga.scan_target(self.target), [])

    def test_truncation_is_detected(self):
        archive, _manifest, _source = self.build()
        size = os.path.getsize(archive)
        with open(archive, "r+b") as handle:
            handle.truncate(size // 2)
        _found, problems = saga.verify_artefact(archive)
        self.assertTrue(problems)
        self.assertTrue(any("truncated" in p or "sha256" in p for p in problems),
                        problems)

    def test_a_tampered_member_is_detected(self):
        """The archive is rebuilt with different contents under the same member name
        and the same byte count, so only the per-member digest catches it."""
        archive, manifest, source = self.build(payload=b"a" * 4096)
        sidecar = saga.manifest_path_for(archive)
        with open(sidecar, "r") as handle:
            original = json.load(handle)

        with open(source, "wb") as handle:
            handle.write(b"b" * 4096)
        os.remove(archive)
        os.remove(sidecar)
        saga.write_artefact([("scylla/vms/me-1-big-Data.db", source)], archive, {})
        # Put the original manifest back: this is the artefact somebody swapped the
        # payload inside while keeping the paperwork.
        with open(sidecar, "w") as handle:
            json.dump(original, handle)

        _found, problems = saga.verify_artefact(archive)
        self.assertTrue(problems)
        self.assertTrue(any("sha256" in p for p in problems), problems)

    def test_a_member_added_after_the_fact_is_detected(self):
        archive, _manifest, _source = self.build()
        rebuilt = archive + ".rebuilt"
        with tarfile.open(archive, "r:gz") as src, tarfile.open(rebuilt, "w:gz") as dst:
            for member in src:
                data = src.extractfile(member) if member.isfile() else None
                dst.addfile(member, data)
            extra = os.path.join(self.root, "extra.txt")
            with open(extra, "w") as handle:
                handle.write("smuggled")
            dst.add(extra, arcname="scylla/vms/extra.txt")
        os.replace(rebuilt, archive)
        _found, problems = saga.verify_artefact(archive)
        self.assertTrue(any("not in the manifest" in p for p in problems), problems)

    def test_a_missing_sidecar_is_a_failure_not_a_pass(self):
        archive, _manifest, _source = self.build()
        os.remove(saga.manifest_path_for(archive))
        found, problems = saga.verify_artefact(archive)
        self.assertIsNone(found)
        self.assertTrue(problems)


# ---------------------------------------------------------------------------------
# The backup driver
# ---------------------------------------------------------------------------------

class BackupTest(TempTree):

    ROUND = "20260822T101500Z"

    def test_a_complete_backup_produces_a_verifiable_artefact(self):
        shell = FakeShell()
        instance = self.build_saga(shell)
        self.plant_snapshot(saga.SNAPSHOT_PREFIX + self.ROUND)
        manifest, _out = self.quiet_backup(instance, round_tag=self.ROUND)

        archive = os.path.join(self.target, manifest["archive"]["name"])
        self.assertTrue(os.path.exists(archive))
        _found, problems = saga.verify_artefact(archive)
        self.assertEqual(problems, [])

        self.assertEqual(sorted(manifest["tables"]), ["vm_nvram", "vms"])
        self.assertEqual(manifest["schema_migrations"],
                         {"0001-baseline": "aaaa", "0002-cluster-locks": "bbbb"})
        self.assertEqual(manifest["cluster_json"]["vip"], "10.10.102.45")
        self.assertFalse(manifest["covers_guest_data"],
                         "the manifest must state plainly that guest data is not in it")
        names = {m["name"] for m in manifest["members"]}
        self.assertIn("meta/schema.cql", names)
        self.assertIn("etc-hci/cluster.json", names)

    def test_the_snapshot_is_cleared_when_the_backup_succeeds(self):
        shell = FakeShell()
        instance = self.build_saga(shell)
        self.plant_snapshot(saga.SNAPSHOT_PREFIX + self.ROUND)
        self.quiet_backup(instance, round_tag=self.ROUND)
        self.assertIn("clearsnapshot", shell.kinds())

    def test_private_keys_are_left_out_unless_asked_for(self):
        """An artefact on a shared NFS export that quietly contains every node's TLS
        key is a different kind of object from one that contains metadata."""
        shell = FakeShell()
        instance = self.build_saga(shell)
        self.plant_snapshot(saga.SNAPSHOT_PREFIX + self.ROUND)
        manifest, _out = self.quiet_backup(instance, round_tag=self.ROUND)
        names = {m["name"] for m in manifest["members"]}
        self.assertNotIn("etc-hci/spark/certs/node.key", names)
        self.assertIn("etc-hci/spark/certs/ca.crt", names)
        self.assertFalse(manifest["contains_ca"])
        self.assertTrue(any("private key" in note for note in manifest["notes"]))

    def test_a_failed_snapshot_is_a_failed_backup(self):
        """nodetool exits non-zero and the run must stop there. Writing an artefact
        from whatever files happened to be lying around would produce a backup that
        verifies and restores the wrong point in time."""
        shell = FakeShell(snapshot=(1, "", "nodetool: Failed to connect to '127.0.0.1:7199'"))
        instance = self.build_saga(shell)
        with self.assertRaises(saga.SagaError) as caught:
            self.quiet_backup(instance, round_tag=self.ROUND)
        self.assertIn("nodetool snapshot failed", str(caught.exception))
        self.assertEqual(os.listdir(self.target), [],
                         "a failed backup must leave nothing at the target")

    def test_an_empty_snapshot_is_a_failed_backup(self):
        """The snapshot command succeeded but produced no files. Archiving that would
        write a well-formed artefact containing no data, which verifies cleanly and
        restores nothing."""
        shell = FakeShell()
        instance = self.build_saga(shell)
        # The keyspace exists on disk with its table directories, but the snapshot tag
        # left nothing behind -- what a silently no-op snapshot looks like.
        self.plant_snapshot("some-other-tag")
        with self.assertRaises(saga.SagaError) as caught:
            self.quiet_backup(instance, round_tag=self.ROUND)
        self.assertIn("produced no files", str(caught.exception))
        self.assertEqual(os.listdir(self.target), [])

    def test_a_failure_after_the_snapshot_still_clears_it(self):
        """A snapshot is hardlinks, so it is free until it isn't: it pins every SSTable
        it references, and compaction can no longer release the space. A tool that
        leaks one on every failure fills the disk the database is on."""
        # Fail on the replication read, which happens after the snapshot exists and
        # after its files have been gathered -- the deepest point in the run.
        shell = FakeShell(keyspaces=(1, "", "connection refused"))
        instance = self.build_saga(shell)
        self.plant_snapshot(saga.SNAPSHOT_PREFIX + self.ROUND)
        with self.assertRaises(saga.SagaError):
            self.quiet_backup(instance, round_tag=self.ROUND)
        self.assertIn("clearsnapshot", shell.kinds())
        self.assertEqual(os.listdir(self.target), [],
                         "a run that died mid-way must leave no artefact behind")

    def test_an_unreadable_migration_ledger_stops_the_backup(self):
        """Without hydra.schema_migrations the artefact cannot say what shape its
        tables had, so no later restore can check compatibility. A backup nobody can
        verify against a schema is the false-confidence case this whole tool exists to
        avoid."""
        shell = FakeShell(migrations=(0, "\n (0 rows)\n", ""))
        instance = self.build_saga(shell)
        with self.assertRaises(saga.SagaError) as caught:
            self.quiet_backup(instance, round_tag=self.ROUND)
        self.assertIn("schema_migrations", str(caught.exception))
        self.assertNotIn("snapshot", shell.kinds(),
                         "the schema is read before the snapshot is taken")

    def test_a_missing_cluster_json_stops_the_backup(self):
        os.remove(self.cluster_json)
        instance = self.build_saga(FakeShell())
        with self.assertRaises(saga.SagaError) as caught:
            self.quiet_backup(instance, round_tag=self.ROUND)
        self.assertIn("cluster.json", str(caught.exception))

    def test_the_manifest_no_longer_claims_a_controller_database(self):
        """There is no second database to capture.

        LINSTOR kept its own, on exactly one node, so an artefact from any other node was
        incomplete and had to say so. Sidon has none: the map lives in the keyspace this
        already snapshots, so a per-node artefact is complete on every node -- and a
        manifest still advertising the old field would have a restore looking for a member
        that no artefact will ever contain.
        """
        shell = FakeShell()
        instance = self.build_saga(shell)
        self.plant_snapshot(saga.SNAPSHOT_PREFIX + self.ROUND)
        manifest, _out = self.quiet_backup(instance, round_tag=self.ROUND)
        self.assertNotIn("contains_linstor_db", manifest)
        self.assertNotIn("linstor_note", manifest)


class TargetTest(TempTree):

    def test_a_target_on_the_database_filesystem_is_refused(self):
        """A backup on the disk it protects survives only the failures that were never
        going to lose the data anyway, and competes for the space whose exhaustion
        stops the cluster."""
        with self.assertRaises(saga.TargetUnusable) as caught:
            saga.check_target(self.target, self.data,
                              stat_fn=lambda p: os.stat_result(
                                  (0, 0, 7, 0, 0, 0, 0, 0, 0, 0)))
        self.assertIn("same filesystem", str(caught.exception))

    def test_the_refusal_can_be_overridden_and_is_recorded(self):
        facts = saga.check_target(
            self.target, self.data, allow_same_filesystem=True,
            stat_fn=lambda p: os.stat_result((0, 0, 7, 0, 0, 0, 0, 0, 0, 0)))
        self.assertTrue(facts["same_filesystem"])

    def test_a_missing_target_is_refused_rather_than_created(self):
        """A target that is a mount point with nothing mounted on it looks exactly like
        a missing directory, and silently creating it fills the root filesystem with
        backups nobody will find."""
        with self.assertRaises(saga.TargetUnusable):
            saga.check_target(os.path.join(self.root, "not-mounted"), self.data)

    def test_no_target_at_all_is_refused(self):
        with self.assertRaises(saga.TargetUnusable):
            saga.check_target("", self.data)


# ---------------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------------

class SchemaCompatibilityTest(unittest.TestCase):

    def test_identical_ledgers_pass(self):
        ledger = {"0001-baseline": "aa", "0002-cluster-locks": "bb"}
        self.assertEqual(saga.check_schema_compatible(ledger, dict(ledger)), [])

    def test_a_cluster_ahead_of_the_backup_is_refused(self):
        with self.assertRaises(saga.RestoreRefused) as caught:
            saga.check_schema_compatible({"0001": "aa"},
                                         {"0001": "aa", "0004": "dd"})
        self.assertIn("0004", str(caught.exception))

    def test_a_cluster_behind_the_backup_is_refused(self):
        with self.assertRaises(saga.RestoreRefused) as caught:
            saga.check_schema_compatible({"0001": "aa", "0004": "dd"},
                                         {"0001": "aa"})
        self.assertIn("0004", str(caught.exception))

    def test_an_edited_migration_is_refused(self):
        """Same id, different DDL: two clusters that both believe they are current and
        have different tables. helios_schema raises SchemaDivergence for the same
        reason."""
        with self.assertRaises(saga.RestoreRefused) as caught:
            saga.check_schema_compatible({"0001": "aa"}, {"0001": "zz"})
        self.assertIn("different DDL", str(caught.exception))

    def test_force_downgrades_the_refusal_to_a_reported_warning(self):
        warnings = saga.check_schema_compatible({"0001": "aa"},
                                                {"0001": "aa", "0004": "dd"},
                                                force=True)
        self.assertTrue(warnings)
        self.assertTrue(any("0004" in line for line in warnings))


class RestoreTest(TempTree):

    ROUND = "20260822T101500Z"

    def make_artefact(self, shell=None):
        shell = shell or FakeShell()
        instance = self.build_saga(shell)
        self.plant_snapshot(saga.SNAPSHOT_PREFIX + self.ROUND)
        manifest, _out = self.quiet_backup(instance, round_tag=self.ROUND)
        return os.path.join(self.target, manifest["archive"]["name"])

    def test_a_restore_into_a_different_schema_is_refused(self):
        archive = self.make_artefact()
        moved_on = MIGRATIONS_OUT.rstrip() + '\n {"id": "0005-new", "checksum": "eeee"}\n'
        shell = FakeShell(migrations=(0, moved_on, ""))
        instance = self.build_saga(shell)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            with self.assertRaises(saga.RestoreRefused) as caught:
                instance.restore(archive)
        self.assertIn("0005-new", str(caught.exception))
        self.assertNotIn("refresh", shell.kinds(),
                         "nothing may be loaded before the schema check passes")

    def test_a_corrupt_artefact_is_refused_before_anything_is_loaded(self):
        archive = self.make_artefact()
        with open(archive, "r+b") as handle:
            handle.truncate(os.path.getsize(archive) // 2)
        shell = FakeShell()
        instance = self.build_saga(shell)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            with self.assertRaises(saga.RestoreRefused):
                instance.restore(archive)
        self.assertNotIn("refresh", shell.kinds())

    def test_a_matching_artefact_is_loaded_through_refresh(self):
        archive = self.make_artefact()
        shell = FakeShell()
        instance = self.build_saga(shell)
        # The live table directory carries the *current* table id. A table that has
        # been dropped and recreated leaves the old directory behind, and copying into
        # that one is a restore whose data never appears.
        live = os.path.join(self.data, "data", "hydra",
                            "vms-5d7b14209a7e11f1b832ba30b24e98e1")
        stale = os.path.join(self.data, "data", "hydra",
                             "vms-0000000000000000000000000000dead")
        os.makedirs(live)
        os.makedirs(stale)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            restored, skipped = instance.restore(archive, tables=["vms"])
        self.assertEqual([r[0] for r in restored], ["vms"])
        self.assertTrue(os.listdir(os.path.join(live, "upload")))
        self.assertFalse(os.path.exists(os.path.join(stale, "upload")),
                         "the stale directory of a recreated table must be untouched")
        self.assertIn("refresh", shell.kinds())

    def test_a_table_the_live_schema_does_not_have_is_skipped_not_guessed_at(self):
        archive = self.make_artefact()
        instance = self.build_saga(FakeShell())
        os.makedirs(os.path.join(self.data, "data", "hydra",
                                 "vms-5d7b14209a7e11f1b832ba30b24e98e1"))
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            _restored, skipped = instance.restore(archive)
        self.assertIn("vm_nvram", [name for name, _reason in skipped])


# ---------------------------------------------------------------------------------
# nodetool output
# ---------------------------------------------------------------------------------

class ListSnapshotsTest(unittest.TestCase):

    def test_rows_are_read_off_the_first_three_fields(self):
        rows = saga.parse_listsnapshots(LISTSNAPSHOTS_OUT)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["tag"], "pre-drop-1787345836577")
        self.assertEqual(rows[1], {"tag": "saga-20260822T101500Z",
                                   "keyspace": "hydra", "table": "vms"})

    def test_the_header_and_the_total_are_not_snapshots(self):
        """`Total TrueDiskSpaceUsed: 452 KiB` splits into three fields and would
        otherwise become a snapshot named 'Total' that the pruner tries to clear."""
        tags = {row["tag"] for row in saga.parse_listsnapshots(LISTSNAPSHOTS_OUT)}
        self.assertNotIn("Total", tags)
        self.assertNotIn("Snapshot", tags)

    def test_foreign_snapshot_tags_are_distinguishable_from_ours(self):
        """Scylla writes a pre-drop-* auto-snapshot on every DROP TABLE and never
        clears it. On the live cluster there were ten of them. They are the last copy
        of a dropped table, so --prune must not touch them."""
        rows = saga.parse_listsnapshots(LISTSNAPSHOTS_OUT)
        ours = [r for r in rows if r["tag"].startswith(saga.SNAPSHOT_PREFIX)]
        self.assertEqual(len(ours), 1)

    def test_an_empty_listing_yields_nothing(self):
        self.assertEqual(saga.parse_listsnapshots(""), [])
        self.assertEqual(saga.parse_listsnapshots("There are no snapshots"), [])


if __name__ == "__main__":
    unittest.main()
