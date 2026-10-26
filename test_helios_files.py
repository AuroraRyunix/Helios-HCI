#!/usr/bin/env python3
"""Unit tests for Helios Files: Scale-out HA NFSv4/SMB3 file services on Sidon storage."""

import importlib.util
import json
import os
import tempfile
import unittest
from unittest import mock

import helios_files
import helios_schema
import mipha

HERE = os.path.dirname(os.path.abspath(__file__))


def load_spark_daemon():
    spec = importlib.util.spec_from_file_location(
        "spark_daemon_files_test", os.path.join(HERE, "spark_daemon_decoded.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HeliosFilesTests(unittest.TestCase):
    def test_schema_includes_helios_files_migration(self):
        migration_ids = [m["id"] for m in helios_schema.MIGRATIONS]
        self.assertIn("0058-helios-files-shares", migration_ids)
        mig = next(m for m in helios_schema.MIGRATIONS if m["id"] == "0058-helios-files-shares")
        joined = " ".join(mig["statements"])
        self.assertIn("hydra.files_shares", joined)

    def test_validate_share_name_and_protocol(self):
        name, err = helios_files.validate_share_name("eng-shared_01")
        self.assertEqual(name, "eng-shared_01")
        self.assertIsNone(err)

        for invalid in ("", "UpperCase", "../escape", "-leading-dash", "a" * 60):
            res, err = helios_files.validate_share_name(invalid)
            self.assertIsNone(res, invalid)
            self.assertIsNotNone(err, invalid)

        for proto in ("nfs", "smb", "dual"):
            p, err = helios_files.validate_protocol(proto)
            self.assertEqual(p, proto)
            self.assertIsNone(err)

        p, err = helios_files.validate_protocol("ftp")
        self.assertIsNone(p)
        self.assertIsNotNone(err)

    def test_validate_allowed_clients_normalizes_cidrs_and_rejects_bad_input(self):
        clients, err = helios_files.validate_allowed_clients("10.10.102.0/24, 192.168.1.50")
        self.assertIsNone(err)
        self.assertEqual(clients, "10.10.102.0/24,192.168.1.50")

        bad, err = helios_files.validate_allowed_clients("10.10.102.999/24")
        self.assertIsNone(bad)
        self.assertIsNotNone(err)

    def test_render_nfs_and_samba_configs(self):
        nfs_line = helios_files.render_nfs_export_line(
            "media-vault", "/srv/helios-files/media-vault", "10.10.102.0/24,10.10.103.0/24", "no_root_squash"
        )
        self.assertIn("/srv/helios-files/media-vault", nfs_line)
        self.assertIn("10.10.102.0/24(rw,sync,no_subtree_check,no_root_squash,fsid=", nfs_line)
        self.assertIn("10.10.103.0/24(rw,sync,no_subtree_check,no_root_squash,fsid=", nfs_line)

        smb_conf = helios_files.render_samba_share_conf(
            "media-vault", "/srv/helios-files/media-vault", "10.10.102.0/24", guest_ok=False
        )
        self.assertIn("[media-vault]", smb_conf)
        self.assertIn("path = /srv/helios-files/media-vault", smb_conf)
        self.assertIn("guest ok = no", smb_conf)
        self.assertIn("hosts allow = 10.10.102.0/24", smb_conf)

    def test_activate_and_deactivate_share_writes_exports_and_reloads_daemons(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mount_root = os.path.join(tmpdir, "mnt")
            nfs_dir = os.path.join(tmpdir, "exports.d")
            smb_dir = os.path.join(tmpdir, "samba.d")
            smb_incl = os.path.join(tmpdir, "helios-shares.conf")

            ran_argv = []

            def fake_run_argv(argv, timeout=60):
                ran_argv.append(list(argv))
                return 0, "", ""

            with mock.patch.multiple(
                helios_files,
                FILES_MOUNT_ROOT=mount_root,
                NFS_EXPORTS_DIR=nfs_dir,
                SAMBA_SHARES_DIR=smb_dir,
                SAMBA_MAIN_INCL=smb_incl,
            ):
                export_path = os.path.join(mount_root, "team-docs")
                ok, err = helios_files.activate_share_on_host(
                    {
                        "share_name": "team-docs",
                        "vdisk_id": "files-team-docs",
                        "export_path": export_path,
                        "protocol": "dual",
                        "allowed_clients": "10.10.102.0/24",
                        "squash_mode": "root_squash",
                        "smb_guest_ok": False,
                    },
                    run_argv_fn=fake_run_argv,
                )
                self.assertTrue(ok, err)
                self.assertTrue(os.path.isfile(os.path.join(nfs_dir, "helios-team-docs.exports")))
                self.assertTrue(os.path.isfile(os.path.join(smb_dir, "team-docs.conf")))
                self.assertTrue(os.path.isfile(smb_incl))
                self.assertIn(["exportfs", "-ra"], ran_argv)
                self.assertIn(["smbcontrol", "all", "reload-config"], ran_argv)

                ok_d, err_d = helios_files.deactivate_share_on_host(
                    "team-docs", export_path=export_path, run_argv_fn=fake_run_argv
                )
                self.assertTrue(ok_d, err_d)
                self.assertFalse(os.path.exists(os.path.join(nfs_dir, "helios-team-docs.exports")))
                self.assertFalse(os.path.exists(os.path.join(smb_dir, "team-docs.conf")))

    def test_mipha_failover_orphaned_file_shares_claims_vdisk_and_activates_on_survivor(self):
        share_row = {
            "share_name": "corp-home",
            "vdisk_id": "files-corp-home",
            "export_path": "/srv/helios-files/corp-home",
            "protocol": "nfs",
            "allowed_clients": "*",
            "squash_mode": "root_squash",
            "host_ip": "10.10.102.41",
            "state": "ONLINE",
        }
        cql_calls = []
        spark_calls = []

        def fake_cql(q):
            cql_calls.append(q)
            if "SELECT JSON * FROM hydra.files_shares" in q:
                return 0, json.dumps(share_row) + "\n", ""
            return 0, "", ""

        def fake_spark(ip, path, payload, method="POST"):
            spark_calls.append((ip, path, payload))
            return 200, {"ok": True}, ""

        with mock.patch.object(mipha, "run_cql_query", side_effect=fake_cql), \
             mock.patch.object(mipha, "run_mtls_spark_api_full", side_effect=fake_spark):
            recovered = mipha.failover_orphaned_file_shares(
                "10.10.102.41",
                [{"ip": "10.10.102.41"}, {"ip": "10.10.102.42"}],
            )
        self.assertEqual(recovered, [("corp-home", "10.10.102.42")])
        self.assertEqual(spark_calls[0], ("10.10.102.42", "/api/v1/dfs/vdisk", {"op": "attach", "vdisk_id": "files-corp-home"}))
        self.assertEqual(spark_calls[1][0], "10.10.102.42")
        self.assertEqual(spark_calls[1][1], "/api/v1/host/files")
        self.assertTrue(any("UPDATE hydra.files_shares SET host_ip = '10.10.102.42'" in q for q in cql_calls))

    def test_spark_daemon_routes_host_files_endpoint(self):
        daemon = load_spark_daemon()
        self.assertTrue(hasattr(daemon.SparkDaemonHandler, "handle_host_files_get"))
        self.assertTrue(hasattr(daemon.SparkDaemonHandler, "handle_host_files_post"))


if __name__ == "__main__":
    unittest.main()
