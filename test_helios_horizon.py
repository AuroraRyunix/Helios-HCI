#!/usr/bin/env python3
"""Unit tests for Helios Horizon VDI broker, SidonRemote replication, and R-6/R-19 hardening."""

import importlib.util
import json
import os
import unittest
from unittest import mock

import helios_horizon
import helios_schema
import rauru_replication

HERE = os.path.dirname(os.path.abspath(__file__))


def load_spark_daemon():
    spec = importlib.util.spec_from_file_location(
        "spark_daemon_horizon_test", os.path.join(HERE, "spark_daemon_decoded.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HeliosHorizonTests(unittest.TestCase):
    def test_schema_includes_helios_horizon_migration(self):
        migration_ids = [m["id"] for m in helios_schema.MIGRATIONS]
        self.assertIn("0059-helios-horizon-vdi", migration_ids)
        mig = next(m for m in helios_schema.MIGRATIONS if m["id"] == "0059-helios-horizon-vdi")
        joined = " ".join(mig["statements"])
        self.assertIn("hydra.horizon_pools", joined)
        self.assertIn("hydra.horizon_sessions", joined)

    def test_validate_pool_fields(self):
        name, err = helios_horizon.validate_pool_name("eng-vdi_01")
        self.assertEqual(name, "eng-vdi_01")
        self.assertIsNone(err)

        for bad in ("", "Upper", "../escape", "-dash", "a" * 40):
            res, err = helios_horizon.validate_pool_name(bad)
            self.assertIsNone(res, bad)
            self.assertIsNotNone(err, bad)

        for ptype in ("instant_clone", "persistent"):
            val, err = helios_horizon.validate_pool_type(ptype)
            self.assertEqual(val, ptype)
            self.assertIsNone(err)

        for proto in ("rdp", "vnc", "spice"):
            val, err = helios_horizon.validate_display_protocol(proto)
            self.assertEqual(val, proto)
            self.assertIsNone(err)

    def test_user_entitlement_checks_wildcard_username_and_ad_groups(self):
        open_pool = {"entitled_groups": "*"}
        self.assertTrue(helios_horizon.user_is_entitled("alice", [], open_pool))
        self.assertFalse(helios_horizon.user_is_entitled("", [], open_pool))

        restricted = {"entitled_groups": "vdi-engineers, Domain Admins, bob"}
        self.assertTrue(helios_horizon.user_is_entitled("alice", ["VDI-Engineers"], restricted))
        self.assertTrue(helios_horizon.user_is_entitled("bob", [], restricted))
        self.assertFalse(helios_horizon.user_is_entitled("charlie", ["finance"], restricted))

    def test_sign_and_verify_horizon_session_token(self):
        session = {
            "session_id": "hzn-12345",
            "vm_name": "vdi-eng-001",
            "username": "alice",
            "protocol": "rdp",
            "host_ip": "10.10.102.41",
            "guacd_port": 4822,
        }
        pool = {
            "pool_name": "eng",
            "protocol": "rdp",
            "desktop_port": 3389,
            "domain_join": "CORP.LOCAL",
        }
        spec = helios_horizon.build_guacamole_connection_spec(session, pool, "192.168.100.15")
        self.assertEqual(spec["parameters"]["hostname"], "192.168.100.15")
        self.assertEqual(spec["parameters"]["domain"], "CORP.LOCAL")

        token = helios_horizon.sign_horizon_session_token(
            spec, "cluster-secret-key", ttl_seconds=600, now_s=1000
        )
        decoded, err = helios_horizon.verify_horizon_session_token(
            token, "cluster-secret-key", now_s=1200
        )
        self.assertIsNone(err)
        self.assertEqual(decoded["session_id"], "hzn-12345")
        self.assertEqual(decoded["vm_name"], "vdi-eng-001")

        # Wrong secret rejected
        bad, err_bad = helios_horizon.verify_horizon_session_token(
            token, "wrong-secret", now_s=1200
        )
        self.assertIsNone(bad)
        self.assertIn("signature", err_bad)

        # Expired token rejected
        exp, err_exp = helios_horizon.verify_horizon_session_token(
            token, "cluster-secret-key", now_s=2000
        )
        self.assertIsNone(exp)
        self.assertIn("expired", err_exp)

    def test_broker_desktop_session_provisions_sidon_linked_clone_and_reconnects(self):
        pools_db = {}
        vms_db = {}
        sessions_db = {}
        cloned_vdisks = []

        def fake_query(cql):
            if "INSERT INTO hydra.horizon_pools" in cql:
                pools_db["eng"] = {
                    "pool_name": "eng",
                    "template_vm": "win11-golden",
                    "pool_type": "instant_clone",
                    "protocol": "rdp",
                    "desktop_port": 3389,
                    "min_desktops": 0,
                    "max_desktops": 2,
                    "vcpu": 4,
                    "memory_mb": 8192,
                    "network_name": "default",
                    "domain_join": "CORP",
                    "entitled_groups": "engineers",
                    "power_policy": "always_on",
                    "idle_timeout_min": 60,
                    "state": "READY",
                }
                return 0, "", ""
            if "SELECT JSON * FROM hydra.horizon_pools WHERE pool_name = 'eng'" in cql:
                if "eng" in pools_db:
                    return 0, json.dumps(pools_db["eng"]) + "\n", ""
                return 0, "", ""
            if "SELECT JSON * FROM hydra.horizon_sessions" in cql:
                return 0, "\n".join(json.dumps(s) for s in sessions_db.values()) + "\n", ""
            if "SELECT JSON * FROM hydra.vms" in cql:
                return 0, "\n".join(json.dumps(v) for v in vms_db.values()) + "\n", ""
            if "INSERT INTO hydra.vms" in cql:
                vm_name = "vdi-eng-%03d" % (len(vms_db) + 1)
                vms_db[vm_name] = {
                    "name": vm_name,
                    "node": "10.10.102.41",
                    "status": "RUNNING",
                    "guest_ips": "172.16.10.25",
                }
                return 0, "", ""
            if "INSERT INTO hydra.horizon_sessions" in cql:
                # Extract session_id from VALUES ('hzn-...',
                sid = cql.split("VALUES ('", 1)[1].split("'", 1)[0]
                sessions_db[sid] = {
                    "session_id": sid,
                    "pool_name": "eng",
                    "vm_name": "vdi-eng-001",
                    "username": "alice",
                    "protocol": "rdp",
                    "host_ip": "10.10.102.41",
                    "guacd_port": 4822,
                    "state": "CONNECTED",
                    "connected_at_ms": 1000000,
                    "last_active_ms": 1000000,
                }
                return 0, "", ""
            if "DELETE FROM hydra.horizon_sessions" in cql:
                sid = cql.split("session_id = '", 1)[1].split("'", 1)[0]
                sessions_db.pop(sid, None)
                return 0, "", ""
            if "DELETE FROM hydra.vms" in cql:
                vname = cql.split("name = '", 1)[1].split("'", 1)[0]
                vms_db.pop(vname, None)
                return 0, "", ""
            return 0, "", ""

        pool, err = helios_horizon.create_pool_record(
            {
                "pool_name": "eng",
                "template_vm": "win11-golden",
                "pool_type": "instant_clone",
                "protocol": "rdp",
                "min_desktops": 0,
                "max_desktops": 2,
                "vcpu": 4,
                "memory_mb": 8192,
                "entitled_groups": "engineers",
            },
            run_query=fake_query,
        )
        self.assertIsNone(err)
        self.assertEqual(pool["pool_name"], "eng")

        # Unentitled user refused
        denied, derr = helios_horizon.broker_desktop_session(
            "eng",
            "mallory",
            user_groups=["guests"],
            run_query=fake_query,
            secret="sec-123",
        )
        self.assertIsNone(denied)
        self.assertIn("not entitled", derr)

        # Entitled user provisions a linked clone via Sidon
        res, berr = helios_horizon.broker_desktop_session(
            "eng",
            "alice",
            user_groups=["engineers"],
            default_host_ip="10.10.102.41",
            sidon_clone_fn=lambda parent, child: cloned_vdisks.append((parent, child)),
            run_query=fake_query,
            secret="sec-123",
            now_ms=1000000,
        )
        self.assertIsNone(berr)
        self.assertFalse(res["reconnected"])
        self.assertEqual(cloned_vdisks, [("win11-golden-disk0", "vdi-eng-001-disk0")])
        self.assertEqual(res["session"]["vm_name"], "vdi-eng-001")

        # Second launch by same user reconnects existing session without cloning a second VM
        res2, berr2 = helios_horizon.broker_desktop_session(
            "eng",
            "alice",
            user_groups=["engineers"],
            default_host_ip="10.10.102.41",
            sidon_clone_fn=lambda parent, child: cloned_vdisks.append((parent, child)),
            run_query=fake_query,
            secret="sec-123",
            now_ms=1005000,
        )
        self.assertIsNone(berr2)
        self.assertTrue(res2["reconnected"])
        self.assertEqual(len(cloned_vdisks), 1)

    def test_sidon_remote_drives_replicator_over_sidon_control_ops(self):
        manifest, source_ids = rauru_replication.build_manifest(
            "snap-vdi-1",
            1024 * 1024 * 1024,
            1024 * 1024,
            [
                {
                    "extent_index": 0,
                    "egroup_id": "eg-100",
                    "egroup_offset": 0,
                    "length": 4096,
                    "vdisk_hash": 12345,
                }
            ],
            {"eg-100": {"state": "sealed", "size": 4096 + 32, "seal_hash": "ab" * 32}},
        )
        calls = []

        def fake_source_call(op, **params):
            calls.append(("source", op, params))
            return {"ok": True, "data_b64": "AAAA", "sha256": "cd" * 32, "length": 4128}

        def fake_target_call(op, **params):
            calls.append(("target", op, params))
            if op == "replicate-offer":
                return {"ok": True, "status": "proceed", "have": {"eg-100": {"state": "absent"}}}
            if op == "replicate-receive":
                return {"ok": True, "state": "complete"}
            if op == "replicate-publish":
                return {"ok": True, "status": "published", "vdisk_id": "r-site-a-snap-vdi-1"}
            raise AssertionError("unexpected op " + op)

        remote = rauru_replication.SidonRemote(
            "site-a", fake_source_call, fake_target_call, source_ids=source_ids
        )
        store = rauru_replication.MemoryStateStore()
        rep = rauru_replication.Replicator(remote, store)
        result = rep.replicate("site-b", "set-1", manifest)
        self.assertEqual(result.state, rauru_replication.VISIBLE)
        self.assertEqual(result.bytes_sent, 4128)
        self.assertEqual([c[1] for c in calls], [
            "replicate-offer",
            "replicate-export",
            "replicate-receive",
            "replicate-publish",
        ])

    def test_r6_vali_balance_forwards_when_not_drs_leader(self):
        import vali
        fake_cand = mock.MagicMock()
        fake_cand.leading.return_value = False
        fake_cand.leader_identity.return_value = b"10.10.102.42"
        sent = []
        with mock.patch.object(vali, "candidacy", return_value=fake_cand), \
             mock.patch.object(vali, "LOCAL_IP", "10.10.102.41"), \
             mock.patch.object(vali, "forward_drs_balance_to_leader", return_value=(True, {"status": "triggered"}, "")):
            vali.trigger_drs_balance({"aggressive": True}, lambda code, body: sent.append((code, body)))
        self.assertEqual(sent, [(200, {"status": "triggered", "forwarded_to": "10.10.102.42"})])

    def test_r19_console_token_verifier_requires_shared_secret_before_consuming_token(self):
        import spectrum_server
        queries = []

        def fake_cql(q):
            queries.append(q)
            if "SELECT JSON host_ip, port, expires_at FROM hydra.console_sessions" in q:
                return 0, json.dumps({"host_ip": "10.10.102.41", "port": 5900, "expires_at": 9999999999}) + "\n", ""
            return 0, "", ""

        with mock.patch.object(spectrum_server, "run_cql_query", side_effect=fake_cql):
            # Missing or wrong secret is rejected before querying or deleting the token
            res_bad = spectrum_server.verify_console_token_request(
                "AUTH|wrong-secret|tok-test-1", expected_secret="top-secret", now=1000
            )
            self.assertEqual(res_bad, "ERROR")
            self.assertEqual(queries, [])

            res_ok = spectrum_server.verify_console_token_request(
                "AUTH|top-secret|tok-test-1", expected_secret="top-secret", now=1000
            )
            self.assertEqual(res_ok, "OK|10.10.102.41|5900")
            self.assertTrue(any("SELECT JSON host_ip, port, expires_at FROM hydra.console_sessions" in q for q in queries))

    def test_spark_daemon_routes_host_horizon_endpoint(self):
        daemon = load_spark_daemon()
        self.assertTrue(hasattr(daemon.SparkDaemonHandler, "handle_host_horizon_get"))
        self.assertTrue(hasattr(daemon.SparkDaemonHandler, "handle_host_horizon_post"))


if __name__ == "__main__":
    unittest.main()
