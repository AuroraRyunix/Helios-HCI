#!/usr/bin/env python3
"""Unit tests for Scale-Out Urbosa: FRRouting BGP L2VPN EVPN control plane and ARP suppression."""

import importlib.util
import json
import os
import tempfile
import unittest

import helios_schema
import urbosa

HERE = os.path.dirname(os.path.abspath(__file__))


def load_spark_daemon():
    spec = importlib.util.spec_from_file_location(
        "spark_daemon_evpn_test", os.path.join(HERE, "spark_daemon_decoded.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class UrbosaEvpnTests(unittest.TestCase):
    def test_schema_includes_urbosa_bgp_evpn_migration(self):
        migration_ids = [m["id"] for m in helios_schema.MIGRATIONS]
        self.assertIn("0057-urbosa-bgp-evpn", migration_ids)
        mig = next(m for m in helios_schema.MIGRATIONS if m["id"] == "0057-urbosa-bgp-evpn")
        joined = " ".join(mig["statements"])
        self.assertIn("hydra.urbosa_bgp_config", joined)
        self.assertIn("hydra.urbosa_bgp_peers", joined)

    def test_render_frr_config_ibgp_mesh_and_vnis(self):
        bgp_cfg = {
            "asn": 65001,
            "router_id": "10.10.102.41",
            "evpn_enabled": True,
            "arp_suppression": True,
            "bfd_enabled": True,
            "route_reflectors": "",
        }
        bgp_peers = [
            {
                "peer_ip": "10.10.100.1",
                "remote_asn": 65000,
                "description": "spine-tor-01",
                "bfd_enabled": True,
                "route_reflector_client": False,
            }
        ]
        node_ips = ["10.10.102.41", "10.10.102.42", "10.10.102.43"]
        segments = [
            {"vni": 10100, "name": "prod-web"},
            {"vni": 10200, "name": "prod-db"},
        ]
        conf = urbosa.render_frr_config(
            bgp_cfg, bgp_peers, node_ips, "10.10.102.41", segments, hostname="hci-01"
        )
        self.assertIn("router bgp 65001", conf)
        self.assertIn("bgp router-id 10.10.102.41", conf)
        self.assertIn("neighbor 10.10.102.42 remote-as 65001", conf)
        self.assertIn("neighbor 10.10.102.43 remote-as 65001", conf)
        self.assertIn("neighbor 10.10.100.1 remote-as 65000", conf)
        self.assertIn("address-family l2vpn evpn", conf)
        self.assertIn("advertise-all-vni", conf)
        self.assertIn("advertise-svi-ip", conf)
        self.assertIn("vni 10100", conf)
        self.assertIn("route-target import 65001:10100", conf)
        self.assertIn("vni 10200", conf)

    def test_render_frr_config_route_reflector_mode(self):
        bgp_cfg = {
            "asn": 65010,
            "router_id": "10.10.102.41",
            "evpn_enabled": True,
            "route_reflectors": "10.10.102.41",
            "bfd_enabled": False,
        }
        node_ips = ["10.10.102.41", "10.10.102.42", "10.10.102.43"]
        # When rendered on RR node 10.10.102.41, peers 42 and 43 get route-reflector-client
        conf_rr = urbosa.render_frr_config(
            bgp_cfg, [], node_ips, "10.10.102.41", [], hostname="hci-01"
        )
        self.assertIn("bgp cluster-id 10.10.102.41", conf_rr)
        self.assertIn("neighbor 10.10.102.42 route-reflector-client", conf_rr)
        self.assertIn("neighbor 10.10.102.43 route-reflector-client", conf_rr)

        # When rendered on client node 10.10.102.42, it only peers with RR 10.10.102.41
        conf_client = urbosa.render_frr_config(
            bgp_cfg, [], node_ips, "10.10.102.42", [], hostname="hci-02"
        )
        self.assertIn("neighbor 10.10.102.41 remote-as 65010", conf_client)
        self.assertNotIn("neighbor 10.10.102.43", conf_client)

    def test_build_vxlan_segment_commands_evpn_and_arp_suppression(self):
        bgp_cfg = {
            "evpn_enabled": True,
            "arp_suppression": True,
            "disable_headend_flood": True,
        }
        cmds = urbosa.build_vxlan_segment_commands(
            vni=5001,
            phys_if="ens192",
            mtu_size=1450,
            node_ips=["10.10.102.41", "10.10.102.42"],
            local_ip="10.10.102.41",
            bgp_cfg=bgp_cfg,
            br_exists=False,
            vx_exists=False,
        )
        joined = "\n".join(cmds)
        self.assertIn("nolearning", joined)
        self.assertIn("bridge link set dev vxlan-5001 learning off", joined)
        self.assertIn("bridge link set dev vxlan-5001 neigh_suppress on", joined)
        self.assertIn("ip link set dev br-ov-5001 type bridge neigh_suppress on", joined)
        # Head-end flooding is omitted when disable_headend_flood is True under EVPN
        self.assertNotIn("bridge fdb append 00:00:00:00:00:00", joined)

    def test_build_vxlan_segment_commands_static_flood_when_evpn_disabled(self):
        cmds = urbosa.build_vxlan_segment_commands(
            vni=5001,
            phys_if="ens192",
            mtu_size=1450,
            node_ips=["10.10.102.41", "10.10.102.42"],
            local_ip="10.10.102.41",
            bgp_cfg={"evpn_enabled": False, "arp_suppression": False},
            br_exists=True,
            vx_exists=True,
        )
        joined = "\n".join(cmds)
        self.assertIn("ip link set dev vxlan-5001 type vxlan learning", joined)
        self.assertIn("bridge link set dev vxlan-5001 neigh_suppress off", joined)
        self.assertIn("bridge fdb append 00:00:00:00:00:00 dev vxlan-5001 dst 10.10.102.42", joined)

    def test_reconcile_frr_evpn_writes_config_and_reloads_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            frr_conf = os.path.join(tmpdir, "frr.conf")
            daemons_conf = os.path.join(tmpdir, "daemons")
            with open(daemons_conf, "w", encoding="utf-8") as df:
                df.write("zebra=yes\nbgpd=no\nbfdd=no\n")

            ran = []

            def fake_run(cmd):
                ran.append(cmd)
                return 0, "", ""

            bgp_cfg = {"asn": 65000, "evpn_enabled": True, "arp_suppression": True, "bfd_enabled": True}
            res1 = urbosa.reconcile_frr_evpn(
                bgp_cfg, [], ["10.10.102.41", "10.10.102.42"], "10.10.102.41", [{"vni": 100}],
                frr_conf_path=frr_conf, daemons_path=daemons_conf, run=fake_run
            )
            self.assertTrue(res1["applied"])
            self.assertEqual(len(ran), 1)
            with open(daemons_conf, "r", encoding="utf-8") as df:
                self.assertIn("bgpd=yes", df.read())

            # Second pass with identical state is a no-op
            res2 = urbosa.reconcile_frr_evpn(
                bgp_cfg, [], ["10.10.102.41", "10.10.102.42"], "10.10.102.41", [{"vni": 100}],
                frr_conf_path=frr_conf, daemons_path=daemons_conf, run=fake_run
            )
            self.assertFalse(res2["applied"])
            self.assertEqual(len(ran), 1)

    def test_collect_evpn_status_parses_vtysh_and_bridge_json(self):
        def fake_run(cmd):
            if "show version" in cmd:
                return 0, "FRRouting 9.1", ""
            if "show bgp l2vpn evpn summary json" in cmd:
                return 0, json.dumps({
                    "l2vpnEvpn": {
                        "routerId": "10.10.102.41",
                        "as": 65000,
                        "peers": {
                            "10.10.102.42": {
                                "remoteAs": 65000,
                                "state": "Established",
                                "peerUptime": "04:12:33",
                                "pfxRcd": 18,
                                "pfxSnt": 19,
                            }
                        }
                    }
                }), ""
            if "show evpn vni json" in cmd:
                return 0, json.dumps({
                    "10100": {
                        "vni": 10100,
                        "type": "L2",
                        "vxlanIf": "vxlan-10100",
                        "numMacs": 6,
                        "numArpNd": 6,
                        "numRemoteVteps": 2,
                    }
                }), ""
            if "bridge -d -json link show" in cmd:
                return 0, json.dumps([
                    {"ifname": "vxlan-10100", "neigh_suppress": True},
                    {"ifname": "veth-ov-10100", "neigh_suppress": False},
                ]), ""
            return 1, "", ""

        status = urbosa.collect_evpn_status(run=fake_run)
        self.assertTrue(status["frr_installed"])
        self.assertTrue(status["bgpd_running"])
        self.assertEqual(status["router_id"], "10.10.102.41")
        self.assertEqual(status["local_as"], 65000)
        self.assertEqual(len(status["peers"]), 1)
        self.assertEqual(status["peers"][0]["state"], "Established")
        self.assertEqual(status["vnis"][0]["num_arps"], 6)
        self.assertEqual(status["arp_suppression_interfaces"], ["vxlan-10100"])

    def test_spark_daemon_routes_host_bgp_endpoint(self):
        daemon = load_spark_daemon()
        self.assertTrue(hasattr(daemon.SparkDaemonHandler, "handle_host_bgp_get"))
        self.assertTrue(hasattr(daemon.SparkDaemonHandler, "handle_host_bgp_post"))


if __name__ == "__main__":
    unittest.main()
