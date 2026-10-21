import json
import os
import tempfile
import unittest

import cluster_new as cluster


class ClusterWitnessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8")
        def cleanup():
            try:
                self.tmp.close()
            except Exception:
                pass
            if os.path.exists(self.tmp.name):
                try:
                    os.unlink(self.tmp.name)
                except Exception:
                    pass
        self.addCleanup(cleanup)

    def test_get_witness_ip_reads_witness_field(self):
        cdata = {
            "cluster_name": "hci-01",
            "redundancy_factor": 1,
            "hosts": [
                {"node_id": 1, "ip": "10.0.0.1", "hostname": "node1", "role": "hypervisor"},
                {"node_id": 2, "ip": "10.0.0.2", "hostname": "node2", "role": "hypervisor"},
                {"node_id": 3, "ip": "10.0.0.3", "hostname": "node3", "role": "witness", "is_witness": True}
            ]
        }
        json.dump(cdata, self.tmp)
        self.tmp.flush()

        self.assertEqual(cluster.get_witness_ip(self.tmp.name), "10.0.0.3")

    def test_get_witness_ip_returns_none_when_no_witness(self):
        cdata = {
            "cluster_name": "hci-01",
            "redundancy_factor": 1,
            "hosts": [
                {"node_id": 1, "ip": "10.0.0.1", "hostname": "node1"},
                {"node_id": 2, "ip": "10.0.0.2", "hostname": "node2"}
            ]
        }
        json.dump(cdata, self.tmp)
        self.tmp.flush()

        self.assertIsNone(cluster.get_witness_ip(self.tmp.name))

    def test_render_node_block_shows_witness_badge_and_restricts_services(self):
        witness_data = {
            "hostname": "witness-01",
            "ip": "10.0.0.3",
            "is_witness": True,
            "role": "witness",
            "services": {
                "ZooKeeper": {"status": "UP", "pids": [1234]},
                "Spark": {"status": "UP", "pids": [5678]},
                "HydraDB": {"status": "DOWN"},
                "Sidon": {"status": "DOWN"}
            },
            "disabled_services": ["Urbosa"]
        }
        rendered = cluster.render_node_block("10.0.0.3", witness_data, use_color=False)
        self.assertIn("[Witness / Quorum Tie-Breaker]", rendered)
        self.assertIn("ZooKeeper", rendered)
        self.assertIn("Spark", rendered)
        self.assertNotIn("HydraDB", rendered)
        self.assertNotIn("Sidon", rendered)
        self.assertNotIn("DISABLED", rendered)

    def test_render_node_block_normal_node_shows_full_services(self):
        normal_data = {
            "hostname": "hypervisor-01",
            "ip": "10.0.0.1",
            "is_witness": False,
            "role": "hypervisor",
            "services": {
                "ZooKeeper": {"status": "UP", "pids": [1234]},
                "HydraDB": {"status": "UP", "pids": [2345]},
                "Sidon": {"status": "UP", "pids": [3456]}
            }
        }
        rendered = cluster.render_node_block("10.0.0.1", normal_data, use_color=False)
        self.assertNotIn("[Witness / Quorum Tie-Breaker]", rendered)
        self.assertIn("ZooKeeper", rendered)
        self.assertIn("HydraDB", rendered)
        self.assertIn("Sidon", rendered)


if __name__ == "__main__":
    unittest.main()
