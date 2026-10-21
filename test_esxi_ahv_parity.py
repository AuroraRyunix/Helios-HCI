import importlib.util
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def _load_module(filename, mod_name):
    spec = importlib.util.spec_from_file_location(mod_name, os.path.join(HERE, filename))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class UrbosaVniClaimsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = _load_module("helios_schema.py", "helios_schema_vni_test")
        with open(os.path.join(HERE, "daruk.py"), "r", encoding="utf-8") as f:
            cls.daruk_src = f.read()

    def test_vni_claim_statement_uses_if_not_exists(self):
        stmt = self.schema.claim_vni_statement(5001, "seg-1", "overlay-a", 123456)
        self.assertIn("INSERT INTO hydra.urbosa_vni_claims", stmt)
        self.assertIn("IF NOT EXISTS", stmt)
        self.assertIn("5001", stmt)
        self.assertIn("'seg-1'", stmt)

    def test_daruk_exposes_vni_lwt_endpoints(self):
        self.assertIn('"/v1/network/claim-vni"', self.daruk_src)
        self.assertIn('"/v1/network/release-vni"', self.daruk_src)
        self.assertIn('"/v1/network/reclaim-vni"', self.daruk_src)


class ValiParityFeaturesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Extract pure functions from vali.py without running daemon loops
        with open(os.path.join(HERE, "vali.py"), "r", encoding="utf-8") as f:
            src = f.read()
        ns = {"__name__": "vali_test_ns"}
        # Execute up to start_api_server or full module since main is guarded by __name__ == "__main__"
        exec(compile(src, os.path.join(HERE, "vali.py"), "exec"), ns)
        cls.ns = ns

    def test_build_cidata_iso_bytes_produces_valid_iso9660_rock_ridge(self):
        iso_bytes = self.ns["build_cidata_iso_bytes"](
            "vm-test-01",
            "#cloud-init\nusers:\n  - name: admin\n",
            "version: 2\nethernets: {}\n",
        )
        self.assertEqual(len(iso_bytes) % 2048, 0)
        # PVD at sector 16
        pvd = iso_bytes[16 * 2048 : 17 * 2048]
        self.assertEqual(pvd[0:6], b"\x01CD001")
        self.assertEqual(pvd[40:72].strip(), b"cidata")
        self.assertIn(b"RRIP_1991A", iso_bytes)
        self.assertIn(b"meta-data", iso_bytes)
        self.assertIn(b"user-data", iso_bytes)
        self.assertIn(b"network-config", iso_bytes)
        self.assertIn(b"instance-id: vm-test-01", iso_bytes)
        self.assertIn(b"#cloud-init\nusers:\n  - name: admin\n", iso_bytes)

    def test_check_vm_host_affinity_enforces_pinning_and_anti_affinity(self):
        check = self.ns["check_vm_host_affinity"]
        peers = [
            {"name": "web-1", "state": "Running", "host_ip": "10.10.102.41", "anti_affinity_group": "ha-web", "affinity_group": "tier-app"},
            {"name": "web-2", "state": "Stopped", "host_ip": "10.10.102.42", "anti_affinity_group": "ha-web"},
        ]
        # Host pinning
        vm_pinned = {"name": "db-1", "pinned_hosts": "10.10.102.42, 10.10.102.43"}
        ok1, _, reason1 = check(vm_pinned, "10.10.102.41", all_vms=peers)
        self.assertFalse(ok1)
        self.assertIn("pinned", reason1)
        ok2, _, _ = check(vm_pinned, "10.10.102.42", all_vms=peers)
        self.assertTrue(ok2)

        # Anti-affinity
        vm_anti = {"name": "web-3", "anti_affinity_group": "ha-web"}
        ok3, _, reason3 = check(vm_anti, "10.10.102.41", all_vms=peers)
        self.assertFalse(ok3)
        self.assertIn("anti-affinity", reason3)
        ok4, _, _ = check(vm_anti, "10.10.102.42", all_vms=peers)
        self.assertTrue(ok4)

    def test_generate_vm_xml_supports_vtpm_secure_boot_and_cidata(self):
        gen = self.ns["generate_vm_xml"]
        xml = gen(
            "sec-vm", 2048, 2, "uefi", [], "hd",
            [{"path": "/var/run/hci/sidon/nbd/sec-vm-disk0.sock", "bus": "virtio", "vdisk_id": "sec-vm-disk0"}],
            vtpm=True,
            secure_boot=True,
            cloud_init_iso="/var/lib/hci/aether/nvram/sec-vm_cidata.iso",
        )
        self.assertIn("OVMF_CODE.secboot.fd", xml)
        self.assertIn("OVMF_VARS.secboot.fd", xml)
        self.assertIn("secure='yes'", xml)
        self.assertIn("<tpm model='tpm-crb'>", xml)
        self.assertIn("version='2.0'", xml)
        self.assertIn("/var/lib/hci/aether/nvram/sec-vm_cidata.iso", xml)


if __name__ == "__main__":
    unittest.main()
