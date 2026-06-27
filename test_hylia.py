#!/usr/bin/env python3
import os
import json
import zipfile
import hashlib
import shutil
import unittest
import hylia

class TestHyliaLCM(unittest.TestCase):
    def setUp(self):
        self.test_dir = "/tmp/yggdrasil_test_env"
        self.extract_dir = "/tmp/yggdrasil_test_extract"
        os.makedirs(self.test_dir, exist_ok=True)
        
        # 1. Create a dummy component
        self.comp_name = "mock_service.py"
        self.comp_path = os.path.join(self.test_dir, self.comp_name)
        self.comp_content = '__build__ = "2.0.1-mock45"\nprint("Mock Service running")\n'
        with open(self.comp_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(self.comp_content)
            
        # 2. Compute SHA-256 hash
        sha = hashlib.sha256()
        sha.update(self.comp_content.encode("utf-8"))
        self.expected_hash = sha.hexdigest()
        
        # 3. Create dummy changelog
        self.changelog_content = "# Version 2.0.1-mock45\n- Added cool mock feature\n- Resolved mock bug"
        self.changelog_name = "changelog.md"
        self.changelog_path = os.path.join(self.test_dir, self.changelog_name)
        with open(self.changelog_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(self.changelog_content)
            
        # 4. Create manifest
        self.manifest_data = {
            "version": "2.0.1",
            "build": "mock45",
            "changelog": self.changelog_name,
            "components": {
                "mock_service": {
                    "file": self.comp_name,
                    "sha256": self.expected_hash,
                    "target_path": f"/usr/local/bin/{self.comp_name}"
                }
            }
        }
        self.manifest_path = os.path.join(self.test_dir, "manifest.json")
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(self.manifest_data, f)
            
        # 5. Pack into zip
        self.zip_path = os.path.join(self.test_dir, "mock_update.zip")
        with zipfile.ZipFile(self.zip_path, "w") as z:
            z.write(self.comp_path, self.comp_name)
            z.write(self.changelog_path, self.changelog_name)
            z.write(self.manifest_path, "manifest.json")

    def tearDown(self):
        for path in [self.test_dir, self.extract_dir]:
            if os.path.exists(path):
                try:
                    shutil.rmtree(path)
                except Exception:
                    pass

    def test_valid_package_verification(self):
        # Verify valid package extracts and validates correctly
        manifest, changelog = hylia.validate_and_extract_zip(self.zip_path, self.extract_dir)
        self.assertEqual(manifest["version"], "2.0.1")
        self.assertEqual(manifest["build"], "mock45")
        self.assertEqual(changelog.strip(), self.changelog_content.strip())
        
        # Verify build parsing function
        extracted_comp_path = os.path.join(self.extract_dir, self.comp_name)
        build_num = hylia.get_service_build_number(extracted_comp_path)
        self.assertEqual(build_num, "2.0.1-mock45")

    def test_invalid_hash_verification(self):
        # Modify manifest with a bad hash to simulate corrupt package
        corrupt_manifest = self.manifest_data.copy()
        corrupt_manifest["components"]["mock_service"]["sha256"] = "badhash123"
        
        corrupt_dir = os.path.join(self.test_dir, "corrupt")
        os.makedirs(corrupt_dir, exist_ok=True)
        
        corrupt_manifest_path = os.path.join(corrupt_dir, "manifest.json")
        with open(corrupt_manifest_path, "w", encoding="utf-8") as f:
            json.dump(corrupt_manifest, f)
            
        bad_zip_path = os.path.join(self.test_dir, "corrupt_update.zip")
        with zipfile.ZipFile(bad_zip_path, "w") as z:
            z.write(self.comp_path, self.comp_name)
            z.write(self.changelog_path, self.changelog_name)
            z.write(corrupt_manifest_path, "manifest.json")
            
        with self.assertRaises(Exception) as ctx:
            hylia.validate_and_extract_zip(bad_zip_path, self.extract_dir)
        self.assertIn("Checksum verification failed", str(ctx.exception))

if __name__ == "__main__":
    unittest.main()
