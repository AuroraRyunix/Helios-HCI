import os
import json
import hashlib
import zipfile
import shutil

VERSION = "1.2.3-stable"
ZIP_NAME = "upgrade_1.2.3-stable.zip"
BUILD_DIR = "upgrade_build"

components_map = {
    "spark": {"src": "spark.py", "target": "/usr/local/bin/spark"},
    "cluster": {"src": "cluster_new.py", "target": "/usr/local/bin/cluster"},
    "spark-daemon": {"src": "spark_daemon_decoded.py", "target": "/usr/local/bin/spark-daemon"},
    "bifrost": {"src": "bifrost.py", "target": "/usr/local/bin/bifrost"},
    "valcli": {"src": "valcli.py", "target": "/usr/local/bin/valcli"},
    "mcli": {"src": "mcli", "target": "/usr/local/bin/mcli"},
    "mcli-runner": {"src": "mcli-runner", "target": "/usr/local/bin/mcli-runner"},
    "dagur": {"src": "dagur.py", "target": "/usr/local/bin/dagur"},
    "mimir": {"src": "mimir.py", "target": "/usr/local/bin/mimir"},
    "vali": {"src": "vali.py", "target": "/usr/local/bin/vali"},
    "catalyst": {"src": "catalyst.py", "target": "/usr/local/bin/catalyst"},
    "catcli": {"src": "catcli", "target": "/usr/local/bin/catcli"},
    "gatoway": {"src": "gatoway.py", "target": "/usr/local/bin/gatoway"},
    "urbosa": {"src": "urbosa.py", "target": "/usr/local/bin/urbosa"},
    "logos": {"src": "logos.py", "target": "/usr/local/bin/logos"},
    "mipha": {"src": "mipha.py", "target": "/usr/local/bin/mipha"},
    "urbosa-bootstrap": {"src": "urbosa_bootstrap.py", "target": "/usr/local/bin/urbosa-bootstrap"},
    "daruk": {"src": "daruk.py", "target": "/usr/local/bin/daruk.py"},
    "hylia": {"src": "hylia.py", "target": "/usr/local/bin/hylia"},
    "spectrum": {"src": "spectrum_server.py", "target": "/usr/local/bin/spectrum_server"},
    "Dockerfile": {"src": "Dockerfile", "target": "/usr/local/bin/Dockerfile"}
}

changelog_content = """# Helios-HCI Update Package Changelog History

## [1.2.3-stable]
### [provision]
- Reverted automated Secure Boot reboot logic to fail cleanly with clear instructions.
- Implemented LVM system.devices cleanup to resolve 'Device or resource busy' error during cluster recreate.
- Deployed the online check-updates script (/usr/local/bin/check-updates) directly in the provisioner.
- Added hylia daemon coordination to cluster create/start/stop/destroy commands.
- Updated mcli-runner database ring status check to run nodetool inside container.

## [1.2.2]
### [lcm]
- Dummy release for testing LCM capabilities.

## [1.2.1-b4085]
### [hylia]
- Fixed exit status 127 caused by Windows CRLF carriage returns in shebang lines during replication.
- Updated path configurations from yggdrasil_update to helios_update.
- Added dynamic imports using SourceFileLoader to load extensionless hylia on host nodes.

## [1.2.0-b4084]
### [spectrum]
- Simplified the Cluster Component Inventory layout to a single version column.
- Added base version fallback for components without explicit version identifiers.
- Added live on-demand inventory check button.

## [1.2.0-b4083]
### [hylia]
- Added support for selective component rolling upgrades.
- Enforced hylia upgrade dependency checks during rolling updates.
- Added direct CQL container cqlsh query fallback.

### [spectrum]
- Added cache buster to bypass Cloudflare CDN caching for package downloads.
- Integrated checkboxes in LCM preview table to select/deselect individual components for update.
- Added interactive component-level differential changelog filtering in the UI.

## [1.2.0-b4082]
### [spectrum]
- Exposed API endpoints for upgrade check and download.
- Resolved noVNC and WebGL console loading dependencies.
"""

def main():
    if os.path.exists(BUILD_DIR):
        shutil.rmtree(BUILD_DIR)
    os.makedirs(BUILD_DIR)
    
    components_manifest = {}
    
    for comp_name, info in components_map.items():
        src_path = info["src"]
        dest_filename = comp_name
        dest_path = os.path.join(BUILD_DIR, dest_filename)
        
        # Read original file
        with open(src_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
            
        # Parse version string directly from the file content
        comp_version = None
        lines = content.splitlines()
        for line in lines:
            if line.strip().startswith("__build__") and "=" in line:
                parts = line.split("=", 1)
                if len(parts) >= 2:
                    comp_version = parts[1].strip().strip('"').strip("'")
                    break
                    
        if not comp_version:
            comp_version = VERSION
            if comp_name != "Dockerfile":
                # Ensure the __build__ tag is present in the output file
                if lines and lines[0].startswith("#!"):
                    lines.insert(1, f'__build__ = "{comp_version}"')
                else:
                    lines.insert(0, f'__build__ = "{comp_version}"')
                content = "\n".join(lines) + "\n"
            
        modified_content = content
        
        # Write modified file
        with open(dest_path, "w", encoding="utf-8", newline="\n") as f_out:
            f_out.write(modified_content)
            
        # Calculate SHA-256
        sha256 = hashlib.sha256()
        with open(dest_path, "rb") as f_bin:
            while chunk := f_bin.read(8192):
                sha256.update(chunk)
        file_hash = sha256.hexdigest()
        
        components_manifest[comp_name] = {
            "file": dest_filename,
            "sha256": file_hash,
            "target_path": info["target"],
            "version": comp_version
        }
        
    # Write changelog
    changelog_filename = "changelog.md"
    with open(os.path.join(BUILD_DIR, changelog_filename), "w", encoding="utf-8") as f_ch:
        f_ch.write(changelog_content)
        
    # Write manifest.json
    manifest = {
        "build": VERSION,
        "changelog": changelog_filename,
        "components": components_manifest,
        "min_hylia_version": "1.2.1-b4085"
    }
    with open(os.path.join(BUILD_DIR, "manifest.json"), "w", encoding="utf-8") as f_man:
        json.dump(manifest, f_man, indent=2)
        
    # Package into ZIP
    if os.path.exists(ZIP_NAME):
        os.remove(ZIP_NAME)
        
    with zipfile.ZipFile(ZIP_NAME, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
        for file in os.listdir(BUILD_DIR):
            file_path = os.path.join(BUILD_DIR, file)
            zip_ref.write(file_path, arcname=file)
            
    shutil.rmtree(BUILD_DIR)
    print(f"Successfully created {ZIP_NAME} with version {VERSION}!")

if __name__ == "__main__":
    main()
