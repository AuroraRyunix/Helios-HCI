import io
import os
import json
import stat
import time
import hashlib
import zipfile
import shutil

import helios_sig

VERSION = "1.2.3-stable"
# Derived from VERSION so the package name can never drift from the build it carries.
ZIP_NAME = f"upgrade_{VERSION}.zip"
BUILD_DIR = "upgrade_build"

# The signed document the update server serves at /api/v1/releases/latest. It is
# produced here, on the machine that holds the signing key, because that is the only
# place the zip's digest can be asserted by someone rather than merely observed: a
# digest the release server computes for a file the release server hosts proves
# nothing about who wrote the file.
RELEASE_DOC_NAME = f"upgrade_{VERSION}.release.json"
RELEASE_DOWNLOAD_URL = os.environ.get("HELIOS_RELEASE_DOWNLOAD_URL", "").strip() or \
    f"https://updates-helios.zerotwo.cloud/downloads/{ZIP_NAME}"

# Components that are built on the node rather than copied to it.
#
# The package carries *sources*, and hylia compiles them. The alternative -- shipping
# binaries -- was rejected because it moves what the signature means: signing a tarball of
# Rust you can read is a claim about the code, while signing an ELF is a claim about
# whoever's machine produced it. This way the release key vouches for something a reader
# can audit.
#
# The cost is honest and worth stating: every node needs a toolchain, a build takes
# minutes rather than milliseconds, and a build that fails on one node fails that node's
# upgrade. hylia keeps the running binary until the new one exists, so the failure is a
# node that did not update rather than a node without storage.
#
# Only Rust crates. The console's container image is a different problem -- it pulls base
# images from a public registry at build time -- and is deliberately not solved here.
SOURCE_COMPONENTS = {
    "sidon-src": {
        "dir": "sidon",
        "binary": "sidon",
        "target": "/usr/local/bin/sidon",
        "unit": "sidon",
    },
    "ganon-src": {
        "dir": "ganon",
        "binary": "ganon",
        "target": "/usr/local/bin/ganon",
        # No unit: ganon is a harness an operator runs, not a service.
        "unit": None,
    },
    "agahnim-src": {
        "dir": "agahnim",
        "binary": "agahnim",
        "target": "/usr/local/bin/agahnim",
        "unit": "agahnim",
    },
}

# What goes into a crate tarball. Everything else -- target/, .git, editor droppings --
# is rebuilt or irrelevant, and including it would make the digest depend on whether the
# packager had run a build.
SOURCE_INCLUDE = ("Cargo.toml", "Cargo.lock", "src")


def crate_tarball(crate_dir, dest):
    """A byte-identical tarball for identical sources.

    Reproducibility is the whole point of shipping sources: the release document asserts
    a sha256, and a digest that changes because tar recorded a different mtime or walked
    the directory in a different order is a digest nobody can check. So every varying
    field is pinned -- mtime, uid/gid, owner names, and the order of entries -- and the
    gzip header gets no timestamp of its own.
    """
    import gzip
    import tarfile

    members = []
    for name in SOURCE_INCLUDE:
        path = os.path.join(crate_dir, name)
        if not os.path.exists(path):
            continue
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                dirs.sort()
                for f in sorted(files):
                    if f.endswith(".rs"):
                        full = os.path.join(root, f)
                        members.append((os.path.relpath(full, crate_dir).replace("\\", "/"), full))
        else:
            members.append((name, path))
    members.sort()

    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as tar:
        for arcname, full in members:
            with open(full, "rb") as handle:
                # LF, always. A packager on Windows would otherwise ship CRLF into a
                # Rust file and change the digest without changing the code.
                data = handle.read().replace(b"\r\n", b"\n")
            info = tarfile.TarInfo(arcname)
            info.size = len(data)
            info.mtime = 0
            info.mode = 0o644
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.type = tarfile.REGTYPE
            tar.addfile(info, io.BytesIO(data))

    with open(dest, "wb") as out:
        # mtime=0 and no embedded filename. Both default to something that varies: the
        # timestamp to "now", and the name to `fileobj.name` -- so without `filename=""`
        # the digest depends on the path the packager happened to write to, which is the
        # sort of reproducibility bug that passes every check that builds to the same
        # place twice.
        with gzip.GzipFile(filename="", fileobj=out, mode="wb", mtime=0) as gz:
            gz.write(raw.getvalue())
    return [name for name, _ in members]


components_map = {
    "spark": {"src": "spark.py", "target": "/usr/local/bin/spark"},
    "impa": {"src": "impa.py", "target": "/usr/local/bin/impa"},
    "saga": {"src": "saga.py", "target": "/usr/local/bin/saga"},
    "helios-schema": {"src": "helios_schema.py", "target": "/usr/local/bin/helios_schema.py"},
    "cluster": {"src": "cluster_new.py", "target": "/usr/local/bin/cluster"},
    "spark-daemon": {"src": "spark_daemon_decoded.py", "target": "/usr/local/bin/spark-daemon"},
    "bifrost": {"src": "bifrost.py", "target": "/usr/local/bin/bifrost"},
    "valcli": {"src": "valcli.py", "target": "/usr/local/bin/valcli"},
    "mcli": {"src": "mcli", "target": "/usr/local/bin/mcli"},
    "mcli-runner": {"src": "mcli-runner", "target": "/usr/local/bin/mcli-runner"},
    "nodetool": {"src": "nodetool", "target": "/usr/local/bin/nodetool"},
    "allssh": {"src": "allssh", "target": "/usr/local/bin/allssh"},
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
    # daruk keeps its .py suffix on purpose: cluster_new.py copies /usr/local/bin/daruk.py
    # into the hydra-db volume and the unit runs `python3 /var/lib/scylla/daruk.py`.
    "daruk": {"src": "daruk.py", "target": "/usr/local/bin/daruk.py"},
    "hylia": {"src": "hylia.py", "target": "/usr/local/bin/hylia"},
    # check_updates is installed under its hyphenated name (provision.py deploys it as
    # /usr/local/bin/check-updates and the scheduler runs `python3 /usr/local/bin/check-updates`).
    "check-updates": {"src": "check_updates.py", "target": "/usr/local/bin/check-updates"},
    "spectrum": {"src": "spectrum_server.py", "target": "/usr/local/bin/spectrum_server"},
    # lanayru is imported as a module by spectrum_server, so it must keep its .py suffix.
    "lanayru": {"src": "lanayru.py", "target": "/usr/local/bin/lanayru.py"},
    "helios-zk": {"src": "helios_zk.py", "target": "/usr/local/bin/helios_zk.py"},
    # Imported by check-updates (and by hylia when it verifies a package manifest), so
    # it keeps its .py suffix like the other importable modules here.
    "helios-sig": {"src": "helios_sig.py", "target": "/usr/local/bin/helios_sig.py"},
    "helios-sidon": {"src": "helios_sidon.py", "target": "/usr/local/bin/helios_sidon.py"},
    "Dockerfile": {"src": "Dockerfile", "target": "/usr/local/bin/Dockerfile"}
}

changelog_content = """# Helios-HCI Update Package Changelog History

## [1.2.3-stable]
### [lcm]
- Signed the update chain: manifest.json now ships with a detached Ed25519 signature
  (manifest.sig) and the release document is signed as a whole, both verified against
  a public key pinned on each node at provision time.
- check-updates now takes the download URL and package digest from the signed release
  payload only, and refuses a release it cannot verify.

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

def source_mode(src_path, content):
    """Permissions to give the packaged copy.

    The build copy is written from scratch, which would otherwise hand every
    component the default 0644 and silently drop the exec bit from mcli,
    mcli-runner, catcli and friends. Filesystems without POSIX permissions
    (Windows build hosts) report no exec bit at all, so anything carrying a
    shebang is marked executable regardless of what the source claims.
    """
    try:
        mode = stat.S_IMODE(os.stat(src_path).st_mode)
    except OSError:
        mode = 0o644
    # Windows reports 0o666; never ship anything group/world writable into /usr/local/bin.
    mode &= ~0o022
    if content.startswith("#!"):
        mode |= 0o111
    return mode

def resolve_signing_key():
    """Locate the release signing key, or refuse to build.

    Resolved before any file is copied: an unsigned package is refused by every node
    that has a key pinned, so discovering the missing key after the zip exists just
    produces an artefact nobody can install.

    Returns None only when the operator has explicitly opted into an unsigned build,
    which is a real need while a release pipeline is being migrated but must never be
    what happens by default.
    """
    key_path = helios_sig.default_private_key_path()
    if os.path.exists(key_path):
        return key_path
    if helios_sig.unsigned_updates_permitted():
        print("=" * 78)
        print(f"WARNING: no signing key at {key_path}, and {helios_sig.UNSIGNED_OVERRIDE_ENV} is set.")
        print("Building an UNSIGNED package. Every node that pins a release key will refuse it.")
        print("=" * 78)
        return None
    raise SystemExit(
        f"No release signing key at {key_path}, so this package cannot be signed and "
        f"no node will install it.\nGenerate one with:\n{helios_sig.keygen_instructions(key_path)}\n"
        f"Point {helios_sig.PRIVATE_KEY_ENV} at an existing key, or set "
        f"{helios_sig.UNSIGNED_OVERRIDE_ENV}={helios_sig.UNSIGNED_OVERRIDE_VALUE} to build an "
        "unsigned package deliberately."
    )


def write_release_document(signing_key, components_manifest, manifest_bytes):
    """Emit the document the update server serves at /api/v1/releases/latest.

    Everything a node acts on -- the download URL, the digest of what it points at,
    the version it claims to be -- lives inside the signed half. check-updates reads
    those fields from there and ignores whatever surrounds them in the response, so
    the update host can no longer name its own package and vouch for it.
    """
    if not RELEASE_DOWNLOAD_URL.lower().startswith("https://"):
        raise SystemExit(
            f"HELIOS_RELEASE_DOWNLOAD_URL must be an https URL (got {RELEASE_DOWNLOAD_URL!r}); "
            "check-updates rejects anything else, and signing a URL nobody will fetch just "
            "moves the failure later."
        )

    sha256 = hashlib.sha256()
    with open(ZIP_NAME, "rb") as f_zip:
        while chunk := f_zip.read(8192):
            sha256.update(chunk)

    payload = {
        "latest_version": VERSION,
        "release_date": os.environ.get("HELIOS_RELEASE_DATE", "").strip() or time.strftime("%Y-%m-%d"),
        "download_url": RELEASE_DOWNLOAD_URL,
        "sha256": sha256.hexdigest(),
        "size": os.path.getsize(ZIP_NAME),
        "changelog": changelog_content,
        # check-updates compares this component -> version map against the per-node
        # inventory, so it decides whether an upgrade is offered and has to be signed.
        "components": {name: info["version"] for name, info in components_manifest.items()},
        # Ties this document to the manifest inside the package: both are signed, and
        # an auditor can confirm they describe the same build without unpacking anything.
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    }

    if signing_key:
        document = helios_sig.build_signed_document(payload, signing_key)
    else:
        # The flat, unsigned shape the release endpoint used to return. Emitted so an
        # unsigned build is still publishable, and refused by any node that has not had
        # the escape hatch set on purpose.
        document = payload

    with open(RELEASE_DOC_NAME, "w", encoding="utf-8", newline="\n") as f_rel:
        json.dump(document, f_rel, indent=2)
        f_rel.write("\n")
    return payload["sha256"]


def main():
    if f"## [{VERSION}]" not in changelog_content:
        raise SystemExit(f"Changelog has no '## [{VERSION}]' section; update changelog_content to match VERSION before building.")

    signing_key = resolve_signing_key()

    if os.path.exists(BUILD_DIR):
        shutil.rmtree(BUILD_DIR)
    os.makedirs(BUILD_DIR)

    components_manifest = {}
    file_modes = {}

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
        
        # Write modified file, preserving the source permissions
        with open(dest_path, "w", encoding="utf-8", newline="\n") as f_out:
            f_out.write(modified_content)
        file_modes[dest_filename] = source_mode(src_path, modified_content)
        try:
            os.chmod(dest_path, file_modes[dest_filename])
        except OSError:
            pass

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
        
    # Source components: one deterministic tarball each, digested like any other member.
    for comp_name, info in SOURCE_COMPONENTS.items():
        crate_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), info["dir"])
        if not os.path.isdir(crate_dir):
            raise SystemExit(f"{comp_name}: {crate_dir} is not a directory")
        dest_filename = f"{comp_name}.tar.gz"
        dest_path = os.path.join(BUILD_DIR, dest_filename)
        files = crate_tarball(crate_dir, dest_path)
        if not files:
            raise SystemExit(f"{comp_name}: no sources found under {crate_dir}")

        sha256 = hashlib.sha256()
        with open(dest_path, "rb") as f_bin:
            while chunk := f_bin.read(8192):
                sha256.update(chunk)

        file_modes[dest_filename] = 0o644
        components_manifest[comp_name] = {
            "file": dest_filename,
            "sha256": sha256.hexdigest(),
            "target_path": info["target"],
            "version": VERSION,
            # The presence of this key is what tells hylia to build rather than copy.
            "build": {
                "kind": "cargo",
                "binary": info["binary"],
                "unit": info["unit"],
                "files": len(files),
            },
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
    manifest_path = os.path.join(BUILD_DIR, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8", newline="\n") as f_man:
        json.dump(manifest, f_man, indent=2)

    # Sign the manifest bytes exactly as they were just written. Every digest in the
    # manifest describes a file shipped in the same zip, so on its own the manifest
    # certifies itself; the detached signature is what ties it to a key that no node
    # holds and no update server can produce.
    with open(manifest_path, "rb") as f_man_bytes:
        manifest_bytes = f_man_bytes.read()
    if signing_key:
        helios_sig.sign_manifest_file(manifest_path, signing_key)

    # Package into ZIP
    if os.path.exists(ZIP_NAME):
        os.remove(ZIP_NAME)
        
    with zipfile.ZipFile(ZIP_NAME, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
        for file in sorted(os.listdir(BUILD_DIR)):
            file_path = os.path.join(BUILD_DIR, file)
            # The mode is stamped onto the entry explicitly: os.chmod is a no-op on
            # Windows build hosts, so zipfile.write() would archive 0644 there.
            zinfo = zipfile.ZipInfo.from_file(file_path, arcname=file)
            zinfo.compress_type = zipfile.ZIP_DEFLATED
            zinfo.external_attr = (file_modes.get(file, 0o644) & 0xFFFF) << 16
            with open(file_path, "rb") as f_src, zip_ref.open(zinfo, "w") as f_dst:
                shutil.copyfileobj(f_src, f_dst)

    shutil.rmtree(BUILD_DIR)

    package_sha256 = write_release_document(signing_key, components_manifest, manifest_bytes)

    print(f"Successfully created {ZIP_NAME} with version {VERSION}!")
    print(f"  sha256: {package_sha256}")
    if signing_key:
        print(f"  Signed with {signing_key} (key {helios_sig.key_fingerprint(signing_key, private=True)}).")
        print(f"  Publish {RELEASE_DOC_NAME} as the body of /api/v1/releases/latest.")
    else:
        print(f"  UNSIGNED. {RELEASE_DOC_NAME} carries no signature and nodes will refuse it.")

if __name__ == "__main__":
    main()
