#!/usr/bin/env python3
import urllib.request
import urllib.parse
import json
import time
import sys
import re
import hashlib

# The cluster's one CQL query layer. Fifteen files carried their own copy of this, most
# of them identical, and the guard against conditional statements had reached only three
# of them -- see helios_cql for what that cost.
from helios_cql import (  # noqa: F401  (re-exported for modules that import from here)
    ConditionalStatementError,
    cql_escape,
    cql_int,
    is_conditional_cql,
    run_conditional_cql_query,
    run_cql_query,
)

# Build string reported when an installed component carries no __build__ tag.
# This script is deployed standalone as /usr/local/bin/check-updates, so the value
# cannot be imported from hylia; it is the single source of truth within this file.
#
# It means "installed, from before builds were tagged". It does NOT mean "we could not
# find out" -- see read_current_version() for why that distinction is the whole bug this
# file used to have.
FALLBACK_BUILD = "1.2.0-b4081"

# What lcm_update_state.current_version says when this node's build could not be read.
# Deliberately not a version string: it must not compare equal or unequal to a real one
# by accident, and it has to be visibly wrong in the console.
UNKNOWN_VERSION = "unknown"

# What Spark's /api/v1/node/binary-version returns for a component it could not be asked
# about at all, versus the ones it could. "Not Installed" and "Unknown" are answers --
# the file is missing, or it is there without a __build__ tag. "N/A" is what this script
# writes when the request itself failed, which is not an answer about the component.
VERSION_UNREADABLE = "N/A"

# hylia carries this node's build tag. Named here so a test can point the read at a
# fixture instead of the installed file.
HYLIA_PATH = "/usr/local/bin/hylia"

_SHA256_RE = re.compile(r'^[0-9a-fA-F]{64}$')



def validate_download_url(url):
    """The download URL is fetched (and shown in the UI) later on, so only accept a
    plain https:// URL from the update server."""
    url = "" if url is None else str(url).strip()
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise Exception(f"Update server returned an invalid download_url: {url!r}")
    return url

def validate_package_digest(digest):
    """The package digest is what Spectrum checks the downloaded zip against, and it
    refuses anything that is not 64 hex characters. Catch it here instead, where the
    release that supplied it can still be named."""
    digest = "" if digest is None else str(digest).strip()
    if not _SHA256_RE.match(digest):
        raise Exception(f"Update server returned an invalid package sha256: {digest!r}")
    return digest.lower()

def load_signing_module():
    """Import helios_sig, or fail the check.

    This script is deployed as /usr/local/bin/check-updates, a name python cannot
    import, so it loads its neighbour by path the same way it loads hylia. There is no
    branch here that carries on without the module: a check that cannot verify a
    signature has nothing to say about whether an update is safe to offer, and the
    honest outcome is an error in the console rather than an update nobody vouched for.
    """
    try:
        import helios_sig
        return helios_sig
    except ImportError:
        pass
    module_path = "/usr/local/bin/helios_sig.py"
    try:
        import importlib.util
        import importlib.machinery
        import os
        if os.path.exists(module_path):
            loader = importlib.machinery.SourceFileLoader("helios_sig", module_path)
            spec = importlib.util.spec_from_loader("helios_sig", loader)
            module = importlib.util.module_from_spec(spec)
            loader.exec_module(module)
            return module
    except Exception as e:
        raise Exception(f"Update signature verification is unavailable: {module_path} could not be loaded ({e}).")
    raise Exception(
        f"Update signature verification is unavailable: {module_path} is missing. "
        "It ships in the upgrade package and is written by provision.py; reinstall it "
        "on this node before update checks can run."
    )

def resolve_release_document(document, public_key_path=None):
    """Return the release fields that a signature actually covers.

    The bug this closes: the old code read latest_version, download_url and its
    sha256 straight out of the response body. A digest supplied by whoever supplied
    the URL proves the download arrived intact, and nothing else -- so anyone able to
    answer for the update host could hand every node a package of their choosing and
    the matching hash, and the whole chain below (Spectrum's zip check, hylia's
    per-component digests) would confirm it faithfully all the way onto root's PATH.

    Now the server sends a signed text blob and the fields are read out of that,
    verified against the key pinned on this node at provision time. Values sitting
    beside it in the response are ignored entirely; trusting one because a signature
    elsewhere in the body verified is the same bug with a signature stapled to it.

    Returns (release fields, note) where note is empty for a verified release and a
    banner for the one case an operator can deliberately opt into.
    """
    helios_sig = load_signing_module()
    key_path = public_key_path or helios_sig.PINNED_PUBLIC_KEY_PATH
    try:
        release, key_id = helios_sig.verify_signed_document(document, key_path)
    except helios_sig.SignatureMissing as e:
        if not helios_sig.unsigned_updates_permitted():
            raise Exception(f"Refusing this release. {e} {helios_sig.unsigned_override_hint()}")
        note = (f"UNVERIFIED RELEASE: accepted without a signature because "
                f"{helios_sig.UNSIGNED_OVERRIDE_ENV} is set. {e}")
        print("=" * 78)
        print(note)
        print("Nothing in this release has been shown to come from the Helios release key.")
        print("=" * 78)
        return document, note
    except helios_sig.SignatureError as e:
        # Present and wrong is never a migration problem, so the escape hatch does not
        # reach this branch. Either the package was tampered with or the release key
        # was rotated without re-pinning it; both need a human, not a retry.
        raise Exception(f"Refusing this release. {e}")

    detail = f" (key {key_id})" if key_id else ""
    print(f"Release signature verified against the key pinned at {key_path}{detail}.")
    return release, ""


def load_schema_module():
    """Import the ordered cluster schema, wherever this process is running from.

    On a host it sits in /usr/local/bin beside this file; inside the Spectrum container
    it is copied to /app. Neither location is importable by name from the other.
    """
    try:
        import helios_schema
        return helios_schema
    except ImportError:
        pass
    import importlib.util
    import os as _os
    for candidate in ("/usr/local/bin/helios_schema.py",
                      _os.path.join(_os.path.dirname(_os.path.abspath(__file__)),
                                    "helios_schema.py")):
        if not _os.path.exists(candidate):
            continue
        spec = importlib.util.spec_from_file_location("helios_schema", candidate)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    raise ImportError(
        "helios_schema.py was not found. The cluster schema cannot be applied without "
        "it; reinstall the Helios components.")

def collect_inventory():
    try:
        import socket
        import json
        import urllib.request
        import urllib.parse
        from concurrent.futures import ThreadPoolExecutor
        import concurrent.futures
        
        import importlib.util
        import importlib.machinery
        import os
        
        hylia = None
        hylia_path = "/usr/local/bin/hylia"
        if os.path.exists(hylia_path):
            loader = importlib.machinery.SourceFileLoader("hylia", hylia_path)
            spec = importlib.util.spec_from_loader("hylia", loader)
            hylia = importlib.util.module_from_spec(spec)
            loader.exec_module(hylia)
        else:
            try:
                import hylia as hylia_import
                hylia = hylia_import
            except ImportError:
                pass
                
        if not hylia:
            raise Exception("Could not load hylia module")
            
        hosts = hylia.get_cluster_hosts()
        if not hosts:
            hosts = [{"hostname": socket.gethostname(), "ip": "127.0.0.1"}]
            
        components_paths = {
            "spark": "/usr/local/bin/spark",
            "impa": "/usr/local/bin/impa",
            "saga": "/usr/local/bin/saga",
            "helios-schema": "/usr/local/bin/helios_schema.py",
            "spark-daemon": "/usr/local/bin/spark-daemon",
            "bifrost": "/usr/local/bin/bifrost",
            "valcli": "/usr/local/bin/valcli",
            "mcli": "/usr/local/bin/mcli",
            "mcli-runner": "/usr/local/bin/mcli-runner",
            "dagur": "/usr/local/bin/dagur",
            "mimir": "/usr/local/bin/mimir",
            "vali": "/usr/local/bin/vali",
            "catalyst": "/usr/local/bin/catalyst",
            "catcli": "/usr/local/bin/catcli",
            "gatoway": "/usr/local/bin/gatoway",
            "urbosa": "/usr/local/bin/urbosa",
            "logos": "/usr/local/bin/logos",
            "mipha": "/usr/local/bin/mipha",
            "urbosa-bootstrap": "/usr/local/bin/urbosa-bootstrap",
            "daruk": "/usr/local/bin/daruk.py",
            "cluster": "/usr/local/bin/cluster",
            "hylia": "/usr/local/bin/hylia",
            "lanayru": "/usr/local/bin/lanayru.py",
            "helios-zk": "/usr/local/bin/helios_zk.py",
            "helios-sig": "/usr/local/bin/helios_sig.py",
            "helios-sidon": "/usr/local/bin/helios_sidon.py",
            "check-updates": "/usr/local/bin/check-updates",
            "nodetool": "/usr/local/bin/nodetool",
            "allssh": "/usr/local/bin/allssh",
            "spectrum": "/usr/local/bin/spectrum_server",
            "Dockerfile": "/usr/local/bin/Dockerfile"
        }
        
        inventory = {}
        
        def fetch_version(host_ip, comp_name, target_path):
            rc_v, res_v, err_v = hylia.run_mtls_spark_api(
                host_ip,
                f"/api/v1/node/binary-version?path={urllib.parse.quote(target_path)}",
                None,
                method="GET"
            )
            if rc_v == 0 and "version" in res_v:
                return comp_name, res_v["version"]
            return comp_name, "N/A"
            
        with ThreadPoolExecutor(max_workers=30) as executor:
            futures = {}
            for h in hosts:
                host_ip = h["ip"]
                host_name = h["hostname"]
                inventory[host_name] = {"ip": host_ip, "versions": {}}
                for comp_name, target_path in components_paths.items():
                    f = executor.submit(fetch_version, host_ip, comp_name, target_path)
                    futures[f] = (host_name, comp_name)
                    
            for f in concurrent.futures.as_completed(futures):
                host_name, comp_name = futures[f]
                _, version = f.result()
                inventory[host_name]["versions"][comp_name] = version
                
        # hydra.lcm_inventory belongs to the cluster schema, not to this script.
        load_schema_module().ensure_schema(run_cql_query)
        
        inventory_escaped = cql_escape(json.dumps(inventory))
        cql_insert = f"""
        INSERT INTO hydra.lcm_inventory (key, inventory_json, last_updated) VALUES (
            'latest', '{inventory_escaped}', toTimestamp(now())
        );
        """
        run_cql_query(cql_insert)
        print("Cluster inventory successfully collected and saved to ScyllaDB.")
        return inventory
    except Exception as e:
        print(f"Warning: Failed to collect cluster inventory: {e}")
        return {}

def read_current_version(hylia_path=None):
    """This node's installed build, or None when it could not be determined.

    The bug this closes: `current_version` started at FALLBACK_BUILD and every failure
    path left it there. So a node where hylia could not be imported *or* read -- a broken
    interpreter, a half-finished upgrade, a permissions problem, the file simply missing
    -- reported the build from before builds were tagged, which is not equal to any
    release the server will ever publish. The check below is `latest_version !=
    current_version`, so that node announced "update available" on every run, forever,
    and no amount of updating could clear it: the next run could not read the version it
    had just installed either.

    Unknown is not old. Returning None here lets the caller say "this node's version
    could not be read" instead of answering a question it has no data for.

    A hylia that loads but has no `__build__` attribute is a different case and does
    return FALLBACK_BUILD: the component is installed and genuinely predates build tags.
    """
    import importlib.machinery
    import importlib.util
    import os

    hylia_path = HYLIA_PATH if hylia_path is None else hylia_path
    try:
        if os.path.exists(hylia_path):
            loader = importlib.machinery.SourceFileLoader("hylia", hylia_path)
            spec = importlib.util.spec_from_loader("hylia", loader)
            hylia_mod = importlib.util.module_from_spec(spec)
            loader.exec_module(hylia_mod)
            return getattr(hylia_mod, "__build__", FALLBACK_BUILD)
        import hylia
        return getattr(hylia, "__build__", FALLBACK_BUILD)
    except Exception:
        pass

    # Executing hylia failed. It may still be readable as text, and the build tag is a
    # literal assignment near the top of it.
    try:
        with open(hylia_path, "r") as f:
            for line in f:
                if "__build__" in line:
                    parts = line.split("=")
                    if len(parts) >= 2:
                        value = parts[1].strip().strip('"').strip("'")
                        if value:
                            return value
    except Exception:
        pass

    return None


def decide_update_available(latest_version, current_version, latest_components, installed_inv):
    """Whether an update should be offered, and what could not be compared.

    `current_version` is None when this node's build could not be read at all. Returns
    (update_available, notes), where `notes` names everything that was skipped rather
    than guessed at; the caller writes them into `lcm_update_state.error_msg`, which
    Spectrum renders on the LCM page.

    The rule the whole function exists to enforce: **unknown never counts as a
    mismatch.** Every comparison here is an inequality against a release version, so any
    value substituted for "we could not find out" is unequal to the release forever, and
    the console offers an update that installing cannot clear. An operator who is not
    being offered an update is entitled to know whether that is because there is none or
    because the question could not be answered -- but not to be told there is one on the
    strength of a value nobody read.
    """
    notes = []

    # 1. Base check: this node's build against the release.
    if current_version is None:
        update_available = False
        notes.append(
            f"This node's installed build could not be read from {HYLIA_PATH}, so it "
            f"cannot be compared against the latest release. Repair or reinstall hylia on "
            f"this node; until then this check cannot say whether an update is needed.")
    else:
        update_available = (latest_version != current_version)

    # 2. Component check: any component on any node that differs from the release.
    #
    # Only components that actually answered are compared. VERSION_UNREADABLE means the
    # node could not be asked, which is not a version -- the old code substituted
    # FALLBACK_BUILD for it, so a single unreachable node was a permanent update prompt.
    # "Not Installed" and "Unknown" *are* answers about the component and are compared.
    unreadable = []
    if latest_components and installed_inv:
        for host_name, host_info in (installed_inv or {}).items():
            versions = (host_info or {}).get("versions", {}) or {}
            for comp_name, target_ver in latest_components.items():
                installed_ver = versions.get(comp_name)
                if not installed_ver or installed_ver == VERSION_UNREADABLE:
                    unreadable.append(f"{host_name}/{comp_name}")
                    continue
                if installed_ver == "Unknown":
                    installed_ver = FALLBACK_BUILD
                if installed_ver != target_ver:
                    update_available = True

    if unreadable:
        shown = ", ".join(sorted(unreadable)[:8])
        more = "" if len(unreadable) <= 8 else f" (+{len(unreadable) - 8} more)"
        notes.append(
            f"{len(unreadable)} component version(s) could not be read and were not "
            f"compared: {shown}{more}.")

    return update_available, notes


def main():
    sys.path.append("/usr/local/bin")
    sys.path.append(".")

    current_version = read_current_version()
    current_version_known = current_version is not None
    if not current_version_known:
        current_version = UNKNOWN_VERSION

    cb = int(time.time())
    url = f"https://updates-helios.zerotwo.cloud/api/v1/releases/latest?cb={cb}"
    print(f"Checking updates from: {url}")
    req = urllib.request.Request(url, headers={'User-Agent': 'Helios-Spectrum-Updater'})
    
    now_ms = int(time.time() * 1000)
    
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode('utf-8'))
            
        # Nothing below reads from `data` again: every field an update decision rests
        # on comes out of the half the release key signed.
        release, signature_note = resolve_release_document(data)

        latest_version = release.get("latest_version")
        release_date = release.get("release_date")
        download_url = validate_download_url(release.get("download_url"))
        sha256 = validate_package_digest(release.get("sha256"))
        size = cql_int(release.get("size", 0))
        changelog = release.get("changelog", "")
        latest_components = release.get("components", {})
        
        # Collect current inventory first
        installed_inv = collect_inventory()

        update_available, unknowns = decide_update_available(
            latest_version,
            current_version if current_version_known else None,
            latest_components,
            installed_inv)

        if unknowns and signature_note:
            signature_note = signature_note + " " + " ".join(unknowns)
        elif unknowns:
            signature_note = " ".join(unknowns)

        # Ensure schema table exists first
        cql_schema = """
        CREATE TABLE IF NOT EXISTS hydra.lcm_update_state (
            key text PRIMARY KEY,
            latest_version text,
            release_date text,
            download_url text,
            sha256 text,
            size bigint,
            changelog text,
            current_version text,
            update_available boolean,
            last_checked timestamp,
            error_msg text
        );
        """
        run_cql_query(cql_schema)
        
        # Insert update state. Every value below comes from the remote update server,
        # so all of them are escaped and 'size' is coerced to an integer literal.
        # error_msg carries the unsigned-release banner when there is one: Spectrum
        # surfaces that column as the LCM page's error, which is the only place an
        # operator would see that this release was never verified.
        cql_insert = f"""
        INSERT INTO hydra.lcm_update_state (
            key, latest_version, release_date, download_url, sha256, size,
            changelog, current_version, update_available, last_checked, error_msg
        ) VALUES (
            'latest', '{cql_escape(latest_version)}', '{cql_escape(release_date)}',
            '{cql_escape(download_url)}', '{cql_escape(sha256)}', {size},
            '{cql_escape(changelog)}', '{cql_escape(current_version)}',
            {'true' if update_available else 'false'}, {now_ms}, '{cql_escape(signature_note)}'
        );
        """
        rc, _, err = run_cql_query(cql_insert)
        if rc != 0:
            raise Exception(f"Database write failed: {err}")
            
        print("Update status successfully checked and saved to ScyllaDB.")
        print(f"Latest: {latest_version} (Current: {current_version}) | Available: {update_available}")
        for note in unknowns:
            print(f"NOT COMPARED: {note}")
        sys.exit(0)
        
    except Exception as e:
        error_msg = cql_escape(e)
        print(f"Error checking updates: {e}")
        
        # Write error state to database
        cql_schema = """
        CREATE TABLE IF NOT EXISTS hydra.lcm_update_state (
            key text PRIMARY KEY,
            latest_version text,
            release_date text,
            download_url text,
            sha256 text,
            size bigint,
            changelog text,
            current_version text,
            update_available boolean,
            last_checked timestamp,
            error_msg text
        );
        """
        run_cql_query(cql_schema)
        
        cql_error = f"""
        INSERT INTO hydra.lcm_update_state (
            key, last_checked, error_msg
        ) VALUES (
            'latest', {now_ms}, '{error_msg}'
        );
        """
        run_cql_query(cql_error)
        try:
            collect_inventory()
        except Exception:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()
