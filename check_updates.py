#!/usr/bin/env python3
import urllib.request
import urllib.parse
import json
import time
import sys
import re
import hashlib

# Build string reported when an installed component carries no __build__ tag.
# This script is deployed standalone as /usr/local/bin/check-updates, so the value
# cannot be imported from hylia; it is the single source of truth within this file.
FALLBACK_BUILD = "1.2.0-b4081"

_SHA256_RE = re.compile(r'^[0-9a-fA-F]{64}$')

def cql_escape(value):
    """Escape a value for embedding inside a single-quoted CQL string literal.
    Everything written here originates from a remote update server, so no value
    may be interpolated raw."""
    if value is None:
        return ""
    return str(value).replace("'", "''")

def cql_int(value, default=0):
    """Coerce a remote-supplied numeric field to an integer literal. A non-numeric
    value would otherwise be injected into the statement unquoted."""
    try:
        coerced = int(value)
    except (TypeError, ValueError, OverflowError):
        try:
            coerced = int(float(str(value).strip()))
        except (TypeError, ValueError, OverflowError):
            return default
    return coerced if coerced >= 0 else default

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

def run_cql_query(cql_query):
    try:
        url = "http://127.0.0.1:9043/query"
        req = urllib.request.Request(
            url,
            data=cql_query.encode('utf-8'),
            headers={'Content-Type': 'text/plain'}
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            res = json.loads(response.read().decode('utf-8'))
            if res.get("status") == "success":
                lines = []
                for row in res.get("rows", []):
                    if isinstance(row, dict):
                        if "json" in row:
                            lines.append(row["json"])
                        else:
                            vals = [str(v) for v in row.values()]
                            lines.append(" ".join(vals))
                    else:
                        lines.append(str(row))
                return 0, "\n".join(lines), ""
            else:
                return 1, "", res.get("error", "Database query execution error")
    except Exception as e1:
        # Fallback: run cqlsh inside local podman systemd-hydra-db container
        try:
            import subprocess
            import socket
            import os
            import tempfile
            
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                s.connect(('10.255.255.255', 1))
                local_ip = s.getsockname()[0]
            except Exception:
                local_ip = '127.0.0.1'
            finally:
                s.close()
            
            # Write query to a temporary file on the host
            fd, temp_path = tempfile.mkstemp(suffix=".cql")
            try:
                with os.fdopen(fd, 'w') as tmp:
                    tmp.write(cql_query)
                
                # Copy to container
                container_tmp = f"/tmp/{os.path.basename(temp_path)}"
                subprocess.run(f"podman cp {temp_path} systemd-hydra-db:{container_tmp}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                # Run cqlsh -f inside container
                cmd = f'podman exec systemd-hydra-db cqlsh {local_ip} -f {container_tmp}'
                proc = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=15)
                
                # Clean up in container
                subprocess.run(f"podman exec systemd-hydra-db rm -f {container_tmp}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            finally:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
            
            if proc.returncode == 0:
                lines = []
                if "select" in cql_query.lower():
                    for line in proc.stdout.splitlines():
                        line_stripped = line.strip()
                        if line_stripped.startswith('{') and line_stripped.endswith('}'):
                            lines.append(line_stripped)
                return 0, "\n".join(lines), ""
            else:
                return proc.returncode, "", proc.stderr
        except Exception as e2:
            return -1, "", f"Primary failed ({e1}). Fallback failed ({e2})."

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
                
        cql_schema = """
        CREATE TABLE IF NOT EXISTS hydra.lcm_inventory (
            key text PRIMARY KEY,
            inventory_json text,
            last_updated timestamp
        );
        """
        run_cql_query(cql_schema)
        
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

def main():
    sys.path.append("/usr/local/bin")
    sys.path.append(".")
    
    current_version = FALLBACK_BUILD
    try:
        import importlib.util
        import importlib.machinery
        import os
        hylia_path = "/usr/local/bin/hylia"
        if os.path.exists(hylia_path):
            loader = importlib.machinery.SourceFileLoader("hylia", hylia_path)
            spec = importlib.util.spec_from_loader("hylia", loader)
            hylia_mod = importlib.util.module_from_spec(spec)
            loader.exec_module(hylia_mod)
            current_version = getattr(hylia_mod, "__build__", FALLBACK_BUILD)
        else:
            import hylia
            current_version = getattr(hylia, "__build__", FALLBACK_BUILD)
    except Exception:
        try:
            with open("/usr/local/bin/hylia", "r") as f:
                for line in f:
                    if "__build__" in line:
                        parts = line.split("=")
                        if len(parts) >= 2:
                            current_version = parts[1].strip().strip('"').strip("'")
                            break
        except Exception:
            pass

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
        
        # 1. Base check: compare overall build version
        update_available = (latest_version != current_version)
        
        # 2. Component check: check if any component on any node does not match the latest release
        if not update_available and latest_components and installed_inv:
            for host_name, host_info in installed_inv.items():
                for comp_name, target_ver in latest_components.items():
                    installed_ver = host_info.get("versions", {}).get(comp_name)
                    if installed_ver == "Unknown" or not installed_ver:
                        installed_ver = FALLBACK_BUILD
                    if installed_ver != target_ver:
                        update_available = True
                        break
                if update_available:
                    break
        
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
