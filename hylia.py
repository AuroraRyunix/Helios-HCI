#!/usr/bin/env python3
__build__ = "1.2.3-stable"
import sys
import os
import json
import time
import socket
import urllib.request
import ssl
import subprocess
import base64
import uuid
import threading
import zipfile
import hashlib
import re

def run_command_local(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return res.returncode, res.stdout.decode('utf-8', errors='ignore').strip(), res.stderr.decode('utf-8', errors='ignore').strip()

def spark_endpoint(ip):
    """Return (address, verify_identity) for an mTLS call to a spark-daemon.

    Node certificates carry `subjectAltName = IP:<node ip>` and nothing else, so a
    connection can only be tied to the node answering it when it is addressed by that
    same IP. Verification used to be off everywhere, which meant any certificate the
    cluster CA ever signed -- every node's own included -- satisfied a connection to any
    other node.

    Loopback is in no node's SAN. spark-daemon binds 0.0.0.0:9099, so this node's own
    address reaches the same listener and does verify; where that address is unknown the
    identity check is dropped rather than failing the call, since a loopback connection
    cannot be answered by another node in the first place.
    """
    if ip in ("127.0.0.1", "::1", "localhost"):
        if LOCAL_IP and LOCAL_IP not in ("127.0.0.1", "::1", "localhost"):
            return LOCAL_IP, True
        return ip, False
    return ip, True

def run_remote_spark(ip, command, timeout=45):
    ip, verify_identity = spark_endpoint(ip)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = verify_identity

    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command, "timeout": timeout}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, context=context, timeout=timeout + 15) as response:
                res = json.loads(response.read().decode("utf-8"))
                return res["returncode"], res["stdout"], res["stderr"]
        except Exception as e:
            if attempt == 4:
                return -1, "", str(e)
            time.sleep(2)

def run_mtls_spark_api(ip, path, payload, method="POST"):
    ip, verify_identity = spark_endpoint(ip)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = verify_identity

    url = f"https://{ip}:9099{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=120) as response:
            res = json.loads(response.read().decode("utf-8"))
            return 0, res, ""
    except Exception as e:
        return -1, {}, str(e)

def run_cql_query(cql_query):
    try:
        url = "http://127.0.0.1:9043/query"
        req = urllib.request.Request(
            url,
            data=cql_query.encode('utf-8'),
            headers={'Content-Type': 'text/plain'}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
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
    except Exception as e:
        import base64
        import subprocess
        import socket
        try:
            # Resolve local IP
            local_ip = "127.0.0.1"
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(('10.255.255.255', 1))
                local_ip = s.getsockname()[0]
                s.close()
            except Exception:
                pass
            b64_query = base64.b64encode(cql_query.encode('utf-8')).decode('utf-8')
            cmd = f'echo {b64_query} | base64 -d | podman exec -i systemd-hydra-db cqlsh {local_ip}'
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            stdout_str = stdout.decode('utf-8', errors='ignore').strip()
            # Fix double backslashes introduced by cqlsh formatting
            stdout_str = stdout_str.replace('\\\\', '\\')
            return p.returncode, stdout_str, stderr.decode('utf-8', errors='ignore').strip()
        except Exception as ex:
            return -1, "", f"HTTP error: {e}, fallback error: {ex}"

def get_cluster_hosts():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            return cdata.get("hosts", [])
    except Exception:
        return []

LOCAL_IP = "127.0.0.1"
try:
    with open("/etc/hci/spectrum/spectrum.env", "r") as f:
        for line in f:
            if "LOCAL_IP=" in line or "LOCAL_HYPERVISOR_IP=" in line:
                v = line.split("=", 1)[1].strip().strip("'\"")
                if v:
                    LOCAL_IP = v
except Exception:
    pass

def get_zookeeper_leader_ip():
    hosts = get_cluster_hosts()
    ips = [h.get("ip") for h in hosts if h.get("ip")] if hosts else []
    for ip in ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((ip, 2181))
            s.sendall(b"stat")
            resp = s.recv(1024).decode('utf-8', errors='ignore')
            s.close()
            if "mode: leader" in resp.lower() or "mode: standalone" in resp.lower():
                return ip
        except Exception:
            pass
    return None

def is_zookeeper_leader():
    return get_zookeeper_leader_ip() == LOCAL_IP

def log_upgrade(job_id, line):
    print(f"[Hylia] {line}")
    timestamp = int(time.time() * 1000)
    # Escape quotes
    escaped_line = line.replace("'", "''")
    cql = f"INSERT INTO hydra.hylia_logs (job_id, timestamp, log_line) VALUES ({job_id}, {timestamp}, '{escaped_line}');"
    run_cql_query(cql)

# manifest.json is the one file in an update package that nothing hashes, so every
# value it carries is untrusted input that eventually reaches a root shell on every
# node. Components may only be installed below these directory prefixes, using a
# restricted character set that cannot escape a shell word.
ALLOWED_TARGET_PREFIXES = ("/usr/local/bin/",)
_SAFE_PATH_RE = re.compile(r'^[A-Za-z0-9._/-]+$')
_SAFE_FILENAME_RE = re.compile(r'^[A-Za-z0-9._-]+$')
_SHA256_RE = re.compile(r'^[0-9a-fA-F]{64}$')

def validate_target_path(target_path, comp_name):
    """Reject any manifest target_path that is not a plain absolute file path under
    an allowlisted install directory. Returns the validated path."""
    if not target_path or not isinstance(target_path, str):
        raise Exception(f"Component '{comp_name}' declares no target_path in the manifest.")
    if not target_path.startswith("/"):
        raise Exception(f"Component '{comp_name}' target_path '{target_path}' is not an absolute path.")
    if not _SAFE_PATH_RE.match(target_path):
        raise Exception(f"Component '{comp_name}' target_path '{target_path}' contains illegal characters.")
    if target_path.endswith("/"):
        raise Exception(f"Component '{comp_name}' target_path '{target_path}' is a directory, not a file.")
    if ".." in target_path.split("/"):
        raise Exception(f"Component '{comp_name}' target_path '{target_path}' contains a '..' segment.")
    if not any(target_path.startswith(prefix) for prefix in ALLOWED_TARGET_PREFIXES):
        allowed = ", ".join(ALLOWED_TARGET_PREFIXES)
        raise Exception(f"Component '{comp_name}' target_path '{target_path}' is outside the permitted install directories ({allowed}).")
    return target_path

def validate_component_filename(comp_file, comp_name):
    """Manifest 'file' entries must be bare filenames living inside the extract
    directory; anything with a path separator could escape it."""
    if not comp_file or not isinstance(comp_file, str):
        raise Exception(f"Component '{comp_name}' declares no file in the manifest.")
    if comp_file in (".", "..") or not _SAFE_FILENAME_RE.match(comp_file):
        raise Exception(f"Component '{comp_name}' file '{comp_file}' is not a plain filename.")
    return comp_file

def validate_declared_hash(declared_hash, comp_name, comp_file):
    """A component entry without a well-formed digest cannot be verified at all."""
    if not isinstance(declared_hash, str) or not _SHA256_RE.match(declared_hash):
        raise Exception(f"Checksum verification failed for '{comp_file}' (component '{comp_name}'): declared sha256 '{declared_hash}' is not a 64-character hex digest.")
    return declared_hash.lower()

def validate_manifest_component(comp_name, comp_info):
    """Structurally validate one manifest component entry.
    Returns (comp_file, declared_hash, target_path)."""
    if not isinstance(comp_info, dict):
        raise Exception(f"Manifest entry for component '{comp_name}' is not an object.")
    comp_file = validate_component_filename(comp_info.get("file"), comp_name)
    declared_hash = validate_declared_hash(comp_info.get("sha256"), comp_name, comp_file)
    target_path = validate_target_path(comp_info.get("target_path") or f"/usr/local/bin/{comp_name}", comp_name)
    return comp_file, declared_hash, target_path

def hash_file(file_path):
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f_bin:
        while chunk := f_bin.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()

def _load_signing_module():
    """Import helios_sig from wherever this process happens to be running.

    On a host, hylia sits in /usr/local/bin next to it. Inside the Spectrum container it
    runs from /app, where the module is copied by the Dockerfile. Neither location is
    importable by name from the other, so both are tried.
    """
    try:
        import helios_sig
        return helios_sig
    except ImportError:
        pass
    import importlib.util
    for candidate in ("/usr/local/bin/helios_sig.py",
                      os.path.join(os.path.dirname(os.path.abspath(__file__)), "helios_sig.py")):
        if not os.path.exists(candidate):
            continue
        spec = importlib.util.spec_from_file_location("helios_sig", candidate)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    raise Exception(
        "Cannot verify this update package: helios_sig.py was not found. Refusing to "
        "install an unverified package. Reinstall the LCM components, or see "
        "docs/update_signing.md.")


def _verify_package_signature(extract_dir):
    """Refuse a package whose manifest was not signed by the pinned release key.

    An unsigned package is refused unless the operator has explicitly set the transition
    variable; a *badly* signed one is refused in every case, because a bad signature is
    evidence of tampering rather than of an old release process.
    """
    helios_sig = _load_signing_module()
    try:
        helios_sig.verify_package_manifest(extract_dir)
    except helios_sig.SignatureMissing as exc:
        if not helios_sig.unsigned_updates_permitted():
            raise Exception(
                "Refusing this update package. %s %s"
                % (exc, helios_sig.unsigned_override_hint()))
        print("[Hylia] UNVERIFIED PACKAGE: %s Installing anyway because the unsigned "
              "override is set. Nothing in this package has been shown to come from the "
              "Helios release key." % exc)


def validate_and_extract_zip(zip_path, extract_dir):
    if os.path.exists(extract_dir):
        import shutil
        try:
            shutil.rmtree(extract_dir)
        except Exception:
            pass
    os.makedirs(extract_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    # Before the manifest is trusted for anything. Every component digest lives in the
    # manifest, so one signature over it transitively covers every file and every install
    # path in the package -- but only if it is checked before those digests are read.
    #
    # This is the anchor for the manual upload path in particular. `check-updates`
    # verifies what the update server advertises, and a package handed straight to the
    # console never passed through it.
    _verify_package_signature(extract_dir)

    manifest_path = os.path.join(extract_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        raise Exception("manifest.json not found in update package.")

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    if not isinstance(manifest, dict):
        raise Exception("manifest.json is malformed: expected a JSON object.")

    components = manifest.get("components")
    if not isinstance(components, dict) or not components:
        raise Exception("manifest.json is malformed: 'components' is missing or is not a non-empty object.")

    for comp_name, comp_info in components.items():
        comp_file, declared_hash, _ = validate_manifest_component(comp_name, comp_info)

        file_path = os.path.join(extract_dir, comp_file)
        if not os.path.exists(file_path):
            raise Exception(f"Declared file '{comp_file}' for component '{comp_name}' is missing.")

        actual_hash = hash_file(file_path)

        if actual_hash != declared_hash:
            raise Exception(f"Checksum verification failed for '{comp_file}'. Declared: {declared_hash}, Actual: {actual_hash}")

    # The changelog name is manifest-controlled too; keep it inside the package.
    changelog_file = manifest.get("changelog") or "changelog.md"
    validate_component_filename(changelog_file, "changelog")
    changelog_path = os.path.join(extract_dir, changelog_file)
    changelog_content = ""
    if os.path.exists(changelog_path):
        with open(changelog_path, "r", encoding="utf-8", errors="ignore") as f_ch:
            changelog_content = f_ch.read()
            
    return manifest, changelog_content

def get_service_build_number(target_path):
    if not os.path.exists(target_path):
        return "Not Installed"
    try:
        with open(target_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if "__build__" in line and "=" in line:
                    parts = line.split("=", 1)
                    val = parts[1].strip().strip("'\"")
                    return val
    except Exception:
        pass
    return "Unknown"

def get_remote_sha256(node_ip, remote_path):
    rc, out, _ = run_remote_spark(node_ip, f"sha256sum '{remote_path}'")
    if rc != 0 or not out or not out.strip():
        return None
    return out.strip().split()[0].lower()

def deploy_component(job_id, node_ip, hostname, comp_name, comp_info, extract_dir):
    """Install one component on a node via a staged, verified, atomic swap.

    The live file is never removed up front: the payload is streamed to a staging
    path, decoded, checksum-verified against the manifest and only then renamed
    over the target. The previous file is kept as a backup until the new one is
    confirmed in place, so an interrupted transfer can never leave a node without
    (for example) spark-daemon, which is the channel used to push a fix.
    """
    comp_file, declared_hash, target_path = validate_manifest_component(comp_name, comp_info)

    local_file_path = os.path.join(extract_dir, comp_file)
    if not os.path.exists(local_file_path):
        raise Exception(f"Staged file '{local_file_path}' for component '{comp_name}' is missing. The extracted update package is gone; re-upload the package and restart the job.")

    with open(local_file_path, "rb") as f_bin:
        file_bytes = f_bin.read()

    # Re-verify the staged copy against the manifest before it leaves this node:
    # extraction happened at upload time and /tmp is writable by anyone.
    local_hash = hashlib.sha256(file_bytes).hexdigest()
    if local_hash != declared_hash:
        raise Exception(f"Staged copy of '{comp_file}' no longer matches the manifest checksum (declared {declared_hash}, actual {local_hash}). Refusing to deploy component '{comp_name}'.")

    # CRLF is normalised here rather than with a remote 'sed -i' so that the digest
    # verified on the node covers exactly the bytes that were transmitted. For a
    # package built with LF endings this is a no-op and expected_hash == declared_hash.
    payload = file_bytes.replace(b"\r\n", b"\n")
    expected_hash = hashlib.sha256(payload).hexdigest()
    b64_data = base64.b64encode(payload).decode("utf-8")

    remote_dir = os.path.dirname(target_path)
    staged_b64 = f"{target_path}.hylia_b64"
    staged_new = f"{target_path}.hylia_new"
    backup_path = f"{target_path}.hylia_bak"
    cleanup_cmd = f"rm -f '{staged_b64}' '{staged_new}'"
    # 'cp -p' preserves the original mode into the backup, so restoring is a plain move.
    restore_cmd = f"if [ -e '{backup_path}' ]; then mv -f '{backup_path}' '{target_path}'; fi; {cleanup_cmd}"

    log_upgrade(job_id, f"[{hostname}] Transferring component '{comp_name}' to {target_path}...")

    # Clear stale staging files from any previously interrupted transfer, otherwise
    # the appends below would concatenate onto them and produce corrupt base64.
    # The live target is deliberately left untouched.
    rc_p, _, err_p = run_remote_spark(node_ip, f"mkdir -p '{remote_dir}' && {cleanup_cmd}")
    if rc_p != 0:
        raise Exception(f"Failed to prepare staging area for '{comp_name}' on {hostname}: {err_p}")

    chunk_size = 64000
    for c_idx in range(0, len(b64_data), chunk_size):
        sub_chunk = b64_data[c_idx:c_idx+chunk_size]
        rc_w, _, err_w = run_remote_spark(node_ip, f"echo '{sub_chunk}' >> '{staged_b64}'")
        if rc_w != 0:
            run_remote_spark(node_ip, cleanup_cmd)
            raise Exception(f"Failed to write file chunk for '{comp_name}' to {hostname}: {err_w}")

    rc_d, _, err_d = run_remote_spark(node_ip, f"base64 -d < '{staged_b64}' > '{staged_new}' && rm -f '{staged_b64}'")
    if rc_d != 0:
        run_remote_spark(node_ip, cleanup_cmd)
        raise Exception(f"Failed to decode transferred component '{comp_name}' on {hostname}: {err_d}")

    staged_hash = get_remote_sha256(node_ip, staged_new)
    if staged_hash != expected_hash:
        run_remote_spark(node_ip, cleanup_cmd)
        raise Exception(f"Checksum verification failed for '{comp_name}' on {hostname} after transfer (expected {expected_hash}, got {staged_hash}). Existing file left untouched.")

    # Back up the running file, then swap the verified copy in with a rename, which
    # is atomic because staging and target share a directory.
    swap_cmd = (
        f"rm -f '{backup_path}' && "
        f"if [ -e '{target_path}' ]; then cp -p '{target_path}' '{backup_path}'; fi && "
        f"chmod +x '{staged_new}' && mv -f '{staged_new}' '{target_path}'"
    )
    rc_s, _, err_s = run_remote_spark(node_ip, swap_cmd)
    if rc_s != 0:
        run_remote_spark(node_ip, restore_cmd)
        raise Exception(f"Failed to activate component '{comp_name}' on {hostname}: {err_s}. Previous version restored.")

    # Confirm the live path really is the new file before discarding the backup.
    live_hash = get_remote_sha256(node_ip, target_path)
    if live_hash != expected_hash:
        run_remote_spark(node_ip, restore_cmd)
        raise Exception(f"Post-install verification failed for '{comp_name}' on {hostname} (expected {expected_hash}, got {live_hash}). Previous version restored.")

    run_remote_spark(node_ip, f"rm -f '{backup_path}'; {cleanup_cmd}")
    log_upgrade(job_id, f"[{hostname}] Component '{comp_name}' installed and verified ({expected_hash[:12]}...).")
    return True

def get_hostname_by_ip(node_ip):
    rc_h, stdout_h, _ = run_cql_query(f"SELECT JSON hostname FROM hydra.nodes WHERE ip = '{node_ip}' ALLOW FILTERING;")
    if rc_h == 0 and stdout_h:
        try:
            for line in stdout_h.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    return json.loads(line).get("hostname") or node_ip
        except Exception:
            pass
    return node_ip

def verify_node_storage_health(job_id, node_ip, hostname):
    log_upgrade(job_id, f"[{hostname}] Verifying DRBD volume synchronization status...")
    
    # Get status of other nodes in the cluster
    normal_hosts = set()
    rc_n, stdout_n, _ = run_cql_query("SELECT JSON ip, hostname, status FROM hydra.nodes;")
    if rc_n == 0 and stdout_n:
        for line in stdout_n.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    nd = json.loads(line)
                    if nd.get("status") == "NORMAL" and nd.get("hostname") != hostname:
                        normal_hosts.add(nd.get("hostname"))
                except Exception:
                    pass

    single_node = len(normal_hosts) == 0
    if single_node:
        log_upgrade(job_id, f"[{hostname}] Single-node cluster detected. Skipping DRBD peer checks (no replication peers expected).")

    # Poll for up to 5 minutes
    for attempt in range(60):
        rc_st, out_st, _ = run_remote_spark(node_ip, "drbdadm status 2>/dev/null || true")
        if rc_st != 0 or not out_st:
            if attempt % 5 == 0:
                log_upgrade(job_id, f"[{hostname}] Warning: failed to query DRBD status, retrying...")
            time.sleep(5)
            continue
            
        # Parse DRBD status
        lines = out_st.splitlines()
        unhealthy = False
        reasons = []
        
        current_resource = "unknown"
        for line in lines:
            line_strip = line.strip()
            if not line_strip:
                continue
            
            if not line.startswith(" ") and "role:" in line_strip:
                current_resource = line_strip.split()[0]
            elif line_strip.startswith("disk:"):
                disk_state = line_strip.split(":", 1)[1].split()[0]
                # On a single-node cluster, DUnknown is expected (no peer to negotiate with)
                bad_disk_states = ["Inconsistent", "Outdated", "Negotiating"]
                if not single_node:
                    bad_disk_states.append("DUnknown")
                if disk_state in bad_disk_states:
                    unhealthy = True
                    reasons.append(f"{current_resource} local disk state is {disk_state}")
            elif "connection:" in line_strip:
                # Only check connection state against known healthy peers
                parts = line_strip.split()
                peer_name = parts[0]
                conn_state = "unknown"
                for p in parts:
                    if p.startswith("connection:"):
                        conn_state = p.split(":", 1)[1]
                if peer_name in normal_hosts:
                    if conn_state not in ["Connected", "SyncSource", "SyncTarget", "PausedSyncSource", "PausedSyncTarget"]:
                        unhealthy = True
                        reasons.append(f"{current_resource} peer {peer_name} connection is {conn_state}")
            elif "peer-disk:" in line_strip:
                parts = line_strip.split()
                peer_disk = "unknown"
                for p in parts:
                    if p.startswith("peer-disk:"):
                        peer_disk = p.split(":", 1)[1]
                # Only flag peer disk issues if that peer is a known healthy node
                if not single_node and peer_disk in ["Inconsistent", "Outdated", "DUnknown"]:
                    unhealthy = True
                    reasons.append(f"{current_resource} peer disk state is {peer_disk}")
                    
        if not unhealthy:
            if single_node:
                log_upgrade(job_id, f"[{hostname}] DRBD local disk is healthy (single-node, no peer replication).")
            else:
                log_upgrade(job_id, f"[{hostname}] DRBD volume replication is healthy and fully synchronized.")
            return True
            
        if attempt % 5 == 0:
            log_upgrade(job_id, f"[{hostname}] Waiting for DRBD volume synchronization: {', '.join(reasons)}")
            
        time.sleep(5)
        
    log_upgrade(job_id, f"[{hostname}] Warning: DRBD volume synchronization checks timed out or failed.")
    return False

# Active set of jobs running on this host thread
running_jobs = set()

def hylia_rolling_upgrade(job_id):
    if job_id in running_jobs:
        return
    running_jobs.add(job_id)
    
    try:
        log_upgrade(job_id, "=== Initiating Hylia Rolling Upgrade Sequence ===")
        
        # 1. Fetch job data
        cql_job = f"SELECT JSON job_id, state, target_nodes, current_node, build_number, manifest_json, changelog_md FROM hydra.hylia_jobs WHERE job_id = {job_id};"
        rc_j, stdout_j, _ = run_cql_query(cql_job)
        if rc_j != 0 or not stdout_j:
            raise Exception("Failed to load upgrade job data from ScyllaDB.")
            
        job_data = None
        for line in stdout_j.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                job_data = json.loads(line)
                break
        if not job_data:
            raise Exception("Failed to parse upgrade job data from ScyllaDB.")
            
        target_nodes = job_data.get("target_nodes", [])
        current_node_state = job_data.get("current_node")
        build_number = job_data.get("build_number", "Unknown")
        manifest = json.loads(job_data.get("manifest_json", "{}"))
        components = manifest.get("components", {})
        reboot_required_components = {"spark", "gatoway", "urbosa", "Dockerfile"}
        needs_reboot = any(comp in reboot_required_components for comp in components)
        
        if needs_reboot:
            log_upgrade(job_id, "[Orchestrator] Reboot-required components detected. Upgrading via ROLLING REBOOT mode.")
        else:
            log_upgrade(job_id, "[Orchestrator] No reboot-required components detected. Upgrading via FAST PATCH mode.")
            
        log_upgrade(job_id, f"Target version build: {build_number}")
        log_upgrade(job_id, f"Target nodes to upgrade: {', '.join(target_nodes)}")
        
        # Transition state to UPGRADING
        run_cql_query(f"UPDATE hydra.hylia_jobs SET state = 'UPGRADING' WHERE job_id = {job_id};")
        
        # Skip completed hosts if resuming
        start_index = 0
        if current_node_state:
            if current_node_state in target_nodes:
                start_index = target_nodes.index(current_node_state)
                log_upgrade(job_id, f"Resuming rolling upgrade starting at node {current_node_state}...")
            else:
                log_upgrade(job_id, f"Resuming; current node {current_node_state} not in target list. Starting from scratch.")
        
        for idx in range(start_index, len(target_nodes)):
            node_ip = target_nodes[idx]
            
            # Update ScyllaDB with current node progress
            run_cql_query(f"UPDATE hydra.hylia_jobs SET current_node = '{node_ip}' WHERE job_id = {job_id};")
            
            # Query hostname of node_ip
            hostname = "Unknown"
            rc_h, stdout_h, _ = run_cql_query(f"SELECT JSON hostname FROM hydra.nodes WHERE ip = '{node_ip}' ALLOW FILTERING;")
            if rc_h == 0 and stdout_h:
                try:
                    for line in stdout_h.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            hostname = json.loads(line).get("hostname")
                            break
                except Exception:
                    pass
            
            log_upgrade(job_id, f"--- Starting Upgrade Phase for Host {hostname} ({node_ip}) ---")
            
            # If there are other nodes in the cluster, wait until they are all healthy (NORMAL status)
            # to avoid having multiple degraded hosts in the cluster simultaneously.
            if len(target_nodes) > 1:
                log_upgrade(job_id, f"[{hostname}] Checking status of other cluster nodes before upgrading...")
                other_nodes_stable = False
                for attempt in range(120): # wait up to 4 minutes
                    rc_n, stdout_n, _ = run_cql_query("SELECT JSON hostname, status FROM hydra.nodes;")
                    if rc_n == 0 and stdout_n:
                        try:
                            all_other_normal = True
                            unhealthy_nodes = []
                            for line in stdout_n.splitlines():
                                line = line.strip()
                                if line.startswith("{") and line.endswith("}"):
                                    node_info = json.loads(line)
                                    h_name = node_info.get("hostname")
                                    h_status = node_info.get("status", "NORMAL")
                                    if h_name != hostname and h_name != "Unknown":
                                        if h_status != "NORMAL":
                                            all_other_normal = False
                                            unhealthy_nodes.append(f"{h_name} ({h_status})")
                            if all_other_normal:
                                other_nodes_stable = True
                                break
                            else:
                                if attempt % 5 == 0:
                                    log_upgrade(job_id, f"[{hostname}] Waiting for other cluster nodes to recover to NORMAL: {', '.join(unhealthy_nodes)}")
                        except Exception as e:
                            pass
                    time.sleep(2)
                if not other_nodes_stable:
                    raise Exception(f"Other cluster nodes failed to reach NORMAL state before upgrading {hostname}.")
            
            # Check if we already rebooted this node for this job
            already_rebooted = False
            rc_r, _, _ = run_remote_spark(node_ip, f"test -f /var/lib/hylia/upgrade_rebooted_{job_id}")
            if rc_r == 0:
                already_rebooted = True
                log_upgrade(job_id, f"[{hostname}] Detected post-reboot resume. Skipping reboot step.")

            if not already_rebooted:
                if needs_reboot:
                    if len(target_nodes) == 1:
                        # Single-node: no live migration possible, no maintenance mode needed.
                        # The user is responsible for powering off VMs before triggering the upgrade.
                        log_upgrade(job_id, f"[{hostname}] Single-node cluster: skipping VM evacuation and maintenance mode.")
                    else:
                        # Check if node is already in maintenance mode in database
                        node_in_maint = False
                        cql_check = f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{hostname}';"
                        rc_c, stdout_c, _ = run_cql_query(cql_check)
                        if rc_c == 0 and stdout_c:
                            try:
                                for line in stdout_c.splitlines():
                                    line = line.strip()
                                    if line.startswith("{") and line.endswith("}"):
                                        n_status = json.loads(line).get("status")
                                        if n_status == "IN_MAINTENANCE":
                                            node_in_maint = True
                                            break
                            except Exception:
                                pass

                        if not node_in_maint:
                            # Step 1: Evacuate Host & Enter Maintenance
                            log_upgrade(job_id, f"[{hostname}] Evacuating host VMs and entering maintenance mode...")
                            payload_enter = {"hostname": hostname, "action": "enter", "force_stop": True}
                            rc_e, res_e, err_e = run_mtls_spark_api("127.0.0.1", "/api/v1/host/maintenance", payload_enter, method="POST")
                            if rc_e != 0 or "error" in res_e:
                                raise Exception(f"Failed to submit maintenance enter task to Vali: {res_e.get('error', err_e)}")
                                
                            maint_task_id = res_e.get("task_id")
                            log_upgrade(job_id, f"[{hostname}] Maintenance task submitted (Task ID: {maint_task_id}). Waiting for evacuation...")
                            
                            maint_success = False
                            for _ in range(150): # up to 5 minutes
                                cql_check = f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{hostname}';"
                                rc_c, stdout_c, _ = run_cql_query(cql_check)
                                if rc_c == 0 and stdout_c:
                                    try:
                                        for line in stdout_c.splitlines():
                                            line = line.strip()
                                            if line.startswith("{") and line.endswith("}"):
                                                n_status = json.loads(line).get("status")
                                                if n_status == "IN_MAINTENANCE":
                                                    maint_success = True
                                                    break
                                    except Exception:
                                        pass
                                    if maint_success:
                                        break
                                time.sleep(2)
                                
                            if not maint_success:
                                raise Exception(f"Host {hostname} failed to enter maintenance mode (evacuation timeout).")
                            log_upgrade(job_id, f"[{hostname}] Successfully evacuated and entered maintenance mode.")
                        else:
                            log_upgrade(job_id, f"[{hostname}] Host is already in maintenance mode. Skipping evacuation step.")
                
                # Step 2: Deploy Verified Files
                log_upgrade(job_id, f"[{hostname}] Deploying verified components...")
                extract_dir = "/tmp/helios_update"
                
                # Validate every manifest entry before touching the node, so a bad
                # package is rejected outright instead of half-deployed.
                for comp_name, comp_info in components.items():
                    validate_manifest_component(comp_name, comp_info)

                for comp_name, comp_info in components.items():
                    deploy_component(job_id, node_ip, hostname, comp_name, comp_info, extract_dir)

                if "spectrum" in components:
                    log_upgrade(job_id, f"[{hostname}] Rebuilding Spectrum container on host...")
                    build_cmd = (
                        "rm -rf /tmp/spectrum_build && mkdir -p /tmp/spectrum_build/static && "
                        "cp /usr/local/bin/spectrum_server /tmp/spectrum_build/spectrum_server.py && "
                        "cp /usr/local/bin/hylia /tmp/spectrum_build/hylia.py && "
                        "cp /usr/local/bin/Dockerfile /tmp/spectrum_build/Dockerfile && "
                        # lanayru is imported by spectrum_server at runtime; tolerate its
                        # absence on nodes provisioned before it shipped.
                        "if [ -f /usr/local/bin/lanayru.py ]; then cp /usr/local/bin/lanayru.py /tmp/spectrum_build/lanayru.py; fi && "
                        "cp -r /usr/local/bin/static/* /tmp/spectrum_build/static/ && "
                        "podman build -t localhost/spectrum:latest /tmp/spectrum_build && "
                        "systemctl stop spectrum && podman rm -f systemd-spectrum && systemctl start spectrum"
                    )
                    rc_b, out_b, err_b = run_remote_spark(node_ip, build_cmd)
                    if rc_b != 0:
                        log_upgrade(job_id, f"[{hostname}] Warning during Spectrum build: {err_b or out_b}")
                        
                log_upgrade(job_id, f"[{hostname}] All files successfully copied.")
                
                if needs_reboot:
                    # Pre-flight check: Verify that other nodes are healthy and fully synced before taking this node offline
                    log_upgrade(job_id, f"[{hostname}] Running pre-flight storage synchronization checks on remaining cluster nodes...")
                    preflight_ok = True
                    for other_node in target_nodes:
                        # target_nodes is a list of IP strings (see hydra.hylia_jobs);
                        # tolerate the dict form in case a job row predates that.
                        if isinstance(other_node, dict):
                            o_ip = other_node.get("ip")
                            o_host = other_node.get("hostname") or o_ip
                        else:
                            o_ip = other_node
                            o_host = get_hostname_by_ip(o_ip)
                        if o_ip and o_ip != node_ip:
                            log_upgrade(job_id, f"[{hostname}] Checking storage replica sync status on {o_host}...")
                            if not verify_node_storage_health(job_id, o_ip, o_host):
                                log_upgrade(job_id, f"[{hostname}] ERROR: Storage replica on node {o_host} is degraded or unsynchronized. Cannot reboot safely.")
                                preflight_ok = False
                                break
                    if not preflight_ok:
                        raise Exception("Pre-flight storage health check failed. Aborting rolling upgrade reboot to prevent quorum storage loss.")

                    # Write marker file before rebooting
                    run_remote_spark(node_ip, f"mkdir -p /var/lib/hylia && touch /var/lib/hylia/upgrade_rebooted_{job_id}")
                    
                    # Step 3: Reboot Host
                    log_upgrade(job_id, f"[{hostname}] Initiating host reboot sequence...")
                    run_remote_spark(node_ip, "reboot || true")
                    time.sleep(10)
                    
                    log_upgrade(job_id, f"[{hostname}] Waiting for node to go offline...")
                    for _ in range(60):
                        rc_p, _, _ = run_remote_spark(node_ip, "echo 1")
                        if rc_p != 0:
                            log_upgrade(job_id, f"[{hostname}] Node went offline.")
                            break
                        time.sleep(2)
                        
                    log_upgrade(job_id, f"[{hostname}] Waiting for node to come back online...")
                    online = False
                    for _ in range(120):
                        rc_p, _, _ = run_remote_spark(node_ip, "echo 1")
                        if rc_p == 0:
                            online = True
                            log_upgrade(job_id, f"[{hostname}] Node is back online.")
                            break
                        time.sleep(3)
                        
                    if not online:
                        raise Exception(f"Node {hostname} did not return online after reboot.")

            if needs_reboot:
                # Dynamic Wait for Services to Stabilize
                log_upgrade(job_id, f"[{hostname}] Waiting for host services to stabilize...")
                services_stable = False
                for attempt in range(60):
                    try:
                        rc_s, res_s, err_s = run_mtls_spark_api(node_ip, "/api/v1/node/status", None, method="GET")
                        if rc_s == 0:
                            services = res_s.get("services", {})
                            critical_services = ["ZooKeeper", "HydraDB", "Aether", "Spark"]
                            all_up = True
                            down_services = []
                            for svc in critical_services:
                                if svc in services:
                                    status = services[svc].get("status", "DOWN")
                                    if status != "UP":
                                        all_up = False
                                        down_services.append(svc)
                                else:
                                    all_up = False
                                    down_services.append(svc)
                            
                            if all_up:
                                log_upgrade(job_id, f"[{hostname}] All critical services are UP and stable.")
                                services_stable = True
                                break
                            else:
                                if attempt % 5 == 0:
                                    log_upgrade(job_id, f"[{hostname}] Waiting for critical services to start: {', '.join(down_services)}")
                        else:
                            if attempt % 5 == 0:
                                log_upgrade(job_id, f"[{hostname}] Waiting for spark-daemon to respond: {err_s}")
                    except Exception as e:
                        if attempt % 5 == 0:
                            log_upgrade(job_id, f"[{hostname}] Connection warning: {e}")
                    time.sleep(5)
                    
                if not services_stable:
                    raise Exception(f"Node {hostname} services failed to stabilize after reboot.")
                    
                # Dynamic Wait for DRBD Storage Sync
                if not verify_node_storage_health(job_id, node_ip, hostname):
                    raise Exception(f"Node {hostname} storage volumes failed to synchronize after reboot.")
                
                # Step 4: Leave Maintenance Mode (multi-node only)
                if len(target_nodes) > 1:
                    log_upgrade(job_id, f"[{hostname}] Restoring node from maintenance mode...")
                    payload_leave = {"hostname": hostname, "action": "leave"}
                    rc_l, res_l, err_l = run_mtls_spark_api("127.0.0.1", "/api/v1/host/maintenance", payload_leave, method="POST")
                    if rc_l != 0 or "error" in res_l:
                        raise Exception(f"Failed to submit maintenance leave task: {res_l.get('error', err_l)}")
                        
                    leave_success = False
                    for _ in range(60):
                        cql_check = f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{hostname}';"
                        rc_c, stdout_c, _ = run_cql_query(cql_check)
                        if rc_c == 0 and stdout_c:
                            try:
                                for line in stdout_c.splitlines():
                                    line = line.strip()
                                    if line.startswith("{") and line.endswith("}"):
                                        n_status = json.loads(line).get("status")
                                        if n_status == "NORMAL":
                                            leave_success = True
                                            break
                            except Exception:
                                pass
                            if leave_success:
                                break
                        time.sleep(2)
                        
                    if not leave_success:
                        raise Exception(f"Host {hostname} failed to leave maintenance mode.")
                else:
                    log_upgrade(job_id, f"[{hostname}] Single-node cluster: skipping maintenance leave.")

                # Cleanup the reboot marker file
                run_remote_spark(node_ip, f"rm -f /var/lib/hylia/upgrade_rebooted_{job_id}")
                
                log_upgrade(job_id, f"[{hostname}] Starting Hylia daemon service on upgraded host...")
                rc_st, stdout_st, stderr_st = run_remote_spark(node_ip, "systemctl start hylia")
                if rc_st != 0:
                    log_upgrade(job_id, f"[{hostname}] Warning: failed to start Hylia service: {stderr_st or stdout_st}")
                else:
                    log_upgrade(job_id, f"[{hostname}] Hylia daemon service started successfully.")
            else:
                # Fast Patch Mode Service Restarts (excluding hylia itself until job completion)
                log_upgrade(job_id, f"[{hostname}] Restarting service components for fast patch...")
                service_components = {
                    "zookeeper": "zookeeper", "hydra-db": "hydra-db", "aether": "aether", "spark": "spark", 
                    "spectrum": "spectrum", "bifrost": "bifrost", "dagur": "dagur", "mimir": "mimir", 
                    "vali": "vali", "catalyst": "catalyst", "gatoway": "gatoway", "logos": "logos", 
                    "mipha": "mipha", "daruk": "daruk", "agahnim": "agahnim", "slate": "slate", "urbosa": "urbosa"
                }
                for comp in components:
                    if comp in service_components and comp != "hylia":
                        svc_name = service_components[comp]
                        log_upgrade(job_id, f"[{hostname}] Restarting service '{svc_name}'...")
                        run_remote_spark(node_ip, f"systemctl restart {svc_name}")
            
            log_upgrade(job_id, f"[{hostname}] Upgraded successfully and returned to normal service.")
            
        # Upgrade completed successfully!
        log_upgrade(job_id, "=== Rolling Upgrade Completed Successfully on all Nodes ===")
        run_cql_query(f"UPDATE hydra.hylia_jobs SET state = 'COMPLETED' WHERE job_id = {job_id};")
        
        # If Hylia was updated, trigger a fire-and-forget restart on all nodes
        if "hylia" in components:
            log_upgrade(job_id, "[Orchestrator] Restarting Hylia upgrader daemons to apply version update...")
            for node_ip in target_nodes:
                rc_h, stdout_h, _ = run_cql_query(f"SELECT JSON hostname FROM hydra.nodes WHERE ip = '{node_ip}' ALLOW FILTERING;")
                node_hostname = "Unknown"
                if rc_h == 0 and stdout_h:
                    try:
                        for line in stdout_h.splitlines():
                            line = line.strip()
                            if line.startswith("{") and line.endswith("}"):
                                node_hostname = json.loads(line).get("hostname")
                                break
                    except Exception:
                        pass
                
                # Determine local node IP to avoid killing current thread until job completion is written
                import socket
                s_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    s_sock.connect(('10.255.255.255', 1))
                    local_ip = s_sock.getsockname()[0]
                except Exception:
                    local_ip = '127.0.0.1'
                finally:
                    s_sock.close()
                
                if node_ip == local_ip or node_ip in ("127.0.0.1", "::1"):
                    log_upgrade(job_id, f"[{node_hostname}] Scheduled local Hylia restart in background...")
                    import subprocess
                    subprocess.Popen("nohup sh -c 'sleep 2 && systemctl restart hylia' > /dev/null 2>&1 &", shell=True)
                else:
                    log_upgrade(job_id, f"[{node_hostname}] Restarting remote Hylia service...")
                    run_remote_spark(node_ip, "systemctl restart hylia")
                    
    except Exception as ex:
        log_upgrade(job_id, f"CRITICAL ERROR: Rolling Upgrade Failed: {ex}")
        run_cql_query(f"UPDATE hydra.hylia_jobs SET state = 'FAILED' WHERE job_id = {job_id};")
    finally:
        running_jobs.discard(job_id)

def hylia_loop():
    print("[Hylia] Daemon loop started.")
    while True:
        try:
            if is_zookeeper_leader():
                # Query upgrading jobs
                rc, stdout, _ = run_cql_query("SELECT JSON job_id, state FROM hydra.hylia_jobs;")
                if rc == 0 and stdout:
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            job = json.loads(line)
                            job_state = job.get("state")
                            job_id = job.get("job_id")
                            if job_state in ["STARTING", "UPGRADING"] and job_id not in running_jobs:
                                print(f"[Hylia] Found active job {job_id} in state {job_state}. Running rolling upgrade...")
                                threading.Thread(target=hylia_rolling_upgrade, args=(job_id,), daemon=True).start()
        except Exception as e:
            sys.stderr.write(f"[Hylia Loop Error] {e}\n")
        time.sleep(5)

if __name__ == "__main__":
    hylia_loop()
