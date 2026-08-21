#!/usr/bin/env python3
__build__ = "1.2.2"
import sys
import os
import json
import re
import time
import stat
import socket
import urllib.request
import ssl
import subprocess
import base64
import uuid
import threading

def run_command_local(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return res.returncode, res.stdout.decode('utf-8', errors='ignore').strip(), res.stderr.decode('utf-8', errors='ignore').strip()

def check_linstor_db_mount():
    rc, stdout, stderr = run_command_local("mountpoint -q /var/lib/linstor")
    return rc == 0

def check_linstor_controller_active():
    rc, stdout, stderr = run_command_local("systemctl is-active linstor-controller")
    return stdout == "active"

def get_local_drbd_role(resource_name):
    rc, stdout, stderr = run_command_local(f"drbdadm role {resource_name}")
    if rc == 0:
        return stdout.split("/", 1)[0].strip()
    return "Unknown"

import glob

def get_all_drbd_resources():
    resources = []
    try:
        for path in glob.glob("/etc/drbd.d/*.res"):
            name = os.path.basename(path).replace(".res", "")
            if name != "global_common" and name != "loop_device_mapping":
                resources.append(name)
    except Exception:
        pass
    return resources

def ensure_drbd_resource_up(resource_name):
    rc, stdout, stderr = run_command_local(f"drbdadm status {resource_name}")
    if rc != 0:
        print(f"[Mipha HA] DRBD resource {resource_name} is not loaded. Loading with drbdadm up...")
        run_command_local(f"drbdadm up {resource_name}")

def get_drbd_resource_state(resource_name):
    # Per-resource view (local role, devices and peer connections) taken from drbdsetup JSON
    rc, stdout, stderr = run_command_local("drbdsetup status --json")
    if rc != 0 or not stdout.strip():
        return None

    try:
        data = json.loads(stdout)
    except Exception:
        return None

    for resource in data:
        if resource.get("name") != resource_name:
            continue
        return {
            "role": resource.get("role", "Unknown"),
            "devices": resource.get("devices", []),
            "connections": resource.get("connections", [])
        }
    return None

def get_drbd_device_holders(resource_name, devices):
    # Returns a list of everything currently holding the DRBD device(s) of this resource open:
    # a mounted filesystem, a stacked block device, or a live process (qemu keeps the raw device
    # open for as long as the guest runs). A resource with holders must never discard its writes.
    holders = []
    rdev_map = {}

    for dev in devices:
        vol = dev.get("volume", 0)
        minor = dev.get("minor")
        candidates = []
        if minor is not None:
            candidates.append(f"/dev/drbd{minor}")
        candidates.append(f"/dev/drbd/by-res/{resource_name}/{vol}")
        for path in candidates:
            try:
                st = os.stat(path)
            except Exception:
                continue
            if stat.S_ISBLK(st.st_mode):
                rdev_map[st.st_rdev] = (candidates[0], minor)

    if not rdev_map:
        return holders

    # Mounted filesystems on top of the device
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 2 or not parts[0].startswith("/dev/"):
                    continue
                try:
                    st = os.stat(parts[0])
                except Exception:
                    continue
                if stat.S_ISBLK(st.st_mode) and st.st_rdev in rdev_map:
                    holders.append(f"{rdev_map[st.st_rdev][0]} is mounted at {parts[1]}")
    except Exception:
        pass

    # Stacked block devices (LVM/md/dm layered on top of the DRBD device)
    for dev_path, minor in rdev_map.values():
        if minor is None:
            continue
        try:
            for h in os.listdir(f"/sys/block/drbd{minor}/holders"):
                holders.append(f"{dev_path} is stacked under /dev/{h}")
        except Exception:
            pass

    # Processes with the device open (running guest, dd, rsync, ...)
    try:
        pids = os.listdir("/proc")
    except Exception:
        pids = []
    for pid in pids:
        if not pid.isdigit():
            continue
        try:
            fds = os.listdir(f"/proc/{pid}/fd")
        except Exception:
            continue
        for fd in fds:
            try:
                st = os.stat(f"/proc/{pid}/fd/{fd}")
            except Exception:
                continue
            if stat.S_ISBLK(st.st_mode) and st.st_rdev in rdev_map:
                pname = "unknown"
                try:
                    with open(f"/proc/{pid}/comm", "r") as f:
                        pname = f.read().strip()
                except Exception:
                    pass
                holders.append(f"{rdev_map[st.st_rdev][0]} is open by pid {pid} ({pname})")
                break

    return holders

def get_peer_drbd_role(resource_name, connections, probe_peers=False):
    # Peer role for THIS resource. DRBD reports peer-role Unknown while the connection is
    # StandAlone, so optionally fall back to asking the reachable cluster peers over spark-daemon.
    # Returns "Primary", "Secondary" or "Unknown" (Unknown means: do not auto-discard).
    roles = set()
    for conn in connections:
        peer_role = (conn.get("peer-role") or "").strip()
        if peer_role and peer_role != "Unknown":
            roles.add(peer_role)

    if len(roles) == 1:
        return roles.pop()
    if len(roles) > 1:
        return "Unknown"
    if not probe_peers:
        return "Unknown"

    answered = 0
    for h in get_cluster_hosts():
        ip = h.get("ip")
        if not ip or ip == LOCAL_IP:
            continue
        if not ping_host(ip):
            continue
        rc, stdout, stderr = run_remote_spark(ip, f"drbdadm role {resource_name}")
        if rc != 0 or not stdout.strip():
            # Peer is unreachable or does not host this resource
            continue
        answered += 1
        roles.add(stdout.strip().splitlines()[-1].split("/", 1)[0].strip())

    if answered == 0 or len(roles) != 1:
        return "Unknown"
    return roles.pop()

def resolve_drbd_standalone(resource_name):
    try:
        ensure_drbd_resource_up(resource_name)
        rc, stdout, stderr = run_command_local(f"drbdadm status {resource_name}")
        if rc != 0 or "StandAlone" not in stdout:
            return

        # The victim of a split-brain is a per-resource question: ZooKeeper leadership is a single
        # cluster-wide property and says nothing about which node holds the authoritative copy of
        # this resource, so it must never gate a --discard-my-data.
        state = get_drbd_resource_state(resource_name)
        if not state:
            print(f"[Mipha HA] WARNING: DRBD resource {resource_name} is StandAlone but drbdsetup returned no usable state for it. NOT discarding anything. Leaving {resource_name} StandAlone for manual resolution.")
            return

        role = state["role"]
        if role in ("", "Unknown"):
            role = get_local_drbd_role(resource_name)

        # A live holder (running guest, mounted filesystem, stacked device) means the local copy is
        # being served right now, so it must never be thrown away. Only a Primary can hold the device
        # open, and a Primary is never the victim, so the peer only has to be probed when we are not
        # Primary and could therefore end up discarding.
        holders = []
        if role != "Primary":
            holders = get_drbd_device_holders(resource_name, state["devices"])
            if holders:
                print(f"[Mipha HA] WARNING: DRBD resource {resource_name} is StandAlone with role={role} but its device still has a live holder ({'; '.join(holders)}). Refusing to touch the connection so nothing can discard local writes. Operator intervention required for {resource_name}.")
                return

        peer_role = get_peer_drbd_role(resource_name, state["connections"], probe_peers=(role != "Primary"))
        print(f"[Mipha HA] DRBD resource {resource_name} is in StandAlone state. Resolving (role={role}, peer role={peer_role})...")

        if role == "Secondary" and peer_role == "Primary":
            # Local is Secondary, nothing is using the device and the peer serves the resource,
            # so the local copy is the safe victim.
            print(f"[Mipha HA] Local node is Secondary on {resource_name} while the peer holds Primary. Discarding local writes to auto-heal...")
            run_command_local(f"drbdadm disconnect {resource_name}")
            rc_s, stdout_s, stderr_s = run_command_local(f"drbdadm secondary {resource_name}")
            if rc_s != 0:
                print(f"[Mipha HA] CRITICAL: Failed to demote {resource_name} to Secondary ({stderr_s or stdout_s}). Aborting discard. Operator intervention required for {resource_name}.")
                return
            run_command_local(f"drbdadm connect --discard-my-data {resource_name}")
            return

        if role == "Primary" and peer_role != "Primary":
            print(f"[Mipha HA] Local node holds Primary on {resource_name} (peer role={peer_role}). Keeping local writes and reconnecting without discard...")
        else:
            # Both Primary, both Secondary or peer role undeterminable: no safe victim can be picked
            # here. Reconnect without discarding anything and let the resource-definition split-brain
            # policy (after-sb-0pri/1pri/2pri) settle it, or stay StandAlone for the operator.
            print(f"[Mipha HA] WARNING: DRBD resource {resource_name} is StandAlone with local role={role} and peer role={peer_role}. No safe victim can be determined, so local writes are NOT discarded. Reconnecting without discard - operator intervention may be required for {resource_name}.")
        run_command_local(f"drbdadm disconnect {resource_name}")
        run_command_local(f"drbdadm connect {resource_name}")
    except Exception as e:
        sys.stderr.write(f"[Mipha HA] Error resolving DRBD standalone for {resource_name}: {e}\n")

DRBD_SYNC_TRACKER = {}

def check_and_resolve_stuck_resync():
    global DRBD_SYNC_TRACKER
    rc, stdout, stderr = run_command_local("drbdsetup status --json")
    if rc != 0 or not stdout.strip():
        return
        
    try:
        data = json.loads(stdout)
    except Exception:
        return
        
    current_time = time.time()
    current_keys = set()
    
    for resource in data:
        rname = resource.get("name")
        connections = resource.get("connections", [])
        for conn in connections:
            peer_name = conn.get("name")
            peer_devices = conn.get("peer_devices", [])
            for dev in peer_devices:
                vol = dev.get("volume", 0)
                repl_state = dev.get("replication-state", "")
                out_of_sync = dev.get("out-of-sync", 0)
                
                if repl_state in ("SyncTarget", "SyncSource") and out_of_sync > 0:
                    key = (rname, peer_name, vol)
                    current_keys.add(key)
                    
                    tracker = DRBD_SYNC_TRACKER.get(key)
                    if not tracker:
                        DRBD_SYNC_TRACKER[key] = {
                            "last_out_of_sync": out_of_sync,
                            "stalled_count": 0,
                            "last_check_time": current_time
                        }
                    else:
                        # Check every 30 seconds
                        if current_time - tracker["last_check_time"] >= 30:
                            if out_of_sync == tracker["last_out_of_sync"]:
                                tracker["stalled_count"] += 1
                                print(f"[Mipha HA] DRBD resource {rname} resync with {peer_name} is stalled at {out_of_sync} bytes. Stalled count = {tracker['stalled_count']}/3.")
                            else:
                                tracker["stalled_count"] = 0
                                tracker["last_out_of_sync"] = out_of_sync
                            tracker["last_check_time"] = current_time
                            
                            if tracker["stalled_count"] >= 3:
                                print(f"[Mipha HA] DRBD resource {rname} resync with {peer_name} is STUCK (no progress for 90s). Triggering self-heal disconnect/connect...")
                                run_command_local(f"drbdadm disconnect {rname}")
                                time.sleep(1)
                                run_command_local(f"drbdadm connect {rname}")
                                tracker["stalled_count"] = 0
                                tracker["last_check_time"] = current_time
                                
    # Clean up keys that are no longer syncing
    for k in list(DRBD_SYNC_TRACKER.keys()):
        if k not in current_keys:
            DRBD_SYNC_TRACKER.pop(k, None)

def linstor_ha_loop():
    print("[Mipha HA] Linstor Controller HA Thread started.")
    while True:
        try:
            is_leader = is_zookeeper_leader()
            for r in get_all_drbd_resources():
                resolve_drbd_standalone(r)
            
            check_and_resolve_stuck_resync()
            
            # Only manage database HA if the linstor-db resource definition exists on this node
            if os.path.exists("/etc/drbd.d/linstor-db.res"):
                if is_leader:
                    mounted = check_linstor_db_mount()
                    role = get_local_drbd_role("linstor-db")
                    
                    if not mounted or role != "Primary" or not check_linstor_controller_active():
                        print(f"[Mipha HA] Leader State: linstor-db role={role}, mounted={mounted}. Aligning to active...")
                        
                        # Stop the local controller first if we are about to mount, so we don't hold file handles on the root directory
                        if not mounted and check_linstor_controller_active():
                            print("[Mipha HA] Stopping local linstor-controller prior to mounting...")
                            run_command_local("systemctl stop linstor-controller")
                        
                        hosts = get_cluster_hosts()
                        for h in hosts:
                            ip = h.get("ip")
                            if ip and ip != LOCAL_IP:
                                if ping_host(ip):
                                    print(f"[Mipha HA] Coordinating with standby node {h['hostname']} ({ip}) to release linstor-db...")
                                    stop_cmd = (
                                        "if mountpoint -q /var/lib/linstor || [ \"$(drbdadm role linstor-db 2>/dev/null)\" = \"Primary\" ]; then "
                                        "systemctl stop linstor-controller || true; "
                                        "systemctl stop aether || true; "
                                        "umount -l /var/lib/linstor || true; "
                                        "drbdadm secondary linstor-db || true; "
                                        "systemctl start aether || true; "
                                        "else "
                                        "systemctl stop linstor-controller || true; "
                                        "fi"
                                    )
                                    run_remote_spark(ip, stop_cmd)
                                    
                        if role != "Primary":
                            print("[Mipha HA] Promoting linstor-db to Primary...")
                            rc_p, stdout_p, stderr_p = run_command_local("drbdadm primary linstor-db")
                            if rc_p != 0:
                                print(f"[Mipha HA] drbdadm primary failed ({stderr_p or stdout_p}). Attempting forced promotion to bypass unresponsive host locks...")
                                rc_p, stdout_p, stderr_p = run_command_local("drbdadm primary --force linstor-db")
                                if rc_p != 0:
                                    print(f"[Mipha HA] CRITICAL: Forced promotion of linstor-db failed: {stderr_p or stdout_p}")
                                
                        if not check_linstor_db_mount():
                            print("[Mipha HA] Mounting linstor-db volume at /var/lib/linstor...")
                            run_command_local("mkdir -p /var/lib/linstor")
                            rc_m, stdout_m, stderr_m = run_command_local("mount -t xfs /dev/drbd/by-res/linstor-db/0 /var/lib/linstor")
                            if rc_m != 0:
                                print(f"[Mipha HA] ERROR: Failed to mount linstor-db volume: {stderr_m or stdout_m}")
                                
                        if check_linstor_db_mount():
                            if not check_linstor_controller_active():
                                print("[Mipha HA] Starting linstor-controller service...")
                                run_command_local("systemctl start linstor-controller")
                        else:
                            print("[Mipha HA] Refusing to start linstor-controller because mount failed.")
                else:
                    if check_linstor_controller_active():
                        print("[Mipha HA] Follower State: Stopping linstor-controller...")
                        run_command_local("systemctl stop linstor-controller")
                        
                    role = get_local_drbd_role("linstor-db")
                    if check_linstor_db_mount() or role == "Primary":
                        print("[Mipha HA] Follower State: Unmounting /var/lib/linstor and demoting to Secondary...")
                        run_command_local("systemctl stop aether || true")
                        run_command_local("umount -l /var/lib/linstor || true")
                        run_command_local("drbdadm secondary linstor-db || true")
                        run_command_local("systemctl start aether || true")

            # Align default storage containers (default-vm-container and default-image-container)
            for container in ["default-vm-container", "default-image-container"]:
                if os.path.exists(f"/etc/drbd.d/{container}.res"):
                    mount_path = f"/var/lib/hci/aether/volumes/{container}"
                    if is_leader:
                        os.makedirs(mount_path, exist_ok=True)
                        rc_m, _, _ = run_command_local(f"mountpoint -q {mount_path}")
                        mounted = (rc_m == 0)
                        role = get_local_drbd_role(container)
                        
                        if not mounted or role != "Primary":
                            print(f"[Mipha HA] Leader State: {container} role={role}, mounted={mounted}. Aligning to active...")
                            
                            # Release on peer standby nodes
                            hosts = get_cluster_hosts()
                            for h in hosts:
                                ip = h.get("ip")
                                if ip and ip != LOCAL_IP:
                                    if ping_host(ip):
                                        stop_cmd = f"umount -l {mount_path} || true; drbdadm secondary {container} || true"
                                        run_remote_spark(ip, stop_cmd)
                                        
                            if role != "Primary":
                                run_command_local(f"drbdadm primary {container}")
                            
                            rc_m, _, _ = run_command_local(f"mountpoint -q {mount_path}")
                            if rc_m != 0:
                                run_command_local(f"mount -t xfs /dev/drbd/by-res/{container}/0 {mount_path}")
                    else:
                        rc_m, _, _ = run_command_local(f"mountpoint -q {mount_path}")
                        if rc_m == 0:
                            print(f"[Mipha HA] Follower State: Unmounting {container}...")
                            run_command_local(f"umount -l {mount_path} || true")
                        role = get_local_drbd_role(container)
                        if role == "Primary":
                            print(f"[Mipha HA] Follower State: Demoting {container} to Secondary...")
                            run_command_local(f"drbdadm secondary {container} || true")
                        
        except Exception as ex:
            sys.stderr.write(f"[Mipha HA] Error in Linstor HA loop: {ex}\n")
            
        time.sleep(2)


LOCAL_IP = "127.0.0.1"

# Load local environment settings if available
try:
    with open("/etc/hci/spectrum/spectrum.env", "r") as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                if k == "LOCAL_HYPERVISOR_IP":
                    LOCAL_IP = v
except Exception:
    pass

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

def spark_mtls_context(verify_identity):
    """Build the mTLS context Mipha dials peers with, or None if no keypair is usable.

    None rather than ssl._create_unverified_context(): Mipha fences hosts and promotes
    the LINSTOR controller off what these calls report, and an unverified context turned
    a missing keypair into a connection that trusts whatever answers on 9099. Failing the
    call is the safe reading of "this node has no identity".
    """
    cert_paths = [
        ("/etc/hci/spark/certs/ca.crt", "/etc/hci/spark/certs/node.crt", "/etc/hci/spark/certs/node.key"),
        ("/root/.certs/ca.crt", "/root/.certs/client.crt", "/root/.certs/client.key")
    ]
    for ca, cert, key in cert_paths:
        if os.path.exists(ca) and os.path.exists(cert) and os.path.exists(key):
            try:
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
                context.load_cert_chain(certfile=cert, keyfile=key)
                context.check_hostname = verify_identity
                return context
            except Exception:
                pass
    return None

def run_remote_spark(ip, command):
    ip, verify_identity = spark_endpoint(ip)
    context = spark_mtls_context(verify_identity)
    if not context:
        return -1, "", "no usable mTLS keypair in /etc/hci/spark/certs or /root/.certs"

    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=15) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)

def run_mtls_spark_api(ip, path, payload, method="POST"):
    ip, verify_identity = spark_endpoint(ip)
    context = spark_mtls_context(verify_identity)
    if not context:
        return -1, {}, "no usable mTLS keypair in /etc/hci/spark/certs or /root/.certs"

    url = f"https://{ip}:9099{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=15) as response:
            res = json.loads(response.read().decode("utf-8"))
            return 0, res, ""
    except Exception as e:
        return -1, {}, str(e)

class ConditionalStatementError(RuntimeError):
    """A compare-and-swap was handed to the query path, which cannot report one."""


def _cql_outside_string_literals(cql_query):
    """The statement with every single-quoted literal blanked out.

    Task error messages, DRBD command output and operator-supplied text all end up inside
    CQL literals here, and any of them can contain the word "if". Searching the raw text
    for the keyword would refuse an ordinary INSERT because a resync failure happened to
    print "check if the peer is reachable". A doubled quote ('') is an escaped quote
    inside a literal, not the end of one.
    """
    out = []
    index = 0
    length = len(cql_query)
    while index < length:
        char = cql_query[index]
        if char != "'":
            out.append(char)
            index += 1
            continue
        index += 1
        while index < length:
            if cql_query[index] == "'":
                if index + 1 < length and cql_query[index + 1] == "'":
                    index += 2
                    continue
                index += 1
                break
            index += 1
        out.append("''")
    return "".join(out)


# A mutating statement whose text carries an IF clause. DDL is excluded on purpose:
# "CREATE TABLE IF NOT EXISTS" is not a compare-and-swap and its result carries nothing a
# caller needs.
_CONDITIONAL_CQL = re.compile(r"\s*(?:insert|update|delete|begin)\b.*\bif\b", re.I | re.S)


def is_conditional_cql(cql_query):
    """True when the statement is a lightweight transaction rather than a plain write."""
    return bool(_CONDITIONAL_CQL.match(_cql_outside_string_literals(cql_query or "")))


def run_cql_query(cql_query, *args, **kwargs):
    """Run a statement whose only interesting outcome is "did it execute".

    Conditional statements are refused rather than run. Daruk's /query endpoint renders a
    *rejected* lightweight transaction as its row of values joined by spaces --

        False 10.10.102.41

    -- and returns rc=0, which is indistinguishable from a successful write, so every
    caller that used this function for a compare-and-swap was treating lost races as wins.
    The refusal is here rather than in a review comment because the bug comes back the
    moment somebody appends "IF ..." to an existing call and the tests still pass.

    Conditional writes belong on one of Daruk's typed /v1/... endpoints; see run_lwt().
    """
    if is_conditional_cql(cql_query):
        raise ConditionalStatementError(
            "a conditional statement cannot be run through run_cql_query(): its result "
            "cannot say whether the condition held. Use a Daruk /v1/... endpoint via "
            f"run_lwt(). Statement: {' '.join(cql_query.split())[:200]}")
    import urllib.request
    import json
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
        b64_query = base64.b64encode(cql_query.encode('utf-8')).decode('utf-8')
        local_ip = "127.0.0.1"
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('10.255.255.255', 1))
            local_ip = s.getsockname()[0]
            s.close()
        except Exception:
            pass
        cmd = f'echo {b64_query} | base64 -d | podman exec -i systemd-hydra-db cqlsh {local_ip}'
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        return p.returncode, stdout.decode('utf-8', errors='ignore').strip(), stderr.decode('utf-8', errors='ignore').strip()
def is_zookeeper_leader(ip="127.0.0.1"):
    if ip == "127.0.0.1" or ip == LOCAL_IP:
        return get_zookeeper_leader_ip() == LOCAL_IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        s.connect((ip, 2181))
        s.sendall(b"stat")
        resp = s.recv(1024).decode('utf-8', errors='ignore')
        s.close()
        return "mode: leader" in resp.lower() or "mode: standalone" in resp.lower()
    except Exception:
        return False

def get_zookeeper_leader_ip(hosts=None):
    if not hosts:
        hosts = get_cluster_hosts()
    if not hosts:
        ips = [LOCAL_IP]
    else:
        ips = [h.get("ip") for h in hosts if h.get("ip")]
        
    leader_ip = None
    for ip in ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((ip, 2181))
            s.sendall(b"stat")
            resp = s.recv(1024).decode('utf-8', errors='ignore')
            s.close()
            if "mode: leader" in resp.lower() or "mode: standalone" in resp.lower():
                leader_ip = ip
                break
        except Exception:
            pass
            
    # Check if leader is active on port 9091
    leader_active = False
    if leader_ip:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((leader_ip, 9091))
            s.close()
            leader_active = True
        except Exception:
            leader_active = False
            
    if leader_active:
        return leader_ip
        
    # If leader is inactive, find active candidates with port 9091 open
    candidates = []
    for ip in ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((ip, 9091))
            s.close()
            candidates.append(ip)
        except Exception:
            pass
            
    if not candidates:
        return leader_ip if leader_ip else "127.0.0.1"
        
    candidates.sort()
    return candidates[0]

def get_cluster_hosts():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            return cdata.get("hosts", [])
    except Exception:
        return []

def ping_host(ip):
    # Runs standard Linux ping command, sending 1 packet with 2 second timeout
    try:
        p = subprocess.Popen(f"ping -c 1 -W 2 {ip}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p.communicate()
        return p.returncode == 0
    except Exception:
        return False

def check_vali_health(ip):
    # Vali requires mutual TLS, so even a health probe presents a certificate. A probe
    # that could not authenticate would report every healthy host as down, and Mipha
    # fences on that signal.
    address, verify_identity = spark_endpoint(ip)
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                     cafile="/etc/hci/spark/certs/ca.crt")
    ctx.load_cert_chain(certfile="/etc/hci/spark/certs/node.crt",
                        keyfile="/etc/hci/spark/certs/node.key")
    ctx.check_hostname = verify_identity
    url = f"https://{address}:9095/api/v1/hosts"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, context=ctx, timeout=5) as response:
            return response.status == 200
    except Exception:
        return False

def get_dfs_engine():
    return "linstor"

def get_linstor_pending_sync():
    hosts = get_cluster_hosts()
    ips = [h["ip"] for h in hosts] if hosts else ["127.0.0.1"]
    controllers_str = ",".join(ips)
    cmd = f"podman exec -e LS_CONTROLLERS={controllers_str} systemd-aether linstor volume list"
    candidate_ips = ["127.0.0.1"] + ips
    rc = -1
    stdout = ""
    for ip in candidate_ips:
        rc, stdout, stderr = run_remote_spark(ip, cmd)
        if rc == 0:
            break
    if rc != 0:
        return -1
    if "Syncing" in stdout or "PausedSync" in stdout or "Inconsistent" in stdout:
        return 1
    return 0


def submit_catalyst_task(leader_ip, service, action, payload):
    # Catalyst requires a cluster-signed certificate: it dispatches VM lifecycle
    # work and used to accept it from anything that could open a socket to 9091.
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                     cafile="/etc/hci/spark/certs/ca.crt")
    ctx.load_cert_chain(certfile="/etc/hci/spark/certs/node.crt",
                        keyfile="/etc/hci/spark/certs/node.key")
    url = f"https://{leader_ip}:9091/api/v1/tasks/submit"
    data = json.dumps({
        "service": service,
        "action": action,
        "payload": payload
    }).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            if response.status == 200:
                res_data = json.loads(response.read().decode("utf-8"))
                return res_data.get("task_id")
    except Exception:
        pass
    return None

def ssh_fence_host(ip):
    print(f"[Mipha HA] Initiating Spark-based fence for host {ip}...")
    fence_cmd = "systemctl stop libvirtd virtqemud || true; pkill -9 qemu-system-x86_64 || true; pkill -9 qemu || true"
    
    # Try fencing via mTLS Spark Daemon
    rc, stdout, stderr = run_remote_spark(ip, fence_cmd)
    if rc == 0:
        print(f"[Mipha HA] Fenced host {ip} using Spark Daemon")
        return True
        
    print(f"[Mipha HA] Spark Fencing failed for host {ip}: {stderr}")
    return False

# ---------------------------------------------------------------------------
# Ring lifecycle.
#
# Two jobs live here, and only one of them acts.
#
# The first is keeping the cluster maintenance lock alive. Vali takes it when a host
# starts draining and gives it back when that host has finished rejoining, which can be
# hours apart -- far longer than any TTL short enough to be useful when the holder dies.
# Mipha already runs a ten second control loop on the leader and already knows which host
# is in maintenance, so it renews the lock for as long as hydra.nodes says the host is
# still transitioning. That is what lets the TTL stay at five minutes.
#
# The second is reporting, not acting. When a node is gone for good its ScyllaDB is still
# a ring member holding token ranges that nobody serves, and every QUORUM operation keeps
# counting it. Nutanix detaches such a node from the ring automatically; Helios does not,
# and deliberately: `nodetool removenode` and `nodetool decommission` stream data, run
# unbounded, cannot be undone, and are wrong to trigger from a health check that has been
# failing for thirty seconds -- a network partition looks exactly the same from here. So
# the ring state is surfaced with the command that would fix it, and a human runs it.
# See docs/ring_lifecycle.md.

MAINTENANCE_LOCK_NAME = "cluster-maintenance"
MAINTENANCE_LOCK_TTL_SECONDS = 300

# hydra.nodes states in which a host still holds the maintenance lock. RECOVERING is
# included because a host that has left maintenance but has not finished resyncing its
# storage is not yet a replica anyone should count on, and no second host should start
# draining until it is.
MAINTENANCE_LOCK_STATES = ("ENTERING_MAINTENANCE", "IN_MAINTENANCE", "RECOVERING")

DARUK_URL = "http://127.0.0.1:9043"


def run_lwt(endpoint, params, timeout=15):
    """Call one of Daruk's typed compare-and-swap endpoints.

    Returns `(ok, applied, current, error)`. A refused compare-and-swap is
    `(True, False, {...}, "")` -- a lost race, not a failure. Collapsing the two would
    make a lock this node does not hold look like an error, and, worse, make an error
    look like a lock it does hold.

    Deliberately no cqlsh fallback: that path can run statement text but cannot report
    whether a condition held, and a lock renewal that cannot be made conditional must not
    be made at all.
    """
    import urllib.error
    try:
        req = urllib.request.Request(
            f"{DARUK_URL}{endpoint}",
            data=json.dumps(params).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            res = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return False, False, {}, json.loads(e.read().decode("utf-8")).get("error", f"HTTP {e.code}")
        except Exception:
            return False, False, {}, f"HTTP {e.code}"
    except Exception as e:
        return False, False, {}, f"Daruk is not answering on {DARUK_URL}: {e}"
    if res.get("status") != "success":
        return False, False, {}, res.get("error", "compare-and-swap failed")
    return True, bool(res.get("applied")), res.get("current") or {}, ""


def read_maintenance_lock():
    """The cluster maintenance lock row as it stands, or {} if nobody holds it."""
    rc, stdout, _ = run_cql_query(
        "SELECT JSON name, holder, holder_token, reason, acquired_at_ms "
        f"FROM hydra.cluster_locks WHERE name = '{MAINTENANCE_LOCK_NAME}';")
    if rc != 0 or not stdout:
        return {}
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except Exception:
                pass
    return {}


def renew_maintenance_lock_for(hostname):
    """Extend the lock's TTL, but only if `hostname` is the holder of record.

    The token is read rather than remembered: this process did not acquire the lock and
    may not even be the process that will release it. Reading it and then renewing
    conditionally on it is still exact -- if the lock changed hands between the read and
    the renew, the token no longer matches and the renew is refused rather than stealing
    somebody else's lock and extending it in this host's name.
    """
    lock = read_maintenance_lock()
    if not lock or lock.get("holder") != hostname:
        return False
    token = lock.get("holder_token") or ""
    if not token:
        return False
    ok, applied, _current, error = run_lwt("/v1/lock/renew", {
        "name": MAINTENANCE_LOCK_NAME,
        "holder": hostname,
        "holder_token": token,
        "reason": lock.get("reason") or f"{hostname} is in maintenance",
        "acquired_at_ms": int(time.time() * 1000),
        "ttl_seconds": MAINTENANCE_LOCK_TTL_SECONDS,
    })
    if not ok:
        sys.stderr.write(
            f"[Mipha HA] Could not renew the cluster maintenance lock for {hostname}: {error}\n")
    return applied


def release_orphaned_vm(vm_name, dead_host_ip):
    """Unplace a VM stranded on a host that died, conditional on it still being there.

    Returns True only when this failover now owns the recovery of `vm_name`, so a caller
    that gets False must not go on to start it.

    SSH fencing and three consecutive failed health checks make a live host here unlikely,
    not impossible -- and the write it guards was unconditional. The VM list was read
    seconds before this runs, and in that window the guest can have been recovered
    elsewhere: by a previous failover pass whose start task only just landed, by an
    operator, or by a Vali start that was already in flight. A blind
    `SET state='Stopped', host_ip=''` then unplaces a *running* VM, and the start task
    that follows boots a second copy of it against the same DRBD device -- two qemu
    processes on one raw device, which is the corruption failover exists to prevent.

    `IF host_ip = '<dead ip>'` scopes the reset to the host that actually died. A refusal
    is not an error: it means the VM is somewhere else and needs nothing from us.
    """
    ok, applied, current, error = run_lwt("/v1/vm/release", {
        "name": vm_name,
        "expected_host_ip": dead_host_ip,
    })
    if not ok:
        # Daruk is unreachable or the write failed. Do not fall back to an unconditional
        # reset and do not start the VM: we no longer know who owns it, and guessing is
        # how both sides of a partition come to own the same guest. The loop retries in
        # ten seconds; a VM that stays down for one more pass is recoverable, a VM started
        # twice is not.
        print(f"[Mipha HA] ERROR: Could not release '{vm_name}' from {dead_host_ip}: "
              f"{error}. Leaving it placed and skipping it this pass.")
        return False
    if not applied:
        print(f"[Mipha HA] VM '{vm_name}' is no longer on the dead host "
              f"(host_ip is now '{current.get('host_ip')}'). Already recovered elsewhere; "
              f"leaving it alone.")
        return False
    return True


_HOST_ID_RE = re.compile(
    r"\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\Z")


def parse_nodetool_status(text):
    """Ring members from `nodetool status`, as {address, status, state, host_id}.

    The first column is two characters: U/D for up or down, then N/L/J/M for normal,
    leaving, joining or moving. A member is only a replica that can answer a query when
    it is both -- `UJ` has not finished streaming in and `UL` is streaming out.

    The host id is matched by shape, not by column index: `Load` occupies two fields
    ("2.38 MB") or one ("?"), so the columns after it shift from node to node -- and the
    host id is the argument `nodetool removenode` takes.
    """
    members = []
    for line in (text or "").splitlines():
        fields = line.split()
        if len(fields) < 2:
            continue
        marker = fields[0]
        if len(marker) != 2 or marker[0] not in "UD" or marker[1] not in "NLJM":
            continue
        members.append({
            "address": fields[1],
            "status": marker[0],
            "state": marker[1],
            "available": marker == "UN",
            "host_id": next((f for f in fields[2:] if _HOST_ID_RE.match(f)), ""),
        })
    return members


def report_ring_detach_candidate(hostname, ip):
    """Say whether a host that just failed over is still counted as a ScyllaDB replica.

    A failed-over host is out of the VM scheduler immediately, because hydra.nodes says
    DOWN. Its ScyllaDB is not out of anything: the ring still assigns it token ranges and
    every QUORUM operation still counts it toward the replicas it needs. On a three node
    RF=3 cluster that is the difference between "one node is down" and "the next node to
    go down takes the cluster with it" -- which is exactly the state the maintenance
    quorum gate will refuse to add to, without ever explaining why.

    This does not detach anything. See the note at the top of this section.
    """
    rc, stdout, stderr = run_remote_spark(LOCAL_IP, "nodetool status")
    if rc != 0:
        sys.stderr.write(f"[Mipha HA] Could not read the ScyllaDB ring: "
                         f"{(stderr or stdout or '').strip()[:200]}\n")
        return
    members = parse_nodetool_status(stdout)
    target = next((m for m in members if m["address"] == ip), None)
    if target is None:
        print(f"[Mipha HA] Host {hostname} ({ip}) is not a ScyllaDB ring member; "
              "nothing to detach.")
        return
    up = sum(1 for m in members if m["available"])
    print(f"[Mipha HA] RING: {hostname} ({ip}) is still a ring member, reported "
          f"'{target['status']}{target['state']}'. {up} of {len(members)} members are up.")
    print(f"[Mipha HA] RING: the ring still assigns it token ranges, so QUORUM keeps "
          f"counting it and maintenance on any other host will be refused while it is down.")
    print(f"[Mipha HA] RING: if this host is coming back, do nothing. If it is gone for "
          f"good, detach it deliberately -- 'cluster decommission -s {ip}' explains the "
          f"sequence. Automatic detach is not done here: from a health check a dead node "
          f"and a partitioned one look identical, and removenode cannot be undone.")

# ---------------------------------------------------------------------------
# Scheduled storage auto-heal.
#
# Invoked by the Dagur cron entry as `mipha --auto-heal`. This is deliberately a
# subcommand on the existing daemon rather than a separate service: the DRBD healing
# logic already lives here, and a second owner of DRBD state would race the HA loop.
# It also avoids a fifth place for a component to drift out of (provision embedding,
# sync_provision mapping, upgrade package, LCM inventory, deploy_updates).
#
# What belongs here is the slow work that must not run in a liveness loop: verify
# scrubs, capacity reporting, and detecting under-replicated resources.
LINSTOR_EXEC = "podman exec systemd-aether linstor"


def _heal_log(msg):
    print(f"[AutoHeal] {msg}", flush=True)


def auto_heal_drbd_verify():
    """Run an online verify pass over every DRBD resource.

    `drbdadm verify` checksums the peers against each other and marks any differing
    blocks out-of-sync so the next resync repairs them. It is read-only with respect to
    application data, but it is I/O heavy -- which is exactly why it belongs in a nightly
    job rather than in Mipha's 10-second control loop.
    """
    resources = get_all_drbd_resources()
    if not resources:
        _heal_log("No DRBD resources configured; skipping verify pass.")
        return 0
    failures = 0
    for res in resources:
        rc, out, err = run_command_local(f"drbdadm status {res}")
        if rc != 0:
            _heal_log(f"{res}: not loaded, skipping verify.")
            continue
        if "Connected" not in out and "UpToDate" not in out:
            _heal_log(f"{res}: not in a connected/UpToDate state, skipping verify to avoid noise.")
            continue
        rc_v, out_v, err_v = run_command_local(f"drbdadm verify {res}")
        if rc_v == 0:
            _heal_log(f"{res}: verify started.")
        else:
            detail = (err_v or out_v).strip()
            # A single-node resource has no peer to verify against; that is not a fault.
            if "peer" in detail.lower() or "no connection" in detail.lower():
                _heal_log(f"{res}: no peer to verify against (single-node resource).")
            else:
                _heal_log(f"{res}: verify FAILED: {detail}")
                failures += 1
    return failures


def auto_heal_report_capacity():
    """Report Linstor storage-pool usage, flagging thin-pool overcommit."""
    rc, out, err = run_command_local(f"{LINSTOR_EXEC} --machine-readable storage-pool list")
    if rc != 0:
        _heal_log(f"Could not read storage pools: {(err or out).strip()[:200]}")
        return 0
    warnings = 0
    try:
        data = json.loads(out)
        rows = data[0] if isinstance(data, list) and data and isinstance(data[0], list) else data
        for entry in rows if isinstance(rows, list) else []:
            name = entry.get("storage_pool_name", "?")
            node = entry.get("node_name", "?")
            # Diskless pools carry no real capacity -- Linstor reports INT64_MAX for them,
            # which would otherwise render as a meaningless 0.0% used line.
            if entry.get("provider_kind") == "DISKLESS":
                continue
            free = entry.get("free_capacity")
            total = entry.get("total_capacity")
            if not total or total >= 2 ** 62:
                continue
            used_pct = 100.0 * (total - (free or 0)) / total
            total_gib = total / (1024.0 * 1024.0)
            level = "WARNING" if used_pct >= 85 else "ok"
            _heal_log(f"pool {name} on {node}: {used_pct:.1f}% used of {total_gib:.0f} GiB ({level})")
            if used_pct >= 85:
                warnings += 1
            # Thin pools overcommit: allocated volumes can exceed the backing pool, so
            # report the metadata pressure Linstor tracks separately.
            meta = (entry.get("props") or {}).get("StorDriver/internal/lvmthin/thinPoolMetadataPercent")
            if meta:
                try:
                    if float(meta) >= 80.0:
                        _heal_log(f"pool {name} on {node}: thin-pool metadata {float(meta):.1f}% used -- WARNING")
                        warnings += 1
                except ValueError:
                    pass
    except Exception as exc:
        _heal_log(f"Could not parse storage-pool output: {exc}")
    return warnings


def auto_heal_check_replicas():
    """Flag resources with fewer replicas than the cluster's redundancy factor."""
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cfg = json.load(f)
        rf = int(cfg.get("redundancy_factor", 0))
        node_count = len(cfg.get("hosts", []))
    except Exception as exc:
        _heal_log(f"Could not read cluster.json: {exc}")
        return 0
    # FTT 0 on a single node means one copy is the intended state.
    expected = rf + 1
    if expected <= 1 or node_count <= 1:
        _heal_log(f"Redundancy factor {rf} on {node_count} node(s): single-copy is expected, skipping replica check.")
        return 0
    rc, out, err = run_command_local(f"{LINSTOR_EXEC} --machine-readable resource list")
    if rc != 0:
        _heal_log(f"Could not read resource list: {(err or out).strip()[:200]}")
        return 0
    warnings = 0
    try:
        data = json.loads(out)
        rows = data[0] if isinstance(data, list) and data and isinstance(data[0], list) else data
        counts = {}
        for entry in rows if isinstance(rows, list) else []:
            counts[entry.get("name", "?")] = counts.get(entry.get("name", "?"), 0) + 1
        for name, have in sorted(counts.items()):
            if have < expected:
                _heal_log(f"resource {name}: {have} replica(s), expected {expected} -- UNDER-REPLICATED")
                warnings += 1
    except Exception as exc:
        _heal_log(f"Could not parse resource list: {exc}")
    return warnings


def run_auto_heal():
    """Entry point for `mipha --auto-heal`. Exit code 0 = clean, 1 = attention needed."""
    _heal_log("Starting scheduled storage auto-heal pass.")
    issues = 0
    for step, fn in (("DRBD verify", auto_heal_drbd_verify),
                     ("capacity report", auto_heal_report_capacity),
                     ("replica check", auto_heal_check_replicas)):
        try:
            issues += fn()
        except Exception as exc:
            _heal_log(f"{step} raised: {exc}")
            issues += 1
    if issues:
        _heal_log(f"Completed with {issues} item(s) needing attention.")
        return 1
    _heal_log("Completed cleanly.")
    return 0


def main():
    print("Mipha High-Availability Host Monitor and VM Failover Coordinator started.")
    
    # Start the Linstor HA thread in the background
    t = threading.Thread(target=linstor_ha_loop, daemon=True)
    t.start()
    
    # Track consecutive failures per host IP
    consecutive_failures = {}
    
    while True:
        try:
            # 1. Leadership Check
            if not is_zookeeper_leader("127.0.0.1"):
                # I am a follower, reset trackers and idle
                consecutive_failures.clear()
                time.sleep(10)
                continue
                
            hosts = get_cluster_hosts()
            if not hosts:
                time.sleep(10)
                continue
                
            # Filter out local host from checking itself
            target_hosts = [h for h in hosts if h.get("ip") != LOCAL_IP]
            
            for h in target_hosts:
                ip = h.get("ip")
                hostname = h.get("hostname")
                if not ip:
                    continue
                    
                # Query node's current status in ScyllaDB
                cql_status = f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{hostname}';"
                rc_s, stdout_s, _ = run_cql_query(cql_status)
                db_status = "NORMAL"
                if rc_s == 0 and stdout_s:
                    for line in stdout_s.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                node_obj = json.loads(line)
                                db_status = node_obj.get("status", "NORMAL")
                            except:
                                pass

                # Keep the cluster maintenance lock alive for whichever host is mid
                # transition. Vali takes it and gives it back, but the window between
                # those two events is an operator's maintenance window, not a task's
                # runtime, so nothing inside Vali is still running to renew it. Without
                # this the lock's TTL would have to cover the longest maintenance anyone
                # ever performs, which is the same TTL that leaves a dead holder's lock
                # standing for hours.
                if db_status in MAINTENANCE_LOCK_STATES:
                    renew_maintenance_lock_for(hostname)

                if db_status in ["IN_MAINTENANCE", "ENTERING_MAINTENANCE"]:
                    consecutive_failures[ip] = 0
                    continue

                # 2. Run Health checks on host
                ping_ok = ping_host(ip)
                spark_ok = False
                
                # Check Spark Daemon API
                rc, res, _ = run_mtls_spark_api(ip, "/api/v1/node/status", None, method="GET")
                if rc == 0 and res.get("ip") == ip:
                    # Verify if host is in maintenance mode
                    if res.get("maintenance_status") == "IN_MAINTENANCE":
                        # If in maintenance, skip monitoring
                        consecutive_failures[ip] = 0
                        continue
                    spark_ok = True
                
                # Host is down if spark-daemon is unresponsive
                if not spark_ok:
                    consecutive_failures[ip] = consecutive_failures.get(ip, 0) + 1
                    print(f"[Mipha HA] Host {hostname} ({ip}) health check failed (Count: {consecutive_failures[ip]}/3)")
                else:
                    consecutive_failures[ip] = 0
                    
                    # If host was previously marked DOWN, initiate rejoin/sync sequence
                    if db_status == "DOWN":
                        print(f"[Mipha HA] Host {hostname} ({ip}) is back online! Starting rejoining and Linstor/DRBD sync sequence...")
                        
                        # A1. Set host status to RECOVERING
                        cql_recovering = f"UPDATE hydra.nodes SET status = 'RECOVERING' WHERE hostname = '{hostname}';"
                        run_cql_query(cql_recovering)
                        
                        # A2. Create parent join task in Catalyst
                        parent_task_id = str(uuid.uuid4())
                        now_ms = int(time.time() * 1000)
                        parent_payload = json.dumps({"hostname": hostname, "ip": ip})
                        cql_parent = f"""
                        INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
                        VALUES ({parent_task_id}, 'mipha', 'host_join', 'processing', '{parent_payload.replace("'", "''")}', 10, {now_ms}, {now_ms});
                        """
                        run_cql_query(cql_parent)
                        
                        # B. Start all hypervisor services on the returning host
                        print(f"[Mipha HA] Starting all services on returning host {hostname}...")
                        start_cmd = "systemctl start zookeeper hydra-db aether linstor-controller spectrum bifrost dagur mimir vali catalyst gatoway logos mipha"
                        run_remote_spark(ip, start_cmd)
                        
                        # Sleep 10 seconds to allow services (especially Aether/storage) to boot
                        time.sleep(10)
                        
                        # Update parent task progress to 20%
                        cql_up = f"UPDATE hydra.catalyst_tasks SET progress = 20, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                        run_cql_query(cql_up)
                        
                        # C. Trigger self-heal (skipped for Linstor/DRBD)
                        pass
                        
                        # D. Create child Catalyst task for Linstor/DRBD sync
                        child_task_id = str(uuid.uuid4())
                        child_payload = json.dumps({"hostname": hostname, "parent_task_id": parent_task_id})
                        cql_child = f"""
                        INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
                        VALUES ({child_task_id}, 'aether', 'sync', 'processing', '{child_payload.replace("'", "''")}', 10, {now_ms}, {now_ms});
                        """
                        run_cql_query(cql_child)
                        
                        # E. Poll sync status
                        synced = False
                        # Poll up to 60 iterations (3 minutes)
                        for iteration in range(60):
                            child_progress = min(95, 10 + iteration * 5)
                            parent_progress = int(20 + (child_progress / 100.0) * 70)
                            
                            cql_up_child = f"UPDATE hydra.catalyst_tasks SET progress = {child_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {child_task_id};"
                            run_cql_query(cql_up_child)
                            
                            cql_up_parent = f"UPDATE hydra.catalyst_tasks SET progress = {parent_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_up_parent)
                            
                            pending = get_linstor_pending_sync()
                            print(f"[Mipha HA] Linstor/DRBD sync status: pending_sync_active={pending}")
                            if pending == 0:
                                synced = True
                                print(f"[Mipha HA] Linstor/DRBD resources fully synced on host {hostname}!")
                                break
                                
                            time.sleep(3)
                            
                        # F. Conclude task and update node status
                        now_ms_end = int(time.time() * 1000)
                        if synced:
                            # Set child & parent task to completed
                            cql_child_end = f"UPDATE hydra.catalyst_tasks SET status = 'completed', progress = 100, updated_at = {now_ms_end} WHERE task_id = {child_task_id};"
                            run_cql_query(cql_child_end)
                            
                            cql_parent_end = f"UPDATE hydra.catalyst_tasks SET status = 'completed', progress = 100, updated_at = {now_ms_end} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_parent_end)
                            
                            # Set node status to NORMAL
                            cql_normal = f"UPDATE hydra.nodes SET status = 'NORMAL' WHERE hostname = '{hostname}';"
                            run_cql_query(cql_normal)
                            print(f"[Mipha HA] Host {hostname} rejoin and sync completed successfully.")
                        else:
                            # Failed/timed out.
                            #
                            # `storage_name` was never defined anywhere in this file, so
                            # this branch raised NameError, was swallowed by the control
                            # loop's `except Exception`, and left both Catalyst tasks
                            # stuck at 'processing' forever -- a rejoin that timed out
                            # looked identical to one still in progress.
                            err_msg = "Linstor/DRBD volume sync timed out or failed to complete self-heal."
                            cql_child_end = f"UPDATE hydra.catalyst_tasks SET status = 'failed', progress = 100, error_msg = '{err_msg}', updated_at = {now_ms_end} WHERE task_id = {child_task_id};"
                            run_cql_query(cql_child_end)
                            
                            cql_parent_end = f"UPDATE hydra.catalyst_tasks SET status = 'failed', progress = 100, error_msg = '{err_msg}', updated_at = {now_ms_end} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_parent_end)
                            
                            # Leave status as RECOVERING so Vali does not use it
                            print(f"[Mipha HA] ERROR: Host {hostname} rejoin failed. Linstor/DRBD sync not complete.")
                    
                # 3. Trigger Failover if threshold reached
                if consecutive_failures.get(ip, 0) >= 3:
                    print(f"[Mipha HA] Host {hostname} ({ip}) confirmed OFFLINE! Starting failover orchestration...")
                    consecutive_failures[ip] = 0 # Reset counter to avoid loop
                    
                    # SSH Fencing if the host is still pingable
                    if ping_ok:
                        ssh_fence_host(ip)
                    
                    # A. Mark Host as DOWN in ScyllaDB
                    #
                    # Keyed on `hostname`, which is the partition key. This was written
                    # `WHERE ip = ...` and `ip` is a plain column, so Scylla rejected the
                    # statement outright ("Cannot execute this query as it might involve
                    # data filtering") -- and run_cql_query's rc=1 was never read. A host
                    # that died has therefore never actually been marked DOWN, which left
                    # Vali free to keep scheduling VMs onto it.
                    #
                    # Conditional on the status this pass read, so a host an operator moved
                    # into maintenance in the meantime is not dragged back out by a
                    # failover decision that was made before they touched it. A refusal is
                    # not a reason to abandon the failover: the per-VM release below is
                    # what actually keeps two hosts off one disk.
                    print(f"[Mipha HA] Marking host {hostname} status as DOWN in metadata store...")
                    ok_down, applied_down, current_down, err_down = run_lwt(
                        "/v1/node/maintenance", {
                            "hostname": hostname,
                            "status": "DOWN",
                            "maintenance_mode": False,
                            "expected_status": db_status,
                        })
                    if not ok_down:
                        print(f"[Mipha HA] WARNING: could not mark {hostname} DOWN: {err_down}")
                    elif not applied_down:
                        print(f"[Mipha HA] {hostname} was not '{db_status}' any more "
                              f"(it is '{current_down.get('status')}'); leaving its status alone.")

                    # Marking it DOWN takes it out of the VM scheduler. It does not take
                    # it out of the ScyllaDB ring, which is a separate lifecycle with its
                    # own consequences -- say what those are rather than leaving the
                    # cluster quietly one failure away from losing quorum.
                    report_ring_detach_candidate(hostname, ip)
                    
                    # A1. Create parent failover task in Catalyst for WebUI visibility
                    parent_task_id = str(uuid.uuid4())
                    now_ms = int(time.time() * 1000)
                    parent_payload = json.dumps({"hostname": hostname})
                    cql_parent = f"""
                    INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
                    VALUES ({parent_task_id}, 'mipha', 'failover', 'processing', '{parent_payload.replace("'", "''")}', 0, {now_ms}, {now_ms});
                    """
                    run_cql_query(cql_parent)
                    
                    # B. Active Polling for ZooKeeper Recovery
                    print("[Mipha HA] Waiting for ZooKeeper cluster consensus to settle...")
                    zk_leader_ip = None
                    for i in range(15): # Max 30 seconds polling
                        # Update progress: 10% to 25% during ZK wait
                        zk_progress = int(10 + (i / 15.0) * 15)
                        cql_up = f"UPDATE hydra.catalyst_tasks SET progress = {zk_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                        run_cql_query(cql_up)
                        
                        zk_leader_ip = get_zookeeper_leader_ip(hosts)
                        if zk_leader_ip:
                            print(f"[Mipha HA] ZooKeeper leader resolved at {zk_leader_ip}.")
                            break
                        time.sleep(2)
                        
                    if not zk_leader_ip:
                        print("[Mipha HA] ERROR: Failed to resolve ZooKeeper leader. Proceeding failover using local context.")
                        zk_leader_ip = LOCAL_IP
                        
                    # C. Active Polling for Vali Recovery
                    print("[Mipha HA] Verifying Vali VM Manager status...")
                    vali_ok = False
                    for i in range(10): # Check if Vali is responsive
                        # Update progress: 30% to 45% during Vali check
                        val_progress = int(30 + (i / 10.0) * 15)
                        cql_up = f"UPDATE hydra.catalyst_tasks SET progress = {val_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                        run_cql_query(cql_up)
                        
                        if check_vali_health(zk_leader_ip):
                            vali_ok = True
                            print("[Mipha HA] Vali VM Manager is active and responding.")
                            break
                        time.sleep(2)
                        
                    if not vali_ok:
                        # Vali is down, trigger active restart on all surviving hosts
                        print("[Mipha HA] Vali is unresponsive. Initiating remote restart across surviving hosts...")
                        surviving_hosts = [sh.get("ip") for sh in hosts if sh.get("ip") != ip]
                        for sh_ip in surviving_hosts:
                            run_remote_spark(sh_ip, "systemctl restart vali")
                            
                        # Active polling loop for Vali startup
                        print("[Mipha HA] Polling Vali API for recovery status...")
                        for i in range(15):
                            # Update progress: 50% to 65% during Vali startup
                            val_progress = int(50 + (i / 15.0) * 15)
                            cql_up = f"UPDATE hydra.catalyst_tasks SET progress = {val_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_up)
                            
                            if check_vali_health(zk_leader_ip):
                                vali_ok = True
                                print("[Mipha HA] Vali recovered and back online.")
                                break
                            time.sleep(2)
                            
                    if not vali_ok:
                        print("[Mipha HA] WARNING: Vali remains unresponsive. Proceeding with database orchestration.")
                        
                    # D. Query dead host's VMs in ScyllaDB
                    print(f"[Mipha HA] Scanning ScyllaDB for active VMs hosted on dead node {ip}...")
                    cql_up = f"UPDATE hydra.catalyst_tasks SET progress = 70, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                    run_cql_query(cql_up)
                    
                    cql_vms = "SELECT JSON name, memory, host_ip, state FROM hydra.vms;"
                    rc_v, stdout_v, _ = run_cql_query(cql_vms)
                    orphaned_vms = []
                    if rc_v == 0 and stdout_v:
                        for line in stdout_v.splitlines():
                            line = line.strip()
                            if line.startswith("{") and line.endswith("}"):
                                try:
                                    vm = json.loads(line)
                                    if vm.get("host_ip") == ip and vm.get("state") == "Running":
                                        orphaned_vms.append(vm)
                                except Exception:
                                    pass
                                    
                    if not orphaned_vms:
                        print(f"[Mipha HA] No running virtual machines found on dead node {ip}. Failover complete.")
                        now_ms_end = int(time.time() * 1000)
                        cql_parent_end = f"""
                        UPDATE hydra.catalyst_tasks 
                        SET status = 'completed', progress = 100, updated_at = {now_ms_end}
                        WHERE task_id = {parent_task_id};
                        """
                        run_cql_query(cql_parent_end)
                        continue
                        
                    print(f"[Mipha HA] Found {len(orphaned_vms)} orphaned VMs: {[v['name'] for v in orphaned_vms]}")
                    
                    # E. Failover VMs
                    submitted_tasks = []
                    for vm in orphaned_vms:
                        vm_name = vm.get("name")
                        print(f"[Mipha HA] Recovering VM '{vm_name}'...")
                        
                        # Give the placement back so Vali will allow a fresh start. If the
                        # VM is not on the dead host any more, it needs nothing from this
                        # failover and must not be started a second time.
                        if not release_orphaned_vm(vm_name, ip):
                            continue

                        # Submit task to Catalyst queue to start the VM.
                        # target_host is left empty so Vali schedules it on the best surviving node.
                        task_payload = {"vm_name": vm_name, "target_host": "", "parent_task_id": parent_task_id}
                        sub_task_id = submit_catalyst_task(zk_leader_ip, "vali", "start", task_payload)
                        if sub_task_id:
                            print(f"[Mipha HA] Successfully submitted failover task {sub_task_id} for '{vm_name}' to Catalyst.")
                            submitted_tasks.append((vm_name, sub_task_id))
                        else:
                            print(f"[Mipha HA] ERROR: Failed to submit failover task for '{vm_name}' to Catalyst.")
                            
                    # Wait/poll for the VM start tasks to finish
                    if submitted_tasks:
                        print(f"[Mipha HA] Polling {len(submitted_tasks)} VM start tasks for completion...")
                        finished_tasks = set()
                        # Poll up to 60 seconds (30 iterations of 2 seconds)
                        for iteration in range(30):
                            for vm_name, sub_task_id in submitted_tasks:
                                if sub_task_id in finished_tasks:
                                    continue
                                
                                cql_check = f"SELECT JSON status FROM hydra.catalyst_tasks WHERE task_id = {sub_task_id};"
                                rc_c, stdout_c, _ = run_cql_query(cql_check)
                                if rc_c == 0 and stdout_c:
                                    for line in stdout_c.splitlines():
                                        line = line.strip()
                                        if line.startswith("{") and line.endswith("}"):
                                            try:
                                                task_status_obj = json.loads(line)
                                                sub_status = task_status_obj.get("status")
                                                if sub_status in ["completed", "failed"]:
                                                    finished_tasks.add(sub_task_id)
                                                    print(f"[Mipha HA] Subtask {sub_task_id} for VM '{vm_name}' finished with status: {sub_status}")
                                            except Exception:
                                                pass
                            
                            # Update parent task progress: 70% to 95% depending on finished VM startups
                            num_finished = len(finished_tasks)
                            pct_finished = num_finished / len(submitted_tasks)
                            parent_progress = int(70 + pct_finished * 25)
                            cql_up = f"UPDATE hydra.catalyst_tasks SET progress = {parent_progress}, updated_at = {int(time.time()*1000)} WHERE task_id = {parent_task_id};"
                            run_cql_query(cql_up)
                            
                            if len(finished_tasks) == len(submitted_tasks):
                                print("[Mipha HA] All VM start tasks have finished.")
                                break
                            time.sleep(2)
                            
                    # F. Mark parent task as completed in Catalyst
                    now_ms_end = int(time.time() * 1000)
                    cql_parent_end = f"""
                    UPDATE hydra.catalyst_tasks 
                    SET status = 'completed', progress = 100, updated_at = {now_ms_end}
                    WHERE task_id = {parent_task_id};
                    """
                    run_cql_query(cql_parent_end)
                    print("[Mipha HA] Failover recovery orchestration completed.")
                    
        except Exception as e:
            sys.stderr.write(f"Error in Mipha HA control loop: {e}\n")
            
        time.sleep(10)

if __name__ == "__main__":
    # `mipha --auto-heal` runs the nightly storage pass and exits; with no arguments the
    # HA daemon runs as normal.
    if "--auto-heal" in sys.argv:
        sys.exit(run_auto_heal())
    main()
