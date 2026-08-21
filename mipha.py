#!/usr/bin/env python3
__build__ = "1.3.0"
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
import shutil
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
            # A fenced host behaves as a follower whatever ZooKeeper says about it. This
            # loop promotes linstor-db and the storage containers back to Primary within
            # two seconds of a demotion, so without this check a fence on the leader node
            # would undo itself immediately -- and the host would be holding Primary on
            # storage it has just declared it cannot serve.
            fenced = self_fence_is_active()
            is_leader = is_zookeeper_leader() and not fenced
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

def run_mtls_spark_api_full(ip, path, payload, method="POST"):
    """Like run_mtls_spark_api, but keeps the status code and the 4xx body.

    The fence endpoint answers 409 with the reason the fence did not take, and
    run_mtls_spark_api turns every HTTPError into `(-1, {}, "<message>")` -- which would
    reduce "two qemu processes are still running" to "the request failed". A fence is the
    one call in this daemon where the failure detail is the whole point.

    Returns (status, body, error). status is 0 when the request could not be made at all.

    The timeout is 60s rather than the 15s the other calls use, because the fence it
    carries out on the far side destroys guests and demotes resources and legitimately
    takes tens of seconds. A fence that overruns it is reported as unanswered, which is
    the safe reading -- the next control-loop pass retries and finds the work already
    done.
    """
    import urllib.error
    address, verify_identity = spark_endpoint(ip)
    context = spark_mtls_context(verify_identity)
    if not context:
        return 0, {}, "no usable mTLS keypair in /etc/hci/spark/certs or /root/.certs"

    url = f"https://{address}:9099{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=60) as response:
            return response.status, json.loads(response.read().decode("utf-8")), ""
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8")), ""
        except Exception:
            return exc.code, {}, f"HTTP {exc.code}"
    except Exception as exc:
        return 0, {}, str(exc)


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

# ---------------------------------------------------------------------------
# Fencing.
#
# A failover is safe only if the host being failed over has stopped writing. The fence
# this replaces was one request to spark-daemon on the host itself:
#
#     fence_cmd = "systemctl stop libvirtd virtqemud || true; pkill -9 qemu || true"
#     rc, _, _ = run_remote_spark(ip, fence_cmd)
#     return rc == 0
#
# Three defects, each of which alone is enough to corrupt a disk:
#
#   1. It asks the wedged host to fence itself. A host whose storage stack has hung, or
#      whose kernel is livelocked, still answers ICMP and can still complete a TCP
#      handshake while executing nothing.
#   2. Every command in that string ends in `|| true`, so the shell exits 0 whatever
#      happened. `rc == 0` means "spark-daemon accepted the request" and has never meant
#      "no qemu is running any more".
#   3. The return value was discarded at the call site, and the fence was only attempted
#      when the host still answered ping -- so a host that stopped answering ping was
#      assumed dead on no evidence at all, which is the one case the fence exists for.
#
# What replaces it is a ladder. Every rung must *read back* the state it claims to have
# produced, and a failover does not proceed unless a rung confirmed. "Could not tell" is
# recorded as a failure, never as a success:
#
#   self     the host already fenced itself and said so (hydra.nodes status FENCED).
#   spark    in-band. Kill the guests, stop libvirt, demote DRBD -- then read back that
#            no qemu remains, nothing holds a device open and nothing is Primary.
#   bmc      out-of-band. ipmitool chassis power off, then poll chassis power status
#            until it reads off. A command that returned 0 is not a power-off.
#   storage  the host cannot be reached or powered off, so prove instead that its kernel
#            is already refusing its writes -- see storage_fence_assert().
#
# The honest part: with no BMC and no DRBD quorum there is a residual case that none of
# these rungs can close, and it is documented in docs/fencing.md rather than papered
# over. In that case the default is to refuse the failover.
# ---------------------------------------------------------------------------

FENCING_CONFIG_PATH = "/etc/hci/fencing.json"

# Written by whichever path fenced this host -- its own watchdog or spark-daemon acting
# on a remote fence request. It lives on tmpfs on purpose: a reboot ends the fence,
# because a rebooted host is not running the guests any more.
SELF_FENCE_MARKER = "/run/hci/mipha-self-fence.json"

FENCE_METHOD_NONE = "none"
FENCE_METHOD_SELF = "self"
FENCE_METHOD_SPARK = "spark"
FENCE_METHOD_BMC = "bmc"
FENCE_METHOD_STORAGE = "storage-quorum"

# `hydra.nodes.status` for a host that has fenced itself. Vali already refuses to place
# on any host whose status is not exactly NORMAL, so this needs no scheduler change.
NODE_STATUS_FENCED = "FENCED"
NODE_STATUS_DEGRADED = "DEGRADED"

# Only these make a DRBD node that loses quorum stop writing. `on-no-quorum` has no
# other useful value, but reading it rather than assuming it is the difference between
# checking and hoping.
QUORUM_IO_POLICIES = ("io-error", "suspend-io")

DRBD_RESOURCE_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.-]{0,62}\Z")

# Mount points Mipha's own storage loop puts on top of DRBD devices. A fence has to take
# these down before a resource can be demoted, and the list is fixed rather than derived
# so a fence never unmounts something it did not put there.
FENCED_MOUNTS = ("/var/lib/linstor",
                 "/var/lib/hci/aether/volumes/default-vm-container",
                 "/var/lib/hci/aether/volumes/default-image-container")

DEFAULT_FENCING_CONFIG = {
    # What to do when no rung of the ladder could confirm. "block" refuses the failover
    # and says why; "failover" is the old behaviour and can corrupt a disk.
    "unconfirmed_fence_policy": "block",
    "bmc": {
        "defaults": {
            "interface": "lanplus",
            "power_off_timeout_seconds": 60,
        },
        "hosts": {},
    },
    "self_fence": {
        "enabled": True,
        "interval_seconds": 10,
        # Three consecutive passes, matching the cluster-side failover threshold. A
        # single failed probe is a blip; DRBD reports no quorum for a moment during
        # `drbdadm up` and virsh times out under load.
        "threshold": 3,
        # Nothing self-fences during startup: resources come up Secondary without
        # quorum and every probe would fire at once.
        "grace_seconds": 180,
        # A host that killed its own guests had a real fault. Returning it to service
        # automatically is how one flapping host takes VMs and drops them repeatedly, so
        # 0 means "an operator runs `mipha --clear-self-fence`".
        "auto_recover_after_clean_seconds": 0,
        # Stopping ZooKeeper hands leadership -- Mipha's, LINSTOR's controller, Bifrost's
        # VIP -- to a node that can still serve. Refused below three nodes, where the
        # remaining ensemble could not form a quorum anyway.
        "release_zookeeper_leadership": True,
    },
}

_FENCING_CONFIG_CACHE = {"key": None, "value": None}


def _merge_defaults(defaults, override):
    """Two levels of dict merge; anything deeper is taken from the operator verbatim."""
    merged = {}
    for key, value in defaults.items():
        if isinstance(value, dict):
            branch = dict(value)
            supplied = override.get(key)
            if isinstance(supplied, dict):
                for sub_key, sub_value in supplied.items():
                    branch[sub_key] = sub_value
            merged[key] = branch
        else:
            merged[key] = override.get(key, value)
    for key, value in override.items():
        if key not in merged:
            merged[key] = value
    return merged


def fencing_config_is_private(path):
    """True when nothing but root can read the file.

    Its own function because the answer decides whether BMC credentials are usable, and
    that decision has to be assertable in a test without depending on the mode bits the
    test host's filesystem happens to support.
    """
    try:
        return not (os.stat(path).st_mode & 0o077)
    except OSError:
        return False


def host_is_in_maintenance():
    """True when an operator has put this host into maintenance.

    A host being drained on purpose looks a great deal like a host whose storage is
    failing -- resources are demoted, guests are being moved off -- and self-fencing it
    would turn a planned maintenance window into an unplanned outage.
    """
    return os.path.exists("/etc/hci/maintenance.state")


def load_fencing_config(path=None):
    """Read /etc/hci/fencing.json, or the built-in defaults when it is absent.

    Absent is a supported state and means "no BMC" -- it must never mean "assume the
    fence worked", which is why the defaults refuse an unconfirmed failover.

    BMC credentials are dropped, loudly, from a file any non-root account can read. They
    are power-off credentials for the whole cluster; a fence that silently used them out
    of a world-readable file would be a worse outcome than a fence that does not run.

    Returns (config, warnings).
    """
    path = path or FENCING_CONFIG_PATH
    warnings = []
    try:
        st = os.stat(path)
    except OSError:
        return dict(DEFAULT_FENCING_CONFIG), warnings

    cache_key = (path, st.st_mtime_ns, st.st_size)
    cached = _FENCING_CONFIG_CACHE
    if cached.get("key") == cache_key:
        return cached["value"][0], list(cached["value"][1])

    try:
        with open(path, "r") as handle:
            raw = json.load(handle)
    except Exception as exc:
        warnings.append(f"{path} could not be read ({exc}); using built-in defaults, "
                        "which means no BMC fencing.")
        raw = {}
    if not isinstance(raw, dict):
        warnings.append(f"{path} is not a JSON object; using built-in defaults.")
        raw = {}

    if not fencing_config_is_private(path):
        warnings.append(
            f"{path} is mode {oct(st.st_mode & 0o777)}: it is readable by accounts other "
            "than root and BMC credentials are therefore ignored. chmod 600 it to enable "
            "out-of-band fencing.")
        raw = dict(raw)
        raw.pop("bmc", None)

    config = _merge_defaults(DEFAULT_FENCING_CONFIG, raw)
    policy = str(config.get("unconfirmed_fence_policy", "block")).lower()
    if policy not in ("block", "failover"):
        warnings.append(f"unconfirmed_fence_policy '{policy}' is not understood; "
                        "treating it as 'block'.")
        policy = "block"
    config["unconfirmed_fence_policy"] = policy

    _FENCING_CONFIG_CACHE["key"] = cache_key
    _FENCING_CONFIG_CACHE["value"] = (config, list(warnings))
    return config, warnings


def run_argv_local(argv, timeout=45):
    """Run a local command as an argv list, never through a shell.

    The fencing paths pass DRBD resource names and BMC addresses into commands. A list
    with shell=False makes each of those exactly one argument whatever it contains.
    """
    try:
        res = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             timeout=timeout)
    except FileNotFoundError:
        return 127, "", f"{argv[0]}: command not found"
    except subprocess.TimeoutExpired:
        return -1, "", f"{argv[0]}: timed out after {timeout}s"
    except OSError as exc:
        return -1, "", str(exc)
    return (res.returncode,
            res.stdout.decode("utf-8", errors="ignore"),
            res.stderr.decode("utf-8", errors="ignore"))


def local_hostname():
    """This node's name as the cluster document spells it."""
    for host in get_cluster_hosts():
        if host.get("ip") == LOCAL_IP:
            return host.get("hostname") or socket.gethostname()
    return socket.gethostname()


class FenceResult:
    """What a fence attempt proved, and how.

    `confirmed` is the only field the failover gate reads, and it is set only by a rung
    that read back the state it claims to have produced.
    """

    def __init__(self, hostname, ip):
        self.hostname = hostname
        self.ip = ip
        self.confirmed = False
        self.method = FENCE_METHOD_NONE
        self.detail = ""
        self.steps = []
        self.at = time.time()

    def record(self, method, confirmed, detail):
        self.steps.append({"method": method, "confirmed": bool(confirmed),
                           "detail": detail})
        if confirmed and not self.confirmed:
            self.confirmed = True
            self.method = method
            self.detail = detail
        return confirmed

    def summary(self):
        if self.confirmed:
            return f"confirmed by {self.method}: {self.detail}"
        attempts = "; ".join(f"{step['method']} -> {step['detail']}" for step in self.steps)
        return "NOT confirmed: " + (attempts or "no method was attempted")

    def as_dict(self):
        return {"hostname": self.hostname, "ip": self.ip, "confirmed": self.confirmed,
                "method": self.method, "detail": self.detail, "steps": self.steps,
                "at": self.at}


# Hosts fenced during the current outage, so a fence that succeeded is not repeated on
# every pass of the control loop. Only *confirmed* fences are recorded: repeating a fence
# that failed is exactly what we want to keep doing. Cleared by clear_fence_record() when
# the host answers its health check again, because the next outage is a new episode.
FENCE_LEDGER = {}


def clear_fence_record(ip):
    """Forget a host's fence once it is healthy again; its next outage is a new one."""
    return FENCE_LEDGER.pop(ip, None) is not None


def spark_fence_host(ip):
    """In-band fence: tell the host to stop, then read back that it did.

    Returns (confirmed, detail). The typed endpoint does the work and returns the
    post-state it observed; a daemon too old to have it falls back to the legacy command
    plus an explicit verification pass, because the legacy command's exit status says
    nothing (see the note at the top of this section).
    """
    status, res, err = run_mtls_spark_api_full(ip, "/api/v1/host/fence", {"confirm": True})
    if status in (200, 409) and isinstance(res, dict) and "fenced" in res:
        if res.get("fenced"):
            return True, (res.get("detail")
                          or "spark-daemon reports no guest processes, no open DRBD "
                             "device and no Primary resource")
        return False, ("spark-daemon ran the fence and it did not take: "
                       + (res.get("detail") or json.dumps(res))[:300])

    # A 404 means the daemon on that host predates the typed fence endpoint, which
    # happens mid-upgrade. Anything else means the request did not land.
    if status == 404:
        return legacy_spark_fence(ip)
    return False, f"spark-daemon did not answer the fence request: {err or 'unknown error'}"


def legacy_spark_fence(ip):
    """The pre-typed-endpoint fence, with the verification it never had.

    Kept for a rolling upgrade, where the host being fenced may still be running the old
    spark-daemon. The command itself is unchanged; what is new is that its exit status is
    ignored and the host is asked afterwards what is actually still running.
    """
    fence_cmd = ("systemctl stop libvirtd virtqemud || true; "
                 "pkill -9 qemu-system-x86_64 || true; pkill -9 qemu || true")
    run_remote_spark(ip, fence_cmd)

    rc, stdout, stderr = run_remote_spark(ip, "pgrep -a qemu")
    if rc not in (0, 1):
        return False, ("could not read the process table on the host after the fence "
                       f"({(stderr or stdout or '').strip()[:200]}), so nothing is proven")
    if rc == 0 and stdout.strip():
        return False, ("guest processes are still running after the fence: "
                       + " | ".join(stdout.strip().splitlines()[:5]))

    rc_s, status, err_s = run_mtls_spark_api(ip, "/api/v1/storage/drbd/status", None,
                                             method="GET")
    if rc_s != 0 or not isinstance(status, list):
        return False, ("no qemu is left, but the host's DRBD state could not be read "
                       f"({err_s or 'unparseable response'}), so it is not proven that it "
                       "released its disks")
    held = []
    for resource in status:
        if not isinstance(resource, dict):
            continue
        name = resource.get("name", "?")
        if str(resource.get("role", "")).lower() == "primary":
            held.append(f"{name} is Primary")
        for device in resource.get("devices") or []:
            if device.get("open"):
                held.append(f"{name}/{device.get('volume', 0)} is open")
    if held:
        return False, "the host still holds its storage: " + "; ".join(held[:5])
    return True, ("no guest process is left and no DRBD resource is Primary or open "
                  "(verified through the legacy fence path)")


def bmc_entry_for(hostname, ip, config):
    """The BMC record for a host, looked up by hostname then by address."""
    hosts = ((config.get("bmc") or {}).get("hosts") or {})
    if not isinstance(hosts, dict):
        return None
    for key in (hostname, ip):
        if key and key in hosts and isinstance(hosts[key], dict):
            entry = dict((config.get("bmc") or {}).get("defaults") or {})
            entry.update(hosts[key])
            return entry
    return None


def _bmc_password(entry):
    """(password, error). A password_file is preferred; it keeps the secret off argv."""
    path = entry.get("password_file")
    if path:
        try:
            st = os.stat(path)
        except OSError as exc:
            return None, f"password_file {path} cannot be read: {exc}"
        if not fencing_config_is_private(path):
            return None, (f"password_file {path} is mode {oct(st.st_mode & 0o777)}; "
                          "chmod 600 it before it will be used")
        try:
            with open(path, "r") as handle:
                return handle.read().strip("\r\n"), None
        except OSError as exc:
            return None, f"password_file {path} cannot be read: {exc}"
    password = entry.get("password")
    if isinstance(password, str) and password:
        return password, None
    return None, "no password or password_file in the BMC entry"


def bmc_fence_host(hostname, ip, config):
    """Out-of-band fence: power the chassis off and confirm from the BMC that it is off.

    Returns (confirmed, detail).

    Nothing here treats a zero exit status as a power-off. ipmitool returns 0 for a
    chassis command the BMC accepted and then failed to carry out, and for a session it
    opened against the wrong host entirely; the only evidence that counts is
    `chassis power status` reading off.

    The password is passed in the environment (`-E`) rather than on the command line,
    because argv is readable by every account on the host through /proc.
    """
    entry = bmc_entry_for(hostname, ip, config)
    if not entry:
        return False, (f"no BMC entry for {hostname} in {FENCING_CONFIG_PATH}; "
                       "out-of-band fencing is not configured for this host")
    address = entry.get("address")
    username = entry.get("username")
    if not address or not username:
        return False, f"the BMC entry for {hostname} has no address or no username"

    tool = shutil.which("ipmitool")
    if not tool:
        return False, ("ipmitool is not installed on this host, so the configured BMC "
                       "cannot be used")

    password, error = _bmc_password(entry)
    if error:
        return False, f"BMC credentials for {hostname} are unusable: {error}"

    base = [tool, "-I", str(entry.get("interface") or "lanplus"), "-H", str(address),
            "-U", str(username), "-E"]
    if entry.get("cipher_suite") is not None:
        base += ["-C", str(entry["cipher_suite"])]
    if entry.get("privilege_level"):
        base += ["-L", str(entry["privilege_level"])]

    saved_env = os.environ.copy()
    os.environ["IPMI_PASSWORD"] = password
    try:
        rc, stdout, stderr = run_argv_local(base + ["chassis", "power", "off"], timeout=60)
        if rc != 0:
            detail = (stderr or stdout).strip()[:200]
            return False, f"ipmitool chassis power off failed against {address}: {detail}"

        deadline = time.time() + float(entry.get("power_off_timeout_seconds") or 60)
        last = ""
        while time.time() < deadline:
            rc_s, out_s, err_s = run_argv_local(base + ["chassis", "power", "status"],
                                                timeout=30)
            last = (out_s or err_s).strip()
            if rc_s == 0 and "is off" in last.lower():
                return True, f"the BMC at {address} reports the chassis powered off"
            time.sleep(3)
    finally:
        os.environ.clear()
        os.environ.update(saved_env)

    return False, (f"the BMC at {address} accepted the power-off but never reported the "
                   f"chassis off (last answer: {last or 'no answer'}). The host is not "
                   "proven to be down.")


def _drbd_status_from(ip):
    """`drbdsetup status --json` for a host, as a list; None when it cannot be read."""
    if ip in (LOCAL_IP, "127.0.0.1"):
        rc, stdout, _ = run_argv_local(["drbdsetup", "status", "--json"], timeout=30)
        if rc != 0:
            return None
        try:
            parsed = json.loads(stdout.strip() or "[]")
        except Exception:
            return None
        return parsed if isinstance(parsed, list) else None
    rc, res, _ = run_mtls_spark_api(ip, "/api/v1/storage/drbd/status", None, method="GET")
    if rc != 0 or not isinstance(res, list):
        return None
    return res


def _drbd_options_from(ip, resource):
    """Configured resource options for one DRBD resource; None when unreadable.

    The device-level `quorum` flag in `drbdsetup status` reads true both when quorum is
    held and when quorum is switched off entirely, so the *configuration* has to be read
    separately. Conflating the two is what would let a storage fence claim a majority on
    a cluster that has no quorum at all.
    """
    if not DRBD_RESOURCE_RE.match(resource or ""):
        return None
    if ip in (LOCAL_IP, "127.0.0.1"):
        rc, stdout, _ = run_argv_local(["drbdsetup", "show", "--json", resource], timeout=30)
        if rc != 0:
            return None
        try:
            parsed = json.loads(stdout.strip() or "[]")
        except Exception:
            return None
        for entry in parsed if isinstance(parsed, list) else []:
            if isinstance(entry, dict) and entry.get("resource") == resource:
                options = entry.get("options")
                return options if isinstance(options, dict) else None
        return None
    rc, res, _ = run_mtls_spark_api(
        ip, f"/api/v1/storage/drbd/options?resource={resource}", None, method="GET")
    if rc != 0 or not isinstance(res, dict):
        return None
    options = res.get("options")
    return options if isinstance(options, dict) else None


def quorum_arms_the_fence(options, node_count):
    """Would losing quorum actually stop this resource's writes? (bool, why not).

    `quorum majority` and `quorum all` are safe by construction: two disjoint sets cannot
    both be a majority, and only one set can be all of them. A *numeric* quorum is only
    safe when it is more than half the nodes -- `quorum 1` is satisfied by every node on
    its own, so both sides of a partition would keep writing while `drbdsetup status`
    reported quorum on both.
    """
    if not isinstance(options, dict):
        return False, "the resource options could not be read"
    policy = str(options.get("on-no-quorum", "")).strip().lower()
    if policy not in QUORUM_IO_POLICIES:
        return False, (f"on-no-quorum is '{policy or 'unset'}', so a node that loses "
                       "quorum keeps writing")
    setting = str(options.get("quorum", "off")).strip().lower()
    if setting in ("majority", "all"):
        return True, ""
    if setting in ("off", "", "none"):
        return False, ("quorum is off, so DRBD does not stop a partitioned node from "
                       "writing")
    try:
        votes = int(setting)
    except ValueError:
        return False, f"quorum is '{setting}', which cannot be interpreted"
    if node_count and 2 * votes > node_count:
        return True, ""
    return False, (f"quorum is {votes} of {node_count} nodes, which both sides of a "
                   "partition can hold at once")


def storage_fence_assert(dead_hostname, dead_ip, hosts):
    """Prove, from DRBD, that the dead host's kernel is already refusing its writes.

    Returns (confirmed, detail).

    This is the rung that exists on every cluster, because the cluster always owns the
    storage even when it owns no BMC. It is worth being exact about what it can and
    cannot do, because the obvious readings of "cut the host off from its DRBD resources"
    do not work:

      * Disconnecting the resource on the survivors does nothing to the dead host: DRBD
        replication is peer-to-peer and the old Primary goes on writing its own local
        copy exactly as before.
      * `linstor resource delete <deadnode> <res>` needs the satellite on that node to
        carry it out. Against an unreachable node it records an intent, it does not stop
        a writer. It is storage *cleanup*, and calling it a fence would be a lie.
      * Promoting the resource here proves nothing either. DRBD's refusal to allow two
        Primaries is enforced across a *connection*; once the connection is gone, the
        promotion check has no peer to consult.

    What does work is a property the dead host's own kernel enforces without cooperation
    from its userspace: DRBD quorum. With `quorum majority` and `on-no-quorum io-error`,
    a node that cannot see a majority of the resource's nodes fails every I/O on it. If
    we hold quorum here, the dead host by definition does not hold it there, so its
    writes are already erroring. That is a proof, not a mitigation -- and it is the only
    thing in this file that makes an unreachable host safe to fail over.

    LINSTOR arms this automatically (`DrbdOptions/auto-quorum`, on by default, plus
    diskless tiebreakers for two-replica resources), so most real clusters have it. A
    single-replica resource has no majority to hold and is reported as such.
    """
    hosts = hosts or get_cluster_hosts()
    survivors = [h for h in hosts if h.get("ip") and h.get("ip") != dead_ip]
    if not survivors:
        return False, "there is no surviving host to read the storage state from"

    # Which resources does the dead host back? Ask the survivors: a resource that
    # replicates to the dead node has a connection named after it. This is DRBD's own
    # view rather than an inventory that can be stale.
    findings = []
    checked = 0
    seen = set()
    for host in survivors:
        ip = host.get("ip")
        status = _drbd_status_from(ip)
        if status is None:
            findings.append(f"{host.get('hostname') or ip}: DRBD state unreadable")
            continue
        for resource in status:
            if not isinstance(resource, dict):
                continue
            name = resource.get("name")
            if not name or (name, ip) in seen:
                continue
            connections = [c for c in (resource.get("connections") or [])
                           if isinstance(c, dict)]
            peer = next((c for c in connections if c.get("name") == dead_hostname), None)
            if peer is None:
                continue
            seen.add((name, ip))
            checked += 1

            state = str(peer.get("connection", "")).strip()
            if state.lower() == "connected":
                findings.append(f"{name}: still Connected to {dead_hostname} from "
                                f"{host.get('hostname') or ip}, so it is not cut off")
                continue

            # node_count counts this survivor plus every peer DRBD knows about, which is
            # what a numeric quorum setting is measured against.
            node_count = 1 + len(connections)
            options = _drbd_options_from(ip, name)
            armed, why_not = quorum_arms_the_fence(options, node_count)
            if not armed:
                findings.append(f"{name}: {why_not}")
                continue

            devices = [d for d in (resource.get("devices") or []) if isinstance(d, dict)]
            if not devices:
                findings.append(f"{name}: no local device on {host.get('hostname') or ip} "
                                "to read quorum from")
                continue
            without = [str(d.get("volume", 0)) for d in devices if d.get("quorum") is not True]
            if without:
                findings.append(f"{name}: this side does not hold quorum on volume(s) "
                                + ", ".join(without))
                continue
            findings.append(f"{name}: quorum held here, {dead_hostname} is "
                            f"{state or 'disconnected'} and its I/O is failing")

    if not checked:
        return False, ("no DRBD resource on any surviving host replicates to "
                       f"{dead_hostname}, so storage gives no evidence either way")
    bad = [f for f in findings if ": quorum held here" not in f]
    if bad:
        return False, ("storage fencing could not be asserted for every resource the "
                       "host backs: " + "; ".join(bad[:6]))
    return True, ("every DRBD resource the host backs is quorate here and disconnected "
                  f"there, so its writes are already failing ({len(findings)} resource(s))")


def fence_host(hostname, ip, hosts=None, db_status=None, config=None):
    """Fence a host and return what was actually proven. Never raises.

    A host already confirmed fenced during this outage is not fenced again -- powering
    off a chassis that is already off, or killing guests that are already dead, adds
    nothing and takes time the failover does not have. The ledger records confirmations
    only, so a fence that failed is retried on the next pass.
    """
    cached = FENCE_LEDGER.get(ip)
    if cached is not None:
        result = FenceResult(hostname, ip)
        result.confirmed = True
        result.method = cached.get("method", FENCE_METHOD_NONE)
        result.detail = cached.get("detail", "")
        result.at = cached.get("at", time.time())
        result.steps = [{"method": "ledger", "confirmed": True,
                         "detail": f"already fenced by {result.method} at "
                                   f"{time.strftime('%H:%M:%S', time.localtime(result.at))}"}]
        return result

    if config is None:
        config, warnings = load_fencing_config()
        for warning in warnings:
            print(f"[Mipha HA] FENCE CONFIG: {warning}")

    result = FenceResult(hostname, ip)
    print(f"[Mipha HA] Fencing {hostname} ({ip})...")

    if db_status == NODE_STATUS_FENCED:
        result.record(FENCE_METHOD_SELF, True,
                      "the host fenced itself and recorded it in hydra.nodes")
        FENCE_LEDGER[ip] = result.as_dict()
        return result

    for method, call in ((FENCE_METHOD_SPARK, lambda: spark_fence_host(ip)),
                         (FENCE_METHOD_BMC, lambda: bmc_fence_host(hostname, ip, config)),
                         (FENCE_METHOD_STORAGE,
                          lambda: storage_fence_assert(hostname, ip, hosts))):
        try:
            confirmed, detail = call()
        except Exception as exc:
            confirmed, detail = False, f"raised {exc.__class__.__name__}: {exc}"
        print(f"[Mipha HA] Fence [{method}] {'CONFIRMED' if confirmed else 'no'}: {detail}")
        if result.record(method, confirmed, detail):
            FENCE_LEDGER[ip] = result.as_dict()
            return result

    return result


def failover_permitted(fence, config):
    """May the failover proceed on this fence result? (allowed, reason).

    The whole point of the ladder is this decision, so it is one function rather than a
    condition buried in the control loop.

    "block" is the default because the alternative is to assume a fence that could not be
    confirmed worked, and that assumption is what puts a second qemu on a DRBD device
    that the first one is still writing. A cluster with no BMC and no DRBD quorum will
    sometimes reach this and stop; that is the honest outcome, and the operator is told
    exactly which of the two to configure.
    """
    if fence.confirmed:
        return True, f"the fence was confirmed by {fence.method}"
    policy = (config or {}).get("unconfirmed_fence_policy", "block")
    if policy == "failover":
        return True, ("unconfirmed_fence_policy is 'failover', so the failover proceeds "
                      "on a host that is not proven to have stopped writing")
    return False, fence.summary()


# ---------------------------------------------------------------------------
# Self-fencing.
#
# Mipha's cluster-side liveness is a ping and a spark-daemon reply, and both keep
# answering on a host whose storage or libvirt has died. Such a host is reported healthy,
# keeps its VMs, and is never evacuated -- the guests sit there with failing I/O while
# the cluster believes nothing is wrong.
#
# This loop runs on *every* host, not only the leader, because the failure it detects is
# local and the host is the only party that can see it.
#
# Two tiers, and the difference between them is the whole design:
#
#   quarantine  the host stops accepting new placement (hydra.nodes status DEGRADED,
#               which Vali already excludes) and keeps running what it has. This is the
#               answer for libvirt being dead: qemu keeps running and the guests keep
#               working when libvirtd dies, so killing them would be a self-inflicted
#               outage, and failing them over while they are still writing would be
#               corruption. Losing management of a working VM is not a reason to destroy
#               it.
#   fence       the host stops its guests, gives up Primary on every DRBD resource, and
#               records itself FENCED so the leader evacuates it. This is reserved for
#               conditions under which the guests are *already* broken: DRBD has lost
#               quorum, or the device has no usable data path. Under those conditions
#               stopping is strictly better than continuing, and -- the point -- it makes
#               the host provably safe to fail over without a BMC.
#
# The failure mode of self-fencing is a healthy host that evacuates itself on a blip, so:
# a probe that cannot determine an answer never counts toward the hard tier; the trigger
# must hold for three consecutive passes; there is a startup grace period; a host in
# maintenance is exempt; a single-node cluster never self-fences (there is nowhere for
# the guests to go, so it would be a pure outage); and the hardware-fault trigger
# additionally requires a peer that could actually take the load. Quorum loss is exempt
# from that last check because quorum *is* a majority test -- if we lost it, a majority
# exists elsewhere by definition.
# ---------------------------------------------------------------------------

SELF_FENCE_STATE = {
    "active": False,
    "quarantined": False,
    "reason": "",
    "at": 0,
    "announced": False,
    "report": {},
}


def self_fence_is_active():
    """True when this host has fenced itself, including across a Mipha restart."""
    if SELF_FENCE_STATE.get("active"):
        return True
    return os.path.exists(SELF_FENCE_MARKER)


def load_self_fence_marker():
    """Re-adopt a fence recorded before this process started.

    Without this a Mipha restart on a fenced host would promote DRBD straight back to
    Primary, which is the fence undoing itself two seconds after it was applied.
    """
    try:
        with open(SELF_FENCE_MARKER, "r") as handle:
            data = json.load(handle)
    except Exception:
        return False
    if not isinstance(data, dict):
        return False
    SELF_FENCE_STATE.update({
        "active": True,
        "reason": data.get("reason", "recorded by an earlier fence"),
        "at": data.get("at", time.time()),
        "announced": False,
        "report": data.get("report") or {},
    })
    print(f"[Mipha Self-Fence] This host is fenced ({SELF_FENCE_STATE['reason']}). "
          "It will not take Primary or accept placement until "
          "`mipha --clear-self-fence` is run.")
    return True


def write_self_fence_marker(reason, report):
    try:
        os.makedirs(os.path.dirname(SELF_FENCE_MARKER), exist_ok=True)
        with open(SELF_FENCE_MARKER, "w") as handle:
            json.dump({"reason": reason, "at": time.time(), "report": report}, handle)
        return True
    except Exception as exc:
        sys.stderr.write(f"[Mipha Self-Fence] Could not write {SELF_FENCE_MARKER}: {exc}\n")
        return False


def resource_is_unserviceable(resource):
    """Is this resource Primary here while unable to serve I/O? (bool, cause, detail).

    Three causes, kept apart because they escalate differently:

      quorum-lost   DRBD says we do not hold quorum. With on-no-quorum io-error the
                    guest's writes are already failing, and a majority exists elsewhere.
      io-failures   the resource has been put into forced I/O failure or suspended for
                    quorum -- same effect on the guest, local origin.
      no-data       the local disk failed and no connected peer is UpToDate, so there is
                    nothing left to read from. A failed local disk *with* a good peer is
                    not listed: DRBD 9 keeps serving over the network and the guest never
                    notices, which is the whole point of replication.
    """
    if not isinstance(resource, dict):
        return False, "", ""
    if str(resource.get("role", "")).lower() != "primary":
        return False, "", ""
    name = resource.get("name", "?")

    if resource.get("force-io-failures") or resource.get("suspended-quorum"):
        return True, "io-failures", f"{name} is Primary with I/O failing or suspended for quorum"

    connections = [c for c in (resource.get("connections") or []) if isinstance(c, dict)]
    for device in resource.get("devices") or []:
        if not isinstance(device, dict):
            continue
        volume = device.get("volume", 0)
        if device.get("quorum") is False:
            return True, "quorum-lost", f"{name}/{volume} is Primary without quorum"
        if str(device.get("disk-state", "")).strip() in ("Failed", "Detaching"):
            healthy_peer = False
            for connection in connections:
                if str(connection.get("connection", "")).lower() != "connected":
                    continue
                for peer_device in connection.get("peer_devices") or []:
                    if (isinstance(peer_device, dict)
                            and peer_device.get("volume") == volume
                            and str(peer_device.get("peer-disk-state", "")) == "UpToDate"):
                        healthy_peer = True
            if not healthy_peer:
                return True, "no-data", (f"{name}/{volume} disk is "
                                         f"{device.get('disk-state')} with no UpToDate peer")
    return False, "", ""


def probe_local_health():
    """One pass of the local subsystem probes.

    Every probe returns one of "ok", "failed" or "unknown", and "unknown" is load-bearing:
    it means the probe could not reach a verdict, and it must never escalate to the tier
    that destroys running guests. That distinction is what keeps a slow virsh or a
    momentarily unreadable drbdsetup from evacuating a healthy host.
    """
    probe = {"libvirt": "unknown", "drbd_control": "unknown", "unserviceable": [],
             "detail": {}}

    if shutil.which("virsh") is None:
        probe["detail"]["libvirt"] = "virsh is not installed"
    else:
        rc, stdout, stderr = run_argv_local(
            ["virsh", "-c", "qemu:///system", "list", "--name"], timeout=15)
        if rc == 0:
            probe["libvirt"] = "ok"
        elif rc == 127:
            probe["detail"]["libvirt"] = "virsh disappeared between checks"
        else:
            probe["libvirt"] = "failed"
            probe["detail"]["libvirt"] = (stderr or stdout).strip()[:200] or f"virsh exited {rc}"

    configured = get_all_drbd_resources()
    rc, stdout, stderr = run_argv_local(["drbdsetup", "status", "--json"], timeout=20)
    if rc == 0:
        try:
            status = json.loads(stdout.strip() or "[]")
        except Exception:
            status = None
        if isinstance(status, list):
            probe["drbd_control"] = "ok"
            for resource in status:
                bad, cause, detail = resource_is_unserviceable(resource)
                if bad:
                    probe["unserviceable"].append(
                        {"resource": resource.get("name", "?"), "cause": cause,
                         "detail": detail})
        else:
            probe["detail"]["drbd_control"] = "drbdsetup returned output that is not JSON"
    elif configured:
        # Resources are configured on this host, so drbdsetup refusing to answer is a
        # storage-stack failure rather than "this node has no DRBD".
        probe["drbd_control"] = "failed"
        probe["detail"]["drbd_control"] = (stderr or stdout).strip()[:200] or f"drbdsetup exited {rc}"
    else:
        probe["detail"]["drbd_control"] = "no DRBD resources are configured on this host"

    return probe


def healthy_peer_exists(hosts=None):
    """True when some other host answers spark-daemon and is not itself fenced."""
    for host in hosts or get_cluster_hosts():
        ip = host.get("ip")
        if not ip or ip == LOCAL_IP:
            continue
        rc, res, _ = run_mtls_spark_api(ip, "/api/v1/node/status", None, method="GET")
        if rc == 0 and isinstance(res, dict) and res.get("ip") == ip:
            return True
    return False


def self_fence_decide(probe, counters, config, hosts, uptime_seconds):
    """(action, reason) for one pass. action is "none", "quarantine" or "fence".

    `counters` is mutated: it holds the consecutive-failure count per condition, and any
    condition that is not present this pass is reset to zero. A blip therefore has to
    survive `threshold` passes in a row to do anything, and a single good pass wipes the
    history.
    """
    settings = config.get("self_fence") or {}
    if not settings.get("enabled", True):
        counters.clear()
        return "none", "self-fencing is disabled in " + FENCING_CONFIG_PATH

    hosts = hosts if hosts is not None else get_cluster_hosts()
    if len(hosts) <= 1:
        counters.clear()
        return "none", ("this is a single-node cluster: there is nowhere for the guests "
                        "to be restarted, so fencing would be a pure outage")

    if host_is_in_maintenance():
        counters.clear()
        return "none", "this host is in maintenance"

    if uptime_seconds < float(settings.get("grace_seconds", 180)):
        counters.clear()
        return "none", "within the startup grace period"

    threshold = int(settings.get("threshold", 3))

    hard = [item for item in probe["unserviceable"]]
    counters["unserviceable"] = counters.get("unserviceable", 0) + 1 if hard else 0
    soft = [name for name in ("libvirt", "drbd_control") if probe.get(name) == "failed"]
    for name in ("libvirt", "drbd_control"):
        counters[name] = counters.get(name, 0) + 1 if name in soft else 0

    if hard and counters["unserviceable"] >= threshold:
        causes = sorted({item["cause"] for item in hard})
        detail = "; ".join(item["detail"] for item in hard[:5])
        if "quorum-lost" in causes:
            # No peer check: losing quorum *is* the majority test. If we lost it, some
            # other set of nodes holds it and can serve these resources.
            return "fence", f"DRBD quorum lost while Primary ({detail})"
        if not healthy_peer_exists(hosts):
            return "quarantine", (f"local storage is unserviceable ({detail}) but no peer "
                                  "is answering, so stopping the guests here would not "
                                  "get them started anywhere else")
        return "fence", f"local storage cannot serve I/O ({detail})"

    for name in ("drbd_control", "libvirt"):
        if counters.get(name, 0) >= threshold:
            return "quarantine", (f"{name} has failed {counters[name]} consecutive probes: "
                                  + probe["detail"].get(name, "no detail"))

    return "none", ""


def local_fence_fallback():
    """Fence this host without spark-daemon. Returns the same report shape as the API.

    Only reached when the local daemon is not answering, which is exactly the case a host
    in trouble is likely to be in. Deliberately short: kill the guests, drop the mounts
    Mipha itself created, demote everything, then report what is *still* held rather than
    what was attempted.
    """
    report = {"fenced": False, "qemu_pids": [], "primary_resources": [], "open_devices": [],
              "libvirt_active": False, "detail": ""}

    run_argv_local(["systemctl", "stop", "libvirtd", "virtqemud",
                    "libvirtd.socket", "virtqemud.socket"], timeout=60)

    def qemu_pids():
        found = []
        try:
            entries = os.listdir("/proc")
        except OSError:
            return found
        for entry in entries:
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/comm", "r") as handle:
                    if handle.read().strip().startswith("qemu"):
                        found.append(int(entry))
            except Exception:
                continue
        return found

    for _attempt in range(3):
        pids = qemu_pids()
        if not pids:
            break
        for pid in pids:
            try:
                os.kill(pid, 9)
            except Exception:
                pass
        time.sleep(2)
    report["qemu_pids"] = qemu_pids()

    for mount in FENCED_MOUNTS:
        rc, _, _ = run_argv_local(["mountpoint", "-q", mount], timeout=15)
        if rc == 0:
            run_argv_local(["umount", "-l", mount], timeout=30)

    for resource in get_all_drbd_resources():
        if get_local_drbd_role(resource) == "Primary":
            run_argv_local(["drbdadm", "secondary", resource], timeout=60)
    still = [r for r in get_all_drbd_resources() if get_local_drbd_role(r) == "Primary"]
    report["primary_resources"] = still
    report["fenced"] = not report["qemu_pids"] and not still
    report["detail"] = ("fenced locally without spark-daemon" if report["fenced"]
                        else "local fence did not take: "
                             + (f"qemu still running {report['qemu_pids']} " if report["qemu_pids"] else "")
                             + (f"still Primary on {still}" if still else ""))
    return report


def execute_self_fence(reason, hosts=None, config=None):
    """Take this host out: stop the guests, give up Primary, tell the cluster.

    The marker is written *first*. linstor_ha_loop() promotes resources back to Primary
    within two seconds if this node holds ZooKeeper leadership, so a fence that demotes
    before the loop knows to stand down undoes itself immediately.

    Returns the verification report. `fenced` false means the host is NOT safe to fail
    over -- something still holds a device -- and that is reported rather than smoothed.
    """
    config = config or load_fencing_config()[0]
    settings = config.get("self_fence") or {}

    SELF_FENCE_STATE.update({"active": True, "reason": reason, "at": time.time(),
                             "announced": False, "report": {}})
    write_self_fence_marker(reason, {})
    print(f"[Mipha Self-Fence] FENCING THIS HOST: {reason}")

    status, res, err = run_mtls_spark_api_full(LOCAL_IP, "/api/v1/host/fence",
                                               {"confirm": True})
    if status in (200, 409) and isinstance(res, dict) and "fenced" in res:
        report = res
    else:
        print(f"[Mipha Self-Fence] The local spark-daemon could not run the fence "
              f"({err or 'unexpected response'}); fencing directly.")
        report = local_fence_fallback()

    SELF_FENCE_STATE["report"] = report
    write_self_fence_marker(reason, report)

    if report.get("fenced"):
        print("[Mipha Self-Fence] This host holds no guest process and no Primary DRBD "
              "resource. It is safe to restart its VMs elsewhere.")
    else:
        print("[Mipha Self-Fence] CRITICAL: the fence did not fully take -- "
              f"{report.get('detail') or json.dumps(report)[:300]}. This host is NOT safe "
              "to fail over; an operator must power it off.")

    # Leadership has to move off a host that has just admitted it cannot serve storage,
    # or nothing evacuates it: the Mipha leader does not monitor itself, and the LINSTOR
    # controller would stay here on top of storage that does not work. Refused below three
    # nodes, where the remaining ZooKeeper ensemble could not form a quorum either.
    if settings.get("release_zookeeper_leadership", True):
        cluster_hosts = hosts if hosts is not None else get_cluster_hosts()
        if len(cluster_hosts) >= 3:
            if is_zookeeper_leader("127.0.0.1"):
                print("[Mipha Self-Fence] Releasing ZooKeeper leadership so a healthy "
                      "node takes over coordination.")
                run_argv_local(["systemctl", "stop", "zookeeper"], timeout=60)
        else:
            print(f"[Mipha Self-Fence] Not releasing ZooKeeper leadership: "
                  f"{len(cluster_hosts)} nodes cannot keep a quorum without this one. "
                  "Coordination stays here until an operator intervenes.")

    announce_node_status(self_fence_announcement())
    return report


def self_fence_announcement():
    """The status this host should publish for its own fence.

    FENCED is a claim the leader acts on: it skips its own fencing ladder entirely and
    goes straight to restarting this host's VMs elsewhere. So it may only be published
    when the local fence actually *took*. A fence that could not stop a guest or could
    not demote a resource publishes DEGRADED instead -- the host stops receiving work,
    and the leader still has to prove the fence itself before it moves anything.
    """
    if SELF_FENCE_STATE.get("report", {}).get("fenced"):
        return NODE_STATUS_FENCED
    return NODE_STATUS_DEGRADED


def announce_node_status(target_status):
    """Move this host's hydra.nodes row to `target_status`, conditionally.

    Returns True when the row now reads `target_status`. The compare-and-swap is against
    the status just read, so a status an operator changed in the meantime is not
    clobbered by a decision made before they touched it.

    A failure here costs visibility, not safety: the host is already fenced locally
    whatever the database says, and the caller retries on the next pass.
    """
    hostname = local_hostname()
    rc, stdout, _ = run_cql_query(
        f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{hostname}';")
    current = None
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    current = json.loads(line).get("status")
                except Exception:
                    pass
    if current == target_status:
        return True
    if current is None:
        sys.stderr.write("[Mipha Self-Fence] Could not read this host's row in "
                         "hydra.nodes; will retry.\n")
        return False
    ok, applied, row, error = run_lwt("/v1/node/maintenance", {
        "hostname": hostname,
        "status": target_status,
        "maintenance_mode": False,
        "expected_status": current,
    })
    if not ok:
        sys.stderr.write(f"[Mipha Self-Fence] Could not record status {target_status} for "
                         f"{hostname}: {error}\n")
        return False
    if not applied:
        print(f"[Mipha Self-Fence] {hostname} is '{row.get('status')}', not '{current}'; "
              f"leaving it alone rather than forcing {target_status}.")
        return False
    print(f"[Mipha Self-Fence] Recorded {hostname} as {target_status}.")
    return True


def clear_self_fence(force=False):
    """`mipha --clear-self-fence`: return a fenced host to service.

    Deliberately manual. A host that destroyed its own guests had a real fault, and a
    host that returns to service by itself is a host that can take VMs and drop them
    again on a loop. The probe is re-run and printed first so the operator sees whether
    the fault is actually gone.
    """
    probe = probe_local_health()
    still_bad = probe["unserviceable"] or probe["libvirt"] == "failed" or probe["drbd_control"] == "failed"
    print("[Mipha] Local health probe:")
    print(f"  libvirt      : {probe['libvirt']} {probe['detail'].get('libvirt', '')}".rstrip())
    print(f"  drbd control : {probe['drbd_control']} {probe['detail'].get('drbd_control', '')}".rstrip())
    for item in probe["unserviceable"]:
        print(f"  storage      : {item['cause']} -- {item['detail']}")
    if still_bad and not force:
        print("[Mipha] The condition that fenced this host is still present. Re-run with "
              "--force to clear the fence anyway.")
        return 1

    removed = False
    try:
        os.remove(SELF_FENCE_MARKER)
        removed = True
    except FileNotFoundError:
        pass
    except OSError as exc:
        print(f"[Mipha] Could not remove {SELF_FENCE_MARKER}: {exc}")
        return 1
    SELF_FENCE_STATE.update({"active": False, "quarantined": False, "reason": "",
                             "at": 0, "announced": False, "report": {}})

    hostname = local_hostname()
    rc, stdout, _ = run_cql_query(
        f"SELECT JSON status FROM hydra.nodes WHERE hostname = '{hostname}';")
    current = None
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    current = json.loads(line).get("status")
                except Exception:
                    pass
    if current in (NODE_STATUS_FENCED, NODE_STATUS_DEGRADED):
        ok, applied, row, error = run_lwt("/v1/node/maintenance", {
            "hostname": hostname, "status": "NORMAL", "maintenance_mode": False,
            "expected_status": current})
        if ok and applied:
            print(f"[Mipha] {hostname} is NORMAL again.")
        else:
            print(f"[Mipha] Could not return {hostname} to NORMAL "
                  f"({error or row.get('status')}); set it by hand.")

    rc_z, _, _ = run_argv_local(["systemctl", "is-active", "zookeeper"], timeout=15)
    if rc_z != 0:
        print("[Mipha] Starting ZooKeeper, which the fence stopped...")
        run_argv_local(["systemctl", "start", "zookeeper"], timeout=60)

    print("[Mipha] Fence cleared." if removed else "[Mipha] This host was not fenced.")
    return 0


def self_fence_loop():
    """The per-host watchdog. Runs everywhere, leader or not."""
    started = time.time()
    counters = {}
    config, warnings = load_fencing_config()
    for warning in warnings:
        print(f"[Mipha Self-Fence] CONFIG: {warning}")
    load_self_fence_marker()
    interval = float((config.get("self_fence") or {}).get("interval_seconds", 10))
    print("[Mipha Self-Fence] Local subsystem watchdog started.")

    clean_since = None
    while True:
        try:
            config, _ = load_fencing_config()
            settings = config.get("self_fence") or {}
            interval = float(settings.get("interval_seconds", 10))
            hosts = get_cluster_hosts()

            if self_fence_is_active():
                # Keep trying to tell the cluster, because the fence is only useful to a
                # failover once somebody else can see it.
                if not SELF_FENCE_STATE.get("announced"):
                    SELF_FENCE_STATE["announced"] = announce_node_status(
                        self_fence_announcement())
                recover_after = float(settings.get("auto_recover_after_clean_seconds", 0))
                if recover_after > 0:
                    probe = probe_local_health()
                    healthy = (not probe["unserviceable"]
                               and probe["libvirt"] != "failed"
                               and probe["drbd_control"] != "failed")
                    clean_since = clean_since if healthy else None
                    if healthy and clean_since is None:
                        clean_since = time.time()
                    if healthy and time.time() - clean_since >= recover_after:
                        print("[Mipha Self-Fence] Local health has been clean for "
                              f"{recover_after:.0f}s; clearing the fence automatically.")
                        clear_self_fence(force=True)
                        clean_since = None
                time.sleep(interval)
                continue

            probe = probe_local_health()
            action, reason = self_fence_decide(probe, counters, config, hosts,
                                               time.time() - started)

            if action == "fence":
                execute_self_fence(reason, hosts=hosts, config=config)
            elif action == "quarantine":
                if not SELF_FENCE_STATE.get("quarantined"):
                    print(f"[Mipha Self-Fence] Quarantining this host: {reason}. It keeps "
                          "its running VMs; it stops receiving new ones.")
                    SELF_FENCE_STATE["quarantined"] = True
                    SELF_FENCE_STATE["reason"] = reason
                    SELF_FENCE_STATE["announced"] = False
                # Retried until it lands, and separately from the decision above: a
                # quarantine only takes effect once Vali can see it, and a Daruk that was
                # briefly unreachable must not leave this host silently schedulable.
                if not SELF_FENCE_STATE.get("announced"):
                    SELF_FENCE_STATE["announced"] = announce_node_status(NODE_STATUS_DEGRADED)
            elif SELF_FENCE_STATE.get("quarantined"):
                if announce_node_status("NORMAL"):
                    print("[Mipha Self-Fence] Local subsystems are healthy again; this "
                          "host has left quarantine.")
                    SELF_FENCE_STATE["quarantined"] = False
                    SELF_FENCE_STATE["announced"] = False
                    SELF_FENCE_STATE["reason"] = ""
        except Exception as exc:
            sys.stderr.write(f"[Mipha Self-Fence] Error in watchdog loop: {exc}\n")
        time.sleep(interval)


def report_fence_status():
    """`mipha --fence-status`: what fencing this host could actually perform.

    Printed rather than inferred from logs, because "we thought we had a fence" is how
    the cluster gets to the split-brain this whole file exists to avoid. No secret is
    printed -- only whether one is present and usable.
    """
    config, warnings = load_fencing_config()
    for warning in warnings:
        print(f"WARNING: {warning}")

    print(f"Fencing configuration : {FENCING_CONFIG_PATH}"
          + ("" if os.path.exists(FENCING_CONFIG_PATH) else " (absent -- defaults in use)"))
    print(f"Unconfirmed fence     : {config['unconfirmed_fence_policy']}")

    hosts = get_cluster_hosts()
    print(f"\nOut-of-band (BMC) coverage, {len(hosts)} host(s) in the cluster:")
    if shutil.which("ipmitool") is None:
        print("  ipmitool is NOT installed on this host: no BMC fence can run from here.")
    for host in hosts:
        hostname = host.get("hostname") or "?"
        entry = bmc_entry_for(hostname, host.get("ip"), config)
        if not entry:
            print(f"  {hostname:<24} no BMC entry -- power fencing unavailable")
            continue
        password, error = _bmc_password(entry)
        state = "usable" if password else f"UNUSABLE ({error})"
        print(f"  {hostname:<24} {entry.get('address')} as {entry.get('username')} -- {state}")

    print("\nStorage fencing (DRBD quorum) for the resources on this host:")
    status = _drbd_status_from(LOCAL_IP)
    if status is None:
        print("  drbdsetup status could not be read.")
    elif not status:
        print("  no DRBD resources on this host.")
    else:
        unarmed = 0
        for resource in status:
            name = resource.get("name", "?")
            connections = [c for c in (resource.get("connections") or []) if isinstance(c, dict)]
            armed, why_not = quorum_arms_the_fence(_drbd_options_from(LOCAL_IP, name),
                                                   1 + len(connections))
            if armed:
                print(f"  {name:<28} ARMED -- a partitioned peer stops writing")
            else:
                unarmed += 1
                print(f"  {name:<28} NOT ARMED -- {why_not}")
        if unarmed:
            # Say what to run, not just what is wrong. Without quorum this rung can never
            # confirm, and on a cluster with no BMC that is the whole storage fence.
            print("\n  Arm it on the LINSTOR controller, per resource definition:")
            print("    linstor resource-definition drbd-options --quorum majority "
                  "--on-no-quorum io-error <resource>")
            print("  or for everything LINSTOR creates from now on:")
            print("    linstor controller set-property DrbdOptions/auto-quorum io-error")
            print("  A resource with fewer than three nodes -- counting the diskless "
                  "tiebreakers LINSTOR adds for two-replica resources -- has no majority "
                  "to hold, so quorum cannot be armed for it at all.")

    settings = config.get("self_fence") or {}
    print(f"\nSelf-fencing          : {'enabled' if settings.get('enabled', True) else 'disabled'}"
          f" (threshold {settings.get('threshold', 3)} x {settings.get('interval_seconds', 10)}s)")
    if self_fence_is_active():
        print(f"  THIS HOST IS FENCED  : {SELF_FENCE_STATE.get('reason') or 'see the marker file'}")
        print("  Clear it with        : mipha --clear-self-fence")
    return 0

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

    # The local subsystem watchdog runs on every host, not only the ZooKeeper leader:
    # the failure it exists to catch -- storage or libvirt dead while the network keeps
    # answering -- is invisible from anywhere else in the cluster.
    threading.Thread(target=self_fence_loop, daemon=True).start()

    # Track consecutive failures per host IP
    consecutive_failures = {}
    # Hosts whose self-fence this leader has already acted on. A self-fenced host keeps
    # answering its health check, so without this the failover would re-run every pass.
    self_fence_handled = set()

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

                    # The host is answering again, so whatever it was fenced for is over.
                    # Forget the confirmation: the next outage has to be proven on its own
                    # evidence, not on a fence that succeeded an hour ago. A host still
                    # marked FENCED is excepted -- it answers precisely because it fenced
                    # itself and is still fenced.
                    if db_status != NODE_STATUS_FENCED and clear_fence_record(ip):
                        print(f"[Mipha HA] Host {hostname} ({ip}) is answering again; "
                              "its fence record is cleared.")

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
                    
                # 3. Trigger failover.
                #
                # Two triggers. The first is the original one, three consecutive failed
                # health checks. The second is a host that fenced *itself*: its storage
                # or its hypervisor died while its network kept answering, so it never
                # misses a health check and the old code left its VMs stopped on a host
                # nobody was evacuating. `self_fence_handled` keeps that second trigger
                # from re-running every ten seconds for as long as the host stays FENCED.
                self_fenced = (db_status == NODE_STATUS_FENCED)
                if not self_fenced:
                    self_fence_handled.discard(ip)
                if consecutive_failures.get(ip, 0) >= 3 or (self_fenced and ip not in self_fence_handled):
                    if self_fenced:
                        print(f"[Mipha HA] Host {hostname} ({ip}) has fenced itself. "
                              "Starting failover orchestration...")
                        self_fence_handled.add(ip)
                    else:
                        print(f"[Mipha HA] Host {hostname} ({ip}) confirmed OFFLINE! Starting failover orchestration...")
                    consecutive_failures[ip] = 0 # Reset counter to avoid loop

                    # A0. The parent task is created before the fence, not after it, so
                    # a failover that is *refused* is as visible in the UI as one that
                    # runs. A refusal that only appears in the journal is a refusal
                    # nobody acts on.
                    parent_task_id = str(uuid.uuid4())
                    now_ms = int(time.time() * 1000)
                    parent_payload = json.dumps({"hostname": hostname})
                    cql_parent = f"""
                    INSERT INTO hydra.catalyst_tasks (task_id, service, action, status, payload, progress, created_at, updated_at)
                    VALUES ({parent_task_id}, 'mipha', 'failover', 'processing', '{parent_payload.replace("'", "''")}', 0, {now_ms}, {now_ms});
                    """
                    run_cql_query(cql_parent)

                    # A0b. Fence, and require proof.
                    #
                    # The old code fenced only when the host still answered ping and then
                    # threw the answer away, so a host that had stopped answering ping was
                    # assumed dead on no evidence -- the exact case the fence exists for.
                    # The fence now always runs and its result decides whether anything
                    # else happens.
                    print(f"[Mipha HA] Host {hostname} ping={'up' if ping_ok else 'down'}, "
                          f"spark={'up' if spark_ok else 'down'}.")
                    fence = fence_host(hostname, ip, hosts=hosts, db_status=db_status)
                    print(f"[Mipha HA] Fence for {hostname}: {fence.summary()}")

                    fence_config, _ = load_fencing_config()
                    allowed, why = failover_permitted(fence, fence_config)
                    if not allowed:
                        # Marking it DOWN is safe whatever the fence did: it only stops
                        # Vali placing *new* work there. It is the restart of the old
                        # host's guests elsewhere that the fence gates.
                        run_lwt("/v1/node/maintenance", {
                            "hostname": hostname, "status": "DOWN",
                            "maintenance_mode": False, "expected_status": db_status})
                        err_msg = ("HA did not fail this host over because the fence "
                                   "could not be confirmed. " + why)
                        print(f"[Mipha HA] CRITICAL: {err_msg}")
                        print("[Mipha HA] Its VMs are left placed on it. Restarting them "
                              "elsewhere now could put two writers on one DRBD device. "
                              "Power the host off, or configure a BMC in "
                              f"{FENCING_CONFIG_PATH} / enable DRBD quorum -- see "
                              "docs/fencing.md -- then this proceeds by itself.")
                        end_ms = int(time.time() * 1000)
                        run_cql_query(f"""
                        UPDATE hydra.catalyst_tasks
                        SET status = 'failed', progress = 100,
                            error_msg = '{err_msg.replace("'", "''")[:900]}',
                            updated_at = {end_ms}
                        WHERE task_id = {parent_task_id};
                        """)
                        continue
                    if not fence.confirmed:
                        print(f"[Mipha HA] WARNING: {why}. If the host is still running "
                              "its guests, this can corrupt their disks.")

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
                    #
                    # A host that fenced *itself* is left FENCED rather than moved to
                    # DOWN. DOWN is the state Mipha's rejoin path watches, and a
                    # self-fenced host has never stopped answering, so marking it DOWN
                    # would have it "rejoin" on the next pass -- starting its services
                    # and taking Primary again on the storage it just gave up.
                    if self_fenced:
                        print(f"[Mipha HA] Leaving {hostname} in {NODE_STATUS_FENCED}; it "
                              "returns to service through `mipha --clear-self-fence` on "
                              "that host.")
                    else:
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
    # `mipha --fence-status` reports what fencing this host could actually perform, and
    # `mipha --clear-self-fence` returns a host that fenced itself to service. Both are
    # subcommands on the daemon rather than a separate tool for the same reason
    # --auto-heal is: the state they read and write lives in this process.
    if "--fence-status" in sys.argv:
        sys.exit(report_fence_status())
    if "--clear-self-fence" in sys.argv:
        sys.exit(clear_self_fence(force="--force" in sys.argv))
    main()
