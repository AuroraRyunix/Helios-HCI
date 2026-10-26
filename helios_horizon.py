#!/usr/bin/env python3
"""Helios Horizon: Scale-out VDI & Application Streaming Broker backed by Sidon linked clones.

Provides desktop pool lifecycle management (`instant_clone` non-persistent and `persistent`
dedicated desktop pools), AD/LDAP user/group entitlement verification, Sidon zero-copy
linked-clone provisioning (`helios_sidon.clone`), Apache Guacamole (`guacd`) HTML5 RDP/VNC/SPICE
session brokering, and HMAC-SHA256 signed session tickets.
"""

import base64
import hashlib
import hmac
import json
import os
import re
import socket
import subprocess
import time
import uuid

from helios_cql import cql_escape, run_cql_query

POOL_NAME_RE = re.compile(r"\A[a-z0-9][a-z0-9_-]{0,31}\Z")
VALID_POOL_TYPES = ("instant_clone", "persistent")
VALID_PROTOCOLS = ("rdp", "vnc", "spice")
VALID_POWER_POLICIES = ("always_on", "suspend_on_idle", "power_off_on_idle")
DEFAULT_PORTS = {"rdp": 3389, "vnc": 5900, "spice": 5930}
GUACD_CONF_PATH = "/etc/guacamole/guacd.conf"
GUACD_DEFAULT_HOST = "127.0.0.1"
GUACD_DEFAULT_PORT = 4822
HORIZON_SECRET_FILE = "/etc/hci/certs/console_token.secret"
CLUSTER_JSON_PATH = "/etc/hci/cluster.json"


def validate_pool_name(name):
    """Validate a VDI pool name (lowercase alphanumeric, '-', '_', 1..32 chars)."""
    if not isinstance(name, str) or not POOL_NAME_RE.match(name):
        return None, (
            "pool_name must be 1..32 characters of lowercase letters, digits, '-' or '_', "
            "starting with a letter or digit"
        )
    return name, None


def validate_pool_type(pool_type):
    """Validate desktop pool type ('instant_clone' or 'persistent')."""
    val = str(pool_type or "instant_clone").strip().lower()
    if val not in VALID_POOL_TYPES:
        return None, "pool_type must be one of " + ", ".join(VALID_POOL_TYPES)
    return val, None


def validate_display_protocol(protocol):
    """Validate display streaming protocol ('rdp', 'vnc', or 'spice')."""
    val = str(protocol or "rdp").strip().lower()
    if val not in VALID_PROTOCOLS:
        return None, "protocol must be one of " + ", ".join(VALID_PROTOCOLS)
    return val, None


def validate_power_policy(policy):
    """Validate idle desktop power policy."""
    val = str(policy or "always_on").strip().lower()
    if val not in VALID_POWER_POLICIES:
        return None, "power_policy must be one of " + ", ".join(VALID_POWER_POLICIES)
    return val, None


def desktop_vm_name(pool_name, index):
    """Deterministic VM name for a desktop instance in `pool_name` (1-based index)."""
    return f"vdi-{pool_name}-{int(index):03d}"


def desktop_vdisk_id(pool_name, index):
    """Deterministic Sidon vdisk identifier for a desktop instance's primary disk."""
    return f"{desktop_vm_name(pool_name, index)}-disk0"


def render_guacd_conf(bind_host=GUACD_DEFAULT_HOST, bind_port=GUACD_DEFAULT_PORT):
    """Render `/etc/guacamole/guacd.conf` configuration file."""
    return (
        "# Managed by Helios Horizon VDI Broker\n"
        "[server]\n"
        f"bind_host = {bind_host}\n"
        f"bind_port = {int(bind_port)}\n"
    )


def parse_json_rows(stdout):
    rows = []
    for line in (stdout or "").splitlines():
        s = line.strip()
        if s.startswith("{") and s.endswith("}"):
            try:
                rows.append(json.loads(s))
            except Exception:
                pass
    return rows


def list_db_pools(run_query=None):
    """Read all VDI desktop pools from `hydra.horizon_pools`."""
    runner = run_query or run_cql_query
    rc, stdout, err = runner("SELECT JSON * FROM hydra.horizon_pools;")
    if rc != 0:
        return None, (err or stdout or "Failed to read hydra.horizon_pools").strip()
    pools = parse_json_rows(stdout)
    pools.sort(key=lambda p: str(p.get("pool_name") or ""))
    return pools, None


def get_db_pool(pool_name, run_query=None):
    """Read a single VDI desktop pool from `hydra.horizon_pools`."""
    name, err = validate_pool_name(pool_name)
    if err:
        return None, err
    runner = run_query or run_cql_query
    rc, stdout, qerr = runner(
        f"SELECT JSON * FROM hydra.horizon_pools WHERE pool_name = '{cql_escape(name)}';"
    )
    if rc != 0:
        return None, (qerr or stdout or "Failed to query hydra.horizon_pools").strip()
    rows = parse_json_rows(stdout)
    if not rows:
        return None, None
    return rows[0], None


def list_db_sessions(pool_name=None, run_query=None):
    """Read VDI sessions from `hydra.horizon_sessions`."""
    runner = run_query or run_cql_query
    rc, stdout, err = runner("SELECT JSON * FROM hydra.horizon_sessions;")
    if rc != 0:
        return None, (err or stdout or "Failed to read hydra.horizon_sessions").strip()
    sessions = parse_json_rows(stdout)
    if pool_name:
        sessions = [s for s in sessions if s.get("pool_name") == pool_name]
    sessions.sort(key=lambda s: int(s.get("connected_at_ms") or 0), reverse=True)
    return sessions, None


def get_db_session(session_id, run_query=None):
    """Read a single VDI session from `hydra.horizon_sessions`."""
    sid = str(session_id or "").strip()
    if not sid:
        return None, "session_id is required"
    runner = run_query or run_cql_query
    rc, stdout, err = runner(
        f"SELECT JSON * FROM hydra.horizon_sessions WHERE session_id = '{cql_escape(sid)}';"
    )
    if rc != 0:
        return None, (err or stdout or "Failed to query hydra.horizon_sessions").strip()
    rows = parse_json_rows(stdout)
    if not rows:
        return None, None
    return rows[0], None


def user_is_entitled(username, user_groups, pool):
    """Verify whether `username` (with `user_groups`) is entitled to launch a session in `pool`.

    An empty or `'*'` `entitled_groups` field allows any authenticated cluster user.
    Otherwise `entitled_groups` is a comma-separated list of user or AD/LDAP group names
    (case-insensitive).
    """
    if not username or not str(username).strip():
        return False
    raw = str((pool or {}).get("entitled_groups") or "*").strip()
    if not raw or raw == "*":
        return True
    allowed = {tok.strip().lower() for tok in raw.split(",") if tok.strip()}
    if not allowed or "*" in allowed:
        return True
    uname = str(username).strip().lower()
    if uname in allowed:
        return True
    for grp in (user_groups or ()):
        if str(grp).strip().lower() in allowed:
            return True
    return False


def get_horizon_signing_secret(secret_path=HORIZON_SECRET_FILE, cluster_json=CLUSTER_JSON_PATH):
    """Return the cluster secret used to HMAC-sign VDI session tokens."""
    env_secret = os.environ.get("HELIOS_CONSOLE_VERIFIER_SECRET", "").strip()
    if env_secret:
        return env_secret
    try:
        if os.path.exists(secret_path):
            with open(secret_path, "r", encoding="utf-8") as sf:
                val = sf.read().strip()
                if val:
                    return val
    except OSError:
        pass
    try:
        if os.path.exists(cluster_json):
            with open(cluster_json, "r", encoding="utf-8") as cf:
                data = json.load(cf)
                val = str(data.get("cluster_uuid") or data.get("vip") or "").strip()
                if val:
                    return hashlib.sha256(("horizon:" + val).encode("utf-8")).hexdigest()
    except Exception:
        pass
    return None


def build_guacamole_connection_spec(session, pool, target_host, target_port=None):
    """Build the Apache Guacamole (`guacd`) protocol connection parameters for a desktop session."""
    protocol = str(session.get("protocol") or pool.get("protocol") or "rdp").lower()
    port = int(target_port or pool.get("desktop_port") or DEFAULT_PORTS.get(protocol, 3389))
    params = {
        "hostname": str(target_host),
        "port": str(port),
    }
    if protocol == "rdp":
        params.update({
            "security": "any",
            "ignore-cert": "true",
            "enable-wallpaper": "false",
            "enable-theming": "true",
            "enable-font-smoothing": "true",
            "resize-method": "display-update",
            "username": str(session.get("username") or ""),
        })
        domain = str(pool.get("domain_join") or "").strip()
        if domain:
            params["domain"] = domain
    elif protocol == "vnc":
        params.update({
            "autoretry": "3",
            "color-depth": "24",
        })
    elif protocol == "spice":
        params.update({
            "enable-audio": "true",
        })
    return {
        "session_id": session.get("session_id"),
        "pool_name": pool.get("pool_name"),
        "vm_name": session.get("vm_name"),
        "username": session.get("username"),
        "protocol": protocol,
        "guacd_host": str(session.get("host_ip") or GUACD_DEFAULT_HOST),
        "guacd_port": int(session.get("guacd_port") or GUACD_DEFAULT_PORT),
        "parameters": params,
    }


def sign_horizon_session_token(spec, secret, ttl_seconds=3600, now_s=None):
    """Sign a Guacamole/Horizon connection specification into a compact HMAC-SHA256 token."""
    if not secret:
        raise ValueError("Horizon signing secret is unavailable")
    issued = int(now_s if now_s is not None else time.time())
    payload = dict(spec)
    payload["iat"] = issued
    payload["exp"] = issued + int(ttl_seconds)
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    b64 = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    sig = hmac.new(secret.encode("utf-8"), b64.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"


def verify_horizon_session_token(token, secret, now_s=None):
    """Verify and decode a signed Horizon session token. Returns `(payload, error)`."""
    if not secret:
        return None, "Horizon signing secret is unavailable"
    if not isinstance(token, str) or "." not in token:
        return None, "Malformed session token"
    b64, sig = token.rsplit(".", 1)
    expected = hmac.new(secret.encode("utf-8"), b64.encode("ascii"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None, "Invalid session token signature"
    pad = "=" * ((4 - len(b64) % 4) % 4)
    try:
        raw = base64.urlsafe_b64decode((b64 + pad).encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        return None, f"Failed to decode token payload: {exc}"
    now_val = int(now_s if now_s is not None else time.time())
    exp = int(payload.get("exp") or 0)
    if exp and now_val > exp:
        return None, "Session token has expired"
    return payload, None


def _tcp_port_listening(host, port, timeout=0.5):
    """Check whether a local TCP port is accepting connections."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        return sock.connect_ex((host, int(port))) == 0
    except OSError:
        return False
    finally:
        sock.close()


def inspect_local_horizon_host(run_argv_fn=None, guacd_port=GUACD_DEFAULT_PORT):
    """Inspect authentic local `guacd` service state and socket readiness on this host.

    Never fabricates or hardcodes status: queries live `systemctl` and live TCP sockets.
    """
    if run_argv_fn is None:
        def run_argv_fn(argv, timeout=10):
            try:
                res = subprocess.run(
                    argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout
                )
                return (
                    res.returncode,
                    res.stdout.decode("utf-8", errors="ignore"),
                    res.stderr.decode("utf-8", errors="ignore"),
                )
            except Exception as exc:
                return -1, "", str(exc)

    rc, out, _ = run_argv_fn(["systemctl", "is-active", "guacd"], timeout=5)
    guacd_service = out.strip() if rc == 0 else (out.strip() or "inactive")
    guacd_listening = _tcp_port_listening(GUACD_DEFAULT_HOST, guacd_port)
    agahnim_listening = _tcp_port_listening("127.0.0.1", 8081)

    return {
        "guacd_service": guacd_service,
        "guacd_active": guacd_service == "active",
        "guacd_port": int(guacd_port),
        "guacd_listening": guacd_listening,
        "agahnim_listening": agahnim_listening,
        "guacd_conf_present": os.path.exists(GUACD_CONF_PATH),
    }


def ensure_guacd_on_host(bind_host=GUACD_DEFAULT_HOST, bind_port=GUACD_DEFAULT_PORT, run_argv_fn=None):
    """Write `/etc/guacamole/guacd.conf` and start/enable `guacd` on the local host."""
    if run_argv_fn is None:
        def run_argv_fn(argv, timeout=30):
            try:
                res = subprocess.run(
                    argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout
                )
                return (
                    res.returncode,
                    res.stdout.decode("utf-8", errors="ignore"),
                    res.stderr.decode("utf-8", errors="ignore"),
                )
            except Exception as exc:
                return -1, "", str(exc)

    try:
        os.makedirs(os.path.dirname(GUACD_CONF_PATH), mode=0o755, exist_ok=True)
        with open(GUACD_CONF_PATH, "w", encoding="utf-8") as cf:
            cf.write(render_guacd_conf(bind_host=bind_host, bind_port=bind_port))
    except OSError as exc:
        return False, f"Failed to write {GUACD_CONF_PATH}: {exc}"

    rc, out, err = run_argv_fn(["systemctl", "enable", "--now", "guacd"], timeout=20)
    if rc != 0:
        return False, (err or out or "systemctl enable --now guacd failed").strip()
    return True, None


def create_pool_record(params, run_query=None, now_ms=None):
    """Validate and insert or update a VDI pool in `hydra.horizon_pools`."""
    runner = run_query or run_cql_query
    pool_name, err = validate_pool_name(params.get("pool_name"))
    if err:
        return None, err
    pool_type, err = validate_pool_type(params.get("pool_type", "instant_clone"))
    if err:
        return None, err
    protocol, err = validate_display_protocol(params.get("protocol", "rdp"))
    if err:
        return None, err
    power_policy, err = validate_power_policy(params.get("power_policy", "always_on"))
    if err:
        return None, err

    template_vm = str(params.get("template_vm") or "").strip()
    if not template_vm or not re.match(r"\A[A-Za-z0-9][A-Za-z0-9_.-]{0,62}\Z", template_vm):
        return None, "template_vm must be a valid VM or golden template name"

    try:
        min_desktops = int(params.get("min_desktops", 1))
        max_desktops = int(params.get("max_desktops", 10))
        vcpu = int(params.get("vcpu", 2))
        memory_mb = int(params.get("memory_mb", 4096))
        desktop_port = int(params.get("desktop_port") or DEFAULT_PORTS[protocol])
        idle_timeout_min = int(params.get("idle_timeout_min", 60))
    except (TypeError, ValueError):
        return None, "min_desktops, max_desktops, vcpu, memory_mb, desktop_port, and idle_timeout_min must be integers"

    if min_desktops < 0 or max_desktops < 1 or min_desktops > max_desktops or max_desktops > 500:
        return None, "require 0 <= min_desktops <= max_desktops <= 500 and max_desktops >= 1"
    if vcpu < 1 or vcpu > 128:
        return None, "vcpu must be between 1 and 128"
    if memory_mb < 512 or memory_mb > 1048576:
        return None, "memory_mb must be between 512 and 1048576"

    network_name = str(params.get("network_name") or "default").strip()
    domain_join = str(params.get("domain_join") or "").strip()
    entitled_groups = str(params.get("entitled_groups") or "*").strip() or "*"
    ts = int(now_ms if now_ms is not None else time.time() * 1000)

    cql = (
        "INSERT INTO hydra.horizon_pools ("
        "pool_name, template_vm, pool_type, protocol, desktop_port, "
        "min_desktops, max_desktops, vcpu, memory_mb, network_name, "
        "domain_join, entitled_groups, power_policy, idle_timeout_min, "
        "state, created_at_ms, updated_at_ms"
        ") VALUES ("
        f"'{cql_escape(pool_name)}', '{cql_escape(template_vm)}', '{cql_escape(pool_type)}', "
        f"'{cql_escape(protocol)}', {desktop_port}, {min_desktops}, {max_desktops}, "
        f"{vcpu}, {memory_mb}, '{cql_escape(network_name)}', '{cql_escape(domain_join)}', "
        f"'{cql_escape(entitled_groups)}', '{cql_escape(power_policy)}', {idle_timeout_min}, "
        f"'READY', {ts}, {ts});"
    )
    rc, out, qerr = runner(cql)
    if rc != 0:
        return None, (qerr or out or "Failed to write hydra.horizon_pools").strip()

    return {
        "pool_name": pool_name,
        "template_vm": template_vm,
        "pool_type": pool_type,
        "protocol": protocol,
        "desktop_port": desktop_port,
        "min_desktops": min_desktops,
        "max_desktops": max_desktops,
        "vcpu": vcpu,
        "memory_mb": memory_mb,
        "network_name": network_name,
        "domain_join": domain_join,
        "entitled_groups": entitled_groups,
        "power_policy": power_policy,
        "idle_timeout_min": idle_timeout_min,
        "state": "READY",
        "created_at_ms": ts,
        "updated_at_ms": ts,
    }, None


def delete_pool_record(pool_name, force=False, run_query=None):
    """Delete a VDI pool from `hydra.horizon_pools`, refusing if active sessions exist unless `force=True`."""
    name, err = validate_pool_name(pool_name)
    if err:
        return False, err
    runner = run_query or run_cql_query
    sessions, serr = list_db_sessions(pool_name=name, run_query=runner)
    if serr:
        return False, serr
    active = [s for s in (sessions or []) if s.get("state") == "CONNECTED"]
    if active and not force:
        return False, f"Pool '{name}' has {len(active)} active session(s); disconnect them or pass force=true"
    for sess in (sessions or []):
        sid = sess.get("session_id")
        if sid:
            runner(f"DELETE FROM hydra.horizon_sessions WHERE session_id = '{cql_escape(sid)}';")
    rc, out, qerr = runner(f"DELETE FROM hydra.horizon_pools WHERE pool_name = '{cql_escape(name)}';")
    if rc != 0:
        return False, (qerr or out or "Failed to delete pool").strip()
    return True, None


def list_pool_desktop_vms(pool_name, run_query=None):
    """List all desktop VMs (`vdi-<pool_name>-*`) registered in `hydra.vms`."""
    name, err = validate_pool_name(pool_name)
    if err:
        return None, err
    runner = run_query or run_cql_query
    rc, stdout, qerr = runner("SELECT JSON * FROM hydra.vms;")
    if rc != 0:
        return None, (qerr or stdout or "Failed to read hydra.vms").strip()
    prefix = f"vdi-{name}-"
    vms = [row for row in parse_json_rows(stdout) if str(row.get("name") or "").startswith(prefix)]
    vms.sort(key=lambda r: str(r.get("name") or ""))
    return vms, None


def provision_desktop_clone(pool, index, host_ip, sidon_clone_fn=None, run_query=None):
    """Provision a single linked-clone desktop VM in `pool` using Sidon zero-copy clone.

    1. Clones `<template_vm>-disk0` (or `template_vm` if already a vdisk id) into
       `vdi-<pool_name>-<index:03d>-disk0` via Sidon (`helios_sidon.clone`).
    2. Registers the linked-clone VM row in `hydra.vms`.
    """
    runner = run_query or run_cql_query
    pool_name = pool["pool_name"]
    template_vm = pool["template_vm"]
    vm_name = desktop_vm_name(pool_name, index)
    vdisk_id = desktop_vdisk_id(pool_name, index)
    parent_vdisk = template_vm if template_vm.endswith("-disk0") else f"{template_vm}-disk0"

    if sidon_clone_fn is None:
        import helios_sidon
        sidon_clone_fn = lambda parent, child: helios_sidon.clone(parent, child)

    try:
        sidon_clone_fn(parent_vdisk, vdisk_id)
    except Exception as exc:
        return None, f"Sidon linked clone failed ({parent_vdisk} -> {vdisk_id}): {exc}"

    vcpu = int(pool.get("vcpu") or 2)
    memory_mb = int(pool.get("memory_mb") or 4096)
    disks_json = json.dumps([{"vdisk_id": vdisk_id, "bus": "virtio", "index": 0}])
    tags_json = json.dumps(["horizon-vdi", f"pool:{pool_name}"])
    notes = f"Helios Horizon VDI desktop ({pool.get('pool_type', 'instant_clone')}) from {template_vm}"

    cql = (
        "INSERT INTO hydra.vms ("
        "name, vcpu, memory_mb, status, node, disks_list, tags, notes, is_template"
        ") VALUES ("
        f"'{cql_escape(vm_name)}', {vcpu}, {memory_mb}, 'RUNNING', '{cql_escape(str(host_ip or ''))}', "
        f"'{cql_escape(disks_json)}', '{cql_escape(tags_json)}', '{cql_escape(notes)}', false);"
    )
    rc, out, qerr = runner(cql)
    if rc != 0:
        return None, (qerr or out or "Failed to register desktop VM in hydra.vms").strip()

    return {
        "name": vm_name,
        "vdisk_id": vdisk_id,
        "pool_name": pool_name,
        "template_vm": template_vm,
        "vcpu": vcpu,
        "memory_mb": memory_mb,
        "node": host_ip,
        "status": "RUNNING",
    }, None


def broker_desktop_session(
    pool_name,
    username,
    user_groups=None,
    client_ip="",
    default_host_ip="127.0.0.1",
    sidon_clone_fn=None,
    run_query=None,
    secret=None,
    now_ms=None,
):
    """Broker a VDI desktop session for `username` on `pool_name`.

    - Verifies pool existence and user/group entitlement.
    - Reconnects an existing active session for `(pool_name, username)` if present.
    - Otherwise assigns an idle desktop VM in the pool or provisions a new linked clone
      up to `max_desktops`.
    - Records the session in `hydra.horizon_sessions` and returns a signed Guacamole token.
    """
    runner = run_query or run_cql_query
    pool, err = get_db_pool(pool_name, run_query=runner)
    if err:
        return None, err
    if not pool:
        return None, f"VDI pool '{pool_name}' not found"

    if not user_is_entitled(username, user_groups, pool):
        return None, f"User '{username}' is not entitled to VDI pool '{pool_name}'"

    sessions, serr = list_db_sessions(pool_name=pool_name, run_query=runner)
    if serr:
        return None, serr
    active_sessions = [s for s in (sessions or []) if s.get("state") == "CONNECTED"]

    ts_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    signing_secret = secret or get_horizon_signing_secret()
    if not signing_secret:
        return None, "Horizon session signing secret is unavailable"

    # 1. Check if this user already has a connected session in this pool
    for existing in active_sessions:
        if existing.get("username") == username:
            sid = existing["session_id"]
            runner(
                f"UPDATE hydra.horizon_sessions SET last_active_ms = {ts_ms}, "
                f"client_ip = '{cql_escape(str(client_ip or ''))}' "
                f"WHERE session_id = '{cql_escape(sid)}';"
            )
            existing["last_active_ms"] = ts_ms
            spec = build_guacamole_connection_spec(
                existing, pool, target_host=existing.get("host_ip") or default_host_ip
            )
            token = sign_horizon_session_token(spec, signing_secret, now_s=ts_ms // 1000)
            return {
                "session": existing,
                "token": token,
                "websocket_path": f"/ws/horizon/{sid}?token={token}",
                "reconnected": True,
            }, None

    # 2. Find an unassigned desktop VM or provision a new linked clone
    vms, verr = list_pool_desktop_vms(pool_name, run_query=runner)
    if verr:
        return None, verr
    busy_vms = {s.get("vm_name") for s in active_sessions if s.get("vm_name")}
    chosen_vm = None
    for vm in (vms or []):
        if vm.get("name") not in busy_vms:
            chosen_vm = vm
            break

    if chosen_vm is None:
        max_desktops = int(pool.get("max_desktops") or 10)
        if len(vms or []) >= max_desktops:
            return None, f"VDI pool '{pool_name}' is at capacity ({max_desktops} desktops)"
        next_idx = len(vms or []) + 1
        chosen_vm, cerr = provision_desktop_clone(
            pool,
            next_idx,
            host_ip=default_host_ip,
            sidon_clone_fn=sidon_clone_fn,
            run_query=runner,
        )
        if cerr:
            return None, cerr

    session_id = "hzn-" + uuid.uuid4().hex[:20]
    host_ip = str(chosen_vm.get("node") or default_host_ip)
    guest_ips = str(chosen_vm.get("guest_ips") or "").strip()
    target_host = guest_ips.split(",")[0].strip() if guest_ips else host_ip
    protocol = str(pool.get("protocol") or "rdp")
    guacd_port = GUACD_DEFAULT_PORT

    session_row = {
        "session_id": session_id,
        "pool_name": pool_name,
        "vm_name": chosen_vm["name"],
        "username": username,
        "protocol": protocol,
        "host_ip": host_ip,
        "guacd_port": guacd_port,
        "client_ip": str(client_ip or ""),
        "state": "CONNECTED",
        "connected_at_ms": ts_ms,
        "last_active_ms": ts_ms,
    }
    cql = (
        "INSERT INTO hydra.horizon_sessions ("
        "session_id, pool_name, vm_name, username, protocol, host_ip, "
        "guacd_port, client_ip, state, connected_at_ms, last_active_ms"
        ") VALUES ("
        f"'{cql_escape(session_id)}', '{cql_escape(pool_name)}', '{cql_escape(chosen_vm['name'])}', "
        f"'{cql_escape(username)}', '{cql_escape(protocol)}', '{cql_escape(host_ip)}', "
        f"{guacd_port}, '{cql_escape(str(client_ip or ''))}', 'CONNECTED', {ts_ms}, {ts_ms});"
    )
    rc, out, qerr = runner(cql)
    if rc != 0:
        return None, (qerr or out or "Failed to record session in hydra.horizon_sessions").strip()

    spec = build_guacamole_connection_spec(session_row, pool, target_host=target_host)
    token = sign_horizon_session_token(spec, signing_secret, now_s=ts_ms // 1000)
    return {
        "session": session_row,
        "token": token,
        "websocket_path": f"/ws/horizon/{session_id}?token={token}",
        "reconnected": False,
    }, None


def disconnect_session(session_id, delete_instant_clone=False, sidon_delete_fn=None, run_query=None):
    """Disconnect a VDI session and optionally reclaim its non-persistent instant clone."""
    runner = run_query or run_cql_query
    sess, err = get_db_session(session_id, run_query=runner)
    if err:
        return False, err
    if not sess:
        return False, f"Session '{session_id}' not found"

    rc, out, qerr = runner(
        f"DELETE FROM hydra.horizon_sessions WHERE session_id = '{cql_escape(session_id)}';"
    )
    if rc != 0:
        return False, (qerr or out or "Failed to delete session record").strip()

    if delete_instant_clone and sess.get("vm_name"):
        vm_name = sess["vm_name"]
        vdisk_id = f"{vm_name}-disk0"
        if sidon_delete_fn is None:
            try:
                import helios_sidon
                helios_sidon.delete_vdisk(vdisk_id)
            except Exception:
                pass
        else:
            try:
                sidon_delete_fn(vdisk_id)
            except Exception:
                pass
        runner(f"DELETE FROM hydra.vms WHERE name = '{cql_escape(vm_name)}';")

    return True, None
