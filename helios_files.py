#!/usr/bin/env python3
"""Helios Files: Scale-out HA NFSv4 and SMB3 file services backed by Sidon DFS vdisks.

Every file share is backed by a replicated Sidon vdisk (`files-<share_name>`) whose
ownership is governed by Sidon's `(owner, epoch)` compare-and-swap in ScyllaDB (`hydra.dfs_vdisks`).
Only the active owner host attaches the vdisk, mounts its XFS filesystem under
`/srv/helios-files/<share_name>`, and exports it over NFSv4 (`/etc/exports.d/`) and/or
SMB3 (`/etc/samba/helios-shares.d/`). On host failure or maintenance evacuation, a
surviving NORMAL host claims the Sidon vdisk epoch, fences stale replicas, mounts XFS,
and resumes the export.
"""

import ipaddress
import json
import os
import re
import subprocess
import time

from helios_cql import cql_escape, run_cql_query

SHARE_NAME_RE = re.compile(r"\A[a-z0-9][a-z0-9_-]{0,46}\Z")
VALID_PROTOCOLS = ("nfs", "smb", "dual")
VALID_SQUASH_MODES = ("root_squash", "no_root_squash", "all_squash")
FILES_MOUNT_ROOT = "/srv/helios-files"
NFS_EXPORTS_DIR = "/etc/exports.d"
SAMBA_SHARES_DIR = "/etc/samba/helios-shares.d"
SAMBA_MAIN_INCL = "/etc/samba/helios-shares.conf"
SIDON_NBD_DIR = "/var/lib/hci/sidon/nbd"


def validate_share_name(name):
    """Validate a share name (lowercase alphanumeric, '-', '_', 1..47 chars)."""
    if not isinstance(name, str) or not SHARE_NAME_RE.match(name):
        return None, (
            "share_name must be 1..47 characters of lowercase letters, digits, '-' or '_', "
            "starting with a letter or digit"
        )
    return name, None


def validate_protocol(protocol):
    """Validate share protocol ('nfs', 'smb', or 'dual')."""
    val = str(protocol or "nfs").strip().lower()
    if val not in VALID_PROTOCOLS:
        return None, "protocol must be one of " + ", ".join(VALID_PROTOCOLS)
    return val, None


def validate_squash(squash_mode):
    """Validate NFS squash mode."""
    val = str(squash_mode or "root_squash").strip().lower()
    if val not in VALID_SQUASH_MODES:
        return None, "squash_mode must be one of " + ", ".join(VALID_SQUASH_MODES)
    return val, None


def validate_allowed_clients(clients_raw):
    """Validate allowed client specification ('*' or comma-separated IPv4/CIDR entries)."""
    text = str(clients_raw or "*").strip()
    if not text or text == "*":
        return "*", None
    normalized = []
    for part in text.split(","):
        token = part.strip()
        if not token:
            continue
        if token == "*":
            return "*", None
        try:
            if "/" in token:
                net = ipaddress.ip_network(token, strict=False)
                normalized.append(str(net))
            else:
                addr = ipaddress.ip_address(token)
                normalized.append(str(addr))
        except ValueError as exc:
            return None, f"Invalid client address or CIDR '{token}': {exc}"
    if not normalized:
        return "*", None
    return ",".join(normalized), None


def vdisk_id_for_share(share_name):
    """Deterministic Sidon vdisk identifier for a Helios Files share."""
    return f"files-{share_name}"


def export_path_for_share(share_name):
    """Canonical mount and export directory on the active file server host."""
    return f"{FILES_MOUNT_ROOT}/{share_name}"


def render_nfs_export_line(share_name, export_path, allowed_clients="*", squash_mode="root_squash"):
    """Render `/etc/exports.d/helios-<share_name>.exports` content."""
    squash, err = validate_squash(squash_mode)
    if err:
        raise ValueError(err)
    clients, err = validate_allowed_clients(allowed_clients)
    if err:
        raise ValueError(err)
    # Deterministic fsid derived from share_name so NFSv4 filehandles remain stable across HA failover
    fsid = (abs(hash(share_name)) % 90000) + 10000
    opts = f"rw,sync,no_subtree_check,{squash},fsid={fsid}"
    specs = " ".join(f"{client}({opts})" for client in clients.split(","))
    return f"{export_path} {specs}\n"


def render_samba_share_conf(share_name, export_path, allowed_clients="*", guest_ok=False):
    """Render `/etc/samba/helios-shares.d/<share_name>.conf` section."""
    clients, err = validate_allowed_clients(allowed_clients)
    if err:
        raise ValueError(err)
    lines = [
        f"[{share_name}]",
        f"   path = {export_path}",
        "   browseable = yes",
        "   read only = no",
        "   writable = yes",
        f"   guest ok = {'yes' if guest_ok else 'no'}",
        "   create mask = 0664",
        "   directory mask = 0775",
        "   vfs objects = acl_xattr",
    ]
    if clients != "*":
        hosts_allow = " ".join(c.strip() for c in clients.split(",") if c.strip())
        lines.append(f"   hosts allow = {hosts_allow}")
    return "\n".join(lines) + "\n"


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


def list_db_shares(run_query=None):
    """Read all shares from hydra.files_shares."""
    runner = run_query or run_cql_query
    rc, stdout, err = runner("SELECT JSON * FROM hydra.files_shares;")
    if rc != 0:
        return None, (err or stdout or "Failed to read hydra.files_shares").strip()
    shares = parse_json_rows(stdout)
    shares.sort(key=lambda s: str(s.get("share_name") or ""))
    return shares, None


def get_db_share(share_name, run_query=None):
    """Read a single share row from hydra.files_shares."""
    name, err = validate_share_name(share_name)
    if err:
        return None, err
    runner = run_query or run_cql_query
    rc, stdout, qerr = runner(
        f"SELECT JSON * FROM hydra.files_shares WHERE share_name = '{cql_escape(name)}';"
    )
    if rc != 0:
        return None, (qerr or stdout or "Failed to query hydra.files_shares").strip()
    rows = parse_json_rows(stdout)
    if not rows:
        return None, None
    return rows[0], None


def inspect_local_share_mount(share_name, export_path=None):
    """Read authentic kernel mount state and filesystem capacity for `export_path`.

    Never invents or estimates capacity numbers: returns `mounted: False` and `None`
    byte counters when the share is not mounted on this host.
    """
    target_path = export_path or export_path_for_share(share_name)
    mounted = False
    device = None
    fstype = None
    try:
        if os.path.exists("/proc/mounts"):
            with open("/proc/mounts", "r", encoding="utf-8") as mf:
                for line in mf:
                    parts = line.split()
                    if len(parts) >= 3 and parts[1] == target_path:
                        mounted = True
                        device = parts[0]
                        fstype = parts[2]
                        break
    except OSError:
        mounted = False

    total_bytes = None
    used_bytes = None
    avail_bytes = None
    if mounted and hasattr(os, "statvfs"):
        try:
            st = os.statvfs(target_path)
            total_bytes = int(st.f_frsize * st.f_blocks)
            avail_bytes = int(st.f_frsize * st.f_bavail)
            used_bytes = max(0, total_bytes - int(st.f_frsize * st.f_bfree))
        except OSError:
            pass

    return {
        "share_name": share_name,
        "export_path": target_path,
        "mounted": mounted,
        "device": device,
        "fstype": fstype,
        "total_bytes": total_bytes,
        "used_bytes": used_bytes,
        "avail_bytes": avail_bytes,
    }


def _find_free_nbd_device():
    """Find an unattached /dev/nbdX block device on the host."""
    for idx in range(16):
        dev = f"/dev/nbd{idx}"
        sys_pid = f"/sys/block/nbd{idx}/pid"
        if os.path.exists(dev) and not os.path.exists(sys_pid):
            return dev
    return None


def _nbd_device_for_share(export_path):
    """Return the /dev/nbdX currently mounted at `export_path`, if any."""
    try:
        if os.path.exists("/proc/mounts"):
            with open("/proc/mounts", "r", encoding="utf-8") as mf:
                for line in mf:
                    parts = line.split()
                    if len(parts) >= 2 and parts[1] == export_path and parts[0].startswith("/dev/nbd"):
                        return parts[0]
    except OSError:
        pass
    return None


def activate_share_on_host(share, run_argv_fn=None):
    """Mount the Sidon-backed vdisk and configure NFS/SMB exports on the local host."""
    if run_argv_fn is None:
        def run_argv_fn(argv, timeout=60):
            try:
                res = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
                return res.returncode, res.stdout.decode("utf-8", errors="ignore"), res.stderr.decode("utf-8", errors="ignore")
            except Exception as exc:
                return -1, "", str(exc)

    name, err = validate_share_name(share.get("share_name"))
    if err:
        return False, err
    protocol, err = validate_protocol(share.get("protocol", "nfs"))
    if err:
        return False, err
    squash, err = validate_squash(share.get("squash_mode", "root_squash"))
    if err:
        return False, err
    clients, err = validate_allowed_clients(share.get("allowed_clients", "*"))
    if err:
        return False, err

    vdisk_id = str(share.get("vdisk_id") or vdisk_id_for_share(name))
    export_path = str(share.get("export_path") or export_path_for_share(name))
    nbd_sock = f"{SIDON_NBD_DIR}/{vdisk_id}"

    try:
        os.makedirs(export_path, mode=0o775, exist_ok=True)
    except OSError as exc:
        return False, f"Could not create export directory {export_path}: {exc}"

    mount_info = inspect_local_share_mount(name, export_path)
    if not mount_info["mounted"] and os.path.exists(nbd_sock):
        run_argv_fn(["modprobe", "nbd", "max_part=8"], timeout=15)
        nbd_dev = _find_free_nbd_device()
        if nbd_dev:
            rc_c, _, err_c = run_argv_fn(
                ["qemu-nbd", "--connect", nbd_dev, "--format=raw", f"nbd+unix:///{vdisk_id}?socket={nbd_sock}"],
                timeout=30,
            )
            if rc_c == 0:
                rc_blkid, out_blkid, _ = run_argv_fn(["blkid", "-o", "value", "-s", "TYPE", nbd_dev], timeout=15)
                if rc_blkid != 0 or not (out_blkid or "").strip():
                    run_argv_fn(["mkfs.xfs", "-f", "-L", name[:12], nbd_dev], timeout=60)
                run_argv_fn(["mount", nbd_dev, export_path], timeout=30)

    # Write NFS export configuration if protocol is nfs or dual
    nfs_file = os.path.join(NFS_EXPORTS_DIR, f"helios-{name}.exports")
    if protocol in ("nfs", "dual"):
        try:
            os.makedirs(NFS_EXPORTS_DIR, mode=0o755, exist_ok=True)
            with open(nfs_file, "w", encoding="utf-8") as nf:
                nf.write(render_nfs_export_line(name, export_path, clients, squash))
            run_argv_fn(["exportfs", "-ra"], timeout=30)
        except OSError as exc:
            return False, f"Failed writing NFS export {nfs_file}: {exc}"
    else:
        if os.path.exists(nfs_file):
            try:
                os.remove(nfs_file)
                run_argv_fn(["exportfs", "-ra"], timeout=30)
            except OSError:
                pass

    # Write Samba share configuration if protocol is smb or dual
    smb_file = os.path.join(SAMBA_SHARES_DIR, f"{name}.conf")
    if protocol in ("smb", "dual"):
        try:
            os.makedirs(SAMBA_SHARES_DIR, mode=0o755, exist_ok=True)
            with open(smb_file, "w", encoding="utf-8") as sf:
                sf.write(render_samba_share_conf(name, export_path, clients, bool(share.get("smb_guest_ok"))))
            _rebuild_samba_master_include()
            run_argv_fn(["smbcontrol", "all", "reload-config"], timeout=20)
        except OSError as exc:
            return False, f"Failed writing SMB config {smb_file}: {exc}"
    else:
        if os.path.exists(smb_file):
            try:
                os.remove(smb_file)
                _rebuild_samba_master_include()
                run_argv_fn(["smbcontrol", "all", "reload-config"], timeout=20)
            except OSError:
                pass

    return True, ""


def deactivate_share_on_host(share_name, export_path=None, run_argv_fn=None):
    """Remove NFS/SMB exports, unmount `export_path`, and disconnect `/dev/nbdX`."""
    if run_argv_fn is None:
        def run_argv_fn(argv, timeout=60):
            try:
                res = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
                return res.returncode, res.stdout.decode("utf-8", errors="ignore"), res.stderr.decode("utf-8", errors="ignore")
            except Exception as exc:
                return -1, "", str(exc)

    name, err = validate_share_name(share_name)
    if err:
        return False, err
    target_path = export_path or export_path_for_share(name)

    nfs_file = os.path.join(NFS_EXPORTS_DIR, f"helios-{name}.exports")
    if os.path.exists(nfs_file):
        try:
            os.remove(nfs_file)
        except OSError:
            pass
        run_argv_fn(["exportfs", "-ra"], timeout=30)

    smb_file = os.path.join(SAMBA_SHARES_DIR, f"{name}.conf")
    if os.path.exists(smb_file):
        try:
            os.remove(smb_file)
            _rebuild_samba_master_include()
        except OSError:
            pass
        run_argv_fn(["smbcontrol", "all", "reload-config"], timeout=20)

    nbd_dev = _nbd_device_for_share(target_path)
    mount_info = inspect_local_share_mount(name, target_path)
    if mount_info["mounted"]:
        run_argv_fn(["umount", "-l", target_path], timeout=30)
    if nbd_dev:
        run_argv_fn(["qemu-nbd", "--disconnect", nbd_dev], timeout=20)

    return True, ""


def _rebuild_samba_master_include():
    """Rebuild `/etc/samba/helios-shares.conf` from `/etc/samba/helios-shares.d/*.conf`."""
    if not os.path.isdir(SAMBA_SHARES_DIR):
        return
    blocks = []
    for entry in sorted(os.listdir(SAMBA_SHARES_DIR)):
        if not entry.endswith(".conf"):
            continue
        p = os.path.join(SAMBA_SHARES_DIR, entry)
        try:
            with open(p, "r", encoding="utf-8") as fh:
                blocks.append(fh.read().strip())
        except OSError:
            continue
    parent = os.path.dirname(SAMBA_MAIN_INCL)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(SAMBA_MAIN_INCL, "w", encoding="utf-8") as out:
        out.write("# Managed by Helios Files\n\n" + "\n\n".join(blocks) + "\n")
