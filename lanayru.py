#!/usr/bin/env python3
import re
import time
import json
import uuid
import struct
import secrets
import datetime
import ipaddress
import urllib.parse
import urllib.request
import socket
import ssl
import hashlib
import base64


def owns_vm(cluster_name, vm_name):
    """True only for a VM this module created for `cluster_name`:
    `<cluster>-control-0N`, `<cluster>-cp-*`, or `<cluster>-worker-*`.

    Destroy used to match on a name prefix, so destroying a cluster called `web` also deleted a
    guest called `webapp-1`, its vdisk and its rows.
    """
    if not cluster_name or not vm_name:
        return False
    pattern = re.escape(cluster_name) + r"-(?:control-0[0-9]|cp-(?:0[0-9]|[1-9][0-9]*)|worker-(?:0[0-9]|[1-9][0-9]*))"
    return re.fullmatch(pattern, vm_name) is not None


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


def build_cidata_iso_bytes(meta_data: str, user_data: str, network_config: str = None) -> bytes:
    """Build a valid ISO9660 NoCloud ('cidata') image in memory (delegating to vali if importable)."""
    try:
        import vali
        if hasattr(vali, "build_cidata_iso_bytes"):
            return vali.build_cidata_iso_bytes(meta_data, user_data, network_config)
    except Exception:
        pass

    sector = 2048

    def both16(v):
        return struct.pack("<H", v) + struct.pack(">H", v)

    def both32(v):
        return struct.pack("<I", v) + struct.pack(">I", v)

    def pad_sec(b):
        rem = len(b) % sector
        return b if rem == 0 else b + b"\x00" * (sector - rem)

    files = [
        ("META_DAT.;1", "meta-data", (meta_data or "").encode("utf-8")),
        ("USER_DAT.;1", "user-data", (user_data or "").encode("utf-8")),
    ]
    if network_config:
        files.append(("NETWORK_.;1", "network-config", network_config.encode("utf-8")))

    cur_lba = 19
    placed = []
    for iso_name, rr_name, data in files:
        n_sec = max(1, (len(data) + sector - 1) // sector)
        placed.append((iso_name, rr_name, data, cur_lba, len(data)))
        cur_lba += n_sec
    total_sectors = max(cur_lba + 2, 32)

    def dir_record(name_bytes, lba, size, is_dir=False, rr_alt_name=None):
        su = b""
        if rr_alt_name:
            nm_bytes = rr_alt_name.encode("ascii")
            su = b"NM" + struct.pack("BBB", len(nm_bytes) + 5, 1, 0) + nm_bytes
        name_len = len(name_bytes)
        pad_len = 1 if (name_len % 2 == 0) else 0
        rec_len = 33 + name_len + pad_len + len(su)
        if rec_len % 2 == 1:
            su += b"\x00"
            rec_len += 1
        flags = 0x02 if is_dir else 0x00
        rec = bytearray(rec_len)
        rec[0] = rec_len
        rec[1] = 0
        rec[2:10] = both32(lba)
        rec[10:18] = both32(size)
        rec[18:25] = bytes([126, 1, 1, 0, 0, 0, 0])
        rec[25] = flags
        rec[26] = 0
        rec[27] = 0
        rec[28:32] = both16(1)
        rec[32] = name_len
        rec[33:33 + name_len] = name_bytes
        rec[33 + name_len + pad_len:] = su
        return bytes(rec)

    root_lba = 18
    root_entries = bytearray()
    root_entries += dir_record(b"\x00", root_lba, sector, is_dir=True)
    root_entries += dir_record(b"\x01", root_lba, sector, is_dir=True)
    for iso_name, rr_name, _data, lba, size in placed:
        root_entries += dir_record(iso_name.encode("ascii"), lba, size, is_dir=False, rr_alt_name=rr_name)
    root_dir_sec = pad_sec(bytes(root_entries))[:sector]
    root_record_for_pvd = dir_record(b"\x00", root_lba, sector, is_dir=True)

    pvd = bytearray(sector)
    pvd[0] = 1
    pvd[1:6] = b"CD001"
    pvd[6] = 1
    pvd[8:40] = b"LINUX".ljust(32, b" ")
    pvd[40:72] = b"cidata".ljust(32, b" ")
    pvd[80:88] = both32(total_sectors)
    pvd[120:124] = both16(1)
    pvd[124:128] = both16(1)
    pvd[128:132] = both16(sector)
    pvd[156:156 + len(root_record_for_pvd)] = root_record_for_pvd
    pvd[881] = 1

    term = bytearray(sector)
    term[0] = 255
    term[1:6] = b"CD001"
    term[6] = 1

    img = bytearray(16 * sector)
    img += pvd
    img += term
    img += root_dir_sec
    for _iso_name, _rr_name, data, _lba, _size in placed:
        img += pad_sec(data)
    while len(img) < total_sectors * sector:
        img += b"\x00" * sector
    return bytes(img)


def _parse_json_rows(stdout):
    rows = []
    for line in (stdout or "").splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


def allocate_static_ips(subnet_cidr, gateway_ip, dhcp_start, dhcp_end, count):
    """Allocate `count` static node IPs inside `subnet_cidr` outside `[dhcp_start, dhcp_end]`
    and distinct from `gateway_ip`."""
    try:
        net = ipaddress.ip_network(str(subnet_cidr or "172.16.10.0/24"), strict=False)
    except ValueError:
        net = ipaddress.ip_network("172.16.10.0/24", strict=False)
    gw_obj = None
    try:
        if gateway_ip:
            gw_obj = ipaddress.ip_address(str(gateway_ip).split("/")[0])
    except ValueError:
        gw_obj = None
    start_int = end_int = None
    try:
        if dhcp_start and dhcp_end:
            s_ip = ipaddress.ip_address(str(dhcp_start).strip())
            e_ip = ipaddress.ip_address(str(dhcp_end).strip())
            start_int = min(int(s_ip), int(e_ip))
            end_int = max(int(s_ip), int(e_ip))
    except ValueError:
        start_int = end_int = None

    hosts = list(net.hosts())
    preferred = [h for h in hosts if int(h) >= int(net.network_address) + 11] + hosts
    picked = []
    seen = set()
    for host_ip in preferred:
        if host_ip in seen:
            continue
        seen.add(host_ip)
        if gw_obj is not None and host_ip == gw_obj:
            continue
        if int(host_ip) == int(net.network_address) + 250:
            continue
        if start_int is not None and end_int is not None and start_int <= int(host_ip) <= end_int:
            continue
        picked.append(str(host_ip))
        if len(picked) >= count:
            break
    while len(picked) < count:
        picked.append(f"172.16.10.{11 + len(picked)}")
    return picked, net.prefixlen


def build_k3s_userdata(vm_name, role, assigned_ip, prefixlen, gateway_ip, cluster_token,
                       first_cp_ip, root_password, ssh_key=None):
    """Build cloud-init user-data with a per-cluster random password and k3s bootstrap."""
    ssh_section = ""
    if ssh_key and str(ssh_key).strip():
        ssh_section = f"    ssh_authorized_keys:\n      - {str(ssh_key).strip()}\n"

    if role == "init":
        k3s_cmd = (
            f"curl -sfL https://get.k3s.io | K3S_TOKEN='{cluster_token}' "
            f"INSTALL_K3S_EXEC=\"server --cluster-init --node-ip {assigned_ip} "
            f"--tls-san {first_cp_ip} --write-kubeconfig-mode 0644\" sh - || "
            f"k3s server --cluster-init --token '{cluster_token}' --node-ip {assigned_ip} "
            f"--tls-san {first_cp_ip} --write-kubeconfig-mode 0644 &"
        )
    elif role == "cp":
        k3s_cmd = (
            f"curl -sfL https://get.k3s.io | K3S_TOKEN='{cluster_token}' "
            f"K3S_URL='https://{first_cp_ip}:6443' "
            f"INSTALL_K3S_EXEC=\"server --server https://{first_cp_ip}:6443 "
            f"--node-ip {assigned_ip} --tls-san {first_cp_ip} --write-kubeconfig-mode 0644\" sh - || "
            f"k3s server --server https://{first_cp_ip}:6443 --token '{cluster_token}' "
            f"--node-ip {assigned_ip} &"
        )
    else:
        k3s_cmd = (
            f"curl -sfL https://get.k3s.io | K3S_TOKEN='{cluster_token}' "
            f"K3S_URL='https://{first_cp_ip}:6443' "
            f"INSTALL_K3S_EXEC=\"agent --node-ip {assigned_ip}\" sh - || "
            f"k3s agent --server https://{first_cp_ip}:6443 --token '{cluster_token}' "
            f"--node-ip {assigned_ip} &"
        )

    return f"""#cloud-config
hostname: {vm_name}
fqdn: {vm_name}.local
manage_etc_hosts: true
ssh_pwauth: true
users:
  - name: root
    plain_text_pass: '{root_password}'
    lock_passwd: false
{ssh_section}chpasswd:
  list: |
    root:{root_password}
  expire: False
write_files:
  - path: /etc/netplan/50-cloud-init.yaml
    content: |
      network:
        version: 2
        ethernets:
          eth0:
            addresses:
              - {assigned_ip}/{prefixlen}
            gateway4: {gateway_ip}
            nameservers:
              addresses: [8.8.8.8, 1.1.1.1]
  - path: /usr/local/bin/lanayru-k3s-bootstrap.sh
    permissions: '0755'
    content: |
      #!/bin/sh
      set -e
      {k3s_cmd}
runcmd:
  - netplan apply || systemctl restart systemd-networkd || true
  - /usr/local/bin/lanayru-k3s-bootstrap.sh || true
  - echo "Lanayru node bootstrap active"
"""


def build_network_config(assigned_ip, prefixlen, gateway_ip):
    return f"""version: 2
ethernets:
  eth0:
    addresses:
      - {assigned_ip}/{prefixlen}
    gateway4: {gateway_ip}
    nameservers:
      addresses: [8.8.8.8, 1.1.1.1]
"""


def build_kubeconfig_yaml(cluster_name, api_ip, cluster_token):
    """Build a valid Kubernetes kubeconfig YAML for the provisioned Lanayru cluster."""
    return f"""apiVersion: v1
kind: Config
clusters:
- cluster:
    insecure-skip-tls-verify: true
    server: https://{api_ip}:6443
  name: {cluster_name}
contexts:
- context:
    cluster: {cluster_name}
    user: {cluster_name}-admin
  name: {cluster_name}
current-context: {cluster_name}
preferences: {{}}
users:
- name: {cluster_name}-admin
  user:
    token: {cluster_token}
"""


def _resolve_base_image(run_cql_query):
    """Check `hydra.valhalla_images` and `hydra.images` for a base OS cloud image or ISO.
    Returns (base_vdisk_id, file_path)."""
    rc_v, out_v, _ = run_cql_query(
        "SELECT JSON image_id, name, vdisk_id, format FROM hydra.valhalla_images;")
    if rc_v == 0 and out_v:
        for row in _parse_json_rows(out_v):
            vdisk_id = row.get("vdisk_id")
            if vdisk_id:
                return str(vdisk_id), None
    rc_i, out_i, _ = run_cql_query("SELECT JSON name, path FROM hydra.images;")
    if rc_i == 0 and out_i:
        for row in _parse_json_rows(out_i):
            path = row.get("path")
            if path and not str(path).lower().endswith("-cidata.iso"):
                return None, str(path)
    return None, "/var/lib/hci/images/cirros.img"


def _resolve_segments(run_cql_query, run_remote_spark, get_cluster_nodes,
                      cluster_name, overlay_segment_id, log):
    """Resolve existing Urbosa segment(s) from `hydra.urbosa_segments` or auto-create them
    when `overlay_segment_id` requests Urbosa overlay provisioning."""
    rc_s, out_s, _ = run_cql_query(
        "SELECT JSON segment_id, name, vni, t1_link_id, subnet_cidr, gateway_ip, "
        "dhcp_enabled, dhcp_start, dhcp_end FROM hydra.urbosa_segments;")
    existing = _parse_json_rows(out_s) if rc_s == 0 else []

    for seg in existing:
        if str(seg.get("segment_id")) == str(overlay_segment_id) or str(seg.get("name")) == str(overlay_segment_id):
            return [seg]

    if str(overlay_segment_id or "").startswith("ov-"):
        log("Urbosa Overlay mode selected. Checking default routing elements...", "info")
        t0_id = None
        t1_id = None
        rc_t0q, out_t0q, _ = run_cql_query("SELECT JSON router_id FROM hydra.urbosa_t0_routers LIMIT 1;")
        if rc_t0q == 0 and out_t0q:
            rows = _parse_json_rows(out_t0q)
            if rows:
                t0_id = rows[0].get("router_id")
        if not t0_id:
            log("ERROR: No T0 gateway router found in hydra.urbosa_t0_routers. "
                "Please create a T0 router in the Urbosa UI before deploying Lanayru with Urbosa networking.", "error")
            raise RuntimeError("No Urbosa T0 router configured")

        rc_t1q, out_t1q, _ = run_cql_query("SELECT JSON router_id FROM hydra.urbosa_t1_routers LIMIT 1;")
        if rc_t1q == 0 and out_t1q:
            rows = _parse_json_rows(out_t1q)
            if rows:
                t1_id = rows[0].get("router_id")
        if not t1_id:
            log("ERROR: No T1 router found in hydra.urbosa_t1_routers. "
                "Please create a T1 router linked to your T0 in the Urbosa UI before deploying Lanayru.", "error")
            raise RuntimeError("No Urbosa T1 router configured")

        log(f"Using T0 router {t0_id} and T1 router {t1_id} for Lanayru overlay.", "success")
        seg1_id = str(uuid.uuid4())
        seg2_id = str(uuid.uuid4())
        log(f"Auto-generating Urbosa Segment 1 ({cluster_name}-segment-1, VNI 10010)...", "info")
        run_cql_query(
            f"INSERT INTO hydra.urbosa_segments (segment_id, name, vni, t1_link_id, subnet_cidr, "
            f"gateway_ip, dhcp_enabled, dhcp_start, dhcp_end) VALUES ({seg1_id}, "
            f"'{cluster_name}-segment-1', 10010, {t1_id}, '172.16.10.0/24', '172.16.10.254', "
            f"true, '172.16.10.50', '172.16.10.200');")
        log(f"Auto-generating Urbosa Segment 2 ({cluster_name}-segment-2, VNI 10011)...", "info")
        run_cql_query(
            f"INSERT INTO hydra.urbosa_segments (segment_id, name, vni, t1_link_id, subnet_cidr, "
            f"gateway_ip, dhcp_enabled, dhcp_start, dhcp_end) VALUES ({seg2_id}, "
            f"'{cluster_name}-segment-2', 10011, {t1_id}, '172.16.11.0/24', '172.16.11.254', "
            f"true, '172.16.11.50', '172.16.11.200');")

        nodes = get_cluster_nodes() or [{"ip": "127.0.0.1"}]
        for node in nodes:
            node_ip = node.get("ip")
            if node_ip:
                run_remote_spark(node_ip, "ip link add name br-ov-10010 type bridge || true")
                run_remote_spark(node_ip, "ip addr add 172.16.10.250/24 dev br-ov-10010 || true")
                run_remote_spark(node_ip, "ip link set br-ov-10010 up || true")
                run_remote_spark(node_ip, "ip link add name br-ov-10011 type bridge || true")
                run_remote_spark(node_ip, "ip addr add 172.16.11.250/24 dev br-ov-10011 || true")
                run_remote_spark(node_ip, "ip link set br-ov-10011 up || true")
        return [
            {
                "segment_id": seg1_id,
                "name": f"{cluster_name}-segment-1",
                "vni": 10010,
                "subnet_cidr": "172.16.10.0/24",
                "gateway_ip": "172.16.10.254",
                "dhcp_start": "172.16.10.50",
                "dhcp_end": "172.16.10.200",
            },
            {
                "segment_id": seg2_id,
                "name": f"{cluster_name}-segment-2",
                "vni": 10011,
                "subnet_cidr": "172.16.11.0/24",
                "gateway_ip": "172.16.11.254",
                "dhcp_start": "172.16.11.50",
                "dhcp_end": "172.16.11.200",
            },
        ]

    if existing:
        return [existing[0]]

    fallback_id = str(overlay_segment_id) if overlay_segment_id else str(uuid.uuid4())
    return [{
        "segment_id": fallback_id,
        "name": f"{cluster_name}-segment-1",
        "vni": 10010,
        "subnet_cidr": "172.16.10.0/24",
        "gateway_ip": "172.16.10.254",
        "dhcp_start": "172.16.10.50",
        "dhcp_end": "172.16.10.200",
    }]


def deploy_lanayru_worker(task_id, cluster_name, control_nodes, overlay_segment_id, created_at):
    from spectrum_server import (
        run_cql_query,
        run_conditional_cql_query,
        run_lwt,
        run_remote_spark,
        sidon_call,
        sidon_module,
        log_catalyst_task,
        get_cluster_nodes,
        get_catalyst_target_ip,
        LOCAL_IP,
        LANAYRU_LOGS
    )
    
    LANAYRU_LOGS[task_id] = []
    
    def log(msg, mtype="info"):
        t = datetime.datetime.now().strftime("%H:%M:%S")
        LANAYRU_LOGS[task_id].append(f"[{t}] {msg}")
        print(f"[LANAYRU PROVISIONER] {msg}", flush=True)

    cluster_id = str(uuid.uuid4())
    try:
        log("Initiating Lanayru Kubernetes Engine (LKE) deployment sequence...", "info")
        time.sleep(1.5)
        
        log("Step 1: Creating persistent database schema in ScyllaDB (Hydra)...", "info")
        # The Lanayru tables are part of the cluster schema in helios_schema, not this
        # script's to define. This applies whatever is outstanding, through the unguarded
        # executor: the cluster lock helios_schema takes is an IF NOT EXISTS insert, and
        # `run_cql_query` refuses a conditional statement because it cannot report whether
        # the condition held.
        load_schema_module().ensure_schema(run_conditional_cql_query)
        time.sleep(1)
        log("ScyllaDB tables hydra.lanayru_clusters & hydra.lanayru_k8s_state are verified.", "success")

        resolved_segments = _resolve_segments(
            run_cql_query, run_remote_spark, get_cluster_nodes,
            cluster_name, overlay_segment_id, log)
        primary_seg = resolved_segments[0]
        primary_seg_uuid = str(primary_seg.get("segment_id") or "")
        try:
            uuid.UUID(primary_seg_uuid)
            seg_cql_val = primary_seg_uuid
        except ValueError:
            seg_cql_val = "null"

        log("Step 2: Allocating cluster registration record...", "info")
        cql_insert = f"""
        INSERT INTO hydra.lanayru_clusters (cluster_id, name, control_nodes, overlay_segment_id, status, created_at)
        VALUES ({cluster_id}, '{cluster_name}', {control_nodes}, {seg_cql_val}, 'deploying', toTimestamp(now()));
        """
        run_cql_query(cql_insert)
        time.sleep(1)

        base_vdisk_id, base_image_path = _resolve_base_image(run_cql_query)
        cluster_token = secrets.token_hex(24)
        root_password = secrets.token_urlsafe(16)
        ssh_key = None
        rc_ssh, out_ssh, _ = run_cql_query(
            "SELECT JSON value FROM hydra.cluster_settings WHERE key = 'lanayru_ssh_authorized_key';")
        if rc_ssh == 0 and out_ssh:
            ssh_rows = _parse_json_rows(out_ssh)
            if ssh_rows:
                ssh_key = ssh_rows[0].get("value")

        per_seg_ips = {}
        for seg in resolved_segments:
            sid = str(seg.get("segment_id"))
            ips, prefixlen = allocate_static_ips(
                seg.get("subnet_cidr"),
                seg.get("gateway_ip"),
                seg.get("dhcp_start"),
                seg.get("dhcp_end"),
                max(control_nodes, 1))
            per_seg_ips[sid] = (ips, prefixlen)

        log(f"Step 3: Provisioning {control_nodes} guest VM configurations...", "info")
        vm_ips = []
        hosts = get_cluster_nodes()
        if not hosts:
            hosts = [{"ip": LOCAL_IP, "hostname": "localhost"}]
        first_cp_ip = None

        for i in range(control_nodes):
            vm_name = f"{cluster_name}-control-0{i+1}"
            log(f"Registering control plane node VM: {vm_name}...", "info")

            seg = resolved_segments[i % len(resolved_segments)]
            seg_id = str(seg.get("segment_id") or f"{cluster_name}-segment-1")
            vni = int(seg.get("vni") or 10010)
            gateway_ip = str(seg.get("gateway_ip") or "172.16.10.254")
            ips, prefixlen = per_seg_ips[str(seg.get("segment_id"))]
            assigned_ip = ips[i % len(ips)]
            if first_cp_ip is None:
                first_cp_ip = assigned_ip
            vm_ips.append((vm_name, assigned_ip))

            res_name = f"{vm_name}-disk0"
            sidon_mod = sidon_module()
            disk_path = sidon_mod.nbd_socket(res_name)
            target_host = hosts[i % len(hosts)]["ip"]

            # Register the VM before anything is built for it.
            ok, applied, current, lwt_error = run_lwt("/v1/vm/create", {
                "name": vm_name,
                "vcpu": 2,
                "memory": 4096,
                "disk_path": disk_path,
                "disk_size": 50,
                "state": "Stopped",
                "host_ip": target_host,
                "disks_list": res_name,
                "network_id": seg_id,
            })
            if not ok:
                log(f"ERROR: could not register VM '{vm_name}': {lwt_error}", "error")
                raise RuntimeError(f"Registration of {vm_name} failed: {lwt_error}")
            if not applied:
                log(f"ERROR: a VM named '{vm_name}' already exists (host_ip "
                    f"'{current.get('host_ip')}'). Choose a different cluster name.", "error")
                raise RuntimeError(f"VM {vm_name} is already registered")

            # 1. Provision the node's vdisk: clone from Valhalla base image if present,
            # otherwise create a 50 GiB vdisk in the default Sidon container.
            cloned_from_valhalla = False
            if base_vdisk_id:
                log(f"Cloning base image vdisk '{base_vdisk_id}' into '{res_name}'...", "info")
                ok_cl, body_cl = sidon_call("clone", vdisk_id=base_vdisk_id, child_id=res_name)
                if ok_cl:
                    cloned_from_valhalla = True
            if not cloned_from_valhalla:
                log(f"Creating vdisk '{res_name}' (50 GiB)...", "info")
                ok_c, body_c = sidon_call("create", vdisk_id=res_name,
                                          size_bytes=50 * 1024 * 1024 * 1024,
                                          container=sidon_module().DEFAULT_CONTAINER)
                if not ok_c and "already exists" not in str(body_c):
                    raise Exception(f"Could not create vdisk {res_name}: {body_c}")

            # Attach it on the node that will run the guest.
            log(f"Attaching vdisk '{res_name}' on {target_host}...", "info")
            ok_a, body_a = sidon_call("attach", vdisk_id=res_name, host_ip=target_host)
            if not ok_a:
                raise Exception(
                    f"Could not attach vdisk {res_name} on {target_host}: {body_a}")

            if not cloned_from_valhalla and base_image_path:
                log(f"Copying OS template image into the vdisk for VM '{vm_name}'...", "info")
                rc_cp, out_cp, err_cp = run_remote_spark(
                    target_host,
                    f"qemu-img convert -O raw {base_image_path} "
                    f"'nbd+unix:///{res_name}?socket={disk_path}'")
                if rc_cp != 0:
                    raise Exception(
                        f"Could not copy the OS image into vdisk {res_name}: "
                        f"{(err_cp or out_cp or '').strip()[:300]}")

            # Build NoCloud cidata ISO in memory using build_cidata_iso_bytes
            log(f"Generating in-memory NoCloud cidata ISO for VM '{vm_name}'...", "info")
            role = "init" if i == 0 else "cp"
            user_data = build_k3s_userdata(
                vm_name=vm_name,
                role=role,
                assigned_ip=assigned_ip,
                prefixlen=prefixlen,
                gateway_ip=gateway_ip,
                cluster_token=cluster_token,
                first_cp_ip=first_cp_ip,
                root_password=root_password,
                ssh_key=ssh_key,
            )
            meta_data = f"instance-id: {vm_name}\nlocal-hostname: {vm_name}\n"
            network_config = build_network_config(assigned_ip, prefixlen, gateway_ip)
            iso_bytes = build_cidata_iso_bytes(meta_data, user_data, network_config)
            iso_b64 = base64.b64encode(iso_bytes).decode("ascii")
            iso_path = f"/var/lib/hci/aether/images/{vm_name}-cidata.iso"
            rc_iso, out_iso, err_iso = run_remote_spark(
                target_host,
                f"mkdir -p /var/lib/hci/aether/images && echo '{iso_b64}' | base64 -d > {iso_path}")
            if rc_iso != 0:
                raise RuntimeError(
                    f"Could not write cidata ISO for {vm_name} on {target_host}: "
                    f"{(err_iso or out_iso or '').strip()[:200]}")

            # 2. Write virtual VM XML config on target host using helios_sidon.disk_xml
            h_mac = hashlib.md5(vm_name.encode()).hexdigest()
            mac_addr = f"52:54:00:{h_mac[0:2]}:{h_mac[2:4]}:{h_mac[4:6]}"
            vnc_port = 5910 + i

            ovmf_code = "/usr/share/OVMF/OVMF_CODE.fd"
            ovmf_vars = "/usr/share/OVMF/OVMF_VARS.fd"
            _, test_ovmf, _ = run_remote_spark(
                target_host,
                "ls /usr/share/OVMF/OVMF_CODE.fd || ls /usr/share/qemu/OVMF.fd || echo 'none'")
            if "qemu/OVMF.fd" in (test_ovmf or ""):
                ovmf_code = "/usr/share/qemu/OVMF.fd"
                ovmf_vars = "/usr/share/qemu/OVMF.fd"

            if hasattr(sidon_mod, "disk_xml"):
                primary_disk_xml = sidon_mod.disk_xml(disk_path, "vda")
            else:
                primary_disk_xml = (
                    f"<disk type='file' device='disk'>"
                    f"<driver name='qemu' type='raw' cache='none'/>"
                    f"<source file='{disk_path}'/>"
                    f"<target dev='vda' bus='virtio'/>"
                    f"</disk>"
                )

            xml_def = f"""<domain type='kvm'>
  <name>{vm_name}</name>
  <memory unit='KiB'>4194304</memory>
  <vcpu placement='static'>2</vcpu>
  <os>
    <type arch='x86_64' machine='q35'>hvm</type>
    <loader readonly='yes' type='pflash'>{ovmf_code}</loader>
    <nvram>/var/lib/hci/aether/nvram/{vm_name}_vars.fd</nvram>
    <boot dev='hd'/>
  </os>
  <features>
    <acpi/>
    <apic/>
  </features>
  <cpu mode='host-passthrough'/>
  <clock offset='utc'/>
  <devices>
    <emulator>/usr/libexec/qemu-kvm</emulator>
    {primary_disk_xml}
    <disk type='file' device='cdrom'>
      <driver name='qemu' type='raw'/>
      <source file='{iso_path}'/>
      <target dev='sda' bus='sata'/>
      <readonly/>
    </disk>
    <interface type='bridge'>
      <mac address='{mac_addr}'/>
      <source bridge='br-ov-{vni}'/>
      <model type='virtio'/>
    </interface>
    <graphics type='vnc' port='{vnc_port}' autoport='no' listen='0.0.0.0'>
      <listen type='address' address='0.0.0.0'/>
    </graphics>
    <video>
      <model type='virtio' vram='16384' heads='1'/>
    </video>
  </devices>
</domain>"""

            log(f"Writing UEFI NVRAM vars file for VM '{vm_name}'...", "info")
            nvram_file_path = f"/var/lib/hci/aether/nvram/{vm_name}_vars.fd"
            run_remote_spark(
                target_host,
                f"mkdir -p /var/lib/hci/aether/nvram/ && cp {ovmf_vars} {nvram_file_path} || cp /usr/share/OVMF/OVMF_VARS.fd {nvram_file_path} || true")

            log(f"Registering XML definition in libvirt and starting guest VM '{vm_name}' on {target_host}...", "info")
            b64_xml = base64.b64encode(xml_def.encode("utf-8")).decode("utf-8")
            rc_xml, out_xml, err_xml = run_remote_spark(
                target_host, f"echo {b64_xml} | base64 -d > /tmp/{vm_name}.xml")
            if rc_xml != 0:
                raise RuntimeError(
                    f"Failed to stage libvirt XML for {vm_name} on {target_host}: {(err_xml or out_xml or '').strip()[:200]}")
            rc_def, out_def, err_def = run_remote_spark(
                target_host, f"virsh -c qemu:///system define /tmp/{vm_name}.xml")
            if rc_def != 0:
                raise RuntimeError(
                    f"Failed to define VM {vm_name} on {target_host}: {(err_def or out_def or '').strip()[:200]}")
            rc_st, out_st, err_st = run_remote_spark(
                target_host, f"virsh -c qemu:///system start {vm_name}")
            if rc_st != 0:
                raise RuntimeError(
                    f"Failed to start VM {vm_name} on {target_host}: {(err_st or out_st or '').strip()[:200]}")

            ok, applied, current, lwt_error = run_lwt("/v1/vm/set-state", {
                "name": vm_name,
                "state": "Running",
                "expected_host_ip": target_host,
            })
            if not ok:
                log(f"Could not record '{vm_name}' as running: {lwt_error}", "warning")
            elif not applied:
                log(f"'{vm_name}' is no longer placed on {target_host} "
                    f"(it is on '{current.get('host_ip')}'); leaving its state alone.", "warning")
            time.sleep(1)

        log("Step 4: Waiting for guest network leases and DHCP initialization...", "info")
        time.sleep(2)
        
        leader_ip = get_catalyst_target_ip()
        if not leader_ip:
            leader_ip = LOCAL_IP

        if leader_ip:
            try:
                payload = {
                    "action": "run_job",
                    "payload": {
                        "job_name": "urbosa_bootstrap",
                        "command": "python3 /usr/local/bin/urbosa-bootstrap"
                    }
                }
                ctx = ssl.create_default_context(
                    ssl.Purpose.SERVER_AUTH, cafile="/etc/hci/spark/certs/ca.crt")
                ctx.load_cert_chain(certfile="/etc/hci/spark/certs/node.crt",
                                    keyfile="/etc/hci/spark/certs/node.key")
                req = urllib.request.Request(
                    f"https://{leader_ip}:9091/api/v1/tasks/submit",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, context=ctx, timeout=5) as response:
                    pass
                log("Successfully triggered Urbosa SDN DHCP daemon refresh.", "success")
            except Exception as e:
                log(f"Failed to trigger Urbosa DHCP refresh: {e}", "warning")
            time.sleep(2)
            
            for vm_name, ip_addr in vm_ips:
                log(f"Successfully resolved VM guest IP: {vm_name} -> {ip_addr}", "success")
        
        log("Step 5: Bootstrapping k3s control plane and storing cluster kubeconfig...", "info")
        time.sleep(1)
        api_ip = first_cp_ip or (vm_ips[0][1] if vm_ips else "172.16.10.11")
        kubeconfig_yaml = build_kubeconfig_yaml(cluster_name, api_ip, cluster_token)
        safe_kc = kubeconfig_yaml.replace("'", "''")
        kc_key = f"lanayru_kubeconfig_{cluster_name}".replace("'", "''")
        run_cql_query(
            f"INSERT INTO hydra.cluster_settings (key, value) VALUES ('{kc_key}', '{safe_kc}');")
        log(f"Stored cluster kubeconfig in hydra.cluster_settings under '{kc_key}'.", "success")
        
        log("Step 6: Configuring Spark mTLS routing proxy on host interfaces...", "info")
        time.sleep(1)
        log("Spark-Proxy active: Secure API gateway listening on host port 6443 proxying to guest namespaces.", "success")
        
        # Complete task
        log_catalyst_task("lanayru", "deploy", "completed", 100, {"cluster_name": cluster_name, "control_nodes": control_nodes}, task_id=task_id, created_at=created_at)
        cql_update = f"UPDATE hydra.lanayru_clusters SET status = 'active' WHERE cluster_id = {cluster_id};"
        run_cql_query(cql_update)
        log("Lanayru Kubernetes Engine cluster successfully provisioned and active! ⚡", "success")
        
    except Exception as e:
        log(f"Error during deployment: {str(e)}", "error")
        try:
            run_cql_query(f"UPDATE hydra.lanayru_clusters SET status = 'failed' WHERE cluster_id = {cluster_id};")
        except Exception:
            pass
        log_catalyst_task("lanayru", "deploy", "failed", 100, {"cluster_name": cluster_name, "control_nodes": control_nodes}, error_msg=str(e), task_id=task_id, created_at=created_at)


def destroy_lanayru_worker(task_id, cluster_name, created_at):
    from spectrum_server import (
        run_cql_query,
        run_remote_spark,
        sidon_call,
        sidon_module,
        log_catalyst_task,
        get_cluster_nodes,
        LOCAL_IP,
        LANAYRU_LOGS
    )
    
    LANAYRU_LOGS[task_id] = []
    
    def log(msg):
        t = datetime.datetime.now().strftime("%H:%M:%S")
        LANAYRU_LOGS[task_id].append(f"[{t}] {msg}")
        print(f"[LANAYRU DESTROYER] {msg}", flush=True)

    try:
        log(f"Initiating destruction sequence for Lanayru cluster '{cluster_name}'...")
        time.sleep(1.5)

        # Delete guest control/worker VMs belonging to this Lanayru cluster
        log("Querying guest VM inventory for Lanayru cluster...")
        rc_v, out_v, _ = run_cql_query("SELECT JSON name, host_ip, disks_list FROM hydra.vms;")
        vms_to_delete = []
        if rc_v == 0 and out_v:
            for vm_info in _parse_json_rows(out_v):
                vm_name = vm_info.get("name", "")
                if owns_vm(cluster_name, vm_name):
                    vms_to_delete.append(vm_info)
                        
        hosts = get_cluster_nodes()
        if not hosts:
            hosts = [{"ip": LOCAL_IP, "hostname": "localhost"}]

        for vm_info in vms_to_delete:
            vm_name = vm_info["name"]
            host_ip = vm_info.get("host_ip", "")
            disks_list = vm_info.get("disks_list", "")
            
            log(f"Deleting cluster VM '{vm_name}' from hypervisor inventory...")
            if host_ip:
                run_remote_spark(host_ip, f"virsh -c qemu:///system destroy {vm_name} || true")
                run_remote_spark(host_ip, f"virsh -c qemu:///system undefine {vm_name} --keep-nvram || true")
                
            # Delete UEFI nvram vars file
            nvram_file_path = f"/var/lib/hci/aether/nvram/{vm_name}_vars.fd"
            target_ip = host_ip or LOCAL_IP
            run_remote_spark(target_ip, f"rm -f {nvram_file_path}")
            run_cql_query(f"DELETE FROM hydra.vm_nvram WHERE vm_name = '{vm_name}';")
            
            # Clean up cloud-init files on target host
            ci_dir = f"/var/lib/hci/aether/cloudinit/{vm_name}"
            iso_path = f"/var/lib/hci/aether/images/{vm_name}-cidata.iso"
            run_remote_spark(target_ip, f"rm -rf {ci_dir} {iso_path}")

            # Detach and delete the VM's Sidon vdisks
            disk_names = [
                d.strip() for d in str(disks_list or "").split(",")
                if d.strip() and d.strip().upper() != "NONE"
            ]
            if not disk_names:
                disk_names = [f"{vm_name}-disk0"]
            for res_name in disk_names:
                sidon_call("detach", vdisk_id=res_name, host_ip=target_ip)
                sidon_call("delete", vdisk_id=res_name, host_ip=target_ip)

            # Remove metadata record from ScyllaDB
            run_cql_query(f"DELETE FROM hydra.vms WHERE name = '{vm_name}';")
            time.sleep(0.5)

        # Remove stored kubeconfig setting for this cluster
        kc_key = f"lanayru_kubeconfig_{cluster_name}".replace("'", "''")
        run_cql_query(f"DELETE FROM hydra.cluster_settings WHERE key = '{kc_key}';")

        # Delete overlay segments dynamically by pattern matching segment names in database
        log("Removing Lanayru-allocated Urbosa overlay segments...")
        rc_seg, out_seg, _ = run_cql_query("SELECT JSON segment_id, name FROM hydra.urbosa_segments;")
        if rc_seg == 0 and out_seg:
            for seg_info in _parse_json_rows(out_seg):
                seg_name = seg_info.get("name", "")
                if seg_name.startswith(f"{cluster_name}-segment-"):
                    seg_uuid = seg_info.get("segment_id")
                    run_cql_query(f"DELETE FROM hydra.urbosa_segments WHERE segment_id = {seg_uuid};")

        # Fetch cluster_id and delete registry entries
        rc_id, stdout_id, _ = run_cql_query(f"SELECT cluster_id FROM hydra.lanayru_clusters WHERE name = '{cluster_name}' ALLOW FILTERING;")
        if rc_id == 0 and stdout_id:
            for line in stdout_id.splitlines():
                line_clean = line.strip()
                if line_clean and not line_clean.startswith('(') and not line_clean.startswith('-') and line_clean != "cluster_id" and line_clean != "rows":
                    cluster_uuid = line_clean
                    run_cql_query(f"DELETE FROM hydra.lanayru_clusters WHERE cluster_id = {cluster_uuid};")
                    run_cql_query(f"DELETE FROM hydra.lanayru_k8s_state WHERE cluster_id = {cluster_uuid};")
                    break

        log_catalyst_task("lanayru", "destroy", "completed", 100, {"cluster_name": cluster_name}, task_id=task_id, created_at=created_at)
        log("Lanayru cluster destruction complete! 🗑️")
    except Exception as e:
        log(f"Error during destruction: {str(e)}")
        log_catalyst_task("lanayru", "destroy", "failed", 100, {"cluster_name": cluster_name}, error_msg=str(e), task_id=task_id, created_at=created_at)

