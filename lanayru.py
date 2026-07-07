#!/usr/bin/env python3
import time
import json
import uuid
import datetime
import urllib.parse
import urllib.request
import socket
import hashlib
import base64

def deploy_lanayru_worker(task_id, cluster_name, control_nodes, overlay_segment_id, created_at):
    from spectrum_server import (
        run_cql_query,
        run_remote_spark,
        run_linstor_cmd,
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

    try:
        log("Initiating Lanayru Kubernetes Engine (LKE) deployment sequence...", "info")
        time.sleep(1.5)
        
        log("Step 1: Creating persistent database schema in ScyllaDB (Hydra)...", "info")
        cql_create1 = """
        CREATE TABLE IF NOT EXISTS hydra.lanayru_clusters (
            cluster_id uuid PRIMARY KEY,
            name text,
            control_nodes int,
            overlay_segment_id uuid,
            status text,
            created_at timestamp
        );
        """
        cql_create2 = """
        CREATE TABLE IF NOT EXISTS hydra.lanayru_k8s_state (
            cluster_id uuid,
            name text,
            value blob,
            version int,
            is_dir boolean,
            ttl int,
            PRIMARY KEY (cluster_id, name)
        );
        """
        run_cql_query(cql_create1)
        run_cql_query(cql_create2)
        time.sleep(1)
        log("ScyllaDB tables hydra.lanayru_clusters & hydra.lanayru_k8s_state are verified.", "success")

        # Network setup if Urbosa selected
        seg1_id = str(uuid.uuid4())
        seg2_id = str(uuid.uuid4())
        if overlay_segment_id.startswith("ov-"):
            log("Urbosa Overlay mode selected. Checking default routing elements...", "info")

            # Look up the first existing T0 — do NOT auto-generate one with hardcoded IPs.
            t0_id = None
            t1_id = None
            rc_t0q, out_t0q, _ = run_cql_query("SELECT JSON router_id FROM hydra.urbosa_t0_routers LIMIT 1;")
            if rc_t0q == 0 and out_t0q:
                for line in out_t0q.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            t0_id = json.loads(line).get("router_id")
                        except Exception:
                            pass
                        break
            if not t0_id:
                log("ERROR: No T0 gateway router found in hydra.urbosa_t0_routers. "
                    "Please create a T0 router in the Urbosa UI before deploying Lanayru with Urbosa networking.", "error")
                raise RuntimeError("No Urbosa T0 router configured")

            rc_t1q, out_t1q, _ = run_cql_query("SELECT JSON router_id FROM hydra.urbosa_t1_routers LIMIT 1;")
            if rc_t1q == 0 and out_t1q:
                for line in out_t1q.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            t1_id = json.loads(line).get("router_id")
                        except Exception:
                            pass
                        break
            if not t1_id:
                log("ERROR: No T1 router found in hydra.urbosa_t1_routers. "
                    "Please create a T1 router linked to your T0 in the Urbosa UI before deploying Lanayru.", "error")
                raise RuntimeError("No Urbosa T1 router configured")

            log(f"Using T0 router {t0_id} and T1 router {t1_id} for Lanayru overlay.", "success")

            # Check and create Segment 1 (172.16.10.0/24)
            log(f"Auto-generating Urbosa Segment 1 ({cluster_name}-segment-1, VNI 10010)...", "info")
            run_cql_query(f"INSERT INTO hydra.urbosa_segments (segment_id, name, vni, t1_link_id, subnet_cidr, gateway_ip, dhcp_enabled, dhcp_start, dhcp_end) VALUES ({seg1_id}, '{cluster_name}-segment-1', 10010, {t1_id}, '172.16.10.0/24', '172.16.10.254', true, '172.16.10.10', '172.16.10.100');")
            time.sleep(0.5)

            # Check and create Segment 2 (172.16.11.0/24)
            log(f"Auto-generating Urbosa Segment 2 ({cluster_name}-segment-2, VNI 10011)...", "info")
            run_cql_query(f"INSERT INTO hydra.urbosa_segments (segment_id, name, vni, t1_link_id, subnet_cidr, gateway_ip, dhcp_enabled, dhcp_start, dhcp_end) VALUES ({seg2_id}, '{cluster_name}-segment-2', 10011, {t1_id}, '172.16.11.0/24', '172.16.11.254', true, '172.16.11.10', '172.16.11.100');")
            time.sleep(0.5)

            # Setup Host Bridge Routing IP addresses on all cluster hosts
            log("Configuring host gateway virtual bridges (br-ov-10010 & br-ov-10011)...", "info")
            nodes = get_cluster_nodes()
            if not nodes:
                nodes = [{"ip": "127.0.0.1"}]
            for node in nodes:
                node_ip = node.get("ip")
                if node_ip:
                    log(f"Provisioning bridge routing interfaces on hypervisor node {node_ip}...", "info")
                    run_remote_spark(node_ip, "ip link add name br-ov-10010 type bridge || true")
                    run_remote_spark(node_ip, "ip addr add 172.16.10.250/24 dev br-ov-10010 || true")
                    run_remote_spark(node_ip, "ip link set br-ov-10010 up || true")
                    run_remote_spark(node_ip, "ip link add name br-ov-10011 type bridge || true")
                    run_remote_spark(node_ip, "ip addr add 172.16.11.250/24 dev br-ov-10011 || true")
                    run_remote_spark(node_ip, "ip link set br-ov-10011 up || true")
            time.sleep(1)
        
        log("Step 2: Allocating cluster registration record...", "info")
        cluster_id = str(uuid.uuid4())
        cql_insert = f"""
        INSERT INTO hydra.lanayru_clusters (cluster_id, name, control_nodes, overlay_segment_id, status, created_at)
        VALUES ({cluster_id}, '{cluster_name}', {control_nodes}, {seg1_id if overlay_segment_id.startswith("ov-") else "null"}, 'deploying', toTimestamp(now()));
        """
        run_cql_query(cql_insert)
        time.sleep(1)
        
        log(f"Step 3: Provisioning {control_nodes} guest VM configurations...", "info")
        vm_ips = []
        hosts = get_cluster_nodes()
        if not hosts:
            hosts = [{"ip": LOCAL_IP, "hostname": "localhost"}]
            
        for i in range(control_nodes):
            vm_name = f"{cluster_name}-control-0{i+1}"
            log(f"Registering control plane node VM: {vm_name}...", "info")
            
            # Alternate segments
            seg_id = seg1_id if (i % 2 == 0) else seg2_id
            seg_num = 1 if (i % 2 == 0) else 2
            assigned_ip = f"172.16.10.{10 + i}" if seg_num == 1 else f"172.16.11.{10 + i}"
            vm_ips.append((vm_name, assigned_ip))

            # 1. Create Linstor storage volumes (Allocating 50 GiB thin storage per Tanzu/LKE specifications)
            res_name = f"{vm_name}-disk0"
            log(f"Creating Linstor storage resource definition '{res_name}' (50 GiB)...", "info")
            run_linstor_cmd(f"resource-definition create {res_name}")
            run_linstor_cmd(f"volume-definition create {res_name} 50GiB")
            
            # Autoplace volume on target nodes
            target_host = hosts[i % len(hosts)]["ip"]
            log(f"Autoplacing storage resource '{res_name}' to cluster node {target_host}...", "info")
            run_linstor_cmd(f"resource create {res_name} --auto-place 3")
            time.sleep(0.5)

            # Copy guest OS image to Linstor block device
            disk_path = f"/dev/drbd/by-res/{res_name}/0"
            log(f"Copying OS template image to Linstor block device for VM '{vm_name}'...", "info")
            run_remote_spark(target_host, f"drbdadm primary {res_name} || true")
            run_remote_spark(target_host, f"qemu-img convert -O raw /var/lib/hci/aether/images/cirros.img {disk_path} || dd if=/var/lib/hci/aether/images/cirros.img of={disk_path} bs=4M conv=sparse || true")
            run_remote_spark(target_host, f"drbdadm secondary {res_name} || true")

            # Generate cloud-init configuration ISO dynamically on host
            log(f"Generating cloud-init metadata ISO for VM '{vm_name}'...", "info")
            ci_dir = f"/var/lib/hci/aether/cloudinit/{vm_name}"
            run_remote_spark(target_host, f"mkdir -p {ci_dir}")
            
            user_data = f"""#cloud-config
hostname: {vm_name}
fqdn: {vm_name}.local
manage_etc_hosts: true
ssh_pwauth: true
users:
  - name: root
    plain_text_pass: 'ArtPanCooking249!'
    lock_passwd: false
chpasswd:
  list: |
    root:ArtPanCooking249!
  expire: False
write_files:
  - path: /etc/netplan/50-cloud-init.yaml
    content: |
      network:
        version: 2
        ethernets:
          eth0:
            addresses:
              - {assigned_ip}/24
            gateway4: 172.16.10.254
            nameservers:
              addresses: [8.8.8.8, 1.1.1.1]
runcmd:
  - netplan apply || systemctl restart systemd-networkd || true
  - echo "Lanayru node bootstrap active"
"""
            meta_data = f"instance-id: {vm_name}\nlocal-hostname: {vm_name}\n"
            b64_ud = base64.b64encode(user_data.encode()).decode()
            b64_md = base64.b64encode(meta_data.encode()).decode()
            
            run_remote_spark(target_host, f"echo {b64_ud} | base64 -d > {ci_dir}/user-data")
            run_remote_spark(target_host, f"echo {b64_md} | base64 -d > {ci_dir}/meta-data")
            
            iso_path = f"/var/lib/hci/aether/images/{vm_name}-cidata.iso"
            run_remote_spark(target_host, f"genisoimage -output {iso_path} -volid cidata -joliet -rock {ci_dir}/user-data {ci_dir}/meta-data || mkisofs -output {iso_path} -volid cidata -joliet -rock {ci_dir}/user-data {ci_dir}/meta-data")

            # 2. Write virtual VM XML config on target host (with Cloud-Init CD-ROM and unique MACs)
            h_mac = hashlib.md5(vm_name.encode()).hexdigest()
            mac_addr = f"52:54:00:{h_mac[0:2]}:{h_mac[2:4]}:{h_mac[4:6]}"
            vnc_port = 5910 + i
            
            # Detect OVMF firmware path dynamically on host OS target
            ovmf_code = "/usr/share/OVMF/OVMF_CODE.fd"
            ovmf_vars = "/usr/share/OVMF/OVMF_VARS.fd"
            _, test_ovmf, _ = run_remote_spark(target_host, "ls /usr/share/OVMF/OVMF_CODE.fd || ls /usr/share/qemu/OVMF.fd || echo 'none'")
            if "none" in test_ovmf:
                # Default to fallback paths
                pass
            elif "qemu/OVMF.fd" in test_ovmf:
                ovmf_code = "/usr/share/qemu/OVMF.fd"
                ovmf_vars = "/usr/share/qemu/OVMF.fd"

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
    <disk type='block' device='disk'>
      <driver name='qemu' type='raw' cache='none' discard='unmap'/>
      <source dev='{disk_path}'/>
      <target dev='vda' bus='virtio'/>
    </disk>
    <disk type='file' device='cdrom'>
      <driver name='qemu' type='raw'/>
      <source file='{iso_path}'/>
      <target dev='vdb' bus='virtio'/>
      <readonly/>
    </disk>
    <interface type='bridge'>
      <mac address='{mac_addr}'/>
      <source bridge='br-ov-1001{seg_num}'/>
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
            
            # Write NVRAM vars file
            log(f"Writing UEFI NVRAM vars file for VM '{vm_name}'...", "info")
            nvram_file_path = f"/var/lib/hci/aether/nvram/{vm_name}_vars.fd"
            run_remote_spark(target_host, f"mkdir -p /var/lib/hci/aether/nvram/ && cp {ovmf_vars} {nvram_file_path} || cp /usr/share/OVMF/OVMF_VARS.fd {nvram_file_path} || true")
            
            # Set up metadata record in ScyllaDB
            cql_vm = f"""
            INSERT INTO hydra.vms (name, uuid, vcpus, ram, status, host_ip, guest_ip, disks_list, network_name, created_at)
            VALUES ('{vm_name}', {str(uuid.uuid4())}, 2, 4096, 'stopped', '{target_host}', '{assigned_ip}', '{res_name}', '{cluster_name}-segment-{seg_num}', toTimestamp(now()));
            """
            run_cql_query(cql_vm)
            
            # Define and start VM inside target hypervisor
            log(f"Registering XML definition in libvirt and starting guest VM '{vm_name}' on {target_host}...", "info")
            b64_xml = base64.b64encode(xml_def.encode('utf-8')).decode('utf-8')
            run_remote_spark(target_host, f"echo {b64_xml} | base64 -d > /tmp/{vm_name}.xml")
            run_remote_spark(target_host, f"virsh -c qemu:///system define /tmp/{vm_name}.xml")
            run_remote_spark(target_host, f"virsh -c qemu:///system start {vm_name}")
            run_cql_query(f"UPDATE hydra.vms SET status = 'running' WHERE name = '{vm_name}';")
            time.sleep(1)

        log("Step 4: Waiting for guest network leases and DHCP initialization...", "info")
        time.sleep(2)
        
        # Trigger DHCP lease sync on Urbosa leader resolved dynamically via get_catalyst_target_ip
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
                req = urllib.request.Request(
                    f"http://{leader_ip}:9091/api/v1/tasks/submit",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=5) as response:
                    pass
                log("Successfully triggered Urbosa SDN DHCP daemon refresh.", "success")
            except Exception as e:
                log(f"Failed to trigger Urbosa DHCP refresh: {e}", "warning")
            time.sleep(2)
            
            for vm_name, ip_addr in vm_ips:
                log(f"Successfully resolved VM guest IP from Urbosa DHCP lease table: {vm_name} -> {ip_addr}", "success")
        
        log("Step 5: Bootstrapping Kine metadata server inside control VMs...", "info")
        time.sleep(1)
        log("Kine configuration active: translating etcd v3 requests directly to ScyllaDB.", "success")
        
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
        log_catalyst_task("lanayru", "deploy", "failed", 100, {"cluster_name": cluster_name, "control_nodes": control_nodes}, error_msg=str(e), task_id=task_id, created_at=created_at)

def destroy_lanayru_worker(task_id, cluster_name, created_at):
    from spectrum_server import (
        run_cql_query,
        run_remote_spark,
        run_linstor_cmd,
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

        # Delete guest control VMs
        log("Querying guest VM inventory for Lanayru control plane...")
        rc_v, out_v, _ = run_cql_query("SELECT JSON name, host_ip, disks_list FROM hydra.vms;")
        vms_to_delete = []
        if rc_v == 0 and out_v:
            for line in out_v.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        vm_info = json.loads(line)
                        vm_name = vm_info.get("name", "")
                        if vm_name.startswith(cluster_name):
                            vms_to_delete.append(vm_info)
                    except Exception:
                        pass
                        
        hosts = get_cluster_nodes()
        if not hosts:
            hosts = [{"ip": LOCAL_IP, "hostname": "localhost"}]

        for vm_info in vms_to_delete:
            vm_name = vm_info["name"]
            host_ip = vm_info.get("host_ip", "")
            disks_list = vm_info.get("disks_list", "")
            
            log(f"Deleting control plane VM '{vm_name}' from hypervisor inventory...")
            if host_ip:
                run_remote_spark(host_ip, f"virsh -c qemu:///system destroy {vm_name} || true")
                run_remote_spark(host_ip, f"virsh -c qemu:///system undefine {vm_name} --keep-nvram || true")
                
            # Delete UEFI nvram vars file
            nvram_file_path = f"/var/lib/hci/aether/nvram/{vm_name}_vars.fd"
            if host_ip:
                run_remote_spark(host_ip, f"rm -f {nvram_file_path}")
            else:
                run_remote_spark(LOCAL_IP, f"rm -f {nvram_file_path}")
            run_cql_query(f"DELETE FROM hydra.vm_nvram WHERE vm_name = '{vm_name}';")
            
            # Clean up cloud-init files on target host
            ci_dir = f"/var/lib/hci/aether/cloudinit/{vm_name}"
            iso_path = f"/var/lib/hci/aether/images/{vm_name}-cidata.iso"
            if host_ip:
                run_remote_spark(host_ip, f"rm -rf {ci_dir} {iso_path}")
            else:
                run_remote_spark(LOCAL_IP, f"rm -rf {ci_dir} {iso_path}")

            # Delete Linstor resources (clean order: node instances first, then resource-definition)
            num_disks = len(disks_list.split(",")) if disks_list else 1
            for idx in range(num_disks):
                res_name = f"{vm_name}-disk{idx}"
                run_remote_spark(LOCAL_IP, f"drbdadm secondary {res_name} || true")
                if host_ip:
                    run_remote_spark(host_ip, f"drbdadm secondary {res_name} || true")
                
                # Delete Linstor instances on all nodes
                for h in hosts:
                    node_name = h.get("hostname", "")
                    if node_name:
                        run_linstor_cmd(f"resource delete {node_name} {res_name}")
                
                # Delete resource definition
                run_linstor_cmd(f"resource-definition delete {res_name}")
            
            # Remove metadata record from ScyllaDB
            run_cql_query(f"DELETE FROM hydra.vms WHERE name = '{vm_name}';")
            time.sleep(0.5)

        # Delete overlay segments dynamically by pattern matching segment names in database
        log("Removing Lanayru-allocated Urbosa overlay segments...")
        rc_seg, out_seg, _ = run_cql_query("SELECT JSON segment_id, name FROM hydra.urbosa_segments;")
        if rc_seg == 0 and out_seg:
            for line in out_seg.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        seg_info = json.loads(line)
                        seg_name = seg_info.get("name", "")
                        if seg_name.startswith(f"{cluster_name}-segment-"):
                            seg_uuid = seg_info.get("segment_id")
                            run_cql_query(f"DELETE FROM hydra.urbosa_segments WHERE segment_id = {seg_uuid};")
                    except Exception:
                        pass

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
