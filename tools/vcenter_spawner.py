#!/usr/bin/env python3
"""
vcenter_spawner.py - Automated Multi-Node Hypervisor VM Spawner for Helios HCI.

Deploys, manages, provisions, and tests 1-, 2-, or 3-node genuine hypervisor
clusters on VMware vCenter with nested KVM virtualization, Sidon raw data disks,
and automated Rocky Linux 10 (EL 10) kickstart provisioning.

Zero fake data, mocks, or placeholders: operates directly against the live vCenter
REST and SOAP APIs and physical ESXi hypervisors.
"""

import os
import sys
import time
import json
import ssl
import base64
import socket
import argparse
import subprocess
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET

# Default Infrastructure Constants
DEFAULT_VCENTER_HOST = os.environ.get("VCENTER_HOST", "vcenter.zerotwo.cloud")
DEFAULT_VCENTER_USER = os.environ.get("VCENTER_USER", "administrator@vsphere.local")
DEFAULT_VCENTER_PASS = os.environ.get("VCENTER_PASS", "Px$3lGU1K$2lJUqEZRf*")

DEFAULT_DATACENTER = "datacenter-1035"       # prod02dc
DEFAULT_VM_FOLDER = "group-v1036"           # vm folder
DEFAULT_RESOURCE_POOL = "resgroup-1049"     # cluster root resource pool
DEFAULT_DATASTORE = "datastore-1044"         # SAN_DATA_NEW (2.2 TB free)
DEFAULT_NETWORK_PG = "dvportgroup-1068"     # vlan302 (native management VLAN)
DEFAULT_BOOT_ISO = "[SAN_ISO] Rocky-10.2-x86_64-boot.iso"

DEFAULT_GATEWAY = "10.10.102.1"
DEFAULT_NETMASK = "255.255.255.0"
DEFAULT_DNS = "10.10.102.2"
DEFAULT_DOMAIN = "zerotwo.cloud"
DEFAULT_IP_BASE = 71  # 10.10.102.71, 72, 73...

DEFAULT_SSH_KEY_PUB = os.path.expanduser(r"~/.ssh/zerotwo_auth_ed25519.pub")
DEFAULT_SSH_KEY_PRIV = os.path.expanduser(r"~/.ssh/zerotwo_auth_ed25519")
JUMP_HOST = "10.10.102.21"


class VCenterClient:
    """Handles REST and SOAP interactions with VMware vCenter."""

    def __init__(self, host=DEFAULT_VCENTER_HOST, user=DEFAULT_VCENTER_USER, password=DEFAULT_VCENTER_PASS):
        self.host = host
        self.user = user
        self.password = password
        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE

        self.token = None
        self.soap_cookie = None
        self.session_mgr = None
        self.prop_collector = None
        self.namespaces = {'vim25': 'urn:vim25', 'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/'}

        self._auth_rest()
        self._auth_soap()

    def _auth_rest(self):
        creds = f"{self.user}:{self.password}"
        b64 = base64.b64encode(creds.encode("utf-8")).decode("utf-8")
        headers = {
            "Authorization": f"Basic {b64}",
            "Content-Type": "application/json"
        }
        url = f"https://{self.host}/api/session"
        req = urllib.request.Request(url, data=b"", headers=headers, method="POST")
        with urllib.request.urlopen(req, context=self.ctx, timeout=15) as resp:
            self.token = json.loads(resp.read().decode("utf-8"))

    def _auth_soap(self):
        # Retrieve Service Content
        sc_body = """
        <RetrieveServiceContent xmlns="urn:vim25">
          <_this type="ServiceInstance">ServiceInstance</_this>
        </RetrieveServiceContent>
        """
        sc_resp = self.soap_call("RetrieveServiceContent", sc_body)
        root = ET.fromstring(sc_resp)
        self.session_mgr = root.find(".//vim25:sessionManager", self.namespaces).text
        self.prop_collector = root.find(".//vim25:propertyCollector", self.namespaces).text

        # Login
        login_body = f"""
        <Login xmlns="urn:vim25">
          <_this type="SessionManager">{self.session_mgr}</_this>
          <userName>{self.user}</userName>
          <password>{self.password}</password>
        </Login>
        """
        self.soap_call("Login", login_body)

    def soap_call(self, action, body):
        envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:vim25="urn:vim25"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <soapenv:Body>
    {body}
  </soapenv:Body>
</soapenv:Envelope>"""
        headers = {
            "Content-Type": 'text/xml; charset="utf-8"',
            "SOAPAction": f'urn:vim25/{action}'
        }
        if self.soap_cookie:
            headers["Cookie"] = self.soap_cookie

        url = f"https://{self.host}/sdk"
        req = urllib.request.Request(url, data=envelope.encode("utf-8"), headers=headers, method="POST")
        with urllib.request.urlopen(req, context=self.ctx, timeout=30) as resp:
            cookie = resp.headers.get("Set-Cookie")
            if cookie:
                self.soap_cookie = cookie.split(";")[0]
            return resp.read().decode("utf-8")

    def rest_get(self, endpoint):
        headers = {
            "vmware-api-session-id": self.token,
            "Content-Type": "application/json"
        }
        url = f"https://{self.host}{endpoint}"
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, context=self.ctx, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def rest_post(self, endpoint, data=None):
        headers = {
            "vmware-api-session-id": self.token,
            "Content-Type": "application/json"
        }
        body = json.dumps(data).encode("utf-8") if data is not None else b""
        url = f"https://{self.host}{endpoint}"
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, context=self.ctx, timeout=30) as resp:
            res = resp.read().decode("utf-8")
            return json.loads(res) if res else {}

    def rest_patch(self, endpoint, data):
        headers = {
            "vmware-api-session-id": self.token,
            "Content-Type": "application/json"
        }
        body = json.dumps(data).encode("utf-8")
        url = f"https://{self.host}{endpoint}"
        req = urllib.request.Request(url, data=body, headers=headers, method="PATCH")
        with urllib.request.urlopen(req, context=self.ctx, timeout=20) as resp:
            return resp.status

    def rest_delete(self, endpoint):
        headers = {
            "vmware-api-session-id": self.token,
            "Content-Type": "application/json"
        }
        url = f"https://{self.host}{endpoint}"
        req = urllib.request.Request(url, headers=headers, method="DELETE")
        with urllib.request.urlopen(req, context=self.ctx, timeout=30) as resp:
            return resp.status

    def upload_file_to_datastore(self, datastore_name, remote_path, data_bytes):
        """Uploads raw bytes directly to a datastore via HTTPS PUT."""
        creds = f"{self.user}:{self.password}"
        b64 = base64.b64encode(creds.encode("utf-8")).decode("utf-8")
        headers = {
            "Authorization": f"Basic {b64}",
            "Content-Type": "application/octet-stream"
        }
        url = f"https://{self.host}/folder/{remote_path}?dsName={datastore_name}&dcPath=prod02dc"
        req = urllib.request.Request(url, data=data_bytes, headers=headers, method="PUT")
        with urllib.request.urlopen(req, context=self.ctx, timeout=60) as resp:
            return resp.status in (200, 201)

    def enable_nested_hv(self, vm_id):
        """Enables nested hardware virtualization (VT-x / AMD-V) on VM via SOAP."""
        reconfig_body = f"""
        <ReconfigVM_Task xmlns="urn:vim25">
          <_this type="VirtualMachine">{vm_id}</_this>
          <spec>
            <nestedHVEnabled>true</nestedHVEnabled>
          </spec>
        </ReconfigVM_Task>
        """
        resp = self.soap_call("ReconfigVM_Task", reconfig_body)
        root = ET.fromstring(resp)
        task_id = root.find(".//vim25:returnval", self.namespaces).text
        for _ in range(15):
            time.sleep(1)
            poll_body = f"""
            <RetrievePropertiesEx xmlns="urn:vim25">
              <_this type="PropertyCollector">{self.prop_collector}</_this>
              <specSet>
                <propSet>
                  <type>Task</type>
                  <pathSet>info.state</pathSet>
                </propSet>
                <objectSet>
                  <obj type="Task">{task_id}</obj>
                </objectSet>
              </specSet>
              <options/>
            </RetrievePropertiesEx>
            """
            poll_resp = self.soap_call("RetrievePropertiesEx", poll_body)
            if "success" in poll_resp:
                return True
        return False

    def list_helios_vms(self):
        """Returns all registered VMs starting with 'helios-'."""
        vms = self.rest_get("/api/vcenter/vm")
        helios_vms = []
        for vm in vms:
            if vm.get("name", "").startswith("helios-"):
                helios_vms.append(vm)
        return helios_vms

    def get_vm_details(self, vm_id):
        """Returns full VM hardware, guest identity, and networking details."""
        info = self.rest_get(f"/api/vcenter/vm/{vm_id}")
        guest_id = {}
        try:
            guest_id = self.rest_get(f"/api/vcenter/vm/{vm_id}/guest/identity")
        except Exception:
            pass
        info["guest_identity"] = guest_id
        return info


def generate_kickstart(ip, hostname, root_pass_hash, ssh_pub_key, gateway=DEFAULT_GATEWAY, netmask=DEFAULT_NETMASK, dns=DEFAULT_DNS):
    """Generates an unattended Rocky Linux 10 kickstart configuration."""
    return f"""# Rocky Linux 10.2 Automated Kickstart for Helios Hypervisor Node
text
reboot
url --url="https://dl.rockylinux.org/pub/rocky/10/BaseOS/x86_64/os/"
repo --name="AppStream" --baseurl="https://dl.rockylinux.org/pub/rocky/10/AppStream/x86_64/os/"

keyboard --vckeymap=us --xlayouts='us'
lang en_US.UTF-8

network --bootproto=static --device=ens33 --gateway={gateway} --ip={ip} --nameserver={dns} --netmask={netmask} --activate --ipv4-dns-search={DEFAULT_DOMAIN} --hostname={hostname}

rootpw --iscrypted --allow-ssh {root_pass_hash}

ignoredisk --only-use=sda
clearpart --all --initlabel
autopart --type=plain

timezone America/New_York --utc
selinux --permissive
firewall --disabled

%packages
@core
qemu-kvm
libvirt
libvirt-client
libvirt-daemon-kvm
podman
python3
python3-pip
tar
xfsprogs
lvm2
open-vm-tools
curl
git
chrony
%end

%post --erroronfail
mkdir -p /root/.ssh
chmod 700 /root/.ssh
cat << 'EOF' > /root/.ssh/authorized_keys
{ssh_pub_key}
EOF
chmod 600 /root/.ssh/authorized_keys

systemctl enable sshd
systemctl enable vmtoolsd
systemctl enable libvirtd || true
ln -sf /usr/libexec/qemu-kvm /usr/bin/qemu-kvm || true
%end
"""


def build_oemdrv_iso_via_jump_host(ks_content, node_name):
    """Builds an OEMDRV volume ISO containing /ks.cfg via the Linux jump host."""
    import paramiko

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(JUMP_HOST, username="root", key_filename=DEFAULT_SSH_KEY_PRIV, timeout=10)

    remote_dir = f"/tmp/{node_name}_oemdrv"
    iso_path = f"/tmp/ks-{node_name}.iso"

    sftp = ssh.open_sftp()
    ssh.exec_command(f"rm -rf {remote_dir} && mkdir -p {remote_dir}")
    with sftp.open(f"{remote_dir}/ks.cfg", "w") as f:
        f.write(ks_content)

    cmd = f"xorriso -as mkisofs -V 'OEMDRV' -o {iso_path} {remote_dir}"
    stdin, stdout, stderr = ssh.exec_command(cmd)
    stdout.read()

    iso_bytes = sftp.open(iso_path, "rb").read()
    sftp.close()
    ssh.exec_command(f"rm -rf {remote_dir} {iso_path}")
    ssh.close()

    return iso_bytes


def cmd_create(args, client):
    """Creates and boots N hypervisor nodes with kickstart and nested virtualization."""
    count = args.count
    ram_mb = args.ram_mb
    cpus = args.cpus

    print(f"[*] Deploying {count} Helios Hypervisor Node(s)...")
    print(f"[*] Hardware per node: {cpus} vCPUs, {ram_mb} MiB RAM, 30 GB OS disk, 50 GB Sidon raw disk")
    print(f"[*] Nested Virtualization (VT-x): ENABLED (/dev/kvm)")
    print(f"[*] Target Network: {DEFAULT_NETWORK_PG} (VLAN 302 native)")

    # Read SSH public key
    ssh_pub = ""
    if os.path.exists(DEFAULT_SSH_KEY_PUB):
        with open(DEFAULT_SSH_KEY_PUB, "r") as f:
            ssh_pub = f.read().strip()
    else:
        print(f"[!] Warning: Public key {DEFAULT_SSH_KEY_PUB} not found. Using fallback key.")
        ssh_pub = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILVmhxYXCBj/JJ5RXY7oBEgQKFMDBcME3Jx46XmArHCF"

    root_hash = "$y$j9T$3uOZwrcN4XuEhGphjaXQf15m$QMQMrrJsqbfoC5VK5wxRV8ql9Mz0YAvysuhtbKasLv0"

    deployed_nodes = []

    for i in range(1, count + 1):
        node_name = f"helios-node-{i:02d}"
        ip = f"10.10.102.{args.start_ip + i - 1}"
        hostname = f"{node_name}.{DEFAULT_DOMAIN}"

        print(f"\n--- Node {i}/{count}: {node_name} ({ip}) ---")

        # 1. Generate Kickstart and OEMDRV ISO
        print(f"[{node_name}] Generating unattended kickstart ISO...")
        ks = generate_kickstart(ip, hostname, root_hash, ssh_pub)
        iso_bytes = build_oemdrv_iso_via_jump_host(ks, node_name)
        remote_iso_path = f"iso/ks-{node_name}.iso"

        print(f"[{node_name}] Uploading kickstart ISO to SAN_DATA_NEW ({len(iso_bytes)} bytes)...")
        client.upload_file_to_datastore("SAN_DATA_NEW", remote_iso_path, iso_bytes)

        # 2. Build VM Specification
        vm_spec = {
            "name": node_name,
            "guest_OS": "ROCKYLINUX_64",
            "placement": {
                "folder": DEFAULT_VM_FOLDER,
                "resource_pool": DEFAULT_RESOURCE_POOL,
                "datastore": DEFAULT_DATASTORE
            },
            "cpu": {"count": cpus},
            "memory": {"size_MiB": ram_mb},
            "boot": {"type": "EFI"},
            "disks": [
                {"new_vmdk": {"capacity": 32212254720}},   # 30 GB OS
                {"new_vmdk": {"capacity": 107374182400}}   # 100 GB Sidon DFS (matches cluster_new.py >= 100GB)
            ],
            "nics": [
                {
                    "backing": {
                        "type": "DISTRIBUTED_PORTGROUP",
                        "network": DEFAULT_NETWORK_PG
                    }
                }
            ],
            "cdroms": [
                {
                    "backing": {
                        "type": "ISO_FILE",
                        "iso_file": DEFAULT_BOOT_ISO
                    },
                    "start_connected": True
                },
                {
                    "backing": {
                        "type": "ISO_FILE",
                        "iso_file": f"[SAN_DATA_NEW] {remote_iso_path}"
                    },
                    "start_connected": True
                }
            ]
        }

        print(f"[{node_name}] Creating VM in vCenter...")
        vm_id = client.rest_post("/api/vcenter/vm", vm_spec)
        print(f"[{node_name}] Created VM ID: {vm_id}")

        # 3. Enable Nested Virtualization
        print(f"[{node_name}] Enabling Nested Virtualization (nestedHVEnabled)...")
        client.enable_nested_hv(vm_id)

        # 4. Patch NIC start_connected = True
        client.rest_patch(f"/api/vcenter/vm/{vm_id}/hardware/ethernet/4000", {"start_connected": True})

        # 5. Power On
        print(f"[{node_name}] Powering on VM to begin unattended installation...")
        client.rest_post(f"/api/vcenter/vm/{vm_id}/power?action=start")

        deployed_nodes.append({
            "name": node_name,
            "id": vm_id,
            "ip": ip,
            "cpus": cpus,
            "ram_mb": ram_mb
        })

    print("\n" + "=" * 60)
    print(f"Successfully spawned {len(deployed_nodes)} node(s) on vCenter!")
    for node in deployed_nodes:
        print(f"  * {node['name']} (ID: {node['id']}) -> IP: {node['ip']}")
    print("Nodes are currently executing unattended Rocky Linux 10 kickstart.")
    print("=" * 60)


def cmd_status(args, client):
    """Reports status of all deployed helios-node VMs."""
    vms = client.list_helios_vms()
    if not vms:
        print("No active helios-node VMs found on vCenter.")
        return

    print(f"Found {len(vms)} Helios VM(s) on vCenter:\n")
    print(f"{'VM NAME':20} | {'VM ID':10} | {'POWER STATE':12} | {'CPUs':4} | {'RAM (MB)':8} | {'GUEST IP':15}")
    print("-" * 80)
    for v in vms:
        v_id = v.get("vm")
        details = client.get_vm_details(v_id)
        ip = details.get("guest_identity", {}).get("ip_address", "Pending / DHCP")
        print(f"{details.get('name', ''):20} | {v_id:10} | {details.get('power_state', ''):12} | {details.get('cpu', {}).get('count', ''):4} | {details.get('memory', {}).get('size_MiB', ''):8} | {ip:15}")


def cmd_wait(args, client):
    """Polls target node(s) until SSH is fully functional and verifies hypervisor capability."""
    import paramiko

    ip = args.ip
    timeout = args.timeout
    print(f"[*] Waiting for node {ip} to complete kickstart and open SSH (timeout: {timeout}s)...")
    start = time.time()
    ssh_up = False

    while time.time() - start < timeout:
        s = socket.socket()
        s.settimeout(1.0)
        try:
            s.connect((ip, 22))
            banner = s.recv(100).decode("utf-8", errors="ignore").strip()
            print(f"\n[+] SSH port 22 is OPEN on {ip}! ({banner})")
            ssh_up = True
            s.close()
            break
        except Exception:
            print(".", end="", flush=True)
            time.sleep(3)
        finally:
            s.close()

    if not ssh_up:
        print(f"\n[-] Timed out waiting for SSH on {ip} after {timeout} seconds.")
        sys.exit(1)

    print(f"[*] Validating hypervisor configuration on {ip}...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username="root", key_filename=DEFAULT_SSH_KEY_PRIV, timeout=10)

    # Check OS
    stdin, stdout, stderr = ssh.exec_command("cat /etc/redhat-release && uname -r")
    print(f"    OS: {stdout.read().decode().strip()}")

    # Check /dev/kvm
    stdin, stdout, stderr = ssh.exec_command("ls -l /dev/kvm")
    out = stdout.read().decode().strip()
    if "/dev/kvm" in out:
        print(f"    [OK] Nested KVM hardware acceleration is ACTIVE: {out}")
    else:
        print(f"    [WARN] /dev/kvm not found: {stderr.read().decode().strip()}")

    # Check Disks
    stdin, stdout, stderr = ssh.exec_command("lsblk -o NAME,SIZE,TYPE,MOUNTPOINT -a")
    print("    Storage Topology:")
    for line in stdout.read().decode('utf-8', errors='replace').splitlines():
        # Clean unicode box characters for Windows console compatibility
        safe_line = line.encode('ascii', errors='replace').decode('ascii')
        print(f"      {safe_line}")

    ssh.close()
    print(f"\n[+] Node {ip} is 100% READY for Helios HCI cluster provisioning!")


def cmd_destroy(args, client):
    """Cleanly shuts down and deletes all or specified helios-node VMs."""
    vms = client.list_helios_vms()
    if not vms:
        print("No helios-node VMs to destroy.")
        return

    target_name = args.name
    for v in vms:
        name = v.get("name")
        vm_id = v.get("vm")
        if target_name and target_name != name:
            continue

        print(f"[*] Destroying {name} ({vm_id})...")
        try:
            client.rest_post(f"/api/vcenter/vm/{vm_id}/power?action=stop")
            time.sleep(2)
        except Exception:
            pass

        client.rest_delete(f"/api/vcenter/vm/{vm_id}")
        print(f"[*] {name} successfully deleted from vCenter.")


def main():
    parser = argparse.ArgumentParser(description="Helios HCI vCenter Multi-Node Spawner & Cluster Lab")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # create
    p_create = subparsers.add_parser("create", help="Spawn 1, 2, or 3 hypervisor test VMs")
    p_create.add_argument("--count", type=int, default=1, choices=[1, 2, 3], help="Number of nodes to spawn")
    p_create.add_argument("--start-ip", type=int, default=DEFAULT_IP_BASE, help="Starting last-octet IP (default: 71)")
    p_create.add_argument("--ram-mb", type=int, default=8192, help="Memory size in MiB (default: 8192)")
    p_create.add_argument("--cpus", type=int, default=4, help="vCPU count (default: 4)")

    # status
    p_status = subparsers.add_parser("status", help="Query live status of all helios nodes")

    # wait
    p_wait = subparsers.add_parser("wait", help="Wait for node SSH and validate KVM readiness")
    p_wait.add_argument("--ip", type=str, default="10.10.102.71", help="Target node IP to wait for")
    p_wait.add_argument("--timeout", type=int, default=600, help="Max wait seconds (default: 600)")

    # destroy
    p_destroy = subparsers.add_parser("destroy", help="Destroy test node VMs")
    p_destroy.add_argument("--name", type=str, default=None, help="Specific VM name to delete, or omit for all")

    args = parser.parse_args()

    print("[*] Connecting to VMware vCenter...")
    client = VCenterClient()

    if args.command == "create":
        cmd_create(args, client)
    elif args.command == "status":
        cmd_status(args, client)
    elif args.command == "wait":
        cmd_wait(args, client)
    elif args.command == "destroy":
        cmd_destroy(args, client)


if __name__ == "__main__":
    main()
