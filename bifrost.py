#!/usr/bin/env python3
import sys
import os
import json
import time
import socket
import subprocess

def get_local_net_info(hosts):
    try:
        res = subprocess.run("ip -json addr show", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if res.returncode == 0:
            data = json.loads(res.stdout.decode('utf-8'))
            for iface in data:
                for addr in iface.get("addr_info", []):
                    local_ip = addr.get("local")
                    if local_ip in hosts:
                        return iface["ifname"], local_ip
    except Exception as e:
        sys.stderr.write(f"Error getting network info: {e}\n")
    return "ens192", None

def get_zookeeper_leader_ip():
    """Finds the IP of the current ZooKeeper leader, with active designated leader fallback if the leader is in maintenance."""
    ips = []
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            ips = [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        ips = ["10.10.102.220", "10.10.102.222", "10.10.102.223"]
        
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
            
    # Check if leader is active on port 8443 (Spectrum)
    leader_active = False
    if leader_ip:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((leader_ip, 8443))
            s.close()
            leader_active = True
        except Exception:
            leader_active = False
            
    if leader_active:
        return leader_ip
        
    # If leader is inactive, find active candidates with port 8443 open
    candidates = []
    for ip in ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((ip, 8443))
            s.close()
            candidates.append(ip)
        except Exception:
            pass
            
    if not candidates:
        return leader_ip if leader_ip else "127.0.0.1"
        
    candidates.sort()
    return candidates[0]

def is_zookeeper_leader(local_ip=None):
    if not local_ip:
        local_ip = "127.0.0.1"
        try:
            with open("/etc/hci/spectrum/spectrum.env", "r") as f:
                for line in f:
                    if "=" in line:
                        k, v = line.strip().split("=", 1)
                        if k == "LOCAL_HYPERVISOR_IP":
                            local_ip = v
                            break
        except Exception:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                local_ip = s.getsockname()[0]
                s.close()
            except Exception:
                pass
    return get_zookeeper_leader_ip() == local_ip

def is_vip_bound(iface, vip):
    try:
        res = subprocess.run(f"ip addr show dev {iface}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if res.returncode == 0:
            return vip in res.stdout.decode('utf-8')
    except Exception:
        pass
    return False

import signal

running = True
current_vip = None
current_iface = None

def signal_handler(signum, frame):
    global running
    sys.stdout.write(f"Received signal {signum}. Stopping Bifrost VIP Manager...\n")
    sys.stdout.flush()
    running = False
    
    if current_vip and current_iface:
        try:
            # Check if bound and delete it
            res = subprocess.run(f"ip addr show dev {current_iface}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if res.returncode == 0 and current_vip in res.stdout.decode('utf-8'):
                sys.stdout.write(f"Releasing VIP {current_vip} from {current_iface} on shutdown...\n")
                sys.stdout.flush()
                cmd_del = f"ip addr del {current_vip}/24 dev {current_iface} label {current_iface}:vip"
                subprocess.run(cmd_del, shell=True)
        except Exception as e:
            sys.stderr.write(f"Error releasing VIP on signal: {e}\n")
            sys.stderr.flush()
    sys.exit(0)

def is_local_spectrum_listening():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        s.connect(("127.0.0.1", 8443))
        s.close()
        return True
    except Exception:
        return False

def main():
    global current_vip, current_iface, running
    print("Bifrost VIP Manager daemon started.")
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    while running:
        try:
            # 1. Read cluster config
            if not os.path.exists("/etc/hci/cluster.json"):
                time.sleep(2)
                continue
            
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
            
            vip = cdata.get("vip")
            hosts = [h["ip"] for h in cdata.get("hosts", [])]
            
            if not vip:
                # No VIP configured yet, wait
                time.sleep(2)
                continue
            
            iface, local_ip = get_local_net_info(hosts)
            if not local_ip:
                # Local IP not in cluster.json, wait
                time.sleep(2)
                continue
            
            # Update global trackers for signal handler
            current_vip = vip
            current_iface = iface
            
            # 2. Check ZK leadership
            leader = is_zookeeper_leader(local_ip)
            bound = is_vip_bound(iface, vip)
            
            if leader and is_local_spectrum_listening():
                if not bound:
                    print(f"I am the ZooKeeper leader and Spectrum is active. Binding VIP {vip} to {iface}...")
                    cmd_add = f"ip addr add {vip}/24 dev {iface} label {iface}:vip"
                    subprocess.run(cmd_add, shell=True)
                    # Broadcast Gratuitous ARP
                    print(f"Broadcasting GARP for VIP {vip} on {iface}...")
                    cmd_arp = f"/usr/sbin/arping -U -c 3 -I {iface} {vip}"
                    subprocess.run(cmd_arp, shell=True)
            else:
                if bound:
                    print(f"Releasing VIP {vip} from {iface} (not leader or local Spectrum is inactive)...")
                    cmd_del = f"ip addr del {vip}/24 dev {iface} label {iface}:vip"
                    subprocess.run(cmd_del, shell=True)
                    
        except Exception as e:
            sys.stderr.write(f"Error in Bifrost loop: {e}\n")
            
        time.sleep(2)

if __name__ == "__main__":
    main()

