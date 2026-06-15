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

def is_zookeeper_leader():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        s.connect(("127.0.0.1", 2181))
        s.sendall(b"stat")
        resp = s.recv(1024).decode('utf-8', errors='ignore')
        s.close()
        return "mode: leader" in resp.lower()
    except Exception:
        return False

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
            leader = is_zookeeper_leader()
            bound = is_vip_bound(iface, vip)
            
            if leader:
                if not bound:
                    print(f"I am the ZooKeeper leader. Binding VIP {vip} to {iface}...")
                    cmd_add = f"ip addr add {vip}/24 dev {iface} label {iface}:vip"
                    subprocess.run(cmd_add, shell=True)
                    # Broadcast Gratuitous ARP
                    print(f"Broadcasting GARP for VIP {vip} on {iface}...")
                    cmd_arp = f"/usr/sbin/arping -U -c 3 -I {iface} {vip}"
                    subprocess.run(cmd_arp, shell=True)
            else:
                if bound:
                    print(f"I am a follower. Releasing VIP {vip} from {iface}...")
                    cmd_del = f"ip addr del {vip}/24 dev {iface} label {iface}:vip"
                    subprocess.run(cmd_del, shell=True)
                    
        except Exception as e:
            sys.stderr.write(f"Error in Bifrost loop: {e}\n")
            
        time.sleep(2)

if __name__ == "__main__":
    main()

