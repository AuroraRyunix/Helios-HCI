#!/usr/bin/env python3
import sys
import os
import json
import time
import socket
import subprocess

# Slate/Traefik client-facing ingress. This is the port clients actually reach
# through the VIP (README section 8), so it is what gates VIP ownership.
INGRESS_PORT = 443
# Spectrum WebUI/API. Slate proxies to https://127.0.0.1:8443, so a node with
# 443 up but 8443 down still returns 502 for every client request.
SPECTRUM_PORT = 8443
# The Phoenix console. Slate routes the rebuilt pages to http://127.0.0.1:8444 and
# everything else to 8443, so since that split there are two backends a node has to be
# able to answer on -- and the rebuilt half includes "/", the page an operator lands on.
SPECTRUM_PHX_PORT = 8444
ZK_CLIENT_PORT = 2181

# Probes against other nodes cross the network. 0.2s turned a brief latency
# spike into an apparent consensus loss and flapped the VIP every 2s loop.
REMOTE_PROBE_TIMEOUT = 1.0
# Loopback probes never leave the host, so they stay fast.
LOCAL_PROBE_TIMEOUT = 0.5

def probe_tcp(host, port, timeout):
    """Returns True if a TCP connect to host:port succeeds within timeout."""
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        return True
    except Exception:
        return False
    finally:
        if s is not None:
            try:
                s.close()
            except Exception:
                pass

def get_iface_addresses(iface):
    """Returns the exact addresses configured on iface, one entry per address.

    Used instead of a substring search over 'ip addr show': VIP 10.10.102.13 is
    a substring of the unrelated address 10.10.102.130.
    """
    addrs = []
    try:
        res = subprocess.run(f"ip -json addr show dev {iface}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if res.returncode == 0:
            out = res.stdout.decode('utf-8').strip()
            if out:
                for entry in json.loads(out):
                    for addr in entry.get("addr_info", []):
                        local_ip = addr.get("local")
                        if local_ip:
                            addrs.append(local_ip)
    except Exception as e:
        sys.stderr.write(f"Error reading addresses on {iface}: {e}\n")
    return addrs

def get_local_net_info(hosts):
    try:
        res = subprocess.run("ip -json addr show", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if res.returncode == 0:
            data = json.loads(res.stdout.decode('utf-8'))
            for iface in data:
                for addr in iface.get("addr_info", []):
                    local_ip = addr.get("local")
                    if local_ip in hosts:
                        prefixlen = addr.get("prefixlen", 24)
                        return iface["ifname"], local_ip, prefixlen
    except Exception as e:
        sys.stderr.write(f"Error getting network info: {e}\n")
    return "ens192", None, 24

def get_zookeeper_leader_ip():
    """Finds the IP of the current ZooKeeper leader, with active designated leader fallback if the leader is in maintenance."""
    ips = []
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            ips = [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        ips = ["127.0.0.1"]
        
    leader_ip = None
    for ip in ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(REMOTE_PROBE_TIMEOUT)
            s.connect((ip, ZK_CLIENT_PORT))
            s.sendall(b"stat")
            resp = s.recv(1024).decode('utf-8', errors='ignore')
            s.close()
            if "mode: leader" in resp.lower() or "mode: standalone" in resp.lower():
                leader_ip = ip
                break
        except Exception:
            pass

    # Check the leader is actually serving clients on the Slate ingress port.
    # 8443 (Spectrum) was the wrong signal here: clients reach 443, not 8443.
    leader_active = False
    if leader_ip:
        leader_active = probe_tcp(leader_ip, INGRESS_PORT, REMOTE_PROBE_TIMEOUT)

    if leader_active:
        return leader_ip

    # No leader in a multi-node cluster means consensus is lost. Binding the VIP on the
    # strength of a local guess is how both sides of a partition end up advertising it.
    if not leader_ip and len(ips) > 1:
        sys.stdout.write("ZooKeeper consensus lost or unreachable in multi-node cluster. Refusing split-brain candidate fallback.\n")
        sys.stdout.flush()
        return None

    # Single node: there is no peer to conflict with, so this node may hold the VIP as
    # long as it is actually serving the ingress port.
    if not leader_ip:
        local = ips[0] if ips else "127.0.0.1"
        return local if probe_tcp(local, INGRESS_PORT, REMOTE_PROBE_TIMEOUT) else None

    # A leader exists but is not serving. Deliberately do NOT pick a replacement by sort
    # order: that is a second, independent election which can disagree with the
    # ensemble's, and in a partition each side would choose the lowest candidate it can
    # see -- so both could bind the VIP and produce an address conflict.
    #
    # Releasing is the safe outcome: the WebUI is briefly unreachable, which is visible
    # and recoverable, rather than duplicated, which is neither.
    sys.stdout.write(
        "ZooKeeper leader " + str(leader_ip) + " is not serving the ingress port. "
        "Refusing to elect a replacement independently; releasing the VIP.\n")
    sys.stdout.flush()
    return None

    # If leader is inactive, find active candidates serving the ingress port
    candidates = []
    for ip in ips:
        if probe_tcp(ip, INGRESS_PORT, REMOTE_PROBE_TIMEOUT):
            candidates.append(ip)

    if not candidates:
        return leader_ip if leader_ip else "127.0.0.1"

    # Deliberately do NOT fall back to "lowest reachable candidate" here.
    #
    # Reaching this point means ZooKeeper named a leader but that leader is not serving.
    # Picking a different node by sort order is a second, independent election that can
    # disagree with the ensemble's -- and in a partition each side would pick the lowest
    # candidate *it* can see, so both could bind the VIP and produce an address conflict.
    #
    # Releasing the VIP is the safe outcome: the WebUI is briefly unreachable, which is
    # visible and recoverable, rather than duplicated, which is neither.
    sys.stdout.write(
        "ZooKeeper leader is not serving on the ingress port. Refusing to elect a "
        "replacement independently; releasing the VIP until consensus resolves.\n")
    sys.stdout.flush()
    return None

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
    # Exact element match against the parsed address list, not a substring
    # search over the raw 'ip addr show' text.
    return vip in get_iface_addresses(iface)

import signal

running = True
current_vip = None
current_iface = None
current_prefixlen = 24

def signal_handler(signum, frame):
    global running
    sys.stdout.write(f"Received signal {signum}. Stopping Bifrost VIP Manager...\n")
    sys.stdout.flush()
    running = False
    
    if current_vip and current_iface:
        try:
            # Check if bound (exact match) and delete it. current_prefixlen must
            # be the prefix the VIP was actually added with, or 'ip addr del'
            # silently fails to match and the VIP stays bound on a dying node.
            if current_vip in get_iface_addresses(current_iface):
                sys.stdout.write(f"Releasing VIP {current_vip} from {current_iface} on shutdown...\n")
                sys.stdout.flush()
                cmd_del = f"ip addr del {current_vip}/{current_prefixlen} dev {current_iface} label {current_iface}:vip"
                subprocess.run(cmd_del, shell=True)
        except Exception as e:
            sys.stderr.write(f"Error releasing VIP on signal: {e}\n")
            sys.stderr.flush()
    sys.exit(0)

def is_local_ingress_listening():
    """Local Slate/Traefik ingress on 443 - the port clients actually connect to."""
    return probe_tcp("127.0.0.1", INGRESS_PORT, LOCAL_PROBE_TIMEOUT)

def is_local_spectrum_listening():
    """Local Spectrum WebUI/API on 8443 - the API and the pages not yet rebuilt."""
    return probe_tcp("127.0.0.1", SPECTRUM_PORT, LOCAL_PROBE_TIMEOUT)

def is_local_spectrum_phx_listening():
    """Local Phoenix console on 8444 - the rebuilt pages, including "/"."""
    return probe_tcp("127.0.0.1", SPECTRUM_PHX_PORT, LOCAL_PROBE_TIMEOUT)

last_health_msg = None

def is_local_stack_healthy():
    """VIP health guard.

    443 is the hard gate: it is the client-facing port, and holding the VIP with
    Traefik down blackholes every client. The backends behind it are checked as
    secondary signals, because a node with 443 up and a backend down answers 502
    for whatever that backend serves, which is no better for clients than a
    blackhole.

    There are two backends since the console was split: slate_config/dynamic.yml
    routes the rebuilt pages to 8444 and everything else -- the whole HTTP API and
    the pages not yet rebuilt -- to 8443. Neither half alone is a working console,
    and the 8444 half includes "/", so both gate.

    Failing the guard is not the same as making the console unreachable: every node
    serves 443 on its own address and accepts it as an origin, so an operator can
    still reach one directly while the VIP is looking for a node that has both.

    Each layer is reported separately so an operator can tell which failed, but only
    on transition: this runs every 2s and would otherwise flood the journal while
    degraded.
    """
    global last_health_msg
    msg = None
    if not is_local_ingress_listening():
        msg = f"Local health guard: Slate ingress on 127.0.0.1:{INGRESS_PORT} is not listening."
    elif not is_local_spectrum_listening():
        msg = f"Local health guard: Slate is up but its backend Spectrum on 127.0.0.1:{SPECTRUM_PORT} is not listening."
    elif not is_local_spectrum_phx_listening():
        msg = (f"Local health guard: Slate is up but the Phoenix console on "
               f"127.0.0.1:{SPECTRUM_PHX_PORT} is not listening, so the pages it serves "
               f"-- including the landing page -- would answer 502.")

    if msg != last_health_msg:
        if msg:
            print(msg)
        elif last_health_msg is not None:
            print("Local health guard: Slate ingress and both console backends are healthy again.")
        last_health_msg = msg

    return msg is None

def main():
    global current_vip, current_iface, current_prefixlen, running
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
            
            iface, local_ip, prefixlen = get_local_net_info(hosts)
            if not local_ip:
                # Local IP not in cluster.json, wait
                time.sleep(2)
                continue
            
            # Update global trackers for signal handler
            current_vip = vip
            current_iface = iface
            current_prefixlen = prefixlen
            
            # 2. Check ZK leadership
            leader = is_zookeeper_leader(local_ip)
            bound = is_vip_bound(iface, vip)
            
            if leader and is_local_stack_healthy():
                if not bound:
                    print(f"I am the ZooKeeper leader and the local Slate ingress is active. Binding VIP {vip} to {iface}...")
                    cmd_add = f"ip addr add {vip}/{prefixlen} dev {iface} label {iface}:vip"
                    subprocess.run(cmd_add, shell=True)
                    # Broadcast Gratuitous ARP
                    print(f"Broadcasting GARP for VIP {vip} on {iface}...")
                    cmd_arp = f"/usr/sbin/arping -U -c 3 -I {iface} {vip}"
                    subprocess.run(cmd_arp, shell=True)
            else:
                if bound:
                    print(f"Releasing VIP {vip} from {iface} (not leader or local ingress is inactive)...")
                    cmd_del = f"ip addr del {vip}/{prefixlen} dev {iface} label {iface}:vip"
                    subprocess.run(cmd_del, shell=True)
                    
        except Exception as e:
            sys.stderr.write(f"Error in Bifrost loop: {e}\n")
            
        time.sleep(2)

if __name__ == "__main__":
    main()

