#!/usr/bin/env python3
import time
import json
import socket
import subprocess
import base64
import sys
import os

def run_cmd(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return p.returncode, stdout.decode('utf-8', errors='ignore').strip(), stderr.decode('utf-8', errors='ignore').strip()

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def run_cql_query(cql_query, *args, **kwargs):
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
def is_urbosa_enabled():
    cql = "SELECT value FROM hydra.cluster_settings WHERE key = 'urbosa_enabled';"
    rc, stdout, stderr = run_cql_query(cql)
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            if "true" in line.lower():
                return True
    return False

def get_vip():
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                data = json.load(f)
                return data.get("vip")
    except Exception:
        pass
    return None

def is_leader():
    vip = get_vip()
    if not vip:
        try:
            if os.path.exists("/etc/hci/cluster.json"):
                with open("/etc/hci/cluster.json", "r") as f:
                    data = json.load(f)
                    hosts = data.get("hosts", [])
                    if len(hosts) <= 1:
                        return True
                    local_ip = get_local_ip()
                    if hosts and hosts[0].get("ip") == local_ip:
                        return True
        except Exception:
            pass
        return False
    rc, stdout, _ = run_cmd("ip addr show")
    return rc == 0 and vip in stdout

def get_uplink_interface(preferred_if):
    rc, _, _ = run_cmd(f"ip link show {preferred_if}")
    if rc == 0:
        return preferred_if
    rc, stdout, _ = run_cmd("ip route get 8.8.8.8")
    if rc == 0 and "dev " in stdout:
        parts = stdout.split()
        if "dev" in parts:
            idx = parts.index("dev")
            if idx + 1 < len(parts):
                return parts[idx+1]
    rc, stdout, _ = run_cmd("ip route | grep default")
    if rc == 0 and stdout:
        parts = stdout.split()
        if "dev" in parts:
            idx = parts.index("dev")
            if idx + 1 < len(parts):
                return parts[idx+1]
    return preferred_if

def get_db_routers_t0():
    cql = "SELECT JSON * FROM hydra.urbosa_t0_routers;"
    rc, stdout, _ = run_cql_query(cql)
    items = []
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    items.append(json.loads(line))
                except Exception:
                    pass
    return items

def get_db_routers_t1():
    cql = "SELECT JSON * FROM hydra.urbosa_t1_routers;"
    rc, stdout, _ = run_cql_query(cql)
    items = []
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    items.append(json.loads(line))
                except Exception:
                    pass
    return items

def get_db_segments():
    cql = "SELECT JSON * FROM hydra.urbosa_segments;"
    rc, stdout, _ = run_cql_query(cql)
    items = []
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    items.append(json.loads(line))
                except Exception:
                    pass
    return items

def get_db_firewall_rules():
    cql = "SELECT JSON * FROM hydra.urbosa_firewall_rules;"
    rc, stdout, _ = run_cql_query(cql)
    items = []
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    items.append(json.loads(line))
                except Exception:
                    pass
    return items

def main():
    print("Urbosa SDN logical router and overlay orchestrator started.")
    
    while True:
        try:
            if not is_urbosa_enabled():
                time.sleep(5)
                continue
            
            # Fetch resources
            t0_routers = get_db_routers_t0()
            t1_routers = get_db_routers_t1()
            segments = get_db_segments()
            firewall_rules = get_db_firewall_rules()
            
            leader_status = is_leader()
            
            # 1. Reconcile Tier-0 Gateways (Active-Passive Namespace)
            for r in t0_routers:
                ns_name = f"ns-t0-{r['router_id'][:8]}"
                if leader_status:
                    # Ensure namespace exists
                    rc_ns, _, _ = run_cmd(f"ip netns show | grep {ns_name}")
                    if rc_ns != 0:
                        print(f"I am the VIP leader. Creating Tier-0 namespace {ns_name}...")
                        run_cmd(f"ip netns add {ns_name}")
                        run_cmd(f"ip netns exec {ns_name} ip link set lo up")
                    
                    # Set up external uplink inside the namespace
                    ext_if = r.get("uplink_interface", "ens192")
                    ext_ip = r.get("uplink_ip")
                    gw_ip = r.get("gateway_ip")
                    
                    # Detect actual uplink interface dynamically
                    ext_if_detected = get_uplink_interface(ext_if)
                    mv_name = f"mv-t0-{r['router_id'][:8]}"
                    
                    # Ensure macvlan interface exists inside Tier-0 namespace
                    rc_mv, _, _ = run_cmd(f"ip netns exec {ns_name} ip link show {mv_name}")
                    if rc_mv != 0:
                        run_cmd(f"ip link del {mv_name} 2>/dev/null || true")
                        run_cmd(f"ip link add link {ext_if_detected} name {mv_name} type macvlan mode bridge")
                        run_cmd(f"ip link set {mv_name} netns {ns_name}")
                        run_cmd(f"ip netns exec {ns_name} ip link set {mv_name} up")
                    
                    # Assign IP inside netns
                    if ext_ip:
                        _, ip_out, _ = run_cmd(f"ip netns exec {ns_name} ip addr show {mv_name}")
                        ip_clean = ext_ip.split('/')[0]
                        if ip_clean not in ip_out:
                            run_cmd(f"ip netns exec {ns_name} ip addr add {ext_ip} dev {mv_name}")
                    
                    # Set default route inside netns
                    if gw_ip:
                        _, route_out, _ = run_cmd(f"ip netns exec {ns_name} ip route show")
                        if f"default via {gw_ip}" not in route_out:
                            run_cmd(f"ip netns exec {ns_name} ip route del default 2>/dev/null || true")
                            run_cmd(f"ip netns exec {ns_name} ip route add default via {gw_ip} dev {mv_name}")
                    
                    # Ensure IP forwarding is enabled inside the namespace
                    run_cmd(f"ip netns exec {ns_name} sysctl -w net.ipv4.ip_forward=1")
                    
                    # Set up Source NAT (Masquerade)
                    run_cmd(f"ip netns exec {ns_name} iptables -t nat -C POSTROUTING -j MASQUERADE 2>/dev/null || ip netns exec {ns_name} iptables -t nat -A POSTROUTING -j MASQUERADE")
                else:
                    # Clean up namespaces on passive nodes
                    rc_ns, _, _ = run_cmd(f"ip netns show | grep {ns_name}")
                    if rc_ns == 0:
                        print(f"I am not the leader. Removing Tier-0 namespace {ns_name}...")
                        run_cmd(f"ip netns del {ns_name}")
                        mv_name = f"mv-t0-{r['router_id'][:8]}"
                        run_cmd(f"ip link del {mv_name} 2>/dev/null || true")

            # 2. Reconcile Tier-1 Routers (Distributed Namespace)
            for r in t1_routers:
                ns_name = f"ns-t1-{r['router_id'][:8]}"
                # Ensure T1 namespace exists locally on ALL hosts
                rc_ns, _, _ = run_cmd(f"ip netns show | grep {ns_name}")
                if rc_ns != 0:
                    print(f"Creating Tier-1 distributed router namespace {ns_name}...")
                    run_cmd(f"ip netns add {ns_name}")
                    run_cmd(f"ip netns exec {ns_name} ip link set lo up")
                    run_cmd(f"ip netns exec {ns_name} sysctl -w net.ipv4.ip_forward=1")
                
                # Check DHCP status
                if r.get("dhcp_enabled"):
                    # Check if dnsmasq is running inside the namespace
                    rc_dns, _, _ = run_cmd(f"ip netns exec {ns_name} pgrep dnsmasq")
                    if rc_dns != 0:
                        print(f"Starting DHCP server (dnsmasq) inside {ns_name}...")
                        # Run dnsmasq inside namespace (dummy start, catches error if sandbox blocks)
                        run_cmd(f"ip netns exec {ns_name} dnsmasq --interface=lo --dhcp-range=100.64.0.2,100.64.0.254,12h")

                # Connect Tier-1 distributed namespace to Tier-0 edge namespace if linked and active on this host
                t0_id = r.get("t0_link_id")
                if t0_id:
                    t0_ns = f"ns-t0-{t0_id[:8]}"
                    rc_t0_ns, _, _ = run_cmd(f"ip netns show | grep {t0_ns}")
                    if rc_t0_ns == 0:
                        veth_t1 = f"t1-{r['router_id'][:8]}"
                        veth_t0 = f"t0-{r['router_id'][:8]}"
                        
                        # Ensure veth pair exists inside respective namespaces
                        rc_veth, _, _ = run_cmd(f"ip netns exec {ns_name} ip link show {veth_t1}")
                        if rc_veth != 0:
                            run_cmd(f"ip link del {veth_t1} 2>/dev/null || true")
                            run_cmd(f"ip link del {veth_t0} 2>/dev/null || true")
                            run_cmd(f"ip link add {veth_t1} type veth peer name {veth_t0}")
                            run_cmd(f"ip link set {veth_t1} netns {ns_name}")
                            run_cmd(f"ip link set {veth_t0} netns {t0_ns}")
                            run_cmd(f"ip netns exec {ns_name} ip link set {veth_t1} up")
                            run_cmd(f"ip netns exec {t0_ns} ip link set {veth_t0} up")
                        
                        # Generate transit subnet IPs based on hash of t1_router_id
                        import hashlib
                        h_idx = int(hashlib.md5(r['router_id'].encode()).hexdigest()[:4], 16) % 16384
                        octet2 = (h_idx >> 6) & 0xff
                        octet3 = (h_idx & 0x3f) * 4
                        
                        t0_ip = f"100.64.{octet2}.{octet3 + 1}/30"
                        t1_ip = f"100.64.{octet2}.{octet3 + 2}/30"
                        
                        # Assign transit IP to T1 interface
                        _, t1_ip_out, _ = run_cmd(f"ip netns exec {ns_name} ip addr show {veth_t1}")
                        if f"100.64.{octet2}.{octet3 + 2}" not in t1_ip_out:
                            run_cmd(f"ip netns exec {ns_name} ip addr add {t1_ip} dev {veth_t1}")
                            
                        # Assign transit IP to T0 interface
                        _, t0_ip_out, _ = run_cmd(f"ip netns exec {t0_ns} ip addr show {veth_t0}")
                        if f"100.64.{octet2}.{octet3 + 1}" not in t0_ip_out:
                            run_cmd(f"ip netns exec {t0_ns} ip addr add {t0_ip} dev {veth_t0}")
                        
                        # Configure default gateway route in T1 namespace pointing to T0
                        _, t1_routes, _ = run_cmd(f"ip netns exec {ns_name} ip route show")
                        if f"default via 100.64.{octet2}.{octet3 + 1}" not in t1_routes:
                            run_cmd(f"ip netns exec {ns_name} ip route del default 2>/dev/null || true")
                            run_cmd(f"ip netns exec {ns_name} ip route add default via 100.64.{octet2}.{octet3 + 1} dev {veth_t1}")
                        
                        # Add route back to guest subnets inside T0 namespace
                        for s in segments:
                            if s.get("t1_link_id") == r.get("router_id"):
                                subnet = s.get("subnet_cidr")
                                if subnet:
                                    _, t0_routes, _ = run_cmd(f"ip netns exec {t0_ns} ip route show")
                                    if subnet not in t0_routes:
                                        run_cmd(f"ip netns exec {t0_ns} ip route add {subnet} via 100.64.{octet2}.{octet3 + 2} dev {veth_t0}")

            # Fetch MTU from settings, default to 1500
            mtu_size = 1500
            try:
                cql_mtu = "SELECT value FROM hydra.cluster_settings WHERE key = 'dns_mtu';"
                rc_m, out_m, _ = run_cql_query(cql_mtu)
                if rc_m == 0 and out_m:
                    for line in out_m.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        val_clean = "".join(c for c in line if c.isdigit())
                        if val_clean:
                            mtu_size = int(val_clean)
                            break
            except Exception:
                mtu_size = 1500

            # 3. Reconcile Overlay Segments (VXLAN Mesh)
            for s in segments:
                br_name = f"br-ov-{s['vni']}"
                vx_name = f"vxlan-{s['vni']}"
                
                # Ensure overlay bridge exists
                rc_br, _, _ = run_cmd(f"ip link show {br_name}")
                if rc_br != 0:
                    print(f"Creating Overlay segment bridge {br_name}...")
                    run_cmd(f"ip link add {br_name} type bridge")
                    run_cmd(f"ip link set {br_name} up")
                
                # Ensure VXLAN device exists
                rc_vx, _, _ = run_cmd(f"ip link show {vx_name}")
                if rc_vx != 0:
                    print(f"Creating VXLAN tunnel interface {vx_name} (VNI {s['vni']})...")
                    run_cmd(f"ip link add {vx_name} type vxlan id {s['vni']} dstport 4789 dev eth0 2>/dev/null || ip link add {vx_name} type vxlan id {s['vni']} dstport 4789")
                    run_cmd(f"ip link set {vx_name} master {br_name} 2>/dev/null")
                    run_cmd(f"ip link set {vx_name} up")

                # Enforce dynamic MTU
                run_cmd(f"ip link set dev {br_name} mtu {mtu_size} 2>/dev/null || true")
                run_cmd(f"ip link set dev {vx_name} mtu {mtu_size} 2>/dev/null || true")

                # Connect Segment Bridge to Tier-1 Namespace via VETH Pair
                t1_id = s.get("t1_link_id")
                if t1_id:
                    ns_name = f"ns-t1-{t1_id[:8]}"
                    # Ensure T1 namespace exists
                    rc_ns, _, _ = run_cmd(f"ip netns show | grep {ns_name}")
                    if rc_ns == 0:
                        veth_host = f"veth-ov-{s['vni']}"
                        veth_ns = f"veth-t1-{s['vni']}"
                        
                        # Create veth pair if it doesn't exist
                        rc_veth, _, _ = run_cmd(f"ip link show {veth_host}")
                        if rc_veth != 0:
                            print(f"Creating veth pair to connect bridge {br_name} to namespace {ns_name}...")
                            run_cmd(f"ip link add {veth_host} type veth peer name {veth_ns}")
                            run_cmd(f"ip link set {veth_ns} netns {ns_name}")
                            run_cmd(f"ip link set {veth_host} master {br_name}")
                            run_cmd(f"ip link set {veth_host} up")
                            run_cmd(f"ip link set dev {veth_host} mtu {mtu_size} 2>/dev/null || true")
                            
                        # Set MTU inside netns
                        run_cmd(f"ip netns exec {ns_name} ip link set dev {veth_ns} mtu {mtu_size} 2>/dev/null || true")
                        run_cmd(f"ip netns exec {ns_name} ip link set {veth_ns} up")
                        
                        # Assign Gateway IP to the veth inside the namespace
                        gw_ip = s.get("gateway_ip")
                        subnet = s.get("subnet_cidr", "")
                        mask = subnet.split('/')[-1] if '/' in subnet else '24'
                        if gw_ip:
                            # Check if already assigned
                            _, out_ip, _ = run_cmd(f"ip netns exec {ns_name} ip addr show {veth_ns}")
                            if gw_ip not in out_ip:
                                print(f"Assigning gateway IP {gw_ip}/{mask} to interface {veth_ns} inside {ns_name}...")
                                run_cmd(f"ip netns exec {ns_name} ip addr add {gw_ip}/{mask} dev {veth_ns} 2>/dev/null || true")

                        # DHCP Server Configuration for the segment
                        dhcp_enabled = s.get("dhcp_enabled")
                        dhcp_start = s.get("dhcp_start")
                        dhcp_end = s.get("dhcp_end")
                        
                        if dhcp_enabled and dhcp_start and dhcp_end:
                            # Check if dnsmasq is already running for this interface/segment
                            rc_dns, _, _ = run_cmd(f"ip netns exec {ns_name} pgrep -f 'dnsmasq.*{veth_ns}'")
                            if rc_dns != 0:
                                print(f"Starting DHCP server (dnsmasq) inside {ns_name} for segment interface {veth_ns}...")
                                run_cmd(f"ip netns exec {ns_name} dnsmasq --interface={veth_ns} --dhcp-range={dhcp_start},{dhcp_end},12h --dhcp-option=option:router,{gw_ip}")
                        else:
                            # Kill any running dnsmasq for this interface
                            _, dns_pids, _ = run_cmd(f"ip netns exec {ns_name} pgrep -f 'dnsmasq.*{veth_ns}'")
                            if dns_pids:
                                print(f"Stopping DHCP server inside {ns_name} for segment interface {veth_ns}...")
                                for pid in dns_pids.split():
                                    run_cmd(f"ip netns exec {ns_name} kill -9 {pid}")

            # 4. Reconcile Distributed Firewall (iptables micro-segmentation)
            for rule in firewall_rules:
                src = rule.get("source_ip", "ANY")
                dst = rule.get("dest_ip", "ANY")
                proto = rule.get("protocol", "ANY")
                port = rule.get("port", 0)
                act = rule.get("action", "ALLOW")
                
                rule_action = "-j ACCEPT" if act == "ALLOW" else "-j DROP"
                rule_proto = "" if proto == "ANY" else f"-p {proto.lower()}"
                rule_port = "" if (port == 0 or proto == "ANY") else f"--dport {port}"
                rule_src = "" if src == "ANY" else f"-s {src}"
                rule_dst = "" if dst == "ANY" else f"-d {dst}"
                
                # Apply rule to FORWARD chain on host
                cmd = f"iptables -C FORWARD {rule_src} {rule_dst} {rule_proto} {rule_port} {rule_action} 2>/dev/null || iptables -A FORWARD {rule_src} {rule_dst} {rule_proto} {rule_port} {rule_action}"
                run_cmd(cmd)

        except Exception as e:
            sys.stderr.write(f"Error in Urbosa control loop: {e}\n")

        time.sleep(15)

if __name__ == "__main__":
    main()
