#!/usr/bin/env python3
import time
import json
import socket
import subprocess
import base64
import sys
import os
import ipaddress
import tempfile

# Distributed firewall rules live in their own chain so they can be rebuilt
# wholesale each pass instead of being appended to FORWARD forever.
FW_CHAIN = "URBOSA-FWD"
FW_PROTOCOLS = ("ANY", "TCP", "UDP", "ICMP")

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

def get_local_addresses():
    """Returns the exact addresses configured on this host, one entry each.

    Parsed rather than substring-matched: VIP 10.10.102.13 is a substring of
    the unrelated host address 10.10.102.130, and a false leadership claim
    stands up a duplicate T0 macvlan uplink IP on the physical network.
    """
    addrs = []
    rc, stdout, _ = run_cmd("ip -json addr show")
    if rc == 0 and stdout:
        try:
            for entry in json.loads(stdout):
                for addr in entry.get("addr_info", []):
                    local = addr.get("local")
                    if local:
                        addrs.append(local)
        except Exception as e:
            sys.stderr.write(f"Error parsing 'ip -json addr show': {e}\n")
    return addrs

def get_iface_addresses(iface, ns_name=None):
    """Returns the exact addresses on one interface, optionally inside a namespace.

    Same substring hazard as get_local_addresses(): a segment gateway 10.0.0.1 is
    a substring of 10.0.0.10, so a plain `in` test reports the gateway as already
    assigned and the segment is left without one.
    """
    prefix = f"ip netns exec {ns_name} " if ns_name else ""
    addrs = []
    rc, stdout, _ = run_cmd(f"{prefix}ip -json addr show {iface}")
    if rc == 0 and stdout:
        try:
            for entry in json.loads(stdout):
                for addr in entry.get("addr_info", []):
                    local = addr.get("local")
                    if local:
                        addrs.append(local)
        except Exception as e:
            sys.stderr.write(f"Error parsing addresses for {iface}: {e}\n")
    return addrs

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
    return vip in get_local_addresses()

def read_proc_argv(pid):
    """Returns the argv list of pid, or [] if it cannot be read."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read().decode("utf-8", errors="ignore")
        return [a for a in raw.split("\0") if a]
    except Exception:
        return []

def netns_dnsmasq_pids(ns_name, iface):
    """PIDs of the dnsmasq bound to iface inside network namespace ns_name.

    'ip netns exec <ns> pgrep dnsmasq' is not namespace aware: netns exec
    changes only the NETWORK namespace, so the process table is still the
    host-wide one and pgrep matches dnsmasq processes owned by every other
    namespace. 'ip netns pids' filters by actual namespace membership, and the
    argv comparison below is exact-token so --interface=veth-t1-10 never
    matches --interface=veth-t1-100.
    """
    pids = []
    rc, stdout, _ = run_cmd(f"ip netns pids {ns_name}")
    if rc != 0 or not stdout:
        return pids
    for pid in stdout.split():
        if not pid.isdigit():
            continue
        argv = read_proc_argv(pid)
        if not argv:
            continue
        if "dnsmasq" not in os.path.basename(argv[0]):
            continue
        if f"--interface={iface}" not in argv:
            continue
        pids.append(pid)
    return pids

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

# Last firewall-table read failure logged, so a sustained outage does not
# repeat the same message on every 15s pass.
fw_read_error = None

def get_db_firewall_rules():
    """Reads the firewall table.

    Returns None if the read failed and a list (possibly empty) if it
    succeeded. The chain is rebuilt from this result, so a partial read would
    silently DELETE the rules that failed to parse - the caller must skip the
    rebuild entirely rather than act on an incomplete ruleset.
    """
    global fw_read_error
    cql = "SELECT JSON * FROM hydra.urbosa_firewall_rules;"
    rc, stdout, stderr = run_cql_query(cql)
    err = None
    items = []
    if rc != 0:
        err = f"Read of hydra.urbosa_firewall_rules failed (rc={rc}): {stderr or stdout}."
    else:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    items.append(json.loads(line))
                except Exception as e:
                    err = f"Unparseable row in hydra.urbosa_firewall_rules ({e}); treating this read as untrustworthy."
                    break

    # Logged on change only: this runs every 15s.
    if err != fw_read_error:
        if err:
            print(f"{err} Skipping firewall rebuild; leaving {FW_CHAIN} as-is.")
        elif fw_read_error is not None:
            print(f"Read of hydra.urbosa_firewall_rules recovered; resuming {FW_CHAIN} rebuild.")
        fw_read_error = err

    if err:
        return None
    return items

# Last notice logged per firewall rule id. The control loop re-validates every
# rule every 15s, so warnings are logged on change rather than on every pass.
fw_rule_notices = {}

def note_fw_rule(rule_id, message):
    """Logs message for rule_id only when it differs from the previous pass."""
    key = str(rule_id)
    if fw_rule_notices.get(key) != message:
        if message:
            print(message)
        fw_rule_notices[key] = message

def validate_fw_address(value, field):
    """Validates an address field. Returns (normalized_or_ANY, error_message)."""
    if value is None:
        return "ANY", None
    value = str(value).strip()
    if not value or value.upper() == "ANY":
        return "ANY", None
    try:
        # strict=True: '10.0.0.5/24' is rejected rather than silently widened
        # to the whole 10.0.0.0/24 subnet.
        net = ipaddress.ip_network(value, strict=True)
    except ValueError as e:
        return None, f"{field} '{value}' is not a valid IPv4 address or CIDR ({e})"
    if net.version != 4:
        return None, f"{field} '{value}' is IPv6; only IPv4 rules are supported"
    return str(net), None

def build_firewall_rule_specs(rule):
    """Validates one DB row into iptables argument strings.

    Returns (list_of_arg_strings, None) or (None, reason). Every field here is
    operator input arriving through the web API and is interpolated into a
    command run as root, so nothing is trusted: addresses must parse via
    ipaddress, protocol must be in the allowlist, port must be 0 (any) or
    1-65535, action must be ALLOW or DENY. A rule that fails any check is never
    executed.
    """
    src, err = validate_fw_address(rule.get("source_ip"), "source_ip")
    if err:
        return None, err
    dst, err = validate_fw_address(rule.get("dest_ip"), "dest_ip")
    if err:
        return None, err

    proto = str(rule.get("protocol") or "ANY").strip().upper()
    if proto not in FW_PROTOCOLS:
        return None, f"protocol '{proto}' is not one of {'/'.join(FW_PROTOCOLS)}"

    raw_port = rule.get("port")
    if raw_port is None or raw_port == "":
        raw_port = 0
    try:
        port = int(raw_port)
    except (TypeError, ValueError):
        return None, f"port '{raw_port}' is not an integer"
    if port != 0 and not (1 <= port <= 65535):
        return None, f"port {port} is outside the valid range 1-65535"

    act = str(rule.get("action") or "").strip().upper()
    if act not in ("ALLOW", "DENY"):
        return None, f"action '{act}' is not ALLOW or DENY"

    target = "ACCEPT" if act == "ALLOW" else "DROP"
    match = ""
    if src != "ANY":
        match += f"-s {src} "
    if dst != "ANY":
        match += f"-d {dst} "

    if port and proto == "ICMP":
        return None, f"port {port} is set but protocol is ICMP, which has no ports"

    if port and proto == "ANY":
        # iptables cannot express --dport without -p. The old code dropped the
        # port silently, turning 'any protocol, port 443' into a rule matching
        # ALL traffic. Expand into explicit tcp+udp instead: it preserves the
        # admin's intent and can only ever narrow the match, never widen it.
        note_fw_rule(rule.get("rule_id"), f"Urbosa firewall: rule {rule.get('rule_id')} specifies protocol ANY with port {port}; iptables cannot match a port without a protocol, so expanding it into explicit tcp and udp rules.")
        return [f"{match}-p tcp --dport {port} -j {target}",
                f"{match}-p udp --dport {port} -j {target}"], None

    note_fw_rule(rule.get("rule_id"), None)

    if proto == "ANY":
        return [f"{match}-j {target}"], None

    spec = f"{match}-p {proto.lower()} "
    if port:
        spec += f"--dport {port} "
    return [f"{spec}-j {target}"], None

def firewall_rule_sort_key(rule):
    """Deterministic ordering, identical on every node.

    'SELECT JSON *' returns rows in token order, which differs per node, so
    appending in row order gave each host a different first-match-wins policy
    from the same ruleset. priority is the operator-visible ordering column
    (the WebUI sorts ascending); rule_id breaks ties.
    """
    try:
        priority = int(rule.get("priority"))
    except (TypeError, ValueError):
        priority = 1 << 30
    return (priority, str(rule.get("rule_id", "")))

def ensure_firewall_chain():
    """Creates FW_CHAIN and installs exactly one jump to it from FORWARD."""
    rc, _, _ = run_cmd(f"iptables -n -L {FW_CHAIN}")
    if rc != 0:
        print(f"Creating distributed firewall chain {FW_CHAIN}...")
        run_cmd(f"iptables -N {FW_CHAIN}")
    rc_jump, _, _ = run_cmd(f"iptables -C FORWARD -j {FW_CHAIN}")
    if rc_jump != 0:
        print(f"Installing FORWARD jump to {FW_CHAIN}...")
        run_cmd(f"iptables -I FORWARD 1 -j {FW_CHAIN}")

# Last iptables-restore fallback reason logged, so a permanently unavailable
# iptables-restore does not repeat the same warning every pass.
fw_restore_warned = None

def count_chain_rules(chain):
    """Number of rules currently in chain, or -1 if it cannot be read."""
    rc, stdout, _ = run_cmd(f"iptables -S {chain}")
    if rc != 0:
        return -1
    return len([l for l in stdout.splitlines() if l.strip().startswith("-A ")])

def apply_firewall_rules(firewall_rules):
    """Rebuilds FW_CHAIN from the database ruleset.

    Owning a dedicated chain means a rule deleted from the database actually
    disappears from the host, instead of living in FORWARD forever. Validation
    of every rule happens before anything is touched, so a bad row can never
    leave the chain half-applied.
    """
    global fw_restore_warned
    ensure_firewall_chain()

    specs = []
    seen_ids = set()
    for rule in sorted(firewall_rules, key=firewall_rule_sort_key):
        seen_ids.add(str(rule.get("rule_id")))
        rule_specs, err = build_firewall_rule_specs(rule)
        if err:
            note_fw_rule(rule.get("rule_id"), f"Urbosa firewall: SKIPPING invalid rule {rule.get('rule_id')} ({rule.get('description')}): {err}")
            continue
        specs.extend(rule_specs)

    # Forget notices for rules that no longer exist in the database.
    for stale in [k for k in fw_rule_notices if k not in seen_ids]:
        del fw_rule_notices[stale]

    # Preferred path: one iptables-restore transaction replaces the whole
    # chain atomically, so traffic never sees a partially built ruleset.
    # --noflush keeps every other chain (including the FORWARD jump) intact.
    payload = ["*filter", f":{FW_CHAIN} - [0:0]", f"-F {FW_CHAIN}"]
    payload.extend(f"-A {FW_CHAIN} {spec}" for spec in specs)
    payload.append("COMMIT")

    restored = False
    fallback_reason = None
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(prefix="urbosa-fw-", suffix=".rules")
        with os.fdopen(fd, "w") as f:
            f.write("\n".join(payload) + "\n")
        rc_r, _, err_r = run_cmd(f"iptables-restore -n {tmp_path}")
        if rc_r != 0:
            fallback_reason = f"iptables-restore failed ({err_r})"
        elif count_chain_rules(FW_CHAIN) != len(specs):
            fallback_reason = f"iptables-restore left {FW_CHAIN} with an unexpected rule count"
        else:
            restored = True
    except Exception as e:
        fallback_reason = f"could not stage iptables-restore ({e})"
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    if not restored:
        # Non-atomic fallback: brief window where the chain is empty.
        if fallback_reason != fw_restore_warned:
            print(f"Urbosa firewall: {fallback_reason}; falling back to flush and append.")
            fw_restore_warned = fallback_reason
        run_cmd(f"iptables -F {FW_CHAIN}")
        for spec in specs:
            rc_a, _, err_a = run_cmd(f"iptables -A {FW_CHAIN} {spec}")
            if rc_a != 0:
                print(f"Urbosa firewall: failed to install rule '{spec}': {err_a}")
    else:
        fw_restore_warned = None

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
                        ip_clean = ext_ip.split('/')[0]
                        if ip_clean not in get_iface_addresses(mv_name, ns_name):
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
                    # Check for a dnsmasq bound to lo INSIDE this namespace.
                    # A bare 'ip netns exec ... pgrep dnsmasq' saw every other
                    # namespace's dnsmasq, so this server never started.
                    if not netns_dnsmasq_pids(ns_name, "lo"):
                        print(f"Starting DHCP server (dnsmasq) inside {ns_name}...")
                        # Run dnsmasq inside namespace (dummy start, catches error if sandbox blocks)
                        run_cmd(f"ip netns exec {ns_name} dnsmasq --bind-interfaces --interface=lo --dhcp-range=100.64.0.2,100.64.0.254,12h")

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
                        if t1_ip.split('/')[0] not in get_iface_addresses(veth_t1, ns_name):
                            run_cmd(f"ip netns exec {ns_name} ip addr add {t1_ip} dev {veth_t1}")

                        # Assign transit IP to T0 interface
                        if t0_ip.split('/')[0] not in get_iface_addresses(veth_t0, t0_ns):
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
                    phys_if = get_uplink_interface("eth0")
                    run_cmd(f"ip link add {vx_name} type vxlan id {s['vni']} dstport 4789 dev {phys_if} 2>/dev/null || ip link add {vx_name} type vxlan id {s['vni']} dstport 4789")
                    run_cmd(f"ip link set {vx_name} master {br_name} 2>/dev/null")
                    run_cmd(f"ip link set {vx_name} up")
                
                # Reconcile FDB flooding entries for VXLAN mesh
                node_ips = []
                try:
                    cql_nodes = "SELECT JSON ip FROM hydra.nodes;"
                    rc_n, out_n, _ = run_cql_query(cql_nodes)
                    if rc_n == 0 and out_n:
                        for line in out_n.splitlines():
                            line = line.strip()
                            if line.startswith("{") and line.endswith("}"):
                                node_ips.append(json.loads(line)["ip"])
                except Exception:
                    pass
                local_ip = get_local_ip()
                for ip in node_ips:
                    if ip and ip != local_ip:
                        run_cmd(f"bridge fdb append 00:00:00:00:00:00 dev {vx_name} dst {ip} 2>/dev/null || true")

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
                            if gw_ip not in get_iface_addresses(veth_ns, ns_name):
                                print(f"Assigning gateway IP {gw_ip}/{mask} to interface {veth_ns} inside {ns_name}...")
                                run_cmd(f"ip netns exec {ns_name} ip addr add {gw_ip}/{mask} dev {veth_ns} 2>/dev/null || true")

                        # DHCP Server Configuration for the segment
                        dhcp_enabled = s.get("dhcp_enabled")
                        dhcp_start = s.get("dhcp_start")
                        dhcp_end = s.get("dhcp_end")
                        
                        if dhcp_enabled and dhcp_start and dhcp_end:
                            # Namespace-scoped, exact-interface match.
                            if not netns_dnsmasq_pids(ns_name, veth_ns):
                                print(f"Starting DHCP server (dnsmasq) inside {ns_name} for segment interface {veth_ns}...")
                                run_cmd(f"ip netns exec {ns_name} dnsmasq --bind-interfaces --except-interface=lo --interface={veth_ns} --dhcp-range={dhcp_start},{dhcp_end},12h --dhcp-option=option:router,{gw_ip}")
                        else:
                            # Only kill PIDs proven to live in THIS namespace and
                            # to be bound to THIS interface. PIDs are host-global
                            # (netns does not isolate the process table), so the
                            # kill runs directly rather than via netns exec.
                            dns_pids = netns_dnsmasq_pids(ns_name, veth_ns)
                            if dns_pids:
                                print(f"Stopping DHCP server inside {ns_name} for segment interface {veth_ns}...")
                                for pid in dns_pids:
                                    run_cmd(f"kill -9 {pid}")

            # 4. Reconcile Distributed Firewall (iptables micro-segmentation)
            # firewall_rules is None when the read failed; the chain is left
            # untouched rather than rebuilt from an incomplete ruleset.
            # get_db_firewall_rules() has already logged the reason.
            if firewall_rules is not None:
                apply_firewall_rules(firewall_rules)

        except Exception as e:
            sys.stderr.write(f"Error in Urbosa control loop: {e}\n")

        time.sleep(15)

if __name__ == "__main__":
    main()
