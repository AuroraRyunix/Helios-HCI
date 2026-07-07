#!/usr/bin/env python3
import time
import json
import socket
import subprocess
import base64
import sys

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
def get_default_interface():
    rc, stdout, _ = run_cmd("ip route show")
    if rc == 0 and stdout:
        best_iface = None
        lowest_metric = 999999
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("default"):
                parts = line.split()
                try:
                    dev_idx = parts.index("dev")
                    iface = parts[dev_idx + 1]
                    metric = 0
                    if "metric" in parts:
                        metric_idx = parts.index("metric")
                        metric = int(parts[metric_idx + 1])
                    if metric < lowest_metric:
                        lowest_metric = metric
                        best_iface = iface
                except (ValueError, IndexError):
                    pass
        if best_iface:
            return best_iface
    return "ens192"

def get_db_networks():
    cql = "SELECT JSON * FROM hydra.gatoway_networks;"
    rc, stdout, stderr = run_cql_query(cql)
    networks = []
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("{") and line.endswith("}"):
                try:
                    net = json.loads(line)
                    networks.append(net)
                except Exception:
                    pass
    return networks

def get_active_vlan_bridges():
    rc, stdout, _ = run_cmd("ip -o link show | grep -o 'br-vlan-[0-9]*'")
    if rc == 0 and stdout:
        return list(set(stdout.splitlines()))
    return []

def is_gato_enabled():
    cql = "SELECT value FROM hydra.cluster_settings WHERE key = 'gato_enabled';"
    rc, stdout, stderr = run_cql_query(cql)
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            if "true" in line.lower():
                return True
    return False

def main():
    print("Gatoway physical L2 and VLAN coordinator started.")
    phys_iface = get_default_interface()
    print(f"Detected physical network uplink interface: {phys_iface}")

    while True:
        try:
            if not is_gato_enabled():
                time.sleep(5)
                continue

            db_nets = get_db_networks()
            db_vlans = {}
            for net in db_nets:
                if net.get("type") == "vlan" and net.get("vlan_id") is not None:
                    db_vlans[int(net["vlan_id"])] = net

            # 1. Reconcile existing VLAN bridges from database
            for vlan_id, net in db_vlans.items():
                br_name = f"br-vlan-{vlan_id}"
                vlan_iface = f"{phys_iface}.{vlan_id}"

                # Ensure VLAN bridge exists
                rc_br, _, _ = run_cmd(f"ip link show {br_name}")
                if rc_br != 0:
                    print(f"Creating VLAN bridge {br_name}...")
                    run_cmd(f"ip link add {br_name} type bridge")

                # Ensure VLAN sub-interface exists on physical uplink
                rc_vif, _, _ = run_cmd(f"ip link show {vlan_iface}")
                if rc_vif != 0:
                    print(f"Creating VLAN sub-interface {vlan_iface} on {phys_iface}...")
                    run_cmd(f"ip link add link {phys_iface} name {vlan_iface} type vlan id {vlan_id}")

                # Ensure VLAN interface is enslaved to the bridge
                rc_slave, stdout_slave, _ = run_cmd(f"ip link show {vlan_iface}")
                if f"master {br_name}" not in stdout_slave:
                    print(f"Enslaving {vlan_iface} to {br_name}...")
                    run_cmd(f"ip link set {vlan_iface} master {br_name}")

                # Ensure both are set UP
                run_cmd(f"ip link set {vlan_iface} up")
                run_cmd(f"ip link set {br_name} up")

            # 2. Cleanup stale/deleted VLAN bridges
            active_bridges = get_active_vlan_bridges()
            for br in active_bridges:
                try:
                    vlan_id = int(br.split("-")[-1])
                except ValueError:
                    continue

                if vlan_id not in db_vlans:
                    br_name = f"br-vlan-{vlan_id}"
                    vlan_iface = f"{phys_iface}.{vlan_id}"
                    print(f"Cleaning up deleted network segment {br_name}...")
                    run_cmd(f"ip link set {br_name} down")
                    run_cmd(f"ip link delete {br_name}")
                    run_cmd(f"ip link set {vlan_iface} down")
                    run_cmd(f"ip link delete {vlan_iface}")

        except Exception as e:
            sys.stderr.write(f"Error in Gatoway control loop: {e}\n")

        time.sleep(5)

if __name__ == "__main__":
    main()
