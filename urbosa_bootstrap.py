#!/usr/bin/env python3
import json
import base64
import subprocess
import socket
import ssl
import urllib.request
import uuid
import sys

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

def run_cmd(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return p.returncode, stdout.decode('utf-8', errors='ignore').strip(), stderr.decode('utf-8', errors='ignore').strip()

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
def run_remote_spark(ip, command):
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = False
    
    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=30) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)

def main():
    # 1. Load cluster hosts
    hosts = []
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            hosts = [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        hosts = [get_local_ip()]
        
    if "--cleanup" in sys.argv:
        print(f"Stopping and cleaning up Urbosa SDN on hosts: {', '.join(hosts)}")
        
        # 1. Retrieve all firewall rules first before truncating tables
        fw_rules = []
        rc_fw, out_fw, _ = run_cql_query("SELECT JSON * FROM hydra.urbosa_firewall_rules;")
        if rc_fw == 0 and out_fw:
            for line in out_fw.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        fw_rules.append(json.loads(line))
                    except Exception:
                        pass
        
        # Build iptables cleanup commands
        iptables_cleanup = []
        for rule in fw_rules:
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
            
            iptables_cleanup.append(
                f"while iptables -C FORWARD {rule_src} {rule_dst} {rule_proto} {rule_port} {rule_action} 2>/dev/null; do "
                f"  iptables -D FORWARD {rule_src} {rule_dst} {rule_proto} {rule_port} {rule_action} || break; "
                f"done"
            )
        
        iptables_cmd_str = " && ".join(iptables_cleanup) if iptables_cleanup else "true"
        
        # 2. Iterate hosts to clean up namespaces, processes, links, and firewall rules
        for ip in hosts:
            cmd = (
                "systemctl stop urbosa || true && systemctl disable urbosa || true && "
                "for ns in $(ip netns show | awk '{print $1}'); do "
                "  if [[ \"$ns\" =~ ^ns-t[01]- ]]; then "
                "    for pid in $(ip netns pids \"$ns\" 2>/dev/null); do "
                "      kill -9 \"$pid\" 2>/dev/null || true; "
                "    done; "
                "    ip netns del \"$ns\" || true; "
                "  fi; "
                "done && "
                "for link in $(ip -o link show | awk -F': ' '{print $2}' | cut -d'@' -f1); do "
                "  if [[ \"$link\" =~ ^br-ov- || \"$link\" =~ ^vxlan- ]]; then "
                "    ip link del \"$link\" || true; "
                "  fi; "
                "done && "
                f"{iptables_cmd_str}"
            )
            rc, stdout, stderr = run_remote_spark(ip, cmd)
            if rc == 0:
                print(f"[{ip}] Cleaned up successfully.")
            else:
                print(f"[{ip}] Error during cleanup: {stderr or stdout}")
                
        print("Cleaning up database tables...")
        run_cql_query("TRUNCATE hydra.urbosa_t0_routers;")
        run_cql_query("TRUNCATE hydra.urbosa_t1_routers;")
        run_cql_query("TRUNCATE hydra.urbosa_segments;")
        run_cql_query("TRUNCATE hydra.urbosa_firewall_rules;")
        print("Urbosa cleanup completed successfully.")
        return

    print("Starting Urbosa bootstrap and default configuration...")
        
    # 2. Start urbosa systemd service on all hosts
    print(f"Enabling and starting urbosa service on nodes: {', '.join(hosts)}")
    for ip in hosts:
        rc, stdout, stderr = run_remote_spark(ip, "systemctl enable urbosa && systemctl start urbosa")
        if rc == 0:
            print(f"[{ip}] Service started successfully.")
        else:
            print(f"[{ip}] Warning: Failed to start service: {stderr or stdout}")
            
    # 3. Configure Defaults in ScyllaDB if empty
    print("Checking if default logical routers and segments exist...")
    rc_t0, out_t0, _ = run_cql_query("SELECT router_id FROM hydra.urbosa_t0_routers;")
    if rc_t0 == 0:
        lines = [l.strip() for l in out_t0.splitlines() if l.strip()]
        routers = [l for l in lines if not l.startswith('(') and not l.startswith('-') and l != 'router_id' and l != '']
        if not routers:
            print("No Tier-0 routers found. Dynamic topology creation will be prompted upon enabling SDN in Settings.")
            
            # Default Allow All Firewall Rule
            fw_id = str(uuid.uuid4())
            cql_fw = f"""
            INSERT INTO hydra.urbosa_firewall_rules (rule_id, description, source_ip, dest_ip, protocol, port, action, priority)
            VALUES ({fw_id}, 'Default Allow All Rule', 'ANY', 'ANY', 'ANY', 0, 'ALLOW', 1000);
            """
            run_cql_query(cql_fw)
            
            print("Default Urbosa SDN topology configured successfully!")
        else:
            print("Urbosa SDN configuration already exists. Skipping default topology configuration.")
    else:
        print("Error connecting to ScyllaDB or querying tables. Skipping default seeding.")
        
    print("Urbosa bootstrap completed successfully.")

if __name__ == "__main__":
    main()
