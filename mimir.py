#!/usr/bin/env python3
import sys
import os
import json
import time
import socket
import urllib.request
import ssl
import threading

socket.setdefaulttimeout(45.0)

LOCAL_IP = "127.0.0.1"

# Load local environment settings if available
try:
    with open("/etc/hci/spectrum/spectrum.env", "r") as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                if k == "LOCAL_HYPERVISOR_IP":
                    LOCAL_IP = v
except Exception:
    pass

def run_remote_spark(ip, command):
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = False
    
    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=120) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)

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
            
    # Check if leader is active on port 9091
    leader_active = False
    if leader_ip:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((leader_ip, 9091))
            s.close()
            leader_active = True
        except Exception:
            leader_active = False
            
    if leader_active:
        return leader_ip
        
    # If leader is inactive, find active candidates with port 9091 open
    candidates = []
    for ip in ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((ip, 9091))
            s.close()
            candidates.append(ip)
        except Exception:
            pass
            
    if not candidates:
        return leader_ip if leader_ip else "127.0.0.1"
        
    candidates.sort()
    return candidates[0]

def is_zookeeper_leader():
    return get_zookeeper_leader_ip() == LOCAL_IP

def main():
    print("Mimir health checker daemon started.")
    local_last_run = {}
    while True:
        try:
            if is_zookeeper_leader():
                cql = "SELECT JSON * FROM hydra.mimir_schedules;"
                rc, stdout, stderr = run_cql_query(cql)
                if rc == 0:
                    schedules = []
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                schedules.append(json.loads(line))
                            except Exception:
                                pass
                    
                    now = int(time.time())
                    for s in schedules:
                        if s.get("enabled", False):
                            name = s.get("schedule_name")
                            last_run = s.get("last_run_epoch", 0)
                            interval = 3600 if name == "hourly_checks" else 86400
                            
                            if name in local_last_run and now - local_last_run[name] < interval:
                                continue
                                
                            if now - last_run >= interval:
                                print(f"[Mimir] Triggering check: {name}...")
                                local_last_run[name] = now
                                cql_update = f"UPDATE hydra.mimir_schedules SET last_run_epoch = {now} WHERE schedule_name = '{name}';"
                                run_cql_query(cql_update)
                                
                                category = s.get("category", "all")
                                run_cmd = f"/usr/local/bin/mcli health_checks run_all" if category == "all" else f"/usr/local/bin/mcli health_checks {category}"
                                threading.Thread(target=run_remote_spark, args=("127.0.0.1", run_cmd), daemon=True).start()
        except Exception as e:
            sys.stderr.write(f"Error in Mimir loop: {e}\n")
            
        time.sleep(60)

if __name__ == "__main__":
    main()
