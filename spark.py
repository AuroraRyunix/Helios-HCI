#!/usr/bin/env python3
import sys
import subprocess
import json
import socket
import os

def run_local(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return res.returncode, res.stdout.decode('utf-8', errors='ignore').strip(), res.stderr.decode('utf-8', errors='ignore').strip()

def check_tcp_port(port, local_ip=None):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.3)
        s.connect(("127.0.0.1", port))
        s.close()
        return True
    except Exception:
        pass

    if local_ip and local_ip != "127.0.0.1":
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.3)
            s.connect((local_ip, port))
            s.close()
            return True
        except Exception:
            pass

    return False

def check_mount(path):
    try:
        with open("/proc/mounts", "r") as f:
            mounts = f.read()
        return path in mounts
    except Exception:
        return os.path.ismount(path)

def get_local_maintenance_status(ip_addr):
    if os.path.exists("/etc/hci/maintenance.state"):
        return "IN_MAINTENANCE"
    # Query ScyllaDB container if active
    rc_db, out_db, _ = run_local("podman ps --filter name=systemd-hydra-db --format '{{.Status}}'")
    if "up" in out_db.lower():
        # Try Daruk HTTP proxy first
        try:
            import urllib.request
            import json
            url = "http://127.0.0.1:9043/query"
            cql = "SELECT ip, status FROM hydra.nodes;"
            req = urllib.request.Request(url, data=cql.encode('utf-8'), headers={'Content-Type': 'text/plain'})
            with urllib.request.urlopen(req, timeout=5) as response:
                res = json.loads(response.read().decode('utf-8'))
                if res.get("status") == "success":
                    for row in res.get("rows", []):
                        if row.get("ip") == ip_addr:
                            status = row.get("status")
                            if status in ["IN_MAINTENANCE", "ENTERING_MAINTENANCE"]:
                                return status
                    return "NORMAL"
        except Exception:
            pass

        # Fallback to cqlsh via podman exec
        import base64
        cql = "SELECT ip, status FROM hydra.nodes;"
        b64_cql = base64.b64encode(cql.encode()).decode()
        cmd = f"echo {b64_cql} | base64 -d | podman exec -i systemd-hydra-db cqlsh {ip_addr}"
        rc, stdout, _ = run_local(cmd)
        if rc == 0 and stdout:
            for line in stdout.splitlines():
                if ip_addr in line:
                    if "IN_MAINTENANCE" in line:
                        return "IN_MAINTENANCE"
                    elif "ENTERING_MAINTENANCE" in line:
                        return "ENTERING_MAINTENANCE"
    return "NORMAL"

def check_urbosa_enabled():
    # Try Daruk HTTP proxy first
    try:
        import urllib.request
        import json
        url = "http://127.0.0.1:9043/query"
        cql = "SELECT value FROM hydra.cluster_settings WHERE key = 'urbosa_enabled';"
        req = urllib.request.Request(url, data=cql.encode('utf-8'), headers={'Content-Type': 'text/plain'})
        with urllib.request.urlopen(req, timeout=5) as response:
            res = json.loads(response.read().decode('utf-8'))
            if res.get("status") == "success":
                for row in res.get("rows", []):
                    if row.get("value") == "true":
                        return True
                return False
    except Exception:
        pass

    # Fallback to cqlsh via podman exec
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        local_ip = '127.0.0.1'
    
    import base64
    cql = "SELECT value FROM hydra.cluster_settings WHERE key = 'urbosa_enabled';"
    b64_query = base64.b64encode(cql.encode('utf-8')).decode('utf-8')
    cmd = f'echo {b64_query} | base64 -d | podman exec -i systemd-hydra-db cqlsh {local_ip}'
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, _ = p.communicate()
    if p.returncode == 0 and stdout:
        for line in stdout.decode('utf-8', errors='ignore').splitlines():
            if "true" in line.strip().lower():
                return True
    return False

def get_dfs_engine():
    return "linstor"

def show_status_json():
    services = ["zookeeper", "hydra-db", "aether", "spark-daemon", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "hylia", "gatoway", "logos", "mipha", "daruk", "agahnim", "slate"]
    svc_map = {
        "zookeeper": "ZooKeeper",
        "hydra-db": "HydraDB",
        "aether": "Aether",
        "spark-daemon": "Spark",
        "spectrum": "Spectrum",
        "bifrost": "Bifrost",
        "dagur": "Dagur",
        "mimir": "Mimir",
        "vali": "Vali",
        "catalyst": "Catalyst",
        "hylia": "Hylia",
        "gatoway": "Gatoway",
        "logos": "Logos",
        "mipha": "Mipha",
        "daruk": "Daruk",
        "agahnim": "Agahnim",
        "slate": "Slate"
    }
    if check_urbosa_enabled():
        services.append("urbosa")
        svc_map["urbosa"] = "Urbosa"
    
    _, hostname, _ = run_local("hostname")
    hostname = hostname.strip()
    
    ip_addr = "127.0.0.1"
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        s.connect(("8.8.8.8", 80))
        ip_addr = s.getsockname()[0]
        s.close()
    except Exception:
        pass
        
    is_leader = False
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        s.connect(("127.0.0.1", 2181))
        s.sendall(b"stat")
        resp = s.recv(1024).decode('utf-8', errors='ignore')
        s.close()
        if "mode: leader" in resp.lower() or "mode: standalone" in resp.lower():
            is_leader = True
    except Exception:
        pass
        
    maint_status = get_local_maintenance_status(ip_addr)
    result = {
        "ip": ip_addr,
        "hostname": hostname,
        "zk_leader": is_leader,
        "maintenance_status": maint_status,
        "services": {}
    }
    
    for svc in services:
        rc, out, _ = run_local(f"systemctl is-active {svc}")
        is_active = (rc == 0 and out.strip() == "active")
        
        healthy = True
        if is_active:
            if svc == "zookeeper":
                healthy = check_tcp_port(2181, ip_addr)
            elif svc == "hydra-db":
                healthy = check_tcp_port(9042, ip_addr)
            elif svc == "daruk":
                healthy = check_tcp_port(9043, "127.0.0.1")
            elif svc == "spark-daemon":
                healthy = check_tcp_port(9099, ip_addr)
            elif svc == "spectrum":
                healthy = check_tcp_port(8443, ip_addr)
            elif svc == "vali":
                healthy = check_tcp_port(9095, ip_addr)
            elif svc == "agahnim":
                healthy = check_tcp_port(8081, ip_addr)
            elif svc == "slate":
                healthy = check_tcp_port(443, ip_addr)
            elif svc == "aether":
                healthy = check_tcp_port(3366, ip_addr)
 
        if is_active and healthy:
            if svc in ["spark-daemon", "bifrost", "dagur", "mimir", "vali", "catalyst", "hylia", "gatoway", "urbosa", "logos", "mipha", "daruk", "agahnim"]:
                _, pid_out, _ = run_local(f"systemctl show -p MainPID --value {svc}")
                pids = [pid_out.strip()] if (pid_out.strip() and pid_out.strip() != "0") else []
            else:
                rc_top, top_out, _ = run_local(f"podman top systemd-{svc} hpid")
                pids = []
                if rc_top == 0:
                    lines = top_out.strip().splitlines()
                    if len(lines) > 1:
                        pids = [line.strip() for line in lines[1:] if line.strip() and line.strip() != "?"]
            result["services"][svc_map[svc]] = {
                "status": "UP",
                "pids": pids
            }
        else:
            result["services"][svc_map[svc]] = {
                "status": "DOWN",
                "pids": []
            }
            
    print(json.dumps(result))
 
def show_status():
    services = ["zookeeper", "hydra-db", "aether", "spark-daemon", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "hylia", "gatoway", "logos", "mipha", "daruk", "agahnim", "slate"]
    svc_map = {
        "zookeeper": "ZooKeeper",
        "hydra-db": "HydraDB",
        "aether": "Aether",
        "spark-daemon": "Spark",
        "spectrum": "Spectrum",
        "bifrost": "Bifrost",
        "dagur": "Dagur",
        "mimir": "Mimir",
        "vali": "Vali",
        "catalyst": "Catalyst",
        "hylia": "Hylia",
        "gatoway": "Gatoway",
        "logos": "Logos",
        "mipha": "Mipha",
        "daruk": "Daruk",
        "agahnim": "Agahnim",
        "slate": "Slate"
    }
    if check_urbosa_enabled():
        services.append("urbosa")
        svc_map["urbosa"] = "Urbosa"
    
    _, hostname, _ = run_local("hostname")
    
    ip_addr = "127.0.0.1"
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        s.connect(("8.8.8.8", 80))
        ip_addr = s.getsockname()[0]
        s.close()
    except Exception:
        pass
        
    is_leader = False
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        s.connect(("127.0.0.1", 2181))
        s.sendall(b"stat")
        resp = s.recv(1024).decode('utf-8', errors='ignore')
        s.close()
        if "mode: leader" in resp.lower() or "mode: standalone" in resp.lower():
            is_leader = True
    except Exception:
        pass
        
    # ANSI Color definitions
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    RESET = "\033[0m"
    GRAY = "\033[90m"

    maint_status = get_local_maintenance_status(ip_addr)
    maint_str = ""
    if maint_status == "IN_MAINTENANCE":
        maint_str = f" {YELLOW}[MAINTENANCE]{RESET}"
    elif maint_status == "ENTERING_MAINTENANCE":
        maint_str = f" {YELLOW}[ENTERING MAINTENANCE]{RESET}"

    print(f"\n        Host: {BOLD}{ip_addr}{RESET} {GREEN}Up{RESET} {GRAY}({hostname.strip()}){RESET}{maint_str}")
    
    for svc in services:
        rc, out, _ = run_local(f"systemctl is-active {svc}")
        is_active = (rc == 0 and out.strip() == "active")
        
        healthy = True
        if is_active:
            if svc == "zookeeper":
                healthy = check_tcp_port(2181, ip_addr)
            elif svc == "hydra-db":
                healthy = check_tcp_port(9042, ip_addr)
            elif svc == "daruk":
                healthy = check_tcp_port(9043, "127.0.0.1")
            elif svc == "spark-daemon":
                healthy = check_tcp_port(9099, ip_addr)
            elif svc == "spectrum":
                healthy = check_tcp_port(8443, ip_addr)
            elif svc == "vali":
                healthy = check_tcp_port(9095, ip_addr)
            elif svc == "catalyst":
                healthy = check_tcp_port(9091, ip_addr)
            elif svc == "agahnim":
                healthy = check_tcp_port(8081, ip_addr)
            elif svc == "slate":
                healthy = check_tcp_port(443, ip_addr)
            elif svc == "aether":
                healthy = check_tcp_port(3366, ip_addr)

        if is_active and healthy:
            if svc in ["spark-daemon", "bifrost", "dagur", "mimir", "vali", "catalyst", "hylia", "gatoway", "urbosa", "logos", "mipha", "daruk", "agahnim"]:
                _, pid_out, _ = run_local(f"systemctl show -p MainPID --value {svc}")
                pids = [pid_out.strip()] if (pid_out.strip() and pid_out.strip() != "0") else []
            else:
                rc_top, top_out, _ = run_local(f"podman top systemd-{svc} hpid")
                pids = []
                if rc_top == 0:
                    lines = top_out.strip().splitlines()
                    if len(lines) > 1:
                        pids = [line.strip() for line in lines[1:] if line.strip() and line.strip() != "?"]
            pid_str = f"{GRAY}[{', '.join(pids)}]{RESET}" if pids else "[]"
            print(f"                    {svc_map[svc]:<16}   {GREEN}UP{RESET}       {pid_str}")
        else:
            print(f"                    {svc_map[svc]:<16}   {RED}DOWN{RESET}")

def check_any_cluster_service_active():
    services = ["spectrum", "aether", "hydra-db", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "logos", "mipha", "slate"]
    for svc in services:
        rc, out, _ = run_local(f"systemctl is-active {svc}")
        if rc == 0 and out.strip() == "active":
            return True
    return False

def main():
    if "--json" in sys.argv:
        show_status_json()
        sys.exit(0)
    elif len(sys.argv) < 2 or sys.argv[1] == "status":
        show_status()
        sys.exit(0)
        
    cmd = sys.argv[1]
    if cmd in ["start", "stop", "restart"]:
        if cmd == "stop":
            # Check for 'all' parameter
            is_all = len(sys.argv) > 2 and sys.argv[2] == "all"
            if not is_all and check_any_cluster_service_active():
                # Check if node is in maintenance mode
                ip_addr = "127.0.0.1"
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.settimeout(0.5)
                    s.connect(("8.8.8.8", 80))
                    ip_addr = s.getsockname()[0]
                    s.close()
                except Exception:
                    pass
                maint_status = get_local_maintenance_status(ip_addr)
                if maint_status in ["IN_MAINTENANCE", "ENTERING_MAINTENANCE"]:
                    print("Node is in maintenance mode. Stopping spark and catalyst services locally...")
                    for svc in ["catalyst", "spark-daemon"]:
                        rc_act, out_act, _ = run_local(f"systemctl is-active {svc}")
                        if rc_act == 0 and out_act.strip() == "active":
                            print(f"Stopping service {svc}...")
                            run_local(f"systemctl stop {svc}")
                    print("Local spark and catalyst services stopped successfully.")
                    sys.exit(0)
                else:
                    print("Error: Cluster services are active on this node. You can only stop local spark if the cluster is not running.")
                    print("To stop all cluster services on this node, run: spark stop all")
                    sys.exit(1)
            
            if is_all:
                print("Stopping all cluster services on this node...")
                services = ["logos", "mipha", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "hylia", "gatoway", "urbosa", "agahnim", "slate", "aether", "hydra-db", "zookeeper", "spark-daemon"]
                for svc in services:
                    rc_act, out_act, _ = run_local(f"systemctl is-active {svc}")
                    if rc_act == 0 and out_act.strip() == "active":
                        print(f"Stopping service {svc}...")
                        run_local(f"systemctl stop {svc}")
                print("All cluster services stopped successfully.")
                sys.exit(0)
            else:
                print("Stopping local spark and zookeeper services...")
                for svc in ["zookeeper", "spark-daemon"]:
                    rc_act, out_act, _ = run_local(f"systemctl is-active {svc}")
                    if rc_act == 0 and out_act.strip() == "active":
                        print(f"Stopping service {svc}...")
                        run_local(f"systemctl stop {svc}")
                print("Spark and ZooKeeper services stopped successfully.")
                sys.exit(0)
                
        if cmd in ["start", "restart"]:
            print("Starting local spark and zookeeper services...")
            for svc in ["zookeeper", "spark-daemon"]:
                print(f"Running systemctl {cmd} {svc}...")
                run_local(f"systemctl {cmd} {svc}")
            print("Spark and ZooKeeper services started successfully.")
        else:
            print(f"Running systemctl {cmd} spark-daemon...")
            rc, _, err = run_local(f"systemctl {cmd} spark-daemon")
            if rc != 0:
                print(f"Error: {err}")
                sys.exit(rc)
            else:
                print(f"Service spark-daemon successfully {cmd}ed locally.")
    else:
        print("Usage: spark [status|start|stop|restart]")
        sys.exit(1)

if __name__ == "__main__":
    main()
