#!/usr/bin/env python3
import sys
import json
import ssl
import socket
import subprocess
import re
import urllib.request
import urllib.error
import time
import os
import threading

LOCAL_IP = "127.0.0.1"
try:
    with open("/etc/hci/spectrum/spectrum.env", "r") as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                if k == "LOCAL_HYPERVISOR_IP":
                    LOCAL_IP = v
except Exception:
    pass

def spark_endpoint(ip):
    """Return (address, verify_identity) for an mTLS call to a spark-daemon.

    Node certificates carry `subjectAltName = IP:<node ip>`, so a connection can only be
    tied to the node answering it when it is addressed by that same IP. Verification used
    to be off here, which meant any certificate the cluster CA ever signed -- every node's
    own included -- satisfied a connection to any other node.

    Loopback is in no node's SAN. spark-daemon binds 0.0.0.0:9099, so this node's own
    address reaches the same listener and does verify; where that address is unknown the
    identity check is dropped rather than failing a call that cannot leave the machine.
    """
    local = globals().get("LOCAL_IP")
    if ip in ("127.0.0.1", "::1", "localhost"):
        if local and local not in ("127.0.0.1", "::1", "localhost"):
            return local, True
        return ip, False
    return ip, True


def run_remote_spark(ip, command):
    """Executes a command on local/remote node via spark-daemon mTLS API."""
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    ip, verify_identity = spark_endpoint(ip)
    context.check_hostname = verify_identity
    
    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=15) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)

def run_mtls_api(ip, path, payload, method="POST"):
    import urllib.error
    
    def execute_request(target_ip):
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
        context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
        target_ip, verify_identity = spark_endpoint(target_ip)
        context.check_hostname = verify_identity
        url = f"https://{target_ip}:9099{path}"
        data = None
        if payload is not None and method != "GET":
            data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, context=context, timeout=120) as response:
                res = json.loads(response.read().decode("utf-8"))
                return 0, res, ""
        except urllib.error.HTTPError as e:
            try:
                res = json.loads(e.read().decode("utf-8"))
                return 0, res, ""
            except Exception:
                return -1, {}, str(e)
        except Exception as e:
            return -1, {}, str(e)

    rc, res, err = execute_request(ip)
    if ip == "127.0.0.1" and (rc != 0 or "error" in res):
        # Try failover to other cluster nodes
        ips = []
        try:
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                ips = [h["ip"] for h in cdata.get("hosts", [])]
        except Exception:
            pass
        for other_ip in ips:
            if other_ip != "127.0.0.1":
                rc_alt, res_alt, err_alt = execute_request(other_ip)
                if rc_alt == 0 and "error" not in res_alt:
                    return rc_alt, res_alt, err_alt
    return rc, res, err

def slugify_image_name(filename):
    """The vdisk id an image is stored under, from its filename.

    Character-for-character what `slugify_image_name` in spectrum_server.py produces.
    The two must agree: a delete that computes a different slug removes nothing, or --
    worse -- something else. Duplicated rather than imported because valcli is a
    stdlib-only script installed on its own, with no import path back to the console.
    """
    base = filename
    for extension in (".iso", ".qcow2", ".img"):
        if filename.lower().endswith(extension):
            base = filename[:-len(extension)]
            break

    slug = re.sub(r"[^a-z0-9_-]", "-", base.lower())
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")[:28]


def run_mtls_spark_api(ip, path, payload, method="POST"):
    """One mTLS call to one node's spark-daemon. No failover, deliberately.

    `run_mtls_api` above retries a failed loopback call on another node, which is right
    for reading cluster state and wrong for everything under /api/v1/dfs/. A vdisk has
    exactly one owner, so "attach on this node" retried elsewhere is not the same request
    -- it is a different and incorrect one. The name matches vali.py and mipha.py, which
    is the semantics these call sites were written against.
    """
    ip, verify_identity = spark_endpoint(ip)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = verify_identity

    url = f"https://{ip}:9099{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=120) as response:
            return 0, json.loads(response.read().decode("utf-8")), ""
    except urllib.error.HTTPError as e:
        # The daemon answers a refused storage operation with 409 and a body naming the
        # host that holds the disk. Swallowing that as a transport error would turn the
        # one useful message in the exchange into "call failed".
        try:
            return 0, json.loads(e.read().decode("utf-8")), ""
        except Exception:
            return -1, {}, str(e)
    except Exception as e:
        return -1, {}, str(e)


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
def print_table(headers, rows):
    """Prints a beautiful ASCII table from headers and row list."""
    if not rows:
        print("No records found.")
        return
        
    str_rows = [[str(val) for val in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in str_rows:
        for idx, val in enumerate(row):
            widths[idx] = max(widths[idx], len(val))
            
    sep = "+" + "+".join(["-" * (w + 2) for w in widths]) + "+"
    print(sep)
    header_line = "| " + " | ".join([f"{h:<{widths[idx]}}" for idx, h in enumerate(headers)]) + " |"
    print(header_line)
    print(sep)
    for row in str_rows:
        row_line = "| " + " | ".join([f"{val:<{widths[idx]}}" for idx, val in enumerate(row)]) + " |"
        print(row_line)
    print(sep)

def cmd_vm_list():
    # Fetch hostnames to IPs map
    host_map = {}
    rc_n, stdout_n, _ = run_cql_query("SELECT JSON hostname, ip FROM hydra.nodes;")
    if rc_n == 0 and stdout_n:
        for line in stdout_n.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    node = json.loads(line)
                    if node.get("ip") and node.get("hostname"):
                        host_map[node["ip"]] = node["hostname"]
                except:
                    pass

    cql = "SELECT JSON name, vcpu, memory, disk_size, state, host_ip FROM hydra.vms;"
    rc, stdout, err = run_cql_query(cql)
    if rc != 0:
        print(err)
        sys.exit(1)
        
    records = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
                
    headers = ["VM Name", "vCPUs", "Memory (MB)", "Disk (GB)", "Host", "Status"]
    rows = []
    for r in records:
        ip = r.get("host_ip")
        if not ip or ip == "None" or ip == "N/A":
            host_display = "N/A"
        else:
            host_display = f"{host_map.get(ip, ip)} ({ip})" if ip in host_map else ip
            
        rows.append([
            r.get("name", "N/A"),
            r.get("vcpu", 1),
            r.get("memory", 1024),
            r.get("disk_size", 10),
            host_display,
            r.get("state", "Stopped")
        ])
    print_table(headers, rows)

def cmd_vm_on(name):
    print(f"Requesting power-on for VM '{name}'...")
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/vm/power", {"name": name, "action": "on"})
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error starting VM: {res['error']}")
        sys.exit(1)
    print(f"Success: {res.get('message', 'VM powered on.')}")

def cmd_vm_off(name):
    print(f"Requesting power-off for VM '{name}'...")
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/vm/power", {"name": name, "action": "off"})
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error stopping VM: {res['error']}")
        sys.exit(1)
    print(f"Success: {res.get('message', 'VM powered off.')}")

def cmd_vm_migrate(name, target_host):
    print(f"Requesting migration for VM '{name}' to host {target_host}...")
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/vm/migrate", {"name": name, "target_host": target_host})
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error migrating VM: {res['error']}")
        sys.exit(1)
    print(f"Success: {res.get('message', 'VM migration triggered.')}")

def cmd_vm_balance():
    print("Requesting manual cluster load rebalancing (DRS)...")
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/vm/balance", {"aggressive": True})
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error rebalancing cluster: {res['error']}")
        sys.exit(1)
    print(f"Success: {res.get('message', 'DRS rebalancing initiated.')}")

def cmd_drs_status():
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/vm/drs", {}, method="GET")
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error querying DRS status: {res['error']}")
        sys.exit(1)
        
    print("==========================================================")
    print("                 DRS Load Balancing Status                ")
    print("==========================================================")
    deviation = res.get("current_deviation", 0.0)
    balance_score = max(0, min(100, int((1 - 2 * deviation) * 100)))
    print(f"Cluster Balance Score : {balance_score}%")
    print(f"Standard Deviation    : {deviation:.4f}")
    print(f"Status String         : {res.get('status_str', 'N/A')}")
    
    last_run = res.get("last_drs_run", 0)
    last_run_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_run)) if last_run else "N/A"
    print(f"Last DRS Run Timestamp: {last_run_str}")
    
    print("\n--- Migration History ---")
    history = res.get("history", [])
    if history:
        headers = ["Time", "VM Name", "Source Host", "Target Host", "Reason"]
        rows = []
        for h in history:
            t_val = h.get("event_time", "")
            if isinstance(t_val, (int, float)):
                t_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t_val / 1000.0))
            else:
                t_str = str(t_val)
            rows.append([
                t_str,
                h.get("vm_name", "N/A"),
                h.get("source_host", "N/A"),
                h.get("target_host", "N/A"),
                h.get("reason", "N/A")
            ])
        print_table(headers, rows)
    else:
        print("No recent DRS migration events.")
    print("==========================================================")

def cmd_storage_list():
    cql = "SELECT JSON name, tier, quota_bytes, path, ftt FROM hydra.storage_containers;"
    rc, stdout, err = run_cql_query(cql)
    if rc != 0:
        print(err)
        sys.exit(1)
        
    records = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
                
    headers = ["Container Name", "Storage Tier", "Quota (GB)", "POSIX Path", "FTT"]
    # Detect host count for FTT override
    hosts_count = 1
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts_count = len(cdata.get("hosts", []))
    except Exception:
        pass

    rows = []
    for r in records:
        quota_bytes = r.get("quota_bytes", 0)
        quota_str = f"{quota_bytes // (1024**3)} GB" if quota_bytes > 0 else "Unlimited"
        ftt_val = r.get("ftt", 1)
        if hosts_count <= 1:
            ftt_val = 0
        rows.append([
            r.get("name", "N/A"),
            r.get("tier", "SSD"),
            quota_str,
            r.get("path", "N/A"),
            ftt_val
        ])
    print("=== Storage Containers ===")
    print_table(headers, rows)
    print()

    # Per-node extent store. This replaces two LINSTOR listings -- `node list` and
    # `volume list` -- printed as the controller rendered them. There is no controller,
    # and each node answers for itself, so the table is built here from what each one
    # says rather than from one host's view of everyone.
    hosts = []
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                hosts = json.load(f).get("hosts", [])
    except Exception:
        pass
    if not hosts:
        hosts = [{"ip": "127.0.0.1", "hostname": "this node"}]

    print("=== Extent Store ===")
    store_rows = []
    for host in hosts:
        ip = host.get("ip")
        if not ip:
            continue
        rc, body, err = run_mtls_spark_api(ip, "/api/v1/dfs/vdisk", {"op": "capacity"})
        if rc != 0 or not isinstance(body, dict):
            store_rows.append([host.get("hostname") or ip, "unreachable",
                               "-", "-", "-",
                               (str(err) or "no response")[:40]])
            continue
        gib = 1024 ** 3
        total = int(body.get("total_bytes") or 0)
        avail = int(body.get("available_bytes") or 0)
        store_rows.append([
            body.get("node") or host.get("hostname") or ip,
            "online",
            "%.1f GiB" % (total / gib),
            "%.1f GiB" % ((total - avail) / gib),
            str(body.get("egroup_count", 0)),
            "%.2f GiB" % (int(body.get("journal_bytes") or 0) / gib),
        ])
    print_table(["Node", "State", "Total", "Used", "Extent groups", "Journal"], store_rows)
    print()

    print("=== Vdisks ===")
    rc_v, out_v, err_v = run_cql_query(
        "SELECT JSON vdisk_id, owner, epoch, rf, replicas, size_bytes FROM hydra.dfs_vdisks;")
    if rc_v != 0:
        print("Warning: hydra.dfs_vdisks could not be read (%s)." % (err_v or out_v))
        return
    vdisk_rows = []
    for line in (out_v or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        want = int(row.get("rf") or 0)
        have = len(row.get("replicas") or [])
        vdisk_rows.append([
            row.get("vdisk_id", "?"),
            row.get("owner") or "unattached",
            str(row.get("epoch", "-")),
            "%d/%d%s" % (have, want, "" if have >= want else "  DEGRADED"),
            "%.1f GiB" % (int(row.get("size_bytes") or 0) / (1024 ** 3)),
        ])
    print_table(["Vdisk", "Owner", "Epoch", "Replicas", "Size"], sorted(vdisk_rows))


def cmd_db_print():
    if len(sys.argv) < 3:
        print("Error: Table name is required.")
        print("Usage: valcli db.print <table_name> [--columns col1,col2,...]")
        sys.exit(1)
        
    table_name = sys.argv[2]
    
    # Check for columns flag
    filter_cols = None
    if "--columns" in sys.argv:
        try:
            idx = sys.argv.index("--columns")
            filter_cols = [c.strip() for c in sys.argv[idx+1].split(",")]
        except Exception:
            print("Error: Invalid --columns format.")
            sys.exit(1)
            
    cql = f"SELECT JSON * FROM hydra.{table_name};"
    rc, stdout, err = run_cql_query(cql)
    if rc != 0:
        print(err)
        sys.exit(1)
        
    records = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
                
    if not records:
        print(f"No records found in table 'hydra.{table_name}'.")
        return
        
    # Set headers
    if filter_cols:
        headers = [c for c in filter_cols if c in records[0]]
        if not headers:
            print("Error: None of the specified columns exist in the table.")
            sys.exit(1)
    else:
        # Defaults for known tables to make them look nice
        known_headers = {
            "vms": ["name", "vcpu", "memory", "disk_path", "disk_size", "state", "host_ip"],
            "storage_containers": ["name", "tier", "quota_bytes", "path", "ftt"],
            "mimir_schedules": ["schedule_name", "category", "cron_expression", "enabled", "last_run_epoch"],
            "mimir_results": ["category", "check_name", "node_ip", "status", "timestamp", "execution_id"],
            "dagur_schedules": ["job_name", "task_type", "interval_seconds", "enabled", "command"],
            "dagur_runs": ["job_name", "start_time", "end_time", "status", "exit_code"]
        }
        if table_name in known_headers:
            headers = [h for h in known_headers[table_name] if h in records[0]]
        else:
            headers = sorted(records[0].keys())
            
    rows = []
    for r in records:
        rows.append([r.get(col, "N/A") for col in headers])
        
    print_table(headers, rows)

def cmd_db_query():
    if len(sys.argv) < 3:
        print("Error: CQL query string is required.")
        print("Usage: valcli db.query \"<cql_query>\"")
        sys.exit(1)
        
    query = sys.argv[2]
    rc, stdout, err = run_cql_query(query)
    if stdout:
        print(stdout)
    if err:
        print(err)
    if rc != 0:
        sys.exit(rc)

def cmd_storage_benchmark(container_name):
    # Resolve controller IPs
    controllers_str = "127.0.0.1"
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = cdata.get("hosts", [])
                if hosts:
                    controllers_str = ",".join([h["ip"] for h in hosts])
    except Exception:
        pass

    import uuid
    bench_id = str(uuid.uuid4())[:8]
    vdisk_id = "bench-temp-%s" % bench_id

    # A throwaway vdisk, benchmarked through the same NBD path a guest uses.
    #
    # The version this replaces created a LINSTOR resource definition, a volume
    # definition and a resource, waited for a DRBD device to appear, ran fio against it,
    # then demoted and deleted -- five controller round trips and a device-node poll, each
    # with its own cleanup path. What it measured also included DRBD's replication, which
    # is the right thing to measure but was indistinguishable from the local disk's
    # contribution.
    print("Creating temporary vdisk '%s' (100 MiB)..." % vdisk_id)
    rc_c, body_c, err_c = run_mtls_spark_api(
        "127.0.0.1", "/api/v1/dfs/vdisk",
        {"op": "create", "vdisk_id": vdisk_id, "size_bytes": 100 * 1024 * 1024})
    if rc_c != 0:
        detail = body_c.get("error") if isinstance(body_c, dict) else err_c
        print("Error: could not create the benchmark vdisk: %s" % detail)
        return

    socket_path = None
    try:
        rc_a, body_a, err_a = run_mtls_spark_api(
            "127.0.0.1", "/api/v1/dfs/vdisk", {"op": "attach", "vdisk_id": vdisk_id})
        if rc_a != 0 or not isinstance(body_a, dict):
            detail = body_a.get("error") if isinstance(body_a, dict) else err_a
            print("Error: could not attach the benchmark vdisk: %s" % detail)
            return
        socket_path = body_a.get("socket")
        nbd_url = "nbd+unix:///%s?socket=%s" % (vdisk_id, socket_path)

        print("[1/2] Sequential write...")
        subprocess.run(
            ["qemu-io", "-f", "raw", "-c", "write -P 0xAB 0 64M", nbd_url], check=False)
        print("[2/2] Sequential read...")
        subprocess.run(
            ["qemu-io", "-f", "raw", "-c", "read -P 0xAB 0 64M", nbd_url], check=False)
    except Exception as ex:
        print("Error during benchmark: %s" % ex)
    finally:
        print("Cleaning up the temporary vdisk...")
        # Detach before delete: a vdisk still being served is refused, which is the guard
        # against removing storage from under something that has not let go of it.
        run_mtls_spark_api("127.0.0.1", "/api/v1/dfs/vdisk",
                           {"op": "detach", "vdisk_id": vdisk_id})
        run_mtls_spark_api("127.0.0.1", "/api/v1/dfs/vdisk",
                           {"op": "delete", "vdisk_id": vdisk_id})

    print("Benchmark completed.")

def cmd_storage_cleanup_orphaned():
    """Report reclaimable space, and ask Purah to reclaim it.

    This used to glob the container volumes for *.raw and *_vars.fd files and match the
    filenames against hydra.vms -- a disk was a file named after its VM, so an orphan was
    a file no row mentioned. An extent group is not named after anything: it holds extents
    from whichever vdisk was draining, and the only statement of what is referenced is the
    block map.

    So this asks Purah rather than working it out. A second implementation of the mark
    phase would be a second thing to get wrong, and the consequence of getting it wrong is
    deleting live data. The two-scan rule means one invocation may report candidates and
    reclaim nothing; that is the rule working, not a failure.
    """
    hosts = []
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                hosts = json.load(f).get("hosts", [])
    except Exception:
        pass
    if not hosts:
        hosts = [{"ip": "127.0.0.1", "hostname": "this node"}]

    rows = []
    for host in hosts:
        ip = host.get("ip")
        if not ip:
            continue
        rc, body, err = run_mtls_spark_api(
            ip, "/api/v1/dfs/vdisk", {"op": "purah-sweep"})
        if rc != 0 or not isinstance(body, dict):
            detail = body.get("error") if isinstance(body, dict) else err
            rows.append([host.get("hostname") or ip, "-", "-", "-",
                         (str(detail) or "no response")[:40]])
            continue
        reclaimed = body.get("reclaimed") or []
        rows.append([
            host.get("hostname") or ip,
            str(body.get("egroups_known", 0)),
            str(body.get("egroups_referenced", 0)),
            "%d (%.1f MiB)" % (len(reclaimed),
                               int(body.get("bytes_reclaimed") or 0) / (1024 * 1024)),
            "%d awaiting a second scan" % body.get("skipped_awaiting_grace", 0),
        ])

    print_table(["Node", "Extent groups", "Referenced", "Reclaimed", "Notes"], rows)
    print()
    print("An extent group is reclaimed only after two consecutive scans have found it "
          "unreferenced. One run reporting candidates and reclaiming nothing is that rule "
          "working: a drain makes bytes durable before the map points at them, so a single "
          "scan landing in that window sees a group that is milliseconds from being live.")


def format_size(bytes_val):
    if bytes_val is None:
        return "N/A"
    try:
        bytes_val = float(bytes_val)
    except:
        return "N/A"
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PiB"

def cmd_image_list():
    # 1. Query ScyllaDB using SELECT JSON to avoid delimiter issues
    cql = "SELECT JSON name, filename, size_bytes, type, path FROM hydra.valhalla_images;"
    rc, stdout, err = run_cql_query(cql)
    db_images = []
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    db_images.append(json.loads(line))
                except Exception:
                    pass

    # 2. Which images actually have storage behind them.
    #
    # An image is a sealed, immutable vdisk. The listing this replaces asked LINSTOR for
    # volume definitions and matched the ones named img-*, which is the same question
    # asked of a system that no longer exists.
    backing = {}
    rc_v, out_v, _err_v = run_cql_query(
        "SELECT JSON vdisk_id, class, size_bytes FROM hydra.dfs_vdisks;")
    if rc_v == 0:
        for line in (out_v or "").splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            vdisk_id = row.get("vdisk_id") or ""
            if vdisk_id.startswith("img-"):
                backing[vdisk_id] = row

    records = {}
    for image in db_images:
        name = image.get("name") or image.get("filename") or "?"
        vdisk_id = "img-%s" % slugify_image_name(name)
        row = backing.pop(vdisk_id, None)
        if row is None:
            status = "Missing storage"
        elif row.get("class") != "immutable":
            # An immutable class is the whole guarantee: a sealed image cannot be written
            # by anything, which is what replaced DRBD's dual-primary for templates. One
            # still 'rw' was written and never sealed, and would let a guest scribble on
            # the template every other guest is cloned from.
            status = "Not sealed"
        else:
            status = "Active"
        records[name] = {
            "name": name,
            "type": image.get("type") or "unknown",
            "size": format_size(image.get("size_bytes")),
            "scylla": "Yes",
            "vdisk": "Yes" if row else "No",
            "status": status,
        }

    # Anything left is a vdisk named like an image with no catalogue row behind it.
    for vdisk_id, row in backing.items():
        name = vdisk_id[4:]
        records[name] = {
            "name": name,
            "type": "unknown",
            "size": format_size(row.get("size_bytes")),
            "scylla": "No",
            "vdisk": "Yes",
            "status": "Orphaned",
        }

    headers = ["Image Name", "Type", "Size", "ScyllaDB Registered", "Vdisk", "Status"]
    rows = []
    for name, r in sorted(records.items()):
        rows.append([r["name"], r["type"], r["size"], r["scylla"], r["vdisk"], r["status"]])
    print_table(headers, rows)

def cmd_image_delete(image_name):
    # 1. Resolve the vdisk behind the image.
    #
    # The recorded path is the NBD socket, so the id is derivable from the name -- but the
    # row is read anyway, because an image whose row says something else is an image the
    # catalogue and the storage layer disagree about, and deleting the derived one would
    # leave the recorded one allocated and unreachable.
    cql = f"SELECT JSON name, path FROM hydra.valhalla_images WHERE name = '{image_name}';"
    rc, stdout, err = run_cql_query(cql)
    in_db = False
    recorded_path = None
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if row.get("name"):
                    in_db = True
                    recorded_path = row.get("path")

    vdisk_id = None
    if recorded_path and recorded_path.startswith("/var/lib/hci/sidon/nbd/"):
        vdisk_id = os.path.basename(recorded_path)
        if vdisk_id.endswith(".sock"):
            vdisk_id = vdisk_id[:-5]
    if not vdisk_id:
        vdisk_id = image_name if image_name.startswith("img-") else "img-%s" % slugify_image_name(image_name)

    print(f"Target vdisk: '{vdisk_id}'")

    # 2. Detach, then delete.
    #
    # Detach first on every node: an image is attached read-only wherever a guest is using
    # it, and Sidon refuses to delete one it is still serving. That refusal is the guard
    # against removing a template out from under running VMs, so it is worked with rather
    # than forced. This used to run `drbdadm secondary` on every host for the same reason,
    # and had to, because nothing else would have stopped it.
    hosts_list = []
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                hosts_list = json.load(f).get("hosts", [])
    except Exception:
        pass
    if not hosts_list:
        hosts_list = [{"ip": "127.0.0.1"}]

    for host in hosts_list:
        ip = host.get("ip")
        if not ip:
            continue
        run_mtls_spark_api(ip, "/api/v1/dfs/vdisk", {"op": "detach", "vdisk_id": vdisk_id})

    # 3. Delete it. The extents themselves are left for Purah: an image may be the parent
    # of a snapshot chain, and deleting shared data because one referrer went away is the
    # bug reference counting exists to cause.
    print(f"Deleting vdisk '{vdisk_id}'...")
    rc_del, body_del, err_del = run_mtls_spark_api(
        hosts_list[0].get("ip", "127.0.0.1"), "/api/v1/dfs/vdisk",
        {"op": "delete", "vdisk_id": vdisk_id})
    if rc_del != 0:
        detail = body_del.get("error") if isinstance(body_del, dict) else err_del
        print(f"Warning: could not delete the vdisk: {detail}")
    else:
        print("Successfully deleted the vdisk. Its extents will be reclaimed by Purah.")

    # 4. Delete from ScyllaDB
    if in_db:
        print(f"Deleting image metadata for '{image_name}' from ScyllaDB...")
        rc_db, stdout_db, err_db = run_cql_query(f"DELETE FROM hydra.valhalla_images WHERE name = '{image_name}';")
        if rc_db == 0:
            print("Successfully deleted image metadata from ScyllaDB.")
        else:
            print(f"Error deleting metadata from ScyllaDB: {err_db or stdout_db}")
    else:
        print("Image was not registered in ScyllaDB. No metadata deletion needed.")

def cmd_disk_list():
    """Every vdisk, who owns it, and whether it is holding its replicas.

    The version this replaces cross-referenced two LINSTOR listings against hydra.vms and
    filtered out the names it knew were not VM disks -- img-*, linstor-db, bench-*. The
    map holds all of it in one table, and there is no controller database to exclude.
    """
    attachments = {}
    rc, stdout, _err = run_cql_query("SELECT JSON name, disks_list FROM hydra.vms;")
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                vm = json.loads(line)
            except Exception:
                continue
            vm_name = vm.get("name")
            disks = vm.get("disks_list") or ""
            if not vm_name:
                continue
            count = len([d for d in disks.split(",") if d.strip()]) if disks and disks != "NONE" else 1
            for idx in range(count):
                attachments["%s-disk%d" % (vm_name, idx)] = vm_name

    rc_v, out_v, err_v = run_cql_query(
        "SELECT JSON vdisk_id, owner, size_bytes, class, rf, replicas FROM hydra.dfs_vdisks;")
    if rc_v != 0:
        print("Error: hydra.dfs_vdisks could not be read: %s" % (err_v or out_v))
        return

    disks = []
    for line in (out_v or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        vdisk_id = row.get("vdisk_id") or "?"
        if vdisk_id.startswith("img-") or vdisk_id.startswith("bench-temp-"):
            continue
        want = int(row.get("rf") or 0)
        have = len(row.get("replicas") or [])
        if row.get("class") == "immutable":
            status = "Sealed"
        elif want and have < want:
            status = "Degraded (%d/%d replicas)" % (have, want)
        elif not row.get("owner"):
            status = "Unattached"
        else:
            status = "Active"
        disks.append({
            "name": vdisk_id,
            "size": format_size(row.get("size_bytes")),
            "owner": row.get("owner") or "-",
            "attached": attachments.get(vdisk_id, "-"),
            "status": status,
        })

    headers = ["Vdisk", "Size", "Owner", "Attached To VM", "Status"]
    rows = [[d["name"], d["size"], d["owner"], d["attached"], d["status"]]
            for d in sorted(disks, key=lambda x: x["name"])]
    print_table(headers, rows)


def cmd_disk_delete(disk_name):
    # 1. Query ScyllaDB VMs to check attachments
    cql = "SELECT JSON name, disks_list FROM hydra.vms;"
    rc, stdout, err = run_cql_query(cql)
    disk_to_vm = {}
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    vm_name = row.get("name")
                    disks_list = row.get("disks_list", "")
                    
                    if disks_list and disks_list != "NONE" and disks_list != "None" and disks_list != 'null':
                        disks_payload = disks_list.split(",")
                        for idx, entry in enumerate(disks_payload):
                            disk_res_name = f"{vm_name}-disk{idx}"
                            disk_to_vm[disk_res_name] = vm_name
                except Exception:
                    pass

    # 2. Check mapping safety
    if disk_name in disk_to_vm:
        attached_vm = disk_to_vm[disk_name]
        print(f"Error: Disk '{disk_name}' is currently attached to VM '{attached_vm}' and cannot be deleted.")
        sys.exit(1)

    print(f"Disk '{disk_name}' is not attached to any VM. Safe to delete.")
    
    # 3. Demote on all hosts
    hosts = []
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                hosts = [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        pass
    if not hosts:
        hosts = ["127.0.0.1"]

    # Detach on every node, then delete.
    #
    # This used to run `drbdadm secondary` on every host before deleting the resource
    # definition, because nothing else would stop a host holding the device open. Sidon
    # refuses to delete a vdisk it is still serving, so the detach is worked with rather
    # than forced -- and a refusal here means something still has the disk, which is
    # exactly what should stop a delete.
    for ip in hosts:
        run_mtls_spark_api(ip, "/api/v1/dfs/vdisk", {"op": "detach", "vdisk_id": disk_name})

    print(f"Deleting vdisk '{disk_name}'...")
    rc_del, body_del, err_del = run_mtls_spark_api(
        hosts[0], "/api/v1/dfs/vdisk", {"op": "delete", "vdisk_id": disk_name})
    if rc_del != 0:
        detail = body_del.get("error") if isinstance(body_del, dict) else err_del
        print(f"Error: could not delete the vdisk: {detail}")
        return
    print("Successfully deleted the vdisk. Its extents will be reclaimed by Purah.")


def run_node_checks(ip, hostname, local_ip, results_dict):
    cmd = "/usr/local/bin/mcli-runner --category all"
    if ip == local_ip or ip == "127.0.0.1":
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rc, stdout, stderr = res.returncode, res.stdout.decode('utf-8', errors='ignore'), res.stderr.decode('utf-8', errors='ignore')
    else:
        rc, stdout, stderr = run_remote_spark(ip, cmd)
    
    results_dict[ip] = {
        "rc": rc,
        "stdout": stdout,
        "stderr": stderr,
        "hostname": hostname
    }

def cmd_health_check():
    local_ip = "127.0.0.1"
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        pass
        
    hosts_info = []
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            hosts_info = cdata.get("hosts", [])
    except Exception:
        pass
        
    if not hosts_info:
        hosts_info = [{"ip": "127.0.0.1", "hostname": "localhost"}]
        
    print(f"Running Mimir diagnostics on {len(hosts_info)} cluster nodes in parallel...")
    
    results_dict = {}
    threads = []
    for h in hosts_info:
        t = threading.Thread(target=run_node_checks, args=(h["ip"], h["hostname"], local_ip, results_dict))
        t.start()
        threads.append(t)
        
    bar_width = 30
    while any(t.is_alive() for t in threads):
        done_count = sum(1 for t in threads if not t.is_alive())
        pct = (done_count / len(threads)) * 100
        filled = int(bar_width * pct / 100)
        bar = "=" * filled + ">" + " " * (bar_width - filled - 1)
        if filled == bar_width:
            bar = "=" * bar_width
        sys.stdout.write(f"\rProgress: [{bar}] {pct:.0f}% ({done_count}/{len(threads)} hosts completed)")
        sys.stdout.flush()
        time.sleep(0.1)
        
    bar = "=" * bar_width
    sys.stdout.write(f"\rProgress: [{bar}] 100% ({len(threads)}/{len(threads)} hosts completed)\n\n")
    sys.stdout.flush()
    
    failed_checks = []
    for h in hosts_info:
        ip = h["ip"]
        res = results_dict.get(ip)
        if not res or res["rc"] != 0:
            err_msg = res["stderr"] if res else "No response"
            failed_checks.append({
                "host": ip,
                "hostname": h["hostname"],
                "check": "Host Connectivity",
                "status": "FAIL",
                "output": f"Failed to execute Mimir checks on node: {err_msg}"
            })
            continue
            
        try:
            node_data = json.loads(res["stdout"])
            for check_name, check_res in node_data.items():
                status = check_res.get("status", "FAIL")
                if status != "PASS":
                    failed_checks.append({
                        "host": ip,
                        "hostname": h["hostname"],
                        "check": check_name,
                        "status": status,
                        "output": check_res.get("output", "")
                    })
        except Exception as ex:
            failed_checks.append({
                "host": ip,
                "hostname": h["hostname"],
                "check": "JSON Parsing",
                "status": "FAIL",
                "output": f"Failed to parse JSON response: {ex}\nRaw stdout: {res['stdout'][:200]}"
            })
            
    if not failed_checks:
        print("PASS: All Mimir checks passed cluster-wide! No issues detected.")
    else:
        print(f"WARN/FAIL: The following Mimir checks failed or reported warnings:\n")
        
        headers = ["Host IP", "Hostname", "Check ID", "Status"]
        rows = []
        for fc in failed_checks:
            rows.append([fc["host"], fc["hostname"], fc["check"], fc["status"]])
            
        print_table(headers, rows)
        print("\n--- Failure Details ---")
        for fc in failed_checks:
            print(f"Host: {fc['host']} ({fc['hostname']}) | Check: {fc['check']} | Status: {fc['status']}")
            indented = "  " + "\n  ".join(fc["output"].splitlines())
            print(indented)
            print("-" * 50)

def run_spectrum_api(path, method="GET", payload=None):
    import ssl
    import urllib.request
    # Pinned to the console certificate rather than CERT_NONE. Loopback, so the exposure
    # was small, but "verify nothing" and "verify the local console" are different things.
    # check_hostname stays off because that certificate is CN=Spectrum and provisioning
    # installs the same one on every node, so there is no per-node name to match --
    # pinning the certificate is the identity check.
    ctx = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH, cafile="/etc/hci/spectrum/certs/server.crt")
    ctx.check_hostname = False
    
    url = f"https://127.0.0.1:8443{path}"
    data = None
    if payload is not None:
        data = json.dumps(payload).encode('utf-8')
        
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as response:
            return 0, json.loads(response.read().decode('utf-8'))
    except Exception as e:
        return -1, str(e)

def cmd_scheduler_list():
    cql = "SELECT JSON job_name, task_type, interval_seconds, enabled, command FROM hydra.dagur_schedules;"
    rc, stdout, err = run_cql_query(cql)
    if rc != 0:
        print(err)
        sys.exit(1)
    records = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
    headers = ["Job Name", "Task Type", "Interval", "Enabled", "Command"]
    rows = []
    for r in records:
        interval = r.get("interval_seconds", 0)
        interval_str = f"{interval // 3600} Hour(s)" if interval >= 3600 else f"{interval // 60} Minute(s)"
        rows.append([
            r.get("job_name", "N/A"),
            r.get("task_type", "N/A"),
            interval_str,
            "Yes" if r.get("enabled") else "No",
            r.get("command", "N/A")
        ])
    print("=== Dagur Scheduler Policies ===")
    print_table(headers, rows)

def cmd_scheduler_history():
    cql = "SELECT JSON job_name, start_time, end_time, status, exit_code FROM hydra.dagur_runs;"
    rc, stdout, err = run_cql_query(cql)
    if rc != 0:
        print(err)
        sys.exit(1)
    records = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
    headers = ["Job Name", "Start Time", "End Time", "Status", "Exit Code"]
    rows = []
    for r in records:
        rows.append([
            r.get("job_name", "N/A"),
            r.get("start_time", "N/A"),
            r.get("end_time", "N/A") or "Running...",
            r.get("status", "N/A"),
            r.get("exit_code") if r.get("exit_code") != -1 else "N/A"
        ])
    print("=== Dagur Scheduler Execution History ===")
    print_table(headers, rows)

def cmd_scheduler_trigger(name):
    rc, err_or_res = run_spectrum_api("/api/dagur/schedule/trigger", method="POST", payload={"job_name": name})
    if rc == 0:
        print(f"Success: Job '{name}' manual execution triggered.")
    else:
        print(f"Error triggering job: {err_or_res}")

def cmd_host_list():
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/hosts", {}, method="GET")
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error querying host list: {res['error']}")
        sys.exit(1)
    
    hosts = res.get("hosts", [])
    headers = ["Hostname", "IP Address", "Status", "Maintenance Mode"]
    rows = []
    for h in hosts:
        rows.append([
            h.get("hostname", "N/A"),
            h.get("ip", "N/A"),
            h.get("status", "N/A"),
            "Yes" if h.get("maintenance_mode", False) else "No"
        ])
    print_table(headers, rows)

def get_zookeeper_leader_ip():
    """Finds the IP of the current ZooKeeper leader, with active designated leader fallback if the leader is in maintenance."""
    ips = []
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            ips = [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        ips = [LOCAL_IP]
        
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

def catalyst_client_context():
    """Client certificate for Catalyst, which now requires mutual TLS.

    It dispatches VM lifecycle work and used to accept it from anything that could open
    a socket to port 9091, checking neither a credential nor a source address.
    """
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                         cafile="/etc/hci/spark/certs/ca.crt")
    context.load_cert_chain(certfile="/etc/hci/spark/certs/node.crt",
                            keyfile="/etc/hci/spark/certs/node.key")
    return context


def wait_for_catalyst_task(task_id):
    leader_ip = get_zookeeper_leader_ip()
    url = f"https://{leader_ip}:9091/api/v1/tasks/status/{task_id}"
    print(f"Waiting for Catalyst task {task_id} to finish...")
    
    last_progress = -1
    while True:
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(
                    req, context=catalyst_client_context(), timeout=35) as response:
                if response.status == 200:
                    res = json.loads(response.read().decode("utf-8"))
                    status = res.get("status")
                    progress = res.get("progress", 0)
                    error_msg = res.get("error_msg", "")
                    
                    if progress != last_progress:
                        print(f"Task status: {status} | Progress: {progress}%")
                        last_progress = progress
                        
                    if status == "completed":
                        print("Task completed successfully.")
                        return True
                    elif status == "failed":
                        print(f"Task failed: {error_msg}")
                        sys.exit(1)
                elif response.status == 204:
                    # Long polling timeout, update leader IP and keep waiting
                    leader_ip = get_zookeeper_leader_ip()
                    url = f"https://{leader_ip}:9091/api/v1/tasks/status/{task_id}"
                    continue
                else:
                    print(f"Unexpected response status from Catalyst: {response.status}")
                    time.sleep(2)
        except Exception as e:
            # Check if this host has entered maintenance mode locally
            if os.path.exists("/etc/hci/maintenance.state"):
                print("Host has successfully entered maintenance mode. Catalyst is offline. Exiting wait loop.")
                return True
            # Maybe leader is switching/rebooting, try to find new leader IP
            time.sleep(2)
            leader_ip = get_zookeeper_leader_ip()
            url = f"https://{leader_ip}:9091/api/v1/tasks/status/{task_id}"

def cmd_host_maintenance_enter(hostname, force_stop=False):
    if hostname == "--all":
        rc_hosts, res_hosts, err_hosts = run_mtls_api("127.0.0.1", "/api/v1/hosts", {}, method="GET")
        if rc_hosts != 0 or "error" in res_hosts:
            hosts = []
            try:
                with open("/etc/hci/cluster.json", "r") as f:
                    cdata = json.load(f)
                    hosts = cdata.get("hosts", [])
            except Exception:
                print(f"Failed to get host list for --all: {err_hosts}")
                sys.exit(1)
        else:
            hosts = res_hosts.get("hosts", [])
        
        hostnames = [h.get("hostname") for h in hosts if h.get("hostname")]
        if not hostnames:
            print("No hosts found.")
            sys.exit(1)
            
        print(f"Requesting all hosts to enter maintenance mode sequentially: {', '.join(hostnames)}...")
        for hn in hostnames:
            print(f"\n--- Processing host '{hn}' ---")
            payload = {"hostname": hn, "action": "enter", "force_stop": force_stop}
            rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/host/maintenance", payload, method="POST")
            if rc != 0:
                print(f"Failed to communicate with spark-daemon for {hn}: {err}")
            elif "error" in res:
                print(f"Error for {hn}: {res['error']}")
            else:
                task_id = res.get("task_id")
                if task_id:
                    wait_for_catalyst_task(task_id)
                else:
                    print(f"Success for {hn}: {res.get('message', 'Maintenance mode transition initiated.')}")
        return

    print(f"Requesting host '{hostname}' to enter maintenance mode (force_stop={force_stop})...")
    payload = {"hostname": hostname, "action": "enter", "force_stop": force_stop}
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/host/maintenance", payload, method="POST")
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error: {res['error']}")
        sys.exit(1)
    
    # Wait for task completion
    task_id = res.get("task_id")
    if task_id:
        wait_for_catalyst_task(task_id)
    else:
        print(f"Success: {res.get('message', 'Maintenance mode transition initiated.')}")

def cmd_host_maintenance_leave(hostname):
    if hostname == "--all":
        rc_hosts, res_hosts, err_hosts = run_mtls_api("127.0.0.1", "/api/v1/hosts", {}, method="GET")
        if rc_hosts != 0 or "error" in res_hosts:
            hosts = []
            try:
                with open("/etc/hci/cluster.json", "r") as f:
                    cdata = json.load(f)
                    hosts = cdata.get("hosts", [])
            except Exception:
                print(f"Failed to get host list for --all: {err_hosts}")
                sys.exit(1)
        else:
            hosts = res_hosts.get("hosts", [])
            
        hostnames = [h.get("hostname") for h in hosts if h.get("hostname")]
        if not hostnames:
            print("No hosts found.")
            sys.exit(1)
            
        print(f"Requesting all hosts to leave maintenance mode sequentially: {', '.join(hostnames)}...")
        for hn in hostnames:
            print(f"\n--- Processing host '{hn}' ---")
            payload = {"hostname": hn, "action": "leave"}
            rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/host/maintenance", payload, method="POST")
            if rc != 0:
                print(f"Failed to communicate with spark-daemon for {hn}: {err}")
            elif "error" in res:
                print(f"Error for {hn}: {res['error']}")
            else:
                task_id = res.get("task_id")
                if task_id:
                    wait_for_catalyst_task(task_id)
                else:
                    print(f"Success for {hn}: {res.get('message', 'Host returned to normal status.')}")
        return

    print(f"Requesting host '{hostname}' to leave maintenance mode...")
    payload = {"hostname": hostname, "action": "leave"}
    rc, res, err = run_mtls_api("127.0.0.1", "/api/v1/host/maintenance", payload, method="POST")
    if rc != 0:
        print(f"Failed to communicate with spark-daemon: {err}")
        sys.exit(1)
    if "error" in res:
        print(f"Error: {res['error']}")
        sys.exit(1)
    
    # Wait for task completion
    task_id = res.get("task_id")
    if task_id:
        wait_for_catalyst_task(task_id)
    else:
        if res.get("status") == "transitioning" and "Vali offline" in res.get("message", ""):
            print("Vali was offline. Local services bootstrapped. Waiting for Vali to come online...")
            import socket, time
            vali_online = False
            for _ in range(30):
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(0.5)
                    s.connect(("127.0.0.1", 9095))
                    s.close()
                    vali_online = True
                    break
                except:
                    time.sleep(1)
            
            if vali_online:
                print("Vali is online. Finalizing leave maintenance sequence...")
                # Retry submitting the leave request up to 6 times (with 5 seconds sleep in between) if it fails
                for attempt in range(6):
                    rc_final, res_final, err_final = run_mtls_api("127.0.0.1", "/api/v1/host/maintenance", payload, method="POST")
                    if rc_final == 0 and "error" not in res_final:
                        final_task_id = res_final.get("task_id")
                        if final_task_id:
                            wait_for_catalyst_task(final_task_id)
                            return
                    if attempt < 5:
                        print(f"Database or Catalyst not fully initialized yet (attempt {attempt+1}/6). Retrying in 5 seconds...")
                        time.sleep(5)
                print("Success: Local services started, but database state finalization timed out. Please run the command again if status is not NORMAL.")
            else:
                print("Timeout waiting for Vali to initialize. Please check service status or run the command again.")
        else:
            print(f"Success: {res.get('message', 'Host returned to normal status.')}")

def cmd_cluster_vip_set(vip_ip):
    import base64
    if not os.path.exists("/etc/hci/cluster.json"):
        print("Error: /etc/hci/cluster.json not found on this host.")
        sys.exit(1)
        
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
    except Exception as e:
        print(f"Error reading cluster.json: {e}")
        sys.exit(1)
        
    cdata["vip"] = vip_ip
    
    try:
        with open("/etc/hci/cluster.json", "w") as f:
            json.dump(cdata, f, indent=4)
    except Exception as e:
        print(f"Error writing local cluster.json: {e}")
        sys.exit(1)
        
    hosts = [h["ip"] for h in cdata.get("hosts", [])]
    json_str = json.dumps(cdata, indent=4)
    json_b64 = base64.b64encode(json_str.encode()).decode()
    
    cmd_write = f"mkdir -p /etc/hci && echo {json_b64} | base64 -d > /etc/hci/cluster.json && systemctl restart bifrost"
    
    for ip in hosts:
        print(f"Propagating VIP configuration to host {ip}...")
        rc, stdout, stderr = run_remote_spark(ip, cmd_write)
        if rc != 0:
            print(f"Warning: Failed to configure VIP on host {ip}: {stderr or stdout}")
            
    print(f"Successfully configured cluster Virtual IP (VIP) to {vip_ip} cluster-wide.")

def cmd_system_cleanup():
    cutoff_days = 3
    cutoff_sec = int(time.time() - cutoff_days * 86400)
    
    print(f"Starting execution history cleanup (older than {cutoff_days} days)...")
    
    import datetime
    def parse_db_timestamp(ts_val):
        if ts_val is None:
            return time.time()
        if isinstance(ts_val, (int, float)):
            if ts_val > 5000000000:
                return ts_val / 1000.0
            return float(ts_val)
        if isinstance(ts_val, str):
            if ts_val.isdigit():
                val = int(ts_val)
                if val > 5000000000:
                    return val / 1000.0
                return float(val)
            for fmt in [
                "%Y-%m-%d %H:%M:%S.%f%z",
                "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S.%f+0000",
                "%Y-%m-%d %H:%M:%S+0000",
            ]:
                try:
                    clean_ts = ts_val
                    if clean_ts.endswith("Z"):
                        clean_ts = clean_ts[:-1] + "+0000"
                    dt = datetime.datetime.strptime(clean_ts, fmt)
                    return dt.timestamp()
                except:
                    pass
        return time.time()

    # 1. Clean dagur_runs
    rc, stdout, _ = run_cql_query("SELECT JSON job_name, start_time FROM hydra.dagur_runs;")
    if rc == 0 and stdout:
        cnt = 0
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    job_name = row.get("job_name")
                    st_str = row.get("start_time")
                    if job_name and st_str:
                        st_epoch = parse_db_timestamp(st_str)
                        if st_epoch < cutoff_sec:
                            run_cql_query(f"DELETE FROM hydra.dagur_runs WHERE job_name = '{job_name}' AND start_time = '{st_str}';")
                            cnt += 1
                except:
                    pass
        print(f"Cleaned {cnt} old Dagur job execution records.")

    # 2. Clean mimir_results
    rc, stdout, _ = run_cql_query("SELECT JSON category, check_name, node_ip, timestamp FROM hydra.mimir_results;")
    if rc == 0 and stdout:
        cnt = 0
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    cat = row.get("category")
                    cname = row.get("check_name")
                    nip = row.get("node_ip")
                    ts_str = row.get("timestamp")
                    if cat and cname and nip and ts_str:
                        ts_epoch = parse_db_timestamp(ts_str)
                        if ts_epoch < cutoff_sec:
                            run_cql_query(f"DELETE FROM hydra.mimir_results WHERE category = '{cat}' AND check_name = '{cname}' AND node_ip = '{nip}';")
                            cnt += 1
                except:
                    pass
        print(f"Cleaned {cnt} old Mimir diagnostic results.")

    # 3. Clean vali_tasks
    rc, stdout, _ = run_cql_query("SELECT JSON task_id, created_at FROM hydra.vali_tasks;")
    if rc == 0 and stdout:
        cnt = 0
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    tid = row.get("task_id")
                    cat_ms = row.get("created_at")
                    if tid and cat_ms:
                        cat_epoch = parse_db_timestamp(cat_ms)
                        if cat_epoch < cutoff_sec:
                            run_cql_query(f"DELETE FROM hydra.vali_tasks WHERE task_id = {tid};")
                            cnt += 1
                except:
                    pass
        print(f"Cleaned {cnt} old Vali placement tasks.")

    # 4. Clean vali_drs_history
    rc, stdout, _ = run_cql_query("SELECT JSON event_time, vm_name FROM hydra.vali_drs_history;")
    if rc == 0 and stdout:
        cnt = 0
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    row = json.loads(line)
                    ev_time = row.get("event_time")
                    vname = row.get("vm_name")
                    if ev_time and vname:
                        ev_epoch = parse_db_timestamp(ev_time)
                        if ev_epoch < cutoff_sec:
                            run_cql_query(f"DELETE FROM hydra.vali_drs_history WHERE event_time = '{ev_time}' AND vm_name = '{vname}';")
                            cnt += 1
                except:
                    pass
        print(f"Cleaned {cnt} old Vali DRS migration history records.")

def cmd_vm_create():
    if len(sys.argv) < 5:
        print("Error: Name, vCPUs, and Memory are required.")
        print("Usage: valcli vm.create <vm_name> <vcpus> <memory_mb> [options]")
        print("Options:")
        print("  --firmware <uefi|bios>    (default: uefi)")
        print("  --iso <iso_file>          (default: none)")
        print("  --boot-device <hd|cdrom>  (default: hd)")
        print("  --network-id <uuid>       (default: default)")
        print("  --disks <disks_comma>     (default: 10G)")
        print("  --cpu-model <model>       (default: host-passthrough)")
        sys.exit(1)
        
    name = sys.argv[2]
    try:
        vcpus = int(sys.argv[3])
        memory = int(sys.argv[4])
    except ValueError:
        print("Error: vCPUs and Memory must be integers.")
        sys.exit(1)
        
    firmware = "uefi"
    iso = ""
    boot_device = "hd"
    network_id = ""
    disks = ["10G"]
    cpu_model = "host-passthrough"
    
    idx = 5
    while idx < len(sys.argv):
        arg = sys.argv[idx]
        if arg == "--firmware" and idx + 1 < len(sys.argv):
            firmware = sys.argv[idx+1]
            idx += 2
        elif arg == "--iso" and idx + 1 < len(sys.argv):
            iso = sys.argv[idx+1]
            idx += 2
        elif arg == "--boot-device" and idx + 1 < len(sys.argv):
            boot_device = sys.argv[idx+1]
            idx += 2
        elif arg == "--network-id" and idx + 1 < len(sys.argv):
            network_id = sys.argv[idx+1]
            idx += 2
        elif arg == "--disks" and idx + 1 < len(sys.argv):
            disks = sys.argv[idx+1].split(",")
            idx += 2
        elif arg == "--cpu-model" and idx + 1 < len(sys.argv):
            cpu_model = sys.argv[idx+1]
            idx += 2
        else:
            print(f"Error: Unknown or malformed option '{arg}'")
            sys.exit(1)
            
    payload = {
        "name": name,
        "vcpus": vcpus,
        "memory": memory,
        "firmware": firmware,
        "iso": iso,
        "boot_device": boot_device,
        "network_id": network_id,
        "disks": disks,
        "cpu_model": cpu_model
    }
    
    print(f"Creating VM '{name}' ({vcpus} vCPUs, {memory}MB RAM)...")
    rc, data = run_spectrum_api("/api/vms/create", method="POST", payload=payload)
    if rc == 0:
        print(f"Success: {data.get('message', 'VM creation task scheduled.')}")
    else:
        print(f"Error creating VM: {data}")
        sys.exit(1)

def cmd_vm_delete():
    if len(sys.argv) < 3:
        print("Error: VM Name is required.")
        print("Usage: valcli vm.delete <vm_name>")
        sys.exit(1)
        
    name = sys.argv[2]
    print(f"Deleting VM '{name}'...")
    rc, data = run_spectrum_api("/api/vms/delete", method="POST", payload={"name": name})
    if rc == 0:
        print(f"Success: {data.get('message', 'VM deletion task scheduled.')}")
    else:
        print(f"Error deleting VM: {data}")
        sys.exit(1)

def cmd_vm_edit():
    if len(sys.argv) < 3:
        print("Error: VM Name is required.")
        print("Usage: valcli vm.edit <vm_name> [options]")
        print("Options:")
        print("  --vcpus <count>")
        print("  --memory <memory_mb>")
        print("  --firmware <uefi|bios>")
        print("  --iso <iso_file>")
        print("  --boot-device <hd|cdrom>")
        print("  --network-id <uuid>")
        print("  --disks <disks_comma>")
        print("  --cpu-model <model>")
        sys.exit(1)
        
    name = sys.argv[2]
    payload = {"name": name}
    
    idx = 3
    while idx < len(sys.argv):
        arg = sys.argv[idx]
        if arg == "--vcpus" and idx + 1 < len(sys.argv):
            payload["vcpus"] = int(sys.argv[idx+1])
            idx += 2
        elif arg == "--memory" and idx + 1 < len(sys.argv):
            payload["memory"] = int(sys.argv[idx+1])
            idx += 2
        elif arg == "--firmware" and idx + 1 < len(sys.argv):
            payload["firmware"] = sys.argv[idx+1]
            idx += 2
        elif arg == "--iso" and idx + 1 < len(sys.argv):
            payload["iso"] = sys.argv[idx+1]
            idx += 2
        elif arg == "--boot-device" and idx + 1 < len(sys.argv):
            payload["boot_device"] = sys.argv[idx+1]
            idx += 2
        elif arg == "--network-id" and idx + 1 < len(sys.argv):
            payload["network_id"] = sys.argv[idx+1]
            idx += 2
        elif arg == "--disks" and idx + 1 < len(sys.argv):
            payload["disks"] = sys.argv[idx+1].split(",")
            idx += 2
        elif arg == "--cpu-model" and idx + 1 < len(sys.argv):
            payload["cpu_model"] = sys.argv[idx+1]
            idx += 2
        else:
            print(f"Error: Unknown or malformed option '{arg}'")
            sys.exit(1)
            
    if len(payload) == 1:
        print("Error: No configuration modifications specified.")
        sys.exit(1)
        
    print(f"Updating VM '{name}' configuration...")
    rc, data = run_spectrum_api("/api/vms/update", method="POST", payload=payload)
    if rc == 0:
        print(f"Success: {data.get('message', 'VM update task scheduled.')}")
    else:
        print(f"Error updating VM: {data}")
        sys.exit(1)

SAGA_BIN = "/usr/local/bin/saga"


def run_saga(args):
    """Hand a subcommand to Saga, the metadata backup tool, and exit with its code.

    A pass-through rather than a reimplementation. Saga has to work on a host whose
    metadata layer is the broken thing -- that is the whole point of a restore -- so it
    talks to cqlsh and nodetool directly and does not go through Daruk or Spectrum the
    way the rest of this CLI does. Wrapping it here keeps `valcli` the one place an
    operator looks without duplicating any of that.

    Output is not captured: a backup prints progress for as long as it runs, and
    swallowing it until the end would make a slow run look like a hung one.
    """
    if not os.path.exists(SAGA_BIN):
        print(f"Error: {SAGA_BIN} is not installed on this node.")
        print("Backups run on the node that holds the data; deploy saga and retry.")
        sys.exit(1)
    try:
        result = subprocess.run([SAGA_BIN] + list(args))
    except KeyboardInterrupt:
        sys.exit(130)
    sys.exit(result.returncode)


def cmd_backup_run():
    """valcli backup.run [--all-nodes] [--include-ca] [--allow-same-filesystem] ..."""
    run_saga(["backup"] + sys.argv[2:])


def cmd_backup_list():
    run_saga(["list"] + sys.argv[2:])


def cmd_backup_verify():
    run_saga(["verify"] + sys.argv[2:])


def cmd_backup_restore():
    run_saga(["restore"] + sys.argv[2:])


def cmd_backup_prune():
    run_saga(["prune"] + sys.argv[2:])


def cmd_backup_target():
    run_saga(["target"] + sys.argv[2:])


def print_usage():
    print("Valkyrie CLI (valcli) v1.2.0 - Helios HCI command-line manager\n")
    print("Usage:")
    print("  valcli vm.list                     List all virtual machines in the cluster")
    print("  valcli vm.create <name> <vc> <mem> Create a new VM configuration and disks")
    print("  valcli vm.delete <name>            Delete VM configuration and its disks")
    print("  valcli vm.edit <name> [options]    Modify VM CPU, memory, disks, network, or ISO")
    print("  valcli vm.on <vm_name>             Power ON a virtual machine")
    print("  valcli vm.off <vm_name>            Power OFF (destroy) a virtual machine")
    print("  valcli vm.migrate <name> <host>    Migrate a running VM to another cluster node")
    print("  valcli vm.balance                  Manually trigger aggressive cluster DRS load balancing")
    print("  valcli drs.status                  Print cluster balance score and recent DRS migrations")
    print("  valcli host.list                   List all hosts and their maintenance state")
    print("  valcli host.maintenance.enter <h>  Put host (or '--all') into maintenance mode and evacuate VMs")
    print("      Options:")
    print("        --force-stop                 Forcefully stop/suspend VMs that fail migration")
    print("  valcli host.maintenance.leave <h>  Take host (or '--all') out of maintenance mode")
    print("  valcli cluster.vip.set <vip>       Configure cluster-wide Virtual IP (VIP)")
    print("  valcli storage.list                List storage containers, per-node extent stores and vdisks")
    print("  valcli storage.benchmark <name>    Run safe read/write performance benchmark")
    print("  valcli storage.cleanup_orphaned    Delete orphaned virtual disk and NVRAM files")
    print("  valcli image.list                  List registered images and whether each has a sealed vdisk")
    print("  valcli image.delete <name>         Demote and delete image from storage and database")
    print("  valcli disk.list                   List all active and orphaned virtual disks")
    print("  valcli disk.delete <name>          Delete virtual disk (fails if disk is attached to a VM)")
    print("  valcli health.check                Run parallel Mimir diagnostics with progress bar")
    print("  valcli scheduler.list              List all Dagur scheduled policies")
    print("  valcli scheduler.history           List past executions of Dagur jobs")
    print("  valcli scheduler.trigger <name>    Manually trigger execution of a Dagur job")
    print("  valcli system.cleanup              Prune execution history tables older than 3 days")
    print("  valcli backup.target [<dir>]       Show or set where metadata backups are written")
    print("  valcli backup.run                  Back up the hydra keyspace and /etc/hci")
    print("      Options:")
    print("        --all-nodes                  Also run on every peer, in parallel")
    print("        --include-ca                 Also capture the cluster CA and node private keys")
    print("        --allow-same-filesystem      Accept a target on the database's own disk")
    print("  valcli backup.list                 List artefacts at the backup target")
    print("  valcli backup.verify [<file>]      Check an artefact against its manifest (default: latest)")
    print("  valcli backup.restore [<file>]     Load an artefact back into this cluster")
    print("      Options:")
    print("        --tables a,b                 Restore only these tables")
    print("        --extract-only <dir>         Unpack the artefact without touching the cluster")
    print("        --force                      Proceed despite a schema-version mismatch")
    print("  valcli backup.prune                Apply the retention policy now (--dry-run to preview)")
    print("      Note: backups cover cluster METADATA only. Guest data inside")
    print("      vdisks is not backed up by any of this -- see docs/backup_restore.md.")
    print("  valcli db.print <table_name>       Print ScyllaDB table contents as ASCII table")
    print("      Options:")
    print("        --columns c1,c2              Specify a comma-separated list of columns to print")
    print("  valcli db.query \"<query>\"          Execute raw CQL query and display formatted output")
    print("\nAvailable tables: vms, storage_containers, dagur_schedules, dagur_runs")

def main():
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)
        
    cmd = sys.argv[1]
    if cmd in ["--version", "-v", "-version", "version"]:
        print("Valkyrie CLI (valcli) v1.2.0")
        sys.exit(0)
        
    if cmd == "vm.list":
        cmd_vm_list()
    elif cmd == "vm.create":
        cmd_vm_create()
    elif cmd == "vm.delete":
        cmd_vm_delete()
    elif cmd == "vm.edit":
        cmd_vm_edit()
    elif cmd == "vm.on":
        if len(sys.argv) < 3:
            print("Error: VM Name is required.")
            print("Usage: valcli vm.on <vm_name>")
            sys.exit(1)
        cmd_vm_on(sys.argv[2])
    elif cmd == "vm.off":
        if len(sys.argv) < 3:
            print("Error: VM Name is required.")
            print("Usage: valcli vm.off <vm_name>")
            sys.exit(1)
        cmd_vm_off(sys.argv[2])
    elif cmd == "vm.migrate":
        if len(sys.argv) < 4:
            print("Error: VM Name and Target Host are required.")
            print("Usage: valcli vm.migrate <vm_name> <target_host>")
            sys.exit(1)
        cmd_vm_migrate(sys.argv[2], sys.argv[3])
    elif cmd == "vm.balance":
        cmd_vm_balance()
    elif cmd == "drs.status":
        cmd_drs_status()
    elif cmd == "host.list":
        cmd_host_list()
    elif cmd == "host.maintenance.enter":
        args = sys.argv[2:]
        if not args or (len(args) == 1 and args[0] == "--force-stop"):
            print("Error: Hostname is required.")
            print("Usage: valcli host.maintenance.enter <hostname> [--force-stop]")
            sys.exit(1)
        
        force_stop = "--force-stop" in args
        hostname = None
        for arg in args:
            if arg != "--force-stop":
                hostname = arg
                break
                
        if not hostname:
            print("Error: Hostname is required.")
            sys.exit(1)
            
        cmd_host_maintenance_enter(hostname, force_stop)
    elif cmd == "host.maintenance.leave":
        if len(sys.argv) < 3:
            print("Error: Hostname is required.")
            print("Usage: valcli host.maintenance.leave <hostname>")
            sys.exit(1)
        cmd_host_maintenance_leave(sys.argv[2])
    elif cmd == "cluster.vip.set":
        if len(sys.argv) < 3:
            print("Error: VIP IP address is required.")
            print("Usage: valcli cluster.vip.set <vip_ip>")
            sys.exit(1)
        cmd_cluster_vip_set(sys.argv[2])
    elif cmd == "storage.list":
        cmd_storage_list()
    elif cmd == "storage.benchmark":
        if len(sys.argv) < 3:
            print("Error: Storage container name is required.")
            print("Usage: valcli storage.benchmark <container_name>")
            sys.exit(1)
        cmd_storage_benchmark(sys.argv[2])
    elif cmd == "storage.cleanup_orphaned":
        cmd_storage_cleanup_orphaned()
    elif cmd == "image.list":
        cmd_image_list()
    elif cmd == "image.delete":
        if len(sys.argv) < 3:
            print("Error: Image name is required.")
            print("Usage: valcli image.delete <image_name>")
            sys.exit(1)
        cmd_image_delete(sys.argv[2])
    elif cmd == "disk.list":
        cmd_disk_list()
    elif cmd == "disk.delete":
        if len(sys.argv) < 3:
            print("Error: Disk name is required.")
            print("Usage: valcli disk.delete <disk_name>")
            sys.exit(1)
        cmd_disk_delete(sys.argv[2])
    elif cmd == "health.check":
        cmd_health_check()
    elif cmd == "db.print":
        cmd_db_print()
    elif cmd == "db.query":
        cmd_db_query()
    elif cmd == "scheduler.list":
        cmd_scheduler_list()
    elif cmd == "scheduler.history":
        cmd_scheduler_history()
    elif cmd == "system.cleanup":
        cmd_system_cleanup()
    elif cmd == "backup.run":
        cmd_backup_run()
    elif cmd == "backup.list":
        cmd_backup_list()
    elif cmd == "backup.verify":
        cmd_backup_verify()
    elif cmd == "backup.restore":
        cmd_backup_restore()
    elif cmd == "backup.prune":
        cmd_backup_prune()
    elif cmd == "backup.target":
        cmd_backup_target()
    elif cmd == "scheduler.trigger":
        if len(sys.argv) < 3:
            print("Error: Job name is required.")
            print("Usage: valcli scheduler.trigger <job_name>")
            sys.exit(1)
        cmd_scheduler_trigger(sys.argv[2])
    else:
        print(f"Error: Unknown command '{cmd}'")
        print_usage()
        sys.exit(1)

if __name__ == "__main__":
    main()
