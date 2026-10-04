#!/usr/bin/env python3
import sys
import json
import ssl
import socket
import helios_zk
import subprocess
import re
import urllib.request
import urllib.parse
import urllib.error
import time
import os
import threading

# The cluster's one CQL query layer. Fifteen files carried their own copy of this, most
# of them identical, and the guard against conditional statements had reached only three
# of them -- see helios_cql for what that cost.
from helios_cql import (  # noqa: F401  (re-exported for modules that import from here)
    ConditionalStatementError,
    HYDRA_DB_CONTAINER,
    cql_escape,
    cql_int,
    is_conditional_cql,
    run_conditional_cql_query,
    run_cql_query as _local_run_cql_query,
)

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


# What a failed query says when the database on THIS node is what is unreachable, as opposed to
# the statement being wrong. Only these are worth asking another node about: a syntax error would
# fail identically everywhere and waiting for every peer to say so is a slower way to the same error.
_DATABASE_DOWN_MARKERS = (
    "nohostavailable", "connection refused", "unable to connect", "could not connect",
    "is not running", "no such container", "cannot connect", "timed out", "timeout",
    "unavailable", "connection error", "database query execution error", "no route",
    "connection reset", "operation timed out", "no container with name",
)


def database_looks_down(rc, stderr):
    """True when a failed query's error is about reaching the database, not about the query."""
    if rc == 0:
        return False
    text = (stderr or "").lower()
    return any(marker in text for marker in _DATABASE_DOWN_MARKERS)


def cluster_peer_ips(path="/etc/hci/cluster.json"):
    """Every other host in the cluster document, in its order. [] when it cannot be read."""
    try:
        with open(path, "r") as handle:
            hosts = [h.get("ip") for h in json.load(handle).get("hosts", []) if h.get("ip")]
    except (OSError, ValueError, AttributeError, TypeError):
        return []
    return [ip for ip in hosts if ip not in (LOCAL_IP, "127.0.0.1")]


def run_cql_query_via_peers(cql_query, *args, peers=None, remote=None, **kwargs):
    """The shared CQL runner, with one addition: when this node's own database is down, ask a
    live peer instead of failing.

    Daruk and the cqlsh fallback both talk to the database on this host, so a node whose
    `hydra-db` is down could run no `valcli` command that reads the cluster -- including the
    ones needed to see why. After a local failure that is about reaching the database, each
    other node is tried in turn through its spark-daemon (`cqlsh` against that node, from its own
    container), and the first answer wins. A statement the database refused is not retried
    elsewhere, and a conditional statement still raises, as it does everywhere.
    """
    rc, out, err = _local_run_cql_query(cql_query, *args, **kwargs)
    if not database_looks_down(rc, err):
        return rc, out, err
    import base64
    remote = remote or run_remote_spark
    encoded = base64.b64encode(cql_query.encode("utf-8")).decode("utf-8")
    tried = []
    for peer in (cluster_peer_ips() if peers is None else peers):
        command = "echo %s | base64 -d | podman exec -i %s cqlsh %s" % (
            encoded, HYDRA_DB_CONTAINER, peer)
        prc, pout, perr = remote(peer, command)
        if prc == 0:
            sys.stderr.write("[valcli] this node's database did not answer (%s); answered by %s.\n"
                             % ((err or "no detail").strip().splitlines()[-1][:120] if err else "no detail", peer))
            return 0, (pout or "").strip(), ""
        tried.append("%s: %s" % (peer, (perr or "no answer").strip()[:80]))
    if tried:
        err = (err or "").rstrip() + " (and no peer answered either: " + "; ".join(tried) + ")"
    return rc, out, err


# An assignment, not a def: helios_cql stays the only module that defines the query layer.
run_cql_query = run_cql_query_via_peers


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


def run_mtls_spark_api_full(ip, path, payload, method="POST", timeout=120):
    """Like run_mtls_spark_api, but says which HTTP status the body came with.

    `run_mtls_spark_api` returns rc 0 for any answer that has a JSON body, including a 503
    whose body is `{"error": ..., "kind": ...}`. A caller that then reads `body.get("total_bytes")
    or 0` turns "sidon is not answering" into "online, 0.0 GiB". Returns
    (status, body, error); status is None when nothing answered at all.
    """
    ip, verify_identity = spark_endpoint(ip)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
    context.check_hostname = verify_identity
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(f"https://{ip}:9099{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8")), ""
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8")), ""
        except Exception:
            return e.code, {}, str(e)
    except Exception as e:
        return None, {}, str(e)


def extent_store_row(host_label, status, body, err=""):
    """One row of the Extent Store table, and the answer it may be trusted for.

    Returns (row, body_or_None). The body is returned only for a real answer, so the
    per-disk tables are built from nothing else.

    Four states, and none of them is "online with zero":
      online       200 and a capacity that is a positive number of bytes
      not ready    the node answers but its sidon does not: spark's 503 while the daemon is
                   starting (its control socket does not exist until its mounts are done), or
                   a 200 that carries no capacity. The capacity is unknown, not zero.
      unreachable  nothing answered
      error        any other refusal, with what it said
    """
    def row(state, detail):
        return [host_label, state, "-", "-", "-", str(detail)[:40]]

    if status is None:
        return row("unreachable", err or "no response"), None
    if status != 200 or not isinstance(body, dict):
        said = (body.get("error") if isinstance(body, dict) else None) or err \
            or "HTTP %s" % status
        if status == 503:
            return row("not ready", "sidon starting? " + said), None
        return row("error", said), None
    try:
        total = int(body.get("total_bytes"))
    except (TypeError, ValueError):
        total = 0
    if total <= 0:
        absent = body.get("absent_disks") or []
        why = "no mounted disk" + (" (%d absent)" % len(absent) if absent else "")
        return row("not ready", why), None
    gib = 1024 ** 3
    avail = int(body.get("available_bytes") or 0)
    return [
        body.get("node") or host_label,
        "online",
        "%.1f GiB" % (total / gib),
        "%.1f GiB" % ((total - avail) / gib),
        str(body.get("egroup_count", 0)),
        "%.2f GiB" % (int(body.get("journal_bytes") or 0) / gib),
    ], body


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

def cmd_storage_container_create(argv):
    """Create a storage container: the policy every vdisk in it inherits.

    Goes through Spectrum rather than writing the row here, so the CLI and the console
    enforce one set of rules -- name shape, tier, and a compression value the storage
    daemon will actually recognise -- instead of two that drift.
    """
    if len(argv) < 3:
        print("Usage: valcli storage.container.create <name> [--tier SSD|HDD|NVME] "
              "[--quota-gb N] [--ftt N] [--compression none|lz4]")
        sys.exit(1)

    payload = {"name": argv[2]}
    i = 3
    while i < len(argv):
        flag = argv[i]
        value = argv[i + 1] if i + 1 < len(argv) else None
        if value is None:
            print(f"Error: {flag} needs a value.")
            sys.exit(1)
        if flag == "--tier":
            payload["tier"] = value
        elif flag == "--quota-gb":
            try:
                payload["quota_bytes"] = int(value) * (1024 ** 3)
            except ValueError:
                print("Error: --quota-gb must be a whole number of gigabytes.")
                sys.exit(1)
        elif flag == "--ftt":
            payload["ftt"] = value
        elif flag == "--compression":
            payload["compression"] = value
        else:
            print(f"Error: unknown option {flag}")
            sys.exit(1)
        i += 2

    rc, body = run_spectrum_api(
        "/api/storage/containers/create", method="POST", payload=payload)
    _print_container_result(rc, body, "created")


def cmd_storage_container_update(argv):
    """Change a container's policy. Only the flags given are touched."""
    if len(argv) < 3:
        print("Usage: valcli storage.container.update <name> [--tier ...] [--quota-gb N] "
              "[--ftt N] [--compression none|lz4]")
        sys.exit(1)

    payload = {"name": argv[2]}
    i = 3
    while i < len(argv):
        flag = argv[i]
        value = argv[i + 1] if i + 1 < len(argv) else None
        if value is None:
            print(f"Error: {flag} needs a value.")
            sys.exit(1)
        if flag == "--tier":
            payload["tier"] = value
        elif flag == "--quota-gb":
            try:
                payload["quota_bytes"] = int(value) * (1024 ** 3)
            except ValueError:
                print("Error: --quota-gb must be a whole number of gigabytes.")
                sys.exit(1)
        elif flag == "--ftt":
            payload["ftt"] = value
        elif flag == "--compression":
            payload["compression"] = value
        else:
            print(f"Error: unknown option {flag}")
            sys.exit(1)
        i += 2

    if len(payload) == 1:
        print("Nothing to change. Give at least one of --tier, --quota-gb, --ftt, --compression.")
        sys.exit(1)
    # The server treats an absent quota as zero, which is "unlimited" rather than "leave
    # it alone", so an update that does not mean to touch it has to say what it is.
    if "quota_bytes" not in payload:
        payload["quota_bytes"] = _current_quota_bytes(argv[2])

    rc, body = run_spectrum_api(
        "/api/storage/containers/update", method="POST", payload=payload)
    _print_container_result(rc, body, "updated")


def cmd_storage_container_delete(argv):
    """Delete a container. Refused while any vdisk still names it."""
    if len(argv) < 3:
        print("Usage: valcli storage.container.delete <name>")
        sys.exit(1)
    rc, body = run_spectrum_api(
        "/api/storage/containers/delete", method="POST", payload={"name": argv[2]})
    _print_container_result(rc, body, "deleted")


def _current_quota_bytes(name):
    """This container's quota as it stands, so an update can leave it where it was."""
    rc, stdout, _ = run_cql_query(
        "SELECT JSON quota_bytes FROM hydra.storage_containers WHERE name = '%s';" % name)
    if rc != 0:
        return 0
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                return int(json.loads(line).get("quota_bytes") or 0)
            except Exception:
                return 0
    return 0


def _print_container_result(rc, body, verb):
    if rc != 0:
        detail = body.get("error") if isinstance(body, dict) else body
        print("Error: %s" % (detail or "the request was refused"))
        sys.exit(1)
    if isinstance(body, dict):
        print("Success: %s" % body.get("message", "container %s." % verb))
        if body.get("note"):
            print("  Note: %s" % body["note"])
    else:
        print("Success: container %s." % verb)


def cmd_storage_list():
    cql = ("SELECT JSON name, tier, quota_bytes, path, ftt, compression "
           "FROM hydra.storage_containers;")
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
                
    headers = ["Container Name", "Storage Tier", "Quota (GB)", "POSIX Path", "FTT", "Compression"]
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
            ftt_val,
            # Null is how every container that predates the column reads, and it means the
            # same thing the column means when it is set to "none".
            r.get("compression") or "none",
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
    disk_rows = []
    absent_rows = []
    for host in hosts:
        ip = host.get("ip")
        if not ip:
            continue
        status, body, err = run_mtls_spark_api_full(ip, "/api/v1/dfs/vdisk", {"op": "capacity"})
        store_row, body = extent_store_row(host.get("hostname") or ip, status, body, err)
        store_rows.append(store_row)
        if body is None:
            continue
        for disk in body.get("disks") or []:
            disk_rows.append(_disk_row(body.get("node") or host.get("hostname") or ip, disk))
        for gone in body.get("absent_disks") or []:
            absent_rows.append([body.get("node") or host.get("hostname") or ip,
                                gone.get("uuid") or "?", gone.get("role") or "?",
                                gone.get("reason") or "?"])
    print_table(["Node", "State", "Total", "Used", "Extent groups", "Journal"], store_rows)
    print()
    if disk_rows:
        print("=== Extent Store Disks ===")
        print_table(["Node", "Disk", "Mount", "Device", "Class", "Total", "Used", "Groups"],
                    disk_rows)
        print("  Disk is the identity written on the disk's own filesystem. Mount is the")
        print("  directory it is mounted at, which is only a label; Device is what the kernel")
        print("  says backs it now. The two can disagree, and the identity is the one that")
        print("  stays true across a reboot.")
        print()
    if absent_rows:
        # Reported by the storage layer, which is the only thing that knows what a missing
        # disk means. Sidon refuses these paths; it does not write to the root filesystem
        # in their place.
        print("=== Disks a node is configured to have and cannot use ===")
        print_table(["Node", "Filesystem", "Role", "Why"], absent_rows)
        print("  Named in /etc/hci/sidon-disks. `sidon mounts` on the node says the same.")
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
    # The operator supplies the statement and reads the output, including a conditional
    # statement's [applied] column, so this deliberately does not take the guard.
    rc, stdout, err = run_conditional_cql_query(query)
    if stdout:
        print(stdout)
    if err:
        print(err)
    if rc != 0:
        sys.exit(rc)

# The vdisk the benchmark runs on is deliberately several times Sidon's journal high-water
# mark (64 MiB by default). A benchmark that fits inside the journal measures the journal and
# nothing else; one that overruns it forces the drain -- the journal's move into extent
# groups -- to happen *during* the run, and what it reports is the rate Sidon can sustain
# with that cost amortised into it rather than the burst rate before the first drain.
BENCH_VDISK_BYTES = 256 * 1024 * 1024
MIB = 1024 * 1024

# (label, direction, block size, request count, queue depth, start offset). Offsets are
# disjoint so no phase overwrites another's range, and the read phase re-reads exactly what
# the sequential-write phase wrote.
BENCH_WARMUP = ("warm-up", "write", MIB, 16, 1, 0)
BENCH_PHASES = [
    ("sequential write 1M qd1", "write", MIB, 96, 1, 16 * MIB),
    ("sync write 4k qd1", "write", 4096, 400, 1, 112 * MIB),
    ("sequential read 1M qd1", "read", MIB, 96, 1, 16 * MIB),
    # The same small synchronous write with sixteen in flight, which is what a database or a
    # guest filesystem journal does: Sidon commits the writes that are waiting together, one
    # sync and one round trip for the lot, so this line is several times the qd1 one while
    # the qd1 line does not move. The pair is the measure of group commit. It runs after the
    # read so that the read line stays comparable with earlier runs: a read straight after
    # these 1,600 small writes measured about half as fast, for a reason that was not run down.
    ("sync write 4k qd16", "write", 4096, 1600, 16, 184 * MIB),
    ("write 1M qd4", "write", MIB, 32, 4, 120 * MIB),
    ("write 1M qd16", "write", MIB, 32, 16, 152 * MIB),
    # A guest that issues big requests (a copy, a restore) rather than one per block: each
    # is several journal records and one commit marker, which is the path that pipelining
    # between records is for.
    ("large write 16M qd1", "write", 16 * MIB, 4, 1, 192 * MIB),
]
BENCH_PATTERN = 0xAB


def _bench_run(nbd_url, direction, block, count, depth, offset):
    """One `qemu-img bench` run against an NBD export; returns elapsed seconds or None.

    qemu-img rather than qemu-io because it takes a real queue depth (`-d`) and times the
    run itself to the millisecond. qemu-io reports whole hundredths of a second, which is
    more than the whole of a 4 KiB write's latency, and its aio commands cannot hold a
    depth steady.
    """
    cmd = ["qemu-img", "bench", "-f", "raw", "-s", str(block), "-S", str(block),
           "-c", str(count), "-d", str(depth), "-o", str(offset)]
    if direction == "write":
        cmd += ["-w", "--pattern", str(BENCH_PATTERN)]
    cmd.append(nbd_url)
    try:
        out = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True, check=False)
    except OSError as ex:
        print("    could not run qemu-img: %s" % ex)
        return None
    m = re.search(r"Run completed in ([0-9.]+) seconds", out.stdout)
    if out.returncode != 0 or not m:
        print("    qemu-img bench failed: %s" % ((out.stderr or out.stdout).strip() or out.returncode))
        return None
    return float(m.group(1))


def _bench_settle(vdisk_id, timeout=60.0):
    """Wait until Sidon is not draining this vdisk; returns the seconds waited.

    A write is acknowledged before the drain it triggered has finished, so the guest-visible
    rate of a write phase leaves the drain's tail uncounted, and that tail would then run
    underneath the next phase. Waiting for it here does two things: the next phase starts
    from a quiet vdisk, and the wait is what lets a write phase also report its *sustained*
    rate -- bytes over the time until every drain it caused has finished.

    A daemon from before the drain moved to its own thread reports no `draining` field and
    drains inside the write, so there is never anything to wait for and the two rates agree.
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        rc, body, _ = run_mtls_spark_api(
            "127.0.0.1", "/api/v1/dfs/vdisk", {"op": "status", "vdisk_id": vdisk_id})
        if rc != 0 or not isinstance(body, dict) or not body.get("draining"):
            break
        time.sleep(0.05)
    return time.monotonic() - start


def _bench_line(label, block, count, depth, seconds, settle=0.0):
    """Format one result: throughput, request rate and the per-request time.

    For a write, `settle` is the time Sidon then spent finishing the drains the write caused;
    when it is not negligible the sustained rate (counting it) is printed beside the rate the
    guest saw.
    """
    if seconds is None or seconds <= 0:
        return "  %-26s no result" % label
    total = float(block) * count
    mibs = total / MIB / seconds
    iops = count / seconds
    if depth == 1:
        tail = "%.2f ms/op latency" % (seconds / count * 1000.0)
    else:
        # With several requests in flight there is no single latency; what is meaningful
        # is how far apart completions land, which is the inverse of the rate.
        tail = "%.2f ms/op completion interval" % (seconds / count * 1000.0)
    size = ("%d KiB" % (block // 1024)) if block < MIB else ("%d MiB" % (block // MIB))
    line = "  %-26s %8.1f MiB/s  %9.1f IOPS  %s  [%d x %s in %.2fs]" % (
        label, mibs, iops, tail, count, size, seconds)
    if settle >= 0.1:
        line += "  sustained incl. drain tail: %.1f MiB/s (+%.2fs)" % (
            total / MIB / (seconds + settle), settle)
    return line


def _bench_verify(nbd_url, offset, length):
    """Read back a range the benchmark wrote and check every byte is the pattern."""
    out = subprocess.run(
        ["qemu-io", "-f", "raw", "-c", "read -P 0x%X %d %d" % (BENCH_PATTERN, offset, length),
         nbd_url],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, check=False)
    return out.returncode == 0 and "verification failed" not in out.stdout.lower()


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
    #
    # The version after that wrote 64 MiB in one request into a brand-new 100 MiB vdisk and
    # printed qemu-io's own line. One request is not a rate: it landed exactly on the
    # journal's high-water mark, so the drain ran inside that single write and the number it
    # printed was a drain divided into 64 MiB, not what a guest streaming data would see.
    # This one warms up first, then reports each workload on its own line, steady-state.
    print("Creating temporary vdisk '%s' (%d MiB)..." % (vdisk_id, BENCH_VDISK_BYTES // MIB))
    rc_c, body_c, err_c = run_mtls_spark_api(
        "127.0.0.1", "/api/v1/dfs/vdisk",
        {"op": "create", "vdisk_id": vdisk_id, "size_bytes": BENCH_VDISK_BYTES,
         "container": default_container()})
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

        label, direction, block, count, depth, offset = BENCH_WARMUP
        print("Warming up (%d x %d MiB, not reported)..." % (count, block // MIB))
        _bench_run(nbd_url, direction, block, count, depth, offset)

        print("Results (the vdisk is larger than the journal high-water mark, so drains happen "
              "during the run; a write line also shows the sustained rate when a drain "
              "outlived it):")
        for label, direction, block, count, depth, offset in BENCH_PHASES:
            seconds = _bench_run(nbd_url, direction, block, count, depth, offset)
            settle = _bench_settle(vdisk_id) if direction == "write" else 0.0
            print(_bench_line(label, block, count, depth, seconds, settle))

        # Not timed: a check that what was written reads back, after the drains the write
        # phases forced. A benchmark that is fast because it lost data is worse than a slow one.
        ok = _bench_verify(nbd_url, 16 * MIB, 96 * MIB)
        print("  %-26s %s" % ("read-back verify", "ok" if ok else "FAILED -- data did not read back"))
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

def cmd_storage_derive(parent, child, kind):
    """`storage.snapshot` and `storage.clone`. One function; they differ by class.

    Sent to the node that owns the parent, not to this one. A writable parent has to be
    drained before its map is a complete answer, and only its owner can drain it -- so
    addressing the wrong node produces a refusal rather than a half-copied disk. An
    immutable parent has no journal and any node can copy it, which is the
    clone-from-image case.
    """
    owner = None
    rc, stdout, _ = run_cql_query(
        "SELECT JSON vdisk_id, owner, class FROM hydra.dfs_vdisks WHERE vdisk_id = '%s';" % parent)
    if rc == 0:
        for line in stdout.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("vdisk_id") == parent:
                owner = (row.get("owner") or "").strip()
                parent_class = row.get("class") or "rw"
                break
    else:
        print("Error: could not read hydra.dfs_vdisks.")
        sys.exit(1)

    if owner is None:
        print("Error: no vdisk named '%s'." % parent)
        sys.exit(1)

    target = "127.0.0.1"
    if owner:
        target = _ip_for_host(owner) or "127.0.0.1"

    rc, body, err = run_mtls_spark_api(
        target, "/api/v1/dfs/vdisk",
        {"op": kind, "vdisk_id": parent, "child_id": child})
    # A refusal comes back as rc 0 with the daemon's explanation in the body, so testing rc
    # alone reported "Snapshot created" for a snapshot Sidon had declined to take.
    if rc != 0 or (isinstance(body, dict) and body.get("error")):
        detail = body.get("error") if isinstance(body, dict) and body.get("error") else err
        print("Error: %s of '%s' failed: %s" % (kind, parent, detail))
        sys.exit(1)

    if kind == "snapshot":
        _index_manual_snapshot(parent, child)
    print("%s '%s' created from '%s'." % (kind.capitalize(), child, parent))
    print("  class    : %s" % body.get("class"))
    print("  size     : %.1f GiB" % ((body.get("size_bytes") or 0) / (1024.0 ** 3)))
    print("  extents  : %s shared with the parent" % body.get("extents"))
    print("  copied   : %s bytes -- a %s copies the map, never the data."
          % (body.get("bytes_copied", 0), kind))


def _ip_for_host(hostname):
    """The address of a node by its Sidon name, from cluster.json."""
    try:
        with open("/etc/hci/cluster.json", "r") as handle:
            data = json.load(handle)
    except Exception:
        return None
    for host in data.get("hosts", []):
        if host.get("hostname") == hostname:
            return host.get("ip")
    return None


def cmd_storage_children(parent):
    """Which snapshots and clones came from a vdisk.

    Worth having because nothing else can tell you. The extents are shared, so there is
    no way to work out afterwards which vdisk was copied from which -- `parent_vdisk` on
    the child's row is the only record, and it is why deleting a parent is safe and also
    why it looks alarming without this.
    """
    rc, stdout, _ = run_cql_query(
        "SELECT JSON vdisk_id, class, parent_vdisk, size_bytes FROM hydra.dfs_vdisks;")
    if rc != 0:
        print("Error: could not read hydra.dfs_vdisks.")
        sys.exit(1)

    rows = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if (row.get("parent_vdisk") or "") == parent:
            rows.append([
                row.get("vdisk_id", "?"),
                row.get("class", "?"),
                "%.1f" % ((row.get("size_bytes") or 0) / (1024.0 ** 3)),
            ])

    if not rows:
        print("No snapshots or clones were taken from '%s'." % parent)
        return
    print_table(["Child", "Class", "Size (GiB)"], sorted(rows))
    print("")
    print("These share extents with '%s'. Deleting it frees nothing they still point at:"
          % parent)
    print("mark-sweep reads the whole block map, so an extent group is live while any")
    print("vdisk references it.")


def _snapshots_module():
    """helios_snapshots and helios_schema, or an explanation of which is missing."""
    try:
        import helios_snapshots
        import helios_schema
        return helios_snapshots, helios_schema
    except ImportError as exc:
        print("Error: %s. The snapshot policy needs helios_snapshots.py and "
              "helios_schema.py installed beside valcli." % exc)
        sys.exit(1)


def _dfs_call(ip, payload):
    """One `/api/v1/dfs/vdisk` call whose refusal is a failure.

    `run_mtls_spark_api` answers an HTTP 409 with `rc == 0` and the daemon's explanation in
    the body, so a caller that tests only `rc` reads a refused operation as a successful
    one. Callers here want a refusal to be an error with the daemon's words attached.
    """
    rc, body, err = run_mtls_spark_api(ip, "/api/v1/dfs/vdisk", payload)
    if rc == 0 and isinstance(body, dict) and body.get("error"):
        return -1, body, body["error"]
    return rc, body, err


def _snapshots_env(say=None):
    snapshots, _schema = _snapshots_module()
    return snapshots.Env(
        run_cql_query, _dfs_call, say=say or print,
        # Set by Dagur when it runs this as a job, absent from a shell. Absent means the
        # tasks written here have no parent, which is the truth for a manual run.
        parent_task_id=os.environ.get("CATALYST_TASK_ID"))


def _index_manual_snapshot(parent, child):
    """Record a snapshot an operator took, so listings can tell it from a policy's.

    Best effort and silent: the snapshot exists whether or not this row does, and an
    unindexed snapshot is one retention never touches, which is the safe direction.
    """
    try:
        snapshots, _schema = _snapshots_module()
        runner = snapshots.Runner(_snapshots_env(say=lambda line: None), _schema)
        runner.record(parent, child, snapshots.ORIGIN_MANUAL, int(time.time() * 1000))
    except SystemExit:
        pass
    except Exception:
        pass


def _age(now_ms, then_ms):
    try:
        seconds = max(0, int((now_ms - int(then_ms)) / 1000))
    except (TypeError, ValueError):
        return "?"
    if seconds < 3600:
        return "%dm" % (seconds // 60)
    if seconds < 86400:
        return "%dh" % (seconds // 3600)
    return "%dd" % (seconds // 86400)


def cmd_storage_snapshots(vdisk_id):
    """Every snapshot of a vdisk, who took it, and whether anything depends on it.

    Read from `dfs_vdisks` (the lineage, which is always right) and joined with the index
    (who took it, which is only there for snapshots made since it existed). A snapshot the
    index has not heard of is shown as `unindexed`, and is never pruned.
    """
    snapshots, schema = _snapshots_module()
    runner = snapshots.Runner(_snapshots_env(), schema)
    try:
        vdisks = runner.vdisks()
        index = dict((r.get("snapshot_id"), r) for r in runner.index(vdisk_id))
    except RuntimeError as exc:
        print("Error: %s" % exc)
        sys.exit(1)
    referenced = set(v.get("parent_vdisk") for v in vdisks if v.get("parent_vdisk"))
    now_ms = int(time.time() * 1000)
    rows = []
    for v in vdisks:
        if v.get("parent_vdisk") != vdisk_id or v.get("class") != "immutable":
            continue
        sid = v.get("vdisk_id")
        meta = index.get(sid) or {}
        rows.append([
            sid, meta.get("origin") or "unindexed", _age(now_ms, v.get("created_at_ms")),
            "%.1f" % ((v.get("size_bytes") or 0) / (1024.0 ** 3)),
            "yes" if sid in referenced else "no",
        ])
    if not rows:
        print("No snapshots of '%s'." % vdisk_id)
        return
    print_table(["Snapshot", "Taken by", "Age", "Size (GiB)", "Has children"], sorted(rows))
    print("")
    print("'Has children' snapshots are never pruned by retention: a vdisk was derived from them.")


def _parse_policy_target(text):
    """`cluster`, `container:<name>` or `vdisk:<id>` as (scope, target)."""
    if text == "cluster":
        return "cluster", "*"
    scope, _sep, target = text.partition(":")
    if scope in ("container", "vdisk") and target:
        return scope, target
    return None, None


def cmd_storage_snapshot_policy():
    """The policies, narrowest scope first, and what each currently covers."""
    snapshots, schema = _snapshots_module()
    runner = snapshots.Runner(_snapshots_env(), schema)
    try:
        policies = runner.policies()
        vdisks = runner.vdisks()
    except RuntimeError as exc:
        print("Error: %s" % exc)
        sys.exit(1)
    if not policies:
        print("No snapshot policy is set, so nothing is snapshotted on a schedule.")
        print("Set one:  valcli storage.snapshot-policy.set cluster --every-hours 24 --keep 7")
        return
    rank = dict((s, i) for i, s in enumerate(snapshots.SCOPE_PRECEDENCE))
    rows = []
    for p in sorted(policies, key=lambda r: (rank.get(r.get("scope"), 9), r.get("target", ""))):
        covers = sum(
            1 for v in vdisks
            if (v.get("class") or "rw") == "rw"
            and snapshots.effective_policy(policies, v.get("vdisk_id"), v.get("container")) is p)
        rows.append([
            p.get("scope"), p.get("target"),
            "yes" if p.get("enabled") else "no (exempts)",
            "%gh" % ((p.get("interval_seconds") or 0) / 3600.0),
            p.get("keep_last"), covers,
        ])
    print_table(["Scope", "Target", "Enabled", "Every", "Keep", "Vdisks covered"], rows)
    print("")
    print("The narrowest policy for a vdisk wins. The job runs every %dh, so that is the"
          % (snapshots.RUN_INTERVAL_SECONDS // 3600))
    print("shortest interval a policy can keep. Only attached vdisks are snapshotted.")


def cmd_storage_snapshot_policy_set(argv):
    snapshots, schema = _snapshots_module()
    usage = ("Usage: valcli storage.snapshot-policy.set cluster|container:<name>|vdisk:<id> "
             "--every-hours N --keep N [--disable]")
    if len(argv) < 3:
        print(usage)
        sys.exit(1)
    scope, target = _parse_policy_target(argv[2])
    if scope is None:
        print(usage)
        sys.exit(1)
    options = {"--every-hours": None, "--keep": None}
    enabled = True
    i = 3
    while i < len(argv):
        if argv[i] == "--disable":
            enabled = False
            i += 1
        elif argv[i] in options and i + 1 < len(argv):
            options[argv[i]] = argv[i + 1]
            i += 2
        else:
            print(usage)
            sys.exit(1)
    try:
        hours = float(options["--every-hours"]) if options["--every-hours"] else 24.0
        keep = int(options["--keep"]) if options["--keep"] else 7
        statement = snapshots.policy_insert_statement(
            scope, target, enabled, int(hours * 3600), keep, int(time.time() * 1000))
    except (snapshots.PolicyError, ValueError) as exc:
        print("Error: %s" % exc)
        sys.exit(1)
    rc, _out, err = run_cql_query(statement)
    if rc != 0:
        print("Error: could not write the policy: %s" % err)
        sys.exit(1)
    print("%s policy for %s: %s, every %gh, keep %d." % (
        scope, target, "enabled" if enabled else "disabled (an exemption)", hours, keep))


def cmd_storage_snapshot_policy_delete(argv):
    snapshots, _schema = _snapshots_module()
    if len(argv) < 3:
        print("Usage: valcli storage.snapshot-policy.delete cluster|container:<name>|vdisk:<id>")
        sys.exit(1)
    scope, target = _parse_policy_target(argv[2])
    if scope is None:
        print("Usage: valcli storage.snapshot-policy.delete cluster|container:<name>|vdisk:<id>")
        sys.exit(1)
    rc, _out, err = run_cql_query(snapshots.policy_delete_statement(scope, target))
    if rc != 0:
        print("Error: could not delete the policy: %s" % err)
        sys.exit(1)
    print("Deleted the %s policy for %s. Snapshots it already took are kept; "
          "nothing prunes them any more." % (scope, target))


def cmd_storage_snapshot_run(argv):
    """One pass of the snapshot policy. The same pass Rauru runs on its interval; this is the manual form.

    The exit status is the verdict: non-zero when any snapshot or prune failed, which is what
    a script or an operator keys on. A vdisk skipped because it
    is not attached is not a failure -- nothing is writing to it, so the newest snapshot is
    still a true picture of it -- but it is printed, never silent.
    """
    snapshots, schema = _snapshots_module()
    runner = snapshots.Runner(_snapshots_env(), schema, dry_run="--dry-run" in argv)
    try:
        summary = runner.run()
    except RuntimeError as exc:
        print("Error: %s" % exc)
        sys.exit(1)
    print("snapshots taken: %d, pruned: %d, skipped: %d, kept past retention: %d, failed: %d" % (
        len(summary.taken), len(summary.pruned), len(summary.skipped),
        len(summary.spared), len(summary.failures)))
    if not summary.ok:
        sys.exit(1)


def _vm_power_call(host_ip, vm_name, action):
    """One typed power call to the host a VM runs on: (rc, body, err).

    A refusal comes back as an HTTP 409 whose body carries the domain's state after the
    attempt, which is what a caller needs and what `_dfs_call`'s rule of turning every
    `error` into a failure would discard. The body is returned as it came.
    """
    return run_mtls_spark_api(
        host_ip, "/api/v1/vm/%s/power" % urllib.parse.quote(vm_name, safe=""),
        {"action": action})


def cmd_storage_domain(argv):
    """The `storage.domain*` commands: protection domains. The implementation is
    rauru_protection, which the Rauru daemon imports too, so the CLI and the daemon run one."""
    try:
        import rauru_protection
        import helios_schema
    except ImportError as exc:
        print("Error: %s. Protection domains need rauru_protection.py and helios_schema.py "
              "installed beside valcli." % exc)
        sys.exit(1)
    env = rauru_protection.Env(
        run_cql_query, _dfs_call, _vm_power_call, say=print,
        parent_task_id=os.environ.get("CATALYST_TASK_ID"))
    status = rauru_protection.run_command(argv, env, helios_schema)
    if status:
        sys.exit(status)


def cmd_storage_rollback(argv):
    snapshots, schema = _snapshots_module()
    if len(argv) < 4:
        print("Usage: valcli storage.rollback <vdisk> <snapshot> [--no-keep]")
        sys.exit(1)
    vdisk_id, snapshot_id = argv[2], argv[3]
    runner = snapshots.Runner(_snapshots_env(), schema)
    try:
        body = runner.rollback(vdisk_id, snapshot_id, keep="--no-keep" not in argv)
    except snapshots.RollbackRefused as exc:
        print("Refused: %s" % exc)
        sys.exit(1)
    except (RuntimeError, snapshots.PolicyError) as exc:
        print("Error: rollback of '%s' to '%s' failed: %s" % (vdisk_id, snapshot_id, exc))
        sys.exit(1)
    print("Rolled '%s' back to '%s'." % (vdisk_id, snapshot_id))
    print("  epoch    : %s -> %s" % (body.get("previous_epoch"), body.get("epoch")))
    print("  extents  : %s restored from the snapshot, 0 bytes copied" % body.get("extents"))
    if body.get("kept_as"):
        print("  kept as  : %s (what the disk held before; roll back to it to undo)"
              % body.get("kept_as"))
    print("Start the VM when ready.")


def _replication_policy():
    """(ftt, node_count) from cluster.json -- the redundancy factor and who could hold a copy.

    `redundancy_factor` counts failures survived, not copies kept, which is the same unit a
    container's `ftt` uses and one less than `dfs_vdisks.rf`. Returning it in its own unit
    and converting once, at the single place that needs copies, is deliberate: the reason
    every vdisk on this cluster was created single-copy is that the two units met at a
    boundary where nothing named either of them.

    `(None, n)` means the document could not be read. Distinct from ftt 0, which is a
    cluster that asked for one copy and got what it asked for.
    """
    try:
        with open("/etc/hci/cluster.json", "r") as handle:
            data = json.load(handle)
    except Exception:
        return None, 1
    nodes = len(data.get("hosts") or []) or 1
    try:
        return int(data["redundancy_factor"]), nodes
    except (KeyError, TypeError, ValueError):
        return None, nodes


def _copies_for_ftt(ftt, nodes):
    """Copies a vdisk should hold to survive `ftt` failures on a cluster of `nodes`.

    The +1 that went missing. Clamped to the cluster, because a single-node deployment
    holding one copy at ftt=1 is a supported topology and not a disk to shout about.
    """
    return max(1, min(int(ftt) + 1, max(int(nodes), 1)))


def cmd_storage_replication():
    """What each vdisk asked for against what it actually has.

    This view did not exist, and its absence is the whole reason a cluster of
    single-copy vdisks looked healthy for as long as it did. Everything that reported on
    replication compared a vdisk's replica list to the `rf` on its own row -- and since
    creates defaulted to `rf=1` and duly placed one replica, every disk in the fleet read
    1/1 and nothing was ever short of anything. The number being satisfied was the number
    that was wrong.

    So this prints three columns rather than two, and the third is the one that was
    missing: the copies the *container or cluster policy* asks for, beside the copies the
    vdisk asked for, beside the copies that exist. A row where the last two agree and the
    first is larger is not a degraded disk -- it is a disk that was never asked to be
    durable, which is a different problem with a different fix.
    """
    cluster_ftt, nodes = _replication_policy()

    # Container policy first: migration 0006 records a vdisk's rf as copied from its
    # container's ftt, so the container is what a vdisk is measured against. The cluster's
    # own factor stands in for containers that say nothing, which today includes every
    # vdisk created without naming one.
    container_ftt = {}
    rc, stdout, _ = run_cql_query("SELECT JSON name, ftt FROM hydra.storage_containers;")
    if rc == 0:
        for line in stdout.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("ftt") is not None:
                container_ftt[row.get("name")] = row.get("ftt")

    rc, stdout, err = run_cql_query(
        "SELECT JSON vdisk_id, container, class, rf, replicas FROM hydra.dfs_vdisks;")
    if rc != 0:
        print("Error: could not read hydra.dfs_vdisks: %s" % (err or stdout))
        sys.exit(1)

    rows = []
    short_of_policy = 0
    degraded = 0
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue

        container = row.get("container") or ""
        ftt = container_ftt.get(container, cluster_ftt)
        want = _copies_for_ftt(ftt, nodes) if ftt is not None else None
        asked = int(row.get("rf") or 0)
        have = len(row.get("replicas") or [])

        # Three states, and keeping them apart is the point of the command. A disk short
        # of its own rf has lost copies it once had and Purah's heal is the answer. A disk
        # whose rf is below policy never asked for them, so there is nothing to restore --
        # it needs a top-up, which is a deliberate act and says so here rather than
        # happening on a timer.
        if want is not None and asked < want:
            verdict = "under-policy"
            short_of_policy += 1
        elif asked and have < asked:
            verdict = "degraded"
            degraded += 1
        elif row.get("class") == "immutable":
            verdict = "sealed"
        else:
            verdict = "ok"

        rows.append([
            row.get("vdisk_id", "?"),
            container or "(unset)",
            "?" if want is None else str(want),
            str(asked or "?"),
            str(have),
            verdict,
        ])

    if not rows:
        print("No vdisks exist yet.")
        return

    print("=== Replication ===")
    if cluster_ftt is None:
        print("cluster.json could not be read, so policy is unknown for any container")
        print("that does not set its own ftt.")
    else:
        print("Cluster redundancy factor %d (survive %d host loss%s) across %d node(s)."
              % (cluster_ftt, cluster_ftt, "" if cluster_ftt == 1 else "es", nodes))
        if cluster_ftt == 0 and nodes >= 2:
            # `cluster create` sets 0 for one node and nothing raised it when the cluster
            # grew, so this is usually an inheritance and not a choice.
            print("That is probably left over from creating the cluster on one node. Every")
            print("new vdisk in a container with no ftt of its own gets ONE copy. To change")
            print("it:  cluster add-node --node <a member ip> -r 1")
    print()
    print_table(["Vdisk", "Container", "Policy", "Asked (rf)", "Copies", "State"],
                sorted(rows))
    print()
    if short_of_policy:
        print("%d vdisk(s) record an rf below what their container or the cluster asks"
              % short_of_policy)
        print("for. They are not damaged and nothing is missing: they were created before")
        print("the create path consulted the redundancy factor, so they never requested a")
        print("second copy. Topping one up copies its extents to another node, so it is")
        print("something to schedule rather than something to trigger by reading this:")
        print()
        print("  valcli storage.replicate <vdisk_id>       one vdisk, on its owner")
        print("  valcli storage.replicate --all            every vdisk this cluster owns")
        print()
    if degraded:
        print("%d vdisk(s) hold fewer copies than their own rf asks for. Purah re-replicates"
              % degraded)
        print("these on its own; a persistent count here means no spare node was available.")


def cmd_storage_replicate(target, everything=False):
    """Add a copy to vdisks that are short of the rf they record.

    The deliberate half of `storage.replication`. Purah has done re-replication since the
    DFS shipped, but only ever as an *emergency*: it replaces a replica that stopped
    answering, because write-all means the guest is taking EIO until it does. A vdisk that
    is merely short of copies is not an emergency and must not be treated as one -- healing
    those on the timer would have turned the create-time fix into an unannounced copy of
    every disk on the cluster the next time a node restarted.

    So the top-up is this, typed by an operator, and it adds one copy per vdisk per run.
    Run it again for the next one. That is not a limitation to work around: it is how the
    amount of copying stays something you can watch finish.
    """
    hosts = []
    try:
        with open("/etc/hci/cluster.json", "r") as handle:
            hosts = json.load(handle).get("hosts", [])
    except Exception:
        pass
    if not hosts:
        hosts = [{"ip": "127.0.0.1", "hostname": "this node"}]

    # Addressed to the owner, because only the owner holds the data to copy. For --all
    # that means every node: each answers for the vdisks it owns and ignores the rest.
    if everything:
        targets = [h.get("ip") for h in hosts if h.get("ip")]
    else:
        owner = None
        rc, stdout, _ = run_cql_query(
            "SELECT JSON vdisk_id, owner FROM hydra.dfs_vdisks WHERE vdisk_id = '%s';" % target)
        if rc != 0:
            print("Error: could not read hydra.dfs_vdisks.")
            sys.exit(1)
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    owner = (json.loads(line).get("owner") or "").strip()
                except ValueError:
                    pass
        if not owner:
            print("Error: '%s' is not attached anywhere, so no node holds it to copy from."
                  % target)
            print("Attach it to a host first -- re-replication runs on the owner.")
            sys.exit(1)
        targets = [_ip_for_host(owner) or "127.0.0.1"]

    healed_total = 0
    for ip in targets:
        # The vdisk id goes to the daemon rather than being used to filter its report.
        # Asking it to heal everything and then printing one line would top up every disk
        # on the node while looking like it had touched one, which is the whole thing this
        # command exists not to do.
        payload = {"op": "purah-heal", "restore_rf": True}
        if not everything:
            payload["vdisk_id"] = target
        rc, body, err = run_mtls_spark_api(ip, "/api/v1/dfs/vdisk", payload)
        if rc != 0 or not isinstance(body, dict):
            detail = body.get("error") if isinstance(body, dict) else err
            print("[%s] re-replication could not be started: %s" % (ip, detail))
            continue
        healed = body.get("healed") or []
        stuck = body.get("degraded") or []
        for entry in healed:
            healed_total += 1
            print("[%s] %s: copied %s extent(s) to %s"
                  % (ip, entry.get("vdisk_id"), entry.get("extents_copied"),
                     entry.get("with")))
        for entry in stuck:
            print("[%s] %s: %s" % (ip, entry.get("vdisk_id"), entry.get("detail")))

    if healed_total:
        print()
        print("%d vdisk(s) gained a copy. Run `valcli storage.replication` to see what is"
              % healed_total)
        print("still short -- a vdisk needing two more copies takes two runs.")
    else:
        print("Nothing was re-replicated. Either everything already holds the copies it")
        print("asks for, or no spare node was free to take one.")


def _disk_row(node, disk):
    """One extent-store disk as a table row, from a `capacity` entry."""
    gib = 1024 ** 3
    total = disk.get("total_bytes")
    avail = disk.get("available_bytes")
    uid = disk.get("uid") or "?"
    if not disk.get("uid_persisted", True):
        # Derived from the label rather than stored on the disk, so no more stable than the
        # label is. Marked so it is not mistaken for an identity.
        uid += " (not persisted)"
    return [
        node,
        uid,
        disk.get("label") or "?",
        disk.get("device") or "-",
        disk.get("tier") or "unknown",
        "-" if total is None else "%.1f GiB" % (int(total) / gib),
        "-" if total is None or avail is None else "%.1f GiB" % ((int(total) - int(avail)) / gib),
        str(disk.get("egroup_count", 0)),
    ]


def _storage_hosts():
    hosts = []
    try:
        with open("/etc/hci/cluster.json", "r") as handle:
            hosts = json.load(handle).get("hosts", [])
    except Exception:
        pass
    if not hosts:
        hosts = [{"ip": "127.0.0.1", "hostname": "this node"}]
    return hosts


def cmd_storage_placement(limit=10):
    """Which disk of each node holds which extent groups.

    Read by the daemon from the disks' own directories and not from its bookkeeping, so
    that it is still an answer when the bookkeeping is what is in doubt. A group listed
    under two disks of one node is a surplus copy a move left behind, and is named
    separately below the disks rather than hidden inside them.
    """
    answered = 0
    for host in _storage_hosts():
        ip = host.get("ip")
        if not ip:
            continue
        label = host.get("hostname") or ip
        rc, body, err = run_mtls_spark_api(
            ip, "/api/v1/dfs/vdisk", {"op": "purah-placement", "limit": limit})
        if rc != 0 or not isinstance(body, dict) or "disks" not in body:
            detail = body.get("error") if isinstance(body, dict) else err
            print("[%s] no placement data: %s" % (label, detail))
            continue
        answered += 1
        print()
        print("%s" % label)
        for disk in body.get("disks") or []:
            print("  disk %s  (mounted at %s, device %s, class %s)  %d group(s)"
                  % (disk.get("uid"), disk.get("label"), disk.get("device") or "-",
                     disk.get("tier"), disk.get("egroup_count") or 0))
            for group in disk.get("groups") or []:
                print("      %-48s %10d bytes" % (group.get("egroup_id"), group.get("size") or 0))
            if disk.get("groups_truncated"):
                print("      ... and %d more" % ((disk.get("egroup_count") or 0)
                                                 - len(disk.get("groups") or [])))
            if disk.get("in_flight_copies"):
                print("      %d copy in progress or abandoned" % disk["in_flight_copies"])
        for extra in body.get("surplus_copies") or []:
            print("  surplus copy: %s on disk %s (a move left it; the sweep removes it)"
                  % (extra.get("egroup_id"), extra.get("disk")))
    if not answered:
        print()
        print("No node answered. Either sidon is older than disk placement or it is down.")


def cmd_storage_tier(apply=False):
    """Plan the disk-to-disk moves the heat ranking argues for, or carry them out.

    Plans only unless --apply is given, and nothing runs it on a timer. The ranking behind
    it was shipped as reporting precisely so that somebody would read it before anything
    acted on it.

    Heat decides where a copy of an extent group sits. It never decides whether one exists:
    the counters are approximate and a crash loses a window of them, so nothing here
    deletes, shortens a replica set or reclaims on their strength. A move copies the group,
    proves the copy, switches to it, and leaves the old one for the sweep.

    On a node whose disks are all the same class there is nothing to decide, and the plan
    says so rather than printing an empty list.
    """
    for host in _storage_hosts():
        ip = host.get("ip")
        if not ip:
            continue
        label = host.get("hostname") or ip
        rc, body, err = run_mtls_spark_api(
            ip, "/api/v1/dfs/vdisk", {"op": "purah-tier", "apply": bool(apply)})
        if rc != 0 or not isinstance(body, dict) or "planned" not in body:
            detail = body.get("error") if isinstance(body, dict) else err
            print("[%s] tiering failed: %s" % (label, detail))
            continue
        print()
        print("%s -- %d sealed group(s) considered" % (label, body.get("considered") or 0))
        for disk in body.get("disks") or []:
            print("  disk %s (%s) class %s" % (disk.get("uid"), disk.get("label"), disk.get("tier")))
        if body.get("dropped"):
            print("  WARNING: the tally is at capacity; unmeasured groups are not offered as cold.")
        for move in body.get("planned") or []:
            print("  %s %s  %s -> %s  %d bytes  heat %s"
                  % ("would" if not apply else "plan:", move.get("direction"),
                     move.get("from_label"), move.get("to_label"), move.get("bytes") or 0,
                     "unmeasured" if move.get("heat") is None else "%.2f" % move["heat"]))
            print("      %s" % move.get("egroup_id"))
        for note in body.get("notes") or []:
            print("  note: %s" % note)
        for done in body.get("executed") or []:
            print("  moved %s to %s (%s)" % (done.get("egroup_id"), done.get("to"), done.get("hash")))
        for bad in body.get("failed") or []:
            print("  FAILED %s: %s" % (bad.get("egroup_id"), bad.get("error")))
    if not apply:
        print()
        print("Nothing was moved. Re-run with --apply to carry the plan out.")


def cmd_storage_move(egroup_id, disk, node):
    """Move one sealed extent group to another disk of one node.

    The mechanism with no policy in front of it: how a group is rebalanced by hand, and the
    way to exercise a move on nodes whose disks are identical and so give the policy
    nothing to decide. The node is named because a disk label such as sdc exists on every
    node and would otherwise be ambiguous.
    """
    target = None
    for host in _storage_hosts():
        if node in (host.get("ip"), host.get("hostname")):
            target = host.get("ip")
    if not target:
        print("Error: no node %r in the cluster." % node)
        sys.exit(1)
    rc, body, err = run_mtls_spark_api(
        target, "/api/v1/dfs/vdisk",
        {"op": "purah-move", "egroup_id": egroup_id, "disk": disk})
    if rc != 0 or not isinstance(body, dict) or "to" not in body:
        detail = body.get("error") if isinstance(body, dict) else err
        print("Error: %s" % detail)
        sys.exit(1)
    print("Moved %s to disk %s (%d bytes, %s)." % (
        body.get("egroup_id"), body.get("to"), body.get("bytes") or 0, body.get("hash")))
    print("The old copy stays until the sweep has seen it surplus on two passes.")


def cmd_storage_takeover(vdisk_id, host):
    """Finish a live migration's storage handover by hand.

    A migration that moved the guest but could not finish the handover leaves its disks
    served through the destination's forwarder; this runs the takeover on that node again.
    It is safe to repeat: it reads Hydra afresh, asks the owner to let go if it still serves
    the disk, wins the claim and fences the replicas, and does nothing if the node already
    owns the disk. Run it on the node the guest is running on.
    """
    target = None
    for h in _storage_hosts():
        if host in (h.get("ip"), h.get("hostname")):
            target = h.get("ip")
    if not target:
        print("Error: no node %r in the cluster." % host)
        sys.exit(1)
    status, body, err = run_mtls_spark_api_full(
        target, "/api/v1/dfs/vdisk", {"op": "takeover", "vdisk_id": vdisk_id}, timeout=330)
    if status != 200 or not isinstance(body, dict):
        detail = body.get("error") if isinstance(body, dict) and body.get("error") else err
        print("Error: %s" % (detail or "HTTP %s" % status))
        sys.exit(1)
    if body.get("already_owned"):
        print("%s is already owned by %s." % (vdisk_id, host))
        return
    print("%s: %s took over from %s at epoch %s." % (
        vdisk_id, host, body.get("previous_owner") or "nobody", body.get("epoch")))


def _compact_request(argv):
    """The control request `storage.compact` sends, from its command line.

    Plans unless `--apply` is present, and that is the only way to apply. Anything it does not
    recognise is an error and not an ignored word: a misspelt `--aplly` that fell through to a
    plan would be harmless, but a misspelt limit that fell through to the default would run a
    bigger pass than the operator asked for.
    """
    request = {"op": "purah-compact", "apply": False}
    numeric = {"--threshold": ("threshold", float), "--max-groups": ("max_groups", int),
               "--max-bytes": ("max_bytes", int), "--seconds": ("seconds", int),
               "--rate": ("rate_bytes_per_second", int)}
    args = list(argv)
    while args:
        word = args.pop(0)
        if word == "--apply":
            request["apply"] = True
        elif word in numeric and args:
            key, kind = numeric[word]
            try:
                request[key] = kind(args.pop(0))
            except ValueError:
                raise SystemExit("Usage: valcli storage.compact [--apply] [--threshold F] "
                                 "[--max-groups N] [--max-bytes N] [--seconds N] [--rate BYTES]")
        else:
            raise SystemExit("Usage: valcli storage.compact [--apply] [--threshold F] "
                             "[--max-groups N] [--max-bytes N] [--seconds N] [--rate BYTES]")
    return request


def cmd_storage_compact(argv):
    """Plan, or with --apply carry out, compaction of sparse sealed extent groups.

    A guest overwrite leaves garbage inside a sealed group, and the sweep frees a group only
    when nothing in it is live. Compaction copies the live extents of groups that are mostly
    dead into new groups, verifies them, repoints the map by compare-and-swap, and leaves the
    old groups for the sweep's two-scan grace.

    Plans unless --apply is given, and nothing runs it on a timer (D-32). A pass is bounded by
    a number of groups, a number of bytes, a rate and a time, and prints which bound it hit; run
    it again to continue. Per node: a node compacts the groups it created.

    The old groups are freed by the sweep, here and (D-33) on every replica, which the sweep asks
    to drop its copy. Each plan step prints what is added on the replicas and what they get back
    once the sweep has run twice; a replica still running a build from before D-33 keeps its copy,
    and for that replica the added figure is growth with no saving until it is upgraded.
    """
    request = _compact_request(argv)
    apply = request["apply"]
    answered = 0
    for host in _storage_hosts():
        ip = host.get("ip")
        if not ip:
            continue
        label = host.get("hostname") or ip
        rc, body, err = run_mtls_spark_api(ip, "/api/v1/dfs/vdisk", request)
        if rc != 0 or not isinstance(body, dict) or "candidate_count" not in body:
            detail = body.get("error") if isinstance(body, dict) else err
            print("[%s] compaction failed: %s" % (label, detail))
            continue
        answered += 1
        print()
        print("%s: %s" % (label, body.get("status")))
        for cand in body.get("candidates") or []:
            print("  %-44s %5.1f%% live  %9d of %9d bytes  %d extent(s), %d shared  [%s]"
                  % (cand.get("egroup_id"), 100.0 * (cand.get("live_fraction") or 0),
                     cand.get("live_bytes") or 0, cand.get("size_bytes") or 0,
                     cand.get("live_extents") or 0, cand.get("shared_extents") or 0,
                     ", ".join(cand.get("vdisks") or [])))
        if (body.get("candidate_count") or 0) > len(body.get("candidates") or []):
            print("  ... and %d more" % (body["candidate_count"] - len(body.get("candidates") or [])))
        for skip in body.get("skipped") or []:
            print("  skipped %s: %s" % (skip.get("egroup_id"), skip.get("reason")))
        for step in body.get("plan") or []:
            replicas = ", ".join(step.get("replicas") or [])
            line = ("  %s: copy %d bytes from %s to a new group; %d bytes freed here once swept"
                    % ("would" if not apply else "plan", step.get("bytes_to_copy") or 0,
                       ", ".join(step.get("sources") or []), step.get("freed_here_after_sweep") or 0))
            if replicas:
                line += ("; on %s %d bytes added now and %d freed once swept (net %d)"
                         % (replicas, step.get("added_on_replicas") or 0,
                            step.get("freed_on_replicas_after_sweep") or 0,
                            step.get("net_freed_on_replicas") or 0))
            print(line)
        for done in body.get("executed") or []:
            print("  done: %s (%d bytes) from %s; %d row(s) repointed, %d lost to an overwrite"
                  % (done.get("new_group"), done.get("bytes") or 0, ", ".join(done.get("sources") or []),
                     done.get("rows_repointed") or 0, done.get("lost_races") or 0))
            for bad in done.get("unusable_sources") or []:
                print("    left out %s: %s" % (bad.get("egroup_id"), bad.get("reason")))
        for bad in body.get("failed") or []:
            print("  FAILED %s: %s" % (", ".join(bad.get("sources") or []), bad.get("error")))
        for anomaly in body.get("anomalies") or []:
            print("  ANOMALY: %s" % anomaly)
        if body.get("stopped_by"):
            print("  stopped by %s; run it again to continue." % body["stopped_by"])
    if not answered:
        print()
        print("No node answered. Either sidon is older than compaction or it is down.")
    elif not apply:
        print()
        print("Nothing was changed. Re-run with --apply to carry the plan out.")


def _dedup_request(argv):
    request = {"op": "purah-dedup", "digests": True}
    args = list(argv)
    usage = "Usage: valcli storage.dedup.estimate [--sample F] [--seconds N]"
    while args:
        word = args.pop(0)
        if word == "--sample" and args:
            try:
                request["sample"] = float(args.pop(0))
            except ValueError:
                raise SystemExit(usage)
            if not 0 < request["sample"] <= 1:
                raise SystemExit(usage + "   (the sample is a fraction above 0 and at most 1)")
        elif word == "--seconds" and args:
            try:
                request["seconds"] = int(args.pop(0))
            except ValueError:
                raise SystemExit(usage)
        else:
            raise SystemExit(usage)
    return request


def _merge_dedup(bodies):
    """Cluster-wide figures per container from every node's answer.

    A node hashes only the groups it created, so two copies of one extent on two nodes are
    invisible to either. Each node returns a short digest per sampled extent; counting those
    across nodes is what finds the duplicates that straddle them. Nodes sampled the same
    locations by construction, and a node cut short by its time budget covered less, so the
    scaled figure uses the smallest fraction any node covered.
    """
    merged = {}
    covered = min([b.get("sample_covered") or 0 for b in bodies] or [0])
    exact = all(b.get("exact") for b in bodies)
    for body in bodies:
        for c in body.get("containers") or []:
            m = merged.setdefault(c["container"], {
                "stored_bytes": 0, "stored_extents": 0, "logical_bytes": 0,
                "shared_by_clone_bytes": 0, "sampled_bytes": 0, "digests": [],
                "zero_bytes": 0, "unreadable": 0, "have_digests": True})
            m["stored_bytes"] += c.get("stored_bytes") or 0
            m["stored_extents"] += c.get("stored_extents") or 0
            m["logical_bytes"] += c.get("logical_bytes") or 0
            m["shared_by_clone_bytes"] += c.get("shared_by_clone_bytes") or 0
            m["sampled_bytes"] += c.get("sampled_bytes") or 0
            m["zero_bytes"] += c.get("zero_extent_bytes_in_sample") or 0
            m["unreadable"] += c.get("unreadable_extents") or 0
            if "digests" in c:
                m["digests"].extend(c["digests"])
            elif c.get("sampled_extents"):
                m["have_digests"] = False
    out = []
    for name in sorted(merged):
        m = merged[name]
        seen = set()
        unique = 0
        for digest, size in m["digests"]:
            if digest not in seen:
                seen.add(digest)
                unique += size
        observed = max(0, sum(s for _, s in m["digests"]) - unique)
        scaled = observed if exact or covered <= 0 else min(m["stored_bytes"], int(round(observed / covered)))
        out.append({
            "container": name, "stored_bytes": m["stored_bytes"], "stored_extents": m["stored_extents"],
            "logical_bytes": m["logical_bytes"], "shared_by_clone_bytes": m["shared_by_clone_bytes"],
            "would_share_bytes": scaled, "zero_bytes_in_sample": m["zero_bytes"],
            "unreadable": m["unreadable"], "fraction": (float(scaled) / m["stored_bytes"]) if m["stored_bytes"] else 0.0,
            "merged_across_nodes": m["have_digests"],
        })
    return {"containers": out, "exact": exact, "covered": covered}


def cmd_storage_dedup_estimate(argv):
    """How many bytes dedup would share beyond what clone-from-image already shares.

    The first step D-23's addendum asks for before anything is built: a number measured on
    this cluster's own data. Read-only. It hashes sealed extents (changing no extent id),
    counts the ones whose content another extent holds, and reports, per container, what is
    already shared by clone and what dedup would add. Sampled by default so it is cheap; at
    --sample 1 it reads every sealed extent and the figure is exact.

    Writes nothing, builds no index and has no setting: it is not dedup. The working threshold
    in D-23 is roughly ten to fifteen percent beyond clone sharing; below that the estimate
    argues against building anything.
    """
    request = _dedup_request(argv)
    bodies = []
    for host in _storage_hosts():
        ip = host.get("ip")
        if not ip:
            continue
        label = host.get("hostname") or ip
        rc, body, err = run_mtls_spark_api(ip, "/api/v1/dfs/vdisk", request)
        if rc != 0 or not isinstance(body, dict) or "containers" not in body:
            detail = body.get("error") if isinstance(body, dict) else err
            print("[%s] no estimate: %s" % (label, detail))
            continue
        bodies.append(body)
        print("%s: %s" % (label, body.get("status")))
    if not bodies:
        print()
        print("No node answered. Either sidon is older than the estimator or it is down.")
        return
    mib = float(1 << 20)
    merged = _merge_dedup(bodies)
    print()
    print("Across %d node(s), %s (sampled %.1f%% of stored extents):"
          % (len(bodies), "exact" if merged["exact"] else "an estimate", 100.0 * merged["covered"]))
    for c in merged["containers"]:
        print("  container %s" % c["container"])
        print("    stored            %10.1f MiB in %d extent(s)" % (c["stored_bytes"] / mib, c["stored_extents"]))
        print("    already shared    %10.1f MiB by clone and snapshot (logical %.1f MiB)"
              % (c["shared_by_clone_bytes"] / mib, c["logical_bytes"] / mib))
        print("    would also share  %10.1f MiB  = %.1f%% of stored%s"
              % (c["would_share_bytes"] / mib, 100.0 * c["fraction"],
                 "" if c["merged_across_nodes"] else "  (per node only: a node did not return digests)"))
        if c["zero_bytes_in_sample"]:
            print("    of the sample, %.1f MiB is zero-filled extents, which a sparse map would drop"
                  " without hashing anything" % (c["zero_bytes_in_sample"] / mib))
        if c["unreadable"]:
            print("    %d sampled extent(s) could not be read and are not counted" % c["unreadable"])
    print()
    print("A sample sees a duplicate only when both copies were sampled, so it undercounts content")
    print("that exists twice and is fair for content that exists many times. Extent granularity")
    print("only. This is a measurement: nothing was written and nothing was deduplicated.")
    print("D-23 puts the bar for building anything at roughly 10-15% beyond clone sharing.")


def cmd_storage_heat(limit=10):
    """Which extent groups each node reads and writes most, and which have gone cold.

    The operator's window onto `hydra.dfs_egroup_access`. Sidon counts every extent read
    and every extent a drain appends, in memory, and flushes the totals to Hydra on a
    timer; this asks each node to flush and rank, so the answer describes the cluster now
    rather than as of the last tick.

    Asked of the daemon rather than computed from the table here, for the same reason
    `storage.cleanup_orphaned` asks Purah rather than globbing files: a second
    implementation of the scoring would be a second answer to "is this hot", and the two
    would disagree in exactly the situation somebody is using them to settle. The formula
    lives in Purah, and every column it uses is in the report so the arithmetic can be
    checked by hand.

    **The counters are approximate and they are meant to be.** They live in the daemon
    between flushes, so a node that crashed lost everything it had counted since its last
    one and started a fresh window. This data decides which disk a copy of something should
    sit on, never whether the copy exists, so the cost of being wrong about it is a
    misplaced extent group -- which is why it is allowed to be cheap enough to record on
    the read path at all.

    Nothing here moves data. Tiering -- spilling cold extent groups to slower disks -- is
    the work this ranking exists to feed, and `storage.tier` is the command that reads it;
    see docs/dfs/multi_disk.md.
    """
    hosts = []
    try:
        with open("/etc/hci/cluster.json", "r") as handle:
            hosts = json.load(handle).get("hosts", [])
    except Exception:
        pass
    if not hosts:
        hosts = [{"ip": "127.0.0.1", "hostname": "this node"}]

    answered = 0
    for host in hosts:
        ip = host.get("ip")
        if not ip:
            continue
        label = host.get("hostname") or ip
        rc, body, err = run_mtls_spark_api(
            ip, "/api/v1/dfs/vdisk", {"op": "purah-heat", "limit": limit})
        if rc != 0 or not isinstance(body, dict) or "hot" not in body:
            detail = body.get("error") if isinstance(body, dict) else err
            print("[%s] no access data: %s" % (label, detail))
            continue
        answered += 1
        print()
        print("%s -- %d extent group(s) held, %d with access data"
              % (label, body.get("inventory") or 0, body.get("observed") or 0))
        dropped = body.get("dropped") or 0
        if dropped:
            # Said before the ranking, not after it. A capped tally means the ranking
            # describes part of the node, and a partial ranking read as a complete one is
            # how a tiering decision spills something busy.
            print("  WARNING: the tally is at capacity; %d extent group(s) are uncounted,"
                  % dropped)
            print("  so some of what follows reads as cold because it was never measured.")
        _print_heat_rows("hottest", body.get("hot") or [])
        _print_heat_rows("coldest", body.get("cold") or [])
        never = body.get("unobserved_count") or 0
        if never:
            print("  %d extent group(s) have no access data at all. Nothing has read or"
                  % never)
            print("  written them since the daemons holding them last started, which makes")
            print("  them the coldest thing on the node -- or the least measured.")

    if not answered:
        print()
        print("No node reported access data. Either sidon is older than the tally, or it")
        print("is disabled on these nodes (SIDON_ACCESS_FLUSH=0), in which case nothing is")
        print("counted and tiering has no input to work from.")


def _print_heat_rows(label, rows):
    """One ranked group per line. Rates rather than raw totals, because a total means
    nothing without the window it was counted over and the window is per row."""
    if not rows:
        return
    print("  %s:" % label)
    for row in rows:
        window_ms = row.get("window_ms") or 0
        hours = (window_ms / 3600000.0) if window_ms else 0
        idle_ms = row.get("idle_ms") or 0
        print("    %-40s %6s r %6s w  over %5.1fh  idle %5.1fh  heat %8.1f  %s"
              % (row.get("egroup_id"), row.get("reads"), row.get("writes"), hours,
                 idle_ms / 3600000.0, row.get("heat") or 0.0, row.get("state")))


def _sweep_mib(count):
    return "%.1f MiB" % (int(count or 0) / (1024.0 * 1024.0))


def _grace_phrase(body):
    """The grace period as a duration in words, or "the grace period" when the daemon did not say.

    A daemon older than this field does not report its grace, and then the honest thing is to
    say there is one rather than to guess a number.
    """
    grace = body.get("grace_seconds")
    if not isinstance(grace, (int, float)) or grace <= 0:
        return "the grace period"
    if grace % 60 == 0:
        return "%d minute(s)" % int(grace // 60)
    return "%d second(s)" % int(grace)


def sweep_lines(label, body):
    """What one node's sweep did, as plain lines.

    Every number the daemon reports is shown, because the sweep is deliberately slow and an
    operator who sees "reclaimed 0" needs to be able to tell the three different reasons for
    it apart: nothing is garbage, the garbage has not been seen twice yet, or it was seen
    and something kept it (open, held by an attached vdisk, too young). Fields an older
    daemon does not send are left out rather than shown as zero.
    """
    def n(key):
        value = body.get(key)
        return int(value) if isinstance(value, (int, float)) else 0

    lines = []
    lines.append("%s: %d extent group(s) recorded on this node, %d still referenced"
                 % (label, n("egroups_known"), n("egroups_referenced")))
    reclaimed = body.get("reclaimed") or []
    lines.append("    unreferenced for %s or more: %d candidate(s); reclaimed %d group(s), %s freed here"
                 % (_grace_phrase(body), n("candidates"), len(reclaimed),
                    _sweep_mib(body.get("bytes_reclaimed"))))
    awaiting = n("skipped_awaiting_grace")
    if awaiting:
        lines.append("    awaiting a second scan: %d group(s) first seen unreferenced, or seen too "
                     "recently, to be removed yet" % awaiting)
    skipped = []
    if n("skipped_open"):
        skipped.append("%d open (the target of a drain, or too recent to call abandoned)"
                       % n("skipped_open"))
    if n("skipped_held"):
        skipped.append("%d held by a vdisk attached here" % n("skipped_held"))
    if n("skipped_young"):
        skipped.append("%d too young" % n("skipped_young"))
    if skipped:
        lines.append("    left alone: " + "; ".join(skipped))
    abandoned = body.get("reclaimed_abandoned_open")
    if isinstance(abandoned, list) and abandoned:
        lines.append("    of the reclaimed groups, %d were open and abandoned (no drain owns them)"
                     % len(abandoned))
    for drop in body.get("replica_drops") or []:
        if not isinstance(drop, dict):
            continue
        node = drop.get("node") or "?"
        if drop.get("error"):
            lines.append("    replica %s: could not be asked (%s); its copies are found by its own "
                         "orphan scan" % (node, drop["error"]))
        elif drop.get("unsupported"):
            lines.append("    replica %s: runs a sidon older than replica reclamation and keeps its "
                         "copies until it is upgraded" % node)
        else:
            lines.append("    replica %s: dropped %d copy(ies), %s; already gone %d; refused %d"
                         % (node, int(drop.get("dropped") or 0), _sweep_mib(drop.get("bytes")),
                            int(drop.get("absent") or 0), int(drop.get("refused") or 0)))
            for why in drop.get("refusals") or []:
                lines.append("        refused: %s" % why)
    orphans = body.get("replica_orphans")
    if isinstance(orphans, dict):
        if orphans.get("error"):
            lines.append("    replica copies held for other nodes: scan failed (%s)" % orphans["error"])
        else:
            lines.append("    replica copies held for other nodes: %d scanned; dropped %d orphan(s), "
                         "%s; %d awaiting a second scan"
                         % (int(orphans.get("scanned") or 0), len(orphans.get("dropped") or []),
                            _sweep_mib(orphans.get("bytes_dropped")),
                            int(orphans.get("awaiting_grace") or 0)))
            for anomaly in orphans.get("anomalies") or []:
                lines.append("        ANOMALY: %s" % anomaly)
    for anomaly in body.get("anomalies") or []:
        lines.append("    ANOMALY: %s" % anomaly)
    if n("missing_count"):
        lines.append("    WARNING: %d referenced group(s) have no file on this node's disks: %s"
                     % (n("missing_count"), ", ".join((body.get("missing") or [])[:5])))
    return lines


def cmd_storage_sweep():
    """Run the reclaimer on every node and say what it did and what it is waiting for.

    Space is returned by mark-sweep (I-7): a group goes only after two scans, at least the
    grace period apart, have found nothing pointing at it. So this command may run once, find
    garbage, and reclaim none of it; that is the rule working, and the output says which
    groups are waiting. It does not shorten the rule. Sidon runs the same pass by itself
    every few minutes, so running it here brings the second scan forward and nothing else.
    """
    answered = 0
    awaiting = 0
    grace = None
    for host in _storage_hosts():
        ip = host.get("ip")
        if not ip:
            continue
        label = host.get("hostname") or ip
        rc, body, err = run_mtls_spark_api(ip, "/api/v1/dfs/vdisk", {"op": "purah-sweep"})
        if rc != 0 or not isinstance(body, dict) or "egroups_known" not in body:
            detail = body.get("error") if isinstance(body, dict) else err
            print("[%s] sweep failed: %s" % (label, detail or "no response"))
            continue
        answered += 1
        awaiting += int(body.get("skipped_awaiting_grace") or 0)
        if grace is None:
            grace = body
        for line in sweep_lines(label, body):
            print(line)
    print()
    if not answered:
        print("No node answered. Either sidon is down or the spark API is unreachable.")
        return
    if awaiting:
        print("%d group(s) are awaiting a second scan. A group is removed only when two scans, at "
              "least %s apart, have both found nothing pointing at it: a drain makes bytes durable "
              "before the map names them, so one scan can see a group that is milliseconds from "
              "being live. Run this again after that time; sidon also repeats the pass on its own."
              % (awaiting, _grace_phrase(grace or {})))
    else:
        print("Nothing is waiting for a second scan.")


def scrub_lines(label, body):
    """One node's scrub answer: how many sealed groups were re-hashed, and any damage."""
    lines = ["%s: %d sealed group(s) re-hashed, %d not sealed yet (skipped)"
             % (label, int(body.get("checked") or 0), int(body.get("skipped_unsealed") or 0))]
    for group in body.get("mismatched") or []:
        lines.append("    DAMAGED: %s no longer hashes to what it was sealed as" % group)
    for group in body.get("missing") or []:
        lines.append("    MISSING: %s is recorded sealed here and has no file on any disk" % group)
    if body.get("clean"):
        lines.append("    clean")
    return lines


def cmd_storage_scrub():
    """Re-hash every sealed extent group on every node against the hash taken at seal time.

    Sealed means immutable, so any difference is damage. Exits non-zero if any node reports
    damage or a missing group, so that a script can act on it.
    """
    answered = 0
    damaged = False
    for host in _storage_hosts():
        ip = host.get("ip")
        if not ip:
            continue
        label = host.get("hostname") or ip
        rc, body, err = run_mtls_spark_api(ip, "/api/v1/dfs/vdisk", {"op": "purah-scrub"})
        if rc != 0 or not isinstance(body, dict) or "checked" not in body:
            detail = body.get("error") if isinstance(body, dict) else err
            print("[%s] scrub failed: %s" % (label, detail or "no response"))
            continue
        answered += 1
        if body.get("clean") is not True:
            damaged = True
        for line in scrub_lines(label, body):
            print(line)
    if not answered:
        print("No node answered. Either sidon is down or the spark API is unreachable.")
        sys.exit(1)
    if damaged:
        sys.exit(1)


def cmd_storage_cleanup_orphaned():
    """The name the daily Dagur job and older documents use for the sweep.

    Kept so that the scheduled job and anyone's muscle memory keep working; it is
    `storage.sweep`. This used to glob the container volumes for *.raw and *_vars.fd files and
    match them against hydra.vms, which stopped being the right question when a disk stopped
    being a file named after its VM. An extent group is not named after anything, and the only
    statement of what is referenced is the block map, so this asks Purah rather than working it
    out: a second implementation of the mark phase would be a second thing to get wrong, and
    the consequence is deleting live data.
    """
    cmd_storage_sweep()


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

def default_container():
    """The container a vdisk lands in when nothing names one.

    Read from helios_sidon so the CLI cannot disagree with the console about what the
    default is -- they did, and a vdisk created without an explicit container ended up
    referencing a container that matched no row at all.
    """
    try:
        import helios_sidon
        return helios_sidon.DEFAULT_CONTAINER
    except Exception:
        return "default-pool"


def run_spectrum_api(path, method="GET", payload=None):
    import ssl
    import urllib.error
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
    except urllib.error.HTTPError as e:
        # The body is the useful half: Spectrum refuses with a sentence saying why, and
        # reporting the status line instead throws that sentence away.
        try:
            return -1, json.loads(e.read().decode('utf-8'))
        except Exception:
            return -1, "%s %s" % (e.code, e.reason)
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
    # One cached probe, shared by every daemon -- see helios_zk.leader_ip. Nine
    # copies of this loop on nine timers had the ensemble answering eleven `stat`
    # probes a second forever, and ZooKeeper logs two INFO lines for each one.
    leader_ip = helios_zk.leader_ip(ips)
            
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

LIVE_USAGE = """Usage: valcli vm.live <name> <change>
  vcpus <count>                          Bring more vCPUs online (up to the VM's maximum)
  memory <mib>                           Set the memory balloon (runtime only, within the configured memory)
  cdrom <slot> <image>|eject             Put an image in CD-ROM drive <slot> (0 is the first), or eject it
  nic attach <network_id> [model]        Add a NIC on a VLAN or overlay network
  nic detach                             Remove the last NIC
  nic link <index> up|down               Set a NIC's link state (until the VM next starts)
  disk attach <size_gib> [container]     Create and attach a data disk
  disk resize <index> <size_gib>         Grow a disk and tell the running guest
  disk detach <index> --confirm-delete   Remove the last data disk and DELETE its data
Changes apply to the running VM and to its record. A stopped VM is edited in the console."""


def live_change_from_args(args):
    """The change object `vm.live` sends, from its words. Raises SystemExit with the usage on a
    word it does not understand, so a typo is an error and never a different change."""
    def whole(word):
        try:
            return int(word)
        except (TypeError, ValueError):
            raise SystemExit(LIVE_USAGE)

    words = list(args)
    if not words:
        raise SystemExit(LIVE_USAGE)
    op = words.pop(0)
    if op == "vcpus" and len(words) == 1:
        return {"op": "vcpus", "count": whole(words[0])}
    if op == "memory" and len(words) == 1:
        return {"op": "memory", "mib": whole(words[0])}
    if op == "cdrom" and len(words) == 2:
        return {"op": "cdrom", "slot": whole(words[0]), "image": None if words[1] == "eject" else words[1]}
    if op == "nic" and words:
        action = words.pop(0)
        if action == "attach" and len(words) in (1, 2):
            change = {"op": "nic", "action": "attach", "network_id": words[0]}
            if len(words) == 2:
                change["model"] = words[1]
            return change
        if action == "detach" and not words:
            return {"op": "nic", "action": "detach", "index": None}
        if action == "link" and len(words) == 2 and words[1] in ("up", "down"):
            return {"op": "nic", "action": "link", "index": whole(words[0]), "state": words[1]}
    if op == "disk" and words:
        action = words.pop(0)
        if action == "attach" and len(words) in (1, 2):
            change = {"op": "disk", "action": "attach", "size_gib": whole(words[0])}
            if len(words) == 2:
                change["container"] = words[1]
            return change
        if action == "resize" and len(words) == 2:
            return {"op": "disk", "action": "resize", "index": whole(words[0]), "size_gib": whole(words[1])}
        if action == "detach" and len(words) == 2 and words[1] == "--confirm-delete":
            return {"op": "disk", "action": "detach", "index": whole(words[0]), "confirm_delete": True}
    raise SystemExit(LIVE_USAGE)


def cmd_vm_live(argv):
    if len(argv) < 2:
        raise SystemExit(LIVE_USAGE)
    name, change = argv[0], live_change_from_args(argv[1:])
    if change.get("op") == "nic" and change.get("action") == "detach" and change.get("index") is None:
        change.pop("index")
        change["index"] = -1     # resolved by Vali to the last NIC
    print(f"Applying {change['op']} change to VM '{name}'...")
    status, body, err = run_mtls_spark_api_full(
        "127.0.0.1", "/api/v1/vm/live", {"name": name, "change": change}, timeout=300)
    if status != 200 or not isinstance(body, dict):
        detail = body.get("error") if isinstance(body, dict) and body.get("error") else err
        print("Error: %s" % (detail or "HTTP %s" % status))
        sys.exit(1)
    print("Success: %s" % (body.get("message") or "the VM was changed."))


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
    print("  valcli vm.live <name> <change>     Change a RUNNING VM: vCPUs, CD-ROM, NICs, disks (valcli vm.live for the list)")
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
    print("  valcli storage.container.create <name> [--tier T] [--quota-gb N] [--ftt N] [--compression none|lz4]")
    print("                                     Create a storage container")
    print("  valcli storage.container.update <name> [same options]  Change a container's policy")
    print("  valcli storage.container.delete <name>  Delete a container (refused while in use)")
    print("  valcli storage.benchmark <name>    Run safe read/write performance benchmark")
    print("  valcli storage.cleanup_orphaned    Delete orphaned virtual disk and NVRAM files")
    print("  valcli storage.snapshot <vdisk> <name>  Point-in-time read-only copy of a vdisk")
    print("  valcli storage.clone <vdisk> <name>     Writable copy of a vdisk or snapshot")
    print("  valcli storage.children <vdisk>         Snapshots and clones taken from a vdisk")
    print("  valcli storage.snapshots <vdisk>        Snapshots of a vdisk, who took them, what depends on them")
    print("  valcli storage.snapshot-policy          Scheduled snapshot policies and what each covers")
    print("  valcli storage.snapshot-policy.set cluster|container:<n>|vdisk:<id> --every-hours N --keep N [--disable]")
    print("  valcli storage.snapshot-policy.delete cluster|container:<n>|vdisk:<id>")
    print("  valcli storage.snapshot-run [--dry-run] Take what is due and prune what is not kept (Rauru does this hourly)")
    print("  valcli storage.rollback <vdisk> <snapshot> [--no-keep]  Put a stopped VM's disk back to a snapshot")
    print("  valcli storage.domain                   Protection domains: VMs and vdisks snapshotted together")
    print("  valcli storage.domain.create|delete|add|remove|snapshot|run|sets|restore|recover ...")
    print("                                          (run valcli storage.domain.help for each form)")
    print("  valcli storage.replication              Per vdisk: copies policy asks for, rf it")
    print("                                          asked for, copies it actually has")
    print("  valcli storage.replicate <vdisk>|--all  Add a copy to vdisks short of their rf")
    print("  valcli storage.heat [N]                 Hottest and coldest extent groups per node")
    print("  valcli storage.placement [N]            Which disk of each node holds which extent groups")
    print("  valcli storage.tier [--apply]           Plan (or with --apply, make) disk-to-disk moves")
    print("  valcli storage.move <egroup> <disk> <node>  Move one sealed extent group to another disk")
    print("  valcli storage.takeover <vdisk> <node>  Finish a migration's storage handover on the node the guest runs on (safe to repeat)")
    print("  valcli storage.sweep                    Reclaim unreferenced extent groups on every node; says what is waiting")
    print("  valcli storage.scrub                    Re-hash every sealed extent group against its seal hash")
    print("  valcli storage.compact [--apply]        Plan (or with --apply, make) compaction of sparse sealed groups")
    print("                                          [--threshold F] [--max-groups N] [--max-bytes N] [--seconds N] [--rate B/s]")
    print("  valcli storage.dedup.estimate [--sample F]  Bytes dedup would share beyond clones (read-only)")
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
    elif cmd == "vm.live":
        cmd_vm_live(sys.argv[2:])
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
    elif cmd == "storage.container.create":
        cmd_storage_container_create(sys.argv)
    elif cmd == "storage.container.update":
        cmd_storage_container_update(sys.argv)
    elif cmd == "storage.container.delete":
        cmd_storage_container_delete(sys.argv)
    elif cmd == "storage.benchmark":
        if len(sys.argv) < 3:
            print("Error: Storage container name is required.")
            print("Usage: valcli storage.benchmark <container_name>")
            sys.exit(1)
        cmd_storage_benchmark(sys.argv[2])
    elif cmd == "storage.cleanup_orphaned":
        cmd_storage_cleanup_orphaned()
    elif cmd == "storage.snapshot":
        if len(sys.argv) < 4:
            print("Usage: valcli storage.snapshot <vdisk_id> <snapshot_name>")
            sys.exit(1)
        cmd_storage_derive(sys.argv[2], sys.argv[3], "snapshot")
    elif cmd == "storage.clone":
        if len(sys.argv) < 4:
            print("Usage: valcli storage.clone <vdisk_id> <clone_name>")
            sys.exit(1)
        cmd_storage_derive(sys.argv[2], sys.argv[3], "clone")
    elif cmd == "storage.children":
        if len(sys.argv) < 3:
            print("Usage: valcli storage.children <vdisk_id>")
            sys.exit(1)
        cmd_storage_children(sys.argv[2])
    elif cmd == "storage.snapshots":
        if len(sys.argv) < 3:
            print("Usage: valcli storage.snapshots <vdisk_id>")
            sys.exit(1)
        cmd_storage_snapshots(sys.argv[2])
    elif cmd == "storage.snapshot-policy":
        cmd_storage_snapshot_policy()
    elif cmd == "storage.snapshot-policy.set":
        cmd_storage_snapshot_policy_set(sys.argv)
    elif cmd == "storage.snapshot-policy.delete":
        cmd_storage_snapshot_policy_delete(sys.argv)
    elif cmd == "storage.snapshot-run":
        cmd_storage_snapshot_run(sys.argv)
    elif cmd == "storage.rollback":
        cmd_storage_rollback(sys.argv)
    elif cmd.startswith("storage.domain"):
        cmd_storage_domain(sys.argv)
    elif cmd == "storage.heat":
        limit = 10
        if len(sys.argv) > 2:
            try:
                limit = max(1, min(1000, int(sys.argv[2])))
            except ValueError:
                print("Usage: valcli storage.heat [N]")
                sys.exit(1)
        cmd_storage_heat(limit)
    elif cmd == "storage.placement":
        limit = 10
        if len(sys.argv) > 2:
            try:
                limit = max(1, min(100000, int(sys.argv[2])))
            except ValueError:
                print("Usage: valcli storage.placement [N]")
                sys.exit(1)
        cmd_storage_placement(limit)
    elif cmd == "storage.tier":
        extra = sys.argv[2:]
        if extra not in ([], ["--apply"]):
            print("Usage: valcli storage.tier [--apply]")
            sys.exit(1)
        cmd_storage_tier(apply=bool(extra))
    elif cmd == "storage.sweep":
        if sys.argv[2:]:
            print("Usage: valcli storage.sweep")
            sys.exit(1)
        cmd_storage_sweep()
    elif cmd == "storage.scrub":
        if sys.argv[2:]:
            print("Usage: valcli storage.scrub")
            sys.exit(1)
        cmd_storage_scrub()
    elif cmd == "storage.compact":
        cmd_storage_compact(sys.argv[2:])
    elif cmd == "storage.dedup.estimate":
        cmd_storage_dedup_estimate(sys.argv[2:])
    elif cmd == "storage.takeover":
        if len(sys.argv) < 4:
            print("Usage: valcli storage.takeover <vdisk> <node>")
            sys.exit(1)
        cmd_storage_takeover(sys.argv[2], sys.argv[3])
    elif cmd == "storage.move":
        if len(sys.argv) != 5:
            print("Usage: valcli storage.move <egroup_id> <disk> <node>")
            sys.exit(1)
        cmd_storage_move(sys.argv[2], sys.argv[3], sys.argv[4])
    elif cmd == "storage.replication":
        cmd_storage_replication()
    elif cmd == "storage.replicate":
        if len(sys.argv) < 3:
            print("Usage: valcli storage.replicate <vdisk_id>")
            print("       valcli storage.replicate --all")
            sys.exit(1)
        if sys.argv[2] == "--all":
            cmd_storage_replicate(None, everything=True)
        else:
            cmd_storage_replicate(sys.argv[2])
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
