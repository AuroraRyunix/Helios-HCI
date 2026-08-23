#!/usr/bin/env python3
import sys
import argparse
import json
import shlex
import re
import ssl
import urllib.request
import os
import time
import base64
import threading
import socket

# The cluster's one CQL query layer. Fifteen files carried their own copy of this, most
# of them identical, and the guard against conditional statements had reached only three
# of them -- see helios_cql for what that cost.
from helios_cql import (  # noqa: F401  (re-exported for modules that import from here)
    ConditionalStatementError,
    cql_escape,
    cql_int,
    is_conditional_cql,
    parse_replication_factor,
    run_conditional_cql_query,
    run_cql_query,
)

def run_parallel(ips, cmd):
    results = {}
    threads = []
    def worker(ip):
        rc, stdout, stderr = run_remote_spark(ip, cmd)
        results[ip] = (rc, stdout, stderr)
    for ip in ips:
        t = threading.Thread(target=worker, args=(ip,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    return results


# --- ZooKeeper-backed cluster state -----------------------------------------
#
# Each node's spark-daemon publishes an ephemeral znode under ZK_NODES_PATH. Reading
# that tree gives the whole cluster's state from a single connection, instead of fanning
# mTLS calls out to every host on every invocation -- and because the znodes are
# ephemeral, a dead node's entry is removed by the ensemble rather than inferred from a
# failed probe. Rendering happens here in the CLI, so presentation is not baked into the
# daemon and `--json` is possible.
ZK_NODES_PATH = "/helios/nodes"
ZK_CLUSTER_STATE = "/cluster_state"
NODE_STALE_AFTER = 30      # seconds; a znode older than this is reported as stale

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
GRAY = "\033[90m"
RESET = "\033[0m"

SERVICE_DISPLAY_ORDER = ["ZooKeeper", "HydraDB", "Daruk", "Sidon", "Spark", "Spectrum",
                         "Bifrost", "Dagur", "Mimir", "Vali", "Catalyst", "Hylia",
                         "Gatoway", "Logos", "Mipha", "Agahnim", "Slate", "Urbosa"]


def load_helios_zk():
    """Import the shared ZooKeeper client, or return None if it is not deployed."""
    try:
        import helios_zk
        return helios_zk
    except ImportError:
        pass
    try:
        import importlib.util
        import importlib.machinery
        for candidate in ("/usr/local/bin/helios_zk.py", "/usr/local/bin/helios_zk"):
            if os.path.exists(candidate):
                loader = importlib.machinery.SourceFileLoader("helios_zk", candidate)
                spec = importlib.util.spec_from_loader("helios_zk", loader)
                mod = importlib.util.module_from_spec(spec)
                loader.exec_module(mod)
                return mod
    except Exception:
        pass
    return None


def zk_read_cluster_state():
    """Read (nodes, desired_state) from ZooKeeper. Returns None if ZK is unreachable."""
    zkmod = load_helios_zk()
    if zkmod is None:
        return None
    hosts = ["127.0.0.1"] + [ip for ip in get_cluster_ips() if ip != "127.0.0.1"]
    client = None
    try:
        client = zkmod.connect(hosts, timeout=3.0)
        nodes = {}
        try:
            for name in client.get_children(ZK_NODES_PATH):
                try:
                    nodes[name] = json.loads(client.get(ZK_NODES_PATH + "/" + name).decode("utf-8"))
                except Exception:
                    pass
        except Exception:
            pass  # tree not created yet -- an empty result is still a successful read
        desired = None
        try:
            raw = client.get(ZK_CLUSTER_STATE)
            desired = raw.decode("utf-8", "replace").strip() or None
        except Exception:
            pass
        return {"nodes": nodes, "desired": desired, "via": client.connected_host}
    except Exception:
        return None
    finally:
        if client:
            try:
                client.close()
            except Exception:
                pass


def render_node_block(ip, data, use_color=True):
    """Render one node's services. The CLI owns presentation, not the daemon."""
    g, r, y, b, gr, x = (GREEN, RED, YELLOW, BOLD, GRAY, RESET) if use_color else ("",) * 6
    hostname = data.get("hostname", "")
    leader = ", OdinLeader" if data.get("zk_leader") else ""
    maint = data.get("maintenance_status", "NORMAL")
    maint_str = f" {y}[{maint.replace('_', ' ')}]{x}" if maint != "NORMAL" else ""

    age = int(time.time()) - int(data.get("ts", 0) or 0)
    stale_str = f" {y}[STALE {age}s]{x}" if age > NODE_STALE_AFTER else ""

    lines = [f"\n        Host: {b}{ip}{x} {g}Up{x} {gr}({hostname}){leader}{x}{maint_str}{stale_str}"]
    services = data.get("services", {})
    for name in SERVICE_DISPLAY_ORDER:
        if name not in services:
            continue
        svc = services[name]
        status = svc.get("status", "DOWN")
        pids = svc.get("pids", [])
        restarts = svc.get("restarts", 0)
        pid_str = f"{gr}[{', '.join(map(str, pids))}]{x}" if pids else ""
        if status == "UP":
            note = f" {y}({restarts} restarts){x}" if restarts else ""
            lines.append(f"                    {name:<16}   {g}UP{x}       {pid_str}{note}")
        elif status == "FLAPPING":
            lines.append(f"                    {name:<16}   {y}FLAPPING{x} {gr}restarting, {restarts} restarts{x}")
        else:
            note = f" {gr}({restarts} restarts){x}" if restarts else ""
            lines.append(f"                    {name:<16}   {r}DOWN{x}{note}")
    return "\n".join(lines)


# Services expected to be running on a healthy node once the cluster is started.
# Urbosa is excluded: it is gated behind the urbosa_enabled cluster setting.
EXPECTED_SERVICES = [s for s in SERVICE_DISPLAY_ORDER if s != "Urbosa"]


def wait_for_cluster_convergence(expected_ips, timeout=300, poll=3):
    """Poll ZooKeeper until every node reports every expected service up.

    The cluster reports its own convergence rather than the CLI declaring success the
    moment it has finished issuing start commands. Each node's spark-daemon republishes
    its state every few seconds, so this reflects what actually came up.
    """
    deadline = time.time() + timeout
    last_line = None
    while time.time() < deadline:
        state = zk_read_cluster_state()
        if state is None:
            line = "Waiting for ZooKeeper to become reachable..."
            if line != last_line:
                print(f"  {line}")
                last_line = line
            time.sleep(poll)
            continue

        nodes = state["nodes"]
        missing = [ip for ip in expected_ips if ip not in nodes]
        pending = {}
        for ip in expected_ips:
            data = nodes.get(ip)
            if not data:
                continue
            services = data.get("services", {})
            not_up = [n for n in EXPECTED_SERVICES
                      if n in services and services[n].get("status") != "UP"]
            if not_up:
                pending[ip] = not_up

        if not missing and not pending:
            elapsed = int(timeout - (deadline - time.time()))
            print(f"  {GREEN}All nodes converged.{RESET}")
            return True

        parts = []
        if missing:
            parts.append("nodes not reporting: " + ", ".join(sorted(missing)))
        for ip in sorted(pending):
            shown = pending[ip][:6]
            more = f" (+{len(pending[ip]) - len(shown)} more)" if len(pending[ip]) > len(shown) else ""
            parts.append(f"{ip}: {', '.join(shown)}{more}")
        line = "Waiting for " + "; ".join(parts)
        if line != last_line:
            print(f"  {line}")
            last_line = line
        time.sleep(poll)

    print(f"  {YELLOW}Timed out after {timeout}s waiting for convergence.{RESET}")
    return False


def get_cluster_ips():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            return [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        return ["127.0.0.1"]


# Control-socket one-liners, defined once.
#
# Every one of these is a shell command carrying a JSON document with quotes and a
# trailing newline, run through spark's remote-exec. Building them inline at each call
# site is how a quote gets lost in the wrong layer of escaping and the command silently
# becomes a different one.
SIDON_SOCKET = "/run/sidon/control.sock"


def sidon_cmd(payload):
    """A shell command that sends one control request and prints the reply."""
    return "printf %s | nc -U %s" % (
        shlex.quote(json.dumps(payload) + "\n"), SIDON_SOCKET)


def sidon_detach_cmd(vdisk_id):
    return sidon_cmd({"op": "detach", "vdisk_id": vdisk_id})


SIDON_CAPACITY_CMD = sidon_cmd({"op": "capacity"})
SIDON_PEERS_CMD = sidon_cmd({"op": "peers"})
SIDON_LIST_CMD = sidon_cmd({"op": "list"})


def get_dfs_engine():
    """Which storage engine a new cluster is built with.

    Was hardcoded and called by nothing -- a vestige of the GlusterFS transition. It reads
    the file when there is one, and defaults to sidon rather than linstor: this decides
    what a *new* cluster gets, and there is no longer a LINSTOR to build one on.
    """
    try:
        with open("/etc/hci/cluster.json", "r") as handle:
            value = str(json.load(handle).get("dfs_engine") or "").strip().lower()
    except Exception:
        return "sidon"
    return value if value in ("linstor", "sidon") else "sidon"


def run_remote_spark(ip, command):
    cert_paths = [
        ("C:/Users/AuraFlight/.hci_temp_certs/ca.crt", "C:/Users/AuraFlight/.hci_temp_certs/client.crt", "C:/Users/AuraFlight/.hci_temp_certs/client.key"),
        ("/root/.certs/ca.crt", "/root/.certs/client.crt", "/root/.certs/client.key")
    ]
    ca_path, cert_path, key_path = None, None, None
    for ca, cert, key in cert_paths:
        if os.path.exists(ca) and os.path.exists(cert) and os.path.exists(key):
            ca_path, cert_path, key_path = ca, cert, key
            break
            
    context = ssl._create_unverified_context()
    if cert_path and key_path:
        context.load_cert_chain(certfile=cert_path, keyfile=key_path)
    
    url = f"https://{ip}:9099/api/v1/execute"
    data = json.dumps({"command": command}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=120) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)


class UdevHelper:
    def __init__(self, ips):
        self.ips = ips
        self.stop_event = threading.Event()
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        if self.thread:
            self.stop_event.set()
            self.thread.join(timeout=5)

    def _run(self):
        while not self.stop_event.is_set():
            for ip in self.ips:
                try:
                    run_remote_spark(ip, "vgscan --mknodes && udevadm trigger")
                except Exception:
                    pass
            # Wait up to 2 seconds, checking the stop_event frequently
            for _ in range(20):
                if self.stop_event.is_set():
                    break
                time.sleep(0.1)


def acquire_cluster_lock(ips):
    print("Acquiring cluster operation lock on all nodes...")
    lock_cmd = "mkdir -p /run/hci && touch /run/hci/cluster_operation.lock"
    run_parallel(ips, lock_cmd)


def release_cluster_lock(ips):
    print("Releasing cluster operation lock on all nodes...")
    unlock_cmd = "rm -f /run/hci/cluster_operation.lock"
    run_parallel(ips, unlock_cmd)



def get_scylla_bootstrap_progress(ip):
    # Fetch recent logs from journalctl related to bootstrap/repair
    cmd = "journalctl -u hydra-db -n 50 | grep -E 'repair|bootstrap|compaction_manager|serving|NORMAL mode' | tail -n 1"
    rc, out, _ = run_remote_spark(ip, cmd)
    if rc == 0 and out.strip():
        line = out.strip()
        if "systemd-hydra-db" in line:
            parts = line.split("systemd-hydra-db", 1)[1]
            if ":" in parts:
                msg = parts.split(":", 1)[1].strip()
                if "]" in msg:
                    msg = msg.split("]", 1)[1].strip()
                return msg
        return line
    return None

def run_checked_cmd(ip, command, allow_already_exists=False):
    print(f"[{ip}] Running command: {command}")
    rc, stdout, stderr = run_remote_spark(ip, command)
    stdout = stdout.strip() if stdout else ""
    stderr = stderr.strip() if stderr else ""
    if stdout:
        print(f"[{ip}] stdout:\n{stdout}")
    if stderr:
        print(f"[{ip}] stderr:\n{stderr}")
    if rc != 0:
        harmless = False
        if allow_already_exists:
            combined = (stdout + "\n" + stderr).lower()
            if any(msg in combined for msg in [
                "already exists",
                "already defined",
                "already created",
                "already registered",
                "already configured",
                "is already",
                "already has"
            ]):
                harmless = True
        if not harmless:
            print(f"[ERROR] Command failed on {ip} with exit code {rc}. Command: {command}")
            sys.exit(1)
    return rc, stdout, stderr

def run_parallel_checked(ips, command, allow_already_exists=False):
    print(f"Running parallel command on {ips}: {command}")
    results = run_parallel(ips, command)
    for ip, (rc, stdout, stderr) in results.items():
        stdout = stdout.strip() if stdout else ""
        stderr = stderr.strip() if stderr else ""
        if stdout:
            print(f"[{ip}] stdout:\n{stdout}")
        if stderr:
            print(f"[{ip}] stderr:\n{stderr}")
        if rc != 0:
            harmless = False
            if allow_already_exists:
                combined = (stdout + "\n" + stderr).lower()
                if any(msg in combined for msg in [
                    "already exists",
                    "already defined",
                    "already created",
                    "already registered",
                    "already configured",
                    "is already",
                    "already has"
                ]):
                    harmless = True
            if not harmless:
                print(f"[ERROR] Parallel command failed on {ip} with exit code {rc}. Command: {command}")
                sys.exit(1)
    return results

# --- the ScyllaDB ring ------------------------------------------------------------------
#
# hydra.nodes and the ring are two different memberships and they are not kept in step.
# A host marked DOWN leaves the VM scheduler immediately; its ScyllaDB stays a ring
# member holding token ranges, and every QUORUM operation keeps counting it. Nothing in
# Helios ever reconciled the two, so a node that was replaced months ago could still be
# the reason a maintenance request is refused, with nothing on any screen saying so.
#
# These helpers are the read side. `cluster ring`, `cluster decommission` and
# `cluster rejoin` are built on them; see docs/ring_lifecycle.md for the sequences.


_HOST_ID_RE = re.compile(
    r"\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\Z")
_LOAD_UNITS = ("bytes", "KB", "MB", "GB", "TB", "KiB", "MiB", "GiB", "TiB")


def parse_nodetool_status(text):
    """Ring members from `nodetool status`, as {address, status, state, host_id}.

    The first column is two characters: U/D for up or down, then N/L/J/M for normal,
    leaving, joining or moving. Only a member that is both up and normal is a replica
    that can answer a query -- `UJ` has not finished streaming in, `UL` is streaming out.

    The host id is found by shape rather than by column index. `Load` is printed as
    "2.38 MB", two whitespace-separated fields, and as a bare "?" when it is unknown, so
    every column after it shifts depending on the node. Indexing positionally returned
    the `Owns` column -- a literal "?" -- as the host id, which is the argument
    `nodetool removenode` needs and the one thing a decommission plan cannot get wrong.
    """
    members = []
    for line in (text or "").splitlines():
        fields = line.split()
        if len(fields) < 2:
            continue
        marker = fields[0]
        if len(marker) != 2 or marker[0] not in "UD" or marker[1] not in "NLJM":
            continue
        host_id = next((f for f in fields[2:] if _HOST_ID_RE.match(f)), "")
        if len(fields) > 3 and fields[3] in _LOAD_UNITS:
            load = fields[2] + " " + fields[3]
        else:
            load = fields[2] if len(fields) > 2 else ""
        members.append({
            "address": fields[1],
            "status": marker[0],
            "state": marker[1],
            "available": marker == "UN",
            "load": load,
            "host_id": host_id,
        })
    return members




def get_hydra_replication_factor():
    """RF as the database reports it, or None. Never a plausible-looking guess: assuming
    3 on a cluster actually running RF=1 would wave through the removal that takes the
    only copy of the metadata with it."""
    rc, stdout, _ = run_cql_query(
        "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'hydra';")
    if rc != 0:
        return None
    return parse_replication_factor(stdout)


def zookeeper_quadlet(node_id, ips):
    """One node's ZooKeeper unit for an ensemble of `ips`.

    Every node's unit names the whole ensemble, so adding a member means rewriting all of
    them. ZooKeeper has supported dynamic reconfiguration since 3.5 and the deployed
    version is 3.9.2, but `reconfigEnabled` is off, so `reconfig` is refused and the
    ensemble can only be changed by rewriting the config and restarting. That is the cost
    being paid here; see the dynamic-reconfiguration item in TODO.md.

    Nodes beyond the third are observers: they scale reads without joining the quorum, so
    a five-node cluster still needs two failures to lose consensus rather than three.
    """
    if len(ips) == 1:
        servers_env = ""
    else:
        parts = []
        for i, ip in enumerate(ips, start=1):
            suffix = ":observer" if i > 3 else ""
            parts.append("server.%d=%s:2888:3888%s;2181" % (i, ip, suffix))
        servers_env = ' ZOO_SERVERS="%s"' % " ".join(parts)
    peer_type_env = " ZOO_PEER_TYPE=observer" if node_id > 3 else ""
    return (
        "[Unit]\n"
        "Description=ZooKeeper Cluster Consensus Service\n"
        "After=network.target\n\n"
        "[Service]\n"
        "Restart=always\n"
        "CPUWeight=100\n"
        "MemoryMax=512M\n"
        "MemoryHigh=400M\n\n"
        "[Container]\n"
        "Image=docker.io/library/zookeeper:3.9.2\n"
        "Network=host\n"
        "Volume=/var/lib/hci/zookeeper/data:/data:Z\n"
        "Volume=/var/lib/hci/zookeeper/log:/datalog:Z\n"
        "Environment=ZOO_MY_ID=%d%s%s ZOO_4LW_COMMANDS_WHITELIST=*\n"
        % (node_id, servers_env, peer_type_env)
    )


def write_zookeeper_ensemble(ips):
    """Rewrite every node's ZooKeeper unit for this membership. Returns the nodes that failed."""
    failed = []
    for idx, ip in enumerate(ips):
        quad = zookeeper_quadlet(idx + 1, ips)
        encoded = base64.b64encode(quad.encode()).decode()
        rc, _, _ = run_remote_spark(
            ip,
            "mkdir -p /etc/containers/systemd && echo %s | base64 -d "
            "> /etc/containers/systemd/zookeeper.container && systemctl daemon-reload"
            % encoded)
        if rc != 0:
            failed.append(ip)
    return failed


def hydra_db_quadlet(node_ip, seed_ips):
    """The ScyllaDB unit for one node, seeded by the cluster it is joining.

    The seeds are the whole point of writing this again on a joining node. `provision.py
    --join` seeds a node from the list it was provisioned with, which for a joining node is
    only the other joiners -- so it would bootstrap into a ring of its own rather than into
    the cluster. A node that has formed its own ring cannot simply be pointed at another
    one afterwards.
    """
    return (
        "[Unit]\n"
        "Description=Hydra Metadata Database (ScyllaDB)\n"
        "After=zookeeper.service\n\n"
        "[Service]\n"
        "Restart=always\n"
        "CPUWeight=100\n"
        "MemoryMax=2.5G\n"
        "MemoryHigh=2.2G\n\n"
        "[Container]\n"
        "Image=docker.io/scylladb/scylla:5.4.0\n"
        "Network=host\n"
        "Volume=/var/lib/hci/hydra/data:/var/lib/scylla:Z\n"
        "Volume=/etc/hci/hydra/cassandra-rackdc.properties:"
        "/etc/scylla/cassandra-rackdc.properties:ro\n"
        "Exec=--listen-address %s --broadcast-address %s --broadcast-rpc-address %s "
        "--seeds %s --cluster-name hci-metadata --rpc-address %s --num-tokens 256 "
        "--overprovisioned 1 --endpoint-snitch GossipingPropertyFileSnitch\n"
        % (node_ip, node_ip, node_ip, ",".join(seed_ips), node_ip)
    )


def wait_for_ring_member(ips, target, attempts=60, delay=5):
    """Wait until `target` is up and normal in the ring. Returns (ok, last_state).

    Bootstrapping streams data, so this is minutes rather than seconds on a cluster with
    anything in it. A node sitting at `UJ` is still joining, which is not a failure and
    must not be reported as one.
    """
    last = "not in the ring"
    for _ in range(attempts):
        members, error = read_ring(ips)
        if not error:
            member = next((m for m in members if m["address"] == target), None)
            if member is not None:
                last = "%s%s" % (member["status"], member["state"])
                if member["available"] and member["state"] == "N":
                    return True, last
        time.sleep(delay)
    return False, last


def cmd_add_node(args):
    """Bring a provisioned, enrolled machine into this cluster.

    Deliberately not `cluster create` with one more address. That path claims disks --
    `wipefs -a` on anything it decides is unclaimed -- which is right when building a
    cluster and catastrophic when run against nodes already serving guests. Adding is a
    different operation from creating and gets its own command.

    The order matters and is not arbitrary:

      0. **Identity before everything.** A node reads its own address out of
         `spectrum.env`, and without it every daemon on it believes it is 127.0.0.1.
         That is not a local problem: the node cannot recognise itself as the ZooKeeper
         leader, and the leader is the only node that drains the Catalyst queue, so
         leadership landing there stops VM power tasks for the whole cluster.
      1. **Membership before consensus.** Every node's `cluster.json` learns the new
         address first, so anything that reads the host list while the ensemble is
         restarting sees the intended membership rather than a half-written one.
      2. **Consensus before storage.** ZooKeeper is what the cluster coordinates through;
         a node whose ScyllaDB is up but which is not in the ensemble is a node the rest
         cannot agree about.
      3. **Storage before scheduling.** The node is registered in `hydra.nodes` only once
         the ring reports it `UN`. Registering it while it is still bootstrapping hands it
         VMs it cannot run -- the same rule `rejoin` follows, for the same reason.
    """
    target = args.node.strip()
    existing = get_cluster_ips()
    if not existing:
        print("[ERROR] /etc/hci/cluster.json lists no hosts, so there is no cluster to "
              "join. Use 'cluster create' to form one.")
        return 1
    # Resumable on purpose. The steps below are ordered so that membership is written
    # before the ring join, which means a join that fails part-way leaves the node in
    # cluster.json and out of the ring -- the exact state re-running has to be able to
    # finish. Refusing here because the name is already in the config would make the first
    # failure unrecoverable by the tool that caused it.
    resuming = target in existing
    if resuming:
        members, _ = read_ring([ip for ip in existing if ip != target] or existing)
        live = next((m for m in members if m["address"] == target and m["available"]), None)
        if live is not None:
            print("[ERROR] %s is already a live member of this cluster." % target)
            return 1
        print("[NOTE] %s is already in cluster.json but not serving in the ring; "
              "resuming the join." % target)
        existing = [ip for ip in existing if ip != target]

    print("==========================================================")
    print("   Adding %s to a %d-node cluster" % (target, len(existing)))
    print("==========================================================")

    # Preflight. Each of these is a way the join fails silently later.
    rc, out, err = run_remote_spark(target, "echo online")
    if rc != 0 or "online" not in (out or "").lower():
        print("[ERROR] spark-daemon on %s is not answering over mTLS: %s"
              % (target, (err or out or "").strip()[:200]))
        print("[ERROR] Provision it with 'provision.py --join' and enrol it with "
              "'impa enroll --node %s' first. A node the cluster cannot authenticate "
              "cannot be added to it." % target)
        return 1
    print("[%s] spark-daemon answers over mTLS, so it is provisioned and enrolled." % target)

    rc_d, stdout_d, _ = run_remote_spark(
        target, "ls -A /var/lib/hci/hydra/data/data 2>/dev/null | head -5")
    if rc_d == 0 and (stdout_d or "").strip():
        print("[ERROR] %s already carries ScyllaDB data under /var/lib/hci/hydra/data." % target)
        print("[ERROR] A node bootstrapping on top of existing sstables either refuses to "
              "start or re-introduces rows deleted while it was away. Wipe that directory, "
              "or use 'cluster rejoin' if this node was previously a member.")
        return 1

    members, error = read_ring(existing)
    if error:
        print("[ERROR] Could not read the ring: %s" % error)
        return 1
    if any(not m["available"] for m in members):
        print("[ERROR] Not every existing node is up in the ring. Adding a node while the "
              "cluster is already degraded compounds two problems.")
        print(render_ring(members, get_hydra_replication_factor()))
        return 1
    print("[ring] all %d existing node(s) are up." % len(members))

    ips = existing + [target]

    # 1. Identity, before membership. Eleven modules and the Phoenix console read
    # LOCAL_HYPERVISOR_IP out of spectrum.env and fall back to 127.0.0.1 without it, and
    # a node that cannot recognise itself as the ZooKeeper leader never drains the
    # Catalyst queue -- so leadership landing on it stops VM power tasks cluster-wide,
    # not just on that node. `provision.py` writes this, but a node provisioned by an
    # older toolkit got the version that carried no address at all; writing it here is
    # idempotent and makes the join self-sufficient.
    env_b64 = base64.b64encode(
        ("LOCAL_HYPERVISOR_IP=%s\n" % target).encode("utf-8")).decode("utf-8")
    rc_e, _, err_e = run_remote_spark(
        target, "mkdir -p /etc/hci/spectrum && echo %s | base64 -d "
                "> /etc/hci/spectrum/spectrum.env" % env_b64)
    if rc_e != 0:
        print("[ERROR] Could not write /etc/hci/spectrum/spectrum.env on %s: %s"
              % (target, (err_e or "").strip()[:200]))
        return 1
    print("[config] %s knows its own address." % target)

    # 2. Membership.
    config = cluster_hosts_config()
    if config is None:
        print("[ERROR] /etc/hci/cluster.json could not be read.")
        return 1
    rc_h, hostname, _ = run_remote_spark(target, "hostname")
    hostname = (hostname or "").strip()
    if rc_h != 0 or not hostname:
        print("[ERROR] Could not resolve the hostname of %s." % target)
        return 1
    hosts = list(config.get("hosts", []))
    # Idempotent, because this runs again on a resumed join. Appending unconditionally
    # would put the node in cluster.json twice, and every reader that counts hosts -- the
    # replication factor, the quorum gate, the console -- would believe in a node that
    # does not exist.
    if not any(h.get("ip") == target for h in hosts):
        hosts.append({"node_id": len(hosts) + 1, "ip": target, "hostname": hostname})
    config["hosts"] = hosts
    failed = write_cluster_config(ips, config)
    if failed:
        print("[ERROR] Could not write /etc/hci/cluster.json on: %s" % ", ".join(failed))
        return 1
    print("[config] %s (%s) is in cluster.json on all %d node(s)." % (target, hostname, len(ips)))

    # 3. Consensus.
    print("[zookeeper] rewriting the ensemble for %d member(s)..." % len(ips))
    failed = write_zookeeper_ensemble(ips)
    if failed:
        print("[ERROR] Could not write the ZooKeeper unit on: %s" % ", ".join(failed))
        return 1
    # Restarted one at a time, oldest first: a rolling restart keeps a quorum of the
    # *previous* ensemble alive throughout, which an all-at-once restart does not.
    for ip in ips:
        rc, _, err = run_remote_spark(ip, "systemctl restart zookeeper")
        if rc != 0:
            print("[ERROR] [%s] ZooKeeper did not restart: %s" % (ip, (err or "").strip()[:200]))
            return 1
        print("[zookeeper] %s restarted." % ip)
        time.sleep(3)

    # 4. Storage.
    print("[hydra-db] seeding %s from the existing cluster..." % target)
    rc, _, err = run_remote_spark(
        target,
        "mkdir -p /var/lib/hci/hydra/data /etc/hci/hydra && "
        "cp -f /usr/local/bin/daruk.py /var/lib/hci/hydra/data/daruk.py && "
        "chmod 644 /var/lib/hci/hydra/data/daruk.py && "
        "if [ ! -f /etc/hci/hydra/cassandra-rackdc.properties ]; then "
        "printf 'dc=datacenter1\\nrack=rack1\\nprefer_local=true\\n' "
        "> /etc/hci/hydra/cassandra-rackdc.properties; fi")
    if rc != 0:
        print("[ERROR] [%s] could not prepare the database directories: %s"
              % (target, (err or "").strip()[:200]))
        return 1

    quad = hydra_db_quadlet(target, existing)
    encoded = base64.b64encode(quad.encode()).decode()
    rc, _, err = run_remote_spark(
        target,
        "echo %s | base64 -d > /etc/containers/systemd/hydra-db.container && "
        "systemctl daemon-reload && systemctl start hydra-db" % encoded)
    if rc != 0:
        print("[ERROR] [%s] hydra-db did not start: %s" % (target, (err or "").strip()[:200]))
        return 1
    print("[hydra-db] started; bootstrapping from %s." % ", ".join(existing))

    ok, state = wait_for_ring_member(ips, target)
    members, error = read_ring(ips)
    if not error:
        print()
        print(render_ring(members, get_hydra_replication_factor()))
    if not ok:
        print("[ERROR] %s did not reach UN in the ring (last seen '%s')." % (target, state))
        print("[ERROR] It may still be streaming. Watch 'cluster ring'; once it is UN, "
              "re-run this command to finish the bookkeeping.")
        return 1
    print("[ring] %s is UN." % target)

    # 5. Scheduling, only now.
    run_cql_query(
        "INSERT INTO hydra.nodes (hostname, ip, status, maintenance_mode) "
        "VALUES ('%s', '%s', 'NORMAL', false);" % (hostname, target))
    print("[hydra] registered %s (%s) as a schedulable host." % (hostname, target))

    print()
    print("Still to do, and deliberately not automatic:")
    print("  - Raise the keyspace replication factor now that there are %d nodes:" % len(ips))
    print("      ALTER KEYSPACE hydra WITH replication = "
          "{'class': 'NetworkTopologyStrategy', '<datacenter>': %d};" % min(3, len(ips)))
    print("    then 'nodetool repair -pr hydra' on every node. ALTER changes the strategy")
    print("    only; the data is not on the new replicas until a repair has run, and until")
    print("    then the cluster reports a redundancy it does not have.")
    print("  - Storage needs nothing: Purah places replicas onto the new node as vdisks")
    print("    come to need them.")
    return 0


def read_ring(ips):
    """Read the ring from whichever node will answer. Returns (members, error).

    Any member's `nodetool status` describes the whole ring, so this tries each node in
    turn -- the one that cannot answer is frequently the one being asked about.
    """
    errors = []
    for ip in ips:
        rc, stdout, stderr = run_remote_spark(ip, "nodetool status")
        if rc == 0:
            members = parse_nodetool_status(stdout)
            if members:
                return members, ""
            errors.append(f"{ip}: nodetool status returned no ring members")
        else:
            errors.append(f"{ip}: {(stderr or stdout or 'unreachable').strip()[:120]}")
    return [], "; ".join(errors)


def quorum_of(replication_factor):
    """What Scylla demands at ConsistencyLevel.QUORUM: a strict majority of RF."""
    return replication_factor // 2 + 1


def render_ring(members, replication_factor):
    lines = []
    if replication_factor:
        lines.append(f"  hydra replication factor: {replication_factor} "
                     f"(QUORUM needs {quorum_of(replication_factor)} replicas)")
    else:
        lines.append(f"  hydra replication factor: {YELLOW}unknown{RESET}")
    up = sum(1 for m in members if m["available"])
    lines.append(f"  ring members: {up} of {len(members)} up and normal")
    for m in members:
        marker = f"{m['status']}{m['state']}"
        colour = GREEN if m["available"] else RED
        lines.append(f"    {colour}{marker}{RESET}  {m['address']:<16} {m['load']:<12} {GRAY}{m['host_id']}{RESET}")
    return "\n".join(lines)


def cluster_hosts_config():
    """The parsed /etc/hci/cluster.json, or None."""
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            return json.load(f)
    except Exception:
        return None


def write_cluster_config(ips, config):
    """Push a cluster.json to every listed node. Returns the list of nodes that failed."""
    payload = base64.b64encode(json.dumps(config, indent=4).encode("utf-8")).decode("utf-8")
    command = f"mkdir -p /etc/hci && echo {payload} | base64 -d > /etc/hci/cluster.json"
    failed = []
    for ip, (rc, _out, _err) in run_parallel(ips, command).items():
        if rc != 0:
            failed.append(ip)
    return failed


def check_urbosa_enabled():
    rc, stdout, _ = run_cql_query("SELECT value FROM hydra.cluster_settings WHERE key = 'urbosa_enabled';")
    if rc == 0 and stdout:
        for line in stdout.splitlines():
            if "true" in line.lower():
                return True
    return False

_SPARK_LOCAL_IP = None

def spark_local_ip():
    """This node's address as its own certificate names it.

    spectrum.env is what provision.py wrote; the UDP-connect trick is the fallback the
    rest of this file already uses. Only a non-loopback answer is cached.
    """
    global _SPARK_LOCAL_IP
    if _SPARK_LOCAL_IP:
        return _SPARK_LOCAL_IP
    resolved = "127.0.0.1"
    try:
        with open("/etc/hci/spectrum/spectrum.env", "r") as f:
            for line in f:
                if line.startswith("LOCAL_HYPERVISOR_IP="):
                    value = line.strip().split("=", 1)[1].strip()
                    if value:
                        resolved = value
    except Exception:
        pass
    if resolved == "127.0.0.1":
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("10.255.255.255", 1))
            resolved = s.getsockname()[0]
            s.close()
        except Exception:
            pass
    if resolved not in ("127.0.0.1", "::1", "localhost"):
        _SPARK_LOCAL_IP = resolved
    return resolved

def spark_endpoint(ip):
    """Return (address, verify_identity) for an mTLS call to a spark-daemon.

    Node certificates carry `subjectAltName = IP:<node ip>` and nothing else, so a
    connection can only be tied to the node answering it when it is addressed by that
    same IP. Loopback is in no node's SAN; spark-daemon binds 0.0.0.0:9099, so this
    node's own address reaches the same listener and does verify.
    """
    if ip in ("127.0.0.1", "::1", "localhost"):
        local = spark_local_ip()
        if local not in ("127.0.0.1", "::1", "localhost"):
            return local, True
        return ip, False
    return ip, True

class ClusterPeerSSLContext(ssl.SSLContext):
    """mTLS context for the VIP, which no certificate is issued for.

    The VIP floats -- it is answered by whichever node currently holds it -- so there is
    no single address to hand check_hostname. Verifying the chain alone is what let any
    certificate the cluster CA ever signed stand in for any node, so rather than drop the
    identity check entirely this requires the peer's IP SAN to name a host that is in
    cluster.json. That still refuses the shared client certificate, which carries no SAN
    at all and sits on every node, being used to answer on the VIP.

    Adding the VIP to every node certificate's SAN would let this become an ordinary
    check_hostname check; see docs/mtls_lifecycle.md.
    """

    cluster_ips = frozenset()

    def wrap_socket(self, sock, *args, **kwargs):
        wrapped = super().wrap_socket(sock, *args, **kwargs)
        try:
            san = (wrapped.getpeercert() or {}).get("subjectAltName", ())
            peer_ips = set(value for kind, value in san if kind == "IP Address")
            if not peer_ips & self.cluster_ips:
                raise ssl.SSLCertVerificationError(
                    "the VIP is answered by a certificate for %s, which is not a configured "
                    "cluster node" % (", ".join(sorted(peer_ips)) or "no IP address"))
        except BaseException:
            wrapped.close()
            raise
        return wrapped

def make_request(path, method="GET", payload=None):
    # Try VIP if configured
    vip = None
    cluster_ips = []
    try:
        if os.path.exists("/etc/hci/cluster.json"):
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                vip = cdata.get("vip")
                cluster_ips = [h["ip"] for h in cdata.get("hosts", []) if h.get("ip")]
    except Exception:
        pass

    target_ips = []
    if vip:
        target_ips.append(vip)
    target_ips.append("127.0.0.1")

    last_err = ""
    for ip in target_ips:
        if vip and ip == vip:
            context = ClusterPeerSSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(cafile="/root/.certs/ca.crt")
            context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
            context.cluster_ips = frozenset(cluster_ips)
        else:
            ip, verify_identity = spark_endpoint(ip)
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
            context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
            context.check_hostname = verify_identity

        url = f"https://{ip}:9099{path}"
        data = None
        if payload is not None:
            data = json.dumps(payload).encode('utf-8')
            
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            # Short timeout for checking VIP, longer for orchestration
            timeout = 15 if "status" in path else 130
            with urllib.request.urlopen(req, context=context, timeout=timeout) as response:
                return 0, json.loads(response.read().decode('utf-8'))
        except Exception as e:
            last_err = str(e)
            
    return -1, {"error": f"Failed to connect to spark-daemon (tried {', '.join(target_ips)}): {last_err}"}

def main():
    parser = argparse.ArgumentParser(description="HCI Cluster Management Utility")
    parser.add_argument("-s", "--servers", required=False, help="Comma-separated list of host IPs")
    parser.add_argument("-r", "--redundancy_factor", type=int, default=None, help="Fault Tolerance to Tolerate (FTT) / Redundancy Factor (e.g. 0, 1, or 2)")
    parser.add_argument("-v", "--vip", required=False, help="Floating Cluster Virtual IP (VIP)")
    parser.add_argument("--verbose", action="store_true", help="Print verbose status information")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable status (ZooKeeper-backed path only)")
    parser.add_argument("--node", required=False,
                        help="Single host IP, for 'decommission', 'rejoin' and 'add-node'")
    parser.add_argument("--finalize", action="store_true", help="Perform the bookkeeping half of a decommission or rejoin, once the ring work is done")
    parser.add_argument("command", choices=["create", "status", "start", "stop", "destroy",
                                            "ring", "decommission", "rejoin",
                                            "add-node"], help="Action to perform")

    args = parser.parse_args()

    if args.command == "add-node":
        if not args.node:
            parser.error("add-node requires --node <ip>")
        sys.exit(cmd_add_node(args))

    if args.command == "create":
        # Ensure we have servers
        config_ips = []
        try:
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                config_ips = [h["ip"] for h in cdata.get("hosts", [])]
        except Exception:
            pass

        if args.servers:
            ips = [ip.strip() for ip in args.servers.split(",") if ip.strip()]
        elif config_ips:
            ips = config_ips
        else:
            parser.error("the following arguments are required: -s/--servers (or a valid /etc/hci/cluster.json config)")

        rf = args.redundancy_factor if args.redundancy_factor is not None else 1
        if len(ips) == 1:
            if rf > 0:
                print(f"[WARNING] Single-node cluster detected. Forcing redundancy factor (FTT) from {rf} to 0 (no replication).")
            rf = 0
        vip = args.vip if args.vip else ""

        acquire_cluster_lock(ips)
        import atexit
        atexit.register(release_cluster_lock, ips)


        print("==========================================================")
        print(f"   Creating HCI Cluster (Redundancy Factor/FTT={rf})  ")
        print("==========================================================")

        # 1. Connectivity & Pre-checks
        print("\n--- Phase 1: Connectivity & Pre-checks ---")
        for ip in ips:
            print(f"[{ip}] Testing connectivity...")
            rc, stdout, stderr = run_remote_spark(ip, "echo 'online'")
            if rc != 0 or "online" not in stdout.lower():
                print(f"[ERROR] Could not connect to spark-daemon on {ip}: {stderr}")
                sys.exit(1)
            print(f"[{ip}] spark-daemon is online.")
            
            # Check port conflicts
            print(f"[{ip}] Checking port conflicts...")
            rc, stdout, _ = run_remote_spark(ip, "ss -tlnp")
            if rc == 0:
                for port in ["7000", "3370"]:
                    if port in stdout:
                        print(f"[WARNING] Port {port} is already in use on {ip}. This may cause conflicts.")

            # Validate Secure Boot and ELRepo module signing key
            rc_sb, sb_out, _ = run_remote_spark(ip, "mokutil --is-sb-enabled")
            if rc_sb == 0 and "secureboot enabled" in sb_out.lower():
                rc_key, _, _ = run_remote_spark(ip, "mokutil --test-key /etc/pki/elrepo/SECURE-BOOT-KEY-elrepo.org.der")
                if rc_key != 0:
                    print(f"[ERROR] Secure Boot is enabled on host {ip} and the ELRepo Secure Boot key is not enrolled.")
                    print(f"[ERROR] Unsigned out-of-tree kernel modules will fail to load under Secure Boot.")
                    print(f"[ERROR] Please disable Secure Boot in the UEFI/BIOS settings of {ip}, or import the key ('mokutil --import /etc/pki/elrepo/SECURE-BOOT-KEY-elrepo.org.der') and reboot to enroll it.")
                    sys.exit(1)

        # Ensure any running core services are stopped to prevent them interfering with boot
        print("Ensuring any running cluster services are stopped for a clean bootstrap...")
        cleanup_services = ["hylia", "logos", "mipha", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "sidon", "daruk", "hydra-db", "zookeeper"]
        run_parallel(ips, f"systemctl stop {' '.join(cleanup_services)} || true")

        # 2. Hostname Resolution & Cluster JSON Config
        print("\n--- Phase 2: Hostname Resolution & Cluster Setup ---")
        hosts_info = []
        for idx, ip in enumerate(ips):
            print(f"[{ip}] Resolving hostname...")
            rc, hostname, _ = run_remote_spark(ip, "hostname")
            hostname = hostname.strip() if rc == 0 else f"node-{idx+1}"
            print(f"[{ip}] Resolved hostname: {hostname}")
            hosts_info.append({
                "node_id": idx + 1,
                "ip": ip,
                "hostname": hostname
            })

        cluster_json_data = {
            "cluster_name": "hci-01",
            "redundancy_factor": rf,
            "dfs_engine": "sidon",
            "vip": vip,
            "hosts": hosts_info
        }
        
        json_b64 = base64.b64encode(json.dumps(cluster_json_data, indent=4).encode('utf-8')).decode('utf-8')
        write_config_cmd = f"mkdir -p /etc/hci && echo {json_b64} | base64 -d > /etc/hci/cluster.json"
        print("Writing /etc/hci/cluster.json on all nodes...")
        results = run_parallel(ips, write_config_cmd)
        for ip, (rc, _, err) in results.items():
            if rc != 0:
                print(f"[ERROR] Failed to write cluster.json on {ip}: {err}")
                sys.exit(1)

        # Configure SELinux permanently to Permissive on all nodes to prevent helper command failures
        print("Setting SELinux to Permissive on all nodes...")
        selinux_results = run_parallel(ips, "setenforce 0 || true; sed -i 's/SELINUX=enforcing/SELINUX=permissive/g' /etc/selinux/config || true")
        for ip, (rc, _, err) in selinux_results.items():
            if rc != 0:
                print(f"[WARNING] Failed to configure SELinux on {ip}: {err}")

        # 3. Dynamic Disk Setup (Non-boot disks >= 100GB)
        print("\n--- Phase 3: Dynamic Disk Scan & LVM Setup ---")
        disk_claim_script = """
import subprocess, json, sys, os
res_vg = subprocess.run("vgs vg_aether --noheadings -o pv_name", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
pvs = []
if res_vg.returncode == 0:
    pvs = [line.strip() for line in res_vg.stdout.decode().splitlines() if line.strip()]

if pvs:
    dev = pvs[0]
    res_lv = subprocess.run("lvs vg_aether/thin_pool_aether", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res_lv.returncode != 0:
        subprocess.run("lvcreate -y -l 100%FREE -T vg_aether/thin_pool_aether", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res_pv_sz = subprocess.run("pvs " + dev + " --units b --noheadings -o pv_size", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    size_bytes = 200 * 10**9
    if res_pv_sz.returncode == 0:
        val = res_pv_sz.stdout.decode().strip().lower().replace("b", "")
        try: size_bytes = int(val)
        except: pass
    print(json.dumps({"status": "exists", "device": dev, "size_bytes": size_bytes}))
    sys.exit(0)

res_lsblk = subprocess.run("lsblk -b -d -n -o NAME,SIZE,TYPE,ROTA", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
if res_lsblk.returncode != 0:
    print(json.dumps({"error": "lsblk failed"}))
    sys.exit(1)

candidate = None
for line in res_lsblk.stdout.decode().splitlines():
    parts = line.split()
    if len(parts) >= 4 and parts[2] == "disk":
        name = parts[0]
        try: size_bytes = int(parts[1])
        except ValueError: continue
        dev_path = "/dev/" + name
        # A claimed disk is wiped, so skip any disk with ANY non-empty mountpoint anywhere in
        # its tree (system path, /srv, /data, swap, ...) -- an in-use disk is never a candidate.
        res_m = subprocess.run("lsblk -n -o MOUNTPOINT " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        is_in_use = False
        for m in res_m.stdout.decode().splitlines():
            m = m.strip()
            if (m and m != "-") or "swap" in m.lower():
                is_in_use = True
                break
        if is_in_use: continue
        res_p = subprocess.run("lsblk -n -o TYPE " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if "part" in res_p.stdout.decode().splitlines(): continue
        if size_bytes >= 100 * 10**9:
            candidate = (dev_path, size_bytes)
            break

if not candidate:
    print(json.dumps({"error": "No empty disk >= 100GB found"}))
    sys.exit(1)

dev_path, size_bytes = candidate
subprocess.run("wipefs -a " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# Zero the first and last 1024MB of the raw disk so no old superblock interferes
subprocess.run("dd if=/dev/zero of=" + dev_path + " bs=1M count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
seek_val = (size_bytes // 1048576) - 1024
subprocess.run("dd if=/dev/zero of=" + dev_path + " bs=1M seek=" + str(seek_val) + " count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("pvcreate -y " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("rm -rf /dev/vg_aether", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("vgcreate vg_aether " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
subprocess.run("lvcreate -y -l 100%FREE -T vg_aether/thin_pool_aether", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
print(json.dumps({"status": "created", "device": dev_path, "size_bytes": size_bytes}))
"""
        claim_script_b64 = base64.b64encode(disk_claim_script.strip().encode()).decode()
        cmd_claim = f"python3 -c \"import base64; exec(base64.b64decode('{claim_script_b64}').decode())\""
        
        print("Scanning and setting up storage pools on remote hosts in parallel...")
        claim_results = run_parallel(ips, cmd_claim)
        
        host_claimed_disks = {}
        for ip, (rc, stdout, stderr) in claim_results.items():
            if rc == 0:
                try:
                    disk_info = json.loads(stdout.strip())
                    if "error" in disk_info:
                        print(f"[ERROR] Host {ip} disk setup failed: {disk_info['error']}")
                        sys.exit(1)
                    host_claimed_disks[ip] = disk_info
                    print(f"[{ip}] Successfully configured storage on device {disk_info['device']} ({disk_info['size_bytes'] / 10**9:.1f} GB) - Status: {disk_info['status']}")
                except Exception as e:
                    print(f"[ERROR] Host {ip} returned invalid json: {stdout} ({e})")
                    sys.exit(1)
            else:
                print(f"[ERROR] Host {ip} failed disk claiming: {stderr}")
                sys.exit(1)

        # 4. Storage engine setup.
        #
        # What this replaces, in order: create /var/lib/linstor and /etc/linstor on every
        # node; start the satellites; start the controller on the leader and stop it
        # everywhere else; wait for port 3370; set TcpPortAutoRange to 7700-7890 so DRBD
        # would not collide with ScyllaDB on 7000; register every node with the
        # controller; register a storage pool per node; create a DRBD resource for
        # LINSTOR's *own* database, format it, stop the controller, copy /var/lib/linstor
        # onto it, remount, restart the controller on top of it, wait up to four minutes
        # for that replication to reach UpToDate everywhere, then align the standbys.
        # Roughly a hundred lines and a dozen ways to fail, most of it protecting the
        # metadata of the thing that was storing the metadata.
        #
        # Sidon has no controller, no node registry, no storage pools and no database of
        # its own. Its map lives in Hydra, which is already replicated and already backed
        # up. Provisioning creates the thin LV and the mount; this starts the daemon and
        # checks that the mount actually took.
        print("\n--- Phase 4: Starting the storage data path ---")
        run_parallel_checked(ips, "systemctl enable sidon && systemctl restart sidon")

        print("Verifying each node's extent store is mounted and answering...")
        for ip in ips:
            rc_cap, out_cap, err_cap = run_remote_spark(ip, SIDON_CAPACITY_CMD)
            if rc_cap != 0 or not out_cap.strip():
                print(f"[ERROR] [{ip}] sidon did not answer on its control socket: "
                      f"{(err_cap or 'no output').strip()[:200]}")
                return
            try:
                cap = json.loads(out_cap.strip().splitlines()[0])
            except Exception:
                print(f"[ERROR] [{ip}] sidon answered with something unparseable.")
                return
            total = int(cap.get("total_bytes") or 0)
            if total <= 0:
                # Almost always an unmounted store. Sidon would write extent groups onto
                # the root filesystem instead, silently, until the root filesystem filled
                # and took the host with it -- so this refuses to continue rather than
                # building a cluster that works until it suddenly does not.
                print(f"[ERROR] [{ip}] the extent store reports no capacity, which means "
                      f"it is not mounted. Check vg_aether/sidon and the fstab entry.")
                return
            print(f"[{ip}] extent store ready: {total / (1024 ** 3):.1f} GiB.")

        print("Writing storage pools config and spectrum configuration on all hosts...")
        for ip in ips:
            # No storage-pools.json and no linstor-client.conf. The first described a
            # pool name, a thin pool and a volume group to a controller that no longer
            # exists; the second named the controllers. Sidon writes extent groups onto
            # one filesystem, and where that filesystem is mounted is the configuration.

            # Only the address is written, because only the address was ever read. See
            # the note in provision.py: SPECTRUM_API_PORT and CLUSTER_SEEDS had no reader
            # anywhere in the tree, and the two writers disagreeing about the rest is
            # what let a node join without an identity.
            spectrum_env = f"LOCAL_HYPERVISOR_IP={ip}\n"
            env_b64 = base64.b64encode(spectrum_env.encode('utf-8')).decode('utf-8')
            run_remote_spark(ip, f"mkdir -p /etc/hci/spectrum && echo {env_b64} | base64 -d > /etc/hci/spectrum/spectrum.env")


        # 5. Database Quorum Setup
        print("Creating ZooKeeper, ScyllaDB, and Aether volume directories on all nodes...")
        run_parallel_checked(ips, "mkdir -p /var/lib/hci/zookeeper/data /var/lib/hci/zookeeper/log /var/lib/hci/hydra/data /var/lib/hci/aether/volumes /var/lib/hci/aether/images /var/lib/hci/aether/nvram")
        
        # Copy Daruk proxy script to ScyllaDB volume directory
        print("Copying Daruk query proxy script to ScyllaDB volume directory on all nodes...")
        run_parallel_checked(ips, "mkdir -p /var/lib/hci/hydra/data && cp /usr/local/bin/daruk.py /var/lib/hci/hydra/data/daruk.py && chmod 644 /var/lib/hci/hydra/data/daruk.py")

        print("Writing dynamic ZooKeeper container configs on all hosts...")
        if len(ips) == 1:
            zoo_servers_env = ""
        else:
            zoo_servers_parts = []
            for i, ip in enumerate(ips, start=1):
                if i > 3:
                    zoo_servers_parts.append(f"server.{i}={ip}:2888:3888:observer;2181")
                else:
                    zoo_servers_parts.append(f"server.{i}={ip}:2888:3888;2181")
            zoo_servers_str = " ".join(zoo_servers_parts)
            zoo_servers_env = f' ZOO_SERVERS="{zoo_servers_str}"'

        for idx, ip in enumerate(ips):
            node_id = idx + 1
            peer_type_env = " ZOO_PEER_TYPE=observer" if node_id > 3 else ""
            zk_quad = (
                "[Unit]\n"
                "Description=ZooKeeper Cluster Consensus Service\n"
                "After=network.target\n\n"
                "[Service]\n"
                "Restart=always\n"
                "CPUWeight=100\n"
                "MemoryMax=512M\n"
                "MemoryHigh=400M\n\n"
                "[Container]\n"
                "Image=docker.io/library/zookeeper:3.9.2\n"
                "Network=host\n"
                "Volume=/var/lib/hci/zookeeper/data:/data:Z\n"
                "Volume=/var/lib/hci/zookeeper/log:/datalog:Z\n"
                f"Environment=ZOO_MY_ID={node_id}{zoo_servers_env}{peer_type_env} ZOO_4LW_COMMANDS_WHITELIST=*\n\n"
                "[Install]\n"
                "WantedBy=multi-user.target\n"
            )
            zk_b64 = base64.b64encode(zk_quad.encode()).decode()
            run_remote_spark(ip, f"mkdir -p /etc/containers/systemd && echo {zk_b64} | base64 -d > /etc/containers/systemd/zookeeper.container && systemctl daemon-reload")

        print("Starting ZooKeeper service in parallel...")
        run_parallel_checked(ips, "systemctl restart zookeeper")
        for ip in ips:
            for _ in range(30):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active zookeeper")
                if rc == 0 and out.strip() == "active":
                    break
                time.sleep(1)
            else:
                print(f"[ERROR] ZooKeeper failed to start on {ip}")
                sys.exit(1)

        print("Writing cluster state 'started' to ZooKeeper consensus...")
        zk_set = False
        for ip in ips:
            rc_state, _, _ = run_remote_spark(ip, "podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 set /cluster_state started || podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 create /cluster_state started")
            if rc_state == 0:
                zk_set = True
                break
        if not zk_set:
            print("[WARNING] Could not write cluster state to ZooKeeper.")

        print("Starting ScyllaDB Database Service in parallel...")
        run_parallel_checked(ips, "systemctl restart hydra-db")
        for ip in ips:
            for _ in range(40):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active hydra-db")
                if rc == 0 and out.strip() == "active":
                    break
                time.sleep(1)
            else:
                print(f"[ERROR] hydra-db failed to start on {ip}")
                sys.exit(1)

        print("Waiting for ScyllaDB to listen on port 9042 on all nodes...")
        for ip in ips:
            print(f"[{ip}] Waiting for ScyllaDB to listen on port 9042...")
            last_progress = None
            for i in range(600):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9042")
                if rc == 0 and "9042" in out:
                    break
                
                # Check and print bootstrap/repair progress every 10 seconds
                if i % 10 == 0:
                    progress = get_scylla_bootstrap_progress(ip)
                    if progress and progress != last_progress:
                        print(f"[{ip}] ScyllaDB Bootstrap Status: {progress}")
                        last_progress = progress
                time.sleep(1)
            else:
                print(f"[ERROR] ScyllaDB port 9042 timeout on {ip}")
                sys.exit(1)

        print("Starting Daruk query proxy service on all hosts...")
        run_parallel_checked(ips, "systemctl restart daruk")
        print("Waiting for Daruk query proxy to listen on port 9043 on all nodes...")
        for ip in ips:
            daruk_ready = False
            for _ in range(30):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9043")
                if rc == 0 and "9043" in out:
                    daruk_ready = True
                    break
                time.sleep(1)
            if not daruk_ready:
                print(f"[ERROR] Daruk query proxy failed to listen on port 9043 on {ip}")
                sys.exit(1)
        print("Daruk query proxy is ready on all nodes.")

        # 6. Start Workload Services
        print("\n--- Phase 6: Starting Core HCI Services ---")
        services = ["spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "logos", "mipha", "agahnim", "slate", "hylia"]
        
        # Check if urbosa enabled
        urbosa_enabled = False
        time.sleep(3) # Wait briefly for ScyllaDB schemas/proxies to stabilize
        rc, out, _ = run_cql_query("SELECT value FROM hydra.cluster_settings WHERE key = 'urbosa_enabled';")
        if rc == 0 and out:
            for line in out.splitlines():
                if "true" in line.lower():
                    urbosa_enabled = True
                    break
        if urbosa_enabled:
            services.append("urbosa")

        for svc in services:
            print(f"Starting {svc} service in parallel across all nodes...")
            run_parallel_checked(ips, f"systemctl restart {svc}")
            for ip in ips:
                for _ in range(30):
                    rc, out, _ = run_remote_spark(ip, f"systemctl is-active {svc}")
                    if rc == 0 and out.strip() == "active":
                        break
                    time.sleep(1)
                else:
                    print(f"[ERROR] Service {svc} failed to enter active state on {ip}")
                    sys.exit(1)

        # 7. Verification & Liveness Check Loop
        print("\n--- Phase 7: Verifying Liveness & Cluster Health ---")
        print("Polling ScyllaDB Gossip Status until all nodes are Up-Normal (UN)...")
        gossip_healthy = False
        for i in range(30):
            rc, out, _ = run_remote_spark(ips[0], "podman exec systemd-hydra-db nodetool status")
            if rc == 0:
                un_count = 0
                for line in out.splitlines():
                    if line.strip().startswith("UN"):
                        un_count += 1
                print(f"Gossip health check {i+1}/30: found {un_count}/{len(ips)} nodes in UN state.")
                if un_count >= len(ips):
                    gossip_healthy = True
                    break
            time.sleep(5)
            
        if not gossip_healthy:
            print("[ERROR] ScyllaDB Gossip ring failed to stabilize. nodetool status output:")
            rc, out, _ = run_remote_spark(ips[0], "podman exec systemd-hydra-db nodetool status")
            print(out)
            sys.exit(1)

        print("Checking ZooKeeper consensus and node states...")
        zk_healthy = True
        leaders = 0
        followers = 0
        for ip in ips:
            zk_cmd = "python3 -c \"import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1', 2181)); s.sendall(b'stat'); print(s.recv(1024).decode('utf-8', errors='ignore'))\""
            rc_zk, out_zk, _ = run_remote_spark(ip, zk_cmd)
            if rc_zk == 0 and "Mode:" in out_zk:
                mode = "unknown"
                for line in out_zk.splitlines():
                    if line.strip().startswith("Mode:"):
                        mode = line.split(":", 1)[1].strip()
                print(f"  [{ip}] ZooKeeper is active in mode: {mode}")
                if mode == "leader" or mode == "standalone":
                    leaders += 1
                elif mode == "follower":
                    followers += 1
            else:
                print(f"  [{ip}] [ERROR] ZooKeeper consensus check failed: {out_zk}")
                zk_healthy = False
        if not zk_healthy or leaders != 1 or followers != len(ips) - 1:
            print(f"[ERROR] ZooKeeper quorum is not healthy. Leaders: {leaders}, Followers: {followers}")
            sys.exit(1)

        print("Verifying every node's storage daemon is reachable from its peers...")
        for ip in ips:
            rc_p, out_p, _ = run_remote_spark(ip, SIDON_PEERS_CMD)
            if rc_p != 0 or not out_p.strip():
                print(f"[WARNING] [{ip}] could not read peer reachability.")
                continue
            try:
                body = json.loads(out_p.strip().splitlines()[0])
            except Exception:
                print(f"[WARNING] [{ip}] the peer listing was unparseable.")
                continue
            unreachable = [peer.get("node") for peer in (body.get("peers") or [])
                           if not peer.get("reachable")]
            if unreachable:
                # Not fatal to cluster creation: writes are only refused when a node in a
                # vdisk's own replica set is down, and no vdisk exists yet. But a peer
                # that cannot be reached now will not be reachable when one does, so it
                # is said out loud rather than discovered by the first failed write.
                print(f"[WARNING] [{ip}] cannot reach: {', '.join(str(u) for u in unreachable)}")
            else:
                print(f"[{ip}] all peers reachable.")

        print("Verifying Spectrum Web UI reachability on port 8443...")
        spectrum_healthy = True
        for ip in ips:
            reached = False
            for _ in range(20):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 8443")
                if rc == 0 and "8443" in out:
                    reached = True
                    break
                time.sleep(2)
            if not reached:
                print(f"[ERROR] Spectrum UI is unreachable on {ip}:8443.")
                spectrum_healthy = False
            else:
                print(f"[{ip}] Spectrum API/UI is responsive on port 8443.")

        if not spectrum_healthy:
            sys.exit(1)

        print("Running diagnostic verification checks using Mimir...")
        rc_m, out_m, _ = run_remote_spark(ips[0], "/usr/local/bin/mcli health_checks run_all")
        if rc_m != 0:
            print(f"[ERROR] Mimir health check execution failed.")
            sys.exit(1)
        fail_count = 0
        for line in out_m.splitlines():
            if "[ FAIL ]" in line:
                fail_count += 1
        if fail_count > 0:
            print(f"[ERROR] Mimir diagnostic checks found {fail_count} failures! Cluster is not healthy.")
            for line in out_m.splitlines():
                if "FAIL" in line:
                    print(line)
            sys.exit(1)
        else:
            print("Mimir diagnostics verified successfully (0 failures detected).")

        print("\n==========================================================")
        print("      HCI Cluster Creation Successful & Verified!         ")
        print("==========================================================")

    elif args.command == "status":
        # Preferred path: read the state ZooKeeper already holds. One connection, no
        # fan-out, and liveness comes from ephemeral znode presence rather than a probe
        # that cannot distinguish "running" from "restarting".
        zk_state = zk_read_cluster_state()
        if zk_state is not None and zk_state["nodes"]:
            if getattr(args, "json", False):
                print(json.dumps({
                    "cluster_state": zk_state["desired"] or "unknown",
                    "source": "zookeeper",
                    "nodes": zk_state["nodes"],
                }, indent=2))
                sys.exit(0)
            print("==========================================================")
            print("                 HCI Cluster Status                       ")
            print("==========================================================")
            print(f"The state of the cluster: {zk_state['desired'] or 'unknown'}")
            print("Lockdown mode: Disabled")
            print(f"{GRAY}Source: ZooKeeper via {zk_state['via']}{RESET}")

            print("\n--- Cluster Services Status ---")
            configured = set(get_cluster_ips())
            for ip in sorted(zk_state["nodes"], key=lambda a: [int(p) for p in a.split(".")] if a.count(".") == 3 and all(p.isdigit() for p in a.split(".")) else [999]):
                print(render_node_block(ip, zk_state["nodes"][ip]))
            # A configured node with no znode is not reporting: either it is down, or its
            # spark-daemon is not running. Ephemeral znodes make this unambiguous.
            for ip in sorted(configured - set(zk_state["nodes"])):
                print(f"\n        Host: {BOLD}{ip}{RESET} {RED}Down{RESET} {GRAY}(no ZooKeeper registration){RESET}")
            print("==========================================================")
            sys.exit(0)

        if zk_state is None:
            print(f"{YELLOW}ZooKeeper unreachable; probing nodes directly over mTLS.{RESET}")
        else:
            print(f"{YELLOW}ZooKeeper reachable but no nodes registered; probing directly.{RESET}")

        print("==========================================================")
        print("                 HCI Cluster Status                       ")
        print("==========================================================")

        path = "/api/v1/cluster/status"
        if args.verbose:
            path += "?verbose=true"
            
        rc, res = make_request(path, method="GET")
        if rc == 0:
            cluster_state = res.get("cluster_state", "stop")
            # map 'start' to 'started', 'stop' to 'stopped'
            state_str = "started" if cluster_state == "start" else "stopped"
            print(f"The state of the cluster: {state_str}")
            print("Lockdown mode: Disabled")
            
            print("\n--- Storage Engine Status (Sidon) ---")
            print(res.get("peer_status") or "No peer info")
            
            print("\n--- Storage Engine Volumes (Aether) ---")
            print(res.get("volume_info") or "No volume info")
            
            print("\n--- Cluster Services Status ---")
            node_statuses = res.get("node_statuses", {})
            for ip, info in node_statuses.items():
                if info.get("online"):
                    print(info.get("output"))
                else:
                    print(f"\n        Host: {ip} Down")
                    print(f"                    Error: {info.get('error')}")
            print("==========================================================")
        else:
            print(f"[ERROR] Failed to query status: {res.get('error')}")
            sys.exit(1)

    elif args.command == "start":
        print("==========================================================")
        print("                 Starting HCI Cluster                     ")
        print("==========================================================")
        ips = get_cluster_ips()
        print(f"Connecting to cluster nodes: {', '.join(ips)}")

        acquire_cluster_lock(ips)
        import atexit
        atexit.register(release_cluster_lock, ips)

        
        # 1. Verify spark-daemon is running on all hosts
        spark_online = {}
        for ip in ips:
            print(f"[{ip}] Contacting spark-daemon on port 9099...")
            rc, stdout, stderr = run_remote_spark(ip, "echo 'online'")
            if rc == 0 and "online" in stdout.lower():
                print(f"[{ip}] spark-daemon is online.")
                spark_online[ip] = True
            else:
                print(f"[{ip}] ERROR: spark-daemon is offline or unreachable: {stderr or 'Connection timeout'}")
                spark_online[ip] = False
                
        if not all(spark_online.values()):
            print("[ERROR] Cannot start cluster: spark-daemon must be online on all nodes.")
            sys.exit(1)

        # Identify nodes in maintenance mode
        maintenance_ips = []
        for ip in ips:
            rc, _, _ = run_remote_spark(ip, "test -f /etc/hci/maintenance.state")
            if rc == 0:
                maintenance_ips.append(ip)
                print(f"[{ip}] Note: Host is currently in maintenance mode.")

        # 2. Start ZooKeeper Service
        print("\n--- Phase 1: Starting ZooKeeper Service ---")
        for ip in ips:
            print(f"[{ip}] Starting ZooKeeper service...")
            run_checked_cmd(ip, "systemctl restart zookeeper")
            
        # Poll ZooKeeper active state
        for ip in ips:
            print(f"[{ip}] Waiting for ZooKeeper service to become active...")
            for _ in range(30):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active zookeeper")
                if rc == 0 and out.strip() == "active":
                    print(f"[{ip}] ZooKeeper service is active.")
                    break
                time.sleep(1)
            else:
                print(f"[{ip}] ERROR: ZooKeeper failed to start.")
                sys.exit(1)
                
        # Wait for consensus quorum
        print("Waiting for ZooKeeper quorum consensus to form...")
        time.sleep(4)
        
        leader_found = False
        for ip in ips:
            cmd_stat = "echo stat | nc 127.0.0.1 2181"
            rc_s, out_s, _ = run_remote_spark(ip, cmd_stat)
            if rc_s == 0 and ("mode: leader" in out_s.lower() or "mode: standalone" in out_s.lower()):
                print(f"[{ip}] Found ZooKeeper Leader node.")
                leader_found = True
        if not leader_found:
            print("[WARNING] ZooKeeper leader node could not be identified, continuing anyway.")

        # 3. Set cluster state in ZooKeeper
        print("Writing cluster state 'started' to ZooKeeper consensus...")
        zk_set = False
        for ip in ips:
            rc_state, _, _ = run_checked_cmd(ip, "podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 set /cluster_state started || podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 create /cluster_state started")
            if rc_state == 0:
                zk_set = True
                break
        if zk_set:
            print("Cluster state successfully set to 'started' in ZooKeeper.")
        else:
            print("[WARNING] Could not write cluster state to ZooKeeper.")

        # 4. Start ScyllaDB (hydra-db)
        print("\n--- Phase 2: Starting ScyllaDB Database Service ---")
        for ip in ips:
            print(f"[{ip}] Starting hydra-db systemd service...")
            run_checked_cmd(ip, "systemctl restart hydra-db")
            
        for ip in ips:
            print(f"[{ip}] Waiting for hydra-db service to become active...")
            for _ in range(35):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active hydra-db")
                if rc == 0 and out.strip() == "active":
                    break
                time.sleep(1)
            else:
                print(f"[{ip}] ERROR: hydra-db service failed to start.")
                sys.exit(1)
                
        for ip in ips:
            print(f"[{ip}] Waiting for ScyllaDB to start listening on port 9042...")
            last_progress = None
            for i in range(300):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9042")
                if rc == 0 and "9042" in out:
                    print(f"[{ip}] ScyllaDB is accepting database connections on port 9042.")
                    break
                
                # Check and print bootstrap/repair progress every 10 seconds
                if i % 10 == 0:
                    progress = get_scylla_bootstrap_progress(ip)
                    if progress and progress != last_progress:
                        print(f"[{ip}] ScyllaDB Bootstrap Status: {progress}")
                        last_progress = progress
                time.sleep(1)
            else:
                print(f"[{ip}] ERROR: ScyllaDB database connection port 9042 timeout.")
                sys.exit(1)

        # 4.5 Start Daruk Query Proxy
        for ip in ips:
            print(f"[{ip}] Starting Daruk ScyllaDB query proxy...")
            run_checked_cmd(ip, "systemctl restart daruk")

        print("Waiting for Daruk query proxy to listen on port 9043 on all nodes...")
        for ip in ips:
            daruk_ready = False
            for _ in range(30):
                rc, out, _ = run_remote_spark(ip, "ss -tlnp | grep 9043")
                if rc == 0 and "9043" in out:
                    daruk_ready = True
                    break
                time.sleep(1)
            if not daruk_ready:
                print(f"[ERROR] Daruk query proxy failed to listen on port 9043 on {ip}")
                sys.exit(1)
        print("Daruk query proxy is ready on all nodes.")

        # 5. Start Aether Storage Service
        print("\n--- Phase 3: Starting Aether Storage Service ---")
        for ip in ips:
            print(f"[{ip}] Starting aether systemd service...")
            run_checked_cmd(ip, "systemctl restart aether")
            
        for ip in ips:
            print(f"[{ip}] Waiting for aether service to become active...")
            for _ in range(30):
                rc, out, _ = run_remote_spark(ip, "systemctl is-active aether")
                if rc == 0 and out.strip() == "active":
                    break
                time.sleep(1)
            else:
                print(f"[{ip}] ERROR: aether service failed to start.")
                sys.exit(1)
                

        # 6. Start remaining services
        print("\n--- Phase 4: Starting Core Workload & Coordination Services ---")
        services = ["spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "logos", "mipha", "agahnim", "slate", "hylia"]
        if check_urbosa_enabled():
            services.append("urbosa")
        service_ports = {
            "spectrum": 8443,
            "vali": 9095,
            "catalyst": 9091,
            "agahnim": 8081,
            "slate": 443
        }
        
        for svc in services:
            for ip in ips:
                if ip in maintenance_ips:
                    continue
                print(f"[{ip}] Starting systemd service: {svc}...")
                run_checked_cmd(ip, f"systemctl restart {svc}")
                
        for svc in services:
            for ip in ips:
                if ip in maintenance_ips:
                    continue
                print(f"[{ip}] Verifying service {svc} is active...")
                for _ in range(30):
                    rc, out, _ = run_remote_spark(ip, f"systemctl is-active {svc}")
                    if rc == 0 and out.strip() == "active":
                        break
                    time.sleep(1)
                else:
                    print(f"[{ip}] ERROR: Service '{svc}' failed to enter active state.")
                    sys.exit(1)
                    
                if svc in service_ports:
                    port = service_ports[svc]
                    print(f"[{ip}] Waiting for service {svc} to listen on port {port}...")
                    for _ in range(45):
                        rc_p, out_p, _ = run_remote_spark(ip, f"ss -tlnp | grep {port}")
                        if rc_p == 0 and str(port) in out_p:
                            print(f"[{ip}] Service {svc} is listening on port {port}.")
                            break
                        time.sleep(1)
                    else:
                        print(f"[{ip}] ERROR: Service {svc} failed to listen on port {port}.")
                        sys.exit(1)
                        
        # 7. Wait for every node to report convergence through ZooKeeper. The desired
        # state was recorded in Phase 1; each node's spark-daemon converges toward it and
        # republishes what it actually achieved, so this observes the cluster rather than
        # assuming the start commands above were sufficient.
        print("\n--- Phase 5: Waiting for Cluster Convergence ---")
        converged = wait_for_cluster_convergence(ips)

        print("\n--- Cluster Services Status ---")
        final_state = zk_read_cluster_state()
        if final_state and final_state["nodes"]:
            for ip in sorted(final_state["nodes"]):
                print(render_node_block(ip, final_state["nodes"][ip]))
        else:
            print("  (ZooKeeper unreachable; run 'cluster status' for a direct probe)")
        print("==========================================================")

        if not converged:
            print(f"{YELLOW}Cluster started but did not fully converge. See the table above.{RESET}")

        # 8. Post-Start Health Verification Checks
        print("\n--- Phase 6: Cluster Health Verification ---")
        
        # A. ZooKeeper Consensus Check
        print("Checking ZooKeeper consensus quorum...")
        leaders = 0
        followers = 0
        zk_healthy = True
        for ip in ips:
            zk_cmd = "python3 -c \"import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1', 2181)); s.sendall(b'stat'); print(s.recv(1024).decode('utf-8', errors='ignore'))\""
            rc_zk, out_zk, _ = run_remote_spark(ip, zk_cmd)
            if rc_zk == 0 and "Mode:" in out_zk:
                mode = "unknown"
                for line in out_zk.splitlines():
                    if line.strip().startswith("Mode:"):
                        mode = line.split(":", 1)[1].strip()
                if mode == "leader" or mode == "standalone":
                    leaders += 1
                elif mode == "follower":
                    followers += 1
            else:
                zk_healthy = False
        if not zk_healthy or leaders != 1 or followers != len(ips) - 1:
            print(f"[ERROR] Cluster start verification failed: ZooKeeper quorum is not healthy. Leaders: {leaders}, Followers: {followers}")
            sys.exit(1)
        print("  ZooKeeper quorum is healthy.")

        # B. Every node's data path is up.
        #
        # This used to wait up to 45 seconds for Mipha to promote the linstor-db DRBD
        # volume and bring a controller up on one node, then confirm it was listening on
        # 3370. There is no controller and no election: each node runs its own daemon and
        # answers for itself, so the check is per node and there is nothing to elect.
        for ip in ips:
            rc_s, out_s, _ = run_remote_spark(ip, "systemctl is-active sidon")
            if rc_s != 0 or out_s.strip() != "active":
                print(f"[ERROR] Cluster start verification failed: sidon is not active on {ip}.")
                sys.exit(1)
        print("  The storage data path is running on every node.")

        # C. Peers can reach each other
        print("Verifying every node's storage daemon is reachable from its peers...")
        for ip in ips:
            rc_p, out_p, _ = run_remote_spark(ip, SIDON_PEERS_CMD)
            if rc_p != 0 or not out_p.strip():
                print(f"[WARNING] [{ip}] could not read peer reachability.")
                continue
            try:
                body = json.loads(out_p.strip().splitlines()[0])
            except Exception:
                print(f"[WARNING] [{ip}] the peer listing was unparseable.")
                continue
            unreachable = [peer.get("node") for peer in (body.get("peers") or [])
                           if not peer.get("reachable")]
            if unreachable:
                # Not fatal to cluster creation: writes are only refused when a node in a
                # vdisk's own replica set is down, and no vdisk exists yet. But a peer
                # that cannot be reached now will not be reachable when one does, so it
                # is said out loud rather than discovered by the first failed write.
                print(f"[WARNING] [{ip}] cannot reach: {', '.join(str(u) for u in unreachable)}")
            else:
                print(f"[{ip}] all peers reachable.")

        print("Running diagnostic verification checks using Mimir...")
        rc_m, out_m, _ = run_remote_spark(ips[0], "/usr/local/bin/mcli health_checks run_all")
        if rc_m != 0:
            print(f"[ERROR] Cluster start verification failed: Mimir health check execution failed.")
            sys.exit(1)
        if "[FAIL]" in out_m or "FAIL" in out_m:
            failed_checks = []
            for line in out_m.splitlines():
                if "[FAIL]" in line or "FAIL" in line:
                    failed_checks.append(line.strip())
            print(f"[ERROR] Cluster start verification failed: Mimir diagnostic checks failed:\n" + "\n".join(failed_checks))
            sys.exit(1)
        print("  All Mimir diagnostic checks passed successfully.")

        print("\n==========================================================")
        print("      HCI Cluster Started & Verified Successfully!       ")
        print("==========================================================")

    elif args.command == "stop":
        print("==========================================================")
        print("                 Stopping HCI Cluster                     ")
        print("==========================================================")
        
        ips = get_cluster_ips()
        acquire_cluster_lock(ips)
        import atexit
        atexit.register(release_cluster_lock, ips)

        
        # 1. Stop running VMs step-by-step
        print("--- Step 1: Stopping running VMs step-by-step ---")
        rc, stdout, err = run_cql_query("SELECT JSON name, host_ip, state FROM hydra.vms;")
        vms = []
        if rc == 0:
            for line in stdout.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        vms.append(json.loads(line))
                    except:
                        pass
        
        running_vms = [v for v in vms if v.get("state") in ["Running", "start", "on"]]
        if running_vms:
            for vm in running_vms:
                name = vm.get("name")
                host_ip = vm.get("host_ip")
                if not host_ip or host_ip == "N/A":
                    continue
                print(f"Stopping VM '{name}' on host {host_ip}...")
                run_remote_spark(host_ip, f"virsh shutdown {name}")
                
                # Poll up to 5 seconds
                stopped = False
                for _ in range(5):
                    time.sleep(1)
                    rc_dom, dom_state, _ = run_remote_spark(host_ip, f"virsh domstate {name}")
                    if rc_dom == 0 and "shut off" in dom_state.lower():
                        stopped = True
                        break
                if not stopped:
                    print(f"VM '{name}' did not shut down gracefully. Forcing power off (destroy)...")
                    run_remote_spark(host_ip, f"virsh destroy {name}")
                
                # Update ScyllaDB
                run_cql_query(f"UPDATE hydra.vms SET state = 'Stopped', host_ip = '' WHERE name = '{name}';")
        else:
            print("No running VMs detected.")
            
        # 2. Set cluster state to stopped in ZooKeeper
        print("\n--- Step 2: Setting cluster state in ZooKeeper ---")
        zk_set = False
        for ip in get_cluster_ips():
            rc_zk, _, _ = run_remote_spark(ip, "podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 set /cluster_state stopped")
            if rc_zk == 0:
                zk_set = True
                break
        if zk_set:
            print("Cluster state set to 'stopped' in ZooKeeper.")
        else:
            print("Warning: Failed to set cluster state to stopped in ZooKeeper.")
            
        # 3. Stop workload and HA services in parallel
        print("\n--- Step 3: Stopping workload and HA services in parallel across all nodes ---")
        workload_services = ["hylia", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "logos", "mipha", "agahnim", "slate"]
        for svc in workload_services:
            print(f"Stopping systemd service '{svc}' in parallel across all nodes...")
            run_parallel(ips, f"systemctl stop {svc}")
            
        # 3.5. Drain the journals, then unmount.
        #
        # This used to wait up to two minutes for DRBD resyncs to finish before shutting
        # down, because a node stopped mid-resync came back with a stale replica that had
        # to catch up block by block. There is no resync: extent groups are immutable, so
        # a returning node's copies are correct or absent, and Purah restores absent ones.
        #
        # What is worth doing before stopping is draining. Every acknowledged write is
        # already durable in the journal, so stopping right now would lose nothing -- but
        # a full journal is one the next start has to replay, and draining here turns a
        # slow startup into a slightly slower shutdown. Detach does it, because a clean
        # detach drains before it releases.
        print("\n--- Step 3.5: Draining journals before shutdown ---")
        for ip in ips:
            rc_l, out_l, _ = run_remote_spark(ip, SIDON_LIST_CMD)
            if rc_l != 0 or not out_l.strip():
                continue
            try:
                attached = json.loads(out_l.strip().splitlines()[0]).get("attached") or []
            except Exception:
                continue
            for vdisk in attached:
                vdisk_id = vdisk.get("vdisk_id")
                # A forwarded vdisk has nothing local to drain: the owner holds the
                # journal and draining it is the owner's business.
                if not vdisk_id or vdisk.get("role") == "forwarding":
                    continue
                run_remote_spark(ip, sidon_detach_cmd(vdisk_id))
        print("Journals drained.")

        # 4. Unmount the extent store in parallel
        print("\n--- Step 4: Unmounting the extent store across all nodes ---")
        run_parallel(ips, "umount -l /var/lib/hci/sidon || true")

        # 5. Stop storage and controller services in parallel
        print("\n--- Step 5: Stopping storage services in parallel across all nodes ---")
        storage_services = ["sidon", "daruk"]
        if check_urbosa_enabled():
            storage_services.insert(0, "urbosa")
        for svc in storage_services:
            print(f"Stopping systemd service '{svc}' in parallel across all nodes...")
            run_parallel(ips, f"systemctl stop {svc}")

        # Nothing to bring down at the block layer. `drbdadm down all` detached every
        # resource from its device; a vdisk was never attached to one.

        # 7. Stop database and coordination services in parallel
        print("\n--- Step 7: Stopping database and coordination services in parallel across all nodes ---")
        db_services = ["hydra-db", "zookeeper"]
        for svc in db_services:
            print(f"Stopping systemd service '{svc}' in parallel across all nodes...")
            run_parallel(ips, f"systemctl stop {svc}")
            
        # 8. Restart spark-daemon asynchronously in parallel
        print("\n--- Step 8: Restarting spark-daemon asynchronously in parallel ---")
        run_parallel(ips, "(sleep 1 && systemctl restart spark-daemon) >/dev/null 2>&1 < /dev/null &")
            
        print("Stop command execution completed.")

    elif args.command == "destroy":
        print("==========================================================")
        print("                 Destroying HCI Cluster                   ")
        print("==========================================================")
        config_ips = []
        try:
            with open("/etc/hci/cluster.json", "r") as f:
                cdata = json.load(f)
                config_ips = [h["ip"] for h in cdata.get("hosts", [])]
        except Exception:
            pass

        if args.servers:
            ips = [ip.strip() for ip in args.servers.split(",") if ip.strip()]
        elif config_ips:
            ips = config_ips
        else:
            ips = ["127.0.0.1"]

        print(f"Target cluster hosts: {', '.join(ips)}")

        acquire_cluster_lock(ips)
        import atexit
        atexit.register(release_cluster_lock, ips)


        # 1. Stop and undefine all libvirt VMs (with a timeout to prevent hanging)
        print("\n--- Phase 1: Stopping & Undefining libvirt VMs ---")
        vm_cleanup_cmd = "timeout 15 sh -c 'for vm in $(virsh list --all --name); do echo \"Forcing VM destroy: $vm\"; virsh destroy $vm || true; virsh undefine $vm --nvram || true; done' || echo 'VM cleanup timed out'"
        for ip in ips:
            print(f"[{ip}] Cleaning up virtual machines...")
            rc, out, err = run_remote_spark(ip, vm_cleanup_cmd)
            if out.strip():
                print(f"[{ip}] Log:\n{out}")
            if rc != 0:
                print(f"[{ip}] [WARNING] Failed to clean VMs: {err}")

        # 2. Stop all core HCI services in parallel
        print("\n--- Phase 2: Stopping Core HCI Services ---")
        services = ["hylia", "logos", "mipha", "spectrum", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "sidon", "daruk", "hydra-db", "zookeeper"]
        svc_list = " ".join(services)
        for ip in ips:
            print(f"[{ip}] Stopping services: {', '.join(services)}")
            rc, out, err = run_remote_spark(ip, f"systemctl stop {svc_list} || true")
            if out.strip():
                print(f"[{ip}] Log:\n{out}")
            if rc != 0:
                print(f"[{ip}] [WARNING] Failed to stop services: {err}")

        # 3. Unmount the extent store on all hosts
        print("\n--- Phase 3: Unmounting Storage Volumes ---")
        for ip in ips:
            print(f"[{ip}] Unmounting volume paths...")
            rc1, out1, err1 = run_remote_spark(ip, "umount -l /var/lib/hci/aether/volumes/default-vm-container || true")
            if out1.strip() or err1.strip():
                print(f"[{ip}] VM Volume Unmount Output: {out1 or err1}")
            rc2, out2, err2 = run_remote_spark(ip, "umount -l /var/lib/hci/aether/volumes/default-image-container || true")
            if out2.strip() or err2.strip():
                print(f"[{ip}] Image Volume Unmount Output: {out2 or err2}")

        # 4. Stop the data path.
        #
        # This used to enumerate DRBD resources with drbdsetup and bring each one down,
        # because a resource left up held its backing device open and the LVM wipe below
        # would fail on it. A vdisk is a file on a filesystem: stopping the daemon and
        # unmounting is the whole teardown.
        print("\n--- Phase 4: Stopping the storage data path ---")
        for ip in ips:
            run_remote_spark(ip, "systemctl stop sidon || true")
            run_remote_spark(ip, "umount -l /var/lib/hci/sidon || true")
            print(f"[{ip}] storage stopped and unmounted.")

        # 5. Wipe LVM vg/thin-pool and disk signatures dynamically
        print("\n--- Phase 5: Wiping LVM Pools & Disk Signatures ---")
        for ip in ips:
            print(f"[{ip}] Removing LVM thin pool 'thin_pool_aether' and VG 'vg_aether'...")
            rc, out, err = run_remote_spark(ip, "lvchange -an -f /dev/vg_aether/* || true; lvremove -y -f vg_aether || true; vgremove -y -f vg_aether || true; rm -rf /dev/vg_aether || true; dmsetup ls | grep vg_aether | awk '{print $1}' | while read -r dm; do dmsetup remove -f \"$dm\" || true; done")
            if out.strip():
                print(f"[{ip}] LVM VG removal log:\n{out}")
            if rc != 0:
                print(f"[{ip}] [WARNING] LVM VG removal failed: {err}")

        # Python script to dynamically discover the physical disks this cluster actually claimed
        # (pool disks recorded in storage-pools.json, vg_aether/orphaned PVs, and qualifying raw
        # disks) and zero them. There is deliberately NO hardcoded device fallback: a disk that
        # none of the discovery sources returns is never touched, and an empty result is a no-op.
        wipe_devices_script = """
import subprocess, json, sys, os
devs = []
reasons = {}
skipped = []

def add_dev(dev, reason):
    if dev and dev not in devs:
        devs.append(dev)
        reasons[dev] = reason

def mountpoints_of(dev):
    mounts = []
    res_m = subprocess.run("lsblk -n -o MOUNTPOINT " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for m in res_m.stdout.decode().splitlines():
        m = m.strip()
        if m and m != "-" and m not in mounts:
            mounts.append(m)
    return mounts

def size_of(dev):
    res_sz = subprocess.run("blockdev --getsize64 " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res_sz.returncode == 0:
        try: return int(res_sz.stdout.decode().strip())
        except ValueError: return -1
    return -1

def signatures_of(dev):
    sigs = []
    res_w = subprocess.run("wipefs " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in res_w.stdout.decode().splitlines()[1:]:
        cols = line.split()
        if len(cols) >= 3 and cols[2] not in sigs:
            sigs.append(cols[2])
    return sigs

# 0. Disks this node recorded as its own Aether storage pool members (authoritative)
try:
    with open("/etc/hci/aether/storage-pools.json", "r") as f:
        spdata = json.load(f)
    for disk in spdata.get("local_disks", []):
        add_dev(disk.get("device"), "configured in storage-pools.json")
except Exception:
    pass

# 1. Find PVs of vg_aether or orphaned PVs
res_pvs = subprocess.run("pvs --noheadings -o pv_name,vg_name", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
if res_pvs.returncode == 0:
    for line in res_pvs.stdout.decode().splitlines():
        parts = line.split()
        if len(parts) >= 1:
            pv = parts[0].strip()
            vg = parts[1].strip() if len(parts) >= 2 else ""
            if vg in ["vg_aether", ""]:
                add_dev(pv, "LVM PV (vg=" + (vg if vg else "orphaned") + ")")

# 2. Scan for candidate disks >= 100GB (unmounted, no partitions)
res_lsblk = subprocess.run("lsblk -b -d -n -o NAME,SIZE,TYPE,ROTA", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
if res_lsblk.returncode == 0:
    for line in res_lsblk.stdout.decode().splitlines():
        parts = line.split()
        if len(parts) >= 4 and parts[2] == "disk":
            name = parts[0]
            try: size_bytes = int(parts[1])
            except ValueError: continue
            dev_path = "/dev/" + name
            if dev_path in devs: continue
            # Skip any disk carrying ANY non-empty mountpoint anywhere in its tree, not just
            # recognised system paths: a disk mounted at /srv or /data is in use, not a candidate.
            skip_reason = ""
            for m in mountpoints_of(dev_path):
                if "swap" in m.lower():
                    skip_reason = "active swap (" + m + ")"
                else:
                    skip_reason = "mounted at " + m
                break
            if not skip_reason:
                res_p = subprocess.run("lsblk -n -o TYPE " + dev_path, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if "part" in res_p.stdout.decode().splitlines():
                    skip_reason = "disk has partitions"
            if not skip_reason and size_bytes < 100 * 10**9:
                skip_reason = "smaller than 100GB (" + ("%.1f" % (size_bytes / 10.0**9)) + " GB)"
            if skip_reason:
                skipped.append((dev_path, skip_reason))
                continue
            add_dev(dev_path, "unpartitioned unmounted disk >= 100GB")

# 3. Final veto: never touch a device that is missing or still has anything mounted on it,
#    whichever source proposed it.
vetted = []
for dev in devs:
    if not os.path.exists(dev):
        skipped.append((dev, "device not present on this host"))
        continue
    mounts = mountpoints_of(dev)
    swap_mounts = [m for m in mounts if "swap" in m.lower()]
    if swap_mounts:
        skipped.append((dev, "active swap (" + ",".join(swap_mounts) + ") -- refusing to wipe"))
        continue
    if mounts:
        skipped.append((dev, "still mounted at " + ",".join(mounts) + " -- refusing to wipe"))
        continue
    vetted.append(dev)
devs = vetted

# 4. Print the exact wipe set (and every rejection) before destroying anything
print("=== cluster destroy: disk wipe plan for this host ===")
for dev, why in skipped:
    print("  SKIP  " + dev + " -- " + why)
if not devs:
    print("  No qualifying devices found. Nothing will be wiped on this host.")
    print("=== end of wipe plan (no-op) ===")
    sys.exit(0)
for dev in devs:
    size_bytes = size_of(dev)
    size_str = ("%.1f GB" % (size_bytes / 10.0**9)) if size_bytes > 0 else "unknown"
    sigs = signatures_of(dev)
    mounts = mountpoints_of(dev)
    print("  WIPE  " + dev + " -- size=" + size_str + " signatures=" + (",".join(sigs) if sigs else "none") + " mountpoints=" + (",".join(mounts) if mounts else "none") + " reason=" + reasons.get(dev, "unknown"))
print("=== wiping " + str(len(devs)) + " device(s): " + ", ".join(devs) + " ===")

for dev in devs:
    subprocess.run("pvremove -y " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if os.path.exists("/etc/lvm/devices/system.devices"):
        dev_name = dev.split("/")[-1]
        subprocess.run("sed -i '/" + dev_name + "/d' /etc/lvm/devices/system.devices", shell=True)
    subprocess.run("wipefs -a " + dev, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    subprocess.run("dd if=/dev/zero of=" + dev + " bs=1M count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    size_bytes = size_of(dev)
    if size_bytes > 0:
        seek_val = (size_bytes // 1048576) - 1024
        if seek_val > 0:
            subprocess.run("dd if=/dev/zero of=" + dev + " bs=1M seek=" + str(seek_val) + " count=1024 conv=notrunc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        print("Failed to determine size of " + dev + "; skipped zeroing end of device")
    print("Wiped " + dev)
"""
        wipe_script_b64 = base64.b64encode(wipe_devices_script.strip().encode()).decode()
        cmd_wipe = f"python3 -c \"import base64; exec(base64.b64decode('{wipe_script_b64}').decode())\""
        
        for ip in ips:
            print(f"[{ip}] Running physical disk signature wipe & zeroing...")
            rc_pv, out_pv, err_pv = run_remote_spark(ip, cmd_wipe)
            if out_pv.strip():
                print(f"[{ip}] Wipe log:\n{out_pv}")
            if rc_pv != 0:
                print(f"[{ip}] [WARNING] Wipe execution failed: {err_pv}")

        # 6. Run clean-up script (removes files, folders, fstab mappings)
        print("\n--- Phase 6: Wiping Storage Directories & Containers ---")
        wipe_script = """
import subprocess
import os
import sys

def run_with_timeout(cmd, timeout=15):
    print(f"Running command: {cmd}", flush=True)
    try:
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        print(f"Status: {res.returncode}", flush=True)
        if res.stdout:
            print(res.stdout.decode(errors='ignore').strip(), flush=True)
        if res.stderr:
            print(res.stderr.decode(errors='ignore').strip(), flush=True)
        return res.returncode
    except subprocess.TimeoutExpired:
        print(f"Command timed out after {timeout} seconds", flush=True)
        return -1

print("--- Running local wipe script ---", flush=True)
res = subprocess.run("lsblk -n -o NAME,MOUNTPOINT", shell=True, stdout=subprocess.PIPE)
out = res.stdout.decode()
claimed = []
for line in out.splitlines():
    if '/var/lib/hci/aether/bricks/' in line:
        parts = line.split()
        if len(parts) >= 2:
            claimed.append((f"/dev/{parts[0]}", parts[1]))

try:
    with open("/etc/fstab", "r") as f:
        for line in f:
            if '/var/lib/hci/aether/bricks/' in line:
                parts = line.split()
                if len(parts) >= 2:
                    dev_path = parts[0]
                    mount_point = parts[1]
                    if not any(c[1] == mount_point for c in claimed):
                        claimed.append((dev_path, mount_point))
except Exception as e:
    print(f"Error reading fstab: {e}", flush=True)

for dev, mount in claimed:
    real_dev = dev
    if dev.startswith("UUID="):
        uuid_val = dev.split("=", 1)[1]
        uuid_path = f"/dev/disk/by-uuid/{uuid_val}"
        if os.path.exists(uuid_path):
            real_dev = os.path.realpath(uuid_path)
        else:
            res_ff = subprocess.run(f"findfs {dev}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if res_ff.returncode == 0:
                real_dev = res_ff.stdout.decode().strip()
    print(f"Wiping mount point {mount} on device {real_dev}...", flush=True)
    run_with_timeout(f"umount -l {mount}", timeout=10)
    run_with_timeout(f"sed -i '\\\\|{mount}|d' /etc/fstab", timeout=5)
    run_with_timeout(f"wipefs -a {real_dev}", timeout=10)
    run_with_timeout(f"rm -rf {mount}", timeout=10)

print("Stopping the storage data path...", flush=True)
run_with_timeout("systemctl stop sidon || true", timeout=15)
run_with_timeout("umount -l /var/lib/hci/sidon || true", timeout=10)

print("Removing system containers...", flush=True)
run_with_timeout("podman rm -f systemd-hydra-db systemd-zookeeper systemd-spectrum || true", timeout=15)

print("Removing storage directories...", flush=True)
run_with_timeout("rm -rf /var/lib/hci/zookeeper/data /var/lib/hci/zookeeper/log /var/lib/hci/hydra/data /var/lib/hci/aether/data /var/lib/hci/aether/volumes /var/lib/hci/aether/images /var/lib/hci/aether/nvram /run/hci/*", timeout=10)
run_with_timeout("rm -rf /etc/hci/odin /etc/hci/spectrum /etc/hci/cluster.json /var/lib/hci/sidon", timeout=10)
print("--- Local wipe completed ---", flush=True)
"""
        wipe_b64 = base64.b64encode(wipe_script.encode()).decode()
        cmd_wipe = f"python3 -c \"import base64; exec(base64.b64decode('{wipe_b64}').decode())\""
        for ip in ips:
            print(f"[{ip}] Wiping local filesystem data and system containers...")
            rc, out, err = run_remote_spark(ip, cmd_wipe)
            if out.strip():
                print(f"[{ip}] Log:\n{out}")
            if rc != 0:
                print(f"[{ip}] [WARNING] Cleanup failed: {err}")

        # 7. Restart spark-daemon asynchronously on all hosts to complete destroy
        print("\n--- Phase 7: Restarting spark-daemon Services ---")
        for ip in ips:
            print(f"[{ip}] Restarting spark-daemon...")
            rc, out, err = run_remote_spark(ip, "(sleep 1 && systemctl restart spark-daemon) >/dev/null 2>&1 < /dev/null &")
            if rc != 0:
                print(f"[{ip}] [WARNING] Failed to launch background spark-daemon restart: {err or out}")

        print("\n==========================================================")
        print("      HCI Cluster Destroyed & Cleaned Successfully!        ")
        print("==========================================================")

    elif args.command == "ring":
        ips = get_cluster_ips()
        members, error = read_ring(ips)
        if error:
            print(f"[ERROR] Could not read the ScyllaDB ring: {error}")
            sys.exit(1)
        print("\nScyllaDB ring (Hydra metadata):")
        print(render_ring(members, get_hydra_replication_factor()))

        # The two memberships side by side. They diverge silently, and the divergence is
        # what makes a maintenance refusal look arbitrary.
        rc, stdout, _ = run_cql_query("SELECT JSON hostname, ip, status FROM hydra.nodes;")
        rows = []
        if rc == 0 and stdout:
            for line in stdout.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        pass
        if rows:
            print("\n  Cluster membership (hydra.nodes):")
            addresses = {m["address"] for m in members}
            for row in rows:
                in_ring = "in ring" if row.get("ip") in addresses else f"{YELLOW}not in ring{RESET}"
                print(f"    {row.get('hostname', ''):<20} {row.get('ip', ''):<16} "
                      f"{row.get('status', ''):<22} {in_ring}")
            configured = {row.get("ip") for row in rows}
            for address in sorted(addresses - configured):
                label = "(no hydra.nodes row)"
                print(f"    {GRAY}{label:<20}{RESET} {address:<16} "
                      f"{YELLOW}ring member the cluster does not know about{RESET}")
        print()

    elif args.command == "decommission":
        # Permanently removing a node from the ring. This command does the parts that are
        # reversible and refuses when the destructive part would be unsafe; it never runs
        # `nodetool decommission` or `nodetool removenode` itself. Those stream every
        # token range this node owns to its replicas, run for as long as that takes, and
        # cannot be undone or re-run -- a decommission interrupted half way leaves a node
        # that is neither in the ring nor out of it. That is an operator's decision made
        # while watching it, not a side effect of a CLI verb.
        if not args.node:
            parser.error("decommission requires --node <ip>")
        target = args.node.strip()
        ips = get_cluster_ips()
        survivors = [ip for ip in ips if ip != target]

        print("==========================================================")
        print(f"   Decommission preflight for {target}")
        print("==========================================================")

        if target not in ips:
            print(f"[WARNING] {target} is not listed in /etc/hci/cluster.json.")

        replication_factor = get_hydra_replication_factor()
        members, error = read_ring(survivors or ips)
        if error:
            print(f"[ERROR] Could not read the ScyllaDB ring: {error}")
            print("[ERROR] Refusing to plan a decommission against a ring that cannot be read.")
            sys.exit(1)

        print("\nScyllaDB ring:")
        print(render_ring(members, replication_factor))

        ring_member = next((m for m in members if m["address"] == target), None)
        others_down = [m for m in members if not m["available"] and m["address"] != target]

        blockers = []
        notes = []

        # There is no sequence to print for the last node: every step below assumes
        # somewhere for its data to go. Say so and stop, rather than emitting a plan whose
        # third step is "lower the replication factor to 0".
        if ring_member is not None and len(members) == 1:
            print(f"\n[BLOCKED] {target} is the only member of the ring. Decommissioning it "
                  "does not shrink the cluster, it destroys it: there is no remaining "
                  "replica for its data to stream to. Use 'cluster destroy' if that is "
                  "what you mean.")
            sys.exit(1)

        if replication_factor is None:
            blockers.append(
                "The hydra keyspace's replication factor could not be read, so the effect "
                "of removing a replica is unknown.")
        elif ring_member is not None:
            remaining = len(members) - 1
            assigned_after = min(replication_factor, remaining)
            required = quorum_of(replication_factor)
            if assigned_after < required:
                blockers.append(
                    f"After removal the ring holds {remaining} node(s), so a partition has "
                    f"{assigned_after} replica(s), and QUORUM at RF={replication_factor} needs "
                    f"{required}. Lower the keyspace replication factor to at most {remaining} "
                    f"and run a full repair BEFORE decommissioning.")
            elif assigned_after == required:
                notes.append(
                    f"After removal the ring has exactly {assigned_after} replica(s) for a "
                    f"quorum of {required}: the cluster will survive the removal and will not "
                    f"survive the next node failure. Plan a replacement.")

        if others_down:
            blockers.append(
                "Other ring members are not up and normal (" +
                ", ".join(f"{m['address']} {m['status']}{m['state']}" for m in others_down) +
                "). A decommission streams this node's data to its replicas; with a replica "
                "unavailable the stream cannot complete and the data it carried is lost.")

        if ring_member is None:
            notes.append(f"{target} is not a ring member. Nothing to detach -- only the "
                         "bookkeeping below is outstanding.")
        elif not ring_member["available"]:
            notes.append(
                f"{target} is '{ring_member['status']}{ring_member['state']}', not up. "
                "`nodetool decommission` runs ON the node being removed and needs it "
                "running; a node that is gone for good is removed from a SURVIVING node "
                f"with `nodetool removenode {ring_member['host_id'] or '<host-id>'}` instead, "
                "which rebuilds its ranges from the remaining replicas.")

        # VMs still placed here. Removing the node's metadata row while a VM still points
        # at it leaves that VM unstartable and unfindable.
        rc, stdout, _ = run_cql_query("SELECT JSON name, host_ip, state FROM hydra.vms;")
        placed = []
        if rc == 0 and stdout:
            for line in stdout.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        vm = json.loads(line)
                        if vm.get("host_ip") == target:
                            placed.append(vm.get("name"))
                    except Exception:
                        pass
        if placed:
            blockers.append(
                f"{len(placed)} VM(s) are still placed on {target}: {', '.join(sorted(placed)[:10])}"
                f"{' ...' if len(placed) > 10 else ''}. Drain the host first "
                f"(maintenance mode), which migrates them and leaves the placements clean.")

        print()
        for note in notes:
            print(f"[NOTE] {note}")
        for blocker in blockers:
            print(f"[BLOCKED] {blocker}")

        if args.finalize:
            if ring_member is not None:
                print(f"\n[ERROR] {target} is still a ring member. --finalize is the "
                      "bookkeeping that follows the ring removal, not a substitute for it.")
                sys.exit(1)
            if blockers:
                print("\n[ERROR] Refusing to finalize while the checks above are unresolved.")
                sys.exit(1)

            print("\n--- Finalizing: removing the node from cluster metadata ---")
            config = cluster_hosts_config()
            if config:
                remaining_hosts = [h for h in config.get("hosts", []) if h.get("ip") != target]
                if len(remaining_hosts) != len(config.get("hosts", [])):
                    # Renumber, because node_id is an index other tooling counts on -- the
                    # witness node in a three node layout is identified by position.
                    for index, host in enumerate(remaining_hosts):
                        host["node_id"] = index + 1
                    config["hosts"] = remaining_hosts
                    failed = write_cluster_config(survivors, config)
                    if failed:
                        print(f"[WARNING] Could not update /etc/hci/cluster.json on: {', '.join(failed)}")
                    else:
                        print(f"Updated /etc/hci/cluster.json on {len(survivors)} node(s).")
                else:
                    print("/etc/hci/cluster.json does not list this node; nothing to change.")
            else:
                print("[WARNING] /etc/hci/cluster.json could not be read; skipped.")

            # hydra.nodes is keyed by hostname, and CQL has no DELETE on a non-key column,
            # so the row is looked up by address first.
            rc_r, stdout_r, _ = run_cql_query(
                f"SELECT JSON hostname FROM hydra.nodes WHERE ip = '{target}' ALLOW FILTERING;")
            removed = []
            if rc_r == 0 and stdout_r:
                for line in stdout_r.splitlines():
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        try:
                            hostname = json.loads(line).get("hostname")
                        except Exception:
                            continue
                        if hostname:
                            run_cql_query(f"DELETE FROM hydra.nodes WHERE hostname = '{hostname}';")
                            removed.append(hostname)
            if removed:
                print(f"Removed hydra.nodes row(s): {', '.join(removed)}")
            else:
                print("No hydra.nodes row referenced this address.")

            print("\nStill manual, and deliberately so:")
            print(f"  - Storage: nothing. A removed node needs no deregistration, and Purah")
            print(f"    re-replicates whatever it held onto a surviving node. Confirm that")
            print(f"    finished before wiping it -- a node still holding the only copy of a")
            print(f"    vdisk loses that vdisk.")
            print(f"  - ZooKeeper: remove the node from the ensemble configuration on every")
            print(f"    remaining host and restart them one at a time. A voter that is gone")
            print(f"    still counts toward the ensemble's quorum until it is removed.")
            print("\nDecommission bookkeeping complete.")
        else:
            print("\n--- Decommission sequence ---")
            print(f"  1. Drain {target}: put it in maintenance mode so its VMs migrate off.")
            print(f"     The quorum gate refuses this if the cluster cannot spare the replica,")
            print(f"     which is the same condition that makes step 4 unsafe.")
            print(f"  2. Storage: Purah moves its replicas to surviving nodes on its own. Watch")
            print(f"     'valcli storage.list' until no vdisk shows a short replica set.")
            if replication_factor is not None and ring_member is not None:
                remaining = len(members) - 1
                if min(replication_factor, remaining) < quorum_of(replication_factor):
                    print(f"  3. Lower the keyspace replication factor to at most {remaining}:")
                    # The datacenter is left as a placeholder rather than guessed: the
                    # operator running this is at a cqlsh prompt and can read it from
                    # `nodetool status`, and printing the wrong one produces a keyspace
                    # with replicas in a datacenter that has no nodes.
                    print(f"     ALTER KEYSPACE hydra WITH replication = {{'class': 'NetworkTopologyStrategy',")
                    print(f"       '<datacenter>': {remaining}}};   -- datacenter per 'nodetool status'")
                    print(f"     then 'nodetool repair -pr hydra' on every remaining node.")
                else:
                    print(f"  3. Replication factor {replication_factor} still fits a "
                          f"{remaining}-node ring; no ALTER KEYSPACE needed.")
            else:
                print("  3. Check the keyspace replication factor still fits the smaller ring.")
            if ring_member is not None and ring_member["available"]:
                print(f"  4. ON {target}: 'nodetool decommission'. It streams every range it")
                print(f"     owns to the remaining replicas and can run for hours. Watch it;")
                print(f"     do not interrupt it and do not run it twice.")
            elif ring_member is not None:
                print(f"  4. ON A SURVIVING NODE: 'nodetool removenode "
                      f"{ring_member['host_id'] or '<host-id>'}'. Use this rather than")
                print(f"     decommission because {target} is not running.")
            else:
                print(f"  4. (already done -- {target} is not in the ring)")
            print(f"  5. 'cluster decommission --node {target} --finalize' to clear its")
            print(f"     cluster.json entry and its hydra.nodes row.")
            print(f"  6. ZooKeeper ensemble reconfiguration, by hand.")
            if blockers:
                print("\n[ERROR] The blockers above must be resolved before step 4.")
                sys.exit(1)

    elif args.command == "rejoin":
        # Bringing a node back. The dangerous half here is not the ring operation, it is
        # the data the node still has on disk: a node that was decommissioned and then
        # started again with its old commitlog and sstables either refuses to start or
        # re-introduces rows that were deleted while it was away, because its tombstones
        # are older than gc_grace and its data is not. Scylla cannot tell the difference.
        if not args.node:
            parser.error("rejoin requires --node <ip>")
        target = args.node.strip()
        ips = get_cluster_ips()
        survivors = [ip for ip in ips if ip != target]

        print("==========================================================")
        print(f"   Rejoin preflight for {target}")
        print("==========================================================")

        rc, out, err = run_remote_spark(target, "echo online")
        if rc != 0 or "online" not in (out or "").lower():
            print(f"[ERROR] spark-daemon on {target} is not answering: {(err or out or '').strip()[:200]}")
            print("[ERROR] The node has to be reachable before it can be brought back.")
            sys.exit(1)
        print(f"[{target}] spark-daemon is online.")

        replication_factor = get_hydra_replication_factor()
        members, error = read_ring(survivors or ips)
        if error:
            print(f"[ERROR] Could not read the ScyllaDB ring: {error}")
            sys.exit(1)
        print("\nScyllaDB ring:")
        print(render_ring(members, replication_factor))

        ring_member = next((m for m in members if m["address"] == target), None)
        in_config = target in ips

        # Does it still carry data from its previous life in the ring?
        rc_d, stdout_d, _ = run_remote_spark(
            target, "ls -A /var/lib/hci/hydra/data/data 2>/dev/null | head -5")
        has_old_data = rc_d == 0 and bool((stdout_d or "").strip())

        print()
        if ring_member is not None and ring_member["available"]:
            print(f"[NOTE] {target} is already a live ring member "
                  f"('{ring_member['status']}{ring_member['state']}'). Nothing to rejoin.")
        elif ring_member is not None:
            print(f"[NOTE] {target} is in the ring but reported "
                  f"'{ring_member['status']}{ring_member['state']}'. This is a node that never "
                  "left, so it does not need to rejoin -- start hydra-db on it and let it "
                  "catch up, then repair.")
        else:
            print(f"[NOTE] {target} is not in the ring, so it joins as a new member and "
                  "bootstraps its ranges from the seeds.")
            if has_old_data:
                print(f"[BLOCKED] {target} still has ScyllaDB data under "
                      "/var/lib/hci/hydra/data. A node that left the ring must not rejoin "
                      "carrying it: rows deleted cluster-wide while it was away have "
                      "tombstones the returning node never saw, and bootstrapping on top of "
                      "its old sstables resurrects them. Wipe the directory first.")

        if not in_config:
            print(f"[NOTE] {target} is not listed in /etc/hci/cluster.json; --finalize adds it.")

        if args.finalize:
            print("\n--- Finalizing: restoring the node's cluster metadata ---")
            config = cluster_hosts_config()
            if config is None:
                print("[ERROR] /etc/hci/cluster.json could not be read.")
                sys.exit(1)

            if not in_config:
                rc_h, hostname, _ = run_remote_spark(target, "hostname")
                hostname = (hostname or "").strip()
                if rc_h != 0 or not hostname:
                    print(f"[ERROR] Could not resolve the hostname of {target}.")
                    sys.exit(1)
                hosts = list(config.get("hosts", []))
                hosts.append({"node_id": len(hosts) + 1, "ip": target, "hostname": hostname})
                config["hosts"] = hosts
                failed = write_cluster_config([h["ip"] for h in hosts], config)
                if failed:
                    print(f"[WARNING] Could not update /etc/hci/cluster.json on: {', '.join(failed)}")
                else:
                    print(f"Restored {target} ({hostname}) to /etc/hci/cluster.json on "
                          f"{len(hosts)} node(s).")
            else:
                print("/etc/hci/cluster.json already lists this node.")

            if ring_member is not None and ring_member["available"]:
                # Only once it is genuinely serving. Registering it as NORMAL while it is
                # still bootstrapping hands it VMs it cannot run.
                entry = next((h for h in config.get("hosts", []) if h.get("ip") == target), None)
                hostname = (entry or {}).get("hostname", "")
                if hostname:
                    run_cql_query(
                        "INSERT INTO hydra.nodes (hostname, ip, status, maintenance_mode) "
                        f"VALUES ('{hostname}', '{target}', 'NORMAL', false);")
                    print(f"Registered {hostname} ({target}) in hydra.nodes as NORMAL.")
            else:
                print(f"[NOTE] {target} is not yet up and normal in the ring, so it was NOT "
                      "registered as a schedulable host. Re-run --finalize once "
                      "'cluster ring' shows it UN.")

            print("\nStill manual, and deliberately so:")
            print("  - 'nodetool repair -pr hydra' on every node once the join completes.")
            print("    Bootstrapping streams the ranges the new node now owns; it does not")
            print("    reconcile what the survivors wrote while it was gone.")
            print("  - Raising the keyspace replication factor back, if it was lowered for")
            print("    the smaller ring. ALTER KEYSPACE changes the strategy only -- the data")
            print("    is not copied to the new replicas until a repair runs.")
            print("  - Storage: nothing to re-create. Purah replicates onto the returning")
            print("    node as it becomes the spare for anything short of its replica count.")
        else:
            print("\n--- Rejoin sequence ---")
            print(f"  1. Confirm {target} is meant to come back as the same node. If it was")
            print(f"     decommissioned, wipe /var/lib/hci/hydra/data before anything else.")
            print(f"  2. 'cluster rejoin --node {target} --finalize' to restore its")
            print(f"     /etc/hci/cluster.json entry on every node, so the seeds are right.")
            print(f"  3. ON {target}: 'systemctl start zookeeper hydra-db'. It bootstraps into")
            print(f"     the ring by streaming from the seeds; watch 'cluster ring' until it")
            print(f"     reports UN. A node stuck at UJ is still streaming, not broken.")
            print(f"  4. Raise the replication factor back if it was lowered, then")
            print(f"     'nodetool repair -pr hydra' on every node.")
            print(f"  5. Storage: Purah restores replica counts in the background; no resync to wait for.")
            print(f"  6. 'cluster rejoin --node {target} --finalize' again to register it in")
            print(f"     hydra.nodes as a schedulable host, then 'cluster start'.")

if __name__ == "__main__":
    main()
