#!/usr/bin/env python3
import sys
import argparse
import json
import shlex
import re
import ssl
import urllib.request
import urllib.error
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
    default_metadata_replication_factor,
    is_conditional_cql,
    metadata_replication_factor,
    parse_replication_factor,
    run_conditional_cql_query,
    run_cql_query,
    two_replica_warning,
)

# Carve the extent store's first volume out of the thin pool, give it a filesystem, register
# every empty disk and record which filesystems are sidon's. Provisioning does the same for a
# node being built; this is the copy `cluster create` runs, because `cluster destroy` removes
# the volume group and a create that only re-claimed the first disk left a node with a thin
# pool and no volume for sidon to use. Nothing here mounts anything or writes /etc/fstab:
# sidon mounts what /etc/hci/sidon-disks names, when it starts (D-27). The three scripts are
# identical in every file that has them; test_multi_disk.py asserts so.
CARVE_SIDON_VOLUME = r"""
set -e
lvs vg_aether/sidon >/dev/null 2>&1 || lvcreate -y -V 150G --thinpool thin_pool_aether -n sidon vg_aether
blkid /dev/vg_aether/sidon >/dev/null 2>&1 || mkfs.xfs -q /dev/vg_aether/sidon
"""

CLAIM_EXTRA_DISKS = r"""
set -e
MANIFEST=/etc/hci/sidon-disks
mkdir -p /etc/hci
claimed_pvs="$(pvs --noheadings -o pv_name 2>/dev/null | tr -d ' ' | tr '\n' ' ')"
added=0
while read -r name size type _rest; do
    [ "$type" = "disk" ] || continue
    dev="/dev/$name"
    case " $claimed_pvs " in *" $dev "*) continue ;; esac
    if lsblk -n -o MOUNTPOINT "$dev" | grep -qE '[^[:space:]-]'; then continue; fi
    if lsblk -n -o TYPE "$dev" | grep -qx part; then continue; fi
    [ "$size" -ge 100000000000 ] || continue

    if ! blkid "$dev" >/dev/null 2>&1; then
        mkfs.xfs -q "$dev"
        echo "formatted $dev"
    fi
    # Only an XFS filesystem is an extent store. Anything else already on the disk is
    # somebody's data and is left exactly as it is.
    fstype="$(blkid -s TYPE -o value "$dev")"
    if [ "$fstype" != "xfs" ]; then
        echo "skipped $dev: it carries a $fstype signature, which is not an extent store"
        continue
    fi

    # Registered, and nothing more. The disk is not mounted from here and nothing is
    # written to /etc/fstab: a line there is what let one late disk fail local-fs.target
    # and take a node to a console with no network. Sidon mounts what the manifest names,
    # by filesystem UUID, at its next start, under /var/lib/hci/sidon/disks/<uuid>, and
    # refuses any path whose disk is not there.
    uuid="$(blkid -s UUID -o value "$dev")"
    if ! grep -qs "^$uuid[[:space:]]" "$MANIFEST"; then
        echo "$uuid extent" >> "$MANIFEST"
    fi
    echo "extent store disk: $dev ($uuid) registered in $MANIFEST; sidon mounts it at its next start"
    added=$((added + 1))
done <<EOF
$(lsblk -b -n -o NAME,SIZE,TYPE)
EOF
if [ "$added" -eq 0 ]; then
    echo "no additional empty disk to claim"
fi
"""

STAGE_SIDON_DISKS = r"""
set -e
MANIFEST=/etc/hci/sidon-disks
FSTAB=/etc/fstab
ROOT=/var/lib/hci/sidon
mkdir -p /etc/hci
raw="$(mktemp)"
want="$(mktemp)"
trap 'rm -f "$raw" "$want"' EXIT

# What sidon is to mount, in priority order: the volume this toolkit carves, what the
# manifest already says, and what a node built before the manifest declared in fstab. The
# fstab is only read here, never edited: removing a mount's line is safe, but the mount
# itself is moved by sidon when it next starts and not by a rollout, because moving a
# mount under a running sidon is the thing this must never do.
lv="$(blkid -s UUID -o value /dev/vg_aether/sidon 2>/dev/null || true)"
[ -z "$lv" ] || echo "$lv journal" >> "$raw"
[ ! -f "$MANIFEST" ] || awk '!/^[[:space:]]*#/ && NF >= 2 { print $1, $2 }' "$MANIFEST" >> "$raw"
if [ -f "$FSTAB" ]; then
    awk -v root="$ROOT" '
        /^[[:space:]]*#/ { next }
        NF < 2 { next }
        $2 == root { print $1, "journal"; next }
        index($2, root "/") == 1 { print $1, "extent" }
    ' "$FSTAB" | while read -r spec role; do
        case "$spec" in
            UUID=*) uuid="${spec#UUID=}" ;;
            /dev/*) uuid="$(blkid -s UUID -o value "$spec" 2>/dev/null || true)" ;;
            *) uuid="" ;;
        esac
        [ -z "$uuid" ] || echo "$uuid $role"
    done >> "$raw"
fi

# One line per filesystem, the journal volume first. Two sources disagreeing about which is
# the journal volume are settled in the priority order above, and the loser is kept as an
# extent disk rather than dropped, so a disagreement shows up as a disk that is absent and
# reported instead of one that silently stopped being part of the store.
{
    echo "# <filesystem-uuid> <journal|extent>: the filesystems sidon mounts itself, under"
    echo "# $ROOT/disks/<uuid>. Nothing sidon owns is in /etc/fstab. See docs/dfs/multi_disk.md."
    awk '
        $1 !~ /^[A-Za-z0-9-]+$/ { next }
        $2 != "journal" && $2 != "extent" { next }
        !($1 in seen) { seen[$1] = ++n; order[n] = $1 }
        $2 == "journal" { claims[$1] = 1 }
        END {
            for (i = 1; i <= n; i++) if (claims[order[i]]) { j = order[i]; break }
            if (j != "") print j, "journal"
            for (i = 1; i <= n; i++) if (order[i] != j) print order[i], "extent"
        }
    ' "$raw"
} > "$want"

if ! grep -q ' journal$' "$want"; then
    echo "sidon disks: no journal volume found on this node; manifest left alone"
elif [ -f "$MANIFEST" ] && cmp -s "$want" "$MANIFEST"; then
    echo "sidon disks: ok"
else
    cat "$want" > "$MANIFEST"
    chmod 0644 "$MANIFEST"
    echo "sidon disks: manifest written ($(grep -vc '^#' "$MANIFEST") filesystems); sidon mounts them at its next start"
fi

# The socket directory libvirt's domain XML names. A plain directory on the root filesystem
# in the new layout, and already present inside the volume in the old one.
mkdir -p "$ROOT/nbd"
if chgrp qemu "$ROOT/nbd" 2>/dev/null; then chmod 0750 "$ROOT/nbd"; fi
"""

# Take off every mount under the sidon root and forget which filesystems are sidon's. For
# `cluster destroy`, with sidon already stopped: lazy, because a destroy is meant to leave
# the disks free to be wiped whatever still has them open. Repeated, because a mount that a
# later mount covered is only revealed once the cover is gone. Deepest first. A sidon line in
# an older node's fstab goes too, and the old file is only replaced by a non-empty one.
SIDON_TEARDOWN = (
    "for i in 1 2 3 4 5; do "
    "awk -v r=/var/lib/hci/sidon '$5 == r || index($5, r \"/\") == 1 { print $5 }' /proc/self/mountinfo "
    "| sort -r | while read -r m; do umount -l \"$m\" 2>/dev/null; done; "
    "done; "
    "rm -f /etc/hci/sidon-disks; "
    "if [ -f /etc/fstab ] && awk -v r=/var/lib/hci/sidon '$2 != r && index($2, r \"/\") != 1' /etc/fstab > /etc/fstab.hci-new "
    "&& [ -s /etc/fstab.hci-new ]; then cat /etc/fstab.hci-new > /etc/fstab; fi; "
    "rm -f /etc/fstab.hci-new"
)


def shell_script_command(script):
    """A multi-line script as one command, so no transport has to quote it."""
    return "echo " + base64.b64encode(script.encode("utf-8")).decode("ascii") + " | base64 -d | bash"


def run_parallel(ips, cmd, timeout=None, label=None, heartbeat=10, now=time.time):
    """Run `cmd` on every host at once and return {ip: (rc, stdout, stderr)}.

    With a `label` it also says what it is doing while it does it: the label up front, a line as
    each host finishes with how long that host took, and -- every `heartbeat` seconds, for as long
    as anything is still running -- which hosts it is still waiting on. Without a label it is
    silent, as it always was, so quick commands do not chatter.

    The silence is the reason this exists. A step like preparing a disk takes a minute or more,
    and a console that prints "Scanning and setting up storage pools..." and then nothing for a
    minute is indistinguishable from a hung one; an operator in that position cannot tell whether
    to wait or to interrupt, and the honest answer is rarely "interrupt".
    """
    results = {}
    threads = []
    started = now()
    printing = threading.Lock()

    def worker(ip):
        rc, stdout, stderr = run_remote_spark(ip, cmd, timeout=timeout)
        results[ip] = (rc, stdout, stderr)
        if label:
            with printing:
                print(f"[{ip}] {label}: {'done' if rc == 0 else 'FAILED'} in "
                      f"{int(now() - started)}s", flush=True)

    if label:
        print(f"{label} on {len(ips)} node(s)...", flush=True)
    for ip in ips:
        t = threading.Thread(target=worker, args=(ip,))
        threads.append(t)
        t.start()

    next_beat = started + heartbeat
    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(timeout=0.2)
        if label and now() >= next_beat:
            waiting = [ip for ip in ips if ip not in results]
            if waiting:
                with printing:
                    print(f"  ... {label}: still working on {', '.join(waiting)} "
                          f"({int(now() - started)}s elapsed)", flush=True)
            next_beat += heartbeat
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

SERVICE_DISPLAY_ORDER = ["ZooKeeper", "HydraDB", "Daruk", "Sidon", "Spark", "Spectrum", "Phoenix",
                         "Bifrost", "Dagur", "Mimir", "Rauru", "Vali", "Catalyst", "Hylia",
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
        # The node publishes why a service is not where it was asked to be, so the reason
        # is printed next to the service rather than left in a journal on that host.
        error = str(svc.get("last_error") or "").strip().splitlines()
        err_str = f" {r}{error[0][:160]}{x}" if error else ""
        # In maintenance, a unit that is up either stays up on purpose (the node says which) or
        # is something the maintenance sequence should have stopped, and neither is left for the
        # reader to work out.
        maint_note = ""
        if maint != "NORMAL" and status == "UP":
            maint_note = (f" {gr}(kept up in maintenance){x}" if svc.get("kept_in_maintenance")
                          else f" {y}(expected to be stopped in maintenance){x}")
        if status == "UP":
            note = f" {y}({restarts} restarts){x}" if restarts else ""
            lines.append(f"                    {name:<16}   {g}UP{x}       {pid_str}{note}{maint_note}{err_str}")
        elif status == "FLAPPING":
            lines.append(f"                    {name:<16}   {y}FLAPPING{x} {gr}restarting, {restarts} restarts{x}{err_str}")
        else:
            note = f" {gr}({restarts} restarts){x}" if restarts else ""
            lines.append(f"                    {name:<16}   {r}DOWN{x}{note}{err_str}")
    # A service that is declared but switched off is reported as such. Omitting it would
    # leave an operator unable to tell "disabled" from "nobody ever deployed it".
    for name in data.get("disabled_services") or []:
        lines.append(f"                    {name:<16}   {gr}DISABLED{x}")
    return "\n".join(lines)


# The reported services a start or a stop is waiting on.
#
# ZooKeeper and Spark are reported by every node but are deliberately not waited on: the
# store that holds the desired state and the daemon that converges toward it are both
# running throughout, so a stop that waited for their PIDs to go away would wait forever.
# Urbosa stays in the list -- a node that has it switched off reports it as disabled
# instead of reporting it here, and a disabled service is not something to wait for.
EXPECTED_SERVICES = [s for s in SERVICE_DISPLAY_ORDER if s not in ("ZooKeeper", "Spark")]

# How many consecutive passes a node may report nothing before the wait stops counting on
# it. A node that is down is not a reason to abandon an operation the rest of the cluster
# is completing: it converges when it returns, because the desired state outlives it.
UNREPORTED_NODE_ATTEMPTS = 10


def service_is_compliant(svc, op):
    """Compliance is PIDs, and it is symmetric.

    A started service has processes; a stopped one does not. Not `status`: `active` is
    what a unit reports during each restart window of a crash loop, so a service that has
    never once stayed up answers "started" as often as not. A PID list is a fact.
    """
    pids = svc.get("pids") or []
    return bool(pids) if op == "start" else not pids


def published_service_errors(nodes, ips):
    """Every (node, service, error) the cluster is currently publishing."""
    found = []
    for ip in sorted(ips):
        services = (nodes.get(ip) or {}).get("services", {})
        for name in SERVICE_DISPLAY_ORDER:
            error = str((services.get(name) or {}).get("last_error") or "").strip()
            if error:
                found.append((ip, name, error.splitlines()[0]))
    return found


def print_cluster_table(nodes, ips):
    """The published state of every node, rendered the way `cluster status` renders it."""
    for ip in sorted(ips):
        if ip in nodes:
            print(render_node_block(ip, nodes[ip]))
        else:
            print(f"\n        Host: {BOLD}{ip}{RESET} {RED}Down{RESET} {GRAY}(no ZooKeeper registration){RESET}")


def wait_until_error_or_done(expected_ips, op="start", timeout=600, poll=3):
    """Watch the cluster converge toward the state that was declared, or stop at an error.

    The CLI issues no service commands: it declared an intent, and each node's reconcile
    loop decides what to start or stop, in what order, and when it is finished. So this is
    the whole of `cluster start` after the declaration -- it observes, it does not drive.

    Three things it does not do:

      * it does not decide convergence for itself. Each node publishes `retry`, its own
        answer to "am I done?", and this loops on that as well as on compliance;
      * it does not read a journal. A service that will not come up publishes
        `last_error`, and one of those **aborts the wait immediately** and prints the full
        table -- waiting out a timeout to then say nothing useful is the failure mode this
        replaces;
      * it does not give up on an unreachable node. That node is retried a bounded number
        of times and then reported, because the desired state outlives it.
    """
    deadline = time.time() + timeout
    attempts = {ip: 0 for ip in expected_ips}
    waiting = set(expected_ips)
    last_line = None
    if poll > 0:
        time.sleep(poll)
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
        failures = published_service_errors(nodes, waiting)
        if failures:
            print(f"  {RED}Convergence stopped: a node is reporting a service error.{RESET}")
            for ip, name, error in failures:
                print(f"  {RED}[{ip}] {name}: {error}{RESET}")
            print_cluster_table(nodes, expected_ips)
            return False

        pending = {}
        unreported = []
        node_retry = False
        for ip in sorted(waiting):
            data = nodes.get(ip)
            if not data:
                attempts[ip] += 1
                if attempts[ip] >= UNREPORTED_NODE_ATTEMPTS:
                    print(f"  {YELLOW}{ip} has not reported in {attempts[ip]} attempts; "
                          f"continuing without it.{RESET}")
                    waiting.discard(ip)
                else:
                    unreported.append(ip)
                continue
            # A host in maintenance is published as such and its reconcile loop refuses to
            # act on the desired state, so waiting for it to converge would wait forever.
            # The CLI used to ask every node for /etc/hci/maintenance.state over mTLS
            # before starting anything; the node already says so in what it publishes.
            if data.get("maintenance_status", "NORMAL") != "NORMAL":
                print(f"  {YELLOW}{ip} is in maintenance; its services are left as they "
                      f"are.{RESET}")
                waiting.discard(ip)
                continue
            services = data.get("services", {})
            not_compliant = [n for n in EXPECTED_SERVICES
                             if n in services and not service_is_compliant(services[n], op)]
            if not_compliant:
                pending[ip] = not_compliant
            if data.get("retry"):
                node_retry = True

        if not unreported and not pending and not node_retry:
            print(f"  {GREEN}All nodes report the cluster {'started' if op == 'start' else 'stopped'}.{RESET}")
            return True

        parts = []
        if unreported:
            parts.append("nodes not reporting: " + ", ".join(sorted(unreported)))
        for ip in sorted(pending):
            parts.append(f"{ip}: {', '.join(pending[ip])}")
        line = "Waiting for " + ("; ".join(parts) or "nodes to finish converging")
        if line != last_line:
            print(f"  {line}")
            last_line = line
        time.sleep(poll)

    print(f"  {YELLOW}Timed out after {timeout}s waiting for convergence.{RESET}")
    return False


def declare_cluster_state(ips, desired, stop_state_store=False):
    """Declare the desired cluster state on every reachable node. Returns the ones that
    took it.

    One call per node and no service list: the node starts the store the state lives in,
    writes the state, and its reconcile loop does the rest. A node that cannot be reached
    is reported and skipped rather than failing the operation -- the state is cluster-wide,
    so one node recording it is enough for every node to read it.
    """
    payload = {"desired": desired}
    if stop_state_store:
        payload["stop_state_store"] = True
    declared = []
    for ip in ips:
        status, body, error = run_mtls_spark_api_full(
            ip, "/api/v1/cluster/state", payload, timeout=120)
        if status == 200:
            declared.append(ip)
        else:
            print(f"  {YELLOW}[{ip}] did not record the desired state: "
                  f"{spark_api_error(status, body, error)}{RESET}")
    return declared


# How long guests are given to shut down on their own before they are powered off. One window for
# all of them together: it used to be five seconds *each*, one guest after another, so a dozen
# guests that ignored the request cost a minute while a guest that needed ten seconds to shut
# down cleanly never got them.
VM_SHUTDOWN_GRACE_SECONDS = 20


def stop_vms_together(running_vms, grace=VM_SHUTDOWN_GRACE_SECONDS, runner=None, sleep=None,
                      update_row=None):
    """Shut down every running guest at once, then power off whatever has not gone.

    A guest is cluster state and not node state, so this runs from the CLI once. Every
    guest is asked to shut down at the same time and polled together against one shared
    deadline; the ones still running when it passes are destroyed together; and each one's
    row is then marked stopped and unplaced. Returns the names of the guests that had to be
    powered off.
    """
    import concurrent.futures

    runner = runner or run_remote_spark
    sleep = sleep or time.sleep
    update_row = update_row or (lambda name: run_cql_query(
        "UPDATE hydra.vms SET state = 'Stopped', host_ip = '' WHERE name = '%s';"
        % name.replace("'", "''")))

    guests = []
    for vm in running_vms:
        name, host_ip = vm.get("name"), vm.get("host_ip")
        if not name or not host_ip or host_ip == "N/A":
            continue
        guests.append((name, host_ip))
    if not guests:
        return []

    def each(fn, items):
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(items))) as pool:
            return list(pool.map(fn, items))

    for name, host_ip in guests:
        print(f"Stopping VM '{name}' on host {host_ip}...")
    each(lambda g: runner(g[1], f"virsh shutdown {shlex.quote(g[0])}"), guests)

    deadline = time.time() + grace
    remaining = list(guests)
    while remaining and time.time() < deadline:
        sleep(1)
        states = each(lambda g: runner(g[1], f"virsh domstate {shlex.quote(g[0])}"), remaining)
        remaining = [g for g, (rc, out, _) in zip(remaining, states)
                     if not (rc == 0 and "shut off" in out.lower())]

    if remaining:
        for name, _ in remaining:
            print(f"VM '{name}' did not shut down gracefully. Forcing power off (destroy)...")
        each(lambda g: runner(g[1], f"virsh destroy {shlex.quote(g[0])}"), remaining)
    for name, _ in guests:
        update_row(name)
    return [name for name, _ in remaining]


def get_cluster_ips():
    try:
        with open("/etc/hci/cluster.json", "r") as f:
            cdata = json.load(f)
            return [h["ip"] for h in cdata.get("hosts", [])]
    except Exception:
        return ["127.0.0.1"]


def configured_cluster_ips(path="/etc/hci/cluster.json"):
    """The hosts this node's cluster consists of, or None when no cluster is configured.

    get_cluster_ips() answers the same question with a silent fallback to 127.0.0.1, which
    other commands lean on and which is wrong for `status`: asked about a cluster that does
    not exist, it invented a one-host cluster and printed every service on it as DOWN. That
    is a report about a cluster that was never there. "Nothing is configured" and "a
    configured cluster is down" are different situations that need different answers, and
    only the second one has a table to print.
    """
    try:
        with open(path, "r") as handle:
            hosts = [h["ip"] for h in json.load(handle).get("hosts", []) if h.get("ip")]
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None
    return hosts or None


def print_no_cluster(as_json=False):
    """Say there is no cluster, and how to make one. Nothing is probed to say it."""
    if as_json:
        print(json.dumps({"cluster_state": "not_configured", "source": "local",
                          "nodes": {}}, indent=2))
        return
    print("==========================================================")
    print("                 HCI Cluster Status                       ")
    print("==========================================================")
    print("No cluster is configured on this node.")
    print("")
    print(f"{GRAY}/etc/hci/cluster.json is absent or lists no hosts, so there is nothing to ask")
    print(f"about: either no cluster has been created here, or it was destroyed.{RESET}")
    print("")
    print("Create one with:")
    print("    cluster -s <ip1,ip2,ip3> -r 1 -v <vip> create")
    print("==========================================================")


def describe_sidon_failure(ip, runner=None):
    """What sidon on `ip` actually says, for a failure message that reports evidence.

    The check this serves used to print "sidon refuses to start while its journal volume is not
    mounted" whenever the control socket did not answer. That is one possible cause stated as the
    only one, and on the run that prompted this change it was wrong: sidon was up, both of its
    volumes mounted, and the socket simply was not there yet. A message that guesses sends an
    operator after the wrong fault. This asks the node.
    """
    runner = runner or run_remote_spark
    probe = ("echo \"unit: $(systemctl is-active sidon 2>&1)  restarts: "
             "$(systemctl show -p NRestarts --value sidon 2>&1)\"; "
             "echo '--- journal ---'; journalctl -u sidon --no-pager -n 8 2>&1 | cut -c1-200; "
             "echo '--- sidon mounts ---'; /usr/local/bin/sidon mounts 2>&1 | head -12")
    rc, out, err = runner(ip, probe)
    text = (out or err or "no answer from the node").strip()
    return "\n".join("        " + line for line in text.splitlines())


def wait_for_sidon_capacity(ip, runner=None, timeout=90, interval=2, sleep=time.sleep,
                            now=time.time, say=print):
    """Ask sidon for its capacity, waiting for it to come up. Returns (rc, stdout, stderr).

    `systemctl restart` returns once the process has been started, not once it is serving, and
    sidon creates its control socket only after it has mounted its disks and read its peer list.
    Probing the instant the restart returns is a race that the daemon usually loses by a moment,
    so a single probe reported "No such file or directory" for a sidon that was fine. This keeps
    asking, says so now and then so the wait does not look like a hang, and gives up only after
    `timeout` seconds -- long enough for a slow mount, short enough that a real failure is not
    left to sit.
    """
    runner = runner or run_remote_spark
    started = now()
    last_said = started
    while True:
        rc, out, err = runner(ip, SIDON_CAPACITY_CMD)
        if rc == 0 and (out or "").strip():
            return rc, out, err
        waited = now() - started
        if waited >= timeout:
            return rc, out, err
        if now() - last_said >= 10:
            say(f"[{ip}] still waiting for sidon to answer ({int(waited)}s)...")
            last_said = now()
        sleep(interval)


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


# How long a storage-preparation command may run on a node.
#
# spark-daemon's /api/v1/execute applies 45 seconds when the caller names no timeout, and every
# command here used to go through that default. Preparing a disk is not a 45 second job: the
# claim zeroes a gigabyte at each end of the disk, then builds a physical volume, a volume group
# and a thin pool, and formatting a volume comes after. On this cluster's virtual disks that is
# right at the limit -- one node finished in time, the daemon killed the command on the other
# two -- and `cluster create` stopped with "Command timed out after 45 seconds" on a node whose
# work had in fact completed. A timeout is a ceiling on a hung command, not a budget for a
# healthy one, so it is generous.
STORAGE_PREP_TIMEOUT = 900

def spark_client_material():
    """(ca, client certificate, client key) for talking to spark-daemon, or None for each that
    is absent. `HCI_CERT_DIR` relocates the directory (for running the CLI away from a node);
    the default is where provisioning puts them."""
    directory = os.environ.get("HCI_CERT_DIR") or "/root/.certs"
    found = []
    for name in ("ca.crt", "client.crt", "client.key"):
        path = os.path.join(directory, name)
        found.append(path if os.path.exists(path) else None)
    return tuple(found)


def spark_client_context(ip):
    """A TLS context that verifies the daemon it is about to call, and the address to call.

    Raises ValueError, saying what is missing, when the cluster CA is not there: the previous
    behaviour was to connect without verifying anything, which let any host that answered on
    9099 receive a root-capable command and return whatever output it liked. Verification is
    the same as every other client's (`spark_endpoint`): the node certificate names the node's
    IP, so addressing the node by that IP is what ties the connection to it.
    """
    ca_path, cert_path, key_path = spark_client_material()
    if not ca_path:
        raise ValueError("the cluster CA certificate is not at %s; run this on a cluster node, "
                         "or point HCI_CERT_DIR at a directory holding ca.crt, client.crt and "
                         "client.key" % os.path.join(os.environ.get("HCI_CERT_DIR") or "/root/.certs", "ca.crt"))
    address, verify_identity = spark_endpoint(ip)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_path)
    context.check_hostname = verify_identity
    if cert_path and key_path:
        context.load_cert_chain(certfile=cert_path, keyfile=key_path)
    return context, address


def run_remote_spark(ip, command, timeout=None):
    try:
        context, ip = spark_client_context(ip)
    except (ValueError, OSError, ssl.SSLError) as e:
        return -1, "", str(e)

    url = f"https://{ip}:9099/api/v1/execute"
    body = {"command": command}
    wait = 120
    if timeout is not None:
        # The daemon runs the command under this limit; without one it applies 45 seconds,
        # which is why a long command used to be killed whatever the client was prepared to
        # wait for. The client waits a little longer than the command is allowed to run.
        body["timeout"] = timeout
        wait = max(wait, timeout + 30)
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=wait) as response:
            res = json.loads(response.read().decode("utf-8"))
            return res["returncode"], res["stdout"], res["stderr"]
    except Exception as e:
        return -1, "", str(e)


def run_mtls_spark_api_full(ip, path, payload=None, method="POST", timeout=120):
    """Call a typed spark-daemon endpoint, keeping the status code and the 4xx body.

    The same connection to the same daemon as run_remote_spark -- the same certificate
    search, the same context -- differing only in which endpoint it reaches. The status
    and the body are both kept because the typed API answers a refused parameter with
    400 and a message naming it, and a refused parameter here means *this* call site is
    wrong: `systemctl restart aether` was a shell string that failed silently as an
    unknown unit for months, and "aether is not a unit this cluster manages" is the
    sentence that would have ended it.

    Returns (status, body, error). status is 0 when the request could not be made.
    """
    try:
        context, ip = spark_client_context(ip)
    except (ValueError, OSError, ssl.SSLError) as e:
        return 0, {}, str(e)

    url = f"https://{ip}:9099{path}"
    data = None
    if payload is not None and method != "GET":
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=context, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8")), ""
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8")), ""
        except Exception:
            return exc.code, {}, str(exc)
    except Exception as e:
        return 0, {}, str(e)


def spark_api_error(status, body, error):
    """The one sentence worth printing about a failed typed call."""
    if error:
        return error
    detail = body.get("error") if isinstance(body, dict) else None
    return str(detail or "spark-daemon answered %s" % status)


def unit_action(ip, action, units=None, detach=False, ignore_failed=False):
    """Act on systemd units on one node. Returns (ok, detail).

    Replaces `run_remote_spark(ip, "systemctl <verb> <names>")`. The unit names are
    parameters the far side checks against its own allow-list, so a name this cluster
    does not manage is refused with a message instead of being run as root.

    `ignore_failed` is what those shell strings wrote as `|| true`, and `detach` is what
    they wrote as `(sleep 1 && ...) >/dev/null 2>&1 < /dev/null &`.
    """
    payload = {"action": action}
    if units:
        payload["units"] = list(units)
    if detach:
        payload["detach"] = True
    if ignore_failed:
        payload["ignore_failed"] = True
    status, body, error = run_mtls_spark_api_full(ip, "/api/v1/host/units", payload)
    if status == 200:
        return True, ""
    return False, spark_api_error(status, body, error)


def unit_action_parallel(ip_list, action, units=None, detach=False, ignore_failed=False):
    """`unit_action` across every node at once, keyed by address."""
    results = {}
    threads = []

    def worker(ip):
        results[ip] = unit_action(ip, action, units, detach, ignore_failed)

    for ip in ip_list:
        thread = threading.Thread(target=worker, args=(ip,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    return results


def peer_host_key_command(ips):
    """Shell that makes this node trust every peer's SSH host key, and only adds what is missing.

    Provisioning seeds /root/.ssh/known_hosts, but a node that was not provisioned that way (or
    one whose file is gone) ends up with nodes that cannot ssh to each other under
    StrictHostKeyChecking=yes, which Mimir's inter-node trust check reports as a failure on a
    cluster that was just created. The cluster knows every member's address when it is created, so
    it makes sure of this itself. `ssh-keygen -F` makes it idempotent: a host already listed is not
    scanned again, so re-running never duplicates entries or replaces one that is already pinned.
    """
    targets = " ".join(ips)
    return ("mkdir -p /root/.ssh && chmod 700 /root/.ssh && touch /root/.ssh/known_hosts && "
            "chmod 600 /root/.ssh/known_hosts && "
            "for peer in " + targets + "; do "
            "ssh-keygen -F $peer -f /root/.ssh/known_hosts >/dev/null 2>&1 || "
            "ssh-keyscan -H $peer >> /root/.ssh/known_hosts 2>/dev/null; done")


def wait_for_spark_daemons(ips, probe=None, grace=3, timeout=60, interval=1,
                           sleep=time.sleep, now=time.time, say=print):
    """Wait until spark-daemon answers on every host after a detached restart. Returns the hosts
    that never did.

    `cluster destroy` ends by restarting the daemon on each node, detached, which fires about a
    second later and takes a few more to complete. It used to return without waiting, so a
    `cluster create` started straight afterwards could land on a daemon mid-restart and fail its
    first parallel command with "Remote end closed connection". The grace period is there because
    the daemon still answers for a moment before the restart takes it down; answering twice in a
    row after that is what is taken as "back".
    """
    probe = probe or (lambda ip: run_remote_spark(ip, "true")[0] == 0)
    say("Waiting for spark-daemon to come back on %s..." % ", ".join(ips))
    sleep(grace)
    started = now()
    pending = {ip: 0 for ip in ips}
    while pending and now() - started < timeout:
        for ip in list(pending):
            if probe(ip):
                pending[ip] += 1
                if pending[ip] >= 2:
                    del pending[ip]
            else:
                pending[ip] = 0
        if pending:
            sleep(interval)
    return sorted(pending)


def start_scylla_in_order(ips, restart=None, is_active=None, listening=None, progress=None,
                          sleep=time.sleep, say=print, listen_seconds=600):
    """Start ScyllaDB on one node at a time, seed first, each listening before the next starts.
    Returns None on success, or a sentence saying which node failed and how.

    This used to restart hydra-db on every node at once and then wait. ScyllaDB forms its Raft
    group 0 as the nodes come up, and nodes that start together can join the group before the
    first one's gossip has learned who they are: the seed then logs "Raft server id ... cannot be
    translated to an IP address" thousands of times and the others sit in "ensuring that the
    cluster has fully upgraded to use Raft" for ever, never listening. It worked on some runs and
    not others, which is how it was found. Bringing nodes in one at a time is ScyllaDB's own
    procedure for a new cluster; the first address in the list is the one every node is told to
    contact, so it goes first.
    """
    restart = restart or (lambda ip: unit_action_checked([ip], "restart", ["hydra-db"]))
    is_active = is_active or (lambda ip: unit_is_active(ip, "hydra-db"))
    listening = listening or (lambda ip: port_listening(ip, 9042))
    progress = progress or get_scylla_bootstrap_progress
    for position, ip in enumerate(ips):
        role = "the seed" if position == 0 else "joining"
        say(f"[{ip}] Starting ScyllaDB ({role}; node {position + 1} of {len(ips)})...")
        restart(ip)
        for _ in range(40):
            if is_active(ip):
                break
            sleep(1)
        else:
            return f"hydra-db failed to start on {ip}"
        say(f"[{ip}] Waiting for ScyllaDB to listen on port 9042...")
        last_progress = None
        for i in range(listen_seconds):
            if listening(ip):
                say(f"[{ip}] ScyllaDB is listening.")
                break
            if i % 10 == 0:
                latest = progress(ip)
                if latest and latest != last_progress:
                    say(f"[{ip}] ScyllaDB Bootstrap Status: {latest}")
                    last_progress = latest
            sleep(1)
        else:
            return f"ScyllaDB port 9042 timeout on {ip}"
    return None


def run_health_checks_settled(ip, runner=None, attempts=4, interval=20, sleep=time.sleep, say=print):
    """Run Mimir's checks on `ip`, giving a just-created cluster a few chances to settle.
    Returns (ran_ok, output, failing_lines).

    Some checks are only true a little after the last service starts: bifrost binds the VIP once
    its health guard sees the console answer, and elections settle. Reporting the first run as
    "Cluster is not healthy" told an operator a fresh cluster was broken when it was seconds from
    fine. A check that is still failing after `attempts` runs is reported as it always was.
    """
    runner = runner or (lambda: run_remote_spark(ip, "/usr/local/bin/mcli health_checks run_all"))
    output = ""
    failing = []
    for attempt in range(1, attempts + 1):
        rc, output, _ = runner()
        if rc != 0:
            return False, output, []
        failing = [line for line in output.splitlines() if "[ FAIL ]" in line]
        if not failing or attempt == attempts:
            break
        say(f"{len(failing)} check(s) not passing yet; waiting {interval}s and running them again "
            f"({attempt}/{attempts - 1})...")
        sleep(interval)
    return True, output, failing


def unit_action_checked(ip_list, action, units=None, ignore_failed=False):
    """`unit_action_parallel`, exiting on the first node that refuses.

    The counterpart of run_parallel_checked, and it keeps that function's contract: a
    service that will not start is not something the rest of a cluster build can be run
    on top of.
    """
    print(f"Running '{action} {' '.join(units or [])}' on {ip_list}")
    failures = unit_action_parallel(ip_list, action, units, ignore_failed=ignore_failed)
    for ip, (ok, detail) in sorted(failures.items()):
        if not ok:
            print(f"[ERROR] [{ip}] could not {action} {' '.join(units or [])}: {detail}")
            sys.exit(1)


def unit_is_active(ip, unit):
    """True when the unit is active on that node, False when it is not or unreadable.

    Where the shell form compared `systemctl is-active` output against the literal
    string "active", this reads a boolean the daemon derived from the same fact -- and
    from `systemctl show`, which names the unit each answer belongs to rather than
    leaving the caller to match answers to units by line number.
    """
    status, body, _ = run_mtls_spark_api_full(
        ip, f"/api/v1/host/units?units={unit}", method="GET")
    if status != 200:
        return False
    for state in body.get("units") or []:
        if state.get("unit") == unit:
            return bool(state.get("active"))
    return False


PHX_ENV_PATH = "/etc/hci/spectrum/spectrum-phx.env"


def ensure_phoenix_env(ips):
    """Make sure every node has the Phoenix console's environment file, with one secret.

    Provisioning writes it, but `cluster destroy` removes /etc/hci/spectrum and with it the
    secret, and the console's unit refuses to start without that file (a crash loop on a
    missing SECRET_KEY_BASE is worse than an inactive unit). A cluster that is created again
    on nodes that were already provisioned would otherwise reach Phase 6 with nothing for the
    console to start from.

    The secret has to be identical on every node -- a session cookie signed on one must verify
    on the others -- so an existing one is reused, and a node that has none is given the same
    value. A new one is minted only when no node has any, which is also the only time nobody
    holds a session to lose. Existing files are never rewritten. Returns the nodes it wrote.
    """
    import secrets as _secrets

    secret = ""
    has_file = {}
    for ip in ips:
        rc, out, _ = run_remote_spark(
            ip, "grep -h '^SECRET_KEY_BASE=' %s 2>/dev/null; true" % PHX_ENV_PATH)
        value = out.strip().partition("=")[2].strip() if rc == 0 else ""
        has_file[ip] = bool(value)
        if value and not secret:
            secret = value
    if not secret:
        secret = base64.b64encode(_secrets.token_bytes(48)).decode("ascii").rstrip("=")

    written = []
    for ip in ips:
        if has_file[ip]:
            continue
        text = (f"SECRET_KEY_BASE={secret}\nPHX_HOST={ip}\n"
                f"PHX_EXTRA_ORIGINS={','.join(ips)}\n")
        encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
        rc, _, err = run_remote_spark(
            ip, f"mkdir -p /etc/hci/spectrum && echo {encoded} | base64 -d > {PHX_ENV_PATH} "
                f"&& chmod 600 {PHX_ENV_PATH}")
        if rc != 0:
            raise RuntimeError(f"could not write {PHX_ENV_PATH} on {ip}: {err}")
        written.append(ip)
    return written


def port_listening(ip, port):
    """True when the node has something in LISTEN on that TCP port.

    This replaces `ss -tlnp | grep <port>`, which matched the port number anywhere in
    the output -- a peer address of 10.0.90.42, a queue depth, another process's pid --
    and so could report a service as listening on a node where nothing had bound it.
    """
    status, body, _ = run_mtls_spark_api_full(
        ip, f"/api/v1/host/listeners?port={port}", method="GET")
    if status != 200:
        return False
    return bool(body.get("listening"))


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

def run_parallel_checked(ips, command, allow_already_exists=False, timeout=None, label=None):
    if label is None:
        print(f"Running parallel command on {ips}: {command}")
    # With a label the command is not echoed: a script is sent as a base64 blob, and a screenful of
    # `echo CnNldCAtZQ... | base64 -d | bash` tells an operator nothing about what is happening.
    results = run_parallel(ips, command, timeout=timeout, label=label)
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


PARTICIPANT = "participant"
OBSERVER = "observer"


def ensemble_roles(members, roles=None):
    """`{member id: role}` for a membership of `(id, ip)` pairs.

    Without an explicit mapping the first three members vote and everything after them
    observes, which is what position has always decided here and what `cluster create` and
    `cluster add-node` still want: a cluster that has only ever grown has its voters at the
    front of the list by construction.

    With one -- which is what a `reconfig` produces -- position decides nothing, because
    the entire point of asking for a change is that the voters are no longer the first
    three. Passing the roles through rather than recomputing them is what keeps a unit
    rewrite from silently undoing a promotion.
    """
    if roles:
        return {member_id: roles.get(member_id, PARTICIPANT) for member_id, _ip in members}
    return {member_id: (OBSERVER if position > 3 else PARTICIPANT)
            for position, (member_id, _ip) in enumerate(members, start=1)}


def zookeeper_quadlet(node_id, members, roles=None):
    """One node's ZooKeeper unit for an ensemble of `members`, a list of `(id, ip)`.

    Every node's unit names the whole ensemble. With `reconfigEnabled=true` the running
    ensemble can be changed without touching any of them -- but the unit is still the only
    *durable* record of the membership, because the image regenerates `/conf/zoo.cfg` from
    `ZOO_SERVERS` on every start and `/conf` does not survive the container. So a unit that
    disagrees with the live configuration is a membership change waiting to be undone by a
    restart, and every path that reconfigures the ensemble writes these again to match.
    See docs/zookeeper.md.

    Ids are passed in rather than derived from position. A member's id *is* its ZooKeeper
    identity: it has to match the `server.<id>` entry every other member holds for it, and
    the image writes it into the data directory as `myid`. Deriving it from position is
    fine while an ensemble only ever grows, but removing a member would then renumber
    everyone after it -- handing a node a new identity on top of a data directory that
    remembers the old one. Ids do not have to be contiguous, so a removal simply leaves
    a gap.

    Voter or observer is `roles`, and by default position: the first three members form
    the quorum and any beyond that scale reads without joining it, so a five-node cluster
    still needs two failures to lose consensus rather than three.

    `ZOO_PEER_TYPE` is written because it has always been written, and it is worth being
    clear that it does nothing: the official image never reads it. The `:observer` suffix
    on the member's own `server.<id>` entry is what decides the role, so the two are kept
    saying the same thing rather than one of them being quietly wrong.
    """
    resolved = ensemble_roles(members, roles)
    if len(members) == 1:
        servers_env = ""
    else:
        parts = []
        for member_id, ip in members:
            suffix = ":observer" if resolved.get(member_id) == OBSERVER else ""
            parts.append("server.%d=%s:2888:3888%s;2181" % (member_id, ip, suffix))
        servers_env = ' ZOO_SERVERS="%s"' % " ".join(parts)
    peer_type_env = " ZOO_PEER_TYPE=observer" if resolved.get(node_id) == OBSERVER else ""
    return (
        "[Unit]\n"
        "Description=ZooKeeper Cluster Consensus Service\n"
        "After=network.target\n\n"
        "[Service]\n"
        # A ceiling on what this unit can put in the journal, whatever it decides to say.
        # ZooKeeper logs two INFO lines for every four-letter-word probe, and nine daemons
        # each running their own leader check had it writing eleven lines a second forever
        # on an idle cluster. The probing is fixed at the source -- helios_zk.leader_ip
        # caches it -- and this is the guard that makes the next caller to get it wrong
        # cost a rate-limit notice instead of a saturated journal. Generous enough for the
        # burst an election legitimately produces.
        "LogRateLimitIntervalSec=10s\n"
        "LogRateLimitBurst=100\n"
        "Restart=always\n"
        "CPUWeight=100\n"
        "MemoryMax=512M\n"
        "MemoryHigh=400M\n\n"
        "[Container]\n"
        "Image=docker.io/library/zookeeper:3.9.2\n"
        "Network=host\n"
        "Volume=/etc/hci/zookeeper/logback.xml:/conf/logback.xml:ro,Z\n"
        "Volume=/var/lib/hci/zookeeper/data:/data:Z\n"
        "Volume=/var/lib/hci/zookeeper/log:/datalog:Z\n"
        # ZOO_CFG_EXTRA is appended to the generated zoo.cfg verbatim, one entry per
        # whitespace-separated token. reconfigEnabled is what makes `reconfig` anything
        # other than a refusal, and it has to be the same on every member: whichever server
        # becomes leader is the one that decides, so a half-enabled ensemble answers
        # differently depending on an election.
        "Environment=ZOO_MY_ID=%d%s%s ZOO_4LW_COMMANDS_WHITELIST=* "
        "ZOO_CFG_EXTRA=reconfigEnabled=true\n"
        % (node_id, servers_env, peer_type_env)
    )


def write_zookeeper_ensemble(members, roles=None):
    """Rewrite every node's ZooKeeper unit for this membership. Returns the nodes that failed.

    `members` is a list of `(id, ip)` in ensemble order, `roles` an optional
    `{id: role}` for an ensemble whose voters are no longer its first three members.
    """
    failed = []
    for member_id, ip in members:
        quad = zookeeper_quadlet(member_id, members, roles)
        encoded = base64.b64encode(quad.encode()).decode()
        # The file write is still a shell string -- writing a unit file is a filesystem
        # operation and has no typed endpoint yet -- but the reload that follows it does,
        # so it is a separate call rather than a third clause of an `&&` chain. The
        # ordering the `&&` gave is kept explicitly: no reload unless the write landed.
        rc, _, _ = run_remote_spark(
            ip,
            "mkdir -p /etc/containers/systemd && echo %s | base64 -d "
            "> /etc/containers/systemd/zookeeper.container"
            % encoded)
        if rc != 0:
            failed.append(ip)
            continue
        ok, _ = unit_action(ip, "daemon-reload")
        if not ok:
            failed.append(ip)
    return failed


def read_zookeeper_ids(ips):
    """Each node's current ZooKeeper id, keyed by address.

    Read from the unit rather than assumed from position, so that a membership change can
    leave the survivors' identities exactly as they are. A node whose unit cannot be read
    is absent from the result and the caller decides what to do about it -- guessing an id
    for a node that already has one is how two members end up claiming the same identity.
    """
    ids = {}
    for ip in ips:
        rc, out, _ = run_remote_spark(
            ip, "grep -o 'ZOO_MY_ID=[0-9]*' /etc/containers/systemd/zookeeper.container "
                "2>/dev/null | head -1")
        if rc != 0:
            continue
        match = re.search(r"ZOO_MY_ID=(\d+)", out or "")
        if match:
            ids[ip] = int(match.group(1))
    return ids


# -- Changing who votes ------------------------------------------------------------------
#
# Until now the voters were the first three nodes ever provisioned, and the only way to
# change that was to rewrite every unit and restart the ensemble. Losing two of those three
# stopped cluster coordination with every other node healthy and idle, and the nearest
# thing to a fix -- `cluster decommission --finalize` -- reached the right membership as a
# *side effect* of a config rewrite, because voter-or-observer followed position in the
# list. During the rolling restart the members briefly disagreed about who votes.
#
# ZooKeeper 3.9.2 can do this properly. `reconfig` is one atomic operation: no restart, no
# window in which the members hold different configurations, and the server itself will
# only commit it while a quorum of *both* the old and the new configuration is available.
#
# What ZooKeeper will not do is decide whether the change is a good idea. It will happily
# take a three-voter ensemble down to one, or hand a vote to a node that is not answering
# -- both of those satisfy the dual-quorum rule at the instant of the change and leave a
# cluster that the next single failure finishes off. That judgement is `plan_ensemble_roles`
# below, and it is the whole point of the command: a promotion that is merely usually fine
# is worse than not having one, because it will be trusted.

def helios_zk_module():
    """The shared ZooKeeper module, or a failure that names what is missing.

    Loudly rather than by returning None: every caller of this is about to reason about
    quorum, and a silent fallback there is exactly the wrong shape of mistake.
    """
    zkmod = load_helios_zk()
    if zkmod is None or not hasattr(zkmod, "member_spec_with_role"):
        raise RuntimeError(
            "helios_zk is missing or predates ensemble reconfiguration. Roll the toolkit "
            "out (deploy_updates.py) before changing the ensemble.")
    return zkmod


def read_ensemble_config(ips):
    """What the running ensemble says its membership is, or None if it cannot be read.

    Read from ZooKeeper's own /zookeeper/config rather than from the units, because with
    reconfiguration enabled those answer different questions: a unit says what a node would
    come back with after a restart, and /zookeeper/config says who is voting now. Acting on
    the first would be reasoning about an ensemble that is not running.
    """
    zkmod = load_helios_zk()
    if zkmod is None or not hasattr(zkmod, "read_ensemble_config"):
        return None
    hosts = ["127.0.0.1"] + [ip for ip in ips if ip != "127.0.0.1"]
    client = None
    try:
        client = zkmod.connect(hosts, timeout=3.0)
        return zkmod.read_ensemble_config(client)
    except Exception:
        return None
    finally:
        if client:
            try:
                client.close()
            except Exception:
                pass


def probe_ensemble_modes(members, timeout=1.0):
    """Each member's mode as the member itself reports it, keyed by id.

    None means it did not answer, which for this purpose covers both "the node is down"
    and "the node is up and mid-election" -- a server that is not serving requests answers
    `stat` without a mode line. Both are reasons not to reconfigure, so they need no
    distinction here.

    `server_mode` and not `leader_ip`: this asks about a particular server rather than
    which one leads, and the cached form would answer the second question.
    """
    zkmod = load_helios_zk()
    if zkmod is None:
        return None
    return {member["id"]: zkmod.server_mode(member["host"], timeout=timeout)
            for member in members}


def reconfig_enabled_on(hosts):
    """`{address: bool}` -- whether each node's *running* ZooKeeper accepts a reconfig.

    Read out of the container's own /conf/zoo.cfg and not out of the unit, because those
    stop agreeing the moment a rollout has staged the change: `deploy_updates` writes the
    unit and deliberately does not restart the consensus layer, so a node can have the
    setting in its unit and still be running the configuration it started with. A check
    against the unit would say yes and the server would then refuse the reconfiguration.
    """
    enabled = {}
    for host in hosts:
        rc, out, _ = run_remote_spark(
            host, "podman exec systemd-zookeeper grep -c '^reconfigEnabled=true' "
                  "/conf/zoo.cfg 2>/dev/null || true")
        enabled[host] = rc == 0 and (out or "").strip().startswith("1")
    return enabled


def quorum_size(voters):
    """A strict majority of `voters` -- what ZooKeeper needs to commit anything."""
    return voters // 2 + 1


def plan_ensemble_roles(members, modes, promote=(), demote=(), drop=()):
    """The membership a role change would produce, or the reason it is refused.

    Returns `(new_members, refusal)`; exactly one of them is None. `members` is the parsed
    /zookeeper/config membership, `modes` the `{id: mode}` every member reports, and the
    three id collections say what is being asked for.

    Four refusals, and between them they are the argument that this cannot lose quorum:

      * a member that is not in the ensemble -- promotion moves a vote between members
        that already exist, it does not add or remove one;
      * no single leader among the current voters, which means either no quorum or an
        election in progress, and an election is the worst possible moment to change the
        membership out from under;
      * fewer than a quorum of the *current* voters answering, so the reconfiguration
        could not be committed by the configuration it is leaving;
      * any voter in the *new* set not answering. This is the one that matters. A vote
        held by a node that is down is counted in every quorum and cast in none of them,
        so the ensemble is weaker afterwards than it was before -- and because it demands
        that *every* new voter is live, it also gives the new configuration its quorum for
        free, which is the other half of what ZooKeeper needs to commit.

    Then one refusal that is not about the transition but about what it leaves behind: an
    ensemble whose quorum is its entire membership tolerates no failure at all, so a
    result of fewer than three voters is refused. That is why demoting a voter on a
    three-node cluster is not a thing you can do: all three of them have to vote.
    """
    known = {member["id"]: member for member in members}
    for member_id in list(promote) + list(demote) + list(drop):
        if member_id not in known:
            return None, ("server.%d is not in the ensemble. Promotion and demotion move "
                          "the vote between members that are already there; adding or "
                          "removing one is `cluster add-node` or `cluster decommission`."
                          % member_id)

    voters = [member["id"] for member in members if member["role"] == PARTICIPANT]
    leaders = [member_id for member_id in voters if modes.get(member_id) == "leader"]
    if len(leaders) != 1:
        return None, ("the ensemble reports %d leaders among its %d voters. It is either "
                      "mid-election or partitioned, and changing the membership of an "
                      "ensemble that is not agreeing on one is how both halves end up "
                      "believing they are the quorum." % (len(leaders), len(voters)))

    answering = [member_id for member_id in voters if modes.get(member_id)]
    needed = quorum_size(len(voters))
    if len(answering) < needed:
        return None, ("%d of the %d current voters are answering and a reconfiguration "
                      "needs a quorum of the configuration it is leaving, which is %d. "
                      "Get the ensemble healthy before changing it."
                      % (len(answering), len(voters), needed))

    with_role = helios_zk_module().member_spec_with_role
    new_members = []
    for member in members:
        if member["id"] in drop:
            continue
        role = member["role"]
        if member["id"] in promote:
            role = PARTICIPANT
        elif member["id"] in demote:
            role = OBSERVER
        entry = dict(member)
        entry["role"] = role
        entry["spec"] = with_role(member["spec"], role)
        new_members.append(entry)

    new_voters = [member["id"] for member in new_members if member["role"] == PARTICIPANT]
    silent = [member_id for member_id in new_voters if not modes.get(member_id)]
    if silent:
        return None, ("this would leave %s voting without answering. A vote held by a node "
                      "that is down counts towards every quorum and is cast in none of "
                      "them, so the ensemble comes out of this weaker than it went in. "
                      "Hand that member's vote to a live one in the same change instead "
                      "(--replacing)."
                      % ", ".join("server.%d" % member_id for member_id in sorted(silent)))

    if len(new_voters) < 3:
        return None, ("this would leave %d voter(s), whose quorum is %d -- an ensemble "
                      "that tolerates no failure at all, which is worse than the one it "
                      "replaces. Changing the *size* of the cluster is `cluster add-node` "
                      "and `cluster decommission`; this changes which of its nodes vote."
                      % (len(new_voters), quorum_size(len(new_voters))))

    return new_members, None


def ensemble_warnings(members):
    """What is worth saying about a membership that is not worth refusing over."""
    warnings = []
    voters = [member for member in members if member["role"] == PARTICIPANT]
    if len(voters) % 2 == 0:
        # Not unsafe, just paid for and not delivered: four voters tolerate the single
        # failure three do, and put a fourth node in the way of every write.
        warnings.append(
            "%d voters is an even number: it tolerates the same %d failure(s) that %d "
            "voters would, and adds a node to every quorum. An odd count is usually what "
            "was meant." % (len(voters), len(voters) - quorum_size(len(voters)),
                            len(voters) - 1))
    return warnings


def format_reconfig_members(members):
    """The `-members` argument for a non-incremental reconfig: comma-separated
    `server.<id>=<spec>`, with each spec exactly as ZooKeeper last wrote it apart from the
    role. Addresses and ports are never reconstructed here -- they are whatever the
    ensemble already believes, which is the only version of them that cannot be wrong."""
    return ",".join("server.%d=%s" % (member["id"], member["spec"]) for member in members)


def describe_ensemble(members, modes):
    """One line per member: who it is, what it does, and whether it is answering."""
    lines = []
    for member in members:
        mode = modes.get(member["id"]) or "not answering"
        lines.append("  server.%-3d %-15s %-12s %s"
                     % (member["id"], member["host"], member["role"], mode))
    return "\n".join(lines)


def write_units_from(members):
    """Rewrite every member's unit to say exactly this membership.

    Returns `(written, unwritten)`. Takes the parsed configuration rather than a plan, so
    what lands in the units is what the ensemble said when it was last read -- which also
    repairs a unit that had drifted, without anyone having to notice that it had.
    """
    pairs = [(member["id"], member["host"]) for member in members]
    roles = {member["id"]: member["role"] for member in members}
    unwritten = write_zookeeper_ensemble(pairs, roles)
    return len(pairs) - len(unwritten), unwritten


def apply_ensemble_reconfig(new_members, version, leader_host, modes):
    """Commit `new_members` and make the units say the same thing. Returns (ok, message).

    Three steps, in this order and no other:

      1. `reconfig -v <version>`, which is atomic and refuses to commit if the ensemble has
         been reconfigured since the plan was made -- the version is the guard against
         acting on a membership that was read a moment ago and has moved since.
      2. read the membership back and check it is the one that was asked for, because a
         client that reports success is a weaker claim than the ensemble agreeing.
      3. rewrite every member's unit from what step 2 read, so that a container restart
         reproduces this ensemble rather than reverting to whichever one the unit
         described. Nothing is restarted: the running ensemble is already correct, and the
         unit is only what it would be rebuilt from.

    Step 3 is not bookkeeping. The image regenerates /conf/zoo.cfg from ZOO_SERVERS
    whenever it is absent, and /conf lives in the container rather than on a volume, so the
    dynamic configuration ZooKeeper writes next to it is gone the moment the container is
    recreated. The unit is the only durable record there is.
    """
    if not version:
        return False, ("the ensemble did not report a configuration version, so the "
                       "change cannot be made conditional on it. Refusing.")
    argument = format_reconfig_members(new_members)
    _rc, out, err = run_remote_spark(
        leader_host,
        "podman exec systemd-zookeeper zkCli.sh -server 127.0.0.1:2181 reconfig -v %s "
        "-members %s" % (shlex.quote(version), shlex.quote(argument)))
    said = ((err or "") + (out or "")).strip()[:400]

    # The client's exit status is not the answer. What decides whether this worked is the
    # ensemble agreeing that it did, so the membership is read back and compared, and the
    # client's output is kept only to explain a disagreement.
    after = read_ensemble_config([member["host"] for member in new_members])
    if after is None:
        return False, ("the reconfiguration was submitted and the result could not be read "
                       "back. Check `cluster status` and /zookeeper/config before doing "
                       "anything else; the units have NOT been rewritten. zkCli said: %s"
                       % said)
    wanted = {(member["id"], member["role"]) for member in new_members}
    got = {(member["id"], member["role"]) for member in after["members"]}
    if wanted != got:
        return False, ("the ensemble did not take the membership it was asked for, and is "
                       "running:\n%s\nThe units have NOT been rewritten. zkCli said: %s"
                       % ("\n".join("  server.%d %s %s" % (m["id"], m["host"], m["role"])
                                    for m in after["members"]), said))

    ids = {member["host"]: member["id"] for member in after["members"]}
    written, unwritten = write_units_from(after["members"])
    answering = [host for host in unwritten if modes.get(ids[host])]
    if answering:
        return False, ("the ensemble is reconfigured, but the unit could not be written on "
                       "%s, which is answering. It will revert to the previous membership "
                       "the next time its container is recreated -- rerun this."
                       % ", ".join(answering))
    message = ("ensemble reconfigured to version %s and %d unit(s) rewritten to match. "
               "Nothing was restarted." % (after["version"], written))
    if unwritten:
        # Moving a role off a permanently failed node means the node is not there to be
        # written to, so this is the expected outcome of the operation that matters most --
        # returning failure would say nothing happened, which is the opposite of true. It
        # is still a loose end, and it is the one that bites later rather than now.
        message += ("\n[WARNING] %s did not answer, so its unit still describes the "
                    "membership it has just left. Rerun this command once it does: until "
                    "then, starting ZooKeeper there brings up a member that believes it "
                    "still holds the role it was relieved of." % ", ".join(unwritten))
    return True, message


def cmd_zk_role(args, promoting):
    """`cluster zk-promote` / `cluster zk-demote`."""
    ips = get_cluster_ips()
    config = read_ensemble_config(ips)
    if config is None or not config["members"]:
        print("[ERROR] Could not read /zookeeper/config. The ensemble has to be readable "
              "before its membership can be changed -- check `cluster status`.")
        return 1

    members = config["members"]
    by_host = {member["host"]: member for member in members}

    def resolve(address):
        member = by_host.get((address or "").strip())
        if member is None:
            print("[ERROR] %s is not a member of the ensemble. It has: %s"
                  % (address, ", ".join(sorted(by_host))))
        return member

    target = resolve(args.node)
    if target is None:
        return 1
    other = None
    if args.replacing:
        other = resolve(args.replacing)
        if other is None:
            return 1
        if other["id"] == target["id"]:
            print("[ERROR] --node and --replacing name the same member.")
            return 1

    # --replacing is the other half of the same sentence, so it takes the opposite role.
    # It is not decoration: on a three-voter ensemble it is the *only* legal change, because
    # promoting without it makes four voters of which one is the node being replaced, and
    # demoting without it makes two.
    promote = {target["id"]} if promoting else set()
    demote = set() if promoting else {target["id"]}
    if other is not None:
        (demote if promoting else promote).add(other["id"])

    enabled = reconfig_enabled_on([member["host"] for member in members])
    missing = sorted(host for host, ok in enabled.items() if not ok)
    if missing:
        print("[ERROR] Reconfiguration is not enabled on the running ZooKeeper on: %s"
              % ", ".join(missing))
        print("        Roll the toolkit out to write the unit, then restart ZooKeeper one "
              "node at a time, waiting for each to report a mode before the next.")
        return 1

    modes = probe_ensemble_modes(members)
    if modes is None:
        print("[ERROR] helios_zk is not available, so no member's mode can be read.")
        return 1

    print("Ensemble now (configuration version %s):" % config["version"])
    print(describe_ensemble(members, modes))
    print()

    new_members, refusal = plan_ensemble_roles(members, modes, promote, demote)
    if refusal:
        print("[REFUSED] %s" % refusal)
        return 1
    if {(m["id"], m["role"]) for m in new_members} == {(m["id"], m["role"]) for m in members}:
        # Nothing for the ensemble, but the units may still disagree with it -- which is
        # exactly the state a node that was down during an earlier change is left in. So
        # the no-op writes them, and this doubles as the way to bring that node into line
        # when it comes back: ask for the role it already has.
        print("The ensemble already has that membership. Making the units say so.")
        written, unwritten = write_units_from(members)
        print("%d unit(s) written; nothing restarted." % written)
        if unwritten:
            print("[WARNING] Still not reachable, and still describing an older "
                  "membership: %s" % ", ".join(unwritten))
            return 1
        return 0

    print("Ensemble after:")
    print(describe_ensemble(new_members, modes))
    for warning in ensemble_warnings(new_members):
        print("\n[NOTE] %s" % warning)
    print()

    try:
        leader = next(member["host"] for member in members
                      if modes.get(member["id"]) == "leader")
    except StopIteration:  # plan_ensemble_roles refuses this, so it cannot happen here
        print("[ERROR] The ensemble has no leader.")
        return 1

    ok, message = apply_ensemble_reconfig(new_members, config["version"], leader, modes)
    print(("" if ok else "[ERROR] ") + message)
    return 0 if ok else 1


def hand_off_ensemble_vote(target, survivors):
    """Give a departing voter's vote to a live observer, in one reconfiguration.

    Returns `(handled, message)`. `handled` is False whenever there is nothing deliberate
    to be done -- reconfiguration is unavailable, the departing node does not vote, or
    there is no observer to hand the vote to -- and the caller falls back to rewriting the
    units and restarting them one at a time, which is what a decommission always did.

    The fallback is not a lesser path; on a three-node cluster it is the only one. Removing
    a node from three leaves two voters, and there is no observer waiting to become a third,
    so the ensemble genuinely does become one that tolerates no failure. That is a property
    of the cluster the operator asked for and not something a reconfiguration can fix, and
    `plan_ensemble_roles` refuses to pretend otherwise.

    Where it does apply -- five nodes, three voters, two observers -- this replaces the
    whole rewrite-and-roll: the vote moves atomically, no member ever holds a different
    view of who votes, and nothing is restarted.
    """
    config = read_ensemble_config(survivors)
    if config is None or not config["members"]:
        return False, "[zookeeper] /zookeeper/config could not be read."
    departing = next((m for m in config["members"] if m["host"] == target), None)
    if departing is None:
        return False, "[zookeeper] %s is not in the ensemble configuration." % target
    if departing["role"] != PARTICIPANT:
        return False, "[zookeeper] %s does not vote; there is no vote to hand on." % target

    enabled = reconfig_enabled_on([m["host"] for m in config["members"] if m["host"] != target])
    if not all(enabled.values()):
        return False, ("[zookeeper] reconfiguration is not enabled on every survivor, so "
                       "the vote cannot be handed on deliberately.")

    modes = probe_ensemble_modes(config["members"])
    if modes is None:
        return False, "[zookeeper] helios_zk is not available."
    successor = next((m for m in config["members"]
                      if m["role"] == OBSERVER and modes.get(m["id"])), None)
    if successor is None:
        return False, ("[zookeeper] no live observer to take over the vote, so the "
                       "ensemble shrinks with the cluster.")

    new_members, refusal = plan_ensemble_roles(
        config["members"], modes, promote={successor["id"]}, drop={departing["id"]})
    if refusal:
        return False, "[zookeeper] %s" % refusal

    print("[zookeeper] handing server.%d's vote to server.%d (%s) and removing it..."
          % (departing["id"], successor["id"], successor["host"]))
    leader = next((m["host"] for m in config["members"]
                   if modes.get(m["id"]) == "leader"), None)
    if leader is None or leader == target:
        # The departing node may be the leader. Reconfiguring through it works -- it is
        # still the leader -- but it is about to be wiped, so the change is sent to a
        # survivor and forwarded.
        leader = next((m["host"] for m in new_members if modes.get(m["id"])), None)
    ok, message = apply_ensemble_reconfig(new_members, config["version"], leader, modes)
    return ok, "[zookeeper] " + message


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


def redundancy_factor_refusal(factor, node_count):
    """Why `factor` cannot be this cluster's redundancy factor, or None if it can.

    The factor counts host losses survived, so it needs one more host than it names: ftt 1
    keeps two copies and there must be two hosts to hold them. Refused rather than
    clamped, because a clamped value is written to cluster.json and read back later as
    the operator's decision.
    """
    if factor < 0:
        return "a redundancy factor cannot be negative"
    if factor + 1 > node_count:
        return ("a redundancy factor of %d keeps %d copies and this cluster has %d node(s)"
                % (factor, factor + 1, node_count))
    return None


def redundancy_factor_warning(config, node_count, node):
    """Lines telling the operator the cluster's replication policy is stale, or [].

    `cluster create` forces the factor to 0 for a one-node cluster, which is correct then:
    there is nowhere to put a second copy. Nothing revisits it when the cluster grows, so a
    cluster created on one node and grown to three keeps asking for one copy of every disk
    for as long as it lives, and every tool reads that as the operator's choice.

    This only ever *says* so. The factor is a replication policy: raising it changes how
    many copies every new vdisk gets and what Purah heals toward, and that is a decision
    for the operator and not a side effect of adding a machine. Hence a warning that
    carries the exact command, and a flag (`-r`) that makes the decision explicit.

    A document with no factor at all is treated the same way: Sidon falls back to one copy
    for it, so the cluster is in the same state without anyone having chosen it.
    """
    factor = config.get("redundancy_factor")
    if node_count < 2 or (isinstance(factor, int) and not isinstance(factor, bool)
                          and factor >= 1):
        return []
    shown = "0" if factor is None or factor == 0 else str(factor)
    return [
        "[WARNING] cluster.json says redundancy_factor %s, and this cluster now has %d "
        "nodes." % (shown, node_count),
        "[WARNING] Every new vdisk is created with ONE copy, whatever the hosts could "
        "hold. 'cluster create' sets 0 for a single-node cluster and nothing raised it "
        "when this one grew.",
        "[WARNING] This is a replication policy, so it has not been changed. To keep two "
        "copies of new vdisks:",
        "    cluster add-node --node %s -r 1" % node,
        "[WARNING] That rewrites cluster.json on every node. Sidon reads it at start, so "
        "restart sidon on each node afterwards (one at a time) for creates to see it. "
        "Existing vdisks keep the rf they were created with; "
        "'valcli storage.replication' lists them and 'valcli storage.replicate --all' "
        "tops them up.",
    ]


def set_redundancy_factor(target, factor, existing):
    """Change the cluster's redundancy factor and nothing else.

    What `add-node -r N` does when the node is already a live member, so a cluster that was
    grown before this existed has a way to make the decision without re-adding anything.
    """
    config = cluster_hosts_config()
    if config is None:
        print("[ERROR] /etc/hci/cluster.json could not be read.")
        return 1
    refusal = redundancy_factor_refusal(factor, len(existing))
    if refusal:
        print("[ERROR] %s." % refusal)
        return 1
    before = config.get("redundancy_factor")
    config["redundancy_factor"] = factor
    failed = write_cluster_config(existing, config)
    if failed:
        print("[ERROR] Could not write /etc/hci/cluster.json on: %s" % ", ".join(failed))
        return 1
    print("[config] redundancy_factor %s -> %d on all %d node(s)."
          % ("unset" if before is None else before, factor, len(existing)))
    print("Sidon reads this at start. Restart it on each node, one at a time, for new "
          "vdisks to be created with %d cop%s." % (factor + 1, "y" if factor == 0 else "ies"))
    print("Existing vdisks keep the rf they were created with: 'valcli storage.replication' "
          "shows them, 'valcli storage.replicate --all' tops them up.")
    return 0


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
            if args.redundancy_factor is not None:
                # Nothing to join: the node is in, and the operator is using the one
                # command that exists to settle the replication policy.
                return set_redundancy_factor(target, args.redundancy_factor, existing)
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

    # Checked here, with nothing changed yet: a factor the grown cluster cannot satisfy
    # must not be discovered after the ZooKeeper ensemble has been rewritten.
    if args.redundancy_factor is not None:
        refusal = redundancy_factor_refusal(args.redundancy_factor, len(ips))
        if refusal:
            print("[ERROR] %s." % refusal)
            return 1

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
    # Only when asked. The factor is a replication policy and adding a machine does not
    # change it; see redundancy_factor_warning for what happens when it goes unrevisited.
    if args.redundancy_factor is not None:
        config["redundancy_factor"] = args.redundancy_factor
    failed = write_cluster_config(ips, config)
    if failed:
        print("[ERROR] Could not write /etc/hci/cluster.json on: %s" % ", ".join(failed))
        return 1
    print("[config] %s (%s) is in cluster.json on all %d node(s)." % (target, hostname, len(ips)))
    if args.redundancy_factor is not None:
        print("[config] redundancy_factor is %d. Sidon reads it at start, so restart it on "
              "each node for new vdisks to see it." % args.redundancy_factor)

    # 3. Consensus.
    #
    # Existing members keep the ids they already hold, read from their own units rather
    # than recomputed from position. A cluster that has had a node removed has a gap in
    # its ids, and recomputing would renumber every member after the gap -- giving each
    # of them a new ZooKeeper identity on top of a data directory that remembers the old
    # one. The joining node takes the first id nothing else is using.
    known_ids = read_zookeeper_ids(existing)
    unreadable = [ip for ip in existing if ip not in known_ids]
    if unreadable:
        print("[ERROR] Could not read the ZooKeeper id of: %s. Rewriting the ensemble "
              "without them would hand some node an identity that does not match its "
              "data directory." % ", ".join(unreadable))
        return 1
    members = ([(known_ids[ip], ip) for ip in existing]
               + [(max(known_ids.values()) + 1, target)])
    print("[zookeeper] rewriting the ensemble for %d member(s)..." % len(members))
    failed = write_zookeeper_ensemble(members)
    if failed:
        print("[ERROR] Could not write the ZooKeeper unit on: %s" % ", ".join(failed))
        return 1
    # Restarted one at a time, oldest first: a rolling restart keeps a quorum of the
    # *previous* ensemble alive throughout, which an all-at-once restart does not.
    for ip in ips:
        ok, detail = unit_action(ip, "restart", ["zookeeper"])
        if not ok:
            print("[ERROR] [%s] ZooKeeper did not restart: %s" % (ip, detail[:200]))
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
        "echo %s | base64 -d > /etc/containers/systemd/hydra-db.container" % encoded)
    if rc != 0:
        print("[ERROR] [%s] could not write the hydra-db unit: %s"
              % (target, (err or "").strip()[:200]))
        return 1
    ok, detail = unit_action(target, "daemon-reload")
    if ok:
        ok, detail = unit_action(target, "start", ["hydra-db"])
    if not ok:
        print("[ERROR] [%s] hydra-db did not start: %s" % (target, detail[:200]))
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

    stale = redundancy_factor_warning(config, len(ips), target)
    if stale:
        print()
        for line in stale:
            print(line)

    print()
    print("Still to do, and deliberately not automatic:")
    print("  - Raise the keyspace replication factor now that there are %d nodes:" % len(ips))
    wanted = default_metadata_replication_factor(config.get("redundancy_factor"), len(ips))
    print("      ALTER KEYSPACE hydra WITH replication = "
          "{'class': 'NetworkTopologyStrategy', '<datacenter>': %d};" % wanted)
    print("    then 'nodetool repair -pr hydra' on every node. ALTER changes the strategy")
    print("    only; the data is not on the new replicas until a repair has run, and until")
    print("    then the cluster reports a redundancy it does not have.")
    for line in two_replica_warning(wanted):
        print("    " + line)
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

def confirm_destroy(ips, assume_yes=False, read=input, interactive=None):
    """Make `cluster destroy` ask before it does the one thing that cannot be undone.

    It used to start straight away: stop every VM, wipe the LVM pool and disk signatures,
    delete the ZooKeeper and Hydra data and /etc/hci/cluster.json, and remove the sidon
    store -- on every host named, with no prompt, because nothing had ever put one in. The
    command that erases a cluster was easier to run by accident than `rm -r`.

    The phrase is typed rather than a bare y/n, because "y" is what a finger does when it
    is expecting a different question. It is the word `destroy` and not the host list, since
    a script that has to quote the hosts back is just a longer way to say --yes.

    A non-interactive stdin with no --yes is a refusal, not a pass: a pipe or a cron job
    that reaches this line has not been asked, and "nobody answered" must never mean "yes".
    Returns True when the destroy may proceed.
    """
    if assume_yes:
        print("[--yes] Skipping the confirmation prompt.")
        return True

    if interactive is None:
        try:
            interactive = sys.stdin.isatty()
        except Exception:
            interactive = False

    if not interactive:
        print("Refusing to destroy a cluster with no one at the keyboard to confirm it.")
        print("Run it from a terminal, or pass --yes if a script really means it.")
        return False

    print("")
    print("This will permanently destroy the cluster on:")
    for ip in ips:
        print(f"    {ip}")
    print("")
    print("  - every VM is stopped and undefined, and its disks are lost")
    print("  - the LVM pool and disk signatures are wiped")
    print("  - ZooKeeper and Hydra data, and the sidon extent store, are deleted")
    print("  - /etc/hci/cluster.json is removed, so `create` will need -s afterwards")
    print("")
    try:
        answer = read("Type 'destroy' to continue: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("")
        return False
    if answer != "destroy":
        print("Not confirmed. Nothing was changed.")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="HCI Cluster Management Utility")
    parser.add_argument("-s", "--servers", required=False, help="Comma-separated list of host IPs")
    parser.add_argument("-r", "--redundancy_factor", type=int, default=None, help="Fault Tolerance to Tolerate (FTT) / Redundancy Factor (e.g. 0, 1, or 2). "
                             "With 'add-node' it sets the factor explicitly; on a node that is "
                             "already a member it changes only the factor")
    parser.add_argument("-v", "--vip", required=False, help="Floating Cluster Virtual IP (VIP)")
    parser.add_argument("--verbose", action="store_true", help="Print verbose status information")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable status (ZooKeeper-backed path only)")
    parser.add_argument("--node", required=False,
                        help="Single host IP, for 'decommission', 'rejoin', 'add-node', "
                             "'zk-promote' and 'zk-demote'")
    parser.add_argument("--replacing", required=False,
                        help="For 'zk-promote'/'zk-demote': the member that takes the "
                             "opposite role in the same change, so a vote is handed over "
                             "rather than added or dropped")
    parser.add_argument("-y", "--yes", action="store_true",
                        help="Skip the confirmation prompt of 'destroy', for scripts")
    parser.add_argument("--finalize", action="store_true", help="Perform the bookkeeping half of a decommission or rejoin, once the ring work is done")
    parser.add_argument("command", choices=["create", "status", "start", "stop", "destroy",
                                            "ring", "decommission", "rejoin",
                                            "add-node", "zk-promote", "zk-demote"],
                        help="Action to perform")

    args = parser.parse_args()

    if args.command == "add-node":
        if not args.node:
            parser.error("add-node requires --node <ip>")
        sys.exit(cmd_add_node(args))

    if args.command in ("zk-promote", "zk-demote"):
        if not args.node:
            parser.error("%s requires --node <ip>" % args.command)
        sys.exit(cmd_zk_role(args, promoting=args.command == "zk-promote"))

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
                print(f"[WARNING] Single-node cluster detected. Forcing redundancy factor (FTT) from {rf} to 0 (no replication). Adding nodes later will not raise it: 'cluster add-node --node <ip> -r N' does.")
            rf = 0
        for line in two_replica_warning(default_metadata_replication_factor(rf, len(ips))):
            print(line)
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
            
            # Check port conflicts. The shell form searched `ss -tlnp` output for the
            # port as a *substring*, which is why this used to warn about port 7000 on a
            # node whose only match was a pid of 7000 -- the endpoint answers with the
            # ports themselves, and holder names them.
            print(f"[{ip}] Checking port conflicts...")
            status, body, _ = run_mtls_spark_api_full(
                ip, "/api/v1/host/listeners", method="GET")
            if status == 200:
                held = {}
                for entry in body.get("listeners") or []:
                    held.setdefault(entry.get("port"), entry.get("process"))
                for port in (7000, 3370):
                    if port in held:
                        holder = held[port] or "an unidentified process"
                        print(f"[WARNING] Port {port} is already in use on {ip} by "
                              f"{holder}. This may cause conflicts.")

            # There is no Secure Boot check here any more, and that is worth stating rather
            # than silently dropping. This refused to create a cluster on any host with Secure
            # Boot enabled and the ELRepo key not enrolled, because DRBD shipped as an
            # out-of-tree kernel module (kmod-drbd9x) the kernel rejects without that key --
            # which took the whole storage layer down. Sidon is a userspace daemon speaking
            # NBD over a unix socket and loads no module, so Secure Boot can simply stay on.
            #
            # provision.py and spark-daemon dropped the same gate when DRBD went; this copy
            # was missed, so `cluster create` kept refusing hosts that nothing required to
            # change. Two of three nodes failed on it.

        # Ensure any running core services are stopped to prevent them interfering with boot
        print("Ensuring any running cluster services are stopped for a clean bootstrap...")
        cleanup_services = ["hylia", "rauru", "logos", "mipha", "spectrum", "spectrum-phx", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "agahnim", "slate", "sidon", "daruk", "hydra-db", "zookeeper"]
        # `ignore_failed` is the `|| true` this used to carry: a service that is not
        # running cannot be stopped, and on a clean host none of them are.
        unit_action_parallel(ips, "stop", cleanup_services, ignore_failed=True)

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

        # Nodes must trust each other's SSH host keys; see peer_host_key_command.
        run_parallel_checked(ips, peer_host_key_command(ips),
                             label="Making each node trust its peers' SSH host keys")

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
        
        claim_results = run_parallel(
            ips, cmd_claim, timeout=STORAGE_PREP_TIMEOUT,
            label="Preparing the extent-store disk (clearing it, then a volume group and thin pool; "
                  "about a minute)")
        
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

        # 3b. The extent store's own volumes.
        #
        # `cluster destroy` removes the volume group, so the claim above leaves a node with a
        # thin pool and nothing for sidon to journal to. Carve the volume, register every
        # further empty disk and record which filesystems are sidon's. Nothing is mounted
        # and /etc/fstab is not touched: sidon mounts what /etc/hci/sidon-disks names, by
        # filesystem UUID, when it starts, and refuses any path whose disk is not there.
        print("Preparing each node's extent store: volume, further disks, and the disk record...")
        run_parallel_checked(ips, shell_script_command(CARVE_SIDON_VOLUME),
                             timeout=STORAGE_PREP_TIMEOUT,
                             label="Carving sidon's journal volume")
        run_parallel_checked(ips, shell_script_command(CLAIM_EXTRA_DISKS),
                             timeout=STORAGE_PREP_TIMEOUT,
                             label="Registering any further disks for the extent store")
        run_parallel_checked(ips, shell_script_command(STAGE_SIDON_DISKS),
                             timeout=STORAGE_PREP_TIMEOUT,
                             label="Writing the record of which disks are sidon's")

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
        # up. The step above prepares the volumes; this starts the daemon, which mounts them
        # by filesystem UUID, and checks that they actually came up.
        print("\n--- Phase 4: Starting the coordination, metadata and storage services ---")

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

        # The Phoenix console's own environment, which Phase 6 needs before it can start the
        # console. Provisioning wrote it unless a destroy has removed it since.
        try:
            for ip in ensure_phoenix_env(ips):
                print(f"[{ip}] wrote the Phoenix console environment (it was missing).")
        except RuntimeError as exc:
            print(f"[ERROR] {exc}")
            sys.exit(1)


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
                # Journal ceiling. See zookeeper_quadlet above for why.
                "LogRateLimitIntervalSec=10s\n"
                "LogRateLimitBurst=100\n"
                "Restart=always\n"
                "CPUWeight=100\n"
                "MemoryMax=512M\n"
                "MemoryHigh=400M\n\n"
                "[Container]\n"
                "Image=docker.io/library/zookeeper:3.9.2\n"
                "Network=host\n"
                # No logback mount here, deliberately, and it is a divergence rather than a
                # decision: nothing writes /etc/hci/zookeeper/logback.xml except
                # deploy_updates.py, so on a cluster that has only ever been provisioned
                # the file this would mount does not exist. See TODO.md.
                "Volume=/var/lib/hci/zookeeper/data:/data:Z\n"
                "Volume=/var/lib/hci/zookeeper/log:/datalog:Z\n"
                # reconfigEnabled from the first boot. See zookeeper_quadlet above.
                f"Environment=ZOO_MY_ID={node_id}{zoo_servers_env}{peer_type_env} "
                f"ZOO_4LW_COMMANDS_WHITELIST=* ZOO_CFG_EXTRA=reconfigEnabled=true\n\n"
                "[Install]\n"
                "WantedBy=multi-user.target\n"
            )
            zk_b64 = base64.b64encode(zk_quad.encode()).decode()
            # The unit file is written through /execute because writing a file has no
            # typed endpoint; the reload it used to be chained to has one.
            run_remote_spark(ip, f"mkdir -p /etc/containers/systemd && echo {zk_b64} | base64 -d > /etc/containers/systemd/zookeeper.container")
            unit_action(ip, "daemon-reload")

        print("Starting ZooKeeper service in parallel...")
        unit_action_checked(ips, "restart", ["zookeeper"])
        for ip in ips:
            for _ in range(30):
                if unit_is_active(ip, "zookeeper"):
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

        print("Starting ScyllaDB one node at a time, seed first...")
        scylla_error = start_scylla_in_order(ips)
        if scylla_error:
            print(f"[ERROR] {scylla_error}")
            sys.exit(1)

        print("Starting Daruk query proxy service on all hosts...")
        unit_action_checked(ips, "restart", ["daruk"])
        print("Waiting for Daruk query proxy to listen on port 9043 on all nodes...")
        for ip in ips:
            daruk_ready = False
            for _ in range(30):
                if port_listening(ip, 9043):
                    daruk_ready = True
                    break
                time.sleep(1)
            if not daruk_ready:
                print(f"[ERROR] Daruk query proxy failed to listen on port 9043 on {ip}")
                sys.exit(1)
        print("Daruk query proxy is ready on all nodes.")

        # The extent store starts last, after Hydra and Daruk. Sidon keeps vdisk ownership
        # as an (owner, epoch) record in Hydra, and MANAGED_SERVICES declares it behind daruk;
        # create used to start it first, the reverse of the order `cluster start` follows.
        print("\nStarting the extent store (sidon) on all nodes...")
        unit_action_checked(ips, "enable", ["sidon"])
        unit_action_checked(ips, "restart", ["sidon"])

        print("Verifying each node's extent store is mounted and answering...")
        for ip in ips:
            rc_cap, out_cap, err_cap = wait_for_sidon_capacity(ip)
            if rc_cap != 0 or not out_cap.strip():
                print(f"[ERROR] [{ip}] sidon did not answer on its control socket within 90s: "
                      f"{(err_cap or 'no output').strip()[:200]}")
                print(f"        What the node says:")
                print(describe_sidon_failure(ip))
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
                      f"none of its disks is mounted. Check /etc/hci/sidon-disks against "
                      f"`/usr/local/bin/sidon mounts` and vg_aether/sidon.")
                return
            for gone in cap.get("absent_disks") or []:
                print(f"[WARN] [{ip}] disk {gone.get('uuid')} ({gone.get('role')}) is not "
                      f"available: {gone.get('reason')}")
            print(f"[{ip}] extent store ready: {total / (1024 ** 3):.1f} GiB.")

        # 6. Start Workload Services
        print("\n--- Phase 6: Starting Core HCI Services ---")
        services = ["spectrum", "spectrum-phx", "bifrost", "dagur", "mimir", "rauru", "vali", "catalyst", "gatoway", "urbosa", "logos", "mipha", "agahnim", "slate", "hylia"]
        
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
            unit_action_checked(ips, "restart", [svc])
            for ip in ips:
                for _ in range(30):
                    if unit_is_active(ip, svc):
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
            zk_cmd = "python3 -c \"import sys; sys.path.insert(0, '/usr/local/bin'); import helios_zk; print('Mode: ' + (helios_zk.server_mode('127.0.0.1', timeout=2.0) or 'unknown'))\""
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
                if port_listening(ip, 8443):
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

        # The Phoenix console listens on 8444, on loopback only, behind Slate. Bifrost's
        # health guard refuses to bind the VIP while Slate's console backend is down, so a
        # console that is not listening here is a cluster with no VIP, found by Mimir later
        # and less legibly than by the port.
        print("Verifying Phoenix console reachability on port 8444...")
        phoenix_healthy = True
        for ip in ips:
            reached = False
            for _ in range(20):
                if port_listening(ip, 8444):
                    reached = True
                    break
                time.sleep(2)
            if not reached:
                print(f"[ERROR] Phoenix console is not listening on {ip}:8444.")
                phoenix_healthy = False
            else:
                print(f"[{ip}] Phoenix console is listening on port 8444.")

        if not phoenix_healthy:
            sys.exit(1)

        print("Running diagnostic verification checks using Mimir...")
        ran_ok, out_m, failing_lines = run_health_checks_settled(ips[0])
        if not ran_ok:
            print(f"[ERROR] Mimir health check execution failed.")
            sys.exit(1)
        fail_count = len(failing_lines)
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
        # No cluster configured is an answer in its own right. Without this the command
        # fell back to 127.0.0.1 and reported a table of DOWN services for a cluster that
        # does not exist -- which reads as a cluster in trouble rather than as no cluster.
        if not args.servers and configured_cluster_ips() is None:
            print_no_cluster(as_json=getattr(args, "json", False))
            sys.exit(1)

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
        # `cluster start` names no service, and that is the property worth keeping.
        #
        # It used to start ZooKeeper, then ScyllaDB, then Daruk, then thirteen more units
        # by hand, waiting on each -- and then wait again for the reconcile loop to
        # converge the same services toward the state recorded in its first phase. Two
        # actors drove every service on a cold start, which is what the visible flapping
        # was, and the CLI's copy of the ordering was free to rot: it went on restarting
        # `aether` for months after the unit was deleted, failing every start on a service
        # that does not exist, because nothing read that list but the CLI itself.
        #
        # What is left is what a declaration is: record the intent on each node, then watch
        # what the nodes publish. The ordering lives in the reconcile loop, next to the
        # thing that acts on it.
        print("==========================================================")
        print("                 Starting HCI Cluster                     ")
        print("==========================================================")
        ips = get_cluster_ips()
        print(f"Connecting to cluster nodes: {', '.join(ips)}")

        acquire_cluster_lock(ips)
        import atexit
        atexit.register(release_cluster_lock, ips)

        print("\n--- Declaring the desired cluster state ---")
        declared = declare_cluster_state(ips, "started")
        if not declared:
            print("[ERROR] No node accepted the desired cluster state; nothing was started.")
            sys.exit(1)
        print(f"Desired state 'started' recorded via {', '.join(declared)}.")

        print("\n--- Waiting for the cluster to converge ---")
        converged = wait_until_error_or_done(ips, op="start")

        print("\n--- Cluster Services Status ---")
        final_state = zk_read_cluster_state()
        if final_state and final_state["nodes"]:
            print_cluster_table(final_state["nodes"], ips)
        else:
            print("  (ZooKeeper unreachable; run 'cluster status' for a direct probe)")
        print("==========================================================")

        if not converged:
            print(f"{RED}The cluster did not converge. The table above says which service "
                  f"on which node, and why.{RESET}")
            sys.exit(1)

        # Post-start verification of the things no single node can see about itself.
        #
        # Per-service liveness is not checked here any more: a node publishes its own
        # services and the wait above already refused to finish until every one of them
        # reported a PID. What is left is the cluster-level properties -- one ensemble
        # leader, peers that can reach each other, and the diagnostic suite.
        print("\n--- Cluster Health Verification ---")

        print("Checking ZooKeeper consensus quorum...")
        leaders = 0
        followers = 0
        zk_healthy = True
        for ip in ips:
            zk_cmd = "python3 -c \"import sys; sys.path.insert(0, '/usr/local/bin'); import helios_zk; print('Mode: ' + (helios_zk.server_mode('127.0.0.1', timeout=2.0) or 'unknown'))\""
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

        # Peers can reach each other.
        #
        # This used to wait up to 45 seconds for Mipha to promote the linstor-db DRBD
        # volume and bring a controller up on one node, then confirm it was listening on
        # 3370. There is no controller and no election: each node runs its own daemon and
        # answers for itself, so the question is reachability rather than leadership.
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
                # Not fatal: writes are only refused when a node in a vdisk's own replica
                # set is down. But a peer that cannot be reached now will not be reachable
                # when one is, so it is said out loud rather than discovered by the first
                # failed write.
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
        # The same shape as `cluster start`, inverted, and it names no service either.
        #
        # It used to stop twelve workload units, then drain the storage journals, then
        # unmount, then stop the storage and database units, then ZooKeeper -- an ordering
        # that only ran when an operator typed this command, and a second copy of the one
        # the reconcile loop already walks in reverse. The draining moved into the loop,
        # next to the stop it has to precede.
        print("==========================================================")
        print("                 Stopping HCI Cluster                     ")
        print("==========================================================")

        ips = get_cluster_ips()
        acquire_cluster_lock(ips)
        import atexit
        atexit.register(release_cluster_lock, ips)

        # Guests first, and from the CLI rather than the loop: a VM is cluster state, not
        # node state. Which host a guest is on is a scheduling decision recorded in the
        # database, so shutting guests down is one decision taken once, not fifteen nodes
        # each deciding about whatever they happen to be running.
        print("--- Stopping running VMs ---")
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
            stop_vms_together(running_vms)
        else:
            print("No running VMs detected.")

        print("\n--- Declaring the desired cluster state ---")
        declared = declare_cluster_state(ips, "stopped")
        if not declared:
            print("[ERROR] No node accepted the desired cluster state; nothing was stopped.")
            sys.exit(1)
        print(f"Desired state 'stopped' recorded via {', '.join(declared)}.")

        print("\n--- Waiting for the cluster to converge ---")
        converged = wait_until_error_or_done(ips, op="stop")

        if not converged:
            print("\n--- Cluster Services Status ---")
            final_state = zk_read_cluster_state()
            if final_state and final_state["nodes"]:
                print_cluster_table(final_state["nodes"], ips)
            print("==========================================================")
            # The state store stays up on purpose. Taking it down now would leave the
            # services that are still running with no desired state to read, and a node
            # that cannot read intent changes nothing -- services up, no way to converge
            # them, and nothing to say why.
            print(f"{RED}The cluster did not converge; the state store was left running "
                  f"so the stop can be retried.{RESET}")
            sys.exit(1)

        # Last, and only once everything that reads it has gone: the store the desired
        # state lives in. This is an explicit operator instruction to quiesce, which is why
        # it is a separate call and not something the reconcile loop could ever do -- a loop
        # cannot stop the thing it reads its instructions from.
        print("\n--- Quiescing the state store ---")
        quiesced = declare_cluster_state(ips, "stopped", stop_state_store=True)
        if len(quiesced) != len(ips):
            print(f"{YELLOW}The state store is still running on: "
                  f"{', '.join(sorted(set(ips) - set(quiesced)))}{RESET}")

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

        if not confirm_destroy(ips, assume_yes=args.yes):
            sys.exit(1)

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
        services = ["hylia", "rauru", "logos", "mipha", "spectrum", "spectrum-phx", "bifrost", "dagur", "mimir", "vali", "catalyst", "gatoway", "urbosa", "agahnim", "slate", "sidon", "daruk", "hydra-db", "zookeeper"]
        for ip in ips:
            print(f"[{ip}] Stopping services: {', '.join(services)}")
            ok, detail = unit_action(ip, "stop", services, ignore_failed=True)
            if not ok:
                print(f"[{ip}] [WARNING] Failed to stop services: {detail}")

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
            unit_action(ip, "stop", ["sidon"], ignore_failed=True)
            run_remote_spark(ip, SIDON_TEARDOWN)
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
run_with_timeout(__SIDON_TEARDOWN__, timeout=30)

print("Removing system containers...", flush=True)
run_with_timeout("podman rm -f systemd-hydra-db systemd-zookeeper systemd-spectrum spectrum-phx || true", timeout=15)

print("Removing storage directories...", flush=True)
run_with_timeout("rm -rf /var/lib/hci/zookeeper/data /var/lib/hci/zookeeper/log /var/lib/hci/hydra/data /var/lib/hci/aether/data /var/lib/hci/aether/volumes /var/lib/hci/aether/images /var/lib/hci/aether/nvram /run/hci/*", timeout=10)
run_with_timeout("rm -rf --one-file-system /etc/hci/odin /etc/hci/spectrum /etc/hci/cluster.json /var/lib/hci/sidon", timeout=10)
print("--- Local wipe completed ---", flush=True)
"""
        # The teardown is one shell command shared with the unmount above, so the wipe
        # cannot disagree with it about what sidon's mounts are.
        wipe_script = wipe_script.replace("__SIDON_TEARDOWN__", repr(SIDON_TEARDOWN))
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
            ok, detail = unit_action(ip, "restart", ["spark-daemon"], detach=True)
            if not ok:
                print(f"[{ip}] [WARNING] Failed to launch background spark-daemon restart: {detail}")
        missing = wait_for_spark_daemons(ips)
        if missing:
            print(f"[WARNING] spark-daemon did not answer again within 60s on: {', '.join(missing)}")

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
                    # Renumber, because node_id is an index other tooling counts on: whether a
                    # ZooKeeper member votes or observes follows its position in this list
                    # (the first three vote), so a removal slides everyone after it up a place
                    # and the ids have to follow.
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

            # The ensemble, shrunk to match. `add-node` grows it as part of the join, and
            # this used to tell the operator to do the reverse by hand -- an asymmetry
            # with teeth. A survivor whose unit still lists the departed node counts it in
            # the quorum, so a three-entry ensemble with two live members needs both of
            # them: the cluster ends up *less* fault-tolerant than the two-node cluster it
            # has actually become, and one more failure loses consensus entirely.
            #
            # Survivors keep the ids they already hold, so removing the middle member
            # leaves a gap rather than renumbering the one after it.
            #
            # First, though, ask for it rather than arrive at it: see
            # hand_off_ensemble_vote. The rewrite below is what happens when it cannot.
            handed_off, note = hand_off_ensemble_vote(target, survivors)
            print(note)
            survivor_ids = {} if handed_off else read_zookeeper_ids(survivors)
            unreadable = [ip for ip in survivors if ip not in survivor_ids]
            if handed_off:
                pass  # the reconfiguration wrote the units already, and correctly
            elif unreadable:
                print(f"[WARNING] Could not read the ZooKeeper id of: {', '.join(unreadable)}. "
                      f"The ensemble was left alone -- rewriting it without those ids would "
                      f"hand a node an identity that does not match its data directory.")
                print(f"[WARNING] Remove {target} from every survivor's "
                      f"/etc/containers/systemd/zookeeper.container by hand, then restart "
                      f"zookeeper on each in turn.")
            else:
                zk_members = [(survivor_ids[ip], ip) for ip in survivors]
                print(f"[zookeeper] rewriting the ensemble for {len(zk_members)} member(s)...")
                failed_zk = write_zookeeper_ensemble(zk_members)
                if failed_zk:
                    print(f"[WARNING] Could not write the ZooKeeper unit on: "
                          f"{', '.join(failed_zk)}. Those nodes still count {target} "
                          f"towards their quorum.")
                else:
                    # One at a time, as in add-node: a rolling restart keeps a quorum of
                    # the *previous* ensemble alive throughout, which an all-at-once
                    # restart does not.
                    for ip in survivors:
                        ok_z, err_z = unit_action(ip, "restart", ["zookeeper"])
                        if not ok_z:
                            print(f"[WARNING] [{ip}] ZooKeeper did not restart: "
                                  f"{(err_z or '').strip()[:200]}")
                        else:
                            print(f"[zookeeper] {ip} restarted.")
                        time.sleep(3)

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
                # Either way the database ends up on two replicas when two nodes remain.
                for line in two_replica_warning(min(replication_factor, remaining)):
                    print("     " + line)
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
            print(f"     cluster.json entry and its hydra.nodes row, and to shrink the")
            print(f"     ZooKeeper ensemble to the survivors -- which is not optional:")
            print(f"     until it happens they still count {target} towards their quorum.")
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
