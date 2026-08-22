#!/usr/bin/env python3
import time
import json
import socket
import subprocess
import base64
import sys
import os
import re
import uuid
import hashlib
import ipaddress
import tempfile

# Distributed firewall rules live in their own chain so they can be rebuilt
# wholesale each pass instead of being appended to FORWARD forever.
FW_CHAIN = "URBOSA-FWD"
FW_PROTOCOLS = ("ANY", "TCP", "UDP", "ICMP")

# How long a pass waits before repeating. Kept as a constant because the
# reclaimer's log messages quote it.
LOOP_SECONDS = 15

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

def read_iface_addresses(iface, ns_name=None, run=None, scope=None):
    """Addresses on one interface, or None if they could not be read at all.

    Callers that only decide whether to add an address treat None as "not
    present" and re-add it, which is harmless. The reclaimer must not: it
    refuses to delete a bridge that carries a host address, and "the query
    failed" arriving as "no addresses" would turn that guard off exactly when
    the host is least healthy.

    `scope` filters to one address scope. The reclaimer asks for "global",
    because every bridge that is up carries a kernel-assigned fe80::/64 link
    local address - checked on a live node, where treating that as "somebody
    configured an address here" refused to reclaim any bridge at all, ever.
    """
    runner = run or run_cmd
    prefix = f"ip netns exec {ns_name} " if ns_name else ""
    rc, stdout, _ = runner(f"{prefix}ip -json addr show {iface}")
    if rc != 0:
        return None
    addrs = []
    if stdout:
        try:
            for entry in json.loads(stdout):
                for addr in entry.get("addr_info", []):
                    local = addr.get("local")
                    if not local:
                        continue
                    if scope is not None and addr.get("scope") != scope:
                        continue
                    addrs.append(local)
        except Exception as e:
            sys.stderr.write(f"Error parsing addresses for {iface}: {e}\n")
            return None
    return addrs

def get_iface_addresses(iface, ns_name=None):
    """Returns the exact addresses on one interface, optionally inside a namespace.

    Same substring hazard as get_local_addresses(): a segment gateway 10.0.0.1 is
    a substring of 10.0.0.10, so a plain `in` test reports the gateway as already
    assigned and the segment is left without one.
    """
    return read_iface_addresses(iface, ns_name) or []

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

# Last read failure logged per table, so a sustained outage does not repeat the
# same message on every pass.
db_read_errors = {}

def read_json_table(table, columns="*", consequence="Skipping this pass."):
    """Reads a table as a list of dicts, or None if the read cannot be trusted.

    The distinction between None and [] is the whole point, and it is why every
    caller below returns it unchanged. The desired state read here is what the
    reconciler builds the host from and what the reclaimer deletes host resources
    for being absent from: a failed query arriving as an empty list reads as
    "every router, segment and bridge was deleted at once" and would take the
    entire overlay down on a single database blip. Gatoway carries the same
    guard for the same reason - see get_db_networks() there.

    A row that looks like JSON but does not parse invalidates the whole read.
    A partial set is worse than no set, because the rows that vanished are
    indistinguishable from rows an operator deleted.
    """
    cql = f"SELECT JSON {columns} FROM {table};"
    rc, stdout, stderr = run_cql_query(cql)
    err = None
    items = []
    if rc != 0:
        err = f"Read of {table} failed (rc={rc}): {stderr or stdout}."
    else:
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    items.append(json.loads(line))
                except Exception as e:
                    err = f"Unparseable row in {table} ({e}); treating this read as untrustworthy."
                    break

    # Logged on change only: this runs every pass.
    if err != db_read_errors.get(table):
        if err:
            print(f"{err} {consequence}")
        elif db_read_errors.get(table) is not None:
            print(f"Read of {table} recovered; resuming reconciliation.")
        db_read_errors[table] = err

    if err:
        return None
    return items

def get_db_routers_t0():
    return read_json_table(
        "hydra.urbosa_t0_routers",
        consequence="Skipping this pass entirely; namespaces and links are left exactly as they are.")

def get_db_routers_t1():
    return read_json_table(
        "hydra.urbosa_t1_routers",
        consequence="Skipping this pass entirely; namespaces and links are left exactly as they are.")

def get_db_segments():
    return read_json_table(
        "hydra.urbosa_segments",
        consequence="Skipping this pass entirely; bridges and tunnels are left exactly as they are.")

def get_db_node_ips():
    """Peer IPs for the VXLAN mesh, or None if hydra.nodes could not be read.

    An empty result is also returned as None: this host is itself a row in
    hydra.nodes, so "zero nodes" is never a true answer, and the flood-entry
    reclaimer would read it as "every peer has been decommissioned" and prune
    the entire mesh.
    """
    rows = read_json_table(
        "hydra.nodes", "ip",
        consequence="VXLAN flood entries are left exactly as they are.")
    if rows is None:
        return None
    ips = [r.get("ip") for r in rows if r.get("ip")]
    return ips or None

def get_db_firewall_rules():
    """Reads the firewall table.

    Returns None if the read failed and a list (possibly empty) if it
    succeeded. The chain is rebuilt from this result, so a partial read would
    silently DELETE the rules that failed to parse - the caller must skip the
    rebuild entirely rather than act on an incomplete ruleset.
    """
    return read_json_table(
        "hydra.urbosa_firewall_rules",
        consequence=f"Skipping firewall rebuild; leaving {FW_CHAIN} as-is.")

# --------------------------------------------------------------------------------
# Transit /30 allocation
# --------------------------------------------------------------------------------
#
# Every Tier-1 router reaches its Tier-0 over a point-to-point link out of
# 100.64.0.0/16. The addressing used to be *derived*: md5(router_id)[:4] % 16384
# indexed into that /16. Deriving it means two routers whose hashes collide are
# handed the same /30 - the same two addresses on both links - and the Tier-0
# then has two routes to two tenants' subnets pointing at one next hop. Whichever
# veth the ARP cache resolved first wins, silently, and the other tenant's return
# traffic is delivered into the wrong namespace. 16384 slots put the first
# expected collision at roughly 150 routers (birthday bound), 178 in the
# arithmetic recorded in TODO.md, and nothing in the old code checked.
#
# So the allocation is recorded rather than derived. One row per slot, claimed
# with a lightweight transaction keyed on the slot, which is what makes two
# claimants racing for the same slot resolvable at all: the loser is told it
# lost. The hashed value survives as the *preferred* slot, so a cluster upgrading
# into this keeps the transit addressing it already has wherever that slot is
# free, and only the colliding minority move.
#
# `router_id` is text and `allocated_at_ms` is bigint, not uuid and timestamp,
# for the reason hydra.cluster_locks records: a refused lightweight transaction
# returns the whole existing row, and Daruk's make_serializable passes driver
# UUID and datetime objects through untouched, so json.dumps raises on exactly
# the response that says who won the slot. Checked against the live cluster - a
# uuid column turned every refusal into "Object of type UUID is not JSON
# serializable", which arrives here as an unexplained query failure.
TRANSIT_TABLE = "hydra.urbosa_transit_pool"
TRANSIT_SLOTS = 16384        # 100.64.0.0/16 divided into /30s: 2 ** (30 - 16)
TRANSIT_CLAIM_ATTEMPTS = 8   # bounded: a pass must not spin on a contended pool

def transit_addresses(index):
    """(t0_ip, t1_ip, network) for a pool slot, as CIDR strings.

    Slot 0 is 100.64.0.0/30 with .1 on the Tier-0 side and .2 on the Tier-1 side.
    This is the same arithmetic the derived scheme used, so a slot number means
    the same three addresses before and after this change - which is what lets an
    upgrade keep existing links in place.
    """
    index = int(index)
    if not 0 <= index < TRANSIT_SLOTS:
        raise ValueError(f"transit slot {index} is outside 0..{TRANSIT_SLOTS - 1}")
    octet2 = (index >> 6) & 0xff
    octet3 = (index & 0x3f) * 4
    return (f"100.64.{octet2}.{octet3 + 1}/30",
            f"100.64.{octet2}.{octet3 + 2}/30",
            f"100.64.{octet2}.{octet3}/30")

def preferred_transit_index(router_id):
    """The slot the pre-pool build would have derived for this router.

    Kept only as a preference. md5 here is a spreading function, not a security
    primitive - it is what the deployed clusters were addressed with.
    """
    return int(hashlib.md5(str(router_id).encode()).hexdigest()[:4], 16) % TRANSIT_SLOTS

def is_uuid(value):
    """True if value is a UUID, which is the only thing interpolated into CQL here.

    Router ids come back out of the database, but they went in through the web
    API, and everything below builds CQL text by interpolation.
    """
    try:
        uuid.UUID(str(value))
        return True
    except (ValueError, AttributeError, TypeError):
        return False

def lwt_was_applied(stdout):
    """Whether a conditional write actually took effect.

    A rejected lightweight transaction is not an error. It returns rc=0 with a
    row whose first column is [applied]=False, so the return code alone reports a
    lost race as a success - which is precisely how an allocation scheme ends up
    handing the same slot to two routers. Only the first field is read, because
    run_cql_query flattens rows to space-joined values and a refusal arrives
    through Daruk as "False 41 0f0f-...". helios_schema.lwt_applied documents the
    same two shapes for the schema lock.
    """
    text = (stdout or "").strip()
    if not text:
        return False
    if "[applied]" in text:
        # cqlsh fallback path: header, rule, then the row.
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or "[applied]" in stripped or set(stripped) <= set("-+ "):
                continue
            first = stripped.split("|", 1)[0].strip()
            if first in ("True", "true"):
                return True
            if first in ("False", "false"):
                return False
        return False
    first = text.split(None, 1)[0]
    return first in ("True", "true")

def read_transit_pool():
    """{slot: router_id} for every recorded allocation, or None if unreadable.

    None here means "do not touch transit links this pass". An empty pool is a
    perfectly normal answer - it is what a cluster with no Tier-1 routers has.
    """
    rows = read_json_table(
        TRANSIT_TABLE, "subnet_index, router_id",
        consequence=("Transit links are left exactly as they are; new Tier-1 routers "
                     "will wait rather than risk a colliding /30."))
    if rows is None:
        return None
    pool = {}
    for row in rows:
        try:
            index = int(row.get("subnet_index"))
        except (TypeError, ValueError):
            continue
        router_id = row.get("router_id")
        if router_id:
            pool[index] = str(router_id)
    return pool

def next_free_transit_index(router_id, pool):
    """The slot to try for router_id: its preferred one, else the lowest free one."""
    preferred = preferred_transit_index(router_id)
    if preferred not in pool:
        return preferred
    for index in range(TRANSIT_SLOTS):
        if index not in pool:
            return index
    return None

# Slots already announced, so a stable allocation is logged once and not every pass.
transit_announced = {}

def claim_transit_index(router_id, node_id, pool=None):
    """The slot allocated to router_id, claiming one if it does not have one yet.

    Returns None when no allocation could be made, which the caller must read as
    "leave this transit link alone this pass". Falling back to the derived index
    on a database failure would reintroduce exactly the collision this replaces,
    and would do it at the worst possible moment.
    """
    if not is_uuid(router_id):
        print(f"Urbosa transit: router id {router_id!r} is not a UUID; refusing to allocate a transit /30 for it.")
        return None

    for _attempt in range(TRANSIT_CLAIM_ATTEMPTS):
        if pool is None:
            pool = read_transit_pool()
        if pool is None:
            return None

        for index, owner in sorted(pool.items()):
            if owner == str(router_id):
                if transit_announced.get(str(router_id)) != index:
                    _t0, _t1, network = transit_addresses(index)
                    print(f"Urbosa transit: Tier-1 router {router_id} holds slot {index} ({network}).")
                    transit_announced[str(router_id)] = index
                return index

        candidate = next_free_transit_index(router_id, pool)
        if candidate is None:
            print(f"Urbosa transit: all {TRANSIT_SLOTS} transit /30s in 100.64.0.0/16 are allocated; "
                  f"router {router_id} cannot be given one.")
            return None

        rc, stdout, stderr = run_cql_query(
            f"INSERT INTO {TRANSIT_TABLE} (subnet_index, router_id, node_id, allocated_at_ms) "
            f"VALUES ({candidate}, '{router_id}', '{node_id}', {int(time.time() * 1000)}) "
            f"IF NOT EXISTS;")
        if rc != 0:
            print(f"Urbosa transit: claim of slot {candidate} for router {router_id} failed: {stderr or stdout}")
            return None
        # A refused claim comes back rc=0 with [applied]=False and the winning
        # row beside it, which is why this reads the result rather than the
        # return code. Trusting rc here is how two routers end up on one /30.
        if lwt_was_applied(stdout):
            _t0, _t1, network = transit_addresses(candidate)
            print(f"Urbosa transit: allocated slot {candidate} ({network}) to Tier-1 router {router_id}.")
            transit_announced[str(router_id)] = candidate
            return candidate

        # Somebody else took that slot between the read and the write. Re-read and
        # try again: the winner may even have been another node claiming it for
        # this same router, in which case the next pass through the loop adopts it.
        pool = None

    print(f"Urbosa transit: gave up claiming a slot for router {router_id} after "
          f"{TRANSIT_CLAIM_ATTEMPTS} contended attempts; will retry next pass.")
    return None

def release_transit_index(index, router_id):
    """Frees one slot, but only if it is still recorded against router_id.

    Conditional on the owner because the reclaimer runs on every node: an
    unconditional delete lets a stale view of the pool free a slot that has since
    been reallocated, and the next router to claim it would collide with a live
    link - the failure this whole mechanism exists to prevent.
    """
    if not is_uuid(router_id):
        return False
    rc, stdout, stderr = run_cql_query(
        f"DELETE FROM {TRANSIT_TABLE} WHERE subnet_index = {int(index)} "
        f"IF router_id = '{router_id}';")
    if rc != 0:
        print(f"Urbosa transit: could not release slot {index}: {stderr or stdout}")
        return False
    transit_announced.pop(str(router_id), None)
    return lwt_was_applied(stdout)

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

# --------------------------------------------------------------------------------
# Resource reclamation
# --------------------------------------------------------------------------------
#
# Deleting a router or a segment from the database used to delete nothing at all
# from the host. The namespace kept routing, the bridge kept forwarding, the
# tunnel kept carrying the VNI, and the veth kept the two joined - so a segment an
# operator had removed for a reason went on passing traffic until the next reboot.
# On a long-lived cluster the kernel objects only ever accumulate.
#
# The dangerous half of fixing that is the other direction. A collector that reads
# "not in the database" as "delete it" will, on the day the database is briefly
# unreachable, tear the NIC out of every running VM. Three rules keep that from
# happening, and every refusal below is one of them:
#
#   1. Nothing is reclaimed unless the desired state was read successfully. That
#      is enforced by the caller: read_json_table() returns None, not [].
#   2. Only names Urbosa generates are candidates, matched whole. A namespace or
#      link that does not match is not ours and is never touched, whatever it is.
#   3. A candidate must be provably idle. Anything unrecognised inside it, and
#      anything we could not read, refuses the deletion and says so. Leaking one
#      namespace is recoverable; deleting a live one is not.
NS_T0_RE = re.compile(r"^ns-t0-([0-9a-f]{8})$")
NS_T1_RE = re.compile(r"^ns-t1-([0-9a-f]{8})$")
BR_OV_RE = re.compile(r"^br-ov-(\d+)$")
VXLAN_RE = re.compile(r"^vxlan-(\d+)$")
VETH_OV_RE = re.compile(r"^veth-ov-(\d+)$")
VETH_T1_RE = re.compile(r"^veth-t1-(\d+)$")
TRANSIT_VETH_RE = re.compile(r"^t[01]-([0-9a-f]{8})$")
MACVLAN_T0_RE = re.compile(r"^mv-t0-([0-9a-f]{8})$")

# The all-zero destination MAC is how a static VXLAN mesh floods: one entry per
# peer, "send broadcast, unknown-unicast and multicast there too". Learned unicast
# entries are a different thing entirely and are never touched - deleting one
# blackholes a live VM until the fabric relearns it.
FLOOD_MAC = "00:00:00:00:00:00"

class ReclaimAction:
    """One resource to remove: what it is, why it is garbage, and how to remove it."""

    def __init__(self, kind, target, reason, commands=(), queries=()):
        self.kind = kind
        self.target = target
        self.reason = reason
        self.commands = list(commands)
        self.queries = list(queries)

    def __repr__(self):
        return f"<ReclaimAction {self.kind} {self.target}>"

class ReclaimRefusal:
    """A resource that looks orphaned but must not be removed, and why not."""

    def __init__(self, kind, target, reason):
        self.kind = kind
        self.target = target
        self.reason = reason

    def __repr__(self):
        return f"<ReclaimRefusal {self.kind} {self.target}>"

def list_netns(run=None):
    """[(name, nsid)] from `ip netns list`, or [] if the list could not be read.

    Parsed as text rather than with `ip -j`: the JSON form of the netns
    subcommand is newer than the addr and link forms used elsewhere in this file,
    and the text form already carries both fields:

        ns-t1-aabbccdd (id: 0)

    The nsid is the number the *root* namespace uses to refer to that namespace,
    which is what a veth's link_netnsid points at, and is how the peer end of a
    host-side veth is identified below.

    An unreadable list yields no candidates, which is the safe direction: the
    reclaimer then proposes nothing.
    """
    runner = run or run_cmd
    rc, stdout, _ = runner("ip netns list")
    if rc != 0 or not stdout:
        return []
    entries = []
    for line in stdout.splitlines():
        parts = line.split()
        if not parts:
            continue
        match = re.search(r"\(id:\s*(\d+)\)", line)
        entries.append((parts[0], int(match.group(1)) if match else None))
    return entries

def list_links(run=None):
    """{ifname: {kind, master, netnsid}} for the root namespace.

    The device kind comes from `ip -d` and is required to match before anything
    is deleted. A name is a hint; the kind is the fact. Refusing to delete a
    br-ov-9 that is not actually a bridge costs nothing and stops a name
    collision from turning into a deleted device.
    """
    runner = run or run_cmd
    rc, stdout, _ = runner("ip -d -json link show")
    if rc != 0 or not stdout:
        return {}
    links = {}
    try:
        entries = json.loads(stdout)
    except Exception as e:
        sys.stderr.write(f"Error parsing 'ip -d -json link show': {e}\n")
        return {}
    for entry in entries:
        name = entry.get("ifname")
        if not name:
            continue
        links[name] = {
            "kind": (entry.get("linkinfo") or {}).get("info_kind"),
            "master": entry.get("master"),
            "netnsid": entry.get("link_netnsid"),
        }
    return links

def list_bridge_ports(bridge, run=None):
    """Interfaces enslaved to bridge, or None if that could not be determined.

    None must be read as "busy", never as "empty". `ip link show master X` exits
    non-zero when X does not exist and prints an empty JSON array when it exists
    with nothing attached, so the two are distinguishable - and worth
    distinguishing, because the empty case is the only one that permits deleting
    the bridge.
    """
    runner = run or run_cmd
    rc, stdout, _ = runner(f"ip -json -o link show master {bridge}")
    if rc != 0:
        return None
    ports = []
    try:
        for entry in json.loads(stdout or "[]"):
            name = entry.get("ifname")
            if name:
                ports.append(name.split("@")[0])
    except Exception:
        return None
    return ports

def list_netns_links(ns_name, run=None):
    """Interface names inside a namespace, or None if they could not be read."""
    runner = run or run_cmd
    rc, stdout, _ = runner(f"ip -json -n {ns_name} link show")
    if rc != 0:
        return None
    names = []
    try:
        for entry in json.loads(stdout or "[]"):
            name = entry.get("ifname")
            if name:
                names.append(name.split("@")[0])
    except Exception:
        return None
    return names

def list_netns_processes(ns_name, run=None, argv_reader=None):
    """[(pid, command, argv)] for processes inside a namespace, or None if unreadable.

    Namespace membership, not the process table: `ip netns exec <ns> ps` shows
    every process on the host, because netns exec swaps only the network
    namespace. That mistake already cost this daemon a DHCP server that never
    started - see netns_dnsmasq_pids().
    """
    runner = run or run_cmd
    read_argv = argv_reader or read_proc_argv
    rc, stdout, _ = runner(f"ip netns pids {ns_name}")
    if rc != 0:
        return None
    procs = []
    for pid in (stdout or "").split():
        if not pid.isdigit():
            continue
        argv = read_argv(pid)
        procs.append((pid, os.path.basename(argv[0]) if argv else "", argv))
    return procs

def dnsmasq_interface(argv):
    """The interface a dnsmasq was bound to, from its argv, or None.

    Exact-token match on the whole `--interface=X` argument, so veth-t1-10 is
    never read as veth-t1-100 - the same trap netns_dnsmasq_pids() documents.
    """
    for arg in argv or []:
        if arg.startswith("--interface="):
            return arg.split("=", 1)[1]
    return None

def list_netns_routes(ns_name, run=None):
    """[(destination, gateway)] inside a namespace, or None if unreadable.

    Only routes with a gateway are returned. Connected routes have no next hop
    and are removed with their interface, so nothing here needs to consider them.
    """
    runner = run or run_cmd
    rc, stdout, _ = runner(f"ip -json -n {ns_name} route show")
    if rc != 0:
        return None
    routes = []
    try:
        for entry in json.loads(stdout or "[]"):
            dst = entry.get("dst")
            gateway = entry.get("gateway")
            if dst and gateway:
                routes.append((dst, gateway))
    except Exception:
        return None
    return routes

def list_flood_entries(device, run=None):
    """Peer addresses in device's flood list, or None if it could not be read.

    Only the all-zero-MAC "self" entries, which are the head-end replication list
    this daemon appends to. Learned unicast entries are deliberately excluded.
    """
    runner = run or run_cmd
    rc, stdout, _ = runner(f"bridge -json fdb show dev {device}")
    if rc != 0:
        return None
    peers = []
    try:
        for entry in json.loads(stdout or "[]"):
            if entry.get("mac") != FLOOD_MAC:
                continue
            if "self" not in (entry.get("flags") or []):
                continue
            dst = entry.get("dst")
            if dst:
                peers.append(dst)
    except Exception:
        return None
    return peers

def collect_inventory(desired, run=None, argv_reader=None):
    """Everything on this host that the reclaimer is allowed to have an opinion on.

    Gathering is separated from deciding so the decision is a pure function of
    two dictionaries and can be tested without a kernel.
    """
    runner = run or run_cmd
    netns = list_netns(runner)
    ns_names = [name for name, _nsid in netns]
    inventory = {
        "netns": netns,
        "ns_names": set(ns_names),
        "nsid_names": {nsid: name for name, nsid in netns if nsid is not None},
        "links": list_links(runner),
        "bridge_ports": {},
        "bridge_addrs": {},
        "ns_links": {},
        "ns_procs": {},
        "ns_routes": {},
        "flood": {},
    }

    for name in ns_names:
        if not (NS_T0_RE.match(name) or NS_T1_RE.match(name)):
            continue
        inventory["ns_links"][name] = list_netns_links(name, runner)
        inventory["ns_procs"][name] = list_netns_processes(name, runner, argv_reader)

    for name, attrs in inventory["links"].items():
        if BR_OV_RE.match(name):
            inventory["bridge_ports"][name] = list_bridge_ports(name, runner)
            inventory["bridge_addrs"][name] = read_iface_addresses(name, run=runner, scope="global")

    # Flood lists are only read for tunnels that should exist: a tunnel that
    # should not is being deleted whole, entries included.
    for vni in sorted(desired.get("vnis") or ()):
        device = f"vxlan-{vni}"
        if device in inventory["links"]:
            inventory["flood"][device] = list_flood_entries(device, runner)

    for ns_name in sorted((desired.get("t0_routes") or {}).keys()):
        if ns_name in inventory["ns_names"]:
            inventory["ns_routes"][ns_name] = list_netns_routes(ns_name, runner)

    return inventory

def namespace_blocker(ns_name, desired, inventory):
    """Why ns_name must not be deleted, or None if deleting it is safe.

    "It exists and the database does not mention it" is not sufficient evidence.
    A namespace can hold something Urbosa never put there - a tap an operator
    moved in, a packet capture, a container runtime that happened to pick the
    same name - and `ip netns del` takes all of it with the namespace. So the
    namespace has to be recognisably and entirely ours: every interface a name
    this daemon generates, every process a dnsmasq this daemon would have
    started. Anything unrecognised refuses, and so does anything that could not
    be read at all.
    """
    devices = inventory["ns_links"].get(ns_name)
    if devices is None:
        return "its interface list could not be read"
    vnis = desired.get("vnis") or set()
    for device in devices:
        if device == "lo":
            continue
        segment_veth = VETH_T1_RE.match(device)
        if segment_veth and int(segment_veth.group(1)) in vnis:
            # The router row is gone but a segment attached to it is not, and
            # this interface is that segment's default gateway. Deleting the
            # namespace would strip the gateway from a segment that is still
            # configured and still carrying traffic. Refuse and name the
            # segment: removing it is the operator's decision, not ours.
            return (f"segment VNI {segment_veth.group(1)} is still configured and this namespace "
                    f"holds its gateway on {device}; delete the segment first")
        if (TRANSIT_VETH_RE.match(device) or segment_veth
                or MACVLAN_T0_RE.match(device)):
            continue
        return f"it carries interface {device}, which Urbosa did not create"

    processes = inventory["ns_procs"].get(ns_name)
    if processes is None:
        return "its process list could not be read"
    for pid, command, _argv in processes:
        if "dnsmasq" not in command:
            return f"process {pid} ({command or 'unidentified'}) is running inside it, and Urbosa did not start it"
    return None

def plan_reclamation(desired, inventory):
    """(actions, refusals) for one host, from desired state and observed state.

    Pure: it runs no commands and reads nothing. Every deletion it proposes is
    justified by a name Urbosa owns being absent from the desired state, and by
    the observed resource being provably idle.
    """
    actions = []
    refusals = []
    links = inventory["links"]
    vnis = desired.get("vnis") or set()
    t0_prefixes = desired.get("t0_prefixes") or set()
    t1_prefixes = desired.get("t1_prefixes") or set()
    is_leader_here = desired.get("is_leader", False)

    # -- overlay bridges ---------------------------------------------------------
    # Done first because whether the bridge survives decides whether its tunnel
    # and veth may be removed: they are what a still-busy bridge forwards over.
    busy_vnis = set()
    for name in sorted(links):
        match = BR_OV_RE.match(name)
        if not match:
            continue
        vni = int(match.group(1))
        if vni in vnis:
            continue
        if links[name].get("kind") != "bridge":
            refusals.append(ReclaimRefusal("bridge", name, "it is not a bridge device"))
            busy_vnis.add(vni)
            continue
        ports = inventory["bridge_ports"].get(name)
        if ports is None:
            refusals.append(ReclaimRefusal("bridge", name, "its enslaved interfaces could not be listed"))
            busy_vnis.add(vni)
            continue
        strangers = sorted(p for p in ports if p not in (f"vxlan-{vni}", f"veth-ov-{vni}"))
        if strangers:
            # Guest taps. Deleting the bridge would pull the NIC out from under a
            # running VM, which is far worse than leaving a bridge behind.
            refusals.append(ReclaimRefusal(
                "bridge", name,
                f"it still has {', '.join(strangers)} enslaved; live VM interfaces are attached, "
                f"detach them before removing this segment"))
            busy_vnis.add(vni)
            continue
        addrs = inventory["bridge_addrs"].get(name)
        if addrs is None:
            refusals.append(ReclaimRefusal("bridge", name, "its addresses could not be read"))
            busy_vnis.add(vni)
            continue
        if addrs:
            # Urbosa puts a segment's gateway inside the T1 namespace, never on
            # the host bridge. An address here was configured by something else -
            # Lanayru does exactly this for its Kubernetes segments - and the
            # host may be routing through it.
            refusals.append(ReclaimRefusal(
                "bridge", name,
                f"it holds host address {', '.join(addrs)}; Urbosa assigns segment gateways inside the "
                f"Tier-1 namespace, so this address belongs to another component"))
            busy_vnis.add(vni)
            continue
        actions.append(ReclaimAction(
            "bridge", name,
            f"no segment with VNI {vni} exists in hydra.urbosa_segments",
            [f"ip link set {name} down", f"ip link delete {name}"]))

    # -- segment veths -----------------------------------------------------------
    for name in sorted(links):
        match = VETH_OV_RE.match(name)
        if not match:
            continue
        vni = int(match.group(1))
        if links[name].get("kind") != "veth":
            continue
        if vni not in vnis:
            if vni in busy_vnis:
                refusals.append(ReclaimRefusal(
                    "veth", name, f"overlay bridge br-ov-{vni} is still in use"))
                continue
            actions.append(ReclaimAction(
                "veth", name,
                f"no segment with VNI {vni} exists in hydra.urbosa_segments",
                [f"ip link delete {name}"]))
            continue

        # The segment still exists. Two ways this veth can still be wrong:
        expected_ns = (desired.get("segment_ns") or {}).get(vni)
        if not expected_ns or expected_ns not in inventory["ns_names"]:
            continue
        if f"veth-t1-{vni}" in links:
            # Both ends in the root namespace: `ip link add` succeeded and
            # `ip link set netns` did not, so this segment has no gateway at all.
            actions.append(ReclaimAction(
                "veth", name,
                f"its namespace end veth-t1-{vni} is still in the root namespace, so the pair was never "
                f"moved into {expected_ns}; removing it lets the next pass rebuild it",
                [f"ip link delete {name}"]))
            continue
        peer_ns = inventory["nsid_names"].get(links[name].get("netnsid"))
        if peer_ns and peer_ns != expected_ns:
            # The segment was re-attached to a different Tier-1 router. The veth
            # is only ever created when the host side is missing, so it would
            # otherwise keep the segment wired to the old router forever.
            actions.append(ReclaimAction(
                "veth", name,
                f"it lands in {peer_ns} but VNI {vni} is now attached to {expected_ns}; "
                f"removing it lets the next pass rebuild it against the right router",
                [f"ip link delete {name}"]))

    # -- VXLAN tunnels -----------------------------------------------------------
    for name in sorted(links):
        match = VXLAN_RE.match(name)
        if not match:
            continue
        vni = int(match.group(1))
        if vni in vnis:
            continue
        if links[name].get("kind") != "vxlan":
            refusals.append(ReclaimRefusal("tunnel", name, "it is not a VXLAN device"))
            continue
        if vni in busy_vnis:
            refusals.append(ReclaimRefusal(
                "tunnel", name, f"overlay bridge br-ov-{vni} is still in use and forwards over it"))
            continue
        actions.append(ReclaimAction(
            "tunnel", name,
            f"no segment with VNI {vni} exists in hydra.urbosa_segments",
            [f"ip link set {name} down", f"ip link delete {name}"]))

    # -- half-built transit veths and macvlans left in the root namespace ---------
    swept_transit_pairs = set()
    for name in sorted(links):
        attrs = links[name]
        transit = TRANSIT_VETH_RE.match(name)
        if transit and attrs.get("kind") == "veth":
            # Both ends of a transit veth are moved into namespaces in the same
            # breath as they are created. One still sitting in the root namespace
            # is the wreckage of a pass that died in between.
            #
            # The two ends share the router prefix, and deleting either takes the
            # other with it. Proposing both produces one deletion and one "Cannot
            # find device" - a false alarm in the log, which is worth more care
            # than it costs. If they turn out not to be peers after all, the next
            # pass sees the survivor and removes it.
            prefix = transit.group(1)
            if prefix in swept_transit_pairs:
                continue
            swept_transit_pairs.add(prefix)
            actions.append(ReclaimAction(
                "veth", name,
                "a transit veth end is never left in the root namespace; this is a partially created link",
                [f"ip link delete {name}"]))
            continue
        macvlan = MACVLAN_T0_RE.match(name)
        if macvlan and attrs.get("kind") == "macvlan":
            prefix = macvlan.group(1)
            if is_leader_here and prefix in t0_prefixes:
                # The reconciler owns this case: it deletes and recreates the
                # macvlan when the namespace copy is missing.
                continue
            reason = ("this host does not hold the cluster VIP, and the Tier-0 uplink is active-passive"
                      if not is_leader_here else
                      f"no Tier-0 router with id prefix {prefix} exists in hydra.urbosa_t0_routers")
            actions.append(ReclaimAction("macvlan", name, reason, [f"ip link delete {name}"]))

    # -- namespaces --------------------------------------------------------------
    for ns_name, _nsid in sorted(inventory["netns"]):
        t0_match = NS_T0_RE.match(ns_name)
        t1_match = NS_T1_RE.match(ns_name)
        if not (t0_match or t1_match):
            continue  # not a name this daemon generates; not ours to remove
        if t0_match:
            prefix = t0_match.group(1)
            if is_leader_here:
                if prefix in t0_prefixes:
                    continue
                reason = f"no Tier-0 router with id prefix {prefix} exists in hydra.urbosa_t0_routers"
            else:
                reason = "this host does not hold the cluster VIP, and the Tier-0 edge is active-passive"
            kind = "tier-0 namespace"
        else:
            prefix = t1_match.group(1)
            if prefix in t1_prefixes:
                continue
            reason = f"no Tier-1 router with id prefix {prefix} exists in hydra.urbosa_t1_routers"
            kind = "tier-1 namespace"

        blocker = namespace_blocker(ns_name, desired, inventory)
        if blocker:
            refusals.append(ReclaimRefusal(kind, ns_name, blocker))
            continue
        # `ip netns del` unlinks the name; it does not stop what is running
        # inside. A dnsmasq left behind keeps the namespace and its interfaces
        # alive, invisibly, with no name left to reach them by.
        commands = [f"kill -9 {pid}" for pid, _cmd, _argv in inventory["ns_procs"].get(ns_name) or []]
        commands.append(f"ip netns del {ns_name}")
        actions.append(ReclaimAction(kind, ns_name, reason, commands))

    # -- DHCP servers for segments that no longer want one -----------------------
    # A namespace that survives keeps its processes, so a dnsmasq only stops when
    # something stops it. The reconciler stops the one belonging to a segment it
    # can still see; nothing stopped the one whose segment row was deleted, and it
    # went on answering DHCP on an interface that had been taken out from under it.
    for ns_name, allowed in sorted((desired.get("ns_dnsmasq") or {}).items()):
        processes = inventory["ns_procs"].get(ns_name)
        if processes is None:
            continue
        for pid, command, argv in processes:
            if "dnsmasq" not in command:
                continue
            iface = dnsmasq_interface(argv)
            if iface is None or iface in allowed:
                continue
            actions.append(ReclaimAction(
                "dhcp server", f"{ns_name}: pid {pid} on {iface}",
                f"nothing in the database asks for DHCP on {iface} in this router any more",
                [f"kill -9 {pid}"]))

    # -- stale flood entries -----------------------------------------------------
    peers = desired.get("peer_ips")
    if peers:
        for device in sorted(inventory["flood"]):
            entries = inventory["flood"].get(device)
            if entries is None:
                continue
            for dst in sorted(set(entries)):
                if dst in peers:
                    continue
                actions.append(ReclaimAction(
                    "flood entry", f"{device} -> {dst}",
                    f"{dst} is not a member of hydra.nodes; every broadcast on this segment is still "
                    f"being replicated to it",
                    [f"bridge fdb del {FLOOD_MAC} dev {device} dst {dst}"]))

    # -- stale Tier-0 return routes ----------------------------------------------
    for ns_name, wanted in sorted((desired.get("t0_routes") or {}).items()):
        if wanted is None:
            # The desired routes for this namespace could not be computed in full
            # (a transit slot was unavailable). Pruning against a partial answer
            # would delete live return paths.
            continue
        observed = inventory["ns_routes"].get(ns_name)
        if observed is None:
            continue
        for dst, gateway in sorted(set(observed)):
            # Only routes pointing into the transit range are ours. The uplink
            # default route and anything an operator added are not.
            if not gateway.startswith("100.64."):
                continue
            if (dst, gateway) in wanted:
                continue
            actions.append(ReclaimAction(
                "return route", f"{ns_name}: {dst} via {gateway}",
                "no segment behind that transit link uses this prefix any more",
                [f"ip netns exec {ns_name} ip route del {dst} via {gateway}"]))

    # -- orphaned transit reservations -------------------------------------------
    pool = desired.get("transit_pool")
    router_ids = desired.get("t1_router_ids")
    if is_leader_here and pool is not None and router_ids is not None:
        for index, owner in sorted(pool.items()):
            if owner in router_ids:
                continue
            _t0_ip, _t1_ip, network = transit_addresses(index)
            actions.append(ReclaimAction(
                "transit reservation", f"slot {index} ({network})",
                f"Tier-1 router {owner} no longer exists in hydra.urbosa_t1_routers",
                queries=[("release-transit", index, owner)]))

    return actions, refusals

# Refusals already reported, so a bridge with a VM on it is explained once rather
# than every pass. Keyed by target so a *changed* reason is reported again.
reclaim_refusals_logged = {}

def execute_plan(actions, refusals, dry_run=False, run=None):
    """Carries out a plan, or describes it. Returns the actions it performed.

    Dry run is the default everywhere it is offered, and it is the mode to reach
    for first: a report of what would be removed is worth more than an eager
    collector, because the report can be checked against what the cluster is
    actually doing.
    """
    runner = run or run_cmd
    performed = []

    for refusal in refusals:
        if reclaim_refusals_logged.get(refusal.target) != refusal.reason:
            print(f"Urbosa reclaim: REFUSING to remove {refusal.kind} {refusal.target}: {refusal.reason}.")
            reclaim_refusals_logged[refusal.target] = refusal.reason
    live_refusals = {r.target for r in refusals}
    for target in [t for t in reclaim_refusals_logged if t not in live_refusals]:
        del reclaim_refusals_logged[target]

    for action in actions:
        if dry_run:
            print(f"Urbosa reclaim (dry run): would remove {action.kind} {action.target} - {action.reason}.")
            for command in action.commands:
                print(f"    {command}")
            for query in action.queries:
                print(f"    {describe_query(query)}")
            continue

        print(f"Urbosa reclaim: removing {action.kind} {action.target} - {action.reason}.")
        failed = False
        for command in action.commands:
            rc, _stdout, stderr = runner(command)
            if rc != 0:
                # Reported, not retried. The next pass re-observes and either
                # proposes the same action or finds the resource already gone.
                print(f"Urbosa reclaim: '{command}' failed: {stderr.strip() or f'exit {rc}'}")
                failed = True
                break
        if not failed:
            for query in action.queries:
                if not run_reclaim_query(query):
                    failed = True
                    break
        if not failed:
            performed.append(action)
    return performed

def describe_query(query):
    """Human-readable form of a database action, for the dry run."""
    if query and query[0] == "release-transit":
        return f"DELETE FROM {TRANSIT_TABLE} WHERE subnet_index = {query[1]} IF router_id = {query[2]};"
    return str(query)

def run_reclaim_query(query):
    """Executes a database action from a plan. Returns True if it took effect."""
    if query and query[0] == "release-transit":
        return release_transit_index(query[1], query[2])
    return False

def build_desired_state(t0_routers, t1_routers, segments, node_ips, leader,
                        transit_pool=None, t0_routes=None):
    """The shape plan_reclamation() compares the host against.

    Router ids are reduced to the same eight-character prefix the namespace names
    use, because that is all a namespace name carries. Two routers whose UUIDs
    share a prefix would already share a namespace, so comparing prefixes does
    not lose anything the naming scheme has not already lost.
    """
    vnis = set()
    segment_ns = {}
    # Which interfaces should have a dnsmasq bound to them, per namespace. An
    # empty set is meaningful - it means "this router exists and wants no DHCP
    # server at all" - so only namespaces present here are ever considered.
    ns_dnsmasq = {}
    for router in t1_routers:
        router_id = router.get("router_id")
        if not router_id:
            continue
        allowed = set()
        if router.get("dhcp_enabled"):
            allowed.add("lo")
        ns_dnsmasq[f"ns-t1-{str(router_id)[:8]}"] = allowed

    for segment in segments:
        try:
            vni = int(segment.get("vni"))
        except (TypeError, ValueError):
            continue
        vnis.add(vni)
        t1_link = segment.get("t1_link_id")
        if t1_link:
            ns_name = f"ns-t1-{str(t1_link)[:8]}"
            segment_ns[vni] = ns_name
            if (segment.get("dhcp_enabled") and segment.get("dhcp_start")
                    and segment.get("dhcp_end") and ns_name in ns_dnsmasq):
                ns_dnsmasq[ns_name].add(f"veth-t1-{vni}")
    return {
        "t0_prefixes": {str(r.get("router_id"))[:8] for r in t0_routers if r.get("router_id")},
        "t1_prefixes": {str(r.get("router_id"))[:8] for r in t1_routers if r.get("router_id")},
        "t1_router_ids": {str(r.get("router_id")) for r in t1_routers if r.get("router_id")},
        "vnis": vnis,
        "segment_ns": segment_ns,
        "ns_dnsmasq": ns_dnsmasq,
        "peer_ips": set(node_ips) if node_ips else None,
        "is_leader": leader,
        "transit_pool": transit_pool,
        "t0_routes": t0_routes or {},
    }

def get_reclaim_settings():
    """(enabled, dry_run) for the collector, from hydra.cluster_settings.

    Reclamation is on unless it is switched off, because the alternative is the
    leak. `urbosa_gc_dry_run` is the setting to reach for when a cluster is doing
    something unusual: the daemon keeps reporting what it would remove without
    removing anything.
    """
    enabled, dry_run = True, False
    rc, stdout, _ = run_cql_query(
        "SELECT JSON key, value FROM hydra.cluster_settings WHERE key IN "
        "('urbosa_gc_enabled', 'urbosa_gc_dry_run');")
    if rc != 0:
        return enabled, dry_run
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not (line.startswith("{") and line.endswith("}")):
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        value = str(row.get("value", "")).strip().lower() == "true"
        if row.get("key") == "urbosa_gc_enabled":
            enabled = value
        elif row.get("key") == "urbosa_gc_dry_run":
            dry_run = value
    return enabled, dry_run

def reclaim_once(dry_run=True):
    """One collection pass driven from the database. Returns (actions, refusals).

    Used by the daemon loop and by `urbosa --reclaim` on the command line. The
    desired state must read cleanly in full: anything else and this returns
    nothing to do, because "the query failed" and "the operator deleted
    everything" are the same sentence to a collector that cannot tell them apart.
    """
    t0_routers = get_db_routers_t0()
    t1_routers = get_db_routers_t1()
    segments = get_db_segments()
    if t0_routers is None or t1_routers is None or segments is None:
        print("Urbosa reclaim: the desired state could not be read in full; nothing will be removed.")
        return [], []

    node_ips = get_db_node_ips()
    pool = read_transit_pool()
    leader = is_leader()

    # Return-route pruning needs every transit address that should exist, so it
    # is only attempted for Tier-0 namespaces whose whole set could be computed.
    t0_routes = plan_t0_routes(t0_routers, t1_routers, segments, pool)

    desired = build_desired_state(t0_routers, t1_routers, segments, node_ips, leader,
                                  transit_pool=pool, t0_routes=t0_routes)
    inventory = collect_inventory(desired)
    actions, refusals = plan_reclamation(desired, inventory)
    execute_plan(actions, refusals, dry_run=dry_run)
    return actions, refusals

def plan_t0_routes(t0_routers, t1_routers, segments, pool):
    """{t0_namespace: {(cidr, next_hop)} or None} - the return paths that should exist.

    None for a namespace whose answer is incomplete: a Tier-1 router with no
    recorded transit slot means its return routes cannot be named, and pruning
    against a partial set would delete a live path back to a tenant.
    """
    wanted = {}
    for router in t0_routers:
        router_id = router.get("router_id")
        if router_id:
            wanted[f"ns-t0-{str(router_id)[:8]}"] = set()

    for t1 in t1_routers:
        t0_link = t1.get("t0_link_id")
        t1_id = t1.get("router_id")
        if not t0_link or not t1_id:
            continue
        ns_name = f"ns-t0-{str(t0_link)[:8]}"
        if ns_name not in wanted:
            continue
        index = None
        if pool is not None:
            for slot, owner in pool.items():
                if owner == str(t1_id):
                    index = slot
                    break
        if index is None:
            wanted[ns_name] = None  # incomplete; this namespace is not pruned
            continue
        if wanted[ns_name] is None:
            continue
        _t0_ip, t1_ip, _network = transit_addresses(index)
        next_hop = t1_ip.split("/")[0]
        for segment in segments:
            if str(segment.get("t1_link_id")) != str(t1_id):
                continue
            subnet = segment.get("subnet_cidr")
            if subnet:
                wanted[ns_name].add((str(subnet), next_hop))
    return wanted

def main():
    print("Urbosa SDN logical router and overlay orchestrator started.")
    
    while True:
        try:
            if not is_urbosa_enabled():
                time.sleep(5)
                continue
            
            # Fetch resources. None means the read could not be trusted, and the
            # whole pass is skipped: reconciling against half a topology rebuilds
            # things that were deleted, and reclaiming against it deletes things
            # that were not.
            t0_routers = get_db_routers_t0()
            t1_routers = get_db_routers_t1()
            segments = get_db_segments()
            if t0_routers is None or t1_routers is None or segments is None:
                time.sleep(LOOP_SECONDS)
                continue
            firewall_rules = get_db_firewall_rules()

            leader_status = is_leader()
            local_ip = get_local_ip()
            # Read once per pass, not once per segment: the old placement inside
            # the segment loop issued one query per segment per 15 seconds.
            node_ips = get_db_node_ips()

            # Transit /30s are allocated from a recorded pool rather than derived
            # from a hash, so two routers can never be handed the same one.
            transit_pool = read_transit_pool()
            transit_slots = {}
            for r in t1_routers:
                router_id = r.get("router_id")
                if not router_id or not r.get("t0_link_id"):
                    continue
                slot = claim_transit_index(router_id, local_ip, pool=transit_pool)
                if slot is not None:
                    transit_slots[str(router_id)] = slot
                    if transit_pool is not None:
                        transit_pool[slot] = str(router_id)

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

                        # Transit addressing comes from the recorded pool. No slot
                        # means the pool could not be read: leave the link exactly
                        # as it is rather than fall back to the derived index,
                        # which is what used to hand two routers the same /30.
                        # Checked before the veth is built, so a pool outage does
                        # not leave an unaddressed pair behind either.
                        slot = transit_slots.get(str(r['router_id']))
                        if slot is None:
                            continue

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

                        t0_ip, t1_ip, _network = transit_addresses(slot)
                        t0_addr = t0_ip.split('/')[0]
                        t1_addr = t1_ip.split('/')[0]

                        # Assign transit IP to T1 interface
                        if t1_addr not in get_iface_addresses(veth_t1, ns_name):
                            run_cmd(f"ip netns exec {ns_name} ip addr add {t1_ip} dev {veth_t1}")

                        # Assign transit IP to T0 interface
                        if t0_addr not in get_iface_addresses(veth_t0, t0_ns):
                            run_cmd(f"ip netns exec {t0_ns} ip addr add {t0_ip} dev {veth_t0}")

                        # Configure default gateway route in T1 namespace pointing to T0
                        _, t1_routes, _ = run_cmd(f"ip netns exec {ns_name} ip route show")
                        if f"default via {t0_addr} " not in t1_routes + " ":
                            run_cmd(f"ip netns exec {ns_name} ip route del default 2>/dev/null || true")
                            run_cmd(f"ip netns exec {ns_name} ip route add default via {t0_addr} dev {veth_t1}")

                        # Add route back to guest subnets inside T0 namespace.
                        # Matched exactly rather than by substring: 10.0.1.0/24 is
                        # a substring of 110.0.1.0/24, and reading a neighbour's
                        # route as this one's leaves the tenant unreachable.
                        t0_routes_now = list_netns_routes(t0_ns)
                        for s in segments:
                            if s.get("t1_link_id") == r.get("router_id"):
                                subnet = s.get("subnet_cidr")
                                if subnet and (t0_routes_now is None or (subnet, t1_addr) not in t0_routes_now):
                                    run_cmd(f"ip netns exec {t0_ns} ip route replace {subnet} via {t1_addr} dev {veth_t0}")

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
                
                # Reconcile FDB flooding entries for VXLAN mesh. `bridge fdb
                # append` is idempotent for a given (mac, dst) pair - verified on
                # the live cluster - so repeating this every pass adds nothing.
                # Entries for peers that have LEFT hydra.nodes are removed by the
                # reclaimer, not here.
                for ip in node_ips or []:
                    if ip and ip != local_ip:
                        run_cmd(f"bridge fdb append {FLOOD_MAC} dev {vx_name} dst {ip} 2>/dev/null || true")

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

            # 5. Reclaim resources whose database rows are gone. Runs last, on a
            # desired state that has already been proven readable, and after
            # reconciliation has had its chance to recreate anything it wants.
            gc_enabled, gc_dry_run = get_reclaim_settings()
            if gc_enabled:
                desired = build_desired_state(
                    t0_routers, t1_routers, segments, node_ips, leader_status,
                    transit_pool=transit_pool,
                    t0_routes=plan_t0_routes(t0_routers, t1_routers, segments, transit_pool))
                inventory = collect_inventory(desired)
                actions, refusals = plan_reclamation(desired, inventory)
                execute_plan(actions, refusals, dry_run=gc_dry_run)

        except Exception as e:
            sys.stderr.write(f"Error in Urbosa control loop: {e}\n")

        time.sleep(LOOP_SECONDS)

USAGE = """Urbosa - SDN logical router and overlay orchestrator.

  urbosa                   run the reconciliation daemon (this is what urbosa.service does)
  urbosa --reclaim         report what the reclaimer would remove on this host, and remove nothing
  urbosa --reclaim --apply remove it

--reclaim reads the same desired state the daemon does and refuses to remove
anything it cannot prove is idle; the refusals it prints are as much the point as
the removals. It is safe to run while the daemon is running.
"""

def main_cli(argv):
    """Command-line entry point. Returns a process exit status."""
    args = list(argv)
    if "--help" in args or "-h" in args:
        print(USAGE)
        return 0
    if "--reclaim" not in args:
        main()
        return 0

    apply_changes = "--apply" in args
    if not is_urbosa_enabled():
        print("Note: urbosa_enabled is not set in hydra.cluster_settings. Reporting anyway.")
    actions, refusals = reclaim_once(dry_run=not apply_changes)
    if not actions and not refusals:
        print("Urbosa reclaim: nothing to reclaim on this host.")
    elif not apply_changes and actions:
        print(f"Urbosa reclaim: {len(actions)} resource(s) would be removed. "
              f"Re-run with --apply to remove them.")
    return 0

if __name__ == "__main__":
    sys.exit(main_cli(sys.argv[1:]))
