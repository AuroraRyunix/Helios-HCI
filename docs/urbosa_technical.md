# Urbosa (SDN Controller) - Technical Documentation

This document details the internal technical structure, functions, flowcharts, and mindmaps of the Urbosa Software-Defined Networking controller (`urbosa.py`).

## Technical Mindmap

```mermaid
mindmap
  root((Urbosa SDN))
    Consensus & IP
      get_local_ip via UDP socket connect
      is_leader via checking VIP bound locally
      get_uplink_interface (default route resolution)
    Tier-0 Edge Gateways
      Active-Passive namespace ns-t0-ID
      Runs strictly on VIP leader node
      MacVlan uplink connection mode bridge
      Source NAT (Masquerade) & IPv4 forwarding
    Tier-1 Distributed Routers
      ns-t1-ID namespaces running on all nodes
      Links T1 to T0 Edge via Veth transit subnet
      Configures default route in T1 pointing to T0
      Transit /30 claimed from hydra.urbosa_transit_pool
      Integrated dnsmasq DHCP servers
    Overlay Segments
      VXLAN tunnel mesh interfaces vxlan-VNI dstport 4789
      Overlay bridges br-ov-VNI
      Append nodes to bridge FDB (flood table entries)
      Dynamic MTU configuration
      Veth connections connecting Segment Bridge to T1 namespace
    Distributed Firewall
      Micro-segmentation iptables rules in URBOSA-FWD chain
    Reclamation
      Observe host, compare to desired state, refuse anything not provably idle
      Dry run by default on the command line
```

## Function & Logic Breakdown

### Uplink and Leader Resolution
- **`is_leader()`**:
  1. Resolves floating VIP from `/etc/hci/cluster.json`. If no VIP is configured, returns True on Node 1 (as local fallback), False elsewhere.
  2. Runs `ip addr show`. If the floating VIP is bound to any local interface, returns True (active coordinator). Otherwise, returns False (standby).
- **`get_uplink_interface(preferred_if)`**:
  - Dynamically queries route endpoints (`ip route get 8.8.8.8` or parses `ip route | grep default`) to resolve the host gateway adapter interface.

### Database Query
- **`run_cql_query(cql_query)`**: Submits requests via Daruk proxy on `http://127.0.0.1:9043/query` or container cqlsh.
- **`read_json_table(table, columns, consequence)`**: Reads a table into a list of dicts, or returns **`None`** if the read cannot be trusted — a failed query, or a row that looks like JSON and does not parse. Every desired-state read goes through it, and the distinction is load-bearing: the reclaimer deletes host resources for being absent from the result, so a failed query arriving as `[]` reads as "every router and segment was deleted at once". A partial set is treated as no set, because the rows that vanished are indistinguishable from rows an operator deleted.
- **`lwt_was_applied(stdout)`**: Whether a conditional write took effect. A rejected lightweight transaction is *not* an error — it returns rc=0 with `[applied]=False` and the winning row beside it — so the return code alone reports a lost race as a success. `run_cql_query` flattens rows to space-joined values, so only the first field can be read: through Daruk a refusal arrives as `False 777 1 10.10.102.41 aaaa`, and through the cqlsh fallback as a rendered table. Both shapes are handled.

### Transit /30 Allocation
Slot `N` maps to `100.64.(N>>6).((N&63)*4)/30`, T0 on `.1` and T1 on `.2` — the same arithmetic the old derived scheme used, so a slot number means the same addresses before and after the change.

- **`preferred_transit_index(router_id)`**: `md5(router_id)[:4] % 16384`. Kept only as a *preference*, so an upgraded cluster keeps the transit addressing it already has wherever that slot is free.
- **`read_transit_pool()`**: `{slot: router_id}`, or `None` if unreadable.
- **`claim_transit_index(router_id, node_id, pool)`**: Returns the slot this router already holds, or claims one with `INSERT ... IF NOT EXISTS` keyed on the slot. A refused claim means another node won it; the pool is re-read and another slot tried, up to `TRANSIT_CLAIM_ATTEMPTS`. Returns `None` when no allocation could be made, which the caller must read as "leave this transit link alone" — falling back to the derived index on a database failure would reintroduce the collision at the worst moment.
- **`release_transit_index(index, router_id)`**: `DELETE ... IF router_id = ...`. Conditional because the reclaimer runs on every node: an unconditional delete lets a stale view free a slot that has since been reallocated.

`hydra.urbosa_transit_pool` uses `router_id text` and `allocated_at_ms bigint`, not `uuid` and `timestamp`. Verified against the live cluster: a refused LWT returns the whole existing row, and Daruk's `make_serializable` passes driver UUID and datetime objects through untouched, so `json.dumps` raises and the refusal comes back as `Object of type UUID is not JSON serializable`. The one response that names the winner would be the one response no caller could read. `hydra.cluster_locks` carries the same scar.

### Reclamation
Split into three pieces so the decision is testable without a kernel:

- **`collect_inventory(desired, run, argv_reader)`** — observation only. `list_netns()`, `list_links()` (with `ip -d` so the device *kind* can be required to match, not just the name), `list_bridge_ports()`, `read_iface_addresses(scope="global")`, `list_netns_links()`, `list_netns_processes()`, `list_netns_routes()`, `list_flood_entries()`. Anything that cannot be read returns `None`, and `None` is treated as "busy" everywhere it matters.
- **`plan_reclamation(desired, inventory)`** — a pure function of two dictionaries returning `(actions, refusals)`. Runs no commands and reads nothing.
- **`execute_plan(actions, refusals, dry_run, run)`** — carries out the plan or describes it. Refusals are logged on change, not every pass. A failed command aborts its own action and nothing else.

`namespace_blocker(ns, desired, inventory)` is the gate that decides whether a namespace may be deleted at all: every interface inside must match a name Urbosa generates (`lo`, `t0-`/`t1-<8 hex>`, `veth-t1-<vni>`, `mv-t0-<8 hex>`), every process must be a `dnsmasq`, and no `veth-t1-<vni>` may belong to a segment that still exists — deleting the namespace would strip a live segment of its default gateway, which is the operator's decision to make and not the collector's. `ip netns del` unlinks the name and stops nothing, so the plan kills the processes first.

Two details that only showed up on real hardware:
- Every device that is up carries a kernel-assigned `fe80::/64` address. The first version of the "bridge holds a host address" guard therefore refused every bridge on the host. Only globally scoped addresses count.
- Deleting either end of a veth takes its peer with it, so proposing both ends of an abandoned transit pair produced one deletion and one `Cannot find device`. One action is emitted per pair.

### main() Coordination Loop
Runs every 15 seconds:

#### 1. Tier-0 Gateway Namespace (Active-Passive Edge)
- Executes namespace configurations **only on the VIP leader**:
  - Creates netns named `ns-t0-<router_hash>`.
  - Creates a `macvlan` interface (`mv-t0-<router_hash>`) bound to the default physical uplink interface.
  - Places the macvlan interface inside the Tier-0 namespace.
  - Assigns the public external IP address.
  - Installs a default gateway route and turns on `net.ipv4.ip_forward`.
  - Enables Source NAT: `iptables -t nat -A POSTROUTING -j MASQUERADE`.
- On follower (passive) nodes, tears down the namespace and associated macvlan interfaces to prevent IP conflicts.

#### 2. Tier-1 Gateway Namespace (Distributed Routers)
- Runs on **all hosts**:
  - Ensures local T1 namespace exists: `ns-t1-<router_hash>`.
  - Enables IPv4 forwarding inside the namespace.
  - Links T1 to T0 namespace (if active locally) via veth pairs (`t1-<hash>` and `t0-<hash>`).
  - Takes the transit IPs (`100.64.X.1/30` and `100.64.X.2/30`) from the slot claimed in `hydra.urbosa_transit_pool`. No slot means the pool could not be read, and the link is left untouched rather than addressed from the derived index.
  - Configures default route in T1 netns pointing to the transit IP on T0. The existing route is matched on the whole next-hop token: `default via 100.64.0.1` is a prefix of `default via 100.64.0.13`, and both are addresses this scheme hands out.
  - Adds return guest subnet routes inside T0 netns with `ip route replace`, matched on the exact `(prefix, next hop)` pair rather than by substring — `10.0.1.0/24` is a substring of `110.0.1.0/24`. `replace` also re-points a return route whose transit slot has changed, instead of failing with `File exists` and leaving the old next hop in place.

#### 3. Overlay Segment Reconciliation (VXLAN Tunnel Mesh)
- Runs on **all hosts**:
  - Creates bridge `br-ov-<vni>`.
  - Configures VXLAN interface `vxlan-<vni>` with destination port `4789` and binds it to the bridge.
  - Pulls all node IPs from `hydra.nodes` and appends flooding entries to bridge FDB:
    `bridge fdb append 00:00:00:00:00:00 dev vxlan-<vni> dst <peer_ip>`
  - Links segment bridges to respective Tier-1 namespaces via host-to-netns veth pairs (`veth-ov-<vni>` and `veth-t1-<vni>`).
  - Assigns segment gateway IP to the namespace interface.
  - Configures local `dnsmasq` instances inside the T1 namespace to serve DHCP ranges.

#### 3b. VXLAN Flood List
- Appends one flood entry per peer: `bridge fdb append 00:00:00:00:00:00 dev vxlan-<vni> dst <peer>`. Verified idempotent for a given (MAC, destination) pair on the live cluster, so repeating it every pass adds nothing.
- The peer list is read **once per pass**, not once per segment — the query used to sit inside the segment loop and issued one round trip per segment every 15 seconds.
- Withdrawal of entries for departed peers belongs to the reclaimer, not here.

#### 4. Distributed Firewall
- Reads rules from `hydra.urbosa_firewall_rules`.
- Generates corresponding `iptables` statements and rebuilds the dedicated `URBOSA-FWD` chain from them in one `iptables-restore` transaction, reached by a single jump from `FORWARD` (enforces `ALLOW` / `ACCEPT` or `DENY` / `DROP` policies by source, destination, protocol, and port).

#### 5. Reclamation
- Skipped entirely when `urbosa_gc_enabled` is `false`; reports without acting when `urbosa_gc_dry_run` is `true`.
- Builds the desired state, observes the host, plans, and executes. See **Reclamation** above and §1.E of the [user guide](./urbosa.md) for the table of what is reclaimed and what is refused.

### Command Line
- `urbosa` — run the daemon (what `urbosa.service` executes).
- `urbosa --reclaim` — report what would be reclaimed on this host. Removes nothing.
- `urbosa --reclaim --apply` — remove it.

Everything lives in the single `urbosa.py` file that ships as `/usr/local/bin/urbosa`, so the command-line mode needs no deployment change of its own.
