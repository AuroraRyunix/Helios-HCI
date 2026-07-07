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
      Transit subnet: 100.64.Y.Z
      Integrated dnsmasq DHCP servers
    Overlay Segments
      VXLAN tunnel mesh interfaces vxlan-VNI dstport 4789
      Overlay bridges br-ov-VNI
      Append nodes to bridge FDB (flood table entries)
      Dynamic MTU configuration
      Veth connections connecting Segment Bridge to T1 namespace
    Distributed Firewall
      Micro-segmentation iptables rules in FORWARD chain
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
  - Generates transit IPs (e.g. `100.64.X.1/30` and `100.64.X.2/30`) using a hash of the T1 router UUID to avoid collisions.
  - Configures default route in T1 netns pointing to the transit IP on T0.
  - Adds return guest subnet routes inside T0 netns.

#### 3. Overlay Segment Reconciliation (VXLAN Tunnel Mesh)
- Runs on **all hosts**:
  - Creates bridge `br-ov-<vni>`.
  - Configures VXLAN interface `vxlan-<vni>` with destination port `4789` and binds it to the bridge.
  - Pulls all node IPs from `hydra.nodes` and appends flooding entries to bridge FDB:
    `bridge fdb append 00:00:00:00:00:00 dev vxlan-<vni> dst <peer_ip>`
  - Links segment bridges to respective Tier-1 namespaces via host-to-netns veth pairs (`veth-ov-<vni>` and `veth-t1-<vni>`).
  - Assigns segment gateway IP to the namespace interface.
  - Configures local `dnsmasq` instances inside the T1 namespace to serve DHCP ranges.

#### 4. Distributed Firewall
- Reads rules from `hydra.urbosa_firewall_rules`.
- Generates corresponding `iptables` statements and appends them to the host's `FORWARD` chain (enforces `ALLOW` / `ACCEPT` or `DENY` / `DROP` policies by source, destination, protocol, and port).
