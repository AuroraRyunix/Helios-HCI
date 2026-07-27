# Architecture Overview

This is a mid-length system-design overview: request path, control-plane vs. data-plane split, and network architecture. For full depth (every daemon's internals, every edge case), see [docs/hci_master_architecture_guide.md](./hci_master_architecture_guide.md) (1500+ lines) and the per-daemon `docs/<name>.md` / `docs/<name>_technical.md` pairs. For the AI-agent-oriented "where does everything live and how do I change it" reference, see [docs/AGENTS.md](./AGENTS.md).

---

## 1. Request path

```
Client (browser)
   │  HTTPS, port 443
   ▼
Slate (Traefik reverse proxy)
   │
   ├──── /api/*, /  ────────────► Spectrum (spectrum_server.py) — WebUI + REST API
   │                                   │
   │                                   ▼
   │                          Catalyst task queue (catalyst.py, :9091)
   │                                   │
   │                     ┌─────────────┼──────────────┐
   │                     ▼             ▼              ▼
   │                   Vali          Dagur      Mipha / Bifrost / Gatoway / Urbosa
   │              (:9095, DRS/       (cron)      (HA, VIP, L2/L3 networking)
   │               VM scheduling)
   │                     │
   │                     ▼
   │           spark-daemon (mTLS, :9099) on whichever host owns the operation
   │                     │
   │                     ▼
   │          libvirt/KVM, Linstor/DRBD, ScyllaDB (via the Daruk CQL proxy)
   │
   └──── /console/* (VNC/SPICE) ────► Agahnim (Rust WebSocket console proxy, :8081)
                                            │
                                            ▼
                                   Guest VM's VNC/SPICE socket (via libvirt)
```

A typical write path (e.g. "power on a VM" from the WebUI) looks like: browser → Slate → Spectrum's REST API validates the request and submits a task to Catalyst → Catalyst enqueues it for the `vali` service queue and persists it in ScyllaDB → Vali's scheduler picks a target host and calls that host's `spark-daemon` over mTLS to run the actual `virsh`/libvirt command → Spectrum polls Catalyst (or the browser long-polls) for task completion status.

A typical read path (e.g. loading the VM list) goes straight from Spectrum to ScyllaDB via the Daruk proxy (`http://127.0.0.1:9043/query`, a lightweight local HTTP-to-CQL bridge that avoids the overhead of a fresh Cassandra-driver connection per request), without touching Catalyst at all.

## 2. Control-plane vs. data-plane split

* **Control plane** — the daemons that decide *what should happen* and *where*: Spectrum (API/UI), Catalyst (task queue/orchestration), Vali (VM placement/DRS), Mipha (HA/failover decisions), Bifrost (VIP ownership), Hylia (rolling-upgrade sequencing), Dagur (scheduled jobs), Mimir (health checks), Logos (telemetry collection). These are mostly stateless Python processes that read/write their shared state to ScyllaDB and ZooKeeper rather than holding authoritative state themselves — any one of them can restart without losing cluster state.
* **Data plane** — the systems that actually hold or move data/traffic: ScyllaDB (`hydra-db`, cluster/VM/task metadata — the `hydra` keyspace), ZooKeeper (`zookeeper`, leader election + small coordination znodes like `/cluster_state`), Linstor/DRBD (`aether`, replicated VM disk block storage), libvirt/KVM (actual VM execution, driven locally by `spark-daemon`), and Slate/Agahnim (client-facing HTTP and console traffic).
* Gatoway (L2 VLAN sync) and Urbosa (L3 SDN overlay) sit at the boundary — they are control-plane daemons whose job is to configure the data-plane networking (bridges, VXLANs) that guest VM traffic actually flows over.

There is deliberately no separate "Controller VM" tier the way Nutanix AHV uses CVMs: every one of the above daemons runs directly on the hypervisor host (as a Quadlet container or native `systemd` unit — see [docs/deployment.md](./deployment.md)), which is the core value proposition of the project (see the top-level [README.md](../README.md) §1).

## 3. Network architecture

The cluster's network traffic falls into three tiers, verified against the code (see the top-level [README.md](../README.md) §7 for the full port table and Mermaid flowchart):

1. **Client-facing ingress**: only Slate (port `443`) is meant to be exposed to end users; it reverse-proxies to Spectrum and Agahnim same-origin.
2. **mTLS orchestration mesh**: every cross-node administrative action (remote command execution, VM start/stop/migrate, node-to-node coordination) goes over `spark-daemon`'s mutual-TLS API on port `9099`. This is the only channel that crosses the trust boundary between hosts for arbitrary command execution.
3. **Cluster-internal consensus/data mesh**: ScyllaDB gossip/replication (`7000`) and client queries (`9042`), and ZooKeeper's leader election/sync ports (`2888`/`3888`) and client port (`2181`), plus DRBD's replication traffic and Linstor's controller/satellite ports (`3366`/`3370`).

One current gap worth calling out here (tracked in [TODO.md](../TODO.md)): Catalyst (`:9091`) and Vali (`:9095`) are Quadlet containers running with `Network=host`, and both bind their HTTP servers to `0.0.0.0` with no caller authentication — so in practice they are reachable from anywhere on the cluster network, not confined to the mTLS mesh or to loopback the way the rest of the control plane is. This is a known, not-yet-hardened seam in the network model, not an intentional second ingress.

## 4. Where storage and networking config actually live

* `/etc/hci/cluster.json` — the single source of truth for host list, VIP, and redundancy factor that most daemons fall back to reading when ZooKeeper/ScyllaDB state is unavailable.
* `/var/lib/hci/aether/volumes/` — where local VM disk raw files live once a Linstor/DRBD volume is mounted.
* `/etc/hci/spark/certs/` and `/root/.certs/` — the mTLS certificate material securing the port-`9099` mesh.

See the top-level [README.md](../README.md) §6 for the complete directory layout.
