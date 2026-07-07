# Nayru Technical Guide & System Mindmap

This document provides a detailed technical reference and architectural mindmap for the **Nayru** guest Kubernetes orchestration engine.

---

## 1. Nayru System Mindmap

```mermaid
mindmap
  root((Nayru Guest K8s))
    Architecture
      Kine Integration
        API Mocking
          etcd v3 API translation
          Listens on port 2379
        Host Persistence
          CQL over TLS
          Queries host ScyllaDB
      Database Schema
        nayru_clusters
          Metadata table
        nayru_k8s_state
          etcd key-value translation
    Deployment Topologies
      Single Control Node
        1 VM Control Plane
        Non-HA testing
      Quorum HA Topology
        3 VM Control Plane
        Vali Anti-Affinity rules
        Distributed across hosts
    Network Bridging
      NAT Bypass Challenge
        Private segment IPs
        No direct routing from host
      Veth Bridge Link
        veth-host assigned to host L2
        veth-overlay enslaved in br-ov-VNI
        Direct un-NAT'ed L2 path
    Deployment Pre-Checks
      ScyllaDB Status
        nodetool status verification
      Storage Space
        vg_aether check 50GB thin
      Compute Capacity
        RAM usage free -m 4GB check
      SDN Status
        Active overlay segment
```

---

## 2. Component Specifications

| Technical Metric | Value / Implementation |
| :--- | :--- |
| **Kine Interface Port** | Port `2379` (etcd compatibility proxy) |
| **Database Target** | Host ScyllaDB (`hydra`) keyspace |
| **HA VM Requirements** | 3 x VMs (2 vCPUs, 4 GB RAM, 50 GB storage each) |
| **Anti-Affinity Rule** | Hard VM-to-Host distribution (Vali-enforced) |
| **Host Bridge Address** | `10.244.0.254/24` (or subnets matching guest overlay) |

---

## 3. Operations & Lifecycle

```
[ WebUI/API (Spectrum) ]
         │
         │ (Pre-Checks: ScyllaDB, Storage, RAM)
         ▼
[ Provision Guest VMs ] ───(Vali Anti-Affinity)───► Scheduled on Host 1, 2, 3
         │
         ▼
[ Bootstrap Kine Daemon ] ──(mTLS Connection)───► Host ScyllaDB (nayru_k8s_state)
         │
         ▼
[ Create Veth Bridge ] ────(IP Link Commands)───► Enslave in segment bridge (br-ov)
```
