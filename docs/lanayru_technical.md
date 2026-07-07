# Lanayru Technical Guide & System Mindmap

This document provides a detailed technical reference and architectural mindmap for the refactored **Lanayru** guest Kubernetes workload engine agent ([lanayru.py](file:///C:/Users/AuraFlight/Desktop/container-hci/lanayru.py)).

---

## 1. Lanayru System Mindmap

```mermaid
mindmap
  root((Lanayru K8s Agent))
    Execution Module
      deploy_lanayru_worker
        Create DB Schemas
          lanayru_clusters
          lanayru_k8s_state
        Overlay Segment Creation
          Segment 1 (172.16.10.0/24)
          Segment 2 (172.16.11.0/24)
        Host Bridges Setup
          br-ov-10010
          br-ov-10011
        Provision Linstor Storage
          resource-definition create
          volume-definition 5GiB
        Libvirt VM Creation
          Generate KVM XML
          Start VM (virsh start)
        Urbosa DHCP Refresh
          Submit Catalyst job
      destroy_lanayru_worker
        VM Destruction
          virsh destroy / undefine
        Linstor Cleanup
          resource-definition delete
        Overlay Segment Cleanup
          DELETE from urbosa_segments
        Metadata Cleanup
          DELETE from lanayru_clusters
          DELETE from lanayru_k8s_state
    External Helper Imports
      run_cql_query
      run_remote_spark
      run_linstor_cmd
      log_catalyst_task
      get_cluster_nodes
      LOCAL_IP
      LANAYRU_LOGS
```

---

## 2. Component Specifications

| Technical Metric | Value / Implementation |
| :--- | :--- |
| **Script Path** | [lanayru.py](file:///C:/Users/AuraFlight/Desktop/container-hci/lanayru.py) |
| **Port Mapping** | VNC console ports: `5910` + i |
| **Orchestration** | Triggered asynchronously via threads in `spectrum_server.py` |

---

## 3. Operations & Lifecycle

```
[ Spectrum Client POST ]
          │
          │ (Thread Spawn)
          ▼
[ lanayru.py: deploy_lanayru_worker ]
          │
          ├─► Create ScyllaDB Schemas
          ├─► Setup L2 Overlay bridges
          ├─► Provision thin Linstor disks (RF=3)
          └─► Define & Start Libvirt VMs
```

---

## Technical Reference
* For details on high-level architecture designs and the host NAT bypass solution, refer to the main [Lanayru Design Guide](file:///C:/Users/AuraFlight/Desktop/container-hci/docs/lanayru.md).
