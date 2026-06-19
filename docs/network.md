# Helios-HCI Network Architecture & Port Reference

This document maps all services, network ports, scope boundaries (localhost-only vs. cluster-wide mesh), and communication paths across the Helios-HCI hypervisor cluster.

---

## 1. Cluster Port Allocation Table

| Service Name | Port | Protocol | Network Scope | Description |
| :--- | :--- | :--- | :--- | :--- |
| **ZooKeeper** (Odin) | `2181` | TCP | Localhost & Cluster | Client API port. Used by Vali, Dagur, Mimir, Catalyst, and Bifrost. |
| **ZooKeeper Peers** | `2888` / `3888` | TCP | Cluster Mesh | Inter-node ZooKeeper sync (follower connections & leader election). |
| **ScyllaDB** (HydraDB) | `9042` | TCP | Localhost & Cluster | Native CQL query port. Used by Logos, Vali, Catalyst, and Spectrum. |
| **ScyllaDB Cluster** | `7000` | TCP | Cluster Mesh | Inter-node database cluster communication (gossip protocol). |
| **Spark Daemon** | `9099` | TCP | Localhost & Cluster | Secure mTLS API port for remote command execution and orchestrating. |
| **Spectrum Web UI** | `8443` | TCP (HTTPS) | Public / Management | Prism Web Console interface and REST API gateway. |
| **Catalyst Manager** | `9091` | TCP (HTTP) | Localhost | Task Manager API. Mapped locally for scheduling and submission. |
| **Vali Placement** | `9095` | TCP (HTTP) | Localhost | Acropolis VM placement, live migration, and DRS controls. |
| **Linstor Controller** | `3370` | TCP | Localhost & Cluster | Linstor Controller REST API and orchestration port. |
| **Linstor Satellite** | `3376` | TCP | Cluster Mesh | Linstor Satellite communication port. |
| **DRBD Replication** | `7700`-`7890` | TCP | Cluster Mesh | DRBD synchronous block-level replication traffic. |

---

## 2. Cluster Communication Flow Chart

The following diagram illustrates how requests flow from the Web UI / Console down to the hypervisor host command layer, distinguishing between localhost-only bindings and inter-node Mutual TLS / consensus connections.

```mermaid
flowchart TB
    subgraph Host1 [hci-node01]
        Spectrum1["Spectrum (WebUI/API)<br>Port 8443"]
        Catalyst1["Catalyst (Orchestrator)<br>Port 9091 (Localhost)"]
        Vali1["Vali (VM Scheduler/DRS)<br>Port 9095 (Localhost)"]
        Spark1["Spark Daemon (mTLS API)<br>Port 9099"]
        ZK1["ZooKeeper (Consensus)<br>Port 2181"]
        DB1["ScyllaDB (Metadata)<br>Port 9042"]
        AetherCtrl1["Linstor Controller<br>Port 3370"]
        AetherSat1["Linstor Satellite<br>Port 3376"]
    end

    subgraph Host2 [hci-node02]
        Spark2["Spark Daemon (mTLS API)<br>Port 9099"]
        ZK2["ZooKeeper (Consensus)<br>Port 2181"]
        DB2["ScyllaDB (Metadata)<br>Port 9042"]
        AetherSat2["Linstor Satellite<br>Port 3376"]
    end

    %% Internal service orchestration and query flows on Host1
    Spectrum1 -.->|"Local API Calls / Command Exec (Port 9099)"| Spark1
    Catalyst1 -.->|"Submit Tasks / Local command (Port 9099)"| Spark1
    Vali1 -.->|"Schedule VM / Run command (Port 9099)"| Spark1
    
    %% Direct TCP status checks
    Spectrum1 -.->|"Check status (Port 2181)"| ZK1
    Catalyst1 -.->|"Check status (Port 2181)"| ZK1
    Vali1 -.->|"Check status (Port 2181)"| ZK1

    %% Local database access via spark execution
    Spark1 -.->|"Executes cqlsh (Port 9042)"| DB1

    %% Inter-node replication and consensus (Cluster Mesh)
    DB1 <===>|"ScyllaDB Gossip & Replication (Port 7000)"| DB2
    ZK1 <===>|"Consensus Election & Sync (Ports 2888/3888)"| ZK2
    AetherSat1 <===>|"Linstor/DRBD Control & Sync (Ports 3376, 7788+)"| AetherSat2
    AetherCtrl1 -.->|"Orchestrate Satellites (Port 3376)"| AetherSat1
    AetherCtrl1 -.->|"Orchestrate Satellites (Port 3376)"| AetherSat2

    %% Remote orchestration and fallbacks
    Spark1 -.->|"Orchestrate remote node (Port 9099)"| Spark2
    Vali1 -.->|"Remote VM Run (Port 9099)"| Spark2
    DB1 -.->|"cqlsh fallback query (Port 9042)"| DB2
```

---

## 3. Communication Boundary Descriptions

### A. Localhost Bindings (No External Access)
To ensure isolation and security, internal daemon API ports are bound exclusively to the loopback interface (`127.0.0.1`):
* **Vali (`9095`) & Catalyst (`9091`)**: These services are not exposed externally. Access from Spectrum is routed locally through the `spark-daemon` mTLS wrapper to prevent unauthenticated commands.
* **Storage Mounts**: Hypervisor VMs access storage containers directly via block-level DRBD device mapping (e.g. `/dev/drbd/by-res/...`), bypassing network-attached filesystem shares entirely for localized guests.

### B. Mutual TLS Mesh (Port `9099`)
* All node-to-node remote command execution is performed through the **Spark Daemon** over port `9099`.
* The daemon requires valid mTLS certificates (`node.crt` and `client.crt` signed by the cluster CA) for every connection, replacing the need for inter-node root SSH keys.

### C. Cluster Data Mesh (Ports `7000`, `2888`, `3888`, `3376`, `7700`-`7890`)
* **Gossip Database Layer**: ScyllaDB nodes talk to each other directly on port `7000` to share cluster metadata, tables partition maps, and telemetry stats.
* **Consensus Sync Layer**: ZooKeeper nodes use ports `2888` and `3888` to elect Odin leaders and synchronize locks.
* **Software-Defined Storage**: Linstor Satellites and Controller communicate over ports `3370` and `3376` for cluster resource provisioning, and DRBD volumes replicate synchronously across the physical disks using TCP ports `7700` to `7890`.

