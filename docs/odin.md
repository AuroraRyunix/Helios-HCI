# Odin (Cluster Configuration & Interface Service)

Odin is the local cluster configuration gateway. It is the direct equivalent of Nutanix **Zeus**.

> [!NOTE]
> **Name Origin:** In Norse mythology, **Odin** is the Allfather and king of Asgard, who sits at the head of the gods to govern order, consensus, and wisdom. In Helios-HCI, **Odin** (working with ZooKeeper) provides the centralized coordination, configuration store, and leader election for the entire cluster.

## Nutanix Role (Zeus)
In Nutanix, Zeus is the software library/wrapper interface that all other local services (such as Stargate, Medusa, Curator) use to communicate with Zookeeper. Instead of services querying Zookeeper nodes directly, they interface with Zeus to read and update cluster-wide configurations.

## Containerized HCI Approach
In our architecture, **Odin** runs as a lightweight service (e.g., Python or Go) in a container on every host. 
Instead of linking complex ZooKeeper client libraries into every single component:
1. **Zookeeper Client Daemon**: Odin connects to the Zookeeper cluster and maintains active watches on key nodes (such as cluster membership, disk status, and active VM list).
2. **Local Interface**: Odin exposes a simple REST API and/or writes state to a shared directory (e.g., `/run/hci/state.json`) mounted by other services on the host.
3. **Cluster Status Reporting**: Other local services publish their health and topology updates directly to Odin, which handles pushing these updates to Zookeeper under transaction guarantees.

---

## Technical Details

### API Endpoints Provided by Odin
* `GET /api/v1/cluster`: Returns the global cluster topology, active hosts, and system state.
* `GET /api/v1/storage`: Returns the list of storage pools and disk allocations.
* `POST /api/v1/heartbeat`: Receives local service heartbeats (Aether, Hydra, Spark) and updates Zookeeper.
* `POST /api/v1/vm/register`: Registers a VM running on the local hypervisor to the global registry.

### Sample Odin Configuration (`/etc/hci/odin/odin.json`)
```json
{
  "zookeeper_hosts": "127.0.0.1:2181",
  "local_node_ip": "10.10.102.222",
  "api_port": 8080,
  "heartbeat_interval_sec": 5
}
```

### Communication Flow
```
[ Aether / Hydra / Spectrum ] (Local Services)
           │  (REST API / Unix Socket)
           ▼
     [ Odin (Zeus) ] (Container on Local Host)
           │  (Zookeeper Protocol)
           ▼
 [ Zookeeper Cluster ] (Distributed Nodes)
```
