# Hydra (Distributed Metadata Abstraction)

Hydra is the distributed metadata database and abstraction layer. It is the direct equivalent of Nutanix **Medusa**.

## Nutanix Role (Medusa)
In Nutanix, Medusa acts as the database proxy and abstraction layer sitting in front of **Cassandra** (a highly customized Apache Cassandra database running on the CVMs). Medusa manages metadata such as virtual disk locations, block maps, snapshots, and cluster configuration. It handles Paxos operations to ensure strict consistency.

## Containerized HCI Approach
In our architecture, **Hydra** runs in a container on every host, leveraging a distributed **ScyllaDB** (a C++ rewritten, high-performance Cassandra-compatible database) or standard **Cassandra** container cluster.
1. **Consensus & Clustering**: The ScyllaDB/Cassandra instances on all three hosts auto-discover each other using Zookeeper/Odin and form a ring topology.
2. **Replication & Consensus**: Data keyspaces use a replication factor of 3 (RF=3) with local quorum write/read consistency. This ensures metadata is consistent and partition-tolerant.
3. **Domain API Wrapper**: The Hydra service provides a REST API that sits in front of the local database port (`9042`). Other services query this local API to avoid linking full database driver layers everywhere.

---

## Technical Details

### Backend Configuration (ScyllaDB / Cassandra)
- **Container Name**: `hydra-db`
- **Port Mapping**:
  - `9042`: CQL Native client port (used by Hydra api layer).
  - `7000`: Cassandra inter-node communication port.
  - `7001`: Cassandra SSL inter-node port.
  - `9160`: Thrift client API (disabled by default).
- **Persistent Data Volume on Host**: `/var/lib/hci/hydra/data`

### Schema and Key Entities
Hydra stores:
1. **VM Metadata**: VM configurations, power state, host assignment.
2. **Virtual Disk Map**: Logical disk IDs mapped to Aether storage blocks.
3. **Snapshot History**: References to point-in-time storage states.
4. **Task Progress**: Global operations status (e.g., migration progress, backup progress).

---

## Sample Podman Quadlet / Run Definition
When deploying, the database container is configured with cluster seed node IPs:

```bash
podman run -d \
  --name hydra-db \
  --net=host \
  --restart=always \
  -v /var/lib/hci/hydra/data:/var/lib/cassandra:Z \
  -e CASSANDRA_CLUSTER_NAME=aura-hci-metadata \
  -e CASSANDRA_SEEDS=10.10.102.220,10.10.102.222 \
  -e CASSANDRA_NUM_TOKENS=256 \
  scylladb/scylla:5.4.0
```

*(Note: In Quadlet deployment, these options are written dynamically by Spark during bootstrap).*
