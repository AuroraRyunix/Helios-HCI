# Hydra (Distributed Metadata Abstraction)

Hydra is the distributed metadata database and abstraction layer. It is the direct equivalent of Nutanix **Medusa**.

> [!NOTE]
> **Name Origin:** In Greek mythology, the **Hydra** is a multi-headed serpent that grows back heads when they are cut off, representing extreme resilience. **HydraDB** uses a multi-node ScyllaDB replication ring so that database access remains online and available even if individual nodes go offline.

## Nutanix Role (Medusa)
In Nutanix, Medusa acts as the database proxy and abstraction layer sitting in front of **Cassandra** (a highly customized Apache Cassandra database running on the CVMs). Medusa manages metadata such as virtual disk locations, block maps, snapshots, and cluster configuration. It handles Paxos operations to ensure strict consistency.

## Containerized HCI Approach
In our architecture, **Hydra** runs in a container on every host, leveraging a distributed **ScyllaDB** (a C++ rewritten, high-performance Cassandra-compatible database) or standard **Cassandra** container cluster.
1. **Consensus & Clustering**: The ScyllaDB/Cassandra instances on all three hosts auto-discover each other using Zookeeper/Odin and form a ring topology.
2. **Replication & Consensus**: Data keyspaces use a replication factor of 3 (RF=3) with local quorum write/read consistency. This ensures metadata is consistent and partition-tolerant.
3. **CQL HTTP Proxy (Daruk)**: To avoid the massive host CPU overhead of spawning containerized `cqlsh` python sessions repeatedly, a persistent **CQL HTTP Proxy (Daruk)** (`daruk.service`) runs inside the `systemd-hydra-db` container.
    * **Port**: `9043` on `localhost` (bridged via `Network=host`).
    * **Connection**: Maintains a single, persistent native python `cassandra-driver` connection to ScyllaDB.
    * **Uptime Fallback**: Clients issue HTTP POST requests containing CQL queries, which execute in under 2ms. If the proxy is unavailable, clients automatically fall back to executing `cqlsh` directly, ensuring zero-downtime database access.

---

## CQL HTTP Proxy API Specification

The CQL HTTP Proxy exposes a single lightweight endpoint on localhost for executing raw database queries without startup latency.

### Execute Query Endpoint
* **URL**: `http://127.0.0.1:9043/query`
* **Method**: `POST`
* **Headers**: `Content-Type: text/plain`
* **Request Body**: Raw CQL statement string.

#### Success Response (HTTP 200 OK)
Returns a list of rows represented as dictionary objects mapping columns to values:
```json
{
  "status": "success",
  "rows": [
    {
      "key": "urbosa_enabled",
      "value": "true"
    },
    {
      "key": "dns_mtu",
      "value": "1500"
    }
  ]
}
```

#### Error Response (HTTP 400 Bad Request / 500 Internal Error)
Returns error status with the ScyllaDB driver execution exception:
```json
{
  "status": "error",
  "error": "Error Message details (e.g., SyntaxException, KeyspaceNotDefined)"
}
```

### Python Client Integration (`run_cql_query`)
All internal python daemons call `run_cql_query(cql_query)` which encapsulates this HTTP REST API. It handles returned data formatting dynamically:
1. **JSON Select Queries** (e.g., `SELECT JSON *`): Unpacks the raw `"json"` column value directly.
2. **Standard Select Queries**: Joins row values with space separators (e.g., `key value`). If clients query settings (`SELECT key, value`), it falls back to space-separated lines (`urbosa_enabled true`).
3. **Execution Fallback**: If the HTTP query fails, it encodes the CQL query in base64 and spawns `podman exec -i systemd-hydra-db cqlsh` as a secondary channel.

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
