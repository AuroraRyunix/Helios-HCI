# Spark (Cluster Service & Bootstrap Manager)

Spark is the host-level bootstrap manager and service state coordinator. It is the direct equivalent of Nutanix **Genesis**.

> [!NOTE]
> **Name Origin:** A **spark** is a tiny, fiery particle that initiates a fire or combustion. In Helios-HCI, **Spark** is the bootstrap manager and Genesis agent that ignites and starts all other cluster containers, services, and management nodes.

## Nutanix Role (Genesis)
In Nutanix, Genesis runs on every node and is responsible for managing the lifecycle of other services (starting, stopping, monitoring). It is the first service to start, running independently of cluster consensus, and is used to bootstrap the cluster initially.

## Containerized & Host-Level HCI Approach
In our architecture, **Spark** is split into two components:
1.  **Spark CLI** (`/usr/local/bin/spark`): A host-level command-line utility used to query local container states, PIDs, and detect ZooKeeper leadership.
2.  **Spark Daemon** (`/usr/local/bin/spark-daemon`): A secure, host-level HTTPS service running on port `9099`. It replaces legacy passwordless root SSH key distribution across nodes, allowing secure remote command orchestration.

---

## Spark Daemon mTLS API Spec

The `spark-daemon` listens on port `9099` using **Mutual TLS (mTLS)** for secure authentication and authorization:
* **Server Verification**: The daemon presents its node certificate (`node.crt`) and key (`node.key`). Clients verify it against the trusted CA.
* **Client Verification**: The daemon requires clients to present a valid client certificate (`client.crt`) signed by the cluster CA (`ssl.CERT_REQUIRED`).

### API Endpoints
* `POST /api/v1/execute`
  * **Description**: Executes a command locally on the host.
  * **Request Body** (JSON):
    ```json
    {
      "command": "systemctl restart hydra-db"
    }
    ```
  * **Response Body** (JSON):
    ```json
    {
      "returncode": 0,
      "stdout": "...",
      "stderr": ""
    }
    ```

---

## Directory & Certificate Layout

### 1. Staging Directory (Node 1 only, post-provisioning)
Path: `/var/lib/hci/certs_staging/`
* Stores CA, client, and all individual node certificates/keys before cluster-wide distribution.
* Automatically cleaned up after successful bootstrapping.

### 2. Daemon Certificates (Restricted to Root)
Path: `/etc/hci/spark/certs/`
* `ca.crt`: Trusted Root CA certificate.
* `node.crt`: Node certificate containing the host IP address in the Subject Alternative Name (SAN).
* `node.key`: Node private key (`chmod 600`).

### 3. Client Certificates (Used by `cluster` and `allssh`)
Path: `/root/.certs/`
* `ca.crt`: Trusted Root CA certificate.
* `client.crt`: Signed client certificate.
* `client.key`: Client private key (`chmod 600`).

---

## mTLS Bootstrapping & Security Lifecycle

### 1. Staging (`provision.py`)
* The local provisioning tool generates all certificates (CA, client, and host keys) and stages them in `/var/lib/hci/certs_staging/` on **Node 1** via SSH.
* The `spark-daemon` systemd service files and binaries are copied to all nodes, but the service is kept inactive.

### 2. Synchronization & SSH Hardening (`cluster create`)
* Executing `cluster create` on Node 1 prompts the administrator for the root SSH password.
* It uses `sshpass` to copy the staged certificates to `/etc/hci/spark/certs/` and `/root/.certs/` on all target nodes.
* It starts the `spark-daemon` on each node.
* It deletes the inter-node passwordless SSH keys (`/root/.ssh/id_rsa` and `/root/.ssh/id_rsa.pub`) on all nodes.
* The local staging folder is then removed.

### 3. Cleanup (`cluster destroy`)
* Running `cluster destroy` executes a remote background command on all nodes to stop and disable `spark-daemon` and delete the `/etc/hci/spark/certs/` and `/root/.certs/` directories.

---

## Systemd Daemon Configuration (`/etc/systemd/system/spark-daemon.service`)

```ini
[Unit]
Description=Spark Host Management Daemon
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/spark-daemon
Restart=always
User=root

[Install]
WantedBy=multi-user.target
```

---

## Command Examples & Syntax

### A. Host CLI Tool (`spark`)
The `spark` utility is run directly on the host console to check and control local cluster processes:
```bash
# Check status of all managed services (UP/DOWN with active MainPIDs)
spark status

# Output status details in machine-readable JSON format
spark status --json

# Start core bootstrap components (ZooKeeper and Spark Daemon)
spark start

# Stop core bootstrap components locally
spark stop

# Gracefully stop all containerized and native cluster services on this node
spark stop all

# Restart the local spark-daemon
spark restart
```

### B. Remote Orchestration Command Execution (mTLS API)
You can test the secure remote command endpoint from any node using `curl`. Since the daemon enforces Mutual TLS, you must supply the client certificate, client private key, and trust store:
```bash
# Execute a systemctl status command on a remote node via spark-daemon
curl --cacert /root/.certs/ca.crt \
     --cert /root/.certs/client.crt \
     --key /root/.certs/client.key \
     --header "Content-Type: application/json" \
     --data '{"command": "systemctl is-active spectrum"}' \
     https://10.10.102.222:9099/api/v1/execute

# Query cluster-wide status from the Spark orchestration layer
curl --cacert /root/.certs/ca.crt \
     --cert /root/.certs/client.crt \
     --key /root/.certs/client.key \
     https://10.10.102.220:9099/api/v1/cluster/status
```

---

## Service Bootstrap & Workload Autostart Sequence

When `spark-daemon` starts up (e.g. during host boot), it spawns a background thread to orchestrate starting local workloads:

1. **ZooKeeper Startup**: Starts the local ZooKeeper instance (`systemctl start zookeeper`) if it is not already active. (This executes unconditionally, even in maintenance mode, to preserve cluster quorum).
2. **Maintenance Mode Check**: Checks if `/etc/hci/maintenance.state` exists. If the host is in maintenance mode, it halts the autostart thread here, leaving all other database, storage, and UI workloads stopped.
3. **Quorum Consensus Verification**: Polls local ZooKeeper port `2181` until a quorum mode (`follower`, `leader`, or `standalone`) is established.
4. **Cluster State Verification**: Queries the ZooKeeper database for `/cluster_state`. If the cluster state is set to `stopped` (e.g. administrator manually stopped the cluster), it skips starting the workloads.
5. **Workload Autostart**: Starts the following local cluster services sequentially:
   - `hydra-db` (ScyllaDB container)
   - `aether` (Linstor Satellite storage container)
   - `spectrum` (Management WebUI container)
   - `bifrost` (Floating VIP Manager)
   - `dagur` (Task Scheduler)
   - `mimir` (Diagnostics/Health Monitoring)
   - `vali` (VM Scheduler / DRS)
   - `catalyst` (Task Coordinator)
   - `gatoway` (L2 Network Sync)
   - `logos` (Distributed Metrics)
   - `mipha` (HA Cluster Monitor)

