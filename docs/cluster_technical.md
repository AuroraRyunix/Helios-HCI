# Cluster Management & Lifecycle Utility - Technical Documentation

This document details the internal technical structure, functions, flows, and mindmaps of the cluster management utility (`cluster_new.py`).

## Technical Mindmap

```mermaid
mindmap
  root((Cluster Orchestrator))
    Parallel Command Execution
      run_parallel (threading)
      run_parallel_checked (verifies rc == 0)
      run_remote_spark (Port 9099 execution)
    Cluster Status & DB Health
      get_scylla_bootstrap_progress
      check_urbosa_enabled
      make_request (status endpoints via Spark)
    Lifecycle Operations (main command parsing)
      create
        bootstrap cluster.json (witness-aware)
        format disk pools (linstor) - skipped on witness
        generate certs (Odin/Zookeeper/ScyllaDB)
        seed ssh known_hosts
      start
        starts container services via systemd - filtered for workload services
      stop
        graceful unmount and service teardown - filtered for workload services
      destroy
        podman container purge
        LinStor pool deletion - skipped on witness
        data path wipe
```

## Function & Logic Breakdown

### `run_parallel(ips, cmd)`
- Spawns concurrent `threading.Thread` instances to execute commands on multiple IP targets in parallel using `run_remote_spark`.

### `run_remote_spark(ip, command)`
- Calls Spark's REST API execution endpoint on mTLS port `9099`.
- Locates mTLS credentials at local folders `/root/.certs/` or client directories.

### `run_checked_cmd(ip, command, allow_already_exists=False)`
- Runs `run_remote_spark` on a single node and checks return code.
- If command fails (return code != 0) and the error is not a harmless `"already exists"` warning, it prints the error and aborts execution with `sys.exit(1)`.

### `run_parallel_checked(ips, command, allow_already_exists=False)`
- Runs the specified command in parallel on list of target hosts.
- Aborts execution globally with `sys.exit(1)` if any node encounters a fatal error.

### `run_cql_query(cql_query, *args, **kwargs)`
- Submits CQL queries to the ScyllaDB cluster via the local Daruk proxy (`http://127.0.0.1:9043/query`) or direct `podman exec` to the container as fallback.

### `make_request(path, method="GET", payload=None)`
- Helper function that queries Spark REST endpoints over TLS. Tries the floating VIP first, falling back to localhost `127.0.0.1` on port `9099`.

### `main()` Command Processing

#### `cluster create`
- Writes `/etc/hci/cluster.json` on all nodes.
- Orchestrates formatting of storage drives to establish storage pools.
- Seeds TLS certs and SSH public keys to allow passwordless live migration.
- Fires up ZooKeeper (`Odin`), ScyllaDB (`HydraDB`), and launches application daemons.

#### `cluster status`
- Queries host systems, service container states, and keyspaces to report health metrics.
- `--verbose` prints detailed pool allocations, node roles, and disk layout.

#### `cluster start`
- Sends API commands to activate systemd units: `linstor-controller`, `linstor-satellite`, `odin`, `hydra-db`, `spectrum`, `bifrost`, `dagur`, `mimir`, `vali`, `catalyst`, `gatoway`, `logos`.
- Automatically filters out non-witness workloads (e.g. ScyllaDB, Daruk, and application services) to avoid service startup errors on the witness node.

#### `cluster stop`
- Safely shuts down virtual workloads, stops systemd daemons, and unmounts local directories.
- Skips unmounting and stopping non-existent databases and application containers on the witness node.

#### `cluster destroy`
- Purges systemd unit templates, deletes Podman containers, removes storage targets, and cleans `/var/lib/hci` configuration directories.
- Skips LVM signatures removal and physical disks wiping on the witness node.

### Witness Node Orchestration Logic
- **`WITNESS_IP` Detection**: Loads host config from `/etc/hci/cluster.json` and evaluates the `is_witness` boolean (auto-flagged for the 3rd IP in a 3-node layout).
- **Service & Volume Filtering**: Employs `non_witness_ips` lists to prevent SSH command execution for non-witness components (e.g. libvirt VM management, `hydra-db` nodetool checks, ScyllaDB cluster settings, and Spectrum UI reachability on port 8443).
- **Linstor Client configuration**: Restricts client config `controllers` seeds list to `non_witness_ips` to avoid client request timeout attempts directed at the witness.
