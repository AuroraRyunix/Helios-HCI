# Vali CLI Utility - Technical Documentation

This document details the internal technical structure, functions, flowcharts, and mindmaps of the VM manager CLI wrapper (`valcli.py`).

## Technical Mindmap

```mermaid
mindmap
  root((valcli.py))
    API Connections
      run_remote_spark (Port 9099 execution)
      run_mtls_api (REST calls with other nodes fallback)
      run_cql_query (Daruk proxy with container fallback)
    UI Rendering
      print_table ASCII formatter
    CLI Command Parser
      vm commands
        vm.list (retrieves VM json structures)
        vm.create, vm.start, vm.stop, vm.delete
      host commands
        host.list (retrieves node metadata)
      db commands
        db.query (executes raw cql statements)
      backup commands
        backup.target (get/set the artefact destination)
        backup.run, backup.list, backup.verify
        backup.restore, backup.prune
```

## Function & Logic Breakdown

### Communication Routines
- **`run_remote_spark(ip, command)`**: Submits commands to remote hosts via Spark's mTLS port `9099`.
- **`run_mtls_api(ip, path, payload, method="POST")`**: Calls local REST services. If localhost fails or throws an error, iterates over peer IPs listed in `/etc/hci/cluster.json` to retry the request (enforces cluster-wide command failover availability).
- **`run_cql_query(cql_query)`**: Communicates queries to ScyllaDB via the local Daruk proxy port `9043` or container fallback.

### Interface Formatting
- **`print_table(headers, rows)`**: Formats inputs into standard text-based ASCII borders: computes maximum widths per column, prints separating grids (`+---+`), header boundaries, and left-aligns values.

### Subcommand Handlers (`main()`)
- **`vm.list`**: Fetches registered VM records from database table `hydra.vms` and maps node IPs to human-readable hostnames.
- **`vm.create`**: Prompts/reads VM parameters, submits a POST creation payload to Vali's REST endpoints, and polls progress.
- **`vm.start` / `vm.stop` / `vm.delete`**: Submits VM task states to the Catalyst scheduler queue.
- **`host.list`**: Prints cluster nodes, statuses, and hardware information.
- **`db.query`**: Passes raw CQL arguments directly to the Cassandra database cluster.
- **`backup.*`**: Pass-throughs to `/usr/local/bin/saga` via `run_saga()`, which `exec`s
  the tool with the remaining `sys.argv` and exits with its return code.

### Why `backup.*` shells out instead of calling the API

Every other command here reaches the cluster through Daruk, Spectrum or spark-daemon.
The backup commands cannot: a restore has to work on a host whose metadata layer is the
broken thing, so `saga` talks to `cqlsh` and `nodetool` inside the containers directly,
and `DESCRIBE KEYSPACE` — which a metadata backup must capture — is a cqlsh meta-command
with no equivalent over the native protocol. Wrapping the tool keeps `valcli` the single
place an operator looks without duplicating any of that.

`run_saga()` does not capture output. A backup prints progress for as long as it runs,
and buffering it until the end makes a slow run indistinguishable from a hung one.

See [backup_restore.md](./backup_restore.md).
