# Vali (VM Manager & Scheduler Service)

Vali is the standalone VM management, placement scheduling, and DRS (load balancing) coordinator for the HCI cluster. It is the direct equivalent of Nutanix **Acropolis (AHV VM Management)**.

## Architecture & Lifecycle
- **Daemon Service**: Runs as a standalone python service (`/usr/local/bin/vali`) listening locally on port `9095`. Managed by systemd (`vali.service`).
- **Leader Election**: All Vali instances run ZooKeeper leader election using ephemeral sequential nodes at `/vali/leader`. The elected Leader is responsible for consuming tasks and executing DRS checks.
- **Autostart Constraint**: Vali is a static systemd service that is dynamically started/stopped by Spark commands (`cluster start` / `cluster stop`) and does not auto-start on boot unless the cluster is online.

## Database Schema
Vali relies on a task queue table in ScyllaDB (`hydra` keyspace):
```sql
CREATE TABLE IF NOT EXISTS hydra.vali_tasks (
    task_id uuid PRIMARY KEY,
    vm_name text,
    action text,         -- 'start', 'stop', 'reboot', 'shutdown', 'reset', 'migrate'
    status text,         -- 'pending', 'processing', 'completed', 'failed'
    target_host text,    -- target IP for migration or explicit start (optional)
    created_at bigint,
    updated_at bigint,
    error_msg text
);
```

## Communication Routing & Security
To keep the Spectrum container boundaries secure, Spectrum is not allowed to communicate directly with Vali. Instead, all actions are routed as follows:
1. Spectrum calls the local `spark-daemon` on `127.0.0.1:9099` via mTLS.
2. The local `spark-daemon` forwards the request locally to `vali` on `127.0.0.1:9095`.
3. Vali queues the task in `hydra.vali_tasks` and polls the database for task completion, returning a synchronous response once processed.

```
[ Spectrum Container ] 
       │ (Secure mTLS)
       ▼
[ spark-daemon (Port 9099) ] (Local Host Daemon)
       │ (Local Forwarding)
       ▼
[ Vali Daemon (Port 9095) ] (Local Host Daemon)
```

## VM Placement & Scheduling (Task Processing)
When the Vali Leader processes a `start` task from the queue:
1. It queries available memory across all online nodes in the cluster.
2. It filters out nodes without sufficient memory to accommodate the VM configuration.
3. It selects the candidate node with the least used memory (dynamic scheduling).
4. It compiles the VM's XML and calls the target node's `spark-daemon` `/api/v1/execute` to define and start the VM.
5. It updates the VM record state to `Running` and `host_ip` to the chosen hypervisor node.

## Distributed Resource Scheduler (DRS)
The Vali Leader runs a periodic DRS loop (every 30 seconds):
1. **Load Evaluation**: It checks memory utilization percentages across all active hypervisor nodes.
2. **Overload Trigger**: A host is considered overloaded if its memory usage exceeds `85%` or if its usage is more than `15%` higher than the average cluster node utilization.
3. **Rebalancing Action**: If an overloaded node is detected, Vali selects a running VM on that host and queues a `migrate` task to live-migrate it to the node with the highest available memory.
4. **Live Migration**: Vali executes live migrations via libvirt:
   `virsh -c qemu:///system migrate --live --unsafe <vm_name> qemu+ssh://root@<target_ip>/system`
   And updates the VM's `host_ip` in ScyllaDB on completion.
