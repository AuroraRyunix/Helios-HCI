# Kass: Cluster Event Streaming & Telemetry Bus (Apache Kafka)

Kass is Helios's distributed event streaming service, powered by **Apache Kafka**. Named after the traveling Rito bard who chronicles and broadcasts ancient ballads across all corners of Hyrule, Kass acts as the cluster-wide event broadcaster and telemetry messaging backbone.

## Overview & Architecture

* **Unit Name:** `kass.service` (managed as a systemd Podman Quadlet: `/etc/containers/systemd/kass.container`)
* **Underlying Engine:** Apache Kafka (`docker.io/apache/kafka:3.8.0`)
* **Default Port:** `9092` (bound on `0.0.0.0:9092`, advertised as `<node_ip>:9092`)
* **Coordination Plane:** ZooKeeper ensemble (`127.0.0.1:2181` / `<node_ips>:2181`)
* **Storage Volumes:**
  * Topic Partitions & Commit Logs: `/var/lib/hci/kafka/data` (mapped to `/var/lib/kafka/data:Z`)
  * Container Application Logs: `/var/lib/hci/kafka/log` (mapped to `/var/lib/kafka/logs:Z`)
* **Resource Controls:**
  * Hard Memory Ceiling: `MemoryMax=1.5G`
  * High Memory Watermark: `MemoryHigh=1.2G`
  * CPU Scheduling Weight: `CPUWeight=100`

## Supervision & Lifecycle

1. **Reconciliation Order:**
   * Kass is declared in `MANAGED_SERVICES` in `spark_daemon_decoded.py`.
   * It starts after core storage dependencies (`daruk`) and ZooKeeper are healthy.
   * `spark-daemon` monitors Kass health and readiness via port `9092`.
2. **Maintenance Mode:**
   * Like all cluster services, Kass carries `ConditionPathExists=!/etc/hci/maintenance.state`.
   * During both **Full Maintenance** and **Service Maintenance**, Kass is cleanly stopped on the maintenance host to allow software upgrades, OS kernel patches, and JVM updates.
   * When the host leaves maintenance, Kass is restarted automatically alongside the rest of the cluster services.
3. **Diagnostics & Status:**
   * Health status is verified by `mcli` via `kass_status`.
   * Monitored for crash-looping and flapping by `mcli-runner`'s watchdog.
   * Visible in Phoenix LiveView (`spectrum-phx`), `static/app.js`, and CLI tools (`spark`, `cluster status`).

## Configuration (Quadlet Container)

```ini
[Unit]
Description=Kass (Kafka) Cluster Event Streaming Service
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Restart=always
CPUWeight=100
MemoryMax=1.5G
MemoryHigh=1.2G

[Container]
Image={image}
Network=host
Volume=/var/lib/hci/kafka/data:/var/lib/kafka/data:Z
Volume=/var/lib/hci/kafka/log:/var/lib/kafka/logs:Z
Environment=KAFKA_NODE_ID={node_id} "KAFKA_ZOOKEEPER_CONNECT={zoo_servers}" KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{node_ip}:9092" KAFKA_LOG_DIRS=/var/lib/kafka/data KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 KAFKA_AUTO_CREATE_TOPICS_ENABLE=true

[Install]
WantedBy=multi-user.target
```
