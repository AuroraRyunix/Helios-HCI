# Walkthrough - Hylia (HA Rolling Upgrade & Life Cycle Management Service)

We have successfully designed, implemented, and deployed **Hylia**, the High-Availability (HA) rolling upgrade and Life Cycle Management (LCM) service. Hylia orchestrates zero-downtime, node-by-node rolling upgrades by leveraging Vali's native maintenance APIs, validates update package files via SHA-256 checksums, and features active-passive ZooKeeper-backed failover to resume upgrades when the leader node reboots.

---

## Changes Implemented

### 1. Database Persistence & HA Resume Hook
* **Schema**: Created ScyllaDB tables `hydra.hylia_jobs` and `hydra.hylia_logs` during Spectrum keyspace initialization in [spectrum_server.py](file:///C:/Users/AuraFlight/Desktop/container-hci/spectrum_server.py):
  * `hydra.hylia_jobs`: Tracks the active UUID, state (`IDLE`, `VALIDATING`, `STARTING`, `UPGRADING`, `COMPLETED`, `FAILED`), target host IPs, current node being upgraded, target build version, parsed manifest JSON, and changelog markdown.
  * `hydra.hylia_logs`: Tracks timestamped log lines for the upgrade run.
* **Resume Capability**: When a ZooKeeper leader reboots during its upgrade phase, leadership is lost. The standby node that becomes the new leader queries `hydra.hylia_jobs` on loop startup, detects the active upgrade job, and seamlessly resumes the rolling loop from the last active node.

### 2. Hylia Upgrade Daemon
* Developed the core orchestrator daemon in [hylia.py](file:///C:/Users/AuraFlight/Desktop/container-hci/hylia.py):
  * **Package Verification**: Unzips uploaded packages to `/tmp/yggdrasil_update` and verifies that all component binaries match their declared SHA-256 hashes in `manifest.json`.
  * **Vali Maintenance Integration**: Submits a `/api/v1/host/maintenance` `enter` task to trigger native VM evacuations via live migrations. Polls node status until it transitions to `IN_MAINTENANCE`.
  * **Remote Deployment**: Pushes updated component files to target hosts via base64 Spark CLI remote execution.
  * **Reboot & Stabilization**: Triggers a remote `reboot` command. Monitors host ping/Spark connection until it goes offline, returns online, and stabilizes.
  * **Normal Restore**: Submits a `/api/v1/host/maintenance` `leave` task. Polls node status until it returns to `NORMAL`.

### 3. REST API Endpoints
* Exposed endpoints in [spectrum_server.py](file:///C:/Users/AuraFlight/Desktop/container-hci/spectrum_server.py):
  * `POST /api/lcm/upload`: Streams the update `.zip` binary to disk, verifies contents/checksums via Hylia, registers a new job, and returns the upgrade preview.
  * `POST /api/lcm/upgrade/start`: Launches the rolling upgrade loop by transitioning the job state to `STARTING`.
  * `GET /api/lcm/upgrade/status`: Returns the active job state, targets, current node, estimated progress percentage, and log history.

### 4. Provisioning Pipeline Integration
* Modified [provision.py](file:///C:/Users/AuraFlight/Desktop/container-hci/provision.py) and [sync_provision.py](file:///C:/Users/AuraFlight/Desktop/container-hci/sync_provision.py):
  * Registered `HYLIA_B64` to package `hylia.py`.
  * Injected deployment directives inside `provision.py` to write `/usr/local/bin/hylia`, set up the systemd unit `hylia.service`, and enable the service on host provision.

### 5. Premium Glassmorphic Frontend
* **UI Dashboard**: Replaced the placeholder card in [lcm.html](file:///C:/Users/AuraFlight/Desktop/container-hci/static/lcm.html) with a rich glassmorphic dashboard:
  * **Drag-and-Drop Dropzone**: Selects or drops update packages with real-time upload progress bars.
  * **Upgrade Preview Card**: Displays target build versions, current vs. new build versions for each component, and the package's `changelog.md`.
  * **Rolling Stepper UI**: Shows host progress cards that change color and state dynamically based on logs (e.g. Evacuating VMs -> Deploying files -> Rebooting -> Stabilizing -> Rejoining -> Completed).
  * **Live Console**: Streamed shell-like output with auto-scroll locking.
* **JS Controller**: Updated [app.js](file:///C:/Users/AuraFlight/Desktop/container-hci/static/app.js) to connect input elements, process binary XMLHttpRequests, send start signals, and handle status logs.

---

## Verification & Testing

### 1. Automated Unit Tests
* Created [test_yggdrasil.py](file:///C:/Users/AuraFlight/Desktop/container-hci/test_yggdrasil.py):
  * Simulates dummy update packages containing mock component scripts, changelogs, and `manifest.json`.
  * Verifies correct file unzipping, hash comparisons, validation error raising (detects corrupt/modified files), and `__build__` version parsing.
* **Results**: Passed successfully.
  ```
  Ran 2 tests in 0.048s
  OK
  ```

### 2. Cluster Deployment & Validation
* Executed `python deploy_updates.py` to compile and distribute updates to all cluster nodes (`10.10.102.120`, `10.10.102.121`, `10.10.102.122`).
* Rebuilt the Podman Spectrum containers and confirmed that the hylia daemon is active across all nodes:
  ```
  10.10.102.120 active
  10.10.102.121 active
  10.10.102.122 active
  ```
* Verified that the UI displays the LCM page with the upload dropzone.
