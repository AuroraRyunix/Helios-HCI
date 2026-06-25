# Spectrum (Cluster Management Portal)

Spectrum is the cluster management gateway and web administration console. It is the direct equivalent of Nutanix **Prism**.

> [!NOTE]
> **Name Origin:** A **prism** splits light into a **spectrum** of colors. Since this component is the direct equivalent of Nutanix **Prism**, it is named **Spectrum** to represent the visual interface showing the full range of cluster states, workloads, and performance metrics.

## Nutanix Role (Prism)
In Nutanix, Prism (both Prism Element and Prism Central) is the management interface. It exposes a HTML5 web UI, REST APIs, and command-line interfaces (nCLI) for VM creation, virtual disk provisioning, performance monitoring, cluster expansions, and hardware alerts.

## Containerized HCI Approach
In our architecture, **Spectrum** runs as a containerized web application on each host (or on a subset of hosts for HA).
1. **Unified API Gateway**: The Spectrum backend exposes a clean REST API that handles orchestrating actions across the cluster.
2. **Local Libvirt Integration**: It connects to the host's `/var/run/libvirt/libvirt-sock` (mounted into the container) to perform hypervisor actions (start/stop/migrate VMs).
3. **Consensus & Metadata Interaction**:
   - Spectrum queries **Odin** (Zeus) to get active cluster topology, node IPs, and service status.
   - It reads/writes VM configs and task states in **Hydra** (Medusa).
4. **Interactive Dashboard**: Serves a premium, responsive web interface built using HTML, CSS, and JS (with modern typography and dark modes) on port `8443` or `443`.

---

## Technical Architecture

```
                 [ Web Browser / API Clients ]
                              │
                              │ (HTTPS on Port 8443)
                              ▼
                   [ Spectrum Container ]
                    ├── Frontend: HTML5 / CSS / Vanilla JS
                    └── Backend: Go or Python Web Server
                          │
         ┌────────────────┼────────────────┐
         ▼                ▼                ▼
  [ Local libvirt ]   [ Odin API ]   [ Hydra DB ]
  (VM Operations)    (Cluster State) (VM Metadata)
```

---

## Deployment Configuration

### Volumes Mapped into Spectrum
- `/var/run/libvirt/libvirt-sock:/var/run/libvirt/libvirt-sock` (To trigger VM management commands on the host hypervisor).
- `/etc/hci/spectrum/spectrum.env` (Environment file for admin password, API ports, SSL certs).

### Sample REST API endpoints
* `GET /api/status`: Returns current hypervisor state, VM statistics, and cluster daemon status.
* `GET /api/catalyst/tasks`: Returns recent Catalyst task execution queue states and progress.
* `POST /api/mimir/run`: Submits a manual diagnostics task (`mimir_diagnostics`) to Catalyst to execute health checks.
* `POST /api/host/reboot`: Initiates a graceful reboot task sequence for a cluster host (coordinates entering maintenance, evacuating/stopping VMs, invoking spark reboot, waiting for host lifecycle, and rejoining the cluster).
* `POST /api/v1/vms/create`: Creates a new VM template, allocates virtual storage via `Hydra` & `Aether`, and registers the VM in `libvirt`.
* `DELETE /api/v1/vms/<name>`: Destroys a VM and deletes its virtual disks from `Aether`.


---

## Technical Details & Resilience Fixes

### 1. ScyllaDB Bootstrap Fallback
During startup, the Spectrum container (`systemd-spectrum`) establishes a connection to the local database to verify keyspaces and tables. If the local ScyllaDB instance is bootstrapping or down (e.g. after a reboot/rejoin), Spectrum reads all cluster IP addresses from `/etc/hci/cluster.json` and automatically falls back to active database nodes. This prevents the WebUI from blocking or timing out during startup.

### 2. Task Cache Fallback
To ensure UI responsiveness, the `/api/catalyst/tasks` endpoint maintains an in-memory cache of recent tasks. If a database query fails due to temporary connection latency or quorum changes, Spectrum serves the cached task list rather than throwing an error, preventing the UI progress indicator from resetting to grey.

### 3. Guest Display Auto-Resize (Windows vgpusrv Service)
When using the VirtIO-GPU display driver (`viogpu` / `viogpudo`) in Windows guests, dynamic resolution auto-resizing via the VNC standard `SetDesktopSize` command is supported only if the user-mode helper service (`vgpusrv.exe`) is registered and active in the guest. By default, standard Windows driver setup installs the kernel display driver but does not register this service.

To resolve display auto-resize constraints inside Windows guests:
1. Open PowerShell or Command Prompt as **Administrator** inside the guest OS.
2. Locate `vgpusrv.exe` (on the mounted VirtIO CD-ROM under `viogpudo\2k12\amd64\vgpusrv.exe` or local path `C:\Program Files\Qemu-Ga\vgpusrv.exe`).
3. Execute the service installer:
   ```cmd
   vgpusrv.exe -i
   ```
4. Start the service:
   ```cmd
   net start vgpusrv
   ```
This configures `vgpusrv` to start automatically on system boot, enabling the guest OS to dynamically adjust its display resolution when the VNC console's browser window is resized.

---

## Command Examples & Syntax

### 1. Check Spectrum Service Status
Spectrum is managed as a systemd service that wraps a Podman container:
```bash
systemctl status spectrum
```

### 2. View Container Logs
Since Spectrum runs in a Podman container, you can check its logs directly:
```bash
# View recent logs from the container
podman logs systemd-spectrum

# View logs via journalctl
journalctl -u spectrum -n 50 --no-pager
```

### 3. Restart Spectrum Service
```bash
systemctl restart spectrum
```

### 4. Query local API
You can query the WebUI status endpoint directly using curl:
```bash
curl -k -s https://127.0.0.1:8443/api/status
```

