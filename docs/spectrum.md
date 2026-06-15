# Spectrum (Cluster Management Portal)

Spectrum is the cluster management gateway and web administration console. It is the direct equivalent of Nutanix **Prism**.

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
* `GET /api/v1/cluster/status`: Aggregated health of all services.
* `POST /api/v1/vms/create`: Creates a new VM template, allocates virtual storage via `Hydra` & `Aether`, and registers the VM in `libvirt`.
* `DELETE /api/v1/vms/<name>`: Destroys a VM and deletes its virtual disks from `Aether`.
