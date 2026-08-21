# Spectrum (Cluster Management Portal)

> **Being replaced.** A Phoenix LiveView rewrite runs beside this service on port 8444
> and takes over routes as they are ported. See [spectrum_phx.md](./spectrum_phx.md),
> which also records where the pages documented here disagreed with the database.

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

### 2a. Polls read one partition at a time

`hydra.logos_metrics` is `PRIMARY KEY (node_ip, timestamp)` clustered `timestamp DESC`
with a 24-hour TTL, and Logos writes one row per node every 30 seconds — about 2,880 live
rows per node (measured: 2,879 on the single-node test cluster). `/api/cluster/metrics`
used to read all of them with `SELECT JSON * FROM hydra.logos_metrics` — no `WHERE`, no
`LIMIT` — on every poll of every open browser tab, and `metrics.html` then discarded all
but the newest 40 samples per host. That is a full cluster scan every 30 seconds per
viewer, to draw 120 points.

Each node's partition is now read with a `LIMIT`, which the clustering order answers
directly. The response carries a `metrics_unavailable` list naming any node whose
partition could not be read: a node missing from the chart because nobody could ask it is
not a node that reported nothing, and the two must not look alike.

`hydra.dagur_runs` had the same shape with a worse symptom. `SELECT JSON * FROM
hydra.dagur_runs LIMIT 100` has no `WHERE`, so its 100 rows are the first ones the
coordinator reaches in *token* order — never "the 100 most recent runs" the page claims
to show. On the test cluster that answer was 61 rows of one job and three of another, out
of order. Runs are now read per job (`WHERE job_name = ? LIMIT n`) and merged newest-first.

### 2b. Deleting removes the storage first, and checks

`POST /api/images/delete` used to delete the catalogue row, then fire
`linstor resource-definition delete` and a fan-out `rm -f {path}` without checking either,
and answer `200` regardless. That ordering is the one where a downstream failure is
unrecoverable from the UI: the storage is still allocated and the only handle on it — the
row naming its path — has already been thrown away. The DRBD resource sits on every node
holding space that nothing in the console can see, and the operator has been told the
delete worked.

The order is inverted. The backing store goes first and its result is checked; a failure
leaves the row in place, so the image is still listed and the delete can be retried. Only
once there is nothing left for the row to point at is the row removed. A `404` is returned
for an image that is not in the catalogue rather than a cheerful `200`.

The path is validated, not merely quoted. A correctly quoted `rm -f /etc` is still
`rm -f /etc`, so the recorded path must be a DRBD device under `/dev/drbd/` or a file
under `/var/lib/hci/aether/volumes/`, with no `..` segment; anything else is refused and
reported. A `/dev/drbd/...` path is never removed with `rm` at all — that would delete a
udev symlink and leave the resource, and its storage, behind. It is removed by deleting
the LINSTOR resource definition, which tears the device down on every node.

`GET /api/images` is now a read. It used to scan
`/var/lib/hci/aether/volumes/default-image-container` and `INSERT` a catalogue row for
every image-looking file it found, so opening the Images page wrote to the database, from
every tab, on every refresh. See [Reconciling the catalogue](#reconciling-the-catalogue)
below for why that scan was not merely misplaced.

### 2c. A VM's record outlives its guest until the guest is provably gone

`POST /api/vms/delete` used to read `host_ip`, destroy the domain at that address, and
then delete the row unconditionally. A VM that migrated between the read and the destroy
was destroyed nowhere — the destroy went to the host it had just left — and its row
disappeared anyway. The result is a guest running on a host that nothing in the cluster
associates with it: invisible in the console, uncounted against the host's capacity, and
holding its DRBD device open against the next thing to claim the name.

The delete now uses [Daruk](./daruk.md)'s typed compare-and-swap endpoints:

1. **Read the row first.** "No such VM" is decided before any conditional write, because
   `UPDATE ... IF status != ?` *applies* against a row that does not exist and creates a
   partial one — taking the migration lock on an unknown name would invent a VM rather
   than report a missing one.
2. **Take the migration lock** (`/v1/vm/migrate-lock`). A refusal means a live migration
   is in flight, which is the worst possible moment to delete a VM. Holding it also
   serialises two concurrent deletes of the same VM.
3. **Prove the placement** (`/v1/vm/set-state`, conditioned `IF host_ip = ?`). Migration
   is not the only writer — the reconciler releases a placement too — so the address the
   destroy is about to be sent to is confirmed by a compare-and-swap, not by a read.
4. **Destroy, undefine and delete storage, checked.** A domain or LINSTOR resource that is
   already gone is the state being asked for and is not an error; anything else is, and
   leaves the row in place.

At every refusal the row survives, the operator is told where the VM actually is, and the
lock is released conditionally so a late cleanup cannot unlock a migration that started
afterwards.

What is still missing is a conditional *delete*. Daruk has no `/v1/vm/delete`, so the
final `DELETE FROM hydra.vms` is unconditional. It runs while this caller holds the
migration lock and has just proved the placement, which closes the window the defect was
about; a `DELETE ... IF host_ip = ?` would close it outright.

### Reconciling the catalogue

`GET /api/images` no longer writes, and nothing replaces the scan it used to perform. That
is deliberate, and the reason is not only "a GET should not write":

- The rows it wrote were guesses. Image upload puts an image on a replicated DRBD device
  (`/dev/drbd/by-res/img-<slug>/0`), not in that directory, so the only files the scan
  ever caught were ones nobody registered. It recorded them with a `path` that no LINSTOR
  resource backs — which is exactly the row shape the delete path then has to treat as a
  special case.
- It only ever saw one node. The console runs in a container whose view of
  `/var/lib/hci/aether/volumes` is this host's, so a page load on node A and the same page
  load on node B disagreed about what the cluster's catalogue contained.
- It had no way to report a failure. A scan that half-succeeded left a partly-reconciled
  catalogue and returned `200`.

If reconciling the catalogue against the filesystem is genuinely wanted, it belongs in a
[Dagur](./dagur.md) job in `hydra.dagur_schedules`, beside `orphaned_disks_cleanup`: it
runs once for the cluster rather than once per viewer, its result is recorded in
`dagur_runs`, and a failure is visible and retriable. No such job has been added here —
nothing establishes that the directory is authoritative over the catalogue, and inventing
a scheduled writer on that assumption is how the wrong rows would get written on a
schedule instead of on a page load.

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



---

## Technical Reference

For the internal code structure, class/function details, and execution flowcharts, see the [Technical Guide](./spectrum_technical.md).
