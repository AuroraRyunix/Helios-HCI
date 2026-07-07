# Deploy Updates Utility - Technical Documentation

This document details the internal technical structure, functions, flowcharts, and mindmaps of the updates deployment orchestrator (`deploy_updates.py`).

## Technical Mindmap

```mermaid
mindmap
  root((deploy_updates.py))
    Environment & Inputs
      HELIOS_NODES & HELIOS_PASSWORD environment variables
      Fast mode switch (--fast)
    Certificate Sync
      Ensures shared SSL cert on Node 1 (via OpenSSL)
      Replicates server.crt/key across other nodes
    SSH/SFTP File Transfer
      paramiko.SSHClient & paramiko.AutoAddPolicy
      replaces Windows CRLF (\r\n) with Unix LF (\n)
      chmod 755 /usr/local/bin/ executions
    Systemd & Container Config
      Injects systemd unit configuration strings
      Tears down and rebuilds Spectrum UI container
      Reloads systemctl daemon-reload
```

## Function & Logic Breakdown

### Line Ending Normalization
- **`put_text_file(sftp, local_path, remote_path)`**: Reads local source files, replaces `\r\n` (carriage returns) with standard Unix `\n` to prevent `127 Command not found` shebang execution errors on Linux hosts, and transfers files via SFTP.

### Shared Certificate Seeding
- Standardizes cluster ingress certificates.
- Connects to **Node 1** via SSH. If `/etc/hci/spectrum/certs/server.crt` is missing, generates a self-signed key using `openssl req`.
- Copies these credentials into memory to write them on all remaining cluster nodes.

### Deployment Loop (`main()`)
Iterates over node IPs:
1. Opens Paramiko SSH and SFTP clients.
2. Copies all 20+ core python binaries, CLI scripts (`spark`, `cluster`, `valcli`, `mcli`, `catcli`, `nodetool`), and configuration models directly to `/usr/local/bin/`.
3. Ensures clean execution permissions (`chmod 755`).
4. Writes systemd unit configuration files (`/etc/systemd/system/*.service`).
5. If not running in `--fast` mode:
   - Copies static UI assets and `Dockerfile` to target hosts.
   - Triggers `podman build -t localhost/spectrum:latest /tmp/spectrum_build` on the remote hosts to rebuild the UI container.
   - Purges old containers and restarts the systemd units.
6. Calls `systemctl daemon-reload` and restarts services to load updates.
