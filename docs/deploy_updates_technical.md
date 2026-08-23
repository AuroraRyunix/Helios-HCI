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
    Release Key Pinning
      Reads the release public key (HELIOS_RELEASE_PUBKEY)
      Refuses to distribute anything containing a PRIVATE KEY
      Writes /etc/hci/keys/release_ed25519.pub on every node
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

### Release Public Key Pinning
- Resolves the release public key from `HELIOS_RELEASE_PUBKEY`, then `./release_ed25519.pub`,
  then `~/.helios/release_ed25519.pub`.
- Writes it to `/etc/hci/keys/release_ed25519.pub` (mode `0644`, directory `0755`) on every
  node. `provision.py` pins this when a node is built; a node built before update signing
  existed has no key at all, and `check-updates` fails closed without one.
- Refuses to run if the file contains `PRIVATE KEY`. Pointing this at the signing key
  instead of its public half would copy the one secret the whole scheme depends on onto
  every node in the fleet.
- If no key is found locally, the rollout continues and prints how to supply one; nodes
  keep whatever `provision.py` pinned.
- See [update_signing.md](./update_signing.md).

### Deployment Loop (`main()`)
Iterates over node IPs:
1. Opens Paramiko SSH and SFTP clients.
2. Copies all 20+ core python binaries, CLI scripts (`spark`, `cluster`, `valcli`, `mcli`, `catcli`, `nodetool`), shared modules (`helios_zk.py`, `helios_sig.py`), and configuration models directly to `/usr/local/bin/`.
3. Ensures clean execution permissions (`chmod 755`).
4. Writes systemd unit configuration files (`/etc/systemd/system/*.service`).
5. If not running in `--fast` mode:
   - Copies static UI assets and `Dockerfile` to target hosts.
   - Triggers `podman build -t localhost/spectrum:latest /tmp/spectrum_build` on the remote hosts to rebuild the UI container. A build failure is fatal, because restarting afterwards would only reinstate the image already running.
   - Restarts the console with a single `systemctl restart spectrum`, then checks the unit is `active`. Both halves matter: a `stop && podman rm -f && start` chain short-circuits on a failed stop and does nothing at all, and the gap between its commands is long enough for spark-daemon's drift check to start the unit and cancel the pending stop job — see [cluster_state.md](./cluster_state.md#a-unit-that-is-mid-transition-is-not-drift). An accepted job is also not a serving console, so the end state is verified rather than inferred from an exit code, and a console that will not come back stops the rollout for that node.
6. Calls `systemctl daemon-reload` and restarts services to load updates.

### It refuses to roll out under running guests

`hylia` drains a host before a rolling upgrade. This script does not: it restarts fourteen
services in place. Whether that is acceptable while workloads are live is a judgement for
whoever is running it, so the script asks rather than assumes.

- **`running_guests(ssh)`** — `virsh list --state-running --name` on the node. A host that
  cannot answer returns empty: an unanswerable question is not evidence that guests are
  running, and a cluster with no hypervisor must still be deployable.
- **`refuse_if_guests_running(ip, ssh)`** — runs immediately after the SSH connection is
  established and **before anything is uploaded or restarted**, so a refusal costs nothing
  and leaves the node exactly as it was. Names the guests it found. Set
  `HELIOS_ALLOW_RUNNING_GUESTS=1` to proceed anyway; the override is matched exactly, so
  `true`/`yes`/`0` are not accepted as permission.

This was added after a rollout destroyed a guest mid-install. That specific defect was in
spark-daemon's startup and is fixed there, but the shape of the operation has not changed —
`slate`, `vali`, `mipha` and `urbosa` all restart underneath whatever is running.

### SELinux labelling for guest nvram

libvirt writes each UEFI guest's variables to `/var/lib/hci/aether/nvram/`. That is outside
libvirt's own tree, so it inherits the generic `var_lib_t` label and `virtqemud` is denied
`remove_name` and `unlink` there. On an **Enforcing** host the nvram cleanup during a VM
delete fails *after* the domain is already gone, leaving the VM half removed.

`NVRAM_SELINUX` applies `qemu_var_run_t` — the label libvirt's own
`/var/lib/libvirt/qemu/nvram` carries — via `semanage fcontext` plus `restorecon`. It falls
from `-a` to `-m` so an already-labelled node is not an error, and skips entirely where
`semanage` is absent. `provision.py` applies it at install time; this script applies it to
existing nodes.

The test node runs **Permissive**, where this appears only as a denial in the journal and
everything otherwise works — which is exactly the kind of difference that becomes a support
call on somebody else's Enforcing cluster, so `test_rollout_safety.py` asserts both
deployment paths carry it.
