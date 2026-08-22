import paramiko
import os
import sys

fast_mode = "--fast" in sys.argv

def put_text_file(sftp, local_path, remote_path):
    with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read().replace("\r\n", "\n")
    with sftp.open(remote_path, "wb") as f_remote:
        f_remote.write(content.encode("utf-8"))


# The Rust services, by crate directory and the binary each produces. They are built on
# the node rather than shipped as binaries: the cluster is one architecture and one
# distribution, and a cross-built binary in an update package is a thing nobody can
# reproduce from the repository.
#
# `sidon` was reachable only through provision.py until this list existed, which meant a
# cluster could be upgraded but its storage daemon could not -- the one component where
# "reprovision the node to get the fix" is the least acceptable answer.
RUST_CRATES = (
    ("agahnim", "agahnim"),
    ("sidon", "sidon"),
    ("ganon", "ganon"),
)


def mkdir_p(sftp, path):
    """sftp has no mkdir -p, and an existing directory raises."""
    built = ""
    for part in path.split("/"):
        if not part:
            continue
        built += "/" + part
        try:
            sftp.mkdir(built)
        except IOError:
            pass


def upload_crate(sftp, ssh, local_root, crate):
    """Stage one crate's sources under /tmp/<crate>_build. Returns the build directory.

    Every .rs file under src/ is sent, not a named list. sidon has twelve modules and
    ganon four, and a list here would go stale the first time one is added -- silently,
    because a missing module is a compile error on the node long after this script has
    reported success.
    """
    build_dir = "/tmp/%s_build" % crate
    ssh.exec_command("rm -rf %s" % build_dir)
    mkdir_p(sftp, build_dir + "/src")

    local_crate = os.path.join(local_root, crate)
    put_text_file(sftp, os.path.join(local_crate, "Cargo.toml"), build_dir + "/Cargo.toml")

    local_src = os.path.join(local_crate, "src")
    for name in sorted(os.listdir(local_src)):
        if name.endswith(".rs"):
            put_text_file(sftp, os.path.join(local_src, name), build_dir + "/src/" + name)
    return build_dir

# Blindly accepting unknown host keys (paramiko.AutoAddPolicy) means every rollout
# re-trusts whatever currently answers on the node IP -- and what this script pushes
# is root-executed code, so a MITM here owns the whole cluster. Verify against
# known_hosts by default; set HELIOS_SSH_TRUST_NEW_HOSTS=1 only for first contact on
# a trusted provisioning network (provision.py seeds /root/.ssh/known_hosts).
trust_new_hosts = os.environ.get("HELIOS_SSH_TRUST_NEW_HOSTS", "").strip().lower() in ("1", "true", "yes")

def new_ssh_client():
    client = paramiko.SSHClient()
    try:
        client.load_system_host_keys()
    except Exception:
        pass
    user_known_hosts = os.path.expanduser("~/.ssh/known_hosts")
    if os.path.exists(user_known_hosts):
        try:
            client.load_host_keys(user_known_hosts)
        except Exception as e:
            print(f"Warning: could not read {user_known_hosts}: {e}")
    if trust_new_hosts:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    else:
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
    return client


# libvirt writes each UEFI guest's variables to /var/lib/hci/aether/nvram/, which is
# outside its own tree and so inherits the generic var_lib_t label. virtqemud is then
# denied remove_name and unlink on those files, which on an Enforcing host fails the
# nvram cleanup during a VM delete -- after the domain is already gone, so the VM is half
# removed. The test node runs Permissive, where this shows up only as a denial in the
# journal and everything appears to work; that is exactly the kind of difference that
# turns into a support call on somebody's Enforcing cluster.
#
# qemu_var_run_t is the label libvirt's own /var/lib/libvirt/qemu/nvram carries.
NVRAM_SELINUX = """NVRAM_DIR=/var/lib/hci/aether/nvram
mkdir -p "$NVRAM_DIR"
if command -v semanage >/dev/null 2>&1; then
    # -a fails if the rule is already there, so fall through to -m rather than treating an
    # already-correct node as an error.
    semanage fcontext -a -t qemu_var_run_t "${NVRAM_DIR}(/.*)?" 2>/dev/null \
        || semanage fcontext -m -t qemu_var_run_t "${NVRAM_DIR}(/.*)?" 2>/dev/null \
        || true
    restorecon -R "$NVRAM_DIR" 2>/dev/null || true
    echo "nvram label: $(ls -Zd "$NVRAM_DIR")"
else
    echo "nvram label: semanage unavailable, leaving SELinux context alone"
fi
"""

DRBD_TEARDOWN = """# Tear down what is left of DRBD on a node upgraded from a DRBD cluster.
#
# Removing the Quadlets stops the satellite and the controller; it does nothing about
# the kernel side. Four resources stayed up with the module at refcount 5, /dev/drbd*
# nodes still present, and /var/lib/linstor still mounted on one of them, days after
# every trace of LINSTOR had been removed from the tree.
#
# Deliberately not forced at any step. `drbdadm down` refuses a resource something
# still has open, and that refusal is the guard against tearing storage out from under
# a process still using it -- so a failure here leaves the module loaded and the
# packages installed, which is the safe direction. Nothing that follows depends on it.
#
# The backing logical volumes are left alone. They are thin and hold tens of megabytes
# between them, and removing a volume is a decision about data rather than about
# packages.
umount /var/lib/linstor 2>/dev/null
sed -i '\\|/var/lib/linstor|d' /etc/fstab 2>/dev/null
command -v drbdadm >/dev/null 2>&1 && drbdadm down all 2>/dev/null
# A few seconds, because the module does not drop to refcount 0 the instant the last
# resource is downed -- the first pass over a live node downed four resources, failed
# to unload, and left the packages installed and a warning printed for a node that was
# in fact finished. `modprobe -r` rather than rmmod: it takes the dependent transport
# module with it, which rmmod will not.
for _attempt in 1 2 3 4 5; do
    lsmod | grep -q '^drbd ' || break
    modprobe -r drbd 2>/dev/null && break
    sleep 2
done
if ! lsmod | grep -q '^drbd '; then
    rpm -q kmod-drbd9x >/dev/null 2>&1 && dnf remove -y kmod-drbd9x 2>/dev/null
    rpm -q drbd9x-utils >/dev/null 2>&1 && dnf remove -y drbd9x-utils 2>/dev/null
fi
rm -rf /etc/drbd.d /etc/drbd.conf /var/lib/linstor 2>/dev/null

# Name the backing volumes, and remove none of them.
#
# LINSTOR suffixed every volume it created with _00000, so they are identifiable, and on
# a node upgraded from a DRBD cluster they are all that is left of it -- thin, sharing
# vg_aether/thin_pool_aether with the extent store, and invisible to everything. Nothing
# reports them anywhere else, which is how they sit for months.
#
# Not removed, because one of them may be a VM disk whose guest was never migrated, and
# this is a rollout rather than a decision about data. The operator gets the names and
# the sizes and runs lvremove themselves.
leftover=$(lvs --noheadings -o lv_name,vg_name,lv_size,data_percent 2>/dev/null \
           | grep -E '_00000[[:space:]]' || true)
if [ -n "$leftover" ]; then
    echo "LEFTOVER-LVS-BEGIN"
    echo "$leftover"
    echo "LEFTOVER-LVS-END"
fi
true"""


# What `podman build` needs, from spectrum_phx/'s Dockerfile: mix.exs, mix.lock,
# config/, priv/, lib/, assets/ and rel/. `test/` is not copied by the Dockerfile and
# `_build/` and `deps/` are rebuilt in the image, so shipping them would triple the
# payload for nothing.
SPECTRUM_PHX_FILES = ("mix.exs", "mix.lock", "Dockerfile")
SPECTRUM_PHX_DIRS = ("config", "priv", "lib", "assets", "rel")


def upload_spectrum_phx(ssh, sftp, local_root):
    """Stage the Phoenix app's build context as one tarball.

    A hundred-odd files over SFTP is a hundred-odd round trips. One tar upload and one
    remote extract is the same bytes in a fraction of the time, and -- more usefully --
    it is atomic enough that a connection dropped mid-transfer leaves an unusable
    archive rather than a build context that is silently half a version old.
    """
    import tarfile
    import tempfile

    local_app = os.path.join(local_root, "spectrum_phx")
    handle, archive = tempfile.mkstemp(suffix=".tar.gz")
    os.close(handle)
    try:
        with tarfile.open(archive, "w:gz") as tar:
            for name in SPECTRUM_PHX_FILES:
                tar.add(os.path.join(local_app, name), arcname=name)
            for name in SPECTRUM_PHX_DIRS:
                tar.add(os.path.join(local_app, name), arcname=name)

        _, stdout, _ = ssh.exec_command(
            "rm -rf /tmp/spectrum_phx_build && mkdir -p /tmp/spectrum_phx_build")
        stdout.channel.recv_exit_status()
        sftp.put(archive, "/tmp/spectrum_phx.tar.gz")
        _, stdout, _ = ssh.exec_command(
            "tar -xzf /tmp/spectrum_phx.tar.gz -C /tmp/spectrum_phx_build && "
            "rm -f /tmp/spectrum_phx.tar.gz")
        return stdout.channel.recv_exit_status()
    finally:
        try:
            os.unlink(archive)
        except OSError:
            pass


def live_sftp(ssh, sftp):
    """Return a usable SFTP channel, reopening it if the one we hold has been closed.

    A rollout opens one SFTP channel at the start and then spends minutes running remote
    commands -- three cargo builds and a podman image build. The SFTP channel is idle
    through all of it, and it does not reliably survive: the next write comes back
    "Socket is closed" from paramiko's own Channel, which is the server having closed the
    channel out from under a connection that is otherwise perfectly healthy.

    Keepalives on the transport do not help, because the transport was never the thing
    that died. Probing and reopening does, and it is the honest shape anyway: a channel
    opened ten minutes ago is not something to assume is still there.
    """
    try:
        sftp.stat("/")
        return sftp
    except Exception:
        return ssh.open_sftp()


def keep_alive(ssh):
    """Send SSH keepalives so a long remote build does not cost us the connection.

    A rollout opens one SFTP channel and then runs commands that take minutes -- three
    cargo builds and a podman image build. With nothing on the wire in between, the
    connection is idle for the whole of it, and the next SFTP call comes back "Socket is
    closed" from a channel that was reaped while we waited on a command.

    Thirty seconds is well inside any sensible ClientAliveInterval and costs one packet.
    """
    transport = ssh.get_transport()
    if transport is not None:
        transport.set_keepalive(30)

def explain_host_key_failure(ip, err):
    if isinstance(err, paramiko.BadHostKeyException):
        print(f"[{ip}] HOST KEY MISMATCH -- the key presented does not match known_hosts.")
        print(f"[{ip}] Refusing to push root-executed code. Investigate before retrying.")
        return True
    if isinstance(err, paramiko.SSHException) and "not found in known_hosts" in str(err):
        print(f"[{ip}] Host key is not in known_hosts, so it cannot be verified.")
        print(f"[{ip}] Add it with: ssh-keyscan -H {ip} >> ~/.ssh/known_hosts")
        print(f"[{ip}] Or, for first contact on a trusted network, re-run with HELIOS_SSH_TRUST_NEW_HOSTS=1")
        return True
    return False

nodes_env = os.environ.get("HELIOS_NODES")
if nodes_env:
    nodes = [ip.strip() for ip in nodes_env.split(",") if ip.strip()]
else:
    try:
        nodes_input = input("Enter cluster node IPs (comma separated): ").strip()
        nodes = [ip.strip() for ip in nodes_input.split(",") if ip.strip()]
    except (IOError, NameError):
        nodes = []

if not nodes:
    print("Error: No cluster node IPs specified.")
    sys.exit(1)

username = "root"

password = os.environ.get("HELIOS_PASSWORD")
if not password:
    import getpass
    try:
        password = getpass.getpass("Enter cluster root password: ").strip()
    except (IOError, NameError):
        print("Error: Password environment variable HELIOS_PASSWORD must be set in non-interactive environments.")
        sys.exit(1)

shared_cert = None
shared_key = None

# SECRET_KEY_BASE for the Phoenix console. One value for the whole cluster, because a
# session cookie signed on one node has to verify on every other -- with per-node
# secrets, Slate moving a request to a different backend logs the operator out.
#
# Read from a node that already has one rather than regenerated: rewriting it would
# invalidate every live session on every rollout.
shared_phx_secret = None

def ensure_phx_secret(ssh):
    """The cluster's SECRET_KEY_BASE, read from this node or newly generated."""
    _, stdout, _ = ssh.exec_command(
        "grep -h '^SECRET_KEY_BASE=' /etc/hci/spectrum/spectrum-phx.env 2>/dev/null "
        "| head -1 | cut -d= -f2-")
    stdout.channel.recv_exit_status()
    existing = stdout.read().decode().strip()
    if existing:
        return existing, False

    _, stdout, _ = ssh.exec_command("openssl rand -base64 48 | tr -d '[:space:]'")
    stdout.channel.recv_exit_status()
    return stdout.read().decode().strip(), True


print("=== Ensuring a single shared SSL certificate exists on Node 1 ===")
ssh_cert = new_ssh_client()
try:
    key_path = os.path.expanduser('~/.ssh/id_rsa_hci')
    if os.path.exists(key_path):
        ssh_cert.connect(nodes[0], username=username, key_filename=key_path, timeout=15)
    else:
        ssh_cert.connect(nodes[0], username=username, password=password, timeout=15)
    keep_alive(ssh_cert)
    shared_phx_secret, minted = ensure_phx_secret(ssh_cert)
    if shared_phx_secret:
        print("[Node 1] Phoenix console secret %s."
              % ("generated for this cluster" if minted else "read from this node"))
    cmd_check = "test -f /etc/hci/spectrum/certs/server.crt && test -f /etc/hci/spectrum/certs/server.key"
    stdin_chk, stdout_chk, stderr_chk = ssh_cert.exec_command(cmd_check)
    if stdout_chk.channel.recv_exit_status() != 0:
        print("[Node 1] Generating shared SSL certificate for Spectrum/Slate...")
        cmd_gen = (
            "mkdir -p /etc/hci/spectrum/certs && "
            "openssl req -x509 -nodes -newkey rsa:2048 "
            "-keyout /etc/hci/spectrum/certs/server.key "
            "-out /etc/hci/spectrum/certs/server.crt -days 3650 -subj '/CN=Spectrum'"
        )
        stdin_g, stdout_g, stderr_g = ssh_cert.exec_command(cmd_gen)
        stdout_g.channel.recv_exit_status()
    
    # Read the cert and key
    sftp_cert = ssh_cert.open_sftp()
    with sftp_cert.open("/etc/hci/spectrum/certs/server.crt", "r") as f:
        shared_cert = f.read()
    with sftp_cert.open("/etc/hci/spectrum/certs/server.key", "r") as f:
        shared_key = f.read()
    sftp_cert.close()
    print("=== Shared SSL certificate loaded successfully ===")
except Exception as e:
    if not explain_host_key_failure(nodes[0], e):
        print(f"Error ensuring shared SSL certificate: {e}")
finally:
    ssh_cert.close()

local_spark = "spark.py"
local_cluster = "cluster_new.py"
local_daemon = "spark_daemon_decoded.py"
local_helios_zk = "helios_zk.py"
# Every file the Spectrum image's Dockerfile COPYs, as (repo path, name in the build
# context). `static/` is walked separately because it is a directory.
#
# This is an inventory rather than a run of put_text_file calls because it drifted:
# the Dockerfile gained `COPY lanayru.py` and `COPY helios_sidon.py` and this list did
# not, so `podman build` failed on every rollout with "no such file or directory" -- and
# the failure was printed, stepped over, and followed by "Deployment successful", with
# spectrum restarted onto the image it was already running. The console had silently
# stopped being upgraded. test_deployment_manifest.py now keeps the two in agreement.
SPECTRUM_BUILD_FILES = (
    ("spectrum_server.py", "spectrum_server.py"),
    ("hylia.py", "hylia.py"),
    ("helios_sig.py", "helios_sig.py"),
    ("helios_schema.py", "helios_schema.py"),
    ("lanayru.py", "lanayru.py"),
    ("helios_sidon.py", "helios_sidon.py"),
)

local_helios_sig = "helios_sig.py"
local_impa = "impa.py"
local_helios_schema = "helios_schema.py"
local_saga = "saga.py"
local_bifrost = "bifrost.py"
local_valcli = "valcli.py"
local_mcli = "mcli"
local_mcli_runner = "mcli-runner"
local_allssh = "allssh"
local_dagur = "dagur.py"
local_mimir_daemon = "mimir.py"
local_vali = "vali.py"
local_catalyst = "catalyst.py"
local_catcli = "catcli"
local_gatoway = "gatoway.py"
local_urbosa = "urbosa.py"
local_logos = "logos.py"
local_mipha = "mipha.py"
local_urbosa_bootstrap = "urbosa_bootstrap.py"
local_daruk = "daruk.py"
local_yggdrasil = "hylia.py"
local_check_updates = "check_updates.py"
local_nodetool = "nodetool"

# The release public key check-updates verifies signed releases against. provision.py
# pins it when a node is built; a node built before update signing existed has none at
# all, and without one every update check fails closed. A rollout can therefore seed
# it, over the same host-key-verified session that writes root-executed code into
# /usr/local/bin -- and only ever the public half.
RELEASE_PUBKEY_REMOTE_PATH = "/etc/hci/keys/release_ed25519.pub"

release_pubkey_path = os.environ.get("HELIOS_RELEASE_PUBKEY", "").strip()
if not release_pubkey_path:
    for candidate in ("release_ed25519.pub", os.path.expanduser("~/.helios/release_ed25519.pub")):
        if os.path.exists(candidate):
            release_pubkey_path = candidate
            break

release_pubkey_pem = None
if release_pubkey_path and os.path.exists(release_pubkey_path):
    with open(release_pubkey_path, "r", encoding="utf-8", errors="ignore") as f_relkey:
        release_pubkey_pem = f_relkey.read().replace("\r\n", "\n")
    # Pointing this at the signing key instead of its public half would copy the one
    # secret the whole scheme depends on onto every node in the fleet.
    if "PRIVATE KEY" in release_pubkey_pem:
        print(f"Error: {release_pubkey_path} contains a PRIVATE key. Only the public half may be")
        print("       distributed; the signing key never leaves the release workstation.")
        sys.exit(1)
    if "BEGIN PUBLIC KEY" not in release_pubkey_pem:
        print(f"Error: {release_pubkey_path} is not a PEM public key.")
        sys.exit(1)
    print(f"=== Release public key {release_pubkey_path} will be pinned at {RELEASE_PUBKEY_REMOTE_PATH} ===")
else:
    print("=== No release public key found locally; nodes keep whatever provision.py pinned ===")
    print("    Update checks fail closed on a node with no pinned key. Set HELIOS_RELEASE_PUBKEY")
    print("    to the public half of the release signing key to pin it during this rollout.")

local_dir = "."
local_server = os.path.join(local_dir, "spectrum_server.py")
local_dockerfile = os.path.join(local_dir, "Dockerfile")
local_static_dir = os.path.join(local_dir, "static")

# Third-party image references, and the registry override that makes an air-gapped or
# mirrored site possible. Kept in step with provision.py's IMAGES catalogue -- these
# quadlet bodies are a second copy of the ones there, and a rolling update that wrote a
# different image than provisioning did would silently downgrade a service.
IMAGES = {
    "slate": "docker.io/library/traefik:v2.10",
}

REGISTRY = (os.environ.get("HELIOS_REGISTRY") or "").strip()


def resolve_image(name):
    """The image reference for a component, with any registry override applied.

    Replaces the registry host and keeps the repository path, which is what a mirror or
    pull-through cache expects. Same rule as provision.py's resolver.
    """
    reference = IMAGES[name]
    if not REGISTRY:
        return reference
    head, _, rest = reference.partition("/")
    if "." in head or ":" in head or head == "localhost":
        return REGISTRY.rstrip("/") + "/" + rest
    return REGISTRY.rstrip("/") + "/" + reference


logos_service_content = """[Unit]
Description=Logos Distributed Metrics Service
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/logos
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=100
MemoryMax=256M
MemoryHigh=200M

[Install]
WantedBy=multi-user.target
"""

gatoway_service_content = """[Unit]
Description=Gatoway L2 Network Sync Daemon
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/gatoway
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=100
MemoryMax=256M
MemoryHigh=200M

[Install]
WantedBy=multi-user.target
"""

urbosa_service_content = """[Unit]
Description=Urbosa SDN Logical Router and Overlay Orchestrator
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/urbosa
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=100
MemoryMax=256M
MemoryHigh=200M

[Install]
WantedBy=multi-user.target
"""

mipha_service_content = """[Unit]
Description=Mipha HA Cluster Monitor Daemon
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/mipha
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=100
MemoryMax=256M
MemoryHigh=200M

[Install]
WantedBy=multi-user.target
"""

yggdrasil_service_content = """[Unit]
Description=Hylia Rolling Upgrade and Life Cycle Manager
After=zookeeper.service

[Service]
Type=simple
ExecStart=/usr/local/bin/hylia
Restart=always
RestartSec=5
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=100
MemoryMax=256M
MemoryHigh=200M

[Install]
WantedBy=multi-user.target
"""

dagur_service_content = """[Unit]
Description=Dagur HA Task Scheduler Service
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/dagur
Restart=always
RestartSec=3
User=root
CPUWeight=100
MemoryMax=256M
MemoryHigh=200M

[Install]
WantedBy=multi-user.target
"""

mimir_service_content = """[Unit]
Description=Mimir Health Check and Diagnostics Daemon
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/mimir
Restart=always
RestartSec=3
User=root
CPUWeight=100
MemoryMax=256M
MemoryHigh=200M

[Install]
WantedBy=multi-user.target
"""

vali_service_content = """[Unit]
Description=Vali Audit Log and Compliance Daemon
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/vali
Restart=always
RestartSec=3
User=root
CPUWeight=100
MemoryMax=256M
MemoryHigh=200M

[Install]
WantedBy=multi-user.target
"""

catalyst_service_content = """[Unit]
Description=Catalyst API Gateway Daemon
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/catalyst
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=100
MemoryMax=256M
MemoryHigh=200M

[Install]
WantedBy=multi-user.target
"""

bifrost_service_content = """[Unit]
Description=Bifrost VM Lifecycle Management Service
After=zookeeper.service
ConditionPathExists=/etc/hci/cluster.json
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/bifrost
Restart=always
RestartSec=3
User=root
CPUWeight=100
MemoryMax=512M
MemoryHigh=400M

[Install]
WantedBy=multi-user.target
"""

daemon_service_content = """[Unit]
Description=Spark Host Management Daemon
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/spark-daemon
Restart=always
RestartSec=3
User=root
CPUWeight=200
MemoryMax=512M
MemoryHigh=400M

[Install]
WantedBy=multi-user.target
"""

daruk_service_content = """[Unit]
Description=Daruk Database Query Proxy Service
After=hydra-db.service
Requires=hydra-db.service

[Service]
Type=simple
ExecStartPre=-/usr/bin/podman exec systemd-hydra-db pkill -f daruk.py
ExecStart=/usr/bin/podman exec systemd-hydra-db python3 /var/lib/scylla/daruk.py
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=200
"""

spectrum_container_content = """[Unit]
Description=Spectrum (Prism) Web Console & Management UI
After=hydra-db.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Restart=always
CPUWeight=500
MemoryMax=1.0G
MemoryHigh=800M

[Container]
Image=localhost/spectrum:latest
Pull=never
Network=host
Volume=/etc/hci/spectrum:/etc/hci/spectrum:Z
Volume=/etc/hci:/etc/hci:ro,z
Volume=/root/.certs:/root/.certs:ro,z
Volume=/var/lib/hci/aether/volumes:/var/lib/hci/aether/volumes:rslave
PodmanArgs=--privileged
"""

slate_container_content = """[Unit]
Description=Slate (Traefik) Edge Reverse Proxy & Ingress
After=spectrum.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Restart=always
CPUWeight=200
MemoryMax=512M
MemoryHigh=400M

[Container]
Image=""" + resolve_image("slate") + """
Network=host
Volume=/etc/hci/slate:/etc/traefik:z
Volume=/etc/hci/spectrum/certs:/etc/hci/spectrum/certs:ro,z
User=root
"""


def running_guests(ssh):
    """The names of domains running on this node, newest information available.

    Empty on any failure to ask: a host that cannot answer "what is running here" is not
    evidence that nothing is, but neither is it a reason to refuse a rollout on a cluster
    with no hypervisor at all. The caller says which way it treats silence.
    """
    try:
        _, stdout, _ = ssh.exec_command("virsh list --state-running --name 2>/dev/null")
        return [n for n in stdout.read().decode("utf-8", "replace").split() if n]
    except Exception:
        return []


def refuse_if_guests_running(ip, ssh):
    """A rollout restarts the control plane under whatever is running. Say so first.

    `hylia` drains a host before a rolling upgrade. This script does not: it restarts
    fourteen services in place, and until recently spark-daemon's startup destroyed every
    local domain, so a rollout could take a guest down mid-install and report success.
    That specific defect is fixed, but the shape of the operation has not changed -- slate,
    vali, mipha and urbosa all restart underneath a running guest -- so the decision to do
    it while workloads are live belongs to whoever is running it.

    Returns True when it is safe (or explicitly allowed) to continue.
    """
    guests = running_guests(ssh)
    if not guests:
        return True
    if os.environ.get("HELIOS_ALLOW_RUNNING_GUESTS") == "1":
        print(f"[{ip}] WARNING: {len(guests)} guest(s) running ({', '.join(guests)}). "
              f"Continuing because HELIOS_ALLOW_RUNNING_GUESTS=1. Expect the control "
              f"plane to restart underneath them.")
        return True
    print(f"[{ip}] REFUSING: {len(guests)} guest(s) are running here: {', '.join(guests)}.")
    print(f"[{ip}]   This rollout restarts the control plane in place and does not drain "
          f"the host first.")
    print(f"[{ip}]   Migrate or stop them, or re-run with HELIOS_ALLOW_RUNNING_GUESTS=1 "
          f"to proceed anyway.")
    return False


def deploy_to_node(ip):
        print(f"================ Deploying to {ip} ================")
        ssh = new_ssh_client()

        try:
            key_path = os.path.expanduser('~/.ssh/id_rsa_hci')
            if os.path.exists(key_path):
                ssh.connect(ip, username=username, key_filename=key_path, timeout=15)
            else:
                ssh.connect(ip, username=username, password=password, timeout=15)
            
            keep_alive(ssh)

            # Before anything is uploaded or restarted, so a refusal costs nothing.
            if not refuse_if_guests_running(ip, ssh):
                return

            if not fast_mode:
                # 1. Clean and recreate build directory for Spectrum
                print(f"[{ip}] Preparing build directories on remote host...")
                ssh.exec_command("rm -rf /tmp/spectrum_build && mkdir -p /tmp/spectrum_build/static")
            
            sftp = ssh.open_sftp()
            
            # 1a. Copy Spark CLI
            print(f"[{ip}] Uploading spark CLI to /usr/local/bin/spark...")
            put_text_file(sftp, local_spark, "/usr/local/bin/spark")
            
            # 1b. Copy Cluster CLI
            print(f"[{ip}] Uploading cluster CLI to /usr/local/bin/cluster...")
            put_text_file(sftp, local_cluster, "/usr/local/bin/cluster")
            
            # 2. Copy Spark Daemon
            print(f"[{ip}] Uploading spark-daemon to /usr/local/bin/spark-daemon...")
            put_text_file(sftp, local_daemon, "/usr/local/bin/spark-daemon")

            # Shared ZooKeeper client, imported by spark-daemon and the cluster CLI.
            print(f"[{ip}] Uploading helios_zk to /usr/local/bin/helios_zk.py...")
            put_text_file(sftp, local_helios_zk, "/usr/local/bin/helios_zk.py")

            # Update signature verification, imported by check-updates.
            print(f"[{ip}] Uploading helios_sig to /usr/local/bin/helios_sig.py...")
            put_text_file(sftp, local_helios_sig, "/usr/local/bin/helios_sig.py")

            # Certificate lifecycle tool. Reaches peers over SSH, not mTLS, so it still
            # works once the certificates it exists to renew have expired.
            print(f"[{ip}] Uploading impa to /usr/local/bin/impa...")
            put_text_file(sftp, local_impa, "/usr/local/bin/impa")

            # Metadata backup and restore. The keyspace is the only statement of
            # which extent group holds which part of which vdisk.
            print(f"[{ip}] Uploading saga to /usr/local/bin/saga...")
            put_text_file(sftp, local_saga, "/usr/local/bin/saga")

            # The ordered schema, imported by the daemons at startup.
            print(f"[{ip}] Uploading helios_schema to /usr/local/bin/helios_schema.py...")
            put_text_file(sftp, local_helios_schema, "/usr/local/bin/helios_schema.py")

            # The storage client, imported at runtime by vali, mipha, hylia, valcli and
            # the console. It reached nodes only through provision.py's embedded copy
            # until now, so a rollout could change every daemon that calls it and leave
            # the module they call at whatever version the node was built with.
            print(f"[{ip}] Uploading helios_sidon to /usr/local/bin/helios_sidon.py...")
            put_text_file(sftp, "helios_sidon.py", "/usr/local/bin/helios_sidon.py")

            # Imported by spectrum_server at runtime, so it needs to be on the host as
            # well as inside the console image.
            print(f"[{ip}] Uploading lanayru to /usr/local/bin/lanayru.py...")
            put_text_file(sftp, "lanayru.py", "/usr/local/bin/lanayru.py")
            ssh.exec_command("chmod +x /usr/local/bin/impa")
            
            # 2a. Copy Bifrost CLI
            print(f"[{ip}] Uploading bifrost to /usr/local/bin/bifrost...")
            put_text_file(sftp, local_bifrost, "/usr/local/bin/bifrost")
            
            # 2b. Write bifrost.container Quadlet
            print(f"[{ip}] Writing bifrost.container Quadlet...")
            f_bif = sftp.open("/etc/systemd/system/bifrost.service", "w")
            f_bif.write(bifrost_service_content)
            f_bif.close()
            
            # 2ba. Write spark-daemon.container Quadlet
            print(f"[{ip}] Writing spark-daemon.container Quadlet...")
            f_sd = sftp.open("/etc/systemd/system/spark-daemon.service", "w")
            f_sd.write(daemon_service_content)
            f_sd.close()
    
            # 2c. Copy Mimir CLI
            print(f"[{ip}] Uploading mcli to /usr/local/bin/mcli...")
            put_text_file(sftp, local_mcli, "/usr/local/bin/mcli")
            
            # 2d. Copy Mimir CLI Runner
            print(f"[{ip}] Uploading mcli-runner to /usr/local/bin/mcli-runner...")
            put_text_file(sftp, local_mcli_runner, "/usr/local/bin/mcli-runner")
            
            # 2e. Copy valcli CLI
            print(f"[{ip}] Uploading valcli to /usr/local/bin/valcli...")
            put_text_file(sftp, local_valcli, "/usr/local/bin/valcli")
            
            # 2ea. Copy allssh CLI
            print(f"[{ip}] Uploading allssh to /usr/local/bin/allssh...")
            put_text_file(sftp, local_allssh, "/usr/local/bin/allssh")
            
            # 2f. Copy Dagur and Mimir daemons
            print(f"[{ip}] Uploading dagur daemon to /usr/local/bin/dagur...")
            put_text_file(sftp, local_dagur, "/usr/local/bin/dagur")
            
            print(f"[{ip}] Uploading mimir daemon to /usr/local/bin/mimir...")
            put_text_file(sftp, local_mimir_daemon, "/usr/local/bin/mimir")
            
            # 2g. Write Dagur and Mimir Quadlets
            print(f"[{ip}] Writing dagur.container Quadlet...")
            f_dag = sftp.open("/etc/systemd/system/dagur.service", "w")
            f_dag.write(dagur_service_content)
            f_dag.close()
            
            print(f"[{ip}] Writing mimir.container Quadlet...")
            f_mim = sftp.open("/etc/systemd/system/mimir.service", "w")
            f_mim.write(mimir_service_content)
            f_mim.close()
            
            # 2h. Copy vali CLI
            print(f"[{ip}] Uploading vali to /usr/local/bin/vali...")
            put_text_file(sftp, local_vali, "/usr/local/bin/vali")
            
            # 2i. Write vali Quadlet
            print(f"[{ip}] Writing vali.container Quadlet...")
            f_val = sftp.open("/etc/systemd/system/vali.service", "w")
            f_val.write(vali_service_content)
            f_val.close()
    
            # 2ib. Copy gatoway daemon
            print(f"[{ip}] Uploading gatoway daemon to /usr/local/bin/gatoway...")
            put_text_file(sftp, local_gatoway, "/usr/local/bin/gatoway")
            
            # 2ic. Write gatoway Quadlet
            print(f"[{ip}] Writing gatoway.container Quadlet...")
            f_gate = sftp.open("/etc/systemd/system/gatoway.service", "w")
            f_gate.write(gatoway_service_content)
            f_gate.close()
    
            # 2ica. Copy urbosa daemon
            print(f"[{ip}] Uploading urbosa daemon to /usr/local/bin/urbosa...")
            put_text_file(sftp, local_urbosa, "/usr/local/bin/urbosa")
            
            # 2icb. Write urbosa Quadlet
            print(f"[{ip}] Writing urbosa.container Quadlet...")
            f_urb = sftp.open("/etc/systemd/system/urbosa.service", "w")
            f_urb.write(urbosa_service_content)
            f_urb.close()
    
            # 2id. Copy logos daemon
            print(f"[{ip}] Uploading logos daemon to /usr/local/bin/logos...")
            put_text_file(sftp, local_logos, "/usr/local/bin/logos")
            
            # 2ie. Write logos Quadlet
            print(f"[{ip}] Writing logos.container Quadlet...")
            f_log = sftp.open("/etc/systemd/system/logos.service", "w")
            f_log.write(logos_service_content)
            f_log.close()
    
            # 2if. Copy mipha daemon
            print(f"[{ip}] Uploading mipha daemon to /usr/local/bin/mipha...")
            put_text_file(sftp, local_mipha, "/usr/local/bin/mipha")
            
            # 2ig. Write mipha Quadlet
            print(f"[{ip}] Writing mipha.container Quadlet...")
            f_miph = sftp.open("/etc/systemd/system/mipha.service", "w")
            f_miph.write(mipha_service_content)
            f_miph.close()
 
            # Copy hylia daemon
            print(f"[{ip}] Uploading hylia daemon to /usr/local/bin/hylia...")
            put_text_file(sftp, local_yggdrasil, "/usr/local/bin/hylia")
            
            # Copy check-updates script
            print(f"[{ip}] Uploading check-updates script to /usr/local/bin/check-updates...")
            put_text_file(sftp, local_check_updates, "/usr/local/bin/check-updates")

            # Pin the release public key. World-readable on purpose: it is public, and
            # the Spectrum container reads it through the same read-only /etc/hci mount.
            if release_pubkey_pem:
                print(f"[{ip}] Pinning release public key at {RELEASE_PUBKEY_REMOTE_PATH}...")
                stdin_key, stdout_key, stderr_key = ssh.exec_command("mkdir -p /etc/hci/keys && chmod 755 /etc/hci/keys")
                stdout_key.channel.recv_exit_status()
                f_relkey_remote = sftp.open(RELEASE_PUBKEY_REMOTE_PATH, "w")
                f_relkey_remote.write(release_pubkey_pem)
                f_relkey_remote.close()
                ssh.exec_command(f"chmod 644 {RELEASE_PUBKEY_REMOTE_PATH}")

            # Write hylia Quadlet
            print(f"[{ip}] Writing hylia.container Quadlet...")
            f_ygg = sftp.open("/etc/systemd/system/hylia.service", "w")
            f_ygg.write(yggdrasil_service_content)
            f_ygg.close()
    
            # 2j. Copy catalyst daemon
            print(f"[{ip}] Uploading catalyst daemon to /usr/local/bin/catalyst...")
            put_text_file(sftp, local_catalyst, "/usr/local/bin/catalyst")
    
            # 2ja. Copy catalyst CLI (catcli)
            print(f"[{ip}] Uploading catcli to /usr/local/bin/catcli...")
            put_text_file(sftp, local_catcli, "/usr/local/bin/catcli")
            
            # 2jb. Copy Urbosa bootstrap script
            print(f"[{ip}] Uploading urbosa-bootstrap script to /usr/local/bin/urbosa-bootstrap...")
            put_text_file(sftp, local_urbosa_bootstrap, "/usr/local/bin/urbosa-bootstrap")
            
            # 2jc. Copy nodetool host wrapper
            print(f"[{ip}] Uploading nodetool wrapper to /usr/local/bin/nodetool...")
            put_text_file(sftp, local_nodetool, "/usr/local/bin/nodetool")
            
            # 2k. Write catalyst Quadlet
            print(f"[{ip}] Writing catalyst.container Quadlet...")
            f_cat = sftp.open("/etc/systemd/system/catalyst.service", "w")
            f_cat.write(catalyst_service_content)
            f_cat.close()
            
            # 2ka. Copy Daruk Proxy and write systemd unit
            print(f"[{ip}] Uploading Daruk proxy to /usr/local/bin/daruk.py...")
            sftp.put(local_daruk, "/usr/local/bin/daruk.py")
            ssh.exec_command("mkdir -p /var/lib/hci/hydra/data && cp /usr/local/bin/daruk.py /var/lib/hci/hydra/data/daruk.py && chmod 644 /var/lib/hci/hydra/data/daruk.py || true")
            
            print(f"[{ip}] Writing daruk.service unit...")
            f_proxy = sftp.open("/etc/systemd/system/daruk.service", "w")
            f_proxy.write(daruk_service_content)
            f_proxy.close()
            
            # The satellite and controller Quadlets are removed rather than written,
            # and the containers they generated are stopped.
            #
            # Removing the .container file alone was not enough, and the gap was
            # invisible: the generated unit is gone at the next daemon-reload, but a
            # container that is already running keeps running until the host reboots.
            # A node upgraded from a DRBD cluster therefore kept a LINSTOR satellite and
            # controller alive for days after every trace of LINSTOR had been removed
            # from the tree, holding ports and answering queries that nothing asked any
            # more -- while `podman ps` showed a storage stack the cluster no longer has.
            for stale in ("aether", "linstor-controller"):
                try:
                    sftp.remove(f"/etc/containers/systemd/{stale}.container")
                    print(f"[{ip}] Removed stale {stale}.container Quadlet.")
                except IOError:
                    pass

            # `|| true` throughout: on a cluster that never had DRBD none of this exists,
            # and a rollout must not fail because it had nothing to clean up.
            _, stdout_stale, _ = ssh.exec_command(
                "systemctl stop aether linstor-controller 2>/dev/null; "
                "podman rm -f systemd-aether systemd-linstor-controller 2>/dev/null; "
                "systemctl reset-failed aether linstor-controller 2>/dev/null; true")
            stdout_stale.channel.recv_exit_status()

            print(f"[{ip}] Labelling the UEFI nvram directory for SELinux...")
            _, stdout_nv, _ = ssh.exec_command(NVRAM_SELINUX)
            stdout_nv.channel.recv_exit_status()
            for line in stdout_nv.read().decode("utf-8", "replace").splitlines():
                if line.strip():
                    print(f"[{ip}] {line.strip()}")

            print(f"[{ip}] Tearing down any leftover DRBD...")
            _, stdout_drbd, _ = ssh.exec_command(DRBD_TEARDOWN)
            stdout_drbd.channel.recv_exit_status()
            teardown_output = stdout_drbd.read().decode()
            if "LEFTOVER-LVS-BEGIN" in teardown_output:
                body = teardown_output.split("LEFTOVER-LVS-BEGIN", 1)[1]
                body = body.split("LEFTOVER-LVS-END", 1)[0]
                print(f"[{ip}] NOTE: LINSTOR left these logical volumes behind. They are "
                      f"thin and share the pool with the extent store. Nothing uses them; "
                      f"nothing will remove them either, because one may be a VM disk "
                      f"whose guest was never migrated:")
                for row in body.strip().splitlines():
                    print(f"[{ip}]   {row.strip()}")
                print(f"[{ip}]   Remove with: lvremove -y vg_aether/<name>")
            _, stdout_check, _ = ssh.exec_command("lsmod | grep -c '^drbd ' || true")
            stdout_check.channel.recv_exit_status()
            if stdout_check.read().decode().strip() not in ("0", ""):
                # Not fatal. Something still has a DRBD device open, which is a reason
                # to leave it alone rather than a reason to stop the rollout -- but it
                # is the only place anyone would find out.
                print(f"[{ip}] NOTE: the DRBD module is still loaded, so something is "
                      f"still holding a device. Run `drbdsetup status` to see what.")

            # 3a. Update spectrum.container Quadlet
            print(f"[{ip}] Writing updated spectrum.container Quadlet...")
            f_spec = sftp.open("/etc/containers/systemd/spectrum.container", "w")
            f_spec.write(spectrum_container_content)
            f_spec.close()
    
            # Update slate.container Quadlet
            print(f"[{ip}] Writing slate.container Quadlet...")
            f_slate = sftp.open("/etc/containers/systemd/slate.container", "w")
            f_slate.write(slate_container_content)
            f_slate.close()
    
            # Write Slate dynamic and static configuration files
            print(f"[{ip}] Writing Slate configuration files...")
            ssh.exec_command("mkdir -p /etc/hci/slate")
            
            local_dir_path = os.path.dirname(os.path.abspath(__file__))
            
            with open(os.path.join(local_dir_path, "slate_config", "traefik.yml"), "r", encoding="utf-8") as f_yml:
                slate_yml = f_yml.read()
            with open(os.path.join(local_dir_path, "slate_config", "dynamic.yml"), "r", encoding="utf-8") as f_dyn:
                dynamic_yml = f_dyn.read()
                
            with sftp.open("/etc/hci/slate/traefik.yml", "w") as f_rem:
                f_rem.write(slate_yml)
            with sftp.open("/etc/hci/slate/dynamic.yml", "w") as f_rem:
                f_rem.write(dynamic_yml)
    
            # Upload and load traefik.tar offline if it exists
            local_tar = os.path.join(local_dir_path, "traefik.tar")
            if os.path.exists(local_tar):
                print(f"[{ip}] Found local traefik.tar. Uploading...")
                ssh.exec_command("rm -f /tmp/traefik.tar")
                sftp.put(local_tar, "/tmp/traefik.tar")
                print(f"[{ip}] Loading Traefik image offline...")
                ssh.exec_command("podman load -i /tmp/traefik.tar && rm -f /tmp/traefik.tar")
            
            if not fast_mode:
                # 3b. Stage the Spectrum build context: the Dockerfile and every file it
                # COPYs. `spectrum_server.py` becomes `server.py` inside the image, but
                # it is staged under its own name -- the rename is the Dockerfile's job.
                print(f"[{ip}] Uploading Dockerfile for Spectrum build...")
                put_text_file(sftp, local_dockerfile, "/tmp/spectrum_build/Dockerfile")

                print(f"[{ip}] Uploading {len(SPECTRUM_BUILD_FILES)} sources for Spectrum build...")
                for source, staged in SPECTRUM_BUILD_FILES:
                    put_text_file(sftp, os.path.join(local_dir, source),
                                  "/tmp/spectrum_build/" + staged)
                
                # 3c. Upload all static assets for Spectrum build (recursively)
                print(f"[{ip}] Uploading static assets for Spectrum build...")
                for root, dirs, files in os.walk(local_static_dir):
                    for file in files:
                        local_filepath = os.path.join(root, file)
                        rel_path = os.path.relpath(local_filepath, local_static_dir).replace('\\', '/')
                        remote_filepath = f"/tmp/spectrum_build/static/{rel_path}"
                        
                        # Ensure remote parent directories exist
                        remote_parent = os.path.dirname(remote_filepath)
                        parts = remote_parent.split('/')
                        path_to_create = ""
                        for part in parts:
                            if not part:
                                continue
                            path_to_create += "/" + part
                            try:
                                sftp.mkdir(path_to_create)
                            except IOError:
                                pass
                        
                        put_text_file(sftp, local_filepath, remote_filepath)
                
                # Upload every Rust crate's sources.
                local_root = os.path.dirname(os.path.abspath(__file__))
                for crate, _binary in RUST_CRATES:
                    print(f"[{ip}] Uploading {crate} source...")
                    upload_crate(sftp, ssh, local_root, crate)
            
            # Write shared SSL certificates to ensure uniform certs across all Traefik (Slate) instances
            if shared_cert and shared_key:
                print(f"[{ip}] Writing shared SSL certificates to /etc/hci/spectrum/certs/...")
                try:
                    sftp.mkdir("/etc/hci/spectrum")
                except IOError:
                    pass
                try:
                    sftp.mkdir("/etc/hci/spectrum/certs")
                except IOError:
                    pass
                
                f_crt = sftp.open("/etc/hci/spectrum/certs/server.crt", "w")
                f_crt.write(shared_cert)
                f_crt.close()
                
                f_key = sftp.open("/etc/hci/spectrum/certs/server.key", "w")
                f_key.write(shared_key)
                f_key.close()
                
                ssh.exec_command("chmod 600 /etc/hci/spectrum/certs/server.key")
            
            sftp.close()
            
            # 4. Make executables runnable
            print(f"[{ip}] Setting executable permissions...")
            ssh.exec_command("chmod +x /usr/local/bin/spark /usr/local/bin/cluster /usr/local/bin/spark-daemon /usr/local/bin/bifrost /usr/local/bin/mcli /usr/local/bin/mcli-runner /usr/local/bin/valcli /usr/local/bin/allssh /usr/local/bin/dagur /usr/local/bin/mimir /usr/local/bin/vali /usr/local/bin/catalyst /usr/local/bin/catcli /usr/local/bin/gatoway /usr/local/bin/urbosa /usr/local/bin/logos /usr/local/bin/mipha /usr/local/bin/hylia /usr/local/bin/urbosa-bootstrap /usr/local/bin/check-updates /usr/local/bin/nodetool")
            
            # Copy spectrum files to /usr/local/bin/ for future rolling upgrades
            ssh.exec_command("mkdir -p /usr/local/bin/static && cp -rf /tmp/spectrum_build/static/* /usr/local/bin/static/ && cp -f /tmp/spectrum_build/Dockerfile /usr/local/bin/Dockerfile && cp -f /tmp/spectrum_build/spectrum_server.py /usr/local/bin/spectrum_server && chmod +x /usr/local/bin/spectrum_server")
            
            # 5. Strip [Install] and WantedBy sections from Quadlets (for zookeeper, hydra-db, spectrum)
            print(f"[{ip}] Removing auto-start dependency from other container Quadlets...")
            cmd_strip = (
                "sed -i '/\\[Install\\]/d' /etc/containers/systemd/zookeeper.container /etc/containers/systemd/hydra-db.container /etc/containers/systemd/spectrum.container || true && "
                "sed -i '/WantedBy=multi-user.target/d' /etc/containers/systemd/zookeeper.container /etc/containers/systemd/hydra-db.container /etc/containers/systemd/spectrum.container || true"
            )
            stdin, stdout, stderr = ssh.exec_command(cmd_strip)
            exit_code = stdout.channel.recv_exit_status()
            
            # 6. Reload systemd daemon to regenerate service units
            print(f"[{ip}] Reloading systemd generator configurations...")
            stdin, stdout, stderr = ssh.exec_command("systemctl daemon-reload")
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0:
                print(f"[{ip}] Error reloading systemd: {stderr.read().decode()}")
                
            # 7. Enable and restart spark-daemon and bifrost
            print(f"[{ip}] Enabling and restarting spark-daemon and bifrost...")
            stdin, stdout, stderr = ssh.exec_command("systemctl enable spark-daemon && systemctl restart spark-daemon && systemctl enable bifrost && systemctl restart bifrost")
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0:
                print(f"[{ip}] Error enabling/restarting spark-daemon and bifrost: {stderr.read().decode()}")
                
            # 8. Open the migration port range. There is no storage remount any more:
            # the extent store is one XFS filesystem mounted by fstab at boot, not two
            # DRBD devices that had to be unmounted and remounted to clear stale mount
            # points -- and which this had to skip whenever a VM was running, because
            # remounting storage under a live guest breaks it.
            ssh.exec_command(
                "firewall-cmd --permanent --add-port=49152-49215/tcp && "
                "firewall-cmd --reload || true")

            if not fast_mode:
                # Ensure clang and lld are installed on target host
                stdin_chk, stdout_chk, stderr_chk = ssh.exec_command("which clang && which lld")
                if stdout_chk.channel.recv_exit_status() != 0:
                    print(f"[{ip}] clang or lld not found. Installing clang and lld via dnf...")
                    stdin_inst, stdout_inst, stderr_inst = ssh.exec_command("dnf install -y --nogpgcheck clang lld")
                    if stdout_inst.channel.recv_exit_status() != 0:
                        print(f"[{ip}] Error installing clang/lld: {stderr_inst.read().decode()}")
                        
                # Compile WebAssembly SPICE LZ decompressor
                print(f"[{ip}] Compiling WebAssembly SPICE LZ decompressor...")
                cmd_wasm = (
                    "mkdir -p /tmp/spectrum_build/static/vendor/wasm-spice && "
                    "clang -target wasm32 -nostdlib -Wl,--no-entry -Wl,--export-all "
                    "-o /tmp/spectrum_build/static/vendor/wasm-spice/wasm_spice.wasm "
                    "/tmp/spectrum_build/static/spice-html5/src/lz_decompress.c"
                )
                stdin, stdout, stderr = ssh.exec_command(cmd_wasm)
                exit_code = stdout.channel.recv_exit_status()
                if exit_code != 0:
                    print(f"[{ip}] Error compiling WASM: {stderr.read().decode()}")
                    
                # Compile the Rust services.
                #
                # sidon is installed but not restarted here. Restarting the storage daemon
                # detaches every vdisk on the node, so it belongs in the maintenance
                # window the rolling upgrade already opens -- hylia drains the host first
                # and puts the new binary into service with the reboot. Copying it in
                # early is safe because systemd runs the inode it already opened.
                for crate, binary in RUST_CRATES:
                    print(f"[{ip}] Compiling {crate}...")
                    cmd_build = (
                        "cd /tmp/{c}_build && cargo build --release && "
                        "install -m 0755 /tmp/{c}_build/target/release/{b} /usr/local/bin/{b} && "
                        "rm -rf /tmp/{c}_build"
                    ).format(c=crate, b=binary)
                    stdin, stdout, stderr = ssh.exec_command(cmd_build)
                    exit_code = stdout.channel.recv_exit_status()
                    if exit_code != 0:
                        detail = stderr.read().decode()
                        print(f"[{ip}] Error compiling {crate}: {detail}")
                        if crate == "sidon":
                            # Every other component can limp. A node whose storage daemon
                            # did not build has the old one still running and a rollout
                            # that reported success, which is how a cluster ends up
                            # running two versions of the thing that owns the data path.
                            raise RuntimeError(
                                "sidon failed to build on %s; refusing to continue this "
                                "node's update: %s" % (ip, detail.strip()[:400]))
                
            # Deploy/update systemd service unit
            # Agahnim is a compiled Rust binary, so it is a native systemd unit -- not a
            # container. The previous heredoc here also never terminated: its EOF marker was
            # indented, which `<< 'EOF'` (unquoted delimiter position) does not match.
            sftp = live_sftp(ssh, sftp)
            f_agah = sftp.open("/etc/systemd/system/agahnim.service", "w")
            f_agah.write("""[Unit]
Description=Agahnim Console Proxy Daemon
After=network.target
ConditionPathExists=/etc/hci/cluster.json
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/agahnim 8081
Restart=always
RestartSec=3
User=root
CPUWeight=100
MemoryMax=256M

[Install]
WantedBy=multi-user.target
""")
            f_agah.close()
                
            if not fast_mode:
                # 10. Rebuild the spectrum container image locally
                # 9b. The Phoenix console, beside the Python one and never instead of
                # it: different unit, container, port and image. Slate keeps routing to
                # 8443 until slate_config/dynamic.yml says otherwise.
                print(f"[{ip}] Uploading spectrum-phx build context...")
                if upload_spectrum_phx(ssh, sftp, local_dir) != 0:
                    raise RuntimeError(
                        "the spectrum-phx build context could not be extracted on %s" % ip)

                # The env file carries the secret and is 0600. Written only when absent,
                # because rewriting SECRET_KEY_BASE logs every operator out.
                if shared_phx_secret:
                    _, stdout_env, _ = ssh.exec_command(
                        "install -d -m 0755 /etc/hci/spectrum && "
                        "test -f /etc/hci/spectrum/spectrum-phx.env || { "
                        "printf 'SECRET_KEY_BASE=%s\\nPHX_HOST=%s\\nPHX_EXTRA_ORIGINS=%s\\n' "
                        "> /etc/hci/spectrum/spectrum-phx.env && "
                        "chmod 600 /etc/hci/spectrum/spectrum-phx.env; }"
                        % (shared_phx_secret, ip, ",".join(nodes)))
                    stdout_env.channel.recv_exit_status()

                # The unit is read from the repository rather than duplicated into a
                # string here. This deployment path already keeps five hand-maintained
                # inventories in step; a sixth copy of a file that exists is not worth
                # adding.
                sftp = live_sftp(ssh, sftp)
                put_text_file(
                    sftp,
                    os.path.join(local_dir, "spectrum_phx", "quadlet", "spectrum-phx.container"),
                    "/etc/containers/systemd/spectrum-phx.container")

                print(f"[{ip}] Rebuilding spectrum-phx container image...")
                stdin, stdout, stderr = ssh.exec_command(
                    "podman build -t localhost/spectrum-phx:latest /tmp/spectrum_phx_build")
                exit_code = stdout.channel.recv_exit_status()
                if exit_code != 0:
                    raise RuntimeError(
                        "the spectrum-phx image failed to build on %s: %s"
                        % (ip, stderr.read().decode().strip()[:600]))

                print(f"[{ip}] Restarting spectrum-phx...")
                _, stdout_phx, _ = ssh.exec_command(
                    "systemctl daemon-reload && "
                    "systemctl stop spectrum-phx 2>/dev/null; "
                    "podman rm -f spectrum-phx 2>/dev/null; "
                    "systemctl start spectrum-phx")
                if stdout_phx.channel.recv_exit_status() != 0:
                    print(f"[{ip}] Warning: spectrum-phx did not start. "
                          f"`journalctl -u spectrum-phx` will say why.")

                print(f"[{ip}] Rebuilding spectrum container image...")
                stdin, stdout, stderr = ssh.exec_command("podman build -t localhost/spectrum:latest /tmp/spectrum_build")
                exit_code = stdout.channel.recv_exit_status()
                if exit_code != 0:
                    # Fatal, because the next step restarts spectrum -- onto the image
                    # that is already there. Reporting the build error and then reporting
                    # the deployment successful is how a console goes months without
                    # being upgraded while every rollout says it worked.
                    raise RuntimeError(
                        "the spectrum image failed to build on %s, so restarting it "
                        "would only reinstate the running one: %s"
                        % (ip, stderr.read().decode().strip()[:600]))
                
            # 11. Restart systemd-spectrum service
            print(f"[{ip}] Restarting spectrum service...")
            stdin, stdout, stderr = ssh.exec_command("systemctl stop spectrum && podman rm -f systemd-spectrum && systemctl start spectrum")
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0:
                print(f"[{ip}] Error restarting spectrum service: {stderr.read().decode()}")
            else:
                print(f"[{ip}] Spectrum service restarted successfully.")
                
            # 12. Restart catalyst, dagur, mimir, and vali if active to apply updates, and manage daruk/hydra-db-proxy cleanup
            print(f"[{ip}] Cleaning up old hydra-db-proxy and restarting services...")
            for cmd in [
                "systemctl stop hydra-db-proxy || true",
                "systemctl disable hydra-db-proxy || true",
                "rm -f /etc/systemd/system/hydra-db-proxy.service || true",
                "podman exec systemd-hydra-db rm -f /var/lib/scylla/cql_proxy.py || true",
                "systemctl daemon-reload",
                "systemctl is-active hydra-db && systemctl restart daruk || true",
                "systemctl enable catalyst && systemctl restart catalyst || true",
                "systemctl is-active dagur && systemctl restart dagur || true",
                "systemctl is-active mimir && systemctl restart mimir || true",
                "systemctl is-active vali && systemctl restart vali || true",
                "systemctl daemon-reload && systemctl enable agahnim && systemctl restart agahnim || true",
                # slate is a genuine Quadlet; generated units cannot be enabled (their [Install]
                # section is what the generator acts on), so reload and restart only.
                "systemctl daemon-reload && systemctl restart slate || true",
                "systemctl enable gatoway && systemctl restart gatoway || true",
                "systemctl enable urbosa && systemctl restart urbosa || true",
                "systemctl enable logos && systemctl restart logos || true",
                "systemctl enable mipha && systemctl restart mipha || true",
                "systemctl enable hylia && systemctl restart hylia || true",
                "systemctl stop helios-config-syncer || true",
                "systemctl disable helios-config-syncer || true",
                "rm -f /etc/systemd/system/helios-config-syncer.service || true",
                "rm -f /usr/local/bin/helios-config-syncer.py || true",
                "systemctl daemon-reload || true"
            ]:
                _, stdout, _ = ssh.exec_command(cmd)
                stdout.channel.recv_exit_status()
                
            print(f"[{ip}] Deployment and storage recovery successful.\n")
            
        except Exception as e:
            if not explain_host_key_failure(ip, e):
                # With the traceback. A rollout has forty-odd steps and "Socket is
                # closed" names none of them, which turns a one-line failure into a
                # bisect of the script.
                import traceback
                print(f"[{ip}] Failed to deploy: {e}")
                traceback.print_exc()
                print()
        finally:
            ssh.close()

import threading
threads = []
for ip in nodes:
    t = threading.Thread(target=deploy_to_node, args=(ip,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()
