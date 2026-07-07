import paramiko
import os
import sys

fast_mode = "--fast" in sys.argv

def put_text_file(sftp, local_path, remote_path):
    with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read().replace("\r\n", "\n")
    with sftp.open(remote_path, "wb") as f_remote:
        f_remote.write(content.encode("utf-8"))

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

print("=== Ensuring a single shared SSL certificate exists on Node 1 ===")
ssh_cert = paramiko.SSHClient()
ssh_cert.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    key_path = os.path.expanduser('~/.ssh/id_rsa_hci')
    if os.path.exists(key_path):
        ssh_cert.connect(nodes[0], username=username, key_filename=key_path, timeout=15)
    else:
        ssh_cert.connect(nodes[0], username=username, password=password, timeout=15)
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
    print(f"Error ensuring shared SSL certificate: {e}")
finally:
    ssh_cert.close()

local_spark = "spark.py"
local_cluster = "cluster_new.py"
local_daemon = "spark_daemon_decoded.py"
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

local_dir = "."
local_server = os.path.join(local_dir, "spectrum_server.py")
local_dockerfile = os.path.join(local_dir, "Dockerfile")
local_static_dir = os.path.join(local_dir, "static")

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
"""

yggdrasil_service_content = """[Unit]
Description=Hylia HA Life Cycle Management Daemon
After=zookeeper.service

[Service]
Type=simple
ExecStart=/usr/local/bin/hylia
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

dagur_service_content = """[Unit]
Description=Dagur Task Scheduler Daemon
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/dagur
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=100
MemoryMax=512M
MemoryHigh=400M
"""

mimir_service_content = """[Unit]
Description=Mimir Health Checker Daemon
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/mimir
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=100
MemoryMax=512M
MemoryHigh=400M
"""

vali_service_content = """[Unit]
Description=Vali VM Placement and DRS Daemon
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/vali
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=100
MemoryMax=512M
MemoryHigh=400M
"""

catalyst_service_content = """[Unit]
Description=Catalyst Task Management Service
After=zookeeper.service
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/catalyst
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=200
MemoryMax=512M
MemoryHigh=400M
"""

bifrost_service_content = """[Unit]
Description=Bifrost Floating VIP Manager Daemon
After=zookeeper.service
ConditionPathExists=/etc/hci/cluster.json
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Type=simple
ExecStart=/usr/local/bin/bifrost
Restart=always
RestartSec=3
Environment=PYTHONUNBUFFERED=1
CPUWeight=100
MemoryMax=256M
MemoryHigh=200M

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
User=root

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

aether_container_content = """[Unit]
Description=Linstor Satellite Container (Aether Storage Engine Backend)
After=hydra-db.service

[Service]
Restart=always
CPUWeight=500
MemoryMax=1G
MemoryHigh=900M
ExecStartPre=-/usr/bin/mkdir -p /etc/systemd/system/drbd.service.d
ExecStartPre=-/usr/bin/bash -c "printf '[Unit]\\\\nAfter=lvm2-monitor.service network-online.target\\\\nWants=lvm2-monitor.service network-online.target\\\\n' > /etc/systemd/system/drbd.service.d/override.conf"
ExecStartPre=-/usr/bin/systemctl daemon-reload

[Container]
Image=quay.io/piraeusdatastore/piraeus-server:v1.31.0
Network=host
Volume=/dev:/dev
Volume=/lib/modules:/lib/modules:ro
Volume=/run:/run
Volume=/var/lib/linstor:/var/lib/linstor:z
Volume=/etc/linstor:/etc/linstor:z
Volume=/etc/drbd.d:/var/lib/linstor.d:z
PodmanArgs=--privileged
Exec=startSatellite

[Install]
WantedBy=multi-user.target
"""

linstor_controller_content = """[Unit]
Description=Linstor Controller Container (Aether Orchestrator)
After=hydra-db.service

[Service]
Restart=always
CPUWeight=200
MemoryMax=1G
MemoryHigh=900M

[Container]
Image=quay.io/piraeusdatastore/piraeus-server:v1.31.0
Network=host
Volume=/var/lib/linstor:/var/lib/linstor:z
Volume=/etc/linstor:/etc/linstor:z
Exec=startController

[Install]
WantedBy=multi-user.target
"""

spectrum_container_content = """[Unit]
Description=Spectrum (Prism) Web Console & Management UI
After=hydra-db.service aether.service
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
Image=docker.io/library/traefik:v2.10
Network=host
Volume=/etc/hci/slate:/etc/traefik:z
Volume=/etc/hci/spectrum/certs:/etc/hci/spectrum/certs:ro,z
User=root
"""


def deploy_to_node(ip):
        print(f"================ Deploying to {ip} ================")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            key_path = os.path.expanduser('~/.ssh/id_rsa_hci')
            if os.path.exists(key_path):
                ssh.connect(ip, username=username, key_filename=key_path, timeout=15)
            else:
                ssh.connect(ip, username=username, password=password, timeout=15)
            
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
            
            # 2a. Copy Bifrost CLI
            print(f"[{ip}] Uploading bifrost to /usr/local/bin/bifrost...")
            put_text_file(sftp, local_bifrost, "/usr/local/bin/bifrost")
            
            # 2b. Write bifrost.service unit
            print(f"[{ip}] Writing bifrost.service unit...")
            f_bif = sftp.open("/etc/systemd/system/bifrost.service", "w")
            f_bif.write(bifrost_service_content)
            f_bif.close()
            
            # 2ba. Write spark-daemon.service unit
            print(f"[{ip}] Writing spark-daemon.service unit...")
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
            
            # 2g. Write Dagur and Mimir systemd units
            print(f"[{ip}] Writing dagur.service unit...")
            f_dag = sftp.open("/etc/systemd/system/dagur.service", "w")
            f_dag.write(dagur_service_content)
            f_dag.close()
            
            print(f"[{ip}] Writing mimir.service unit...")
            f_mim = sftp.open("/etc/systemd/system/mimir.service", "w")
            f_mim.write(mimir_service_content)
            f_mim.close()
            
            # 2h. Copy vali CLI
            print(f"[{ip}] Uploading vali to /usr/local/bin/vali...")
            put_text_file(sftp, local_vali, "/usr/local/bin/vali")
            
            # 2i. Write vali systemd unit
            print(f"[{ip}] Writing vali.service unit...")
            f_val = sftp.open("/etc/systemd/system/vali.service", "w")
            f_val.write(vali_service_content)
            f_val.close()
    
            # 2ib. Copy gatoway daemon
            print(f"[{ip}] Uploading gatoway daemon to /usr/local/bin/gatoway...")
            put_text_file(sftp, local_gatoway, "/usr/local/bin/gatoway")
            
            # 2ic. Write gatoway systemd unit
            print(f"[{ip}] Writing gatoway.service unit...")
            f_gate = sftp.open("/etc/systemd/system/gatoway.service", "w")
            f_gate.write(gatoway_service_content)
            f_gate.close()
    
            # 2ica. Copy urbosa daemon
            print(f"[{ip}] Uploading urbosa daemon to /usr/local/bin/urbosa...")
            put_text_file(sftp, local_urbosa, "/usr/local/bin/urbosa")
            
            # 2icb. Write urbosa systemd unit
            print(f"[{ip}] Writing urbosa.service unit...")
            f_urb = sftp.open("/etc/systemd/system/urbosa.service", "w")
            f_urb.write(urbosa_service_content)
            f_urb.close()
    
            # 2id. Copy logos daemon
            print(f"[{ip}] Uploading logos daemon to /usr/local/bin/logos...")
            put_text_file(sftp, local_logos, "/usr/local/bin/logos")
            
            # 2ie. Write logos systemd unit
            print(f"[{ip}] Writing logos.service unit...")
            f_log = sftp.open("/etc/systemd/system/logos.service", "w")
            f_log.write(logos_service_content)
            f_log.close()
    
            # 2if. Copy mipha daemon
            print(f"[{ip}] Uploading mipha daemon to /usr/local/bin/mipha...")
            put_text_file(sftp, local_mipha, "/usr/local/bin/mipha")
            
            # 2ig. Write mipha systemd unit
            print(f"[{ip}] Writing mipha.service unit...")
            f_miph = sftp.open("/etc/systemd/system/mipha.service", "w")
            f_miph.write(mipha_service_content)
            f_miph.close()

            # Copy hylia daemon
            print(f"[{ip}] Uploading hylia daemon to /usr/local/bin/hylia...")
            put_text_file(sftp, local_yggdrasil, "/usr/local/bin/hylia")
            
            # Copy check-updates script
            print(f"[{ip}] Uploading check-updates script to /usr/local/bin/check-updates...")
            put_text_file(sftp, local_check_updates, "/usr/local/bin/check-updates")
            
            # Write hylia systemd unit
            print(f"[{ip}] Writing hylia.service unit...")
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
            
            # 2k. Write catalyst systemd unit
            print(f"[{ip}] Writing catalyst.service unit...")
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
            
            # 3. Update aether.container Quadlet
            print(f"[{ip}] Writing updated aether.container Quadlet...")
            f = sftp.open("/etc/containers/systemd/aether.container", "w")
            f.write(aether_container_content)
            f.close()
            
            # Write controller on all nodes for HA control plane
            print(f"[{ip}] Writing linstor-controller.container Quadlet...")
            f_ctrl = sftp.open("/etc/containers/systemd/linstor-controller.container", "w")
            f_ctrl.write(linstor_controller_content)
            f_ctrl.close()
            
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
                # 3b. Upload Dockerfile and server.py for Spectrum build
                print(f"[{ip}] Uploading Dockerfile for Spectrum build...")
                put_text_file(sftp, local_dockerfile, "/tmp/spectrum_build/Dockerfile")
                
                print(f"[{ip}] Uploading server.py for Spectrum build...")
                put_text_file(sftp, local_server, "/tmp/spectrum_build/server.py")
                
                print(f"[{ip}] Uploading hylia.py for Spectrum build...")
                put_text_file(sftp, local_yggdrasil, "/tmp/spectrum_build/hylia.py")
                
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
                
                # Upload Agahnim proxy source files
                print(f"[{ip}] Uploading Agahnim proxy source...")
                ssh.exec_command("rm -rf /tmp/agahnim_build && mkdir -p /tmp/agahnim_build/src")
                local_agahnim_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agahnim")
                try:
                    sftp.mkdir("/tmp/agahnim_build")
                except IOError:
                    pass
                try:
                    sftp.mkdir("/tmp/agahnim_build/src")
                except IOError:
                    pass
                put_text_file(sftp, os.path.join(local_agahnim_dir, "Cargo.toml"), "/tmp/agahnim_build/Cargo.toml")
                put_text_file(sftp, os.path.join(local_agahnim_dir, "src", "main.rs"), "/tmp/agahnim_build/src/main.rs")
            
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
            ssh.exec_command("mkdir -p /usr/local/bin/static && cp -rf /tmp/spectrum_build/static/* /usr/local/bin/static/ && cp -f /tmp/spectrum_build/Dockerfile /usr/local/bin/Dockerfile && cp -f /tmp/spectrum_build/server.py /usr/local/bin/spectrum_server && chmod +x /usr/local/bin/spectrum_server")
            
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
                
            # 8. Force unmount and remount Aether volume locally to clear any stale mount points immediately, but ONLY if no VMs are running to avoid storage disruption.
            stdin, stdout, stderr = ssh.exec_command("virsh -c qemu:///system list --name | grep -v '^$'")
            running_vms = stdout.read().decode().strip()
            if running_vms:
                print(f"[{ip}] VM(s) running ({running_vms.replace(chr(10), ', ')}). Skipping Aether storage remount and restart to prevent VM storage failure.")
                # Dynamically apply CPUWeight to aether on host without restart
                ssh.exec_command("systemctl set-property aether CPUWeight=500")
            else:
                print(f"[{ip}] Remounting Aether DRBD volume and opening firewall ports...")
                cmd_remount = (
                    "umount -l /var/lib/hci/aether/volumes/default-vm-container || true; "
                    "umount -l /var/lib/hci/aether/volumes/default-image-container || true; "
                    "mkdir -p /var/lib/hci/aether/volumes/default-vm-container || true; "
                    "mountpoint -q /var/lib/hci/aether/volumes/default-vm-container || "
                    "mount -t xfs /dev/drbd/by-res/default-vm-container/0 /var/lib/hci/aether/volumes/default-vm-container || true; "
                    "mkdir -p /var/lib/hci/aether/volumes/default-image-container || true; "
                    "mountpoint -q /var/lib/hci/aether/volumes/default-image-container || "
                    "mount -t xfs /dev/drbd/by-res/default-image-container/0 /var/lib/hci/aether/volumes/default-image-container || true; "
                    "firewall-cmd --permanent --add-port=49152-49215/tcp && firewall-cmd --reload || true"
                )
                stdin, stdout, stderr = ssh.exec_command(cmd_remount)
                exit_code = stdout.channel.recv_exit_status()
                if exit_code != 0:
                    print(f"[{ip}] Error remounting Aether or configuring firewall: {stderr.read().decode()}")
                    
                # 9. Restart aether to make sure systemd registers everything cleanly
                print(f"[{ip}] Restarting aether storage service...")
                stdin, stdout, stderr = ssh.exec_command("systemctl restart aether")
                exit_code = stdout.channel.recv_exit_status()
                if exit_code != 0:
                    print(f"[{ip}] Error restarting aether: {stderr.read().decode()}")
    
                
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
                    
                # Compile Agahnim Console Proxy (Rust)
                print(f"[{ip}] Compiling Agahnim console proxy...")
                cmd_agahnim = (
                    "cd /tmp/agahnim_build && cargo build --release && "
                    "cp /tmp/agahnim_build/target/release/agahnim /usr/local/bin/agahnim && "
                    "chmod +x /usr/local/bin/agahnim && "
                    "rm -rf /tmp/agahnim_build"
                )
                stdin, stdout, stderr = ssh.exec_command(cmd_agahnim)
                exit_code = stdout.channel.recv_exit_status()
                if exit_code != 0:
                    print(f"[{ip}] Error compiling Agahnim: {stderr.read().decode()}")
                
            # Deploy/update systemd service unit
            agahnim_svc_cmd = """cat << 'EOF' > /etc/systemd/system/agahnim.service
    [Unit]
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
    EOF
    """
            stdin, stdout, stderr = ssh.exec_command(agahnim_svc_cmd)
            stdout.channel.recv_exit_status()
                
            if not fast_mode:
                # 10. Rebuild the spectrum container image locally
                print(f"[{ip}] Rebuilding spectrum container image...")
                stdin, stdout, stderr = ssh.exec_command("podman build -t localhost/spectrum:latest /tmp/spectrum_build")
                exit_code = stdout.channel.recv_exit_status()
                if exit_code != 0:
                    print(f"[{ip}] Error building spectrum container: {stderr.read().decode()}")
                
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
                "systemctl daemon-reload && systemctl enable slate && systemctl restart slate || true",
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
            print(f"[{ip}] Failed to deploy: {e}\n")
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
