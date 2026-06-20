import paramiko
import os

def put_text_file(sftp, local_path, remote_path):
    with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read().replace("\r\n", "\n")
    with sftp.open(remote_path, "w") as f_remote:
        f_remote.write(content)

nodes = ["10.10.102.220"]
username = "root"
password = "ArtPanCooking249!"

local_spark = "spark.py"
local_cluster = "cluster_new.py"
local_daemon = "spark_daemon_decoded.py"
local_bifrost = "bifrost.py"
local_valcli = "valcli.py"
local_mcli = "mcli"
local_mcli_runner = "mcli-runner"
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
ConditionPathExists=!/etc/hci/maintenance.state

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
ConditionPathExists=!/etc/hci/maintenance.state

[Service]
Restart=always
CPUWeight=500
MemoryMax=1G
MemoryHigh=900M

[Container]
Image=quay.io/piraeusdatastore/piraeus-server:v1.31.0
Network=host
Volume=/dev:/dev
Volume=/lib/modules:/lib/modules:ro
Volume=/run:/run
Volume=/var/lib/linstor:/var/lib/linstor:z
Volume=/etc/linstor:/etc/linstor:z
PodmanArgs=--privileged
Exec=startSatellite

[Install]
WantedBy=multi-user.target
"""

linstor_controller_content = """[Unit]
Description=Linstor Controller Container (Aether Orchestrator)
After=hydra-db.service
ConditionPathExists=!/etc/hci/maintenance.state

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


for ip in nodes:
    print(f"================ Deploying to {ip} ================")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(ip, username=username, password=password, timeout=15)
        
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

        # 2j. Copy catalyst daemon
        print(f"[{ip}] Uploading catalyst daemon to /usr/local/bin/catalyst...")
        put_text_file(sftp, local_catalyst, "/usr/local/bin/catalyst")

        # 2ja. Copy catalyst CLI (catcli)
        print(f"[{ip}] Uploading catcli to /usr/local/bin/catcli...")
        put_text_file(sftp, local_catcli, "/usr/local/bin/catcli")
        
        # 2jb. Copy Urbosa bootstrap script
        print(f"[{ip}] Uploading urbosa-bootstrap script to /usr/local/bin/urbosa-bootstrap...")
        put_text_file(sftp, local_urbosa_bootstrap, "/usr/local/bin/urbosa-bootstrap")
        
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
        
        # 3b. Upload Dockerfile and server.py for Spectrum build
        print(f"[{ip}] Uploading Dockerfile for Spectrum build...")
        put_text_file(sftp, local_dockerfile, "/tmp/spectrum_build/Dockerfile")
        
        print(f"[{ip}] Uploading server.py for Spectrum build...")
        put_text_file(sftp, local_server, "/tmp/spectrum_build/server.py")
        
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
        
        sftp.close()
        
        # 4. Make executables runnable
        print(f"[{ip}] Setting executable permissions...")
        ssh.exec_command("chmod +x /usr/local/bin/spark /usr/local/bin/cluster /usr/local/bin/spark-daemon /usr/local/bin/bifrost /usr/local/bin/mcli /usr/local/bin/mcli-runner /usr/local/bin/valcli /usr/local/bin/dagur /usr/local/bin/mimir /usr/local/bin/vali /usr/local/bin/catalyst /usr/local/bin/catcli /usr/local/bin/gatoway /usr/local/bin/urbosa /usr/local/bin/logos /usr/local/bin/mipha /usr/local/bin/urbosa-bootstrap")
        
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
            import json
            stdin, stdout, stderr = ssh.exec_command("cat /etc/hci/cluster.json")
            dfs_engine = "glusterfs"
            try:
                cdata = json.loads(stdout.read().decode())
                dfs_engine = cdata.get("dfs_engine", "glusterfs")
            except Exception:
                pass

            if dfs_engine == "linstor":
                print(f"[{ip}] Remounting Aether DRBD volume to clear stale mounts...")
                cmd_remount = (
                    "umount -l /var/lib/hci/aether/volumes/default-vm-container || true; "
                    "umount -l /var/lib/hci/aether/volumes/default-image-container || true; "
                    "mkdir -p /var/lib/hci/aether/volumes/default-vm-container || true; "
                    "mountpoint -q /var/lib/hci/aether/volumes/default-vm-container || "
                    "mount -t xfs /dev/drbd/by-res/default-vm-container/0 /var/lib/hci/aether/volumes/default-vm-container || true; "
                    "mkdir -p /var/lib/hci/aether/volumes/default-image-container || true; "
                    "mountpoint -q /var/lib/hci/aether/volumes/default-image-container || "
                    "mount -t xfs /dev/drbd/by-res/default-image-container/0 /var/lib/hci/aether/volumes/default-image-container || true"
                )
            else:
                print(f"[{ip}] Remounting Aether GlusterFS volume to clear stale mounts...")
                cmd_remount = (
                    "umount -l /var/lib/hci/aether/volumes/default-vm-container || true; "
                    "umount -l /var/lib/hci/aether/volumes/default-image-container || true; "
                    "podman exec systemd-aether umount -f /var/lib/hci/aether/volumes/default-vm-container || true; "
                    "podman exec systemd-aether mkdir -p /var/lib/hci/aether/volumes/default-vm-container || true; "
                    "podman exec systemd-aether mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-vm-container /var/lib/hci/aether/volumes/default-vm-container || true; "
                    "podman exec systemd-aether umount -f /var/lib/hci/aether/volumes/default-image-container || true; "
                    "podman exec systemd-aether mkdir -p /var/lib/hci/aether/volumes/default-image-container || true; "
                    "podman exec systemd-aether mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-image-container /var/lib/hci/aether/volumes/default-image-container || true"
                )
            stdin, stdout, stderr = ssh.exec_command(cmd_remount)
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0:
                print(f"[{ip}] Error remounting Aether: {stderr.read().decode()}")
                
            # 9. Restart aether to make sure systemd registers everything cleanly
            print(f"[{ip}] Restarting aether storage service...")
            stdin, stdout, stderr = ssh.exec_command("systemctl restart aether")
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0:
                print(f"[{ip}] Error restarting aether: {stderr.read().decode()}")

            
        # 10. Rebuild the spectrum container image locally
        print(f"[{ip}] Rebuilding spectrum container image...")
        stdin, stdout, stderr = ssh.exec_command("podman build -t localhost/spectrum:latest /tmp/spectrum_build")
        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            print(f"[{ip}] Error building spectrum container: {stderr.read().decode()}")
            
        # 11. Restart systemd-spectrum service
        print(f"[{ip}] Restarting spectrum service...")
        stdin, stdout, stderr = ssh.exec_command("systemctl restart spectrum")
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
            "systemctl enable gatoway && systemctl restart gatoway || true",
            "systemctl enable urbosa && systemctl restart urbosa || true",
            "systemctl enable logos && systemctl restart logos || true",
            "systemctl enable mipha && systemctl restart mipha || true"
        ]:
            _, stdout, _ = ssh.exec_command(cmd)
            stdout.channel.recv_exit_status()
            
        print(f"[{ip}] Deployment and storage recovery successful.\n")
        
    except Exception as e:
        print(f"[{ip}] Failed to deploy: {e}\n")
    finally:
        ssh.close()
