import paramiko
import os

def put_text_file(sftp, local_path, remote_path):
    with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read().replace("\r\n", "\n")
    with sftp.open(remote_path, "w") as f_remote:
        f_remote.write(content)

nodes = ["10.10.102.220", "10.10.102.222", "10.10.102.223"]
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
local_logos = "logos.py"
local_mipha = "mipha.py"

local_dir = "."
local_server = os.path.join(local_dir, "spectrum_server.py")
local_dockerfile = os.path.join(local_dir, "Dockerfile")
local_static_dir = os.path.join(local_dir, "static")

logos_service_content = """[Unit]
Description=Logos Distributed Metrics Service
After=zookeeper.service

[Service]
Type=simple
ExecStart=/usr/local/bin/logos
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=50
MemoryMax=256M
MemoryHigh=200M
"""

gatoway_service_content = """[Unit]
Description=Gatoway L2 Network Sync Daemon
After=zookeeper.service

[Service]
Type=simple
ExecStart=/usr/local/bin/gatoway
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=50
MemoryMax=256M
MemoryHigh=200M
"""

mipha_service_content = """[Unit]
Description=Mipha HA Cluster Monitor Daemon
After=zookeeper.service

[Service]
Type=simple
ExecStart=/usr/local/bin/mipha
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=50
MemoryMax=256M
MemoryHigh=200M
"""

dagur_service_content = """[Unit]
Description=Dagur Task Scheduler Daemon
After=zookeeper.service

[Service]
Type=simple
ExecStart=/usr/local/bin/dagur
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=50
MemoryMax=512M
MemoryHigh=400M
"""

mimir_service_content = """[Unit]
Description=Mimir Health Checker Daemon
After=zookeeper.service

[Service]
Type=simple
ExecStart=/usr/local/bin/mimir
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=50
MemoryMax=512M
MemoryHigh=400M
"""

vali_service_content = """[Unit]
Description=Vali VM Placement and DRS Daemon
After=zookeeper.service

[Service]
Type=simple
ExecStart=/usr/local/bin/vali
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=50
MemoryMax=512M
MemoryHigh=400M
"""

catalyst_service_content = """[Unit]
Description=Catalyst Task Management Service
After=zookeeper.service

[Service]
Type=simple
ExecStart=/usr/local/bin/catalyst
Restart=always
RestartSec=3
User=root
Environment=PYTHONUNBUFFERED=1
CPUWeight=50
MemoryMax=512M
MemoryHigh=400M
"""

bifrost_service_content = """[Unit]
Description=Bifrost Floating VIP Manager Daemon
After=zookeeper.service

[Service]
Type=simple
ExecStart=/usr/local/bin/bifrost
Restart=always
RestartSec=3
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

aether_container_content = """[Unit]
Description=Aether Storage Engine (GlusterFS & NFS-Ganesha)
After=hydra-db.service

[Service]
Restart=always
ExecStartPost=/usr/bin/sleep 5
ExecStartPost=/usr/bin/podman exec systemd-aether sh -c "umount -f /var/lib/hci/aether/volumes/default-vm-container || true"
ExecStartPost=/usr/bin/podman exec systemd-aether sh -c "mkdir -p /var/lib/hci/aether/volumes/default-vm-container || true"
ExecStartPost=/usr/bin/podman exec systemd-aether sh -c "mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-vm-container /var/lib/hci/aether/volumes/default-vm-container || true"
ExecStartPost=/usr/bin/podman exec systemd-aether sh -c "umount -f /var/lib/hci/aether/volumes/default-image-container || true"
ExecStartPost=/usr/bin/podman exec systemd-aether sh -c "mkdir -p /var/lib/hci/aether/volumes/default-image-container || true"
ExecStartPost=/usr/bin/podman exec systemd-aether sh -c "mount -t glusterfs -o direct-io-mode=disable,attribute-timeout=10,entry-timeout=10 localhost:/default-image-container /var/lib/hci/aether/volumes/default-image-container || true"
CPUWeight=80
MemoryMax=1.5G
MemoryHigh=1.2G

[Container]
Image=docker.io/gluster/gluster-centos:latest
Network=host
Volume=/var/lib/hci/aether/data:/var/lib/glusterd:Z
Volume=/var/lib/hci/aether/bricks:/var/lib/hci/aether/bricks:Z,rslave
Volume=/etc/hci/aether:/etc/hci/aether:ro
Volume=/var/lib/hci/aether/volumes:/var/lib/hci/aether/volumes:shared
PodmanArgs=--privileged
Exec=glusterd --no-daemon --log-level=INFO
"""

spectrum_container_content = """[Unit]
Description=Spectrum (Prism) Web Console & Management UI
After=hydra-db.service aether.service

[Service]
Restart=always
CPUWeight=50
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
        
        # 2k. Write catalyst systemd unit
        print(f"[{ip}] Writing catalyst.service unit...")
        f_cat = sftp.open("/etc/systemd/system/catalyst.service", "w")
        f_cat.write(catalyst_service_content)
        f_cat.close()
        
        # 3. Update aether.container Quadlet
        print(f"[{ip}] Writing updated aether.container Quadlet...")
        f = sftp.open("/etc/containers/systemd/aether.container", "w")
        f.write(aether_container_content)
        f.close()
        
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
        ssh.exec_command("chmod +x /usr/local/bin/spark /usr/local/bin/cluster /usr/local/bin/spark-daemon /usr/local/bin/bifrost /usr/local/bin/mcli /usr/local/bin/mcli-runner /usr/local/bin/valcli /usr/local/bin/dagur /usr/local/bin/mimir /usr/local/bin/vali /usr/local/bin/catalyst /usr/local/bin/catcli /usr/local/bin/gatoway /usr/local/bin/logos /usr/local/bin/mipha")
        
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
            
        # 7. Restart spark-daemon
        print(f"[{ip}] Restarting spark-daemon...")
        stdin, stdout, stderr = ssh.exec_command("systemctl restart spark-daemon")
        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            print(f"[{ip}] Error restarting spark-daemon: {stderr.read().decode()}")
            
        # 8. Force unmount and remount Aether volume locally to clear any stale mount points immediately
        print(f"[{ip}] Remounting Aether volume to clear stale mounts...")
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
            
        # 12. Restart catalyst, dagur, mimir, and vali if active to apply updates
        print(f"[{ip}] Restarting catalyst, dagur, mimir, vali, and gatoway if active...")
        for cmd in [
            "systemctl enable catalyst && systemctl restart catalyst || true",
            "systemctl is-active dagur && systemctl restart dagur || true",
            "systemctl is-active mimir && systemctl restart mimir || true",
            "systemctl is-active vali && systemctl restart vali || true",
            "systemctl enable gatoway && systemctl restart gatoway || true",
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
