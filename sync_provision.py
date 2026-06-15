import re
import base64
import os

provision_path = "provision.py"

mapping = {
    "CATCLI_B64": "catcli",
    "CATALYST_CLI_B64": "catalyst.py",
    "VALI_CLI_B64": "vali.py",
    "VALCLI_CLI_B64": "valcli.py",
    "DAGUR_CLI_B64": "dagur.py",
    "MIMIR_CLI_B64": "mimir.py",
    "CLUSTER_CLI_B64": "cluster_new.py",
    "SPARK_CLI_B64": "spark.py",
    "SPARK_DAEMON_B64": "spark_daemon_decoded.py",
    "SPECTRUM_SERVER_B64": "spectrum_server.py",
    "SPECTRUM_DOCKERFILE_B64": "Dockerfile",
    "GATOWAY_B64": "gatoway.py",
    "LOGOS_CLI_B64": "logos.py"
}

print(f"Reading {provision_path}...")
with open(provision_path, "r", encoding="utf-8") as f:
    content = f.read()

# 1. Inject declarations at the top if missing
if "GATOWAY_B64 = " not in content:
    print("Injecting GATOWAY_B64 declaration...")
    # Insert right after QUADLETS = { ... } (or right before CATCLI_B64)
    content = content.replace('CATCLI_B64 = "', 'GATOWAY_B64 = ""\n\nCATCLI_B64 = "', 1)

if "VALCLI_CLI_B64 = " not in content:
    print("Injecting VALCLI_CLI_B64 declaration...")
    content = content.replace('VALI_CLI_B64 = "', 'VALCLI_CLI_B64 = ""\n\nVALI_CLI_B64 = "', 1)

if "LOGOS_CLI_B64 = " not in content:
    print("Injecting LOGOS_CLI_B64 declaration...")
    content = content.replace('VALCLI_CLI_B64 = "', 'LOGOS_CLI_B64 = ""\n\nVALCLI_CLI_B64 = "', 1)

# 2. Inject deploy logic if missing
if "Deploy Gatoway Daemon" not in content:
    print("Injecting Gatoway deployment code...")
    target_pattern = 'node.write_file("/etc/systemd/system/vali.service", vali_svc)'
    replacement = (
        'node.write_file("/etc/systemd/system/vali.service", vali_svc)\n\n'
        '            # Deploy Gatoway Daemon\n'
        '            gatoway_cli = base64.b64decode(GATOWAY_B64).decode(\'utf-8\')\n'
        '            node.write_file("/usr/local/bin/gatoway", gatoway_cli)\n'
        '            node.execute("chmod +x /usr/local/bin/gatoway")\n\n'
        '            gatoway_svc = """[Unit]\n'
        'Description=Gatoway L2 Network Sync Daemon\n'
        'After=zookeeper.service\n\n'
        '[Service]\n'
        'Type=simple\n'
        'ExecStart=/usr/local/bin/gatoway\n'
        'Restart=always\n'
        'RestartSec=3\n'
        'User=root\n'
        'Environment=PYTHONUNBUFFERED=1\n'
        'CPUWeight=50\n'
        'MemoryMax=256M\n'
        'MemoryHigh=200M\n'
        '"""\n'
        '            node.write_file("/etc/systemd/system/gatoway.service", gatoway_svc)'
    )
    content = content.replace(target_pattern, replacement, 1)

if "Deploy Logos Daemon" not in content:
    print("Injecting Logos deployment code...")
    target_pattern = 'node.write_file("/etc/systemd/system/gatoway.service", gatoway_svc)'
    replacement = (
        'node.write_file("/etc/systemd/system/gatoway.service", gatoway_svc)\n\n'
        '            # Deploy Logos Daemon\n'
        '            logos_cli = base64.b64decode(LOGOS_CLI_B64).decode(\'utf-8\')\n'
        '            node.write_file("/usr/local/bin/logos", logos_cli)\n'
        '            node.execute("chmod +x /usr/local/bin/logos")\n\n'
        '            logos_svc = """[Unit]\n'
        'Description=Logos Distributed Metrics Service\n'
        'After=zookeeper.service\n\n'
        '[Service]\n'
        'Type=simple\n'
        'ExecStart=/usr/local/bin/logos\n'
        'Restart=always\n'
        'RestartSec=3\n'
        'User=root\n'
        'Environment=PYTHONUNBUFFERED=1\n'
        'CPUWeight=50\n'
        'MemoryMax=256M\n'
        'MemoryHigh=200M\n'
        '"""\n'
        '            node.write_file("/etc/systemd/system/logos.service", logos_svc)'
    )
    content = content.replace(target_pattern, replacement, 1)

if "Deploy valcli CLI" not in content:
    print("Injecting valcli deployment code...")
    target_pattern = (
        'node.write_file("/usr/local/bin/vali", vali_cli)\n'
        '            node.execute("chmod +x /usr/local/bin/vali")'
    )
    replacement = (
        'node.write_file("/usr/local/bin/vali", vali_cli)\n'
        '            node.execute("chmod +x /usr/local/bin/vali")\n\n'
        '            # Deploy valcli CLI\n'
        '            valcli_cli = base64.b64decode(VALCLI_CLI_B64).decode(\'utf-8\')\n'
        '            node.write_file("/usr/local/bin/valcli", valcli_cli)\n'
        '            node.execute("chmod +x /usr/local/bin/valcli")'
    )
    content = content.replace(target_pattern, replacement, 1)

# 3. Base64-encode files and replace their declarations
for var_name, file_path in mapping.items():
    if not os.path.exists(file_path):
        print(f"Warning: File {file_path} not found. Skipping...")
        continue
    
    print(f"Encoding {file_path} into {var_name}...")
    with open(file_path, "rb") as f:
        file_bytes = f.read()
    
    b64_str = base64.b64encode(file_bytes).decode("utf-8")
    
    # Replace the variable definition in provision.py
    pattern = rf'{var_name}\s*=\s*".*?"'
    replacement = f'{var_name} = "{b64_str}"'
    
    # We use re.sub with re.DOTALL in case the pattern matches across lines (it shouldn't but just in case)
    content, count = re.subn(pattern, replacement, content, count=1, flags=re.DOTALL)
    if count == 0:
        print(f"Error: Could not find definition of {var_name} in provision.py!")
    else:
        print(f"Successfully updated {var_name} ({count} replacement)")

# 4. Write back to provision.py
print(f"Writing updated content back to {provision_path}...")
with open(provision_path, "w", encoding="utf-8", newline="\n") as f:
    f.write(content)

print("Synchronization complete!")
