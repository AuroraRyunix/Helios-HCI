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
    "URBOSA_B64": "urbosa.py",
    "LOGOS_CLI_B64": "logos.py",
    "MIPHA_CLI_B64": "mipha.py",
    "URBOSA_BOOTSTRAP_B64": "urbosa_bootstrap.py",
    "DARUK_B64": "daruk.py",
    "BIFROST_B64": "bifrost.py",
    "MCLI_B64": "mcli",
    "MCLI_RUNNER_B64": "mcli-runner",
    "HYLIA_B64": "hylia.py"
}

print(f"Reading {provision_path}...")
with open(provision_path, "r", encoding="utf-8") as f:
    content = f.read()

# Base64-encode files and replace their declarations
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

    content, count = re.subn(pattern, replacement, content, count=1, flags=re.DOTALL)
    if count == 0:
        print(f"Error: Could not find definition of {var_name} in provision.py!")
    else:
        print(f"Successfully updated {var_name} ({count} replacement)")

# Write back to provision.py
print(f"Writing updated content back to {provision_path}...")
with open(provision_path, "w", encoding="utf-8", newline="\n") as f:
    f.write(content)

print("Synchronization complete!")
