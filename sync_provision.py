import re
import base64
import os
import sys

# Resolve everything against this script's own directory: run from anywhere else and
# a CWD-relative "provision.py" would silently sync nothing (or the wrong tree).
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
provision_path = os.path.join(SCRIPT_DIR, "provision.py")

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
    "NODETOOL_B64": "nodetool",
    "ALLSSH_CLI_B64": "allssh",
    "CHECK_UPDATES_B64": "check_updates.py",
    "HYLIA_B64": "hylia.py",
    "LANAYRU_B64": "lanayru.py",
    "HELIOS_ZK_B64": "helios_zk.py",
    "HELIOS_SIG_B64": "helios_sig.py",
    "HELIOS_SIDON_B64": "helios_sidon.py",
    "HELIOS_CQL_B64": "helios_cql.py",
    "IMPA_B64": "impa.py",
    "HELIOS_SCHEMA_B64": "helios_schema.py",
    "SAGA_B64": "saga.py"
}

# Any constant provision.py embeds must be listed above, otherwise editing its source
# file and running this script silently ships the previously embedded copy.
B64_CONSTANT_RE = re.compile(r'^([A-Z_]+_B64)\s*=', re.MULTILINE)

errors = []

print(f"Reading {provision_path}...")
with open(provision_path, "r", encoding="utf-8") as f:
    content = f.read()

# Base64-encode files and replace their declarations
for var_name, file_name in mapping.items():
    file_path = os.path.join(SCRIPT_DIR, file_name)
    if not os.path.exists(file_path):
        errors.append(f"Source file {file_name} not found, so {var_name} could not be re-embedded.")
        print(f"Error: File {file_path} not found. Skipping...")
        continue

    print(f"Encoding {file_name} into {var_name}...")
    with open(file_path, "rb") as f:
        file_bytes = f.read()

    # Normalize CRLF -> LF before embedding. These payloads are decoded straight into
    # /usr/local/bin on Linux hosts, so a CRLF working tree on the workstation (which is
    # exactly what git core.autocrlf=true produces on a Windows checkout) would ship
    # scripts whose shebang reads `#!/usr/bin/env python3\r`, failing at exec with
    # "/usr/bin/env: 'python3\r': No such file or directory". .gitattributes pins these
    # files to LF as the primary defence; this is the backstop for a working tree that
    # predates it or was created by other tooling.
    normalized = file_bytes.replace(b"\r\n", b"\n")
    if normalized != file_bytes:
        print(f"  Normalized CRLF line endings in {file_name} before embedding.")
    file_bytes = normalized

    b64_str = base64.b64encode(file_bytes).decode("utf-8")

    # Replace the variable definition in provision.py
    pattern = rf'^{var_name}\s*=\s*".*?"'
    replacement = f'{var_name} = "{b64_str}"'

    content, count = re.subn(pattern, replacement, content, count=1, flags=re.DOTALL | re.MULTILINE)
    if count == 0:
        errors.append(f"Could not find the definition of {var_name} in provision.py; {file_name} was not embedded.")
        print(f"Error: Could not find definition of {var_name} in provision.py!")
    else:
        print(f"Successfully updated {var_name} ({count} replacement)")

# Detect drift: every *_B64 constant provision.py declares must be covered above.
declared = set(B64_CONSTANT_RE.findall(content))
unmapped = sorted(declared - set(mapping))
if unmapped:
    for var_name in unmapped:
        print(f"Error: provision.py declares {var_name} but the mapping does not cover it.")
    errors.append(f"Unmapped provision.py constants: {', '.join(unmapped)}. Add them to 'mapping' so their source files are embedded.")

stale = sorted(set(mapping) - declared)
if stale:
    for var_name in stale:
        print(f"Error: mapping references {var_name}, which provision.py no longer declares.")
    errors.append(f"Stale mapping entries: {', '.join(stale)}.")

if errors:
    print()
    print("Synchronization ABORTED; provision.py was left unchanged:")
    for err in errors:
        print(f"  - {err}")
    sys.exit(1)

# Write back to provision.py
print(f"Writing updated content back to {provision_path}...")
with open(provision_path, "w", encoding="utf-8", newline="\n") as f:
    f.write(content)

print(f"Synchronization complete! ({len(mapping)} constants embedded)")
