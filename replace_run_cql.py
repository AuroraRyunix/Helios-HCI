import os
import re

new_run_cql = r"""def run_cql_query(cql_query, *args, **kwargs):
    import urllib.request
    import json
    try:
        url = "http://127.0.0.1:9043/query"
        req = urllib.request.Request(
            url,
            data=cql_query.encode('utf-8'),
            headers={'Content-Type': 'text/plain'}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            res = json.loads(response.read().decode('utf-8'))
            if res.get("status") == "success":
                lines = []
                for row in res.get("rows", []):
                    if isinstance(row, dict):
                        if "json" in row:
                            lines.append(row["json"])
                        else:
                            vals = [str(v) for v in row.values()]
                            lines.append(" ".join(vals))
                    else:
                        lines.append(str(row))
                return 0, "\n".join(lines), ""
            else:
                return 1, "", res.get("error", "Database query execution error")
    except Exception as e:
        import base64
        import subprocess
        b64_query = base64.b64encode(cql_query.encode('utf-8')).decode('utf-8')
        local_ip = "127.0.0.1"
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('10.255.255.255', 1))
            local_ip = s.getsockname()[0]
            s.close()
        except Exception:
            pass
        cmd = f'echo {b64_query} | base64 -d | podman exec -i systemd-hydra-db cqlsh {local_ip}'
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        return p.returncode, stdout.decode('utf-8', errors='ignore').strip(), stderr.decode('utf-8', errors='ignore').strip()"""

static_dir = r"C:\Users\AuraFlight\Desktop\container-hci"
files_to_update = [
    "catalyst.py",
    "cluster_new.py",
    "dagur.py",
    "gatoway.py",
    "logos.py",
    "mimir.py",
    "mipha.py",
    "spark_daemon_decoded.py",
    "spectrum_server.py",
    "urbosa.py",
    "urbosa_bootstrap.py",
    "valcli.py",
    "vali.py"
]

# Correct pattern that does NOT consume the newline
pattern = r"def run_cql_query\b.*?(?=\n(?:def |class |if __name__)|\Z)"

for filename in files_to_update:
    filepath = os.path.join(static_dir, filename)
    if not os.path.exists(filepath):
        print(f"Skipping {filename} (not found)")
        continue
        
    print(f"Updating {filename}...")
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
        
    # First, fix any joined definitions from the previous run
    content = content.replace("strip()def ", "strip()\ndef ")
    content = content.replace("strip()class ", "strip()\nclass ")
    content = content.replace("strip()if __name__", "strip()\nif __name__")
        
    new_content, count = re.subn(pattern, lambda m: new_run_cql, content, flags=re.DOTALL)
    if count > 0:
        with open(filepath, "w", encoding="utf-8", newline="\n") as f:
            f.write(new_content)
        print(f"  Successfully replaced run_cql_query in {filename}")
    else:
        print(f"  Warning: Pattern not matched in {filename}")

print("Replacement done!")
