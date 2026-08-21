"""Throwaway: deploy saga through the toolkit's inventory lists and seed its schedule."""
import io
import re

# 1. sync_provision mapping
p = "sync_provision.py"
s = io.open(p, encoding="utf-8").read()
old = '    "HELIOS_SCHEMA_B64": "helios_schema.py"\n'
assert s.count(old) == 1
io.open(p, "w", encoding="utf-8", newline="\n").write(
    s.replace(old, '    "HELIOS_SCHEMA_B64": "helios_schema.py",\n    "SAGA_B64": "saga.py"\n', 1))
print("sync_provision: SAGA_B64 mapped")

# 2. provision.py constant + deploy
p = "provision.py"
s = io.open(p, encoding="utf-8").read()
if not re.search(r"^SAGA_B64 = ", s, re.M):
    m = re.search(r"^HELIOS_SCHEMA_B64 = ", s, re.M)
    s = s[:m.start()] + 'SAGA_B64 = ""\n' + s[m.start():]
    print("provision: SAGA_B64 declared")

old = '            node.execute("chmod +x /usr/local/bin/impa")\n'
assert s.count(old) == 1, "impa deploy anchor"
new = old + (
    "\n"
    "            # Metadata backup and restore. The keyspace is the only statement of which\n"
    "            # DRBD volume belongs to which VM -- the volumes survive a node loss and\n"
    "            # mean nothing without it.\n"
    '            node.write_file("/usr/local/bin/saga", base64.b64decode(SAGA_B64).decode(\'utf-8\'))\n'
    '            node.execute("chmod +x /usr/local/bin/saga")\n')
s = s.replace(old, new, 1)
io.open(p, "w", encoding="utf-8", newline="\n").write(s)
print("provision: saga deployed to nodes")

# 3. create_upgrade_zip components_map
p = "create_upgrade_zip.py"
s = io.open(p, encoding="utf-8").read()
old = '    "impa": {"src": "impa.py", "target": "/usr/local/bin/impa"},\n'
assert s.count(old) == 1
io.open(p, "w", encoding="utf-8", newline="\n").write(
    s.replace(old, old + '    "saga": {"src": "saga.py", "target": "/usr/local/bin/saga"},\n', 1))
print("create_upgrade_zip: saga packaged")

# 4. check_updates components_paths
p = "check_updates.py"
s = io.open(p, encoding="utf-8").read()
old = '            "impa": "/usr/local/bin/impa",\n'
assert s.count(old) == 1
io.open(p, "w", encoding="utf-8", newline="\n").write(
    s.replace(old, old + '            "saga": "/usr/local/bin/saga",\n', 1))
print("check_updates: saga inventoried")

# 5. deploy_updates upload
p = "deploy_updates.py"
s = io.open(p, encoding="utf-8").read()
old = 'local_helios_schema = "helios_schema.py"\n'
assert s.count(old) == 1
s = s.replace(old, old + 'local_saga = "saga.py"\n', 1)
old2 = ('            print(f"[{ip}] Uploading impa to /usr/local/bin/impa...")\n'
        '            put_text_file(sftp, local_impa, "/usr/local/bin/impa")\n'
        '            ssh.exec_command("chmod +x /usr/local/bin/impa")\n')
assert s.count(old2) == 1
s = s.replace(old2, old2 + (
    '\n'
    '            print(f"[{ip}] Uploading saga to /usr/local/bin/saga...")\n'
    '            put_text_file(sftp, local_saga, "/usr/local/bin/saga")\n'
    '            ssh.exec_command("chmod +x /usr/local/bin/saga")\n'), 1)
io.open(p, "w", encoding="utf-8", newline="\n").write(s)
print("deploy_updates: saga uploaded")

# 6. spectrum_server seeds the nightly schedule
p = "spectrum_server.py"
s = io.open(p, encoding="utf-8").read()
anchor = "    insert_orphaned_disks_cleanup = "
i = s.find(anchor)
assert i != -1, "orphaned disks seed anchor"
# insert a new seed literal immediately before it
seed = '''    # Enabled on a fresh cluster even though no backup target is configured yet, so it
    # fails nightly with a message naming the fix. A cluster with no backups should say so
    # once a day; a disabled schedule is silent, which is what "no backup/DR" looked like.
    insert_metadata_backup = """
    INSERT INTO hydra.dagur_schedules (job_name, task_type, cron_expression, interval_seconds, enabled, last_run_epoch, command)
    VALUES ('metadata_backup', 'backup', '30 1 * * *', 86400, true, 0, '/usr/local/bin/saga backup --all-nodes') IF NOT EXISTS;
    """

'''
s = s[:i] + seed + s[i:]

run_anchor = "                run_cql_query(insert_orphaned_disks_cleanup)\n"
assert s.count(run_anchor) == 1, "orphaned disks run anchor"
s = s.replace(run_anchor, run_anchor + "                run_cql_query(insert_metadata_backup)\n", 1)
io.open(p, "w", encoding="utf-8", newline="\n").write(s)
print("spectrum_server: metadata_backup schedule seeded")
