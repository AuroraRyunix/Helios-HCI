#!/usr/bin/env python3
"""Saga -- backup and restore of the metadata a Helios cluster cannot rebuild.

The DRBD volumes under a VM are replicated and survive a host loss. What does not
survive is the record of *which* volume is which: `hydra.vms` says a guest exists, how
much memory it has, which host it runs on, which network it is on, and
`hydra.vm_nvram` holds its UEFI variables. Lose the keyspace and the cluster still has
every byte of guest data and no way to say what any of it is.

So this tool backs up the things that are authoritative and cannot be derived:

  * the `hydra` keyspace -- via `nodetool snapshot`, the standard Cassandra/Scylla
    mechanism, which hardlinks a consistent per-node set of SSTables;
  * the LINSTOR controller database -- via `linstor controller backupdb`, which is the
    only thing that knows a DRBD resource's port, minor, node-id and placement;
  * `/etc/hci` -- `cluster.json` above all, because it names the nodes and holds the
    redundancy factor and the VIP, and because you need it before you can reach Hydra
    at all;
  * optionally the cluster CA, which exists on exactly one host and nowhere else.

It deliberately does NOT back up ZooKeeper. `/helios/nodes/<ip>` is ephemeral by
construction -- it is republished within seconds of a node starting -- and
`/cluster_state` is one word an operator retypes with `cluster start`. Capturing them
would imply the tree is a system of record, and restoring a stale `stopped` into a
cluster you are trying to bring up would actively hurt.

It also does NOT back up guest data. See docs/backup_restore.md; the word "backup"
here means cluster metadata and nothing else.

Naming: Saga is the Norse goddess of record and recollection, and a saga is the written
account of what happened. Nutanix's analog is Cerebro.

Everything is stdlib. Run `saga --help`.
"""

import argparse
import errno
import hashlib
import json
import os
import re
import shutil
import socket
import ssl
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request

# ---------------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------------

DEFAULT_KEYSPACE = "hydra"
SCYLLA_CONTAINER = "systemd-hydra-db"
LINSTOR_CONTAINER = "systemd-linstor-controller"
NODETOOL_WRAPPER = "/usr/local/bin/nodetool"

CLUSTER_JSON = "/etc/hci/cluster.json"
ETC_HCI = "/etc/hci"
CERTS_STAGING = "/var/lib/hci/certs_staging"
LINSTOR_DIR = "/var/lib/linstor"

# Settings live in hydra.cluster_settings beside dns_servers / urbosa_enabled, so the
# scheduled job needs no new table and no schema migration.
SETTING_TARGET = "saga_target"
SETTING_KEEP = "saga_keep"
SETTING_KEEP_DAYS = "saga_keep_days"

DEFAULT_KEEP = 7
DEFAULT_KEEP_DAYS = 30

# A snapshot this tool took. `nodetool clearsnapshot -t` matches on the tag, so the
# prefix is what keeps `saga snapshots --prune` from deleting a snapshot somebody else
# is relying on -- including Scylla's own `pre-drop-*` auto-snapshots, which are the
# last copy of a table somebody just dropped.
SNAPSHOT_PREFIX = "saga-"

ARCHIVE_SUFFIX = ".tar.gz"
MANIFEST_SUFFIX = ".tar.gz.manifest.json"
PARTIAL_SUFFIX = ".tar.gz.partial"

MANIFEST_VERSION = 1

# A file under /etc/hci larger than this is not configuration; something has gone wrong
# (a core dump, a log, an image someone parked there) and silently swallowing it into
# every nightly backup is how a metadata artefact becomes a gigabyte.
ETC_FILE_MAX_BYTES = 8 * 1024 * 1024

# Files Scylla puts inside a snapshot directory that are not SSTable components.
# `nodetool refresh` ignores them, so staging them into upload/ leaves litter that
# looks exactly like a refresh that failed half way.
NON_SSTABLE_SNAPSHOT_FILES = frozenset(("manifest.json", "schema.cql"))

# An interrupted run leaves a .partial. Younger ones may belong to a backup still in
# flight on another node, so only clearly abandoned ones are swept.
PARTIAL_MAX_AGE_SECONDS = 24 * 3600

STAMP_FORMAT = "%Y%m%dT%H%M%SZ"

_ARTEFACT_RE = re.compile(
    r"\Asaga-(?P<cluster>[A-Za-z0-9_]+)-(?P<node>[A-Za-z0-9_]+)-"
    r"(?P<stamp>[0-9]{8}T[0-9]{6}Z)\.tar\.gz\Z")


class SagaError(RuntimeError):
    """Anything that should stop the run and be reported as a failure."""


class TargetUnusable(SagaError):
    """The backup destination is missing, unwritable, or the wrong disk."""


class RestoreRefused(SagaError):
    """A restore preflight said no. Never downgraded to a warning without --force."""


# ---------------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------------

def utc_stamp(when=None):
    return time.strftime(STAMP_FORMAT, time.gmtime(when if when is not None else time.time()))


def parse_stamp(stamp):
    """Epoch seconds for a stamp produced by utc_stamp(). UTC, not local time.

    calendar.timegm rather than time.mktime: mktime interprets the struct as local
    time, which would make every retention decision wrong by the host's UTC offset --
    and wrong by a different amount either side of a DST boundary.
    """
    import calendar

    return calendar.timegm(time.strptime(stamp, STAMP_FORMAT))


def sanitize(value):
    """Reduce a name to the character class the artefact filename parser accepts.

    Cluster names carry hyphens ("hci-01") and node addresses carry dots, and both are
    the field separators in the filename. Folding them to underscores is what makes
    `parse_artefact_name` unambiguous rather than a guess.
    """
    return re.sub(r"[^A-Za-z0-9_]", "_", str(value)) or "unknown"


def artefact_name(cluster, node, stamp):
    return "saga-%s-%s-%s%s" % (sanitize(cluster), sanitize(node), stamp, ARCHIVE_SUFFIX)


def parse_artefact_name(filename):
    """Fields of an artefact filename, or None if this is not one of ours.

    Returning None for anything unrecognised is what keeps retention from ever
    considering a file it did not write. A shared NFS target holds other people's
    files.
    """
    match = _ARTEFACT_RE.match(filename)
    if not match:
        return None
    try:
        epoch = parse_stamp(match.group("stamp"))
    except ValueError:
        return None
    return {
        "cluster": match.group("cluster"),
        "node": match.group("node"),
        "stamp": match.group("stamp"),
        "epoch": epoch,
    }


def sha256_and_size(path, chunk=1024 * 1024):
    digest = hashlib.sha256()
    total = 0
    with open(path, "rb") as handle:
        while True:
            block = handle.read(chunk)
            if not block:
                break
            digest.update(block)
            total += len(block)
    return digest.hexdigest(), total


def human_bytes(count):
    value = float(count or 0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return "%.0f %s" % (value, unit) if unit == "B" else "%.1f %s" % (value, unit)
        value /= 1024
    return "%.1f TiB" % value


def print_table(headers, rows):
    if not rows:
        print("No records found.")
        return
    str_rows = [[str(v) for v in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in str_rows:
        for idx, val in enumerate(row):
            widths[idx] = max(widths[idx], len(val))
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    print(sep)
    print("| " + " | ".join("%-*s" % (widths[i], h) for i, h in enumerate(headers)) + " |")
    print(sep)
    for row in str_rows:
        print("| " + " | ".join("%-*s" % (widths[i], v) for i, v in enumerate(row)) + " |")
    print(sep)


# ---------------------------------------------------------------------------------
# Command execution
# ---------------------------------------------------------------------------------

class Shell:
    """Every external command this tool runs, in one injectable place.

    No `shell=True` anywhere: the arguments include operator-supplied paths and a
    cluster name read out of the database, and building a command line out of those is
    how a backup tool ends up running someone else's command.
    """

    def run(self, argv, timeout=300, stdin_data=None):
        try:
            proc = subprocess.run(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                input=stdin_data,
                timeout=timeout,
            )
        except FileNotFoundError as exc:
            return 127, "", str(exc)
        except subprocess.TimeoutExpired as exc:
            out = (exc.stdout or b"").decode("utf-8", "ignore")
            return -1, out, "timed out after %ss: %s" % (timeout, " ".join(argv[:4]))
        return (
            proc.returncode,
            proc.stdout.decode("utf-8", "ignore"),
            proc.stderr.decode("utf-8", "ignore"),
        )


def local_ip(default="127.0.0.1"):
    """This node's cluster address.

    Read from spectrum.env the way every other daemon here does, because cqlsh must be
    pointed at the address Scylla actually bound; 127.0.0.1 is refused.
    """
    try:
        with open("/etc/hci/spectrum/spectrum.env", "r") as handle:
            for line in handle:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    if key == "LOCAL_HYPERVISOR_IP" and value:
                        return value
    except Exception:
        pass
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("10.255.255.255", 1))
        address = sock.getsockname()[0]
        sock.close()
        if address:
            return address
    except Exception:
        pass
    try:
        with open(CLUSTER_JSON, "r") as handle:
            hosts = json.load(handle).get("hosts", [])
        if hosts:
            return hosts[0]["ip"]
    except Exception:
        pass
    return default


def read_cluster_json(path=None):
    with open(path or CLUSTER_JSON, "r") as handle:
        return json.load(handle)


# ---------------------------------------------------------------------------------
# Target checks
# ---------------------------------------------------------------------------------

def check_target(path, data_dir, allow_same_filesystem=False, stat_fn=os.stat):
    """Validate the backup destination. Returns a dict of facts about it.

    The refusal that matters is the last one. A backup written to the same filesystem
    as the database it is protecting survives exactly the failures that were never
    going to destroy the data anyway, and it competes for the disk that stops the
    cluster when it fills. An operator can override it -- a local copy is better than
    none while the NFS export is being arranged -- but the artefact records that it did,
    so `saga list` can say out loud which backups are not actually off-box.
    """
    if not path:
        raise TargetUnusable(
            "no backup target configured. Set one with `valcli backup.target <dir>`, "
            "or pass --target. There is no default: an unconfigured default would "
            "silently write backups to the boot disk.")
    if not os.path.isdir(path):
        raise TargetUnusable(
            "backup target %r does not exist or is not a directory. If it is a mount "
            "point, the mount is not up -- writing here would fill the root filesystem "
            "with backups nobody can find." % path)
    if not os.access(path, os.W_OK | os.X_OK):
        raise TargetUnusable("backup target %r is not writable by this user" % path)

    facts = {"path": os.path.abspath(path), "same_filesystem": False}
    try:
        same = stat_fn(path).st_dev == stat_fn(data_dir).st_dev
    except OSError as exc:
        raise TargetUnusable("cannot stat %r or %r: %s" % (path, data_dir, exc))
    facts["same_filesystem"] = bool(same)
    if same and not allow_same_filesystem:
        raise TargetUnusable(
            "backup target %r is on the same filesystem as the database directory %r. "
            "A backup stored on the disk it protects is not a backup. Mount external "
            "storage, or pass --allow-same-filesystem to accept a local-only copy "
            "(the artefact will be marked LOCAL)." % (path, data_dir))
    return facts


# ---------------------------------------------------------------------------------
# Manifest and artefact integrity
# ---------------------------------------------------------------------------------

def write_artefact(members, archive_path, manifest_body, open_fn=tarfile.open):
    """Write one artefact and its sidecar manifest. Returns the completed manifest.

    `members` is a list of (arcname, real path) pairs.

    Two properties this has to have, both learned the hard way elsewhere in this repo:

      * The archive is built under a `.partial` name and renamed only once it is
        complete and flushed. A half-written file that already carries the final name
        is indistinguishable from a good backup, and retention would happily count it
        as one of the N it is keeping.
      * The manifest records every member's size and sha256 individually, and the
        sidecar additionally records the finished archive's size and sha256. A
        truncated archive fails on the archive digest; a member swapped out fails on
        its own. Note this detects corruption and truncation, not a determined
        tamperer who can rewrite the sidecar too -- `helios_sig.py` exists for
        signatures and this deliberately does not pretend to be one.
    """
    entries = []
    for arcname, real in members:
        digest, size = sha256_and_size(real)
        entries.append({"name": arcname, "bytes": size, "sha256": digest})

    manifest = dict(manifest_body)
    manifest["manifest_version"] = MANIFEST_VERSION
    manifest["members"] = entries

    partial = archive_path[: -len(ARCHIVE_SUFFIX)] + PARTIAL_SUFFIX
    scratch = tempfile.mkdtemp(prefix="saga-manifest-")
    try:
        inner = os.path.join(scratch, "manifest.json")
        with open(inner, "w") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)

        with open_fn(partial, "w:gz") as tar:
            # manifest.json first so the artefact is self-describing even if it is
            # separated from its sidecar. It is not listed in `members` -- it cannot
            # contain its own digest.
            tar.add(inner, arcname="manifest.json")
            for arcname, real in members:
                tar.add(real, arcname=arcname)

        try:
            with open(partial, "rb") as handle:
                os.fsync(handle.fileno())
        except OSError:
            # Best effort. Not every platform lets you fsync a read handle, and the
            # rename below is still what makes the artefact appear atomically.
            pass
        os.chmod(partial, 0o600)
        os.replace(partial, archive_path)
    except BaseException:
        if os.path.exists(partial):
            try:
                os.remove(partial)
            except OSError:
                pass
        raise
    finally:
        shutil.rmtree(scratch, ignore_errors=True)

    digest, size = sha256_and_size(archive_path)
    manifest["archive"] = {"name": os.path.basename(archive_path),
                           "bytes": size, "sha256": digest}
    sidecar = manifest_path_for(archive_path)
    with open(sidecar, "w") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
    os.chmod(sidecar, 0o600)
    return manifest


def manifest_path_for(archive_path):
    return archive_path[: -len(ARCHIVE_SUFFIX)] + MANIFEST_SUFFIX


def load_sidecar(archive_path):
    with open(manifest_path_for(archive_path), "r") as handle:
        return json.load(handle)


def quick_health(archive_path):
    """Cheap "is this artefact plausible" check, used by `list` and by retention.

    Deliberately does not hash: retention runs on every backup and a target holding a
    month of artefacts would be re-read in full every night for no new information.
    The size check catches the case that actually happens -- a run killed part way --
    and `saga verify` does the real work when it matters.
    """
    sidecar = manifest_path_for(archive_path)
    if not os.path.exists(sidecar):
        return False, "no manifest"
    try:
        with open(sidecar, "r") as handle:
            manifest = json.load(handle)
    except Exception as exc:
        return False, "manifest unreadable (%s)" % exc
    recorded = (manifest.get("archive") or {}).get("bytes")
    if recorded is None:
        return False, "manifest records no archive size"
    try:
        actual = os.path.getsize(archive_path)
    except OSError as exc:
        return False, "unreadable (%s)" % exc
    if actual != recorded:
        return False, "size %d, manifest says %d" % (actual, recorded)
    return True, ""


def verify_artefact(archive_path, open_fn=tarfile.open):
    """Full integrity check. Returns (manifest_or_None, list_of_problems)."""
    problems = []
    sidecar = manifest_path_for(archive_path)
    if not os.path.exists(sidecar):
        return None, ["no manifest sidecar beside %s" % os.path.basename(archive_path)]
    try:
        with open(sidecar, "r") as handle:
            manifest = json.load(handle)
    except Exception as exc:
        return None, ["manifest is not readable JSON: %s" % exc]

    recorded = manifest.get("archive") or {}
    try:
        digest, size = sha256_and_size(archive_path)
    except OSError as exc:
        return manifest, ["archive is unreadable: %s" % exc]

    if recorded.get("bytes") != size:
        problems.append("archive is %d bytes, manifest records %s -- truncated or "
                        "replaced" % (size, recorded.get("bytes")))
    if recorded.get("sha256") != digest:
        problems.append("archive sha256 %s does not match the manifest's %s"
                        % (digest[:16], str(recorded.get("sha256"))[:16]))

    expected = {m["name"]: m for m in manifest.get("members", [])}
    seen = {}
    try:
        with open_fn(archive_path, "r:gz") as tar:
            for member in tar:
                if not member.isfile():
                    continue
                if member.name == "manifest.json":
                    continue
                seen[member.name] = member.size
                wanted = expected.get(member.name)
                if wanted is None:
                    continue
                if member.size != wanted["bytes"]:
                    problems.append("%s is %d bytes, manifest records %d"
                                    % (member.name, member.size, wanted["bytes"]))
                    continue
                handle = tar.extractfile(member)
                if handle is None:
                    problems.append("%s could not be read out of the archive" % member.name)
                    continue
                observed = hashlib.sha256()
                while True:
                    block = handle.read(1024 * 1024)
                    if not block:
                        break
                    observed.update(block)
                if observed.hexdigest() != wanted["sha256"]:
                    problems.append("%s contents do not match its recorded sha256"
                                    % member.name)
    except (tarfile.TarError, EOFError, OSError) as exc:
        problems.append("archive could not be read to the end (%s) -- truncated" % exc)

    for name in sorted(set(expected) - set(seen)):
        problems.append("%s is listed in the manifest but missing from the archive" % name)
    for name in sorted(set(seen) - set(expected)):
        problems.append("%s is in the archive but not in the manifest" % name)

    return manifest, problems


# ---------------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------------

def scan_target(target, cluster=None):
    """Every artefact and abandoned partial at `target`, newest first."""
    entries = []
    try:
        names = sorted(os.listdir(target))
    except OSError as exc:
        raise TargetUnusable("cannot list backup target %r: %s" % (target, exc))

    for name in names:
        path = os.path.join(target, name)
        if name.endswith(PARTIAL_SUFFIX):
            fields = parse_artefact_name(name[: -len(PARTIAL_SUFFIX)] + ARCHIVE_SUFFIX)
            if not fields:
                continue
            if cluster and fields["cluster"] != sanitize(cluster):
                continue
            entries.append(dict(fields, name=name, path=path, partial=True,
                                healthy=False, health_reason="incomplete",
                                bytes=_size_or_zero(path)))
            continue
        fields = parse_artefact_name(name)
        if not fields:
            continue
        if cluster and fields["cluster"] != sanitize(cluster):
            continue
        healthy, reason = quick_health(path)
        entries.append(dict(fields, name=name, path=path, partial=False,
                            healthy=healthy, health_reason=reason,
                            bytes=_size_or_zero(path)))
    entries.sort(key=lambda e: e["epoch"], reverse=True)
    return entries


def _size_or_zero(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def retention_plan(entries, owner_node, keep, keep_days, now_epoch,
                   partial_max_age=PARTIAL_MAX_AGE_SECONDS):
    """Decide what to delete. Returns (retained, removals) where each removal is
    (entry, reason).

    Four rules, each one a way this has gone wrong somewhere:

      * `keep` is a floor that age can never override. A policy where "everything is
        older than 30 days" means "delete everything" is the one that turns a quiet
        month into an empty target.
      * A corrupt artefact never occupies one of the `keep` slots. Otherwise a run of
        three bad nights pushes the last good backup out of a keep=3 window, and the
        thing being kept is three copies of nothing.
      * A node only ever deletes its own artefacts. The target is shared; a node that
        prunes globally would delete a peer's backups on the peer's behalf using its
        own idea of how many there should be.
      * Deletion requires being outside BOTH limits: not in the newest `keep`, and
        older than `keep_days`. So keep_days=0 gives count-only retention and
        keep_days=30 keeps a month plus at least `keep` older ones.
    """
    if keep is None or int(keep) < 1:
        raise ValueError(
            "retention must keep at least one backup; keep=%r would allow a run to "
            "delete every artefact it has" % (keep,))
    keep = int(keep)
    keep_days = 0 if keep_days is None else int(keep_days)
    owner = sanitize(owner_node)

    retained, removals = [], []
    mine = [e for e in entries if e["node"] == owner]
    for entry in entries:
        if entry["node"] != owner:
            retained.append(entry)

    for entry in mine:
        if entry.get("partial"):
            age = now_epoch - entry["epoch"]
            if age > partial_max_age:
                removals.append((entry, "abandoned partial from an interrupted run"))
            else:
                retained.append(entry)

    healthy = sorted([e for e in mine if not e.get("partial") and e.get("healthy")],
                     key=lambda e: e["epoch"], reverse=True)
    protected = {e["name"] for e in healthy[:keep]}

    for entry in sorted([e for e in mine if not e.get("partial")],
                        key=lambda e: e["epoch"], reverse=True):
        if entry["name"] in protected:
            retained.append(entry)
            continue
        age_days = (now_epoch - entry["epoch"]) / 86400.0
        if age_days > keep_days:
            if entry.get("healthy"):
                reason = "beyond keep=%d and older than %d days" % (keep, keep_days)
            else:
                reason = "unusable (%s) and older than %d days" % (
                    entry.get("health_reason") or "unknown", keep_days)
            removals.append((entry, reason))
        else:
            retained.append(entry)

    removals.sort(key=lambda pair: pair[0]["epoch"])
    retained.sort(key=lambda e: e["epoch"], reverse=True)
    return retained, removals


def apply_retention(entries, owner_node, keep, keep_days, now_epoch, dry_run=False):
    retained, removals = retention_plan(entries, owner_node, keep, keep_days, now_epoch)
    for entry, reason in removals:
        print("  prune %s (%s)" % (entry["name"], reason))
        if dry_run:
            continue
        doomed = [entry["path"]]
        if not entry.get("partial"):
            doomed.append(manifest_path_for(entry["path"]))
        for path in doomed:
            try:
                os.remove(path)
            except OSError as exc:
                if exc.errno != errno.ENOENT:
                    print("  WARNING: could not remove %s: %s" % (path, exc))
    return retained, removals


# ---------------------------------------------------------------------------------
# Schema compatibility
# ---------------------------------------------------------------------------------

def check_schema_compatible(artefact_migrations, live_migrations, force=False):
    """Refuse a restore into a schema that is not the one the artefact was taken from.

    `hydra.schema_migrations` is the ledger helios_schema.py maintains: id -> checksum
    of the DDL that was applied. It is the only thing in the cluster that says what
    shape the tables have.

    Loading SSTables written against a different table definition is the failure with
    no symptom -- Scylla accepts them, and the columns that moved simply read wrong
    afterwards. So any difference stops the restore and names it. `--force` exists
    because after a total loss the operator may knowingly be restoring into a newer
    build, but it prints the whole difference first.

    Returns a list of warnings (empty when the schemas match exactly).
    """
    artefact = dict(artefact_migrations or {})
    live = dict(live_migrations or {})

    missing = sorted(set(artefact) - set(live))
    extra = sorted(set(live) - set(artefact))
    changed = sorted(mid for mid in set(artefact) & set(live)
                     if artefact[mid] != live[mid])

    if not (missing or extra or changed):
        return []

    lines = []
    if missing:
        lines.append(
            "the backup was taken with migrations this cluster has not applied: %s. "
            "Restoring would load SSTables for tables that do not exist here."
            % ", ".join(missing))
    if extra:
        lines.append(
            "this cluster has applied migrations the backup predates: %s. The tables "
            "have changed shape since the snapshot was taken."
            % ", ".join(extra))
    if changed:
        lines.append(
            "these migrations have the same id but different DDL here and in the "
            "backup: %s. One of the two clusters ran an edited migration."
            % ", ".join(changed))

    if not force:
        raise RestoreRefused(
            "schema mismatch between the artefact and this cluster:\n  - "
            + "\n  - ".join(lines)
            + "\nDeploy the build the backup was taken with and run the restore again, "
              "or pass --force if you have decided this is safe.")
    return lines


# ---------------------------------------------------------------------------------
# nodetool output parsing
# ---------------------------------------------------------------------------------

def parse_listsnapshots(text):
    """Rows out of `nodetool listsnapshots`.

    The size columns carry a space ("80 KB"), so only the first three fields can be
    read positionally. That is all the pruner needs, and guessing at the rest is how a
    parser starts deleting the wrong tag.
    """
    rows = []
    for line in (text or "").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Snapshot Details"):
            continue
        if stripped.startswith("Snapshot name"):
            continue
        if stripped.startswith("Total"):
            continue
        if stripped.lower().startswith("there are no snapshots"):
            continue
        parts = stripped.split()
        if len(parts) < 3:
            continue
        rows.append({"tag": parts[0], "keyspace": parts[1], "table": parts[2]})
    return rows


# ---------------------------------------------------------------------------------
# The cluster-facing half
# ---------------------------------------------------------------------------------

class Saga:
    """Everything that touches the live cluster, behind one injectable Shell."""

    def __init__(self, shell=None, address=None, container=SCYLLA_CONTAINER,
                 linstor_container=LINSTOR_CONTAINER, etc_hci=ETC_HCI,
                 certs_staging=CERTS_STAGING, cluster_json=CLUSTER_JSON):
        self.shell = shell or Shell()
        self.address = address or local_ip()
        self.container = container
        self.linstor_container = linstor_container
        # Host paths as constructor arguments rather than module constants read at
        # call time: the backup driver is the part worth testing, and it is only
        # testable if it can be pointed at a directory that is not /etc/hci.
        self.etc_hci = etc_hci
        self.certs_staging = certs_staging
        self.cluster_json = cluster_json
        self._data_dir = None
        self._settings = None

    # -- primitives -----------------------------------------------------------------
    def nodetool(self, args, timeout=600):
        if os.access(NODETOOL_WRAPPER, os.X_OK):
            argv = [NODETOOL_WRAPPER] + list(args)
        else:
            argv = ["podman", "exec", "-i", self.container, "nodetool"] + list(args)
        return self.shell.run(argv, timeout=timeout)

    def cqlsh(self, statement, timeout=120):
        """Run one CQL statement through the containerised cqlsh.

        Not through Daruk. Two reasons, both about when this tool is used: the restore
        path has to work on a cluster where the proxy is not running, and DESCRIBE is a
        cqlsh meta-command with no equivalent over the native protocol -- and the
        keyspace definition is a thing a metadata backup must contain.

        The address is this node's own; Scylla binds it and refuses 127.0.0.1.
        """
        argv = ["podman", "exec", "-i", self.container, "cqlsh", self.address,
                "-e", statement]
        rc, out, err = self.shell.run(argv, timeout=timeout)
        if rc != 0:
            raise SagaError("cqlsh failed (%s): %s" % (rc, (err or out).strip()[:400]))
        return out

    def cql_json(self, statement, timeout=120):
        """Rows from a `SELECT JSON ...`, as dicts.

        cqlsh prints a header, a rule, a blank line and a row count around the data,
        and on this cluster also a replication-factor warning block. Only lines that
        parse as a JSON object are rows.
        """
        out = self.cqlsh(statement, timeout=timeout)
        rows = []
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass
        return rows

    def data_dir(self):
        """The host path Scylla's data directory is bind-mounted from.

        Discovered rather than hardcoded: the snapshot files have to be read from the
        host, and `/var/lib/hci/hydra/data` is a provisioning decision, not a
        guarantee.
        """
        if self._data_dir:
            return self._data_dir
        rc, out, _err = self.shell.run(
            ["podman", "inspect", self.container, "--format", "{{json .Mounts}}"],
            timeout=60)
        if rc == 0 and out.strip():
            try:
                for mount in json.loads(out):
                    if mount.get("Destination") == "/var/lib/scylla":
                        self._data_dir = mount.get("Source")
                        break
            except ValueError:
                pass
        if not self._data_dir:
            self._data_dir = "/var/lib/hci/hydra/data"
        return self._data_dir

    # -- reads ----------------------------------------------------------------------
    def settings(self):
        """hydra.cluster_settings as a dict. Raises if the database cannot be read.

        Deliberately not swallowed. "no backup target configured" and "the database is
        down so I could not find out" are different sentences and an operator needs the
        second one.
        """
        if self._settings is None:
            rows = self.cql_json("SELECT JSON key, value FROM hydra.cluster_settings;")
            self._settings = {r.get("key"): r.get("value") for r in rows if r.get("key")}
        return self._settings

    def set_setting(self, key, value):
        self.cqlsh("INSERT INTO hydra.cluster_settings (key, value) VALUES ('%s', '%s');"
                   % (key.replace("'", "''"), str(value).replace("'", "''")))
        self._settings = None

    def schema_migrations(self):
        rows = self.cql_json("SELECT JSON id, checksum FROM hydra.schema_migrations;")
        return {r["id"]: r.get("checksum") for r in rows if r.get("id")}

    def table_directories(self, keyspace):
        """table name -> on-disk directory, for the tables the schema says exist now.

        The directory suffix is the table's UUID with the dashes stripped, and a table
        that has ever been dropped and recreated leaves the old directory behind: this
        cluster carries four `schema_migrations-*` directories and three
        `cluster_locks-*`, only one of each live. Globbing for `<table>-*` and taking
        the first hit would restore into a directory Scylla has forgotten about, and
        the data would simply never appear.
        """
        rows = self.cql_json(
            "SELECT JSON table_name, id FROM system_schema.tables "
            "WHERE keyspace_name = '%s';" % keyspace)
        base = os.path.join(self.data_dir(), "data", keyspace)
        out = {}
        for row in rows:
            name, table_id = row.get("table_name"), row.get("id")
            if not name or not table_id:
                continue
            out[name] = os.path.join(base, "%s-%s" % (name, table_id.replace("-", "")))
        return out

    def replication(self, keyspace):
        rows = self.cql_json(
            "SELECT JSON keyspace_name, replication FROM system_schema.keyspaces "
            "WHERE keyspace_name = '%s';" % keyspace)
        return rows[0].get("replication") if rows else None

    def linstor_controller_here(self):
        rc, out, _err = self.shell.run(
            ["podman", "ps", "--format", "{{.Names}}"], timeout=60)
        if rc != 0:
            return False
        return self.linstor_container in out.split()

    # -- snapshots ------------------------------------------------------------------
    def take_snapshot(self, keyspace, tag):
        rc, out, err = self.nodetool(["snapshot", "-t", tag, keyspace])
        if rc != 0:
            raise SagaError("nodetool snapshot failed (%s): %s"
                            % (rc, (err or out).strip()[:400]))
        return out

    def clear_snapshot(self, keyspace, tag):
        rc, out, err = self.nodetool(["clearsnapshot", "-t", tag, keyspace])
        if rc != 0:
            print("WARNING: could not clear snapshot %s: %s"
                  % (tag, (err or out).strip()[:200]), file=sys.stderr)
        return rc == 0

    def list_snapshots(self):
        rc, out, err = self.nodetool(["listsnapshots"])
        if rc != 0:
            raise SagaError("nodetool listsnapshots failed (%s): %s"
                            % (rc, (err or out).strip()[:200]))
        return parse_listsnapshots(out)

    def snapshot_members(self, keyspace, tag):
        """(arcname, path) for every SSTable file in this snapshot.

        Reads the directories under the data path directly. `nodetool` gives no way to
        list a snapshot's files, and the layout
        `<data>/data/<ks>/<table>-<uuid>/snapshots/<tag>/` is stable across
        Cassandra and Scylla.
        """
        base = os.path.join(self.data_dir(), "data", keyspace)
        members, tables = [], set()
        if not os.path.isdir(base):
            raise SagaError("keyspace directory %s does not exist" % base)
        for table_dir in sorted(os.listdir(base)):
            snap = os.path.join(base, table_dir, "snapshots", tag)
            if not os.path.isdir(snap):
                continue
            table = table_dir.rsplit("-", 1)[0]
            tables.add(table)
            for filename in sorted(os.listdir(snap)):
                real = os.path.join(snap, filename)
                if not os.path.isfile(real):
                    continue
                members.append(("scylla/%s/%s" % (table, filename), real))
        if not members:
            raise SagaError(
                "snapshot %r produced no files under %s. The snapshot was taken but "
                "nothing was found to archive -- refusing to write an empty backup."
                % (tag, base))
        return members, sorted(tables)

    # -- linstor --------------------------------------------------------------------
    def backup_linstor_db(self, name, scratch):
        """A consistent copy of the LINSTOR controller database.

        `linstor controller backupdb` writes into the controller's own
        /var/lib/linstor -- which on this cluster is the `linstor-db` DRBD volume, i.e.
        the very volume being protected. So it is moved off immediately and the
        original removed; leaving them there would grow the HA volume by a copy of
        itself on every run.
        """
        rc, out, err = self.shell.run(
            ["podman", "exec", self.linstor_container,
             "linstor", "controller", "backupdb", name], timeout=300)
        if rc != 0:
            return None, (err or out).strip()[:300]
        produced = os.path.join(LINSTOR_DIR, "%s.zip" % name)
        if not os.path.exists(produced):
            return None, "linstor reported success but %s is not there" % produced
        landed = os.path.join(scratch, "linstordb.zip")
        try:
            shutil.copy2(produced, landed)
        except OSError as exc:
            return None, "could not copy %s: %s" % (produced, exc)
        finally:
            try:
                os.remove(produced)
            except OSError:
                pass
        return landed, None

    # -- fan-out --------------------------------------------------------------------
    def run_on_peer(self, ip, command, timeout=1800):
        """Execute a command on another node through spark-daemon's mTLS API.

        Not `allssh`: it prints every node's output and then exits 0 regardless, so a
        scheduled backup driven through it reports SUCCESS on the night every node
        failed. The exit codes are the whole point here.

        `timeout` is sent in the payload because spark-daemon's own default is 45
        seconds and a backup on a large keyspace will exceed it.
        """
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                             cafile="/root/.certs/ca.crt")
        context.load_cert_chain(certfile="/root/.certs/client.crt",
                                keyfile="/root/.certs/client.key")
        # Node certificates carry `subjectAltName = IP:<node ip>` and nothing else, so
        # addressing a peer by that IP is what ties the connection to the node
        # answering it. See dagur.spark_endpoint() for the same reasoning.
        context.check_hostname = ip not in ("127.0.0.1", "::1", "localhost")
        payload = json.dumps({"command": command, "timeout": timeout}).encode("utf-8")
        request = urllib.request.Request(
            "https://%s:9099/api/v1/execute" % ip, data=payload,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, context=context,
                                        timeout=timeout + 60) as response:
                body = json.loads(response.read().decode("utf-8"))
            return body.get("returncode", -1), body.get("stdout", ""), body.get("stderr", "")
        except Exception as exc:
            return -1, "", "mTLS call to %s failed: %s" % (ip, exc)

    # -- the backup itself ----------------------------------------------------------
    def backup(self, target, keyspace=DEFAULT_KEYSPACE, round_tag=None,
               include_ca=False, keep=DEFAULT_KEEP, keep_days=DEFAULT_KEEP_DAYS,
               prune=True, allow_same_filesystem=False, now=None):
        """Take one node's metadata backup. Returns the completed manifest.

        Raises SagaError on any failure. It never returns a manifest for a run that did
        not complete, and it never leaves a file at the final artefact name unless the
        whole thing succeeded -- the two halves of "a failed backup does not report
        success".
        """
        now = time.time() if now is None else now
        stamp = utc_stamp(now)
        round_tag = round_tag or stamp
        data_dir = self.data_dir()
        target_facts = check_target(target, data_dir,
                                    allow_same_filesystem=allow_same_filesystem)

        try:
            cluster_cfg = read_cluster_json(self.cluster_json)
        except Exception as exc:
            raise SagaError(
                "cannot read %s (%s). It names the nodes and holds the redundancy "
                "factor; a metadata backup without it is not restorable."
                % (self.cluster_json, exc))
        cluster_name = cluster_cfg.get("cluster_name") or "helios"

        # Read the schema before the snapshot, so what is recorded describes the shape
        # the SSTables were written in.
        migrations = self.schema_migrations()
        if not migrations:
            raise SagaError(
                "hydra.schema_migrations is empty or unreadable. Without the migration "
                "ledger a restore cannot tell whether the tables it is loading into "
                "have the same shape, so this backup would be unverifiable.")
        schema_cql = self.cqlsh("DESCRIBE KEYSPACE %s;" % keyspace)
        if "CREATE TABLE" not in schema_cql:
            raise SagaError("DESCRIBE KEYSPACE %s returned no table definitions"
                            % keyspace)

        scratch = tempfile.mkdtemp(prefix="saga-stage-")
        snapshot_tag = "%s%s" % (SNAPSHOT_PREFIX, round_tag)
        snapshot_taken = False
        try:
            print("Snapshotting keyspace %s as %s ..." % (keyspace, snapshot_tag))
            self.take_snapshot(keyspace, snapshot_tag)
            snapshot_taken = True

            members, tables = self.snapshot_members(keyspace, snapshot_tag)
            print("  %d SSTable files across %d tables" % (len(members), len(tables)))

            notes = []

            # /etc/hci, cluster.json first. Private keys are skipped unless the
            # operator asked for them: an artefact on an NFS share that contains every
            # node's TLS key is a different risk from one that contains metadata.
            config_members, skipped = collect_etc_hci(self.etc_hci,
                                                      include_keys=include_ca)
            members.extend(config_members)
            notes.extend(skipped)

            ca_captured = False
            if include_ca:
                ca_members, ca_notes = collect_ca(self.certs_staging)
                members.extend(ca_members)
                notes.extend(ca_notes)
                ca_captured = bool(ca_members)

            linstor_note = None
            if self.linstor_controller_here():
                path, error = self.backup_linstor_db("saga-%s" % round_tag, scratch)
                if path:
                    members.append(("linstor/linstordb.zip", path))
                else:
                    linstor_note = error
                    notes.append("LINSTOR controller database NOT captured: %s" % error)
            else:
                linstor_note = "the LINSTOR controller does not run on this node"
                notes.append("LINSTOR controller database not captured here: %s"
                             % linstor_note)

            for name, text in (
                    ("meta/schema.cql", schema_cql),
                    ("meta/schema_migrations.json",
                     json.dumps(migrations, indent=2, sort_keys=True)),
                    ("meta/nodetool-status.txt", self._nodetool_status_text()),
            ):
                path = os.path.join(scratch, name.replace("/", "_"))
                with open(path, "w") as handle:
                    handle.write(text)
                members.append((name, path))

            manifest_body = {
                "tool": "saga",
                "created_at": stamp,
                "created_at_epoch": int(now),
                "round_tag": round_tag,
                "snapshot_tag": snapshot_tag,
                "cluster_name": cluster_name,
                "node": self.address,
                "keyspace": keyspace,
                "tables": tables,
                "replication": self.replication(keyspace),
                "cluster_json": cluster_cfg,
                "schema_migrations": migrations,
                "contains_ca": ca_captured,
                "contains_linstor_db": linstor_note is None,
                "linstor_note": linstor_note,
                "target_on_data_filesystem": target_facts["same_filesystem"],
                "notes": notes,
                "covers_guest_data": False,
            }

            archive = os.path.join(
                target, artefact_name(cluster_name, self.address, stamp))
            manifest = write_artefact(members, archive, manifest_body)
            print("Wrote %s (%s)" % (os.path.basename(archive),
                                     human_bytes(manifest["archive"]["bytes"])))
            for note in notes:
                print("  note: %s" % note)
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
            # Unconditionally, including on the failure paths above. A snapshot is
            # hardlinks, so it costs nothing at first and then costs everything: it
            # pins every SSTable it references against deletion, so compaction can no
            # longer free the space. A tool that leaves one behind every time it fails
            # fills the disk the database lives on.
            if snapshot_taken:
                self.clear_snapshot(keyspace, snapshot_tag)

        if prune:
            print("Applying retention (keep=%s, keep_days=%s) ..." % (keep, keep_days))
            apply_retention(scan_target(target, cluster_name), self.address,
                            keep, keep_days, int(now))
        return manifest

    def _nodetool_status_text(self):
        rc, out, err = self.nodetool(["status"], timeout=120)
        if rc != 0:
            # Diagnostic only. The ring layout is useful when reading a backup months
            # later; it is not something to fail a backup over.
            return "nodetool status failed (%s): %s\n" % (rc, (err or out).strip())
        return out

    # -- the restore ----------------------------------------------------------------
    def restore(self, archive_path, keyspace=None, tables=None, force=False,
                skip_verify=False):
        """Load an artefact's SSTables back into the live cluster.

        `nodetool refresh --load-and-stream` rather than a file copy into the live
        table directory: it makes the node hand any SSTable whose token range it does
        not own to the node that does, which is what makes one node's artefact
        restorable on a cluster whose ring has changed shape since.
        """
        if not skip_verify:
            print("Verifying %s ..." % os.path.basename(archive_path))
            manifest, problems = verify_artefact(archive_path)
            if problems:
                raise RestoreRefused(
                    "artefact failed verification; refusing to restore from it:\n  - "
                    + "\n  - ".join(problems))
        else:
            manifest = load_sidecar(archive_path)

        keyspace = keyspace or manifest.get("keyspace") or DEFAULT_KEYSPACE
        warnings = check_schema_compatible(
            manifest.get("schema_migrations"), self.schema_migrations(), force=force)
        for line in warnings:
            print("WARNING (forced past): %s" % line)

        live_dirs = self.table_directories(keyspace)
        scratch = tempfile.mkdtemp(prefix="saga-restore-")
        restored, skipped = [], []
        try:
            extract_archive(archive_path, scratch)
            staged = os.path.join(scratch, "scylla")
            if not os.path.isdir(staged):
                raise RestoreRefused("artefact contains no scylla/ payload")

            for table in sorted(os.listdir(staged)):
                if tables and table not in tables:
                    continue
                source = os.path.join(staged, table)
                if not os.path.isdir(source):
                    continue
                live = live_dirs.get(table)
                if not live:
                    skipped.append((table, "no such table in the live schema"))
                    continue
                if not os.path.isdir(live):
                    skipped.append((table, "live directory %s missing" % live))
                    continue
                upload = os.path.join(live, "upload")
                os.makedirs(upload, exist_ok=True)
                count = 0
                for filename in sorted(os.listdir(source)):
                    # A Scylla snapshot directory also holds its own manifest.json and
                    # the table's schema.cql. Both are worth keeping in the artefact
                    # and neither is an SSTable, so `nodetool refresh` leaves them
                    # sitting in upload/ forever -- where they are indistinguishable
                    # from an SSTable the refresh failed to consume.
                    if filename in NON_SSTABLE_SNAPSHOT_FILES:
                        continue
                    shutil.copy2(os.path.join(source, filename),
                                 os.path.join(upload, filename))
                    count += 1
                rc, out, err = self.nodetool(
                    ["refresh", "--load-and-stream", keyspace, table])
                if rc != 0:
                    skipped.append((table, "refresh failed: %s"
                                    % (err or out).strip()[:200]))
                    continue
                leftover = sorted(os.listdir(upload))
                restored.append((table, count, len(leftover)))
                print("  %-28s %d SSTable files loaded" % (table, count))
                if leftover:
                    # refresh consumes what it loads. Anything still here was not
                    # loaded, and the rows it holds are not in the table.
                    print("    WARNING: %d file(s) left in %s -- refresh did not "
                          "consume them: %s"
                          % (len(leftover), upload, ", ".join(leftover[:5])))
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

        for table, reason in skipped:
            print("  SKIPPED %-24s %s" % (table, reason))
        if not restored:
            raise SagaError("no tables were restored")
        return restored, skipped


def extract_archive(archive_path, dest):
    with tarfile.open(archive_path, "r:gz") as tar:
        try:
            # Python 3.12 deprecates the unfiltered form; 'data' refuses absolute
            # paths, links escaping the destination and device nodes.
            tar.extractall(dest, filter="data")
        except TypeError:
            tar.extractall(dest)


def collect_etc_hci(root=ETC_HCI, include_keys=False, max_bytes=ETC_FILE_MAX_BYTES):
    """(arcname, path) pairs for the host configuration tree, plus notes on skips."""
    members, notes = [], []
    if not os.path.isdir(root):
        return members, ["%s does not exist on this node" % root]
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in sorted(filenames):
            real = os.path.join(dirpath, filename)
            if os.path.islink(real) or not os.path.isfile(real):
                continue
            relative = os.path.relpath(real, root).replace(os.sep, "/")
            if filename.endswith(".key") and not include_keys:
                notes.append("skipped private key etc-hci/%s (use --include-ca to "
                             "capture key material)" % relative)
                continue
            try:
                size = os.path.getsize(real)
            except OSError as exc:
                notes.append("could not stat %s: %s" % (real, exc))
                continue
            if size > max_bytes:
                notes.append("skipped etc-hci/%s: %s, larger than the %s configuration "
                             "limit" % (relative, human_bytes(size),
                                        human_bytes(max_bytes)))
                continue
            members.append(("etc-hci/%s" % relative, real))
    return members, notes


def collect_ca(root=CERTS_STAGING):
    """The cluster CA. Present on exactly one host in the cluster and nowhere else."""
    members, notes = [], []
    if not os.path.isdir(root):
        return members, ["%s is not on this node, so the CA was not captured here "
                         "(it lives on the first host in cluster.json)" % root]
    for filename in sorted(os.listdir(root)):
        real = os.path.join(root, filename)
        if os.path.isfile(real) and not os.path.islink(real):
            members.append(("ca/%s" % filename, real))
    if members:
        notes.append("this artefact contains the cluster CA private key -- treat the "
                     "backup target as secret material")
    return members, notes


# ---------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------

def resolve_target(saga, explicit):
    """Where artefacts live. Explicit beats environment beats the cluster setting.

    The explicit forms are checked first and short-circuit the database read entirely,
    because `saga restore` has to work on a cluster whose metadata layer is the thing
    that is broken.
    """
    if explicit:
        return explicit
    from_env = os.environ.get("SAGA_TARGET")
    if from_env:
        return from_env
    try:
        target = (saga.settings() or {}).get(SETTING_TARGET)
    except SagaError as exc:
        raise SagaError(
            "no --target was given and the %s cluster setting could not be read: %s. "
            "This says nothing about whether backups exist -- pass --target to say "
            "where they are." % (SETTING_TARGET, exc))
    if not target:
        raise TargetUnusable(
            "no backup target is configured. Set one with `valcli backup.target "
            "<dir>` (or `saga target <dir>`), pointing at storage that is not this "
            "node's disk.")
    return target


def resolve_retention(saga, keep, keep_days):
    settings = {}
    if keep is None or keep_days is None:
        try:
            settings = saga.settings() or {}
        except SagaError:
            # Retention defaults are safe to fall back on; unlike the target, a wrong
            # guess here cannot send the artefact somewhere unexpected.
            settings = {}
    if keep is None:
        keep = settings.get(SETTING_KEEP, DEFAULT_KEEP)
    if keep_days is None:
        keep_days = settings.get(SETTING_KEEP_DAYS, DEFAULT_KEEP_DAYS)
    try:
        keep = int(keep)
    except (TypeError, ValueError):
        keep = DEFAULT_KEEP
    try:
        keep_days = int(keep_days)
    except (TypeError, ValueError):
        keep_days = DEFAULT_KEEP_DAYS
    return keep, keep_days


def peer_ips(exclude):
    try:
        hosts = read_cluster_json().get("hosts", [])
    except Exception:
        return []
    return [h["ip"] for h in hosts if h.get("ip") and h["ip"] != exclude]


def cmd_backup(args):
    saga = Saga()
    target = resolve_target(saga, args.target)
    keep, keep_days = resolve_retention(saga, args.keep, args.keep_days)
    round_tag = args.tag or utc_stamp()

    failures = []
    print("=== %s (local) ===" % saga.address)
    try:
        saga.backup(target, keyspace=args.keyspace, round_tag=round_tag,
                    include_ca=args.include_ca, keep=keep, keep_days=keep_days,
                    prune=not args.no_prune,
                    allow_same_filesystem=args.allow_same_filesystem)
    except SagaError as exc:
        print("FAILED: %s" % exc, file=sys.stderr)
        failures.append(saga.address)

    if args.all_nodes:
        peers = peer_ips(saga.address)
        if peers:
            remote = ["/usr/local/bin/saga", "backup",
                      "--keyspace", args.keyspace,
                      "--target", target or "",
                      "--tag", round_tag,
                      "--keep", str(keep), "--keep-days", str(keep_days)]
            if args.include_ca:
                remote.append("--include-ca")
            if args.no_prune:
                remote.append("--no-prune")
            if args.allow_same_filesystem:
                remote.append("--allow-same-filesystem")
            command = " ".join(remote)
            results = {}

            def worker(ip):
                results[ip] = saga.run_on_peer(ip, command, timeout=args.timeout)

            threads = [threading.Thread(target=worker, args=(ip,)) for ip in peers]
            # In parallel: spark-daemon runs each node's backup synchronously, so a
            # sequential fan-out costs the sum of every node's runtime, and the whole
            # thing is invoked from Dagur inside a bounded window.
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            for ip in peers:
                rc, out, err = results.get(ip, (-1, "", "no result"))
                print("=== %s ===" % ip)
                if out:
                    print(out.rstrip())
                if err:
                    print(err.rstrip(), file=sys.stderr)
                if rc != 0:
                    failures.append(ip)

    if failures:
        print("Backup FAILED on: %s" % ", ".join(failures), file=sys.stderr)
        return 1
    return 0


def cmd_list(args):
    saga = Saga()
    target = resolve_target(saga, args.target)
    entries = scan_target(target)
    rows, rounds = [], {}
    for entry in entries:
        manifest = {}
        if not entry["partial"]:
            try:
                manifest = load_sidecar(entry["path"])
            except Exception:
                manifest = {}
        flags = []
        if manifest.get("contains_ca"):
            flags.append("CA")
        if manifest.get("contains_linstor_db"):
            flags.append("LINSTOR")
        if manifest.get("target_on_data_filesystem"):
            flags.append("LOCAL")
        if entry["partial"]:
            flags.append("PARTIAL")
        rows.append([entry["stamp"], entry["cluster"], entry["node"],
                     human_bytes(entry["bytes"]),
                     "ok" if entry["healthy"] else (entry["health_reason"] or "bad"),
                     ",".join(flags) or "-"])
        tag = manifest.get("round_tag")
        if tag:
            bucket = rounds.setdefault(tag, {"nodes": set(), "linstor": False})
            bucket["nodes"].add(entry["node"])
            bucket["linstor"] = bucket["linstor"] or bool(manifest.get("contains_linstor_db"))

    print_table(["When (UTC)", "Cluster", "Node", "Size", "State", "Flags"], rows)

    if rounds:
        try:
            expected = len(read_cluster_json().get("hosts", []))
        except Exception:
            expected = 0
        print()
        print("Backup rounds (one artefact per node makes one restorable set):")
        for tag in sorted(rounds, reverse=True)[:10]:
            bucket = rounds[tag]
            complete = "" if not expected else " of %d" % expected
            print("  %s  %d%s node(s)%s"
                  % (tag, len(bucket["nodes"]), complete,
                     "" if bucket["linstor"] else "  [no LINSTOR controller DB]"))
    return 0


def resolve_artefact(target, name):
    if name in (None, "latest"):
        entries = [e for e in scan_target(target) if not e["partial"]]
        if not entries:
            raise SagaError("no artefacts at %s" % target)
        return entries[0]["path"]
    if os.path.isabs(name) or os.path.exists(name):
        return name
    return os.path.join(target, name)


def cmd_verify(args):
    saga = Saga()
    target = resolve_target(saga, args.target)
    path = resolve_artefact(target, args.artefact)
    manifest, problems = verify_artefact(path)
    print("Artefact: %s" % path)
    if manifest:
        print("  taken %s on node %s, keyspace %s, %d tables"
              % (manifest.get("created_at"), manifest.get("node"),
                 manifest.get("keyspace"), len(manifest.get("tables") or [])))
        print("  migrations: %s" % ", ".join(sorted(manifest.get("schema_migrations") or {})))
        if not manifest.get("contains_linstor_db"):
            print("  LINSTOR controller DB: not in this artefact (%s)"
                  % manifest.get("linstor_note"))
    if problems:
        print("FAILED:")
        for problem in problems:
            print("  - %s" % problem)
        return 1
    print("OK: archive and every member match the manifest.")
    return 0


def cmd_restore(args):
    saga = Saga()
    target = resolve_target(saga, args.target)
    path = resolve_artefact(target, args.artefact)

    if args.extract_only:
        os.makedirs(args.extract_only, exist_ok=True)
        extract_archive(path, args.extract_only)
        print("Extracted %s to %s" % (os.path.basename(path), args.extract_only))
        print("Nothing was written to the cluster. cluster.json, the CA and the "
              "LINSTOR database are restored by hand -- see docs/backup_restore.md.")
        return 0

    if not args.yes:
        if not sys.stdin.isatty():
            print("Refusing to restore without --yes when stdin is not a terminal.",
                  file=sys.stderr)
            return 1
        print("This loads SSTables from %s into the live %s keyspace."
              % (os.path.basename(path), args.keyspace or "backed-up"))
        print("Restore MERGES into what is there now; it does not replace it.")
        if input("Type 'restore' to continue: ").strip() != "restore":
            print("Aborted.")
            return 1

    tables = [t.strip() for t in args.tables.split(",")] if args.tables else None
    saga.restore(path, keyspace=args.keyspace, tables=tables, force=args.force)
    print("Restore complete.")
    return 0


def cmd_prune(args):
    saga = Saga()
    target = resolve_target(saga, args.target)
    keep, keep_days = resolve_retention(saga, args.keep, args.keep_days)
    entries = scan_target(target)
    retained, removals = apply_retention(entries, saga.address, keep, keep_days,
                                         int(time.time()), dry_run=args.dry_run)
    print("%d artefact(s) retained, %d %s."
          % (len(retained), len(removals),
             "would be removed" if args.dry_run else "removed"))
    return 0


def cmd_snapshots(args):
    saga = Saga()
    rows = saga.list_snapshots()
    ours = sorted({r["tag"] for r in rows if r["tag"].startswith(SNAPSHOT_PREFIX)})
    others = sorted({r["tag"] for r in rows if not r["tag"].startswith(SNAPSHOT_PREFIX)})
    print_table(["Tag", "Keyspace", "Table"],
                [[r["tag"], r["keyspace"], r["table"]] for r in rows])
    print()
    print("saga snapshots: %d  |  other tags: %d" % (len(ours), len(others)))
    if others:
        print("Not touched by --prune: %s" % ", ".join(others[:8])
              + (" ..." if len(others) > 8 else ""))
        print("  (Scylla writes a pre-drop-* snapshot every time a table is dropped "
              "and never clears it. It is the last copy of that table -- clear it "
              "with `nodetool clearsnapshot -t <tag>` once you are sure.)")
    if args.prune:
        for tag in ours:
            keyspaces = sorted({r["keyspace"] for r in rows if r["tag"] == tag})
            for keyspace in keyspaces:
                print("Clearing %s / %s" % (tag, keyspace))
                saga.clear_snapshot(keyspace, tag)
    return 0


def cmd_target(args):
    saga = Saga()
    if args.path:
        saga.set_setting(SETTING_TARGET, args.path)
        print("Backup target set to %s" % args.path)
        try:
            facts = check_target(args.path, saga.data_dir(),
                                 allow_same_filesystem=True)
            if facts["same_filesystem"]:
                print("WARNING: %s is on the same filesystem as %s. Backups written "
                      "here are lost with the disk they protect; runs will need "
                      "--allow-same-filesystem." % (args.path, saga.data_dir()))
        except TargetUnusable as exc:
            print("WARNING: %s" % exc)
        return 0
    current = saga.settings().get(SETTING_TARGET)
    print(current if current else "(not configured)")
    return 0 if current else 1


def build_parser():
    parser = argparse.ArgumentParser(
        prog="saga",
        description="Backup and restore of Helios cluster metadata. Guest data inside "
                    "DRBD volumes is NOT covered -- see docs/backup_restore.md.")
    sub = parser.add_subparsers(dest="command")

    def add_target(p):
        p.add_argument("--target", help="destination directory (default: the "
                                        "%s cluster setting)" % SETTING_TARGET)

    backup = sub.add_parser("backup", help="take a metadata backup")
    add_target(backup)
    backup.add_argument("--keyspace", default=DEFAULT_KEYSPACE)
    backup.add_argument("--tag", help="shared round tag (set automatically by --all-nodes)")
    backup.add_argument("--all-nodes", action="store_true",
                        help="also run on every peer in cluster.json, in parallel")
    backup.add_argument("--include-ca", action="store_true",
                        help="also capture the cluster CA and node private keys")
    backup.add_argument("--keep", type=int, default=None)
    backup.add_argument("--keep-days", type=int, default=None)
    backup.add_argument("--no-prune", action="store_true")
    backup.add_argument("--allow-same-filesystem", action="store_true",
                        help="accept a target on the same disk as the database")
    backup.add_argument("--timeout", type=int, default=1800,
                        help="per-peer timeout for --all-nodes")
    backup.set_defaults(func=cmd_backup)

    listing = sub.add_parser("list", help="artefacts at the backup target")
    add_target(listing)
    listing.set_defaults(func=cmd_list)

    verify = sub.add_parser("verify", help="check one artefact against its manifest")
    add_target(verify)
    verify.add_argument("artefact", nargs="?", default="latest")
    verify.set_defaults(func=cmd_verify)

    restore = sub.add_parser("restore", help="load an artefact back into the cluster")
    add_target(restore)
    restore.add_argument("artefact", nargs="?", default="latest")
    restore.add_argument("--keyspace", default=None)
    restore.add_argument("--tables", default=None, help="comma-separated subset")
    restore.add_argument("--force", action="store_true",
                         help="proceed despite a schema mismatch")
    restore.add_argument("--yes", action="store_true")
    restore.add_argument("--extract-only", metavar="DIR",
                         help="unpack the artefact and touch nothing")
    restore.set_defaults(func=cmd_restore)

    prune = sub.add_parser("prune", help="apply retention now")
    add_target(prune)
    prune.add_argument("--keep", type=int, default=None)
    prune.add_argument("--keep-days", type=int, default=None)
    prune.add_argument("--dry-run", action="store_true")
    prune.set_defaults(func=cmd_prune)

    snapshots = sub.add_parser("snapshots", help="list or clear Scylla snapshots")
    snapshots.add_argument("--prune", action="store_true",
                           help="clear leftover saga-* snapshots")
    snapshots.set_defaults(func=cmd_snapshots)

    target = sub.add_parser("target", help="get or set the backup target")
    target.add_argument("path", nargs="?")
    target.set_defaults(func=cmd_target)

    return parser


def main(argv=None):
    try:
        # Dagur captures stdout and stderr together and writes the pair into
        # hydra.dagur_runs. Block-buffered stdout puts every failure message before the
        # progress lines that explain it, which is exactly backwards in a run record
        # somebody reads months later.
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 1
    try:
        return args.func(args)
    except (SagaError, ValueError) as exc:
        print("Error: %s" % exc, file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except BrokenPipeError:
        # `saga verify | head` is a normal thing to do, and a listing of 800 members is
        # exactly when somebody does it. Without this the interpreter's shutdown flush
        # prints a traceback over the output the operator was reading.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return 0


if __name__ == "__main__":
    sys.exit(main())
