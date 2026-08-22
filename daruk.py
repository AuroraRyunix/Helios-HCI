import sys
import json
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler
from cassandra.cluster import Cluster

# Get local hypervisor IP dynamically using UDP socket method
def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

from cassandra import ConsistencyLevel, Unavailable, ReadTimeout, OperationTimedOut
from cassandra.cluster import NoHostAvailable
import time

cluster = None
session = None

def connect_db():
    global cluster, session
    LOCAL_IP = get_local_ip()
    retries = 30
    while retries > 0:
        try:
            print(f"Daruk connecting to ScyllaDB at {LOCAL_IP}...")
            cluster = Cluster([LOCAL_IP])
            session = cluster.connect()
            session.default_consistency_level = ConsistencyLevel.QUORUM
            print("Daruk successfully connected to ScyllaDB.")
            return
        except Exception as e:
            print(f"ScyllaDB connection failed: {e}. Retrying in 2 seconds... ({retries} left)")
            time.sleep(2)
            retries -= 1
    raise RuntimeError("Failed to connect to ScyllaDB after 30 attempts.")

connect_db()

def make_serializable(obj):
    """Convert a driver result into something json.dumps will accept.

    The types below are the ones the driver hands back that JSON has no notion of, and
    every one of them used to fall through to `return obj` and raise inside json.dumps --
    *after* the handler's try block, so the caller got a 400 with no body.

    That failed worst on exactly the response worth having. A refused lightweight
    transaction returns the whole existing row, so a conditional write against any table
    with a `timestamp` or `uuid` column answered 400 on rejection and 200 on success --
    which reads as "the call failed" rather than "somebody else holds it". Two tables
    were given `bigint`/`text` columns specifically to dodge this before it was fixed
    here; that should not have been necessary and is not, now.

    Timestamps become epoch milliseconds, matching how the Python tier already writes
    them (`INSERT ... JSON` with an integer), so a value survives a round trip unchanged.
    """
    import datetime
    import decimal
    import ipaddress
    import uuid as uuid_module

    if isinstance(obj, dict):
        return {str(k): make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [make_serializable(v) for v in obj]
    if isinstance(obj, uuid_module.UUID):
        return str(obj)
    if isinstance(obj, datetime.datetime):
        # Naive values out of the driver are UTC.
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=datetime.timezone.utc)
        return int(obj.timestamp() * 1000)
    if isinstance(obj, (datetime.date, datetime.time)):
        return obj.isoformat()
    if isinstance(obj, datetime.timedelta):
        return int(obj.total_seconds() * 1000)
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (bytes, bytearray)):
        return obj.hex()
    if isinstance(obj, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return str(obj)
    if hasattr(obj, 'items'):
        return {str(k): make_serializable(v) for k, v in obj.items()}
    return obj

# Statements that only read. Anything else -- INSERT, UPDATE, DELETE, BATCH, TRUNCATE,
# and every DDL form -- mutates and must never be silently retried at a weaker
# consistency level.
_READ_PREFIXES = ("select",)

# Lightweight transactions carry an IF clause and are resolved by Paxos at SERIAL. A
# retry at ONE would defeat the compare-and-swap entirely, so they are never degraded
# even though some are syntactically writes and some reads.
def _is_lwt(statement):
    lowered = statement.lower()
    return " if " in lowered or lowered.rstrip().endswith(" if exists")


def _is_read(statement):
    return statement.lstrip().lower().startswith(_READ_PREFIXES)


def _is_degradable_failure(exc):
    """True only for genuine availability failures, identified by driver exception type.

    The previous check matched the substrings "unavailable", "timeout" and "active"
    anywhere in the exception text. "active" in particular matches a large range of
    unrelated errors, so ordinary failures were being retried at a weaker consistency
    level rather than surfaced.
    """
    return isinstance(exc, (Unavailable, ReadTimeout, OperationTimedOut, NoHostAvailable))


# --- typed compare-and-swap endpoints -------------------------------------------------
#
# /query runs whatever CQL it is handed and answers only "did the statement execute".
# That is enough for a blind UPDATE and useless for a conditional one: a lightweight
# transaction that was *rejected* executed perfectly well, and every caller's
# run_cql_query() flattens the driver's rows into space-joined strings, so "another host
# already owns this VM" and "the claim succeeded" arrive as the same rc=0. Ownership
# writes in the Python tier were therefore unconditional in practice even where someone
# had written an IF clause.
#
# The operations below take structured parameters instead. Their CQL lives in this table
# and nowhere else, so no caller-supplied text can reach a statement, and each one is
# prepared once and executed with bound values.

MIGRATION_LOCK = "migrating"
UNLOCKED_STATUS = "running"

# Cluster-wide mutual exclusion lives in one row of hydra.cluster_locks, keyed by lock
# name. A lightweight transaction is confined to a single partition, so any exclusion
# that has to hold across hosts must condition on a row every contender shares -- which
# a scan of hydra.nodes is not, and never was.
#
# Every non-key column is rewritten on renew, holder_token included. Leaving one out
# would let that column keep the *original* insert's TTL and expire first, after which
# the row is still alive (other cells are live) but no longer renewable or releasable by
# its holder. Verified against Scylla 5.4: a renewed row still refuses a competing
# IF NOT EXISTS after the original insert marker has expired.
_LOCK_COLUMNS = ("name", "holder", "holder_token", "reason", "acquired_at_ms")
_LOCK_ACQUIRE_CQL = (
    "INSERT INTO hydra.cluster_locks (" + ", ".join(_LOCK_COLUMNS) + ") VALUES ("
    + ", ".join("?" * len(_LOCK_COLUMNS)) + ") IF NOT EXISTS USING TTL ?"
)

# Built from one tuple so the column list and the placeholder count cannot drift apart.
_VM_COLUMNS = (
    "name", "vcpu", "memory", "disk_path", "disk_size", "state", "host_ip", "disks_list",
    "firmware", "iso", "boot_device", "network_id", "cpu_model", "audio_enabled", "status",
)
_VM_CREATE_CQL = (
    "INSERT INTO hydra.vms (" + ", ".join(_VM_COLUMNS) + ") VALUES ("
    + ", ".join("?" * len(_VM_COLUMNS)) + ") IF NOT EXISTS"
)

LWT_OPS = {
    # Placement. `expected_host_ip` is the value the caller read before it decided to
    # start the VM; the swap only lands if the row still holds it. Without the condition
    # two nodes write their own IP a millisecond apart, both believe they own the VM, and
    # both open the same DRBD device -- the dual-primary corruption this exists to stop.
    "/v1/vm/claim": {
        "cql": "UPDATE hydra.vms SET host_ip = ?, state = ? WHERE name = ? IF host_ip = ?",
        "binds": ("host_ip", "state", "name", "expected_host_ip"),
        "params": {
            "name": {"type": "text", "required": True},
            "host_ip": {"type": "text", "required": True},
            "expected_host_ip": {"type": "text", "default": "", "nullable": True},
            "state": {"type": "text", "default": "Running"},
        },
    },
    # Giving a placement back. `expected_host_ip` is required and has no default: a
    # release that matches any owner is the blind write these endpoints replace, and it
    # is the one that frees a *live* VM's row for a second host to claim.
    "/v1/vm/release": {
        "cql": "UPDATE hydra.vms SET host_ip = ?, state = ? WHERE name = ? IF host_ip = ?",
        "binds": ("released_host_ip", "state", "name", "expected_host_ip"),
        "fixed": {"released_host_ip": ""},
        "params": {
            "name": {"type": "text", "required": True},
            "expected_host_ip": {"type": "text", "required": True, "nullable": True},
            "state": {"type": "text", "default": "Stopped"},
        },
    },
    # Recording what a hypervisor is actually doing, conditional on this caller still
    # being the host of record. A reconciler that writes state for a VM that moved away
    # is reporting on someone else's machine.
    "/v1/vm/set-state": {
        "cql": "UPDATE hydra.vms SET state = ? WHERE name = ? IF host_ip = ?",
        "binds": ("state", "name", "expected_host_ip"),
        "params": {
            "name": {"type": "text", "required": True},
            "state": {"type": "text", "required": True},
            "expected_host_ip": {"type": "text", "required": True, "nullable": True},
        },
    },
    # The migration lock. `IF status != ?` rather than a read of `status` followed by a
    # write: the condition and the write are one Paxos round, so there is no window in
    # which two callers both pass the check. A null `status` satisfies `!=` (verified
    # against Scylla 5.4), which matters because it is null for every VM that has never
    # migrated -- the common case is the null case.
    "/v1/vm/migrate-lock": {
        "cql": "UPDATE hydra.vms SET status = ? WHERE name = ? IF status != ?",
        "binds": ("lock", "name", "lock"),
        "fixed": {"lock": MIGRATION_LOCK},
        "params": {
            "name": {"type": "text", "required": True},
        },
    },
    # Only the holder may release it, so a late cleanup from a failed attempt cannot
    # unlock a migration that has since started.
    "/v1/vm/migrate-unlock": {
        "cql": "UPDATE hydra.vms SET status = ? WHERE name = ? IF status = ?",
        "binds": ("status", "name", "lock"),
        "fixed": {"lock": MIGRATION_LOCK},
        "params": {
            "name": {"type": "text", "required": True},
            "status": {"type": "text", "default": UNLOCKED_STATUS},
        },
    },
    # Hand-over: move the placement and drop the lock in a single round, conditional on
    # both the source host and the lock still being ours. Two statements would leave a
    # window where the VM is placed on the target with the lock still held, or worse,
    # unlocked while still recorded on the source.
    "/v1/vm/migrate-commit": {
        "cql": (
            "UPDATE hydra.vms SET host_ip = ?, status = ? WHERE name = ? "
            "IF host_ip = ? AND status = ?"
        ),
        "binds": ("host_ip", "status", "name", "expected_host_ip", "lock"),
        "fixed": {"lock": MIGRATION_LOCK},
        "params": {
            "name": {"type": "text", "required": True},
            "host_ip": {"type": "text", "required": True},
            "expected_host_ip": {"type": "text", "required": True, "nullable": True},
            "status": {"type": "text", "default": UNLOCKED_STATUS},
        },
    },
    # Registration. INSERT is an upsert in CQL, so a create that reused a live VM's name
    # silently reset its placement to unplaced -- after which anything was free to start
    # a second copy of it. The columns are listed explicitly on purpose: Scylla accepts
    # "INSERT INTO ... JSON ? IF NOT EXISTS" and then ignores the condition, returning no
    # [applied] column and overwriting the row.
    "/v1/vm/create": {
        "cql": _VM_CREATE_CQL,
        "binds": _VM_COLUMNS,
        "params": {
            "name": {"type": "text", "required": True},
            "vcpu": {"type": "int", "default": 1},
            "memory": {"type": "int", "default": 1024},
            "disk_path": {"type": "text", "default": ""},
            "disk_size": {"type": "int", "default": 0},
            "state": {"type": "text", "default": "Stopped"},
            "host_ip": {"type": "text", "default": ""},
            "disks_list": {"type": "text", "default": "NONE"},
            "firmware": {"type": "text", "default": "uefi"},
            "iso": {"type": "text", "default": ""},
            "boot_device": {"type": "text", "default": ""},
            "network_id": {"type": "text", "default": ""},
            "cpu_model": {"type": "text", "default": ""},
            "audio_enabled": {"type": "bool", "default": False},
            "status": {"type": "text", "default": None, "nullable": True},
        },
    },
    # Claiming a scheduler tick. `last_run_epoch` is the schedule's clock and its lock at
    # once: a scheduler reads it, decides the job is due, and writes the current time back.
    # Blind, that read-decide-write is a double submission waiting for two schedulers to
    # run at once -- which is not hypothetical, because leadership here is decided by
    # probing ZooKeeper's four-letter `stat` and falling back to "lowest node with 9091
    # open". A partitioned or slow ZooKeeper gives two nodes that answer at the same
    # instant, both submit the same backup, the same scrub, the same compaction.
    #
    # `IF last_run_epoch = ?` makes the claim and the clock one Paxos round, so the loser
    # is told the tick is taken and skips it. `expected_last_run_epoch` is required and
    # nullable, with no default: a default would silently match the schedules whose clock
    # has never been written and turn the claim back into the blind write.
    #
    # Two entries rather than one with a table parameter: the table and its key column are
    # part of the statement, and a statement assembled from a request is the thing these
    # endpoints exist to prevent.
    "/v1/schedule/claim-job": {
        "cql": (
            "UPDATE hydra.dagur_schedules SET last_run_epoch = ? "
            "WHERE job_name = ? IF last_run_epoch = ?"
        ),
        "binds": ("last_run_epoch", "job_name", "expected_last_run_epoch"),
        "params": {
            "job_name": {"type": "text", "required": True},
            "last_run_epoch": {"type": "int", "required": True},
            "expected_last_run_epoch": {"type": "int", "required": True, "nullable": True},
        },
    },
    "/v1/schedule/claim-check": {
        "cql": (
            "UPDATE hydra.mimir_schedules SET last_run_epoch = ? "
            "WHERE schedule_name = ? IF last_run_epoch = ?"
        ),
        "binds": ("last_run_epoch", "schedule_name", "expected_last_run_epoch"),
        "params": {
            "schedule_name": {"type": "text", "required": True},
            "last_run_epoch": {"type": "int", "required": True},
            "expected_last_run_epoch": {"type": "int", "required": True, "nullable": True},
        },
    },
    # Host maintenance transitions. Draining a host is a lock on that host: entering it
    # twice starts two evacuations of the same VM set, and a leave racing an in-flight
    # enter marks the host schedulable while its VMs are still being moved off.
    "/v1/node/maintenance": {
        "cql": (
            "UPDATE hydra.nodes SET status = ?, maintenance_mode = ? "
            "WHERE hostname = ? IF status = ?"
        ),
        "binds": ("status", "maintenance_mode", "hostname", "expected_status"),
        "params": {
            "hostname": {"type": "text", "required": True},
            "status": {"type": "text", "required": True},
            "maintenance_mode": {"type": "bool", "required": True},
            "expected_status": {"type": "text", "required": True, "nullable": True},
        },
    },
    # Cluster-wide locks. `IF NOT EXISTS` on one shared row is the only form of exclusion
    # that actually excludes here: the previous "only one host in maintenance at a time"
    # check read every node row and then wrote, so two hosts entering a second apart both
    # saw nobody in maintenance and both went on to stop their local ScyllaDB.
    #
    # The TTL is not optional. A node that dies holding this lock must not wedge
    # maintenance for the whole cluster until somebody notices and deletes the row by
    # hand, because on a cluster that cannot enter maintenance nobody can replace the
    # hardware that died.
    "/v1/lock/acquire": {
        "cql": _LOCK_ACQUIRE_CQL,
        "binds": _LOCK_COLUMNS + ("ttl_seconds",),
        "params": {
            "name": {"type": "text", "required": True},
            "holder": {"type": "text", "required": True},
            # Identifies this acquisition, not this node. Required, with no default: a
            # token every caller shares is not a token.
            "holder_token": {"type": "text", "required": True},
            "reason": {"type": "text", "default": ""},
            "acquired_at_ms": {"type": "int", "required": True},
            "ttl_seconds": {"type": "int", "required": True},
        },
    },
    # Extending a lock the caller still holds. Long operations -- evacuating a host's VMs
    # can run for many minutes -- would otherwise force a TTL long enough to cover the
    # slowest case, which is the same TTL that leaves a dead holder's lock standing.
    "/v1/lock/renew": {
        "cql": (
            "UPDATE hydra.cluster_locks USING TTL ? "
            "SET holder = ?, holder_token = ?, reason = ?, acquired_at_ms = ? "
            "WHERE name = ? IF holder_token = ?"
        ),
        "binds": ("ttl_seconds", "holder", "holder_token", "reason", "acquired_at_ms",
                  "name", "holder_token"),
        "params": {
            "name": {"type": "text", "required": True},
            "holder": {"type": "text", "required": True},
            "holder_token": {"type": "text", "required": True},
            "reason": {"type": "text", "default": ""},
            "acquired_at_ms": {"type": "int", "required": True},
            "ttl_seconds": {"type": "int", "required": True},
        },
    },
    # Conditional on the token, not the holder. A release that matches on holder alone
    # drops the lock a node holds *now* on behalf of an acquisition of its own that
    # expired earlier -- the same late-cleanup flaw daruk.md records against the
    # migration lock, except that here it releases a host that is still shutting down.
    "/v1/lock/release": {
        "cql": "DELETE FROM hydra.cluster_locks WHERE name = ? IF holder_token = ?",
        "binds": ("name", "holder_token"),
        "params": {
            "name": {"type": "text", "required": True},
            "holder_token": {"type": "text", "required": True},
        },
    },

    # ---- The extent-based DFS (Sidon) --------------------------------------------
    # Explicit columns, never `INSERT ... JSON ? IF NOT EXISTS`: that form silently
    # ignores the condition and overwrites the row, which was verified against this
    # cluster's Scylla and is recorded in daruk_technical.md. A vdisk create that
    # quietly overwrote an existing vdisk's (owner, epoch) would hand two daemons the
    # same disk at the same epoch, which is the corruption every other line here exists
    # to prevent.
    "/v1/dfs/vdisk-create": {
        "cql": (
            "INSERT INTO hydra.dfs_vdisks (vdisk_id, container, size_bytes, class, owner, "
            "epoch, drain_seq, extent_bytes, egroup_bytes, created_at_ms, replicas, rf) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
        ),
        "binds": ("vdisk_id", "container", "size_bytes", "class", "owner", "epoch",
                  "drain_seq", "extent_bytes", "egroup_bytes", "created_at_ms",
                  "replicas", "rf"),
        "params": {
            "vdisk_id": {"type": "text", "required": True},
            "container": {"type": "text", "default": "default"},
            "size_bytes": {"type": "int", "required": True},
            "class": {"type": "text", "default": "rw"},
            "owner": {"type": "text", "default": ""},
            "epoch": {"type": "int", "default": 0},
            "drain_seq": {"type": "int", "default": 0},
            "extent_bytes": {"type": "int", "default": 1048576},
            "egroup_bytes": {"type": "int", "default": 4194304},
            "created_at_ms": {"type": "int", "required": True},
            # The write-all set. A list rather than a count: which nodes, not how many,
            # because an append has to reach named hosts and a takeover has to fence them.
            "replicas": {"type": "list", "required": True},
            "rf": {"type": "int", "default": 1},
        },
    },
    # Ownership transfer. Conditional on *both* halves of the pair: owner alone would
    # let a node that legitimately owned the disk two epochs ago re-take it after a
    # round trip it never noticed losing. The refused response names the actual owner
    # and epoch, which is what the loser needs in order to stop.
    "/v1/dfs/claim": {
        "cql": (
            "UPDATE hydra.dfs_vdisks SET owner = ?, epoch = ? WHERE vdisk_id = ? "
            "IF owner = ? AND epoch = ?"
        ),
        "binds": ("owner", "epoch", "vdisk_id", "expected_owner", "expected_epoch"),
        "params": {
            "vdisk_id": {"type": "text", "required": True},
            "owner": {"type": "text", "required": True},
            "epoch": {"type": "int", "required": True},
            "expected_owner": {"type": "text", "required": True, "nullable": True},
            "expected_epoch": {"type": "int", "required": True, "nullable": True},
        },
    },
    # The exactly-once drain. One lightweight transaction per batch -- thousands of
    # guest writes amortise one Paxos round -- conditioned on the epoch as well as the
    # counter, so a deposed owner whose batch is already durable on disk cannot commit
    # it into a map that has moved on. Its egroup bytes become orphans and Purah sweeps
    # them; the journal records stay undrained and the new owner drains them itself.
    "/v1/dfs/drain-commit": {
        "cql": (
            "UPDATE hydra.dfs_vdisks SET drain_seq = ? WHERE vdisk_id = ? "
            "IF drain_seq = ? AND epoch = ?"
        ),
        "binds": ("drain_seq", "vdisk_id", "expected_drain_seq", "expected_epoch"),
        "params": {
            "vdisk_id": {"type": "text", "required": True},
            "drain_seq": {"type": "int", "required": True},
            "expected_drain_seq": {"type": "int", "required": True},
            "expected_epoch": {"type": "int", "required": True},
        },
    },
    # Grow-only, and conditional on the size the caller read. Shrinking a vdisk under a
    # guest discards whatever lived past the new end, which no filesystem survives; the
    # condition additionally stops two concurrent resizes from racing to different sizes
    # and leaving the map disagreeing with what the guest was told.
    "/v1/dfs/vdisk-resize": {
        "cql": "UPDATE hydra.dfs_vdisks SET size_bytes = ? WHERE vdisk_id = ? IF size_bytes = ?",
        "binds": ("size_bytes", "vdisk_id", "expected_size_bytes"),
        "params": {
            "vdisk_id": {"type": "text", "required": True},
            "size_bytes": {"type": "int", "required": True},
            "expected_size_bytes": {"type": "int", "required": True},
        },
    },
    "/v1/dfs/egroup-create": {
        "cql": (
            "INSERT INTO hydra.dfs_egroups (egroup_id, state, node, path, size, "
            "seal_hash, vdisk_hint, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "IF NOT EXISTS"
        ),
        "binds": ("egroup_id", "state", "node", "path", "size", "seal_hash",
                  "vdisk_hint", "created_at_ms"),
        "params": {
            "egroup_id": {"type": "text", "required": True},
            "state": {"type": "text", "default": "open"},
            "node": {"type": "text", "required": True},
            "path": {"type": "text", "required": True},
            "size": {"type": "int", "default": 0},
            "seal_hash": {"type": "text", "default": ""},
            "vdisk_hint": {"type": "text", "default": ""},
            "created_at_ms": {"type": "int", "required": True},
        },
    },
    # Sealed means immutable, so the transition is one-way and conditional on the state
    # it is leaving. open -> sealed, open -> dead, sealed -> dead. Nothing re-opens a
    # sealed group: that is the property that makes repair a checksum comparison and
    # snapshots a map copy.
    # Sealing a vdisk: rw -> immutable, one way, conditional on it still being rw. This
    # is what replaces DRBD's --allow-two-primaries for golden images. That option existed
    # because several hosts attach one image at once and each needed Primary to read it;
    # it is also what made the corruption the fencing work spent weeks defending against.
    # An immutable vdisk cannot express the hazard: writes are refused by class, so any
    # number of hosts may serve reads from it and none of them can write.
    "/v1/dfs/vdisk-seal": {
        "cql": "UPDATE hydra.dfs_vdisks SET class = ? WHERE vdisk_id = ? IF class = ?",
        "binds": ("sealed_class", "vdisk_id", "expected_class"),
        "fixed": {"sealed_class": "immutable"},
        "params": {
            "vdisk_id": {"type": "text", "required": True},
            "expected_class": {"type": "text", "default": "rw"},
        },
    },
    "/v1/dfs/egroup-state": {
        "cql": (
            "UPDATE hydra.dfs_egroups SET state = ?, seal_hash = ?, size = ? "
            "WHERE egroup_id = ? IF state = ?"
        ),
        "binds": ("state", "seal_hash", "size", "egroup_id", "expected_state"),
        "params": {
            "egroup_id": {"type": "text", "required": True},
            "state": {"type": "text", "required": True},
            "seal_hash": {"type": "text", "default": ""},
            "size": {"type": "int", "default": 0},
            "expected_state": {"type": "text", "required": True},
        },
    },
}

_APPLIED_COLUMN = "[applied]"

_prepared_lwt = {}


def _lwt_statement(cql):
    """Prepare once, reuse forever. Values travel bound, never spliced into the text."""
    statement = _prepared_lwt.get(cql)
    if statement is None:
        statement = session.prepare(cql)
        statement.consistency_level = ConsistencyLevel.QUORUM
        # Paxos ballots are resolved at SERIAL. Without it the compare and the swap are
        # not one operation and the endpoint is back to being the blind write it replaced.
        statement.serial_consistency_level = ConsistencyLevel.SERIAL
        _prepared_lwt[cql] = statement
    return statement


def _coerce(op_path, key, spec, value):
    kind = spec["type"]
    if value is None:
        if spec.get("nullable"):
            return None
        raise ValueError(f"{op_path}: parameter '{key}' may not be null")
    if kind == "text":
        if not isinstance(value, str):
            raise ValueError(f"{op_path}: parameter '{key}' must be a string")
        return value
    if kind == "int":
        # bool is a subclass of int in Python, so an unguarded isinstance check would let
        # `true` through as a vcpu count of 1.
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{op_path}: parameter '{key}' must be an integer")
        return value
    if kind == "bool":
        if not isinstance(value, bool):
            raise ValueError(f"{op_path}: parameter '{key}' must be a boolean")
        return value
    if kind == "list":
        # A CQL list<text>. Elements are checked individually rather than trusting the
        # container: a list carrying a dict or a number binds without complaint and lands
        # in the column as something no reader expects, which for the replica set would
        # mean a vdisk whose write-all target cannot be resolved to a host.
        if not isinstance(value, (list, tuple)):
            raise ValueError(f"{op_path}: parameter '{key}' must be a list")
        for element in value:
            if not isinstance(element, str):
                raise ValueError(
                    f"{op_path}: parameter '{key}' must contain only strings")
        return list(value)
    raise ValueError(f"{op_path}: parameter '{key}' has an unsupported type '{kind}'")


def _bind_lwt(op_path, op, params):
    """Turn a request body into the operation's bound values, or raise ValueError."""
    if not isinstance(params, dict):
        raise ValueError(f"{op_path}: request body must be a JSON object of parameters")
    specs = op["params"]
    unknown = sorted(set(params) - set(specs))
    if unknown:
        # Rejected rather than ignored: a misspelt "expected_host_ip" would silently fall
        # back to its default and turn the compare-and-swap into an unconditional claim,
        # which is precisely the bug this endpoint exists to remove.
        raise ValueError(f"{op_path}: unknown parameter(s): {', '.join(unknown)}")
    values = dict(op.get("fixed", {}))
    for key, spec in specs.items():
        if key in params:
            values[key] = _coerce(op_path, key, spec, params[key])
        elif spec.get("required"):
            raise ValueError(f"{op_path}: parameter '{key}' is required")
        else:
            values[key] = spec.get("default")
    return tuple(values[name] for name in op["binds"])


def _split_lwt_result(rows):
    """Return (applied, current) for a conditional statement's result.

    `ResultSet.was_applied` is deliberately not used: it is only readable before the
    result set is iterated and raises RuntimeError afterwards, so any handler that also
    wants the conflicting values has to get the order right or crash. Reading column 0
    positionally has neither constraint.

    On rejection the driver returns the conditioned columns as they stand *now*, which is
    what lets a caller say which host actually owns the VM. All-null values there mean
    either a row that does not exist or a column that was never written; the two are not
    distinguishable from the result alone, which is why callers read the row first.
    """
    names = list(rows.column_names or [])
    materialized = [tuple(row) for row in rows]
    if not names or names[0] != _APPLIED_COLUMN:
        # Scylla accepts some statement forms with an IF clause and then executes them
        # unconditionally (INSERT ... JSON ? IF NOT EXISTS is one: no [applied] column,
        # no rows, and the row is overwritten). Reporting that as applied would claim a
        # compare-and-swap that never took place.
        raise RuntimeError(
            "statement did not execute as a lightweight transaction: no [applied] column")
    if len(materialized) != 1:
        raise RuntimeError(
            f"lightweight transaction returned {len(materialized)} rows, expected 1")
    row = materialized[0]
    current = {name: make_serializable(value) for name, value in zip(names[1:], row[1:])}
    return bool(row[0]), current


class CQLProxyHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_json(self, status, payload):
        body = json.dumps(payload).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(body)

    def handle_lwt(self, op_path):
        op = LWT_OPS[op_path]
        try:
            content_length = int(self.headers.get('Content-Length', 0) or 0)
            raw = self.rfile.read(content_length).decode('utf-8') if content_length else ''
            params = json.loads(raw) if raw.strip() else {}
            binds = _bind_lwt(op_path, op, params)
        except ValueError as e:
            self.send_json(400, {"status": "error", "error": str(e)})
            return
        except Exception as e:
            self.send_json(400, {"status": "error", "error": f"invalid request body: {e}"})
            return

        try:
            rows = session.execute(_lwt_statement(op["cql"]), binds)
            applied, current = _split_lwt_result(rows)
        except Exception as e:
            # Never retried at a weaker consistency level, degradable failure or not: a
            # QUORUM failure on a conditional write means we do not know who owns the row,
            # and guessing is how both sides of a partition come to own the same VM.
            print(f"LWT {op_path} failed: {type(e).__name__}: {e}")
            self.send_json(400, {"status": "error", "error": str(e)})
            return

        # A rejected compare-and-swap is a lost race, not a failure, and is answered 200
        # so that a caller's generic error handling cannot turn a correctly refused claim
        # into a spurious outage. `applied` is the only field that says whether the write
        # happened; `current` carries the values that beat it.
        response = {"status": "success", "applied": applied}
        if not applied:
            response["current"] = current
        self.send_json(200, response)

    def do_POST(self):
        if self.path in LWT_OPS:
            self.handle_lwt(self.path)
            return
        if self.path == '/query':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length).decode('utf-8')
            try:
                try:
                    rows = session.execute(post_data)
                except Exception as e:
                    # A read may be answered from a single replica when the cluster is
                    # degraded: the caller gets possibly-stale data, which is recoverable.
                    #
                    # A write may not. Retrying a mutation at ONE during a partition lets
                    # both sides accept conflicting writes, reconciled afterwards by
                    # last-write-wins timestamp -- which is exactly how two hosts come to
                    # believe they own the same VM. The same applies to lightweight
                    # transactions, whose whole purpose is the compare-and-swap that a
                    # weaker consistency level would discard.
                    if not _is_degradable_failure(e):
                        raise
                    if not _is_read(post_data) or _is_lwt(post_data):
                        print(
                            "QUORUM failed for a mutating statement; refusing to retry at "
                            "ConsistencyLevel.ONE. Surfacing the failure instead: "
                            f"{type(e).__name__}: {e}"
                        )
                        raise
                    print(
                        "QUORUM failed for a read; retrying at ConsistencyLevel.ONE. "
                        f"Results may be stale. ({type(e).__name__})"
                    )
                    from cassandra.query import SimpleStatement
                    statement = SimpleStatement(post_data, consistency_level=ConsistencyLevel.ONE)
                    rows = session.execute(statement)
                result = []
                for row in rows:
                    if hasattr(row, '_asdict'):
                        result.append(row._asdict())
                    elif hasattr(row, '_fields'):
                        result.append(dict(zip(row._fields, row)))
                    else:
                        result.append(list(row))
                
                serializable_result = make_serializable(result)
                response = json.dumps({"status": "success", "rows": serializable_result}).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(response)
            except Exception as e:
                response = json.dumps({"status": "error", "error": str(e)}).encode('utf-8')
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(response)
        else:
            self.send_response(404)
            self.end_headers()

def run():
    server = HTTPServer(('127.0.0.1', 9043), CQLProxyHandler)
    print("Daruk CQL HTTP Proxy listening on 127.0.0.1:9043...")
    server.serve_forever()

if __name__ == '__main__':
    run()
