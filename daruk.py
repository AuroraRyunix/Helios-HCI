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
    if isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [make_serializable(v) for v in obj]
    elif hasattr(obj, 'items'):
        return {str(k): make_serializable(v) for k, v in obj.items()}
    else:
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
