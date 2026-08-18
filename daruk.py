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


class CQLProxyHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_POST(self):
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
