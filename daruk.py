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

from cassandra import ConsistencyLevel
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
                    # Dynamic fallback to ConsistencyLevel.ONE if QUORUM fails due to unavailable nodes
                    if "unavailable" in str(e).lower() or "timeout" in str(e).lower() or "active" in str(e).lower():
                        print("QUORUM consistency level failed or database degraded. Falling back to ConsistencyLevel.ONE...")
                        from cassandra.query import SimpleStatement
                        statement = SimpleStatement(post_data, consistency_level=ConsistencyLevel.ONE)
                        rows = session.execute(statement)
                    else:
                        raise e
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
