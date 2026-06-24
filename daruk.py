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

LOCAL_IP = get_local_ip()
print(f"Daruk connecting to ScyllaDB at {LOCAL_IP}...")
cluster = Cluster([LOCAL_IP])
session = cluster.connect()

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
                rows = session.execute(post_data)
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
