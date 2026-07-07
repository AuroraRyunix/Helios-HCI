#!/usr/bin/env python3
import time
import os
import sys
import socket
import re
import base64
import subprocess

def get_local_ip():
    local_ip = "127.0.0.1"
    try:
        if os.path.exists("/etc/hci/spectrum/spectrum.env"):
            with open("/etc/hci/spectrum/spectrum.env", "r") as f:
                for line in f:
                    if line.startswith("LOCAL_HYPERVISOR_IP="):
                        local_ip = line.split("=", 1)[1].strip()
                        break
    except Exception:
        pass
    return local_ip

def get_cpu_stats():
    try:
        with open("/proc/stat", "r") as f:
            line = f.readline()
        parts = line.strip().split()
        if len(parts) >= 8 and parts[0] == "cpu":
            # user, nice, sys, idle, iowait, irq, softirq, steal
            vals = [float(x) for x in parts[1:9]]
            total = sum(vals)
            # idle + iowait
            idle = vals[3] + vals[4]
            return idle, total
    except Exception:
        pass
    return None

def get_mem_usage():
    """Returns (mem_pct, mem_total_kb) for the current host."""
    try:
        meminfo = {}
        with open("/proc/meminfo", "r") as f:
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    key = parts[0].strip()
                    val_parts = parts[1].strip().split()
                    if val_parts:
                        meminfo[key] = float(val_parts[0])
        total = meminfo.get("MemTotal", 0)
        free = meminfo.get("MemFree", 0)
        buffers = meminfo.get("Buffers", 0)
        cached = meminfo.get("Cached", 0)
        sreclaim = meminfo.get("SReclaimable", 0)
        
        if total > 0:
            used = total - free - buffers - cached - sreclaim
            if used < 0:
                used = total - free
            return (used / total) * 100.0, int(total)
    except Exception:
        pass
    return 0.0, 0

def get_cpu_cores():
    """Returns the number of logical CPU cores on this host."""
    try:
        count = 0
        with open("/proc/cpuinfo", "r") as f:
            for line in f:
                if line.strip().startswith("processor"):
                    count += 1
        return count if count > 0 else 1
    except Exception:
        pass
    return 1

def is_disk(name):
    if re.match(r'^(sd|vd|hd|xvd)[a-z]+$', name):
        return True
    if re.match(r'^nvme[0-9]+n[0-9]+$', name):
        return True
    return False

def get_disk_stats():
    read_ios = 0
    write_ios = 0
    read_sectors = 0
    write_sectors = 0
    try:
        with open("/proc/diskstats", "r") as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 10:
                    name = parts[2]
                    if is_disk(name):
                        read_ios += int(parts[3])
                        read_sectors += int(parts[5])
                        write_ios += int(parts[7])
                        write_sectors += int(parts[9])
        total_ios = read_ios + write_ios
        total_bytes = (read_sectors + write_sectors) * 512
        return total_ios, total_bytes
    except Exception:
        pass
    return 0, 0

def get_net_stats():
    rx_bytes = 0
    tx_bytes = 0
    try:
        with open("/proc/net/dev", "r") as f:
            lines = f.readlines()
        for line in lines[2:]:
            if ":" in line:
                parts = line.split(":", 1)
                iface = parts[0].strip()
                if iface == "lo":
                    continue
                stats = parts[1].split()
                if len(stats) >= 9:
                    rx_bytes += int(stats[0])
                    tx_bytes += int(stats[8])
    except Exception:
        pass
    return rx_bytes, tx_bytes

def get_interface_stats():
    stats = {}
    try:
        with open("/proc/net/dev", "r") as f:
            lines = f.readlines()
        for line in lines[2:]:
            if ":" in line:
                parts = line.split(":", 1)
                iface = parts[0].strip()
                if iface.startswith("vxlan") or iface.startswith("br-ov") or iface.startswith("veth"):
                    vals = parts[1].split()
                    if len(vals) >= 9:
                        stats[iface] = (int(vals[0]), int(vals[1]), int(vals[8]), int(vals[9]))
    except Exception:
        pass
    return stats

def run_cql_query(cql_query, *args, **kwargs):
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
        return p.returncode, stdout.decode('utf-8', errors='ignore').strip(), stderr.decode('utf-8', errors='ignore').strip()
def main():
    print("Logos Telemetry Daemon started.")
    local_ip = get_local_ip()
    print(f"Local IP resolved: {local_ip}")

    # Initial sample
    last_time = time.time()
    last_cpu = get_cpu_stats()
    last_disk_ios, last_disk_bytes = get_disk_stats()
    last_net_rx, last_net_tx = get_net_stats()
    last_iface_stats = get_interface_stats()

    # Sleep a bit to establish baseline
    time.sleep(1)

    while True:
        try:
            curr_time = time.time()
            dt = curr_time - last_time
            if dt <= 0:
                dt = 0.001

            # CPU calculation
            curr_cpu = get_cpu_stats()
            cpu_pct = 0.0
            if curr_cpu and last_cpu:
                curr_idle, curr_total = curr_cpu
                last_idle, last_total = last_cpu
                delta_idle = curr_idle - last_idle
                delta_total = curr_total - last_total
                if delta_total > 0:
                    cpu_pct = max(0.0, min(100.0, 100.0 * (1.0 - delta_idle / delta_total)))
            last_cpu = curr_cpu

            # Memory calculation
            mem_pct, mem_total_kb = get_mem_usage()
            cpu_cores = get_cpu_cores()

            # Disk calculation
            curr_disk_ios, curr_disk_bytes = get_disk_stats()
            disk_iops = 0.0
            disk_bandwidth_kbps = 0.0
            if curr_disk_ios >= last_disk_ios:
                disk_iops = (curr_disk_ios - last_disk_ios) / dt
            if curr_disk_bytes >= last_disk_bytes:
                disk_bandwidth_kbps = ((curr_disk_bytes - last_disk_bytes) / 1024.0) / dt
            last_disk_ios, last_disk_bytes = curr_disk_ios, curr_disk_bytes

            # Network calculation
            curr_net_rx, curr_net_tx = get_net_stats()
            net_rx_kbps = 0.0
            net_tx_kbps = 0.0
            if curr_net_rx >= last_net_rx:
                net_rx_kbps = ((curr_net_rx - last_net_rx) / 1024.0) / dt
            if curr_net_tx >= last_net_tx:
                net_tx_kbps = ((curr_net_tx - last_net_tx) / 1024.0) / dt
            last_net_rx, last_net_tx = curr_net_rx, curr_net_tx

            # Interface specific calculations (tunnels & bridges)
            curr_iface_stats = get_interface_stats()
            timestamp_ms = int(curr_time * 1000)

            statements = []
            for iface, vals in curr_iface_stats.items():
                if iface in last_iface_stats:
                    last_rx_b, last_rx_p, last_tx_b, last_tx_p = last_iface_stats[iface]
                    curr_rx_b, curr_rx_p, curr_tx_b, curr_tx_p = vals
                    
                    rx_kbps = 0.0
                    tx_kbps = 0.0
                    rx_pps = 0.0
                    tx_pps = 0.0
                    
                    if curr_rx_b >= last_rx_b:
                        rx_kbps = ((curr_rx_b - last_rx_b) / 1024.0) / dt
                    if curr_tx_b >= last_tx_b:
                        tx_kbps = ((curr_tx_b - last_tx_b) / 1024.0) / dt
                    if curr_rx_p >= last_rx_p:
                        rx_pps = (curr_rx_p - last_rx_p) / dt
                    if curr_tx_p >= last_tx_p:
                        tx_pps = (curr_tx_p - last_tx_p) / dt
                        
                    cql_iface = f"""
                    INSERT INTO hydra.urbosa_tunnel_metrics (
                        node_ip, interface_name, timestamp,
                        rx_kbps, tx_kbps, rx_packets, tx_packets
                    ) VALUES (
                        '{local_ip}', '{iface}', {timestamp_ms},
                        {rx_kbps:.4f}, {tx_kbps:.4f}, {rx_pps:.4f}, {tx_pps:.4f}
                    );
                    """
                    statements.append(cql_iface.strip())
            last_iface_stats = curr_iface_stats

            last_time = curr_time

            cql = f"""
            INSERT INTO hydra.logos_metrics (
                node_ip, timestamp, cpu_pct, mem_pct, mem_total_kb, cpu_cores,
                disk_iops, disk_bandwidth_kbps, net_rx_kbps, net_tx_kbps
            ) VALUES (
                '{local_ip}', {timestamp_ms}, {cpu_pct:.4f}, {mem_pct:.4f}, {mem_total_kb}, {cpu_cores},
                {disk_iops:.4f}, {disk_bandwidth_kbps:.4f}, {net_rx_kbps:.4f}, {net_tx_kbps:.4f}
            );
            """
            statements.append(cql.strip())
            
            combined_cql = "\n".join(statements)
            rc, out, err = run_cql_query(combined_cql, local_ip)
            if rc != 0:
                print(f"Failed to write batch metrics to ScyllaDB: {err.strip()}")

        except Exception as e:
            print(f"Error in telemetry collection loop: {e}", file=sys.stderr)

        time.sleep(30)

if __name__ == "__main__":
    main()
