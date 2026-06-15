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
            return (used / total) * 100.0
    except Exception:
        pass
    return 0.0

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

def run_cql_query(cql_query, ip_addr):
    try:
        b64_query = base64.b64encode(cql_query.encode('utf-8')).decode('utf-8')
        cmd = f"echo {b64_query} | base64 -d | podman exec -i systemd-hydra-db cqlsh {ip_addr}"
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return res.returncode, res.stdout.decode('utf-8', errors='ignore'), res.stderr.decode('utf-8', errors='ignore')
    except Exception as e:
        return -1, "", str(e)

def main():
    print("Logos Telemetry Daemon started.")
    local_ip = get_local_ip()
    print(f"Local IP resolved: {local_ip}")

    # Initial sample
    last_time = time.time()
    last_cpu = get_cpu_stats()
    last_disk_ios, last_disk_bytes = get_disk_stats()
    last_net_rx, last_net_tx = get_net_stats()

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
            mem_pct = get_mem_usage()

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

            last_time = curr_time

            timestamp_ms = int(curr_time * 1000)

            cql = f"""
            INSERT INTO hydra.logos_metrics (
                node_ip, timestamp, cpu_pct, mem_pct,
                disk_iops, disk_bandwidth_kbps, net_rx_kbps, net_tx_kbps
            ) VALUES (
                '{local_ip}', {timestamp_ms}, {cpu_pct:.4f}, {mem_pct:.4f},
                {disk_iops:.4f}, {disk_bandwidth_kbps:.4f}, {net_rx_kbps:.4f}, {net_tx_kbps:.4f}
            );
            """
            rc, out, err = run_cql_query(cql, local_ip)
            if rc != 0:
                print(f"Failed to write metrics to ScyllaDB: {err.strip()}")

        except Exception as e:
            print(f"Error in telemetry collection loop: {e}", file=sys.stderr)

        time.sleep(10)

if __name__ == "__main__":
    main()
