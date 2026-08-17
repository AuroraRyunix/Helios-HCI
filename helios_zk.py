#!/usr/bin/env python3
"""Minimal ZooKeeper client for Helios (Odin/Zeus).

Why this exists: the rest of the stack talks to ZooKeeper only through the
four-letter-word commands (`stat` over a raw socket), which are read-only server
diagnostics. Publishing cluster state needs the real client protocol -- in particular
*ephemeral* znodes, whose lifetime is bound to a live session, so a node that dies has
its entry removed by the ensemble rather than by anyone noticing and cleaning up.

This repo is stdlib-only by design (no requirements.txt; EL10.2 host image), so kazoo is
not available. This implements the subset of the ZooKeeper 3.x wire protocol Helios
needs: connect/session, ping keepalive, create, exists, get, set, get_children, delete.

Wire format notes (all integers are big-endian):
  string : int32 length + UTF-8 bytes            (-1 means null)
  buffer : int32 length + raw bytes              (-1 means null)
  request: int32 frame_len + int32 xid + int32 opcode + payload
  reply  : int32 frame_len + int32 xid + int64 zxid + int32 err + payload
"""

import socket
import struct
import threading
import time

# Opcodes
OP_CREATE = 1
OP_DELETE = 2
OP_EXISTS = 3
OP_GET_DATA = 4
OP_SET_DATA = 5
OP_GET_CHILDREN = 8
OP_PING = 11
OP_CLOSE = -11

XID_PING = -2

# Error codes we care about
ERR_OK = 0
ERR_NO_NODE = -101
ERR_NODE_EXISTS = -110
ERR_NOT_EMPTY = -111
ERR_SESSION_EXPIRED = -112

# CreateMode flags
PERSISTENT = 0
EPHEMERAL = 1

# ACL: world:anyone with all permissions (0x1f). Access control is handled by the
# network boundary here, exactly as the existing 4lw usage assumes.
_ACL_OPEN_UNSAFE = [(0x1F, "world", "anyone")]


class ZKError(Exception):
    """A ZooKeeper server-side error, carrying the protocol error code."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


class ZKNoNode(ZKError):
    pass


class ZKNodeExists(ZKError):
    pass


class ZKSessionExpired(ZKError):
    pass


def _pack_string(value):
    if value is None:
        return struct.pack("!i", -1)
    raw = value.encode("utf-8")
    return struct.pack("!i", len(raw)) + raw


def _pack_buffer(value):
    if value is None:
        return struct.pack("!i", -1)
    if isinstance(value, str):
        value = value.encode("utf-8")
    return struct.pack("!i", len(value)) + bytes(value)


def _unpack_string(buf, offset):
    (length,) = struct.unpack_from("!i", buf, offset)
    offset += 4
    if length < 0:
        return None, offset
    return buf[offset:offset + length].decode("utf-8", "replace"), offset + length


def _unpack_buffer(buf, offset):
    (length,) = struct.unpack_from("!i", buf, offset)
    offset += 4
    if length < 0:
        return None, offset
    return buf[offset:offset + length], offset + length


def _raise_for(code, path):
    if code == ERR_NO_NODE:
        raise ZKNoNode(code, f"node does not exist: {path}")
    if code == ERR_NODE_EXISTS:
        raise ZKNodeExists(code, f"node already exists: {path}")
    if code == ERR_SESSION_EXPIRED:
        raise ZKSessionExpired(code, "session expired")
    raise ZKError(code, f"ZooKeeper error {code} on {path}")


class ZKClient(object):
    """A single-threaded-request ZooKeeper client with a background ping keepalive.

    Requests are serialized under a lock, so it is safe to share one client between the
    publisher loop and ad-hoc reads.
    """

    def __init__(self, hosts=("127.0.0.1",), port=2181, timeout=10.0, session_timeout_ms=15000):
        if isinstance(hosts, str):
            hosts = [hosts]
        self.hosts = list(hosts)
        self.port = port
        self.timeout = timeout
        self.session_timeout_ms = session_timeout_ms
        self._sock = None
        self._xid = 0
        self._lock = threading.RLock()
        self._session_id = 0
        self._passwd = b"\x00" * 16
        self._ping_thread = None
        self._stop = threading.Event()
        self.connected_host = None

    # -- connection ---------------------------------------------------------

    def connect(self):
        """Establish a session against the first reachable host. Returns self."""
        last_err = None
        for host in self.hosts:
            try:
                sock = socket.create_connection((host, self.port), timeout=self.timeout)
                sock.settimeout(self.timeout)
                # ConnectRequest: protocolVersion, lastZxidSeen, timeOut, sessionId, passwd
                body = struct.pack("!iqiq", 0, 0, self.session_timeout_ms, self._session_id)
                body += _pack_buffer(self._passwd)
                body += struct.pack("!?", False)  # readOnly
                sock.sendall(struct.pack("!i", len(body)) + body)

                reply = self._recv_frame(sock)
                # ConnectResponse: protocolVersion, timeOut, sessionId, passwd
                _, negotiated, session_id = struct.unpack_from("!iiq", reply, 0)
                passwd, _ = _unpack_buffer(reply, 16)
                if session_id == 0:
                    raise ZKError(ERR_SESSION_EXPIRED, "server refused the session")
                self._sock = sock
                self._session_id = session_id
                self._passwd = passwd or b"\x00" * 16
                self.session_timeout_ms = negotiated or self.session_timeout_ms
                self.connected_host = host
                self._stop.clear()
                self._start_pinger()
                return self
            except Exception as exc:  # try the next host
                last_err = exc
                try:
                    sock.close()
                except Exception:
                    pass
        raise ZKError(-1, f"could not connect to any ZooKeeper host {self.hosts}: {last_err}")

    def close(self):
        self._stop.set()
        with self._lock:
            if self._sock:
                try:
                    self._request(OP_CLOSE, b"", expect_reply=False)
                except Exception:
                    pass
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None
        self.connected_host = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, *exc):
        self.close()

    # -- framing ------------------------------------------------------------

    @staticmethod
    def _recv_exactly(sock, count):
        chunks = []
        remaining = count
        while remaining > 0:
            chunk = sock.recv(remaining)
            if not chunk:
                raise ZKError(-1, "connection closed by ZooKeeper")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _recv_frame(self, sock=None):
        sock = sock or self._sock
        (length,) = struct.unpack("!i", self._recv_exactly(sock, 4))
        return self._recv_exactly(sock, length)

    def _next_xid(self):
        self._xid += 1
        return self._xid

    def _request(self, opcode, payload, expect_reply=True, path="/"):
        """Send one request and return (reply_bytes, offset_after_header)."""
        with self._lock:
            if not self._sock:
                raise ZKError(-1, "not connected")
            xid = XID_PING if opcode == OP_PING else self._next_xid()
            body = struct.pack("!ii", xid, opcode) + payload
            self._sock.sendall(struct.pack("!i", len(body)) + body)
            if not expect_reply:
                return b"", 0
            while True:
                reply = self._recv_frame()
                r_xid, _zxid, err = struct.unpack_from("!iqi", reply, 0)
                # Skip notifications (xid -1) and stray pings we did not ask for.
                if r_xid == -1:
                    continue
                if r_xid == XID_PING and opcode != OP_PING:
                    continue
                if err != ERR_OK:
                    _raise_for(err, path)
                return reply, 16

    # -- keepalive ----------------------------------------------------------

    def _start_pinger(self):
        if self._ping_thread and self._ping_thread.is_alive():
            return
        interval = max(1.0, (self.session_timeout_ms / 1000.0) / 3.0)

        def loop():
            while not self._stop.wait(interval):
                try:
                    self._request(OP_PING, b"")
                except Exception:
                    return  # connection is gone; callers will observe it and reconnect

        self._ping_thread = threading.Thread(target=loop, name="zk-ping", daemon=True)
        self._ping_thread.start()

    # -- operations ---------------------------------------------------------

    def create(self, path, data=b"", ephemeral=False, makepath=False):
        """Create a znode. Returns the created path."""
        if makepath:
            self.ensure_path(path.rsplit("/", 1)[0] or "/")
        acl = struct.pack("!i", len(_ACL_OPEN_UNSAFE))
        for perms, scheme, ident in _ACL_OPEN_UNSAFE:
            acl += struct.pack("!i", perms) + _pack_string(scheme) + _pack_string(ident)
        payload = _pack_string(path) + _pack_buffer(data) + acl
        payload += struct.pack("!i", EPHEMERAL if ephemeral else PERSISTENT)
        reply, off = self._request(OP_CREATE, payload, path=path)
        created, _ = _unpack_string(reply, off)
        return created

    def ensure_path(self, path):
        """Create every missing persistent parent of path (idempotent)."""
        parts = [p for p in path.strip("/").split("/") if p]
        current = ""
        for part in parts:
            current += "/" + part
            try:
                self.create(current, b"")
            except ZKNodeExists:
                pass
        return path or "/"

    def exists(self, path):
        try:
            self._request(OP_EXISTS, _pack_string(path) + struct.pack("!?", False), path=path)
            return True
        except ZKNoNode:
            return False

    def get(self, path):
        """Return the node's data as bytes."""
        reply, off = self._request(OP_GET_DATA, _pack_string(path) + struct.pack("!?", False), path=path)
        data, _ = _unpack_buffer(reply, off)
        return data or b""

    def set(self, path, data, version=-1):
        payload = _pack_string(path) + _pack_buffer(data) + struct.pack("!i", version)
        self._request(OP_SET_DATA, payload, path=path)

    def get_children(self, path):
        reply, off = self._request(OP_GET_CHILDREN, _pack_string(path) + struct.pack("!?", False), path=path)
        (count,) = struct.unpack_from("!i", reply, off)
        off += 4
        names = []
        for _ in range(count):
            name, off = _unpack_string(reply, off)
            names.append(name)
        return names

    def delete(self, path, version=-1):
        try:
            self._request(OP_DELETE, _pack_string(path) + struct.pack("!i", version), path=path)
        except ZKNoNode:
            pass

    def upsert_ephemeral(self, path, data):
        """Create the ephemeral node, or update it if this session already owns it."""
        try:
            self.create(path, data, ephemeral=True, makepath=True)
        except ZKNodeExists:
            self.set(path, data)


def connect(hosts=("127.0.0.1",), port=2181, timeout=10.0, session_timeout_ms=15000):
    """Convenience wrapper returning a connected client."""
    return ZKClient(hosts=hosts, port=port, timeout=timeout,
                    session_timeout_ms=session_timeout_ms).connect()
