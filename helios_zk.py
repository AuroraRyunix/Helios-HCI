#!/usr/bin/env python3
"""Minimal ZooKeeper client for Helios (Odin/Zeus).

Why this exists: the rest of the stack talks to ZooKeeper only through the
four-letter-word commands (`stat` over a raw socket), which are read-only server
diagnostics. Publishing cluster state needs the real client protocol -- in particular
*ephemeral* znodes, whose lifetime is bound to a live session, so a node that dies has
its entry removed by the ensemble rather than by anyone noticing and cleaning up.

This repo is stdlib-only by design (no requirements.txt; EL10.2 host image), so kazoo is
not available. This implements the subset of the ZooKeeper 3.x wire protocol Helios
needs: connect/session, ping keepalive, create, exists, get, set, get_children, delete,
and watches -- one reader thread demultiplexing replies and server-pushed events off the
one socket, with every watch re-armed after it fires and after every reconnect.

Wire format notes (all integers are big-endian):
  string : int32 length + UTF-8 bytes            (-1 means null)
  buffer : int32 length + raw bytes              (-1 means null)
  request: int32 frame_len + int32 xid + int32 opcode + payload
  reply  : int32 frame_len + int32 xid + int64 zxid + int32 err + payload
"""

import queue
import re
import socket
import struct
import threading
import time

# -- Which node is the leader -----------------------------------------------------------
#
# Nine daemons each carried their own copy of this loop, and each ran it on its own timer.
# vali's queue worker alone asked every two seconds, and every call into Catalyst asked
# again, so the ensemble was answering roughly eleven `stat` probes a second across the
# cluster -- forever, on an idle cluster. ZooKeeper logs two INFO lines per probe, which
# turned into a permanent log storm: 11 lines/second into the journal, and the CPU to
# ingest them.
#
# The probe is cheap. Asking constantly is not, and there is no reason to: leadership
# changes only at an election, which is measured in seconds at best. A short cache turns
# a hot loop into one probe every `ttl` seconds however many callers there are, and costs
# nothing in correctness -- a caller that acts on a five-second-old leader is in exactly
# the position of a caller whose probe raced an election, which every caller already has
# to tolerate.
#
# Callers keep their own fallbacks. What each daemon does when the leader is missing or
# not serving differs on purpose -- bifrost refuses a split-brain candidate where vali
# picks any node running Catalyst -- and consolidating that would be consolidating
# reasoning, not code. Only the probe is shared.

LEADER_CACHE_SECONDS = 5.0

_LEADER_LOCK = threading.Lock()
_LEADER_CACHE = {"ip": None, "at": 0.0, "key": None}


def leader_ip(ips, ttl=LEADER_CACHE_SECONDS, timeout=0.2, now=None):
    """The address in `ips` the ensemble reports as leader, or None.

    "standalone" counts: a one-node ensemble has no leader to elect and answers that way,
    and every caller means "the node to talk to" rather than "the winner of an election".

    Cached for `ttl` seconds, keyed by the address list so that a membership change is not
    served from a cache built against the old one. `None` is cached too: a cluster with no
    leader is a cluster mid-election, and hammering it with probes is the least useful
    thing to do about that.
    """
    addresses = tuple(ip for ip in (ips or []) if ip)
    if not addresses:
        return None

    moment = time.time() if now is None else now

    with _LEADER_LOCK:
        fresh = moment - _LEADER_CACHE["at"] < ttl
        if fresh and _LEADER_CACHE["key"] == addresses:
            return _LEADER_CACHE["ip"]

    found = None
    for ip in addresses:
        mode = server_mode(ip, timeout=timeout)
        if mode in ("leader", "standalone"):
            found = ip
            break

    with _LEADER_LOCK:
        _LEADER_CACHE["ip"] = found
        _LEADER_CACHE["at"] = moment
        _LEADER_CACHE["key"] = addresses

    return found


def server_mode(ip, port=2181, timeout=0.2):
    """One server's role from its `stat` output: "leader", "follower", "observer",
    "standalone", or None when it did not answer.

    Uncached on purpose. `leader_ip` is the cached question; this is the raw one, for the
    places that want to know about a *particular* server rather than find the leader.
    """
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((ip, port))
        sock.sendall(b"stat")
        # Read until the server closes, rather than taking one recv and hoping.
        #
        # `Mode:` is the *last* line of `stat`, printed after a line per connected client.
        # On this cluster that is 1647 bytes with `Mode:` at byte 1578 behind 22
        # connections, so a single recv(4096) happens to work and a single recv(1024) can
        # never see it -- which is exactly how the node status published zk_leader=False on
        # the actual leader, making the console's OdinLeader marker appear and disappear
        # as clients connected and disconnected.
        #
        # One recv is one TCP segment's worth at best. The size that is "obviously enough"
        # scales with the cluster, so it is not a size to pick: the server closes the
        # connection when it has finished answering, and that is the only reliable end.
        chunks = []
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
            if len(chunks) > 64:  # ~256 KiB; a `stat` reply is never this big.
                break
        reply = b"".join(chunks).decode("utf-8", errors="ignore").lower()
    except Exception:
        return None
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass

    for role in ("leader", "follower", "observer", "standalone"):
        if "mode: " + role in reply:
            return role
    return None


def leader_cache_clear():
    """Forget the cached leader. For tests, and for a caller that has just changed
    membership and knows the answer is stale."""
    with _LEADER_LOCK:
        _LEADER_CACHE["ip"] = None
        _LEADER_CACHE["at"] = 0.0
        _LEADER_CACHE["key"] = None


# -- Who votes ---------------------------------------------------------------------------
#
# ZooKeeper publishes its own membership at /zookeeper/config: one `server.<id>=` line per
# member, and a `version=` in hexadecimal. The znode is world-readable, so this needs no
# credentials, and it is the only honest answer to "who votes right now".
#
# A unit file answers a different question. Before `reconfigEnabled`, the two were the same
# thing because the only way to change membership was to rewrite the unit and restart. With
# reconfiguration on, the unit says what a node would come back with after a restart and
# /zookeeper/config says what the ensemble is doing, and a caller that confuses them is
# reasoning about an ensemble that does not exist.
#
# The role is derived rather than read straight out, in one direction: ZooKeeper always
# writes it (`:participant` or `:observer`), but a member spec that a human wrote may leave
# it off, and an omitted role means participant. Treating that as "no role" would drop a
# voter out of the count -- which is the arithmetic every quorum decision here is built on.

ENSEMBLE_CONFIG_PATH = "/zookeeper/config"

PARTICIPANT = "participant"
OBSERVER = "observer"

_MEMBER_LINE = re.compile(r"^server\.(\d+)\s*=\s*(.+?)\s*$")
# The role is the last field of the address half and never appears anywhere else, so both
# reading it and replacing it are anchored to the end. Searching the whole spec for the
# word instead would read a host called `observer.example.com` as an observer.
_ROLE_SUFFIX = re.compile(r":(?:participant|observer)$")


def parse_ensemble_config(text):
    """The ensemble's membership, from the body of the /zookeeper/config znode.

    Returns {"members": [...], "version": "<hex>"}, where each member is
    {"id": int, "host": str, "spec": str, "role": "participant"|"observer"} in the order
    the config lists them. `version` is the hexadecimal string ZooKeeper wrote, kept as a
    string because that is exactly the form `reconfig -v` wants back.

    `spec` is kept verbatim so that a caller handing membership back to ZooKeeper changes
    only the role and never reconstructs an address or a port it did not choose.
    """
    members = []
    version = None
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("version="):
            version = line.split("=", 1)[1].strip()
            continue
        match = _MEMBER_LINE.match(line)
        if not match:
            continue
        spec = match.group(2)
        address = spec.split(";", 1)[0]
        members.append({
            "id": int(match.group(1)),
            "host": address.split("|", 1)[0].split(":", 1)[0],
            "spec": spec,
            "role": OBSERVER if address.endswith(":" + OBSERVER) else PARTICIPANT,
        })
    return {"members": members, "version": version}


def member_spec_with_role(spec, role):
    """The same member spec with its role set to `role`, and nothing else touched.

    The role sits on the address half, before the `;` that introduces the client address,
    and there is at most one of it however many addresses the member lists.
    """
    address, separator, client = spec.partition(";")
    address = _ROLE_SUFFIX.sub("", address)
    return address + ":" + role + separator + client


def read_ensemble_config(client):
    """Read and parse /zookeeper/config over an existing connection."""
    return parse_ensemble_config(client.get(ENSEMBLE_CONFIG_PATH).decode("utf-8", "replace"))


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
# Every frame the server pushes without being asked carries this xid. A watch firing is
# the only one of them this client cares about.
XID_WATCH_EVENT = -1

# WatcherEvent types. NONE is the session itself changing rather than any one path.
EVENT_NONE = -1
EVENT_NODE_CREATED = 1
EVENT_NODE_DELETED = 2
EVENT_NODE_DATA_CHANGED = 3
EVENT_NODE_CHILDREN_CHANGED = 4

# Keeper states, as they arrive on a session event.
STATE_DISCONNECTED = 0
STATE_SYNC_CONNECTED = 3
STATE_AUTH_FAILED = 4
STATE_CONNECTED_READ_ONLY = 5
STATE_EXPIRED = -112

# What a registered watch is watching. The three are distinct in ZooKeeper: a data watch
# and an exists watch both fire on create/delete/data-change, a child watch only on the
# child list changing, and they are armed by different reads.
WATCH_DATA = "data"
WATCH_EXISTS = "exists"
WATCH_CHILDREN = "children"

# Why a callback is being called. There is no reason for the *initial* arm, because that
# read hands its value straight back to whoever registered the watch; a callback only ever
# sees one of these three.
REASON_EVENT = "event"            # the ensemble fired the watch
REASON_RECONNECTED = "reconnected"  # the session came back and the watch was re-armed
REASON_ERROR = "error"            # re-arming failed; `error` carries why

# Error codes we care about
ERR_OK = 0
ERR_NO_NODE = -101
ERR_NODE_EXISTS = -110
ERR_NOT_EMPTY = -111
ERR_SESSION_EXPIRED = -112

# CreateMode flags
PERSISTENT = 0
EPHEMERAL = 1
# Create flags are a bitmask, so an ephemeral sequential node is 3. Sequential appends a
# ten-digit monotonic counter to the name the caller asked for, assigned by the ensemble --
# which is what makes it usable as a queue ticket or an election ballot.
SEQUENTIAL = 2

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


class WatchEvent(object):
    """What a watch callback is told.

    `value` is the result of the read that re-armed the watch, so a callback never has to
    go and ask: bytes (or None when the node does not exist) for a data watch, a bool for
    an exists watch, a list of names for a child watch. When `reason` is REASON_ERROR the
    re-arm failed, `value` is None and `error` says why.
    """

    __slots__ = ("path", "kind", "reason", "event_type", "state", "value", "error")

    def __init__(self, path, kind, reason, value=None, error=None,
                 event_type=EVENT_NONE, state=STATE_SYNC_CONNECTED):
        self.path = path
        self.kind = kind
        self.reason = reason
        self.event_type = event_type
        self.state = state
        self.value = value
        self.error = error

    def __repr__(self):
        return "<WatchEvent %s %s %s>" % (self.kind, self.path, self.reason)


class Watch(object):
    """A standing interest in one path.

    ZooKeeper watches are one-shot: the server forgets a watch the moment it fires, so
    anything long-lived has to re-arm. This object is the *registration*, which outlives
    both the firing and the session -- the client re-arms it after it fires and again
    after every reconnect, and only `cancel` ends it.
    """

    __slots__ = ("client", "kind", "path", "callback", "active")

    def __init__(self, client, kind, path, callback):
        self.client = client
        self.kind = kind
        self.path = path
        self.callback = callback
        self.active = True

    def cancel(self):
        """Stop re-arming this watch. The ensemble may still deliver one event that was
        already in flight; it is dropped here rather than handed to the callback."""
        self.active = False
        self.client._unregister_watch(self)

    def __repr__(self):
        return "<Watch %s %s%s>" % (self.kind, self.path, "" if self.active else " cancelled")


class ZKClient(object):
    """A ZooKeeper client with a frame demultiplexer, watches, and a ping keepalive.

    One socket carries everything: replies, server-pushed watch events, and ping traffic.
    A dedicated reader thread owns the receive side and routes each frame by its xid --
    `xid >= 0` to whichever thread is waiting for that request, `xid == -1` to the watch
    dispatcher, `XID_PING` to the floor. Senders hold a lock only for the length of a
    `sendall`, so any number of threads can have requests in flight at once.

    That split is what makes watches possible at all. The previous shape held one lock
    across send-and-receive, so the only thread that could read a frame was the one that
    had just sent a request -- and anything it did not recognise, including every watch
    event, had to be discarded to find its own reply.

    Watch callbacks run on a third thread, the dispatcher. They run there rather than on
    the reader because re-arming a watch means issuing a read, and a read waits for a
    reply that only the reader can deliver.
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
        # Held for the duration of a send, and nothing else. `_lock` keeps its name
        # because `close` and the connect path have always taken it.
        self._lock = threading.RLock()
        self._state = threading.Lock()
        self._pending = {}
        self._alive = False
        self._session_id = 0
        self._passwd = b"\x00" * 16
        self._ping_thread = None
        self._reader_thread = None
        self._dispatch_thread = None
        self._events = queue.Queue()
        self._watches = {}
        self._stop = threading.Event()
        self._closing = False
        self.connected_host = None
        self.session_expired = False

    # -- connection ---------------------------------------------------------

    def connect(self, _after_expiry=False):
        """Establish a session against the first reachable host. Returns self.

        Reconnecting the same client object is supported and is how a watch survives a
        session loss: every registration still on the client is re-armed here, and its
        callback is told REASON_RECONNECTED. The ensemble delivers no events for the
        window a client was away, so a reconnect has to be treated as "the value may have
        changed" -- a callback that was told nothing would be a watch that silently
        stopped working, which is worse than polling because nothing looks wrong.
        """
        self._drop_transport()
        # `_drop_transport` raises the "this is deliberate" flag so retiring the old socket
        # does not look like a disconnect to the watches. Lower it again here, or a connect
        # that fails outright would leave the next real failure silent.
        self._closing = False
        last_err = None
        resumed = self._session_id
        for host in self.hosts:
            sock = None
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
                    raise ZKSessionExpired(ERR_SESSION_EXPIRED, "server refused the session")
                # The reader blocks on recv for as long as the session is idle, so the
                # handshake timeout must not outlive the handshake. A request's own
                # deadline is enforced where it waits for its reply, not on the socket.
                sock.settimeout(None)
                self._sock = sock
                self._session_id = session_id
                self._passwd = passwd or b"\x00" * 16
                self.session_timeout_ms = negotiated or self.session_timeout_ms
                self.connected_host = host
                self.session_expired = _after_expiry
                self._closing = False
                self._stop.clear()
                with self._state:
                    self._alive = True
                # The dispatcher first: the reader starts pushing events onto the queue
                # immediately, and the dispatcher owns which queue that is.
                self._start_dispatcher()
                self._start_reader(sock)
                self._start_pinger()
                self._rearm_all()
                return self
            except ZKSessionExpired:
                # The session we asked to resume is gone. A fresh one is the only way
                # back, and the caller has to be able to find out: ephemeral nodes made
                # under the old session -- a published node entry, an election ballot --
                # did not come with it.
                try:
                    sock.close()
                except Exception:
                    pass
                if not resumed:
                    raise
                self._session_id = 0
                self._passwd = b"\x00" * 16
                resumed = 0
                return self.connect(_after_expiry=True)
            except Exception as exc:  # try the next host
                last_err = exc
                try:
                    sock.close()
                except Exception:
                    pass
        raise ZKError(-1, f"could not connect to any ZooKeeper host {self.hosts}: {last_err}")

    def _drop_transport(self):
        """Retire the previous socket and its threads, keeping the watch registrations.

        `connect` calls this first, because the threads are guarded by `is_alive()` and a
        reconnect that left the previous reader running would start a new socket nobody
        reads from -- a client that looks connected, accepts requests, and times out every
        one of them. The registrations deliberately survive: they are what `_rearm_all`
        puts back.
        """
        had_threads = any(t and t.is_alive() for t in
                          (self._reader_thread, self._dispatch_thread, self._ping_thread))
        if self._sock is None and not had_threads:
            return
        self._closing = True        # a deliberate reconnect is not a disconnect to report
        self._stop.set()
        sock = self._sock
        self._sock = None
        if sock:
            for action in (lambda: sock.shutdown(socket.SHUT_RDWR), sock.close):
                try:
                    action()
                except Exception:
                    pass
        self._fail_transport(ZKError(-1, "reconnecting"))
        self._events.put(None)
        current = threading.current_thread()
        for thread in (self._reader_thread, self._dispatch_thread, self._ping_thread):
            if thread is not None and thread is not current:
                thread.join(self.timeout)
        self._reader_thread = None
        self._dispatch_thread = None
        self._ping_thread = None

    def is_connected(self):
        """True while the transport is usable.

        A caller that drives itself off watches has no request in flight to fail, so this
        is how it notices the socket died: there is no other symptom.
        """
        with self._state:
            return self._alive and self._sock is not None

    def close(self):
        self._closing = True
        self._stop.set()
        with self._lock:
            sock = self._sock
            if sock:
                try:
                    self._send(OP_CLOSE, b"")
                except Exception:
                    pass
        if sock:
            # Shut the socket down rather than only closing it: the reader is parked in a
            # blocking recv and a close alone does not necessarily wake it.
            for action in (lambda: sock.shutdown(socket.SHUT_RDWR), sock.close):
                try:
                    action()
                except Exception:
                    pass
        self._fail_transport(ZKError(-1, "client closed"))
        with self._lock:
            self._sock = None
        self._events.put(None)
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

    # -- demultiplexer ------------------------------------------------------

    def _start_reader(self, sock):
        if self._reader_thread and self._reader_thread.is_alive():
            return
        self._reader_thread = threading.Thread(
            target=self._read_loop, args=(sock,), name="zk-reader", daemon=True)
        self._reader_thread.start()

    def _read_loop(self, sock):
        """Own the receive side of the socket and route every frame by its xid."""
        try:
            while not self._stop.is_set():
                frame = self._recv_frame(sock)
                xid, _zxid, err = struct.unpack_from("!iqi", frame, 0)
                if xid == XID_WATCH_EVENT:
                    self._queue_event(frame)
                    continue
                if xid == XID_PING:
                    continue        # the keepalive wants nothing back
                waiter = None
                with self._state:
                    waiter = self._pending.pop(xid, None)
                if waiter is None:
                    continue        # a reply nobody is waiting for any more
                waiter["frame"] = frame
                waiter["err"] = err
                waiter["event"].set()
        except Exception as exc:
            self._fail_transport(exc)
        else:
            self._fail_transport(ZKError(-1, "reader stopped"))

    def _fail_transport(self, exc):
        """Mark the connection dead and wake everyone waiting on it.

        This is the lost-wakeup guard: a request that is parked on its event when the
        socket dies would otherwise wait out its whole timeout, and one that registers
        after the reader has gone would wait forever.
        """
        with self._state:
            was_alive = self._alive
            self._alive = False
            waiters = list(self._pending.values())
            self._pending.clear()
        for waiter in waiters:
            waiter["error"] = exc
            waiter["event"].set()
        if was_alive and not self._closing:
            # Tell the watches the session is down. They are re-armed by `connect`, not
            # from here; whoever owns the client decides when to reconnect.
            self._events.put(("disconnected", exc))

    def _queue_event(self, frame):
        # WatcherEvent: type, state, path -- after the 16-byte reply header.
        event_type, state = struct.unpack_from("!ii", frame, 16)
        path, _ = _unpack_string(frame, 24)
        self._events.put(("event", event_type, state, path))

    def _send(self, opcode, payload, xid=None):
        """Frame and write one request. Caller holds `self._lock`."""
        sock = self._sock
        if sock is None:
            raise ZKError(-1, "not connected")
        if xid is None:
            xid = XID_PING if opcode == OP_PING else self._next_xid()
        body = struct.pack("!ii", xid, opcode) + payload
        sock.sendall(struct.pack("!i", len(body)) + body)
        return xid

    def _request(self, opcode, payload, expect_reply=True, path="/"):
        """Send one request and return (reply_bytes, offset_after_header).

        Safe from any thread, including from inside a watch callback: the xid is
        registered before the bytes go out, and the reader hands the reply back through
        it. Nothing is read from the socket here.
        """
        if opcode == OP_PING:
            # The keepalive is fire-and-forget. Its reply arrives on the shared socket
            # and the reader drops it; the session's health shows up as the reader
            # surviving, not as a round trip anyone waits for.
            with self._lock:
                self._send(OP_PING, payload)
            return b"", 0

        waiter = {"event": threading.Event(), "frame": None, "err": ERR_OK, "error": None}
        with self._lock:
            with self._state:
                if not self._alive or self._sock is None:
                    raise ZKError(-1, "not connected")
                xid = XID_PING if opcode == OP_PING else self._next_xid()
                if expect_reply:
                    self._pending[xid] = waiter
            try:
                self._send(opcode, payload, xid=xid)
            except Exception:
                with self._state:
                    self._pending.pop(xid, None)
                raise
        if not expect_reply:
            return b"", 0

        if not waiter["event"].wait(self.timeout):
            with self._state:
                self._pending.pop(xid, None)
            raise ZKError(-1, f"timed out waiting for a reply to {opcode} on {path}")
        if waiter["error"] is not None:
            error = waiter["error"]
            if isinstance(error, ZKError):
                raise error
            raise ZKError(-1, f"connection lost waiting for {opcode} on {path}: {error}")
        if waiter["err"] != ERR_OK:
            _raise_for(waiter["err"], path)
        return waiter["frame"], 16

    # -- watches ------------------------------------------------------------

    def _start_dispatcher(self):
        if self._dispatch_thread and self._dispatch_thread.is_alive():
            return
        # A queue per dispatcher. Events queued against the session that just died are
        # worthless -- `_rearm_all` re-reads every watched path anyway -- and keeping the
        # old queue risks the new dispatcher consuming the sentinel that stopped the old
        # one and exiting immediately.
        events = queue.Queue()
        self._events = events
        self._dispatch_thread = threading.Thread(
            target=self._dispatch_loop, args=(events,), name="zk-watch", daemon=True)
        self._dispatch_thread.start()

    def _dispatch_loop(self, events):
        # The queue is an argument rather than read off `self`: a dispatcher must keep
        # draining the queue it was started on even after a reconnect has installed a new
        # one, or the sentinel that is supposed to stop it lands somewhere it never looks.
        while True:
            item = events.get()
            if item is None:
                return
            try:
                if item[0] == "event":
                    self._deliver(item[1], item[2], item[3])
                elif item[0] == "disconnected":
                    self._notify_disconnect(item[1])
            except Exception:
                # A callback that raises must not take the dispatcher with it, or every
                # later event on this client is lost without a trace.
                pass

    def _watch_kinds_for(self, event_type):
        if event_type == EVENT_NODE_CHILDREN_CHANGED:
            return (WATCH_CHILDREN,)
        if event_type in (EVENT_NODE_CREATED, EVENT_NODE_DELETED, EVENT_NODE_DATA_CHANGED):
            return (WATCH_DATA, WATCH_EXISTS)
        return ()

    def _deliver(self, event_type, state, path):
        if event_type == EVENT_NONE:
            if state == STATE_EXPIRED:
                # The ensemble says so before it closes the socket. Taking its word for it
                # means `is_connected` is false immediately rather than whenever the close
                # lands, and ephemeral state is already gone either way.
                self._fail_transport(ZKSessionExpired(ERR_SESSION_EXPIRED, "session expired"))
            return
        for kind in self._watch_kinds_for(event_type):
            for watch in self._registered(kind, path):
                self._rearm_and_call(watch, REASON_EVENT, event_type=event_type, state=state)

    def _notify_disconnect(self, exc):
        for watch in self._all_registered():
            self._call(watch, WatchEvent(watch.path, watch.kind, REASON_ERROR,
                                         error=exc, state=STATE_DISCONNECTED))

    def _registered(self, kind, path):
        with self._state:
            return list(self._watches.get((kind, path), ()))

    def _all_registered(self):
        with self._state:
            return [w for group in self._watches.values() for w in group]

    def _register_watch(self, watch):
        with self._state:
            self._watches.setdefault((watch.kind, watch.path), []).append(watch)

    def _unregister_watch(self, watch):
        with self._state:
            group = self._watches.get((watch.kind, watch.path))
            if not group:
                return
            if watch in group:
                group.remove(watch)
            if not group:
                self._watches.pop((watch.kind, watch.path), None)

    def _arm(self, watch):
        """Issue the read that leaves the watch set, and return what it saw."""
        if watch.kind == WATCH_CHILDREN:
            return self.get_children(watch.path, watch=True)
        if watch.kind == WATCH_EXISTS:
            return self.exists(watch.path, watch=True)
        try:
            return self.get(watch.path, watch=True)
        except ZKNoNode:
            # A getData that fails leaves no watch behind -- the server registers it only
            # on the success path -- so a data watch on a path that does not exist yet
            # would arm once and never fire. An exists watch does get registered on a
            # missing node, and fires on the create, which is the event the caller wants.
            self.exists(watch.path, watch=True)
            return None

    def _rearm_and_call(self, watch, reason, event_type=EVENT_NONE, state=STATE_SYNC_CONNECTED):
        if not watch.active:
            return
        try:
            value = self._arm(watch)
        except (ZKError, OSError) as exc:
            self._call(watch, WatchEvent(watch.path, watch.kind, REASON_ERROR,
                                         error=exc, event_type=event_type, state=state))
            return
        self._call(watch, WatchEvent(watch.path, watch.kind, reason, value=value,
                                     event_type=event_type, state=state))

    @staticmethod
    def _call(watch, event):
        if not watch.active:
            return
        try:
            watch.callback(event)
        except Exception:
            pass

    def _rearm_all(self):
        """Re-arm every registration against a freshly connected session.

        On its own thread, not inline: the reads it issues need the reader thread `connect`
        has only just started, and a callback must never run on the thread that is still
        inside `connect` -- a caller whose callback reconnects would recurse into it.
        """
        watches = self._all_registered()
        if not watches:
            return

        def rearm():
            for watch in watches:
                self._rearm_and_call(watch, REASON_RECONNECTED)

        threading.Thread(target=rearm, name="zk-rearm", daemon=True).start()

    def _watch(self, kind, path, callback):
        """Register a watch and arm it. Returns (watch, what the arming read saw).

        Registered before it is armed, so an event the ensemble sends the moment the read
        lands has somewhere to be delivered. Unregistered again if the arming read fails,
        because a registration that was never armed would be re-armed on the next reconnect
        and look like a watch that works.
        """
        watch = Watch(self, kind, path, callback)
        self._register_watch(watch)
        try:
            return watch, self._arm(watch)
        except Exception:
            self._unregister_watch(watch)
            raise

    def watch_data(self, path, callback):
        """Call `callback` whenever `path` is created, deleted or written.

        Returns the Watch. Use `watch_data_now` when the current value is wanted too: the
        arming read has already fetched it, so taking it from there costs nothing where a
        second `get` costs a round trip.
        """
        return self._watch(WATCH_DATA, path, callback)[0]

    def watch_data_now(self, path, callback):
        """`watch_data`, also returning what the arming read saw.

        A caller about to act on the value needs it now rather than at the first change.
        Returns (watch, value); value is None when the node does not exist.
        """
        return self._watch(WATCH_DATA, path, callback)

    def watch_children(self, path, callback):
        """Call `callback` whenever the child list of `path` changes. Returns
        (watch, children)."""
        return self._watch(WATCH_CHILDREN, path, callback)

    def watch_exists(self, path, callback):
        """Call `callback` whenever `path` appears or disappears. Returns
        (watch, exists)."""
        return self._watch(WATCH_EXISTS, path, callback)

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

    def create(self, path, data=b"", ephemeral=False, makepath=False, sequential=False):
        """Create a znode. Returns the created path.

        `sequential` makes the ensemble append a ten-digit counter to the name, so the
        returned path is not the one that was asked for -- callers must use the return
        value. Combined with `ephemeral` this is the standard election ballot: the node
        disappears when the session ends, and the counter decides whose turn it was.
        """
        if makepath:
            self.ensure_path(path.rsplit("/", 1)[0] or "/")
        acl = struct.pack("!i", len(_ACL_OPEN_UNSAFE))
        for perms, scheme, ident in _ACL_OPEN_UNSAFE:
            acl += struct.pack("!i", perms) + _pack_string(scheme) + _pack_string(ident)
        payload = _pack_string(path) + _pack_buffer(data) + acl
        flags = (EPHEMERAL if ephemeral else PERSISTENT) | (SEQUENTIAL if sequential else 0)
        payload += struct.pack("!i", flags)
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

    def exists(self, path, watch=False):
        """True when the node exists.

        `watch=True` leaves a watch that fires when the node is created or deleted. Unlike
        `get`, this registers the watch whether or not the node is there, which is the only
        way to be told about a path that does not exist yet.
        """
        try:
            self._request(OP_EXISTS, _pack_string(path) + struct.pack("!?", bool(watch)), path=path)
            return True
        except ZKNoNode:
            return False

    def get(self, path, watch=False):
        """Return the node's data as bytes.

        `watch=True` leaves a watch that fires when the node is written or deleted -- but
        only if this read succeeds; a getData that raises leaves no watch behind. Callers
        that want to hear about a path appearing want `exists(watch=True)`, which is what
        the watch machinery falls back to.
        """
        reply, off = self._request(
            OP_GET_DATA, _pack_string(path) + struct.pack("!?", bool(watch)), path=path)
        data, _ = _unpack_buffer(reply, off)
        return data or b""

    def set(self, path, data, version=-1):
        payload = _pack_string(path) + _pack_buffer(data) + struct.pack("!i", version)
        self._request(OP_SET_DATA, payload, path=path)

    def get_children(self, path, watch=False):
        """The node's child names. `watch=True` leaves a watch that fires when a child is
        added or removed -- not when a child's data changes."""
        reply, off = self._request(
            OP_GET_CHILDREN, _pack_string(path) + struct.pack("!?", bool(watch)), path=path)
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


# -- Per-service leadership ---------------------------------------------------------------
#
# Which node runs a leader-only job is a question per job, and nothing to do with which
# node happens to lead the ZooKeeper ensemble.
#
# Helios used to answer it by comparing addresses: `vali` decided it was the queue worker
# when `helios_zk.leader_ip(ips) == LOCAL_IP`. That has three problems, and they are all
# the same problem. The ensemble elects a leader for its own reasons -- a restart, a
# network blip, a rolling upgrade -- and every leader-only workload in the cluster moves
# at once when it does. One node runs all of them, so the busiest node is also the only
# one doing coordination work. And "am I the leader" is decided by a string comparison
# against a cached probe, which is true or false some seconds after the fact.
#
# This is the standard ZooKeeper recipe instead, and it is what a Nutanix cluster does:
# a persistent parent per service, and one ephemeral sequential child per candidate. The
# lowest sequence number is the leader. Nobody announces anything and nobody times anyone
# out -- the ballot is tied to the session, so a process that dies, hangs long enough to
# lose its session, or is partitioned away stops being the leader because its node is
# gone, not because somebody noticed.
#
# Correctness here does not depend on watches. The lowest-sequence rule holds whenever it
# is evaluated, so asking periodically is correct and only costs latency; a watch makes
# handover prompt rather than eventual. `Election.watch` is that, and it is strictly
# additive: a caller that never calls it behaves exactly as before.
LEADERS_ROOT = "/helios/leaders"

# The prefix for a ballot. The ensemble appends ten digits to it.
_BALLOT_PREFIX = "n_"


def _ballot_sequence(name):
    """The counter out of a ballot name, or None if it does not look like one."""
    tail = name.rsplit(_BALLOT_PREFIX, 1)[-1]
    try:
        return int(tail)
    except ValueError:
        return None


def lowest_ballot(children):
    """The winning ballot name, or None when nobody is standing.

    Sorted by the numeric counter rather than by string, because ZooKeeper's ten-digit
    zero padding is only reliable until the counter rolls past it.
    """
    standing = [(seq, name) for seq, name in
                ((_ballot_sequence(n), n) for n in children) if seq is not None]
    if not standing:
        return None
    return min(standing)[1]


class Election(object):
    """One candidacy for one service.

    Not a context manager by accident: a daemon holds its candidacy for its whole life, and
    the thing that ends it is the process ending. `resign` exists for a clean shutdown and
    for tests; losing the session does the same job without being asked.
    """

    def __init__(self, client, service, identity=b""):
        self.client = client
        self.service = service
        self.identity = identity if isinstance(identity, bytes) else str(identity).encode()
        self.parent = "%s/%s" % (LEADERS_ROOT, service)
        self.ballot = None

    def stand(self):
        """Create this candidate's ballot. Idempotent per instance."""
        if self.ballot is not None:
            return self.ballot
        self.client.ensure_path(self.parent)
        created = self.client.create(
            "%s/%s" % (self.parent, _BALLOT_PREFIX), data=self.identity,
            ephemeral=True, sequential=True)
        self.ballot = created.rsplit("/", 1)[-1]
        return self.ballot

    def is_leader(self):
        """True when this candidate holds the lowest ballot.

        False -- never an exception -- when the answer cannot be established: no ballot
        yet, the parent gone, the ensemble unreachable. A daemon that cannot tell whether
        it leads must not act as though it does, because the alternative is two nodes
        draining one queue.
        """
        if self.ballot is None:
            return False
        try:
            children = self.client.get_children(self.parent)
        except (ZKError, OSError):
            # An unreachable ensemble or a missing parent means "cannot tell", which must
            # read as "not the leader". Deliberately narrow: a blanket catch here returned
            # False for a mistyped call too, and that is how leader_identity shipped
            # broken -- it swallowed its own ValueError and looked like an empty ballot.
            return False
        if self.ballot not in children:
            # The session took the ballot with it. Standing again is the caller's business;
            # pretending to lead is not.
            return False
        return lowest_ballot(children) == self.ballot

    def leader_identity(self):
        """Whatever the current leader wrote in its ballot, or None.

        This is how a follower finds the leader -- by reading it, rather than by inferring
        it from an address it probed separately.
        """
        try:
            children = self.client.get_children(self.parent)
            winner = lowest_ballot(children)
            if winner is None:
                return None
            return self.client.get("%s/%s" % (self.parent, winner))
        except (ZKError, OSError):
            return None

    def watch(self, callback):
        """Call `callback(is_leader)` whenever the standing set changes.

        Additive: `stand`/`is_leader`/`leader_identity`/`resign` are unchanged and still
        correct without this. What it buys is promptness -- the survivor of a lost session
        learns it leads when the ballot disappears rather than at its next poll.

        This watches the parent's children rather than only the predecessor ballot. The
        predecessor watch is the textbook form because it makes exactly one candidate wake
        per handover; the child watch wakes all of them. With three candidates per service
        that difference is two extra `get_children` calls per election, against having to
        re-pick a predecessor every time the set changes -- which is where the subtle bug
        in this recipe lives. Promptness is the point here, not herd size.

        Returns the Watch, or None if this client cannot watch (a test double, or an older
        deployed copy of this module).
        """
        watcher = getattr(self.client, "watch_children", None)
        if watcher is None:
            return None
        watch, _children = watcher(self.parent, lambda _event: callback(self.is_leader()))
        return watch

    def resign(self):
        if self.ballot is None:
            return
        try:
            self.client.delete("%s/%s" % (self.parent, self.ballot))
        except (ZKError, OSError):
            # Already gone, or the ensemble is unreachable. Either way the ballot is not
            # ours any more: it is ephemeral, so the session ending finishes the job.
            pass
        finally:
            self.ballot = None

    def standing(self):
        """True when this candidate's ballot is still in the parent.

        Separate from `is_leader` because the two false answers are different work for the
        caller. "I am standing and somebody else is first" needs nothing; "my ballot is
        gone" needs a new one, and a daemon that cannot tell those apart stops leading for
        the rest of its life the first time its session drops. False when the answer cannot
        be established, which keeps the caller from tearing down a candidacy it still holds
        over a momentary read failure.
        """
        if self.ballot is None:
            return False
        try:
            return self.ballot in self.client.get_children(self.parent)
        except (ZKError, OSError):
            return False


# -- A candidacy that outlives its session ------------------------------------------------
#
# `Election` is the recipe. This is what a daemon needs wrapped around it, and the reason
# it is here rather than copied into eight loops.
#
# A Helios daemon runs for months. Its ZooKeeper session does not: the ensemble restarts,
# a rolling upgrade moves the ensemble leader, a link blips, the host pauses long enough
# to miss its pings. The ballot is ephemeral, so when the session goes the candidacy goes
# with it -- and a process that does not notice is not merely wrong for a moment, it never
# leads again for as long as it runs. That is the quiet half of the failure the address
# comparison had: at least `leader_ip(ips) == LOCAL_IP` started answering True again when
# ZooKeeper came back.
#
# So `leading()` is the whole interface, and it is allowed to do work: connect if it is
# not connected, stand if it is not standing, and answer the one question the caller has.
# Three rules make it safe to call from a two-second loop:
#
#   * False means "I could not establish that I lead". Never an exception, and never True
#     on a guess -- the thing on the other side of the answer is a queue being drained,
#     and two drainers is worse than none.
#   * A lost ballot is noticed and reported as False *before* a new one is created, so a
#     stale candidate never acts in the window where it has stopped leading. Standing again
#     puts it at the back, which is where a returning candidate belongs.
#   * A reconnect gets a *new* session rather than resuming the old one. Resuming would
#     bring the old ballot back and leave the daemon holding two, which is harmless for
#     correctness and a permanent leak.
#
# A caller that only wants to know *who* leads a service never calls `leading()`: it reads
# `leader_identity()`, which needs a session and no ballot. That is how a submitter finds
# the node holding a queue without standing for the job of draining it.

# A failed connect is retried on a timer rather than on every pass. A down ensemble is the
# case where every daemon in the cluster is looping, and the last thing it needs is nine
# processes opening sockets to it as fast as their loops allow.
CANDIDACY_RETRY_SECONDS = 5.0

# `leader_identity` is read per submission in some callers, where `leading()` is read once
# per loop. Same reasoning as `leader_ip`'s cache: leadership changes at an election, so a
# few seconds of staleness is the position every caller is in anyway.
IDENTITY_CACHE_SECONDS = 3.0


class Candidacy(object):
    """One daemon's standing candidacy for one service, maintained across reconnects.

    `service` is the name of a *job*, not of a daemon: two leader-only loops in one process
    take two candidacies, because funnelling them through one name rebuilds exactly the
    coupling this replaces.
    """

    def __init__(self, service, identity=b"", hosts=("127.0.0.1",), port=2181,
                 session_timeout_ms=15000, retry_seconds=CANDIDACY_RETRY_SECONDS,
                 identity_ttl=IDENTITY_CACHE_SECONDS, connect=None, now=None):
        self.service = service
        self.identity = identity if isinstance(identity, bytes) else str(identity).encode()
        self.hosts = [host for host in (hosts or []) if host] or ["127.0.0.1"]
        self.port = port
        self.session_timeout_ms = session_timeout_ms
        self.retry_seconds = retry_seconds
        self.identity_ttl = identity_ttl
        self._connect = connect
        self._now = now or time.time
        self._client = None
        self._election = None
        self._blocked_until = 0.0
        self._identity_cache = None
        self._identity_at = 0.0
        self._lock = threading.RLock()

    # -- the two questions ---------------------------------------------------

    def leading(self):
        """True only when this process holds the lowest ballot for the service."""
        with self._lock:
            if not self._ballot():
                return False
            leads = self._election.is_leader()
            if not leads and not self._election.standing():
                # The ballot is gone, which means the session is. Report not-leading now
                # and stand again on the next pass with a fresh session.
                self._drop()
            return leads

    def leader_identity(self):
        """What the current leader of the service published, or None.

        Does not stand, so a process may ask who leads a job it does not do.
        """
        with self._lock:
            moment = self._now()
            if self._identity_cache is not None and moment - self._identity_at < self.identity_ttl:
                return self._identity_cache
            if not self._session():
                return None
            found = self._election_view().leader_identity()
            if found is None:
                # Nobody standing and an unreachable ensemble are the same answer here, so
                # neither is cached as though it were known.
                return None
            self._identity_cache = found
            self._identity_at = moment
            return found

    # -- lifecycle ----------------------------------------------------------

    def withdraw(self):
        """Give up the ballot but keep the session, so this may stand again later.

        For a candidate whose fitness is conditional on something local -- Bifrost holds
        the VIP only while this node is actually serving the ingress port -- where the
        honest move is to stop being a candidate rather than to win and decline.
        """
        with self._lock:
            if self._election is not None:
                self._election.resign()

    def close(self):
        with self._lock:
            self.withdraw()
            self._drop(block=False)

    # -- internals ----------------------------------------------------------

    def _open(self):
        if self._connect is not None:
            return self._connect()
        return ZKClient(hosts=self.hosts, port=self.port,
                        session_timeout_ms=self.session_timeout_ms).connect()

    def _session(self):
        if self._client is not None:
            return True
        if self._now() < self._blocked_until:
            return False
        try:
            self._client = self._open()
        except (ZKError, OSError):
            self._drop()
            return False
        return True

    def _ballot(self):
        if self._election is not None and self._election.ballot is not None:
            return True
        if not self._session():
            return False
        try:
            election = Election(self._client, self.service, identity=self.identity)
            election.stand()
        except (ZKError, OSError):
            self._drop()
            return False
        self._election = election
        return True

    def _election_view(self):
        """An Election used only to read. It never stands, so it has no ballot and
        `is_leader` on it would always be False -- which is why nothing calls it."""
        if self._election is not None:
            return self._election
        return Election(self._client, self.service, identity=self.identity)

    def _drop(self, block=True):
        client, self._client, self._election = self._client, None, None
        self._identity_cache = None
        self._identity_at = 0.0
        if block:
            self._blocked_until = self._now() + self.retry_seconds
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def cluster_candidacy(service, identity, hosts=None, cluster_file="/etc/hci/cluster.json",
                      **kwargs):
    """A Candidacy against the addresses in the cluster document.

    Every daemon that stands for something reads the same file to find the ensemble, and
    none of them should fail to stand because the read of it was spelled slightly
    differently. Loopback is the fallback, which is correct for a single node and is the
    only thing available before the cluster document exists.
    """
    addresses = [ip for ip in (hosts or []) if ip]
    if not addresses:
        try:
            import json

            with open(cluster_file, "r") as handle:
                addresses = [host.get("ip") for host in json.load(handle).get("hosts", [])
                             if host.get("ip")]
        except Exception:
            addresses = []
    return Candidacy(service, identity=identity, hosts=addresses or ["127.0.0.1"], **kwargs)


# The service names in use. Collected here because the string is the contract between the
# daemon that stands and anyone reading `/helios/leaders` to find out who won, and a typo
# in one copy is a second election nobody notices.
SERVICE_CATALYST_DISPATCH = "catalyst-dispatch"
SERVICE_CATALYST_SCHEDULER = "catalyst-scheduler"
SERVICE_VALI_QUEUE = "vali-queue"
SERVICE_VALI_DRS = "vali-drs"
SERVICE_DAGUR_QUEUE = "dagur-queue"
SERVICE_LANAYRU_QUEUE = "lanayru-queue"
SERVICE_MIMIR_SCHEDULES = "mimir-schedules"
SERVICE_HYLIA_UPGRADES = "hylia-upgrades"
SERVICE_MIPHA_HA = "mipha-ha"
SERVICE_BIFROST_VIP = "bifrost-vip"
