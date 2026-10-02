#!/usr/bin/env python3
"""A watch that stops firing is worse than a poll, because nothing looks wrong.

`helios_zk` could not watch anything. The frames a watch arrives on were being *read and
thrown away*: one lock was held across send-and-receive, so the only thread allowed to
touch the socket was the one waiting for its own reply, and everything else on the wire --
every notification -- had to be discarded to find it.

So `zk_reconcile_loop` polled `/cluster_state` on a five-second timer and re-asserted every
thirty, and `cluster start` stayed an imperative drive in numbered phases because a
declaration nobody notices for half a minute is not one. A real Zeus ensemble answers this
differently: ninety connections watching three hundred-odd paths, 568 watches, and nothing
polling for state.

What is asserted here is the part that is easy to get wrong, which is not the happy path:

  * frames are **demultiplexed**, so two threads can have requests in flight at once and a
    notification arriving in the middle of one reaches the watch rather than the floor;
  * a watch **re-arms** -- after it fires, because ZooKeeper watches are one-shot, and
    after a reconnect, because the ensemble delivers nothing for the window a client was
    away and a silently-dead watch is the failure this whole change exists to avoid;
  * a dead socket **fails the waiters** rather than parking them until their timeout;
  * the reconcile loop's trigger is the watch, and its timers are a net under it.

Run with:  python -m unittest test_zk_watches
"""

import importlib.util
import io
import os
import re
import socket
import struct
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_helios_zk():
    spec = importlib.util.spec_from_file_location(
        "helios_zk_watches", os.path.join(HERE, "helios_zk.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


zk = load_helios_zk()


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


# -- A ZooKeeper that speaks the wire ----------------------------------------------------
#
# The demultiplexer is framing code, and framing code is only honestly tested against
# frames. This serves the real protocol over loopback: handshake, the six opcodes
# helios_zk sends, one-shot watch registration, and server-pushed notifications.
#
# Every request is handled on its own thread so the test can hold one reply open while
# other traffic flows past it -- which is the whole point of having a demultiplexer and is
# untestable against a server that answers in order.

_STAT = b"\x00" * 68   # the client never parses a Stat; it only has to be there


def _pack_string(value):
    raw = value.encode("utf-8")
    return struct.pack("!i", len(raw)) + raw


def _unpack_string(buf, offset):
    (length,) = struct.unpack_from("!i", buf, offset)
    offset += 4
    if length < 0:
        return None, offset
    return buf[offset:offset + length].decode("utf-8"), offset + length


class FakeEnsemble(object):
    def __init__(self, nodes=None):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(8)
        self.port = self.listener.getsockname()[1]

        self.nodes = dict(nodes or {})
        self.data_watches = {}     # path -> count of armed data/exists watches
        self.child_watches = {}
        self.hold = {}             # path -> Event a read on it waits for
        self.counters = {}         # parent -> next sequential counter
        self.refuse_resume = False
        self.refuse_all = False
        self.connections = 0
        self.pings = 0
        self.requests = []

        self._conn = None
        self._send_lock = threading.Lock()
        self._stop = False
        self._accept = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept.start()

    # -- lifecycle ------------------------------------------------------

    def stop(self):
        self._stop = True
        for sock in (self._conn, self.listener):
            try:
                sock.close()
            except Exception:
                pass

    def drop_connection(self):
        """What a server restart looks like from the client: the socket just goes."""
        conn = self._conn
        self._conn = None
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    def _accept_loop(self):
        while not self._stop:
            try:
                conn, _addr = self.listener.accept()
            except Exception:
                return
            self.connections += 1
            previous = self._conn
            self._conn = conn
            if previous is not None:
                try:
                    previous.close()
                except Exception:
                    pass
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    # -- framing --------------------------------------------------------

    @staticmethod
    def _recv_exactly(conn, count):
        chunks = []
        while count > 0:
            chunk = conn.recv(count)
            if not chunk:
                raise EOFError
            chunks.append(chunk)
            count -= len(chunk)
        return b"".join(chunks)

    def _recv_frame(self, conn):
        (length,) = struct.unpack("!i", self._recv_exactly(conn, 4))
        return self._recv_exactly(conn, length)

    def _send(self, conn, body):
        with self._send_lock:
            conn.sendall(struct.pack("!i", len(body)) + body)

    def _reply(self, conn, xid, err=0, payload=b""):
        self._send(conn, struct.pack("!iqi", xid, 1, err) + payload)

    def _notify(self, event_type, path):
        conn = self._conn
        if conn is None:
            return
        body = struct.pack("!iqi", -1, 1, 0)
        body += struct.pack("!ii", event_type, zk.STATE_SYNC_CONNECTED) + _pack_string(path)
        try:
            self._send(conn, body)
        except Exception:
            pass

    # -- the server -----------------------------------------------------

    def _serve(self, conn):
        try:
            handshake = self._recv_frame(conn)
            (_proto, _zxid, timeout, session) = struct.unpack_from("!iqiq", handshake, 0)
            if self.refuse_all or (session and self.refuse_resume):
                # A refused resume is how the wire says "your session expired": session id
                # zero and a null password.
                self._send(conn, struct.pack("!iiq", 0, timeout, 0) + struct.pack("!i", -1))
                return
            self._send(conn, struct.pack("!iiq", 0, timeout, 0x5E55 + self.connections)
                       + struct.pack("!i", 16) + b"p" * 16)
            while not self._stop:
                frame = self._recv_frame(conn)
                threading.Thread(target=self._handle, args=(conn, frame), daemon=True).start()
        except Exception:
            return

    def _handle(self, conn, frame):
        xid, opcode = struct.unpack_from("!ii", frame, 0)
        try:
            self._dispatch(conn, xid, opcode, frame)
        except Exception:
            pass

    def _dispatch(self, conn, xid, opcode, frame):
        if opcode == zk.OP_PING:
            self.pings += 1
            self._reply(conn, zk.XID_PING)
            return
        if opcode == zk.OP_CLOSE:
            return

        path, offset = _unpack_string(frame, 8)
        self.requests.append((opcode, path))

        if opcode in (zk.OP_GET_DATA, zk.OP_EXISTS, zk.OP_GET_CHILDREN):
            (watch,) = struct.unpack_from("!?", frame, offset)
            held = self.hold.get(path)
            if held is not None:
                held.wait(5.0)

        if opcode == zk.OP_GET_DATA:
            if path not in self.nodes:
                # Note what is *not* done here: no watch is registered. ZooKeeper only
                # arms a data watch on the success path, which is the hole the client has
                # to cover with an exists watch.
                self._reply(conn, xid, zk.ERR_NO_NODE)
                return
            if watch:
                self.data_watches[path] = self.data_watches.get(path, 0) + 1
            value = self.nodes[path]
            self._reply(conn, xid, 0, struct.pack("!i", len(value)) + value + _STAT)
            return

        if opcode == zk.OP_EXISTS:
            if watch:
                # Armed whether or not the node is there. That is the difference.
                self.data_watches[path] = self.data_watches.get(path, 0) + 1
            if path not in self.nodes:
                self._reply(conn, xid, zk.ERR_NO_NODE)
            else:
                self._reply(conn, xid, 0, _STAT)
            return

        if opcode == zk.OP_GET_CHILDREN:
            if path not in self.nodes:
                self._reply(conn, xid, zk.ERR_NO_NODE)
                return
            if watch:
                self.child_watches[path] = self.child_watches.get(path, 0) + 1
            kids = self._children(path)
            payload = struct.pack("!i", len(kids))
            for name in kids:
                payload += _pack_string(name)
            self._reply(conn, xid, 0, payload + _STAT)
            return

        if opcode == zk.OP_CREATE:
            (length,) = struct.unpack_from("!i", frame, offset)
            data = b"" if length < 0 else frame[offset + 4:offset + 4 + length]
            (flags,) = struct.unpack_from("!i", frame, len(frame) - 4)
            if flags & zk.SEQUENTIAL:
                # The counter is the ensemble's, which is the whole reason an election can
                # trust the order.
                parent = path.rsplit("/", 1)[0] or "/"
                seq = self.counters.get(parent, 0)
                self.counters[parent] = seq + 1
                path = "%s%010d" % (path, seq)
            if path in self.nodes:
                self._reply(conn, xid, zk.ERR_NODE_EXISTS)
                return
            self.create(path, data)
            self._reply(conn, xid, 0, _pack_string(path))
            return

        if opcode == zk.OP_SET_DATA:
            (length,) = struct.unpack_from("!i", frame, offset)
            data = b"" if length < 0 else frame[offset + 4:offset + 4 + length]
            if path not in self.nodes:
                self._reply(conn, xid, zk.ERR_NO_NODE)
                return
            self.set(path, data)
            self._reply(conn, xid, 0, _STAT)
            return

        if opcode == zk.OP_DELETE:
            if path not in self.nodes:
                self._reply(conn, xid, zk.ERR_NO_NODE)
                return
            self.delete(path)
            self._reply(conn, xid, 0)
            return

        self._reply(conn, xid, -1)

    # -- mutations, which are also what fires watches -------------------

    def _children(self, path):
        prefix = path.rstrip("/") + "/"
        return sorted(k[len(prefix):] for k in self.nodes
                      if k.startswith(prefix) and "/" not in k[len(prefix):])

    def _fire_data(self, event_type, path):
        if self.data_watches.pop(path, 0):
            self._notify(event_type, path)

    def _fire_children(self, path):
        parent = path.rsplit("/", 1)[0] or "/"
        if self.child_watches.pop(parent, 0):
            self._notify(zk.EVENT_NODE_CHILDREN_CHANGED, parent)

    def set(self, path, data):
        self.nodes[path] = data
        self._fire_data(zk.EVENT_NODE_DATA_CHANGED, path)

    def create(self, path, data=b""):
        self.nodes[path] = data
        self._fire_data(zk.EVENT_NODE_CREATED, path)
        self._fire_children(path)

    def delete(self, path):
        self.nodes.pop(path, None)
        self._fire_data(zk.EVENT_NODE_DELETED, path)
        self._fire_children(path)


class EnsembleCase(unittest.TestCase):
    NODES = {"/": b"", "/cluster_state": b"started", "/helios": b""}

    def setUp(self):
        self.server = FakeEnsemble(dict(self.NODES))
        self.addCleanup(self.server.stop)
        self.client = zk.ZKClient(hosts=["127.0.0.1"], port=self.server.port,
                                  timeout=5.0, session_timeout_ms=30000)
        self.client.connect()
        self.addCleanup(self.client.close)

    def collector(self):
        """A callback that records what it was told and lets the test wait for it."""
        seen = []
        arrived = threading.Event()

        def callback(event):
            seen.append(event)
            arrived.set()

        callback.seen = seen
        callback.arrived = arrived
        return callback

    def wait(self, callback, count=1, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if len(callback.seen) >= count:
                return callback.seen
            callback.arrived.wait(0.05)
            callback.arrived.clear()
        self.fail("expected %d watch event(s), got %d" % (count, len(callback.seen)))


class TheOperationsStillWorkWithoutWatches(EnsembleCase):
    """Every existing caller passes no watch flag and must behave exactly as before."""

    def test_reads_and_writes(self):
        self.assertEqual(self.client.get("/cluster_state"), b"started")
        self.assertTrue(self.client.exists("/cluster_state"))
        self.assertFalse(self.client.exists("/nope"))
        self.client.set("/cluster_state", b"stopped")
        self.assertEqual(self.client.get("/cluster_state"), b"stopped")
        self.client.create("/helios/nodes", b"x")
        self.assertIn("nodes", self.client.get_children("/helios"))
        self.client.delete("/helios/nodes")
        self.assertEqual(self.client.get_children("/helios"), [])

    def test_a_plain_read_arms_nothing(self):
        """The default has to stay False on the wire, not merely in the signature: a
        client that quietly watched everything it read would accumulate watches on the
        ensemble forever."""
        self.client.get("/cluster_state")
        self.client.exists("/cluster_state")
        self.client.get_children("/helios")
        self.assertEqual(self.server.data_watches, {})
        self.assertEqual(self.server.child_watches, {})

    def test_a_missing_node_still_raises_no_node(self):
        with self.assertRaises(zk.ZKNoNode):
            self.client.get("/absent")


class TheFramesAreDemultiplexed(EnsembleCase):
    def test_two_threads_can_have_requests_in_flight_at_once(self):
        """The reply to the second arrives first. Under the old shape there was only ever
        one reader -- whoever had just sent -- so a reply landing out of order was a reply
        delivered to the wrong caller."""
        gate = threading.Event()
        self.server.nodes["/slow"] = b"slow"
        self.server.hold["/slow"] = gate

        results = {}

        def slow():
            try:
                results["slow"] = self.client.get("/slow")
            except Exception as exc:        # pragma: no cover - a failure path
                results["slow"] = exc

        thread = threading.Thread(target=slow)
        thread.start()
        time.sleep(0.2)
        # The held request is still outstanding, and this one must not be behind it.
        self.assertEqual(self.client.get("/cluster_state"), b"started")
        gate.set()
        thread.join(5.0)
        self.assertEqual(results.get("slow"), b"slow")

    def test_a_watch_fires_while_a_request_is_in_flight(self):
        """The notification and the reply share one socket. A demultiplexer is the only
        thing that can tell them apart -- and the re-arm the notification triggers is
        itself a request, issued while another is still outstanding."""
        callback = self.collector()
        self.client.watch_data("/cluster_state", callback)

        gate = threading.Event()
        self.server.nodes["/slow"] = b"slow"
        self.server.hold["/slow"] = gate
        held = {}

        def slow():
            held["value"] = self.client.get("/slow")

        thread = threading.Thread(target=slow)
        thread.start()
        time.sleep(0.2)
        self.server.set("/cluster_state", b"stopped")

        self.assertEqual(self.wait(callback)[0].value, b"stopped")
        gate.set()
        thread.join(5.0)
        self.assertEqual(held.get("value"), b"slow")

    def test_the_keepalive_reply_is_not_mistaken_for_a_request_reply(self):
        self.client._request(zk.OP_PING, b"")
        time.sleep(0.3)
        self.assertGreaterEqual(self.server.pings, 1)
        self.assertEqual(self.client.get("/cluster_state"), b"started")


class AWatchRearmsItself(EnsembleCase):
    def test_a_data_watch_fires_on_every_change_not_only_the_first(self):
        """ZooKeeper watches are one-shot. A client that did not re-arm would look correct
        in any test that changed the value once."""
        callback = self.collector()
        self.client.watch_data("/cluster_state", callback)

        for value in (b"stopped", b"started", b"stopped"):
            self.server.set("/cluster_state", value)
            time.sleep(0.05)

        seen = self.wait(callback, count=3)
        self.assertEqual([e.value for e in seen[:3]], [b"stopped", b"started", b"stopped"])

    def test_the_arming_read_hands_back_the_current_value(self):
        """A caller that is about to act on the value needs it now, and the arming read has
        already fetched it."""
        _watch, value = self.client.watch_data_now("/cluster_state", self.collector())
        self.assertEqual(value, b"started")

    def test_a_data_watch_on_a_path_that_does_not_exist_yet_still_fires(self):
        """The hole a getData watch leaves. ZooKeeper arms it only when the read succeeds,
        so watching a path before it exists silently watches nothing -- and `/cluster_state`
        does not exist on a cluster that has never been started.
        """
        callback = self.collector()
        _watch, value = self.client.watch_data_now("/later", callback)
        self.assertIsNone(value, "a missing node must read as absent, not as empty data")
        self.assertIn("/later", self.server.data_watches,
                      "nothing is armed on the ensemble, so the create will never arrive")

        self.server.create("/later", b"here")
        self.assertEqual(self.wait(callback)[0].value, b"here")

    def test_a_deleted_node_reads_as_absent_and_the_watch_survives_it(self):
        callback = self.collector()
        self.client.watch_data("/cluster_state", callback)

        self.server.delete("/cluster_state")
        self.assertIsNone(self.wait(callback)[0].value)

        self.server.create("/cluster_state", b"started")
        self.assertEqual(self.wait(callback, count=2)[1].value, b"started")

    def test_a_child_watch_fires_on_a_child_appearing_and_going(self):
        callback = self.collector()
        _watch, children = self.client.watch_children("/helios", callback)
        self.assertEqual(children, [])

        self.server.create("/helios/nodes", b"")
        self.assertEqual(self.wait(callback)[0].value, ["nodes"])

        self.server.delete("/helios/nodes")
        self.assertEqual(self.wait(callback, count=2)[1].value, [])

    def test_a_cancelled_watch_stops_being_re_armed(self):
        callback = self.collector()
        watch = self.client.watch_data("/cluster_state", callback)
        watch.cancel()

        self.server.set("/cluster_state", b"stopped")
        time.sleep(0.4)
        self.assertEqual(callback.seen, [])

    def test_a_callback_that_raises_does_not_silence_the_next_event(self):
        """One bad callback taking the dispatcher down would lose every watch on the
        client, which is exactly the invisible failure this is all about."""
        good = self.collector()

        def bad(_event):
            raise RuntimeError("callback is wrong")

        self.client.watch_data("/cluster_state", bad)
        self.client.watch_data("/cluster_state", good)

        self.server.set("/cluster_state", b"stopped")
        self.wait(good, count=1)
        self.server.set("/cluster_state", b"started")
        self.assertEqual(self.wait(good, count=2)[1].value, b"started")


class TheSocketGoingAwayIsNotSilence(EnsembleCase):
    def test_an_in_flight_request_fails_rather_than_waiting_out_its_timeout(self):
        gate = threading.Event()
        self.server.nodes["/slow"] = b"slow"
        self.server.hold["/slow"] = gate
        outcome = {}

        def ask():
            try:
                outcome["value"] = self.client.get("/slow")
            except Exception as exc:
                outcome["error"] = exc

        thread = threading.Thread(target=ask)
        thread.start()
        time.sleep(0.2)
        self.server.drop_connection()

        thread.join(3.0)
        gate.set()
        self.assertFalse(thread.is_alive(), "the waiter was never woken")
        self.assertIsInstance(outcome.get("error"), zk.ZKError)

    def test_the_client_reports_itself_disconnected(self):
        """A loop driven by watches has no request in flight to fail, so this is its only
        symptom -- and the thing that tells it to rebuild the client."""
        self.assertTrue(self.client.is_connected())
        self.server.drop_connection()
        deadline = time.time() + 3.0
        while self.client.is_connected() and time.time() < deadline:
            time.sleep(0.05)
        self.assertFalse(self.client.is_connected())

    def test_a_request_after_the_socket_died_raises_instead_of_hanging(self):
        self.server.drop_connection()
        time.sleep(0.3)
        with self.assertRaises(zk.ZKError):
            self.client.get("/cluster_state")

    def test_the_watches_are_told_the_session_went_away(self):
        callback = self.collector()
        self.client.watch_data("/cluster_state", callback)
        self.server.drop_connection()
        event = self.wait(callback)[0]
        self.assertEqual(event.reason, zk.REASON_ERROR)
        self.assertIsNotNone(event.error)


class AWatchSurvivesAReconnect(EnsembleCase):
    def test_reconnecting_re_arms_every_registration(self):
        callback = self.collector()
        self.client.watch_data("/cluster_state", callback)

        self.server.drop_connection()
        self.wait(callback)                      # the disconnect notice
        self.server.data_watches.clear()

        self.client.connect()
        deadline = time.time() + 5.0
        while "/cluster_state" not in self.server.data_watches and time.time() < deadline:
            time.sleep(0.05)
        self.assertIn("/cluster_state", self.server.data_watches,
                      "the watch was not re-armed, so it would never fire again")

        self.server.set("/cluster_state", b"stopped")
        values = [e.value for e in self.wait(callback, count=3) if e.reason != zk.REASON_ERROR]
        self.assertIn(b"stopped", values)

    def test_a_reconnect_is_reported_as_a_possible_change(self):
        """Nothing is delivered for the window a client was away, so the value may have
        moved and come back, or moved and stayed. The callback has to be told to look."""
        callback = self.collector()
        self.client.watch_data("/cluster_state", callback)

        self.server.drop_connection()
        self.wait(callback)
        self.server.nodes["/cluster_state"] = b"stopped"    # changed while nobody watched
        self.client.connect()

        reasons = [e.reason for e in self.wait(callback, count=2)]
        self.assertIn(zk.REASON_RECONNECTED, reasons)
        rearmed = [e for e in callback.seen if e.reason == zk.REASON_RECONNECTED]
        self.assertEqual(rearmed[0].value, b"stopped",
                         "the re-arm did not report what changed while the client was away")

    def test_an_expired_session_becomes_a_new_one_and_says_so(self):
        """An ephemeral node does not come back with a new session -- a published node
        entry, an election ballot -- so a caller has to be able to find out that this is a
        different session rather than a resumed one."""
        callback = self.collector()
        self.client.watch_data("/cluster_state", callback)
        self.server.drop_connection()
        # The refusal is for the *resume*. The client drops the session it was holding and
        # comes back with id 0, which the ensemble accepts -- exactly what happens to a
        # client that was away longer than its session timeout.
        self.server.refuse_resume = True
        self.client.connect()

        self.assertTrue(self.client.is_connected())
        self.assertTrue(self.client.session_expired,
                        "the client resumed silently; ephemeral state is gone and nobody "
                        "was told")

    def test_reconnecting_does_not_leave_the_old_reader_on_the_old_socket(self):
        """The threads are guarded by is_alive(), so a reconnect that left the previous
        reader running would start a socket nobody reads -- a client that looks connected
        and times out every request."""
        before = self.client._reader_thread
        self.client.connect()
        self.assertIsNot(self.client._reader_thread, before)
        self.assertEqual(self.client.get("/cluster_state"), b"started")

    def test_a_fresh_client_whose_session_is_refused_still_raises(self):
        """Only a *resume* gets retried. A server refusing a brand-new session is a failure
        to report, not something to loop on -- and retrying it would be an infinite
        recursion through connect().
        """
        refuser = FakeEnsemble({})
        self.addCleanup(refuser.stop)
        refuser.refuse_all = True
        fresh = zk.ZKClient(hosts=["127.0.0.1"], port=refuser.port, timeout=2.0)
        with self.assertRaises(zk.ZKError):
            fresh.connect()


class TheElectionCanWatchToo(EnsembleCase):
    def test_a_candidate_is_told_when_the_standing_set_changes(self):
        """Additive: the lowest-ballot rule is still correct without this. What the watch
        buys is the survivor learning it leads at handover rather than at its next poll."""
        self.client.ensure_path(zk.LEADERS_ROOT + "/catalyst")
        first = zk.Election(self.client, "catalyst", identity=b"a")
        second = zk.Election(self.client, "catalyst", identity=b"b")
        first.stand()
        second.stand()

        told = []
        arrived = threading.Event()
        watch = second.watch(lambda leading: (told.append(leading), arrived.set()))
        self.assertIsNotNone(watch)

        self.client.delete("%s/%s" % (first.parent, first.ballot))
        self.assertTrue(arrived.wait(5.0), "the survivor was never told")
        self.assertIn(True, told)

    def test_a_client_that_cannot_watch_is_not_a_failure(self):
        """`Election` is being wired to services right now, against a deployed helios_zk
        that may predate watches."""
        class Older(object):
            def get_children(self, _path):
                return []

        election = zk.Election(Older(), "catalyst")
        self.assertIsNone(election.watch(lambda _leading: None))


class TheReconcileLoopIsEventDriven(unittest.TestCase):
    """The point of the whole change. A declaration nobody notices for thirty seconds is
    why `cluster start` still drives services in numbered phases."""

    def setUp(self):
        self.source = read("spark_daemon_decoded.py")
        start = self.source.index("def zk_reconcile_loop():")
        self.loop = self.source[start:self.source.index("def _decode_desired_state(")]

    def test_the_trigger_is_a_watch(self):
        self.assertIn("watch_data_now(ZK_CLUSTER_STATE", self.loop,
                      "the loop no longer watches the desired state")

    def test_it_does_not_poll_the_desired_state_on_the_publish_timer(self):
        """`time.sleep(ZK_PUBLISH_INTERVAL)` at the bottom of the loop *was* the
        mechanism: every node re-read /cluster_state every five seconds forever, and a
        change still took up to thirty to be acted on."""
        self.assertNotIn("time.sleep(ZK_PUBLISH_INTERVAL)", self.loop,
                         "the loop is still on a five-second poll")
        self.assertNotIn("time.sleep(", self.loop,
                         "the loop sleeps on a timer rather than waiting to be woken")

    def test_it_waits_to_be_woken(self):
        self.assertIn("woken.wait(", self.loop)
        self.assertIn("woken.set()", self.loop)

    def test_convergence_is_not_left_entirely_to_the_events(self):
        """A missed notification must not wedge a node forever, and a unit that dies while
        the desired state is unchanged produces no notification at all."""
        self.assertIn("ZK_STATE_REREAD_INTERVAL", self.loop,
                      "nothing re-reads the desired state, so a dropped event is permanent")
        self.assertIn("ZK_DRIFT_CHECK_INTERVAL", self.loop,
                      "the local drift check is gone; a service that dies stays dead")

    def test_the_safety_net_is_long_enough_not_to_be_the_mechanism(self):
        match = re.search(r"^ZK_STATE_REREAD_INTERVAL\s*=\s*(\d+)", self.source, re.MULTILINE)
        self.assertTrue(match)
        interval = int(match.group(1))
        publish = int(re.search(r"^ZK_PUBLISH_INTERVAL\s*=\s*(\d+)",
                                self.source, re.MULTILINE).group(1))
        self.assertGreaterEqual(interval, 10 * publish,
                                "the re-read is close enough to the old poll to be one")

    def test_it_notices_a_dead_socket_without_a_request_to_fail(self):
        self.assertIn("is_connected()", self.loop,
                      "a watch-driven loop has nothing in flight to fail on, so it has to "
                      "ask whether the transport is still there")

    def test_the_callback_does_not_run_systemctl(self):
        """Convergence shells out, and the dispatcher is what delivers every other watch on
        that client. Blocking it there would stall them all."""
        callback = self.loop[self.loop.index("def on_state_change("):]
        callback = callback[: callback.index("\n    client = None")]
        self.assertNotIn("converge_to_desired_state", callback)
        self.assertIn("woken.set()", callback)

    def test_it_still_publishes_exactly_what_it_published_before(self):
        """Other work in flight changes *what* the loop publishes. Only the trigger moved."""
        self.assertIn("converge_to_desired_state(desired, full=changed)", self.loop)
        self.assertIn("/etc/hci/maintenance.state", self.loop)


class TheEmbeddedCopyMatchesTheTree(unittest.TestCase):
    """`provision.py` ships helios_zk.py as base64, and an embedded copy that drifts is a
    node running the previous client."""

    def test_the_embedded_helios_zk_is_the_one_in_the_tree(self):
        import base64

        match = re.search(r'^HELIOS_ZK_B64\s*=\s*"([^"]*)"', read("provision.py"), re.MULTILINE)
        self.assertTrue(match and match.group(1))
        embedded = base64.b64decode(match.group(1)).decode("utf-8")
        self.assertEqual(embedded.splitlines(), read("helios_zk.py").splitlines(),
                         "provision.py ships a different helios_zk.py; run sync_provision.py")


if __name__ == "__main__":
    unittest.main()
