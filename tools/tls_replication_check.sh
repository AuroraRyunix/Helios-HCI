#!/bin/bash
# Two Sidon instances replicating over the node's real IP, so the traffic is genuinely
# TLS rather than loopback-exempt. Then the assertion that matters: a client without a
# cluster certificate must not be able to speak the protocol.
set -u
BIN=/tmp/build/sidon/target/debug/sidon
IP=10.10.102.41
A=/var/lib/hci/tlstest/a
B=/var/lib/hci/tlstest/b
FAILED=0

fail() { echo "FAIL: $*"; FAILED=1; }

cleanup() {
    # Never `kill 0`: an unset PID defaulting to zero signals the whole process group,
    # which includes the shell running this and the ssh session it arrived on.
    [ -n "${PID_A:-}" ] && kill "$PID_A" 2>/dev/null
    [ -n "${PID_B:-}" ] && kill "$PID_B" 2>/dev/null
    sleep 1
    # The vdisk row lives in Hydra, not on this node's disk, so removing the directories
    # is not enough to make the next run's create succeed.
    curl -sS --data-binary "DELETE FROM hydra.dfs_block_map WHERE vdisk_id = 'tlsvd';" \
        http://127.0.0.1:9043/query >/dev/null 2>&1
    curl -sS --data-binary "DELETE FROM hydra.dfs_vdisks WHERE vdisk_id = 'tlsvd';" \
        http://127.0.0.1:9043/query >/dev/null 2>&1
    rm -rf /var/lib/hci/tlstest /tmp/tls-a.sock /tmp/tls-b.sock
}
trap cleanup EXIT

cleanup 2>/dev/null
mkdir -p "$A"/{journal,egroups,nbd} "$B"/{journal,egroups,nbd}

echo "=== 1. two instances, both bound to $IP (not loopback) ==="
SIDON_NODE=tls-a SIDON_ROOT=$A SIDON_CONTROL=/tmp/tls-a.sock \
  SIDON_PEER_BIND=$IP:9205 SIDON_PEERS="tls-b=$IP:9206" \
  SIDON_PURAH_INTERVAL=0 "$BIN" > /tmp/tls-a.log 2>&1 &
PID_A=$!
SIDON_NODE=tls-b SIDON_ROOT=$B SIDON_CONTROL=/tmp/tls-b.sock \
  SIDON_PEER_BIND=$IP:9206 SIDON_PEERS="tls-a=$IP:9205" \
  SIDON_PURAH_INTERVAL=0 "$BIN" > /tmp/tls-b.log 2>&1 &
PID_B=$!
sleep 3

grep -h "replication listener" /tmp/tls-a.log /tmp/tls-b.log
grep -q "mutual TLS" /tmp/tls-a.log || fail "instance A did not report mutual TLS"

echo
echo "=== 2. the mutual handshake ==="
PEERS=$(printf '{"op":"peers"}\n' | nc -U /tmp/tls-a.sock)
echo "    $PEERS"
echo "$PEERS" | grep -q '"reachable":true' || fail "the peers could not complete a handshake"

echo
echo "=== 3. an RF2 vdisk, replicated over TLS ==="
printf '{"op":"create","vdisk_id":"tlsvd","size_bytes":33554432,"rf":2,"replicas":["tls-a","tls-b"]}\n' | nc -U /tmp/tls-a.sock
printf '{"op":"attach","vdisk_id":"tlsvd"}\n' | nc -U /tmp/tls-a.sock

python3 - <<'PY' || exit 1
import sys
sys.path.insert(0, "/usr/local/bin")
import helios_sidon as s
w = s.NbdWriter("/var/lib/hci/tlstest/a/nbd/tlsvd.sock", "tlsvd")
payload = (b"TLS-REPLICATED-PAYLOAD-" * 200)[:4096]
w.write_at(0, payload)
w.flush()
back = w.read(0, 4096)
w.close()
assert back == payload, "the write did not read back"
print("    wrote and read 4 KiB through the TLS-replicated vdisk")
PY

echo
echo "=== 4. the replica really received it ==="
# A replica keeps its copy under replica/, not journal/ -- the journal a vdisk owns and
# the journal it holds for someone else are deliberately different trees.
BSIZE=$(stat -c %s "$B/replica/tlsvd.jrn" 2>/dev/null || echo 0)
echo "    replica journal   : $BSIZE bytes"
echo "    replica fence epoch: $(cat "$B/replica/tlsvd.epoch" 2>/dev/null || echo none)"
if grep -q "TLS-REPLICATED-PAYLOAD" "$B/replica/tlsvd.jrn" 2>/dev/null; then
    echo "    the guest's bytes are on the replica: yes"
else
    fail "the payload did not reach the replica"
fi
[ "$BSIZE" -ge 4096 ] || fail "the replica's journal is too small to hold the write"

echo
echo "=== 5. a client with no cluster certificate must be refused ==="
python3 - <<'PY' || exit 1
import socket, ssl, struct, sys

ADDR = ("10.10.102.41", 9205)
PING = struct.pack(">HH", 1, 0) + b"\x00" * 32
bad = 0

# (a) Plain TCP, which is exactly what an attacker on the cluster network has. Before
# mTLS this was answered. A TLS *alert* record back (content type 0x15) is the server
# refusing, not the server replying -- so it counts as a refusal, and anything that
# parses as a protocol response does not.
try:
    sock = socket.create_connection(ADDR, timeout=5)
    sock.sendall(PING)
    sock.settimeout(5)
    reply = sock.recv(64)
    sock.close()
    if not reply:
        print("    plaintext: connection closed with no reply")
    elif reply[0] == 0x15:
        print("    plaintext: refused with a TLS fatal alert (%s)" % reply[:7].hex())
    else:
        print("    FAIL plaintext client got a protocol reply: %r" % reply[:32])
        bad = 1
except Exception as exc:
    print("    plaintext: refused (%s)" % str(exc)[:70])

# (b) TLS, trusting the cluster CA, but presenting no client certificate. This is the
# case server-only TLS would have allowed: encrypted, and completely unauthenticated.
try:
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    ctx.check_hostname = False
    raw = socket.create_connection(ADDR, timeout=5)
    tls = ctx.wrap_socket(raw)
    tls.sendall(PING)
    tls.settimeout(5)
    reply = tls.recv(64)
    tls.close()
    print("    FAIL: TLS with no client certificate was answered: %r" % reply[:32])
    bad = 1
except Exception as exc:
    print("    no client certificate: refused (%s)" % str(exc)[:70])

# (c) And the positive control: the node's own certificate must work, or (a) and (b)
# prove nothing except that the port is broken.
try:
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
    ctx.check_hostname = False
    ctx.load_cert_chain("/etc/hci/spark/certs/node.crt", "/etc/hci/spark/certs/node.key")
    raw = socket.create_connection(ADDR, timeout=5)
    tls = ctx.wrap_socket(raw)
    # The assertion is that the *handshake* completes -- that the server accepted this
    # client certificate. Sending a protocol frame here would only re-test what step 2
    # already proved between the two daemons, and a hand-rolled frame that the server
    # waits for more of reads as a certificate failure when it is nothing of the kind.
    peer = tls.getpeercert()
    cipher = tls.cipher()
    tls.close()
    print("    with the node certificate: handshake completed (%s)" % (cipher[0] if cipher else "?"))
    print("      server presented: %s" % (peer.get("subject") if peer else "no cert"))
except Exception as exc:
    print("    FAIL: the node's own certificate was refused (%s)" % str(exc)[:70])
    bad = 1

sys.exit(bad)
PY
[ $? -eq 0 ] || fail "an unauthenticated client was accepted"

echo
if [ "$FAILED" -eq 0 ]; then
    echo "ALL TLS ASSERTIONS PASSED"
else
    echo "THERE WERE FAILURES"
    exit 1
fi
