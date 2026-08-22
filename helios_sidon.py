"""Client for Sidon's control socket, and the switch that decides whether to use it.

Sidon listens on a unix socket rather than a port, so only code running natively on the
host can reach it -- which is deliberate. Spectrum runs in a container and must not get a
bind mount into `/run`; it asks spark-daemon over the existing mutual-TLS mesh on 9099,
and spark, which is native, makes the local call. One authenticated surface for the whole
cluster instead of a second one per storage tier.

The protocol is newline-delimited JSON: one request object per line, one response object
per line, `{"ok": true, ...}` or `{"ok": false, "error": ..., "kind": ...}`. `kind` is
carried through because it says whether retrying is sensible: a `meta` failure may clear
when Hydra comes back, a `refused` failure will not clear no matter how often it is
retried.
"""

import json
import os
import socket

CONTROL_SOCKET = os.environ.get("SIDON_CONTROL", "/run/sidon/control.sock")
NBD_DIR = os.environ.get("SIDON_NBD_DIR", "/var/lib/hci/sidon/nbd")
CLUSTER_JSON = "/etc/hci/cluster.json"

# The engine names `cluster.json` may carry. The key has existed since the GlusterFS
# transition and was, until recently, read by four hardcoded copies of get_dfs_engine()
# that all returned "linstor" and were called by nothing. "linstor" is kept only so an
# old cluster.json reads as a recognised value rather than a typo.
ENGINE_LINSTOR = "linstor"
ENGINE_SIDON = "sidon"


class SidonError(Exception):
    """A refusal or failure from the daemon. `kind` distinguishes retryable from not."""

    def __init__(self, message, kind="unknown"):
        super().__init__(message)
        self.kind = kind


def dfs_engine(cluster_json=CLUSTER_JSON):
    """Which storage engine this cluster uses for VM disks.

    The default flipped, and the reason is worth writing down. It used to be LINSTOR:
    while both existed, a cluster whose configuration could not be read was a cluster
    whose disks were DRBD resources, and guessing the new engine would have pointed a VM
    at storage that had never held it. Now there is no LINSTOR code left, so answering
    "linstor" names an engine nothing can serve -- a failure mode with no recovery rather
    than a cautious one. Sidon is the only answer that can be acted on.

    ENGINE_LINSTOR survives only to recognise the value in an old cluster.json without
    treating it as a typo.
    """
    try:
        with open(cluster_json, "r", encoding="utf-8") as handle:
            value = json.load(handle).get("dfs_engine", ENGINE_SIDON)
    except (OSError, ValueError):
        return ENGINE_SIDON
    value = str(value or "").strip().lower()
    return value if value in (ENGINE_LINSTOR, ENGINE_SIDON) else ENGINE_SIDON


def using_sidon(cluster_json=CLUSTER_JSON):
    return dfs_engine(cluster_json) == ENGINE_SIDON


def nbd_socket(vdisk_id, nbd_dir=NBD_DIR):
    return os.path.join(nbd_dir, "%s.sock" % vdisk_id)


def vdisk_id_for(vm_name, index):
    """The vdisk id for a VM's Nth disk.

    Deliberately the same string LINSTOR resources use (`<vm>-disk<n>`), so an operator
    reading a task log, a libvirt domain and the block map sees one name rather than
    three, and so the migration can address both substrates by the same key.
    """
    return "%s-disk%d" % (vm_name, index)


def call(op, socket_path=None, timeout=60, **params):
    """Send one control request and return its response body.

    Raises SidonError on refusal so callers cannot mistake `{"ok": false}` for success by
    forgetting to check a flag -- the mistake the LWT endpoints in Daruk were built to
    stop callers making with `applied`.
    """
    path = socket_path or CONTROL_SOCKET
    request = dict(params)
    request["op"] = op
    payload = (json.dumps(request) + "\n").encode("utf-8")

    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.settimeout(timeout)
    try:
        try:
            conn.connect(path)
        except OSError as exc:
            raise SidonError(
                "sidon control socket %s is unreachable: %s" % (path, exc), "io"
            )
        conn.sendall(payload)
        chunks = []
        while True:
            chunk = conn.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
            if b"\n" in chunk:
                break
    except socket.timeout:
        raise SidonError("sidon did not answer '%s' within %ss" % (op, timeout), "io")
    finally:
        conn.close()

    raw = b"".join(chunks).split(b"\n", 1)[0].decode("utf-8", "replace").strip()
    if not raw:
        raise SidonError("sidon closed the connection without answering '%s'" % op, "io")
    try:
        body = json.loads(raw)
    except ValueError:
        raise SidonError("sidon answered '%s' with non-JSON: %.200s" % (op, raw), "io")
    if not body.get("ok"):
        raise SidonError(
            body.get("error") or "sidon refused '%s' without saying why" % op,
            body.get("kind", "unknown"),
        )
    return body


def create_vdisk(vdisk_id, size_bytes, container="default", vdisk_class="rw", **kw):
    return call(
        "create",
        vdisk_id=vdisk_id,
        size_bytes=int(size_bytes),
        container=container,
        **{"class": vdisk_class, **kw}
    )


def attach(vdisk_id, **kw):
    """Claim ownership and start serving. Returns the response, including `socket`.

    Idempotent by design: attaching a vdisk this node already serves returns the existing
    socket rather than bumping the epoch, because bumping it would fence the qemu that is
    currently using it.
    """
    return call("attach", vdisk_id=vdisk_id, **kw)


def detach(vdisk_id, **kw):
    return call("detach", vdisk_id=vdisk_id, **kw)


def delete_vdisk(vdisk_id, **kw):
    return call("delete", vdisk_id=vdisk_id, **kw)


def snapshot(vdisk_id, child_id, **kw):
    """A point-in-time immutable copy. `child_id` is the new vdisk.

    Nothing is copied but the block map. Sealed extent groups are immutable, so parent
    and snapshot share every one of them, and a later write to the parent is redirected
    into new extents rather than over shared ones. The cost is proportional to the
    number of extents in the map, not to the bytes on disk, so a snapshot of a terabyte
    costs what a snapshot of a gigabyte costs.

    A writable parent must be attached on the node this call reaches: it has to be
    drained first, and only its owner can drain it. An immutable parent needs neither,
    which is why cloning an image works from anywhere.
    """
    return call("snapshot", vdisk_id=vdisk_id, child_id=child_id, **kw)


def clone(vdisk_id, child_id, **kw):
    """A writable copy. A snapshot but for the class it ends in.

    This is what clone-from-image is. The template is already immutable, so every VM
    cloned from it shares its extents until each writes its own -- which is the win
    deduplication is usually bought for, had without buying it.
    """
    return call("clone", vdisk_id=vdisk_id, child_id=child_id, **kw)


def seal(vdisk_id, **kw):
    """Freeze a vdisk to the immutable class, permanently.

    What golden images get instead of DRBD's `--allow-two-primaries`. That option existed
    so several hosts could each hold Primary on one image and read it; it is also the
    option that caused the corruption the fencing work exists to prevent. An immutable
    vdisk cannot express the hazard at all -- writes are refused by class, so any number
    of readers is safe and no writer is possible.
    """
    return call("seal", vdisk_id=vdisk_id, **kw)


def resize(vdisk_id, size_bytes, **kw):
    """Grow a vdisk. Shrinking is refused by the daemon, not merely discouraged."""
    return call("resize", vdisk_id=vdisk_id, size_bytes=int(size_bytes), **kw)


def status(vdisk_id, **kw):
    return call("status", vdisk_id=vdisk_id, **kw)


def capacity(**kw):
    """What this node's extent store holds and how much room is left."""
    return call("capacity", **kw)


def peers(**kw):
    """Which peers this node can reach. Reachability is not safety -- an append needs
    every replica and an unreachable one fails the write -- but it is what an operator
    needs to see when a vdisk stops accepting writes."""
    return call("peers", **kw)


def list_attached(**kw):
    return call("list", **kw)


def flush(vdisk_id, **kw):
    """Force a drain. Not needed for durability -- writes are already durable in the
    journal -- but it bounds replay time and is what a clean shutdown should do."""
    return call("flush", vdisk_id=vdisk_id, **kw)


# --- A minimal NBD client -------------------------------------------------------------
#
# Written from the protocol specification rather than from Sidon's server, and kept here
# rather than in a dependency because the only thing that streams into a vdisk from
# Python is image upload. Newstyle handshake, NBD_OPT_GO, simple replies -- the same
# subset the server implements, and nothing more.

NBD_MAGIC = 0x4E42444D41474943
NBD_IHAVEOPT = 0x49484156454F5054
NBD_REP_MAGIC = 0x3E889045565A9
NBD_REQUEST_MAGIC = 0x25609513
NBD_SIMPLE_REPLY_MAGIC = 0x67446698
NBD_OPT_GO = 7
NBD_REP_ACK = 1
NBD_REP_INFO = 3
NBD_INFO_EXPORT = 0
NBD_CMD_READ = 0
NBD_CMD_WRITE = 1
NBD_CMD_FLUSH = 3
NBD_CMD_DISC = 2

# 1 MiB per request: it matches the journal's record cap, so one client write becomes one
# journal record rather than being split and re-joined.
NBD_CHUNK = 1 << 20


class NbdWriter(object):
    """Streams bytes into a vdisk over its NBD socket.

    Used by spark for image upload. The web tier never touches this: it has no access to
    the socket and should not, so the bytes are relayed to spark, which is native to the
    host and owns storage.
    """

    def __init__(self, socket_path, export, timeout=3600):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect(socket_path)
        self.handle = 0
        self.size = self._handshake(export)

    def _recv(self, count):
        buf = b""
        while len(buf) < count:
            chunk = self.sock.recv(count - len(buf))
            if not chunk:
                raise SidonError("nbd connection closed after %d of %d bytes" % (len(buf), count), "io")
            buf += chunk
        return buf

    def _handshake(self, export):
        import struct
        greeting = self._recv(18)
        magic, ihaveopt, _flags = struct.unpack(">QQH", greeting)
        if magic != NBD_MAGIC or ihaveopt != NBD_IHAVEOPT:
            raise SidonError("not an NBD newstyle server", "io")
        # fixed newstyle | no zeroes
        self.sock.sendall(struct.pack(">I", 3))

        name = export.encode("utf-8")
        payload = struct.pack(">I", len(name)) + name + struct.pack(">H", 0)
        self.sock.sendall(struct.pack(">QII", NBD_IHAVEOPT, NBD_OPT_GO, len(payload)) + payload)

        size = None
        while True:
            head = self._recv(20)
            rep_magic, _opt, rep_type, length = struct.unpack(">QIII", head)
            if rep_magic != NBD_REP_MAGIC:
                raise SidonError("nbd option reply magic 0x%x is wrong" % rep_magic, "io")
            data = self._recv(length) if length else b""
            if rep_type == NBD_REP_INFO and len(data) >= 10:
                info_type, export_size = struct.unpack(">HQ", data[0:10])
                if info_type == NBD_INFO_EXPORT:
                    size = export_size
            elif rep_type == NBD_REP_ACK:
                break
            elif rep_type & 0x80000000:
                raise SidonError("nbd server refused NBD_OPT_GO (reply type 0x%x)" % rep_type, "refused")
        if size is None:
            raise SidonError("nbd server never reported the export size", "io")
        return size

    def _request(self, cmd, offset, length, data=None, want_data=False):
        import struct
        self.handle += 1
        self.sock.sendall(struct.pack(">IHHQQI", NBD_REQUEST_MAGIC, 0, cmd,
                                      self.handle, offset, length))
        if data:
            self.sock.sendall(data)
        if cmd == NBD_CMD_DISC:
            return
        reply = self._recv(16)
        magic, errno, handle = struct.unpack(">IIQ", reply)
        if magic != NBD_SIMPLE_REPLY_MAGIC:
            raise SidonError("nbd reply magic 0x%x; the stream is desynchronised" % magic, "io")
        if handle != self.handle:
            raise SidonError("nbd reply handle %d does not match request %d" % (handle, self.handle), "io")
        if errno:
            raise SidonError("nbd server returned errno %d at offset %d" % (errno, offset), "io")
        if want_data:
            return self._recv(length)
        return None

    def write_stream(self, stream, total, progress=None):
        """Copy `total` bytes from `stream` into the vdisk. Returns bytes written."""
        if total > self.size:
            raise SidonError(
                "image is %d bytes but the vdisk holds %d" % (total, self.size), "refused")
        written = 0
        while written < total:
            want = min(NBD_CHUNK, total - written)
            chunk = stream.read(want)
            if not chunk:
                break
            # A short read is not an error here; it is a slow client. Write what arrived
            # and keep the offset honest rather than padding to the requested length.
            self._request(NBD_CMD_WRITE, written, len(chunk), chunk)
            written += len(chunk)
            if progress:
                progress(written)
        return written

    def read(self, offset, length):
        """Read `length` bytes at `offset`.

        Here because verifying storage means reading it back. A writer that cannot read
        can confirm that a write was accepted and never that the right bytes are there,
        and "the server said OK" is the weakest possible evidence about a data path.
        """
        if length > NBD_CHUNK:
            out = []
            done = 0
            while done < length:
                want = min(NBD_CHUNK, length - done)
                out.append(self._request(NBD_CMD_READ, offset + done, want, want_data=True))
                done += want
            return b"".join(out)
        return self._request(NBD_CMD_READ, offset, length, want_data=True)

    def write_at(self, offset, data):
        """Write `data` at `offset`. `write_stream` is the bulk path; this is the small one."""
        if offset + len(data) > self.size:
            raise SidonError(
                "write of %d bytes at %d runs past the end of a %d byte vdisk"
                % (len(data), offset, self.size), "refused")
        done = 0
        while done < len(data):
            chunk = data[done:done + NBD_CHUNK]
            self._request(NBD_CMD_WRITE, offset + done, len(chunk), chunk)
            done += len(chunk)
        return done

    def flush(self):
        self._request(NBD_CMD_FLUSH, 0, 0)

    def close(self):
        try:
            self._request(NBD_CMD_DISC, 0, 0)
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass


def disk_xml(vdisk_id, dev_letter, vcpu=1, nbd_dir=NBD_DIR):
    """The libvirt <disk> element for a Sidon-backed vdisk.

    `type='network'` with a unix transport: qemu speaks NBD to the local daemon over the
    socket, never to a block device. There is no `/dev/` node to leak, no kernel client
    in the path, and nothing to promote or demote before a guest can start.
    """
    return (
        "\n    <disk type='network' device='disk'>"
        "\n      <driver name='qemu' type='raw' cache='none' io='native' queues='%d' iothread='1'/>"
        "\n      <source protocol='nbd' name='%s'>"
        "\n        <host transport='unix' socket='%s'/>"
        "\n      </source>"
        "\n      <target dev='vd%s' bus='virtio'/>"
        "\n    </disk>"
    ) % (max(1, int(vcpu)), vdisk_id, nbd_socket(vdisk_id, nbd_dir), dev_letter)
