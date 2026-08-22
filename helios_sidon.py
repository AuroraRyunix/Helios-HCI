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

# The engine names `cluster.json` may carry. "linstor" is what every cluster provisioned
# to date holds; the key has existed since the GlusterFS transition and was, until now,
# read by four hardcoded copies of get_dfs_engine() that all returned "linstor" and were
# called by nothing.
ENGINE_LINSTOR = "linstor"
ENGINE_SIDON = "sidon"


class SidonError(Exception):
    """A refusal or failure from the daemon. `kind` distinguishes retryable from not."""

    def __init__(self, message, kind="unknown"):
        super().__init__(message)
        self.kind = kind


def dfs_engine(cluster_json=CLUSTER_JSON):
    """Which storage engine this cluster uses for VM disks.

    Defaults to LINSTOR when the file is missing or unreadable, because a cluster whose
    configuration cannot be read is a cluster whose existing disks are DRBD resources.
    Guessing the new engine there would point a VM at storage that has never held it.
    """
    try:
        with open(cluster_json, "r", encoding="utf-8") as handle:
            value = json.load(handle).get("dfs_engine", ENGINE_LINSTOR)
    except (OSError, ValueError):
        return ENGINE_LINSTOR
    value = str(value or "").strip().lower()
    return value if value in (ENGINE_LINSTOR, ENGINE_SIDON) else ENGINE_LINSTOR


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


def status(vdisk_id, **kw):
    return call("status", vdisk_id=vdisk_id, **kw)


def list_attached(**kw):
    return call("list", **kw)


def flush(vdisk_id, **kw):
    """Force a drain. Not needed for durability -- writes are already durable in the
    journal -- but it bounds replay time and is what a clean shutdown should do."""
    return call("flush", vdisk_id=vdisk_id, **kw)


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
