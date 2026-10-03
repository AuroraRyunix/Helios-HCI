#!/usr/bin/env python3
"""The control-plane half of replicating a snapshot to another site: what to send, and how a
transfer is driven to completion.

**Status: exercised only against fakes.** There is no transport here, no listener and no second
site; `Remote` is an interface and the tests implement it with an object that can drop the
link, run out of space and be offered the same snapshot twice. The design is
`docs/dfs/replication.md` (D-28 to D-31). The bytes are moved by Sidon
(`sidon/src/replicate*`), never by Python; this module decides *what* and drives the retries.

Pure functions build the manifest from `dfs_block_map` and `dfs_egroups` rows and decide the
delta against what the remote reports holding. `Replicator` is the thin loop that offers,
sends, publishes and records state, with every effect injected.

This is a library for the Rauru daemon: it imports no daemon, asks nobody who leads ZooKeeper,
and holds no candidacy.
"""

import hashlib
import json
import re

FORMAT = 1
FOOTER_LEN = 32
CHUNK = 1 << 20
MAX_GROUP = 256 << 20

NAME_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.~-]{0,199}\Z")

# A transfer's states. `paused` is not a failure: staged bytes are kept and the next attempt
# resumes. `failed` is permanent until a person looks.
OFFERED = "offered"
SENDING = "sending"
PUBLISHING = "publishing"
VISIBLE = "visible"
PAUSED = "paused"
FAILED = "failed"
STATES = (OFFERED, SENDING, PUBLISHING, VISIBLE, PAUSED, FAILED)
TRANSITIONS = {
    OFFERED: (SENDING, PUBLISHING, VISIBLE, PAUSED, FAILED),
    SENDING: (PUBLISHING, PAUSED, FAILED),
    PUBLISHING: (VISIBLE, PAUSED, FAILED),
    PAUSED: (OFFERED, FAILED),
    VISIBLE: (),
    FAILED: (OFFERED,),
}


class ReplicationError(Exception):
    """A manifest or a transfer that cannot proceed, with the reason an operator can act on."""


class Conflict(ReplicationError):
    """The remote holds the same name with different content. Never retried."""


class NoSpace(ReplicationError):
    """The remote cannot hold what is needed. The transfer pauses; nothing is deleted there."""


class LinkDropped(ReplicationError):
    """The connection failed mid-transfer. The transfer pauses and resumes."""


class GroupDamaged(ReplicationError):
    """A group did not verify, at the source or at the remote. Names the group."""


# -- names ----------------------------------------------------------------------------------

def replica_vdisk_name(site_id, snapshot):
    """The local name a replicated snapshot is stored under: chosen by the receiving site from
    the sending site's id and the snapshot's name, and never taken from the wire as a vdisk id.

    Qualified by site so two sites cannot collide, and bounded to the 63 characters a vdisk id
    may have: a name that would be longer keeps its start and ends in a digest of the whole, so
    two long names that share a prefix still differ.
    """
    for what, value in (("site id", site_id), ("snapshot", snapshot)):
        if not re.match(r"\A[A-Za-z0-9][A-Za-z0-9_.-]*\Z", str(value or "")):
            raise ReplicationError("%s %r cannot name a replica" % (what, value))
    site = str(site_id)[:8]
    name = "r-%s-%s" % (site, snapshot)
    if len(name) <= 63:
        return name
    tag = hashlib.sha256(("%s/%s" % (site_id, snapshot)).encode()).hexdigest()[:10]
    return "%s-%s" % (name[:52], tag)


# -- the manifest -----------------------------------------------------------------------------

def target_group_id(source_id, length, sealed):
    """A sealed group travels whole under its own id. One still open travels as its immutable
    prefix, named for the length so it is a complete file of its own on the remote."""
    return source_id if sealed else "%s~%d" % (source_id, length)


def build_manifest(snapshot, size_bytes, extent_bytes, block_rows, egroups):
    """The manifest of one snapshot, from `dfs_block_map` rows and `dfs_egroups` rows.

    `block_rows`: dicts with extent_index, egroup_id, egroup_offset, length, vdisk_hash (signed,
    as Hydra returns it) and optionally extent_id. `egroups`: {egroup_id: {state, size,
    seal_hash}}. Returns `(manifest, source_ids)` where `source_ids` maps each target group id
    to the id the source knows it by.

    Raises ReplicationError for what cannot be shipped: a row naming an `extent_id` (the third
    map level, D-23 stage 3, is not built), a group that is missing or dead, or a row reaching
    past its group.
    """
    per_group = {}
    for row in block_rows:
        if row.get("extent_id"):
            raise ReplicationError(
                "extent %s names an extent id; replicating the extent id map is not built"
                % row.get("extent_index"))
        per_group.setdefault(row["egroup_id"], []).append(row)

    groups, rows, source_ids = [], [], {}
    for gid in sorted(per_group):
        info = egroups.get(gid)
        if info is None or info.get("state") not in ("sealed", "open"):
            raise ReplicationError(
                "extent group %s is %s, so the snapshot cannot be replicated"
                % (gid, "unknown" if info is None else info.get("state")))
        end = max(int(r["egroup_offset"]) + int(r["length"]) + FOOTER_LEN for r in per_group[gid])
        sealed = info["state"] == "sealed"
        length = int(info["size"]) if sealed else end
        if end > length:
            raise ReplicationError("a row reaches past the end of extent group %s" % gid)
        if length > MAX_GROUP:
            raise ReplicationError("extent group %s is too large to ship" % gid)
        tid = target_group_id(gid, length, sealed)
        source_ids[tid] = gid
        groups.append({"id": tid, "length": length,
                       "seal": (info.get("seal_hash") or "") if sealed else ""})
        for r in per_group[gid]:
            rows.append({"extent_index": int(r["extent_index"]), "group": tid,
                         "offset": int(r["egroup_offset"]), "length": int(r["length"]),
                         "vdisk_hash": int(r["vdisk_hash"])})
    rows.sort(key=lambda r: r["extent_index"])
    manifest = {"format": FORMAT, "snapshot": snapshot, "size_bytes": int(size_bytes),
                "extent_bytes": int(extent_bytes), "rows": rows, "groups": groups}
    validate_manifest(manifest)
    return manifest, source_ids


def canonical(manifest):
    """The text the map digest is taken over: the same bytes `Manifest::canonical` produces in
    Sidon. The vdisk hash is signed because that is how Hydra stores it; there is no timestamp
    and no name in it, so the same content has the same digest on both sites and at any time."""
    text = "helios-map-v%d\nsize %d\nextent %d\n" % (
        FORMAT, manifest["size_bytes"], manifest["extent_bytes"])
    for r in manifest["rows"]:
        text += "%d %s %d %d %d\n" % (r["extent_index"], r["group"], r["offset"], r["length"],
                                      r["vdisk_hash"])
    return text


def map_digest(manifest):
    return hashlib.sha256(canonical(manifest).encode("utf-8")).hexdigest()


def validate_manifest(manifest):
    """Raise ReplicationError unless this could describe a snapshot and names nothing unsafe."""
    if manifest.get("format") != FORMAT:
        raise ReplicationError("unknown manifest format %r" % manifest.get("format"))
    if not NAME_RE.match(str(manifest.get("snapshot") or "")):
        raise ReplicationError("snapshot name %r is not acceptable" % manifest.get("snapshot"))
    lengths = {}
    for g in manifest["groups"]:
        if not NAME_RE.match(str(g["id"])) or str(g["id"]).startswith("."):
            raise ReplicationError("group id %r is not acceptable" % g["id"])
        if not 0 < g["length"] <= MAX_GROUP:
            raise ReplicationError("group %s has an unacceptable length" % g["id"])
        if g["id"] in lengths:
            raise ReplicationError("group %s is listed twice" % g["id"])
        lengths[g["id"]] = g["length"]
    previous = None
    for r in manifest["rows"]:
        if previous is not None and r["extent_index"] <= previous:
            raise ReplicationError("rows are not strictly ordered at extent %d" % r["extent_index"])
        previous = r["extent_index"]
        if r["group"] not in lengths:
            raise ReplicationError("row %d names group %s, which is not listed"
                                   % (r["extent_index"], r["group"]))
        if r["offset"] + r["length"] + FOOTER_LEN > lengths[r["group"]]:
            raise ReplicationError("row %d reaches past its group" % r["extent_index"])


def manifest_to_json(manifest):
    out = dict(manifest)
    out["rows"] = [[r["extent_index"], r["group"], r["offset"], r["length"], r["vdisk_hash"]]
                   for r in manifest["rows"]]
    out["groups"] = [[g["id"], g["length"], g["seal"]] for g in manifest["groups"]]
    out["map_sha256"] = map_digest(manifest)
    return json.dumps(out, sort_keys=True)


# -- the delta --------------------------------------------------------------------------------

HAVE_COMPLETE = "complete"
HAVE_ABSENT = "absent"
HAVE_PARTIAL = "partial"


class Delta(object):
    def __init__(self, ship, bytes_needed, complete):
        self.ship = ship                # [(group id, start offset)]
        self.bytes_needed = bytes_needed
        self.complete = complete        # group ids the remote already holds


def plan_delta(manifest, have):
    """What must be sent, given what the remote says it holds.

    `have` maps group id to `("complete",)`, `("absent",)` or `("partial", staged_bytes)`. A
    group the remote did not mention is treated as absent; one it mentions that is not in the
    manifest is ignored. A partial start is rounded down to a chunk boundary here as well as at
    the remote, because the remote's word is not what makes a resume safe: its end-of-group
    digest is.
    """
    ship, needed, complete = [], 0, []
    for g in manifest["groups"]:
        kind = (have.get(g["id"]) or (HAVE_ABSENT,))[0]
        if kind == HAVE_COMPLETE:
            complete.append(g["id"])
            continue
        start = 0
        if kind == HAVE_PARTIAL:
            staged = int(have[g["id"]][1])
            start = min(staged, g["length"]) // CHUNK * CHUNK
        ship.append((g["id"], start))
        needed += g["length"] - start
    return Delta(ship, needed, complete)


# -- state ------------------------------------------------------------------------------------

class StateStore(object):
    """Where transfer state lives. The daemon supplies a Hydra-backed one when the tables exist
    (docs/dfs/replication.md section 7); the tests use `MemoryStateStore`."""

    def get(self, site, set_id, snapshot):
        raise NotImplementedError

    def put(self, site, set_id, snapshot, record):
        raise NotImplementedError

    def records(self, site):
        raise NotImplementedError


class MemoryStateStore(StateStore):
    def __init__(self):
        self.data = {}

    def get(self, site, set_id, snapshot):
        return self.data.get((site, set_id, snapshot))

    def put(self, site, set_id, snapshot, record):
        self.data[(site, set_id, snapshot)] = dict(record)

    def records(self, site):
        return [dict(v, set_id=k[1], snapshot=k[2]) for k, v in self.data.items() if k[0] == site]


def advance(record, state, **fields):
    """Move a transfer record to `state`, refusing a transition that is not on the map: a
    visible snapshot does not become anything else, and a failed one is retried by an operator
    by going back to `offered`, never silently."""
    current = record.get("state")
    if current is not None and state != current and state not in TRANSITIONS.get(current, ()):
        raise ReplicationError("a transfer cannot go from %s to %s" % (current, state))
    record = dict(record, state=state)
    record.update(fields)
    return record


def pinned_sets(store, site, required_sets):
    """{set_id: reason} the retention must not delete: sets that are required at `site` and not
    yet visible there. Deleting the source of the only copy that has not arrived would be
    deleting the backup to save space. This is what the daemon returns from
    `rauru_protection.Runner.pinned_sets`."""
    pending = {}
    for set_id, snapshots in required_sets.items():
        done = all((store.get(site, set_id, s) or {}).get("state") == VISIBLE for s in snapshots)
        if not done:
            pending[set_id] = "not yet replicated to %s" % site
    return pending


# -- the loop ---------------------------------------------------------------------------------

class Remote(object):
    """What a remote site offers, over whatever transport the daemon builds. Methods raise the
    exceptions above; anything else is a bug."""

    def offer(self, job, manifest_json):
        """-> ("already_present",) or ("proceed", {group id: have}). Raises Conflict, NoSpace."""
        raise NotImplementedError

    def send(self, job, manifest, wants):
        """Send `wants` ([(group id, start)]) and receive them. Returns bytes sent. Raises
        LinkDropped, NoSpace, GroupDamaged."""
        raise NotImplementedError

    def publish(self, job, manifest_json):
        """-> "published" or "already_visible". Raises Conflict, ReplicationError."""
        raise NotImplementedError


def job_id(site, set_id, snapshot, digest):
    """Stable for one snapshot and map, so a retry after a restart finds its staged bytes."""
    return "j-" + hashlib.sha256(("%s/%s/%s/%s" % (site, set_id, snapshot, digest)).encode()).hexdigest()[:24]


class Result(object):
    def __init__(self, state, bytes_sent=0, attempts=0, error=None):
        self.state = state
        self.bytes_sent = bytes_sent
        self.attempts = attempts
        self.error = error


class Replicator(object):
    """Drives one snapshot to one site. `max_attempts` bounds how many times a dropped link is
    retried within one call; a paused transfer is picked up by the next call, from the remote's
    own account of what it holds."""

    def __init__(self, remote, store, say=None, max_attempts=3):
        self.remote = remote
        self.store = store
        self.say = say or (lambda line: None)
        self.max_attempts = max_attempts

    def replicate(self, site, set_id, manifest):
        digest = map_digest(manifest)
        snapshot = manifest["snapshot"]
        validate_manifest(manifest)
        job = job_id(site, set_id, snapshot, digest)
        record = self.store.get(site, set_id, snapshot) or {}
        if record.get("state") == VISIBLE and record.get("digest") == digest:
            return Result(VISIBLE)
        record = advance(record, OFFERED, digest=digest, job=job, error=None)
        self.store.put(site, set_id, snapshot, record)
        text = manifest_to_json(manifest)
        sent, attempts = int(record.get("bytes_sent") or 0), 0
        while attempts < self.max_attempts:
            attempts += 1
            try:
                answer = self.remote.offer(job, text)
                if answer[0] == "already_present":
                    return self._done(site, set_id, snapshot, record, sent, attempts)
                delta = plan_delta(manifest, answer[1])
                record = advance(record, SENDING, attempts=int(record.get("attempts") or 0) + 1)
                self.store.put(site, set_id, snapshot, record)
                if delta.ship:
                    sent += self.remote.send(job, manifest, delta.ship)
                record = advance(record, PUBLISHING, bytes_sent=sent)
                self.store.put(site, set_id, snapshot, record)
                self.remote.publish(job, text)
                return self._done(site, set_id, snapshot, record, sent, attempts)
            except LinkDropped as exc:
                self.say("link dropped sending %s to %s: %s" % (snapshot, site, exc))
                record = advance(record, PAUSED, error=str(exc), bytes_sent=sent)
                self.store.put(site, set_id, snapshot, record)
                if attempts >= self.max_attempts:
                    return Result(PAUSED, sent, attempts, str(exc))
                record = advance(record, OFFERED)
            except NoSpace as exc:
                # Not retried within this call: waiting a second will not make room, and
                # nothing here frees space on the remote.
                record = advance(record, PAUSED, error=str(exc), bytes_sent=sent)
                self.store.put(site, set_id, snapshot, record)
                return Result(PAUSED, sent, attempts, "no space: %s" % exc)
            except GroupDamaged as exc:
                # The remote restarts a damaged group from zero on its own; a second attempt is
                # that restart. A group that is damaged at the *source* fails every time.
                record = advance(record, PAUSED, error=str(exc), bytes_sent=sent)
                self.store.put(site, set_id, snapshot, record)
                if attempts >= self.max_attempts:
                    record = advance(record, FAILED, error=str(exc))
                    self.store.put(site, set_id, snapshot, record)
                    return Result(FAILED, sent, attempts, str(exc))
                record = advance(record, OFFERED)
            except Conflict as exc:
                record = advance(record, FAILED, error=str(exc))
                self.store.put(site, set_id, snapshot, record)
                return Result(FAILED, sent, attempts, str(exc))
        return Result(PAUSED, sent, attempts, record.get("error"))

    def _done(self, site, set_id, snapshot, record, sent, attempts):
        record = advance(record, VISIBLE, bytes_sent=sent, error=None)
        self.store.put(site, set_id, snapshot, record)
        return Result(VISIBLE, sent, attempts)

    def replicate_set(self, site, set_id, manifests):
        """Every member of a set. The set is visible at the remote only when each member is, and
        a failure of one does not stop the others from arriving (what has arrived is not
        wasted: groups are shared and a retry resends nothing that is already there)."""
        results = dict((m["snapshot"], self.replicate(site, set_id, m)) for m in manifests)
        state = VISIBLE if all(r.state == VISIBLE for r in results.values()) else (
            FAILED if any(r.state == FAILED for r in results.values()) else PAUSED)
        return state, results
