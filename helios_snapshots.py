#!/usr/bin/env python3
"""Scheduled snapshots, the retention that keeps them bounded, and rollback.

Taking a snapshot has been a command since Sidon had them. What was missing is everything
around it that makes it a backup tier rather than a button: something that takes them on a
timer, something that stops them accumulating, and a way to put a VM back that is not a
clone under a new name.

This module is the policy. The mechanism stays in Sidon, and the scheduling stays in Dagur:

  * **There is no new daemon.** Dagur already runs jobs on an interval, claims each tick
    once cluster-wide (`/v1/schedule/claim-job`), and runs it on the node holding the
    `dagur-queue` candidacy. The `snapshot_policy` job is `valcli storage.snapshot-run`, and
    inherits both properties. Nothing here asks who leads ZooKeeper, or compares an address
    to anything: that question was removed from ten places for being the wrong one.
  * **A run is a task.** Dagur's own task is the parent; each snapshot taken, each snapshot
    pruned and each rollback is a child row in `hydra.catalyst_tasks`, with the component and
    sequence every task carries. A failure is a `failed` row in the console's task ring and
    a non-zero exit from the job, which is what makes Dagur record the run failed.

Every function that decides something takes plain values and returns plain values, so the
decisions -- which policy applies, whether a snapshot is due, what retention may delete,
whether a rollback is allowed -- are tested without a cluster. `Runner` is the thin layer
that reads and writes the cluster, with every effect injected through `Env`.
"""

import json
import os
import re
import time
import urllib.error
import urllib.request
import uuid

POLICY_TABLE = "hydra.dfs_snapshot_policies"
INDEX_TABLE = "hydra.dfs_snapshot_index"

SCOPE_CLUSTER = "cluster"
SCOPE_CONTAINER = "container"
SCOPE_VDISK = "vdisk"
SCOPES = (SCOPE_CLUSTER, SCOPE_CONTAINER, SCOPE_VDISK)
# Narrowest first: the order `effective_policy` consults them in.
SCOPE_PRECEDENCE = (SCOPE_VDISK, SCOPE_CONTAINER, SCOPE_CLUSTER)
CLUSTER_TARGET = "*"

ORIGIN_POLICY = "policy"
ORIGIN_MANUAL = "manual"
ORIGIN_PRE_ROLLBACK = "pre-rollback"

# How often the Dagur job runs, and therefore the shortest interval a policy can honestly
# promise. A policy asking for ten minutes would be a lie the scheduler could not keep.
RUN_INTERVAL_SECONDS = 3600
# A snapshot is due when the newest is within this much of a full interval. The scheduler
# fires at its interval give or take a poll, so without slack a one-hour policy would take
# a snapshot every other run: the newest is 3599.8 seconds old when the next tick arrives.
DUE_SLACK_SECONDS = 300
MAX_KEEP_LAST = 1000

# The same shape spark-daemon accepts for a vdisk id (NAME_RE there), and for the same
# reason: these values end up inside CQL text and inside a socket path.
NAME_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.-]{0,62}\Z")

TASK_COMPONENT = "Catalyst"
TASK_SERVICE = "dagur"

DARUK_URL = "http://127.0.0.1:9043"
CLUSTER_JSON = "/etc/hci/cluster.json"


class PolicyError(ValueError):
    """A policy that cannot be stored, with the reason an operator can act on."""


class RollbackRefused(Exception):
    """A rollback that must not start, with the reason an operator can act on."""


# -- reading ------------------------------------------------------------------------------

def parse_json_rows(stdout):
    """The rows of a `SELECT JSON` as dicts. Non-row lines (headers, blanks) are skipped."""
    rows = []
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not (line.startswith("{") and line.endswith("}")):
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return rows


def cql_text(value):
    """A string literal. Only ever applied to values that have already passed NAME_RE or
    were generated here; the doubling is belt and braces, not the guard."""
    return "'%s'" % str(value).replace("'", "''")


# -- policy -------------------------------------------------------------------------------

def validate_policy(scope, target, interval_seconds, keep_last):
    """Raise PolicyError unless this policy could be honoured. Returns the normalised tuple."""
    if scope not in SCOPES:
        raise PolicyError("scope must be one of %s, not %r" % (", ".join(SCOPES), scope))
    if scope == SCOPE_CLUSTER:
        target = CLUSTER_TARGET
    elif not NAME_RE.match(str(target or "")):
        raise PolicyError("%r is not a valid %s name" % (target, scope))
    try:
        interval_seconds = int(interval_seconds)
        keep_last = int(keep_last)
    except (TypeError, ValueError):
        raise PolicyError("interval and keep must be whole numbers")
    if interval_seconds < RUN_INTERVAL_SECONDS:
        raise PolicyError(
            "the interval cannot be shorter than %d seconds: the job that takes snapshots "
            "runs that often, so a shorter promise could not be kept" % RUN_INTERVAL_SECONDS)
    # Zero kept would make retention delete the snapshot it had just taken, which is a
    # policy that takes snapshots in order to throw them away.
    if keep_last < 1 or keep_last > MAX_KEEP_LAST:
        raise PolicyError("keep must be between 1 and %d" % MAX_KEEP_LAST)
    return scope, target, interval_seconds, keep_last


def policy_insert_statement(scope, target, enabled, interval_seconds, keep_last, now_ms):
    scope, target, interval_seconds, keep_last = validate_policy(
        scope, target, interval_seconds, keep_last)
    return (
        "INSERT INTO %s (scope, target, enabled, interval_seconds, keep_last, updated_at_ms) "
        "VALUES (%s, %s, %s, %d, %d, %d);"
        % (POLICY_TABLE, cql_text(scope), cql_text(target),
           "true" if enabled else "false", interval_seconds, keep_last, int(now_ms)))


def policy_delete_statement(scope, target):
    if scope not in SCOPES:
        raise PolicyError("scope must be one of %s" % ", ".join(SCOPES))
    if scope == SCOPE_CLUSTER:
        target = CLUSTER_TARGET
    elif not NAME_RE.match(str(target or "")):
        raise PolicyError("%r is not a valid %s name" % (target, scope))
    return ("DELETE FROM %s WHERE scope = %s AND target = %s;"
            % (POLICY_TABLE, cql_text(scope), cql_text(target)))


def effective_policy(policies, vdisk_id, container):
    """The policy that governs one vdisk, or None.

    The narrowest row that exists wins -- vdisk, then container, then cluster -- *including
    a disabled one*, which is how a single disk is exempted from a cluster-wide policy. A
    disabled winner returns None rather than falling through to the broader row: falling
    through would make the exemption impossible to express.
    """
    by_key = {(p.get("scope"), p.get("target")): p for p in policies}
    for scope in SCOPE_PRECEDENCE:
        target = {SCOPE_VDISK: vdisk_id, SCOPE_CONTAINER: container,
                  SCOPE_CLUSTER: CLUSTER_TARGET}[scope]
        row = by_key.get((scope, target))
        if row is None:
            continue
        if not row.get("enabled"):
            return None
        return row
    return None


# -- taking ------------------------------------------------------------------------------

def snapshot_name(vdisk_id, now_ms):
    """`<vdisk>-auto-YYYYMMDDHHMM`, in UTC.

    Minute resolution on purpose: two runs of the job in the same minute -- the scheduled
    one and an operator's by hand -- compute the *same* name, and the second is refused by
    Sidon as already existing instead of producing a near-duplicate. The time is in the name
    so a listing sorts and reads without a lookup, but retention never parses it: ordering
    comes from `created_at_ms` in the index.

    Raises PolicyError when the result is not a valid vdisk id; the one way that happens is
    a vdisk id so long that the suffix pushes it past 63 characters.
    """
    stamp = time.strftime("%Y%m%d%H%M", time.gmtime(int(now_ms) / 1000.0))
    name = "%s-auto-%s" % (vdisk_id, stamp)
    if not NAME_RE.match(name):
        raise PolicyError(
            "%r is too long to take an automatic snapshot name from (the result must be at "
            "most 63 characters)" % vdisk_id)
    return name


def is_due(policy, newest_policy_snapshot_ms, now_ms):
    """Whether a snapshot should be taken now.

    Measured from the newest *policy* snapshot and not from the last attempt: a run that
    failed took nothing, so the next one must try again immediately rather than wait out an
    interval for a snapshot that does not exist. A manual snapshot does not count either --
    an operator snapshotting before a change has not satisfied the schedule.
    """
    if newest_policy_snapshot_ms is None:
        return True
    interval_ms = int(policy.get("interval_seconds") or RUN_INTERVAL_SECONDS) * 1000
    return int(now_ms) - int(newest_policy_snapshot_ms) >= interval_ms - DUE_SLACK_SECONDS * 1000


# -- retention ---------------------------------------------------------------------------

class Retention(object):
    """What a retention pass will do, and why it will not do the rest."""

    def __init__(self, keep, prune, protected):
        self.keep = keep            # snapshot ids inside the keep_last window
        self.prune = prune          # snapshot ids that may be deleted, oldest last
        self.protected = protected  # {snapshot_id: reason it was spared}


def plan_retention(snapshots, keep_last, referenced, attached):
    """Which snapshots a policy may delete.

    `snapshots` are the index rows for one vdisk: dicts with `snapshot_id`, `created_at_ms`
    and `origin`. Only `policy` rows are ever candidates -- a snapshot a person took, or one
    taken before a rollback, is not the policy's to delete however old it is.

    The newest `keep_last` policy snapshots are kept. Older ones are pruned unless something
    depends on them:

      * **`referenced`** -- every vdisk named as some other vdisk's `parent_vdisk`. A clone
        of this snapshot (or a snapshot taken *of* it) is lineage that deleting the snapshot
        would leave pointing at nothing. The data would survive -- Purah marks from every
        block-map row, so the child's own rows keep its extent groups alive -- but
        `storage.children` would lose the only record of where the child came from, and a
        child with no recorded parent is indistinguishable from one that never had any.
      * **`attached`** -- a snapshot some node is serving right now. Sidon refuses to delete
        one it serves itself, but only on that node, and a read-only attach on another node
        is exactly where a backup reader would be.

    A protected snapshot is left alone and reported, and it does *not* count towards
    `keep_last`: the window is "the newest N the policy took", so a pinned old snapshot
    costs one extra rather than displacing a recent one. Nothing cascades: sparing a snapshot
    never causes a younger one to be deleted instead.
    """
    ours = sorted(
        (s for s in snapshots if s.get("origin") == ORIGIN_POLICY),
        key=lambda s: (-int(s.get("created_at_ms") or 0), s.get("snapshot_id", "")))
    keep = [s["snapshot_id"] for s in ours[:max(1, int(keep_last))]]
    prune, protected = [], {}
    for snap in ours[max(1, int(keep_last)):]:
        sid = snap["snapshot_id"]
        if sid in referenced:
            protected[sid] = "a vdisk was derived from it (see storage.children)"
        elif sid in attached:
            protected[sid] = "it is attached on %s" % attached[sid]
        else:
            prune.append(sid)
    return Retention(keep, prune, protected)


# -- rollback ----------------------------------------------------------------------------

_VM_DISK_RE = re.compile(r"\A(.+)-disk(\d+)\Z")


def vm_for_vdisk(vdisk_id):
    """The VM a vdisk belongs to by the `<vm>-disk<n>` convention, or None."""
    match = _VM_DISK_RE.match(vdisk_id or "")
    return match.group(1) if match else None


def rollback_refusal(vdisk_id, snapshot_id, vdisk_row, snapshot_row, vm_row,
                     attached_on, unreachable):
    """Why a rollback must not start, or None when the checks here pass.

    These are the checks only the control plane can make, because Sidon sees its own node
    and nothing else: whether the VM that uses the disk is running, and whether *any* node
    is serving the vdisk. Sidon makes its own checks again (class, lineage, ownership,
    attachment on its node) and refuses independently; this is the first line, and it is the
    one that knows about VMs.

    `unreachable` is a refusal and not a warning. A node that does not answer might be the
    one with the guest on it, and "I could not check" is not "it is detached".
    """
    if vdisk_row is None:
        return "no vdisk named '%s'" % vdisk_id
    if snapshot_row is None:
        return "no snapshot named '%s'" % snapshot_id
    if (snapshot_row.get("parent_vdisk") or "") != vdisk_id:
        return "'%s' is not a snapshot of '%s'" % (snapshot_id, vdisk_id)
    if vm_row is not None:
        state = str(vm_row.get("state") or "").strip().lower()
        status = str(vm_row.get("status") or "").strip()
        if state != "stopped":
            return ("VM %s is %s. Rolling its disk back under a running guest changes the "
                    "bytes beneath its filesystem; stop the VM first."
                    % (vm_row.get("name"), vm_row.get("state") or "in an unknown state"))
        if status:
            return ("VM %s is mid-operation (%s); wait for it to finish." % (vm_row.get("name"), status))
    if attached_on:
        return ("vdisk %s is attached on %s. Stop whatever is using it and detach it first."
                % (vdisk_id, ", ".join(sorted(attached_on))))
    if unreachable:
        return ("could not ask %s whether %s is attached, and a node that does not answer "
                "might be the one serving it" % (", ".join(sorted(unreachable)), vdisk_id))
    return None


def pre_rollback_name(vdisk_id, now_ms):
    """The name the safety copy taken before a rollback is given."""
    stamp = time.strftime("%Y%m%d%H%M", time.gmtime(int(now_ms) / 1000.0))
    name = "%s-pre-rollback-%s" % (vdisk_id, stamp)
    if not NAME_RE.match(name):
        raise PolicyError("%r is too long to name a pre-rollback copy from" % vdisk_id)
    return name


# -- the cluster ----------------------------------------------------------------------------

def cluster_nodes(path=CLUSTER_JSON):
    """[{hostname, ip}] from cluster.json; empty when it cannot be read."""
    try:
        with open(path, "r") as handle:
            hosts = json.load(handle).get("hosts") or []
    except (IOError, OSError, ValueError):
        return []
    return [{"hostname": h.get("hostname"), "ip": h.get("ip")} for h in hosts if h.get("ip")]


def run_lwt(endpoint, params, timeout=15):
    """One Daruk compare-and-swap, as `(ok, applied, current, error)`.

    The same contract as catalyst.run_lwt, restated here because that module is a daemon
    and this one is imported by a CLI.
    """
    try:
        request = urllib.request.Request(
            DARUK_URL + endpoint, data=json.dumps(params).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return False, False, {}, "Daruk is not answering on %s: %s" % (DARUK_URL, exc)
    if body.get("status") != "success":
        return False, False, {}, body.get("error", "compare-and-swap failed")
    return True, bool(body.get("applied")), body.get("current") or {}, ""


class Env(object):
    """Every effect the runner has on the world, injected.

    `query(cql) -> (rc, stdout, stderr)`; `dfs(ip, payload) -> (rc, body, err)` is one call
    to a node's `/api/v1/dfs/vdisk`; `nodes() -> [{hostname, ip}]`; `lwt` and `now_ms` are
    what they sound like. Tests pass fakes; valcli passes its own helpers.
    """

    def __init__(self, query, dfs, nodes=cluster_nodes, lwt=run_lwt,
                 now_ms=lambda: int(time.time() * 1000), say=None, parent_task_id=None):
        self.query = query
        self.dfs = dfs
        self.nodes = nodes
        self.lwt = lwt
        self.now_ms = now_ms
        self.say = say or (lambda line: None)
        self.parent_task_id = parent_task_id


class TaskLog(object):
    """Writes this module's work into the Catalyst task table.

    Rows are written with the statements `helios_schema` owns, so they carry the parent,
    component and sequence every other writer's rows do. A failure to record a task is
    never a failure of the work: the snapshot either happened or it did not, and an
    unreachable task table must not turn a good run into a bad one.
    """

    def __init__(self, env, schema):
        self.env = env
        self.schema = schema
        self._hint = None

    def _sequence(self):
        try:
            claimed = self.schema.claim_task_sequence(
                self.env.lwt, TASK_COMPONENT, expected=self._hint)
        except Exception:
            return None
        if claimed is not None:
            self._hint = claimed
        return claimed

    def start(self, task_type, action, payload):
        """Record a task as `processing` and return its id, or None if it could not be."""
        task_id = str(uuid.uuid4())
        try:
            statement = self.schema.task_insert_statement(
                task_id, TASK_SERVICE, action, json.dumps(payload), self.env.now_ms(),
                component=TASK_COMPONENT, task_type=task_type,
                sequence_id=self._sequence(), parent_task_id=self.env.parent_task_id,
                status=self.schema.TASK_PROCESSING)
            rc, _out, _err = self.env.query(statement)
        except Exception:
            return None
        return task_id if rc == 0 else None

    def finish(self, task_id, error=None):
        if not task_id:
            return
        status = self.schema.TASK_FAILED if error else self.schema.TASK_COMPLETED
        try:
            self.env.query(self.schema.task_update_statement(
                task_id, status, 100, self.env.now_ms(), error_msg=error))
        except Exception:
            pass


class Summary(object):
    def __init__(self):
        self.taken = []
        self.pruned = []
        self.skipped = []   # (vdisk, reason): not a failure, but never silent
        self.spared = []    # (snapshot, reason)
        self.failures = []  # (what, reason)

    @property
    def ok(self):
        return not self.failures


class Runner(object):
    """One pass of the snapshot policy over the cluster."""

    def __init__(self, env, schema, dry_run=False):
        self.env = env
        self.schema = schema
        self.dry_run = dry_run
        self.tasks = TaskLog(env, schema)

    # reading ---------------------------------------------------------------------------

    def _rows(self, cql):
        rc, out, err = self.env.query(cql)
        if rc != 0:
            raise RuntimeError("could not read the cluster: %s" % (err or cql))
        return parse_json_rows(out)

    def policies(self):
        return self._rows("SELECT JSON scope, target, enabled, interval_seconds, keep_last, "
                          "updated_at_ms FROM %s;" % POLICY_TABLE)

    def vdisks(self):
        return self._rows("SELECT JSON vdisk_id, class, owner, epoch, container, parent_vdisk, "
                          "size_bytes, created_at_ms FROM hydra.dfs_vdisks;")

    def index(self, vdisk_id):
        return self._rows("SELECT JSON vdisk_id, created_at_ms, snapshot_id, origin FROM %s "
                          "WHERE vdisk_id = %s;" % (INDEX_TABLE, cql_text(vdisk_id)))

    def attached(self):
        """({vdisk_id: [node, ...]}, [unreachable nodes]) from every node's own view.

        Sidon knows only what it serves itself, so there is no single place to ask. A node
        that does not answer is reported, not assumed empty.
        """
        where, unreachable = {}, []
        for node in self.env.nodes():
            rc, body, _err = self.env.dfs(node["ip"], {"op": "list"})
            if rc != 0 or not isinstance(body, dict) or "attached" not in body:
                unreachable.append(node.get("hostname") or node["ip"])
                continue
            for item in body.get("attached") or []:
                where.setdefault(item.get("vdisk_id"), []).append(
                    "%s (%s)" % (node.get("hostname") or node["ip"], item.get("role", "?")))
        return where, unreachable

    def _ip_for_owner(self, hostname):
        for node in self.env.nodes():
            if node.get("hostname") == hostname:
                return node["ip"]
        return None

    # the run -----------------------------------------------------------------------------

    def run(self):
        summary = Summary()
        all_policies = self.policies()
        if not any(p.get("enabled") for p in all_policies):
            self.env.say("No snapshot policy is enabled, so there is nothing to do. "
                         "See `valcli storage.snapshot-policy`.")
            return summary

        vdisks = self.vdisks()
        referenced = set(v.get("parent_vdisk") for v in vdisks if v.get("parent_vdisk"))
        where, unreachable = self.attached()
        owners = {}
        for vdisk_id, places in where.items():
            for place in places:
                if place.endswith("(owner)"):
                    owners[vdisk_id] = place.split(" ")[0]

        for vdisk in sorted(vdisks, key=lambda v: v.get("vdisk_id", "")):
            vdisk_id = vdisk.get("vdisk_id")
            if (vdisk.get("class") or "rw") != "rw":
                continue
            policy = effective_policy(all_policies, vdisk_id, vdisk.get("container"))
            if policy is None:
                continue
            self._one_vdisk(summary, vdisk, policy, owners, where, unreachable, referenced)
        return summary

    def _one_vdisk(self, summary, vdisk, policy, owners, where, unreachable, referenced):
        vdisk_id = vdisk["vdisk_id"]
        index = self.index(vdisk_id)
        ours = [s for s in index if s.get("origin") == ORIGIN_POLICY]
        newest = max(int(s.get("created_at_ms") or 0) for s in ours) if ours else None
        now = self.env.now_ms()
        failed_here = False

        if is_due(policy, newest, now):
            owner = owners.get(vdisk_id)
            if owner is None:
                reason = "not attached on any node, so there is nothing new to capture"
                if unreachable:
                    reason = ("not seen attached; %s did not answer, so it may be on one of "
                              "them" % ", ".join(sorted(unreachable)))
                summary.skipped.append((vdisk_id, reason))
                self.env.say("skipped %s: %s" % (vdisk_id, reason))
            else:
                failed_here = not self._take(summary, vdisk, owner, now)

        # Never prune on the strength of a run that could not take its own snapshot: a
        # cluster that cannot snapshot must not also be shedding the snapshots it has.
        if failed_here:
            return
        # Nor while a node is unreachable: it might be the one serving a snapshot.
        if unreachable:
            return
        attached = dict((vid, ", ".join(places)) for vid, places in where.items())
        plan = plan_retention(self.index(vdisk_id), policy.get("keep_last") or 1,
                              referenced, attached)
        for sid, reason in sorted(plan.protected.items()):
            summary.spared.append((sid, reason))
            self.env.say("kept %s beyond keep=%s: %s" % (sid, policy.get("keep_last"), reason))
        for sid in plan.prune:
            self._prune(summary, vdisk_id, sid)

    def _take(self, summary, vdisk, owner_hostname, now):
        vdisk_id = vdisk["vdisk_id"]
        try:
            child = snapshot_name(vdisk_id, now)
        except PolicyError as exc:
            summary.failures.append((vdisk_id, str(exc)))
            return False
        if self.dry_run:
            self.env.say("would snapshot %s as %s" % (vdisk_id, child))
            return True
        ip = self._ip_for_owner(owner_hostname)
        task = self.tasks.start("snapshot_policy", "snapshot",
                                {"vdisk_id": vdisk_id, "snapshot_id": child})
        if ip is None:
            error = "no address for node %s in cluster.json" % owner_hostname
        else:
            rc, body, err = self.env.dfs(
                ip, {"op": "snapshot", "vdisk_id": vdisk_id, "child_id": child})
            error = None
            if rc != 0:
                error = (body.get("error") if isinstance(body, dict) and body.get("error")
                         else err) or "snapshot refused"
                # The same minute's run got there first: not a failure, and not a new row.
                if "already exists" in str(error):
                    self.tasks.finish(task)
                    self.env.say("%s already exists; another run took it" % child)
                    return True
        if error:
            self.tasks.finish(task, "snapshot of %s failed: %s" % (vdisk_id, error))
            summary.failures.append((vdisk_id, error))
            self.env.say("FAILED snapshot of %s: %s" % (vdisk_id, error))
            return False
        self.record(vdisk_id, child, ORIGIN_POLICY, now)
        self.tasks.finish(task)
        summary.taken.append(child)
        self.env.say("took %s" % child)
        return True

    def record(self, vdisk_id, snapshot_id, origin, now_ms):
        """Index a snapshot. A failure here is reported by the caller's exit status only
        through the next listing: the snapshot exists, and an unindexed snapshot is merely
        one retention will never touch."""
        self.env.query(
            "INSERT INTO %s (vdisk_id, created_at_ms, snapshot_id, origin) "
            "VALUES (%s, %d, %s, %s);"
            % (INDEX_TABLE, cql_text(vdisk_id), int(now_ms), cql_text(snapshot_id),
               cql_text(origin)))

    def _children_now(self, snapshot_id):
        """Vdisks derived from `snapshot_id`, read fresh. The retention plan was made from a
        listing taken a moment earlier; this is the second look, immediately before the
        delete, and the only one that can see a clone made in between."""
        return [v.get("vdisk_id") for v in self._rows(
            "SELECT JSON vdisk_id, parent_vdisk FROM hydra.dfs_vdisks;")
            if v.get("parent_vdisk") == snapshot_id]

    def _prune(self, summary, vdisk_id, snapshot_id):
        if self.dry_run:
            self.env.say("would delete %s" % snapshot_id)
            return
        task = None
        try:
            children = self._children_now(snapshot_id)
            if children:
                summary.spared.append(
                    (snapshot_id, "a vdisk was derived from it: %s" % ", ".join(children)))
                self.env.say("kept %s: %s was derived from it" % (snapshot_id, ", ".join(children)))
                return
            task = self.tasks.start("snapshot_prune", "snapshot_prune",
                                    {"vdisk_id": vdisk_id, "snapshot_id": snapshot_id})
            nodes = self.env.nodes()
            ip = nodes[0]["ip"] if nodes else "127.0.0.1"
            rc, body, err = self.env.dfs(ip, {"op": "delete", "vdisk_id": snapshot_id})
            if rc != 0:
                raise RuntimeError((body.get("error") if isinstance(body, dict) and body.get("error")
                                    else err) or "delete refused")
            self.env.query("DELETE FROM %s WHERE vdisk_id = %s AND created_at_ms = %d "
                           "AND snapshot_id = %s;" % (
                               INDEX_TABLE, cql_text(vdisk_id),
                               self._created_ms(vdisk_id, snapshot_id), cql_text(snapshot_id)))
        except Exception as exc:
            self.tasks.finish(task, "pruning %s failed: %s" % (snapshot_id, exc))
            summary.failures.append((snapshot_id, str(exc)))
            self.env.say("FAILED to prune %s: %s" % (snapshot_id, exc))
            return
        self.tasks.finish(task)
        summary.pruned.append(snapshot_id)
        self.env.say("deleted %s" % snapshot_id)

    def _created_ms(self, vdisk_id, snapshot_id):
        for row in self.index(vdisk_id):
            if row.get("snapshot_id") == snapshot_id:
                return int(row.get("created_at_ms") or 0)
        return 0

    # rollback ----------------------------------------------------------------------------

    def rollback(self, vdisk_id, snapshot_id, keep=True):
        """Put a detached vdisk back to a snapshot of itself. Returns Sidon's answer.

        Raises RollbackRefused with the reason when the control-plane checks fail, and
        RuntimeError carrying Sidon's own message when it refuses or fails.
        """
        rows = dict((v.get("vdisk_id"), v) for v in self._rows(
            "SELECT JSON vdisk_id, class, owner, epoch, parent_vdisk FROM hydra.dfs_vdisks;"))
        vm_row = None
        vm_name = vm_for_vdisk(vdisk_id)
        if vm_name:
            found = self._rows("SELECT JSON name, state, status FROM hydra.vms WHERE name = %s;"
                               % cql_text(vm_name)) if NAME_RE.match(vm_name) else []
            vm_row = found[0] if found else None
        where, unreachable = self.attached()
        refusal = rollback_refusal(
            vdisk_id, snapshot_id, rows.get(vdisk_id), rows.get(snapshot_id), vm_row,
            where.get(vdisk_id) or [], unreachable)
        if refusal:
            raise RollbackRefused(refusal)

        now = self.env.now_ms()
        keep_as = pre_rollback_name(vdisk_id, now) if keep else None
        owner = (rows[vdisk_id].get("owner") or "").strip()
        ip = self._ip_for_owner(owner) if owner else None
        if ip is None:
            nodes = self.env.nodes()
            ip = nodes[0]["ip"] if nodes else "127.0.0.1"

        task = self.tasks.start("snapshot_rollback", "snapshot_rollback",
                                {"vdisk_id": vdisk_id, "snapshot_id": snapshot_id,
                                 "keep_as": keep_as})
        payload = {"op": "rollback", "vdisk_id": vdisk_id, "snapshot_id": snapshot_id}
        if keep_as:
            payload["keep_as"] = keep_as
        rc, body, err = self.env.dfs(ip, payload)
        if rc != 0:
            message = (body.get("error") if isinstance(body, dict) and body.get("error")
                       else err) or "rollback refused"
            self.tasks.finish(task, "rollback of %s to %s failed: %s"
                              % (vdisk_id, snapshot_id, message))
            raise RuntimeError(message)
        if keep_as and body.get("kept_as"):
            self.record(vdisk_id, keep_as, ORIGIN_PRE_ROLLBACK, now)
        self.tasks.finish(task)
        return body
