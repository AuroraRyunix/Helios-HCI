#!/usr/bin/env python3
"""Protection domains: a named group of VMs and vdisks snapshotted together, under one policy.

`helios_snapshots` snapshots one vdisk at a time. For a VM with three disks that means three
snapshots taken minutes apart, and a restore of them is three states of the same machine that
never coexisted: a database on disk 1 pointing at a log on disk 2 that is older or newer than
it. A protection domain is the unit that fixes that, and the unit a policy, a retention window
and (later) replication can be attached to.

What "together" can honestly mean here
--------------------------------------

The achievable target is **crash consistency across the disks**: the set is a state the
machine could have been left in by a power cut at one instant. The mechanism is the only one
available without a guest agent. Sidon snapshots a vdisk by draining its owner's journal and
copying its map; there is no operation that snapshots several vdisks at one instant. So the
guest is stopped from issuing writes for the duration, by suspending its vCPUs (`virsh
suspend`), every disk of the VM is snapshotted, and the guest is resumed.

Why that is crash-consistent, and what it is not:

  * Writes the guest had completed before the suspend are in every snapshot. Writes in flight
    at the suspend may land in some snapshots and not others -- which is exactly the set of
    states a power cut can leave, because the guest cannot have ordered two writes that were
    both outstanding (it orders with a flush, and a flush it saw complete is before the
    suspend). The filesystem journal does its normal job on restore.
  * **It is not application-consistent.** Nothing quiesces a database, flushes a guest page
    cache or freezes a filesystem: that needs an agent inside the guest, and there is none.
    A restored guest recovers as from a power cut.
  * The guest clock jumps forward at resume by the pause, and network peers may time out.
    The pause is bounded (`max_pause_seconds`); a set that cannot finish inside it is
    abandoned, the guest is resumed at once, and the set is a failure, never a quietly
    inconsistent success.

The policy chooses how far the barrier reaches (`quiesce`): `none` (no pause; the set is
labelled `none` and carries its measured skew), `vm` (each VM is paused for its own disks;
disks of one VM are consistent with each other, VMs are skewed by seconds) and `domain` (every
VM paused together, so the whole domain shares one cut). A set records which it actually got.

A set is complete or it is not a set. If any member cannot be captured the whole set is
failed and the snapshots it did take are deleted; an operator who wants VMs protected
independently puts them in separate domains.

The same shape as `helios_snapshots`: decisions are pure functions over plain values, `Runner`
is the thin layer that reads and writes the cluster, every effect is injected through `Env`.
"""

import json
import re
import time

import helios_snapshots as snapshots

DOMAIN_TABLE = "hydra.dfs_protection_domains"
MEMBER_TABLE = "hydra.dfs_protection_domain_members"
SET_TABLE = "hydra.dfs_protection_sets"

KIND_VM = "vm"
KIND_VDISK = "vdisk"
KINDS = (KIND_VM, KIND_VDISK)

QUIESCE_NONE = "none"
QUIESCE_VM = "vm"
QUIESCE_DOMAIN = "domain"
QUIESCES = (QUIESCE_NONE, QUIESCE_VM, QUIESCE_DOMAIN)

# What a set actually achieved, which is not always what its policy asked for.
CONSISTENCY_DOMAIN = "crash:domain"   # every member captured inside one barrier
CONSISTENCY_VM = "crash:vm"           # each VM's disks share a barrier; VMs are skewed
CONSISTENCY_NONE = "none"             # no barrier; see the recorded skew

SET_TAKING = "taking"
SET_COMPLETE = "complete"
SET_FAILED = "failed"

SET_ORIGIN_POLICY = "policy"
SET_ORIGIN_MANUAL = "manual"

# The `origin` a member snapshot carries in `dfs_snapshot_index`. Per-vdisk retention in
# helios_snapshots deletes only `policy` rows, so a domain's snapshots are never pruned by a
# vdisk policy: only by the set retention here, which deletes a set whole or not at all.
ORIGIN_DOMAIN = "domain"

DEFAULT_MAX_PAUSE_SECONDS = 30
MAX_PAUSE_CEILING_SECONDS = 300
RESUME_ATTEMPTS = 3
# A set whose last barrier began this long ago belongs to a run that died. Longer than the
# pause ceiling plus the time one Sidon call may take, so a live run is never mistaken for a
# dead one: resuming a guest that a running set is still holding would silently turn that set
# into one taken from a guest that was not held still.
STALE_TAKING_MS = 10 * 60 * 1000

MEMBER_SNAPSHOT_INFIX = "-dom-"


class DomainError(ValueError):
    """A domain, member or policy that cannot be stored, with the reason an operator can act on."""


class DomainRefused(Exception):
    """An operation that must not start, with the reason an operator can act on."""


class RestoreIncomplete(Exception):
    """A restore that began and did not finish. `done` and `remaining` are vdisk ids."""

    def __init__(self, message, done, remaining):
        Exception.__init__(self, message)
        self.done = done
        self.remaining = remaining


# -- names and statements -----------------------------------------------------------------

def validate_domain_name(name):
    if not snapshots.NAME_RE.match(str(name or "")):
        raise DomainError("%r is not a valid domain name (letters, digits, '.', '_' and '-', "
                          "at most 63 characters)" % (name,))
    return name


def validate_policy(interval_seconds, keep_last, quiesce, max_pause_seconds):
    """Raise DomainError unless this policy could be honoured. Returns the normalised tuple."""
    try:
        interval_seconds = int(interval_seconds)
        keep_last = int(keep_last)
        max_pause_seconds = int(max_pause_seconds)
    except (TypeError, ValueError):
        raise DomainError("interval, keep and max pause must be whole numbers")
    if interval_seconds < snapshots.RUN_INTERVAL_SECONDS:
        raise DomainError(
            "the interval cannot be shorter than %d seconds: the job that takes snapshots runs "
            "that often, so a shorter promise could not be kept" % snapshots.RUN_INTERVAL_SECONDS)
    if keep_last < 1 or keep_last > snapshots.MAX_KEEP_LAST:
        raise DomainError("keep must be between 1 and %d" % snapshots.MAX_KEEP_LAST)
    if quiesce not in QUIESCES:
        raise DomainError("quiesce must be one of %s, not %r" % (", ".join(QUIESCES), quiesce))
    # A pause longer than this is an outage the guest's network peers will notice, and a
    # ceiling is what stops a typo from asking for an hour.
    if max_pause_seconds < 1 or max_pause_seconds > MAX_PAUSE_CEILING_SECONDS:
        raise DomainError("max pause must be between 1 and %d seconds"
                          % MAX_PAUSE_CEILING_SECONDS)
    return interval_seconds, keep_last, quiesce, max_pause_seconds


def domain_insert_statement(name, enabled, interval_seconds, keep_last, quiesce,
                            max_pause_seconds, created_at_ms, updated_at_ms):
    validate_domain_name(name)
    interval_seconds, keep_last, quiesce, max_pause_seconds = validate_policy(
        interval_seconds, keep_last, quiesce, max_pause_seconds)
    return (
        "INSERT INTO %s (name, enabled, interval_seconds, keep_last, quiesce, "
        "max_pause_seconds, created_at_ms, updated_at_ms) VALUES (%s, %s, %d, %d, %s, %d, %d, %d);"
        % (DOMAIN_TABLE, snapshots.cql_text(name), "true" if enabled else "false",
           interval_seconds, keep_last, snapshots.cql_text(quiesce), max_pause_seconds,
           int(created_at_ms), int(updated_at_ms)))


def domain_delete_statement(name):
    validate_domain_name(name)
    return "DELETE FROM %s WHERE name = %s;" % (DOMAIN_TABLE, snapshots.cql_text(name))


def parse_member(spec):
    """`vm:<name>` or `vdisk:<id>` as (kind, name). Raises DomainError otherwise."""
    kind, sep, name = str(spec or "").partition(":")
    if not sep or kind not in KINDS or not snapshots.NAME_RE.match(name):
        raise DomainError("%r is not a member: write vm:<name> or vdisk:<id>" % (spec,))
    return kind, name


def member_insert_statement(domain, kind, name, added_at_ms):
    validate_domain_name(domain)
    kind, name = parse_member("%s:%s" % (kind, name))
    return ("INSERT INTO %s (domain, kind, name, added_at_ms) VALUES (%s, %s, %s, %d);"
            % (MEMBER_TABLE, snapshots.cql_text(domain), snapshots.cql_text(kind),
               snapshots.cql_text(name), int(added_at_ms)))


def member_delete_statement(domain, kind, name):
    validate_domain_name(domain)
    kind, name = parse_member("%s:%s" % (kind, name))
    return ("DELETE FROM %s WHERE domain = %s AND kind = %s AND name = %s;"
            % (MEMBER_TABLE, snapshots.cql_text(domain), snapshots.cql_text(kind),
               snapshots.cql_text(name)))


def set_id_for(domain, now_ms):
    """`<domain>-<UTC minute>`. Minute resolution so two runs in one minute name the same set
    and the second is refused rather than producing a near-duplicate, as snapshot names do."""
    return "%s-%s" % (domain, time.strftime("%Y%m%d%H%M", time.gmtime(int(now_ms) / 1000.0)))


def member_snapshot_name(vdisk_id, now_ms):
    """`<vdisk>-dom-<UTC minute>`. Raises DomainError when the result is not a valid vdisk id."""
    stamp = time.strftime("%Y%m%d%H%M", time.gmtime(int(now_ms) / 1000.0))
    name = "%s%s%s" % (vdisk_id, MEMBER_SNAPSHOT_INFIX, stamp)
    if not snapshots.NAME_RE.match(name):
        raise DomainError("%r is too long to name a domain snapshot from (the result must be at "
                          "most 63 characters)" % vdisk_id)
    return name


def set_row_statement(row):
    """The full `dfs_protection_sets` row as an INSERT (an upsert in CQL). One writer, so
    rewriting the whole row at each state change is safe and needs no condition."""
    def num(key):
        value = row.get(key)
        return "null" if value is None else "%d" % int(value)

    def text(key):
        value = row.get(key)
        return "null" if value is None else snapshots.cql_text(value)

    return (
        "INSERT INTO %s (domain, taken_at_ms, set_id, origin, state, consistency, quiesce, "
        "started_at_ms, finished_at_ms, cut_start_ms, cut_end_ms, paused_ms, members, "
        "paused_vms, error) VALUES (%s, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        % (SET_TABLE, snapshots.cql_text(row["domain"]), int(row["taken_at_ms"]),
           snapshots.cql_text(row["set_id"]), text("origin"), text("state"), text("consistency"),
           text("quiesce"), num("started_at_ms"), num("finished_at_ms"), num("cut_start_ms"),
           num("cut_end_ms"), num("paused_ms"),
           snapshots.cql_text(json.dumps(row.get("members") or [], sort_keys=True)),
           snapshots.cql_text(json.dumps(row.get("paused_vms") or [], sort_keys=True)),
           text("error")))


def set_delete_statement(domain, taken_at_ms, set_id):
    return ("DELETE FROM %s WHERE domain = %s AND taken_at_ms = %d AND set_id = %s;"
            % (SET_TABLE, snapshots.cql_text(domain), int(taken_at_ms), snapshots.cql_text(set_id)))


def _json_field(value, default):
    if isinstance(value, (list, dict)):
        return value
    try:
        loaded = json.loads(value)
    except (TypeError, ValueError):
        return default
    return loaded if isinstance(loaded, type(default)) else default


def normalise_set(row):
    """A `SELECT JSON` row of dfs_protection_sets with its two JSON columns decoded."""
    row = dict(row)
    row["members"] = _json_field(row.get("members"), [])
    row["paused_vms"] = _json_field(row.get("paused_vms"), [])
    return row


# -- resolving members into things that can be snapshotted -------------------------------

def vm_vdisk_ids(vm_row):
    """The vdisk ids of a VM, by the `<vm>-disk<n>` convention and `disks_list`'s count.

    The same rule the rest of the tree uses (valcli, lanayru, the console): a missing, empty
    or `NONE` list is one disk, otherwise one per non-empty comma-separated entry.
    """
    name = vm_row.get("name")
    disks = vm_row.get("disks_list") or ""
    if disks and disks not in ("NONE", "None", "null"):
        count = len([d for d in disks.split(",") if d.strip()])
    else:
        count = 1
    return ["%s-disk%d" % (name, index) for index in range(max(1, count))]


class Group(object):
    """Vdisks snapshotted inside one barrier.

    `pause` lists the VMs to suspend for the duration (each as {name, host_ip}); `targets`
    are the vdisks to snapshot, each {vm, vdisk, owner, snapshot}.
    """

    def __init__(self, targets, pause):
        self.targets = targets
        self.pause = pause

    @property
    def atomic(self):
        """Whether every target in this group shares one cut: a single vdisk is atomic by
        itself, several are only if each is held still by a paused VM."""
        if len(self.targets) <= 1:
            return True
        paused = set(p["name"] for p in self.pause)
        return all(t.get("vm") in paused for t in self.targets)


class SetPlan(object):
    def __init__(self):
        self.groups = []
        self.skipped = []    # (label, reason): nothing to capture, not a failure
        self.failures = []   # (label, reason): this set cannot be taken
        self.consistency = CONSISTENCY_NONE

    @property
    def targets(self):
        return [t for g in self.groups for t in g.targets]


def plan_set(members, vms, vdisks, owners, unreachable, quiesce, now_ms):
    """Decide what one set will capture and how it will be held still. Pure.

    `members` are `(kind, name)`; `vms` maps name -> hydra.vms row (name, state, status,
    host_ip, disks_list); `vdisks` maps id -> dfs_vdisks row; `owners` maps vdisk id -> the
    hostname of the node serving it as owner; `unreachable` lists nodes that did not answer.

    A VM that is not running is skipped: nothing is writing, and a detached writable vdisk
    cannot be snapshotted anyway (the owner has to drain its journal). A VM that *is* running
    but whose disk no node reports attached is a failure and not a skip -- capturing the other
    disks of it would produce a set that silently lacks one, which is the inconsistency a
    domain exists to prevent.
    """
    plan = SetPlan()
    per_vm = {}      # vm name -> [targets]
    bare = []        # targets with no VM behind them
    seen = set()

    def target(vm, vdisk_id):
        return {"vm": vm, "vdisk": vdisk_id, "owner": owners.get(vdisk_id),
                "snapshot": member_snapshot_name(vdisk_id, now_ms)}

    for kind, name in sorted(set(members)):
        if kind == KIND_VM:
            row = vms.get(name)
            if row is None:
                plan.skipped.append(("vm:" + name, "no such VM"))
                continue
            state = str(row.get("state") or "").strip().lower()
            if str(row.get("status") or "").strip():
                plan.failures.append(("vm:" + name, "it is mid-operation (%s); try again when it "
                                      "has finished" % row.get("status")))
                continue
            if state != "running":
                plan.skipped.append((
                    "vm:" + name, "it is %s, so nothing is writing and its disks cannot be "
                    "snapshotted detached" % (row.get("state") or "not running")))
                continue
            if not row.get("host_ip"):
                plan.failures.append(("vm:" + name, "it is running but Hydra records no host for "
                                      "it, so it cannot be paused"))
                continue
            ids = vm_vdisk_ids(row)
            problem = None
            for vdisk_id in ids:
                vrow = vdisks.get(vdisk_id)
                if vrow is None:
                    problem = "its disk %s does not exist" % vdisk_id
                elif (vrow.get("class") or "rw") != "rw":
                    problem = "its disk %s has class %s" % (vdisk_id, vrow.get("class"))
                elif owners.get(vdisk_id) is None:
                    problem = ("its disk %s is not attached on any node%s" % (
                        vdisk_id, "" if not unreachable else
                        " (%s did not answer, so it may be there)" % ", ".join(sorted(unreachable))))
                if problem:
                    break
            if problem:
                plan.failures.append(("vm:" + name, "it is running but " + problem))
                continue
            for vdisk_id in ids:
                if vdisk_id not in seen:
                    seen.add(vdisk_id)
                    per_vm.setdefault(name, []).append(target(name, vdisk_id))
        else:
            if name in seen:
                continue
            vrow = vdisks.get(name)
            if vrow is None:
                plan.skipped.append(("vdisk:" + name, "no such vdisk"))
            elif (vrow.get("class") or "rw") != "rw":
                plan.skipped.append(("vdisk:" + name, "its class is %s" % vrow.get("class")))
            elif owners.get(name) is None:
                reason = "not attached on any node, so there is nothing new to capture"
                if unreachable:
                    reason = ("not seen attached; %s did not answer, so it may be on one of "
                              "them" % ", ".join(sorted(unreachable)))
                plan.skipped.append(("vdisk:" + name, reason))
            else:
                seen.add(name)
                bare.append(target("", name))

    def pause_for(vm_name):
        return {"name": vm_name, "host_ip": vms[vm_name].get("host_ip")}

    if quiesce == QUIESCE_NONE:
        every = [t for ts in per_vm.values() for t in ts] + bare
        if every:
            plan.groups.append(Group(sorted(every, key=lambda t: t["vdisk"]), []))
    elif quiesce == QUIESCE_VM:
        for vm_name in sorted(per_vm):
            ts = per_vm[vm_name]
            # One disk needs no barrier: a single snapshot is already one instant.
            plan.groups.append(Group(ts, [pause_for(vm_name)] if len(ts) > 1 else []))
        if bare:
            plan.groups.append(Group(sorted(bare, key=lambda t: t["vdisk"]), []))
    else:
        every = [t for ts in per_vm.values() for t in ts] + bare
        if every:
            pause = [pause_for(v) for v in sorted(per_vm)] if len(every) > 1 else []
            plan.groups.append(Group(sorted(every, key=lambda t: t["vdisk"]), pause))

    if not plan.groups:
        plan.consistency = CONSISTENCY_DOMAIN   # vacuous: nothing to be inconsistent about
    elif len(plan.groups) == 1 and plan.groups[0].atomic:
        plan.consistency = CONSISTENCY_DOMAIN
    elif all(g.atomic for g in plan.groups):
        plan.consistency = CONSISTENCY_VM
    else:
        plan.consistency = CONSISTENCY_NONE
    return plan


def is_due(domain, newest_complete_policy_set_ms, now_ms):
    """Whether a set should be taken now. The same arithmetic as a vdisk policy, measured from
    the newest *complete policy* set: a failed set took nothing, so the next run tries again at
    once instead of waiting out an interval for a set that does not exist."""
    return snapshots.is_due(domain, newest_complete_policy_set_ms, now_ms)


# -- retention of sets --------------------------------------------------------------------

class SetRetention(object):
    def __init__(self, keep, prune, protected):
        self.keep = keep
        self.prune = prune
        self.protected = protected


def plan_set_retention(sets, keep_last, referenced, attached, pinned=None):
    """Which sets a policy may delete. A set is deleted whole or not at all.

    The same rules as `helios_snapshots.plan_retention`, applied to a unit that is several
    snapshots: only complete sets the *policy* took are candidates; the newest `keep_last` are
    kept; an older one is spared when **any** member snapshot has a vdisk derived from it
    (`referenced`), is attached somewhere (`attached`), or when something has `pinned` the set
    (a replication that has not yet delivered it, say: deleting the source while it is being
    shipped would be deleting the backup to save space). Sparing is per set because pruning
    half of one leaves a set that cannot be restored and cannot be told from a complete one.

    A spared set does not count towards `keep_last`, and sparing never causes a younger one to
    be deleted instead.
    """
    pinned = pinned or {}
    ours = sorted(
        (s for s in sets if s.get("origin") == SET_ORIGIN_POLICY and s.get("state") == SET_COMPLETE),
        key=lambda s: (-int(s.get("taken_at_ms") or 0), s.get("set_id", "")))
    window = max(1, int(keep_last))
    keep = [s["set_id"] for s in ours[:window]]
    prune, protected = [], {}
    for entry in ours[window:]:
        sid = entry["set_id"]
        snaps = [m.get("snapshot") for m in entry.get("members") or [] if m.get("snapshot")]
        derived = [s for s in snaps if s in referenced]
        busy = [s for s in snaps if s in attached]
        if sid in pinned:
            protected[sid] = pinned[sid]
        elif derived:
            protected[sid] = "a vdisk was derived from %s" % ", ".join(sorted(derived))
        elif busy:
            protected[sid] = "%s is attached on %s" % (busy[0], attached[busy[0]])
        else:
            prune.append(sid)
    return SetRetention(keep, prune, protected)


FAILED_ROWS_KEPT = 5


def failed_rows_to_drop(sets):
    """Failed set rows older than the newest `FAILED_ROWS_KEPT`, which are kept so that a person
    asking why last night's set is missing can see the reason. Everything older is noise."""
    failed = sorted((s for s in sets if s.get("state") == SET_FAILED),
                    key=lambda s: -int(s.get("taken_at_ms") or 0))
    return failed[FAILED_ROWS_KEPT:]


def stale_taking(sets, now_ms, force=False):
    """Sets still `taking` long after their last barrier began: the run that wrote them died.

    `force` treats every `taking` set as dead. Only a caller that knows no run of its own is in
    flight may say so: a daemon at start-up, or an operator who has looked.
    """
    return [s for s in sets if s.get("state") == SET_TAKING and (force or
            int(now_ms) - int(s.get("cut_start_ms") or s.get("started_at_ms") or 0)
            >= STALE_TAKING_MS)]


# -- the cluster ----------------------------------------------------------------------------

class Env(snapshots.Env):
    """`helios_snapshots.Env` plus the one effect a barrier needs.

    `vm_power(host_ip, vm_name, action) -> (rc, body, err)` is one call to that host's typed
    `/api/v1/vm/<name>/power`. Its body carries the libvirt `state` after the call, which is
    what is trusted: a call that "succeeded" but left the domain running is not a pause.
    """

    def __init__(self, query, dfs, vm_power, **kwargs):
        snapshots.Env.__init__(self, query, dfs, **kwargs)
        self.vm_power = vm_power


class Summary(object):
    def __init__(self):
        self.taken = []      # set ids
        self.pruned = []     # set ids
        self.skipped = []    # (what, reason)
        self.spared = []     # (set id, reason)
        self.failures = []   # (what, reason)
        self.resumed = []    # VMs found paused by a dead run and resumed

    @property
    def ok(self):
        return not self.failures


class _GroupFailed(Exception):
    pass


class Runner(object):
    """One pass of the protection domains over the cluster.

    Compose, not inherit: the snapshot runner already knows how to ask every node what it has
    attached, how to index a snapshot and how to write a task, and what it does not know is
    left to this class.
    """

    def __init__(self, env, schema, dry_run=False):
        self.env = env
        self.schema = schema
        self.dry_run = dry_run
        self.base = snapshots.Runner(env, schema, dry_run=dry_run)
        self.tasks = self.base.tasks

    # reading ---------------------------------------------------------------------------

    def _rows(self, cql):
        return self.base._rows(cql)

    def domains(self):
        return self._rows(
            "SELECT JSON name, enabled, interval_seconds, keep_last, quiesce, max_pause_seconds, "
            "created_at_ms, updated_at_ms FROM %s;" % DOMAIN_TABLE)

    def domain(self, name):
        for row in self.domains():
            if row.get("name") == name:
                return row
        return None

    def members(self, name):
        return [(r.get("kind"), r.get("name")) for r in self._rows(
            "SELECT JSON kind, name FROM %s WHERE domain = %s;"
            % (MEMBER_TABLE, snapshots.cql_text(name)))]

    def sets(self, name):
        return [normalise_set(r) for r in self._rows(
            "SELECT JSON domain, taken_at_ms, set_id, origin, state, consistency, quiesce, "
            "started_at_ms, finished_at_ms, cut_start_ms, cut_end_ms, paused_ms, members, "
            "paused_vms, error FROM %s WHERE domain = %s;" % (SET_TABLE, snapshots.cql_text(name)))]

    def vms(self):
        return dict((r.get("name"), r) for r in self._rows(
            "SELECT JSON name, state, status, host_ip, disks_list FROM hydra.vms;"))

    def vdisks(self):
        return dict((r.get("vdisk_id"), r) for r in self.base.vdisks())

    def owners_and_unreachable(self):
        where, unreachable = self.base.attached()
        owners = {}
        for vdisk_id, places in where.items():
            for place in places:
                if place.endswith("(owner)"):
                    owners[vdisk_id] = place.split(" ")[0]
        return owners, where, unreachable

    # administration ---------------------------------------------------------------------

    def create_domain(self, name, enabled=True, interval_seconds=86400, keep_last=7,
                      quiesce=QUIESCE_VM, max_pause_seconds=DEFAULT_MAX_PAUSE_SECONDS):
        """Create or update a domain's policy. Membership and history are untouched."""
        now = self.env.now_ms()
        existing = self.domain(name)
        created = int(existing.get("created_at_ms") or now) if existing else now
        statement = domain_insert_statement(name, enabled, interval_seconds, keep_last, quiesce,
                                            max_pause_seconds, created, now)
        self._write(statement, "could not write the domain")
        return existing is None

    def add_member(self, domain, kind, name):
        if self.domain(domain) is None:
            raise DomainRefused("no domain named '%s'; create it first" % domain)
        if kind == KIND_VM and name not in self.vms():
            raise DomainRefused("no VM named '%s'" % name)
        if kind == KIND_VDISK:
            row = self.vdisks().get(name)
            if row is None:
                raise DomainRefused("no vdisk named '%s'" % name)
            if (row.get("class") or "rw") != "rw":
                raise DomainRefused("vdisk '%s' is %s, and only writable vdisks are snapshotted"
                                    % (name, row.get("class")))
        self._write(member_insert_statement(domain, kind, name, self.env.now_ms()),
                    "could not add the member")

    def remove_member(self, domain, kind, name):
        self._write(member_delete_statement(domain, kind, name), "could not remove the member")

    def delete_domain(self, name, with_sets=False):
        """Delete a domain. Refuses while it holds sets unless `with_sets`, in which case every
        set is deleted first through the same checks a retention pass makes."""
        if self.domain(name) is None:
            raise DomainRefused("no domain named '%s'" % name)
        held = self.sets(name)
        if held and not with_sets:
            raise DomainRefused(
                "domain '%s' holds %d snapshot set(s); they pin extent groups. Delete the domain "
                "with --with-sets to delete them too." % (name, len(held)))
        for entry in held:
            self.delete_set(name, entry["set_id"])
        for kind, member in self.members(name):
            self.remove_member(name, kind, member)
        self._write(domain_delete_statement(name), "could not delete the domain")

    def _write(self, statement, what):
        rc, _out, err = self.env.query(statement)
        if rc != 0:
            raise RuntimeError("%s: %s" % (what, err or statement))

    # the run ----------------------------------------------------------------------------

    def run(self):
        """One pass: resume anything a dead run left paused, take what is due, prune."""
        summary = Summary()
        self.recover(summary)
        domains = [d for d in self.domains() if d.get("enabled")]
        if not domains:
            self.env.say("No protection domain is enabled, so there is nothing to do. "
                         "See `valcli storage.domain`.")
            return summary
        owners, where, unreachable = self.owners_and_unreachable()
        vdisks = self.vdisks()
        referenced = set(v.get("parent_vdisk") for v in vdisks.values() if v.get("parent_vdisk"))
        for domain in sorted(domains, key=lambda d: d.get("name", "")):
            self._one_domain(summary, domain, referenced, where, unreachable)
        return summary

    def _one_domain(self, summary, domain, referenced, where, unreachable):
        name = domain["name"]
        held = self.sets(name)
        now = self.env.now_ms()
        ours = [s for s in held if s.get("origin") == SET_ORIGIN_POLICY
                and s.get("state") == SET_COMPLETE]
        newest = max(int(s.get("taken_at_ms") or 0) for s in ours) if ours else None
        failed_here = False
        if is_due(domain, newest, now):
            ok = self.take_set(summary, domain, SET_ORIGIN_POLICY)
            failed_here = not ok
        if failed_here or unreachable:
            # Never shed history on the strength of a run that could not take its own set, nor
            # while a node that might be serving a member does not answer.
            return
        self._sweep_failed(summary, name)
        attached = dict((v, ", ".join(p)) for v, p in where.items())
        plan = plan_set_retention(self.sets(name), domain.get("keep_last") or 1, referenced,
                                  attached, pinned=self.pinned_sets(name))
        for sid, reason in sorted(plan.protected.items()):
            summary.spared.append((sid, reason))
            self.env.say("kept %s beyond keep=%s: %s" % (sid, domain.get("keep_last"), reason))
        for sid in plan.prune:
            self._prune_set(summary, name, sid)

    def _sweep_failed(self, summary, name):
        """Finish deleting the snapshots of failed sets, and drop the oldest failed rows.

        A failed set deleted what it took when it failed; one that could not (a node was down
        at that moment) names the leftovers in its row, and this is the retry. Without it a
        failed set would pin extent groups until someone read the log.
        """
        held = self.sets(name)
        for entry in held:
            if entry.get("state") != SET_FAILED:
                continue
            changed = False
            for member in entry["members"]:
                if member.get("status") == "taken":
                    if self._delete_snapshot(member["vdisk"], member["snapshot"]):
                        member["status"] = "deleted"
                        changed = True
                    else:
                        summary.failures.append((member["snapshot"], "a failed set's snapshot "
                                                 "could not be deleted"))
            if changed:
                self._write(set_row_statement(entry), "could not update the failed set")
        for entry in failed_rows_to_drop(held):
            if not any(m.get("status") == "taken" for m in entry["members"]):
                self._write(set_delete_statement(name, entry["taken_at_ms"], entry["set_id"]),
                            "could not drop an old failed set")

    def pinned_sets(self, domain_name):
        """{set_id: reason} the retention must not delete. Empty until replication exists: the
        Rauru daemon overrides this (or passes `pinned` to `plan_set_retention` itself) with the
        sets it has not yet delivered to a remote site."""
        return {}

    # taking a set -------------------------------------------------------------------------

    def snapshot_domain(self, name, origin=SET_ORIGIN_MANUAL):
        """Take a set now. Returns the set row; raises DomainRefused or RuntimeError."""
        domain = self.domain(name)
        if domain is None:
            raise DomainRefused("no domain named '%s'" % name)
        summary = Summary()
        self.recover(summary)
        if not self.take_set(summary, domain, origin):
            reason = summary.failures[0][1] if summary.failures else "the set could not be taken"
            raise RuntimeError(reason)
        if not summary.taken:
            reason = summary.skipped[0][1] if summary.skipped else "nothing was captured"
            raise DomainRefused(reason)
        for entry in self.sets(name):
            if entry["set_id"] == summary.taken[0]:
                return entry
        raise RuntimeError("the set was taken but could not be read back")

    def take_set(self, summary, domain, origin):
        """Take one set. True unless it failed; a set with nothing to capture is not a failure."""
        name = domain["name"]
        now = self.env.now_ms()
        set_id = set_id_for(name, now)
        if any(s["set_id"] == set_id and s.get("state") != SET_FAILED for s in self.sets(name)):
            self.env.say("%s already exists; another run took it" % set_id)
            summary.skipped.append((name, "a set was already taken this minute"))
            return True
        owners, _where, unreachable = self.owners_and_unreachable()
        quiesce = domain.get("quiesce") or QUIESCE_VM
        try:
            plan = plan_set(self.members(name), self.vms(), self.vdisks(), owners, unreachable,
                            quiesce, now)
        except DomainError as exc:
            summary.failures.append((name, str(exc)))
            self.env.say("FAILED set of %s: %s" % (name, exc))
            return False
        for label, reason in plan.skipped:
            summary.skipped.append((label, reason))
            self.env.say("skipped %s: %s" % (label, reason))
        if plan.failures:
            for label, reason in plan.failures:
                summary.failures.append((label, reason))
                self.env.say("FAILED set of %s: %s: %s" % (name, label, reason))
            if not self.dry_run:
                # Nothing was paused or snapshotted, but the attempt is recorded, so that
                # `storage.domain.sets` can say why last night has no set.
                self._write(set_row_statement({
                    "domain": name, "taken_at_ms": now, "set_id": set_id, "origin": origin,
                    "state": SET_FAILED, "consistency": plan.consistency, "quiesce": quiesce,
                    "started_at_ms": now, "finished_at_ms": now, "members": [],
                    "error": "; ".join("%s: %s" % f for f in plan.failures)}),
                    "could not record the failed set")
            return False
        if not plan.groups:
            return True
        if self.dry_run:
            for group in plan.groups:
                self.env.say("would %ssnapshot %s together" % (
                    "pause %s and " % ", ".join(p["name"] for p in group.pause) if group.pause else "",
                    ", ".join(t["vdisk"] for t in group.targets)))
            return True

        budget_ms = int(domain.get("max_pause_seconds") or DEFAULT_MAX_PAUSE_SECONDS) * 1000
        row = {
            "domain": name, "taken_at_ms": now, "set_id": set_id, "origin": origin,
            "state": SET_TAKING, "consistency": plan.consistency, "quiesce": quiesce,
            "started_at_ms": now,
            "members": [{"vm": t["vm"], "vdisk": t["vdisk"], "snapshot": t["snapshot"],
                         "status": "planned"} for t in plan.targets],
            "paused_vms": [],
        }
        task = self.tasks.start("protection_set", "protection_set",
                                {"domain": name, "set_id": set_id})
        taken, error = [], None
        try:
            self._write(set_row_statement(row), "could not record the set")
            cut_start = cut_end = None
            paused_ms = 0
            for group in plan.groups:
                # Written before each barrier, so a run that dies with a guest suspended leaves
                # a row that says which guest and where, and when, and `recover` can resume it.
                # `cut_start_ms` is the heartbeat while the set is `taking`.
                row["paused_vms"] = list(group.pause)
                row["cut_start_ms"] = self.env.now_ms()
                self._write(set_row_statement(row), "could not record the set")
                start, end, paused = self._take_group(group, budget_ms, taken, row)
                cut_start = start if cut_start is None else min(cut_start, start)
                cut_end = end if cut_end is None else max(cut_end, end)
                paused_ms = max(paused_ms, paused)
        except (_GroupFailed, RuntimeError, DomainError) as exc:
            error = str(exc)
        if error:
            self._abandon(row, taken, error)
            self.tasks.finish(task, "domain %s: %s" % (name, error))
            summary.failures.append((name, error))
            self.env.say("FAILED set of %s: %s" % (name, error))
            return False

        row.update({"state": SET_COMPLETE, "finished_at_ms": self.env.now_ms(),
                    "cut_start_ms": cut_start, "cut_end_ms": cut_end, "paused_ms": paused_ms,
                    "paused_vms": []})
        try:
            self._write(set_row_statement(row), "could not record the finished set")
        except RuntimeError as exc:
            self._abandon(row, taken, str(exc))
            self.tasks.finish(task, "domain %s: %s" % (name, exc))
            summary.failures.append((name, str(exc)))
            return False
        self.tasks.finish(task)
        summary.taken.append(set_id)
        self.env.say("took %s (%s, %d snapshot(s), cut spread %d ms, paused %d ms)" % (
            set_id, plan.consistency, len(taken), (cut_end or 0) - (cut_start or 0), paused_ms))
        return True

    def _take_group(self, group, budget_ms, taken, row):
        """Pause, snapshot every target, resume. Returns (cut_start_ms, cut_end_ms, paused_ms).

        The resume is in a `finally`: a guest left suspended is a worse outcome than any
        snapshot is worth, so nothing that goes wrong in between may skip it.
        """
        paused = []
        try:
            for vm in group.pause:
                ok, why = self._power(vm, "suspend", "paused")
                if not ok:
                    raise _GroupFailed("could not pause %s: %s" % (vm["name"], why))
                paused.append(vm)
            began = self.env.now_ms()
            cut_start = None
            for target in group.targets:
                if paused and self.env.now_ms() - began > budget_ms:
                    raise _GroupFailed(
                        "the guests were still paused after %d seconds, the limit for this domain, "
                        "so the set was abandoned rather than hold them longer"
                        % (budget_ms // 1000))
                if cut_start is None:
                    cut_start = self.env.now_ms()
                self._snapshot_target(target, taken, row)
            cut_end = self.env.now_ms()
        finally:
            stranded = []
            for vm in reversed(paused):
                ok, why = self._power(vm, "resume", "running", attempts=RESUME_ATTEMPTS)
                if not ok:
                    stranded.append((vm, why))
            if stranded:
                # Raised from `finally`, deliberately: this outranks whatever was in flight.
                raise _GroupFailed("STRANDED: could not resume %s. They are suspended; run "
                                   "`valcli storage.domain.recover`. %s" % (
                                       ", ".join(v["name"] for v, _ in stranded),
                                       "; ".join(w for _, w in stranded)))
        if cut_start is None:
            cut_start = cut_end
        return cut_start, cut_end, (cut_end - began) if paused else 0

    def _snapshot_target(self, target, taken, row):
        ip = self.base._ip_for_owner(target["owner"])
        if ip is None:
            raise _GroupFailed("no address for node %s in cluster.json" % target["owner"])
        rc, body, err = self.env.dfs(ip, {"op": "snapshot", "vdisk_id": target["vdisk"],
                                          "child_id": target["snapshot"]})
        if rc != 0:
            message = (body.get("error") if isinstance(body, dict) and body.get("error")
                       else err) or "snapshot refused"
            raise _GroupFailed("snapshot of %s failed: %s" % (target["vdisk"], message))
        taken.append(target)
        self.base.record(target["vdisk"], target["snapshot"], ORIGIN_DOMAIN, self.env.now_ms())
        for member in row["members"]:
            if member["snapshot"] == target["snapshot"]:
                member["status"] = "taken"
                member["node"] = target["owner"]

    def _power(self, vm, action, want_state, attempts=1):
        """(ok, reason). A call is a success only if libvirt then reports `want_state`; a refusal
        that nonetheless left the domain there (resuming one that is already running) is too."""
        reason = "no answer"
        for _ in range(max(1, attempts)):
            rc, body, err = self.env.vm_power(vm["host_ip"], vm["name"], action)
            state = str(body.get("state") or "").strip().lower() if isinstance(body, dict) else ""
            if state == want_state:
                return True, ""
            reason = ((body.get("error") if isinstance(body, dict) and body.get("error") else err)
                      or "the domain is %s, not %s" % (state or "in an unknown state", want_state))
        return False, str(reason)

    def _abandon(self, row, taken, error):
        """Delete the snapshots a failed set took, and record it failed. Best effort and loud:
        what cannot be deleted stays named in the row, and `recover` retries it."""
        leftovers = []
        for target in taken:
            if self._delete_snapshot(target["vdisk"], target["snapshot"]):
                for member in row["members"]:
                    if member["snapshot"] == target["snapshot"]:
                        member["status"] = "deleted"
            else:
                leftovers.append(target["snapshot"])
        row.update({"state": SET_FAILED, "finished_at_ms": self.env.now_ms(),
                    "error": error + ("; could not delete %s" % ", ".join(leftovers)
                                      if leftovers else "")})
        try:
            self._write(set_row_statement(row), "could not record the failed set")
        except RuntimeError:
            pass

    # deleting ---------------------------------------------------------------------------

    def _delete_snapshot(self, vdisk_id, snapshot_id):
        """Delete one member snapshot and its index row. An absent one is already deleted."""
        nodes = self.env.nodes()
        ip = nodes[0]["ip"] if nodes else "127.0.0.1"
        rc, body, err = self.env.dfs(ip, {"op": "delete", "vdisk_id": snapshot_id})
        if rc != 0:
            message = (body.get("error") if isinstance(body, dict) and body.get("error") else err) or ""
            if not re.search(r"does not exist|no vdisk|not found", str(message), re.I):
                return False
        for row in self.base.index(vdisk_id):
            if row.get("snapshot_id") == snapshot_id:
                self.env.query("DELETE FROM %s WHERE vdisk_id = %s AND created_at_ms = %d "
                               "AND snapshot_id = %s;" % (
                                   snapshots.INDEX_TABLE, snapshots.cql_text(vdisk_id),
                                   int(row.get("created_at_ms") or 0), snapshots.cql_text(snapshot_id)))
        return True

    def _children_of(self, snapshot_ids):
        wanted = set(snapshot_ids)
        found = {}
        for row in self._rows("SELECT JSON vdisk_id, parent_vdisk FROM hydra.dfs_vdisks;"):
            if row.get("parent_vdisk") in wanted:
                found.setdefault(row["parent_vdisk"], []).append(row.get("vdisk_id"))
        return found

    def _find_set(self, domain, set_id):
        """The set with this id. A failed attempt in the same minute shares the id of the set
        that replaced it, so a non-failed one wins."""
        found = sorted((s for s in self.sets(domain) if s["set_id"] == set_id),
                       key=lambda s: s.get("state") == SET_FAILED)
        if not found:
            raise DomainRefused("domain '%s' has no set '%s'" % (domain, set_id))
        return found[0]

    def delete_set(self, domain, set_id):
        """Delete a set whole. Refuses, deleting nothing, if any member has a vdisk derived from it.

        The check is made for every member before the first delete, because a set with one
        snapshot missing is not restorable and looks complete.
        """
        entry = self._find_set(domain, set_id)
        members = [m for m in entry["members"] if m.get("snapshot")]
        children = self._children_of([m["snapshot"] for m in members])
        if children:
            raise DomainRefused("%s cannot be deleted: %s" % (set_id, "; ".join(
                "a vdisk was derived from %s (%s)" % (s, ", ".join(c))
                for s, c in sorted(children.items()))))
        failed = [m["snapshot"] for m in members
                  if not self._delete_snapshot(m["vdisk"], m["snapshot"])]
        if failed:
            raise RuntimeError("could not delete %s; the set is kept so a retry finds them"
                               % ", ".join(failed))
        self._write(set_delete_statement(domain, entry["taken_at_ms"], set_id),
                    "could not delete the set")

    def _prune_set(self, summary, domain, set_id):
        if self.dry_run:
            self.env.say("would delete %s" % set_id)
            return
        task = self.tasks.start("protection_set_prune", "protection_set_prune",
                                {"domain": domain, "set_id": set_id})
        try:
            self.delete_set(domain, set_id)
        except DomainRefused as exc:
            # A clone appeared between the plan and the delete: the second look doing its job.
            self.tasks.finish(task)
            summary.spared.append((set_id, str(exc)))
            self.env.say("kept %s: %s" % (set_id, exc))
            return
        except RuntimeError as exc:
            self.tasks.finish(task, "pruning %s failed: %s" % (set_id, exc))
            summary.failures.append((set_id, str(exc)))
            self.env.say("FAILED to prune %s: %s" % (set_id, exc))
            return
        self.tasks.finish(task)
        summary.pruned.append(set_id)
        self.env.say("deleted %s" % set_id)

    # recovering -------------------------------------------------------------------------

    def recover(self, summary=None, force=False):
        """Resume any guest a dead run left suspended, and finish what it left half done.

        A `taking` row older than `STALE_TAKING_MS` belongs to a run that no longer exists. Its
        guests are resumed (a domain that is already running is the state being asked for) and
        the snapshots it took are deleted, so a crash costs one set and never a stopped VM.
        `force` skips the age test; see `stale_taking`.
        """
        summary = summary if summary is not None else Summary()
        if self.dry_run:
            return summary
        now = self.env.now_ms()
        for domain in self.domains():
            for entry in stale_taking(self.sets(domain["name"]), now, force):
                stuck = []
                for vm in entry.get("paused_vms") or []:
                    ok, why = self._power(vm, "resume", "running", attempts=RESUME_ATTEMPTS)
                    if ok:
                        summary.resumed.append(vm["name"])
                        self.env.say("resumed %s, left paused by an interrupted run" % vm["name"])
                    else:
                        stuck.append("%s (%s)" % (vm["name"], why))
                if stuck:
                    summary.failures.append((entry["set_id"], "could not resume " + ", ".join(stuck)))
                    continue
                taken = [m for m in entry["members"] if m.get("status") == "taken"]
                entry["paused_vms"] = []
                self._abandon(entry, [{"vdisk": m["vdisk"], "snapshot": m["snapshot"]}
                                      for m in taken], "interrupted: the run that took it died")
        return summary

    # restoring --------------------------------------------------------------------------

    def restore_set(self, domain, set_id, keep=True):
        """Put every vdisk of a complete set back to the set. Returns the vdisk ids restored.

        Every member is checked first and none is touched unless all pass, because restoring
        half of a set produces the very inconsistency the set exists to avoid. Past that point
        the rollbacks are sequential and not atomic: if one fails, `RestoreIncomplete` names
        what was restored and what was not, and running the same restore again completes it
        (a rollback to a snapshot the disk already matches changes nothing).
        """
        entry = self._find_set(domain, set_id)
        if entry.get("state") != SET_COMPLETE:
            raise DomainRefused("%s is %s, not a complete set" % (set_id, entry.get("state")))
        members = [m for m in entry["members"] if m.get("status") == "taken"]
        if not members:
            raise DomainRefused("%s holds no snapshots" % set_id)
        refusals = []
        for member in members:
            try:
                self.base.check_rollback(member["vdisk"], member["snapshot"])
            except snapshots.RollbackRefused as exc:
                refusals.append("%s: %s" % (member["vdisk"], exc))
        if refusals:
            raise DomainRefused("nothing was restored. " + "; ".join(refusals))
        done = []
        for member in members:
            try:
                self.base.rollback(member["vdisk"], member["snapshot"], keep=keep)
            except (snapshots.RollbackRefused, RuntimeError, snapshots.PolicyError) as exc:
                remaining = [m["vdisk"] for m in members if m["vdisk"] not in done]
                raise RestoreIncomplete(
                    "restored %s but not %s: %s. Run the same restore again to finish." % (
                        ", ".join(done) or "nothing", ", ".join(remaining), exc),
                    done, remaining)
            done.append(member["vdisk"])
        return done


def claimed_vdisks(query):
    """The vdisk ids governed by an *enabled* domain, for the per-vdisk policy to leave alone.

    A vdisk in both a domain and a cluster-wide snapshot policy would be snapshotted twice an
    interval, once consistently and once not. `query(cql) -> (rc, stdout, stderr)`. Returns an
    empty set when the tables cannot be read: an older cluster without them has no domains, and
    the policy must keep working there.
    """
    def rows(cql):
        rc, out, _err = query(cql)
        return snapshots.parse_json_rows(out) if rc == 0 else []

    enabled = set(d.get("name") for d in rows(
        "SELECT JSON name, enabled FROM %s;" % DOMAIN_TABLE) if d.get("enabled"))
    if not enabled:
        return set()
    claimed, vm_names = set(), set()
    for member in rows("SELECT JSON domain, kind, name FROM %s;" % MEMBER_TABLE):
        if member.get("domain") not in enabled:
            continue
        if member.get("kind") == KIND_VDISK:
            claimed.add(member.get("name"))
        elif member.get("kind") == KIND_VM:
            vm_names.add(member.get("name"))
    if vm_names:
        for vm in rows("SELECT JSON name, disks_list FROM hydra.vms;"):
            if vm.get("name") in vm_names:
                claimed.update(vm_vdisk_ids(vm))
    return claimed


# -- the command line ---------------------------------------------------------------------

USAGE = """\
Usage:
  valcli storage.domain                                   list domains, members and last sets
  valcli storage.domain.create <name> [--every-hours N] [--keep N] [--quiesce none|vm|domain]
                               [--max-pause S] [--disable]    create or change a policy
  valcli storage.domain.delete <name> [--with-sets]
  valcli storage.domain.add <name> vm:<vm>|vdisk:<id> ...
  valcli storage.domain.remove <name> vm:<vm>|vdisk:<id> ...
  valcli storage.domain.snapshot <name>                   take a set now
  valcli storage.domain.run [--dry-run]                   one scheduled pass (what a timer calls)
  valcli storage.domain.sets <name>                       the sets, their consistency and members
  valcli storage.domain.delete-set <name> <set>
  valcli storage.domain.restore <name> <set> [--no-keep]  put a STOPPED domain back to a set
  valcli storage.domain.recover [--force]                 resume guests an interrupted run paused
"""


def _flag(argv, name, default=None):
    if name in argv:
        index = argv.index(name)
        return argv[index + 1] if index + 1 < len(argv) else default
    return default


def _age(now_ms, then_ms):
    try:
        seconds = max(0, int((now_ms - int(then_ms)) / 1000))
    except (TypeError, ValueError):
        return "?"
    if seconds < 3600:
        return "%dm" % (seconds // 60)
    if seconds < 86400:
        return "%dh" % (seconds // 3600)
    return "%dd" % (seconds // 86400)


def run_command(argv, env, schema, out=print):
    """The `storage.domain*` commands. Returns the process exit status.

    `argv` is valcli's whole argv, so `argv[1]` is the command. Kept here and not in valcli so
    that the Rauru daemon and the CLI run one implementation, and so valcli grows a dispatch
    line instead of two hundred.
    """
    command = argv[1] if len(argv) > 1 else ""
    runner = Runner(env, schema, dry_run="--dry-run" in argv)
    try:
        return _dispatch(command, argv, runner, out)
    except (DomainError, DomainRefused, snapshots.PolicyError) as exc:
        out("Refused: %s" % exc)
        return 1
    except RestoreIncomplete as exc:
        out("Error: %s" % exc)
        return 1
    except RuntimeError as exc:
        out("Error: %s" % exc)
        return 1


def _dispatch(command, argv, runner, out):
    if command == "storage.domain.help":
        out(USAGE)
        return 0
    if command == "storage.domain":
        return _list(runner, out)
    if command == "storage.domain.create":
        if len(argv) < 3:
            out(USAGE)
            return 1
        try:
            interval = int(float(_flag(argv, "--every-hours", 24)) * 3600)
        except ValueError:
            raise DomainError("--every-hours must be a number")
        created = runner.create_domain(
            argv[2], enabled="--disable" not in argv, interval_seconds=interval,
            keep_last=_flag(argv, "--keep", 7), quiesce=_flag(argv, "--quiesce", QUIESCE_VM),
            max_pause_seconds=_flag(argv, "--max-pause", DEFAULT_MAX_PAUSE_SECONDS))
        out("%s domain '%s'." % ("Created" if created else "Updated", argv[2]))
        return 0
    if command == "storage.domain.delete":
        if len(argv) < 3:
            out(USAGE)
            return 1
        runner.delete_domain(argv[2], with_sets="--with-sets" in argv)
        out("Deleted domain '%s'." % argv[2])
        return 0
    if command in ("storage.domain.add", "storage.domain.remove"):
        if len(argv) < 4:
            out(USAGE)
            return 1
        adding = command.endswith(".add")
        for spec in argv[3:]:
            kind, name = parse_member(spec)
            (runner.add_member if adding else runner.remove_member)(argv[2], kind, name)
            out("%s %s:%s %s domain '%s'." % (
                "Added" if adding else "Removed", kind, name, "to" if adding else "from", argv[2]))
        return 0
    if command == "storage.domain.snapshot":
        if len(argv) < 3:
            out(USAGE)
            return 1
        entry = runner.snapshot_domain(argv[2])
        out("Took set '%s': consistency %s, %d snapshot(s)." % (
            entry["set_id"], entry.get("consistency"),
            len([m for m in entry["members"] if m.get("status") == "taken"])))
        out("  cut spread : %s ms between the first and last member snapshot"
            % ((entry.get("cut_end_ms") or 0) - (entry.get("cut_start_ms") or 0)))
        out("  guests held: %s ms" % (entry.get("paused_ms") or 0))
        if entry.get("consistency") == CONSISTENCY_NONE:
            out("  This set is NOT point-in-time across its disks. Use --quiesce vm or domain.")
        out("  Crash-consistent only: nothing quiesced the guest's applications.")
        return 0
    if command == "storage.domain.run":
        summary = runner.run()
        out("sets taken: %d, pruned: %d, skipped: %d, kept past retention: %d, resumed: %d, "
            "failed: %d" % (len(summary.taken), len(summary.pruned), len(summary.skipped),
                            len(summary.spared), len(summary.resumed), len(summary.failures)))
        return 0 if summary.ok else 1
    if command == "storage.domain.sets":
        if len(argv) < 3:
            out(USAGE)
            return 1
        return _sets(runner, argv[2], out)
    if command == "storage.domain.delete-set":
        if len(argv) < 4:
            out(USAGE)
            return 1
        runner.delete_set(argv[2], argv[3])
        out("Deleted set '%s'." % argv[3])
        return 0
    if command == "storage.domain.restore":
        if len(argv) < 4:
            out(USAGE)
            return 1
        done = runner.restore_set(argv[2], argv[3], keep="--no-keep" not in argv)
        out("Restored %s to set '%s'. Start the VM(s) when ready." % (", ".join(done), argv[3]))
        return 0
    if command == "storage.domain.recover":
        summary = runner.recover(force="--force" in argv)
        out("resumed: %s" % (", ".join(summary.resumed) or "nothing was paused"))
        return 0 if summary.ok else 1
    out(USAGE)
    return 1


def _list(runner, out):
    domains = runner.domains()
    if not domains:
        out("No protection domain exists.")
        out("Create one:  valcli storage.domain.create web --every-hours 24 --keep 7")
        return 0
    now = runner.env.now_ms()
    for domain in sorted(domains, key=lambda d: d.get("name", "")):
        name = domain["name"]
        held = runner.sets(name)
        complete = sorted((s for s in held if s.get("state") == SET_COMPLETE),
                          key=lambda s: -int(s.get("taken_at_ms") or 0))
        out("%s  [%s]  every %sh, keep %s, quiesce %s, pause limit %ss" % (
            name, "enabled" if domain.get("enabled") else "disabled",
            int(domain.get("interval_seconds") or 0) // 3600, domain.get("keep_last"),
            domain.get("quiesce"), domain.get("max_pause_seconds")))
        for kind, member in sorted(runner.members(name)):
            out("    %s:%s" % (kind, member))
        if complete:
            newest = complete[0]
            out("    last set: %s, %s ago, %s; %d complete set(s) held"
                % (newest["set_id"], _age(now, newest.get("taken_at_ms")),
                   newest.get("consistency"), len(complete)))
        else:
            out("    no set has been taken")
        stuck = [s for s in held if s.get("state") == SET_TAKING]
        if stuck:
            out("    %d set(s) still taking; if a run died, `valcli storage.domain.recover`"
                % len(stuck))
    return 0


def _sets(runner, domain, out):
    if runner.domain(domain) is None:
        raise DomainRefused("no domain named '%s'" % domain)
    held = sorted(runner.sets(domain), key=lambda s: -int(s.get("taken_at_ms") or 0))
    if not held:
        out("No sets of '%s'." % domain)
        return 0
    now = runner.env.now_ms()
    out("%-34s %-9s %-9s %-13s %-8s %s" % ("SET", "STATE", "ORIGIN", "CONSISTENCY", "AGE", "MEMBERS"))
    for entry in held:
        names = ",".join(m["vdisk"] for m in entry["members"] if m.get("status") == "taken")
        out("%-34s %-9s %-9s %-13s %-8s %s" % (
            entry["set_id"], entry.get("state"), entry.get("origin"), entry.get("consistency"),
            _age(now, entry.get("taken_at_ms")), names or (entry.get("error") or "-")))
    out("Only complete sets the policy took are pruned. Every set is crash-consistent at best.")
    return 0
