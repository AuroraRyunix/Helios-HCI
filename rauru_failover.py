#!/usr/bin/env python3
"""Failover and failback of a replicated set, as a state machine with no effects.

**Status: pure logic, exercised only by its own tests.** It performs nothing: no clone, no VM
start, no network call. `step()` takes the current state and one event and returns the next state
and the *actions* a driver would carry out, or refuses with a reason an operator can act on.
There is no second site to run a driver against, so the driver does not exist; what is pinned
here is the part that can be wrong without one: the order of the steps, and the refusals.
The design is `docs/dfs/replication.md` section 8.

Three rules the machine enforces, because software that can reach only one site cannot enforce
them any other way:

* **Nothing is automatic.** Failover and cutover happen only on an operator's event. A lost link
  is not evidence that the other site is down.
* **Both sites running the guest is a state, not an accident to hope away.** If the original site
  reports the guest running while it runs at the target, the set goes to `split_brain` and every
  event but an explicit `resolve` is refused.
* **A failover from a site that might still be running is a decision someone owns.** It needs the
  operator to state, in the event, that they accept the risk; the refusal says what they would be
  accepting.
"""

REPLICATING = "replicating"
RUNNING_AT_TARGET = "running_at_target"
FAILBACK_SYNCING = "failback_syncing"
FAILBACK_READY = "failback_ready"
CUTTING_OVER = "cutting_over"
SPLIT_BRAIN = "split_brain"
STATES = (REPLICATING, RUNNING_AT_TARGET, FAILBACK_SYNCING, FAILBACK_READY, CUTTING_OVER,
          SPLIT_BRAIN)

# Actions, as data. A driver maps each to real work.
CLONE_MEMBERS = "clone_members"            # a map copy of each replica, zero bytes
REBUILD_DEFINITION = "rebuild_definition"  # the VM definition from the set's recorded members
START_GUEST = "start_guest"
STOP_GUEST = "stop_guest"
REVERSE_REPLICATE = "reverse_replicate"    # the same machinery, roles swapped
FINAL_DELTA = "final_delta"
VERIFY_VISIBLE = "verify_visible"
RETIRE_TARGET_COPY = "retire_target_copy"
ALERT = "alert"


class Refused(Exception):
    """The event is not allowed in this state; the message says why and what to do instead."""


class SetState(object):
    def __init__(self, state=REPLICATING, site="source", rpo_seconds=None, note=""):
        self.state = state
        self.site = site              # where the guest is meant to be running
        self.rpo_seconds = rpo_seconds
        self.note = note

    def __eq__(self, other):
        return isinstance(other, SetState) and vars(self) == vars(other)

    def __repr__(self):
        return "SetState(%s, site=%s)" % (self.state, self.site)


def _go(old, state, site=None, **kw):
    new = SetState(state, site or old.site, kw.get("rpo_seconds", old.rpo_seconds), kw.get("note", ""))
    return new


def step(current, event):
    """-> (SetState, [action]). `event` is a dict with a "kind" and its facts."""
    kind = event.get("kind")
    st = current.state

    if st == SPLIT_BRAIN:
        if kind != "resolve":
            raise Refused("both sites ran this guest; only an operator can say which copy wins "
                          "(event 'resolve' with winner 'source' or 'target'). Nothing else is "
                          "accepted until then.")
        winner = event.get("winner")
        if winner not in ("source", "target"):
            raise Refused("resolve needs winner 'source' or 'target'")
        loser = "target" if winner == "source" else "source"
        return (_go(current, REPLICATING if winner == "source" else RUNNING_AT_TARGET, site=winner,
                    note="split brain resolved; the %s copy was discarded by the operator" % loser),
                [(STOP_GUEST, {"site": loser}), (ALERT, {"text": "the %s copy of the guest was stopped; its writes since the split are not kept" % loser})])

    if kind == "source_reports_guest_running" and st in (RUNNING_AT_TARGET, FAILBACK_SYNCING,
                                                           FAILBACK_READY, CUTTING_OVER):
        return (_go(current, SPLIT_BRAIN, note="the original site reports the guest running"),
                [(ALERT, {"text": "the guest is running at both sites; nothing is changed automatically"})])

    if kind == "failover":
        if st != REPLICATING:
            raise Refused("failover is for a set that is replicating; this one is %s" % st)
        if not event.get("operator"):
            raise Refused("failover is never automatic: it needs an operator's event")
        if event.get("snapshot_visible") is not True:
            raise Refused("no complete snapshot of this set is visible at the target; nothing to start from")
        reach = event.get("source_state")
        if reach == "running":
            raise Refused("the original site reports the guest running; stop it there first, or this "
                          "makes two copies")
        if reach != "stopped" and not event.get("accept_risk_source_may_be_running"):
            raise Refused("the original site cannot be reached, which is not the same as it being down. "
                          "Failing over now risks two copies of the guest writing. Repeat with "
                          "accept_risk_source_may_be_running to take that decision")
        return (_go(current, RUNNING_AT_TARGET, site="target", rpo_seconds=event.get("snapshot_age_seconds")),
                [(CLONE_MEMBERS, {}), (REBUILD_DEFINITION, {}), (START_GUEST, {"site": "target"}),
                 (ALERT, {"text": "failed over; data loss is up to the snapshot's age (%s s)" % event.get("snapshot_age_seconds")})])

    if kind == "failback":
        if st != RUNNING_AT_TARGET:
            raise Refused("failback is for a set running at the target; this one is %s" % st)
        if event.get("source_state") != "reachable":
            raise Refused("the original site must be reachable to receive the changes made at the target")
        return (_go(current, FAILBACK_SYNCING), [(REVERSE_REPLICATE, {})])

    if kind == "delta_caught_up":
        if st != FAILBACK_SYNCING:
            raise Refused("a delta can only catch up while failback is syncing; this set is %s" % st)
        return _go(current, FAILBACK_READY), []

    if kind == "cutover":
        if st != FAILBACK_READY:
            raise Refused("cutover needs failback to be ready; this set is %s" % st)
        if not event.get("operator"):
            raise Refused("cutover is never automatic: it stops the guest")
        return (_go(current, CUTTING_OVER),
                [(STOP_GUEST, {"site": "target"}), (FINAL_DELTA, {}), (VERIFY_VISIBLE, {"site": "source"})])

    if kind == "final_delta_verified":
        if st != CUTTING_OVER:
            raise Refused("nothing is being cut over; this set is %s" % st)
        if event.get("visible_at_source") is not True:
            # The guest is stopped and the final changes are not safely at the source: do not
            # start anywhere on a guess.
            return (_go(current, CUTTING_OVER, note="final delta not visible at the original site"),
                    [(ALERT, {"text": "the guest is stopped at the target and the final changes are not confirmed at the original site; retry the delta or restart the guest at the target"})])
        return (_go(current, REPLICATING, site="source"),
                [(START_GUEST, {"site": "source"}), (RETIRE_TARGET_COPY, {})])

    if kind == "abort_failback":
        if st not in (FAILBACK_SYNCING, FAILBACK_READY):
            raise Refused("only a failback that has not stopped the guest can be abandoned; this set is %s" % st)
        return _go(current, RUNNING_AT_TARGET), []

    raise Refused("event %r means nothing in state %s" % (kind, st))
