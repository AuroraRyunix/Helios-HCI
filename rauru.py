#!/usr/bin/env python3
"""Rauru: the snapshot and data-protection manager.

Its Nutanix counterpart is Cerebro. Today it owns one job, which used to be a Dagur schedule:
one pass of the snapshot policy (`helios_snapshots`) on its interval -- take what is due, prune
what is not kept. It is meant to grow into replication and disaster recovery; that is designed
separately and is not here.

Three properties are the reason this is a daemon of its own and not one more cron line:

  * **It starts before there is anything to start against.** `systemctl enable` runs it at boot,
    and boot is before any cluster exists, before ZooKeeper, before Hydra, before Daruk. It
    therefore never exits because a dependency is absent. It says what it is waiting for, backs
    off, and tries again; a daemon that crashed instead would be restarted by systemd every few
    seconds forever, which is a crash loop that looks like a service.
  * **The work is held behind a per-service election**, `rauru-snapshots`, not an address
    comparison against the ZooKeeper ensemble leader (docs/service_leadership.md). One node runs
    the policy; a node that cannot establish that it leads does nothing.
  * **It can be asked whether it would start** without a cluster: `rauru --check` imports
    everything, validates its configuration and exits 0 or 1 without touching the network.

The decisions (which policy applies, what is due, what retention may delete) are in
`helios_snapshots` and are tested without a cluster. This file is the loop around them.
`valcli storage.snapshot-run` remains as the manual command and runs the same code.
"""

import json
import os
import random
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# Imported defensively so that `--check` can say which module is missing instead of dying with
# a traceback, which is the one time the answer matters.
IMPORT_ERRORS = {}
try:
    import helios_zk
except Exception as exc:    # pragma: no cover - exercised by --check on a broken install
    helios_zk = None
    IMPORT_ERRORS["helios_zk"] = str(exc)
try:
    from helios_cql import run_cql_query
except Exception as exc:    # pragma: no cover
    run_cql_query = None
    IMPORT_ERRORS["helios_cql"] = str(exc)
try:
    import helios_schema
except Exception as exc:    # pragma: no cover
    helios_schema = None
    IMPORT_ERRORS["helios_schema"] = str(exc)
try:
    import helios_snapshots
except Exception as exc:    # pragma: no cover
    helios_snapshots = None
    IMPORT_ERRORS["helios_snapshots"] = str(exc)

# Protection domains ride beside the snapshot policy, and an install that lacks the module still
# runs the policy: it is reported once and the domain pass is skipped, not a reason to refuse to
# start the job that has always worked.
try:
    import rauru_protection
    PROTECTION_IMPORT_ERROR = None
except Exception as exc:    # pragma: no cover
    rauru_protection = None
    PROTECTION_IMPORT_ERROR = str(exc)

TASK_COMPONENT = "Rauru"

LOCAL_IP = "127.0.0.1"
try:
    with open("/etc/hci/spectrum/spectrum.env", "r") as handle:
        for line in handle:
            if "=" in line:
                key, value = line.strip().split("=", 1)
                if key == "LOCAL_HYPERVISOR_IP":
                    LOCAL_IP = value
except Exception:
    pass

# How long to wait between looks at the election while this node does not lead, and between
# looks at the clock while it does. Not a rate at which anything is done.
POLL_SECONDS = 30
# The backoff while a dependency is missing: short enough that the first policy run follows the
# cluster coming up by seconds, long enough that a node with no cluster costs nothing.
BACKOFF_FIRST_SECONDS = 5
BACKOFF_MAX_SECONDS = 300


def log(line):
    print("[Rauru] %s" % line, flush=True)


def spark_endpoint(ip):
    """(address, verify_identity) for an mTLS call to a spark-daemon; see mimir.spark_endpoint."""
    if ip in ("127.0.0.1", "::1", "localhost"):
        if LOCAL_IP and LOCAL_IP not in ("127.0.0.1", "::1", "localhost"):
            return LOCAL_IP, True
        return ip, False
    return ip, True


def dfs_call(ip, payload):
    """One `/api/v1/dfs/vdisk` call to one node, where a refusal is a failure.

    spark-daemon answers a refused storage operation with HTTP 409 and the reason in the body,
    so a caller testing only the transport reads a refusal as success. No failover: a vdisk has
    exactly one owner, and the same request sent elsewhere is a different and wrong one.
    """
    address, verify_identity = spark_endpoint(ip)
    try:
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
        context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
        context.check_hostname = verify_identity
        request = urllib.request.Request(
            "https://%s:9099/api/v1/dfs/vdisk" % address,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, context=context, timeout=120) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            body = json.loads(exc.read().decode("utf-8"))
        except Exception:
            return -1, {}, str(exc)
    except Exception as exc:
        return -1, {}, str(exc)
    if isinstance(body, dict) and body.get("error"):
        return -1, body, body["error"]
    return 0, body, ""


class Backoff(object):
    """Exponential, capped, jittered: nine daemons all retrying in step is its own outage."""

    def __init__(self, first=BACKOFF_FIRST_SECONDS, cap=BACKOFF_MAX_SECONDS, rand=random.random):
        self.first = first
        self.cap = cap
        self.rand = rand
        self.failures = 0

    def fail(self):
        delay = min(self.cap, self.first * (2 ** self.failures))
        self.failures += 1
        return delay * (0.75 + 0.5 * self.rand())

    def reset(self):
        self.failures = 0


def hydra_ready():
    """(ok, why): can a statement be run against Hydra, through Daruk or its fallback?"""
    try:
        rc, _out, err = run_cql_query("SELECT now() FROM system.local;")
    except Exception as exc:
        return False, str(exc)
    if rc != 0:
        return False, (err or "the query was refused").strip()[:200]
    return True, ""


def vm_power_call(host_ip, vm_name, action):
    """One typed power call to the host a VM runs on: (rc, body, err).

    A refusal is an HTTP 409 whose body carries the domain's state after the attempt, which is
    what the barrier trusts, so the body is returned as it came (rc 0, as `valcli` does) and not
    turned into a failure.
    """
    address, verify_identity = spark_endpoint(host_ip)
    try:
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile="/root/.certs/ca.crt")
        context.load_cert_chain(certfile="/root/.certs/client.crt", keyfile="/root/.certs/client.key")
        context.check_hostname = verify_identity
        request = urllib.request.Request(
            "https://%s:9099/api/v1/vm/%s/power" % (address, urllib.parse.quote(vm_name, safe="")),
            data=json.dumps({"action": action}).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, context=context, timeout=120) as response:
            return 0, json.loads(response.read().decode("utf-8")), ""
    except urllib.error.HTTPError as exc:
        try:
            return 0, json.loads(exc.read().decode("utf-8")), ""
        except Exception:
            return -1, {}, str(exc)
    except Exception as exc:
        return -1, {}, str(exc)


def run_snapshot_policy():
    """One pass of the policy. Returns the Summary; raises RuntimeError if the cluster cannot
    be read, which the loop treats as "not ready yet" and not as a fault."""
    env = helios_snapshots.Env(run_cql_query, dfs_call, say=log)
    return helios_snapshots.Runner(env, helios_schema).run()


def run_domain_pass():
    """One pass of the protection domains: resume anything a dead run left paused, take the sets
    that are due, prune. A cluster with no enabled domain costs one table read and one line."""
    env = rauru_protection.Env(run_cql_query, dfs_call, vm_power_call, say=log)
    return rauru_protection.Runner(env, helios_schema).run()


_PROTECTION_SKIP_SAID = [False]


def run_everything():
    """The per-vdisk policy, then the protection domains.

    The domain pass is separate in failure: a domain that cannot be taken, or a module that is
    missing, is reported and does not stop the policy's result being recorded, and a policy
    that cannot read the cluster is raised as it always was (the loop treats it as "not ready
    yet") before any domain is touched.
    """
    summary = run_snapshot_policy()
    if rauru_protection is None:
        if not _PROTECTION_SKIP_SAID[0]:
            _PROTECTION_SKIP_SAID[0] = True
            log("protection domains are not run: rauru_protection cannot be imported (%s)"
                % PROTECTION_IMPORT_ERROR)
        return summary
    try:
        domains = run_domain_pass()
    except Exception as exc:
        log("the protection-domain pass failed (%s); the snapshot policy result stands" % exc)
        return summary
    if not domains.ok:
        log("the protection-domain pass reported %d failure(s); they are recorded as Rauru tasks"
            % len(domains.failures))
    return summary


class Daemon(object):
    """The loop, with every effect passed in so it can be driven without a cluster."""

    def __init__(self, election, ready, run, interval, clock=time.time, say=log, backoff=None):
        self.election = election
        self.ready = ready
        self.run = run
        self.interval = interval
        self.clock = clock
        self.say = say
        self.backoff = backoff or Backoff()
        self.next_run = 0.0
        self._leading = None
        self._waiting_on = None

    def _announce_waiting(self, why):
        # Said once per distinct reason: a node with no cluster would otherwise write the same
        # line every few seconds for as long as it stays that way.
        if why != self._waiting_on:
            self._waiting_on = why
            self.say("waiting: %s" % why)

    def step(self):
        """Do whatever is due and return how many seconds to sleep. Never raises."""
        try:
            return self._step()
        except Exception as exc:
            delay = self.backoff.fail()
            self.say("error in the loop (%s); retrying in %.0fs" % (exc, delay))
            return delay

    def _step(self):
        leads = bool(self.election.leading())
        if leads != self._leading:
            self._leading = leads
            self.say("this node holds the rauru-snapshots election and will run the snapshot "
                     "policy" if leads else
                     "standing by: another node runs the snapshot policy, or the election "
                     "cannot be reached from here")
        if not leads:
            return POLL_SECONDS

        now = self.clock()
        if now < self.next_run:
            return min(POLL_SECONDS, self.next_run - now)

        ok, why = self.ready()
        if not ok:
            self._announce_waiting("Hydra is not answering (%s)" % why)
            return self.backoff.fail()

        try:
            summary = self.run()
        except RuntimeError as exc:
            # The cluster could not be read: the tables are not there yet, or Daruk went away
            # between the probe and the query. Not a fault in the policy, so not an error.
            self._announce_waiting(str(exc))
            return self.backoff.fail()

        self._waiting_on = None
        self.backoff.reset()
        self.next_run = self.clock() + self.interval
        if summary is not None and not getattr(summary, "ok", True):
            self.say("the policy run reported %d failure(s); they are recorded as Rauru tasks"
                     % len(getattr(summary, "failures", [])))
        else:
            self.say("snapshot policy run complete; next in %ds" % self.interval)
        return min(POLL_SECONDS, self.interval)


def check():
    """Would this start? Imports and configuration only; touches no network. Returns problems."""
    problems = []
    for module, why in sorted(IMPORT_ERRORS.items()):
        problems.append("cannot import %s: %s" % (module, why))
    if problems:
        return problems
    if not getattr(helios_zk, "SERVICE_RAURU_SNAPSHOTS", None):
        problems.append("helios_zk has no SERVICE_RAURU_SNAPSHOTS election name")
    if helios_snapshots.TASK_COMPONENT != TASK_COMPONENT:
        problems.append("helios_snapshots writes tasks as %r, not %r"
                        % (helios_snapshots.TASK_COMPONENT, TASK_COMPONENT))
    if helios_snapshots.RUN_INTERVAL_SECONDS <= 0:
        problems.append("the snapshot run interval is not positive")
    ids = [m["id"] for m in helios_schema.MIGRATIONS]
    for needed in ("0022-snapshot-policies", "0023-snapshot-index"):
        if needed not in ids:
            problems.append("helios_schema has no migration %s, so the policy tables cannot exist"
                            % needed)
    for name in ("Env", "Runner"):
        if not hasattr(helios_snapshots, name):
            problems.append("helios_snapshots has no %s" % name)
    try:
        helios_zk.cluster_candidacy(helios_zk.SERVICE_RAURU_SNAPSHOTS, LOCAL_IP, hosts=["127.0.0.1"])
    except Exception as exc:
        problems.append("cannot build the election candidacy: %s" % exc)
    return problems


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--check" in argv:
        problems = check()
        if problems:
            for problem in problems:
                print("rauru: %s" % problem)
            return 1
        print("rauru: configuration ok (election %s, task component %s, interval %ds)"
              % (helios_zk.SERVICE_RAURU_SNAPSHOTS, TASK_COMPONENT,
                 helios_snapshots.RUN_INTERVAL_SECONDS))
        return 0

    problems = check()
    if problems:
        # An install that cannot import its own modules is not a missing dependency that will
        # appear later; it is broken, and saying so is the correct way to fail.
        for problem in problems:
            log("cannot start: %s" % problem)
        return 1

    log("snapshot and data-protection manager started on %s." % LOCAL_IP)
    snapshots_election = helios_zk.cluster_candidacy(helios_zk.SERVICE_RAURU_SNAPSHOTS, LOCAL_IP)
    daemon = Daemon(snapshots_election, hydra_ready, run_everything,
                    helios_snapshots.RUN_INTERVAL_SECONDS)
    while True:
        time.sleep(daemon.step())


if __name__ == "__main__":
    sys.exit(main())
