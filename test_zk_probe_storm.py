#!/usr/bin/env python3
"""One cached leader probe, and a ceiling on what a runaway one can cost.

ZooKeeper was answering roughly eleven `stat` probes a second, on an idle cluster, for
as long as the cluster had been up. It logs two INFO lines for every one, so the journal
took ~11 lines/second forever and journald burned CPU ingesting them.

The cause was not one bad caller. Nine daemons each carried their own copy of the same
probe loop and each ran it on its own timer -- `vali`'s queue worker asked every two
seconds, and every call into Catalyst asked again. Nothing was wrong with any single
copy. There were just nine of them, none of them cached.

Two things are asserted here, because fixing only one leaves the failure available:

  * **the probe is shared and cached**, so the rate is bounded by a TTL rather than by
    how many callers happen to exist;
  * **the unit has a journal ceiling**, so the next caller that gets it wrong costs a
    rate-limit notice rather than a saturated journal.

Run with:  python -m unittest test_zk_probe_storm
"""

import importlib.util
import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

# Every module that used to carry its own copy of the probe loop.
LEADER_CALLERS = (
    "vali.py", "catalyst.py", "mimir.py", "dagur.py", "valcli.py",
    "mipha.py", "bifrost.py", "hylia.py", "spectrum_server.py",
)

# The ones that still ask which node leads the ensemble, because they still mean it:
# `valcli` and `cluster status` report it, `mipha` decides whether to hand ensemble
# leadership off when it fences itself, and the rest keep it as the fallback for addressing
# Catalyst when no candidacy has published an answer. The others stopped asking entirely --
# see WorkloadPlacementIsNotAnEnsembleQuestion -- and a module that does not ask cannot be
# required to ask it through the shared helper.
ENSEMBLE_CALLERS = (
    "vali.py", "valcli.py", "mipha.py", "hylia.py", "spectrum_server.py",
)

# Every file that writes the ZooKeeper unit.
UNIT_WRITERS = ("cluster_new.py", "provision.py", "spark_daemon_decoded.py")


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def load_helios_zk():
    spec = importlib.util.spec_from_file_location(
        "helios_zk_under_test", os.path.join(HERE, "helios_zk.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class NobodyRollsTheirOwnProbe(unittest.TestCase):
    """The loop existed nine times. Each copy was fine; nine of them were not."""

    # `connect((ip, 2181))` followed by a `stat` write is the probe, however it is spelled.
    PROBE = re.compile(r"connect\(\((?:[^)]*)\s*,\s*(?:2181|ZK_CLIENT_PORT)\)\)")

    def test_nothing_in_the_tree_opens_its_own_probe(self):
        """LEADER_CALLERS is a list, and a list is only as good as its entries.

        A tenth private copy lived in `spark_daemon_decoded.py`, which is not on that list,
        and it was the one that was actually wrong: `recv(1024)` with a 100ms timeout, where
        `Mode:` is the *last* line of `stat`, printed after a line per connected client --
        1647 bytes with `Mode:` at byte 1578 on this cluster. So the leader published
        `zk_leader=False` whenever enough clients were connected to push that line past the
        first kilobyte, and the console's OdinLeader marker appeared and vanished as clients
        came and went, which reads as the ensemble re-electing itself.

        Scanning the tree rather than a list is the only version of this that cannot go
        stale.
        """
        offenders = []
        for name in sorted(os.listdir(HERE)):
            if not name.endswith(".py") or name.startswith("test_"):
                continue
            if name == "helios_zk.py":  # the one module allowed to speak to 2181
                continue
            if self.PROBE.search(read(name)):
                offenders.append(name)
        self.assertEqual(
            offenders, [],
            "these open their own connection to ZooKeeper's client port instead of calling "
            "helios_zk.server_mode or leader_ip: %s" % ", ".join(offenders))

    def test_the_mode_line_is_read_to_completion(self):
        """`Mode:` is the last thing `stat` prints. One recv is one segment at best, and the
        size that is "obviously enough" grows with the number of connected clients -- so the
        shared probe reads until the server closes rather than picking a buffer size."""
        source = read("helios_zk.py")
        block = source[source.index("def server_mode("):]
        block = block[: block.index("\ndef ")]
        self.assertIn("while True:", block,
                      "server_mode takes a single recv, which truncates on a busy cluster")
        self.assertIn("if not chunk:", block, "it never notices the server closing")

    def test_the_leader_probe_lives_in_one_module(self):
        for name in LEADER_CALLERS:
            source = read(name)
            self.assertNotRegex(
                source, self.PROBE,
                "%s opens its own connection to ZooKeeper's client port. Use "
                "helios_zk.leader_ip, which caches -- nine private copies of this is what "
                "produced eleven probes a second." % name)

    def test_every_caller_that_still_asks_uses_the_shared_one(self):
        for name in ENSEMBLE_CALLERS:
            source = read(name)
            self.assertIn(
                "helios_zk.leader_ip(", source,
                "%s no longer calls the shared probe" % name)
            self.assertIn("import helios_zk", source, "%s does not import it" % name)

    def test_the_ones_that_stopped_asking_did_not_keep_a_private_copy(self):
        """The cheapest probe is the one nobody makes.

        Four of the nine callers asked only in order to compare the answer to their own
        address and decide whether to run a leader-only loop. That question moved to a
        per-service candidacy, so they do not probe at all any more -- and what has to be
        asserted about them is not that they use the shared helper, but that removing the
        caller removed the loop rather than leaving a copy behind under another name.
        """
        for name in set(LEADER_CALLERS) - set(ENSEMBLE_CALLERS):
            source = read(name)
            self.assertNotIn(
                "def get_zookeeper_leader_ip", source,
                "%s still defines its own leader lookup; its only caller was the "
                "leadership gate, and the gate has gone" % name)
            self.assertIn("import helios_zk", source,
                          "%s no longer imports helios_zk at all, so it is not standing "
                          "for anything either" % name)

    def test_the_shared_module_ships_everywhere_its_callers_run(self):
        """Two of the callers run inside the Spectrum container, which has its own
        file list. A module they import and the image does not carry is an ImportError
        at start-up, not a missing optimisation."""
        deploy = read("deploy_updates.py")
        self.assertIn('("helios_zk.py", "helios_zk.py")', deploy,
                      "helios_zk is not staged into the Spectrum image")
        self.assertIn("COPY helios_zk.py .", read("Dockerfile"),
                      "the Dockerfile does not copy helios_zk in")


class TheProbeIsCached(unittest.TestCase):
    def setUp(self):
        self.zk = load_helios_zk()
        self.probes = []

        def counting_mode(ip, **_kwargs):
            self.probes.append(ip)
            return "leader" if ip == "10.0.0.2" else "follower"

        self.zk.server_mode = counting_mode
        self.zk.leader_cache_clear()
        self.addCleanup(self.zk.leader_cache_clear)

    def test_it_finds_the_leader(self):
        self.assertEqual(self.zk.leader_ip(["10.0.0.1", "10.0.0.2"]), "10.0.0.2")

    def test_a_second_ask_inside_the_window_does_not_probe_again(self):
        ips = ["10.0.0.1", "10.0.0.2"]
        self.zk.leader_ip(ips)
        before = len(self.probes)

        for _ in range(20):
            self.zk.leader_ip(ips)

        self.assertEqual(len(self.probes), before,
                         "twenty callers produced twenty probes; that is the storm")

    def test_the_cache_expires(self):
        ips = ["10.0.0.1", "10.0.0.2"]
        self.zk.leader_ip(ips, now=1000.0)
        before = len(self.probes)

        self.zk.leader_ip(ips, now=1000.0 + self.zk.LEADER_CACHE_SECONDS + 1)
        self.assertGreater(len(self.probes), before, "the leader is never re-checked")

    def test_a_changed_membership_is_not_served_from_the_old_cache(self):
        self.zk.leader_ip(["10.0.0.1", "10.0.0.2"])
        before = len(self.probes)

        self.zk.leader_ip(["10.0.0.1", "10.0.0.2", "10.0.0.3"])
        self.assertGreater(len(self.probes), before,
                           "a node was added and the answer came from the old cache")

    def test_no_leader_is_cached_too(self):
        """A cluster mid-election is the worst moment to add probe load to."""
        self.zk.server_mode = lambda ip, **_kwargs: self.probes.append(ip) or "follower"
        self.zk.leader_cache_clear()

        self.assertIsNone(self.zk.leader_ip(["10.0.0.1"]))
        before = len(self.probes)
        self.assertIsNone(self.zk.leader_ip(["10.0.0.1"]))
        self.assertEqual(len(self.probes), before)

    def test_standalone_counts_as_the_leader(self):
        """A one-node ensemble has no election to win, and every caller means "the node
        to talk to"."""
        self.zk.server_mode = lambda ip, **_kwargs: "standalone"
        self.zk.leader_cache_clear()

        self.assertEqual(self.zk.leader_ip(["10.0.0.9"]), "10.0.0.9")

    def test_no_addresses_asks_nothing(self):
        self.assertIsNone(self.zk.leader_ip([]))
        self.assertEqual(self.probes, [])


class TheJournalHasACeiling(unittest.TestCase):
    """The probing is fixed at the source. This is what makes the next mistake cheap."""

    def test_every_zookeeper_unit_is_rate_limited(self):
        for name in UNIT_WRITERS:
            source = read(name)
            self.assertIn(
                "LogRateLimitIntervalSec", source,
                "%s writes a ZooKeeper unit with no journal ceiling" % name)
            self.assertIn("LogRateLimitBurst", source, name)

    def test_the_ceiling_leaves_room_for_an_election(self):
        """An election legitimately logs a burst. A limit that clipped it would hide the
        one thing worth reading."""
        burst = re.search(r"LogRateLimitBurst=(\d+)", read("cluster_new.py"))
        interval = re.search(r"LogRateLimitIntervalSec=(\d+)s", read("cluster_new.py"))

        self.assertTrue(burst and interval)
        self.assertGreaterEqual(int(burst.group(1)), 50)
        # And still below the storm it exists to cap.
        self.assertLess(int(burst.group(1)) / int(interval.group(1)), 11.0)


class TheProbesAreNotLoggedAtInfo(unittest.TestCase):
    """The probes are cheap. Logging two INFO lines for each of them was not.

    Caching took the rate down but cannot take it to zero: every daemon process keeps its
    own cache and there are a couple of dozen across a cluster. What removes the cost
    entirely is ZooKeeper not narrating a health check.
    """

    def test_every_xml_the_toolkit_ships_actually_parses(self):
        """The assertions below read the file as text, which is true of a file no parser
        will accept.

        The shipped logback.xml did not parse: two of its comments used the `--` this
        repository writes prose with, and XML forbids it inside a comment. logback reported
        a JoranException and then attached no appender at all, so ZooKeeper logged
        *nothing* -- not the probes this config exists to quieten, and not the elections and
        quorum changes it deliberately left at INFO. The journal went quiet, which is what
        the fix was measured by, so the measurement looked like success.
        """
        import xml.etree.ElementTree as ET

        found = []
        for root, dirs, files in os.walk(HERE):
            dirs[:] = [d for d in dirs
                       if d not in (".git", ".claude", "node_modules", "_build", "deps",
                                    "target")]
            for name in files:
                if name.endswith(".xml"):
                    path = os.path.join(root, name)
                    found.append(path)
                    try:
                        ET.parse(path)
                    except ET.ParseError as problem:
                        self.fail("%s is not well-formed XML: %s. A config the parser "
                                  "rejects is a config that does not apply."
                                  % (os.path.relpath(path, HERE), problem))

        self.assertIn(os.path.join(HERE, "zookeeper_config", "logback.xml"), found,
                      "the logging config is gone; this test is now checking nothing")

    def test_the_quietened_config_is_in_the_toolkit(self):
        config = read(os.path.join("zookeeper_config", "logback.xml"))

        self.assertIn('name="org.apache.zookeeper.server.NIOServerCnxn" level="WARN"', config)
        self.assertIn('name="org.apache.zookeeper.server.command" level="WARN"', config)

    def test_everything_else_still_logs_at_info(self):
        """Elections, quorum changes and session commits are what this log is for. A
        blanket WARN would have taken those too, which is why the two loggers are named
        rather than the root being raised."""
        config = read(os.path.join("zookeeper_config", "logback.xml"))
        self.assertIn('<root level="INFO">', config)

    def test_every_unit_writer_mounts_it_exactly_once(self):
        """Once, not merely at least once.

        The first version of this asserted `in`, which is true of a file that mounts it
        twice -- and `cluster_new.py` did, from the moment the mount was added. Podman
        refuses a container with two mounts on one destination, so every node built by
        `cluster add-node` or `decommission --finalize` got a ZooKeeper that could not
        start, and this test stayed green throughout.
        """
        for name in UNIT_WRITERS:
            self.assertEqual(
                read(name).count("logback.xml:/conf/logback.xml"), 1,
                "%s does not mount the logging config exactly once; podman refuses two "
                "mounts on one destination" % name)

    def test_something_creates_the_file_the_units_mount(self):
        """A bind mount is not a request for a file, it is an assertion that one exists.

        Only the rollout wrote this. A cluster built by `provision.py` mounted a path
        that had never been created -- and podman answers a missing bind source by
        creating a directory there, so ZooKeeper reads an empty config rather than
        failing in a way anyone would notice.
        """
        self.assertIn('"/etc/hci/zookeeper/logback.xml", "w"', read("deploy_updates.py"),
                      "the rollout no longer writes the file")
        provision = read("provision.py")
        self.assertIn('node.write_file("/etc/hci/zookeeper/logback.xml"', provision,
                      "provision.py mounts the logging config but never writes it")
        self.assertIn("ZOOKEEPER_LOGBACK_B64", provision)

    def test_the_embedded_copy_is_the_one_in_the_tree(self):
        """provision.py ships a base64 copy, and an unlisted constant is re-embedded by
        nothing -- so editing the XML would quietly keep shipping the previous copy."""
        import base64
        import re as _re

        self.assertIn('"ZOOKEEPER_LOGBACK_B64"', read("sync_provision.py"),
                      "the constant is not in sync_provision's mapping, so it is never "
                      "re-embedded")

        match = _re.search(r'^ZOOKEEPER_LOGBACK_B64\s*=\s*"([^"]*)"',
                           read("provision.py"), _re.MULTILINE)
        self.assertTrue(match and match.group(1), "the embedded copy is empty")
        embedded = base64.b64decode(match.group(1)).decode("utf-8")
        self.assertEqual(embedded.splitlines(),
                         read(os.path.join("zookeeper_config", "logback.xml")).splitlines(),
                         "provision.py ships a different logback.xml than the tree; run "
                         "sync_provision.py")

    def test_the_rollout_ships_it(self):
        """A config the units mount and the rollout never uploads is a container that
        will not start."""
        deploy = read("deploy_updates.py")

        self.assertIn("/etc/hci/zookeeper/logback.xml", deploy)
        self.assertIn('"zookeeper_config", "logback.xml"', deploy)


class TheRolloutConvergesTheUnit(unittest.TestCase):
    """The unit is written by three paths, none of which a rollout runs.

    Before this, `deploy_updates.py` only stripped `[Install]` from zookeeper.container --
    so a change to what the Quadlet writers produce reached a node through `cluster
    create` or `cluster add-node` and no other way, and an existing cluster kept the old
    unit forever. That is the shape of bug this repository keeps finding: the toolkit is
    correct and the node never hears about it.
    """

    def test_the_rollout_reconciles_the_unit(self):
        deploy = read("deploy_updates.py")

        self.assertIn("RECONCILE_ZOOKEEPER_UNIT", deploy)
        self.assertIn("zookeeper.container", deploy)

    def test_it_is_additive_rather_than_a_rewrite(self):
        """Three other paths write this file. A rollout should add what is missing and
        have no opinion about the rest of it."""
        block = read("deploy_updates.py")
        block = block[block.index("RECONCILE_ZOOKEEPER_UNIT"):]
        block = block[: block.index('"""', block.index('r"""') + 4)]

        self.assertIn("if changed:", block)
        self.assertIn("zookeeper unit: ok", block, "it does not report the no-op case")

    def test_it_does_not_restart_zookeeper(self):
        """The rollout runs against every node in parallel. Restarting the consensus layer
        on all of them at once is how a rollout takes quorum away; the change is staged
        and the operator is told it needs a rolling restart."""
        block = read("deploy_updates.py")
        block = block[block.index("RECONCILE_ZOOKEEPER_UNIT"):]
        block = block[: block.index('"""', block.index('r"""') + 4)]

        self.assertNotIn("systemctl restart zookeeper", block)
        self.assertIn("rolling restart", block)


if __name__ == "__main__":
    unittest.main()
