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

# Every module that asks which node leads the ensemble.
LEADER_CALLERS = (
    "vali.py", "catalyst.py", "mimir.py", "dagur.py", "valcli.py",
    "mipha.py", "bifrost.py", "hylia.py", "spectrum_server.py",
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

    def test_the_leader_probe_lives_in_one_module(self):
        for name in LEADER_CALLERS:
            source = read(name)
            self.assertNotRegex(
                source, self.PROBE,
                "%s opens its own connection to ZooKeeper's client port. Use "
                "helios_zk.leader_ip, which caches -- nine private copies of this is what "
                "produced eleven probes a second." % name)

    def test_every_caller_uses_the_shared_one(self):
        for name in LEADER_CALLERS:
            source = read(name)
            self.assertIn(
                "helios_zk.leader_ip(", source,
                "%s no longer calls the shared probe" % name)
            self.assertIn("import helios_zk", source, "%s does not import it" % name)

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


if __name__ == "__main__":
    unittest.main()
