#!/usr/bin/env python3
"""Tests for changing which nodes vote, and for refusing to when it would cost quorum.

The ensemble's voters were the first three nodes ever provisioned, and there was no way to
change that on purpose: `provision.py` makes everything past node three an observer, and
the only thing that ever moved a vote was `cluster decommission --finalize` rewriting the
units, where a removal happened to slide the next member into the quorum because
voter-or-observer followed position in the list. Lose two of those three and cluster
coordination stops with every other node healthy and idle.

`reconfig` is the deliberate form, and it is dangerous in a specific way: ZooKeeper will
commit any membership for which a quorum of the old and of the new configuration is
available at that instant. That includes handing a vote to a node which is not answering,
and taking three voters down to one. Both leave a cluster the next single failure finishes
off, and both look like success.

So the assertions below are about the refusals rather than about the mechanics. Each one
corresponds to a membership that ZooKeeper would accept and that would leave the ensemble
weaker than it was found:

  * a vote given to a member that is not answering;
  * a result with fewer than three voters, whose quorum is its whole membership;
  * a change made while the ensemble has no single leader, or without a quorum of the
    configuration being left;
  * a member that is not in the ensemble at all.

And two about durability, which is the half that does not announce itself: the ensemble
owns membership only while it is running, and the unit is the only record of it that
survives a container.

Run with:  python -m unittest test_zk_reconfig
"""

import base64
import importlib.util
import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

# Every file that writes the ZooKeeper unit. The same three test_zk_probe_storm.py names:
# a change to one of them alone diverges the others.
UNIT_WRITERS = ("cluster_new.py", "provision.py", "spark_daemon_decoded.py")


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def load_module(alias, filename):
    spec = importlib.util.spec_from_file_location(alias, os.path.join(HERE, filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cluster = load_module("cluster_reconfig_under_test", "cluster_new.py")
zk = load_module("helios_zk_reconfig_under_test", "helios_zk.py")


def config_text(*roles, **kwargs):
    """A /zookeeper/config body for members 1..n with the given roles."""
    version = kwargs.get("version", "100000005")
    lines = ["server.%d=10.0.0.%d:2888:3888:%s;0.0.0.0:2181" % (index, index, role)
             for index, role in enumerate(roles, start=1)]
    return "\n".join(lines + ["version=" + version])


def members(*roles):
    return zk.parse_ensemble_config(config_text(*roles))["members"]


def modes_where(live_ids, leader):
    """`{id: mode}` where `live_ids` answer and `leader` is the one leading."""
    return {member_id: ("leader" if member_id == leader else "follower")
            for member_id in live_ids}


# -- reading what the ensemble says ------------------------------------------------------

class TheEnsembleIsAskedRatherThanTheUnits(unittest.TestCase):
    """A unit says what a node would come back with. /zookeeper/config says who is voting.
    Before reconfiguration those were the same sentence; they are not any more."""

    def test_members_roles_and_version_are_read_back(self):
        parsed = zk.parse_ensemble_config(
            config_text("participant", "participant", "observer", version="10000000a"))
        self.assertEqual([m["id"] for m in parsed["members"]], [1, 2, 3])
        self.assertEqual([m["role"] for m in parsed["members"]],
                         ["participant", "participant", "observer"])
        self.assertEqual([m["host"] for m in parsed["members"]],
                         ["10.0.0.1", "10.0.0.2", "10.0.0.3"])
        # Hex, and kept as the string ZooKeeper wrote: that is what `reconfig -v` wants.
        self.assertEqual(parsed["version"], "10000000a")

    def test_an_entry_with_no_role_is_a_voter(self):
        """An omitted role means participant. Reading it as "no role" would quietly drop a
        voter out of the arithmetic every quorum decision here is built on."""
        parsed = zk.parse_ensemble_config(
            "server.1=10.0.0.1:2888:3888;2181\nversion=1\n")
        self.assertEqual(parsed["members"][0]["role"], "participant")

    def test_a_host_named_like_a_role_is_not_read_as_one(self):
        parsed = zk.parse_ensemble_config(
            "server.7=observer.example.com:2888:3888;2181\nversion=1\n")
        self.assertEqual(parsed["members"][0]["role"], "participant")
        self.assertEqual(parsed["members"][0]["host"], "observer.example.com")

    def test_only_the_role_is_rewritten(self):
        """Addresses and ports are handed back to ZooKeeper exactly as it wrote them.
        Reconstructing them is how a reconfiguration moves a member to a port nothing is
        listening on."""
        spec = "10.0.0.4:2888:3888:observer;0.0.0.0:2181"
        self.assertEqual(zk.member_spec_with_role(spec, "participant"),
                         "10.0.0.4:2888:3888:participant;0.0.0.0:2181")
        self.assertEqual(
            zk.member_spec_with_role("10.0.0.4:2888:3888;2181", "observer"),
            "10.0.0.4:2888:3888:observer;2181")

    def test_the_reconfig_argument_is_what_the_ensemble_last_wrote(self):
        plan = members("participant", "participant", "observer")
        self.assertEqual(
            cluster.format_reconfig_members(plan),
            "server.1=10.0.0.1:2888:3888:participant;0.0.0.0:2181,"
            "server.2=10.0.0.2:2888:3888:participant;0.0.0.0:2181,"
            "server.3=10.0.0.3:2888:3888:observer;0.0.0.0:2181")


# -- the refusals ------------------------------------------------------------------------

class AVoteIsNeverGivenToANodeThatIsNotAnswering(unittest.TestCase):
    """The failure this exists to prevent. A vote held by a node that is down is counted in
    every quorum and cast in none of them, so promoting a dead observer makes the ensemble
    strictly worse while reporting success -- ZooKeeper commits it happily, because a
    quorum of the old and of the new configuration is available at that instant."""

    def test_promoting_a_silent_observer_is_refused(self):
        # Five members, the fifth silent: promoting it would make four voters of which one
        # never votes, so the quorum of three would need all three of the others.
        current = members("participant", "participant", "participant",
                          "observer", "observer")
        modes = modes_where([1, 2, 3, 4], leader=1)
        plan, refusal = cluster.plan_ensemble_roles(current, modes, promote={5})
        self.assertIsNone(plan)
        self.assertIn("server.5", refusal)
        self.assertIn("without answering", refusal)

    def test_a_dead_voter_may_not_be_left_in_the_quorum(self):
        """Promoting an observer while a voter is dead is the tempting half-measure: it
        would work, and it would leave four voters with one of them silent and a quorum of
        three -- no failure tolerated at all. The refusal is what sends the operator to the
        swap instead."""
        current = members("participant", "participant", "participant", "observer")
        modes = modes_where([1, 2, 4], leader=1)   # server.3 is gone
        plan, refusal = cluster.plan_ensemble_roles(current, modes, promote={4})
        self.assertIsNone(plan)
        self.assertIn("server.3", refusal)

    def test_the_swap_is_allowed_because_every_new_voter_answers(self):
        """And this is the operation the refusal above points at: the dead voter's vote
        moves to a live observer in one reconfiguration. Three live voters afterwards, and
        no instant at which the members hold different views of who votes."""
        current = members("participant", "participant", "participant", "observer")
        modes = modes_where([1, 2, 4], leader=1)
        plan, refusal = cluster.plan_ensemble_roles(
            current, modes, promote={4}, demote={3})
        self.assertIsNone(refusal)
        self.assertEqual([(m["id"], m["role"]) for m in plan],
                         [(1, "participant"), (2, "participant"),
                          (3, "observer"), (4, "participant")])

    def test_the_replaced_node_stays_in_the_ensemble(self):
        """Demoted, not removed. Moving the ZooKeeper role off a permanently failed node
        must not take the node out of the cluster -- it still serves reads when it comes
        back, and it is still a host the ring knows about."""
        current = members("participant", "participant", "participant", "observer")
        modes = modes_where([1, 2, 4], leader=1)
        plan, _ = cluster.plan_ensemble_roles(current, modes, promote={4}, demote={3})
        self.assertIn(3, [m["id"] for m in plan])


class TheResultMustStillSurviveAFailure(unittest.TestCase):
    """An ensemble whose quorum is its entire membership tolerates nothing. Two voters need
    both; one voter is not an ensemble. ZooKeeper will configure either."""

    def test_demoting_a_voter_from_three_is_refused(self):
        current = members("participant", "participant", "participant")
        plan, refusal = cluster.plan_ensemble_roles(
            current, modes_where([1, 2, 3], leader=1), demote={3})
        self.assertIsNone(plan)
        self.assertIn("2 voter(s)", refusal)

    def test_removing_a_voter_from_three_is_refused_too(self):
        """The decommission path asks for exactly this when there is no observer to take
        the vote, and it has to hear no rather than be talked into a two-voter ensemble."""
        current = members("participant", "participant", "participant")
        plan, refusal = cluster.plan_ensemble_roles(
            current, modes_where([1, 2, 3], leader=1), drop={3})
        self.assertIsNone(plan)
        self.assertIn("tolerates no failure", refusal)

    def test_five_voters_may_be_taken_to_three(self):
        current = members(*(["participant"] * 5))
        plan, refusal = cluster.plan_ensemble_roles(
            current, modes_where([1, 2, 3, 4, 5], leader=1), demote={4, 5})
        self.assertIsNone(refusal)
        self.assertEqual(len([m for m in plan if m["role"] == "participant"]), 3)

    def test_an_even_result_is_allowed_and_said_out_loud(self):
        """Four voters are not unsafe, they are just paid for and not delivered: the same
        single failure tolerated, and a fourth node in every quorum. That is a warning,
        not a refusal -- refusing it would make a swap impossible to reach."""
        current = members("participant", "participant", "participant", "observer")
        plan, refusal = cluster.plan_ensemble_roles(
            current, modes_where([1, 2, 3, 4], leader=1), promote={4})
        self.assertIsNone(refusal)
        warnings = cluster.ensemble_warnings(plan)
        self.assertTrue(any("even number" in w for w in warnings), warnings)

    def test_an_odd_result_says_nothing(self):
        current = members("participant", "participant", "participant")
        self.assertEqual(cluster.ensemble_warnings(current), [])


class TheEnsembleMustBeHealthyBeforeItIsChanged(unittest.TestCase):
    def test_no_leader_is_refused(self):
        """Mid-election is the worst possible moment: `stat` answers without a mode while a
        server is not serving, so this also covers the case where the voters are up and
        have not agreed on anything yet."""
        current = members("participant", "participant", "participant")
        modes = {1: "follower", 2: "follower", 3: None}
        plan, refusal = cluster.plan_ensemble_roles(current, modes, demote={3})
        self.assertIsNone(plan)
        self.assertIn("0 leaders", refusal)

    def test_two_leaders_are_refused(self):
        current = members("participant", "participant", "participant")
        modes = {1: "leader", 2: "leader", 3: "follower"}
        plan, refusal = cluster.plan_ensemble_roles(current, modes, demote={3})
        self.assertIsNone(plan)
        self.assertIn("2 leaders", refusal)

    def test_a_configuration_without_a_quorum_cannot_be_left(self):
        """ZooKeeper needs a quorum of the old configuration as well as of the new one.
        Checking it here turns a hang into a sentence."""
        current = members(*(["participant"] * 5))
        modes = {1: "leader", 2: "follower"}
        plan, refusal = cluster.plan_ensemble_roles(current, modes, demote={4, 5})
        self.assertIsNone(plan)
        self.assertIn("quorum of the configuration it is leaving", refusal)

    def test_a_stranger_is_refused(self):
        current = members("participant", "participant", "participant")
        plan, refusal = cluster.plan_ensemble_roles(
            current, modes_where([1, 2, 3], leader=1), promote={9})
        self.assertIsNone(plan)
        self.assertIn("not in the ensemble", refusal)
        self.assertIn("add-node", refusal)


# -- durability --------------------------------------------------------------------------

class TheUnitStillOwnsTheMembershipAcrossARestart(unittest.TestCase):
    """The subtle half. ZooKeeper writes its dynamic configuration next to the static
    zoo.cfg, which the image regenerates from ZOO_SERVERS whenever it is absent -- and
    /conf is in the container, not on a volume, so both files go when the container is
    recreated. A reconfiguration that did not also rewrite the units would be undone by the
    next restart, silently and one node at a time."""

    def setUp(self):
        self.written = {}
        original = cluster.run_remote_spark
        original_units = cluster.unit_action

        def fake(ip, command):
            self.written[ip] = command
            return 0, "", ""

        # Writing the unit and reloading systemd are two calls: the file write has no
        # typed endpoint, the reload does. A unit file systemd has not been told to
        # re-read changes nothing until something else happens to reload it.
        def fake_units(ip, action, units=None, **kwargs):
            return True, ""

        cluster.run_remote_spark = fake
        cluster.unit_action = fake_units
        self.addCleanup(setattr, cluster, "run_remote_spark", original)
        self.addCleanup(setattr, cluster, "unit_action", original_units)

    def unit(self, ip):
        blob = re.search(r"echo (\S+) \| base64 -d", self.written[ip]).group(1)
        return base64.b64decode(blob).decode()

    def test_a_promotion_is_written_into_every_unit(self):
        pairs = [(1, "10.0.0.1"), (2, "10.0.0.2"), (3, "10.0.0.3"), (4, "10.0.0.4")]
        roles = {1: "participant", 2: "participant", 3: "observer", 4: "participant"}
        self.assertEqual(cluster.write_zookeeper_ensemble(pairs, roles), [])
        for ip in ("10.0.0.1", "10.0.0.4"):
            self.assertIn("server.3=10.0.0.3:2888:3888:observer;2181", self.unit(ip))
            self.assertIn("server.4=10.0.0.4:2888:3888;2181", self.unit(ip))

    def test_the_promoted_node_is_not_told_it_is_an_observer(self):
        """`ZOO_PEER_TYPE` follows the role rather than the position. The image never reads
        it, which is exactly why it must not be left saying the opposite of the entry that
        does decide."""
        pairs = [(1, "10.0.0.1"), (2, "10.0.0.2"), (3, "10.0.0.3"), (4, "10.0.0.4")]
        roles = {1: "participant", 2: "participant", 3: "observer", 4: "participant"}
        cluster.write_zookeeper_ensemble(pairs, roles)
        self.assertNotIn("ZOO_PEER_TYPE=observer", self.unit("10.0.0.4"))
        self.assertIn("ZOO_PEER_TYPE=observer", self.unit("10.0.0.3"))

    def test_without_roles_position_still_decides(self):
        """`cluster create` and `cluster add-node` have only ever grown an ensemble, so
        their voters are the first three by construction and nothing about that changes."""
        pairs = [(1, "10.0.0.1"), (2, "10.0.0.2"), (3, "10.0.0.3"), (4, "10.0.0.4")]
        unit = cluster.zookeeper_quadlet(4, pairs)
        self.assertIn("server.4=10.0.0.4:2888:3888:observer;2181", unit)
        self.assertIn("ZOO_PEER_TYPE=observer", unit)


class ANodeThatIsDownStillHasAUnitToFix(unittest.TestCase):
    """The operation that matters most -- moving the role off a permanently failed node --
    is precisely the one where a member's unit cannot be written, because the member is not
    there. Reporting that as a failure would say nothing happened, which is the opposite of
    true; saying nothing about it leaves a node that comes back claiming a role it no
    longer has."""

    def setUp(self):
        self.reachable = set()
        self.written = []
        original_remote = cluster.run_remote_spark
        original_read = cluster.read_ensemble_config
        original_units = cluster.unit_action
        self.addCleanup(setattr, cluster, "run_remote_spark", original_remote)
        self.addCleanup(setattr, cluster, "read_ensemble_config", original_read)
        self.addCleanup(setattr, cluster, "unit_action", original_units)

        def fake(ip, command):
            if ip not in self.reachable:
                return 1, "", "unreachable"
            self.written.append(ip)
            return 0, "", ""

        # The daemon-reload that follows the unit write, which is a typed call now and so
        # does not go through run_remote_spark. Unreachable is unreachable either way.
        def fake_units(ip, action, units=None, **kwargs):
            if ip not in self.reachable:
                return False, "unreachable"
            return True, ""

        cluster.run_remote_spark = fake
        cluster.unit_action = fake_units
        self.after = members("participant", "participant", "observer", "participant")
        cluster.read_ensemble_config = lambda ips: {"members": self.after,
                                                    "version": "100000006"}

    def test_a_member_that_is_answering_and_unwritable_is_a_failure(self):
        self.reachable = {"10.0.0.1", "10.0.0.2", "10.0.0.3"}
        ok, message = cluster.apply_ensemble_reconfig(
            self.after, "100000005", "10.0.0.1", modes_where([1, 2, 3, 4], leader=1))
        self.assertFalse(ok)
        self.assertIn("10.0.0.4", message)
        self.assertIn("which is answering", message)

    def test_a_member_that_is_down_is_a_warning_and_the_change_still_stands(self):
        self.reachable = {"10.0.0.1", "10.0.0.2", "10.0.0.4"}
        ok, message = cluster.apply_ensemble_reconfig(
            self.after, "100000005", "10.0.0.1", modes_where([1, 2, 4], leader=1))
        self.assertTrue(ok, message)
        self.assertIn("[WARNING]", message)
        self.assertIn("10.0.0.3", message)

    def test_asking_for_the_role_a_member_already_has_brings_its_unit_into_line(self):
        """Which is how the loose end above is tied off once the node answers: the ensemble
        needs nothing, and the units are made to say what it says."""
        self.reachable = {"10.0.0.%d" % i for i in (1, 2, 3, 4)}
        written, unwritten = cluster.write_units_from(self.after)
        self.assertEqual((written, unwritten), (4, []))
        self.assertEqual(sorted(self.written), sorted(self.reachable))


class ReconfigurationIsEnabledEverywhereOrNowhere(unittest.TestCase):
    """`reconfigEnabled` is read by whichever server becomes leader, so an ensemble that
    has it on some members answers differently depending on an election."""

    def test_every_unit_writer_sets_it(self):
        for name in UNIT_WRITERS:
            self.assertIn(
                "ZOO_CFG_EXTRA=reconfigEnabled=true", read(name),
                "%s writes a ZooKeeper unit that cannot be reconfigured" % name)

    def test_the_rollout_adds_it_to_an_existing_node(self):
        """Otherwise it reaches a node through `cluster create` and no other way, and the
        cluster that most needs it -- the one already running -- never gets it."""
        deploy = read("deploy_updates.py")
        block = deploy[deploy.index("RECONCILE_ZOOKEEPER_UNIT"):]
        block = block[: block.index('"""', block.index('r"""') + 4)]
        self.assertIn("ZOO_CFG_EXTRA=reconfigEnabled=true", block)
        self.assertIn("Environment=ZOO_MY_ID=", block)

    def test_the_rollout_does_not_touch_identity_or_membership(self):
        """It appends. ZOO_MY_ID is the node's identity and ZOO_SERVERS is the ensemble --
        a rollout running on every node at once has no business rewriting either."""
        deploy = read("deploy_updates.py")
        block = deploy[deploy.index("RECONCILE_ZOOKEEPER_UNIT"):]
        block = block[: block.index('"""', block.index('r"""') + 4)]
        self.assertIn('line.rstrip() + " ZOO_CFG_EXTRA=reconfigEnabled=true"', block)
        self.assertNotIn("ZOO_SERVERS=", block)

    def test_it_is_checked_against_the_running_server_not_the_unit(self):
        """After a rollout the unit has the setting and the container does not, because the
        rollout deliberately does not restart ZooKeeper. A check against the unit would say
        yes and the server would then refuse the reconfiguration."""
        source = read("cluster_new.py")
        block = source[source.index("def reconfig_enabled_on("):]
        block = block[: block.index("\ndef ", 1)]
        self.assertIn("/conf/zoo.cfg", block)
        self.assertNotIn("zookeeper.container", block)


class ADecommissionHandsTheVoteOnDeliberately(unittest.TestCase):
    def test_finalize_asks_for_the_hand_off_before_rewriting_units(self):
        source = read("cluster_new.py")
        start = source.index(
            'print("\\n--- Finalizing: removing the node from cluster metadata ---")')
        block = source[start:start + 4000]
        self.assertIn("hand_off_ensemble_vote(target, survivors)", block)
        self.assertLess(block.index("hand_off_ensemble_vote"),
                        block.index("write_zookeeper_ensemble"),
                        "the units are rewritten before the vote is handed on, which "
                        "undoes the reconfiguration by position")

    def test_the_rewrite_and_restart_remain_as_the_fallback(self):
        """On three nodes there is no observer to take the vote, so removing one really
        does leave a two-voter ensemble and there is nothing a reconfiguration can do about
        it. The old path is not a lesser one; it is the only one that fits."""
        source = read("cluster_new.py")
        start = source.index(
            'print("\\n--- Finalizing: removing the node from cluster metadata ---")')
        block = source[start:start + 4000]
        self.assertIn("read_zookeeper_ids(survivors)", block)
        self.assertIn('unit_action(ip, "restart", ["zookeeper"])', block)

    def test_a_departing_observer_needs_no_hand_off(self):
        original = cluster.read_ensemble_config
        cluster.read_ensemble_config = lambda ips: zk.parse_ensemble_config(
            config_text("participant", "participant", "participant", "observer"))
        self.addCleanup(setattr, cluster, "read_ensemble_config", original)
        handled, message = cluster.hand_off_ensemble_vote("10.0.0.4", ["10.0.0.1"])
        self.assertFalse(handled)
        self.assertIn("does not vote", message)


if __name__ == "__main__":
    unittest.main()
