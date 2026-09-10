#!/usr/bin/env python3
"""Every vdisk was created single-copy, and nothing ever asked otherwise.

`op_create` read the replica count out of the request with `unwrap_or(1)`, and no caller
anywhere -- not vali, not the console, not the CLI, not the Elixir tier -- has ever sent
one. So the default was not a default. It was the policy, applied to every vdisk on every
cluster, and it said one copy regardless of what the operator had configured.

Nothing caught it, and the reason is worth stating because it is the more interesting
half. The cluster's redundancy factor *was* being read -- by the console's capacity
arithmetic, by the keyspace replication reconcile, by the settings page -- everywhere
except the one place that decides how many copies of a guest's disk exist. And every
report of replication health compared a vdisk's replica list against the `rf` on its own
row, which the same defect had written as 1. One replica, one requested: healthy. The
number being satisfied was the number that was wrong.

The unit is the trap underneath it. `cluster.json`'s `redundancy_factor` and a container's
`ftt` both count *failures survived* -- 0 is a single-node cluster and means it -- while
`dfs_vdisks.rf` counts *copies*. They differ by exactly one, and nothing in either name
says so, so reading one straight into the other looks like the correct line of code. It
turns "survive one host loss" into "keep one copy", which is the opposite instruction.

Four properties are asserted, because fixing any three leaves the failure available:

  * **the conversion adds the one back**, and clamps to the nodes that exist rather than
    refusing on a cluster smaller than its own factor;
  * **create and clone both consult it**, since a clone that inherited its parent's rf
    propagated the single-copy default one generation at a time;
  * **an explicit request still wins**, because the replication harness names both an rf
    and the nodes to put it on;
  * **nothing tops up an existing vdisk on a timer**, because until this was fixed that
    described every vdisk on the cluster, and a metadata fix must not become an
    unannounced full-cluster data copy.

Run with:  python -m unittest test_vdisk_replication_factor
"""

import ast
import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
CONTROL_RS = os.path.join(HERE, "sidon", "src", "control.rs")
VDISK_RS = os.path.join(HERE, "sidon", "src", "vdisk.rs")
MAIN_RS = os.path.join(HERE, "sidon", "src", "main.rs")
VALCLI = os.path.join(HERE, "valcli.py")
SPARK = os.path.join(HERE, "spark_daemon_decoded.py")
DARUK = os.path.join(HERE, "daruk.py")


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def load_from(path, names, scope=None):
    """Compile named functions/assignments out of a module that does work at import."""
    tree = ast.parse(read(path), filename=path)
    body = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            body.append(node)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id in names:
                    body.append(node)
    got = set()
    for n in body:
        got.add(n.name if isinstance(n, ast.FunctionDef) else n.targets[0].id)
    missing = names - got
    assert not missing, "%s no longer defines %s" % (os.path.basename(path), missing)
    ns = dict(scope or {})
    exec(compile(ast.Module(body=body, type_ignores=[]), path, "exec"), ns)
    return ns


def rust_fn(source, name):
    """The body of one Rust fn, from its signature to the next one at the same indent."""
    match = re.search(r"^(\s*)(?:pub(?:\(crate\))? )?fn %s\b" % re.escape(name),
                      source, re.M)
    assert match, "no fn named %s" % name
    indent = match.group(1)
    rest = source[match.end():]
    nxt = re.search(r"^%s(?:pub(?:\(crate\))? )?fn " % re.escape(indent), rest, re.M)
    return rest[: nxt.start()] if nxt else rest


class TheConversionAddsTheOneBack(unittest.TestCase):
    """A fault tolerance is not a copy count, and the difference is the whole defect."""

    @classmethod
    def setUpClass(cls):
        cls.valcli = load_from(VALCLI, {"_copies_for_ftt"})

    def test_surviving_one_host_loss_takes_two_copies(self):
        """The arithmetic that was missing.

        `redundancy_factor: 1` is what `cluster create` writes by default and what this
        cluster runs. It asks to survive one host loss, which cannot be done with one
        copy however the column is named.
        """
        self.assertEqual(self.valcli["_copies_for_ftt"](1, 3), 2)
        self.assertEqual(self.valcli["_copies_for_ftt"](2, 3), 3)

    def test_no_fault_tolerance_still_means_one_copy(self):
        """ftt=0 is a decision, and the decision is one copy -- not zero.

        Pinned separately because it is the input where reading the ftt straight into rf
        errs in the other direction. A guard that only tried ftt=1 would not see it, and
        a create that recorded rf=0 would be claiming a disk with no copies at all.
        """
        self.assertEqual(self.valcli["_copies_for_ftt"](0, 1), 1)
        self.assertEqual(self.valcli["_copies_for_ftt"](0, 3), 1)

    def test_a_cluster_is_never_asked_for_more_copies_than_it_has_nodes(self):
        """A single-node cluster carrying a multi-node cluster.json still creates disks.

        Not a corner case: it is what any single-node deployment looks like once its
        configuration has been copied from a larger one. Refusing every create there
        would take a cluster that serves guests today and stop it, so the shortfall is
        clamped into a recorded number instead of an outage.
        """
        self.assertEqual(self.valcli["_copies_for_ftt"](1, 1), 1)
        self.assertEqual(self.valcli["_copies_for_ftt"](5, 2), 2)


class TheClusterFactorIsReadFromWhereItActuallyLives(unittest.TestCase):
    """Not ZooKeeper, and not the keyspace's replication setting."""

    @classmethod
    def setUpClass(cls):
        cls.valcli = load_from(VALCLI, {"_replication_policy"},
                               scope={"json": __import__("json"), "open": open})
        cls.main = read(MAIN_RS)

    def test_the_daemon_reads_the_cluster_document(self):
        """`/etc/hci/cluster.json` holds it, and sidon already reads that file.

        ZooKeeper's `/cluster_state` holds one word -- `started` or `stopped` -- and
        `hydra.cluster_settings.replication_factor` governs how many copies of the
        *metadata* Scylla keeps, which is a different question with a similar name. A
        create that consulted either would still be guessing at the guest's durability.
        """
        body = rust_fn(self.main, "cluster_ftt")
        self.assertIn("redundancy_factor", body)
        self.assertIn("cluster_document()", body)

    def test_an_unreadable_document_is_not_the_same_as_no_replication(self):
        """`None` and `0` must stay distinguishable all the way down.

        A cluster created with `-r 0` asked for one copy and should get it silently. A
        node that cannot read its own configuration has been told nothing, and quietly
        treating that as "the operator wants one copy" is how a misconfiguration becomes
        a durability setting.
        """
        ftt, nodes = self.valcli["_replication_policy"]()
        # There is no /etc/hci on a development machine, which is the unreadable case.
        self.assertIsNone(ftt)
        self.assertGreaterEqual(nodes, 1)

    def test_absent_and_zero_are_different_values_in_the_daemon_too(self):
        body = rust_fn(self.main, "cluster_ftt")
        self.assertTrue(body.lstrip().startswith("() -> Option<u64>"),
                        "cluster_ftt collapsed 'unset' and 'zero' into one number")


class CreateAndCloneBothConsultIt(unittest.TestCase):
    """The two places a vdisk is born. Fixing one leaves the other making single copies."""

    @classmethod
    def setUpClass(cls):
        cls.control = read(CONTROL_RS)

    def test_create_no_longer_defaults_the_replica_count_to_one(self):
        """The literal defect: `unwrap_or(1)` on a value nobody ever sends."""
        body = rust_fn(self.control, "op_create")
        self.assertNotIn('req.get("rf").and_then(Value::as_u64).unwrap_or(1)', body)
        self.assertIn("default_copies(container)", body)

    def test_a_clone_does_not_inherit_its_parents_single_copy(self):
        """A clone is a new vdisk, so it is made under the policy in force now.

        Inheriting the parent's rf is what this used to do, and with every parent on the
        cluster recorded at 1 it meant a clone could never be more durable than the
        defect that produced its parent -- the default propagating itself one generation
        at a time, through the one operation an operator reaches for when they want a
        second copy of something.
        """
        body = rust_fn(self.control, "derive_child")
        self.assertNotIn('parent.get("rf").and_then(Value::as_i64).unwrap_or(1)', body)
        self.assertIn("default_copies(container)", body)

    def test_the_container_is_asked_before_the_cluster(self):
        """Migration 0006 says rf is copied from the container's ftt. It should be.

        The container is the only one of the two that can say "these particular disks are
        scratch", and the schema already describes rf as its copy. The cluster factor is
        the fallback for a container that says nothing -- which today is any create that
        omits the container and lands on Sidon's unmatched "default".
        """
        body = rust_fn(self.control, "default_copies")
        self.assertLess(body.index("container_ftt"), body.index("cluster_ftt"))

    def test_the_endpoint_still_records_what_was_asked_for(self):
        """`dfs_vdisks.rf` is the record, and it is what the new view reads.

        Daruk's own default staying 1 is correct and deliberate rather than a leftover:
        it is the floor for a create that names no rf at all, which after this change
        means a caller that bypassed Sidon entirely. The defect was never this number --
        it was that the only thing setting it never looked anywhere else.
        """
        self.assertIn('"rf": {"type": "int", "default": 1}', read(DARUK))


class AnExplicitRequestStillWins(unittest.TestCase):
    """Policy is a default, not an override. Two callers name their own placement."""

    @classmethod
    def setUpClass(cls):
        cls.control = read(CONTROL_RS)

    def test_a_named_rf_is_not_overruled_by_policy(self):
        body = rust_fn(self.control, "op_create")
        self.assertIn("(Some(rf), _) => rf.max(1) as usize", body)

    def test_a_named_replica_set_carries_its_own_count(self):
        """`tools/tls_replication_check.sh` sends `replicas` and `rf` together.

        A caller that names the nodes has already said how many copies it wants. Deriving
        a larger number from policy and then refusing the request for being short of it
        would break a request that names a perfectly valid set -- and the set it names is
        how the replication path is tested at all.
        """
        body = rust_fn(self.control, "op_create")
        self.assertIn("(None, Some(list)) => list.len().max(1)", body)


class NothingTopsUpAnExistingVdiskOnATimer(unittest.TestCase):
    """Purah re-replicates. Until now it never had cause to, and that must stay opt-in."""

    @classmethod
    def setUpClass(cls):
        cls.control = read(CONTROL_RS)

    def test_the_top_up_is_off_unless_the_request_asks_for_it(self):
        body = rust_fn(self.control, "op_purah_heal")
        self.assertIn('req.get("restore_rf").and_then(Value::as_bool).unwrap_or(false)',
                      body)

    def test_neither_background_caller_asks_for_it(self):
        """The five-second watcher and the sweep timer both heal without it.

        Those exist for a replica that stopped answering, which is an emergency -- the
        journal is write-all, so the guest is taking EIO until the set is restored.
        Having them also restore rf would mean that the first node restart after this
        change began copying every disk on the cluster, with nobody having asked and
        nothing saying it had started.
        """
        # Every call that is not the dispatch arm forwarding an operator's own request.
        background = [c for c in re.findall(r"op_purah_heal\(([^)]*)\)", self.control)
                      if c.strip() not in ("", "req", "&self, req: &Value")]
        self.assertTrue(background, "the background heal callers have moved or gone")
        for call in background:
            self.assertNotIn("restore_rf", call)

    def test_an_emergency_heal_and_a_top_up_are_still_one_code_path(self):
        """Both add a replica and swing the map under the same compare-and-swap.

        Worth pinning: a second implementation of "join the write-all set, then backfill"
        would be a second chance to get the ordering wrong, and the ordering is the whole
        reason a crash midway leaves garbage rather than a hole.
        """
        body = rust_fn(self.control, "op_purah_heal")
        self.assertEqual(body.count("add_replica"), 1)
        self.assertEqual(body.count("/v1/dfs/set-replicas"), 1)


class TheGapIsVisibleWithoutReadingTheDatabase(unittest.TestCase):
    """The missing view, and the reason this went unnoticed for as long as it did."""

    @classmethod
    def setUpClass(cls):
        cls.valcli = read(VALCLI)
        cls.vdisk = read(VDISK_RS)
        cls.control = read(CONTROL_RS)
        cls.spark = read(SPARK)

    def test_the_cli_shows_policy_beside_what_was_asked_for_and_what_exists(self):
        """Three numbers, not two.

        Every existing report compared the replica list to the vdisk's own rf, and the
        defect had written both as 1, so every disk in the fleet read 1/1 and nothing was
        ever short of anything. The third column -- what policy asks for -- is the one
        that would have shown it.
        """
        self.assertIn("def cmd_storage_replication(", self.valcli)
        body = self.valcli[self.valcli.index("def cmd_storage_replication("):]
        body = body[: body.index("\ndef ", 10)]
        for column in ("Policy", "Asked (rf)", "Copies"):
            self.assertIn(column, body)

    def test_a_disk_that_never_asked_reads_differently_from_one_that_lost_a_copy(self):
        """Two states with two different fixes, so they must not share a word.

        A disk short of its own rf has lost copies and Purah restores them by itself. A
        disk whose rf is below policy never requested them, so there is nothing to
        restore and no amount of healing will change it.
        """
        body = self.valcli[self.valcli.index("def cmd_storage_replication("):]
        body = body[: body.index("\ndef ", 10)]
        self.assertIn("under-policy", body)
        self.assertIn("degraded", body)

    def test_the_daemon_reports_the_requested_count_beside_the_set(self):
        """`list` and `status` carry rf, so the console need not query Hydra to show it."""
        self.assertIn('"rf": self.rf', self.vdisk)
        self.assertIn('"rf": v.rf', self.control)

    def test_the_top_up_can_actually_be_reached_from_the_cli(self):
        """`purah-heal` is a node op on spark's allow-list, and takes no vdisk_id.

        The relay forwards every key but `op`, so `restore_rf` reaches the daemon. Pinned
        because the allow-list is split by whether an op needs a vdisk id, and a purah job
        landing on the wrong side of that split is exactly how capacity, peers and all
        three purah ops were once refused with "Invalid vdisk id".
        """
        self.assertIn("purah-heal", self.spark)
        node_ops = self.spark[self.spark.index("DFS_NODE_OPS = ("):]
        self.assertIn("purah-heal", node_ops[: node_ops.index(")")])
        self.assertIn('{"op": "purah-heal", "restore_rf": True}', self.valcli)


if __name__ == "__main__":
    unittest.main()
