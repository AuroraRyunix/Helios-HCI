#!/usr/bin/env python3
"""spark-daemon's /api/v1/dfs/vdisk allow-list, checked against Sidon's own.

The endpoint fronts Sidon's unix control socket with an allow-list of operations rather
than a pass-through, because forwarding whatever arrives would make it exactly as
powerful as the socket it fronts. That allow-list is hand-maintained on one side and
`match op` in Rust on the other, and nothing compared them.

It also decides which operations need a `vdisk_id`, and that is where it broke. The
guard read "everything except list and ping", so `capacity`, `peers` and all three
`purah-*` jobs -- which take no vdisk and never did -- were refused with "Invalid vdisk
id". Everything that asks a node how much room it has goes through this endpoint, so:

  * hylia's storage guard could not read a node's capacity, and refused every node's
    exit from maintenance;
  * vali's migration capacity gate read the target's free space as unknown, and its
    documented behaviour on unknown is to refuse;
  * `valcli storage.list` printed every node's extent store as 0.0 GiB;
  * the console rendered a cluster whose storage did not exist.

None of it raised. The daemon answered 400 with a sentence, and every caller treated
that as "the node did not answer", which is a state they all handle by being cautious.

Read statically: spark_daemon_decoded.py is deployed as a script and starts an HTTP
server at import.
"""

import ast
import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
DAEMON = os.path.join(HERE, "spark_daemon_decoded.py")
CONTROL_RS = os.path.join(HERE, "sidon", "src", "control.rs")
SIDON_CLIENT = os.path.join(HERE, "helios_sidon.py")

# The arms of `fn dispatch`'s `match op`. Scoped to that block rather than matched
# file-wide, because control.rs has other `match` statements over string literals --
# and not required to be `self.op_...`, because `ping` is answered inline.
RUST_ARM_RE = re.compile(r'^\s*"([a-z-]+)"\s*=>', re.MULTILINE)


def rust_dispatch_ops(source):
    start = source.index("fn dispatch(")
    end = source.index("other =>", start)
    return set(RUST_ARM_RE.findall(source[start:end]))


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def class_attribute(path, class_name, attribute):
    """The literal value of `attribute` in `class_name`, or a tuple concatenation."""
    tree = ast.parse(read(path), filename=path)
    for node in ast.walk(tree):
        if not (isinstance(node, ast.ClassDef) and node.name == class_name):
            continue
        literals = {}
        for stmt in node.body:
            if not isinstance(stmt, ast.Assign):
                continue
            for target in stmt.targets:
                if not isinstance(target, ast.Name):
                    continue
                try:
                    literals[target.id] = ast.literal_eval(stmt.value)
                except ValueError:
                    # `A + B` over two names assigned above it.
                    if (isinstance(stmt.value, ast.BinOp)
                            and isinstance(stmt.value.op, ast.Add)
                            and isinstance(stmt.value.left, ast.Name)
                            and isinstance(stmt.value.right, ast.Name)):
                        literals[target.id] = (
                            tuple(literals[stmt.value.left.id])
                            + tuple(literals[stmt.value.right.id]))
        if attribute in literals:
            return literals[attribute]
    raise AssertionError("%s has no %s.%s" % (os.path.basename(path), class_name, attribute))


class DfsAllowListTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # The handler class is found by the attribute rather than by name, so renaming
        # it does not quietly turn this file into a no-op.
        handler = None
        tree = ast.parse(read(DAEMON), filename=DAEMON)
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                names = [t.id for s in node.body if isinstance(s, ast.Assign)
                         for t in s.targets if isinstance(t, ast.Name)]
                if "DFS_OPS" in names:
                    handler = node.name
                    break
        assert handler, "no class in spark_daemon_decoded.py declares DFS_OPS"
        cls.handler = handler
        cls.vdisk_ops = set(class_attribute(DAEMON, handler, "DFS_VDISK_OPS"))
        cls.node_ops = set(class_attribute(DAEMON, handler, "DFS_NODE_OPS"))
        cls.allowed = set(class_attribute(DAEMON, handler, "DFS_OPS"))
        cls.dispatched = rust_dispatch_ops(read(CONTROL_RS))

    def test_the_two_halves_make_up_the_allow_list(self):
        self.assertEqual(self.vdisk_ops | self.node_ops, self.allowed)

    def test_no_operation_is_in_both_halves(self):
        """An op in both is one whose vdisk requirement nobody decided. It would be
        enforced or not depending on which tuple happened to be tested first."""
        self.assertEqual(self.vdisk_ops & self.node_ops, set())

    def test_the_node_scoped_operations_are_the_ones_that_take_no_vdisk(self):
        """Pinned by name rather than derived, because this is the assertion. Adding an
        op to DFS_NODE_OPS means asserting it works with no vdisk_id in the payload; a
        change here should be a deliberate edit to this list, not a silent consequence
        of editing the daemon."""
        self.assertEqual(
            self.node_ops,
            {"list", "ping", "capacity", "peers",
             "purah-sweep", "purah-scrub", "purah-heal"})

    def test_capacity_is_node_scoped(self):
        """Called out on its own: every caller that asks how much room a node has goes
        through it, and each one's behaviour on a refusal is to be cautious -- refuse a
        migration, refuse a maintenance exit, render nothing. A regression here is
        silent everywhere."""
        self.assertIn("capacity", self.node_ops)
        self.assertNotIn("capacity", self.vdisk_ops)

    def test_every_allowed_operation_exists_in_sidon(self):
        """An op the daemon forwards that Sidon does not implement is a 503 the caller
        cannot distinguish from an outage."""
        missing = sorted(self.allowed - self.dispatched)
        self.assertEqual(
            missing, [],
            "spark-daemon forwards these but sidon/src/control.rs dispatches no "
            "handler for them: %s" % missing)

    def test_the_client_helper_only_offers_allowed_operations(self):
        """helios_sidon.py's named helpers go through this endpoint from the console
        tier, so one naming an op the daemon refuses is a dead command."""
        source = read(SIDON_CLIENT)
        called = set(re.findall(r'\bcall\(\s*"([a-z-]+)"', source))
        # `call(op, ...)` with a variable is the generic passthrough, not a named op.
        refused = sorted(called - self.allowed)
        self.assertEqual(
            refused, [],
            "helios_sidon.py calls these operations but spark-daemon's allow-list "
            "does not permit them: %s" % refused)


if __name__ == "__main__":
    unittest.main()
