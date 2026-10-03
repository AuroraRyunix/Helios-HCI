#!/usr/bin/env python3
"""A name bound to a leadership candidacy must never be bound to anything else.

`mimir.main()` did this:

    schedules = candidacy(helios_zk.SERVICE_MIMIR_SCHEDULES)
    while True:
        if schedules.leading():
            ...
            schedules = []                # the rows of hydra.mimir_schedules
            for line in stdout.splitlines():
                schedules.append(json.loads(line))

The candidacy was created once, before the loop, and the first pass in which this node led
rebound the same name to a list of table rows. From then on `schedules.leading()` raised
AttributeError on every iteration. A broad `except Exception` around the loop body turned that
into one log line a minute, so nothing crashed, nothing alerted, and Mimir's scheduled health
checks simply stopped running -- which looks exactly like a healthy daemon with nothing to do.

It slipped in with the move from comparing addresses to holding a per-service election: the old
code asked a function each time, so the name `schedules` was only ever data. A candidacy is
long-lived state with a method on it, and the same name for "my election" and "the thing the
election protects" is an invitation.

The property asserted is structural, so it also covers the next daemon: within one scope, a name
assigned from a candidacy constructor has exactly that one assignment.

Run with:  python -m unittest test_candidacy_not_rebound
"""

import ast
import io
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

# What constructs a candidacy. Matched on the called name, whatever it is imported as.
CANDIDACY_CONSTRUCTORS = {"candidacy", "cluster_candidacy", "Candidacy"}


def called_name(call):
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def assigned_names(node):
    """Every simple name this statement binds."""
    targets = []
    if isinstance(node, ast.Assign):
        targets = node.targets
    elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
        targets = [node.target]
    elif isinstance(node, (ast.For, ast.AsyncFor)):
        targets = [node.target]
    elif isinstance(node, ast.With):
        targets = [item.optional_vars for item in node.items if item.optional_vars is not None]
    names = []
    for target in targets:
        for sub in ast.walk(target):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Store):
                names.append(sub.id)
    return names


def own_statements(scope):
    """Statements in this scope, not descending into nested function or class bodies."""
    stack = list(ast.iter_child_nodes(scope))
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        stack.extend(ast.iter_child_nodes(node))


def scopes(tree):
    yield tree
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


DATA_LITERALS = (ast.List, ast.Dict, ast.Set, ast.Tuple, ast.ListComp, ast.DictComp,
                 ast.SetComp, ast.GeneratorExp)


def rebinds_to_data(node):
    """True when this statement binds a name to something that is plainly not a candidacy.

    A container literal, a comprehension, a non-None constant, a loop variable, a `with`
    target or an augmented assignment. A *lookup* or a call is deliberately allowed, because
    `existing = cache.get(service)` followed by `existing = make_candidacy(...)` is the
    ordinary get-or-create shape and the name holds a candidacy at both points.
    """
    if isinstance(node, ast.Assign):
        value = node.value
        if isinstance(value, DATA_LITERALS):
            return True
        return isinstance(value, ast.Constant) and value.value is not None
    return isinstance(node, (ast.AugAssign, ast.For, ast.AsyncFor, ast.With))


def offenders(source, filename="<src>"):
    tree = ast.parse(source, filename)
    found = []
    for scope in scopes(tree):
        candidacy_names, rebinds = set(), {}
        for node in own_statements(scope):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)                     and called_name(node.value) in CANDIDACY_CONSTRUCTORS:
                candidacy_names.update(assigned_names(node))
        for node in own_statements(scope):
            if not rebinds_to_data(node):
                continue
            for name in assigned_names(node):
                if name in candidacy_names:
                    rebinds.setdefault(name, []).append(getattr(node, "lineno", 0))
        for name in sorted(rebinds):
            found.append((name, sorted(rebinds[name])))
    return found


class ACandidacyKeepsItsName(unittest.TestCase):
    def test_no_daemon_rebinds_the_name_of_its_candidacy(self):
        problems = []
        for name in sorted(os.listdir(HERE)):
            if not name.endswith(".py") or name.startswith("test_"):
                continue
            with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
                try:
                    found = offenders(handle.read(), name)
                except SyntaxError:
                    continue
            for bound, lines in found:
                problems.append("%s: %r is a candidacy and is also assigned at lines %s"
                                % (name, bound, lines))
        self.assertEqual(problems, [],
                         "a name that holds a leadership candidacy is reused for something "
                         "else, so the second pass raises AttributeError:\n  "
                         + "\n  ".join(problems))

    def test_the_guard_catches_the_mimir_shape(self):
        """Without this the test above could pass by finding nothing."""
        bad = (
            "def main():\n"
            "    schedules = candidacy('x')\n"
            "    while True:\n"
            "        if schedules.leading():\n"
            "            schedules = []\n"
            "            schedules.append(1)\n")
        self.assertEqual([n for n, _ in offenders(bad)], ["schedules"])

    def test_the_guard_ignores_a_name_that_is_only_ever_a_candidacy(self):
        good = (
            "def main():\n"
            "    schedules = candidacy('x')\n"
            "    rows = []\n"
            "    while True:\n"
            "        if schedules.leading():\n"
            "            rows = [1]\n")
        self.assertEqual(offenders(good), [])

    def test_a_nested_function_may_reuse_the_name(self):
        """Scopes are separate: a helper's local `schedules` is not the outer one."""
        nested = (
            "def main():\n"
            "    schedules = candidacy('x')\n"
            "    def helper():\n"
            "        schedules = []\n"
            "        return schedules\n")
        self.assertEqual(offenders(nested), [])

    def test_the_get_or_create_pattern_is_not_an_offence(self):
        """catalyst, dagur, hylia and mimir each cache their candidacies in a helper that does
        `existing = cache.get(x)` and then `existing = make(...)`. The name holds a candidacy
        at both points, which is the opposite of the bug."""
        helper = (
            "def candidacy(service):\n"
            "    existing = _CACHE.get(service)\n"
            "    if existing is None:\n"
            "        existing = helios_zk.cluster_candidacy(service)\n"
            "    return existing\n")
        self.assertEqual(offenders(helper), [])

    def test_mimir_in_particular_is_fixed(self):
        with io.open(os.path.join(HERE, "mimir.py"), encoding="utf-8") as handle:
            self.assertEqual(offenders(handle.read(), "mimir.py"), [])


if __name__ == "__main__":
    unittest.main()
