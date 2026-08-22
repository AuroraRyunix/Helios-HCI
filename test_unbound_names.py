#!/usr/bin/env python3
"""Every name a deployed component reads is bound somewhere it can see.

This exists because of a specific, repeated failure. Removing a feature means cutting
a region out of a file, and a region has two ends -- so a cut sized to the feature
routinely takes a definition that outlived it. That is exactly how
`read_host_capabilities` came to return a `secure_boot` it never assigned, and how the
allowlists behind `/api/v1/storage/device/prepare`, `/api/v1/storage/container/ensure`,
`/api/v1/vm/<name>/power` and `/api/v1/db/ring` were deleted along with the LINSTOR
endpoints that happened to sit beside them.

None of that is visible at import: Python resolves a global when the line runs, so a
file with a missing constant parses, imports, deploys, starts, serves every request
that does not touch it, and raises NameError on the one that does. The suite was green
and four endpoints were dead.

So this reads the files rather than importing them, and answers one question per
function: is every name it *loads* bound by an argument, a local, a comprehension, an
except clause, an enclosing function, the module, or builtins? A name that is not is
a NameError with a delay fuse.

The checker is deliberately over-permissive -- a conditional assignment counts as a
binding, and so does one that only happens in a branch that never runs. It cannot
prove a name is always bound; it proves a name is bound *nowhere*, which is the bug
this class of edit actually produces.
"""

import ast
import builtins
import io
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

# Every file that ends up on a node, plus the libraries they import. A file that is
# not deployed can still be checked, and is: the failure mode is the same.
COMPONENTS = [
    "spark_daemon_decoded.py", "spark.py", "vali.py", "mipha.py", "hylia.py",
    "catalyst.py", "dagur.py", "daruk.py", "bifrost.py", "gatoway.py", "logos.py",
    "impa.py", "saga.py", "lanayru.py", "spectrum_server.py", "cluster_new.py",
    "provision.py", "deploy_updates.py", "check_updates.py", "create_upgrade_zip.py",
    "sync_provision.py", "helios_schema.py", "helios_sidon.py", "helios_sig.py",
    "helios_zk.py", "mimir.py", "mcli", "mcli-runner", "valcli",
]

BUILTIN_NAMES = frozenset(dir(builtins)) | {
    "__name__", "__file__", "__doc__", "__builtins__", "__spec__", "__package__",
    "WindowsError",
}


def _bindings(node):
    """Names this node binds, not descending into nested scopes."""
    bound = set()

    def target(t):
        for n in ast.walk(t):
            if isinstance(n, ast.Name):
                bound.add(n.id)

    def walk(n, top=False):
        if not top and isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(n.name)
            return                      # its body is a scope of its own
        if isinstance(n, ast.Lambda) and not top:
            return
        if isinstance(n, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
            for t in ([n.target] if hasattr(n, "target") else n.targets):
                if t is not None:
                    target(t)
        if isinstance(n, (ast.For, ast.AsyncFor)):
            target(n.target)
        if isinstance(n, (ast.With, ast.AsyncWith)):
            for item in n.items:
                if item.optional_vars is not None:
                    target(item.optional_vars)
        if isinstance(n, ast.ExceptHandler) and n.name:
            bound.add(n.name)
        if isinstance(n, (ast.Import, ast.ImportFrom)):
            for alias in n.names:
                bound.add((alias.asname or alias.name).split(".")[0])
        if isinstance(n, (ast.Global, ast.Nonlocal)):
            bound.update(n.names)
        if isinstance(n, (ast.NamedExpr,)):
            target(n.target)
        if isinstance(n, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
            # A comprehension is its own scope; its targets are not visible outside,
            # but treating them as bound here only makes this checker kinder.
            for gen in n.generators:
                target(gen.target)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and top:
            args = n.args
            for a in args.posonlyargs + args.args + args.kwonlyargs:
                bound.add(a.arg)
            if args.vararg:
                bound.add(args.vararg.arg)
            if args.kwarg:
                bound.add(args.kwarg.arg)
        if isinstance(n, ast.Lambda) and top:
            args = n.args
            for a in args.posonlyargs + args.args + args.kwonlyargs:
                bound.add(a.arg)
            if args.vararg:
                bound.add(args.vararg.arg)
            if args.kwarg:
                bound.add(args.kwarg.arg)
        if isinstance(n, ast.Try):
            pass
        for child in ast.iter_child_nodes(n):
            walk(child)

    walk(node, top=True)
    return bound


def _loads(node):
    """(name, lineno) for every name this node reads, not counting nested scopes."""
    found = []

    def walk(n, top=False):
        if not top and isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            return
        if isinstance(n, ast.Lambda) and not top:
            return
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load):
            found.append((n.id, n.lineno))
        for child in ast.iter_child_nodes(n):
            walk(child)

    walk(node, top=True)
    return found


def unbound_names(path):
    """[(function, name, line)] for names bound in no visible scope."""
    with io.open(path, encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=path)

    module_scope = _bindings(tree) | BUILTIN_NAMES
    findings = []

    def visit(node, enclosing):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                own = _bindings(child)
                visible = enclosing | own
                for name, line in _loads(child):
                    if name not in visible:
                        owner = getattr(child, "name", "<lambda>")
                        findings.append((owner, name, line))
                visit(child, visible)
            elif isinstance(child, ast.ClassDef):
                # A class body's names are not visible to methods defined in it, so
                # methods are checked against the enclosing scope, not the class's.
                visit(child, enclosing)
            else:
                visit(child, enclosing)

    visit(tree, module_scope)
    return findings


class UnboundNameTests(unittest.TestCase):
    def test_no_component_reads_a_name_nothing_binds(self):
        broken = {}
        for component in COMPONENTS:
            path = os.path.join(HERE, component)
            if not os.path.exists(path):
                continue
            findings = unbound_names(path)
            if findings:
                broken[component] = findings

        if broken:
            lines = []
            for component, findings in sorted(broken.items()):
                for func, name, line in findings:
                    lines.append("%s:%d  %s() reads '%s', which nothing binds"
                                 % (component, line, func, name))
            self.fail(
                "These raise NameError the first time the line runs, and nothing "
                "before then says so:\n  " + "\n  ".join(sorted(lines)))

    def test_the_checker_notices_a_name_that_was_cut_away(self):
        # The shape of the real bug: a function reads a constant whose definition a
        # neighbouring removal took with it.
        import tempfile

        source = (
            "import os\n"
            "KEPT = 1\n"
            "def f():\n"
            "    return {'a': KEPT, 'b': REMOVED}\n"
        )
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False,
                                         encoding="utf-8") as handle:
            handle.write(source)
            temp = handle.name
        try:
            self.assertEqual([("f", "REMOVED", 4)], unbound_names(temp))
        finally:
            os.unlink(temp)

    def test_the_checker_does_not_flag_ordinary_bindings(self):
        import tempfile

        source = (
            "import os\n"
            "TOP = 1\n"
            "def outer(arg, *rest, **kw):\n"
            "    local = arg + TOP\n"
            "    def inner():\n"
            "        return local + os.sep.count('/')\n"
            "    for item in rest:\n"
            "        local += item\n"
            "    with open('x') as handle:\n"
            "        handle.read()\n"
            "    try:\n"
            "        pass\n"
            "    except OSError as exc:\n"
            "        return str(exc)\n"
            "    return [n for n in kw if n], inner()\n"
        )
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False,
                                         encoding="utf-8") as handle:
            handle.write(source)
            temp = handle.name
        try:
            self.assertEqual([], unbound_names(temp))
        finally:
            os.unlink(temp)


if __name__ == "__main__":
    unittest.main()
