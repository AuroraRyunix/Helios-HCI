#!/usr/bin/env python3
"""No code path creates a vdisk without naming a container that exists.

Sidon decides a new vdisk's copy count from its container's `ftt`. A create that names no
container reaches the daemon as a container that matches no row in
`hydra.storage_containers`, the lookup returns "not configured", and the policy falls
through to the cluster factor -- which on a cluster created on one node is 0. Nothing fails.
Both vdisks on the test cluster were created that way and held one copy on a cluster whose
`default-pool` container asks for two.

The guard that existed for this sliced `spectrum_server.py` up to the first `sidon_call(`
and listed two callers by hand, so it passed while the whole Phoenix tier and the add-disk
path of `/api/vms/update` omitted the container. A list of known callers cannot catch the
next one. This test finds the creates itself: it parses every Python module and every Elixir
source file, takes each place that issues a vdisk create, and requires the container to be
named there and, unless it is the cluster default, checked against the catalogue on that
same route.

Run with:  python -m unittest test_vdisk_creates_name_a_container
"""

import ast
import glob
import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
PHX_LIB = os.path.join(HERE, "spectrum_phx", "lib")

# Not part of the product: the tests themselves, and copies of daemons kept for reference or
# embedded as base64 by provision.py. A create in any of these is not a code path.
SKIPPED = {"provision.py", "spark_daemon_decoded.py", "sync_provision.py"}


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def python_sources():
    for path in sorted(glob.glob(os.path.join(HERE, "*.py"))):
        name = os.path.basename(path)
        if name.startswith("test_") or name in SKIPPED:
            continue
        yield path


def elixir_sources():
    for root, _dirs, files in os.walk(PHX_LIB):
        for name in sorted(files):
            if name.endswith(".ex"):
                yield os.path.join(root, name)


def _callee(node):
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _is_create(node):
    """The create operations in Python: a sidon_call, a helios_sidon helper, or a raw op."""
    if isinstance(node, ast.Call):
        callee = _callee(node)
        if callee == "create_vdisk":
            return True
        if (callee == "sidon_call" and node.args
                and isinstance(node.args[0], ast.Constant) and node.args[0].value == "create"):
            return True
    if isinstance(node, ast.Dict):
        for key, value in zip(node.keys, node.values):
            if (isinstance(key, ast.Constant) and key.value == "op"
                    and isinstance(value, ast.Constant) and value.value == "create"):
                return True
    return False


def _names_container(node):
    """The expression the create gives as its container, or None when it gives none."""
    if isinstance(node, ast.Call):
        for kw in node.keywords:
            if kw.arg == "container":
                return kw.value
            if kw.arg is None:  # **kwargs: cannot be shown to name one
                return None
        return None
    for key, value in zip(node.keys, node.values):
        if isinstance(key, ast.Constant) and key.value == "container":
            return value
    return None


def python_creates():
    """Every vdisk create in the Python tier: (file, line, node, container_expr, source)."""
    found = []
    for path in python_sources():
        source = read(path)
        for node in ast.walk(ast.parse(source, filename=path)):
            if _is_create(node):
                found.append((os.path.basename(path), node.lineno, node,
                              _names_container(node), source))
    return found


def _is_cluster_default(expr):
    """`DEFAULT_CONTAINER`, `default_container()` and friends: the installer's own row."""
    text = ast.unparse(expr) if hasattr(ast, "unparse") else ""
    return bool(re.search(r"DEFAULT_CONTAINER|default_container\(\)", text))


def _route_text(source, line):
    """The source of the route a line belongs to, from its dispatch test down to the line."""
    lines = source.splitlines()
    head = "\n".join(lines[:line])
    markers = list(re.finditer(r"(self\.)?path (==|\.startswith\()", head))
    start = markers[-1].start() if markers else 0
    return head[start:]


class EveryPythonCreateNamesAContainer(unittest.TestCase):

    def test_the_scan_finds_the_creates_it_is_meant_to_police(self):
        """A scan that matches nothing would pass vacuously, which is how the last guard
        stayed green. These are the sites known to exist; the property below covers the
        ones that do not yet."""
        sites = {(name, ) for name, *_ in python_creates()}
        for expected in ("spectrum_server.py", "lanayru.py", "valcli.py"):
            self.assertIn((expected,), sites, "no vdisk create found in %s" % expected)
        in_spectrum = [c for c in python_creates() if c[0] == "spectrum_server.py"]
        self.assertGreaterEqual(
            len(in_spectrum), 3,
            "image upload, VM create and VM update (add disk) each create a vdisk")

    def test_none_omits_it(self):
        for name, line, _node, container, _source in python_creates():
            self.assertIsNotNone(
                container,
                "%s:%d creates a vdisk without naming a container. Sidon would file it "
                "under one that matches no row, so it inherits no ftt and gets one copy."
                % (name, line))

    def test_each_one_names_the_default_or_a_container_it_checked(self):
        """Naming a container is not enough: a typo is a container that does not exist,
        which reads downstream as 'not configured' and fails nothing."""
        for name, line, _node, container, source in python_creates():
            if container is None or _is_cluster_default(container):
                continue
            route = _route_text(source, line)
            self.assertTrue(
                "hydra.storage_containers" in route or "require_container(" in route,
                "%s:%d creates a vdisk in a container taken from the request, and nothing "
                "on that route looks it up first" % (name, line))


class TheDefaultIsOneContainerEverywhere(unittest.TestCase):
    """Three tiers each fall back to a container name, and a fallback only helps if it is
    the container that exists."""

    def test_python_elixir_and_sidon_agree(self):
        python = re.search(r'DEFAULT_CONTAINER = "([^"]+)"', read(
            os.path.join(HERE, "helios_sidon.py"))).group(1)
        elixir = re.search(r'def default_name, do: "([^"]+)"', read(os.path.join(
            PHX_LIB, "spectrum_phx", "storage", "containers.ex"))).group(1)
        rust = re.search(r'const DEFAULT_CONTAINER: &str = "([^"]+)";', read(
            os.path.join(HERE, "sidon", "src", "control.rs"))).group(1)
        self.assertEqual({python, elixir, rust}, {python},
                         "the default container is spelled differently in two tiers")

    def test_the_installer_creates_that_container(self):
        """A default that nothing creates is a name that matches no row."""
        self.assertIn(
            "VALUES ('default-pool'", read(os.path.join(HERE, "spectrum_server.py")))

    def test_sidon_does_not_fall_back_to_a_name_nothing_creates(self):
        """The literal that was here, "default", is what made every omitted container a
        miss."""
        control = read(os.path.join(HERE, "sidon", "src", "control.rs"))
        body = control[control.index("fn op_create"):]
        body = body[:body.index("\n    }\n")]
        self.assertNotIn('unwrap_or("default")', body)


def _balanced(text, open_at):
    """The text of the parenthesised argument list that opens at `open_at`."""
    depth = 0
    for i in range(open_at, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return text[open_at:i + 1]
    return text[open_at:]


def elixir_creates():
    """Every call to dfs_create and every raw dfs(..., "create", ...) outside spark.ex."""
    found = []
    for path in elixir_sources():
        source = read(path)
        for m in re.finditer(r"dfs_create\(", source):
            before = source[max(0, m.start() - 4):m.start()]
            line_start = source.rfind("\n", 0, m.start()) + 1
            prefix = source[line_start:m.start()]
            if before.endswith("def ") or prefix.strip().startswith(("def ", "defp ", "#")):
                continue
            found.append((path, source.count("\n", 0, m.start()) + 1,
                          _balanced(source, m.end() - 1), source))
        if not path.endswith(os.path.join("spectrum_phx", "spark.ex")):
            for m in re.finditer(r'\bdfs\(\s*[^,]+,\s*"create"', source):
                found.append((path, source.count("\n", 0, m.start()) + 1,
                              source[m.start():m.start() + 200], source))
    return found


class EveryElixirCreateNamesAContainer(unittest.TestCase):

    def test_the_scan_finds_the_creates_it_is_meant_to_police(self):
        files = {os.path.basename(path) for path, *_ in elixir_creates()}
        self.assertTrue({"vms.ex", "images.ex"} <= files,
                        "the VM and image creates were not found; the scan is not looking")

    def test_none_omits_it(self):
        for path, line, call, _source in elixir_creates():
            self.assertIn(
                "container", call,
                "%s:%d creates a vdisk without a container" % (os.path.basename(path), line))

    def test_the_client_refuses_a_create_with_none(self):
        spark = read(os.path.join(PHX_LIB, "spectrum_phx", "spark.ex"))
        self.assertIn("needs a :container", spark)
        self.assertNotIn('"size_bytes" => size_bytes}\n', spark,
                         "dfs_create builds a request body that has no container in it")

    def test_a_module_that_creates_checks_the_container_exists(self):
        for path in {p for p, *_ in elixir_creates()}:
            if path.endswith("uploader.ex"):
                continue
            self.assertIn(
                "Containers.ensure_exists", read(path),
                "%s creates vdisks and never checks the container is a real one"
                % os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
