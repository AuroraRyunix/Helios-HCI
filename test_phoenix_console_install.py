#!/usr/bin/env python3
"""A created cluster has the Phoenix console running, without a rollout.

`cluster create` finished with Spectrum on 8443 and nothing on 8444, because the console's
Quadlet and image were installed only by `deploy_updates.py`. Slate routes the console to
127.0.0.1:8444, and Bifrost's local health guard refuses to bind the VIP while that backend is
down, so Mimir's `vip_binding_status` failed on every brand-new cluster until a rollout ran.

Where each part now lives:

  * provisioning installs the unit (read from `spectrum_phx/quadlet/`, the same file the
    rollout installs, so the two cannot write different consoles) and builds the image, on
    every node it touches;
  * `cluster create` makes sure the environment file exists (a destroy removes it), starts the
    unit in Phase 6, and checks 8444 in Phase 7;
  * `spectrum-phx` is declared in MANAGED_SERVICES, so start/stop/status and the reconcile loop
    handle it (the registries are asserted in test_service_wiring.py).

Nothing here imports deploy_updates.py (it prompts on stdin) or provision.py (1.6 MB of base64
and import-time side effects): they are read as text and through the syntax tree.

Run with:  python -m unittest test_phoenix_console_install
"""

import ast
import base64
import importlib.util
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def read(*parts):
    with open(os.path.join(HERE, *parts), "r", encoding="utf-8", newline="") as handle:
        return handle.read().replace("\r\n", "\n")


def module_constant(path, name):
    for node in ast.parse(read(path)).body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == name:
            return ast.literal_eval(node.value)
    raise AssertionError("%s no longer defines %s" % (path, name))


def function_source(path, name):
    text = read(path)
    for node in ast.parse(text).body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(text, node)
    raise AssertionError("%s no longer defines %s()" % (path, name))


def string_lists(path, function, target):
    """Every list literal assigned to `target` inside `function`, as lists of strings."""
    found = []
    for node in ast.walk(ast.parse(read(path))):
        if isinstance(node, ast.FunctionDef) and node.name == function:
            for child in ast.walk(node):
                if (isinstance(child, ast.Assign) and isinstance(child.value, ast.List)
                        and getattr(child.targets[0], "id", "") == target):
                    found.append([e.value for e in child.value.elts
                                  if isinstance(e, ast.Constant) and isinstance(e.value, str)])
    return found


class ProvisioningInstallsTheConsole(unittest.TestCase):

    def test_it_writes_the_unit_from_the_one_file_the_rollout_installs(self):
        provision = read("provision.py")
        self.assertRegex(
            provision,
            r'node\.write_file\("/etc/containers/systemd/spectrum-phx\.container",\s*phx_quad\)',
            "provisioning does not install the Phoenix console's Quadlet, so a created cluster "
            "has no console until a rollout writes it")
        self.assertIn('PHX_QUADLET_PATH = os.path.join("spectrum_phx", "quadlet", '
                      '"spectrum-phx.container")', provision)
        self.assertIn("spectrum_phx", read("deploy_updates.py"))
        self.assertIn('"quadlet", "spectrum-phx.container"', read("deploy_updates.py"),
                      "the rollout stopped reading the same Quadlet file as provisioning")

    def test_it_builds_the_image_the_unit_refuses_to_pull(self):
        provision = read("provision.py")
        unit = read("spectrum_phx", "quadlet", "spectrum-phx.container")
        self.assertIn("Pull=never", unit)
        image = re.search(r"^Image=(\S+)$", unit, re.M).group(1)
        self.assertIn("podman build -t %s " % image, provision,
                      "the unit is Pull=never, so without a build here it cannot start")

    def test_the_build_context_is_the_same_set_the_rollout_ships(self):
        self.assertEqual(module_constant("provision.py", "PHX_BUILD_FILES"),
                         module_constant("deploy_updates.py", "SPECTRUM_PHX_FILES"))
        self.assertEqual(module_constant("provision.py", "PHX_BUILD_DIRS"),
                         module_constant("deploy_updates.py", "SPECTRUM_PHX_DIRS"))

    def test_every_path_in_the_build_context_exists(self):
        for name in (module_constant("provision.py", "PHX_BUILD_FILES")
                     + module_constant("provision.py", "PHX_BUILD_DIRS")):
            self.assertTrue(os.path.exists(os.path.join(HERE, "spectrum_phx", name)), name)

    def test_the_gzip_archive_is_not_run_through_the_crlf_rewrite(self):
        # write_file() rewrites CRLF in bytes, which corrupts a gzip stream. The archive has to
        # travel by sftp.put.
        provision = read("provision.py")
        self.assertRegex(provision, r'phx_sftp\.put\(phx_archive, "/tmp/spectrum_phx\.tar\.gz"\)')
        self.assertNotRegex(provision, r'write_file\("/tmp/spectrum_phx\.tar\.gz"')

    def test_a_joining_node_is_not_given_a_secret_of_its_own(self):
        self.assertRegex(read("provision.py"), r"if PHX_SECRET and not args\.join:",
                         "a node joining an existing cluster must not write the secret minted "
                         "for this run: it is not that cluster's, and sessions would not verify")

    def test_provisioning_does_not_start_it(self):
        # It needs the database; create's Phase 6 starts it once that is up.
        for services in string_lists("provision.py", "main", "services"):
            self.assertNotIn("spectrum-phx", services)


class TheHardenedFormIsTheOneInstalled(unittest.TestCase):
    """The open question in TODO.md is about the *Python* console's Quadlet. The Phoenix one has
    a single source and is the hardened form, so there is nothing to decide here."""

    def setUp(self):
        self.unit = read("spectrum_phx", "quadlet", "spectrum-phx.container")

    def test_it_is_not_privileged(self):
        self.assertNotRegex(self.unit, r"(?m)^PodmanArgs=.*--privileged")
        self.assertRegex(self.unit, r"(?m)^DropCapability=ALL$")
        self.assertRegex(self.unit, r"(?m)^NoNewPrivileges=true$")

    def test_provisioning_and_the_rollout_do_not_carry_a_copy_of_it(self):
        for path in ("provision.py", "deploy_updates.py"):
            self.assertNotIn("ContainerName=spectrum-phx", read(path),
                             "%s embeds its own copy of the console Quadlet" % path)

    def test_it_serves_the_port_slate_routes_to(self):
        self.assertIn("Environment=PORT=8444", self.unit)
        self.assertIn("127.0.0.1:8444", read("slate_config", "dynamic.yml"))


class CreateStartsAndChecksIt(unittest.TestCase):

    def test_it_is_declared_and_waits_for_the_database_proxy(self):
        table = module_constant("spark_daemon_decoded.py", "MANAGED_SERVICES")
        entry = [e for e in table if e["unit"] == "spectrum-phx"]
        self.assertEqual(len(entry), 1)
        self.assertEqual(entry[0]["requires"], ("daruk",))

    def test_phase_6_starts_it_before_the_vip_is_bound(self):
        for path, function in (("cluster_new.py", "main"),
                               ("spark_daemon_decoded.py", "handle_cluster_create")):
            lists = [l for l in string_lists(path, function, "services") if "bifrost" in l]
            self.assertTrue(lists, path)
            for services in lists:
                self.assertIn("spectrum-phx", services, path)
                self.assertLess(services.index("spectrum-phx"), services.index("bifrost"),
                                "%s: Bifrost's guard needs the console already listening" % path)

    def test_phase_7_looks_for_it_on_8444(self):
        self.assertIn("port_listening(ip, 8444)", read("cluster_new.py"))
        self.assertIn("Phoenix console is not listening on", read("spark_daemon_decoded.py"))

    def test_create_ensures_the_environment_file_before_phase_6(self):
        text = read("cluster_new.py")
        self.assertLess(text.index("ensure_phoenix_env(ips)"),
                        text.index("Phase 6: Starting Core HCI Services"))

    def test_destroy_removes_the_container(self):
        for path in ("cluster_new.py", "spark_daemon_decoded.py"):
            self.assertRegex(read(path), r"podman rm -f systemd-hydra-db systemd-zookeeper "
                                         r"systemd-spectrum spectrum-phx")


class _Nodes(object):
    """Stands in for run_remote_spark: a dict of node -> file contents, and a log of writes."""

    def __init__(self, files=None):
        self.files = dict(files or {})
        self.commands = []

    def run(self, ip, command, timeout=None):
        self.commands.append((ip, command))
        if command.startswith("grep -h '^SECRET_KEY_BASE='"):
            text = self.files.get(ip, "")
            lines = [l for l in text.splitlines() if l.startswith("SECRET_KEY_BASE=")]
            return 0, "\n".join(lines), ""
        match = re.search(r"echo (\S+) \| base64 -d > (\S+)", command)
        if match:
            self.files[ip] = base64.b64decode(match.group(1)).decode("utf-8")
            self.mode = ("chmod 600 %s" % match.group(2)) in command
            return 0, "", ""
        return 1, "", "unexpected command"


def _secret(text):
    return re.search(r"^SECRET_KEY_BASE=(.*)$", text, re.M).group(1)


class EnsureThePhoenixEnvironment(unittest.TestCase):
    """The same function lives in cluster_new.py and spark_daemon_decoded.py; both are run."""

    def _function(self, path, nodes):
        namespace = {"base64": base64, "run_remote_spark": nodes.run}
        exec(compile("PHX_ENV_PATH = %r\n%s" % (
            module_constant(path, "PHX_ENV_PATH"), function_source(path, "ensure_phoenix_env")),
            path, "exec"), namespace)
        return namespace["ensure_phoenix_env"]

    def each(self):
        for path in ("cluster_new.py", "spark_daemon_decoded.py"):
            with self.subTest(path):
                yield path

    def test_nodes_without_a_file_all_get_the_same_new_secret(self):
        for path in self.each():
            nodes = _Nodes()
            written = self._function(path, nodes)(["10.0.0.1", "10.0.0.2", "10.0.0.3"])
            self.assertEqual(written, ["10.0.0.1", "10.0.0.2", "10.0.0.3"])
            secrets = set(_secret(t) for t in nodes.files.values())
            self.assertEqual(len(secrets), 1)
            self.assertGreaterEqual(len(secrets.pop()), 60)
            self.assertTrue(nodes.mode, "the file must be mode 0600: it holds the secret")

    def test_each_node_names_itself_and_every_origin(self):
        for path in self.each():
            nodes = _Nodes()
            self._function(path, nodes)(["10.0.0.1", "10.0.0.2"])
            self.assertIn("PHX_HOST=10.0.0.2\n", nodes.files["10.0.0.2"])
            self.assertIn("PHX_EXTRA_ORIGINS=10.0.0.1,10.0.0.2\n", nodes.files["10.0.0.1"])

    def test_an_existing_secret_is_reused_and_never_rewritten(self):
        for path in self.each():
            kept = "SECRET_KEY_BASE=keepme\nPHX_HOST=10.0.0.1\n"
            nodes = _Nodes({"10.0.0.1": kept})
            written = self._function(path, nodes)(["10.0.0.1", "10.0.0.2"])
            self.assertEqual(written, ["10.0.0.2"])
            self.assertEqual(nodes.files["10.0.0.1"], kept)
            self.assertEqual(_secret(nodes.files["10.0.0.2"]), "keepme")

    def test_a_failed_write_is_an_error_not_a_silent_skip(self):
        for path in self.each():
            nodes = _Nodes()
            original = nodes.run

            def failing(ip, command, timeout=None, _o=original):
                if "base64 -d" in command:
                    return 1, "", "read-only file system"
                return _o(ip, command, timeout)

            nodes.run = failing
            with self.assertRaises(RuntimeError):
                self._function(path, nodes)(["10.0.0.1"])


class TheDocsSayWhereItComesFrom(unittest.TestCase):

    def test_cluster_doc_phase_6_names_the_console(self):
        text = read("docs", "cluster.md")
        phase = text[text.index("6. **Core services.**"):text.index("7. **Liveness")]
        self.assertIn("spectrum-phx", phase)
        self.assertIn("8444", phase)

    def test_the_phoenix_doc_says_provisioning_installs_it(self):
        self.assertRegex(read("docs", "spectrum_phx.md"),
                         r"(?s)## 0\. Where it comes from.*provision\.py. writes")

    def test_the_todo_entry_is_resolved(self):
        self.assertNotIn("## P1 — A freshly created cluster has no Phoenix console until the "
                         "rollout runs", read("TODO.md"))

    def test_readme_still_links_the_subdocs(self):
        readme = read("README.md")
        for doc in ("docs/cluster.md", "docs/spectrum_phx.md"):
            self.assertIn(doc, readme)


if __name__ == "__main__":
    unittest.main()
