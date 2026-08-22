#!/usr/bin/env python3
"""Regression tests for the deployment manifest.

Every assertion here corresponds to a defect this repository has actually shipped.
The deployment story is spread across five independent, hand-maintained lists, and
nothing until now compared them to each other:

    sync_provision.py       mapping                source file to embed, by var name
    provision.py            *_B64                  the embedded payloads themselves
    create_upgrade_zip.py   components_map         component -> {source, target path}
    check_updates.py        components_paths       component -> target path (LCM)
    deploy_updates.py       SPECTRUM_BUILD_FILES   the console image's build context

Drift between any two of them is silent. `sync_provision.py` covering 21 of 24
constants meant three daemons kept shipping the copy embedded at whatever commit
they were last synced; Lanayru was missing from four of these lists simultaneously
while `spectrum_server.py` imported it at runtime.

The remaining two tests cover the other two ways this deployment path has broken:
Python source embedded as a string literal (never parsed at import, so a syntax
error survives every `py_compile`), and CRLF line endings in a file that is decoded
straight into `/usr/local/bin` on a Linux host.

Nothing here imports the modules under test. `sync_provision.py` rewrites
`provision.py` at import time, `check_updates.py` and `create_upgrade_zip.py` do
real work, and `provision.py` is ~1.6MB of base64. The declarations are read
statically instead.

Run with:  python -m unittest test_deployment_manifest
"""

import ast
import os
import re
import unittest

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))

PROVISION = os.path.join(REPO_ROOT, "provision.py")
SYNC_PROVISION = os.path.join(REPO_ROOT, "sync_provision.py")
CREATE_UPGRADE_ZIP = os.path.join(REPO_ROOT, "create_upgrade_zip.py")
CHECK_UPDATES = os.path.join(REPO_ROOT, "check_updates.py")
DEPLOY_UPDATES = os.path.join(REPO_ROOT, "deploy_updates.py")
DOCKERFILE = os.path.join(REPO_ROOT, "Dockerfile")

# `COPY <source> <destination>` in the Spectrum Dockerfile. Only the source matters
# here: it names a file that has to be in the build context.
DOCKERFILE_COPY_RE = re.compile(r"^COPY\s+(\S+)\s+\S+\s*$", re.MULTILINE)

# Files whose embedded string literals are dispatched to nodes and executed as
# Python. `handle_cluster_create` sends these to every host and JSON-parses the
# result, so a syntax error surfaces only as "returned invalid json".
EMBEDDED_SCRIPT_SOURCES = ("spark_daemon_decoded.py", "cluster_new.py")

# The same pattern sync_provision.py uses to detect its own drift, so this test
# sees exactly the set of constants that tool sees.
B64_CONSTANT_RE = re.compile(r"^([A-Z_]+_B64)\s*=", re.MULTILINE)


def read_text(path):
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def literal_assignment(path, name):
    """Return the literal value assigned to `name` anywhere in `path`.

    Uses ast rather than import because every module inspected here executes
    real work at import time. Searches the whole tree, not just module scope --
    `components_paths` lives inside `check_updates.collect_inventory()`.
    """
    tree = ast.parse(read_text(path), filename=path)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == name:
                return ast.literal_eval(node.value)
    raise AssertionError(f"{os.path.basename(path)} no longer assigns a literal '{name}'")


def embedded_script_literals(path):
    """Yield (variable name, line number, source) for each `*_script` str literal."""
    tree = ast.parse(read_text(path), filename=path)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not (isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id.endswith("_script"):
                yield target.id, node.lineno, node.value.value


class TestSpectrumBuildContext(unittest.TestCase):
    """The Dockerfile's COPY lines vs what deploy_updates.py stages for the build.

    This drifted twice, and both times silently. The Dockerfile gained
    `COPY lanayru.py` and later `COPY helios_sidon.py`; deploy_updates.py staged
    neither, so `podman build` failed with "no such file or directory" on every
    rollout -- and the script printed the error, continued, restarted spectrum onto
    the image that was already running, and reported the deployment successful. The
    console silently stopped being upgraded while nothing said so.

    The build failure is fatal now. This keeps it from happening in the first place.
    """

    @classmethod
    def setUpClass(cls):
        cls.build_files = literal_assignment(DEPLOY_UPDATES, "SPECTRUM_BUILD_FILES")
        cls.staged = {staged for _source, staged in cls.build_files}
        copied = DOCKERFILE_COPY_RE.findall(read_text(DOCKERFILE))
        # `static/` is a directory, walked separately rather than listed.
        cls.copied = {name for name in copied if not name.endswith("/")}

    def test_every_file_the_dockerfile_copies_is_staged(self):
        missing = sorted(self.copied - self.staged)
        self.assertEqual(
            missing,
            [],
            "Dockerfile COPYs these into the Spectrum image but deploy_updates.py's "
            f"SPECTRUM_BUILD_FILES does not stage them, so podman build fails: {missing}",
        )

    def test_nothing_is_staged_that_the_image_does_not_want(self):
        """The reverse is not a build failure, but it is worth being told about: a
        file in the context that no COPY names is either a forgotten COPY line or a
        stale entry."""
        extra = sorted(self.staged - self.copied)
        self.assertEqual(
            extra,
            [],
            "deploy_updates.py stages these into the Spectrum build context but the "
            f"Dockerfile COPYs none of them: {extra}",
        )

    def test_every_staged_source_exists(self):
        missing = sorted(
            source for source, _staged in self.build_files
            if not os.path.exists(os.path.join(REPO_ROOT, source))
        )
        self.assertEqual(
            missing,
            [],
            f"SPECTRUM_BUILD_FILES names sources that do not exist: {missing}",
        )


class TestRustCrates(unittest.TestCase):
    """Every Rust service the tree builds is one an upgrade can actually ship.

    `sidon` was reachable only through provision.py, which meant the storage daemon
    could be changed in the repository and never reach a running cluster except by
    reprovisioning the node -- the one component where that is the least acceptable
    answer.
    """

    @classmethod
    def setUpClass(cls):
        cls.crates = literal_assignment(DEPLOY_UPDATES, "RUST_CRATES")

    def test_every_crate_in_the_tree_is_deployed(self):
        in_tree = {
            name for name in os.listdir(REPO_ROOT)
            if os.path.isfile(os.path.join(REPO_ROOT, name, "Cargo.toml"))
        }
        listed = {crate for crate, _binary in self.crates}
        missing = sorted(in_tree - listed)
        self.assertEqual(
            missing,
            [],
            "these crates have a Cargo.toml in the repository but deploy_updates.py's "
            f"RUST_CRATES does not build them, so no rollout can ship them: {missing}",
        )

    def test_every_deployed_crate_exists(self):
        missing = sorted(
            crate for crate, _binary in self.crates
            if not os.path.isfile(os.path.join(REPO_ROOT, crate, "Cargo.toml"))
        )
        self.assertEqual(
            missing, [], f"RUST_CRATES names crates that are not in the tree: {missing}"
        )

    def test_each_crate_declares_the_binary_it_is_deployed_as(self):
        """The install step copies target/release/<binary>. A crate whose Cargo.toml
        produces a differently-named artefact would build cleanly and then fail to
        install, at the end of a rollout rather than the start."""
        for crate, binary in self.crates:
            manifest = read_text(os.path.join(REPO_ROOT, crate, "Cargo.toml"))
            self.assertIn(
                'name = "%s"' % binary,
                manifest,
                f"deploy_updates.py installs {crate} as '{binary}', but "
                f"{crate}/Cargo.toml does not declare that name",
            )


class TestProvisionEmbedding(unittest.TestCase):
    """provision.py's *_B64 constants vs sync_provision.py's mapping."""

    @classmethod
    def setUpClass(cls):
        cls.mapping = literal_assignment(SYNC_PROVISION, "mapping")
        cls.declared = set(B64_CONSTANT_RE.findall(read_text(PROVISION)))

    def test_every_provision_constant_is_mapped(self):
        """A constant provision.py declares but sync_provision.py does not map is
        never re-encoded: editing its source file ships the previously embedded
        copy, with no warning. This is how 3 of 24 constants went stale."""
        unmapped = sorted(self.declared - set(self.mapping))
        self.assertEqual(
            unmapped,
            [],
            "provision.py declares these *_B64 constants but sync_provision.py's "
            f"mapping does not cover them, so their source files are never "
            f"re-embedded: {unmapped}",
        )

    def test_every_mapped_constant_is_declared(self):
        """Drift in the other direction: a mapping entry with no constant in
        provision.py is a silent no-op, because the regex replacement matches
        nothing and the component simply never reaches a node."""
        stale = sorted(set(self.mapping) - self.declared)
        self.assertEqual(
            stale,
            [],
            "sync_provision.py's mapping references these constants, which "
            f"provision.py does not declare: {stale}",
        )

    def test_every_mapped_source_file_exists(self):
        """sync_provision.py aborts on a missing source file. Catch it here rather
        than halfway through a sync."""
        missing = sorted(
            f"{var} -> {src}"
            for var, src in self.mapping.items()
            if not os.path.exists(os.path.join(REPO_ROOT, src))
        )
        self.assertEqual(missing, [], f"Mapped source files that do not exist: {missing}")


class TestUpgradePackageInventory(unittest.TestCase):
    """create_upgrade_zip.py's components_map vs check_updates.py's components_paths."""

    @classmethod
    def setUpClass(cls):
        cls.components_map = literal_assignment(CREATE_UPGRADE_ZIP, "components_map")
        cls.components_paths = literal_assignment(CHECK_UPDATES, "components_paths")

    def test_every_packaged_component_is_inventoried(self):
        """A component shipped in the upgrade zip but absent from the LCM
        inventory is invisible to check-updates: it is deployed, but its version
        is never read back, so it can never be reported as out of date."""
        missing = sorted(set(self.components_map) - set(self.components_paths))
        self.assertEqual(
            missing,
            [],
            "create_upgrade_zip.py packages these components but "
            f"check_updates.py's components_paths does not inventory them: {missing}",
        )

    def test_every_inventoried_component_is_packaged(self):
        """The reverse: check-updates reports 'N/A' forever for a component that
        no upgrade package can ever install."""
        missing = sorted(set(self.components_paths) - set(self.components_map))
        self.assertEqual(
            missing,
            [],
            "check_updates.py inventories these components but "
            f"create_upgrade_zip.py does not package them: {missing}",
        )

    def test_target_paths_agree(self):
        """Both lists name an absolute install path, and they must be the same
        path. Several components deliberately keep a suffix or a hyphenated name
        (daruk.py, lanayru.py, helios_zk.py, check-updates, spectrum_server); a
        mismatch means hylia installs to one path while the inventory reads
        another, so the component reports its old version forever after a
        successful upgrade."""
        mismatched = sorted(
            f"{name}: zip installs {info['target']!r}, inventory reads "
            f"{self.components_paths[name]!r}"
            for name, info in self.components_map.items()
            if name in self.components_paths
            and info["target"] != self.components_paths[name]
        )
        self.assertEqual(mismatched, [], f"Target path mismatches: {mismatched}")

    def test_every_packaged_source_file_exists(self):
        missing = sorted(
            f"{name} -> {info['src']}"
            for name, info in self.components_map.items()
            if not os.path.exists(os.path.join(REPO_ROOT, info["src"]))
        )
        self.assertEqual(missing, [], f"Packaged source files that do not exist: {missing}")


class TestEmbeddedScripts(unittest.TestCase):
    """Python embedded as a string literal is never parsed at import."""

    def test_embedded_scripts_compile(self):
        """`disk_claim_script` shipped with an IndentationError for months.
        handle_cluster_create dispatches it to every node and JSON-parses stdout,
        so cluster creation failed with 'returned invalid json' and nothing in
        the repo -- not py_compile, not import -- looked inside the string."""
        total = 0
        for filename in EMBEDDED_SCRIPT_SOURCES:
            path = os.path.join(REPO_ROOT, filename)
            found = 0
            for name, lineno, source in embedded_script_literals(path):
                found += 1
                total += 1
                with self.subTest(file=filename, script=name, line=lineno):
                    try:
                        compile(source, f"{filename}:{lineno}:{name}", "exec")
                    except SyntaxError as exc:
                        self.fail(
                            f"{filename}:{lineno} {name} does not compile: "
                            f"{type(exc).__name__}: {exc.msg} "
                            f"(embedded line {exc.lineno})"
                        )
            self.assertGreater(
                found,
                0,
                f"No *_script string literals found in {filename}. Either they were "
                "renamed or moved -- update EMBEDDED_SCRIPT_SOURCES / the naming "
                "convention rather than letting this test pass vacuously.",
            )
        self.assertGreaterEqual(total, 6, "Expected at least the six known embedded scripts")


class TestLineEndings(unittest.TestCase):
    """CRLF in a file that is decoded straight onto a Linux node."""

    maxDiff = None

    def test_no_crlf_in_embedded_sources(self):
        """A CRLF working tree shipped `#!/usr/bin/env python3\\r` to every node,
        so every script failed at exec with:
            /usr/bin/env: 'python3\\r': No such file or directory

        .gitattributes pins these to LF and both packagers now normalize on the
        way out, but the invariant is that the working tree itself is LF: these
        files are also read by tooling that does not normalize, and a CRLF tree
        is what git core.autocrlf=true produces on a Windows checkout.

        Fix a failure with:  git add --renormalize . && git checkout -- .
        """
        sources = set(literal_assignment(SYNC_PROVISION, "mapping").values())
        sources.update(
            info["src"] for info in literal_assignment(CREATE_UPGRADE_ZIP, "components_map").values()
        )

        offenders = []
        for name in sorted(sources):
            path = os.path.join(REPO_ROOT, name)
            if not os.path.exists(path):
                continue  # reported by the existence tests above
            with open(path, "rb") as handle:
                blob = handle.read()
            count = blob.count(b"\r\n")
            if count:
                offenders.append(f"{name} ({count} CRLF line endings)")

        self.assertEqual(
            offenders,
            [],
            "These files are embedded into provision.py / packaged into the upgrade "
            "zip and decoded onto Linux hosts, but contain CRLF line endings in the "
            f"working tree: {offenders}. Repair with "
            "`git add --renormalize . && git checkout -- .`",
        )



class WorkflowFilesTest(unittest.TestCase):
    """GitHub Actions workflows must parse, or CI silently never runs.

    A workflow that fails to parse reports as a failed run with zero jobs and no logs,
    which is easy to mistake for a flaky runner. This caught a stray carriage return
    embedded mid-comment: YAML treats a bare CR as a line break, so the remainder of the
    comment became a syntax error on the following line.
    """

    def _workflow_paths(self):
        root = os.path.join(REPO_ROOT, ".github", "workflows")
        if not os.path.isdir(root):
            return []
        return [os.path.join(root, f) for f in os.listdir(root)
                if f.endswith((".yml", ".yaml"))]

    def test_workflows_contain_no_stray_carriage_returns(self):
        offenders = []
        for path in self._workflow_paths():
            raw = open(path, "rb").read()
            if bytes([13]) in raw:
                offenders.append(os.path.basename(path))
        self.assertEqual(
            [], offenders,
            "Workflow files contain carriage returns. YAML treats a bare CR as a line "
            "break, so this corrupts the document and the workflow fails to start: "
            + ", ".join(offenders))

    def test_workflows_parse_as_yaml(self):
        try:
            import yaml
        except ImportError:
            self.skipTest("pyyaml not installed; workflow parsing not verified")
        for path in self._workflow_paths():
            with self.subTest(workflow=os.path.basename(path)):
                try:
                    doc = yaml.safe_load(open(path, encoding="utf-8").read())
                except Exception as exc:
                    self.fail(f"{os.path.basename(path)} is not valid YAML: {exc}")
                self.assertIsInstance(doc, dict, "workflow must be a mapping")
                self.assertIn("jobs", doc, "workflow defines no jobs")
                self.assertTrue(doc["jobs"], "workflow defines an empty jobs map")

class QuadletPrivilegeTest(unittest.TestCase):
    """Container privilege is declared in provision.py and nowhere else.

    `--privileged` gives a container every capability, turns SELinux confinement off,
    and bind-mounts the host's whole /dev -- every block device, including the boot
    disk. The web console had it for years while being unable to use it: a container's
    /dev has device nodes but not udev's subdirectories, so the /dev/drbd/by-res/...
    paths it actually referenced were never there. It was pure attack surface on the
    most exposed component in the cluster, and these tests keep it from coming back by
    accident.
    """

    # Only the Linstor satellite legitimately manipulates the host: kernel modules,
    # device mapper, block devices. Anything else asking for privilege is a bug.
    ALLOWED_PRIVILEGED = {"aether"}

    def _quadlets(self):
        source = open(PROVISION, encoding="utf-8").read()
        # The QUADLETS values are triple-quoted literals keyed by unit name.
        found = dict(re.findall(r'"([a-z0-9-]+)":\s*"""(.*?)"""', source, re.S))
        self.assertTrue(found, "no quadlet definitions found in provision.py")
        return found

    def _directives(self, body):
        """Real directive lines only -- '##' comments discuss privilege in prose."""
        return [
            line.strip() for line in body.splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]

    def test_only_the_storage_satellite_is_privileged(self):
        for name, body in self._quadlets().items():
            privileged = any(
                "PodmanArgs" in line and "--privileged" in line
                for line in self._directives(body)
            )
            if name in self.ALLOWED_PRIVILEGED:
                continue
            with self.subTest(unit=name):
                self.assertFalse(
                    privileged,
                    f"{name}.container requests --privileged. If it genuinely needs host "
                    f"access, add it to ALLOWED_PRIVILEGED with the reason; otherwise "
                    f"scope it to explicit AddDevice/AddCapability grants.")

    def test_the_web_console_drops_capabilities(self):
        # Not merely "not privileged": podman still grants a default capability set,
        # and Spectrum needs none of it. It binds a port above 1024 and reads files it
        # owns.
        body = self._quadlets().get("spectrum")
        self.assertIsNotNone(body, "no spectrum quadlet in provision.py")
        directives = self._directives(body)
        self.assertIn("DropCapability=ALL", directives)
        self.assertIn("NoNewPrivileges=true", directives)

    def test_privileged_units_explain_themselves(self):
        # A privilege nobody wrote a reason for is one nobody can review.
        for name, body in self._quadlets().items():
            if not any("--privileged" in l for l in self._directives(body)):
                continue
            with self.subTest(unit=name):
                self.assertIn(
                    "##", body,
                    f"{name}.container is privileged with no comment saying why")


class WorkflowTestDiscoveryTest(unittest.TestCase):
    """CI must discover test files, not enumerate them.

    The unit-test step used to name every file. Adding a suite and forgetting to add it
    there meant the suite simply never ran -- silently, with CI still green. Two were
    missing when this was noticed, one of them covering fencing.
    """

    def test_ci_discovers_tests_rather_than_naming_them(self):
        path = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")
        content = read_text(path)
        step = [l for l in content.splitlines() if "python -m unittest" in l]
        self.assertTrue(step, "no unittest step in ci.yml")
        for line in step:
            self.assertIn("discover", line,
                          "the unit-test step enumerates files; a new suite would not run")
            self.assertNotRegex(
                line, r"test_\w+\.py",
                "the unit-test step still names individual test files")

    def test_every_test_file_would_be_discovered(self):
        # Discovery uses the pattern in ci.yml; a file outside it is invisible.
        names = [f for f in os.listdir(REPO_ROOT)
                 if f.startswith("test_") and f.endswith(".py")]
        self.assertGreater(len(names), 10, "test files seem to have gone missing")


if __name__ == "__main__":
    unittest.main()
