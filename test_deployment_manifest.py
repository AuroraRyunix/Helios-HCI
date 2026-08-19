#!/usr/bin/env python3
"""Regression tests for the deployment manifest.

Every assertion here corresponds to a defect this repository has actually shipped.
The deployment story is spread across four independent, hand-maintained lists, and
nothing until now compared them to each other:

    sync_provision.py       mapping          var name  -> source file to embed
    provision.py            *_B64            the embedded payloads themselves
    create_upgrade_zip.py   components_map   component -> {source file, target path}
    check_updates.py        components_paths component -> target path (LCM inventory)

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


if __name__ == "__main__":
    unittest.main()
