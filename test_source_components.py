#!/usr/bin/env python3
"""Source components: what the signed package carries, and that it is reproducible.

The upgrade package installs files by copying them, which cannot deliver a compiled
binary. The decision taken was to ship *sources* and build them on the node, so the
release key vouches for code a reader can audit rather than for an artefact from
somebody's machine.

That only means anything if two properties hold, and both are easy to break by accident:

  * **The tarball is byte-identical for identical sources.** The manifest declares a
    sha256 and the release document signs it. A digest that changes because `tar`
    recorded a different mtime, or walked a directory in a different order, or because
    the packager was on Windows and shipped CRLF, is a digest nobody can reproduce and
    therefore nobody can check.

  * **The dependency graph is pinned too.** Without a `Cargo.lock` in the package, two
    nodes building the same signed sources on different days can resolve different
    versions of a transitive crate. The signature would then cover the repository's own
    code and nothing else -- which is most of the code that ends up in the binary.

Run with:  python -m unittest test_source_components
"""

import ast
import hashlib
import io
import os
import tarfile
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
CREATE_UPGRADE_ZIP = os.path.join(HERE, "create_upgrade_zip.py")
HYLIA = os.path.join(HERE, "hylia.py")


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def literal(path, name):
    tree = ast.parse(read(path), filename=path)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError("%s does not assign %s" % (os.path.basename(path), name))


def packager():
    """`crate_tarball` and its inputs, without importing create_upgrade_zip.

    That module does real work at import -- it reads a changelog and resolves a signing
    key -- so the function is compiled out of the source instead.
    """
    tree = ast.parse(read(CREATE_UPGRADE_ZIP), filename=CREATE_UPGRADE_ZIP)
    wanted = {"crate_tarball"}
    body = [n for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name in wanted]
    assert body, "create_upgrade_zip.py no longer defines crate_tarball"
    module = ast.Module(body=body, type_ignores=[])
    scope = {"io": io, "os": os, "SOURCE_INCLUDE": literal(CREATE_UPGRADE_ZIP, "SOURCE_INCLUDE")}
    exec(compile(module, CREATE_UPGRADE_ZIP, "exec"), scope)
    return scope["crate_tarball"]


class SourceComponentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.components = literal(CREATE_UPGRADE_ZIP, "SOURCE_COMPONENTS")
        cls.include = literal(CREATE_UPGRADE_ZIP, "SOURCE_INCLUDE")
        cls.crate_tarball = staticmethod(packager())

    def test_every_source_component_is_a_crate_in_the_tree(self):
        for name, info in self.components.items():
            manifest = os.path.join(HERE, info["dir"], "Cargo.toml")
            self.assertTrue(
                os.path.isfile(manifest),
                f"{name} names {info['dir']}, which has no Cargo.toml")

    def test_each_crate_declares_the_binary_it_is_installed_as(self):
        """The build installs `target/release/<binary>`. A crate producing a differently
        named artefact compiles cleanly and then fails to install, at the end of a
        rolling upgrade rather than the start."""
        for name, info in self.components.items():
            manifest = read(os.path.join(HERE, info["dir"], "Cargo.toml"))
            self.assertIn(
                'name = "%s"' % info["binary"], manifest,
                f"{name} installs as '{info['binary']}' but its Cargo.toml says otherwise")

    def test_every_crate_has_a_committed_lockfile(self):
        """Without this the signature covers this repository's code and not the two
        hundred-odd crates that end up compiled into the binary beside it."""
        missing = sorted(
            name for name, info in self.components.items()
            if not os.path.isfile(os.path.join(HERE, info["dir"], "Cargo.lock")))
        self.assertEqual(
            missing, [],
            f"these crates have no Cargo.lock, so a build from the signed package can "
            f"resolve dependencies the signature never covered: {missing}")

    def test_the_lockfile_is_included_in_what_gets_packaged(self):
        self.assertIn("Cargo.lock", self.include)

    def test_the_tarball_is_byte_identical_across_runs(self):
        crate = os.path.join(HERE, "sidon")
        digests = set()
        for _ in range(2):
            handle, path = tempfile.mkstemp(suffix=".tar.gz")
            os.close(handle)
            try:
                self.crate_tarball(crate, path)
                with open(path, "rb") as f:
                    digests.add(hashlib.sha256(f.read()).hexdigest())
            finally:
                os.unlink(path)
        self.assertEqual(
            len(digests), 1,
            "packaging the same sources twice produced different bytes, so the sha256 in "
            "a signed release cannot be reproduced or checked")

    def test_the_tarball_carries_the_sources_and_nothing_else(self):
        handle, path = tempfile.mkstemp(suffix=".tar.gz")
        os.close(handle)
        try:
            self.crate_tarball(os.path.join(HERE, "sidon"), path)
            with tarfile.open(path) as tar:
                names = sorted(tar.getnames())
                infos = tar.getmembers()
        finally:
            os.unlink(path)

        self.assertIn("Cargo.toml", names)
        self.assertIn("Cargo.lock", names)
        self.assertTrue(any(n.startswith("src/") for n in names))
        # No build output, no VCS, nothing that would make the digest depend on whether
        # the packager happened to have run a build.
        for n in names:
            self.assertFalse(n.startswith("target/"), f"{n} is build output")
            self.assertNotIn(".git", n)

        # Every varying field pinned, which is what makes the digest stable.
        for info in infos:
            self.assertEqual(info.mtime, 0, f"{info.name} carries a timestamp")
            self.assertEqual(info.uid, 0)
            self.assertEqual(info.gid, 0)
            self.assertEqual(info.uname, "")
            self.assertEqual(info.gname, "")

    def test_no_packaged_source_carries_crlf(self):
        """A CRLF file changes the digest without changing the code, so a package built
        on Windows would not match one built on Linux from the same commit."""
        handle, path = tempfile.mkstemp(suffix=".tar.gz")
        os.close(handle)
        try:
            self.crate_tarball(os.path.join(HERE, "sidon"), path)
            with tarfile.open(path) as tar:
                for info in tar.getmembers():
                    data = tar.extractfile(info).read()
                    self.assertNotIn(
                        b"\r\n", data, f"{info.name} was packaged with CRLF endings")
        finally:
            os.unlink(path)

    def test_hylia_builds_with_the_lockfile_enforced(self):
        """`--locked` is what makes the shipped Cargo.lock binding. Without it cargo will
        happily update the lockfile and build something else."""
        source = read(HYLIA)
        self.assertIn(
            "cargo build --release --locked", source,
            "hylia builds without --locked, so the lockfile in the signed package is a "
            "suggestion rather than a pin")

    def test_hylia_only_accepts_the_build_kind_it_implements(self):
        """The build spec arrives inside a package and reaches a root shell. An
        unrecognised kind must be refused rather than interpreted."""
        source = read(HYLIA)
        self.assertIn('if kind != "cargo"', source)


if __name__ == "__main__":
    unittest.main()
