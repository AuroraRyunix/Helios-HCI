#!/usr/bin/env python3
"""Tests for the container-registry override.

Third-party images were written straight into the Quadlet bodies, pinned to docker.io
and quay.io, so provisioning a site that cannot reach either was not possible at all.

The catalogue is duplicated: `provision.py` writes the Quadlets during provisioning and
`deploy_updates.py` rewrites three of the same ones during a rolling update. That
duplication is the risk these tests exist for -- an update that wrote a different image
than provisioning did would silently downgrade a service, and nothing else would notice.

Run with:  python -m unittest test_registry_override
"""

import ast
import importlib.util
import io
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_provision():
    """Import provision.py. Everything at module level is definitions."""
    spec = importlib.util.spec_from_file_location(
        "provision_under_test", os.path.join(HERE, "provision.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules["provision_under_test"] = module
    spec.loader.exec_module(module)
    return module


def literal_images_from(path):
    """The IMAGES dict of a module, read statically.

    `deploy_updates.py` prompts for input at import, so it cannot simply be imported.
    """
    tree = ast.parse(io.open(os.path.join(HERE, path), encoding="utf-8").read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            target = getattr(node.targets[0], "id", None)
            if target == "IMAGES":
                return ast.literal_eval(node.value)
    raise AssertionError("no IMAGES dict in " + path)


provision = load_provision()


class ResolverTests(unittest.TestCase):
    def setUp(self):
        self._saved = provision.REGISTRY
        provision.REGISTRY = ""

    def tearDown(self):
        provision.REGISTRY = self._saved

    def test_without_an_override_images_are_untouched(self):
        for name, reference in provision.IMAGES.items():
            self.assertEqual(provision.resolve_image(name), reference)

    def test_the_registry_host_is_replaced_and_the_path_kept(self):
        # What a mirror or pull-through cache expects. Replacing the whole reference, or
        # appending to it, produces a path that exists on no registry.
        provision.REGISTRY = "mirror.local:5000"
        self.assertEqual(provision.resolve_image("zookeeper"),
                         "mirror.local:5000/library/zookeeper:3.9.2")
        self.assertEqual(provision.resolve_image("aether"),
                         "mirror.local:5000/piraeusdatastore/piraeus-server:v1.31.0")

    def test_a_trailing_slash_does_not_double_up(self):
        provision.REGISTRY = "mirror.local:5000/"
        self.assertNotIn("//", provision.resolve_image("slate").replace("://", ""))

    def test_tags_survive(self):
        # A mirror serving a different image under the same tag is a problem nothing here
        # can detect, but silently dropping the tag would be one this code caused.
        provision.REGISTRY = "mirror.local:5000"
        for name in provision.IMAGES:
            self.assertRegex(provision.resolve_image(name), r":[\w.-]+$")

    def test_an_unknown_component_raises(self):
        # A typo that returned something plausible would fail at pull time on a node,
        # long after the operator stopped watching.
        with self.assertRaises(KeyError):
            provision.resolve_image("no-such-component")


class QuadletTests(unittest.TestCase):
    """The bodies must still format, and must carry no hardcoded registry."""

    FORMAT_ARGS = {
        "zookeeper": dict(node_id=1, zoo_servers="s", peer_type=""),
        "hydra-db": dict(seeds="10.0.0.1", node_ip="10.0.0.1"),
        "aether": {},
        "linstor-controller": {},
        "slate": {},
    }

    def test_every_overridable_quadlet_formats(self):
        for name, kwargs in self.FORMAT_ARGS.items():
            with self.subTest(quadlet=name):
                body = provision.QUADLETS[name].format(
                    image=provision.resolve_image(name), **kwargs)
                self.assertIn("Image=" + provision.IMAGES[name], body)

    def test_no_quadlet_hardcodes_a_public_registry(self):
        source = io.open(os.path.join(HERE, "provision.py"), encoding="utf-8").read()
        # Inside a Quadlet body an Image= line must be the placeholder or a local image.
        offenders = [m for m in re.findall(r"^Image=(.+)$", source, re.M)
                     if not m.startswith(("{image}", "localhost/"))]
        self.assertEqual(offenders, [], "these Image= lines cannot be redirected")


class CatalogueAgreementTests(unittest.TestCase):
    """provision.py and deploy_updates.py must not drift."""

    def test_the_shared_components_reference_the_same_images(self):
        theirs = literal_images_from("deploy_updates.py")
        shared = set(theirs) & set(provision.IMAGES)
        self.assertTrue(shared, "deploy_updates shares no components with provision")
        for name in sorted(shared):
            with self.subTest(component=name):
                self.assertEqual(
                    theirs[name], provision.IMAGES[name],
                    "a rolling update would write a different image than provisioning did")

    def test_deploy_updates_covers_the_quadlets_it_rewrites(self):
        # It rewrites aether, the linstor controller and slate. Any of those missing from
        # its catalogue means that quadlet still carries a hardcoded registry.
        theirs = literal_images_from("deploy_updates.py")
        for name in ("aether", "linstor-controller", "slate"):
            self.assertIn(name, theirs)

    def test_each_component_resolves_under_its_own_key(self):
        # aether and the linstor controller happen to be the same image today. Resolving
        # the controller under the aether key works by coincidence and would break
        # silently the moment either moved.
        source = io.open(os.path.join(HERE, "deploy_updates.py"), encoding="utf-8").read()
        self.assertIn('resolve_image("linstor-controller")', source)
        self.assertEqual(source.count('resolve_image("aether")'), 1)


class DockerfileTests(unittest.TestCase):
    def test_the_base_image_is_a_build_arg(self):
        source = io.open(os.path.join(HERE, "Dockerfile"), encoding="utf-8").read()
        self.assertRegex(source, r"ARG BASE_IMAGE=")
        self.assertRegex(source, r"FROM \$\{BASE_IMAGE\}")


if __name__ == "__main__":
    unittest.main()
