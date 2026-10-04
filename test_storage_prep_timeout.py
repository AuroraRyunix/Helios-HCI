#!/usr/bin/env python3
"""Preparing a disk is not a 45 second job.

`cluster create` stopped in its disk phase with:

    [ERROR] Host 10.10.102.43 failed disk claiming:
    Command timed out after 45 seconds

on a node whose work had in fact finished -- the volume group and the thin pool were there. 45
seconds is what spark-daemon's /api/v1/execute applies when a caller names no timeout, and
`run_remote_spark` had no way to name one: it sent only {"command": ...}. Every storage
preparation command went through that default. The claim zeroes a gigabyte at each end of the
disk and then builds a physical volume, a volume group and a thin pool; on this cluster's virtual
disks that sits right at the limit, so one node made it and the daemon killed the command on the
other two, leaving them with nothing.

Two properties, because fixing only the first leaves the failure available again:

  * a caller *can* name a timeout, and it reaches the daemon in the request body;
  * every storage-preparation call, in both the CLI and the daemon-side create, *does*.

Run with:  python -m unittest test_storage_prep_timeout
"""

import ast
import importlib.util
import io
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

PREP_SCRIPTS = {"CARVE_SIDON_VOLUME", "CLAIM_EXTRA_DISKS", "STAGE_SIDON_DISKS"}
FILES = ("cluster_new.py", "spark_daemon_decoded.py")


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def load(name):
    spec = importlib.util.spec_from_file_location(
        "under_test_" + name.replace(".py", ""), os.path.join(HERE, name))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeResponse(object):
    def __init__(self):
        pass

    def read(self):
        return json.dumps({"returncode": 0, "stdout": "", "stderr": ""}).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class ACallerCanNameATimeout(unittest.TestCase):
    def capture(self, module, **kwargs):
        seen = {}

        def fake_urlopen(request, context=None, timeout=None):
            seen["body"] = json.loads(request.data.decode("utf-8"))
            seen["wait"] = timeout
            return FakeResponse()

        class Context(object):
            """Stands in for the TLS context: the daemon's client verifies against a CA file
            that exists on a node and not on the machine running this test."""
            check_hostname = False
            verify_mode = None

            def load_cert_chain(self, *args, **kwargs):
                return None

        original_open = module.urllib.request.urlopen
        original_ctx = module.ssl.create_default_context
        module.urllib.request.urlopen = fake_urlopen
        if hasattr(module, "spark_client_material"):
            # A node has the CA; the machine running this test does not.
            original_material = module.spark_client_material
            module.spark_client_material = lambda: ("/ca", "/cert", "/key")
            self.addCleanup(setattr, module, "spark_client_material", original_material)
        module.ssl.create_default_context = lambda *a, **k: Context()
        self.addCleanup(setattr, module.urllib.request, "urlopen", original_open)
        self.addCleanup(setattr, module.ssl, "create_default_context", original_ctx)
        module.run_remote_spark("10.0.0.1", "true", **kwargs)
        return seen

    def test_the_timeout_reaches_the_daemon(self):
        for name in FILES:
            seen = self.capture(load(name), timeout=900)
            self.assertEqual(seen["body"].get("timeout"), 900,
                             "%s does not send the timeout, so the daemon applies 45s" % name)

    def test_the_client_waits_longer_than_the_command_may_run(self):
        """Otherwise the client gives up on a command the daemon is still allowed to run, and
        reports a timeout for work that is going fine."""
        for name in FILES:
            seen = self.capture(load(name), timeout=900)
            self.assertGreater(seen["wait"], 900)

    def test_a_caller_that_names_none_is_unchanged(self):
        for name in FILES:
            seen = self.capture(load(name))
            self.assertNotIn("timeout", seen["body"],
                             "%s now sends a timeout nobody asked for" % name)
            self.assertEqual(seen["wait"], 120)


def calls(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            yield node


def func_name(call):
    if isinstance(call.func, ast.Name):
        return call.func.id
    return None


def is_prep_argument(arg):
    """shell_script_command(CARVE_SIDON_VOLUME) and friends, or the claim's cmd_claim."""
    if isinstance(arg, ast.Call) and func_name(arg) == "shell_script_command":
        return any(isinstance(a, ast.Name) and a.id in PREP_SCRIPTS for a in arg.args)
    return isinstance(arg, ast.Name) and arg.id == "cmd_claim"


class EveryStoragePrepCallNamesOne(unittest.TestCase):
    def prep_calls(self, name):
        tree = ast.parse(read(name))
        found = []
        for call in calls(tree):
            if func_name(call) in ("run_parallel", "run_parallel_checked") \
                    and any(is_prep_argument(a) for a in call.args):
                found.append(call)
        return found

    def test_every_storage_prep_call_passes_the_timeout(self):
        for name in FILES:
            found = self.prep_calls(name)
            self.assertTrue(found, "%s: found no storage-prep call; has the shape changed?" % name)
            for call in found:
                keywords = {k.arg: k.value for k in call.keywords}
                self.assertIn(
                    "timeout", keywords,
                    "%s line %d runs a storage-preparation command with the 45 second default"
                    % (name, call.lineno))
                self.assertIsInstance(keywords["timeout"], ast.Name)
                self.assertEqual(keywords["timeout"].id, "STORAGE_PREP_TIMEOUT")

    def test_the_cli_covers_the_claim_and_all_three_preparation_steps(self):
        self.assertEqual(len(self.prep_calls("cluster_new.py")), 4)

    def test_the_daemon_side_create_covers_the_claim_and_the_three_preparation_steps(self):
        """Four, like the CLI. The first version of the fix covered three and missed the claim
        itself -- the call that actually timed out -- which is why this counts rather than
        trusting that a list of call sites is complete."""
        self.assertEqual(len(self.prep_calls("spark_daemon_decoded.py")), 4)


class TheLimitIsAGenerousCeiling(unittest.TestCase):
    def test_it_is_far_above_the_default_and_the_same_in_both_files(self):
        values = {name: getattr(load(name), "STORAGE_PREP_TIMEOUT") for name in FILES}
        self.assertEqual(len(set(values.values())), 1, values)
        for value in values.values():
            self.assertGreaterEqual(value, 300,
                                    "a timeout this short is the 45 second bug with a bigger number")


if __name__ == "__main__":
    unittest.main()
