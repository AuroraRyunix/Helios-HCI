#!/usr/bin/env python3
"""run_argv returns (returncode, stdout, stderr); every caller must unpack three.

`read_host_capabilities` unpacked two, so GET /api/v1/host/capabilities raised ValueError and the
daemon dropped the connection without answering. Phoenix's new-VM page asks every host for it (to
decide whether to offer SPICE) and retries a closed socket with back-off, so the page took about six
seconds to load. Nothing noticed because the reader was only ever tested with a stub.

Run with:  python -m unittest test_run_argv_shape
"""

import ast
import importlib.util
import io
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(HERE, "spark_daemon_decoded.py")

DOMCAPS = """<domainCapabilities><devices>
<graphics supported='yes'><enum name='type'><value>sdl</value><value>vnc</value><value>spice</value></enum></graphics>
</devices></domainCapabilities>"""


def load():
    spec = importlib.util.spec_from_file_location("sd_run_argv", SOURCE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class EveryCallerUnpacksThreeValues(unittest.TestCase):
    def test_no_assignment_unpacks_run_argv_into_the_wrong_number_of_names(self):
        with io.open(SOURCE, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        wrong = []
        for node in ast.walk(tree):
            if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                    and getattr(node.value.func, "id", None) == "run_argv"
                    and isinstance(node.targets[0], ast.Tuple)
                    and len(node.targets[0].elts) != 3):
                wrong.append(node.lineno)
        self.assertEqual(wrong, [], "run_argv unpacked into the wrong number of names at lines %s" % wrong)


class CapabilitiesAreReadable(unittest.TestCase):
    def test_graphics_support_reads_a_real_shaped_answer(self):
        m = load()
        original = m.run_argv
        m.run_argv = lambda argv, timeout=45: (0, DOMCAPS, "")
        try:
            self.assertEqual(m.read_graphics_support(), ["vnc", "spice"])
        finally:
            m.run_argv = original

    def test_the_whole_capabilities_answer_is_built_without_raising(self):
        m = load()
        original = m.run_argv
        m.run_argv = lambda argv, timeout=45: (0, DOMCAPS, "")
        try:
            answer = m.read_host_capabilities()
        finally:
            m.run_argv = original
        self.assertIn("graphics", answer)
        self.assertEqual(answer["graphics"], ["vnc", "spice"])

    def test_a_failing_virsh_means_no_spice_not_an_exception(self):
        m = load()
        original = m.run_argv
        m.run_argv = lambda argv, timeout=45: (127, "", "virsh: command not found")
        try:
            self.assertEqual(m.read_graphics_support(), [])
        finally:
            m.run_argv = original


if __name__ == "__main__":
    unittest.main()
