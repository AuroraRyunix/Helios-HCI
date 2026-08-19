#!/usr/bin/env python3
"""Tests for spark-daemon's request validation.

The daemon runs what it is given as root, so its validators are the boundary between a
web request and a shell. These cover the ones an image upload added, plus the volume
sizing they share with VM disk creation -- the place where a unit mix-up allocates a
volume a million times too large or too small.

Run with:  python -m unittest test_spark_daemon
"""

import importlib.util
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_daemon():
    """Import spark_daemon_decoded.py by path.

    It is not a package and its filename is not an identifier, so a plain import will not
    reach it. Everything at module level is definitions; the server only starts under
    `if __name__ == "__main__"`.
    """
    spec = importlib.util.spec_from_file_location(
        "spark_daemon_under_test", os.path.join(HERE, "spark_daemon_decoded.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


daemon = load_daemon()


class VolumeSizeTests(unittest.TestCase):
    """`validate_volume_size_kib` resolves a size from either unit."""

    def test_gib_converts_to_kib(self):
        size, error = daemon.validate_volume_size_kib({"size_gib": 2})
        self.assertIsNone(error)
        self.assertEqual(size, 2 * daemon.KIB_PER_GIB)

    def test_kib_is_taken_as_given_when_already_aligned(self):
        size, error = daemon.validate_volume_size_kib({"size_kib": 8192})
        self.assertIsNone(error)
        self.assertEqual(size, 8192)

    def test_kib_is_rounded_up_to_drbd_alignment(self):
        # LINSTOR aligns to 4 KiB itself. Without rounding here, the resource comes back
        # larger than the request, and the *next* idempotent create compares its own
        # unaligned request against the aligned reality and rejects the retry as a size
        # mismatch -- which is a failed upload that looks like a conflict.
        for requested, expected in ((1, 4), (5, 8), (4095, 4096), (4097, 4100)):
            size, error = daemon.validate_volume_size_kib({"size_kib": requested})
            self.assertIsNone(error, requested)
            self.assertEqual(size, expected, requested)

    def test_both_units_is_an_error_rather_than_a_precedence_rule(self):
        size, error = daemon.validate_volume_size_kib({"size_gib": 1, "size_kib": 1024})
        self.assertIsNone(size)
        self.assertIn("not both", error)

    def test_neither_unit_is_rejected(self):
        size, error = daemon.validate_volume_size_kib({})
        self.assertIsNone(size)
        self.assertIn("size_gib", error)

    def test_booleans_are_not_integers_here(self):
        # isinstance(True, int) is True in Python, so without an explicit check `True`
        # would be accepted and formatted as a 1 KiB volume.
        for payload in ({"size_kib": True}, {"size_gib": True}):
            size, error = daemon.validate_volume_size_kib(payload)
            self.assertIsNone(size, payload)
            self.assertIsNotNone(error, payload)

    def test_non_integers_are_rejected(self):
        for value in ("1024", 1024.5, None, [], {}):
            size, error = daemon.validate_volume_size_kib({"size_kib": value})
            self.assertIsNone(size, value)
            self.assertIsNotNone(error, value)

    def test_bounds(self):
        size, error = daemon.validate_volume_size_kib({"size_kib": 0})
        self.assertIsNone(size)
        self.assertIn("greater than zero", error)

        too_big = daemon.MAX_VOLUME_GIB * daemon.KIB_PER_GIB + 1
        size, error = daemon.validate_volume_size_kib({"size_kib": too_big})
        self.assertIsNone(size)
        self.assertIn("exceed", error)

    def test_gib_bounds_still_apply(self):
        size, error = daemon.validate_volume_size_kib({"size_gib": 0})
        self.assertIsNone(size)
        self.assertIn("between", error)


class FlagTests(unittest.TestCase):
    """`validate_flag` is strict on purpose.

    `allow_two_primaries` is the option that let one VM run on two hosts and corrupt its
    own disk. A truthy string must not be able to turn it on.
    """

    def test_absent_is_false(self):
        value, error = daemon.validate_flag(None, "allow_two_primaries")
        self.assertIsNone(error)
        self.assertIs(value, False)

    def test_booleans_pass_through(self):
        self.assertEqual(daemon.validate_flag(True, "f"), (True, None))
        self.assertEqual(daemon.validate_flag(False, "f"), (False, None))

    def test_truthy_non_booleans_are_refused(self):
        for value in ("yes", "true", 1, "1", [1], {"a": 1}):
            flag, error = daemon.validate_flag(value, "allow_two_primaries")
            self.assertIsNone(flag, value)
            self.assertIn("allow_two_primaries", error)


class DrbdOptionTests(unittest.TestCase):
    def test_split_brain_options_do_not_include_dual_primary(self):
        # The default path must never set it: these options are applied to every VM disk.
        self.assertNotIn("--allow-two-primaries", daemon.DRBD_SPLIT_BRAIN_OPTIONS)

    def test_split_brain_options_are_flag_value_pairs(self):
        # They are spliced into an argv list, so an odd length means a value is being
        # passed as a flag or the resource name is being consumed as one.
        self.assertEqual(len(daemon.DRBD_SPLIT_BRAIN_OPTIONS) % 2, 0)


class PathValidationTests(unittest.TestCase):
    """The device an upload writes to is taken from a query string."""

    def test_traversal_and_control_characters_are_refused(self):
        for candidate in ("/dev/drbd/../../etc/shadow", "/dev/drbd/by-res/a\0b", "", None):
            ok, _error = daemon.validate_path(candidate)
            self.assertFalse(ok, candidate)


if __name__ == "__main__":
    unittest.main()
