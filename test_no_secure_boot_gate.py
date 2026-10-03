#!/usr/bin/env python3
"""Nothing in the toolkit may refuse a host because Secure Boot is on.

That gate existed for one reason: DRBD shipped as an out-of-tree kernel module (kmod-drbd9x)
which the kernel refuses to load unless the ELRepo signing key is enrolled, so a Secure Boot
host had no storage at all. Sidon is a userspace daemon speaking NBD over a unix socket and
loads no module, so the reason is gone.

It was removed from `provision.py` and from spark-daemon when DRBD went, and it was missed in
`cluster create`. So a cluster that had been provisioned happily was then refused at creation:

    [ERROR] Secure Boot is enabled on host 10.10.102.42 and the ELRepo Secure Boot key is not
    enrolled. Unsigned out-of-tree kernel modules will fail to load under Secure Boot.

on a node that loads no such module. That is the shape of bug this repository keeps finding --
one rule written in three files, retired from two of them -- and the property that catches it
is not "the three copies match" but "none of them exists".

Run with:  python -m unittest test_no_secure_boot_gate
"""

import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def toolkit_sources():
    """Every non-test Python file the deployment ships."""
    for name in sorted(os.listdir(HERE)):
        if name.endswith(".py") and not name.startswith("test_"):
            with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
                yield name, handle.read()


class SecureBootIsNotARequirement(unittest.TestCase):
    def test_nothing_shells_out_to_mokutil(self):
        """`mokutil` is how the old gate asked the firmware. Reading Secure Boot state for
        display is fine and goes through /sys; asking in order to refuse is not."""
        offenders = [name for name, source in toolkit_sources()
                     if re.search(r"\bmokutil\b", source)
                     and not source.lstrip().startswith("#")
                     and re.search(r"run_\w*\([^)]*mokutil", source)]
        self.assertEqual(
            offenders, [],
            "these still ask the firmware about Secure Boot to decide whether to proceed: %s"
            % ", ".join(offenders))

    def test_no_refusal_names_the_elrepo_key(self):
        for name, source in toolkit_sources():
            self.assertNotIn(
                "ELRepo Secure Boot key is not enrolled", source,
                "%s still refuses a host over a kernel-module signing key that nothing loads"
                % name)

    def test_cluster_create_does_not_exit_over_secure_boot(self):
        """The specific miss. `create` is a long function, so assert on the neighbourhood of
        the word rather than on the whole file: no `sys.exit` within a few lines of any
        mention of Secure Boot."""
        with io.open(os.path.join(HERE, "cluster_new.py"), encoding="utf-8") as handle:
            lines = handle.read().splitlines()
        for number, line in enumerate(lines):
            if "secure boot" in line.lower() and not line.strip().startswith("#"):
                window = "\n".join(lines[number:number + 6])
                self.assertNotIn(
                    "sys.exit", window,
                    "cluster_new.py line %d refuses on Secure Boot" % (number + 1))

    def test_the_removal_is_explained_where_it_happened(self):
        """A rule that vanishes silently gets re-added by the next person who notices the
        gap. Both places that dropped it say why."""
        for name in ("provision.py", "cluster_new.py"):
            with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
                source = handle.read()
            self.assertIn("no Secure Boot", source,
                          "%s dropped the gate without recording why" % name)

    def test_the_daemon_still_reports_it_without_acting_on_it(self):
        """Reporting is useful to an operator and harmless. Gating on it is the bug."""
        with io.open(os.path.join(HERE, "spark_daemon_decoded.py"), encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn('"secure_boot": secure_boot', source)
        self.assertIn("no longer gates anything", source)


if __name__ == "__main__":
    unittest.main()
