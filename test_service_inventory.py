#!/usr/bin/env python3
"""Every service spark-daemon reports must be one the node actually runs.

`vali.select_best_start_host()` skips any host where **all** services are not `UP`. So a
name in spark-daemon's service list that can never come up is not a cosmetic wart: it
means no host is ever eligible and no VM can ever be started.

That happened. Removing DRBD took the `aether` unit with it and left `"aether": "Aether"`
in the inventory, so every node reported `Aether: DOWN` forever. `valcli vm.create`
worked, `valcli vm.on` refused with **"No active hypervisor host has sufficient
memory"** -- on a host with 9 GB free -- because the loop `continue`s past an ineligible
host and the caller's only message is about memory. The symptom named the wrong
subsystem entirely.

So this compares the inventory against the units the deployment toolkit actually
installs, in both directions.

Run with:  python -m unittest test_service_inventory
"""

import ast
import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
DAEMON = os.path.join(HERE, "spark_daemon_decoded.py")
PROVISION = os.path.join(HERE, "provision.py")
DEPLOY = os.path.join(HERE, "deploy_updates.py")

# The `services = [...]` list inside read_service_status(), and the display list.
SERVICES_RE = re.compile(r'services = \[\s*"zookeeper".*?\]', re.S)
SVC_LIST_RE = re.compile(r'svc_list = \[\s*"ZooKeeper".*?\]', re.S)

# Units that are not systemd services on the host at all, or are not deployed by the
# toolkit under that name. Each needs a reason, because an unexplained exception is how
# the next stale entry gets waved through.
NOT_A_HOST_UNIT = {
    # Reported for the operator's benefit; it is a container the console tier manages.
    "urbosa",
}


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def reported_units():
    """The systemd unit names spark-daemon reports on."""
    source = read(DAEMON)
    match = SERVICES_RE.search(source)
    assert match, "spark_daemon_decoded.py no longer has the services list"
    return set(ast.literal_eval(match.group(0).split("=", 1)[1].strip()))


def reported_labels():
    """The display names `spark cluster status` prints."""
    source = read(DAEMON)
    match = SVC_LIST_RE.search(source)
    assert match, "spark_daemon_decoded.py no longer has svc_list"
    return set(ast.literal_eval(match.group(0).split("=", 1)[1].strip()))


def svc_map():
    """unit -> label, as the daemon maps them."""
    source = read(DAEMON)
    start = source.index("svc_map = {")
    end = source.index("}", start) + 1
    return ast.literal_eval(source[start:end].split("=", 1)[1].strip())


class ServiceInventoryTests(unittest.TestCase):
    def test_every_reported_unit_has_a_label(self):
        missing = sorted(reported_units() - set(svc_map()))
        self.assertEqual(
            missing, [],
            f"these units are reported but svc_map gives them no display name: {missing}")

    def test_the_labels_printed_match_the_labels_mapped(self):
        """`svc_list` and `svc_map`'s values are two hand-written copies of one list. A
        name in one and not the other prints as permanently DOWN or never prints at
        all."""
        mapped = set(svc_map().values())
        printed = reported_labels()
        self.assertEqual(
            sorted(printed - mapped), [],
            f"printed but never reported, so always DOWN: {sorted(printed - mapped)}")
        self.assertEqual(
            sorted(mapped - printed), [],
            f"reported but never printed: {sorted(mapped - printed)}")

    def test_no_reported_unit_is_one_the_toolkit_removes(self):
        """The failure this file exists for. A unit the deployment toolkit deletes can
        never be UP, and one that can never be UP makes every host ineligible for VM
        placement."""
        toolkit = read(PROVISION) + read(DEPLOY)
        # Names the toolkit explicitly stops, removes or unlinks.
        removed = set()
        for name in ("aether", "linstor-controller", "linstor-satellite", "odin", "spark"):
            if re.search(r"(rm -f [^\n]*%s\.container|systemctl stop [^\n]*\b%s\b)" % (name, name),
                         toolkit):
                removed.add(name)
        offenders = sorted(reported_units() & removed)
        self.assertEqual(
            offenders,
            [],
            "spark-daemon reports these as services while the deployment toolkit removes "
            f"them, so they are permanently DOWN and no host is ever eligible to run a "
            f"VM: {offenders}")

    def test_sidon_is_in_the_inventory(self):
        """Named explicitly. It is the storage data path: a node whose sidon is down can
        neither attach a disk nor serve one, and placing a VM there would fail at the
        attach instead of at the gate."""
        self.assertIn("sidon", reported_units())
        self.assertIn("Sidon", reported_labels())

    def test_aether_is_not(self):
        self.assertNotIn("aether", reported_units())
        self.assertNotIn("Aether", reported_labels())


if __name__ == "__main__":
    unittest.main()
