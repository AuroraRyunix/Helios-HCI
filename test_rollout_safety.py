#!/usr/bin/env python3
"""What a rollout is allowed to do to a guest that is running.

On 2026-08-22 a VM died three seconds into a `deploy_updates.py` rollout, mid-install, and
nothing in the journal said why. The cause was spark-daemon's startup, which walked every
domain with `virsh list --all --name`, destroyed it and undefined it with `--nvram`, and
captured both streams rather than logging them.

The intent is sound -- a hypervisor is a stateless executor, so a definition it still
carries on startup is left over from before and should go -- but that reasoning is about a
host that just *booted*. spark-daemon is restarted on every rollout, and on a live host the
same command destroys the workloads the host is running, deletes their nvram, and leaves
`hydra.vms` saying Stopped, which nothing brings back.

Two guards, because either alone still leaves a hole:

  * spark-daemon clears only *inactive* definitions, so a restart cannot take a guest down.
  * `deploy_updates.py` refuses to restart the control plane under running guests unless
    told to. That specific defect is fixed, but the operation is still fourteen service
    restarts with no drain, and `hylia` drains a host for a reason.

Run with:  python -m unittest test_rollout_safety
"""

import ast
import io
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SPARK = os.path.join(HERE, "spark_daemon_decoded.py")
DEPLOY = os.path.join(HERE, "deploy_updates.py")
PROVISION = os.path.join(HERE, "provision.py")


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def load_functions(path, names, scope=None):
    """Compile named functions out of a module that does work at import."""
    tree = ast.parse(read(path), filename=path)
    body = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    missing = names - {n.name for n in body}
    assert not missing, "%s no longer defines %s" % (os.path.basename(path), missing)
    scope = dict(scope or {})
    exec(compile(ast.Module(body=body, type_ignores=[]), path, "exec"), scope)
    return scope


class FakeStdout:
    def __init__(self, text):
        self._text = text.encode("utf-8")

    def read(self):
        return self._text


class FakeSSH:
    def __init__(self, out="", raises=False):
        self.out = out
        self.raises = raises
        self.commands = []

    def exec_command(self, cmd):
        self.commands.append(cmd)
        if self.raises:
            raise OSError("channel closed")
        return None, FakeStdout(self.out), None


def strip_comments(text):
    """Code only. These assertions are about what the daemon runs, not how it is
    explained -- and the explanation necessarily quotes the command being replaced."""
    return "\n".join(l for l in text.splitlines() if not l.strip().startswith("#"))


class SparkStartupLeavesRunningGuestsAlone(unittest.TestCase):
    def setUp(self):
        source = read(SPARK)
        start = source.index("def check_cluster_and_autostart():")
        self.startup = source[start:source.index("\ndef ", start + 10)]
        self.code = strip_comments(self.startup)

    def test_startup_clears_only_inactive_definitions(self):
        self.assertIn("virsh list --inactive --name", self.code)
        self.assertNotIn(
            "virsh list --all --name", self.code,
            "spark-daemon's startup walks every domain again, so restarting it destroys "
            "whatever the host is running")

    def test_startup_never_destroys_a_domain(self):
        self.assertNotIn(
            "virsh destroy", self.code,
            "a daemon restart must not stop a guest; only an operator or a drain may")

    def test_running_guests_are_reported_rather_than_passed_over_in_silence(self):
        """The old command said nothing because both streams were captured. A restart that
        deliberately leaves guests up should say so, or the next person reads the same
        silence and assumes nothing happened."""
        self.assertIn("--state-running --name", self.code)
        self.assertIn("Leaving", self.code)


class AutostartNamesServicesThatExist(unittest.TestCase):
    """The autostart lists and the 30-second watchdog still named `aether`.

    That unit was removed with DRBD, so the watchdog spent every cycle running
    `systemctl start aether` and logging "Unit aether.service not found" -- forever, on
    every node. Worse than the noise: `sidon` was in none of the lists, so the daemon that
    actually serves guest disks was not among the services autostart brings up or the
    watchdog keeps up.
    """

    def setUp(self):
        self.source = read(SPARK)

    def test_no_service_list_names_the_removed_daemon(self):
        self.assertNotIn(
            '"aether"', self.source,
            "a service list still names aether, which no longer exists as a unit")

    def test_the_storage_daemon_is_in_the_lists_that_keep_services_up(self):
        for marker in ('["zookeeper", "hydra-db", "sidon"]',
                       '["hydra-db", "daruk", "sidon", "spectrum"'):
            self.assertIn(
                marker, self.source,
                "sidon is missing from a service list, so nothing autostarts or restarts "
                "the daemon that serves every guest disk")

    def test_the_volumes_path_is_left_alone(self):
        """The *directory* is still called aether and renaming it is a data migration,
        not a rename. Only the service names were wrong."""
        self.assertIn('AETHER_VOLUMES_ROOT = "/var/lib/hci/aether/volumes"', self.source)


class DeployRefusesToRunUnderGuests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        scope = load_functions(
            DEPLOY, {"running_guests", "refuse_if_guests_running"}, {"os": os})
        cls.refuse = staticmethod(scope["refuse_if_guests_running"])
        cls.listing = staticmethod(scope["running_guests"])

    def setUp(self):
        self._saved = os.environ.pop("HELIOS_ALLOW_RUNNING_GUESTS", None)

    def tearDown(self):
        os.environ.pop("HELIOS_ALLOW_RUNNING_GUESTS", None)
        if self._saved is not None:
            os.environ["HELIOS_ALLOW_RUNNING_GUESTS"] = self._saved

    def test_an_idle_host_is_deployed_to(self):
        self.assertTrue(self.refuse("10.0.0.1", FakeSSH("")))

    def test_a_host_with_a_running_guest_is_refused(self):
        self.assertFalse(
            self.refuse("10.0.0.1", FakeSSH("test\n")),
            "the rollout proceeds under a running guest with no way for the operator to "
            "have known")

    def test_the_override_is_explicit_and_works(self):
        os.environ["HELIOS_ALLOW_RUNNING_GUESTS"] = "1"
        self.assertTrue(self.refuse("10.0.0.1", FakeSSH("test\nother\n")))

    def test_only_the_exact_override_value_counts(self):
        for value in ("0", "", "yes", "true", "TRUE"):
            os.environ["HELIOS_ALLOW_RUNNING_GUESTS"] = value
            self.assertFalse(
                self.refuse("10.0.0.1", FakeSSH("test\n")),
                "%r was accepted as permission to roll out under running guests" % value)

    def test_a_host_that_cannot_answer_is_not_treated_as_busy(self):
        """A cluster with no hypervisor must still be deployable."""
        self.assertEqual(self.listing(FakeSSH(raises=True)), [])
        self.assertTrue(self.refuse("10.0.0.1", FakeSSH(raises=True)))

    def test_the_check_runs_before_anything_is_uploaded_or_restarted(self):
        source = read(DEPLOY)
        body = source[source.index("def deploy_to_node(ip):"):]
        check = body.index("refuse_if_guests_running(ip, ssh)")
        for later in ("put_text_file(sftp", "systemctl restart spark-daemon"):
            self.assertLess(
                check, body.index(later),
                "the guest check happens after %r, so a refused rollout has already "
                "changed the node" % later)


class NvramIsLabelledForSelinux(unittest.TestCase):
    """virtqemud must be able to remove a guest's UEFI variables.

    The directory sits outside libvirt's tree and inherits var_lib_t, so on an Enforcing
    host the nvram cleanup in a VM delete is denied -- after the domain is already gone.
    The test node runs Permissive, where it appears only as a journal denial and everything
    looks fine, which is why this belongs in a test rather than in someone's memory.
    """

    def nvram_block(self, source):
        start = source.index('NVRAM_SELINUX = """')
        end = source.index('"""', start + len('NVRAM_SELINUX = """'))
        return source[start:end]

    def test_both_deployment_paths_label_the_directory(self):
        for path in (DEPLOY, PROVISION):
            source = read(path)
            name = os.path.basename(path)
            self.assertIn("NVRAM_SELINUX", source, name)
            self.assertIn("qemu_var_run_t", source, name)
            self.assertIn("/var/lib/hci/aether/nvram", source, name)
            self.assertIn("restorecon", source, name)

    def test_it_survives_a_node_that_is_already_labelled(self):
        """`semanage fcontext -a` fails when the rule exists; a rollout must not."""
        for path in (DEPLOY, PROVISION):
            block = self.nvram_block(read(path))
            self.assertIn("-m -t qemu_var_run_t", block, os.path.basename(path))

    def test_it_is_skipped_where_semanage_does_not_exist(self):
        for path in (DEPLOY, PROVISION):
            block = self.nvram_block(read(path))
            self.assertIn("command -v semanage", block, os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
