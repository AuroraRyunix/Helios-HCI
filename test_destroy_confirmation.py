#!/usr/bin/env python3
"""`cluster destroy` has to ask before it erases a cluster.

It did not. The command stopped every VM, wiped the LVM pool and the disk signatures, deleted
the ZooKeeper and Hydra data, /etc/hci/cluster.json and the sidon store, on every host
named -- starting the instant it was run, with no prompt, because nothing had ever put one
there. The one command that cannot be undone was easier to run by accident than `rm -r`.

Three properties are asserted, because fixing only the first leaves the failure available:

  * **nothing happens before the answer** -- the prompt sits ahead of the cluster lock and
    of every phase, since a confirmation that runs after the first phase confirms nothing;
  * **only the exact word proceeds** -- a bare "y" is what a finger does when it is
    expecting a different question;
  * **nobody answering is not a yes** -- a pipe, a cron job or a closed stdin that reaches
    the prompt has not been asked, and silence must never read as consent.

Run with:  python -m unittest test_destroy_confirmation
"""

import importlib.util
import io
import os
import re
import sys
import unittest
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))


def load_cluster():
    spec = importlib.util.spec_from_file_location(
        "cluster_new_under_test", os.path.join(HERE, "cluster_new.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def ask(cluster, answer=None, **kwargs):
    """Run the prompt with a scripted answer; return (proceeds, printed_text)."""
    def read(_prompt):
        if isinstance(answer, BaseException):
            raise answer
        return answer

    out = io.StringIO()
    with redirect_stdout(out):
        result = cluster.confirm_destroy(["10.0.0.1", "10.0.0.2"], read=read, **kwargs)
    return result, out.getvalue()


class OnlyTheExactWordProceeds(unittest.TestCase):
    def setUp(self):
        self.cluster = load_cluster()

    def test_typing_destroy_proceeds(self):
        proceeds, _ = ask(self.cluster, "destroy", interactive=True)
        self.assertTrue(proceeds)

    def test_surrounding_whitespace_is_forgiven(self):
        proceeds, _ = ask(self.cluster, "  destroy \n", interactive=True)
        self.assertTrue(proceeds)

    def test_a_bare_yes_does_not(self):
        """y / yes / Y is the answer to a different, milder question."""
        for answer in ("y", "Y", "yes", "YES", "ok", "true", "1"):
            proceeds, _ = ask(self.cluster, answer, interactive=True)
            self.assertFalse(proceeds, "%r was accepted as confirmation" % answer)

    def test_the_wrong_case_or_a_near_miss_does_not(self):
        for answer in ("Destroy", "DESTROY", "destroy it", "destory", "", " "):
            proceeds, _ = ask(self.cluster, answer, interactive=True)
            self.assertFalse(proceeds, "%r was accepted as confirmation" % answer)

    def test_the_host_list_is_not_the_password(self):
        """Typing the hosts back would make the prompt a longer way of saying --yes."""
        proceeds, _ = ask(self.cluster, "10.0.0.1,10.0.0.2", interactive=True)
        self.assertFalse(proceeds)

    def test_a_declined_prompt_says_nothing_was_changed(self):
        _, text = ask(self.cluster, "no", interactive=True)
        self.assertIn("Nothing was changed", text)


class NobodyAnsweringIsNotAYes(unittest.TestCase):
    def setUp(self):
        self.cluster = load_cluster()

    def test_a_non_interactive_run_is_refused(self):
        """A pipe or a cron job that reaches this line has not been asked."""
        proceeds, text = ask(self.cluster, "destroy", interactive=False)
        self.assertFalse(proceeds, "a non-interactive stdin was allowed to destroy a cluster")
        self.assertIn("--yes", text, "the refusal does not say how a script opts in")

    def test_a_closed_stdin_is_a_refusal(self):
        proceeds, _ = ask(self.cluster, EOFError(), interactive=True)
        self.assertFalse(proceeds)

    def test_ctrl_c_at_the_prompt_is_a_refusal(self):
        proceeds, _ = ask(self.cluster, KeyboardInterrupt(), interactive=True)
        self.assertFalse(proceeds)

    def test_yes_is_the_only_way_past_it_without_a_terminal(self):
        proceeds, text = ask(self.cluster, None, assume_yes=True, interactive=False)
        self.assertTrue(proceeds)
        self.assertIn("--yes", text, "skipping the prompt should be said out loud")

    def test_the_prompt_names_what_it_is_about_to_do(self):
        _, text = ask(self.cluster, "no", interactive=True)
        for host in ("10.0.0.1", "10.0.0.2"):
            self.assertIn(host, text)
        for consequence in ("VM", "LVM", "ZooKeeper", "cluster.json"):
            self.assertIn(consequence, text,
                          "the prompt does not mention %s, so it is asking about nothing"
                          % consequence)


class TheCommandActuallyAsks(unittest.TestCase):
    """The function is worthless if main() does not call it first."""

    def setUp(self):
        with io.open(os.path.join(HERE, "cluster_new.py"), encoding="utf-8") as handle:
            source = handle.read()
        start = source.index('elif args.command == "destroy":')
        end = source.index("\n    elif args.command", start + 10) \
            if "\n    elif args.command" in source[start + 10:] else len(source)
        self.block = source[start:end]

    def test_the_prompt_comes_before_the_lock_and_every_phase(self):
        ask_at = self.block.index("confirm_destroy(")
        for later in ("acquire_cluster_lock(", "Phase 1", "run_remote_spark("):
            self.assertIn(later, self.block)
            self.assertLess(
                ask_at, self.block.index(later),
                "destroy touches %s before it has asked" % later)

    def test_a_refusal_exits_non_zero(self):
        match = re.search(r"if not confirm_destroy\([^)]*\):\s*\n\s*sys\.exit\((\d+)\)",
                          self.block)
        self.assertTrue(match, "a declined destroy does not stop the command")
        self.assertNotEqual(match.group(1), "0",
                            "declining exits 0, which a script reads as success")

    def test_the_yes_flag_is_wired_to_the_prompt(self):
        self.assertIn("assume_yes=args.yes", self.block)

    def test_the_flag_exists(self):
        with io.open(os.path.join(HERE, "cluster_new.py"), encoding="utf-8") as handle:
            self.assertIn('"--yes"', handle.read())


if __name__ == "__main__":
    unittest.main()
