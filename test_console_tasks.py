#!/usr/bin/env python3
"""The five console controls that are now Catalyst tasks, and what has to hold for them.

Starting an upgrade, loading a package, deploying and destroying Kubernetes, and building
or tearing down the overlay were the five things the rebuilt console deliberately did not
offer. Each of them is long, cluster-wide and able to fail, and the reason they were left
out is that a button which returns instantly and leaves the work happening somewhere is
worse than no button: an operator has nothing to watch and nothing to read when it goes
wrong.

They are tasks now, and these are the properties that make that true rather than nominal:

  * a service name is a queue, and a queue with no worker draining it produces a task that
    is written, listed, and never run -- which on the console reads as "slow";
  * dagur runs the command, and spark-daemon kills a command at 45 seconds when the caller
    does not say otherwise, so a task type for cluster-wide work has to say otherwise;
  * a command that reports its own progress must not be fighting a ticker that invents it;
  * the package staging endpoint may write exactly one path, and must not leave a
    truncated archive where the loader will find one;
  * a worker that raises must end its task, or the task stays `processing` forever.

Run with:  python -m unittest test_console_tasks
"""

import ast
import importlib.util
import io
import json
import os
import re
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def read(path):
    with io.open(os.path.join(HERE, path), encoding="utf-8") as handle:
        return handle.read()


def load_module(name, filename):
    """Import one of the daemons by path. Everything at module level is definitions."""
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def extract_function(source, name, namespace):
    """Compile one top-level function out of a module that cannot be imported.

    `spectrum_server.py` opens a database and binds a socket to be useful, so its loops
    are exercised the way `test_thread_supervision` exercises the supervisor: by lifting
    the function out and giving it the globals it expects.
    """
    tree = ast.parse(source)
    node = next((n for n in tree.body
                 if isinstance(n, ast.FunctionDef) and n.name == name), None)
    assert node is not None, "%s() not found" % name
    exec(compile(ast.Module(body=[node], type_ignores=[]), "<%s>" % name, "exec"), namespace)
    return namespace[name]


def extract_method(source, class_name, method_name, namespace):
    """The same, for one method of a handler class."""
    tree = ast.parse(source)
    cls = next(n for n in tree.body
               if isinstance(n, ast.ClassDef) and n.name == class_name)
    node = next(n for n in cls.body
                if isinstance(n, ast.FunctionDef) and n.name == method_name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), "<%s>" % method_name, "exec"),
         namespace)
    return namespace[method_name]


hylia = load_module("hylia_under_test", "hylia.py")
dagur = load_module("dagur_under_test", "dagur.py")


class QueueAndWorkerTests(unittest.TestCase):
    """Every service the console submits to must be drained by something.

    Catalyst's queues are `queue.Queue` objects inside the leader's process. Submitting to
    a name that exists in that dict and has no worker long-polling it writes a row, returns
    a task id, and then nothing happens for ever -- and `pending` on the task ring is
    indistinguishable from a task that is merely slow. Submitting to a name that is not in
    the dict at all is a 404 the caller can at least report.

    So the three lists -- the queues Catalyst holds, the queues daemons drain, and the
    services the Phoenix tier will submit to -- are one statement in three files.
    """

    def setUp(self):
        self.catalyst_source = read("catalyst.py")

    def catalyst_queues(self):
        tree = ast.parse(self.catalyst_source)
        for node in ast.walk(tree):
            if (isinstance(node, ast.Assign)
                    and any(getattr(t, "id", None) == "queues" for t in node.targets)
                    and isinstance(node.value, ast.Dict)):
                return {key.value for key in node.value.keys}
        self.fail("catalyst.py has no `queues` dict")

    def drained_queues(self):
        drained = {}
        for filename in ("vali.py", "dagur.py", "spectrum_server.py"):
            for name in re.findall(r"/api/v1/queues/([a-z_]+)", read(filename)):
                drained.setdefault(name, set()).add(filename)
        return drained

    def phoenix_services(self):
        source = read(os.path.join("spectrum_phx", "lib", "spectrum_phx", "catalyst.ex"))
        match = re.search(r"@services\s+~w\(([^)]*)\)", source)
        self.assertIsNotNone(match, "SpectrumPhx.Catalyst does not declare @services")
        return set(match.group(1).split())

    def test_every_queue_the_console_submits_to_exists_in_catalyst(self):
        missing = self.phoenix_services() - self.catalyst_queues()
        self.assertEqual(missing, set(),
                         "the console would submit to a queue Catalyst does not hold: %r"
                         % (missing,))

    def test_every_queue_the_console_submits_to_has_a_worker(self):
        # The one that matters. A queue in the dict with nobody draining it is a task that
        # is accepted and never runs, and the console cannot tell that from a slow one.
        undrained = self.phoenix_services() - set(self.drained_queues())
        self.assertEqual(undrained, set(),
                         "no daemon long-polls these queues: %r" % (undrained,))

    def test_the_kubernetes_queue_is_drained_by_the_tier_that_owns_the_code(self):
        # `lanayru.py`'s workers import run_cql_query, run_lwt, sidon_call and the log
        # buffer from spectrum_server. Draining its queue anywhere else means moving all
        # of that first, and a worker that cannot import its own workers is a queue with
        # no worker by another name.
        self.assertIn("spectrum_server.py", self.drained_queues().get("lanayru", set()))

    def test_a_worker_drains_no_queue_catalyst_does_not_hold(self):
        unknown = set(self.drained_queues()) - self.catalyst_queues()
        self.assertEqual(unknown, set(),
                         "a daemon polls a queue that does not exist: %r" % (unknown,))


class DagurCommandTests(unittest.TestCase):
    """The `dagur`/`execute` task type, which is what four of the five controls submit."""

    def test_a_command_is_given_a_timeout_rather_than_the_daemon_default(self):
        # spark-daemon reads `timeout` from the request and applies 45 seconds when it is
        # absent, so a job that omitted it was capped at forty-five seconds of work and
        # came back as "Command timed out" from a daemon the caller never mentioned. Every
        # cluster-wide operation the console now submits runs longer than that.
        sent = {}

        class FakeResponse:
            status = 200

            def read(self):
                return json.dumps({"returncode": 0, "stdout": "", "stderr": ""}).encode()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        def fake_urlopen(request, context=None, timeout=None):
            sent["body"] = json.loads(request.data.decode("utf-8"))
            sent["socket_timeout"] = timeout
            return FakeResponse()

        original_urlopen = dagur.urllib.request.urlopen
        original_ssl = dagur.ssl
        dagur.urllib.request.urlopen = fake_urlopen
        # The cluster CA is not on a development machine, and this test is about the
        # request rather than about the transport that carries it.
        dagur.ssl = type("ssl", (), {
            "Purpose": dagur.ssl.Purpose,
            "create_default_context": staticmethod(
                lambda purpose, cafile=None: type("ctx", (), {
                    "load_cert_chain": lambda self, certfile=None, keyfile=None: None,
                    "check_hostname": True,
                })()),
        })
        try:
            dagur.run_remote_spark("127.0.0.1", "sleep 200", timeout=900)
        finally:
            dagur.urllib.request.urlopen = original_urlopen
            dagur.ssl = original_ssl

        self.assertEqual(sent["body"]["timeout"], 900)
        # And the socket must outlive the command it is waiting for, or a job that runs to
        # its limit is reported as a transport failure instead of as its own exit code.
        self.assertGreater(sent["socket_timeout"], 900)

    def test_the_default_timeout_is_not_a_control_plane_number(self):
        self.assertGreaterEqual(dagur.DEFAULT_JOB_TIMEOUT, 600)

    def _run_job(self, payload_timeout=None, reports_progress=False):
        updates = []
        commands = []

        original_cql = dagur.run_cql_query
        original_call = dagur.call_catalyst_api
        original_spark = dagur.run_remote_spark
        original_insert = dagur.insert_dagur_run

        dagur.run_cql_query = lambda *a, **k: (0, "", "")
        dagur.insert_dagur_run = lambda *a, **k: None
        dagur.call_catalyst_api = lambda path, payload=None, method="GET": (
            updates.append(payload) or (200, {}))

        def fake_spark(ip, command, timeout=None):
            commands.append((command, timeout))
            return 0, "done", ""

        dagur.run_remote_spark = fake_spark
        try:
            kwargs = {"reports_progress": reports_progress}
            if payload_timeout is not None:
                kwargs["timeout"] = payload_timeout
            dagur.execute_dagur_job_thread("task-1", "urbosa_bootstrap", "true", **kwargs)
        finally:
            dagur.run_cql_query = original_cql
            dagur.call_catalyst_api = original_call
            dagur.run_remote_spark = original_spark
            dagur.insert_dagur_run = original_insert

        return updates, commands

    def test_the_task_timeout_reaches_the_command(self):
        _, commands = self._run_job(payload_timeout=1234)
        self.assertEqual(commands[0][1], 1234)

    def test_the_task_id_is_in_the_commands_environment(self):
        # It is how a command that knows where it has got to can say so. `hylia
        # --start-upgrade` reports the nodes it has watched finish; without this it would
        # have no way to name the task it is running as.
        _, commands = self._run_job()
        self.assertTrue(commands[0][0].startswith("CATALYST_TASK_ID=task-1 "))

    def test_a_command_that_reports_progress_is_not_also_guessed_at(self):
        # The ticker climbs to 95 in ten seconds and stays there. Left running beside a
        # command that writes real progress, it overwrites a true 20% with an invented 95%
        # a second later, and the bar an operator is watching becomes a lie that moves.
        updates, _ = self._run_job(reports_progress=True)
        progresses = [u.get("progress") for u in updates]
        self.assertEqual(progresses, [5, 100],
                         "the ticker ran for a task that reports its own progress")

    def test_a_command_that_says_nothing_still_gets_a_moving_bar(self):
        updates, _ = self._run_job(reports_progress=False)
        self.assertEqual(updates[0]["progress"], 5)
        self.assertEqual(updates[-1]["progress"], 100)

    def test_a_failing_command_ends_the_task_as_failed_with_its_output(self):
        original_cql = dagur.run_cql_query
        original_call = dagur.call_catalyst_api
        original_spark = dagur.run_remote_spark
        original_insert = dagur.insert_dagur_run
        updates = []

        dagur.run_cql_query = lambda *a, **k: (0, "", "")
        dagur.insert_dagur_run = lambda *a, **k: None
        dagur.call_catalyst_api = lambda path, payload=None, method="GET": (
            updates.append(payload) or (200, {}))
        dagur.run_remote_spark = lambda ip, command, timeout=None: (1, "", "no such host")
        try:
            dagur.execute_dagur_job_thread("task-2", "urbosa_cleanup", "false",
                                           reports_progress=True)
        finally:
            dagur.run_cql_query = original_cql
            dagur.call_catalyst_api = original_call
            dagur.run_remote_spark = original_spark
            dagur.insert_dagur_run = original_insert

        self.assertEqual(updates[-1]["status"], "failed")
        self.assertIn("no such host", updates[-1]["error_msg"])


class HyliaEntryPointTests(unittest.TestCase):
    """`hylia --load-package` and `hylia --start-upgrade`, the two LCM task commands."""

    def test_no_arguments_is_still_the_daemon(self):
        # hylia.service runs /usr/local/bin/hylia with no arguments and has to keep
        # meaning what it meant; a CLI that changed the no-argument case would stop every
        # rolling upgrade in the fleet at the next deploy.
        ran = []
        original = hylia.hylia_loop
        hylia.hylia_loop = lambda: ran.append(True)
        try:
            self.assertEqual(hylia.main(["/usr/local/bin/hylia"]), 0)
        finally:
            hylia.hylia_loop = original
        self.assertEqual(ran, [True])

    def test_an_unknown_option_does_not_fall_through_to_the_daemon(self):
        # A typo in a task's command must not start a second upgrade daemon on the leader.
        ran = []
        original = hylia.hylia_loop
        hylia.hylia_loop = lambda: ran.append(True)
        try:
            self.assertEqual(hylia.main(["hylia", "--upgrade-now"]), 2)
        finally:
            hylia.hylia_loop = original
        self.assertEqual(ran, [])

    def test_a_subcommand_without_its_argument_is_refused(self):
        self.assertEqual(hylia.main(["hylia", "--start-upgrade"]), 2)
        self.assertEqual(hylia.main(["hylia", "--load-package"]), 2)

    def test_a_job_id_that_is_not_a_job_id_never_reaches_a_statement(self):
        # The id is interpolated into CQL and the value arrives from a web tier. Refusing
        # it before the first statement is the difference between a rejected argument and
        # an injected one.
        statements = []
        original = hylia.run_cql_query
        hylia.run_cql_query = lambda cql: statements.append(cql) or (0, "", "")
        try:
            self.assertEqual(hylia.start_upgrade("'; DROP KEYSPACE hydra; --"), 1)
        finally:
            hylia.run_cql_query = original
        self.assertEqual(statements, [])

    def test_loading_a_package_that_is_not_there_truncates_nothing(self):
        # `load_package` clears hylia_jobs before writing the new one. Reaching that on a
        # missing or invalid archive would destroy the record of the package that *is*
        # loaded in exchange for nothing.
        statements = []
        original = hylia.run_cql_query
        hylia.run_cql_query = lambda cql: statements.append(cql) or (0, "", "")
        try:
            self.assertEqual(hylia.load_package("/nonexistent/helios_update.zip"), 1)
        finally:
            hylia.run_cql_query = original
        self.assertEqual(statements, [])

    def test_a_package_that_fails_validation_truncates_nothing(self):
        statements = []
        original_cql = hylia.run_cql_query
        original_validate = hylia.validate_and_extract_zip
        hylia.run_cql_query = lambda cql: statements.append(cql) or (0, "", "")

        def refuse(zip_path, extract_dir):
            raise Exception("signature verification failed")

        hylia.validate_and_extract_zip = refuse
        handle, path = tempfile.mkstemp(suffix=".zip")
        os.close(handle)
        try:
            self.assertEqual(hylia.load_package(path), 1)
        finally:
            hylia.run_cql_query = original_cql
            hylia.validate_and_extract_zip = original_validate
            os.remove(path)
        self.assertEqual(statements, [])

    def test_progress_counts_nodes_finished_and_nothing_else(self):
        nodes = ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
        self.assertEqual(hylia.upgrade_percent(
            {"target_nodes": nodes, "current_node": "10.0.0.1"}), 0)
        self.assertEqual(hylia.upgrade_percent(
            {"target_nodes": nodes, "current_node": "10.0.0.3"}), 66)

    def test_a_node_outside_the_target_list_contributes_nothing(self):
        # "We do not know where it is" and "it has just begun" are different states, and
        # only one of them should be drawn as a bar that is about to move.
        self.assertEqual(hylia.upgrade_percent(
            {"target_nodes": ["10.0.0.1"], "current_node": "10.0.0.9"}), 0)

    def test_an_upgrade_with_no_targets_does_not_divide_by_zero(self):
        self.assertEqual(hylia.upgrade_percent({"target_nodes": [], "current_node": None}), 0)

    def test_progress_is_not_reported_when_the_command_is_not_a_task(self):
        # Run by hand from a shell there is no task to report against, and inventing one
        # would put a row in the console's task log that no operator asked for.
        calls = []
        original_env = os.environ.pop("CATALYST_TASK_ID", None)
        original_call = hylia.call_catalyst_api
        hylia.call_catalyst_api = lambda *a, **k: calls.append(a) or (200, {})
        try:
            hylia.report_task_progress(50)
        finally:
            hylia.call_catalyst_api = original_call
            if original_env is not None:
                os.environ["CATALYST_TASK_ID"] = original_env
        self.assertEqual(calls, [])

    def test_progress_is_reported_against_the_task_dagur_named(self):
        calls = []
        original_call = hylia.call_catalyst_api
        hylia.call_catalyst_api = lambda path, payload=None, method="GET": (
            calls.append((path, payload)) or (200, {}))
        os.environ["CATALYST_TASK_ID"] = "abc-123"
        try:
            hylia.report_task_progress(42)
        finally:
            hylia.call_catalyst_api = original_call
            os.environ.pop("CATALYST_TASK_ID", None)

        self.assertEqual(calls[0][0], "/api/v1/tasks/update")
        self.assertEqual(calls[0][1]["task_id"], "abc-123")
        self.assertEqual(calls[0][1]["progress"], 42)


class PackageStagingEndpointTests(unittest.TestCase):
    """spark-daemon's `/api/v1/lcm/package`, which is where an uploaded archive lands."""

    def setUp(self):
        self.source = read("spark_daemon_decoded.py")

    def test_the_endpoint_is_routed(self):
        route = ast.parse(self.source)
        cls = next(n for n in ast.walk(route)
                   if isinstance(n, ast.ClassDef) and n.name == "SparkDaemonHandler")
        router = next(n for n in cls.body
                      if isinstance(n, ast.FunctionDef) and n.name == "route_typed_post")
        paths = {n.value for n in ast.walk(router)
                 if isinstance(n, ast.Constant) and isinstance(n.value, str)}
        self.assertIn("/api/v1/lcm/package", paths)

    def test_the_destination_is_a_constant_and_not_a_request_parameter(self):
        # The same guard /api/v1/dfs/write gets by naming a vdisk rather than a file: with
        # no path in the request, a caller cannot use this to place bytes anywhere else on
        # a hypervisor. A query parameter here would be an arbitrary root file write.
        cls = next(n for n in ast.walk(ast.parse(self.source))
                   if isinstance(n, ast.ClassDef) and n.name == "SparkDaemonHandler")
        handler = next(n for n in cls.body
                       if isinstance(n, ast.FunctionDef) and n.name == "handle_lcm_package")
        self.assertEqual([arg.arg for arg in handler.args.args], ["self"],
                         "the handler takes a parameter, so the path can come from a caller")

        reads_request_path = [
            node for node in ast.walk(handler)
            if isinstance(node, ast.Attribute)
            and getattr(node.value, "id", None) == "self"
            and node.attr in ("path", "query")
        ]
        self.assertEqual(reads_request_path, [],
                         "the handler reads the request line, so a caller can name a file")
        self.assertNotIn("parse_qs", ast.dump(handler))

    def _call(self, content_length, body, tmpdir):
        """Drive the handler with a fake request and return (status, payload)."""
        namespace = {"os": os, "json": json}
        handler = extract_method(self.source, "SparkDaemonHandler", "handle_lcm_package",
                                 namespace)

        answered = {}

        class FakeSelf:
            LCM_PACKAGE_PATH = os.path.join(tmpdir, "helios_update.zip")
            LCM_PACKAGE_MAX_BYTES = 1024

            headers = {"Content-Length": str(content_length)}
            rfile = io.BytesIO(body)

            def send_json_response(self, status, payload):
                answered["status"] = status
                answered["payload"] = payload

        fake = FakeSelf()
        fake.headers = {"Content-Length": str(content_length)}
        handler(fake)
        return answered, fake.LCM_PACKAGE_PATH

    def test_a_body_without_a_length_is_refused(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            answered, path = self._call(0, b"", tmpdir)
            self.assertEqual(answered["status"], 400)
            self.assertFalse(os.path.exists(path))

    def test_a_body_larger_than_the_cap_is_refused_before_it_is_read(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            answered, path = self._call(4096, b"x" * 4096, tmpdir)
            self.assertEqual(answered["status"], 413)
            self.assertFalse(os.path.exists(path))

    def test_a_complete_body_is_staged_and_the_count_reported(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            answered, path = self._call(5, b"hello", tmpdir)
            self.assertEqual(answered["status"], 200)
            self.assertEqual(answered["payload"]["written"], 5)
            with open(path, "rb") as handle:
                self.assertEqual(handle.read(), b"hello")

    def test_a_truncated_body_leaves_nothing_where_the_loader_looks(self):
        # A short write does fail validation later, but it fails it as "not a zip file",
        # which sends an operator looking at the package rather than at the transfer. The
        # rename is what makes the staged path either the whole archive or absent.
        with tempfile.TemporaryDirectory() as tmpdir:
            answered, path = self._call(10, b"hel", tmpdir)
            self.assertEqual(answered["status"], 500)
            self.assertIn("short", answered["payload"]["error"])
            self.assertEqual(answered["payload"]["written"], 3)
            self.assertFalse(os.path.exists(path))
            self.assertFalse(os.path.exists(path + ".part"))

    def test_the_loader_and_the_daemon_agree_on_where_a_package_is(self):
        # Two constants in two files, and the whole task turns on their being the same
        # string: the console streams to the daemon, then asks hylia to read a path.
        match = re.search(r'LCM_PACKAGE_PATH\s*=\s*"([^"]+)"', self.source)
        self.assertIsNotNone(match)
        elixir = read(os.path.join("spectrum_phx", "lib", "spectrum_phx", "lcm.ex"))
        self.assertIn('@package_path "%s"' % match.group(1), elixir)


class LanayruTaskWorkerTests(unittest.TestCase):
    """The console backend's Catalyst worker for the Kubernetes engine."""

    def setUp(self):
        self.source = read("spectrum_server.py")

    def test_only_one_node_drains_the_queue(self):
        # Two nodes draining this queue is two deployments of the same Kubernetes cluster.
        #
        # It used to be gated on holding ZooKeeper leadership, which was one node by
        # accident of the ensemble's own election -- so the gate was correct and relocated
        # the worker, along with every other leader-only workload in the cluster, every time
        # ZooKeeper re-elected. The gate is a candidacy for this job now. What is asserted is
        # unchanged: the loop decides, before it polls, that it is the one worker.
        tree = ast.parse(self.source)
        loop = next(n for n in tree.body
                    if isinstance(n, ast.FunctionDef) and n.name == "lanayru_queue_loop")
        calls = {n.func.id for n in ast.walk(loop)
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        self.assertIn("candidacy", calls)
        self.assertIn("lanayru_queue_loop", self.source)
        body = self.source[self.source.index("def lanayru_queue_loop"):]
        body = body[:body.index("\nclass ")]
        self.assertIn("worker.leading()", body,
                      "the loop no longer asks whether it leads before polling")

    def _run_task(self, action, worker):
        reported = []
        logged = []
        namespace = {
            "time": __import__("time"),
            "traceback": type("quiet", (), {"print_exc": staticmethod(lambda *a, **k: None)}),
            "call_catalyst_api": lambda path, payload=None, method="GET": (
                reported.append(payload) or (200, {})),
            "deploy_lanayru_worker": worker,
            "destroy_lanayru_worker": worker,
            "log_catalyst_task": lambda *a, **k: logged.append((a, k)) or (None, None),
        }
        run = extract_function(self.source, "run_lanayru_task", namespace)
        run("task-9", action, {"cluster_name": "k8s", "control_nodes": 1})
        return reported, logged

    def test_a_completed_deploy_ends_its_task(self):
        reported, _ = self._run_task("deploy", lambda *args: None)
        self.assertEqual(reported[0]["status"], "processing")
        self.assertEqual(reported[-1]["status"], "completed")

    def test_a_worker_that_raises_ends_its_task_as_failed(self):
        # The workers record the failures they catch. This is for the ones they do not --
        # an import that fails, a row shaped differently than expected -- which would
        # otherwise leave the task `processing` for ever, and the ring spinning on it.
        def explode(*args):
            raise RuntimeError("kine would not start")

        reported, logged = self._run_task("deploy", explode)
        self.assertEqual(reported[-1]["status"], "failed")
        self.assertIn("kine would not start", reported[-1]["error_msg"])
        # And in the table, which is what the console actually reads.
        self.assertTrue(any(call[0][2] == "failed" for call in logged))

    def test_an_action_the_worker_does_not_know_fails_rather_than_succeeds_quietly(self):
        reported, _ = self._run_task("resize", lambda *args: None)
        self.assertEqual(reported[-1]["status"], "failed")
        self.assertIn("resize", reported[-1]["error_msg"])

    def test_the_worker_is_given_catalysts_task_id_and_not_one_of_its_own(self):
        # `log_catalyst_task` writes by primary key, so the row the queue submission wrote
        # and the rows the worker writes are the same row only if they share an id. A
        # worker minting its own would leave the submitted task pending for ever beside a
        # second task that appears from nowhere and finishes.
        seen = []
        self._run_task("deploy", lambda task_id, *rest: seen.append(task_id))
        self.assertEqual(seen, ["task-9"])


if __name__ == "__main__":
    unittest.main()
