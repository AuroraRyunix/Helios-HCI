#!/usr/bin/env python3
"""valcli keeps working when this node's own database is down.

Daruk and the cqlsh fallback both talk to the database on this host, so a node whose hydra-db
was down could run no valcli command that reads the cluster -- including the ones needed to see
why. After a failure that is about *reaching* the database, each other node is asked in turn
through its spark-daemon and the first answer wins. A statement the database rejected is not
retried elsewhere, and a conditional statement still raises.

Run with:  python -m unittest test_valcli_peer_fallback
"""

import base64
import importlib.util
import io
import json
import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))


def load():
    spec = importlib.util.spec_from_file_location("valcli_peer_fallback", os.path.join(HERE, "valcli.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


valcli = load()
Q = "SELECT JSON hostname FROM hydra.nodes;"


class Remote:
    def __init__(self, answers):
        self.answers = answers
        self.calls = []

    def __call__(self, ip, command):
        self.calls.append((ip, command))
        return self.answers.get(ip, (-1, "", "unreachable"))


def run(local, remote, peers=("10.0.0.2", "10.0.0.3")):
    with mock.patch.object(valcli, "_local_run_cql_query", lambda q, *a, **k: local), \
            mock.patch("sys.stderr", io.StringIO()) as err:
        return valcli.run_cql_query(Q, peers=list(peers), remote=remote), err.getvalue()


class TheFallback(unittest.TestCase):
    DOWN = (1, "", "NoHostAvailable: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError})")

    def test_a_healthy_local_database_is_the_answer_and_no_peer_is_asked(self):
        remote = Remote({})
        (result, _) = run((0, '{"hostname": "a"}', ""), remote)
        self.assertEqual(result, (0, '{"hostname": "a"}', ""))
        self.assertEqual(remote.calls, [])

    def test_a_local_database_that_is_down_is_answered_by_a_peer(self):
        remote = Remote({"10.0.0.2": (0, '{"hostname": "b"}\n', "")})
        (result, said), = [run(self.DOWN, remote)]
        self.assertEqual(result, (0, '{"hostname": "b"}', ""))
        self.assertIn("answered by 10.0.0.2", said)

    def test_the_statement_travels_intact_and_runs_against_that_peers_own_database(self):
        remote = Remote({"10.0.0.2": (0, "", "")})
        run(self.DOWN, remote)
        ip, command = remote.calls[0]
        self.assertEqual(ip, "10.0.0.2")
        self.assertIn(base64.b64encode(Q.encode()).decode(), command)
        self.assertTrue(command.endswith("cqlsh 10.0.0.2"))
        self.assertIn("podman exec -i systemd-hydra-db", command)

    def test_the_next_peer_is_tried_when_the_first_does_not_answer(self):
        remote = Remote({"10.0.0.3": (0, "ok", "")})
        (result, _), = [run(self.DOWN, remote)]
        self.assertEqual([c[0] for c in remote.calls], ["10.0.0.2", "10.0.0.3"])
        self.assertEqual(result[0], 0)

    def test_when_nobody_answers_the_original_failure_is_returned_with_what_was_tried(self):
        remote = Remote({})
        ((rc, out, err), _), = [run(self.DOWN, remote)]
        self.assertEqual(rc, 1)
        self.assertIn("NoHostAvailable", err)
        self.assertIn("no peer answered either", err)
        self.assertIn("10.0.0.3", err)

    def test_a_rejected_statement_is_not_asked_of_every_node(self):
        remote = Remote({"10.0.0.2": (0, "", "")})
        for message in ("SyntaxException: line 1:7 no viable alternative at input 'FORM'",
                        "InvalidRequest: Error from server: code=2200 [Invalid query]",
                        "Unknown identifier nodez"):
            with self.subTest(message=message):
                ((rc, _, err), _), = [run((1, "", message), remote)]
                self.assertEqual((rc, err), (1, message))
        self.assertEqual(remote.calls, [])

    def test_a_conditional_statement_still_raises(self):
        with self.assertRaises(valcli.ConditionalStatementError):
            valcli.run_cql_query("UPDATE hydra.vms SET state = 'x' WHERE name = 'a' IF state = 'y';")

    def test_a_single_node_cluster_has_no_peer_to_ask(self):
        remote = Remote({})
        ((rc, _, _), _), = [run(self.DOWN, remote, peers=())]
        self.assertEqual((rc, remote.calls), (1, []))

    def test_peers_come_from_the_cluster_document_without_this_node(self):
        import json
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump({"hosts": [{"ip": valcli.LOCAL_IP}, {"ip": "10.0.0.2"}, {"ip": "127.0.0.1"}, {"ip": "10.0.0.3"}]}, handle)
        try:
            self.assertEqual(valcli.cluster_peer_ips(handle.name), ["10.0.0.2", "10.0.0.3"])
        finally:
            os.unlink(handle.name)
        self.assertEqual(valcli.cluster_peer_ips("/nonexistent/cluster.json"), [])


class TheClassifier(unittest.TestCase):
    def test_what_counts_as_the_database_being_down(self):
        for text in ("NoHostAvailable: ...", "Connection refused", "Error: no container with name or ID",
                     "Operation timed out", "Database query execution error", "container is not running"):
            self.assertTrue(valcli.database_looks_down(1, text), text)
        self.assertFalse(valcli.database_looks_down(0, "NoHostAvailable"))
        self.assertFalse(valcli.database_looks_down(1, "SyntaxException"))
        self.assertFalse(valcli.database_looks_down(1, None))


class TheCatalystTaskWaiter(unittest.TestCase):
    def test_wait_for_catalyst_task_succeeds_via_http_on_candidate_node(self):
        tid = "52adb8e1-adb6-413b-af4e-256c3fddaf3c"
        mock_resp = mock.MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = json.dumps({"status": "completed", "progress": 100}).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp
        mock_resp.__exit__.return_value = None

        with mock.patch("urllib.request.urlopen", return_value=mock_resp), \
             mock.patch.object(valcli, "get_zookeeper_leader_ip", return_value="10.0.0.1"), \
             mock.patch.object(valcli, "catalyst_client_context", return_value=None), \
             mock.patch("sys.stdout", io.StringIO()):
            res = valcli.wait_for_catalyst_task(tid, timeout_seconds=10)
            self.assertTrue(res)

    def test_wait_for_catalyst_task_falls_back_to_scylladb_when_http_fails(self):
        tid = "52adb8e1-adb6-413b-af4e-256c3fddaf3c"
        # HTTP raises exception
        def urlopen_fail(*args, **kwargs):
            raise Exception("Connection refused on port 9091")

        cql_row = json.dumps({"status": "completed", "progress": 100, "error_msg": ""})
        with mock.patch("urllib.request.urlopen", side_effect=urlopen_fail), \
             mock.patch.object(valcli, "get_zookeeper_leader_ip", return_value="10.0.0.1"), \
             mock.patch.object(valcli, "run_cql_query_via_peers", return_value=(0, cql_row, "")), \
             mock.patch("sys.stdout", io.StringIO()):
            res = valcli.wait_for_catalyst_task(tid, timeout_seconds=10)
            self.assertTrue(res)


if __name__ == "__main__":
    unittest.main()
