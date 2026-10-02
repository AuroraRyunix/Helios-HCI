#!/usr/bin/env python3
"""A task Catalyst forgets is a task that did not survive being written down.

Catalyst persisted a *record* of every task and then threw the tasks away. `recover_stuck_tasks`
ran at every start, on every node, and marked every `pending` and every `processing` row
`failed` with "Task aborted due to system daemon restart." A task submitted a second before a
restart was recorded, aborted, and never attempted -- which is data loss, arrived at
deliberately, in the function named after recovering from it.

Three properties are asserted here, because fixing one of them leaves the others available:

  * **a task that had not started is replayed**, because nothing in the cluster has been
    touched and running it is simply doing what was asked;
  * **a task that had started is failed with a reason that says so**, because what it did
    before its dispatcher stopped is not recorded and the actions behind these rows -- a live
    migration, a rolling upgrade step -- are not replayable on a guess;
  * **nothing is dropped silently**, which is the property that distinguishes the two cases
    above from the one behaviour that covered both.

And the shape the rows need for any of that to be reportable: a parent, a component, and a
per-component sequence id, which is what a real task framework records and this one did not.

Run with:  python -m unittest test_catalyst_task_tree
"""

import importlib.util
import io
import json
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name, alias):
    spec = importlib.util.spec_from_file_location(alias, os.path.join(HERE, name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


class TheMigrationIsSafeToRerun(unittest.TestCase):
    """A migration is recorded once, which makes a re-run unusual rather than impossible.

    0008 and 0010 each added one column with a bare `ALTER TABLE ... ADD`, and got away with
    it because one statement either applies or does not. These six columns first shipped as
    one migration that used `ADD IF NOT EXISTS` to get the same safety -- correct reasoning,
    and a syntax this ScyllaDB does not have. It answered `no viable alternative at input
    'IF'`, and because `ensure_schema` runs before Catalyst serves, every node crash-looped.

    So the safety comes from the ledger instead: one statement per migration, which is what
    `test_a_migration_that_alters_does_so_once` pins. A failure loses exactly one column, it
    is not recorded, and the re-run starts at it.
    """

    COLUMN_MIGRATIONS = (
        "0011-catalyst-task-parent",
        "0012-catalyst-task-component",
        "0013-catalyst-task-sequence-id",
        "0014-catalyst-task-type",
        "0015-catalyst-task-completed-at",
        "0016-catalyst-task-sequence",
    )

    def setUp(self):
        self.schema = load("helios_schema.py", "helios_schema_tasks")
        by_id = {m["id"]: m for m in self.schema.MIGRATIONS}
        for name in self.COLUMN_MIGRATIONS:
            self.assertIn(name, by_id, "%s is gone from the migration list" % name)
        self.migrations = [by_id[name] for name in self.COLUMN_MIGRATIONS]
        # Kept so the per-statement assertions below read over the whole change.
        self.migration = {"statements": [st for m in self.migrations
                                         for st in m["statements"]]}

    def test_each_column_arrives_in_its_own_migration(self):
        """The property that replaced ADD IF NOT EXISTS. One ALTER per migration means a
        partial apply is recorded as far as it got."""
        for migration in self.migrations:
            alters = [st for st in migration["statements"]
                      if st.strip().upper().startswith("ALTER")]
            self.assertLessEqual(len(alters), 1, migration["id"])
            for statement in alters:
                self.assertNotIn(
                    "IF NOT EXISTS", statement.upper(),
                    "%s would not parse on the deployed ScyllaDB" % migration["id"])

    def test_the_sequence_table_is_created_conditionally(self):
        """CREATE TABLE IF NOT EXISTS *is* supported -- it is only the column form that is
        not -- so the allocator keeps its guard."""
        creates = [st for st in self.migration["statements"] if "CREATE TABLE" in st]
        self.assertTrue(creates, "the sequence allocator has no table")
        for statement in creates:
            self.assertIn("IF NOT EXISTS", statement)

    def test_it_rewrites_no_existing_row(self):
        """Nothing in a migration may need a cluster restart to be readable.

        Every column added here is nullable, and a task recorded before the migration keeps
        reading correctly with all of them null -- which is the truth about it. An UPDATE that
        backfilled a component or a sequence id would be inventing a fact about work that has
        already happened.
        """
        for statement in self.migration["statements"]:
            upper = statement.upper()
            self.assertNotIn("UPDATE ", upper, statement)
            self.assertNotIn("INSERT ", upper, statement)
            self.assertNotIn("DELETE ", upper, statement)

    def test_the_columns_a_task_framework_needs_are_all_there(self):
        statements = " ".join(self.migration["statements"])
        for column in ("parent_task_id uuid", "component text", "sequence_id bigint",
                       "task_type text", "completed_at timestamp"):
            self.assertIn(column, statements)

    def test_no_migration_uses_a_column_guard_this_dialect_does_not_have(self):
        """`ALTER TABLE ... ADD IF NOT EXISTS` does not parse on the deployed ScyllaDB.

        These columns first shipped as one migration using exactly that, on the correct
        reasoning that a six-statement migration failing on its fourth leaves three applied
        and unrecorded. The reasoning survived; the syntax did not -- the server answered
        `no viable alternative at input 'IF'` and Catalyst crash-looped on every node, 171
        failures in two minutes, because `ensure_schema` runs before it serves. A rollout
        found it and no test could have, which is why this one exists.
        """
        for migration in self.schema.MIGRATIONS:
            for statement in migration["statements"]:
                if statement.strip().upper().startswith("ALTER"):
                    self.assertNotIn(
                        "IF NOT EXISTS", statement.upper(),
                        "%s uses ALTER ... IF NOT EXISTS, which this ScyllaDB rejects"
                        % migration["id"])

    def test_a_migration_that_alters_does_so_once(self):
        """Re-runnability comes from the ledger, not from the dialect.

        One ALTER per migration means a failure loses exactly one column and the re-run
        starts at it. Asserted only for migrations added after this was understood: 0006 and
        0008 carry several and are already applied everywhere, and editing a shipped
        migration changes its checksum, which is the thing the checksum exists to catch.
        """
        grandfathered = {"0006-dfs-replication", "0008-container-compression"}
        for migration in self.schema.MIGRATIONS:
            if migration["id"] in grandfathered:
                continue
            alters = [st for st in migration["statements"]
                      if st.strip().upper().startswith("ALTER")]
            self.assertLessEqual(
                len(alters), 1,
                "%s has %d ALTERs; split it so a partial failure is recoverable"
                % (migration["id"], len(alters)))

    def test_nothing_before_it_moved(self):
        """Editing a shipped migration gives two clusters different schemas and both the
        belief that they are up to date. The checksum catches that; this catches reordering.

        Deliberately not asserting this is the *last* migration. It was when it was written,
        and `0017-egroup-access-data` landed beside it the same day -- an assertion that
        breaks whenever someone appends a migration is testing the calendar, not the
        property. What has to hold is that the order is stable and the ids are unique.
        """
        ids = [m["id"] for m in self.schema.MIGRATIONS]
        self.assertIn("0011-catalyst-task-parent", ids)
        self.assertEqual(sorted(ids), ids, "the migrations are no longer in order")
        self.assertEqual(len(set(ids)), len(ids), "two migrations share an id")
        # And it still sits after everything that shipped before it.
        self.assertGreater(ids.index("0011-catalyst-task-parent"), ids.index("0010-vm-graphics"))


class TheTaskRowCarriesItsPlaceInTheTree(unittest.TestCase):
    def setUp(self):
        self.schema = load("helios_schema.py", "helios_schema_rows")

    def test_a_parent_is_a_bare_uuid_and_a_missing_one_is_null(self):
        """A uuid literal is not quoted in CQL. Writing one as a string is a type error, and
        writing an empty string for "no parent" is a type error that only shows up on the
        rows that have no parent -- which is most of them."""
        with_parent = self.schema.task_insert_statement(
            "11111111-1111-1111-1111-111111111111", "vali", "start", "{}", 1000,
            parent_task_id="22222222-2222-2222-2222-222222222222")
        self.assertIn("22222222-2222-2222-2222-222222222222", with_parent)
        self.assertNotIn("'22222222", with_parent)

        without = self.schema.task_insert_statement(
            "11111111-1111-1111-1111-111111111111", "vali", "start", "{}", 1000)
        self.assertIn("NULL", without)
        self.assertNotIn("''", without.split("VALUES", 1)[1].replace("'{}'", ""))

    def test_a_parent_that_is_not_a_uuid_is_dropped_rather_than_interpolated(self):
        """`parent_task_id` arrives from a request body. This builds statement text."""
        statement = self.schema.task_insert_statement(
            "11111111-1111-1111-1111-111111111111", "vali", "start", "{}", 1000,
            parent_task_id="'; DROP TABLE hydra.catalyst_tasks; --")
        self.assertNotIn("DROP TABLE", statement)

    def test_the_component_defaults_to_the_service_and_the_type_to_the_action(self):
        """A caller that knows nothing new still produces a row where every column says
        something true, rather than a null a reader has to interpret."""
        statement = self.schema.task_insert_statement(
            "11111111-1111-1111-1111-111111111111", "dagur", "execute", "{}", 1000)
        self.assertIn("'dagur'", statement)
        self.assertIn("'execute'", statement)

    def test_the_component_is_not_forced_to_be_the_executor(self):
        """The distinction the column exists for: a Hylia upgrade step runs on the `dagur`
        queue, and recording it as a Dagur task attributes a rolling upgrade to the cron
        runner."""
        statement = self.schema.task_insert_statement(
            "11111111-1111-1111-1111-111111111111", "dagur", "execute", "{}", 1000,
            component="Hylia", task_type="upgrade_node")
        self.assertIn("'Hylia'", statement)
        self.assertIn("'upgrade_node'", statement)
        self.assertIn("'dagur'", statement)

    def test_a_null_sequence_is_written_as_null_and_not_as_zero(self):
        """Zero is a position. None means the number could not be claimed, and a task
        framework that renders those the same way says a task was the first thing its
        component ever did."""
        statement = self.schema.task_insert_statement(
            "11111111-1111-1111-1111-111111111111", "vali", "start", "{}", 1000,
            sequence_id=None)
        self.assertNotIn(", 0,", statement.split("VALUES", 1)[1].replace(", 0, 1000", ""))
        self.assertIn("NULL", statement)

    def test_a_payload_quote_cannot_end_the_literal(self):
        payload = json.dumps({"vm_name": "o'brien"})
        statement = self.schema.task_insert_statement(
            "11111111-1111-1111-1111-111111111111", "vali", "start", payload, 1000)
        self.assertIn("o''brien", statement)


class CompletionIsRecordedSeparatelyFromProgress(unittest.TestCase):
    """`updated_at` moves on every progress report, so it is not an end time.

    A completed task's duration was therefore unknowable from its row even though both ends
    of it had been written: the start was in `created_at` and the end had been overwritten
    by the last progress report before it.
    """

    def setUp(self):
        self.schema = load("helios_schema.py", "helios_schema_updates")

    def test_a_progress_report_does_not_claim_the_task_finished(self):
        statement = self.schema.task_update_statement(
            "11111111-1111-1111-1111-111111111111", "processing", 40, 2000)
        self.assertIn("updated_at = 2000", statement)
        self.assertNotIn("completed_at", statement)

    def test_both_terminal_statuses_record_when_they_happened(self):
        for status in ("completed", "failed"):
            statement = self.schema.task_update_statement(
                "11111111-1111-1111-1111-111111111111", status, 100, 3000)
            self.assertIn("completed_at = 3000", statement,
                          "a %s task has no end time" % status)

    def test_a_reason_is_recorded_when_there_is_one_and_not_blanked_when_there_is_not(self):
        """An update with no error must not overwrite the reason an earlier one recorded."""
        with_reason = self.schema.task_update_statement(
            "11111111-1111-1111-1111-111111111111", "failed", 100, 3000,
            error_msg="the fence could not be confirmed")
        self.assertIn("error_msg = 'the fence could not be confirmed'", with_reason)

        without = self.schema.task_update_statement(
            "11111111-1111-1111-1111-111111111111", "processing", 10, 3000)
        self.assertNotIn("error_msg", without)


class TheSequenceIsClaimedRatherThanIncremented(unittest.TestCase):
    """A blind `n + 1` is a lost update, and a lost update here is two tasks with one
    sequence id -- the single property the column exists to provide."""

    def setUp(self):
        self.schema = load("helios_schema.py", "helios_schema_sequence")

    def fake_counter(self, start=0, steal=0):
        """A counter row behind a compare-and-swap, optionally with a competing claimant that
        gets in first `steal` times."""
        state = {"value": start, "steals": steal, "calls": []}

        def lwt(endpoint, params):
            state["calls"].append(params)
            expected = params["expected_sequence_id"]
            held = state["value"] or None
            if state["steals"] > 0:
                state["steals"] -= 1
                state["value"] = (state["value"] or 0) + 1
                return True, False, {"next_sequence_id": state["value"]}, ""
            if expected != held:
                return True, False, {"next_sequence_id": state["value"]}, ""
            state["value"] = params["next_sequence_id"]
            return True, True, {}, ""

        return lwt, state

    def test_the_first_claim_for_a_component_needs_no_seeded_row(self):
        """`IF next_sequence_id = NULL` matches a row that does not exist, which is the
        property /v1/schedule/claim-job already relies on for a clock never written."""
        lwt, state = self.fake_counter()
        self.assertEqual(self.schema.claim_task_sequence(lwt, "Hylia"), 1)
        self.assertIsNone(state["calls"][0]["expected_sequence_id"])

    def test_two_claimants_never_get_the_same_number(self):
        lwt, _state = self.fake_counter(start=0, steal=1)
        first = self.schema.claim_task_sequence(lwt, "Vali")
        self.assertEqual(first, 2, "the loser of the race reused the winner's number")

    def test_a_hint_that_is_right_costs_one_round_trip(self):
        lwt, state = self.fake_counter(start=7)
        self.assertEqual(self.schema.claim_task_sequence(lwt, "Vali", expected=7), 8)
        self.assertEqual(len(state["calls"]), 1)

    def test_a_hint_that_is_wrong_is_corrected_by_the_refusal(self):
        lwt, state = self.fake_counter(start=7)
        self.assertEqual(self.schema.claim_task_sequence(lwt, "Vali", expected=2), 8)
        self.assertEqual(len(state["calls"]), 2)

    def test_a_failing_database_gives_no_number_rather_than_a_guess(self):
        def broken(_endpoint, _params):
            return False, False, {}, "Daruk is not answering"

        self.assertIsNone(self.schema.claim_task_sequence(broken, "Vali"))

    def test_an_endlessly_contended_counter_gives_up_instead_of_spinning(self):
        lwt, _state = self.fake_counter(start=0, steal=99)
        self.assertIsNone(self.schema.claim_task_sequence(lwt, "Vali", attempts=3))


class RecoveryReplaysRatherThanAborts(unittest.TestCase):
    """The behaviour this replaces, stated as the thing that must not happen again."""

    def setUp(self):
        self.source = read("catalyst.py")

    def test_the_abort_on_startup_is_gone(self):
        self.assertNotIn("def recover_stuck_tasks", self.source)
        # The old reason survives only as prose explaining what was removed. Asserting on
        # the whole file would make the explanation the thing that fails the test.
        code = [line for line in self.source.splitlines()
                if not line.lstrip().startswith("#")]
        self.assertNotIn("Task aborted due to system daemon restart", "\n".join(code),
                         "every pending task is still being failed at start-up")

    def test_recovery_runs_where_the_queues_are_and_not_at_every_start(self):
        """It used to run in `main`, on every node. Two of three nodes then rewrote rows they
        had nowhere to put the work for, and the node that did have the queues raced them."""
        main = self.source[self.source.index("def main():"):]
        self.assertNotIn("recover_open_tasks()", main,
                         "recovery is still a start-up pass rather than something the node "
                         "holding the queues does when it acquires them")
        self.assertIn("dispatch_thread_loop", main)

    def test_a_pending_task_is_re_queued_and_a_running_one_is_failed_with_a_reason(self):
        namespace = self.recovery_namespace([
            {"task_id": "pending-1", "service": "vali", "action": "start",
             "status": "pending", "payload": '{"vm_name": "db"}'},
            {"task_id": "running-1", "service": "vali", "action": "migrate",
             "status": "processing", "payload": "{}"},
            {"task_id": "done-1", "service": "vali", "action": "stop",
             "status": "completed", "payload": "{}"},
        ])
        requeued, failed = namespace["recover_open_tasks"](fail_in_flight=True)

        self.assertEqual((requeued, failed), (1, 1))
        self.assertEqual([t["task_id"] for t in namespace["_queued"]], ["pending-1"])
        self.assertEqual([t for t, _reason in namespace["_failed"]], ["running-1"])

    def test_a_later_sweep_leaves_a_running_task_alone(self):
        """`processing` reads two ways a few seconds apart, and the sweep must not confuse
        them. On the first pass after acquiring the queues it means a worker was talking to a
        dispatcher that has stopped, so the row is a corpse. On every later pass it means a
        worker here is running the task right now -- and failing those would be this sweep
        killing every live task in the cluster on a fifteen-second timer.
        """
        namespace = self.recovery_namespace([
            {"task_id": "running-now", "service": "vali", "action": "migrate",
             "status": "processing", "payload": "{}"},
        ])
        self.assertEqual(namespace["recover_open_tasks"](), (0, 0))
        self.assertEqual(namespace["_failed"], [])

    def test_the_reason_says_it_was_in_flight_rather_than_that_a_daemon_restarted(self):
        """An operator reading "a daemon restarted" learns nothing about what to do. The row
        has to say that the task was running and that how far it got is not recorded."""
        namespace = self.recovery_namespace([
            {"task_id": "running-1", "service": "vali", "action": "migrate",
             "status": "processing", "payload": "{}"},
        ])
        namespace["recover_open_tasks"](fail_in_flight=True)
        _task, reason = namespace["_failed"][0]
        self.assertIn("running", reason.lower())
        self.assertIn("not recorded", reason.lower())

    def test_a_pending_task_for_a_queue_nothing_drains_is_failed_and_said_so(self):
        """Left pending it would be replayed on every sweep forever, and read as "slow"."""
        namespace = self.recovery_namespace([
            {"task_id": "orphan", "service": "nobody", "action": "x",
             "status": "pending", "payload": "{}"},
        ])
        requeued, failed = namespace["recover_open_tasks"]()

        self.assertEqual((requeued, failed), (0, 1))
        self.assertIn("no queue named", namespace["_failed"][0][1].lower())

    def test_a_task_already_on_the_queue_is_not_queued_twice(self):
        """A task is `pending` from the moment it is recorded until a worker picks it up,
        which is exactly the window the sweep runs in. Two copies of one task on one queue is
        two power operations on one guest."""
        namespace = self.recovery_namespace([
            {"task_id": "pending-1", "service": "vali", "action": "start",
             "status": "pending", "payload": "{}"},
        ])
        namespace["queued_task_ids"].add("pending-1")
        requeued, failed = namespace["recover_open_tasks"]()

        self.assertEqual((requeued, failed), (0, 0))
        self.assertEqual(namespace["_queued"], [])

    def test_an_unreadable_table_fails_nothing_and_replays_nothing(self):
        """"The database did not answer" and "there is nothing to replay" are different
        facts, and acting on the first as though it were the second fails every task in the
        cluster."""
        namespace = self.recovery_namespace(None)
        self.assertEqual(namespace["recover_open_tasks"](), (0, 0))
        self.assertEqual(namespace["_failed"], [])
        self.assertEqual(namespace["_queued"], [])

    # -- harness -----------------------------------------------------------

    def recovery_namespace(self, rows):
        """`recover_open_tasks` and the helpers it calls, with the database replaced.

        Extracted from the source rather than imported, because importing catalyst.py opens a
        socket to the schema layer at module scope.
        """
        import ast
        import threading

        tree = ast.parse(self.source)
        wanted = {"read_open_tasks", "fail_task", "recover_open_tasks"}
        queued = []
        failed = []
        namespace = {
            "json": json,
            "time": __import__("time"),
            "lock": threading.Lock(),
            "queued_task_ids": set(),
            "queues": {"vali": None, "dagur": None, "lanayru": None},
            "IN_FLIGHT_REASON": None,
            "print": lambda *args, **kwargs: None,
            "run_cql_query": lambda _cql: (0, "", ""),
            "schema_module": lambda: None,
            "submit_task_to_memory": lambda _service, task: queued.append(task),
            "_queued": queued,
            "_failed": failed,
        }
        namespace["read_open_tasks"] = lambda: (
            None if rows is None
            else [r for r in rows if r.get("status") in ("pending", "processing")])
        namespace["fail_task"] = lambda task_id, reason: failed.append((task_id, reason))

        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in wanted:
                exec(compile(ast.Module(body=[node], type_ignores=[]), "<catalyst>", "exec"),
                     namespace)
            if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == \
                    "IN_FLIGHT_REASON":
                exec(compile(ast.Module(body=[node], type_ignores=[]), "<catalyst>", "exec"),
                     namespace)
        # The extracted read and fail helpers are the fakes, not the real ones.
        namespace["read_open_tasks"] = lambda: (
            None if rows is None
            else [r for r in rows if r.get("status") in ("pending", "processing")])
        namespace["fail_task"] = lambda task_id, reason: failed.append((task_id, reason))
        return namespace


class EveryWriterOfTheTableWritesTheSameRow(unittest.TestCase):
    """Four writers, four column lists, which is how a task's parent came to live inside a
    JSON payload in one of them and nowhere else."""

    WRITERS = ("catalyst.py", "mipha.py", "spectrum_server.py")

    def test_nobody_hand_writes_an_insert_into_the_task_table(self):
        for name in self.WRITERS:
            source = read(name)
            self.assertNotIn("INSERT INTO hydra.catalyst_tasks", source,
                             "%s still builds its own task row; use the statement builder "
                             "in helios_schema so a row one writer creates is a row the "
                             "others can complete" % name)

    def test_the_parent_is_read_from_both_the_column_and_the_payload(self):
        """The payload is where it lived before there was a column, and the retention window
        is thirty days -- so a cluster that upgrades today reads those rows for a month."""
        tasks_ex = read(os.path.join("spectrum_phx", "lib", "spectrum_phx", "tasks.ex"))
        self.assertIn('get(row, "parent_task_id")', tasks_ex)
        self.assertIn('%{"parent_task_id" => id}', tasks_ex)


if __name__ == "__main__":
    unittest.main()
