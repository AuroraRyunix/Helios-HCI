import unittest

import rauru_failover as f


def run(*events, start=None):
    state, actions = start or f.SetState(), []
    for e in events:
        state, acts = f.step(state, e)
        actions.append([a for a, _ in acts])
    return state, actions


FAILOVER = {"kind": "failover", "operator": "alice", "snapshot_visible": True,
            "source_state": "stopped", "snapshot_age_seconds": 120}


class Failover(unittest.TestCase):
    def test_the_steps_are_clone_rebuild_start_in_that_order(self):
        state, acts = run(FAILOVER)
        self.assertEqual((state.state, state.site, state.rpo_seconds), (f.RUNNING_AT_TARGET, "target", 120))
        self.assertEqual(acts[0][:3], [f.CLONE_MEMBERS, f.REBUILD_DEFINITION, f.START_GUEST])

    def test_it_is_never_automatic(self):
        with self.assertRaisesRegex(f.Refused, "never automatic"):
            f.step(f.SetState(), dict(FAILOVER, operator=None))

    def test_without_a_complete_snapshot_there_is_nothing_to_start(self):
        with self.assertRaisesRegex(f.Refused, "no complete snapshot"):
            f.step(f.SetState(), dict(FAILOVER, snapshot_visible=False))

    def test_a_source_that_is_running_blocks_it(self):
        with self.assertRaisesRegex(f.Refused, "two copies"):
            f.step(f.SetState(), dict(FAILOVER, source_state="running"))

    def test_an_unreachable_source_needs_the_risk_accepted_by_name(self):
        unreachable = dict(FAILOVER, source_state="unreachable")
        with self.assertRaisesRegex(f.Refused, "not the same as it being down"):
            f.step(f.SetState(), unreachable)
        state, _ = f.step(f.SetState(), dict(unreachable, accept_risk_source_may_be_running=True))
        self.assertEqual(state.state, f.RUNNING_AT_TARGET)

    def test_only_a_replicating_set_fails_over(self):
        with self.assertRaises(f.Refused):
            f.step(f.SetState(f.RUNNING_AT_TARGET, "target"), FAILOVER)


class Failback(unittest.TestCase):
    def at_target(self):
        return f.SetState(f.RUNNING_AT_TARGET, "target")

    def test_the_whole_way_back(self):
        state, acts = run(
            {"kind": "failback", "source_state": "reachable"},
            {"kind": "delta_caught_up"},
            {"kind": "cutover", "operator": "alice"},
            {"kind": "final_delta_verified", "visible_at_source": True},
            start=self.at_target())
        self.assertEqual((state.state, state.site), (f.REPLICATING, "source"))
        self.assertEqual(acts[0], [f.REVERSE_REPLICATE])
        self.assertEqual(acts[2], [f.STOP_GUEST, f.FINAL_DELTA, f.VERIFY_VISIBLE],
                         "stop, then the last changes, then proof, in that order")
        self.assertEqual(acts[3], [f.START_GUEST, f.RETIRE_TARGET_COPY])

    def test_the_guest_is_not_started_at_the_source_on_an_unconfirmed_delta(self):
        state, acts = run(
            {"kind": "failback", "source_state": "reachable"}, {"kind": "delta_caught_up"},
            {"kind": "cutover", "operator": "a"},
            {"kind": "final_delta_verified", "visible_at_source": False},
            start=self.at_target())
        self.assertEqual(state.state, f.CUTTING_OVER)
        self.assertNotIn(f.START_GUEST, acts[3])
        self.assertEqual(acts[3], [f.ALERT])

    def test_failback_needs_the_source_reachable(self):
        with self.assertRaises(f.Refused):
            f.step(self.at_target(), {"kind": "failback", "source_state": "unreachable"})

    def test_cutover_needs_a_caught_up_delta_and_an_operator(self):
        syncing = f.SetState(f.FAILBACK_SYNCING, "target")
        with self.assertRaisesRegex(f.Refused, "ready"):
            f.step(syncing, {"kind": "cutover", "operator": "a"})
        ready = f.SetState(f.FAILBACK_READY, "target")
        with self.assertRaisesRegex(f.Refused, "never automatic"):
            f.step(ready, {"kind": "cutover"})

    def test_a_failback_can_be_abandoned_until_the_guest_is_stopped(self):
        for s in (f.FAILBACK_SYNCING, f.FAILBACK_READY):
            state, _ = f.step(f.SetState(s, "target"), {"kind": "abort_failback"})
            self.assertEqual(state.state, f.RUNNING_AT_TARGET)
        with self.assertRaises(f.Refused):
            f.step(f.SetState(f.CUTTING_OVER, "target"), {"kind": "abort_failback"})


class SplitBrain(unittest.TestCase):
    def test_the_source_reporting_the_guest_while_it_runs_at_the_target(self):
        for s in (f.RUNNING_AT_TARGET, f.FAILBACK_SYNCING, f.FAILBACK_READY, f.CUTTING_OVER):
            state, acts = f.step(f.SetState(s, "target"), {"kind": "source_reports_guest_running"})
            self.assertEqual(state.state, f.SPLIT_BRAIN)
            self.assertEqual([a for a, _ in acts], [f.ALERT], "nothing is stopped on a guess")

    def test_nothing_but_a_decision_gets_out_of_it(self):
        sb = f.SetState(f.SPLIT_BRAIN, "target")
        for e in ({"kind": "failback", "source_state": "reachable"}, FAILOVER, {"kind": "cutover", "operator": "a"}):
            with self.assertRaisesRegex(f.Refused, "only an operator"):
                f.step(sb, e)
        with self.assertRaises(f.Refused):
            f.step(sb, {"kind": "resolve", "winner": "both"})

    def test_resolving_stops_the_loser_and_says_so(self):
        state, acts = f.step(f.SetState(f.SPLIT_BRAIN, "target"), {"kind": "resolve", "winner": "source"})
        self.assertEqual((state.state, state.site), (f.REPLICATING, "source"))
        self.assertEqual(acts[0], (f.STOP_GUEST, {"site": "target"}))
        state, acts = f.step(f.SetState(f.SPLIT_BRAIN, "target"), {"kind": "resolve", "winner": "target"})
        self.assertEqual((state.state, acts[0][1]["site"]), (f.RUNNING_AT_TARGET, "source"))

    def test_a_replicating_set_is_not_split_by_a_report_from_the_source(self):
        with self.assertRaises(f.Refused):
            f.step(f.SetState(), {"kind": "source_reports_guest_running"})


class Whole(unittest.TestCase):
    def test_every_state_is_reachable_and_unknown_events_are_refused(self):
        with self.assertRaises(f.Refused):
            f.step(f.SetState(), {"kind": "nonsense"})
        self.assertEqual(len(f.STATES), 6)

    def test_step_does_not_mutate_its_input(self):
        s = f.SetState()
        f.step(s, FAILOVER)
        self.assertEqual(s, f.SetState())


if __name__ == "__main__":
    unittest.main()
