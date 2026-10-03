#!/usr/bin/env python3
"""Tiering had no idea what was hot, and the design that needs to know said so.

`docs/dfs/multi_disk.md` specifies placing extent groups by free space, which needs no
history, and migrating cold ones down to slower media, which is nothing *but* history. The
second half could not be built because nothing anywhere recorded how often an extent group
was read or written. Nutanix's `medusa_extentgroupaccessdatamap` is the table that makes
their Curator able to rank its work; Helios had no equivalent, so the tiering design was
blocked on a missing input rather than on effort.

`hydra.dfs_egroup_access` is that input. The properties guarded here are the ones that would
quietly undo it, and each is a different way the obvious implementation is the wrong one:

  * **the read path does not talk to Hydra**, because a metadata round trip per read is the
    one thing the whole design forbids, and recording an access is exactly the sort of
    harmless-looking addition that would do it;
  * **the flush writes absolute totals**, not increments and not a counter column, so a
    lost flush leaves no hole and a duplicated one no double count -- the same objection
    D-8 raised against reference counts, applied to a statistic;
  * **the rows are keyed per observer**, because absolute writes to a row two nodes share
    would have each clobber the other and an extent group read on three nodes would report
    the heat seen by whichever flushed last;
  * **a write is counted on the drain, not on the guest's write**, because an extent group
    never sees a guest write at all -- it reaches the journal and is acknowledged there;
  * **reclaiming a group takes its access data with it**, or a table of facts about extent
    groups fills with facts about extent groups that no longer exist;
  * **nothing migrates data on the strength of the ranking**, which is measuring before
    moving and is the order the whole thing depends on being able to be trusted in.

And one that is not about code: the approximation has to be *written down*. Counters
aggregated in memory may decide where a copy of data goes and must never decide whether it
exists, and the only thing that keeps that boundary is the document saying so.

Run with:  python -m unittest test_egroup_access_data
"""

import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA = os.path.join(HERE, "helios_schema.py")
HEAT_RS = os.path.join(HERE, "sidon", "src", "heat.rs")
META_RS = os.path.join(HERE, "sidon", "src", "meta.rs")
PURAH_RS = os.path.join(HERE, "sidon", "src", "purah.rs")
VDISK_RS = os.path.join(HERE, "sidon", "src", "vdisk.rs")
CONTROL_RS = os.path.join(HERE, "sidon", "src", "control.rs")
MAIN_RS = os.path.join(HERE, "sidon", "src", "main.rs")
VALCLI = os.path.join(HERE, "valcli.py")
SIDON_PY = os.path.join(HERE, "helios_sidon.py")
METADATA_MD = os.path.join(HERE, "docs", "dfs", "metadata.md")
DECISIONS_MD = os.path.join(HERE, "docs", "dfs", "decisions.md")
MULTI_DISK_MD = os.path.join(HERE, "docs", "dfs", "multi_disk.md")

MIGRATION_ID = "0017-egroup-access-data"


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def rust_fn(source, name):
    """The body of one Rust fn, from its signature to the next one at the same indent."""
    match = re.search(r"^(\s*)(?:pub(?:\(crate\))? )?fn %s\b" % re.escape(name),
                      source, re.M)
    assert match, "no fn named %s" % name
    indent = match.group(1)
    rest = source[match.end():]
    nxt = re.search(r"^%s(?:pub(?:\(crate\))? )?fn " % re.escape(indent), rest, re.M)
    return rest[: nxt.start()] if nxt else rest


def migration():
    import helios_schema

    for entry in helios_schema.MIGRATIONS:
        if entry["id"] == MIGRATION_ID:
            return entry
    raise AssertionError("no migration %s in helios_schema.MIGRATIONS" % MIGRATION_ID)


class TheTableIsShapedForAbsoluteWritesFromSeveralNodes(unittest.TestCase):
    """The schema half. Two properties, and both are about what a flush may do."""

    @classmethod
    def setUpClass(cls):
        cls.migration = migration()
        cls.ddl = " ".join(" ".join(cls.migration["statements"]).split())

    def test_the_table_is_created(self):
        self.assertIn("hydra.dfs_egroup_access", self.ddl)
        self.assertIn("CREATE TABLE IF NOT EXISTS", self.ddl)

    def test_one_row_per_observing_node(self):
        """Keyed ((egroup_id), node), which is what makes every row single-writer.

        The flush writes absolute totals. Two nodes sharing a row would therefore each
        overwrite the other's counts on every flush, and the temperature of an extent group
        being read on three nodes would read as the temperature seen by whichever flushed
        last -- an under-count, which is the direction that makes a tiering pass spill
        something busy.
        """
        self.assertRegex(
            self.ddl,
            r"PRIMARY KEY\s*\(\s*\(\s*egroup_id\s*\)\s*,\s*node\s*\)",
            "dfs_egroup_access is not keyed per observing node, so two nodes' flushes "
            "overwrite each other")

    def test_no_counter_columns(self):
        """A counter would be the obvious type and is the wrong one.

        CQL counters are not idempotent under retry -- a timed-out write may be applied
        twice -- and a counter table cannot carry the timestamp columns beside them. The
        design writes whole totals instead, which is what makes a flush safe to fire and
        forget.
        """
        self.assertNotIn(" counter", self.ddl.lower())

    def test_every_count_has_a_window_to_be_read_over(self):
        """A total with no window is uninterpretable.

        A hundred reads means something different over a minute than over a week, and two
        extent groups may have been known to the daemon for different lengths of time. Both
        ends of the window are per row for exactly that reason.
        """
        for column in ("since_ms", "updated_at_ms", "last_read_ms", "last_write_ms"):
            self.assertIn(column, self.ddl)

    def test_the_description_names_the_invariants_it_serves(self):
        """metadata.md section 7: any migration touching dfs_* states which invariant it
        serves or preserves. This one holds no references at all, which is the reason it is
        safe for Purah to ignore."""
        description = self.migration.get("description") or ""
        self.assertTrue(
            re.search(r"\bI-\d\b", description),
            "a dfs_* migration must say which invariant it serves or preserves")


class TheReadPathNeverWaitsOnHydra(unittest.TestCase):
    """The rule the whole design does not bend, in the direction nobody checks."""

    @classmethod
    def setUpClass(cls):
        cls.vdisk = read(VDISK_RS)
        cls.read_fn = rust_fn(cls.vdisk, "read")

    def test_a_read_records_the_extent_group_it_served_from(self):
        self.assertIn("record_read", self.read_fn)

    def test_a_read_issues_no_metadata_statement(self):
        """The failure this guards is not a wrong number, it is a slow one.

        Recording an access is the natural place to put a metadata write, and a write there
        would be correct, auditable, exact -- and would make every guest read wait on a
        Paxos-adjacent round trip to Hydra. The counters live in memory precisely so this
        line stays true.
        """
        for forbidden in ("self.daruk", "INSERT INTO", "UPDATE hydra", "SELECT "):
            self.assertNotIn(
                forbidden, self.read_fn,
                "Vdisk::read issues %r, which puts a metadata operation on the guest's "
                "read path" % forbidden)

    def test_a_guest_write_records_nothing_against_an_extent_group(self):
        """An extent group never sees a guest write.

        The write reaches the journal, is replicated and is acknowledged; no extent group is
        touched. Counting a write here would attribute an append to whichever group happened
        to be open and make the one number that identifies still-being-appended-to groups
        mean nothing.
        """
        write_fn = rust_fn(self.vdisk, "write")
        self.assertNotIn("record_write", write_fn)

    def test_the_drain_is_what_counts_a_write(self):
        drain_fn = rust_fn(self.vdisk, "drain")
        self.assertIn("record_write", drain_fn)


class TheFlushIsIdempotent(unittest.TestCase):
    """A write nobody waits on must be safe to lose and safe to repeat."""

    @classmethod
    def setUpClass(cls):
        cls.meta = read(META_RS)
        cls.batches = rust_fn(cls.meta, "access_batches")

    def test_the_flush_writes_whole_totals(self):
        self.assertIn("INSERT INTO hydra.dfs_egroup_access", self.batches)

    def test_the_flush_never_increments(self):
        """`reads = reads + n` is the shape that makes a retry a double count.

        Nothing waits on this write, so it will be retried by the next tick whether or not
        the last one landed. Absolute values are what make that free.
        """
        self.assertNotRegex(self.batches, r"reads\s*=\s*reads\s*\+")
        self.assertNotRegex(self.batches, r"writes\s*=\s*writes\s*\+")

    def test_the_flush_names_the_node_that_observed_the_accesses(self):
        """The partition is keyed by it. A flush that omitted it would write some other
        node's row."""
        self.assertIn("node", self.batches)
        self.assertRegex(self.batches, r"cql_str\(node\)")

    def test_sampling_the_tally_does_not_empty_it(self):
        """A destructive sample turns one unreachable Hydra into a permanent hole.

        The totals are absolute, so the next flush carries the same numbers plus whatever
        arrived since -- but only if taking them did not take them away.
        """
        sample_fn = rust_fn(read(HEAT_RS), "sample")
        self.assertNotIn(".drain(", sample_fn)
        self.assertNotIn(".clear()", sample_fn)
        self.assertNotIn("take(", sample_fn)


class ReclamationTakesTheAccessDataWithIt(unittest.TestCase):
    """A table of facts about extent groups must not fill with facts about dead ones."""

    @classmethod
    def setUpClass(cls):
        cls.reclaim = rust_fn(read(PURAH_RS), "reclaim")

    def test_the_whole_partition_goes(self):
        """Every node's row, not this node's: the group is gone everywhere."""
        self.assertIn("DELETE FROM hydra.dfs_egroup_access", self.reclaim)
        self.assertNotIn("dfs_egroup_access WHERE egroup_id = {} AND node", self.reclaim)

    def test_the_in_memory_entry_goes_too(self):
        """Otherwise the id keeps being flushed for the life of the daemon, and the row it
        describes comes straight back after being deleted."""
        self.assertIn("self.access.forget(", self.reclaim)

    def test_access_data_cannot_block_reclaiming_disk(self):
        """Deleted last and best-effort. A leftover access row is a stale label that the
        ranking pass ignores because no inventory lists it; failing the reclaim over one
        would let a statistic stop disk from being freed."""
        self.assertLess(
            self.reclaim.index("DELETE FROM hydra.dfs_egroups "),
            self.reclaim.index("DELETE FROM hydra.dfs_egroup_access"),
            "the access row is deleted before the row the extent group is described by")
        tail = self.reclaim[self.reclaim.index("dfs_egroup_access"):]
        self.assertNotRegex(
            tail.split("\n")[0], r"\)\?",
            "a failed access-data delete propagates and aborts the reclaim")


class TheRankingOnlyReports(unittest.TestCase):
    """Measure first, move later. The order is the whole reason it can be trusted."""

    @classmethod
    def setUpClass(cls):
        cls.purah = read(PURAH_RS)
        cls.heat = rust_fn(cls.purah, "heat")

    def test_nothing_in_the_ranking_pass_touches_a_byte(self):
        """A curator that started migrating extent groups the moment it could measure
        temperature would be acting on a ranking nobody had read."""
        for forbidden in ("remove_file", "copy", "rename", "DELETE FROM", "egroup-state"):
            self.assertNotIn(
                forbidden, self.heat,
                "the heat pass performs %r; it is a report and must stay one" % forbidden)

    def test_a_truncated_list_of_never_touched_groups_reports_its_real_size(self):
        """Three thousand unmeasured extent groups and three are different findings, and a
        list cut to a limit cannot tell them apart on its own."""
        self.assertIn("unobserved_count", self.heat)

    def test_the_ranking_carries_the_counters_it_was_computed_from(self):
        """A ranking used to argue for moving data has to be checkable by hand. Every term
        of the score is in the row."""
        for column in ("reads", "writes", "idle_ms", "window_ms", "heat"):
            self.assertIn('"%s"' % column, self.heat)

    def test_groups_with_no_data_are_reported_apart_from_cold_ones(self):
        """Measured and cold is a fact about the workload. Never measured is a fact about
        the measurement, and spilling a disk on the strength of the second while believing
        it was the first is the mistake available here."""
        self.assertIn("unobserved", self.heat)
        self.assertIn("cold", self.heat)


class TheTallyIsReachable(unittest.TestCase):
    """An operator who cannot see it cannot act on it, and three layers have to agree."""

    def test_sidon_dispatches_the_op(self):
        self.assertIn('"purah-heat" =>', read(CONTROL_RS))

    def test_the_flush_has_its_own_timer(self):
        """Separate from the sweep's. The sweep is minutes because reclaiming early costs
        data; the only thing a slow flush buys is a larger window of counts to lose."""
        main = read(MAIN_RS)
        self.assertIn("SIDON_ACCESS_FLUSH", main)
        self.assertIn("access_flush", main)
        control = read(CONTROL_RS)
        self.assertIn("cfg.access_flush", control)

    def test_turning_the_tally_off_is_honest_about_the_consequence(self):
        """Zero means zero, and it means the ranking has nothing to rank. Silently
        substituting a default would make an explicit setting do something other than what
        it says."""
        control = read(CONTROL_RS)
        self.assertIn("flush_every.is_zero()", control)

    def test_the_cli_and_the_helper_both_expose_it(self):
        valcli = read(VALCLI)
        self.assertIn("storage.heat", valcli)
        self.assertIn("def cmd_storage_heat(", valcli)
        self.assertIn('"op": "purah-heat"', valcli)
        self.assertIn('call("purah-heat"', read(SIDON_PY))

    def test_the_cli_asks_the_daemon_rather_than_scoring_the_table_itself(self):
        """A second implementation of the score would be a second answer to "is this hot",
        and the two would disagree in exactly the situation somebody is using them to
        settle. The formula lives in Purah."""
        cli = read(VALCLI)
        body = cli[cli.index("def cmd_storage_heat("):cli.index("def _print_heat_rows(")]
        self.assertIn('"op": "purah-heat"', body)
        self.assertNotIn("run_cql_query", body,
                         "valcli reads the access table itself instead of asking Purah, "
                         "which is a second answer to whether an extent group is hot")


class TheApproximationIsWrittenDown(unittest.TestCase):
    """The boundary is a sentence in a document, and nothing else enforces it."""

    def test_the_metadata_document_says_what_a_crash_loses(self):
        metadata = read(METADATA_MD)
        self.assertIn("dfs_egroup_access", metadata)
        self.assertIn("approximate", metadata.lower())

    def test_the_boundary_on_what_approximate_counters_may_decide_is_recorded(self):
        """Where a copy goes, never whether it exists. A lost flush must never be able to
        cost data, and the only thing standing between those two uses is this being
        stated."""
        for path in (METADATA_MD, DECISIONS_MD):
            # Emphasis markers stripped: the sentence matters, not how it is typeset.
            text = " ".join(re.sub(r"[*_]", "", read(path)).split())
            self.assertRegex(
                text, r"never whether (it|a copy|the copy) exists",
                "%s does not record that approximate access data may decide placement "
                "and not durability" % os.path.basename(path))

    def test_the_decision_is_in_the_adr_list_with_its_alternatives(self):
        decisions = read(DECISIONS_MD)
        self.assertIn("**D-22", decisions)
        self.assertIn("counter", decisions)
        self.assertIn("footer", decisions)

    def test_the_tiering_design_records_that_its_input_now_exists(self):
        """The document is the specification. A prerequisite that was filled without the
        design saying so leaves the next reader believing the work is still blocked."""
        multi = read(MULTI_DISK_MD)
        self.assertIn("dfs_egroup_access", multi)
        self.assertIn("storage.heat", multi)


class TheExtentIdMapIsReservedRatherThanHalfBuilt(unittest.TestCase):
    """The missing middle level is designed, and deliberately not created.

    `dfs_block_map` points straight at an extent group where Nutanix points at an extent
    first, which is why a clone shares groups wholesale and cannot diverge one extent at a
    time. The schema and the staged plan are D-23 and the migration id is reserved.

    The table is not created, and that is the decision rather than the unfinished part.
    `multi_disk.md` already records what an empty table with a suggestive shape costs: the
    one that existed, `dfs_egroup_replicas` (dropped since, migration 0025), had nothing
    writing to it, and every design that came afterwards had to open by establishing that it
    is not a source of truth.

    The hazard the flag would not have covered is the reason this is not landed behind one:
    Purah marks from `dfs_block_map.egroup_id`, and the moment any row names an extent
    instead, a sweep that still marked from that column sees the live groups as
    unreferenced. The flag protects the read path; the curator runs on a timer whether
    anyone opted in or not.
    """

    def test_the_migration_id_is_reserved_and_unused(self):
        import helios_schema

        ids = [m["id"] for m in helios_schema.MIGRATIONS]
        self.assertFalse([i for i in ids if "extent-id-map" in i],
                         "the extent id map migration exists before its mark phase does")
        statements = " ".join(
            " ".join(m["statements"]) for m in helios_schema.MIGRATIONS)
        self.assertNotIn("dfs_extent_id_map", statements)

    def test_nothing_resolves_a_read_through_an_extent_id(self):
        """The read path is byte-for-byte what it was. A two-level map and a half-wired
        three-level one are not two points on a spectrum."""
        for path in (VDISK_RS, CONTROL_RS, PURAH_RS):
            self.assertNotIn("extent_id", read(path),
                             "%s references an extent id that no table holds"
                             % os.path.basename(path))

    def test_the_design_and_the_reason_it_waits_are_recorded(self):
        decisions = read(DECISIONS_MD)
        self.assertIn("**D-23", decisions)
        self.assertIn("next free migration id", decisions,
                      "D-23 reserves a number again; the last one was taken by something else")
        self.assertIn("dfs_extent_id_map", decisions)
        # The two things the next person has to know before touching it: that existing
        # vdisks keep the two-level path, and that the curator is the part a read-path flag
        # does not protect.
        self.assertIn("Null means the two-level", decisions)
        self.assertIn("mark phase", decisions)

    def test_the_case_against_dedup_is_recorded_rather_than_deferred(self):
        """Not "later": argued. The win on VM disks is identical OS images, which
        clone-from-image already gets as a map copy."""
        decisions = read(DECISIONS_MD)
        self.assertIn("Dedup is not being built", decisions)
        self.assertIn("clone-from-image", decisions)


if __name__ == "__main__":
    unittest.main()
