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
RECLAIM_RS = os.path.join(HERE, "sidon", "src", "purah", "reclaim.rs")
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
        # The drain's work is `DrainJob::run`: the drain runs on its own thread from a job
        # that owns everything it needs, so the loop that appends extents -- and counts them
        # -- lives there rather than in `Vdisk::drain`, which only plans, runs and finishes it.
        drain_fn = rust_fn(self.vdisk, "run")
        self.assertIn("append_framed", drain_fn)
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
        cls.reclaim = rust_fn(read(RECLAIM_RS), "forget_group")

    def test_the_whole_partition_goes(self):
        """Every node's row, not this node's: the group is gone everywhere."""
        self.assertIn("DELETE FROM hydra.dfs_egroup_access", self.reclaim)
        self.assertNotIn("dfs_egroup_access WHERE egroup_id = {} AND node", self.reclaim)

    def test_the_in_memory_entry_goes_too(self):
        """Otherwise the id keeps being flushed for the life of the daemon, and the row it
        describes comes straight back after being deleted."""
        self.assertIn("p.access.forget(", self.reclaim)

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


class TheExtentIdMapIsStagedSoTheMarkPhaseComesFirst(unittest.TestCase):
    """The middle level is built in an order, and the order is what stops it deleting data.

    `dfs_block_map` points straight at an extent group where Nutanix points at an extent
    first. D-23 adds that level: `dfs_extent_id_map`, and a nullable `extent_id` on the block
    map. The first row that names an extent instead of a group would make a sweep that only
    reads `egroup_id` see every group behind it as unreferenced, and a flag on the read path
    does nothing about that because the sweep is not on the read path. So Purah has to learn
    both levels *before* anything can write the second one, and these tests pin the pieces of
    that ordering that code can pin:

      * the mark phase reads the extent level (stage 1), and falls back to the statement it
        always issued when the ledger says the column does not exist yet;
      * the migrations exist under the assigned ids, one bare ALTER, and the Rust and Python
        sides agree on the ids, because the mark phase decides which statement to send by
        looking for them in the ledger;
      * **nothing writes an extent id**, so no vdisk can be on the three-level path before
        the rollout document says it may be.
    """

    EXTENT_MAP_RS = os.path.join(HERE, "sidon", "src", "extent_id_map.rs")
    RESOLVE_RS = os.path.join(HERE, "sidon", "src", "extent_resolve.rs")
    ROLLOUT_MD = os.path.join(HERE, "docs", "dfs", "extent_id_map.md")

    def test_the_migration_ids_are_the_assigned_ones_and_rust_agrees(self):
        import helios_schema

        ids = [m["id"] for m in helios_schema.MIGRATIONS]
        self.assertIn("0020-dfs-extent-id-map", ids)
        self.assertIn("0021-dfs-block-map-extent-id", ids)
        source = read(self.EXTENT_MAP_RS)
        for rust_name, wanted in (("TABLE_MIGRATION", "0020-dfs-extent-id-map"),
                                  ("COLUMN_MIGRATION", "0021-dfs-block-map-extent-id")):
            match = re.search(r'pub const %s: &str = "([^"]+)";' % rust_name, source)
            self.assertTrue(match, rust_name)
            self.assertEqual(match.group(1), wanted,
                             "the mark phase looks for an id the schema does not declare, "
                             "which would make it read the old column forever")

    def test_the_column_is_one_bare_alter_and_the_table_is_create_if_not_exists(self):
        import helios_schema

        by_id = {m["id"]: m for m in helios_schema.MIGRATIONS}
        column = by_id["0021-dfs-block-map-extent-id"]["statements"]
        self.assertEqual(column, ["ALTER TABLE hydra.dfs_block_map ADD extent_id text;"])
        table = by_id["0020-dfs-extent-id-map"]["statements"]
        self.assertEqual(len(table), 1)
        self.assertIn("CREATE TABLE IF NOT EXISTS hydra.dfs_extent_id_map", table[0])
        for column_name in ("extent_id text PRIMARY KEY", "egroup_id", "egroup_offset",
                            "length", "vdisk_hash", "created_at_ms"):
            self.assertIn(column_name, table[0])
        # No refcount, now or ever (D-8).
        self.assertNotIn("refcount", table[0].lower())

    def test_the_migrations_rewrite_no_existing_row(self):
        """A column and a table. A backfill would turn a null `extent_id` -- the two-level
        path every existing vdisk uses -- into something else."""
        import helios_schema

        by_id = {m["id"]: m for m in helios_schema.MIGRATIONS}
        for mid in ("0020-dfs-extent-id-map", "0021-dfs-block-map-extent-id"):
            self.assertNotIn("backfill", by_id[mid], mid)

    def test_the_mark_phase_goes_through_the_extent_map_and_not_only_the_old_column(self):
        sweep = rust_fn(read(RECLAIM_RS), "sweep_pass")
        self.assertIn("extent_id_map::referenced_egroups", sweep)
        for path in (PURAH_RS, RECLAIM_RS):
            self.assertNotIn("SELECT egroup_id FROM hydra.dfs_block_map", read(path),
                             "Purah scans the old column on its own again, bypassing the extent level")
        source = read(self.EXTENT_MAP_RS)
        # The statement every cluster has always been sent, byte for byte.
        self.assertIn('"SELECT egroup_id FROM hydra.dfs_block_map"', source)
        self.assertIn("SELECT egroup_id, extent_id FROM hydra.dfs_block_map", source)
        self.assertIn("hydra.dfs_extent_id_map", source)

    def test_a_mark_phase_that_cannot_follow_an_extent_aborts_instead_of_skipping(self):
        """The property the whole ordering exists for, stated on the source because the Rust
        test that proves it cannot run here: an extent that cannot be followed is an error."""
        source = read(self.EXTENT_MAP_RS)
        self.assertIn("marked_groups_are_exactly_those_reachable_through_either_level", source)
        self.assertIn("an_extent_that_cannot_be_followed_aborts_rather_than_being_skipped", source)
        self.assertIn("a_failed_extent_map_scan_never_yields_a_partial_answer", source)

    def test_nothing_writes_an_extent_id(self):
        """The writer is what stays off. Every statement that writes the block map, or the
        extent map, lives in meta.rs/vdisk.rs/control.rs, so none of them may name either."""
        for path in (META_RS, VDISK_RS, CONTROL_RS, PURAH_RS, self.EXTENT_MAP_RS,
                     self.RESOLVE_RS):
            source = read(path)
            self.assertNotRegex(source, r"INSERT INTO hydra\.dfs_extent_id_map",
                                "%s writes the extent map" % os.path.basename(path))
            self.assertNotRegex(source, r"UPDATE hydra\.dfs_extent_id_map",
                                "%s writes the extent map" % os.path.basename(path))
        batches = rust_fn(read(META_RS), "block_map_batches")
        self.assertNotIn("extent_id", batches,
                         "the drain's block-map writer names the column; that is stage 3, "
                         "and it breaks every node that has not applied 0021")

    def test_a_vdisk_with_no_extent_rows_issues_exactly_the_statement_it_always_did(self):
        load = rust_fn(read(VDISK_RS), "load_map")
        self.assertIn("SELECT extent_index, egroup_id, egroup_offset, length, vdisk_hash "
                      "FROM hydra.dfs_block_map", load)
        # The extent level is reached only for a row that has no egroup_id.
        self.assertIn("by_extent", load)
        self.assertIn("if !by_extent.is_empty()", load)

    def test_the_rollout_says_stage_one_is_the_one_to_deploy_first_and_let_soak(self):
        text = read(self.ROLLOUT_MD)
        self.assertIn("Stage 1", text)
        self.assertIn("the one to roll out first", text)
        self.assertIn("full sweep cycle", text)
        self.assertIn("restart sidon", text)
        for term in ("0020", "0021", "purah: sweep failed", "dfs_extent_id_map"):
            self.assertIn(term, text)

    def test_the_design_and_the_reason_for_the_order_are_recorded(self):
        decisions = read(DECISIONS_MD)
        self.assertIn("**D-23", decisions)
        self.assertIn("0020-dfs-extent-id-map", decisions)
        self.assertIn("0021-dfs-block-map-extent-id", decisions)
        # The two things the next person has to know before touching it: that existing
        # vdisks keep the two-level path, and that the curator is the part a read-path flag
        # does not protect.
        self.assertIn("Null `extent_id` means the two-level path", decisions)
        self.assertIn("mark phase", decisions)
        self.assertIn("full sweep", decisions)

    def test_dedup_is_costed_and_not_implemented(self):
        """The case against dedup is recorded and so is the addendum that revisits it. What
        is not allowed to exist is a content hash anywhere in the tree."""
        decisions = read(DECISIONS_MD)
        self.assertIn("Dedup is not being built", decisions)
        self.assertIn("clone-from-image", decisions)
        self.assertIn("D-23 addendum", decisions)
        for cost in ("Write amplification", "Memory", "Garbage collection", "resurrection",
                     "Recommendation", "estimator"):
            self.assertIn(cost, decisions)
        for path in (self.EXTENT_MAP_RS, self.RESOLVE_RS, META_RS, VDISK_RS, PURAH_RS):
            self.assertNotRegex(read(path), r"(?i)sha256|blake3|content_hash|by_hash",
                                "%s hashes content; dedup is a recommendation, not code"
                                % os.path.basename(path))

    def test_the_new_document_is_linked_from_the_indexes(self):
        for index in (os.path.join(HERE, "README.md"),
                      os.path.join(HERE, "docs", "README.md"),
                      os.path.join(HERE, "docs", "dfs", "README.md")):
            self.assertIn("extent_id_map.md", read(index), index)


if __name__ == "__main__":
    unittest.main()
