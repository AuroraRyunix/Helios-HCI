defmodule SpectrumPhx.HealthTest do
  use ExUnit.Case, async: true

  alias SpectrumPhx.Health

  defp result(attrs) do
    Map.merge(
      %{
        "category" => "all",
        "check_name" => "cpu_load",
        "node_ip" => "10.10.0.11",
        "status" => "PASS",
        "output" => "load average 0.4",
        "execution_id" => "3f1c9d5e-0000-0000-0000-000000000001",
        "timestamp" => ~U[2026-08-19 10:00:00Z]
      },
      attrs
    )
  end

  defp fetch(opts) do
    # Every read this module makes is stubbed, so no test reaches for a database that a
    # dev machine does not have.
    Health.fetch(Keyword.merge([results: [], schedules: [], runs: []], opts))
  end

  defp categories_of(snapshot) do
    Map.new(snapshot.categories, fn category ->
      {category.key, Enum.map(category.checks, & &1.check)}
    end)
  end

  describe "category derivation" do
    test "groups by what the check examines, not by the scope it was invoked with" do
      # Every row here says category = "all", which is what `mcli health_checks run_all`
      # writes and therefore what the hourly job produces for the entire table.
      rows = [
        result(%{"check_name" => "zookeeper_status"}),
        result(%{"check_name" => "aether_split_brain"}),
        result(%{"check_name" => "ram_usage"})
      ]

      assert %{
               services: ["zookeeper_status"],
               storage: ["aether_split_brain"],
               hardware: ["ram_usage"]
             } =
               categories_of(fetch(results: rows, schedules: [], runs: []))
    end

    test "follows mcli's own lists where the old page's JavaScript had drifted from them" do
      # `orphaned_disks_check` and `broken_disks_check` are storage checks in mcli's
      # checks_map but fell through getCheckCategory's else branch into Services.
      assert Health.category_for("replica_health") == :storage
      # And the names a cluster upgraded from the DRBD era still has rows for, so a
      # historical row is shown where it belongs rather than filed under :other.
      assert Health.category_for("drbd_split_brain_check") == :storage
      assert Health.category_for("broken_disks_check") == :storage

      # These are service checks in mcli but were listed as hardware in app.js.
      assert Health.category_for("hostname_resolution") == :services
      assert Health.category_for("mtls_cert_expiration") == :services
      assert Health.category_for("security_config_audit") == :services
      assert Health.category_for("spectrum_privilege_check") == :services
    end

    test "groups the checks that mcli gained after these lists were first copied" do
      # Added to mcli's checks_map after this module was written, so each was :other here
      # -- and `hylia_status` had never reached either console at all: mcli-runner builds
      # the name as results[f"{svc}_status"], it had no CHECK_ID_TO_FUNC entry, and every
      # run wrote it to the invoked scope's partition for the legacy cleanup to delete
      # seconds later.
      assert Health.category_for("watchdog_daemon_status") == :services
      assert Health.category_for("drs_storage_capacity_check") == :services
      assert Health.category_for("migration_lock_status") == :services
      assert Health.category_for("hylia_status") == :services
      assert Health.category_for("sidon_latency_check") == :storage
      assert Health.category_for("linstor_latency_check") == :storage
    end

    test "a check in neither list is grouped as other rather than silently as a service" do
      assert Health.category_for("some_new_check") == :other

      snapshot = fetch(results: [result(%{"check_name" => "some_new_check"})])
      assert %{other: ["some_new_check"]} = categories_of(snapshot)
    end

    test "keeps the stored category as the run's scope" do
      snapshot = fetch(results: [result(%{"category" => "storage"})])
      assert [check] = snapshot.categories |> List.first() |> Map.fetch!(:checks)
      assert check.run_scope == "storage"
    end
  end

  describe "duplicate rows" do
    test "keeps the newest row for a check and node, and says how many it dropped" do
      rows = [
        result(%{
          "category" => "all",
          "check_name" => "disk_space",
          "status" => "FAIL",
          "timestamp" => ~U[2026-08-19 09:00:00Z]
        }),
        result(%{
          "category" => "hardware",
          "check_name" => "disk_space",
          "status" => "PASS",
          "timestamp" => ~U[2026-08-19 11:00:00Z]
        })
      ]

      snapshot = fetch(results: rows)

      assert snapshot.duplicates == 1
      assert snapshot.summary.total == 1
      assert snapshot.summary.pass == 1
      assert snapshot.summary.fail == 0
    end

    test "the same check on two different nodes is not a duplicate" do
      rows = [
        result(%{"node_ip" => "10.10.0.11"}),
        result(%{"node_ip" => "10.10.0.12"})
      ]

      snapshot = fetch(results: rows)

      assert snapshot.duplicates == 0
      assert snapshot.summary.total == 2
      assert snapshot.summary.nodes == 2
    end
  end

  describe "nothing unproven is healthy" do
    test "an empty table is 'not run', not 'passing'" do
      snapshot = fetch(results: [], schedules: [], runs: [])

      assert snapshot.available?
      assert snapshot.summary.state == :none
      assert snapshot.summary.total == 0
      assert snapshot.categories == []
    end

    test "an unreadable table is unavailable, and is not an empty one" do
      # Deliberately not the stubbed helper: this is the live path with a failing read.
      snapshot = Health.fetch(error: :econnrefused)

      refute snapshot.available?
      assert snapshot.error =~ "econnrefused"
      assert snapshot.summary.state == :none
    end

    test "a status Mimir does not define is unknown, and is not counted as a pass" do
      rows = [
        result(%{"check_name" => "cpu_load", "status" => "SKIPPED"}),
        result(%{"check_name" => "ram_usage", "status" => nil})
      ]

      snapshot = fetch(results: rows)

      assert snapshot.summary.unknown == 2
      assert snapshot.summary.pass == 0
      assert snapshot.summary.state == :warn
      assert length(snapshot.failing) == 2
    end

    test "any failure makes the whole cluster state fail" do
      rows = [
        result(%{"check_name" => "cpu_load", "status" => "PASS"}),
        result(%{"check_name" => "ram_usage", "status" => "WARN"}),
        result(%{"check_name" => "disk_space", "status" => "FAIL"})
      ]

      summary = fetch(results: rows).summary

      assert summary.state == :fail
      assert summary.pass == 1
      assert summary.warn == 1
      assert summary.fail == 1
    end

    test "everything passing is the only way to reach a passing state" do
      rows = [result(%{"check_name" => "cpu_load"}), result(%{"check_name" => "ram_usage"})]
      assert fetch(results: rows).summary.state == :pass
    end
  end

  describe "failures first" do
    test "the failing list carries fails, warns and unknowns, worst first" do
      rows = [
        result(%{"check_name" => "cpu_load", "status" => "PASS"}),
        result(%{"check_name" => "ram_usage", "status" => "WARN"}),
        result(%{"check_name" => "disk_space", "status" => "FAIL"}),
        result(%{"check_name" => "ntp_sync", "status" => "SKIPPED"})
      ]

      failing = fetch(results: rows).failing

      assert Enum.map(failing, & &1.check) == ~w(disk_space ram_usage ntp_sync)
      refute "cpu_load" in Enum.map(failing, & &1.check)
    end

    test "checks inside a category are ordered by severity too" do
      rows = [
        result(%{"check_name" => "cpu_load", "status" => "PASS"}),
        result(%{"check_name" => "disk_space", "status" => "FAIL"})
      ]

      assert %{hardware: ["disk_space", "cpu_load"]} = categories_of(fetch(results: rows))
    end
  end

  describe "presentation fields" do
    test "resolves a hostname, an age and a readable label for each check" do
      at = DateTime.add(DateTime.utc_now(), -300, :second)
      snapshot = fetch(results: [result(%{"timestamp" => at})])

      assert [check] = snapshot.categories |> List.first() |> Map.fetch!(:checks)
      assert check.label == "Cpu Load"
      assert check.hostname == "10.10.0.11"
      assert check.age_seconds >= 300
      assert check.message == "load average 0.4"
    end
  end

  describe "the Dagur scheduler" do
    test "parses schedules and treats a never-dispatched job as never, not as 1970" do
      rows = [
        %{
          "job_name" => "mimir_diagnostics",
          "task_type" => "mimir_health",
          "cron_expression" => "0 * * * *",
          "interval_seconds" => 3600,
          "enabled" => true,
          "last_run_epoch" => 0,
          "command" => "/usr/local/bin/mcli health_checks run_all"
        }
      ]

      snapshot = fetch(results: [], schedules: rows, runs: [])

      assert snapshot.scheduler_available?
      assert [schedule] = snapshot.schedules
      assert schedule.job == "mimir_diagnostics"
      assert schedule.enabled?
      assert schedule.last_run == nil
      refute schedule.overdue?
    end

    test "flags a job that has not been dispatched for more than twice its interval" do
      stale = DateTime.utc_now() |> DateTime.add(-10_000, :second) |> DateTime.to_unix()

      rows = [
        %{"job_name" => "storage_scrub", "interval_seconds" => 3600, "last_run_epoch" => stale},
        %{
          "job_name" => "db_compaction",
          "interval_seconds" => 43_200,
          "last_run_epoch" => DateTime.to_unix(DateTime.utc_now())
        }
      ]

      snapshot = fetch(results: [], schedules: rows, runs: [])
      overdue = Map.new(snapshot.schedules, fn schedule -> {schedule.job, schedule.overdue?} end)

      assert overdue["storage_scrub"]
      refute overdue["db_compaction"]
    end

    test "a run stuck in RUNNING is neither a success nor a failure" do
      rows = [
        %{
          "job_name" => "storage_scrub",
          "start_time" => ~U[2026-08-19 06:00:00Z],
          "end_time" => nil,
          "status" => "RUNNING",
          "exit_code" => -1,
          "run_id" => "aaaa1111-0000-0000-0000-000000000000",
          "output" => "Job started..."
        },
        %{
          "job_name" => "db_compaction",
          "start_time" => ~U[2026-08-19 08:00:00Z],
          "end_time" => ~U[2026-08-19 08:00:12Z],
          "status" => "FAILED",
          "exit_code" => 1,
          "run_id" => "bbbb2222-0000-0000-0000-000000000000",
          "output" => "nodetool: connection refused"
        }
      ]

      snapshot = fetch(results: [], schedules: [], runs: rows)
      by_job = Map.new(snapshot.runs, fn run -> {run.job, run} end)

      assert by_job["storage_scrub"].severity == :running
      assert by_job["storage_scrub"].duration_seconds == nil
      assert by_job["db_compaction"].severity == :failed
      assert by_job["db_compaction"].duration_seconds == 12
      # Newest first.
      assert Enum.map(snapshot.runs, & &1.job) == ~w(db_compaction storage_scrub)
    end

    test "an unreadable scheduler is reported apart from the diagnostics themselves" do
      snapshot = Health.fetch(results: [result(%{})], error: :econnrefused)

      refute snapshot.scheduler_available?
      assert snapshot.scheduler_error =~ "econnrefused"
    end
  end

  describe "statements" do
    test "read named columns, and one job partition at a time" do
      assert Health.results_cql() =~ "FROM hydra.mimir_results"
      refute Health.results_cql() =~ "JSON"
      assert Health.runs_cql() =~ "WHERE job_name = ?"
      assert Health.runs_cql() =~ "LIMIT ?"
    end
  end
end
