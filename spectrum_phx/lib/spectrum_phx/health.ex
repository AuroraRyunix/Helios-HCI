defmodule SpectrumPhx.Health do
  @moduledoc """
  Mimir diagnostic results from `hydra.mimir_results`, plus the Dagur scheduler that runs
  them (`hydra.dagur_schedules`, `hydra.dagur_runs`).

  ## The category column does not mean what the page needs

  `mimir_results` is `PRIMARY KEY (category, check_name, node_ip)`, and `category` holds
  the *invocation scope* `mcli` was called with -- not the kind of check. The hourly
  `mimir_diagnostics` job runs `mcli health_checks run_all`, and `mcli` writes the literal
  string `all` into `category` for every row of that run. So in normal operation every row
  in the table says `all`, and grouping by that column produces one bucket.

  The old page worked around this by ignoring the column entirely and re-deriving the
  category from a hardcoded list of check names in `app.js`. That list had drifted from
  `mcli`'s own `checks_map`: `drbd_split_brain_check` and `broken_disks_check` are storage
  checks in `mcli` but fell through `getCheckCategory`'s `else` branch into the Services
  grid, and seven service checks (`hostname_resolution`, `mtls_cert_expiration`,
  `security_config_audit`, `auth_seeding_check`, `maintenance_mode_check`,
  `libvirt_responsiveness`, `spectrum_privilege_check`) were listed as hardware.

  This module also derives the category from the check name -- there is no other source --
  but from `mcli`'s `checks_map` verbatim, and a check that is in neither list is grouped
  as `:other` instead of silently becoming a service. The stored `category` is kept and
  shown as the run's scope, which is the only thing it actually records.

  ## The same check can be in the table twice

  Because `category` is the partition key, running `mcli health_checks storage` after a
  `run_all` writes a *second* row for every storage check -- same check, same node, a
  different partition -- and nothing removes the first. Reads therefore deduplicate on
  `{check_name, node_ip}`, keeping the newest by timestamp, and report how many older
  copies were dropped.

  ## Nothing unrecognised is healthy

  `mcli` writes `PASS`, `WARN` and `FAIL`, and `Host Connectivity` / `JSON Parsing` rows
  when a node could not be reached or answered with something unparseable. Any status
  outside those three is `:unknown` here and is counted apart from `pass`. An empty table
  is `:none`, not "healthy" -- diagnostics that never ran say nothing about the cluster.

  ## Test seam

  `fetch/1` accepts `:results`, `:schedules`, `:runs` and `:error` directly.
  `Application.get_env(:spectrum_phx, :health_source)` accepts
  `{:static, %{results: rows, schedules: rows, runs: rows}}` or `{:error, reason}` for
  tests that drive the LiveView through its real route.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Tasks

  @results_cql "SELECT category, check_name, node_ip, status, output, execution_id, " <>
                 "timestamp FROM hydra.mimir_results"

  @schedules_cql "SELECT job_name, task_type, cron_expression, interval_seconds, enabled, " <>
                   "last_run_epoch, command FROM hydra.dagur_schedules"

  # `dagur_runs` is `PRIMARY KEY (job_name, start_time)` clustered `start_time DESC`, so
  # the recent history of one job is a single-partition read. The old endpoint asked for
  # `LIMIT 100` with no `WHERE`, which is a full scan whose 100 rows are whatever the
  # coordinator happened to reach first -- not the 100 most recent.
  @runs_cql "SELECT job_name, start_time, run_id, end_time, status, exit_code, output " <>
              "FROM hydra.dagur_runs WHERE job_name = ? LIMIT ?"

  @runs_per_job 5
  @runs_shown 25

  @pubsub SpectrumPhx.PubSub
  @topic "health"

  # Verbatim from `mcli`'s `checks_map`, which is what actually decides which checks a
  # category runs. Anything not here is `:other`, deliberately.
  @service_checks ~w(
    zookeeper_status hydra-db_status daruk_status aether_status spectrum_status
    spark-daemon_status libvirtd_status catalyst_status bifrost_status dagur_status
    mimir_status vali_status gatoway_status urbosa_status logos_status mipha_status
    agahnim_status slate_status scylladb_ring_status zookeeper_consensus
    scylladb_replication spectrum_api_port mtls_cert_expiration ingress_cert_expiration
    certs_seeding_check slate_config_status libvirt_responsiveness hostname_resolution
    spectrum_privilege_check auth_seeding_check vip_binding_status maintenance_mode_check
    vali_leader_status scylladb_tasks_schema security_config_audit virsh_power_off_check
    stuck_tasks_check urbosa_compliance ssh_known_hosts_seeding zookeeper_ring_scale
    scylladb_quorum_safety replication_factor_vs_repair mtls_cert_expiry_warning
    flapping_service_check stale_node_registration
  )

  @storage_checks ~w(
    aether_peers aether_volume aether_split_brain aether_heal_pending
    aether_storage_pools aether_storage_pools_space storage_capacity
    storage_mount_options storage_volume_writable fstab_safety_check
    orphaned_disks_check broken_disks_check drbd_split_brain_check
  )

  @hardware_checks ~w(
    cpu_load ram_usage disk_space ntp_sync host_virtualization firmware_upgrades
    dns_ntp_sync_check
  )

  @categories [
    {:services, "Core services"},
    {:storage, "Aether storage"},
    {:hardware, "Hardware"},
    {:other, "Other checks"}
  ]

  @doc "CQL used to read `mimir_results`."
  def results_cql, do: @results_cql

  @doc "CQL used to read `dagur_schedules`."
  def schedules_cql, do: @schedules_cql

  @doc "CQL used to read one job's partition of `dagur_runs`."
  def runs_cql, do: @runs_cql

  @doc "PubSub topic the dashboard subscribes to for pushed snapshots."
  def topic, do: @topic

  @doc "Subscribe the calling process to pushed health snapshots."
  def subscribe, do: Phoenix.PubSub.subscribe(@pubsub, @topic)

  @doc "Broadcast a snapshot to every connected dashboard as `{:health, snapshot}`."
  def broadcast(snapshot) do
    Phoenix.PubSub.broadcast(@pubsub, @topic, {:health, snapshot})
  rescue
    ArgumentError -> :ok
  catch
    :exit, _reason -> :ok
  end

  @doc """
  A full snapshot of cluster diagnostics.

      %{
        categories: [%{key: :services, label: "Core services", checks: [...], counts: %{}}],
        failing: [check],
        summary: %{state: :fail, pass: 40, warn: 1, fail: 2, unknown: 0, total: 43, ...},
        duplicates: 0,
        available?: true,
        error: nil,
        schedules: [schedule],
        runs: [run],
        scheduler_available?: true,
        scheduler_error: nil
      }

  Diagnostics and the scheduler are reported separately because they fail separately: the
  results table can be readable while the scheduler tables are not, and vice versa.

  Options: `:results`, `:schedules`, `:runs` (row lists), `:error` (fail the read).
  """
  def fetch(opts \\ []) do
    now = DateTime.utc_now()

    results =
      case results(opts) do
        {:ok, rows} -> assemble(rows, now)
        {:error, reason} -> unavailable(describe(reason))
      end

    Map.merge(results, scheduler(opts, now))
  end

  @doc """
  Where health reads come from: `:hydra` (the default), `{:static, map}` or
  `{:error, reason}`.
  """
  def source, do: Application.get_env(:spectrum_phx, :health_source, :hydra)

  @doc "The category a check name belongs to, from `mcli`'s own check lists."
  def category_for(name) when is_binary(name) do
    cond do
      name in @service_checks -> :services
      name in @storage_checks -> :storage
      name in @hardware_checks -> :hardware
      true -> :other
    end
  end

  def category_for(_name), do: :other

  @doc "Human label for a category key."
  def category_label(key) do
    case List.keyfind(@categories, key, 0) do
      {^key, label} -> label
      _other -> "Other checks"
    end
  end

  @doc "Severity for a raw Mimir status. Anything unrecognised is `:unknown`, not `:pass`."
  def severity("PASS"), do: :pass
  def severity("WARN"), do: :warn
  def severity("WARNING"), do: :warn
  def severity("FAIL"), do: :fail
  def severity("CRITICAL"), do: :fail
  def severity(_other), do: :unknown

  # -- sourcing --------------------------------------------------------------

  defp results(opts) do
    cond do
      Keyword.keyword?(opts) and Keyword.has_key?(opts, :results) ->
        {:ok, Keyword.fetch!(opts, :results)}

      Keyword.keyword?(opts) and Keyword.has_key?(opts, :error) ->
        {:error, Keyword.fetch!(opts, :error)}

      true ->
        case source() do
          {:static, %{} = data} -> {:ok, Map.get(data, :results, [])}
          {:error, reason} -> {:error, reason}
          _hydra -> safely(fn -> Hydra.query(@results_cql, []) end)
        end
    end
  end

  defp scheduler(opts, now) do
    case schedules(opts) do
      {:ok, rows} ->
        schedules = rows |> Enum.map(&schedule(&1, now)) |> Enum.sort_by(& &1.job)

        %{
          schedules: schedules,
          runs: runs(opts, schedules),
          scheduler_available?: true,
          scheduler_error: nil
        }

      {:error, reason} ->
        %{
          schedules: [],
          runs: [],
          scheduler_available?: false,
          scheduler_error: describe(reason)
        }
    end
  end

  defp schedules(opts) do
    cond do
      Keyword.keyword?(opts) and Keyword.has_key?(opts, :schedules) ->
        {:ok, Keyword.fetch!(opts, :schedules)}

      Keyword.keyword?(opts) and Keyword.has_key?(opts, :error) ->
        {:error, Keyword.fetch!(opts, :error)}

      true ->
        case source() do
          {:static, %{} = data} -> {:ok, Map.get(data, :schedules, [])}
          {:error, reason} -> {:error, reason}
          _hydra -> safely(fn -> Hydra.query(@schedules_cql, []) end)
        end
    end
  end

  defp runs(opts, schedules) do
    rows =
      cond do
        Keyword.keyword?(opts) and Keyword.has_key?(opts, :runs) ->
          Keyword.fetch!(opts, :runs)

        true ->
          case source() do
            {:static, %{} = data} -> Map.get(data, :runs, [])
            {:error, _reason} -> []
            _hydra -> read_runs(schedules)
          end
      end

    rows
    |> Enum.map(&run/1)
    |> Enum.sort_by(&unix(&1.started_at), :desc)
    |> Enum.take(@runs_shown)
  end

  defp read_runs([]), do: []

  defp read_runs(schedules) do
    schedules
    |> Task.async_stream(fn schedule -> read_job_runs(schedule.job) end,
      max_concurrency: max(length(schedules), 1),
      timeout: 15_000,
      on_timeout: :kill_task,
      ordered: false
    )
    |> Enum.flat_map(fn
      {:ok, rows} when is_list(rows) -> rows
      _other -> []
    end)
  end

  defp read_job_runs(job) do
    case safely(fn -> Hydra.query(@runs_cql, [job, @runs_per_job]) end) do
      {:ok, rows} -> rows
      {:error, _reason} -> []
    end
  end

  # A dev machine has no ScyllaDB. Xandra answers `{:error, _}` for a missing connection,
  # but a pool that is starting or wedged can exit instead, and an exit here would crash
  # the socket rather than render "the database is unreachable".
  defp safely(fun) do
    fun.()
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  # -- assembly --------------------------------------------------------------

  defp assemble(rows, now) do
    all = Enum.map(rows, &check(&1, now))
    {checks, duplicates} = deduplicate(all)

    %{
      categories: categorize(checks),
      failing: checks |> Enum.filter(&(&1.severity in [:fail, :warn, :unknown])) |> order(),
      summary: summarize(checks),
      duplicates: duplicates,
      available?: true,
      error: nil
    }
  end

  defp unavailable(reason) do
    %{
      categories: [],
      failing: [],
      summary: summarize([]),
      duplicates: 0,
      available?: false,
      error: reason
    }
  end

  defp check(row, now) do
    row = stringify(row)
    name = string(Map.get(row, "check_name")) || "unnamed check"
    status = row |> Map.get("status") |> normalize_status()
    node_ip = string(Map.get(row, "node_ip")) || "unknown"
    ran_at = Tasks.timestamp(Map.get(row, "timestamp"))
    category = category_for(name)

    %{
      check: name,
      label: humanize(name),
      category: category,
      category_label: category_label(category),
      run_scope: string(Map.get(row, "category")) || "unknown",
      node_ip: node_ip,
      hostname: hostname_for(node_ip),
      status: status,
      severity: severity(status),
      message: string(Map.get(row, "output")),
      execution_id: string(Map.get(row, "execution_id")),
      ran_at: ran_at,
      age_seconds: ran_at && max(DateTime.diff(now, ran_at, :second), 0)
    }
  end

  defp normalize_status(value) when is_binary(value) do
    case value |> String.trim() |> String.upcase() do
      "" -> "UNKNOWN"
      other -> other
    end
  end

  defp normalize_status(_value), do: "UNKNOWN"

  # Keep the newest row per {check, node}; count what was dropped. See the moduledoc: the
  # partition key makes older copies of a check survive a re-run under a narrower scope.
  defp deduplicate(checks) do
    kept =
      checks
      |> Enum.group_by(fn check -> {check.check, check.node_ip} end)
      |> Map.values()
      |> Enum.map(fn group -> Enum.max_by(group, &unix(&1.ran_at)) end)

    {kept, length(checks) - length(kept)}
  end

  defp categorize(checks) do
    grouped = Enum.group_by(checks, & &1.category)

    @categories
    |> Enum.map(fn {key, label} ->
      group = Map.get(grouped, key, [])
      %{key: key, label: label, checks: order(group), counts: counts(group)}
    end)
    |> Enum.reject(fn category -> category.checks == [] end)
  end

  # Failures first, then warnings, then unknowns, then passes; alphabetical within a
  # severity so a check keeps its place between refreshes.
  defp order(checks) do
    Enum.sort_by(checks, fn check -> {rank(check.severity), check.check, check.node_ip} end)
  end

  defp rank(:fail), do: 0
  defp rank(:warn), do: 1
  defp rank(:unknown), do: 2
  defp rank(_pass), do: 3

  defp counts(checks) do
    Enum.reduce(checks, %{pass: 0, warn: 0, fail: 0, unknown: 0, total: 0}, fn check, acc ->
      acc |> Map.update!(check.severity, &(&1 + 1)) |> Map.update!(:total, &(&1 + 1))
    end)
  end

  defp summarize(checks) do
    counts = counts(checks)

    counts
    |> Map.put(:nodes, checks |> Enum.map(& &1.node_ip) |> Enum.uniq() |> length())
    |> Map.put(:last_run, checks |> Enum.map(& &1.ran_at) |> latest())
    |> Map.put(:state, state(counts))
  end

  # No results is `:none`. It is not `:pass`: a table with nothing in it means the checks
  # have not run, which says nothing at all about the cluster's health, and drawing that
  # as green is exactly the lie this page exists to stop telling.
  defp state(%{total: 0}), do: :none
  defp state(%{fail: fail}) when fail > 0, do: :fail
  defp state(%{warn: warn}) when warn > 0, do: :warn
  defp state(%{unknown: unknown}) when unknown > 0, do: :warn
  defp state(_counts), do: :pass

  # -- scheduler -------------------------------------------------------------

  defp schedule(row, now) do
    row = stringify(row)
    last = epoch_seconds(Map.get(row, "last_run_epoch"))
    interval = integer(Map.get(row, "interval_seconds"))

    %{
      job: string(Map.get(row, "job_name")) || "unnamed job",
      task_type: string(Map.get(row, "task_type")),
      cron: string(Map.get(row, "cron_expression")),
      interval_seconds: interval,
      enabled?: Map.get(row, "enabled") == true,
      command: string(Map.get(row, "command")),
      last_run: last,
      # `last_run_epoch` is seeded 0 and only written once a run is dispatched, so 0 means
      # "never", not 1970.
      overdue?: overdue?(last, interval, now)
    }
  end

  defp overdue?(nil, _interval, _now), do: false
  defp overdue?(_last, nil, _now), do: false

  defp overdue?(last, interval, now) do
    DateTime.diff(now, last, :second) > interval * 2
  end

  defp run(row) do
    row = stringify(row)
    started = Tasks.timestamp(Map.get(row, "start_time"))
    ended = Tasks.timestamp(Map.get(row, "end_time"))
    status = row |> Map.get("status") |> normalize_status()

    %{
      job: string(Map.get(row, "job_name")) || "unnamed job",
      run_id: string(Map.get(row, "run_id")),
      status: status,
      severity: run_severity(status),
      exit_code: integer(Map.get(row, "exit_code")),
      output: string(Map.get(row, "output")),
      started_at: started,
      ended_at: ended,
      duration_seconds: duration(started, ended)
    }
  end

  # `dagur.py` writes `RUNNING` when a job starts and overwrites that row with `SUCCESS`
  # or `FAILED` when it ends. A job whose worker died leaves `RUNNING` behind forever, so
  # it gets its own severity rather than being read as either finished state.
  defp run_severity("SUCCESS"), do: :ok
  defp run_severity("FAILED"), do: :failed
  defp run_severity("RUNNING"), do: :running
  defp run_severity(_other), do: :unknown

  defp duration(nil, _ended), do: nil
  defp duration(_started, nil), do: nil
  defp duration(started, ended), do: max(DateTime.diff(ended, started, :second), 0)

  # -- helpers ---------------------------------------------------------------

  defp humanize(name) do
    name
    |> String.replace(~r/[_-]+/, " ")
    |> String.trim()
    |> String.split(" ")
    |> Enum.map_join(" ", &capitalize_word/1)
  end

  # Acronyms and daemon names stay as written; only ordinary words are title-cased.
  defp capitalize_word(word) do
    if word == String.upcase(word), do: word, else: String.capitalize(word)
  end

  defp hostname_for(ip) do
    Config.hostname_for(ip)
  rescue
    _exception -> ip
  catch
    :exit, _reason -> ip
  end

  defp latest(datetimes) do
    case Enum.reject(datetimes, &is_nil/1) do
      [] -> nil
      defined -> Enum.max_by(defined, &DateTime.to_unix(&1, :millisecond))
    end
  end

  defp unix(nil), do: 0
  defp unix(%DateTime{} = value), do: DateTime.to_unix(value, :millisecond)

  defp epoch_seconds(value) do
    case integer(value) do
      nil -> nil
      0 -> nil
      seconds -> Tasks.timestamp(seconds * 1000)
    end
  end

  defp stringify(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end

  defp stringify(other), do: other

  defp string(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp string(nil), do: nil
  defp string(value), do: to_string(value)

  defp integer(value) when is_integer(value), do: value
  defp integer(value) when is_float(value), do: trunc(value)

  defp integer(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {int, _rest} -> int
      :error -> nil
    end
  end

  defp integer(_value), do: nil

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(%{__exception__: true} = reason), do: Exception.message(reason)
  defp describe(reason), do: inspect(reason)
end
