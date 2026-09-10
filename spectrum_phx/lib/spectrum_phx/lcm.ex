defmodule SpectrumPhx.Lcm do
  @moduledoc """
  Life-cycle management: what is installed, what is available, and a rolling upgrade
  while it runs.

  Three tables, and they answer three different questions:

    * `hydra.lcm_inventory` -- what each component is running now.
    * `hydra.lcm_update_state` -- what the release feed last said was available, and when
      it was asked.
    * `hydra.hylia_jobs` + `hydra.hylia_logs` -- the rolling upgrade in flight, node by
      node, with its output.

  ## Progress is per node, and honest about the part it is guessing

  A rolling upgrade's real unit of progress is "nodes finished", which is exactly
  countable: the position of `current_node` in `target_nodes`. Within a node the phases
  are inferred from log lines, which is a guess, and it is confined to the fraction of
  the bar that one node owns so a wrong guess can never move the bar backwards or past
  the node it is on.

  A job with no `current_node` in its target list contributes no sub-progress at all
  rather than defaulting to the start, because "we do not know where it is" and "it has
  just begun" are different states.

  ## Only reads and the two safe actions live here

  Checking the feed asks a remote for a version, and aborting stops a running upgrade --
  both are things an operator watching a bad upgrade must be able to do. *Starting* one
  rolls every node through maintenance and reboots, and *uploading* a package accepts a
  signed archive; neither is offered from the rebuilt console yet, and the page says so
  rather than presenting a button that is not wired.
  """

  alias SpectrumPhx.Hydra

  @inventory_cql "SELECT key, inventory_json, last_updated FROM hydra.lcm_inventory"
  @state_cql "SELECT key, latest_version, release_date, changelog, current_version, update_available, last_checked, error_msg, size FROM hydra.lcm_update_state"
  @jobs_cql "SELECT job_id, state, target_nodes, current_node, build_number FROM hydra.hylia_jobs"

  @doc "The CQL this module reads."
  def statements, do: %{inventory: @inventory_cql, update: @state_cql, jobs: @jobs_cql}

  @doc """
  Everything the LCM page shows.

  `source: {:static, map}` supplies rows instead of querying, keyed `:inventory`,
  `:update`, `:jobs`, `:logs`.
  """
  def overview(opts \\ []) do
    static = static_source(opts)

    %{
      inventory: inventory(static),
      update: update_state(static),
      job: job(static)
    }
  end

  defp inventory(static) do
    case read(static, :inventory, @inventory_cql) do
      {:ok, rows} ->
        components =
          rows
          |> Enum.map(&stringify/1)
          |> Enum.flat_map(&components_of/1)
          |> Enum.sort_by(& &1.name)

        %{available?: true, error: nil, components: components}

      {:error, reason} ->
        %{available?: false, error: describe(reason), components: []}
    end
  end

  # The inventory is a JSON blob per row, and it comes in two shapes.
  #
  # What the cluster actually writes is `{"ip": ..., "versions": {name => version}}` --
  # one row per node, with the node's address beside the component list. A bare
  # `{name => version}` map is also accepted, because that is the shape the schema's name
  # suggests and the shape a hand-written row would take.
  #
  # A blob that will not parse is reported as one unreadable entry rather than dropped: an
  # inventory quietly missing a component is an operator upgrading something they cannot
  # see. So is a version that is not a scalar -- rendering one crashed this page, and the
  # fix is to say "unreadable" about that component rather than to lose the other thirty.
  defp components_of(row) do
    key = string(Map.get(row, "key")) || "inventory"

    case Jason.decode(Map.get(row, "inventory_json") || "") do
      {:ok, %{"versions" => versions} = blob} when is_map(versions) ->
        source = string(Map.get(blob, "ip")) || key
        components(versions, source)

      {:ok, map} when is_map(map) ->
        components(map, key)

      _ ->
        [%{name: key, version: "unreadable", source: key, readable?: false}]
    end
  end

  defp components(versions, source) do
    Enum.map(versions, fn {name, version} ->
      case scalar(version) do
        nil -> %{name: to_string(name), version: "unreadable", source: source, readable?: false}
        text -> %{name: to_string(name), version: text, source: source, readable?: true}
      end
    end)
  end

  defp scalar(value) when is_binary(value), do: value
  defp scalar(value) when is_number(value), do: to_string(value)
  defp scalar(value) when is_boolean(value), do: to_string(value)
  defp scalar(_value), do: nil

  defp update_state(static) do
    case read(static, :update, @state_cql) do
      {:ok, [row | _]} ->
        row = stringify(row)

        %{
          available?: true,
          error: string(Map.get(row, "error_msg")),
          update_available?: Map.get(row, "update_available") == true,
          latest_version: string(Map.get(row, "latest_version")),
          current_version: string(Map.get(row, "current_version")),
          release_date: string(Map.get(row, "release_date")),
          changelog: string(Map.get(row, "changelog")),
          size_bytes: integer(Map.get(row, "size")),
          last_checked: Map.get(row, "last_checked")
        }

      {:ok, []} ->
        %{
          available?: true,
          error: nil,
          update_available?: false,
          latest_version: nil,
          current_version: nil,
          release_date: nil,
          changelog: nil,
          size_bytes: nil,
          last_checked: nil
        }

      {:error, reason} ->
        %{
          available?: false,
          error: describe(reason),
          update_available?: false,
          latest_version: nil,
          current_version: nil,
          release_date: nil,
          changelog: nil,
          size_bytes: nil,
          last_checked: nil
        }
    end
  end

  @doc """
  The rolling upgrade, or `nil` when none is recorded.

  Logs come from `hydra.hylia_logs` for that job, oldest first.
  """
  def job(static) do
    case read(static, :jobs, @jobs_cql) do
      {:ok, [row | _]} ->
        row = stringify(row)
        id = Map.get(row, "job_id")
        targets = list(Map.get(row, "target_nodes"))
        current = string(Map.get(row, "current_node"))
        state = string(Map.get(row, "state")) || "IDLE"
        logs = logs(static, id)

        %{
          id: id,
          state: state,
          build: string(Map.get(row, "build_number")),
          targets: targets,
          current: current,
          logs: logs,
          progress: progress(state, targets, current, logs),
          finished: finished_nodes(targets, current, state)
        }

      _ ->
        nil
    end
  end

  defp logs(%{logs: logs}, _id) when is_list(logs), do: Enum.map(logs, &log_line/1)

  defp logs(%{} = _static, _id), do: []

  defp logs(nil, id) when is_binary(id) do
    # Bound by the partition: logs are keyed by job, so this reads one upgrade's output
    # and not the history of every upgrade the cluster has ever run.
    case query("SELECT timestamp, log_line FROM hydra.hylia_logs WHERE job_id = ?", [id]) do
      {:ok, rows} -> rows |> Enum.map(&stringify/1) |> Enum.map(&log_line/1)
      {:error, _reason} -> []
    end
  end

  defp logs(_static, _id), do: []

  defp log_line(row) when is_map(row) do
    row = stringify(row)
    %{at: Map.get(row, "timestamp"), line: string(Map.get(row, "log_line")) || ""}
  end

  defp log_line(line) when is_binary(line), do: %{at: nil, line: line}

  @doc """
  How far a rolling upgrade has got, 0..100.

  Nodes finished is the countable part. The phase within the node being worked on is
  inferred from its log lines and confined to that node's share of the bar, so a wrong
  guess cannot move the bar past the node it is on -- or backwards, which is what makes
  a progress bar untrustworthy.
  """
  def progress(state, targets, current, logs)

  def progress("COMPLETED", _targets, _current, _logs), do: 100
  def progress("FAILED", _targets, _current, _logs), do: 100

  def progress("UPGRADING", targets, current, logs) when is_list(targets) and targets != [] do
    case Enum.find_index(targets, &(&1 == current)) do
      nil ->
        # Where it is cannot be established. That is not the same as "it has just begun",
        # so nothing is added for the node in flight.
        0

      index ->
        per_node = 100 / length(targets)
        base = index * per_node
        round(base + phase_fraction(current, logs) * per_node)
    end
  end

  def progress(_state, _targets, _current, _logs), do: 0

  # The phases hylia logs as it works a node, as a fraction of that node's share.
  @phases [{"restore", 1.0}, {"reboot", 0.83}, {"deploy", 0.5}, {"cop", 0.5}, {"maintenance", 0.17}]

  defp phase_fraction(nil, _logs), do: 0.0

  defp phase_fraction(current, logs) do
    mine = for %{line: line} <- logs, String.contains?(line, current), do: String.downcase(line)

    Enum.reduce(@phases, 0.0, fn {needle, fraction}, best ->
      if fraction > best and Enum.any?(mine, &String.contains?(&1, needle)),
        do: fraction,
        else: best
    end)
  end

  @doc "The nodes a running upgrade has already finished."
  def finished_nodes(targets, current, state)

  def finished_nodes(targets, _current, "COMPLETED") when is_list(targets), do: targets

  def finished_nodes(targets, current, _state) when is_list(targets) do
    case Enum.find_index(targets, &(&1 == current)) do
      nil -> []
      index -> Enum.take(targets, index)
    end
  end

  def finished_nodes(_targets, _current, _state), do: []

  @doc "Whether a job is still moving."
  def running?(%{state: state}), do: state in ["UPGRADING", "DOWNLOADING", "PENDING"]
  def running?(_job), do: false

  # -- plumbing ---------------------------------------------------------------------------

  defp static_source(opts) do
    case Keyword.get(opts, :source, :live) do
      {:static, map} -> map
      :live -> nil
    end
  end

  defp read(nil, _key, cql), do: query(cql, [])

  defp read(%{} = static, key, _cql) do
    case Map.get(static, key) do
      {:error, reason} -> {:error, reason}
      rows when is_list(rows) -> {:ok, rows}
      nil -> {:ok, []}
    end
  end

  defp query(statement, params) do
    Hydra.query(statement, params)
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp stringify(row) when is_map(row) do
    Map.new(row, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp stringify(row), do: row

  defp list(value) when is_list(value), do: Enum.map(value, &to_string/1)
  defp list(_value), do: []

  defp string(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp string(_), do: nil

  defp integer(value) when is_integer(value), do: value
  defp integer(_), do: nil

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)
end
