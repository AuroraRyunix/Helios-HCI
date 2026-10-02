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

  ## The two long operations are Catalyst tasks, not requests

  Starting an upgrade rolls every node through maintenance and a reboot; loading a package
  validates a signed archive and copies it to every node. Both run for minutes and both
  fail in ways an operator has to see, so neither happens inside a LiveView event. Each is
  submitted to Catalyst as a `dagur` task running a hylia entry point on the ZooKeeper
  leader, which puts its progress in the header's task ring and its failure -- the
  command's own output -- in `hydra.catalyst_tasks` where the console reads it back.

  The upgrade task is *not* the upgrade. `hylia --start-upgrade` marks the job STARTING and
  then watches it: the rolling upgrade itself is run by the hylia daemon on the leader,
  exactly as it was before, so nothing about how an upgrade proceeds changes here. What the
  task adds is a row that lives as long as the upgrade does and ends in its verdict.
  """

  alias SpectrumPhx.Catalyst
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
        pairs = rows |> Enum.map(&stringify/1) |> Enum.flat_map(&components_of/1)
        components = group_by_component(pairs)

        %{
          available?: true,
          error: nil,
          components: components,
          nodes: pairs |> Enum.map(& &1.node) |> Enum.uniq() |> Enum.sort(),
          # The single most useful thing this page can say. A rolling upgrade that stopped
          # half way leaves the cluster on two versions of something, and every other view
          # of this data -- including the one the Python console draws -- shows one number
          # per component and hides it.
          disagreements: Enum.count(components, &(not &1.consistent?))
        }

      {:error, reason} ->
        %{available?: false, error: describe(reason), components: [], nodes: [], disagreements: 0}
    end
  end

  # One entry per component, carrying what each node reports for it.
  defp group_by_component(pairs) do
    pairs
    |> Enum.group_by(& &1.name)
    |> Enum.map(fn {name, entries} ->
      by_node = Map.new(entries, &{&1.node, &1.version})
      distinct = entries |> Enum.map(& &1.version) |> Enum.uniq()

      %{
        name: name,
        by_node: by_node,
        version: if(length(distinct) == 1, do: hd(distinct)),
        consistent?: length(distinct) == 1,
        readable?: Enum.all?(entries, & &1.readable?)
      }
    end)
    |> Enum.sort_by(&{&1.consistent?, &1.name})
  end

  # The inventory is a JSON blob, and what the cluster actually writes is a map of
  # hostname to `{"ip": ..., "versions": {name => version}}` -- one entry per node inside
  # a single `latest` row.
  #
  # Two narrower shapes are accepted too, because they are what the column name suggests
  # and what a hand-written row would look like: a lone `{"ip", "versions"}` object, and a
  # bare `{name => version}` map.
  #
  # Everything else at the top level is skipped rather than treated as a node. The real
  # blob carries a `level` key beside the hostnames, and reading that as a machine is how
  # the first attempt produced a component table listing "level".
  #
  # A blob that will not parse is reported as one unreadable entry rather than dropped: an
  # inventory quietly missing a component is an operator upgrading something they cannot
  # see. So is a version that is not a scalar -- rendering one crashed this page.
  defp components_of(row) do
    key = string(Map.get(row, "key")) || "inventory"

    case Jason.decode(Map.get(row, "inventory_json") || "") do
      {:ok, %{"versions" => versions} = blob} when is_map(versions) ->
        components(versions, node_name(blob, key))

      {:ok, map} when is_map(map) ->
        per_node =
          for {host, blob} <- map,
              is_map(blob),
              versions = Map.get(blob, "versions"),
              is_map(versions),
              component <- components(versions, to_string(host)),
              do: component

        if per_node == [], do: components(map, key), else: per_node

      _ ->
        [%{name: key, version: "unreadable", node: key, readable?: false}]
    end
  end

  defp node_name(blob, fallback), do: string(Map.get(blob, "ip")) || fallback

  defp components(versions, node) do
    Enum.map(versions, fn {name, version} ->
      readable = scalar(version)

      %{
        name: to_string(name),
        version: readable || "unreadable",
        node: node,
        readable?: readable != nil
      }
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
  @phases [
    {"restore", 1.0},
    {"reboot", 0.83},
    {"deploy", 0.5},
    {"cop", 0.5},
    {"maintenance", 0.17}
  ]

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
  def running?(%{state: state}), do: state in ["UPGRADING", "DOWNLOADING", "PENDING", "STARTING"]
  def running?(_job), do: false

  # -- the two long operations ------------------------------------------------------------

  # Where the console stages an uploaded package on the leader, and the only path
  # `--load-package` will read. It is fixed rather than passed, so nothing an operator
  # types reaches a command line: the browser names the file, the daemon does not care
  # what it was called, and hylia is told to look in one place.
  @package_path "/tmp/helios_update.zip"

  # Read by `SpectrumPhx.Tasks` out of the row's payload and shown in the ring, so they
  # are part of what an operator sees rather than internal labels.
  @upgrade_job "lcm_rolling_upgrade"
  @package_job "lcm_load_package"

  # A rolling upgrade drains, deploys, reboots and waits for each node in turn. Three nodes
  # with a reboot apiece is comfortably an hour; the ceiling exists so a wedged upgrade
  # eventually becomes a failed task rather than one that is pending forever.
  @upgrade_timeout 14_400
  # Validating an archive is quick; copying it to every node is not, and it goes through
  # spark in base64 chunks.
  @package_timeout 1_800

  @doc "Where an uploaded package is staged on the leader."
  def package_path, do: @package_path

  @doc "The `job_name` each of the two tasks carries."
  def job_names, do: %{upgrade: @upgrade_job, package: @package_job}

  @doc """
  Start the rolling upgrade for the package that is already loaded.

  Refused when there is no job -- an upgrade with nothing to install is an operator who
  believes a package was uploaded and was not -- and when one is already running, because
  the second start would have hylia resuming a job it is in the middle of.

  On success, `{:ok, task_id}`. The task is the *watcher*: hylia's daemon runs the upgrade,
  and this row tracks it from STARTING to its verdict.
  """
  @spec start_upgrade(keyword()) :: {:ok, String.t()} | {:error, String.t()}
  def start_upgrade(opts \\ []) do
    with {:ok, job} <- loaded_job(opts),
         :ok <- refuse_if_running(job),
         {:ok, id} <- upgrade_job_id(job) do
      Catalyst.run_on_leader(
        @upgrade_job,
        "python3 /usr/local/bin/hylia --start-upgrade " <> id,
        timeout: @upgrade_timeout,
        reports_progress: true,
        payload: %{"job_id" => id, "build" => job.build}
      )
      |> submitted("The upgrade could not be started")
    end
  end

  @doc """
  Validate the staged package, distribute it, and record the job it describes.

  The bytes are already on the leader by the time this is called --
  `SpectrumPhx.Lcm.PackageUploadWriter` streams them there from the browser, so this tier
  never holds an archive. What is left is the part that talks to every node, and that is
  the task.

  `size_bytes` is carried only so the row says how big the thing being installed was; the
  daemon reads the file it was handed and checks the signature over the manifest before it
  trusts a single digest inside it.
  """
  @spec load_package(String.t(), non_neg_integer()) :: {:ok, String.t()} | {:error, String.t()}
  def load_package(filename, size_bytes) do
    Catalyst.run_on_leader(
      @package_job,
      "python3 /usr/local/bin/hylia --load-package " <> @package_path,
      timeout: @package_timeout,
      payload: %{"filename" => clean_filename(filename), "size_bytes" => size_bytes}
    )
    |> submitted("The package could not be handed to the cluster")
  end

  @doc """
  Why the package upload is built the way it is.

  Shown on the page, because an operator watching a multi-hundred-megabyte transfer is
  owed an explanation of where it is going.
  """
  def upload_note do
    """
    The archive streams from your browser to the ZooKeeper leader's own daemon and is
    staged at #{@package_path}. Nothing is written in the console tier, so a package
    larger than this container's disk is not a problem. Validating the signature,
    checking every component digest and copying the archive to the other nodes then runs
    as a Catalyst task, and its result is in the task ring.
    """
  end

  defp loaded_job(opts) do
    case job(static_source(opts)) do
      nil ->
        {:error,
         "No upgrade package is loaded. Upload one first -- there is nothing for a " <>
           "rolling upgrade to install."}

      job ->
        {:ok, job}
    end
  end

  defp refuse_if_running(job) do
    if running?(job),
      do:
        {:error,
         "An upgrade is already running (#{job.state}). Abort it before starting another."},
      else: :ok
  end

  # `job_id` is a CQL uuid, and it is about to be a word in a root shell command on the
  # leader. Xandra hands it back as a string in the normal case and as a `%Xandra.UUID{}`
  # or a raw binary in others, so it is coerced and then *matched*: anything that is not a
  # UUID is refused rather than escaped, because there is no legitimate way for this value
  # to be anything else.
  @uuid_regex ~r/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/

  defp upgrade_job_id(job) do
    id = to_string(job.id || "")

    if Regex.match?(@uuid_regex, id) do
      {:ok, id}
    else
      {:error,
       "The loaded upgrade job has no usable id (#{inspect(job.id)}). Re-upload the package."}
    end
  end

  defp submitted({:ok, %{"task_id" => id}}, _context) when is_binary(id), do: {:ok, id}

  defp submitted({:ok, _other}, context),
    do: {:error, context <> ": Catalyst accepted the task but did not name it."}

  defp submitted({:error, reason}, context),
    do: {:error, context <> ": " <> Catalyst.describe(reason)}

  # The row is read back and rendered, and the browser chose this string. Keeping it to a
  # basename of ordinary characters means a task label cannot carry a path or markup.
  defp clean_filename(name) when is_binary(name) do
    name
    |> Path.basename()
    |> String.replace(~r/[^A-Za-z0-9._-]/, "_")
    |> String.slice(0, 120)
  end

  defp clean_filename(_name), do: "update.zip"

  # -- plumbing ---------------------------------------------------------------------------

  @doc """
  Where reads come from when the caller does not say: `:live`, or `{:static, map}` set in
  `Application.get_env(:spectrum_phx, :lcm_source)`.

  The same seam `SpectrumPhx.Tasks` carries, and it exists for the same reason: the
  controls on this page are only worth having if they are exercised through the real
  route, and a page that can only be mounted against a cluster is a page whose buttons
  are never tested.
  """
  def source, do: Application.get_env(:spectrum_phx, :lcm_source, :live)

  defp static_source(opts) do
    case Keyword.get(opts, :source, :live) do
      {:static, map} ->
        map

      :live ->
        case source() do
          {:static, map} -> map
          _live -> nil
        end
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
