defmodule SpectrumPhx.Tasks do
  @moduledoc """
  Catalyst task history, read from `hydra.catalyst_tasks`.

  The table is written from three places -- `catalyst.py` (submit and update),
  `spectrum_server.py`'s `log_catalyst_task/8`, and `mipha.py`'s multi-step workflows --
  so this module's job is to turn a heterogeneous pile of rows into one shape the UI can
  render honestly.

  ## What the rows actually contain

    * `status` is one of `pending`, `processing`, `completed`, `failed`. Nothing
      constrains it, so anything unrecognised is classified `:unknown` here and is *never*
      styled as success. Reporting an unknown state as healthy is the failure this rewrite
      exists to remove.

    * `progress` is not a success signal. `spectrum_server.py` writes `progress = 100`
      together with `status = 'failed'` at every failure site, so a full bar means
      "finished", not "worked". `state/1` is the only thing that says whether it worked.

    * `payload` is a JSON *string*, not a map, and it is the only place a task's subject
      lives: `vm_name`, `job_name`, `hostname`, `filename`. It also carries
      `parent_task_id`, which is how `mipha.py` relates a workflow to its steps -- there is
      no parent column.

    * `created_at`/`updated_at` are `timestamp` columns; Xandra decodes them to
      `DateTime`. Rows written from JSON fixtures may carry epoch milliseconds instead, so
      both are accepted.

  ## Reading the table

  `task_id` is the whole primary key, so there is no clustering order to read in and no
  server-side way to ask for "the most recent N": every read is a full scan, exactly as
  `/api/catalyst/tasks` did. Ordering and the cap therefore happen here, after the read.
  That is a schema limitation, not a choice -- a `created_at` clustering key would fix it.

  ## Test seam

  `fetch/1` accepts `:rows` (raw row maps) or `:error` directly, so the whole assembly
  path -- normalisation, the parent/child tree, the summary -- is exercisable with no
  database. The same may be set as `{:static, rows}` or `{:error, reason}` in
  `Application.get_env(:spectrum_phx, :tasks_source)` for tests that drive the LiveView
  through its real route. When neither is set, the live path is taken.
  """

  alias SpectrumPhx.Hydra

  @columns "task_id, service, action, status, payload, progress, error_msg, created_at, updated_at"
  @list_cql "SELECT #{@columns} FROM hydra.catalyst_tasks"

  @default_limit 200

  @pubsub SpectrumPhx.PubSub
  @topic "tasks"

  @doc "CQL used by `fetch/1`. Exposed so tests can pin the statement without a cluster."
  def list_cql, do: @list_cql

  @doc "PubSub topic the dashboard subscribes to for pushed snapshots."
  def topic, do: @topic

  @doc "Subscribe the calling process to pushed task snapshots."
  def subscribe, do: Phoenix.PubSub.subscribe(@pubsub, @topic)

  @doc """
  Broadcast a snapshot to every connected dashboard as `{:tasks, snapshot}`.

  Nothing here calls it. It is the hook for whichever process ends up watching Catalyst,
  so the dashboard can stop re-reading on a timer entirely.
  """
  def broadcast(snapshot) do
    Phoenix.PubSub.broadcast(@pubsub, @topic, {:tasks, snapshot})
  rescue
    ArgumentError -> :ok
  catch
    :exit, _reason -> :ok
  end

  @doc """
  A full snapshot of task history.

      %{
        tasks: [task],
        summary: %{total: 12, running: 1, pending: 0, completed: 9, failed: 2, unknown: 0},
        available?: true,
        error: nil,
        truncated?: false,
        limit: 200
      }

  `available?` is false when Hydra could not be read; `tasks` is then empty and `error`
  says why. An empty list with `available?: true` genuinely means no tasks were recorded.
  Those two cases must never render the same way.

  Options:

    * `:limit` - how many tasks to keep after ordering (default #{@default_limit}).
    * `:rows` - use these rows instead of reading the database.
    * `:error` - pretend the read failed with this reason.
  """
  def fetch(opts \\ []) do
    limit = Keyword.get(opts, :limit, @default_limit)

    case rows(opts) do
      {:ok, rows} ->
        all = Enum.map(rows, &task/1)
        kept = all |> Enum.sort_by(&sort_key/1, :desc) |> Enum.take(limit)

        %{
          tasks: tree(kept),
          summary: summarize(kept),
          available?: true,
          error: nil,
          truncated?: length(all) > length(kept),
          limit: limit
        }

      {:error, reason} ->
        %{
          tasks: [],
          summary: summarize([]),
          available?: false,
          error: describe(reason),
          truncated?: false,
          limit: limit
        }
    end
  end

  @doc """
  Where task reads come from: `:hydra` (the default), `{:static, rows}` or
  `{:error, reason}`.
  """
  def source, do: Application.get_env(:spectrum_phx, :tasks_source, :hydra)

  # -- sourcing --------------------------------------------------------------

  defp rows(opts) do
    cond do
      Keyword.keyword?(opts) and Keyword.has_key?(opts, :rows) ->
        {:ok, Keyword.fetch!(opts, :rows)}

      Keyword.keyword?(opts) and Keyword.has_key?(opts, :error) ->
        {:error, Keyword.fetch!(opts, :error)}

      true ->
        from_source()
    end
  end

  defp from_source do
    case source() do
      {:static, rows} when is_list(rows) -> {:ok, rows}
      {:error, reason} -> {:error, reason}
      _hydra -> query()
    end
  end

  # A dev machine has no ScyllaDB at all. Xandra answers with `{:error, _}` for a missing
  # connection, but a pool that is starting, stopping or wedged can exit instead, and an
  # exit here would take the socket down and show the operator a crashed page rather than
  # "the database is unreachable" -- which is the honest answer and the one they can act on.
  defp query do
    Hydra.query(@list_cql, [])
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  # -- normalisation ---------------------------------------------------------

  defp task(row) do
    payload_raw = string(get(row, "payload"))
    payload = decode_payload(payload_raw)
    status = row |> get("status") |> normalize_status()
    id = string(get(row, "task_id")) || ""

    %{
      id: id,
      short_id: String.slice(id, 0, 8),
      service: string(get(row, "service")) || "system",
      action: string(get(row, "action")) || "task",
      status: status,
      state: state(status),
      progress: progress(get(row, "progress")),
      error: string(get(row, "error_msg")),
      payload: payload,
      payload_raw: payload_raw,
      label: label(row, payload),
      parent_id: parent_id(payload),
      created_at: timestamp(get(row, "created_at")),
      updated_at: timestamp(get(row, "updated_at")),
      depth: 0
    }
  end

  defp normalize_status(value) when is_binary(value) do
    case value |> String.trim() |> String.downcase() do
      "" -> "unknown"
      other -> other
    end
  end

  defp normalize_status(value) when is_atom(value) and not is_nil(value) do
    normalize_status(Atom.to_string(value))
  end

  defp normalize_status(_value), do: "unknown"

  @doc """
  Classify a raw status string.

  Only the four statuses the Python tier actually writes are recognised. Everything else
  -- including a null -- is `:unknown`, which the UI must not draw as success.
  """
  def state("completed"), do: :completed
  def state("failed"), do: :failed
  def state("processing"), do: :processing
  def state("pending"), do: :pending
  def state(_other), do: :unknown

  @doc "True when the task is still expected to move."
  def active?(%{state: state}), do: state in [:pending, :processing]

  # `progress` is an int column but arrives as a string from JSON fixtures, and nothing
  # validates its range on write. Clamping here means a bad value cannot produce a bar
  # wider than its track.
  defp progress(value) do
    value
    |> integer()
    |> Kernel.||(0)
    |> max(0)
    |> min(100)
  end

  defp decode_payload(nil), do: nil

  defp decode_payload(raw) when is_binary(raw) do
    case Jason.decode(raw) do
      {:ok, map} when is_map(map) -> map
      _other -> nil
    end
  end

  defp decode_payload(map) when is_map(map), do: stringify(map)
  defp decode_payload(_other), do: nil

  defp parent_id(%{"parent_task_id" => id}) when is_binary(id) and id != "", do: id
  defp parent_id(_payload), do: nil

  # The same naming the old tasks page derived in JS, kept because it is what operators
  # read: the payload is where the subject of a task lives.
  defp label(row, payload) do
    service = string(get(row, "service")) || "system"
    action = string(get(row, "action")) || "task"

    case payload do
      %{"vm_name" => name} when is_binary(name) and name != "" ->
        "VM '" <> name <> "' - " <> action

      %{"job_name" => name} when is_binary(name) and name != "" ->
        "Job '" <> name <> "' - execute"

      %{"hostname" => host} when is_binary(host) and host != "" ->
        host_label(host, action)

      %{"filename" => file} when is_binary(file) and file != "" ->
        "Image '" <> file <> "' - " <> action

      _other ->
        service <> " - " <> action
    end
  end

  defp host_label(host, "host_maintenance_enter"), do: "Host '" <> host <> "' - enter maintenance"
  defp host_label(host, "host_maintenance_leave"), do: "Host '" <> host <> "' - leave maintenance"
  defp host_label(host, action), do: "Host '" <> host <> "' - " <> action

  # -- parent/child ----------------------------------------------------------

  # `mipha.py` submits a workflow as a parent task plus one child per step, related only
  # by `parent_task_id` inside the child's JSON payload. Flattening them into one list
  # loses which failure belongs to which workflow, so the relationship is rebuilt here and
  # carried as `depth` for the view to indent by.
  defp tree(tasks) do
    by_id = Map.new(tasks, fn task -> {task.id, task} end)

    children =
      tasks
      |> Enum.filter(fn task -> task.parent_id && Map.has_key?(by_id, task.parent_id) end)
      |> Enum.group_by(& &1.parent_id)
      |> Map.new(fn {parent, kids} -> {parent, Enum.sort_by(kids, &sort_key/1)} end)

    tasks
    |> Enum.reject(fn task -> task.parent_id && Map.has_key?(by_id, task.parent_id) end)
    |> Enum.flat_map(fn root -> flatten(root, children, 0) end)
  end

  # Depth is capped rather than recursed without bound: a payload that names its own task
  # as parent, or two tasks that name each other, would otherwise loop forever. Such a
  # cycle cannot be built by the writers today, but it is a JSON field with no constraint
  # on it, and a wedged socket is a worse answer than a flat row.
  defp flatten(task, _children, depth) when depth > 4, do: [%{task | depth: depth}]

  defp flatten(task, children, depth) do
    kids = Map.get(children, task.id, [])
    [%{task | depth: depth} | Enum.flat_map(kids, &flatten(&1, children, depth + 1))]
  end

  # -- summary ---------------------------------------------------------------

  defp summarize(tasks) do
    Enum.reduce(
      tasks,
      %{total: 0, running: 0, pending: 0, completed: 0, failed: 0, unknown: 0},
      fn task, acc ->
        key =
          case task.state do
            :processing -> :running
            :pending -> :pending
            :completed -> :completed
            :failed -> :failed
            _other -> :unknown
          end

        acc |> Map.update!(key, &(&1 + 1)) |> Map.update!(:total, &(&1 + 1))
      end
    )
  end

  # -- helpers ---------------------------------------------------------------

  # Newest first, by whichever timestamp the row has. Rows with neither sort last rather
  # than to the top, so a row missing its timestamps cannot bury the recent work.
  defp sort_key(%{created_at: nil, updated_at: nil}), do: 0

  defp sort_key(%{created_at: created, updated_at: updated}) do
    [created, updated]
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&DateTime.to_unix(&1, :millisecond))
    |> Enum.max(fn -> 0 end)
  end

  defp get(row, key) when is_map(row) do
    case Map.fetch(row, key) do
      {:ok, value} -> value
      :error -> Map.get(row, String.to_atom(key))
    end
  end

  defp get(_row, _key), do: nil

  defp stringify(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end

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

  @doc """
  Coerce a `timestamp` column to a `DateTime`.

  Xandra decodes CQL timestamps to `DateTime` already. Epoch milliseconds are also
  accepted because that is what the Python tier writes and what a JSON fixture carries.
  """
  def timestamp(%DateTime{} = value), do: value
  def timestamp(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")

  def timestamp(value) when is_integer(value) do
    case DateTime.from_unix(value, :millisecond) do
      {:ok, datetime} -> datetime
      {:error, _reason} -> nil
    end
  end

  def timestamp(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {int, ""} ->
        timestamp(int)

      _other ->
        case DateTime.from_iso8601(value) do
          {:ok, datetime, _offset} -> datetime
          _error -> nil
        end
    end
  end

  def timestamp(_value), do: nil

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(%{__exception__: true} = reason), do: Exception.message(reason)
  defp describe(reason), do: inspect(reason)
end
