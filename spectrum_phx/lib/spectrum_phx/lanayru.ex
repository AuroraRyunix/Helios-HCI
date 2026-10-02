defmodule SpectrumPhx.Lanayru do
  @moduledoc """
  The Kubernetes engine: whether the cluster can host one, and the one it is hosting.

  ## The pre-flight is the page

  Deploying Kubernetes onto this cluster needs four things to be true at once, and the
  reason the old page led with them is that finding out halfway through a deploy is
  expensive. Each is a real question asked of a real component rather than a green tick
  for having the component installed:

    * **Consensus** -- how many ring members are actually `UN`, against how many the
      cluster expects. Not "is ScyllaDB running".
    * **Storage** -- how full the extent store is. A store at 95% is a deploy that will
      fail partway, so it warns before writes start failing rather than after.
    * **Compute** -- free memory on the host, because the control plane has to fit.
    * **Overlay** -- whether a segment exists to put the cluster on. Kubernetes on no
      network is the one failure that looks like success until a pod tries to talk.

  Each check reports `:ready`, `:warning` or `:error` with a sentence saying what it
  found. A check that could not be run is `:error` and says so -- never `:ready` by
  default, because the entire point is to be believed.

  ## Deploy and destroy are Catalyst tasks

  Both build or tear down a Kubernetes cluster across every node, so both go on Catalyst's
  `lanayru` queue and are drained on the ZooKeeper leader by the console backend -- which
  is where `lanayru.py` already lives, and where its two workers already report progress
  and their failure into `hydra.catalyst_tasks`. The ring in the header is therefore the
  thing an operator watches, and a deploy that dies part way is a failed row rather than a
  request that returned 200 and a background thread that stopped.

  What this module adds is the refusals. Each is a condition the deploy would otherwise
  discover several minutes in:

    * a cluster is already on record -- `hydra.lanayru_clusters` holds one, and a second
      deploy would overwrite the row describing the one that exists;
    * the overlay is disabled, so there is no network to put the cluster on. The Python
      tier checks this too, and it is the check that saves the most time;
    * the name is not a legal object name, or the control-plane count is not a number of
      nodes this cluster has.

  Destroy asks for the cluster's name back and refuses anything else, because "destroy the
  Kubernetes cluster" is not a request that should succeed by being clicked.
  """

  alias SpectrumPhx.Catalyst
  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Settings
  alias SpectrumPhx.Spark
  alias SpectrumPhx.Vms.Vm

  @clusters_cql "SELECT cluster_id, name, control_nodes, overlay_segment_id, status, created_at FROM hydra.lanayru_clusters"
  @segments_cql "SELECT segment_id, name FROM hydra.urbosa_segments"

  # A store this full will not survive a deploy; one this full will not survive much.
  @storage_error_pct 95.0
  @storage_warn_pct 85.0
  # The control plane needs room to start.
  @memory_warn_mb 2048

  @doc "The CQL this module reads."
  def statements, do: %{clusters: @clusters_cql, segments: @segments_cql}

  @doc """
  The page's whole state: the pre-flight, and the cluster if one exists.

  `source: {:static, map}` supplies fixtures, keyed `:clusters`, `:segments`, `:ring`,
  `:capacity`, `:memory`.
  """
  def overview(opts \\ []) do
    static = static_source(opts)

    %{
      checks: checks(static),
      cluster: cluster(static),
      segments: segments(static)
    }
  end

  @doc "The four pre-flight checks, in the order an operator should read them."
  def checks(static \\ nil) do
    [
      consensus_check(static),
      storage_check(static),
      compute_check(static),
      overlay_check(static)
    ]
  end

  @doc "Whether every check passed, so a deploy would be sensible."
  def ready?(checks), do: Enum.all?(checks, &(&1.status == :ready))

  defp consensus_check(static) do
    expected = expected_nodes(static)

    case ring(static) do
      {:ok, nodes} ->
        up = Enum.count(nodes, &up_and_normal?/1)

        cond do
          up >= expected and expected > 0 ->
            ready(:consensus, "Consensus healthy: #{up} of #{expected} members are UN.")

          up > 0 ->
            warning(
              :consensus,
              "Only #{up} of #{expected} members are UN. A deploy on a degraded ring can lose its metadata writes."
            )

          true ->
            error(:consensus, "No ring member is UN.")
        end

      {:error, reason} ->
        error(:consensus, "The ring could not be read: #{describe(reason)}")
    end
  end

  defp up_and_normal?(node) do
    node = stringify(node)
    status = node |> Map.get("status") |> to_string() |> String.upcase()
    state = node |> Map.get("state") |> to_string() |> String.upcase()
    String.starts_with?(status, "U") and String.starts_with?(state, "N")
  end

  defp storage_check(static) do
    case capacity(static) do
      {:ok, document} ->
        document = stringify(document)
        total = integer(Map.get(document, "total_bytes")) || 0
        available = integer(Map.get(document, "available_bytes")) || 0

        if total > 0 do
          used_pct = (total - available) / total * 100
          used_gib = Float.round((total - available) / 1_073_741_824, 1)
          total_gib = Float.round(total / 1_073_741_824, 1)
          summary = "#{used_gib} of #{total_gib} GiB used"

          cond do
            used_pct >= @storage_error_pct ->
              error(
                :storage,
                "The extent store is #{Float.round(used_pct, 1)}% full (#{summary}). A deploy would run it out."
              )

            used_pct >= @storage_warn_pct ->
              warning(
                :storage,
                "The extent store is #{Float.round(used_pct, 1)}% full (#{summary})."
              )

            true ->
              ready(:storage, "Extent store healthy: #{summary}.")
          end
        else
          # Zero capacity is not an empty store; it is a store that is not mounted.
          error(
            :storage,
            "The extent store reports no capacity, which usually means it is not mounted."
          )
        end

      {:error, reason} ->
        error(:storage, "The extent store could not be reached: #{describe(reason)}")
    end
  end

  defp compute_check(static) do
    case memory(static) do
      {:ok, document} ->
        document = stringify(document)
        free = integer(Map.get(document, "free_mb")) || 0

        if free >= @memory_warn_mb,
          do: ready(:compute, "#{free} MB free on this host."),
          else:
            warning(:compute, "Only #{free} MB free on this host; the control plane may not fit.")

      {:error, reason} ->
        error(:compute, "Host memory could not be read: #{describe(reason)}")
    end
  end

  defp overlay_check(static) do
    case read(static, :segments, @segments_cql) do
      {:ok, []} ->
        # Kubernetes on no network is the failure that looks like success until a pod
        # tries to talk to another one.
        error(
          :overlay,
          "No overlay segment exists to put a cluster on. Create one on the SDN page first."
        )

      {:ok, rows} ->
        ready(:overlay, "#{length(rows)} overlay segment(s) available.")

      {:error, reason} ->
        error(:overlay, "The segment table could not be read: #{describe(reason)}")
    end
  end

  defp ready(id, message), do: %{id: id, status: :ready, message: message, label: label(id)}
  defp warning(id, message), do: %{id: id, status: :warning, message: message, label: label(id)}
  defp error(id, message), do: %{id: id, status: :error, message: message, label: label(id)}

  defp label(:consensus), do: "Metadata consensus"
  defp label(:storage), do: "Extent store"
  defp label(:compute), do: "Host compute"
  defp label(:overlay), do: "Overlay network"

  @doc "The Kubernetes cluster on record, or nil."
  def cluster(static) do
    case read(static, :clusters, @clusters_cql) do
      {:ok, [row | _]} ->
        row = stringify(row)

        %{
          id: Map.get(row, "cluster_id"),
          name: string(Map.get(row, "name")) || "unnamed",
          control_nodes: integer(Map.get(row, "control_nodes")),
          segment_id: Map.get(row, "overlay_segment_id"),
          status: string(Map.get(row, "status")) || "unknown",
          created_at: Map.get(row, "created_at")
        }

      _ ->
        nil
    end
  end

  defp segments(static) do
    case read(static, :segments, @segments_cql) do
      {:ok, rows} ->
        rows
        |> Enum.map(&stringify/1)
        |> Enum.map(fn row ->
          %{id: Map.get(row, "segment_id"), name: string(Map.get(row, "name")) || "unnamed"}
        end)

      _ ->
        []
    end
  end

  # -- deploy and destroy -------------------------------------------------------------------

  @service "lanayru"

  @doc """
  Ask the cluster to build a Kubernetes cluster.

  `params` is the form's map: `"cluster_name"`, `"control_nodes"`, `"overlay_segment_id"`.
  Returns `{:ok, task_id}` once Catalyst has the task, or `{:error, message}` with a
  sentence for the page. Nothing is written to `hydra.lanayru_clusters` here -- the worker
  on the leader owns that row, and a row written by the console for work that was never
  queued is a cluster that exists only in the console.
  """
  @spec deploy(map(), keyword()) :: {:ok, String.t()} | {:error, String.t()}
  def deploy(params, opts \\ []) do
    static = static_source(opts)

    with :ok <- refuse_if_deployed(static),
         :ok <- require_overlay_enabled(static),
         {:ok, name} <- cluster_name(Map.get(params, "cluster_name")),
         {:ok, control} <- control_nodes(Map.get(params, "control_nodes"), static),
         {:ok, segment} <- segment_id(Map.get(params, "overlay_segment_id"), static) do
      Catalyst.submit(@service, "deploy", %{
        "cluster_name" => name,
        "control_nodes" => control,
        "overlay_segment_id" => segment
      })
      |> submitted("The deployment could not be queued")
    end
  end

  @doc """
  Tear the Kubernetes cluster down.

  `confirmation` must be the recorded cluster's name. It is not a formality: destroy
  removes the guest VMs and the rows describing them, and the only thing standing between
  a mis-click and that is having to name what is about to go.
  """
  @spec destroy(String.t(), keyword()) :: {:ok, String.t()} | {:error, String.t()}
  def destroy(confirmation, opts \\ []) do
    static = static_source(opts)

    case cluster(static) do
      nil ->
        {:error, "There is no Kubernetes cluster on record to destroy."}

      %{name: name} ->
        if to_string(confirmation) == name do
          Catalyst.submit(@service, "destroy", %{"cluster_name" => name})
          |> submitted("The teardown could not be queued")
        else
          {:error, "That is not the cluster's name. Type #{name} to confirm the teardown."}
        end
    end
  end

  defp refuse_if_deployed(static) do
    case cluster(static) do
      nil ->
        :ok

      %{name: name} ->
        {:error,
         "'#{name}' is already on record. Destroy it before deploying another -- a second " <>
           "deploy would overwrite the row describing the one that exists."}
    end
  end

  # The check that saves the most time. Urbosa builds the namespaces, bridges and VXLAN
  # interfaces the cluster's pods talk over; deploying onto a disabled overlay produces a
  # cluster whose nodes cannot reach each other, and that failure only shows up once
  # something tries to schedule.
  defp require_overlay_enabled(static) do
    if overlay_enabled?(static) do
      :ok
    else
      {:error,
       "Urbosa overlay networking is disabled. Enable it on Settings first; Kubernetes on " <>
         "no overlay is the failure that looks like success until a pod tries to talk."}
    end
  end

  defp overlay_enabled?(%{urbosa_enabled: value}), do: to_string(value) == "true"
  defp overlay_enabled?(%{} = _static), do: false
  defp overlay_enabled?(nil), do: Settings.urbosa_enabled?()

  # The cluster-wide object-name rule, not a second one written here: the name reaches a
  # CQL statement and a shell command on the leader, and two rules that are nearly the
  # same is how one of them ends up being the lenient one.
  defp cluster_name(value) do
    case Vm.validate_name(value && String.trim(to_string(value))) do
      {:ok, name} -> {:ok, name}
      {:error, message} -> {:error, "The cluster name " <> message <> "."}
    end
  end

  defp control_nodes(value, static) do
    ceiling = expected_nodes(static)

    case integer(value) do
      count when is_integer(count) and count >= 1 and count <= ceiling ->
        {:ok, count}

      count when is_integer(count) ->
        {:error,
         "A control plane of #{count} does not fit: this cluster has #{ceiling} node(s), and " <>
           "the control plane runs on them."}

      nil ->
        {:error, "The number of control-plane nodes is required."}
    end
  end

  # An empty segment is legal -- `deploy_lanayru_worker` builds default routing elements
  # when it is not given an overlay id -- but a segment that was named and does not exist
  # is a typo that would otherwise be discovered by the deploy.
  defp segment_id(value, static) do
    id = String.trim(to_string(value || ""))

    cond do
      id == "" ->
        {:ok, ""}

      Enum.any?(segments(static), &(to_string(&1.id) == id)) ->
        {:ok, id}

      true ->
        {:error, "No overlay segment with that id exists. Re-check the SDN page."}
    end
  end

  defp submitted({:ok, %{"task_id" => id}}, _context) when is_binary(id), do: {:ok, id}

  defp submitted({:ok, _other}, context),
    do: {:error, context <> ": Catalyst accepted the task but did not name it."}

  defp submitted({:error, reason}, context),
    do: {:error, context <> ": " <> Catalyst.describe(reason)}

  # -- sources ----------------------------------------------------------------------------

  defp expected_nodes(%{expected_nodes: count}), do: count
  defp expected_nodes(_static), do: max(length(Config.node_ips()), 1)

  defp ring(%{ring: ring}), do: ring

  defp ring(nil) do
    case Spark.db_ring(Config.local_ip()) do
      {:ok, body} when is_map(body) -> {:ok, Map.get(body, "nodes") || []}
      {:ok, _other} -> {:error, :unexpected_reply}
      {:error, reason} -> {:error, reason}
    end
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp ring(_static), do: {:error, :not_in_fixture}

  defp capacity(%{capacity: capacity}), do: capacity

  defp capacity(nil) do
    Spark.dfs_capacity(Config.local_ip())
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp capacity(_static), do: {:error, :not_in_fixture}

  defp memory(%{memory: memory}), do: memory

  defp memory(nil) do
    Spark.host_memory(Config.local_ip())
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp memory(_static), do: {:error, :not_in_fixture}

  # -- plumbing ---------------------------------------------------------------------------

  @doc """
  Where reads come from when the caller does not say: `:live`, or `{:static, map}` set in
  `Application.get_env(:spectrum_phx, :lanayru_source)`.

  The same seam `SpectrumPhx.Tasks` carries. A deploy button that has only ever been
  exercised against a live cluster is a deploy button nobody has exercised.
  """
  def source, do: Application.get_env(:spectrum_phx, :lanayru_source, :live)

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

  defp read(nil, _key, cql) do
    Hydra.query(cql, [])
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp read(%{} = static, key, _cql) do
    case Map.get(static, key) do
      {:error, reason} -> {:error, reason}
      rows when is_list(rows) -> {:ok, rows}
      nil -> {:ok, []}
    end
  end

  defp stringify(row) when is_map(row) do
    Map.new(row, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp stringify(row), do: row

  defp string(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp string(_), do: nil

  defp integer(value) when is_integer(value), do: value

  defp integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {number, _} -> number
      :error -> nil
    end
  end

  defp integer(value) when is_float(value), do: round(value)
  defp integer(_), do: nil

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)
end
