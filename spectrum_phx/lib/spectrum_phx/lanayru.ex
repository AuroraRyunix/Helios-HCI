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

  ## Deploy and destroy are not offered here

  Both are Catalyst tasks that build or tear down a Kubernetes cluster across every node.
  They belong behind the task queue, where the ring reports them; wiring them to a button
  in this console before that is done would hide a long, failure-prone operation behind
  something that returns instantly.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Spark

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
            warning(:consensus, "Only #{up} of #{expected} members are UN. A deploy on a degraded ring can lose its metadata writes.")

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
              error(:storage, "The extent store is #{Float.round(used_pct, 1)}% full (#{summary}). A deploy would run it out.")

            used_pct >= @storage_warn_pct ->
              warning(:storage, "The extent store is #{Float.round(used_pct, 1)}% full (#{summary}).")

            true ->
              ready(:storage, "Extent store healthy: #{summary}.")
          end
        else
          # Zero capacity is not an empty store; it is a store that is not mounted.
          error(:storage, "The extent store reports no capacity, which usually means it is not mounted.")
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
          else: warning(:compute, "Only #{free} MB free on this host; the control plane may not fit.")

      {:error, reason} ->
        error(:compute, "Host memory could not be read: #{describe(reason)}")
    end
  end

  defp overlay_check(static) do
    case read(static, :segments, @segments_cql) do
      {:ok, []} ->
        # Kubernetes on no network is the failure that looks like success until a pod
        # tries to talk to another one.
        error(:overlay, "No overlay segment exists to put a cluster on. Create one on the SDN page first.")

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

  defp static_source(opts) do
    case Keyword.get(opts, :source, :live) do
      {:static, map} -> map
      :live -> nil
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
