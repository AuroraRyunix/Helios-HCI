defmodule SpectrumPhx.Cluster.Status do
  @moduledoc """
  Assembles the cluster's current state for the dashboard LiveViews.

  There are two sources, in preference order:

    1. **ZooKeeper** -- `SpectrumPhx.Zk.State.read_cluster_state/0`. Every node holds an
       *ephemeral* znode under `/helios/nodes/<ip>`, so a node that dies has its entry
       removed by the ensemble rather than by anyone noticing and cleaning up. Liveness
       stops being a sample and becomes a fact. That is why a node that is *configured*
       but has no registration is reported `:down` here, and not "unknown": the absence
       of the znode is the signal.

    2. **Probe fallback** -- when ZooKeeper cannot be read we fan `Spark.node_status/1`
       across the configured nodes concurrently. That is the old polling model, so the
       snapshot is tagged `source: :probe` and the UI says so; liveness under `:probe`
       really is only a sample and should not be presented as though it were not.

  ## Service status

  Per-service `status` is one of `"UP"`, `"DOWN"` or `"FLAPPING"`. `FLAPPING` means the
  unit is `active` according to systemd but has no main PID and a restart history --
  i.e. it was sampled inside a restart window of a `Restart=always` unit. The previous
  UI reported exactly that case as healthy, which is the bug this view exists to make
  impossible, so anything not positively `UP` is counted as not-up.

  ## Test seam

  `fetch/1` accepts `:nodes`/`:desired`/`:source`/`:node_ips` directly, so the whole
  assembly path is exercisable without a cluster. The same shape may be placed in
  `Application.get_env(:spectrum_phx, :cluster_status_override)` for tests that drive
  the LiveViews through their real routes. When neither is set, nothing here consults
  the application environment and the live path is taken.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Spark

  require Logger

  @stale_after_seconds 30
  @probe_timeout_ms 20_000
  @topic "cluster:status"

  @up "UP"
  @down "DOWN"
  @flapping "FLAPPING"

  @type source :: :zookeeper | :probe | :unconfigured

  @doc "PubSub topic the dashboards subscribe to for pushed snapshots."
  def topic, do: @topic

  @doc "Subscribe the calling process to pushed cluster snapshots."
  def subscribe, do: Phoenix.PubSub.subscribe(SpectrumPhx.PubSub, @topic)

  @doc """
  Broadcast a snapshot to every subscribed dashboard as `{:cluster_status, snapshot}`.

  Nothing in this module calls it; it is the hook for whichever process ends up
  watching ZooKeeper, so the dashboards can stop polling entirely.
  """
  def broadcast(snapshot) do
    Phoenix.PubSub.broadcast(SpectrumPhx.PubSub, @topic, {:cluster_status, snapshot})
  end

  @doc "Seconds after which a node's published document is considered stale."
  def stale_after_seconds, do: @stale_after_seconds

  @doc """
  Full cluster snapshot.

      %{
        nodes: [node_view],
        desired: "started" | "stopped" | nil,
        source: :zookeeper | :probe | :unconfigured,
        configured?: boolean,
        error: binary | nil,
        summary: map
      }

  `error` carries why ZooKeeper was not used, when it was not.
  """
  def fetch(opts \\ []) do
    opts
    |> raw_snapshot()
    |> assemble()
  end

  @doc """
  Counts across the whole cluster.

      %{
        total_nodes: 3, nodes_up: 2, nodes_down: 1, stale_nodes: 0,
        services_up: 33, services_down: 2, services_flapping: 1,
        desired: "started", source: :zookeeper
      }

  A node that is `:down` publishes no service list, so its services are counted
  nowhere; `nodes_down` is the number that matters for it.
  """
  def summary(opts \\ []), do: fetch(opts).summary

  @doc "The node view for a single IP, or `nil` when that IP is not part of the cluster."
  def node(ip, opts \\ []) do
    fetch(opts).nodes |> Enum.find(&(&1.ip == ip))
  end

  @doc "Module used to read ZooKeeper. Resolved at runtime -- see the moduledoc."
  def zk_module do
    Application.get_env(:spectrum_phx, :zk_state_module, SpectrumPhx.Zk.State)
  end

  # -- snapshot sourcing -----------------------------------------------------

  defp raw_snapshot(opts) do
    opts =
      cond do
        Keyword.keyword?(opts) and Keyword.has_key?(opts, :nodes) -> opts
        override = override_opts() -> override
        true -> []
      end

    if Keyword.keyword?(opts) and Keyword.has_key?(opts, :nodes) do
      from_opts(opts)
    else
      live_snapshot()
    end
  end

  defp override_opts do
    case Application.get_env(:spectrum_phx, :cluster_status_override) do
      nil -> nil
      map when is_map(map) -> Map.to_list(map)
      list when is_list(list) -> list
      _ -> nil
    end
  end

  defp from_opts(opts) do
    nodes = opts |> Keyword.fetch!(:nodes) |> stringify_keys()

    %{
      nodes: nodes,
      desired: Keyword.get(opts, :desired),
      source: Keyword.get(opts, :source, :zookeeper),
      node_ips: Keyword.get(opts, :node_ips) || Map.keys(nodes),
      error: describe(Keyword.get(opts, :error))
    }
  end

  defp live_snapshot do
    configured = configured_ips()

    case read_zookeeper() do
      {:ok, state} ->
        nodes = state |> Map.get(:nodes) |> stringify_keys()

        %{
          nodes: nodes,
          desired: Map.get(state, :desired),
          source: :zookeeper,
          # Configured order first; a node ZooKeeper knows about but cluster.json does
          # not is still shown rather than silently dropped.
          node_ips: union(configured, Map.keys(nodes)),
          error: nil
        }

      {:error, reason} ->
        probe_snapshot(configured, reason)
    end
  end

  defp probe_snapshot([], reason) do
    %{nodes: %{}, desired: nil, source: :unconfigured, node_ips: [], error: describe(reason)}
  end

  defp probe_snapshot(ips, reason) do
    Logger.debug("ZooKeeper unavailable (#{describe(reason)}); probing #{length(ips)} nodes.")
    %{nodes: probe(ips), desired: nil, source: :probe, node_ips: ips, error: describe(reason)}
  end

  # `SpectrumPhx.Zk.State` belongs to another part of the rewrite. It is resolved
  # through `Application.get_env/3` and checked with `Code.ensure_loaded?/1` +
  # `function_exported?/3`, so this module compiles, boots and tests whether or not that
  # module exists yet -- and never emits a compile-time reference to it.
  defp read_zookeeper do
    mod = zk_module()

    if Code.ensure_loaded?(mod) and function_exported?(mod, :read_cluster_state, 0) do
      try do
        case apply(mod, :read_cluster_state, []) do
          {:ok, state} when is_map(state) -> {:ok, state}
          {:error, reason} -> {:error, reason}
          other -> {:error, {:unexpected_reply, other}}
        end
      rescue
        exception -> {:error, Exception.message(exception)}
      catch
        :exit, reason -> {:error, {:exit, reason}}
      end
    else
      {:error, :zookeeper_reader_unavailable}
    end
  end

  defp probe(ips) do
    ips
    |> Task.async_stream(&safe_node_status/1,
      max_concurrency: max(length(ips), 1),
      timeout: @probe_timeout_ms,
      on_timeout: :kill_task,
      ordered: true
    )
    |> Enum.zip(ips)
    |> Enum.reduce(%{}, fn
      {{:ok, {:ok, doc}}, ip}, acc when is_map(doc) -> Map.put(acc, ip, doc)
      # Unreachable, timed out, or answered with something unusable: the node is down.
      {_result, _ip}, acc -> acc
    end)
  end

  defp safe_node_status(ip) do
    Spark.node_status(ip)
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp configured_ips do
    Config.node_ips()
  catch
    :exit, _reason -> []
  end

  # -- assembly --------------------------------------------------------------

  defp assemble(raw) do
    now = System.system_time(:second)
    ips = raw.node_ips
    source = if ips == [], do: :unconfigured, else: raw.source
    nodes = Enum.map(ips, fn ip -> node_view(ip, Map.get(raw.nodes, ip), now) end)

    %{
      nodes: nodes,
      desired: raw.desired,
      source: source,
      configured?: ips != [],
      error: raw.error,
      summary: summarize(nodes, raw.desired, source)
    }
  end

  # No document means no registration means down. This is the whole point of the
  # ephemeral-znode model and it has to survive into the UI intact.
  defp node_view(ip, nil, _now) do
    %{
      ip: ip,
      hostname: hostname_for(ip),
      state: :down,
      registered?: false,
      zk_leader?: false,
      maintenance: nil,
      in_maintenance?: false,
      disks: nil,
      build: nil,
      ts: nil,
      age_seconds: nil,
      stale?: false,
      services: [],
      counts: %{up: 0, down: 0, flapping: 0, total: 0}
    }
  end

  defp node_view(ip, raw, now) when is_map(raw) do
    ts = integer(Map.get(raw, "ts"))
    age = ts && max(now - ts, 0)
    services = services(raw)
    maintenance = Map.get(raw, "maintenance_status") || Map.get(raw, "maintenance")

    %{
      ip: Map.get(raw, "ip") || ip,
      hostname: blank_to_nil(Map.get(raw, "hostname")) || hostname_for(ip),
      state: :up,
      registered?: true,
      zk_leader?: Map.get(raw, "zk_leader") == true,
      maintenance: maintenance,
      in_maintenance?: is_binary(maintenance) and maintenance != "NORMAL",
      disks: integer(Map.get(raw, "disks")),
      build: blank_to_nil(Map.get(raw, "build")),
      ts: ts,
      age_seconds: age,
      stale?: is_integer(age) and age > @stale_after_seconds,
      services: services,
      counts: count_services(services)
    }
  end

  defp services(raw) do
    raw
    |> Map.get("services")
    |> case do
      map when is_map(map) -> map
      _ -> %{}
    end
    |> Enum.map(fn {name, value} -> service(name, value) end)
    |> Enum.sort_by(& &1.name)
  end

  defp service(name, value) when is_map(value) do
    %{
      name: to_string(name),
      status: normalize_status(Map.get(value, "status")),
      pids: pids(Map.get(value, "pids")),
      restarts: integer(Map.get(value, "restarts")) || 0
    }
  end

  defp service(name, value) when is_binary(value) do
    %{name: to_string(name), status: normalize_status(value), pids: [], restarts: 0}
  end

  defp service(name, _value) do
    %{name: to_string(name), status: @down, pids: [], restarts: 0}
  end

  defp normalize_status(value) when is_binary(value) do
    case String.upcase(String.trim(value)) do
      @up -> @up
      @down -> @down
      @flapping -> @flapping
      other -> other
    end
  end

  defp normalize_status(_value), do: @down

  defp pids(list) when is_list(list), do: Enum.reject(list, &is_nil/1)
  defp pids(pid) when is_integer(pid), do: [pid]
  defp pids(_other), do: []

  # Only a positive "UP" counts as up. Anything unrecognised counts as down, because
  # reporting an unknown state as healthy is precisely the failure being designed out.
  defp count_services(services) do
    Enum.reduce(services, %{up: 0, down: 0, flapping: 0, total: 0}, fn svc, acc ->
      key =
        case svc.status do
          @up -> :up
          @flapping -> :flapping
          _ -> :down
        end

      acc |> Map.update!(key, &(&1 + 1)) |> Map.update!(:total, &(&1 + 1))
    end)
  end

  defp summarize(nodes, desired, source) do
    Enum.reduce(
      nodes,
      %{
        total_nodes: 0,
        nodes_up: 0,
        nodes_down: 0,
        stale_nodes: 0,
        services_up: 0,
        services_down: 0,
        services_flapping: 0,
        desired: desired,
        source: source
      },
      fn node, acc ->
        %{
          acc
          | total_nodes: acc.total_nodes + 1,
            nodes_up: acc.nodes_up + if(node.state == :up, do: 1, else: 0),
            nodes_down: acc.nodes_down + if(node.state == :down, do: 1, else: 0),
            stale_nodes: acc.stale_nodes + if(node.stale?, do: 1, else: 0),
            services_up: acc.services_up + node.counts.up,
            services_down: acc.services_down + node.counts.down,
            services_flapping: acc.services_flapping + node.counts.flapping
        }
      end
    )
  end

  # -- small helpers ---------------------------------------------------------

  defp hostname_for(ip) do
    Config.hostname_for(ip)
  catch
    :exit, _reason -> ip
  end

  defp stringify_keys(nil), do: %{}

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end

  defp stringify_keys(_other), do: %{}

  defp union(first, second), do: first ++ Enum.reject(second, &(&1 in first))

  defp integer(value) when is_integer(value), do: value
  defp integer(value) when is_float(value), do: trunc(value)

  defp integer(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {int, _rest} -> int
      :error -> nil
    end
  end

  defp integer(_value), do: nil

  defp blank_to_nil(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp blank_to_nil(_value), do: nil

  defp describe(nil), do: nil
  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)
end
