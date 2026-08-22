defmodule SpectrumPhx.Metrics do
  @moduledoc """
  Per-node telemetry, read from `hydra.logos_metrics`, joined to what
  `SpectrumPhx.Cluster.Status` already knows about each node.

  ## Reading one partition at a time

  `logos_metrics` is `PRIMARY KEY (node_ip, timestamp)` with
  `CLUSTERING ORDER BY (timestamp DESC)` and a 24h TTL. `logos.py` writes one row per node
  every 30 seconds, so a three-node cluster holds roughly 8,600 live rows.

  `/api/cluster/metrics` fetched all of them -- `SELECT JSON * FROM hydra.logos_metrics`,
  no `WHERE`, no `LIMIT` -- on every poll, from every open browser, and then threw away
  everything but the newest 40 samples per host in JavaScript. That is a full cluster scan
  every 30 seconds per viewer to render 120 points.

  Here each node is read as its own single-partition query, `WHERE node_ip = ? LIMIT ?`,
  which the clustering order answers directly: the newest N rows, in order, touching one
  replica set. The limit is a *bound parameter*, not interpolated.

  ## What the table does and does not hold

  `cpu_pct` and `mem_pct` are percentages. `mem_total_kb` and `cpu_cores` are the node's
  capacity, added by a later `ALTER TABLE`, so rows written before that migration have
  them null.

  There is **no disk-usage column**. `disk_iops` and `disk_bandwidth_kbps` are I/O rate,
  not fullness, and the old metrics page labelled its third chart "Disk" while plotting
  `disk_iops`. Nothing in this table can answer "how full is that disk"; that lives behind
  `SpectrumPhx.Spark.host_disks/1`. The view says "Disk I/O" and means it.

  ## Test seam

  `fetch/1` accepts `:rows`, `:node_ips`, `:cluster` and `:error` directly, so the whole
  assembly path runs against fixture rows with no database and no cluster. The same may be
  set as `{:static, rows}` or `{:error, reason}` in
  `Application.get_env(:spectrum_phx, :metrics_source)` for tests that drive the LiveView
  through its real route.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Cluster.Status
  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Tasks

  @columns "node_ip, timestamp, cpu_pct, mem_pct, mem_total_kb, cpu_cores, " <>
             "disk_iops, disk_bandwidth_kbps, net_rx_kbps, net_tx_kbps"

  @node_cql "SELECT #{@columns} FROM hydra.logos_metrics WHERE node_ip = ? LIMIT ?"

  # Matches the window the old charts drew, which is 20 minutes at logos.py's 30s cadence.
  @default_window 40

  # logos.py samples every 30 seconds. Two missed samples is a node that has stopped
  # reporting, which is worth saying out loud rather than drawing its last value forever.
  @stale_after_seconds 90

  @pubsub SpectrumPhx.PubSub
  @topic "metrics"

  @doc "CQL used for each node's partition. Exposed so tests can pin the statement."
  def node_cql, do: @node_cql

  @doc "Seconds after which a node's newest sample is considered stale."
  def stale_after_seconds, do: @stale_after_seconds

  @doc "PubSub topic the dashboard subscribes to for pushed snapshots."
  def topic, do: @topic

  @doc "Subscribe the calling process to pushed metric snapshots."
  def subscribe, do: Phoenix.PubSub.subscribe(@pubsub, @topic)

  @doc "Broadcast a snapshot to every connected dashboard as `{:metrics, snapshot}`."
  def broadcast(snapshot) do
    Phoenix.PubSub.broadcast(@pubsub, @topic, {:metrics, snapshot})
  rescue
    ArgumentError -> :ok
  catch
    :exit, _reason -> :ok
  end

  @doc """
  A full snapshot of cluster telemetry.

      %{
        nodes: [node_view],
        summary: %{...},
        cluster: Cluster.Status snapshot,
        configured?: true,
        available?: true,
        error: nil,
        window: 40
      }

  `configured?` is false when there is no cluster at all -- a fresh dev machine with no
  `/etc/hci/cluster.json`. `available?` is false when Hydra could not be read. They are
  different problems with different answers, so they are reported separately, and neither
  is allowed to render as a healthy cluster of zero nodes.

  Options:

    * `:window` - samples to keep per node (default #{@default_window}).
    * `:rows` - a flat list of `logos_metrics` row maps to use instead of the database.
    * `:node_ips` - the nodes to show, instead of the configured ones.
    * `:cluster` - a `Cluster.Status` snapshot to join against, instead of fetching one.
    * `:error` - pretend the read failed with this reason.
  """
  def fetch(opts \\ []) do
    window = Keyword.get(opts, :window, @default_window)
    cluster = Keyword.get_lazy(opts, :cluster, &cluster_snapshot/0)
    now = DateTime.utc_now()

    case samples(opts, window) do
      {:ok, by_node} ->
        ips = node_ips(opts, cluster, by_node)
        nodes = Enum.map(ips, &node_view(&1, Map.get(by_node, &1, []), cluster, now))

        %{
          nodes: nodes,
          summary: summarize(nodes),
          cluster: cluster,
          configured?: ips != [],
          available?: true,
          error: nil,
          window: window
        }

      {:error, reason} ->
        ips = node_ips(opts, cluster, %{})
        nodes = Enum.map(ips, &node_view(&1, [], cluster, now))

        %{
          nodes: nodes,
          summary: summarize([]),
          cluster: cluster,
          configured?: ips != [],
          available?: false,
          error: describe(reason),
          window: window
        }
    end
  end

  @doc """
  Where metric reads come from: `:hydra` (the default), `{:static, rows}` or
  `{:error, reason}`.
  """
  def source, do: Application.get_env(:spectrum_phx, :metrics_source, :hydra)

  # -- sourcing --------------------------------------------------------------

  defp samples(opts, window) do
    cond do
      Keyword.keyword?(opts) and Keyword.has_key?(opts, :rows) ->
        {:ok, opts |> Keyword.fetch!(:rows) |> group(window)}

      Keyword.keyword?(opts) and Keyword.has_key?(opts, :error) ->
        {:error, Keyword.fetch!(opts, :error)}

      true ->
        from_source(opts, window)
    end
  end

  defp from_source(opts, window) do
    case source() do
      {:static, rows} when is_list(rows) ->
        {:ok, group(rows, window)}

      {:error, reason} ->
        {:error, reason}

      _hydra ->
        opts |> node_ips(nil, %{}) |> read_partitions(window)
    end
  end

  # One query per node, concurrently, each one a single-partition read. A node whose
  # partition cannot be read does not fail the whole page -- it is reported as not
  # reporting, which is what it is -- but if *every* node fails, the database itself is
  # the problem and that is said plainly instead of drawing an empty cluster.
  defp read_partitions([], _window), do: {:ok, %{}}

  defp read_partitions(ips, window) do
    results =
      ips
      |> Task.async_stream(fn ip -> {ip, read_partition(ip, window)} end,
        max_concurrency: max(length(ips), 1),
        timeout: 15_000,
        on_timeout: :kill_task,
        ordered: true
      )
      |> Enum.zip(ips)
      |> Enum.map(fn
        {{:ok, {ip, result}}, _ip} -> {ip, result}
        {_other, ip} -> {ip, {:error, :timeout}}
      end)

    failures = for {_ip, {:error, reason}} <- results, do: reason

    if length(failures) == length(ips) do
      {:error, List.first(failures)}
    else
      {:ok, for({ip, {:ok, rows}} <- results, into: %{}, do: {ip, sort_samples(rows)})}
    end
  end

  defp read_partition(ip, window) do
    case Hydra.query(@node_cql, [ip, window]) do
      {:ok, rows} -> {:ok, Enum.map(rows, &sample/1)}
      {:error, reason} -> {:error, reason}
    end
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  # Fixture rows arrive as one flat list; the live path has already partitioned them.
  defp group(rows, window) do
    rows
    |> Enum.map(&sample/1)
    |> Enum.group_by(& &1.node_ip)
    |> Map.new(fn {ip, samples} ->
      {ip, samples |> sort_samples() |> Enum.take(-window)}
    end)
  end

  # Oldest first: that is the order a chart is drawn in, and `List.last/1` is then the
  # current value.
  defp sort_samples(samples) do
    Enum.sort_by(samples, fn sample -> sample.at && DateTime.to_unix(sample.at, :millisecond) end)
  end

  defp cluster_snapshot do
    Status.fetch()
  rescue
    _exception -> nil
  catch
    :exit, _reason -> nil
  end

  defp node_ips(opts, cluster, by_node) do
    configured =
      cond do
        Keyword.keyword?(opts) and Keyword.has_key?(opts, :node_ips) ->
          Keyword.fetch!(opts, :node_ips)

        is_map(cluster) ->
          Enum.map(cluster.nodes, & &1.ip)

        true ->
          configured_ips()
      end

    # A node that is publishing metrics but is absent from cluster.json is still shown.
    # Dropping it would hide a host that is very much part of this cluster.
    configured ++ Enum.reject(Map.keys(by_node), &(&1 in configured))
  end

  defp configured_ips do
    Config.node_ips()
  rescue
    _exception -> []
  catch
    :exit, _reason -> []
  end

  # -- normalisation ---------------------------------------------------------

  defp sample(row) do
    row = stringify(row)
    total_kb = integer(Map.get(row, "mem_total_kb"))
    mem_pct = number(Map.get(row, "mem_pct"))

    %{
      node_ip: string(Map.get(row, "node_ip")) || "unknown",
      at: Tasks.timestamp(Map.get(row, "timestamp")),
      cpu_pct: number(Map.get(row, "cpu_pct")),
      mem_pct: mem_pct,
      mem_total_kb: total_kb,
      mem_used_kb: used_kb(total_kb, mem_pct),
      cpu_cores: integer(Map.get(row, "cpu_cores")),
      disk_iops: number(Map.get(row, "disk_iops")),
      disk_bandwidth_kbps: number(Map.get(row, "disk_bandwidth_kbps")),
      net_rx_kbps: number(Map.get(row, "net_rx_kbps")),
      net_tx_kbps: number(Map.get(row, "net_tx_kbps"))
    }
  end

  defp used_kb(nil, _pct), do: nil
  defp used_kb(_total, nil), do: nil
  defp used_kb(total, pct), do: round(total * pct / 100)

  defp node_view(ip, samples, cluster, now) do
    latest = List.last(samples)
    age = age_seconds(latest, now)
    node = cluster_node(cluster, ip)

    %{
      ip: ip,
      hostname: (node && node.hostname) || hostname_for(ip),
      state: (node && node.state) || :unknown,
      registered?: (node && node.registered?) || false,
      in_maintenance?: (node && node.in_maintenance?) || false,
      disks: node && node.disks,
      latest: latest,
      samples: samples,
      reporting?: latest != nil,
      age_seconds: age,
      stale?: is_integer(age) and age > @stale_after_seconds
    }
  end

  defp cluster_node(cluster, ip) when is_map(cluster) do
    Enum.find(cluster.nodes, &(&1.ip == ip))
  end

  defp cluster_node(_cluster, _ip), do: nil

  defp age_seconds(nil, _now), do: nil
  defp age_seconds(%{at: nil}, _now), do: nil
  defp age_seconds(%{at: at}, now), do: max(DateTime.diff(now, at, :second), 0)

  defp hostname_for(ip) do
    Config.hostname_for(ip)
  rescue
    _exception -> ip
  catch
    :exit, _reason -> ip
  end

  # -- summary ---------------------------------------------------------------

  # Averages are taken over *reporting* nodes only. Counting a silent node as 0% CPU
  # would drag the cluster average down and make a dead node look like a quiet one.
  defp summarize(nodes) do
    reporting = Enum.filter(nodes, & &1.reporting?)
    cpu = Enum.map(reporting, & &1.latest.cpu_pct)
    mem = Enum.map(reporting, & &1.latest.mem_pct)

    %{
      nodes_total: length(nodes),
      nodes_reporting: length(reporting),
      nodes_silent: length(nodes) - length(reporting),
      nodes_stale: Enum.count(nodes, & &1.stale?),
      cpu_avg: average(cpu),
      cpu_max: maximum(cpu),
      mem_avg: average(mem),
      mem_max: maximum(mem),
      cores_total: reporting |> Enum.map(& &1.latest.cpu_cores) |> sum_defined(),
      mem_total_kb: reporting |> Enum.map(& &1.latest.mem_total_kb) |> sum_defined()
    }
  end

  defp average(values) do
    case Enum.reject(values, &is_nil/1) do
      [] -> nil
      defined -> Enum.sum(defined) / length(defined)
    end
  end

  defp maximum(values) do
    case Enum.reject(values, &is_nil/1) do
      [] -> nil
      defined -> Enum.max(defined)
    end
  end

  defp sum_defined(values) do
    case Enum.reject(values, &is_nil/1) do
      [] -> nil
      defined -> Enum.sum(defined)
    end
  end

  @doc """
  Points for a sparkline over one field, as `{x, y}` in a 0..100 by 0..100 box.

  `max` bounds the y axis; percentages pass 100 and rates pass their own observed peak.
  Returns `[]` for fewer than two samples, because a single point is not a trend and
  drawing it as a flat line across the panel would claim history that is not there.
  """
  def spark_points(samples, field, max) do
    values = Enum.map(samples, fn sample -> Map.get(sample, field) end)

    if Enum.count(values, &is_number/1) < 2 do
      []
    else
      ceiling = if is_number(max) and max > 0, do: max, else: 1
      last = length(values) - 1

      values
      |> Enum.with_index()
      |> Enum.filter(fn {value, _index} -> is_number(value) end)
      |> Enum.map(fn {value, index} ->
        x = index / last * 100
        y = 100 - min(value / ceiling, 1) * 100
        {Float.round(x, 2), Float.round(y * 1.0, 2)}
      end)
    end
  end

  @doc "The largest value of `field` across every sample of every node, or nil."
  def peak(nodes, field) do
    nodes
    |> Enum.flat_map(& &1.samples)
    |> Enum.map(fn sample -> Map.get(sample, field) end)
    |> maximum()
  end

  # -- helpers ---------------------------------------------------------------

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

  defp number(value) when is_float(value), do: value
  defp number(value) when is_integer(value), do: value * 1.0

  defp number(value) when is_binary(value) do
    case Float.parse(String.trim(value)) do
      {float, _rest} -> float
      :error -> nil
    end
  end

  defp number(_value), do: nil

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
