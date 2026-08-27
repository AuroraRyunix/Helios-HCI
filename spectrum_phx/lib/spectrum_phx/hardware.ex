defmodule SpectrumPhx.Hardware do
  @moduledoc """
  Physical inventory of the hypervisors: processors, memory, disks and interfaces.

  This is the page an operator opens to answer "what is actually in these machines", so
  it reads the hosts rather than the database. Nothing here is cached in Hydra: a node's
  hardware is not cluster state, and a stale row describing a disk that has since been
  pulled is worse than no row.

  ## Four reads per node, in parallel, and none of them fatal

  Each node is asked for its CPU, memory, disks and network separately, and each answer
  is kept or lost on its own. A node whose `lsblk` fails still reports its processors;
  a node that is down at all still appears, marked down, rather than vanishing from an
  inventory. An inventory that silently omits a machine is worse than one that says it
  could not reach it -- the whole point is to be able to count what you have.

  ## `execute` is not used

  The Python console reads the processor by sending
  `nproc; grep -m1 "model name" /proc/cpuinfo` through `/api/v1/execute`, the general
  run-this-string-as-root endpoint. That endpoint is the last open P1 security item in
  TODO.md, and every call site is one more place it has to stay trusted. Spark grew a
  typed `/api/v1/host/cpu` for this instead, so porting the page removes a call site
  rather than adding one.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Spark

  @fanout_timeout_ms 15_000

  @doc """
  The inventory, one entry per configured node.

  `source: {:static, map}` supplies fixtures instead of reaching the hosts, so the page
  can be tested without a cluster. The map is keyed by read (`:cpu`, `:memory`, `:disks`,
  `:network`) and then by node IP.
  """
  def inventory(opts \\ []) do
    static = static_source(opts)
    nodes = nodes(static)

    cpu = fan_out(static, :cpu, nodes, &Spark.host_cpu/1)
    memory = fan_out(static, :memory, nodes, &Spark.host_memory/1)
    disks = fan_out(static, :disks, nodes, &Spark.host_disks/1)
    network = fan_out(static, :network, nodes, &Spark.host_network/1)

    entries =
      nodes
      |> Enum.with_index()
      |> Enum.map(fn {node, index} ->
        build(node, at(cpu, index), at(memory, index), at(disks, index), at(network, index))
      end)

    %{nodes: entries, summary: summarize(entries), configured?: nodes != []}
  end

  defp static_source(opts) do
    case Keyword.get(opts, :source, :live) do
      {:static, map} -> map
      :live -> nil
    end
  end

  defp nodes(nil), do: configured_nodes()

  defp nodes(%{} = static), do: Map.get(static, :nodes) || configured_nodes()

  defp configured_nodes do
    Enum.map(Config.node_ips(), fn ip -> %{ip: ip, hostname: Config.hostname_for(ip)} end)
  end

  defp at(results, index) do
    case Enum.at(results, index) do
      {_node, result} -> result
      nil -> {:error, :missing}
    end
  end

  # -- one node ------------------------------------------------------------------------

  defp build(node, cpu, memory, disks, network) do
    cpu_view = cpu_view(cpu)
    memory_view = memory_view(memory)
    disk_views = disk_views(disks)
    interface_views = interface_views(network)

    %{
      ip: node.ip,
      hostname: node.hostname,
      # A node is "reachable" when *any* read came back. One failing endpoint is a gap in
      # the record, not an absent machine.
      reachable?: Enum.any?([cpu, memory, disks, network], &match?({:ok, _}, &1)),
      cpu: cpu_view,
      memory: memory_view,
      disks: disk_views,
      interfaces: interface_views,
      errors: errors(cpu: cpu, memory: memory, disks: disks, network: network)
    }
  end

  defp errors(reads) do
    for {name, {:error, reason}} <- reads, do: %{read: name, reason: describe(reason)}
  end

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)

  defp cpu_view({:ok, body}) when is_map(body) do
    %{
      model: string(Map.get(body, "model")) || "unknown",
      cores: integer(Map.get(body, "cores")),
      physical_cores: integer(Map.get(body, "physical_cores")),
      sockets: integer(Map.get(body, "sockets")),
      load_average: load(Map.get(body, "load_average"))
    }
  end

  defp cpu_view(_), do: %{model: nil, cores: nil, physical_cores: nil, sockets: nil, load_average: nil}

  defp load(values) when is_list(values) do
    numbers = Enum.filter(values, &is_number/1)
    if numbers == [], do: nil, else: numbers
  end

  defp load(_), do: nil

  # The endpoint reports MiB. Bytes are what everything else on the page speaks, and a
  # page mixing the two is a page where somebody eventually compares them.
  defp memory_view({:ok, body}) when is_map(body) do
    total = mib_to_bytes(Map.get(body, "total_mb"))
    used = mib_to_bytes(Map.get(body, "used_mb"))

    %{
      total_bytes: total,
      used_bytes: used,
      free_bytes: mib_to_bytes(Map.get(body, "free_mb")),
      used_percent: percent(used, total)
    }
  end

  defp memory_view(_), do: %{total_bytes: nil, used_bytes: nil, free_bytes: nil, used_percent: nil}

  defp mib_to_bytes(value) when is_number(value), do: round(value * 1024 * 1024)

  defp mib_to_bytes(value) when is_binary(value) do
    case Float.parse(value) do
      {number, _} -> round(number * 1024 * 1024)
      :error -> nil
    end
  end

  defp mib_to_bytes(_), do: nil

  defp percent(_used, nil), do: nil
  defp percent(_used, 0), do: nil
  defp percent(nil, _total), do: nil
  defp percent(used, total), do: Float.round(used / total * 100, 1)

  @doc """
  Whole disks only, with their partitions folded away.

  `lsblk -J` nests partitions under their disk, and an inventory listing both counts the
  same platter several times. What an operator wants here is "what is physically in the
  machine", so partitions are summarised as a count and a mounted flag rather than
  listed.
  """
  def disk_views({:ok, body}) when is_map(body) do
    body
    |> Map.get("blockdevices")
    |> as_list()
    |> Enum.filter(fn device -> is_map(device) and Map.get(device, "type") == "disk" end)
    |> Enum.map(&disk_view/1)
  end

  def disk_views(_), do: []

  defp disk_view(device) do
    children = as_list(Map.get(device, "children"))

    %{
      name: string(Map.get(device, "name")) || "unknown",
      path: string(Map.get(device, "path")),
      size_bytes: integer(Map.get(device, "size")),
      model: string(Map.get(device, "model")),
      serial: string(Map.get(device, "serial")),
      # ROTA is 1 for spinning media. It is the one field that says what the disk *is*
      # rather than what is on it, and it is what an operator checks before assuming a
      # node can take a write-heavy workload.
      rotational?: rotational(Map.get(device, "rota")),
      partitions: length(children),
      mountpoints: mountpoints(device, children)
    }
  end

  defp rotational(1), do: true
  defp rotational(true), do: true
  defp rotational("1"), do: true
  defp rotational(0), do: false
  defp rotational(false), do: false
  defp rotational("0"), do: false
  defp rotational(_), do: nil

  defp mountpoints(device, children) do
    [device | children]
    |> Enum.flat_map(fn entry ->
      case string(Map.get(entry, "mountpoint")) do
        nil -> []
        point -> [point]
      end
    end)
    |> Enum.uniq()
  end

  defp interface_views({:ok, body}) when is_map(body) do
    body
    |> Map.get("addresses")
    |> as_list()
    |> Enum.filter(&is_map/1)
    |> Enum.map(fn entry ->
      %{
        name: string(Map.get(entry, "ifname")) || string(Map.get(entry, "name")) || "unknown",
        state: string(Map.get(entry, "operstate")),
        mac: string(Map.get(entry, "address")),
        addresses: addresses(entry)
      }
    end)
    |> Enum.reject(fn interface -> interface.name == "lo" end)
  end

  defp interface_views(_), do: []

  defp addresses(entry) do
    entry
    |> Map.get("addr_info")
    |> as_list()
    |> Enum.filter(&is_map/1)
    |> Enum.flat_map(fn info ->
      case {string(Map.get(info, "local")), Map.get(info, "prefixlen")} do
        {nil, _} -> []
        {address, prefix} when is_integer(prefix) -> ["#{address}/#{prefix}"]
        {address, _} -> [address]
      end
    end)
  end

  # -- cluster totals ------------------------------------------------------------------

  defp summarize(entries) do
    reachable = Enum.filter(entries, & &1.reachable?)

    %{
      nodes_total: length(entries),
      nodes_reachable: length(reachable),
      cores: sum_of(reachable, fn entry -> entry.cpu.cores end),
      memory_bytes: sum_of(reachable, fn entry -> entry.memory.total_bytes end),
      disks: Enum.reduce(reachable, 0, fn entry, acc -> acc + length(entry.disks) end),
      disk_bytes:
        Enum.reduce(reachable, 0, fn entry, acc ->
          acc + Enum.reduce(entry.disks, 0, fn disk, inner -> inner + (disk.size_bytes || 0) end)
        end)
    }
  end

  # nil when nothing reported, rather than 0: a cluster whose nodes are all unreachable
  # has an unknown core count, not none.
  defp sum_of(entries, extract) do
    case entries |> Enum.map(extract) |> Enum.filter(&is_integer/1) do
      [] -> nil
      values -> Enum.sum(values)
    end
  end

  # -- plumbing ------------------------------------------------------------------------

  defp fan_out(%{} = static, key, nodes, _fun) do
    fixtures = Map.get(static, key) || %{}
    Enum.map(nodes, fn node -> {node, Map.get(fixtures, node.ip, {:error, :not_in_fixture})} end)
  end

  defp fan_out(nil, _key, [], _fun), do: []

  defp fan_out(nil, _key, nodes, fun) do
    nodes
    |> Task.async_stream(fn node -> {node, safe_call(fun, node.ip)} end,
      max_concurrency: max(length(nodes), 1),
      timeout: @fanout_timeout_ms,
      on_timeout: :kill_task,
      ordered: true
    )
    |> Enum.zip(nodes)
    |> Enum.map(fn
      {{:ok, {node, result}}, _node} -> {node, result}
      {{:exit, reason}, node} -> {node, {:error, {:exit, reason}}}
    end)
  end

  defp safe_call(fun, ip) do
    case fun.(ip) do
      {:ok, body} -> {:ok, body}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_reply, other}}
    end
  rescue
    exception -> {:error, Exception.message(exception)}
  end

  defp as_list(value) when is_list(value), do: value
  defp as_list(_), do: []

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

  defp integer(_), do: nil
end
