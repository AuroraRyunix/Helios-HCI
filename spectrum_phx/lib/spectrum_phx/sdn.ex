defmodule SpectrumPhx.Sdn do
  @moduledoc """
  Urbosa's overlay network, assembled into the shape it actually has.

  The five tables are flat and reference each other by uuid: a segment names a T1, a T1
  names a T0, and a VM names a segment. Read separately they are four lists an operator
  has to join in their head. Read together they are a tree, which is what the topology
  view draws:

      T0 (uplink to the physical network)
       └── T1 router
            └── segment (VNI, CIDR, DHCP)
                 └── guest

  ## Everything dangling is kept, and said

  A segment whose `t1_link_id` matches no T1 is the interesting case, not an error to
  discard: it is an overlay network with no route off itself, and an operator staring at
  a guest that cannot reach anything needs to see exactly that. Orphans are collected
  under an explicit `:orphans` rather than dropped, and the topology draws them detached.

  ## The mesh is separate from the tree

  T0/T1/segments are *logical* -- rows an operator created. The tunnels between hosts are
  *physical* -- what those rows are carried over. They fail independently: a perfectly
  configured segment on a host whose tunnels are down is unreachable, and nothing in the
  logical tree shows it. Both are read, and the page draws them as two views of one
  fabric.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra

  @t0_cql "SELECT router_id, name, uplink_interface, uplink_ip, gateway_ip, nat_rules FROM hydra.urbosa_t0_routers"
  @t1_cql "SELECT router_id, name, t0_link_id, dhcp_enabled FROM hydra.urbosa_t1_routers"
  @segment_cql "SELECT segment_id, name, vni, t1_link_id, subnet_cidr, gateway_ip, dhcp_enabled, dhcp_start, dhcp_end FROM hydra.urbosa_segments"
  @firewall_cql "SELECT rule_id, description, source_ip, dest_ip, protocol, port, action, priority FROM hydra.urbosa_firewall_rules"
  @vm_cql "SELECT name, network_id, state, host_ip FROM hydra.vms"

  @doc "The CQL this module reads, exposed so tests can assert it stays bounded."
  def statements, do: %{t0: @t0_cql, t1: @t1_cql, segments: @segment_cql, firewall: @firewall_cql}

  @doc """
  The whole fabric: the logical tree, the firewall table, and the host mesh.

  `source: {:static, map}` supplies rows instead of querying, keyed `:t0`, `:t1`,
  `:segments`, `:firewall`, `:vms`, `:nodes`.
  """
  def fabric(opts \\ []) do
    static = static_source(opts)

    with {:ok, t0_rows} <- read(static, :t0, @t0_cql),
         {:ok, t1_rows} <- read(static, :t1, @t1_cql),
         {:ok, segment_rows} <- read(static, :segments, @segment_cql),
         {:ok, firewall_rows} <- read(static, :firewall, @firewall_cql),
         {:ok, vm_rows} <- read(static, :vms, @vm_cql) do
      assemble(t0_rows, t1_rows, segment_rows, firewall_rows, vm_rows, nodes(static))
    else
      {:error, reason} -> unavailable(describe(reason), nodes(static))
    end
  end

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

  defp nodes(nil), do: Config.node_ips()
  defp nodes(%{} = static), do: Map.get(static, :nodes) || Config.node_ips()

  defp unavailable(reason, node_ips) do
    %{
      available?: false,
      error: reason,
      tier0: [],
      orphans: %{tier1: [], segments: []},
      segments: [],
      firewall: [],
      nodes: Enum.map(node_ips, &%{ip: &1, hostname: Config.hostname_for(&1)}),
      summary: summarize([], [], [], [], node_ips)
    }
  end

  @doc """
  Per-segment tunnel throughput, grouped by node.

  Read separately from `fabric/1` and on its own clock: the tree is one database read and
  this walks every node and segment on the far side. A page that refreshed both together
  would make the cheap half as slow as the expensive one.

  Asked of one node, because the daemon already answers for the whole cluster. The first
  node that answers wins; the rest are not asked, and a cluster where none answer reports
  that rather than an empty mesh, which would read as "no tunnels" -- the opposite of the
  truth when the tunnels exist and cannot be reached.
  """
  def tunnels(opts \\ []) do
    case static_source(opts) do
      %{} = static ->
        case Map.get(static, :tunnels) do
          {:error, reason} -> %{available?: false, error: describe(reason), nodes: []}
          rows when is_list(rows) -> %{available?: true, error: nil, nodes: group_tunnels(rows)}
          nil -> %{available?: true, error: nil, nodes: []}
        end

      nil ->
        ask_any(Config.node_ips())
    end
  end

  defp ask_any([]), do: %{available?: false, error: "no hosts configured", nodes: []}

  defp ask_any([ip | rest]) do
    case safely_get(ip) do
      {:ok, body} when is_map(body) ->
        rows = Map.get(body, "tunnels") || []
        %{available?: true, error: nil, nodes: group_tunnels(rows)}

      {:error, reason} ->
        if rest == [],
          do: %{available?: false, error: describe(reason), nodes: []},
          else: ask_any(rest)
    end
  end

  defp safely_get(ip) do
    SpectrumPhx.Spark.urbosa_tunnels(ip)
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp group_tunnels(rows) do
    rows
    |> Enum.map(&stringify/1)
    |> Enum.group_by(fn row -> string(get(row, "node_ip")) end)
    |> Enum.reject(fn {ip, _rows} -> is_nil(ip) end)
    |> Enum.map(fn {ip, node_rows} ->
      interfaces =
        node_rows
        |> Enum.map(fn row ->
          %{
            interface: string(get(row, "interface_name")) || "unknown",
            vni: integer(get(row, "vni")),
            segment: string(get(row, "segment_name")),
            rx_kbps: number(get(row, "rx_kbps")),
            tx_kbps: number(get(row, "tx_kbps"))
          }
        end)
        |> Enum.sort_by(&{&1.vni || 0, &1.interface})

      %{
        ip: ip,
        hostname: string(get(hd(node_rows), "node_name")) || Config.hostname_for(ip),
        interfaces: interfaces,
        rx_kbps: interfaces |> Enum.map(& &1.rx_kbps) |> sum_numbers(),
        tx_kbps: interfaces |> Enum.map(& &1.tx_kbps) |> sum_numbers()
      }
    end)
    |> Enum.sort_by(& &1.ip)
  end

  defp sum_numbers(values) do
    case Enum.filter(values, &is_number/1) do
      [] -> nil
      numbers -> Enum.sum(numbers)
    end
  end

  defp number(value) when is_number(value), do: value

  defp number(value) when is_binary(value) do
    case Float.parse(value) do
      {parsed, _} -> parsed
      :error -> nil
    end
  end

  defp number(_), do: nil

  # -- assembly ------------------------------------------------------------------------

  defp assemble(t0_rows, t1_rows, segment_rows, firewall_rows, vm_rows, node_ips) do
    guests_by_segment = guests_by_segment(vm_rows)

    segments =
      segment_rows
      |> Enum.map(&segment(&1, guests_by_segment))
      |> Enum.sort_by(& &1.name)

    segments_by_t1 = Enum.group_by(segments, & &1.t1_id)

    tier1 =
      t1_rows
      |> Enum.map(&tier1(&1, segments_by_t1))
      |> Enum.sort_by(& &1.name)

    tier1_by_t0 = Enum.group_by(tier1, & &1.t0_id)
    t0_ids = MapSet.new(t0_rows, &uuid(get(&1, "router_id")))
    t1_ids = MapSet.new(tier1, & &1.id)

    tier0 =
      t0_rows
      |> Enum.map(&tier0(&1, tier1_by_t0))
      |> Enum.sort_by(& &1.name)

    orphan_t1 = Enum.reject(tier1, fn router -> MapSet.member?(t0_ids, router.t0_id) end)
    orphan_segments = Enum.reject(segments, fn segment -> MapSet.member?(t1_ids, segment.t1_id) end)

    %{
      available?: true,
      error: nil,
      tier0: tier0,
      orphans: %{tier1: orphan_t1, segments: orphan_segments},
      segments: segments,
      firewall: firewall_rows |> Enum.map(&rule/1) |> Enum.sort_by(&{&1.priority, &1.description}),
      nodes: Enum.map(node_ips, &%{ip: &1, hostname: Config.hostname_for(&1)}),
      summary: summarize(tier0, tier1, segments, firewall_rows, node_ips)
    }
  end

  defp guests_by_segment(vm_rows) do
    vm_rows
    |> Enum.map(&stringify/1)
    |> Enum.flat_map(fn row ->
      case string(get(row, "network_id")) do
        nil ->
          []

        segment_id ->
          [
            {segment_id,
             %{
               name: string(get(row, "name")) || "unnamed",
               state: string(get(row, "state")) || "unknown",
               host_ip: string(get(row, "host_ip"))
             }}
          ]
      end
    end)
    |> Enum.group_by(fn {segment_id, _guest} -> segment_id end, fn {_id, guest} -> guest end)
  end

  defp tier0(row, tier1_by_t0) do
    row = stringify(row)
    id = uuid(get(row, "router_id"))
    routers = Map.get(tier1_by_t0, id, [])

    %{
      id: id,
      name: string(get(row, "name")) || "unnamed",
      uplink_interface: string(get(row, "uplink_interface")),
      uplink_ip: string(get(row, "uplink_ip")),
      gateway_ip: string(get(row, "gateway_ip")),
      nat_rules: string(get(row, "nat_rules")),
      tier1: routers,
      segment_count: Enum.reduce(routers, 0, fn router, acc -> acc + length(router.segments) end)
    }
  end

  defp tier1(row, segments_by_t1) do
    row = stringify(row)
    id = uuid(get(row, "router_id"))

    %{
      id: id,
      name: string(get(row, "name")) || "unnamed",
      t0_id: uuid(get(row, "t0_link_id")),
      dhcp_enabled?: get(row, "dhcp_enabled") == true,
      segments: Map.get(segments_by_t1, id, [])
    }
  end

  defp segment(row, guests_by_segment) do
    row = stringify(row)
    id = uuid(get(row, "segment_id"))

    %{
      id: id,
      name: string(get(row, "name")) || "unnamed",
      vni: integer(get(row, "vni")),
      t1_id: uuid(get(row, "t1_link_id")),
      subnet_cidr: string(get(row, "subnet_cidr")),
      gateway_ip: string(get(row, "gateway_ip")),
      dhcp_enabled?: get(row, "dhcp_enabled") == true,
      dhcp_range: dhcp_range(get(row, "dhcp_start"), get(row, "dhcp_end")),
      guests: Map.get(guests_by_segment, id, [])
    }
  end

  defp dhcp_range(start_ip, end_ip) do
    case {string(start_ip), string(end_ip)} do
      {nil, _} -> nil
      {_, nil} -> nil
      {from, to} -> from <> " – " <> to
    end
  end

  defp rule(row) do
    row = stringify(row)

    %{
      id: uuid(get(row, "rule_id")),
      description: string(get(row, "description")) || "(no description)",
      source: string(get(row, "source_ip")) || "any",
      destination: string(get(row, "dest_ip")) || "any",
      protocol: string(get(row, "protocol")) || "any",
      port: integer(get(row, "port")),
      # An unreadable action is never rendered as "allow". A firewall table that guesses
      # permissive when it cannot read a row is worse than one that says it cannot.
      action: action(string(get(row, "action"))),
      priority: integer(get(row, "priority")) || 0
    }
  end

  defp action(value) when is_binary(value) do
    case String.downcase(value) do
      "allow" -> :allow
      "accept" -> :allow
      "deny" -> :deny
      "drop" -> :deny
      "reject" -> :deny
      _ -> :unknown
    end
  end

  defp action(_), do: :unknown

  defp summarize(tier0, tier1, segments, firewall_rows, node_ips) do
    %{
      tier0: length(tier0),
      tier1: length(tier1),
      segments: length(segments),
      guests_attached: Enum.reduce(segments, 0, fn segment, acc -> acc + length(segment.guests) end),
      firewall_rules: length(firewall_rows),
      nodes: length(node_ips)
    }
  end

  # -- row helpers ---------------------------------------------------------------------

  defp stringify(row) when is_map(row) do
    Map.new(row, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp stringify(row), do: row

  defp get(row, key) when is_map(row), do: Map.get(row, key)
  defp get(_row, _key), do: nil

  # uuids arrive as binaries from Xandra and as strings from fixtures; both compare as
  # strings, which is all the joins here need.
  defp uuid(value) when is_binary(value), do: value
  defp uuid(_), do: nil

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

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(%{message: message}) when is_binary(message), do: message
  defp describe(reason), do: inspect(reason)
end
