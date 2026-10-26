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

  # -- mutations -----------------------------------------------------------------------

  @vni_claim_cql "INSERT INTO hydra.urbosa_vni_claims (vni, segment_id, name, claimed_at_ms) VALUES (?, ?, ?, ?) IF NOT EXISTS"
  @vni_release_cql "DELETE FROM hydra.urbosa_vni_claims WHERE vni = ? IF segment_id = ?"
  @vni_range 1..16_777_215

  @doc "Generate a v4 UUID for SDN resources."
  def generate_id do
    SpectrumPhx.Networking.generate_net_id()
  end

  @doc "Whether `value` is a canonical UUID string."
  def uuid?(value), do: SpectrumPhx.Networking.uuid?(value)

  @doc "Create a Tier-0 gateway router in `hydra.urbosa_t0_routers`."
  def create_t0_router(attrs, opts \\ []) when is_map(attrs) do
    attrs = stringify(attrs)
    id = string(get(attrs, "router_id")) || generate_id()
    name = string(get(attrs, "name"))
    uplink_iface = string(get(attrs, "uplink_interface")) || string(get(attrs, "uplink_iface")) || "eth0"
    uplink_ip = string(get(attrs, "uplink_ip")) || ""
    gateway_ip = string(get(attrs, "gateway_ip")) || ""
    nat_rules = string(get(attrs, "nat_rules")) || "[]"

    cond do
      is_nil(name) ->
        {:error, "Tier-0 router name is required."}

      not uuid?(id) ->
        {:error, "Invalid router_id UUID."}

      true ->
        cql =
          "INSERT INTO hydra.urbosa_t0_routers (router_id, name, uplink_interface, uplink_ip, gateway_ip, nat_rules) " <>
            "VALUES (#{id}, ?, ?, ?, ?, ?)"

        case write(cql, [name, uplink_iface, uplink_ip, gateway_ip, nat_rules], opts) do
          :ok -> {:ok, id}
          {:error, reason} -> {:error, "Could not create Tier-0 router: #{describe(reason)}"}
        end
    end
  end

  @doc "Delete a Tier-0 gateway router from `hydra.urbosa_t0_routers`."
  def delete_t0_router(router_id, opts \\ []) do
    id = string(router_id)

    if uuid?(id) do
      case write("DELETE FROM hydra.urbosa_t0_routers WHERE router_id = #{id}", [], opts) do
        :ok -> :ok
        {:error, reason} -> {:error, "Could not delete Tier-0 router: #{describe(reason)}"}
      end
    else
      {:error, "Invalid Tier-0 router ID."}
    end
  end

  @doc "Create a Tier-1 logical router in `hydra.urbosa_t1_routers`."
  def create_t1_router(attrs, opts \\ []) when is_map(attrs) do
    attrs = stringify(attrs)
    id = string(get(attrs, "router_id")) || generate_id()
    name = string(get(attrs, "name"))
    t0_id = string(get(attrs, "t0_link_id")) || string(get(attrs, "t0_id"))
    dhcp_enabled = truthy?(get(attrs, "dhcp_enabled"))

    cond do
      is_nil(name) ->
        {:error, "Tier-1 router name is required."}

      not uuid?(id) ->
        {:error, "Invalid router_id UUID."}

      not is_nil(t0_id) and not uuid?(t0_id) ->
        {:error, "Invalid Tier-0 link UUID."}

      true ->
        t0_literal = if t0_id, do: t0_id, else: "NULL"

        cql =
          "INSERT INTO hydra.urbosa_t1_routers (router_id, name, t0_link_id, dhcp_enabled) " <>
            "VALUES (#{id}, ?, #{t0_literal}, ?)"

        case write(cql, [name, dhcp_enabled], opts) do
          :ok -> {:ok, id}
          {:error, reason} -> {:error, "Could not create Tier-1 router: #{describe(reason)}"}
        end
    end
  end

  @doc "Delete a Tier-1 logical router from `hydra.urbosa_t1_routers`."
  def delete_t1_router(router_id, opts \\ []) do
    id = string(router_id)

    if uuid?(id) do
      case write("DELETE FROM hydra.urbosa_t1_routers WHERE router_id = #{id}", [], opts) do
        :ok -> :ok
        {:error, reason} -> {:error, "Could not delete Tier-1 router: #{describe(reason)}"}
      end
    else
      {:error, "Invalid Tier-1 router ID."}
    end
  end

  @doc """
  Create an overlay segment in `hydra.urbosa_segments` after atomically claiming its VNI
  in `hydra.urbosa_vni_claims`.
  """
  def create_segment(attrs, opts \\ []) when is_map(attrs) do
    attrs = stringify(attrs)
    id = string(get(attrs, "segment_id")) || generate_id()
    name = string(get(attrs, "name"))
    vni = integer(get(attrs, "vni"))
    t1_id = string(get(attrs, "t1_link_id")) || string(get(attrs, "t1_id"))
    subnet_cidr = string(get(attrs, "subnet_cidr"))
    gateway_ip = string(get(attrs, "gateway_ip")) || ""
    dhcp_enabled = truthy?(get(attrs, "dhcp_enabled"))
    dhcp_start = string(get(attrs, "dhcp_start")) || ""
    dhcp_end = string(get(attrs, "dhcp_end")) || ""

    cond do
      is_nil(name) ->
        {:error, "Segment name is required."}

      not uuid?(id) ->
        {:error, "Invalid segment_id UUID."}

      is_nil(vni) or vni not in @vni_range ->
        {:error, "VNI must be an integer between 1 and 16777215."}

      is_nil(subnet_cidr) or not String.contains?(subnet_cidr, "/") ->
        {:error, "A valid subnet CIDR (e.g. 10.20.1.0/24) is required."}

      not is_nil(t1_id) and not uuid?(t1_id) ->
        {:error, "Invalid Tier-1 router UUID."}

      true ->
        with :ok <- claim_vni(vni, id, name, opts) do
          t1_literal = if t1_id, do: t1_id, else: "NULL"

          cql =
            "INSERT INTO hydra.urbosa_segments " <>
              "(segment_id, name, vni, t1_link_id, subnet_cidr, gateway_ip, dhcp_enabled, dhcp_start, dhcp_end) " <>
              "VALUES (#{id}, ?, ?, #{t1_literal}, ?, ?, ?, ?, ?)"

          params = [name, vni, subnet_cidr, gateway_ip, dhcp_enabled, dhcp_start, dhcp_end]

          case write(cql, params, opts) do
            :ok ->
              {:ok, id}

            {:error, reason} ->
              _ = release_vni(vni, id, opts)
              {:error, "Could not create segment: #{describe(reason)}"}
          end
        end
    end
  end

  @doc """
  Delete an overlay segment from `hydra.urbosa_segments` and release its VNI claim.
  Refuses if any VMs are still attached to the segment.
  """
  def delete_segment(segment_id, opts \\ []) do
    id = string(segment_id)

    if not uuid?(id) do
      {:error, "Invalid segment ID."}
    else
      with :ok <- ensure_no_attached_guests(id, opts),
           {:ok, vni} <- segment_vni(id, opts),
           :ok <- write("DELETE FROM hydra.urbosa_segments WHERE segment_id = #{id}", [], opts) do
        _ = if vni, do: release_vni(vni, id, opts), else: :ok
        :ok
      end
    end
  end

  @doc "Create a distributed firewall rule in `hydra.urbosa_firewall_rules`."
  def create_firewall_rule(attrs, opts \\ []) when is_map(attrs) do
    attrs = stringify(attrs)
    id = string(get(attrs, "rule_id")) || generate_id()
    description = string(get(attrs, "description"))
    source_ip = string(get(attrs, "source_ip")) || "any"
    dest_ip = string(get(attrs, "dest_ip")) || "any"
    protocol = (string(get(attrs, "protocol")) || "any") |> String.downcase()
    port = integer(get(attrs, "port")) || 0
    action_raw = (string(get(attrs, "action")) || "") |> String.downcase()
    priority = integer(get(attrs, "priority")) || 100

    cond do
      is_nil(description) ->
        {:error, "Firewall rule description is required."}

      not uuid?(id) ->
        {:error, "Invalid rule_id UUID."}

      action_raw not in ~w(allow deny accept drop reject) ->
        {:error, "Firewall action must be allow or deny."}

      protocol not in ~w(any tcp udp icmp) ->
        {:error, "Protocol must be any, tcp, udp, or icmp."}

      true ->
        normalized_action = if action_raw in ~w(allow accept), do: "allow", else: "deny"

        cql =
          "INSERT INTO hydra.urbosa_firewall_rules " <>
            "(rule_id, description, source_ip, dest_ip, protocol, port, action, priority) " <>
            "VALUES (#{id}, ?, ?, ?, ?, ?, ?, ?)"

        params = [description, source_ip, dest_ip, protocol, port, normalized_action, priority]

        case write(cql, params, opts) do
          :ok -> {:ok, id}
          {:error, reason} -> {:error, "Could not create firewall rule: #{describe(reason)}"}
        end
    end
  end

  @doc "Delete a distributed firewall rule from `hydra.urbosa_firewall_rules`."
  def delete_firewall_rule(rule_id, opts \\ []) do
    id = string(rule_id)

    if uuid?(id) do
      case write("DELETE FROM hydra.urbosa_firewall_rules WHERE rule_id = #{id}", [], opts) do
        :ok -> :ok
        {:error, reason} -> {:error, "Could not delete firewall rule: #{describe(reason)}"}
      end
    else
      {:error, "Invalid firewall rule ID."}
    end
  end

  defp claim_vni(vni, segment_id, name, opts) do
    case static_source(opts) do
      %{} = static ->
        report(static, {:vni_claim, vni, segment_id})

        case static
             |> Map.get(:vni_claims, [])
             |> Enum.map(&stringify/1)
             |> Enum.find(&(integer(get(&1, "vni")) == vni)) do
          nil ->
            :ok

          holder ->
            holder_name = string(get(holder, "name")) || "another segment"
            {:error, "VNI #{vni} is already claimed by #{holder_name}."}
        end

      nil ->
        now_ms = System.system_time(:millisecond)

        case lwt(@vni_claim_cql, [vni, segment_id, name, now_ms]) do
          {:ok, true, _row} ->
            :ok

          {:ok, false, holder} ->
            holder = stringify(holder || %{})
            holder_name = string(get(holder, "name")) || string(get(holder, "segment_id")) || "another segment"
            {:error, "VNI #{vni} is already claimed by #{holder_name}."}

          {:error, reason} ->
            {:error, "Could not claim VNI #{vni}: #{describe(reason)}"}
        end
    end
  end

  defp release_vni(vni, segment_id, opts) do
    case static_source(opts) do
      %{} = static ->
        report(static, {:vni_release, vni, segment_id})
        :ok

      nil ->
        case lwt(@vni_release_cql, [vni, segment_id]) do
          {:ok, _, _} -> :ok
          {:error, reason} -> {:error, describe(reason)}
        end
    end
  end

  defp ensure_no_attached_guests(segment_id, opts) do
    case read(static_source(opts), :vms, @vm_cql) do
      {:ok, vm_rows} ->
        attached =
          vm_rows
          |> Enum.map(&stringify/1)
          |> Enum.filter(&(string(get(&1, "network_id")) == segment_id))

        if attached == [] do
          :ok
        else
          names = Enum.map_join(attached, ", ", &(string(get(&1, "name")) || "unnamed"))
          {:error, "Segment still has attached guests (#{names}) and cannot be deleted."}
        end

      {:error, reason} ->
        {:error, "Could not verify attached guests before deleting segment: #{describe(reason)}"}
    end
  end

  defp segment_vni(segment_id, opts) do
    case read(static_source(opts), :segments, @segment_cql) do
      {:ok, rows} ->
        vni =
          rows
          |> Enum.map(&stringify/1)
          |> Enum.find_value(fn row ->
            if uuid(get(row, "segment_id")) == segment_id, do: integer(get(row, "vni"))
          end)

        {:ok, vni}

      {:error, reason} ->
        {:error, "Could not read segment before delete: #{describe(reason)}"}
    end
  end

  defp write(cql, params, opts) do
    case static_source(opts) do
      %{} = static ->
        report(static, {:write, cql, params})
        Map.get(static, :write_result, :ok)

      nil ->
        try do
          case Hydra.query(cql, params) do
            {:ok, _} -> :ok
            {:error, reason} -> {:error, reason}
          end
        rescue
          exception -> {:error, Exception.message(exception)}
        catch
          :exit, reason -> {:error, {:exit, reason}}
        end
    end
  end

  defp lwt(cql, params) do
    Hydra.apply_lwt_row(cql, params)
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp report(%{sink: pid}, message) when is_pid(pid), do: send(pid, message)
  defp report(_static, _message), do: :ok

  defp truthy?(true), do: true
  defp truthy?("true"), do: true
  defp truthy?("1"), do: true
  defp truthy?("on"), do: true
  defp truthy?(1), do: true
  defp truthy?(_), do: false

  @bgp_config_cql "SELECT config_id, asn, router_id, evpn_enabled, arp_suppression, disable_headend_flood, route_reflectors, bfd_enabled, updated_at_ms FROM hydra.urbosa_bgp_config"
  @bgp_peers_cql "SELECT peer_ip, remote_asn, description, bfd_enabled, route_reflector_client, created_at_ms FROM hydra.urbosa_bgp_peers"

  @doc "Read Scale-Out Urbosa BGP EVPN configuration and configured peers."
  def bgp_state(opts \\ []) do
    static = static_source(opts)

    with {:ok, cfg_rows} <- read(static, :bgp_config, @bgp_config_cql),
         {:ok, peer_rows} <- read(static, :bgp_peers, @bgp_peers_cql) do
      cfg =
        cfg_rows
        |> Enum.map(&stringify/1)
        |> List.first()
        |> case do
          nil ->
            %{
              asn: 65000,
              router_id: "",
              evpn_enabled?: false,
              arp_suppression?: false,
              disable_headend_flood?: false,
              route_reflectors: "",
              bfd_enabled?: true
            }

          row ->
            %{
              asn: integer(get(row, "asn")) || 65000,
              router_id: get(row, "router_id") || "",
              evpn_enabled?: truthy?(get(row, "evpn_enabled")),
              arp_suppression?: truthy?(get(row, "arp_suppression")),
              disable_headend_flood?: truthy?(get(row, "disable_headend_flood")),
              route_reflectors: get(row, "route_reflectors") || "",
              bfd_enabled?: truthy?(Map.get(row, "bfd_enabled", true))
            }
        end

      peers =
        peer_rows
        |> Enum.map(&stringify/1)
        |> Enum.map(fn r ->
          %{
            peer_ip: get(r, "peer_ip") || "",
            remote_asn: integer(get(r, "remote_asn")) || 65000,
            description: get(r, "description") || "",
            bfd_enabled?: truthy?(Map.get(r, "bfd_enabled", true)),
            route_reflector_client?: truthy?(get(r, "route_reflector_client"))
          }
        end)
        |> Enum.sort_by(& &1.peer_ip)

      {:ok, %{available?: true, error: nil, config: cfg, peers: peers}}
    else
      {:error, reason} ->
        {:ok,
         %{
           available?: false,
           error: describe(reason),
           config: %{
             asn: 65000,
             router_id: "",
             evpn_enabled?: false,
             arp_suppression?: false,
             disable_headend_flood?: false,
             route_reflectors: "",
             bfd_enabled?: true
           },
           peers: []
         }}
    end
  end

  @doc "Update cluster-wide FRRouting BGP EVPN and ARP suppression configuration."
  def configure_bgp(params, opts \\ []) when is_map(params) do
    p = stringify(params)
    asn = integer(get(p, "asn")) || 65000

    if asn < 1 or asn > 4_294_967_294 do
      {:error, "BGP ASN must be between 1 and 4294967294."}
    else
      router_id = get(p, "router_id") || ""
      evpn? = truthy?(get(p, "evpn_enabled"))
      arp_supp? = truthy?(get(p, "arp_suppression"))
      no_flood? = truthy?(get(p, "disable_headend_flood"))
      bfd? = truthy?(Map.get(p, "bfd_enabled", true))
      rr = get(p, "route_reflectors") || ""
      now_ms = System.system_time(:millisecond)

      cql =
        "INSERT INTO hydra.urbosa_bgp_config (config_id, asn, router_id, evpn_enabled, arp_suppression, disable_headend_flood, route_reflectors, bfd_enabled, updated_at_ms) " <>
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

      case write(cql, ["cluster", asn, router_id, evpn?, arp_supp?, no_flood?, rr, bfd?, now_ms], opts) do
        :ok ->
          reload_host_bgp(opts)
          :ok

        {:error, reason} ->
          {:error, "Could not save BGP EVPN configuration: #{describe(reason)}"}
      end
    end
  end

  @doc "Register a BGP EVPN neighbor in `hydra.urbosa_bgp_peers` and trigger FRR reload."
  def add_bgp_peer(params, opts \\ []) when is_map(params) do
    p = stringify(params)
    peer_ip = string(get(p, "peer_ip"))
    remote_asn = integer(get(p, "remote_asn")) || 65000
    description = string(get(p, "description")) || ""
    bfd? = truthy?(Map.get(p, "bfd_enabled", true))
    rr_client? = truthy?(get(p, "route_reflector_client"))

    cond do
      is_nil(peer_ip) or not valid_ipv4?(peer_ip) ->
        {:error, "A valid IPv4 peer address is required."}

      remote_asn < 1 or remote_asn > 4_294_967_294 ->
        {:error, "Remote ASN must be between 1 and 4294967294."}

      true ->
        now_ms = System.system_time(:millisecond)

        cql =
          "INSERT INTO hydra.urbosa_bgp_peers (peer_ip, remote_asn, description, bfd_enabled, route_reflector_client, created_at_ms) " <>
            "VALUES (?, ?, ?, ?, ?, ?)"

        case write(cql, [peer_ip, remote_asn, description, bfd?, rr_client?, now_ms], opts) do
          :ok ->
            reload_host_bgp(opts)
            {:ok, peer_ip}

          {:error, reason} ->
            {:error, "Could not add BGP peer: #{describe(reason)}"}
        end
    end
  end

  @doc "Remove a BGP EVPN neighbor from `hydra.urbosa_bgp_peers` and trigger FRR reload."
  def delete_bgp_peer(peer_ip, opts \\ []) do
    ip = string(peer_ip)

    if is_nil(ip) or not valid_ipv4?(ip) do
      {:error, "Invalid BGP peer IPv4 address."}
    else
      case write("DELETE FROM hydra.urbosa_bgp_peers WHERE peer_ip = ?", [ip], opts) do
        :ok ->
          reload_host_bgp(opts)
          :ok

        {:error, reason} ->
          {:error, "Could not delete BGP peer: #{describe(reason)}"}
      end
    end
  end

  defp valid_ipv4?(ip) when is_binary(ip) do
    case :inet.parse_ipv4strict_address(String.to_charlist(ip)) do
      {:ok, _} -> true
      _ -> false
    end
  end

  defp valid_ipv4?(_), do: false

  defp reload_host_bgp(opts) do
    case static_source(opts) do
      %{} ->
        :ok

      nil ->
        Enum.each(nodes(nil), fn nip ->
          try do
            SpectrumPhx.SparkClient.request(nip, :post, "/api/v1/host/bgp", %{"action" => "reload"})
          rescue
            _ -> :ok
          catch
            :exit, _ -> :ok
          end
        end)
    end
  end
end
