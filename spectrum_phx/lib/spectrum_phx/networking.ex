defmodule SpectrumPhx.Networking do
  @moduledoc """
  The layer-2 networks a guest can be put on, and the physical adapters underneath them.

  Two kinds of network share this page, and the Python console normalised them into one
  list, which is the right call: from a guest's point of view "which network am I on" has
  one answer whether the answer is a VLAN on a bridge or a VXLAN segment.

    * **Gatoway networks** (`hydra.gatoway_networks`) -- `direct` onto the host bridge, or
      a `vlan` tagged onto it. Created here.
    * **Urbosa segments** (`hydra.urbosa_segments`) -- overlay networks with a VNI, owned
      by the SDN page and read here so the list is complete.

  They are kept distinguishable by `kind`, because what you can *do* with them differs:
  a segment is deleted from the SDN side, and a VLAN cannot be edited into a VNI.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra

  @networks_cql "SELECT net_id, name, type, vlan_id FROM hydra.gatoway_networks"
  @segments_cql "SELECT segment_id, name, vni, subnet_cidr, gateway_ip FROM hydra.urbosa_segments"

  @vlan_range 1..4094

  @doc "The CQL this module reads, exposed so tests can assert it stays bounded."
  def statements, do: %{networks: @networks_cql, segments: @segments_cql}

  @doc "The VLAN ids a tagged network may use."
  def vlan_range, do: @vlan_range

  @doc """
  Every network a guest can be attached to, plus what is carrying them.

  `source: {:static, map}` supplies rows instead of querying, keyed `:networks`,
  `:segments`, `:nodes`.
  """
  def overview(opts \\ []) do
    static = static_source(opts)

    with {:ok, network_rows} <- read(static, :networks, @networks_cql),
         {:ok, segment_rows} <- read(static, :segments, @segments_cql) do
      networks =
        (Enum.map(network_rows, &gatoway_network/1) ++ Enum.map(segment_rows, &overlay_network/1))
        |> Enum.sort_by(&{kind_order(&1.kind), &1.name})

      %{
        available?: true,
        error: nil,
        networks: networks,
        summary: summarize(networks)
      }
    else
      {:error, reason} ->
        %{
          available?: false,
          error: describe(reason),
          networks: [],
          summary: summarize([])
        }
    end
  end

  defp kind_order(:direct), do: 0
  defp kind_order(:vlan), do: 1
  defp kind_order(:overlay), do: 2

  defp gatoway_network(row) do
    row = stringify(row)
    type = string(get(row, "type"))

    %{
      id: uuid(get(row, "net_id")),
      name: string(get(row, "name")) || "unnamed",
      kind: if(type == "vlan", do: :vlan, else: :direct),
      vlan_id: integer(get(row, "vlan_id")),
      vni: nil,
      subnet_cidr: nil,
      gateway_ip: nil,
      # Only these can be removed here; a segment belongs to the SDN page.
      removable?: true
    }
  end

  defp overlay_network(row) do
    row = stringify(row)

    %{
      id: uuid(get(row, "segment_id")),
      name: string(get(row, "name")) || "unnamed",
      kind: :overlay,
      vlan_id: nil,
      vni: integer(get(row, "vni")),
      subnet_cidr: string(get(row, "subnet_cidr")),
      gateway_ip: string(get(row, "gateway_ip")),
      removable?: false
    }
  end

  defp summarize(networks) do
    by_kind = Enum.frequencies_by(networks, & &1.kind)

    %{
      total: length(networks),
      direct: Map.get(by_kind, :direct, 0),
      vlan: Map.get(by_kind, :vlan, 0),
      overlay: Map.get(by_kind, :overlay, 0),
      vlans_used: networks |> Enum.map(& &1.vlan_id) |> Enum.filter(&is_integer/1) |> Enum.sort()
    }
  end

  # -- creating -------------------------------------------------------------------------

  @doc """
  Create a layer-2 network.

  Returns `{:ok, name}` or `{:error, message}` with a message meant for an operator.

  ## The VLAN check is advisory, and says so

  A tagged network's id is unique but its VLAN is not: the table is keyed by `net_id`, so
  nothing in the database stops two networks claiming VLAN 100. This reads the existing
  networks and refuses a duplicate, which is what the Python console does and is worth
  keeping -- it catches the mistake an operator actually makes. What it cannot do is
  serialise against a *concurrent* create, because a read followed by a write is not
  atomic and there is no key to make it one.

  Making it airtight needs a claim table keyed by vlan id, written with `IF NOT EXISTS`,
  which is a schema change and Gatoway's business as much as this page's. It is recorded
  in TODO.md rather than pretended away here.
  """
  def create_network(params, opts \\ []) do
    with {:ok, name} <- validate_name(params),
         {:ok, kind} <- validate_kind(params),
         {:ok, vlan} <- validate_vlan(kind, params),
         :ok <- refuse_duplicate_vlan(vlan, opts),
         :ok <- insert_network(name, kind, vlan, opts) do
      {:ok, name}
    end
  end

  defp validate_name(params) do
    case params |> Map.get("name", "") |> to_string() |> String.trim() do
      "" ->
        {:error, "A name is required."}

      name ->
        if Regex.match?(~r/^[A-Za-z0-9][A-Za-z0-9 ._-]{0,62}$/, name),
          do: {:ok, name},
          else: {:error, "A name may use letters, digits, spaces, dots, dashes and underscores."}
    end
  end

  defp validate_kind(params) do
    case params |> Map.get("type", "") |> to_string() |> String.trim() do
      "direct" -> {:ok, :direct}
      "vlan" -> {:ok, :vlan}
      other -> {:error, "Unknown network type #{inspect(other)}; expected direct or vlan."}
    end
  end

  defp validate_vlan(:direct, _params), do: {:ok, nil}

  defp validate_vlan(:vlan, params) do
    raw = params |> Map.get("vlan_id", "") |> to_string() |> String.trim()

    case Integer.parse(raw) do
      {value, ""} ->
        if value in @vlan_range,
          do: {:ok, value},
          else: {:error, "A VLAN id must be between 1 and 4094."}

      _ ->
        {:error, "A VLAN id must be a whole number between 1 and 4094."}
    end
  end

  defp refuse_duplicate_vlan(nil, _opts), do: :ok

  defp refuse_duplicate_vlan(vlan, opts) do
    case overview(opts) do
      %{available?: false, error: reason} ->
        {:error, "The existing networks could not be read, so a duplicate VLAN cannot be ruled out: #{reason}"}

      %{networks: networks} ->
        case Enum.find(networks, &(&1.vlan_id == vlan)) do
          nil -> :ok
          clash -> {:error, "VLAN #{vlan} is already used by #{clash.name}."}
        end
    end
  end

  defp insert_network(name, kind, vlan, opts) do
    case static_source(opts) do
      %{} ->
        :ok

      nil ->
        # `uuid()` rather than a generated parameter: the id is the database's to mint,
        # and it saves depending on how the driver chooses to encode a uuid argument.
        # Everything an operator typed stays a bound parameter.
        statement =
          "INSERT INTO hydra.gatoway_networks (net_id, name, type, vlan_id) " <>
            "VALUES (uuid(), ?, ?, ?)"

        case write(statement, [name, Atom.to_string(kind), vlan]) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, "The network could not be written: #{describe(reason)}"}
        end
    end
  end

  @doc """
  Remove a Gatoway network.

  Segments are not removable here: they belong to the SDN page, where deleting one also
  has to account for the tunnels carrying it.
  """
  def delete_network(id, opts \\ []) do
    cond do
      not uuid?(id) ->
        {:error, "That is not a network id."}

      static_source(opts) != nil ->
        {:ok, id}

      true ->
        # Interpolated, and safe to be: `uuid?/1` admits nothing but hex digits and
        # dashes, so there is no string here that could close a literal. A bound
        # parameter would be preferable on principle, but a uuid argument's encoding
        # depends on the driver and this does not.
        case write("DELETE FROM hydra.gatoway_networks WHERE net_id = " <> id, []) do
          {:ok, _} -> {:ok, id}
          {:error, reason} -> {:error, "The network could not be removed: #{describe(reason)}"}
        end
    end
  end

  @doc "Whether `value` is a canonical uuid, and therefore safe to write into a statement."
  def uuid?(value) when is_binary(value) do
    Regex.match?(
      ~r/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/,
      value
    )
  end

  def uuid?(_value), do: false

  defp write(statement, params) do
    Hydra.query(statement, params)
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  # -- plumbing --------------------------------------------------------------------------

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

  @doc "The configured hosts, for the adapters panel."
  def nodes(opts \\ []) do
    case static_source(opts) do
      %{nodes: nodes} when is_list(nodes) -> nodes
      _ -> Enum.map(Config.node_ips(), fn ip -> %{ip: ip, hostname: Config.hostname_for(ip)} end)
    end
  end

  defp stringify(row) when is_map(row) do
    Map.new(row, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp stringify(row), do: row

  defp get(row, key) when is_map(row), do: Map.get(row, key)
  defp get(_row, _key), do: nil

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
