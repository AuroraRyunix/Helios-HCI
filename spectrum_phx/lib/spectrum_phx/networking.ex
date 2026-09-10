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

  # The uniqueness constraint hydra.gatoway_networks cannot express. See `create_network/2`.
  @claim_cql "INSERT INTO hydra.gatoway_vlan_claims (vlan_id, net_id, name, claimed_at_ms) " <>
               "VALUES (?, ?, ?, ?) IF NOT EXISTS"
  @release_cql "DELETE FROM hydra.gatoway_vlan_claims WHERE vlan_id = ? IF net_id = ?"
  @reclaim_cql "UPDATE hydra.gatoway_vlan_claims SET net_id = ?, name = ?, claimed_at_ms = ? " <>
                 "WHERE vlan_id = ? IF net_id = ?"

  @vlan_range 1..4094

  # How old a claim must be before a create that lost to it may take it over.
  #
  # The takeover exists because a claim whose network was never written makes its VLAN
  # unusable forever. Without an age it would also break the thing it is protecting: a
  # create claims the VLAN and then writes the row, and for the few milliseconds in
  # between its network legitimately does not exist. A second create checking in that
  # window would take the claim from a create that is still running, and both would write
  # a network on VLAN 100 -- the duplicate, reintroduced by the repair.
  #
  # Five minutes is far longer than the two writes take and far shorter than an
  # operator's patience with a VLAN that cannot be used. This is what `claimed_at_ms` is
  # for; the same number is in `spectrum_server.py` for the same reason.
  @claim_grace_ms 300_000

  @doc "The CQL this module reads, exposed so tests can assert it stays bounded."
  def statements, do: %{networks: @networks_cql, segments: @segments_cql}

  @doc "The VLAN ids a tagged network may use."
  def vlan_range, do: @vlan_range

  @doc """
  Every network a guest can be attached to, plus what is carrying them.

  `source: {:static, map}` supplies rows instead of querying, keyed `:networks`,
  `:segments`, `:nodes`. `:claims` and `:insert` stand in for the two writes, and `:sink`
  is a pid told about every claim and release -- see `claim_vlan/4`.
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

  ## Two VLAN checks, and why both are here

  A tagged network's id is unique but its VLAN is not: `hydra.gatoway_networks` is keyed
  by `net_id`, so nothing in that table stops two networks claiming VLAN 100.

  The first check reads the existing networks and refuses a duplicate. It is advisory --
  a read followed by a write is two operations, and two creates a millisecond apart both
  read "VLAN 100 is free" -- and it is kept because it gives the better message and
  catches the mistake an operator actually makes.

  The second is the constraint. `hydra.gatoway_vlan_claims` is keyed by the VLAN id, which
  is what lets `IF NOT EXISTS` decide a race: a lightweight transaction is confined to one
  partition, so an exclusion between two creates has to live in a row they both condition
  on, and a VLAN id is the only thing they share. The loser is told so deterministically
  rather than by timing.

  ## Order, and the claim that must not be left behind

  The claim is taken before the network row is written, because the claim is what decides
  the race and anything written before it is written on the strength of a read that may
  already be stale. Gatoway polls the network table every five seconds and builds bridges
  from what it finds, so a row that exists for even a moment is a network that may be
  configured on every host.

  That order has a cost: a create that claims and then fails must give the claim back, or
  the VLAN is unusable forever -- a worse failure than the duplicate this prevents. It is
  given back on the failure path here, and a claim stranded by something more abrupt (the
  node losing power between the two writes) is recovered by `claim_vlan/4`, which takes
  over a claim whose network no longer exists.
  """
  def create_network(params, opts \\ []) do
    net_id = generate_net_id()

    with {:ok, name} <- validate_name(params),
         {:ok, kind} <- validate_kind(params),
         {:ok, vlan} <- validate_vlan(kind, params),
         :ok <- refuse_duplicate_vlan(vlan, opts),
         :ok <- claim_vlan(vlan, net_id, name, opts),
         :ok <- insert_network_or_release(net_id, name, kind, vlan, opts) do
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

  defp insert_network_or_release(net_id, name, kind, vlan, opts) do
    case insert_network(net_id, name, kind, vlan, opts) do
      :ok ->
        :ok

      {:error, message} ->
        # The claim goes back before the error goes out. A claim left behind by a create
        # that could not finish makes its VLAN unusable, and an operator reading "the
        # network could not be written" has no reason to suspect that the VLAN they typed
        # is now permanently spoken for.
        case release_vlan(vlan, net_id, opts) do
          :ok ->
            {:error, message}

          {:error, release_message} ->
            {:error,
             message <>
               " " <>
               release_message <>
               " It names a network that does not exist, so the next create of VLAN " <>
               "#{vlan} takes it over."}
        end
    end
  end

  defp insert_network(net_id, name, kind, vlan, opts) do
    case static_source(opts) do
      %{} = static ->
        Map.get(static, :insert, :ok)

      nil ->
        # `net_id` is interpolated and everything an operator typed stays a bound
        # parameter. It used to be `uuid()`, minted by the database, which was simpler
        # and is no longer possible: the claim's holder token is the net_id, so this side
        # has to know it before the row is written. `uuid?/1` admits nothing but hex
        # digits and dashes, and this one was generated here rather than received.
        statement =
          "INSERT INTO hydra.gatoway_networks (net_id, name, type, vlan_id) " <>
            "VALUES (" <> net_id <> ", ?, ?, ?)"

        case write(statement, [name, Atom.to_string(kind), vlan]) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, "The network could not be written: #{describe(reason)}"}
        end
    end
  end

  @doc """
  A canonical v4 uuid, from the same primitive `Accounts` uses for its tokens.

  Minted here rather than by the database's `uuid()` because the claim's holder token is
  the net_id, and this side has to know it before the row it will name exists.
  """
  def generate_net_id do
    <<head::48, _version::4, middle::12, _variant::2, tail::62>> = :crypto.strong_rand_bytes(16)

    <<part1::binary-8, part2::binary-4, part3::binary-4, part4::binary-4, part5::binary-12>> =
      Base.encode16(<<head::48, 4::4, middle::12, 2::2, tail::62>>, case: :lower)

    Enum.join([part1, part2, part3, part4, part5], "-")
  end

  @doc """
  Remove a Gatoway network.

  Segments are not removable here: they belong to the SDN page, where deleting one also
  has to account for the tunnels carrying it.

  The VLAN claim is released *after* the row is gone, never before. Releasing first would
  leave a window in which the network still exists and its VLAN is free, so a create
  racing this delete could take VLAN 100 while a network still carries it -- which is the
  duplicate the claim exists to prevent, reintroduced by the delete path.

  Returns `{:ok, id}`, or `{:ok, id, warning}` when the row is gone and its claim is not.
  That is a third shape rather than an error because the delete did happen and saying
  otherwise would be a lie about it -- but it is also not silence, because a claim nobody
  can release is a VLAN nobody can use.
  """
  def delete_network(id, opts \\ []) do
    with :ok <- ensure_network_id(id),
         {:ok, vlan} <- vlan_of(id, opts),
         :ok <- remove_network(id, opts) do
      case release_vlan(vlan, id, opts) do
        :ok -> {:ok, id}
        # Reported rather than swallowed, and the delete still stands: the row is already
        # gone, so refusing now would only be a lie about what happened.
        {:error, message} -> {:ok, id, message}
      end
    end
  end

  defp ensure_network_id(id), do: if(uuid?(id), do: :ok, else: {:error, "That is not a network id."})

  defp remove_network(id, opts) do
    case static_source(opts) do
      %{} ->
        :ok

      nil ->
        # Interpolated, and safe to be: `uuid?/1` admits nothing but hex digits and
        # dashes, so there is no string here that could close a literal. A bound
        # parameter would be preferable on principle, but a uuid argument's encoding
        # depends on the driver and this does not.
        case write("DELETE FROM hydra.gatoway_networks WHERE net_id = " <> id, []) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, "The network could not be removed: #{describe(reason)}"}
        end
    end
  end

  # The VLAN a network is holding, read before the row is deleted because afterwards
  # nothing says which claim belonged to it. A read that fails refuses the delete rather
  # than proceeding: a delete that cannot identify the claim strands it, and a VLAN nobody
  # can use again is worse than a delete an operator has to retry.
  defp vlan_of(id, opts) do
    case network_rows(id, opts) do
      {:ok, rows} ->
        {:ok,
         rows
         |> Enum.map(&stringify/1)
         |> Enum.find_value(fn row -> integer(get(row, "vlan_id")) end)}

      {:error, reason} ->
        {:error,
         "The network could not be read, so its VLAN claim cannot be given back: " <>
           describe(reason)}
    end
  end

  # -- the claim ------------------------------------------------------------------------

  @doc """
  Take the cluster-wide claim on a VLAN id, or say who holds it.

  `nil` is not a VLAN and claims nothing: a `direct` network has no tag to collide with,
  which is why `hydra.gatoway_networks` stores a null `vlan_id` for every one of them --
  the shape of the seeded Physical-Direct row on the live cluster.
  """
  def claim_vlan(vlan, net_id, name, opts \\ [])

  def claim_vlan(nil, _net_id, _name, _opts), do: :ok

  def claim_vlan(vlan, net_id, name, opts) do
    case take_claim(vlan, net_id, name, opts) do
      :ok -> :ok
      {:held, row} -> resolve_clash(vlan, net_id, name, stringify(row), opts)
      {:error, message} -> {:error, message}
    end
  end

  defp take_claim(vlan, net_id, name, opts) do
    case static_source(opts) do
      %{} = static ->
        report(static, {:vlan_claim, vlan, net_id})

        case static |> Map.get(:claims, []) |> Enum.map(&stringify/1)
             |> Enum.find(&(integer(get(&1, "vlan_id")) == vlan)) do
          nil -> :ok
          holder -> {:held, holder}
        end

      nil ->
        case lwt(@claim_cql, [vlan, net_id, name, System.system_time(:millisecond)]) do
          {:ok, true, _row} ->
            :ok

          {:ok, false, row} ->
            {:held, row}

          {:error, reason} ->
            {:error,
             "VLAN #{vlan} could not be claimed, so it is not known whether another " <>
               "network already has it: #{describe(reason)}"}
        end
    end
  end

  defp taken_message(vlan, holder) do
    "VLAN #{vlan} is already used by #{string(get(holder, "name")) || "an unnamed network"}. " <>
      "Another create claimed it first."
  end

  # A refused claim is either a genuine clash or a claim stranded by a create that died
  # between claiming and writing its row. The second is why this is not simply an error:
  # a stranded claim makes its VLAN unusable for good, which is worse than the duplicate
  # the claim prevents.
  defp resolve_clash(vlan, net_id, name, row, opts) do
    holder_id = string(get(row, "net_id")) || ""

    case network_exists?(holder_id, opts) do
      :unknown ->
        {:error,
         "VLAN #{vlan} is claimed by #{holder_id}, and the network table could not be " <>
           "read to confirm that network still exists. Refusing rather than taking the " <>
           "VLAN away from one that may be live."}

      true ->
        {:error, taken_message(vlan, row)}

      false ->
        take_over_stale_claim(vlan, net_id, name, holder_id, claim_age_ms(row), opts)
    end
  end

  # A claim with no timestamp at all cannot have been written by this code, and is treated
  # as old: the alternative is a VLAN nothing can recover.
  defp claim_age_ms(row) do
    System.system_time(:millisecond) - (integer(get(row, "claimed_at_ms")) || 0)
  end

  defp take_over_stale_claim(vlan, _net_id, _name, _holder_id, age, _opts)
       when age < @claim_grace_ms do
    {:error,
     "VLAN #{vlan} was just claimed by a network create that has not finished writing " <>
       "its row. If that create failed, the claim can be taken over in about " <>
       "#{max(1, div(@claim_grace_ms - age, 1000))} seconds."}
  end

  defp take_over_stale_claim(vlan, net_id, name, holder_id, _age, opts) do
    take_over_claim(vlan, net_id, name, holder_id, opts)
  end

  # Conditional on the stale net_id, so two callers finding the same stranded claim
  # produce one winner and a claim whose network was re-created meanwhile is left alone.
  defp take_over_claim(vlan, net_id, name, holder_id, opts) do
    case static_source(opts) do
      %{} = static ->
        report(static, {:vlan_reclaim, vlan, net_id, holder_id})
        :ok

      nil ->
        case lwt(@reclaim_cql, [net_id, name, System.system_time(:millisecond), vlan, holder_id]) do
          {:ok, true, _row} ->
            :ok

          _other ->
            {:error,
             "VLAN #{vlan} is held by a claim belonging to network #{holder_id}, which " <>
               "no longer exists, and the claim could not be taken over. Try again."}
        end
    end
  end

  # Answered from the same rows the page is showing under a static source, so the whole
  # decision -- clash, stranded claim, or a table that will not answer -- is exercised by
  # the same code in a test as on a cluster.
  defp network_exists?(id, opts) do
    cond do
      # A claim whose holder is not a network id -- empty, or anything else -- cannot name
      # a network at all, so no delete will ever release it. Treated as stranded, because
      # the alternative is a VLAN nothing can use again.
      not uuid?(id) ->
        false

      true ->
        case network_rows(id, opts) do
          {:ok, []} -> false
          {:ok, _rows} -> true
          {:error, _reason} -> :unknown
        end
    end
  end

  defp network_rows(id, opts) do
    case static_source(opts) do
      %{} = static ->
        case Map.get(static, :networks, []) do
          rows when is_list(rows) ->
            {:ok, Enum.filter(rows, &(uuid(get(stringify(&1), "net_id")) == id))}

          {:error, reason} ->
            {:error, reason}
        end

      nil ->
        read(nil, :networks, @networks_cql <> " WHERE net_id = " <> id)
    end
  end

  @doc """
  Give a VLAN claim back, conditional on it still being this network's.

  A claim that is not there is a release that has already happened, and is success: a
  `DELETE ... IF` against a missing row answers `[applied] = false` with every conditioned
  column null, which is not a lost race. Getting that backwards would make an ordinary
  second delete look like a conflict.
  """
  def release_vlan(vlan, net_id, opts \\ [])

  def release_vlan(nil, _net_id, _opts), do: :ok

  def release_vlan(vlan, net_id, opts) do
    case static_source(opts) do
      %{} = static ->
        report(static, {:vlan_release, vlan, net_id})
        :ok

      nil ->
        case lwt(@release_cql, [vlan, net_id]) do
          {:ok, true, _row} ->
            :ok

          {:ok, false, row} ->
            case string(get(stringify(row), "net_id")) do
              nil ->
                :ok

              holder ->
                {:error,
                 "The claim on VLAN #{vlan} now belongs to #{holder} and was left alone."}
            end

          {:error, reason} ->
            {:error, "The claim on VLAN #{vlan} could not be released: #{describe(reason)}"}
        end
    end
  end

  defp lwt(statement, params) do
    Hydra.apply_lwt_row(statement, params)
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  # Under `{:static, map}` no write runs, so a test cannot see the one property that is
  # entirely about order: that a create which could not finish gave its claim back. A
  # `:sink` pid is sent every claim and release as it happens, which is what makes that
  # assertable without a database.
  defp report(%{sink: pid}, message) when is_pid(pid), do: send(pid, message)
  defp report(_static, _message), do: :ok

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
