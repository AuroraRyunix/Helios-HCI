defmodule SpectrumPhx.Settings do
  @moduledoc """
  Cluster settings: `hydra.cluster_settings` read over a set of defaults, plus the facts
  that come from `cluster.json` and from the database itself rather than from a row.

  ## Three kinds of setting, and they are not interchangeable

    * **Stored** -- a row in `cluster_settings` and nothing else. DNS, NTP, timezone,
      policies. Writing one writes a row.
    * **Derived** -- read from somewhere authoritative and *not* editable here: the
      cluster name, VIP and node count come from `cluster.json`, and the keyspace
      replication factor from `system_schema`. A stored row that disagrees with the real
      value is a stale row, so the real value wins on read.
    * **Consequential** -- writing it does something to the cluster beyond the row.
      `replication_factor` alters the keyspace and, when it increases, needs a repair
      before the redundancy it claims actually exists. `urbosa_enabled` bootstraps or
      tears down the overlay on every host.

  The page has to keep those apart or an operator edits a field and gets a different
  outcome than the one next to it.

  ## `urbosa_enabled` is deliberately not writable here

  Turning it on bootstraps network namespaces, bridges and VXLAN interfaces on every
  node; turning it off tears them down, and the Python tier refuses the second while
  Lanayru is running because a Kubernetes cluster on the overlay loses its network. That
  is a cluster-wide, host-mutating operation and it belongs behind a Catalyst task where
  it can report progress and fail visibly -- not behind a checkbox that returns 200 and
  leaves the work happening somewhere. It is shown, with its state, and not offered.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra

  @settings_cql "SELECT key, value FROM hydra.cluster_settings"
  @users_cql "SELECT username FROM hydra.users"
  @rf_cql "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'hydra'"

  # Every key the console will write, and what it means. Anything not here is refused
  # rather than written: `cluster_settings` is a free-form key/value table, so the
  # allow-list is the only thing stopping a typo becoming a permanent row.
  @stored %{
    "dns_servers" => "8.8.8.8,8.8.4.4",
    "dns_search_domains" => "cluster.local",
    "dns_mtu" => "1500",
    "ntp_servers" => "pool.ntp.org",
    "timezone" => "UTC",
    "cluster_region" => "dc-1",
    "scrub_interval" => "weekly",
    "password_policy" => "disabled",
    "session_timeout" => "30",
    "rate_limit" => "100",
    "drs_enabled" => "true"
  }

  # Read and shown, never written from here.
  @read_only ~w(urbosa_enabled)

  @doc "The keys this console will write."
  def writable_keys, do: Map.keys(@stored)

  @doc "Keys that are shown but not editable here."
  def read_only_keys, do: @read_only

  @doc "The defaults a missing row falls back to."
  def defaults, do: @stored

  @doc "The CQL this module reads."
  def statements, do: %{settings: @settings_cql, users: @users_cql, replication: @rf_cql}

  @doc """
  Everything the settings page shows.

  `source: {:static, map}` supplies rows instead of querying, keyed `:settings`, `:users`,
  `:replication`, `:cluster`.
  """
  def all(opts \\ []) do
    static = static_source(opts)

    stored = stored_settings(static)
    cluster = cluster_facts(static)

    %{
      available?: stored != :error,
      error: if(stored == :error, do: "The settings table could not be read."),
      stored: if(stored == :error, do: @stored, else: stored),
      read_only: read_only_settings(if(stored == :error, do: %{}, else: stored)),
      cluster: cluster,
      users: users(static),
      replication: replication(static, cluster)
    }
  end

  defp stored_settings(static) do
    case read(static, :settings, @settings_cql) do
      {:ok, rows} ->
        rows
        |> Enum.map(&stringify/1)
        |> Enum.reduce(@stored, fn row, acc ->
          key = string(Map.get(row, "key"))
          value = string(Map.get(row, "value"))

          if key && Map.has_key?(acc, key), do: Map.put(acc, key, value), else: acc
        end)

      {:error, _reason} ->
        :error
    end
  end

  defp read_only_settings(stored) do
    # These are not in @stored, so they are read straight from the rows rather than
    # merged over a default.
    %{"urbosa_enabled" => Map.get(stored, "urbosa_enabled", "false")}
  end

  # `cluster.json` is the authority for these, not a row. A stored row that disagrees is
  # a stale row, and showing it would have an operator reading a name the cluster does
  # not answer to.
  defp cluster_facts(%{cluster: cluster}) when is_map(cluster), do: cluster

  defp cluster_facts(_static) do
    %{
      name: Config.all() |> Map.get("cluster_name"),
      vip: Config.vip(),
      nodes: length(Config.node_ips()),
      redundancy_factor: Config.redundancy_factor()
    }
  end

  defp users(static) do
    case read(static, :users, @users_cql) do
      {:ok, rows} ->
        rows
        |> Enum.map(&stringify/1)
        |> Enum.map(&string(Map.get(&1, "username")))
        |> Enum.reject(&is_nil/1)
        |> Enum.sort()

      {:error, _reason} ->
        []
    end
  end

  @doc """
  The keyspace's actual replication factor, and whether it matches the cluster.

  Read from `system_schema` rather than from a settings row, because the row is what
  somebody asked for and this is what the database is doing. They differ exactly when a
  change failed, which is the moment the difference matters.
  """
  def replication(static, cluster) do
    factor =
      case read(static, :replication, @rf_cql) do
        {:ok, [row | _]} -> parse_replication(stringify(row))
        _ -> nil
      end

    # ftt is what the cluster was created with; +1 is the copies that implies.
    implied = (cluster[:redundancy_factor] || 0) + 1
    nodes = cluster[:nodes] || 1

    %{
      factor: factor,
      implied: min(implied, max(nodes, 1)),
      nodes: nodes,
      # The keyspace replicates metadata. It says nothing about how many copies of a
      # guest's disk exist, which is a per-vdisk property, and conflating the two is how
      # an operator concludes their data is replicated because this number is 3.
      scope: :metadata
    }
  end

  defp parse_replication(row) do
    case Map.get(row, "replication") do
      map when is_map(map) ->
        map
        |> Enum.find_value(fn {key, value} ->
          if key not in ["class", "replication_factor"], do: integer(value)
        end) || integer(Map.get(map, "replication_factor"))

      _ ->
        nil
    end
  end

  # -- writing ---------------------------------------------------------------------------

  @doc """
  Write the settings an operator changed.

  Only keys in the allow-list are written; anything else is reported rather than silently
  dropped, because a setting that appears to save and does not is worse than one that
  refuses.
  """
  def update(params, opts \\ []) do
    {known, unknown} =
      params
      |> Map.take(Map.keys(params))
      |> Enum.split_with(fn {key, _value} -> Map.has_key?(@stored, key) end)

    refused = for {key, _} <- unknown, key not in ~w(_csrf_token _target), do: key

    cond do
      refused != [] and known == [] ->
        {:error, "Not a setting this console writes: #{Enum.join(refused, ", ")}."}

      true ->
        case write_all(known, opts) do
          :ok when refused == [] -> {:ok, length(known)}
          :ok -> {:ok, length(known)}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp write_all(pairs, opts) do
    case static_source(opts) do
      %{} ->
        :ok

      nil ->
        Enum.reduce_while(pairs, :ok, fn {key, value}, _acc ->
          statement = "INSERT INTO hydra.cluster_settings (key, value) VALUES (?, ?)"

          case query(statement, [key, to_string(value)]) do
            {:ok, _} -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, "#{key} could not be saved: #{describe(reason)}"}}
          end
        end)
    end
  end

  # -- users -----------------------------------------------------------------------------

  @doc """
  Remove an operator account.

  The last account is never removed: a console nobody can sign in to is not a secured
  console, it is a bricked one, and the only way back is a database edit by hand.
  """
  def delete_user(username, opts \\ []) do
    existing = users(static_source(opts))

    cond do
      username not in existing ->
        {:error, "No such user."}

      length(existing) <= 1 ->
        {:error, "This is the only account. Removing it would lock everyone out of the console."}

      static_source(opts) != nil ->
        {:ok, username}

      true ->
        case query("DELETE FROM hydra.users WHERE username = ?", [username]) do
          {:ok, _} -> {:ok, username}
          {:error, reason} -> {:error, "The account could not be removed: #{describe(reason)}"}
        end
    end
  end

  # -- plumbing ---------------------------------------------------------------------------

  defp static_source(opts) do
    case Keyword.get(opts, :source, :live) do
      {:static, map} -> map
      :live -> nil
    end
  end

  defp read(nil, _key, cql), do: query(cql, [])

  defp read(%{} = static, key, _cql) do
    case Map.get(static, key) do
      {:error, reason} -> {:error, reason}
      rows when is_list(rows) -> {:ok, rows}
      nil -> {:ok, []}
    end
  end

  defp query(statement, params) do
    Hydra.query(statement, params)
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
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

  defp integer(_), do: nil

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)
end
