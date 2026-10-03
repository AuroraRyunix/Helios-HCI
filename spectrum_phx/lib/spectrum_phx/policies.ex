defmodule SpectrumPhx.Policies do
  @moduledoc """
  Every policy object the cluster keeps, gathered onto one read-only view.

  The Policies panel this replaces sat on the settings page and held six unrelated fields --
  region, scrub interval, session timeout, rate limit, password policy and DRS -- under a
  name that promised something none of them was. None of those is a policy in the sense the
  rest of the system uses the word: a rule, attached to a set of things, that something
  acts on later. The cluster has exactly three kinds of those:

    * **Snapshot policies** (`hydra.dfs_snapshot_policies`): at cluster, container or vdisk
      scope, how often to snapshot and how many to keep. Narrowest scope wins, and a
      *disabled* narrow row is an exemption rather than being skipped.
    * **Protection domains** (`hydra.dfs_protection_domains`): a named group of VMs and
      vdisks snapshotted together under one policy, with a consistency level and a bound on
      how long a guest may be held still for it.
    * **Container policies** (`hydra.storage_containers`): tier, quota, fault tolerance and
      compression, which every vdisk in a container inherits.

  The security fields (password complexity, session timeout, rate limit) are policy in the
  ordinary sense and are shown last, read from the same settings the Settings page writes.

  This view only reads. Editing a snapshot policy or a domain is `valcli`'s job today
  (`storage.snapshot-policy.set`, `storage.domain*`); containers are edited on the Storage
  page. The page says so and links to it, rather than offering a form that would be the
  second writer of a table whose first writer has rules this page does not know.

  ## Test seam

  `Application.get_env(:spectrum_phx, :policies_source)` is `:hydra` or `{:static, map}` with
  `:snapshot_policies`, `:domains`, `:members` and `:sets` as lists of string-keyed rows
  (`:sets` the latest set of each domain) and `:containers` already normalised.
  """

  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Settings
  alias SpectrumPhx.Storage.Containers

  @snapshot_cql "SELECT scope, target, enabled, interval_seconds, keep_last " <>
                  "FROM hydra.dfs_snapshot_policies"
  @domains_cql "SELECT name, enabled, interval_seconds, keep_last, quiesce, max_pause_seconds " <>
                 "FROM hydra.dfs_protection_domains"
  @members_cql "SELECT domain, kind, name FROM hydra.dfs_protection_domain_members"
  @latest_set_cql "SELECT domain, taken_at_ms, state, consistency FROM hydra.dfs_protection_sets " <>
                    "WHERE domain = ? LIMIT 1"

  @scope_order %{"cluster" => 0, "container" => 1, "vdisk" => 2}

  @doc "Where reads come from: `:hydra` or `{:static, map}`."
  def source, do: Application.get_env(:spectrum_phx, :policies_source, :hydra)

  @doc "The CQL this module reads."
  def statements,
    do: %{
      snapshot: @snapshot_cql,
      domains: @domains_cql,
      members: @members_cql,
      latest_set: @latest_set_cql
    }

  @doc """
  Everything the page shows. Each section is `{:ok, rows}` or `{:error, message}`, so an
  unreadable table is reported as unreadable and never drawn as "no policies" -- which on
  this page would read as "nothing is protected".
  """
  def overview(opts \\ []) do
    static = static(opts)

    %{
      snapshot: snapshot_policies(static),
      domains: domains(static),
      containers: containers(static),
      security: security(static)
    }
  end

  # -- snapshot policies ----------------------------------------------------------------

  defp snapshot_policies(static) do
    with {:ok, rows} <- read(static, :snapshot_policies, @snapshot_cql) do
      {:ok,
       rows
       |> Enum.map(&snapshot_row/1)
       |> Enum.sort_by(&{Map.get(@scope_order, &1.scope, 9), &1.target})}
    end
  end

  defp snapshot_row(row) do
    row = stringify(row)

    %{
      scope: to_string(row["scope"]),
      target: to_string(row["target"]),
      enabled?: row["enabled"] == true,
      every_seconds: row["interval_seconds"],
      keep: row["keep_last"],
      # A disabled row at container or vdisk scope exempts what it names from the wider
      # policy. At cluster scope it simply means there is no cluster policy.
      exemption?: row["enabled"] != true and row["scope"] in ["container", "vdisk"]
    }
  end

  # -- protection domains ---------------------------------------------------------------

  defp domains(static) do
    with {:ok, rows} <- read(static, :domains, @domains_cql),
         {:ok, members} <- read(static, :members, @members_cql) do
      by_domain = members |> Enum.map(&stringify/1) |> Enum.group_by(& &1["domain"])

      {:ok,
       rows
       |> Enum.map(&stringify/1)
       |> Enum.map(&domain_row(&1, Map.get(by_domain, &1["name"], []), latest_set(static, &1["name"])))
       |> Enum.sort_by(& &1.name)}
    end
  end

  defp domain_row(row, members, latest) do
    %{
      name: to_string(row["name"]),
      enabled?: row["enabled"] == true,
      every_seconds: row["interval_seconds"],
      keep: row["keep_last"],
      quiesce: row["quiesce"] || "none",
      max_pause_seconds: row["max_pause_seconds"],
      vms: Enum.count(members, &(&1["kind"] == "vm")),
      vdisks: Enum.count(members, &(&1["kind"] == "vdisk")),
      latest: latest
    }
  end

  defp latest_set(static, domain) do
    result =
      case static do
        %{sets: sets} when is_list(sets) -> {:ok, Enum.filter(sets, &(stringify(&1)["domain"] == domain))}
        %{} -> {:ok, []}
        nil -> query(@latest_set_cql, [domain])
      end

    case result do
      {:ok, [row | _]} ->
        row = stringify(row)
        %{state: row["state"], consistency: row["consistency"], taken_at_ms: row["taken_at_ms"]}

      _ ->
        nil
    end
  end

  # -- container policies -----------------------------------------------------------------

  defp containers(%{containers: rows}) when is_list(rows), do: {:ok, rows}

  defp containers(%{}), do: {:ok, []}

  defp containers(nil) do
    case Containers.list() do
      {:ok, rows} -> {:ok, rows}
      {:error, reason} -> {:error, describe(reason)}
    end
  end

  # -- security ---------------------------------------------------------------------------

  defp security(static) do
    # Under a live source the settings are read from Hydra by `Settings.all/1` itself.
    opts =
      case static do
        %{} -> [source: {:static, %{settings: Map.get(static, :security, []), cluster: %{}}}]
        nil -> []
      end

    stored = Settings.all(opts).stored

    %{
      password_policy: stored["password_policy"],
      session_timeout: stored["session_timeout"],
      rate_limit: stored["rate_limit"]
    }
  end

  # -- plumbing ---------------------------------------------------------------------------

  defp static(opts) do
    case Keyword.get(opts, :source, source()) do
      {:static, map} -> map
      _hydra -> nil
    end
  end

  defp read(nil, _key, cql) do
    case query(cql, []) do
      {:ok, rows} -> {:ok, rows}
      {:error, reason} -> {:error, "could not be read: " <> describe(reason)}
    end
  end

  defp read(%{} = static, key, _cql) do
    case Map.get(static, key, []) do
      {:error, reason} -> {:error, "could not be read: " <> describe(reason)}
      rows when is_list(rows) -> {:ok, rows}
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

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)
end
