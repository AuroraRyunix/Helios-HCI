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

  alias SpectrumPhx.Catalyst
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
  @dr_targets_cql "SELECT domain, remote_ip, remote_port, pin_sha256, last_set_id, last_sync_ms, status " <>
                    "FROM hydra.dr_replication_targets"

  @scope_order %{"cluster" => 0, "container" => 1, "vdisk" => 2}
  @valid_scopes ~w(cluster container vdisk)
  @valid_quiesce ~w(none vm domain)

  @doc "Where reads come from: `:hydra` or `{:static, map}`."
  def source, do: Application.get_env(:spectrum_phx, :policies_source, :hydra)

  @doc "The CQL this module reads."
  def statements,
    do: %{
      snapshot: @snapshot_cql,
      domains: @domains_cql,
      members: @members_cql,
      latest_set: @latest_set_cql,
      dr_targets: @dr_targets_cql
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
      dr_targets: dr_targets(static),
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

  @doc "Create or update a snapshot policy in `hydra.dfs_snapshot_policies`."
  def save_snapshot_policy(attrs, opts \\ []) when is_map(attrs) do
    attrs = stringify(attrs)
    scope = string(attrs["scope"])
    target = string(attrs["target"]) || if(scope == "cluster", do: "*", else: nil)
    interval_seconds = integer(attrs["interval_seconds"]) || integer(attrs["every_seconds"])
    keep_last = integer(attrs["keep_last"]) || integer(attrs["keep"])
    enabled = truthy?(Map.get(attrs, "enabled", true))

    cond do
      scope not in @valid_scopes ->
        {:error, "Scope must be cluster, container, or vdisk."}

      is_nil(target) ->
        {:error, "Target is required."}

      is_nil(interval_seconds) or interval_seconds <= 0 ->
        {:error, "Interval (seconds) must be a positive integer."}

      is_nil(keep_last) or keep_last <= 0 ->
        {:error, "Keep count must be a positive integer."}

      true ->
        now_ms = System.system_time(:millisecond)

        cql =
          "INSERT INTO hydra.dfs_snapshot_policies " <>
            "(scope, target, enabled, interval_seconds, keep_last, updated_at_ms) " <>
            "VALUES (?, ?, ?, ?, ?, ?)"

        case write(cql, [scope, target, enabled, interval_seconds, keep_last, now_ms], opts) do
          :ok -> {:ok, %{scope: scope, target: target}}
          {:error, reason} -> {:error, "Could not save snapshot policy: #{describe(reason)}"}
        end
    end
  end

  @doc "Delete a snapshot policy from `hydra.dfs_snapshot_policies`."
  def delete_snapshot_policy(scope, target, opts \\ []) do
    scope = string(scope)
    target = string(target)

    if scope in @valid_scopes and not is_nil(target) do
      cql = "DELETE FROM hydra.dfs_snapshot_policies WHERE scope = ? AND target = ?"

      case write(cql, [scope, target], opts) do
        :ok -> :ok
        {:error, reason} -> {:error, "Could not delete snapshot policy: #{describe(reason)}"}
      end
    else
      {:error, "Invalid snapshot policy scope or target."}
    end
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

  @doc "Create or update a protection domain and its members."
  def create_domain(attrs, opts \\ []) when is_map(attrs) do
    attrs = stringify(attrs)
    name = string(attrs["name"])
    interval_seconds = integer(attrs["interval_seconds"]) || integer(attrs["every_seconds"]) || 3600
    keep_last = integer(attrs["keep_last"]) || integer(attrs["keep"]) || 24
    quiesce = (string(attrs["quiesce"]) || "none") |> String.downcase()
    max_pause_seconds = integer(attrs["max_pause_seconds"]) || 10
    enabled = truthy?(Map.get(attrs, "enabled", true))
    vm_members = parse_member_list(attrs["vms"] || attrs["members"])
    vdisk_members = parse_member_list(attrs["vdisks"])

    cond do
      is_nil(name) or not valid_identifier?(name) ->
        {:error, "Domain name is required and may only contain letters, digits, dots, underscores, and hyphens."}

      quiesce not in @valid_quiesce ->
        {:error, "Quiesce mode must be none, vm, or domain."}

      interval_seconds <= 0 or keep_last <= 0 ->
        {:error, "Interval and keep count must be positive integers."}

      true ->
        now_ms = System.system_time(:millisecond)

        domain_cql =
          "INSERT INTO hydra.dfs_protection_domains " <>
            "(name, enabled, interval_seconds, keep_last, quiesce, max_pause_seconds, created_at_ms, updated_at_ms) " <>
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

        with :ok <-
               write(
                 domain_cql,
                 [name, enabled, interval_seconds, keep_last, quiesce, max_pause_seconds, now_ms, now_ms],
                 opts
               ),
             :ok <- insert_domain_members(name, "vm", vm_members, now_ms, opts),
             :ok <- insert_domain_members(name, "vdisk", vdisk_members, now_ms, opts) do
          {:ok, name}
        else
          {:error, reason} -> {:error, "Could not create protection domain: #{describe(reason)}"}
        end
    end
  end

  defp insert_domain_members(_domain, _kind, [], _now_ms, _opts), do: :ok

  defp insert_domain_members(domain, kind, [member | rest], now_ms, opts) do
    cql =
      "INSERT INTO hydra.dfs_protection_domain_members (domain, kind, name, added_at_ms) VALUES (?, ?, ?, ?)"

    case write(cql, [domain, kind, member, now_ms], opts) do
      :ok -> insert_domain_members(domain, kind, rest, now_ms, opts)
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_member_list(nil), do: []
  defp parse_member_list(list) when is_list(list), do: list |> Enum.map(&string/1) |> Enum.reject(&is_nil/1)

  defp parse_member_list(raw) when is_binary(raw) do
    raw
    |> String.split([",", " ", "\n"], trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp parse_member_list(_), do: []

  @doc "Delete a protection domain and its members."
  def delete_domain(name, opts \\ []) do
    name = string(name)

    if is_nil(name) do
      {:error, "Domain name is required."}
    else
      with :ok <- write("DELETE FROM hydra.dfs_protection_domain_members WHERE domain = ?", [name], opts),
           :ok <- write("DELETE FROM hydra.dfs_protection_domains WHERE name = ?", [name], opts) do
        :ok
      else
        {:error, reason} -> {:error, "Could not delete protection domain: #{describe(reason)}"}
      end
    end
  end

  @doc "Trigger an immediate snapshot set for a protection domain via Catalyst."
  def run_domain_now(name) do
    name = string(name)

    if is_nil(name) or not valid_identifier?(name) do
      {:error, "Invalid protection domain name."}
    else
      Catalyst.submit(
        "dagur",
        "execute",
        %{
          "job_name" => "Protection Domain Snapshot (#{name})",
          "command" => "/usr/local/bin/ncli storage.domain.run #{name}"
        },
        component: "Policies"
      )
    end
  end

  # -- DR replication targets -----------------------------------------------------------

  defp dr_targets(static) do
    with {:ok, rows} <- read(static, :dr_targets, @dr_targets_cql) do
      {:ok,
       rows
       |> Enum.map(&dr_target_row/1)
       |> Enum.sort_by(&{&1.domain, &1.remote_ip})}
    end
  end

  defp dr_target_row(row) do
    row = stringify(row)
    domain = to_string(row["domain"] || row["target_name"] || "")
    remote_ip = to_string(row["remote_ip"] || "")

    %{
      domain: domain,
      target_name: domain,
      remote_ip: remote_ip,
      remote_port: integer(row["remote_port"]) || 8443,
      pin_sha256: string(row["pin_sha256"]),
      last_set_id: string(row["last_set_id"]),
      last_sync_ms: integer(row["last_sync_ms"]),
      status: string(row["status"]) || "configured"
    }
  end

  @doc "Create or update an asynchronous DR replication target in `hydra.dr_replication_targets`."
  def create_dr_target(attrs, opts \\ []) when is_map(attrs) do
    attrs = stringify(attrs)
    domain = string(attrs["domain"]) || string(attrs["target_name"])
    remote_ip = string(attrs["remote_ip"]) || string(attrs["remote_cluster"])
    remote_port = integer(attrs["remote_port"]) || 8443
    pin_sha256 = string(attrs["pin_sha256"]) || ""

    cond do
      is_nil(domain) or not valid_identifier?(domain) ->
        {:error, "Protection domain / target name is required."}

      is_nil(remote_ip) ->
        {:error, "Remote IP / endpoint is required."}

      remote_port <= 0 or remote_port > 65_535 ->
        {:error, "Remote port must be between 1 and 65535."}

      true ->
        cql =
          "INSERT INTO hydra.dr_replication_targets " <>
            "(domain, remote_ip, remote_port, pin_sha256, status) " <>
            "VALUES (?, ?, ?, ?, ?)"

        case write(cql, [domain, remote_ip, remote_port, pin_sha256, "configured"], opts) do
          :ok -> {:ok, %{domain: domain, remote_ip: remote_ip}}
          {:error, reason} -> {:error, "Could not create DR target: #{describe(reason)}"}
        end
    end
  end

  @doc "Delete a DR replication target from `hydra.dr_replication_targets`."
  def delete_dr_target(target_or_domain, opts \\ [])

  def delete_dr_target(%{} = attrs, opts) do
    attrs = stringify(attrs)
    delete_dr_target(attrs["domain"] || attrs["target_name"], attrs["remote_ip"], opts)
  end

  def delete_dr_target(target_str, opts) when is_binary(target_str) do
    case String.split(target_str, "|", parts: 2) do
      [domain, remote_ip] ->
        delete_dr_target(domain, remote_ip, opts)

      [domain] ->
        domain = string(domain)

        if is_nil(domain) do
          {:error, "Invalid DR target."}
        else
          case write("DELETE FROM hydra.dr_replication_targets WHERE domain = ?", [domain], opts) do
            :ok -> :ok
            {:error, reason} -> {:error, "Could not delete DR target: #{describe(reason)}"}
          end
        end
    end
  end

  def delete_dr_target(domain, remote_ip, opts) do
    domain = string(domain)
    remote_ip = string(remote_ip)

    cond do
      is_nil(domain) ->
        {:error, "Domain is required."}

      is_nil(remote_ip) ->
        delete_dr_target(domain, opts)

      true ->
        cql = "DELETE FROM hydra.dr_replication_targets WHERE domain = ? AND remote_ip = ?"

        case write(cql, [domain, remote_ip], opts) do
          :ok -> :ok
          {:error, reason} -> {:error, "Could not delete DR target: #{describe(reason)}"}
        end
    end
  end

  @doc "Trigger an immediate asynchronous DR replication sync via Catalyst."
  def sync_dr_target(target_name) do
    target =
      target_name
      |> to_string()
      |> String.split("|", parts: 2)
      |> hd()
      |> string()

    if is_nil(target) or not valid_identifier?(target) do
      {:error, "Invalid DR target name."}
    else
      Catalyst.submit(
        "dagur",
        "execute",
        %{
          "job_name" => "DR Replication Sync (#{target})",
          "command" => "/usr/local/bin/ncli dr.sync #{target}"
        },
        component: "Policies"
      )
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

  defp write(cql, params, opts) do
    case static(opts) do
      %{} = s ->
        if is_pid(s[:sink]), do: send(s[:sink], {:write, cql, params})
        Map.get(s, :write_result, :ok)

      nil ->
        case query(cql, params) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, reason}
        end
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

  defp string(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp string(value) when is_atom(value) and not is_nil(value) and not is_boolean(value),
    do: Atom.to_string(value)

  defp string(_), do: nil

  defp integer(value) when is_integer(value), do: value

  defp integer(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {n, ""} -> n
      _ -> nil
    end
  end

  defp integer(_), do: nil

  defp truthy?(true), do: true
  defp truthy?("true"), do: true
  defp truthy?("1"), do: true
  defp truthy?("on"), do: true
  defp truthy?(1), do: true
  defp truthy?(_), do: false

  defp valid_identifier?(name) when is_binary(name) do
    Regex.match?(~r/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/, name)
  end

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)
end
