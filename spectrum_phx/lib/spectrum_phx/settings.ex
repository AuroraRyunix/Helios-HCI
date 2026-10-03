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

  ## `urbosa_enabled` is not a field on the settings form

  It is the one setting whose write is an operation. Turning it on bootstraps network
  namespaces, bridges and VXLAN interfaces on every node; turning it off tears them down,
  and doing that under a running Lanayru cluster takes that cluster's network away. So it
  is refused by `update/2` along with every other key that is not in the allow-list, and
  has its own path -- `set_urbosa_enabled/2` -- which writes the row *and* submits the
  Catalyst task that does the work, reports which of the two directions it took, and puts
  the row back if the task could not be queued.

  That last part is the whole reason it is not a checkbox. A row saying `true` with no
  bootstrap behind it is a cluster that believes it has an overlay and has not got one,
  and everything downstream -- Lanayru's pre-flight, the SDN page, a deploy -- reads the
  row.
  """

  alias SpectrumPhx.Catalyst
  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Accounts
  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Settings.Apply

  @settings_cql "SELECT key, value FROM hydra.cluster_settings"
  @users_cql "SELECT username FROM hydra.users"
  @rf_cql "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'hydra'"
  @urbosa_cql "SELECT value FROM hydra.cluster_settings WHERE key = 'urbosa_enabled'"
  @lanayru_cql "SELECT name, status FROM hydra.lanayru_clusters"

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

  # Read and shown, never written by `update/2`. The value is what it defaults to when the
  # table has no row for it -- which is not the same as the table being unreadable, and
  # both have to end up false: an overlay reported as enabled because a read failed is a
  # deploy that will be allowed and will not work.
  @read_only %{"urbosa_enabled" => "false"}

  @doc "The keys this console will write."
  def writable_keys, do: Map.keys(@stored)

  @doc "Keys that are shown but not editable through the settings form."
  def read_only_keys, do: Map.keys(@read_only)

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
        # Seeded with the read-only keys as well as the writable ones. The reduce only
        # accepts a key the accumulator already has -- that is what stops a stray row in
        # this free-form table becoming a setting -- so a key absent from the seed can
        # never be read at all, however often the cluster writes it. `urbosa_enabled` was
        # exactly that: written by both consoles, never seeded, and therefore reported as
        # "disabled" on every settings page ever rendered.
        rows
        |> Enum.map(&stringify/1)
        |> Enum.reduce(Map.merge(@stored, @read_only), fn row, acc ->
          key = string(Map.get(row, "key"))
          value = string(Map.get(row, "value"))

          if key && Map.has_key?(acc, key), do: Map.put(acc, key, value), else: acc
        end)

      {:error, _reason} ->
        :error
    end
  end

  defp read_only_settings(stored) do
    Map.new(@read_only, fn {key, default} -> {key, Map.get(stored, key, default)} end)
  end

  # `cluster.json` is the authority for these, not a row. A stored row that disagrees is
  # a stale row, and showing it would have an operator reading a name the cluster does
  # not answer to.
  defp cluster_facts(%{cluster: cluster}) when is_map(cluster), do: cluster

  defp cluster_facts(_static) do
    %{
      # `Config.all/0` is keyed by atoms. This used to ask for the string "cluster_name",
      # which is never there, so the settings page named no cluster at all.
      name: Config.all() |> Map.get(:cluster_name),
      vip: Config.vip(),
      subnet: Config.cluster_subnet(),
      id: Config.cluster_id(),
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

  # -- saving what an operator changed ------------------------------------------------------

  @doc """
  Save the settings form: validate it as a whole, write what is stored, and *apply* what
  has to be applied. See `SpectrumPhx.Settings.Apply` for why saving is more than a write.

  Returns `{:ok, %{saved: n, applied: [sentence], failed: [sentence]}}` or
  `{:error, message}`. `failed` is not an error: the row is written and one host did not
  take it, which is a state to report and not to roll back, because the other hosts did.
  Nothing is written if any field fails validation.
  """
  def save(params, opts \\ []) when is_map(params) do
    # A blank cluster field means "not shown", not "clear it": when cluster.json lacks a
    # subnet or the keyspace could not be read the input renders empty, and submitting the
    # page must not turn that into an instruction to blank the VIP on every host.
    blank_owned = for key <- Apply.cluster_fields() ++ [Apply.replication_field()], String.trim(to_string(params[key])) == "", do: key
    params = params |> Map.drop(~w(_csrf_token _target)) |> Map.drop(blank_owned)
    static = static_source(opts)

    case Apply.validate(params) do
      {:error, errors} ->
        {:error, Enum.join(errors, " ")}

      :ok ->
        before = all(opts)
        do_save(params, before, static, opts)
    end
  end

  defp do_save(params, before, static, opts) do
    owned = Apply.cluster_fields() ++ [Apply.replication_field()]
    {mine, stored_params} = Map.split(params, owned)
    # The form submits every field, so "changed" is measured against what is in force, not
    # against what was submitted: re-saving the page must not rewrite every host's DNS.
    changed = changed_keys(stored_params, before.stored)

    with :ok <- write_stored(stored_params, opts) do
      outcome =
        %{saved: length(changed), applied: [], failed: []}
        |> apply_stored(changed, stored_params, static)
        |> apply_cluster(mine, before, static)
        |> apply_replication(mine, before, static)

      refresh_config(static)

      {:ok,
       %{outcome | applied: Enum.reverse(outcome.applied), failed: Enum.reverse(outcome.failed)}}
    end
  end

  defp write_stored(params, _opts) when map_size(params) == 0, do: :ok

  defp write_stored(params, opts) do
    case update(params, opts) do
      {:ok, _count} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp changed_keys(params, current) do
    for {key, value} <- params,
        Map.has_key?(current, key),
        String.trim(to_string(value)) != String.trim(to_string(current[key])),
        do: key
  end

  defp apply_stored(outcome, changed, params, static) do
    outcome
    |> scrub(changed, params, static)
    |> push_dns(changed, params, static)
    |> push_ntp(changed, params, static)
    |> push_timezone(changed, params, static)
  end

  defp scrub(outcome, changed, params, static) do
    if "scrub_interval" in changed do
      value = String.trim(params["scrub_interval"])

      case Apply.schedule_scrub(static, value) do
        :ok -> note(outcome, "Disk scrub rescheduled (#{value}).")
        {:error, reason} -> fail(outcome, "The scrub schedule could not be changed: #{describe(reason)}")
      end
    else
      outcome
    end
  end

  defp push_dns(outcome, changed, params, static) do
    if Enum.any?(changed, &(&1 in ~w(dns_servers dns_search_domains))) do
      content = Apply.resolv_conf(params["dns_servers"], params["dns_search_domains"])
      command = Apply.write_file_command("/etc/resolv.conf", content)
      to_hosts(outcome, static, [{:host, command}], "DNS")
    else
      outcome
    end
  end

  defp push_ntp(outcome, changed, params, static) do
    if "ntp_servers" in changed do
      command = Apply.write_file_command("/etc/chrony.conf", Apply.chrony_conf(params["ntp_servers"]))
      to_hosts(outcome, static, [{:host, command}, {:units, "restart", ["chronyd"]}], "NTP")
    else
      outcome
    end
  end

  defp push_timezone(outcome, changed, params, static) do
    if "timezone" in changed do
      command = Apply.timezone_command(String.trim(params["timezone"]))
      to_hosts(outcome, static, [{:host, command}], "Timezone")
    else
      outcome
    end
  end

  defp apply_cluster(outcome, mine, before, static) do
    current = %{
      "cluster_name" => before.cluster[:name],
      "vip" => before.cluster[:vip],
      "cluster_subnet" => before.cluster[:subnet]
    }

    updates =
      for {key, value} <- mine,
          key in Apply.cluster_fields(),
          String.trim(value) != to_string(current[key]),
          into: %{},
          do: {key, String.trim(value)}

    if map_size(updates) == 0 do
      outcome
    else
      steps = [{:host, Apply.cluster_json_command(updates)}]

      steps =
        if Map.has_key?(updates, "vip"), do: steps ++ [{:units, "restart", ["bifrost"]}], else: steps

      what = updates |> Map.keys() |> Enum.sort() |> Enum.join(", ")
      to_hosts(%{outcome | saved: outcome.saved + map_size(updates)}, static, steps, "Cluster details (#{what})")
    end
  end

  defp apply_replication(outcome, mine, before, static) do
    with value when is_binary(value) <- mine[Apply.replication_field()],
         {factor, ""} <- Integer.parse(String.trim(value)),
         true <- factor != before.replication.factor do
      outcome = %{outcome | saved: outcome.saved + 1}

      case Apply.set_replication(static, factor, before.replication.factor) do
        {:ok, {:altered, applied}} ->
          note(outcome, "Keyspace replication factor set to #{applied}.")

        {:ok, {:altered_and_repairing, applied}} ->
          note(
            outcome,
            "Keyspace replication factor raised to #{applied}; a repair was started and runs in " <>
              "the background, and the new replicas are not populated until it finishes."
          )

        {:ok, {:altered_repair_failed, applied, reason}} ->
          fail(
            outcome,
            "Replication factor is now #{applied} but the repair did not start " <>
              "(#{describe(reason)}). The new replicas are empty until one is run: " <>
              "nodetool repair -pr hydra."
          )

        {:error, message} ->
          fail(%{outcome | saved: outcome.saved - 1}, message)
      end
    else
      _ -> outcome
    end
  end

  defp to_hosts(outcome, static, steps, what) do
    results = Apply.to_hosts(static, steps)
    failures = for {ip, {:error, reason}} <- results, do: "#{ip} (#{describe(reason)})"

    cond do
      results == [] -> fail(outcome, "#{what} was not applied: no hosts are configured.")
      failures == [] -> note(outcome, "#{what} applied on #{length(results)} host(s).")
      true -> fail(outcome, "#{what} did not apply on: #{Enum.join(failures, "; ")}.")
    end
  end

  defp note(outcome, sentence), do: %{outcome | applied: [sentence | outcome.applied]}
  defp fail(outcome, sentence), do: %{outcome | failed: [sentence | outcome.failed]}

  # The cluster facts are cached; after cluster.json changed on disk the page would
  # otherwise keep showing the old VIP until the container restarted.
  defp refresh_config(nil) do
    if Process.whereis(Config), do: Config.refresh()
    :ok
  end

  defp refresh_config(_static), do: :ok

  defp write_all(pairs, opts) do
    case static_source(opts) do
      %{} ->
        :ok

      nil ->
        Enum.reduce_while(pairs, :ok, fn {key, value}, _acc ->
          statement = "INSERT INTO hydra.cluster_settings (key, value) VALUES (?, ?)"

          case query(statement, [key, to_string(value)]) do
            {:ok, _} ->
              {:cont, :ok}

            {:error, reason} ->
              {:halt, {:error, "#{key} could not be saved: #{describe(reason)}"}}
          end
        end)
    end
  end

  # -- the overlay switch ------------------------------------------------------------------

  # Both already exist on every node and both are already what the Python console submits
  # for this switch: the same script, with and without `--cleanup`.
  @bootstrap_job "urbosa_bootstrap"
  @cleanup_job "urbosa_cleanup"
  @bootstrap_command "python3 /usr/local/bin/urbosa-bootstrap"
  @cleanup_command "python3 /usr/local/bin/urbosa-bootstrap --cleanup"

  # Building or removing namespaces, bridges and VXLAN interfaces on every host, one host
  # at a time through spark.
  @urbosa_timeout 1_800

  # A Lanayru cluster in either of these states is using the overlay right now.
  @lanayru_live ~w(active deploying running ready)

  @doc "The two job names the overlay switch submits."
  def urbosa_job_names, do: %{bootstrap: @bootstrap_job, cleanup: @cleanup_job}

  @doc "The CQL the overlay switch reads."
  def urbosa_statements, do: %{urbosa: @urbosa_cql, lanayru: @lanayru_cql}

  @doc """
  Whether the overlay is recorded as enabled.

  Anything that is not exactly `true` is false, including an unreadable table. Defaulting
  the other way would have a database outage read as "the overlay is up".
  """
  @spec urbosa_enabled?(keyword()) :: boolean()
  def urbosa_enabled?(opts \\ []) do
    case static_source(opts) do
      %{urbosa_enabled: value} ->
        to_string(value) == "true"

      %{} = static ->
        Map.get(read_only_settings(stored_or_empty(static)), "urbosa_enabled") == "true"

      nil ->
        case query(@urbosa_cql, []) do
          {:ok, [row | _]} -> string(Map.get(stringify(row), "value")) == "true"
          _other -> false
        end
    end
  end

  @doc """
  Turn overlay networking on or off.

  Returns `{:ok, %{direction: :bootstrap | :teardown, task_id: id}}`, `{:ok, :unchanged}`
  when the row already says what was asked for, or `{:error, message}`.

  The order is: refuse, write, submit, and put the row back if the submission failed. It
  cannot be "submit then write" -- the bootstrap reads the row -- and it must not be
  "write and hope", which is what the Python endpoint does: it writes the row, tries to
  submit, prints the failure to a log and answers `200` either way.
  """
  @spec set_urbosa_enabled(term(), keyword()) ::
          {:ok, :unchanged} | {:ok, map()} | {:error, String.t()}
  def set_urbosa_enabled(value, opts \\ []) do
    wanted = to_string(value) == "true"
    current = urbosa_enabled?(opts)

    # Read once. Asked twice -- in the condition and again in the message -- this is two
    # round trips to say one thing, and the second could disagree with the first.
    holder = if wanted, do: nil, else: lanayru_holder(opts)

    cond do
      wanted == current ->
        {:ok, :unchanged}

      holder != nil ->
        {:error,
         "'#{holder}' is running on the overlay. Tearing it down would take that Kubernetes " <>
           "cluster's network away; destroy the cluster first."}

      true ->
        apply_urbosa(wanted, current, opts)
    end
  end

  defp apply_urbosa(wanted, previous, opts) do
    with :ok <- write_urbosa(wanted, opts) do
      {job, command} =
        if wanted,
          do: {@bootstrap_job, @bootstrap_command},
          else: {@cleanup_job, @cleanup_command}

      case Catalyst.run_on_leader(job, command, timeout: @urbosa_timeout) do
        {:ok, %{"task_id" => id}} when is_binary(id) ->
          {:ok, %{direction: direction(wanted), task_id: id}}

        {:ok, _other} ->
          restore_urbosa(previous, opts)
          {:error, "Catalyst accepted the task but did not name it, so nothing was changed."}

        {:error, reason} ->
          # The row is put back before the error is reported. A row that says the overlay
          # is on, with no bootstrap behind it, is worse than the refusal an operator can
          # act on: Lanayru's pre-flight and the SDN page both believe it.
          restore_urbosa(previous, opts)

          {:error,
           "The overlay task could not be queued, so the setting was left as it was: " <>
             Catalyst.describe(reason)}
      end
    end
  end

  defp direction(true), do: :bootstrap
  defp direction(false), do: :teardown

  defp write_urbosa(value, opts) do
    case static_source(opts) do
      %{} ->
        :ok

      nil ->
        statement = "INSERT INTO hydra.cluster_settings (key, value) VALUES (?, ?)"

        case query(statement, ["urbosa_enabled", to_string(value)]) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, "urbosa_enabled could not be saved: #{describe(reason)}"}
        end
    end
  end

  defp restore_urbosa(previous, opts), do: write_urbosa(previous, opts)

  # The name of a Kubernetes cluster that is using the overlay, or nil. Read rather than
  # counted: the refusal names the cluster, because "something is running" is not a
  # sentence an operator can act on.
  defp lanayru_holder(opts) do
    rows =
      case static_source(opts) do
        %{lanayru: rows} when is_list(rows) ->
          rows

        %{} ->
          []

        nil ->
          case query(@lanayru_cql, []) do
            {:ok, rows} -> rows
            _other -> []
          end
      end

    rows
    |> Enum.map(&stringify/1)
    |> Enum.find_value(fn row ->
      status = row |> Map.get("status") |> to_string() |> String.downcase()
      if status in @lanayru_live, do: string(Map.get(row, "name")) || "A Kubernetes cluster"
    end)
  end

  defp stored_or_empty(static) do
    case stored_settings(static) do
      :error -> %{}
      stored -> stored
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

  @doc """
  Create an operator account.

  The password is checked against the cluster's password policy -- `enabled` asks for eight
  characters with an upper-case letter, a digit and a symbol, anything else for five -- the
  same rule the Python console applied, and the username against the same pattern. An
  account that already exists is refused: `INSERT` is an upsert, and creating `helios` a
  second time would silently reset its password.
  """
  def create_user(username, password, opts \\ []) do
    username = String.trim(to_string(username))
    static = static_source(opts)
    policy = stored_or_empty(static)["password_policy"] || "disabled"

    cond do
      not Regex.match?(~r/\A[A-Za-z0-9_]{3,20}\z/, username) ->
        {:error, "Username must be 3-20 letters, digits or underscores."}

      (message = password_problem(password, policy)) != nil ->
        {:error, message}

      username in users(static) ->
        {:error, "That user already exists."}

      static != nil ->
        {:ok, username}

      true ->
        statement =
          "INSERT INTO hydra.users (username, password_hash) VALUES (?, ?) IF NOT EXISTS"

        case Hydra.apply_lwt(statement, [username, Accounts.hash_password(password)]) do
          {:ok, true} -> {:ok, username}
          {:ok, false} -> {:error, "That user already exists."}
          {:error, reason} -> {:error, "The account could not be created: #{describe(reason)}"}
        end
    end
  end

  @doc "Set an operator's password, under the same password policy as `create_user/3`."
  def set_user_password(username, password, opts \\ []) do
    static = static_source(opts)
    policy = stored_or_empty(static)["password_policy"] || "disabled"

    cond do
      username not in users(static) ->
        {:error, "No such user."}

      (message = password_problem(password, policy)) != nil ->
        {:error, message}

      static != nil ->
        {:ok, username}

      true ->
        statement = "UPDATE hydra.users SET password_hash = ? WHERE username = ? IF EXISTS"

        case Hydra.apply_lwt(statement, [Accounts.hash_password(password), username]) do
          {:ok, true} -> {:ok, username}
          {:ok, false} -> {:error, "No such user."}
          {:error, reason} -> {:error, "The password could not be changed: #{describe(reason)}"}
        end
    end
  end

  @doc "Why a password is not acceptable under `policy`, or nil."
  def password_problem(password, "enabled") when is_binary(password) do
    if String.length(password) >= 8 and password =~ ~r/[A-Z]/ and password =~ ~r/[0-9]/ and
         password =~ ~r/[^A-Za-z0-9]/ do
      nil
    else
      "Password must be at least 8 characters long, with an upper-case letter, a number and a special character."
    end
  end

  def password_problem(password, _policy) when is_binary(password) do
    if String.length(password) >= 5, do: nil, else: "Password must be at least 5 characters long."
  end

  def password_problem(_password, _policy), do: "Password must be at least 5 characters long."

  # -- plumbing ---------------------------------------------------------------------------

  @doc """
  Where reads come from when the caller does not say: `:live`, or `{:static, map}` set in
  `Application.get_env(:spectrum_phx, :settings_source)`.

  The same seam `SpectrumPhx.Tasks` carries. Under a static source nothing is written
  anywhere -- an in-memory stand-in for a row is a test of the stand-in -- so what it
  exercises is the ordering: refuse, write, submit, and put the row back if the
  submission failed.
  """
  def source, do: Application.get_env(:spectrum_phx, :settings_source, :live)

  defp static_source(opts) do
    case Keyword.get(opts, :source, :live) do
      {:static, map} ->
        map

      :live ->
        case source() do
          {:static, map} -> map
          _live -> nil
        end
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
