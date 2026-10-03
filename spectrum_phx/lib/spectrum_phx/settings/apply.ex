defmodule SpectrumPhx.Settings.Apply do
  @moduledoc """
  What saving a setting *does*, as opposed to what it stores.

  The Python console's `/api/settings/update` was not a row write. Saving the settings form
  also rewrote `/etc/resolv.conf` and `/etc/chrony.conf` on every host, set the timezone,
  rewrote `cluster.json` on every host for the cluster name, VIP and subnet (restarting
  `bifrost` when the VIP moved), altered the keyspace's replication factor and started a
  repair when it rose, and re-scheduled the storage scrub. The port to Phoenix kept the rows
  and dropped all of that, so a DNS server could be "saved" without a single host ever
  learning it -- and the cluster's own identity, the VIP included, became display-only.

  This module is the part that was dropped. Everything here is a pure builder or an effect
  that goes through one seam, so the commands a save would run are testable without a host.

  ## The seam

  Under `source: {:static, map}` nothing runs. If the map carries `:effects`, a function of
  one argument, it is told what would have happened:

      {:cql, statement, params}
      {:host, ip, shell_command}
      {:units, ip, action, [unit]}
      {:repair, ip}

  and its return value (`:ok` or `{:error, reason}`) stands in for the result. `:hosts` and
  `:datacenter` stand in for `cluster.json`'s host list and `system.local`.

  ## Validation happens first, as a whole

  Every field is checked before any is written, and every failure is reported at once. The
  Python endpoint validated nothing: a VIP of `banana` was written to `cluster.json` on every
  host and `bifrost` was restarted onto it.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Spark

  @cluster_fields ~w(cluster_name vip cluster_subnet)
  @replication_field "replication_factor"

  # The cron expression, the interval and whether the job is enabled, per setting.
  @scrub %{
    "daily" => {"0 2 * * *", 86_400, true},
    "weekly" => {"0 2 * * 0", 604_800, true},
    "monthly" => {"0 2 1 * *", 2_592_000, true},
    "disabled" => {"0 */6 * * *", 21_600, false}
  }

  @scrub_cql "UPDATE hydra.dagur_schedules SET cron_expression = ?, interval_seconds = ?, " <>
               "enabled = ? WHERE job_name = 'storage_scrub'"

  @datacenter_cql "SELECT data_center FROM system.local"

  @name_regex ~r/\A[A-Za-z0-9][A-Za-z0-9-]{0,62}\z/
  @host_regex ~r/\A[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?\z/
  @timezone_regex ~r/\A[A-Za-z0-9_+\/-]{1,64}\z/
  @datacenter_regex ~r/\A[A-Za-z0-9_-]{1,64}\z/

  @max_replication 5

  @doc "The settings that live in `cluster.json` on every host, not in a row."
  def cluster_fields, do: @cluster_fields

  @doc "The one setting that is a property of the keyspace."
  def replication_field, do: @replication_field

  @doc "The values `scrub_interval` accepts."
  def scrub_intervals, do: Map.keys(@scrub)

  @doc "The CQL a scrub re-schedule runs."
  def scrub_cql, do: @scrub_cql

  @doc "The cron, interval and enabled flag a `scrub_interval` value means."
  def scrub_schedule(interval), do: Map.fetch(@scrub, interval)

  # -- validation ------------------------------------------------------------------------

  @doc """
  Check every field the form can submit. Returns `:ok` or `{:error, [message]}` listing
  every problem, not the first.
  """
  def validate(params) when is_map(params) do
    errors =
      for {key, value} <- params,
          is_binary(key),
          message = check(key, to_string(value)),
          do: "#{label(key)}: #{message}"

    if errors == [], do: :ok, else: {:error, Enum.sort(errors)}
  end

  defp check("vip", value) do
    case :inet.parse_ipv4strict_address(String.to_charlist(String.trim(value))) do
      {:ok, _} -> nil
      _ -> "must be an IPv4 address such as 10.10.102.45"
    end
  end

  defp check("cluster_subnet", value) do
    with [address, prefix] <- String.split(String.trim(value), "/"),
         {:ok, _} <- :inet.parse_ipv4strict_address(String.to_charlist(address)),
         {bits, ""} <- Integer.parse(prefix),
         true <- bits in 0..32 do
      nil
    else
      _ -> "must be a network in CIDR form such as 10.10.102.0/24"
    end
  end

  defp check("cluster_name", value) do
    if Regex.match?(@name_regex, value),
      do: nil,
      else: "1-63 characters: letters, digits and '-', starting with a letter or digit"
  end

  defp check(@replication_field, value), do: ranged(value, 1, @max_replication)
  defp check("dns_mtu", value), do: ranged(value, 576, 9216)
  defp check("session_timeout", value), do: ranged(value, 5, 1440)
  defp check("rate_limit", value), do: ranged(value, 5, 1000)

  defp check("timezone", value) do
    if Regex.match?(@timezone_regex, value), do: nil, else: "not a timezone name"
  end

  defp check("scrub_interval", value) do
    if Map.has_key?(@scrub, value), do: nil, else: "one of #{Enum.join(Map.keys(@scrub), ", ")}"
  end

  defp check("password_policy", value) do
    if value in ~w(disabled enabled), do: nil, else: "one of disabled, enabled"
  end

  defp check("drs_enabled", value) do
    if value in ~w(true false), do: nil, else: "one of true, false"
  end

  defp check("dns_servers", value) do
    servers = list(value)

    cond do
      servers == [] -> "at least one resolver is required"
      Enum.all?(servers, &match?({:ok, _}, :inet.parse_address(String.to_charlist(&1)))) -> nil
      true -> "every entry must be an IP address"
    end
  end

  defp check("ntp_servers", value) do
    servers = list(value)

    cond do
      servers == [] -> "at least one server is required"
      Enum.all?(servers, &Regex.match?(@host_regex, &1)) -> nil
      true -> "every entry must be a host name or address"
    end
  end

  defp check("dns_search_domains", value) do
    if value == "" or Regex.match?(@host_regex, String.trim(value)),
      do: nil,
      else: "must be a domain name"
  end

  defp check(_key, _value), do: nil

  defp ranged(value, low, high) do
    case Integer.parse(String.trim(value)) do
      {n, ""} when n >= low and n <= high -> nil
      _ -> "a whole number from #{low} to #{high}"
    end
  end

  defp label("vip"), do: "VIP"
  defp label("cluster_name"), do: "Cluster name"
  defp label("cluster_subnet"), do: "Subnet"
  defp label(@replication_field), do: "Replication factor"
  defp label("dns_mtu"), do: "MTU"
  defp label(key), do: key |> String.replace("_", " ") |> String.capitalize()

  defp list(value) do
    value |> String.split(",") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == ""))
  end

  # -- command builders --------------------------------------------------------------------

  @doc "The `resolv.conf` a set of resolvers and search domains means."
  def resolv_conf(servers, search) do
    domains = String.trim(search || "")
    header = if domains == "", do: "", else: "search #{domains}\n"
    header <> Enum.map_join(list(servers), "", &"nameserver #{&1}\n")
  end

  @doc "The `chrony.conf` a set of time servers means."
  def chrony_conf(servers), do: Enum.map_join(list(servers), "", &"server #{&1} iburst\n")

  @doc """
  A shell command that replaces a file's contents.

  The content is base64, so nothing an operator typed is ever parsed by the shell; `path`
  is always one of this module's own constants.
  """
  def write_file_command(path, content) do
    "echo #{Base.encode64(content)} | base64 -d > #{path}"
  end

  @doc """
  A shell command that merges `updates` into `/etc/hci/cluster.json`.

  Only the keys in `updates` are touched, which is what keeps a save of the name from
  clearing the VIP: the Python endpoint built the same map for the same reason.
  """
  def cluster_json_command(updates) when is_map(updates) do
    script =
      "import json,os,sys; p='/etc/hci/cluster.json'; " <>
        "d=json.load(open(p)) if os.path.exists(p) else {}; " <>
        "d.update(json.load(sys.stdin)); json.dump(d, open(p,'w'), indent=4)"

    "echo #{Base.encode64(Jason.encode!(updates))} | base64 -d | python3 -c #{Spark.escape(script)}"
  end

  @doc "The timezone command. The value was validated, and is quoted regardless."
  def timezone_command(zone), do: "timedatectl set-timezone #{Spark.escape(zone)}"

  @doc "The keyspace's replication clause for one datacenter."
  def replication_clause(datacenter, factor) do
    "{'class': 'NetworkTopologyStrategy', '#{datacenter}': #{factor}}"
  end

  # -- effects ----------------------------------------------------------------------------

  @doc """
  Push a changed group of settings to every host. Returns `[{ip, :ok | {:error, reason}}]`
  across *all* hosts, so one unreachable node does not hide that the others took it.
  """
  def to_hosts(static, steps) when is_list(steps) do
    for ip <- hosts(static) do
      {ip, run_steps(static, ip, steps)}
    end
  end

  defp run_steps(_static, _ip, []), do: :ok

  defp run_steps(static, ip, [step | rest]) do
    result =
      case step do
        {:host, command} -> effect(static, {:host, ip, command}, fn -> host(ip, command) end)
        {:units, action, units} -> effect(static, {:units, ip, action, units}, fn -> units(ip, action, units) end)
      end

    case result do
      :ok -> run_steps(static, ip, rest)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Re-schedule the storage scrub."
  def schedule_scrub(static, interval) do
    {cron, seconds, enabled} = Map.fetch!(@scrub, interval)
    params = [cron, seconds, enabled]
    effect(static, {:cql, @scrub_cql, params}, fn -> cql(@scrub_cql, params) end)
  end

  @doc """
  Alter the keyspace's replication factor, and start a repair when it rose.

  ALTER KEYSPACE only changes the strategy. Existing data reaches the new replicas when a
  repair runs, so raising the factor without one is a cluster that reports redundancy it
  does not have. Returns `{:ok, :altered | :altered_and_repairing}`, or `{:error, message}`.
  The factor is capped at the node count: a factor above it can never be satisfied.
  """
  def set_replication(static, factor, before) do
    nodes = max(length(hosts(static)), 1)
    wanted = min(factor, nodes)

    with {:ok, datacenter} <- datacenter(static),
         statement = "ALTER KEYSPACE hydra WITH replication = #{replication_clause(datacenter, wanted)}",
         :ok <- effect(static, {:cql, statement, []}, fn -> cql(statement, []) end) do
      if is_integer(before) and wanted > before do
        case effect(static, {:repair, repair_ip(static)}, fn -> repair(repair_ip(static)) end) do
          :ok -> {:ok, {:altered_and_repairing, wanted}}
          {:error, reason} -> {:ok, {:altered_repair_failed, wanted, reason}}
        end
      else
        {:ok, {:altered, wanted}}
      end
    else
      {:error, reason} -> {:error, "The replication factor could not be changed: #{describe(reason)}"}
    end
  end

  defp datacenter(%{} = static), do: {:ok, Map.get(static, :datacenter, "datacenter1")}

  defp datacenter(nil) do
    # Read rather than assumed: a keyspace naming a datacenter the snitch does not report is
    # accepted and places no replicas at all, which reads as a healthy ALTER.
    case query(@datacenter_cql, []) do
      {:ok, [row | _]} ->
        name = Map.get(row, "data_center") || Map.get(row, :data_center)
        if is_binary(name) and Regex.match?(@datacenter_regex, name), do: {:ok, name}, else: {:ok, "datacenter1"}

      _ ->
        {:ok, "datacenter1"}
    end
  end

  defp repair_ip(%{} = static), do: static |> hosts() |> List.first()
  defp repair_ip(nil), do: Config.local_ip()

  defp hosts(%{} = static), do: Map.get(static, :hosts, [])
  defp hosts(nil), do: Config.node_ips()

  defp effect(%{effects: fun}, tag, _live) when is_function(fun, 1), do: fun.(tag)
  defp effect(%{}, _tag, _live), do: :ok
  defp effect(nil, _tag, live), do: live.()

  defp host(ip, command) do
    case Spark.execute(ip, command) do
      {0, _out, _err} -> :ok
      {rc, _out, err} -> {:error, "exit #{rc}: #{String.slice(to_string(err), 0, 200)}"}
    end
  end

  defp units(ip, action, units) do
    case Spark.host_units(ip, action, units) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, describe(reason)}
    end
  end

  defp repair(nil), do: {:error, "no host to start it on"}

  defp repair(ip) do
    case Spark.db_repair(ip) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, describe(reason)}
    end
  end

  defp cql(statement, params) do
    case query(statement, params) do
      {:ok, _rows} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp query(statement, params) do
    Hydra.query(statement, params)
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp describe(reason) when is_binary(reason), do: reason
  defp describe({status, message}) when is_integer(status) and is_binary(message), do: message
  defp describe(reason), do: inspect(reason)
end
