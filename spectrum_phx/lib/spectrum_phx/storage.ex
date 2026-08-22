defmodule SpectrumPhx.Storage do
  @moduledoc """
  Read-only assembly of the storage fabric: LINSTOR pools, DRBD resources and the
  per-node block-device inventory.

  ## Why this is not the old `/api/storage/*`

  The Python tier derived storage health from the *human* table `linstor storage-pool
  list` prints, splitting on `|` and dropping empty cells. A diskless pool has empty
  `PoolName`, `FreeCapacity` and `TotalCapacity` cells, so dropping empties shifts every
  later column left and the row parses as a different pool entirely -- which is why the
  parser had to skip any line containing "diskless" to stay upright. It then labelled a
  pool `ONLINE` whenever the state cell contained "ok", and reported nothing at all about
  DRBD. A resource that was `Inconsistent`, `StandAlone` or missing a replica rendered
  exactly like a healthy one.

  Here the machine-readable form is used instead (`linstor --machine-readable ...`,
  which is what `mipha.py` already uses for its auto-heal pass), DRBD state comes from
  `drbdsetup status --json` through Spark's typed endpoint, and health is derived rather
  than assumed:

    * a device is healthy only when its disk state is `UpToDate` and it holds quorum;
    * a connection is healthy only when it is `Connected`, its peer disk is `UpToDate`
      and replication is `Established`;
    * a resource is healthy only when every placement is, *and* the number of replicas
      actually seen meets the cluster's redundancy factor.

  ## Unknown is not healthy

  Every per-node read can fail, and a node that does not answer tells us nothing about
  the resources it backs. Such a node is recorded in `:unreachable` and any resource
  that would otherwise look clean is reported `:unknown`, never `:ok` -- and
  `under_replicated?` becomes `nil` rather than `false`, because a replica we could not
  count is not a replica we know is absent. A dev machine with no cluster at all reports
  `configured?: false` and unavailable sections, which is a different statement from
  "everything is fine".

  ## Test seam

  `snapshot/1` reads through `source/0`, which defaults to `:live` and can be set to
  `{:static, map}` in application env. The static payloads are the *raw* daemon shapes
  (LINSTOR entries, `drbdsetup status --json` documents, `lsblk -J` documents), so the
  parsing and health derivation this module exists for are what the tests exercise --
  not a pre-chewed result.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Spark

  @pubsub SpectrumPhx.PubSub
  @topic "storage:status"

  # Caps the fan-out from this side. `Spark.drbd_status/2` and `Spark.host_disks/1` take
  # no timeout option, so bounding the page render is this module's job.
  @fanout_timeout_ms 8_000
  @pool_attempt_timeout_ms 6_000
  @pool_candidates 3

  @uptodate "UpToDate"
  @connected "Connected"
  @established "Established"
  @primary "Primary"

  # LINSTOR reports INT64_MAX as the capacity of a diskless pool. Anything above 2^62
  # KiB (four exbibytes) is that sentinel rather than a disk, and averaging it into the
  # cluster total is how a fabric with no free space reports itself as nearly empty.
  @implausible_capacity_kib 4_611_686_018_427_387_904

  # Image resources are created with `--allow-two-primaries` on purpose: the golden image
  # is attached read-only to guests on several hosts at once. VM disks are not, and two
  # Primaries on one of those is the corruption case, so the two are judged differently.
  @dual_primary_prefix "img-"

  @doc "PubSub topic for pushed storage snapshots."
  def topic, do: @topic

  @doc "Subscribe the calling process to pushed storage snapshots."
  def subscribe, do: Phoenix.PubSub.subscribe(@pubsub, @topic)

  @doc """
  Broadcast a snapshot as `{:storage_status, snapshot}`.

  Nothing in this module calls it. It is the hook for whichever process ends up watching
  the fabric, so `StorageLive` can stop refreshing on a timer.
  """
  def broadcast(snapshot) do
    Phoenix.PubSub.broadcast(@pubsub, @topic, {:storage_status, snapshot})
  rescue
    ArgumentError -> :ok
  catch
    :exit, _reason -> :ok
  end

  @doc """
  Everything the storage page renders, in one pass.

      %{
        configured?: boolean,
        nodes: [%{ip: binary, hostname: binary}],
        pools: %{state: :ok | :unavailable, entries: [pool], error: binary | nil},
        resources: %{
          state: :ok | :partial | :unavailable,
          entries: [resource],
          unreachable: [%{ip: binary, hostname: binary, error: binary}]
        },
        disks: [%{ip:, hostname:, state: :ok | :unavailable, error:, devices: [device]}],
        capacity: %{...},
        summary: %{...}
      }
  """
  def snapshot(opts \\ []) do
    static = static_payload(opts)

    node_ips = node_ips(static)
    nodes = Enum.map(node_ips, fn ip -> %{ip: ip, hostname: hostname_for(ip)} end)
    expected = expected_replicas(static, length(node_ips))

    pools = pools_section(static, node_ips)
    disks = disks_section(static, nodes)
    resources = resources_section(static, nodes, expected)
    capacity = capacity(pools, expected)

    %{
      configured?: node_ips != [],
      nodes: nodes,
      expected_replicas: expected,
      pools: pools,
      resources: resources,
      disks: disks,
      capacity: capacity,
      summary: summarize(pools, resources, disks)
    }
  end

  @doc """
  Where storage reads come from: `:live` (the default) or `{:static, map}`.

  The static map may carry `:pools`, `:drbd`, `:disks`, `:node_ips` and
  `:redundancy_factor`; see the moduledoc. Anything it omits is treated as unavailable
  rather than as empty-and-fine.
  """
  @spec source() :: :live | {:static, map()}
  def source, do: Application.get_env(:spectrum_phx, :storage_source, :live)

  @doc """
  The LINSTOR command used to list storage pools, for a given set of controller IPs.

  Exposed so a test can pin its shape without a cluster. The IPs are re-rendered from
  `:inet.parse_strict_address/1` rather than passed through, so nothing that is not
  literally an address can reach the shell Spark runs this with.
  """
  def pools_command(controller_ips) do
    controllers =
      controller_ips
      |> Enum.map(&normalize_ip/1)
      |> Enum.reject(&is_nil/1)
      |> case do
        [] -> ["127.0.0.1"]
        ips -> ips
      end
      |> Enum.join(",")

    "podman exec -e LS_CONTROLLERS=" <>
      controllers <> " systemd-aether linstor --machine-readable storage-pool list"
  end

  # -- sections ----------------------------------------------------------------------

  defp pools_section(static, node_ips) do
    case fetch_pools(static, node_ips) do
      {:ok, raw} ->
        %{state: :ok, entries: raw |> unwrap_linstor() |> Enum.map(&pool_view/1), error: nil}

      {:error, reason} ->
        %{state: :unavailable, entries: [], error: describe(reason)}
    end
  end

  defp fetch_pools(%{} = static, _node_ips) when is_map_key(static, :pools) do
    static.pools
  end

  defp fetch_pools(%{}, _node_ips) do
    {:error, :no_pool_fixture}
  end

  defp fetch_pools(nil, []) do
    {:error, :no_cluster_configured}
  end

  defp fetch_pools(nil, node_ips) do
    command = pools_command(node_ips)

    candidates =
      [local_ip() | node_ips]
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Enum.take(@pool_candidates)

    candidates
    |> Task.async_stream(fn ip -> run_pool_command(ip, command) end,
      max_concurrency: 1,
      timeout: @pool_attempt_timeout_ms,
      on_timeout: :kill_task,
      ordered: true
    )
    |> Enum.find_value(fn
      {:ok, {:ok, entries}} -> {:ok, entries}
      _other -> nil
    end)
    |> Kernel.||({:error, :linstor_controller_unreachable})
  end

  defp run_pool_command(ip, command) do
    case Spark.execute(ip, command, timeout: 20) do
      {0, stdout, _stderr} ->
        case Jason.decode(String.trim(stdout || "")) do
          {:ok, decoded} -> {:ok, decoded}
          {:error, _reason} -> {:error, :unparsable_linstor_output}
        end

      {_rc, _stdout, stderr} ->
        {:error, String.trim(stderr || "")}
    end
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  # `linstor --machine-readable` wraps its rows in an outer list. Accept both forms;
  # `mipha.py` does the same because different LINSTOR versions differ here.
  defp unwrap_linstor([[_ | _] = rows | _rest]), do: rows
  defp unwrap_linstor([[] | _rest]), do: []
  defp unwrap_linstor(rows) when is_list(rows), do: rows
  defp unwrap_linstor(_other), do: []

  defp disks_section(static, nodes) do
    fan_out(static, :disks, nodes, &Spark.host_disks/1)
    |> Enum.map(fn
      {node, {:ok, document}} ->
        Map.merge(node, %{state: :ok, error: nil, devices: block_devices(document)})

      {node, {:error, reason}} ->
        Map.merge(node, %{state: :unavailable, error: describe(reason), devices: []})
    end)
  end

  defp resources_section(static, nodes, expected) do
    results = fan_out(static, :drbd, nodes, &Spark.drbd_status/1)

    unreachable =
      for {node, {:error, reason}} <- results,
          do: Map.put(node, :error, describe(reason))

    placements =
      for {node, {:ok, document}} <- results,
          resource <- as_list(document),
          is_map(resource),
          do: {resource_name(resource), placement_view(node, resource)}

    entries =
      placements
      |> Enum.group_by(fn {name, _p} -> name end, fn {_name, p} -> p end)
      |> Enum.map(fn {name, list} -> resource_view(name, list, expected, unreachable) end)
      |> Enum.sort_by(& &1.name)

    state =
      cond do
        nodes == [] -> :unavailable
        length(unreachable) == length(nodes) -> :unavailable
        unreachable != [] -> :partial
        true -> :ok
      end

    %{state: state, entries: entries, unreachable: unreachable}
  end

  # One concurrent read per node, with the result kept next to the node it came from so
  # an unreachable host stays visible instead of silently thinning the list.
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
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  # -- pools -------------------------------------------------------------------------

  defp pool_view(entry) when is_map(entry) do
    provider = string(Map.get(entry, "provider_kind")) || "unknown"
    diskless? = String.contains?(String.upcase(provider), "DISKLESS")
    total = capacity_bytes(Map.get(entry, "total_capacity"))
    free = capacity_bytes(Map.get(entry, "free_capacity"))
    reports = report_messages(Map.get(entry, "reports"))
    used = used_bytes(total, free)

    %{
      name: string(Map.get(entry, "storage_pool_name")) || "unnamed",
      node: string(Map.get(entry, "node_name")) || "unknown",
      provider: provider,
      backing: backing_name(entry),
      diskless?: diskless?,
      total_bytes: total,
      free_bytes: free,
      used_bytes: used,
      used_percent: percent(used, total),
      state: pool_state(diskless?, total, reports),
      messages: reports
    }
  end

  defp pool_view(_other) do
    %{
      name: "unreadable",
      node: "unknown",
      provider: "unknown",
      backing: nil,
      diskless?: false,
      total_bytes: nil,
      free_bytes: nil,
      used_bytes: nil,
      used_percent: nil,
      state: :unknown,
      messages: ["LINSTOR returned an entry this view could not read."]
    }
  end

  defp used_bytes(total, free) when is_integer(total) and is_integer(free) do
    max(total - free, 0)
  end

  defp used_bytes(_total, _free), do: nil

  # A pool whose capacity LINSTOR could not report is `:unknown`, not `:ok`. The old page
  # rendered a blank capacity cell as a healthy 0%-used bar.
  defp pool_state(_diskless?, _total, [_ | _]), do: :error
  defp pool_state(true, _total, []), do: :diskless
  defp pool_state(false, total, []) when is_integer(total) and total > 0, do: :ok
  defp pool_state(false, _total, []), do: :unknown

  @backing_props ["StorDriver/StorPoolName", "StorDriver/ZPool", "StorDriver/LvmVg"]

  defp backing_name(entry) do
    case Map.get(entry, "props") do
      props when is_map(props) ->
        Enum.find_value(@backing_props, fn key -> string(Map.get(props, key)) end)

      _other ->
        nil
    end
  end

  defp report_messages(reports) when is_list(reports) do
    reports
    |> Enum.map(fn
      %{"message" => message} -> string(message)
      message when is_binary(message) -> string(message)
      _other -> nil
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp report_messages(_other), do: []

  # LINSTOR capacities are KiB. The diskless sentinel is dropped rather than converted.
  defp capacity_bytes(value) do
    case integer(value) do
      nil -> nil
      kib when kib < 0 -> nil
      kib when kib >= @implausible_capacity_kib -> nil
      kib -> kib * 1024
    end
  end

  defp capacity(%{state: :unavailable}, _expected) do
    %{
      known?: false,
      raw_total_bytes: nil,
      raw_used_bytes: nil,
      usable_total_bytes: nil,
      usable_used_bytes: nil,
      used_percent: nil
    }
  end

  defp capacity(%{entries: entries}, expected) do
    backed = Enum.filter(entries, &(not &1.diskless? and is_integer(&1.total_bytes)))
    total = Enum.reduce(backed, 0, &(&1.total_bytes + &2))
    used = Enum.reduce(backed, 0, fn pool, acc -> (pool.used_bytes || 0) + acc end)
    factor = max(expected, 1)

    %{
      known?: backed != [],
      raw_total_bytes: total,
      raw_used_bytes: used,
      # Usable capacity is raw divided by the number of copies the cluster keeps. A
      # three-way-replicated fabric does not have its raw capacity available to guests.
      usable_total_bytes: div(total, factor),
      usable_used_bytes: div(used, factor),
      used_percent: percent(used, total)
    }
  end

  # -- DRBD --------------------------------------------------------------------------

  defp resource_name(resource) do
    string(Map.get(resource, "name")) || "unnamed"
  end

  defp placement_view(node, resource) do
    devices = resource |> Map.get("devices") |> as_list() |> Enum.map(&device_view/1)

    connections =
      resource |> Map.get("connections") |> as_list() |> Enum.map(&connection_view/1)

    %{
      ip: node.ip,
      hostname: node.hostname,
      role: string(Map.get(resource, "role")) || "Unknown",
      suspended?: Map.get(resource, "suspended") == true,
      devices: devices,
      connections: connections,
      # A diskless (client) placement is an access point, not a copy of the data.
      replica?: Enum.any?(devices, &(not &1.client?))
    }
  end

  defp device_view(device) when is_map(device) do
    %{
      volume: integer(Map.get(device, "volume")) || 0,
      disk_state: string(Map.get(device, "disk-state")) || "Unknown",
      client?: Map.get(device, "client") == true,
      # `quorum` is absent on DRBD builds without quorum enabled; only an explicit
      # `false` is a lost quorum.
      quorum?: Map.get(device, "quorum") != false,
      size_bytes: kib_to_bytes(Map.get(device, "size"))
    }
  end

  defp device_view(_other) do
    %{volume: 0, disk_state: "Unknown", client?: false, quorum?: true, size_bytes: nil}
  end

  defp connection_view(connection) when is_map(connection) do
    peer_devices =
      connection |> Map.get("peer_devices") |> as_list() |> Enum.map(&peer_device_view/1)

    %{
      peer: string(Map.get(connection, "name")) || "unknown peer",
      state: string(Map.get(connection, "connection")) || "Unknown",
      peer_role: string(Map.get(connection, "peer-role")) || "Unknown",
      peer_devices: peer_devices
    }
  end

  defp connection_view(_other) do
    %{peer: "unknown peer", state: "Unknown", peer_role: "Unknown", peer_devices: []}
  end

  defp peer_device_view(peer) when is_map(peer) do
    %{
      volume: integer(Map.get(peer, "volume")) || 0,
      peer_disk_state: string(Map.get(peer, "peer-disk-state")) || "Unknown",
      replication: string(Map.get(peer, "replication")) || "Unknown",
      client?: Map.get(peer, "peer-client") == true
    }
  end

  defp peer_device_view(_other) do
    %{volume: 0, peer_disk_state: "Unknown", replication: "Unknown", client?: false}
  end

  defp resource_view(name, placements, expected, unreachable) do
    placements = Enum.sort_by(placements, & &1.hostname)
    replicas = Enum.count(placements, & &1.replica?)
    primaries = for p <- placements, p.role == @primary, do: p.hostname
    evidenced = replicas + peer_replicas(placements)

    issues =
      placement_issues(placements) ++
        replica_issues(replicas, evidenced, expected, unreachable) ++
        dual_primary_issues(name, primaries)

    %{
      name: name,
      placements: placements,
      replicas: replicas,
      evidenced_replicas: evidenced,
      expected_replicas: expected,
      primaries: primaries,
      # `nil`, not `false`: a node we could not read might hold the missing replica, and
      # might equally hold nothing. Saying "not under-replicated" here would be a claim
      # the data does not support.
      under_replicated?: if(unreachable == [], do: replicas < expected),
      issues: issues,
      health: health(issues, unreachable),
      size_bytes: largest_device_size(placements)
    }
  end

  defp placement_issues(placements) do
    Enum.flat_map(placements, fn placement ->
      suspended_issue(placement) ++
        device_issues(placement) ++
        connection_issues(placement)
    end)
  end

  defp suspended_issue(%{suspended?: true, hostname: host}) do
    ["#{host}: I/O is suspended"]
  end

  defp suspended_issue(_placement), do: []

  defp device_issues(placement) do
    Enum.flat_map(placement.devices, fn device ->
      cond do
        device.client? and device.disk_state in ["Diskless", @uptodate] ->
          []

        device.disk_state != @uptodate ->
          ["#{placement.hostname}: volume #{device.volume} is #{device.disk_state}"]

        not device.quorum? ->
          ["#{placement.hostname}: volume #{device.volume} has lost quorum"]

        true ->
          []
      end
    end)
  end

  defp connection_issues(placement) do
    Enum.flat_map(placement.connections, fn connection ->
      if connection.state == @connected do
        peer_device_issues(placement, connection)
      else
        ["#{placement.hostname}: connection to #{connection.peer} is #{connection.state}"]
      end
    end)
  end

  defp peer_device_issues(placement, connection) do
    Enum.flat_map(connection.peer_devices, fn peer ->
      cond do
        peer.client? ->
          []

        peer.peer_disk_state != @uptodate ->
          [
            "#{placement.hostname}: #{connection.peer} volume #{peer.volume} is " <>
              peer.peer_disk_state
          ]

        peer.replication != @established ->
          [
            "#{placement.hostname}: replication to #{connection.peer} is " <>
              peer.replication
          ]

        true ->
          []
      end
    end)
  end

  # A copy on a node that did not answer is still visible from the nodes that did: DRBD
  # names its peers and reports their disk state. Counting those keeps an unreachable
  # node from producing a false under-replication warning for a resource that is in fact
  # fully replicated -- while the resource still reads `:unknown`, because peer-reported
  # state is second-hand.
  defp peer_replicas(placements) do
    reporting = MapSet.new(placements, & &1.hostname)

    placements
    |> Enum.flat_map(& &1.connections)
    |> Enum.reject(&MapSet.member?(reporting, &1.peer))
    |> Enum.filter(fn connection ->
      Enum.any?(connection.peer_devices, &(not &1.client?))
    end)
    |> Enum.map(& &1.peer)
    |> Enum.uniq()
    |> length()
  end

  # Every node answered, so the count of copies seen locally is the count of copies.
  defp replica_issues(replicas, _evidenced, expected, []) when replicas < expected do
    ["#{replicas} of #{expected} replicas present"]
  end

  defp replica_issues(_replicas, _evidenced, _expected, []), do: []

  # With a node unread, the count is a floor rather than a fact, so it is reported as
  # something that could not be seen rather than as under-replication.
  defp replica_issues(_replicas, evidenced, expected, [_ | _]) when evidenced < expected do
    ["only #{evidenced} of #{expected} replicas could be seen; some nodes did not answer"]
  end

  defp replica_issues(_replicas, _evidenced, _expected, _unreachable), do: []

  defp dual_primary_issues(name, primaries) when length(primaries) > 1 do
    if String.starts_with?(name, @dual_primary_prefix) do
      []
    else
      ["Primary on #{Enum.join(primaries, " and ")} at once (dual-primary)"]
    end
  end

  defp dual_primary_issues(_name, _primaries), do: []

  defp health([], []), do: :ok
  defp health([], [_ | _]), do: :unknown
  defp health([_ | _], _unreachable), do: :degraded

  defp largest_device_size(placements) do
    placements
    |> Enum.flat_map(& &1.devices)
    |> Enum.map(& &1.size_bytes)
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> nil
      sizes -> Enum.max(sizes)
    end
  end

  # `drbdsetup` reports device sizes in KiB.
  defp kib_to_bytes(value) do
    case integer(value) do
      nil -> nil
      kib -> kib * 1024
    end
  end

  # -- block devices -----------------------------------------------------------------

  defp block_devices(%{"blockdevices" => devices}), do: flatten_devices(devices, 0)
  defp block_devices(devices) when is_list(devices), do: flatten_devices(devices, 0)
  defp block_devices(_other), do: []

  defp flatten_devices(devices, depth) when is_list(devices) do
    Enum.flat_map(devices, fn
      device when is_map(device) ->
        children = Map.get(device, "children")
        [device_row(device, depth) | flatten_devices(children || [], depth + 1)]

      _other ->
        []
    end)
  end

  defp flatten_devices(_devices, _depth), do: []

  defp device_row(device, depth) do
    %{
      depth: depth,
      name: string(Map.get(device, "name")) || "unknown",
      path: string(Map.get(device, "path")),
      type: string(Map.get(device, "type")) || "unknown",
      size_bytes: integer(Map.get(device, "size")),
      mountpoint: mountpoint(device),
      fstype: string(Map.get(device, "fstype")),
      model: string(Map.get(device, "model")),
      serial: string(Map.get(device, "serial")),
      # `rota` is 1 for spinning media, 0 for flash. Older lsblk emits booleans.
      rotational?: rotational?(Map.get(device, "rota"))
    }
  end

  defp mountpoint(device) do
    case Map.get(device, "mountpoint") do
      value when is_binary(value) -> string(value)
      _other -> device |> Map.get("mountpoints") |> as_list() |> Enum.find_value(&string/1)
    end
  end

  defp rotational?(true), do: true
  defp rotational?(false), do: false
  defp rotational?(1), do: true
  defp rotational?(0), do: false
  defp rotational?("1"), do: true
  defp rotational?("0"), do: false
  defp rotational?(_other), do: nil

  # -- summary -----------------------------------------------------------------------

  defp summarize(pools, resources, disks) do
    by_health = Enum.frequencies_by(resources.entries, & &1.health)

    %{
      pools_total: length(pools.entries),
      pools_error: Enum.count(pools.entries, &(&1.state == :error)),
      pools_unknown: Enum.count(pools.entries, &(&1.state == :unknown)),
      resources_total: length(resources.entries),
      resources_ok: Map.get(by_health, :ok, 0),
      resources_degraded: Map.get(by_health, :degraded, 0),
      resources_unknown: Map.get(by_health, :unknown, 0),
      resources_under_replicated: Enum.count(resources.entries, &(&1.under_replicated? == true)),
      nodes_unreadable: Enum.count(disks, &(&1.state == :unavailable)),
      # The one flag the header acts on: anything not positively healthy.
      attention?:
        pools.state == :unavailable or resources.state != :ok or
          Map.get(by_health, :degraded, 0) > 0 or Map.get(by_health, :unknown, 0) > 0 or
          Enum.any?(pools.entries, &(&1.state in [:error, :unknown]))
    }
  end

  # -- configuration ------------------------------------------------------------------

  defp static_payload(opts) do
    if Keyword.keyword?(opts) and Keyword.has_key?(opts, :static) do
      Keyword.fetch!(opts, :static)
    else
      case source() do
        {:static, payload} when is_map(payload) -> payload
        _other -> nil
      end
    end
  end

  defp node_ips(%{} = static) when is_map_key(static, :node_ips), do: static.node_ips

  defp node_ips(%{} = static) do
    (Map.get(static, :drbd) || Map.get(static, :disks) || %{}) |> Map.keys() |> Enum.sort()
  end

  defp node_ips(nil) do
    Config.node_ips()
  catch
    :exit, _reason -> []
  end

  defp expected_replicas(%{} = static, node_count) when is_map_key(static, :redundancy_factor) do
    clamp_replicas(static.redundancy_factor, node_count)
  end

  defp expected_replicas(_static, node_count) do
    factor =
      try do
        Config.redundancy_factor()
      catch
        :exit, _reason -> 0
      end

    clamp_replicas(factor, node_count)
  end

  # A single-node cluster keeps one copy however the redundancy factor reads; asking for
  # two replicas there would flag every resource on a deployment that is behaving.
  defp clamp_replicas(factor, node_count) do
    factor = integer(factor) || 0
    max(min(factor + 1, max(node_count, 1)), 1)
  end

  defp local_ip do
    Config.local_ip()
  catch
    :exit, _reason -> nil
  end

  defp hostname_for(ip) do
    Config.hostname_for(ip)
  catch
    :exit, _reason -> ip
  end

  # -- small helpers -------------------------------------------------------------------

  defp normalize_ip(value) when is_binary(value) do
    case :inet.parse_strict_address(String.to_charlist(String.trim(value))) do
      {:ok, address} -> address |> :inet.ntoa() |> to_string()
      {:error, _reason} -> nil
    end
  end

  defp normalize_ip(_value), do: nil

  defp as_list(value) when is_list(value), do: value
  defp as_list(_value), do: []

  defp percent(used, total) when is_integer(used) and is_integer(total) and total > 0 do
    used / total * 100
  end

  defp percent(_used, _total), do: nil

  defp string(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp string(_value), do: nil

  defp integer(value) when is_integer(value), do: value
  defp integer(value) when is_float(value), do: trunc(value)

  defp integer(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {int, _rest} -> int
      :error -> nil
    end
  end

  defp integer(_value), do: nil

  defp describe(nil), do: nil
  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)
end
