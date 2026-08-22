defmodule SpectrumPhx.Storage do
  @moduledoc """
  Read-only assembly of the storage fabric: per-node extent stores, the vdisks served
  from them, peer reachability, and the block-device inventory underneath.

  ## What this reads, and what it used to

  The page this replaces described LINSTOR pools and DRBD resources. Both are gone, and
  the shape of the question changed with them. DRBD replicated *devices*, so a resource
  had a role on each node, a connection state per peer, a disk state per volume, and
  health was a matter of deciding which combinations of those were bad. Sidon replicates
  extents behind a single owner, so a vdisk has one owner, one epoch, and a replica set —
  and the interesting question is no longer "do the copies agree" but "are all the copies
  there".

  That is not a simplification for its own sake. There is no divergence to detect,
  because a write that has not reached every replica is not acknowledged; there is no
  split-brain to report, because every replica persists the highest epoch it has been
  fenced at and refuses anything older. The states this module can distinguish are the
  states the storage layer can actually be in.

  ## Health is derived, not assumed

    * a store is healthy when its filesystem answers and has room in it; a store that
      reports no capacity is `:unknown`, not an empty-and-fine `0%`;
    * a vdisk is healthy when it is not degraded and its replica set meets the cluster's
      redundancy factor;
    * a peer that cannot be reached is a fault on every vdisk replicated to it, because
      the journal is write-all: an append that does not reach a replica is refused, so an
      unreachable peer means those guests are taking EIO right now, not "reduced
      redundancy".

  ## Unknown is not healthy

  Every per-node read can fail, and a node that does not answer tells us nothing about
  what it holds. Such a node is recorded in `:unreachable`, and a vdisk that would
  otherwise look clean is reported `:unknown` rather than `:ok` — with
  `under_replicated?` `nil` rather than `false`, because a replica we could not count is
  not a replica we know is absent. A dev machine with no cluster reports
  `configured?: false` and unavailable sections, which is a different statement from
  "everything is fine".

  ## Test seam

  `snapshot/1` reads through `source/0`, which defaults to `:live` and can be set to
  `{:static, map}` in application env. The static payloads are the *raw* daemon shapes
  (Sidon's `capacity`, `list` and `peers` documents, `lsblk -J` documents), so the
  parsing and health derivation this module exists for are what the tests exercise, not
  a pre-chewed result.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Spark

  @pubsub SpectrumPhx.PubSub
  @topic "storage:status"

  # Caps the fan-out from this side, so one wedged node cannot hold the page open.
  @fanout_timeout_ms 8_000

  # A store this full cannot drain: a guest whose journal fills backpressures and stops.
  # Warned well before that, because reclaiming space is Purah's job and takes time.
  @store_full_percent 95
  @store_warn_percent 85

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
        expected_replicas: pos_integer,
        stores: %{state: :ok | :partial | :unavailable, entries: [store],
                  unreachable: [%{ip:, hostname:, error:}]},
        vdisks: %{state: :ok | :partial | :unavailable, entries: [vdisk],
                  unreachable: [%{ip:, hostname:, error:}]},
        peers: %{state: :ok | :partial | :unavailable, unreachable: [link]},
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

    stores = stores_section(static, nodes)
    peers = peers_section(static, nodes)
    disks = disks_section(static, nodes)
    vdisks = vdisks_section(static, nodes, expected, peers)
    capacity = capacity(stores, expected)

    %{
      configured?: node_ips != [],
      nodes: nodes,
      expected_replicas: expected,
      stores: stores,
      vdisks: vdisks,
      peers: peers,
      disks: disks,
      capacity: capacity,
      summary: summarize(stores, vdisks, peers, disks)
    }
  end

  @doc """
  Where storage reads come from: `:live` (the default) or `{:static, map}`.

  The static map may carry `:capacity`, `:vdisks`, `:peers`, `:disks`, `:node_ips` and
  `:redundancy_factor`; see the moduledoc. Anything it omits is treated as unavailable
  rather than as empty-and-fine.
  """
  @spec source() :: :live | {:static, map()}
  def source, do: Application.get_env(:spectrum_phx, :storage_source, :live)

  # -- extent stores -------------------------------------------------------------------

  defp stores_section(static, nodes) do
    results = fan_out(static, :capacity, nodes, &Spark.dfs_capacity/1)

    unreachable =
      for {node, {:error, reason}} <- results,
          do: Map.put(node, :error, describe(reason))

    entries =
      for {node, {:ok, document}} <- results,
          do: store_view(node, document)

    %{state: section_state(nodes, unreachable), entries: entries, unreachable: unreachable}
  end

  defp store_view(node, document) when is_map(document) do
    total = integer(Map.get(document, "total_bytes"))
    available = integer(Map.get(document, "available_bytes"))
    used = used_bytes(total, available)
    percent = percent(used, total)

    %{
      ip: node.ip,
      hostname: node.hostname,
      node: string(Map.get(document, "node")) || node.hostname,
      path: string(Map.get(document, "path")),
      total_bytes: total,
      available_bytes: available,
      used_bytes: used,
      used_percent: percent,
      egroup_bytes: integer(Map.get(document, "egroup_bytes")),
      egroup_count: integer(Map.get(document, "egroup_count")),
      journal_bytes: integer(Map.get(document, "journal_bytes")),
      state: store_state(total, percent),
      messages: store_messages(node, total, percent)
    }
  end

  defp store_view(node, _other) do
    %{
      ip: node.ip,
      hostname: node.hostname,
      node: node.hostname,
      path: nil,
      total_bytes: nil,
      available_bytes: nil,
      used_bytes: nil,
      used_percent: nil,
      egroup_bytes: nil,
      egroup_count: nil,
      journal_bytes: nil,
      state: :unknown,
      messages: ["Sidon answered with something this view could not read."]
    }
  end

  # A store reporting no capacity is not an empty one -- it is almost always a store that
  # is not mounted, which is why it is `:unknown` and never a healthy 0%.
  defp store_state(total, _percent) when not is_integer(total) or total <= 0, do: :unknown

  defp store_state(_total, percent) when is_number(percent) and percent >= @store_full_percent,
    do: :full

  defp store_state(_total, percent) when is_number(percent) and percent >= @store_warn_percent,
    do: :warn

  defp store_state(_total, percent) when is_number(percent), do: :ok
  defp store_state(_total, _percent), do: :unknown

  defp store_messages(node, total, _percent) when not is_integer(total) or total <= 0 do
    [
      "#{node.hostname}: the extent store reports no capacity, which usually means it is not mounted"
    ]
  end

  defp store_messages(node, _total, percent)
       when is_number(percent) and percent >= @store_full_percent do
    ["#{node.hostname}: the extent store is #{format_percent(percent)} full and cannot drain"]
  end

  defp store_messages(node, _total, percent)
       when is_number(percent) and percent >= @store_warn_percent do
    ["#{node.hostname}: the extent store is #{format_percent(percent)} full"]
  end

  defp store_messages(_node, _total, _percent), do: []

  defp used_bytes(total, available) when is_integer(total) and is_integer(available) do
    max(total - available, 0)
  end

  defp used_bytes(_total, _available), do: nil

  defp capacity(%{entries: []}, _expected) do
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
    measured = Enum.filter(entries, &is_integer(&1.total_bytes))
    total = Enum.reduce(measured, 0, &(&1.total_bytes + &2))
    used = Enum.reduce(measured, 0, fn store, acc -> (store.used_bytes || 0) + acc end)
    factor = max(expected, 1)

    %{
      known?: measured != [],
      raw_total_bytes: total,
      raw_used_bytes: used,
      # Usable capacity is raw divided by the number of copies the cluster keeps. A
      # three-way-replicated fabric does not have its raw capacity available to guests.
      usable_total_bytes: div(total, factor),
      usable_used_bytes: div(used, factor),
      used_percent: percent(used, total)
    }
  end

  # -- peers ---------------------------------------------------------------------------

  defp peers_section(static, nodes) do
    results = fan_out(static, :peers, nodes, &Spark.dfs_peers/1)

    unreachable =
      for {node, {:ok, document}} <- results,
          peer <- as_list(Map.get(document, "peers")),
          is_map(peer),
          Map.get(peer, "reachable") == false do
        %{
          from: node.hostname,
          from_ip: node.ip,
          peer: string(Map.get(peer, "node")) || "unknown peer",
          detail: string(Map.get(peer, "detail"))
        }
      end

    unread =
      for {node, {:error, reason}} <- results,
          do: Map.put(node, :error, describe(reason))

    state =
      cond do
        nodes == [] -> :unavailable
        length(unread) == length(nodes) -> :unavailable
        unread != [] -> :partial
        true -> :ok
      end

    %{state: state, unreachable: unreachable, unread: unread}
  end

  # Which nodes some peer says it cannot reach. Used to explain a degraded vdisk without
  # making the reader cross-reference two tables.
  defp unreachable_nodes(%{unreachable: links}) do
    MapSet.new(links, & &1.peer)
  end

  # -- vdisks --------------------------------------------------------------------------

  defp vdisks_section(static, nodes, expected, peers) do
    results = fan_out(static, :vdisks, nodes, &Spark.dfs_list/1)

    unreachable =
      for {node, {:error, reason}} <- results,
          do: Map.put(node, :error, describe(reason))

    attachments =
      for {node, {:ok, document}} <- results,
          attachment <- as_list(Map.get(document, "attached")),
          is_map(attachment),
          do: {vdisk_id(attachment), attachment_view(node, attachment)}

    down = unreachable_nodes(peers)

    entries =
      attachments
      |> Enum.group_by(fn {id, _a} -> id end, fn {_id, a} -> a end)
      |> Enum.map(fn {id, list} -> vdisk_view(id, list, expected, unreachable, down) end)
      |> Enum.sort_by(& &1.id)

    %{
      state: section_state(nodes, unreachable),
      entries: entries,
      unreachable: unreachable
    }
  end

  defp vdisk_id(attachment) do
    string(Map.get(attachment, "vdisk_id")) || "unnamed"
  end

  defp attachment_view(node, attachment) do
    %{
      ip: node.ip,
      hostname: node.hostname,
      role: role(Map.get(attachment, "role")),
      socket: string(Map.get(attachment, "socket")),
      epoch: integer(Map.get(attachment, "epoch")),
      size_bytes: integer(Map.get(attachment, "size_bytes")),
      class: string(Map.get(attachment, "class")),
      degraded?: Map.get(attachment, "degraded") == true,
      replicas: replica_names(Map.get(attachment, "replicas")),
      forwarding_to: string(Map.get(attachment, "forwarding_to"))
    }
  end

  defp role("owner"), do: :owner
  defp role("forwarding"), do: :forwarding
  defp role(_other), do: :unknown

  defp replica_names(values) when is_list(values) do
    values |> Enum.map(&string/1) |> Enum.reject(&is_nil/1) |> Enum.uniq() |> Enum.sort()
  end

  defp replica_names(_other), do: []

  defp vdisk_view(id, attachments, expected, unreachable, down) do
    owner = Enum.find(attachments, &(&1.role == :owner))
    forwarders = for a <- attachments, a.role == :forwarding, do: a.hostname

    replicas = (owner && owner.replicas) || []
    replica_count = length(replicas)
    stranded = Enum.filter(replicas, &MapSet.member?(down, &1))

    issues =
      owner_issues(id, owner, attachments) ++
        replica_issues(owner, replica_count, expected, unreachable) ++
        stranded_issues(stranded)

    %{
      id: id,
      owner: owner && owner.hostname,
      owner_ip: owner && owner.ip,
      epoch: owner && owner.epoch,
      size_bytes: owner && owner.size_bytes,
      class: (owner && owner.class) || "unknown",
      sealed?: owner && owner.class == "immutable",
      degraded?: owner && owner.degraded?,
      socket: owner && owner.socket,
      replicas: replicas,
      replica_count: replica_count,
      expected_replicas: expected,
      forwarders: Enum.sort(forwarders),
      attachments: Enum.sort_by(attachments, & &1.hostname),
      # `nil`, not `false`: a node we could not read might be the owner, and the replica
      # set is only visible from there. Saying "not under-replicated" would be a claim
      # the data does not support.
      under_replicated?: if(unreachable == [] and owner, do: replica_count < expected),
      issues: issues,
      health: health(issues, owner, unreachable),
      stranded_replicas: stranded
    }
  end

  # Two owners at once is the state the epoch fence exists to make impossible, so seeing
  # it means the fence itself is not working -- reported as loudly as it deserves rather
  # than folded in with a missing replica.
  defp owner_issues(id, _owner, attachments) do
    owners = for a <- attachments, a.role == :owner, do: a.hostname

    cond do
      length(owners) > 1 ->
        [
          "#{id} is owned by #{Enum.join(owners, " and ")} at once, which the epoch fence " <>
            "is supposed to prevent"
        ]

      owners == [] ->
        ["#{id} has no owner on any node that answered; it is being relayed, not served"]

      true ->
        degraded_issue(attachments)
    end
  end

  defp degraded_issue(attachments) do
    for a <- attachments, a.role == :owner, a.degraded?, do: "#{a.hostname}: writes are refused"
  end

  # Every node answered, so the replica set the owner reports is the replica set.
  defp replica_issues(owner, count, expected, []) when not is_nil(owner) and count < expected do
    ["#{count} of #{expected} replicas present"]
  end

  defp replica_issues(_owner, _count, _expected, _unreachable), do: []

  defp stranded_issues([]), do: []

  defp stranded_issues(nodes) do
    ["replica on #{Enum.join(nodes, ", ")} is unreachable, so writes are being refused"]
  end

  # No owner on any node that answered means nothing here can say how the vdisk is; the
  # owner is the only node that knows its replica set.
  defp health(_issues, nil, _unreachable), do: :unknown
  defp health([], _owner, []), do: :ok
  defp health([], _owner, [_ | _]), do: :unknown
  defp health([_ | _], _owner, _unreachable), do: :degraded

  # -- block devices -------------------------------------------------------------------

  defp disks_section(static, nodes) do
    fan_out(static, :disks, nodes, &Spark.host_disks/1)
    |> Enum.map(fn
      {node, {:ok, document}} ->
        Map.merge(node, %{state: :ok, error: nil, devices: block_devices(document)})

      {node, {:error, reason}} ->
        Map.merge(node, %{state: :unavailable, error: describe(reason), devices: []})
    end)
  end

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

  # -- fan-out -------------------------------------------------------------------------

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

  defp section_state([], _unreachable), do: :unavailable

  defp section_state(nodes, unreachable) do
    cond do
      length(unreachable) == length(nodes) -> :unavailable
      unreachable != [] -> :partial
      true -> :ok
    end
  end

  # -- summary -------------------------------------------------------------------------

  defp summarize(stores, vdisks, peers, disks) do
    by_health = Enum.frequencies_by(vdisks.entries, & &1.health)
    stores_full = Enum.count(stores.entries, &(&1.state == :full))
    stores_unknown = Enum.count(stores.entries, &(&1.state == :unknown))

    %{
      stores_total: length(stores.entries),
      stores_full: stores_full,
      stores_warn: Enum.count(stores.entries, &(&1.state == :warn)),
      stores_unknown: stores_unknown,
      vdisks_total: length(vdisks.entries),
      vdisks_ok: Map.get(by_health, :ok, 0),
      vdisks_degraded: Map.get(by_health, :degraded, 0),
      vdisks_unknown: Map.get(by_health, :unknown, 0),
      vdisks_under_replicated: Enum.count(vdisks.entries, &(&1.under_replicated? == true)),
      peer_links_down: length(peers.unreachable),
      nodes_unreadable: Enum.count(disks, &(&1.state == :unavailable)),
      # The one flag the header acts on: anything not positively healthy.
      attention?:
        stores.state != :ok or vdisks.state != :ok or peers.unreachable != [] or
          stores_full > 0 or stores_unknown > 0 or
          Map.get(by_health, :degraded, 0) > 0 or Map.get(by_health, :unknown, 0) > 0
    }
  end

  # -- configuration -------------------------------------------------------------------

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
    (Map.get(static, :vdisks) || Map.get(static, :capacity) || Map.get(static, :disks) || %{})
    |> Map.keys()
    |> Enum.sort()
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
  # two replicas there would flag every vdisk on a deployment that is behaving. This is
  # ftt=0, a supported topology rather than a broken one.
  defp clamp_replicas(factor, node_count) do
    factor = integer(factor) || 0
    max(min(factor + 1, max(node_count, 1)), 1)
  end

  defp hostname_for(ip) do
    Config.hostname_for(ip)
  catch
    :exit, _reason -> ip
  end

  # -- small helpers ---------------------------------------------------------------------

  defp as_list(value) when is_list(value), do: value
  defp as_list(_value), do: []

  defp percent(used, total) when is_integer(used) and is_integer(total) and total > 0 do
    used / total * 100
  end

  defp percent(_used, _total), do: nil

  defp format_percent(value) when is_number(value) do
    :erlang.float_to_binary(value / 1, decimals: 1) <> "%"
  end

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
