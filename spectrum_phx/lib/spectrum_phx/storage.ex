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
          do: node |> Map.put(:error, describe(reason)) |> Map.put(:status, node_status(reason))

    entries =
      for {node, {:ok, document}} <- results,
          do: store_view(node, document)

    %{state: section_state(nodes, unreachable), entries: entries, unreachable: unreachable}
  end

  # What a node that gave no capacity is doing, as far as the answer says. Spark answers 503
  # when the node is up but Sidon's control socket is not: the daemon is restarting, or its
  # disks are not mounted yet, and the capacity is not zero -- it is not known. That is a
  # different statement from a node that does not answer at all, so it is kept apart and the
  # page can say "starting" instead of leaving the operator to guess from a transport error.
  defp node_status({503, _message}), do: :starting
  defp node_status(_reason), do: :unreachable

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
      # What the vdisk was created asking for, which is neither the copies it holds nor what
      # the cluster policy asks today. `nil` from a daemon that predates the field.
      rf: integer(Map.get(attachment, "rf")),
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
    requested = owner && owner.rf
    stranded = Enum.filter(replicas, &MapSet.member?(down, &1))

    issues =
      owner_issues(id, owner, attachments) ++
        replica_issues(owner, replica_count, expected, requested, unreachable) ++
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
      requested_replicas: requested,
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
    claimants = for a <- attachments, a.role == :owner, do: a

    cond do
      contested?(claimants) ->
        [
          "#{id} is owned by #{claimants |> Enum.map(& &1.hostname) |> Enum.join(" and ")} " <>
            "at the same epoch, which the epoch fence is supposed to prevent"
        ]

      claimants == [] ->
        ["#{id} has no owner on any node that answered; it is being relayed, not served"]

      true ->
        degraded_issue(attachments)
    end
  end

  @doc false
  # Two nodes claiming ownership is only a fault when they claim it at the *same* epoch.
  #
  # Attaching a vdisk bumps its epoch and fences every replica against the new one, so a
  # node that used to own it keeps a local record at a lower epoch until something clears
  # it. That record is stale, not a second owner -- the fence has already refused it, and
  # the highest epoch is the owner by definition.
  #
  # Reporting those as "owned by two nodes at once, which the epoch fence is supposed to
  # prevent" is how a working fence gets reported as a broken one. It happens routinely:
  # any guest that has been scheduled onto a different host leaves the old one behind.
  #
  # Epochs that cannot be read are treated as contested, because two owners and no way to
  # tell which is current is exactly when an operator should be told.
  def contested?(claimants) when length(claimants) > 1 do
    epochs = Enum.map(claimants, & &1.epoch)

    cond do
      Enum.any?(epochs, &is_nil/1) -> true
      true -> length(Enum.uniq(epochs)) < length(epochs)
    end
  end

  def contested?(_claimants), do: false

  defp degraded_issue(attachments) do
    for a <- attachments, a.role == :owner, a.degraded?, do: "#{a.hostname}: writes are refused"
  end

  # Every node answered, so the replica set the owner reports is the replica set.
  #
  # Three different states hide behind "fewer copies than the cluster asks for", and they
  # want different responses, so they are told apart when the vdisk recorded what it asked
  # for:
  #
  #   * it holds fewer copies than it asked for -- something was lost, and Purah heals it;
  #   * it holds what it asked for, which is less than the policy asks *now* -- nothing is
  #     lost, it was created under a weaker policy (or, until the create path named a
  #     container, under none), and nothing will grow it unless an operator tops it up;
  #   * it recorded nothing, a daemon that predates the field -- the old message.
  #
  # Reading the second as healthy because it matches its own request is the mistake this
  # exists to avoid: a vdisk that asked for one copy has one copy, and one copy is exactly
  # what a node loss destroys.
  defp replica_issues(owner, count, expected, requested, [])
       when not is_nil(owner) and count < expected do
    cond do
      is_nil(requested) ->
        ["#{count} of #{expected} replicas present"]

      count < requested ->
        ["#{count} of the #{requested} replicas it asked for present"]

      true ->
        [
          "asked for #{requested} #{copies(requested)} when it was created and the cluster now " <>
            "asks for #{expected}; it was never topped up"
        ]
    end
  end

  defp replica_issues(_owner, _count, _expected, _requested, _unreachable), do: []

  defp copies(1), do: "copy"
  defp copies(_n), do: "copies"

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

  # Under khost, a single-node cluster keeps one copy. Under kdisk, a single-node
  # cluster can mirror extents across local physical drives, supporting FTT 1 (2 copies).
  defp clamp_replicas(factor, node_count) do
    factor = integer(factor) || 0
    domain =
      try do
        Config.fault_domain()
      catch
        :exit, _reason -> "khost"
      end

    if domain == "kdisk" do
      max(min(factor + 1, 2), 1)
    else
      max(min(factor + 1, max(node_count, 1)), 1)
    end
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

  # -- Purah & storage maintenance operations -------------------------------------------

  @doc "Run a Purah or DFS maintenance operation via Catalyst."
  def run_maintenance("sweep"), do: run_sweep()
  def run_maintenance("scrub"), do: run_scrub()
  def run_maintenance("compact"), do: run_compaction(false)
  def run_maintenance("compact_apply"), do: run_compaction(true)
  def run_maintenance("dedup_estimate"), do: run_dedup_estimate()
  def run_maintenance("replicate_all"), do: run_replicate_all()
  def run_maintenance(other), do: {:error, "Unknown storage maintenance operation: #{inspect(other)}"}

  @doc "Trigger Purah garbage collection sweep."
  def run_sweep do
    SpectrumPhx.Catalyst.submit(
      "dagur",
      "execute",
      %{
        "job_name" => "Purah Garbage Collection Sweep",
        "command" => "/usr/local/bin/ncli storage.sweep"
      },
      component: "Purah"
    )
  end

  @doc "Trigger Purah integrity scrub."
  def run_scrub do
    SpectrumPhx.Catalyst.submit(
      "dagur",
      "execute",
      %{
        "job_name" => "Purah Integrity Scrub",
        "command" => "/usr/local/bin/ncli storage.scrub"
      },
      component: "Purah"
    )
  end

  @doc "Trigger Purah extent group compaction plan or execution."
  def run_compaction(apply? \\ false) do
    cmd =
      if apply? in [true, "true", "1", "apply"],
        do: "/usr/local/bin/ncli storage.compact --apply",
        else: "/usr/local/bin/ncli storage.compact"

    label =
      if apply? in [true, "true", "1", "apply"],
        do: "Purah Compaction (Apply)",
        else: "Purah Compaction (Plan)"

    SpectrumPhx.Catalyst.submit(
      "dagur",
      "execute",
      %{
        "job_name" => label,
        "command" => cmd
      },
      component: "Purah"
    )
  end

  @doc "Trigger DFS deduplication savings estimate."
  def run_dedup_estimate do
    SpectrumPhx.Catalyst.submit(
      "dagur",
      "execute",
      %{
        "job_name" => "DFS Deduplication Estimate",
        "command" => "/usr/local/bin/ncli storage.dedup.estimate"
      },
      component: "Purah"
    )
  end

  @doc "Trigger replication of all under-replicated vdisks."
  def run_replicate_all do
    SpectrumPhx.Catalyst.submit(
      "dagur",
      "execute",
      %{
        "job_name" => "Replicate Under-replicated Vdisks",
        "command" => "/usr/local/bin/ncli storage.replicate --all"
      },
      component: "Storage"
    )
  end

  # -- Helios Files (HA NFSv4 / SMB3 file shares) ---------------------------------------

  @files_shares_cql "SELECT share_name, vdisk_id, container, size_gb, protocol, export_path, allowed_clients, host_ip, vip, state, squash_mode, smb_guest_ok, created_at_ms, updated_at_ms FROM hydra.files_shares"
  @share_name_re ~r/^[a-z0-9][a-z0-9_-]{0,46}$/
  @share_protocols ~w(nfs smb dual)
  @share_squash_modes ~w(root_squash no_root_squash all_squash)

  @doc "Where Helios Files share reads come from: `:hydra` (default) or `{:static, rows}`."
  def files_source, do: Application.get_env(:spectrum_phx, :files_source, :hydra)

  @doc "Valid Helios Files protocols (`nfs`, `smb`, `dual`)."
  def share_protocols, do: @share_protocols

  @doc "Valid NFS squash modes."
  def share_squash_modes, do: @share_squash_modes

  @doc "List all Helios Files NFSv4/SMB3 shares from `hydra.files_shares`."
  def list_file_shares(opts \\ []) do
    src = Keyword.get(opts, :source, files_source())

    case src do
      {:static, rows} when is_list(rows) ->
        {:ok, rows |> Enum.map(&normalise_share_row/1) |> Enum.sort_by(& &1.share_name)}

      :hydra ->
        try do
          case SpectrumPhx.Hydra.query(@files_shares_cql, []) do
            {:ok, rows} ->
              {:ok, rows |> Enum.map(&normalise_share_row/1) |> Enum.sort_by(& &1.share_name)}

            {:error, reason} ->
              {:error, describe(reason)}
          end
        rescue
          exception -> {:error, Exception.message(exception)}
        catch
          :exit, reason -> {:error, describe(reason)}
        end
    end
  end

  @doc "Create a scale-out HA NFSv4/SMB3 file share in `hydra.files_shares`."
  def create_file_share(attrs, opts \\ []) when is_map(attrs) do
    share_name = string(attrs["share_name"] || attrs[:share_name])
    protocol = (string(attrs["protocol"] || attrs[:protocol]) || "nfs") |> String.downcase()
    size_gb = integer(attrs["size_gb"] || attrs[:size_gb]) || 50
    container = string(attrs["container"] || attrs[:container]) || "default-pool"
    allowed_clients = string(attrs["allowed_clients"] || attrs[:allowed_clients]) || "*"
    squash_mode = (string(attrs["squash_mode"] || attrs[:squash_mode]) || "root_squash") |> String.downcase()
    smb_guest_ok = (attrs["smb_guest_ok"] || attrs[:smb_guest_ok]) in [true, "true", "1", "on", 1]

    cond do
      is_nil(share_name) or not Regex.match?(@share_name_re, share_name) ->
        {:error, "Share name must be 1..47 lowercase alphanumeric characters, '-' or '_'."}

      protocol not in @share_protocols ->
        {:error, "Protocol must be nfs, smb, or dual."}

      size_gb < 1 or size_gb > 65536 ->
        {:error, "Share size must be between 1 GB and 65536 GB."}

      squash_mode not in @share_squash_modes ->
        {:error, "Squash mode must be root_squash, no_root_squash, or all_squash."}

      true ->
        vdisk_id = "files-#{share_name}"
        export_path = "/srv/helios-files/#{share_name}"
        now_ms = System.system_time(:millisecond)
        host_ip = List.first(node_ips(static_payload(opts))) || ""

        cql =
          "INSERT INTO hydra.files_shares " <>
            "(share_name, vdisk_id, container, size_gb, protocol, export_path, allowed_clients, host_ip, vip, state, squash_mode, smb_guest_ok, created_at_ms, updated_at_ms) " <>
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        params = [
          share_name,
          vdisk_id,
          container,
          size_gb,
          protocol,
          export_path,
          allowed_clients,
          host_ip,
          "",
          "EXPORTED",
          squash_mode,
          smb_guest_ok,
          now_ms,
          now_ms
        ]

        case write_files(cql, params, opts) do
          :ok ->
            if host_ip != "" and Keyword.get(opts, :source, files_source()) == :hydra do
              try do
                SpectrumPhx.SparkClient.request(host_ip, :post, "/api/v1/host/files", %{
                  "action" => "activate",
                  "share" => %{
                    "share_name" => share_name,
                    "vdisk_id" => vdisk_id,
                    "protocol" => protocol,
                    "export_path" => export_path,
                    "allowed_clients" => allowed_clients,
                    "squash_mode" => squash_mode,
                    "smb_guest_ok" => smb_guest_ok
                  }
                })
              rescue
                _ -> :ok
              catch
                :exit, _ -> :ok
              end
            end

            {:ok, share_name}

          {:error, reason} ->
            {:error, "Could not create file share: #{describe(reason)}"}
        end
    end
  end

  @doc "Delete a Helios Files share from `hydra.files_shares`."
  def delete_file_share(share_name, opts \\ []) do
    name = string(share_name)

    if is_nil(name) or not Regex.match?(@share_name_re, name) do
      {:error, "Invalid file share name."}
    else
      case write_files("DELETE FROM hydra.files_shares WHERE share_name = ?", [name], opts) do
        :ok -> :ok
        {:error, reason} -> {:error, "Could not delete file share: #{describe(reason)}"}
      end
    end
  end

  defp normalise_share_row(row) when is_map(row) do
    name = string(row["share_name"] || row[:share_name]) || "unknown"

    %{
      share_name: name,
      vdisk_id: string(row["vdisk_id"] || row[:vdisk_id]) || "files-#{name}",
      container: string(row["container"] || row[:container]) || "default-pool",
      size_gb: integer(row["size_gb"] || row[:size_gb]) || 0,
      protocol: string(row["protocol"] || row[:protocol]) || "nfs",
      export_path: string(row["export_path"] || row[:export_path]) || "/srv/helios-files/#{name}",
      allowed_clients: string(row["allowed_clients"] || row[:allowed_clients]) || "*",
      host_ip: string(row["host_ip"] || row[:host_ip]),
      state: string(row["state"] || row[:state]) || "UNKNOWN",
      squash_mode: string(row["squash_mode"] || row[:squash_mode]) || "root_squash",
      smb_guest_ok?: (row["smb_guest_ok"] || row[:smb_guest_ok]) == true
    }
  end

  defp write_files(cql, params, opts) do
    case Keyword.get(opts, :source, files_source()) do
      {:static, _} ->
        :ok

      :hydra ->
        try do
          case SpectrumPhx.Hydra.query(cql, params) do
            {:ok, _} -> :ok
            {:error, reason} -> {:error, reason}
          end
        rescue
          exception -> {:error, Exception.message(exception)}
        catch
          :exit, reason -> {:error, reason}
        end
    end
  end
end
