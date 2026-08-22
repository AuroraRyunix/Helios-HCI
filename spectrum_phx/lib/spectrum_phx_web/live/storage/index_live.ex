defmodule SpectrumPhxWeb.Storage.IndexLive do
  @moduledoc """
  The storage fabric view: LINSTOR pools, DRBD resources and per-node disks.

  Read-only. Nothing here provisions, resizes or deletes storage -- those paths run
  through Vali and Catalyst, and a view that can only observe cannot be the thing that
  breaks a resource.

  ## Updates are pushed

  A snapshot is assembled on the server and sent down the websocket. Two mechanisms,
  following `Cluster.OverviewLive`:

    * PubSub on `SpectrumPhx.Storage.topic/0`, so whatever ends up watching the fabric
      can push to every connected page at once.
    * A server-side interval until such a watcher exists. It runs on the server, so one
      operator with the page open costs one fan-out, not one per browser tab -- which is
      what the old page's per-client polling of `/api/storage/*` cost.

  Both are gated on `connected?/1`.

  ## What this page refuses to do

  Render an unknown as a zero. A pool whose capacity LINSTOR would not report gets no
  usage bar, a node that did not answer is named rather than dropped from the list, and a
  resource on an unread node is `unknown` rather than `healthy`. The whole reason this
  view exists is that the previous one showed a degraded resource and a healthy one
  identically.

  ## Route

  Not wired here. `live "/storage", SpectrumPhxWeb.Storage.IndexLive, :index` belongs in
  the router, inside whatever `live_session` carries the authentication `on_mount` hook.
  This module assumes nothing about the session and adds no auth of its own.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Storage.Components

  alias SpectrumPhx.Storage

  @refresh_interval_ms 10_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Storage.subscribe()
      :timer.send_interval(@refresh_interval_ms, self(), :refresh)
    end

    {:ok, assign_snapshot(socket, Storage.snapshot())}
  end

  @impl true
  def handle_info(:refresh, socket) do
    {:noreply, assign_snapshot(socket, Storage.snapshot())}
  end

  # Pushed by whichever process is watching the fabric, when one exists.
  def handle_info({:storage_status, snapshot}, socket) do
    {:noreply, assign_snapshot(socket, snapshot)}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket) do
    {:noreply, assign_snapshot(socket, Storage.snapshot())}
  end

  defp assign_snapshot(socket, snapshot) do
    socket
    |> assign(:page_title, "Storage")
    |> assign(:snapshot, snapshot)
    |> assign(:summary, snapshot.summary)
    |> assign(:troubled, Enum.reject(snapshot.resources.entries, &(&1.health == :ok)))
    |> assign(:updated_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_username={@current_username} active={:storage}>
      <.header>
        Storage fabric
        <:subtitle>
          <span class="flex flex-wrap items-center gap-2 mt-1">
            <span
              class={[
                "badge badge-sm gap-1",
                if(@summary.attention?, do: "badge-error", else: "badge-success")
              ]}
              id="fabric-state"
            >
              <.icon
                name={
                  if @summary.attention?,
                    do: "hero-exclamation-triangle",
                    else: "hero-check-circle"
                }
                class="size-3"
              />
              {if @summary.attention?, do: "needs attention", else: "all healthy"}
            </span>
            <span class="text-xs opacity-60" id="updated-at">
              updated {Calendar.strftime(@updated_at, "%H:%M:%S")} UTC
            </span>
          </span>
        </:subtitle>
        <:actions>
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </.button>
        </:actions>
      </.header>

      <div :if={not @snapshot.configured?} class="alert alert-warning items-start" id="no-cluster">
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <div>
          <p class="font-semibold">No cluster configured</p>
          <p class="text-sm opacity-90">
            <code class="font-mono">/etc/hci/cluster.json</code>
            lists no hosts, so there is no fabric to inspect. This is not an empty cluster
            reporting itself healthy -- nothing was read at all.
          </p>
        </div>
      </div>

      <%!-- The whole point of the page: anything not positively healthy is stated first,
      above the tables, where it cannot be missed. --%>
      <div :if={@troubled != []} class="alert alert-error items-start" id="attention">
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <div class="min-w-0">
          <p class="font-semibold">
            {length(@troubled)} of {@summary.resources_total} resources are not healthy
          </p>
          <ul class="mt-1 space-y-1 text-sm">
            <li :for={resource <- @troubled} id={"attention-" <> slug(resource.name)}>
              <span class="font-mono font-semibold">{resource.name}</span>
              <span :if={resource.issues == []} class="opacity-90">
                state could not be established on every node.
              </span>
              <span :if={resource.issues != []} class="opacity-90">
                {Enum.join(resource.issues, "; ")}
              </span>
            </li>
          </ul>
        </div>
      </div>

      <div
        :if={@snapshot.configured?}
        class="stats stats-vertical sm:stats-horizontal w-full shadow"
      >
        <div class="stat">
          <div class="stat-title">Resources</div>
          <div class="stat-value text-2xl" id="stat-resources">
            {@summary.resources_ok}/{@summary.resources_total}
          </div>
          <div class="stat-desc">fully replicated and UpToDate</div>
        </div>

        <div class="stat">
          <div class="stat-title">Degraded</div>
          <div
            class={["stat-value text-2xl", @summary.resources_degraded > 0 && "text-error"]}
            id="stat-degraded"
          >
            {@summary.resources_degraded}
          </div>
          <div class="stat-desc">
            <span
              :if={@summary.resources_under_replicated > 0}
              id="stat-under-replicated"
              class="text-error font-semibold"
            >
              {@summary.resources_under_replicated} under-replicated
            </span>
            <span :if={@summary.resources_under_replicated == 0}>resources</span>
          </div>
        </div>

        <div class="stat">
          <div class="stat-title">Unknown</div>
          <div
            class={["stat-value text-2xl", @summary.resources_unknown > 0 && "text-warning"]}
            id="stat-unknown"
          >
            {@summary.resources_unknown}
          </div>
          <div class="stat-desc">state could not be read</div>
        </div>

        <div class="stat">
          <div class="stat-title">Pools</div>
          <div class="stat-value text-2xl" id="stat-pools">{@summary.pools_total}</div>
          <div class="stat-desc">
            <span :if={@summary.pools_error > 0} class="text-error font-semibold">
              {@summary.pools_error} reporting errors
            </span>
            <span :if={@summary.pools_error == 0}>LINSTOR storage pools</span>
          </div>
        </div>
      </div>

      <.capacity_card :if={@snapshot.configured?} capacity={@snapshot.capacity} snapshot={@snapshot} />

      <.resources_section resources={@snapshot.resources} configured?={@snapshot.configured?} />

      <.pools_section pools={@snapshot.pools} configured?={@snapshot.configured?} />

      <.disks_section disks={@snapshot.disks} configured?={@snapshot.configured?} />

      <p :if={@snapshot.configured?} class="text-xs opacity-50">
        Live over the websocket; the server assembles one snapshot for every connected
        page rather than each browser polling on its own.
      </p>
    </Layouts.app>
    """
  end

  # -- sections -------------------------------------------------------------------------

  attr :capacity, :map, required: true
  attr :snapshot, :map, required: true

  defp capacity_card(assigns) do
    ~H"""
    <div class="card card-border bg-base-100" id="capacity">
      <div class="card-body gap-3 p-4">
        <h2 class="font-semibold">Capacity</h2>

        <p :if={not @capacity.known?} class="text-sm text-warning" id="capacity-unknown">
          No backed storage pool reported a capacity, so the fabric's size is unknown.
          Nothing here is a claim that it is empty.
        </p>

        <div :if={@capacity.known?} class="space-y-2">
          <.usage_bar
            used={@capacity.raw_used_bytes}
            total={@capacity.raw_total_bytes}
            percent={@capacity.used_percent}
          />
          <p class="text-xs opacity-70">
            Raw across every backed pool. With {copies(@snapshot.expected_replicas)} kept,
            usable is about <span class="font-semibold">{bytes(@capacity.usable_total_bytes)}</span>
            of which <span class="font-semibold">{bytes(@capacity.usable_used_bytes)}</span>
            is allocated.
          </p>
        </div>
      </div>
    </div>
    """
  end

  attr :resources, :map, required: true
  attr :configured?, :boolean, required: true

  defp resources_section(assigns) do
    ~H"""
    <section class="space-y-3">
      <h2 class="font-semibold text-lg">DRBD resources</h2>

      <.unavailable
        :if={@configured? and @resources.state == :unavailable}
        id="resources-unavailable"
        title="DRBD state could not be read from any node"
        error={format_unreachable(@resources.unreachable)}
      />

      <div
        :if={@resources.state == :partial}
        class="alert alert-warning alert-soft items-start"
        id="resources-partial"
      >
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <div>
          <p class="text-sm">
            Some nodes did not answer, so this list is incomplete and replica counts are a
            floor rather than a fact.
          </p>
          <p class="text-xs opacity-70 mt-1 font-mono break-all">
            {format_unreachable(@resources.unreachable)}
          </p>
        </div>
      </div>

      <p
        :if={@resources.state != :unavailable and @resources.entries == []}
        class="text-sm opacity-70 italic"
        id="resources-empty"
      >
        Every node answered and none of them backs a DRBD resource. There is nothing
        replicated on this cluster yet.
      </p>

      <div
        :for={resource <- @resources.entries}
        id={"resource-" <> slug(resource.name)}
        class={[
          "card card-border bg-base-100",
          resource.health == :degraded && "border-error/60",
          resource.health == :unknown && "border-warning/60"
        ]}
      >
        <div class="card-body gap-3 p-4">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <div class="min-w-0">
              <p class="font-mono font-semibold truncate">{resource.name}</p>
              <p class="text-xs opacity-60">
                {bytes(resource.size_bytes)} &middot; {primary_summary(resource)}
              </p>
            </div>
            <div class="flex flex-wrap items-center gap-1.5">
              <.health_badge health={resource.health} />
              <span class={[
                "badge badge-sm gap-1 font-medium",
                replica_class(resource)
              ]}>
                <.icon name="hero-square-3-stack-3d" class="size-3" />
                {resource.replicas}/{resource.expected_replicas} replicas
              </span>
            </div>
          </div>

          <ul :if={resource.issues != []} class="text-sm text-error space-y-0.5">
            <li :for={issue <- resource.issues}>&bull; {issue}</li>
          </ul>

          <div class="space-y-2 border-t border-base-300 pt-2">
            <div :for={placement <- resource.placements} class="text-xs space-y-1">
              <div class="flex flex-wrap items-center gap-1.5">
                <span class="font-semibold">{placement.hostname}</span>
                <span class="badge badge-sm badge-ghost font-mono">{placement.role}</span>
                <span :if={placement.suspended?} class="badge badge-sm badge-error">
                  I/O suspended
                </span>
                <span :if={not placement.replica?} class="badge badge-sm badge-ghost">
                  diskless
                </span>
              </div>
              <div class="flex flex-wrap gap-1.5">
                <.disk_state_badge
                  :for={device <- placement.devices}
                  state={device.disk_state}
                  label={"vol #{device.volume}"}
                />
                <span
                  :for={device <- placement.devices}
                  :if={not device.quorum?}
                  class="badge badge-sm badge-error"
                >
                  no quorum
                </span>
              </div>
              <div :for={connection <- placement.connections} class="flex flex-wrap gap-1.5">
                <.connection_badge connection={connection} />
                <.replication_badge :for={peer <- connection.peer_devices} peer={peer} />
                <.disk_state_badge
                  :for={peer <- connection.peer_devices}
                  state={peer.peer_disk_state}
                  label="peer disk"
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
    """
  end

  attr :pools, :map, required: true
  attr :configured?, :boolean, required: true

  defp pools_section(assigns) do
    ~H"""
    <section class="space-y-3">
      <h2 class="font-semibold text-lg">Storage pools</h2>

      <.unavailable
        :if={@configured? and @pools.state == :unavailable}
        id="pools-unavailable"
        title="The LINSTOR controller did not answer"
        error={@pools.error}
      />

      <p
        :if={@pools.state == :ok and @pools.entries == []}
        class="text-sm opacity-70 italic"
        id="pools-empty"
      >
        LINSTOR answered and reported no storage pools.
      </p>

      <div
        :for={pool <- @pools.entries}
        id={"pool-" <> slug(pool.node <> "-" <> pool.name)}
        class={[
          "card card-border bg-base-100",
          pool.state == :error && "border-error/60",
          pool.state == :unknown && "border-warning/60"
        ]}
      >
        <div class="card-body gap-2 p-4">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <div class="min-w-0">
              <p class="font-semibold truncate">{pool.name}</p>
              <p class="text-xs opacity-60 font-mono truncate">
                {pool.node} &middot; {pool.provider}
                <span :if={pool.backing}>&middot; {pool.backing}</span>
              </p>
            </div>
            <.pool_state_badge state={pool.state} />
          </div>

          <.usage_bar
            :if={not pool.diskless?}
            used={pool.used_bytes}
            total={pool.total_bytes}
            percent={pool.used_percent}
          />

          <p :if={pool.diskless?} class="text-xs opacity-70">
            Diskless: an access point for resources stored elsewhere, with no capacity of
            its own.
          </p>

          <ul :if={pool.messages != []} class="text-xs text-error space-y-0.5">
            <li :for={message <- pool.messages}>&bull; {message}</li>
          </ul>
        </div>
      </div>
    </section>
    """
  end

  attr :disks, :list, required: true
  attr :configured?, :boolean, required: true

  defp disks_section(assigns) do
    ~H"""
    <section class="space-y-3">
      <h2 class="font-semibold text-lg">Disks by node</h2>

      <p :if={@configured? and @disks == []} class="text-sm opacity-70 italic" id="disks-empty">
        No nodes to inventory.
      </p>

      <div
        :for={node <- @disks}
        id={"disks-" <> slug(node.ip)}
        class={["card card-border bg-base-100", node.state == :unavailable && "border-error/60"]}
      >
        <div class="card-body gap-2 p-4">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <div class="min-w-0">
              <p class="font-semibold truncate">{node.hostname}</p>
              <p class="text-xs font-mono opacity-60">{node.ip}</p>
            </div>
            <span :if={node.state == :unavailable} class="badge badge-sm badge-error gap-1">
              <.icon name="hero-signal-slash" class="size-3" /> unreadable
            </span>
          </div>

          <p :if={node.state == :unavailable} class="text-sm text-error/90">
            This node did not return a disk inventory, so its disks are unknown -- not
            absent. <span class="font-mono text-xs opacity-70 break-all">{node.error}</span>
          </p>

          <p
            :if={node.state == :ok and node.devices == []}
            class="text-sm opacity-70 italic"
          >
            The node answered and reported no block devices.
          </p>

          <div :if={node.devices != []} class="overflow-x-auto">
            <table class="table table-zebra table-sm">
              <thead>
                <tr>
                  <th>Device</th>
                  <th>Type</th>
                  <th>Size</th>
                  <th>Media</th>
                  <th>Mounted at</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={device <- node.devices} id={"disk-" <> slug(node.ip <> "-" <> device.name)}>
                  <td class="font-mono text-xs">
                    <span style={"padding-left: #{device.depth * 12}px"}>{device.name}</span>
                  </td>
                  <td class="text-xs">{device.type}</td>
                  <td class="text-xs tabular-nums">{bytes(device.size_bytes)}</td>
                  <td class="text-xs">{media(device)}</td>
                  <td class="text-xs font-mono truncate">{device.mountpoint || "-"}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </section>
    """
  end

  # -- helpers --------------------------------------------------------------------------

  # "Primary on no node" is a real and normal state: a resource nothing has opened yet.
  defp primary_summary(%{primaries: []}), do: "Primary on no node"

  defp primary_summary(%{primaries: hosts}) do
    "Primary on " <> Enum.join(hosts, ", ")
  end

  defp copies(1), do: "1 copy"
  defp copies(count), do: Integer.to_string(count) <> " copies"

  defp format_unreachable([]), do: nil

  defp format_unreachable(nodes) do
    Enum.map_join(nodes, "; ", fn node -> node.hostname <> " (" <> node.error <> ")" end)
  end

  # `nil` means "we could not count", so it must not be coloured like a satisfied count.
  defp replica_class(%{under_replicated?: true}), do: "badge-error"
  defp replica_class(%{under_replicated?: nil}), do: "badge-warning"
  defp replica_class(_resource), do: "badge-ghost"

  defp media(%{rotational?: true}), do: "HDD"
  defp media(%{rotational?: false}), do: "SSD"
  defp media(_device), do: "unknown"
end
