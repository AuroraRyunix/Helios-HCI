defmodule SpectrumPhxWeb.Storage.IndexLive do
  @moduledoc """
  The storage fabric view: per-node extent stores, the vdisks served from them, and the
  disks underneath.

  Read-only. Nothing here provisions, resizes or deletes storage -- those paths run
  through Vali and Catalyst, and a view that can only observe cannot be the thing that
  breaks a vdisk.

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

  Render an unknown as a zero. A store whose filesystem reported no capacity gets no usage
  bar, a node that did not answer is named rather than dropped from the list, and a vdisk
  whose owner is on an unread node is `unknown` rather than `healthy`. The whole reason
  this view exists is that the previous one showed a degraded resource and a healthy one
  identically.

  ## Route

  Not wired here. `live "/storage", SpectrumPhxWeb.Storage.IndexLive, :index` belongs in
  the router, inside whatever `live_session` carries the authentication `on_mount` hook.
  This module assumes nothing about the session and adds no auth of its own.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Storage.Components
  import SpectrumPhxWeb.Cluster.Components, only: [panel: 1]

  alias SpectrumPhx.Storage
  alias SpectrumPhx.Storage.Containers

  @refresh_interval_ms 10_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Storage.subscribe()
      :timer.send_interval(@refresh_interval_ms, self(), :refresh)
    end

    socket =
      socket
      |> assign_snapshot(Storage.snapshot())
      |> assign(:container_form, default_container_form())
      |> load_containers()

    {:ok, socket}
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
    {:noreply, socket |> assign_snapshot(Storage.snapshot()) |> load_containers()}
  end

  def handle_event("container_form", params, socket) do
    {:noreply, assign(socket, :container_form, Map.merge(socket.assigns.container_form, params))}
  end

  def handle_event("create_container", params, socket) do
    # The form asks for gigabytes because that is what an operator thinks in; the column
    # is bytes. Converting here rather than in `Containers` keeps the unit a property of
    # this form instead of a second thing every caller has to know.
    case Containers.create(Map.put(params, "quota_bytes", gb_to_bytes(params["quota_gb"]))) do
      {:ok, container} ->
        {:noreply,
         socket
         |> put_flash(:info, "Container #{container.name} created.")
         |> assign(:container_form, default_container_form())
         |> load_containers()}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, to_message(message))}
    end
  end

  def handle_event("set_compression", %{"name" => name, "compression" => mode}, socket) do
    case Containers.update(name, %{"compression" => mode}) do
      {:ok, :updated} ->
        {:noreply,
         socket
         |> put_flash(
           :info,
           "#{name}: compression #{mode}. Applies to extents sealed from now on; " <>
             "existing data is not rewritten, and it takes effect when a vdisk is next attached."
         )
         |> load_containers()}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, to_message(message))}
    end
  end

  def handle_event("delete_container", %{"name" => name}, socket) do
    case Containers.delete(name) do
      {:ok, :deleted} ->
        {:noreply,
         socket |> put_flash(:info, "Container #{name} deleted.") |> load_containers()}

      {:error, {:in_use, users}} ->
        shown = users |> Enum.take(5) |> Enum.join(", ")
        more = if length(users) > 5, do: " and #{length(users) - 5} more", else: ""

        {:noreply,
         put_flash(
           socket,
           :error,
           "#{name} still holds #{length(users)} vdisk(s): #{shown}#{more}. " <>
             "Move or delete them first."
         )}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, to_message(message))}
    end
  end

  defp load_containers(socket) do
    case Containers.list() do
      {:ok, containers} ->
        socket |> assign(:containers, containers) |> assign(:containers_error, nil)

      {:error, reason} ->
        # An unreadable catalogue is not an empty one, and drawing "no containers" over a
        # database outage is the mistake this page exists to avoid making elsewhere.
        socket
        |> assign(:containers, [])
        |> assign(:containers_error, inspect(reason))
    end
  end

  defp default_container_form do
    %{"name" => "", "tier" => "SSD", "quota_gb" => "0", "ftt" => "0", "compression" => "none"}
  end

  # A blank field is zero, which is "unlimited". Anything that is not a number becomes -1
  # so `Containers.create/1` refuses it rather than this quietly deciding it meant nothing.
  defp gb_to_bytes(nil), do: 0
  defp gb_to_bytes(""), do: 0

  defp gb_to_bytes(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {n, _} when n >= 0 -> n * 1024 * 1024 * 1024
      _ -> -1
    end
  end

  defp gb_to_bytes(value) when is_integer(value) and value >= 0, do: value * 1024 * 1024 * 1024
  defp gb_to_bytes(_), do: -1

  defp to_message(message) when is_binary(message), do: message
  defp to_message(:not_found), do: "That container no longer exists."
  defp to_message(:invalid_name), do: "That is not a usable container name."
  defp to_message(other), do: inspect(other)

  defp assign_snapshot(socket, snapshot) do
    socket
    |> assign(:page_title, "Storage")
    |> assign(:snapshot, snapshot)
    |> assign(:summary, snapshot.summary)
    |> assign(:troubled, Enum.reject(snapshot.vdisks.entries, &(&1.health == :ok)))
    |> assign(:updated_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:storage}>
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

      <%!-- A peer link that is down is stated on its own, above everything else. The
      journal is write-all, so this is not "reduced redundancy": guests whose vdisks
      replicate to that node are taking EIO right now. --%>
      <div
        :if={@snapshot.peers.unreachable != []}
        class="alert alert-error items-start"
        id="peers-down"
      >
        <.icon name="hero-signal-slash" class="size-5 shrink-0" />
        <div class="min-w-0">
          <p class="font-semibold">
            {length(@snapshot.peers.unreachable)} replication link(s) are down
          </p>
          <p class="text-sm opacity-90">
            An append has to reach every replica before it is acknowledged, so a vdisk
            replicated onto an unreachable node is refusing writes -- not merely running
            with fewer copies.
          </p>
          <ul class="mt-1 space-y-0.5 text-sm font-mono">
            <li
              :for={link <- @snapshot.peers.unreachable}
              id={"peer-" <> slug(link.from <> "-" <> link.peer)}
            >
              {link.from} &rarr; {link.peer}
              <span :if={link.detail} class="opacity-70">({link.detail})</span>
            </li>
          </ul>
        </div>
      </div>

      <%!-- The whole point of the page: anything not positively healthy is stated first,
      above the tables, where it cannot be missed. --%>
      <div :if={@troubled != []} class="alert alert-error items-start" id="attention">
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <div class="min-w-0">
          <p class="font-semibold">
            {length(@troubled)} of {@summary.vdisks_total} vdisks are not healthy
          </p>
          <ul class="mt-1 space-y-1 text-sm">
            <li :for={vdisk <- @troubled} id={"attention-" <> slug(vdisk.id)}>
              <span class="font-mono font-semibold">{vdisk.id}</span>
              <span :if={vdisk.issues == []} class="opacity-90">
                state could not be established on every node.
              </span>
              <span :if={vdisk.issues != []} class="opacity-90">
                {Enum.join(vdisk.issues, "; ")}
              </span>
            </li>
          </ul>
        </div>
      </div>

      <.panel :if={@snapshot.configured?} id="fabric-summary">
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
          <div class="min-w-0">
            <p class="panel-title truncate">Vdisks</p>
            <p class="text-2xl font-semibold tabular-nums leading-tight mt-1" id="stat-vdisks">
              {@summary.vdisks_ok}/{@summary.vdisks_total}
            </p>
            <p class="text-xs opacity-55 truncate">owned and fully replicated</p>
          </div>

          <div class="min-w-0">
            <p class="panel-title truncate">Degraded</p>
            <p
              class={[
                "text-2xl font-semibold tabular-nums leading-tight mt-1",
                @summary.vdisks_degraded > 0 && "text-error"
              ]}
              id="stat-degraded"
            >
              {@summary.vdisks_degraded}
            </p>
            <p class="text-xs opacity-55 truncate">
              <span
                :if={@summary.vdisks_under_replicated > 0}
                id="stat-under-replicated"
                class="text-error font-semibold"
              >
                {@summary.vdisks_under_replicated} under-replicated
              </span>
              <span :if={@summary.vdisks_under_replicated == 0}>vdisks</span>
            </p>
          </div>

          <div class="min-w-0">
            <p class="panel-title truncate">Unknown</p>
            <p
              class={[
                "text-2xl font-semibold tabular-nums leading-tight mt-1",
                @summary.vdisks_unknown > 0 && "text-warning"
              ]}
              id="stat-unknown"
            >
              {@summary.vdisks_unknown}
            </p>
            <p class="text-xs opacity-55 truncate">state could not be read</p>
          </div>

          <div class="min-w-0">
            <p class="panel-title truncate">Extent stores</p>
            <p class="text-2xl font-semibold tabular-nums leading-tight mt-1" id="stat-stores">
              {@summary.stores_total}
            </p>
            <p class="text-xs opacity-55 truncate">
              <span :if={@summary.stores_full > 0} class="text-error font-semibold">
                {@summary.stores_full} full
              </span>
              <span
                :if={@summary.stores_full == 0 and @summary.stores_warn > 0}
                class="text-warning font-semibold"
              >
                {@summary.stores_warn} filling up
              </span>
              <span :if={@summary.stores_full == 0 and @summary.stores_warn == 0}>
                one per node
              </span>
            </p>
          </div>
        </div>

        <div class="mt-4 pt-4 border-t border-base-300" id="capacity">
          <p :if={not @snapshot.capacity.known?} class="text-sm text-warning" id="capacity-unknown">
            No node reported an extent store capacity, so the fabric's size is unknown.
            Nothing here is a claim that it is empty.
          </p>

          <div :if={@snapshot.capacity.known?} class="space-y-2">
            <.usage_bar
              used={@snapshot.capacity.raw_used_bytes}
              total={@snapshot.capacity.raw_total_bytes}
              percent={@snapshot.capacity.used_percent}
            />
            <p class="text-xs opacity-70">
              Raw across every node's extent store. With {copies(@snapshot.expected_replicas)} kept, usable is about
              <span class="font-semibold">{bytes(@snapshot.capacity.usable_total_bytes)}</span>
              of which <span class="font-semibold">{bytes(@snapshot.capacity.usable_used_bytes)}</span>
              is allocated.
            </p>
          </div>
        </div>
      </.panel>

      <div class="grid gap-4 xl:grid-cols-5">
        <.stores_section
          stores={@snapshot.stores}
          configured?={@snapshot.configured?}
          class="xl:col-span-2"
        />

        <.containers_section
          containers={@containers}
          error={@containers_error}
          form={@container_form}
          class="xl:col-span-3"
        />
      </div>

      <.vdisks_section vdisks={@snapshot.vdisks} configured?={@snapshot.configured?} />

      <.disks_section disks={@snapshot.disks} configured?={@snapshot.configured?} />

      <p :if={@snapshot.configured?} class="text-xs opacity-50">
        Live over the websocket; the server assembles one snapshot for every connected
        page rather than each browser polling on its own.
      </p>
    </Layouts.app>
    """
  end

  # -- sections -------------------------------------------------------------------------

  attr :containers, :list, required: true
  attr :error, :string, default: nil
  attr :form, :map, required: true
  attr :class, :any, default: nil

  defp containers_section(assigns) do
    ~H"""
    <.panel
      id="containers-section"
      class={@class}
      title="Containers"
      subtitle="Policy, not allocation: tier, quota, fault tolerance and compression that every vdisk in a container inherits."
    >
      <div :if={@error} class="alert alert-warning text-sm mb-3" id="containers-error">
        <.icon name="hero-exclamation-triangle" class="size-4" />
        <span>The container catalogue could not be read: {@error}</span>
      </div>

      <div class="overflow-x-auto">
        <table class="table table-sm" id="containers-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Tier</th>
              <th>Quota</th>
              <th>FTT</th>
              <th>Compression</th>
              <th class="text-right">Actions</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={container <- @containers} id={"container-" <> container.name}>
              <td class="font-medium">{container.name}</td>
              <td>{container.tier}</td>
              <td>{quota_label(container.quota_bytes)}</td>
              <td>{container.ftt}</td>
              <td>
                <span class={[
                  "badge badge-sm",
                  if(container.compression == "none", do: "badge-ghost", else: "badge-info")
                ]}>
                  {container.compression}
                </span>
              </td>
              <td class="text-right whitespace-nowrap">
                <button
                  class="btn btn-xs"
                  phx-click="set_compression"
                  phx-value-name={container.name}
                  phx-value-compression={if container.compression == "none", do: "lz4", else: "none"}
                >
                  {if container.compression == "none", do: "Compress", else: "Stop compressing"}
                </button>
                <button
                  class="btn btn-xs btn-error btn-outline"
                  phx-click="delete_container"
                  phx-value-name={container.name}
                  data-confirm={"Delete container " <> container.name <> "?"}
                >
                  Delete
                </button>
              </td>
            </tr>
            <tr :if={@containers == [] and is_nil(@error)}>
              <td colspan="6" class="text-sm opacity-60">No containers defined.</td>
            </tr>
          </tbody>
        </table>
      </div>

      <form
        id="create-container-form"
        phx-submit="create_container"
        phx-change="container_form"
        class="flex flex-wrap gap-2 items-end mt-4 pt-4 border-t border-base-300"
      >
        <label class="form-control">
          <span class="label-text text-xs">Name</span>
          <input
            type="text"
            name="name"
            value={@form["name"]}
            placeholder="templates"
            class="input input-sm input-bordered"
            required
          />
        </label>
        <label class="form-control">
          <span class="label-text text-xs">Tier</span>
          <select name="tier" class="select select-sm select-bordered">
            <option :for={tier <- Containers.tiers()} value={tier} selected={@form["tier"] == tier}>
              {tier}
            </option>
          </select>
        </label>
        <label class="form-control">
          <span class="label-text text-xs">Quota (GB, 0 = unlimited)</span>
          <input
            type="number"
            name="quota_gb"
            min="0"
            value={@form["quota_gb"]}
            class="input input-sm input-bordered w-32"
          />
        </label>
        <label class="form-control">
          <span class="label-text text-xs">FTT</span>
          <input
            type="number"
            name="ftt"
            min="0"
            value={@form["ftt"]}
            class="input input-sm input-bordered w-20"
          />
        </label>
        <label class="form-control">
          <span class="label-text text-xs">Compression</span>
          <select name="compression" class="select select-sm select-bordered">
            <option
              :for={mode <- Containers.compression_modes()}
              value={mode}
              selected={@form["compression"] == mode}
            >
              {mode}
            </option>
          </select>
        </label>
        <button type="submit" class="btn btn-sm btn-primary">Create container</button>
      </form>

      <p class="text-xs opacity-50 mt-3">
        Compression applies to extents sealed from then on. Existing data is never
        rewritten, and a change takes effect the next time a vdisk is attached — which is
        what makes it safe to change while guests are running.
      </p>
    </.panel>
    """
  end

  defp quota_label(0), do: "Unlimited"
  defp quota_label(nil), do: "Unlimited"

  defp quota_label(bytes) when is_integer(bytes) do
    Float.round(bytes / 1024 / 1024 / 1024, 1)
    |> :erlang.float_to_binary(decimals: 1)
    |> Kernel.<>(" GB")
  end

  attr :vdisks, :map, required: true
  attr :configured?, :boolean, required: true

  # One table, one body per vdisk. The body is what carries the vdisk's id, because a vdisk
  # can take a second row for what is wrong with it, and an issue has to stay inside the
  # thing it is an issue of.
  defp vdisks_section(assigns) do
    ~H"""
    <.panel
      id="vdisks-section"
      title="Vdisks"
      subtitle="Every disk being served, by the node that owns it"
    >
      <.unavailable
        :if={@configured? and @vdisks.state == :unavailable}
        id="vdisks-unavailable"
        title="No node answered with its vdisk list"
        error={format_unreachable(@vdisks.unreachable)}
      />

      <div
        :if={@vdisks.state == :partial}
        class="alert alert-warning alert-soft items-start mb-3"
        id="vdisks-partial"
      >
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <div>
          <p class="text-sm">
            Some nodes did not answer, so this list is incomplete: a vdisk owned on an
            unread node does not appear here at all.
          </p>
          <p class="text-xs opacity-70 mt-1 font-mono break-all">
            {format_unreachable(@vdisks.unreachable)}
          </p>
        </div>
      </div>

      <p
        :if={@vdisks.state != :unavailable and @vdisks.entries == []}
        class="text-sm opacity-70 italic"
        id="vdisks-empty"
      >
        Every node answered and none of them is serving a vdisk. There is nothing attached
        on this cluster yet.
      </p>

      <div :if={@vdisks.entries != []} class="overflow-x-auto">
        <table class="table table-sm" id="vdisks-table">
          <thead>
            <tr>
              <th>Vdisk</th>
              <th>Size</th>
              <th>Served by</th>
              <th>Replicas</th>
              <th>Health</th>
              <th></th>
            </tr>
          </thead>
          <tbody
            :for={vdisk <- @vdisks.entries}
            id={"vdisk-" <> slug(vdisk.id)}
            class={[
              vdisk.health == :degraded && "bg-error/5",
              vdisk.health == :unknown && "bg-warning/5"
            ]}
          >
            <tr>
              <td class="font-mono font-semibold whitespace-nowrap">
                {vdisk.id}
                <span :if={vdisk.sealed?} class="badge badge-sm badge-ghost gap-1 ml-1 font-sans">
                  <.icon name="hero-lock-closed" class="size-3" /> sealed
                </span>
              </td>
              <td class="tabular-nums whitespace-nowrap">{bytes(vdisk.size_bytes)}</td>
              <td>
                <div class="flex flex-wrap items-center gap-1.5">
                  <.role_badge
                    :for={attachment <- vdisk.attachments}
                    attachment={attachment}
                    current_epoch={vdisk.epoch}
                  />
                  <.epoch_badge epoch={vdisk.epoch} />
                </div>
                <p :if={vdisk.owner == nil} class="text-xs text-warning">{owner_summary(vdisk)}</p>
              </td>
              <td>
                <.replica_badge
                  have={if vdisk.owner_ip, do: vdisk.replica_count}
                  want={vdisk.expected_replicas}
                  asked={vdisk.requested_replicas}
                />
                <p :if={vdisk.replicas != []} class="text-xs opacity-60 font-mono mt-1">
                  {Enum.join(vdisk.replicas, ", ")}
                </p>
              </td>
              <td><.health_badge health={vdisk.health} /></td>
              <td class="text-right whitespace-nowrap">
                <.link
                  :if={!vdisk.sealed?}
                  navigate={~p"/storage/vdisks/#{vdisk.id}/snapshots"}
                  id={"snapshots-link-" <> slug(vdisk.id)}
                  class="btn btn-ghost btn-xs gap-1"
                >
                  <.icon name="hero-camera" class="size-3" /> snapshots
                </.link>
              </td>
            </tr>
            <tr :if={vdisk.issues != []}>
              <td colspan="6" class="pt-0 border-t-0">
                <ul class="text-sm text-error space-y-0.5">
                  <li :for={issue <- vdisk.issues}>&bull; {issue}</li>
                </ul>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </.panel>
    """
  end

  attr :stores, :map, required: true
  attr :configured?, :boolean, required: true
  attr :class, :any, default: nil

  defp stores_section(assigns) do
    ~H"""
    <.panel
      id="stores-section"
      class={@class}
      title="Extent stores"
      subtitle="One per node: where sealed extent groups and journals live"
    >
      <.unavailable
        :if={@configured? and @stores.state == :unavailable}
        id="stores-unavailable"
        title="No node reported its extent store"
        error={format_unreachable(@stores.unreachable)}
      />

      <div
        :if={@stores.state == :partial}
        class="alert alert-warning alert-soft items-start mb-3"
        id="stores-partial"
      >
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <div>
          <p class="text-sm">
            Some nodes did not answer, so the capacity below is a partial view of the
            cluster and not its total.
          </p>
          <p class="text-xs opacity-70 mt-1 font-mono break-all">
            {format_unreachable(@stores.unreachable)}
          </p>
        </div>
      </div>

      <div class="grid gap-3 sm:grid-cols-2 xl:grid-cols-1 2xl:grid-cols-2">
        <div
          :for={store <- @stores.entries}
          id={"store-" <> slug(store.ip)}
          class={[
            "rounded-lg border p-3 space-y-2",
            store.state == :ok && "border-base-300",
            store.state == :full && "border-error/60",
            store.state in [:warn, :unknown] && "border-warning/60"
          ]}
        >
          <div class="flex items-start justify-between gap-2">
            <div class="min-w-0">
              <p class="font-semibold truncate">{store.hostname}</p>
              <p class="text-xs opacity-60 font-mono truncate">
                {store.ip}<span :if={store.path}> &middot; {store.path}</span>
              </p>
            </div>
            <.store_state_badge state={store.state} />
          </div>

          <.usage_bar
            used={store.used_bytes}
            total={store.total_bytes}
            percent={store.used_percent}
          />

          <p class="text-xs opacity-70">
            {store.egroup_count || 0} extent group(s), {bytes(store.egroup_bytes)} sealed, {bytes(
              store.journal_bytes
            )} still in journals waiting to drain.
          </p>

          <ul :if={store.messages != []} class="text-xs text-error space-y-0.5">
            <li :for={message <- store.messages}>&bull; {message}</li>
          </ul>
        </div>
      </div>
    </.panel>
    """
  end

  attr :disks, :list, required: true
  attr :configured?, :boolean, required: true

  defp disks_section(assigns) do
    ~H"""
    <.panel
      id="disks-section"
      title="Physical disks"
      subtitle="Block devices on each node, as lsblk sees them"
    >
      <p :if={@configured? and @disks == []} class="text-sm opacity-70 italic" id="disks-empty">
        No nodes to inventory.
      </p>

      <div class="grid gap-4 xl:grid-cols-2 2xl:grid-cols-3">
        <div
          :for={node <- @disks}
          id={"disks-" <> slug(node.ip)}
          class={[
            "rounded-lg border p-3 space-y-2",
            if(node.state == :unavailable, do: "border-error/60", else: "border-base-300")
          ]}
        >
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

          <p :if={node.state == :ok and node.devices == []} class="text-sm opacity-70 italic">
            The node answered and reported no block devices.
          </p>

          <div :if={node.devices != []} class="overflow-x-auto">
            <table class="table table-zebra table-xs">
              <thead>
                <tr>
                  <th>Device</th>
                  <th>Type</th>
                  <th class="text-right">Size</th>
                  <th>Media</th>
                  <th>Mounted at</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={device <- node.devices} id={"disk-" <> slug(node.ip <> "-" <> device.name)}>
                  <td class="font-mono">
                    <span style={"padding-left: #{device.depth * 12}px"}>{device.name}</span>
                  </td>
                  <td>{device.type}</td>
                  <td class="tabular-nums text-right whitespace-nowrap">{bytes(device.size_bytes)}</td>
                  <td>{media(device)}</td>
                  <td class="font-mono truncate max-w-48">{device.mountpoint || "-"}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </.panel>
    """
  end

  # -- helpers --------------------------------------------------------------------------

  # "Owned by no node" is not a normal state and is not drawn as one: an attachment with
  # no owner means every node holding this vdisk is relaying to somewhere that did not
  # answer.
  defp owner_summary(%{owner: nil}), do: "no owner on any node that answered"

  defp copies(1), do: "1 copy"
  defp copies(count), do: Integer.to_string(count) <> " copies"

  defp format_unreachable([]), do: nil

  defp format_unreachable(nodes) do
    Enum.map_join(nodes, "; ", fn node -> node.hostname <> " (" <> node.error <> ")" end)
  end

  defp media(%{rotational?: true}), do: "HDD"
  defp media(%{rotational?: false}), do: "SSD"
  defp media(_device), do: "unknown"
end
