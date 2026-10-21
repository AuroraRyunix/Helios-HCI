defmodule SpectrumPhxWeb.Cluster.HostsLive do
  @moduledoc """
  Per-host detail: identity, ZooKeeper leadership, maintenance state, disk count, build
  version, publication staleness, and the full service table for every node.

  Like the overview, this is pushed over the websocket rather than polled from the
  browser. A `?node=<ip>` parameter scrolls-to/expands one host without a round trip
  through a REST endpoint.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Cluster.Status
  alias SpectrumPhx.Settings

  @refresh_interval_ms 5_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Status.subscribe()
      :timer.send_interval(@refresh_interval_ms, self(), :refresh)
    end

    {:ok,
     socket
     |> assign(:selected, nil)
     |> assign(:show_add_host, false)
     |> assign_snapshot(Status.fetch())}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    {:noreply, assign(socket, :selected, params["node"])}
  end

  @impl true
  def handle_info(:refresh, socket) do
    {:noreply, assign_snapshot(socket, Status.fetch())}
  end

  def handle_info({:cluster_status, snapshot}, socket) do
    {:noreply, assign_snapshot(socket, snapshot)}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket) do
    {:noreply, assign_snapshot(socket, Status.fetch())}
  end

  def handle_event("toggle_add_host", _params, socket) do
    {:noreply, assign(socket, :show_add_host, not socket.assigns.show_add_host)}
  end

  def handle_event("add_host", params, socket) do
    hostname = Map.get(params, "hostname", "")
    ip = Map.get(params, "ip", "")
    witness? = Map.get(params, "witness") in ["true", "on", "1", true]
    rf = Map.get(params, "redundancy_factor", "")

    case Settings.add_node(hostname, ip,
           witness: witness?,
           redundancy_factor: rf,
           source: settings_source()
         ) do
      {:ok, msg} ->
        {:noreply,
         socket
         |> assign(:show_add_host, false)
         |> assign_snapshot(Status.fetch())
         |> put_flash(:info, msg)}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("toggle_maintenance", %{"ip" => ip, "action" => action}, socket)
      when action in ["enter", "leave", "service"] do
    case Settings.node_maintenance(ip, action, source: settings_source()) do
      {:ok, msg} ->
        {:noreply,
         socket
         |> assign_snapshot(Status.fetch())
         |> put_flash(:info, msg)}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("clear_degraded", %{"ip" => ip}, socket) do
    case Settings.clear_node_degraded(ip, source: settings_source()) do
      {:ok, msg} ->
        {:noreply,
         socket
         |> assign_snapshot(Status.fetch())
         |> put_flash(:info, msg)}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("rescue_host", %{"ip" => ip}, socket) do
    case Settings.rescue_node(ip, source: settings_source()) do
      {:ok, msg} ->
        {:noreply,
         socket
         |> assign_snapshot(Status.fetch())
         |> put_flash(:info, msg)}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("balance_cluster", _params, socket) do
    case Settings.balance_cluster(source: settings_source()) do
      {:ok, msg} ->
        {:noreply, put_flash(socket, :info, msg)}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("zk_promote", %{"ip" => ip} = params, socket) do
    replacing = Map.get(params, "replacing", "")

    case Settings.zk_role_change(ip, "promote",
           replacing: replacing,
           source: settings_source()
         ) do
      {:ok, msg} ->
        {:noreply,
         socket
         |> assign_snapshot(Status.fetch())
         |> put_flash(:info, msg)}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  defp settings_source do
    Application.get_env(:spectrum_phx, :settings_source, :live)
  end

  defp assign_snapshot(socket, snapshot) do
    socket
    |> assign(:page_title, "Hosts")
    |> assign(:snapshot, snapshot)
    |> assign(:summary, snapshot.summary)
    |> assign(:updated_at, DateTime.utc_now())
  end

  defp rack_slot_label(ip, idx) do
    info = Config.host_info(ip)
    rack_num = div(idx, 4) + 1
    slot = Map.get(info, "node_position") || <<?A + rem(idx, 4)>>

    case Map.get(info, "rack") do
      "rack" <> num -> "Rack #{num} · Slot #{slot}"
      r when is_binary(r) and r != "" -> "#{r} · Slot #{slot}"
      _ -> "Rack #{rack_num} · Slot #{slot}"
    end
  end

  defp zk_role_label(host, idx) do
    voter_or_observer = if idx < 3, do: "voter", else: "observer"
    mode = if host.zk_leader?, do: "leader", else: "follower"
    "#{mode} (#{voter_or_observer})"
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:hosts}>
      <.header>
        Hosts
        <:subtitle>
          <span class="flex flex-wrap items-center gap-2 mt-1">
            <.source_note snapshot={@snapshot} />
            <.desired_badge desired={@summary.desired} />
            <span class="text-xs opacity-60" id="updated-at">
              updated {Calendar.strftime(@updated_at, "%H:%M:%S")} UTC
            </span>
          </span>
        </:subtitle>
        <:actions>
          <button
            type="button"
            phx-click="balance_cluster"
            class="btn btn-sm btn-outline gap-1"
            id="drs-balance-btn"
          >
            <.icon name="hero-scale" class="size-4" /> DRS Balance Cluster
          </button>
          <button
            type="button"
            phx-click="toggle_add_host"
            class="btn btn-sm btn-primary gap-1"
            id="open-add-host-btn"
          >
            <.icon name="hero-plus" class="size-4" /> Add Host
          </button>
          <.link navigate={~p"/topology"} class="btn btn-sm btn-ghost gap-1" id="topology-link">
            <.icon name="hero-server-stack" class="size-4" /> Topology
          </.link>
          <.button navigate={~p"/"} id="overview-link">Overview</.button>
        </:actions>
      </.header>

      <div :if={@show_add_host} class="card card-border bg-base-200/50 mb-6" id="add-host-panel">
        <div class="card-body p-4 gap-3">
          <div class="flex items-center justify-between">
            <div>
              <h3 class="font-semibold text-sm">Expand Cluster — Add Node</h3>
              <p class="text-xs opacity-70">
                Join a provisioned, mTLS-enrolled host (`provision.py --join` + `impa enroll --node &lt;ip&gt;`) into this cluster.
              </p>
            </div>
            <button type="button" phx-click="toggle_add_host" class="btn btn-xs btn-ghost">
              Close
            </button>
          </div>
          <form phx-submit="add_host" id="add-host-form" class="grid grid-cols-1 sm:grid-cols-5 gap-3 items-end">
            <div>
              <label class="label text-xs">Hostname</label>
              <input
                type="text"
                name="hostname"
                placeholder="helios04"
                required
                class="input input-sm input-bordered w-full font-mono"
              />
            </div>
            <div>
              <label class="label text-xs">Node IPv4</label>
              <input
                type="text"
                name="ip"
                placeholder="10.10.102.44"
                required
                class="input input-sm input-bordered w-full font-mono"
              />
            </div>
            <div>
              <label class="label text-xs">Redundancy Policy</label>
              <select name="redundancy_factor" class="select select-sm select-bordered w-full">
                <option value="">Keep current (RF={Config.redundancy_factor()})</option>
                <option value="1">RF=1 (2 copies, FTT=1)</option>
                <option value="2">RF=2 (3 copies, FTT=2)</option>
              </select>
            </div>
            <div>
              <label class="label cursor-pointer justify-start gap-2">
                <input type="checkbox" name="witness" value="true" class="checkbox checkbox-sm" />
                <span class="text-xs">Quorum Witness only</span>
              </label>
            </div>
            <div class="flex justify-end">
              <button type="submit" class="btn btn-sm btn-primary w-full sm:w-auto">
                Join Cluster
              </button>
            </div>
          </form>
        </div>
      </div>

      <.no_cluster :if={not @snapshot.configured?} snapshot={@snapshot} />

      <div class="grid gap-6">
        <section
          :for={{host, idx} <- Enum.with_index(@snapshot.nodes)}
          id={"host-#{dom_slug(host.ip)}"}
          class={[
            "card card-border bg-base-100",
            host.state == :down && "border-error/50",
            @selected == host.ip && "ring-2 ring-primary/40"
          ]}
        >
          <div class="card-body gap-4 p-4">
            <div class="flex flex-wrap items-start justify-between gap-2">
              <div class="min-w-0">
                <div class="flex items-center gap-2">
                  <h2 class="font-semibold text-base truncate">{host.hostname}</h2>
                  <span class="badge badge-xs badge-neutral font-mono">
                    {rack_slot_label(host.ip, idx)}
                  </span>
                </div>
                <p class="text-xs font-mono opacity-60">{host.ip}</p>
              </div>
              <div class="flex flex-wrap items-center gap-1.5">
                <.node_badge node={host} />
                <.staleness node={host} />
                <button
                  type="button"
                  phx-click="toggle_maintenance"
                  phx-value-ip={host.ip}
                  phx-value-action={if host.in_maintenance?, do: "leave", else: "enter"}
                  id={"maint-btn-#{dom_slug(host.ip)}"}
                  class="btn btn-xs btn-ghost"
                >
                  {if host.in_maintenance?, do: "Exit Maintenance", else: "Maintenance"}
                </button>
                <button
                  :if={not host.in_maintenance?}
                  type="button"
                  phx-click="toggle_maintenance"
                  phx-value-ip={host.ip}
                  phx-value-action="service"
                  id={"service-maint-btn-#{dom_slug(host.ip)}"}
                  class="btn btn-xs btn-ghost"
                  title="Enter maintenance mode without evacuating VMs (--preserve-vms)"
                >
                  Service Maintenance (Keep VMs)
                </button>
                <button
                  type="button"
                  phx-click="clear_degraded"
                  phx-value-ip={host.ip}
                  id={"clear-degraded-btn-#{dom_slug(host.ip)}"}
                  class="btn btn-xs btn-ghost text-warning"
                >
                  Clear Degraded
                </button>
                <button
                  type="button"
                  phx-click="rescue_host"
                  phx-value-ip={host.ip}
                  id={"rescue-host-btn-#{dom_slug(host.ip)}"}
                  class="btn btn-xs btn-ghost text-error"
                >
                  Rescue Host
                </button>
              </div>
            </div>

            <dl class="grid grid-cols-2 sm:grid-cols-3 gap-x-4 gap-y-2 text-sm">
              <div>
                <dt class="text-xs uppercase tracking-wide opacity-60">ZK leadership</dt>
                <dd class={["font-medium", host.zk_leader? && "text-info"]}>
                  {zk_role_label(host, idx)}
                </dd>
              </div>
              <div>
                <dt class="text-xs uppercase tracking-wide opacity-60">Maintenance</dt>
                <dd class={["font-medium", host.in_maintenance? && "text-warning"]}>
                  {host.maintenance || "unknown"}
                </dd>
              </div>
              <div>
                <dt class="text-xs uppercase tracking-wide opacity-60">Disks</dt>
                <dd class="font-medium tabular-nums">{host.disks || "-"}</dd>
              </div>
              <div>
                <dt class="text-xs uppercase tracking-wide opacity-60">Build</dt>
                <dd class="font-mono text-xs truncate">{host.build || "unknown"}</dd>
              </div>
              <div>
                <dt class="text-xs uppercase tracking-wide opacity-60">Published</dt>
                <dd class={["font-medium", host.stale? && "text-warning"]}>
                  <%= cond do %>
                    <% is_nil(host.age_seconds) -> %>
                      never
                    <% host.stale? -> %>
                      {host.age_seconds}s ago (stale)
                    <% true -> %>
                      {host.age_seconds}s ago
                  <% end %>
                </dd>
              </div>
              <div>
                <dt class="text-xs uppercase tracking-wide opacity-60">Services</dt>
                <dd class="font-medium flex flex-wrap gap-2 text-xs">
                  <span class="text-success">{host.counts.up} up</span>
                  <span class={host.counts.down > 0 && "text-error"}>
                    {host.counts.down} down
                  </span>
                  <span class={host.counts.flapping > 0 && "text-warning font-semibold"}>
                    {host.counts.flapping} flapping
                  </span>
                </dd>
              </div>
            </dl>

            <form
              :if={length(@snapshot.nodes) > 3 and idx >= 3}
              phx-submit="zk_promote"
              id={"zk-promote-form-#{dom_slug(host.ip)}"}
              class="flex flex-wrap items-center gap-2 text-xs bg-base-200/40 p-2 rounded"
            >
              <input type="hidden" name="ip" value={host.ip} />
              <span class="opacity-75">ZooKeeper observer — hand off vote from:</span>
              <select name="replacing" class="select select-xs select-bordered font-mono">
                <option value="">(Add voter without replacement)</option>
                <option :for={peer <- Enum.reject(@snapshot.nodes, &(&1.ip == host.ip))} value={peer.ip}>
                  {peer.hostname} ({peer.ip})
                </option>
              </select>
              <button type="submit" class="btn btn-xs btn-outline">
                Promote to Voter
              </button>
            </form>

            <div
              :if={host.state == :down}
              class="alert alert-error alert-soft text-sm"
              id={"host-#{dom_slug(host.ip)}-down"}
            >
              <.icon name="hero-signal-slash" class="size-5 shrink-0" />
              <span>
                No ephemeral znode for this node. It is configured in the cluster but
                ZooKeeper has no live session for it, so it is down -- not merely unpolled.
              </span>
            </div>

            <.service_table node={host} id={"services-#{dom_slug(host.ip)}"} />
          </div>
        </section>
      </div>
    </Layouts.app>
    """
  end
end
