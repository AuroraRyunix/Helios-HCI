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

  alias SpectrumPhx.Cluster.Status

  @refresh_interval_ms 5_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Status.subscribe()
      :timer.send_interval(@refresh_interval_ms, self(), :refresh)
    end

    {:ok, socket |> assign(:selected, nil) |> assign_snapshot(Status.fetch())}
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

  defp assign_snapshot(socket, snapshot) do
    socket
    |> assign(:page_title, "Hosts")
    |> assign(:snapshot, snapshot)
    |> assign(:summary, snapshot.summary)
    |> assign(:updated_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_username={@current_username} active={:hosts}>
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
          <.button navigate={~p"/"} id="overview-link">Overview</.button>
        </:actions>
      </.header>

      <.no_cluster :if={not @snapshot.configured?} snapshot={@snapshot} />

      <div class="grid gap-6">
        <section
          :for={host <- @snapshot.nodes}
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
                <h2 class="font-semibold text-base truncate">{host.hostname}</h2>
                <p class="text-xs font-mono opacity-60">{host.ip}</p>
              </div>
              <div class="flex flex-wrap items-center gap-1.5">
                <.node_badge node={host} />
                <.staleness node={host} />
              </div>
            </div>

            <dl class="grid grid-cols-2 sm:grid-cols-3 gap-x-4 gap-y-2 text-sm">
              <div>
                <dt class="text-xs uppercase tracking-wide opacity-60">ZK leadership</dt>
                <dd class={["font-medium", host.zk_leader? && "text-info"]}>
                  {if host.zk_leader?, do: "leader", else: "follower"}
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
