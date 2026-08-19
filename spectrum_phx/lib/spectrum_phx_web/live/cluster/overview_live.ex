defmodule SpectrumPhxWeb.Cluster.OverviewLive do
  @moduledoc """
  Cluster dashboard.

  Updates are *pushed* over the LiveView websocket. The old frontend polled REST
  endpoints from the browser, which is the reason for this rewrite: every client
  re-derived the same cluster state on its own schedule, and a node that went away
  between polls looked healthy until the next one landed.

  Here the socket subscribes to `SpectrumPhx.Cluster.Status.topic/0`, so any process
  that learns of a change can push a snapshot to every connected dashboard at once.
  Until such a watcher exists, a server-side interval refreshes the snapshot and sends
  only the diff down the wire. Both are set up inside `connected?/1`, so the initial
  static render does no extra work and dead sockets schedule nothing.
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

    {:ok, assign_snapshot(socket, Status.fetch())}
  end

  @impl true
  def handle_info(:refresh, socket) do
    {:noreply, assign_snapshot(socket, Status.fetch())}
  end

  # Pushed by whichever process is watching ZooKeeper, when one exists.
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
    |> assign(:page_title, "Cluster")
    |> assign(:snapshot, snapshot)
    |> assign(:summary, snapshot.summary)
    |> assign(:updated_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_username={@current_username} active={:overview}>
      <.header>
        Cluster
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
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </.button>
        </:actions>
      </.header>

      <.no_cluster :if={not @snapshot.configured?} snapshot={@snapshot} />

      <div :if={@snapshot.source == :probe} class="alert alert-warning alert-soft" id="probe-notice">
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <span class="text-sm">
          ZooKeeper is unreachable, so liveness below is a probe of each host rather than
          a fact from the ensemble. A host that is merely unreachable will read as down.
        </span>
      </div>

      <div :if={@snapshot.configured?} class="stats stats-vertical sm:stats-horizontal w-full shadow">
        <div class="stat">
          <div class="stat-title">Nodes</div>
          <div class="stat-value text-2xl" id="stat-nodes">
            {@summary.nodes_up}/{@summary.total_nodes}
          </div>
          <div class="stat-desc">
            <span :if={@summary.nodes_down > 0} class="text-error font-semibold">
              {@summary.nodes_down} down
            </span>
            <span :if={@summary.nodes_down == 0}>all registered</span>
          </div>
        </div>

        <div class="stat">
          <div class="stat-title">Services up</div>
          <div class="stat-value text-2xl text-success" id="stat-services-up">
            {@summary.services_up}
          </div>
          <div class="stat-desc">across reporting nodes</div>
        </div>

        <div class="stat">
          <div class="stat-title">Down</div>
          <div
            class={[
              "stat-value text-2xl",
              @summary.services_down > 0 && "text-error"
            ]}
            id="stat-services-down"
          >
            {@summary.services_down}
          </div>
          <div class="stat-desc">services</div>
        </div>

        <div class="stat">
          <div class="stat-title">Flapping</div>
          <div
            class={[
              "stat-value text-2xl",
              @summary.services_flapping > 0 && "text-warning"
            ]}
            id="stat-services-flapping"
          >
            {@summary.services_flapping}
          </div>
          <div class="stat-desc">restarting repeatedly</div>
        </div>
      </div>

      <div class="grid gap-4 mt-2">
        <div
          :for={host <- @snapshot.nodes}
          id={"node-#{dom_slug(host.ip)}"}
          class={[
            "card card-border bg-base-100",
            host.state == :down && "border-error/50",
            host.counts.flapping > 0 && "border-warning/60"
          ]}
        >
          <div class="card-body gap-3 p-4">
            <div class="flex flex-wrap items-center justify-between gap-2">
              <div class="min-w-0">
                <.link
                  navigate={~p"/hosts?node=#{host.ip}"}
                  class="font-semibold hover:underline truncate block"
                >
                  {host.hostname}
                </.link>
                <p class="text-xs font-mono opacity-60">{host.ip}</p>
              </div>
              <div class="flex flex-wrap items-center gap-1.5">
                <.node_badge node={host} />
                <.staleness node={host} />
                <span :if={host.zk_leader?} class="badge badge-sm badge-info gap-1">
                  <.icon name="hero-star" class="size-3" /> leader
                </span>
                <span :if={host.in_maintenance?} class="badge badge-sm badge-warning gap-1">
                  <.icon name="hero-wrench-screwdriver" class="size-3" /> {host.maintenance}
                </span>
              </div>
            </div>

            <p :if={host.state == :down} class="text-sm text-error/90">
              Configured in the cluster but holding no ephemeral znode. ZooKeeper has not
              seen this node.
            </p>

            <div :if={host.state == :up} class="flex flex-wrap gap-1.5">
              <span
                :for={service <- host.services}
                class="tooltip"
                data-tip={"#{service.name}: #{service.status}"}
              >
                <.service_badge status={service.status} restarts={service.restarts} />
              </span>
            </div>

            <div
              :if={host.state == :up}
              class="flex flex-wrap gap-3 text-xs opacity-70 border-t border-base-300 pt-2"
            >
              <span>{host.counts.up} up</span>
              <span :if={host.counts.down > 0} class="text-error">{host.counts.down} down</span>
              <span :if={host.counts.flapping > 0} class="text-warning font-semibold">
                {host.counts.flapping} flapping
              </span>
              <span :if={host.disks}>{host.disks} disks</span>
              <span :if={host.build} class="font-mono truncate">build {host.build}</span>
            </div>
          </div>
        </div>
      </div>

      <p :if={@snapshot.configured?} class="text-xs opacity-50">
        Live over the websocket; no page refresh required.
      </p>
    </Layouts.app>
    """
  end
end
