defmodule SpectrumPhxWeb.Metrics.IndexLive do
  @moduledoc """
  Cluster telemetry at `/metrics`.

  Everything on this page comes from one server-side snapshot, taken at one instant and
  pushed to every connected socket. The page it replaces ran its own thirty-second timer
  in the browser, fetched `/api/cluster/metrics` -- a full scan of `hydra.logos_metrics`
  plus three more tables -- and redrew six canvases from the result. Two browsers watching
  the same cluster showed different numbers, and each of them was paying for a full table
  scan to render forty points per host.

  `SpectrumPhx.Metrics` reads one partition per node instead, and the interval here exists
  only until something watches the collectors and calls `SpectrumPhx.Metrics.broadcast/1`.
  Ten seconds, because `logos.py` samples every thirty: polling faster than the source
  changes buys nothing.

  Authentication is not this view's business. It reads no session and adds no checks, so
  it mounts unchanged under a `live_session` whose `on_mount` hook has already run.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [dom_slug: 1]
  import SpectrumPhxWeb.Metrics.Components

  alias SpectrumPhx.Metrics

  @refresh_interval_ms 10_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Metrics.subscribe()
      schedule_refresh()
    end

    {:ok, socket |> assign(:page_title, "Metrics") |> assign_snapshot(Metrics.fetch())}
  end

  @impl true
  def handle_info(:refresh, socket) do
    # The next tick is scheduled only once this read has returned. A fixed interval
    # would queue ticks faster than a slow or wedged database could serve them, and the
    # socket would fall further behind with every one; this degrades the refresh rate
    # instead, which is the failure mode an operator can live with.
    snapshot = Metrics.fetch()
    schedule_refresh()
    {:noreply, assign_snapshot(socket, snapshot)}
  end

  # Pushed by whichever process ends up watching the collectors, when one exists.
  def handle_info({:metrics, snapshot}, socket) do
    {:noreply, assign_snapshot(socket, snapshot)}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket) do
    {:noreply, assign_snapshot(socket, Metrics.fetch())}
  end

  defp schedule_refresh do
    Process.send_after(self(), :refresh, @refresh_interval_ms)
  end

  defp assign_snapshot(socket, snapshot) do
    socket
    |> assign(:snapshot, snapshot)
    |> assign(:summary, snapshot.summary)
    # Rates have no natural ceiling, so every node's sparkline is scaled to the same
    # cluster-wide peak. Scaling each panel to its own maximum would make an idle node's
    # noise look exactly like a saturated one's load.
    |> assign(:iops_peak, Metrics.peak(snapshot.nodes, :disk_iops))
    |> assign(:net_peak, Metrics.peak(snapshot.nodes, :net_rx_kbps))
    |> assign(:updated_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_username={@current_username} active={:metrics}>
      <.header>
        Metrics
        <:subtitle>
          <span class="flex flex-wrap items-center gap-2 mt-1">
            <span class="badge badge-sm badge-ghost gap-1" id="metrics-source">
              <.icon name="hero-circle-stack" class="size-3" /> hydra.logos_metrics
            </span>
            <span class="badge badge-sm badge-ghost gap-1" id="metrics-window">
              <.icon name="hero-chart-bar" class="size-3" /> last {@snapshot.window} samples
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

      <.no_cluster :if={not @snapshot.configured?} />

      <.db_unavailable :if={not @snapshot.available?} error={@snapshot.error} />

      <div
        :if={@snapshot.configured? and @snapshot.available?}
        class="stats stats-vertical sm:stats-horizontal w-full shadow"
      >
        <div class="stat">
          <div class="stat-title">Reporting</div>
          <div class="stat-value text-2xl" id="stat-reporting">
            {@summary.nodes_reporting}/{@summary.nodes_total}
          </div>
          <div class="stat-desc" id="stat-reporting-desc">
            <span :if={@summary.nodes_silent > 0} class="text-error font-semibold">
              {@summary.nodes_silent} silent
            </span>
            <span :if={@summary.nodes_silent == 0}>every node</span>
          </div>
        </div>

        <div class="stat">
          <div class="stat-title">CPU</div>
          <div class="stat-value text-2xl" id="stat-cpu">{percent(@summary.cpu_avg)}</div>
          <div class="stat-desc">peak {percent(@summary.cpu_max)}</div>
        </div>

        <div class="stat">
          <div class="stat-title">Memory</div>
          <div class="stat-value text-2xl" id="stat-mem">{percent(@summary.mem_avg)}</div>
          <div class="stat-desc">peak {percent(@summary.mem_max)}</div>
        </div>

        <div class="stat">
          <div class="stat-title">Capacity</div>
          <div class="stat-value text-2xl" id="stat-capacity">{@summary.cores_total || "-"}</div>
          <div class="stat-desc" id="stat-capacity-desc">
            cores &middot; {from_kb(@summary.mem_total_kb)} RAM
          </div>
        </div>
      </div>

      <div class="grid gap-4">
        <div
          :for={node <- @snapshot.nodes}
          id={"metrics-node-#{dom_slug(node.ip)}"}
          class={[
            "card card-border bg-base-100",
            not node.reporting? && "border-warning/50",
            node.state == :down && "border-error/50"
          ]}
        >
          <div class="card-body gap-3 p-4">
            <div class="flex flex-wrap items-center justify-between gap-2">
              <div class="min-w-0">
                <p class="font-semibold truncate">{node.hostname}</p>
                <p class="text-xs font-mono opacity-60">{node.ip}</p>
              </div>
              <div class="flex flex-wrap items-center gap-1.5">
                <.node_badges node={node} />
              </div>
            </div>

            <p
              :if={not node.reporting?}
              class="text-sm text-warning/90"
              id={"metrics-node-#{dom_slug(node.ip)}-silent"}
            >
              No samples in <code class="font-mono">logos_metrics</code>
              for this node. Either <code class="font-mono">logos</code>
              is not running on it or it cannot write to Hydra; either way its load is
              unknown, not zero.
            </p>

            <div :if={node.reporting?} class="flex flex-wrap gap-4">
              <div class="flex-1 min-w-40">
                <.gauge
                  label="CPU"
                  value={node.latest.cpu_pct}
                  id={"metrics-node-#{dom_slug(node.ip)}-cpu"}
                />
                <.sparkline samples={node.samples} field={:cpu_pct} max={100} class="text-primary" />
              </div>
              <div class="flex-1 min-w-40">
                <.gauge
                  label="Memory"
                  value={node.latest.mem_pct}
                  id={"metrics-node-#{dom_slug(node.ip)}-mem"}
                />
                <.sparkline
                  samples={node.samples}
                  field={:mem_pct}
                  max={100}
                  class="text-secondary"
                />
              </div>
            </div>

            <div :if={node.reporting?} class="flex flex-wrap gap-4">
              <div class="flex-1 min-w-40">
                <div class="flex items-baseline justify-between text-xs">
                  <span class="opacity-60">Disk I/O</span>
                  <span class="font-semibold tabular-nums" id={"metrics-node-#{dom_slug(node.ip)}-io"}>
                    {rate(node.latest.disk_iops, "IOPS")}
                  </span>
                </div>
                <.sparkline
                  samples={node.samples}
                  field={:disk_iops}
                  max={@iops_peak}
                  class="text-accent"
                />
                <p class="text-xs opacity-50">
                  {rate(node.latest.disk_bandwidth_kbps, "KB/s")} throughput
                </p>
              </div>
              <div class="flex-1 min-w-40">
                <div class="flex items-baseline justify-between text-xs">
                  <span class="opacity-60">Network in</span>
                  <span
                    class="font-semibold tabular-nums"
                    id={"metrics-node-#{dom_slug(node.ip)}-net"}
                  >
                    {rate(node.latest.net_rx_kbps, "KB/s")}
                  </span>
                </div>
                <.sparkline
                  samples={node.samples}
                  field={:net_rx_kbps}
                  max={@net_peak}
                  class="text-info"
                />
                <p class="text-xs opacity-50">
                  {rate(node.latest.net_tx_kbps, "KB/s")} out
                </p>
              </div>
            </div>

            <div
              :if={node.reporting?}
              class="flex flex-wrap gap-3 text-xs opacity-70 border-t border-base-300 pt-2"
            >
              <span :if={node.latest.cpu_cores}>{node.latest.cpu_cores} cores</span>
              <span :if={node.latest.mem_total_kb}>
                {from_kb(node.latest.mem_used_kb)} of {from_kb(node.latest.mem_total_kb)}
              </span>
              <span :if={node.disks}>{node.disks} disks</span>
              <span class={node.stale? && "text-warning font-semibold"}>
                sampled {humanize_age(node.age_seconds)} ago
              </span>
              <span>{length(node.samples)} samples</span>
            </div>
          </div>
        </div>
      </div>

      <p :if={@snapshot.configured?} class="text-xs opacity-50" id="metrics-disk-note">
        Disk figures are I/O rate. <code class="font-mono">logos_metrics</code>
        records <code class="font-mono">disk_iops</code>
        and <code class="font-mono">disk_bandwidth_kbps</code>
        and has no disk-usage column at all, so nothing on this page answers how full a
        disk is; that comes from the host inventory, not from telemetry.
      </p>
    </Layouts.app>
    """
  end
end
