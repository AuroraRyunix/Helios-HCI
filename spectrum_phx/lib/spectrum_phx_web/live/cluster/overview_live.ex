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

  ## Two clocks, on purpose

  Liveness is what an operator is watching this page for, and it is cheap: one read of
  ZooKeeper's tree. Telemetry, storage and the guest list are each a fan-out across every
  node, and none of them changes meaningfully between two seconds. Refreshing all of it on
  the liveness clock would multiply the cluster's load by an idle browser tab.

  So liveness refreshes on `@status_interval_ms` and the rest on `@detail_interval_ms`.
  Manual refresh takes everything, because someone pressing it has a reason.

  ## Nothing here blanks on failure

  Every panel keeps its last good reading when its source goes away, and says the reading
  is stale rather than replacing it with zeroes. A cluster whose metrics table is
  unreachable is not a cluster at 0% CPU, and drawing it as one is worse than drawing
  nothing.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components
  import SpectrumPhxWeb.Storage.Components, only: [bytes: 1]

  alias SpectrumPhx.Cluster.Status
  alias SpectrumPhx.Metrics
  alias SpectrumPhx.Storage
  alias SpectrumPhx.Vms

  @status_interval_ms 5_000
  @detail_interval_ms 20_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Status.subscribe()
      :timer.send_interval(@status_interval_ms, self(), :refresh)
      :timer.send_interval(@detail_interval_ms, self(), :refresh_detail)
    end

    socket =
      socket
      |> assign(metrics: nil, storage: nil, guests: nil)
      |> assign_snapshot(Status.fetch())
      |> load_detail()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, assign_snapshot(socket, Status.fetch())}

  def handle_info(:refresh_detail, socket), do: {:noreply, load_detail(socket)}

  # Pushed by whichever process is watching ZooKeeper, when one exists.
  def handle_info({:cluster_status, snapshot}, socket) do
    {:noreply, assign_snapshot(socket, snapshot)}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket) do
    {:noreply, socket |> assign_snapshot(Status.fetch()) |> load_detail()}
  end

  defp assign_snapshot(socket, snapshot) do
    socket
    |> assign(:page_title, "Cluster")
    |> assign(:snapshot, snapshot)
    |> assign(:summary, snapshot.summary)
    |> assign(:updated_at, DateTime.utc_now())
  end

  # Each source is kept independently: one being unreachable must not blank the others.
  defp load_detail(socket) do
    socket
    |> assign_detail(:metrics, fn -> Metrics.fetch() end)
    |> assign_detail(:storage, fn -> Storage.snapshot() end)
    |> assign_detail(:guests, fn ->
      case Vms.list_vms() do
        {:ok, vms} -> vms
        {:error, _reason} -> nil
      end
    end)
    |> assign_series()
  end

  defp assign_detail(socket, key, fetch) do
    case safely(fetch) do
      nil -> socket
      value -> assign(socket, key, value)
    end
  end

  # A source that raises is a source that is missing, not a page that falls over.
  defp safely(fetch) do
    fetch.()
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  # Charts are derived once here rather than in the template, so a re-render that changed
  # nothing does not re-walk every sample of every node.
  defp assign_series(socket) do
    nodes = (socket.assigns.metrics && socket.assigns.metrics.nodes) || []

    assign(socket, :series, %{
      cpu: series(nodes, :cpu_pct, :percent),
      mem: series(nodes, :mem_pct, :percent),
      iops: series(nodes, :disk_iops, :peak),
      net: series(nodes, :net_rx_kbps, :peak)
    })
  end

  defp series(nodes, field, scale) do
    points = Metrics.cluster_series(nodes, field)
    ceiling = Metrics.series_ceiling(points, scale)

    %{
      points: Metrics.spark_points(points, :value, ceiling),
      latest: points |> List.last() |> then(&(&1 && &1.value)),
      ceiling: ceiling
    }
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

      <div :if={@snapshot.configured?} class="flex flex-col gap-4">
        <.panel id="headline">
          <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
            <.figure
              id="figure-nodes"
              label="Nodes"
              value={"#{@summary.nodes_up}/#{@summary.total_nodes}"}
              caption={if @summary.nodes_down > 0, do: "#{@summary.nodes_down} down", else: "all registered"}
              tone={if @summary.nodes_down > 0, do: :bad, else: :good}
            />
            <.figure
              id="figure-services-up"
              label="Services up"
              value={@summary.services_up}
              caption="across reporting nodes"
              tone={:good}
            />
            <.figure
              id="figure-services-down"
              label="Services down"
              value={@summary.services_down}
              caption="not running"
              tone={if @summary.services_down > 0, do: :bad, else: :neutral}
            />
            <.figure
              id="figure-flapping"
              label="Flapping"
              value={@summary.services_flapping}
              caption="restarting repeatedly"
              tone={if @summary.services_flapping > 0, do: :warn, else: :neutral}
            />
            <.figure
              id="figure-guests"
              label="Guests"
              value={guest_count(@guests, :running)}
              caption={guest_caption(@guests)}
              tone={:primary}
            />
          </div>
        </.panel>

        <div class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <.chart_panel
            id="chart-cpu"
            label="CPU"
            value={percent_reading(@series.cpu.latest)}
            ceiling="100%"
            points={@series.cpu.points}
            tone="text-primary"
          />
          <.chart_panel
            id="chart-memory"
            label="Memory"
            value={percent_reading(@series.mem.latest)}
            ceiling="100%"
            points={@series.mem.points}
            tone="text-secondary"
          />
          <.chart_panel
            id="chart-iops"
            label="Disk IOPS"
            value={rate_reading(@series.iops.latest, "")}
            ceiling={"peak #{round_reading(@series.iops.ceiling)}"}
            points={@series.iops.points}
            tone="text-accent"
          />
          <.chart_panel
            id="chart-network"
            label="Network in"
            value={rate_reading(@series.net.latest, " KB/s")}
            ceiling={"peak #{round_reading(@series.net.ceiling)}"}
            points={@series.net.points}
            tone="text-success"
          />
        </div>

        <div class="grid gap-4 lg:grid-cols-2">
          <.panel
            id="storage-summary"
            title="Storage fabric"
            subtitle={storage_subtitle(@storage)}
          >
            <:actions>
              <.link navigate={~p"/storage"} class="btn btn-ghost btn-xs">
                Details <.icon name="hero-arrow-right" class="size-3" />
              </.link>
            </:actions>

            <p :if={is_nil(@storage)} class="text-sm opacity-55 italic">
              Storage has not reported yet.
            </p>

            <div :if={@storage} class="flex flex-col gap-4">
              <div>
                <div class="flex items-baseline justify-between text-sm">
                  <span class="opacity-70">Usable used</span>
                  <span class="tabular-nums font-semibold">
                    {bytes(@storage.capacity.usable_used_bytes)}
                    <span class="opacity-50 font-normal">
                      of {bytes(@storage.capacity.usable_total_bytes)}
                    </span>
                  </span>
                </div>
                <progress
                  :if={is_number(@storage.capacity.used_percent)}
                  class={["progress w-full h-2 mt-2", fill_class(@storage.capacity.used_percent)]}
                  value={Float.round(@storage.capacity.used_percent * 1.0, 1)}
                  max="100"
                >
                </progress>
                <div
                  :if={not is_number(@storage.capacity.used_percent)}
                  class="h-2 mt-2 rounded bg-base-300"
                >
                </div>
              </div>

              <div class="grid grid-cols-3 gap-3">
                <.figure
                  id="figure-vdisks"
                  label="vDisks"
                  value={@storage.summary.vdisks_total}
                  caption="#{@storage.summary.vdisks_ok} healthy"
                />
                <.figure
                  id="figure-degraded"
                  label="Degraded"
                  value={@storage.summary.vdisks_degraded}
                  caption="under-replicated #{@storage.summary.vdisks_under_replicated}"
                  tone={if @storage.summary.vdisks_degraded > 0, do: :warn, else: :neutral}
                />
                <.figure
                  id="figure-peer-links"
                  label="Peer links down"
                  value={@storage.summary.peer_links_down}
                  caption="between extent stores"
                  tone={if @storage.summary.peer_links_down > 0, do: :bad, else: :neutral}
                />
              </div>
            </div>
          </.panel>

          <.panel id="capacity-summary" title="Compute" subtitle="Reported by the telemetry layer">
            <:actions>
              <.link navigate={~p"/metrics"} class="btn btn-ghost btn-xs">
                Details <.icon name="hero-arrow-right" class="size-3" />
              </.link>
            </:actions>

            <p :if={is_nil(@metrics)} class="text-sm opacity-55 italic">
              Telemetry has not reported yet.
            </p>

            <div :if={@metrics} class="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <.figure
                id="figure-cores"
                label="Cores"
                value={@metrics.summary.cores_total}
                caption="across reporting nodes"
              />
              <.figure
                id="figure-memory-total"
                label="Memory"
                value={memory_total(@metrics.summary.mem_total_kb)}
                caption="installed"
              />
              <.figure
                id="figure-cpu-peak"
                label="Busiest CPU"
                value={percent_reading(@metrics.summary.cpu_max)}
                caption="any one node"
                tone={load_tone(@metrics.summary.cpu_max)}
              />
              <.figure
                id="figure-silent"
                label="Not reporting"
                value={@metrics.summary.nodes_silent}
                caption={"#{@metrics.summary.nodes_stale} stale"}
                tone={if @metrics.summary.nodes_silent > 0, do: :warn, else: :neutral}
              />
            </div>
          </.panel>
        </div>

        <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <section
            :for={host <- @snapshot.nodes}
            id={"node-#{dom_slug(host.ip)}"}
            class={[
              "glass-card glass-card-hover p-4 flex flex-col gap-3",
              host.state == :down && "border-error/60 glow-error",
              host.state != :down && host.counts.flapping > 0 && "border-warning/60"
            ]}
          >
            <div class="flex flex-wrap items-start justify-between gap-2">
              <div class="min-w-0">
                <.link
                  navigate={~p"/hosts?node=#{host.ip}"}
                  class="font-semibold hover:underline truncate block"
                >
                  {host.hostname}
                </.link>
                <p class="text-xs font-mono opacity-55">{host.ip}</p>
              </div>
              <div class="flex flex-wrap items-center gap-1.5 justify-end">
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

            <.node_load :if={host.state == :up} metrics={@metrics} ip={host.ip} />

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
              class="flex flex-wrap gap-3 text-xs opacity-65 border-t border-base-300/60 pt-2 mt-auto"
            >
              <span>{host.counts.up} up</span>
              <span :if={host.counts.down > 0} class="text-error">{host.counts.down} down</span>
              <span :if={host.counts.flapping > 0} class="text-warning font-semibold">
                {host.counts.flapping} flapping
              </span>
              <span :if={host.disks}>{host.disks} disks</span>
              <span :if={host.build} class="font-mono truncate">build {host.build}</span>
            </div>
          </section>
        </div>
      </div>

      <p :if={@snapshot.configured?} class="text-xs opacity-45">
        Live over the websocket; no page refresh required. Liveness every {status_seconds()}s,
        telemetry and storage every {detail_seconds()}s.
      </p>
    </Layouts.app>
    """
  end

  # -- small presentation helpers -------------------------------------------------------

  # Functions, not `@attribute`: inside ~H, `@name` is `assigns.name`, so referring to the
  # module attribute there would render the interval as nil.
  defp status_seconds, do: div(@status_interval_ms, 1000)
  defp detail_seconds, do: div(@detail_interval_ms, 1000)


  attr :metrics, :any, required: true
  attr :ip, :string, required: true

  defp node_load(assigns) do
    node =
      assigns.metrics &&
        Enum.find(assigns.metrics.nodes, fn candidate -> candidate.ip == assigns.ip end)

    assigns = assign(assigns, :node, node)

    ~H"""
    <div :if={@node && @node.reporting?} class="grid grid-cols-2 gap-3">
      <div>
        <div class="flex items-baseline justify-between text-xs">
          <span class="opacity-55">CPU</span>
          <span class="tabular-nums">{percent_reading(@node.latest.cpu_pct)}</span>
        </div>
        <progress
          class={["progress w-full h-1.5 mt-1", fill_class(@node.latest.cpu_pct)]}
          value={progress_value(@node.latest.cpu_pct)}
          max="100"
        >
        </progress>
      </div>
      <div>
        <div class="flex items-baseline justify-between text-xs">
          <span class="opacity-55">Memory</span>
          <span class="tabular-nums">{percent_reading(@node.latest.mem_pct)}</span>
        </div>
        <progress
          class={["progress w-full h-1.5 mt-1", fill_class(@node.latest.mem_pct)]}
          value={progress_value(@node.latest.mem_pct)}
          max="100"
        >
        </progress>
      </div>
    </div>
    """
  end

  defp percent_reading(value) when is_number(value), do: "#{:erlang.float_to_binary(value * 1.0, decimals: 0)}%"
  defp percent_reading(_), do: nil

  defp round_reading(value) when is_number(value),
    do: :erlang.float_to_binary(value * 1.0, decimals: 0)

  defp round_reading(_), do: "—"

  defp rate_reading(value, suffix) when is_number(value), do: round_reading(value) <> suffix
  defp rate_reading(_, _), do: nil

  defp progress_value(value) when is_number(value), do: Float.round(value * 1.0, 1)
  defp progress_value(_), do: 0

  defp fill_class(value) when is_number(value) and value >= 90, do: "progress-error"
  defp fill_class(value) when is_number(value) and value >= 75, do: "progress-warning"
  defp fill_class(value) when is_number(value), do: "progress-primary"
  defp fill_class(_), do: "progress-primary"

  defp load_tone(value) when is_number(value) and value >= 90, do: :bad
  defp load_tone(value) when is_number(value) and value >= 75, do: :warn
  defp load_tone(_), do: :neutral

  defp memory_total(kb) when is_integer(kb) and kb > 0, do: bytes(kb * 1024)
  defp memory_total(_), do: nil

  defp guest_count(nil, _state), do: nil

  defp guest_count(vms, :running) do
    Enum.count(vms, fn vm -> running?(vm) end)
  end

  defp running?(vm) do
    state = Map.get(vm, :state) || Map.get(vm, :status)
    is_binary(state) and String.downcase(state) == "running"
  end

  defp guest_caption(nil), do: "not reported"
  defp guest_caption(vms), do: "of #{length(vms)} defined"

  defp storage_subtitle(nil), do: nil

  defp storage_subtitle(storage) do
    if storage.summary.attention?, do: "needs attention", else: "all stores healthy"
  end
end
