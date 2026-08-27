defmodule SpectrumPhxWeb.Sdn.IndexLive do
  @moduledoc """
  Urbosa's overlay network: the logical fabric, drawn, and the tunnels it rides on.

  Two clocks, as on the dashboard and for the same reason. The tree is one database read
  and refreshes with the page; the tunnel throughput walks every node and segment on the
  far side, so it has its own slower interval. A page that refreshed both together would
  make the cheap half as slow as the expensive one.

  Selecting a box in the topology filters the tables beneath it. That is the whole reason
  the diagram is worth drawing rather than tabulating: an operator looking at a segment
  wants its rules and its tunnels, not the cluster's.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [panel: 1, figure: 1, dom_slug: 1]
  import SpectrumPhxWeb.Sdn.Topology, only: [diagram: 1]

  alias SpectrumPhx.Sdn

  @fabric_interval_ms 15_000
  @tunnel_interval_ms 30_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(@fabric_interval_ms, self(), :refresh)
      :timer.send_interval(@tunnel_interval_ms, self(), :refresh_tunnels)
    end

    socket =
      socket
      |> assign(page_title: "SDN", selected: nil, tunnels: %{available?: true, error: nil, nodes: []})
      |> load_fabric()
      |> load_tunnels()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load_fabric(socket)}
  def handle_info(:refresh_tunnels, socket), do: {:noreply, load_tunnels(socket)}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket) do
    {:noreply, socket |> load_fabric() |> load_tunnels()}
  end

  def handle_event("select", %{"id" => id}, socket) do
    # Clicking the selected element clears it, so the operator is never stuck filtered.
    {:noreply, assign(socket, :selected, if(socket.assigns.selected == id, do: nil, else: id))}
  end

  def handle_event("clear", _params, socket), do: {:noreply, assign(socket, :selected, nil)}

  defp load_fabric(socket) do
    socket
    |> assign(:fabric, Sdn.fabric())
    |> assign(:read_at, DateTime.utc_now())
  end

  # Kept on failure rather than blanked: tunnels that cannot be read are not tunnels that
  # are not there.
  defp load_tunnels(socket) do
    case Sdn.tunnels() do
      %{available?: true} = tunnels -> assign(socket, :tunnels, tunnels)
      %{available?: false} = tunnels -> assign(socket, :tunnels, %{tunnels | nodes: socket.assigns.tunnels.nodes})
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_username={@current_username} active={:sdn}>
      <.header>
        SDN
        <:subtitle>
          <span class="flex flex-wrap items-center gap-2 mt-1">
            <span class="text-xs opacity-60">
              Urbosa overlay · read {Calendar.strftime(@read_at, "%H:%M:%S")} UTC
            </span>
            <span :if={orphan_count(@fabric) > 0} class="badge badge-sm badge-error gap-1">
              <.icon name="hero-exclamation-triangle" class="size-3" />
              {orphan_count(@fabric)} detached
            </span>
            <span :if={not @tunnels.available?} class="badge badge-sm badge-warning gap-1">
              <.icon name="hero-signal-slash" class="size-3" /> tunnel status unavailable
            </span>
          </span>
        </:subtitle>
        <:actions>
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </.button>
        </:actions>
      </.header>

      <div :if={not @fabric.available?} class="alert alert-error alert-soft" id="fabric-error">
        <.icon name="hero-exclamation-circle" class="size-5 shrink-0" />
        <span class="text-sm">
          The overlay tables could not be read, so nothing below is the fabric:
          <span class="font-mono">{@fabric.error}</span>
        </span>
      </div>

      <div class="flex flex-col gap-4">
        <.panel id="sdn-totals">
          <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
            <.figure id="total-t0" label="Tier-0" value={@fabric.summary.tier0} caption="uplink routers" tone={:primary} />
            <.figure id="total-t1" label="Tier-1" value={@fabric.summary.tier1} caption="tenant routers" />
            <.figure id="total-segments" label="Segments" value={@fabric.summary.segments} caption="overlay networks" />
            <.figure id="total-guests" label="Guests attached" value={@fabric.summary.guests_attached} caption="on a segment" />
            <.figure
              id="total-rules"
              label="Firewall rules"
              value={@fabric.summary.firewall_rules}
              caption="distributed"
            />
          </div>
        </.panel>

        <.panel
          id="sdn-topology-panel"
          title="Logical topology"
          subtitle="Tier-0 down to the guests on each segment"
        >
          <:actions>
            <button :if={@selected} phx-click="clear" class="btn btn-ghost btn-xs">
              Clear selection
            </button>
          </:actions>

          <p :if={@fabric.tier0 == [] and orphan_count(@fabric) == 0} class="text-sm opacity-55 italic">
            No overlay is configured. Nothing has been drawn because there is nothing to draw.
          </p>

          <.diagram
            :if={@fabric.tier0 != [] or orphan_count(@fabric) > 0}
            fabric={@fabric}
            selected={@selected}
          />

          <div class="flex flex-wrap gap-4 mt-3 text-xs opacity-60">
            <span class="flex items-center gap-1.5">
              <span class="inline-block size-3 rounded bg-primary/40"></span> Tier-0
            </span>
            <span class="flex items-center gap-1.5">
              <span class="inline-block size-3 rounded bg-secondary/40"></span> Tier-1
            </span>
            <span class="flex items-center gap-1.5">
              <span class="inline-block size-3 rounded bg-accent/30"></span> Segment
            </span>
            <span class="flex items-center gap-1.5">
              <span class="inline-block size-3 rounded bg-error/30"></span> Detached
            </span>
            <span class="flex items-center gap-1.5">
              <span class="status-dot text-success"></span> Running guest
            </span>
          </div>
        </.panel>

        <.panel id="sdn-segments" title="Segments" subtitle="Every overlay network and what is on it">
          <p :if={@fabric.segments == []} class="text-sm opacity-55 italic">No segments defined.</p>

          <div :if={@fabric.segments != []} class="overflow-x-auto">
            <table class="table table-sm">
              <thead>
                <tr>
                  <th>Segment</th>
                  <th>VNI</th>
                  <th>Subnet</th>
                  <th>Gateway</th>
                  <th>DHCP</th>
                  <th>Guests</th>
                  <th>Router</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  :for={segment <- @fabric.segments}
                  id={"segment-#{dom_slug(segment.id || segment.name)}"}
                  phx-click="select"
                  phx-value-id={segment.id}
                  class={["cursor-pointer", @selected == segment.id && "bg-primary/10"]}
                >
                  <td class="font-medium">{segment.name}</td>
                  <td class="font-mono tabular-nums">{segment.vni || "—"}</td>
                  <td class="font-mono">{segment.subnet_cidr || "—"}</td>
                  <td class="font-mono">{segment.gateway_ip || "—"}</td>
                  <td>
                    <span :if={segment.dhcp_enabled?} class="badge badge-xs badge-success">on</span>
                    <span :if={not segment.dhcp_enabled?} class="badge badge-xs badge-ghost">off</span>
                    <span :if={segment.dhcp_range} class="text-xs opacity-55 ml-1 font-mono">
                      {segment.dhcp_range}
                    </span>
                  </td>
                  <td class="tabular-nums">{length(segment.guests)}</td>
                  <td>
                    <span :if={attached?(@fabric, segment)} class="text-xs opacity-70">
                      {router_name(@fabric, segment)}
                    </span>
                    <span :if={not attached?(@fabric, segment)} class="badge badge-xs badge-error">
                      detached
                    </span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </.panel>

        <div class="grid gap-4 lg:grid-cols-2">
          <.panel id="sdn-firewall" title="Distributed firewall" subtitle="In priority order">
            <p :if={@fabric.firewall == []} class="text-sm opacity-55 italic">No rules defined.</p>

            <div :if={@fabric.firewall != []} class="overflow-x-auto">
              <table class="table table-xs">
                <thead>
                  <tr>
                    <th>#</th>
                    <th>Rule</th>
                    <th>Source</th>
                    <th>Destination</th>
                    <th>Service</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={rule <- @fabric.firewall}>
                    <td class="tabular-nums opacity-55">{rule.priority}</td>
                    <td>{rule.description}</td>
                    <td class="font-mono">{rule.source}</td>
                    <td class="font-mono">{rule.destination}</td>
                    <td class="font-mono">{service(rule)}</td>
                    <td>
                      <span class={["badge badge-xs", action_class(rule.action)]}>
                        {rule.action}
                      </span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </.panel>

          <.panel
            id="sdn-tunnels"
            title="Tunnels"
            subtitle="What the overlay is actually carried over"
          >
            <p :if={not @tunnels.available? and @tunnels.nodes == []} class="text-sm opacity-55 italic">
              Tunnel status could not be read: <span class="font-mono">{@tunnels.error}</span>
            </p>
            <p :if={@tunnels.available? and @tunnels.nodes == []} class="text-sm opacity-55 italic">
              No tunnel interfaces reported. With no segments there are none to build.
            </p>

            <div :for={node <- @tunnels.nodes} class="mb-3 last:mb-0" id={"tunnel-#{dom_slug(node.ip)}"}>
              <div class="flex items-baseline justify-between gap-2">
                <span class="text-sm font-medium truncate">{node.hostname}</span>
                <span class="text-xs font-mono opacity-55 tabular-nums">
                  ↓ {rate(node.rx_kbps)} ↑ {rate(node.tx_kbps)}
                </span>
              </div>
              <ul class="mt-1 flex flex-col gap-0.5">
                <li
                  :for={interface <- visible_interfaces(node, @selected, @fabric)}
                  class="flex items-baseline justify-between gap-2 text-xs"
                >
                  <span class="font-mono opacity-70 truncate">{interface.interface}</span>
                  <span class="opacity-50 truncate flex-1 px-2">{interface.segment}</span>
                  <span class="font-mono opacity-60 tabular-nums whitespace-nowrap">
                    ↓ {rate(interface.rx_kbps)} ↑ {rate(interface.tx_kbps)}
                  </span>
                </li>
              </ul>
            </div>
          </.panel>
        </div>
      </div>
    </Layouts.app>
    """
  end

  defp orphan_count(fabric),
    do: length(fabric.orphans.tier1) + length(fabric.orphans.segments)

  defp attached?(fabric, segment) do
    not Enum.any?(fabric.orphans.segments, fn orphan -> orphan.id == segment.id end)
  end

  defp router_name(fabric, segment) do
    fabric.tier0
    |> Enum.flat_map(& &1.tier1)
    |> Enum.find(fn router -> router.id == segment.t1_id end)
    |> case do
      nil -> "—"
      router -> router.name
    end
  end

  defp service(%{protocol: protocol, port: nil}), do: protocol
  defp service(%{protocol: protocol, port: port}), do: "#{protocol}/#{port}"

  defp action_class(:allow), do: "badge-success"
  defp action_class(:deny), do: "badge-error"
  defp action_class(_), do: "badge-warning"

  # Selecting a segment narrows the tunnel list to the interfaces carrying it. Selecting
  # anything else leaves it alone: a T0 has no interface of its own to show.
  defp visible_interfaces(node, nil, _fabric), do: node.interfaces

  defp visible_interfaces(node, selected, fabric) do
    case Enum.find(fabric.segments, &(&1.id == selected)) do
      nil -> node.interfaces
      segment -> Enum.filter(node.interfaces, fn interface -> interface.vni == segment.vni end)
    end
  end

  defp rate(nil), do: "—"

  defp rate(kbps) when kbps >= 1000,
    do: :erlang.float_to_binary(kbps / 1000, decimals: 1) <> " Mb/s"

  defp rate(kbps), do: :erlang.float_to_binary(kbps * 1.0, decimals: 0) <> " kb/s"
end
