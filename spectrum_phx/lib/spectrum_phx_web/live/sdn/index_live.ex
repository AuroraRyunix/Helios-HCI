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

  def handle_event("create_t0", params, socket) do
    case Sdn.create_t0_router(params, sdn_opts()) do
      {:ok, _id} ->
        {:noreply,
         socket
         |> put_flash(:info, "Tier-0 router created.")
         |> load_fabric()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("delete_t0", %{"id" => id}, socket) do
    case Sdn.delete_t0_router(id, sdn_opts()) do
      :ok ->
        {:noreply,
         socket
         |> put_flash(:info, "Tier-0 router deleted.")
         |> load_fabric()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("create_t1", params, socket) do
    case Sdn.create_t1_router(params, sdn_opts()) do
      {:ok, _id} ->
        {:noreply,
         socket
         |> put_flash(:info, "Tier-1 router created.")
         |> load_fabric()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("delete_t1", %{"id" => id}, socket) do
    case Sdn.delete_t1_router(id, sdn_opts()) do
      :ok ->
        {:noreply,
         socket
         |> put_flash(:info, "Tier-1 router deleted.")
         |> load_fabric()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("create_segment", params, socket) do
    case Sdn.create_segment(params, sdn_opts()) do
      {:ok, _id} ->
        {:noreply,
         socket
         |> put_flash(:info, "Overlay segment created.")
         |> load_fabric()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("delete_segment", %{"id" => id}, socket) do
    case Sdn.delete_segment(id, sdn_opts()) do
      :ok ->
        {:noreply,
         socket
         |> put_flash(:info, "Overlay segment deleted.")
         |> load_fabric()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("create_firewall_rule", params, socket) do
    case Sdn.create_firewall_rule(params, sdn_opts()) do
      {:ok, _id} ->
        {:noreply,
         socket
         |> put_flash(:info, "Firewall rule created.")
         |> load_fabric()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("delete_firewall_rule", %{"id" => id}, socket) do
    case Sdn.delete_firewall_rule(id, sdn_opts()) do
      :ok ->
        {:noreply,
         socket
         |> put_flash(:info, "Firewall rule deleted.")
         |> load_fabric()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("configure_bgp", params, socket) do
    case Sdn.configure_bgp(params, sdn_opts()) do
      :ok ->
        {:noreply,
         socket
         |> put_flash(:info, "BGP EVPN configuration saved and FRR reload triggered.")
         |> load_fabric()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("add_bgp_peer", params, socket) do
    case Sdn.add_bgp_peer(params, sdn_opts()) do
      {:ok, peer_ip} ->
        {:noreply,
         socket
         |> put_flash(:info, "BGP neighbor #{peer_ip} added.")
         |> load_fabric()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  def handle_event("delete_bgp_peer", %{"ip" => peer_ip}, socket) do
    case Sdn.delete_bgp_peer(peer_ip, sdn_opts()) do
      :ok ->
        {:noreply,
         socket
         |> put_flash(:info, "BGP neighbor #{peer_ip} removed.")
         |> load_fabric()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  defp sdn_opts do
    [source: Application.get_env(:spectrum_phx, :sdn_source, :live)]
  end

  defp load_fabric(socket) do
    {:ok, bgp} = Sdn.bgp_state(sdn_opts())

    socket
    |> assign(:fabric, Sdn.fabric(sdn_opts()))
    |> assign(:bgp, bgp)
    |> assign(:read_at, DateTime.utc_now())
  end

  # Kept on failure rather than blanked: tunnels that cannot be read are not tunnels that
  # are not there.
  defp load_tunnels(socket) do
    case Sdn.tunnels(sdn_opts()) do
      %{available?: true} = tunnels -> assign(socket, :tunnels, tunnels)
      %{available?: false} = tunnels -> assign(socket, :tunnels, %{tunnels | nodes: socket.assigns.tunnels.nodes})
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:sdn}>
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

        <div class="grid gap-4 lg:grid-cols-2" id="sdn-routers">
          <.panel id="sdn-t0-panel" title="Tier-0 Routers" subtitle="Physical uplink gateways">
            <form id="t0-form" phx-submit="create_t0" class="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-3">
              <input type="text" name="name" placeholder="Name (e.g. T0-Edge)" class="input input-xs input-bordered" required />
              <input type="text" name="uplink_interface" placeholder="Uplink (eth0)" class="input input-xs input-bordered font-mono" />
              <input type="text" name="uplink_ip" placeholder="Uplink IP/CIDR" class="input input-xs input-bordered font-mono" />
              <div class="flex gap-1">
                <input type="text" name="gateway_ip" placeholder="Gateway IP" class="input input-xs input-bordered font-mono flex-1" />
                <button type="submit" class="btn btn-xs btn-primary">Add T0</button>
              </div>
            </form>

            <p :if={@fabric.tier0 == []} class="text-xs opacity-55 italic">No Tier-0 routers defined.</p>
            <table :if={@fabric.tier0 != []} class="table table-xs">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Uplink</th>
                  <th>IP / Gateway</th>
                  <th>T1s</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                <tr :for={t0 <- @fabric.tier0} id={"t0-row-#{dom_slug(t0.id || t0.name)}"}>
                  <td class="font-medium">{t0.name}</td>
                  <td class="font-mono">{t0.uplink_interface || "—"}</td>
                  <td class="font-mono text-xs">{t0.uplink_ip || "—"} → {t0.gateway_ip || "—"}</td>
                  <td class="tabular-nums">{length(t0.tier1)}</td>
                  <td class="text-right">
                    <button
                      type="button"
                      phx-click="delete_t0"
                      phx-value-id={t0.id}
                      id={"delete-t0-#{dom_slug(t0.id || t0.name)}"}
                      class="btn btn-ghost btn-xs text-error"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              </tbody>
            </table>
          </.panel>

          <.panel id="sdn-t1-panel" title="Tier-1 Routers" subtitle="Tenant logical routers">
            <form id="t1-form" phx-submit="create_t1" class="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-3">
              <input type="text" name="name" placeholder="Name (e.g. T1-Prod)" class="input input-xs input-bordered" required />
              <select name="t0_link_id" class="select select-xs select-bordered">
                <option value="">No T0 uplink (isolated)</option>
                <option :for={t0 <- @fabric.tier0} value={t0.id}>{t0.name}</option>
              </select>
              <label class="label cursor-pointer justify-start gap-2 text-xs">
                <input type="checkbox" name="dhcp_enabled" value="true" class="checkbox checkbox-xs" />
                <span>DHCP Relay</span>
              </label>
              <button type="submit" class="btn btn-xs btn-primary">Add T1</button>
            </form>

            <p :if={all_t1_routers(@fabric) == []} class="text-xs opacity-55 italic">No Tier-1 routers defined.</p>
            <table :if={all_t1_routers(@fabric) != []} class="table table-xs">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Tier-0 Uplink</th>
                  <th>DHCP</th>
                  <th>Segments</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                <tr :for={t1 <- all_t1_routers(@fabric)} id={"t1-row-#{dom_slug(t1.id || t1.name)}"}>
                  <td class="font-medium">{t1.name}</td>
                  <td class="font-mono text-xs">{t1.t0_id || "detached"}</td>
                  <td>
                    <span :if={t1.dhcp_enabled?} class="badge badge-xs badge-success">on</span>
                    <span :if={not t1.dhcp_enabled?} class="badge badge-xs badge-ghost">off</span>
                  </td>
                  <td class="tabular-nums">{length(t1.segments)}</td>
                  <td class="text-right">
                    <button
                      type="button"
                      phx-click="delete_t1"
                      phx-value-id={t1.id}
                      id={"delete-t1-#{dom_slug(t1.id || t1.name)}"}
                      class="btn btn-ghost btn-xs text-error"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              </tbody>
            </table>
          </.panel>
        </div>

        <.panel id="sdn-segments" title="Segments" subtitle="Every overlay network and what is on it">
          <form id="segment-form" phx-submit="create_segment" class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-2 mb-3">
            <input type="text" name="name" placeholder="Segment name" class="input input-xs input-bordered" required />
            <input type="number" name="vni" placeholder="VNI (e.g. 5001)" min="1" max="16777215" class="input input-xs input-bordered font-mono" required />
            <input type="text" name="subnet_cidr" placeholder="CIDR (10.10.1.0/24)" class="input input-xs input-bordered font-mono" required />
            <input type="text" name="gateway_ip" placeholder="Gateway IP" class="input input-xs input-bordered font-mono" />
            <select name="t1_link_id" class="select select-xs select-bordered">
              <option value="">No Tier-1 router</option>
              <option :for={t1 <- all_t1_routers(@fabric)} value={t1.id}>{t1.name}</option>
            </select>
            <input type="text" name="dhcp_start" placeholder="DHCP start" class="input input-xs input-bordered font-mono" />
            <input type="text" name="dhcp_end" placeholder="DHCP end" class="input input-xs input-bordered font-mono" />
            <div class="flex items-center gap-2">
              <label class="label cursor-pointer gap-1 text-xs">
                <input type="checkbox" name="dhcp_enabled" value="true" class="checkbox checkbox-xs" />
                <span>DHCP</span>
              </label>
              <button type="submit" class="btn btn-xs btn-primary flex-1">Add Segment</button>
            </div>
          </form>

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
                  <th></th>
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
                  <td class="text-right">
                    <button
                      type="button"
                      phx-click="delete_segment"
                      phx-value-id={segment.id}
                      id={"delete-segment-#{dom_slug(segment.id || segment.name)}"}
                      disabled={segment.guests != []}
                      title={if segment.guests != [], do: "Detach guests before deleting segment", else: "Delete segment"}
                      class="btn btn-ghost btn-xs text-error"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </.panel>

        <div class="grid gap-4 lg:grid-cols-2">
          <.panel id="sdn-firewall" title="Distributed firewall" subtitle="In priority order">
            <form id="firewall-form" phx-submit="create_firewall_rule" class="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-3">
              <input type="text" name="description" placeholder="Rule description" class="input input-xs input-bordered col-span-2" required />
              <input type="text" name="source_ip" placeholder="Source (any / CIDR)" class="input input-xs input-bordered font-mono" />
              <input type="text" name="dest_ip" placeholder="Dest (any / CIDR)" class="input input-xs input-bordered font-mono" />
              <select name="protocol" class="select select-xs select-bordered font-mono">
                <option value="any">any</option>
                <option value="tcp">tcp</option>
                <option value="udp">udp</option>
                <option value="icmp">icmp</option>
              </select>
              <input type="number" name="port" placeholder="Port (0=any)" class="input input-xs input-bordered font-mono" />
              <select name="action" class="select select-xs select-bordered">
                <option value="allow">allow</option>
                <option value="deny">deny</option>
              </select>
              <div class="flex gap-1">
                <input type="number" name="priority" placeholder="Prio (100)" value="100" class="input input-xs input-bordered font-mono w-20" />
                <button type="submit" class="btn btn-xs btn-primary flex-1">Add Rule</button>
              </div>
            </form>

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
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={rule <- @fabric.firewall} id={"fw-rule-#{dom_slug(rule.id || rule.description)}"}>
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
                    <td class="text-right">
                      <button
                        type="button"
                        phx-click="delete_firewall_rule"
                        phx-value-id={rule.id}
                        id={"delete-fw-#{dom_slug(rule.id || rule.description)}"}
                        class="btn btn-ghost btn-xs text-error"
                      >
                        Delete
                      </button>
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

        <.panel
          id="sdn-bgp-evpn"
          title="Scale-Out BGP EVPN Control Plane"
          subtitle="FRRouting EVPN Type-2/Type-3 route distribution, ARP suppression, and BGP neighbors"
        >
          <p :if={not @bgp.available?} class="text-sm text-warning mb-3" id="bgp-error">
            BGP EVPN configuration could not be read from ScyllaDB: <span class="font-mono">{@bgp.error}</span>
          </p>

          <div class="grid gap-4 lg:grid-cols-2">
            <div>
              <h4 class="text-xs font-semibold uppercase tracking-wide opacity-60 mb-2">
                Cluster BGP / EVPN Settings
              </h4>
              <form id="bgp-config-form" phx-submit="configure_bgp" class="grid grid-cols-2 sm:grid-cols-3 gap-2">
                <label class="form-control">
                  <span class="label-text text-xs">Local ASN</span>
                  <input
                    type="number"
                    name="asn"
                    value={@bgp.config.asn}
                    min="1"
                    max="4294967294"
                    class="input input-xs input-bordered font-mono"
                    required
                  />
                </label>
                <label class="form-control">
                  <span class="label-text text-xs">Router ID (optional)</span>
                  <input
                    type="text"
                    name="router_id"
                    value={@bgp.config.router_id}
                    placeholder="Auto (host IP)"
                    class="input input-xs input-bordered font-mono"
                  />
                </label>
                <label class="form-control">
                  <span class="label-text text-xs">Route Reflectors</span>
                  <input
                    type="text"
                    name="route_reflectors"
                    value={@bgp.config.route_reflectors}
                    placeholder="10.10.0.11,10.10.0.12"
                    class="input input-xs input-bordered font-mono"
                  />
                </label>
                <label class="label cursor-pointer justify-start gap-2 text-xs">
                  <input
                    type="checkbox"
                    name="evpn_enabled"
                    value="true"
                    checked={@bgp.config.evpn_enabled?}
                    class="checkbox checkbox-xs"
                  />
                  <span>Advertise all VNI (EVPN)</span>
                </label>
                <label class="label cursor-pointer justify-start gap-2 text-xs">
                  <input
                    type="checkbox"
                    name="arp_suppression"
                    value="true"
                    checked={@bgp.config.arp_suppression?}
                    class="checkbox checkbox-xs"
                  />
                  <span>Bridge ARP Suppression</span>
                </label>
                <label class="label cursor-pointer justify-start gap-2 text-xs">
                  <input
                    type="checkbox"
                    name="disable_headend_flood"
                    value="true"
                    checked={@bgp.config.disable_headend_flood?}
                    class="checkbox checkbox-xs"
                  />
                  <span>Disable Head-End Flood</span>
                </label>
                <label class="label cursor-pointer justify-start gap-2 text-xs">
                  <input
                    type="checkbox"
                    name="bfd_enabled"
                    value="true"
                    checked={@bgp.config.bfd_enabled?}
                    class="checkbox checkbox-xs"
                  />
                  <span>BFD Fast Failover</span>
                </label>
                <div class="col-span-2 flex items-center justify-end">
                  <button type="submit" class="btn btn-xs btn-primary">Save & Reload FRR</button>
                </div>
              </form>
            </div>

            <div>
              <h4 class="text-xs font-semibold uppercase tracking-wide opacity-60 mb-2">
                BGP EVPN Neighbors ({length(@bgp.peers)})
              </h4>
              <form id="bgp-peer-form" phx-submit="add_bgp_peer" class="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-3">
                <input
                  type="text"
                  name="peer_ip"
                  placeholder="Neighbor IPv4"
                  class="input input-xs input-bordered font-mono"
                  required
                />
                <input
                  type="number"
                  name="remote_asn"
                  placeholder="Remote ASN"
                  value={@bgp.config.asn}
                  min="1"
                  max="4294967294"
                  class="input input-xs input-bordered font-mono"
                  required
                />
                <input
                  type="text"
                  name="description"
                  placeholder="Description (e.g. ToR-1)"
                  class="input input-xs input-bordered"
                />
                <div class="flex items-center gap-2">
                  <label class="label cursor-pointer gap-1 text-xs" title="Route Reflector Client">
                    <input type="checkbox" name="route_reflector_client" value="true" class="checkbox checkbox-xs" />
                    <span>RR</span>
                  </label>
                  <button type="submit" class="btn btn-xs btn-primary flex-1">Add Peer</button>
                </div>
              </form>

              <p :if={@bgp.peers == []} class="text-xs opacity-55 italic">
                No explicit BGP neighbors configured.
              </p>

              <div :if={@bgp.peers != []} class="overflow-x-auto">
                <table class="table table-xs" id="bgp-peers-table">
                  <thead>
                    <tr>
                      <th>Neighbor IP</th>
                      <th>Remote ASN</th>
                      <th>Description</th>
                      <th>Flags</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr :for={peer <- @bgp.peers} id={"bgp-peer-#{dom_slug(peer.peer_ip)}"}>
                      <td class="font-mono font-medium">{peer.peer_ip}</td>
                      <td class="font-mono tabular-nums">{peer.remote_asn}</td>
                      <td>{if peer.description != "", do: peer.description, else: "—"}</td>
                      <td>
                        <span :if={peer.bfd_enabled?} class="badge badge-xs badge-info mr-1">BFD</span>
                        <span :if={peer.route_reflector_client?} class="badge badge-xs badge-secondary">RR-Client</span>
                      </td>
                      <td class="text-right">
                        <button
                          type="button"
                          phx-click="delete_bgp_peer"
                          phx-value-ip={peer.peer_ip}
                          id={"delete-bgp-peer-#{dom_slug(peer.peer_ip)}"}
                          class="btn btn-ghost btn-xs text-error"
                        >
                          Remove
                        </button>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </.panel>
      </div>
    </Layouts.app>
    """
  end

  defp all_t1_routers(fabric) do
    Enum.flat_map(fabric.tier0, & &1.tier1) ++ fabric.orphans.tier1
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
