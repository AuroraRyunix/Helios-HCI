defmodule SpectrumPhxWeb.Networking.IndexLive do
  @moduledoc """
  Layer-2 networks and the adapters carrying them.

  The list spans both kinds a guest can be attached to -- Gatoway's `direct` and `vlan`
  networks, and Urbosa's overlay segments -- because from a guest's point of view "which
  network am I on" has one answer regardless of how it is built. They stay
  distinguishable by kind, since what can be done with them differs: a segment is the SDN
  page's to remove, and a VLAN cannot be edited into a VNI.

  The adapters come from the same host reads the hardware page uses, so the two pages
  cannot disagree about what is in the machine.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [panel: 1, figure: 1, dom_slug: 1]

  alias SpectrumPhx.Hardware
  alias SpectrumPhx.Networking

  @refresh_interval_ms 30_000
  @adapter_interval_ms 120_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(@refresh_interval_ms, self(), :refresh)
      :timer.send_interval(@adapter_interval_ms, self(), :refresh_adapters)
    end

    {:ok,
     socket
     |> assign(page_title: "Networking", form: blank_form(), adapters: nil)
     |> load()
     |> load_adapters()}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load(socket)}
  def handle_info(:refresh_adapters, socket), do: {:noreply, load_adapters(socket)}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket) do
    {:noreply, socket |> load() |> load_adapters()}
  end

  # Re-rendered as the operator types so the VLAN field appears with the type, rather
  # than the form being one submit behind what it is asking for.
  def handle_event("form_changed", params, socket) do
    {:noreply, assign(socket, :form, Map.take(params, ["name", "type", "vlan_id"]))}
  end

  def handle_event("create", params, socket) do
    case Networking.create_network(params) do
      {:ok, name} ->
        {:noreply,
         socket
         |> put_flash(:info, "Network #{name} created.")
         |> assign(:form, blank_form())
         |> load()}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event("delete", %{"id" => id}, socket) do
    case Networking.delete_network(id) do
      {:ok, _id} -> {:noreply, socket |> put_flash(:info, "Network removed.") |> load()}
      {:error, message} -> {:noreply, put_flash(socket, :error, message)}
    end
  end

  defp blank_form, do: %{"name" => "", "type" => "direct", "vlan_id" => ""}

  defp load(socket) do
    socket
    |> assign(:overview, Networking.overview())
    |> assign(:read_at, DateTime.utc_now())
  end

  # The adapters are four reads per node; they keep their own slower clock, and the last
  # good answer stays on screen while a refresh is in flight.
  defp load_adapters(socket) do
    assign(socket, :adapters, Hardware.inventory())
  rescue
    _ -> socket
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:networking}>
      <.header>
        Networking
        <:subtitle>
          <span class="text-xs opacity-60">
            read {Calendar.strftime(@read_at, "%H:%M:%S")} UTC
          </span>
        </:subtitle>
        <:actions>
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </.button>
        </:actions>
      </.header>

      <div :if={not @overview.available?} class="alert alert-error alert-soft" id="networks-error">
        <.icon name="hero-exclamation-circle" class="size-5 shrink-0" />
        <span class="text-sm">
          The network tables could not be read: <span class="font-mono">{@overview.error}</span>
        </span>
      </div>

      <div class="flex flex-col gap-4">
        <.panel id="network-totals">
          <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <.figure id="total-networks" label="Networks" value={@overview.summary.total} caption="a guest can join" tone={:primary} />
            <.figure id="total-direct" label="Direct" value={@overview.summary.direct} caption="untagged on the bridge" />
            <.figure id="total-vlan" label="VLAN" value={@overview.summary.vlan} caption="tagged" />
            <.figure id="total-overlay" label="Overlay" value={@overview.summary.overlay} caption="VXLAN segments" />
          </div>
        </.panel>

        <div class="grid gap-4 lg:grid-cols-3">
          <.panel
            id="network-create"
            title="New layer-2 network"
            subtitle="Direct joins the host bridge; VLAN tags onto it"
            class="lg:col-span-1"
          >
            <form phx-submit="create" phx-change="form_changed" class="flex flex-col gap-3">
              <label class="form-control">
                <span class="label-text text-xs opacity-70">Name</span>
                <input
                  type="text"
                  name="name"
                  value={@form["name"]}
                  class="input input-bordered input-sm w-full"
                  placeholder="production"
                  autocomplete="off"
                />
              </label>

              <label class="form-control">
                <span class="label-text text-xs opacity-70">Type</span>
                <select name="type" class="select select-bordered select-sm w-full">
                  <option value="direct" selected={@form["type"] == "direct"}>direct</option>
                  <option value="vlan" selected={@form["type"] == "vlan"}>vlan</option>
                </select>
              </label>

              <label :if={@form["type"] == "vlan"} class="form-control">
                <span class="label-text text-xs opacity-70">VLAN id</span>
                <input
                  type="number"
                  name="vlan_id"
                  value={@form["vlan_id"]}
                  min="1"
                  max="4094"
                  class="input input-bordered input-sm w-full"
                  placeholder="100"
                />
                <span :if={@overview.summary.vlans_used != []} class="text-[0.65rem] opacity-50 mt-1">
                  in use: {Enum.join(@overview.summary.vlans_used, ", ")}
                </span>
              </label>

              <button type="submit" class="btn btn-primary btn-sm" id="create-network">
                <.icon name="hero-plus" class="size-4" /> Create
              </button>
            </form>
          </.panel>

          <.panel
            id="network-list"
            title="Defined networks"
            subtitle="Every network a guest can be attached to"
            class="lg:col-span-2"
          >
            <p :if={@overview.networks == []} class="text-sm opacity-55 italic">
              No networks defined.
            </p>

            <div :if={@overview.networks != []} class="overflow-x-auto">
              <table class="table table-sm">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Kind</th>
                    <th>Tag</th>
                    <th>Subnet</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={network <- @overview.networks} id={"network-#{dom_slug(network.id || network.name)}"}>
                    <td class="font-medium">{network.name}</td>
                    <td>
                      <span class={["badge badge-xs", kind_class(network.kind)]}>{network.kind}</span>
                    </td>
                    <td class="font-mono tabular-nums">{tag(network)}</td>
                    <td class="font-mono">{network.subnet_cidr || "—"}</td>
                    <td class="text-right">
                      <button
                        :if={network.removable?}
                        phx-click="delete"
                        phx-value-id={network.id}
                        data-confirm={"Remove #{network.name}? Guests attached to it will lose their network."}
                        class="btn btn-ghost btn-xs text-error"
                      >
                        Remove
                      </button>
                      <span :if={not network.removable?} class="text-xs opacity-45">
                        managed on <.link navigate={~p"/sdn"} class="link">SDN</.link>
                      </span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </.panel>
        </div>

        <.panel
          id="physical-adapters"
          title="Physical adapters"
          subtitle="What the networks above are carried on"
        >
          <:actions>
            <.link navigate={~p"/hardware"} class="btn btn-ghost btn-xs">
              Hardware <.icon name="hero-arrow-right" class="size-3" />
            </.link>
          </:actions>

          <p :if={is_nil(@adapters)} class="text-sm opacity-55 italic">
            The hosts have not reported their adapters yet.
          </p>

          <div :if={@adapters} class="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            <div :for={node <- @adapters.nodes} id={"adapters-#{dom_slug(node.ip)}"}>
              <div class="flex items-baseline justify-between gap-2">
                <span class="text-sm font-medium truncate">{node.hostname}</span>
                <span :if={not node.reachable?} class="badge badge-xs badge-error">unreachable</span>
              </div>
              <ul :if={node.interfaces != []} class="mt-1 flex flex-col gap-1">
                <li :for={interface <- node.interfaces} class="text-xs flex flex-wrap items-baseline gap-2">
                  <span class={[
                    "status-dot shrink-0",
                    interface.state == "UP" && "text-success",
                    interface.state != "UP" && "text-base-content/30"
                  ]}>
                  </span>
                  <span class="font-mono font-semibold">{interface.name}</span>
                  <span class="font-mono opacity-65">{Enum.join(interface.addresses, ", ")}</span>
                </li>
              </ul>
              <p :if={node.interfaces == []} class="text-xs opacity-45 italic mt-1">
                No adapters reported.
              </p>
            </div>
          </div>
        </.panel>
      </div>
    </Layouts.app>
    """
  end

  defp kind_class(:direct), do: "badge-ghost"
  defp kind_class(:vlan), do: "badge-info"
  defp kind_class(:overlay), do: "badge-accent"

  defp tag(%{kind: :vlan, vlan_id: vlan}) when is_integer(vlan), do: "VLAN #{vlan}"
  defp tag(%{kind: :overlay, vni: vni}) when is_integer(vni), do: "VNI #{vni}"
  defp tag(_network), do: "—"
end
