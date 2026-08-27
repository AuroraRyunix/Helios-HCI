defmodule SpectrumPhxWeb.Hardware.IndexLive do
  @moduledoc """
  Physical inventory of the hypervisors.

  Deliberately slow to refresh. Hardware does not change between two page loads, and each
  refresh is four reads against every node; the page an operator opens to count what they
  have should not be the page that keeps the cluster busiest.

  The reload is manual and on a long interval, and the last good inventory stays on
  screen while a refresh is in flight rather than the page emptying and refilling.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [panel: 1, figure: 1, dom_slug: 1]
  import SpectrumPhxWeb.Storage.Components, only: [bytes: 1]

  alias SpectrumPhx.Hardware

  @refresh_interval_ms 120_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket), do: :timer.send_interval(@refresh_interval_ms, self(), :refresh)

    {:ok, socket |> assign(page_title: "Hardware", loading?: false) |> load()}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load(socket)}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, load(socket)}

  defp load(socket) do
    inventory = Hardware.inventory()

    socket
    |> assign(:inventory, inventory)
    |> assign(:read_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_username={@current_username} active={:hardware}>
      <.header>
        Hardware
        <:subtitle>
          <span class="flex flex-wrap items-center gap-2 mt-1">
            <span class="text-xs opacity-60">
              read from the hosts at {Calendar.strftime(@read_at, "%H:%M:%S")} UTC
            </span>
            <span
              :if={unreachable(@inventory.summary) > 0}
              class="badge badge-sm badge-warning gap-1"
            >
              <.icon name="hero-exclamation-triangle" class="size-3" />
              {unreachable(@inventory.summary)} unreachable
            </span>
          </span>
        </:subtitle>
        <:actions>
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </.button>
        </:actions>
      </.header>

      <div :if={not @inventory.configured?} class="alert alert-warning alert-soft">
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <span class="text-sm">
          No hosts are configured, so there is nothing to inventory.
        </span>
      </div>

      <div :if={@inventory.configured?} class="flex flex-col gap-4">
        <.panel id="hardware-totals">
          <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
            <.figure
              id="total-nodes"
              label="Hosts"
              value={"#{@inventory.summary.nodes_reachable}/#{@inventory.summary.nodes_total}"}
              caption="answering"
              tone={if unreachable(@inventory.summary) > 0, do: :warn, else: :good}
            />
            <.figure id="total-cores" label="Logical cores" value={@inventory.summary.cores} caption="online" />
            <.figure
              id="total-memory"
              label="Memory"
              value={@inventory.summary.memory_bytes && bytes(@inventory.summary.memory_bytes)}
              caption="installed"
            />
            <.figure id="total-disks" label="Disks" value={@inventory.summary.disks} caption="whole devices" />
            <.figure
              id="total-raw"
              label="Raw capacity"
              value={@inventory.summary.disk_bytes > 0 && bytes(@inventory.summary.disk_bytes)}
              caption="before replication"
              tone={:primary}
            />
          </div>
        </.panel>

        <section
          :for={node <- @inventory.nodes}
          id={"host-#{dom_slug(node.ip)}"}
          class={[
            "glass-card glass-card-hover p-4 sm:p-5 flex flex-col gap-4",
            not node.reachable? && "border-error/60"
          ]}
        >
          <div class="flex flex-wrap items-start justify-between gap-2">
            <div class="min-w-0">
              <h2 class="font-semibold truncate">{node.hostname}</h2>
              <p class="text-xs font-mono opacity-55">{node.ip}</p>
            </div>
            <span :if={not node.reachable?} class="badge badge-sm badge-error gap-1">
              <.icon name="hero-signal-slash" class="size-3" /> unreachable
            </span>
          </div>

          <p :if={not node.reachable?} class="text-sm text-error/90">
            None of this host's four hardware reads answered. It is listed so the count is
            honest, not because anything is known about it.
          </p>

          <div :if={node.reachable?} class="grid gap-4 lg:grid-cols-2">
            <div class="flex flex-col gap-3">
              <div>
                <p class="panel-title">Processor</p>
                <p class="text-sm mt-1 truncate" title={node.cpu.model}>
                  {node.cpu.model || "—"}
                </p>
                <p class="text-xs opacity-60 mt-0.5">
                  {cpu_shape(node.cpu)}
                </p>
                <p :if={node.cpu.load_average} class="text-xs opacity-55 mt-0.5 tabular-nums">
                  load {Enum.map_join(node.cpu.load_average, " ", &:erlang.float_to_binary(&1 * 1.0, decimals: 2))}
                </p>
              </div>

              <div>
                <div class="flex items-baseline justify-between text-xs">
                  <span class="panel-title">Memory</span>
                  <span :if={node.memory.total_bytes} class="tabular-nums opacity-70">
                    {bytes(node.memory.used_bytes)} of {bytes(node.memory.total_bytes)}
                  </span>
                  <span :if={is_nil(node.memory.total_bytes)} class="opacity-45 italic">
                    not reported
                  </span>
                </div>
                <progress
                  :if={is_number(node.memory.used_percent)}
                  class={["progress w-full h-2 mt-1.5", memory_class(node.memory.used_percent)]}
                  value={node.memory.used_percent}
                  max="100"
                >
                </progress>
              </div>

              <div :if={node.interfaces != []}>
                <p class="panel-title">Interfaces</p>
                <ul class="mt-1 flex flex-col gap-1">
                  <li :for={interface <- node.interfaces} class="text-xs flex flex-wrap gap-2">
                    <span class={[
                      "status-dot mt-1.5 shrink-0",
                      interface.state == "UP" && "text-success",
                      interface.state != "UP" && "text-base-content/30"
                    ]}>
                    </span>
                    <span class="font-mono font-semibold">{interface.name}</span>
                    <span class="font-mono opacity-70">{Enum.join(interface.addresses, ", ")}</span>
                    <span :if={interface.mac} class="font-mono opacity-40">{interface.mac}</span>
                  </li>
                </ul>
              </div>
            </div>

            <div>
              <p class="panel-title">Disks</p>
              <p :if={node.disks == []} class="text-sm opacity-50 italic mt-1">
                No block devices reported.
              </p>
              <div :if={node.disks != []} class="overflow-x-auto mt-1">
                <table class="table table-xs">
                  <thead>
                    <tr>
                      <th>Device</th>
                      <th>Size</th>
                      <th>Kind</th>
                      <th>Model</th>
                      <th>Mounted</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr :for={disk <- node.disks}>
                      <td class="font-mono">{disk.name}</td>
                      <td class="tabular-nums whitespace-nowrap">{bytes(disk.size_bytes)}</td>
                      <td>{kind(disk.rotational?)}</td>
                      <td class="truncate max-w-40" title={disk.model}>{disk.model || "—"}</td>
                      <td class="font-mono text-xs opacity-70">
                        {mounted(disk)}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div :if={node.errors != []} class="text-xs opacity-60 border-t border-base-300/60 pt-2">
            <span :for={error <- node.errors} class="mr-3">
              {error.read} unavailable: <span class="font-mono">{error.reason}</span>
            </span>
          </div>
        </section>
      </div>
    </Layouts.app>
    """
  end

  # Guarded rather than compared inline: the type checker cannot narrow these out of a
  # map, and warns that structs do not compare meaningfully.
  defp unreachable(%{nodes_total: total, nodes_reachable: reachable})
       when is_integer(total) and is_integer(reachable),
       do: total - reachable

  defp unreachable(_), do: 0

  defp cpu_shape(%{cores: nil}), do: "core count not reported"

  defp cpu_shape(cpu) do
    [
      "#{cpu.cores} logical",
      cpu.physical_cores && "#{cpu.physical_cores} physical",
      cpu.sockets && "#{cpu.sockets} socket#{if cpu.sockets == 1, do: "", else: "s"}"
    ]
    |> Enum.filter(& &1)
    |> Enum.join(" · ")
  end

  defp kind(true), do: "spinning"
  defp kind(false), do: "solid state"
  defp kind(nil), do: "unknown"

  defp mounted(%{mountpoints: []}), do: "—"
  defp mounted(%{mountpoints: points}), do: Enum.join(points, " ")

  defp memory_class(value) when value >= 90, do: "progress-error"
  defp memory_class(value) when value >= 75, do: "progress-warning"
  defp memory_class(_), do: "progress-primary"
end
