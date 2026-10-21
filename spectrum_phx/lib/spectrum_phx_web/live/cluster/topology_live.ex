defmodule SpectrumPhxWeb.Cluster.TopologyLive do
  @moduledoc """
  Cluster fault domains and physical hardware topology visualization:
  Cluster -> Racks -> Blocks/Chassis -> Hosts -> Disks.

  Displays the active fault tolerance level (`kDisk`, `kHost`, `kBlock`, `kRack`)
  and redundancy factor (FTT). On single-node clusters in `kDisk` domain, highlights
  local multi-disk extent mirroring (tolerating single SSD/NVMe drive loss).
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [panel: 1, dom_slug: 1]
  import SpectrumPhxWeb.Storage.Components, only: [bytes: 1]

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hardware
  alias SpectrumPhx.Settings

  @refresh_interval_ms 15_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(@refresh_interval_ms, self(), :refresh)
    end

    {:ok, socket |> assign(page_title: "Topology") |> load_topology()}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load_topology(socket)}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, load_topology(socket)}

  defp load_topology(socket) do
    config = Config.all()
    fault_domain = Config.fault_domain()
    ftt = Config.redundancy_factor()
    hosts = Settings.cluster_nodes()
    inventory = Hardware.inventory()

    # Map inventory node hardware by IP
    hw_by_ip =
      case inventory do
        %{nodes: inv_nodes} when is_list(inv_nodes) ->
          Map.new(inv_nodes, fn node -> {node.ip, node} end)

        _ ->
          %{}
      end

    # Group hosts by rack and chassis
    racks =
      hosts
      |> Enum.group_by(fn h -> h.rack || "rack1" end)
      |> Enum.sort_by(fn {rack_name, _} -> rack_name end)
      |> Enum.map(fn {rack_name, rack_hosts} ->
        blocks =
          rack_hosts
          |> Enum.group_by(fn h -> h.chassis_serial || "chassis-01" end)
          |> Enum.sort_by(fn {block_name, _} -> block_name end)
          |> Enum.map(fn {block_name, block_hosts} ->
            enriched_hosts =
              block_hosts
              |> Enum.sort_by(fn h -> h.node_position || h.hostname end)
              |> Enum.map(fn h ->
                hw = Map.get(hw_by_ip, h.ip, %{})
                disks = Map.get(hw, :disks, [])
                Map.merge(h, %{hw: hw, disks: disks})
              end)

            %{
              chassis_serial: block_name,
              hosts: enriched_hosts
            }
          end)

        %{
          rack: rack_name,
          blocks: blocks,
          host_count: length(rack_hosts)
        }
      end)

    single_node_resilient? =
      length(hosts) == 1 and fault_domain == "kdisk" and ftt >= 1

    socket
    |> assign(:cluster_name, Map.get(config, :cluster_name, "hci-01"))
    |> assign(:vip, Config.vip())
    |> assign(:fault_domain, fault_domain)
    |> assign(:ftt, ftt)
    |> assign(:hosts, hosts)
    |> assign(:racks, racks)
    |> assign(:single_node_resilient?, single_node_resilient?)
    |> assign(:read_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
      ~H"""
      <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:topology}>
        <.header>
          Cluster Topology & Fault Domains
          <:subtitle>
            <span class="flex flex-wrap items-center gap-2 mt-1">
              <span class="text-xs opacity-60">
                updated at {Calendar.strftime(@read_at, "%H:%M:%S")} UTC
              </span>
              <span class="badge badge-sm badge-info gap-1 font-mono uppercase">
                domain: {@fault_domain}
              </span>
              <span class={["badge badge-sm gap-1", if(@ftt > 0, do: "badge-success", else: "badge-warning")]}>
                FTT: {@ftt} ({ftt_label(@ftt)})
              </span>
            </span>
          </:subtitle>
          <:actions>
            <.link navigate={~p"/settings"} class="btn btn-sm btn-ghost gap-1.5" id="domain-settings-link">
              <.icon name="hero-cog-6-tooth" class="size-4" /> Domain Settings
            </.link>
            <.button phx-click="refresh" id="refresh-button">
              <.icon name="hero-arrow-path" class="size-4" /> Refresh
            </.button>
          </:actions>
        </.header>

        <div :if={@single_node_resilient?} class="alert alert-success alert-soft mt-3" id="single-node-kdisk-banner">
          <.icon name="hero-shield-check" class="size-5 shrink-0 text-success" />
          <div class="text-sm">
            <span class="font-semibold">Single-Node Drive Resilience (kDisk) Active:</span>
            FTT={@ftt} redundancy is achieved through local multi-disk mirroring across physical SSD/NVMe drives.
            Extent groups are mirrored locally to survive an individual physical drive failure without data loss.
          </div>
        </div>

        <div class="grid gap-4 mt-4">
          <.panel id="fault-domain-summary" title="Fault Tolerance Boundary">
            <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <.domain_card
                id="domain-kdisk"
                name="kDisk"
                title="Disk-Level Resilience"
                desc="Tolerates disk drive loss via local/remote mirroring. Enables FTT=1 on single-node clusters with 2+ SSDs."
                active?={@fault_domain == "kdisk"}
              />
              <.domain_card
                id="domain-khost"
                name="kHost"
                title="Host-Level Resilience"
                desc="Tolerates complete hypervisor node failure. Replicas distributed across independent nodes (requires FTT+1 hosts)."
                active?={@fault_domain == "khost"}
              />
              <.domain_card
                id="domain-kblock"
                name="kBlock"
                title="Chassis/Block Awareness"
                desc="Tolerates multi-node chassis loss. Replicas distributed across independent chassis enclosures (power/backplane)."
                active?={@fault_domain == "kblock"}
              />
              <.domain_card
                id="domain-krack"
                name="kRack"
                title="Rack-Level Resilience"
                desc="Tolerates full rack failure (TOR switch/PDU loss). Replicas distributed across separate physical racks."
                active?={@fault_domain == "krack"}
              />
            </div>
          </.panel>

          <div class="flex flex-col gap-6" id="racks-container">
            <div
              :for={rack <- @racks}
              id={"rack-#{dom_slug(rack.rack)}"}
              class="border border-base-300 rounded-lg bg-base-200/40 p-4 flex flex-col gap-4"
            >
              <div class="flex flex-wrap items-center justify-between border-b border-base-300 pb-2">
                <div class="flex items-center gap-2">
                  <.icon name="hero-server-stack" class="size-5 text-primary" />
                  <h3 class="font-bold text-base uppercase tracking-wider">{rack.rack}</h3>
                  <span class="badge badge-sm badge-neutral">{rack.host_count} host(s)</span>
                </div>
                <span class="text-xs opacity-60">Physical Server Rack</span>
              </div>

              <div class="grid gap-4 sm:grid-cols-1 lg:grid-cols-2 xl:grid-cols-3">
                <div
                  :for={block <- rack.blocks}
                  id={"block-#{dom_slug(block.chassis_serial)}"}
                  class="card card-border bg-base-100 shadow-sm"
                >
                  <div class="card-body p-4 gap-3">
                    <div class="flex items-center justify-between border-b border-base-200 pb-2">
                      <div class="flex items-center gap-1.5 font-medium text-sm">
                        <.icon name="hero-cube" class="size-4 text-secondary" />
                        <span>Chassis: {block.chassis_serial}</span>
                      </div>
                      <span class="badge badge-xs badge-ghost">2U Enclosure</span>
                    </div>

                    <div class="flex flex-col gap-3">
                      <div
                        :for={host <- block.hosts}
                        id={"host-card-#{dom_slug(host.ip)}"}
                        class="p-3 rounded-md bg-base-200/60 border border-base-300/80 flex flex-col gap-2"
                      >
                        <div class="flex items-start justify-between gap-2">
                          <div>
                            <div class="flex items-center gap-1.5">
                              <span class="badge badge-xs badge-primary font-mono font-bold">
                                Slot {host.node_position}
                              </span>
                              <span class="font-semibold text-sm">{host.hostname}</span>
                            </div>
                            <p class="text-xs font-mono opacity-60 mt-0.5">{host.ip}</p>
                          </div>
                          <div class="flex flex-wrap items-center gap-1">
                            <span :if={host.is_witness?} class="badge badge-xs badge-warning">witness</span>
                            <span :if={not host.is_witness?} class="badge badge-xs badge-success">hypervisor</span>
                            <span :if={host.in_maintenance?} class="badge badge-xs badge-warning">maintenance</span>
                          </div>
                        </div>

                        <!-- Disks section -->
                        <div class="mt-2 border-t border-base-300/50 pt-2">
                          <div class="flex items-center justify-between text-xs font-medium opacity-75 mb-1.5">
                            <span class="flex items-center gap-1">
                              <.icon name="hero-circle-stack" class="size-3.5" />
                              Physical Disks ({length(host.disks)})
                            </span>
                            <span :if={@fault_domain == "kdisk"} class="text-success text-[11px] font-semibold">
                              mirrored local tier
                            </span>
                          </div>

                          <div :if={host.disks == []} class="text-xs opacity-50 italic py-1">
                            No disk inventory reported yet.
                          </div>

                          <div :if={host.disks != []} class="grid gap-1.5">
                            <div
                              :for={disk <- host.disks}
                              id={"disk-#{dom_slug(host.ip)}-#{disk.name}"}
                              class="flex items-center justify-between text-xs bg-base-100 px-2 py-1.5 rounded border border-base-300/60 font-mono"
                            >
                              <div class="flex items-center gap-1.5 truncate">
                                <span class={["badge badge-xs", if(disk.rotational?, do: "badge-neutral", else: "badge-accent")]}>
                                  {if disk.rotational?, do: "HDD", else: "SSD"}
                                </span>
                                <span class="font-semibold text-base-content truncate">{disk.name}</span>
                                <span :if={disk.model} class="opacity-50 text-[10px] hidden sm:inline truncate">
                                  {disk.model}
                                </span>
                              </div>
                              <div class="flex items-center gap-2 shrink-0">
                                <span class="tabular-nums font-semibold">
                                  {disk.size_bytes && bytes(disk.size_bytes)}
                                </span>
                                <span
                                  :if={extent_disk?(disk)}
                                  class="badge badge-xs badge-success"
                                  title="Dedicated Sidon extent store drive"
                                >
                                  extent
                                </span>
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </Layouts.app>
      """
  end

  attr :id, :string, required: true
  attr :name, :string, required: true
  attr :title, :string, required: true
  attr :desc, :string, required: true
  attr :active?, :boolean, default: false

  defp domain_card(assigns) do
    ~H"""
    <div
      id={@id}
      class={[
        "p-3 rounded-lg border transition-all flex flex-col gap-1.5",
        if(@active?,
          do: "bg-primary/10 border-primary ring-1 ring-primary/40",
          else: "bg-base-100 border-base-300/80 opacity-70 hover:opacity-100"
        )
      ]}
    >
      <div class="flex items-center justify-between">
        <span class="font-bold font-mono text-sm uppercase">{@name}</span>
        <span :if={@active?} class="badge badge-xs badge-primary font-bold">Active</span>
      </div>
      <h4 class="font-semibold text-xs">{@title}</h4>
      <p class="text-[11px] opacity-70 leading-relaxed">{@desc}</p>
    </div>
    """
  end

  defp ftt_label(0), do: "No fault tolerance"
  defp ftt_label(1), do: "Survives 1 failure"
  defp ftt_label(2), do: "Survives 2 failures"
  defp ftt_label(n), do: "Survives #{n} failures"

  defp extent_disk?(disk) do
    mounts = Map.get(disk, :mountpoints, [])
    Enum.any?(mounts, fn m -> String.contains?(m, "sidon") or String.contains?(m, "aether") end)
  end
end
