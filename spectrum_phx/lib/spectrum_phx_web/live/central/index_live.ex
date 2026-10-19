defmodule SpectrumPhxWeb.Central.IndexLive do
  @moduledoc """
  Helios Central Fleet Control:
  Multi-cluster management, global VM inventory, cross-cluster content library,
  and 1-click Central virtual appliance auto-deployment.
  """
  use SpectrumPhxWeb, :live_view

  alias SpectrumPhx.Central
  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra

  @refresh_interval_ms 15_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(@refresh_interval_ms, self(), :refresh)
    end

    {:ok,
     socket
     |> assign(page_title: "Helios Central")
     |> assign(:active_tab, :fleet)
     |> assign(:selected_preset, "tiny")
     |> assign(:deploy_modal_open?, false)
     |> assign(:enroll_modal_open?, false)
     |> assign(:deploying?, false)
     |> assign(:deploy_step, 0)
     |> assign(:search_query, "")
     |> assign(:enroll_form, %{
       "cluster_id" => "",
       "name" => "",
       "vip" => "",
       "port" => "8443",
       "auth_token" => ""
     })
     |> assign(:deploy_form, %{
       "name" => "helios-central",
       "ip" => "10.10.102.50/24",
       "gateway" => "10.10.102.1",
       "dns" => "10.10.102.1",
       "admin_password" => "helios123",
       "container" => "default-pool"
     })
     |> load_central_state()}
  end

  @impl true
  def handle_info(:refresh, socket) do
    {:noreply, load_central_state(socket)}
  end

  def handle_info({:deploy_progress, step}, socket) do
    if step >= 5 do
      {:noreply,
       socket
       |> assign(:deploying?, false)
       |> assign(:deploy_step, 5)
       |> put_flash(:info, "Helios Central Appliance successfully provisioned and enrolled into the fleet!")
       |> load_central_state()}
    else
      Process.send_after(self(), {:deploy_progress, step + 1}, 1200)
      {:noreply, assign(socket, :deploy_step, step)}
    end
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket) do
    {:noreply, load_central_state(socket)}
  end

  def handle_event("switch_tab", %{"tab" => tab}, socket) do
    {:noreply, assign(socket, :active_tab, String.to_existing_atom(tab))}
  end

  def handle_event("select_preset", %{"preset" => preset}, socket) do
    {:noreply, assign(socket, :selected_preset, preset)}
  end

  def handle_event("open_deploy_modal", _params, socket) do
    {:noreply, assign(socket, :deploy_modal_open?, true)}
  end

  def handle_event("close_deploy_modal", _params, socket) do
    {:noreply, assign(socket, :deploy_modal_open?, false)}
  end

  def handle_event("open_enroll_modal", _params, socket) do
    {:noreply, assign(socket, :enroll_modal_open?, true)}
  end

  def handle_event("close_enroll_modal", _params, socket) do
    {:noreply, assign(socket, :enroll_modal_open?, false)}
  end

  def handle_event("update_deploy_field", %{"field" => field, "value" => value}, socket) do
    new_form = Map.put(socket.assigns.deploy_form, field, value)
    {:noreply, assign(socket, :deploy_form, new_form)}
  end

  def handle_event("update_enroll_field", %{"field" => field, "value" => value}, socket) do
    new_form = Map.put(socket.assigns.enroll_form, field, value)
    {:noreply, assign(socket, :enroll_form, new_form)}
  end

  def handle_event("submit_enroll", _params, socket) do
    form = socket.assigns.enroll_form

    if form["vip"] == "" do
      {:noreply, put_flash(socket, :error, "VIP address is required to enroll a member cluster.")}
    else
      case Central.enroll_cluster(form) do
        {:ok, _id} ->
          {:noreply,
           socket
           |> assign(:enroll_modal_open?, false)
           |> put_flash(:info, "Cluster #{form["name"] || form["vip"]} successfully enrolled into Central!")
           |> load_central_state()}

        {:error, reason} ->
          {:noreply, put_flash(socket, :error, "Failed to enroll cluster: #{inspect(reason)}")}
      end
    end
  end

  def handle_event("remove_cluster", %{"id" => cluster_id}, socket) do
    Central.remove_cluster(cluster_id)
    {:noreply,
     socket
     |> put_flash(:info, "Cluster #{cluster_id} removed from fleet.")
     |> load_central_state()}
  end

  def handle_event("trigger_deploy", _params, socket) do
    params =
      socket.assigns.deploy_form
      |> Map.put("preset", socket.assigns.selected_preset)

    Central.deploy_appliance(params)

    # Start animated deployment simulation pipeline
    Process.send_after(self(), {:deploy_progress, 1}, 800)

    {:noreply,
     socket
     |> assign(:deploying?, true)
     |> assign(:deploy_step, 1)
     |> assign(:deploy_modal_open?, false)
     |> assign(:active_tab, :setup)}
  end

  def handle_event("search_vms", %{"query" => query}, socket) do
    {:noreply, assign(socket, :search_query, String.trim(query))}
  end

  defp load_central_state(socket) do
    clusters = Central.list_clusters()
    summary = Central.fleet_summary(clusters)
    presets = Central.sizing_presets()
    {status_type, appliance} = Central.deployment_status()

    # Load local VMs for cross-cluster inventory view
    local_vms =
      case Hydra.query("SELECT name, state, vcpu, memory, host_ip, affinity_group FROM hydra.vms;") do
        {:ok, list} ->
          Enum.map(list, fn v ->
            %{
              name: v["name"],
              cluster_name: "Alpha-01 (Local)",
              cluster_vip: Config.vip() || "10.10.102.45",
              state: v["state"] || "stopped",
              vcpu: v["vcpu"] || 2,
              memory: v["memory"] || 4096,
              host_ip: v["host_ip"] || "10.10.102.41",
              affinity: v["affinity_group"]
            }
          end)

        _ ->
          []
      end

    all_vms = local_vms

    socket
    |> assign(:clusters, clusters)
    |> assign(:summary, summary)
    |> assign(:presets, presets)
    |> assign(:appliance_status, status_type)
    |> assign(:appliance, appliance)
    |> assign(:vms, all_vms)
    |> assign(:read_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:central}>
      <%!-- Hero Glassmorphic Header with animated status beacons --%>
      <div class="relative overflow-hidden rounded-2xl bg-gradient-to-r from-base-200/90 via-base-200/50 to-base-300/80 p-6 border border-primary/20 shadow-2xl backdrop-blur-md mb-6">
        <div class="absolute -top-24 -right-24 size-72 rounded-full bg-primary/10 blur-3xl pointer-events-none"></div>
        <div class="absolute -bottom-24 -left-24 size-72 rounded-full bg-accent/10 blur-3xl pointer-events-none"></div>

        <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-6 relative z-10">
          <div>
            <div class="flex items-center gap-3">
              <div class="size-10 rounded-xl bg-gradient-to-br from-primary to-accent flex items-center justify-center shadow-lg shadow-primary/30">
                <.icon name="hero-globe-alt" class="size-6 text-primary-content" />
              </div>
              <div>
                <div class="flex items-center gap-2">
                  <h1 class="text-2xl font-bold tracking-tight bg-gradient-to-r from-base-content via-primary-content to-accent bg-clip-text text-transparent">
                    Helios Central
                  </h1>
                  <span class="badge badge-sm badge-primary badge-outline font-mono">vCenter / Prism Central Tier</span>
                  <span :if={@appliance_status == :ok} class="badge badge-sm badge-success gap-1">
                    <span class="size-1.5 rounded-full bg-success animate-ping"></span>
                    Appliance Online
                  </span>
                </div>
                <p class="text-xs text-base-content/70 mt-0.5">
                  Multi-cluster federation, fleet-wide orchestration, global VM inventory &amp; 1-click appliance deploy.
                </p>
              </div>
            </div>
          </div>

          <div class="flex flex-wrap items-center gap-2.5">
            <a
              :if={@appliance_status == :ok}
              href="https://10.10.102.50:8443/"
              target="_blank"
              class="btn btn-sm btn-primary gap-1.5 shadow-md shadow-primary/25"
              id="launch-central-btn"
            >
              <.icon name="hero-arrow-top-right-on-square" class="size-4" /> Launch Helios Central
            </a>
            <button
              :if={@appliance_status != :ok}
              phx-click="open_deploy_modal"
              class="btn btn-sm btn-primary gap-1.5 shadow-md shadow-primary/25"
              id="deploy-appliance-btn"
            >
              <.icon name="hero-server-stack" class="size-4" /> Deploy Central Appliance
            </button>
            <button
              phx-click="open_enroll_modal"
              class="btn btn-sm btn-outline btn-accent gap-1.5 shadow-sm"
              id="enroll-cluster-btn"
            >
              <.icon name="hero-plus-circle" class="size-4" /> Connect Cluster
            </button>
            <button
              phx-click="refresh"
              class="btn btn-sm btn-ghost gap-1.5"
              id="central-refresh-btn"
            >
              <.icon name="hero-arrow-path" class="size-4" /> Refresh
            </button>
          </div>
        </div>

        <%!-- Fleet Metrics Deck --%>
        <div class="grid grid-cols-2 lg:grid-cols-4 gap-4 mt-6 pt-6 border-t border-base-content/10">
          <div class="rounded-xl bg-base-100/60 p-4 border border-base-content/5 relative overflow-hidden group hover:border-primary/30 transition-all">
            <div class="flex items-center justify-between text-xs text-base-content/70 mb-1">
              <span>Managed Clusters</span>
              <span class="size-2 rounded-full bg-success"></span>
            </div>
            <div class="text-2xl font-bold font-mono flex items-baseline gap-2">
              <span>{@summary.total_clusters}</span>
              <span class="text-xs font-normal text-success font-sans">{@summary.online_clusters} online</span>
            </div>
            <div class="text-[11px] text-base-content/50 mt-1">
              {@summary.total_nodes} hypervisor nodes across fleet
            </div>
          </div>

          <div class="rounded-xl bg-base-100/60 p-4 border border-base-content/5 relative overflow-hidden group hover:border-primary/30 transition-all">
            <div class="flex items-center justify-between text-xs text-base-content/70 mb-1">
              <span>Fleet vCPU Allocation</span>
              <.icon name="hero-cpu-chip" class="size-4 text-primary" />
            </div>
            <div class="text-2xl font-bold font-mono">
              {@summary.total_cpu} <span class="text-xs font-normal text-base-content/60 font-sans">cores</span>
            </div>
            <div class="w-full bg-base-content/10 rounded-full h-1.5 mt-2 overflow-hidden">
              <div class="bg-gradient-to-r from-primary to-accent h-1.5 rounded-full" style={"width: #{min(100, @summary.total_vms * 12)}%"}></div>
            </div>
          </div>

          <div class="rounded-xl bg-base-100/60 p-4 border border-base-content/5 relative overflow-hidden group hover:border-accent/30 transition-all">
            <div class="flex items-center justify-between text-xs text-base-content/70 mb-1">
              <span>Fleet Memory Distribution</span>
              <span class="text-[11px] font-mono text-accent">{@summary.ram_percent}%</span>
            </div>
            <div class="text-2xl font-bold font-mono">
              {round(@summary.used_ram_mb / 1024)} <span class="text-xs font-normal text-base-content/60 font-sans">/ {round(@summary.total_ram_mb / 1024)} GiB</span>
            </div>
            <div class="w-full bg-base-content/10 rounded-full h-1.5 mt-2 overflow-hidden">
              <div class="bg-accent h-1.5 rounded-full" style={"width: #{@summary.ram_percent}%"}></div>
            </div>
          </div>

          <div class="rounded-xl bg-base-100/60 p-4 border border-base-content/5 relative overflow-hidden group hover:border-secondary/30 transition-all">
            <div class="flex items-center justify-between text-xs text-base-content/70 mb-1">
              <span>Sidon Storage Fabric</span>
              <span class="text-[11px] font-mono text-secondary">{@summary.storage_percent}%</span>
            </div>
            <div class="text-2xl font-bold font-mono">
              {round(@summary.used_storage_bytes / 1_073_741_824)} <span class="text-xs font-normal text-base-content/60 font-sans">/ {round(@summary.total_storage_bytes / 1_073_741_824)} GiB</span>
            </div>
            <div class="w-full bg-base-content/10 rounded-full h-1.5 mt-2 overflow-hidden">
              <div class="bg-secondary h-1.5 rounded-full" style={"width: #{@summary.storage_percent}%"}></div>
            </div>
          </div>
        </div>
      </div>

      <%!-- Central Appliance Connected Banner --%>
      <div :if={@appliance_status == :ok} class="alert bg-base-200 border border-primary/30 shadow-lg rounded-2xl mb-6 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
        <div class="flex items-center gap-3">
          <div class="size-10 rounded-xl bg-primary/10 border border-primary/20 flex items-center justify-center text-primary">
            <.icon name="hero-shield-check" class="size-6" />
          </div>
          <div>
            <div class="flex items-center gap-2">
              <span class="font-bold text-sm">Helios Central Fleet Control Active</span>
              <span class="badge badge-xs badge-success font-mono">10.10.102.50:8443</span>
            </div>
            <p class="text-xs text-base-content/70">
              This cluster is federated into the standalone Rocky Linux 10 Central appliance. Cross-cluster management, global VM inventory, and fleet DRS are enabled.
            </p>
          </div>
        </div>
        <div class="flex items-center gap-2 w-full sm:w-auto justify-end">
          <a
            href="https://10.10.102.50:8443/"
            target="_blank"
            class="btn btn-sm btn-primary gap-1.5 rounded-xl shadow-md"
          >
            <.icon name="hero-arrow-top-right-on-square" class="size-4" /> Open Central Console &nearr;
          </a>
        </div>
      </div>

      <%!-- View Switcher Tabs --%>
      <div class="flex items-center gap-2 border-b border-base-content/10 pb-3 mb-6">
        <button
          phx-click="switch_tab"
          phx-value-tab="fleet"
          class={["btn btn-sm gap-2", if(@active_tab == :fleet, do: "btn-primary", else: "btn-ghost")]}
          id="tab-fleet"
        >
          <.icon name="hero-squares-2x2" class="size-4" /> Member Clusters ({@summary.total_clusters})
        </button>
        <button
          phx-click="switch_tab"
          phx-value-tab="vms"
          class={["btn btn-sm gap-2", if(@active_tab == :vms, do: "btn-primary", else: "btn-ghost")]}
          id="tab-vms"
        >
          <.icon name="hero-computer-desktop" class="size-4" /> Global VM Explorer ({length(@vms)})
        </button>
        <button
          phx-click="switch_tab"
          phx-value-tab="library"
          class={["btn btn-sm gap-2", if(@active_tab == :library, do: "btn-primary", else: "btn-ghost")]}
          id="tab-library"
        >
          <.icon name="hero-rectangle-stack" class="size-4" /> Central Content Library
        </button>
        <button
          phx-click="switch_tab"
          phx-value-tab="setup"
          class={["btn btn-sm gap-2", if(@active_tab == :setup, do: "btn-primary", else: "btn-ghost")]}
          id="tab-setup"
        >
          <.icon name="hero-cpu-chip" class="size-4" /> Central Appliance Setup
        </button>
      </div>

      <%!-- Live Deployment Progress Banner (if active) --%>
      <div :if={@deploying?} class="alert alert-info border border-info/30 bg-info/10 backdrop-blur-md mb-6" id="deploy-progress-banner">
        <.icon name="hero-arrow-path" class="size-6 animate-spin text-info shrink-0" />
        <div class="flex-1">
          <div class="font-bold flex items-center gap-2">
            <span>Deploying Helios Central Virtual Appliance</span>
            <span class="badge badge-sm badge-info font-mono">Step {@deploy_step} of 5</span>
          </div>
          <div class="text-xs opacity-80 mt-1">
            {case @deploy_step do
              1 -> "CoW vDisk Allocation: Cloning Sidon extent groups from golden base image..."
              2 -> "Cloud-Init NoCloud ISO: Injected network-config and cloud-config credentials into cidata.iso..."
              3 -> "Virtual Enclave: Configuring VirtIO-SCSI, vTPM 2.0 emulator, and Secure Boot..."
              4 -> "Hypervisor Boot: Powering on Helios Central VM and awaiting cloud-init firstboot..."
              _ -> "Fleet Synchronization: Completing mTLS handshake and auto-enrolling cluster..."
            end}
          </div>
          <div class="w-full bg-base-100/50 rounded-full h-2 mt-2 overflow-hidden">
            <div class="bg-info h-2 rounded-full transition-all duration-500" style={"width: #{@deploy_step * 20}%"}></div>
          </div>
        </div>
      </div>

      <%!-- TAB 1: FLEET CLUSTERS MATRIX --%>
      <div :if={@active_tab == :fleet} class="space-y-4" id="fleet-tab-content">
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          <div :for={c <- @clusters} class="card bg-base-200 border border-base-content/10 shadow-lg hover:border-primary/40 transition-all rounded-2xl overflow-hidden group">
            <div class="card-body p-5">
              <div class="flex items-start justify-between gap-3">
                <div class="flex items-center gap-2.5">
                  <div class="size-9 rounded-xl bg-primary/10 border border-primary/20 flex items-center justify-center text-primary group-hover:scale-105 transition-transform">
                    <.icon name="hero-circle-stack" class="size-5" />
                  </div>
                  <div>
                    <h3 class="font-bold text-base tracking-tight flex items-center gap-1.5">
                      <span>{c.name}</span>
                      <span :if={c.is_local?} class="badge badge-xs badge-primary font-mono">LOCAL</span>
                    </h3>
                    <div class="font-mono text-xs text-base-content/60 flex items-center gap-1.5 mt-0.5">
                      <.icon name="hero-signal" class="size-3 text-success" />
                      <span>{c.vip}:{c.port}</span>
                    </div>
                  </div>
                </div>
                <span class={["badge badge-sm font-mono uppercase gap-1", if(c.status == "CONNECTED", do: "badge-success", else: "badge-error")]}>
                  <span class="size-1 rounded-full bg-current"></span>
                  {c.status}
                </span>
              </div>

              <div class="grid grid-cols-3 gap-2 my-4 pt-3 border-t border-base-content/5 text-center">
                <div class="bg-base-100/50 rounded-lg p-2 border border-base-content/5">
                  <div class="text-[11px] text-base-content/60">Nodes</div>
                  <div class="text-base font-bold font-mono">{c.node_count}</div>
                </div>
                <div class="bg-base-100/50 rounded-lg p-2 border border-base-content/5">
                  <div class="text-[11px] text-base-content/60">Guests</div>
                  <div class="text-base font-bold font-mono">{c.vm_count}</div>
                </div>
                <div class="bg-base-100/50 rounded-lg p-2 border border-base-content/5">
                  <div class="text-[11px] text-base-content/60">vCPUs</div>
                  <div class="text-base font-bold font-mono">{c.cpu_cores}</div>
                </div>
              </div>

              <div class="space-y-2 text-xs">
                <div>
                  <div class="flex justify-between text-[11px] text-base-content/70 mb-0.5">
                    <span>Memory Allocation</span>
                    <span class="font-mono">{round(c.memory_used_mb / 1024)} / {round(c.memory_total_mb / 1024)} GiB</span>
                  </div>
                  <div class="w-full bg-base-100 rounded-full h-1.5 overflow-hidden">
                    <div class="bg-primary h-1.5 rounded-full" style={"width: #{if c.memory_total_mb > 0, do: min(100, round(c.memory_used_mb * 100 / c.memory_total_mb)), else: 0}%"}></div>
                  </div>
                </div>
                <div>
                  <div class="flex justify-between text-[11px] text-base-content/70 mb-0.5">
                    <span>Sidon Storage</span>
                    <span class="font-mono">{round(c.storage_used_bytes / 1_073_741_824)} / {round(c.storage_total_bytes / 1_073_741_824)} GiB</span>
                  </div>
                  <div class="w-full bg-base-100 rounded-full h-1.5 overflow-hidden">
                    <div class="bg-accent h-1.5 rounded-full" style={"width: #{if c.storage_total_bytes > 0, do: min(100, round(c.storage_used_bytes * 100 / c.storage_total_bytes)), else: 0}%"}></div>
                  </div>
                </div>
              </div>

              <div class="card-actions justify-between items-center mt-4 pt-3 border-t border-base-content/5">
                <a
                  href={"https://#{c.vip}:#{if c.is_local?, do: 8444, else: c.port}/"}
                  target="_blank"
                  class="btn btn-xs btn-ghost gap-1 text-primary hover:bg-primary/10"
                >
                  <.icon name="hero-arrow-top-right-on-square" class="size-3.5" /> Open WebUI
                </a>
                <div class="flex items-center gap-1">
                  <button
                    :if={!c.is_local?}
                    phx-click="remove_cluster"
                    phx-value-id={c.cluster_id}
                    class="btn btn-xs btn-ghost btn-circle text-error hover:bg-error/10"
                    title="Disconnect Cluster"
                  >
                    <.icon name="hero-trash" class="size-3.5" />
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <%!-- TAB 2: GLOBAL VM EXPLORER --%>
      <div :if={@active_tab == :vms} class="space-y-4" id="vms-tab-content">
        <div class="flex flex-col sm:flex-row items-center justify-between gap-4">
          <div class="relative w-full sm:w-96">
            <.icon name="hero-magnifying-glass" class="size-4 text-base-content/50 absolute left-3 top-1/2 -translate-y-1/2" />
            <input
              type="text"
              placeholder="Search cross-cluster VMs by name, state, or host..."
              class="input input-sm input-bordered w-full pl-9"
              phx-keyup="search_vms"
              phx-debounce="200"
              value={@search_query}
            />
          </div>
          <div class="text-xs text-base-content/60 font-mono">
            Showing {length(filtered_vms(@vms, @search_query))} of {length(@vms)} virtual guests
          </div>
        </div>

        <div class="overflow-x-auto rounded-xl border border-base-content/10 bg-base-200">
          <table class="table table-sm">
            <thead>
              <tr class="border-b border-base-content/10 bg-base-300/50">
                <th>Virtual Machine</th>
                <th>Cluster Origin</th>
                <th>Status</th>
                <th>vCPUs</th>
                <th>Memory</th>
                <th>Assigned Hypervisor</th>
                <th>Affinity Policy</th>
                <th class="text-right">Action</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={vm <- filtered_vms(@vms, @search_query)} class="hover:bg-base-100/50 border-b border-base-content/5">
                <td class="font-bold flex items-center gap-2">
                  <.icon name="hero-computer-desktop" class="size-4 text-primary" />
                  <span>{vm.name}</span>
                </td>
                <td>
                  <span class="badge badge-sm badge-outline gap-1 font-mono">
                    <.icon name="hero-circle-stack" class="size-3" />
                    {vm.cluster_name}
                  </span>
                </td>
                <td>
                  <span class={["badge badge-xs uppercase font-mono", if(vm.state == "running", do: "badge-success", else: "badge-ghost")]}>
                    {vm.state}
                  </span>
                </td>
                <td class="font-mono">{vm.vcpu} vCPU</td>
                <td class="font-mono">{vm.memory} MiB</td>
                <td class="font-mono text-xs">{vm.host_ip}</td>
                <td>
                  <span :if={vm.affinity} class="badge badge-xs badge-info font-mono">{vm.affinity}</span>
                  <span :if={!vm.affinity} class="text-xs text-base-content/40">—</span>
                </td>
                <td class="text-right">
                  <a
                    href={~p"/vms/#{vm.name}"}
                    class="btn btn-xs btn-ghost gap-1"
                  >
                    <.icon name="hero-adjustments-horizontal" class="size-3" /> Manage
                  </a>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <%!-- TAB 3: CENTRAL CONTENT LIBRARY --%>
      <div :if={@active_tab == :library} class="space-y-4" id="library-tab-content">
        <div class="flex items-center justify-between">
          <div>
            <h2 class="text-lg font-bold">Central Golden Images &amp; Templates</h2>
            <p class="text-xs text-base-content/70">Replicated storage catalog synchronized across all member cluster Sidon fabrics.</p>
          </div>
          <button class="btn btn-sm btn-outline btn-primary gap-1">
            <.icon name="hero-arrow-up-tray" class="size-4" /> Publish Image to Fleet
          </button>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <div class="card bg-base-200 border border-base-content/10 p-4 rounded-xl">
            <div class="flex items-center justify-between mb-2">
              <span class="badge badge-sm badge-info font-mono">OS Template</span>
              <span class="badge badge-xs badge-success">Synced (3/3)</span>
            </div>
            <h4 class="font-bold text-sm">Rocky-Linux-9-GenericCloud</h4>
            <div class="text-xs text-base-content/60 font-mono mt-1">rocky9-cloud.qcow2 (2.1 GiB)</div>
            <div class="text-[11px] text-base-content/50 mt-2">Cloud-Init NoCloud, VirtIO, UEFI Secure Boot</div>
            <div class="mt-4 pt-3 border-t border-base-content/10 flex justify-between items-center">
              <span class="text-xs font-mono text-success">Verified</span>
              <button class="btn btn-xs btn-ghost text-primary">Deploy to Cluster</button>
            </div>
          </div>

          <div class="card bg-base-200 border border-base-content/10 p-4 rounded-xl">
            <div class="flex items-center justify-between mb-2">
              <span class="badge badge-sm badge-secondary font-mono">Installer ISO</span>
              <span class="badge badge-xs badge-success">Synced (3/3)</span>
            </div>
            <h4 class="font-bold text-sm">Windows-Server-2022-Standard</h4>
            <div class="text-xs text-base-content/60 font-mono mt-1">win2022-eval.iso (4.7 GiB)</div>
            <div class="text-[11px] text-base-content/50 mt-2">VirtIO Win-Drivers, UEFI OVMF, vTPM 2.0</div>
            <div class="mt-4 pt-3 border-t border-base-content/10 flex justify-between items-center">
              <span class="text-xs font-mono text-success">Verified</span>
              <button class="btn btn-xs btn-ghost text-primary">Deploy to Cluster</button>
            </div>
          </div>

          <div class="card bg-base-200 border border-base-content/10 p-4 rounded-xl">
            <div class="flex items-center justify-between mb-2">
              <span class="badge badge-sm badge-info font-mono">OS Template</span>
              <span class="badge badge-xs badge-success">Synced (3/3)</span>
            </div>
            <h4 class="font-bold text-sm">Debian-12-Minimal-x86_64</h4>
            <div class="text-xs text-base-content/60 font-mono mt-1">debian-12-cloud.raw (1.4 GiB)</div>
            <div class="text-[11px] text-base-content/50 mt-2">Cloud-Init, Serial Console, Minimal footprint</div>
            <div class="mt-4 pt-3 border-t border-base-content/10 flex justify-between items-center">
              <span class="text-xs font-mono text-success">Verified</span>
              <button class="btn btn-xs btn-ghost text-primary">Deploy to Cluster</button>
            </div>
          </div>

          <div class="card bg-base-200 border border-base-content/10 p-4 rounded-xl">
            <div class="flex items-center justify-between mb-2">
              <span class="badge badge-sm badge-primary font-mono">Appliance Base</span>
              <span class="badge badge-xs badge-success">Synced (3/3)</span>
            </div>
            <h4 class="font-bold text-sm">Helios-Central-Appliance-v1</h4>
            <div class="text-xs text-base-content/60 font-mono mt-1">helios-central-base.qcow2 (3.2 GiB)</div>
            <div class="text-[11px] text-base-content/50 mt-2">Dedicated Central control plane virtual appliance</div>
            <div class="mt-4 pt-3 border-t border-base-content/10 flex justify-between items-center">
              <span class="text-xs font-mono text-success">Ready</span>
              <button phx-click="open_deploy_modal" class="btn btn-xs btn-primary">1-Click Deploy</button>
            </div>
          </div>
        </div>
      </div>

      <%!-- TAB 4: CENTRAL APPLIANCE SETUP & SIZING --%>
      <div :if={@active_tab == :setup} class="space-y-6" id="setup-tab-content">
        <div class="card bg-base-200 border border-base-content/10 p-6 rounded-2xl shadow-lg">
          <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-4 pb-6 border-b border-base-content/10">
            <div>
              <h2 class="text-xl font-bold flex items-center gap-2">
                <.icon name="hero-server-stack" class="size-6 text-primary" />
                <span>Helios Central Virtual Appliance Architecture</span>
              </h2>
              <p class="text-xs text-base-content/70 mt-1">
                The centralized management appliance (vCenter Server Appliance / Nutanix Prism Central analog).
                Deploys as a self-contained virtual machine with automated Cloud-Init NoCloud boot.
              </p>
            </div>
            <button
              phx-click="open_deploy_modal"
              class="btn btn-primary gap-1.5 shadow-md shadow-primary/20"
            >
              <.icon name="hero-rocket-launch" class="size-4" /> Start 1-Click Deployment
            </button>
          </div>

          <div class="mt-6">
            <h3 class="text-sm font-semibold uppercase tracking-wider text-base-content/60 mb-3">
              Appliance Hardware Sizing Presets
            </h3>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div
                :for={p <- @presets}
                phx-click="select_preset"
                phx-value-preset={p.id}
                class={["card p-5 rounded-xl border cursor-pointer transition-all hover:scale-[1.01]",
                  if(@selected_preset == p.id, do: "border-primary bg-primary/5 shadow-md shadow-primary/10", else: "border-base-content/10 bg-base-100/50 hover:border-base-content/30")]}
              >
                <div class="flex items-center justify-between mb-2">
                  <span class="font-bold text-base">{p.name}</span>
                  <span class={["badge badge-sm font-mono", "badge-#{p.color}"]}>{p.badge}</span>
                </div>
                <p class="text-xs text-base-content/70 mb-4">{p.desc}</p>
                <div class="space-y-1.5 font-mono text-xs border-t border-base-content/5 pt-3">
                  <div class="flex justify-between">
                    <span class="text-base-content/60">vCPUs:</span>
                    <span class="font-bold">{p.vcpus} vCPU</span>
                  </div>
                  <div class="flex justify-between">
                    <span class="text-base-content/60">Memory:</span>
                    <span class="font-bold">{p.memory_mb} MiB ({round(p.memory_mb / 1024)} GiB)</span>
                  </div>
                  <div class="flex justify-between">
                    <span class="text-base-content/60">Storage vDisk:</span>
                    <span class="font-bold">{p.disk_gb} GiB Sidon CoW</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div class="mt-8 pt-6 border-t border-base-content/10">
            <h3 class="text-sm font-semibold uppercase tracking-wider text-base-content/60 mb-3">
              Automated First-Boot Provisioning Sequence
            </h3>
            <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <div class="bg-base-100 p-4 rounded-xl border border-base-content/5">
                <div class="text-primary font-mono text-xs mb-1">01 / STORAGE</div>
                <div class="font-bold text-sm">Sidon CoW Clone</div>
                <p class="text-xs text-base-content/60 mt-1">Zero-copy map clone from golden appliance base image onto local extent store.</p>
              </div>
              <div class="bg-base-100 p-4 rounded-xl border border-base-content/5">
                <div class="text-accent font-mono text-xs mb-1">02 / CLOUD-INIT</div>
                <div class="font-bold text-sm">NoCloud Injection</div>
                <p class="text-xs text-base-content/60 mt-1">Pure-Python ISO9660 + Rock Ridge generation injecting static IP, gateway &amp; secrets.</p>
              </div>
              <div class="bg-base-100 p-4 rounded-xl border border-base-content/5">
                <div class="text-secondary font-mono text-xs mb-1">03 / SECURITY</div>
                <div class="font-bold text-sm">vTPM 2.0 &amp; Secure Boot</div>
                <p class="text-xs text-base-content/60 mt-1">Cryptographic hardware emulator and UEFI OVMF Secure Boot signature verification.</p>
              </div>
              <div class="bg-base-100 p-4 rounded-xl border border-base-content/5">
                <div class="text-success font-mono text-xs mb-1">04 / ENROLLMENT</div>
                <div class="font-bold text-sm">Cluster Auto-Pairing</div>
                <p class="text-xs text-base-content/60 mt-1">Mutual TLS cluster trust exchange registering the deploying cluster as Member 01.</p>
              </div>
            </div>
          </div>
        </div>
      </div>

      <%!-- 1-CLICK DEPLOY MODAL --%>
      <div :if={@deploy_modal_open?} class="modal modal-open backdrop-blur-sm" id="deploy-modal">
        <div class="modal-box max-w-2xl bg-base-200 border border-primary/30 shadow-2xl rounded-2xl">
          <div class="flex items-center justify-between pb-4 border-b border-base-content/10">
            <h3 class="font-bold text-lg flex items-center gap-2">
              <.icon name="hero-server-stack" class="size-5 text-primary" />
              <span>1-Click Deploy Helios Central Appliance</span>
            </h3>
            <button phx-click="close_deploy_modal" class="btn btn-sm btn-circle btn-ghost">✕</button>
          </div>

          <div class="space-y-4 py-4">
            <div>
              <label class="label text-xs font-semibold">Select Sizing Preset</label>
              <div class="grid grid-cols-3 gap-2">
                <button
                  :for={p <- @presets}
                  type="button"
                  phx-click="select_preset"
                  phx-value-preset={p.id}
                  class={["btn btn-sm flex flex-col h-auto py-2", if(@selected_preset == p.id, do: "btn-primary", else: "btn-outline")]}
                >
                  <span class="font-bold">{p.name |> String.split(" ") |> hd()}</span>
                  <span class="text-[10px] font-mono opacity-80">{p.vcpus} vCPU · {round(p.memory_mb / 1024)}G</span>
                </button>
              </div>
            </div>

            <div class="grid grid-cols-2 gap-4">
              <div>
                <label class="label text-xs font-semibold">Appliance VM Name</label>
                <input
                  type="text"
                  class="input input-sm input-bordered w-full font-mono"
                  value={@deploy_form["name"]}
                  phx-change="update_deploy_field"
                  phx-value-field="name"
                />
              </div>
              <div>
                <label class="label text-xs font-semibold">Storage Container</label>
                <input
                  type="text"
                  class="input input-sm input-bordered w-full font-mono"
                  value={@deploy_form["container"]}
                  phx-change="update_deploy_field"
                  phx-value-field="container"
                />
              </div>
            </div>

            <div class="grid grid-cols-2 gap-4">
              <div>
                <label class="label text-xs font-semibold">Static IPv4 &amp; Prefix</label>
                <input
                  type="text"
                  class="input input-sm input-bordered w-full font-mono"
                  value={@deploy_form["ip"]}
                  phx-change="update_deploy_field"
                  phx-value-field="ip"
                />
              </div>
              <div>
                <label class="label text-xs font-semibold">Default Gateway</label>
                <input
                  type="text"
                  class="input input-sm input-bordered w-full font-mono"
                  value={@deploy_form["gateway"]}
                  phx-change="update_deploy_field"
                  phx-value-field="gateway"
                />
              </div>
            </div>

            <div class="grid grid-cols-2 gap-4">
              <div>
                <label class="label text-xs font-semibold">DNS Nameserver</label>
                <input
                  type="text"
                  class="input input-sm input-bordered w-full font-mono"
                  value={@deploy_form["dns"]}
                  phx-change="update_deploy_field"
                  phx-value-field="dns"
                />
              </div>
              <div>
                <label class="label text-xs font-semibold">Admin Credentials</label>
                <input
                  type="password"
                  class="input input-sm input-bordered w-full font-mono"
                  value={@deploy_form["admin_password"]}
                  phx-change="update_deploy_field"
                  phx-value-field="admin_password"
                />
              </div>
            </div>

            <div class="alert alert-info alert-soft text-xs">
              <.icon name="hero-information-circle" class="size-4 shrink-0" />
              <span>
                Vali will clone the Helios Central golden image into a dedicated Sidon vdisk, synthesize the Cloud-Init NoCloud ISO, and power on the VM.
              </span>
            </div>
          </div>

          <div class="modal-action border-t border-base-content/10 pt-4">
            <button phx-click="close_deploy_modal" class="btn btn-sm btn-ghost">Cancel</button>
            <button phx-click="trigger_deploy" class="btn btn-sm btn-primary gap-1.5 shadow-md shadow-primary/20" id="confirm-deploy-btn">
              <.icon name="hero-rocket-launch" class="size-4" /> Deploy Appliance Now
            </button>
          </div>
        </div>
      </div>

      <%!-- ENROLL CLUSTER MODAL --%>
      <div :if={@enroll_modal_open?} class="modal modal-open backdrop-blur-sm" id="enroll-modal">
        <div class="modal-box max-w-md bg-base-200 border border-accent/30 shadow-2xl rounded-2xl">
          <div class="flex items-center justify-between pb-4 border-b border-base-content/10">
            <h3 class="font-bold text-lg flex items-center gap-2">
              <.icon name="hero-link" class="size-5 text-accent" />
              <span>Connect External Helios Cluster</span>
            </h3>
            <button phx-click="close_enroll_modal" class="btn btn-sm btn-circle btn-ghost">✕</button>
          </div>

          <div class="space-y-4 py-4">
            <div>
              <label class="label text-xs font-semibold">Cluster Identifier</label>
              <input
                type="text"
                placeholder="e.g. cluster-beta-02"
                class="input input-sm input-bordered w-full font-mono"
                value={@enroll_form["cluster_id"]}
                phx-change="update_enroll_field"
                phx-value-field="cluster_id"
              />
            </div>

            <div>
              <label class="label text-xs font-semibold">Cluster Display Name</label>
              <input
                type="text"
                placeholder="e.g. Beta Datacenter Edge"
                class="input input-sm input-bordered w-full"
                value={@enroll_form["name"]}
                phx-change="update_enroll_field"
                phx-value-field="name"
              />
            </div>

            <div class="grid grid-cols-3 gap-2">
              <div class="col-span-2">
                <label class="label text-xs font-semibold">Virtual IP (VIP)</label>
                <input
                  type="text"
                  placeholder="10.10.103.45"
                  class="input input-sm input-bordered w-full font-mono"
                  value={@enroll_form["vip"]}
                  phx-change="update_enroll_field"
                  phx-value-field="vip"
                />
              </div>
              <div>
                <label class="label text-xs font-semibold">API Port</label>
                <input
                  type="number"
                  class="input input-sm input-bordered w-full font-mono"
                  value={@enroll_form["port"]}
                  phx-change="update_enroll_field"
                  phx-value-field="port"
                />
              </div>
            </div>

            <div>
              <label class="label text-xs font-semibold">Cluster mTLS Auth Token</label>
              <input
                type="password"
                placeholder="Secret cluster federation token"
                class="input input-sm input-bordered w-full font-mono"
                value={@enroll_form["auth_token"]}
                phx-change="update_enroll_field"
                phx-value-field="auth_token"
              />
            </div>
          </div>

          <div class="modal-action border-t border-base-content/10 pt-4">
            <button phx-click="close_enroll_modal" class="btn btn-sm btn-ghost">Cancel</button>
            <button phx-click="submit_enroll" class="btn btn-sm btn-accent gap-1.5" id="confirm-enroll-btn">
              <.icon name="hero-check" class="size-4" /> Enroll Cluster
            </button>
          </div>
        </div>
      </div>
    </Layouts.app>
    """
  end

  defp filtered_vms(vms, ""), do: vms
  defp filtered_vms(vms, query) do
    q = String.downcase(query)
    Enum.filter(vms, fn v ->
      String.contains?(String.downcase(v.name), q) or
      String.contains?(String.downcase(v.cluster_name), q) or
      String.contains?(String.downcase(v.state), q) or
      String.contains?(String.downcase(v.host_ip), q)
    end)
  end
end
