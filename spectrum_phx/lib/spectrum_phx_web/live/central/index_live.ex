defmodule SpectrumPhxWeb.Central.IndexLive do
  @moduledoc """
  Helios Central Fleet Control:
  Multi-cluster federation, global VM inventory, central content library,
  and Helios Central virtual appliance deployment.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Storage.Components, only: [bytes: 1]

  alias SpectrumPhx.Central
  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Images
  alias SpectrumPhx.Storage.Containers
  alias SpectrumPhx.Tasks

  @refresh_interval_ms 15_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(@refresh_interval_ms, self(), :refresh)
      Tasks.subscribe()
    end

    {:ok,
     socket
     |> assign(page_title: "Helios Central")
     |> assign(:active_tab, :fleet)
     |> assign(:selected_preset, "tiny")
     |> assign(:deploy_modal_open?, false)
     |> assign(:deploy_error, nil)
     |> assign(:deploying?, false)
     |> assign(:deploying_task_id, nil)
     |> assign(:active_deployment, nil)
     |> assign(:failed_deployment, nil)
     |> assign(:deployment_subtasks, [])
     |> assign(:enroll_modal_open?, false)
     |> assign(:enroll_error, nil)
     |> assign(:enrolling?, false)
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
       "ip" => "",
       "gateway" => "",
       "dns" => "",
       "admin_password" => "",
       "container" => "default-pool"
     })
     |> load_central_state()}
  end

  @impl true
  def handle_info(:refresh, socket) do
    {:noreply, load_central_state(socket)}
  end

  def handle_info({:tasks, _snapshot}, socket) do
    {:noreply, load_central_state(socket)}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket) do
    {:noreply, load_central_state(socket)}
  end

  def handle_event("switch_tab", %{"tab" => tab}, socket) do
    tab_atom =
      case tab do
        "fleet" -> :fleet
        "vms" -> :vms
        "library" -> :library
        "setup" -> :setup
        _ -> :fleet
      end

    {:noreply, assign(socket, :active_tab, tab_atom)}
  end

  def handle_event("select_preset", %{"preset" => preset}, socket) do
    {:noreply, assign(socket, :selected_preset, preset)}
  end

  def handle_event("open_deploy_modal", _params, socket) do
    {:noreply, assign(socket, deploy_modal_open?: true, deploy_error: nil, deploying?: false)}
  end

  def handle_event("close_deploy_modal", _params, socket) do
    {:noreply, assign(socket, deploy_modal_open?: false, deploy_error: nil, deploying?: false)}
  end

  def handle_event("open_enroll_modal", _params, socket) do
    {:noreply, assign(socket, enroll_modal_open?: true, enroll_error: nil, enrolling?: false)}
  end

  def handle_event("close_enroll_modal", _params, socket) do
    {:noreply, assign(socket, enroll_modal_open?: false, enroll_error: nil, enrolling?: false)}
  end

  def handle_event("update_deploy_form", params, socket) do
    new_form = Map.merge(socket.assigns.deploy_form, params)
    {:noreply, assign(socket, deploy_form: new_form, deploy_error: nil)}
  end

  def handle_event("update_deploy_field", %{"field" => field, "value" => value}, socket) do
    new_form = Map.put(socket.assigns.deploy_form, field, value)
    {:noreply, assign(socket, deploy_form: new_form, deploy_error: nil)}
  end

  def handle_event("update_enroll_form", params, socket) do
    new_form = Map.merge(socket.assigns.enroll_form, params)
    {:noreply, assign(socket, enroll_form: new_form, enroll_error: nil)}
  end

  def handle_event("update_enroll_field", %{"field" => field, "value" => value}, socket) do
    new_form = Map.put(socket.assigns.enroll_form, field, value)
    {:noreply, assign(socket, enroll_form: new_form, enroll_error: nil)}
  end

  def handle_event("submit_enroll", params, socket) do
    form =
      socket.assigns.enroll_form
      |> Map.merge(params)

    vip = String.trim(form["vip"] || "")

    if vip == "" do
      {:noreply, assign(socket, enroll_error: "VIP address is required to enroll a member cluster.", enrolling?: false)}
    else
      socket = assign(socket, enrolling?: true, enroll_error: nil)

      case Central.enroll_cluster(form) do
        {:ok, _id} ->
          {:noreply,
           socket
           |> assign(enroll_modal_open?: false, enroll_error: nil, enrolling?: false)
           |> put_flash(:info, "Cluster #{form["name"] || form["vip"]} successfully enrolled into Central.")
           |> load_central_state()}

        {:error, reason} ->
          error_msg =
            case reason do
              err when is_binary(err) -> err
              err -> inspect(err)
            end

          {:noreply,
           socket
           |> assign(enroll_error: error_msg, enrolling?: false)
           |> put_flash(:error, "Failed to enroll cluster: #{error_msg}")}
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

  def handle_event("trigger_deploy", params, socket) do
    form_params =
      socket.assigns.deploy_form
      |> Map.merge(params)
      |> Map.put("preset", socket.assigns.selected_preset)

    name = String.trim(form_params["name"] || "helios-central")
    ip = String.trim(form_params["ip"] || "")
    gateway = String.trim(form_params["gateway"] || "")
    dns = String.trim(form_params["dns"] || "")
    admin_password = String.trim(form_params["admin_password"] || "")
    container = String.trim(form_params["container"] || "default-pool")

    cond do
      ip == "" ->
        {:noreply,
         socket
         |> assign(deploy_error: "Appliance IPv4 address with CIDR prefix is required (e.g. 10.10.102.75/24).", deploying?: false)
         |> put_flash(:error, "Failed to deploy Central Appliance: Appliance IPv4 address with CIDR prefix is required.")}

      gateway == "" ->
        {:noreply,
         socket
         |> assign(deploy_error: "Default gateway address is required (e.g. 10.10.102.1).", deploying?: false)
         |> put_flash(:error, "Failed to deploy Central Appliance: Default gateway address is required.")}

      admin_password == "" ->
        {:noreply,
         socket
         |> assign(deploy_error: "Admin password is required.", deploying?: false)
         |> put_flash(:error, "Failed to deploy Central Appliance: Admin password is required.")}

      true ->
        deploy_payload = %{
          "name" => if(name == "", do: "helios-central", else: name),
          "ip" => ip,
          "gateway" => gateway,
          "dns" => if(dns == "", do: gateway, else: dns),
          "admin_password" => admin_password,
          "container" => container,
          "preset" => socket.assigns.selected_preset
        }

        socket = assign(socket, deploying?: true, deploy_error: nil)

        case Central.start_deploy_workflow(deploy_payload) do
          {:ok, %{"task_id" => task_id}} ->
            {:noreply,
             socket
             |> assign(deploy_modal_open?: false, deploy_error: nil, deploying?: false, deploying_task_id: task_id)
             |> put_flash(:info, "Helios Central deployment task queued (Task ID: #{String.slice(task_id, 0, 8)}). Initiating multi-step provisioning workflow...")
             |> load_central_state()}

          {:error, reason} ->
            error_msg = reason

            {:noreply,
             socket
             |> assign(deploy_error: error_msg, deploying?: false)
             |> put_flash(:error, "Failed to deploy Central Appliance: #{error_msg}")}
        end
    end
  end

  def handle_event("search_vms", %{"query" => query}, socket) do
    {:noreply, assign(socket, :search_query, String.trim(query))}
  end

  defp load_central_state(socket) do
    clusters = Central.list_clusters()
    summary = Central.fleet_summary(clusters)
    presets = Central.sizing_presets()
    {status_type, appliance} = Central.deployment_status()

    images =
      case Images.list_images() do
        {:ok, list} -> list
        _ -> []
      end

    containers =
      case Containers.list() do
        {:ok, list} -> list
        _ -> []
      end

    container_names = Enum.map(containers, & &1.name)
    current_container = socket.assigns.deploy_form["container"]
    selected_container =
      cond do
        current_container in container_names ->
          current_container
        containers != [] ->
          hd(containers).name
        true ->
          Containers.default_name()
      end

    deploy_form = Map.put(socket.assigns.deploy_form, "container", selected_container)

    # Load local VMs for cross-cluster inventory view
    local_vms =
      case Hydra.query("SELECT name, state, vcpu, memory, host_ip, affinity_group FROM hydra.vms;") do
        {:ok, list} when is_list(list) ->
          Enum.map(list, fn v ->
            %{
              name: v["name"] || "unknown",
              cluster_name: Config.all()[:cluster_name] || "hci-01",
              cluster_vip: Config.vip() || "",
              state: v["state"] || "unknown",
              vcpu: v["vcpu"] || 0,
              memory: v["memory"] || 0,
              host_ip: v["host_ip"] || "",
              affinity: v["affinity_group"]
            }
          end)

        _ ->
          []
      end

    allocated_vcpus = Enum.sum(Enum.map(local_vms, & &1.vcpu))
    vcpu_percent =
      if summary.total_cpu > 0 do
        min(100, round(allocated_vcpus * 100 / summary.total_cpu))
      else
        0
      end

    appliance_ip =
      if is_map(appliance) and is_binary(appliance["ip"]) do
        String.split(appliance["ip"], "/") |> hd()
      else
        nil
      end

    tasks_snap = Tasks.fetch(limit: 50)

    active_deployment =
      Enum.find(tasks_snap.tasks, fn t ->
        t.component == "central" and t.action == "deploy" and Tasks.active?(t)
      end)

    failed_deployment =
      if is_nil(active_deployment) do
        Enum.find(tasks_snap.tasks, fn t ->
          t.component == "central" and t.action == "deploy" and t.state == :failed
        end)
      else
        nil
      end

    subtasks =
      if active_deployment do
        tasks_snap.tasks
        |> Enum.filter(fn t -> t.parent_id == active_deployment.id end)
        |> Enum.sort_by(& &1.created_at, {:asc, DateTime})
      else
        []
      end

    central_deployed? = status_type == :ok or length(clusters) > 0

    socket
    |> assign(:clusters, clusters)
    |> assign(:summary, summary)
    |> assign(:presets, presets)
    |> assign(:appliance_status, status_type)
    |> assign(:appliance, appliance)
    |> assign(:appliance_ip, appliance_ip)
    |> assign(:central_deployed?, central_deployed?)
    |> assign(:active_deployment, active_deployment)
    |> assign(:failed_deployment, failed_deployment)
    |> assign(:deployment_subtasks, subtasks)
    |> assign(:vms, local_vms)
    |> assign(:allocated_vcpus, allocated_vcpus)
    |> assign(:vcpu_percent, vcpu_percent)
    |> assign(:images, images)
    |> assign(:containers, containers)
    |> assign(:deploy_form, deploy_form)
    |> assign(:read_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:central}>
      <%!-- Top Header --%>
      <div class="relative overflow-hidden rounded-2xl bg-base-200/80 p-6 border border-base-content/10 shadow-lg mb-6">
        <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
          <div class="flex items-center gap-3">
            <div class="size-10 rounded-xl bg-primary/10 border border-primary/20 flex items-center justify-center text-primary">
              <.icon name="hero-globe-alt" class="size-6" />
            </div>
            <div>
              <div class="flex items-center gap-2">
                <h1 class="text-xl font-bold tracking-tight">Helios Central</h1>
                <span :if={@central_deployed? and @appliance_ip} class="badge badge-sm badge-success gap-1">
                  <span class="size-1.5 rounded-full bg-success animate-ping"></span>
                  Appliance Online
                </span>
                <span :if={!@central_deployed?} class="badge badge-sm badge-ghost border border-base-content/20 text-base-content/60 font-mono">
                  Not Deployed
                </span>
              </div>
              <p class="text-xs text-base-content/70 mt-0.5">
                Multi-cluster federation and fleet orchestration.
              </p>
            </div>
          </div>

          <div class="flex items-center gap-2">
            <a
              :if={@central_deployed? and @appliance_ip}
              href={"https://#{@appliance_ip}:8443/"}
              target="_blank"
              class="btn btn-sm btn-primary gap-1.5 shadow-md shadow-primary/25"
              id="launch-central-btn"
            >
              <.icon name="hero-arrow-top-right-on-square" class="size-4" /> Open Central Console &nearr;
            </a>
            <button
              phx-click="refresh"
              class="btn btn-sm btn-ghost gap-1.5"
              id="central-refresh-btn"
            >
              <.icon name="hero-arrow-path" class="size-4" /> Refresh
            </button>
          </div>
        </div>

        <%!-- Fleet Metrics Deck (Rendered only when Central is deployed and managing clusters) --%>
        <div :if={@central_deployed?} class="grid grid-cols-2 lg:grid-cols-4 gap-4 mt-6 pt-6 border-t border-base-content/10">
          <div class="rounded-xl bg-base-100/60 p-4 border border-base-content/5">
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

          <div class="rounded-xl bg-base-100/60 p-4 border border-base-content/5">
            <div class="flex items-center justify-between text-xs text-base-content/70 mb-1">
              <span>Fleet vCPU Allocation</span>
              <.icon name="hero-cpu-chip" class="size-4 text-primary" />
            </div>
            <div class="text-2xl font-bold font-mono">
              {@allocated_vcpus} <span class="text-xs font-normal text-base-content/60 font-sans">/ {@summary.total_cpu} cores</span>
            </div>
            <div class="w-full bg-base-content/10 rounded-full h-1.5 mt-2 overflow-hidden">
              <div class="bg-gradient-to-r from-primary to-accent h-1.5 rounded-full" style={"width: #{@vcpu_percent}%"}></div>
            </div>
          </div>

          <div class="rounded-xl bg-base-100/60 p-4 border border-base-content/5">
            <div class="flex items-center justify-between text-xs text-base-content/70 mb-1">
              <span>Fleet Memory</span>
              <span class="text-[11px] font-mono text-accent">{@summary.ram_percent}%</span>
            </div>
            <div class="text-2xl font-bold font-mono">
              {round(@summary.used_ram_mb / 1024)} <span class="text-xs font-normal text-base-content/60 font-sans">/ {round(@summary.total_ram_mb / 1024)} GiB</span>
            </div>
            <div class="w-full bg-base-content/10 rounded-full h-1.5 mt-2 overflow-hidden">
              <div class="bg-accent h-1.5 rounded-full" style={"width: #{@summary.ram_percent}%"}></div>
            </div>
          </div>

          <div class="rounded-xl bg-base-100/60 p-4 border border-base-content/5">
            <div class="flex items-center justify-between text-xs text-base-content/70 mb-1">
              <span>Sidon Storage</span>
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

      <%!-- STATE A: HELIOS CENTRAL NOT DEPLOYED --%>
      <div :if={!@central_deployed?} class="space-y-6" id="central-not-deployed-view">
        <%!-- Failure banner from recent attempt --%>
        <div :if={@failed_deployment && is_nil(@active_deployment)} class="alert alert-error text-xs shadow-md rounded-xl" id="central-failed-alert">
          <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
          <div class="space-y-0.5">
            <div class="font-bold">Appliance Deployment Failed</div>
            <div class="opacity-90">{@failed_deployment.error || "Provisioning interrupted."}</div>
          </div>
          <.link navigate={~p"/tasks"} class="btn btn-xs btn-outline ml-auto">View Task in Console &rarr;</.link>
        </div>

        <%!-- IN-PROGRESS CATALYST DEPLOYMENT WORKFLOW --%>
        <div :if={@active_deployment} class="card bg-base-200 border border-primary/30 p-6 rounded-2xl shadow-xl space-y-6" id="central-deploying-view">
          <div class="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 pb-4 border-b border-base-content/10">
            <div class="flex items-center gap-3">
              <div class="size-11 rounded-xl bg-primary/10 border border-primary/20 flex items-center justify-center text-primary">
                <span class="loading loading-bars loading-sm"></span>
              </div>
              <div>
                <div class="flex items-center gap-2">
                  <h2 class="text-base font-bold tracking-tight">Deploying Helios Central Virtual Appliance</h2>
                  <span class="badge badge-sm badge-primary font-mono gap-1">
                    <span class="size-1.5 rounded-full bg-current animate-ping"></span>
                    Workflow Active
                  </span>
                </div>
                <p class="text-xs text-base-content/60 mt-0.5 font-mono">
                  Catalyst Task: {@active_deployment.short_id} &middot; {@active_deployment.label}
                </p>
              </div>
            </div>
            <div class="flex items-center gap-2">
              <.link navigate={~p"/tasks"} class="btn btn-sm btn-outline btn-primary gap-1.5 shadow-sm" id="view-catalyst-tasks-btn">
                <.icon name="hero-queue-list" class="size-4" /> View in Tasks Console &rarr;
              </.link>
            </div>
          </div>

          <div>
            <div class="flex justify-between items-center text-xs font-mono mb-2">
              <span class="text-base-content/70">Multi-Step Provisioning Progress</span>
              <span class="font-bold text-primary text-sm">{@active_deployment.progress}%</span>
            </div>
            <div class="w-full bg-base-content/10 rounded-full h-3 overflow-hidden">
              <div
                class="bg-gradient-to-r from-primary via-accent to-primary h-3 rounded-full transition-all duration-500"
                style={"width: #{@active_deployment.progress}%"}
              ></div>
            </div>
          </div>

          <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            <div
              :for={{action, title, desc} <- deploy_steps()}
              class={[
                "p-3.5 rounded-xl border transition-all text-xs flex flex-col justify-between",
                step_class(@deployment_subtasks, action)
              ]}
            >
              <div>
                <div class="flex items-center justify-between mb-1.5">
                  <span class="font-mono font-bold text-xs">{title}</span>
                  <.step_badge subtasks={@deployment_subtasks} action={action} />
                </div>
                <p class="text-[11px] text-base-content/60 leading-relaxed">{desc}</p>
              </div>

              <div :if={step_error(@deployment_subtasks, action)} class="mt-2 text-[10px] text-error font-mono truncate" title={step_error(@deployment_subtasks, action)}>
                {step_error(@deployment_subtasks, action)}
              </div>
            </div>
          </div>
        </div>

        <%!-- OPTION CARDS (Rendered when no active deployment workflow) --%>
        <div :if={is_nil(@active_deployment)} class="card bg-base-200 border border-base-content/10 p-6 rounded-2xl shadow-lg">
          <div class="pb-6 border-b border-base-content/10">
            <h2 class="text-lg font-bold tracking-tight">Helios Central is Not Deployed</h2>
            <p class="text-xs text-base-content/70 mt-1">
              This cluster is operating in standalone cluster mode (Element tier). Deploy the Helios Central virtual appliance
              to enable multi-cluster management, or connect this cluster to an existing Central instance.
            </p>
          </div>

          <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6">
            <%!-- Option 1: Deploy New Central Appliance --%>
            <div class="rounded-xl bg-base-100/50 p-5 border border-base-content/10 flex flex-col justify-between">
              <div>
                <div class="flex items-center justify-between mb-2">
                  <h3 class="font-bold text-sm flex items-center gap-2">
                    <.icon name="hero-server-stack" class="size-4 text-primary" />
                    Option 1: Deploy New Central Appliance
                  </h3>
                  <span class="badge badge-xs badge-primary font-mono">Virtual Appliance</span>
                </div>
                <p class="text-xs text-base-content/70 mb-4">
                  Deploys a dedicated Helios Central virtual machine on this cluster with automated Cloud-Init bootstrap.
                </p>

                <div class="grid grid-cols-3 gap-2 mb-4">
                  <div
                    :for={p <- @presets}
                    phx-click="select_preset"
                    phx-value-preset={p.id}
                    class={["p-2.5 rounded-lg border text-center cursor-pointer transition-all",
                      if(@selected_preset == p.id, do: "border-primary bg-primary/10", else: "border-base-content/10 bg-base-200/50 hover:border-base-content/30")]}
                  >
                    <div class="font-bold text-xs">{p.name |> String.split(" ") |> hd()}</div>
                    <div class="text-[11px] font-mono text-base-content/60 mt-0.5">{p.vcpus}C / {round(p.memory_mb / 1024)}G</div>
                    <div class="text-[10px] text-base-content/40 font-mono">{p.disk_gb}G Disk</div>
                  </div>
                </div>
              </div>

              <button phx-click="open_deploy_modal" class="btn btn-sm btn-primary gap-1.5 w-full shadow-sm" id="deploy-appliance-btn">
                <.icon name="hero-server-stack" class="size-4" /> Deploy Central Appliance
              </button>
            </div>

            <%!-- Option 2: Connect to Existing Central --%>
            <div class="rounded-xl bg-base-100/50 p-5 border border-base-content/10 flex flex-col justify-between">
              <div>
                <div class="flex items-center justify-between mb-2">
                  <h3 class="font-bold text-sm flex items-center gap-2">
                    <.icon name="hero-link" class="size-4 text-accent" />
                    Option 2: Connect to Existing Central
                  </h3>
                  <span class="badge badge-xs badge-accent font-mono">Federate</span>
                </div>
                <p class="text-xs text-base-content/70 mb-4">
                  Register this cluster into an existing Helios Central instance via its Virtual IP and federation token.
                </p>

                <div class="bg-base-200/50 p-3 rounded-lg border border-base-content/5 space-y-2 text-xs font-mono mb-4">
                  <div class="flex justify-between">
                    <span class="text-base-content/60">Local Cluster VIP:</span>
                    <span class="font-bold">{Config.vip() || "Not configured"}</span>
                  </div>
                  <div class="flex justify-between">
                    <span class="text-base-content/60">Local Hypervisor Nodes:</span>
                    <span class="font-bold">{length(SpectrumPhx.Settings.cluster_nodes())} nodes</span>
                  </div>
                  <div class="flex justify-between">
                    <span class="text-base-content/60">Federation Port:</span>
                    <span class="font-bold">8443 (mTLS)</span>
                  </div>
                </div>
              </div>

              <button phx-click="open_enroll_modal" class="btn btn-sm btn-outline btn-accent gap-1.5 w-full" id="enroll-cluster-btn">
                <.icon name="hero-plus-circle" class="size-4" /> Connect to Existing Central
              </button>
            </div>
          </div>
        </div>
      </div>

      <%!-- STATE B: HELIOS CENTRAL DEPLOYED --%>
      <div :if={@central_deployed?} class="space-y-6" id="central-deployed-view">
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
            <.icon name="hero-rectangle-stack" class="size-4" /> Central Content Library ({length(@images)})
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

        <%!-- TAB 1: FLEET CLUSTERS MATRIX --%>
        <div :if={@active_tab == :fleet} class="space-y-4" id="fleet-tab-content">
          <div :if={@clusters == []} class="p-12 text-center text-base-content/50 text-sm bg-base-200 rounded-xl border border-base-content/10">
            <.icon name="hero-circle-stack" class="size-8 mx-auto mb-2 opacity-40" />
            No member clusters currently enrolled in this Central fleet.
          </div>

          <div :if={@clusters != []} class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
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

          <div :if={@vms == []} class="p-12 text-center text-base-content/50 text-sm bg-base-200 rounded-xl border border-base-content/10">
            <.icon name="hero-computer-desktop" class="size-8 mx-auto mb-2 opacity-40" />
            No virtual machines provisioned across the fleet.
          </div>

          <div :if={@vms != []} class="overflow-x-auto rounded-xl border border-base-content/10 bg-base-200">
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
                  <td class="font-mono text-xs">{if vm.host_ip != "", do: vm.host_ip, else: "—"}</td>
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
              <p class="text-xs text-base-content/70">Replicated storage catalog registered in Sidon DFS and hydra.valhalla_images.</p>
            </div>
            <a href={~p"/images"} class="btn btn-sm btn-outline btn-primary gap-1">
              <.icon name="hero-arrow-up-tray" class="size-4" /> Manage Images Catalogue
            </a>
          </div>

          <div :if={@images == []} class="p-12 text-center text-base-content/50 text-sm bg-base-200 rounded-xl border border-base-content/10">
            <.icon name="hero-rectangle-stack" class="size-8 mx-auto mb-2 opacity-40" />
            No golden images or ISO templates registered in the Content Library.
            <div class="mt-2">
              <a href={~p"/images"} class="btn btn-xs btn-primary gap-1">
                <.icon name="hero-plus" class="size-3" /> Upload an Image
              </a>
            </div>
          </div>

          <div :if={@images != []} class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <div :for={img <- @images} class="card bg-base-200 border border-base-content/10 p-4 rounded-xl">
              <div class="flex items-center justify-between mb-2">
                <span class="badge badge-sm badge-info font-mono uppercase">{img.type}</span>
                <span :if={img.on_vdisk?} class="badge badge-xs badge-success font-mono">vDisk</span>
                <span :if={!img.on_vdisk?} class="badge badge-xs badge-ghost font-mono">File</span>
              </div>
              <h4 class="font-bold text-sm truncate" title={img.name}>{img.name}</h4>
              <div class="text-xs text-base-content/60 font-mono mt-1 truncate" title={img.filename}>
                {img.filename} ({bytes(img.size_bytes)})
              </div>
              <div class="text-[11px] text-base-content/50 mt-2 font-mono truncate" title={img.path}>
                {img.path}
              </div>
              <div class="mt-4 pt-3 border-t border-base-content/10 flex justify-between items-center">
                <span class="text-xs font-mono text-base-content/60">
                  {if img.created_at, do: Calendar.strftime(img.created_at, "%Y-%m-%d %H:%M"), else: "—"}
                </span>
                <a href={~p"/images"} class="btn btn-xs btn-ghost text-primary">View in Catalogue</a>
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
                  The autonomous fleet management virtual appliance for multi-cluster orchestration, DRS, and lifecycle operations.
                </p>
              </div>
              <button
                phx-click="open_deploy_modal"
                class="btn btn-primary gap-1.5 shadow-md shadow-primary/20"
              >
                <.icon name="hero-server-stack" class="size-4" /> Deploy Appliance
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
          </div>
        </div>
      </div>

      <%!-- DEPLOY MODAL --%>
      <div :if={@deploy_modal_open?} class="modal modal-open backdrop-blur-sm" id="deploy-modal">
        <div class="modal-box max-w-2xl bg-base-200 border border-primary/30 shadow-2xl rounded-2xl">
          <div class="flex items-center justify-between pb-4 border-b border-base-content/10">
            <h3 class="font-bold text-lg flex items-center gap-2">
              <.icon name="hero-server-stack" class="size-5 text-primary" />
              <span>Deploy Helios Central Appliance</span>
            </h3>
            <button type="button" phx-click="close_deploy_modal" class="btn btn-sm btn-circle btn-ghost">✕</button>
          </div>

          <form id="deploy-appliance-form" phx-change="update_deploy_form" phx-submit="trigger_deploy">
            <div class="space-y-4 py-4">
              <div :if={@deploy_error} class="alert alert-error text-xs py-2 px-3 rounded-lg shadow-sm" id="deploy-error-alert">
                <.icon name="hero-exclamation-triangle" class="size-4 shrink-0" />
                <span>{@deploy_error}</span>
              </div>

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
                    name="name"
                    placeholder="helios-central"
                    class="input input-sm input-bordered w-full font-mono"
                    value={@deploy_form["name"]}
                  />
                </div>
                <div>
                  <label class="label text-xs font-semibold">Storage Container</label>
                  <select
                    class="select select-sm select-bordered w-full font-mono"
                    name="container"
                  >
                    <option :if={@containers == []} value="" disabled selected>No storage containers found</option>
                    <option :for={c <- @containers} value={c.name} selected={@deploy_form["container"] == c.name}>
                      {c.name} ({c.tier}, FTT={c.ftt})
                    </option>
                  </select>
                </div>
              </div>

              <div class="grid grid-cols-2 gap-4">
                <div>
                  <label class="label text-xs font-semibold">Static IPv4 &amp; Prefix <span class="text-error">*</span></label>
                  <input
                    type="text"
                    name="ip"
                    placeholder="e.g. 10.10.102.75/24"
                    class="input input-sm input-bordered w-full font-mono"
                    value={@deploy_form["ip"]}
                    required
                  />
                </div>
                <div>
                  <label class="label text-xs font-semibold">Default Gateway <span class="text-error">*</span></label>
                  <input
                    type="text"
                    name="gateway"
                    placeholder="e.g. 10.10.102.1"
                    class="input input-sm input-bordered w-full font-mono"
                    value={@deploy_form["gateway"]}
                    required
                  />
                </div>
              </div>

              <div class="grid grid-cols-2 gap-4">
                <div>
                  <label class="label text-xs font-semibold">DNS Nameserver</label>
                  <input
                    type="text"
                    name="dns"
                    placeholder="e.g. 10.10.102.1"
                    class="input input-sm input-bordered w-full font-mono"
                    value={@deploy_form["dns"]}
                  />
                </div>
                <div>
                  <label class="label text-xs font-semibold">Admin Password <span class="text-error">*</span></label>
                  <input
                    type="password"
                    name="admin_password"
                    placeholder="Enter initial appliance password"
                    class="input input-sm input-bordered w-full font-mono"
                    value={@deploy_form["admin_password"]}
                    required
                  />
                </div>
              </div>
            </div>

            <div class="modal-action border-t border-base-content/10 pt-4">
              <button type="button" phx-click="close_deploy_modal" class="btn btn-sm btn-ghost" disabled={@deploying?}>Cancel</button>
              <button
                type="submit"
                class="btn btn-sm btn-primary gap-1.5 shadow-md shadow-primary/20"
                id="confirm-deploy-btn"
                disabled={@deploying?}
              >
                <span :if={@deploying?} class="loading loading-spinner loading-xs"></span>
                <.icon :if={!@deploying?} name="hero-check" class="size-4" />
                {if @deploying?, do: "Deploying Appliance...", else: "Deploy Appliance Now"}
              </button>
            </div>
          </form>
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
            <button type="button" phx-click="close_enroll_modal" class="btn btn-sm btn-circle btn-ghost">✕</button>
          </div>

          <form id="enroll-cluster-form" phx-change="update_enroll_form" phx-submit="submit_enroll">
            <div class="space-y-4 py-4">
              <div :if={@enroll_error} class="alert alert-error text-xs py-2 px-3 rounded-lg shadow-sm" id="enroll-error-alert">
                <.icon name="hero-exclamation-triangle" class="size-4 shrink-0" />
                <span>{@enroll_error}</span>
              </div>

              <div>
                <label class="label text-xs font-semibold">Cluster Identifier</label>
                <input
                  type="text"
                  name="cluster_id"
                  placeholder="e.g. cluster-beta-02"
                  class="input input-sm input-bordered w-full font-mono"
                  value={@enroll_form["cluster_id"]}
                />
              </div>

              <div>
                <label class="label text-xs font-semibold">Cluster Display Name</label>
                <input
                  type="text"
                  name="name"
                  placeholder="e.g. Beta Datacenter Edge"
                  class="input input-sm input-bordered w-full"
                  value={@enroll_form["name"]}
                />
              </div>

              <div class="grid grid-cols-3 gap-2">
                <div class="col-span-2">
                  <label class="label text-xs font-semibold">Virtual IP (VIP) <span class="text-error">*</span></label>
                  <input
                    type="text"
                    name="vip"
                    placeholder="e.g. 10.10.103.45"
                    class="input input-sm input-bordered w-full font-mono"
                    value={@enroll_form["vip"]}
                    required
                  />
                </div>
                <div>
                  <label class="label text-xs font-semibold">API Port</label>
                  <input
                    type="number"
                    name="port"
                    placeholder="8443"
                    class="input input-sm input-bordered w-full font-mono"
                    value={@enroll_form["port"]}
                  />
                </div>
              </div>

              <div>
                <label class="label text-xs font-semibold">Cluster mTLS Auth Token</label>
                <input
                  type="password"
                  name="auth_token"
                  placeholder="Secret cluster federation token"
                  class="input input-sm input-bordered w-full font-mono"
                  value={@enroll_form["auth_token"]}
                />
              </div>
            </div>

            <div class="modal-action border-t border-base-content/10 pt-4">
              <button type="button" phx-click="close_enroll_modal" class="btn btn-sm btn-ghost" disabled={@enrolling?}>Cancel</button>
              <button type="submit" class="btn btn-sm btn-accent gap-1.5" id="confirm-enroll-btn" disabled={@enrolling?}>
                <span :if={@enrolling?} class="loading loading-spinner loading-xs"></span>
                <.icon :if={!@enrolling?} name="hero-check" class="size-4" />
                {if @enrolling?, do: "Connecting...", else: "Enroll Cluster"}
              </button>
            </div>
          </form>
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

  defp deploy_steps do
    [
      {"allocate_storage", "01 / Storage Allocation", "Sidon vdisk allocation on storage container"},
      {"stage_image", "02 / Golden Image Staging", "Writing Rocky Linux 10 OS image into Sidon extent store"},
      {"inject_cloud_init", "03 / Cloud-Init Synthesis", "Injecting static IP, gateway & credentials"},
      {"register_domain", "04 / Domain Registration", "Registering UEFI OVMF domain in Hydra"},
      {"power_on", "05 / Hypervisor Power-On", "Vali host placement & VM boot"},
      {"verify_services", "06 / Fleet Auto-Pairing", "Health check & mutual cluster federation trust"}
    ]
  end

  defp step_task(subtasks, action), do: Enum.find(subtasks, fn s -> s.action == action end)

  defp step_state(subtasks, action) do
    case step_task(subtasks, action) do
      %{state: state} -> state
      _ -> :pending
    end
  end

  defp step_error(subtasks, action) do
    case step_task(subtasks, action) do
      %{error: err} when is_binary(err) and err != "" -> err
      _ -> nil
    end
  end

  defp step_class(subtasks, action) do
    case step_state(subtasks, action) do
      :completed -> "border-success/30 bg-success/5"
      :processing -> "border-primary/40 bg-primary/10 ring-1 ring-primary/30"
      :failed -> "border-error/40 bg-error/10"
      _ -> "border-base-content/10 bg-base-100/40 opacity-70"
    end
  end

  defp step_badge(assigns) do
    state = step_state(assigns.subtasks, assigns.action)
    assigns = assign(assigns, :state, state)

    ~H"""
    <span :if={@state == :completed} class="badge badge-xs badge-success gap-1 font-mono">
      <.icon name="hero-check" class="size-3" /> Done
    </span>
    <span :if={@state == :processing} class="badge badge-xs badge-primary gap-1 font-mono">
      <span class="loading loading-spinner loading-[10px]"></span> Running
    </span>
    <span :if={@state == :failed} class="badge badge-xs badge-error gap-1 font-mono">
      <.icon name="hero-x-mark" class="size-3" /> Failed
    </span>
    <span :if={@state == :pending} class="badge badge-xs badge-ghost font-mono opacity-50">
      Pending
    </span>
    """
  end
end
