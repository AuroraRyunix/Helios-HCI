defmodule SpectrumPhxWeb.Vms.ShowLive do
  @moduledoc """
  One VM at `/vms/:name`: specs, disks, placement, power state, and live hardware operations.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Vms.Components, except: [console_page: 1, console_label: 1]

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Vms
  alias SpectrumPhx.Vms.Options
  alias SpectrumPhx.Vms.Vm

  @poll_interval 3_000

  @impl true
  def mount(%{"name" => name}, _session, socket) do
    if connected?(socket) do
      Vms.subscribe()
      :timer.send_interval(@poll_interval, :poll)
    end

    case Vms.get_vm(name) do
      {:ok, vm} ->
        options = Options.load(probe_hosts: connected?(socket))

        {:ok,
         assign(socket,
           vm: vm,
           disks: Vm.disks(vm),
           nics: vm_nics(vm),
           name: vm.name,
           page_title: vm.name,
           options: options,
           db_error: nil
         )}

      {:error, reason} ->
        {:ok,
         socket
         |> put_flash(:error, error_message(reason))
         |> push_navigate(to: ~p"/vms")}
    end
  end

  @impl true
  def handle_info(:poll, socket), do: {:noreply, reload(socket)}
  def handle_info({:vm_updated, _name}, socket), do: {:noreply, reload(socket)}
  def handle_info({:vm_created, _name}, socket), do: {:noreply, socket}
  def handle_info({:vm_task_submitted, _name, _action}, socket), do: {:noreply, reload(socket)}

  @impl true
  def handle_event("power_on", _params, socket) do
    {:noreply, act(socket, "Start requested.", fn -> Vms.power_on(socket.assigns.name) end)}
  end

  def handle_event("power_off", _params, socket) do
    {:noreply, act(socket, "Stop requested.", fn -> Vms.power_off(socket.assigns.name) end)}
  end

  def handle_event("reboot", _params, socket) do
    {:noreply, act(socket, "Reboot requested.", fn -> Vms.reboot(socket.assigns.name) end)}
  end

  def handle_event("delete_vm", params, socket) do
    force = params["force"] == "true" or Vm.running?(socket.assigns.vm)
    keep_disks = params["keep_disks"] == "true"
    name = socket.assigns.name

    case Vms.delete_vm(name, force: force, keep_disks: keep_disks) do
      {:ok, _result} ->
        {:noreply,
         socket
         |> put_flash(:info, "VM #{name} deleted successfully.")
         |> push_navigate(to: ~p"/vms")}

      {:error, reason} ->
        {:noreply,
         socket
         |> put_flash(:error, "#{name}: #{error_message(reason)}")
         |> reload()}
    end
  end

  # Live hardware mutator event handlers
  def handle_event("live_vcpus", %{"count" => count}, socket) do
    count = String.to_integer(count)
    {:noreply, act(socket, "vCPUs updated to #{count}.", fn ->
      Vms.live_change(socket.assigns.name, %{"op" => "vcpus", "count" => count})
    end)}
  end

  def handle_event("live_memory", %{"mib" => mib}, socket) do
    mib = String.to_integer(mib)
    {:noreply, act(socket, "Memory balloon updated to #{mib} MiB.", fn ->
      Vms.live_change(socket.assigns.name, %{"op" => "memory", "mib" => mib})
    end)}
  end

  def handle_event("live_cdrom", %{"image" => image}, socket) do
    img = if image in ["", nil, "__empty__"], do: nil, else: image
    msg = if img, do: "Mounted #{img} to CD-ROM.", else: "CD-ROM ejected."
    {:noreply, act(socket, msg, fn ->
      Vms.live_change(socket.assigns.name, %{"op" => "cdrom", "slot" => 0, "image" => img})
    end)}
  end

  def handle_event("live_cdrom_eject", _params, socket) do
    {:noreply, act(socket, "CD-ROM ejected.", fn ->
      Vms.live_change(socket.assigns.name, %{"op" => "cdrom", "slot" => 0, "image" => nil})
    end)}
  end

  def handle_event("live_attach_disk", %{"size_gib" => size, "container" => container}, socket) do
    size = String.to_integer(size)
    {:noreply, act(socket, "Attached #{size} GiB virtual disk.", fn ->
      Vms.live_change(socket.assigns.name, %{
        "op" => "disk",
        "action" => "attach",
        "size_gib" => size,
        "container" => (if container == "", do: nil, else: container)
      })
    end)}
  end

  def handle_event("live_grow_disk", %{"index" => idx, "size_gib" => size}, socket) do
    idx = String.to_integer(idx)
    size = String.to_integer(size)
    {:noreply, act(socket, "Disk #{idx + 1} grown to #{size} GiB.", fn ->
      Vms.live_change(socket.assigns.name, %{
        "op" => "disk",
        "action" => "resize",
        "index" => idx,
        "size_gib" => size
      })
    end)}
  end

  def handle_event("live_attach_nic", %{"network_id" => net, "model" => model}, socket) do
    {:noreply, act(socket, "Network interface hot-plugged.", fn ->
      Vms.live_change(socket.assigns.name, %{
        "op" => "nic",
        "action" => "attach",
        "network_id" => net,
        "model" => (if model == "", do: "virtio", else: model)
      })
    end)}
  end

  def handle_event("live_detach_nic", %{"index" => idx}, socket) do
    idx = String.to_integer(idx)
    {:noreply, act(socket, "Removed network interface.", fn ->
      Vms.live_change(socket.assigns.name, %{
        "op" => "nic",
        "action" => "detach",
        "index" => idx
      })
    end)}
  end

  def handle_event("live_link_nic", %{"index" => idx, "state" => st}, socket) do
    idx = String.to_integer(idx)
    {:noreply, act(socket, "NIC #{idx + 1} link state set to #{st}.", fn ->
      Vms.live_change(socket.assigns.name, %{
        "op" => "nic",
        "action" => "link",
        "index" => idx,
        "state" => st
      })
    end)}
  end

  def handle_event("live_change_nic", %{"index" => idx, "network_id" => net}, socket) do
    idx = String.to_integer(idx)
    {:noreply, act(socket, "NIC #{idx} moved.", fn ->
      Vms.live_change(socket.assigns.name, %{"op" => "nic", "action" => "change", "index" => idx, "network_id" => net})
    end)}
  end

  def handle_event("live_cdrom_slot", %{"slot" => slot} = params, socket) do
    slot = String.to_integer(slot)
    img = if params["image"] in ["", nil, "__empty__"], do: nil, else: params["image"]
    {:noreply, act(socket, "CD-ROM updated.", fn ->
      Vms.live_change(socket.assigns.name, %{"op" => "cdrom", "slot" => slot, "image" => img})
    end)}
  end

  # One form for compute: live operations while running, an offline edit while stopped.
  def handle_event("save_compute", params, socket) do
    vm = socket.assigns.vm

    if Vm.running?(vm) do
      vcpu = to_int(params["vcpu"], vm.vcpu)
      mem = to_int(params["memory"], vm.memory)

      socket =
        socket
        |> then(&if(vcpu != vm.vcpu, do: act(&1, "", fn -> Vms.live_change(vm.name, %{"op" => "vcpus", "count" => vcpu}) end), else: &1))
        |> then(&if(mem != vm.memory, do: act(&1, "", fn -> Vms.live_change(vm.name, %{"op" => "memory", "mib" => mem}) end), else: &1))

      {:noreply, socket}
    else
      edit =
        %{
          "vcpu" => params["vcpu"],
          "memory" => params["memory"],
          "firmware" => params["firmware"] || vm.firmware,
          "cpu_model" => params["cpu_model"] || vm.cpu_model,
          "boot_device" => params["boot_device"] || vm.boot_device,
          "graphics" => params["graphics"] || vm.graphics,
          "audio_enabled" => params["audio_enabled"] == "true",
          "disks" => vm.disks_list,
          "iso" => vm.iso,
          "network_id" => vm.network_id
        }

      case Vms.update_vm(vm.name, edit) do
        {:ok, _} -> {:noreply, reload(socket)}
        {:error, reason} -> {:noreply, socket |> put_flash(:error, error_message(reason)) |> reload()}
      end
    end
  end

  defp to_int(nil, default), do: default
  defp to_int(v, default) when is_binary(v) do
    case Integer.parse(v) do
      {n, _} -> n
      :error -> default
    end
  end
  defp to_int(v, _default) when is_integer(v), do: v

  defp act(socket, message, fun) do
    case fun.() do
      # No info toast: the task ring in the header announces the task while it runs.
      {:ok, _result} -> _ = message; reload(socket)
      {:error, reason} -> socket |> put_flash(:error, error_message(reason)) |> reload()
    end
  end

  defp reload(socket) do
    case Vms.get_vm(socket.assigns.name) do
      {:ok, vm} ->
        assign(socket,
          vm: vm,
          disks: Vm.disks(vm),
          nics: vm_nics(vm),
          db_error: nil
        )

      {:error, :not_found} ->
        assign(socket, db_error: :not_found)

      {:error, reason} ->
        assign(socket, db_error: reason)
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:vms}>
      <.header>
        <span class="font-mono text-2xl font-bold">{@vm.name}</span>
        <:subtitle>
          <span class="flex items-center gap-2 mt-1">
            <.state_badge vm={@vm} />
            <.lock_badge vm={@vm} />
            <span :if={Vm.placed?(@vm)} class="text-xs text-base-content/60">
              Host: {Config.hostname_for(@vm.host_ip)} (<code>{@vm.host_ip}</code>)
            </span>
          </span>
        </:subtitle>
        <:actions>
          <.button navigate={~p"/vms"} variant="secondary">
            <.icon name="hero-arrow-left-solid" class="size-4" /> Back to VMs
          </.button>
        </:actions>
      </.header>

      <div
        :if={Vm.migrating?(@vm)}
        id="migration-lock"
        class="mt-4 rounded-lg border border-amber-500/40 bg-amber-500/10 p-4 text-sm text-amber-700"
      >
        <p class="font-semibold flex items-center gap-2">
          <.icon name="hero-lock-closed-micro" class="size-4" /> Migration lock held
        </p>
        <p class="mt-1">
          This VM's <code>status</code> column reads <code>{@vm.status}</code>, so a migration is in flight. Lifecycle
          operations are refused until it clears.
        </p>
      </div>

      <div
        :if={@db_error}
        id="vm-db-error"
        class="mt-4 rounded-lg border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-700"
      >
        Showing the last known state: Hydra is not answering ({inspect(@db_error)}).
      </div>

      <!-- Action Buttons Bar -->
      <div class="mt-6 flex flex-wrap items-center gap-3 p-4 rounded-xl glass-card">
        <.button
          :if={not Vm.running?(@vm)}
          id="start"
          phx-click="power_on"
          phx-disable-with="Starting..."
          disabled={Vm.migrating?(@vm)}
          variant="success"
        >
          <.icon name="hero-play-solid" class="size-4" /> Start
        </.button>

        <.button
          :if={Vm.running?(@vm)}
          id="stop"
          phx-click="power_off"
          phx-disable-with="Stopping..."
          disabled={Vm.migrating?(@vm)}
          variant="warning"
        >
          <.icon name="hero-stop-solid" class="size-4" /> Stop
        </.button>

        <.button
          :if={Vm.running?(@vm)}
          id="reboot"
          phx-click="reboot"
          phx-disable-with="Rebooting..."
          disabled={Vm.migrating?(@vm)}
          variant="secondary"
        >
          <.icon name="hero-arrow-path-solid" class="size-4" /> Reboot
        </.button>

        <.button
          :if={not Vm.running?(@vm)}
          id="edit"
          navigate={~p"/vms/#{@vm.name}/edit"}
          variant="primary"
        >
          <.icon name="hero-pencil-square-solid" class="size-4" /> Edit VM
        </.button>

        <span
          :if={Vm.running?(@vm)}
          id="edit-hint"
          class="text-xs text-base-content/60 inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded bg-base-300/50"
        >
          <.icon name="hero-information-circle-solid" class="size-4 text-info" />
          Stop VM to edit offline definition. Live controls are active below.
        </span>

        <a
          :if={Vm.running?(@vm)}
          id="console"
          href={"/#{console_page(@vm)}?name=#{URI.encode_www_form(@vm.name)}"}
          target="_blank"
          rel="noopener"
          class="btn btn-info shadow-sm hover:shadow"
        >
          <.icon name="hero-computer-desktop-solid" class="size-4" />
          {console_label(@vm)}
        </a>

        <div class="flex-1"></div>

        <.button
          id="delete-vm"
          phx-click="delete_vm"
          phx-value-force={if Vm.running?(@vm), do: "true", else: "false"}
          phx-value-keep_disks="false"
          data-confirm={if Vm.running?(@vm),
            do: "VM #{@vm.name} is currently running! Are you sure you want to FORCE DESTROY and delete it and its storage?",
            else: "Are you sure you want to permanently delete VM #{@vm.name} and its virtual disks?"}
          phx-disable-with="Deleting..."
          disabled={Vm.migrating?(@vm)}
          variant="danger"
        >
          <.icon name="hero-trash-solid" class="size-4" />
          {if Vm.running?(@vm), do: "Force Destroy & Delete", else: "Delete VM"}
        </.button>
      </div>

      <% running = Vm.running?(@vm) %>
      <% offline_tip = "Only while the VM is stopped" %>
      <% cdroms = cdrom_slots(@vm) %>

      <!-- Hardware: one page, live controls enabled while running, offline-only controls greyed out -->
      <section id="vm-hardware" class="mt-6 grid gap-6 xl:grid-cols-3">
        <!-- Compute -->
        <form id="compute-form" phx-submit="save_compute" class="glass-card p-5 rounded-xl border border-base-content/10 space-y-3">
          <h3 class="panel-title flex items-center justify-between">
            <span>Compute</span>
            <span class={["badge badge-sm", if(running, do: "badge-success", else: "badge-ghost")]}>
              {if running, do: "live", else: "offline edit"}
            </span>
          </h3>

          <div class="grid grid-cols-2 gap-3">
            <label class="form-control">
              <span class="label-text text-xs opacity-70">vCPUs</span>
              <input type="number" name="vcpu" min="1" max="128" value={@vm.vcpu} class="input input-sm font-mono" />
            </label>
            <label class="form-control">
              <span class="label-text text-xs opacity-70">Memory ({@vm.memory} MiB)</span>
              <input type="number" name="memory" min="256" step="256" value={@vm.memory} class="input input-sm font-mono" />
            </label>
          </div>

          <fieldset disabled={running} title={running && offline_tip} class={["grid grid-cols-2 gap-3", running && "opacity-50 cursor-not-allowed"]}>
            <label class="form-control">
              <span class="label-text text-xs opacity-70">Firmware</span>
              <select name="firmware" class="select select-sm">
                <option :for={f <- Vm.firmwares()} value={f} selected={f == @vm.firmware}>{f}</option>
              </select>
            </label>
            <label class="form-control">
              <span class="label-text text-xs opacity-70">CPU model</span>
              <select name="cpu_model" class="select select-sm">
                <option :for={m <- Vm.cpu_models()} value={m} selected={m == @vm.cpu_model}>{blank(m, "host default")}</option>
              </select>
            </label>
            <label class="form-control">
              <span class="label-text text-xs opacity-70">Boot device</span>
              <select name="boot_device" class="select select-sm">
                <option :for={b <- Vm.boot_devices()} value={b} selected={b == @vm.boot_device}>{blank(b, "default order")}</option>
              </select>
            </label>
            <label class="form-control">
              <span class="label-text text-xs opacity-70">Graphics</span>
              <select name="graphics" class="select select-sm">
                <option :for={g <- Vm.graphics_types()} value={g} selected={g == @vm.graphics}>{blank(g, "vnc")}</option>
              </select>
            </label>
            <label class="label cursor-pointer justify-start gap-2 col-span-2">
              <input type="hidden" name="audio_enabled" value="false" />
              <input type="checkbox" name="audio_enabled" value="true" checked={@vm.audio_enabled} class="toggle toggle-sm" />
              <span class="label-text text-xs">ICH9 audio</span>
            </label>
          </fieldset>

          <div class="flex items-center justify-between pt-1">
            <span class="text-[11px] opacity-60">
              {if running, do: "vCPU hot-add and memory balloon apply live.", else: "Applied at next power-on."}
            </span>
            <button type="submit" class="btn btn-primary btn-sm" phx-disable-with="Applying…">Apply</button>
          </div>
        </form>

        <!-- Placement -->
        <div class="glass-card p-5 rounded-xl border border-base-content/10 xl:col-span-2">
          <h3 class="panel-title mb-3">Placement &amp; lifecycle</h3>
          <.list>
            <:item title="Host">
              <span :if={Vm.placed?(@vm)}>{Config.hostname_for(@vm.host_ip)} <span class="opacity-60">({@vm.host_ip})</span></span>
              <span :if={not Vm.placed?(@vm)} class="opacity-60">Unassigned. Placement is claimed when the VM starts.</span>
            </:item>
            <:item title="Power state">{@vm.state}</:item>
            <:item title="Lifecycle lock">{blank(@vm.status, "none")}</:item>
          </.list>
        </div>
      </section>

      <!-- Disks & media: one list, disks and CD-ROMs alike -->
      <section id="vm-disks" class="glass-card p-5 rounded-xl border border-base-content/10 mt-6">
        <h3 class="panel-title mb-3">Disks &amp; media</h3>
        <div class="overflow-x-auto">
          <table class="table table-sm w-full">
            <thead>
              <tr>
                <th>#</th><th>Type</th><th>Size / media</th><th>Container / bus</th><th>Target</th><th class="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              <tr :if={@disks == [] and cdroms == []}>
                <td colspan="6" class="text-center text-xs opacity-60 py-4">This VM has no disks registered.</td>
              </tr>
              <tr :for={disk <- @disks} id={"disk-row-#{disk.index}"}>
                <td class="font-mono">{disk.index}</td>
                <td><span class="badge badge-sm badge-primary badge-outline">Disk</span></td>
                <td class="font-mono">{disk.size}</td>
                <td>
                  <span class="badge badge-sm badge-secondary font-mono">{blank(disk.container, "default")}</span>
                  <span class="font-mono text-xs opacity-60 ml-1">{disk.resource}</span>
                </td>
                <td class="font-mono text-xs opacity-70">
                  <span>vd{<<?a + disk.index>>}</span>
                  <span class="block text-[10px] opacity-60">{disk.path}</span>
                </td>
                <td class="text-right">
                  <form phx-submit="live_grow_disk" class="join justify-end">
                    <input type="hidden" name="index" value={disk.index} />
                    <input type="number" name="size_gib" min={(disk.size_gib || 0) + 1} value={(disk.size_gib || 0) + 5}
                      disabled={not running} class="input input-xs join-item w-20 font-mono" />
                    <button type="submit" disabled={not running} title={not running && "Grow live while running"}
                      class="btn btn-xs join-item">Grow GiB</button>
                  </form>
                </td>
              </tr>
              <tr :for={{slot, iso} <- cdroms} id={"cdrom-row-#{slot}"}>
                <td class="font-mono">{slot}</td>
                <td><span class="badge badge-sm badge-info badge-outline">CD-ROM</span></td>
                <td class="font-mono text-xs truncate max-w-[16rem]">{iso || "empty"}</td>
                <td><span class="font-mono text-xs">scsi</span></td>
                <td class="font-mono text-xs opacity-70">sd{<<?a + slot>>}</td>
                <td class="text-right">
                  <form phx-submit="live_cdrom_slot" class="join justify-end">
                    <input type="hidden" name="slot" value={slot} />
                    <select name="image" disabled={not running} class="select select-xs join-item max-w-[14rem]">
                      <option value="">— empty —</option>
                      <option :for={img <- @options.images} value={img.name} selected={img.name == iso}>{img.label}</option>
                    </select>
                    <button type="submit" disabled={not running} class="btn btn-xs join-item">Mount</button>
                    <button :if={iso} type="button" phx-click="live_cdrom_slot" phx-value-slot={slot} phx-value-image=""
                      disabled={not running} class="btn btn-xs join-item">Eject</button>
                  </form>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <form phx-submit="live_attach_disk" class="mt-3 flex flex-wrap items-center gap-2 text-xs">
          <span class="opacity-70">Add disk</span>
          <input type="number" name="size_gib" min="1" value="20" disabled={not running} class="input input-xs w-20 font-mono" /> GiB
          <select name="container" disabled={not running} class="select select-xs">
            <option :for={c <- @options.containers} value={c}>{c}</option>
          </select>
          <button type="submit" disabled={not running} class="btn btn-xs btn-primary">Attach</button>
        </form>
        <form phx-submit="live_cdrom_slot" class="mt-2 flex flex-wrap items-center gap-2 text-xs">
          <span class="opacity-70">Add CD-ROM</span>
          <input type="hidden" name="slot" value={length(cdroms)} />
          <select name="image" disabled={not running} class="select select-xs">
            <option :for={img <- @options.images} value={img.name}>{img.label}</option>
          </select>
          <button type="submit" disabled={not running} class="btn btn-xs btn-primary">Attach</button>
          <span :if={not running} class="opacity-60">Start the VM to hot-attach; offline changes apply at next start.</span>
        </form>
      </section>

      <!-- NICs -->
      <section id="vm-nics" class="glass-card p-5 rounded-xl border border-base-content/10 mt-6">
        <h3 class="panel-title mb-3">Network interfaces</h3>
        <div class="overflow-x-auto">
          <table class="table table-sm w-full">
            <thead><tr><th>#</th><th>Network</th><th>Model</th><th class="text-right">Actions</th></tr></thead>
            <tbody>
              <tr :for={nic <- @nics} id={"nic-row-#{nic.index}"}>
                <td class="font-mono">{nic.index}</td>
                <td>
                  <form phx-submit="live_change_nic" class="join items-center gap-2">
                    <input type="hidden" name="index" value={nic.index} />
                    <span class="font-mono text-xs opacity-70">{nic.network}</span>
                    <select name="network_id" disabled={not running} class="select select-xs join-item min-w-[14rem]">
                      <option :if={not Enum.any?(@options.networks, &(&1.id == nic.network))} value={nic.network} selected>
                        {nic.network}
                      </option>
                      <option :for={net <- @options.networks} value={net.id} selected={net.id == nic.network}>{net.label} ({net.id})</option>
                    </select>
                    <button type="submit" disabled={not running} class="btn btn-xs join-item">Move</button>
                  </form>
                </td>
                <td><span class="badge badge-sm badge-ghost font-mono">{nic.model}</span></td>
                <td class="text-right">
                  <div class="join">
                    <button phx-click="live_link_nic" phx-value-index={nic.index} phx-value-state="up" disabled={not running} class="btn btn-xs join-item">Up</button>
                    <button phx-click="live_link_nic" phx-value-index={nic.index} phx-value-state="down" disabled={not running} class="btn btn-xs join-item">Down</button>
                    <button :if={nic.index == length(@nics) - 1} phx-click="live_detach_nic" phx-value-index={nic.index}
                      data-confirm="Detach this network interface?" disabled={not running} class="btn btn-xs join-item text-error">Remove</button>
                  </div>
                </td>
              </tr>
              <tr :if={@nics == []}><td colspan="4" class="text-center text-xs opacity-60">No network interfaces (isolated guest)</td></tr>
            </tbody>
          </table>
        </div>
        <form phx-submit="live_attach_nic" class="mt-3 flex flex-wrap items-center gap-2 text-xs">
          <span class="opacity-70">Add NIC</span>
          <select name="network_id" disabled={not running} class="select select-xs min-w-[14rem]">
            <option :for={net <- @options.networks} value={net.id}>{net.label}</option>
          </select>
          <select name="model" disabled={not running} class="select select-xs">
            <option value="virtio">virtio</option><option value="e1000">e1000</option>
          </select>
          <button type="submit" disabled={not running} class="btn btn-xs btn-primary">Hotplug</button>
          <span class="opacity-60">Direct (macvtap) networks cannot be hot-moved; VLAN/overlay bridges can.</span>
        </form>
      </section>
    </Layouts.app>
    """
  end

  # CD-ROM drives from the iso column (comma-separated, "__empty__" for an empty drive).
  defp cdrom_slots(%{iso: iso}) when is_binary(iso) and iso != "" do
    iso
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.with_index(fn spec, idx -> {idx, if(spec in ["", "__empty__"], do: nil, else: spec)} end)
  end

  defp cdrom_slots(_vm), do: []

  # The console page a VM's graphics device actually has. Vm.decode already narrows the
  # column to "spice" or "vnc", so there is no third case to carry here.
  defp console_page(%{graphics: "spice"}), do: "spice_auto.html"
  defp console_page(_vm), do: "vnc_auto.html"

  defp console_label(%{graphics: "spice"}), do: "SPICE Console"
  defp console_label(_vm), do: "Console"

  defp blank(nil, placeholder), do: placeholder
  defp blank("", placeholder), do: placeholder
  defp blank(value, _placeholder), do: value

  defp vm_nics(%{network_id: nil}), do: []
  defp vm_nics(%{network_id: ""}), do: []
  defp vm_nics(%{network_id: "[" <> _ = json}) do
    case Jason.decode(json) do
      {:ok, list} when is_list(list) ->
        Enum.with_index(list, fn entry, idx ->
          case String.split(to_string(entry), ":") do
            [net, model] -> %{index: idx, network: net, model: model}
            [net] -> %{index: idx, network: net, model: "virtio"}
            _ -> %{index: idx, network: to_string(entry), model: "virtio"}
          end
        end)

      _ ->
        [%{index: 0, network: json, model: "virtio"}]
    end
  end
  defp vm_nics(%{network_id: single}), do: [%{index: 0, network: single, model: "virtio"}]
end
