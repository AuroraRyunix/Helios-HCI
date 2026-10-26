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
      if Application.get_env(:spectrum_phx, :vms_source, :hydra) == :hydra do
        send(self(), :probe_live_devices)
      end
    end

    case Vms.get_vm(name) do
      {:ok, vm} ->
        options = Options.load(probe_hosts: connected?(socket))
        snapshots = case Vms.list_snapshots(name) do {:ok, s} -> s; _ -> [] end

        {:ok,
         assign(socket,
           vm: vm,
           disks: Vm.disks(vm),
           nics: vm_nics(vm),
           snapshots: snapshots,
           name: vm.name,
           page_title: vm.name,
           options: options,
           db_error: nil,
           show_clone_modal: false,
           host_pci_devices: [],
           host_usb_devices: [],
           guest_agent: nil,
           guest_exec_output: nil,
           serial_output: ""
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

  def handle_info(:probe_live_devices, socket) do
    {:noreply, probe_live_devices(socket)}
  end

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

  def handle_event("live_detach_disk", %{"index" => idx}, socket) do
    idx = String.to_integer(idx)
    {:noreply, act(socket, "Detached and removed data disk vd#{<<?a + idx>>}.", fn ->
      Vms.live_change(socket.assigns.name, %{
        "op" => "disk",
        "action" => "detach",
        "index" => idx,
        "confirm_delete" => true
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
        |> then(&if(mem != vm.memory, do: act(&1, "", fn -> Vms.live_change(vm.name, %{"op" => "memory", "mib" => mem, "persist" => true}) end), else: &1))

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
          "vtpm_enabled" => params["vtpm_enabled"] == "true",
          "secure_boot" => params["secure_boot"] == "true",
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

  def handle_event("toggle_template", _params, socket) do
    vm = socket.assigns.vm
    target = not Vm.template?(vm)
    label = if target, do: "converted to golden image template", else: "unmarked as template"

    case Vms.set_template(vm.name, target) do
      {:ok, _} ->
        {:noreply, socket |> put_flash(:info, "VM #{vm.name} #{label}.") |> reload()}

      {:error, reason} ->
        {:noreply, socket |> put_flash(:error, error_message(reason)) |> reload()}
    end
  end

  def handle_event("save_affinity", params, socket) do
    vm = socket.assigns.vm

    case Vms.update_affinity(vm.name, params) do
      {:ok, _} ->
        {:noreply, socket |> put_flash(:info, "Placement & DRS affinity rules saved.") |> reload()}

      {:error, reason} ->
        {:noreply, socket |> put_flash(:error, error_message(reason)) |> reload()}
    end
  end

  def handle_event("save_cloud_init", params, socket) do
    vm = socket.assigns.vm

    case Vms.update_cloud_init(vm.name, params) do
      {:ok, _} ->
        {:noreply, socket |> put_flash(:info, "Cloud-Init customization saved.") |> reload()}

      {:error, reason} ->
        {:noreply, socket |> put_flash(:error, error_message(reason)) |> reload()}
    end
  end

  def handle_event("open_clone_modal", _params, socket) do
    {:noreply, assign(socket, show_clone_modal: true)}
  end

  def handle_event("close_clone_modal", _params, socket) do
    {:noreply, assign(socket, show_clone_modal: false)}
  end

  def handle_event("clone_vm", %{"target_name" => target_name}, socket) do
    name = socket.assigns.name
    case Vms.clone_vm(name, target_name) do
      {:ok, _new_vm} ->
        {:noreply,
         socket
         |> assign(show_clone_modal: false)
         |> put_flash(:info, "VM #{name} successfully cloned to #{target_name}.")
         |> push_navigate(to: ~p"/vms/#{target_name}")}

      {:error, reason} ->
        {:noreply, socket |> put_flash(:error, "Clone failed: #{error_message(reason)}") |> reload()}
    end
  end

  def handle_event("save_annotations", %{"notes" => notes, "tags" => tags}, socket) do
    vm = socket.assigns.vm
    if Vm.running?(vm) do
      socket =
        socket
        |> then(&act(&1, "Annotations saved.", fn ->
          Vms.live_change(vm.name, %{"op" => "annotate", "notes" => notes})
          Vms.live_change(vm.name, %{"op" => "tags", "tags" => tags})
        end))
      {:noreply, socket}
    else
      edit = %{
        "vcpu" => vm.vcpu,
        "memory" => vm.memory,
        "firmware" => vm.firmware,
        "cpu_model" => vm.cpu_model,
        "boot_device" => vm.boot_device,
        "graphics" => vm.graphics,
        "audio_enabled" => vm.audio_enabled,
        "disks" => vm.disks_list,
        "iso" => vm.iso,
        "network_id" => vm.network_id,
        "notes" => notes,
        "tags" => tags,
        "cpu_shares" => vm.cpu_shares,
        "cpu_limit" => vm.cpu_limit,
        "cpu_reservation" => vm.cpu_reservation,
        "mem_reservation" => vm.mem_reservation,
        "mem_limit" => vm.mem_limit
      }
      case Vms.update_vm(vm.name, edit) do
        {:ok, _} -> {:noreply, reload(socket)}
        {:error, reason} -> {:noreply, socket |> put_flash(:error, error_message(reason)) |> reload()}
      end
    end
  end

  def handle_event("save_resources", params, socket) do
    vm = socket.assigns.vm
    shares = to_opt_num(params["cpu_shares"])
    limit = to_opt_num(params["cpu_limit"])
    cpu_res = to_opt_num(params["cpu_reservation"])
    mem_res = to_opt_num(params["mem_reservation"])
    mem_lim = to_opt_num(params["mem_limit"])

    if Vm.running?(vm) do
      body = %{
        "op" => "resources",
        "cpu_shares" => shares,
        "cpu_limit" => limit,
        "cpu_reservation" => cpu_res,
        "mem_reservation" => mem_res,
        "mem_limit" => mem_lim
      }
      {:noreply, act(socket, "Resource QoS applied live.", fn ->
        Vms.live_change(vm.name, body)
      end)}
    else
      edit = %{
        "vcpu" => vm.vcpu,
        "memory" => vm.memory,
        "firmware" => vm.firmware,
        "cpu_model" => vm.cpu_model,
        "boot_device" => vm.boot_device,
        "graphics" => vm.graphics,
        "audio_enabled" => vm.audio_enabled,
        "disks" => vm.disks_list,
        "iso" => vm.iso,
        "network_id" => vm.network_id,
        "notes" => vm.notes,
        "tags" => vm.tags,
        "cpu_shares" => shares,
        "cpu_limit" => limit,
        "cpu_reservation" => cpu_res,
        "mem_reservation" => mem_res,
        "mem_limit" => mem_lim
      }
      case Vms.update_vm(vm.name, edit) do
        {:ok, _} -> {:noreply, reload(socket)}
        {:error, reason} -> {:noreply, socket |> put_flash(:error, error_message(reason)) |> reload()}
      end
    end
  end

  def handle_event("live_throttle_disk", %{"index" => idx, "iops" => iops, "bytes_sec" => bps}, socket) do
    idx = String.to_integer(idx)
    iops_val = to_opt_num(iops)
    bps_val = to_opt_num(bps)
    body = %{
      "op" => "disk",
      "action" => "throttle",
      "index" => idx,
      "iops" => iops_val,
      "bytes_sec" => bps_val
    }
    {:noreply, act(socket, "Disk #{idx + 1} throttling updated.", fn ->
      Vms.live_change(socket.assigns.name, body)
    end)}
  end

  def handle_event("take_snapshot", %{"snapshot_name" => snap_name}, socket) do
    {:noreply, act(socket, "Snapshot #{snap_name} taken.", fn ->
      Vms.create_snapshot(socket.assigns.name, snap_name)
    end)}
  end

  def handle_event("rollback_snapshot", %{"snapshot_name" => snap_name} = params, socket) do
    live? = params["live"] == "true" or Vm.running?(socket.assigns.vm)

    {:noreply, act(socket, "Rolled back to #{snap_name}.", fn ->
      Vms.rollback_snapshot(socket.assigns.name, snap_name, live: live?)
    end)}
  end

  def handle_event("scan_host_devices", _params, socket) do
    vm = socket.assigns.vm
    host_ip = if Vm.placed?(vm), do: vm.host_ip, else: nil

    pci =
      case Vms.host_pci_devices(host_ip) do
        {:ok, %{"devices" => list}} when is_list(list) -> list
        _ -> []
      end

    usb =
      case Vms.host_usb_devices(host_ip) do
        {:ok, %{"devices" => list}} when is_list(list) -> list
        _ -> []
      end

    {:noreply, assign(socket, host_pci_devices: pci, host_usb_devices: usb)}
  end

  def handle_event("attach_pci", %{"bdf" => bdf}, socket) do
    bdf = String.trim(bdf || "")
    if bdf == "" do
      {:noreply, put_flash(socket, :error, "PCI BDF address is required.")}
    else
      case Vms.change_pci_device(socket.assigns.name, "attach", bdf) do
        {:ok, _} -> {:noreply, socket |> put_flash(:info, "Attached PCI device #{bdf}.") |> reload()}
        {:error, reason} -> {:noreply, socket |> put_flash(:error, error_message(reason)) |> reload()}
      end
    end
  end

  def handle_event("detach_pci", %{"bdf" => bdf}, socket) do
    case Vms.change_pci_device(socket.assigns.name, "detach", bdf) do
      {:ok, _} -> {:noreply, socket |> put_flash(:info, "Detached PCI device #{bdf}.") |> reload()}
      {:error, reason} -> {:noreply, socket |> put_flash(:error, error_message(reason)) |> reload()}
    end
  end

  def handle_event("attach_usb", %{"device" => device}, socket) do
    device = String.trim(device || "")
    if device == "" do
      {:noreply, put_flash(socket, :error, "USB vendor:product ID is required.")}
    else
      case Vms.change_usb_device(socket.assigns.name, "attach", device) do
        {:ok, _} -> {:noreply, socket |> put_flash(:info, "Attached USB device #{device}.") |> reload()}
        {:error, reason} -> {:noreply, socket |> put_flash(:error, error_message(reason)) |> reload()}
      end
    end
  end

  def handle_event("detach_usb", %{"device" => device}, socket) do
    case Vms.change_usb_device(socket.assigns.name, "detach", device) do
      {:ok, _} -> {:noreply, socket |> put_flash(:info, "Detached USB device #{device}.") |> reload()}
      {:error, reason} -> {:noreply, socket |> put_flash(:error, error_message(reason)) |> reload()}
    end
  end

  def handle_event("probe_guest_agent", _params, socket) do
    {:ok, status} = Vms.guest_agent_status(socket.assigns.vm)
    {:noreply, socket |> assign(guest_agent: status) |> reload()}
  end

  def handle_event("guest_fsfreeze", _params, socket) do
    case Vms.guest_agent_action(socket.assigns.name, %{"action" => "fsfreeze-freeze"}) do
      {:ok, res} ->
        {:noreply, socket |> assign(guest_exec_output: "Frozen filesystems: #{inspect(res["frozen"])}") |> put_flash(:info, "Guest filesystems frozen.")}
      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Freeze failed: #{error_message(reason)}")}
    end
  end

  def handle_event("guest_fsthaw", _params, socket) do
    case Vms.guest_agent_action(socket.assigns.name, %{"action" => "fsfreeze-thaw"}) do
      {:ok, res} ->
        {:noreply, socket |> assign(guest_exec_output: "Thawed filesystems: #{inspect(res["thawed"])}") |> put_flash(:info, "Guest filesystems thawed.")}
      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Thaw failed: #{error_message(reason)}")}
    end
  end

  def handle_event("guest_set_password", %{"username" => user, "password" => pass}, socket) do
    case Vms.guest_agent_action(socket.assigns.name, %{"action" => "set-user-password", "username" => user, "password" => pass}) do
      {:ok, _} ->
        {:noreply, put_flash(socket, :info, "Password updated for guest user #{user}.")}
      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Password reset failed: #{error_message(reason)}")}
    end
  end

  def handle_event("guest_exec", %{"path" => path, "args" => args_str}, socket) do
    args =
      (args_str || "")
      |> String.split(~r/\s+/, trim: true)

    case Vms.guest_agent_action(socket.assigns.name, %{"action" => "exec", "path" => path, "arg" => args, "wait" => true}) do
      {:ok, res} ->
        out = "exitcode=#{res["exitcode"]}\n#{res["stdout"] || ""}#{res["stderr"] || ""}"
        {:noreply, assign(socket, guest_exec_output: String.trim(out))}
      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Guest exec failed: #{error_message(reason)}")}
    end
  end

  def handle_event("serial_refresh", _params, socket) do
    out =
      case Vms.serial_read(socket.assigns.name) do
        {:ok, %{"output" => o}} when is_binary(o) -> o
        _ -> socket.assigns.serial_output
      end

    {:noreply, assign(socket, serial_output: out || "")}
  end

  def handle_event("serial_send", %{"input" => input}, socket) do
    case Vms.serial_send(socket.assigns.name, (input || "") <> "\n") do
      {:ok, %{"output" => out}} -> {:noreply, assign(socket, serial_output: out || "")}
      {:error, reason} -> {:noreply, put_flash(socket, :error, "Serial console write failed: #{error_message(reason)}")}
    end
  end

  defp probe_live_devices(socket) do
    vm = socket.assigns.vm
    host_ip = if Vm.placed?(vm), do: vm.host_ip, else: nil

    pci =
      case Vms.host_pci_devices(host_ip) do
        {:ok, %{"devices" => list}} when is_list(list) -> list
        _ -> []
      end

    usb =
      case Vms.host_usb_devices(host_ip) do
        {:ok, %{"devices" => list}} when is_list(list) -> list
        _ -> []
      end

    {ga, serial} =
      if Vm.running?(vm) do
        ga_res =
          case Vms.guest_agent_status(vm) do
            {:ok, res} -> res
          end

        ser_res =
          case Vms.serial_read(vm) do
            {:ok, %{"output" => out}} when is_binary(out) -> out
            _ -> socket.assigns.serial_output
          end

        {ga_res, ser_res}
      else
        {nil, socket.assigns.serial_output}
      end

    socket
    |> assign(host_pci_devices: pci, host_usb_devices: usb, guest_agent: ga, serial_output: serial)
    |> reload()
  end

  defp to_opt_num(nil), do: nil
  defp to_opt_num(""), do: nil
  defp to_opt_num(v) when is_integer(v), do: v
  defp to_opt_num(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, _} -> n
      :error -> nil
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
      {:ok, _result} -> _ = message; reload(socket)
      {:error, reason} -> socket |> put_flash(:error, error_message(reason)) |> reload()
    end
  end

  defp reload(socket) do
    case Vms.get_vm(socket.assigns.name) do
      {:ok, vm} ->
        snapshots = case Vms.list_snapshots(socket.assigns.name) do {:ok, s} -> s; _ -> [] end
        assign(socket,
          vm: vm,
          disks: Vm.disks(vm),
          nics: vm_nics(vm),
          snapshots: snapshots,
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
          <span class="flex flex-wrap items-center gap-2 mt-1">
            <.state_badge vm={@vm} />
            <.template_badge vm={@vm} />
            <.security_badges vm={@vm} />
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
          :if={not Vm.running?(@vm) and not Vm.template?(@vm)}
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

        <.button
          id="clone-vm-btn"
          phx-click="open_clone_modal"
          variant={if Vm.template?(@vm), do: "primary", else: "secondary"}
        >
          <.icon name="hero-document-duplicate-solid" class="size-4" />
          {if Vm.template?(@vm), do: "Deploy from Template", else: "Clone VM"}
        </.button>

        <.button
          :if={not Vm.running?(@vm)}
          id="toggle-template-btn"
          phx-click="toggle_template"
          variant="secondary"
        >
          <.icon name="hero-cube-transparent-solid" class="size-4" />
          {if Vm.template?(@vm), do: "Unmark Template", else: "Convert to Template"}
        </.button>

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

      <div :if={@show_clone_modal} class="modal modal-open">
        <div class="modal-box">
          <h3 class="font-bold text-lg">
            {if Vm.template?(@vm), do: "Deploy VM from Template #{@vm.name}", else: "Clone VM #{@vm.name}"}
          </h3>
          <p class="py-2 text-sm text-base-content/70">
            Create an independent copy of this virtual machine with its disks cloned copy-on-write on Sidon.
          </p>
          <form phx-submit="clone_vm" class="space-y-4 mt-2">
            <label class="form-control">
              <span class="label-text text-sm">Target VM Name</span>
              <input type="text" name="target_name" required pattern="[A-Za-z0-9][A-Za-z0-9_.-]{0,62}" placeholder={"#{@vm.name}-clone"} class="input input-bordered w-full font-mono" />
            </label>
            <div class="modal-action">
              <button type="button" phx-click="close_clone_modal" class="btn btn-ghost">Cancel</button>
              <button type="submit" class="btn btn-primary" phx-disable-with="Cloning...">
                {if Vm.template?(@vm), do: "Deploy VM", else: "Clone"}
              </button>
            </div>
          </form>
        </div>
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
                <option :for={g <- graphics_types_for(@options, @vm.graphics)} value={g} selected={g == @vm.graphics}>{blank(g, "vnc")}</option>
              </select>
            </label>
            <label class="label cursor-pointer justify-start gap-2">
              <input type="hidden" name="audio_enabled" value="false" />
              <input type="checkbox" name="audio_enabled" value="true" checked={@vm.audio_enabled} class="toggle toggle-sm" />
              <span class="label-text text-xs">ICH9 audio</span>
            </label>
            <label class="label cursor-pointer justify-start gap-2">
              <input type="hidden" name="secure_boot" value="false" />
              <input type="checkbox" name="secure_boot" value="true" checked={@vm.secure_boot} class="toggle toggle-sm" />
              <span class="label-text text-xs">Secure Boot</span>
            </label>
            <label class="label cursor-pointer justify-start gap-2 col-span-2">
              <input type="hidden" name="vtpm_enabled" value="false" />
              <input type="checkbox" name="vtpm_enabled" value="true" checked={@vm.vtpm_enabled} class="toggle toggle-sm" />
              <span class="label-text text-xs">Virtual TPM 2.0 (swtpm)</span>
            </label>
          </fieldset>

          <div class="flex items-center justify-between pt-1">
            <span class="text-[11px] opacity-60">
              {if running, do: "vCPU hot-add and memory balloon apply live.", else: "Applied at next power-on."}
            </span>
            <button type="submit" class="btn btn-primary btn-sm" phx-disable-with="Applying…">Apply</button>
          </div>
        </form>

        <!-- Resource Tuning & QoS -->
        <form id="resources-form" phx-submit="save_resources" class="glass-card p-5 rounded-xl border border-base-content/10 space-y-3">
          <h3 class="panel-title flex items-center justify-between">
            <span>Resource QoS</span>
            <span class={["badge badge-sm", if(running, do: "badge-success", else: "badge-ghost")]}>
              {if running, do: "live", else: "offline edit"}
            </span>
          </h3>

          <div class="grid grid-cols-2 gap-3">
            <label class="form-control">
              <span class="label-text text-xs opacity-70">CPU Shares (e.g. 1024)</span>
              <input type="number" name="cpu_shares" min="2" max="262144" value={@vm.cpu_shares} placeholder="Normal (1024)" class="input input-sm font-mono" />
            </label>
            <label class="form-control">
              <span class="label-text text-xs opacity-70">CPU Limit (%)</span>
              <input type="number" name="cpu_limit" min="1" max="10000" value={@vm.cpu_limit} placeholder="Unlimited" class="input input-sm font-mono" />
            </label>
            <label class="form-control">
              <span class="label-text text-xs opacity-70">CPU Reservation (MHz)</span>
              <input type="number" name="cpu_reservation" min="0" value={@vm.cpu_reservation} placeholder="0" class="input input-sm font-mono" />
            </label>
            <label class="form-control">
              <span class="label-text text-xs opacity-70">Mem Reservation (MiB)</span>
              <input type="number" name="mem_reservation" min="0" value={@vm.mem_reservation} placeholder="0" class="input input-sm font-mono" />
            </label>
            <label class="form-control col-span-2">
              <span class="label-text text-xs opacity-70">Mem Hard Limit (MiB)</span>
              <input type="number" name="mem_limit" min="128" value={@vm.mem_limit} placeholder="Unlimited" class="input input-sm font-mono" />
            </label>
          </div>

          <div class="flex items-center justify-between pt-1">
            <span class="text-[11px] opacity-60">
              {if running, do: "QoS limits apply live to cgroups.", else: "Applies at next power-on."}
            </span>
            <button type="submit" class="btn btn-primary btn-sm" phx-disable-with="Applying…">Apply QoS</button>
          </div>
        </form>

        <!-- Annotations & Tags -->
        <form id="annotations-form" phx-submit="save_annotations" class="glass-card p-5 rounded-xl border border-base-content/10 space-y-3">
          <h3 class="panel-title flex items-center justify-between">
            <span>Notes &amp; Tags</span>
            <div class="flex flex-wrap gap-1">
              <span :for={tag <- Vm.tag_list(@vm)} class="badge badge-xs badge-outline badge-primary">{tag}</span>
            </div>
          </h3>

          <label class="form-control">
            <span class="label-text text-xs opacity-70">Tags (comma-separated)</span>
            <input type="text" name="tags" value={@vm.tags || ""} placeholder="production, web, tier-1" class="input input-sm font-mono" />
          </label>

          <label class="form-control">
            <span class="label-text text-xs opacity-70">Operator Notes / Description</span>
            <textarea name="notes" rows="3" placeholder="Add annotations or operational notes here..." class="textarea textarea-sm font-sans">{@vm.notes || ""}</textarea>
          </label>

          <div class="flex items-center justify-end pt-1">
            <button type="submit" class="btn btn-secondary btn-sm" phx-disable-with="Saving…">Save Notes</button>
          </div>
        </form>

        <!-- DRS Placement & Host Affinity -->
        <form id="affinity-form" phx-submit="save_affinity" class="glass-card p-5 rounded-xl border border-base-content/10 space-y-3">
          <h3 class="panel-title flex items-center justify-between">
            <span>DRS Affinity &amp; Host Pinning</span>
            <span class="badge badge-sm badge-ghost">scheduler</span>
          </h3>

          <div class="grid grid-cols-2 gap-3">
            <label class="form-control">
              <span class="label-text text-xs opacity-70">Affinity Group (keep together)</span>
              <input type="text" name="affinity_group" value={@vm.affinity_group || ""} placeholder="db-cache-pair" class="input input-sm font-mono" />
            </label>
            <label class="form-control">
              <span class="label-text text-xs opacity-70">Anti-Affinity Group (separate hosts)</span>
              <input type="text" name="anti_affinity_group" value={@vm.anti_affinity_group || ""} placeholder="ha-web-tier" class="input input-sm font-mono" />
            </label>
            <label class="form-control col-span-2">
              <span class="label-text text-xs opacity-70">Pinned Host IPs (comma-separated allowlist)</span>
              <input type="text" name="pinned_hosts" value={@vm.pinned_hosts || ""} placeholder="10.10.102.41, 10.10.102.42" class="input input-sm font-mono" />
            </label>
          </div>

          <div class="flex items-center justify-between pt-1">
            <span class="text-[11px] opacity-60">Enforced at placement and DRS rebalancing.</span>
            <button type="submit" class="btn btn-secondary btn-sm" phx-disable-with="Saving…">Save Affinity</button>
          </div>
        </form>

        <!-- Cloud-Init Customization -->
        <form id="cloud-init-form" phx-submit="save_cloud_init" class="glass-card p-5 rounded-xl border border-base-content/10 space-y-3 xl:col-span-2">
          <h3 class="panel-title flex items-center justify-between">
            <span>Cloud-Init Customization (NoCloud <code>cidata</code>)</span>
            <span class={["badge badge-sm", if((@vm.cloud_init_userdata || "") != "", do: "badge-info", else: "badge-ghost")]}>
              {if (@vm.cloud_init_userdata || "") != "", do: "configured", else: "inactive"}
            </span>
          </h3>

          <div class="grid gap-3 sm:grid-cols-2">
            <label class="form-control">
              <span class="label-text text-xs opacity-70">user-data (#cloud-config YAML or script)</span>
              <textarea name="cloud_init_userdata" rows="3" placeholder="#cloud-config&#10;ssh_authorized_keys: ..." class="textarea textarea-sm font-mono text-xs">{@vm.cloud_init_userdata || ""}</textarea>
            </label>
            <label class="form-control">
              <span class="label-text text-xs opacity-70">network-config (Netplan v2 YAML)</span>
              <textarea name="cloud_init_network" rows="3" placeholder="version: 2&#10;ethernets: ..." class="textarea textarea-sm font-mono text-xs">{@vm.cloud_init_network || ""}</textarea>
            </label>
          </div>

          <div class="flex items-center justify-between pt-1">
            <span class="text-[11px] opacity-60">Synthesizes and attaches a NoCloud seed ISO at next boot.</span>
            <button type="submit" class="btn btn-secondary btn-sm" phx-disable-with="Saving…">Save Cloud-Init</button>
          </div>
        </form>

        <!-- Placement -->
        <div class="glass-card p-5 rounded-xl border border-base-content/10 xl:col-span-3">
          <h3 class="panel-title mb-3">Placement &amp; lifecycle</h3>
          <.list>
            <:item title="Host">
              <span :if={Vm.placed?(@vm)}>{Config.hostname_for(@vm.host_ip)} <span class="opacity-60">({@vm.host_ip})</span></span>
              <span :if={not Vm.placed?(@vm)} class="opacity-60">Unassigned. Placement is claimed when the VM starts.</span>
            </:item>
            <:item title="Power state">{@vm.state}</:item>
            <:item title="Lifecycle lock">{blank(@vm.status, "none")}</:item>
            <:item :if={Vm.guest_ip_list(@vm) != []} title="Guest IPs">
              <span class="flex flex-wrap gap-1">
                <span :for={ip <- Vm.guest_ip_list(@vm)} class="badge badge-xs badge-info font-mono">{ip}</span>
              </span>
            </:item>
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
                  <span>{disk.target}</span>
                  <span class="block text-[10px] opacity-60">{disk.path}</span>
                </td>
                <td class="text-right">
                  <div class="flex flex-col items-end gap-1">
                    <div class="flex items-center justify-end gap-1">
                      <form phx-submit="live_grow_disk" class="join justify-end">
                        <input type="hidden" name="index" value={disk.index} />
                        <input type="number" name="size_gib" min={(disk.size_gib || 0) + 1} value={(disk.size_gib || 0) + 5}
                          disabled={not running} class="input input-xs join-item w-20 font-mono" />
                        <button type="submit" disabled={not running} title={not running && "Grow live while running"}
                          class="btn btn-xs join-item">Grow GiB</button>
                      </form>
                      <button :if={disk.index > 0}
                        type="button"
                        phx-click="live_detach_disk"
                        phx-value-index={disk.index}
                        data-confirm="Detach and permanently delete this data disk? The underlying storage will be destroyed."
                        disabled={not running}
                        title={if running, do: "Hot-remove and destroy data disk", else: "VM must be running"}
                        class="btn btn-xs btn-error btn-outline">
                        Detach
                      </button>
                    </div>
                    <form phx-submit="live_throttle_disk" class="join justify-end">
                      <input type="hidden" name="index" value={disk.index} />
                      <input type="number" name="iops" min="0" placeholder="IOPS"
                        disabled={not running} class="input input-xs join-item w-16 font-mono" title="Max IOPS (0 for unlimited)" />
                      <input type="number" name="bytes_sec" min="0" placeholder="B/s"
                        disabled={not running} class="input input-xs join-item w-20 font-mono" title="Max B/s (0 for unlimited)" />
                      <button type="submit" disabled={not running} title={if running, do: "Live throttle disk IOPS / Bps", else: "VM must be running"}
                        class="btn btn-xs join-item">Throttle</button>
                    </form>
                  </div>
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
                    <button phx-click="live_detach_nic" phx-value-index={nic.index}
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

      <!-- PCI & USB Hardware Passthrough -->
      <section id="vm-passthrough" class="glass-card p-5 rounded-xl border border-base-content/10 mt-6">
        <div class="flex flex-wrap items-center justify-between gap-2 mb-3">
          <h3 class="panel-title">PCI &amp; USB Hardware Passthrough</h3>
          <button type="button" phx-click="scan_host_devices" class="btn btn-xs btn-secondary">
            <.icon name="hero-magnifying-glass-solid" class="size-3.5" /> Scan Host PCI / USB Inventory
          </button>
        </div>

        <div class="grid gap-6 lg:grid-cols-2">
          <!-- PCI VFIO Passthrough -->
          <div class="space-y-3">
            <div class="text-xs font-semibold uppercase tracking-wider opacity-70">PCIe / GPU / NVMe (VFIO)</div>
            <div class="flex flex-wrap gap-2">
              <span :if={Vm.pci_device_list(@vm) == []} class="text-xs opacity-60">No PCI devices attached.</span>
              <span :for={bdf <- Vm.pci_device_list(@vm)} class="badge badge-sm badge-outline gap-1.5 font-mono py-2.5">
                {bdf}
                <button type="button" phx-click="detach_pci" phx-value-bdf={bdf} class="text-error hover:underline text-[10px]">✕</button>
              </span>
            </div>
            <form id="attach-pci-form" phx-submit="attach_pci" class="flex flex-wrap items-center gap-2 text-xs">
              <input type="text" name="bdf" list="host-pci-list" placeholder="0000:01:00.0" class="input input-xs font-mono w-44" />
              <datalist id="host-pci-list">
                <option :for={dev <- @host_pci_devices} value={dev["bdf"]}>{dev["description"]} (IOMMU {dev["iommu_group"]})</option>
              </datalist>
              <button type="submit" class="btn btn-xs btn-primary">Attach PCI</button>
            </form>
          </div>

          <!-- USB Device Passthrough -->
          <div class="space-y-3">
            <div class="text-xs font-semibold uppercase tracking-wider opacity-70">USB Host Devices (Live Hotplug)</div>
            <div class="flex flex-wrap gap-2">
              <span :if={Vm.usb_device_list(@vm) == []} class="text-xs opacity-60">No USB devices attached.</span>
              <span :for={usb <- Vm.usb_device_list(@vm)} class="badge badge-sm badge-outline gap-1.5 font-mono py-2.5">
                {usb}
                <button type="button" phx-click="detach_usb" phx-value-device={usb} class="text-error hover:underline text-[10px]">✕</button>
              </span>
            </div>
            <form id="attach-usb-form" phx-submit="attach_usb" class="flex flex-wrap items-center gap-2 text-xs">
              <input type="text" name="device" list="host-usb-list" placeholder="1050:0407 (vendor:product)" class="input input-xs font-mono w-52" />
              <datalist id="host-usb-list">
                <option :for={dev <- @host_usb_devices} value={dev["id"]}>{dev["description"]}</option>
              </datalist>
              <button type="submit" class="btn btn-xs btn-primary">Attach USB</button>
            </form>
          </div>
        </div>
      </section>

      <!-- QEMU Guest Agent & Serial Console -->
      <section id="vm-guest-tools" class="mt-6 grid gap-6 lg:grid-cols-2">
        <!-- QEMU Guest Agent -->
        <div id="vm-guest-agent" class="glass-card p-5 rounded-xl border border-base-content/10 space-y-3">
          <div class="flex items-center justify-between">
            <h3 class="panel-title">QEMU Guest Agent (<code>org.qemu.guest_agent.0</code>)</h3>
            <div class="flex items-center gap-2">
              <span :if={@guest_agent && @guest_agent["connected"]} class="badge badge-xs badge-success">connected</span>
              <span :if={@guest_agent && not @guest_agent["connected"]} class="badge badge-xs badge-warning">not responding</span>
              <button type="button" phx-click="probe_guest_agent" disabled={not running} class="btn btn-xs btn-secondary">
                Probe Agent
              </button>
            </div>
          </div>

          <div :if={@guest_agent && @guest_agent["connected"]} class="text-xs space-y-1 bg-base-300/40 p-3 rounded-lg font-mono">
            <div><span class="opacity-60">Hostname:</span> {@guest_agent["hostname"] || "unknown"}</div>
            <div><span class="opacity-60">OS:</span> {get_in(@guest_agent, ["os", "pretty-name"]) || get_in(@guest_agent, ["os", "name"]) || "unknown"}</div>
            <div><span class="opacity-60">IPs:</span> {Enum.join(@guest_agent["ips"] || @guest_agent["ip_addresses"] || [], ", ")}</div>
          </div>

          <div class="flex flex-wrap items-center gap-2 pt-1">
            <button type="button" phx-click="guest_fsfreeze" disabled={not running} class="btn btn-xs btn-outline">
              Freeze Filesystems (fsfreeze)
            </button>
            <button type="button" phx-click="guest_fsthaw" disabled={not running} class="btn btn-xs btn-outline">
              Thaw Filesystems (fsthaw)
            </button>
          </div>

          <form id="guest-password-form" phx-submit="guest_set_password" class="flex flex-wrap items-center gap-2 text-xs pt-2 border-t border-base-content/10">
            <span class="opacity-70">Reset Password:</span>
            <input type="text" name="username" placeholder="root" required disabled={not running} class="input input-xs font-mono w-28" />
            <input type="password" name="password" placeholder="New password" required disabled={not running} class="input input-xs font-mono w-36" />
            <button type="submit" disabled={not running} class="btn btn-xs btn-warning">Set Password</button>
          </form>

          <form id="guest-exec-form" phx-submit="guest_exec" class="flex flex-wrap items-center gap-2 text-xs pt-2 border-t border-base-content/10">
            <span class="opacity-70">Run Command:</span>
            <input type="text" name="path" placeholder="/bin/uname" required disabled={not running} class="input input-xs font-mono w-32" />
            <input type="text" name="args" placeholder="-a" disabled={not running} class="input input-xs font-mono flex-1" />
            <button type="submit" disabled={not running} class="btn btn-xs btn-primary">Exec</button>
          </form>

          <pre :if={@guest_exec_output} class="text-xs font-mono bg-black/80 text-emerald-300 p-3 rounded-lg overflow-x-auto max-h-36">{@guest_exec_output}</pre>
        </div>

        <!-- Serial Console (ttyS0) -->
        <div id="vm-serial-console" class="glass-card p-5 rounded-xl border border-base-content/10 space-y-3 flex flex-col">
          <div class="flex items-center justify-between">
            <h3 class="panel-title">Serial Console (<code>ttyS0</code>)</h3>
            <button type="button" phx-click="serial_refresh" disabled={not running} class="btn btn-xs btn-secondary">
              Refresh Output
            </button>
          </div>

          <pre class="text-xs font-mono bg-black/85 text-green-400 p-3 rounded-lg overflow-auto h-44 flex-1">{if @serial_output == "", do: "No serial console output captured yet. Click 'Refresh Output' or send a newline below.", else: @serial_output}</pre>

          <form id="serial-send-form" phx-submit="serial_send" class="flex items-center gap-2 text-xs">
            <input type="text" name="input" placeholder="Send command or press Enter for login prompt..." disabled={not running} class="input input-xs font-mono flex-1" />
            <button type="submit" disabled={not running} class="btn btn-xs btn-primary">Send to ttyS0</button>
          </form>
        </div>
      </section>

      <!-- Snapshots -->
      <section id="vm-snapshots" class="glass-card p-5 rounded-xl border border-base-content/10 mt-6">
        <h3 class="panel-title mb-3 flex items-center justify-between">
          <span>Snapshots</span>
          <span class="badge badge-sm badge-ghost font-mono">{length(@snapshots)}</span>
        </h3>

        <div class="overflow-x-auto">
          <table class="table table-sm w-full">
            <thead>
              <tr><th>Name</th><th>Underlying VDisk</th><th>Size</th><th class="text-right">Actions</th></tr>
            </thead>
            <tbody>
              <tr :if={@snapshots == []}>
                <td colspan="4" class="text-center text-xs opacity-60 py-4">No snapshots taken yet.</td>
              </tr>
              <tr :for={snap <- @snapshots} id={"snap-row-#{snap.name}"}>
                <td class="font-mono font-semibold">{snap.name}</td>
                <td class="text-xs opacity-70 font-mono">{snap.vdisk_snapshot}</td>
                <td class="text-xs opacity-70 font-mono">{round(snap.size_bytes / (1024 * 1024 * 1024))} GiB</td>
                <td class="text-right">
                  <button
                    type="button"
                    phx-click="rollback_snapshot"
                    phx-value-snapshot_name={snap.name}
                    phx-value-live={if running, do: "true", else: "false"}
                    data-confirm={
                      if running,
                        do: "Live-roll back running VM #{@vm.name} to snapshot #{snap.name}? The VM will briefly stop, roll back its vdisks, and restart.",
                        else: "Roll back VM #{@vm.name} to snapshot #{snap.name}? Any unsaved changes since this snapshot will be lost."
                    }
                    class="btn btn-xs btn-warning btn-outline"
                  >
                    {if running, do: "Live Rollback", else: "Rollback"}
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <form phx-submit="take_snapshot" class="mt-4 flex flex-wrap items-center gap-2 text-xs">
          <span class="opacity-70">New Snapshot:</span>
          <input
            type="text"
            name="snapshot_name"
            placeholder="snap-01"
            required
            pattern="[A-Za-z0-9][A-Za-z0-9_.-]{0,62}"
            class="input input-xs w-48 font-mono"
          />
          <button type="submit" class="btn btn-xs btn-primary">Take Snapshot</button>
        </form>
      </section>
    </Layouts.app>
    """
  end

  defp graphics_types_for(%{spice?: true}, _current), do: Vm.graphics_types()
  defp graphics_types_for(_options, "spice"), do: ["vnc", "spice"]
  defp graphics_types_for(_options, _current), do: ["vnc"]

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
            [net, model, _mac | _] -> %{index: idx, network: net, model: model}
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
