defmodule SpectrumPhxWeb.Vms.ShowLive do
  @moduledoc """
  One VM at `/vms/:name`: specs, disks, placement, power state, and the migration lock
  when it is held.

  The lock is shown prominently and separately from the power state on purpose. `state`
  and `status` are different columns, and an operator who cannot see that a migration is
  in flight is an operator who will try to start the VM somewhere else.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Vms.Components

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Vms
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
        {:ok,
         assign(socket,
           vm: vm,
           disks: Vm.disks(vm),
           name: vm.name,
           page_title: vm.name,
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

  defp act(socket, message, fun) do
    case fun.() do
      {:ok, _result} -> socket |> put_flash(:info, message) |> reload()
      {:error, reason} -> socket |> put_flash(:error, error_message(reason)) |> reload()
    end
  end

  defp reload(socket) do
    case Vms.get_vm(socket.assigns.name) do
      {:ok, vm} -> assign(socket, vm: vm, disks: Vm.disks(vm), db_error: nil)
      {:error, :not_found} -> assign(socket, db_error: :not_found)
      {:error, reason} -> assign(socket, db_error: reason)
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <.header>
        <span class="font-mono">{@vm.name}</span>
        <:subtitle>
          <span class="flex items-center gap-2">
            <.state_badge vm={@vm} />
            <.lock_badge vm={@vm} />
          </span>
        </:subtitle>
        <:actions>
          <.button navigate={~p"/vms"}>Back</.button>
        </:actions>
      </.header>

      <div
        :if={Vm.migrating?(@vm)}
        id="migration-lock"
        class="mt-4 rounded-lg border border-amber-500/40 bg-amber-500/10 p-4 text-sm text-amber-700"
      >
        <p class="font-semibold">
          <.icon name="hero-lock-closed-micro" class="size-4" /> Migration lock held
        </p>
        <p class="mt-1">
          This VM's <code>status</code>
          column reads <code>{@vm.status}</code>, so a migration is in flight. Lifecycle
          operations are refused until it clears. The lock is released by whoever took it;
          it is not cleared by starting or stopping the VM.
        </p>
      </div>

      <div
        :if={@db_error}
        id="vm-db-error"
        class="mt-4 rounded-lg border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-700"
      >
        Showing the last known state: Hydra is not answering ({inspect(@db_error)}).
      </div>

      <div class="mt-6 flex flex-wrap gap-2">
        <.button
          :if={not Vm.running?(@vm)}
          id="start"
          phx-click="power_on"
          phx-disable-with="Starting..."
          disabled={Vm.migrating?(@vm)}
          variant="primary"
        >
          Start
        </.button>
        <.button
          :if={Vm.running?(@vm)}
          id="stop"
          phx-click="power_off"
          phx-disable-with="Stopping..."
          disabled={Vm.migrating?(@vm)}
        >
          Stop
        </.button>
        <.button
          :if={Vm.running?(@vm)}
          id="reboot"
          phx-click="reboot"
          phx-disable-with="Rebooting..."
          disabled={Vm.migrating?(@vm)}
        >
          Reboot
        </.button>
      </div>

      <section id="vm-specs" class="mt-8">
        <h2 class="text-sm font-semibold uppercase tracking-wide text-zinc-500">Specification</h2>
        <.list>
          <:item title="vCPU">{@vm.vcpu}</:item>
          <:item title="Memory">{@vm.memory} MiB</:item>
          <:item title="Firmware">{@vm.firmware}</:item>
          <:item title="CPU model">{blank(@vm.cpu_model, "host default")}</:item>
          <:item title="Boot device">{blank(@vm.boot_device, "default order")}</:item>
          <:item title="CD-ROM">{blank(@vm.iso, "empty")}</:item>
          <:item title="Network">{blank(@vm.network_id, "unassigned")}</:item>
          <:item title="Audio">{if @vm.audio_enabled, do: "enabled", else: "disabled"}</:item>
        </.list>
      </section>

      <section id="vm-placement" class="mt-8">
        <h2 class="text-sm font-semibold uppercase tracking-wide text-zinc-500">Placement</h2>
        <.list>
          <:item title="Host">
            <span :if={Vm.placed?(@vm)}>
              {Config.hostname_for(@vm.host_ip)}
              <span class="text-zinc-500">({@vm.host_ip})</span>
            </span>
            <span :if={not Vm.placed?(@vm)} class="text-zinc-500">
              Unassigned. Placement is claimed when the VM starts.
            </span>
          </:item>
          <:item title="Power state">{@vm.state}</:item>
          <:item title="Lifecycle lock">{blank(@vm.status, "none")}</:item>
        </.list>
      </section>

      <section id="vm-disks" class="mt-8">
        <h2 class="text-sm font-semibold uppercase tracking-wide text-zinc-500">Disks</h2>
        <p :if={@disks == []} class="mt-2 text-sm text-zinc-500">
          This VM has no disks registered.
        </p>
        <.table :if={@disks != []} id="disks" rows={@disks}>
          <:col :let={disk} label="#">{disk.index}</:col>
          <:col :let={disk} label="Size">{disk.size}</:col>
          <:col :let={disk} label="Container">{blank(disk.container, "default")}</:col>
          <:col :let={disk} label="DRBD resource">
            <span class="font-mono text-xs">{disk.resource}</span>
          </:col>
          <:col :let={disk} label="Path">
            <span class="font-mono text-xs">{disk.path}</span>
          </:col>
        </.table>
      </section>
    </Layouts.app>
    """
  end

  defp blank(nil, placeholder), do: placeholder
  defp blank("", placeholder), do: placeholder
  defp blank(value, _placeholder), do: value
end
