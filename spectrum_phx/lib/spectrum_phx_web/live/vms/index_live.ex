defmodule SpectrumPhxWeb.Vms.IndexLive do
  @moduledoc """
  The VM list at `/vms`, with live state, placement and power controls.

  State is pushed over the websocket rather than waiting for a page refresh. Two
  mechanisms, because there are two ways a VM's row changes:

    * PubSub, for changes this web tier made -- immediate feedback on a click.
    * A poll, for changes made by anything else. Vali writes `state` and `host_ip` when a
      task completes, DRS moves VMs on its own, and a guest can be shut down from inside.
      None of those go through this node, so there is nothing to subscribe to; the row has
      to be re-read.

  Both are gated on `connected?/1` so the static render does not start timers or
  subscriptions it will immediately throw away.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Vms.Components

  alias SpectrumPhx.Vms
  alias SpectrumPhx.Vms.Vm

  @poll_interval 3_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Vms.subscribe()
      :timer.send_interval(@poll_interval, :poll)
    end

    {:ok, socket |> assign(page_title: "Virtual Machines", db_error: nil) |> load_vms()}
  end

  @impl true
  def handle_info(:poll, socket), do: {:noreply, load_vms(socket)}

  def handle_info({:vm_updated, _name}, socket), do: {:noreply, load_vms(socket)}
  def handle_info({:vm_created, _name}, socket), do: {:noreply, load_vms(socket)}
  def handle_info({:vm_task_submitted, _name, _action}, socket), do: {:noreply, load_vms(socket)}

  @impl true
  def handle_event("power_on", %{"name" => name}, socket) do
    {:noreply, act(socket, name, "Start requested for", fn -> Vms.power_on(name) end)}
  end

  def handle_event("power_off", %{"name" => name}, socket) do
    {:noreply, act(socket, name, "Stop requested for", fn -> Vms.power_off(name) end)}
  end

  def handle_event("reboot", %{"name" => name}, socket) do
    {:noreply, act(socket, name, "Reboot requested for", fn -> Vms.reboot(name) end)}
  end

  # Every power control funnels through here so that a refusal is surfaced to the operator
  # rather than logged and forgotten. A refused start is the interesting case: it means
  # something else owns the VM.
  defp act(socket, name, verb, fun) do
    case fun.() do
      {:ok, _result} ->
        socket
        |> put_flash(:info, "#{verb} #{name}.")
        |> load_vms()

      {:error, reason} ->
        socket
        |> put_flash(:error, "#{name}: #{error_message(reason)}")
        |> load_vms()
    end
  end

  defp load_vms(socket) do
    case Vms.list_vms() do
      {:ok, vms} ->
        assign(socket, vms: vms, db_error: nil)

      {:error, reason} ->
        # Keep the last-known list on screen. A cluster whose database is unreachable is a
        # cluster whose VMs are still running, and blanking the page loses the operator's
        # last good picture of it.
        socket
        |> assign_new(:vms, fn -> [] end)
        |> assign(db_error: reason)
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <.header>
        Virtual machines
        <:subtitle>
          {length(@vms)} registered in this cluster. State updates as the cluster changes.
        </:subtitle>
        <:actions>
          <.button navigate={~p"/vms/new"} variant="primary" id="new-vm">
            <.icon name="hero-plus-micro" class="size-4" /> New VM
          </.button>
        </:actions>
      </.header>

      <div
        :if={@db_error}
        id="vms-db-error"
        class="mt-4 rounded-lg border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-700"
      >
        <.icon name="hero-exclamation-triangle-micro" class="size-4" />
        Showing the last known state: Hydra is not answering ({inspect(@db_error)}).
      </div>

      <div :if={@vms == []} id="vms-empty" class="mt-10 text-center text-sm text-zinc-500">
        No virtual machines are registered yet.
      </div>

      <ul id="vms" phx-update="replace" class="mt-6 divide-y divide-zinc-200/70">
        <li :for={vm <- @vms} id={"vm-" <> vm.name} class="flex items-center gap-4 py-4">
          <div class="min-w-0 flex-1">
            <div class="flex items-center gap-2">
              <.link navigate={~p"/vms/#{vm.name}"} class="font-semibold hover:underline">
                {vm.name}
              </.link>
              <.state_badge vm={vm} />
              <.lock_badge vm={vm} />
            </div>
            <div class="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-zinc-500">
              <span>{vm.vcpu} vCPU</span>
              <span>{vm.memory} MiB</span>
              <span>{vm.firmware}</span>
              <span class="flex items-center gap-1">
                <.icon name="hero-server-micro" class="size-3" /> <.placement vm={vm} />
              </span>
            </div>
          </div>

          <div class="flex shrink-0 items-center gap-2">
            <.button
              :if={not Vm.running?(vm)}
              id={"start-" <> vm.name}
              phx-click="power_on"
              phx-value-name={vm.name}
              phx-disable-with="Starting..."
              disabled={Vm.migrating?(vm)}
            >
              Start
            </.button>
            <.button
              :if={Vm.running?(vm)}
              id={"stop-" <> vm.name}
              phx-click="power_off"
              phx-value-name={vm.name}
              phx-disable-with="Stopping..."
              disabled={Vm.migrating?(vm)}
            >
              Stop
            </.button>
            <.button
              :if={Vm.running?(vm)}
              id={"reboot-" <> vm.name}
              phx-click="reboot"
              phx-value-name={vm.name}
              phx-disable-with="Rebooting..."
              disabled={Vm.migrating?(vm)}
            >
              Reboot
            </.button>
          </div>
        </li>
      </ul>
    </Layouts.app>
    """
  end
end
