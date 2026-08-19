defmodule SpectrumPhxWeb.Vms.NewLive do
  @moduledoc """
  The VM creation form at `/vms/new`.

  Validation is `SpectrumPhx.Vms.Vm.new/1` -- the same function the context calls before
  it writes anything -- so what the form shows and what the write path enforces cannot
  drift apart. In particular the name is *rejected*, never repaired: the field says why,
  and nothing is created until the operator fixes it.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Vms.Components

  alias SpectrumPhx.Vms
  alias SpectrumPhx.Vms.Vm

  @defaults %{
    "name" => "",
    "vcpu" => "2",
    "memory" => "2048",
    "firmware" => "uefi",
    "disks" => "10G",
    "iso" => "",
    "boot_device" => "",
    "cpu_model" => "",
    "network_id" => "",
    "audio_enabled" => "false"
  }

  @impl true
  def mount(_params, _session, socket) do
    {:ok,
     socket
     |> assign(page_title: "New VM", params: @defaults)
     |> assign_form([])}
  end

  @impl true
  def handle_event("validate", %{"vm" => params}, socket) do
    errors =
      case Vm.new(params) do
        {:ok, _vm} -> []
        {:error, errors} -> errors
      end

    {:noreply, socket |> assign(params: params) |> assign_form(errors)}
  end

  def handle_event("save", %{"vm" => params}, socket) do
    case Vms.create_vm(params) do
      {:ok, vm} ->
        {:noreply,
         socket
         |> put_flash(:info, "VM #{vm.name} created, and its disks are allocated.")
         |> push_navigate(to: ~p"/vms/#{vm.name}")}

      {:error, errors} when is_list(errors) ->
        {:noreply, socket |> assign(params: params) |> assign_form(errors)}

      {:error, reason} ->
        {:noreply,
         socket
         |> assign(params: params)
         |> assign_form([])
         |> put_flash(:error, error_message(reason))}
    end
  end

  # `translate_error/1` in core_components expects the `{message, opts}` shape that Ecto
  # produces. The domain returns plain strings, so they are wrapped here rather than
  # letting an Ecto-shaped error format leak into a context that has no Ecto.
  defp assign_form(socket, errors) do
    errors = Enum.map(errors, fn {field, message} -> {field, {message, []}} end)
    assign(socket, form: to_form(socket.assigns.params, as: :vm, errors: errors))
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_username={@current_username} active={:vms}>
      <.header>
        New virtual machine
        <:subtitle>
          Creates the VM and allocates its disks. If disk allocation fails, the VM is removed again rather than left without storage.
        </:subtitle>
        <:actions>
          <.button navigate={~p"/vms"}>Cancel</.button>
        </:actions>
      </.header>

      <.form
        for={@form}
        id="vm-form"
        phx-change="validate"
        phx-submit="save"
        class="mt-6 space-y-5"
      >
        <.input
          field={@form[:name]}
          type="text"
          label="Name"
          autocomplete="off"
          placeholder="web-01"
        />
        <p class="-mt-3 text-xs text-zinc-500">
          1-63 characters, starting with a letter or digit, then letters, digits, <code>.</code>,
          <code>-</code>
          or <code>_</code>. The name is used verbatim on the hypervisor, so it is rejected
          rather than corrected.
        </p>

        <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <.input field={@form[:vcpu]} type="number" label="vCPU" min="1" step="1" />
          <.input field={@form[:memory]} type="number" label="Memory (MiB)" min="128" step="1" />
        </div>

        <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <.input
            field={@form[:firmware]}
            type="select"
            label="Firmware"
            options={Enum.map(Vm.firmwares(), &{String.upcase(&1), &1})}
          />
          <.input field={@form[:disks]} type="text" label="Disks" placeholder="10G,500G" />
        </div>
        <p class="-mt-3 text-xs text-zinc-500">
          Comma separated sizes in GiB or TiB, optionally <code>size:container</code>.
          Each becomes one DRBD resource.
        </p>

        <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <.input field={@form[:iso]} type="text" label="Boot ISO" placeholder="(none)" />
          <.input field={@form[:boot_device]} type="text" label="Boot device" placeholder="(default)" />
        </div>

        <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <.input
            field={@form[:cpu_model]}
            type="text"
            label="CPU model"
            placeholder="(host default)"
          />
          <.input
            field={@form[:network_id]}
            type="text"
            label="Network ID"
            placeholder="(default network)"
          />
        </div>

        <.input field={@form[:audio_enabled]} type="checkbox" label="Enable audio device" />

        <div class="flex items-center gap-3 pt-2">
          <.button id="create-vm" variant="primary" phx-disable-with="Creating...">
            Create VM
          </.button>
          <.button navigate={~p"/vms"}>Cancel</.button>
        </div>
      </.form>
    </Layouts.app>
    """
  end
end
