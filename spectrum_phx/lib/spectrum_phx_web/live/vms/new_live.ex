defmodule SpectrumPhxWeb.Vms.NewLive do
  @moduledoc """
  The VM creation form at `/vms/new`.

  Validation is `SpectrumPhx.Vms.Vm.new/1` -- the same function the context calls before
  it writes anything -- so what the form shows and what the write path enforces cannot
  drift apart. In particular the name is *rejected*, never repaired: the field says why,
  and nothing is created until the operator fixes it.

  ## The whole option set, as repeatable rows

  The form carries what the old console's wizard carried, laid out as one page rather than
  five steps: name, vCPU, memory with a unit, firmware, boot device override, CPU model,
  the graphics device (VNC or SPICE), any number of disks (size, unit, container, bus), any
  number of CD-ROM drives (one image each) and any number of NICs (network and model).
  Containers, images and networks are drop-downs over the real catalogues -- see
  `SpectrumPhx.Vms.Options` -- not free text. `SpectrumPhx.Vms.Form` says how the rows
  become the strings `hydra.vms` stores.

  One thing the old form showed is deliberately not here: *Secure Boot*. Nothing in the VM
  record or in `generate_vm_xml` expresses it, so a checkbox would be a lie. Adding it is a
  change to Vali and to the schema, not to this page. (*Network (PXE)* as a boot device was
  left out for the same reason until the domain gave each device its own boot order.)

  ## Editing

  The same page edits a stopped VM at `/vms/:name/edit` (`live_action :edit`): the form starts
  as `Form.from_vm/1`, the name is fixed, and saving is `Vms.update_vm/2`. Everything but the
  name is editable; a disk can only grow, keeps its container and position, and only the last
  disks can be removed (their vdisks are named after the position). A VM that is not stopped is
  not editable here, and the page says what can be changed while it runs.
  """
  use SpectrumPhxWeb, :live_view

  alias SpectrumPhx.Vms
  alias SpectrumPhx.Vms.Form
  alias SpectrumPhx.Vms.Options
  alias SpectrumPhx.Vms.Vm

  @impl true
  def mount(%{"name" => name}, _session, %{assigns: %{live_action: :edit}} = socket) do
    case Vms.get_vm(name) do
      {:ok, vm} ->
        if Vms.editable?(vm) do
          options = Options.load(probe_hosts: connected?(socket))
          params = Form.from_vm(vm)

          {:ok,
           socket
           |> assign(
             page_title: "Edit #{vm.name}",
             options: options,
             params: params,
             vm: vm,
             mode: :edit,
             existing_disks: length(Vm.disks(vm))
           )
           |> assign_form([])}
        else
          {:ok,
           socket
           |> put_flash(:error, not_stopped_message(vm))
           |> push_navigate(to: ~p"/vms/#{vm.name}")}
        end

      {:error, _reason} ->
        {:ok,
         socket
         |> put_flash(:error, "VM #{name} was not found.")
         |> push_navigate(to: ~p"/vms")}
    end
  end

  def mount(_params, _session, socket) do
    # Hosts are probed for SPICE only once the socket is connected: the static render that
    # precedes it would otherwise block on every node twice.
    options = Options.load(probe_hosts: connected?(socket))

    params =
      Form.defaults(
        container: List.first(options.containers),
        network: options.networks |> List.first() |> Map.get(:id)
      )

    {:ok,
     socket
     |> assign(
       page_title: "New VM",
       options: options,
       params: params,
       vm: nil,
       mode: :new,
       existing_disks: 0
     )
     |> assign_form([])}
  end

  defp not_stopped_message(%Vm{} = vm) do
    "#{vm.name} is #{String.downcase(vm.state || "not stopped")}; stop it to edit it. " <>
      "(While a VM runs, live updates are performed directly on the VM details page.)"
  end

  @impl true
  def handle_event("validate", %{"vm" => params}, socket) do
    {:noreply, socket |> assign(params: params) |> assign_form(validate(params, socket))}
  end

  def handle_event("save", %{"vm" => params}, %{assigns: %{mode: :edit, vm: vm}} = socket) do
    save_fun =
      if Vm.running?(vm) do
        fn -> Vms.update_running_vm(vm.name, Form.to_attrs(params)) end
      else
        fn -> Vms.update_vm(vm.name, Form.to_attrs(params)) end
      end

    case save_fun.() do
      {:ok, _result} ->
        msg =
          if Vm.running?(vm) do
            "VM #{vm.name} updated. Live changes applied to the running guest; cold changes apply at next reboot."
          else
            "VM #{vm.name} updated. Changes apply the next time it starts."
          end

        {:noreply,
         socket
         |> put_flash(:info, msg)
         |> push_navigate(to: ~p"/vms/#{vm.name}")}

      {:error, errors} when is_list(errors) ->
        {:noreply, socket |> assign(params: params) |> assign_form(errors)}

      {:error, :not_stopped} ->
        {:noreply,
         socket
         |> put_flash(:error, "#{vm.name} is not stopped.")
         |> push_navigate(to: ~p"/vms/#{vm.name}")}

      {:error, :changed} ->
        {:noreply,
         socket
         |> assign(params: params)
         |> assign_form([])
         |> put_flash(
           :error,
           "#{vm.name} changed while it was being edited. Nothing was saved."
         )}

      {:error, reason} ->
        {:noreply,
         socket
         |> assign(params: params)
         |> assign_form([])
         |> put_flash(:error, error_message(reason))}
    end
  end

  def handle_event("save", %{"vm" => params}, socket) do
    case Vms.create_vm(Form.to_attrs(params)) do
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

  def handle_event("add_row", %{"kind" => kind}, socket) do
    {:noreply,
     socket |> change_rows(&Form.add_row(&1, kind, new_row(kind, socket.assigns.options)))}
  end

  def handle_event("remove_row", %{"kind" => kind, "index" => index}, socket) do
    {:noreply, change_rows(socket, &Form.remove_row(&1, kind, index))}
  end

  # Only the three row sets this page knows. The kind arrives from the browser, and it
  # becomes a map key in the form, so an unknown one is ignored rather than stored.
  defp new_row("disk_rows", options), do: Form.disk_row(List.first(options.containers), "20")
  defp new_row("cdrom_rows", _options), do: Form.cdrom_row()

  defp new_row("nic_rows", options),
    do: Form.nic_row(options.networks |> List.first() |> Map.get(:id))

  defp change_rows(socket, fun) do
    params = fun.(socket.assigns.params)
    socket |> assign(params: params) |> assign_form(validate(params, socket))
  end

  defp validate(params, %{assigns: %{mode: :edit, vm: vm}}) do
    Vms.edit_errors(vm, Form.to_attrs(params))
  end

  defp validate(params, _socket) do
    case params |> Form.to_attrs() |> Vm.new() do
      {:ok, _vm} -> []
      {:error, errors} -> errors
    end
  end

  # `translate_error/1` in core_components expects the `{message, opts}` shape that Ecto
  # produces. The domain returns plain strings, so they are wrapped here rather than
  # letting an Ecto-shaped error format leak into a context that has no Ecto.
  defp assign_form(socket, errors) do
    wrapped = Enum.map(errors, fn {field, message} -> {field, {message, []}} end)

    socket
    |> assign(errors: Map.new(errors))
    |> assign(form: to_form(socket.assigns.params, as: :vm, errors: wrapped))
  end

  defp error_message({:storage, message}), do: "Storage could not be allocated: #{message}"
  defp error_message(:already_exists), do: "A VM with that name already exists."
  defp error_message(other), do: "The VM could not be saved: #{inspect(other)}"

  @impl true
  def render(assigns) do
    assigns = assign(assigns, :rows, rows(assigns.params))

    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:vms}>
      <.header :if={@mode == :new}>
        New virtual machine
        <:subtitle>
          Creates the VM and allocates its disks. If disk allocation fails, the VM is removed again rather than left without storage.
        </:subtitle>
        <:actions>
          <.button navigate={~p"/vms"}>Cancel</.button>
        </:actions>
      </.header>
      <.header :if={@mode == :edit}>
        Edit {@vm.name}
        <:subtitle>
          <span :if={Vm.running?(@vm)} class="text-info font-medium block">
            Running guest: Live changes (vCPUs, Memory, CD-ROMs, Disks, NICs) take effect immediately. Cold properties apply on reboot.
          </span>
          <span :if={not Vm.running?(@vm)} class="block">
            The VM is stopped, so everything but its name can change. Changes take effect the next time it starts.
            A disk can only grow and keeps its container; only the last disks can be removed, and removing one deletes its data.
          </span>
        </:subtitle>
        <:actions>
          <.button navigate={~p"/vms/#{@vm.name}"} variant="secondary">Cancel</.button>
        </:actions>
      </.header>

      <p
        :for={note <- @options.notes}
        class="alert alert-warning alert-soft text-sm"
        id="options-note"
      >
        {note}
      </p>

      <.form for={@form} id="vm-form" phx-change="validate" phx-submit="save" class="space-y-4">
        <input type="hidden" name="vm[structured]" value="true" />

        <div class="grid gap-4 xl:grid-cols-2">
          <div class="flex flex-col gap-4">
            <section class="glass-card p-4 sm:p-5" id="general-section">
              <h2 class="panel-title mb-3">General</h2>
              <.input
                field={@form[:name]}
                type="text"
                label="Name"
                autocomplete="off"
                placeholder="web-01"
                readonly={@mode == :edit}
              />
              <p class="-mt-1 mb-3 text-xs opacity-60">
                1-63 characters, starting with a letter or digit, then letters, digits, <code>.</code>,
                <code>-</code>
                or <code>_</code>. The name is used verbatim on the hypervisor, so it is rejected
                rather than corrected.
              </p>
            </section>

            <section class="glass-card p-4 sm:p-5" id="compute-section">
              <h2 class="panel-title mb-3">Compute</h2>
              <div class="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                <.input field={@form[:vcpu]} type="number" label="vCPU" min="1" step="1" />
                <div class="grid grid-cols-[1fr_6rem] gap-2">
                  <.input field={@form[:memory]} type="number" label="Memory" min="1" step="1" />
                  <.input
                    field={@form[:memory_unit]}
                    type="select"
                    label="Unit"
                    options={Form.memory_units()}
                  />
                </div>
                <.input
                  field={@form[:firmware]}
                  type="select"
                  label="Boot firmware"
                  options={[{"UEFI", "uefi"}, {"Legacy BIOS", "bios"}]}
                />
                <.input
                  field={@form[:boot_device]}
                  type="select"
                  label="Boot device override"
                  options={[
                    {"Default (CD-ROM if an ISO is attached, else disk)", ""},
                    {"Hard disk", "hd"},
                    {"CD-ROM / ISO", "cdrom"},
                    {"Network (PXE)", "network"}
                  ]}
                />
                <.input
                  field={@form[:cpu_model]}
                  type="select"
                  label="CPU model"
                  options={[
                    {"Auto (host-model, or host-passthrough on bare metal)", ""},
                    {"host-model", "host-model"},
                    {"host-passthrough", "host-passthrough"},
                    {"Haswell-noTSX (VMware / ESXi safe)", "Haswell-noTSX"},
                    {"Denverton", "Denverton"}
                  ]}
                />
                <.input
                  field={@form[:graphics]}
                  type="select"
                  label="Console"
                  options={graphics_options(@options)}
                />
              </div>
              <p :if={not @options.spice?} class="text-xs opacity-60" id="spice-note">
                SPICE is not offered: it needs every host to report it in its capabilities,
                and one that does not would refuse to start the VM.
              </p>
              <.input field={@form[:audio_enabled]} type="checkbox" label="Enable audio device" />
            </section>
          </div>

          <div class="flex flex-col gap-4">
            <section class="glass-card p-4 sm:p-5" id="disks-section">
              <div class="flex items-start justify-between gap-3 mb-3">
                <div>
                  <h2 class="panel-title">Virtual disks</h2>
                  <p class="text-xs opacity-55 mt-0.5">
                    Allocated on Sidon. The first disk is the boot disk.
                  </p>
                </div>
                <button
                  type="button"
                  class="btn btn-sm"
                  phx-click="add_row"
                  phx-value-kind="disk_rows"
                  id="add-disk"
                >
                  <.icon name="hero-plus" class="size-4" /> Add disk
                </button>
              </div>

              <div
                :for={{{index, row}, position} <- Enum.with_index(@rows.disks)}
                class="grid grid-cols-[1fr_5rem] sm:grid-cols-[1fr_5rem_1.6fr_6rem_auto] gap-2 items-end mb-2"
                id={"disk-row-#{index}"}
              >
                <label class="form-control">
                  <span class="label-text text-xs opacity-70">
                    {if position == 0, do: "Boot disk size", else: "Disk #{position + 1} size"}
                  </span>
                  <input
                    type="number"
                    min="1"
                    name={"vm[disk_rows][#{index}][size]"}
                    value={row["size"]}
                    class="input input-bordered input-sm w-full"
                  />
                </label>
                <label class="form-control">
                  <span class="label-text text-xs opacity-70">Unit</span>
                  <select
                    name={"vm[disk_rows][#{index}][unit]"}
                    class="select select-bordered select-sm w-full"
                  >
                    {Phoenix.HTML.Form.options_for_select(Form.disk_units(), row["unit"])}
                  </select>
                </label>
                <label class="form-control">
                  <span class="label-text text-xs opacity-70">Container</span>
                  <select
                    name={"vm[disk_rows][#{index}][container]"}
                    class="select select-bordered select-sm w-full"
                    disabled={existing_disk?(@mode, index, @existing_disks)}
                  >
                    {Phoenix.HTML.Form.options_for_select(
                      choices(@options.containers, row["container"]),
                      row["container"]
                    )}
                  </select>
                  <%!-- A disabled control is not submitted, and an existing disk keeps its container. --%>
                  <input
                    :if={existing_disk?(@mode, index, @existing_disks)}
                    type="hidden"
                    name={"vm[disk_rows][#{index}][container]"}
                    value={row["container"]}
                  />
                </label>
                <label class="form-control">
                  <span class="label-text text-xs opacity-70">Bus</span>
                  <select
                    name={"vm[disk_rows][#{index}][bus]"}
                    class="select select-bordered select-sm w-full"
                  >
                    {Phoenix.HTML.Form.options_for_select(Vm.buses(), row["bus"])}
                  </select>
                </label>
                <button
                  :if={@mode == :new or position == length(@rows.disks) - 1}
                  type="button"
                  class="btn btn-ghost btn-sm text-error"
                  phx-click="remove_row"
                  phx-value-kind="disk_rows"
                  phx-value-index={index}
                  aria-label="Remove disk"
                >
                  <.icon name="hero-trash" class="size-4" />
                </button>
                <span :if={@mode == :edit and position != length(@rows.disks) - 1} />
              </div>

              <p :if={@rows.disks == []} class="text-sm opacity-60" id="no-disks">
                No disks yet. At least one is required.
              </p>
              <p :if={@errors[:disks]} class="text-sm text-error" id="disks-error">
                {@errors[:disks]}
              </p>
            </section>

            <section class="glass-card p-4 sm:p-5" id="cdroms-section">
              <div class="flex items-start justify-between gap-3 mb-3">
                <div>
                  <h2 class="panel-title">CD-ROM drives</h2>
                  <p class="text-xs opacity-55 mt-0.5">
                    One image per drive, from the image catalogue.
                  </p>
                </div>
                <button
                  type="button"
                  class="btn btn-sm"
                  phx-click="add_row"
                  phx-value-kind="cdrom_rows"
                  id="add-cdrom"
                >
                  <.icon name="hero-plus" class="size-4" /> Add CD-ROM
                </button>
              </div>

              <div
                :for={{{index, row}, position} <- Enum.with_index(@rows.cdroms)}
                class="grid grid-cols-[1fr_auto] gap-2 items-end mb-2"
                id={"cdrom-row-#{index}"}
              >
                <label class="form-control">
                  <span class="label-text text-xs opacity-70">Drive {position + 1} image</span>
                  <select
                    name={"vm[cdrom_rows][#{index}][image]"}
                    class="select select-bordered select-sm w-full"
                  >
                    <option value="">None / empty drive</option>
                    {Phoenix.HTML.Form.options_for_select(
                      Enum.map(@options.images, &{&1.label, &1.name}),
                      row["image"]
                    )}
                  </select>
                </label>
                <button
                  type="button"
                  class="btn btn-ghost btn-sm text-error"
                  phx-click="remove_row"
                  phx-value-kind="cdrom_rows"
                  phx-value-index={index}
                  aria-label="Remove CD-ROM"
                >
                  <.icon name="hero-trash" class="size-4" />
                </button>
              </div>

              <p :if={@rows.cdroms == []} class="text-sm opacity-60" id="no-cdroms">
                No CD-ROM drives.
              </p>
              <p :if={@errors[:iso]} class="text-sm text-error">{@errors[:iso]}</p>
            </section>

            <section class="glass-card p-4 sm:p-5" id="nics-section">
              <div class="flex items-start justify-between gap-3 mb-3">
                <div>
                  <h2 class="panel-title">Network interfaces</h2>
                  <p class="text-xs opacity-55 mt-0.5">None is a valid choice: an isolated VM.</p>
                </div>
                <button
                  type="button"
                  class="btn btn-sm"
                  phx-click="add_row"
                  phx-value-kind="nic_rows"
                  id="add-nic"
                >
                  <.icon name="hero-plus" class="size-4" /> Add NIC
                </button>
              </div>

              <div
                :for={{{index, row}, position} <- Enum.with_index(@rows.nics)}
                class="grid grid-cols-[1fr_7rem_auto] gap-2 items-end mb-2"
                id={"nic-row-#{index}"}
              >
                <label class="form-control">
                  <span class="label-text text-xs opacity-70">NIC {position + 1} network</span>
                  <select
                    name={"vm[nic_rows][#{index}][network]"}
                    class="select select-bordered select-sm w-full"
                  >
                    {Phoenix.HTML.Form.options_for_select(
                      Enum.map(@options.networks, &{&1.label, &1.id}),
                      row["network"]
                    )}
                  </select>
                </label>
                <label class="form-control">
                  <span class="label-text text-xs opacity-70">Model</span>
                  <select
                    name={"vm[nic_rows][#{index}][model]"}
                    class="select select-bordered select-sm w-full"
                  >
                    {Phoenix.HTML.Form.options_for_select(Vm.nic_models(), row["model"])}
                  </select>
                </label>
                <button
                  type="button"
                  class="btn btn-ghost btn-sm text-error"
                  phx-click="remove_row"
                  phx-value-kind="nic_rows"
                  phx-value-index={index}
                  aria-label="Remove NIC"
                >
                  <.icon name="hero-trash" class="size-4" />
                </button>
              </div>

              <p :if={@rows.nics == []} class="text-sm opacity-60" id="no-nics">
                No network interfaces: the VM will be isolated.
              </p>
              <p :if={@errors[:network_id]} class="text-sm text-error">{@errors[:network_id]}</p>
            </section>
          </div>
        </div>

        <div class="flex items-center gap-3 pt-2">
          <.button :if={@mode == :new} id="create-vm" variant="primary" phx-disable-with="Creating...">
            Create VM
          </.button>
          <.button :if={@mode == :edit} id="save-vm" variant="primary" phx-disable-with="Saving...">
            Save changes
          </.button>
          <.button navigate={if @mode == :edit, do: ~p"/vms/#{@vm.name}", else: ~p"/vms"}>Cancel</.button>
        </div>
      </.form>
    </Layouts.app>
    """
  end

  # An existing disk of a VM being edited: its vdisk exists, so its container cannot change.
  defp existing_disk?(:edit, index, existing) do
    case Integer.parse(to_string(index)) do
      {n, _} -> n < existing
      :error -> false
    end
  end

  defp existing_disk?(_mode, _index, _existing), do: false

  defp rows(params) do
    %{
      disks: Form.rows(params, "disk_rows"),
      cdroms: Form.rows(params, "cdrom_rows"),
      nics: Form.rows(params, "nic_rows")
    }
  end

  defp graphics_options(%{spice?: true}), do: [{"VNC", "vnc"}, {"SPICE", "spice"}]
  defp graphics_options(_options), do: [{"VNC", "vnc"}]

  # The value in force is always an option, whether or not the catalogue still lists it: a
  # select that cannot show it would silently change it on the next keystroke.
  defp choices(names, current) do
    if current in [nil, ""] or current in names, do: names, else: [current | names]
  end
end
