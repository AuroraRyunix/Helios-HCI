defmodule SpectrumPhx.Vms.Vm do
  @moduledoc """
  A VM record from `hydra.vms`, plus validation for the fields an operator supplies.

  There is no Ecto in this project, so validation is plain functions returning
  `{:ok, struct}` or `{:error, keyword}` where the keyword list is `[{field, message}]`.

  ## Why the name is *rejected* rather than sanitised

  A VM name reaches a root shell on the hypervisor (Vali builds `virsh` command lines
  from it and hands them to spark-daemon's `/api/v1/execute`,
  which runs them with a shell as root) and it reaches CQL. It is also the operator-visible
  identity of the VM: silently rewriting `web 01` into `web-01`, the way image names are
  slugified, would leave an operator looking at a VM that is not the one they asked for,
  and would let two different requested names collide onto one record. So a bad name is an
  error, not something to repair.

  ## `state` vs `status`

  These are two different columns and mean different things:

    * `state` is the power state -- `"Running"` or `"Stopped"`.
    * `status` is a transient lifecycle lock -- `"migrating"` while a migration is in
      flight, otherwise `"running"` or (for VMs created before the lock existed) `nil`.

  Anything reading "is this VM up?" wants `state`. Anything asking "may I start a
  lifecycle operation on this VM?" wants `status`.
  """

  @enforce_keys [:name]
  defstruct name: nil,
            vcpu: 1,
            memory: 1024,
            disk_path: "",
            disk_size: 0,
            state: "Stopped",
            host_ip: "",
            disks_list: "",
            firmware: "uefi",
            iso: "",
            boot_device: "",
            network_id: nil,
            cpu_model: nil,
            audio_enabled: false,
            graphics: "vnc",
            status: nil

  @type t :: %__MODULE__{
          name: String.t(),
          vcpu: integer(),
          memory: integer(),
          disk_path: String.t() | nil,
          disk_size: integer(),
          state: String.t(),
          host_ip: String.t() | nil,
          disks_list: String.t() | nil,
          firmware: String.t(),
          iso: String.t() | nil,
          boot_device: String.t() | nil,
          network_id: String.t() | nil,
          cpu_model: String.t() | nil,
          audio_enabled: boolean(),
          graphics: String.t(),
          status: String.t() | nil
        }

  # `\A` and `\z` rather than `^` and `$`.
  #
  # In PCRE -- which is what Elixir's Regex uses -- an unanchored `$` also matches
  # immediately before a final newline, so `~r/^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$/` happily
  # accepts "web-01\n". That trailing newline survives into the shell command Vali builds
  # and into CQL, which is exactly the hole `vali.py` documents when it uses Python's `\Z`
  # for the same pattern. `\z` is the true end-of-subject anchor.
  @name_regex ~r/\A[A-Za-z0-9][A-Za-z0-9_.-]{0,62}\z/

  @name_error "must be 1-63 characters, start with a letter or digit, and contain only letters, digits, '.', '-' and '_'"

  @firmwares ~w(uefi bios)

  # What the old console's create form offered, and what Vali actually does with each.
  # `network` is deliberately absent from the boot devices: the old form listed "Network
  # (PXE)", but `generate_vm_xml` only distinguishes `cdrom` from everything else, so
  # choosing it booted the disk. A control that does nothing is not offered.
  @boot_devices ~w(hd cdrom)
  @cpu_models ~w(host-model host-passthrough Haswell-noTSX Denverton)
  @buses ~w(virtio sata scsi)
  @nic_models ~w(virtio e1000 e1000e vmxnet3 rtl8139)
  @graphics ~w(vnc spice)
  @max_nics 16

  # 128 MiB is the floor below which no supported guest boots; the ceiling is a
  # sanity bound, not a capacity check (the real capacity gate lives in Vali's scheduler).
  @min_memory_mib 128
  @max_memory_mib 4_194_304
  @min_vcpu 1
  @max_vcpu 512

  # Sizes are ultimately handed to a vdisk create as a byte count, and
  # Vali only knows how to convert G and T suffixes. Accepting "512M" here would produce a
  # crash three services away rather than a field error here, so M is rejected up front.
  @disk_size_regex ~r/\A(\d+)\s*(G|GB|GIB|T|TB|TIB)?\z/i

  # The Gato default network, matching `/api/vms/create` in spectrum_server.py.
  @default_network_id "7a68e0d6-11f8-4e89-9430-b3b44b8bc438"

  @doc "The compiled VM-name pattern. Exposed so tests can assert on the anchors."
  def name_regex, do: @name_regex

  @doc "Human-readable explanation of the VM-name rule."
  def name_error, do: @name_error

  @doc "Firmware values the hypervisor can actually build a domain for."
  def firmwares, do: @firmwares

  @doc "Boot device overrides Vali honours. Empty means its own default."
  def boot_devices, do: @boot_devices

  @doc "CPU model overrides. Empty means Vali picks host-model or host-passthrough."
  def cpu_models, do: @cpu_models

  @doc "Disk buses Vali builds a `<target>` for."
  def buses, do: @buses

  @doc "NIC models the domain XML accepts."
  def nic_models, do: @nic_models

  @doc "Graphics devices a VM can have."
  def graphics_types, do: @graphics

  @doc "The network assigned when a creation request does not name one."
  def default_network_id, do: @default_network_id

  @doc """
  Validate a VM name.

  Returns `{:ok, name}` or `{:error, message}`. Every entry point that can reach a shell
  command or a CQL statement must call this (or `valid_name?/1`) before doing anything
  else -- including read-only lookups, since the name is a bound parameter there but is
  still echoed back into the UI.
  """
  @spec validate_name(term()) :: {:ok, String.t()} | {:error, String.t()}
  def validate_name(name) when is_binary(name) do
    if Regex.match?(@name_regex, name) do
      {:ok, name}
    else
      {:error, @name_error}
    end
  end

  def validate_name(_other), do: {:error, @name_error}

  @doc "True only for names that are safe to put in a shell command or CQL statement."
  @spec valid_name?(term()) :: boolean()
  def valid_name?(name) do
    match?({:ok, _}, validate_name(name))
  end

  @doc """
  Build a `%Vm{}` from operator-supplied creation parameters.

  Accepts string-keyed (form) or atom-keyed maps. Returns `{:ok, vm}` or
  `{:error, [field: message]}` with *every* failing field reported, so a form can show all
  of its errors at once rather than one per round trip.
  """
  @spec new(map()) :: {:ok, t()} | {:error, keyword()}
  def new(params) when is_map(params) do
    name = fetch(params, :name)
    vcpu = fetch(params, :vcpu) || fetch(params, :vcpus)
    memory = fetch(params, :memory)
    firmware = fetch(params, :firmware) || "uefi"
    disks = fetch(params, :disks) || fetch(params, :disk_size) || "10G"

    results = [
      name: validate_name(name),
      vcpu: validate_vcpu(vcpu),
      memory: validate_memory(memory),
      firmware: validate_firmware(firmware),
      disks: validate_disks(disks),
      iso: validate_iso(fetch(params, :iso)),
      boot_device: validate_boot_device(fetch(params, :boot_device)),
      network_id: validate_networks(fetch(params, :network_id)),
      cpu_model: validate_cpu_model(fetch(params, :cpu_model)),
      graphics: validate_graphics(fetch(params, :graphics))
    ]

    errors = for {field, {:error, message}} <- results, do: {field, message}

    if errors == [] do
      values = Map.new(results, fn {field, {:ok, value}} -> {field, value} end)
      {disk_specs, disks_list} = values.disks

      {:ok,
       %__MODULE__{
         name: values.name,
         vcpu: values.vcpu,
         memory: values.memory,
         firmware: values.firmware,
         disks_list: disks_list,
         disk_size: disk_specs |> List.first(%{}) |> Map.get(:size_gib, 0),
         disk_path: "",
         state: "Stopped",
         host_ip: "",
         status: nil,
         iso: values.iso,
         boot_device: values.boot_device,
         cpu_model: values.cpu_model,
         network_id: values.network_id,
         graphics: values.graphics,
         audio_enabled: truthy?(fetch(params, :audio_enabled))
       }}
    else
      {:error, errors}
    end
  end

  @doc """
  Build a `%Vm{}` from a `hydra.vms` row (string-keyed map, as Xandra returns it).

  Passes an existing `%Vm{}` straight through, which is what lets tests inject fixture
  structs through the same path production rows take.
  """
  @spec from_row(map() | t()) :: t()
  def from_row(%__MODULE__{} = vm), do: vm

  def from_row(row) when is_map(row) do
    %__MODULE__{
      name: get(row, "name", ""),
      vcpu: get(row, "vcpu", 1),
      memory: get(row, "memory", 1024),
      disk_path: get(row, "disk_path", ""),
      disk_size: get(row, "disk_size", 0),
      state: get(row, "state", "Stopped"),
      host_ip: get(row, "host_ip", ""),
      disks_list: get(row, "disks_list", ""),
      firmware: get(row, "firmware", "uefi"),
      iso: get(row, "iso", ""),
      boot_device: get(row, "boot_device", ""),
      network_id: get(row, "network_id", nil),
      cpu_model: get(row, "cpu_model", nil),
      audio_enabled: get(row, "audio_enabled", false),
      # Null means VNC: every row written before the graphics column has one.
      graphics: normalise_graphics(get(row, "graphics", nil)),
      status: get(row, "status", nil)
    }
  end

  @doc "True when the VM's power state is Running."
  @spec running?(t()) :: boolean()
  def running?(%__MODULE__{state: state}), do: downcase(state) == "running"

  @doc """
  True when the transient lifecycle lock is held.

  This reads `status`, not `state`. The two were conflated for a long time, which is how
  the migration lock came to be set and then never actually checked.
  """
  @spec migrating?(t()) :: boolean()
  def migrating?(%__MODULE__{status: status}), do: downcase(status) == "migrating"

  @doc "True when the VM currently claims a host."
  @spec placed?(t()) :: boolean()
  def placed?(%__MODULE__{host_ip: host_ip}), do: is_binary(host_ip) and host_ip != ""

  @doc """
  The VM's disks, parsed out of the `disks_list` column.

  Entries are `"<size>"` or `"<size>:<container>"`, comma separated; the literal
  `"NONE"` means the VM has no disks. Each disk N of VM `foo` is the vdisk `foo-diskN`,
  served to qemu over the unix socket `/var/lib/hci/sidon/nbd/foo-diskN.sock`.
  """
  @spec disks(t()) :: [map()]
  def disks(%__MODULE__{disks_list: list, name: name}) do
    list
    |> split_disks()
    |> Enum.with_index()
    |> Enum.map(fn {entry, index} ->
      {size, container, bus} = split_entry(entry)

      resource = "#{name}-disk#{index}"

      %{
        index: index,
        size: size,
        size_gib: size_gib_or_nil(size),
        container: container,
        bus: bus || "virtio",
        resource: resource,
        path: "/var/lib/hci/sidon/nbd/#{resource}.sock"
      }
    end)
  end

  @doc """
  Validate one disk-size string, returning its size in GiB.

  Accepts a bare integer (read as GiB) or an integer with a G/GB/GiB/T/TB/TiB suffix.
  """
  @spec validate_disk_size(term()) :: {:ok, pos_integer()} | {:error, String.t()}
  def validate_disk_size(value) when is_integer(value) and value > 0, do: {:ok, value}
  def validate_disk_size(value) when is_integer(value), do: {:error, "must be greater than 0"}

  def validate_disk_size(value) when is_binary(value) do
    case Regex.run(@disk_size_regex, String.trim(value)) do
      [_, digits] ->
        to_gib(digits, "G")

      [_, digits, suffix] ->
        to_gib(digits, String.upcase(suffix))

      nil ->
        {:error,
         "must be a whole number of gibibytes or tebibytes, for example \"10G\" or \"1T\""}
    end
  end

  def validate_disk_size(_other),
    do:
      {:error, "must be a whole number of gibibytes or tebibytes, for example \"10G\" or \"1T\""}

  # -- internals -------------------------------------------------------------------

  defp size_gib_or_nil(size) do
    case validate_disk_size(size) do
      {:ok, gib} -> gib
      {:error, _} -> nil
    end
  end

  defp validate_vcpu(value) do
    case to_integer(value) do
      {:ok, n} when n < @min_vcpu -> {:error, "must be at least #{@min_vcpu}"}
      {:ok, n} when n > @max_vcpu -> {:error, "must be at most #{@max_vcpu}"}
      {:ok, n} -> {:ok, n}
      :error -> {:error, "must be a whole number of vCPUs"}
    end
  end

  defp validate_memory(value) do
    case to_integer(value) do
      {:ok, n} when n < @min_memory_mib -> {:error, "must be at least #{@min_memory_mib} MiB"}
      {:ok, n} when n > @max_memory_mib -> {:error, "must be at most #{@max_memory_mib} MiB"}
      {:ok, n} -> {:ok, n}
      :error -> {:error, "must be a whole number of mebibytes"}
    end
  end

  defp validate_firmware(value) do
    normalised = value |> to_string_or_empty() |> String.downcase()

    if normalised in @firmwares do
      {:ok, normalised}
    else
      {:error, "must be one of: #{Enum.join(@firmwares, ", ")}"}
    end
  end

  # Returns `{parsed_specs, disks_list_column_value}`. The column keeps the operator's
  # original strings (including any ":container" suffix) because that is what Vali reads
  # back when it counts disks and builds resource names.
  defp validate_disks(value) do
    entries =
      case value do
        list when is_list(list) -> Enum.map(list, &to_string_or_empty/1)
        other -> other |> to_string_or_empty() |> split_disks()
      end

    cond do
      entries == [] ->
        {:error, "at least one disk is required"}

      length(entries) > 16 ->
        {:error, "at most 16 disks are supported"}

      true ->
        entries
        |> Enum.map(&parse_disk_entry/1)
        |> Enum.reduce_while({:ok, []}, fn
          {:ok, spec}, {:ok, acc} -> {:cont, {:ok, [spec | acc]}}
          {:error, message}, _acc -> {:halt, {:error, message}}
        end)
        |> case do
          {:ok, specs} ->
            specs = Enum.reverse(specs)
            {:ok, {specs, specs |> Enum.map(& &1.raw) |> Enum.join(",")}}

          {:error, message} ->
            {:error, message}
        end
    end
  end

  # `size`, `size:container` or `size:container:bus`. The third part is what the old console
  # wrote for every disk (`20GB:default-vm-container:virtio`) and what Vali reads to pick
  # `vd` or `sd` for the device name. Splitting on the first colon only, as this used to,
  # made the container `default-vm-container:virtio` -- which is not a name, so every disk
  # the old form could describe was refused.
  defp split_entry(entry) do
    case String.split(entry, ":") do
      [size] -> {String.trim(size), nil, nil}
      [size, container] -> {String.trim(size), blank_to_nil(container), nil}
      [size, container, bus | _] -> {String.trim(size), blank_to_nil(container), blank_to_nil(bus)}
    end
  end

  defp blank_to_nil(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp parse_disk_entry(entry) do
    {size, container, bus} = split_entry(entry)

    with {:ok, gib} <- validate_disk_size(size),
         :ok <- validate_container(container),
         :ok <- validate_bus(bus) do
      {:ok, %{raw: entry, size: size, size_gib: gib, container: container, bus: bus}}
    end
  end

  defp validate_bus(nil), do: :ok

  defp validate_bus(bus) do
    if bus in @buses, do: :ok, else: {:error, "disk bus must be one of: #{Enum.join(@buses, ", ")}"}
  end

  # -- the optional fields --------------------------------------------------------------

  # One image name per CD-ROM, comma separated, which is what Vali splits. A name that
  # contains the separator would silently become two drives.
  defp validate_iso(value) do
    names =
      case value do
        list when is_list(list) -> Enum.map(list, &to_string_or_empty/1)
        other -> other |> to_string_or_empty() |> String.split(",")
      end
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))

    cond do
      Enum.any?(names, &String.match?(&1, ~r/[\x00-\x1f\x7f,:]/)) ->
        {:error, "an image name cannot contain ',' ':' or control characters"}

      length(names) > 8 ->
        {:error, "at most 8 CD-ROM drives are supported"}

      true ->
        {:ok, Enum.join(names, ",")}
    end
  end

  defp validate_boot_device(value) do
    case to_string_or_empty(value) do
      "" -> {:ok, ""}
      device when device in @boot_devices -> {:ok, device}
      _ -> {:error, "must be one of: #{Enum.join(@boot_devices, ", ")}"}
    end
  end

  defp validate_cpu_model(value) do
    case to_string_or_empty(value) do
      "" -> {:ok, ""}
      model when model in @cpu_models -> {:ok, model}
      _ -> {:error, "must be one of: #{Enum.join(@cpu_models, ", ")}"}
    end
  end

  # Anything that is not exactly "spice" is VNC, the same rule `normalise_graphics/1`
  # applies to a stored row -- but a *form* value that is neither is a mistake worth
  # reporting rather than quietly turning into VNC.
  defp validate_graphics(value) do
    case value |> to_string_or_empty() |> String.downcase() do
      "" -> {:ok, "vnc"}
      type when type in @graphics -> {:ok, type}
      _ -> {:error, "must be one of: #{Enum.join(@graphics, ", ")}"}
    end
  end

  # One network id, or the JSON list the old console wrote (`["<id>:virtio", ...]`), which
  # is what Vali parses -- one NIC per entry. An empty list is a VM with no NIC, which is
  # not the same as a VM that was not asked: blank means the default network.
  defp validate_networks(value) when is_list(value) do
    validate_networks(Jason.encode!(Enum.map(value, &to_string_or_empty/1)))
  end

  defp validate_networks(value) do
    case String.trim(to_string_or_empty(value)) do
      "" ->
        {:ok, @default_network_id}

      "[" <> _ = json ->
        case Jason.decode(json) do
          {:ok, entries} when is_list(entries) -> check_nics(entries, json)
          _ -> {:error, "is not a list of networks"}
        end

      single ->
        {:ok, single}
    end
  end

  defp check_nics(entries, json) do
    parsed = Enum.map(entries, &nic_entry/1)

    cond do
      length(entries) > @max_nics -> {:error, "at most #{@max_nics} network interfaces are supported"}
      Enum.any?(parsed, &(&1 == :error)) -> {:error, "every interface needs a network, and a known NIC model"}
      true -> {:ok, json}
    end
  end

  defp nic_entry(entry) when is_binary(entry) do
    case String.split(entry, ":") do
      [network] when network != "" -> network
      [network, model] when network != "" and model in @nic_models -> network
      _ -> :error
    end
  end

  defp nic_entry(_other), do: :error

  # The container name is appended to a storage argument, so it gets the
  # same treatment as a VM name rather than being passed through.
  defp validate_container(nil), do: :ok

  defp validate_container(container) do
    if valid_name?(container) do
      :ok
    else
      {:error, "storage container name #{@name_error}"}
    end
  end

  defp split_disks(nil), do: []

  defp split_disks(list) when is_binary(list) do
    case String.trim(list) do
      "" ->
        []

      "NONE" ->
        []

      trimmed ->
        trimmed |> String.split(",") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == ""))
    end
  end

  defp to_gib(digits, suffix) do
    case Integer.parse(digits) do
      {n, ""} when n > 0 ->
        case suffix do
          "T" -> {:ok, n * 1024}
          "TB" -> {:ok, n * 1024}
          "TIB" -> {:ok, n * 1024}
          _ -> {:ok, n}
        end

      {_n, _} ->
        {:error, "must be greater than 0"}

      :error ->
        {:error, "must be a whole number of gibibytes or tebibytes"}
    end
  end

  defp to_integer(value) when is_integer(value), do: {:ok, value}

  defp to_integer(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {n, ""} -> {:ok, n}
      _ -> :error
    end
  end

  defp to_integer(_other), do: :error

  defp fetch(params, key) do
    case Map.fetch(params, key) do
      {:ok, value} -> value
      :error -> Map.get(params, Atom.to_string(key))
    end
  end

  defp get(row, key, default) do
    case Map.get(row, key, default) do
      nil -> default
      value -> value
    end
  end

  defp to_string_or_empty(nil), do: ""
  defp to_string_or_empty(value) when is_binary(value), do: value
  defp to_string_or_empty(value), do: to_string(value)

  defp truthy?(true), do: true
  defp truthy?(value) when is_binary(value), do: String.downcase(value) in ~w(true on 1 yes)
  defp truthy?(_other), do: false

  # Anything that is not exactly "spice" is VNC, including nil -- which is what every row
  # written before the graphics column holds. The same rule as `normalise_graphics` in
  # spectrum_server.py and vali.py, and `vm_graphics_test.exs` asserts the three agree:
  # a VM the console calls SPICE and the daemon builds as VNC is a console that opens on
  # a protocol mismatch, which is the confusing failure rather than the loud one.
  defp normalise_graphics(value) when is_binary(value) do
    if String.downcase(String.trim(value)) == "spice", do: "spice", else: "vnc"
  end

  defp normalise_graphics(_other), do: "vnc"

  defp downcase(nil), do: ""
  defp downcase(value) when is_binary(value), do: String.downcase(value)
  defp downcase(value), do: value |> to_string() |> String.downcase()
end
