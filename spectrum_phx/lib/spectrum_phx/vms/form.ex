defmodule SpectrumPhx.Vms.Form do
  @moduledoc """
  The create form's shape, kept apart from the page that draws it.

  The old console built a VM from repeatable rows: any number of disks (a size, a unit, a
  container, a bus), any number of CD-ROM drives (an image each) and any number of NICs (a
  network and a model each). The port collapsed those into three text boxes, and a
  comma-separated string is not a replacement for a list -- there is nowhere to pick a
  container from, nothing stops a typo in one entry, and the bus and NIC model cannot be
  expressed at all.

  A row set travels in the form as an indexed map, `vm[disk_rows][0][size]`, which is how
  Phoenix submits nested inputs. Indices are never reused within one form, so removing the
  first of three rows does not rename the other two underneath an operator who is typing in
  one of them.

  A form that carries `"structured" => "true"` is the row form. Anything else is the flat
  one -- `"disks" => "10G,500G"` -- and is passed through untouched, because the context
  and its tests have always taken that shape and still do.

  `to_attrs/1` is the one place a row form becomes what `SpectrumPhx.Vms.Vm.new/1` takes,
  so the form and the write path cannot disagree about what a row means.
  """

  @disk_units ~w(GB TB)
  @memory_units ~w(MB GB)

  @doc "Units a disk size can be given in."
  def disk_units, do: @disk_units

  @doc "Units the memory field can be given in."
  def memory_units, do: @memory_units

  @doc """
  The form a new VM starts as: one boot disk, no CD-ROM drive, one NIC on the default
  network -- the same starting point the old wizard opened with.
  """
  def defaults(opts \\ []) do
    container = Keyword.get(opts, :container, "default-pool")
    network = Keyword.get(opts, :network, SpectrumPhx.Vms.Vm.default_network_id())

    %{
      "structured" => "true",
      "name" => "",
      "vcpu" => "2",
      "memory" => "4",
      "memory_unit" => "GB",
      "firmware" => "uefi",
      "boot_device" => "",
      "cpu_model" => "",
      "graphics" => "vnc",
      "audio_enabled" => "false",
      "disk_rows" => %{"0" => disk_row(container)},
      "cdrom_rows" => %{},
      "nic_rows" => %{"0" => nic_row(network)}
    }
  end

  @doc "A new disk row."
  def disk_row(container, size \\ "20") do
    %{"size" => size, "unit" => "GB", "container" => container, "bus" => "virtio"}
  end

  @doc "A new CD-ROM row: no image."
  def cdrom_row, do: %{"image" => ""}

  @doc "A new NIC row."
  def nic_row(network), do: %{"network" => network, "model" => "virtio"}

  @doc "The rows under `key`, as `[{index, row}]` in the order they were added."
  def rows(params, key) do
    case Map.get(params, key) do
      rows when is_map(rows) ->
        rows |> Enum.sort_by(fn {index, _row} -> integer(index) end)

      _ ->
        []
    end
  end

  @doc "Append a row. The new index is one past the highest ever used, never a reused one."
  def add_row(params, key, row) do
    existing = Map.get(params, key) || %{}

    next =
      case existing |> Map.keys() |> Enum.map(&integer/1) do
        [] -> 0
        indices -> Enum.max(indices) + 1
      end

    Map.put(params, key, Map.put(existing, Integer.to_string(next), row))
  end

  @doc "Remove the row at `index`."
  def remove_row(params, key, index) do
    Map.update(params, key, %{}, fn rows -> Map.delete(rows || %{}, to_string(index)) end)
  end

  @doc """
  What `Vm.new/1` takes, from a row form.

  Memory is converted to MiB here when it was given in GB. The disk and NIC rows become the
  strings Vali reads back out of `hydra.vms`: `20GB:default-pool:virtio` and a JSON list of
  `network:model`.
  """
  def to_attrs(%{"structured" => "true"} = params) do
    params
    |> Map.put("disks", Enum.map(rows(params, "disk_rows"), &disk_entry/1))
    # The positions the disk rows carry, for an edit: a row keeps the index of the vdisk it
    # describes, so a gap means a disk was removed from the middle, which cannot be honoured.
    |> Map.put(
      "disk_indices",
      Enum.map(rows(params, "disk_rows"), fn {index, _row} -> integer(index) end)
    )
    |> Map.put("iso", Enum.map(rows(params, "cdrom_rows"), fn {_i, row} -> row["image"] end))
    |> Map.put("network_id", Enum.map(rows(params, "nic_rows"), &nic_entry/1))
    |> Map.put("memory", memory_mib(params["memory"], params["memory_unit"]))
  end

  def to_attrs(params), do: params

  @doc """
  The form an existing VM starts as, for editing it: `to_attrs/1`'s inverse.

  Disk rows are keyed by the disk's position, which is also its vdisk's number; that identity is
  what `Vms.update_vm/2` relies on, so a row added later takes the next number and a row removed
  from the middle leaves a gap that is refused. Memory is shown in GB when it is a whole number
  of them, and a disk in TB when it is a whole number of those.
  """
  def from_vm(%SpectrumPhx.Vms.Vm{} = vm) do
    {memory, memory_unit} = memory_fields(vm.memory)

    %{
      "structured" => "true",
      "name" => vm.name,
      "vcpu" => to_string(vm.vcpu),
      "memory" => memory,
      "memory_unit" => memory_unit,
      "firmware" => vm.firmware || "uefi",
      "boot_device" => vm.boot_device || "",
      "cpu_model" => vm.cpu_model || "",
      "graphics" => vm.graphics || "vnc",
      "audio_enabled" => to_string(vm.audio_enabled == true),
      "disk_rows" =>
        vm |> SpectrumPhx.Vms.Vm.disks() |> Enum.map(&disk_form_row/1) |> index_rows(),
      "cdrom_rows" =>
        (vm.iso || "")
        |> String.split(",", trim: true)
        |> Enum.map(&%{"image" => String.trim(&1)})
        |> Enum.reject(&(&1["image"] in ["", "__empty__"]))
        |> index_rows(),
      "nic_rows" => vm.network_id |> nic_rows() |> index_rows()
    }
  end

  defp index_rows(rows) do
    rows |> Enum.with_index() |> Map.new(fn {row, index} -> {Integer.to_string(index), row} end)
  end

  defp memory_fields(mib) when is_integer(mib) and mib >= 1024 and rem(mib, 1024) == 0,
    do: {Integer.to_string(div(mib, 1024)), "GB"}

  defp memory_fields(mib), do: {to_string(mib), "MB"}

  defp disk_form_row(disk) do
    gib = disk.size_gib || 0

    {size, unit} =
      if gib >= 1024 and rem(gib, 1024) == 0, do: {div(gib, 1024), "TB"}, else: {gib, "GB"}

    %{
      "size" => Integer.to_string(size),
      "unit" => unit,
      "container" => disk.container || "",
      "bus" => disk.bus
    }
  end

  # `network_id` is a JSON list of "network:model" entries, a single "network" or "network:model",
  # or empty (the default network).
  defp nic_rows(nil), do: [nic_row(SpectrumPhx.Vms.Vm.default_network_id())]
  defp nic_rows(""), do: [nic_row(SpectrumPhx.Vms.Vm.default_network_id())]

  defp nic_rows("[" <> _ = json) do
    case Jason.decode(json) do
      {:ok, entries} when is_list(entries) -> Enum.map(entries, &nic_from_entry/1)
      _ -> []
    end
  end

  defp nic_rows(single), do: [nic_from_entry(single)]

  defp nic_from_entry(entry) do
    case entry |> to_string() |> String.split(":") do
      [network] -> nic_row(network)
      [network, model | _] -> %{"network" => network, "model" => model}
    end
  end

  defp disk_entry({_index, row}) do
    size = String.trim(to_string(row["size"]))
    unit = if row["unit"] in @disk_units, do: row["unit"], else: "GB"
    container = String.trim(to_string(row["container"]))
    bus = String.trim(to_string(row["bus"]))

    # An empty size stays empty, so the validator can say it is missing rather than this
    # turning "" into a size of "GB".
    sized = if size == "", do: "", else: size <> unit

    cond do
      bus == "" and container == "" -> sized
      bus == "" -> sized <> ":" <> container
      true -> sized <> ":" <> container <> ":" <> bus
    end
  end

  defp nic_entry({_index, row}) do
    network = String.trim(to_string(row["network"]))
    model = String.trim(to_string(row["model"]))
    if model == "", do: network, else: network <> ":" <> model
  end

  defp memory_mib(value, "GB") do
    case Integer.parse(String.trim(to_string(value))) do
      {n, ""} -> Integer.to_string(n * 1024)
      _ -> value
    end
  end

  defp memory_mib(value, _unit), do: value

  defp integer(value) do
    case Integer.parse(to_string(value)) do
      {n, _} -> n
      :error -> 0
    end
  end
end
