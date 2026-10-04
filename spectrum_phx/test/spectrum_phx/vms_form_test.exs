defmodule SpectrumPhx.VmsFormTest do
  @moduledoc """
  The create form's option set, as the old console had it.

  The port reduced disks, CD-ROMs and networks to three text boxes and dropped the graphics
  device, the bus, the NIC model and the memory unit. The old form wrote strings the rest of
  the system already reads -- `20GB:default-pool:virtio` in `disks_list`, a JSON list of
  `network:model` in `network_id`, comma-separated image names in `iso` -- so these tests pin
  the strings, not the markup.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Vms.Form
  alias SpectrumPhx.Vms.Vm

  defp structured(overrides \\ %{}) do
    Map.merge(
      Form.defaults(container: "default-pool", network: "net-1")
      |> Map.put("name", "web-01"),
      overrides
    )
  end

  defp build(params), do: params |> Form.to_attrs() |> Vm.new()

  describe "disks" do
    test "every row becomes size:container:bus, in the order they were added" do
      params =
        structured(%{
          "disk_rows" => %{
            "0" => Form.disk_row("default-pool", "40"),
            "2" => %{"size" => "2", "unit" => "TB", "container" => "fast", "bus" => "sata"},
            "1" => %{"size" => "100", "unit" => "GB", "container" => "fast", "bus" => "scsi"}
          }
        })

      assert {:ok, vm} = build(params)

      assert vm.disks_list == "40GB:default-pool:virtio,100GB:fast:scsi,2TB:fast:sata"
      assert vm.disk_size == 40
      assert [first, second, third] = Vm.disks(vm)
      assert {first.size_gib, second.size_gib, third.size_gib} == {40, 100, 2048}
      assert {first.bus, second.bus, third.bus} == {"virtio", "scsi", "sata"}
      assert {first.container, second.container} == {"default-pool", "fast"}
    end

    test "the three-part entry the old console wrote is accepted, not read as a container named 'x:virtio'" do
      # Splitting on the first colon only made the container `default-pool:virtio`, which is
      # not a name, so every disk the old form could describe was refused.
      assert {:ok, vm} = Vm.new(%{"name" => "a", "vcpu" => "1", "memory" => "1024", "disks" => "20GB:default-pool:virtio"})
      assert [%{container: "default-pool", bus: "virtio"}] = Vm.disks(vm)
    end

    test "an unknown bus is a field error" do
      assert {:error, errors} =
               Vm.new(%{"name" => "a", "vcpu" => "1", "memory" => "1024", "disks" => "20G:default-pool:ide"})

      assert errors[:disks] =~ "bus must be one of"
    end

    test "an empty size is reported as missing, not turned into a size of just the unit" do
      params = structured(%{"disk_rows" => %{"0" => Form.disk_row("default-pool", "")}})

      assert {:error, errors} = build(params)
      assert errors[:disks] =~ "gibibytes or tebibytes"
    end

    test "removing every row is a field error: a VM needs at least one disk" do
      assert {:error, errors} = build(structured(%{"disk_rows" => %{}}))
      assert errors[:disks] == "at least one disk is required"
    end

    test "row indices are never reused, so removing one row does not rename the others" do
      params = structured() |> Form.add_row("disk_rows", Form.disk_row("c")) |> Form.add_row("disk_rows", Form.disk_row("c"))
      assert params["disk_rows"] |> Map.keys() |> Enum.sort() == ["0", "1", "2"]

      params = Form.remove_row(params, "disk_rows", "1")
      params = Form.add_row(params, "disk_rows", Form.disk_row("c"))

      assert params["disk_rows"] |> Map.keys() |> Enum.sort() == ["0", "2", "3"]
    end
  end

  describe "CD-ROMs" do
    test "each drive is one image, joined the way Vali splits them, and empty drives are dropped" do
      params =
        structured(%{
          "cdrom_rows" => %{
            "0" => %{"image" => "rocky-10.iso"},
            "1" => %{"image" => ""},
            "2" => %{"image" => "virtio-win.iso"}
          }
        })

      assert {:ok, vm} = build(params)
      assert vm.iso == "rocky-10.iso,virtio-win.iso"
    end

    test "no rows means no ISO" do
      assert {:ok, %Vm{iso: ""}} = build(structured())
    end

    test "an image name that would silently become two drives is refused" do
      params = structured(%{"cdrom_rows" => %{"0" => %{"image" => "a.iso,b.iso"}}})
      assert {:error, errors} = build(params)
      assert errors[:iso] =~ "cannot contain"
    end
  end

  describe "network interfaces" do
    test "each NIC is network:model in a JSON list, which is what Vali parses" do
      params =
        structured(%{
          "nic_rows" => %{
            "0" => %{"network" => "net-1", "model" => "virtio"},
            "1" => %{"network" => "net-2", "model" => "e1000e"}
          }
        })

      assert {:ok, vm} = build(params)
      assert Jason.decode!(vm.network_id) == ["net-1:virtio", "net-2:e1000e"]
    end

    test "no NIC is an isolated VM, not the default network" do
      assert {:ok, vm} = build(structured(%{"nic_rows" => %{}}))
      assert vm.network_id == "[]"
    end

    test "an unknown NIC model is refused" do
      params = structured(%{"nic_rows" => %{"0" => %{"network" => "net-1", "model" => "ne2000"}}})
      assert {:error, errors} = build(params)
      assert errors[:network_id] =~ "NIC model"
    end

    test "the flat form still means the default network when it is blank" do
      assert {:ok, vm} = Vm.new(%{"name" => "a", "vcpu" => "1", "memory" => "1024", "disks" => "10G", "network_id" => ""})
      assert vm.network_id == Vm.default_network_id()
    end
  end

  describe "compute and console" do
    test "memory given in GB is converted to MiB" do
      assert {:ok, vm} = build(structured(%{"memory" => "4", "memory_unit" => "GB"}))
      assert vm.memory == 4096

      assert {:ok, vm} = build(structured(%{"memory" => "2048", "memory_unit" => "MB"}))
      assert vm.memory == 2048
    end

    test "graphics is VNC by default and SPICE when asked, never anything else" do
      assert {:ok, %Vm{graphics: "vnc"}} = build(structured())
      assert {:ok, %Vm{graphics: "spice"}} = build(structured(%{"graphics" => "spice"}))
      assert {:error, errors} = build(structured(%{"graphics" => "rdp"}))
      assert errors[:graphics] =~ "must be one of"
    end

    test "boot device and CPU model are validated against what Vali honours" do
      assert {:ok, %Vm{boot_device: "cdrom", cpu_model: "Haswell-noTSX"}} =
               build(structured(%{"boot_device" => "cdrom", "cpu_model" => "Haswell-noTSX"}))

      # "Network (PXE)" was refused while Vali treated anything but cdrom as disk-first. The domain
      # now gives each device its own boot order and puts the first NIC first for it, so it is
      # accepted; anything else is still refused.
      assert {:ok, %Vm{boot_device: "network"}} = build(structured(%{"boot_device" => "network"}))
      assert {:error, errors} = build(structured(%{"boot_device" => "floppy"}))
      assert errors[:boot_device]
      assert {:error, errors} = build(structured(%{"cpu_model" => "pentium"}))
      assert errors[:cpu_model]
    end

    test "firmware stays uefi or bios" do
      assert {:ok, %Vm{firmware: "bios"}} = build(structured(%{"firmware" => "bios"}))
    end
  end

  describe "the flat form" do
    test "is passed through untouched, so the context's existing callers are unaffected" do
      params = %{"name" => "a", "vcpu" => "2", "memory" => "2048", "disks" => "10G,500G:fast"}
      assert Form.to_attrs(params) == params
    end
  end
end
