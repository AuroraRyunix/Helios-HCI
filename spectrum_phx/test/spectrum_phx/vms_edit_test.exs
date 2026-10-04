defmodule SpectrumPhx.VmsEditTest do
  @moduledoc """
  Editing a stopped VM: `Vms.update_vm/2`, `Vms.edit_errors/2` and `Form.from_vm/1`.

  There was no way to change a VM once it was created. The rules the edit enforces are the
  ones a vdisk can honour -- a disk keeps its position because its vdisk is named after it, can
  only grow, and keeps its container -- and the order of the storage steps is what leaves the
  least behind when one fails, so most cases here drive a failure at a chosen step and look at
  what was undone.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias SpectrumPhx.Vms
  alias SpectrumPhx.Vms.Form
  alias SpectrumPhx.Vms.Vm

  @stopped %Vm{
    name: "web-01",
    vcpu: 2,
    memory: 2048,
    state: "Stopped",
    host_ip: "",
    firmware: "uefi",
    disks_list: "10GB:default-pool:virtio,20GB:default-pool:virtio",
    disk_path: "/var/lib/hci/sidon/nbd/web-01-disk0.sock",
    disk_size: 10,
    iso: "debian.iso",
    boot_device: "",
    network_id: ~s(["7a68e0d6-11f8-4e89-9430-b3b44b8bc438:virtio"]),
    cpu_model: "",
    graphics: "vnc",
    audio_enabled: false
  }

  setup do
    Application.put_env(:spectrum_phx, :vms_source, {:static, [@stopped]})

    Application.put_env(
      :spectrum_phx,
      :containers_source,
      {:static,
       [
         %{"name" => "default-pool", "tier" => "SSD", "ftt" => 1, "quota_bytes" => 0},
         %{"name" => "fast", "tier" => "NVME", "ftt" => 1, "quota_bytes" => 0}
       ]}
    )

    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :vms_source)
      Application.delete_env(:spectrum_phx, :containers_source)
      Application.delete_env(:spectrum_phx, :vms_storage_client)
    end)

    :ok
  end

  # A storage client that records every call and answers from `responses` keyed by
  # {action, resource}; anything else succeeds.
  defp stub_storage(responses \\ %{}) do
    test_pid = self()

    Application.put_env(:spectrum_phx, :vms_storage_client, fn action, resource, opts ->
      send(test_pid, {:storage, action, resource, opts})

      case Map.get(responses, {action, resource}) do
        nil ->
          case action do
            :create -> {:ok, %{"created" => true}}
            :resize -> {:ok, %{"resized" => true}}
            :delete -> {:ok, %{"deleted" => true}}
          end

        response ->
          response
      end
    end)
  end

  defp calls(acc \\ []) do
    receive do
      {:storage, action, resource, _opts} -> calls([{action, resource} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # The map `Form.to_attrs/1` hands the context, built from the VM's own form so an edit starts
  # from what the operator would see.
  defp edit_params(changes \\ %{}, vm \\ @stopped) do
    vm |> Form.from_vm() |> Map.merge(changes) |> Form.to_attrs()
  end

  defp disk_rows(rows), do: %{"disk_rows" => rows}

  defp row(size, container \\ "default-pool", bus \\ "virtio"),
    do: %{"size" => size, "unit" => "GB", "container" => container, "bus" => bus}

  describe "Form.from_vm/1 is to_attrs/1's inverse" do
    test "a VM round-trips through the form unchanged" do
      attrs = Form.to_attrs(Form.from_vm(@stopped))

      assert attrs["vcpu"] == "2"
      assert attrs["memory"] == "2048"
      assert attrs["disks"] == ["10GB:default-pool:virtio", "20GB:default-pool:virtio"]
      assert attrs["iso"] == ["debian.iso"]
      assert attrs["network_id"] == [~s(7a68e0d6-11f8-4e89-9430-b3b44b8bc438:virtio)]
      assert attrs["disk_indices"] == [0, 1]
      assert {:ok, %Vm{}} = Vm.new(attrs)
    end

    test "memory is shown in GB when it is a whole number of them, otherwise MB" do
      assert %{"memory" => "4", "memory_unit" => "GB"} = Form.from_vm(%{@stopped | memory: 4096})

      assert %{"memory" => "1536", "memory_unit" => "MB"} =
               Form.from_vm(%{@stopped | memory: 1536})
    end

    test "a disk of whole terabytes is shown in TB" do
      form = Form.from_vm(%{@stopped | disks_list: "2TB:default-pool:virtio"})
      assert %{"0" => %{"size" => "2", "unit" => "TB"}} = form["disk_rows"]
    end

    test "disk rows are keyed by the disk's position, which is its vdisk's number" do
      form = Form.from_vm(@stopped)
      assert Map.keys(form["disk_rows"]) |> Enum.sort() == ["0", "1"]
    end

    test "a single network, a missing one and an empty drive all come out as sane rows" do
      assert %{"0" => %{"network" => "net-1", "model" => "virtio"}} =
               Form.from_vm(%{@stopped | network_id: "net-1"})["nic_rows"]

      assert map_size(Form.from_vm(%{@stopped | network_id: nil})["nic_rows"]) == 1
      assert Form.from_vm(%{@stopped | iso: "__empty__"})["cdrom_rows"] == %{}
    end
  end

  describe "what is editable" do
    test "everything but the name, in one save" do
      stub_storage()

      params =
        edit_params(%{
          "vcpu" => "4",
          "memory" => "8",
          "firmware" => "bios",
          "boot_device" => "hd",
          "cpu_model" => "host-model",
          "graphics" => "spice",
          "audio_enabled" => "true",
          "name" => "ignored-rename"
        })

      assert {:ok, vm} = Vms.update_vm("web-01", params)
      assert %Vm{name: "web-01", vcpu: 4, memory: 8192, firmware: "bios", boot_device: "hd"} = vm
      assert %Vm{cpu_model: "host-model", graphics: "spice", audio_enabled: true} = vm
      assert vm.state == "Stopped"
      assert vm.host_ip == ""
    end

    test "CD-ROMs and NICs" do
      stub_storage()
      form = Form.from_vm(@stopped)

      params =
        form
        |> Map.put("cdrom_rows", %{
          "0" => %{"image" => "rocky.iso"},
          "1" => %{"image" => "tools.iso"}
        })
        |> Map.put("nic_rows", %{"0" => %{"network" => "net-9", "model" => "e1000"}})
        |> Form.to_attrs()

      assert {:ok, vm} = Vms.update_vm("web-01", params)
      assert vm.iso == "rocky.iso,tools.iso"
      assert vm.network_id == ~s(["net-9:e1000"])
    end

    test "an unknown VM is not found and a bad name is not looked up" do
      assert {:error, :not_found} = Vms.update_vm("ghost", edit_params())
      assert {:error, :invalid_name} = Vms.update_vm("a; rm -rf /", edit_params())
    end

    test "invalid values are reported field by field and nothing is touched" do
      stub_storage()

      assert {:error, errors} =
               Vms.update_vm("web-01", edit_params(%{"vcpu" => "0", "firmware" => "coreboot"}))

      assert errors[:vcpu] && errors[:firmware]
      assert calls() == []
    end
  end

  describe "a VM that is not stopped is not edited" do
    for {label, vm} <- [
          running: %{@stopped | state: "Running", host_ip: "10.0.0.1"},
          placed: %{@stopped | host_ip: "10.0.0.1"},
          migrating: %{@stopped | status: "migrating"}
        ] do
      test "#{label}" do
        Application.put_env(:spectrum_phx, :vms_source, {:static, [unquote(Macro.escape(vm))]})
        stub_storage()
        assert {:error, :not_stopped} = Vms.update_vm("web-01", edit_params())
        assert calls() == []
        refute Vms.editable?(unquote(Macro.escape(vm)))
      end
    end

    test "a stopped VM is editable" do
      assert Vms.editable?(@stopped)
    end
  end

  describe "disks: grow, add, remove" do
    test "an existing disk can grow, and only the grown one is resized" do
      stub_storage()
      params = edit_params(disk_rows(%{"0" => row("10"), "1" => row("40")}))
      assert {:ok, vm} = Vms.update_vm("web-01", params)
      assert vm.disks_list == "10GB:default-pool:virtio,40GB:default-pool:virtio"
      assert calls() == [{:resize, "web-01-disk1"}]
    end

    test "a disk cannot shrink" do
      stub_storage()

      assert {:error, [disks: message]} =
               Vms.update_vm(
                 "web-01",
                 edit_params(disk_rows(%{"0" => row("5"), "1" => row("20")}))
               )

      assert message =~ "can only grow"
      assert calls() == []
    end

    test "an existing disk cannot change container" do
      stub_storage()

      assert {:error, [disks: message]} =
               Vms.update_vm(
                 "web-01",
                 edit_params(disk_rows(%{"0" => row("10", "fast"), "1" => row("20")}))
               )

      assert message =~ "another container"
      assert calls() == []
    end

    test "a new disk is created in its container and appended" do
      stub_storage()

      params =
        edit_params(disk_rows(%{"0" => row("10"), "1" => row("20"), "2" => row("100", "fast")}))

      assert {:ok, vm} = Vms.update_vm("web-01", params)
      assert vm.disks_list =~ "100GB:fast:virtio"
      assert calls() == [{:create, "web-01-disk2"}]
    end

    test "only the last disk can be removed, and removing it deletes its vdisk after the row is written" do
      stub_storage()
      assert {:ok, vm} = Vms.update_vm("web-01", edit_params(disk_rows(%{"0" => row("10")})))
      assert vm.disks_list == "10GB:default-pool:virtio"
      assert calls() == [{:delete, "web-01-disk1"}]
    end

    test "a disk removed from the middle is refused: the vdisks are named after the position" do
      Application.put_env(
        :spectrum_phx,
        :vms_source,
        {:static,
         [
           %{
             @stopped
             | disks_list:
                 "10GB:default-pool:virtio,20GB:default-pool:virtio,30GB:default-pool:virtio"
           }
         ]}
      )

      stub_storage()

      vm = %{
        @stopped
        | disks_list: "10GB:default-pool:virtio,20GB:default-pool:virtio,30GB:default-pool:virtio"
      }

      params = edit_params(disk_rows(%{"0" => row("10"), "2" => row("30")}), vm)
      assert {:error, [disks: message]} = Vms.update_vm("web-01", params)
      assert message =~ "only the last disks can be removed"
      assert calls() == []
    end

    test "growth, a new disk and a removal in one save, in the safe order" do
      stub_storage()

      vm = %{
        @stopped
        | disks_list: "10GB:default-pool:virtio,20GB:default-pool:virtio,30GB:default-pool:virtio"
      }

      Application.put_env(:spectrum_phx, :vms_source, {:static, [vm]})
      params = edit_params(disk_rows(%{"0" => row("15"), "1" => row("20")}), vm)
      assert {:ok, _} = Vms.update_vm("web-01", params)
      assert calls() == [{:resize, "web-01-disk0"}, {:delete, "web-01-disk2"}]
    end

    test "there is always at least one disk" do
      stub_storage()
      assert {:error, errors} = Vms.update_vm("web-01", edit_params(disk_rows(%{})))
      assert errors[:disks] =~ "at least one disk"
    end

    test "edit_errors/2 is the same verdict without doing anything" do
      stub_storage()
      assert Vms.edit_errors(@stopped, edit_params()) == []

      assert [disks: _] =
               Vms.edit_errors(
                 @stopped,
                 edit_params(disk_rows(%{"0" => row("1"), "1" => row("20")}))
               )

      assert calls() == []
    end
  end

  describe "failure leaves the least behind" do
    test "a failed create undoes the vdisks this edit created, and writes nothing" do
      stub_storage(%{{:create, "web-01-disk3"} => {:error, {409, "no space"}}})

      params =
        edit_params(
          disk_rows(%{"0" => row("10"), "1" => row("20"), "2" => row("5"), "3" => row("5")})
        )

      capture_log(fn ->
        assert {:error, {:storage, message}} = Vms.update_vm("web-01", params)
        assert message =~ "web-01-disk3" and message =~ "no space"
      end)

      assert calls() == [
               {:create, "web-01-disk2"},
               {:create, "web-01-disk3"},
               {:delete, "web-01-disk2"}
             ]
    end

    test "a container that does not exist is refused before any storage is touched" do
      stub_storage()

      params =
        edit_params(
          disk_rows(%{"0" => row("10"), "1" => row("20"), "2" => row("5", "no-such-pool")})
        )

      assert {:error, {:storage, message}} = Vms.update_vm("web-01", params)
      assert message =~ "disk container"
      assert calls() == []
    end

    test "a failed grow stops the edit before anything is created" do
      stub_storage(%{{:resize, "web-01-disk1"} => {:error, {409, "not owned here"}}})
      params = edit_params(disk_rows(%{"0" => row("10"), "1" => row("40"), "2" => row("5")}))

      capture_log(fn ->
        assert {:error, {:storage, message}} = Vms.update_vm("web-01", params)
        assert message =~ "not owned here"
      end)

      assert calls() == [{:resize, "web-01-disk1"}]
    end

    test "a vdisk that could not be deleted after the row changed is an orphan, said loudly, not a failure" do
      stub_storage(%{{:delete, "web-01-disk1"} => {:error, {409, "still attached"}}})

      log =
        capture_log(fn ->
          assert {:ok, vm} = Vms.update_vm("web-01", edit_params(disk_rows(%{"0" => row("10")})))
          assert vm.disks_list == "10GB:default-pool:virtio"
        end)

      assert log =~ "orphan"
      assert log =~ "web-01-disk1"
    end

    test "a bus change is recorded and touches no storage" do
      stub_storage()

      assert {:ok, vm} =
               Vms.update_vm(
                 "web-01",
                 edit_params(
                   disk_rows(%{"0" => row("10", "default-pool", "sata"), "1" => row("20")})
                 )
               )

      assert vm.disks_list =~ "10GB:default-pool:sata"
      assert calls() == []
    end
  end

  describe "the write" do
    test "is a compare-and-swap on the VM still being stopped and unplaced" do
      assert Vms.update_cql() =~ "WHERE name = ? IF state = ? AND host_ip = ?"
      assert Vms.update_cql() |> String.graphemes() |> Enum.count(&(&1 == "?")) == 15
    end

    test "carries the new definition and the expectation in the order the statement binds them" do
      {:ok, vm} = Vm.new(Map.put(edit_params(), "name", "web-01"))
      params = Vms.update_params(vm, @stopped)

      assert length(params) == 15
      assert {"text", "web-01"} == Enum.at(params, 12)
      assert {"text", "Stopped"} == Enum.at(params, 13)
      assert {"text", ""} == Enum.at(params, 14)
      assert {"int", 2} == Enum.at(params, 0)
    end
  end
end
