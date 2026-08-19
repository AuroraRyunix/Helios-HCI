defmodule SpectrumPhx.VmsStorageTest do
  @moduledoc """
  Disk allocation for `SpectrumPhx.Vms.create_vm/1`.

  The thing worth testing here is not the happy path -- it is what happens when disk two
  of three fails. Before this existed, a create wrote a metadata row and allocated no
  storage at all, which is worse than the feature being absent: the UI shows a VM, the
  operator starts it, and libvirt opens a device that was never created.

  The rollback rules are the point of most of these cases:

    * resources this call *created* are deleted,
    * resources it *adopted* (Spark answered `"created" => false`) are not, because they
      existed beforehand and may hold another VM's data,
    * and the metadata row is removed with a condition, so a VM that was claimed and
      started while allocation was running is never deleted out from under its host.
  """
  # Not async: the source and the storage client are configured through application env,
  # which is global.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias SpectrumPhx.Vms
  alias SpectrumPhx.Vms.Vm

  defp params(overrides \\ %{}) do
    Map.merge(
      %{
        "name" => "web-01",
        "vcpu" => "2",
        "memory" => "2048",
        "firmware" => "uefi",
        "disks" => "10G"
      },
      overrides
    )
  end

  # A stub storage client that records every call and answers from `responses`, keyed by
  # resource name. Anything not named there is created successfully.
  defp stub_storage(responses \\ %{}) do
    test_pid = self()

    Application.put_env(:spectrum_phx, :vms_storage_client, fn action, resource, opts ->
      send(test_pid, {:storage, action, resource, opts})

      case {action, Map.get(responses, resource)} do
        {:create, nil} -> {:ok, %{"created" => true, "device_path" => "/dev/drbd/by-res/x/0"}}
        {:create, response} -> response
        {:delete, _} -> {:ok, %{"deleted" => true}}
      end
    end)
  end

  setup do
    Application.put_env(:spectrum_phx, :vms_source, {:static, []})

    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :vms_source)
      Application.delete_env(:spectrum_phx, :vms_storage_client)
    end)

    :ok
  end

  describe "allocation" do
    test "allocates one Linstor resource per disk, named after the VM" do
      stub_storage()

      assert {:ok, %Vm{name: "web-01"}} =
               Vms.create_vm(params(%{"disks" => "10G,20G,30G"}))

      assert_received {:storage, :create, "web-01-disk0", %{size_gib: 10}}
      assert_received {:storage, :create, "web-01-disk1", %{size_gib: 20}}
      assert_received {:storage, :create, "web-01-disk2", %{size_gib: 30}}
      refute_received {:storage, :delete, _, _}
    end

    test "sizes reach the storage tier in GiB, with T converted" do
      stub_storage()

      assert {:ok, _vm} = Vms.create_vm(params(%{"disks" => "1T,512G,20"}))

      assert_received {:storage, :create, "web-01-disk0", %{size_gib: 1024}}
      assert_received {:storage, :create, "web-01-disk1", %{size_gib: 512}}
      # A bare number is GiB, as the form's help text says.
      assert_received {:storage, :create, "web-01-disk2", %{size_gib: 20}}
    end

    test "the resource names match the paths the VM record carries" do
      # This is the seam between the two tiers: the daemon builds
      # /dev/drbd/by-res/<resource>/0 from the same name, and the domain XML points at it.
      {:ok, vm} = Vm.new(params(%{"disks" => "10G,20G"}))

      assert Enum.map(Vm.disks(vm), & &1.resource) == ["web-01-disk0", "web-01-disk1"]

      assert Enum.map(Vm.disks(vm), & &1.path) == [
               "/dev/drbd/by-res/web-01-disk0/0",
               "/dev/drbd/by-res/web-01-disk1/0"
             ]
    end

    test "a create with no storage client configured allocates nothing" do
      # The default in a test must never reach a hypervisor: with no stub, a create under
      # a static source validates and returns without calling anything.
      assert {:ok, %Vm{name: "web-01"}} = Vms.create_vm(params())
      refute_received {:storage, _, _, _}
    end

    test "an invalid name is refused before any allocation" do
      stub_storage()

      assert {:error, errors} = Vms.create_vm(params(%{"name" => "a; id"}))
      assert Keyword.has_key?(errors, :name)
      refute_received {:storage, _, _, _}
    end

    test "an unusable disk size is refused before any allocation" do
      stub_storage()

      assert {:error, errors} = Vms.create_vm(params(%{"disks" => "512M"}))
      assert Keyword.has_key?(errors, :disks)
      refute_received {:storage, _, _, _}
    end
  end

  describe "rollback" do
    test "a failing disk deletes the resources already created" do
      stub_storage(%{"web-01-disk2" => {:error, {500, "no space left in default-pool"}}})

      assert {:error, {:storage, message}} =
               capture_and_create(params(%{"disks" => "10G,20G,30G"}))

      assert message =~ "disk 2"
      assert message =~ "web-01-disk2"
      assert message =~ "spark returned 500"
      assert message =~ "no space left"

      # Unwound newest first, and only the two that were actually created.
      assert_received {:storage, :delete, "web-01-disk1", _}
      assert_received {:storage, :delete, "web-01-disk0", _}
      refute_received {:storage, :delete, "web-01-disk2", _}
    end

    test "the first disk failing deletes nothing" do
      stub_storage(%{"web-01-disk0" => {:error, {500, "controller unreachable"}}})

      assert {:error, {:storage, message}} = capture_and_create(params(%{"disks" => "10G,20G"}))
      assert message =~ "disk 0"

      # The second disk is never attempted: a VM missing disk 0 is not a VM.
      refute_received {:storage, :create, "web-01-disk1", _}
      refute_received {:storage, :delete, _, _}
    end

    test "a resource that was adopted rather than created is never deleted" do
      # `"created" => false` means it was already there. Deleting it during a rollback
      # would destroy storage this call did not make -- in practice, another VM's disk.
      stub_storage(%{
        "web-01-disk0" => {:ok, %{"created" => false}},
        "web-01-disk1" => {:error, {409, "resource exists at a different size"}}
      })

      assert {:error, {:storage, message}} = capture_and_create(params(%{"disks" => "10G,20G"}))
      assert message =~ "spark returned 409"
      refute_received {:storage, :delete, _, _}
    end

    test "adopting an existing resource is logged" do
      stub_storage(%{"web-01-disk0" => {:ok, %{"created" => false}}})

      log = capture_log(fn -> assert {:ok, _vm} = Vms.create_vm(params()) end)

      assert log =~ "web-01-disk0"
      assert log =~ "leftover"
    end

    test "a transport failure is reported with the disk that failed" do
      stub_storage(%{"web-01-disk0" => {:error, :timeout}})

      assert {:error, {:storage, message}} = capture_and_create(params())
      assert message =~ "web-01-disk0"
      assert message =~ ":timeout"
    end

    test "a rollback delete that itself fails is logged as an orphan, not retried" do
      test_pid = self()

      Application.put_env(:spectrum_phx, :vms_storage_client, fn
        :create, "web-01-disk1", _opts ->
          {:error, {500, "placement failed"}}

        :create, resource, _opts ->
          send(test_pid, {:storage, :create, resource})
          {:ok, %{"created" => true}}

        :delete, resource, _opts ->
          send(test_pid, {:storage, :delete, resource})
          {:error, {409, "resource is in use"}}
      end)

      log =
        capture_log(fn ->
          assert {:error, {:storage, _}} = Vms.create_vm(params(%{"disks" => "10G,20G"}))
        end)

      assert_received {:storage, :delete, "web-01-disk0"}
      # Exactly one attempt: a delete that cannot land is an operator's repair, not a loop.
      refute_received {:storage, :delete, "web-01-disk0"}
      assert log =~ "orphan"
    end
  end

  describe "the metadata row and the storage it points at" do
    test "the rollback delete is conditional on the row this call inserted" do
      # An unconditional DELETE would also remove a VM that was claimed and started while
      # allocation was running -- a live VM's row, deleted by a failed create.
      assert Vms.delete_cql() ==
               "DELETE FROM hydra.vms WHERE name = ? IF state = ? AND host_ip = ?"
    end

    test "the row is inserted conditionally, which is what makes the ordering safe" do
      # The row goes in first so that exactly one caller reaches the storage path: disk
      # resources are named after the VM, and `resource-definition create` is idempotent
      # by adoption, so a loser of this race could not tell its resources from the
      # winner's and would delete live disks while rolling back.
      assert Vms.insert_cql() =~ "IF NOT EXISTS"
    end
  end

  # The failure paths log at error level by design; capture it so the suite output stays
  # readable while still asserting on the return value.
  defp capture_and_create(params) do
    result = nil

    capture_log(fn ->
      send(self(), {:result, Vms.create_vm(params)})
    end)

    receive do
      {:result, value} -> value
    after
      0 -> result
    end
  end
end
