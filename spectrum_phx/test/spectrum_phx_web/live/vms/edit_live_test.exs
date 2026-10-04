defmodule SpectrumPhxWeb.Vms.EditLiveTest do
  @moduledoc """
  The edit page for a stopped VM (`/vms/:name/edit`), which is the creation form in edit mode.

  A VM could not be changed once created. What is checked here is the page's contract: it starts
  as the VM it edits, the name is fixed, a disk keeps its container and can only be removed from
  the end, saving goes through `Vms.update_vm/2`, and a VM that is running is sent back to its
  page with the reason instead of being offered an edit that would fail.
  """
  # Not async: the VM source, the storage stub and the options are application env.
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

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
    iso: "rocky-10.iso",
    boot_device: "",
    network_id: ~s(["net-prod:virtio"]),
    cpu_model: "",
    graphics: "vnc",
    audio_enabled: false
  }

  @running %Vm{@stopped | name: "live-01", state: "Running", host_ip: "10.0.0.11"}

  setup %{conn: conn} do
    Application.put_env(:spectrum_phx, :vms_source, {:static, [@stopped, @running]})

    Application.put_env(
      :spectrum_phx,
      :containers_source,
      {:static,
       [
         %{"name" => "default-pool", "tier" => "SSD", "ftt" => 1, "quota_bytes" => 0},
         %{"name" => "fast", "tier" => "NVME", "ftt" => 1, "quota_bytes" => 0}
       ]}
    )

    Application.put_env(:spectrum_phx, :vm_options, %{
      containers: ["default-pool", "fast"],
      images: [
        %{name: "rocky-10.iso", label: "rocky-10.iso"},
        %{name: "tools.iso", label: "tools.iso"}
      ],
      networks: [%{id: "net-prod", label: "prod"}, %{id: "net-lab", label: "lab"}],
      spice?: false,
      notes: []
    })

    test_pid = self()

    Application.put_env(:spectrum_phx, :vms_storage_client, fn action, resource, opts ->
      send(test_pid, {:storage, action, resource, opts})
      {:ok, %{"created" => true, "resized" => true, "deleted" => true}}
    end)

    on_exit(fn ->
      for key <- [:vms_source, :containers_source, :vm_options, :vms_storage_client],
          do: Application.delete_env(:spectrum_phx, key)
    end)

    %{conn: log_in(conn)}
  end

  defp storage_calls(acc \\ []) do
    receive do
      {:storage, action, resource, _} -> storage_calls([{action, resource} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  describe "the form" do
    test "starts as the VM it edits", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/vms/web-01/edit")

      assert html =~ "Edit web-01"
      assert view |> element("input[name='vm[name]'][value='web-01'][readonly]") |> has_element?()
      assert view |> element("input[name='vm[vcpu]'][value='2']") |> has_element?()
      assert view |> element("input[name='vm[memory]'][value='2']") |> has_element?()
      assert view |> element("#disk-row-0") |> has_element?()
      assert view |> element("#disk-row-1") |> has_element?()

      assert view
             |> element("#cdrom-row-0 option[selected][value='rocky-10.iso']")
             |> has_element?()

      assert view |> element("#nic-row-0 option[selected][value='net-prod']") |> has_element?()
      assert view |> element("#save-vm") |> has_element?()
      refute view |> element("#create-vm") |> has_element?()
    end

    test "only the last disk can be removed", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/web-01/edit")

      refute view |> element("#disk-row-0 button[aria-label='Remove disk']") |> has_element?()
      assert view |> element("#disk-row-1 button[aria-label='Remove disk']") |> has_element?()
    end

    test "an existing disk's container is fixed and still submitted", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/web-01/edit")

      assert view
             |> element("#disk-row-0 select[name='vm[disk_rows][0][container]'][disabled]")
             |> has_element?()

      assert view
             |> element(
               "#disk-row-0 input[type='hidden'][name='vm[disk_rows][0][container]'][value='default-pool']"
             )
             |> has_element?()

      view |> element("#add-disk") |> render_click()

      refute view |> element("#disk-row-2 select[disabled]") |> has_element?(),
             "a new disk may pick its container"
    end

    test "a shrink is a field error while typing", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/web-01/edit")

      html =
        render_change(view, "validate", %{
          "vm" => %{
            "structured" => "true",
            "vcpu" => "2",
            "memory" => "2",
            "memory_unit" => "GB",
            "firmware" => "uefi",
            "disk_rows" => %{
              "0" => %{
                "size" => "2",
                "unit" => "GB",
                "container" => "default-pool",
                "bus" => "virtio"
              },
              "1" => %{
                "size" => "20",
                "unit" => "GB",
                "container" => "default-pool",
                "bus" => "virtio"
              }
            }
          }
        })

      assert html =~ "can only grow"
    end
  end

  describe "saving" do
    test "writes the change and returns to the VM's page", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/web-01/edit")

      form_data = %{
        "vm" => %{
          "structured" => "true",
          "name" => "web-01",
          "vcpu" => "6",
          "memory" => "4",
          "memory_unit" => "GB",
          "firmware" => "uefi",
          "boot_device" => "hd",
          "disk_rows" => %{
            "0" => %{
              "size" => "10",
              "unit" => "GB",
              "container" => "default-pool",
              "bus" => "virtio"
            },
            "1" => %{
              "size" => "50",
              "unit" => "GB",
              "container" => "default-pool",
              "bus" => "virtio"
            }
          },
          "cdrom_rows" => %{"0" => %{"image" => "tools.iso"}},
          "nic_rows" => %{"0" => %{"network" => "net-lab", "model" => "virtio"}}
        }
      }

      result = view |> form("#vm-form") |> render_submit(form_data)
      assert {:ok, _view, html} = follow_redirect(result, conn, "/vms/web-01")
      assert html =~ "web-01 updated"
      assert storage_calls() == [{:resize, "web-01-disk1"}]
    end

    test "a refused edit stays on the form with the reason", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/web-01/edit")

      html =
        render_submit(view, "save", %{
          "vm" => %{
            "structured" => "true",
            "vcpu" => "2",
            "memory" => "2",
            "memory_unit" => "GB",
            "firmware" => "uefi",
            "disk_rows" => %{
              "0" => %{
                "size" => "10",
                "unit" => "GB",
                "container" => "default-pool",
                "bus" => "virtio"
              },
              "2" => %{
                "size" => "20",
                "unit" => "GB",
                "container" => "default-pool",
                "bus" => "virtio"
              }
            }
          }
        })

      assert html =~ "only the last disks can be removed"
      assert html =~ "Edit web-01"
      assert storage_calls() == []
    end
  end

  describe "a VM that is not stopped" do
    test "is sent back to its page with the reason", %{conn: conn} do
      assert {:ok, _view, html} =
               follow_redirect(live(conn, ~p"/vms/live-01/edit"), conn, "/vms/live-01")

      assert html =~ "stop it to edit it"
    end

    test "and a VM that does not exist is sent to the list", %{conn: conn} do
      assert {:ok, _view, html} = follow_redirect(live(conn, ~p"/vms/ghost/edit"), conn, "/vms")
      assert html =~ "not found"
    end
  end

  describe "the VM's page" do
    test "offers Edit on a stopped VM and the reason on a running one", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/web-01")
      assert view |> element("#edit") |> has_element?()

      {:ok, view, _html} = live(conn, ~p"/vms/live-01")
      refute view |> element("#edit") |> has_element?()
      assert view |> element("#edit-hint") |> has_element?()
    end
  end
end
