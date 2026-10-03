defmodule SpectrumPhxWeb.Vms.NewLiveRowsTest do
  @moduledoc """
  The create form's repeatable rows and the options it offers.

  The port reduced disks and CD-ROMs to one text box each and dropped graphics, bus, NIC
  model and the memory unit. A VM that needed anything beyond the defaults could not be
  created from the console at all.
  """
  # Not async: the VM source, the storage stub and the options are application env.
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  setup %{conn: conn} do
    Application.put_env(:spectrum_phx, :vms_source, {:static, []})

    Application.put_env(:spectrum_phx, :containers_source,
      {:static,
       [
         %{"name" => "default-pool", "tier" => "SSD", "ftt" => 1, "quota_bytes" => 0},
         %{"name" => "fast", "tier" => "NVME", "ftt" => 1, "quota_bytes" => 0}
       ]}
    )

    Application.put_env(:spectrum_phx, :vm_options, %{
      containers: ["default-pool", "fast"],
      images: [
        %{name: "rocky-10.iso", label: "rocky-10.iso (1.20 GB)"},
        %{name: "virtio-win.iso", label: "virtio-win.iso (0.60 GB)"}
      ],
      networks: [
        %{id: "net-prod", label: "prod (VLAN 100)"},
        %{id: "net-lab", label: "lab (Direct)"}
      ],
      spice?: false,
      notes: []
    })

    test = self()

    Application.put_env(:spectrum_phx, :vms_storage_client, fn action, resource, opts ->
      send(test, {:storage, action, resource, opts})
      {:ok, %{"created" => true}}
    end)

    on_exit(fn ->
      for key <- ~w(vms_source containers_source vm_options vms_storage_client)a,
          do: Application.delete_env(:spectrum_phx, key)
    end)

    %{conn: log_in(conn)}
  end

  describe "disks" do
    test "start as one boot disk and can be added to and removed from", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      assert has_element?(view, "#disk-row-0")
      refute has_element?(view, "#disk-row-1")

      view |> element("#add-disk") |> render_click()
      view |> element("#add-disk") |> render_click()
      assert has_element?(view, "#disk-row-1")
      assert has_element?(view, "#disk-row-2")

      view |> element("#disk-row-1 button[aria-label='Remove disk']") |> render_click()
      refute has_element?(view, "#disk-row-1")
      assert has_element?(view, "#disk-row-0")
      assert has_element?(view, "#disk-row-2"), "removing a row must not renumber the others"
    end

    test "each row picks its container and bus from the catalogue", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      row = view |> element("#disk-row-0") |> render()
      assert row =~ ~s(value="fast")
      assert row =~ ~s(value="default-pool")
      for bus <- ~w(virtio sata scsi), do: assert(row =~ ~s(value="#{bus}"))
      assert row =~ ~s(value="TB")
    end

    test "a multi-disk VM is created with one vdisk per row, each in its own container", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")
      view |> element("#add-disk") |> render_click()

      html =
        view
        |> form("#vm-form", %{
          "vm" => %{
            "name" => "db-01",
            "disk_rows" => %{
              "0" => %{"size" => "40", "unit" => "GB", "container" => "default-pool", "bus" => "virtio"},
              "1" => %{"size" => "2", "unit" => "TB", "container" => "fast", "bus" => "sata"}
            }
          }
        })
        |> render_submit()

      assert {:error, {:live_redirect, %{to: "/vms/db-01"}}} = html

      assert_received {:storage, :create, "db-01-disk0", %{size_gib: 40, container: "default-pool"}}
      assert_received {:storage, :create, "db-01-disk1", %{size_gib: 2048, container: "fast"}}
    end

    test "no disk row is a field error shown on the disks panel", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")
      view |> element("#disk-row-0 button[aria-label='Remove disk']") |> render_click()

      assert view |> element("#disks-error") |> render() =~ "at least one disk"
    end
  end

  describe "CD-ROMs" do
    test "start empty and one drive is added per click, each choosing an image", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      assert has_element?(view, "#no-cdroms")
      view |> element("#add-cdrom") |> render_click()
      view |> element("#add-cdrom") |> render_click()

      refute has_element?(view, "#no-cdroms")
      assert has_element?(view, "#cdrom-row-0")
      assert has_element?(view, "#cdrom-row-1")
      assert view |> element("#cdrom-row-0") |> render() =~ "rocky-10.iso (1.20 GB)"

      view |> element("#cdrom-row-0 button[aria-label='Remove CD-ROM']") |> render_click()
      refute has_element?(view, "#cdrom-row-0")
      assert has_element?(view, "#cdrom-row-1")
    end
  end

  describe "network interfaces" do
    test "start with one NIC on the first network, and any number can be added or removed", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      assert view |> element("#nic-row-0") |> render() =~ "prod (VLAN 100)"
      view |> element("#add-nic") |> render_click()
      assert has_element?(view, "#nic-row-1")
      assert view |> element("#nic-row-1") |> render() =~ "e1000e"

      view |> element("#nic-row-0 button[aria-label='Remove NIC']") |> render_click()
      view |> element("#nic-row-1 button[aria-label='Remove NIC']") |> render_click()
      assert has_element?(view, "#no-nics")
    end
  end

  describe "compute options" do
    test "the form carries firmware, boot device, CPU model, memory unit and graphics", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      for name <- ~w(firmware boot_device cpu_model memory_unit graphics) do
        assert has_element?(view, "select[name='vm[#{name}]']"), "missing #{name}"
      end

      assert has_element?(view, "input[name='vm[memory]']")
      assert view |> element("select[name='vm[cpu_model]']") |> render() =~ "Denverton"
      assert view |> element("select[name='vm[boot_device]']") |> render() =~ "CD-ROM"
    end

    test "SPICE is not offered unless every host supports it", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      refute view |> element("select[name='vm[graphics]']") |> render() =~ "SPICE"
      assert has_element?(view, "#spice-note")
    end

    test "SPICE is offered when the hosts report it, and is stored on the VM", %{conn: conn} do
      Application.put_env(:spectrum_phx, :vm_options, %{
        containers: ["default-pool"],
        images: [],
        networks: [%{id: "net-1", label: "n"}],
        spice?: true,
        notes: []
      })

      {:ok, view, _html} = live(conn, ~p"/vms/new")

      assert view |> element("select[name='vm[graphics]']") |> render() =~ "SPICE"
      refute has_element?(view, "#spice-note")

      assert {:error, {:live_redirect, %{to: "/vms/console-01"}}} =
               view
               |> form("#vm-form", %{"vm" => %{"name" => "console-01", "graphics" => "spice"}})
               |> render_submit()
    end

    test "memory can be given in GB", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      html =
        view
        |> form("#vm-form", %{"vm" => %{"memory" => "64", "memory_unit" => "MB"}})
        |> render_change()

      assert html =~ "must be at least 128 MiB"

      html =
        view
        |> form("#vm-form", %{"vm" => %{"memory" => "1", "memory_unit" => "GB"}})
        |> render_change()

      refute html =~ "must be at least 128 MiB"
    end

    test "a catalogue that could not be read is said so, not drawn as empty", %{conn: conn} do
      Application.put_env(:spectrum_phx, :vm_options, %{
        containers: ["default-pool"],
        images: [],
        networks: [%{id: "net-1", label: "n"}],
        spice?: false,
        notes: ["The image catalogue could not be read, so no CD-ROM image can be chosen."]
      })

      {:ok, view, _html} = live(conn, ~p"/vms/new")

      assert view |> element("#options-note") |> render() =~ "image catalogue could not be read"
    end
  end

  describe "width" do
    test "the form fills the window instead of a 1280px column", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/vms/new")

      refute html =~ "max-w-7xl"
      assert html =~ "xl:grid-cols-2"
      assert length(Regex.scan(~r/<section[^>]*glass-card/, html)) >= 5
    end
  end
end
