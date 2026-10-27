defmodule SpectrumPhxWeb.CentralLiveTest do
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  setup %{conn: conn}, do: %{conn: log_in(conn)}

  test "mounts and displays Helios Central when not deployed", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/central")

    assert html =~ "Helios Central"
    assert html =~ "Not Deployed"
    assert html =~ "Helios Central is Not Deployed"
    assert html =~ "Option 1: Deploy New Central Appliance"
    assert html =~ "Option 2: Connect to Existing Central"

    # Action buttons exist
    assert has_element?(view, "#deploy-appliance-btn")
    assert has_element?(view, "#enroll-cluster-btn")
    assert has_element?(view, "#central-refresh-btn")

    # Fleet metrics deck should NOT be displayed when not deployed
    refute html =~ "Managed Clusters"
  end

  test "opens and closes deployment modal and sizing presets", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/central")

    # Open deploy modal
    html = render_click(view, "open_deploy_modal")
    assert html =~ "Deploy Helios Central Appliance"
    assert has_element?(view, "#deploy-modal")
    assert has_element?(view, "select[name='container']")
    refute html =~ "rocket"

    # Select standard preset
    html = render_click(view, "select_preset", %{"preset" => "standard"})
    assert html =~ "Standard"

    # Select tiny preset
    html = render_click(view, "select_preset", %{"preset" => "tiny"})
    assert html =~ "Tiny"

    # Close modal
    _html = render_click(view, "close_deploy_modal")
    refute has_element?(view, "#deploy-modal")
  end

  test "opens and closes connect cluster enrollment modal", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/central")

    # Open enroll modal
    html = render_click(view, "open_enroll_modal")
    assert html =~ "Connect External Helios Cluster"
    assert has_element?(view, "#enroll-modal")

    # Close modal
    _html = render_click(view, "close_enroll_modal")
    refute has_element?(view, "#enroll-modal")
  end

  test "handles appliance deployment trigger and validation", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/central")

    # Trigger deploy with missing required parameters (empty ip/gateway)
    html = render_click(view, "trigger_deploy")
    assert html =~ "Failed to deploy Central Appliance"

    # Open modal, verify form elements
    _html = render_click(view, "open_deploy_modal")
    assert has_element?(view, "#deploy-appliance-form")

    # Change form fields
    html = render_change(view, "update_deploy_form", %{
      "name" => "central-test-01",
      "ip" => "10.10.102.75/24",
      "gateway" => "10.10.102.1",
      "admin_password" => "TestPass123!"
    })
    assert html =~ "10.10.102.75/24"
  end

  test "renders multi-step deployment in-progress card when Catalyst task is running", %{conn: conn} do
    parent_id = "11111111-1111-1111-1111-111111111111"
    child1_id = "22222222-2222-2222-2222-222222222222"
    child2_id = "33333333-3333-3333-3333-333333333333"

    now = System.system_time(:millisecond)

    mock_tasks = [
      %{
        "task_id" => parent_id,
        "parent_task_id" => nil,
        "component" => "central",
        "service" => "helios-central",
        "action" => "deploy",
        "task_type" => "deploy",
        "status" => "processing",
        "progress" => 35,
        "payload" => Jason.encode!(%{"appliance_name" => "helios-central"}),
        "created_at" => now,
        "updated_at" => now
      },
      %{
        "task_id" => child1_id,
        "parent_task_id" => parent_id,
        "component" => "central",
        "service" => "helios-central",
        "action" => "allocate_storage",
        "task_type" => "allocate_storage",
        "status" => "completed",
        "progress" => 100,
        "payload" => Jason.encode!(%{"appliance_name" => "helios-central", "parent_task_id" => parent_id}),
        "created_at" => now + 1,
        "updated_at" => now + 1
      },
      %{
        "task_id" => child2_id,
        "parent_task_id" => parent_id,
        "component" => "central",
        "service" => "helios-central",
        "action" => "stage_image",
        "task_type" => "stage_image",
        "status" => "processing",
        "progress" => 40,
        "payload" => Jason.encode!(%{"appliance_name" => "helios-central", "parent_task_id" => parent_id}),
        "created_at" => now + 2,
        "updated_at" => now + 2
      }
    ]

    Application.put_env(:spectrum_phx, :tasks_source, {:static, mock_tasks})
    on_exit(fn -> Application.delete_env(:spectrum_phx, :tasks_source) end)

    {:ok, view, html} = live(conn, ~p"/central")

    # In-progress provisioning card should be shown
    assert html =~ "Deploying Helios Central Virtual Appliance"
    assert html =~ "Workflow Active"
    assert html =~ "35%"
    assert has_element?(view, "#central-deploying-view")
    assert has_element?(view, "#view-catalyst-tasks-btn")

    # The 6 steps are visible
    assert html =~ "Storage Allocation"
    assert html =~ "Golden Image Staging"
    assert html =~ "Cloud-Init Synthesis"
    assert html =~ "Domain Registration"
    assert html =~ "Hypervisor Power-On"
    assert html =~ "Fleet Auto-Pairing"

    # Step badges
    assert html =~ "Done"
    assert html =~ "Running"
    assert html =~ "Pending"
  end
end
