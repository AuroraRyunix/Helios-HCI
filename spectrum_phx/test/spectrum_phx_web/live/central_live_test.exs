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
  end
end
