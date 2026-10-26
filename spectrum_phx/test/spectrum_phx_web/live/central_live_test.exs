defmodule SpectrumPhxWeb.CentralLiveTest do
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  setup %{conn: conn}, do: %{conn: log_in(conn)}

  test "mounts and displays Helios Central fleet dashboard", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/central")

    assert html =~ "Helios Central"
    assert html =~ "Fleet Federation"
    assert html =~ "Managed Clusters"
    assert html =~ "Fleet vCPU Allocation"
    assert html =~ "Fleet Memory Distribution"
    assert html =~ "Sidon Storage Fabric"

    # Tab navigation buttons exist
    assert has_element?(view, "#tab-fleet")
    assert has_element?(view, "#tab-vms")
    assert has_element?(view, "#tab-library")
    assert has_element?(view, "#tab-setup")

    # Action buttons
    assert has_element?(view, "#enroll-cluster-btn")
    assert has_element?(view, "#deploy-appliance-btn")
  end

  test "can switch tabs to global VM explorer and library", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/central")

    # Switch to global VMs
    html = render_click(view, "switch_tab", %{"tab" => "vms"})
    assert html =~ "Global VM Explorer"
    assert has_element?(view, "#vms-tab-content")

    # Switch to content library
    html = render_click(view, "switch_tab", %{"tab" => "library"})
    assert html =~ "Central Golden Images &amp; Templates"
    assert has_element?(view, "#library-tab-content")

    # Switch to setup
    html = render_click(view, "switch_tab", %{"tab" => "setup"})
    assert html =~ "Helios Central Virtual Appliance Architecture"
    assert has_element?(view, "#setup-tab-content")
  end

  test "opens and closes deployment modal and sizing presets", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/central")

    # Open deploy modal
    html = render_click(view, "open_deploy_modal")
    assert html =~ "1-Click Deploy Helios Central Appliance"
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

  test "simulates 1-click appliance deployment pipeline", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/central")

    # Trigger deploy
    html = render_click(view, "trigger_deploy")
    assert html =~ "Deploying Helios Central Virtual Appliance"
    assert has_element?(view, "#deploy-progress-banner")
  end
end
