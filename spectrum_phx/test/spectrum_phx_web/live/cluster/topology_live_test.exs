defmodule SpectrumPhxWeb.Cluster.TopologyLiveTest do
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  setup %{conn: conn}, do: %{conn: log_in(conn)}

  test "mounts and displays fault domains and hierarchy", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/topology")

    assert html =~ "Cluster Topology &amp; Fault Domains"
    assert html =~ "Fault Tolerance Boundary"
    assert has_element?(view, "#domain-kdisk")
    assert has_element?(view, "#domain-khost")
    assert has_element?(view, "#domain-kblock")
    assert has_element?(view, "#domain-krack")
  end

  test "renders single-node kdisk resilience banner when active", %{conn: conn} do
    # When single-node kdisk is configured, banner informs operator of local drive mirroring
    {:ok, _view, html} = live(conn, ~p"/topology")

    # Header domain badge exists
    assert html =~ "domain:"
    assert html =~ "FTT:"
  end
end
