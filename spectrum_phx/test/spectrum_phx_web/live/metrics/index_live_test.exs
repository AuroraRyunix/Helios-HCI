defmodule SpectrumPhxWeb.Metrics.IndexLiveTest do
  use SpectrumPhxWeb.ConnCase

  import Phoenix.LiveViewTest

  alias SpectrumPhx.Metrics

  # Mounted through the route, so each test exercises the real path an operator
  # takes: the plug pipeline, the authentication hook, and the layout around the view.

  defp sample(ip, offset_seconds, attrs \\ %{}) do
    Map.merge(
      %{
        "node_ip" => ip,
        "timestamp" => DateTime.add(DateTime.utc_now(), offset_seconds, :second),
        "cpu_pct" => 12.5,
        "mem_pct" => 48.0,
        "mem_total_kb" => 16_777_216,
        "cpu_cores" => 8,
        "disk_iops" => 140.0,
        "disk_bandwidth_kbps" => 2_048.0,
        "net_rx_kbps" => 512.0,
        "net_tx_kbps" => 256.0
      },
      attrs
    )
  end

  defp fixture do
    [
      sample("10.10.0.11", -60, %{"cpu_pct" => 10.0}),
      sample("10.10.0.11", -30, %{"cpu_pct" => 30.0}),
      sample("10.10.0.11", 0, %{"cpu_pct" => 55.0, "mem_pct" => 91.0}),
      sample("10.10.0.12", -30, %{"cpu_pct" => 20.0}),
      sample("10.10.0.12", 0, %{"cpu_pct" => 22.0})
    ]
  end

  # Three configured nodes; one of them never writes telemetry.
  defp cluster_override do
    now = System.system_time(:second)

    [
      nodes: %{
        "10.10.0.11" => %{"hostname" => "hci-01", "services" => %{}, "ts" => now, "disks" => 6},
        "10.10.0.12" => %{"hostname" => "hci-02", "services" => %{}, "ts" => now, "disks" => 6},
        "10.10.0.13" => %{"hostname" => "hci-03", "services" => %{}, "ts" => now, "disks" => 6}
      },
      node_ips: ~w(10.10.0.11 10.10.0.12 10.10.0.13),
      source: :zookeeper
    ]
  end

  defp put_source(source) do
    Application.put_env(:spectrum_phx, :metrics_source, source)
  end

  setup %{conn: conn} do
    put_source({:static, fixture()})
    Application.put_env(:spectrum_phx, :cluster_status_override, cluster_override())

    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :metrics_source)
      Application.delete_env(:spectrum_phx, :cluster_status_override)
    end)

    %{conn: log_in(conn)}
  end

  describe "mount" do
    test "renders a panel per configured node, named from the cluster", %{conn: conn} do
      {:ok, _view, html} = live(log_in(conn), "/metrics")

      assert html =~ "hci-01"
      assert html =~ "hci-02"
      assert html =~ "hci-03"
      assert html =~ "10.10.0.11"
    end

    test "summarises across reporting nodes only", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/metrics")

      assert view |> element("#stat-reporting") |> render() =~ "2/3"
      assert view |> element("#stat-reporting-desc") |> render() =~ "1 silent"
      # (55.0 + 22.0) / 2 = 38.5 -- the silent node is not counted as 0%.
      assert view |> element("#stat-cpu") |> render() =~ "38.5%"
    end

    test "reports capacity from the samples that carry it", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/metrics")

      assert view |> element("#stat-capacity") |> render() =~ "16"
      assert view |> element("#stat-capacity-desc") |> render() =~ "GiB"
    end

    test "draws a sparkline once there is more than one sample", %{conn: conn} do
      {:ok, _view, html} = live(log_in(conn), "/metrics")

      assert html =~ "<polyline"
      assert html =~ "points="
    end
  end

  describe "silence is not idleness" do
    test "a node with no telemetry says so instead of showing zero load", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/metrics")

      panel = view |> element("#metrics-node-10-10-0-13") |> render()

      assert panel =~ "no telemetry"
      assert panel =~ "unknown, not zero"
      refute panel =~ "0.0%"
    end

    test "a node whose newest sample is old is marked stale", %{conn: conn} do
      put_source({:static, [sample("10.10.0.11", -600), sample("10.10.0.11", -601)]})

      {:ok, view, _html} = live(log_in(conn), "/metrics")

      assert view |> element("#metrics-node-10-10-0-11") |> render() =~ "stale"
    end
  end

  describe "empty and unavailable" do
    test "an unreadable database says so and shows no figures", %{conn: conn} do
      put_source({:error, :econnrefused})

      {:ok, view, html} = live(log_in(conn), "/metrics")

      assert html =~ "Telemetry unavailable"
      assert html =~ "econnrefused"
      refute has_element?(view, "#stat-cpu")
      # The nodes are still listed -- they exist, their load is simply unknown.
      assert has_element?(view, "#metrics-node-10-10-0-11")
      assert view |> element("#metrics-node-10-10-0-11") |> render() =~ "no telemetry"
    end

    test "no cluster at all is said plainly", %{conn: conn} do
      put_source({:static, []})
      Application.put_env(:spectrum_phx, :cluster_status_override, nodes: %{}, node_ips: [])

      {:ok, view, html} = live(log_in(conn), "/metrics")

      assert html =~ "No cluster configured"
      refute has_element?(view, "#stat-reporting")
    end
  end

  describe "honesty about what the table holds" do
    test "the disk panel is labelled as I/O and says usage is not recorded", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/metrics")

      assert view |> element("#metrics-disk-note") |> render() =~ "no disk-usage column"
      assert view |> element("#metrics-node-10-10-0-11-io") |> render() =~ "IOPS"
    end
  end

  describe "live updates" do
    test "a broadcast snapshot re-renders without a page refresh", %{conn: conn} do
      {:ok, view, html} = live(log_in(conn), "/metrics")
      assert html =~ "hci-01"

      Metrics.broadcast(
        Metrics.fetch(
          rows: [sample("10.10.0.99", 0)],
          node_ips: ["10.10.0.99"],
          cluster: nil
        )
      )

      _ = :sys.get_state(view.pid)
      updated = render(view)

      assert updated =~ "10.10.0.99"
      refute updated =~ "hci-01"
    end

    test "the interval tick re-reads the current telemetry", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/metrics")

      put_source({:static, [sample("10.10.0.11", 0, %{"cpu_pct" => 99.0})]})
      send(view.pid, :refresh)
      _ = :sys.get_state(view.pid)

      assert render(view) =~ "99.0%"
    end

    test "the refresh button re-reads too", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/metrics")

      put_source({:static, [sample("10.10.0.11", 0, %{"cpu_pct" => 77.0})]})

      assert view |> element("#refresh-button") |> render_click() =~ "77.0%"
    end

    test "an unrelated message is ignored rather than crashing the socket", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/metrics")

      send(view.pid, :something_else)
      _ = :sys.get_state(view.pid)

      assert render(view) =~ "hci-01"
    end
  end
end
