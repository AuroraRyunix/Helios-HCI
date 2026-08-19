defmodule SpectrumPhxWeb.Cluster.OverviewLiveTest do
  use SpectrumPhxWeb.ConnCase

  # Every dashboard sits behind authentication; sign the connection in.
  setup %{conn: conn}, do: %{conn: log_in(conn)}

  import Phoenix.LiveViewTest

  alias SpectrumPhx.Cluster.Status

  # A three-node cluster: one healthy leader with a flapping unit, one stale node in
  # maintenance, and one node that is configured but holds no ephemeral znode.
  defp fixture do
    now = System.system_time(:second)

    [
      nodes: %{
        "10.10.0.11" => %{
          "ip" => "10.10.0.11",
          "hostname" => "hci-01",
          "zk_leader" => true,
          "maintenance_status" => "NORMAL",
          "disks" => 6,
          "build" => "2026.08.17-1",
          "ts" => now,
          "services" => %{
            "ZooKeeper" => %{"status" => "UP", "pids" => [1201], "restarts" => 0},
            "HydraDB" => %{"status" => "UP", "pids" => [1302], "restarts" => 0},
            "Vali" => %{"status" => "FLAPPING", "pids" => [], "restarts" => 7},
            "Mimir" => %{"status" => "DOWN", "pids" => [], "restarts" => 2}
          }
        },
        "10.10.0.12" => %{
          "ip" => "10.10.0.12",
          "hostname" => "hci-02",
          "zk_leader" => false,
          "maintenance_status" => "IN_MAINTENANCE",
          "disks" => 6,
          "build" => "2026.08.17-1",
          "ts" => now - 120,
          "services" => %{"ZooKeeper" => %{"status" => "UP", "pids" => [1201], "restarts" => 0}}
        }
      },
      node_ips: ["10.10.0.11", "10.10.0.12", "10.10.0.13"],
      desired: "started",
      source: :zookeeper
    ]
  end

  defp put_override(opts) do
    Application.put_env(:spectrum_phx, :cluster_status_override, opts)
  end

  setup do
    put_override(fixture())
    on_exit(fn -> Application.delete_env(:spectrum_phx, :cluster_status_override) end)
    :ok
  end

  describe "mount" do
    test "renders a card per configured node", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/")

      assert html =~ "hci-01"
      assert html =~ "10.10.0.11"
      assert html =~ "hci-02"
      assert html =~ "10.10.0.13"
    end

    test "renders statically for a disconnected client too", %{conn: conn} do
      html = conn |> get(~p"/") |> html_response(200)

      assert html =~ "hci-01"
      assert html =~ "Cluster"
    end

    test "reports the data source and the desired state", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/")

      assert view |> element("#data-source") |> render() =~ "ZooKeeper"
      assert view |> element("#desired-state") |> render() =~ "started"
    end

    test "summarises nodes and services", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/")

      assert view |> element("#stat-nodes") |> render() =~ "2/3"
      assert view |> element("#stat-services-up") |> render() =~ "3"
      assert view |> element("#stat-services-down") |> render() =~ "1"
      assert view |> element("#stat-services-flapping") |> render() =~ "1"
    end

    test "links each node through to its host detail", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/")
      assert html =~ "/hosts?node=10.10.0.11"
    end
  end

  describe "service status rendering" do
    test "FLAPPING is styled apart from both UP and DOWN", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/")

      assert html =~ "FLAPPING"
      # The three states must not share a colour; that collapse is the bug this view
      # exists to prevent -- the old UI drew a crash-looping unit as healthy.
      assert html =~ "badge-warning"
      assert html =~ "badge-success"
      assert html =~ "badge-error"
    end

    test "a flapping node's card carries its restart count", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/")
      card = view |> element("#node-10-10-0-11") |> render()

      assert card =~ "FLAPPING"
      assert card =~ "7"
      assert card =~ "1 flapping"
    end
  end

  describe "node liveness" do
    test "a configured node with no registration is shown as down", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/")
      card = view |> element("#node-10-10-0-13") |> render()

      assert card =~ "DOWN"
      assert card =~ "ephemeral znode"
    end

    test "a node whose document is older than the threshold is marked stale", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/")
      card = view |> element("#node-10-10-0-12") |> render()

      assert card =~ "stale"
      assert card =~ "IN_MAINTENANCE"
    end

    test "the ZooKeeper leader is marked", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/")

      assert view |> element("#node-10-10-0-11") |> render() =~ "leader"
      refute view |> element("#node-10-10-0-12") |> render() =~ "leader"
    end
  end

  describe "live updates" do
    test "a broadcast snapshot re-renders without a page refresh", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/")
      assert html =~ "hci-01"
      refute html =~ "hci-77"

      Status.broadcast(
        Status.fetch(
          nodes: %{"10.10.0.77" => %{"hostname" => "hci-77", "services" => %{}}},
          desired: "stopped"
        )
      )

      _ = :sys.get_state(view.pid)
      updated = render(view)

      assert updated =~ "hci-77"
      refute updated =~ "hci-01"
      assert updated =~ "stopped"
    end

    test "the interval tick re-reads the current cluster state", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/")

      put_override(nodes: %{"10.10.0.11" => %{"hostname" => "renamed-01", "services" => %{}}})
      send(view.pid, :refresh)
      _ = :sys.get_state(view.pid)

      assert render(view) =~ "renamed-01"
    end

    test "the refresh button re-reads too", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/")

      put_override(nodes: %{"10.10.0.11" => %{"hostname" => "clicked-01", "services" => %{}}})

      assert view |> element("#refresh-button") |> render_click() =~ "clicked-01"
    end

    test "unrelated messages are ignored rather than crashing the socket", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/")

      send(view.pid, :something_else)
      _ = :sys.get_state(view.pid)

      assert render(view) =~ "hci-01"
    end
  end

  describe "empty cluster" do
    test "says so plainly instead of rendering a healthy cluster of zero nodes", %{conn: conn} do
      put_override(nodes: %{}, node_ips: [], error: "no cluster.json")

      {:ok, view, html} = live(conn, ~p"/")

      assert html =~ "No cluster configured"
      assert html =~ "no cluster.json"
      refute has_element?(view, "#stat-nodes")
    end
  end

  describe "probe fallback" do
    test "warns that liveness is a sample when ZooKeeper is unavailable", %{conn: conn} do
      put_override(
        nodes: %{"10.10.0.11" => %{"hostname" => "hci-01", "services" => %{}}},
        source: :probe,
        error: "econnrefused"
      )

      {:ok, view, html} = live(conn, ~p"/")

      assert html =~ "probe fallback"
      assert view |> element("#probe-notice") |> render() =~ "ZooKeeper is unreachable"
    end
  end
end
