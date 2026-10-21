defmodule SpectrumPhxWeb.Cluster.HostsLiveTest do
  use SpectrumPhxWeb.ConnCase

  # Every dashboard sits behind authentication; sign the connection in.
  setup %{conn: conn}, do: %{conn: log_in(conn)}

  import Phoenix.LiveViewTest

  alias SpectrumPhx.Cluster.Status

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
            "Vali" => %{"status" => "FLAPPING", "pids" => [], "restarts" => 7},
            "Mimir" => %{"status" => "DOWN", "pids" => [], "restarts" => 2}
          }
        },
        "10.10.0.12" => %{
          "ip" => "10.10.0.12",
          "hostname" => "hci-02",
          "zk_leader" => false,
          "maintenance_status" => "IN_MAINTENANCE",
          "disks" => 4,
          "build" => "2026.08.16-9",
          "ts" => now - 300,
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

  describe "host detail" do
    test "shows identity, leadership, maintenance, disks, build and staleness", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/hosts")
      host = view |> element("#host-10-10-0-11") |> render()

      assert host =~ "hci-01"
      assert host =~ "10.10.0.11"
      assert host =~ "leader"
      assert host =~ "NORMAL"
      assert host =~ "6"
      assert host =~ "2026.08.17-1"
      assert host =~ "ago"
    end

    test "marks a stale publication and a host in maintenance", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/hosts")
      host = view |> element("#host-10-10-0-12") |> render()

      assert host =~ "stale"
      assert host =~ "IN_MAINTENANCE"
      assert host =~ "follower"
      assert host =~ "2026.08.16-9"
    end

    test "renders the full service table for a node", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/hosts")
      table = view |> element("#services-10-10-0-11") |> render()

      assert table =~ "ZooKeeper"
      assert table =~ "Vali"
      assert table =~ "Mimir"
      assert table =~ "1201"
      assert table =~ "Restarts"
    end

    test "FLAPPING, UP and DOWN rows are visually distinct", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/hosts")

      flapping = view |> element("#services-10-10-0-11-vali") |> render()
      assert flapping =~ "FLAPPING"
      assert flapping =~ "badge-warning"
      refute flapping =~ "badge-success"
      refute flapping =~ "badge-error"
      assert flapping =~ "7"

      up = view |> element("#services-10-10-0-11-zookeeper") |> render()
      assert up =~ "UP"
      assert up =~ "badge-success"
      refute up =~ "badge-warning"

      down = view |> element("#services-10-10-0-11-mimir") |> render()
      assert down =~ "DOWN"
      assert down =~ "badge-error"
      refute down =~ "badge-success"
    end

    test "a node with no registration explains why it has no services", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/hosts")
      host = view |> element("#host-10-10-0-13") |> render()

      assert host =~ "DOWN"
      assert host =~ "No services reported"
      assert view |> element("#host-10-10-0-13-down") |> render() =~ "no live session"
    end
  end

  describe "selection" do
    test "highlights the node named in the query string", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/hosts?node=10.10.0.12")

      assert view |> element("#host-10-10-0-12") |> render() =~ "ring-2"
      refute view |> element("#host-10-10-0-11") |> render() =~ "ring-2"
    end
  end

  describe "live updates" do
    test "a broadcast snapshot re-renders without a page refresh", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/hosts")
      assert html =~ "hci-01"

      Status.broadcast(
        Status.fetch(nodes: %{"10.10.0.55" => %{"hostname" => "hci-55", "services" => %{}}})
      )

      _ = :sys.get_state(view.pid)
      updated = render(view)

      assert updated =~ "hci-55"
      refute updated =~ "hci-01"
    end

    test "the interval tick re-reads the current cluster state", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/hosts")

      put_override(nodes: %{"10.10.0.11" => %{"hostname" => "renamed-01", "services" => %{}}})
      send(view.pid, :refresh)
      _ = :sys.get_state(view.pid)

      assert render(view) =~ "renamed-01"
    end
  end

  describe "empty cluster" do
    test "renders an honest empty state", %{conn: conn} do
      put_override(nodes: %{}, node_ips: [])

      {:ok, _view, html} = live(conn, ~p"/hosts")

      assert html =~ "No cluster configured"
      refute html =~ "hci-01"
    end
  end

  describe "cluster expansion and 4th-node observer controls" do
    test "toggles the Add Host drawer and submits add_host", %{conn: conn} do
      test = self()

      static = %{
        hosts: [
          %{"hostname" => "hci-01", "ip" => "10.10.0.11"},
          %{"hostname" => "hci-02", "ip" => "10.10.0.12"},
          %{"hostname" => "hci-03", "ip" => "10.10.0.13"}
        ],
        effects: fn tag ->
          send(test, {:effect, tag})
          :ok
        end
      }

      Application.put_env(:spectrum_phx, :settings_source, {:static, static})
      on_exit(fn -> Application.delete_env(:spectrum_phx, :settings_source) end)

      {:ok, view, _html} = live(conn, ~p"/hosts")
      refute has_element?(view, "#add-host-form")

      view |> element("#open-add-host-btn") |> render_click()
      assert has_element?(view, "#add-host-form")

      view
      |> form("#add-host-form", %{
        "hostname" => "hci-04",
        "ip" => "10.10.0.14",
        "redundancy_factor" => "1"
      })
      |> render_submit()

      assert_received {:effect, {:add_node, "hci-04", "10.10.0.14", false, 1}}
      refute has_element?(view, "#add-host-form")
    end

    test "renders 4th node as ZooKeeper observer with vote hand-off form", %{conn: conn} do
      now = System.system_time(:second)

      put_override(
        nodes: %{
          "10.10.0.11" => %{"ip" => "10.10.0.11", "hostname" => "hci-01", "zk_leader" => true, "ts" => now, "services" => %{}},
          "10.10.0.12" => %{"ip" => "10.10.0.12", "hostname" => "hci-02", "zk_leader" => false, "ts" => now, "services" => %{}},
          "10.10.0.13" => %{"ip" => "10.10.0.13", "hostname" => "hci-03", "zk_leader" => false, "ts" => now, "services" => %{}},
          "10.10.0.14" => %{"ip" => "10.10.0.14", "hostname" => "hci-04", "zk_leader" => false, "ts" => now, "services" => %{}}
        },
        node_ips: ["10.10.0.11", "10.10.0.12", "10.10.0.13", "10.10.0.14"],
        desired: "started",
        source: :zookeeper
      )

      {:ok, view, _html} = live(conn, ~p"/hosts")
      fourth = view |> element("#host-10-10-0-14") |> render()

      assert fourth =~ "follower (observer)"
      assert fourth =~ "Slot D"
      assert has_element?(view, "#zk-promote-form-10-10-0-14")
    end
  end
end
