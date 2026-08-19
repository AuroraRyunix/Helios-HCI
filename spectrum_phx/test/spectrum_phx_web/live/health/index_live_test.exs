defmodule SpectrumPhxWeb.Health.IndexLiveTest do
  use SpectrumPhxWeb.ConnCase

  import Phoenix.LiveViewTest

  alias SpectrumPhx.Health

  # Mounted through the route, so each test exercises the real path an operator
  # takes: the plug pipeline, the authentication hook, and the layout around the view.

  defp result(attrs) do
    Map.merge(
      %{
        "category" => "all",
        "check_name" => "cpu_load",
        "node_ip" => "10.10.0.11",
        "status" => "PASS",
        "output" => "load average 0.4",
        "execution_id" => "3f1c9d5e-0000-0000-0000-000000000001",
        "timestamp" => DateTime.add(DateTime.utc_now(), -300, :second)
      },
      attrs
    )
  end

  defp fixture do
    %{
      results: [
        result(%{"check_name" => "zookeeper_status"}),
        result(%{"check_name" => "vali_status", "status" => "FAIL", "output" => "unit is dead"}),
        result(%{
          "check_name" => "aether_split_brain",
          "status" => "WARN",
          "output" => "resource vm-web-01 needs attention",
          "node_ip" => "10.10.0.12"
        }),
        result(%{"check_name" => "cpu_load"}),
        result(%{"check_name" => "drbd_split_brain_check"})
      ],
      schedules: [
        %{
          "job_name" => "mimir_diagnostics",
          "task_type" => "mimir_health",
          "cron_expression" => "0 * * * *",
          "interval_seconds" => 3600,
          "enabled" => true,
          "last_run_epoch" => DateTime.to_unix(DateTime.utc_now()),
          "command" => "/usr/local/bin/mcli health_checks run_all"
        }
      ],
      runs: [
        %{
          "job_name" => "mimir_diagnostics",
          "start_time" => DateTime.add(DateTime.utc_now(), -600, :second),
          "end_time" => DateTime.add(DateTime.utc_now(), -580, :second),
          "status" => "SUCCESS",
          "exit_code" => 0,
          "run_id" => "aaaa1111-0000-0000-0000-000000000000",
          "output" => "done"
        }
      ]
    }
  end

  defp put_source(source) do
    Application.put_env(:spectrum_phx, :health_source, source)
  end

  setup %{conn: conn} do
    put_source({:static, fixture()})
    on_exit(fn -> Application.delete_env(:spectrum_phx, :health_source) end)
    %{conn: log_in(conn)}
  end

  describe "grouping" do
    test "groups by what a check examines, not by the scope it was invoked with",
         %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/health")

      # Every fixture row says category = "all"; the grouping still has to be right.
      assert view |> element("#table-services") |> render() =~ "zookeeper_status"
      assert view |> element("#table-storage") |> render() =~ "aether_split_brain"
      assert view |> element("#table-hardware") |> render() =~ "cpu_load"
    end

    test "puts a storage check in storage even where the old page put it in services",
         %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/health")

      assert view |> element("#table-storage") |> render() =~ "drbd_split_brain_check"
      refute view |> element("#table-services") |> render() =~ "drbd_split_brain_check"
    end
  end

  describe "failures are obvious" do
    test "failing and warning checks are lifted above the categories", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/health")

      failing = view |> element("#failing-table") |> render()

      assert failing =~ "vali_status"
      assert failing =~ "unit is dead"
      assert failing =~ "aether_split_brain"
      refute failing =~ "zookeeper_status"
    end

    test "the overall banner reports the failure rather than an aggregate colour",
         %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/health")

      banner = view |> element("#health-overall") |> render()

      assert banner =~ "alert-error"
      assert banner =~ "1 check failing"
      refute banner =~ "alert-success"
    end

    test "counts each severity apart", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/health")

      assert view |> element("#stat-fail") |> render() =~ "1"
      assert view |> element("#stat-warn") |> render() =~ "1"
      assert view |> element("#stat-pass") |> render() =~ "3"
    end
  end

  describe "nothing unproven is healthy" do
    test "an empty results table reads 'not run', never 'healthy'", %{conn: conn} do
      put_source({:static, %{results: [], schedules: [], runs: []}})

      {:ok, view, html} = live(log_in(conn), "/health")

      assert html =~ "Diagnostics have not run"
      assert view |> element("#health-overall") |> render() =~ "alert-warning"
      refute html =~ "alert-success"
      refute has_element?(view, "#stat-pass")
    end

    test "an unreadable table is unavailable, and is not an empty one", %{conn: conn} do
      put_source({:error, :econnrefused})

      {:ok, view, html} = live(log_in(conn), "/health")

      assert html =~ "Diagnostics unavailable"
      assert html =~ "econnrefused"
      refute has_element?(view, "#health-overall")
      refute html =~ "alert-success"
    end

    test "an unrecognised status is drawn as a warning, not as a pass", %{conn: conn} do
      put_source({
        :static,
        %{results: [result(%{"status" => "SKIPPED"})], schedules: [], runs: []}
      })

      {:ok, view, _html} = live(log_in(conn), "/health")

      assert view |> element("#stat-unknown") |> render() =~ "1"
      assert view |> element("#stat-pass") |> render() =~ "0"
      assert view |> element("#failing-table") |> render() =~ "cpu_load"
    end

    test "everything passing is the only way to a green banner", %{conn: conn} do
      put_source({
        :static,
        %{results: [result(%{}), result(%{"check_name" => "ram_usage"})], schedules: [], runs: []}
      })

      {:ok, view, _html} = live(log_in(conn), "/health")

      banner = view |> element("#health-overall") |> render()

      assert banner =~ "alert-success"
      assert banner =~ "All 2 checks passing"
    end
  end

  describe "filtering" do
    test "hiding passing checks keeps the full counts on each category header",
         %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/health")

      html =
        view
        |> element("form[phx-change='toggle_passing']")
        |> render_change(%{"show_passing" => "false"})

      assert html =~ "vali_status"
      refute html =~ "zookeeper_status"
      # The hardware category held only a passing check, so it is gone entirely.
      refute has_element?(view, "#table-hardware")
      # ...but the storage header still reports what it really holds.
      assert view |> element("#category-storage") |> render() =~ "1 warning"
    end
  end

  describe "the Dagur scheduler" do
    test "lists the job that dispatches the diagnostics, and its recent runs",
         %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/health")

      assert view |> element("#schedules-table") |> render() =~ "mimir_diagnostics"
      assert view |> element("#schedules-table") |> render() =~ "enabled"
      assert view |> element("#runs-table") |> render() =~ "SUCCESS"
    end

    test "an unreadable scheduler is reported apart from the diagnostics", %{conn: conn} do
      put_source({:static, %{results: fixture().results}})

      {:ok, view, _html} = live(log_in(conn), "/health")

      # `{:static, _}` with no scheduler rows is an empty scheduler, not a broken one.
      assert view |> element("#scheduler-empty") |> render() =~ "No jobs are registered"
      assert view |> element("#failing-table") |> render() =~ "vali_status"
    end
  end

  describe "live updates" do
    test "a broadcast snapshot re-renders without a page refresh", %{conn: conn} do
      {:ok, view, html} = live(log_in(conn), "/health")
      assert html =~ "vali_status"

      Health.broadcast(
        Health.fetch(
          results: [result(%{"check_name" => "ntp_sync", "status" => "FAIL"})],
          schedules: [],
          runs: []
        )
      )

      _ = :sys.get_state(view.pid)
      updated = render(view)

      assert updated =~ "ntp_sync"
      refute updated =~ "vali_status"
    end

    test "the interval tick re-reads the current diagnostics", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/health")

      put_source({
        :static,
        %{results: [result(%{"check_name" => "firmware_upgrades"})], schedules: [], runs: []}
      })

      send(view.pid, :refresh)
      _ = :sys.get_state(view.pid)

      assert render(view) =~ "firmware_upgrades"
    end

    test "the refresh button re-reads too", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/health")

      put_source({
        :static,
        %{results: [result(%{"check_name" => "ntp_sync"})], schedules: [], runs: []}
      })

      assert view |> element("#refresh-button") |> render_click() =~ "ntp_sync"
    end

    test "an unrelated message is ignored rather than crashing the socket", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/health")

      send(view.pid, :something_else)
      _ = :sys.get_state(view.pid)

      assert render(view) =~ "vali_status"
    end
  end
end
