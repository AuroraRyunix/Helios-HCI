defmodule SpectrumPhxWeb.Tasks.IndexLiveTest do
  use SpectrumPhxWeb.ConnCase

  import Phoenix.LiveViewTest

  alias SpectrumPhx.Tasks

  # Mounted through the route, so each test exercises the real path an operator
  # takes: the plug pipeline, the authentication hook, and the layout around the view.

  defp row(attrs) do
    Map.merge(
      %{
        "task_id" => "00000000-0000-0000-0000-000000000001",
        "service" => "vali",
        "action" => "start",
        "status" => "processing",
        "payload" => ~s({"vm_name":"web-01"}),
        "progress" => 40,
        "error_msg" => nil,
        "created_at" => ~U[2026-08-19 10:00:00Z],
        "updated_at" => ~U[2026-08-19 10:00:30Z]
      },
      attrs
    )
  end

  defp fixture do
    [
      row(%{}),
      row(%{
        "task_id" => "00000000-0000-0000-0000-000000000002",
        "service" => "valhalla",
        "action" => "upload_image",
        "status" => "failed",
        "payload" => ~s({"filename":"debian-13.qcow2"}),
        "progress" => 100,
        "error_msg" => "checksum mismatch",
        "created_at" => ~U[2026-08-19 09:00:00Z]
      }),
      row(%{
        "task_id" => "00000000-0000-0000-0000-000000000003",
        "service" => "dagur",
        "action" => "execute",
        "status" => "completed",
        "payload" => ~s({"job_name":"storage_scrub"}),
        "progress" => 100,
        "created_at" => ~U[2026-08-19 08:00:00Z]
      })
    ]
  end

  defp put_source(source) do
    Application.put_env(:spectrum_phx, :tasks_source, source)
  end

  setup %{conn: conn} do
    put_source({:static, fixture()})
    on_exit(fn -> Application.delete_env(:spectrum_phx, :tasks_source) end)
    %{conn: log_in(conn)}
  end

  describe "mount" do
    test "lists every task with its subject, not just its service and action", %{conn: conn} do
      {:ok, _view, html} = live(log_in(conn), "/tasks")

      assert html =~ "VM &#39;web-01&#39; - start"
      assert html =~ "Image &#39;debian-13.qcow2&#39; - upload_image"
      assert html =~ "Job &#39;storage_scrub&#39; - execute"
    end

    test "summarises the queue", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/tasks")

      assert view |> element("#stat-running") |> render() =~ "1"
      assert view |> element("#stat-failed") |> render() =~ "1"
      assert view |> element("#stat-completed") |> render() =~ "1"
    end

    test "shows a failed task's error next to it", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/tasks")

      row = view |> element("#task-00000000-0000-0000-0000-000000000002") |> render()

      assert row =~ "checksum mismatch"
      assert row =~ "failed"
    end
  end

  describe "progress is not success" do
    test "a failed task at 100% is drawn as an error, never as a completed task",
         %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/tasks")

      row = view |> element("#task-00000000-0000-0000-0000-000000000002") |> render()

      # Full bar, error colour. The Python tier writes progress = 100 on every failure.
      assert row =~ "progress-error"
      assert row =~ ~s(value="100")
      refute row =~ "progress-success"
      refute row =~ "badge-success"
    end

    test "an unrecognised status is drawn as a warning, not as a pass", %{conn: conn} do
      put_source({:static, [row(%{"status" => "aborted"})]})

      {:ok, view, _html} = live(log_in(conn), "/tasks")

      row = view |> element("#task-00000000-0000-0000-0000-000000000001") |> render()

      assert row =~ "badge-warning"
      refute row =~ "badge-success"
      assert view |> element("#stat-unknown") |> render() =~ "1"
    end

    test "progress is a progress element, so a running bar is patched rather than rebuilt",
         %{conn: conn} do
      {:ok, _view, html} = live(log_in(conn), "/tasks")

      # The old page rewrote this subtree with innerHTML on every poll, which is what made
      # running bars snap back to zero.
      assert html =~ "<progress"
      assert html =~ ~s(value="40")
    end
  end

  describe "empty and unavailable" do
    test "an unreadable database says so, and does not read as an idle cluster",
         %{conn: conn} do
      put_source({:error, :econnrefused})

      {:ok, view, html} = live(log_in(conn), "/tasks")

      assert html =~ "Task history unavailable"
      assert html =~ "econnrefused"
      refute has_element?(view, "#stat-running")
      refute has_element?(view, "#tasks-empty")
    end

    test "a readable but empty table says nothing has run", %{conn: conn} do
      put_source({:static, []})

      {:ok, view, html} = live(log_in(conn), "/tasks")

      assert html =~ "No tasks have been recorded"
      assert has_element?(view, "#stat-running")
      refute has_element?(view, "#tasks-db-error")
    end
  end

  describe "search" do
    test "filters on payload, service, action, status and error", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/tasks")

      html =
        view
        |> element("form[phx-change='search']")
        |> render_change(%{"query" => "checksum"})

      assert html =~ "debian-13.qcow2"
      refute html =~ "web-01"
    end

    test "says so when nothing matches instead of rendering an empty table", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/tasks")

      html =
        view
        |> element("form[phx-change='search']")
        |> render_change(%{"query" => "nothing-like-this"})

      assert html =~ "No tasks match"
      refute has_element?(view, "#tasks-table")
    end
  end

  describe "live updates" do
    test "a broadcast snapshot re-renders without a page refresh", %{conn: conn} do
      {:ok, view, html} = live(log_in(conn), "/tasks")
      assert html =~ "web-01"

      Tasks.broadcast(
        Tasks.fetch(
          rows: [
            row(%{
              "task_id" => "00000000-0000-0000-0000-0000000000ff",
              "payload" => ~s({"vm_name":"db-07"})
            })
          ]
        )
      )

      _ = :sys.get_state(view.pid)
      updated = render(view)

      assert updated =~ "db-07"
      refute updated =~ "web-01"
    end

    test "the interval tick re-reads the current task history", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/tasks")

      put_source({:static, [row(%{"payload" => ~s({"vm_name":"renamed-vm"})})]})
      send(view.pid, :refresh)
      _ = :sys.get_state(view.pid)

      assert render(view) =~ "renamed-vm"
    end

    test "the refresh button re-reads too", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/tasks")

      put_source({:static, [row(%{"payload" => ~s({"vm_name":"clicked-vm"})})]})

      assert view |> element("#refresh-button") |> render_click() =~ "clicked-vm"
    end

    test "an unrelated message is ignored rather than crashing the socket", %{conn: conn} do
      {:ok, view, _html} = live(log_in(conn), "/tasks")

      send(view.pid, :something_else)
      _ = :sys.get_state(view.pid)

      assert render(view) =~ "web-01"
    end
  end
end
