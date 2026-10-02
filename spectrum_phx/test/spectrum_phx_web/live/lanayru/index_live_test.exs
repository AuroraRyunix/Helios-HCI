defmodule SpectrumPhxWeb.Lanayru.IndexLiveTest do
  @moduledoc """
  Deploying and destroying Kubernetes from the page.

  Two properties are worth the mounting: a pre-flight that came back `:error` must stop the
  deploy rather than advise against it, and destroy must not be reachable by clicking.

  The first is not pedantry. Each `:error` check is a condition already established to make
  the deploy fail -- no overlay segment, an extent store that is not mounted, a ring with
  no member up -- and a deploy started anyway runs for minutes on real hosts before saying
  so.
  """
  # Not async: the Lanayru source and the Catalyst submitter are application env.
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  defp mount_view(conn), do: live(log_in(conn), "/lanayru")

  defp ring_node, do: %{"status" => "U", "state" => "N"}

  defp put_source(overrides \\ %{}) do
    static =
      Map.merge(
        %{
          clusters: [],
          segments: [%{"segment_id" => "s1", "name" => "app-net"}],
          expected_nodes: 3,
          urbosa_enabled: "true",
          ring: {:ok, [ring_node(), ring_node(), ring_node()]},
          capacity:
            {:ok, %{"total_bytes" => 100_000_000_000, "available_bytes" => 60_000_000_000}},
          memory: {:ok, %{"free_mb" => 8192}}
        },
        overrides
      )

    Application.put_env(:spectrum_phx, :lanayru_source, {:static, static})
  end

  defp accepting do
    test = self()

    Application.put_env(:spectrum_phx, :catalyst_submitter, fn service, action, payload ->
      send(test, {:submitted, service, action, payload})
      {:ok, %{"task_id" => "task-77"}}
    end)
  end

  @cluster [
    %{
      "cluster_id" => "c1",
      "name" => "kube-01",
      "control_nodes" => 3,
      "status" => "Running",
      "overlay_segment_id" => "s1"
    }
  ]

  setup do
    put_source()
    accepting()

    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :lanayru_source)
      Application.delete_env(:spectrum_phx, :catalyst_submitter)
    end)

    :ok
  end

  describe "the deploy form" do
    test "is offered when no cluster is on record", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      assert has_element?(view, "#lanayru-deploy")
      assert has_element?(view, "#deploy-submit")
      refute has_element?(view, "#lanayru-destroy")
    end

    test "lists the overlay segments a cluster could go on", %{conn: conn} do
      {:ok, _view, html} = mount_view(conn)
      assert html =~ "app-net"
    end

    test "submits to the Kubernetes queue and names the task", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      view
      |> form("#deploy-form", %{
        "cluster_name" => "kube-01",
        "control_nodes" => "3",
        "overlay_segment_id" => "s1"
      })
      |> render_submit()

      assert_receive {:submitted, "lanayru", "deploy", %{"cluster_name" => "kube-01"}}
      assert render(view) =~ "task-77"
    end

    test "a refusal lands on the form and what was typed survives it", %{conn: conn} do
      # The alternative is an operator retyping a cluster name to find out which of the
      # other two fields was the problem.
      {:ok, view, _html} = mount_view(conn)

      view
      |> form("#deploy-form", %{
        "cluster_name" => "kube 01",
        "control_nodes" => "3",
        "overlay_segment_id" => "s1"
      })
      |> render_submit()

      assert view |> element("#deploy-error") |> render() =~ "cluster name"
      assert render(view) =~ "kube 01"
      refute_receive {:submitted, _service, _action, _payload}
    end
  end

  describe "a blocked pre-flight" do
    test "disables the deploy and lists why", %{conn: conn} do
      # No overlay segment is an `:error` check: Kubernetes on no network is the failure
      # that looks like success until a pod tries to talk to another one.
      put_source(%{segments: []})
      {:ok, view, html} = mount_view(conn)

      assert html =~ "already established that this cannot succeed"
      assert view |> element("#deploy-blocked") |> render() =~ "cannot succeed"
      assert view |> element("#deploy-submit") |> render() =~ "disabled"
    end

    test "a warning does not block, and says so", %{conn: conn} do
      # A nearly-full extent store is a judgement call and it is the operator's.
      put_source(%{
        capacity: {:ok, %{"total_bytes" => 100, "available_bytes" => 10}}
      })

      {:ok, view, html} = mount_view(conn)

      refute has_element?(view, "#deploy-blocked")
      assert html =~ "pre-flight has warnings"
    end
  end

  describe "destroying" do
    test "is offered only when there is a cluster", %{conn: conn} do
      put_source(%{clusters: @cluster})
      {:ok, view, _html} = mount_view(conn)

      assert has_element?(view, "#destroy-start")
      refute has_element?(view, "#lanayru-deploy")
    end

    test "the first click asks for the name rather than destroying", %{conn: conn} do
      put_source(%{clusters: @cluster})
      {:ok, view, _html} = mount_view(conn)

      html = view |> element("#destroy-start") |> render_click()

      assert html =~ "Type"
      assert has_element?(view, "#destroy-confirmation")
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "the wrong name is refused", %{conn: conn} do
      # A second click is a reflex; typing the name is not.
      put_source(%{clusters: @cluster})
      {:ok, view, _html} = mount_view(conn)

      view |> element("#destroy-start") |> render_click()
      view |> form("#destroy-confirm", %{"confirmation" => "yes"}) |> render_submit()

      assert view |> element("#destroy-error") |> render() =~ "not the cluster"
      assert view |> element("#destroy-error") |> render() =~ "kube-01"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "the right name submits the teardown", %{conn: conn} do
      put_source(%{clusters: @cluster})
      {:ok, view, _html} = mount_view(conn)

      view |> element("#destroy-start") |> render_click()
      view |> form("#destroy-confirm", %{"confirmation" => "kube-01"}) |> render_submit()

      assert_receive {:submitted, "lanayru", "destroy", %{"cluster_name" => "kube-01"}}
      assert render(view) =~ "task-77"
    end
  end
end
