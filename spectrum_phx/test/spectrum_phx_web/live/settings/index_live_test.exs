defmodule SpectrumPhxWeb.Settings.IndexLiveTest do
  @moduledoc """
  The overlay switch, which is the most destructive control on the console.

  Mounted through the route, so each test walks the path an operator walks: the plug
  pipeline, the authentication hook, and the layout with its task ring around the page.

  The property under test throughout is that the two directions are not interchangeable.
  Building the overlay adds interfaces to every host; removing it deletes them from every
  host while whatever is routing over them is still routing over them. A single toggle
  renders those identically, and a confirmation that says "are you sure?" tells an operator
  nothing they did not already know.
  """
  # Not async: the settings source and the Catalyst submitter are application env, which
  # is global.
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  defp mount_view(conn), do: live(log_in(conn), "/settings")

  defp put_settings(rows, extra \\ %{}) do
    static =
      Map.merge(
        %{
          settings: rows,
          users: [%{"username" => "helios"}, %{"username" => "second"}],
          lanayru: [],
          cluster: %{name: "hci-01", vip: "10.10.102.45", nodes: 3, redundancy_factor: 1}
        },
        extra
      )

    Application.put_env(:spectrum_phx, :settings_source, {:static, static})
  end

  defp accepting do
    test = self()

    Application.put_env(:spectrum_phx, :catalyst_submitter, fn service, action, payload ->
      send(test, {:submitted, service, action, payload})
      {:ok, %{"task_id" => "task-42"}}
    end)
  end

  defp refusing do
    Application.put_env(:spectrum_phx, :catalyst_submitter, fn _s, _a, _p ->
      {:error, :econnrefused}
    end)
  end

  defp row(key, value), do: %{"key" => key, "value" => value}

  setup do
    put_settings([])
    accepting()

    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :settings_source)
      Application.delete_env(:spectrum_phx, :catalyst_submitter)
    end)

    :ok
  end

  describe "what the panel offers" do
    test "offers to build the overlay when it is off", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      assert has_element?(view, "#urbosa-enable")
      refute has_element?(view, "#urbosa-disable")
    end

    test "offers to tear it down when it is on", %{conn: conn} do
      put_settings([row("urbosa_enabled", "true")])
      {:ok, view, _html} = mount_view(conn)

      assert has_element?(view, "#urbosa-disable")
      refute has_element?(view, "#urbosa-enable")
    end
  end

  describe "confirming" do
    test "the first click submits nothing", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      view |> element("#urbosa-enable") |> render_click()

      assert has_element?(view, "#urbosa-confirm-bootstrap")
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "the teardown's confirmation says what it removes and from how many hosts",
         %{conn: conn} do
      # "Are you sure?" tells an operator nothing. The sentence has to name the thing that
      # goes away and the thing that then loses its network.
      put_settings([row("urbosa_enabled", "true")])
      {:ok, view, _html} = mount_view(conn)

      html = view |> element("#urbosa-disable") |> render_click()

      assert html =~ "removes"
      assert html =~ "3 host(s)"
      assert html =~ "Kubernetes"
      assert has_element?(view, "#urbosa-confirm-teardown")
    end

    test "teardown and bootstrap do not render as the same control", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)
      bootstrap = view |> element("#urbosa-enable") |> render_click()

      put_settings([row("urbosa_enabled", "true")])
      {:ok, teardown_view, _html} = mount_view(conn)
      teardown = teardown_view |> element("#urbosa-disable") |> render_click()

      assert bootstrap =~ "alert-info"
      assert teardown =~ "alert-error"
    end

    test "cancelling puts the panel back", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)
      view |> element("#urbosa-enable") |> render_click()
      view |> element("#urbosa-confirm-bootstrap button", "Cancel") |> render_click()

      refute has_element?(view, "#urbosa-confirm-bootstrap")
      assert has_element?(view, "#urbosa-enable")
    end
  end

  describe "submitting" do
    test "the second click submits the bootstrap and names the task", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      view |> element("#urbosa-enable") |> render_click()
      view |> element("#urbosa-confirm-enable") |> render_click()

      assert_receive {:submitted, "dagur", "execute", payload}
      assert payload["command"] == "python3 /usr/local/bin/urbosa-bootstrap"
      assert render(view) =~ "task-42"
    end

    test "the teardown submits the other command", %{conn: conn} do
      put_settings([row("urbosa_enabled", "true")])
      {:ok, view, _html} = mount_view(conn)

      view |> element("#urbosa-disable") |> render_click()
      view |> element("#urbosa-confirm-disable") |> render_click()

      assert_receive {:submitted, "dagur", "execute", payload}
      assert payload["command"] =~ "--cleanup"
      assert render(view) =~ "Teardown"
    end

    test "a refused teardown says which cluster is holding the overlay", %{conn: conn} do
      put_settings([row("urbosa_enabled", "true")], %{
        lanayru: [%{"name" => "kube-01", "status" => "Active"}]
      })

      {:ok, view, _html} = mount_view(conn)
      view |> element("#urbosa-disable") |> render_click()
      view |> element("#urbosa-confirm-disable") |> render_click()

      assert view |> element("#urbosa-error") |> render() =~ "kube-01"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "a failed submission is on the panel, not only in a flash", %{conn: conn} do
      # A flash is gone at the next refresh, and this page refreshes on a timer. The one
      # control whose failure means the row and the hosts could disagree has to keep
      # saying so.
      refusing()
      {:ok, view, _html} = mount_view(conn)

      view |> element("#urbosa-enable") |> render_click()
      view |> element("#urbosa-confirm-enable") |> render_click()

      assert view |> element("#urbosa-error") |> render() =~ "left as it was"
    end
  end

  describe "editing" do
    # The page used to display the VIP, the cluster name and the replication factor and
    # offer no way to change any of them, which looks like the feature was removed.

    defp capture_effects do
      test = self()
      static = Application.get_env(:spectrum_phx, :settings_source) |> elem(1)

      effects = fn tag ->
        send(test, {:effect, tag})
        :ok
      end

      Application.put_env(
        :spectrum_phx,
        :settings_source,
        {:static,
         static
         |> Map.put(:effects, effects)
         |> Map.put(:hosts, ["10.0.0.1", "10.0.0.2"])
         |> Map.put(:replication, [
           %{"replication" => %{"class" => "NetworkTopologyStrategy", "datacenter1" => "3"}}
         ])}
      )
    end

    defp submit(view, fields) do
      view |> form("#settings-form", fields) |> render_submit()
    end

    test "the VIP, name, subnet and replication factor are inputs, not figures", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      for field <- ~w(vip cluster_name cluster_subnet replication_factor) do
        assert has_element?(view, "#settings-form input[name='#{field}']"), "#{field} is read-only"
      end

      assert view |> element("#setting-vip") |> render() =~ ~s(value="10.10.102.45")
      assert view |> element("#setting-cluster_name") |> render() =~ ~s(value="hci-01")
    end

    test "changing the VIP rewrites cluster.json on every host and restarts bifrost", %{conn: conn} do
      capture_effects()
      {:ok, view, _html} = mount_view(conn)

      html = submit(view, %{"vip" => "10.10.102.99"})

      assert_received {:effect, {:host, "10.0.0.1", command}}
      assert command =~ "cluster.json"
      assert_received {:effect, {:host, "10.0.0.2", _}}
      assert_received {:effect, {:units, "10.0.0.1", "restart", ["bifrost"]}}
      assert html =~ "saved"
    end

    test "an invalid VIP is refused with the reason and nothing is applied", %{conn: conn} do
      capture_effects()
      {:ok, view, _html} = mount_view(conn)

      html = submit(view, %{"vip" => "banana"})

      assert html =~ "VIP: must be an IPv4 address"
      refute_received {:effect, _}
    end

    test "a host that did not take the change is named on the page", %{conn: conn} do
      static = Application.get_env(:spectrum_phx, :settings_source) |> elem(1)

      Application.put_env(
        :spectrum_phx,
        :settings_source,
        {:static,
         static
         |> Map.put(:hosts, ["10.0.0.1", "10.0.0.2"])
         |> Map.put(:effects, fn
           {:host, "10.0.0.2", _} -> {:error, "exit 1: unreachable"}
           _ -> :ok
         end)}
      )

      {:ok, view, _html} = mount_view(conn)
      submit(view, %{"vip" => "10.10.102.99"})

      failures = view |> element("#apply-failures") |> render()
      assert failures =~ "10.0.0.2"
      assert failures =~ "unreachable"
    end

    test "re-saving the page unchanged touches nothing", %{conn: conn} do
      capture_effects()
      {:ok, view, _html} = mount_view(conn)

      html = view |> form("#settings-form") |> render_submit()

      refute_received {:effect, {:host, _, _}}
      assert html =~ "Nothing changed"
    end

    test "there is no panel called Policies on the settings page", %{conn: conn} do
      # Six unrelated fields under a name that promised something none of them was. The
      # real policies are on /policies; the security ones are under Security here.
      {:ok, view, html} = mount_view(conn)

      refute has_element?(view, "#policy-settings")
      refute html =~ ">Policies<"
      assert has_element?(view, "#security-settings")
    end

    test "password policy and scrub interval are selects with the values the backend tests for", %{conn: conn} do
      {:ok, _view, html} = mount_view(conn)

      assert html =~ ~s(<option value="enabled")
      refute html =~ "strict"
      assert html =~ ~s(<option value="monthly")
    end

    test "an operator can be created and a password changed from the page", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      html =
        view
        |> form("#create-user-form", %{"username" => "ops_1", "password" => "hunter22"})
        |> render_submit()

      assert html =~ "ops_1 created"

      html =
        view
        |> form("#set-password-form", %{"username" => "helios", "password" => "newpass1", "confirm" => "different"})
        |> render_submit()

      assert html =~ "do not match"
    end
  end

end
