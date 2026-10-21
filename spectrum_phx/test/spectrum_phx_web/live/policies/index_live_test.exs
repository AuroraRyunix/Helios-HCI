defmodule SpectrumPhxWeb.Policies.IndexLiveTest do
  @moduledoc """
  The Policies page: the cluster's real policy objects, and nothing else.

  The panel this replaces was six unrelated settings under a name that promised something
  none of them was. These tests pin what the page is *for*: snapshot policies by scope,
  protection domains, container policies and the security policy -- and that a table that
  could not be read is never drawn as "no policies", which here would read as "nothing is
  being protected".
  """
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  defp put_policies(overrides \\ %{}) do
    static =
      Map.merge(
        %{
          snapshot_policies: [
            %{"scope" => "cluster", "target" => "*", "enabled" => true, "interval_seconds" => 86_400, "keep_last" => 7},
            %{"scope" => "container", "target" => "scratch", "enabled" => false, "interval_seconds" => 3600, "keep_last" => 1},
            %{"scope" => "vdisk", "target" => "db-01-disk0", "enabled" => true, "interval_seconds" => 3600, "keep_last" => 24}
          ],
          domains: [
            %{"name" => "prod", "enabled" => true, "interval_seconds" => 21_600, "keep_last" => 4, "quiesce" => "vm", "max_pause_seconds" => 5}
          ],
          members: [
            %{"domain" => "prod", "kind" => "vm", "name" => "db-01"},
            %{"domain" => "prod", "kind" => "vm", "name" => "web-01"},
            %{"domain" => "prod", "kind" => "vdisk", "name" => "extra-disk0"}
          ],
          sets: [
            %{"domain" => "prod", "taken_at_ms" => 1_767_225_600_000, "state" => "complete", "consistency" => "crash:vm"}
          ],
          containers: [
            %{name: "default-pool", tier: "SSD", quota_bytes: 0, ftt: 1, compression: "none"},
            %{name: "packed", tier: "HDD", quota_bytes: 107_374_182_400, ftt: 0, compression: "lz4"}
          ],
          security: [
            %{"key" => "password_policy", "value" => "enabled"},
            %{"key" => "session_timeout", "value" => "45"},
            %{"key" => "rate_limit", "value" => "250"}
          ]
        },
        overrides
      )

    Application.put_env(:spectrum_phx, :policies_source, {:static, static})
  end

  setup %{conn: conn} do
    put_policies()
    on_exit(fn -> Application.delete_env(:spectrum_phx, :policies_source) end)
    %{conn: log_in(conn)}
  end

  test "is in the navigation and routed", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/policies")

    assert html =~ "Policies"
    assert has_element?(view, "nav a[href='/policies']")
    assert has_element?(view, "nav a[href='/policies'][aria-current='page']")
  end

  test "snapshot policies are listed by scope, with an exemption called one", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/policies")

    cluster = view |> element("#snapshot-policy-cluster-") |> render()
    assert cluster =~ "every vdisk"
    assert cluster =~ "every 1 d"
    assert cluster =~ "last 7"
    assert cluster =~ "enabled"

    # A disabled narrow row exempts what it names from the wider policy.
    scratch = view |> element("#snapshot-policy-container-scratch") |> render()
    assert scratch =~ "exempt"

    vdisk = view |> element("#snapshot-policy-vdisk-db-01-disk0") |> render()
    assert vdisk =~ "every 1 h"
    assert vdisk =~ "last 24"
  end

  test "protection domains show their members, cadence, consistency and latest set", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/policies")

    row = view |> element("#domain-prod") |> render()
    assert row =~ "2 VM(s), 1 vdisk(s)"
    assert row =~ "every 6 h"
    assert row =~ "last 4"
    assert row =~ "vm"
    assert row =~ "pause at most 5s"
    assert row =~ "complete (crash:vm)"
    assert row =~ "2026-01-01"
  end

  test "container policies show tier, quota, fault tolerance and compression", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/policies")

    row = view |> element("#container-policy-packed") |> render()
    assert row =~ "HDD"
    assert row =~ "100.0 GB"
    assert row =~ "lz4"

    assert view |> element("#container-policy-default-pool") |> render() =~ "Unlimited"
    assert has_element?(view, "#container-policies a[href='/storage']")
  end

  test "the security policy is read from the same settings the Settings page writes", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/policies")

    assert view |> element("#policy-password") |> render() =~ "Strong"
    assert view |> element("#policy-session") |> render() =~ "45 minutes"
    assert view |> element("#policy-rate") |> render() =~ "250"
    assert has_element?(view, "#security-policy a[href='/settings']")
  end

  test "says plainly when there is nothing set, and how to set it", %{conn: conn} do
    put_policies(%{snapshot_policies: [], domains: [], members: [], sets: []})
    {:ok, view, _html} = live(conn, ~p"/policies")

    assert view |> element("#no-snapshot-policies") |> render() =~ "nothing is snapshotted automatically"
    assert view |> element("#no-domains") |> render() =~ "No protection domain"
    assert view |> element("#snapshot-policies") |> render() =~ "valcli storage.snapshot-policy.set"
  end

  test "an unreadable table is not drawn as an empty one", %{conn: conn} do
    put_policies(%{snapshot_policies: {:error, "timeout"}})
    {:ok, view, html} = live(conn, ~p"/policies")

    refute has_element?(view, "#no-snapshot-policies")
    assert view |> element("#snapshot-policies") |> render() =~ "could not be read: timeout"
    assert html =~ "That is not an empty result"
    # The other sections still render: one bad table does not blank the page.
    assert has_element?(view, "#domain-prod")
  end

  test "renders interactive forms and actions for snapshot policies, protection domains, and DR targets", %{conn: conn} do
    test_pid = self()

    Application.put_env(:spectrum_phx, :catalyst_submitter, fn service, action, payload ->
      send(test_pid, {:catalyst, service, action, payload})
      {:ok, %{"task_id" => "task-policy-1"}}
    end)

    on_exit(fn -> Application.delete_env(:spectrum_phx, :catalyst_submitter) end)

    put_policies(%{
      sink: test_pid,
      dr_targets: [
        %{
          "domain" => "prod",
          "remote_ip" => "10.20.30.40",
          "remote_port" => 8443,
          "status" => "synced",
          "last_set_id" => "set-01"
        }
      ]
    })

    {:ok, view, _html} = live(conn, ~p"/policies")

    assert has_element?(view, "#snapshot-policy-form")
    assert has_element?(view, "#domain-form")
    assert has_element?(view, "#dr-target-form")
    assert has_element?(view, "#dr-target-prod-10-20-30-40")

    view
    |> form("#snapshot-policy-form", %{
      "scope" => "container",
      "target" => "gold",
      "interval_seconds" => "1800",
      "keep_last" => "12",
      "enabled" => "true"
    })
    |> render_submit()

    assert_receive {:write, cql, ["container", "gold", true, 1800, 12, _]}
    assert cql =~ "INSERT INTO hydra.dfs_snapshot_policies"

    view
    |> element("#run-domain-prod")
    |> render_click()

    assert_receive {:catalyst, "dagur", "execute", %{"command" => "/usr/local/bin/ncli storage.domain.run prod"}}

    view
    |> element("#sync-dr-prod")
    |> render_click()

    assert_receive {:catalyst, "dagur", "execute", %{"command" => "/usr/local/bin/ncli dr.sync prod"}}
  end

  test "is not the six-field grab bag the settings panel was", %{conn: conn} do
    {:ok, _view, html} = live(conn, ~p"/policies")

    # Region, scrub interval and DRS are settings, not policies.
    refute html =~ "Scrub interval"
    refute html =~ "DRS"
    refute html =~ "Region"
  end
end
