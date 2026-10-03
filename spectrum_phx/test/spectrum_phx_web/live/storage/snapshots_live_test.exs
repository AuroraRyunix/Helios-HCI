defmodule SpectrumPhxWeb.Storage.SnapshotsLiveTest do
  # Not async: the snapshot and storage sources are configured through application env,
  # which is global.
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  defp mount_view(conn, id \\ "vm-disk0"),
    do: live(log_in(conn), "/storage/vdisks/#{id}/snapshots")

  defp row(id, parent, at, class \\ "immutable") do
    %{
      "vdisk_id" => id,
      "class" => class,
      "container" => "pool",
      "parent_vdisk" => parent,
      "size_bytes" => 1_073_741_824,
      "created_at_ms" => at
    }
  end

  defp data do
    %{
      vdisks: [
        row("vm-disk0", nil, 1_700_000_000_000, "rw"),
        row("vm-disk0-auto-202608010100", "vm-disk0", 1_785_546_000_000),
        row("vm-disk0-by-hand", "vm-disk0", 1_785_000_000_000),
        row("restored", "vm-disk0-by-hand", 1_785_100_000_000, "rw")
      ],
      index: [
        %{
          "vdisk_id" => "vm-disk0",
          "created_at_ms" => 1_785_546_000_000,
          "snapshot_id" => "vm-disk0-auto-202608010100",
          "origin" => "policy"
        },
        %{
          "vdisk_id" => "vm-disk0",
          "created_at_ms" => 1_785_000_000_000,
          "snapshot_id" => "vm-disk0-by-hand",
          "origin" => "manual"
        }
      ],
      policies: [
        %{
          "scope" => "cluster",
          "target" => "*",
          "enabled" => true,
          "interval_seconds" => 86_400,
          "keep_last" => 7
        }
      ]
    }
  end

  setup do
    Application.put_env(:spectrum_phx, :snapshots_source, {:static, data()})
    on_exit(fn -> Application.delete_env(:spectrum_phx, :snapshots_source) end)
    :ok
  end

  test "lists the snapshots with who took them", %{conn: conn} do
    {:ok, view, html} = mount_view(conn)

    assert html =~ "vm-disk0-auto-202608010100"
    assert has_element?(view, "#snapshot-vm-disk0-auto-202608010100", "policy")
    assert has_element?(view, "#snapshot-vm-disk0-by-hand", "an operator")
  end

  test "says why a snapshot will outlive keep when something was derived from it", %{conn: conn} do
    {:ok, view, _html} = mount_view(conn)

    assert has_element?(view, "#snapshot-vm-disk0-by-hand", "a vdisk was derived from it")

    refute has_element?(
             view,
             "#snapshot-vm-disk0-auto-202608010100",
             "a vdisk was derived from it"
           )
  end

  test "states the policy that governs the disk", %{conn: conn} do
    {:ok, view, _html} = mount_view(conn)
    assert has_element?(view, "#policy-active", "keeping the newest 7")
  end

  test "says so when no policy covers the disk", %{conn: conn} do
    Application.put_env(:spectrum_phx, :snapshots_source, {:static, %{data() | policies: []}})
    {:ok, view, _html} = mount_view(conn)
    assert has_element?(view, "#policy-none")
  end

  test "says so when the disk is exempt from a cluster-wide policy", %{conn: conn} do
    exempt = %{
      "scope" => "vdisk",
      "target" => "vm-disk0",
      "enabled" => false,
      "interval_seconds" => 86_400,
      "keep_last" => 1
    }

    Application.put_env(
      :spectrum_phx,
      :snapshots_source,
      {:static, %{data() | policies: data().policies ++ [exempt]}}
    )

    {:ok, view, _html} = mount_view(conn)
    assert has_element?(view, "#policy-exempt")
  end

  test "offers no way to change anything", %{conn: conn} do
    {:ok, view, _html} = mount_view(conn)
    refute has_element?(view, "button", "Delete")
    refute has_element?(view, "button", "Rollback")
    refute has_element?(view, "form")
  end

  test "a vdisk with no snapshots says Hydra answered and there are none", %{conn: conn} do
    Application.put_env(
      :spectrum_phx,
      :snapshots_source,
      {:static, %{data() | vdisks: [row("vm-disk0", nil, 1, "rw")], index: []}}
    )

    {:ok, view, _html} = mount_view(conn)
    assert has_element?(view, "#snapshots-empty", "Hydra answered")
  end

  test "a vdisk that does not exist is not rendered as having no snapshots", %{conn: conn} do
    {:ok, view, _html} = mount_view(conn, "ghost-disk0")
    assert has_element?(view, "#snapshots-not-found")
    refute has_element?(view, "#snapshots-empty")
  end
end
