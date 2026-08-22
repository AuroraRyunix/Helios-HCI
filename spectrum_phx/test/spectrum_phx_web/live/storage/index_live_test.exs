defmodule SpectrumPhxWeb.Storage.IndexLiveTest do
  # Not async: the storage source is configured through application env, which is global.
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  # Mounted through the route, so each test exercises the real path an operator
  # takes: the plug pipeline, the authentication hook, and the layout around the view.

  defp mount_view(conn), do: live(log_in(conn), "/storage")

  defp resource(name, opts \\ []) do
    %{
      "name" => name,
      "role" => Keyword.get(opts, :role, "Secondary"),
      "suspended" => false,
      "devices" => [
        %{
          "volume" => 0,
          "disk-state" => Keyword.get(opts, :disk_state, "UpToDate"),
          "client" => false,
          "quorum" => true,
          "size" => 10_485_760
        }
      ],
      "connections" => Keyword.get(opts, :connections, [connection()])
    }
  end

  defp connection(opts \\ []) do
    %{
      "name" => Keyword.get(opts, :peer, "hci-02"),
      "connection" => Keyword.get(opts, :state, "Connected"),
      "peer-role" => "Secondary",
      "peer_devices" => [
        %{
          "volume" => 0,
          "replication" => Keyword.get(opts, :replication, "Established"),
          "peer-disk-state" => Keyword.get(opts, :peer_disk_state, "UpToDate"),
          "peer-client" => false
        }
      ]
    }
  end

  defp pool(name, node, opts \\ []) do
    %{
      "storage_pool_name" => name,
      "node_name" => node,
      "provider_kind" => Keyword.get(opts, :provider, "LVM_THIN"),
      "free_capacity" => Keyword.get(opts, :free, 52_428_800),
      "total_capacity" => Keyword.get(opts, :total, 104_857_600),
      "props" => %{"StorDriver/StorPoolName" => "vg0/thinpool"},
      "reports" => Keyword.get(opts, :reports, [])
    }
  end

  defp lsblk do
    %{
      "blockdevices" => [
        %{
          "name" => "nvme0n1",
          "path" => "/dev/nvme0n1",
          "size" => 512_110_190_592,
          "type" => "disk",
          "rota" => false,
          "children" => [
            %{
              "name" => "nvme0n1p1",
              "size" => 1_073_741_824,
              "type" => "part",
              "mountpoint" => "/boot",
              "rota" => false
            }
          ]
        }
      ]
    }
  end

  defp healthy do
    %{
      node_ips: ["10.10.0.11", "10.10.0.12"],
      redundancy_factor: 1,
      pools: {:ok, [pool("default-pool", "hci-01"), pool("default-pool", "hci-02")]},
      drbd: %{
        "10.10.0.11" => {:ok, [resource("vm-web-01-disk0")]},
        "10.10.0.12" => {:ok, [resource("vm-web-01-disk0")]}
      },
      disks: %{"10.10.0.11" => {:ok, lsblk()}, "10.10.0.12" => {:ok, lsblk()}}
    }
  end

  defp put_source(payload) do
    Application.put_env(:spectrum_phx, :storage_source, {:static, payload})
  end

  setup do
    put_source(healthy())
    on_exit(fn -> Application.delete_env(:spectrum_phx, :storage_source) end)
    :ok
  end

  describe "healthy fabric" do
    test "renders pools, resources and per-node disks", %{conn: conn} do
      {:ok, _view, html} = mount_view(conn)

      assert html =~ "vm-web-01-disk0"
      assert html =~ "default-pool"
      assert html =~ "hci-01"
      assert html =~ "nvme0n1"
      assert html =~ "UpToDate"
      assert html =~ "Connected"
    end

    test "reports capacity in real units rather than as a raw byte count", %{conn: conn} do
      {:ok, _view, html} = mount_view(conn)

      # Two 100 GiB pools, two copies kept: 200 GiB raw, 100 GiB usable.
      assert html =~ "200.0 GiB"
      assert html =~ "100.0 GiB"
      assert html =~ "2 copies"
    end

    test "says the fabric is healthy only when it is", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#fabric-state") |> render() =~ "all healthy"
      refute view |> element("#stat-resources") |> render() =~ "0/1"
      assert view |> element("#stat-resources") |> render() =~ "1/1"
    end

    test "does not raise an attention banner when nothing needs attention", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)
      refute view |> element("#attention") |> has_element?()
    end
  end

  describe "a degraded resource is impossible to miss" do
    setup do
      put_source(%{
        healthy()
        | drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", disk_state: "Inconsistent")]},
            "10.10.0.12" => {:ok, [resource("vm-web-01-disk0")]}
          }
      })

      :ok
    end

    test "it is named in a banner above the tables", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      banner = view |> element("#attention") |> render()
      assert banner =~ "vm-web-01-disk0"
      assert banner =~ "Inconsistent"
      assert banner =~ "are not healthy"
    end

    test "the fabric badge says so", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)
      assert view |> element("#fabric-state") |> render() =~ "needs attention"
    end

    test "the resource card is marked degraded, not healthy", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      card = view |> element("#resource-vm-web-01-disk0") |> render()
      assert card =~ "degraded"
      refute card =~ "healthy"
    end
  end

  describe "under-replication" do
    setup do
      put_source(%{
        healthy()
        | drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", connections: [])]},
            "10.10.0.12" => {:ok, []}
          }
      })

      :ok
    end

    test "the replica shortfall is stated on the card and in the banner", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#resource-vm-web-01-disk0") |> render() =~ "1/2 replicas"
      assert view |> element("#attention") |> render() =~ "1 of 2 replicas present"
    end

    test "the count is surfaced in the stats", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)
      assert view |> element("#stat-degraded") |> render() =~ "1"
      assert view |> element("#stat-under-replicated") |> render() =~ "1 under-replicated"
    end
  end

  describe "unavailable sources" do
    test "an unreadable LINSTOR controller says so instead of showing no pools",
         %{conn: conn} do
      put_source(%{healthy() | pools: {:error, "controller not reachable"}})

      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#pools-unavailable") |> has_element?()
      assert view |> element("#pools-unavailable") |> render() =~ "controller not reachable"
      refute view |> element("#pools-empty") |> has_element?()
    end

    test "an empty-but-answered LINSTOR is stated as such", %{conn: conn} do
      put_source(%{healthy() | pools: {:ok, []}})

      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#pools-empty") |> render() =~ "answered and reported no"
      refute view |> element("#pools-unavailable") |> has_element?()
    end

    test "no node answering for DRBD is unavailable, not an empty resource list",
         %{conn: conn} do
      put_source(%{
        healthy()
        | drbd: %{"10.10.0.11" => {:error, :timeout}, "10.10.0.12" => {:error, :timeout}}
      })

      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#resources-unavailable") |> has_element?()
      refute view |> element("#resources-empty") |> has_element?()
      assert view |> element("#fabric-state") |> render() =~ "needs attention"
    end

    test "one node not answering marks the list partial and names the node", %{conn: conn} do
      put_source(%{
        healthy()
        | drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0")]},
            "10.10.0.12" => {:error, :econnrefused}
          }
      })

      {:ok, view, _html} = mount_view(conn)

      partial = view |> element("#resources-partial") |> render()
      assert partial =~ "10.10.0.12"
      assert partial =~ "incomplete"
      # And the resource itself is unknown rather than healthy.
      assert view |> element("#resource-vm-web-01-disk0") |> render() =~ "unknown"
    end

    test "a node with no disk inventory is unreadable, not diskless", %{conn: conn} do
      put_source(%{
        healthy()
        | disks: %{"10.10.0.11" => {:ok, lsblk()}, "10.10.0.12" => {:error, :nxdomain}}
      })

      {:ok, view, _html} = mount_view(conn)

      card = view |> element("#disks-10-10-0-12") |> render()
      assert card =~ "unreadable"
      assert card =~ "unknown -- not"
      assert card =~ "nxdomain"
    end

    test "a pool with no capacity gets no usage bar", %{conn: conn} do
      put_source(%{healthy() | pools: {:ok, [pool("default-pool", "hci-01", total: nil)]}})

      {:ok, view, _html} = mount_view(conn)

      card = view |> element("#pool-hci-01-default-pool") |> render()
      assert card =~ "Capacity unknown"
      assert card =~ "unknown"
      refute card =~ "<progress"
    end
  end

  describe "no cluster" do
    test "a dev machine with no cluster.json says so rather than rendering a clean page",
         %{conn: conn} do
      put_source(%{node_ips: [], pools: {:error, :no_cluster_configured}, drbd: %{}, disks: %{}})

      {:ok, view, html} = mount_view(conn)

      assert view |> element("#no-cluster") |> has_element?()
      assert html =~ "No cluster configured"
      refute view |> element("#stat-resources") |> has_element?()
    end
  end

  describe "live updates" do
    test "a pushed snapshot replaces the page without a reload", %{conn: conn} do
      {:ok, view, html} = mount_view(conn)
      assert html =~ "all healthy"

      # Something outside this LiveView noticed the fabric change.
      put_source(%{
        healthy()
        | drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", disk_state: "Inconsistent")]},
            "10.10.0.12" => {:ok, [resource("vm-web-01-disk0")]}
          }
      })

      SpectrumPhx.Storage.broadcast(SpectrumPhx.Storage.snapshot())

      assert render(view) =~ "needs attention"
      assert view |> element("#attention") |> render() =~ "Inconsistent"
    end

    test "the server-side interval re-reads the fabric", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      put_source(%{
        healthy()
        | drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-db-01-disk0")]},
            "10.10.0.12" => {:ok, [resource("vm-db-01-disk0")]}
          }
      })

      send(view.pid, :refresh)

      html = render(view)
      assert html =~ "vm-db-01-disk0"
      refute html =~ "vm-web-01-disk0"
    end

    test "the refresh button re-reads without navigating", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      put_source(%{healthy() | pools: {:ok, [pool("second-pool", "hci-03")]}})

      assert view |> element("#refresh-button") |> render_click() =~ "second-pool"
    end
  end
end
