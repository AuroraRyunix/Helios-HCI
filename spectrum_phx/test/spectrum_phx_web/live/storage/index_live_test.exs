defmodule SpectrumPhxWeb.Storage.IndexLiveTest do
  # Not async: the storage source is configured through application env, which is global.
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  @a "10.10.0.11"
  @b "10.10.0.12"

  # Mounted through the route, so each test exercises the real path an operator
  # takes: the plug pipeline, the authentication hook, and the layout around the view.

  defp mount_view(conn), do: live(log_in(conn), "/storage")

  defp capacity(node, opts \\ []) do
    %{
      "node" => node,
      "path" => "/var/lib/hci/sidon/egroups",
      "total_bytes" => Keyword.get(opts, :total, 107_374_182_400),
      "available_bytes" => Keyword.get(opts, :available, 53_687_091_200),
      "egroup_bytes" => 50_331_648,
      "egroup_count" => 12,
      "journal_bytes" => 4_194_304
    }
  end

  defp owned(id, opts \\ []) do
    %{
      "vdisk_id" => id,
      "socket" => "/var/lib/hci/sidon/nbd/#{id}.sock",
      "role" => "owner",
      "epoch" => Keyword.get(opts, :epoch, 3),
      "size_bytes" => 10_737_418_240,
      "degraded" => Keyword.get(opts, :degraded, false),
      "class" => Keyword.get(opts, :class, "rw"),
      "replicas" => Keyword.get(opts, :replicas, ["hci-01", "hci-02"])
    }
  end

  defp attached(entries), do: {:ok, %{"attached" => entries}}

  defp peers(node, entries), do: {:ok, %{"node" => node, "peers" => entries}}

  defp peer(node, opts \\ []) do
    %{
      "node" => node,
      "reachable" => Keyword.get(opts, :reachable, true),
      "detail" => Keyword.get(opts, :detail, "")
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
      node_ips: [@a, @b],
      redundancy_factor: 1,
      capacity: %{@a => {:ok, capacity("hci-01")}, @b => {:ok, capacity("hci-02")}},
      vdisks: %{@a => attached([owned("vm-web-01-disk0")]), @b => attached([])},
      peers: %{
        @a => peers("hci-01", [peer("hci-02")]),
        @b => peers("hci-02", [peer("hci-01")])
      },
      disks: %{@a => {:ok, lsblk()}, @b => {:ok, lsblk()}}
    }
  end

  defp put_source(payload) do
    Application.put_env(:spectrum_phx, :storage_source, {:static, payload})
  end

  defp container(name, opts \\ []) do
    %{
      "name" => name,
      "tier" => Keyword.get(opts, :tier, "SSD"),
      "quota_bytes" => Keyword.get(opts, :quota, 0),
      "path" => name,
      "ftt" => Keyword.get(opts, :ftt, 0),
      "compression" => Keyword.get(opts, :compression, nil)
    }
  end

  defp put_containers(rows),
    do: Application.put_env(:spectrum_phx, :containers_source, {:static, rows})

  defp put_container_vdisks(rows),
    do: Application.put_env(:spectrum_phx, :containers_vdisk_source, {:static, rows})

  setup do
    put_source(healthy())
    put_containers([container("default-pool"), container("packed", compression: "lz4")])
    put_container_vdisks([])

    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :storage_source)
      Application.delete_env(:spectrum_phx, :containers_source)
      Application.delete_env(:spectrum_phx, :containers_vdisk_source)
    end)

    :ok
  end

  describe "containers" do
    test "lists them with their compression setting", %{conn: conn} do
      {:ok, _view, html} = mount_view(conn)

      assert html =~ "default-pool"
      assert html =~ "packed"
      assert html =~ "lz4"
    end

    test "a null compression column is drawn as 'none', not as blank", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      row = view |> element("#container-default-pool") |> render()
      assert row =~ "none"
    end

    test "the compression toggle offers the opposite of what is set", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#container-default-pool") |> render() =~ "Compress"
      assert view |> element("#container-packed") |> render() =~ "Stop compressing"
    end

    test "toggling says plainly that existing data is not rewritten", %{conn: conn} do
      # The surprising half. An operator who turns compression on and sees usage unchanged
      # should already know why.
      {:ok, view, _html} = mount_view(conn)

      html =
        view
        |> element("#container-default-pool button", "Compress")
        |> render_click()

      assert html =~ "existing data is not rewritten"
    end

    test "creating one refuses a name that could not be bound", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      html =
        view
        |> form("form[phx-submit=create_container]", %{
          "name" => "not a name",
          "tier" => "SSD",
          "quota_gb" => "0",
          "ftt" => "0",
          "compression" => "none"
        })
        |> render_submit()

      assert html =~ "Invalid container name"
    end

    test "creating one refuses a name already in use", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      html =
        view
        |> form("form[phx-submit=create_container]", %{
          "name" => "packed",
          "tier" => "SSD",
          "quota_gb" => "0",
          "ftt" => "0",
          "compression" => "none"
        })
        |> render_submit()

      assert html =~ "already exists"
    end

    test "deleting one that still holds vdisks names them", %{conn: conn} do
      put_container_vdisks([
        %{"vdisk_id" => "vm-a-disk0", "container" => "packed"},
        %{"vdisk_id" => "vm-b-disk0", "container" => "packed"}
      ])

      {:ok, view, _html} = mount_view(conn)

      html =
        view
        |> element("#container-packed button", "Delete")
        |> render_click()

      assert html =~ "still holds 2 vdisk(s)"
      assert html =~ "vm-a-disk0"
    end

    test "an unreadable catalogue is not drawn as an empty one", %{conn: conn} do
      # The same rule the rest of this page follows: unknown is not healthy.
      Application.put_env(:spectrum_phx, :containers_source, :hydra)
      {:ok, _view, html} = mount_view(conn)

      assert html =~ "could not be read" or html =~ "containers-error"
    end
  end

  describe "healthy fabric" do
    test "renders vdisks, extent stores and per-node disks", %{conn: conn} do
      {:ok, _view, html} = mount_view(conn)

      assert html =~ "vm-web-01-disk0"
      assert html =~ "/var/lib/hci/sidon/egroups"
      assert html =~ "nvme0n1"
      assert html =~ "epoch"
      assert html =~ "owner"
    end

    test "reports capacity in real units rather than as a raw byte count", %{conn: conn} do
      {:ok, _view, html} = mount_view(conn)

      # Two 100 GiB stores, two copies kept: 200 GiB raw, 100 GiB usable.
      assert html =~ "200.0 GiB"
      assert html =~ "100.0 GiB"
      assert html =~ "2 copies"
    end

    test "says the fabric is healthy only when it is", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#fabric-state") |> render() =~ "all healthy"
      assert view |> element("#stat-vdisks") |> render() =~ "1/1"
    end

    test "does not raise an attention banner when nothing needs attention", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)
      refute view |> element("#attention") |> has_element?()
      refute view |> element("#peers-down") |> has_element?()
    end
  end

  describe "a degraded vdisk is impossible to miss" do
    setup do
      put_source(%{
        healthy()
        | vdisks: %{
            @a => attached([owned("vm-web-01-disk0", degraded: true)]),
            @b => attached([])
          }
      })

      :ok
    end

    test "it is named in a banner above the tables", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      banner = view |> element("#attention") |> render()
      assert banner =~ "vm-web-01-disk0"
      assert banner =~ "writes are refused"
      assert banner =~ "are not healthy"
    end

    test "the fabric badge says so", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)
      assert view |> element("#fabric-state") |> render() =~ "needs attention"
    end

    test "the vdisk card is marked degraded, not healthy", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      card = view |> element("#vdisk-vm-web-01-disk0") |> render()
      assert card =~ "degraded"
      refute card =~ ">healthy<"
    end
  end

  describe "under-replication" do
    setup do
      put_source(%{
        healthy()
        | vdisks: %{
            @a => attached([owned("vm-web-01-disk0", replicas: ["hci-01"])]),
            @b => attached([])
          }
      })

      :ok
    end

    test "the replica shortfall is stated on the card and in the banner", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#vdisk-vm-web-01-disk0") |> render() =~ "1/2"
      assert view |> element("#attention") |> render() =~ "1 of 2 replicas present"
    end

    test "the count is surfaced in the stats", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)
      assert view |> element("#stat-degraded") |> render() =~ "1"
      assert view |> element("#stat-under-replicated") |> render() =~ "1 under-replicated"
    end
  end

  describe "a replication link that is down" do
    setup do
      put_source(%{
        healthy()
        | peers: %{
            @a =>
              peers("hci-01", [peer("hci-02", reachable: false, detail: "connection refused")]),
            @b => peers("hci-02", [peer("hci-01")])
          }
      })

      :ok
    end

    test "is stated as refused writes, not as reduced redundancy", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      banner = view |> element("#peers-down") |> render()
      assert banner =~ "hci-02"
      assert banner =~ "connection refused"
      # The distinction that matters operationally: the guest is taking EIO now.
      assert banner =~ "refusing writes"
    end

    test "and it degrades the vdisks replicated onto that node", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      card = view |> element("#vdisk-vm-web-01-disk0") |> render()
      assert card =~ "degraded"
      assert card =~ "writes are being refused"
    end
  end

  describe "unavailable sources" do
    test "no node reporting an extent store says so instead of showing none", %{conn: conn} do
      put_source(%{
        healthy()
        | capacity: %{@a => {:error, "sidon not answering"}, @b => {:error, :timeout}}
      })

      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#stores-unavailable") |> has_element?()
      assert view |> element("#stores-unavailable") |> render() =~ "sidon not answering"
    end

    test "no node answering for vdisks is unavailable, not an empty list", %{conn: conn} do
      put_source(%{healthy() | vdisks: %{@a => {:error, :timeout}, @b => {:error, :timeout}}})

      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#vdisks-unavailable") |> has_element?()
      refute view |> element("#vdisks-empty") |> has_element?()
      assert view |> element("#fabric-state") |> render() =~ "needs attention"
    end

    test "an empty-but-answered cluster is stated as such", %{conn: conn} do
      put_source(%{healthy() | vdisks: %{@a => attached([]), @b => attached([])}})

      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#vdisks-empty") |> render() =~ "Every node answered"
      refute view |> element("#vdisks-unavailable") |> has_element?()
    end

    test "one node not answering marks the list partial and names the node", %{conn: conn} do
      put_source(%{
        healthy()
        | vdisks: %{@a => attached([owned("vm-web-01-disk0")]), @b => {:error, :econnrefused}}
      })

      {:ok, view, _html} = mount_view(conn)

      partial = view |> element("#vdisks-partial") |> render()
      assert partial =~ @b
      assert partial =~ "incomplete"
      # And the vdisk itself is unknown rather than healthy.
      assert view |> element("#vdisk-vm-web-01-disk0") |> render() =~ "unknown"
    end

    test "a node with no disk inventory is unreadable, not diskless", %{conn: conn} do
      put_source(%{healthy() | disks: %{@a => {:ok, lsblk()}, @b => {:error, :nxdomain}}})

      {:ok, view, _html} = mount_view(conn)

      card = view |> element("#disks-10-10-0-12") |> render()
      assert card =~ "unreadable"
      assert card =~ "unknown -- not"
      assert card =~ "nxdomain"
    end

    test "a store with no capacity gets no usage bar", %{conn: conn} do
      put_source(%{
        healthy()
        | capacity: %{
            @a => {:ok, capacity("hci-01", total: 0, available: 0)},
            @b => {:ok, capacity("hci-02")}
          }
      })

      {:ok, view, _html} = mount_view(conn)

      card = view |> element("#store-10-10-0-11") |> render()
      assert card =~ "Capacity unknown"
      assert card =~ "not mounted"
      refute card =~ "<progress"
    end
  end

  describe "no cluster" do
    test "a dev machine with no cluster.json says so rather than rendering a clean page",
         %{conn: conn} do
      put_source(%{node_ips: [], capacity: %{}, vdisks: %{}, peers: %{}, disks: %{}})

      {:ok, view, html} = mount_view(conn)

      assert view |> element("#no-cluster") |> has_element?()
      assert html =~ "No cluster configured"
      refute view |> element("#stat-vdisks") |> has_element?()
    end
  end

  describe "live updates" do
    test "a pushed snapshot replaces the page without a reload", %{conn: conn} do
      {:ok, view, html} = mount_view(conn)
      assert html =~ "all healthy"

      # Something outside this LiveView noticed the fabric change.
      put_source(%{
        healthy()
        | vdisks: %{
            @a => attached([owned("vm-web-01-disk0", degraded: true)]),
            @b => attached([])
          }
      })

      SpectrumPhx.Storage.broadcast(SpectrumPhx.Storage.snapshot())

      assert render(view) =~ "needs attention"
      assert view |> element("#attention") |> render() =~ "writes are refused"
    end

    test "the server-side interval re-reads the fabric", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      put_source(%{
        healthy()
        | vdisks: %{@a => attached([owned("vm-db-01-disk0")]), @b => attached([])}
      })

      send(view.pid, :refresh)

      html = render(view)
      assert html =~ "vm-db-01-disk0"
      refute html =~ "vm-web-01-disk0"
    end

    test "the refresh button re-reads without navigating", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      put_source(%{
        healthy()
        | vdisks: %{@a => attached([owned("vm-cache-01-disk0")]), @b => attached([])}
      })

      assert view |> element("#refresh-button") |> render_click() =~ "vm-cache-01-disk0"
    end
  end
end
