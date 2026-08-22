defmodule SpectrumPhx.StorageTest do
  # Not async: almost every case drives `snapshot/1` through its `:static` option and
  # touches nothing global, but one deliberately exercises the live sourcing path, which
  # reads the storage source out of application env.
  use ExUnit.Case, async: false

  alias SpectrumPhx.Storage

  @two_nodes ["10.10.0.11", "10.10.0.12"]

  # -- fixtures ------------------------------------------------------------------------

  defp resource(name, opts \\ []) do
    %{
      "name" => name,
      "node-id" => 0,
      "role" => Keyword.get(opts, :role, "Secondary"),
      "suspended" => Keyword.get(opts, :suspended, false),
      "devices" => [
        %{
          "volume" => 0,
          "minor" => 1000,
          "disk-state" => Keyword.get(opts, :disk_state, "UpToDate"),
          "client" => Keyword.get(opts, :client, false),
          "quorum" => Keyword.get(opts, :quorum, true),
          # drbdsetup reports KiB: 10 GiB.
          "size" => 10_485_760
        }
      ],
      "connections" => Keyword.get(opts, :connections, [connection()])
    }
  end

  defp connection(opts \\ []) do
    %{
      "peer-node-id" => 1,
      "name" => Keyword.get(opts, :peer, "hci-02"),
      "connection" => Keyword.get(opts, :state, "Connected"),
      "peer-role" => Keyword.get(opts, :peer_role, "Secondary"),
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
      # LINSTOR reports KiB: 50 GiB free of 100 GiB.
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
          "name" => "sda",
          "path" => "/dev/sda",
          "size" => 512_110_190_592,
          "type" => "disk",
          "mountpoint" => nil,
          "rota" => false,
          "model" => "Samsung SSD 980",
          "children" => [
            %{
              "name" => "sda1",
              "path" => "/dev/sda1",
              "size" => 1_073_741_824,
              "type" => "part",
              "mountpoint" => "/boot",
              "fstype" => "ext4",
              "rota" => false
            }
          ]
        }
      ]
    }
  end

  # Two nodes, both answering, one healthy two-way-replicated resource.
  defp healthy(overrides \\ %{}) do
    Map.merge(
      %{
        node_ips: @two_nodes,
        redundancy_factor: 1,
        pools: {:ok, [pool("default-pool", "hci-01"), pool("default-pool", "hci-02")]},
        drbd: %{
          "10.10.0.11" => {:ok, [resource("vm-web-01-disk0")]},
          "10.10.0.12" => {:ok, [resource("vm-web-01-disk0")]}
        },
        disks: %{"10.10.0.11" => {:ok, lsblk()}, "10.10.0.12" => {:ok, lsblk()}}
      },
      overrides
    )
  end

  defp snapshot(static), do: Storage.snapshot(static: static)

  defp resource_named(snapshot, name) do
    Enum.find(snapshot.resources.entries, &(&1.name == name))
  end

  # -- DRBD health ---------------------------------------------------------------------

  describe "DRBD resource health" do
    test "a fully replicated UpToDate resource is healthy" do
      snapshot = snapshot(healthy())
      resource = resource_named(snapshot, "vm-web-01-disk0")

      assert resource.health == :ok
      assert resource.issues == []
      assert resource.replicas == 2
      assert resource.expected_replicas == 2
      assert resource.under_replicated? == false
      assert resource.size_bytes == 10_485_760 * 1024
      assert snapshot.summary.resources_ok == 1
      refute snapshot.summary.attention?
    end

    test "an Inconsistent local device is degraded, not healthy" do
      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", disk_state: "Inconsistent")]},
            "10.10.0.12" => {:ok, [resource("vm-web-01-disk0")]}
          }
        })

      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.health == :degraded
      assert Enum.any?(resource.issues, &(&1 =~ "Inconsistent"))
    end

    test "a StandAlone connection is degraded" do
      standalone = [connection(state: "StandAlone")]

      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", connections: standalone)]},
            "10.10.0.12" => {:ok, [resource("vm-web-01-disk0")]}
          }
        })

      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.health == :degraded
      assert Enum.any?(resource.issues, &(&1 =~ "StandAlone"))
    end

    test "an Outdated peer disk is degraded even though the local copy is fine" do
      outdated = [connection(peer_disk_state: "Outdated")]

      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", connections: outdated)]},
            "10.10.0.12" => {:ok, [resource("vm-web-01-disk0")]}
          }
        })

      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.health == :degraded
      assert Enum.any?(resource.issues, &(&1 =~ "Outdated"))
    end

    test "a resync in progress is degraded rather than healthy" do
      # Replication "SyncTarget" means this copy is still being filled. Reporting it as
      # replicated is exactly the claim the fabric cannot back up yet.
      syncing = [connection(replication: "SyncTarget")]

      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", connections: syncing)]},
            "10.10.0.12" => {:ok, [resource("vm-web-01-disk0")]}
          }
        })

      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.health == :degraded
      assert Enum.any?(resource.issues, &(&1 =~ "SyncTarget"))
    end

    test "a lost quorum is degraded" do
      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", quorum: false)]},
            "10.10.0.12" => {:ok, [resource("vm-web-01-disk0")]}
          }
        })

      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.health == :degraded
      assert Enum.any?(resource.issues, &(&1 =~ "quorum"))
    end

    test "suspended I/O is degraded" do
      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", suspended: true)]},
            "10.10.0.12" => {:ok, [resource("vm-web-01-disk0")]}
          }
        })

      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.health == :degraded
      assert Enum.any?(resource.issues, &(&1 =~ "suspended"))
    end
  end

  describe "under-replication" do
    test "a resource present on one of two nodes is flagged" do
      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", connections: [])]},
            "10.10.0.12" => {:ok, []}
          }
        })

      snapshot = snapshot(static)
      resource = resource_named(snapshot, "vm-web-01-disk0")

      assert resource.replicas == 1
      assert resource.expected_replicas == 2
      assert resource.under_replicated? == true
      assert resource.health == :degraded
      assert Enum.any?(resource.issues, &(&1 =~ "1 of 2 replicas"))
      assert snapshot.summary.resources_under_replicated == 1
      assert snapshot.summary.attention?
    end

    test "a diskless placement is an access point, not a replica" do
      diskless = resource("vm-web-01-disk0", client: true, disk_state: "Diskless")

      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0")]},
            "10.10.0.12" => {:ok, [diskless]}
          }
        })

      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.replicas == 1
      assert resource.under_replicated? == true
    end

    test "a single-node cluster expects one copy and is not under-replicated" do
      static = %{
        node_ips: ["10.10.0.11"],
        redundancy_factor: 0,
        pools: {:ok, [pool("default-pool", "hci-01")]},
        drbd: %{"10.10.0.11" => {:ok, [resource("vm-web-01-disk0", connections: [])]}},
        disks: %{"10.10.0.11" => {:ok, lsblk()}}
      }

      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.expected_replicas == 1
      assert resource.under_replicated? == false
      assert resource.health == :ok
    end

    test "the redundancy factor cannot demand more copies than there are nodes" do
      # A two-node cluster configured with FTT 2 would otherwise flag every resource on a
      # deployment that is doing exactly what it was built to do.
      static = healthy(%{redundancy_factor: 5})
      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.expected_replicas == 2
      assert resource.under_replicated? == false
    end
  end

  describe "dual primary" do
    test "two Primaries on a VM disk is reported" do
      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", role: "Primary")]},
            "10.10.0.12" => {:ok, [resource("vm-web-01-disk0", role: "Primary")]}
          }
        })

      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.health == :degraded
      assert Enum.any?(resource.issues, &(&1 =~ "dual-primary"))
      assert length(resource.primaries) == 2
    end

    test "two Primaries on an image resource is expected and not flagged" do
      # Image resources carry --allow-two-primaries on purpose: the golden image is
      # attached read-only to guests on several hosts at once.
      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("img-ubuntu-24-04", role: "Primary")]},
            "10.10.0.12" => {:ok, [resource("img-ubuntu-24-04", role: "Primary")]}
          }
        })

      resource = static |> snapshot() |> resource_named("img-ubuntu-24-04")

      assert resource.health == :ok
      assert resource.issues == []
    end
  end

  describe "unreachable nodes" do
    test "a node that did not answer makes an otherwise-clean resource unknown" do
      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0")]},
            "10.10.0.12" => {:error, :econnrefused}
          }
        })

      snapshot = snapshot(static)
      resource = resource_named(snapshot, "vm-web-01-disk0")

      assert snapshot.resources.state == :partial
      assert resource.health == :unknown
      # Not `false`: the unread node might hold the missing replica, and might not.
      assert resource.under_replicated? == nil
      assert snapshot.summary.resources_under_replicated == 0
      assert snapshot.summary.attention?
    end

    test "a peer DRBD names does not become a phantom under-replication warning" do
      # 10.10.0.12 did not answer, but 10.10.0.11's DRBD reports a Connected, UpToDate
      # peer. The copy is evidently there; what is missing is our ability to confirm it.
      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0")]},
            "10.10.0.12" => {:error, :econnrefused}
          }
        })

      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.replicas == 1
      assert resource.evidenced_replicas == 2
      refute Enum.any?(resource.issues, &(&1 =~ "replicas"))
      # Still not healthy: peer-reported state is second-hand.
      assert resource.health == :unknown
    end

    test "a shortfall no peer accounts for is still reported while nodes are unread" do
      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0", connections: [])]},
            "10.10.0.12" => {:error, :econnrefused}
          }
        })

      resource = static |> snapshot() |> resource_named("vm-web-01-disk0")

      assert resource.evidenced_replicas == 1
      assert Enum.any?(resource.issues, &(&1 =~ "only 1 of 2 replicas could be seen"))
      assert resource.health == :degraded
      assert resource.under_replicated? == nil
    end

    test "the unreachable node is named rather than dropped" do
      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:ok, [resource("vm-web-01-disk0")]},
            "10.10.0.12" => {:error, :econnrefused}
          }
        })

      snapshot = snapshot(static)

      assert [%{ip: "10.10.0.12", error: error}] = snapshot.resources.unreachable
      assert error =~ "econnrefused"
    end

    test "no node answering is unavailable, not an empty resource list" do
      static =
        healthy(%{
          drbd: %{
            "10.10.0.11" => {:error, :timeout},
            "10.10.0.12" => {:error, :timeout}
          }
        })

      snapshot = snapshot(static)

      assert snapshot.resources.state == :unavailable
      assert snapshot.resources.entries == []
      assert length(snapshot.resources.unreachable) == 2
      assert snapshot.summary.attention?
    end

    test "every node answering with nothing is an honest empty fabric" do
      static = healthy(%{drbd: %{"10.10.0.11" => {:ok, []}, "10.10.0.12" => {:ok, []}}})
      snapshot = snapshot(static)

      assert snapshot.resources.state == :ok
      assert snapshot.resources.entries == []
      refute snapshot.summary.attention?
    end
  end

  # -- pools ---------------------------------------------------------------------------

  describe "storage pools" do
    test "capacities are read as KiB and converted to bytes" do
      snapshot = snapshot(healthy())
      [first | _rest] = snapshot.pools.entries

      assert first.total_bytes == 104_857_600 * 1024
      assert first.free_bytes == 52_428_800 * 1024
      assert first.used_bytes == 52_428_800 * 1024
      assert_in_delta first.used_percent, 50.0, 0.001
      assert first.state == :ok
      assert first.backing == "vg0/thinpool"
    end

    test "a diskless pool's sentinel capacity is dropped, not averaged in" do
      # LINSTOR reports INT64_MAX for a diskless pool. Counting it would make a full
      # fabric report itself as very nearly empty.
      diskless =
        pool("DfltDisklessStorPool", "hci-01",
          provider: "DISKLESS",
          free: 9_223_372_036_854_775_807,
          total: 9_223_372_036_854_775_807
        )

      static = healthy(%{pools: {:ok, [pool("default-pool", "hci-01"), diskless]}})
      snapshot = snapshot(static)

      assert Enum.any?(snapshot.pools.entries, &(&1.state == :diskless))
      assert snapshot.capacity.raw_total_bytes == 104_857_600 * 1024
    end

    test "a pool LINSTOR reports an error for is an error, not an ok pool" do
      broken =
        pool("default-pool", "hci-02",
          reports: [%{"message" => "Device /dev/sdb is not accessible"}]
        )

      static = healthy(%{pools: {:ok, [broken]}})
      snapshot = snapshot(static)

      assert [%{state: :error, messages: [message]}] = snapshot.pools.entries
      assert message =~ "not accessible"
      assert snapshot.summary.pools_error == 1
      assert snapshot.summary.attention?
    end

    test "a pool with no reported capacity is unknown, not a healthy empty pool" do
      static = healthy(%{pools: {:ok, [pool("default-pool", "hci-01", total: nil, free: nil)]}})
      snapshot = snapshot(static)

      assert [%{state: :unknown, used_percent: nil, total_bytes: nil}] = snapshot.pools.entries
      assert snapshot.summary.attention?
    end

    test "an unreadable LINSTOR controller is unavailable, not zero pools" do
      static = healthy(%{pools: {:error, "connection refused"}})
      snapshot = snapshot(static)

      assert snapshot.pools.state == :unavailable
      assert snapshot.pools.entries == []
      assert snapshot.pools.error == "connection refused"
      refute snapshot.capacity.known?
      assert snapshot.capacity.raw_total_bytes == nil
      assert snapshot.summary.attention?
    end

    test "usable capacity accounts for the copies the cluster keeps" do
      snapshot = snapshot(healthy())

      # Two pools of 100 GiB each, replicated twice.
      assert snapshot.capacity.raw_total_bytes == 2 * 104_857_600 * 1024
      assert snapshot.capacity.usable_total_bytes == 104_857_600 * 1024
      assert snapshot.capacity.usable_used_bytes == 52_428_800 * 1024
    end

    test "LINSTOR's outer list wrapper is unwrapped" do
      # `linstor --machine-readable` wraps rows in an extra list on some versions.
      static = healthy(%{pools: {:ok, [[pool("default-pool", "hci-01")]]}})
      snapshot = snapshot(static)

      assert [%{name: "default-pool"}] = snapshot.pools.entries
    end
  end

  describe "pools_command/1" do
    test "renders only values that parse as addresses" do
      command = Storage.pools_command(["10.10.0.11", "10.10.0.12"])

      assert command =~ "LS_CONTROLLERS=10.10.0.11,10.10.0.12"
      assert command =~ "--machine-readable storage-pool list"
    end

    test "drops anything that is not an address rather than passing it to a shell" do
      command = Storage.pools_command(["10.10.0.11", "; rm -rf /", "$(id)"])

      assert command =~ "LS_CONTROLLERS=10.10.0.11 "
      refute command =~ "rm -rf"
      refute command =~ "$("
    end

    test "falls back to the loopback rather than emitting an empty variable" do
      assert Storage.pools_command([]) =~ "LS_CONTROLLERS=127.0.0.1"
    end
  end

  # -- disks ---------------------------------------------------------------------------

  describe "disk inventory" do
    test "the lsblk tree is flattened with its nesting preserved" do
      snapshot = snapshot(healthy())
      [node | _rest] = snapshot.disks

      assert node.state == :ok
      assert [disk, partition] = node.devices
      assert disk.name == "sda"
      assert disk.depth == 0
      assert disk.size_bytes == 512_110_190_592
      assert disk.rotational? == false
      assert partition.name == "sda1"
      assert partition.depth == 1
      assert partition.mountpoint == "/boot"
    end

    test "a node that did not answer is listed as unreadable, not as having no disks" do
      static =
        healthy(%{disks: %{"10.10.0.11" => {:ok, lsblk()}, "10.10.0.12" => {:error, :nxdomain}}})

      snapshot = snapshot(static)

      assert [_ok, unreadable] = snapshot.disks
      assert unreadable.state == :unavailable
      assert unreadable.devices == []
      assert unreadable.error =~ "nxdomain"
      assert snapshot.summary.nodes_unreadable == 1
    end

    test "the newer lsblk mountpoints list is read as well" do
      document = %{
        "blockdevices" => [
          %{"name" => "nvme0n1", "size" => 100, "type" => "disk", "mountpoints" => [nil, "/data"]}
        ]
      }

      static = healthy(%{disks: %{"10.10.0.11" => {:ok, document}}, node_ips: ["10.10.0.11"]})
      snapshot = snapshot(static)

      assert [%{devices: [%{mountpoint: "/data"}]}] = snapshot.disks
    end
  end

  # -- no cluster ------------------------------------------------------------------------

  describe "no cluster" do
    test "an unconfigured host reports nothing rather than an empty healthy fabric" do
      snapshot =
        snapshot(%{
          node_ips: [],
          redundancy_factor: 0,
          pools: {:error, :no_cluster_configured},
          drbd: %{},
          disks: %{}
        })

      refute snapshot.configured?
      assert snapshot.pools.state == :unavailable
      assert snapshot.resources.state == :unavailable
      assert snapshot.disks == []
      refute snapshot.capacity.known?
      assert snapshot.summary.attention?
    end

    test "the live path on a host with no cluster.json reads nothing and says so" do
      # No static payload at all, so this is the real sourcing code. `Cluster.Config`
      # finds no hosts on a development machine, so there is nobody to call and the fan-out
      # never happens -- the page must not hang, and must not come back looking healthy.
      snapshot = Storage.snapshot()

      refute snapshot.configured?
      assert snapshot.nodes == []
      assert snapshot.pools.state == :unavailable
      assert snapshot.resources.state == :unavailable
      assert snapshot.disks == []
      assert snapshot.summary.attention?
    end

    test "a fixture that omits a section treats it as unread, not as empty" do
      snapshot = snapshot(%{node_ips: ["10.10.0.11"]})

      assert snapshot.pools.state == :unavailable
      assert snapshot.resources.state == :unavailable
      assert [%{state: :unavailable}] = snapshot.disks
    end
  end
end
