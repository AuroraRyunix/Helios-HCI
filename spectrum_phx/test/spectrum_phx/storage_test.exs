defmodule SpectrumPhx.StorageTest do
  # Not async: almost every case drives `snapshot/1` through its `:static` option and
  # touches nothing global, but one deliberately exercises the live sourcing path, which
  # reads the storage source out of application env.
  use ExUnit.Case, async: false

  alias SpectrumPhx.Storage

  @a "10.10.0.11"
  @b "10.10.0.12"
  @two_nodes [@a, @b]

  # -- fixtures ------------------------------------------------------------------------

  # Sidon's `capacity` document. Bytes, from statfs on the extent store's own filesystem.
  defp capacity(node, opts \\ []) do
    %{
      "node" => node,
      "path" => "/var/lib/hci/sidon/egroups",
      "total_bytes" => Keyword.get(opts, :total, 107_374_182_400),
      "available_bytes" => Keyword.get(opts, :available, 53_687_091_200),
      "egroup_bytes" => Keyword.get(opts, :egroup_bytes, 50_331_648),
      "egroup_count" => Keyword.get(opts, :egroup_count, 12),
      "journal_bytes" => Keyword.get(opts, :journal_bytes, 4_194_304)
    }
  end

  # One entry of Sidon's `list` document.
  defp owned(id, opts \\ []) do
    %{
      "vdisk_id" => id,
      "socket" => "/var/lib/hci/sidon/nbd/#{id}.sock",
      "role" => "owner",
      "epoch" => Keyword.get(opts, :epoch, 3),
      "size_bytes" => Keyword.get(opts, :size, 10_737_418_240),
      "degraded" => Keyword.get(opts, :degraded, false),
      "class" => Keyword.get(opts, :class, "mutable"),
      "replicas" => Keyword.get(opts, :replicas, ["hci-01", "hci-02"])
    }
  end

  defp forwarding(id, to) do
    %{
      "vdisk_id" => id,
      "socket" => "/var/lib/hci/sidon/nbd/#{id}.sock",
      "role" => "forwarding",
      "forwarding_to" => to
    }
  end

  defp attached(entries), do: {:ok, %{"attached" => entries}}

  defp peers(node, entries) do
    {:ok, %{"node" => node, "peers" => entries}}
  end

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
              "mountpoints" => ["/boot"],
              "fstype" => "xfs",
              "rota" => false
            }
          ]
        }
      ]
    }
  end

  # A cluster where both nodes answer everything and nothing is wrong.
  defp healthy(opts \\ []) do
    vdisks = Keyword.get(opts, :vdisks, [owned("vm1-disk0")])

    %{
      node_ips: @two_nodes,
      redundancy_factor: Keyword.get(opts, :rf, 1),
      capacity: %{@a => {:ok, capacity("hci-01")}, @b => {:ok, capacity("hci-02")}},
      vdisks: %{@a => attached(vdisks), @b => attached([])},
      peers: %{
        @a => peers("hci-01", [peer("hci-02")]),
        @b => peers("hci-02", [peer("hci-01")])
      },
      disks: %{@a => {:ok, lsblk()}, @b => {:ok, lsblk()}}
    }
  end

  defp snap(static), do: Storage.snapshot(static: static)

  defp vdisk(snapshot, id) do
    Enum.find(snapshot.vdisks.entries, &(&1.id == id))
  end

  defp store(snapshot, ip) do
    Enum.find(snapshot.stores.entries, &(&1.ip == ip))
  end

  # -- no cluster ------------------------------------------------------------------------

  describe "no cluster" do
    test "an unconfigured host reports nothing rather than an empty healthy fabric" do
      snapshot = snap(%{node_ips: []})

      refute snapshot.configured?
      assert snapshot.stores.state == :unavailable
      assert snapshot.vdisks.state == :unavailable
      assert snapshot.disks == []
      refute snapshot.capacity.known?
      # The distinction the whole module exists for: "nothing was read" is not "nothing
      # is wrong".
      assert snapshot.summary.attention?
    end
  end

  # -- extent stores ---------------------------------------------------------------------

  describe "extent stores" do
    test "capacity is read as bytes and used is derived from what is left" do
      snapshot = snap(healthy())
      store = store(snapshot, @a)

      assert store.total_bytes == 107_374_182_400
      assert store.available_bytes == 53_687_091_200
      assert store.used_bytes == 53_687_091_200
      assert_in_delta store.used_percent, 50.0, 0.01
      assert store.state == :ok
      assert store.messages == []
    end

    test "a store reporting no capacity is unknown, not an empty one at 0% used" do
      static = put_in(healthy().capacity[@a], {:ok, capacity("hci-01", total: 0, available: 0)})
      snapshot = snap(static)

      store = store(snapshot, @a)
      assert store.state == :unknown
      assert store.used_percent == nil
      assert [message] = store.messages
      assert message =~ "not mounted"
      assert snapshot.summary.attention?
    end

    test "a store that cannot drain is an error, not a warning" do
      # 96% used.
      full = capacity("hci-01", total: 100_000_000_000, available: 4_000_000_000)
      snapshot = snap(put_in(healthy().capacity[@a], {:ok, full}))

      store = store(snapshot, @a)
      assert store.state == :full
      assert [message] = store.messages
      assert message =~ "cannot drain"
    end

    test "a store filling up warns before it is too late to reclaim" do
      # 88% used: past the warn line, short of the one where a drain fails.
      filling = capacity("hci-01", total: 100_000_000_000, available: 12_000_000_000)
      snapshot = snap(put_in(healthy().capacity[@a], {:ok, filling}))

      assert store(snapshot, @a).state == :warn
      assert snapshot.summary.stores_warn == 1
      assert snapshot.summary.stores_full == 0
    end

    test "a node that did not answer is named rather than dropped" do
      static = put_in(healthy().capacity[@b], {:error, "connection refused"})
      snapshot = snap(static)

      assert snapshot.stores.state == :partial
      assert [%{ip: @b, error: "connection refused"}] = snapshot.stores.unreachable
      # And it is not silently counted as a store with no capacity.
      assert length(snapshot.stores.entries) == 1
    end

    test "no node answering is unavailable, not a cluster with no storage" do
      static =
        healthy()
        |> put_in([:capacity, @a], {:error, :timeout})
        |> put_in([:capacity, @b], {:error, :timeout})

      snapshot = snap(static)

      assert snapshot.stores.state == :unavailable
      assert snapshot.stores.entries == []
      refute snapshot.capacity.known?
    end
  end

  # -- capacity --------------------------------------------------------------------------

  describe "capacity" do
    test "usable is raw divided by the number of copies kept" do
      # rf: 1 means ftt=1, so two copies.
      snapshot = snap(healthy(rf: 1))

      assert snapshot.expected_replicas == 2
      assert snapshot.capacity.raw_total_bytes == 2 * 107_374_182_400
      assert snapshot.capacity.usable_total_bytes == 107_374_182_400
    end

    test "a single-node cluster keeps one copy however the factor reads" do
      static = %{
        node_ips: [@a],
        redundancy_factor: 2,
        capacity: %{@a => {:ok, capacity("hci-01")}},
        vdisks: %{@a => attached([owned("vm1-disk0", replicas: ["hci-01"])])},
        peers: %{@a => peers("hci-01", [])},
        disks: %{@a => {:ok, lsblk()}}
      }

      snapshot = snap(static)

      # ftt=0 is a supported topology, not a broken one: asking for three replicas on one
      # node would flag every vdisk on a deployment that is behaving exactly as designed.
      assert snapshot.expected_replicas == 1
      assert vdisk(snapshot, "vm1-disk0").health == :ok
      assert snapshot.capacity.usable_total_bytes == snapshot.capacity.raw_total_bytes
    end

    test "a partial read is not presented as the cluster total" do
      static = put_in(healthy().capacity[@b], {:error, :timeout})
      snapshot = snap(static)

      assert snapshot.stores.state == :partial
      assert snapshot.capacity.raw_total_bytes == 107_374_182_400
    end
  end

  # -- vdisk health ----------------------------------------------------------------------

  describe "vdisk health" do
    test "an owned, fully replicated vdisk is healthy" do
      snapshot = snap(healthy())
      disk = vdisk(snapshot, "vm1-disk0")

      assert disk.health == :ok
      # Node display names come from the cluster document, which a test has none of, so
      # `hostname_for/1` correctly falls back to the address. Sidon's own node names --
      # the ones in `replicas` and in the peer list -- are unaffected, which is what
      # makes the stranded-replica match below work.
      assert disk.owner == @a
      assert disk.epoch == 3
      assert disk.replica_count == 2
      assert disk.under_replicated? == false
      assert disk.issues == []
      refute snapshot.summary.attention?
    end

    test "a degraded owner means writes are being refused, and says so" do
      static = healthy(vdisks: [owned("vm1-disk0", degraded: true)])
      snapshot = snap(static)

      disk = vdisk(snapshot, "vm1-disk0")
      assert disk.health == :degraded
      assert Enum.any?(disk.issues, &(&1 =~ "writes are refused"))
      assert snapshot.summary.attention?
    end

    test "a vdisk short of its replica count is flagged with the numbers" do
      static = healthy(vdisks: [owned("vm1-disk0", replicas: ["hci-01"])])
      snapshot = snap(static)

      disk = vdisk(snapshot, "vm1-disk0")
      assert disk.under_replicated? == true
      assert disk.health == :degraded
      assert "1 of 2 replicas present" in disk.issues
      assert snapshot.summary.vdisks_under_replicated == 1
    end

    test "a sealed vdisk is reported as such and is not a fault" do
      static = healthy(vdisks: [owned("img-rocky", class: "immutable")])
      snapshot = snap(static)

      disk = vdisk(snapshot, "img-rocky")
      assert disk.sealed?
      assert disk.health == :ok
    end

    test "forwarding is a role, not a fault" do
      static =
        healthy()
        |> put_in([:vdisks, @b], attached([forwarding("vm1-disk0", "hci-01")]))

      snapshot = snap(static)
      disk = vdisk(snapshot, "vm1-disk0")

      # A non-owner relaying I/O to the owner is what removes live migration's cutover
      # instant. It is exercised constantly and must not light the page up.
      assert disk.forwarders == [@b]
      assert disk.owner == @a
      assert disk.health == :ok
      assert disk.issues == []
    end

    test "a vdisk that is only being relayed has no owner and is unknown" do
      static =
        healthy(vdisks: [forwarding("vm1-disk0", "hci-03")])
        |> put_in([:vdisks, @b], attached([forwarding("vm1-disk0", "hci-03")]))

      snapshot = snap(static)
      disk = vdisk(snapshot, "vm1-disk0")

      assert disk.owner == nil
      assert disk.health == :unknown
      assert Enum.any?(disk.issues, &(&1 =~ "no owner"))
    end

    test "two owners at once is reported as the fence failing, not as a replica problem" do
      static =
        healthy()
        |> put_in([:vdisks, @b], attached([owned("vm1-disk0", epoch: 2)]))

      snapshot = snap(static)
      disk = vdisk(snapshot, "vm1-disk0")

      assert disk.health == :degraded
      assert [issue] = Enum.filter(disk.issues, &(&1 =~ "at once"))
      assert issue =~ "epoch fence"
    end
  end

  # -- peers -----------------------------------------------------------------------------

  describe "peer reachability" do
    test "a peer that cannot be reached is reported as a refused write, not lost redundancy" do
      static =
        put_in(
          healthy().peers[@a],
          peers("hci-01", [peer("hci-02", reachable: false, detail: "connection refused")])
        )

      snapshot = snap(static)

      assert [link] = snapshot.peers.unreachable
      assert link.from == @a
      assert link.peer == "hci-02"
      assert link.detail == "connection refused"

      # The journal is write-all, so a vdisk replicated onto that node is taking EIO --
      # which is a different and much worse statement than "fewer copies than we want".
      disk = vdisk(snapshot, "vm1-disk0")
      assert "hci-02" in disk.stranded_replicas
      assert Enum.any?(disk.issues, &(&1 =~ "writes are being refused"))
      assert disk.health == :degraded
      assert snapshot.summary.peer_links_down == 1
    end

    test "a reachable peer set produces no links down" do
      snapshot = snap(healthy())

      assert snapshot.peers.unreachable == []
      assert snapshot.peers.state == :ok
      assert snapshot.summary.peer_links_down == 0
    end

    test "a replica on an unreachable node this vdisk does not use is not its problem" do
      static =
        healthy(vdisks: [owned("vm1-disk0", replicas: ["hci-01", "hci-02"])])
        |> put_in([:peers, @a], peers("hci-01", [peer("hci-09", reachable: false)]))

      snapshot = snap(static)

      assert length(snapshot.peers.unreachable) == 1
      assert vdisk(snapshot, "vm1-disk0").stranded_replicas == []
      assert vdisk(snapshot, "vm1-disk0").issues == []
    end
  end

  # -- unreachable nodes -------------------------------------------------------------------

  describe "unreachable nodes" do
    test "a node that did not answer makes an otherwise-clean vdisk unknown" do
      static = put_in(healthy().vdisks[@b], {:error, "no route to host"})
      snapshot = snap(static)

      assert snapshot.vdisks.state == :partial
      disk = vdisk(snapshot, "vm1-disk0")

      assert disk.health == :unknown
      # `nil` and not `false`: the unread node might be the one holding the copy we could
      # not count, and might equally hold nothing.
      assert disk.under_replicated? == nil
      assert snapshot.summary.vdisks_unknown == 1
      assert snapshot.summary.attention?
    end

    test "a shortfall is not asserted while a node is unread" do
      static =
        healthy(vdisks: [owned("vm1-disk0", replicas: ["hci-01"])])
        |> put_in([:vdisks, @b], {:error, :timeout})

      snapshot = snap(static)
      disk = vdisk(snapshot, "vm1-disk0")

      assert disk.replica_count == 1
      assert disk.under_replicated? == nil
      # No "1 of 2 replicas present": that would be a claim the data does not support.
      refute Enum.any?(disk.issues, &(&1 =~ "replicas present"))
      assert disk.health == :unknown
    end

    test "no node answering is unavailable, not an empty vdisk list" do
      static =
        healthy()
        |> put_in([:vdisks, @a], {:error, :timeout})
        |> put_in([:vdisks, @b], {:error, :timeout})

      snapshot = snap(static)

      assert snapshot.vdisks.state == :unavailable
      assert snapshot.vdisks.entries == []
      assert length(snapshot.vdisks.unreachable) == 2
      assert snapshot.summary.attention?
    end

    test "every node answering with nothing is an honest empty fabric" do
      static = healthy(vdisks: [])
      snapshot = snap(static)

      assert snapshot.vdisks.state == :ok
      assert snapshot.vdisks.entries == []
      # Nothing attached is not a fault. The stores answered and have room.
      refute snapshot.summary.attention?
    end

    test "a fixture that omits a section reports it unavailable rather than empty" do
      static = %{node_ips: @two_nodes, redundancy_factor: 1}
      snapshot = snap(static)

      assert snapshot.stores.state == :unavailable
      assert snapshot.vdisks.state == :unavailable
      assert Enum.all?(snapshot.disks, &(&1.state == :unavailable))
    end
  end

  # -- block devices -----------------------------------------------------------------------

  describe "block devices" do
    test "children are flattened with their depth and their mountpoints found" do
      snapshot = snap(healthy())
      node = Enum.find(snapshot.disks, &(&1.ip == @a))

      assert [disk, part] = node.devices
      assert disk.name == "sda"
      assert disk.depth == 0
      assert disk.rotational? == false
      assert part.name == "sda1"
      assert part.depth == 1
      # lsblk moved from "mountpoint" to a "mountpoints" list; both are read.
      assert part.mountpoint == "/boot"
    end

    test "a node that did not answer has unknown disks, not none" do
      static = put_in(healthy().disks[@b], {:error, "ssh: connect failed"})
      snapshot = snap(static)

      node = Enum.find(snapshot.disks, &(&1.ip == @b))
      assert node.state == :unavailable
      assert node.error == "ssh: connect failed"
      assert node.devices == []
      assert snapshot.summary.nodes_unreadable == 1
    end
  end

  # -- sourcing ----------------------------------------------------------------------------

  describe "source/0" do
    test "defaults to live and can be pinned to a static payload" do
      assert Storage.source() == :live

      Application.put_env(:spectrum_phx, :storage_source, {:static, healthy()})
      on_exit(fn -> Application.delete_env(:spectrum_phx, :storage_source) end)

      snapshot = Storage.snapshot()
      assert snapshot.configured?
      assert length(snapshot.vdisks.entries) == 1
    end
  end
end
