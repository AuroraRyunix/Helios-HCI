defmodule SpectrumPhx.SnapshotsTest do
  # Not async: the source is configured through application env, which is global.
  use ExUnit.Case, async: false

  alias SpectrumPhx.Snapshots

  @disk %{
    "vdisk_id" => "vm-disk0",
    "class" => "rw",
    "container" => "pool",
    "parent_vdisk" => nil,
    "size_bytes" => 10_737_418_240,
    "created_at_ms" => 1_700_000_000_000
  }

  defp snap(id, parent, at, opts \\ []) do
    %{
      "vdisk_id" => id,
      "class" => Keyword.get(opts, :class, "immutable"),
      "container" => "pool",
      "parent_vdisk" => parent,
      "size_bytes" => 10_737_418_240,
      "created_at_ms" => at
    }
  end

  defp indexed(id, at, origin),
    do: %{
      "vdisk_id" => "vm-disk0",
      "created_at_ms" => at,
      "snapshot_id" => id,
      "origin" => origin
    }

  defp put(data), do: Application.put_env(:spectrum_phx, :snapshots_source, {:static, data})

  setup do
    on_exit(fn -> Application.delete_env(:spectrum_phx, :snapshots_source) end)
    :ok
  end

  describe "statements" do
    test "the index read binds the vdisk id and the others name their columns" do
      assert Snapshots.index_cql() =~ "WHERE vdisk_id = ?"
      refute Snapshots.index_cql() =~ "'"
      assert Snapshots.vdisks_cql() =~ "parent_vdisk"
      assert Snapshots.policies_cql() =~ "FROM hydra.dfs_snapshot_policies"
    end
  end

  describe "for_vdisk/1" do
    test "lists only immutable children of that vdisk, newest first" do
      put(%{
        vdisks: [
          @disk,
          snap("vm-disk0-auto-1", "vm-disk0", 100),
          snap("vm-disk0-auto-2", "vm-disk0", 200),
          snap("writable-clone", "vm-disk0", 300, class: "rw"),
          snap("other-snap", "other-disk0", 400)
        ]
      })

      assert {:ok, %{snapshots: snaps}} = Snapshots.for_vdisk("vm-disk0")
      assert Enum.map(snaps, & &1.id) == ["vm-disk0-auto-2", "vm-disk0-auto-1"]
    end

    test "says who took each one, and says unindexed rather than guessing" do
      put(%{
        vdisks: [
          @disk,
          snap("a", "vm-disk0", 100),
          snap("b", "vm-disk0", 200),
          snap("c", "vm-disk0", 300),
          snap("d", "vm-disk0", 400)
        ],
        index: [
          indexed("a", 100, "policy"),
          indexed("b", 200, "manual"),
          indexed("c", 300, "pre-rollback")
        ]
      })

      {:ok, %{snapshots: snaps}} = Snapshots.for_vdisk("vm-disk0")
      origins = Map.new(snaps, &{&1.id, &1.origin})
      assert origins == %{"a" => :policy, "b" => :manual, "c" => :pre_rollback, "d" => :unindexed}
    end

    test "flags a snapshot another vdisk was derived from, because retention will not prune it" do
      put(%{
        vdisks: [
          @disk,
          snap("pinned", "vm-disk0", 100),
          snap("free", "vm-disk0", 200),
          snap("restored", "pinned", 300, class: "rw")
        ]
      })

      {:ok, %{snapshots: snaps}} = Snapshots.for_vdisk("vm-disk0")
      flags = Map.new(snaps, &{&1.id, &1.has_children?})
      assert flags == %{"pinned" => true, "free" => false}
    end

    test "a vdisk that does not exist is not_found, not an empty list" do
      put(%{vdisks: [@disk]})
      assert {:error, :not_found} = Snapshots.for_vdisk("nope-disk0")
    end

    test "a name that could not be a vdisk id never reaches a query" do
      put(%{vdisks: [@disk]})
      assert {:error, :invalid_name} = Snapshots.for_vdisk("x'; DROP TABLE hydra.vms; --")
      assert {:error, :invalid_name} = Snapshots.for_vdisk("../etc")
    end
  end

  describe "policy_for/3" do
    @cluster %{
      "scope" => "cluster",
      "target" => "*",
      "enabled" => true,
      "interval_seconds" => 86_400,
      "keep_last" => 7
    }
    @container %{
      "scope" => "container",
      "target" => "pool",
      "enabled" => true,
      "interval_seconds" => 43_200,
      "keep_last" => 14
    }
    @vdisk %{
      "scope" => "vdisk",
      "target" => "vm-disk0",
      "enabled" => true,
      "interval_seconds" => 3_600,
      "keep_last" => 24
    }

    test "narrowest wins: vdisk, then container, then cluster" do
      all = [@cluster, @container, @vdisk]
      assert {:policy, %{keep: 24}} = Snapshots.policy_for(all, "vm-disk0", "pool")
      assert {:policy, %{keep: 14}} = Snapshots.policy_for(all, "other-disk0", "pool")
      assert {:policy, %{keep: 7}} = Snapshots.policy_for(all, "other-disk0", "elsewhere")
    end

    test "a disabled narrow policy exempts the disk instead of falling through to the default" do
      off = %{@vdisk | "enabled" => false}
      assert :exempt = Snapshots.policy_for([@cluster, off], "vm-disk0", "pool")
      assert {:policy, _} = Snapshots.policy_for([@cluster, off], "other-disk0", "pool")
    end

    test "no policy is :none, which is not the same as exempt" do
      assert :none = Snapshots.policy_for([], "vm-disk0", "pool")
    end
  end
end
