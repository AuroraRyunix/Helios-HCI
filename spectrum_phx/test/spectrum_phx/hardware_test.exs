defmodule SpectrumPhx.HardwareTest do
  @moduledoc """
  The hardware inventory.

  Two properties are load-bearing and neither is obvious from the happy path: a node that
  answers some reads and not others still appears with what it did answer, and a disk is
  counted once rather than once per partition.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Hardware

  @a "10.10.0.11"
  @b "10.10.0.12"

  defp nodes do
    [%{ip: @a, hostname: "hci-01"}, %{ip: @b, hostname: "hci-02"}]
  end

  defp cpu(opts \\ []) do
    %{
      "model" => Keyword.get(opts, :model, "AMD EPYC 7302P 16-Core Processor"),
      "cores" => Keyword.get(opts, :cores, 32),
      "physical_cores" => Keyword.get(opts, :physical, 16),
      "sockets" => Keyword.get(opts, :sockets, 1),
      "load_average" => Keyword.get(opts, :load, [0.4, 0.6, 0.7])
    }
  end

  defp memory(opts \\ []) do
    %{
      "total_mb" => Keyword.get(opts, :total, 65_536),
      "used_mb" => Keyword.get(opts, :used, 16_384),
      "free_mb" => Keyword.get(opts, :free, 49_152)
    }
  end

  # `lsblk -J` nests partitions under their disk.
  defp disks(opts \\ []) do
    %{
      "blockdevices" => [
        %{
          "name" => "sda",
          "path" => "/dev/sda",
          "type" => "disk",
          "size" => 480_103_981_056,
          "model" => "SAMSUNG MZ7LH480",
          "serial" => "S4J1NX0M",
          "rota" => Keyword.get(opts, :rota, 0),
          "mountpoint" => nil,
          "children" => [
            %{"name" => "sda1", "type" => "part", "size" => 1_073_741_824, "mountpoint" => "/boot"},
            %{"name" => "sda2", "type" => "part", "size" => 479_029_239_232, "mountpoint" => "/"}
          ]
        },
        %{
          "name" => "sdb",
          "path" => "/dev/sdb",
          "type" => "disk",
          "size" => 300_000_000_000,
          "rota" => 1,
          "mountpoint" => nil,
          "children" => []
        }
      ]
    }
  end

  defp network do
    %{
      "addresses" => [
        %{"ifname" => "lo", "operstate" => "UNKNOWN", "addr_info" => []},
        %{
          "ifname" => "eno1",
          "operstate" => "UP",
          "address" => "3c:ec:ef:1a:2b:3c",
          "addr_info" => [%{"local" => "10.10.0.11", "prefixlen" => 24}]
        }
      ]
    }
  end

  defp inventory(static) do
    Hardware.inventory(source: {:static, Map.put_new(static, :nodes, nodes())})
  end

  defp node_at(result, ip), do: Enum.find(result.nodes, &(&1.ip == ip))

  describe "assembly" do
    test "reports each configured node with what it answered" do
      result =
        inventory(%{
          cpu: %{@a => {:ok, cpu()}, @b => {:ok, cpu(cores: 16)}},
          memory: %{@a => {:ok, memory()}, @b => {:ok, memory()}},
          disks: %{@a => {:ok, disks()}, @b => {:ok, disks()}},
          network: %{@a => {:ok, network()}, @b => {:ok, network()}}
        })

      assert length(result.nodes) == 2
      assert result.configured?

      node = node_at(result, @a)
      assert node.hostname == "hci-01"
      assert node.reachable?
      assert node.cpu.cores == 32
      assert node.cpu.model =~ "EPYC"
      assert node.errors == []
    end

    test "memory is converted from the MiB the endpoint speaks into bytes" do
      result = inventory(%{memory: %{@a => {:ok, memory(total: 1024, used: 512)}}})
      node = node_at(result, @a)

      assert node.memory.total_bytes == 1_073_741_824
      assert node.memory.used_bytes == 536_870_912
      assert node.memory.used_percent == 50.0
    end
  end

  describe "a node that only half answers" do
    test "keeps the reads that worked and names the ones that did not" do
      result =
        inventory(%{
          cpu: %{@a => {:ok, cpu()}},
          memory: %{@a => {:error, "connection refused"}},
          disks: %{@a => {:error, "lsblk failed"}},
          network: %{@a => {:ok, network()}}
        })

      node = node_at(result, @a)

      assert node.reachable?, "one failing endpoint is a gap in the record, not an absent machine"
      assert node.cpu.cores == 32
      assert node.interfaces != []
      assert node.memory.total_bytes == nil
      assert node.disks == []

      reads = Enum.map(node.errors, & &1.read) |> Enum.sort()
      assert reads == [:disks, :memory]
    end

    test "a node that answers nothing still appears in the inventory" do
      # An inventory that silently omits a machine is worse than one that says it could
      # not reach it: the whole point is to be able to count what you have.
      result = inventory(%{cpu: %{@b => {:ok, cpu()}}})
      node = node_at(result, @a)

      assert node, "the unreachable node vanished from the inventory"
      refute node.reachable?
      assert length(node.errors) == 4
    end

    test "an unreadable node contributes nothing to the totals rather than zero" do
      result = inventory(%{cpu: %{@a => {:ok, cpu(cores: 8)}}})

      assert result.summary.cores == 8
      assert result.summary.nodes_total == 2
      assert result.summary.nodes_reachable == 1
    end

    test "a total nothing reported is unknown, not zero" do
      result = inventory(%{})

      assert result.summary.cores == nil
      assert result.summary.memory_bytes == nil
    end
  end

  describe "disks" do
    test "a disk is listed once, not once per partition" do
      result = inventory(%{disks: %{@a => {:ok, disks()}}})
      node = node_at(result, @a)

      assert length(node.disks) == 2
      assert Enum.map(node.disks, & &1.name) == ["sda", "sdb"]
    end

    test "its partitions are summarised rather than listed" do
      result = inventory(%{disks: %{@a => {:ok, disks()}}})
      sda = result |> node_at(@a) |> Map.fetch!(:disks) |> Enum.find(&(&1.name == "sda"))

      assert sda.partitions == 2
      assert Enum.sort(sda.mountpoints) == ["/", "/boot"]
    end

    test "rotational media is distinguished from solid state" do
      result = inventory(%{disks: %{@a => {:ok, disks()}}})
      by_name = result |> node_at(@a) |> Map.fetch!(:disks) |> Map.new(&{&1.name, &1})

      refute by_name["sda"].rotational?
      assert by_name["sdb"].rotational?
    end

    test "a disk whose rota is absent is unknown rather than assumed solid state" do
      absent = %{"blockdevices" => [%{"name" => "sdc", "type" => "disk", "size" => 1}]}
      result = inventory(%{disks: %{@a => {:ok, absent}}})

      assert [%{rotational?: nil}] = node_at(result, @a).disks
    end

    test "raw capacity sums every disk across reachable nodes" do
      result = inventory(%{disks: %{@a => {:ok, disks()}, @b => {:ok, disks()}}})

      assert result.summary.disks == 4
      assert result.summary.disk_bytes == 2 * (480_103_981_056 + 300_000_000_000)
    end
  end

  describe "interfaces" do
    test "loopback is left out: it is not hardware anybody is inventorying" do
      result = inventory(%{network: %{@a => {:ok, network()}}})

      assert [interface] = node_at(result, @a).interfaces
      assert interface.name == "eno1"
    end

    test "addresses carry their prefix" do
      result = inventory(%{network: %{@a => {:ok, network()}}})

      assert [%{addresses: ["10.10.0.11/24"], state: "UP"}] = node_at(result, @a).interfaces
    end
  end
end
