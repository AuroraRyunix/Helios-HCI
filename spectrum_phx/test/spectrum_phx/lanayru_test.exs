defmodule SpectrumPhx.LanayruTest do
  @moduledoc """
  The Kubernetes engine's pre-flight.

  The whole value of these checks is that they are believed, so the property that matters
  most is the unhappy one: a check that could not be run must never read as ready.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Lanayru

  defp ring_node(status \\ "UN") do
    %{"status" => String.first(status), "state" => String.last(status)}
  end

  defp base(overrides) do
    Map.merge(
      %{
        expected_nodes: 3,
        ring: {:ok, [ring_node(), ring_node(), ring_node()]},
        capacity: {:ok, %{"total_bytes" => 100_000_000_000, "available_bytes" => 60_000_000_000}},
        memory: {:ok, %{"free_mb" => 8192}},
        segments: [%{"segment_id" => "s1", "name" => "app-net"}]
      },
      overrides
    )
  end

  defp checks(overrides \\ %{}), do: Lanayru.checks(base(overrides))

  defp check(overrides, id) do
    checks(overrides) |> Enum.find(&(&1.id == id))
  end

  describe "a healthy cluster" do
    test "passes every check" do
      result = checks()

      assert Lanayru.ready?(result)
      assert length(result) == 4
      assert Enum.map(result, & &1.id) == [:consensus, :storage, :compute, :overlay]
    end
  end

  describe "consensus" do
    test "counts members that are up and normal, not members that exist" do
      assert check(%{}, :consensus).status == :ready
    end

    test "a partial ring warns and says how partial" do
      result = check(%{ring: {:ok, [ring_node(), ring_node("DN")]}}, :consensus)

      assert result.status == :warning
      assert result.message =~ "1 of 3"
    end

    test "a ring that cannot be read is an error, never a pass" do
      result = check(%{ring: {:error, "connection refused"}}, :consensus)

      assert result.status == :error
      assert result.message =~ "connection refused"
    end
  end

  describe "storage" do
    test "a store with room is ready and says how much" do
      result = check(%{}, :storage)

      assert result.status == :ready
      assert result.message =~ "GiB used"
    end

    test "a nearly full store warns before writes start failing" do
      nearly = {:ok, %{"total_bytes" => 100, "available_bytes" => 12}}
      assert check(%{capacity: nearly}, :storage).status == :warning
    end

    test "a full store is an error, because the deploy would run it out" do
      full = {:ok, %{"total_bytes" => 100, "available_bytes" => 2}}
      result = check(%{capacity: full}, :storage)

      assert result.status == :error
      assert result.message =~ "run it out"
    end

    test "zero capacity is an unmounted store, not an empty one" do
      none = {:ok, %{"total_bytes" => 0, "available_bytes" => 0}}
      result = check(%{capacity: none}, :storage)

      assert result.status == :error
      assert result.message =~ "not mounted"
    end

    test "a store that cannot be reached is an error" do
      assert check(%{capacity: {:error, :timeout}}, :storage).status == :error
    end
  end

  describe "compute" do
    test "enough free memory passes" do
      assert check(%{}, :compute).status == :ready
    end

    test "too little warns, because the control plane has to fit" do
      assert check(%{memory: {:ok, %{"free_mb" => 512}}}, :compute).status == :warning
    end

    test "memory that cannot be read is an error rather than assumed sufficient" do
      assert check(%{memory: {:error, :refused}}, :compute).status == :error
    end
  end

  describe "overlay" do
    test "a segment to run on passes" do
      assert check(%{}, :overlay).status == :ready
    end

    test "no segment is an error, not a warning" do
      # Kubernetes on no network is the failure that looks like success until a pod tries
      # to talk to another one.
      result = check(%{segments: []}, :overlay)

      assert result.status == :error
      assert result.message =~ "SDN page"
    end

    test "a segment table that cannot be read is an error" do
      assert check(%{segments: {:error, :timeout}}, :overlay).status == :error
    end
  end

  describe "readiness" do
    test "one warning is enough to not be ready" do
      refute Lanayru.ready?(checks(%{memory: {:ok, %{"free_mb" => 100}}}))
    end

    test "one unreadable source is enough to not be ready" do
      refute Lanayru.ready?(checks(%{ring: {:error, :timeout}}))
    end
  end

  describe "the cluster on record" do
    test "is reported when one exists" do
      rows = [
        %{
          "cluster_id" => "c1",
          "name" => "kube-01",
          "control_nodes" => 3,
          "status" => "Running",
          "overlay_segment_id" => "s1"
        }
      ]

      result = Lanayru.overview(source: {:static, base(%{clusters: rows})})

      assert result.cluster.name == "kube-01"
      assert result.cluster.control_nodes == 3
    end

    test "is nil when none is recorded" do
      assert Lanayru.overview(source: {:static, base(%{})}).cluster == nil
    end
  end

  describe "statements" do
    test "every read names its columns" do
      for {_name, cql} <- Lanayru.statements(), do: refute(cql =~ "SELECT *")
    end
  end
end
