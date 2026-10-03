defmodule SpectrumPhx.SparkDfsCreateTest do
  @moduledoc """
  What a vdisk create sends to Sidon.

  The property is that a create always names a container. Sidon derives a new vdisk's copy
  count from its container's `ftt`, so a create that leaves the container out is a create
  with no replication policy: it reached the daemon as the literal "default", which matches
  no row, and every vdisk the Phoenix tier made had one copy on a cluster configured for
  two. The test is on the request body because that is the only thing the daemon sees.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Spark

  test "the container reaches the daemon in the request" do
    params = Spark.dfs_create_params("web-01-disk0", 10, container: "default-pool")

    assert params["container"] == "default-pool"
    assert params["vdisk_id"] == "web-01-disk0"
    assert params["size_bytes"] == 10
  end

  test "a create that names no container is a programming error, not a create with defaults" do
    for opts <- [[], [container: nil], [container: ""]] do
      assert_raise ArgumentError, ~r/needs a :container/, fn ->
        Spark.dfs_create_params("web-01-disk0", 10, opts)
      end
    end
  end

  test "an explicit rf is passed through and an absent one is not invented" do
    assert Spark.dfs_create_params("a", 1, container: "c", rf: 2)["rf"] == 2
    refute Map.has_key?(Spark.dfs_create_params("a", 1, container: "c"), "rf")
  end
end
