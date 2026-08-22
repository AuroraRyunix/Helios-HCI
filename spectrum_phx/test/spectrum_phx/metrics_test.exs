defmodule SpectrumPhx.MetricsTest do
  use ExUnit.Case, async: true

  alias SpectrumPhx.Cluster.Status
  alias SpectrumPhx.Metrics

  defp sample(ip, offset_seconds, attrs \\ %{}) do
    Map.merge(
      %{
        "node_ip" => ip,
        "timestamp" => DateTime.add(~U[2026-08-19 10:00:00Z], offset_seconds, :second),
        "cpu_pct" => 10.0,
        "mem_pct" => 50.0,
        "mem_total_kb" => 16_777_216,
        "cpu_cores" => 8,
        "disk_iops" => 120.0,
        "disk_bandwidth_kbps" => 4_096.0,
        "net_rx_kbps" => 512.0,
        "net_tx_kbps" => 256.0
      },
      attrs
    )
  end

  # Two nodes reporting, one configured node that has never written a sample.
  defp rows do
    [
      sample("10.10.0.11", 0, %{"cpu_pct" => 5.0}),
      sample("10.10.0.11", 30, %{"cpu_pct" => 25.0}),
      sample("10.10.0.11", 60, %{"cpu_pct" => 45.0, "mem_pct" => 80.0}),
      sample("10.10.0.12", 0, %{"cpu_pct" => 60.0}),
      sample("10.10.0.12", 30, %{"cpu_pct" => 95.0, "mem_pct" => 20.0})
    ]
  end

  defp fetch(opts) do
    Metrics.fetch(
      Keyword.merge(
        [rows: rows(), node_ips: ~w(10.10.0.11 10.10.0.12 10.10.0.13), cluster: nil],
        opts
      )
    )
  end

  describe "assembly" do
    test "groups samples per node, oldest first, with the newest as the current value" do
      snapshot = fetch([])

      node = Enum.find(snapshot.nodes, &(&1.ip == "10.10.0.11"))

      assert length(node.samples) == 3
      assert Enum.map(node.samples, & &1.cpu_pct) == [5.0, 25.0, 45.0]
      assert node.latest.cpu_pct == 45.0
      assert node.reporting?
    end

    test "derives used memory from the percentage and the node's total" do
      node = fetch([]).nodes |> Enum.find(&(&1.ip == "10.10.0.11"))

      assert node.latest.mem_total_kb == 16_777_216
      assert node.latest.mem_used_kb == 13_421_773
    end

    test "keeps only the newest window of samples" do
      rows = for offset <- 0..99, do: sample("10.10.0.11", offset * 30)

      node =
        [rows: rows, node_ips: ["10.10.0.11"], window: 5, cluster: nil]
        |> Metrics.fetch()
        |> Map.fetch!(:nodes)
        |> List.first()

      assert length(node.samples) == 5
      assert node.latest.at == DateTime.add(~U[2026-08-19 10:00:00Z], 99 * 30, :second)
    end

    test "a node that is publishing but absent from cluster.json is still shown" do
      snapshot = Metrics.fetch(rows: rows(), node_ips: ["10.10.0.11"], cluster: nil)

      assert Enum.map(snapshot.nodes, & &1.ip) == ["10.10.0.11", "10.10.0.12"]
    end

    test "joins liveness and hostnames from the cluster snapshot" do
      cluster =
        Status.fetch(
          nodes: %{
            "10.10.0.11" => %{"hostname" => "hci-01", "services" => %{}, "disks" => 6}
          },
          node_ips: ~w(10.10.0.11 10.10.0.12 10.10.0.13)
        )

      snapshot = fetch(cluster: cluster)

      assert Enum.find(snapshot.nodes, &(&1.ip == "10.10.0.11")).hostname == "hci-01"
      assert Enum.find(snapshot.nodes, &(&1.ip == "10.10.0.11")).state == :up
      assert Enum.find(snapshot.nodes, &(&1.ip == "10.10.0.13")).state == :down
    end
  end

  describe "silence is not idleness" do
    test "a configured node with no samples reports no telemetry rather than zero load" do
      node = fetch([]).nodes |> Enum.find(&(&1.ip == "10.10.0.13"))

      refute node.reporting?
      assert node.latest == nil
      assert node.samples == []
      assert node.age_seconds == nil
    end

    test "averages are taken over reporting nodes only" do
      summary = fetch([]).summary

      assert summary.nodes_total == 3
      assert summary.nodes_reporting == 2
      assert summary.nodes_silent == 1
      # (45.0 + 95.0) / 2 -- the silent node does not drag this toward zero.
      assert summary.cpu_avg == 70.0
      assert summary.cpu_max == 95.0
    end

    test "capacity totals only count nodes that reported one" do
      summary = fetch([]).summary

      assert summary.cores_total == 16
      assert summary.mem_total_kb == 33_554_432
    end

    test "a node whose newest sample is older than the threshold is marked stale" do
      old = sample("10.10.0.11", 0, %{"timestamp" => DateTime.add(DateTime.utc_now(), -600)})
      fresh = sample("10.10.0.12", 0, %{"timestamp" => DateTime.utc_now()})

      snapshot =
        Metrics.fetch(rows: [old, fresh], node_ips: ~w(10.10.0.11 10.10.0.12), cluster: nil)

      assert Enum.find(snapshot.nodes, &(&1.ip == "10.10.0.11")).stale?
      refute Enum.find(snapshot.nodes, &(&1.ip == "10.10.0.12")).stale?
      assert snapshot.summary.nodes_stale == 1
    end
  end

  describe "availability" do
    test "an unreadable database still lists the nodes, with no data on any of them" do
      snapshot =
        Metrics.fetch(error: :econnrefused, node_ips: ~w(10.10.0.11 10.10.0.12), cluster: nil)

      refute snapshot.available?
      assert snapshot.error =~ "econnrefused"
      assert length(snapshot.nodes) == 2
      assert Enum.all?(snapshot.nodes, &(&1.reporting? == false))
      assert snapshot.summary.cpu_avg == nil
    end

    test "no configured nodes is reported as unconfigured, not as a healthy empty cluster" do
      snapshot = Metrics.fetch(rows: [], node_ips: [], cluster: nil)

      refute snapshot.configured?
      assert snapshot.nodes == []
      assert snapshot.summary.nodes_total == 0
    end

    test "an empty but readable table is available with every node silent" do
      snapshot = Metrics.fetch(rows: [], node_ips: ["10.10.0.11"], cluster: nil)

      assert snapshot.available?
      assert snapshot.configured?
      refute List.first(snapshot.nodes).reporting?
    end
  end

  describe "spark_points/3" do
    test "a single sample plots nothing: one point is not a trend" do
      node = fetch([]).nodes |> Enum.find(&(&1.ip == "10.10.0.11"))
      assert Metrics.spark_points(Enum.take(node.samples, 1), :cpu_pct, 100) == []
    end

    test "scales the y axis against the given ceiling and inverts it for SVG" do
      node = fetch([]).nodes |> Enum.find(&(&1.ip == "10.10.0.11"))

      assert [{0.0, 95.0}, {50.0, 75.0}, {100.0, 55.0}] ==
               Metrics.spark_points(node.samples, :cpu_pct, 100)
    end

    test "a value above the ceiling is clamped to the top of the box" do
      node = fetch([]).nodes |> Enum.find(&(&1.ip == "10.10.0.12"))

      points = Metrics.spark_points(node.samples, :cpu_pct, 50)
      assert Enum.all?(points, fn {_x, y} -> y >= 0.0 end)
      assert {100.0, 0.0} == List.last(points)
    end

    test "peak/2 is the largest value anywhere in the window" do
      assert Metrics.peak(fetch([]).nodes, :cpu_pct) == 95.0
      assert Metrics.peak(fetch([]).nodes, :disk_iops) == 120.0
    end
  end

  describe "statement" do
    test "reads one partition at a time with a bound limit" do
      assert Metrics.node_cql() =~ "WHERE node_ip = ?"
      assert Metrics.node_cql() =~ "LIMIT ?"
      refute Metrics.node_cql() =~ "JSON"
    end
  end
end
