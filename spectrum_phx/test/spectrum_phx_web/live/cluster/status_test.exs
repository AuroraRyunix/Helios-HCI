defmodule SpectrumPhx.Cluster.StatusTest do
  # Lives under the LiveView test tree because this task owns only that path; the module
  # under test is `SpectrumPhx.Cluster.Status`.
  use ExUnit.Case, async: false

  alias SpectrumPhx.Cluster.Status

  # Stand-in for `SpectrumPhx.Zk.State`, which another part of the rewrite owns. Status
  # resolves the reader module at runtime, so swapping it here exercises the real
  # ZooKeeper branch without a ZooKeeper.
  defmodule ZkStub do
    def read_cluster_state do
      case :persistent_term.get({__MODULE__, :reply}, {:error, :not_set}) do
        fun when is_function(fun, 0) -> fun.()
        reply -> reply
      end
    end
  end

  defmodule ZkMissingCallback do
    def some_other_function, do: :ok
  end

  setup do
    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :zk_state_module)
      Application.delete_env(:spectrum_phx, :cluster_status_override)
      :persistent_term.erase({ZkStub, :reply})
    end)

    :ok
  end

  defp use_zk(reply) do
    :persistent_term.put({ZkStub, :reply}, reply)
    Application.put_env(:spectrum_phx, :zk_state_module, ZkStub)
  end

  defp node_doc(overrides \\ %{}) do
    Map.merge(
      %{
        "ip" => "10.10.0.11",
        "hostname" => "hci-01",
        "zk_leader" => true,
        "maintenance_status" => "NORMAL",
        "disks" => 6,
        "build" => "2026.08.17-1",
        "ts" => System.system_time(:second),
        "services" => %{
          "ZooKeeper" => %{"status" => "UP", "pids" => [1201], "restarts" => 0},
          "HydraDB" => %{"status" => "UP", "pids" => [1302, 1303], "restarts" => 0},
          "Vali" => %{"status" => "FLAPPING", "pids" => [], "restarts" => 7},
          "Mimir" => %{"status" => "DOWN", "pids" => [], "restarts" => 2}
        }
      },
      overrides
    )
  end

  describe "fetch/1 with injected nodes" do
    test "normalises a published document into a node view" do
      snapshot = Status.fetch(nodes: %{"10.10.0.11" => node_doc()}, desired: "started")

      assert [host] = snapshot.nodes
      assert host.ip == "10.10.0.11"
      assert host.hostname == "hci-01"
      assert host.state == :up
      assert host.registered?
      assert host.zk_leader?
      assert host.disks == 6
      assert host.build == "2026.08.17-1"
      refute host.in_maintenance?
      refute host.stale?
      assert snapshot.desired == "started"
      assert snapshot.source == :zookeeper
      assert snapshot.configured?
    end

    test "a configured node with no registration is down, not unknown" do
      snapshot =
        Status.fetch(
          nodes: %{"10.10.0.11" => node_doc()},
          node_ips: ["10.10.0.11", "10.10.0.12"]
        )

      assert [up, down] = snapshot.nodes
      assert up.state == :up
      assert down.ip == "10.10.0.12"
      assert down.state == :down
      refute down.registered?
      assert down.services == []
      assert snapshot.summary.nodes_up == 1
      assert snapshot.summary.nodes_down == 1
    end

    test "FLAPPING is counted apart from UP and from DOWN" do
      snapshot = Status.fetch(nodes: %{"10.10.0.11" => node_doc()})
      [host] = snapshot.nodes

      assert host.counts == %{up: 2, down: 1, flapping: 1, total: 4}
      assert snapshot.summary.services_up == 2
      assert snapshot.summary.services_down == 1
      assert snapshot.summary.services_flapping == 1

      flapping = Enum.find(host.services, &(&1.name == "Vali"))
      assert flapping.status == "FLAPPING"
      assert flapping.restarts == 7
      assert flapping.pids == []
    end

    test "an unrecognised status is never counted as up" do
      doc = node_doc(%{"services" => %{"Weird" => %{"status" => "whatever"}}})
      snapshot = Status.fetch(nodes: %{"10.10.0.11" => doc})
      [host] = snapshot.nodes

      assert host.counts.up == 0
      assert host.counts.down == 1
    end

    test "a document older than the threshold is stale" do
      old = System.system_time(:second) - (Status.stale_after_seconds() + 15)
      snapshot = Status.fetch(nodes: %{"10.10.0.11" => node_doc(%{"ts" => old})})

      assert [host] = snapshot.nodes
      assert host.stale?
      assert host.age_seconds >= Status.stale_after_seconds()
      assert host.state == :up, "staleness must not be confused with absence"
      assert snapshot.summary.stale_nodes == 1
    end

    test "maintenance state is surfaced" do
      doc = node_doc(%{"maintenance_status" => "IN_MAINTENANCE"})
      assert [host] = Status.fetch(nodes: %{"10.10.0.11" => doc}).nodes
      assert host.in_maintenance?
      assert host.maintenance == "IN_MAINTENANCE"
    end

    test "tolerates a document missing every optional field" do
      assert [host] = Status.fetch(nodes: %{"10.10.0.11" => %{}}).nodes

      assert host.state == :up
      assert host.services == []
      assert host.disks == nil
      assert host.build == nil
      assert host.age_seconds == nil
      refute host.stale?
      refute host.zk_leader?
    end

    test "pids arriving as strings survive" do
      doc = node_doc(%{"services" => %{"Slate" => %{"status" => "UP", "pids" => ["4711"]}}})
      assert [host] = Status.fetch(nodes: %{"10.10.0.11" => doc}).nodes
      assert [%{pids: ["4711"]}] = host.services
    end
  end

  describe "source selection" do
    test "ZooKeeper is preferred and reported as the source" do
      use_zk({:ok, %{nodes: %{"10.10.0.11" => node_doc()}, desired: "started"}})

      snapshot = Status.fetch()

      assert snapshot.source == :zookeeper
      assert snapshot.desired == "started"
      assert snapshot.error == nil
      assert [%{ip: "10.10.0.11", state: :up}] = snapshot.nodes
    end

    test "falls back when ZooKeeper errors, recording why" do
      use_zk({:error, :econnrefused})

      snapshot = Status.fetch()

      # No cluster.json on a dev machine, so the fallback has nothing to probe.
      assert snapshot.source == :unconfigured
      refute snapshot.configured?
      assert snapshot.error =~ "econnrefused"
      assert snapshot.nodes == []
    end

    test "a raising reader is contained rather than crashing the dashboard" do
      use_zk(fn -> raise "zookeeper session expired" end)

      snapshot = Status.fetch()

      assert snapshot.source == :unconfigured
      assert snapshot.error =~ "zookeeper session expired"
    end

    test "an exiting reader is contained too" do
      use_zk(fn -> exit(:timeout) end)
      assert Status.fetch().error =~ "timeout"
    end

    test "a reader returning nonsense is treated as a failure" do
      use_zk(:definitely_not_a_result)
      assert Status.fetch().error =~ "unexpected_reply"
    end

    test "a module without the callback is skipped" do
      Application.put_env(:spectrum_phx, :zk_state_module, ZkMissingCallback)
      assert Status.fetch().error =~ "zookeeper_reader_unavailable"
    end

    test "a module that does not exist at all is skipped" do
      Application.put_env(:spectrum_phx, :zk_state_module, SpectrumPhx.Definitely.Not.Loaded)
      assert Status.fetch().error =~ "zookeeper_reader_unavailable"
    end

    test "ZooKeeper-only nodes are shown even when absent from cluster.json" do
      use_zk({:ok, %{nodes: %{"10.10.0.99" => node_doc(%{"ip" => "10.10.0.99"})}, desired: nil}})

      snapshot = Status.fetch()

      assert [%{ip: "10.10.0.99"}] = snapshot.nodes
      assert snapshot.desired == nil
    end
  end

  describe "summary/1" do
    test "aggregates across nodes and carries desired state and source" do
      summary =
        Status.summary(
          nodes: %{"10.10.0.11" => node_doc(), "10.10.0.12" => node_doc(%{"ip" => "10.10.0.12"})},
          node_ips: ["10.10.0.11", "10.10.0.12", "10.10.0.13"],
          desired: "started",
          source: :probe
        )

      assert summary.total_nodes == 3
      assert summary.nodes_up == 2
      assert summary.nodes_down == 1
      assert summary.services_up == 4
      assert summary.services_down == 2
      assert summary.services_flapping == 2
      assert summary.desired == "started"
      assert summary.source == :probe
    end

    test "an empty cluster summarises to zeroes rather than crashing" do
      summary = Status.summary(nodes: %{})

      assert summary.total_nodes == 0
      assert summary.nodes_up == 0
      assert summary.services_up == 0
      assert summary.source == :unconfigured
    end
  end

  describe "application-environment override" do
    test "is used when no explicit nodes are passed" do
      Application.put_env(:spectrum_phx, :cluster_status_override,
        nodes: %{"10.10.0.11" => node_doc()},
        desired: "stopped",
        node_ips: ["10.10.0.11", "10.10.0.12"]
      )

      snapshot = Status.fetch()

      assert snapshot.desired == "stopped"
      assert length(snapshot.nodes) == 2
    end

    test "explicit options win over the override" do
      Application.put_env(:spectrum_phx, :cluster_status_override, nodes: %{}, desired: "stopped")

      snapshot = Status.fetch(nodes: %{"10.10.0.11" => node_doc()}, desired: "started")

      assert snapshot.desired == "started"
      assert length(snapshot.nodes) == 1
    end
  end

  describe "node/2" do
    test "returns a single node view or nil" do
      opts = [nodes: %{"10.10.0.11" => node_doc()}, node_ips: ["10.10.0.11", "10.10.0.12"]]

      assert %{ip: "10.10.0.11", state: :up} = Status.node("10.10.0.11", opts)
      assert %{ip: "10.10.0.12", state: :down} = Status.node("10.10.0.12", opts)
      assert Status.node("10.10.0.77", opts) == nil
    end
  end
end
