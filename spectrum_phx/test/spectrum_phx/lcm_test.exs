defmodule SpectrumPhx.LcmTest do
  @moduledoc """
  Life-cycle management, and mostly the progress bar.

  A rolling upgrade's bar is the only thing an operator has to tell "this is working" from
  "this is stuck", so the properties that matter are the ones that make it trustworthy: it
  never goes backwards, never runs past the node it is on, and does not invent a position
  when it does not have one.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Lcm

  @nodes ["10.0.0.1", "10.0.0.2", "10.0.0.3"]

  defp log(line), do: %{at: nil, line: line}

  describe "progress" do
    test "a finished upgrade is complete however far the nodes got" do
      assert Lcm.progress("COMPLETED", @nodes, "10.0.0.1", []) == 100
    end

    test "a failed upgrade stops the bar rather than leaving it mid-way" do
      # It is not going to move again, and a bar frozen at 33% reads as "still working".
      assert Lcm.progress("FAILED", @nodes, "10.0.0.2", []) == 100
    end

    test "counts the nodes already finished" do
      assert Lcm.progress("UPGRADING", @nodes, "10.0.0.1", []) == 0
      assert Lcm.progress("UPGRADING", @nodes, "10.0.0.2", []) == 33
      assert Lcm.progress("UPGRADING", @nodes, "10.0.0.3", []) == 67
    end

    test "the phase within a node only moves that node's share of the bar" do
      base = Lcm.progress("UPGRADING", @nodes, "10.0.0.2", [])
      restoring = Lcm.progress("UPGRADING", @nodes, "10.0.0.2", [log("10.0.0.2 restore complete")])

      assert restoring > base
      # One node owns a third of the bar, so the guess can never reach the next node's
      # ground: 33 + 33 = 66 is the ceiling, and the next node starts at 67.
      assert restoring <= 67
    end

    test "the furthest phase seen wins, so the bar cannot go backwards" do
      # Logs arrive in order but are read as a set; taking the last match would let an
      # earlier line re-appear and pull the bar back.
      logs = [
        log("10.0.0.2 entering maintenance"),
        log("10.0.0.2 deploying"),
        log("10.0.0.2 reboot requested"),
        log("10.0.0.2 entering maintenance again")
      ]

      assert Lcm.progress("UPGRADING", @nodes, "10.0.0.2", logs) >=
               Lcm.progress("UPGRADING", @nodes, "10.0.0.2", [log("10.0.0.2 reboot requested")])
    end

    test "another node's log lines do not move this node's phase" do
      logs = [log("10.0.0.3 restore complete")]
      assert Lcm.progress("UPGRADING", @nodes, "10.0.0.2", logs) == 33
    end

    test "a current node that is not in the target list adds nothing" do
      # "We do not know where it is" and "it has just begun" are different states, and
      # only one of them should draw a bar that is about to move.
      assert Lcm.progress("UPGRADING", @nodes, "10.0.0.9", [log("10.0.0.9 reboot")]) == 0
    end

    test "an upgrade with no targets does not divide by zero" do
      assert Lcm.progress("UPGRADING", [], nil, []) == 0
    end

    test "an idle cluster is at zero" do
      assert Lcm.progress("IDLE", @nodes, nil, []) == 0
    end
  end

  describe "finished nodes" do
    test "are the ones before the node in flight" do
      assert Lcm.finished_nodes(@nodes, "10.0.0.3", "UPGRADING") == ["10.0.0.1", "10.0.0.2"]
    end

    test "are all of them once it completes" do
      assert Lcm.finished_nodes(@nodes, "10.0.0.1", "COMPLETED") == @nodes
    end

    test "are none when the current node is unknown" do
      assert Lcm.finished_nodes(@nodes, "10.0.0.9", "UPGRADING") == []
    end
  end

  describe "running?" do
    test "is true only while something is expected to move" do
      assert Lcm.running?(%{state: "UPGRADING"})
      assert Lcm.running?(%{state: "DOWNLOADING"})
      refute Lcm.running?(%{state: "COMPLETED"})
      refute Lcm.running?(%{state: "FAILED"})
      refute Lcm.running?(nil)
    end
  end

  describe "inventory" do
    defp overview(static), do: Lcm.overview(source: {:static, static})

    # The real row, taken off the cluster: one `latest` row holding hostname => node.
    defp inventory_row(nodes, extra \\ %{}) do
      blob =
        nodes
        |> Map.new(fn {host, versions} -> {host, %{"ip" => "10.0.0.1", "versions" => versions}} end)
        |> Map.merge(extra)

      [%{"key" => "latest", "inventory_json" => Jason.encode!(blob)}]
    end

    test "reads the shape the cluster actually writes: hostname to versions" do
      rows =
        inventory_row(%{
          "Valkyrie-997A49" => %{"sidon" => "1.2.0", "vali" => "1.2.2"},
          "Valkyrie-3CA91E" => %{"sidon" => "1.2.0", "vali" => "1.2.2"}
        })

      result = overview(%{inventory: rows})

      assert Enum.map(result.inventory.components, & &1.name) == ["sidon", "vali"]
      assert result.inventory.nodes == ["Valkyrie-3CA91E", "Valkyrie-997A49"]
    end

    test "a key beside the hostnames is not read as a machine" do
      # The real blob carries a `level` key. Treating it as a node put "level" in the
      # component table.
      rows =
        inventory_row(%{"Valkyrie-997A49" => %{"sidon" => "1.2.0"}}, %{"level" => "stable"})

      result = overview(%{inventory: rows})

      assert Enum.map(result.inventory.components, & &1.name) == ["sidon"]
      assert result.inventory.nodes == ["Valkyrie-997A49"]
    end

    test "nodes running different versions of a component are reported as disagreeing" do
      # A rolling upgrade that stopped part way looks exactly like this, and one version
      # number per component hides it.
      rows =
        inventory_row(%{
          "node-a" => %{"sidon" => "1.2.0", "vali" => "1.2.2"},
          "node-b" => %{"sidon" => "1.3.0", "vali" => "1.2.2"}
        })

      result = overview(%{inventory: rows})
      by_name = Map.new(result.inventory.components, &{&1.name, &1})

      refute by_name["sidon"].consistent?
      assert by_name["sidon"].by_node == %{"node-a" => "1.2.0", "node-b" => "1.3.0"}
      assert by_name["vali"].consistent?
      assert by_name["vali"].version == "1.2.2"
      assert result.inventory.disagreements == 1
    end

    test "disagreements sort to the top, where they will be seen" do
      rows =
        inventory_row(%{
          "node-a" => %{"aaa" => "1.0", "zzz" => "1.0"},
          "node-b" => %{"aaa" => "1.0", "zzz" => "2.0"}
        })

      assert [%{name: "zzz"} | _] = overview(%{inventory: rows}).inventory.components
    end

    test "a lone ip-and-versions object is read too" do
      rows = [
        %{
          "key" => "10.10.102.43",
          "inventory_json" => ~s({"ip":"10.10.102.43","versions":{"sidon":"1.2.0"}})
        }
      ]

      result = overview(%{inventory: rows})

      assert [%{name: "sidon", by_node: %{"10.10.102.43" => "1.2.0"}}] =
               result.inventory.components
    end

    test "a bare name-to-version map is read too" do
      rows = [%{"key" => "cluster", "inventory_json" => ~s({"sidon":"1.2.0","vali":"0.9"})}]
      result = overview(%{inventory: rows})

      assert Enum.map(result.inventory.components, & &1.name) == ["sidon", "vali"]
    end

    test "a version that is not a scalar loses that component, not the whole list" do
      # Rendering one raised Protocol.UndefinedError and took the page down with a 500.
      rows = inventory_row(%{"node-a" => %{"sidon" => "1.2.0", "weird" => %{"nested" => true}}})

      result = overview(%{inventory: rows})
      by_name = Map.new(result.inventory.components, &{&1.name, &1})

      assert by_name["sidon"].readable?
      refute by_name["weird"].readable?
    end

    test "a blob that will not parse is reported, not dropped" do
      rows = [%{"key" => "cluster", "inventory_json" => "{not json"}]
      result = overview(%{inventory: rows})

      assert [%{readable?: false}] = result.inventory.components
    end

    test "a table that will not read says so rather than reporting nothing installed" do
      result = overview(%{inventory: {:error, :timeout}})

      refute result.inventory.available?
      assert result.inventory.components == []
    end
  end

  describe "available update" do
    test "reads the feed's last answer" do
      rows = [
        %{
          "latest_version" => "2026.09.1",
          "current_version" => "2026.08.17",
          "update_available" => true,
          "changelog" => "* things",
          "size" => 1024
        }
      ]

      result = overview(%{update: rows})

      assert result.update.update_available?
      assert result.update.latest_version == "2026.09.1"
      assert result.update.size_bytes == 1024
    end

    test "no row at all is up to date, not an error" do
      result = overview(%{update: []})

      assert result.update.available?
      refute result.update.update_available?
    end
  end

  describe "statements" do
    test "every read names its columns" do
      for {_name, cql} <- Lcm.statements(), do: refute(cql =~ "SELECT *")
    end
  end
end
