defmodule SpectrumPhxWeb.Sdn.TopologyTest do
  @moduledoc """
  The topology's geometry.

  The page this replaces drew a fixed diagram: the boxes were in the markup and the data
  was written into them, so it could only show the shape somebody anticipated. These
  assert the opposite property -- that the picture is a function of the fabric, including
  for the shapes nobody anticipated.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Sdn
  alias SpectrumPhxWeb.Sdn.Topology

  @t0 "11111111-1111-1111-1111-111111111111"
  @t0_b "aaaaaaaa-1111-1111-1111-111111111111"
  @t1 "22222222-2222-2222-2222-222222222222"
  @segment "44444444-4444-4444-4444-444444444444"

  defp fabric(static) do
    Sdn.fabric(source: {:static, Map.put_new(static, :nodes, [])})
  end

  defp t0(id \\ @t0, name \\ "edge-t0") do
    %{"router_id" => id, "name" => name, "uplink_interface" => "eno1", "uplink_ip" => "10.0.0.1/24"}
  end

  defp t1(id \\ @t1, opts \\ []) do
    %{
      "router_id" => id,
      "name" => Keyword.get(opts, :name, "tenant-t1"),
      "t0_link_id" => Keyword.get(opts, :t0, @t0)
    }
  end

  defp segment(id \\ @segment, opts \\ []) do
    %{
      "segment_id" => id,
      "name" => Keyword.get(opts, :name, "app-net"),
      "vni" => Keyword.get(opts, :vni, 5001),
      "t1_link_id" => Keyword.get(opts, :t1, @t1),
      "subnet_cidr" => "192.168.10.0/24"
    }
  end

  defp band(layout, kind), do: Enum.filter(layout.nodes, &(&1.band == kind))

  describe "bands" do
    test "each tier sits in its own row, top to bottom" do
      layout = Topology.layout(fabric(%{t0: [t0()], t1: [t1()], segments: [segment()]}))

      [tier0] = band(layout, :t0)
      [tier1] = band(layout, :t1)
      [seg] = band(layout, :segment)

      assert tier0.y < tier1.y
      assert tier1.y < seg.y
    end

    test "siblings are laid out left to right without overlapping" do
      layout =
        Topology.layout(
          fabric(%{
            t0: [t0()],
            t1: [t1()],
            segments: [segment(), segment("55555555-5555-5555-5555-555555555555", name: "b")]
          })
        )

      [first, second] = band(layout, :segment) |> Enum.sort_by(& &1.x)
      assert first.x + first.w < second.x
      assert first.y == second.y
    end

    test "the diagram is as wide as its widest band" do
      narrow = Topology.layout(fabric(%{t0: [t0()], t1: [t1()], segments: [segment()]}))

      wide =
        Topology.layout(
          fabric(%{
            t0: [t0()],
            t1: [t1()],
            segments:
              for index <- 1..6 do
                segment("6666666#{index}-6666-6666-6666-666666666666", name: "net-#{index}")
              end
          })
        )

      assert wide.width > narrow.width
    end
  end

  describe "edges" do
    test "every box with a parent is joined to it" do
      layout = Topology.layout(fabric(%{t0: [t0()], t1: [t1()], segments: [segment()]}))

      # T1 to its T0, and the segment to its T1.
      assert length(layout.edges) == 2
      assert Enum.all?(layout.edges, &(&1.kind == :elbow))
    end

    test "an elbow leaves the parent's underside and enters the child's top" do
      layout = Topology.layout(fabric(%{t0: [t0()], t1: [t1()]}))
      [tier0] = band(layout, :t0)
      [tier1] = band(layout, :t1)
      [edge] = layout.edges

      assert edge.d =~ "M #{tier0.x + div(tier0.w, 2)} #{tier0.y + tier0.h}"
      assert String.ends_with?(edge.d, "V #{tier1.y}")
    end

    test "two T0s each keep their own children" do
      layout =
        Topology.layout(
          fabric(%{
            t0: [t0(), t0(@t0_b, "second-t0")],
            t1: [t1(), t1("77777777-7777-7777-7777-777777777777", name: "b-t1", t0: @t0_b)]
          })
        )

      assert length(band(layout, :t0)) == 2
      assert length(band(layout, :t1)) == 2
      assert length(layout.edges) == 2
    end
  end

  describe "orphans" do
    test "a segment with no router is drawn, in its own band, with no edge to it" do
      layout = Topology.layout(fabric(%{t0: [t0()], t1: [], segments: [segment()]}))

      assert [orphan] = band(layout, :orphan)
      assert orphan.detail == "segment with no router"
      assert orphan.parent == nil
      assert layout.edges == []
    end

    test "the orphan band is below the others" do
      layout =
        Topology.layout(
          fabric(%{
            t0: [t0()],
            t1: [t1()],
            segments: [segment(), segment("88888888-8888-8888-8888-888888888888", t1: "gone")]
          })
        )

      [orphan] = band(layout, :orphan)
      [attached] = band(layout, :segment)
      assert orphan.y > attached.y
    end

    test "a fabric with no orphans is shorter than one with them" do
      clean = Topology.layout(fabric(%{t0: [t0()], t1: [t1()], segments: [segment()]}))
      broken = Topology.layout(fabric(%{t0: [t0()], t1: [], segments: [segment()]}))

      assert broken.height > clean.height
    end
  end

  describe "guests" do
    test "a segment carries its guests as dots" do
      vms =
        for index <- 1..3 do
          %{"name" => "vm-#{index}", "network_id" => @segment, "state" => "Running"}
        end

      layout = Topology.layout(fabric(%{t0: [t0()], t1: [t1()], segments: [segment()], vms: vms}))

      assert [%{guests: 3}] = band(layout, :segment)
    end
  end

  describe "an empty fabric" do
    test "lays out without raising and still has a drawable box" do
      layout = Topology.layout(fabric(%{}))

      assert layout.nodes == []
      assert layout.edges == []
      assert layout.width >= 480
      assert layout.height > 0
    end

    test "a database that will not answer draws nothing rather than crashing" do
      layout = Topology.layout(fabric(%{t0: {:error, :timeout}}))

      assert layout.nodes == []
    end
  end
end
