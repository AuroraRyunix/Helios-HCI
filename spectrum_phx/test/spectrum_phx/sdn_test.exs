defmodule SpectrumPhx.SdnTest do
  @moduledoc """
  Assembling Urbosa's five flat tables into the tree they describe.

  The joins are by uuid and nothing enforces them, so the cases that matter are the ones
  where a reference points at nothing: a segment whose T1 was deleted is an overlay
  network with no route off itself, which is exactly what an operator staring at an
  unreachable guest needs to see.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Sdn

  @t0 "11111111-1111-1111-1111-111111111111"
  @t1 "22222222-2222-2222-2222-222222222222"
  @t1_other "33333333-3333-3333-3333-333333333333"
  @segment "44444444-4444-4444-4444-444444444444"

  defp t0(id \\ @t0, name \\ "edge-t0") do
    %{
      "router_id" => id,
      "name" => name,
      "uplink_interface" => "eno1",
      "uplink_ip" => "10.10.102.1/24",
      "gateway_ip" => "10.10.102.254",
      "nat_rules" => nil
    }
  end

  defp t1(id \\ @t1, opts \\ []) do
    %{
      "router_id" => id,
      "name" => Keyword.get(opts, :name, "tenant-t1"),
      "t0_link_id" => Keyword.get(opts, :t0, @t0),
      "dhcp_enabled" => Keyword.get(opts, :dhcp, true)
    }
  end

  defp segment(id \\ @segment, opts \\ []) do
    %{
      "segment_id" => id,
      "name" => Keyword.get(opts, :name, "app-net"),
      "vni" => Keyword.get(opts, :vni, 5001),
      "t1_link_id" => Keyword.get(opts, :t1, @t1),
      "subnet_cidr" => "192.168.10.0/24",
      "gateway_ip" => "192.168.10.1",
      "dhcp_enabled" => true,
      "dhcp_start" => "192.168.10.100",
      "dhcp_end" => "192.168.10.200"
    }
  end

  defp fabric(static) do
    Sdn.fabric(source: {:static, Map.put_new(static, :nodes, ["10.10.0.11"])})
  end

  describe "the tree" do
    test "hangs segments off their T1 and T1s off their T0" do
      result = fabric(%{t0: [t0()], t1: [t1()], segments: [segment()]})

      assert result.available?
      assert [tier0] = result.tier0
      assert tier0.name == "edge-t0"
      assert [router] = tier0.tier1
      assert router.name == "tenant-t1"
      assert [attached] = router.segments
      assert attached.name == "app-net"
      assert attached.vni == 5001
    end

    test "counts the segments beneath a T0 through its routers" do
      result =
        fabric(%{
          t0: [t0()],
          t1: [t1(), t1(@t1_other, name: "other-t1")],
          segments: [segment(), segment("55555555-5555-5555-5555-555555555555", t1: @t1_other)]
        })

      assert [tier0] = result.tier0
      assert tier0.segment_count == 2
    end

    test "guests are attached to the segment their network_id names" do
      vms = [
        %{"name" => "web-01", "network_id" => @segment, "state" => "Running", "host_ip" => "10.10.0.11"},
        %{"name" => "db-01", "network_id" => @segment, "state" => "Stopped", "host_ip" => nil}
      ]

      result = fabric(%{t0: [t0()], t1: [t1()], segments: [segment()], vms: vms})
      [attached] = hd(result.tier0).tier1 |> hd() |> Map.fetch!(:segments)

      assert length(attached.guests) == 2
      assert Enum.map(attached.guests, & &1.name) |> Enum.sort() == ["db-01", "web-01"]
      assert result.summary.guests_attached == 2
    end

    test "a guest on no network is not attached to anything" do
      vms = [%{"name" => "isolated", "network_id" => nil, "state" => "Running"}]
      result = fabric(%{t0: [t0()], t1: [t1()], segments: [segment()], vms: vms})

      assert result.summary.guests_attached == 0
    end
  end

  describe "references that point at nothing" do
    test "a segment whose T1 is gone is kept as an orphan, not dropped" do
      # It is an overlay network with no route off itself. Dropping it is how an operator
      # ends up staring at a guest that cannot reach anything, with nothing on the page
      # to explain why.
      result = fabric(%{t0: [t0()], t1: [], segments: [segment()]})

      assert [orphan] = result.orphans.segments
      assert orphan.name == "app-net"
      assert result.summary.segments == 1
    end

    test "a T1 whose T0 is gone is kept as an orphan" do
      result = fabric(%{t0: [], t1: [t1()], segments: []})

      assert [orphan] = result.orphans.tier1
      assert orphan.name == "tenant-t1"
      assert result.tier0 == []
    end

    test "an attached segment is not also reported as an orphan" do
      result = fabric(%{t0: [t0()], t1: [t1()], segments: [segment()]})

      assert result.orphans.segments == []
      assert result.orphans.tier1 == []
    end
  end

  describe "firewall" do
    defp rule(opts) do
      %{
        "rule_id" => Keyword.get(opts, :id, "66666666-6666-6666-6666-666666666666"),
        "description" => Keyword.get(opts, :description, "allow web"),
        "source_ip" => Keyword.get(opts, :source),
        "dest_ip" => Keyword.get(opts, :dest),
        "protocol" => Keyword.get(opts, :protocol, "tcp"),
        "port" => Keyword.get(opts, :port, 443),
        "action" => Keyword.get(opts, :action, "ALLOW"),
        "priority" => Keyword.get(opts, :priority, 100)
      }
    end

    test "rules come back in priority order" do
      result =
        fabric(%{
          firewall: [
            rule(priority: 200, description: "second"),
            rule(priority: 100, description: "first")
          ]
        })

      assert Enum.map(result.firewall, & &1.description) == ["first", "second"]
    end

    test "an absent source or destination reads as any" do
      result = fabric(%{firewall: [rule(source: nil, dest: nil)]})

      assert [%{source: "any", destination: "any"}] = result.firewall
    end

    test "an action that cannot be read is unknown, never allow" do
      # A firewall table that guesses permissive when it cannot read a row is worse than
      # one that admits it does not know.
      result = fabric(%{firewall: [rule(action: "sudo make me a sandwich"), rule(action: nil)]})

      assert Enum.map(result.firewall, & &1.action) == [:unknown, :unknown]
    end

    test "the spellings Urbosa actually writes are recognised" do
      rules = for verb <- ~w(allow ACCEPT deny DROP reject), do: rule(action: verb)
      result = fabric(%{firewall: rules})

      assert Enum.map(result.firewall, & &1.action) == [:allow, :allow, :deny, :deny, :deny]
    end
  end

  describe "when the database will not answer" do
    test "the page is told, and shown nothing rather than an empty fabric" do
      result = fabric(%{t0: {:error, "connection refused"}})

      refute result.available?
      assert result.error =~ "connection refused"
      assert result.tier0 == []
      assert result.summary.segments == 0
    end

    test "the node list survives, because it does not come from the database" do
      result = fabric(%{t0: {:error, :timeout}, nodes: ["10.10.0.11", "10.10.0.12"]})

      assert length(result.nodes) == 2
    end
  end

  describe "statements" do
    test "every read names its columns rather than selecting everything" do
      for {_name, cql} <- Sdn.statements() do
        refute cql =~ "SELECT *", "#{cql} selects every column"
        assert cql =~ "FROM hydra.urbosa_"
      end
    end
  end

  describe "mutations and VNI claims" do
    test "creates and deletes T0 and T1 routers" do
      sink = self()
      opts = [source: {:static, %{sink: sink}}]

      assert {:ok, t0_id} =
               Sdn.create_t0_router(
                 %{"name" => "edge-01", "uplink_interface" => "eth1", "uplink_ip" => "192.0.2.2/24"},
                 opts
               )

      assert_receive {:write, cql, ["edge-01", "eth1", "192.0.2.2/24", "", "[]"]}
      assert cql =~ "INSERT INTO hydra.urbosa_t0_routers"

      assert {:ok, t1_id} =
               Sdn.create_t1_router(
                 %{"name" => "tenant-01", "t0_link_id" => t0_id, "dhcp_enabled" => "true"},
                 opts
               )

      assert_receive {:write, t1_cql, ["tenant-01", true]}
      assert t1_cql =~ "INSERT INTO hydra.urbosa_t1_routers"

      assert :ok = Sdn.delete_t1_router(t1_id, opts)
      assert :ok = Sdn.delete_t0_router(t0_id, opts)
    end

    test "claims VNI before creating segment and refuses duplicate VNI or attached guest deletion" do
      sink = self()
      seg_id = Sdn.generate_id()

      opts = [
        source:
          {:static,
           %{
             sink: sink,
             vni_claims: [%{"vni" => 5000, "segment_id" => seg_id, "name" => "existing-seg"}],
             segments: [%{"segment_id" => seg_id, "name" => "existing-seg", "vni" => 5000}],
             vms: [%{"name" => "app-vm-1", "network_id" => seg_id, "state" => "RUNNING"}]
           }}
      ]

      # Duplicate VNI is refused
      assert {:error, msg} =
               Sdn.create_segment(
                 %{"name" => "clash-seg", "vni" => "5000", "subnet_cidr" => "10.50.0.0/24"},
                 opts
               )

      assert msg =~ "VNI 5000 is already claimed by existing-seg"

      # Free VNI succeeds and claims VNI first
      assert {:ok, new_seg_id} =
               Sdn.create_segment(
                 %{"name" => "prod-seg", "vni" => "5001", "subnet_cidr" => "10.51.0.0/24"},
                 opts
               )

      assert_receive {:vni_claim, 5001, ^new_seg_id}
      assert_receive {:write, seg_cql, ["prod-seg", 5001, "10.51.0.0/24", "", false, "", ""]}
      assert seg_cql =~ "INSERT INTO hydra.urbosa_segments"

      # Deleting segment with attached guests is refused
      assert {:error, del_err} = Sdn.delete_segment(seg_id, opts)
      assert del_err =~ "app-vm-1"

      # Deleting segment with no attached guests succeeds and releases VNI
      empty_opts = [
        source:
          {:static,
           %{
             sink: sink,
             segments: [%{"segment_id" => seg_id, "name" => "existing-seg", "vni" => 5000}],
             vms: []
           }}
      ]

      assert :ok = Sdn.delete_segment(seg_id, empty_opts)
      assert_receive {:vni_release, 5000, ^seg_id}
    end

    test "creates and deletes distributed firewall rules" do
      sink = self()
      opts = [source: {:static, %{sink: sink}}]

      assert {:ok, rule_id} =
               Sdn.create_firewall_rule(
                 %{
                   "description" => "Allow HTTPS",
                   "source_ip" => "any",
                   "dest_ip" => "10.10.0.0/16",
                   "protocol" => "tcp",
                   "port" => "443",
                   "action" => "allow",
                   "priority" => "50"
                 },
                 opts
               )

      assert_receive {:write, fw_cql, ["Allow HTTPS", "any", "10.10.0.0/16", "tcp", 443, "allow", 50]}
      assert fw_cql =~ "INSERT INTO hydra.urbosa_firewall_rules"

      assert :ok = Sdn.delete_firewall_rule(rule_id, opts)
    end

    test "configures BGP EVPN and manages BGP neighbors" do
      sink = self()
      opts = [source: {:static, %{sink: sink, bgp_config: [], bgp_peers: []}}]

      assert {:ok, %{available?: true, config: cfg, peers: []}} = Sdn.bgp_state(opts)
      assert cfg.asn == 65000
      assert cfg.bfd_enabled? == true

      assert :ok =
               Sdn.configure_bgp(
                 %{
                   "asn" => "65100",
                   "router_id" => "10.10.0.11",
                   "evpn_enabled" => "true",
                   "arp_suppression" => "true",
                   "disable_headend_flood" => "false",
                   "route_reflectors" => "10.10.0.11",
                   "bfd_enabled" => "true"
                 },
                 opts
               )

      assert_receive {:write, bgp_cql, ["cluster", 65100, "10.10.0.11", true, true, false, "10.10.0.11", true, _now]}
      assert bgp_cql =~ "INSERT INTO hydra.urbosa_bgp_config"

      assert {:ok, "10.10.0.12"} =
               Sdn.add_bgp_peer(
                 %{
                   "peer_ip" => "10.10.0.12",
                   "remote_asn" => "65100",
                   "description" => "ToR-Leaf-1",
                   "bfd_enabled" => "true",
                   "route_reflector_client" => "true"
                 },
                 opts
               )

      assert_receive {:write, peer_cql, ["10.10.0.12", 65100, "ToR-Leaf-1", true, true, _peer_now]}
      assert peer_cql =~ "INSERT INTO hydra.urbosa_bgp_peers"

      assert :ok = Sdn.delete_bgp_peer("10.10.0.12", opts)
      assert_receive {:write, del_cql, ["10.10.0.12"]}
      assert del_cql =~ "DELETE FROM hydra.urbosa_bgp_peers"
    end
  end
end
