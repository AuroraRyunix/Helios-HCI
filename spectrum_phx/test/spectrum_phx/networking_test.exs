defmodule SpectrumPhx.NetworkingTest do
  @moduledoc """
  The layer-2 networks a guest can be attached to.

  Two kinds share one list because a guest does not care how its network is built, and
  the tests that matter are the ones about the seam between them: an overlay segment must
  not be removable from here, and a VLAN must not collide with one already in use.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Networking

  defp network(opts \\ []) do
    %{
      "net_id" => Keyword.get(opts, :id, "11111111-1111-1111-1111-111111111111"),
      "name" => Keyword.get(opts, :name, "production"),
      "type" => Keyword.get(opts, :type, "direct"),
      "vlan_id" => Keyword.get(opts, :vlan)
    }
  end

  defp segment(opts \\ []) do
    %{
      "segment_id" => Keyword.get(opts, :id, "22222222-2222-2222-2222-222222222222"),
      "name" => Keyword.get(opts, :name, "app-net"),
      "vni" => Keyword.get(opts, :vni, 5001),
      "subnet_cidr" => "192.168.10.0/24",
      "gateway_ip" => "192.168.10.1"
    }
  end

  defp overview(static), do: Networking.overview(source: {:static, static})

  describe "the list" do
    test "spans Gatoway networks and Urbosa segments" do
      result = overview(%{networks: [network(name: "prod")], segments: [segment(name: "app")]})

      assert Enum.map(result.networks, & &1.name) == ["prod", "app"]
      assert result.summary.total == 2
    end

    test "keeps the kinds distinguishable, because what you can do with them differs" do
      result =
        overview(%{
          networks: [network(name: "flat"), network(id: "3", name: "tagged", type: "vlan", vlan: 100)],
          segments: [segment()]
        })

      kinds = Map.new(result.networks, &{&1.name, &1.kind})
      assert kinds == %{"flat" => :direct, "tagged" => :vlan, "app-net" => :overlay}
    end

    test "an overlay segment is not removable from this page" do
      # Deleting one also has to account for the tunnels carrying it, which is the SDN
      # page's business.
      result = overview(%{segments: [segment()]})
      assert [%{removable?: false}] = result.networks
    end

    test "a Gatoway network is removable" do
      result = overview(%{networks: [network()]})
      assert [%{removable?: true}] = result.networks
    end

    test "the summary lists the VLANs already taken" do
      result =
        overview(%{
          networks: [
            network(id: "1", type: "vlan", vlan: 300),
            network(id: "2", type: "vlan", vlan: 100),
            network(id: "3", type: "direct")
          ]
        })

      assert result.summary.vlans_used == [100, 300]
      assert result.summary.vlan == 2
      assert result.summary.direct == 1
    end

    test "a database that will not answer says so rather than reporting no networks" do
      result = overview(%{networks: {:error, "connection refused"}})

      refute result.available?
      assert result.error =~ "connection refused"
      assert result.networks == []
    end
  end

  describe "creating" do
    defp create(params, static \\ %{}) do
      Networking.create_network(params, source: {:static, static})
    end

    test "a direct network needs only a name" do
      assert {:ok, "production"} = create(%{"name" => "production", "type" => "direct"})
    end

    test "a name is required" do
      assert {:error, message} = create(%{"name" => "  ", "type" => "direct"})
      assert message =~ "name is required"
    end

    test "a name is restricted to what a bridge can be called after it" do
      assert {:error, _} = create(%{"name" => "rm -rf /", "type" => "direct"})
      assert {:error, _} = create(%{"name" => "a'; DROP TABLE--", "type" => "direct"})
      assert {:ok, _} = create(%{"name" => "prod-net_1.0", "type" => "direct"})
    end

    test "an unknown type is refused rather than defaulted" do
      assert {:error, message} = create(%{"name" => "x", "type" => "overlay"})
      assert message =~ "direct or vlan"
    end

    test "a tagged network needs a VLAN in range" do
      assert {:error, _} = create(%{"name" => "x", "type" => "vlan", "vlan_id" => "0"})
      assert {:error, _} = create(%{"name" => "x", "type" => "vlan", "vlan_id" => "4095"})
      assert {:error, _} = create(%{"name" => "x", "type" => "vlan", "vlan_id" => "abc"})
      assert {:ok, _} = create(%{"name" => "x", "type" => "vlan", "vlan_id" => "4094"})
    end

    test "a VLAN already in use is refused, and the clash is named" do
      static = %{networks: [network(name: "existing", type: "vlan", vlan: 100)]}

      assert {:error, message} = create(%{"name" => "new", "type" => "vlan", "vlan_id" => "100"}, static)
      assert message =~ "already used by existing"
    end

    test "a VLAN colliding with an overlay's VNI is allowed: they are different spaces" do
      static = %{segments: [segment(vni: 100)]}
      assert {:ok, _} = create(%{"name" => "new", "type" => "vlan", "vlan_id" => "100"}, static)
    end

    test "a duplicate cannot be ruled out when the table cannot be read, so it is refused" do
      static = %{networks: {:error, :timeout}}

      assert {:error, message} = create(%{"name" => "x", "type" => "vlan", "vlan_id" => "5"}, static)
      assert message =~ "cannot be ruled out"
    end

    test "a direct network does not consult the VLAN table at all" do
      # It has no tag to collide with, so an unreadable table must not stop it.
      static = %{networks: {:error, :timeout}}
      assert {:ok, _} = create(%{"name" => "flat", "type" => "direct"}, static)
    end
  end

  describe "the claim, which is the constraint the read-then-refuse check is not" do
    defp claim(static) do
      Networking.create_network(
        %{"name" => "new", "type" => "vlan", "vlan_id" => "100"},
        source: {:static, static}
      )
    end

    # A network the reader can see, but whose VLAN it cannot: the state a losing create
    # observes when the winner's row has landed and its own read of the table came first.
    defp holder_network(id, name \\ "production") do
      %{"net_id" => id, "name" => name, "type" => "vlan", "vlan_id" => nil}
    end

    # `claimed_at_ms` of 1 is the epoch, so every claim built here is old enough to be
    # taken over if its network turns out not to exist. Freshness is a separate test.
    defp claim_row(id, name \\ "production", at \\ 1) do
      %{"vlan_id" => 100, "net_id" => id, "name" => name, "claimed_at_ms" => at}
    end

    test "a VLAN the network table says is free is still refused when it is claimed" do
      # This is what a concurrent create looks like from the loser's side, one round trip
      # later: the read said VLAN 100 was free, and it was not. The advisory check above
      # cannot see this, which is the whole reason the claim exists.
      id = "11111111-1111-1111-1111-111111111111"

      assert {:error, message} =
               claim(%{networks: [holder_network(id)], claims: [claim_row(id)]})

      assert message =~ "VLAN 100 is already used by production"
      assert message =~ "claimed it first"
    end

    test "a claim younger than the grace period is left alone even with no network" do
      # Otherwise the repair breaks what it protects: for the few milliseconds between a
      # create's claim and its row, its network legitimately does not exist, and a second
      # create checking in that window would take the claim from a create still running.
      gone = "44444444-4444-4444-4444-444444444444"
      now = System.system_time(:millisecond)

      assert {:error, message} =
               claim(%{networks: [], claims: [claim_row(gone, "in flight", now)], sink: self()})

      assert message =~ "has not finished writing its row"
      refute_received {:vlan_reclaim, _vlan, _net_id, _holder}
    end

    test "a direct network claims nothing" do
      # It has no tag, which is why hydra.gatoway_networks stores a null vlan_id for every
      # one of them -- the shape of the seeded Physical-Direct row on the live cluster.
      assert {:ok, _} =
               Networking.create_network(%{"name" => "flat", "type" => "direct"},
                 source: {:static, %{sink: self()}}
               )

      refute_received {:vlan_claim, _vlan, _net_id}
    end

    test "the claim is taken before the network row is written" do
      # The claim is what decides the race, so anything written first is written on the
      # strength of a read that may already be stale -- and Gatoway polls the network
      # table every five seconds, so a row that exists for a moment is a network that may
      # be configured on every host.
      assert {:error, _} =
               claim(%{sink: self(), insert: {:error, "The network could not be written: x"}})

      assert_received {:vlan_claim, 100, _net_id}
    end

    test "a create that cannot write its row gives the claim back" do
      # A claim left behind by a create that could not finish makes its VLAN unusable, and
      # an operator told only that the write failed has no reason to suspect it.
      assert {:error, message} =
               claim(%{sink: self(), insert: {:error, "The network could not be written: x"}})

      assert message =~ "could not be written"
      assert_received {:vlan_claim, 100, net_id}
      assert_received {:vlan_release, 100, ^net_id}
    end

    test "a create that succeeds releases nothing" do
      assert {:ok, "new"} = claim(%{sink: self()})
      assert_received {:vlan_claim, 100, _net_id}
      refute_received {:vlan_release, _vlan, _net_id}
    end

    test "a claim whose network no longer exists is taken over rather than left forever" do
      # The failure a release cannot cover: the node dies between claiming the VLAN and
      # writing the row. Left alone that VLAN is unusable for good, which is worse than
      # the duplicate the claim prevents.
      gone = "22222222-2222-2222-2222-222222222222"

      assert {:ok, "new"} = claim(%{networks: [], claims: [claim_row(gone, "half-created")], sink: self()})

      assert_received {:vlan_reclaim, 100, _net_id, ^gone}
    end

    test "a claim whose holder is not a network id at all is taken over" do
      # No delete will ever release such a claim, because nothing can present that net_id.
      # Treated as stranded for the same reason as the case above: the alternative is a
      # VLAN nothing can use again.
      assert {:ok, "new"} = claim(%{networks: [], claims: [claim_row("")], sink: self()})
      assert_received {:vlan_reclaim, 100, _net_id, ""}
    end

    test "the id written into the network row is a canonical uuid" do
      # It is interpolated into the statement, because a uuid argument's encoding depends
      # on the driver and this does not. It is also the claim's holder token, which is why
      # it is minted here rather than by the database's uuid().
      for _ <- 1..20, do: assert(Networking.uuid?(Networking.generate_net_id()))
      assert Networking.generate_net_id() != Networking.generate_net_id()
    end
  end

  describe "removing" do
    test "refuses anything that is not a network id" do
      # The id is written into the statement, so this is the check that makes that safe.
      for bad <- ["", "abc", "1; DROP TABLE hydra.gatoway_networks--", nil] do
        assert {:error, _} = Networking.delete_network(bad, source: {:static, %{}})
      end
    end

    test "accepts a canonical uuid" do
      id = "11111111-2222-3333-4444-555555555555"
      assert {:ok, ^id} = Networking.delete_network(id, source: {:static, %{}})
    end

    test "uuid?/1 admits nothing but hex and dashes" do
      assert Networking.uuid?("11111111-2222-3333-4444-555555555555")
      refute Networking.uuid?("11111111-2222-3333-4444-55555555555")
      refute Networking.uuid?("11111111-2222-3333-4444-55555555555g")
      refute Networking.uuid?(:not_a_string)
    end

    test "a removed network gives its VLAN back" do
      # Otherwise VLAN 100 is spoken for by a network that no longer exists, and nothing
      # can ever be created on it again.
      id = "11111111-1111-1111-1111-111111111111"

      static = %{
        networks: [%{"net_id" => id, "name" => "production", "type" => "vlan", "vlan_id" => 100}],
        sink: self()
      }

      assert {:ok, ^id} = Networking.delete_network(id, source: {:static, static})
      assert_received {:vlan_release, 100, ^id}
    end

    test "a removed direct network releases nothing" do
      id = "11111111-1111-1111-1111-111111111111"

      static = %{
        networks: [%{"net_id" => id, "name" => "flat", "type" => "direct", "vlan_id" => nil}],
        sink: self()
      }

      assert {:ok, ^id} = Networking.delete_network(id, source: {:static, static})
      refute_received {:vlan_release, _vlan, _net_id}
    end

    test "a delete whose network cannot be read is refused rather than stranding the claim" do
      # A delete that cannot identify the claim strands it, and a VLAN nobody can use
      # again is worse than a delete an operator has to retry.
      id = "11111111-1111-1111-1111-111111111111"

      assert {:error, message} =
               Networking.delete_network(id, source: {:static, %{networks: {:error, :timeout}}})

      assert message =~ "VLAN claim cannot be given back"
    end
  end

  describe "statements" do
    test "every read names its columns rather than selecting everything" do
      for {_name, cql} <- Networking.statements() do
        refute cql =~ "SELECT *"
        assert cql =~ "FROM hydra."
      end
    end
  end
end
