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
