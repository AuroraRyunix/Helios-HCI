defmodule SpectrumPhx.SettingsTest do
  @moduledoc """
  Cluster settings, and the three kinds of them that must not be confused.

  `cluster_settings` is a free-form key/value table, so the allow-list is the only thing
  between a typo and a permanent row; and `cluster.json` is the authority for the cluster's
  own identity, so a stale row claiming otherwise must not win.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Settings

  defp row(key, value), do: %{"key" => key, "value" => value}

  defp cluster(opts \\ []) do
    %{
      name: Keyword.get(opts, :name, "hci-01"),
      vip: Keyword.get(opts, :vip, "10.10.102.45"),
      nodes: Keyword.get(opts, :nodes, 3),
      redundancy_factor: Keyword.get(opts, :ftt, 0)
    }
  end

  defp all(static), do: Settings.all(source: {:static, Map.put_new(static, :cluster, cluster())})

  describe "reading" do
    test "a stored row overrides the default" do
      result = all(%{settings: [row("timezone", "Europe/Brussels")]})

      assert result.stored["timezone"] == "Europe/Brussels"
      assert result.stored["ntp_servers"] == "pool.ntp.org", "an unset key keeps its default"
    end

    test "a row for a key this console does not write is ignored" do
      # The table is free-form, so anything could be in it. Rendering an unknown key as a
      # field would offer to write something the allow-list then refuses.
      result = all(%{settings: [row("something_else", "x")]})

      refute Map.has_key?(result.stored, "something_else")
    end

    test "the cluster's identity comes from cluster.json, not from a row" do
      result = all(%{settings: [row("cluster_name", "impostor")], cluster: cluster(name: "hci-01")})

      assert result.cluster.name == "hci-01"
      refute Map.has_key?(result.stored, "cluster_name")
    end

    test "a table that will not read falls back to defaults and says so" do
      result = all(%{settings: {:error, :timeout}})

      refute result.available?
      assert result.error =~ "could not be read"
      assert result.stored == Settings.defaults()
    end

    test "accounts come back sorted" do
      result = all(%{users: [%{"username" => "zoe"}, %{"username" => "adam"}]})
      assert result.users == ["adam", "zoe"]
    end
  end

  describe "replication" do
    test "reports what the keyspace is doing, separately from what was asked for" do
      result =
        all(%{
          replication: [%{"replication" => %{"class" => "NetworkTopologyStrategy", "dc1" => "3"}}],
          cluster: cluster(ftt: 2, nodes: 3)
        })

      assert result.replication.factor == 3
      assert result.replication.implied == 3
    end

    test "the implied factor is ftt + 1, capped at the node count" do
      # Asking for two failures survivable on three nodes needs three copies; asking for
      # it on one node cannot have more than one.
      assert all(%{cluster: cluster(ftt: 2, nodes: 3)}).replication.implied == 3
      assert all(%{cluster: cluster(ftt: 2, nodes: 1)}).replication.implied == 1
      assert all(%{cluster: cluster(ftt: 0, nodes: 3)}).replication.implied == 1
    end

    test "it is labelled as metadata, because it says nothing about a guest's disk" do
      # Conflating the two is how an operator concludes their data is replicated because
      # this number is three.
      assert all(%{}).replication.scope == :metadata
    end

    test "an unreadable factor is nil rather than assumed" do
      assert all(%{replication: []}).replication.factor == nil
    end
  end

  describe "writing" do
    defp update(params), do: Settings.update(params, source: {:static, %{}})

    test "writes the keys on the allow-list" do
      assert {:ok, 2} = update(%{"timezone" => "UTC", "ntp_servers" => "a,b"})
    end

    test "refuses a key it does not write rather than dropping it silently" do
      # A setting that appears to save and does not is worse than one that refuses.
      assert {:error, message} = update(%{"rm_rf" => "yes"})
      assert message =~ "Not a setting"
    end

    test "the form's own fields are not mistaken for settings" do
      assert {:ok, 1} = update(%{"timezone" => "UTC", "_csrf_token" => "x", "_target" => "y"})
    end

    test "urbosa_enabled is not writable from here" do
      # Turning it on bootstraps namespaces and VXLAN interfaces on every host, which
      # belongs behind a task rather than a settings form.
      refute "urbosa_enabled" in Settings.writable_keys()
      assert "urbosa_enabled" in Settings.read_only_keys()
      assert {:error, _} = update(%{"urbosa_enabled" => "true"})
    end
  end

  describe "removing an account" do
    test "refuses to remove the last one" do
      # A console nobody can sign in to is not secured, it is bricked, and the only way
      # back is editing the database by hand.
      static = %{users: [%{"username" => "helios"}]}

      assert {:error, message} = Settings.delete_user("helios", source: {:static, static})
      assert message =~ "lock everyone out"
    end

    test "removes one when others remain" do
      static = %{users: [%{"username" => "helios"}, %{"username" => "second"}]}
      assert {:ok, "second"} = Settings.delete_user("second", source: {:static, static})
    end

    test "refuses an account that does not exist" do
      static = %{users: [%{"username" => "helios"}, %{"username" => "other"}]}
      assert {:error, "No such user."} = Settings.delete_user("ghost", source: {:static, static})
    end
  end

  describe "statements" do
    test "every read names its columns" do
      for {_name, cql} <- Settings.statements(), do: refute(cql =~ "SELECT *")
    end
  end
end
