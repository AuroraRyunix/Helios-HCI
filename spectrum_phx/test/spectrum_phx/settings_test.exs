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

    test "the overlay's recorded state is read from its row, not defaulted" do
      # The reduce that builds `stored` only accepts a key the accumulator already has --
      # that is what stops a stray row in this free-form table becoming a setting -- so a
      # key missing from the seed can never be read at all, however often the cluster
      # writes it. `urbosa_enabled` was exactly that: written by both consoles, absent from
      # the seed, and therefore reported as disabled on every settings page ever rendered.
      result = all(%{settings: [row("urbosa_enabled", "true")]})

      assert result.read_only["urbosa_enabled"] == "true"

      assert Settings.urbosa_enabled?(
               source: {:static, %{settings: [row("urbosa_enabled", "true")]}}
             )
    end

    test "an unreadable settings table reports the overlay as off, not on" do
      # Defaulting the other way would have a database outage read as "the overlay is up",
      # which is a Kubernetes deploy that is allowed and then does not work.
      result = all(%{settings: {:error, "connection refused"}})

      assert result.read_only["urbosa_enabled"] == "false"
      refute Settings.urbosa_enabled?(source: {:static, %{settings: {:error, "down"}}})
    end

    test "a row for a key this console does not write is ignored" do
      # The table is free-form, so anything could be in it. Rendering an unknown key as a
      # field would offer to write something the allow-list then refuses.
      result = all(%{settings: [row("something_else", "x")]})

      refute Map.has_key?(result.stored, "something_else")
    end

    test "the cluster's identity comes from cluster.json, not from a row" do
      result =
        all(%{settings: [row("cluster_name", "impostor")], cluster: cluster(name: "hci-01")})

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

    test "the implied factor is 2 * ftt + 1, capped at the node count" do
      # The keyspace is read and written at QUORUM, a strict majority, which survives F failures
      # only with 2F+1 replicas. ftt 1 on three nodes is therefore 3, not 2: at 2 either node
      # going down stops every read and write in the cluster, and this page used to ask for 2.
      assert all(%{cluster: cluster(ftt: 1, nodes: 3)}).replication.implied == 3
      assert all(%{cluster: cluster(ftt: 1, nodes: 2)}).replication.implied == 2
      assert all(%{cluster: cluster(ftt: 2, nodes: 3)}).replication.implied == 3
      assert all(%{cluster: cluster(ftt: 2, nodes: 5)}).replication.implied == 5
      assert all(%{cluster: cluster(ftt: 2, nodes: 1)}).replication.implied == 1
      assert all(%{cluster: cluster(ftt: 0, nodes: 3)}).replication.implied == 1
    end

    test "the metadata rule agrees with the Python one" do
      for {ftt, nodes, expected} <- [{1, 3, 3}, {1, 2, 2}, {1, 1, 1}, {0, 3, 1}, {2, 5, 5}, {2, 3, 3}] do
        assert Settings.metadata_replication(ftt, nodes) == expected
      end
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

  describe "ssl deployment" do
    test "rejects missing or empty cert and key" do
      assert {:error, msg} = Settings.deploy_ssl("", "", source: {:static, %{}})
      assert msg =~ "Certificate and Private Key are required"
    end

    test "rejects invalid PEM certificate" do
      assert {:error, msg} = Settings.deploy_ssl("not-a-cert", "-----BEGIN RSA PRIVATE KEY-----\nKEY\n-----END RSA PRIVATE KEY-----", source: {:static, %{}})
      assert msg =~ "BEGIN CERTIFICATE"
    end

    test "rejects invalid PEM private key" do
      valid_cert = "-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----"
      assert {:error, msg} = Settings.deploy_ssl(valid_cert, "plain-text-key", source: {:static, %{}})
      assert msg =~ "private key"
    end

    test "deploys valid cert and key through effects" do
      test = self()
      valid_cert = "-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----"
      valid_key = "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----"

      static = %{
        effects: fn {:ssl, c, k} ->
          send(test, {:ssl_deployed, c, k})
          :ok
        end
      }

      assert {:ok, msg} = Settings.deploy_ssl(valid_cert, valid_key, source: {:static, static})
      assert msg =~ "SSL Certificate deployed"
      assert_received {:ssl_deployed, ^valid_cert, ^valid_key}
    end
  end

  describe "node operations" do
    defp sample_nodes do
      [
        %{"hostname" => "hci-01", "ip" => "10.10.102.41", "role" => "hypervisor", "is_witness" => false},
        %{"hostname" => "hci-02", "ip" => "10.10.102.42", "role" => "hypervisor", "is_witness" => false},
        %{"hostname" => "hci-wit", "ip" => "10.10.102.49", "role" => "witness", "is_witness" => true}
      ]
    end

    test "cluster_nodes lists nodes and identifies witnesses" do
      nodes = Settings.cluster_nodes(source: {:static, %{hosts: sample_nodes()}})
      assert length(nodes) == 3
      assert Enum.find(nodes, &(&1.hostname == "hci-wit")).is_witness?
      refute Enum.find(nodes, &(&1.hostname == "hci-01")).is_witness?
    end

    test "add_node validates hostname and IP" do
      static = %{hosts: sample_nodes()}
      assert {:error, msg} = Settings.add_node("", "10.10.102.43", source: {:static, static})
      assert msg =~ "Hostname is required"

      assert {:error, msg} = Settings.add_node("invalid host", "10.10.102.43", source: {:static, static})
      assert msg =~ "alphanumeric"

      assert {:error, msg} = Settings.add_node("hci-03", "banana", source: {:static, static})
      assert msg =~ "IPv4 address"
    end

    test "add_node refuses duplicates" do
      static = %{hosts: sample_nodes()}
      assert {:error, msg} = Settings.add_node("hci-01", "10.10.102.99", source: {:static, static})
      assert msg =~ "already configured"

      assert {:error, msg} = Settings.add_node("hci-99", "10.10.102.41", source: {:static, static})
      assert msg =~ "already configured"
    end

    test "add_node dispatches addition with witness flag" do
      test = self()
      static = %{
        hosts: sample_nodes(),
        effects: fn tag ->
          send(test, {:effect, tag})
          :ok
        end
      }

      assert {:ok, _} = Settings.add_node("hci-wit2", "10.10.102.50", source: {:static, static}, witness: true)
      assert_received {:effect, {:add_node, "hci-wit2", "10.10.102.50", true}}
    end

    test "remove_node refuses removing the only node or non-existent node" do
      assert {:error, msg} = Settings.remove_node("ghost", source: {:static, %{hosts: sample_nodes()}})
      assert msg =~ "not found"

      single = %{hosts: [%{"hostname" => "sole-node", "ip" => "10.0.0.1"}]}
      assert {:error, msg} = Settings.remove_node("sole-node", source: {:static, single})
      assert msg =~ "only remaining node"
    end

    test "remove_node dispatches removal through effects" do
      test = self()
      static = %{
        hosts: sample_nodes(),
        effects: fn tag ->
          send(test, {:effect, tag})
          :ok
        end
      }

      assert {:ok, _} = Settings.remove_node("hci-02", source: {:static, static})
      assert_received {:effect, {:remove_node, "hci-02", "10.10.102.42"}}
    end

    test "node_maintenance and reboot dispatch through effects" do
      test = self()
      static = %{
        hosts: sample_nodes(),
        effects: fn tag ->
          send(test, {:effect, tag})
          :ok
        end
      }

      assert {:ok, _} = Settings.node_maintenance("10.10.102.41", "enter", source: {:static, static})
      assert_received {:effect, {:maintenance, "10.10.102.41", "enter"}}

      assert {:ok, _} = Settings.node_reboot("10.10.102.41", source: {:static, static})
      assert_received {:effect, {:reboot, "10.10.102.41"}}
    end

    test "add_node supports optional redundancy_factor and zk_role_change dispatches promotion" do
      test = self()
      static = %{
        hosts: sample_nodes(),
        effects: fn tag ->
          send(test, {:effect, tag})
          :ok
        end
      }

      assert {:ok, _} =
               Settings.add_node("hci-04", "10.10.102.44",
                 source: {:static, static},
                 redundancy_factor: "2"
               )

      assert_received {:effect, {:add_node, "hci-04", "10.10.102.44", false, 2}}

      assert {:error, msg} =
               Settings.add_node("hci-05", "10.10.102.45",
                 source: {:static, static},
                 redundancy_factor: "9"
               )

      assert msg =~ "Redundancy factor"

      assert {:ok, _} =
               Settings.zk_role_change("10.10.102.42", "promote",
                 replacing: "10.10.102.41",
                 source: {:static, static}
               )

      assert_received {:effect, {:zk_role, "promote", "10.10.102.42", "10.10.102.41"}}
    end
  end

  describe "statements" do
    test "every read names its columns" do
      for {_name, cql} <- Settings.statements(), do: refute(cql =~ "SELECT *")
    end

    test "console protocol policy and default protocol are stored settings" do
      defaults = Settings.defaults()
      assert defaults["console_protocol_policy"] == "both"
      assert defaults["default_console_protocol"] == "vnc"

      params = %{
        "console_protocol_policy" => "spice",
        "default_console_protocol" => "spice"
      }
      assert {:ok, %{saved: 2}} = Settings.save(params, source: {:static, %{}})

      invalid = %{"console_protocol_policy" => "rdp"}
      assert {:error, msg} = Settings.save(invalid, source: {:static, %{}})
      assert msg =~ "one of both, vnc, spice"
    end
  end
end
