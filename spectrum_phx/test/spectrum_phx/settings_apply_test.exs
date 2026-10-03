defmodule SpectrumPhx.SettingsApplyTest do
  @moduledoc """
  Saving a setting has to *do* what the Python console's save did, not just write a row.

  That endpoint rewrote `resolv.conf` and `chrony.conf` on every host, set the timezone,
  rewrote `cluster.json` for the cluster name, VIP and subnet (restarting `bifrost` when the
  VIP moved), altered the keyspace's replication factor and repaired when it rose, and
  re-scheduled the scrub. The port kept the rows and nothing else, which is how the VIP came
  to be display-only. Each test here fails against that port.

  Effects are observed through the `:effects` seam in the static source: nothing runs.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Settings
  alias SpectrumPhx.Settings.Apply

  @hosts ["10.0.0.1", "10.0.0.2", "10.0.0.3"]

  defp cluster(overrides \\ []) do
    Map.merge(
      %{name: "hci-01", vip: "10.0.0.45", subnet: "10.0.0.0/24", nodes: 3, redundancy_factor: 1},
      Map.new(overrides)
    )
  end

  # Runs a save against a static source and returns the result plus every effect it asked for.
  defp save(params, extra \\ %{}) do
    test = self()

    effects =
      Map.get(extra, :effects, fn tag ->
        send(test, {:effect, tag})
        :ok
      end)

    static =
      Map.merge(
        %{
          settings: [],
          cluster: cluster(),
          hosts: @hosts,
          effects: fn tag ->
            send(test, {:seen, tag})
            effects.(tag)
          end,
          replication: [%{"replication" => %{"class" => "NetworkTopologyStrategy", "datacenter1" => "3"}}]
        },
        Map.delete(extra, :effects)
      )

    result = Settings.save(params, source: {:static, static})
    {result, drain()}
  end

  defp drain(acc \\ []) do
    receive do
      {:seen, tag} -> drain([tag | acc])
      {:effect, _} -> drain(acc)
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp hosts_touched(effects, kind), do: for({^kind, ip, _} <- effects, do: ip)

  describe "the VIP and the rest of cluster.json" do
    test "a changed VIP is written to cluster.json on every host and restarts bifrost" do
      {{:ok, outcome}, effects} = save(%{"vip" => "10.0.0.99"})

      assert outcome.failed == []
      assert hosts_touched(effects, :host) == @hosts

      # The command carries the new value in base64 only; the operator's text is never
      # something the shell parses.
      [{:host, _ip, command} | _] = Enum.filter(effects, &match?({:host, _, _}, &1))
      refute command =~ "10.0.0.99"
      assert command =~ "/etc/hci/cluster.json"

      assert for({:units, ip, "restart", ["bifrost"]} <- effects, do: ip) == @hosts
    end

    test "only the keys that changed are merged, so saving the name cannot clear the VIP" do
      {{:ok, _}, effects} = save(%{"cluster_name" => "renamed", "vip" => "10.0.0.45"})

      [{:host, _ip, command} | _] = Enum.filter(effects, &match?({:host, _, _}, &1))
      payload = command |> String.split(" ") |> Enum.at(1) |> Base.decode64!() |> Jason.decode!()

      assert payload == %{"cluster_name" => "renamed"}
      refute Enum.any?(effects, &match?({:units, _, _, _}, &1)), "the VIP did not move"
    end

    test "re-saving what is already in force touches no host" do
      {{:ok, outcome}, effects} =
        save(%{"vip" => "10.0.0.45", "cluster_name" => "hci-01", "cluster_subnet" => "10.0.0.0/24"})

      assert outcome.saved == 0
      assert effects == []
    end

    test "an unreachable host is named, and the others still took the change" do
      failing = fn
        {:host, "10.0.0.2", _} -> {:error, "exit 1: connection refused"}
        _ -> :ok
      end

      {{:ok, outcome}, effects} = save(%{"vip" => "10.0.0.99"}, %{effects: failing})

      assert [failure] = outcome.failed
      assert failure =~ "10.0.0.2"
      assert failure =~ "connection refused"
      assert "10.0.0.3" in hosts_touched(effects, :host), "one bad host must not stop the rest"
    end
  end

  describe "what is applied to the hosts" do
    test "changed resolvers rewrite resolv.conf everywhere" do
      {{:ok, outcome}, effects} =
        save(%{"dns_servers" => "1.1.1.1, 9.9.9.9", "dns_search_domains" => "lab.local"})

      assert hosts_touched(effects, :host) == @hosts
      assert [applied] = outcome.applied |> Enum.filter(&(&1 =~ "DNS"))
      assert applied =~ "3 host(s)"

      [{:host, _, command} | _] = Enum.filter(effects, &match?({:host, _, _}, &1))
      [_, encoded, _, _, path] = String.split(command, " ", parts: 5)
      assert path =~ "/etc/resolv.conf"

      assert Base.decode64!(encoded) ==
               "search lab.local\nnameserver 1.1.1.1\nnameserver 9.9.9.9\n"
    end

    test "changed NTP servers rewrite chrony.conf and restart chronyd" do
      {{:ok, _}, effects} = save(%{"ntp_servers" => "ntp1.lab,ntp2.lab"})

      assert hosts_touched(effects, :host) == @hosts
      assert for({:units, ip, "restart", ["chronyd"]} <- effects, do: ip) == @hosts
    end

    test "a changed timezone is set on every host" do
      {{:ok, _}, effects} = save(%{"timezone" => "Europe/Brussels"})

      commands = for {:host, _, command} <- effects, do: command
      assert length(commands) == 3
      assert Enum.all?(commands, &(&1 == "timedatectl set-timezone 'Europe/Brussels'"))
    end

    test "a changed scrub interval re-schedules the scrub job" do
      {{:ok, outcome}, effects} = save(%{"scrub_interval" => "monthly"})

      assert [{:cql, statement, ["0 2 1 * *", 2_592_000, true]}] = effects
      assert statement =~ "storage_scrub"
      assert Enum.any?(outcome.applied, &(&1 =~ "scrub"))
    end

    test "disabling the scrub disables the job rather than picking a cron for it" do
      {{:ok, _}, effects} = save(%{"scrub_interval" => "disabled"})

      assert [{:cql, _, [_cron, _seconds, false]}] = effects
    end
  end

  describe "the keyspace replication factor" do
    test "raising it alters the keyspace and starts a repair" do
      {{:ok, outcome}, effects} =
        save(%{"replication_factor" => "3"}, %{
          replication: [%{"replication" => %{"class" => "NetworkTopologyStrategy", "datacenter1" => "1"}}]
        })

      assert [{:cql, statement, []}, {:repair, "10.0.0.1"}] = effects
      assert statement =~ "ALTER KEYSPACE hydra"
      assert statement =~ "'datacenter1': 3"
      assert Enum.any?(outcome.applied, &(&1 =~ "repair was started"))
    end

    test "lowering it needs no repair" do
      {{:ok, _}, effects} = save(%{"replication_factor" => "1"})

      assert [{:cql, statement, []}] = effects
      assert statement =~ "'datacenter1': 1"
    end

    test "it is capped at the node count, because a higher factor can never be satisfied" do
      {{:ok, _}, effects} =
        save(%{"replication_factor" => "5"}, %{
          replication: [%{"replication" => %{"class" => "NetworkTopologyStrategy", "datacenter1" => "1"}}]
        })

      assert [{:cql, statement, []}, {:repair, _}] = effects
      assert statement =~ "'datacenter1': 3"
    end

    test "a repair that did not start is reported, because the new replicas are empty" do
      failing = fn
        {:repair, _} -> {:error, "spark-daemon did not answer"}
        _ -> :ok
      end

      {{:ok, outcome}, _} =
        save(%{"replication_factor" => "3"}, %{
          effects: failing,
          replication: [%{"replication" => %{"class" => "NetworkTopologyStrategy", "datacenter1" => "1"}}]
        })

      assert [failure] = outcome.failed
      assert failure =~ "repair did not start"
      assert failure =~ "nodetool repair"
    end

    test "an ALTER that fails is an error and does not claim the factor changed" do
      failing = fn
        {:cql, "ALTER" <> _, _} -> {:error, "unavailable"}
        _ -> :ok
      end

      {{:ok, outcome}, _} =
        save(%{"replication_factor" => "3"}, %{
          effects: failing,
          replication: [%{"replication" => %{"class" => "NetworkTopologyStrategy", "datacenter1" => "1"}}]
        })

      assert outcome.saved == 0
      assert [failure] = outcome.failed
      assert failure =~ "could not be changed"
    end
  end

  describe "blank cluster fields" do
    test "a blank VIP means 'not shown', never 'clear it on every host'" do
      # With no subnet in cluster.json the input renders empty, and submitting the page
      # sends "". That must not be read as an instruction.
      {{:ok, outcome}, effects} = save(%{"vip" => "", "cluster_subnet" => "", "replication_factor" => ""})

      assert outcome.saved == 0
      assert effects == []
    end
  end

  describe "validation" do
    test "nothing is written or applied when any field is invalid" do
      {result, effects} =
        save(%{"vip" => "banana", "dns_servers" => "1.1.1.1", "cluster_subnet" => "10.0.0.0/99"})

      assert {:error, message} = result
      assert message =~ "VIP"
      assert message =~ "Subnet"
      assert effects == [], "a rejected form must not have half-applied"
    end

    test "every problem is reported together, not the first" do
      assert {:error, errors} =
               Apply.validate(%{
                 "vip" => "10.0.0",
                 "cluster_name" => "bad name",
                 "replication_factor" => "9",
                 "dns_mtu" => "12",
                 "session_timeout" => "1",
                 "dns_servers" => "not-an-ip",
                 "scrub_interval" => "sometimes",
                 "password_policy" => "strict",
                 "timezone" => "UTC; rm -rf /"
               })

      assert length(errors) == 9
    end

    test "the old hint said 'strict' but the check is for 'enabled', so only 'enabled' is accepted" do
      # The page used to suggest "disabled or strict". `validate_password_complexity` in the
      # Python tier tests for "enabled", so typing "strict" silently kept the weak rule.
      assert :ok = Apply.validate(%{"password_policy" => "enabled"})
      assert {:error, _} = Apply.validate(%{"password_policy" => "strict"})
    end

    test "a timezone that could reach a shell is refused, not sanitised into a different one" do
      assert {:error, _} = Apply.validate(%{"timezone" => "UTC;reboot"})
      assert {:error, _} = Apply.validate(%{"timezone" => "$(id)"})
      assert :ok = Apply.validate(%{"timezone" => "America/Argentina/Buenos_Aires"})
    end
  end

  describe "accounts" do
    defp users_static(extra \\ %{}) do
      {:static,
       Map.merge(
         %{
           settings: [%{"key" => "password_policy", "value" => "disabled"}],
           users: [%{"username" => "helios"}],
           cluster: cluster()
         },
         extra
       )}
    end

    test "an operator can be created, under the cluster's password policy" do
      assert {:ok, "ops_1"} = Settings.create_user("ops_1", "hunter22", source: users_static())
    end

    test "the strong policy asks for length, case, a digit and a symbol" do
      strong =
        users_static(%{settings: [%{"key" => "password_policy", "value" => "enabled"}]})

      assert {:error, message} = Settings.create_user("ops_1", "hunter22", source: strong)
      assert message =~ "upper-case"
      assert {:ok, _} = Settings.create_user("ops_1", "Hunter22!", source: strong)
    end

    test "an existing account is refused rather than having its password reset" do
      assert {:error, "That user already exists."} =
               Settings.create_user("helios", "whatever1", source: users_static())
    end

    test "usernames follow the console's pattern" do
      for bad <- ["ab", "has space", "semi;colon", String.duplicate("a", 21)] do
        assert {:error, _} = Settings.create_user(bad, "hunter22", source: users_static())
      end
    end

    test "a password can be changed for an existing account only" do
      assert {:ok, "helios"} = Settings.set_user_password("helios", "newpass1", source: users_static())
      assert {:error, "No such user."} = Settings.set_user_password("ghost", "newpass1", source: users_static())
      assert {:error, _} = Settings.set_user_password("helios", "abc", source: users_static())
    end
  end

  describe "the cluster's name" do
    test "is read from cluster.json's atom-keyed config, which it never was" do
      # `Config.all()` is keyed by atoms. The page asked for the string "cluster_name" and
      # so named no cluster at all. Pin the key the real config uses.
      assert Map.has_key?(SpectrumPhx.Cluster.Config.all(), :cluster_name)
      refute Map.has_key?(SpectrumPhx.Cluster.Config.all(), "cluster_name")
    end
  end
end
