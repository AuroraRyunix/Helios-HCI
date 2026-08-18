defmodule SpectrumPhx.Cluster.Config do
  @moduledoc """
  Reads the on-host Helios cluster configuration.

  The Python tier re-read and re-parsed `/etc/hci/cluster.json` at nearly every call
  site. Here it is read once and cached in an Agent, with an explicit `refresh/0` for
  the paths that genuinely need to observe a change (node add/remove).
  """
  use Agent
  require Logger

  @cluster_json "/etc/hci/cluster.json"
  @spectrum_env "/etc/hci/spectrum/spectrum.env"

  def start_link(_opts \\ []), do: Agent.start_link(fn -> load() end, name: __MODULE__)

  @doc "Full parsed cluster configuration."
  def all, do: Agent.get(__MODULE__, & &1)

  @doc "Re-read configuration from disk."
  def refresh, do: Agent.update(__MODULE__, fn _ -> load() end)

  @doc "Cluster node IPs, in configured order."
  def node_ips, do: all() |> Map.get(:hosts, []) |> Enum.map(& &1["ip"]) |> Enum.reject(&is_nil/1)

  @doc "Hostname for an IP, or the IP itself when unknown."
  def hostname_for(ip) do
    all()
    |> Map.get(:hosts, [])
    |> Enum.find(&(&1["ip"] == ip))
    |> case do
      %{"hostname" => h} when is_binary(h) -> h
      _ -> ip
    end
  end

  @doc "This node's IP, from spectrum.env, falling back to the first configured host."
  def local_ip, do: all() |> Map.get(:local_ip) || List.first(node_ips()) || "127.0.0.1"

  @doc "Floating cluster VIP, if configured."
  def vip, do: all() |> Map.get(:vip)

  @doc "Fault-tolerance factor the cluster was created with."
  def redundancy_factor, do: all() |> Map.get(:redundancy_factor, 0)

  defp load do
    cluster =
      case File.read(@cluster_json) do
        {:ok, body} ->
          case Jason.decode(body) do
            {:ok, map} -> map
            {:error, reason} ->
              Logger.warning("Could not parse #{@cluster_json}: #{inspect(reason)}")
              %{}
          end

        {:error, reason} ->
          # Absent during local development; the app must still boot.
          Logger.info("#{@cluster_json} unavailable (#{inspect(reason)}); using empty config.")
          %{}
      end

    %{
      hosts: Map.get(cluster, "hosts", []),
      vip: Map.get(cluster, "vip"),
      cluster_name: Map.get(cluster, "cluster_name"),
      redundancy_factor: Map.get(cluster, "redundancy_factor", 0),
      local_ip: read_local_ip()
    }
  end

  defp read_local_ip do
    with {:ok, body} <- File.read(@spectrum_env),
         [_ | _] = lines <- String.split(body, "\n") do
      Enum.find_value(lines, fn line ->
        case String.split(String.trim(line), "=", parts: 2) do
          ["LOCAL_HYPERVISOR_IP", v] -> String.trim(v)
          _ -> nil
        end
      end)
    else
      _ -> nil
    end
  end
end
