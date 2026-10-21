defmodule SpectrumPhx.Cluster.Config do
  @moduledoc """
  Reads the on-host Helios cluster configuration.

  The Python tier re-read and re-parsed `/etc/hci/cluster.json` at nearly every call
  site. Here it is read once and cached in an Agent, with an explicit `refresh/0` for
  the paths that genuinely need to observe a change (node add/remove).
  """
  use Agent
  require Logger

  # Overridable so the app can be exercised off-node during development. In the container
  # these stay at their real paths, which are bind-mounted in.
  @default_cluster_json "/etc/hci/cluster.json"
  @default_spectrum_env "/etc/hci/spectrum/spectrum.env"

  defp cluster_json_path,
    do: Application.get_env(:spectrum_phx, :cluster_json_path, @default_cluster_json)

  defp spectrum_env_path,
    do: Application.get_env(:spectrum_phx, :spectrum_env_path, @default_spectrum_env)

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

  @doc "Configured host metadata map (rack, chassis_serial, node_position, node_id, role) for an IP."
  def host_info(ip) do
    all()
    |> Map.get(:hosts, [])
    |> Enum.with_index(1)
    |> Enum.find_value(%{}, fn {h, idx} ->
      if is_map(h) and h["ip"] == ip do
        Map.put_new(h, "node_id", idx)
      end
    end)
  end

  @doc "This node's IP, from spectrum.env, falling back to the first configured host."
  def local_ip, do: all() |> Map.get(:local_ip) || List.first(node_ips()) || "127.0.0.1"

  @doc "Floating cluster VIP, if configured."
  def vip, do: all() |> Map.get(:vip)

  @doc "The cluster's network, in CIDR form, if configured."
  def cluster_subnet, do: all() |> Map.get(:cluster_subnet)

  @doc "The cluster's identifier, if one has been generated."
  def cluster_id, do: all() |> Map.get(:cluster_id)

  @doc "Fault-tolerance factor the cluster was created with."
  def redundancy_factor, do: all() |> Map.get(:redundancy_factor, 0)

  @doc "Active cluster fault tolerance domain level (kdisk, khost, kblock, krack)."
  def fault_domain, do: all() |> Map.get(:fault_domain, "khost")

  defp load do
    cluster =
      case File.read(cluster_json_path()) do
        {:ok, body} ->
          case Jason.decode(body) do
            {:ok, map} ->
              map

            {:error, reason} ->
              Logger.warning("Could not parse #{cluster_json_path()}: #{inspect(reason)}")
              %{}
          end

        {:error, reason} ->
          # Absent during local development; the app must still boot.
          Logger.info(
            "#{cluster_json_path()} unavailable (#{inspect(reason)}); using empty config."
          )

          %{}
      end

    raw_hosts = Map.get(cluster, "hosts", [])
    hosts =
      raw_hosts
      |> Enum.with_index()
      |> Enum.map(fn {host, idx} ->
        if is_map(host) do
          rack_num = div(idx, 4) + 1
          host
          |> Map.put_new("rack", "rack#{rack_num}")
          |> Map.put_new("chassis_serial", "chassis-#{String.pad_leading(Integer.to_string(rack_num), 2, "0")}")
          |> Map.put_new("node_position", <<?A + rem(idx, 4)>>)
        else
          host
        end
      end)

    fault_domain =
      Map.get(
        cluster,
        "fault_domain",
        if(length(hosts) <= 1 and Map.get(cluster, "redundancy_factor", 0) > 0,
          do: "kdisk",
          else: "khost"
        )
      )

    %{
      hosts: hosts,
      vip: Map.get(cluster, "vip"),
      cluster_name: Map.get(cluster, "cluster_name"),
      cluster_subnet: Map.get(cluster, "cluster_subnet"),
      cluster_id: Map.get(cluster, "cluster_id"),
      redundancy_factor: Map.get(cluster, "redundancy_factor", 0),
      fault_domain: fault_domain,
      local_ip: read_local_ip()
    }
  end

  defp read_local_ip do
    with {:ok, body} <- File.read(spectrum_env_path()),
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
