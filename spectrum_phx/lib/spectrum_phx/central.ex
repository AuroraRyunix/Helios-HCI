defmodule SpectrumPhx.Central do
  @moduledoc """
  Multi-cluster orchestration and Helios Central appliance management.
  Provides fleet inventory, multi-cluster federation, and appliance auto-deploy.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Settings

  @sizing_presets [
    %{
      id: "tiny",
      name: "Tiny / Edge Appliance",
      desc: "Ultra-lean footprint tailored for lab environments, testing clusters, and remote branch offices (1-3 clusters).",
      vcpus: 2,
      memory_mb: 4096,
      disk_gb: 32,
      badge: "Lab Recommended",
      color: "info"
    },
    %{
      id: "standard",
      name: "Standard Production",
      desc: "Balanced multi-cluster control plane with telemetry aggregation and automated fleet DRS (3-10 clusters).",
      vcpus: 4,
      memory_mb: 8192,
      disk_gb: 64,
      badge: "Production Ready",
      color: "primary"
    },
    %{
      id: "enterprise",
      name: "Enterprise Scale-Out",
      desc: "High-density multi-cluster management with cross-cluster vMotion, full Loki aggregation, and deep retention (10+ clusters).",
      vcpus: 8,
      memory_mb: 16384,
      disk_gb: 120,
      badge: "Fleet Scale",
      color: "secondary"
    }
  ]

  def sizing_presets, do: @sizing_presets

  def get_preset(id) do
    Enum.find(@sizing_presets, fn p -> p.id == to_string(id) end) || hd(@sizing_presets)
  end

  @doc "Fetch Central deployment status from hydra.central_config"
  def deployment_status do
    case Hydra.query("SELECT value FROM hydra.central_config WHERE key = 'deployment';") do
      {:ok, [%{"value" => val}]} when is_binary(val) ->
        case Jason.decode(val) do
          {:ok, map} when is_map(map) -> {:ok, map}
          _ -> {:not_deployed, nil}
        end

      _ ->
        {:not_deployed, nil}
    end
  end

  @doc "Fetch all registered member clusters from hydra.central_clusters"
  def list_clusters do
    case Hydra.query("SELECT cluster_id, name, vip, port, status, node_count, vm_count, cpu_cores, memory_total_mb, memory_used_mb, storage_total_bytes, storage_used_bytes, auth_token, last_seen_ms, enrolled_at_ms, metadata FROM hydra.central_clusters;") do
      {:ok, rows} when is_list(rows) and length(rows) > 0 ->
        Enum.map(rows, &normalize_cluster/1)

      _ ->
        # Auto-discover and populate the local cluster as Member 01
        [local_cluster_record()]
    end
  end

  def local_cluster_record do
    config = Config.all()
    hosts = Settings.cluster_nodes()
    node_count = max(1, length(hosts))

    vms =
      case Hydra.query("SELECT name, state, vcpu, memory FROM hydra.vms;") do
        {:ok, list} when is_list(list) -> list
        _ -> []
      end

    vdisks =
      case Hydra.query("SELECT vdisk_id, size_bytes FROM hydra.dfs_vdisks;") do
        {:ok, list} when is_list(list) -> list
        _ -> []
      end

    storage_used = Enum.reduce(vdisks, 0, fn row, acc -> acc + (row["size_bytes"] || 0) end)
    storage_snap = SpectrumPhx.Storage.snapshot()
    storage_total =
      case storage_snap do
        %{capacity: %{usable_total_bytes: usable}} when is_integer(usable) and usable > 0 -> usable
        %{capacity: %{raw_total_bytes: raw}} when is_integer(raw) and raw > 0 -> raw
        _ -> storage_used
      end

    {total_cpu, total_ram_mb, used_ram_mb} =
      case Hydra.query("SELECT node_ip, cpu_cores, mem_total_kb, mem_pct FROM hydra.logos_metrics;") do
        {:ok, rows} when is_list(rows) and length(rows) > 0 ->
          latest_per_node =
            Enum.reduce(rows, %{}, fn r, acc ->
              ip = r["node_ip"]
              if Map.has_key?(acc, ip), do: acc, else: Map.put(acc, ip, r)
            end)
            |> Map.values()

          cpu = Enum.sum(Enum.map(latest_per_node, fn r -> r["cpu_cores"] || 0 end))
          ram = Enum.sum(Enum.map(latest_per_node, fn r -> div(r["mem_total_kb"] || 0, 1024) end))
          used = Enum.sum(Enum.map(latest_per_node, fn r ->
            tot_kb = r["mem_total_kb"] || 0
            pct = r["mem_pct"] || 0.0
            div(trunc(tot_kb * pct / 100.0), 1024)
          end))
          {cpu, ram, used}

        _ ->
          {0, 0, 0}
      end

    vip = Config.vip() || (case hosts do [%{ip: ip} | _] -> ip; _ -> "127.0.0.1" end)
    cluster_name = Map.get(config, :cluster_name, "hci-01")

    %{
      cluster_id: cluster_name,
      name: cluster_name,
      vip: vip,
      port: 8443,
      status: "CONNECTED",
      node_count: node_count,
      vm_count: length(vms),
      cpu_cores: total_cpu,
      memory_total_mb: total_ram_mb,
      memory_used_mb: used_ram_mb,
      storage_total_bytes: storage_total,
      storage_used_bytes: storage_used,
      auth_token: "",
      last_seen_ms: System.system_time(:millisecond),
      enrolled_at_ms: 0,
      metadata: Jason.encode!(%{role: "primary", cluster_name: cluster_name}),
      is_local?: true
    }
  end

  def normalize_cluster(row) do
    vip = row["vip"] || ""
    %{
      cluster_id: row["cluster_id"],
      name: row["name"] || row["cluster_id"],
      vip: vip,
      port: row["port"] || 8443,
      status: row["status"] || "CONNECTED",
      node_count: row["node_count"] || 0,
      vm_count: row["vm_count"] || 0,
      cpu_cores: row["cpu_cores"] || 0,
      memory_total_mb: row["memory_total_mb"] || 0,
      memory_used_mb: row["memory_used_mb"] || 0,
      storage_total_bytes: row["storage_total_bytes"] || 0,
      storage_used_bytes: row["storage_used_bytes"] || 0,
      auth_token: row["auth_token"] || "",
      last_seen_ms: row["last_seen_ms"] || 0,
      enrolled_at_ms: row["enrolled_at_ms"] || 0,
      metadata: row["metadata"] || "{}",
      is_local?: row["cluster_id"] == "cluster-local" or (vip != "" and vip == Config.vip())
    }
  end

  @doc "Calculate aggregated fleet-wide metrics across all enrolled clusters"
  def fleet_summary(clusters) do
    total_clusters = length(clusters)
    online_clusters = Enum.count(clusters, fn c -> c.status == "CONNECTED" end)
    offline_clusters = total_clusters - online_clusters

    total_nodes = Enum.sum(Enum.map(clusters, & &1.node_count))
    total_vms = Enum.sum(Enum.map(clusters, & &1.vm_count))

    total_cpu = Enum.sum(Enum.map(clusters, & &1.cpu_cores))
    total_ram = Enum.sum(Enum.map(clusters, & &1.memory_total_mb))
    used_ram = Enum.sum(Enum.map(clusters, & &1.memory_used_mb))

    total_storage = Enum.sum(Enum.map(clusters, & &1.storage_total_bytes))
    used_storage = Enum.sum(Enum.map(clusters, & &1.storage_used_bytes))

    ram_percent = if total_ram > 0, do: min(100, round(used_ram * 100 / total_ram)), else: 0
    storage_percent = if total_storage > 0, do: min(100, round(used_storage * 100 / total_storage)), else: 0

    %{
      total_clusters: total_clusters,
      online_clusters: online_clusters,
      offline_clusters: offline_clusters,
      total_nodes: total_nodes,
      total_vms: total_vms,
      total_cpu: total_cpu,
      total_ram_mb: total_ram,
      used_ram_mb: used_ram,
      ram_percent: ram_percent,
      total_storage_bytes: total_storage,
      used_storage_bytes: used_storage,
      storage_percent: storage_percent
    }
  end

  @doc "Enroll an external cluster into Helios Central"
  def enroll_cluster(attrs) do
    cluster_id = Map.get(attrs, "cluster_id") || "cluster-" <> (:crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower))
    name = Map.get(attrs, "name", cluster_id)
    vip = Map.get(attrs, "vip", "")
    port = String.to_integer(to_string(Map.get(attrs, "port", 8443)))
    token = Map.get(attrs, "auth_token", "")
    now = System.system_time(:millisecond)

    nodes = Map.get(attrs, "node_count", 0)
    vms = Map.get(attrs, "vm_count", 0)
    cpus = Map.get(attrs, "cpu_cores", 0)
    mem_tot = Map.get(attrs, "memory_total_mb", 0)
    mem_used = Map.get(attrs, "memory_used_mb", 0)
    stor_tot = Map.get(attrs, "storage_total_bytes", 0)
    stor_used = Map.get(attrs, "storage_used_bytes", 0)

    statement = """
    INSERT INTO hydra.central_clusters (
      cluster_id, name, vip, port, status, node_count, vm_count,
      cpu_cores, memory_total_mb, memory_used_mb, storage_total_bytes, storage_used_bytes,
      auth_token, last_seen_ms, enrolled_at_ms, metadata
    ) VALUES (
      ?, ?, ?, ?, 'CONNECTED', ?, ?,
      ?, ?, ?, ?, ?,
      ?, ?, ?, '{"region": "remote-dc"}'
    );
    """

    case Hydra.query(statement, [cluster_id, name, vip, port, nodes, vms, cpus, mem_tot, mem_used, stor_tot, stor_used, token, now, now]) do
      {:ok, _} -> {:ok, cluster_id}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Remove an enrolled cluster from Central"
  def remove_cluster(cluster_id) do
    Hydra.query("DELETE FROM hydra.central_clusters WHERE cluster_id = ?;", [cluster_id])
  end

  @doc "Trigger Deployment of the Helios Central Appliance"
  def deploy_appliance(params) do
    now = System.system_time(:millisecond)
    preset_id = Map.get(params, "preset", "tiny")
    preset = get_preset(preset_id)
    name = Map.get(params, "name", "helios-central")
    ip = Map.get(params, "ip", "")
    gateway = Map.get(params, "gateway", "")
    dns = Map.get(params, "dns", "")
    admin_password = Map.get(params, "admin_password", "")

    if ip == "" or gateway == "" or admin_password == "" do
      {:error, "Appliance IP, gateway, and admin password are required."}
    else
      userdata_yaml = """
      #cloud-config
      system_info:
        default_user:
          name: admin
      chpasswd:
        list: |
          admin:#{admin_password}
        expire: False
      hostname: #{name}
      manage_etc_hosts: true
      """

      network_yaml = """
      version: 2
      ethernets:
        eth0:
          dhcp4: false
          addresses:
            - #{ip}
          gateway4: #{gateway}
          nameservers:
            addresses:
              - #{dns}
      """

      # Check if VM already exists in Hydra
      vm_result =
        case Hydra.query("SELECT name, state FROM hydra.vms WHERE name = ?;", [name]) do
          {:ok, [%{"name" => _} | _]} ->
            Hydra.query("UPDATE hydra.vms SET cloud_init_userdata = ?, cloud_init_network = ? WHERE name = ?;", [userdata_yaml, network_yaml, name])
            SpectrumPhx.Vms.power_on(name)

          _ ->
            vm_params = %{
              "name" => name,
              "vcpu" => preset.vcpus,
              "memory" => preset.memory_mb,
              "firmware" => "uefi",
              "disks_list" => "#{preset.disk_gb}G:default-pool",
              "cpu_model" => "host-passthrough",
              "cloud_init_userdata" => userdata_yaml,
              "cloud_init_network" => network_yaml
            }

            case SpectrumPhx.Vms.create_vm(vm_params) do
              {:ok, _vm} ->
                Hydra.query("UPDATE hydra.vms SET cloud_init_userdata = ?, cloud_init_network = ? WHERE name = ?;", [userdata_yaml, network_yaml, name])
                SpectrumPhx.Vms.power_on(name)

              {:error, reason} ->
                {:error, reason}
            end
        end

      case vm_result do
        {:ok, _} ->
          deployment_data = %{
            "name" => name,
            "ip" => ip,
            "gateway" => gateway,
            "dns" => dns,
            "preset" => preset_id,
            "vcpus" => preset.vcpus,
            "memory_mb" => preset.memory_mb,
            "disk_gb" => preset.disk_gb,
            "status" => "DEPLOYED",
            "deployed_at" => now
          }

          json_val = Jason.encode!(deployment_data)
          Hydra.query("INSERT INTO hydra.central_config (key, value, updated_at_ms) VALUES ('deployment', ?, ?);", [json_val, now])
          {:ok, deployment_data}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end
end
