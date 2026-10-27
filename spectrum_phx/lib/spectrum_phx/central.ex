defmodule SpectrumPhx.Central do
  @moduledoc """
  Multi-cluster orchestration and Helios Central appliance management.
  Provides fleet inventory, multi-cluster federation, and appliance auto-deploy.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Settings
  alias SpectrumPhx.Spark
  alias SpectrumPhx.Storage.Containers
  alias SpectrumPhx.Tasks
  alias SpectrumPhx.Vms

  require Logger

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
      {:ok, rows} when is_list(rows) ->
        Enum.map(rows, &normalize_cluster/1)

      _ ->
        []
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

  @doc "Generate an RFC 4122 v4 UUID string"
  def generate_uuid do
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)
    s = <<u0::48, 4::4, u1::12, 2::2, u2::62>> |> Base.encode16(case: :lower)
    <<p1::binary-size(8), p2::binary-size(4), p3::binary-size(4), p4::binary-size(4), p5::binary-size(12)>> = s
    "#{p1}-#{p2}-#{p3}-#{p4}-#{p5}"
  end

  @doc "Insert a task row into hydra.catalyst_tasks and notify task listeners"
  def insert_task(task_id, parent_task_id, action, status, progress, payload_map) do
    now_ms = System.system_time(:millisecond)
    payload_json = Jason.encode!(payload_map)
    parent_cql = if parent_task_id && parent_task_id != "", do: "#{parent_task_id}", else: "null"

    cql = """
    INSERT INTO hydra.catalyst_tasks (
      task_id, parent_task_id, component, service, action, task_type,
      status, payload, progress, created_at, updated_at
    ) VALUES (
      #{task_id}, #{parent_cql}, 'central', 'helios-central', ?, ?,
      ?, ?, ?, ?, ?
    );
    """

    res = Hydra.query(cql, [action, action, status, payload_json, progress, now_ms, now_ms])
    broadcast_tasks()
    res
  end

  @doc "Update task status, progress, and completion timestamp in hydra.catalyst_tasks"
  def update_task(task_id, status, progress, error_msg \\ nil) do
    now_ms = System.system_time(:millisecond)

    res =
      cond do
        status in ["completed", "failed"] and error_msg != nil ->
          cql = """
          UPDATE hydra.catalyst_tasks
          SET status = ?, progress = ?, error_msg = ?, updated_at = ?, completed_at = ?
          WHERE task_id = #{task_id};
          """
          Hydra.query(cql, [status, progress, error_msg, now_ms, now_ms])

        status in ["completed", "failed"] ->
          cql = """
          UPDATE hydra.catalyst_tasks
          SET status = ?, progress = ?, updated_at = ?, completed_at = ?
          WHERE task_id = #{task_id};
          """
          Hydra.query(cql, [status, progress, now_ms, now_ms])

        error_msg != nil ->
          cql = """
          UPDATE hydra.catalyst_tasks
          SET status = ?, progress = ?, error_msg = ?, updated_at = ?
          WHERE task_id = #{task_id};
          """
          Hydra.query(cql, [status, progress, error_msg, now_ms])

        true ->
          cql = """
          UPDATE hydra.catalyst_tasks
          SET status = ?, progress = ?, updated_at = ?
          WHERE task_id = #{task_id};
          """
          Hydra.query(cql, [status, progress, now_ms])
      end

    broadcast_tasks()
    res
  end

  defp broadcast_tasks do
    Tasks.broadcast(Tasks.fetch())
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  @doc """
  Initiate multi-step deployment of the Helios Central virtual appliance.
  Immediately enqueues a Catalyst workflow in `hydra.catalyst_tasks` and spawns
  an asynchronous provisioning task.
  """
  def start_deploy_workflow(params) do
    preset_id = Map.get(params, "preset", "tiny")
    preset = get_preset(preset_id)
    name = Map.get(params, "name", "helios-central") |> String.trim()
    name = if name == "", do: "helios-central", else: name
    ip = Map.get(params, "ip", "") |> String.trim()
    gateway = Map.get(params, "gateway", "") |> String.trim()
    dns = Map.get(params, "dns", "") |> String.trim()
    dns = if dns == "", do: gateway, else: dns
    admin_password = Map.get(params, "admin_password", "") |> String.trim()
    container = Map.get(params, "container", "default-pool") |> String.trim()
    container = if container == "", do: "default-pool", else: container

    clean_params = %{
      "name" => name,
      "ip" => ip,
      "gateway" => gateway,
      "dns" => dns,
      "admin_password" => admin_password,
      "container" => container,
      "preset" => preset_id
    }

    cond do
      ip == "" ->
        {:error, "Appliance IPv4 address with CIDR prefix is required (e.g. 10.10.102.75/24)."}

      gateway == "" ->
        {:error, "Default gateway address is required (e.g. 10.10.102.1)."}

      admin_password == "" ->
        {:error, "Admin password is required."}

      true ->
        parent_id = generate_uuid()
        parent_payload = %{
          "appliance_name" => name,
          "ip" => ip,
          "gateway" => gateway,
          "preset" => preset_id,
          "vcpus" => preset.vcpus,
          "memory_mb" => preset.memory_mb,
          "disk_gb" => preset.disk_gb
        }

        insert_task(parent_id, nil, "deploy", "processing", 0, parent_payload)

        Task.start(fn ->
          run_deploy_workflow(parent_id, clean_params)
        end)

        {:ok, %{"task_id" => parent_id, "name" => name}}
    end
  end

  @doc "Execute the sequential steps of Central appliance provisioning"
  def run_deploy_workflow(parent_id, params) do
    name = Map.get(params, "name", "helios-central")
    preset_id = Map.get(params, "preset", "tiny")
    preset = get_preset(preset_id)
    ip = Map.get(params, "ip", "")
    gateway = Map.get(params, "gateway", "")
    dns = Map.get(params, "dns", "")
    dns = if dns == "", do: gateway, else: dns
    admin_password = Map.get(params, "admin_password", "")
    container = Map.get(params, "container", "default-pool")

    target_host =
      Config.vip() || (case Config.node_ips() do [first | _] -> first; _ -> "127.0.0.1" end)

    vdisk_id = "#{name}-disk0"
    size_bytes = preset.disk_gb * 1024 * 1024 * 1024

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

    try do
      # -------------------------------------------------------------
      # Step 1: Storage Allocation (allocate_storage)
      # -------------------------------------------------------------
      step1_id = generate_uuid()
      step1_payload = %{
        "appliance_name" => name,
        "parent_task_id" => parent_id,
        "step" => "allocate_storage",
        "container" => container,
        "size_gb" => preset.disk_gb
      }
      insert_task(step1_id, parent_id, "allocate_storage", "processing", 10, step1_payload)
      update_task(parent_id, "processing", 10)

      case Containers.ensure_exists(container) do
        :ok -> :ok
        {:error, reason} -> throw({:step_failed, step1_id, "Storage container '#{container}' verification failed: #{inspect(reason)}"})
      end

      case Spark.dfs_create(target_host, vdisk_id, size_bytes, container: container) do
        {:ok, _} -> :ok
        {:error, {409, _}} -> :ok
        {:error, reason} ->
          err_msg = describe_error(reason)
          if String.contains?(String.downcase(err_msg), "already exists") do
            :ok
          else
            throw({:step_failed, step1_id, "Sidon vdisk '#{vdisk_id}' creation failed: #{err_msg}"})
          end
      end

      update_task(step1_id, "completed", 100)
      update_task(parent_id, "processing", 20)

      # -------------------------------------------------------------
      # Step 2: Golden Image Staging (stage_image)
      # -------------------------------------------------------------
      step2_id = generate_uuid()
      step2_payload = %{
        "appliance_name" => name,
        "parent_task_id" => parent_id,
        "step" => "stage_image",
        "image" => "helios-central-golden.raw"
      }
      insert_task(step2_id, parent_id, "stage_image", "processing", 15, step2_payload)
      update_task(parent_id, "processing", 25)

      # Attach the vdisk on target_host so its NBD socket is active
      case Spark.dfs_attach(target_host, vdisk_id) do
        {:ok, _} -> :ok
        {:error, reason} ->
          throw({:step_failed, step2_id, "Failed to attach vdisk '#{vdisk_id}' on #{target_host}: #{describe_error(reason)}"})
      end

      update_task(step2_id, "processing", 40)
      update_task(parent_id, "processing", 35)

      # Stream golden OS raw image onto Sidon NBD socket via qemu-img convert
      convert_cmd = "qemu-img convert -O raw /var/lib/hci/aether/images/helios-central-golden.raw 'nbd+unix:///#{vdisk_id}?socket=/var/lib/hci/sidon/nbd/#{vdisk_id}.sock'"
      convert_res = Spark.execute(target_host, convert_cmd, timeout: 300)

      # Detach from staging host so hypervisor DRS / Vali can claim and attach it wherever it places the VM
      Spark.dfs_detach(target_host, vdisk_id)

      case convert_res do
        {0, _out, _err} ->
          :ok

        {rc, out, err} ->
          detail = String.trim("#{err || ""} #{out || ""}")
          throw({:step_failed, step2_id, "OS image staging failed with exit code #{rc}: #{detail}"})
      end

      update_task(step2_id, "completed", 100)
      update_task(parent_id, "processing", 50)

      # -------------------------------------------------------------
      # Step 3: Cloud-Init NoCloud Injection (inject_cloud_init)
      # -------------------------------------------------------------
      step3_id = generate_uuid()
      step3_payload = %{
        "appliance_name" => name,
        "parent_task_id" => parent_id,
        "step" => "inject_cloud_init",
        "ip" => ip,
        "gateway" => gateway
      }
      insert_task(step3_id, parent_id, "inject_cloud_init", "processing", 50, step3_payload)
      update_task(parent_id, "processing", 55)

      if String.trim(ip) == "" or String.trim(gateway) == "" or String.trim(admin_password) == "" do
        throw({:step_failed, step3_id, "Invalid network or credential payload for Cloud-Init injection."})
      end

      update_task(step3_id, "completed", 100)
      update_task(parent_id, "processing", 65)

      # -------------------------------------------------------------
      # Step 4: VM Domain Registration (register_domain)
      # -------------------------------------------------------------
      step4_id = generate_uuid()
      step4_payload = %{
        "appliance_name" => name,
        "parent_task_id" => parent_id,
        "step" => "register_domain",
        "vcpus" => preset.vcpus,
        "memory_mb" => preset.memory_mb
      }
      insert_task(step4_id, parent_id, "register_domain", "processing", 40, step4_payload)
      update_task(parent_id, "processing", 70)

      vm_params = %{
        "name" => name,
        "vcpu" => preset.vcpus,
        "memory" => preset.memory_mb,
        "firmware" => "uefi",
        "disks" => "#{preset.disk_gb}G:#{container}",
        "disks_list" => "#{preset.disk_gb}G:#{container}",
        "cpu_model" => "host-passthrough",
        "cloud_init_userdata" => userdata_yaml,
        "cloud_init_network" => network_yaml
      }

      case Vms.create_vm(vm_params) do
        {:ok, _vm} ->
          Hydra.query(
            "UPDATE hydra.vms SET cloud_init_userdata = ?, cloud_init_network = ? WHERE name = ?;",
            [userdata_yaml, network_yaml, name]
          )

        {:error, reason} ->
          case Hydra.query("SELECT name FROM hydra.vms WHERE name = ?;", [name]) do
            {:ok, [%{"name" => _} | _]} ->
              Hydra.query(
                "UPDATE hydra.vms SET vcpu = ?, memory = ?, cloud_init_userdata = ?, cloud_init_network = ? WHERE name = ?;",
                [preset.vcpus, preset.memory_mb, userdata_yaml, network_yaml, name]
              )

            _ ->
              throw({:step_failed, step4_id, "VM registration failed: #{inspect(reason)}"})
          end
      end

      update_task(step4_id, "completed", 100)
      update_task(parent_id, "processing", 75)

      # -------------------------------------------------------------
      # Step 5: Hypervisor Boot / Power On (power_on)
      # -------------------------------------------------------------
      step5_id = generate_uuid()
      step5_payload = %{
        "appliance_name" => name,
        "parent_task_id" => parent_id,
        "step" => "power_on"
      }
      insert_task(step5_id, parent_id, "power_on", "processing", 30, step5_payload)
      update_task(parent_id, "processing", 80)

      case Vms.power_on(name) do
        {:ok, _} ->
          update_task(step5_id, "completed", 100)
          update_task(parent_id, "processing", 85)

        {:error, reason} ->
          throw({:step_failed, step5_id, "Hypervisor boot failed: #{inspect(reason)}"})
      end

      # -------------------------------------------------------------
      # Step 6: Service Verification & Auto-Pairing (verify_services)
      # -------------------------------------------------------------
      clean_ip = String.split(ip, "/") |> hd()
      step6_id = generate_uuid()
      step6_payload = %{
        "appliance_name" => name,
        "parent_task_id" => parent_id,
        "step" => "verify_services",
        "endpoint" => "https://#{clean_ip}:8443"
      }
      insert_task(step6_id, parent_id, "verify_services", "processing", 20, step6_payload)
      update_task(parent_id, "processing", 90)

      wait_for_vm_running(name, 15)

      now_ms = System.system_time(:millisecond)
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
        "deployed_at" => now_ms
      }

      json_val = Jason.encode!(deployment_data)
      Hydra.query(
        "INSERT INTO hydra.central_config (key, value, updated_at_ms) VALUES ('deployment', ?, ?);",
        [json_val, now_ms]
      )

      # Auto-enroll local cluster into Central
      local = local_cluster_record()
      Hydra.query("""
      INSERT INTO hydra.central_clusters (
        cluster_id, name, vip, port, status, node_count, vm_count,
        cpu_cores, memory_total_mb, memory_used_mb, storage_total_bytes, storage_used_bytes,
        auth_token, last_seen_ms, enrolled_at_ms, metadata
      ) VALUES (
        ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?
      );
      """, [
        local.cluster_id, local.name, local.vip, local.port, local.status,
        local.node_count, local.vm_count, local.cpu_cores, local.memory_total_mb,
        local.memory_used_mb, local.storage_total_bytes, local.storage_used_bytes,
        local.auth_token, local.last_seen_ms, local.enrolled_at_ms, local.metadata
      ])

      update_task(step6_id, "completed", 100)
      update_task(parent_id, "completed", 100)

      {:ok, deployment_data}
    rescue
      exception ->
        err_msg = "Exception during Central deployment: #{Exception.message(exception)}"
        Logger.error(err_msg)
        update_task(parent_id, "failed", 100, err_msg)
        {:error, err_msg}
    catch
      {:step_failed, failed_step_id, error_message} ->
        Logger.error("Helios Central provisioning failed at step #{failed_step_id}: #{error_message}")
        update_task(failed_step_id, "failed", 100, error_message)
        update_task(parent_id, "failed", 100, error_message)
        {:error, error_message}

      :exit, reason ->
        err_msg = "Provisioning process exited: #{inspect(reason)}"
        Logger.error("Helios Central deployment crash: #{err_msg}")
        update_task(parent_id, "failed", 100, err_msg)
        {:error, err_msg}
    end
  end

  defp wait_for_vm_running(_name, attempts) when attempts <= 0, do: :timeout
  defp wait_for_vm_running(name, attempts) do
    case Hydra.query("SELECT state FROM hydra.vms WHERE name = ?;", [name]) do
      {:ok, [%{"state" => state} | _]} when is_binary(state) ->
        if String.downcase(state) == "running" do
          :ok
        else
          Process.sleep(2000)
          wait_for_vm_running(name, attempts - 1)
        end

      _ ->
        Process.sleep(2000)
        wait_for_vm_running(name, attempts - 1)
    end
  end

  defp describe_error(reason) when is_binary(reason), do: reason
  defp describe_error({_code, msg}) when is_binary(msg), do: msg
  defp describe_error(other), do: inspect(other)

  @doc "Trigger Deployment of the Helios Central Appliance (Synchronous)"
  def deploy_appliance(params) do
    parent_id = generate_uuid()
    run_deploy_workflow(parent_id, params)
  end
end
