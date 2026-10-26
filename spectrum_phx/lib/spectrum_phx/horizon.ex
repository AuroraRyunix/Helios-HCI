defmodule SpectrumPhx.Horizon do
  @moduledoc """
  Helios Horizon VDI: Virtual Desktop Pools and Session Management.
  Queries and manages `hydra.horizon_pools` and `hydra.horizon_sessions`.
  """

  alias SpectrumPhx.Hydra

  @doc "List all Horizon desktop pools"
  def list_pools do
    cql = """
    SELECT pool_name, template_vm, pool_type, protocol, desktop_port,
           min_desktops, max_desktops, vcpu, memory_mb, network_name,
           domain_join, entitled_groups, power_policy, idle_timeout_min,
           state, created_at_ms, updated_at_ms
    FROM hydra.horizon_pools;
    """

    case Hydra.query(cql) do
      {:ok, rows} when is_list(rows) -> {:ok, Enum.map(rows, &normalize_pool/1)}
      error -> error
    end
  end

  @doc "Get a single Horizon desktop pool by name"
  def get_pool(pool_name) do
    cql = "SELECT * FROM hydra.horizon_pools WHERE pool_name = ?;"

    case Hydra.query(cql, [pool_name]) do
      {:ok, [row | _]} -> {:ok, normalize_pool(row)}
      {:ok, []} -> {:error, :not_found}
      error -> error
    end
  end

  @doc "Create or update a desktop pool"
  def create_pool(attrs) do
    pool_name = attrs["pool_name"] || attrs[:pool_name]
    template_vm = attrs["template_vm"] || attrs[:template_vm] || ""
    pool_type = attrs["pool_type"] || attrs[:pool_type] || "linked_clone"
    protocol = attrs["protocol"] || attrs[:protocol] || "BLAST"
    desktop_port = attrs["desktop_port"] || attrs[:desktop_port] || 22443
    min_desktops = attrs["min_desktops"] || attrs[:min_desktops] || 1
    max_desktops = attrs["max_desktops"] || attrs[:max_desktops] || 10
    vcpu = attrs["vcpu"] || attrs[:vcpu] || 2
    memory_mb = attrs["memory_mb"] || attrs[:memory_mb] || 4096
    network_name = attrs["network_name"] || attrs[:network_name] || "vdi-network"
    domain_join = attrs["domain_join"] || attrs[:domain_join] || ""
    entitled_groups = attrs["entitled_groups"] || attrs[:entitled_groups] || "Domain Users"
    power_policy = attrs["power_policy"] || attrs[:power_policy] || "power_off"
    idle_timeout_min = attrs["idle_timeout_min"] || attrs[:idle_timeout_min] || 60
    state = attrs["state"] || attrs[:state] || "active"
    now_ms = System.system_time(:millisecond)

    if is_nil(pool_name) or pool_name == "" do
      {:error, "pool_name is required"}
    else
      cql = """
      INSERT INTO hydra.horizon_pools (
        pool_name, template_vm, pool_type, protocol, desktop_port,
        min_desktops, max_desktops, vcpu, memory_mb, network_name,
        domain_join, entitled_groups, power_policy, idle_timeout_min,
        state, created_at_ms, updated_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
      """

      params = [
        pool_name, template_vm, pool_type, protocol, desktop_port,
        min_desktops, max_desktops, vcpu, memory_mb, network_name,
        domain_join, entitled_groups, power_policy, idle_timeout_min,
        state, now_ms, now_ms
      ]

      case Hydra.query(cql, params) do
        {:ok, _} -> {:ok, pool_name}
        error -> error
      end
    end
  end

  @doc "Delete a desktop pool"
  def delete_pool(pool_name) do
    Hydra.query("DELETE FROM hydra.horizon_pools WHERE pool_name = ?;", [pool_name])
  end

  @doc "List active Horizon VDI sessions"
  def list_sessions do
    cql = """
    SELECT session_id, pool_name, vm_name, username, protocol,
           host_ip, guacd_port, client_ip, state, connected_at_ms, last_active_ms
    FROM hydra.horizon_sessions;
    """

    case Hydra.query(cql) do
      {:ok, rows} when is_list(rows) -> {:ok, Enum.map(rows, &normalize_session/1)}
      error -> error
    end
  end

  @doc "Disconnect a VDI session"
  def disconnect_session(session_id) do
    Hydra.query("DELETE FROM hydra.horizon_sessions WHERE session_id = ?;", [session_id])
  end

  defp normalize_pool(row) do
    %{
      pool_name: row["pool_name"],
      template_vm: row["template_vm"],
      pool_type: row["pool_type"] || "linked_clone",
      protocol: row["protocol"] || "BLAST",
      desktop_port: row["desktop_port"] || 22443,
      min_desktops: row["min_desktops"] || 1,
      max_desktops: row["max_desktops"] || 10,
      vcpu: row["vcpu"] || 2,
      memory_mb: row["memory_mb"] || 4096,
      network_name: row["network_name"],
      domain_join: row["domain_join"],
      entitled_groups: row["entitled_groups"],
      power_policy: row["power_policy"] || "power_off",
      idle_timeout_min: row["idle_timeout_min"] || 60,
      state: row["state"] || "active",
      created_at_ms: row["created_at_ms"] || 0,
      updated_at_ms: row["updated_at_ms"] || 0
    }
  end

  defp normalize_session(row) do
    %{
      session_id: row["session_id"],
      pool_name: row["pool_name"],
      vm_name: row["vm_name"],
      username: row["username"],
      protocol: row["protocol"] || "BLAST",
      host_ip: row["host_ip"],
      guacd_port: row["guacd_port"] || 4822,
      client_ip: row["client_ip"],
      state: row["state"] || "active",
      connected_at_ms: row["connected_at_ms"] || 0,
      last_active_ms: row["last_active_ms"] || 0
    }
  end
end
