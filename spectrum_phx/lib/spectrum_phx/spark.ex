defmodule SpectrumPhx.Spark do
  @moduledoc """
  Client for the Spark daemon's mutual-TLS control API on port 9099.

  Spark is the only component permitted to run commands on a hypervisor, so every
  host-level action taken by the web tier goes through here.

  Note the daemon executes what it is given with a shell, as root. Callers must therefore
  never build a command from unvalidated input; `escape/1` is provided for the cases that
  genuinely need to interpolate a value.
  """

  @port 9099
  @ca "/etc/hci/spark/certs/ca.crt"
  @client_cert "/root/.certs/client.crt"
  @client_key "/root/.certs/client.key"

  @doc """
  Execute a command on `ip` and return `{rc, stdout, stderr}`.

  On transport failure this returns `{-1, "", reason}` rather than raising, matching how
  callers treat an unreachable node as a degraded rather than exceptional condition.
  """
  def execute(ip, command, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 45)
    url = "https://" <> ip <> ":" <> Integer.to_string(@port) <> "/api/v1/execute"
    body = %{"command" => command, "timeout" => timeout}

    case post(url, body, timeout) do
      {:ok, %{"returncode" => rc, "stdout" => out, "stderr" => err}} -> {rc, out, err}
      {:ok, other} -> {-1, "", "unexpected response: " <> inspect(other)}
      {:error, reason} -> {-1, "", inspect(reason)}
    end
  end

  @doc "GET a JSON endpoint on a node's Spark daemon."
  def get_json(ip, path, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 15)
    url = "https://" <> ip <> ":" <> Integer.to_string(@port) <> path

    case Req.get(url,
           connect_options: [transport_opts: tls_opts()],
           receive_timeout: timeout * 1000
         ) do
      {:ok, %Req.Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Req.Response{status: status}} -> {:error, {:http, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Fetch one node's published status document."
  def node_status(ip), do: get_json(ip, "/api/v1/node/status")

  @doc "Run a command on every configured node, concurrently."
  def execute_all(command, opts \\ []) do
    SpectrumPhx.Cluster.Config.node_ips()
    |> Task.async_stream(fn ip -> {ip, execute(ip, command, opts)} end,
      timeout: (Keyword.get(opts, :timeout, 45) + 15) * 1000,
      on_timeout: :kill_task
    )
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, _} -> {nil, {-1, "", "task timeout"}}
    end)
    |> Enum.reject(fn {ip, _} -> is_nil(ip) end)
    |> Map.new()
  end

  @doc """
  Single-quote a value for safe inclusion in a shell command.

  POSIX shells have no escape inside single quotes, so an embedded quote is emitted by
  closing the quote, adding a backslash-escaped quote, and reopening it.

  The replacement literal must be `"'\\''"`. Writing `"'\''"` looks equivalent but
  Elixir resolves `\'` to a bare quote, yielding `'''` -- which closes the quoting and
  leaves the remainder of the value as live shell input. Spark runs it as root.
  """
  def escape(value) when is_binary(value) do
    "'" <> String.replace(value, "'", "'\\''") <> "'"
  end

  # ---------------------------------------------------------------------------
  # Typed domain API (see docs/spark_api.md).
  #
  # These replace building shell strings and posting them to /api/v1/execute.
  # Parameters are values, never command fragments, and responses are parsed JSON
  # rather than captured stdout -- so callers stop being coupled to the command.

  @doc "Network interfaces attached to a VM."
  def vm_interfaces(ip, name), do: get_json(ip, "/api/v1/vm/" <> name <> "/interfaces")

  @doc "Console (VNC/SPICE) details for a VM."
  def vm_console(ip, name), do: get_json(ip, "/api/v1/vm/" <> name <> "/console")

  @doc "Runtime info for a VM: state, vcpus, memory."
  def vm_info(ip, name), do: get_json(ip, "/api/v1/vm/" <> name <> "/info")

  @doc """
  Define a VM from libvirt domain XML.

  The XML is base64-encoded so it never passes through a shell; the daemon decodes it
  to a temp file and defines from that path.
  """
  def vm_define(ip, name, xml) when is_binary(xml) do
    post_json(ip, "/api/v1/vm/define", %{"name" => name, "xml_b64" => Base.encode64(xml)})
  end

  @doc "Undefine a VM, optionally preserving its NVRAM."
  def vm_undefine(ip, name, keep_nvram \\ true) do
    post_json(ip, "/api/v1/vm/undefine", %{"name" => name, "keep_nvram" => keep_nvram})
  end

  @doc "Power action on a VM: start, destroy, reboot, shutdown or reset."
  def vm_power(ip, name, action) when action in ~w(start destroy reboot shutdown reset) do
    post_json(ip, "/api/v1/vm/" <> name <> "/power", %{"action" => action})
  end

  @doc "Parsed DRBD status, optionally for a single resource."
  def drbd_status(ip, resource \\ nil) do
    path = if resource, do: "/api/v1/storage/drbd/status?resource=" <> resource,
                        else: "/api/v1/storage/drbd/status"
    get_json(ip, path)
  end

  @doc """
  Set a DRBD resource's role, returning the role actually achieved.

  The result must be checked: a promotion that does not yield Primary means the peer
  still holds it, which is the condition that previously allowed one VM to be started
  on two hosts and corrupt its disk.
  """
  def drbd_role(ip, resource, role, force \\ false) when role in ~w(primary secondary) do
    post_json(ip, "/api/v1/storage/drbd/role", %{
      "resource" => resource,
      "role" => role,
      "force" => force
    })
  end

  @doc "Existence, block-ness and size of a device path."
  def device_info(ip, path), do: get_json(ip, "/api/v1/storage/device?path=" <> URI.encode(path))

  @doc "Set ownership and mode on a device, from the daemon's allowlists."
  def device_prepare(ip, path, owner, mode) do
    post_json(ip, "/api/v1/storage/device/prepare", %{
      "path" => path,
      "owner" => owner,
      "mode" => mode
    })
  end

  @doc "Flush buffers to a block device before demoting or detaching it."
  def device_flush(ip, path), do: post_json(ip, "/api/v1/storage/device/flush", %{"path" => path})

  @doc "Whether a storage container path is mounted."
  def container_mounted?(ip, path) do
    case get_json(ip, "/api/v1/storage/container/mounted?path=" <> URI.encode(path)) do
      {:ok, %{"mounted" => value}} -> value
      _ -> false
    end
  end

  @doc "Ensure a storage container directory exists; returns its path."
  def container_ensure(ip, name), do: post_json(ip, "/api/v1/storage/container/ensure", %{"name" => name})

  @doc "Default interface, gateway and addresses for a host."
  def host_network(ip), do: get_json(ip, "/api/v1/host/network")

  @doc "Host memory totals in MiB."
  def host_memory(ip), do: get_json(ip, "/api/v1/host/memory")

  @doc "Parsed lsblk output for a host."
  def host_disks(ip), do: get_json(ip, "/api/v1/host/disks")

  @doc "Whether the host has KVM, the DRBD module, and Secure Boot state."
  def host_capabilities(ip), do: get_json(ip, "/api/v1/host/capabilities")

  @doc "Current dnsmasq DHCP leases."
  def dhcp_leases(ip), do: get_json(ip, "/api/v1/host/dhcp-leases")

  @doc "Reboot a host. Requires explicit confirmation."
  def host_reboot(ip), do: post_json(ip, "/api/v1/host/reboot", %{"confirm" => true})

  @doc "ScyllaDB ring membership."
  def db_ring(ip), do: get_json(ip, "/api/v1/db/ring")

  @doc """
  Start a ScyllaDB repair. Returns as soon as it is started, not when it completes.

  A repair is what actually populates new replicas after the replication factor is
  raised; ALTER KEYSPACE alone leaves the cluster reporting redundancy it does not have.
  """
  def db_repair(ip, keyspace \\ "hydra", primary_range \\ true) do
    post_json(ip, "/api/v1/db/repair", %{"keyspace" => keyspace, "primary_range" => primary_range})
  end

  @doc false
  def post_json(ip, path, payload, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 30)
    url = "https://" <> ip <> ":" <> Integer.to_string(@port) <> path

    case Req.post(url,
           json: payload,
           connect_options: [transport_opts: tls_opts()],
           receive_timeout: timeout * 1000
         ) do
      {:ok, %Req.Response{status: status, body: body}} when status in 200..299 -> {:ok, body}
      {:ok, %Req.Response{status: status, body: %{"error" => message}}} -> {:error, {status, message}}
      {:ok, %Req.Response{status: status}} -> {:error, {:http, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp post(url, body, timeout) do
    case Req.post(url,
           json: body,
           connect_options: [transport_opts: tls_opts()],
           receive_timeout: (timeout + 15) * 1000
         ) do
      {:ok, %Req.Response{status: 200, body: resp}} -> {:ok, resp}
      {:ok, %Req.Response{status: status}} -> {:error, {:http, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp tls_opts do
    [
      verify: :verify_peer,
      cacertfile: @ca,
      certfile: @client_cert,
      keyfile: @client_key,
      # The cluster CA issues per-node certs addressed by IP; hostname verification is
      # therefore not meaningful here. Peer verification against the CA still applies.
      customize_hostname_check: [match_fun: fn _ref_id, _presented -> true end]
    ]
  end
end
