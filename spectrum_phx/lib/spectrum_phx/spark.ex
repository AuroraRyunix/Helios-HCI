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

  @doc """
  Call one Sidon operation on a node, through spark-daemon's typed DFS endpoint.

  Every storage call in this module funnels here. The daemon fronts Sidon's unix control
  socket with an allow-list of operations rather than a pass-through, so this cannot ask
  for anything the daemon has not agreed to -- which is the point of fronting it at all.

  `{:error, {409, message}}` means the answer will not change on a retry: a refused
  attach names the host that actually owns the vdisk. `{:error, {503, message}}` means
  it might.
  """
  def dfs(ip, op, params \\ %{}, opts \\ []) do
    payload = params |> Map.new() |> Map.put("op", op)
    post_json(ip, "/api/v1/dfs/vdisk", payload, timeout: Keyword.get(opts, :timeout, 30))
  end

  @doc """
  This node's extent store: total, available, and what Sidon is holding in it.

  `%{"node", "path", "total_bytes", "available_bytes", "egroup_bytes", "egroup_count",
  "journal_bytes"}`. The figures come from `statfs` on the store's own filesystem, so
  they describe the volume Sidon owns rather than the root filesystem it is not on.
  """
  def dfs_capacity(ip), do: dfs(ip, "capacity")

  @doc """
  Vdisks attached on this node: `%{"attached" => [%{"vdisk_id", "role", ...}]}`.

  `role` is `"owner"` or `"forwarding"`. An owner's entry carries its epoch, size and
  whether it is degraded; a forwarding entry names the node it relays to and nothing
  else, because a relay knows nothing about the disk it is passing through.
  """
  def dfs_list(ip), do: dfs(ip, "list")

  @doc "One owned vdisk in detail, including the nodes its replicas are on."
  def dfs_status(ip, vdisk_id), do: dfs(ip, "status", %{"vdisk_id" => vdisk_id})

  @doc """
  Which peers this node can reach right now.

  Reachability is not safety -- an append needs *every* replica, and an unreachable one
  fails the write rather than being skipped -- but an operator looking at a vdisk that
  will not accept writes needs to see which peer is down without reading a log.
  """
  def dfs_peers(ip), do: dfs(ip, "peers")

  @doc """
  Create a vdisk of `size_bytes`, replicated `rf` ways.

  Sparse: nothing is allocated until something writes. A vdisk is a row and a block map,
  not a device, so this is a metadata operation and returns in milliseconds -- where a
  LINSTOR placement built a kernel object on every node and needed minutes.

  An existing vdisk of the same name is an error rather than a silent adoption. That is
  precisely how a new VM ends up attached to a deleted VM's disk.
  """
  def dfs_create(ip, vdisk_id, size_bytes, opts \\ []) do
    params =
      %{"vdisk_id" => vdisk_id, "size_bytes" => size_bytes}
      |> put_present("rf", Keyword.get(opts, :rf))

    dfs(ip, "create", params, timeout: Keyword.get(opts, :timeout, 60))
  end

  @doc """
  Take ownership of a vdisk on this node and expose it as an NBD socket.

  This is the successor to `drbdadm primary`, and a stronger one. It wins the
  `(owner, epoch)` compare-and-swap in Hydra and fences every journal replica at the new
  epoch, so a previous owner that is wedged, lying about its state, or unreachable cannot
  complete another write. A promotion could only *infer* that the peer had stopped, and
  only where quorum was armed.

  A refused attach is `{:error, {409, message}}`, and the message names the host that
  holds the disk.
  """
  def dfs_attach(ip, vdisk_id), do: dfs(ip, "attach", %{"vdisk_id" => vdisk_id})

  @doc "Give up ownership and remove the socket. Drains first."
  def dfs_detach(ip, vdisk_id), do: dfs(ip, "detach", %{"vdisk_id" => vdisk_id})

  @doc """
  Delete a vdisk and free its extent groups on every replica.

  Deleting something that is already gone is a success: a rollback wants the vdisk gone,
  not proof that it once existed.
  """
  def dfs_delete(ip, vdisk_id, opts \\ []) do
    dfs(ip, "delete", %{"vdisk_id" => vdisk_id}, timeout: Keyword.get(opts, :timeout, 120))
  end

  @doc """
  Seal a vdisk: drain it, then make it permanently immutable.

  What replaced `--allow-two-primaries`. That option existed because a golden image is
  attached read-only by guests on several hosts at once, and DRBD needed every one of
  those hosts to hold Primary in order to read -- which is exactly the state that
  corrupts a device the moment anything writes. A sealed vdisk cannot reach it: reads are
  served by any node without a lease, and writes are refused by class at the NBD layer.

  The drain comes first because the drain is itself a write path, and a vdisk frozen
  around an undrained journal could never finish draining it.
  """
  def dfs_seal(ip, vdisk_id, opts \\ []) do
    dfs(ip, "seal", %{"vdisk_id" => vdisk_id}, timeout: Keyword.get(opts, :timeout, 300))
  end

  @doc """
  Grow a vdisk to `size_bytes`.

  Nothing is resized underneath: the vdisk is sparse and its map is keyed by extent
  index, so the new range simply has no entries and reads as zeroes. Only the recorded
  size changes, and only qemu needs telling.
  """
  def dfs_resize(ip, vdisk_id, size_bytes) do
    dfs(ip, "resize", %{"vdisk_id" => vdisk_id, "size_bytes" => size_bytes})
  end

  @doc "Drain a vdisk's journal into extent groups now, rather than at the high-water mark."
  def dfs_flush(ip, vdisk_id, opts \\ []) do
    dfs(ip, "flush", %{"vdisk_id" => vdisk_id}, timeout: Keyword.get(opts, :timeout, 120))
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
  def container_ensure(ip, name),
    do: post_json(ip, "/api/v1/storage/container/ensure", %{"name" => name})

  @doc "Default interface, gateway and addresses for a host."
  def host_network(ip), do: get_json(ip, "/api/v1/host/network")

  @doc "Host memory totals in MiB."
  def host_memory(ip), do: get_json(ip, "/api/v1/host/memory")

  @doc "Parsed lsblk output for a host."
  def host_disks(ip), do: get_json(ip, "/api/v1/host/disks")

  @doc """
  The host's processors: model, online logical cores, physical cores, sockets, load.

  A typed endpoint rather than `execute/3` with `nproc; grep model name /proc/cpuinfo`,
  which is how the Python console reads the same fact. Every `execute` call site is one
  more place the unsandboxed root executor has to stay trusted.
  """
  def host_cpu(ip), do: get_json(ip, "/api/v1/host/cpu")

  @doc """
  Whether the host has KVM, and its Secure Boot state.

  Secure Boot is reported and acted on by nothing. It used to gate provisioning, because
  DRBD was an out-of-tree module the kernel refuses to load unenrolled and a host with
  Secure Boot on therefore had no storage at all. Sidon is a userspace daemon.
  """
  def host_capabilities(ip), do: get_json(ip, "/api/v1/host/capabilities")

  @doc """
  Per-segment tunnel throughput across the cluster.

  Cluster-wide from whichever node is asked: the daemon walks every node and segment
  itself, so this is one call rather than a fan-out.
  """
  def urbosa_tunnels(ip), do: get_json(ip, "/api/v1/urbosa/tunnels/status")

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

  @doc """
  Everything needed to open a raw connection to a node's Spark daemon.

  `Req` covers every request/response call in this module, but an image upload is neither:
  the bytes arrive in chunks over a LiveView channel and have to be pushed onto an
  already-open request as they come. That needs the connection held across calls, so
  `SpectrumPhx.Images.UploadWriter` drives Mint directly -- and it must use exactly the
  same port and mutual-TLS material as everything else here rather than assembling its
  own.

  `:port` and `:transport_opts` only. The connection itself belongs to the caller,
  because only the caller knows when it is finished with it.
  """
  def connection_settings do
    %{port: @port, transport_opts: tls_opts()}
  end

  @doc """
  The path that streams a request body straight into a vdisk.

  No allow-list is needed on the far side, and that is the improvement over the device
  form this replaced: the caller names a *vdisk*, and the daemon derives the socket from
  it. There is no path in the request, so a caller cannot name a file. It is still
  encoded here rather than concatenated, so a name is never a request-line fragment.
  """
  def vdisk_write_path(vdisk_id) do
    "/api/v1/dfs/write?vdisk=" <> URI.encode_www_form(vdisk_id)
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
      {:ok, %Req.Response{status: status, body: body}} when status in 200..299 ->
        {:ok, body}

      {:ok, %Req.Response{status: status, body: %{"error" => message}}} ->
        {:error, {status, message}}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # An omitted option is not the same as a null: leaving the key out lets the daemon apply
  # its own default (every configured node, the default storage pool) rather than being
  # handed a value the caller does not actually have.
  defp put_present(payload, _key, nil), do: payload
  defp put_present(payload, _key, []), do: payload
  defp put_present(payload, key, value), do: Map.put(payload, key, value)

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
