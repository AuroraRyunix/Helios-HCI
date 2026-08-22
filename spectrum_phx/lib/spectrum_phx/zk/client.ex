defmodule SpectrumPhx.Zk.Client do
  @moduledoc """
  A minimal ZooKeeper 3.x wire-protocol client, held open by a single GenServer.

  This is a port of `helios_zk.py`. It exists for the same reason: the rest of the stack
  talks to ZooKeeper only through the four-letter-word commands (`stat` over a raw
  socket), which are read-only server diagnostics. Publishing and reading cluster state
  needs the real client protocol -- in particular *ephemeral* znodes, whose lifetime is
  bound to a live session, so a node that dies has its entry removed by the ensemble
  rather than by anyone noticing and cleaning up.

  Two properties matter more than feature coverage:

    * **The session must not lapse.** Every node's registration under `/helios/nodes` is
      ephemeral, so a missed keepalive silently deletes it and the node reads as absent.
      Pings run on a timer at a third of the *negotiated* session timeout.

    * **Boot must not depend on ZooKeeper.** `start_link/1` always succeeds; connecting
      happens in `handle_continue/2` and, on failure, retries in the background with
      backoff. A cluster that is being brought up cold has no ZooKeeper yet, and the web
      tier has to come up and report that rather than fail alongside it.

  Requests are serialised by virtue of being `GenServer.call/3`s: exactly one is in
  flight at a time, and its reply is correlated by xid. Replies with xid `-1` are watch
  notifications and are skipped; a ping reply arriving while another request is
  outstanding is skipped too.

  Wire format (all integers big-endian, signed):

      string : int32 length + UTF-8 bytes   (-1 means null)
      buffer : int32 length + raw bytes     (-1 means null)
      request: int32 frame_len + int32 xid + int32 opcode + payload
      reply  : int32 frame_len + int32 xid + int64 zxid + int32 err + payload

  The int32 frame length is handled by `:gen_tcp`'s `packet: 4` framing rather than by
  hand, which is the same encoding and removes the partial-read bookkeeping.
  """
  use GenServer
  require Logger

  # -- protocol constants -----------------------------------------------------

  @op_create 1
  @op_delete 2
  @op_exists 3
  @op_get_data 4
  @op_set_data 5
  @op_get_children 8
  @op_ping 11
  @op_close -11

  @xid_ping -2
  @xid_watch -1

  @err_ok 0
  @err_no_node -101
  @err_node_exists -110
  @err_not_empty -111
  @err_session_expired -112

  @persistent 0
  @ephemeral 1

  # Reply header: xid(4) + zxid(8) + err(4).
  @header_size 16

  # ACL: world:anyone with all permissions (0x1f). Access control is the network
  # boundary's job here, exactly as the existing four-letter-word usage assumes.
  @acl_perms 0x1F

  # Bound on a single frame, so a corrupt length field cannot make us allocate without
  # limit. Comfortably above ZooKeeper's 1 MB `jute.maxbuffer` default.
  @max_frame 4_194_304

  @default_hosts ["127.0.0.1"]
  @default_port 2181
  @default_session_timeout_ms 15_000
  @default_connect_timeout_ms 3_000
  @default_operation_timeout_ms 10_000

  # Must exceed a connect attempt plus one operation, or a caller would give up on a
  # client that is about to answer.
  @call_timeout 20_000
  @status_call_timeout 5_000

  @min_ping_interval_ms 1_000
  @max_backoff_ms 30_000
  # Delay between hosts within one sweep of the host list. Short, because a refused
  # connection is the expected case while the cluster is starting.
  @intra_sweep_delay_ms 200

  @type client :: GenServer.server()
  @type reason ::
          :no_node
          | :node_exists
          | :not_empty
          | :session_expired
          | :not_connected
          | :malformed_reply
          | :xid_mismatch
          | {:socket, term()}
          | {:zk_error, integer()}

  # -- lifecycle --------------------------------------------------------------

  @doc """
  Start the client.

  Options:

    * `:hosts` - list of ZooKeeper addresses. Defaults to the local node first, then the
      configured cluster nodes, mirroring `get_zk_hosts()` in spark-daemon.
    * `:port` - defaults to `2181`.
    * `:session_timeout_ms` - requested session timeout, default `15_000`. The server may
      negotiate a different value; the ping interval follows whatever was negotiated.
    * `:connect_timeout_ms` - per-host TCP connect and handshake timeout, default `3_000`.
    * `:operation_timeout_ms` - how long to wait for one reply, default `10_000`.
    * `:name` - registered name, default `#{inspect(__MODULE__)}`.

  Always returns `{:ok, pid}` if the process itself can start; an unreachable ZooKeeper
  is not a startup failure.
  """
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # -- public API -------------------------------------------------------------

  @doc """
  Create a znode, returning `{:ok, created_path}`.

  Options:

    * `:ephemeral` - bind the node's lifetime to this session (default `false`).
    * `:makepath` - create missing persistent parents first (default `false`).

  Returns `{:error, :node_exists}` when the path is already taken.
  """
  @spec create(client(), String.t(), binary(), keyword()) ::
          {:ok, String.t()} | {:error, reason()}
  def create(client, path, data \\ <<>>, opts \\ []) do
    with :ok <- maybe_makepath(client, path, opts) do
      ephemeral? = Keyword.get(opts, :ephemeral, false)
      call(client, {:create, path, data, ephemeral?})
    end
  end

  @doc "Whether a znode exists. `{:error, _}` means the question could not be asked."
  @spec exists(client(), String.t()) :: {:ok, boolean()} | {:error, reason()}
  def exists(client, path), do: call(client, {:exists, path})

  @doc "Read a znode's data as a binary."
  @spec get(client(), String.t()) :: {:ok, binary()} | {:error, reason()}
  def get(client, path), do: call(client, {:get, path})

  @doc "Write a znode's data. `version` of `-1` means unconditional."
  @spec set(client(), String.t(), binary(), integer()) :: :ok | {:error, reason()}
  def set(client, path, data, version \\ -1), do: call(client, {:set, path, data, version})

  @doc "List a znode's children (names only, not full paths)."
  @spec get_children(client(), String.t()) :: {:ok, [String.t()]} | {:error, reason()}
  def get_children(client, path), do: call(client, {:get_children, path})

  @doc """
  Delete a znode.

  Deleting an absent node is `:ok`, matching the reference client: callers use this to
  make sure something is gone, not to assert that it was there.
  """
  @spec delete(client(), String.t(), integer()) :: :ok | {:error, reason()}
  def delete(client, path, version \\ -1), do: call(client, {:delete, path, version})

  @doc "Create every missing persistent parent of `path`. Idempotent."
  @spec ensure_path(client(), String.t()) :: :ok | {:error, reason()}
  def ensure_path(client, path) do
    path
    |> String.split("/", trim: true)
    |> Enum.reduce_while({:ok, ""}, fn part, {:ok, prefix} ->
      current = prefix <> "/" <> part

      case create(client, current, <<>>) do
        {:ok, _created} -> {:cont, {:ok, current}}
        {:error, :node_exists} -> {:cont, {:ok, current}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Create an ephemeral znode, or overwrite it if this session already owns it.

  This is what a publisher loop wants: reconnecting with a resumed session finds its own
  node still present, and a plain `create/4` would fail with `:node_exists`.
  """
  @spec upsert_ephemeral(client(), String.t(), binary()) :: :ok | {:error, reason()}
  def upsert_ephemeral(client, path, data) do
    case create(client, path, data, ephemeral: true, makepath: true) do
      {:ok, _created} -> :ok
      {:error, :node_exists} -> set(client, path, data)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Whether a session is currently established.

  Never raises: a client that is missing, dead, or wedged reads as not connected.
  """
  @spec connected?(client()) :: boolean()
  def connected?(client) do
    GenServer.call(client, :connected?, @status_call_timeout)
  catch
    :exit, _ -> false
  end

  @doc "The host the session is established against, or `nil`."
  @spec connected_host(client()) :: String.t() | nil
  def connected_host(client) do
    GenServer.call(client, :connected_host, @status_call_timeout)
  catch
    :exit, _ -> nil
  end

  # -- GenServer --------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %{
      hosts: normalize_hosts(Keyword.get(opts, :hosts)),
      port: Keyword.get(opts, :port, @default_port),
      session_timeout_ms: Keyword.get(opts, :session_timeout_ms, @default_session_timeout_ms),
      connect_timeout: Keyword.get(opts, :connect_timeout_ms, @default_connect_timeout_ms),
      op_timeout: Keyword.get(opts, :operation_timeout_ms, @default_operation_timeout_ms),
      negotiated_ms: nil,
      socket: nil,
      host: nil,
      xid: 0,
      session_id: 0,
      passwd: <<0::size(128)>>,
      host_index: 0,
      sweeps: 0,
      ping_ref: nil,
      reconnect_ref: nil,
      reported_down?: false
    }

    # Connecting from a continue rather than from init/1 keeps `start_link/1` from
    # blocking the supervisor: the caller is acked before this runs.
    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state), do: {:noreply, attempt_connect(state)}

  @impl true
  def handle_call(:connected?, _from, state), do: {:reply, state.socket != nil, state}

  def handle_call(:connected_host, _from, state), do: {:reply, state.host, state}

  def handle_call({:create, path, data, ephemeral?}, _from, state) do
    respond(state, @op_create, encode_create(path, data, ephemeral?), fn reply ->
      {:ok, decode_create_response(reply)}
    end)
  end

  def handle_call({:exists, path}, _from, state) do
    case request(state, @op_exists, encode_path_watch(path, false)) do
      {:ok, _reply, state} -> {:reply, {:ok, true}, state}
      {:error, :no_node, state} -> {:reply, {:ok, false}, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, path}, _from, state) do
    respond(state, @op_get_data, encode_path_watch(path, false), fn reply ->
      {:ok, decode_get_data_response(reply)}
    end)
  end

  def handle_call({:set, path, data, version}, _from, state) do
    respond(state, @op_set_data, encode_set_data(path, data, version), fn _reply -> :ok end)
  end

  def handle_call({:get_children, path}, _from, state) do
    respond(state, @op_get_children, encode_path_watch(path, false), fn reply ->
      {:ok, decode_children_response(reply)}
    end)
  end

  def handle_call({:delete, path, version}, _from, state) do
    case request(state, @op_delete, encode_delete(path, version)) do
      {:ok, _reply, state} -> {:reply, :ok, state}
      # Absent is the desired end state, so report success.
      {:error, :no_node, state} -> {:reply, :ok, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(:ping, %{socket: nil} = state), do: {:noreply, state}

  def handle_info(:ping, state) do
    case request(%{state | ping_ref: nil}, @op_ping, <<>>) do
      {:ok, _reply, state} -> {:noreply, schedule_ping(state)}
      # The connection is already dropped and a reconnect scheduled by request/3.
      {:error, _reason, state} -> {:noreply, state}
    end
  end

  def handle_info(:reconnect, state) do
    {:noreply, attempt_connect(%{state | reconnect_ref: nil})}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{socket: socket} = state) when socket != nil do
    # Best effort: ending the session deletes our ephemeral nodes immediately rather
    # than leaving them to expire on the server's timeout.
    request(state, @op_close, <<>>, expect_reply: false)
    :gen_tcp.close(socket)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  # -- connection -------------------------------------------------------------

  defp attempt_connect(%{hosts: []} = state), do: state

  defp attempt_connect(state) do
    host = Enum.at(state.hosts, rem(state.host_index, length(state.hosts)))

    case handshake(host, state) do
      {:ok, socket, response} ->
        Logger.info(
          "[ZK] Session #{inspect(response.session_id, base: :hex)} established with #{host}:#{state.port} " <>
            "(timeout #{response.negotiated_timeout_ms}ms)."
        )

        %{
          state
          | socket: socket,
            host: host,
            session_id: response.session_id,
            passwd: response.passwd,
            negotiated_ms: response.negotiated_timeout_ms,
            sweeps: 0,
            reported_down?: false
        }
        |> schedule_ping()

      {:error, reason} ->
        log_connect_failure(state, host, reason)

        # A refused session (`session_id == 0`) means the server has forgotten us. Keep
        # resuming a dead session and we would never reconnect, so drop the credentials.
        state =
          if reason == :session_expired do
            %{state | session_id: 0, passwd: <<0::size(128)>>}
          else
            state
          end

        next_index = state.host_index + 1
        wrapped? = rem(next_index, length(state.hosts)) == 0
        sweeps = if wrapped?, do: state.sweeps + 1, else: state.sweeps
        delay = if wrapped?, do: backoff(sweeps), else: @intra_sweep_delay_ms

        schedule_reconnect(
          %{state | host_index: next_index, sweeps: sweeps, reported_down?: true},
          delay
        )
    end
  end

  defp handshake(host, state) do
    opts = [
      :binary,
      packet: 4,
      packet_size: @max_frame,
      active: false,
      nodelay: true
    ]

    case :gen_tcp.connect(String.to_charlist(host), state.port, opts, state.connect_timeout) do
      {:ok, socket} ->
        request = encode_connect_request(state.session_timeout_ms, state.session_id, state.passwd)

        with :ok <- :gen_tcp.send(socket, request),
             {:ok, frame} <- :gen_tcp.recv(socket, 0, state.connect_timeout),
             {:ok, response} <- decode_connect_response(frame),
             :ok <- validate_session(response) do
          {:ok, socket, response}
        else
          {:error, reason} ->
            :gen_tcp.close(socket)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, {:socket, reason}}
    end
  end

  defp validate_session(%{session_id: 0}), do: {:error, :session_expired}
  defp validate_session(_response), do: :ok

  # Drop the socket and arrange to come back. The session id and password are kept so
  # the next handshake attempts to *resume* the session; if it is still alive on the
  # server our ephemeral nodes survive the blip.
  defp drop_connection(state, opts \\ []) do
    if state.socket, do: :gen_tcp.close(state.socket)

    state =
      if Keyword.get(opts, :reset_session, false) do
        %{state | session_id: 0, passwd: <<0::size(128)>>}
      else
        state
      end

    if state.host do
      Logger.warning("[ZK] Lost the session with #{state.host}; reconnecting.")
    end

    %{state | socket: nil, host: nil, sweeps: 0, reported_down?: true}
    |> cancel_timer(:ping_ref)
    |> schedule_reconnect(@intra_sweep_delay_ms)
  end

  defp schedule_ping(state) do
    interval = max(@min_ping_interval_ms, div(state.negotiated_ms || state.session_timeout_ms, 3))

    state
    |> cancel_timer(:ping_ref)
    |> Map.put(:ping_ref, Process.send_after(self(), :ping, interval))
  end

  defp schedule_reconnect(state, delay) do
    state
    |> cancel_timer(:reconnect_ref)
    |> Map.put(:reconnect_ref, Process.send_after(self(), :reconnect, delay))
  end

  defp cancel_timer(state, key) do
    case Map.get(state, key) do
      nil ->
        state

      ref ->
        Process.cancel_timer(ref)
        Map.put(state, key, nil)
    end
  end

  defp backoff(sweeps) when sweeps <= 1, do: 1_000
  defp backoff(sweeps), do: min(@max_backoff_ms, 1_000 * Integer.pow(2, min(sweeps - 1, 5)))

  defp log_connect_failure(state, host, reason) do
    message = "[ZK] Cannot reach #{host}:#{state.port} (#{inspect(reason)})."

    # Log the transition loudly, then fall back to debug: a cold cluster can spend
    # minutes with no ZooKeeper and should not fill the log with it.
    if state.reported_down? do
      Logger.debug(message)
    else
      Logger.warning(message <> " Retrying in the background.")
    end
  end

  # -- request/reply ----------------------------------------------------------

  defp respond(state, opcode, payload, decoder) do
    case request(state, opcode, payload) do
      {:ok, reply, state} ->
        result =
          try do
            decoder.(reply)
          rescue
            _ -> {:error, :malformed_reply}
          end

        {:reply, result, state}

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp request(state, opcode, payload, opts \\ [])

  defp request(%{socket: nil} = state, _opcode, _payload, _opts) do
    {:error, :not_connected, state}
  end

  defp request(state, opcode, payload, opts) do
    {xid, state} =
      if opcode == @op_ping do
        {@xid_ping, state}
      else
        next = state.xid + 1
        {next, %{state | xid: next}}
      end

    case :gen_tcp.send(state.socket, encode_request(xid, opcode, payload)) do
      :ok ->
        if Keyword.get(opts, :expect_reply, true) do
          await_reply(state, xid, opcode)
        else
          {:ok, <<>>, state}
        end

      {:error, reason} ->
        {:error, {:socket, reason}, drop_connection(state)}
    end
  end

  defp await_reply(state, xid, opcode) do
    case :gen_tcp.recv(state.socket, 0, state.op_timeout) do
      {:ok, frame} ->
        case decode_reply_header(frame) do
          # A watch notification. We register no watches, but the server may still send
          # one (session/connection events), and it must not be read as our reply.
          {:ok, @xid_watch, _zxid, _err} ->
            await_reply(state, xid, opcode)

          # A ping reply that arrived while a real request was outstanding.
          {:ok, @xid_ping, _zxid, _err} when opcode != @op_ping ->
            await_reply(state, xid, opcode)

          {:ok, ^xid, _zxid, @err_ok} ->
            {:ok, frame, state}

          {:ok, ^xid, _zxid, @err_session_expired} ->
            {:error, :session_expired, drop_connection(state, reset_session: true)}

          {:ok, ^xid, _zxid, err} ->
            {:error, error_reason(err), state}

          # The stream is out of step with our xid counter; nothing after this point can
          # be trusted to belong to the request that asked for it.
          {:ok, _other, _zxid, _err} ->
            {:error, :xid_mismatch, drop_connection(state)}

          :error ->
            {:error, :malformed_reply, drop_connection(state)}
        end

      {:error, reason} ->
        {:error, {:socket, reason}, drop_connection(state)}
    end
  end

  defp call(client, message) do
    GenServer.call(client, message, @call_timeout)
  catch
    :exit, reason -> {:error, {:client_unavailable, reason}}
  end

  defp maybe_makepath(client, path, opts) do
    if Keyword.get(opts, :makepath, false) do
      ensure_path(client, parent_of(path))
    else
      :ok
    end
  end

  defp parent_of(path) do
    case String.split(path, "/") do
      [_single] ->
        "/"

      parts ->
        case parts |> Enum.drop(-1) |> Enum.join("/") do
          "" -> "/"
          parent -> parent
        end
    end
  end

  defp normalize_hosts(nil), do: default_hosts()
  defp normalize_hosts(host) when is_binary(host), do: [host]

  defp normalize_hosts(hosts) when is_list(hosts) do
    case hosts |> Enum.filter(&is_binary/1) |> Enum.uniq() do
      [] -> @default_hosts
      list -> list
    end
  end

  defp normalize_hosts(_other), do: @default_hosts

  # Local node first, so a healthy node prefers itself; this mirrors `get_zk_hosts()` in
  # spark-daemon and the host list `cluster status` builds.
  defp default_hosts do
    # An explicit override wins outright, so the app can be pointed at a remote ensemble
    # during development without a local /etc/hci/cluster.json.
    case Application.get_env(:spectrum_phx, :zk_hosts) do
      hosts when is_list(hosts) and hosts != [] ->
        normalize_hosts(hosts)

      _ ->
        default_hosts_from_cluster()
    end
  end

  defp default_hosts_from_cluster do
    ips =
      try do
        SpectrumPhx.Cluster.Config.node_ips()
      rescue
        _ -> []
      catch
        :exit, _ -> []
      end

    normalize_hosts(@default_hosts ++ Enum.reject(ips, &(&1 in @default_hosts)))
  end

  # -- wire encoding ----------------------------------------------------------
  #
  # These are public so the encoding can be tested directly rather than through a
  # socket, but they are not part of the module's interface.

  @doc false
  def encode_string(nil), do: <<-1::32-signed>>
  def encode_string(value) when is_binary(value), do: <<byte_size(value)::32-signed>> <> value

  @doc false
  def encode_buffer(nil), do: <<-1::32-signed>>
  def encode_buffer(value) when is_binary(value), do: <<byte_size(value)::32-signed>> <> value

  @doc false
  def decode_string(binary, offset) when is_binary(binary) and is_integer(offset) do
    <<_skipped::binary-size(^offset), length::32-signed, rest::binary>> = binary

    if length < 0 do
      {nil, offset + 4}
    else
      <<value::binary-size(^length), _::binary>> = rest
      {value, offset + 4 + length}
    end
  end

  @doc false
  def decode_buffer(binary, offset), do: decode_string(binary, offset)

  @doc false
  def encode_request(xid, opcode, payload) do
    <<xid::32-signed, opcode::32-signed>> <> payload
  end

  # Prefix a request or reply body with its int32 length, as `packet: 4` does.
  @doc false
  def frame(body) when is_binary(body), do: <<byte_size(body)::32-signed>> <> body

  @doc false
  def decode_reply_header(<<xid::32-signed, zxid::64-signed, err::32-signed, _rest::binary>>) do
    {:ok, xid, zxid, err}
  end

  def decode_reply_header(_frame), do: :error

  @doc false
  def encode_connect_request(session_timeout_ms, session_id, passwd) do
    # protocolVersion, lastZxidSeen, timeOut, sessionId, passwd, readOnly
    <<0::32-signed, 0::64-signed, session_timeout_ms::32-signed, session_id::64-signed>> <>
      encode_buffer(passwd) <> <<0::8>>
  end

  @doc false
  def decode_connect_response(frame) when byte_size(frame) >= @header_size do
    <<_protocol::32-signed, negotiated::32-signed, session_id::64-signed, _rest::binary>> = frame
    {passwd, _offset} = decode_buffer(frame, 16)

    {:ok,
     %{
       negotiated_timeout_ms: negotiated,
       session_id: session_id,
       passwd: passwd || <<0::size(128)>>
     }}
  rescue
    _ -> {:error, :malformed_connect_response}
  end

  def decode_connect_response(_frame), do: {:error, :malformed_connect_response}

  @doc false
  def encode_acl do
    <<1::32-signed, @acl_perms::32-signed>> <> encode_string("world") <> encode_string("anyone")
  end

  @doc false
  def encode_create(path, data, ephemeral?) do
    flags = if ephemeral?, do: @ephemeral, else: @persistent
    encode_string(path) <> encode_buffer(data) <> encode_acl() <> <<flags::32-signed>>
  end

  @doc false
  def encode_path_watch(path, watch?) do
    encode_string(path) <> <<if(watch?, do: 1, else: 0)::8>>
  end

  @doc false
  def encode_set_data(path, data, version) do
    encode_string(path) <> encode_buffer(data) <> <<version::32-signed>>
  end

  @doc false
  def encode_delete(path, version) do
    encode_string(path) <> <<version::32-signed>>
  end

  @doc false
  def decode_create_response(frame) do
    {path, _offset} = decode_string(frame, @header_size)
    path
  end

  @doc false
  def decode_get_data_response(frame) do
    # A Stat struct follows the data; nothing here needs it.
    {data, _offset} = decode_buffer(frame, @header_size)
    data || <<>>
  end

  @doc false
  def decode_children_response(frame) do
    <<_header::binary-size(@header_size), count::32-signed, _rest::binary>> = frame

    {names, _offset} =
      Enum.reduce(1..count//1, {[], @header_size + 4}, fn _index, {acc, offset} ->
        {name, next} = decode_string(frame, offset)
        {[name | acc], next}
      end)

    Enum.reverse(names)
  end

  @doc false
  def error_reason(@err_ok), do: :ok
  def error_reason(@err_no_node), do: :no_node
  def error_reason(@err_node_exists), do: :node_exists
  def error_reason(@err_not_empty), do: :not_empty
  def error_reason(@err_session_expired), do: :session_expired
  def error_reason(code) when is_integer(code), do: {:zk_error, code}

  @doc false
  def opcodes do
    %{
      create: @op_create,
      delete: @op_delete,
      exists: @op_exists,
      get_data: @op_get_data,
      set_data: @op_set_data,
      get_children: @op_get_children,
      ping: @op_ping,
      close: @op_close
    }
  end
end
