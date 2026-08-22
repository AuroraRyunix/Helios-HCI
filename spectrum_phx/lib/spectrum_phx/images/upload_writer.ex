defmodule SpectrumPhx.Images.UploadWriter do
  @moduledoc """
  Streams an uploaded image onto cluster storage without ever staging it here.

  A `Phoenix.LiveView.UploadWriter` that pushes each chunk the browser sends onto an
  already-open request to spark-daemon, which is native to the host and owns storage. The
  default writer spools to a temporary file; for an install ISO that is gibibytes of the
  web tier's disk, and it puts this tier back on the data path that `SpectrumPhx.Images`
  exists to keep it off. See `SpectrumPhx.Images.upload_note/0`.

  ## Where the work happens

  `init/1` does no network work at all. Preparing the vdisk runs on the *first chunk*,
  because `init/1` is called inside the upload channel's `join` and the browser joins with
  the socket's default ten-second timeout -- then *rejoins* if it expires, which would run
  the whole preparation a second time and leak the first attempt's connection.
  `write_chunk/2` is bounded by `:chunk_timeout`, which the LiveView sets, so the work
  sits under a limit this code controls rather than one the browser picked.

  ## Backpressure

  The transport sends on the socket synchronously, so a host that cannot keep up blocks
  the writer, which blocks the channel, which stops the browser sending more. Memory use
  stays at one chunk regardless of image size.

  ## Failure

  Every exit runs the rollback. `close/2` is called with `:done` when all chunks arrived,
  `:cancel` when the operator navigated away or the socket died, and `{:error, reason}`
  when a chunk failed -- and a half-built vdisk holds storage on every replica that
  nothing reclaims, so all three paths converge on `SpectrumPhx.Images.rollback_upload/1`
  unless the upload actually completed and was registered.

  The outcome travels back to the LiveView through `meta/1`, which is what
  `consume_uploaded_entries/3` receives.
  """
  @behaviour Phoenix.LiveView.UploadWriter

  require Logger

  alias SpectrumPhx.Images

  @doc """
  The transport that carries the bytes: `MintTransport` by default.

  A seam, because otherwise the only way to exercise this state machine -- prepare,
  stream, verify, register, and the four ways it can unwind -- is against a live cluster
  holding real storage, which is exactly the part that must not be guessed at.
  """
  def transport,
    do: Application.get_env(:spectrum_phx, :images_upload_transport, __MODULE__.MintTransport)

  @impl true
  def init(opts) do
    {:ok,
     %{
       name: Keyword.fetch!(opts, :name),
       size_bytes: Keyword.fetch!(opts, :size_bytes),
       # :pending until the first chunk prepares storage, then :streaming, then whatever
       # close/2 settles on. Nothing here touches the network.
       stage: :pending,
       allocation: nil,
       handle: nil,
       written: 0,
       result: nil
     }}
  end

  @impl true
  def meta(state) do
    %{
      name: state.name,
      size_bytes: state.size_bytes,
      written: state.written,
      socket: state.allocation && state.allocation.socket,
      vdisk: state.allocation && state.allocation.vdisk,
      result: state.result
    }
  end

  @impl true
  def write_chunk(data, %{stage: :pending} = state) do
    case start(state) do
      {:ok, started} -> write_chunk(data, started)
      {:error, reason, failed} -> {:error, reason, failed}
    end
  end

  def write_chunk(data, %{stage: :streaming} = state) do
    case transport().send_chunk(state.handle, data) do
      {:ok, handle} ->
        {:ok, %{state | handle: handle, written: state.written + byte_size(data)}}

      {:error, reason} ->
        # The request is dead. There is nothing left to finish, only to undo.
        fail(state, {:transport, reason})
    end
  end

  # A chunk after the stream already failed. Reject it rather than reopening anything:
  # the allocation is gone and the entry is being torn down.
  def write_chunk(_data, %{stage: :failed} = state) do
    {:error, error_of(state), state}
  end

  @impl true
  def close(%{stage: :streaming} = state, :done) do
    with :ok <- declared_size_reached(state),
         {:ok, written} <- transport().finish(state.handle),
         :ok <- Images.finish_upload(state.allocation, written),
         {:ok, image} <- Images.register(register_attrs(state)) do
      {:ok, %{state | stage: :done, written: written, result: {:ok, image}}}
    else
      {:error, reason} ->
        # Connection first, allocation second. The daemon holds the vdisk attached for
        # as long as the request is alive, so a delete issued before the socket closes is
        # refused and the vdisk leaks -- which is the exact failure this rollback exists
        # to prevent.
        transport().close(state.handle)
        Images.rollback_upload(state.allocation)
        # {:error, _} from close/2 fails the entry, which is right: an image that was not
        # registered must not look uploaded.
        {:error, reason}
    end
  end

  # Cancelled, or closed before a single chunk arrived. Nothing was allocated unless the
  # first chunk got as far as preparing, and rollback is safe either way.
  def close(%{stage: stage} = state, reason) when stage in [:pending, :streaming] do
    # Connection first: the daemon releases the vdisk when the request ends, and the
    # delete cannot succeed until it has.
    if state.handle, do: transport().close(state.handle)

    if state.allocation do
      Logger.info(
        "[images] Upload of #{state.name} ended as #{inspect(reason)} after " <>
          "#{state.written} of #{state.size_bytes} bytes; rolling back."
      )

      Images.rollback_upload(state.allocation)
    end

    {:ok, %{state | stage: :cancelled, allocation: nil, handle: nil}}
  end

  # Already failed, or already done. The rollback ran where the failure was seen, and
  # running it again would delete a resource this upload no longer owns.
  def close(state, _reason), do: {:ok, state}

  # -- the first chunk ------------------------------------------------------------------

  defp start(state) do
    case Images.prepare_upload(state.name, state.size_bytes) do
      {:error, reason} ->
        {:error, reason, %{state | stage: :failed, result: {:error, reason}}}

      {:ok, allocation} ->
        case transport().open(allocation, state.size_bytes) do
          {:ok, handle} ->
            {:ok, %{state | stage: :streaming, allocation: allocation, handle: handle}}

          {:error, reason} ->
            # Storage was allocated and the connection was not. Undo the allocation, or
            # it holds space on every node with nothing pointing at it. fail/2 already
            # returns the {:error, reason, state} shape write_chunk/2 expects.
            fail(%{state | allocation: allocation}, {:transport, reason})
        end
    end
  end

  defp fail(state, reason) do
    # Connection first, allocation second: see close/2.
    if state.handle, do: transport().close(state.handle)
    if state.allocation, do: Images.rollback_upload(state.allocation)

    {:error, reason,
     %{state | stage: :failed, allocation: nil, handle: nil, result: {:error, reason}}}
  end

  defp error_of(%{result: {:error, reason}}), do: reason
  defp error_of(_state), do: {:transport, "The upload was already aborted."}

  defp register_attrs(state) do
    %{name: state.name, size_bytes: state.size_bytes, socket: state.allocation.socket}
  end

  # The vdisk was created at the size the browser declared, and the daemon rejects a
  # body that does not match its Content-Length. Catching it here names the actual problem
  # rather than surfacing a truncated-body transport error.
  defp declared_size_reached(%{written: written, size_bytes: size}) when written == size, do: :ok

  defp declared_size_reached(%{written: written, size_bytes: size}) do
    {:error,
     {:truncated,
      "The browser sent #{written} of the #{size} bytes it declared, so the image is " <>
        "incomplete."}}
  end

  defmodule MintTransport do
    @moduledoc """
    The real transport: one Mint request held open across every chunk.

    `Req` covers every other call in `SpectrumPhx.Spark`, because they are all one request
    and one response. An upload is neither -- chunks arrive over a channel and have to be
    pushed onto a request that is already open -- so this drives Mint directly, using the
    same port and mutual-TLS material as the rest of the client.

    HTTP/1 only, and passive: HTTP/2 would add flow control this has no reason to handle,
    and an active connection would post socket messages into the upload channel's mailbox,
    which is not this code's to interpret.
    """
    alias SpectrumPhx.Spark

    # The daemon fsyncs a whole image before it answers, so the wait for the response is
    # minutes-scale rather than the seconds a control-plane call needs.
    @response_timeout 600_000

    def open(allocation, size_bytes) do
      settings = Spark.connection_settings()

      headers = [
        {"content-type", "application/octet-stream"},
        {"content-length", Integer.to_string(size_bytes)}
      ]

      connect_opts = [
        transport_opts: settings.transport_opts,
        protocols: [:http1],
        mode: :passive
      ]

      with {:ok, conn} <- Mint.HTTP.connect(:https, allocation.node, settings.port, connect_opts),
           {:ok, conn, ref} <-
             Mint.HTTP.request(
               conn,
               "POST",
               Spark.vdisk_write_path(allocation.vdisk),
               headers,
               :stream
             ) do
        {:ok, %{conn: conn, ref: ref}}
      else
        {:error, reason} -> {:error, format(reason)}
        {:error, conn, reason} -> {:error, format(reason)} |> tap_close(conn)
      end
    end

    def send_chunk(%{conn: conn, ref: ref} = handle, data) do
      case Mint.HTTP.stream_request_body(conn, ref, data) do
        {:ok, conn} -> {:ok, %{handle | conn: conn}}
        {:error, conn, reason} -> {:error, format(reason)} |> tap_close(conn)
      end
    end

    def finish(%{conn: conn, ref: ref}) do
      case Mint.HTTP.stream_request_body(conn, ref, :eof) do
        {:ok, conn} -> read_response(conn, ref, %{status: nil, body: ""})
        {:error, conn, reason} -> {:error, {:transport, format(reason)}} |> tap_close(conn)
      end
    end

    def close(nil), do: :ok
    def close(%{conn: conn}), do: safe_close(conn)

    defp read_response(conn, ref, acc) do
      case Mint.HTTP.recv(conn, 0, @response_timeout) do
        {:ok, conn, responses} ->
          case reduce(responses, ref, acc) do
            {:done, acc} ->
              safe_close(conn)
              interpret(acc)

            {:cont, acc} ->
              read_response(conn, ref, acc)
          end

        {:error, conn, reason, _responses} ->
          safe_close(conn)
          {:error, {:transport, format(reason)}}
      end
    end

    defp reduce(responses, ref, acc) do
      Enum.reduce(responses, {:cont, acc}, fn
        {:status, ^ref, status}, {_stage, acc} -> {:cont, %{acc | status: status}}
        {:headers, ^ref, _headers}, current -> current
        {:data, ^ref, data}, {_stage, acc} -> {:cont, %{acc | body: acc.body <> data}}
        {:done, ^ref}, {_stage, acc} -> {:done, acc}
        {:error, ^ref, _reason}, {_stage, acc} -> {:done, acc}
        _other, current -> current
      end)
    end

    defp interpret(%{status: 200, body: body}) do
      case Jason.decode(body) do
        {:ok, %{"written" => written}} when is_integer(written) ->
          {:ok, written}

        _other ->
          {:error, {:write, "The host accepted the image but did not report a byte count."}}
      end
    end

    defp interpret(%{status: status, body: body}) do
      detail =
        case Jason.decode(body) do
          {:ok, %{"error" => message}} when is_binary(message) -> message
          _other -> String.slice(body, 0, 300)
        end

      {:error, {:write, "HTTP #{status}: #{detail}"}}
    end

    defp tap_close(result, conn) do
      safe_close(conn)
      result
    end

    defp safe_close(conn) do
      Mint.HTTP.close(conn)
      :ok
    rescue
      _error -> :ok
    catch
      :exit, _reason -> :ok
    end

    defp format(%Mint.TransportError{} = error), do: Exception.message(error)
    defp format(%Mint.HTTPError{} = error), do: Exception.message(error)
    defp format(reason) when is_binary(reason), do: reason
    defp format(reason), do: inspect(reason)
  end
end
