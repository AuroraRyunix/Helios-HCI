defmodule SpectrumPhx.Lcm.PackageUploadWriter do
  @moduledoc """
  Streams an upgrade package to the ZooKeeper leader without staging it here.

  The same shape as `SpectrumPhx.Images.UploadWriter` and for the same reason: a
  `Phoenix.LiveView.UploadWriter` that pushes each chunk the browser sends onto an
  already-open request to spark-daemon. The default writer spools to a temporary file, and
  an upgrade archive is hundreds of megabytes of a container whose disk exists to hold an
  Elixir release.

  It has to be the *leader*, not any node. `hylia --load-package` runs as a Catalyst task,
  Catalyst dispatches on the leader, and the command reads a fixed path on the machine it
  runs on -- so bytes staged anywhere else would produce a task that fails with "no such
  file" while the upload said it worked.

  ## Where the work happens

  `init/1` opens nothing. `write_chunk/2` opens the request on the first chunk, because
  `init/1` runs inside the upload channel's `join` and the browser rejoins if that times
  out, which would open a second request and leak the first. Everything after that is one
  request held open until the last chunk.

  ## Failure

  `close/2` is called with `:done` when every chunk arrived, `:cancel` when the operator
  navigated away or the socket died, and `{:error, reason}` when a chunk failed. A package
  half-written to the leader's disk is not inert -- it is a file at exactly the path the
  loader is about to be told to read -- so every path that is not a completed upload
  removes it.

  Submitting the Catalyst task is deliberately *not* done here. The writer's job ends when
  the bytes are on the leader; asking the cluster to install them is an operator's
  decision, made in the LiveView, out of `meta/1`.
  """
  @behaviour Phoenix.LiveView.UploadWriter

  require Logger

  alias SpectrumPhx.Catalyst
  alias SpectrumPhx.Lcm
  alias SpectrumPhx.Spark

  @doc """
  The transport that carries the bytes: `MintTransport` by default.

  A seam, because the alternative way to exercise "open, stream, finish, and the four ways
  it can unwind" is a live cluster and a real package.
  """
  def transport,
    do: Application.get_env(:spectrum_phx, :package_upload_transport, __MODULE__.MintTransport)

  @doc """
  The path the staged package is removed with when an upload does not complete.

  Exposed so a test can assert the rollback names the file the loader would have read,
  rather than some other file.
  """
  def cleanup_command, do: "rm -f -- " <> Spark.escape(Lcm.package_path())

  @impl true
  def init(opts) do
    {:ok,
     %{
       name: Keyword.fetch!(opts, :name),
       size_bytes: Keyword.fetch!(opts, :size_bytes),
       node: Keyword.get(opts, :node) || Catalyst.leader_ip(),
       stage: :pending,
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
      node: state.node,
      result: state.result
    }
  end

  @impl true
  def write_chunk(data, %{stage: :pending} = state) do
    case transport().open(state.node, state.size_bytes) do
      {:ok, handle} ->
        write_chunk(data, %{state | stage: :streaming, handle: handle})

      {:error, reason} ->
        fail(state, {:transport, reason})
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

  # A chunk after the stream already failed. The staged file is gone and the entry is
  # being torn down; reopening anything would stage a package nobody asked for.
  def write_chunk(_data, %{stage: :failed} = state), do: {:error, error_of(state), state}

  @impl true
  def close(%{stage: :streaming} = state, :done) do
    with :ok <- declared_size_reached(state),
         {:ok, written} <- transport().finish(state.handle) do
      {:ok, %{state | stage: :done, written: written, result: {:ok, staged(state, written)}}}
    else
      {:error, reason} ->
        cleanup(state)
        {:error, reason}
    end
  end

  # Cancelled before a chunk arrived: `init/1` opens nothing, so there is nothing staged
  # and nothing to close.
  def close(%{stage: :pending} = state, _reason), do: {:ok, %{state | stage: :cancelled}}

  # Cancelled part way. The request was open, so a partial archive is on the leader's disk
  # at exactly the path the loader would be told to read.
  def close(%{stage: :streaming} = state, reason) do
    Logger.info(
      "[lcm] Package upload of #{state.name} ended as #{inspect(reason)} after " <>
        "#{state.written} of #{state.size_bytes} bytes; removing the staged file."
    )

    cleanup(state)

    {:ok, %{state | stage: :cancelled, handle: nil}}
  end

  # Already failed, or already done. The removal ran where the failure was seen.
  def close(state, _reason), do: {:ok, state}

  defp staged(state, written) do
    %{
      filename: state.name,
      size_bytes: written,
      node: state.node,
      path: Lcm.package_path()
    }
  end

  defp fail(state, reason) do
    if state.handle, do: transport().close(state.handle)
    cleanup(state)

    {:error, reason, %{state | stage: :failed, handle: nil, result: {:error, reason}}}
  end

  # Connection first: the daemon holds the file open for as long as the request is alive,
  # so a removal issued before the socket closes takes a file that is then re-created by
  # the last flush.
  defp cleanup(state) do
    if state.handle, do: transport().close(state.handle)
    transport().cleanup(state.node)
    :ok
  end

  defp error_of(%{result: {:error, reason}}), do: reason
  defp error_of(_state), do: {:transport, "The upload was already aborted."}

  # spark-daemon rejects a body that does not match its Content-Length, but naming the
  # problem here says "the browser sent less than it promised" rather than surfacing a
  # truncated-body transport error the operator cannot act on.
  defp declared_size_reached(%{written: written, size_bytes: size}) when written == size, do: :ok

  defp declared_size_reached(%{written: written, size_bytes: size}) do
    {:error,
     {:truncated,
      "The browser sent #{written} of the #{size} bytes it declared, so the package is " <>
        "incomplete and was not staged."}}
  end

  @doc "A writer failure as a sentence for the page."
  def describe({:truncated, message}), do: message
  def describe({:transport, reason}) when is_binary(reason), do: reason
  def describe({:transport, reason}), do: inspect(reason)
  def describe({:write, message}) when is_binary(message), do: message
  def describe(reason) when is_binary(reason), do: reason
  def describe(reason), do: inspect(reason)

  defmodule MintTransport do
    @moduledoc """
    The real transport: one Mint request held open across every chunk.

    Identical in construction to the image writer's transport, because the constraint is
    identical -- chunks arrive over a channel and have to be pushed onto a request that is
    already open, which `Req` cannot express. HTTP/1 only and passive, so no socket
    messages land in the upload channel's mailbox.
    """
    alias SpectrumPhx.Spark

    # The daemon fsyncs the whole archive before answering.
    @response_timeout 600_000

    def open(node, size_bytes) do
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

      with {:ok, conn} <- Mint.HTTP.connect(:https, node, settings.port, connect_opts),
           {:ok, conn, ref} <-
             Mint.HTTP.request(conn, "POST", Spark.package_write_path(), headers, :stream) do
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

    # The staged file is removed through the ordinary execute API rather than a second
    # streaming endpoint: it is one `rm` of one fixed path, and the path is escaped even
    # though it is a constant, because the day it stops being one is the day that matters.
    def cleanup(node) do
      Spark.execute(node, SpectrumPhx.Lcm.PackageUploadWriter.cleanup_command(), timeout: 30)
      :ok
    rescue
      _error -> :ok
    catch
      :exit, _reason -> :ok
    end

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
          {:error, {:write, "The leader accepted the package but did not report a byte count."}}
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
