defmodule SpectrumPhx.Spark do
  @moduledoc """
  Client for the Spark daemon's mutual-TLS control API on port 9099.

  Spark is the only component permitted to run commands on a hypervisor, so every
  host-level action taken by the web tier goes through here.

  Note the daemon executes what it is given with a shell, as root. Callers must therefore
  never build a command from unvalidated input; `escape/1` is provided for the cases that
  genuinely need to interpolate a value.
  """
  require Logger

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

    case Req.get(url, connect_options: [transport_opts: tls_opts()], receive_timeout: timeout * 1000) do
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
  closing the quote, adding an escaped quote, and reopening it.
  """
  def escape(value) when is_binary(value) do
    "'" <> String.replace(value, "'", "'\''") <> "'"
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
