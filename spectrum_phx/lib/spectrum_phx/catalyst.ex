defmodule SpectrumPhx.Catalyst do
  @moduledoc """
  The one place this tier hands a long operation to the cluster.

  Catalyst is a task queue whose queues are `queue.Queue` objects *inside the process on
  the ZooKeeper leader*. `POST /api/v1/tasks/submit` does two separate things: it writes a
  row to `hydra.catalyst_tasks`, which is the record the console reads back, and it puts
  the task on the in-memory queue for the named service, which is the only thing that
  causes any work to happen. A console that wrote the row itself would produce a task that
  is listed, never runs, and never fails -- so submission is an API call to the leader and
  cannot be anything else.

  ## Which service runs what

  A service name is a queue, and a queue only moves if some daemon is long-polling it on
  the leader:

    * `vali` -- VM lifecycle and host maintenance. `vali.py`'s worker thread runs each
      task in its own thread with no deadline, which is what makes a ten-minute live
      migration a task rather than a timeout.
    * `dagur` -- one command, run on the leader through its spark-daemon, with its exit
      code as the verdict. This is the general "do a thing to this cluster" task, and it
      is what the Python console already used for the Urbosa bootstrap.
    * `lanayru` -- building and tearing down the guest Kubernetes cluster. Drained by the
      console backend, because `lanayru.py` is a module that tier imports rather than a
      daemon of its own.

  Anything else is a queue nothing drains: the task would sit `pending` forever, which
  looks exactly like slow. `services/0` is the list that is actually consumed, and
  `submit/3` refuses the rest rather than producing an invisible no-op.

  ## Failure is the point

  Every call here returns `{:ok, %{"task_id" => id}}` or `{:error, reason}`, and callers
  are expected to put the reason on screen. A submission that fails silently leaves an
  operator watching a task ring that will never show their work, which is worse than the
  request they thought they made never having been sent.

  ## Test seam

  `submitter/0` replaces the HTTP call with a 3-arity function receiving
  `(service, action, payload)`. Set it in `Application.put_env(:spectrum_phx,
  :catalyst_submitter, fun)` so a control can be driven end to end with no leader present.
  """

  require Logger

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Spark

  @port 9091

  # The queues that have a worker draining them on the leader. See the moduledoc.
  @services ~w(vali dagur lanayru)

  # `dagur` hands the command to spark-daemon, which defaults an absent timeout to 45
  # seconds and kills the command there. Forty-five seconds is a control-plane number and
  # every operation this console submits is a cluster-wide one, so the timeout travels
  # with the task and the caller states it.
  @default_command_timeout 3_600

  @doc "The Catalyst service queues that are actually drained."
  def services, do: @services

  @doc "The port Catalyst's API listens on."
  def port, do: @port

  @doc """
  Submit one unit of work and return without waiting for it.

  `{:ok, %{"task_id" => id}}` means Catalyst wrote the row and queued the task; it says
  nothing about whether the work succeeds. That verdict arrives in the row, which is what
  the task ring and `/tasks` read.
  """
  @spec submit(String.t(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def submit(service, action, payload \\ %{})

  def submit(service, action, payload) when service in @services and is_map(payload) do
    case submitter() do
      nil -> post(service, action, payload)
      fun when is_function(fun, 3) -> fun.(service, action, payload)
    end
    |> case do
      {:ok, result} ->
        {:ok, result}

      {:error, reason} ->
        Logger.warning(
          "Catalyst task #{service}/#{action} could not be submitted: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  def submit(service, _action, _payload) when is_binary(service) do
    {:error,
     {:unknown_service, "No worker drains the '#{service}' queue, so the task would never run."}}
  end

  @doc """
  Run one command on the ZooKeeper leader as a `dagur` task.

  This is the task type for work whose code already lives on the host as a command:
  `urbosa-bootstrap`, and hylia's package and upgrade entry points. Dagur runs it through
  the leader's spark-daemon and reports the exit code back to Catalyst, so a non-zero exit
  becomes a `failed` row carrying the command's own output -- which is the only reason to
  route this through a task rather than running it here.

  Options:

    * `:timeout` -- seconds the command may run for, defaulting to
      #{@default_command_timeout}. Sent to spark-daemon, which otherwise applies 45.
    * `:reports_progress` -- the command updates its own Catalyst task, so dagur's
      guessing ticker must stay out of its way. A command that reports real progress and
      a ticker that invents it would fight over the same column.
    * `:payload` -- extra fields merged into the task payload. They end up in the row's
      JSON, which is where `SpectrumPhx.Tasks` looks for a task's subject.
  """
  @spec run_on_leader(String.t(), String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def run_on_leader(job_name, command, opts \\ []) do
    payload =
      opts
      |> Keyword.get(:payload, %{})
      |> Map.merge(%{
        "job_name" => job_name,
        "command" => command,
        "timeout" => Keyword.get(opts, :timeout, @default_command_timeout)
      })
      |> maybe_put_progress(Keyword.get(opts, :reports_progress, false))

    submit("dagur", "execute", payload)
  end

  defp maybe_put_progress(payload, true), do: Map.put(payload, "reports_progress", true)
  defp maybe_put_progress(payload, _false), do: payload

  @doc "The default ceiling `run_on_leader/3` puts on a command."
  def default_command_timeout, do: @default_command_timeout

  # -- seams -----------------------------------------------------------------------

  @doc """
  Override for the transport: `nil` (post to the leader) or a 3-arity function receiving
  `(service, action, payload)` and returning `{:ok, map}` or `{:error, reason}`.
  """
  @spec submitter() :: nil | (String.t(), String.t(), map() -> {:ok, map()} | {:error, term()})
  def submitter, do: Application.get_env(:spectrum_phx, :catalyst_submitter)

  # -- transport -------------------------------------------------------------------

  # Mutual TLS, with the same client material `SpectrumPhx.Spark` uses.
  #
  # Catalyst dispatches cluster work and used to accept it from anything that could open a
  # socket to 9091, with no credential and no source check. It now requires a certificate
  # the cluster CA signed, so this has to present one; over plain HTTP the submission is
  # refused at the handshake.
  defp post(service, action, payload) do
    settings = Spark.connection_settings()

    url = "https://" <> leader_ip() <> ":" <> Integer.to_string(@port) <> "/api/v1/tasks/submit"

    body = %{"service" => service, "action" => action, "payload" => payload}

    case Req.post(url,
           json: body,
           connect_options: [transport_opts: settings.transport_opts],
           receive_timeout: 35_000
         ) do
      {:ok, %Req.Response{status: 200, body: response}} -> {:ok, response}
      {:ok, %Req.Response{status: status, body: response}} -> {:error, {:http, status, response}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  The node holding Catalyst's dispatch queue.

  Catalyst runs on every node but only the ZooKeeper leader holds the queues (they are
  in-process `queue.Queue` objects, not a table), so tasks must be submitted there and
  nowhere else.

  TODO: leader resolution. `vali.py`'s `get_zookeeper_leader_ip/0` probes each node's
  ZooKeeper four-letter `stat` for "mode: leader", then checks that the leader is actually
  answering on 9091 and otherwise falls back to the lowest-numbered node that is.
  `SpectrumPhx.Zk` has no equivalent yet -- `Zk.Client` and `Zk.State` cover the connection
  and the cluster-state document, not leader election -- and that module is owned
  elsewhere. This resolves the function at runtime rather than compile time so it wires
  itself up when the capability lands; until then it submits to the local node, which is
  correct only when this node happens to be the leader. Configure `:catalyst_ip` to pin it
  in the meantime.
  """
  @spec leader_ip() :: String.t()
  def leader_ip do
    case Application.get_env(:spectrum_phx, :catalyst_ip) do
      ip when is_binary(ip) and ip != "" ->
        ip

      _ ->
        [Module.concat([:SpectrumPhx, :Zk]), Module.concat([:SpectrumPhx, :Zk, :State])]
        |> Enum.find_value(fn module ->
          if Code.ensure_loaded?(module) and function_exported?(module, :leader_ip, 0) do
            case apply(module, :leader_ip, []) do
              ip when is_binary(ip) and ip != "" -> ip
              _ -> nil
            end
          end
        end)
        |> Kernel.||(Config.local_ip())
    end
  end

  @doc """
  A reason turned into a sentence for an operator.

  Every caller of this module puts its failures on a page, and `inspect/1` of a Mint
  error is not something anyone can act on.
  """
  def describe({:unknown_service, message}), do: message
  def describe({:http, _status, %{"error" => message}}) when is_binary(message), do: message
  def describe({:http, status, _body}), do: "Catalyst answered HTTP #{status}."
  def describe(%{__exception__: true} = reason), do: Exception.message(reason)
  def describe(reason) when is_binary(reason), do: reason
  def describe(reason), do: inspect(reason)
end
