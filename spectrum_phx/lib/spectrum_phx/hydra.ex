defmodule SpectrumPhx.Hydra do
  @moduledoc """
  ScyllaDB (Hydra) access via Xandra.

  Every query here is a *prepared statement with bound parameters*. That is the whole
  point of this layer: the Python tier built CQL by string interpolation in six
  copy-pasted `run_cql_query` implementations, which is why injection had to be patched
  at each call site (VM names, session tokens, image filenames, update-server values).
  Parameter binding removes that class of bug structurally rather than per-site.
  """

  @keyspace "hydra"
  @pool __MODULE__.Pool

  def child_spec(_opts) do
    # Resolve contact points inside start_link, NOT here. Supervisor.start_link/2 builds
    # every child spec before starting any child, so calling Cluster.Config at spec-build
    # time reaches an Agent that does not exist yet and the whole application fails to
    # boot with "(EXIT) no process".
    %{id: __MODULE__, start: {__MODULE__, :start_link, []}, type: :supervisor}
  end

  def start_link(_opts \\ []) do
    Xandra.Cluster.start_link(
      name: @pool,
      nodes: contact_points(),
      pool_size: 4,
      # Never block application boot on the database: Spectrum must come up and report
      # that Scylla is down rather than failing to start alongside it.
      sync_connect: false,
      default_consistency: :quorum
    )
  end

  @doc "Contact points: the cluster's own nodes on the CQL port."
  def contact_points do
    ips =
      try do
        SpectrumPhx.Cluster.Config.node_ips()
      catch
        :exit, _ -> []
      end

    case ips do
      [] -> ["127.0.0.1:9042"]
      ips -> Enum.map(ips, fn ip -> ip <> ":9042" end)
    end
  end

  @doc """
  Run a statement with bound parameters and return `{:ok, [map]}` or `{:error, reason}`.

  The statement is genuinely *prepared* (Xandra caches prepared statements per
  connection and re-prepares automatically on schema change), so values travel as bound
  parameters and never as text spliced into CQL. That is the point of this layer: the
  Python tier built CQL by string interpolation in six copy-pasted `run_cql_query`
  implementations, which is why injection had to be patched at each call site.

  Parameters may be given as plain values, or as `{type, value}` tuples for callers that
  were written against the simple-query API. The type tag is redundant once a statement
  is prepared -- the server supplies the parameter metadata -- so it is unwrapped here.

  `consistency` is chosen per call rather than set globally with a silent downgrade:
  `daruk.py` fell back from QUORUM to ONE on any error whose message contained
  "unavailable", "timeout" or even "active", and it did so for *writes* -- which is how
  two sides of a partition both come to believe they own the same VM.
  """
  def query(statement, params \\ [], opts \\ []) do
    consistency = Keyword.get(opts, :consistency, :quorum)

    with {:ok, prepared} <- Xandra.Cluster.prepare(@pool, statement),
         {:ok, result} <-
           Xandra.Cluster.execute(@pool, prepared, strip_types(params), consistency: consistency) do
      case result do
        %Xandra.Page{} = page -> {:ok, Enum.to_list(page)}
        _other -> {:ok, []}
      end
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # Accept both `["v"]` and `[{"text", "v"}]`. Prepared statements carry their own
  # parameter metadata, so an explicit type tag is unnecessary and would be rejected.
  defp strip_types(params) when is_list(params) do
    Enum.map(params, fn
      {type, value} when is_binary(type) -> value
      other -> other
    end)
  end

  defp strip_types(params), do: params

  @doc "Like `query/3` but raises on error. For call sites where failure is a bug."
  def query!(statement, params \\ [], opts \\ []) do
    case query(statement, params, opts) do
      {:ok, rows} -> rows
      {:error, reason} -> raise "Hydra query failed: " <> inspect(reason)
    end
  end

  @doc """
  Compare-and-swap helper for ownership invariants (LWT).

  Returns `{:ok, true}` when applied, `{:ok, false}` when the condition did not hold.
  This is the primitive the VM-ownership and migration-lock paths need: the Python tier
  used blind `UPDATE`s, so a stale `host_ip` was enough to start a VM twice.
  """
  def apply_lwt(statement, params \\ [], opts \\ []) do
    consistency = Keyword.get(opts, :consistency, :quorum)

    with {:ok, prepared} <- Xandra.Cluster.prepare(@pool, statement),
         {:ok, %Xandra.Page{} = page} <-
           Xandra.Cluster.execute(@pool, prepared, strip_types(params),
             consistency: consistency,
             serial_consistency: :serial
           ) do
      applied =
        page
        |> Enum.to_list()
        |> List.first()
        |> case do
          %{"[applied]" => value} -> value
          _ -> true
        end

      {:ok, applied}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "True when the keyspace is reachable."
  def healthy? do
    case query("SELECT release_version FROM system.local", [], consistency: :one) do
      {:ok, _} -> true
      _ -> false
    end
  end

  def keyspace, do: @keyspace
end
