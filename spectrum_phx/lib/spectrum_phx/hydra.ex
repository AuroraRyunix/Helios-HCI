defmodule SpectrumPhx.Hydra do
  @moduledoc """
  ScyllaDB (Hydra) access via Xandra.

  Every query here is a *prepared statement with bound parameters*. That is the whole
  point of this layer: the Python tier built CQL by string interpolation in six
  copy-pasted `run_cql_query` implementations, which is why injection had to be patched
  at each call site (VM names, session tokens, image filenames, update-server values).
  Parameter binding removes that class of bug structurally rather than per-site.
  """
  require Logger

  @keyspace "hydra"
  @pool __MODULE__.Pool

  def child_spec(_opts) do
    nodes = contact_points()

    Xandra.Cluster.child_spec(
      name: @pool,
      nodes: nodes,
      pool_size: 4,
      # Never block application boot on the database: Spectrum must come up and report
      # that Scylla is down rather than failing to start alongside it.
      sync_connect: false,
      default_consistency: :quorum
    )
  end

  @doc "Contact points: the cluster's own nodes on the CQL port."
  def contact_points do
    case SpectrumPhx.Cluster.Config.node_ips() do
      [] -> ["127.0.0.1:9042"]
      ips -> Enum.map(ips, fn ip -> ip <> ":9042" end)
    end
  end

  @doc """
  Run a prepared statement and return `{:ok, [map]}` or `{:error, reason}`.

  `consistency` is chosen per call rather than set globally with a silent downgrade:
  `daruk.py` fell back from QUORUM to ONE on any error whose message contained
  "unavailable", "timeout" or even "active", and it did so for *writes* -- which is how
  two sides of a partition both come to believe they own the same VM.
  """
  def query(statement, params \\ [], opts \\ []) do
    consistency = Keyword.get(opts, :consistency, :quorum)

    case Xandra.Cluster.execute(@pool, statement, params, consistency: consistency) do
      {:ok, %Xandra.Page{} = page} -> {:ok, Enum.to_list(page)}
      {:ok, _other} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

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

    case Xandra.Cluster.execute(@pool, statement, params,
           consistency: consistency,
           serial_consistency: :serial
         ) do
      {:ok, %Xandra.Page{} = page} ->
        applied =
          page
          |> Enum.to_list()
          |> List.first()
          |> case do
            %{"[applied]" => value} -> value
            _ -> true
          end

        {:ok, applied}

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
