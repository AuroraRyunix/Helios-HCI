defmodule SpectrumPhx.Zk.State do
  @moduledoc """
  The Helios-specific reads on top of `SpectrumPhx.Zk.Client`.

  Each node's spark-daemon publishes an ephemeral znode under `/helios/nodes/<ip>` every
  five seconds. Reading that tree gives the whole cluster's state from a single
  connection, instead of fanning mTLS calls out to every host on every page load -- and
  because the znodes are ephemeral, a dead node's entry is removed by the ensemble
  rather than inferred from a failed probe.

  The one distinction this module exists to preserve: **an error is not an empty
  cluster**. `{:error, _}` means ZooKeeper could not be read and the caller should fall
  back to probing nodes directly; `{:ok, %{nodes: %{}}}` means ZooKeeper answered and
  nothing has registered. `cluster status` renders those two very differently, and
  collapsing them is how a healthy cluster comes to be reported as entirely down.

  Presentation stays out of here, exactly as it does in the Python CLI: this returns the
  documents, and the web tier decides how to draw them.
  """
  alias SpectrumPhx.Zk.Client
  require Logger

  @nodes_path "/helios/nodes"
  @cluster_state_path "/cluster_state"

  # Seconds after which a node document is considered stale. spark-daemon republishes
  # every 5s, so this is six missed publishes.
  @stale_after_seconds 30

  # The order `cluster status` lists services in. It belongs with the document schema
  # rather than with any one renderer, since the keys of `"services"` come from here.
  @service_display_order ~w(ZooKeeper HydraDB Daruk Aether Spark Spectrum Bifrost Dagur
                            Mimir Vali Catalyst Hylia Gatoway Logos Mipha Agahnim Slate
                            Urbosa)

  @type node_document :: %{optional(String.t()) => term()}
  @type cluster_state :: %{
          nodes: %{optional(String.t()) => node_document()},
          desired: String.t() | nil,
          via: String.t() | nil
        }

  @doc "Path of the tree of ephemeral per-node status documents."
  def nodes_path, do: @nodes_path

  @doc "Path of the desired cluster state (`\"started\"` / `\"stopped\"`)."
  def cluster_state_path, do: @cluster_state_path

  @doc "Age in seconds beyond which a node document is stale."
  def stale_after_seconds, do: @stale_after_seconds

  @doc "Service display names, in the order the cluster reports them."
  def service_display_order, do: @service_display_order

  @doc """
  Read the whole cluster's published state.

  Returns `{:ok, %{nodes: %{ip => document}, desired: state, via: host}}`, where
  `desired` is `"started"`, `"stopped"`, or `nil` when nothing has been published.

  Returns `{:error, reason}` only when ZooKeeper itself could not be read -- the client
  is not connected, the socket failed mid-read, or the client process is unavailable.
  An empty `/helios/nodes` (or a missing one, before any node has ever registered) is a
  *successful* read with no nodes.

  A node whose document is not valid JSON is skipped rather than failing the whole read,
  matching the reference implementation: one node publishing garbage must not blind the
  operator to the rest of the cluster.
  """
  @spec read_cluster_state(GenServer.server()) :: {:ok, cluster_state()} | {:error, term()}
  def read_cluster_state(client \\ Client) do
    # Fail fast while ZooKeeper is down rather than making every caller wait out a
    # socket timeout; the client reconnects on its own in the background.
    if Client.connected?(client) do
      with {:ok, nodes} <- read_nodes(client),
           {:ok, desired} <- read_desired(client) do
        {:ok, %{nodes: nodes, desired: desired, via: Client.connected_host(client)}}
      end
    else
      {:error, :not_connected}
    end
  end

  @doc """
  IP of the current ZooKeeper leader, or `nil` if it cannot be determined.

  This is leader *discovery*, not leader election. Spectrum never needs to become the
  leader -- it needs to know which node is, because Catalyst's task queue is in-memory on
  the leader, so a task submitted to any other node is dispatched nowhere.

  No new ZooKeeper primitive is required: each node's spark-daemon already publishes
  `zk_leader` in its ephemeral status document, derived from the server's own `stat`
  four-letter-word output. Reading it is strictly better than running a second election,
  which could disagree with the ensemble's.

  Stale documents are ignored: a node that stopped publishing may well be the node that
  stopped being the leader.
  """
  def leader_ip(client \\ Client) do
    case read_cluster_state(client) do
      {:ok, %{nodes: nodes}} ->
        nodes
        |> Enum.find(fn {_ip, doc} ->
          doc["zk_leader"] == true and not node_stale?(doc)
        end)
        |> case do
          {ip, _doc} -> ip
          nil -> nil
        end

      _ ->
        nil
    end
  end

  @doc """
  Whether a node's document is older than #{@stale_after_seconds} seconds.

  A document with no usable `ts` is stale: it cannot be shown to be current, and the
  publisher always sets one.
  """
  @spec node_stale?(node_document()) :: boolean()
  def node_stale?(document) when is_map(document) do
    node_age_seconds(document) > @stale_after_seconds
  end

  def node_stale?(_document), do: true

  @doc "Seconds since a node's document was published."
  @spec node_age_seconds(node_document()) :: integer()
  def node_age_seconds(document) when is_map(document) do
    System.system_time(:second) - timestamp(document)
  end

  def node_age_seconds(_document), do: System.system_time(:second)

  # -- internals --------------------------------------------------------------

  defp read_nodes(client) do
    case Client.get_children(client, @nodes_path) do
      {:ok, names} ->
        documents =
          names
          |> Enum.map(fn name -> {name, read_node(client, name)} end)
          |> Enum.reject(fn {_name, document} -> document == :unreadable end)
          |> Map.new()

        {:ok, documents}

      # The tree has not been created yet. Nobody has registered, which is a fact about
      # the cluster, not a failure to read it.
      {:error, :no_node} ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_node(client, name) do
    with {:ok, raw} <- Client.get(client, @nodes_path <> "/" <> name),
         {:ok, document} when is_map(document) <- Jason.decode(raw) do
      document
    else
      {:error, reason} ->
        Logger.debug("[ZK] Skipping #{@nodes_path}/#{name}: #{inspect(reason)}")
        :unreadable

      _other ->
        :unreadable
    end
  end

  defp read_desired(client) do
    case Client.get(client, @cluster_state_path) do
      {:ok, raw} ->
        case String.trim(raw) do
          "" -> {:ok, nil}
          value -> {:ok, value}
        end

      # Never set. Distinct from "could not be read", which propagates.
      {:error, :no_node} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp timestamp(document) do
    case Map.get(document, "ts") do
      ts when is_integer(ts) ->
        ts

      ts when is_float(ts) ->
        trunc(ts)

      ts when is_binary(ts) ->
        case Integer.parse(ts) do
          {value, _rest} -> value
          :error -> 0
        end

      _absent ->
        0
    end
  end
end
