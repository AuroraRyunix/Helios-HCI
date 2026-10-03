defmodule SpectrumPhx.Snapshots do
  @moduledoc """
  Read-only view of one vdisk's snapshots and the policy that governs them.

  Nothing here writes. Taking, pruning and rolling back are `valcli storage.snapshot-run`,
  `storage.snapshot-policy` and `storage.rollback`, and the console deliberately offers no
  button for any of them yet: a rollback destroys what a guest wrote, and the confirmation
  flow that deserves is its own piece of work.

  ## Where the answer comes from

  A snapshot is an immutable vdisk whose `parent_vdisk` names the disk it was taken from, so
  the list is read from `hydra.dfs_vdisks` -- lineage is always right there. Who took it
  comes from `hydra.dfs_snapshot_index`, which exists only for snapshots made since it was
  added, so a snapshot the index has not heard of is shown as `:unindexed` instead of being
  guessed at. Retention never prunes an unindexed snapshot, which is the reason to say so.

  ## "Has children" is the retention rule made visible

  A snapshot some other vdisk was derived from is never pruned (see
  `helios_snapshots.plan_retention`). The page shows that flag so an operator asking why a
  snapshot outlived `keep` can see the answer instead of inferring it.

  ## The policy is resolved here the way it is there

  Narrowest scope wins -- vdisk, then container, then cluster -- and a *disabled* narrow row
  is an exemption rather than being skipped. The same rule is in `helios_snapshots.py`;
  this is a second copy because the console cannot import it, and the tests pin both to the
  same table of cases.

  ## Test seam

  Reads go through `source/0`: `:hydra` (default) or `{:static, %{vdisks: [...], index:
  [...], policies: [...]}}` holding raw rows with string keys, so the joins this module
  exists for are what the tests exercise.
  """

  alias SpectrumPhx.Hydra

  @vdisks_cql "SELECT vdisk_id, class, container, parent_vdisk, size_bytes, created_at_ms " <>
                "FROM hydra.dfs_vdisks"
  @index_cql "SELECT vdisk_id, created_at_ms, snapshot_id, origin " <>
               "FROM hydra.dfs_snapshot_index WHERE vdisk_id = ?"
  @policies_cql "SELECT scope, target, enabled, interval_seconds, keep_last " <>
                  "FROM hydra.dfs_snapshot_policies"

  @cluster_target "*"

  @doc "CQL used to read the vdisks."
  def vdisks_cql, do: @vdisks_cql

  @doc "CQL used to read one vdisk's index rows. Binds the vdisk id."
  def index_cql, do: @index_cql

  @doc "CQL used to read the policies."
  def policies_cql, do: @policies_cql

  @doc "Where reads come from: `:hydra` or `{:static, map}`."
  def source, do: Application.get_env(:spectrum_phx, :snapshots_source, :hydra)

  @doc """
  A vdisk's snapshots, newest first, and the policy that governs it.

      {:ok, %{vdisk_id:, snapshots: [snapshot], policy: policy}}

  where a snapshot is `%{id:, origin:, taken_at_ms:, size_bytes:, has_children?:}` with
  `origin` one of `:policy`, `:manual`, `:pre_rollback`, `:unindexed`, and `policy` is
  `{:policy, %{scope:, target:, every_seconds:, keep:}}`, `:exempt` or `:none`.

  `{:error, :not_found}` when no such vdisk exists, `{:error, reason}` when Hydra could not
  be read -- which is not an empty list and must not be rendered as one.
  """
  @spec for_vdisk(String.t()) :: {:ok, map()} | {:error, :not_found | :invalid_name | term()}
  def for_vdisk(vdisk_id) do
    with {:ok, vdisk_id} <- validate(vdisk_id),
         {:ok, vdisks} <- read(:vdisks),
         {:ok, index} <- read({:index, vdisk_id}),
         {:ok, policies} <- read(:policies) do
      case Enum.find(vdisks, &(&1["vdisk_id"] == vdisk_id)) do
        nil ->
          {:error, :not_found}

        vdisk ->
          {:ok,
           %{
             vdisk_id: vdisk_id,
             snapshots: snapshots(vdisk_id, vdisks, index),
             policy: policy_for(policies, vdisk_id, vdisk["container"])
           }}
      end
    end
  end

  @doc """
  The policy that governs a vdisk: `{:policy, map}`, `:exempt` or `:none`.

  Public so the table of cases can be tested directly.
  """
  def policy_for(policies, vdisk_id, container) do
    by_key = Map.new(policies, fn p -> {{p["scope"], p["target"]}, p} end)

    candidates = [{"vdisk", vdisk_id}, {"container", container}, {"cluster", @cluster_target}]

    case Enum.find_value(candidates, &Map.get(by_key, &1)) do
      nil -> :none
      %{"enabled" => true} = row -> {:policy, summarize(row)}
      _disabled -> :exempt
    end
  end

  defp summarize(row) do
    %{
      scope: row["scope"],
      target: row["target"],
      every_seconds: row["interval_seconds"],
      keep: row["keep_last"]
    }
  end

  defp snapshots(vdisk_id, vdisks, index) do
    indexed = Map.new(index, fn row -> {row["snapshot_id"], row} end)

    referenced =
      MapSet.new(for v <- vdisks, v["parent_vdisk"] not in [nil, ""], do: v["parent_vdisk"])

    for v <- vdisks, v["parent_vdisk"] == vdisk_id, v["class"] == "immutable" do
      %{
        id: v["vdisk_id"],
        origin: origin(indexed[v["vdisk_id"]]),
        taken_at_ms: v["created_at_ms"],
        size_bytes: v["size_bytes"],
        has_children?: MapSet.member?(referenced, v["vdisk_id"])
      }
    end
    |> Enum.sort_by(&{-(&1.taken_at_ms || 0), &1.id})
  end

  defp origin(nil), do: :unindexed
  defp origin(%{"origin" => "policy"}), do: :policy
  defp origin(%{"origin" => "manual"}), do: :manual
  defp origin(%{"origin" => "pre-rollback"}), do: :pre_rollback
  defp origin(_other), do: :unindexed

  # A name that could not be a vdisk id never reaches a query.
  defp validate(id) when is_binary(id) do
    if Regex.match?(~r/\A[A-Za-z0-9][A-Za-z0-9_.-]{0,62}\z/, id),
      do: {:ok, id},
      else: {:error, :invalid_name}
  end

  defp validate(_other), do: {:error, :invalid_name}

  defp read(what) do
    case {source(), what} do
      {{:static, data}, :vdisks} ->
        {:ok, Map.get(data, :vdisks, [])}

      {{:static, data}, :policies} ->
        {:ok, Map.get(data, :policies, [])}

      {{:static, data}, {:index, id}} ->
        {:ok, Enum.filter(Map.get(data, :index, []), &(&1["vdisk_id"] == id))}

      {:hydra, :vdisks} ->
        Hydra.query(@vdisks_cql, [])

      {:hydra, :policies} ->
        Hydra.query(@policies_cql, [])

      {:hydra, {:index, id}} ->
        Hydra.query(@index_cql, [{"text", id}])
    end
  end
end
