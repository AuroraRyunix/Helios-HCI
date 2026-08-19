defmodule SpectrumPhxWeb.Storage.Components do
  @moduledoc """
  Presentation pieces for the storage fabric view.

  The rule these encode is the same one the cluster badges encode, applied to storage:
  **nothing unknown is drawn as healthy**. A DRBD device that is `Inconsistent`, a
  connection that is `StandAlone`, a pool whose capacity LINSTOR would not report, and a
  node that did not answer at all each get their own colour, and none of those colours is
  green. The old storage page had one green "ONLINE" pill driven by the substring "ok"
  appearing in a table cell, and a resource in the middle of a resync rendered exactly
  like one that was fully replicated.
  """
  use SpectrumPhxWeb, :html

  # Not module attributes: inside `~H` a `@name` is an assign, so `@uptodate` in a
  # template would read `assigns.uptodate` and crash rather than compare a string.
  defp disk_ok?(state), do: state == "UpToDate"
  defp link_ok?(state), do: state == "Connected"
  defp replication_ok?(state), do: state == "Established"

  @doc "Overall health of one DRBD resource."
  attr :health, :atom, required: true

  def health_badge(assigns) do
    ~H"""
    <span class={["badge badge-sm gap-1 font-medium", health_class(@health)]}>
      <.icon name={health_icon(@health)} class="size-3" />
      {health_label(@health)}
    </span>
    """
  end

  @doc """
  A DRBD disk state, coloured by whether it is the only state that means "this copy is
  a complete copy". `Inconsistent`, `Outdated`, `Failed`, `Attaching` and `DUnknown` are
  all not that, and none of them is green here.
  """
  attr :state, :string, required: true
  attr :label, :string, default: nil

  def disk_state_badge(assigns) do
    ~H"""
    <span class={[
      "badge badge-sm gap-1 font-mono text-xs",
      if(disk_ok?(@state), do: "badge-success", else: "badge-error")
    ]}>
      <span :if={@label} class="opacity-70">{@label}</span>
      {@state}
    </span>
    """
  end

  @doc "A DRBD connection state and the peer role behind it."
  attr :connection, :map, required: true

  def connection_badge(assigns) do
    ~H"""
    <span class={[
      "badge badge-sm gap-1 font-mono text-xs",
      if(link_ok?(@connection.state), do: "badge-success", else: "badge-error")
    ]}>
      {@connection.peer}: {@connection.state}
      <span class="opacity-70">({@connection.peer_role})</span>
    </span>
    """
  end

  @doc "Replication state for one peer volume: resync is not the same as replicated."
  attr :peer, :map, required: true

  def replication_badge(assigns) do
    ~H"""
    <span class={[
      "badge badge-sm font-mono text-xs",
      if(replication_ok?(@peer.replication), do: "badge-ghost", else: "badge-warning")
    ]}>
      {@peer.replication}
    </span>
    """
  end

  @doc "State of one LINSTOR storage pool."
  attr :state, :atom, required: true

  def pool_state_badge(assigns) do
    ~H"""
    <span class={["badge badge-sm gap-1 font-medium", pool_class(@state)]}>
      {pool_label(@state)}
    </span>
    """
  end

  @doc """
  A usage bar. Rendered only when the capacity is actually known -- a pool whose totals
  LINSTOR did not report gets a plain "capacity unknown" line instead of a bar sitting
  reassuringly at zero.
  """
  attr :used, :integer, default: nil
  attr :total, :integer, default: nil
  attr :percent, :float, default: nil

  def usage_bar(assigns) do
    ~H"""
    <div :if={is_nil(@percent)} class="text-xs text-warning font-medium">
      Capacity unknown -- LINSTOR did not report totals for this pool.
    </div>

    <div :if={@percent} class="space-y-1">
      <div class="flex justify-between text-xs opacity-70">
        <span>{bytes(@used)} used</span>
        <span>{bytes(@total)} total</span>
      </div>
      <progress class={["progress w-full", usage_class(@percent)]} value={@percent} max="100"></progress>
      <div class="text-xs font-semibold">{format_percent(@percent)} used</div>
    </div>
    """
  end

  @doc """
  Shown in place of a section whose source could not be read.

  Deliberately not an empty table: "there are no storage pools" and "the storage pool
  list could not be read" are different statements, and the second one rendered as the
  first is the failure this page exists to prevent.
  """
  attr :title, :string, required: true
  attr :error, :string, default: nil
  attr :id, :string, required: true

  def unavailable(assigns) do
    ~H"""
    <div class="alert alert-error alert-soft items-start" id={@id}>
      <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
      <div>
        <p class="font-semibold">{@title}</p>
        <p class="text-sm opacity-90">
          This is not an empty result. Nothing is known about it, so nothing here should
          be read as healthy.
        </p>
        <p :if={@error} class="text-xs opacity-70 mt-1 font-mono break-all">{@error}</p>
      </div>
    </div>
    """
  end

  @doc "Human-readable byte count. `nil` renders as an explicit unknown, never as zero."
  def bytes(nil), do: "unknown"

  def bytes(count) when is_integer(count) do
    cond do
      count >= 1024 * 1024 * 1024 * 1024 -> scaled(count, 1024 * 1024 * 1024 * 1024, "TiB")
      count >= 1024 * 1024 * 1024 -> scaled(count, 1024 * 1024 * 1024, "GiB")
      count >= 1024 * 1024 -> scaled(count, 1024 * 1024, "MiB")
      count >= 1024 -> scaled(count, 1024, "KiB")
      true -> Integer.to_string(count) <> " B"
    end
  end

  def bytes(_other), do: "unknown"

  @doc "A percentage to one decimal place."
  def format_percent(nil), do: "unknown"

  def format_percent(value) when is_number(value) do
    :erlang.float_to_binary(value / 1, decimals: 1) <> "%"
  end

  @doc "A DOM-safe slug, so tests and JS can target rows by name."
  def slug(value) do
    value |> to_string() |> String.downcase() |> String.replace(~r/[^a-z0-9]+/, "-")
  end

  defp scaled(count, unit, suffix) do
    :erlang.float_to_binary(count / unit, decimals: 1) <> " " <> suffix
  end

  defp health_class(:ok), do: "badge-success"
  defp health_class(:degraded), do: "badge-error"
  defp health_class(_other), do: "badge-warning"

  defp health_icon(:ok), do: "hero-check-circle"
  defp health_icon(:degraded), do: "hero-x-circle"
  defp health_icon(_other), do: "hero-question-mark-circle"

  defp health_label(:ok), do: "healthy"
  defp health_label(:degraded), do: "degraded"
  defp health_label(_other), do: "unknown"

  defp pool_class(:ok), do: "badge-success"
  defp pool_class(:error), do: "badge-error"
  defp pool_class(:diskless), do: "badge-ghost"
  defp pool_class(_other), do: "badge-warning"

  defp pool_label(:ok), do: "ok"
  defp pool_label(:error), do: "error"
  defp pool_label(:diskless), do: "diskless"
  defp pool_label(_other), do: "unknown"

  defp usage_class(percent) when percent >= 85, do: "progress-error"
  defp usage_class(percent) when percent >= 70, do: "progress-warning"
  defp usage_class(_percent), do: "progress-primary"
end
