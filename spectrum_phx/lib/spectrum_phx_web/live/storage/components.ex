defmodule SpectrumPhxWeb.Storage.Components do
  @moduledoc """
  Presentation pieces for the storage fabric view.

  The rule these encode is the same one the cluster badges encode, applied to storage:
  **nothing unknown is drawn as healthy**. A vdisk whose owner did not answer, a store
  whose filesystem reported no capacity, a replica on a node no peer can reach, and a
  node that did not answer at all each get their own colour, and none of those colours is
  green. The old storage page had one green "ONLINE" pill driven by the substring "ok"
  appearing in a table cell, and a resource in the middle of a resync rendered exactly
  like one that was fully replicated.

  There are fewer badges here than there were, because there are fewer states. Under DRBD
  a copy could be `Inconsistent`, `Outdated` or resyncing, and each needed its own colour.
  A sealed extent group is immutable, so a copy is either byte-identical or absent; the
  question a replica badge answers is "are all of them here", not "do they agree".
  """
  use SpectrumPhxWeb, :html

  @doc "Overall health of one vdisk."
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
  A vdisk's replica count against what the cluster's redundancy factor asks for.

  `nil` for `have` means a node did not answer, so the count is unknown rather than
  short -- drawn as a warning, never as a satisfied count.
  """
  attr :have, :integer, default: nil
  attr :want, :integer, required: true

  def replica_badge(assigns) do
    ~H"""
    <span class={[
      "badge badge-sm gap-1 font-mono text-xs",
      replica_class(@have, @want)
    ]}>
      <span class="opacity-70">replicas</span>
      {if is_nil(@have), do: "?", else: @have}/{@want}
    </span>
    """
  end

  @doc """
  Which node serves a vdisk, and in what role.

  A forwarding node is not a fault: a non-owner relays I/O to the owner, which is what
  removes live migration's cutover instant. It is drawn plainly, not warned about.
  """
  attr :attachment, :map, required: true

  def role_badge(assigns) do
    ~H"""
    <span class={[
      "badge badge-sm gap-1 font-mono text-xs",
      role_class(@attachment.role)
    ]}>
      {@attachment.hostname}: {role_label(@attachment.role)}
      <span :if={@attachment.role == :forwarding and @attachment.forwarding_to} class="opacity-70">
        &rarr; {@attachment.forwarding_to}
      </span>
    </span>
    """
  end

  @doc """
  A vdisk's epoch, the number the ownership fence turns on.

  Shown because it is the one piece of state that explains a refusal: a replica fenced at
  a higher epoch refuses the old owner's writes, and the epoch is how an operator sees
  that a takeover happened at all.
  """
  attr :epoch, :integer, default: nil

  def epoch_badge(assigns) do
    ~H"""
    <span :if={@epoch} class="badge badge-sm badge-ghost font-mono text-xs">
      <span class="opacity-70">epoch</span>&nbsp;{@epoch}
    </span>
    """
  end

  @doc "State of one node's extent store."
  attr :state, :atom, required: true

  def store_state_badge(assigns) do
    ~H"""
    <span class={["badge badge-sm gap-1 font-medium", store_class(@state)]}>
      {store_label(@state)}
    </span>
    """
  end

  @doc """
  A usage bar. Rendered only when the capacity is actually known -- a store whose
  filesystem did not answer gets a plain "capacity unknown" line instead of a bar sitting
  reassuringly at zero.
  """
  attr :used, :integer, default: nil
  attr :total, :integer, default: nil
  attr :percent, :float, default: nil

  def usage_bar(assigns) do
    ~H"""
    <div :if={is_nil(@percent)} class="text-xs text-warning font-medium">
      Capacity unknown -- the extent store reported no totals, which usually means it is
      not mounted.
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

  Deliberately not an empty table: "there are no vdisks" and "the vdisk list could not be
  read" are different statements, and the second one rendered as the first is the failure
  this page exists to prevent.
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

  defp replica_class(nil, _want), do: "badge-warning"
  defp replica_class(have, want) when have >= want, do: "badge-success"
  defp replica_class(_have, _want), do: "badge-error"

  defp role_class(:owner), do: "badge-primary"
  defp role_class(:forwarding), do: "badge-ghost"
  defp role_class(_other), do: "badge-warning"

  defp role_label(:owner), do: "owner"
  defp role_label(:forwarding), do: "forwarding"
  defp role_label(_other), do: "unknown role"

  defp store_class(:ok), do: "badge-success"
  defp store_class(:warn), do: "badge-warning"
  defp store_class(:full), do: "badge-error"
  defp store_class(_other), do: "badge-warning"

  defp store_label(:ok), do: "ok"
  defp store_label(:warn), do: "filling"
  defp store_label(:full), do: "full"
  defp store_label(_other), do: "unknown"

  defp usage_class(percent) when percent >= 85, do: "progress-error"
  defp usage_class(percent) when percent >= 70, do: "progress-warning"
  defp usage_class(_percent), do: "progress-primary"
end
