defmodule SpectrumPhxWeb.Metrics.Components do
  @moduledoc """
  Presentation pieces for the cluster telemetry dashboard.

  The charts are server-rendered SVG rather than a canvas the browser fills from a REST
  poll. That is not a stylistic choice: the old page redrew six canvases from a full-table
  scan every thirty seconds, and each redraw measured the element, reset its backing store
  and repainted from scratch, so every panel flickered and no two of them agreed on which
  instant they were showing. A `<polyline>` whose `points` attribute LiveView patches has
  neither problem.

  A node that is not reporting is never drawn as a node at 0%. Silence and idleness look
  identical on a chart and mean opposite things.
  """
  use SpectrumPhxWeb, :html

  alias SpectrumPhx.Metrics

  @doc "A labelled percentage bar. `nil` renders as 'no data', not as zero."
  attr :label, :string, required: true
  attr :value, :any, default: nil
  attr :id, :string, default: nil

  def gauge(assigns) do
    ~H"""
    <div class="flex-1 min-w-32" id={@id}>
      <div class="flex items-baseline justify-between text-xs">
        <span class="opacity-60">{@label}</span>
        <span :if={is_number(@value)} class="font-semibold tabular-nums">
          {percent(@value)}
        </span>
        <span :if={not is_number(@value)} class="opacity-50 italic">no data</span>
      </div>
      <progress
        :if={is_number(@value)}
        class={["progress w-full h-1.5 mt-1", load_class(@value)]}
        value={Float.round(@value * 1.0, 1)}
        max="100"
      ></progress>
      <div :if={not is_number(@value)} class="h-1.5 mt-1 rounded bg-base-300"></div>
    </div>
    """
  end

  @doc """
  A sparkline over one field of a node's samples.

  Fewer than two samples draws nothing but a caption saying so: one point is not a trend,
  and a flat line across the panel would assert a history the database does not hold.
  """
  attr :samples, :list, required: true
  attr :field, :atom, required: true
  attr :max, :any, default: 100
  attr :class, :string, default: "text-primary"
  attr :id, :string, default: nil

  def sparkline(assigns) do
    assigns =
      assign(assigns, :points, Metrics.spark_points(assigns.samples, assigns.field, assigns.max))

    ~H"""
    <div class="h-10 w-full" id={@id}>
      <svg
        :if={@points != []}
        viewBox="0 0 100 100"
        preserveAspectRatio="none"
        class={["h-full w-full", @class]}
        aria-hidden="true"
      >
        <polyline
          points={Enum.map_join(@points, " ", fn {x, y} -> "#{x},#{y}" end)}
          fill="none"
          stroke="currentColor"
          stroke-width="2"
          stroke-linejoin="round"
          stroke-linecap="round"
          vector-effect="non-scaling-stroke"
        />
      </svg>
      <p :if={@points == []} class="text-xs opacity-40 italic pt-3">
        not enough samples to plot
      </p>
    </div>
    """
  end

  @doc "Liveness and freshness badges for one node."
  attr :node, :map, required: true

  def node_badges(assigns) do
    ~H"""
    <span class={[
      "badge badge-sm gap-1 font-medium",
      case @node.state do
        :up -> "badge-success"
        :down -> "badge-error"
        _other -> "badge-ghost"
      end
    ]}>
      <.icon
        name={if @node.state == :up, do: "hero-signal", else: "hero-signal-slash"}
        class="size-3"
      />
      {node_state_label(@node.state)}
    </span>

    <span :if={not @node.reporting?} class="badge badge-sm badge-warning gap-1 font-medium">
      <.icon name="hero-no-symbol" class="size-3" /> no telemetry
    </span>

    <span
      :if={@node.reporting? and @node.stale?}
      class="badge badge-sm badge-warning gap-1 font-medium"
      title={"Newest sample is #{@node.age_seconds}s old"}
    >
      <.icon name="hero-clock" class="size-3" /> stale {@node.age_seconds}s
    </span>

    <span :if={@node.in_maintenance?} class="badge badge-sm badge-warning gap-1 font-medium">
      <.icon name="hero-wrench-screwdriver" class="size-3" /> maintenance
    </span>
    """
  end

  @doc "Shown when `hydra.logos_metrics` could not be read at all."
  attr :error, :any, required: true

  def db_unavailable(assigns) do
    ~H"""
    <div class="alert alert-warning items-start" id="metrics-db-error">
      <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
      <div>
        <p class="font-semibold">Telemetry unavailable</p>
        <p class="text-sm opacity-90">
          <code class="font-mono">hydra.logos_metrics</code>
          could not be read. The nodes below are shown with no data rather than with zeroes:
          a silent collector and an idle host are not the same thing.
        </p>
        <p class="text-xs opacity-70 mt-1 font-mono break-all">{@error}</p>
      </div>
    </div>
    """
  end

  @doc "Shown when there is no cluster configured at all."
  def no_cluster(assigns) do
    ~H"""
    <div class="alert alert-warning items-start" id="metrics-no-cluster">
      <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
      <div>
        <p class="font-semibold">No cluster configured</p>
        <p class="text-sm opacity-90">
          Neither <code class="font-mono">/etc/hci/cluster.json</code>
          nor <code class="font-mono">hydra.logos_metrics</code>
          named any node, so there is nothing to chart.
        </p>
      </div>
    </div>
    """
  end

  @doc "A percentage, to one decimal place."
  def percent(value) when is_number(value),
    do: :erlang.float_to_binary(value * 1.0, decimals: 1) <> "%"

  def percent(_value), do: "-"

  @doc "A rate, to one decimal place, with its unit."
  def rate(value, unit) when is_number(value) do
    :erlang.float_to_binary(value * 1.0, decimals: 1) <> " " <> unit
  end

  def rate(_value, _unit), do: "-"

  @doc "Kibibytes as a human-readable size."
  def from_kb(nil), do: "-"

  def from_kb(kb) when is_number(kb) do
    cond do
      kb >= 1_048_576 -> :erlang.float_to_binary(kb / 1_048_576, decimals: 1) <> " GiB"
      kb >= 1_024 -> :erlang.float_to_binary(kb / 1_024, decimals: 0) <> " MiB"
      true -> "#{round(kb)} KiB"
    end
  end

  @doc "A duration in seconds, as a short human string."
  def humanize_age(nil), do: "never"
  def humanize_age(seconds) when seconds < 60, do: "#{seconds}s"
  def humanize_age(seconds) when seconds < 3_600, do: "#{div(seconds, 60)}m"
  def humanize_age(seconds) when seconds < 86_400, do: "#{div(seconds, 3_600)}h"
  def humanize_age(seconds), do: "#{div(seconds, 86_400)}d"

  defp node_state_label(:up), do: "UP"
  defp node_state_label(:down), do: "DOWN"
  defp node_state_label(_other), do: "UNKNOWN"

  defp load_class(value) when value >= 90, do: "progress-error"
  defp load_class(value) when value >= 75, do: "progress-warning"
  defp load_class(_value), do: "progress-primary"
end
