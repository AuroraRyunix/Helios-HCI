defmodule SpectrumPhxWeb.Cluster.Components do
  @moduledoc """
  Presentation pieces shared by the cluster dashboards.

  The one rule these encode: `FLAPPING` never looks like `UP`. A crash-looping unit
  reads `active` to systemd for most of a sampling window, and the previous UI showed
  that as healthy. Here it gets its own colour, its own icon and its restart count.
  """
  use SpectrumPhxWeb, :html

  @doc "Colour-coded badge for a single service's status."
  attr :status, :string, required: true
  attr :restarts, :integer, default: 0
  attr :class, :any, default: nil

  def service_badge(assigns) do
    ~H"""
    <span class={[
      "badge badge-sm gap-1 font-medium whitespace-nowrap",
      status_class(@status),
      @class
    ]}>
      <.icon :if={@status == "FLAPPING"} name="hero-arrow-path" class="size-3 animate-spin" />
      <.icon :if={@status == "DOWN"} name="hero-x-circle" class="size-3" />
      <.icon :if={@status == "UP"} name="hero-check-circle" class="size-3" />
      {@status}
      <span :if={@status == "FLAPPING" and @restarts > 0} class="opacity-80">
        &times;{@restarts}
      </span>
    </span>
    """
  end

  @doc "Badge for whether the node itself is registered and alive."
  attr :node, :map, required: true

  def node_badge(assigns) do
    ~H"""
    <span class={[
      "badge badge-sm gap-1 font-medium",
      if(@node.state == :up, do: "badge-success", else: "badge-error")
    ]}>
      <.icon
        name={if @node.state == :up, do: "hero-signal", else: "hero-signal-slash"}
        class="size-3"
      />
      {if @node.state == :up, do: "UP", else: "DOWN"}
    </span>
    """
  end

  @doc """
  Staleness marker, rendered only when the node's document is older than the threshold.
  A node can be registered and still be publishing nothing new; that is worth seeing.
  """
  attr :node, :map, required: true

  def staleness(assigns) do
    ~H"""
    <span
      :if={@node.stale?}
      class="badge badge-sm badge-warning gap-1 font-medium"
      title={"Last published #{@node.age_seconds}s ago"}
    >
      <.icon name="hero-clock" class="size-3" /> stale {@node.age_seconds}s
    </span>
    """
  end

  @doc "Where this snapshot came from, and what that means for its trustworthiness."
  attr :snapshot, :map, required: true

  def source_note(assigns) do
    ~H"""
    <span class={["badge badge-sm gap-1", source_class(@snapshot.source)]} id="data-source">
      <.icon name={source_icon(@snapshot.source)} class="size-3" />
      {source_label(@snapshot.source)}
    </span>
    """
  end

  @doc "Desired cluster state as published to ZooKeeper, if any."
  attr :desired, :string, default: nil

  def desired_badge(assigns) do
    ~H"""
    <span class="badge badge-sm badge-ghost gap-1" id="desired-state">
      <.icon name="hero-flag" class="size-3" /> desired: {@desired || "unset"}
    </span>
    """
  end

  @doc "The full service table for one node."
  attr :node, :map, required: true
  attr :id, :string, required: true

  def service_table(assigns) do
    ~H"""
    <div :if={@node.services == []} class="text-sm text-base-content/60 italic">
      No services reported.
      <span :if={@node.state == :down}>
        This node has no ZooKeeper registration, so nothing is known about its units.
      </span>
    </div>

    <div :if={@node.services != []} class="overflow-x-auto">
      <table class="table table-zebra table-sm" id={@id}>
        <thead>
          <tr>
            <th>Service</th>
            <th>Status</th>
            <th>PIDs</th>
            <th>Restarts</th>
          </tr>
        </thead>
        <tbody>
          <tr :for={service <- @node.services} id={"#{@id}-#{dom_slug(service.name)}"}>
            <td class="font-medium">{service.name}</td>
            <td><.service_badge status={service.status} restarts={service.restarts} /></td>
            <td class="font-mono text-xs">
              {if service.pids == [], do: "-", else: Enum.join(service.pids, ", ")}
            </td>
            <td class={[
              "text-xs tabular-nums",
              service.restarts > 0 && "text-warning font-semibold"
            ]}>
              {service.restarts}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    """
  end

  @doc """
  Shown when there is no cluster to talk about at all -- a fresh dev machine has no
  `/etc/hci/cluster.json`. Saying so plainly beats rendering an empty dashboard that
  looks like a healthy cluster of zero nodes.
  """
  attr :snapshot, :map, required: true

  def no_cluster(assigns) do
    ~H"""
    <div class="alert alert-warning items-start" id="no-cluster">
      <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
      <div>
        <p class="font-semibold">No cluster configured</p>
        <p class="text-sm opacity-90">
          Neither <code class="font-mono">/etc/hci/cluster.json</code>
          nor ZooKeeper reported any nodes, so there is nothing to show.
        </p>
        <p :if={@snapshot.error} class="text-xs opacity-70 mt-1 font-mono break-all">
          {@snapshot.error}
        </p>
      </div>
    </div>
    """
  end

  @doc """
  A panel on the dashboard.

  The console's surface is a translucent, blurred card on a deep field -- see
  `glass-card` in assets/css/app.css. daisyUI's `card` is opaque and flat, and a
  dashboard built out of those reads as a different product wearing the same name.
  """
  attr :title, :string, default: nil
  attr :subtitle, :string, default: nil
  attr :class, :any, default: nil
  attr :id, :string, default: nil
  slot :actions
  slot :inner_block, required: true

  def panel(assigns) do
    ~H"""
    <section id={@id} class={["glass-card glass-card-hover p-4 sm:p-5", @class]}>
      <header :if={@title} class="flex items-start justify-between gap-3 mb-3">
        <div class="min-w-0">
          <h2 class="panel-title">{@title}</h2>
          <p :if={@subtitle} class="text-xs opacity-55 mt-0.5">{@subtitle}</p>
        </div>
        <div :if={@actions != []} class="flex-none">{render_slot(@actions)}</div>
      </header>
      {render_slot(@inner_block)}
    </section>
    """
  end

  @doc """
  One headline number, with a label above and a caption below.

  `value` of `nil` renders as an em dash rather than as zero: a figure the cluster did
  not report and a figure that is genuinely zero are different statements, and only one
  of them is reassuring.
  """
  attr :label, :string, required: true
  attr :value, :any, default: nil
  attr :caption, :string, default: nil
  attr :tone, :atom, default: :neutral, values: [:neutral, :good, :warn, :bad, :primary]
  attr :id, :string, default: nil

  def figure(assigns) do
    ~H"""
    <div id={@id} class="min-w-0">
      <p class="panel-title truncate">{@label}</p>
      <p class={["text-2xl font-semibold tabular-nums leading-tight mt-1", tone_class(@tone)]}>
        <span :if={@value not in [nil, ""]}>{@value}</span>
        <span :if={@value in [nil, ""]} class="opacity-40">&mdash;</span>
      </p>
      <p :if={@caption} class="text-xs opacity-55 truncate">{@caption}</p>
    </div>
    """
  end

  defp tone_class(:good), do: "text-success"
  defp tone_class(:warn), do: "text-warning"
  defp tone_class(:bad), do: "text-error"
  defp tone_class(:primary), do: "text-primary"
  defp tone_class(_), do: nil

  @doc """
  A filled area chart over a series of `{x, y}` points in a 0..100 viewBox.

  Server-rendered SVG, like the sparkline on the telemetry page and for the same reason:
  the canvases this replaces were redrawn from a full-table scan on a browser timer, so
  every panel flickered and no two agreed on which instant they were showing. Here
  LiveView patches the `points` attribute and the browser does no work at all.

  The fill is a gradient to the panel floor rather than a solid, which is what makes a
  thin line legible at a glance without drawing more ink than the data deserves. The
  gradient needs an id unique in the document, so one is required.
  """
  attr :id, :string, required: true
  attr :points, :list, required: true
  attr :class, :any, default: "text-primary"
  attr :height, :string, default: "h-24"
  attr :empty_note, :string, default: "not enough samples to plot"

  def area_chart(assigns) do
    ~H"""
    <div class={["w-full", @height]} id={@id}>
      <svg
        :if={@points != []}
        viewBox="0 0 100 100"
        preserveAspectRatio="none"
        class={["h-full w-full overflow-visible", @class]}
        aria-hidden="true"
      >
        <defs>
          <linearGradient id={"#{@id}-fill"} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="currentColor" stop-opacity="0.35" />
            <stop offset="100%" stop-color="currentColor" stop-opacity="0.02" />
          </linearGradient>
        </defs>
        <polygon
          points={area_points(@points)}
          fill={"url(##{@id}-fill)"}
          stroke="none"
        />
        <polyline
          points={line_points(@points)}
          fill="none"
          stroke="currentColor"
          stroke-width="2"
          stroke-linejoin="round"
          stroke-linecap="round"
          vector-effect="non-scaling-stroke"
        />
      </svg>
      <p :if={@points == []} class="text-xs opacity-40 italic pt-6">{@empty_note}</p>
    </div>
    """
  end

  defp line_points(points), do: Enum.map_join(points, " ", fn {x, y} -> "#{x},#{y}" end)

  # The line, closed down to the baseline at both ends so the fill has an area. Only ever
  # called from the branch that has already established there are points to draw.
  defp area_points(points) do
    {first_x, _} = hd(points)
    {last_x, _} = List.last(points)
    "#{first_x},100 " <> line_points(points) <> " #{last_x},100"
  end

  @doc """
  A chart panel: a caption, the current reading, and the plot beneath it.

  `value` is drawn from the latest sample rather than from the end of the line, so a
  series that stops updating shows a stale reading next to a flat tail instead of
  silently asserting the last value is current.
  """
  attr :label, :string, required: true
  attr :value, :any, default: nil
  attr :ceiling, :string, default: nil
  attr :points, :list, required: true
  attr :tone, :string, default: "text-primary"
  attr :id, :string, required: true

  def chart_panel(assigns) do
    ~H"""
    <div class="glass-card glass-card-hover p-4" id={@id}>
      <div class="flex items-baseline justify-between gap-2">
        <span class="panel-title truncate">{@label}</span>
        <span :if={@ceiling} class="text-[0.65rem] opacity-40 tabular-nums">{@ceiling}</span>
      </div>
      <p class={["text-2xl font-semibold tabular-nums leading-tight mt-1", @tone]}>
        <span :if={@value not in [nil, ""]}>{@value}</span>
        <span :if={@value in [nil, ""]} class="opacity-40">&mdash;</span>
      </p>
      <.area_chart id={"#{@id}-plot"} points={@points} class={@tone} height="h-20" />
    </div>
    """
  end

  @doc "A DOM-safe slug for a service name, so tests and JS can target rows."
  def dom_slug(name) do
    name |> to_string() |> String.downcase() |> String.replace(~r/[^a-z0-9]+/, "-")
  end

  defp status_class("UP"), do: "badge-success"
  defp status_class("DOWN"), do: "badge-error"
  defp status_class("FLAPPING"), do: "badge-warning"
  defp status_class(_other), do: "badge-neutral"

  defp source_class(:zookeeper), do: "badge-info"
  defp source_class(:probe), do: "badge-warning"
  defp source_class(_other), do: "badge-ghost"

  defp source_icon(:zookeeper), do: "hero-circle-stack"
  defp source_icon(:probe), do: "hero-signal"
  defp source_icon(_other), do: "hero-question-mark-circle"

  defp source_label(:zookeeper), do: "source: ZooKeeper"
  defp source_label(:probe), do: "source: probe fallback"
  defp source_label(_other), do: "source: none"
end
