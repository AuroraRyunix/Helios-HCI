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
