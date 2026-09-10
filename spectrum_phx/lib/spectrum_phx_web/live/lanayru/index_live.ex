defmodule SpectrumPhxWeb.Lanayru.IndexLive do
  @moduledoc """
  The Kubernetes engine: whether this cluster can host one, and the one it is hosting.

  The pre-flight leads, because that is what the page is for. Finding out that the ring is
  degraded or the extent store is nearly full *during* a deploy is expensive, and each
  check here asks a real component a real question rather than reporting that a component
  is installed.

  Deploying and destroying are not offered from this console yet: both are cluster-wide
  Catalyst tasks, and they belong behind the task queue where the header ring reports them
  rather than behind a button that returns instantly and leaves the work happening
  somewhere.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [panel: 1, figure: 1]

  alias SpectrumPhx.Lanayru

  @refresh_interval_ms 30_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket), do: :timer.send_interval(@refresh_interval_ms, self(), :refresh)

    {:ok, socket |> assign(page_title: "Lanayru") |> load()}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load(socket)}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, load(socket)}

  defp load(socket) do
    overview = Lanayru.overview()

    socket
    |> assign(:overview, overview)
    |> assign(:ready?, Lanayru.ready?(overview.checks))
    |> assign(:read_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:lanayru}>
      <.header>
        Lanayru
        <:subtitle>
          <span class="flex flex-wrap items-center gap-2 mt-1">
            <span class="text-xs opacity-60">
              Kubernetes engine · checked {Calendar.strftime(@read_at, "%H:%M:%S")} UTC
            </span>
            <span :if={@ready?} class="badge badge-sm badge-success gap-1">
              <.icon name="hero-check" class="size-3" /> ready to deploy
            </span>
            <span :if={not @ready?} class="badge badge-sm badge-warning gap-1">
              <.icon name="hero-exclamation-triangle" class="size-3" /> not ready
            </span>
          </span>
        </:subtitle>
        <:actions>
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Re-check
          </.button>
        </:actions>
      </.header>

      <div class="flex flex-col gap-4">
        <.panel
          :if={@overview.cluster}
          id="lanayru-cluster"
          title="Kubernetes cluster"
          subtitle="On record in hydra.lanayru_clusters"
        >
          <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <.figure id="k8s-name" label="Name" value={@overview.cluster.name} caption="cluster" tone={:primary} />
            <.figure
              id="k8s-status"
              label="Status"
              value={@overview.cluster.status}
              caption="last recorded"
              tone={status_tone(@overview.cluster.status)}
            />
            <.figure
              id="k8s-control"
              label="Control nodes"
              value={@overview.cluster.control_nodes}
              caption="control plane"
            />
            <.figure
              id="k8s-segment"
              label="Segment"
              value={segment_name(@overview)}
              caption="overlay network"
            />
          </div>
        </.panel>

        <.panel
          id="lanayru-preflight"
          title="Pre-flight"
          subtitle="Asked of the components themselves, not of whether they are installed"
        >
          <ul class="flex flex-col gap-3">
            <li
              :for={check <- @overview.checks}
              class="flex items-start gap-3"
              id={"check-#{check.id}"}
            >
              <span class={["status-dot mt-1.5 shrink-0", check_class(check.status)]}></span>
              <div class="min-w-0">
                <p class="text-sm font-medium">
                  {check.label}
                  <span class={["badge badge-xs ml-1", badge_class(check.status)]}>
                    {check.status}
                  </span>
                </p>
                <p class="text-xs opacity-70">{check.message}</p>
              </div>
            </li>
          </ul>
        </.panel>

        <.panel :if={is_nil(@overview.cluster)} id="lanayru-none" title="No cluster deployed">
          <p class="text-sm opacity-70">
            Nothing is recorded in <span class="font-mono">hydra.lanayru_clusters</span>.
            The pre-flight above is what a deploy would be checked against.
          </p>
        </.panel>

        <.panel id="lanayru-not-offered" title="Not offered here yet">
          <p class="text-sm opacity-70">
            Deploying and destroying a Kubernetes cluster are cluster-wide operations that
            run for minutes and fail in interesting ways. They belong behind the task queue,
            where the ring in the header reports their progress -- not behind a button that
            returns instantly and leaves the work happening somewhere. Both remain on the
            <.link href="/lanayru.html" class="link">previous console</.link>
            until they run as tasks.
          </p>
        </.panel>
      </div>
    </Layouts.app>
    """
  end

  defp segment_name(%{cluster: nil}), do: nil

  defp segment_name(%{cluster: cluster, segments: segments}) do
    case Enum.find(segments, &(to_string(&1.id) == to_string(cluster.segment_id))) do
      nil -> nil
      segment -> segment.name
    end
  end

  defp check_class(:ready), do: "text-success"
  defp check_class(:warning), do: "text-warning"
  defp check_class(:error), do: "text-error"

  defp badge_class(:ready), do: "badge-success"
  defp badge_class(:warning), do: "badge-warning"
  defp badge_class(:error), do: "badge-error"

  defp status_tone(status) when is_binary(status) do
    case String.downcase(status) do
      "running" -> :good
      "ready" -> :good
      "failed" -> :bad
      "error" -> :bad
      _ -> :neutral
    end
  end

  defp status_tone(_status), do: :neutral
end
