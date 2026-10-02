defmodule SpectrumPhxWeb.Lanayru.IndexLive do
  @moduledoc """
  The Kubernetes engine: whether this cluster can host one, and the one it is hosting.

  The pre-flight leads, because that is what the page is for. Finding out that the ring is
  degraded or the extent store is nearly full *during* a deploy is expensive, and each
  check here asks a real component a real question rather than reporting that a component
  is installed.

  Deploying and destroying are Catalyst tasks. Pressing either button queues work on the
  ZooKeeper leader and returns; what happens after that is reported by the ring in the
  header and by `/tasks`, which is the only honest way to present an operation that runs
  for minutes and can fail at any point in them.

  ## A blocked pre-flight blocks the deploy

  A check that came back `:error` is not advice. It means the deploy has already been
  established to be unable to succeed -- no overlay segment, an unmounted extent store, a
  ring with no member up -- so the button is disabled and the reasons are listed under it.
  Warnings do not block: a degraded ring or a nearly-full store is a judgement call, and
  it is the operator's.

  ## Destroy asks for the name

  Not a `data-confirm`, which is a browser dialog and guards only the browser, and not a
  second click either -- a second click is a reflex. The cluster's name has to be typed
  back, because the thing on the other side of this button is every guest node of a
  Kubernetes cluster and the rows describing them.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [panel: 1, figure: 1]

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Lanayru

  @refresh_interval_ms 30_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket), do: :timer.send_interval(@refresh_interval_ms, self(), :refresh)

    {:ok,
     socket
     |> assign(
       page_title: "Lanayru",
       deploy_error: nil,
       destroy_error: nil,
       confirming_destroy?: false,
       submitted: nil,
       form: default_form()
     )
     |> load()}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load(socket)}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, load(socket)}

  # Keeps what was typed across a re-render. Without it the periodic refresh empties a
  # half-filled form under the operator's hands.
  def handle_event("validate_deploy", params, socket) do
    {:noreply, assign(socket, form: take_form(params), deploy_error: nil)}
  end

  def handle_event("deploy", params, socket) do
    case Lanayru.deploy(take_form(params)) do
      {:ok, task_id} ->
        {:noreply,
         socket
         |> assign(deploy_error: nil, submitted: {:deploy, task_id}, form: default_form())
         |> put_flash(
           :info,
           "Kubernetes deployment queued. It runs on the leader; watch the ring."
         )
         |> load()}

      {:error, message} ->
        {:noreply, assign(socket, form: take_form(params), deploy_error: message)}
    end
  end

  def handle_event("ask_destroy", _params, socket) do
    {:noreply, assign(socket, confirming_destroy?: true, destroy_error: nil, submitted: nil)}
  end

  def handle_event("cancel_destroy", _params, socket) do
    {:noreply, assign(socket, confirming_destroy?: false, destroy_error: nil)}
  end

  def handle_event("destroy", params, socket) do
    case Lanayru.destroy(Map.get(params, "confirmation", "")) do
      {:ok, task_id} ->
        {:noreply,
         socket
         |> assign(confirming_destroy?: false, destroy_error: nil, submitted: {:destroy, task_id})
         |> put_flash(
           :info,
           "Teardown queued. The cluster is removed by the task, not by this page."
         )
         |> load()}

      {:error, message} ->
        {:noreply, assign(socket, destroy_error: message)}
    end
  end

  defp default_form,
    do: %{"cluster_name" => "", "control_nodes" => "1", "overlay_segment_id" => ""}

  defp take_form(params) do
    Map.take(params, ["cluster_name", "control_nodes", "overlay_segment_id"])
  end

  defp load(socket) do
    overview = Lanayru.overview()
    blocking = Enum.filter(overview.checks, &(&1.status == :error))

    socket
    |> assign(:overview, overview)
    |> assign(:ready?, Lanayru.ready?(overview.checks))
    |> assign(:blocking, blocking)
    |> assign(:node_count, max(length(Config.node_ips()), 1))
    |> assign(:read_at, DateTime.utc_now())
  end

  defp submitted_word({:deploy, _id}), do: "Deployment"
  defp submitted_word({:destroy, _id}), do: "Teardown"

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app
      socket={@socket}
      flash={@flash}
      current_username={@current_username}
      active={:lanayru}
    >
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
            <.figure
              id="k8s-name"
              label="Name"
              value={@overview.cluster.name}
              caption="cluster"
              tone={:primary}
            />
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

        <.panel
          :if={is_nil(@overview.cluster)}
          id="lanayru-deploy"
          title="Deploy a Kubernetes cluster"
          subtitle="Queued as a Catalyst task and run on the ZooKeeper leader"
        >
          <p :if={@blocking != []} class="text-sm text-error" id="deploy-blocked">
            The pre-flight has already established that this cannot succeed:
          </p>
          <ul :if={@blocking != []} class="text-xs text-error list-disc ml-5 mt-1">
            <li :for={check <- @blocking}>{check.message}</li>
          </ul>

          <form
            id="deploy-form"
            phx-submit="deploy"
            phx-change="validate_deploy"
            class="mt-3 flex flex-col gap-3"
          >
            <div class="grid gap-3 sm:grid-cols-3">
              <label class="form-control">
                <span class="label-text text-xs opacity-70">Cluster name</span>
                <input
                  type="text"
                  name="cluster_name"
                  value={@form["cluster_name"]}
                  class="input input-bordered input-sm w-full"
                  autocomplete="off"
                  id="deploy-cluster-name"
                />
              </label>
              <label class="form-control">
                <span class="label-text text-xs opacity-70">Control-plane nodes</span>
                <input
                  type="number"
                  name="control_nodes"
                  value={@form["control_nodes"]}
                  min="1"
                  max={@node_count}
                  class="input input-bordered input-sm w-full"
                  id="deploy-control-nodes"
                />
              </label>
              <label class="form-control">
                <span class="label-text text-xs opacity-70">Overlay segment</span>
                <select
                  name="overlay_segment_id"
                  class="select select-bordered select-sm w-full"
                  id="deploy-segment"
                >
                  <option value="">default routing elements</option>
                  <option
                    :for={segment <- @overview.segments}
                    value={to_string(segment.id)}
                    selected={to_string(segment.id) == @form["overlay_segment_id"]}
                  >
                    {segment.name}
                  </option>
                </select>
              </label>
            </div>

            <p :if={@deploy_error} class="text-sm text-error" id="deploy-error">{@deploy_error}</p>

            <div class="flex items-center gap-3">
              <button
                type="submit"
                class="btn btn-primary btn-sm"
                disabled={@blocking != []}
                id="deploy-submit"
              >
                <.icon name="hero-rocket-launch" class="size-4" /> Deploy
              </button>
              <span :if={@blocking == [] and not @ready?} class="text-xs text-warning">
                The pre-flight has warnings. Read them before you press this.
              </span>
            </div>
          </form>
        </.panel>

        <.panel
          :if={@overview.cluster}
          id="lanayru-destroy"
          title="Destroy this cluster"
          subtitle="Every guest node of it, and the rows describing them"
        >
          <p class="text-sm opacity-70">
            Teardown removes the Kubernetes guest VMs and the state Lanayru keeps for them
            in Hydra. It runs as a Catalyst task, so its progress and its failure are in the
            task ring rather than in a request that returned.
          </p>

          <div :if={not @confirming_destroy?} class="mt-3">
            <button
              phx-click="ask_destroy"
              class="btn btn-error btn-outline btn-sm"
              id="destroy-start"
            >
              <.icon name="hero-trash" class="size-4" /> Destroy {@overview.cluster.name}
            </button>
          </div>

          <form
            :if={@confirming_destroy?}
            phx-submit="destroy"
            class="alert alert-error alert-soft mt-3 flex-col items-start gap-2"
            id="destroy-confirm"
          >
            <span class="text-sm">
              Type <span class="font-mono font-semibold">{@overview.cluster.name}</span>
              to confirm. This cannot be undone from here.
            </span>
            <div class="flex flex-wrap items-center gap-2">
              <input
                type="text"
                name="confirmation"
                class="input input-bordered input-sm"
                autocomplete="off"
                id="destroy-confirmation"
              />
              <button type="submit" class="btn btn-error btn-xs" id="destroy-submit">
                Destroy it
              </button>
              <button type="button" phx-click="cancel_destroy" class="btn btn-ghost btn-xs">
                Cancel
              </button>
            </div>
            <p :if={@destroy_error} class="text-sm" id="destroy-error">{@destroy_error}</p>
          </form>
        </.panel>

        <.panel :if={is_nil(@overview.cluster)} id="lanayru-none" title="No cluster deployed">
          <p class="text-sm opacity-70">
            Nothing is recorded in <span class="font-mono">hydra.lanayru_clusters</span>.
            The pre-flight above is what a deploy is checked against.
          </p>
        </.panel>

        <p :if={@submitted} class="text-xs opacity-70" id="lanayru-task">
          {submitted_word(@submitted)} submitted as
          <span class="font-mono">{elem(@submitted, 1)}</span>
          &mdash; <.link navigate={~p"/tasks"} class="link">watch it</.link>.
        </p>
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
