defmodule SpectrumPhxWeb.Lcm.IndexLive do
  @moduledoc """
  Life-cycle management: what is installed, what is available, and a rolling upgrade
  while it runs.

  The page earns its keep during an upgrade, so that is what it is built around: the
  node-by-node bar, which node is being worked on, which are done, and the job's output
  as it arrives. It polls quickly while something is running and slowly when nothing is,
  because an idle cluster does not need its inventory re-read every two seconds.

  Starting an upgrade and uploading a package are not offered here yet, and the page says
  so plainly rather than showing a button that does nothing. Both are recorded in
  [TODO.md]; checking the feed and aborting a running upgrade are, because an operator
  watching a bad upgrade needs the second one.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [panel: 1, figure: 1, dom_slug: 1]
  import SpectrumPhxWeb.Storage.Components, only: [bytes: 1]

  alias SpectrumPhx.Lcm

  @idle_interval_ms 60_000
  @running_interval_ms 3_000

  @impl true
  def mount(_params, _session, socket) do
    socket = socket |> assign(page_title: "LCM", timer: nil) |> load()

    {:ok, if(connected?(socket), do: retime(socket), else: socket)}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, socket |> load() |> retime()}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, socket |> load() |> retime()}

  defp load(socket) do
    socket
    |> assign(:overview, Lcm.overview())
    |> assign(:read_at, DateTime.utc_now())
  end

  # One interval, re-armed at whichever rate the current state deserves. A single timer
  # started at mount would either hammer an idle cluster or crawl during an upgrade.
  defp retime(socket) do
    if timer = socket.assigns[:timer], do: Process.cancel_timer(timer)

    interval =
      if socket.assigns.overview.job && Lcm.running?(socket.assigns.overview.job),
        do: @running_interval_ms,
        else: @idle_interval_ms

    assign(socket, :timer, Process.send_after(self(), :refresh, interval))
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:lcm}>
      <.header>
        Life cycle
        <:subtitle>
          <span class="text-xs opacity-60">
            read {Calendar.strftime(@read_at, "%H:%M:%S")} UTC
          </span>
        </:subtitle>
        <:actions>
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </.button>
        </:actions>
      </.header>

      <div class="flex flex-col gap-4">
        <.panel
          :if={@overview.job}
          id="upgrade-job"
          title="Rolling upgrade"
          subtitle={"build #{@overview.job.build || "unknown"} · #{@overview.job.state}"}
          class={Lcm.running?(@overview.job) && "glow-primary"}
        >
          <div class="flex items-baseline justify-between gap-2">
            <span class="text-sm">
              <span :if={@overview.job.current}>
                working on <span class="font-mono font-semibold">{@overview.job.current}</span>
              </span>
              <span :if={is_nil(@overview.job.current)} class="opacity-60">
                no node in flight
              </span>
            </span>
            <span class="tabular-nums font-semibold">{@overview.job.progress}%</span>
          </div>

          <progress
            class={["progress w-full h-2 mt-2", job_class(@overview.job.state)]}
            value={@overview.job.progress}
            max="100"
          >
          </progress>

          <div class="flex flex-wrap gap-1.5 mt-3">
            <span
              :for={node <- @overview.job.targets}
              class={["badge badge-sm gap-1", node_class(node, @overview.job)]}
              id={"upgrade-node-#{dom_slug(node)}"}
            >
              <.icon :if={node in @overview.job.finished} name="hero-check" class="size-3" />
              <.icon
                :if={node == @overview.job.current and Lcm.running?(@overview.job)}
                name="hero-arrow-path"
                class="size-3 motion-safe:animate-spin"
              />
              {node}
            </span>
          </div>

          <div :if={@overview.job.logs != []} class="mt-3">
            <p class="panel-title">Output</p>
            <div class="mt-1 max-h-64 overflow-y-auto bg-base-300/40 rounded p-2 font-mono text-[0.7rem] leading-relaxed">
              <div :for={entry <- @overview.job.logs} class="whitespace-pre-wrap break-all">
                {entry.line}
              </div>
            </div>
          </div>
        </.panel>

        <.panel id="available-update" title="Available" subtitle={checked_at(@overview.update)}>
          <p :if={not @overview.update.available?} class="text-sm opacity-55 italic">
            The update table could not be read: <span class="font-mono">{@overview.update.error}</span>
          </p>

          <div :if={@overview.update.available?} class="flex flex-col gap-3">
            <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
              <.figure
                id="update-current"
                label="Running"
                value={@overview.update.current_version}
                caption="this cluster"
              />
              <.figure
                id="update-latest"
                label="Latest"
                value={@overview.update.latest_version}
                caption={@overview.update.release_date || "no release date"}
                tone={if @overview.update.update_available?, do: :primary, else: :neutral}
              />
              <.figure
                id="update-size"
                label="Package"
                value={@overview.update.size_bytes && bytes(@overview.update.size_bytes)}
                caption="download"
              />
              <.figure
                id="update-state"
                label="State"
                value={if @overview.update.update_available?, do: "update available", else: "up to date"}
                tone={if @overview.update.update_available?, do: :warn, else: :good}
              />
            </div>

            <div :if={@overview.update.changelog} class="mt-1">
              <p class="panel-title">Changelog</p>
              <pre class="mt-1 max-h-64 overflow-y-auto text-xs leading-relaxed whitespace-pre-wrap opacity-80">{@overview.update.changelog}</pre>
            </div>

            <p :if={@overview.update.error} class="text-sm text-warning">
              Last check reported: <span class="font-mono">{@overview.update.error}</span>
            </p>
          </div>
        </.panel>

        <.panel
          id="inventory"
          title="Installed components"
          subtitle="One column per node, so a half-finished upgrade is visible"
        >
          <:actions>
            <span :if={@overview.inventory.disagreements > 0} class="badge badge-sm badge-warning gap-1">
              <.icon name="hero-exclamation-triangle" class="size-3" />
              {@overview.inventory.disagreements} disagree
            </span>
          </:actions>

          <p :if={not @overview.inventory.available?} class="text-sm opacity-55 italic">
            The inventory could not be read: <span class="font-mono">{@overview.inventory.error}</span>
          </p>
          <p :if={@overview.inventory.available? and @overview.inventory.components == []} class="text-sm opacity-55 italic">
            No inventory has been recorded yet.
          </p>

          <p :if={@overview.inventory.disagreements > 0} class="text-sm text-warning mb-2">
            The nodes are not all running the same build of everything. A rolling upgrade
            that stopped part way looks exactly like this, and a single version number per
            component would hide it.
          </p>

          <div :if={@overview.inventory.components != []} class="overflow-x-auto">
            <table class="table table-xs">
              <thead>
                <tr>
                  <th>Component</th>
                  <th :for={node <- @overview.inventory.nodes}>{node}</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  :for={component <- @overview.inventory.components}
                  id={"component-#{dom_slug(component.name)}"}
                  class={not component.consistent? && "bg-warning/10"}
                >
                  <td class="font-medium whitespace-nowrap">
                    {component.name}
                    <.icon
                      :if={not component.consistent?}
                      name="hero-exclamation-triangle"
                      class="size-3 text-warning ml-1"
                    />
                  </td>
                  <td
                    :for={node <- @overview.inventory.nodes}
                    class={[
                      "font-mono whitespace-nowrap",
                      not component.consistent? && "font-semibold",
                      version_of(component, node) == "Unknown" && "opacity-40"
                    ]}
                  >
                    {version_of(component, node)}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </.panel>

        <.panel id="lcm-not-offered" title="Not offered here yet">
          <p class="text-sm opacity-70">
            Starting an upgrade rolls every node through maintenance and a reboot, and
            uploading a package accepts a signed archive the cluster will then install.
            Both are real cluster-wide operations and neither is wired to a button here
            yet -- a control that looks live and is not is worse than one that is absent.
            They remain on the
            <.link href="/lcm.html" class="link">previous console</.link>
            until they can be run as tasks that report progress and fail visibly.
          </p>
        </.panel>
      </div>
    </Layouts.app>
    """
  end

  defp version_of(component, node), do: Map.get(component.by_node, node, "—")

  defp checked_at(%{last_checked: nil}), do: "never checked"
  defp checked_at(%{last_checked: at}), do: "last checked #{at}"

  defp job_class("FAILED"), do: "progress-error"
  defp job_class("COMPLETED"), do: "progress-success"
  defp job_class(_state), do: "progress-primary"

  defp node_class(node, job) do
    cond do
      node in job.finished -> "badge-success"
      node == job.current and job.state == "FAILED" -> "badge-error"
      node == job.current -> "badge-info"
      true -> "badge-ghost"
    end
  end
end
