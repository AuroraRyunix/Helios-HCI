defmodule SpectrumPhxWeb.Tasks.IndexLive do
  @moduledoc """
  Catalyst task history at `/tasks`.

  Updates are *pushed* over the LiveView websocket. The page it replaces polled
  `/api/catalyst/tasks` from the browser every three seconds and rebuilt the whole table
  body with `innerHTML` each time. Two things followed from that, and both are fixed here
  by construction rather than by care:

    * A running task's progress bar was destroyed and recreated on every poll, so it
      restarted its transition from zero instead of advancing. Here the bar is a
      `<progress>` element whose `value` LiveView patches in place -- the DOM node the
      browser is animating is never replaced.

    * Nine timers at five cadences meant no two panels on the old console ever showed the
      same instant. Here every connected socket renders the same server-side snapshot.

  The refresh is a server-side interval for now, because nothing yet watches Catalyst.
  `SpectrumPhx.Tasks.broadcast/1` is the seam for that watcher; when it exists the
  interval can go and this page becomes purely event-driven. Both the subscription and the
  timer are set up inside `connected?/1`, so the static first render does no extra work.

  This view assumes nothing about authentication: it takes no user out of the session and
  adds no checks of its own, so it works unchanged under a `live_session` whose `on_mount`
  hook has already established who is asking.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [dom_slug: 1]
  import SpectrumPhxWeb.Tasks.Components

  alias SpectrumPhx.Tasks

  @refresh_interval_ms 3_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Tasks.subscribe()
      schedule_refresh()
    end

    {:ok,
     socket
     |> assign(:page_title, "Tasks")
     |> assign(:query, "")
     |> assign_snapshot(Tasks.fetch())}
  end

  @impl true
  def handle_info(:refresh, socket) do
    # The next tick is scheduled only once this read has returned. A fixed interval
    # would queue ticks faster than a slow or wedged database could serve them, and the
    # socket would fall further behind with every one; this degrades the refresh rate
    # instead, which is the failure mode an operator can live with.
    snapshot = Tasks.fetch()
    schedule_refresh()
    {:noreply, assign_snapshot(socket, snapshot)}
  end

  # Pushed by whichever process ends up watching Catalyst, when one exists.
  def handle_info({:tasks, snapshot}, socket) do
    {:noreply, assign_snapshot(socket, snapshot)}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  # Typing in the filter re-derives what is shown from the snapshot already in hand: it
  # does not re-read the database, and it does not move the "updated" stamp, which would
  # otherwise claim a freshness no keystroke can give it.
  @impl true
  def handle_event("search", %{"query" => query}, socket) do
    {:noreply, socket |> assign(:query, query) |> assign_visible()}
  end

  def handle_event("refresh", _params, socket) do
    {:noreply, assign_snapshot(socket, Tasks.fetch())}
  end

  defp schedule_refresh do
    Process.send_after(self(), :refresh, @refresh_interval_ms)
  end

  defp assign_snapshot(socket, snapshot) do
    socket
    |> assign(:snapshot, snapshot)
    |> assign(:summary, snapshot.summary)
    |> assign(:updated_at, DateTime.utc_now())
    |> assign_visible()
  end

  defp assign_visible(socket) do
    assign(socket, :visible, filter(socket.assigns.snapshot.tasks, socket.assigns.query))
  end

  # An unfiltered list keeps the parent/child indentation `mipha.py`'s workflows imply. A
  # filtered one is flattened: showing a step indented under a parent that the filter
  # removed would claim a relationship the operator cannot see.
  defp filter(tasks, ""), do: tasks

  defp filter(tasks, query) do
    needle = query |> String.trim() |> String.downcase()

    tasks
    |> Enum.filter(&matches?(&1, needle))
    |> Enum.map(&%{&1 | depth: 0})
  end

  defp matches?(task, needle) do
    [task.id, task.service, task.action, task.status, task.label, task.error, task.payload_raw]
    |> Enum.reject(&is_nil/1)
    |> Enum.any?(fn field -> String.contains?(String.downcase(field), needle) end)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_username={@current_username} active={:tasks}>
      <.header>
        Tasks
        <:subtitle>
          <span class="flex flex-wrap items-center gap-2 mt-1">
            <span class="badge badge-sm badge-ghost gap-1" id="tasks-source">
              <.icon name="hero-circle-stack" class="size-3" /> hydra.catalyst_tasks
            </span>
            <span class="text-xs opacity-60" id="updated-at">
              updated {Calendar.strftime(@updated_at, "%H:%M:%S")} UTC
            </span>
          </span>
        </:subtitle>
        <:actions>
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </.button>
        </:actions>
      </.header>

      <.db_unavailable :if={not @snapshot.available?} error={@snapshot.error} />

      <div :if={@snapshot.available?} class="stats stats-vertical sm:stats-horizontal w-full shadow">
        <div class="stat">
          <div class="stat-title">Running</div>
          <div class="stat-value text-2xl text-info" id="stat-running">{@summary.running}</div>
          <div class="stat-desc">{@summary.pending} queued</div>
        </div>

        <div class="stat">
          <div class="stat-title">Failed</div>
          <div class={["stat-value text-2xl", @summary.failed > 0 && "text-error"]} id="stat-failed">
            {@summary.failed}
          </div>
          <div class="stat-desc">of {@summary.total} recorded</div>
        </div>

        <div class="stat">
          <div class="stat-title">Completed</div>
          <div class="stat-value text-2xl text-success" id="stat-completed">
            {@summary.completed}
          </div>
          <div class="stat-desc">finished cleanly</div>
        </div>

        <div class="stat">
          <div class="stat-title">Unclassified</div>
          <div
            class={["stat-value text-2xl", @summary.unknown > 0 && "text-warning"]}
            id="stat-unknown"
          >
            {@summary.unknown}
          </div>
          <div class="stat-desc">status not recognised</div>
        </div>
      </div>

      <form
        :if={@snapshot.available?}
        phx-change="search"
        phx-submit="search"
        id="tasks-filter"
        class="w-full"
      >
        <label class="input input-sm w-full">
          <.icon name="hero-magnifying-glass" class="size-4 opacity-60" />
          <input
            type="text"
            name="query"
            value={@query}
            id="tasks-search"
            phx-debounce="200"
            placeholder="Filter by VM, host, service, action, status or error"
            autocomplete="off"
          />
        </label>
      </form>

      <div
        :if={@snapshot.available? and @snapshot.tasks == []}
        id="tasks-empty"
        class="alert alert-info alert-soft"
      >
        <.icon name="hero-inbox" class="size-5 shrink-0" />
        <span class="text-sm">
          No tasks have been recorded. Catalyst writes a row for every unit of work it
          dispatches, so an empty table means nothing has run -- the database was read
          successfully.
        </span>
      </div>

      <div
        :if={@snapshot.available? and @snapshot.tasks != [] and @visible == []}
        id="tasks-no-match"
        class="text-sm opacity-60 italic"
      >
        No tasks match "{@query}".
      </div>

      <div :if={@visible != []} class="overflow-x-auto">
        <table class="table table-zebra table-sm" id="tasks-table">
          <thead>
            <tr>
              <th>Task</th>
              <th>Status</th>
              <th>Progress</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            <tr
              :for={task <- @visible}
              id={"task-#{dom_slug(task.id)}"}
              class={task.state == :failed && "bg-error/5"}
            >
              <td class="align-top">
                <div class="flex items-start gap-1" style={"padding-left: #{task.depth * 16}px"}>
                  <span :if={task.depth > 0} class="opacity-40 font-mono text-xs">&#8627;</span>
                  <div class="min-w-0">
                    <p class="font-medium">{task.label}</p>
                    <p class="text-xs opacity-50 font-mono">
                      {task.short_id} &middot; {task.service}
                    </p>
                    <p
                      :if={task.error}
                      class="text-xs text-error mt-1 font-mono break-all max-w-md"
                      id={"task-#{dom_slug(task.id)}-error"}
                    >
                      {task.error}
                    </p>
                  </div>
                </div>
              </td>
              <td class="align-top"><.task_badge task={task} /></td>
              <td class="align-top"><.progress_bar task={task} /></td>
              <td class="align-top text-xs">
                <.stamp at={task.updated_at || task.created_at} />
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <p :if={@snapshot.truncated?} class="text-xs opacity-60" id="tasks-truncated">
        Showing the {@snapshot.limit} most recent tasks. <code class="font-mono">catalyst_tasks</code>
        is keyed by <code class="font-mono">task_id</code>
        alone, so there is no clustering order to read the newest rows in and the whole
        table has to be scanned; the cap is applied after the read.
      </p>

      <p :if={@snapshot.available?} class="text-xs opacity-50">
        Live over the websocket; progress advances in place rather than being redrawn.
      </p>
    </Layouts.app>
    """
  end
end
