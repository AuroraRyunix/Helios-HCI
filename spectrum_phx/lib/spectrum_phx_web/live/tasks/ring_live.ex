defmodule SpectrumPhxWeb.Tasks.RingLive do
  @moduledoc """
  The task ring in the console header: what is running, how far along, and what it was.

  A port of the Python console's header widget, which is the one thing on that console
  that reported progress without being asked. An operator kicks off a migration and
  watches the ring rather than a page, and the rebuilt console had nothing like it.

  ## Why this is its own LiveView

  Mounted `sticky: true` from the layout, so it keeps its process, its subscription and
  its state across live navigation. A function component in the layout would have to be
  fed by every page's socket -- fourteen views each remembering to poll Catalyst -- and
  would be torn down and re-mounted on every navigation, which is exactly when a ring
  showing a running task should *not* reset.

  ## State

  Four states, taken from the original:

    * `running` -- something is pending or processing. The ring fills to the *first*
      active task's progress and spins. The badge counts the active ones.
    * `failed` / `done` -- nothing active; the ring is full and coloured by the most
      recent task's outcome, and the badge counts everything recent.
    * `idle` -- nothing at all.

  The ring also grows with progress, 1.0 to 1.3, so the thing being watched gets bigger
  the closer it is to finishing. That is in the original and it is the detail that makes
  it feel alive rather than decorative.

  ## Announcements

  When a task appears, finishes or fails, a label slides out beside the ring naming it,
  and collapses again after a few seconds -- unless something is still running, in which
  case it stays out. Transitions are detected by diffing the previous snapshot against
  the new one, because Catalyst has no event stream to subscribe to; the moment one
  exists this can stop polling entirely.
  """
  use SpectrumPhxWeb, :live_view

  alias SpectrumPhx.Tasks

  # The circumference of an r=9 circle, which is the dasharray the arc is drawn against.
  # Offsetting by the whole of it draws nothing; offsetting by zero draws the full ring.
  @circumference 56.55

  @poll_interval_ms 4_000
  @announcement_ms 4_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Tasks.subscribe()
      :timer.send_interval(@poll_interval_ms, self(), :poll)
    end

    {:ok,
     socket
     |> assign(open?: false, announcement: nil, dual?: false, announced_at: nil)
     |> assign_snapshot(Tasks.fetch(limit: 25)),
     layout: false}
  end

  @impl true
  def handle_info(:poll, socket), do: {:noreply, refresh(socket)}

  def handle_info({:tasks, snapshot}, socket), do: {:noreply, apply_snapshot(socket, snapshot)}

  # The announcement collapses on its own; nothing else has to know it was showing.
  def handle_info(:collapse, socket) do
    if running?(socket.assigns.summary),
      do: {:noreply, socket},
      else: {:noreply, assign(socket, announcement: nil, dual?: false)}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("toggle", _params, socket) do
    {:noreply, assign(socket, :open?, not socket.assigns.open?)}
  end

  def handle_event("close", _params, socket), do: {:noreply, assign(socket, :open?, false)}

  defp refresh(socket), do: apply_snapshot(socket, Tasks.fetch(limit: 25))

  defp apply_snapshot(socket, snapshot) do
    announcement = transition(socket.assigns[:flat] || [], flatten(snapshot.tasks))

    socket
    |> assign_snapshot(snapshot)
    |> announce(announcement)
  end

  defp assign_snapshot(socket, snapshot) do
    flat = flatten(snapshot.tasks)
    active = Enum.filter(flat, &Tasks.active?/1)
    ring = ring(active, flat)

    socket
    |> assign(:snapshot, snapshot)
    |> assign(:summary, snapshot.summary)
    |> assign(:flat, flat)
    |> assign(:active, active)
    |> assign(:ring, ring)
  end

  defp announce(socket, nil), do: socket

  defp announce(socket, {message, sub}) do
    Process.send_after(self(), :collapse, @announcement_ms)
    assign(socket, announcement: message, dual?: sub != nil, sub: sub)
  end

  # -- ring ----------------------------------------------------------------------------

  @doc """
  The ring's state, exposed so it can be tested without rendering.

  Returns `%{state:, progress:, offset:, scale:, count:, spin?:}`.
  """
  def ring(active, all) do
    cond do
      active != [] ->
        progress = (List.first(active) |> Map.get(:progress)) || 0

        %{
          state: :running,
          progress: progress,
          offset: offset(progress),
          scale: scale(progress),
          count: length(active),
          spin?: true
        }

      all != [] ->
        # Nothing is moving, so the ring reports the most recent outcome, full.
        state = if List.first(all).state == :failed, do: :failed, else: :done

        %{state: state, progress: 100, offset: 0.0, scale: 1.0, count: length(all), spin?: false}

      true ->
        %{state: :idle, progress: 0, offset: @circumference, scale: 1.0, count: 0, spin?: false}
    end
  end

  @doc "Where to start the dash so the arc shows `progress` percent of the ring."
  def offset(progress) when is_number(progress) do
    bounded = progress |> max(0) |> min(100)
    Float.round(@circumference - bounded / 100 * @circumference, 2)
  end

  def offset(_progress), do: @circumference

  @doc "1.0 at nothing done, 1.3 at finished. The ring grows as the task closes."
  def scale(progress) when is_number(progress) do
    Float.round(1.0 + (progress |> max(0) |> min(100)) / 100 * 0.3, 2)
  end

  def scale(_progress), do: 1.0

  @doc "The dasharray every ring is drawn against."
  def circumference, do: @circumference

  # -- transitions ---------------------------------------------------------------------

  @doc """
  What changed between two task lists, as `{message, submessage}` or nil.

  A task that finished is worth more than one that started: an operator who walked away
  wants to know how it went, not that it began. Failures win over completions for the
  same reason.
  """
  def transition(before, now) do
    was = Map.new(before, &{&1.id, &1})

    changed =
      for task <- now,
          previous = Map.get(was, task.id),
          previous != nil,
          previous.state != task.state,
          task.state in [:failed, :completed],
          do: task

    started = for task <- now, not Map.has_key?(was, task.id), Tasks.active?(task), do: task

    cond do
      failed = Enum.find(changed, &(&1.state == :failed)) ->
        {"#{failed.label} failed", failed.error}

      done = List.first(changed) ->
        {"#{done.label} finished", nil}

      begun = List.first(started) ->
        {begun.label, nil}

      true ->
        nil
    end
  end

  defp flatten(tasks) do
    Enum.flat_map(tasks, fn task ->
      [Map.delete(task, :children) | flatten(Map.get(task, :children) || [])]
    end)
  end

  defp running?(summary), do: summary.running > 0 or summary.pending > 0

  @impl true
  def render(assigns) do
    ~H"""
    <div class="relative" id="tasks-ring" phx-click-away="close">
      <button
        type="button"
        class="tasks-trigger"
        data-state={@ring.state}
        style={"--tasks-scale: #{@ring.scale}"}
        phx-click="toggle"
        aria-haspopup="true"
        aria-expanded={to_string(@open?)}
        title={title(@ring, @summary)}
        id="tasks-trigger"
      >
        <svg viewBox="0 0 24 24" class={["tasks-ring", @ring.spin? && "tasks-ring-spin"]}>
          <circle cx="12" cy="12" r="9" class="tasks-ring-track" />
          <circle
            cx="12"
            cy="12"
            r="9"
            class="tasks-ring-fill"
            style={"stroke-dashoffset: #{@ring.offset}"}
          />
        </svg>

        <span
          class="badge badge-xs font-semibold tabular-nums"
          data-role="tasks-count"
          id="tasks-count"
        >
          {@ring.count}
        </span>

        <span
          class="tasks-announcement"
          data-expanded={to_string(@announcement != nil)}
          data-dual={to_string(@dual?)}
          id="tasks-announcement"
        >
          <span :if={@announcement} class="truncate max-w-[20rem]">{@announcement}</span>
          <span :if={@dual? and @sub} class="opacity-60 text-[10px] pl-2 truncate max-w-[20rem]">
            {@sub}
          </span>
        </span>
      </button>

      <div
        :if={@open?}
        class="glass-card absolute right-0 mt-2 w-80 sm:w-96 z-50 p-0 overflow-hidden"
        id="tasks-menu"
      >
        <div class="flex items-center justify-between gap-2 px-3 py-2 border-b border-base-300/60">
          <span class="panel-title">
            Recent tasks <span class="opacity-60">({@summary.total})</span>
          </span>
          <span :if={@summary.running > 0} class="text-xs text-info font-medium">
            {@summary.running} running
          </span>
        </div>

        <div class="max-h-80 overflow-y-auto">
          <p :if={not @snapshot.available?} class="px-3 py-4 text-sm opacity-60 italic">
            Catalyst could not be read: <span class="font-mono">{@snapshot.error}</span>
          </p>
          <p :if={@snapshot.available? and @flat == []} class="px-3 py-4 text-sm opacity-55 italic">
            No recent tasks
          </p>

          <div
            :for={task <- @flat}
            class="px-3 py-2 border-b border-base-300/40 last:border-0"
            style={depth_style(task)}
            id={"task-#{task.short_id}"}
          >
            <div class="flex items-baseline justify-between gap-2">
              <span class="text-xs truncate" title={task.label}>
                <span :if={task.depth > 0} class="font-mono opacity-40 mr-1">&#8627;</span>
                {task.label}
              </span>
              <span class="text-xs tabular-nums opacity-70 flex items-center gap-1.5 shrink-0">
                {task.progress}%
                <span class={["status-dot", dot_class(task.state)]}></span>
              </span>
            </div>
            <div class="h-1 mt-1.5 rounded bg-base-300/60 overflow-hidden">
              <div
                class={["h-full rounded transition-[width] duration-300", bar_class(task.state)]}
                style={"width: #{task.progress}%"}
              >
              </div>
            </div>
          </div>
        </div>

        <div class="px-3 py-2 border-t border-base-300/60">
          <.link navigate={~p"/tasks"} class="text-xs text-primary hover:underline">
            View all tasks &rsaquo;
          </.link>
        </div>
      </div>
    </div>
    """
  end

  defp title(%{state: :running}, summary), do: "#{summary.running + summary.pending} task(s) running"
  defp title(%{state: :failed}, _summary), do: "The most recent task failed"
  defp title(%{state: :done}, _summary), do: "The most recent task completed"
  defp title(_ring, _summary), do: "No recent tasks"

  defp depth_style(%{depth: depth}) when depth > 0 do
    "padding-left: #{12 + depth * 16}px; border-left: 2px solid color-mix(in srgb, var(--color-primary) 30%, transparent);"
  end

  defp depth_style(_task), do: nil

  defp dot_class(:completed), do: "text-success"
  defp dot_class(:failed), do: "text-error"
  defp dot_class(:processing), do: "text-info"
  defp dot_class(:pending), do: "text-warning"
  defp dot_class(_state), do: "text-base-content/30"

  defp bar_class(:completed), do: "bg-success"
  defp bar_class(:failed), do: "bg-error"
  defp bar_class(:processing), do: "bg-info"
  defp bar_class(:pending), do: "bg-warning"
  defp bar_class(_state), do: "bg-base-content/30"
end
