defmodule SpectrumPhxWeb.Tasks.Components do
  @moduledoc """
  Presentation pieces for the Catalyst task log.

  Two rules are encoded here.

  **Progress is not success.** `spectrum_server.py` writes `progress = 100` alongside
  `status = 'failed'` at every failure site, so the bar is coloured by `state`, never by
  how full it is. A failed task shows a full *red* bar.

  **An unrecognised status is not success.** Only `completed` is drawn green. Anything the
  Python tier did not write -- including a null `status` -- is `:unknown` and is drawn as a
  warning, because a state nobody can explain is not a state anyone should trust.
  """
  use SpectrumPhxWeb, :html

  @doc "Colour-coded badge for one task's lifecycle state."
  attr :task, :map, required: true

  def task_badge(assigns) do
    ~H"""
    <span class={["badge badge-sm gap-1 font-medium whitespace-nowrap", state_class(@task.state)]}>
      <.icon name={state_icon(@task.state)} class={state_icon_class(@task.state)} />
      {@task.status}
    </span>
    """
  end

  @doc """
  Progress bar for one task.

  Rendered as a `<progress>` element rather than a div whose width is rewritten: the old
  page rebuilt this subtree with `innerHTML` on every poll, which is what made running
  bars visibly snap back to zero between refreshes. Here LiveView patches the one
  attribute that changed.
  """
  attr :task, :map, required: true

  def progress_bar(assigns) do
    ~H"""
    <div class="flex items-center gap-2 min-w-32">
      <progress
        class={["progress w-full h-1.5", progress_class(@task.state)]}
        value={@task.progress}
        max="100"
      ></progress>
      <span class="text-xs tabular-nums opacity-70 w-9 text-right">{@task.progress}%</span>
    </div>
    """
  end

  @doc """
  Shown when Hydra could not be read.

  Distinct from "no tasks recorded" on purpose: an unreadable database and an empty one
  are different facts, and collapsing them is how an operator comes to believe nothing is
  running when in truth nothing is known.
  """
  attr :error, :any, required: true

  def db_unavailable(assigns) do
    ~H"""
    <div class="alert alert-warning items-start" id="tasks-db-error">
      <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
      <div>
        <p class="font-semibold">Task history unavailable</p>
        <p class="text-sm opacity-90">
          <code class="font-mono">hydra.catalyst_tasks</code>
          could not be read, so nothing below is known -- not "nothing is running".
        </p>
        <p class="text-xs opacity-70 mt-1 font-mono break-all">{@error}</p>
      </div>
    </div>
    """
  end

  @doc "Wall-clock time of a task timestamp, or a dash."
  attr :at, :any, default: nil

  def stamp(assigns) do
    ~H"""
    <span :if={@at} class="tabular-nums">{Calendar.strftime(@at, "%Y-%m-%d %H:%M:%S")}</span>
    <span :if={is_nil(@at)} class="opacity-50">-</span>
    """
  end

  @doc """
  A duration in seconds, as a short human string.

  Kept here rather than shared across the three dashboards: the file layout for this work
  puts each view's presentation in its own directory, and a six-line formatter is not
  worth a cross-directory dependency between unrelated pages.
  """
  def humanize_age(nil), do: "never"
  def humanize_age(seconds) when seconds < 60, do: "#{seconds}s"
  def humanize_age(seconds) when seconds < 3_600, do: "#{div(seconds, 60)}m"
  def humanize_age(seconds) when seconds < 86_400, do: "#{div(seconds, 3_600)}h"
  def humanize_age(seconds), do: "#{div(seconds, 86_400)}d"

  defp state_class(:completed), do: "badge-success"
  defp state_class(:failed), do: "badge-error"
  defp state_class(:processing), do: "badge-info"
  defp state_class(:pending), do: "badge-neutral"
  defp state_class(_unknown), do: "badge-warning"

  defp state_icon(:completed), do: "hero-check-circle"
  defp state_icon(:failed), do: "hero-x-circle"
  defp state_icon(:processing), do: "hero-arrow-path"
  defp state_icon(:pending), do: "hero-clock"
  defp state_icon(_unknown), do: "hero-question-mark-circle"

  defp state_icon_class(:processing), do: "size-3 animate-spin"
  defp state_icon_class(_other), do: "size-3"

  defp progress_class(:completed), do: "progress-success"
  defp progress_class(:failed), do: "progress-error"
  defp progress_class(:processing), do: "progress-info"
  defp progress_class(:pending), do: "progress-neutral"
  defp progress_class(_unknown), do: "progress-warning"
end
