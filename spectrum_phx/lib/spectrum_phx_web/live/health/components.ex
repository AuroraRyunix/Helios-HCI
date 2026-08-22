defmodule SpectrumPhxWeb.Health.Components do
  @moduledoc """
  Presentation pieces for the Mimir diagnostics dashboard.

  One rule runs through all of it: **nothing unproven is drawn green.**

    * A status Mimir did not write -- anything that is not `PASS`, `WARN` or `FAIL` --
      is `:unknown` and is drawn as a warning, never as a pass.
    * A table with no results is "not run", not "healthy". The old page's summary card
      started life reading `Healthy` in green and only changed once results arrived, so a
      cluster whose diagnostics had never run, or whose database was unreachable, looked
      exactly like a cluster that had passed every check.
    * A Dagur job stuck in `RUNNING` -- which is what a job whose worker died leaves
      behind -- gets its own colour rather than being folded into success or failure.
  """
  use SpectrumPhxWeb, :html

  @doc "Colour-coded badge for one check's status."
  attr :severity, :atom, required: true
  attr :status, :string, required: true

  def check_badge(assigns) do
    ~H"""
    <span class={[
      "badge badge-sm gap-1 font-medium whitespace-nowrap",
      severity_class(@severity)
    ]}>
      <.icon name={severity_icon(@severity)} class="size-3" />
      {@status}
    </span>
    """
  end

  @doc """
  The cluster's overall diagnostic state, as a banner.

  `:none` is its own case and says so: no results is not a pass.
  """
  attr :summary, :map, required: true

  def overall(assigns) do
    ~H"""
    <div class={["alert items-start", overall_class(@summary.state)]} id="health-overall">
      <.icon name={overall_icon(@summary.state)} class="size-5 shrink-0" />
      <div>
        <p class="font-semibold">{overall_title(@summary)}</p>
        <p class="text-sm opacity-90">{overall_detail(@summary)}</p>
      </div>
    </div>
    """
  end

  @doc "Shown when `hydra.mimir_results` could not be read."
  attr :error, :any, required: true

  def db_unavailable(assigns) do
    ~H"""
    <div class="alert alert-warning items-start" id="health-db-error">
      <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
      <div>
        <p class="font-semibold">Diagnostics unavailable</p>
        <p class="text-sm opacity-90">
          <code class="font-mono">hydra.mimir_results</code>
          could not be read. Nothing below is known -- which is not the same as nothing
          being wrong.
        </p>
        <p class="text-xs opacity-70 mt-1 font-mono break-all">{@error}</p>
      </div>
    </div>
    """
  end

  @doc "Badge for one Dagur run's outcome."
  attr :run, :map, required: true

  def run_badge(assigns) do
    ~H"""
    <span class={["badge badge-sm gap-1 font-medium whitespace-nowrap", run_class(@run.severity)]}>
      <.icon name={run_icon(@run.severity)} class={run_icon_class(@run.severity)} />
      {@run.status}
    </span>
    """
  end

  @doc "A duration in seconds, as a short human string."
  def humanize_age(nil), do: "never"
  def humanize_age(seconds) when seconds < 60, do: "#{seconds}s"
  def humanize_age(seconds) when seconds < 3_600, do: "#{div(seconds, 60)}m"
  def humanize_age(seconds) when seconds < 86_400, do: "#{div(seconds, 3_600)}h"
  def humanize_age(seconds), do: "#{div(seconds, 86_400)}d"

  @doc "A timestamp, or a dash."
  def stamp(nil), do: "-"
  def stamp(%DateTime{} = at), do: Calendar.strftime(at, "%Y-%m-%d %H:%M:%S")

  defp severity_class(:pass), do: "badge-success"
  defp severity_class(:warn), do: "badge-warning"
  defp severity_class(:fail), do: "badge-error"
  defp severity_class(_unknown), do: "badge-warning"

  defp severity_icon(:pass), do: "hero-check-circle"
  defp severity_icon(:warn), do: "hero-exclamation-triangle"
  defp severity_icon(:fail), do: "hero-x-circle"
  defp severity_icon(_unknown), do: "hero-question-mark-circle"

  defp overall_class(:fail), do: "alert-error"
  defp overall_class(:warn), do: "alert-warning"
  defp overall_class(:pass), do: "alert-success"
  defp overall_class(_none), do: "alert-warning alert-soft"

  defp overall_icon(:fail), do: "hero-x-circle"
  defp overall_icon(:warn), do: "hero-exclamation-triangle"
  defp overall_icon(:pass), do: "hero-check-circle"
  defp overall_icon(_none), do: "hero-question-mark-circle"

  defp overall_title(%{state: :fail, fail: fail}), do: "#{fail} check#{plural(fail)} failing"
  defp overall_title(%{state: :warn}), do: "Diagnostics report warnings"
  defp overall_title(%{state: :pass, pass: pass}), do: "All #{pass} checks passing"
  defp overall_title(_summary), do: "Diagnostics have not run"

  defp overall_detail(%{state: :none}) do
    "hydra.mimir_results is empty. That says nothing about the cluster's health: " <>
      "run 'mcli health_checks run_all', or check that the Dagur job below is enabled."
  end

  defp overall_detail(summary) do
    parts =
      [
        {summary.fail, "failing"},
        {summary.warn, "warning"},
        {summary.unknown, "unrecognised"},
        {summary.pass, "passing"}
      ]
      |> Enum.filter(fn {count, _label} -> count > 0 end)
      |> Enum.map_join(", ", fn {count, label} -> "#{count} #{label}" end)

    "#{parts} across #{summary.nodes} node#{plural(summary.nodes)}."
  end

  defp plural(1), do: ""
  defp plural(_count), do: "s"

  defp run_class(:ok), do: "badge-success"
  defp run_class(:failed), do: "badge-error"
  defp run_class(:running), do: "badge-info"
  defp run_class(_unknown), do: "badge-warning"

  defp run_icon(:ok), do: "hero-check-circle"
  defp run_icon(:failed), do: "hero-x-circle"
  defp run_icon(:running), do: "hero-arrow-path"
  defp run_icon(_unknown), do: "hero-question-mark-circle"

  defp run_icon_class(:running), do: "size-3 animate-spin"
  defp run_icon_class(_other), do: "size-3"
end
