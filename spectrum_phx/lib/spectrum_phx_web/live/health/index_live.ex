defmodule SpectrumPhxWeb.Health.IndexLive do
  @moduledoc """
  Mimir diagnostics at `/health`.

  Results are grouped by what each check actually examines, failures are lifted to the top
  of the page, and the overall banner is never green unless every recorded check passed.

  The page it replaces polled `/api/mimir/results`, `/api/mimir/schedules`,
  `/api/dagur/schedules` and `/api/dagur/runs` on separate browser timers and rebuilt each
  panel with `innerHTML`. Its summary card was seeded reading "Healthy" in green before
  any request had returned, so a cluster whose diagnostics had never run -- or whose
  database was unreachable -- was indistinguishable from one that had passed everything.
  Here there is one snapshot, `SpectrumPhx.Health` distinguishes "not run" from "passed",
  and both are distinguished from "could not be read".

  The refresh is a fifteen-second server-side interval. Diagnostics are dispatched hourly
  by Dagur, so nothing here changes faster than that; the interval exists to notice a run
  finishing, and `SpectrumPhx.Health.broadcast/1` is the seam for a watcher that would
  make it unnecessary.

  This view reads no session and adds no authentication of its own, so it mounts unchanged
  under a `live_session` whose `on_mount` hook has already run.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [dom_slug: 1]
  import SpectrumPhxWeb.Health.Components

  alias SpectrumPhx.Health

  @refresh_interval_ms 15_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Health.subscribe()
      schedule_refresh()
    end

    {:ok,
     socket
     |> assign(:page_title, "Health")
     |> assign(:show_passing, true)
     |> assign_snapshot(Health.fetch())}
  end

  @impl true
  def handle_info(:refresh, socket) do
    # The next tick is scheduled only once this read has returned. A fixed interval
    # would queue ticks faster than a slow or wedged database could serve them, and the
    # socket would fall further behind with every one; this degrades the refresh rate
    # instead, which is the failure mode an operator can live with.
    snapshot = Health.fetch()
    schedule_refresh()
    {:noreply, assign_snapshot(socket, snapshot)}
  end

  # Pushed by whichever process ends up watching Mimir, when one exists.
  def handle_info({:health, snapshot}, socket) do
    {:noreply, assign_snapshot(socket, snapshot)}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  # The filter re-derives what is listed from the snapshot already in hand: no database
  # read, and no move of the "updated" stamp, which would claim a freshness a click on a
  # checkbox cannot give it.
  @impl true
  def handle_event("toggle_passing", params, socket) do
    show = Map.get(params, "show_passing") == "true"
    {:noreply, socket |> assign(:show_passing, show) |> assign_categories()}
  end

  def handle_event("refresh", _params, socket) do
    {:noreply, assign_snapshot(socket, Health.fetch())}
  end

  defp schedule_refresh do
    Process.send_after(self(), :refresh, @refresh_interval_ms)
  end

  defp assign_snapshot(socket, snapshot) do
    socket
    |> assign(:snapshot, snapshot)
    |> assign(:summary, snapshot.summary)
    |> assign(:updated_at, DateTime.utc_now())
    |> assign_categories()
  end

  defp assign_categories(socket) do
    categories = visible(socket.assigns.snapshot.categories, socket.assigns.show_passing)
    assign(socket, :categories, categories)
  end

  # Hiding passing checks never hides a category that has failures in it, and the counts
  # shown on each category header are always the full counts -- the filter changes what is
  # listed, not what is claimed.
  defp visible(categories, true), do: categories

  defp visible(categories, false) do
    categories
    |> Enum.map(fn category ->
      %{category | checks: Enum.reject(category.checks, &(&1.severity == :pass))}
    end)
    |> Enum.reject(fn category -> category.checks == [] end)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_username={@current_username} active={:health}>
      <.header>
        Health
        <:subtitle>
          <span class="flex flex-wrap items-center gap-2 mt-1">
            <span class="badge badge-sm badge-ghost gap-1" id="health-source">
              <.icon name="hero-circle-stack" class="size-3" /> hydra.mimir_results
            </span>
            <span class="badge badge-sm badge-ghost gap-1" id="health-last-run">
              <.icon name="hero-clock" class="size-3" /> last run {stamp(@summary.last_run)}
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

      <.overall :if={@snapshot.available?} summary={@summary} />

      <div
        :if={@snapshot.available? and @summary.total > 0}
        class="stats stats-vertical sm:stats-horizontal w-full shadow"
      >
        <div class="stat">
          <div class="stat-title">Failing</div>
          <div class={["stat-value text-2xl", @summary.fail > 0 && "text-error"]} id="stat-fail">
            {@summary.fail}
          </div>
          <div class="stat-desc">must be fixed</div>
        </div>

        <div class="stat">
          <div class="stat-title">Warnings</div>
          <div class={["stat-value text-2xl", @summary.warn > 0 && "text-warning"]} id="stat-warn">
            {@summary.warn}
          </div>
          <div class="stat-desc">degraded, not down</div>
        </div>

        <div class="stat">
          <div class="stat-title">Passing</div>
          <div class="stat-value text-2xl text-success" id="stat-pass">{@summary.pass}</div>
          <div class="stat-desc">of {@summary.total} checks</div>
        </div>

        <div class="stat">
          <div class="stat-title">Unrecognised</div>
          <div
            class={["stat-value text-2xl", @summary.unknown > 0 && "text-warning"]}
            id="stat-unknown"
          >
            {@summary.unknown}
          </div>
          <div class="stat-desc">status Mimir does not define</div>
        </div>
      </div>

      <section :if={@snapshot.failing != []} class="card card-border border-error/50 bg-base-100">
        <div class="card-body gap-3 p-4">
          <h2 class="font-semibold text-error" id="health-failing">
            Needs attention ({length(@snapshot.failing)})
          </h2>
          <div class="overflow-x-auto">
            <table class="table table-zebra table-sm" id="failing-table">
              <thead>
                <tr>
                  <th>Check</th>
                  <th>Node</th>
                  <th>Status</th>
                  <th>Ran</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={check <- @snapshot.failing} id={"failing-#{check_id(check)}"}>
                  <td class="align-top">
                    <p class="font-medium">{check.label}</p>
                    <p class="text-xs opacity-50 font-mono">
                      {check.check} &middot; {check.category_label}
                    </p>
                    <p :if={check.message} class="text-xs opacity-80 mt-1 font-mono break-all">
                      {check.message}
                    </p>
                  </td>
                  <td class="align-top text-xs">
                    <p>{check.hostname}</p>
                    <p class="font-mono opacity-50">{check.node_ip}</p>
                  </td>
                  <td class="align-top">
                    <.check_badge severity={check.severity} status={check.status} />
                  </td>
                  <td class="align-top text-xs opacity-70">
                    {humanize_age(check.age_seconds)} ago
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <form
        :if={@snapshot.available? and @summary.total > 0}
        phx-change="toggle_passing"
        id="health-filter"
      >
        <label class="label cursor-pointer justify-start gap-2 text-sm">
          <input type="hidden" name="show_passing" value="false" />
          <input
            type="checkbox"
            name="show_passing"
            value="true"
            checked={@show_passing}
            class="checkbox checkbox-sm"
            id="show-passing"
          />
          <span>Show passing checks</span>
        </label>
      </form>

      <section
        :for={category <- @categories}
        id={"category-#{category.key}"}
        class="card card-border bg-base-100"
      >
        <div class="card-body gap-3 p-4">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <h2 class="font-semibold">{category.label}</h2>
            <div class="flex flex-wrap items-center gap-1.5 text-xs">
              <span :if={category.counts.fail > 0} class="badge badge-sm badge-error">
                {category.counts.fail} failing
              </span>
              <span :if={category.counts.warn > 0} class="badge badge-sm badge-warning">
                {category.counts.warn} warning
              </span>
              <span :if={category.counts.unknown > 0} class="badge badge-sm badge-warning">
                {category.counts.unknown} unrecognised
              </span>
              <span class="badge badge-sm badge-ghost">
                {category.counts.pass}/{category.counts.total} passing
              </span>
            </div>
          </div>

          <div class="overflow-x-auto">
            <table class="table table-zebra table-sm" id={"table-#{category.key}"}>
              <thead>
                <tr>
                  <th>Check</th>
                  <th>Node</th>
                  <th>Status</th>
                  <th>Ran</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  :for={check <- category.checks}
                  id={"check-#{check_id(check)}"}
                  class={check.severity == :fail && "bg-error/5"}
                >
                  <td class="align-top">
                    <p class="font-medium">{check.label}</p>
                    <p class="text-xs opacity-50 font-mono">{check.check}</p>
                    <p
                      :if={check.message && check.severity != :pass}
                      class="text-xs opacity-80 mt-1 font-mono break-all"
                    >
                      {check.message}
                    </p>
                  </td>
                  <td class="align-top text-xs">
                    <p>{check.hostname}</p>
                    <p class="font-mono opacity-50">{check.node_ip}</p>
                  </td>
                  <td class="align-top">
                    <.check_badge severity={check.severity} status={check.status} />
                  </td>
                  <td class="align-top text-xs opacity-70">
                    {humanize_age(check.age_seconds)} ago
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <p :if={@snapshot.duplicates > 0} class="text-xs opacity-60" id="health-duplicates">
        {@snapshot.duplicates} older row{if @snapshot.duplicates == 1, do: "", else: "s"} hidden.
        <code class="font-mono">mimir_results</code>
        is partitioned by the scope a run was invoked with, so re-running one category
        leaves the previous run's copy of those checks in place; only the newest row for
        each check and node is shown.
      </p>

      <section class="card card-border bg-base-100">
        <div class="card-body gap-3 p-4">
          <h2 class="font-semibold" id="scheduler-heading">Dagur scheduler</h2>

          <div
            :if={not @snapshot.scheduler_available?}
            class="alert alert-warning alert-soft text-sm"
            id="scheduler-error"
          >
            <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
            <span>
              The scheduler tables could not be read ({@snapshot.scheduler_error}), so it is
              unknown whether diagnostics are being dispatched at all.
            </span>
          </div>

          <p
            :if={@snapshot.scheduler_available? and @snapshot.schedules == []}
            class="text-sm opacity-60 italic"
            id="scheduler-empty"
          >
            No jobs are registered in <code class="font-mono">hydra.dagur_schedules</code>.
          </p>

          <div :if={@snapshot.schedules != []} class="overflow-x-auto">
            <table class="table table-zebra table-sm" id="schedules-table">
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Cron</th>
                  <th>Enabled</th>
                  <th>Last dispatched</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={schedule <- @snapshot.schedules} id={"schedule-#{dom_slug(schedule.job)}"}>
                  <td class="align-top">
                    <p class="font-medium">{schedule.job}</p>
                    <p class="text-xs opacity-50 font-mono break-all">{schedule.command}</p>
                  </td>
                  <td class="align-top font-mono text-xs">{schedule.cron || "-"}</td>
                  <td class="align-top">
                    <span class={[
                      "badge badge-sm",
                      if(schedule.enabled?, do: "badge-success", else: "badge-ghost")
                    ]}>
                      {if schedule.enabled?, do: "enabled", else: "disabled"}
                    </span>
                  </td>
                  <td class="align-top text-xs">
                    <span class={schedule.overdue? && "text-warning font-semibold"}>
                      {stamp(schedule.last_run)}
                    </span>
                    <span :if={schedule.overdue?} class="block opacity-70">overdue</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <h3
            :if={@snapshot.runs != []}
            class="font-semibold text-sm mt-2"
            id="runs-heading"
          >
            Recent runs
          </h3>

          <div :if={@snapshot.runs != []} class="overflow-x-auto">
            <table class="table table-zebra table-sm" id="runs-table">
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Started</th>
                  <th>Status</th>
                  <th>Exit</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={run <- @snapshot.runs} id={"run-#{dom_slug(run.job)}-#{run_key(run)}"}>
                  <td class="align-top font-medium">{run.job}</td>
                  <td class="align-top text-xs">
                    {stamp(run.started_at)}
                    <span :if={run.duration_seconds} class="block opacity-60">
                      took {humanize_age(run.duration_seconds)}
                    </span>
                  </td>
                  <td class="align-top"><.run_badge run={run} /></td>
                  <td class="align-top text-xs tabular-nums">
                    {if is_nil(run.exit_code), do: "-", else: run.exit_code}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </section>
    </Layouts.app>
    """
  end

  defp check_id(check), do: dom_slug(check.check <> "-" <> check.node_ip)

  defp run_key(%{run_id: id}) when is_binary(id), do: dom_slug(String.slice(id, 0, 8))
  defp run_key(%{started_at: %DateTime{} = at}), do: DateTime.to_unix(at, :millisecond)
  defp run_key(_run), do: "unknown"
end
