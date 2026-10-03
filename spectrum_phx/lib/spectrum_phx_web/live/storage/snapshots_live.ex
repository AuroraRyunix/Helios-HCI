defmodule SpectrumPhxWeb.Storage.SnapshotsLive do
  @moduledoc """
  One vdisk's snapshots at `/storage/vdisks/:vdisk_id/snapshots`, read-only.

  The page says three things and offers no action. Which snapshots exist and who took them;
  which of them retention is forbidden to prune, and why; and what policy, if any, governs
  the disk. Taking, pruning and rolling back stay on `valcli` for now -- see
  `SpectrumPhx.Snapshots` for why the console does not yet get a button for the one
  operation that destroys what a guest wrote.

  ## An unreadable Hydra is not an empty list

  If the read fails the page says Hydra is not answering and shows nothing else. "No
  snapshots" is a statement about the cluster; a timeout is not.

  ## Refresh

  A server-side interval, because snapshots are written by a scheduled job from another
  process and there is nothing here to subscribe to. Set up inside `connected?/1`, so the
  static first render does no extra work, and rescheduled only after a read returns so a
  slow database degrades the rate instead of queueing ticks.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Storage.Components, only: [bytes: 1, slug: 1]

  alias SpectrumPhx.Snapshots

  @refresh_interval_ms 15_000

  @impl true
  def mount(%{"vdisk_id" => vdisk_id}, _session, socket) do
    if connected?(socket), do: schedule_refresh()

    {:ok,
     socket
     |> assign(page_title: "Snapshots of " <> vdisk_id, vdisk_id: vdisk_id)
     |> assign(view: nil, error: nil)
     |> load()}
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket = load(socket)
    schedule_refresh()
    {:noreply, socket}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, load(socket)}

  defp schedule_refresh, do: Process.send_after(self(), :refresh, @refresh_interval_ms)

  defp load(socket) do
    case Snapshots.for_vdisk(socket.assigns.vdisk_id) do
      {:ok, view} -> assign(socket, view: view, error: nil)
      # Keep what was last on screen: a failed read says nothing about the snapshots.
      {:error, reason} -> assign(socket, error: reason)
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app
      socket={@socket}
      flash={@flash}
      current_username={@current_username}
      active={:storage}
    >
      <.header>
        Snapshots of <span class="font-mono">{@vdisk_id}</span>
        <:subtitle>
          Read-only. Taken and pruned by the snapshot policy; rolled back with <span class="font-mono">valcli storage.rollback</span>.
        </:subtitle>
        <:actions>
          <.link navigate={~p"/storage"} class="btn btn-ghost btn-sm" id="back-to-storage">
            <.icon name="hero-arrow-left" class="size-4" /> Storage
          </.link>
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </.button>
        </:actions>
      </.header>

      <div :if={@error == :not_found} class="alert alert-warning items-start" id="snapshots-not-found">
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <p class="text-sm">There is no vdisk named <span class="font-mono">{@vdisk_id}</span>.</p>
      </div>

      <div
        :if={@error == :invalid_name}
        class="alert alert-warning items-start"
        id="snapshots-invalid"
      >
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <p class="text-sm">That is not a vdisk id.</p>
      </div>

      <div
        :if={@error not in [nil, :not_found, :invalid_name]}
        class="alert alert-warning items-start"
        id="snapshots-db-error"
      >
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <div>
          <p class="font-semibold">Hydra is not answering</p>
          <p class="text-sm opacity-90">
            This is not a statement that the vdisk has no snapshots.
          </p>
          <p class="text-xs opacity-70 mt-1 font-mono break-all">{describe(@error)}</p>
        </div>
      </div>

      <div :if={@view} class="space-y-4">
        <div class="card card-border bg-base-100" id="snapshot-policy">
          <div class="card-body gap-1 p-4">
            <h2 class="font-semibold">Policy</h2>
            <.policy_line policy={@view.policy} />
          </div>
        </div>

        <p :if={@view.snapshots == []} id="snapshots-empty" class="text-sm opacity-70">
          Hydra answered and this vdisk has no snapshots.
        </p>

        <div :if={@view.snapshots != []} class="overflow-x-auto card card-border bg-base-100">
          <table class="table table-sm" id="snapshots-table">
            <thead>
              <tr>
                <th>Snapshot</th>
                <th>Taken by</th>
                <th>Taken</th>
                <th class="text-right">Size</th>
                <th>Retention</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={snap <- @view.snapshots} id={"snapshot-" <> slug(snap.id)}>
                <td class="font-mono">{snap.id}</td>
                <td><.origin_badge origin={snap.origin} /></td>
                <td class="tabular-nums">{format_time(snap.taken_at_ms)}</td>
                <td class="text-right tabular-nums">{bytes(snap.size_bytes)}</td>
                <td class="text-xs">
                  <span :if={snap.has_children?} class="badge badge-sm badge-info gap-1">
                    <.icon name="hero-lock-closed" class="size-3" /> a vdisk was derived from it
                  </span>
                  <span :if={!snap.has_children? and snap.origin == :policy} class="opacity-70">
                    pruned by retention once past keep
                  </span>
                  <span :if={!snap.has_children? and snap.origin != :policy} class="opacity-70">
                    never pruned automatically
                  </span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </Layouts.app>
    """
  end

  attr :policy, :any, required: true

  defp policy_line(%{policy: {:policy, p}} = assigns) do
    assigns = assign(assigns, :p, p)

    ~H"""
    <p class="text-sm" id="policy-active">
      Snapshotted every {format_interval(@p.every_seconds)}, keeping the newest {@p.keep}
      <span class="opacity-60">({@p.scope}{scope_suffix(@p)})</span>.
    </p>
    """
  end

  defp policy_line(%{policy: :exempt} = assigns) do
    ~H"""
    <p class="text-sm" id="policy-exempt">
      Exempt: a policy for this vdisk turns scheduled snapshots off, whatever the cluster default is.
    </p>
    """
  end

  defp policy_line(assigns) do
    ~H"""
    <p class="text-sm opacity-80" id="policy-none">
      No policy covers this vdisk, so nothing snapshots it on a schedule.
    </p>
    """
  end

  attr :origin, :atom, required: true

  defp origin_badge(assigns) do
    ~H"""
    <span class={["badge badge-sm", origin_class(@origin)]}>{origin_label(@origin)}</span>
    """
  end

  defp origin_class(:policy), do: "badge-success"
  defp origin_class(:manual), do: "badge-ghost"
  defp origin_class(:pre_rollback), do: "badge-warning"
  defp origin_class(:unindexed), do: "badge-outline"

  defp origin_label(:policy), do: "policy"
  defp origin_label(:manual), do: "an operator"
  defp origin_label(:pre_rollback), do: "before a rollback"
  defp origin_label(:unindexed), do: "unindexed"

  defp scope_suffix(%{scope: "cluster"}), do: " default"
  defp scope_suffix(%{target: target}), do: " " <> target

  defp format_interval(seconds)
       when is_integer(seconds) and seconds >= 86_400 and rem(seconds, 86_400) == 0,
       do: "#{div(seconds, 86_400)} day(s)"

  defp format_interval(seconds) when is_integer(seconds),
    do: "#{Float.round(seconds / 3600, 1)} hour(s)"

  defp format_interval(_other), do: "an unknown interval"

  defp format_time(ms) when is_integer(ms) do
    case DateTime.from_unix(ms, :millisecond) do
      {:ok, at} -> Calendar.strftime(at, "%Y-%m-%d %H:%M UTC")
      _ -> "unknown"
    end
  end

  defp format_time(_other), do: "unknown"

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)
end
