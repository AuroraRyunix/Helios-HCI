defmodule SpectrumPhxWeb.Lcm.IndexLive do
  @moduledoc """
  Life-cycle management: what is installed, what is available, and a rolling upgrade
  while it runs.

  The page earns its keep during an upgrade, so that is what it is built around: the
  node-by-node bar, which node is being worked on, which are done, and the job's output
  as it arrives. It polls quickly while something is running and slowly when nothing is,
  because an idle cluster does not need its inventory re-read every two seconds.

  ## The two long operations are buttons that queue tasks

  Uploading a package streams the archive past this tier to the ZooKeeper leader and then
  queues the validation and the fan-out as a Catalyst task. Starting an upgrade queues a
  task that marks the job STARTING and then watches it to its verdict. Neither does the
  work inside the event, because both run for minutes and the ring in the header is where
  an operator watches minutes go by.

  Starting confirms first. It is one click away from putting every node in the cluster
  through maintenance and a reboot, and the confirmation names how many nodes that is.

  ## The upload streams

  `SpectrumPhx.Lcm.PackageUploadWriter` pushes each chunk onto an open request to the
  leader's spark-daemon, exactly as the image page does. `auto_upload: false` is
  deliberate: selecting a file must not put an archive on a hypervisor, and the transfer
  starts when the operator says so.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [panel: 1, figure: 1, dom_slug: 1]
  import SpectrumPhxWeb.Storage.Components, only: [bytes: 1]

  alias SpectrumPhx.Lcm
  alias SpectrumPhx.Lcm.PackageUploadWriter

  @idle_interval_ms 60_000
  @running_interval_ms 3_000

  # 64 KB would be four thousand round trips for a 256 MB package. Kept under the
  # endpoint's frame cap with room for the channel envelope.
  @chunk_bytes 1_048_576
  # The first chunk opens the request to the leader, so this is a connection bound, not a
  # transfer one.
  @chunk_timeout_ms 120_000
  @max_package_bytes 4 * 1024 * 1024 * 1024

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(page_title: "LCM", timer: nil)
      |> assign(
        confirming_start?: false,
        upgrade_error: nil,
        upload_error: nil,
        submitted: nil,
        upload_note: Lcm.upload_note(),
        max_package_bytes: @max_package_bytes
      )
      |> allow_upload(:package,
        accept: ~w(.zip),
        max_entries: 1,
        max_file_size: @max_package_bytes,
        chunk_size: @chunk_bytes,
        chunk_timeout: @chunk_timeout_ms,
        auto_upload: false,
        writer: fn _name, entry, _socket ->
          {PackageUploadWriter, [name: entry.client_name, size_bytes: entry.client_size]}
        end
      )
      |> load()

    {:ok, if(connected?(socket), do: retime(socket), else: socket)}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, socket |> load() |> retime()}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, socket |> load() |> retime()}

  def handle_event("validate_upload", _params, socket) do
    {:noreply, assign(socket, upload_error: nil)}
  end

  def handle_event("cancel_upload", %{"ref" => ref}, socket) do
    {:noreply, socket |> cancel_upload(:package, ref) |> assign(upload_error: nil)}
  end

  # Runs once every chunk is on the leader. The writer's job is done by now; what is left
  # is asking the cluster to do something with what was staged, which is a separate act
  # and a separate task.
  def handle_event("upload", _params, socket) do
    results =
      consume_uploaded_entries(socket, :package, fn meta, _entry -> {:ok, meta.result} end)

    case results do
      [{:ok, staged}] ->
        case Lcm.load_package(staged.filename, staged.size_bytes) do
          {:ok, task_id} ->
            {:noreply,
             socket
             |> assign(upload_error: nil, submitted: {:package, task_id})
             |> put_flash(:info, "Package staged on #{staged.node}; validating it is now a task.")
             |> load()
             |> retime()}

          {:error, message} ->
            {:noreply, assign(socket, upload_error: message)}
        end

      [{:error, reason}] ->
        {:noreply, assign(socket, upload_error: PackageUploadWriter.describe(reason))}

      # `consume_uploaded_entries` only yields completed entries, so an empty list means
      # there was nothing to upload rather than an upload that failed.
      [] ->
        {:noreply, assign(socket, upload_error: "Choose an update package first.")}

      other ->
        {:noreply,
         assign(socket, upload_error: "The upload ended unexpectedly: #{inspect(other)}")}
    end
  end

  def handle_event("ask_start", _params, socket) do
    {:noreply, assign(socket, confirming_start?: true, upgrade_error: nil, submitted: nil)}
  end

  def handle_event("cancel_start", _params, socket) do
    {:noreply, assign(socket, confirming_start?: false, upgrade_error: nil)}
  end

  def handle_event("start_upgrade", _params, socket) do
    case Lcm.start_upgrade() do
      {:ok, task_id} ->
        {:noreply,
         socket
         |> assign(confirming_start?: false, upgrade_error: nil, submitted: {:upgrade, task_id})
         |> put_flash(:info, "Rolling upgrade started. Every node goes through maintenance.")
         |> load()
         |> retime()}

      {:error, message} ->
        {:noreply, assign(socket, confirming_start?: false, upgrade_error: message)}
    end
  end

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
          ></progress>

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
            The update table could not be read:
            <span class="font-mono">{@overview.update.error}</span>
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
                value={
                  if @overview.update.update_available?, do: "update available", else: "up to date"
                }
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
            <span
              :if={@overview.inventory.disagreements > 0}
              class="badge badge-sm badge-warning gap-1"
            >
              <.icon name="hero-exclamation-triangle" class="size-3" />
              {@overview.inventory.disagreements} disagree
            </span>
          </:actions>

          <p :if={not @overview.inventory.available?} class="text-sm opacity-55 italic">
            The inventory could not be read:
            <span class="font-mono">{@overview.inventory.error}</span>
          </p>
          <p
            :if={@overview.inventory.available? and @overview.inventory.components == []}
            class="text-sm opacity-55 italic"
          >
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

        <.panel
          id="lcm-package"
          title="Upgrade package"
          subtitle={"A signed .zip, up to #{bytes(@max_package_bytes)}, streamed to the leader"}
        >
          <p class="text-xs opacity-60 whitespace-pre-line">{@upload_note}</p>

          <form
            id="package-form"
            phx-submit="upload"
            phx-change="validate_upload"
            class="mt-3 flex flex-col gap-3"
          >
            <.live_file_input upload={@uploads.package} class="file-input file-input-sm w-full" />

            <div
              :for={entry <- @uploads.package.entries}
              class="space-y-1"
              id={"package-entry-#{entry.ref}"}
            >
              <div class="flex items-center justify-between gap-3 text-sm">
                <span class="truncate font-mono">{entry.client_name}</span>
                <span class="tabular-nums opacity-70">{bytes(entry.client_size)}</span>
              </div>
              <div class="flex items-center gap-3">
                <progress
                  class="progress progress-primary h-2 flex-1"
                  value={entry.progress}
                  max="100"
                ></progress>
                <span class="text-xs tabular-nums opacity-70">{entry.progress}%</span>
                <button
                  type="button"
                  phx-click="cancel_upload"
                  phx-value-ref={entry.ref}
                  class="btn btn-ghost btn-xs"
                  id={"package-cancel-#{entry.ref}"}
                >
                  Cancel
                </button>
              </div>
              <p :for={error <- upload_errors(@uploads.package, entry)} class="text-xs text-error">
                {upload_error_message(error)}
              </p>
            </div>

            <p :for={error <- upload_errors(@uploads.package)} class="text-sm text-error">
              {upload_error_message(error)}
            </p>
            <p :if={@upload_error} class="text-sm text-error" id="package-error">{@upload_error}</p>

            <div>
              <button type="submit" class="btn btn-primary btn-sm" id="package-upload">
                <.icon name="hero-arrow-up-tray" class="size-4" /> Upload package
              </button>
            </div>
          </form>
        </.panel>

        <.panel
          id="lcm-start"
          title="Start a rolling upgrade"
          subtitle="Every node, one at a time, through maintenance"
        >
          <p class="text-sm opacity-70">
            The upgrade itself is run by hylia on the ZooKeeper leader, exactly as it always
            was. What this button adds is a Catalyst task that lives as long as the upgrade
            does, so the ring reports it and a failure is somewhere an operator will see it.
          </p>

          <div :if={not @confirming_start?} class="mt-3">
            <button phx-click="ask_start" class="btn btn-primary btn-sm" id="upgrade-start">
              <.icon name="hero-play" class="size-4" /> Start the upgrade
            </button>
          </div>

          <div
            :if={@confirming_start?}
            class="alert alert-warning alert-soft mt-3 flex-col items-start gap-2"
            id="upgrade-confirm"
          >
            <span class="text-sm">
              This puts {target_count(@overview.job)} node(s) through maintenance and a
              reboot, one after another. Guests are migrated off each node before it goes.
            </span>
            <div class="flex gap-2">
              <button
                phx-click="start_upgrade"
                class="btn btn-warning btn-xs"
                id="upgrade-confirm-start"
              >
                Start it
              </button>
              <button phx-click="cancel_start" class="btn btn-ghost btn-xs">Cancel</button>
            </div>
          </div>

          <p :if={@upgrade_error} class="text-sm text-error mt-3" id="upgrade-error">
            {@upgrade_error}
          </p>
        </.panel>

        <p :if={@submitted} class="text-xs opacity-70" id="lcm-task">
          {submitted_word(@submitted)} submitted as
          <span class="font-mono">{elem(@submitted, 1)}</span>
          &mdash; <.link navigate={~p"/tasks"} class="link">watch it</.link>.
        </p>
      </div>
    </Layouts.app>
    """
  end

  defp version_of(component, node), do: Map.get(component.by_node, node, "—")

  # The count the confirmation quotes. Taken from the loaded job's target list, because
  # that is the list the upgrade will actually walk; when there is no job the start is
  # refused anyway and the sentence is never reached with a real number.
  defp target_count(%{targets: targets}) when is_list(targets) and targets != [],
    do: length(targets)

  defp target_count(_job), do: "every"

  defp submitted_word({:upgrade, _id}), do: "Rolling upgrade"
  defp submitted_word({:package, _id}), do: "Package validation"

  # LiveView's own upload errors, which are about the file the browser offered rather than
  # about anything the cluster did.
  defp upload_error_message(:too_large), do: "That file is larger than this console accepts."
  defp upload_error_message(:not_accepted), do: "An upgrade package is a .zip archive."
  defp upload_error_message(:too_many_files), do: "One package at a time."
  defp upload_error_message(:external_client_failure), do: "The browser could not read the file."
  defp upload_error_message(other), do: "The file was refused: #{inspect(other)}"

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
