defmodule SpectrumPhxWeb.Images.IndexLive do
  @moduledoc """
  The Valhalla image catalogue at `/images`: list and delete.

  ## Updates are pushed

  PubSub on `SpectrumPhx.Images` for deletes this tier made, and a server-side interval
  for everything else -- an upload completing through the Python endpoint writes the row
  from another process, so there is nothing here to subscribe to and the table has to be
  re-read. Both are gated on `connected?/1`.

  ## Delete confirms on the server

  The old page used the browser's `confirm()`. That is a dialog, not a guard: it lives
  entirely in the client, so anything that reaches the endpoint directly skips it, and it
  cannot be tested. Here the first click puts the row into a confirming state and renders
  what is about to happen; only the second, explicitly-named event deletes. LiveView's
  `data-confirm` would have the same client-only weakness, so it is not used.

  ## Delete reports what actually happened

  `/api/images/delete` answered `200` whatever the outcome -- it deleted the catalogue
  row first and then fired the storage delete and a fan-out `rm` without checking either.
  An operator watching the row disappear had no way to know the storage behind it was
  still allocated. `SpectrumPhx.Images.delete_image/1` removes the backing store first and
  returns the daemon's own message on failure; this view puts that message on screen and
  leaves the row where it is.

  ## Upload streams past this tier

  The browser's chunks go straight onto the block device through
  `SpectrumPhx.Images.UploadWriter`; nothing is staged here and no temporary file is
  written. `auto_upload: false` is deliberate -- the transfer starts on submit, so
  selecting a file cannot allocate storage, and an image is only ever registered because
  an operator asked for it.

  The chunk size is raised from the 64 KB default to 1 MiB: an install ISO at 64 KB a
  time is sixteen thousand round trips per gibibyte. `chunk_timeout` is raised because
  the *first* chunk is the one that creates the vdisk and attaches it -- see
  `SpectrumPhx.Images.upload_note/0` for why that work is there and not in the writer's
  `init/1`.

  ## Route

  Not wired here. `live "/images", SpectrumPhxWeb.Images.IndexLive, :index` belongs in
  the router, inside whatever `live_session` carries the authentication `on_mount` hook.
  This module assumes nothing about the session and adds no auth of its own.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Storage.Components, only: [bytes: 1, slug: 1]

  alias SpectrumPhx.Images
  alias SpectrumPhx.Tasks

  @refresh_interval_ms 5_000

  # 64 KB would be sixteen thousand round trips per gibibyte. Raised to 8 MiB (kept under
  # the endpoint's 16 MB frame cap) so each WebSocket frame saturates a full Sidon 8 MiB
  # group-commit batch instead of forcing 1 MiB stop-and-wait round trips.
  @chunk_bytes 8 * 1_048_576
  # Two chunks do the real work and this bounds both. The first creates the vdisk and
  # wins its ownership claim. The *last* one finishes the image: it waits for the host to
  # flush the whole body (up to ten minutes in the Mint transport), seals the vdisk
  # (up to five) and registers it, all inside that one chunk's reply.
  #
  # This is also the browser's own timeout for that push, and the browser has no handler
  # for it expiring: the chunk simply never completes, the upload never finishes, the
  # submit is never sent, and the page sits at "Uploading..." with no error anywhere.
  # That is what the old 120 s did to any image whose seal took longer than two minutes.
  # So the bound sits above the sum of the host-side ones.
  @chunk_timeout_ms 20 * 60_000
  @max_image_bytes 64 * 1024 * 1024 * 1024

  @default_url_form %{"url" => "", "name" => "", "sha256" => "", "insecure" => "false"}

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Images.subscribe()
      :timer.send_interval(@refresh_interval_ms, self(), :refresh)
    end

    socket =
      socket
      |> assign(page_title: "Images", confirming: nil, db_error: nil, delete_error: nil)
      |> assign(
        upload_note: Images.upload_note(),
        upload_error: nil,
        upload_blocked: false,
        upload_finalizing: false,
        url_form: @default_url_form,
        url_error: nil,
        active_downloads: [],
        dismissed_downloads: MapSet.new()
      )
      |> assign(max_image_bytes: @max_image_bytes)
      |> allow_upload(:image,
        accept: ~w(.iso .qcow2 .img .raw .vmdk .ova),
        max_entries: 1,
        max_file_size: @max_image_bytes,
        chunk_size: @chunk_bytes,
        chunk_timeout: @chunk_timeout_ms,
        auto_upload: true,
        writer: fn _name, entry, _socket ->
          # This function runs inside the LiveView process, so `self()` is the page the
          # writer reports to -- see the writer's "Failure is reported, never raised".
          {SpectrumPhx.Images.UploadWriter,
           [
             name: entry.client_name,
             size_bytes: entry.client_size,
             notify: self(),
             ref: entry.ref
           ]}
        end
      )
      |> load_images()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load_images(socket)}
  def handle_info({:image_deleted, _name}, socket), do: {:noreply, load_images(socket)}
  def handle_info({:image_download_queued, _info}, socket), do: {:noreply, load_images(socket)}

  # The upload writer's report of a failure, sent the moment it happens so the page does
  # not wait for the rest of the file to drain before saying so. The same reason arrives
  # again through consume_uploaded_entries on submit; assigning it twice is harmless.
  def handle_info({:upload_failed, _ref, reason}, socket) do
    {:noreply,
     assign(socket, upload_error: Images.describe_upload_error(reason), upload_finalizing: false)}
  end

  # Every byte has been sent; the host is now flushing, sealing and registering the image
  # inside the last chunk's reply, which is the longest silent stretch of an upload.
  def handle_info({:upload_finalizing, _ref}, socket) do
    {:noreply, assign(socket, upload_finalizing: true)}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, load_images(socket)}

  def handle_event("validate_url_download", params, socket) do
    form = Map.merge(@default_url_form, Map.take(params, ["url", "name", "sha256", "insecure"]))
    {:noreply, assign(socket, url_form: form, url_error: nil)}
  end

  def handle_event("download_url", params, socket) do
    form = Map.merge(@default_url_form, Map.take(params, ["url", "name", "sha256", "insecure"]))

    case Images.download_from_url(
           url: form["url"],
           name: form["name"],
           sha256: form["sha256"],
           insecure: form["insecure"] in ["true", "on", "1"]
         ) do
      {:ok, %{name: name, task_id: task_id}} ->
        {:noreply,
         socket
         |> put_flash(
           :info,
           "Started downloading #{name} directly to replicated storage (task #{String.slice(task_id, 0, 8)})."
         )
         |> assign(url_form: @default_url_form, url_error: nil)
         |> load_images()}

      {:error, reason} ->
        {:noreply,
         socket
         |> assign(url_form: form, url_error: Images.describe_upload_error(reason))}
    end
  end

  # Selecting a file only validates it. Nothing is allocated and nothing is transferred
  # until "upload" -- `auto_upload: false` is what makes that true. What *can* be known
  # without storage is checked here, so "that name is already taken" is said before the
  # browser reads a gibibyte, not after.
  def handle_event("validate_upload", _params, socket) do
    {:noreply, assign_preflight(socket)}
  end

  def handle_event("cancel_upload", %{"ref" => ref}, socket) do
    {:noreply,
     socket |> cancel_upload(:image, ref) |> assign(upload_error: nil, upload_blocked: false)}
  end

  def handle_event("dismiss_download", %{"id" => id}, socket) do
    dismissed = MapSet.put(Map.get(socket.assigns, :dismissed_downloads, MapSet.new()), id)
    active_downloads = Enum.reject(socket.assigns.active_downloads, &(&1.id == id))
    {:noreply, assign(socket, dismissed_downloads: dismissed, active_downloads: active_downloads)}
  end

  # Runs after every chunk has been written and the writer has closed. By this point the
  # image is either registered or rolled back; there is nothing left to consume but the
  # outcome the writer recorded.
  def handle_event("upload", _params, socket) do
    results = consume_uploaded_entries(socket, :image, fn meta, _entry -> {:ok, meta.result} end)
    socket = assign(socket, upload_finalizing: false)

    case results do
      [{:ok, image}] ->
        {:noreply,
         socket
         |> put_flash(:info, uploaded_message(image))
         |> assign(upload_error: nil)
         |> load_images()}

      [{:error, reason}] ->
        {:noreply, assign(socket, upload_error: Images.describe_upload_error(reason))}

      # No entry, or an entry the client never finished. consume_uploaded_entries only
      # yields completed entries, so an empty list means there was nothing to upload.
      [] ->
        {:noreply, assign(socket, upload_error: "Choose an image file first.")}

      other ->
        {:noreply,
         assign(socket, upload_error: "The upload ended unexpectedly: #{inspect(other)}")}
    end
  end

  # First click: nothing is deleted, the row asks for confirmation. The name is not
  # trusted here -- it is only used to match against a row that was actually loaded.
  def handle_event("ask_delete", %{"name" => name}, socket) do
    {:noreply, assign(socket, confirming: name, delete_error: nil)}
  end

  def handle_event("cancel_delete", _params, socket) do
    {:noreply, assign(socket, confirming: nil, delete_error: nil)}
  end

  def handle_event("confirm_delete", %{"name" => name}, socket) do
    case Images.delete_image(name) do
      {:ok, result} ->
        {:noreply,
         socket
         |> put_flash(:info, deleted_message(name, result))
         |> assign(confirming: nil, delete_error: nil)
         |> load_images()}

      {:error, reason} ->
        message = error_message(reason)

        # The row stays. The confirmation panel stays open too: collapsing it would
        # detach the message from the image it is about, and the operator needs to see
        # which delete did not happen.
        {:noreply,
         socket
         |> put_flash(:error, "#{name}: #{message}")
         |> assign(delete_error: {name, message})
         |> load_images()}
    end
  end

  defp assign_preflight(socket) do
    with [entry | _] <- socket.assigns.uploads.image.entries,
         {:error, reason} <- Images.preflight_upload(entry.client_name) do
      assign(socket, upload_error: Images.describe_upload_error(reason), upload_blocked: true)
    else
      _ -> assign(socket, upload_error: nil, upload_blocked: false)
    end
  end

  # LiveView's own client-side rejections, which never reach the writer.
  defp upload_error_message(:too_large), do: "That file is larger than this console accepts."

  defp upload_error_message(:not_accepted),
    do: "Only .iso, .qcow2, .img, .raw, .vmdk and .ova files can be uploaded."

  defp upload_error_message(:too_many_files), do: "Upload one image at a time."
  defp upload_error_message(:external_client_failure), do: "The browser could not read the file."

  defp upload_error_message({:writer_failure, reason}),
    do: Images.describe_upload_error(reason)

  defp upload_error_message(other), do: "The file was rejected: #{inspect(other)}"

  defp uploaded_message(%{name: name, size_bytes: size}) do
    "Uploaded #{name} (#{SpectrumPhxWeb.Storage.Components.bytes(size)}) and replicated " <>
      "it across the cluster."
  end

  defp deleted_message(name, %{backing: :skipped}) do
    "Removed #{name} from the catalogue. Its row recorded no path, so there was no " <>
      "backing store to remove -- check by hand that nothing was left behind."
  end

  defp deleted_message(name, _result) do
    "Deleted #{name} and the storage behind it."
  end

  defp load_images(socket) do
    dismissed = Map.get(socket.assigns, :dismissed_downloads, MapSet.new())
    active_downloads = load_active_downloads(dismissed)

    case Images.list_images() do
      {:ok, images} ->
        assign(socket, images: images, db_error: nil, active_downloads: active_downloads)

      {:error, reason} ->
        # Keep whatever was last on screen. An unreachable Hydra does not mean the
        # catalogue is empty, and rendering it as empty would invite a re-upload of an
        # image that is already there.
        socket
        |> assign_new(:images, fn -> [] end)
        |> assign(db_error: describe(reason), active_downloads: active_downloads)
    end
  end

  defp load_active_downloads(dismissed) do
    case Tasks.fetch(limit: 50) do
      %{available?: true, tasks: tasks} ->
        tasks
        |> Enum.filter(fn t ->
          t.action == "download_image" and not MapSet.member?(dismissed, t.id)
        end)
        |> Enum.filter(fn t ->
          t.state in [:pending, :processing, :failed] or
            (t.state == :completed and recent_task?(t.completed_at || t.updated_at))
        end)

      _ ->
        []
    end
  end

  defp recent_task?(nil), do: true

  defp recent_task?(%DateTime{} = dt) do
    DateTime.diff(DateTime.utc_now(), dt, :second) < 3600
  end

  defp recent_task?(_), do: true

  defp error_message(:not_found), do: "not in the image catalogue."
  defp error_message(:invalid_name), do: "that is not a usable image name."

  defp error_message({:unsafe_path, path}) do
    "the catalogue points at #{path}, which is neither a vdisk socket nor a file in " <>
      "#{Images.container_root()}. Refusing to delete it; fix the row by hand."
  end

  defp error_message({:backing, message}) do
    "the storage behind it could not be removed, so the catalogue entry was kept: " <>
      message
  end

  defp error_message({:catalogue, reason}) do
    "the storage was removed but the catalogue row could not be deleted (" <>
      describe(reason) <> "). The entry is now stale and must be cleaned up by hand."
  end

  defp error_message(reason), do: describe(reason)

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:images}>
      <.header>
        Images
        <:subtitle>
          {length(@images)} in the Valhalla catalogue
          <span :if={@db_error} class="text-error">-- last known list</span>
        </:subtitle>
        <:actions>
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </.button>
        </:actions>
      </.header>

      <div :if={@db_error} class="alert alert-warning items-start" id="images-db-error">
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <div>
          <p class="font-semibold">Hydra is not answering</p>
          <p class="text-sm opacity-90">
            This is the last list that was read, not a current one, and it is not a
            statement that the catalogue is empty.
          </p>
          <p class="text-xs opacity-70 mt-1 font-mono break-all">{@db_error}</p>
        </div>
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <form
          id="upload-form"
          phx-submit="upload"
          phx-change="validate_upload"
          class="card card-border bg-base-100"
        >
          <div class="card-body gap-3">
            <div class="flex flex-wrap items-center justify-between gap-2">
              <h2 class="card-title text-base">Upload from computer</h2>
              <span class="text-xs opacity-60">
                ISO, qcow2 or raw, up to {SpectrumPhxWeb.Storage.Components.bytes(@max_image_bytes)}
              </span>
            </div>

            <.live_file_input upload={@uploads.image} class="file-input file-input-bordered w-full" />

            <div :for={entry <- @uploads.image.entries} id={"entry-" <> entry.ref} class="space-y-1">
              <div class="flex items-center justify-between gap-3 text-sm">
                <span class="truncate font-mono">{entry.client_name}</span>
                <span class="tabular-nums opacity-70">
                  {SpectrumPhxWeb.Storage.Components.bytes(entry.client_size)}
                </span>
              </div>
              <div class="flex items-center gap-3">
                <progress
                  id={"progress-" <> entry.ref}
                  class="progress progress-primary w-full"
                  value={entry.progress}
                  max="100"
                >
                  {entry.progress}%
                </progress>
                <span class="text-xs tabular-nums opacity-70 w-10 text-right">
                  {entry.progress}%
                </span>
              </div>
              <div class="flex items-center justify-between gap-3">
                <p :for={error <- upload_errors(@uploads.image, entry)} class="text-xs text-error">
                  {upload_error_message(error)}
                </p>
                <button
                  type="button"
                  id={"cancel-upload-" <> entry.ref}
                  phx-click="cancel_upload"
                  phx-value-ref={entry.ref}
                  class="btn btn-ghost btn-xs"
                >
                  Cancel
                </button>
              </div>
            </div>

            <p :for={error <- upload_errors(@uploads.image)} class="text-sm text-error">
              {upload_error_message(error)}
            </p>

            <p :if={@upload_finalizing} id="upload-finalizing" class="text-sm opacity-80">
              All bytes sent. The host is flushing, sealing and registering the image; this can
              take a few minutes for a large one. Keep this page open.
            </p>

            <p :if={@upload_error} id="upload-error" role="alert" class="text-sm text-error">
              {@upload_error}
            </p>

            <div class="card-actions items-center justify-between mt-auto pt-2">
              <p class="text-xs opacity-60 max-w-md">{@upload_note}</p>
              <.button
                id="upload-button"
                variant="primary"
                phx-disable-with="Uploading..."
                disabled={@uploads.image.entries == [] or @upload_blocked or not Enum.all?(@uploads.image.entries, & &1.done?)}
              >
                <%= cond do %>
                  <% @uploads.image.entries == [] -> %>
                    Upload
                  <% @upload_finalizing -> %>
                    Flushing & Registering...
                  <% not Enum.all?(@uploads.image.entries, & &1.done?) -> %>
                    Uploading... {hd(@uploads.image.entries).progress}%
                  <% true -> %>
                    Register Image
                <% end %>
              </.button>
            </div>
          </div>
        </form>

        <form
          id="download-url-form"
          phx-submit="download_url"
          phx-change="validate_url_download"
          class="card card-border bg-base-100"
        >
          <div class="card-body gap-3">
            <div class="flex flex-wrap items-center justify-between gap-2">
              <h2 class="card-title text-base">Download from URL</h2>
              <span class="text-xs opacity-60">Direct cluster fetch via HTTP/HTTPS</span>
            </div>

            <div class="form-control w-full">
              <label class="label py-1" for="download-url-input">
                <span class="label-text text-xs font-medium">Image URL (HTTP / HTTPS)</span>
              </label>
              <input
                type="url"
                name="url"
                id="download-url-input"
                value={@url_form["url"]}
                placeholder="https://releases.ubuntu.com/24.04/ubuntu-24.04.2-live-server-amd64.iso"
                class="input input-bordered input-sm w-full font-mono text-xs"
                required
              />
            </div>

            <div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
              <div class="form-control">
                <label class="label py-1" for="download-name-input">
                  <span class="label-text text-xs">Save as filename (optional)</span>
                </label>
                <input
                  type="text"
                  name="name"
                  id="download-name-input"
                  value={@url_form["name"]}
                  placeholder="ubuntu-24.04-amd64.iso"
                  class="input input-bordered input-sm w-full font-mono text-xs"
                />
              </div>
              <div class="form-control">
                <label class="label py-1" for="download-sha256-input">
                  <span class="label-text text-xs">Expected SHA-256 (optional)</span>
                </label>
                <input
                  type="text"
                  name="sha256"
                  id="download-sha256-input"
                  value={@url_form["sha256"]}
                  placeholder="64-character hex digest"
                  class="input input-bordered input-sm w-full font-mono text-xs"
                />
              </div>
            </div>

            <label class="label cursor-pointer justify-start gap-2 py-0">
              <input
                type="checkbox"
                name="insecure"
                value="true"
                checked={@url_form["insecure"] in ["true", "on", "1"]}
                class="checkbox checkbox-xs"
              />
              <span class="label-text text-xs opacity-75">
                Allow self-signed / untrusted TLS certificates (<code class="font-mono">-k</code>)
              </span>
            </label>

            <p :if={@url_error} id="download-url-error" role="alert" class="text-sm text-error">
              {@url_error}
            </p>

            <div class="card-actions items-center justify-between mt-auto pt-2">
              <p class="text-xs opacity-60 max-w-md">
                Streams directly from the remote server into a replicated Sidon vdisk in the background.
              </p>
              <.button
                id="download-url-button"
                variant="primary"
                phx-disable-with="Starting..."
                disabled={String.trim(@url_form["url"] || "") == ""}
              >
                <.icon name="hero-cloud-arrow-down" class="size-4" /> Fetch to Storage
              </.button>
            </div>
          </div>
        </form>
      </div>

      <div
        :if={@active_downloads != []}
        id="active-downloads"
        class="card card-border bg-base-100"
      >
        <div class="card-body py-3 gap-2">
          <h3 class="text-xs font-semibold uppercase tracking-wider opacity-70">
            Image Downloads ({length(@active_downloads)})
          </h3>
          <div
            :for={dl <- @active_downloads}
            id={"download-task-" <> dl.id}
            class="space-y-1 py-1"
          >
            <div class="flex items-center justify-between gap-3 text-sm">
              <div class="flex items-center gap-2 min-w-0">
                <span :if={dl.state in [:pending, :processing]} class="loading loading-spinner loading-xs text-primary"></span>
                <span :if={dl.state == :completed} class="badge badge-success badge-xs font-mono">DONE</span>
                <span :if={dl.state == :failed} class="badge badge-error badge-xs font-mono">FAILED</span>
                <span class="font-mono font-medium truncate">
                  {dl.payload["name"] || dl.label}
                </span>
                <span class="text-xs opacity-60 truncate">
                  ({dl.payload["url"]})
                </span>
              </div>
              <div class="flex items-center gap-2">
                <span class="text-xs tabular-nums opacity-75">{dl.progress || 0}%</span>
                <button
                  :if={dl.state in [:completed, :failed]}
                  type="button"
                  id={"dismiss-task-" <> dl.id}
                  phx-click="dismiss_download"
                  phx-value-id={dl.id}
                  class="btn btn-ghost btn-xs text-xs opacity-70 hover:opacity-100"
                  title="Dismiss"
                >
                  ✕
                </button>
              </div>
            </div>
            <progress
              :if={dl.state in [:pending, :processing]}
              class="progress progress-primary w-full"
              value={dl.progress || 0}
              max="100"
            >
              {dl.progress || 0}%
            </progress>
            <p :if={dl.state == :failed and dl.error} class="text-xs text-error font-mono break-all">
              {dl.error}
            </p>
          </div>
        </div>
      </div>

      <p :if={@images == [] and is_nil(@db_error)} id="images-empty" class="text-sm opacity-70">
        Hydra answered and the catalogue is empty. No images are registered yet.
      </p>

      <ul id="images" class="divide-y divide-base-300">
        <li :for={image <- @images} id={"image-" <> slug(image.name)} class="py-3 space-y-2">
          <div class="flex flex-wrap items-start justify-between gap-3">
            <div class="min-w-0">
              <div class="flex flex-wrap items-center gap-2">
                <span class="font-semibold truncate">{image.name}</span>
                <span class="badge badge-sm badge-ghost uppercase">{image.type}</span>
                <span :if={image.on_vdisk?} class="badge badge-sm badge-info gap-1">
                  <.icon name="hero-server-stack" class="size-3" /> replicated
                </span>
              </div>
              <div class="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs opacity-70">
                <span class="font-mono truncate">{image.filename}</span>
                <span class="tabular-nums">{bytes(image.size_bytes)}</span>
                <span>{registered(image.created_at)}</span>
              </div>
              <p class="mt-1 text-xs font-mono opacity-50 truncate">
                {image.path || "no path recorded"}
              </p>
            </div>

            <div class="shrink-0">
              <.button
                :if={@confirming != image.name}
                id={"delete-" <> slug(image.name)}
                phx-click="ask_delete"
                phx-value-name={image.name}
              >
                Delete
              </.button>
            </div>
          </div>

          <div
            :if={@confirming == image.name}
            id={"confirm-" <> slug(image.name)}
            class="rounded-lg border border-error/40 bg-error/5 p-3 space-y-2"
          >
            <p class="text-sm">
              Delete <span class="font-semibold">{image.name}</span>? This removes
              <span :if={image.on_vdisk?}>
                the vdisk <code class="font-mono">{Images.resource_name(image.name)}</code>
                and its extent groups on every replica
              </span>
              <span :if={not image.on_vdisk?}>the file on every node</span>
              , then the catalogue entry. It cannot be undone, and any VM booting from it
              will lose its source.
            </p>
            <div class="flex gap-2">
              <.button
                id={"confirm-delete-" <> slug(image.name)}
                phx-click="confirm_delete"
                phx-value-name={image.name}
                phx-disable-with="Deleting..."
                variant="primary"
              >
                Yes, delete it
              </.button>
              <.button id={"cancel-delete-" <> slug(image.name)} phx-click="cancel_delete">
                Cancel
              </.button>
            </div>
          </div>

          <p
            :if={is_tuple(@delete_error) and elem(@delete_error, 0) == image.name}
            id={"delete-error-" <> slug(image.name)}
            class="text-sm text-error"
          >
            {elem(@delete_error, 1)}
          </p>
        </li>
      </ul>
    </Layouts.app>
    """
  end

  defp registered(nil), do: "registered date unknown"

  defp registered(%DateTime{} = datetime) do
    "registered " <> Calendar.strftime(datetime, "%Y-%m-%d %H:%M UTC")
  end
end
