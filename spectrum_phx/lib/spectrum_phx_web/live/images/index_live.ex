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
  row first and then fired the LINSTOR delete and a fan-out `rm` without checking either.
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
  the *first* chunk is the one that waits for the DRBD resource to be created and the
  device to appear -- see `SpectrumPhx.Images.upload_note/0` for why that work is there
  and not in the writer's `init/1`.

  ## Route

  Not wired here. `live "/images", SpectrumPhxWeb.Images.IndexLive, :index` belongs in
  the router, inside whatever `live_session` carries the authentication `on_mount` hook.
  This module assumes nothing about the session and adds no auth of its own.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Storage.Components, only: [bytes: 1, slug: 1]

  alias SpectrumPhx.Images

  @refresh_interval_ms 10_000

  # 64 KB would be sixteen thousand round trips per gibibyte. Kept under the endpoint's
  # 8 MB frame cap with room for the channel envelope.
  @chunk_bytes 1_048_576
  # The first chunk waits for a DRBD resource to be created on every node and its device
  # to appear, so this is not a transfer timeout -- it is a provisioning one.
  @chunk_timeout_ms 120_000
  @max_image_bytes 64 * 1024 * 1024 * 1024

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Images.subscribe()
      :timer.send_interval(@refresh_interval_ms, self(), :refresh)
    end

    socket =
      socket
      |> assign(page_title: "Images", confirming: nil, db_error: nil, delete_error: nil)
      |> assign(upload_note: Images.upload_note(), upload_error: nil)
      |> assign(max_image_bytes: @max_image_bytes)
      |> allow_upload(:image,
        accept: ~w(.iso .qcow2 .img),
        max_entries: 1,
        max_file_size: @max_image_bytes,
        chunk_size: @chunk_bytes,
        chunk_timeout: @chunk_timeout_ms,
        auto_upload: false,
        writer: fn _name, entry, _socket ->
          {SpectrumPhx.Images.UploadWriter,
           [name: entry.client_name, size_bytes: entry.client_size]}
        end
      )
      |> load_images()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load_images(socket)}
  def handle_info({:image_deleted, _name}, socket), do: {:noreply, load_images(socket)}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, load_images(socket)}

  # Selecting a file only validates it. Nothing is allocated and nothing is transferred
  # until "upload" -- `auto_upload: false` is what makes that true.
  def handle_event("validate_upload", _params, socket) do
    {:noreply, assign(socket, upload_error: nil)}
  end

  def handle_event("cancel_upload", %{"ref" => ref}, socket) do
    {:noreply, socket |> cancel_upload(:image, ref) |> assign(upload_error: nil)}
  end

  # Runs after every chunk has been written and the writer has closed. By this point the
  # image is either registered or rolled back; there is nothing left to consume but the
  # outcome the writer recorded.
  def handle_event("upload", _params, socket) do
    results = consume_uploaded_entries(socket, :image, fn meta, _entry -> {:ok, meta.result} end)

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

  # LiveView's own client-side rejections, which never reach the writer.
  defp upload_error_message(:too_large), do: "That file is larger than this console accepts."

  defp upload_error_message(:not_accepted),
    do: "Only .iso, .qcow2 and .img files can be uploaded."

  defp upload_error_message(:too_many_files), do: "Upload one image at a time."
  defp upload_error_message(:external_client_failure), do: "The browser could not read the file."
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
    case Images.list_images() do
      {:ok, images} ->
        assign(socket, images: images, db_error: nil)

      {:error, reason} ->
        # Keep whatever was last on screen. An unreachable Hydra does not mean the
        # catalogue is empty, and rendering it as empty would invite a re-upload of an
        # image that is already there.
        socket
        |> assign_new(:images, fn -> [] end)
        |> assign(db_error: describe(reason))
    end
  end

  defp error_message(:not_found), do: "not in the image catalogue."
  defp error_message(:invalid_name), do: "that is not a usable image name."

  defp error_message({:unsafe_path, path}) do
    "the catalogue points at #{path}, which is neither a DRBD device nor a file in " <>
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
    <Layouts.app flash={@flash} current_username={@current_username} active={:images}>
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

      <form
        id="upload-form"
        phx-submit="upload"
        phx-change="validate_upload"
        class="card card-border bg-base-100"
      >
        <div class="card-body gap-3">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <h2 class="card-title text-base">Upload an image</h2>
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
            <progress class="progress progress-primary w-full" value={entry.progress} max="100">
              {entry.progress}%
            </progress>
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

          <p :if={@upload_error} id="upload-error" class="text-sm text-error">{@upload_error}</p>

          <div class="card-actions items-center justify-between">
            <p class="text-xs opacity-60 max-w-2xl">{@upload_note}</p>
            <.button
              id="upload-button"
              variant="primary"
              phx-disable-with="Uploading..."
              disabled={@uploads.image.entries == []}
            >
              Upload
            </.button>
          </div>
        </div>
      </form>

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
                <span :if={image.on_drbd?} class="badge badge-sm badge-info gap-1">
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
              <span :if={image.on_drbd?}>
                the LINSTOR resource <code class="font-mono">{Images.resource_name(image.name)}</code>
                and every replica of it
              </span>
              <span :if={not image.on_drbd?}>the file on every node</span>
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
