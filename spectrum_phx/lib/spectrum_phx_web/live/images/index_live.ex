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

  ## Upload is not here

  See `SpectrumPhx.Images.upload_note/0`. The note is rendered on the page rather than
  left in a comment, because an operator looking for the upload button needs to know
  where uploading still works.

  ## Route

  Not wired here. `live "/images", SpectrumPhxWeb.Images.IndexLive, :index` belongs in
  the router, inside whatever `live_session` carries the authentication `on_mount` hook.
  This module assumes nothing about the session and adds no auth of its own.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Storage.Components, only: [bytes: 1, slug: 1]

  alias SpectrumPhx.Images

  @refresh_interval_ms 10_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Images.subscribe()
      :timer.send_interval(@refresh_interval_ms, self(), :refresh)
    end

    socket =
      socket
      |> assign(page_title: "Images", confirming: nil, db_error: nil, delete_error: nil)
      |> assign(upload_note: Images.upload_note())
      |> load_images()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load_images(socket)}
  def handle_info({:image_deleted, _name}, socket), do: {:noreply, load_images(socket)}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, load_images(socket)}

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

      <div class="alert alert-info alert-soft items-start" id="upload-note">
        <.icon name="hero-information-circle" class="size-5 shrink-0" />
        <p class="text-sm">{@upload_note}</p>
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
