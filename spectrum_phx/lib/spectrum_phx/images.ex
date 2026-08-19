defmodule SpectrumPhx.Images do
  @moduledoc """
  The Valhalla image catalogue: reads from `hydra.valhalla_images`, and delete.

  ## Reads are parameterised and columns are named

  `/api/images` ran `SELECT JSON * FROM hydra.valhalla_images` and handed whatever came
  back to the browser. Naming the columns pins the contract, and every value that varies
  travels as a bound parameter through `SpectrumPhx.Hydra` -- including the image name,
  which the Python delete path escaped by doubling single quotes at the call site.

  ## Delete removes the backing store *before* the row

  `/api/images/delete` deleted the catalogue row first, then fired
  `resource-definition delete` and a fan-out `rm -f` without checking either, and
  answered `200` regardless. A failed LINSTOR delete therefore left a DRBD resource
  holding storage that nothing in the UI could ever see again, and the operator was told
  the delete succeeded.

  Here the backing store goes first and its result is checked. If it cannot be removed
  the catalogue row stays and the caller gets the daemon's own message, so the image is
  still on the page and the delete can be retried. The row is only removed once there is
  nothing left for it to point at.

  ## The path is validated, not escaped and hoped for

  The old `rm -f {path_to_delete}` interpolated a database value straight into a root
  shell. The value is quoted with `Spark.escape/1` here, but quoting alone is not the
  guard: the path must also be a DRBD device under `/dev/drbd/` (removed by deleting the
  LINSTOR resource, never with `rm`) or a file under the image container directory.
  Anything else is refused and reported rather than deleted.

  ## Upload is deliberately absent

  See `upload_note/0`.

  ## Test seam

  Reads go through `source/0` (`:hydra` or `{:static, rows}`) and the destructive half of
  `delete_image/1` goes through `backing_remover/0`. Under a static source the row is not
  written anywhere, matching `SpectrumPhx.Vms.create_vm/1`: an in-memory stand-in for the
  database would test the stand-in.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Spark

  @pubsub SpectrumPhx.PubSub
  @topic "images"

  @columns "name, filename, size_bytes, type, path, created_at"

  @list_cql "SELECT #{@columns} FROM hydra.valhalla_images"
  @get_cql "SELECT #{@columns} FROM hydra.valhalla_images WHERE name = ?"
  @delete_cql "DELETE FROM hydra.valhalla_images WHERE name = ?"

  # Where `/api/images` scans for files and where upload writes stage. Only paths under
  # this prefix are eligible for removal with `rm`.
  @container_root "/var/lib/hci/aether/volumes/"
  @drbd_prefix "/dev/drbd/"

  # `slugify_image_name` in spectrum_server.py, reproduced exactly: the LINSTOR resource
  # backing an image is named from the slug, so a different slug would delete the wrong
  # resource or none at all.
  @slug_max_length 28
  @image_extensions [".iso", ".qcow2", ".img"]

  @doc "CQL used by `list_images/0`."
  def list_images_cql, do: @list_cql

  @doc "CQL used by `get_image/1`."
  def get_image_cql, do: @get_cql

  @doc "CQL used by `delete_image/1`."
  def delete_image_cql, do: @delete_cql

  @doc "Directory images are staged in when they are files rather than DRBD devices."
  def container_root, do: @container_root

  @doc "Subscribe the calling process to catalogue change notifications."
  def subscribe, do: Phoenix.PubSub.subscribe(@pubsub, @topic)

  # -- reads ---------------------------------------------------------------------------

  @doc """
  Every image in the catalogue, newest first.

  Returns `{:ok, [image]}` or `{:error, reason}`. `{:error, _}` means the catalogue could
  not be read, which is not the same as an empty catalogue and must not be rendered as
  one.
  """
  @spec list_images() :: {:ok, [map()]} | {:error, term()}
  def list_images do
    case source() do
      {:static, rows} ->
        {:ok, rows |> Enum.map(&from_row/1) |> sort()}

      :hydra ->
        with {:ok, rows} <- Hydra.query(@list_cql, []) do
          {:ok, rows |> Enum.map(&from_row/1) |> sort()}
        end
    end
  end

  @doc "One image by name, or `{:error, :not_found}`."
  @spec get_image(String.t()) :: {:ok, map()} | {:error, :not_found | :invalid_name | term()}
  def get_image(name) do
    with {:ok, name} <- validate_name(name) do
      case source() do
        {:static, rows} ->
          rows
          |> Enum.map(&from_row/1)
          |> Enum.find(&(&1.name == name))
          |> case do
            nil -> {:error, :not_found}
            image -> {:ok, image}
          end

        :hydra ->
          case Hydra.query(@get_cql, [{"text", name}]) do
            {:ok, [row | _rest]} -> {:ok, from_row(row)}
            {:ok, []} -> {:error, :not_found}
            {:error, reason} -> {:error, reason}
          end
      end
    end
  end

  # -- delete --------------------------------------------------------------------------

  @doc """
  Remove an image: its backing store first, then its catalogue row.

  Returns `{:ok, %{name: name, backing: :removed | :skipped}}`, or `{:error, reason}`
  where `reason` carries the daemon's own message. On any failure the catalogue row is
  left in place, so the image stays visible and the operation can be retried.

  `reason` is one of:

    * `:not_found` -- no such image.
    * `:invalid_name` -- the name could not have been catalogued.
    * `{:unsafe_path, path}` -- the row points somewhere this may not delete from.
    * `{:backing, message}` -- the host refused to remove the backing store.
    * `{:catalogue, reason}` -- the backing store is gone but the row could not be
      deleted. The image is no longer usable and the row must be cleaned up by hand.
  """
  @spec delete_image(String.t()) :: {:ok, map()} | {:error, term()}
  def delete_image(name) do
    with {:ok, name} <- validate_name(name),
         {:ok, image} <- get_image(name),
         {:ok, backing} <- remove_backing(image),
         :ok <- delete_row(name) do
      broadcast({:image_deleted, name})
      {:ok, %{name: name, backing: backing}}
    end
  end

  defp delete_row(name) do
    case source() do
      {:static, _rows} ->
        :ok

      :hydra ->
        case Hydra.query(@delete_cql, [{"text", name}]) do
          {:ok, _rows} -> :ok
          {:error, reason} -> {:error, {:catalogue, reason}}
        end
    end
  end

  defp remove_backing(image) do
    case backing_remover() do
      fun when is_function(fun, 1) -> fun.(image)
      nil -> do_remove_backing(image)
    end
  end

  # No path at all: the row was written by a scan that never recorded one. There is
  # nothing to remove, and saying so is better than inventing a path to delete.
  defp do_remove_backing(%{path: path}) when path in [nil, ""], do: {:ok, :skipped}

  defp do_remove_backing(%{path: path} = image) do
    cond do
      String.starts_with?(path, @drbd_prefix) -> delete_linstor_resource(image)
      safe_container_file?(path) -> remove_file_everywhere(path)
      true -> {:error, {:unsafe_path, path}}
    end
  end

  # A DRBD-backed image is removed by deleting the LINSTOR resource definition, which
  # tears the device down on every node. `rm` on `/dev/drbd/...` would delete a symlink
  # and leave the resource -- and the storage it holds -- behind.
  defp delete_linstor_resource(%{name: name}) do
    command = delete_resource_command(resource_name(name))

    case Spark.execute(local_ip(), command, timeout: 60) do
      {0, _stdout, _stderr} ->
        {:ok, :removed}

      {_rc, stdout, stderr} ->
        message = String.trim((stderr || "") <> " " <> (stdout || ""))

        # LINSTOR is being asked to delete something that is already gone. That is the
        # state this call was trying to reach, so it is not a failure -- but every other
        # non-zero exit is, and must not be swallowed the way the Python path did.
        if message =~ ~r/not found|does not exist|unknown resource/i do
          {:ok, :removed}
        else
          {:error, {:backing, blank_to_default(message, "LINSTOR refused the delete.")}}
        end
    end
  end

  @doc """
  The command used to delete an image's LINSTOR resource definition.

  `LS_CONTROLLERS` is set, as `run_linstor_cmd` in `spectrum_server.py` does: the client
  inside the container otherwise talks to localhost, and on a cluster whose controller is
  a different node the delete would fail with a connection error rather than doing
  anything. The addresses are re-rendered through `:inet.parse_strict_address/1`, so only
  something that is literally an address reaches the shell Spark runs this with -- and
  the resource name is `img-<slug>`, whose slug is `[a-z0-9_-]` by construction.

  Exposed so a test can pin the shape without a cluster.
  """
  def delete_resource_command(resource) do
    controllers =
      node_ips()
      |> Enum.map(&normalize_ip/1)
      |> Enum.reject(&is_nil/1)
      |> case do
        [] -> ["127.0.0.1"]
        ips -> ips
      end
      |> Enum.join(",")

    "podman exec -e LS_CONTROLLERS=" <>
      controllers <> " systemd-aether linstor resource-definition delete " <> resource
  end

  defp normalize_ip(value) when is_binary(value) do
    case :inet.parse_strict_address(String.to_charlist(String.trim(value))) do
      {:ok, address} -> address |> :inet.ntoa() |> to_string()
      {:error, _reason} -> nil
    end
  end

  defp normalize_ip(_value), do: nil

  # The image file exists on every node, so it has to be removed on every node. A node
  # that does not answer is reported: leaving a copy behind means the next upload of the
  # same name finds a file already there.
  defp remove_file_everywhere(path) do
    command = "rm -f -- " <> Spark.escape(path)

    case node_ips() do
      [] ->
        {:error, {:backing, "No cluster nodes are configured, so #{path} cannot be removed."}}

      ips ->
        ips
        |> Enum.map(fn ip -> {ip, Spark.execute(ip, command, timeout: 30)} end)
        |> Enum.reject(fn {_ip, {rc, _out, _err}} -> rc == 0 end)
        |> Enum.map(&removal_failure/1)
        |> case do
          [] -> {:ok, :removed}
          messages -> {:error, {:backing, Enum.join(messages, "; ")}}
        end
    end
  end

  defp removal_failure({ip, {_rc, out, err}}) do
    detail = blank_to_default((err || "") <> " " <> (out || ""), "removal failed")
    ip <> ": " <> detail
  end

  @doc """
  The LINSTOR resource definition backing an image, `img-<slug>`.

  This must keep producing what `slugify_image_name` in `spectrum_server.py` produces:
  images uploaded by the Python tier are named by it, and a delete that computes a
  different resource name deletes nothing (or, worse, something else).
  """
  def resource_name(image_name) do
    base =
      Enum.find_value(@image_extensions, image_name, fn extension ->
        if String.ends_with?(String.downcase(image_name), extension) do
          String.slice(image_name, 0, String.length(image_name) - String.length(extension))
        end
      end)

    slug =
      base
      |> String.downcase()
      |> String.replace(~r/[^a-z0-9_-]/, "-")
      |> String.replace(~r/-+/, "-")
      |> String.trim("-")
      |> String.slice(0, @slug_max_length)

    "img-" <> slug
  end

  @doc """
  Whether a path is a plain image file this module may remove.

  Rejects anything outside the container directory and anything containing a `..`
  segment, so a row whose `path` was written by something less careful cannot direct a
  root `rm` at an arbitrary file.
  """
  def safe_container_file?(path) when is_binary(path) do
    String.starts_with?(path, @container_root) and
      not String.contains?(path, "..") and
      not String.contains?(path, "\0") and
      String.length(path) > String.length(@container_root)
  end

  def safe_container_file?(_path), do: false

  # -- upload --------------------------------------------------------------------------

  @doc """
  Why there is no upload here, and what an implementation has to do.

  **Not implemented.** Listing and deleting are; uploading is not, and the gap is
  deliberate rather than an oversight.

  ## The constraint

  The web tier must not touch the data path. Spectrum's container mounts no `/dev`, so
  opening a DRBD device from here fails with `ENOENT` -- and mounting `/dev` in would be
  the wrong fix. Staging the file onto a storage mount instead is equally wrong: it is
  still the web tier writing cluster storage, and it needs somewhere to put a file the
  size of an install ISO. Spark owns host storage, the way Stargate rather than Prism
  owns it on Nutanix, so the bytes have to be handed to Spark and Spark has to do the
  write. `spectrum_server.py` now does exactly that, streaming the request body to
  `POST /api/v1/storage/device/write` on spark-daemon.

  ## What the sequence is

  Per `/api/images/upload`, in order, with the size known up front because the volume
  has to be defined before there is anywhere to write:

    1. `linstor resource-definition create img-<slug>`, then
       `volume-definition create img-<slug> <bytes rounded up to KiB>KiB`.
    2. `resource create <hostname> img-<slug> --storage-pool default-pool` on every host.
    3. `drbd-options --allow-two-primaries yes` -- correct *only* for images, which are
       attached read-only to guests on several hosts at once -- plus the split-brain
       recovery policy.
    4. Poll `Spark.device_info/2` until `/dev/drbd/by-res/img-<slug>/0` is a block device
       on the local host, up to about ten seconds.
    5. `Spark.drbd_role/4` to Primary, and **check the role that comes back**. A
       promotion that did not take means the peer still holds Primary; writing the device
       from a Secondary is the split-brain the check exists to prevent, and it must abort
       the upload rather than be logged.
    6. Stream the bytes to `POST /api/v1/storage/device/write?device=<path>` over the
       mTLS client, and verify the `written` count equals the content length -- a short
       write is a truncated image, not a successful upload.
    7. `Spark.device_prepare/4` to `root:qemu` `0660` (not `0666`: world-writable lets
       any local user corrupt the golden image every VM is cloned from), then
       `Spark.device_flush/2`, then demote back to Secondary.
    8. Insert the catalogue row.
    9. On any failure: demote to Secondary and `resource-definition delete img-<slug>`,
       or the half-built resource holds storage forever.

  ## Why it is a design task, not a port

  In LiveView the bytes arrive through `allow_upload/3` as chunks written by a
  `Phoenix.LiveView.UploadWriter`. The default writer spools to a temporary file, which
  reintroduces exactly the "stage it in the web tier" problem this split exists to avoid
  -- for a file that may be several gibibytes. A correct implementation needs a custom
  writer that opens the connection to Spark in `init/2`, pushes each chunk in `write/2`
  as it arrives, and closes and verifies the byte count in `close/2`, with backpressure
  so a slow host slows the browser rather than filling memory.

  That interacts with the DRBD sequence above in ways worth designing rather than
  guessing at: the resource has to exist before the first chunk arrives (so steps 1-5 run
  in `init/2`, where a failure has to cancel the upload cleanly), and the rollback in
  step 9 has to run from `close/2` on the error path *and* from the LiveView if the
  socket dies mid-upload. Getting that wrong leaks DRBD resources or leaves a resource
  Primary on a node that is no longer writing to it.

  Until then, uploads continue to work through the Python tier's `/api/images/upload`,
  which is the path that was just fixed. Images it creates appear in this list
  immediately, and can be deleted from here.
  """
  def upload_note do
    "Image upload is not implemented in the Phoenix tier. Uploads still go through the " <>
      "Python endpoint POST /api/images/upload, which streams the body to spark-daemon's " <>
      "/api/v1/storage/device/write so the web tier never touches the data path: this " <>
      "container mounts no /dev, and staging the file on a storage mount would be the " <>
      "web tier writing cluster storage just the same. Images uploaded there appear in " <>
      "this list immediately and can be deleted from here. See the documentation of " <>
      "SpectrumPhx.Images.upload_note/0 for what a LiveView implementation must do."
  end

  # -- seams ---------------------------------------------------------------------------

  @doc """
  Where catalogue reads come from: `:hydra` (the default) or `{:static, rows}`.

  Under `{:static, rows}` the row delete in `delete_image/1` is a no-op, so a test drives
  validation, the backing removal and the reporting without a database.
  """
  @spec source() :: :hydra | {:static, list()}
  def source, do: Application.get_env(:spectrum_phx, :images_source, :hydra)

  @doc """
  Override for the destructive half of `delete_image/1`.

  `nil` (the default) removes the real backing store through Spark. A 1-arity function
  receives the image map and returns `{:ok, :removed | :skipped}` or `{:error, reason}`.
  """
  @spec backing_remover() :: nil | (map() -> {:ok, atom()} | {:error, term()})
  def backing_remover, do: Application.get_env(:spectrum_phx, :images_backing_remover)

  # -- rows ----------------------------------------------------------------------------

  @doc """
  Normalise one catalogue row.

  The column is `size_bytes`, not `size`: `images.html` labels the column "Size (GB)" and
  divides by 1024^3 in the browser, which is presentation applied to a byte count. It is
  carried as bytes here and formatted at the edge.
  """
  def from_row(row) when is_map(row) do
    name = field(row, "name") || field(row, :name) || ""
    path = field(row, "path") || field(row, :path)

    %{
      name: to_string(name),
      filename: string(field(row, "filename") || field(row, :filename)) || to_string(name),
      type: string(field(row, "type") || field(row, :type)) || "unknown",
      size_bytes: integer(field(row, "size_bytes") || field(row, :size_bytes)),
      path: string(path),
      created_at: timestamp(field(row, "created_at") || field(row, :created_at)),
      on_drbd?: is_binary(path) and String.starts_with?(path, @drbd_prefix)
    }
  end

  defp field(row, key), do: Map.get(row, key)

  defp sort(images) do
    # Newest first, and rows with no timestamp last rather than sorted as though they
    # were the oldest -- an unknown creation date is not a creation date of zero.
    Enum.sort_by(images, fn image ->
      {is_nil(image.created_at), negated_unix(image.created_at), image.name}
    end)
  end

  defp negated_unix(nil), do: 0
  defp negated_unix(%DateTime{} = datetime), do: -DateTime.to_unix(datetime, :millisecond)

  # `created_at` is a CQL `timestamp`. Xandra decodes it to a `DateTime`; the Python tier
  # wrote it as epoch milliseconds, so an integer is accepted too.
  defp timestamp(%DateTime{} = datetime), do: datetime
  defp timestamp(%NaiveDateTime{} = naive), do: DateTime.from_naive!(naive, "Etc/UTC")

  defp timestamp(value) when is_integer(value) do
    case DateTime.from_unix(value, :millisecond) do
      {:ok, datetime} -> datetime
      {:error, _reason} -> nil
    end
  end

  defp timestamp(_value), do: nil

  # -- validation ----------------------------------------------------------------------

  # The name is a bound parameter everywhere it reaches CQL, so this is not injection
  # defence: it rejects values that could not name a catalogued image at all (empty,
  # oversized, or carrying a control character that would corrupt the log line and the
  # page it is echoed into).
  defp validate_name(name) when is_binary(name) do
    trimmed = String.trim(name)

    cond do
      trimmed == "" -> {:error, :invalid_name}
      String.length(trimmed) > 255 -> {:error, :invalid_name}
      String.match?(trimmed, ~r/[\x00-\x1f\x7f]/) -> {:error, :invalid_name}
      true -> {:ok, trimmed}
    end
  end

  defp validate_name(_name), do: {:error, :invalid_name}

  # -- helpers -------------------------------------------------------------------------

  defp broadcast(message) do
    Phoenix.PubSub.broadcast(@pubsub, @topic, message)
  rescue
    ArgumentError -> :ok
  catch
    :exit, _reason -> :ok
  end

  defp node_ips do
    Config.node_ips()
  catch
    :exit, _reason -> []
  end

  defp local_ip do
    Config.local_ip()
  catch
    :exit, _reason -> "127.0.0.1"
  end

  defp blank_to_default(value, default) do
    case String.trim(value || "") do
      "" -> default
      trimmed -> trimmed
    end
  end

  defp string(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp string(_value), do: nil

  defp integer(value) when is_integer(value), do: value
  defp integer(value) when is_float(value), do: trunc(value)

  defp integer(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {int, _rest} -> int
      :error -> nil
    end
  end

  defp integer(_value), do: nil
end
