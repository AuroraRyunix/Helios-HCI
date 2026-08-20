defmodule SpectrumPhx.Images do
  @moduledoc """
  The Valhalla image catalogue: list, upload, and delete.

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

  ## Upload never touches the data path

  The bytes go from the browser to the host that owns the storage without this tier ever
  opening the device or staging a file. `prepare_upload/2`, `finish_upload/2`,
  `rollback_upload/1` and `register/1` are the ordered steps;
  `SpectrumPhx.Images.UploadWriter` streams the bytes between them. See `upload_note/0`
  for why it is built this way.

  ## Test seam

  Reads go through `source/0` (`:hydra` or `{:static, rows}`), the destructive half of
  `delete_image/1` goes through `backing_remover/0`, and the upload's host calls go
  through `uploader/0`. Under a static source no row is written anywhere, matching
  `SpectrumPhx.Vms.create_vm/1`: an in-memory stand-in for the database would test the
  stand-in.
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
  @insert_cql "INSERT INTO hydra.valhalla_images " <>
                "(name, filename, size_bytes, type, path, created_at) VALUES (?, ?, ?, ?, ?, ?)"

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
  Allocate the replicated storage an image will be written into, and take Primary on it.

  Everything up to the first byte: the LINSTOR resource, the volume definition sized from
  the image, placement on every node, dual-primary, and promotion of the local node. On
  success the device named in the result is a block device this node holds Primary and may
  be written; on failure nothing is left behind.

  `size_bytes` has to be known here, because a DRBD volume is defined with a size and
  cannot be grown into as bytes arrive. It comes from the browser's report of the file
  size; the byte count actually written is checked against it in `finish_upload/1`, so a
  client that under-reports produces a failed upload rather than a truncated image.

  Returns `{:ok, %{resource: ..., device: ..., created: boolean}}` or `{:error, reason}`.
  `created: false` means the resource was already there and was adopted -- which for an
  image the catalogue has never seen means a previous upload died before registering it.
  """
  @spec prepare_upload(String.t(), non_neg_integer()) :: {:ok, map()} | {:error, term()}
  def prepare_upload(name, size_bytes) when is_integer(size_bytes) and size_bytes > 0 do
    with {:ok, name} <- validate_name(name),
         :ok <- refuse_if_catalogued(name) do
      resource = resource_name(name)
      ip = local_ip()

      # Round up: a volume has to hold every byte, and the last partial KiB is a byte the
      # image needs. The daemon aligns further to DRBD's 4 KiB.
      size_kib = div(size_bytes + 1023, 1024)

      with {:ok, _created_info} <- allocate(ip, resource, size_kib),
           {:ok, device} <- await_device(ip, resource),
           :ok <- promote(ip, resource) do
        {:ok, %{resource: resource, device: device, node: ip, size_bytes: size_bytes}}
      else
        {:error, reason} ->
          # Nothing this call created may survive it. A half-built resource holds storage
          # on every node and is invisible to the catalogue, so it is never reclaimed.
          rollback_upload(%{resource: resource, node: ip})
          {:error, reason}
      end
    end
  end

  def prepare_upload(_name, _size_bytes), do: {:error, {:upload, "The image size is unknown."}}

  defp refuse_if_catalogued(name) do
    case get_image(name) do
      {:error, :not_found} -> :ok
      {:ok, _image} -> {:error, {:exists, name}}
      # The catalogue could not be read. Refusing is the safe direction: continuing would
      # promote and write a device that may be a live image's, on nothing more than a
      # failed read.
      {:error, reason} -> {:error, {:catalogue, reason}}
    end
  end

  defp allocate(ip, resource, size_kib) do
    case uploader().linstor_create(ip, resource, size_kib) do
      {:ok, info} -> {:ok, info}
      {:error, {409, message}} -> {:error, {:size_conflict, message}}
      {:error, reason} -> {:error, {:allocate, describe(reason)}}
    end
  end

  # The device is created by DRBD a moment after LINSTOR reports the resource placed, so
  # this polls rather than assuming. Ten seconds by default, as the Python path used; the
  # bound is configurable only so a test that exercises the give-up path need not take ten
  # seconds to do it.
  defp device_poll_attempts,
    do: Application.get_env(:spectrum_phx, :images_device_poll_attempts, 20)

  defp device_poll_interval_ms,
    do: Application.get_env(:spectrum_phx, :images_device_poll_interval_ms, 500)

  defp await_device(ip, resource), do: await_device(ip, resource, device_poll_attempts())

  defp await_device(ip, resource, attempts) do
    device = "/dev/drbd/by-res/" <> resource <> "/0"

    case uploader().device_info(ip, device) do
      {:ok, %{"is_block" => true}} ->
        {:ok, device}

      _other when attempts > 1 ->
        Process.sleep(device_poll_interval_ms())
        await_device(ip, resource, attempts - 1)

      _other ->
        {:error, {:device, "The DRBD device #{device} did not appear on #{ip}."}}
    end
  end

  # The role that comes back is checked, not the call's exit status. A promotion that did
  # not take means a peer still holds Primary, and writing the device from a Secondary is
  # the split-brain this exists to prevent -- so it aborts the upload rather than logging.
  defp promote(ip, resource) do
    case uploader().drbd_role(ip, resource, "primary") do
      {:ok, %{"role" => role}} ->
        if String.downcase(to_string(role)) == "primary" do
          :ok
        else
          {:error,
           {:promote,
            "#{resource} is #{role}, not Primary, after a promotion request. " <>
              "Another node may still hold Primary."}}
        end

      {:error, reason} ->
        {:error, {:promote, describe(reason)}}
    end
  end

  @doc """
  Complete an upload whose bytes have all been written: permissions, flush, demote.

  `written` is compared with the size the volume was defined for. A short write is a
  truncated image, so it fails here rather than being catalogued -- the case that
  previously produced an image that existed, mounted, and was incomplete.

  The device is left `root:qemu 0660`, not `0666`: qemu is the only consumer that needs
  it, and world-writable lets any local user corrupt the golden image every VM is cloned
  from.
  """
  @spec finish_upload(map(), non_neg_integer()) :: :ok | {:error, term()}
  def finish_upload(
        %{resource: resource, device: device, node: ip, size_bytes: expected},
        written
      ) do
    if written != expected do
      {:error,
       {:truncated,
        "The image was truncated: #{written} of #{expected} bytes reached the device."}}
    else
      uploader().device_prepare(ip, device, "root:qemu", "0660")
      uploader().device_flush(ip, device)
      demote(ip, resource)
      :ok
    end
  end

  @doc """
  Undo a prepared upload: demote this node and delete the resource.

  Called on every failure path, including one where the browser vanished mid-transfer.
  Both steps are best-effort and neither can fail the caller -- there is nothing useful a
  caller can do about a rollback that did not work, and the failure it is rolling back is
  the one worth reporting. A resource that will not delete is logged, because it is
  holding storage on every node that nothing will ever reclaim automatically.
  """
  @spec rollback_upload(map()) :: :ok
  def rollback_upload(%{resource: resource, node: ip}) do
    unwind(ip, resource, rollback_attempts())
  end

  # Retried, because the device is released asynchronously. spark-daemon holds the block
  # device open for the life of the write request, so an abandoned upload -- a cancelled
  # transfer, a truncated body, a browser that vanished -- leaves the device busy for a
  # moment after this tier has given up on it. The first demote and delete then fail with
  # "resource is still in use", and a rollback that gives up there leaks a DRBD resource
  # holding storage on every node, which is the thing it exists to prevent.
  defp unwind(ip, resource, attempts) do
    demote(ip, resource)

    case uploader().linstor_delete(ip, resource) do
      {:ok, _result} ->
        :ok

      {:error, _reason} when attempts > 1 ->
        Process.sleep(rollback_interval_ms())
        unwind(ip, resource, attempts - 1)

      {:error, reason} ->
        require Logger

        Logger.error(
          "[images] Could not delete #{resource} while rolling back an upload: " <>
            "#{describe(reason)}. It is holding storage on every node and must be " <>
            "removed by hand."
        )

        :ok
    end
  end

  defp rollback_attempts, do: Application.get_env(:spectrum_phx, :images_rollback_attempts, 6)

  defp rollback_interval_ms,
    do: Application.get_env(:spectrum_phx, :images_rollback_interval_ms, 1_000)

  defp demote(ip, resource) do
    uploader().drbd_role(ip, resource, "secondary")
    :ok
  end

  @doc """
  Register a completed image in the catalogue.

  Last, deliberately. A row is a claim that the image is usable, so it is written only
  once the bytes are on the device and the device has been flushed and demoted. The
  reverse order is what let `/api/images` list images whose upload had failed.
  """
  @spec register(map()) :: {:ok, map()} | {:error, term()}
  def register(%{name: name, size_bytes: size_bytes, device: device}) do
    image = %{
      name: name,
      filename: name,
      size_bytes: size_bytes,
      type: image_type(name),
      path: device,
      created_at: DateTime.utc_now() |> DateTime.to_unix(:millisecond)
    }

    case source() do
      {:static, _rows} ->
        broadcast({:image_registered, name})
        {:ok, from_row(stringify(image))}

      :hydra ->
        params = [
          {"text", image.name},
          {"text", image.filename},
          {"bigint", image.size_bytes},
          {"text", image.type},
          {"text", image.path},
          {"timestamp", image.created_at}
        ]

        case Hydra.query(@insert_cql, params) do
          {:ok, _rows} ->
            broadcast({:image_registered, name})
            {:ok, from_row(stringify(image))}

          {:error, reason} ->
            {:error, {:catalogue, reason}}
        end
    end
  end

  defp stringify(image) do
    Map.new(image, fn {key, value} -> {Atom.to_string(key), value} end)
  end

  defp image_type(name) do
    if String.ends_with?(String.downcase(name), ".iso"), do: "iso", else: "template"
  end

  @doc """
  A human-readable sentence for an upload failure.

  Every clause names the subsystem that refused, because the old console reported
  everything as one message and sent operators to the wrong place.
  """
  def describe_upload_error({:exists, name}),
    do: "An image named #{name} is already in the catalogue. Delete it first."

  def describe_upload_error({:size_conflict, message}),
    do: "The storage for this image already exists at a different size: #{message}"

  def describe_upload_error({:allocate, detail}),
    do: "The cluster could not allocate storage for this image: #{detail}"

  def describe_upload_error({:device, detail}), do: detail
  def describe_upload_error({:promote, detail}), do: detail
  def describe_upload_error({:truncated, detail}), do: detail

  def describe_upload_error({:transport, detail}),
    do: "The upload could not be streamed to the host: #{detail}"

  def describe_upload_error({:write, detail}), do: "The host refused the image write: #{detail}"

  def describe_upload_error({:catalogue, reason}),
    do:
      "The image was written but could not be registered: #{describe(reason)}. " <>
        "Its storage is allocated and no catalogue row points at it."

  def describe_upload_error({:upload, detail}), do: detail
  def describe_upload_error(:invalid_name), do: "That is not a usable image name."
  def describe_upload_error(other), do: "The upload failed: #{describe(other)}"

  defp describe(reason) when is_binary(reason), do: reason
  defp describe({_status, message}) when is_binary(message), do: message
  defp describe(reason), do: inspect(reason)

  @doc """
  Where the upload's host calls go: `SpectrumPhx.Images.SparkUploader` by default.

  The DRBD sequence is six calls against a real host holding real storage, so a test
  drives it through a stand-in. This is the seam for that, and the only one -- the writer
  streams bytes through Mint and is exercised separately.
  """
  def uploader, do: Application.get_env(:spectrum_phx, :images_uploader, __MODULE__.SparkUploader)

  defmodule SparkUploader do
    @moduledoc """
    The real host calls behind an image upload, one function per Spark endpoint.

    Exists so `SpectrumPhx.Images` can be tested without a cluster while still running the
    ordering, the role check and the rollback that matter.
    """
    alias SpectrumPhx.Spark

    def linstor_create(ip, resource, size_kib) do
      Spark.linstor_resource_create(ip, resource, {:kib, size_kib},
        allow_two_primaries: true,
        timeout: 240
      )
    end

    def linstor_delete(ip, resource), do: Spark.linstor_resource_delete(ip, resource)
    def device_info(ip, device), do: Spark.device_info(ip, device)
    def drbd_role(ip, resource, role), do: Spark.drbd_role(ip, resource, role)
    def device_prepare(ip, device, owner, mode), do: Spark.device_prepare(ip, device, owner, mode)
    def device_flush(ip, device), do: Spark.device_flush(ip, device)
  end

  @doc """
  How upload works, and why it is shaped this way.

  ## The constraint

  The web tier must not touch the data path. A container's `/dev` carries device nodes but
  not udev's subdirectories, so `/dev/drbd/by-res/<res>/0` does not exist inside one and
  opening it fails with `ENOENT` -- and mounting `/dev` in would be
  the wrong fix. Staging the file onto a storage mount instead is equally wrong: it is
  still the web tier writing cluster storage, and it needs somewhere to put a file the
  size of an install ISO. Spark owns host storage, the way Stargate rather than Prism
  owns it on Nutanix, so the bytes have to be handed to Spark and Spark has to do the
  write. `spectrum_server.py` now does exactly that, streaming the request body to
  `POST /api/v1/storage/device/write` on spark-daemon.

  ## What the sequence is

  In order, with the size known up front because a DRBD volume is defined with a size and
  cannot be grown into as bytes arrive:

    1. `prepare_upload/2` -- resource definition, volume definition sized in KiB,
       placement on every node, and `--allow-two-primaries` (correct *only* for images,
       which guests on several hosts attach read-only at the same time; never for a VM
       disk). Then poll until the device appears, and promote this node to Primary,
       **checking the role that comes back**. A promotion that did not take means a peer
       still holds Primary, and writing from a Secondary is the split-brain that check
       exists to prevent.
    2. `UploadWriter` streams each chunk onto `POST /api/v1/storage/device/write`.
    3. `finish_upload/2` -- verify the byte count, set `root:qemu 0660`, flush, demote.
    4. `register/1` -- the catalogue row, last, because a row is a claim that the image
       is usable.
    5. `rollback_upload/1` on every failure, or the half-built resource holds storage on
       every node forever.

  ## Why the preparation runs on the first chunk, not in `init/1`

  `Phoenix.LiveView.UploadWriter.init/1` runs inside the upload channel's `join`. The
  browser joins that channel with no explicit timeout, so it uses the socket default of
  ten seconds -- and on timeout it *rejoins*, which would run the whole DRBD sequence a
  second time and leak the first attempt's connection. Waiting for a DRBD device can take
  most of that budget on its own.

  `write_chunk/2` is bounded by `:chunk_timeout` instead, which `allow_upload/3` accepts
  as an option, so the preparation happens there on the first chunk under a limit this
  code actually controls. `init/1` does no network work at all.

  ## Why the writer holds a Mint connection rather than using Req

  Every other call in `SpectrumPhx.Spark` is one request and one response, which `Req`
  covers. An upload is neither: chunks arrive over a channel and have to be pushed onto a
  request that is already open, so the connection has to survive across `write_chunk/2`
  calls. Mint is the layer that allows that -- `stream_request_body/3` per chunk, in
  passive mode so no socket messages land in the channel's mailbox.

  It also gives backpressure for free: the send blocks until the socket accepts the data,
  so a slow host slows the browser rather than filling this node's memory with a
  multi-gibibyte image. That is the whole reason not to use the default writer, which
  spools to a temporary file and puts the web tier back on the data path -- exactly what
  the split above exists to prevent.
  """
  def upload_note do
    "Uploads stream straight through to the host that owns the storage: the browser " <>
      "sends chunks over the LiveView channel, and each one is pushed onto an open " <>
      "request to spark-daemon's /api/v1/storage/device/write. Nothing is staged here. " <>
      "The web tier never opens the device, which is why this container needs no /dev " <>
      "and no storage mount."
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
