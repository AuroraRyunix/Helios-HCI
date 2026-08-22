defmodule SpectrumPhx.ImagesUploadTest do
  # Not async: the uploader, transport and catalogue source are all application env.
  use ExUnit.Case, async: false

  alias SpectrumPhx.Images
  alias SpectrumPhx.Images.UploadWriter
  alias SpectrumPhx.UploadStubs

  setup do
    Application.put_env(:spectrum_phx, :images_source, {:static, []})
    on_exit(fn -> Application.delete_env(:spectrum_phx, :images_source) end)
    :ok
  end

  defp writer(name \\ "rocky.iso", size \\ 12) do
    {:ok, state} = UploadWriter.init(name: name, size_bytes: size)
    state
  end

  defp drain do
    receive do
      {:upload_stub, message} -> [message | drain()]
    after
      0 -> []
    end
  end

  describe "the happy path" do
    setup do
      UploadStubs.install()
      :ok
    end

    test "allocates, streams, verifies, then registers -- in that order" do
      state = writer("rocky.iso", 12)

      {:ok, state} = UploadWriter.write_chunk("hello ", state)
      {:ok, state} = UploadWriter.write_chunk("world!", state)
      {:ok, state} = UploadWriter.close(state, :done)

      assert %{result: {:ok, image}} = UploadWriter.meta(state)
      assert image.name == "rocky.iso"
      assert image.size_bytes == 12
      assert image.path == "/dev/drbd/by-res/img-rocky/0"

      calls = drain()

      # The catalogue row is the last thing that happens. Written earlier it would claim
      # an image is usable while the bytes were still in flight.
      assert [
               {:linstor_create, _ip, "img-rocky", 1},
               {:device_info, _ip2, "/dev/drbd/by-res/img-rocky/0"},
               {:drbd_role, _ip3, "img-rocky", "primary"},
               {:open, _ip4, "/dev/drbd/by-res/img-rocky/0", 12},
               {:chunk, 6},
               {:chunk, 6},
               {:finish, "hello world!"},
               {:device_prepare, _ip5, _device, "root:qemu", "0660"},
               {:device_flush, _ip6, _device2},
               {:drbd_role, _ip7, "img-rocky", "secondary"}
             ] = calls
    end

    test "the bytes arrive intact and in order" do
      state = writer("rocky.iso", 9)

      {:ok, state} = UploadWriter.write_chunk("abc", state)
      {:ok, state} = UploadWriter.write_chunk("def", state)
      {:ok, state} = UploadWriter.write_chunk("ghi", state)
      {:ok, _state} = UploadWriter.close(state, :done)

      assert Enum.any?(drain(), &match?({:finish, "abcdefghi"}, &1))
    end

    test "nothing at all happens until the first chunk" do
      _state = writer()
      assert drain() == []
    end

    test "the device is left root:qemu 0660, never world-writable" do
      state = writer("rocky.iso", 3)
      {:ok, state} = UploadWriter.write_chunk("abc", state)
      {:ok, _state} = UploadWriter.close(state, :done)

      assert Enum.any?(drain(), &match?({:device_prepare, _, _, "root:qemu", "0660"}, &1))
    end
  end

  describe "failures unwind the allocation" do
    test "a promotion that does not take aborts rather than writing from a Secondary" do
      # The peer still holds Primary. Writing the device anyway is the split-brain the
      # role check exists to prevent, so this must fail and must not open a connection.
      UploadStubs.install(%{drbd_role: {:ok, %{"role" => "secondary"}}})

      state = writer()
      assert {:error, {:promote, message}, state} = UploadWriter.write_chunk("abc", state)
      assert message =~ "not Primary"

      calls = drain()
      refute Enum.any?(calls, &match?({:open, _, _, _}, &1))
      assert Enum.any?(calls, &match?({:linstor_delete, _ip, "img-rocky"}, &1))

      # A later chunk must not restart anything.
      assert {:error, _reason, _state} = UploadWriter.write_chunk("def", state)
      refute Enum.any?(drain(), &match?({:linstor_create, _, _, _}, &1))
    end

    test "a connection that cannot be opened deletes the storage it just allocated" do
      UploadStubs.install(%{open: {:error, "connection refused"}})

      state = writer()

      assert {:error, {:transport, "connection refused"}, _state} =
               UploadWriter.write_chunk("abc", state)

      assert Enum.any?(drain(), &match?({:linstor_delete, _ip, "img-rocky"}, &1))
    end

    test "a chunk that fails mid-stream rolls back" do
      UploadStubs.install(%{send_chunk: {:error, "closed"}})

      state = writer()
      assert {:error, {:transport, "closed"}, _state} = UploadWriter.write_chunk("abc", state)

      assert Enum.any?(drain(), &match?({:linstor_delete, _ip, "img-rocky"}, &1))
    end

    test "a short write is not registered" do
      UploadStubs.install(%{finish: {:ok, 2}})

      state = writer("rocky.iso", 3)
      {:ok, state} = UploadWriter.write_chunk("abc", state)

      assert {:error, {:truncated, message}} = UploadWriter.close(state, :done)
      assert message =~ "2 of 3 bytes"
      assert Enum.any?(drain(), &match?({:linstor_delete, _ip, "img-rocky"}, &1))
    end

    test "fewer bytes than declared is caught before the request is even finished" do
      UploadStubs.install()

      state = writer("rocky.iso", 10)
      {:ok, state} = UploadWriter.write_chunk("abc", state)

      assert {:error, {:truncated, message}} = UploadWriter.close(state, :done)
      assert message =~ "sent 3 of the 10 bytes"

      # finish/1 was never called: the body is short, so there is nothing to ask the host.
      refute Enum.any?(drain(), &match?({:finish, _}, &1))
    end

    test "cancelling mid-upload rolls back" do
      UploadStubs.install()

      state = writer("rocky.iso", 100)
      {:ok, state} = UploadWriter.write_chunk("abc", state)
      {:ok, state} = UploadWriter.close(state, :cancel)

      assert state.allocation == nil
      assert Enum.any?(drain(), &match?({:linstor_delete, _ip, "img-rocky"}, &1))
    end

    test "cancelling before any chunk touches nothing" do
      UploadStubs.install()

      {:ok, _state} = UploadWriter.close(writer(), :cancel)
      assert drain() == []
    end

    test "the connection is closed before the storage is deleted" do
      # The daemon holds the device open for the life of the write request, so a delete
      # issued first is refused with "resource is still in use" and the resource leaks.
      UploadStubs.install()

      state = writer("rocky.iso", 100)
      {:ok, state} = UploadWriter.write_chunk("abc", state)
      _ = drain()
      {:ok, _state} = UploadWriter.close(state, :cancel)

      calls = drain()
      closed_at = Enum.find_index(calls, &(&1 == :closed))
      deleted_at = Enum.find_index(calls, &match?({:linstor_delete, _, _}, &1))

      assert closed_at, "the transport was never closed"
      assert deleted_at, "the storage was never deleted"
      assert closed_at < deleted_at, "the delete was issued while the request was still open"
    end

    test "a rollback that is refused is retried rather than abandoned" do
      # Ditto: the device is released a moment after the request ends, so the first
      # delete can legitimately fail. Giving up there leaks storage on every node.
      UploadStubs.install(%{linstor_delete: {:error, "Resource is still in use"}})

      state = writer("rocky.iso", 100)
      {:ok, state} = UploadWriter.write_chunk("abc", state)
      {:ok, _state} = UploadWriter.close(state, :cancel)

      deletes = Enum.count(drain(), &match?({:linstor_delete, _, _}, &1))
      assert deletes > 1, "a refused delete was not retried"
    end

    test "a rollback runs once, not once per close" do
      UploadStubs.install(%{send_chunk: {:error, "closed"}})

      state = writer()
      {:error, _reason, state} = UploadWriter.write_chunk("abc", state)
      _ = drain()

      # close/2 after a failure must not delete again: by then the resource name may have
      # been reused by another upload, and deleting it would take out live storage.
      {:ok, _state} = UploadWriter.close(state, {:error, :whatever})
      refute Enum.any?(drain(), &match?({:linstor_delete, _, _}, &1))
    end
  end

  describe "prepare_upload/2" do
    setup do
      UploadStubs.install()
      :ok
    end

    test "refuses a name already in the catalogue" do
      Application.put_env(
        :spectrum_phx,
        :images_source,
        {:static, [%{"name" => "rocky.iso", "path" => "/dev/drbd/by-res/img-rocky/0"}]}
      )

      assert {:error, {:exists, "rocky.iso"}} = Images.prepare_upload("rocky.iso", 10)
      # Nothing was allocated, so there is nothing to roll back either.
      refute Enum.any?(drain(), &match?({:linstor_create, _, _, _}, &1))
    end

    test "rounds the volume up to whole KiB so the last partial KiB still fits" do
      assert {:ok, _allocation} = Images.prepare_upload("rocky.iso", 1025)
      assert Enum.any?(drain(), &match?({:linstor_create, _ip, "img-rocky", 2}, &1))
    end

    test "refuses a size it cannot define a volume for" do
      assert {:error, {:upload, _message}} = Images.prepare_upload("rocky.iso", 0)
    end

    test "gives up on a device that never appears, and cleans up" do
      UploadStubs.install(%{device_info: {:ok, %{"is_block" => false}}})
      Application.put_env(:spectrum_phx, :images_device_poll_attempts, 3)
      Application.put_env(:spectrum_phx, :images_device_poll_interval_ms, 1)

      on_exit(fn ->
        Application.delete_env(:spectrum_phx, :images_device_poll_attempts)
        Application.delete_env(:spectrum_phx, :images_device_poll_interval_ms)
      end)

      assert {:error, {:device, message}} = Images.prepare_upload("rocky.iso", 10)
      assert message =~ "did not appear"
      assert Enum.any?(drain(), &match?({:linstor_delete, _ip, "img-rocky"}, &1))
    end

    test "reports a size conflict as its own thing, not a generic failure" do
      UploadStubs.install(%{linstor_create: {:error, {409, "already exists at 4096 KiB"}}})

      assert {:error, {:size_conflict, message}} = Images.prepare_upload("rocky.iso", 10)
      assert message =~ "4096 KiB"
    end
  end

  describe "describe_upload_error/1" do
    test "names the subsystem that refused" do
      assert Images.describe_upload_error({:exists, "a.iso"}) =~ "already in the catalogue"
      assert Images.describe_upload_error({:allocate, "no pool"}) =~ "allocate storage"
      assert Images.describe_upload_error({:promote, "still Primary"}) == "still Primary"
      assert Images.describe_upload_error({:write, "HTTP 500"}) =~ "refused the image write"
      assert Images.describe_upload_error({:transport, "closed"}) =~ "streamed to the host"

      # The one an operator has to act on by hand: bytes on disk, no row pointing at them.
      assert Images.describe_upload_error({:catalogue, "timeout"}) =~ "no catalogue row"
    end
  end
end
