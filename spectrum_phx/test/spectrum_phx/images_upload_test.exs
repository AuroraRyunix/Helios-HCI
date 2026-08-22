defmodule SpectrumPhx.ImagesUploadTest do
  # Not async: the uploader, transport and catalogue source are all application env.
  use ExUnit.Case, async: false

  alias SpectrumPhx.Images
  alias SpectrumPhx.Images.UploadWriter
  alias SpectrumPhx.UploadStubs

  @socket "/var/lib/hci/sidon/nbd/img-rocky.sock"

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

    test "creates, claims, streams, seals, then registers -- in that order" do
      state = writer("rocky.iso", 12)

      {:ok, state} = UploadWriter.write_chunk("hello ", state)
      {:ok, state} = UploadWriter.write_chunk("world!", state)
      {:ok, state} = UploadWriter.close(state, :done)

      assert %{result: {:ok, image}} = UploadWriter.meta(state)
      assert image.name == "rocky.iso"
      assert image.size_bytes == 12
      assert image.path == @socket

      calls = drain()

      # The catalogue row is the last thing that happens. Written earlier it would claim
      # an image is usable while the bytes were still in flight. The seal is second-last,
      # because an unsealed image is a writable template.
      assert [
               {:create, _ip, "img-rocky", 12},
               {:attach, _ip2, "img-rocky"},
               {:open, _ip3, "img-rocky", 12},
               {:chunk, 6},
               {:chunk, 6},
               {:finish, "hello world!"},
               {:seal, _ip4, "img-rocky"}
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

    test "the image is sealed, so it can never be written again" do
      state = writer("rocky.iso", 3)
      {:ok, state} = UploadWriter.write_chunk("abc", state)
      {:ok, _state} = UploadWriter.close(state, :done)

      # There are no permissions to set: an image is reached over a per-vdisk socket
      # Sidon creates group-owned by qemu, not through a device node this tier chmods.
      # Immutability is what makes it safe to attach on several hosts at once.
      assert Enum.any?(drain(), &match?({:seal, _ip, "img-rocky"}, &1))
    end

    test "an image the seal refuses is not registered" do
      UploadStubs.install(%{seal: {:error, "journal is not drained"}})

      state = writer("rocky.iso", 3)
      {:ok, state} = UploadWriter.write_chunk("abc", state)

      assert {:error, {:seal, message}} = UploadWriter.close(state, :done)
      assert message =~ "must not be used as a template"
      assert Enum.any?(drain(), &match?({:delete, _ip, "img-rocky"}, &1))
    end
  end

  describe "failures unwind the allocation" do
    test "an attach another host refuses aborts rather than writing anyway" do
      # The ownership compare-and-swap was lost, so another host is serving this vdisk.
      # Writing it from here is what the epoch fence exists to prevent, and the refusal
      # names the host that holds it.
      UploadStubs.install(%{attach: {:error, {409, "hci-02 owns img-rocky at epoch 4"}}})

      state = writer()
      assert {:error, {:claim, message}, state} = UploadWriter.write_chunk("abc", state)
      assert message =~ "hci-02"

      calls = drain()
      refute Enum.any?(calls, &match?({:open, _, _, _}, &1))
      assert Enum.any?(calls, &match?({:delete, _ip, "img-rocky"}, &1))

      # A later chunk must not restart anything.
      assert {:error, _reason, _state} = UploadWriter.write_chunk("def", state)
      refute Enum.any?(drain(), &match?({:create, _, _, _}, &1))
    end

    test "a connection that cannot be opened deletes the storage it just allocated" do
      UploadStubs.install(%{open: {:error, "connection refused"}})

      state = writer()

      assert {:error, {:transport, "connection refused"}, _state} =
               UploadWriter.write_chunk("abc", state)

      assert Enum.any?(drain(), &match?({:delete, _ip, "img-rocky"}, &1))
    end

    test "a chunk that fails mid-stream rolls back" do
      UploadStubs.install(%{send_chunk: {:error, "closed"}})

      state = writer()
      assert {:error, {:transport, "closed"}, _state} = UploadWriter.write_chunk("abc", state)

      assert Enum.any?(drain(), &match?({:delete, _ip, "img-rocky"}, &1))
    end

    test "a short write is not registered" do
      UploadStubs.install(%{finish: {:ok, 2}})

      state = writer("rocky.iso", 3)
      {:ok, state} = UploadWriter.write_chunk("abc", state)

      assert {:error, {:truncated, message}} = UploadWriter.close(state, :done)
      assert message =~ "2 of 3 bytes"
      assert Enum.any?(drain(), &match?({:delete, _ip, "img-rocky"}, &1))
      # And nothing was sealed: a truncated image must not become permanently immutable.
      refute Enum.any?(drain(), &match?({:seal, _, _}, &1))
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
      assert Enum.any?(drain(), &match?({:delete, _ip, "img-rocky"}, &1))
    end

    test "cancelling before any chunk touches nothing" do
      UploadStubs.install()

      {:ok, _state} = UploadWriter.close(writer(), :cancel)
      assert drain() == []
    end

    test "the connection is closed before the storage is deleted" do
      # The daemon keeps the vdisk attached for the life of the write request, so a
      # delete issued first is refused and the vdisk leaks.
      UploadStubs.install()

      state = writer("rocky.iso", 100)
      {:ok, state} = UploadWriter.write_chunk("abc", state)
      _ = drain()
      {:ok, _state} = UploadWriter.close(state, :cancel)

      calls = drain()
      closed_at = Enum.find_index(calls, &(&1 == :closed))
      deleted_at = Enum.find_index(calls, &match?({:delete, _, _}, &1))

      assert closed_at, "the transport was never closed"
      assert deleted_at, "the storage was never deleted"
      assert closed_at < deleted_at, "the delete was issued while the request was still open"
    end

    test "the vdisk is detached before it is deleted" do
      UploadStubs.install()

      state = writer("rocky.iso", 100)
      {:ok, state} = UploadWriter.write_chunk("abc", state)
      _ = drain()
      {:ok, _state} = UploadWriter.close(state, :cancel)

      calls = drain()
      detached_at = Enum.find_index(calls, &match?({:detach, _, _}, &1))
      deleted_at = Enum.find_index(calls, &match?({:delete, _, _}, &1))

      assert detached_at < deleted_at
    end

    test "a rollback that is refused is retried rather than abandoned" do
      # The vdisk is released a moment after the request ends, so the first delete can
      # legitimately fail. Giving up there leaks storage on every replica.
      UploadStubs.install(%{delete: {:error, "vdisk is attached"}})

      state = writer("rocky.iso", 100)
      {:ok, state} = UploadWriter.write_chunk("abc", state)
      {:ok, _state} = UploadWriter.close(state, :cancel)

      deletes = Enum.count(drain(), &match?({:delete, _, _}, &1))
      assert deletes > 1, "a refused delete was not retried"
    end

    test "a rollback runs once, not once per close" do
      UploadStubs.install(%{send_chunk: {:error, "closed"}})

      state = writer()
      {:error, _reason, state} = UploadWriter.write_chunk("abc", state)
      _ = drain()

      # close/2 after a failure must not delete again: by then the vdisk name may have
      # been reused by another upload, and deleting it would take out live storage.
      {:ok, _state} = UploadWriter.close(state, {:error, :whatever})
      refute Enum.any?(drain(), &match?({:delete, _, _}, &1))
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
        {:static, [%{"name" => "rocky.iso", "path" => @socket}]}
      )

      assert {:error, {:exists, "rocky.iso"}} = Images.prepare_upload("rocky.iso", 10)
      # Nothing was allocated, so there is nothing to roll back either.
      refute Enum.any?(drain(), &match?({:create, _, _, _}, &1))
    end

    test "the vdisk is created at the declared byte count, not a rounded one" do
      # A vdisk is sparse and its map is keyed by extent index, so there is no volume
      # alignment to round up to -- the DRBD path rounded to whole KiB and then to
      # DRBD's own 4 KiB, which made an idempotent retry look like a size conflict.
      assert {:ok, _allocation} = Images.prepare_upload("rocky.iso", 1025)
      assert Enum.any?(drain(), &match?({:create, _ip, "img-rocky", 1025}, &1))
    end

    test "refuses a size it cannot create a vdisk for" do
      assert {:error, {:upload, _message}} = Images.prepare_upload("rocky.iso", 0)
    end

    test "reports a size conflict as its own thing, not a generic failure" do
      UploadStubs.install(%{create: {:error, {409, "already exists at 4194304 bytes"}}})

      assert {:error, {:size_conflict, message}} = Images.prepare_upload("rocky.iso", 10)
      assert message =~ "4194304"
    end

    test "the socket comes from the daemon, not from a path this tier assembles" do
      assert {:ok, allocation} = Images.prepare_upload("rocky.iso", 10)
      assert allocation.socket == @socket
      assert allocation.vdisk == "img-rocky"
    end
  end

  describe "describe_upload_error/1" do
    test "names the subsystem that refused" do
      assert Images.describe_upload_error({:exists, "a.iso"}) =~ "already in the catalogue"
      assert Images.describe_upload_error({:allocate, "no space"}) =~ "allocate storage"
      assert Images.describe_upload_error({:claim, "hci-02 owns it"}) == "hci-02 owns it"
      assert Images.describe_upload_error({:seal, "not drained"}) == "not drained"
      assert Images.describe_upload_error({:write, "HTTP 500"}) =~ "refused the image write"
      assert Images.describe_upload_error({:transport, "closed"}) =~ "streamed to the host"

      # The one an operator has to act on by hand: bytes on disk, no row pointing at them.
      assert Images.describe_upload_error({:catalogue, "timeout"}) =~ "no catalogue row"
    end
  end
end
