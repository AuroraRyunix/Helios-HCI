defmodule SpectrumPhxWeb.Images.IndexLiveTest do
  # Not async: the catalogue source and the backing remover are configured through
  # application env, which is global.
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  # Mounted through the route, so each test exercises the real path an operator
  # takes: the plug pipeline, the authentication hook, and the layout around the view.

  defp mount_view(conn), do: live(log_in(conn), "/images")

  @images [
    %{
      "name" => "ubuntu-24.04.iso",
      "filename" => "ubuntu-24.04.iso",
      "size_bytes" => 6_303_121_408,
      "type" => "iso",
      "path" => "/var/lib/hci/sidon/nbd/img-ubuntu-24-04.sock",
      "created_at" => 1_755_000_000_000
    },
    %{
      "name" => "seed.img",
      "filename" => "seed.img",
      "size_bytes" => 1_048_576,
      "type" => "template",
      "path" => "/var/lib/hci/aether/volumes/default-image-container/seed.img",
      "created_at" => 1_754_000_000_000
    }
  ]

  defp put_source(rows), do: Application.put_env(:spectrum_phx, :images_source, {:static, rows})

  defp put_remover(fun) do
    Application.put_env(:spectrum_phx, :images_backing_remover, fun)
  end

  setup do
    put_source(@images)

    put_remover(fn _image -> {:ok, :removed} end)

    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :images_source)
      Application.delete_env(:spectrum_phx, :images_backing_remover)
    end)

    :ok
  end

  describe "listing" do
    test "renders each image with the columns the table actually has", %{conn: conn} do
      {:ok, _view, html} = mount_view(conn)

      assert html =~ "ubuntu-24.04.iso"
      assert html =~ "seed.img"
      assert html =~ "iso"
      assert html =~ "template"
      # 6_303_121_408 bytes, formatted rather than shown raw.
      assert html =~ "5.9 GiB"
      assert html =~ "1.0 MiB"
      assert html =~ "registered 2025-08"
    end

    test "marks a vdisk-backed image as replicated and a file-backed one not", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#image-ubuntu-24-04-iso") |> render() =~ "replicated"
      refute view |> element("#image-seed-img") |> render() =~ "replicated"
    end

    test "shows the path each row points at", %{conn: conn} do
      {:ok, _view, html} = mount_view(conn)
      assert html =~ "/var/lib/hci/sidon/nbd/img-ubuntu-24-04.sock"
    end

    test "an empty catalogue says Hydra answered, not merely that there is nothing",
         %{conn: conn} do
      put_source([])

      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#images-empty") |> render() =~ "Hydra answered"
    end

    test "an undated row says the date is unknown rather than inventing one",
         %{conn: conn} do
      put_source([%{"name" => "mystery.iso", "created_at" => nil}])

      {:ok, _view, html} = mount_view(conn)
      assert html =~ "registered date unknown"
    end
  end

  describe "upload" do
    setup do
      SpectrumPhx.UploadStubs.install()
      :ok
    end

    test "there is a file input and a disabled submit until a file is chosen", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#upload-form input[type=file]") |> has_element?()
      assert view |> element("#upload-button[disabled]") |> has_element?()
    end

    test "a chosen file is listed, with nothing uploaded yet", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      upload = file_input(view, "#upload-form", :image, [entry("rocky.iso", 4096)])
      assert render_upload(upload, "rocky.iso", 0) =~ "rocky.iso"

      # auto_upload is false, so selecting a file must not have allocated anything. If it
      # had, browsing away from this page would leak a vdisk per file picked.
      assert view |> element("#upload-button") |> has_element?()
      refute view |> element("#upload-button[disabled]") |> has_element?()
    end

    test "a file with the wrong extension is rejected client-side", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      upload = file_input(view, "#upload-form", :image, [entry("notes.txt", 10)])
      render_upload(upload, "notes.txt", 0)

      assert render(view) =~ "Only .iso, .qcow2 and .img files can be uploaded."
    end

    test "an entry can be cancelled before it is sent", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      upload = file_input(view, "#upload-form", :image, [entry("rocky.iso", 4096)])
      render_upload(upload, "rocky.iso", 0)

      [entry] = upload.entries
      view |> element("#cancel-upload-" <> entry["ref"]) |> render_click()

      refute render(view) =~ "rocky.iso"
      assert view |> element("#upload-button[disabled]") |> has_element?()
    end

    test "submitting with no file says so rather than failing silently", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      html = view |> element("#upload-form") |> render_submit()
      assert html =~ "Choose an image file first."
    end

    test "a submitted image is streamed, registered, and confirmed on screen", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      upload = file_input(view, "#upload-form", :image, [entry("rocky.iso", 2048)])
      render_upload(upload, "rocky.iso", 100)

      html = view |> element("#upload-form") |> render_submit()

      assert html =~ "Uploaded rocky.iso"
      # The bytes reached the transport rather than any disk of this tier's.
      assert_receive {:upload_stub, {:finish, bytes}}
      assert byte_size(bytes) == 2048
    end

    test "a failure is reported with the subsystem that refused, and rolls back", %{conn: conn} do
      # The peer still holds Primary, so the promotion does not take.
      SpectrumPhx.UploadStubs.install(%{attach: {:error, {409, "hci-02 owns img-rocky"}}})

      {:ok, view, _html} = mount_view(conn)

      upload = file_input(view, "#upload-form", :image, [entry("rocky.iso", 64)])

      # The chunk fails inside the writer; how LiveViewTest surfaces that is not the point.
      # What matters is that the storage allocated a moment earlier was given back.
      try do
        render_upload(upload, "rocky.iso", 100)
      catch
        :exit, _reason -> :ok
      end

      assert_receive {:upload_stub, {:delete, _ip, "img-rocky"}}
      refute_receive {:upload_stub, {:finish, _bytes}}
    end

    test "the page explains that nothing is staged in the web tier", %{conn: conn} do
      {:ok, _view, html} = mount_view(conn)

      # The note is on the page, not in a comment: an operator needs to know the bytes do
      # not pass through this container's disk.
      assert html =~ "Nothing is staged here"
    end
  end

  defp entry(name, size) do
    %{
      name: name,
      content: :binary.copy("0", size),
      size: size,
      type: MIME.from_path(name)
    }
  end

  describe "delete confirmation" do
    test "the first click confirms and deletes nothing", %{conn: conn} do
      test_pid = self()

      put_remover(fn image ->
        send(test_pid, {:removed, image.name})
        {:ok, :removed}
      end)

      {:ok, view, _html} = mount_view(conn)

      html = view |> element("#delete-ubuntu-24-04-iso") |> render_click()

      refute_received {:removed, _name}
      assert html =~ "Delete"
      assert html =~ "cannot be undone"
      assert view |> element("#confirm-ubuntu-24-04-iso") |> has_element?()
    end

    test "the confirmation names what will actually be removed", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      view |> element("#delete-ubuntu-24-04-iso") |> render_click()
      panel = view |> element("#confirm-ubuntu-24-04-iso") |> render()

      assert panel =~ "img-ubuntu-24-04"
      assert panel =~ "every replica"
    end

    test "a file-backed image says the file is removed on every node", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      view |> element("#delete-seed-img") |> render_click()

      assert view |> element("#confirm-seed-img") |> render() =~ "file on every node"
    end

    test "cancelling leaves the image alone", %{conn: conn} do
      test_pid = self()

      put_remover(fn image ->
        send(test_pid, {:removed, image.name})
        {:ok, :removed}
      end)

      {:ok, view, _html} = mount_view(conn)

      view |> element("#delete-ubuntu-24-04-iso") |> render_click()
      view |> element("#cancel-delete-ubuntu-24-04-iso") |> render_click()

      refute_received {:removed, _name}
      refute view |> element("#confirm-ubuntu-24-04-iso") |> has_element?()
    end

    test "the second click deletes", %{conn: conn} do
      test_pid = self()

      put_remover(fn image ->
        send(test_pid, {:removed, image.name})
        {:ok, :removed}
      end)

      {:ok, view, _html} = mount_view(conn)

      view |> element("#delete-ubuntu-24-04-iso") |> render_click()
      html = view |> element("#confirm-delete-ubuntu-24-04-iso") |> render_click()

      assert_received {:removed, "ubuntu-24.04.iso"}
      assert html =~ "Deleted ubuntu-24.04.iso"
    end

    test "the delete event refuses an image that is not in the catalogue", %{conn: conn} do
      # Driving the event past the UI: the guard that matters is the one in the context.
      test_pid = self()

      put_remover(fn image ->
        send(test_pid, {:removed, image.name})
        {:ok, :removed}
      end)

      {:ok, view, _html} = mount_view(conn)

      html = render_click(view, "confirm_delete", %{"name" => "not-an-image.iso"})

      refute_received {:removed, _name}
      assert html =~ "not in the image catalogue"
    end
  end

  describe "delete failures are surfaced" do
    test "a refused backing removal is reported and the row stays", %{conn: conn} do
      put_remover(fn _image ->
        {:error, {:backing, "Resource img-ubuntu-24-04 is in use by node hci-02"}}
      end)

      {:ok, view, _html} = mount_view(conn)

      view |> element("#delete-ubuntu-24-04-iso") |> render_click()
      html = view |> element("#confirm-delete-ubuntu-24-04-iso") |> render_click()

      assert html =~ "in use by node hci-02"
      assert html =~ "catalogue entry was kept"
      # The row is still there: this is the case the old endpoint answered 200 to.
      assert view |> element("#image-ubuntu-24-04-iso") |> has_element?()
      assert view |> element("#delete-error-ubuntu-24-04-iso") |> has_element?()
    end

    test "a row pointing somewhere unsafe is refused with the path named", %{conn: conn} do
      Application.delete_env(:spectrum_phx, :images_backing_remover)
      put_source([%{"name" => "planted.iso", "path" => "/etc/shadow"}])

      {:ok, view, _html} = mount_view(conn)

      view |> element("#delete-planted-iso") |> render_click()
      html = view |> element("#confirm-delete-planted-iso") |> render_click()

      assert html =~ "/etc/shadow"
      assert html =~ "Refusing to delete"
      assert view |> element("#image-planted-iso") |> has_element?()
    end

    test "a delete that removed the storage but not the row says the row is stale",
         %{conn: conn} do
      put_remover(fn _image -> {:error, {:catalogue, :not_connected}} end)

      {:ok, view, _html} = mount_view(conn)

      view |> element("#delete-seed-img") |> render_click()
      html = view |> element("#confirm-delete-seed-img") |> render_click()

      assert html =~ "cleaned up by hand"
    end
  end

  describe "unavailable catalogue" do
    test "an unreachable Hydra is stated rather than rendered as an empty catalogue",
         %{conn: conn} do
      # `:hydra` with no ScyllaDB reachable is exactly the dev-machine case.
      Application.delete_env(:spectrum_phx, :images_source)

      {:ok, view, _html} = mount_view(conn)

      assert view |> element("#images-db-error") |> has_element?()

      assert view |> element("#images-db-error") |> render() =~
               "statement that the catalogue is empty"

      refute view |> element("#images-empty") |> has_element?()
    end
  end

  describe "live updates" do
    test "a delete broadcast elsewhere refreshes this page", %{conn: conn} do
      {:ok, view, html} = mount_view(conn)
      assert html =~ "seed.img"

      put_source(Enum.reject(@images, &(&1["name"] == "seed.img")))
      Phoenix.PubSub.broadcast(SpectrumPhx.PubSub, "images", {:image_deleted, "seed.img"})

      refute render(view) =~ "seed.img"
    end

    test "the server-side interval re-reads the catalogue", %{conn: conn} do
      {:ok, view, html} = mount_view(conn)
      refute html =~ "debian-13.iso"

      # An upload completing through the Python endpoint writes the row from another
      # process; there is nothing here to subscribe to, so the list has to be re-read.
      put_source([%{"name" => "debian-13.iso", "type" => "iso"} | @images])
      send(view.pid, :refresh)

      assert render(view) =~ "debian-13.iso"
    end

    test "the refresh button re-reads the catalogue", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      put_source([%{"name" => "alpine.iso", "type" => "iso"}])

      assert view |> element("#refresh-button") |> render_click() =~ "alpine.iso"
    end
  end
end
