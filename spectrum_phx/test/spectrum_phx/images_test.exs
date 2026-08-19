defmodule SpectrumPhx.ImagesTest do
  # Not async: the catalogue source and the backing remover are configured through
  # application env, which is global.
  use ExUnit.Case, async: false

  alias SpectrumPhx.Images

  @drbd_image %{
    "name" => "ubuntu-24.04.iso",
    "filename" => "ubuntu-24.04.iso",
    "size_bytes" => 6_303_121_408,
    "type" => "iso",
    "path" => "/dev/drbd/by-res/img-ubuntu-24-04/0",
    "created_at" => 1_755_000_000_000
  }

  @file_image %{
    "name" => "seed.img",
    "filename" => "seed.img",
    "size_bytes" => 1_048_576,
    "type" => "template",
    "path" => "/var/lib/hci/aether/volumes/default-image-container/seed.img",
    "created_at" => 1_754_000_000_000
  }

  @undated_image %{
    "name" => "mystery.qcow2",
    "filename" => "mystery.qcow2",
    "size_bytes" => 512,
    "type" => "template",
    "path" => nil,
    "created_at" => nil
  }

  defp put_source(rows), do: Application.put_env(:spectrum_phx, :images_source, {:static, rows})

  defp put_remover(fun) do
    Application.put_env(:spectrum_phx, :images_backing_remover, fun)
  end

  setup do
    put_source([@drbd_image, @file_image, @undated_image])

    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :images_source)
      Application.delete_env(:spectrum_phx, :images_backing_remover)
    end)

    :ok
  end

  describe "statements" do
    test "every statement names its columns and binds its values" do
      # The Python tier ran `SELECT JSON *` and built the WHERE clause by interpolation.
      assert Images.list_images_cql() =~ "SELECT name, filename, size_bytes, type, path"
      assert Images.list_images_cql() =~ "FROM hydra.valhalla_images"
      assert Images.get_image_cql() =~ "WHERE name = ?"
      assert Images.delete_image_cql() == "DELETE FROM hydra.valhalla_images WHERE name = ?"

      refute Images.get_image_cql() =~ "'"
      refute Images.delete_image_cql() =~ "'"
    end
  end

  describe "list_images/0" do
    test "reads the columns the table actually has" do
      {:ok, images} = Images.list_images()

      image = Enum.find(images, &(&1.name == "ubuntu-24.04.iso"))

      assert image.filename == "ubuntu-24.04.iso"
      assert image.type == "iso"
      # The column is `size_bytes`; "Size (GB)" was a label the browser computed.
      assert image.size_bytes == 6_303_121_408
      assert image.path == "/dev/drbd/by-res/img-ubuntu-24-04/0"
      assert image.on_drbd?
      assert %DateTime{} = image.created_at
    end

    test "a file-backed image is not marked as replicated" do
      {:ok, images} = Images.list_images()
      image = Enum.find(images, &(&1.name == "seed.img"))

      refute image.on_drbd?
    end

    test "epoch-millisecond timestamps from the Python tier are decoded" do
      {:ok, images} = Images.list_images()
      image = Enum.find(images, &(&1.name == "ubuntu-24.04.iso"))

      assert DateTime.to_unix(image.created_at, :millisecond) == 1_755_000_000_000
    end

    test "a DateTime straight from Xandra is kept as it is" do
      {:ok, now} = DateTime.now("Etc/UTC")
      put_source([%{"name" => "a.iso", "created_at" => now}])

      {:ok, [image]} = Images.list_images()
      assert DateTime.compare(image.created_at, now) == :eq
    end

    test "newest first, with undated rows last rather than treated as oldest" do
      {:ok, images} = Images.list_images()

      assert Enum.map(images, & &1.name) == ["ubuntu-24.04.iso", "seed.img", "mystery.qcow2"]
    end

    test "a row with no type is 'unknown' rather than blank" do
      put_source([%{"name" => "a.iso"}])

      {:ok, [image]} = Images.list_images()
      assert image.type == "unknown"
      assert image.filename == "a.iso"
      assert image.size_bytes == nil
    end

    test "an empty catalogue is an empty list" do
      put_source([])
      assert {:ok, []} = Images.list_images()
    end
  end

  describe "resource_name/1" do
    test "reproduces the Python slug so a delete finds the resource that exists" do
      assert Images.resource_name("ubuntu-24.04.iso") == "img-ubuntu-24-04"
      assert Images.resource_name("Windows Server 2022.ISO") == "img-windows-server-2022"
      assert Images.resource_name("cloud-init.qcow2") == "img-cloud-init"
      assert Images.resource_name("seed.img") == "img-seed"
    end

    test "collapses runs of separators and trims them, as the Python slug does" do
      assert Images.resource_name("a  b--c.iso") == "img-a-b-c"
      assert Images.resource_name("--edge--.iso") == "img-edge"
    end

    test "truncates to the same 28 characters" do
      name = String.duplicate("x", 60) <> ".iso"
      assert Images.resource_name(name) == "img-" <> String.duplicate("x", 28)
    end

    test "produces a resource name a shell cannot be steered with" do
      slug = Images.resource_name("a; rm -rf / #.iso")

      assert slug == "img-a-rm-rf"
      refute slug =~ ~r/[^a-z0-9_-]/
    end
  end

  describe "delete_resource_command/1" do
    test "names the controllers so the delete reaches a cluster controller" do
      command = Images.delete_resource_command("img-ubuntu-24-04")

      assert command =~ "LS_CONTROLLERS="
      assert command =~ "resource-definition delete img-ubuntu-24-04"
    end

    test "falls back to the loopback rather than emitting an empty variable" do
      # A development host has no cluster.json, so there are no controllers to name.
      assert Images.delete_resource_command("img-a") =~ "LS_CONTROLLERS=127.0.0.1 "
    end
  end

  describe "safe_container_file?/1" do
    test "accepts a file inside the image container directory" do
      assert Images.safe_container_file?(
               "/var/lib/hci/aether/volumes/default-image-container/a.iso"
             )
    end

    test "rejects a path outside it" do
      refute Images.safe_container_file?("/etc/passwd")
      refute Images.safe_container_file?("/dev/drbd/by-res/img-a/0")
      refute Images.safe_container_file?("")
      refute Images.safe_container_file?(nil)
    end

    test "rejects a traversal that would climb back out" do
      refute Images.safe_container_file?("/var/lib/hci/aether/volumes/../../../etc/shadow")
    end

    test "rejects the bare directory itself" do
      refute Images.safe_container_file?("/var/lib/hci/aether/volumes/")
    end
  end

  describe "delete_image/1" do
    test "removes the backing store before the catalogue entry" do
      test_pid = self()

      put_remover(fn image ->
        send(test_pid, {:removed, image.name})
        {:ok, :removed}
      end)

      assert {:ok, %{name: "ubuntu-24.04.iso", backing: :removed}} =
               Images.delete_image("ubuntu-24.04.iso")

      assert_received {:removed, "ubuntu-24.04.iso"}
    end

    test "a failing backing removal is reported and the entry is kept" do
      put_remover(fn _image -> {:error, {:backing, "Resource is still in use"}} end)

      assert {:error, {:backing, message}} = Images.delete_image("ubuntu-24.04.iso")
      assert message =~ "still in use"

      # The catalogue read is unchanged: the image is still there to retry against.
      {:ok, images} = Images.list_images()
      assert Enum.any?(images, &(&1.name == "ubuntu-24.04.iso"))
    end

    test "an unknown image is reported rather than silently succeeding" do
      put_remover(fn _image -> {:ok, :removed} end)
      assert {:error, :not_found} = Images.delete_image("no-such-image.iso")
    end

    test "a name that could not have been catalogued is rejected at the boundary" do
      put_remover(fn _image -> {:ok, :removed} end)

      assert {:error, :invalid_name} = Images.delete_image("")
      assert {:error, :invalid_name} = Images.delete_image("   ")
      assert {:error, :invalid_name} = Images.delete_image("evil\nname.iso")
      assert {:error, :invalid_name} = Images.delete_image(nil)
    end

    test "a row pointing outside the image container is refused, not deleted" do
      # No remover configured, so the real path check runs -- and refuses before it can
      # reach a shell. `rm -f {path}` in the Python tier had no such check.
      put_source([%{"name" => "planted.iso", "path" => "/etc/shadow"}])

      assert {:error, {:unsafe_path, "/etc/shadow"}} = Images.delete_image("planted.iso")
    end

    test "a row with no path reports that nothing was removed" do
      put_source([%{"name" => "orphan.iso", "path" => nil}])

      assert {:ok, %{backing: :skipped}} = Images.delete_image("orphan.iso")
    end

    test "a delete notifies subscribers" do
      put_remover(fn _image -> {:ok, :removed} end)
      Images.subscribe()

      assert {:ok, _result} = Images.delete_image("seed.img")
      assert_receive {:image_deleted, "seed.img"}
    end
  end

  describe "upload_note/0" do
    test "says where uploading still works and why it is not here" do
      note = Images.upload_note()

      assert note =~ "/api/images/upload"
      assert note =~ "/api/v1/storage/device/write"
      assert note =~ "never touches the data path"
    end
  end
end
