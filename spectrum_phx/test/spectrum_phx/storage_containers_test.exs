defmodule SpectrumPhx.Storage.ContainersTest do
  # Not async: both the container source and the vdisk source are configured through
  # application env, which is global.
  use ExUnit.Case, async: false

  alias SpectrumPhx.Storage.Containers

  @default %{
    "name" => "default-pool",
    "tier" => "SSD",
    "quota_bytes" => 0,
    "path" => "default-pool",
    "ftt" => 0,
    # Written before compression existed, so the column is null rather than "none".
    "compression" => nil
  }

  @packed %{
    "name" => "packed",
    "tier" => "NVME",
    "quota_bytes" => 107_374_182_400,
    "path" => "packed",
    "ftt" => 1,
    "compression" => "lz4"
  }

  defp put_containers(rows),
    do: Application.put_env(:spectrum_phx, :containers_source, {:static, rows})

  defp put_vdisks(rows),
    do: Application.put_env(:spectrum_phx, :containers_vdisk_source, {:static, rows})

  setup do
    put_containers([@default, @packed])
    put_vdisks([])

    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :containers_source)
      Application.delete_env(:spectrum_phx, :containers_vdisk_source)
    end)

    :ok
  end

  # -- reading ---------------------------------------------------------------------

  describe "list/0" do
    test "a null compression column reads as 'none' rather than nil" do
      {:ok, [default, _packed]} = Containers.list()
      assert default.name == "default-pool"

      assert default.compression == "none",
             "a container written before the column existed must not read as nil, or every " <>
               "caller decides for itself what a null means"
    end

    test "an explicit setting is preserved" do
      {:ok, [_default, packed]} = Containers.list()
      assert packed.compression == "lz4"
      assert packed.tier == "NVME"
      assert packed.quota_bytes == 107_374_182_400
    end

    test "containers come back in a stable order" do
      put_containers([@packed, @default])
      {:ok, rows} = Containers.list()
      assert Enum.map(rows, & &1.name) == ["default-pool", "packed"]
    end
  end

  describe "get/1" do
    test "finds one by name" do
      assert {:ok, %{name: "packed", compression: "lz4"}} = Containers.get("packed")
    end

    test "a name that is not there is not_found, not an empty container" do
      assert {:error, :not_found} = Containers.get("nope")
    end

    test "a name that could not be safely bound is refused before it is looked up" do
      assert {:error, :invalid_name} = Containers.get("a'; DROP TABLE hydra.vms; --")
    end
  end

  # -- validation ------------------------------------------------------------------

  describe "valid_name?/1" do
    test "accepts the names an operator would actually choose" do
      for name <- ~w(default-pool templates iso_store a tier1.fast A1) do
        assert Containers.valid_name?(name), name
      end
    end

    test "refuses anything that could reach CQL as something other than a name" do
      for name <- [
            "a'; DROP TABLE hydra.vms; --",
            "has space",
            "-leading",
            ".leading",
            "_leading",
            "",
            nil,
            7,
            String.duplicate("x", 64),
            "semi;colon"
          ] do
        refute Containers.valid_name?(name), inspect(name)
      end
    end
  end

  describe "normalise_compression/1" do
    test "resolves every shape a form actually sends" do
      for {input, expected} <- [
            {true, "lz4"},
            {false, "none"},
            {nil, "none"},
            {"lz4", "lz4"},
            {"none", "none"},
            {"on", "lz4"},
            {"off", "none"},
            {"true", "lz4"},
            {"false", "none"},
            {"yes", "lz4"},
            {"no", "none"},
            {"LZ4", "lz4"},
            {"  lz4  ", "lz4"},
            {"", "none"}
          ] do
        assert Containers.normalise_compression(input) == {:ok, expected},
               "#{inspect(input)} did not resolve to #{expected}"
      end
    end

    test "refuses anything else instead of quietly meaning 'off'" do
      # The operator believes compression is on and nothing ever says otherwise.
      for input <- ["zstd", "gzip", "1", "enabled", 7, [], %{}] do
        assert Containers.normalise_compression(input) == :error, inspect(input)
      end
    end

    test "every resolved value is one the storage daemon will recognise" do
      for input <- [true, false, nil, "on", "off", "lz4", "none"] do
        {:ok, mode} = Containers.normalise_compression(input)
        assert mode in Containers.compression_modes()
      end
    end
  end

  # -- creating --------------------------------------------------------------------

  describe "create/1" do
    test "writes the policy it was given" do
      assert {:ok, container} =
               Containers.create(%{
                 "name" => "fresh",
                 "tier" => "hdd",
                 "quota_bytes" => 1024,
                 "ftt" => 2,
                 "compression" => "on"
               })

      assert container.name == "fresh"
      assert container.tier == "HDD", "a tier must be stored in the case the daemon expects"
      assert container.compression == "lz4"
      assert container.ftt == 2
      assert container.quota_bytes == 1024
    end

    test "compression defaults to off when nothing says otherwise" do
      assert {:ok, %{compression: "none"}} = Containers.create(%{"name" => "quiet"})
    end

    test "a name already in use is refused rather than overwritten" do
      assert {:error, message} = Containers.create(%{"name" => "packed"})
      assert message =~ "already exists"
    end

    test "an unusable name never reaches a statement" do
      assert {:error, message} = Containers.create(%{"name" => "not a name"})
      assert message =~ "Invalid container name"
    end

    test "an unknown tier is refused" do
      assert {:error, message} = Containers.create(%{"name" => "x", "tier" => "optane"})
      assert message =~ "Storage tier must be one of"
    end

    test "an unknown codec is refused" do
      assert {:error, message} = Containers.create(%{"name" => "x", "compression" => "zstd"})
      assert message =~ "Compression must be one of"
    end

    test "a negative quota is refused" do
      assert {:error, message} = Containers.create(%{"name" => "x", "quota_bytes" => -1})
      assert message =~ "cannot be negative"
    end
  end

  # -- updating --------------------------------------------------------------------

  describe "update/2" do
    test "changes only what it was given" do
      # The specific defect this guards: a form that edits the quota silently resetting
      # compression to its own default.
      assert {:ok, :updated} = Containers.update("packed", %{"quota_bytes" => 42})
    end

    test "an empty change is refused rather than issuing an empty statement" do
      assert {:error, "Nothing to change."} = Containers.update("packed", %{})
    end

    test "a container that does not exist cannot be updated" do
      assert {:error, :not_found} = Containers.update("nope", %{"ftt" => 1})
    end

    test "a bad value is refused before anything is written" do
      assert {:error, message} = Containers.update("packed", %{"compression" => "zstd"})
      assert message =~ "Compression must be one of"
    end
  end

  # -- deleting --------------------------------------------------------------------

  describe "delete/1" do
    test "an empty container is deleted" do
      assert {:ok, :deleted} = Containers.delete("packed")
    end

    test "a container with vdisks in it is refused, and says which" do
      put_vdisks([
        %{"vdisk_id" => "vm-a-disk0", "container" => "packed"},
        %{"vdisk_id" => "vm-b-disk0", "container" => "packed"},
        %{"vdisk_id" => "elsewhere", "container" => "default-pool"}
      ])

      assert {:error, {:in_use, users}} = Containers.delete("packed")
      assert users == ["vm-a-disk0", "vm-b-disk0"]

      refute "elsewhere" in users,
             "a vdisk in another container must not block this one"
    end

    test "a vdisk with no container recorded belongs to default" do
      # Sidon assumes `default` when the column is absent; the two have to agree, or a
      # delete of `default` succeeds while vdisks are still using it.
      put_vdisks([%{"vdisk_id" => "orphan", "container" => nil}])
      assert {:error, {:in_use, ["orphan"]}} = Containers.delete("default")
    end

    test "an unusable name never reaches a statement" do
      assert {:error, message} = Containers.delete("../../etc")
      assert message =~ "Invalid container name"
    end
  end
end
