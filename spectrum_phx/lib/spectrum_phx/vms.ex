defmodule SpectrumPhx.Vms do
  @moduledoc """
  VM management: reads from `hydra.vms`, ownership transitions, and lifecycle work
  submission.

  Three things about this module are deliberate and are the reason it exists in Elixir
  rather than staying in `spectrum_server.py`:

  ### 1. Every statement is parameterised

  Reads and writes go through `SpectrumPhx.Hydra` with bound values. Nothing here builds
  CQL by interpolation, so there is no per-call-site escaping to forget.

  ### 2. Host claims are compare-and-swap, not blind writes

  The Python tier ran `UPDATE hydra.vms SET host_ip = '<ip>' WHERE name = '<name>'` with
  no condition. Two callers acting on the same VM -- a manual start racing DRS, or a start
  issued while a stale `host_ip` was still in the row -- would both "succeed", and two
  qemu processes would open the same raw DRBD device. `claim_host/4` uses an LWT so
  exactly one caller wins and the loser is told it lost.

  Vali has a second, independent guard for this (it promotes each DRBD resource to
  Primary as a checked step and refuses to boot if the peer still holds Primary). The LWT
  here is the *first* gate: it stops the second start from ever being submitted, and it
  works even for the paths that do not go through DRBD promotion.

  ### 3. The migration lock is conditional

  `set_migration_lock/1` only applies if the lock is not already held, so a concurrent
  migration of the same VM is rejected by the database rather than by a read-then-write
  check that two callers can both pass.

  ## Not implemented here (on purpose)

  DRBD promotion, libvirt XML generation, NVRAM handling, host selection and the
  dual-primary window around live migration all stay in `vali.py`. They are reached by
  submitting a task to Catalyst, exactly as the Python tier does; see `submit_task/3`.

  ## Test seam

  `list_vms/0` and `get_vm/1` read through `source/0`, and `submit_task/3` dispatches
  through `task_submitter/0`. Both default to the real infrastructure and can be pointed
  at in-memory data via application env, which is how the LiveView tests mount against
  fixture VMs with no cluster present.
  """

  require Logger

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Hydra
  alias SpectrumPhx.Spark
  alias SpectrumPhx.Vms.Vm

  @columns "name, vcpu, memory, disk_path, disk_size, state, host_ip, disks_list, firmware, iso, boot_device, network_id, cpu_model, audio_enabled, status"

  @list_cql "SELECT #{@columns} FROM hydra.vms"
  @get_cql "SELECT #{@columns} FROM hydra.vms WHERE name = ?"

  # The compare-and-swap that makes a start safe: the update only lands if `host_ip` is
  # still what the caller believed it was when it decided to start the VM.
  @claim_host_cql "UPDATE hydra.vms SET host_ip = ?, state = ? WHERE name = ? IF host_ip = ?"

  # `IF status != ?` rather than a read-then-write check: the condition and the write are
  # one Paxos round, so there is no window between the test and the act.
  #
  # This relies on a null `status` satisfying `!=`. That matters because `status` *is* null
  # for every VM `/api/vms/create` ever registered -- it inserts JSON without the column --
  # so the common case is the null case. Cassandra handles EQ and NEQ against a null row
  # value explicitly (only the ordering operators are an error there) and Scylla documents
  # LWT conditions as Cassandra-compatible. Verify it against a live Scylla before relying
  # on it: if it turned out that a null never satisfies `!=`, the lock would refuse every
  # first migration rather than allow a concurrent one -- loud, not silent, but wrong.
  @set_migration_lock_cql "UPDATE hydra.vms SET status = ? WHERE name = ? IF status != ?"

  # Only the holder may release it: conditional on the lock actually being held, so a
  # late-arriving cleanup from a failed attempt cannot unlock someone else's migration.
  @clear_migration_lock_cql "UPDATE hydra.vms SET status = ? WHERE name = ? IF status = ?"

  # `IF NOT EXISTS` so a duplicate create cannot silently overwrite a live VM's row --
  # `INSERT` in CQL is an upsert, and the Python create path relies on that.
  @insert_cql "INSERT INTO hydra.vms (#{@columns}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"

  # Undo for a create whose storage allocation failed. Conditional on the row still being
  # the one this call inserted: an unconditional `DELETE` would also remove a VM that
  # something else claimed and started in the meantime, which is a live VM's row.
  @delete_cql "DELETE FROM hydra.vms WHERE name = ? IF state = ? AND host_ip = ?"

  @migration_lock "migrating"
  @unlocked_status "running"

  @catalyst_port 9091
  @catalyst_service "vali"

  @pubsub SpectrumPhx.PubSub
  @topic "vms"

  # -- statements ------------------------------------------------------------------
  # Exposed so tests can assert on the exact CQL without a cluster. The point of these
  # statements is their shape (bound values, LWT conditions); that is worth pinning.

  @doc "CQL used by `list_vms/0`."
  def list_vms_cql, do: @list_cql

  @doc "CQL used by `get_vm/1`."
  def get_vm_cql, do: @get_cql

  @doc "CQL used by `claim_host/4`. The `IF host_ip = ?` clause is the whole point."
  def claim_host_cql, do: @claim_host_cql

  @doc "CQL used by `set_migration_lock/1`."
  def set_migration_lock_cql, do: @set_migration_lock_cql

  @doc "CQL used by `clear_migration_lock/1`."
  def clear_migration_lock_cql, do: @clear_migration_lock_cql

  @doc "CQL used by `create_vm/1`."
  def insert_cql, do: @insert_cql

  @doc "CQL used by `create_vm/1` to undo its row when storage allocation fails."
  def delete_cql, do: @delete_cql

  @doc "The value written into the `status` column while a migration is in flight."
  def migration_lock, do: @migration_lock

  # -- reads -----------------------------------------------------------------------

  @doc """
  Every VM known to the cluster.

  Returns `{:ok, [%Vm{}]}` or `{:error, reason}`. Note this is metadata only: the row's
  `state` is what Vali last wrote, which can lag libvirt if a guest was stopped from
  inside. Reconciling the two is `vali.py`'s job, not the web tier's.
  """
  @spec list_vms() :: {:ok, [Vm.t()]} | {:error, term()}
  def list_vms do
    case source() do
      {:static, vms} ->
        {:ok, Enum.map(vms, &Vm.from_row/1)}

      :hydra ->
        with {:ok, rows} <- Hydra.query(@list_cql, []) do
          {:ok, rows |> Enum.map(&Vm.from_row/1) |> Enum.sort_by(& &1.name)}
        end
    end
  end

  @doc """
  One VM by name.

  The name is validated before it is used, even though it is a bound parameter here: a
  name that could not have been created is not worth a database round trip, and the value
  is echoed back into the UI and into later shell-bound calls.
  """
  @spec get_vm(String.t()) :: {:ok, Vm.t()} | {:error, :not_found | :invalid_name | term()}
  def get_vm(name) do
    with {:ok, name} <- validate_name(name) do
      case source() do
        {:static, vms} ->
          vms
          |> Enum.map(&Vm.from_row/1)
          |> Enum.find(&(&1.name == name))
          |> case do
            nil -> {:error, :not_found}
            vm -> {:ok, vm}
          end

        :hydra ->
          case Hydra.query(@get_cql, [{"text", name}]) do
            {:ok, [row | _]} -> {:ok, Vm.from_row(row)}
            {:ok, []} -> {:error, :not_found}
            {:error, reason} -> {:error, reason}
          end
      end
    end
  end

  # -- ownership -------------------------------------------------------------------

  @doc """
  Compare-and-swap this VM's host placement.

      claim_host("web-01", "10.0.0.12", "")

  Takes the VM from `expected_host_ip` to `host_ip` and sets `state`, but only if the row
  still holds `expected_host_ip`. Returns `{:ok, :claimed}` when this caller won the race
  and `{:error, :already_claimed}` when another caller got there first (or the row never
  held the expected value at all).

  `expected_host_ip` is normally `""` -- an unplaced VM -- because that is the only state
  from which starting a VM is safe. Pass the current host when moving a VM that is already
  placed. `nil` matches a row whose `host_ip` was never written.

  ## Why this is an LWT

  Cassandra/Scylla `UPDATE` is unconditional and last-write-wins. Without the `IF` clause
  two nodes can both write their own IP into `host_ip` a millisecond apart, both believe
  they own the VM, and both start qemu against the same DRBD device. The `IF` makes the
  read-and-write a single Paxos round: one applies, the other comes back `[applied] =
  false`.
  """
  @spec claim_host(String.t(), String.t(), String.t() | nil, keyword()) ::
          {:ok, :claimed} | {:error, :already_claimed | :invalid_name | term()}
  def claim_host(name, host_ip, expected_host_ip \\ "", opts \\ []) do
    state = Keyword.get(opts, :state, "Running")

    with {:ok, name} <- validate_name(name) do
      params = [
        {"text", host_ip},
        {"text", state},
        {"text", name},
        {"text", expected_host_ip}
      ]

      case Hydra.apply_lwt(@claim_host_cql, params) do
        {:ok, true} ->
          broadcast({:vm_updated, name})
          {:ok, :claimed}

        {:ok, false} ->
          {:error, :already_claimed}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Take the migration lock on a VM.

  Sets `status = "migrating"`, conditional on the lock not already being held. Returns
  `{:ok, :locked}` or `{:error, :already_migrating}`.

  This is the guard `vali.py` tries to implement with a blind `UPDATE ... SET status =
  'migrating'` followed by a separate read of `status` -- which is a check-then-act, and
  two callers can both pass the check. Here the condition and the write are one operation.

  A failed lock must abort the migration, not be logged and stepped over: without it a
  second migration of the same VM proceeds concurrently, and live migration is exactly the
  window in which DRBD dual-primary is enabled.
  """
  @spec set_migration_lock(String.t()) ::
          {:ok, :locked} | {:error, :already_migrating | :invalid_name | term()}
  def set_migration_lock(name) do
    with {:ok, name} <- validate_name(name) do
      params = [
        {"text", @migration_lock},
        {"text", name},
        {"text", @migration_lock}
      ]

      case Hydra.apply_lwt(@set_migration_lock_cql, params) do
        {:ok, true} ->
          broadcast({:vm_updated, name})
          {:ok, :locked}

        {:ok, false} ->
          {:error, :already_migrating}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Release the migration lock.

  Conditional on the lock being held, so this cannot clobber a status written by whoever
  currently owns the VM. Returns `{:ok, :unlocked}` or `{:error, :not_locked}`.
  """
  @spec clear_migration_lock(String.t()) ::
          {:ok, :unlocked} | {:error, :not_locked | :invalid_name | term()}
  def clear_migration_lock(name) do
    with {:ok, name} <- validate_name(name) do
      params = [
        {"text", @unlocked_status},
        {"text", name},
        {"text", @migration_lock}
      ]

      case Hydra.apply_lwt(@clear_migration_lock_cql, params) do
        {:ok, true} ->
          broadcast({:vm_updated, name})
          {:ok, :unlocked}

        {:ok, false} ->
          {:error, :not_locked}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  # -- lifecycle -------------------------------------------------------------------

  @doc """
  Register a new VM and allocate its disks.

  Validates with `SpectrumPhx.Vms.Vm.new/1`, inserts `IF NOT EXISTS`, then allocates one
  DRBD-backed Linstor resource per disk through `SpectrumPhx.Spark.linstor_resource_create/4`.
  A VM that comes back `{:ok, vm}` has both a row and its storage.

  ## The row is written before the storage, on purpose

  Disk resources are named after the VM (`<name>-disk<n>`), so the name is the shared
  resource two concurrent creates would collide on. The `IF NOT EXISTS` insert is the only
  thing in this system that makes that collision resolvable: it is one Paxos round, so
  exactly one caller proceeds to allocate.

  Reversing the order breaks the rollback rather than the happy path. `resource-definition
  create` is idempotent by adoption, so the caller that loses the row insert cannot tell
  its own resources from the winner's -- and its rollback would delete the winner's live
  disks. Ordering it row-first also makes the surviving failure mode the recoverable one:
  a row with no storage is deleted below and shows up in the operator's error, whereas
  storage with no row is an orphan nothing ever looks at again.

  ## Rollback

  If any disk fails, the resources this call *created* are deleted and the row is removed
  with an LWT conditional on it still being the row this call inserted (a VM that was
  claimed and started in the meantime is left alone). The caller gets
  `{:error, {:storage, message}}` naming the disk that failed.

  Resources that were *adopted* rather than created -- Spark answering `"created" =>
  false` -- are deliberately not deleted: they existed before this call and may hold
  another VM's data. They are logged instead, because for a VM the database has never seen
  an existing resource is a leftover from an incomplete delete.

  A resource left behind by a failed rollback is logged as an orphan rather than retried
  into a loop; that is a repair an operator does, and the alternative is deleting storage
  this code is not sure it owns.

  Under a `{:static, _}` source there is no row to write: the parameters are validated and
  the built struct is returned. Allocation runs there only when `storage_client/0` is
  stubbed, which is how the tests drive it -- with no stub, nothing is called at all and a
  test can never reach a hypervisor. See `source/0`.
  """
  @spec create_vm(map()) ::
          {:ok, Vm.t()}
          | {:error, keyword() | :already_exists | {:storage, String.t()} | term()}
  def create_vm(params) do
    with {:ok, %Vm{} = vm} <- Vm.new(params) do
      disk_paths = vm |> Vm.disks() |> Enum.map(& &1.path)

      vm = %{
        vm
        | disk_path: List.first(disk_paths) || "",
          disks_list: if(vm.disks_list == "", do: "NONE", else: vm.disks_list)
      }

      insert_params = [
        {"text", vm.name},
        {"int", vm.vcpu},
        {"int", vm.memory},
        {"text", vm.disk_path},
        {"int", vm.disk_size},
        {"text", vm.state},
        {"text", vm.host_ip},
        {"text", vm.disks_list},
        {"text", vm.firmware},
        {"text", vm.iso},
        {"text", vm.boot_device},
        {"text", vm.network_id},
        {"text", vm.cpu_model},
        {"boolean", vm.audio_enabled},
        {"text", vm.status}
      ]

      case source() do
        {:static, _vms} ->
          allocate_without_a_row(vm)

        :hydra ->
          case Hydra.apply_lwt(@insert_cql, insert_params) do
            {:ok, true} ->
              allocate_or_undo(vm)

            {:ok, false} ->
              {:error, :already_exists}

            {:error, reason} ->
              {:error, reason}
          end
      end
    end
  end

  # Under a static source there is no row to write and no row to undo, so a create is
  # validation plus -- when `storage_client/0` is stubbed -- allocation. That is the half
  # of this path a test can drive: the allocation order, the adopt-versus-create
  # distinction, and the rollback of a partially allocated VM. With no stub configured
  # nothing is called at all, so a test can never reach a real hypervisor.
  defp allocate_without_a_row(%Vm{} = vm) do
    case storage_client() do
      nil ->
        {:ok, vm}

      _stub ->
        case allocate_storage(vm) do
          :ok -> {:ok, vm}
          {:error, message} -> {:error, {:storage, message}}
        end
    end
  end

  # The row is ours -- the LWT said so -- which is what makes both the allocation and the
  # undo below safe to run without a second opinion.
  defp allocate_or_undo(%Vm{} = vm) do
    case allocate_storage(vm) do
      :ok ->
        broadcast({:vm_created, vm.name})
        {:ok, vm}

      {:error, message} ->
        Logger.error("Storage allocation for VM #{vm.name} failed: #{message}")
        undo_row(vm)
        {:error, {:storage, message}}
    end
  end

  defp allocate_storage(%Vm{} = vm) do
    vm
    |> Vm.disks()
    |> Enum.reduce_while({:ok, []}, fn disk, {:ok, created} ->
      case allocate_disk(disk) do
        # Adopted, not created: it was already there, so this call does not own it and
        # must not delete it during a rollback.
        {:ok, %{"created" => false}} ->
          Logger.warning(
            "VM #{vm.name}: adopted the existing Linstor resource #{disk.resource} rather " <>
              "than creating it. The database has never seen this VM, so that resource is " <>
              "a leftover from an earlier VM of the same name."
          )

          {:cont, {:ok, created}}

        {:ok, _response} ->
          {:cont, {:ok, [disk.resource | created]}}

        {:error, reason} ->
          # Newest first, which is also the order to unwind in.
          release_disks(created)
          {:halt, {:error, describe_disk_failure(disk, reason)}}
      end
    end)
    |> case do
      {:ok, _created} -> :ok
      {:error, message} -> {:error, message}
    end
  end

  # `nodes` is deliberately not sent: Spark reads the same cluster document this node
  # does, and it is the component that owns the host inventory. Deriving a node list here
  # would duplicate that knowledge and get it wrong exactly when a hostname is missing
  # from the web tier's copy of the file.
  defp allocate_disk(%{size_gib: nil, index: index}) do
    {:error, "disk #{index} has no usable size"}
  end

  defp allocate_disk(%{resource: resource, size_gib: size_gib}) do
    case storage_client() do
      nil ->
        # {:gib, _} and never :allow_two_primaries: a VM disk is read-write, and
        # dual-primary on one is what let a VM run on two hosts and corrupt itself.
        on_a_spark_node(fn ip ->
          Spark.linstor_resource_create(ip, resource, {:gib, size_gib})
        end)

      fun when is_function(fun, 3) ->
        fun.(:create, resource, %{size_gib: size_gib})
    end
  end

  defp release_disks(resources) do
    Enum.each(resources, fn resource ->
      case release_disk(resource) do
        {:ok, _response} ->
          :ok

        {:error, reason} ->
          Logger.error(
            "Rolling back VM creation: Linstor resource #{resource} could not be deleted " <>
              "(#{inspect(reason)}). It is now an orphan and has to be removed by hand."
          )
      end
    end)
  end

  defp release_disk(resource) do
    case storage_client() do
      nil ->
        on_a_spark_node(fn ip -> Spark.linstor_resource_delete(ip, resource) end)

      fun when is_function(fun, 3) ->
        fun.(:delete, resource, %{})
    end
  end

  # Linstor is a cluster-wide service: the client in any node's aether container reaches
  # whichever node currently holds the controller. A node this call could not *reach* is
  # therefore retried on the next node, while an answer from a daemon -- including a
  # refusal -- is final and stops the walk. The endpoints being idempotent is what makes
  # the retry safe: a create that timed out after the daemon acted is adopted, not
  # duplicated.
  defp on_a_spark_node(fun) do
    Enum.reduce_while(spark_ips(), {:error, :no_spark_nodes}, fn ip, _last ->
      case fun.(ip) do
        {:ok, response} -> {:halt, {:ok, response}}
        {:error, {status, _message}} = answer when is_integer(status) -> {:halt, answer}
        {:error, {:http, _status}} = answer -> {:halt, answer}
        {:error, _transport} = failure -> {:cont, failure}
      end
    end)
  end

  defp spark_ips do
    [Config.local_ip() | Config.node_ips()]
    |> Enum.reject(&(&1 in [nil, ""]))
    |> Enum.uniq()
  end

  defp describe_disk_failure(disk, reason) do
    "disk #{disk.index} (#{disk.resource}): " <> failure_text(reason)
  end

  defp failure_text(message) when is_binary(message), do: message

  defp failure_text({status, message}) when is_integer(status) and is_binary(message) do
    "spark returned #{status}: #{message}"
  end

  defp failure_text(other), do: inspect(other)

  defp undo_row(%Vm{} = vm) do
    params = [{"text", vm.name}, {"text", vm.state}, {"text", vm.host_ip}]

    case Hydra.apply_lwt(@delete_cql, params) do
      {:ok, true} ->
        :ok

      {:ok, false} ->
        Logger.error(
          "VM #{vm.name}: storage allocation failed, but the row changed underneath the " <>
            "rollback (something claimed the VM) and was left in place."
        )

      {:error, reason} ->
        Logger.error(
          "VM #{vm.name}: storage allocation failed and the row could not be removed " <>
            "(#{inspect(reason)}). It now points at storage that does not exist."
        )
    end
  end

  @doc """
  Ask Vali to start a VM.

  Options:

    * `:host_ip` - start on this specific host. The host is claimed with `claim_host/4`
      *before* the task is submitted, so a VM that is already placed elsewhere is refused
      here rather than being handed to a hypervisor that will try to promote a DRBD
      resource its peer still holds.

  With no `:host_ip`, placement is left to Vali's `select_best_start_host`, which filters
  out hosts in maintenance and hosts with a service down, then picks the one with the
  lowest used memory. That choice happens inside Vali, so the claim happens there too --
  this path relies on Vali's DRBD promotion check as the ownership gate, not on the LWT.
  """
  @spec power_on(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def power_on(name, opts \\ []) do
    with {:ok, name} <- validate_name(name),
         {:ok, vm} <- get_vm(name),
         :ok <- refuse_if_migrating(vm),
         :ok <- refuse_if_running(vm),
         {:ok, target} <- claim_requested_host(vm, Keyword.get(opts, :host_ip)) do
      submit_task("start", name, target)
    end
  end

  @doc """
  Ask Vali to stop a VM.

  Vali runs `virsh destroy` then `undefine --keep-nvram`, backs the NVRAM up to Hydra, and
  clears `host_ip` -- releasing the placement so the VM can be started elsewhere. It is
  that release, not this call, that makes the next `claim_host/4` succeed.
  """
  @spec power_off(String.t()) :: {:ok, map()} | {:error, term()}
  def power_off(name) do
    with {:ok, name} <- validate_name(name),
         {:ok, vm} <- get_vm(name),
         :ok <- refuse_if_migrating(vm) do
      submit_task("stop", name, nil)
    end
  end

  @doc """
  Ask Vali to reboot a VM in place.

  `reboot` is an ACPI request to the guest and keeps the current placement; the VM must
  already be running for it to mean anything.
  """
  @spec reboot(String.t()) :: {:ok, map()} | {:error, term()}
  def reboot(name) do
    with {:ok, name} <- validate_name(name),
         {:ok, vm} <- get_vm(name),
         :ok <- refuse_if_migrating(vm),
         :ok <- require_running(vm) do
      submit_task("reboot", name, nil)
    end
  end

  @doc """
  Submit one unit of work to Catalyst's `vali` queue and return without waiting.

  Catalyst dispatches from an *in-memory* queue on the ZooKeeper leader
  (`GET /api/v1/queues/vali` is what Vali's worker long-polls); the `hydra.catalyst_tasks`
  row it also writes is a record, not the queue. Writing that row directly would therefore
  never dispatch anything, which is why this posts to the leader's API rather than going
  through Hydra.

  The action names are Vali's: `start`, `stop`, `poweroff`, `reboot`, `shutdown`, `reset`,
  `migrate`.
  """
  @spec submit_task(String.t(), String.t(), String.t() | nil) :: {:ok, map()} | {:error, term()}
  def submit_task(action, vm_name, target_host \\ nil) do
    payload =
      %{"vm_name" => vm_name}
      |> maybe_put("target_host", target_host)

    case task_submitter() do
      nil ->
        post_task(@catalyst_service, action, payload)

      fun when is_function(fun, 3) ->
        fun.(@catalyst_service, action, payload)
    end
    |> case do
      {:ok, result} ->
        broadcast({:vm_task_submitted, vm_name, action})
        {:ok, result}

      {:error, reason} ->
        Logger.warning(
          "Vali task #{action} for #{vm_name} could not be submitted: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  # -- pubsub ----------------------------------------------------------------------

  @doc "Subscribe the calling process to VM change notifications."
  def subscribe do
    Phoenix.PubSub.subscribe(@pubsub, @topic)
  end

  defp broadcast(message) do
    Phoenix.PubSub.broadcast(@pubsub, @topic, message)
  rescue
    # PubSub is supervised by the application; in the odd case where it is not running
    # (a bare unit test, say) a missing notification must not fail the operation that
    # already succeeded against the database.
    ArgumentError -> :ok
  catch
    :exit, _ -> :ok
  end

  # -- seams -----------------------------------------------------------------------

  @doc """
  Where VM reads come from. `:hydra` (the default) or `{:static, vms}`.

  `{:static, vms}` serves reads from an in-memory list of `%Vm{}` structs or row maps, and
  makes `create_vm/1` validate without persisting. That is how the LiveView tests mount
  and drive these views with no ScyllaDB present; it is not used in production, and the
  ownership operations (`claim_host/4`, the migration lock) deliberately ignore it -- an
  in-memory stand-in for a Paxos round would be worse than no test at all.
  """
  @spec source() :: :hydra | {:static, list()}
  def source, do: Application.get_env(:spectrum_phx, :vms_source, :hydra)

  @doc """
  Override for `submit_task/3`'s transport: `nil` (post to Catalyst) or a 3-arity function
  receiving `(service, action, payload)`. Tests set the function so a power action can be
  asserted on without a Catalyst daemon.
  """
  @spec task_submitter() ::
          nil | (String.t(), String.t(), map() -> {:ok, map()} | {:error, term()})
  def task_submitter, do: Application.get_env(:spectrum_phx, :vms_task_submitter)

  @doc """
  Override for the storage calls `create_vm/1` makes: `nil` (talk to Spark) or a 3-arity
  function receiving `(action, resource, opts)`.

  `action` is `:create` (with `%{size_gib: n}`) or `:delete` (with `%{}`), and the return
  value is whatever `SpectrumPhx.Spark.linstor_resource_create/4` and
  `linstor_resource_delete/3` return -- `{:ok, %{"created" => bool}}`,
  `{:ok, %{"deleted" => bool}}` or `{:error, reason}`.

  Allocation is the one part of a create that cannot be exercised against a real cluster
  in a test, and it is also the part whose *failure* path matters most, so the seam exists
  to drive the rollback deliberately rather than hoping it is right.
  """
  @spec storage_client() ::
          nil | (atom(), String.t(), map() -> {:ok, map()} | {:error, term()})
  def storage_client, do: Application.get_env(:spectrum_phx, :vms_storage_client)

  # -- internals -------------------------------------------------------------------

  defp validate_name(name) do
    case Vm.validate_name(name) do
      {:ok, name} -> {:ok, name}
      {:error, _message} -> {:error, :invalid_name}
    end
  end

  defp claim_requested_host(_vm, nil), do: {:ok, nil}

  defp claim_requested_host(%Vm{} = vm, host_ip) do
    case claim_host(vm.name, host_ip, vm.host_ip || "") do
      {:ok, :claimed} -> {:ok, host_ip}
      {:error, reason} -> {:error, reason}
    end
  end

  defp refuse_if_migrating(%Vm{} = vm) do
    if Vm.migrating?(vm), do: {:error, :migrating}, else: :ok
  end

  defp refuse_if_running(%Vm{} = vm) do
    if Vm.running?(vm), do: {:error, :already_running}, else: :ok
  end

  defp require_running(%Vm{} = vm) do
    if Vm.running?(vm), do: :ok, else: {:error, :not_running}
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, _key, ""), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp post_task(service, action, payload) do
    url =
      "http://" <>
        catalyst_ip() <> ":" <> Integer.to_string(@catalyst_port) <> "/api/v1/tasks/submit"

    body = %{"service" => service, "action" => action, "payload" => payload}

    case Req.post(url, json: body, receive_timeout: 35_000) do
      {:ok, %Req.Response{status: 200, body: response}} -> {:ok, response}
      {:ok, %Req.Response{status: status, body: response}} -> {:error, {:http, status, response}}
      {:error, reason} -> {:error, reason}
    end
  end

  # Catalyst runs on every node but only the ZooKeeper leader holds the dispatch queue
  # (the queue is an in-process `queue.Queue`, not a table), so tasks must be submitted
  # there and nowhere else.
  #
  # TODO: leader resolution. `vali.py`'s `get_zookeeper_leader_ip/0` probes each node's
  # ZooKeeper four-letter `stat` for "mode: leader", then checks that the leader is
  # actually answering on 9091 and otherwise falls back to the lowest-numbered node that
  # is. `SpectrumPhx.Zk` has no equivalent yet -- `Zk.Client` and `Zk.State` cover the
  # connection and the cluster-state document, not leader election -- and that module is
  # owned elsewhere. This resolves the function at runtime rather than compile time so it
  # wires itself up when the capability lands; until then it submits to the local node,
  # which is correct only when this node happens to be the leader. Configure
  # `:catalyst_ip` to pin it in the meantime.
  defp catalyst_ip do
    case Application.get_env(:spectrum_phx, :catalyst_ip) do
      ip when is_binary(ip) and ip != "" ->
        ip

      _ ->
        [Module.concat([:SpectrumPhx, :Zk]), Module.concat([:SpectrumPhx, :Zk, :State])]
        |> Enum.find_value(fn module ->
          if Code.ensure_loaded?(module) and function_exported?(module, :leader_ip, 0) do
            case apply(module, :leader_ip, []) do
              ip when is_binary(ip) and ip != "" -> ip
              _ -> nil
            end
          end
        end)
        |> Kernel.||(Config.local_ip())
    end
  end
end
