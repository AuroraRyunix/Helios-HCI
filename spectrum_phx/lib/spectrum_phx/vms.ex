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
  alias SpectrumPhx.Vms.Vm

  @columns "name, vcpu, memory, disk_path, disk_size, state, host_ip, disks_list, firmware, iso, boot_device, network_id, cpu_model, audio_enabled, status"

  @list_cql "SELECT #{@columns} FROM hydra.vms"
  @get_cql "SELECT #{@columns} FROM hydra.vms WHERE name = ?"

  # The compare-and-swap that makes a start safe: the update only lands if `host_ip` is
  # still what the caller believed it was when it decided to start the VM.
  @claim_host_cql "UPDATE hydra.vms SET host_ip = ?, state = ? WHERE name = ? IF host_ip = ?"

  # `IF status != ?` rather than a read-then-write check. A null `status` (every VM
  # created before the lock column was used) satisfies `!=`, so an unlocked VM is claimable
  # and a locked one is not, with no window between the test and the write.
  @set_migration_lock_cql "UPDATE hydra.vms SET status = ? WHERE name = ? IF status != ?"

  # Only the holder may release it: conditional on the lock actually being held, so a
  # late-arriving cleanup from a failed attempt cannot unlock someone else's migration.
  @clear_migration_lock_cql "UPDATE hydra.vms SET status = ? WHERE name = ? IF status = ?"

  # `IF NOT EXISTS` so a duplicate create cannot silently overwrite a live VM's row --
  # `INSERT` in CQL is an upsert, and the Python create path relies on that.
  @insert_cql "INSERT INTO hydra.vms (#{@columns}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"

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
  Register a new VM's metadata.

  Validates with `SpectrumPhx.Vms.Vm.new/1` and inserts `IF NOT EXISTS`, so a create that
  races another create for the same name loses rather than overwriting a live VM's row
  (CQL `INSERT` is an upsert; without the condition the second create silently replaces
  the first VM's disk paths).

  TODO: storage allocation is not done here. `/api/vms/create` also runs, per disk,
  `linstor resource-definition create`, `volume-definition create <n>GiB`, `resource
  create <host> --storage-pool default-pool`, and the split-brain `drbd-options` -- and
  rolls the earlier ones back if a later one fails. That sequence stays in the Python tier
  behind Spark for now, so a VM created through this function has a metadata row and no
  backing DRBD resources until storage is provisioned.

  Under a `{:static, _}` source the parameters are validated and the built struct is
  returned without being written anywhere; see `source/0`.
  """
  @spec create_vm(map()) :: {:ok, Vm.t()} | {:error, keyword() | :already_exists | term()}
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
          {:ok, vm}

        :hydra ->
          case Hydra.apply_lwt(@insert_cql, insert_params) do
            {:ok, true} ->
              broadcast({:vm_created, vm.name})
              {:ok, vm}

            {:ok, false} ->
              {:error, :already_exists}

            {:error, reason} ->
              {:error, reason}
          end
      end
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
