defmodule SpectrumPhxWeb.Vms.Components do
  @moduledoc """
  Presentation pieces shared by the VM LiveViews.

  The important one is `state_badge/1` and `lock_badge/1` staying separate: `state` is the
  power state and `status` is the migration lock, and showing them in one control is how
  an operator ends up believing a VM is idle while a migration is in flight.
  """
  use SpectrumPhxWeb, :html

  alias SpectrumPhx.Vms.Vm

  @doc "Power state, as a badge."
  attr :vm, Vm, required: true

  def state_badge(assigns) do
    ~H"""
    <span class={[
      "inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium ring-1 ring-inset",
      if(Vm.running?(@vm),
        do: "bg-emerald-500/10 text-emerald-600 ring-emerald-500/30",
        else: "bg-zinc-500/10 text-zinc-500 ring-zinc-500/30"
      )
    ]}>
      <span class={[
        "size-1.5 rounded-full",
        if(Vm.running?(@vm), do: "bg-emerald-500", else: "bg-zinc-400")
      ]} />
      {@vm.state}
    </span>
    """
  end

  @doc """
  The transient lifecycle lock, shown only while it is held.

  A held lock means a migration is in flight and no other lifecycle operation on this VM
  may start.
  """
  attr :vm, Vm, required: true

  def lock_badge(assigns) do
    ~H"""
    <span
      :if={Vm.migrating?(@vm)}
      class="inline-flex items-center gap-1.5 rounded-full bg-amber-500/10 px-2.5 py-0.5 text-xs font-medium text-amber-600 ring-1 ring-inset ring-amber-500/30"
      title="Lifecycle lock held: a migration is in flight"
    >
      <.icon name="hero-lock-closed-micro" class="size-3" /> {@vm.status}
    </span>
    """
  end

  @doc "Where the VM is placed, resolved to a hostname when the IP is a known node."
  attr :vm, Vm, required: true

  def placement(assigns) do
    ~H"""
    <span :if={Vm.placed?(@vm)} class="font-mono text-sm">
      {SpectrumPhx.Cluster.Config.hostname_for(@vm.host_ip)}
    </span>
    <span :if={not Vm.placed?(@vm)} class="text-sm text-zinc-500">Unassigned</span>
    """
  end

  @doc """
  Turn a context error into something an operator can act on.

  Every one of these corresponds to a refusal that has a real consequence, so they say
  what was refused rather than "an error occurred".
  """
  # Storage allocation reports its own detail; without this clause the fallback rendered
  # a disk failure as "the cluster database rejected the request", which points an
  # operator at entirely the wrong subsystem.
  def error_message({:storage, detail}) when is_binary(detail),
    do: "Disk allocation failed, so the VM was not created: " <> detail

  def error_message({:storage, detail}),
    do: "Disk allocation failed, so the VM was not created: " <> inspect(detail)

  def error_message(:invalid_name), do: "Invalid VM name: " <> Vm.name_error() <> "."

  def error_message(:not_found), do: "That VM is not in the cluster database."

  def error_message(:already_claimed),
    do:
      "Another host already owns this VM. Its placement changed while you were looking at it; " <>
        "starting it here would put two hypervisors on the same disk."

  def error_message(:already_running), do: "That VM is already running."

  def error_message(:not_running), do: "That VM is not running."

  def error_message(:migrating),
    do: "That VM is migrating. Lifecycle operations are locked until the migration settles."

  def error_message(:already_migrating), do: "That VM is already migrating."

  def error_message(:not_locked), do: "That VM does not hold the migration lock."

  def error_message(:already_exists), do: "A VM with that name already exists."

  def error_message(errors) when is_list(errors) do
    Enum.map_join(errors, "; ", fn {field, message} -> "#{field} #{message}" end)
  end

  def error_message(reason), do: "The cluster database rejected the request: #{inspect(reason)}"
end
