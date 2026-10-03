defmodule SpectrumPhxWeb.Policies.IndexLive do
  @moduledoc """
  The cluster's policies, in one place: snapshot policies, protection domains, container
  policies and the console's security policy.

  See `SpectrumPhx.Policies` for why these four and not the six fields the settings page
  used to call "Policies". The page is read-only on purpose and says where each kind is
  edited; a form here would be a second writer of tables whose first writer has rules this
  page does not know.

  A section whose table could not be read says so. It never renders as "no policies",
  because on this page that sentence means "nothing is being protected".
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [panel: 1]

  alias SpectrumPhx.Policies

  @refresh_interval_ms 30_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket), do: :timer.send_interval(@refresh_interval_ms, self(), :refresh)
    {:ok, socket |> assign(page_title: "Policies") |> load()}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load(socket)}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, load(socket)}

  defp load(socket) do
    socket |> assign(:policies, Policies.overview()) |> assign(:read_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
    # nil when the table could not be read, which is not the same as []: the template shows
    # "unreadable" for one and "none set" for the other.
    assigns =
      assign(assigns,
        snapshot_rows: rows_of(assigns.policies.snapshot),
        domain_rows: rows_of(assigns.policies.domains),
        container_rows: rows_of(assigns.policies.containers)
      )

    ~H"""
    <Layouts.app
      socket={@socket}
      flash={@flash}
      current_username={@current_username}
      active={:policies}
    >
      <.header>
        Policies
        <:subtitle>
          <span class="text-xs opacity-60">
            Rules the cluster applies to things later. read {Calendar.strftime(@read_at, "%H:%M:%S")} UTC
          </span>
        </:subtitle>
        <:actions>
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </.button>
        </:actions>
      </.header>

      <div class="grid gap-4 xl:grid-cols-2">
        <.panel
          id="snapshot-policies"
          title="Snapshot policies"
          subtitle="Automatic snapshots by scope. Narrowest scope wins; a disabled narrow row is an exemption."
        >
          <.unreadable :if={match?({:error, _}, @policies.snapshot)} section={@policies.snapshot} />

          <div :if={@snapshot_rows}>
            <div :if={@snapshot_rows != []} class="overflow-x-auto">
              <table class="table table-sm" id="snapshot-policies-table">
                <thead>
                  <tr>
                    <th>Scope</th>
                    <th>Applies to</th>
                    <th>Takes one</th>
                    <th>Keeps</th>
                    <th>State</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={row <- @snapshot_rows} id={"snapshot-policy-#{row.scope}-#{slug(row.target)}"}>
                    <td class="capitalize">{row.scope}</td>
                    <td class="font-mono text-xs">{target_label(row)}</td>
                    <td>{every(row.every_seconds)}</td>
                    <td>{keep(row.keep)}</td>
                    <td>
                      <span :if={row.enabled?} class="badge badge-sm badge-success">enabled</span>
                      <span :if={row.exemption?} class="badge badge-sm badge-warning">exempt</span>
                      <span :if={not row.enabled? and not row.exemption?} class="badge badge-sm badge-ghost">
                        disabled
                      </span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
            <p :if={@snapshot_rows == []} class="text-sm opacity-70" id="no-snapshot-policies">
              No snapshot policy is set, so nothing is snapshotted automatically.
            </p>
          </div>

          <p class="text-xs opacity-55 mt-3">
            Set with <code class="font-mono">valcli storage.snapshot-policy.set cluster|container:&lt;name&gt;|vdisk:&lt;id&gt; --every-hours N --keep N</code>.
            Retention only prunes snapshots a policy took, never ones taken by hand.
          </p>
        </.panel>

        <.panel
          id="protection-domains"
          title="Protection domains"
          subtitle="VMs and vdisks snapshotted together, as one crash-consistent set"
        >
          <.unreadable :if={match?({:error, _}, @policies.domains)} section={@policies.domains} />

          <div :if={@domain_rows}>
            <div :if={@domain_rows != []} class="overflow-x-auto">
              <table class="table table-sm" id="domains-table">
                <thead>
                  <tr>
                    <th>Domain</th>
                    <th>Members</th>
                    <th>Takes a set</th>
                    <th>Keeps</th>
                    <th>Consistency</th>
                    <th>Latest set</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={row <- @domain_rows} id={"domain-#{slug(row.name)}"}>
                    <td class="font-medium">
                      {row.name}
                      <span :if={not row.enabled?} class="badge badge-sm badge-ghost ml-1">disabled</span>
                    </td>
                    <td class="text-xs">{row.vms} VM(s), {row.vdisks} vdisk(s)</td>
                    <td>{every(row.every_seconds)}</td>
                    <td>{keep(row.keep)}</td>
                    <td class="text-xs">
                      <span class="font-mono">{row.quiesce}</span>
                      <span :if={row.max_pause_seconds} class="opacity-60">
                        &middot; pause at most {row.max_pause_seconds}s
                      </span>
                    </td>
                    <td class="text-xs">{latest(row.latest)}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <p :if={@domain_rows == []} class="text-sm opacity-70" id="no-domains">
              No protection domain exists. VMs are only snapshotted disk by disk, so a
              restore of a VM with several disks is several unrelated moments.
            </p>
          </div>

          <p class="text-xs opacity-55 mt-3">
            Crash-consistent, not application-consistent: nothing quiesces a database inside
            the guest. Managed with <code class="font-mono">valcli storage.domain*</code>.
          </p>
        </.panel>

        <.panel
          id="container-policies"
          title="Storage container policies"
          subtitle="Every vdisk inherits these from the container it is in"
        >
          <:actions>
            <.link navigate={~p"/storage"} class="btn btn-ghost btn-xs">
              Edit on Storage <.icon name="hero-arrow-right" class="size-3" />
            </.link>
          </:actions>

          <.unreadable :if={match?({:error, _}, @policies.containers)} section={@policies.containers} />

          <div :if={@container_rows}>
            <div :if={@container_rows != []} class="overflow-x-auto">
              <table class="table table-sm" id="container-policies-table">
                <thead>
                  <tr>
                    <th>Container</th>
                    <th>Tier</th>
                    <th>Quota</th>
                    <th>Fault tolerance</th>
                    <th>Compression</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={row <- @container_rows} id={"container-policy-#{slug(row.name)}"}>
                    <td class="font-medium">{row.name}</td>
                    <td>{row.tier}</td>
                    <td>{quota(row.quota_bytes)}</td>
                    <td>{row.ftt}</td>
                    <td>
                      <span class={[
                        "badge badge-sm",
                        if(row.compression == "none", do: "badge-ghost", else: "badge-info")
                      ]}>
                        {row.compression}
                      </span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
            <p :if={@container_rows == []} class="text-sm opacity-70" id="no-containers">
              No containers are defined.
            </p>
          </div>
        </.panel>

        <.panel
          id="security-policy"
          title="Security policy"
          subtitle="How operators sign in. Edited on Settings."
        >
          <:actions>
            <.link navigate={~p"/settings"} class="btn btn-ghost btn-xs">
              Edit on Settings <.icon name="hero-arrow-right" class="size-3" />
            </.link>
          </:actions>

          <dl class="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
            <dt class="opacity-60">Password complexity</dt>
            <dd id="policy-password">{password_label(@policies.security.password_policy)}</dd>
            <dt class="opacity-60">Session timeout</dt>
            <dd id="policy-session">{@policies.security.session_timeout} minutes</dd>
            <dt class="opacity-60">Auth requests per minute</dt>
            <dd id="policy-rate">{@policies.security.rate_limit}</dd>
          </dl>
        </.panel>
      </div>
    </Layouts.app>
    """
  end

  attr :section, :any, required: true

  defp unreadable(assigns) do
    {:error, message} = assigns.section
    assigns = assign(assigns, :message, message)

    ~H"""
    <div class="alert alert-warning alert-soft items-start" id="policies-unreadable">
      <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
      <span class="text-sm">
        This table {@message}. That is not an empty result: nothing is known about what it holds.
      </span>
    </div>
    """
  end

  defp rows_of({:ok, rows}), do: rows
  defp rows_of(_error), do: nil

  # -- formatting -----------------------------------------------------------------------

  defp target_label(%{scope: "cluster"}), do: "every vdisk"
  defp target_label(%{target: target}), do: target

  defp every(nil), do: "-"

  defp every(seconds) when is_integer(seconds) do
    cond do
      seconds >= 86_400 and rem(seconds, 86_400) == 0 -> "every #{div(seconds, 86_400)} d"
      seconds >= 3600 and rem(seconds, 3600) == 0 -> "every #{div(seconds, 3600)} h"
      seconds >= 60 and rem(seconds, 60) == 0 -> "every #{div(seconds, 60)} min"
      true -> "every #{seconds} s"
    end
  end

  defp keep(nil), do: "-"
  defp keep(count), do: "last #{count}"

  defp latest(nil), do: "none yet"

  defp latest(%{state: state, consistency: consistency, taken_at_ms: ms}) do
    when_taken =
      case is_integer(ms) && DateTime.from_unix(ms, :millisecond) do
        {:ok, datetime} -> Calendar.strftime(datetime, "%Y-%m-%d %H:%M") <> " UTC"
        _ -> "unknown time"
      end

    "#{state} (#{consistency}), #{when_taken}"
  end

  defp quota(0), do: "Unlimited"
  defp quota(nil), do: "Unlimited"

  defp quota(bytes) when is_integer(bytes),
    do: :erlang.float_to_binary(bytes / 1_073_741_824, decimals: 1) <> " GB"

  defp password_label("enabled"), do: "Strong: 8+ characters, upper-case, digit, symbol"
  defp password_label(_other), do: "Basic: 5+ characters"

  defp slug(value),
    do: value |> to_string() |> String.downcase() |> String.replace(~r/[^a-z0-9]+/, "-") |> String.trim("-")
end
