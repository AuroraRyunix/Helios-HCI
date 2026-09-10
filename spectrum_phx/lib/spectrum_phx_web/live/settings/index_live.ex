defmodule SpectrumPhxWeb.Settings.IndexLive do
  @moduledoc """
  Cluster settings.

  The page's job is to keep three kinds of setting visibly apart, because they behave
  differently and a form that renders them identically invites an operator to expect the
  same thing from each:

    * **stored** -- a row, and nothing else happens;
    * **derived** -- read from `cluster.json` or from the database, shown and not
      editable, because a field that silently disagrees with the cluster is worse than no
      field;
    * **consequential** -- writing it does something to the cluster. `urbosa_enabled` is
      shown with its state and not offered: turning it on bootstraps namespaces, bridges
      and VXLAN interfaces on every host, and that belongs behind a task that can report
      progress rather than a checkbox that returns 200.

  The replication panel is deliberately explicit that it is describing *metadata*
  replication. The keyspace factor says nothing about how many copies of a guest's disk
  exist -- that is per-vdisk -- and conflating them is how an operator concludes their
  data is safe because this number is three.
  """
  use SpectrumPhxWeb, :live_view

  import SpectrumPhxWeb.Cluster.Components, only: [panel: 1, figure: 1]

  alias SpectrumPhx.Settings

  @refresh_interval_ms 60_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket), do: :timer.send_interval(@refresh_interval_ms, self(), :refresh)

    {:ok, socket |> assign(page_title: "Settings") |> load()}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load(socket)}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, load(socket)}

  def handle_event("save", params, socket) do
    case Settings.update(Map.drop(params, ~w(_csrf_token _target))) do
      {:ok, 0} ->
        {:noreply, put_flash(socket, :info, "Nothing changed.")}

      {:ok, count} ->
        {:noreply,
         socket
         |> put_flash(:info, "#{count} setting#{if count == 1, do: "", else: "s"} saved.")
         |> load()}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event("delete_user", %{"username" => username}, socket) do
    case Settings.delete_user(username) do
      {:ok, _} -> {:noreply, socket |> put_flash(:info, "#{username} removed.") |> load()}
      {:error, message} -> {:noreply, put_flash(socket, :error, message)}
    end
  end

  defp load(socket) do
    socket
    |> assign(:settings, Settings.all())
    |> assign(:read_at, DateTime.utc_now())
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app socket={@socket} flash={@flash} current_username={@current_username} active={:settings}>
      <.header>
        Settings
        <:subtitle>
          <span class="text-xs opacity-60">
            read {Calendar.strftime(@read_at, "%H:%M:%S")} UTC
          </span>
        </:subtitle>
        <:actions>
          <.button phx-click="refresh" id="refresh-button">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </.button>
        </:actions>
      </.header>

      <div :if={not @settings.available?} class="alert alert-warning alert-soft" id="settings-error">
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <span class="text-sm">
          {@settings.error} The values below are the defaults, not what this cluster is using.
        </span>
      </div>

      <div class="flex flex-col gap-4">
        <.panel id="cluster-facts" title="Cluster" subtitle="From cluster.json, not from a setting">
          <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <.figure id="fact-name" label="Name" value={@settings.cluster.name} caption="cluster identity" />
            <.figure id="fact-vip" label="VIP" value={@settings.cluster.vip} caption="console address" tone={:primary} />
            <.figure id="fact-nodes" label="Nodes" value={@settings.cluster.nodes} caption="configured hosts" />
            <.figure
              id="fact-ftt"
              label="Fault tolerance"
              value={@settings.cluster.redundancy_factor}
              caption="failures survivable"
              tone={if (@settings.cluster.redundancy_factor || 0) < 1, do: :warn, else: :good}
            />
          </div>
        </.panel>

        <.panel
          id="replication"
          title="Metadata replication"
          subtitle="How many copies of the cluster's own records exist"
        >
          <div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
            <.figure
              id="rf-actual"
              label="Keyspace factor"
              value={@settings.replication.factor}
              caption="what the database is doing"
              tone={replication_tone(@settings.replication)}
            />
            <.figure
              id="rf-implied"
              label="Implied by ftt"
              value={@settings.replication.implied}
              caption="what the cluster asked for"
            />
            <.figure id="rf-nodes" label="Ceiling" value={@settings.replication.nodes} caption="one copy per node" />
          </div>

          <p class="text-xs opacity-60 mt-3">
            This is the <span class="font-semibold">hydra keyspace</span>: VM records, task
            history, the block map. It says nothing about how many copies of a guest's
            <em>disk</em> exist, which is a property of each vdisk and shown on
            <.link navigate={~p"/storage"} class="link">Storage</.link>.
          </p>
          <p
            :if={mismatched?(@settings.replication)}
            class="text-sm text-warning mt-2"
          >
            The keyspace is replicating {@settings.replication.factor} way(s) but the cluster
            asked for {@settings.replication.implied}. That gap is what a failed or unfinished
            change looks like.
          </p>
        </.panel>

        <form phx-submit="save" class="flex flex-col gap-4">
          <.panel id="dns-settings" title="DNS &amp; networking">
            <div class="grid gap-3 sm:grid-cols-3">
              <.setting_field name="dns_servers" label="Resolvers" value={@settings.stored["dns_servers"]} hint="comma separated" />
              <.setting_field name="dns_search_domains" label="Search domains" value={@settings.stored["dns_search_domains"]} />
              <.setting_field name="dns_mtu" label="MTU" value={@settings.stored["dns_mtu"]} type="number" />
            </div>
          </.panel>

          <.panel id="time-settings" title="NTP &amp; time">
            <div class="grid gap-3 sm:grid-cols-2">
              <.setting_field name="ntp_servers" label="NTP servers" value={@settings.stored["ntp_servers"]} hint="comma separated" />
              <.setting_field name="timezone" label="Timezone" value={@settings.stored["timezone"]} />
            </div>
          </.panel>

          <.panel id="policy-settings" title="Policies">
            <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              <.setting_field name="cluster_region" label="Region" value={@settings.stored["cluster_region"]} />
              <.setting_field name="scrub_interval" label="Scrub interval" value={@settings.stored["scrub_interval"]} hint="daily, weekly, monthly" />
              <.setting_field name="session_timeout" label="Session timeout" value={@settings.stored["session_timeout"]} type="number" hint="minutes" />
              <.setting_field name="rate_limit" label="Rate limit" value={@settings.stored["rate_limit"]} type="number" hint="requests per minute" />
              <.setting_field name="password_policy" label="Password policy" value={@settings.stored["password_policy"]} hint="disabled or strict" />
              <.setting_field name="drs_enabled" label="DRS" value={@settings.stored["drs_enabled"]} hint="true or false" />
            </div>
          </.panel>

          <div class="flex justify-end">
            <button type="submit" class="btn btn-primary btn-sm" id="save-settings">
              <.icon name="hero-check" class="size-4" /> Save settings
            </button>
          </div>
        </form>

        <div class="grid gap-4 lg:grid-cols-2">
          <.panel id="overlay-setting" title="Overlay networking" subtitle="Shown, not editable here">
            <div class="flex items-center gap-3">
              <span class={[
                "badge",
                @settings.read_only["urbosa_enabled"] == "true" && "badge-success",
                @settings.read_only["urbosa_enabled"] != "true" && "badge-ghost"
              ]}>
                Urbosa {if @settings.read_only["urbosa_enabled"] == "true", do: "enabled", else: "disabled"}
              </span>
              <.link navigate={~p"/sdn"} class="btn btn-ghost btn-xs">
                SDN <.icon name="hero-arrow-right" class="size-3" />
              </.link>
            </div>
            <p class="text-xs opacity-60 mt-3">
              Turning this on builds network namespaces, bridges and VXLAN interfaces on
              every host; turning it off tears them down, and doing that under a running
              Lanayru cluster takes its network away. It is a cluster-wide, host-mutating
              operation and it belongs behind a task that can report progress and fail
              visibly, not behind a checkbox that returns success and leaves the work
              happening somewhere. Not offered here yet.
            </p>
          </.panel>

          <.panel id="users" title="Operator accounts">
            <p :if={@settings.users == []} class="text-sm opacity-55 italic">
              No accounts could be read.
            </p>
            <ul :if={@settings.users != []} class="flex flex-col gap-1">
              <li
                :for={user <- @settings.users}
                class="flex items-center justify-between gap-2 text-sm"
                id={"user-#{user}"}
              >
                <span class="font-mono">{user}</span>
                <button
                  phx-click="delete_user"
                  phx-value-username={user}
                  data-confirm={"Remove #{user}?"}
                  class="btn btn-ghost btn-xs text-error"
                  disabled={length(@settings.users) <= 1}
                >
                  Remove
                </button>
              </li>
            </ul>
            <p :if={length(@settings.users) <= 1} class="text-xs opacity-55 mt-2">
              The last account cannot be removed: a console nobody can sign in to is not
              secured, it is bricked.
            </p>
          </.panel>
        </div>
      </div>
    </Layouts.app>
    """
  end

  attr :name, :string, required: true
  attr :label, :string, required: true
  attr :value, :any, default: nil
  attr :type, :string, default: "text"
  attr :hint, :string, default: nil

  defp setting_field(assigns) do
    ~H"""
    <label class="form-control">
      <span class="label-text text-xs opacity-70">{@label}</span>
      <input
        type={@type}
        name={@name}
        value={@value}
        class="input input-bordered input-sm w-full"
        autocomplete="off"
        id={"setting-#{@name}"}
      />
      <span :if={@hint} class="text-[0.65rem] opacity-45 mt-0.5">{@hint}</span>
    </label>
    """
  end

  defp mismatched?(%{factor: factor, implied: implied})
       when is_integer(factor) and is_integer(implied),
       do: factor != implied

  defp mismatched?(_replication), do: false

  defp replication_tone(replication) do
    cond do
      is_nil(replication.factor) -> :neutral
      mismatched?(replication) -> :warn
      replication.factor > 1 -> :good
      true -> :neutral
    end
  end
end
