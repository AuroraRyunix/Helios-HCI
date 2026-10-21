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
      the one that does, so it is not a field on this form at all: it has its own control,
      which submits a Catalyst task and says which task it submitted.

  ## The overlay switch confirms, and its two directions do not look alike

  Turning the overlay on builds namespaces, bridges and VXLAN interfaces on every host.
  Turning it off removes them from every host, and anything still using them loses its
  network at that moment. Those are not the same act, and one toggle would render them
  identically -- so the control is two-step, and the second step spells out what is about
  to happen, in error colours, when the direction is teardown.

  The confirmation is server-side state, exactly as the image page's delete is.
  `data-confirm` and `window.confirm` live entirely in the browser: they guard nothing
  that does not arrive through the browser, and they cannot be tested.

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

    {:ok,
     socket
     |> assign(
       page_title: "Settings",
       confirming: nil,
       urbosa_error: nil,
       urbosa_task: nil,
       apply_failures: []
     )
     |> load()}
  end

  @impl true
  def handle_info(:refresh, socket), do: {:noreply, load(socket)}
  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_event("refresh", _params, socket), do: {:noreply, load(socket)}

  def handle_event("save", params, socket) do
    case Settings.save(Map.drop(params, ~w(_csrf_token _target))) do
      {:ok, %{saved: 0, applied: [], failed: []}} ->
        {:noreply, put_flash(socket, :info, "Nothing changed.")}

      {:ok, %{saved: saved, applied: applied, failed: failed}} ->
        info = ["#{saved} setting#{if saved == 1, do: "", else: "s"} saved." | applied]

        socket =
          socket
          |> put_flash(:info, Enum.join(info, " "))
          |> assign(:apply_failures, failed)
          |> load()

        # A host that did not take a change is not a reason to hide that the others did, nor
        # a reason to say "saved" and leave it there: it stays on the page past the next
        # refresh, because the hosts and the rows now disagree.
        {:noreply, if(failed == [], do: socket, else: put_flash(socket, :error, Enum.join(failed, " ")))}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event("create_user", %{"username" => username, "password" => password}, socket) do
    case Settings.create_user(username, password) do
      {:ok, name} -> {:noreply, socket |> put_flash(:info, "#{name} created.") |> load()}
      {:error, message} -> {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event(
        "set_password",
        %{"username" => username, "password" => password, "confirm" => confirm},
        socket
      ) do
    cond do
      password != confirm ->
        {:noreply, put_flash(socket, :error, "The two passwords do not match.")}

      true ->
        case Settings.set_user_password(username, password) do
          {:ok, name} -> {:noreply, put_flash(socket, :info, "Password changed for #{name}.")}
          {:error, message} -> {:noreply, put_flash(socket, :error, message)}
        end
    end
  end

  # The first click asks. Nothing is submitted and nothing is written; the panel renders
  # what the second click would do, which for a teardown is the sentence that matters.
  def handle_event("ask_urbosa", %{"value" => value}, socket) do
    direction = if value == "true", do: :bootstrap, else: :teardown
    {:noreply, assign(socket, confirming: direction, urbosa_error: nil, urbosa_task: nil)}
  end

  def handle_event("cancel_urbosa", _params, socket) do
    {:noreply, assign(socket, confirming: nil, urbosa_error: nil)}
  end

  def handle_event("set_urbosa", %{"value" => value}, socket) do
    case Settings.set_urbosa_enabled(value) do
      {:ok, :unchanged} ->
        {:noreply,
         socket
         |> assign(confirming: nil, urbosa_error: nil)
         |> put_flash(:info, "The overlay is already in that state; nothing was submitted.")
         |> load()}

      {:ok, %{direction: direction, task_id: task_id}} ->
        {:noreply,
         socket
         |> assign(confirming: nil, urbosa_error: nil, urbosa_task: {direction, task_id})
         |> put_flash(:info, submitted_message(direction))
         |> load()}

      {:error, message} ->
        # Kept on the panel and not only in a flash. This is the one control whose failure
        # means the row and the hosts could have disagreed, and the sentence saying they
        # do not must still be there after the next refresh.
        {:noreply, assign(socket, confirming: nil, urbosa_error: message)}
    end
  end

  def handle_event("delete_user", %{"username" => username}, socket) do
    case Settings.delete_user(username) do
      {:ok, _} -> {:noreply, socket |> put_flash(:info, "#{username} removed.") |> load()}
      {:error, message} -> {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event("deploy_ssl", %{"certificate" => cert, "private_key" => key}, socket) do
    case Settings.deploy_ssl(cert, key) do
      {:ok, message} ->
        {:noreply, socket |> put_flash(:info, message) |> load()}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event("add_node", %{"hostname" => hostname, "ip" => ip} = params, socket) do
    is_witness? = params["witness"] in ["true", true]

    case Settings.add_node(hostname, ip, witness: is_witness?) do
      {:ok, message} ->
        {:noreply, socket |> put_flash(:info, message) |> load()}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event("remove_node", %{"target" => target}, socket) do
    case Settings.remove_node(target) do
      {:ok, message} ->
        {:noreply, socket |> put_flash(:info, message) |> load()}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event("node_maintenance", %{"node" => node, "action" => action}, socket) do
    case Settings.node_maintenance(node, action) do
      {:ok, message} ->
        {:noreply, socket |> put_flash(:info, message) |> load()}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event("node_reboot", %{"node" => node}, socket) do
    case Settings.node_reboot(node) do
      {:ok, message} ->
        {:noreply, socket |> put_flash(:info, message) |> load()}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, message)}
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
    <Layouts.app
      socket={@socket}
      flash={@flash}
      current_username={@current_username}
      active={:settings}
    >
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

      <div :if={@apply_failures != []} class="alert alert-error alert-soft" id="apply-failures">
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
        <ul class="text-sm">
          <li :for={failure <- @apply_failures}>{failure}</li>
        </ul>
      </div>

      <div class="flex flex-col gap-4">
        <form phx-submit="save" id="settings-form" class="flex flex-col gap-4">
          <div class="grid gap-4 xl:grid-cols-2">
            <.panel
              id="cluster-settings"
              title="Cluster"
              subtitle="Written to cluster.json on every host"
            >
              <div class="grid gap-3 sm:grid-cols-2">
                <.setting_field
                  name="cluster_name"
                  label="Cluster name"
                  value={@settings.cluster.name}
                  hint="letters, digits and dashes"
                />
                <.setting_field
                  name="vip"
                  label="Virtual IP (VIP)"
                  value={@settings.cluster.vip}
                  hint="the console address; bifrost restarts on every host when it changes"
                />
                <.setting_field
                  name="cluster_subnet"
                  label="Cluster subnet"
                  value={@settings.cluster[:subnet]}
                  hint="CIDR, for example 10.10.102.0/24"
                />
                <.setting_field
                  name="cluster_region"
                  label="Datacenter / region"
                  value={@settings.stored["cluster_region"]}
                />
                <.setting_field
                  name="replication_factor"
                  label="ScyllaDB replication factor"
                  value={@settings.replication.factor}
                  type="number"
                  hint="metadata copies, capped at the node count; raising it starts a repair"
                />
                <.setting_select
                  name="scrub_interval"
                  label="Disk scrub interval"
                  value={@settings.stored["scrub_interval"]}
                  options={[
                    {"daily", "Daily"},
                    {"weekly", "Weekly (recommended)"},
                    {"monthly", "Monthly"},
                    {"disabled", "Disabled"}
                  ]}
                />
                <.setting_select
                  name="fault_domain"
                  label="Fault tolerance domain"
                  value={@settings.stored["fault_domain"] || @settings.cluster[:fault_domain] || "khost"}
                  options={[
                    {"kdisk", "kDisk (Drive-level mirroring: allows FTT 1 on single node)"},
                    {"khost", "kHost (Host-level tolerance: requires FTT+1 hosts)"},
                    {"kblock", "kBlock (Chassis/enclosure tolerance)"},
                    {"krack", "kRack (Rack-level tolerance)"}
                  ]}
                />
              </div>
              <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 mt-4 pt-4 border-t border-base-300">
                <.figure
                  id="fact-id"
                  label="Cluster ID"
                  value={@settings.cluster[:id]}
                  caption="read only"
                />
                <.figure
                  id="fact-nodes"
                  label="Nodes"
                  value={@settings.cluster.nodes}
                  caption="configured hosts"
                />
                <.figure
                  id="fact-ftt"
                  label="Fault tolerance"
                  value={@settings.cluster.redundancy_factor}
                  caption="failures survivable"
                  tone={if (@settings.cluster.redundancy_factor || 0) < 1, do: :warn, else: :good}
                />
                <.figure
                  id="rf-actual"
                  label="Keyspace factor"
                  value={@settings.replication.factor}
                  caption={"asked for #{@settings.replication.implied} by ftt"}
                  tone={replication_tone(@settings.replication)}
                />
              </div>
              <p class="text-xs opacity-60 mt-3" id="replication-note">
                The replication factor is the <span class="font-semibold">hydra keyspace</span>:
                VM records, task history, the block map. It says nothing about how many copies of
                a guest's <em>disk</em>
                exist, which is a property of each vdisk and shown on <.link
                  navigate={~p"/storage"}
                  class="link"
                >Storage</.link>.
              </p>
              <div class="alert alert-info alert-soft text-xs mt-3 p-3 flex items-center justify-between" id="fault-domain-note">
                <div>
                  <span class="font-semibold uppercase tracking-wider">Fault Domain ({@settings.stored["fault_domain"] || @settings.cluster[:fault_domain] || "khost"}):</span>
                  Under <span class="font-mono">kDisk</span>, single-node clusters support <span class="font-semibold">FTT 1</span> via local multi-disk mirroring across physical drives (SSDs/NVMe).
                  Under <span class="font-mono">kHost</span>, surviving failures requires at least FTT+1 physical hosts.
                </div>
                <.link navigate={~p"/topology"} class="btn btn-xs btn-primary shrink-0 ml-3">
                  View Topology <.icon name="hero-arrow-right" class="size-3" />
                </.link>
              </div>
              <p
                :if={mismatched?(@settings.replication)}
                class="text-sm text-warning mt-2"
                id="replication-mismatch"
              >
                The keyspace is replicating {@settings.replication.factor} way(s) but the cluster
                asked for {@settings.replication.implied}. That gap is what a failed or unfinished
                change looks like.
              </p>
            </.panel>

            <div class="flex flex-col gap-4">
              <.panel
                id="dns-settings"
                title="DNS &amp; networking"
                subtitle="Written to resolv.conf on every host"
              >
                <div class="grid gap-3 sm:grid-cols-3">
                  <.setting_field
                    name="dns_servers"
                    label="Resolvers"
                    value={@settings.stored["dns_servers"]}
                    hint="comma separated IP addresses"
                  />
                  <.setting_field
                    name="dns_search_domains"
                    label="Search domains"
                    value={@settings.stored["dns_search_domains"]}
                  />
                  <.setting_field
                    name="dns_mtu"
                    label="MTU"
                    value={@settings.stored["dns_mtu"]}
                    type="number"
                  />
                </div>
              </.panel>

              <.panel
                id="time-settings"
                title="NTP &amp; time"
                subtitle="Written to chrony.conf on every host; chronyd restarts"
              >
                <div class="grid gap-3 sm:grid-cols-2">
                  <.setting_field
                    name="ntp_servers"
                    label="NTP servers"
                    value={@settings.stored["ntp_servers"]}
                    hint="comma separated"
                  />
                  <.setting_select
                    name="timezone"
                    label="Timezone"
                    value={@settings.stored["timezone"]}
                    options={timezones(@settings.stored["timezone"])}
                  />
                </div>
              </.panel>

              <.panel
                id="scheduler-settings"
                title="Scheduling"
                subtitle="Background work the cluster does on its own"
              >
                <div class="grid gap-3 sm:grid-cols-2">
                  <.setting_select
                    name="drs_enabled"
                    label="Distributed resource scheduler"
                    value={@settings.stored["drs_enabled"]}
                    options={[{"true", "Enabled: rebalance VMs across hosts"}, {"false", "Disabled"}]}
                  />
                  <.setting_select
                    name="vm_hotplug_headroom"
                    label="VM CPU & Memory hot-add headroom"
                    value={@settings.stored["vm_hotplug_headroom"] || "false"}
                    options={[
                      {"true", "Enabled: reserve vCPU and memory headroom at boot for live hot-add"},
                      {"false", "Disabled: standard fixed allocation"}
                    ]}
                  />
                </div>
              </.panel>

              <.panel
                id="console-settings"
                title="Guest console"
                subtitle="Remote display protocols and default console for virtual machines"
              >
                <div class="grid gap-3 sm:grid-cols-2">
                  <.setting_select
                    name="console_protocol_policy"
                    label="Supported console protocols"
                    value={@settings.stored["console_protocol_policy"] || "both"}
                    options={[
                      {"both", "Both VNC & SPICE (per-VM choice)"},
                      {"vnc", "VNC only (universal compatibility)"},
                      {"spice", "SPICE only (high-performance desktop)"}
                    ]}
                  />
                  <.setting_select
                    name="default_console_protocol"
                    label="Default console protocol"
                    value={@settings.stored["default_console_protocol"] || "vnc"}
                    options={[
                      {"vnc", "VNC"},
                      {"spice", "SPICE"}
                    ]}
                  />
                </div>
              </.panel>

              <.panel
                id="security-settings"
                title="Security"
                subtitle="Access control for console operators"
              >
                <div class="grid gap-3 sm:grid-cols-3">
                  <.setting_select
                    name="password_policy"
                    label="Password complexity"
                    value={@settings.stored["password_policy"]}
                    options={[
                      {"disabled", "Basic: 5+ characters"},
                      {"enabled", "Strong: 8+, upper-case, digit, symbol"}
                    ]}
                  />
                  <.setting_field
                    name="session_timeout"
                    label="Session timeout"
                    value={@settings.stored["session_timeout"]}
                    type="number"
                    hint="minutes, 5 to 1440"
                  />
                  <.setting_field
                    name="rate_limit"
                    label="Auth requests / minute"
                    value={@settings.stored["rate_limit"]}
                    type="number"
                    hint="5 to 1000"
                  />
                </div>
              </.panel>

              <.panel
                id="appearance-settings"
                title="Appearance &amp; region"
                subtitle="Visual theme and console localization"
              >
                <div class="grid gap-3 sm:grid-cols-2">
                  <.setting_select
                    name="language"
                    label="Console language"
                    value={@settings.stored["language"]}
                    options={[
                      {"en-US", "English (United States)"},
                      {"de-DE", "Deutsch (German)"},
                      {"fr-FR", "Français (French)"},
                      {"es-ES", "Español (Spanish)"},
                      {"ja-JP", "日本語 (Japanese)"},
                      {"nl-NL", "Nederlands (Dutch)"}
                    ]}
                  />
                  <div>
                    <label class="form-control">
                      <span class="label-text text-xs opacity-70">Theme preference</span>
                      <div class="flex items-center gap-2 mt-1">
                        <button
                          type="button"
                          class="btn btn-outline btn-xs"
                          phx-click={JS.dispatch("phx:set-theme", detail: %{})}
                          data-phx-theme="dark"
                          id="theme-dark-btn"
                        >
                          <.icon name="hero-moon" class="size-3" /> Dark
                        </button>
                        <button
                          type="button"
                          class="btn btn-outline btn-xs"
                          phx-click={JS.dispatch("phx:set-theme", detail: %{})}
                          data-phx-theme="light"
                          id="theme-light-btn"
                        >
                          <.icon name="hero-sun" class="size-3" /> Light
                        </button>
                        <button
                          type="button"
                          class="btn btn-outline btn-xs"
                          phx-click={JS.dispatch("phx:set-theme", detail: %{})}
                          data-phx-theme="system"
                          id="theme-system-btn"
                        >
                          <.icon name="hero-computer-desktop" class="size-3" /> System
                        </button>
                      </div>
                    </label>
                  </div>
                </div>
              </.panel>
            </div>
          </div>

          <div class="flex justify-end">
            <button type="submit" class="btn btn-primary btn-sm" id="save-settings">
              <.icon name="hero-check" class="size-4" /> Save settings
            </button>
          </div>
        </form>

        <div class="grid gap-4 lg:grid-cols-2">
          <.panel
            id="overlay-setting"
            title="Overlay networking"
            subtitle="Runs on every host, as a task"
          >
            <div class="flex items-center gap-3">
              <span
                id="urbosa-state"
                class={[
                  "badge",
                  urbosa_on?(@settings) && "badge-success",
                  not urbosa_on?(@settings) && "badge-ghost"
                ]}
              >
                Urbosa {if urbosa_on?(@settings), do: "enabled", else: "disabled"}
              </span>
              <.link navigate={~p"/sdn"} class="btn btn-ghost btn-xs">
                SDN <.icon name="hero-arrow-right" class="size-3" />
              </.link>
            </div>

            <p class="text-xs opacity-60 mt-3">
              Building the overlay creates network namespaces, bridges and VXLAN interfaces
              on every host; removing it deletes them from every host. Either direction runs
              as a Catalyst task on the ZooKeeper leader, so the ring in the header carries
              its progress and a failure lands in the task log rather than nowhere.
            </p>

            <div :if={is_nil(@confirming)} class="mt-3">
              <button
                :if={not urbosa_on?(@settings)}
                phx-click="ask_urbosa"
                phx-value-value="true"
                class="btn btn-primary btn-sm"
                id="urbosa-enable"
              >
                <.icon name="hero-bolt" class="size-4" /> Build the overlay
              </button>
              <button
                :if={urbosa_on?(@settings)}
                phx-click="ask_urbosa"
                phx-value-value="false"
                class="btn btn-error btn-outline btn-sm"
                id="urbosa-disable"
              >
                <.icon name="hero-trash" class="size-4" /> Tear the overlay down
              </button>
            </div>

            <div
              :if={@confirming == :bootstrap}
              class="alert alert-info alert-soft mt-3 flex-col items-start gap-2"
              id="urbosa-confirm-bootstrap"
            >
              <span class="text-sm">
                This creates the overlay namespaces, bridges and VXLAN interfaces on all {@settings.cluster.nodes} host(s). Existing guest networking is not touched.
              </span>
              <div class="flex gap-2">
                <button
                  phx-click="set_urbosa"
                  phx-value-value="true"
                  class="btn btn-primary btn-xs"
                  id="urbosa-confirm-enable"
                >
                  Build it
                </button>
                <button phx-click="cancel_urbosa" class="btn btn-ghost btn-xs">Cancel</button>
              </div>
            </div>

            <div
              :if={@confirming == :teardown}
              class="alert alert-error alert-soft mt-3 flex-col items-start gap-2"
              id="urbosa-confirm-teardown"
            >
              <span class="text-sm">
                This <span class="font-semibold">removes</span>
                the overlay namespaces, bridges and VXLAN interfaces from all {@settings.cluster.nodes} host(s). Anything routing over the overlay -- a
                Kubernetes cluster above all -- loses its network the moment this runs.
              </span>
              <div class="flex gap-2">
                <button
                  phx-click="set_urbosa"
                  phx-value-value="false"
                  class="btn btn-error btn-xs"
                  id="urbosa-confirm-disable"
                >
                  Tear it down
                </button>
                <button phx-click="cancel_urbosa" class="btn btn-ghost btn-xs">Cancel</button>
              </div>
            </div>

            <p :if={@urbosa_error} class="text-sm text-error mt-3" id="urbosa-error">
              {@urbosa_error}
            </p>

            <p :if={@urbosa_task} class="text-xs opacity-70 mt-3" id="urbosa-task">
              {task_word(@urbosa_task)} submitted as
              <span class="font-mono">{elem(@urbosa_task, 1)}</span>
              &mdash; <.link navigate={~p"/tasks"} class="link">watch it</.link>.
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

            <form
              id="create-user-form"
              phx-submit="create_user"
              class="grid gap-2 sm:grid-cols-3 items-end mt-4 pt-4 border-t border-base-300"
            >
              <label class="form-control">
                <span class="label-text text-xs opacity-70">New username</span>
                <input
                  type="text"
                  name="username"
                  class="input input-bordered input-sm w-full"
                  autocomplete="off"
                  required
                />
              </label>
              <label class="form-control">
                <span class="label-text text-xs opacity-70">Password</span>
                <input
                  type="password"
                  name="password"
                  class="input input-bordered input-sm w-full"
                  autocomplete="new-password"
                  required
                />
              </label>
              <button type="submit" class="btn btn-primary btn-sm" id="create-user">
                <.icon name="hero-user-plus" class="size-4" /> Create user
              </button>
            </form>

            <form
              id="set-password-form"
              phx-submit="set_password"
              class="grid gap-2 sm:grid-cols-4 items-end mt-4 pt-4 border-t border-base-300"
            >
              <label class="form-control">
                <span class="label-text text-xs opacity-70">Change password for</span>
                <select name="username" class="select select-bordered select-sm w-full">
                  <option :for={user <- @settings.users} value={user}>{user}</option>
                </select>
              </label>
              <label class="form-control">
                <span class="label-text text-xs opacity-70">New password</span>
                <input
                  type="password"
                  name="password"
                  class="input input-bordered input-sm w-full"
                  autocomplete="new-password"
                  required
                />
              </label>
              <label class="form-control">
                <span class="label-text text-xs opacity-70">Confirm</span>
                <input
                  type="password"
                  name="confirm"
                  class="input input-bordered input-sm w-full"
                  autocomplete="new-password"
                  required
                />
              </label>
              <button type="submit" class="btn btn-sm" id="set-password">Change password</button>
            </form>
          </.panel>
        </div>

        <.panel
          id="ssl-settings"
          title="SSL / TLS certificate"
          subtitle="Deploy public certificate and private key across all cluster nodes"
        >
          <p class="text-xs opacity-60 mb-3">
            Upload or rotate the TLS certificate and unencrypted private key for the web console and proxy.
            Once saved, the files are securely pushed across all cluster hosts and the slate proxy is reloaded.
          </p>

          <form id="ssl-form" phx-submit="deploy_ssl" class="flex flex-col gap-3">
            <div class="grid gap-3 sm:grid-cols-2">
              <label class="form-control">
                <span class="label-text text-xs opacity-70">Certificate (PEM format)</span>
                <textarea
                  name="certificate"
                  id="ssl-cert-input"
                  class="textarea textarea-bordered font-mono text-xs h-28 w-full"
                  placeholder={"-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----"}
                  required
                ></textarea>
                <span class="text-[0.65rem] opacity-45 mt-0.5">X.509 certificate chain in PEM format.</span>
              </label>

              <label class="form-control">
                <span class="label-text text-xs opacity-70">Private key (PEM format)</span>
                <textarea
                  name="private_key"
                  id="ssl-key-input"
                  class="textarea textarea-bordered font-mono text-xs h-28 w-full"
                  placeholder={"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"}
                  required
                ></textarea>
                <span class="text-[0.65rem] opacity-45 mt-0.5">Matching RSA or EC private key in PEM format.</span>
              </label>
            </div>

            <div class="flex justify-end mt-2">
              <button type="submit" class="btn btn-primary btn-sm" id="deploy-ssl-btn">
                <.icon name="hero-shield-check" class="size-4" /> Deploy SSL certificate
              </button>
            </div>
          </form>
        </.panel>

        <.panel
          id="cluster-nodes-panel"
          title="Cluster nodes &amp; membership"
          subtitle="Manage node membership, quorum tie-breaker witnesses, and safe host operations"
        >
          <div class="overflow-x-auto">
            <table class="table table-zebra table-sm" id="cluster-nodes-table">
              <thead>
                <tr>
                  <th>Hostname</th>
                  <th>IP address</th>
                  <th>Role</th>
                  <th>Maintenance</th>
                  <th class="text-right">Operations</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={node <- @settings.nodes} id={"node-row-#{node.hostname}"}>
                  <td class="font-medium font-mono">{node.hostname}</td>
                  <td class="font-mono text-xs">{node.ip}</td>
                  <td>
                    <span :if={node.is_witness?} class="badge badge-sm badge-info gap-1" id={"witness-badge-#{node.hostname}"}>
                      <.icon name="hero-scale" class="size-3" /> Witness / Quorum Tie-Breaker
                    </span>
                    <span :if={not node.is_witness?} class="badge badge-sm badge-ghost gap-1">
                      <.icon name="hero-server" class="size-3" /> Hypervisor
                    </span>
                  </td>
                  <td>
                    <span :if={node.in_maintenance?} class="badge badge-sm badge-warning gap-1" id={"maint-badge-#{node.hostname}"}>
                      <.icon name="hero-wrench-screwdriver" class="size-3" /> Maintenance
                    </span>
                    <span :if={not node.in_maintenance?} class="badge badge-sm badge-success gap-1">
                      <.icon name="hero-check" class="size-3" /> Normal
                    </span>
                  </td>
                  <td class="text-right">
                    <div class="flex items-center justify-end gap-1.5">
                      <button
                        :if={not node.in_maintenance?}
                        phx-click="node_maintenance"
                        phx-value-node={node.ip}
                        phx-value-action="enter"
                        class="btn btn-ghost btn-xs text-warning"
                        id={"enter-maint-#{node.hostname}"}
                      >
                        Enter maint
                      </button>
                      <button
                        :if={node.in_maintenance?}
                        phx-click="node_maintenance"
                        phx-value-node={node.ip}
                        phx-value-action="leave"
                        class="btn btn-ghost btn-xs text-success"
                        id={"leave-maint-#{node.hostname}"}
                      >
                        Leave maint
                      </button>
                      <button
                        phx-click="node_reboot"
                        phx-value-node={node.ip}
                        data-confirm={"Are you sure you want to reboot #{node.hostname} (#{node.ip})?"}
                        class="btn btn-ghost btn-xs text-error"
                        id={"reboot-#{node.hostname}"}
                      >
                        Reboot
                      </button>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <div class="grid gap-4 md:grid-cols-2 mt-4 pt-4 border-t border-base-300">
            <!-- Add Node Form -->
            <form id="add-node-form" phx-submit="add_node" class="flex flex-col gap-3">
              <h3 class="text-xs font-semibold uppercase tracking-wider opacity-70">Add node to cluster</h3>
              <div class="grid gap-2 sm:grid-cols-2">
                <label class="form-control">
                  <span class="label-text text-xs opacity-70">Node hostname</span>
                  <input
                    type="text"
                    name="hostname"
                    id="add-node-hostname"
                    class="input input-bordered input-sm w-full"
                    placeholder="e.g. hci-03"
                    autocomplete="off"
                    required
                  />
                </label>
                <label class="form-control">
                  <span class="label-text text-xs opacity-70">Management IP address</span>
                  <input
                    type="text"
                    name="ip"
                    id="add-node-ip"
                    class="input input-bordered input-sm w-full"
                    placeholder="e.g. 10.10.102.43"
                    autocomplete="off"
                    required
                  />
                </label>
              </div>

              <div class="flex items-center gap-2 mt-1">
                <input
                  type="checkbox"
                  name="witness"
                  value="true"
                  id="add-node-witness"
                  class="checkbox checkbox-sm checkbox-primary"
                />
                <label for="add-node-witness" class="text-xs cursor-pointer">
                  <span class="font-medium">Quorum tie-breaker (Witness node)</span>
                  <span class="block text-[0.65rem] opacity-60">Votes in ZooKeeper without storage or VM workloads.</span>
                </label>
              </div>

              <div class="flex justify-start mt-1">
                <button type="submit" class="btn btn-primary btn-sm" id="btn-add-cluster-node">
                  <.icon name="hero-plus-circle" class="size-4" /> Add node
                </button>
              </div>
            </form>

            <!-- Remove Node Form -->
            <form id="remove-node-form" phx-submit="remove_node" class="flex flex-col gap-3">
              <h3 class="text-xs font-semibold uppercase tracking-wider opacity-70">Remove node from cluster</h3>
              <label class="form-control">
                <span class="label-text text-xs opacity-70">Select node to remove</span>
                <select name="target" id="remove-node-select" class="select select-bordered select-sm w-full" required>
                  <option value="">-- Select node --</option>
                  <option :for={node <- @settings.nodes} value={node.hostname}>
                    {node.hostname} ({node.ip}) {if node.is_witness?, do: "[Witness]", else: ""}
                  </option>
                </select>
                <span class="text-[0.65rem] opacity-45 mt-0.5">Updates cluster membership and ZooKeeper ensemble.</span>
              </label>

              <div class="flex justify-start mt-1">
                <button
                  type="submit"
                  class="btn btn-error btn-outline btn-sm"
                  id="btn-remove-cluster-node"
                  data-confirm="Are you sure you want to remove this node from the cluster? This action will update consensus membership."
                >
                  <.icon name="hero-trash" class="size-4" /> Remove node
                </button>
              </div>
            </form>
          </div>
        </.panel>
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

  attr :name, :string, required: true
  attr :label, :string, required: true
  attr :value, :any, default: nil
  attr :options, :list, required: true

  defp setting_select(assigns) do
    ~H"""
    <label class="form-control">
      <span class="label-text text-xs opacity-70">{@label}</span>
      <select name={@name} id={"setting-#{@name}"} class="select select-bordered select-sm w-full">
        <option :for={{value, text} <- @options} value={value} selected={value == @value}>
          {text}
        </option>
      </select>
    </label>
    """
  end

  @timezones ~w(UTC America/New_York America/Chicago America/Denver America/Los_Angeles
                Europe/London Europe/Paris Europe/Brussels Asia/Tokyo Asia/Singapore)

  # The stored zone is always an option, whatever it is: a select that cannot show the value
  # in force would silently change it on the next save.
  defp timezones(current) do
    zones = if current in [nil, ""] or current in @timezones, do: @timezones, else: [current | @timezones]
    Enum.map(zones, &{&1, &1})
  end

  defp urbosa_on?(settings), do: settings.read_only["urbosa_enabled"] == "true"

  defp submitted_message(:bootstrap),
    do: "Overlay bootstrap submitted. Watch the task ring; it runs on every host."

  defp submitted_message(:teardown),
    do: "Overlay teardown submitted. Watch the task ring; it runs on every host."

  defp task_word({:bootstrap, _id}), do: "Bootstrap"
  defp task_word({:teardown, _id}), do: "Teardown"

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
