defmodule SpectrumPhxWeb.Layouts do
  @moduledoc """
  This module holds layouts and related functionality
  used by your application.
  """
  use SpectrumPhxWeb, :html

  # Embed all files in layouts/* within this module.
  # The default root.html.heex file contains the HTML
  # skeleton of your application, namely HTML headers
  # and other static content.
  embed_templates "layouts/*"

  # The console's navigation, in one place. Every dashboard renders through `app/1`, so
  # this list is the only thing that has to know a page exists.
  #
  # The fourth element says which tier serves the page. `:live` is this application.
  # `:legacy` is the Python console, still serving the pages that have not been rebuilt
  # here yet — Slate routes those paths to it, and they keep their `.html` suffix because
  # that is what it serves them as.
  #
  # The distinction is not cosmetic. A `:legacy` entry has to be an ordinary link: live
  # navigation asks this application's router for the page and it does not have one, so
  # `navigate` would fail where `href` simply leaves for the other tier. Both tiers accept
  # the same session, so crossing between them does not ask the operator to sign in again.
  #
  # As each page is rebuilt its entry moves from `:legacy` to `:live` and loses the
  # suffix. When none are left, the split — and the routing rule in
  # slate_config/dynamic.yml that implements it — goes away with them.
  @nav [
    {:overview, "Overview", "/", :live},
    {:hosts, "Hosts", "/hosts", :live},
    {:vms, "VMs", "/vms", :live},
    {:storage, "Storage", "/storage", :live},
    {:images, "Images", "/images", :live},
    {:tasks, "Tasks", "/tasks", :live},
    {:metrics, "Metrics", "/metrics", :live},
    {:health, "Health", "/health", :live},
    {:hardware, "Hardware", "/hardware", :live},
    {:sdn, "SDN", "/sdn", :live},
    {:networking, "Networking", "/networking.html", :legacy},
    {:lcm, "LCM", "/lcm.html", :legacy},
    {:lanayru, "Lanayru", "/lanayru.html", :legacy},
    {:settings, "Settings", "/settings.html", :legacy}
  ]

  @doc """
  The navigation entries, as `{id, label, path, tier}`. Exposed so tests can walk them.

  `tier` is `:live` for a page this application routes and `:legacy` for one the Python
  console still serves.
  """
  def nav_items, do: @nav

  @doc "The navigation entries this application routes itself."
  def live_nav_items, do: Enum.filter(@nav, fn {_, _, _, tier} -> tier == :live end)

  @doc "The navigation entries still served by the Python console."
  def legacy_nav_items, do: Enum.filter(@nav, fn {_, _, _, tier} -> tier == :legacy end)

  @doc """
  Renders the console layout: navigation, the signed-in operator, and the page itself.

  ## Examples

      <Layouts.app flash={@flash} current_username={@current_username} active={:hosts}>
        <h1>Content</h1>
      </Layouts.app>

  """
  attr :flash, :map, required: true, doc: "the map of flash messages"

  attr :current_username, :string,
    default: nil,
    doc: "the signed-in operator, shown in the header"

  attr :active, :atom,
    default: nil,
    doc: "which entry of `nav_items/0` is the current page"

  slot :inner_block, required: true

  def app(assigns) do
    ~H"""
    <header class="navbar border-b border-base-300 gap-2 px-4 sm:px-6 lg:px-8">
      <div class="flex-none">
        <.link navigate="/" class="flex items-center gap-2 text-lg font-semibold">
          <img src={~p"/images/logo.svg"} width="28" alt="" />
          <span>Helios</span>
        </.link>
      </div>

      <nav class="flex-1 overflow-x-auto" aria-label="Console">
        <ul class="menu menu-horizontal gap-1 flex-nowrap">
          <li :for={{id, label, path, tier} <- nav_items()}>
            <.link
              :if={tier == :live}
              navigate={path}
              aria-current={if @active == id, do: "page"}
              class={["font-medium whitespace-nowrap", @active == id && "menu-active"]}
            >
              {label}
            </.link>
            <.link
              :if={tier == :legacy}
              href={path}
              aria-current={if @active == id, do: "page"}
              class={["font-medium whitespace-nowrap", @active == id && "menu-active"]}
            >
              {label}
            </.link>
          </li>
        </ul>
      </nav>

      <div class="flex-none flex items-center gap-2">
        <.theme_toggle />
        <span :if={@current_username} class="text-sm opacity-70" data-role="current-user">
          {@current_username}
        </span>
        <.link href={~p"/logout"} method="delete" class="btn btn-ghost btn-sm">
          Sign out
        </.link>
      </div>
    </header>

    <main class="px-4 py-8 sm:px-6 lg:px-8">
      <div class="mx-auto max-w-7xl space-y-4">
        {render_slot(@inner_block)}
      </div>
    </main>

    <.flash_group flash={@flash} />
    """
  end

  @doc """
  Shows the flash group with standard titles and content.

  ## Examples

      <.flash_group flash={@flash} />
  """
  attr :flash, :map, required: true, doc: "the map of flash messages"
  attr :id, :string, default: "flash-group", doc: "the optional id of flash container"

  def flash_group(assigns) do
    ~H"""
    <div id={@id} aria-live="polite">
      <.flash kind={:info} flash={@flash} />
      <.flash kind={:error} flash={@flash} />

      <.flash
        id="client-error"
        kind={:error}
        title="We can't find the internet"
        phx-disconnected={
          show(".phx-client-error #client-error")
          |> JS.remove_attribute("hidden", to: ".phx-client-error #client-error")
        }
        phx-connected={hide("#client-error") |> JS.set_attribute({"hidden", ""})}
        hidden
      >
        Attempting to reconnect
        <.icon name="hero-arrow-path" class="ml-1 size-3 motion-safe:animate-spin" />
      </.flash>

      <.flash
        id="server-error"
        kind={:error}
        title="Something went wrong!"
        phx-disconnected={
          show(".phx-server-error #server-error")
          |> JS.remove_attribute("hidden", to: ".phx-server-error #server-error")
        }
        phx-connected={hide("#server-error") |> JS.set_attribute({"hidden", ""})}
        hidden
      >
        Attempting to reconnect
        <.icon name="hero-arrow-path" class="ml-1 size-3 motion-safe:animate-spin" />
      </.flash>
    </div>
    """
  end

  @doc """
  Provides dark vs light theme toggle based on themes defined in app.css.

  See <head> in root.html.heex which applies the theme before page load.
  """
  def theme_toggle(assigns) do
    ~H"""
    <div class="card relative flex flex-row items-center border-2 border-base-300 bg-base-300 rounded-full">
      <div class="absolute w-1/3 h-full rounded-full border-1 border-base-200 bg-base-100 brightness-200 left-0 [[data-theme=light]_&]:left-1/3 [[data-theme=dark]_&]:left-2/3 [[data-theme-source=system]_&]:!left-0 transition-[left]" />

      <button
        class="flex p-2 cursor-pointer w-1/3"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="system"
      >
        <.icon name="hero-computer-desktop-micro" class="size-4 opacity-75 hover:opacity-100" />
      </button>

      <button
        class="flex p-2 cursor-pointer w-1/3"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="light"
      >
        <.icon name="hero-sun-micro" class="size-4 opacity-75 hover:opacity-100" />
      </button>

      <button
        class="flex p-2 cursor-pointer w-1/3"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="dark"
      >
        <.icon name="hero-moon-micro" class="size-4 opacity-75 hover:opacity-100" />
      </button>
    </div>
    """
  end
end
