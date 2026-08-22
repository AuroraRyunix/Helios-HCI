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
  @nav [
    {:overview, "Overview", "/"},
    {:hosts, "Hosts", "/hosts"},
    {:vms, "VMs", "/vms"},
    {:storage, "Storage", "/storage"},
    {:images, "Images", "/images"},
    {:tasks, "Tasks", "/tasks"},
    {:metrics, "Metrics", "/metrics"},
    {:health, "Health", "/health"}
  ]

  @doc "The navigation entries, as `{id, label, path}`. Exposed so tests can walk them."
  def nav_items, do: @nav

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
          <li :for={{id, label, path} <- nav_items()}>
            <.link
              navigate={path}
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
