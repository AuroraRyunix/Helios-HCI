defmodule SpectrumPhxWeb.Router do
  use SpectrumPhxWeb, :router

  import SpectrumPhxWeb.UserAuth

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {SpectrumPhxWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug :fetch_current_user
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  ## Unauthenticated

  scope "/", SpectrumPhxWeb do
    pipe_through [:browser, :redirect_if_authenticated]

    live "/login", LoginLive, :new
    post "/login", SessionController, :create
  end

  scope "/", SpectrumPhxWeb do
    pipe_through :browser

    # Reachable while signed out so a stale cookie can always be cleared.
    delete "/logout", SessionController, :delete
    get "/logout", SessionController, :delete
  end

  ## Authenticated
  ##
  ## Every dashboard sits inside this live_session, so the auth check runs once before
  ## the socket connects rather than being repeated (and forgotten) in each mount.

  scope "/", SpectrumPhxWeb do
    pipe_through [:browser, :require_authenticated_user]

    live_session :authenticated,
      on_mount: [{SpectrumPhxWeb.UserAuth, :ensure_authenticated}] do
      live "/", Cluster.OverviewLive, :overview
      live "/hosts", Cluster.HostsLive, :hosts

      live "/vms", Vms.IndexLive, :index
      # Before "/vms/:name", which would otherwise match "new" as a VM name.
      live "/vms/new", Vms.NewLive, :new
      live "/vms/:name", Vms.ShowLive, :show
    end
  end

  # Enable LiveDashboard in development
  if Application.compile_env(:spectrum_phx, :dev_routes) do
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      # Behind the same authentication as everything else: it exposes process state,
      # environment and metrics for the whole node.
      pipe_through [:browser, :require_authenticated_user]

      live_dashboard "/dashboard", metrics: SpectrumPhxWeb.Telemetry
    end
  end
end
