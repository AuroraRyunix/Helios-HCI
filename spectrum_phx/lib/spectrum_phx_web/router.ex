defmodule SpectrumPhxWeb.Router do
  use SpectrumPhxWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {SpectrumPhxWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", SpectrumPhxWeb do
    pipe_through :browser

    live "/", Cluster.OverviewLive, :overview
    live "/hosts", Cluster.HostsLive, :hosts
    live "/vms", Vms.IndexLive, :index
    # Before "/vms/:name", which would otherwise match "new" as a VM name.
    live "/vms/new", Vms.NewLive, :new
    live "/vms/:name", Vms.ShowLive, :show
  end

  # Other scopes may use custom stacks.
  # scope "/api", SpectrumPhxWeb do
  #   pipe_through :api
  # end

  # Enable LiveDashboard in development
  if Application.compile_env(:spectrum_phx, :dev_routes) do
    # If you want to use the LiveDashboard in production, you should put
    # it behind authentication and allow only admins to access it.
    # If your application does not have an admins-only section yet,
    # you can use Plug.BasicAuth to set up some basic authentication
    # as long as you are also using SSL (which you should anyway).
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard", metrics: SpectrumPhxWeb.Telemetry
    end
  end
end
