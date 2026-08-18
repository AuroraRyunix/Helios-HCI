defmodule SpectrumPhx.Application do
  # See https://elixir.hexdocs.pm/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      SpectrumPhxWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:spectrum_phx, :dns_cluster_query) || :ignore},
      # Cluster configuration is read once and cached; Hydra connects lazily so the
      # web tier boots and reports ScyllaDB as down rather than failing alongside it.
      SpectrumPhx.Cluster.Config,
      SpectrumPhx.Hydra,
      {Phoenix.PubSub, name: SpectrumPhx.PubSub},
      # Start a worker by calling: SpectrumPhx.Worker.start_link(arg)
      # {SpectrumPhx.Worker, arg},
      # Start to serve requests, typically the last entry
      SpectrumPhxWeb.Endpoint
    ]

    # See https://elixir.hexdocs.pm/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: SpectrumPhx.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    SpectrumPhxWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
