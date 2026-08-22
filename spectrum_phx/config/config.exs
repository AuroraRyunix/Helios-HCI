# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

config :spectrum_phx,
  generators: [timestamp_type: :utc_datetime]

# Configure the endpoint
config :spectrum_phx, SpectrumPhxWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: SpectrumPhxWeb.ErrorHTML, json: SpectrumPhxWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: SpectrumPhx.PubSub,
  live_view: [signing_salt: "Z7reP+66"]

# Configure LiveView
config :phoenix_live_view,
  # the attribute set on all root tags. Used for Phoenix.LiveView.ColocatedCSS.
  root_tag_attribute: "phx-r"

# Configure esbuild (the version is required)
config :esbuild,
  version: "0.25.4",
  spectrum_phx: [
    args:
      ~w(js/app.js --bundle --target=es2022 --outdir=../priv/static/assets/js --external:/fonts/* --external:/images/* --alias:@=.),
    cd: Path.expand("../assets", __DIR__),
    env: %{"NODE_PATH" => [Path.expand("../deps", __DIR__), Mix.Project.build_path()]}
  ]

# Configure tailwind (the version is required)
config :tailwind,
  version: "4.3.0",
  spectrum_phx: [
    args: ~w(
      --input=assets/css/app.css
      --output=priv/static/assets/css/app.css
    ),
    cd: Path.expand("..", __DIR__),
    env: %{"NODE_PATH" => [Path.expand("../deps", __DIR__), Mix.Project.build_path()]}
  ]

# Configure Elixir's Logger
config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

# Disk-image extensions, so `allow_upload(accept: ~w(.iso .qcow2 .img))` can resolve them.
# The MIME database ships none of these, and LiveView validates the accept list against it
# at mount, so without this the images page raises rather than rejecting a bad file.
config :mime, :types, %{
  "application/x-iso9660-image" => ["iso"],
  "application/x-qemu-disk" => ["qcow2"],
  "application/x-raw-disk-image" => ["img"]
}

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"

# We use no colocated hooks/js/css; suppress the Windows symlink warning.
config :phoenix_live_view, :colocated_assets, disable_symlink_warning: true
