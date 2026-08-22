import Config

# config/runtime.exs is executed for all environments, including
# during releases. It is executed after compilation and before the
# system starts, so it is typically used to load production configuration
# and secrets from environment variables or elsewhere. Do not define
# any compile-time configuration in here, as it won't be applied.

# ## Releases
#
# The release starts the web server only when PHX_SERVER is set, so that
# `bin/spectrum_phx eval|rpc|remote` can run without trying to bind a port that
# the live instance already holds. The container image sets PHX_SERVER=true (see
# Dockerfile) and so does quadlet/spectrum-phx.container, so a plain
# `podman run` of the image serves traffic with no extra flags.
if System.get_env("PHX_SERVER") do
  config :spectrum_phx, SpectrumPhxWeb.Endpoint, server: true
end

# ## Listener port
#
# 8443 in prod: that is the port Slate (Traefik) already proxies the console to
# for the Python tier, so completing the migration is a one-line change in
# slate_config/dynamic.yml rather than a port renumbering. While both tiers run
# side by side the Quadlet overrides PORT -- see quadlet/spectrum-phx.container.
#
# Dev and test keep Phoenix's usual 4000 so `mix phx.server` behaves normally.
default_port = if config_env() == :prod, do: "8443", else: "4000"
port = String.to_integer(System.get_env("PORT") || default_port)

config :spectrum_phx, SpectrumPhxWeb.Endpoint, http: [port: port]

if config_env() == :dev do
  # Reload browser tabs when matching files change.
  config :spectrum_phx, SpectrumPhxWeb.Endpoint,
    live_reload: [
      web_console_logger: true,
      patterns: [
        # Static assets, except user uploads
        ~r"priv/static/(?!uploads/).*\.(js|css|png|jpeg|jpg|gif|svg)$",
        # Router, Controllers, LiveViews and LiveComponents
        ~r"lib/spectrum_phx_web/router\.ex$",
        ~r"lib/spectrum_phx_web/(controllers|live|components)/.*\.(ex|heex)$"
      ]
    ]
end

if config_env() == :prod do
  # ## Bind address
  #
  # The container runs with Network=host, so this binds the hypervisor's real
  # interfaces. IPv4 0.0.0.0 rather than the generated IPv6 wildcard: Slate
  # dials 127.0.0.1 over IPv4, and a v6 wildcard only covers that when
  # net.ipv6.bindv6only happens to be 0. Override with PHX_BIND_IP if the
  # listener should be restricted (e.g. 127.0.0.1 once Slate is the only
  # client and the port should not be reachable from the storage network).
  bind_ip_string = System.get_env("PHX_BIND_IP", "0.0.0.0")

  bind_ip =
    case :inet.parse_address(String.to_charlist(bind_ip_string)) do
      {:ok, address} ->
        address

      {:error, _} ->
        raise "PHX_BIND_IP must be a literal IP address, got: #{inspect(bind_ip_string)}"
    end

  # ## Secrets
  #
  # The secret key base is used to sign/encrypt cookies and other secrets.
  # A default value is used in config/dev.exs and config/test.exs but you
  # want to use a different value for prod and you most likely don't want
  # to check this value into version control, so we use an environment
  # variable instead.
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.

      It signs the session cookie, so it must be IDENTICAL on every node in the
      cluster: otherwise a session established through the VIP stops working the
      moment Slate lands the next request on a different hypervisor.

      Generate one once per cluster with:

          mix phx.gen.secret

      or, on a host with no Elixir installed:

          openssl rand -base64 48 | tr -d '[:space:]'

      and write it to /etc/hci/spectrum/spectrum-phx.env (mode 0600) as

          SECRET_KEY_BASE=...

      which quadlet/spectrum-phx.container picks up via EnvironmentFile=.
      """

  # Used for URL generation (links, redirects, LiveView's Origin check). This is
  # the name the console is reached by from a browser -- the cluster VIP or its
  # DNS name -- not the node's own hostname.
  host = System.get_env("PHX_HOST") || "localhost"

  # ## Origin checking
  #
  # LiveView rejects the websocket upgrade when the Origin header matches none of
  # the configured entries, and the console is legitimately reached by DNS name,
  # by cluster VIP and by bare node IP. Each entry is written "//host" so that
  # scheme and port stay wildcarded.
  #
  #   PHX_EXTRA_ORIGINS  comma-separated extra hosts (VIP, node IPs, alt names)
  #   PHX_CHECK_ORIGIN   "false" to disable entirely, "true" to compare against
  #                      the endpoint URL only, or a comma-separated override list
  check_origin =
    case System.get_env("PHX_CHECK_ORIGIN") do
      nil ->
        extra = String.split(System.get_env("PHX_EXTRA_ORIGINS", ""), ",", trim: true)

        [host, "localhost", "127.0.0.1"]
        |> Enum.concat(extra)
        |> Enum.map(&String.trim/1)
        |> Enum.reject(&(&1 == ""))
        |> Enum.uniq()
        |> Enum.map(&("//" <> &1))

      "false" ->
        false

      "true" ->
        true

      list ->
        list |> String.split(",", trim: true) |> Enum.map(&String.trim/1)
    end

  config :spectrum_phx, :dns_cluster_query, System.get_env("DNS_CLUSTER_QUERY")

  config :spectrum_phx, SpectrumPhxWeb.Endpoint,
    url: [host: host, port: 443, scheme: "https"],
    http: [ip: bind_ip, port: port],
    check_origin: check_origin,
    secret_key_base: secret_key_base

  # ## Optional direct-TLS listener
  #
  # Slate's `spectrum-backend` service dials **https**://127.0.0.1:8443 with
  # insecureSkipVerify, because the Python tier terminates TLS itself. Pointing
  # Slate at this app therefore needs either that service URL changed to http://,
  # or this listener enabled on the same certificate pair Slate already serves
  # from /etc/hci/spectrum/certs. Enabling it makes this app a drop-in for the
  # Python backend with no change to dynamic.yml at all.
  #
  # Off by default: behind Slate on the same host, plaintext on loopback is not
  # buying an attacker anything they could not get from the host itself.
  tls_port = System.get_env("SPECTRUM_TLS_PORT")

  if tls_port do
    certfile = System.get_env("SPECTRUM_TLS_CERT", "/etc/hci/spectrum/certs/server.crt")
    keyfile = System.get_env("SPECTRUM_TLS_KEY", "/etc/hci/spectrum/certs/server.key")

    Enum.each([{"certificate", certfile}, {"private key", keyfile}], fn {what, path} ->
      File.exists?(path) ||
        raise """
        SPECTRUM_TLS_PORT is set but the TLS #{what} is not readable at #{path}.

        Check that /etc/hci/spectrum is mounted into the container and that
        provision.py has generated the Spectrum server certificate pair.
        """
    end)

    config :spectrum_phx, SpectrumPhxWeb.Endpoint,
      https: [
        ip: bind_ip,
        port: String.to_integer(tls_port),
        cipher_suite: :strong,
        certfile: certfile,
        keyfile: keyfile
      ]
  end
end
