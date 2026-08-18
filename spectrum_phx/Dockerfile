# Spectrum (Phoenix) -- OTP release image for Helios HCI hypervisors.
#
#   podman build -t localhost/spectrum-phx:latest spectrum_phx/
#
# The hypervisors run Rocky Linux 10.2 with Podman 5.8 and have no Elixir or
# Erlang installed, so the release has to carry its own runtime. It does:
# `mix release` bundles ERTS by default, which is why the runtime stage below is
# a bare Alpine rather than an Erlang image -- installing Erlang there would ship
# a second, unused copy of the same VM.
#
# The two Alpine versions MUST stay in lockstep. The release contains ERTS
# binaries compiled against the builder's musl and OpenSSL; dropping them onto a
# different Alpine minor is how you get a crypto NIF that will not load.

ARG ELIXIR_IMAGE=docker.io/hexpm/elixir:1.17.3-erlang-27.1.2-alpine-3.20.3
ARG RUNNER_IMAGE=docker.io/library/alpine:3.20.3


# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM ${ELIXIR_IMAGE} AS build

# git: heroicons and daisyui are git dependencies (see mix.exs).
# build-base: no current dependency has a NIF, but adding one should not turn
#   into a mystery image-build failure.
RUN apk add --no-cache build-base git

WORKDIR /app

RUN mix local.hex --force && mix local.rebar --force

ENV MIX_ENV="prod"

# --- Dependency layers -----------------------------------------------------
# Copied and built before any application source, so editing lib/ or assets/
# does not re-fetch or re-compile a single dependency.

COPY mix.exs mix.lock ./
RUN mix deps.get --only $MIX_ENV

RUN mkdir config
# Only the compile-time configuration: touching these legitimately invalidates
# the dependency build, touching runtime.exs must not (it is copied further
# down, right before the release is assembled).
COPY config/config.exs config/prod.exs config/
RUN mix deps.compile

# The tailwind and esbuild standalone binaries are downloaded, not vendored.
# Fetching them in their own layer keeps that network round trip out of every
# source-only rebuild. The tailwind package detects musl and pulls
# tailwindcss-linux-x64-musl, so Alpine is fine here.
RUN mix assets.setup

# --- Application layers ----------------------------------------------------

COPY priv priv
COPY lib lib
COPY assets assets

# Compile BEFORE building assets, which is the opposite of the stock Phoenix
# Dockerfile and is deliberate: assets/css/app.css imports
# "phoenix-colocated/spectrum_phx/colocated.css", and that file is emitted into
# _build by the :phoenix_live_view compiler. Run tailwind first and it fails to
# resolve the import.
RUN mix compile
RUN mix assets.deploy

COPY config/runtime.exs config/
COPY rel rel

RUN mix release


# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM ${RUNNER_IMAGE} AS runtime

# libstdc++ / libgcc : ERTS links against both.
# openssl            : the crypto NIF dlopens libcrypto; without it the VM will
#                      not boot at all, and TLS to Spark is the whole point.
# ncurses-libs       : needed by erl and by `bin/spectrum_phx remote`.
# ca-certificates    : Spark's mTLS pins the cluster CA explicitly, but any other
#                      outbound HTTPS falls back to the system trust store.
# tzdata             : musl ships no zoneinfo; without it every non-UTC
#                      DateTime lookup fails.
RUN apk add --no-cache \
      ca-certificates \
      libgcc \
      libstdc++ \
      ncurses-libs \
      openssl \
      tzdata \
 && update-ca-certificates

ENV LANG="C.UTF-8" \
    LC_ALL="C.UTF-8" \
    MIX_ENV="prod" \
    PHX_SERVER="true" \
    PORT="8443" \
    RELEASE_TMP="/tmp"

WORKDIR /app

# Non-root by default so the image is not privileged just by existing. Note the
# Quadlet overrides this with User=root, because /root/.certs/client.key is mode
# 0600 and owned by root on the host -- see quadlet/spectrum-phx.container.
RUN addgroup -g 10001 -S spectrum \
 && adduser -u 10001 -S -G spectrum -h /app -s /sbin/nologin spectrum

COPY --from=build --chown=spectrum:spectrum /app/_build/prod/rel/spectrum_phx ./

USER spectrum

# Documentation only: the deployed container uses Network=host.
EXPOSE 8443

# /robots.txt rather than "/": it is served by Plug.Static straight out of the
# release, so this stays a liveness probe for the endpoint instead of quietly
# becoming a readiness probe for ScyllaDB and ZooKeeper (which is what probing a
# LiveView route would make it).
#
# force_ssl in config/prod.exs excludes the hosts "localhost" and "127.0.0.1",
# so this probe gets a 200 rather than a redirect to https.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -q -O /dev/null "http://127.0.0.1:${PORT}/robots.txt" || exit 1

CMD ["/app/bin/spectrum_phx", "start"]
