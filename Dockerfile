# __build__="1.2.2"
# Overridable for air-gapped or mirrored builds, matching spectrum_phx's Dockerfile:
#   podman build --build-arg BASE_IMAGE=mirror.local:5000/library/python:3.11-slim .
ARG BASE_IMAGE=docker.io/library/python:3.11-slim
FROM ${BASE_IMAGE}
RUN apt-get update && apt-get install -y libvirt-clients qemu-utils procps openssl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
# The repository's filename, renamed on the way in. It used to be `COPY server.py .`,
# which meant the build only worked from a staging directory something else had
# already renamed the file into -- `podman build` from a clean checkout failed on a
# file that does not exist in the tree. The in-image name is unchanged, so CMD and
# every path inside the container stay as they were.
COPY spectrum_server.py ./server.py
COPY hylia.py .
COPY helios_sig.py .
# The ordered cluster schema. spectrum_server's load_schema_module() looks for it
# here; without it a rebuilt image logs "helios_schema.py was not found" on every
# start and applies no migrations.
COPY helios_schema.py .
COPY lanayru.py .
# The Sidon client. spectrum only ever calls it through spark over mTLS -- it has no
# access to the control socket from in here, and should not -- but the module carries
# dfs_engine(), the vdisk naming, and the libvirt disk element, so a VM defined by a
# rebuilt image without it silently falls back to DRBD paths that no longer exist.
COPY helios_sidon.py .
COPY helios_cql.py .
COPY static/ ./static/
EXPOSE 8443
CMD ["python", "-u", "server.py"]
