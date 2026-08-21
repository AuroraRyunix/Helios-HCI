# __build__="1.2.2"
FROM docker.io/library/python:3.11-slim
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
COPY static/ ./static/
EXPOSE 8443
CMD ["python", "-u", "server.py"]
