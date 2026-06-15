FROM docker.io/library/python:3.11-slim
RUN apt-get update && apt-get install -y libvirt-clients qemu-utils procps openssl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY server.py .
COPY static/ ./static/
EXPOSE 8443
CMD ["python", "-u", "server.py"]
