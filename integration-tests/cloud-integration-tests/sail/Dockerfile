# Sail Arrow Flight SQL server, used by the sail integration test suite
# (integration-tests/sail). The suite boots this image via testcontainers-go and
# runs real bruin materializations against it.
FROM python:3.12-slim

# Pin pysail for reproducible test runs; bump deliberately.
ARG PYSAIL_VERSION=0.6.4
RUN pip install --no-cache-dir "pysail==${PYSAIL_VERSION}"

# Sail's Flight SQL default port.
EXPOSE 32010

# Bind on all interfaces so the mapped port is reachable from the host/container host.
CMD ["sail", "flight", "server", "--ip", "0.0.0.0", "--port", "32010"]
