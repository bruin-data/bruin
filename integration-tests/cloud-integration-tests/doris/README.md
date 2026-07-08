# Doris integration tests

This package boots an Apache Doris all-in-one Docker image with testcontainers and runs Bruin workflows against the MySQL-compatible query port.

The default image is `apache/doris:4.0.3-all-slim`. Override it when needed:

```bash
DORIS_TEST_IMAGE=apache/doris:3.0.8-all go test -v ./...
```

The all-in-one image requires a Linux Docker host with:

- `vm.max_map_count >= 2000000`
- swap disabled

On macOS, Docker Desktop, OrbStack, Colima, and similar tools run containers in a
Linux VM. Prepare that Docker VM before running the suite:

```bash
docker run --rm --privileged alpine sysctl -w vm.max_map_count=2000000
docker run --rm --privileged alpine swapoff -a
docker run --rm alpine sh -lc 'sysctl vm.max_map_count && cat /proc/swaps'
```

These settings can reset when the Docker VM restarts.

The suite skips by default on non-Linux hosts. After preparing the Docker VM, force
the local run:

```bash
DORIS_RUN_ON_NON_LINUX=1 go test -v ./...
```

Run from this directory after building the CLI:

```bash
(cd ../../.. && make build)
go test -v ./...
```
