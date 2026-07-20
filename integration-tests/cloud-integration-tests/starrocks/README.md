# StarRocks integration tests

This package boots a StarRocks all-in-one Docker image with testcontainers and runs Bruin workflows against the MySQL-compatible FE query port (9030).

The default image is `starrocks/allin1-ubuntu:3.3-latest`. Override it when needed:

```bash
STARROCKS_TEST_IMAGE=starrocks/allin1-ubuntu:3.2-latest go test -v ./...
```

The all-in-one image requires a Linux Docker host with `vm.max_map_count >= 2000000`.

On macOS, Docker Desktop, OrbStack, Colima, and similar tools run containers in a
Linux VM. Prepare that Docker VM before running the suite:

```bash
docker run --rm --privileged alpine sysctl -w vm.max_map_count=2000000
docker run --rm alpine sh -lc 'sysctl vm.max_map_count'
```

These settings can reset when the Docker VM restarts.

The suite skips by default on non-Linux hosts. After preparing the Docker VM, force
the local run:

```bash
STARROCKS_RUN_ON_NON_LINUX=1 go test -v ./...
```

Run from this directory after building the CLI:

```bash
(cd ../../.. && make build)
go test -v ./...
```
