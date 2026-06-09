# Sail integration tests

These tests exercise the `sail` platform end-to-end against a **real** Sail
Arrow Flight SQL server. They live alongside the other platform suites under
`cloud-integration-tests/`, but unlike those (which connect to a pre-provisioned
external server via `.bruin.cloud.yml`), Sail is self-hostable, so this suite is
fully self-contained: it builds a Sail Docker image and boots it via
[testcontainers-go](https://golang.testcontainers.org/), then runs `bruin`
against it. It needs no `.bruin.cloud.yml` connection.

The suite is **skipped automatically** when no Docker provider is available
(`testcontainers.SkipIfProviderIsNotHealthy`), so it never fails for developers
without Docker.

## Running

This suite runs as part of the cloud integration tests (it is wired into the
orchestrator as `TestSailIntegration`, independent of the `.bruin.cloud.yml`
gate):

```bash
make integration-test-cloud
```

Requirements:
- Docker (the test builds `Dockerfile` here — `python:3.12-slim` + `pysail` — and
  starts a container exposing the Flight SQL port `32010`). When no Docker
  provider is available the suite is skipped.
- The `bruin` binary; the `make` target builds it first.

To run only this suite directly (after `make build`):

```bash
cd integration-tests/cloud-integration-tests/sail && go test -v -count=1 -timeout 20m .
```

## How it works

`sail_test.go`:
1. Builds the image from `Dockerfile` and starts one container (shared across
   sub-tests), waiting for port `32010` to listen.
2. Writes a temporary `.bruin.yml` with a `sail` connection (`sail-default`)
   pointing at the container's mapped host/port.
3. Runs each workflow with the existing `pkg/e2e` harness, then terminates the
   container via `t.Cleanup`.

This directory is its **own Go module** (`go.mod` with a `replace` back to the
repo root) so the testcontainers-go / Docker client dependency tree stays out of
the root module that builds the shipped `bruin` CLI.

## What is covered (core)

| Workflow | Feature |
|----------|---------|
| `connection-test` | `bruin connections test` (Flight SQL `Ping`) |
| `checks-and-custom-checks` | `create+replace` materialization, column checks (`not_null`, `unique`, `positive`, `accepted_values`), a custom check, and `bruin query` |
| `failing-check-fails-the-run` | a failing column check makes `bruin run` exit non-zero |
| `view` | `view` materialization with an upstream dependency |
| `ddl` | `ddl` materialization (create an empty table from a column definition) |
| `schema-auto-create` | materializing into a non-existent schema creates it first (`CREATE SCHEMA IF NOT EXISTS`) |
| `query-sensor` | `sail.sensor.query` |

Sail speaks Spark SQL, so assets use backtick-quoted identifiers and Spark types
(`INT`/`STRING`/`DOUBLE`/`TIMESTAMP`). The `append`, `delete+insert`, and
`truncate+insert` strategies are not covered yet.
