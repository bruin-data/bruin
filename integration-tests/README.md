# Integration Tests

This directory contains integration tests for the Bruin data pipeline tool. These tests verify that Bruin works correctly with real pipelines, databases, and various configurations.

## Overview

The integration tests are designed to test Bruin's functionality in realistic scenarios, including:

- **Individual Tasks**: Testing specific Bruin commands and features
- **Workflow Tests**: Testing multi-step workflows and state management
- **Ingestr Tests**: Testing integration with the Ingestr data ingestion tool

## Test Structure

### Test Files

- `integration_test.go`: Individual task, workflow, and ingestion integration tests
- `backfill_test.go`: Backfill CLI workflows using isolated local DuckDB databases
- `test-pipelines/`: Directory containing test pipeline configurations

### Test Categories

1. **Individual Tasks** (`TestIndividualTasks`): Tests for specific Bruin commands

2. **Workflow Tests** (`TestWorkflowTasks`): Tests for multi-step workflows

3. **Ingestr Tests** (`TestIngestrTasks`): Tests for Ingestr integration

### Make Commands

The following make commands are available for running integration tests:

#### Run Full Integration Tests
```bash
# Run all integration tests (including ingestr tests)
make integration-test

# Run all integration tests in parallel
ENABLE_PARALLEL=1 make integration-test
```

#### Run Integration Tests Without Ingestr Tests

```bash
# Run individual task and workflow tests, including backfills
make integration-test-light
# Run individual task and workflow tests in parallel
ENABLE_PARALLEL=1 make integration-test-light
```

**Note**: `ENABLE_PARALLEL=1` enables parallel test execution. By default, tests run sequentially.

#### Run the Backfill Suite

```bash
make integration-test-backfill
```

This builds Bruin and runs the tests in `backfill_test.go`. Each test invokes the
compiled CLI in a temporary Git repository with its own connection configuration
and DuckDB database, then checks the database rows and persisted run records.
No cloud credentials are required.

Coverage includes multi-year daily and monthly ranges, both daylight-saving
transitions, inclusive date boundaries, interval materializations and modifiers,
selectors, variable overrides, failure policies, retries, missing/failed partition
resumes, cancellation, connection limits, and paginated JSON output. The suite
also runs automatically in `make integration-test-light` and `make integration-test`
(the latter is used by CI).

#### Run the Spark Suite

`make integration-test-spark` starts Spark Connect and MinIO with Docker via
testcontainers, so it needs no cloud credentials. It uses the active Docker
context and skips itself when no healthy Docker provider is available. It is
kept out of `make integration-test` because building the Spark image and
pulling the Iceberg and hadoop-aws jars is slow; `make integration-test-cloud`
also runs it, alongside the other self-contained suites.

## Test Pipeline Structure

Each test pipeline in `test-pipelines/` should follow a standard structure:

```text
test-pipelines/
├── pipeline-name/
│   ├── pipeline.yml          # Pipeline configuration
│   ├── assets/               # Pipeline assets (SQL, Python, etc.)
│   │   ├── asset1.sql
│   │   └── asset2.py
│   └── expectations/         # Expected outputs (if applicable)
│       ├── expected.json
│       └── expected.csv
```

### Test Isolation
Each test runs in isolation with:
- Clean temporary directories
- Fresh database instances
- Isolated file systems

## Troubleshooting

### Common Issues

1. **Binary Not Found**: Ensure Bruin is built before running tests
   ```bash
   make build
   ```
2. **Permission Issues**: Ensure write permissions for temporary directories

3. **Ingestr Tests Failing**: Rerun tests. If that doesn't help, verify Ingestr is properly configured if running ingestr tests

## Cloud Integration Tests

Cloud integration tests live under `integration-tests/cloud-integration-tests/` and run only when a matching
connection is present in `integration-tests/cloud-integration-tests/.bruin.cloud.yml`.

### Fabric Warehouse

Add a local config file with a Fabric connection (no credentials are committed):

```yaml
default_environment: default
environments:
  default:
    connections:
      fabric:
        - name: fabric-default
          host: sql-endpoint-guid.datawarehouse.fabric.microsoft.com
          port: 1433
          database: your_warehouse
          use_azure_default_credential: true
```

Then run the cloud integration suite:

```bash
make build
go test ./integration-tests/cloud-integration-tests -run Fabric -v
```

If the Fabric connection is missing, the tests are skipped.
An example config is available at `integration-tests/cloud-integration-tests/.bruin.cloud.yml.example`.
