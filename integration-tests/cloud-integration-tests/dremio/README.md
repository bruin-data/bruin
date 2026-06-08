# Dremio integration tests

These tests exercise the `dremio` platform against a live Dremio instance. They
mirror the other cloud integration suites: they are **skipped** unless a
`dremio` connection is configured in
`integration-tests/cloud-integration-tests/.bruin.cloud.yml`.

## Prerequisites

1. A reachable Dremio instance (Dremio Software, or Dremio Cloud).
2. A **writable, Iceberg-capable source or space** named **`bruin_test`** that the
   connection user can create/drop/insert/delete tables in. The tests create all
   their tables under this name (`"bruin_test"."<table>"`). If your writable
   source has a different name, change the `schema` constant at the top of
   `dremio_test.go`.
3. A `dremio` connection named `dremio-default` in `.bruin.cloud.yml`:

   ```yaml
   environments:
     default:
       connections:
         dremio:
           - name: dremio-default
             host: dremio-coordinator.example.com
             port: 32010
             username: dremio_user
             password: XXXXXXXXXX
   ```

   For Dremio Cloud use a Personal Access Token over TLS instead:

   ```yaml
           - name: dremio-default
             host: data.dremio.cloud   # data.eu.dremio.cloud for the EU control plane
             port: 443
             token: <personal-access-token>
             tls: true
   ```

## Running

```bash
make integration-test-cloud
```

or, directly:

```bash
cd integration-tests/cloud-integration-tests && go test -v -run TestCloudIntegration/Dremio .
# or the suite on its own:
cd integration-tests/cloud-integration-tests/dremio && go test -v .
```

## What is covered

Each workflow is self-contained (its own table names) and cleans up after itself:

| Workflow | Feature |
|----------|---------|
| `connection-test` | `bruin connections test` (Dremio `Ping`) |
| `checks-and-custom-checks` | `create+replace` materialization, column checks (`not_null`, `unique`, `positive`, `accepted_values`), custom checks, `bruin query` |
| `failing-check-fails-the-run` | a failing column check makes `bruin run` exit non-zero |
| `append` | `append` materialization |
| `delete-insert` | `delete+insert` (incremental) materialization |
| `truncate-insert` | `truncate+insert` materialization |
| `view` | `view` materialization with an upstream dependency |
| `ddl` | `ddl` materialization (create empty table from a column definition) |
| `query-sensor` | `dremio.sensor.query` |
| `import-database` | `bruin import database` (schema introspection) |

The `append`/`delete-insert`/`truncate-insert` strategies require the `bruin_test`
source to support Iceberg writes (INSERT/DELETE/TRUNCATE). A CTAS/DROP-only target
(such as Dremio's `$scratch`) will not satisfy those three workflows.
