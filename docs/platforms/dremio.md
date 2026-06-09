# Dremio

Bruin supports [Dremio](https://www.dremio.com/) as a SQL data platform. Bruin connects to Dremio over the [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) wire protocol using the Apache ADBC Flight SQL driver.

## Connection

In order to set up a Dremio connection, you need to add a configuration item to `connections` in the `.bruin.yml` file. Authentication is either username/password (Dremio Software) or a bearer token / Personal Access Token (Dremio Cloud) — the two are mutually exclusive.

### Dremio Software (username/password)

```yaml
    connections:
      dremio:
        - name: "connection_name"
          host: "dremio-coordinator.example.com"
          port: 32010
          username: "dremio_user"
          password: "XXXXXXXXXX" # Optional
          database: "my_database" # Optional
```

### Dremio Cloud (token + TLS)

Dremio Cloud does not use username/password; it authenticates with a [Personal Access Token (PAT)](https://docs.dremio.com/cloud/security/authentication/personal-access-token/) and requires TLS.

```yaml
    connections:
      dremio:
        - name: "connection_name"
          host: "data.dremio.cloud" # or data.eu.dremio.cloud for the EU control plane
          port: 443
          token: "XXXXXXXXXX"
          tls: true
```

### Connection fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | The name of the connection. |
| `host` | yes | The Dremio Flight SQL host. |
| `port` | yes | The gRPC port (Dremio's Flight SQL default is `32010`; Dremio Cloud uses `443`). |
| `username` | no | Username for username/password auth. Mutually exclusive with `token`. |
| `password` | no | Password for username/password auth. Must not contain `;`. |
| `token` | no | Bearer token / PAT, sent as the `Authorization: Bearer <token>` header. Mutually exclusive with `username`/`password`. |
| `database` | no | Used as the database name when introspecting schemas (e.g. `bruin import`). |
| `tls` | no | Use a TLS-encrypted connection (`grpc+tls`). Required for Dremio Cloud. |
| `tls_skip_verify` | no | Skip TLS certificate verification. For testing only; do not use in production. |

## Asset names and folders

The asset `name` is the path to the dataset in Dremio, written as `folder.table`. The last component is the table; the first is the folder it lives in — Dremio's equivalent of a schema:

| Asset `name` | Folder | Table |
|--------------|--------|-------|
| `analytics.installs` | `analytics` | `installs` |
| `staging.events` | `staging` | `events` |

There is no separate folder field — you choose the folder purely by how you name the asset, the same `schema.table` convention used across Bruin's platforms.

> [!WARNING]
> Only a flat folder structure (`folder.table`) is supported. Nested folders (`folder.subfolder.table`) are not — name your assets with exactly one folder component.

## Dremio Assets

### `dremio.sql`

Runs a materialized Dremio SQL asset or a Dremio SQL script. For detailed parameters, you can check the [Definition Schema](../assets/definition-schema.md) page. For information about materialization strategies, see the [Materialization](../assets/materialization.md) page.

Dremio identifiers are quoted with ANSI double quotes (`"schema"."table"`).

> [!IMPORTANT]
> Use a single SQL statement per `dremio.sql` asset. Multi-statement queries are not supported.

#### Example: Create a table using table materialization

```bruin-sql
/* @bruin
name: analytics.installs
type: dremio.sql
materialization:
    type: table
@bruin */

SELECT user_id, event_name, ts
FROM analytics.events
WHERE event_name = 'install'
```

#### Example: Run a Dremio SQL script

```bruin-sql
/* @bruin
name: analytics.installs
type: dremio.sql
@bruin */

CREATE TABLE IF NOT EXISTS analytics.installs AS
SELECT user_id, event_name, ts
FROM analytics.events
WHERE event_name = 'install'
```

### `dremio.sensor.query`

Checks if a query returns any results, running on an interval until the query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters:**

- `query`: Query you expect to return any results.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

#### Example: Wait for upstream data

```yaml
name: analytics.events
type: dremio.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = '{{ end_date }}')
```
