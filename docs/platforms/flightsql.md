# Arrow Flight SQL

Bruin supports any [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) compatible platform as a SQL data platform. Supported engines:

- [Dremio](https://docs.dremio.com/current/developer/arrow-flight-sql) (`dialect: dremio`, the default)
- [Sail](https://docs.lakesail.com/sail/latest/guide/integrations/flight-sql.html) / PySail (`dialect: sail`)

Because Flight SQL is a transport protocol rather than a SQL dialect, a single `flightsql` connection type works across engines. The SQL dialect used for materializations is selected with the `dialect` field — this controls engine-specific SQL such as identifier quoting (Dremio uses ANSI double quotes, Sail/Spark uses backticks).

## Connection

In order to set up a Flight SQL connection, you need to add a configuration item to `connections` in the `.bruin.yml` file. Authentication is either username/password (Dremio Software) or a bearer token / Personal Access Token (Dremio Cloud) — the two are mutually exclusive.

### Dremio Software (username/password)

```yaml
    connections:
      flightsql:
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
      flightsql:
        - name: "connection_name"
          host: "data.dremio.cloud" # or data.eu.dremio.cloud for the EU control plane
          port: 443
          token: "XXXXXXXXXX"
          tls: true
```

### Sail / PySail

[Sail](https://docs.lakesail.com/sail/latest/guide/integrations/flight-sql.html) exposes a Flight SQL server (`sail flight server`, default port `32010`) and speaks Spark SQL. It does not require authentication by default.

```yaml
    connections:
      flightsql:
        - name: "connection_name"
          host: 127.0.0.1
          port: 32010
          dialect: sail
```

### Connection fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | The name of the connection. |
| `host` | yes | The Flight SQL server host. |
| `port` | yes | The gRPC port (Dremio's Flight SQL default is `32010`; Dremio Cloud uses `443`). |
| `username` | no | Username for username/password auth. Mutually exclusive with `token`. |
| `password` | no | Password for username/password auth. Must not contain `;`. |
| `token` | no | Bearer token / PAT, sent as the `Authorization: Bearer <token>` header. Mutually exclusive with `username`/`password`. |
| `database` | no | Used as the database name when introspecting schemas (e.g. `bruin import`). |
| `dialect` | no | Materialization SQL dialect: `dremio` (default) or `sail`. Controls engine-specific SQL such as identifier quoting. |
| `tls` | no | Use a TLS-encrypted connection (`grpc+tls`). Required for Dremio Cloud. |
| `tls_skip_verify` | no | Skip TLS certificate verification. For testing only; do not use in production. |

## Flight SQL Assets

### `flight.sql`

Runs a materialized Flight SQL asset or a Flight SQL script. For detailed parameters, you can check the [Definition Schema](../assets/definition-schema.md) page. For information about materialization strategies, see the [Materialization](../assets/materialization.md) page.

> [!IMPORTANT]
> Use a single SQL statement per `flight.sql` asset. Multi-statement queries are not supported.

#### Example: Create a table using table materialization

```bruin-sql
/* @bruin
name: analytics.installs
type: flight.sql
materialization:
    type: table
@bruin */

SELECT user_id, event_name, ts
FROM analytics.events
WHERE event_name = 'install'
```

#### Example: Run a Flight SQL script

```bruin-sql
/* @bruin
name: analytics.installs
type: flight.sql
@bruin */

CREATE TABLE IF NOT EXISTS analytics.installs AS
SELECT user_id, event_name, ts
FROM analytics.events
WHERE event_name = 'install'
```

### `flight.sensor.query`

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
type: flight.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = '{{ end_date }}')
```
