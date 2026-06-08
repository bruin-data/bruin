# Sail

Bruin supports [Sail](https://docs.lakesail.com/) (LakeSail / PySail) as a SQL data platform. Sail exposes an [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) server and speaks Spark SQL; Bruin connects to it using the Apache ADBC Flight SQL driver.

## Connection

In order to set up a Sail connection, you need to add a configuration item to `connections` in the `.bruin.yml` file. Sail exposes a Flight SQL server (`sail flight server`, default port `32010`) and does not require authentication by default.

```yaml
    connections:
      sail:
        - name: "connection_name"
          host: 127.0.0.1
          port: 32010
```

If your deployment puts authentication in front of Sail, you can supply either username/password or a bearer token (the two are mutually exclusive), and enable TLS:

```yaml
    connections:
      sail:
        - name: "connection_name"
          host: "sail.example.com"
          port: 443
          token: "XXXXXXXXXX"
          tls: true
```

### Connection fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | The name of the connection. |
| `host` | yes | The Sail Flight SQL server host. |
| `port` | yes | The gRPC port (Sail's Flight SQL default is `32010`). |
| `username` | no | Username for username/password auth. Mutually exclusive with `token`. |
| `password` | no | Password for username/password auth. Must not contain `;`. |
| `token` | no | Bearer token, sent as the `Authorization: Bearer <token>` header. Mutually exclusive with `username`/`password`. |
| `database` | no | Used as the database name when introspecting schemas (e.g. `bruin import`). |
| `tls` | no | Use a TLS-encrypted connection (`grpc+tls`). |
| `tls_skip_verify` | no | Skip TLS certificate verification. For testing only; do not use in production. |

## Sail Assets

### `sail.sql`

Runs a materialized Sail SQL asset or a Sail SQL script. For detailed parameters, you can check the [Definition Schema](../assets/definition-schema.md) page. For information about materialization strategies, see the [Materialization](../assets/materialization.md) page.

Sail speaks Spark SQL, so identifiers are quoted with backticks (`` `schema`.`table` ``).

> [!IMPORTANT]
> Use a single SQL statement per `sail.sql` asset. Multi-statement queries are not supported.

#### Example: Create a table using table materialization

```bruin-sql
/* @bruin
name: analytics.installs
type: sail.sql
materialization:
    type: table
@bruin */

SELECT user_id, event_name, ts
FROM analytics.events
WHERE event_name = 'install'
```

#### Example: Run a Sail SQL script

```bruin-sql
/* @bruin
name: analytics.installs
type: sail.sql
@bruin */

CREATE TABLE IF NOT EXISTS analytics.installs AS
SELECT user_id, event_name, ts
FROM analytics.events
WHERE event_name = 'install'
```

### `sail.sensor.query`

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
type: sail.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = '{{ end_date }}')
```
